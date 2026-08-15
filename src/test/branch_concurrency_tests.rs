//! V5: the races the error types already name, actually raced.
//!
//! Before this file, **no branch operation appeared inside a spawned task or
//! thread anywhere in the repository**, and no branch test used a
//! multi-threaded runtime — so even the interleavings tokio could have produced
//! were suppressed. `Error::CompactionPinRaced` exists *solely* to describe a
//! race, and was tested by hand-assembling the state that race would leave.
//!
//! Every test here asserts an **invariant, not a schedule**: the outcome must be
//! one of a named set, and the store must be consistent whichever it was. A test
//! that demanded a particular winner would be asserting a scheduling accident.
//!
//! If one of these ever flakes, that is a finding. The assertion says what must
//! hold under every interleaving; a counter-example is a bug report, not a test
//! to loosen.

use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::compaction::leveled::Strategy;
use crate::lsm::Tree;
use crate::{Error, ForkPoint, MergeStrategy, TreeBuilder};

fn create_shared_store(levels: u8) -> (Arc<Tree>, TempDir) {
	let temp_dir = TempDir::new("branch-concurrency").unwrap();
	let path = temp_dir.path().to_path_buf();
	let tree = TreeBuilder::new().with_path(path).with_level_count(levels).build().unwrap();
	(Arc::new(tree), temp_dir)
}

async fn seed(store: &Tree, key: &[u8], value: &[u8]) {
	let mut txn = store.begin().unwrap();
	txn.set(key, value).unwrap();
	txn.commit().await.unwrap();
}

/// Many forks off one parent, concurrently. Every one that reports success must
/// exist and resolve; ids and generations must be unique.
///
/// The catalog serialises branch operations behind one mutex, so this is a test
/// that the serialisation is real rather than assumed — a lost update here would
/// show up as two branches sharing a generation, which is what generation
/// fencing depends on being impossible.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn concurrent_forks_off_one_parent_all_resolve_and_never_share_a_generation() {
	const FORKS: usize = 16;
	let (store, _temp) = create_shared_store(2);
	seed(&store, b"k", b"v").await;

	let mut tasks = Vec::new();
	for index in 0..FORKS {
		let store = Arc::clone(&store);
		tasks.push(tokio::task::spawn_blocking(move || {
			store.fork_branch("main", &format!("fork/{index}"), ForkPoint::Head)
		}));
	}

	let mut generations = Vec::new();
	for task in tasks {
		let handle = task.await.unwrap().expect("a fork off a live parent must succeed");
		generations.push(handle.generation());
	}

	generations.sort_by_key(|generation| generation.0);
	let unique = {
		let mut seen = generations.clone();
		seen.dedup_by_key(|generation| generation.0);
		seen.len()
	};
	assert_eq!(unique, FORKS, "two branches were handed the same generation");

	// Every one is live and reads what it forked from.
	for index in 0..FORKS {
		let txn = store.begin_on(&format!("fork/{index}")).unwrap();
		assert_eq!(txn.get(b"k").unwrap(), Some(b"v".to_vec()));
	}
	assert_eq!(store.list_branches().unwrap().len(), FORKS + 1);
}

/// Forking while the parent compacts — the race `CompactionPinRaced` exists for.
///
/// Either ordering is legal. What is not legal is a child whose inherited view
/// is missing the version its anchor promises: if the compaction wins and
/// discards a version the new fork needs, it must discard its own output
/// instead. The assertion is on the child's read, which is the property, rather
/// than on which side won, which is the schedule.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn forking_while_the_parent_compacts_never_costs_the_child_its_view() {
	for attempt in 0..8u32 {
		let (store, _temp) = create_shared_store(2);
		seed(&store, b"k", b"v1").await;
		store.drain_flushes_synchronously().unwrap();
		for round in 0..4u32 {
			let mut txn = store.begin().unwrap();
			txn.set(b"k", format!("v{round}").as_bytes()).unwrap();
			txn.commit().await.unwrap();
			store.drain_flushes_synchronously().unwrap();
		}

		let compacting = {
			let store = Arc::clone(&store);
			tokio::task::spawn_blocking(move || {
				let strategy = Arc::new(Strategy::from_options(Arc::clone(&store.core.inner.opts)));
				// A pin race is a legal outcome here, not a failure: the job
				// discards its output and the next cycle re-picks the inputs.
				match store.compact(strategy) {
					Ok(())
					| Err(Error::CompactionPinRaced {
						..
					}) => {}
					Err(error) => panic!("unexpected compaction failure: {error}"),
				}
			})
		};
		let forking = {
			let store = Arc::clone(&store);
			tokio::task::spawn_blocking(move || store.fork_branch("main", "racer", ForkPoint::Head))
		};

		compacting.await.unwrap();
		let child = forking.await.unwrap().expect("a fork at head must succeed");

		// Whatever the interleaving, the child reads the value that was current
		// at its anchor. `v3` is the newest write; a fork at head sees it.
		let txn = child.begin().unwrap();
		assert_eq!(
			txn.get(b"k").unwrap(),
			Some(b"v3".to_vec()),
			"attempt {attempt}: the fork lost the version its anchor promised"
		);
	}
}

/// Deleting a branch while it is being written to. The write either lands or is
/// fenced; it may never land on a branch the catalog says is gone.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn deleting_a_branch_while_it_is_written_either_fences_or_completes() {
	for attempt in 0..8u32 {
		let (store, _temp) = create_shared_store(2);
		seed(&store, b"k", b"v").await;
		let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();

		let writing = {
			let child = child.clone();
			tokio::spawn(async move {
				let mut txn = match child.begin() {
					Ok(txn) => txn,
					// Fenced before the transaction even opened.
					Err(Error::BranchFenced) => return Ok(()),
					Err(error) => panic!("unexpected begin failure: {error}"),
				};
				txn.set(b"racing", b"v").unwrap();
				txn.commit().await
			})
		};
		let deleting = {
			let store = Arc::clone(&store);
			tokio::task::spawn_blocking(move || store.delete_branch("work"))
		};

		let write_result = writing.await.unwrap();
		deleting.await.unwrap().expect("deleting a live branch must succeed");

		match write_result {
			// Fenced: the delete won.
			Err(Error::BranchFenced) => {}
			// Landed: the write won, and the branch is gone now regardless.
			Ok(()) => {}
			Err(error) => panic!("attempt {attempt}: unexpected write outcome: {error}"),
		}
		assert!(
			store.branch("work").is_err(),
			"attempt {attempt}: the branch must be gone whichever side won"
		);
	}
}

/// Two sources merging into one target at the same time. Both may not silently
/// win on the same key: at most one applies it, and the other conflicts,
/// retries, or converges.
///
/// This is the concurrent form of PD2's C5 property, which until now was only
/// ever exercised sequentially.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn two_sources_merging_into_one_target_never_both_win_a_key() {
	for attempt in 0..8u32 {
		let (store, _temp) = create_shared_store(2);
		seed(&store, b"shared", b"base").await;

		let left = store.fork_branch("main", "left", ForkPoint::Head).unwrap();
		let right = store.fork_branch("main", "right", ForkPoint::Head).unwrap();
		for (branch, value) in [(&left, b"by-left".as_slice()), (&right, b"by-right".as_slice())] {
			let mut txn = branch.begin().unwrap();
			txn.set(b"shared", value).unwrap();
			txn.commit().await.unwrap();
		}

		// `merge_into`'s future is not `Send` (it holds a `DiffIter` and a
		// `&mut dyn TargetProbe` across await points), so a merge cannot be
		// `tokio::spawn`ed. Real parallelism comes from a blocking thread with
		// its own `block_on` instead — which is also what a caller has to do.
		let main_for_left = store.branch("main").unwrap();
		let main_for_right = store.branch("main").unwrap();
		let handle = tokio::runtime::Handle::current();
		let merging_left = {
			let handle = handle.clone();
			tokio::task::spawn_blocking(move || {
				handle.block_on(left.merge_into(&main_for_left, MergeStrategy::Strict))
			})
		};
		let merging_right = tokio::task::spawn_blocking(move || {
			handle.block_on(right.merge_into(&main_for_right, MergeStrategy::Strict))
		});

		let outcomes = [merging_left.await.unwrap(), merging_right.await.unwrap()];
		let applied: usize = outcomes
			.iter()
			.map(|outcome| outcome.as_ref().map(|outcome| outcome.applied).unwrap_or(0))
			.sum();
		assert!(applied <= 1, "attempt {attempt}: both merges applied the same key: {outcomes:?}");

		// Whatever landed, the target holds exactly one of the two values — never
		// a blend, never the base if a merge reported success.
		let txn = store.begin().unwrap();
		let value = txn.get(b"shared").unwrap().unwrap();
		let expected: &[&[u8]] = if applied == 0 {
			&[b"base"]
		} else {
			&[b"by-left", b"by-right"]
		};
		assert!(
			expected.contains(&value.as_slice()),
			"attempt {attempt}: target holds {:?}, which no merge could have written",
			String::from_utf8_lossy(&value)
		);
	}
}

/// A merge racing ordinary writes to the target. The oracle's job is to make
/// this a conflict rather than a silent overwrite — the property PD3a's
/// `start_seq` fix exists for, exercised here by a real race instead of a
/// hand-built one.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_merge_racing_target_writes_never_loses_one_silently() {
	for attempt in 0..8u32 {
		let (store, _temp) = create_shared_store(2);
		seed(&store, b"contested", b"base").await;
		let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
		let mut txn = child.begin().unwrap();
		txn.set(b"contested", b"by-child").unwrap();
		txn.commit().await.unwrap();

		let main = store.branch("main").unwrap();
		let handle = tokio::runtime::Handle::current();
		let merging = tokio::task::spawn_blocking(move || {
			handle.block_on(child.merge_into(&main, MergeStrategy::Strict))
		});
		let writing = {
			let store = Arc::clone(&store);
			tokio::spawn(async move {
				let mut txn = store.begin().unwrap();
				txn.set(b"contested", b"by-target").unwrap();
				txn.commit().await
			})
		};

		let merge_result = merging.await.unwrap();
		let write_result = writing.await.unwrap();

		let txn = store.begin().unwrap();
		let value = txn.get(b"contested").unwrap().unwrap();
		drop(txn);

		match (&merge_result, &write_result) {
			// Both succeeded: the writer must have run entirely before the merge
			// planned, or entirely after it committed. Either way the surviving
			// value is one of the two, never the base.
			(Ok(_), Ok(())) => assert!(
				value == b"by-child" || value == b"by-target",
				"attempt {attempt}: both succeeded but the target holds {:?}",
				String::from_utf8_lossy(&value)
			),
			// The merge was refused. The write won and must be intact.
			(Err(_), Ok(())) => assert_eq!(
				value,
				b"by-target".to_vec(),
				"attempt {attempt}: the merge was refused yet overwrote the write"
			),
			// The write was refused; the merge's value stands.
			(Ok(_), Err(_)) => assert_eq!(value, b"by-child".to_vec()),
			(Err(merge), Err(write)) => {
				panic!("attempt {attempt}: neither side made progress: {merge} / {write}")
			}
		}
	}
}

/// The maintenance sweep running against live branch traffic. The sweep must
/// never reclaim something a live branch still needs, and must never fail
/// because the catalog moved under it.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn the_maintenance_sweep_runs_safely_against_live_branch_traffic() {
	let (store, _temp) = create_shared_store(2);
	seed(&store, b"k", b"v").await;

	let sweeping = {
		let store = Arc::clone(&store);
		tokio::task::spawn_blocking(move || {
			for _ in 0..20 {
				store.core.inner.sweep_branch_maintenance().expect("a sweep must not fail");
				std::thread::yield_now();
			}
		})
	};
	let churning = {
		let store = Arc::clone(&store);
		tokio::task::spawn_blocking(move || {
			for index in 0..20 {
				let name = format!("churn/{index}");
				store.fork_branch("main", &name, ForkPoint::Head).unwrap();
				store.delete_branch(&name).unwrap();
			}
		})
	};

	sweeping.await.unwrap();
	churning.await.unwrap();

	// A final sweep leaves nothing behind, and `main` is untouched.
	crate::test::support::sweep(&store);
	assert_eq!(store.list_branches().unwrap().len(), 1, "only main should remain");
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"v".to_vec()));
}
