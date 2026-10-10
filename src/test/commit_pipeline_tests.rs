//! Tests for the commit ring pipeline: conflict windows that outlive a lap of
//! the ring, and serializability of concurrent read-modify-write commits.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tempdir::TempDir;
use test_log::test;
use xxhash_rust::xxh3::xxh3_64;

use crate::clock::LogicalClock;
use crate::lsm::Tree;
use crate::ring::{PipelineHook, COMMIT_RING_CAPACITY, RETIRE_EVERY_ENTRIES, RETIRE_EVERY_GROUPS};
use crate::test::retire_isolation_tests::overflow_drained;
use crate::{Durability, Error, Options, TreeBuilder};

fn create_store() -> (Tree, TempDir) {
	let temp_dir = TempDir::new("test").unwrap();
	let tree = TreeBuilder::new().with_path(temp_dir.path().to_path_buf()).build().unwrap();
	(tree, temp_dir)
}

/// Commits enough single-key transactions to lap the commit ring several
/// times, overwriting every slot that was in use before.
async fn lap_the_ring(store: &Tree) {
	for i in 0..(3 * COMMIT_RING_CAPACITY) {
		let mut txn = store.begin().unwrap();
		txn.set(format!("filler{i}").as_bytes(), b"v").unwrap();
		txn.commit().await.unwrap();
	}
}

#[test(tokio::test)]
async fn long_lived_txn_commits_across_ring_laps() {
	let (store, _dir) = create_store();

	let mut long = store.begin().unwrap();
	long.set(b"long", b"1").unwrap();
	lap_the_ring(&store).await;
	long.commit().await.unwrap();

	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"long").unwrap().unwrap(), b"1");
}

#[test(tokio::test)]
async fn long_lived_txn_detects_a_write_conflict_lapped_by_the_ring() {
	let (store, _dir) = create_store();

	let mut long = store.begin().unwrap();
	long.set(b"contended", b"long").unwrap();

	// The conflicting commit is followed by several laps of others, so its
	// ring slot is overwritten long before `long` validates.
	let mut other = store.begin().unwrap();
	other.set(b"contended", b"other").unwrap();
	other.commit().await.unwrap();
	lap_the_ring(&store).await;

	assert!(matches!(long.commit().await, Err(Error::TransactionWriteConflict)));
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"contended").unwrap().unwrap(), b"other");
}

#[test(tokio::test)]
async fn long_lived_txn_detects_a_locked_read_conflict_lapped_by_the_ring() {
	let (store, _dir) = create_store();

	let mut long = store.begin().unwrap();
	assert!(long.get_for_update(b"locked").unwrap().is_none());
	long.set(b"elsewhere", b"long").unwrap();

	let mut other = store.begin().unwrap();
	other.set(b"locked", b"other").unwrap();
	other.commit().await.unwrap();
	lap_the_ring(&store).await;

	assert!(matches!(long.commit().await, Err(Error::TransactionWriteConflict)));
}

#[test(tokio::test)]
async fn commits_visible_before_begin_never_conflict_across_laps() {
	let (store, _dir) = create_store();

	let mut first = store.begin().unwrap();
	first.set(b"key", b"first").unwrap();
	first.commit().await.unwrap();
	lap_the_ring(&store).await;

	// Everything above is in this transaction's snapshot, so none of it is
	// in its conflict window.
	let mut txn = store.begin().unwrap();
	assert_eq!(txn.get_for_update(b"key").unwrap().unwrap(), b"first");
	txn.set(b"key", b"second").unwrap();
	txn.commit().await.unwrap();
}

/// Runs concurrent tasks that each commit read-modify-write increments of one
/// counter, retrying on conflict, and checks that every successful commit read
/// the value the previous one wrote.
///
/// With `long_lived`, a transaction that read the counter stays open for the
/// whole run and so holds the retired watermark at its window while the tasks
/// lap the commit ring. It must still find the conflict at the end.
async fn assert_increments_serialize(locked: bool, long_lived: bool) {
	let (tasks, increments): (u64, u64) = if long_lived {
		(8, 400)
	} else {
		(32, 40)
	};

	let (store, _dir) = create_store();
	let store = Arc::new(store);

	let long = long_lived.then(|| {
		let mut long = store.begin().unwrap();
		assert!(long.get_for_update(b"counter").unwrap().is_none());
		long
	});
	let window = long.as_ref().map(|long| long.commit_window_for_test());

	let handles: Vec<_> = (0..tasks)
		.map(|_| {
			let store = Arc::clone(&store);
			tokio::spawn(async move {
				let mut done = 0;
				while done < increments {
					let mut txn = store.begin().unwrap();
					let read = if locked {
						txn.get_for_update(b"counter")
					} else {
						txn.get(b"counter")
					};
					let current = read
						.unwrap()
						.map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap()))
						.unwrap_or(0);
					txn.set(b"counter", &(current + 1).to_be_bytes()).unwrap();
					match txn.commit().await {
						Ok(()) => done += 1,
						Err(Error::TransactionWriteConflict) => {}
						Err(e) => panic!("unexpected commit error: {e:?}"),
					}
				}
			})
		})
		.collect();
	for h in handles {
		h.await.unwrap();
	}

	if let Some(mut long) = long {
		let window = window.unwrap();
		let (completed, taken, _) = store.core.commit_pipeline.watermarks();
		assert!(
			completed - window > COMMIT_RING_CAPACITY as u64,
			"the run must lap the ring while the transaction is open ({completed} from {window})"
		);
		assert!(taken <= window, "retired watermark {taken} passed the live window {window}");
		assert!(matches!(long.commit().await, Err(Error::TransactionWriteConflict)));
	}

	let txn = store.begin().unwrap();
	let counter = txn.get(b"counter").unwrap().unwrap();
	assert_eq!(u64::from_be_bytes(counter.as_slice().try_into().unwrap()), tasks * increments);
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn concurrent_locked_increments_never_lose_updates() {
	assert_increments_serialize(true, false).await;
}

/// Under snapshot isolation two transactions that both write the counter
/// conflict, so even plain reads serialize these increments.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn concurrent_increments_never_lose_updates() {
	assert_increments_serialize(false, false).await;
}

/// The same increments with a transaction held open across the whole run.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn concurrent_increments_never_lose_updates_beside_a_long_lived_transaction() {
	assert_increments_serialize(false, true).await;
}

/// A transaction is live at every moment: the next one begins before the previous one
/// commits. The retired watermark must still follow the commits, because the oldest live
/// window starts at the completed prefix of the ring, which keeps moving.
///
/// If a transaction pinned the retired watermark itself instead, the pin of a live
/// transaction could never be above the watermark, so the watermark would stay where it
/// was, every entry a lap of the ring overwrote would be kept in the overflow map, and the
/// map would grow with every commit.
#[test(tokio::test)]
async fn retired_watermark_follows_commits_while_a_transaction_is_always_live() {
	let (store, _dir) = create_store();

	let mut live = store.begin().unwrap();
	for i in 0..(4 * COMMIT_RING_CAPACITY) {
		let next = store.begin().unwrap();
		live.set(format!("overlap{i}").as_bytes(), b"v").unwrap();
		live.commit().await.unwrap();
		live = next;
	}
	// The flusher retires after it wakes the committer; give it a moment.
	tokio::time::sleep(std::time::Duration::from_millis(200)).await;

	let (completed, taken, overflow) = store.core.commit_pipeline.watermarks();
	assert!(
		completed - taken <= 64,
		"retired watermark {taken} lags the completed prefix {completed}"
	);
	assert!(overflow <= 64, "{overflow} lapped entries kept in the overflow map");
}

/// Two transactions stay open across four laps of the ring: one read a key that is written
/// after its window opened, the other read a key nobody writes. While they are live the
/// retired watermark stays at or below their window and every entry a lap overwrote above it
/// is kept for them, so the first still finds its conflict and the second is not failed for
/// want of an entry (validation treats a missing entry as a conflict). Once they end the
/// watermark catches up within `RETIRE_EVERY_GROUPS` groups and the overflow map drains.
#[test(tokio::test)]
async fn long_lived_txns_hold_the_watermark_and_keep_the_entries_they_can_reach() {
	let (store, _dir) = create_store();
	let pipeline = &store.core.commit_pipeline;
	let cap = COMMIT_RING_CAPACITY as u64;

	let mut conflicting = store.begin().unwrap();
	assert!(conflicting.get_for_update(b"contended").unwrap().is_none());
	conflicting.set(b"conflicting-out", b"v").unwrap();
	let mut disjoint = store.begin().unwrap();
	assert!(disjoint.get_for_update(b"untouched").unwrap().is_none());
	disjoint.set(b"disjoint-out", b"v").unwrap();
	let window = conflicting.commit_window_for_test().min(disjoint.commit_window_for_test());

	for i in 0..(4 * COMMIT_RING_CAPACITY) {
		let mut txn = store.begin().unwrap();
		let key = if i == COMMIT_RING_CAPACITY {
			"contended".to_string()
		} else {
			format!("filler{i}")
		};
		txn.set(key.as_bytes(), b"v").unwrap();
		txn.commit().await.unwrap();
	}

	let (completed, taken, overflow) = pipeline.watermarks();
	assert!(completed - window > 3 * cap, "the window spans laps: {window}..{completed}");
	assert!(taken <= window, "retired watermark {taken} passed the live window {window}");
	// Every sequence above the window that a lap overwrote must be in the overflow map.
	assert!(
		overflow as u64 >= completed - cap - window,
		"{overflow} entries kept for a window of {} lapped sequences",
		completed - cap - window
	);

	assert!(matches!(conflicting.commit().await, Err(Error::TransactionWriteConflict)));
	disjoint.commit().await.unwrap();
	assert_eq!(store.core.active_txn_tracker.len(), 0);

	for i in 0..(2 * RETIRE_EVERY_GROUPS) {
		let mut txn = store.begin().unwrap();
		txn.set(format!("after{i}").as_bytes(), b"v").unwrap();
		txn.commit().await.unwrap();
	}
	overflow_drained(&store).await;
	let (completed, taken, overflow) = pipeline.watermarks();
	assert!(
		completed - taken <= u64::from(RETIRE_EVERY_GROUPS),
		"retired watermark {taken} lags the completed prefix {completed} by more than a retire interval"
	);
	assert_eq!(overflow, 0, "the overflow map drains once no window can reach it");
}

/// A logical clock that hands out the same timestamps on every run, so the bytes of a WAL
/// record do not depend on when it was written.
#[derive(Debug)]
struct StepClock(AtomicU64);

impl LogicalClock for StepClock {
	fn now(&self) -> u64 {
		self.0.fetch_add(1, Ordering::SeqCst) + 1000
	}
}

/// Retirement frees memory and nothing else, so what the commit pipeline logs must not
/// depend on it. A fixed single-threaded commit sequence, long enough to retire several
/// times and with a record spanning WAL blocks, must write the exact segment bytes that
/// the pipeline wrote when it retired after every group. The length and digest below were
/// captured from that implementation: a change to them is a change to the WAL format.
#[test(tokio::test)]
async fn wal_bytes_of_a_fixed_commit_sequence_do_not_change() {
	let dir = TempDir::new("wal_bytes").unwrap();
	let store = Tree::new(Arc::new(Options {
		path: dir.path().to_path_buf(),
		flush_on_close: false,
		clock: Arc::new(StepClock(AtomicU64::new(0))),
		..Default::default()
	}))
	.unwrap();

	let big: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
	for i in 0..100u32 {
		let mut txn = store.begin().unwrap();
		// Every fifth commit syncs, the last one included.
		txn.set_durability(if i % 5 == 4 {
			Durability::Immediate
		} else {
			Durability::Eventual
		});
		match i % 10 {
			// A record that spans 32 KiB WAL blocks
			0 => txn.set(format!("big{i:03}").as_bytes(), &big).unwrap(),
			3 => {
				txn.set(format!("gone{i:03}").as_bytes(), b"soon").unwrap();
				txn.delete(format!("key{:03}", i - 1).as_bytes()).unwrap();
			}
			7 => {
				for k in 0..5 {
					txn.set(format!("multi{i:03}_{k}").as_bytes(), vec![k as u8; 3 * k + 1])
						.unwrap();
				}
			}
			_ => txn.set(format!("key{i:03}").as_bytes(), format!("value{i}").as_bytes()).unwrap(),
		}
		txn.commit().await.unwrap();
	}

	// Opening a tree continues in a fresh segment, which leaves the first one empty, so only
	// the segments that hold bytes are counted: all of them must be one, the commits did not
	// rotate the WAL.
	let mut segments: Vec<(u64, PathBuf)> = std::fs::read_dir(dir.path().join("wal"))
		.unwrap()
		.map(|entry| entry.unwrap().path())
		.filter(|path| path.extension().is_some_and(|ext| ext == "wal"))
		.filter(|path| std::fs::metadata(path).unwrap().len() > 0)
		.map(|path| (path.file_stem().unwrap().to_str().unwrap().parse().unwrap(), path))
		.collect();
	segments.sort();
	let bytes: Vec<u8> =
		segments.iter().flat_map(|(_, path)| std::fs::read(path).unwrap()).collect();
	assert_eq!(
		(segments.len(), bytes.len(), xxh3_64(&bytes)),
		(1, 404_281, 5_576_617_551_117_973_792),
		"WAL bytes of the fixed commit sequence"
	);
}

/// Retirement used to run only every `RETIRE_EVERY_GROUPS` groups, so a few huge groups left
/// nearly the whole ring unretired while the flusher sat idle, with no timer to catch up. It
/// also runs once `RETIRE_EVERY_ENTRIES` entries have been drained. Each burst here is held
/// behind the first group so it is drained as one huge group: three of them are 9 groups or
/// fewer, but more than the entry limit.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_burst_of_huge_groups_is_retired_without_waiting_for_more_groups() {
	let (store, _dir) = create_store();
	let store = Arc::new(store);
	let hold = Arc::new(AtomicBool::new(false));
	{
		let hold = Arc::clone(&hold);
		store.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
			if matches!(point, PipelineHook::AfterWalSync { .. }) {
				while hold.load(Ordering::SeqCst) {
					std::thread::sleep(Duration::from_millis(1));
				}
			}
		})));
	}

	// Admission holds at most half the ring, so a burst of that many fills it.
	let burst = COMMIT_RING_CAPACITY / 2;
	for round in 0..3 {
		hold.store(true, Ordering::SeqCst);
		let mut handles = Vec::new();
		for i in 0..burst {
			let store = Arc::clone(&store);
			handles.push(tokio::spawn(async move {
				let mut txn = store.begin().unwrap();
				txn.set(format!("burst{round}-{i}").as_bytes(), b"v").unwrap();
				txn.commit().await.unwrap();
			}));
		}
		tokio::time::sleep(Duration::from_millis(250)).await;
		hold.store(false, Ordering::SeqCst);
		for handle in handles {
			handle.await.unwrap();
		}
	}
	store.core.commit_pipeline.set_hook(None);

	// 3 bursts of `burst` entries in about nine groups: the group count alone would not have
	// retired any of it, and all three would stay held. The entry limit retires at the end of
	// the second burst, but that retire cannot pass the pins of the group's own committers,
	// which drop their guards just after they are woken, so it frees the first burst and not
	// the second. What remains held is therefore the last two bursts, never all three.
	let limit = 2 * (burst as u64) + u64::from(RETIRE_EVERY_GROUPS);
	let deadline = std::time::Instant::now() + Duration::from_secs(5);
	loop {
		let (completed, taken, _) = store.core.commit_pipeline.watermarks();
		if completed - taken <= limit {
			assert!(
				completed >= 3 * burst as u64,
				"all {} commits completed, only {completed} did",
				3 * burst
			);
			break;
		}
		assert!(
			std::time::Instant::now() < deadline,
			"retired watermark {taken} lags the completed prefix {completed} by more than {limit} \
			 although more than {RETIRE_EVERY_ENTRIES} entries were drained"
		);
		tokio::time::sleep(Duration::from_millis(10)).await;
	}
}
