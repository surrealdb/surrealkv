//! V6: randomized histories checked against a model of branch visibility.
//!
//! Every one of the 1,100-odd tests before this file is a hand-written fixed
//! scenario. `proptest` has been a declared dependency with zero uses since it
//! was added. This is the slice that generates histories instead of imagining
//! them.
//!
//! # Why this is not the `BranchModel` PA2 deleted
//!
//! That one was a second *engine* — a 990-line `KernelDatabase` plus ~320 lines
//! of model — compared operation-by-operation against the real one. It had to
//! track storage behaviour to stay honest, which is why it was 1,300 lines and
//! why keeping it in sync was the thing that made it not worth having.
//!
//! [`VisibilityModel`] is the *specification* of what branching means, and is
//! deliberately ignorant of storage: no SSTs, no memtables, no compaction, no
//! WAL, no authority, no sequence numbers except the one global commit counter
//! that fork anchors are expressed in. It answers exactly one question — "what
//! should this branch see for this key?" — which is the question all three of
//! this project's silent-wrong-answer bugs got wrong.
//!
//! **The test for whether it has drifted back into the old shape:** if it ever
//! needs to know about a flush, a level, a table, or a sequence that is not a
//! fork anchor or a commit cap, it has, and it must be cut back rather than
//! extended.

use std::collections::BTreeMap;

use proptest::prelude::*;
use tempdir::TempDir;

use crate::lsm::Tree;
use crate::{ForkPoint, MergeStrategy};

/// Keys and values are drawn from tiny alphabets on purpose: collisions are
/// where the interesting behaviour is, and a random 32-byte key never collides.
const KEYS: [&[u8]; 4] = [b"a", b"b", b"c", b"d"];
const VALUES: [&[u8]; 3] = [b"1", b"2", b"3"];

#[derive(Clone, Debug)]
enum Op {
	/// Fork `child` off `parent` at the parent's current head.
	Fork {
		parent: usize,
		child: usize,
	},
	Write {
		branch: usize,
		key: usize,
		value: usize,
	},
	Delete {
		branch: usize,
		key: usize,
	},
	/// Merge `source` into its parent, source-wins so it never refuses.
	Merge {
		source: usize,
	},
	/// Push the active memtables to SSTs. A no-op for the model — which is the
	/// point: it must be a no-op for the engine's *visible state* too.
	Flush,
	/// Merge SSTs, dropping superseded versions. Also visibly a no-op — and the
	/// operation the retention promise exists to survive.
	Compact,
	/// Close and reopen. Also a no-op for visible state.
	Reopen,
	/// Overwrite one key several times, flushing between, so a later compaction
	/// has superseded versions it could drop.
	///
	/// Random operations almost never build this shape, and a suite that never
	/// makes a compaction drop anything cannot test a retention promise — the
	/// coverage counter said zero until this existed. Biasing a generator toward
	/// the interesting region is the point of writing one.
	Churn {
		key: usize,
	},
}

/// What each branch should see, and nothing else.
#[derive(Clone, Debug, Default)]
struct VisibilityModel {
	/// One global commit clock, exactly as the engine has.
	clock: u64,
	branches: Vec<Option<ModelBranch>>,
}

/// Every write to one key, oldest first: `(seq, value)`, where `None` is a
/// delete. A history rather than a latest-value, because a capped read has to
/// find the newest write at or below its cap.
type KeyHistory = Vec<(u64, Option<Vec<u8>>)>;

#[derive(Clone, Debug)]
struct ModelBranch {
	/// `(parent index, fork anchor)`. `None` for `main`.
	parent: Option<(usize, u64)>,
	/// Every write this branch made itself.
	own: BTreeMap<Vec<u8>, KeyHistory>,
}

impl VisibilityModel {
	fn new() -> Self {
		Self {
			clock: 0,
			branches: vec![Some(ModelBranch {
				parent: None,
				own: BTreeMap::new(),
			})],
		}
	}

	fn exists(&self, index: usize) -> bool {
		self.branches.get(index).is_some_and(|branch| branch.is_some())
	}

	fn fork(&mut self, parent: usize, child: usize) {
		let anchor = self.clock;
		while self.branches.len() <= child {
			self.branches.push(None);
		}
		self.branches[child] = Some(ModelBranch {
			parent: Some((parent, anchor)),
			own: BTreeMap::new(),
		});
	}

	fn write(&mut self, branch: usize, key: &[u8], value: Option<Vec<u8>>) {
		self.clock += 1;
		let seq = self.clock;
		let entry = self.branches[branch].as_mut().expect("write to a live branch");
		entry.own.entry(key.to_vec()).or_default().push((seq, value));
	}

	/// The newest write to `key` on `branch` itself, at or below `cap`.
	fn own_at(&self, branch: usize, key: &[u8], cap: u64) -> Option<(u64, Option<Vec<u8>>)> {
		self.branches[branch]
			.as_ref()?
			.own
			.get(key)?
			.iter()
			.rev()
			.find(|(seq, _)| *seq <= cap)
			.cloned()
	}

	/// What `branch` sees for `key`: its own newest write, or its ancestors'
	/// newest write at the running-minimum of the anchors on the path.
	///
	/// This is the whole specification. `cap` narrows monotonically as the walk
	/// goes up, because a child can never see more of an ancestor than its own
	/// parent could.
	fn get(&self, branch: usize, key: &[u8]) -> Option<Vec<u8>> {
		let mut current = branch;
		let mut cap = u64::MAX;
		loop {
			if let Some((_, value)) = self.own_at(current, key, cap) {
				return value;
			}
			let (parent, anchor) = self.branches[current].as_ref()?.parent?;
			cap = cap.min(anchor);
			current = parent;
		}
	}

	/// Applies to `target` exactly the keys a merge reported writing. The model
	/// does not re-derive the conflict rules — restating the decision table here
	/// would only prove it was written twice.
	fn apply_merge(&mut self, target: usize, applied: Vec<(Vec<u8>, Option<Vec<u8>>)>) {
		for (key, value) in applied {
			self.write(target, &key, value);
		}
	}
}

fn op_strategy() -> impl Strategy<Value = Op> {
	prop_oneof![
		4 => (0usize..4, 0usize..4).prop_map(|(parent, child)| Op::Fork {
			parent,
			child: child + 1
		}),
		8 => (0usize..5, 0usize..KEYS.len(), 0usize..VALUES.len())
			.prop_map(|(branch, key, value)| Op::Write { branch, key, value }),
		3 => (0usize..5, 0usize..KEYS.len()).prop_map(|(branch, key)| Op::Delete { branch, key }),
		3 => (1usize..5).prop_map(|source| Op::Merge { source }),
		4 => Just(Op::Flush),
		3 => Just(Op::Compact),
		2 => Just(Op::Reopen),
		4 => (0usize..KEYS.len()).prop_map(|key| Op::Churn { key }),
	]
}

struct Harness {
	store: Tree,
	_temp: TempDir,
	path: std::path::PathBuf,
	/// Model index -> branch name. Index 0 is always `main`.
	names: Vec<String>,
}

/// Two L0 tables are enough to trigger a merge, and two levels make that merge
/// reach the bottom. Without this the generated histories never build enough
/// tables for a compaction to drop anything, and a suite that never drops a
/// version cannot test a retention promise.
fn options(path: std::path::PathBuf) -> std::sync::Arc<crate::Options> {
	std::sync::Arc::new(crate::Options {
		path,
		level_count: 2,
		level0_max_files: 2,
		..Default::default()
	})
}

impl Harness {
	fn new() -> Self {
		let temp = TempDir::new("branch-proptest").unwrap();
		let path = temp.path().to_path_buf();
		let store = Tree::new(options(path.clone())).unwrap();
		Self {
			store,
			_temp: temp,
			path,
			names: vec!["main".to_string()],
		}
	}

	fn name(&self, index: usize) -> &str {
		&self.names[index]
	}
}

/// What a history exercised, beyond agreeing with the specification.
///
/// Returned rather than accumulated in a process-global counter, which is what
/// this was until 2026-08-15. `run_history` is shared by the seeded test below
/// and by the 48 generated cases, and every caller incremented the same static —
/// so the seeded test's non-vacuity assertion could be satisfied by somebody
/// else's history, and the guard written to stop that test silently ceasing to
/// test the retention promise was itself disarmed.
#[derive(Debug, Clone, Copy)]
struct HistoryOutcome {
	/// A compaction retained at least one version solely because a live fork
	/// anchor still needed it — the retention promise actually engaging, rather
	/// than the history merely running.
	pin_exercised: bool,
}

/// Runs one generated history against the engine and the model, checking every
/// live branch's view of every key after every step.
async fn run_history(ops: Vec<Op>) -> std::result::Result<HistoryOutcome, TestCaseError> {
	let mut harness = Harness::new();
	let mut model = VisibilityModel::new();

	for op in ops {
		match op {
			Op::Fork {
				parent,
				child,
			} => {
				if !model.exists(parent) || model.exists(child) || child >= 5 {
					continue;
				}
				let name = format!("b{child}");
				harness
					.store
					.fork_branch(harness.name(parent), &name, ForkPoint::Head)
					.map_err(|error| TestCaseError::fail(format!("fork failed: {error}")))?;
				while harness.names.len() <= child {
					harness.names.push(String::new());
				}
				harness.names[child] = name;
				model.fork(parent, child);
			}
			Op::Write {
				branch,
				key,
				value,
			} => {
				if !model.exists(branch) {
					continue;
				}
				let mut txn = harness.store.begin_on(harness.name(branch)).unwrap();
				txn.set(KEYS[key], VALUES[value]).unwrap();
				txn.commit()
					.await
					.map_err(|error| TestCaseError::fail(format!("write failed: {error}")))?;
				model.write(branch, KEYS[key], Some(VALUES[value].to_vec()));
			}
			Op::Delete {
				branch,
				key,
			} => {
				if !model.exists(branch) {
					continue;
				}
				let mut txn = harness.store.begin_on(harness.name(branch)).unwrap();
				txn.delete(KEYS[key]).unwrap();
				txn.commit()
					.await
					.map_err(|error| TestCaseError::fail(format!("delete failed: {error}")))?;
				model.write(branch, KEYS[key], None);
			}
			Op::Merge {
				source,
			} => {
				if !model.exists(source) {
					continue;
				}
				let Some((parent, _)) = model.branches[source].as_ref().unwrap().parent else {
					continue;
				};
				if !model.exists(parent) {
					continue;
				}
				let source_handle = harness.store.branch(harness.name(source)).unwrap();
				let target_handle = harness.store.branch(harness.name(parent)).unwrap();
				// What it will write, taken before it writes, so the model can
				// mirror the engine's decisions without re-deriving them.
				let report = source_handle.preview_merge_into(&target_handle).unwrap();
				let mut applied: Vec<(Vec<u8>, Option<Vec<u8>>)> = report
					.applies
					.iter()
					.map(|entry| (entry.key.clone(), entry.op.value().cloned()))
					.collect();
				applied.extend(report.conflicts.iter().map(|c| (c.key.clone(), c.source.clone())));
				source_handle
					.merge_into(&target_handle, MergeStrategy::SourceWins)
					.await
					.map_err(|error| TestCaseError::fail(format!("merge failed: {error}")))?;
				model.apply_merge(parent, applied);
			}
			Op::Flush => {
				harness.store.drain_flushes_synchronously().unwrap();
			}
			Op::Compact => {
				let strategy =
					std::sync::Arc::new(crate::compaction::leveled::Strategy::from_options(
						std::sync::Arc::clone(&harness.store.core.inner.opts),
					));
				match harness.store.compact(strategy) {
					Ok(())
					| Err(crate::Error::CompactionPinRaced {
						..
					}) => {}
					Err(error) => {
						return Err(TestCaseError::fail(format!("compaction failed: {error}")))
					}
				}
			}
			Op::Reopen => {
				harness.store.close().await.unwrap();
				harness.store = Tree::new(options(harness.path.clone())).unwrap();
			}
			Op::Churn {
				key,
			} => {
				for round in 0..3usize {
					let mut txn = harness.store.begin().unwrap();
					txn.set(KEYS[key], VALUES[round % VALUES.len()]).unwrap();
					txn.commit()
						.await
						.map_err(|error| TestCaseError::fail(format!("churn failed: {error}")))?;
					model.write(0, KEYS[key], Some(VALUES[round % VALUES.len()].to_vec()));
					harness.store.drain_flushes_synchronously().unwrap();
				}
			}
		}

		// The invariant, after every single step: every live branch agrees with
		// the specification about every key.
		for index in 0..model.branches.len() {
			if !model.exists(index) {
				continue;
			}
			let txn = harness.store.begin_on(harness.name(index)).unwrap();
			for key in KEYS {
				let actual = txn.get(key).unwrap();
				let expected = model.get(index, key);
				if actual != expected {
					return Err(TestCaseError::fail(format!(
						"branch {} key {:?}: engine says {:?}, the specification says {:?}",
						harness.name(index),
						String::from_utf8_lossy(key),
						actual.as_deref().map(String::from_utf8_lossy),
						expected.as_deref().map(String::from_utf8_lossy),
					)));
				}
			}
		}
	}

	let pinned = harness.store.metrics().unwrap().pin_retained_versions_total;
	harness.store.close().await.unwrap();
	Ok(HistoryOutcome {
		pin_exercised: pinned > 0,
	})
}

proptest! {
	#![proptest_config(ProptestConfig {
		cases: 48,
		max_shrink_iters: 2048,
		..ProptestConfig::default()
	})]

	/// Every live branch's view of every key matches the specification, after
	/// every operation of a generated history — including across compaction and
	/// reopen, which must not move a single visible value.
	#[test]
	fn generated_histories_agree_with_the_visibility_specification(
		ops in prop::collection::vec(op_strategy(), 1..24)
	) {
		let runtime = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.unwrap();
		runtime.block_on(run_history(ops))?;
	}
}

/// A seeded history that reaches the retention pin, kept because the random
/// generator does not reliably reach it.
///
/// This is the FK6 defect expressed against the specification rather than
/// against a fixture. Re-planting that defect — collapsing `retention_anchors`
/// to its single lowest anchor, as before FK6 — makes it fail with
/// `branch b2 key "a": engine says Some("1"), the specification says Some("2")`,
/// which is the bug in one line.
///
/// **Two children at different anchors is the whole point.** With one child the
/// lowest anchor IS the only anchor, so a single point pin is accidentally
/// correct — the first version of this test had one child, exercised the pin,
/// and still passed with the defect planted.
#[test]
fn a_seeded_history_reaches_the_retention_pin_and_agrees_with_the_specification() {
	let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
	let outcome = runtime.block_on(async {
		// Two children at DIFFERENT anchors is the whole point: with one child
		// the lowest anchor IS the only anchor, so a single point pin is
		// accidentally correct and proves nothing.
		let ops = vec![
			Op::Write {
				branch: 0,
				key: 0,
				value: 0,
			},
			Op::Flush,
			Op::Fork {
				parent: 0,
				child: 1,
			},
			Op::Write {
				branch: 0,
				key: 0,
				value: 1,
			},
			Op::Flush,
			Op::Fork {
				parent: 0,
				child: 2,
			},
			Op::Churn {
				key: 0,
			},
			Op::Compact,
		];
		run_history(ops).await.unwrap()
	});
	assert!(
		outcome.pin_exercised,
		"THIS history must make a compaction retain a version for a fork anchor. It no longer \
		 does, so it is no longer testing the retention promise, whatever the other cases did"
	);
}

#[cfg(test)]
mod model_tests {
	use super::*;

	/// The model itself, checked by hand — a specification nobody has verified
	/// is just a second opinion.
	#[test]
	fn the_specification_caps_an_inherited_view_at_the_fork_anchor() {
		let mut model = VisibilityModel::new();
		model.write(0, b"k", Some(b"before".to_vec()));
		model.fork(0, 1);
		model.write(0, b"k", Some(b"after".to_vec()));

		assert_eq!(model.get(0, b"k"), Some(b"after".to_vec()), "the parent sees its own write");
		assert_eq!(
			model.get(1, b"k"),
			Some(b"before".to_vec()),
			"the child is capped at its anchor"
		);

		model.write(1, b"k", Some(b"mine".to_vec()));
		assert_eq!(model.get(1, b"k"), Some(b"mine".to_vec()), "its own write shadows");
		assert_eq!(model.get(0, b"k"), Some(b"after".to_vec()), "and does not leak upward");
	}

	/// Two children at different anchors each see their own — the exact shape of
	/// the FK6 defect, expressed as a specification rather than as a fixture.
	#[test]
	fn the_specification_gives_each_anchor_its_own_answer() {
		let mut model = VisibilityModel::new();
		model.write(0, b"k", Some(b"v1".to_vec()));
		model.fork(0, 1);
		model.write(0, b"k", Some(b"v2".to_vec()));
		model.fork(0, 2);
		model.write(0, b"k", Some(b"v3".to_vec()));

		assert_eq!(model.get(1, b"k"), Some(b"v1".to_vec()));
		assert_eq!(model.get(2, b"k"), Some(b"v2".to_vec()));
		assert_eq!(model.get(0, b"k"), Some(b"v3".to_vec()));
	}

	/// A chain narrows monotonically: a grandchild can never see more of its
	/// grandparent than its parent could.
	#[test]
	fn the_specification_narrows_a_chain_monotonically() {
		let mut model = VisibilityModel::new();
		model.write(0, b"k", Some(b"v1".to_vec()));
		model.fork(0, 1);
		model.write(0, b"k", Some(b"v2".to_vec()));
		model.fork(1, 2);

		assert_eq!(model.get(2, b"k"), Some(b"v1".to_vec()), "capped by the shallower anchor");
	}

	/// A delete is a value, and it shadows an inherited row.
	#[test]
	fn the_specification_treats_absence_as_a_value() {
		let mut model = VisibilityModel::new();
		model.write(0, b"k", Some(b"v".to_vec()));
		model.fork(0, 1);
		model.write(1, b"k", None);

		assert_eq!(model.get(1, b"k"), None, "the child deleted what it inherited");
		assert_eq!(model.get(0, b"k"), Some(b"v".to_vec()), "the parent still has it");
	}
}
