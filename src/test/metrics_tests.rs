//! PE1 gate tests: every counter has an arm that moves it and an arm where it
//! must not move.
//!
//! A counter that only ever goes up under a test that only ever does the thing
//! it counts proves nothing about attribution — the twin is what shows the
//! number means the event it is named after and not "something happened".

use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::compaction::leveled::Strategy;
use crate::lsm::Tree;
use crate::{BranchMetricsSnapshot, ForkPoint, MergeStrategy, TreeBuilder};

fn create_store() -> (Tree, TempDir) {
	let temp_dir = TempDir::new("metrics").unwrap();
	let path = temp_dir.path().to_path_buf();
	let tree = TreeBuilder::new().with_path(path).with_level_count(2).build().unwrap();
	(tree, temp_dir)
}

fn metrics(store: &Tree) -> BranchMetricsSnapshot {
	store.metrics().unwrap()
}

fn child_owner(handle: &crate::BranchHandle) -> crate::batch::BatchOwner {
	crate::batch::BatchOwner {
		branch: handle.id(),
		generation: handle.generation(),
	}
}

/// Forking moves the fork counters; writing does not.
#[test(tokio::test)]
async fn forks_are_counted_and_the_fence_hold_is_timed() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();

	let before = metrics(&store);
	assert_eq!(before.forks, 0);
	assert_eq!(before.fork_drain_nanos, 0);

	store.fork_branch("main", "one", ForkPoint::Head).unwrap();
	store.fork_branch("main", "two", ForkPoint::Head).unwrap();
	let after = metrics(&store);
	assert_eq!(after.forks, 2);
	assert!(after.fork_drain_nanos > 0, "the fence was held for a measurable time");

	// The twin: ordinary writes are not forks.
	let mut txn = store.begin().unwrap();
	txn.set(b"k2", b"v").unwrap();
	txn.commit().await.unwrap();
	assert_eq!(metrics(&store).forks, 2, "a commit must not look like a fork");

	// And a fork retry is idempotent (FK4): it returns the original branch
	// without publishing anything, so it is not a second fork.
	let retry = store.fork_branch("main", "two", ForkPoint::Head).unwrap();
	assert_eq!(retry.name(), "two");
	assert_eq!(metrics(&store).forks, 2, "an idempotent retry must not count twice");

	// A fork that is refused outright counts nothing either.
	store
		.fork_branch("main", "impossible", ForkPoint::AtVersion(u64::MAX))
		.expect_err("a fork above the head is refused");
	assert_eq!(metrics(&store).forks, 2);
}

/// Detach is counted only when it detaches something.
#[test(tokio::test)]
async fn detaches_are_counted_only_when_a_parent_link_is_cleared() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();
	store.flush().unwrap();
	store.fork_branch("main", "child", ForkPoint::Head).unwrap();
	assert_eq!(metrics(&store).detaches, 0);

	store.detach_branch("child").unwrap();
	assert_eq!(metrics(&store).detaches, 1);

	// The twin: a branch with no parent has nothing to detach, and detaching it
	// again is a no-op rather than an event.
	store.detach_branch("child").unwrap();
	store.detach_branch("main").unwrap();
	assert_eq!(metrics(&store).detaches, 1, "a no-op detach must not be counted");
}

/// Merges are counted, and the ones that could not be atomic are counted
/// separately.
#[test(tokio::test)]
async fn merges_are_counted_and_chunked_ones_are_distinguished() {
	let temp_dir = TempDir::new("metrics").unwrap();
	let store = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_max_memtable_size(4 * 1024)
		.build()
		.unwrap();

	let mut txn = store.begin().unwrap();
	txn.set(b"anchor", b"base").unwrap();
	txn.commit().await.unwrap();
	let main = store.branch("main").unwrap();

	// Small: one chunk.
	let small = store.fork_branch("main", "small", ForkPoint::Head).unwrap();
	let mut txn = small.begin().unwrap();
	txn.set(b"a", b"v").unwrap();
	txn.commit().await.unwrap();
	small.merge_into(&main, MergeStrategy::Strict).await.unwrap();
	let after_small = metrics(&store);
	assert_eq!(after_small.merges, 1);
	assert_eq!(after_small.chunked_merges, 0, "a one-chunk merge is not a chunked merge");

	// Large: several chunks.
	let big = store.fork_branch("main", "big", ForkPoint::Head).unwrap();
	let payload = vec![b'x'; 512];
	for round in 0..40u32 {
		let mut txn = big.begin().unwrap();
		txn.set(format!("k{round:04}").as_bytes(), payload.as_slice()).unwrap();
		txn.commit().await.unwrap();
	}
	let outcome = big.merge_into(&main, MergeStrategy::Strict).await.unwrap();
	assert!(outcome.chunks > 1, "the fixture must actually chunk");
	let after_big = metrics(&store);
	assert_eq!(after_big.merges, 2);
	assert_eq!(after_big.chunked_merges, 1);

	// The twin: a merge with nothing to offer still completes, and is still a
	// merge, but writes no chunks.
	let outcome = big.merge_into(&main, MergeStrategy::Strict).await.unwrap();
	assert_eq!(outcome.chunks, 0);
	let after_empty = metrics(&store);
	assert_eq!(after_empty.merges, 3, "an empty merge is still a merge");
	assert_eq!(after_empty.chunked_merges, 1, "but it chunked nothing");
}

/// Deleting a branch tombstones it; the sweep is what reclaims it, and the
/// counter follows the sweep.
#[test(tokio::test)]
async fn reclaimed_branches_are_counted_when_the_sweep_releases_them() {
	let (store, _temp) = create_store();
	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();

	let child = store.fork_branch("main", "doomed", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"own", b"v").unwrap();
	txn.commit().await.unwrap();
	// `Tree::flush` only rotates the default runtime, so the child's own
	// memtable has to be pushed to an SST explicitly — without a table on disk
	// there is nothing for the sweep to unlink.
	let runtime = store.core.inner.runtimes.get(child_owner(&child)).unwrap();
	store.core.inner.rotate_runtime_memtable(&runtime, 0).unwrap();
	while store.core.inner.flush_oldest_immutable_for_test().unwrap().is_some() {}

	store.delete_branch("doomed").unwrap();
	let tombstoned = metrics(&store);
	assert_eq!(tombstoned.branches_reclaimed, 0, "deleting tombstones the entry, nothing more");
	assert_eq!(tombstoned.tables_reclaimed, 0);

	store.core.inner.sweep_branch_maintenance().unwrap();
	let swept = metrics(&store);
	assert_eq!(swept.branches_reclaimed, 1);
	assert_eq!(swept.tables_reclaimed, 1, "the branch had flushed one table of its own");

	// The twin: sweeping again has nothing left to release.
	store.core.inner.sweep_branch_maintenance().unwrap();
	let again = metrics(&store);
	assert_eq!(again.branches_reclaimed, 1, "a second sweep releases nothing");
	assert_eq!(again.tables_reclaimed, 1);

	// The other twin: a branch that wrote but never flushed is reclaimed — its
	// memtable and WAL dependency are real resources — yet frees no file. That
	// is why these are two numbers and not one.
	let unflushed = store.fork_branch("main", "never-flushed", ForkPoint::Head).unwrap();
	let mut txn = unflushed.begin().unwrap();
	txn.set(b"in-memory-only", b"v").unwrap();
	txn.commit().await.unwrap();
	store.delete_branch("never-flushed").unwrap();
	store.core.inner.sweep_branch_maintenance().unwrap();
	let empty = metrics(&store);
	assert_eq!(empty.branches_reclaimed, 2);
	assert_eq!(empty.tables_reclaimed, 1, "it had nothing on disk to free");

	// And a branch that never wrote at all holds nothing, so releasing it
	// releases nothing and is not counted.
	store.fork_branch("main", "untouched", ForkPoint::Head).unwrap();
	store.delete_branch("untouched").unwrap();
	store.core.inner.sweep_branch_maintenance().unwrap();
	assert_eq!(
		metrics(&store).branches_reclaimed,
		2,
		"the counter measures resources released, not tombstones walked"
	);
}

/// The versions compaction keeps alive for an anchor — the running price of a
/// live fork — are counted, and only when an anchor is what keeps them.
#[test(tokio::test)]
async fn versions_kept_for_an_anchor_are_counted() {
	let run = |fork: bool| async move {
		let (store, _temp) = create_store();
		let mut txn = store.begin().unwrap();
		txn.set(b"k", b"v1").unwrap();
		txn.commit().await.unwrap();
		store.flush().unwrap();

		if fork {
			store.fork_branch("main", "reader", ForkPoint::Head).unwrap();
		}

		for round in 0..5u32 {
			let mut txn = store.begin().unwrap();
			txn.set(b"k", format!("v{round}").as_bytes()).unwrap();
			txn.commit().await.unwrap();
			store.flush().unwrap();
		}
		assert!(store.core.inner.snapshot_tracker.get_all_snapshots().is_empty());
		let strategy = Arc::new(Strategy::from_options(Arc::clone(&store.core.inner.opts)));
		store.compact(strategy).unwrap();
		metrics(&store).pin_retained_versions
	};

	assert!(run(true).await > 0, "the fork's anchor must have kept a version alive");
	assert_eq!(
		run(false).await,
		0,
		"with nothing forked the same compaction keeps nothing for an anchor"
	);
}

/// The gauges are read from the state that owns them, so they track it without
/// anyone having to remember to update them.
#[test(tokio::test)]
async fn gauges_follow_the_state_they_are_derived_from() {
	let (store, _temp) = create_store();

	let empty = metrics(&store);
	assert_eq!(empty.live_branches, 1, "main exists before anything else does");
	assert_eq!(empty.timeline_horizon, None, "no commit, no horizon");

	let mut txn = store.begin().unwrap();
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();
	store.fork_branch("main", "a", ForkPoint::Head).unwrap();
	store.fork_branch("main", "b", ForkPoint::Head).unwrap();

	let after = metrics(&store);
	assert_eq!(after.live_branches, 3);
	assert_eq!(
		after.live_branches as usize,
		store.list_branches().unwrap().len(),
		"the gauge must agree with the catalog it is read from"
	);
	let (floor, latest) = after.timeline_horizon.expect("a commit puts the horizon somewhere");
	assert!(floor <= latest);
	assert!(
		after.wal_pinned_segments > 0,
		"an unflushed commit pins the segment it was written to"
	);

	// Deleting is enough to move the branch gauge: it reads live records, not
	// reclaimed ones.
	store.delete_branch("a").unwrap();
	assert_eq!(metrics(&store).live_branches, 2);

	// And flushing releases the WAL dependency the commit held.
	store.flush().unwrap();
	assert_eq!(
		metrics(&store).wal_pinned_segments,
		0,
		"a flushed memtable no longer depends on its segment"
	);
}
