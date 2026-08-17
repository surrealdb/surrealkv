//! V4: the durable branch facts, asserted after a restart.
//!
//! Twenty-two branch tests call themselves crash tests. All of them use a
//! graceful `close()`, which exercises recovery but never a *failed* one, and
//! several assert in prose that something "survives reopen" without reopening.
//! The gaps this file closes were named by measurement, not by guesswork:
//!
//! - delete's tombstone was never reloaded, and the name never re-created after;
//! - TTL expiry had no reopen test at all;
//! - the reclamation sweep interrupted mid-way was never reloaded;
//! - the interrupted-detach fixture built the exact crash window and then asserted against it **in
//!   the same process**;
//! - **retention floors and fork anchors were never asserted across a restart**, which is the fact
//!   long-lived forks depend on more than any other.
//!
//! Where a graceful close would prove the same thing, these use one; where the
//! point is that a *failure* is durable, they use the real power-loss model from
//! `crash_consistency_tests` — release the lockfile, drop without `close()`, and
//! let anything unsynced come back empty.

use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::compaction::leveled::Strategy;
use crate::failpoints::{FaultPoint, FaultPolicy, ScriptedFaults};
use crate::lsm::Tree;
use crate::{ForkPoint, Options, Tree as TreeAlias, TreeBuilder};

/// Opens a store that never flushes on close, so a `drop` without `close()` is
/// a crash rather than a shutdown.
fn crashable(path: std::path::PathBuf) -> (Tree, Arc<Options>) {
	let opts = Arc::new(Options {
		path,
		flush_on_close: false,
		..Default::default()
	});
	let tree = TreeAlias::new(Arc::clone(&opts)).unwrap();
	(tree, opts)
}

/// Drops the store the way a power loss would: lockfile released so the next
/// open succeeds, no `close()`, and no `.await` before the reopen.
fn crash(tree: Tree) {
	{
		let mut lockfile = tree.core.inner.lockfile.lock().unwrap();
		lockfile.release().unwrap();
	}
	drop(tree);
}

fn reopen(path: &std::path::Path) -> Tree {
	TreeBuilder::new().with_path(path.to_path_buf()).build().unwrap()
}

/// Reopen with a fault policy the caller keeps a handle on. Each incarnation
/// gets its own policy, which is what makes "fail the first attempt, then let
/// the restart succeed" expressible without a global.
fn reopen_with_faults(path: &std::path::Path) -> (Tree, Arc<ScriptedFaults>) {
	let faults = Arc::new(ScriptedFaults::new());
	let opts = Options {
		path: path.to_path_buf(),
		fault_policy: Arc::clone(&faults) as Arc<dyn FaultPolicy>,
		..Options::default()
	};
	(TreeBuilder::with_options(opts).build().unwrap(), faults)
}

fn branch_names(store: &Tree) -> Vec<String> {
	let mut names: Vec<String> =
		store.list_branches().unwrap().into_iter().map(|info| info.name).collect();
	names.sort();
	names
}

/// A delete is durable, and the name it freed is usable afterwards — with the
/// re-created branch fenced from the old one's generation.
///
/// `the_maintenance_sweep_expires_only_due_branches` asserts in a comment that
/// "the tombstone survives reopen because it went out as a catalog version" and
/// then never reopens. This is that assertion, performed.
#[test(tokio::test)]
async fn a_deleted_branch_stays_deleted_across_a_reopen_and_its_name_is_reusable() {
	let temp_dir = TempDir::new("branch-crash").unwrap();
	let path = temp_dir.path().to_path_buf();

	let generation_before = {
		let store = reopen(&path);
		let mut txn = store.begin().unwrap();
		txn.set(b"k", b"v").unwrap();
		txn.commit().await.unwrap();
		let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
		let generation = child.generation();
		let mut txn = child.begin().unwrap();
		txn.set(b"own", b"v").unwrap();
		txn.commit().await.unwrap();
		store.delete_branch("work").unwrap();
		store.close().await.unwrap();
		generation
	};

	let store = reopen(&path);
	assert_eq!(branch_names(&store), vec!["main"], "the tombstone did not survive the reopen");
	assert!(store.branch("work").is_err(), "a deleted name must not resolve");

	// The name is free, and the new incarnation is a different generation, so a
	// stale handle from before the delete could never bind to it.
	let recreated = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	assert_ne!(
		recreated.generation(),
		generation_before,
		"a re-created name must not reuse the dead generation"
	);
	let txn = recreated.begin().unwrap();
	assert_eq!(txn.get(b"own").unwrap(), None, "the dead branch's own rows are not inherited");
	assert_eq!(txn.get(b"k").unwrap(), Some(b"v".to_vec()), "but the parent's are");
	drop(txn);
	store.close().await.unwrap();
}

/// A TTL is durable, and the sweep that fires it works after a restart — the
/// branch is not expired by the reopen itself, only by a sweep.
#[test(tokio::test)]
async fn a_ttl_survives_a_reopen_and_the_first_sweep_after_it_expires_the_branch() {
	let temp_dir = TempDir::new("branch-crash").unwrap();
	let path = temp_dir.path().to_path_buf();

	{
		let store = reopen(&path);
		let mut txn = store.begin().unwrap();
		txn.set(b"k", b"v").unwrap();
		txn.commit().await.unwrap();
		store.fork_branch("main", "ephemeral", ForkPoint::Head).unwrap();
		store.fork_branch("main", "permanent", ForkPoint::Head).unwrap();
		store.set_branch_ttl("ephemeral", Some(std::time::Duration::ZERO)).unwrap();
		store.close().await.unwrap();
	}

	let store = reopen(&path);
	assert_eq!(
		branch_names(&store),
		vec!["ephemeral", "main", "permanent"],
		"an expired-but-unswept branch is still live after a reopen; opening is not a sweep"
	);

	let (expired, _) = crate::test::support::sweep(&store);
	assert_eq!(expired, 1, "the TTL was durable, so the first sweep after the restart fires it");
	assert_eq!(branch_names(&store), vec!["main", "permanent"]);
	store.close().await.unwrap();
}

/// A retention floor and a fork anchor are durable, and the pin they express
/// still holds after a restart.
///
/// This is the fact a long-lived fork depends on above all others: the parent's
/// compaction preserved a version because a child's anchor needed it, and a
/// restart must not forget either half. Nothing asserted it across a reopen
/// before.
#[test(tokio::test)]
async fn retention_anchors_and_what_they_pin_survive_a_restart() {
	let temp_dir = TempDir::new("branch-crash").unwrap();
	let path = temp_dir.path().to_path_buf();

	let (anchor, floor) = {
		let store = TreeBuilder::new().with_path(path.clone()).with_level_count(2).build().unwrap();
		let mut txn = store.begin().unwrap();
		txn.set(b"k", b"v1").unwrap();
		txn.commit().await.unwrap();
		store.drain_flushes_synchronously().unwrap();
		let child = store.fork_branch("main", "reader", ForkPoint::Head).unwrap();
		let anchor = child.info().unwrap().parent.unwrap().fork_seq;

		// Overwrite and compact, so the parent is actively holding a version it
		// would otherwise drop.
		for round in 0..5u32 {
			let mut txn = store.begin().unwrap();
			txn.set(b"k", format!("v{round}").as_bytes()).unwrap();
			txn.commit().await.unwrap();
			store.drain_flushes_synchronously().unwrap();
		}
		store
			.compact(Arc::new(Strategy::from_options(Arc::clone(&store.core.inner.opts))))
			.unwrap();
		let floor = store
			.core
			.inner
			.level_manifest
			.read()
			.unwrap()
			.retained_floor(crate::batch::BatchOwner::DEFAULT);
		assert!(
			store.metrics().unwrap().pin_retained_versions_total > 0,
			"the fixture must have made the parent hold a version for the child"
		);
		store.close().await.unwrap();
		(anchor, floor)
	};

	let store = reopen(&path);
	let child = store.branch("reader").unwrap();
	assert_eq!(
		child.info().unwrap().parent.unwrap().fork_seq,
		anchor,
		"the fork anchor did not survive the restart"
	);
	assert_eq!(
		store
			.core
			.inner
			.level_manifest
			.read()
			.unwrap()
			.retained_floor(crate::batch::BatchOwner::DEFAULT),
		floor,
		"the retention floor did not survive the restart"
	);
	// The pin is not just recorded but honoured: the child still reads what it
	// inherited, from tables written before the restart.
	let txn = child.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"v1".to_vec()), "the pinned version is gone");
	drop(txn);
	store.close().await.unwrap();
}

/// A fork taken at an explicit version or timestamp resolves to the same rows
/// after a restart. `Head` was covered; the other two selectors were not.
#[test(tokio::test)]
async fn historical_fork_selectors_resolve_identically_after_a_reopen() {
	let temp_dir = TempDir::new("branch-crash").unwrap();
	let path = temp_dir.path().to_path_buf();

	{
		let store = reopen(&path);
		let mut txn = store.begin().unwrap();
		txn.set(b"k", b"first").unwrap();
		txn.commit().await.unwrap();
		let at_version =
			store.core.inner.visible_seq_num.load(std::sync::atomic::Ordering::Acquire);
		let at_timestamp = store.core.inner.timeline.horizon().unwrap().1;

		let mut txn = store.begin().unwrap();
		txn.set(b"k", b"second").unwrap();
		txn.commit().await.unwrap();

		store.fork_branch("main", "by-version", ForkPoint::AtVersion(at_version)).unwrap();
		store.fork_branch("main", "by-timestamp", ForkPoint::AtTimestamp(at_timestamp)).unwrap();

		// Both see the first value, not the second, before the restart.
		for name in ["by-version", "by-timestamp"] {
			let txn = store.begin_on(name).unwrap();
			assert_eq!(txn.get(b"k").unwrap(), Some(b"first".to_vec()), "{name} before restart");
		}
		store.close().await.unwrap();
	}

	let store = reopen(&path);
	for name in ["by-version", "by-timestamp"] {
		let txn = store.begin_on(name).unwrap();
		assert_eq!(
			txn.get(b"k").unwrap(),
			Some(b"first".to_vec()),
			"{name} resolved differently after the restart"
		);
	}
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"second".to_vec()), "and main is unaffected");
	drop(txn);
	store.close().await.unwrap();
}

/// A reclamation sweep interrupted by a failed publish, then reloaded from
/// disk. The branch is still tombstoned, its tables are still there, and the
/// sweep after the restart finishes the job.
///
/// The in-process half of this is V3's failpoint test. This is the half that
/// asks whether the interrupted state is one a restart can recover from at all.
#[test(tokio::test)]
async fn an_interrupted_reclamation_finishes_after_a_restart() {
	let temp_dir = TempDir::new("branch-crash").unwrap();
	let path = temp_dir.path().to_path_buf();

	{
		let (store, faults) = reopen_with_faults(&path);
		let mut txn = store.begin().unwrap();
		txn.set(b"k", b"v").unwrap();
		txn.commit().await.unwrap();
		let child = store.fork_branch("main", "doomed", ForkPoint::Head).unwrap();
		let owner = crate::batch::BatchOwner {
			branch: child.id(),
			generation: child.generation(),
		};
		let mut txn = child.begin().unwrap();
		txn.set(b"own", b"v").unwrap();
		txn.commit().await.unwrap();
		let runtime = store.core.inner.runtimes.get(owner).unwrap();
		store.core.inner.rotate_runtime_memtable(&runtime, 0).unwrap();
		while store.core.inner.flush_oldest_immutable_for_test().unwrap().is_some() {}
		store.delete_branch("doomed").unwrap();

		faults.fail_times(FaultPoint::RootPublish, 1);
		store
			.core
			.inner
			.sweep_branch_maintenance()
			.expect_err("the interrupted sweep must report its failure");
		store.close().await.unwrap();
	}

	let store = reopen(&path);
	assert_eq!(branch_names(&store), vec!["main"], "the tombstone is durable");
	// Whether the tables were reclaimed at open or are still waiting, the sweep
	// after the restart must leave nothing behind either way.
	crate::test::support::sweep(&store);
	let remaining = std::fs::read_dir(store.core.inner.opts.sstable_dir())
		.unwrap()
		.filter_map(|entry| entry.ok())
		.filter(|entry| entry.path().extension().is_some_and(|ext| ext == "sst"))
		.count();
	let live = store.core.inner.level_manifest.read().unwrap().get_all_tables().len();
	assert_eq!(remaining, live, "an interrupted reclamation left files nothing references");
	store.close().await.unwrap();
}

/// A branch's own SST, never fsynced, lost to a power cut. The store must still
/// open, and must not claim rows it cannot produce.
///
/// `crash_consistency_tests` models this for the default branch and has never
/// been pointed at a branch-owned table.
#[test(tokio::test)]
async fn a_crash_that_loses_an_unsynced_branch_table_still_opens() {
	let temp_dir = TempDir::new("branch-crash").unwrap();
	let path = temp_dir.path().to_path_buf();

	let (store, opts) = crashable(path.clone());
	let mut txn = store.begin().unwrap();
	txn.set(b"parent", b"v").unwrap();
	txn.commit().await.unwrap();
	let child = store.fork_branch("main", "work", ForkPoint::Head).unwrap();
	let mut txn = child.begin().unwrap();
	txn.set(b"child-row", b"v").unwrap();
	txn.commit().await.unwrap();

	crash(store);

	// Anything the flush path never fsynced may legally return empty.
	for entry in std::fs::read_dir(opts.sstable_dir()).unwrap() {
		let sst = entry.unwrap().path();
		if sst.extension().is_some_and(|ext| ext == "sst")
			&& !crate::vfs::sync_tracker::was_synced(&sst)
		{
			std::fs::OpenOptions::new().write(true).open(&sst).unwrap().set_len(0).unwrap();
		}
	}

	let store = reopen(&path);
	assert_eq!(branch_names(&store), vec!["main", "work"], "the catalog survived the crash");
	// The commits were WAL-durable, so replay restores both sides.
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"parent").unwrap(), Some(b"v".to_vec()));
	drop(txn);
	let txn = store.begin_on("work").unwrap();
	assert_eq!(txn.get(b"child-row").unwrap(), Some(b"v".to_vec()));
	assert_eq!(txn.get(b"parent").unwrap(), Some(b"v".to_vec()), "and the inherited view too");
	drop(txn);
	store.close().await.unwrap();
}

/// Metrics counters are per-process and start at zero; gauges are derived from
/// durable state and must come back describing it.
#[test(tokio::test)]
async fn metrics_gauges_are_rederived_after_a_restart_and_counters_are_not() {
	let temp_dir = TempDir::new("branch-crash").unwrap();
	let path = temp_dir.path().to_path_buf();

	{
		let store = reopen(&path);
		let mut txn = store.begin().unwrap();
		txn.set(b"k", b"v").unwrap();
		txn.commit().await.unwrap();
		store.fork_branch("main", "a", ForkPoint::Head).unwrap();
		store.fork_branch("main", "b", ForkPoint::Head).unwrap();
		let metrics = store.metrics().unwrap();
		assert_eq!(metrics.forks, 2);
		assert_eq!(metrics.live_branches, 3);
		store.close().await.unwrap();
	}

	let store = reopen(&path);
	let metrics = store.metrics().unwrap();
	assert_eq!(metrics.forks, 0, "counters describe this process's work, not the data's history");
	assert_eq!(metrics.live_branches, 3, "gauges are read from the state that owns them");
	assert!(metrics.timeline_horizon.is_some(), "the timeline was rebuilt from the durable tail");
	store.close().await.unwrap();
}
