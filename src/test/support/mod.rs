//! Shared test support.
//!
//! A helper redefined in three files is three helpers that can disagree. This
//! module is where the shapes every branch test needs live, so there is one
//! definition to read and one to change.
//!
//! Two rules for what belongs here:
//!
//! 1. **A name means one thing.** If two files want the same word for different questions, that is
//!    two names, not one helper with a footnote.
//! 2. **Prefer the public API.** A helper that reaches `store.core.inner` makes every test using it
//!    depend on an internal field name. Where internals are genuinely needed, the reach is confined
//!    here rather than spread across the test files.

use std::path::Path;
use std::sync::Arc;

use tempdir::TempDir;

use crate::vfs::File;
use crate::Tree;

/// A temporary directory for a store, removed when the returned handle drops.
///
/// Every test file used to define its own; they were byte-identical apart from
/// the prefix, which nothing reads. Hold the returned value for as long as the
/// store lives — dropping it deletes the directory out from under an open
/// store.
pub(crate) fn create_temp_directory() -> TempDir {
	TempDir::new("surrealkv-test").unwrap()
}

/// Presents an in-memory buffer as a [`File`], for tests that build an SSTable
/// without touching a disk.
///
/// `Vec<u8>` implements [`File`] already; this exists so the coercion to
/// `Arc<dyn File>` reads as an intent rather than as a turbofish at every call
/// site.
pub(crate) fn wrap_buffer(src: Vec<u8>) -> Arc<dyn File> {
	Arc::new(src)
}

/// A store on a fresh temporary directory, with default options.
///
/// The `TempDir` comes back with it and must be held for the store's lifetime:
/// dropping it removes the directory the store is writing into.
pub(crate) fn create_store() -> (Tree, TempDir) {
	create_store_with(|builder| builder)
}

/// A store whose builder the caller adjusts first.
///
/// The closure takes and returns the [`TreeBuilder`] rather than an `Options`,
/// so a test configures a store the same way a user would.
pub(crate) fn create_store_with<F>(configure: F) -> (Tree, TempDir)
where
	F: FnOnce(crate::TreeBuilder) -> crate::TreeBuilder,
{
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();
	let tree = configure(crate::TreeBuilder::new().with_path(path)).build().unwrap();
	(tree, temp_dir)
}

/// Flushes one branch's own memtable to an SSTable, and asserts a table
/// actually appeared.
///
/// `Tree::drain_flushes_synchronously` rotates only the **default** runtime's
/// active memtable (`CoreInner::rotate_memtable` →
/// `rotate_runtime_memtable(&default_runtime)`), so a non-default branch's
/// writes stay in its own memtable and a test that drains and then believes it
/// is reading tables is reading the memtable. That vacuity is the reason this
/// asserts rather than just flushes.
///
/// This is the one place a test reaches `core.inner` for the runtime registry,
/// so the reach is confined here instead of repeated per file.
pub(crate) fn flush_branch_to_table(store: &Tree, name: &str) {
	let owner = {
		let catalog = store.core.inner.branch_catalog.read().unwrap();
		let record =
			catalog.get_by_name(name).unwrap_or_else(|_| panic!("branch {name} must exist"));
		crate::batch::BatchOwner {
			branch: record.id,
			generation: record.generation,
		}
	};
	let runtime = store.core.inner.runtimes.get(owner).expect("branch must have a runtime");
	store.core.inner.rotate_runtime_memtable(&runtime, 0).unwrap();
	store.core.inner.flush_all_immutables_sync().unwrap();
	assert!(
		owner_table_count(store, name) > 0,
		"branch {name} was flushed but produced no table; anything reading `on disk` after this \
		 would still be reading the memtable"
	);
}

/// Rotates one branch's memtable and waits for the **background worker** to
/// turn it into a table.
///
/// The async twin of [`flush_branch_to_table`], for tests about something
/// happening while a flush is in flight. Keeps the same assertion that a table
/// actually appeared — that is what caught a vacuous test in V9.
pub(crate) async fn flush_branch_and_wait(store: &Tree, name: &str) {
	let before = owner_table_count(store, name);
	rotate_branch_and_signal(store, name);
	assert!(
		wait_until(|| owner_table_count(store, name) > before).await,
		"the background worker never flushed branch {name}; it is not durable, so anything \
         asserted after this would be reading the memtable"
	);
}

/// Puts one branch's memtable in front of the background worker and returns
/// **without waiting**.
///
/// For tests that want a flush *in flight* rather than finished — racing a
/// delete, a compaction or a read against it. `Tree::flush_and_wait` will not
/// serve: it rotates only the default runtime, so a test using it while claiming
/// to race "the branch's own flush" would in fact be racing `main`'s.
pub(crate) fn rotate_branch_and_signal(store: &Tree, name: &str) {
	let owner = {
		let catalog = store.core.inner.branch_catalog.read().unwrap();
		let record =
			catalog.get_by_name(name).unwrap_or_else(|_| panic!("branch {name} must exist"));
		crate::batch::BatchOwner {
			branch: record.id,
			generation: record.generation,
		}
	};
	let runtime = store.core.inner.runtimes.get(owner).expect("branch must have a runtime");
	store.core.inner.rotate_runtime_memtable(&runtime, 0).unwrap();

	// Production's own signal; the worker drains every runtime's backlog, not
	// just the default one.
	if let Some(ref task_manager) = *store.core.task_manager.lock().unwrap() {
		task_manager.wake_up_memtable();
	}
}

// ---------------------------------------------------------------------------
// Accessors for internals tests legitimately need.
//
// Every one of these replaced a `store.core.inner.<field>` reach repeated across
// the suite. They exist so that renaming a `CoreInner` field is a small edit
// rather than a 150-site one — which matters most for `level_manifest`, the
// field the async port turns into atomic-swap versions.
//
// This is principle 4's allowed case (`docs/PATTERNS.md`): a `#[cfg(test)]`
// accessor on a production type, for a test that genuinely needs an internal.
// ---------------------------------------------------------------------------

/// The manifest's WAL log number — segments below it are reclaimable.
pub(crate) fn wal_log_number(store: &Tree) -> u64 {
	store.core.inner.level_manifest.read().unwrap().get_log_number()
}

/// The last sequence the manifest has persisted.
pub(crate) fn manifest_last_sequence(store: &Tree) -> u64 {
	store.core.inner.level_manifest.read().unwrap().get_last_sequence()
}

/// SSTables across every owner and level.
pub(crate) fn total_table_count(store: &Tree) -> usize {
	store.core.inner.level_manifest.read().unwrap().iter().count()
}

/// Live branch runtimes, including the default one.
pub(crate) fn runtime_count(store: &Tree) -> usize {
	store.core.inner.runtimes.len()
}

/// Summed arena capacity of every runtime's memtables — the write-buffer budget
/// this store is actually consuming.
pub(crate) fn total_arena_bytes(store: &Tree) -> u64 {
	store.core.inner.runtimes.total_arena_capacity_bytes()
}

/// Runs one branch-maintenance pass: expire due branches, prune lineages.
/// Returns `(expired, pruned)`.
pub(crate) fn sweep(store: &Tree) -> (usize, usize) {
	store.core.inner.sweep_branch_maintenance().unwrap()
}

/// Rotates the WAL to a fresh segment.
pub(crate) fn rotate_wal(store: &Tree) {
	store.core.inner.wal.write().rotate().unwrap();
}

/// The WAL segment currently being appended to.
pub(crate) fn active_wal_segment(store: &Tree) -> u64 {
	store.core.inner.wal.read().get_active_log_number()
}

/// Current WAL dependency state, sampled against the active segment.
pub(crate) fn wal_dependency_snapshot(
	store: &Tree,
) -> crate::wal::dependency::WalDependencySnapshot {
	let active = active_wal_segment(store);
	store.core.inner.wal_dependencies.snapshot(active)
}

/// Number of durable catalog records, including transition tombstones.
pub(crate) fn catalog_record_count(store: &Tree) -> usize {
	store.core.inner.branch_catalog.read().unwrap().all_records().count()
}

/// Last timestamp allocated by the commit timeline.
pub(crate) fn last_commit_timestamp(store: &Tree) -> u64 {
	store.core.inner.timeline.last_commit_ts()
}

/// The persisted fork sequence for one live branch.
pub(crate) fn branch_fork_sequence(store: &Tree, name: &str) -> u64 {
	store
		.core
		.inner
		.branch_catalog
		.read()
		.unwrap()
		.get_by_name(name)
		.unwrap_or_else(|_| panic!("branch {name} must exist"))
		.parent
		.as_ref()
		.unwrap_or_else(|| panic!("branch {name} must be a fork"))
		.fork_seq
}

/// The durable retained floor for one live branch owner.
pub(crate) fn branch_retained_floor(store: &Tree, name: &str) -> u64 {
	let owner = {
		let catalog = store.core.inner.branch_catalog.read().unwrap();
		let record =
			catalog.get_by_name(name).unwrap_or_else(|_| panic!("branch {name} must exist"));
		crate::batch::BatchOwner {
			branch: record.id,
			generation: record.generation,
		}
	};
	store.core.inner.level_manifest.read().unwrap().retained_floor(owner)
}

/// Runs one normal leveled compaction with the store's configured options.
pub(crate) fn compact_leveled(store: &Tree) {
	let strategy = Arc::new(crate::compaction::leveled::Strategy::from_options(Arc::clone(
		&store.core.inner.opts,
	)));
	store.compact(strategy).unwrap();
}

/// Deterministically observes whether checkpoint holds the catalog fence while
/// blocked inside its multi-file cut.
pub(crate) fn checkpoint_holds_catalog_fence(store: &Arc<Tree>, checkpoint: &Path) -> bool {
	let levels_guard = store.core.inner.level_manifest.write().unwrap();
	let checkpoint_store = Arc::clone(store);
	let checkpoint_path = checkpoint.to_path_buf();
	let worker = std::thread::spawn(move || checkpoint_store.create_checkpoint(checkpoint_path));

	let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
	while !checkpoint.exists() && std::time::Instant::now() < deadline {
		std::thread::yield_now();
	}
	assert!(checkpoint.exists(), "checkpoint worker never entered the cut");
	let catalog_is_fenced = store.core.inner.catalog_publish.try_lock().is_err();

	drop(levels_guard);
	worker.join().unwrap().unwrap();
	catalog_is_fenced
}

/// SSTables at L0 belonging to a branch.
pub(crate) fn owner_table_count(store: &Tree, name: &str) -> usize {
	let owner = {
		let catalog = store.core.inner.branch_catalog.read().unwrap();
		let Ok(record) = catalog.get_by_name(name) else {
			return 0;
		};
		crate::batch::BatchOwner {
			branch: record.id,
			generation: record.generation,
		}
	};
	store
		.core
		.inner
		.level_manifest
		.read()
		.unwrap()
		.levels_for(owner)
		.map(|levels| levels.get_levels()[0].tables.len())
		.unwrap_or(0)
}

/// How long [`wait_until`] keeps polling before giving up. Generous on purpose:
/// it is a deadlock backstop, not a timing assertion. A test that needs a
/// *short* deadline to be meaningful is measuring the machine.
const WAIT_UNTIL_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

/// How often [`wait_until`] re-checks. Short enough that a satisfied condition
/// costs nothing noticeable, long enough not to spin a core.
const WAIT_UNTIL_POLL: std::time::Duration = std::time::Duration::from_millis(1);

/// Polls `condition` until it holds, returning whether it ever did.
///
/// This is the replacement for `sleep(n)` followed by an assertion. A sleep
/// encodes a guess about how fast the machine is: too short and the test fails
/// on a loaded box, too long and it wastes wall-clock on every run. A poll
/// encodes what the test is actually waiting for, so it gets *slower* rather
/// than *wrong* under load.
///
/// It cannot be used to assert a negative — "this did not happen" needs a
/// bounded wait plus a positive control proving the machinery was running at
/// all. See `test_no_spurious_small_flush` for that shape.
pub(crate) async fn wait_until(mut condition: impl FnMut() -> bool) -> bool {
	let deadline = std::time::Instant::now() + WAIT_UNTIL_DEADLINE;
	while std::time::Instant::now() < deadline {
		if condition() {
			return true;
		}
		tokio::time::sleep(WAIT_UNTIL_POLL).await;
	}
	condition()
}

/// Whether a live branch of this name exists.
///
/// "Live" means not tombstoned: [`Tree::branch`] resolves through the catalog's
/// live-name index, which a delete removes from, so a deleted branch answers
/// `false` from the moment its tombstone is applied.
///
/// This was two functions until 2026-08-15 — one in `fault_injection_tests.rs`
/// asking `list_branches()`, one in `fork_view_tests.rs` reaching into
/// `core.inner.branch_catalog` — under one name. They happened to agree, because
/// `list()` and `get_by_name` are both driven by the same `live_names` index,
/// but nothing said so, and a test moved between the files would have changed
/// which layer it exercised without changing a line. The `list_branches` form
/// was also the worse of the two in the file that used it: it builds a
/// `BranchInfo` for *every* branch, which walks the level manifest, so in a
/// fault-injection test an unrelated failure turned a clean `false` into a
/// panic.
pub(crate) fn branch_exists(store: &Tree, name: &str) -> bool {
	store.branch(name).is_ok()
}
