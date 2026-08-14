//! BR4 gate tests: lazy owner-indexed runtimes and routed writes.

use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::batch::BatchOwner;
use crate::lsm::Tree;
use crate::transaction::{Transaction, TransactionOptions};
use crate::{BranchId, CommitVersion, LSMIterator, TreeBuilder};

fn create_store_with<F>(configure: F) -> (Tree, TempDir)
where
	F: FnOnce(TreeBuilder) -> TreeBuilder,
{
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();
	let tree = configure(TreeBuilder::new().with_path(path)).build().unwrap();
	(tree, temp_dir)
}

/// Registers `branch` in the catalog and returns its physical owner.
fn register_branch(store: &Tree, id: u128, name: &str) -> BatchOwner {
	let branch = BranchId::from_u128(id);
	let record =
		store.core.branch_catalog.write().unwrap().create(branch, name, CommitVersion(0)).unwrap();
	BatchOwner {
		branch,
		generation: record.generation,
	}
}

fn begin_owned(store: &Tree, owner: BatchOwner) -> Transaction {
	Transaction::new_owned(Arc::clone(&store.core), TransactionOptions::write_only(), owner)
		.unwrap()
}

/// An idle branch is a catalog record only: no runtime, no arena. The runtime
/// (and its first arena) appears on the owner's first routed write — the
/// non-vacuity arm proving the counter genuinely moves on writes.
#[test(tokio::test)]
async fn idle_branches_allocate_no_runtime_or_arena() {
	let (store, _temp_dir) = create_store_with(|b| b);

	let mut owners = Vec::new();
	for i in 0..1_000u128 {
		owners.push(register_branch(&store, 1_000 + i, &format!("idle/{i}")));
	}

	assert_eq!(
		store.core.inner.runtimes.len(),
		1,
		"1,000 idle branches must not create runtimes beyond the default"
	);
	let default_only = store.core.inner.runtimes.total_arena_capacity_bytes();

	// Non-vacuity: the first routed write creates exactly one runtime with
	// one small arena.
	let mut txn = begin_owned(&store, owners[0]);
	txn.set(b"k", b"v").unwrap();
	txn.commit().await.unwrap();

	assert_eq!(store.core.inner.runtimes.len(), 2, "first write must create the owner's runtime");
	let with_one_branch = store.core.inner.runtimes.total_arena_capacity_bytes();
	assert_eq!(
		with_one_branch - default_only,
		store.core.inner.opts.branch_memtable_size as u64,
		"a branch runtime allocates exactly the configured branch arena"
	);

	store.close().await.unwrap();
}

/// Same user keys written under two owners stay physically isolated through
/// interleaved writes and independent memtable rotations. Default reads never
/// observe the foreign branch's values, and the foreign runtime holds its own.
#[test(tokio::test)]
async fn same_keys_in_two_branches_stay_isolated_through_rotations() {
	// Three direct rotations queue three immutables per runtime without ever
	// scheduling the flush task; the default stall threshold (2) would park
	// round-two commits waiting for a flush this fixture never runs.
	let (store, _temp_dir) = create_store_with(|b| b.with_memtable_stall_threshold(8));
	let foreign = register_branch(&store, 42, "agent/sandbox");

	for round in 0u32..3 {
		let mut txn = store.begin().unwrap();
		txn.set(b"shared-key", format!("default-{round}").as_bytes()).unwrap();
		txn.commit().await.unwrap();

		let mut txn = begin_owned(&store, foreign);
		txn.set(b"shared-key", format!("foreign-{round}").as_bytes()).unwrap();
		txn.commit().await.unwrap();

		// Independent rotations: each runtime rotates without touching the
		// other or the shared WAL.
		let foreign_runtime = store.core.inner.runtimes.get(foreign).unwrap();
		store.core.inner.rotate_runtime_memtable(&foreign_runtime, 0).unwrap();
		store.core.inner.rotate_memtable().unwrap();
	}

	// Default reads resolve only default-owned components.
	let txn = store.begin().unwrap();
	assert_eq!(
		txn.get(b"shared-key").unwrap(),
		Some(b"default-2".to_vec()),
		"default branch must see its own latest value, never the foreign one"
	);
	drop(txn);

	// The foreign runtime holds the foreign values in its own components.
	let foreign_runtime = store.core.inner.runtimes.get(foreign).unwrap();
	let in_active = foreign_runtime
		.active_memtable
		.read()
		.unwrap()
		.get(b"shared-key", None)
		.map(|(_, value)| value.starts_with(b"foreign-"))
		.unwrap_or(false);
	let in_immutables = foreign_runtime.immutable_memtables.read().unwrap().iter().any(|entry| {
		entry
			.memtable
			.get(b"shared-key", None)
			.map(|(_, value)| value.starts_with(b"foreign-"))
			.unwrap_or(false)
	});
	assert!(
		in_active || in_immutables,
		"foreign values must live in the foreign runtime's own components"
	);
	assert!(
		foreign_runtime.validate_component_owners(),
		"foreign runtime must stay owner-pure through rotations"
	);

	store.close().await.unwrap();
}

/// Sabotage twin for arena right-sizing: a batch larger than the configured
/// branch arena must still commit (the forced rotation right-sizes the
/// replacement), or a WAL-durable batch could never apply.
#[test(tokio::test)]
async fn oversized_batch_right_sizes_the_branch_arena() {
	let (store, _temp_dir) = create_store_with(|b| b.with_branch_memtable_size(4 * 1024));
	let foreign = register_branch(&store, 7, "small/arena");

	// Fill the small arena first so the oversized batch hits ArenaFull.
	let mut txn = begin_owned(&store, foreign);
	txn.set(b"warm", vec![7u8; 2 * 1024].as_slice()).unwrap();
	txn.commit().await.unwrap();

	let oversized = vec![42u8; 16 * 1024];
	assert!(
		oversized.len() > store.core.inner.opts.branch_memtable_size,
		"fixture batch must exceed the branch arena"
	);
	let mut txn = begin_owned(&store, foreign);
	txn.set(b"oversized", oversized.as_slice()).unwrap();
	txn.commit().await.unwrap();

	let foreign_runtime = store.core.inner.runtimes.get(foreign).unwrap();
	let capacity = foreign_runtime.active_memtable.read().unwrap().arena_capacity();
	assert!(
		capacity >= oversized.len(),
		"replacement arena must be right-sized to the batch: {capacity} >= {}",
		oversized.len()
	);

	store.close().await.unwrap();
}

/// The database-wide write-buffer budget rotates the largest active memtable
/// toward flush instead of letting branch arenas accumulate unbounded.
#[test(tokio::test)]
async fn write_buffer_budget_rotates_largest_victim() {
	let (store, _temp_dir) = create_store_with(|b| {
		b.with_branch_memtable_size(8 * 1024).with_write_buffer_budget(Some(16 * 1024))
	});
	let first = register_branch(&store, 11, "budget/first");
	let second = register_branch(&store, 12, "budget/second");

	// First branch becomes the largest non-empty active memtable.
	let mut txn = begin_owned(&store, first);
	txn.set(b"bulk", vec![1u8; 4 * 1024].as_slice()).unwrap();
	txn.commit().await.unwrap();

	let first_runtime = store.core.inner.runtimes.get(first).unwrap();
	assert_eq!(
		first_runtime.immutable_memtables.read().unwrap().iter().count(),
		0,
		"fixture must start with an empty flush queue"
	);

	// Creating the second runtime pushes the arena total past the budget;
	// enforcement must rotate the largest victim (the first branch) and wake
	// the flush task. The flush may therefore already have drained the queue
	// by the time we assert, so the evidence is (a) the victim's active
	// memtable was emptied and (b) the bulk row survives somewhere in its
	// flush pipeline — never dropped.
	let mut txn = begin_owned(&store, second);
	txn.set(b"trigger", b"x").unwrap();
	txn.commit().await.unwrap();

	assert!(
		first_runtime.active_memtable.read().unwrap().is_empty(),
		"budget breach must rotate the largest active memtable"
	);
	let in_queue = first_runtime
		.immutable_memtables
		.read()
		.unwrap()
		.iter()
		.any(|entry| entry.memtable.get(b"bulk", None).is_some());
	let in_owned_l0 =
		store.core.inner.level_manifest.read().unwrap().levels_for(first).is_some_and(|levels| {
			levels.get_levels().first().is_some_and(|l0| !l0.tables.is_empty())
		});
	assert!(
		in_queue || in_owned_l0,
		"rotated data must be queued for flush or already flushed, never dropped"
	);

	store.close().await.unwrap();
}

// ===== BR5: branch-selected snapshots and reads =====

fn begin_owned_rw(store: &Tree, owner: BatchOwner) -> Transaction {
	Transaction::new_owned(Arc::clone(&store.core), TransactionOptions::new(), owner).unwrap()
}

fn collect_forward(iter: &mut impl crate::LSMIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
	let mut out = Vec::new();
	let mut valid = iter.seek_first().unwrap();
	while valid {
		out.push((iter.key().user_key().to_vec(), iter.value_encoded().unwrap().to_vec()));
		valid = iter.next().unwrap();
	}
	out
}

fn collect_reverse(iter: &mut impl crate::LSMIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
	let mut out = Vec::new();
	let mut valid = iter.seek_last().unwrap();
	while valid {
		out.push((iter.key().user_key().to_vec(), iter.value_encoded().unwrap().to_vec()));
		valid = iter.prev().unwrap();
	}
	out
}

/// Point reads and tombstones are owner-scoped: each branch's read
/// transaction resolves its own value for a shared key, a branch's delete
/// hides the key only inside that branch, and keys written only in the
/// default branch never leak into a foreign read.
#[test(tokio::test)]
async fn br5_point_reads_and_tombstones_stay_owner_scoped() {
	let (store, _temp_dir) = create_store_with(|b| b);
	let foreign = register_branch(&store, 51, "read/point");

	let mut txn = store.begin().unwrap();
	txn.set(b"shared", b"default-v").unwrap();
	txn.set(b"default-only", b"d").unwrap();
	txn.commit().await.unwrap();

	let mut txn = begin_owned_rw(&store, foreign);
	txn.set(b"shared", b"foreign-v").unwrap();
	txn.commit().await.unwrap();

	let txn = begin_owned_rw(&store, foreign);
	assert_eq!(txn.get(b"shared").unwrap(), Some(b"foreign-v".to_vec()));
	assert_eq!(
		txn.get(b"default-only").unwrap(),
		None,
		"a key written only in the default branch must not leak into a foreign read"
	);
	drop(txn);

	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"shared").unwrap(), Some(b"default-v".to_vec()));
	drop(txn);

	// A foreign tombstone hides the key only inside the foreign branch.
	let mut txn = begin_owned_rw(&store, foreign);
	txn.delete(b"shared").unwrap();
	txn.commit().await.unwrap();

	let txn = begin_owned_rw(&store, foreign);
	assert_eq!(txn.get(b"shared").unwrap(), None, "foreign delete must hide the foreign value");
	drop(txn);
	let txn = store.begin().unwrap();
	assert_eq!(
		txn.get(b"shared").unwrap(),
		Some(b"default-v".to_vec()),
		"a foreign tombstone must never shadow the default branch's value"
	);
	drop(txn);

	store.close().await.unwrap();
}

/// Forward and reverse range scans resolve only the snapshot owner's
/// components, including a shared key that carries a different value in each
/// branch.
#[test(tokio::test)]
async fn br5_range_and_reverse_scans_stay_owner_scoped() {
	let (store, _temp_dir) = create_store_with(|b| b);
	let foreign = register_branch(&store, 52, "read/range");

	let mut txn = store.begin().unwrap();
	txn.set(b"a", b"d-a").unwrap();
	txn.set(b"c", b"d-c").unwrap();
	txn.set(b"e", b"d-e").unwrap();
	txn.commit().await.unwrap();

	let mut txn = begin_owned_rw(&store, foreign);
	txn.set(b"b", b"f-b").unwrap();
	txn.set(b"c", b"f-c").unwrap();
	txn.set(b"d", b"f-d").unwrap();
	txn.commit().await.unwrap();

	let txn = begin_owned_rw(&store, foreign);
	{
		let mut iter = txn.range(b"a", b"z").unwrap();
		assert_eq!(
			collect_forward(&mut iter),
			vec![
				(b"b".to_vec(), b"f-b".to_vec()),
				(b"c".to_vec(), b"f-c".to_vec()),
				(b"d".to_vec(), b"f-d".to_vec()),
			],
			"foreign forward scan must see exactly the foreign keys and values"
		);
	}
	{
		let mut iter = txn.range(b"a", b"z").unwrap();
		assert_eq!(
			collect_reverse(&mut iter),
			vec![
				(b"d".to_vec(), b"f-d".to_vec()),
				(b"c".to_vec(), b"f-c".to_vec()),
				(b"b".to_vec(), b"f-b".to_vec()),
			],
			"foreign reverse scan must see exactly the foreign keys and values"
		);
	}
	drop(txn);

	let txn = store.begin().unwrap();
	let mut iter = txn.range(b"a", b"z").unwrap();
	assert_eq!(
		collect_forward(&mut iter),
		vec![
			(b"a".to_vec(), b"d-a".to_vec()),
			(b"c".to_vec(), b"d-c".to_vec()),
			(b"e".to_vec(), b"d-e".to_vec()),
		],
		"default scan must see exactly the default keys and values"
	);
	drop(iter);
	drop(txn);

	store.close().await.unwrap();
}

/// Versioned reads (`get_at`, `history`) resolve only the owner's version
/// chain for a shared key: timestamps interleave across branches without
/// leaking either way.
#[test(tokio::test)]
async fn br5_versioned_reads_stay_owner_scoped() {
	let (store, _temp_dir) = create_store_with(|b| b.with_versioning(true, 0));
	let foreign = register_branch(&store, 53, "read/versions");

	let mut txn = store.begin().unwrap();
	txn.set_at(b"k", b"d1", 10).unwrap();
	txn.commit().await.unwrap();
	let mut txn = store.begin().unwrap();
	txn.set_at(b"k", b"d2", 20).unwrap();
	txn.commit().await.unwrap();

	let mut txn = begin_owned_rw(&store, foreign);
	txn.set_at(b"k", b"f1", 15).unwrap();
	txn.commit().await.unwrap();
	let mut txn = begin_owned_rw(&store, foreign);
	txn.set_at(b"k", b"f2", 25).unwrap();
	txn.commit().await.unwrap();

	let txn = begin_owned_rw(&store, foreign);
	assert_eq!(
		txn.get_at(b"k", 12).unwrap(),
		None,
		"the default branch's version at ts=10 must not leak into the foreign timeline"
	);
	assert_eq!(txn.get_at(b"k", 20).unwrap(), Some(b"f1".to_vec()));
	assert_eq!(txn.get_at(b"k", 30).unwrap(), Some(b"f2".to_vec()));

	let mut history = txn.history(b"k", b"l").unwrap();
	let versions = collect_forward(&mut history);
	assert_eq!(
		versions,
		vec![(b"k".to_vec(), b"f2".to_vec()), (b"k".to_vec(), b"f1".to_vec())],
		"foreign history must contain exactly the foreign versions"
	);
	drop(history);
	drop(txn);

	let txn = store.begin().unwrap();
	assert_eq!(txn.get_at(b"k", 12).unwrap(), Some(b"d1".to_vec()));
	assert_eq!(
		txn.get_at(b"k", 17).unwrap(),
		Some(b"d1".to_vec()),
		"the foreign version at ts=15 must not leak into the default timeline"
	);
	drop(txn);

	store.close().await.unwrap();
}

/// A read transaction on an idle branch sees an empty view and allocates
/// nothing — no runtime, no arena (invariant 8 extends to reads). The
/// non-vacuity arm proves the same reads observe data once the branch has
/// its first write.
#[test(tokio::test)]
async fn br5_idle_branch_reads_see_empty_and_allocate_nothing() {
	let (store, _temp_dir) = create_store_with(|b| b);
	let idle = register_branch(&store, 54, "read/idle");

	let baseline_runtimes = store.core.inner.runtimes.len();
	let baseline_arenas = store.core.inner.runtimes.total_arena_capacity_bytes();

	let txn = begin_owned_rw(&store, idle);
	assert_eq!(txn.get(b"anything").unwrap(), None);
	let mut iter = txn.range(b"a", b"z").unwrap();
	assert!(!iter.seek_first().unwrap(), "an idle branch's range scan must be empty");
	drop(iter);
	drop(txn);

	assert_eq!(
		store.core.inner.runtimes.len(),
		baseline_runtimes,
		"reads must not create a runtime for an idle branch"
	);
	assert_eq!(
		store.core.inner.runtimes.total_arena_capacity_bytes(),
		baseline_arenas,
		"reads must not allocate an arena for an idle branch"
	);

	// Non-vacuity: after the first write the same reads observe the value.
	let mut txn = begin_owned_rw(&store, idle);
	txn.set(b"anything", b"now-present").unwrap();
	txn.commit().await.unwrap();
	let txn = begin_owned_rw(&store, idle);
	assert_eq!(txn.get(b"anything").unwrap(), Some(b"now-present".to_vec()));
	drop(txn);
	assert_eq!(store.core.inner.runtimes.len(), baseline_runtimes + 1);

	store.close().await.unwrap();
}

/// Once both branches' memtables are flushed, a shared key's value lives
/// only in each owner's durable level set — reads must resolve it through
/// `levels_for(owner)`, proving table selection is owner-routed rather than
/// memtable-resident.
#[test(tokio::test)]
async fn br5_flushed_branch_values_are_read_from_owned_levels() {
	let (store, _temp_dir) = create_store_with(|b| b);
	let foreign = register_branch(&store, 55, "read/levels");

	let mut txn = store.begin().unwrap();
	txn.set(b"shared", b"default-durable").unwrap();
	txn.commit().await.unwrap();
	let mut txn = begin_owned_rw(&store, foreign);
	txn.set(b"shared", b"foreign-durable").unwrap();
	txn.commit().await.unwrap();

	// Rotate both runtimes and drain every flush queue to SSTs.
	let foreign_runtime = store.core.inner.runtimes.get(foreign).unwrap();
	store.core.inner.rotate_runtime_memtable(&foreign_runtime, 0).unwrap();
	store.core.inner.rotate_memtable().unwrap();
	while store.core.inner.flush_oldest_immutable_for_test().unwrap().is_some() {}

	// Fixture proof: the only copies are durable, one table per owner.
	assert!(foreign_runtime.active_memtable.read().unwrap().is_empty());
	assert_eq!(foreign_runtime.immutable_memtables.read().unwrap().iter().count(), 0);
	{
		let manifest = store.core.inner.level_manifest.read().unwrap();
		for owner in [BatchOwner::DEFAULT, foreign] {
			let tables = manifest
				.levels_for(owner)
				.map(|levels| levels.get_levels().first().map(|l0| l0.tables.len()).unwrap_or(0))
				.unwrap_or(0);
			assert!(tables > 0, "fixture must leave owner {owner:?} with a durable L0 table");
		}
	}

	let txn = begin_owned_rw(&store, foreign);
	assert_eq!(
		txn.get(b"shared").unwrap(),
		Some(b"foreign-durable".to_vec()),
		"foreign read must resolve its own durable table"
	);
	let mut iter = txn.range(b"a", b"z").unwrap();
	assert_eq!(
		collect_forward(&mut iter),
		vec![(b"shared".to_vec(), b"foreign-durable".to_vec())],
		"foreign scan must resolve only the foreign durable table"
	);
	drop(iter);
	drop(txn);

	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"shared").unwrap(), Some(b"default-durable".to_vec()));
	drop(txn);

	store.close().await.unwrap();
}

// ===== BR6: demultiplexed, fenced, duplicate-tolerant recovery =====

fn crash_open(path: std::path::PathBuf) -> Tree {
	TreeBuilder::new().with_path(path).with_flush_on_close(false).build().unwrap()
}

fn crash_open_with_catalog(
	path: std::path::PathBuf,
	catalog: crate::branch::BranchCatalog,
) -> Tree {
	TreeBuilder::new()
		.with_path(path)
		.with_flush_on_close(false)
		.with_initial_branch_catalog(catalog)
		.build()
		.unwrap()
}

/// Two owners interleaved in one WAL segment: reopen rebuilds each owner's
/// runtime from its own batches, and reads stay isolated across the crash.
#[test(tokio::test)]
async fn br6_multi_owner_segment_reopen_restores_both_branches() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let foreign = {
		let store = crash_open(path.clone());
		let foreign = register_branch(&store, 61, "crash/multi");
		let mut txn = store.begin().unwrap();
		txn.set(b"shared", b"default-v").unwrap();
		txn.commit().await.unwrap();
		let mut txn = begin_owned_rw(&store, foreign);
		txn.set(b"shared", b"foreign-v").unwrap();
		txn.commit().await.unwrap();
		store.close().await.unwrap();
		foreign
	};

	let mut catalog = crate::branch::BranchCatalog::new(BranchId::DEFAULT);
	catalog.create(BranchId::from_u128(61), "crash/multi", CommitVersion(0)).unwrap();
	let store = crash_open_with_catalog(path, catalog);

	assert_eq!(
		store.core.inner.runtimes.len(),
		2,
		"reopen must rebuild the foreign runtime from its WAL batches"
	);
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"shared").unwrap(), Some(b"default-v".to_vec()));
	drop(txn);
	let txn = begin_owned_rw(&store, foreign);
	assert_eq!(txn.get(b"shared").unwrap(), Some(b"foreign-v".to_vec()));
	drop(txn);

	store.close().await.unwrap();
}

/// One owner spread across two WAL segments: the intermediate memtable is
/// flushed to the owner's own level set during open, the last becomes the
/// runtime active, and both rows survive.
#[test(tokio::test)]
async fn br6_one_owner_across_segments_flushes_intermediates_at_open() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let foreign = {
		let store = crash_open(path.clone());
		let foreign = register_branch(&store, 62, "crash/segments");
		let mut txn = begin_owned_rw(&store, foreign);
		txn.set(b"k1", b"v1").unwrap();
		txn.commit().await.unwrap();
		store.core.inner.wal.write().rotate().unwrap();
		let mut txn = begin_owned_rw(&store, foreign);
		txn.set(b"k2", b"v2").unwrap();
		txn.commit().await.unwrap();
		store.close().await.unwrap();
		foreign
	};

	let mut catalog = crate::branch::BranchCatalog::new(BranchId::DEFAULT);
	catalog.create(BranchId::from_u128(62), "crash/segments", CommitVersion(0)).unwrap();
	let store = crash_open_with_catalog(path, catalog);

	// The older segment's memtable was flushed into the owner's own L0.
	let owner_l0_tables = store
		.core
		.inner
		.level_manifest
		.read()
		.unwrap()
		.levels_for(foreign)
		.map(|levels| levels.get_levels().first().map(|l0| l0.tables.len()).unwrap_or(0))
		.unwrap_or(0);
	assert!(
		owner_l0_tables > 0,
		"the intermediate recovered memtable must flush into the owner's level set at open"
	);

	let txn = begin_owned_rw(&store, foreign);
	assert_eq!(txn.get(b"k1").unwrap(), Some(b"v1".to_vec()));
	assert_eq!(txn.get(b"k2").unwrap(), Some(b"v2".to_vec()));
	drop(txn);

	store.close().await.unwrap();
}

/// Delete/recreate fencing (invariant 9): a stale generation's batches are
/// dropped during replay — no runtime is built for the dead owner and its
/// rows never leak into the recreated branch.
#[test(tokio::test)]
async fn br6_stale_generation_is_fenced_on_replay() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let (stale_owner, live_owner) = {
		let store = crash_open(path.clone());
		let stale_owner = register_branch(&store, 63, "agent/x");
		let mut txn = begin_owned_rw(&store, stale_owner);
		txn.set(b"k", b"stale-gen0").unwrap();
		txn.commit().await.unwrap();

		// Delete and recreate the same name: the new branch id carries
		// generation 1; the old owner is fenced from this point on.
		store.core.branch_catalog.write().unwrap().delete(BranchId::from_u128(63)).unwrap();
		let live_owner = register_branch(&store, 64, "agent/x");
		assert_eq!(live_owner.generation.0, 1, "recreated name must advance the generation");
		let mut txn = begin_owned_rw(&store, live_owner);
		txn.set(b"k", b"live-gen1").unwrap();
		txn.commit().await.unwrap();
		store.close().await.unwrap();
		(stale_owner, live_owner)
	};

	let mut catalog = crate::branch::BranchCatalog::new(BranchId::DEFAULT);
	catalog.create(BranchId::from_u128(63), "agent/x", CommitVersion(0)).unwrap();
	catalog.delete(BranchId::from_u128(63)).unwrap();
	catalog.create(BranchId::from_u128(64), "agent/x", CommitVersion(0)).unwrap();
	let store = crash_open_with_catalog(path, catalog);

	assert!(
		store.core.inner.runtimes.get(stale_owner).is_none(),
		"a fenced generation must not get a runtime"
	);
	assert_eq!(store.core.inner.runtimes.len(), 2, "default plus the live generation only");
	let txn = begin_owned_rw(&store, live_owner);
	assert_eq!(
		txn.get(b"k").unwrap(),
		Some(b"live-gen1".to_vec()),
		"the recreated branch must see only its own generation's rows"
	);
	drop(txn);

	store.close().await.unwrap();
}

/// Reopening with a catalog that does not know the owner (today's production
/// default until catalogs are durable) succeeds, drops the foreign batches,
/// and still never rewinds the sequence clock below the fenced batches.
#[test(tokio::test)]
async fn br6_unknown_owner_is_fenced_and_seq_clock_never_rewinds() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let pre_crash_seq = {
		let store = crash_open(path.clone());
		let foreign = register_branch(&store, 65, "crash/unknown");
		let mut txn = store.begin().unwrap();
		txn.set(b"k", b"default-v").unwrap();
		txn.commit().await.unwrap();
		// The foreign batch commits last, so the clock's maximum belongs to
		// the batch that will be fenced (the non-vacuity arm).
		let mut txn = begin_owned_rw(&store, foreign);
		txn.set(b"k", b"foreign-v").unwrap();
		txn.commit().await.unwrap();
		let pre_crash_seq = store.core.seq_num();
		store.close().await.unwrap();
		pre_crash_seq
	};

	// No injected catalog: the reopen authority knows only the default branch.
	let store = crash_open(path);

	assert_eq!(store.core.inner.runtimes.len(), 1, "unknown owners must not get runtimes");
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"default-v".to_vec()));
	drop(txn);
	assert_eq!(
		store.core.seq_num(),
		pre_crash_seq,
		"fenced batches must still advance the recovered sequence clock"
	);

	// Post-fence liveness: new commits allocate above the fenced range.
	let mut txn = store.begin().unwrap();
	txn.set(b"after", b"crash").unwrap();
	txn.commit().await.unwrap();
	assert!(store.core.seq_num() > pre_crash_seq);
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"after").unwrap(), Some(b"crash".to_vec()));
	drop(txn);

	store.close().await.unwrap();
}

/// Partial branch flush before the crash: rows already in the owner's SSTs
/// are not re-replayed (the floor skips their segment), the WAL tail is, and
/// both halves read back correctly.
#[test(tokio::test)]
async fn br6_partial_branch_flush_replays_only_the_wal_tail() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let foreign = {
		let store = crash_open(path.clone());
		let foreign = register_branch(&store, 66, "crash/partial");
		let mut txn = begin_owned_rw(&store, foreign);
		txn.set(b"k1", b"v1").unwrap();
		txn.commit().await.unwrap();

		// Close k1's segment, flush it to the owner's SST, and prove the
		// replay floor moved past that segment.
		store.core.inner.wal.write().rotate().unwrap();
		let foreign_runtime = store.core.inner.runtimes.get(foreign).unwrap();
		store.core.inner.rotate_runtime_memtable(&foreign_runtime, 0).unwrap();
		while store.core.inner.flush_oldest_immutable_for_test().unwrap().is_some() {}
		let floor = store.core.inner.level_manifest.read().unwrap().get_log_number();
		assert!(floor >= 1, "fixture: the flush must advance the replay floor, got {floor}");

		let mut txn = begin_owned_rw(&store, foreign);
		txn.set(b"k2", b"v2").unwrap();
		txn.commit().await.unwrap();
		store.close().await.unwrap();
		foreign
	};

	let mut catalog = crate::branch::BranchCatalog::new(BranchId::DEFAULT);
	catalog.create(BranchId::from_u128(66), "crash/partial", CommitVersion(0)).unwrap();
	let store = crash_open_with_catalog(path, catalog);

	let txn = begin_owned_rw(&store, foreign);
	assert_eq!(txn.get(b"k1").unwrap(), Some(b"v1".to_vec()), "flushed row must come from the SST");
	assert_eq!(txn.get(b"k2").unwrap(), Some(b"v2".to_vec()), "tail row must come from replay");
	drop(txn);

	store.close().await.unwrap();
}

/// Sabotage twin for the monotone-sequence rule: a WAL whose batches regress
/// must fail the open closed, while the byte-identical fixture with
/// increasing sequences opens fine (non-vacuity).
#[test(tokio::test)]
async fn br6_sequence_regression_fails_closed() {
	fn craft_wal(path: &std::path::Path, seqs: [u64; 2]) {
		let wal_dir = path.join("wal");
		std::fs::create_dir_all(&wal_dir).unwrap();
		let mut wal =
			crate::wal::manager::Wal::open(&wal_dir, crate::wal::Options::default()).unwrap();
		for (index, seq) in seqs.into_iter().enumerate() {
			let mut batch = crate::batch::Batch::for_owner(seq, BatchOwner::DEFAULT);
			batch.set(format!("k{index}").into_bytes(), b"v".to_vec(), 0).unwrap();
			wal.append(&batch.encode().unwrap()).unwrap();
		}
		wal.close().unwrap();
	}

	// Non-vacuity arm: increasing sequences open fine.
	let temp_dir = TempDir::new("test").unwrap();
	craft_wal(temp_dir.path(), [100, 200]);
	let store = crash_open(temp_dir.path().to_path_buf());
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"k0").unwrap(), Some(b"v".to_vec()));
	drop(txn);
	store.close().await.unwrap();

	// Sabotage arm: regressing sequences must fail the open closed.
	let temp_dir = TempDir::new("test").unwrap();
	craft_wal(temp_dir.path(), [200, 100]);
	let result = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_flush_on_close(false)
		.build();
	assert!(result.is_err(), "a sequence-regressed WAL must fail the open closed");
}

// ===== BR7: maintenance pressure and shutdown/checkpoint parity =====

/// A foreign branch's L0 compacts under the one scheduler. Without per-owner
/// picking, the branch's L0 would grow unboundedly and its owner-scoped
/// stall would eventually park that branch's writes permanently.
#[test(tokio::test)]
async fn br7_foreign_branch_l0_compacts() {
	let (store, _temp_dir) = create_store_with(|b| b);
	let foreign = register_branch(&store, 71, "compact/branch");

	// Five foreign L0 tables (level0_max_files default is 4, so the leveled
	// strategy must pick this owner's L0).
	for i in 0..5u32 {
		let mut txn = begin_owned_rw(&store, foreign);
		txn.set(format!("k{i}").as_bytes(), b"v").unwrap();
		txn.commit().await.unwrap();
		let runtime = store.core.inner.runtimes.get(foreign).unwrap();
		store.core.inner.rotate_runtime_memtable(&runtime, 0).unwrap();
		while store.core.inner.flush_oldest_immutable_for_test().unwrap().is_some() {}
	}
	let l0_count = |owner| {
		store
			.core
			.inner
			.level_manifest
			.read()
			.unwrap()
			.levels_for(owner)
			.map(|levels| levels.get_levels().first().map(|l0| l0.tables.len()).unwrap_or(0))
			.unwrap_or(0)
	};
	assert_eq!(l0_count(foreign), 5, "fixture must build five foreign L0 tables");

	let strategy = std::sync::Arc::new(crate::compaction::leveled::Strategy::from_options(
		Arc::clone(&store.core.inner.opts),
	));
	store.compact(strategy).unwrap();

	let foreign_l1 = store
		.core
		.inner
		.level_manifest
		.read()
		.unwrap()
		.levels_for(foreign)
		.map(|levels| levels.get_levels().get(1).map(|l1| l1.tables.len()).unwrap_or(0))
		.unwrap_or(0);
	assert!(
		l0_count(foreign) < 5 && foreign_l1 > 0,
		"the foreign owner's L0 must compact into its own L1"
	);

	// Every row survives compaction, still owner-scoped.
	let txn = begin_owned_rw(&store, foreign);
	for i in 0..5u32 {
		assert_eq!(txn.get(format!("k{i}").as_bytes()).unwrap(), Some(b"v".to_vec()));
	}
	drop(txn);

	store.close().await.unwrap();
}

/// A flush-on-close shutdown captures every dirty runtime: reopening replays
/// nothing (no foreign runtime is rebuilt) and the branch row is served from
/// the owner's own durable levels.
#[test(tokio::test)]
async fn br7_shutdown_flushes_every_dirty_runtime() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let foreign = {
		let store = TreeBuilder::new().with_path(path.clone()).build().unwrap();
		let foreign = register_branch(&store, 72, "shutdown/dirty");
		let mut txn = store.begin().unwrap();
		txn.set(b"k", b"default-v").unwrap();
		txn.commit().await.unwrap();
		let mut txn = begin_owned_rw(&store, foreign);
		txn.set(b"k", b"foreign-v").unwrap();
		txn.commit().await.unwrap();
		store.close().await.unwrap(); // flush_on_close defaults to true
		foreign
	};

	let mut catalog = crate::branch::BranchCatalog::new(BranchId::DEFAULT);
	catalog.create(BranchId::from_u128(72), "shutdown/dirty", CommitVersion(0)).unwrap();
	let store = crash_open_with_catalog(path, catalog);

	assert_eq!(
		store.core.inner.runtimes.len(),
		1,
		"a clean shutdown must leave nothing to replay — the branch row lives in its SSTs"
	);
	let txn = begin_owned_rw(&store, foreign);
	assert_eq!(txn.get(b"k").unwrap(), Some(b"foreign-v".to_vec()));
	drop(txn);
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), Some(b"default-v".to_vec()));
	drop(txn);

	store.close().await.unwrap();
}

/// A checkpoint captures dirty branch actives: the checkpoint starts a fresh
/// WAL, so the branch row is readable from the checkpoint only if the
/// checkpoint flushed it into the owner's SSTs.
#[test(tokio::test)]
async fn br7_checkpoint_captures_dirty_branch_actives() {
	let temp_dir = TempDir::new("test").unwrap();
	let checkpoint_dir = TempDir::new("checkpoint").unwrap();
	let path = temp_dir.path().to_path_buf();

	let store = TreeBuilder::new().with_path(path).build().unwrap();
	let foreign = register_branch(&store, 73, "checkpoint/dirty");
	let mut txn = begin_owned_rw(&store, foreign);
	txn.set(b"k", b"cp-v").unwrap();
	txn.commit().await.unwrap();

	store.create_checkpoint(checkpoint_dir.path()).unwrap();

	let mut catalog = crate::branch::BranchCatalog::new(BranchId::DEFAULT);
	catalog.create(BranchId::from_u128(73), "checkpoint/dirty", CommitVersion(0)).unwrap();
	let restored = crash_open_with_catalog(checkpoint_dir.path().to_path_buf(), catalog);

	assert_eq!(
		restored.core.inner.runtimes.len(),
		1,
		"the checkpoint's fresh WAL replays nothing — the row must come from SSTs"
	);
	let txn = begin_owned_rw(&restored, foreign);
	assert_eq!(
		txn.get(b"k").unwrap(),
		Some(b"cp-v".to_vec()),
		"a dirty branch active must be captured into the checkpoint"
	);
	drop(txn);

	restored.close().await.unwrap();
	store.close().await.unwrap();
}

/// The WAL-span trickle rotates exactly ONE victim per call — the oldest
/// pinned runtime first — and terminates when no active pins beyond the
/// limit. No mass flush, no lost acknowledged commit.
#[test(tokio::test)]
async fn br7_trickle_rotates_single_oldest_victim() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let (first, second) = {
		let store = TreeBuilder::new()
			.with_path(path.clone())
			.with_wal_pinned_segment_limit(2)
			.with_flush_on_close(false)
			.build()
			.unwrap();
		let first = register_branch(&store, 74, "trickle/oldest");
		let second = register_branch(&store, 75, "trickle/newer");

		// First pins segment 0, second pins segment 1.
		let mut txn = begin_owned_rw(&store, first);
		txn.set(b"a", b"a-v").unwrap();
		txn.commit().await.unwrap();
		store.core.inner.wal.write().rotate().unwrap();
		let mut txn = begin_owned_rw(&store, second);
		txn.set(b"b", b"b-v").unwrap();
		txn.commit().await.unwrap();
		store.core.inner.wal.write().rotate().unwrap();
		store.core.inner.wal.write().rotate().unwrap(); // active segment 3

		// Call 1: only the OLDEST victim rotates (trickle, not a storm).
		assert!(store.core.inner.rotate_wal_pinned_runtime_impl().unwrap());
		let first_runtime = store.core.inner.runtimes.get(first).unwrap();
		let second_runtime = store.core.inner.runtimes.get(second).unwrap();
		assert!(first_runtime.active_memtable.read().unwrap().is_empty());
		assert_eq!(first_runtime.immutable_memtables.read().unwrap().iter().count(), 1);
		assert!(
			!second_runtime.active_memtable.read().unwrap().is_empty(),
			"one victim per call: the newer pinned runtime must be untouched"
		);

		// Call 2 takes the remaining victim; call 3 finds none.
		assert!(store.core.inner.rotate_wal_pinned_runtime_impl().unwrap());
		assert!(
			!store.core.inner.rotate_wal_pinned_runtime_impl().unwrap(),
			"no victim may remain once every span is inside the limit"
		);

		while store.core.inner.flush_oldest_immutable_for_test().unwrap().is_some() {}
		store.close().await.unwrap();
		(first, second)
	};

	// No acknowledged commit was lost to the reclaim policy.
	let mut catalog = crate::branch::BranchCatalog::new(BranchId::DEFAULT);
	catalog.create(BranchId::from_u128(74), "trickle/oldest", CommitVersion(0)).unwrap();
	catalog.create(BranchId::from_u128(75), "trickle/newer", CommitVersion(0)).unwrap();
	let store = crash_open_with_catalog(path, catalog);
	let txn = begin_owned_rw(&store, first);
	assert_eq!(txn.get(b"a").unwrap(), Some(b"a-v".to_vec()));
	drop(txn);
	let txn = begin_owned_rw(&store, second);
	assert_eq!(txn.get(b"b").unwrap(), Some(b"b-v".to_vec()));
	drop(txn);
	store.close().await.unwrap();
}

/// One hundred cold dirty branches cannot retain the WAL: the trickle loop
/// drains them one at a time, the replay floor advances, old segments are
/// reclaimed, and every acknowledged commit survives reopen.
#[test(tokio::test)]
async fn br7_hundred_cold_branches_bounded_wal_and_no_lost_commit() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	{
		let store = TreeBuilder::new()
			.with_path(path.clone())
			.with_branch_memtable_size(4 * 1024)
			.with_wal_pinned_segment_limit(2)
			.build()
			.unwrap();
		for i in 0..100u128 {
			let owner = register_branch(&store, 7_000 + i, &format!("cold/{i}"));
			let mut txn = begin_owned_rw(&store, owner);
			txn.set(format!("k{i}").into_bytes().as_slice(), format!("v{i}").as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}
		store.core.inner.wal.write().rotate().unwrap();
		store.core.inner.wal.write().rotate().unwrap(); // spans now exceed the limit

		// Trickle drain: one rotation, then its flush, then ask again.
		let mut rotations = 0;
		while store.core.inner.rotate_wal_pinned_runtime_impl().unwrap() {
			rotations += 1;
			store.core.inner.flush_all_immutables_sync().unwrap();
		}
		assert_eq!(rotations, 100, "every cold branch must be drained exactly once");

		let floor = store.core.inner.level_manifest.read().unwrap().get_log_number();
		assert!(floor >= 1, "the replay floor must advance past the drained segment");
		let wal_dir = store.core.inner.opts.wal_dir();
		let removed = crate::wal::cleanup_old_segments(&wal_dir, floor).unwrap();
		assert!(removed >= 1, "reclaim must delete segments below the floor");
		let remaining = crate::wal::list_segment_ids(&wal_dir, Some("wal")).unwrap();
		assert!(remaining.len() <= 3, "WAL retention must stay bounded, got {remaining:?}");

		store.close().await.unwrap();
	}

	let mut catalog = crate::branch::BranchCatalog::new(BranchId::DEFAULT);
	for i in 0..100u128 {
		catalog
			.create(BranchId::from_u128(7_000 + i), &format!("cold/{i}"), CommitVersion(0))
			.unwrap();
	}
	let store = crash_open_with_catalog(path, catalog);
	assert_eq!(store.core.inner.runtimes.len(), 1, "clean shutdown leaves nothing to replay");
	for i in 0..100u128 {
		let owner = BatchOwner {
			branch: BranchId::from_u128(7_000 + i),
			generation: crate::BranchGeneration(0),
		};
		let txn = begin_owned_rw(&store, owner);
		assert_eq!(
			txn.get(format!("k{i}").as_bytes()).unwrap(),
			Some(format!("v{i}").into_bytes()),
			"no acknowledged commit may depend on a reclaimed segment"
		);
	}
	store.close().await.unwrap();
}

/// The engine addresses the default branch by the reserved
/// `BranchId::DEFAULT`; an injected catalog whose default identity differs
/// would fence every default transaction, so the open refuses it outright.
#[test(tokio::test)]
async fn br6_open_refuses_catalog_with_foreign_default_identity() {
	let temp_dir = TempDir::new("test").unwrap();
	let wrong = crate::branch::BranchCatalog::new(BranchId::from_u128(999));
	let result = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_initial_branch_catalog(wrong)
		.build();
	assert!(result.is_err(), "a catalog missing the reserved default identity must be refused");
}
