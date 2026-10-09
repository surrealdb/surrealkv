use std::sync::atomic::Ordering;
use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::compaction::leveled::Strategy;
use crate::vlog::ValueLocation;
use crate::{Options, Tree, TreeBuilder};

fn create_temp_directory() -> TempDir {
	TempDir::new("direct_l0_test").unwrap()
}

#[test(tokio::test)]
async fn test_batch_exceeding_max_memtable_size_commits_successfully() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	// Configure a small 64KB max_memtable_size
	const MAX_MEMTABLE: usize = 64 * 1024;
	let tree = TreeBuilder::new()
		.with_path(path.clone())
		.with_max_memtable_size(MAX_MEMTABLE)
		.build()
		.unwrap();

	// Total payload: 400 entries * 300 bytes ≈ 120 KB (> 1.8x max_memtable_size)
	const NUM_ENTRIES: usize = 400;
	let value = vec![0xAB; 300];

	{
		let mut txn = tree.begin().unwrap();
		for i in 0..NUM_ENTRIES {
			let key = format!("oversized_key_{i:06}").into_bytes();
			txn.set(&key, &value).unwrap();
		}
		// Previously, this would fail with Error::ArenaFull because the batch
		// cannot fit in a 64KB arena. Now it should flush directly to an L0 SSTable.
		txn.commit().await.unwrap();
	}

	// Verify all entries can be read back
	{
		let txn = tree.begin().unwrap();
		for i in 0..NUM_ENTRIES {
			let key = format!("oversized_key_{i:06}").into_bytes();
			let val = txn.get(&key).unwrap().expect("entry must exist");
			assert_eq!(val, value);
		}
	}

	// Verify an L0 SSTable was created in the manifest
	{
		let manifest = tree.core.inner.level_manifest.read().unwrap();
		let all_tables = manifest.get_all_tables();
		assert!(!all_tables.is_empty(), "direct-to-L0 flush must create an L0 table");
	}
}

#[test(tokio::test)]
async fn test_mixed_workload_ordering_with_direct_l0_flush() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	const MAX_MEMTABLE: usize = 64 * 1024;
	let tree = TreeBuilder::new()
		.with_path(path.clone())
		.with_max_memtable_size(MAX_MEMTABLE)
		.build()
		.unwrap();

	// 1. Commit small writes into the active memtable
	{
		let mut txn = tree.begin().unwrap();
		txn.set(b"small_1", b"val_small_1").unwrap();
		txn.set(b"small_2", b"val_small_2").unwrap();
		txn.commit().await.unwrap();
	}

	// 2. Commit an oversized batch (exceeds max_memtable_size -> direct to L0)
	const OVERSIZED_COUNT: usize = 300;
	let large_val = vec![0xCD; 300];
	{
		let mut txn = tree.begin().unwrap();
		for i in 0..OVERSIZED_COUNT {
			let k = format!("large_{i:06}").into_bytes();
			txn.set(&k, &large_val).unwrap();
		}
		txn.commit().await.unwrap();
	}

	// 3. Commit more small writes into the new active memtable
	{
		let mut txn = tree.begin().unwrap();
		txn.set(b"small_3", b"val_small_3").unwrap();
		txn.set(b"small_4", b"val_small_4").unwrap();
		txn.commit().await.unwrap();
	}

	// 4. Verify all writes across both memtables and the direct-to-L0 SSTable are readable
	{
		let txn = tree.begin().unwrap();
		assert_eq!(txn.get(b"small_1").unwrap().as_deref(), Some(&b"val_small_1"[..]));
		assert_eq!(txn.get(b"small_2").unwrap().as_deref(), Some(&b"val_small_2"[..]));
		assert_eq!(txn.get(b"small_3").unwrap().as_deref(), Some(&b"val_small_3"[..]));
		assert_eq!(txn.get(b"small_4").unwrap().as_deref(), Some(&b"val_small_4"[..]));

		for i in 0..OVERSIZED_COUNT {
			let k = format!("large_{i:06}").into_bytes();
			let val = txn.get(&k).unwrap().expect("large entry must exist");
			assert_eq!(val, large_val);
		}
	}
}

#[test(tokio::test)]
async fn test_direct_l0_flush_persists_across_restart() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	const MAX_MEMTABLE: usize = 64 * 1024;
	let value = vec![0xEF; 400];
	const COUNT: usize = 250;

	// Open, write oversized batch, close
	{
		let tree = TreeBuilder::new()
			.with_path(path.clone())
			.with_max_memtable_size(MAX_MEMTABLE)
			.build()
			.unwrap();

		let mut txn = tree.begin().unwrap();
		for i in 0..COUNT {
			let k = format!("restart_key_{i:06}").into_bytes();
			txn.set(&k, &value).unwrap();
		}
		txn.commit().await.unwrap();

		tree.close().await.unwrap();
	}

	// Reopen database at same path and verify data is durable
	{
		let tree = TreeBuilder::new()
			.with_path(path)
			.with_max_memtable_size(MAX_MEMTABLE)
			.build()
			.unwrap();

		let txn = tree.begin().unwrap();
		for i in 0..COUNT {
			let k = format!("restart_key_{i:06}").into_bytes();
			let val = txn.get(&k).unwrap().expect("entry must survive reopen");
			assert_eq!(val, value);
		}
	}
}

/// Regression test: a range-delete tombstone written via the direct-to-L0 path must be
/// reflected in the resulting table's `smallest_point`/`largest_point` bounds.
///
/// Before the fix, `write_batch_direct_to_l0_sst` only added `RangeDelete` entries to
/// the table's `range_deletions` metadata, never to its point-key data (unlike
/// `MemTable::flush`, which inserts a skiplist row for a `RangeDelete` entry too, so it
/// goes through `table_writer.add()` same as any other entry). That leaves the table's
/// declared point-key bounds not covering the tombstone's start key whenever it falls
/// outside the batch's other point keys. Those bounds are what compaction's overlap
/// detection consults (`Table::overlaps_with_range`, level sorting, and L1+ binary
/// search all key off `smallest_point`/`largest_point`) to decide which tables a merge
/// needs to include -- a table whose declared bounds don't cover its own tombstone can
/// be left out of a compaction that conceptually needed to see it, which is how a
/// tombstone can end up dropped without ever being consulted. (Range-scan queries
/// happen to be unaffected today: `range_deletions` are currently collected from every
/// table unconditionally, bypassing the same bounds-based pruning point gets already
/// skip -- see `Snapshot::range`/`Snapshot::get`. That collection loop is a plausible
/// future optimization target, though, and making it bounds-aware would silently
/// reintroduce the query-visible half of this bug if the bounds themselves stay wrong.)
#[test(tokio::test)]
async fn test_direct_l0_flush_range_delete_is_reflected_in_table_point_bounds() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	const MAX_MEMTABLE: usize = 64 * 1024;
	let tree = TreeBuilder::new()
		.with_path(path.clone())
		.with_max_memtable_size(MAX_MEMTABLE)
		.build()
		.unwrap();

	let table_ids_before: std::collections::HashSet<u64> = {
		let manifest = tree.core.inner.level_manifest.read().unwrap();
		manifest.get_all_tables().values().map(|t| t.id).collect()
	};

	// One oversized transaction (-> direct-to-L0 path) whose point keys are all
	// lexicographically far from "orphan", plus a range-delete tombstone covering
	// "orphan". The tombstone's start key sits outside the point keys' own bounds.
	const NUM_FILLER: usize = 400;
	let filler_value = vec![0xCDu8; 300];
	{
		let mut txn = tree.begin().unwrap();
		for i in 0..NUM_FILLER {
			let key = format!("aaa_filler_{i:06}").into_bytes();
			txn.set(&key, &filler_value).unwrap();
		}
		txn.delete_range(b"orphan".to_vec(), b"orphan\0".to_vec()).unwrap();
		txn.commit().await.unwrap();
	}

	// Point get is unaffected by this bug (`Snapshot::get` scans every table's
	// range_deletions unconditionally) -- assert it anyway as a baseline.
	{
		let txn = tree.begin().unwrap();
		assert_eq!(txn.get(b"orphan").unwrap(), None, "point get must see the tombstone");
	}

	// Find the newly created direct-to-L0 table and check its declared bounds.
	let new_table = {
		let manifest = tree.core.inner.level_manifest.read().unwrap();
		manifest
			.get_all_tables()
			.into_values()
			.find(|t| !table_ids_before.contains(&t.id))
			.expect("the oversized commit must have created exactly one new table")
	};

	assert!(
		new_table.has_range_deletions.load(std::sync::atomic::Ordering::Acquire),
		"the new table must record the range-delete tombstone"
	);

	let largest_point =
		new_table.meta.largest_point.as_ref().expect("table has point entries (the filler keys)");
	assert!(
		largest_point.user_key.as_slice() >= b"orphan".as_slice(),
		"the table's largest_point ({:?}) must cover the range-delete tombstone's start key \
		(\"orphan\"), or compaction overlap detection (which keys off smallest_point/\
		largest_point, not range_deletions) can decide this table doesn't need to \
		participate in a merge that conceptually overlaps the tombstone",
		String::from_utf8_lossy(largest_point.user_key.as_slice())
	);
}

/// Regression test: after a direct-to-L0 flush of an oversized batch that is the sole
/// occupant of its WAL segment, `log_number` must advance so that WAL segment is never
/// replayed again -- otherwise every restart re-materializes the same batch into a new,
/// duplicate L0 table (data duplication, unbounded WAL growth).
#[test(tokio::test)]
async fn test_direct_l0_flush_advances_log_number_so_wal_segment_is_not_replayed_again() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	const MAX_MEMTABLE: usize = 64 * 1024;
	let opts = Arc::new(Options {
		path: path.clone(),
		max_memtable_size: MAX_MEMTABLE,
		// Crash semantics: nothing may be flushed on drop/close, so the only way the
		// oversized batch's data can be durable is the direct-to-L0 table it already
		// wrote synchronously during commit.
		flush_on_close: false,
		..Default::default()
	});

	const COUNT: usize = 300;
	let value = vec![0xABu8; 300];

	let table_count_after_commit = {
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		let mut txn = tree.begin().unwrap();
		for i in 0..COUNT {
			let k = format!("key_{i:06}").into_bytes();
			txn.set(&k, &value).unwrap();
		}
		txn.commit().await.unwrap();

		let manifest = tree.core.inner.level_manifest.read().unwrap();
		let table_count = manifest.get_all_tables().len();
		let log_number = manifest.get_log_number();
		drop(manifest);

		assert!(
			log_number > 0,
			"log_number should have advanced past the WAL segment the oversized batch was \
			durably written to (an empty, brand-new active memtable means nothing else \
			depends on that segment), so it is never replayed again"
		);

		// Simulate the crash: release the lock (so reopen works) and drop the tree
		// without a clean close(), matching `crash_consistency_tests.rs`'s pattern.
		{
			let mut lockfile = tree.core.inner.lockfile.lock().unwrap();
			lockfile.release().unwrap();
		}

		table_count
		// `tree` drops here without any flush (flush_on_close = false) -- crash
		// simulation: only what was already durably committed during the commit
		// itself (the WAL append and the direct-to-L0 SST + manifest write) survives.
	};

	// Reopen: WAL replay must NOT re-materialize the oversized batch's data into a
	// second, duplicate L0 table, since log_number already marks its WAL segment as
	// captured.
	let tree = Tree::new(opts).unwrap();
	let table_count_after_reopen = {
		let manifest = tree.core.inner.level_manifest.read().unwrap();
		manifest.get_all_tables().len()
	};
	assert_eq!(
		table_count_after_reopen, table_count_after_commit,
		"reopening must not create a duplicate L0 table by re-replaying an already-captured \
		WAL segment"
	);

	// Data must still be fully readable after reopen.
	let txn = tree.begin().unwrap();
	for i in 0..COUNT {
		let k = format!("key_{i:06}").into_bytes();
		let val = txn.get(&k).unwrap().expect("entry must survive reopen");
		assert_eq!(val, value);
	}
}

/// Regression test: a direct-to-L0 flush must flush every older immutable memtable
/// first. If it installs its table while an older memtable is still pending, an L0->L1
/// compaction that runs in between moves the newer table to L1, and the older table
/// that lands in L0 afterwards shadows it: reads return the older value.
#[test(tokio::test)]
async fn test_direct_l0_flush_does_not_let_older_memtable_shadow_it() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = Arc::new(Options {
		path,
		max_memtable_size: 64 * 1024,
		level0_max_files: 1,
		..Default::default()
	});
	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Commit the old value, then rotate it into the immutable queue WITHOUT flushing
	// it to SST, simulating a flush that's still pending.
	{
		let mut txn = tree.begin().unwrap();
		txn.set(b"key", b"old").unwrap();
		txn.commit().await.unwrap();
	}
	tree.core.inner.rotate_memtable().unwrap();
	let pending_wal_number = {
		let immutables = tree.core.inner.immutable_memtables.read().unwrap();
		immutables.first().expect("expected a still-unflushed immutable memtable").wal_number
	};

	// Write the new value straight to L0, as a later oversized batch would.
	let seq = tree.core.inner.visible_seq_num.load(Ordering::Acquire) + 1;
	let mut batch = crate::batch::Batch::new(seq);
	let value = ValueLocation::with_inline_value(b"new".to_vec()).encode();
	batch.set(b"key".to_vec(), value, 0).unwrap();
	let table_id = tree.core.inner.level_manifest.read().unwrap().next_table_id();
	let batch_wal_number = tree.core.inner.seal_active_wal_segment().unwrap();
	tree.core.inner.write_batch_direct_to_l0_sst(&batch, table_id, batch_wal_number).unwrap();
	tree.core.inner.visible_seq_num.store(seq, Ordering::Release);

	assert!(
		tree.core.inner.immutable_memtables.read().unwrap().is_empty(),
		"the older memtable must be flushed before the direct-to-L0 table is installed"
	);
	let log_number = tree.core.inner.level_manifest.read().unwrap().get_log_number();
	assert!(
		log_number > batch_wal_number && batch_wal_number >= pending_wal_number,
		"with nothing older pending, log_number must move past the batch's segment \
		(log_number={log_number}, batch_wal_number={batch_wal_number})"
	);

	// Compact L0 into L1, then flush whatever is still pending.
	tree.compact(Arc::new(Strategy::from_options(Arc::clone(&opts)))).unwrap();
	tree.flush().unwrap();

	let txn = tree.begin().unwrap();
	assert_eq!(txn.get(b"key").unwrap(), Some(b"new".to_vec()));
}

/// Regression test: an oversized memtable that `replay_wal` allocates specifically to
/// absorb a single over-limit batch (see its `ArenaFull` handling) must never become
/// the live active memtable -- its capacity has nothing to do with `max_memtable_size`,
/// so it would silently blow through the configured memory bound for however long it
/// stays active.
#[test(tokio::test)]
async fn test_oversized_recovery_memtable_does_not_become_active() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();
	const MAX_MEMTABLE: usize = 64 * 1024;

	let opts = Options {
		path,
		max_memtable_size: MAX_MEMTABLE,
		..Default::default()
	};

	// Write a WAL segment directly (bypassing the live commit path entirely) holding
	// a single batch whose own size exceeds `max_memtable_size` -- as if a crash
	// happened between the WAL write and the direct-to-L0 SST/manifest commit
	// completing, so replay has to reconstruct the data itself via the
	// oversized-memtable fallback.
	let wal_dir = opts.wal_dir();
	std::fs::create_dir_all(&wal_dir).unwrap();
	let big_value = vec![0xEFu8; MAX_MEMTABLE * 2];
	// Values are stored `ValueLocation`-encoded (even inline ones) by the normal
	// commit path (`flush_group`, before they ever reach the WAL) -- replicate that
	// encoding here since this test writes directly to the WAL, bypassing that path.
	let encoded_value = crate::vlog::ValueLocation::with_inline_value(big_value.clone()).encode();
	{
		let wal_opts = crate::wal::Options::default();
		let mut wal = crate::wal::manager::Wal::open(&wal_dir, wal_opts).unwrap();
		let mut batch = crate::batch::Batch::new(1);
		batch.set(b"oversized_key".to_vec(), encoded_value, 0).unwrap();
		wal.append(&batch.encode().unwrap()).unwrap();
		wal.close().unwrap();
	}

	let tree = TreeBuilder::with_options(opts).build().unwrap();

	{
		let active = tree.core.inner.active_memtable.read().unwrap();
		assert_eq!(
			active.arena_capacity(),
			MAX_MEMTABLE,
			"the oversized recovery memtable must never become the live active memtable"
		);
	}

	let txn = tree.begin().unwrap();
	let val = txn.get(b"oversized_key").unwrap().expect("recovered data must still be readable");
	assert_eq!(val, big_value);
}
