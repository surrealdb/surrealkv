//! Crash-consistency tests.
//!
//! These tests verify the on-disk write barrier: an SST's data (and metadata)
//! must be durable BEFORE the manifest durably references it. Power loss is
//! simulated at the filesystem-contract level: after a crash, a file that was
//! never fsynced may legally come back empty (ext4/XFS delayed allocation),
//! while fsynced files must survive intact. The `vfs::sync_tracker` ledger
//! records which files were made durable via `vfs::fsync_file`.

use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::compaction::leveled::Strategy;
use crate::vfs::sync_tracker;
use crate::{Error, Options, Tree};

/// Truncates a file to zero bytes, the way a power loss can when the file's
/// data was never fsynced.
fn simulate_data_loss(path: &std::path::Path) {
	std::fs::OpenOptions::new().write(true).open(path).unwrap().set_len(0).unwrap();
}

/// Test for https://github.com/surrealdb/surrealdb/issues/7426
///
/// Compaction writes its output SST, the (fsynced) manifest then references
/// it, and the input tables are unlinked. If the output SST is not fsynced
/// before the manifest commit, a power loss shortly after a compaction leaves
/// a durable manifest referencing a 0-byte SST: the store refuses to open
/// with `LoadManifestFail("...file size is too small: 0 bytes...")` and the
/// merged data is unrecoverable (inputs deleted, covering WAL long gone).
///
/// A further batch is flushed after the compaction so the manifest also
/// references a newer flush-created table, mirroring the on-disk state in
/// the report: a 0-byte compaction output (table 5 there) alongside an
/// intact flush output (the 1064-byte table 6 there).
///
/// NOTE: must stay on the default current-thread runtime (no `multi_thread`),
/// and there must be no `.await` between `drop(tree)` and reopen, so that the
/// best-effort async close spawned by `Drop` is never polled.
#[test(tokio::test)]
async fn power_loss_after_compaction_must_not_lose_synced_data() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = Arc::new(Options {
		path,
		// Compact as soon as two L0 tables exist.
		level0_max_files: 2,
		// Crash semantics: nothing may be flushed on drop/close.
		flush_on_close: false,
		..Default::default()
	});

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Two batches, two flushes -> two L0 tables (fsynced by the flush path).
	for batch in 0..2u32 {
		let mut txn = tree.begin().unwrap();
		for i in 0..100u32 {
			let key = format!("key_{batch:02}_{i:03}");
			let value = format!("value_{batch:02}_{i:03}");
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		}
		txn.commit().await.unwrap();
		tree.drain_flushes_synchronously().unwrap();
	}

	let l0_ids: Vec<u64> = {
		let manifest = tree.core.inner.level_manifest.read().unwrap();
		manifest.default_owner_levels().get_levels()[0].tables.iter().map(|t| t.id).collect()
	};
	assert_eq!(l0_ids.len(), 2, "expected two L0 tables before compaction");

	// Merge both L0 tables into one L1 table.
	tree.compact(Arc::new(Strategy::from_options(Arc::clone(&opts)))).unwrap();

	// Don't trust Ok(()) — a Skip decision also returns Ok. Assert post-state.
	let output_id = {
		let manifest = tree.core.inner.level_manifest.read().unwrap();
		let levels = manifest.default_owner_levels().get_levels();
		assert!(levels[0].tables.is_empty(), "L0 should be empty after compaction");
		assert_eq!(levels[1].tables.len(), 1, "L1 should hold the compaction output");
		levels[1].tables[0].id
	};

	// Blast radius: the pre-merge tables are already unlinked, so if the
	// output is lost to a crash the data is unrecoverable.
	for id in &l0_ids {
		assert!(
			!opts.sstable_file_path(*id).exists(),
			"compaction input table {id} should be deleted"
		);
	}

	// One more batch flushed AFTER the compaction, so the manifest
	// references both the compaction output and a newer flush-created
	// table — the issue's exact topology (0-byte table beside an intact
	// sibling).
	{
		let batch = 2u32;
		let mut txn = tree.begin().unwrap();
		for i in 0..100u32 {
			let key = format!("key_{batch:02}_{i:03}");
			let value = format!("value_{batch:02}_{i:03}");
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		}
		txn.commit().await.unwrap();
		tree.drain_flushes_synchronously().unwrap();
	}
	let sibling_id = {
		let manifest = tree.core.inner.level_manifest.read().unwrap();
		let levels = manifest.default_owner_levels().get_levels();
		assert_eq!(levels[0].tables.len(), 1, "post-compaction flush should land in L0");
		levels[0].tables[0].id
	};

	// Simulate the crash: release the lock (so reopen works) and drop the
	// tree without a clean close().
	{
		let mut lockfile = tree.core.inner.lockfile.lock().unwrap();
		lockfile.release().unwrap();
	}
	drop(tree);

	// Simulate power loss: any SST whose data was never fsynced may legally
	// come back empty; fsynced files survive.
	let mut truncated = Vec::new();
	for entry in std::fs::read_dir(opts.sstable_dir()).unwrap() {
		let sst_path = entry.unwrap().path();
		if sst_path.extension().is_some_and(|ext| ext == "sst")
			&& !sync_tracker::was_synced(&sst_path)
		{
			simulate_data_loss(&sst_path);
			truncated.push(sst_path);
		}
	}

	// The flush-created sibling must have survived intact (it was fsynced)
	// — the analogue of the intact 1064-byte table in the issue report.
	let sibling_size = std::fs::metadata(opts.sstable_file_path(sibling_id)).unwrap().len();
	assert!(sibling_size > 0, "fsynced flush table {sibling_id} must survive the crash intact");

	// Reopen: the store must come back with every key intact. Without the
	// fsync barrier in the compaction path, the output table is truncated
	// above and this fails with LoadManifestFail("...file size is too
	// small: 0 bytes...") — exactly the failure in surrealdb#7426.
	let tree = Tree::new(Arc::clone(&opts)).unwrap_or_else(|e| {
		panic!(
			"store failed to reopen after simulated power loss (compaction \
			 output table {output_id} was not fsynced before the manifest \
			 referenced it; truncated: {truncated:?}): {e}"
		)
	});

	let txn = tree.begin().unwrap();
	for batch in 0..3u32 {
		for i in 0..100u32 {
			let key = format!("key_{batch:02}_{i:03}");
			let expected = format!("value_{batch:02}_{i:03}");
			let got = txn.get(key.as_bytes()).unwrap();
			assert_eq!(got, Some(expected.into_bytes()), "missing {key} after crash recovery");
		}
	}
}

/// A manifest that references a zero-byte SST must fail to open with a clear
/// error rather than panicking. This is the startup failure signature from
/// surrealdb/surrealdb#7426, and remains the expected behavior for disks
/// corrupted by means other than a crash.
#[test(tokio::test)]
async fn manifest_referencing_zero_byte_sst_fails_cleanly() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = Arc::new(Options {
		path,
		..Default::default()
	});

	// Create one flushed table, then close cleanly.
	let table_id = {
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let mut txn = tree.begin().unwrap();
		txn.set(b"key", b"value").unwrap();
		txn.commit().await.unwrap();
		tree.drain_flushes_synchronously().unwrap();

		let table_id = {
			let manifest = tree.core.inner.level_manifest.read().unwrap();
			manifest.default_owner_levels().get_levels()[0].tables[0].id
		};
		tree.close().await.unwrap();
		table_id
	};

	// Corrupt the referenced SST the way a crash does: 0 bytes on disk.
	simulate_data_loss(&opts.sstable_file_path(table_id));

	match Tree::new(Arc::clone(&opts)) {
		Ok(_) => panic!("open should fail when the manifest references a zero-byte SST"),
		Err(Error::LoadManifestFail(msg)) => {
			assert!(
				msg.contains("file size is too small"),
				"unexpected LoadManifestFail message: {msg}"
			);
		}
		Err(other) => panic!("expected LoadManifestFail, got: {other}"),
	}
}

/// The sync ledger refuses to answer about anything that is not an SSTable.
///
/// `fsync_file` is called from exactly three places and all three are SST
/// paths, so the ledger contains only SSTs. That makes `was_synced` answer
/// `false` for a WAL segment — not because the segment is vulnerable, but
/// because it was never a candidate. A power-loss simulation that widened its
/// walk from `opts.sstable_dir()` to the store root would act on that `false`
/// and truncate durable data.
///
/// Until 2026-08-15 the only thing preventing that was each caller remembering
/// to filter on the extension. Now it panics instead.
#[test]
#[should_panic(expected = "which is not an SSTable")]
fn the_sync_ledger_refuses_to_answer_about_a_non_sstable() {
	sync_tracker::was_synced(std::path::Path::new("/tmp/surrealkv-probe/000000000000000001.wal"));
}
