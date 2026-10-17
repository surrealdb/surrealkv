//! Tests for compaction failure recovery:
//! - Partial output cleanup on non-uncertain failure
//! - Compaction retry with exponential backoff and recovery
//! - Immediate database halt on table/block corruption
//! - Output preservation on uncertain manifest writes

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tempdir::TempDir;
use test_log::test;

use super::{sfs as fs, tfs};
use crate::compaction::compactor::CompactionStage;
use crate::compaction::leveled::Strategy;
use crate::error::{BackgroundErrorHandler, BackgroundErrorReason, Error, ErrorSeverity};
use crate::levels::{fail_replacements, heal_replacements, ReplaceFailure};
use crate::{Mode, Options, Tree};

fn ssts(dir: &Path) -> BTreeSet<u64> {
	let mut ids = BTreeSet::new();
	let sst_dir = dir.join("sstables");
	if let Ok(entries) = fs::read_dir(sst_dir) {
		for entry in entries.flatten() {
			let name = entry.file_name().to_string_lossy().into_owned();
			if let Some(stem) = name.strip_suffix(".sst") {
				if let Ok(id) = stem.parse::<u64>() {
					ids.insert(id);
				}
			}
		}
	}
	ids
}

#[test(tokio::test)]
async fn test_compaction_partial_output_cleanup_on_failure() {
	let temp_dir = TempDir::new("compaction_cleanup").unwrap();
	let options = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		max_memtable_size: 1024 * 1024,
		..Default::default()
	});

	let tree = Arc::new(Tree::new(Arc::clone(&options)).unwrap());

	// Ingest multiple L0 tables
	for i in 0..4 {
		let mut tx = tree.begin().unwrap();
		for j in 0..25 {
			let k = format!("k_{:02}_{:04}", i, j).into_bytes();
			let v = format!("v_{:02}_{:04}", i, j).into_bytes();
			tx.set(k, v).unwrap();
		}
		tx.commit().await.unwrap();
		tree.flush().unwrap();
	}

	let files_before = ssts(temp_dir.path());
	assert!(files_before.len() >= 4, "Should have created at least 4 L0 files");

	// Install a hook that fails compaction after outputs are written (OutputsDurable)
	*tree.core.inner.compaction_stage_hook.lock() = Some(Arc::new(|stage| {
		if stage == CompactionStage::OutputsDurable {
			Err(Error::Io(Arc::new(std::io::Error::other("injected compaction write failure"))))
		} else {
			Ok(())
		}
	}));

	// Trigger compaction; should fail
	let strategy = Arc::new(Strategy::from_options(Arc::clone(&options)));
	let res = tree.compact(strategy);
	assert!(res.is_err(), "Compaction should fail due to hook");

	// Verify that partial output SSTables were deleted and did not linger as orphans
	let files_after = ssts(temp_dir.path());
	assert_eq!(
		files_before, files_after,
		"Failed compaction must clean up newly created output files"
	);

	// Remove hook
	*tree.core.inner.compaction_stage_hook.lock() = None;

	// Retry compaction; should now succeed
	let strategy = Arc::new(Strategy::from_options(Arc::clone(&options)));
	tree.compact(strategy).unwrap();

	// Verify all data is still readable
	for i in 0..4 {
		let tx = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		for j in 0..25 {
			let k = format!("k_{:02}_{:04}", i, j).into_bytes();
			let expected = format!("v_{:02}_{:04}", i, j).into_bytes();
			let val = tx.get(&k).unwrap().expect("Key should exist after compaction");
			assert_eq!(val, expected);
		}
	}
}

#[test(tokio::test)]
async fn test_compaction_retry_with_backoff_and_recovery() {
	let temp_dir = TempDir::new("compaction_retry").unwrap();
	let options = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		max_memtable_size: 1024 * 1024,
		level0_max_files: 4,
		..Default::default()
	});

	let tree = Arc::new(Tree::new(Arc::clone(&options)).unwrap());

	// Ingest 4 L0 files
	for i in 0..4 {
		let mut tx = tree.begin().unwrap();
		for j in 0..20 {
			let k = format!("k_{:02}_{:04}", i, j).into_bytes();
			let v = format!("v_{:02}_{:04}", i, j).into_bytes();
			tx.set(k, v).unwrap();
		}
		tx.commit().await.unwrap();
		tree.flush().unwrap();
	}

	// Install a hook that fails twice at OutputsDurable then succeeds
	let failures_remaining = Arc::new(AtomicUsize::new(2));
	let failures_clone = Arc::clone(&failures_remaining);
	*tree.core.inner.compaction_stage_hook.lock() = Some(Arc::new(move |stage| {
		if stage == CompactionStage::OutputsDurable && failures_clone.load(Ordering::SeqCst) > 0 {
			failures_clone.fetch_sub(1, Ordering::SeqCst);
			return Err(Error::Io(Arc::new(std::io::Error::other("transient compaction failure"))));
		}
		Ok(())
	}));

	// Wake up background compaction
	if let Some(ref tm) = *tree.core.task_manager.lock().unwrap() {
		tm.wake_up_level();
	}

	// Wait for the transient failure to register as a soft error
	let mut saw_error = false;
	let start = std::time::Instant::now();
	while start.elapsed() < Duration::from_secs(5) {
		if let Some(err) = tree.background_error() {
			assert_eq!(err.reason, BackgroundErrorReason::Compaction);
			assert_eq!(err.severity, ErrorSeverity::SoftError);
			saw_error = true;
			break;
		}
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
	assert!(saw_error, "Should have observed soft error in background error handler");
	assert!(!tree.is_stopped(), "Soft error must not stop the database");

	// Writers can continue while background compaction experiences transient soft error
	let mut tx = tree.begin().unwrap();
	tx.set(b"live_write_key", b"live_write_val").unwrap();
	tx.commit().await.unwrap();

	// Wait for the compaction task to retry and succeed, clearing the error
	let mut recovered = false;
	let start = std::time::Instant::now();
	while start.elapsed() < Duration::from_secs(10) {
		if tree.background_error().is_none() && failures_remaining.load(Ordering::SeqCst) == 0 {
			recovered = true;
			break;
		}
		tokio::time::sleep(Duration::from_millis(50)).await;
	}
	assert!(recovered, "Compaction task should have retried with backoff and recovered");

	// Verify key written during transient failure exists
	let tx = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let val = tx.get(b"live_write_key").unwrap().unwrap();
	assert_eq!(val, b"live_write_val");
}

#[test(tokio::test)]
async fn test_compaction_corruption_halts_database() {
	let temp_dir = TempDir::new("compaction_corruption").unwrap();
	let options = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	});

	let tree = Arc::new(Tree::new(Arc::clone(&options)).unwrap());

	// Classify error verification
	let block_err = Error::CorruptedBlock("block corruption".into());
	assert_eq!(
		BackgroundErrorHandler::classify_error(&block_err, BackgroundErrorReason::Compaction),
		ErrorSeverity::Unrecoverable
	);

	let corrupt_err = Error::Corruption("sstable corruption".into());
	assert_eq!(
		BackgroundErrorHandler::classify_error(&corrupt_err, BackgroundErrorReason::Compaction),
		ErrorSeverity::Unrecoverable
	);

	// Ingest a record
	let mut tx = tree.begin().unwrap();
	tx.set(b"k1", b"v1").unwrap();
	tx.commit().await.unwrap();

	// Inject unrecoverable corruption error via error handler
	let sev = tree.core.error_handler.set_error(block_err, BackgroundErrorReason::Compaction);
	assert_eq!(sev, ErrorSeverity::Unrecoverable);

	// DB is now stopped
	assert!(tree.is_stopped());
	assert!(tree.core.error_handler.is_db_stopped());

	// Further writes are blocked
	let tx_res = tree.begin();
	if let Ok(mut tx) = tx_res {
		tx.set(b"k2", b"v2").unwrap();
		let commit_res = tx.commit().await;
		assert!(commit_res.is_err(), "Commit should fail when DB is stopped by corruption");
	}
}

#[test(tokio::test)]
async fn test_compaction_manifest_write_uncertain_stops_db_and_preserves_outputs() {
	if tfs::skip_listed() {
		return;
	}
	let temp_dir = TempDir::new("compaction_uncertain").unwrap();
	let options = Arc::new(Options {
		path: temp_dir.path().to_path_buf(),
		max_memtable_size: 1024 * 1024,
		..Default::default()
	});

	let tree = Arc::new(Tree::new(Arc::clone(&options)).unwrap());

	// Ingest multiple L0 tables
	for i in 0..4 {
		let mut tx = tree.begin().unwrap();
		for j in 0..25 {
			let k = format!("k_{:02}_{:04}", i, j).into_bytes();
			let v = format!("v_{:02}_{:04}", i, j).into_bytes();
			tx.set(k, v).unwrap();
		}
		tx.commit().await.unwrap();
		tree.flush().unwrap();
	}

	let files_before = ssts(temp_dir.path());
	let manifest = tree.core.inner.level_manifest.read().unwrap().path.clone();

	// Inject failure after manifest rename: ManifestWriteUncertain
	fail_replacements(&manifest, ReplaceFailure::AfterRename, 0, 1);

	let strategy = Arc::new(Strategy::from_options(Arc::clone(&options)));
	let res = tree.compact(strategy);

	heal_replacements(&manifest);

	let err = res.expect_err("Compaction should fail due to manifest write after rename");
	assert!(
		matches!(err, Error::ManifestWriteUncertain(_)),
		"Error must be ManifestWriteUncertain, got: {err:?}"
	);

	// Verify manifest_uncertain flag is set
	assert!(
		tree.core.inner.manifest_uncertain.load(Ordering::Acquire),
		"manifest_uncertain flag must be set"
	);

	// Verify that outputs were NOT deleted because manifest state on disk is uncertain
	let files_after = ssts(temp_dir.path());
	let new_outputs: Vec<u64> = files_after.difference(&files_before).copied().collect();
	assert!(
		!new_outputs.is_empty(),
		"Outputs must be preserved on disk when manifest write is uncertain: {new_outputs:?}"
	);
}
