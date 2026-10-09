//! A failed manifest write in a flush, and a commit group that stopped the database.
//!
//! A flush that fails below `FatalError` is run again by the background task, and the success
//! ends the error it recorded, so commits resume. A commit group that fails after part of it was
//! applied stops the database for good: the memtables hold batches that were never published, so
//! nothing may move them to a table. The two must not meet: the recovery of a flush ends the
//! error of the flush only, and a stopped database refuses every flush, the retried one included.

use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tempdir::TempDir;
use test_log::test;

use super::manifest_write_failure_tests::{
	immutables,
	manifest_path,
	park_attempt,
	table_ids,
	wait_until,
	Healed,
	GRACE,
};
use crate::error::BackgroundErrorReason;
use crate::levels::{fail_replacements, ReplaceFailure};
use crate::{Durability, Error, Mode, Options, Result, Tree};

/// A memtable that holds about 21 small commits.
const SMALL: usize = 4096;

fn open(path: &Path, max_memtable_size: usize) -> Arc<Tree> {
	Arc::new(
		Tree::new(Arc::new(Options {
			path: path.to_path_buf(),
			max_memtable_size,
			flush_on_close: false,
			memtable_stall_threshold: 1000,
			..Default::default()
		}))
		.unwrap(),
	)
}

async fn commit_one(tree: Arc<Tree>, key: String, value: Vec<u8>) -> Result<()> {
	let mut txn = tree.begin()?;
	txn.set_durability(Durability::Immediate);
	txn.set(key.as_bytes(), &value)?;
	txn.commit().await
}

/// One commit per entry, all spawned before the flusher is polled: on the current-thread runtime
/// that is one commit group, in order.
async fn commit_group(tree: &Arc<Tree>, entries: &[(String, Vec<u8>)]) -> Vec<Result<()>> {
	let handles: Vec<_> = entries
		.iter()
		.map(|(key, value)| tokio::spawn(commit_one(Arc::clone(tree), key.clone(), value.clone())))
		.collect();
	let mut results = Vec::new();
	for handle in handles {
		results.push(handle.await.unwrap());
	}
	results
}

fn base() -> Vec<(String, Vec<u8>)> {
	(0..10).map(|i| (format!("base{i}"), vec![b'b'; 40])).collect()
}

/// Twenty commits with an oversized one in the middle: it bypasses the memtable, and the memtable
/// that holds the ten before it is flushed first.
fn group_with_oversized() -> Vec<(String, Vec<u8>)> {
	(0..20)
		.map(|i| match i {
			10 => ("big10".to_string(), vec![b'g'; SMALL + 1000]),
			_ => (format!("group{i}"), vec![b'g'; 40]),
		})
		.collect()
}

fn keys(tree: &Tree) -> Vec<String> {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let all = super::collect_transaction_all(&mut txn.iter().unwrap()).unwrap();
	all.into_iter().map(|(key, _)| String::from_utf8(key).unwrap()).collect()
}

fn files_with_extension(path: &Path, dir: &str, extension: &str) -> Vec<String> {
	let mut names: Vec<String> = fs::read_dir(path.join(dir))
		.unwrap()
		.map(|e| e.unwrap().file_name().to_string_lossy().to_string())
		.filter(|name| name.ends_with(extension))
		.collect();
	names.sort();
	names
}

fn assert_stopped(tree: &Tree, what: &str) {
	let handler = &tree.core.inner.error_handler;
	assert!(handler.is_db_stopped(), "{what}: the database is stopped");
	assert!(matches!(handler.check_error(), Err(Error::DatabaseStopped(_))), "{what}");
	assert!(handler.commit_group_error().is_some(), "{what}");
	assert_eq!(handler.get_error().unwrap().reason, BackgroundErrorReason::CommitGroup, "{what}");
}

/// A flush that was running when a commit group stopped the database, and that succeeds, ends the
/// error of the flush and nothing else: the database stays stopped, commits fail with the error
/// of the group, and the flushes and the checkpoints that come after are refused.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_flush_that_succeeds_after_a_group_stopped_the_database_does_not_end_the_stop() {
	let dir = TempDir::new("manifest_stop_in_flight").unwrap();
	let tree = open(dir.path(), 16 * 1024 * 1024);
	for result in commit_group(&tree, &base()).await {
		result.unwrap();
	}
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	// The first attempt fails, and records the error; the retry passes the check of the stop and
	// waits for the test between its table and the manifest.
	let retry = park_attempt(&tree, 2);
	fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, 1);
	tree.core.inner.rotate_memtable().unwrap();
	// What is committed after the rotation stays in the active memtable, to be queued later.
	commit_one(Arc::clone(&tree), "later".into(), vec![b'l'; 40]).await.unwrap();
	tree.core.task_manager.lock().unwrap().as_ref().unwrap().wake_up_memtable();
	assert!(retry.is_reached(), "the failed flush is retried");
	let flush_error = tree.core.inner.error_handler.get_error().unwrap();
	assert_eq!(flush_error.reason, BackgroundErrorReason::MemtablaFlush);

	// A commit group fails after part of it was applied.
	tree.core.inner.error_handler.stop_commit_group(Error::DatabaseStopped(
		"a commit group failed after part of it was applied, and whether its commits took effect \
		 is decided by recovery when the database is opened again"
			.into(),
	));
	assert_stopped(&tree, "after the stop");

	retry.release();
	wait_until("the flush to finish", || immutables(&tree) == 0).await;
	tokio::time::sleep(GRACE).await;

	assert_eq!(table_ids(&tree), vec![1], "the flush that was running finished");
	assert!(
		!tree.core.inner.error_handler.recover(BackgroundErrorReason::MemtablaFlush),
		"the task ended the error of the flush"
	);
	assert_stopped(&tree, "after the flush");
	let commit = commit_one(Arc::clone(&tree), "after".into(), vec![b'a'; 40]).await;
	assert!(matches!(commit, Err(Error::DatabaseStopped(_))), "{commit:?}");

	// A memtable queued now is not flushed, by the caller or by the task.
	tree.core.inner.rotate_memtable().unwrap();
	assert_eq!(immutables(&tree), 1);
	assert!(matches!(tree.flush(), Err(Error::DatabaseStopped(_))));
	let checkpoint = TempDir::new("manifest_stop_in_flight_checkpoint").unwrap();
	assert!(matches!(
		tree.create_checkpoint(checkpoint.path().join("cp")),
		Err(Error::DatabaseStopped(_))
	));
	assert_eq!(table_ids(&tree), vec![1], "no flush ran after the stop");
	assert_eq!(immutables(&tree), 1, "the memtable queued after the stop is still queued");
	crash(&tree);
}

/// An oversized commit in the middle of a group flushes the memtable that holds the batches in
/// front of it, and the manifest write of that flush fails. The flush records no error of its own,
/// so the stop of the group is what every committer and every later commit gets, and the
/// background task, which retries the same flush, stops with it. The memtable stays queued, no
/// table is written however the manifest recovers, and a crash image recovers the group whole.
#[test(tokio::test)]
async fn a_failed_manifest_write_in_the_flush_before_a_direct_write_stops_the_database() {
	let dir = TempDir::new("manifest_stop_group").unwrap();
	let tree = open(dir.path(), SMALL);
	for result in commit_group(&tree, &base()).await {
		result.unwrap();
	}
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());
	fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, usize::MAX);

	let group = group_with_oversized();
	for (i, result) in commit_group(&tree, &group).await.iter().enumerate() {
		match result {
			Err(Error::DatabaseStopped(message)) => {
				assert!(message.contains("decided by recovery"), "commit {i}: {message}")
			}
			other => panic!("commit {i}: expected the database to be stopped, got {other:?}"),
		}
	}
	assert_stopped(&tree, "after the group");
	assert_eq!(immutables(&tree), 1, "the memtable that holds part of the group is queued");

	// The disk is back, and the background task is woken: whatever it would retry is refused.
	crate::levels::heal_replacements(&manifest);
	let segments = files_with_extension(dir.path(), "wal", ".wal");
	let held: u64 = segments
		.iter()
		.map(|name| fs::metadata(dir.path().join("wal").join(name)).unwrap().len())
		.sum();
	assert!(held > 0, "the segments hold the records of the group: {segments:?}");
	tree.core.task_manager.lock().unwrap().as_ref().unwrap().wake_up_memtable();
	tokio::time::sleep(Duration::from_millis(400)).await;

	assert_stopped(&tree, "after the wake");
	assert_eq!(immutables(&tree), 1, "the memtable is still queued");
	assert!(table_ids(&tree).is_empty(), "no table was written");
	assert_eq!(files_with_extension(dir.path(), "wal", ".wal"), segments, "no segment is removed");
	assert!(matches!(tree.flush(), Err(Error::DatabaseStopped(_))));
	let seen = keys(&tree);
	assert_eq!(seen.len(), 10, "readers see what they saw before the group: {seen:?}");

	// Close leaves the memtables to the WAL, and the reopened database holds the group whole.
	tree.close().await.unwrap();
	drop(tree);
	let reopened = open(dir.path(), SMALL);
	let mut expected: Vec<String> = base().into_iter().chain(group).map(|(key, _)| key).collect();
	expected.sort();
	assert_eq!(keys(&reopened), expected);
	reopened.close().await.unwrap();
}

/// Ends a tree the way a crash does: no close, no flush.
fn crash(tree: &Tree) {
	tree.core.is_closed.store(true, std::sync::atomic::Ordering::SeqCst);
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
}
