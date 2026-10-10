//! A failed manifest write in a memtable flush.
//!
//! The manifest is replaced as a whole: a temporary file is written and synced, renamed over the
//! manifest, and synced again together with its directory. A failure before the rename leaves the
//! old manifest in place and the in-memory one reverted, so the flush can simply be run again; the
//! background task retries it, and the success that follows ends the failure it recorded. A
//! failure from the rename on may have put the new manifest in place without making it durable,
//! so it stays fatal. Whoever else runs a flush (a checkpoint, `close`) hears of its failure and
//! does not stop the database.
//!
//! The failpoint is `levels::fail_replacements`, which fails the replacement of one manifest file
//! at either step.

use std::collections::BTreeMap;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;

use super::collect_transaction_all;
use crate::error::{BackgroundErrorReason, ErrorSeverity, Result};
use crate::levels::{fail_replacements, heal_replacements, ReplaceFailure};
use crate::lsm::FlushHook;
use crate::{Durability, Error, Mode, Options, Tree};

/// Large enough that nothing rotates unless a test rotates it.
const NEVER_ROTATING: usize = 16 * 1024 * 1024;

/// How long a test waits for something it expects to happen.
pub(super) const REACHED: Duration = Duration::from_secs(15);

/// How long a test waits for a call that is expected to return.
pub(super) const STUCK: Duration = Duration::from_secs(60);

/// How long a test lets the background task try again before it says that it did not.
pub(super) const GRACE: Duration = Duration::from_millis(600);

pub(super) type Contents = BTreeMap<Vec<u8>, Vec<u8>>;

fn key_of(i: usize) -> Vec<u8> {
	format!("key_{i:06}").into_bytes()
}

fn val_of(i: usize) -> Vec<u8> {
	let mut v = format!("val_{i:06}_").into_bytes();
	v.resize(100, b'x');
	v
}

fn open(path: &Path) -> Arc<Tree> {
	Arc::new(
		Tree::new(Arc::new(Options {
			path: path.to_path_buf(),
			max_memtable_size: NEVER_ROTATING,
			// A rotation by hand queues a memtable behind the one whose flush fails.
			memtable_stall_threshold: 1000,
			..Default::default()
		}))
		.unwrap(),
	)
}

/// Commits one key, durably.
async fn commit_one(tree: &Tree, i: usize) -> Result<()> {
	let mut txn = tree.begin()?;
	txn.set_durability(Durability::Immediate);
	txn.set(key_of(i), val_of(i))?;
	txn.commit().await
}

/// Commits the keys in `range`, 25 to a transaction, and returns what was written.
async fn put(tree: &Tree, range: Range<usize>) -> Contents {
	let mut written = Contents::new();
	let keys: Vec<usize> = range.collect();
	for chunk in keys.chunks(25) {
		let mut txn = tree.begin().unwrap();
		txn.set_durability(Durability::Immediate);
		for &i in chunk {
			txn.set(key_of(i), val_of(i)).unwrap();
			written.insert(key_of(i), val_of(i));
		}
		txn.commit().await.unwrap();
	}
	written
}

fn contents(tree: &Tree) -> Contents {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let all = collect_transaction_all(&mut txn.iter().unwrap()).unwrap();
	all.into_iter().collect()
}

fn assert_contents(tree: &Tree, expected: &Contents, what: &str) {
	let actual = contents(tree);
	assert_eq!(actual.len(), expected.len(), "{what}: number of keys");
	for (key, value) in expected {
		assert_eq!(actual.get(key), Some(value), "{what}: {}", String::from_utf8_lossy(key));
	}
}

pub(super) fn immutables(tree: &Tree) -> usize {
	tree.core.inner.immutable_count()
}

/// The ids of the tables the manifest lists, one per listing.
pub(super) fn table_ids(tree: &Tree) -> Vec<u64> {
	let mut ids: Vec<u64> =
		tree.core.inner.level_manifest.read().unwrap().iter().map(|t| t.id).collect();
	ids.sort_unstable();
	ids
}

fn log_number(tree: &Tree) -> u64 {
	tree.core.inner.level_manifest.read().unwrap().get_log_number()
}

pub(super) fn manifest_path(tree: &Tree) -> PathBuf {
	tree.core.inner.opts.manifest_file_path(0)
}

/// The files a failed replacement leaves next to the manifest.
pub(super) fn temp_files(manifest: &Path) -> Vec<String> {
	std::fs::read_dir(manifest.parent().unwrap())
		.unwrap()
		.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
		.filter(|name| name.starts_with(".tmp_"))
		.collect()
}

pub(super) fn copy_dir_all(src: &Path, dst: &Path) {
	std::fs::create_dir_all(dst).unwrap();
	for entry in std::fs::read_dir(src).unwrap() {
		let entry = entry.unwrap();
		if entry.file_name() == "LOCK" {
			continue;
		}
		let to = dst.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir_all(&entry.path(), &to);
		} else {
			std::fs::copy(entry.path(), &to).unwrap();
		}
	}
}

/// Rotates the active memtable into the queue and wakes the background flush.
pub(super) fn flush_in_background(tree: &Tree) {
	tree.core.inner.rotate_memtable().unwrap();
	tree.core.task_manager.lock().unwrap().as_ref().unwrap().wake_up_memtable();
}

/// Waits for `condition`, which the background task makes true.
pub(super) async fn wait_until(what: &str, condition: impl Fn() -> bool) {
	let start = Instant::now();
	while !condition() {
		assert!(start.elapsed() < REACHED, "timed out waiting for {what}");
		tokio::time::sleep(Duration::from_millis(5)).await;
	}
}

/// Counts the flushes that reach the point between the SST and the manifest.
pub(super) fn count_attempts(tree: &Tree) -> Arc<AtomicUsize> {
	let attempts = Arc::new(AtomicUsize::new(0));
	let counter = Arc::clone(&attempts);
	let hook: FlushHook = Arc::new(move |_| {
		counter.fetch_add(1, Ordering::SeqCst);
		Ok(())
	});
	*tree.core.inner.flush_hook.lock() = Some(hook);
	attempts
}

/// Parks the `nth` flush that reaches the point between the SST and the manifest.
pub(super) struct Parked {
	reached: mpsc::Receiver<()>,
	release: mpsc::Sender<()>,
}

pub(super) fn park_attempt(tree: &Tree, nth: usize) -> Parked {
	let (reached_tx, reached) = mpsc::channel();
	let (release, release_rx) = mpsc::channel();
	let release_rx = Mutex::new(release_rx);
	let attempts = AtomicUsize::new(0);
	let hook: FlushHook = Arc::new(move |_| {
		if attempts.fetch_add(1, Ordering::SeqCst) + 1 == nth {
			let _ = reached_tx.send(());
			// Bounded, so a broken test fails instead of hanging.
			let _ = release_rx.lock().unwrap().recv_timeout(REACHED);
		}
		Ok(())
	});
	*tree.core.inner.flush_hook.lock() = Some(hook);
	Parked {
		reached,
		release,
	}
}

impl Parked {
	pub(super) fn is_reached(&self) -> bool {
		self.reached.recv_timeout(REACHED).is_ok()
	}

	pub(super) fn release(&self) {
		let _ = self.release.send(());
	}
}

/// Removes the failpoint when the test ends, however it ends.
pub(super) struct Healed(pub(super) PathBuf);

impl Drop for Healed {
	fn drop(&mut self) {
		heal_replacements(&self.0);
	}
}

/// The error a commit gets while the database is stopped, which must say what stopped it.
async fn stopped_by(tree: &Tree) -> Error {
	let error = commit_one(tree, 1_000_000).await.expect_err("the database is stopped");
	assert!(
		error.to_string().to_lowercase().contains("manifest"),
		"the error does not say what stopped the database: {error}"
	);
	error
}

// ---------------------------------------------------------------------------
// the background flush
// ---------------------------------------------------------------------------

/// The first manifest write of the flush fails before the rename. While the retry has not run, the
/// database is stopped on the failure, and the memtable, its table and the manifest are as they
/// were. The retry succeeds, ends the failure and the database takes commits again; the table is
/// listed once, in the live tree, in a crash image and after a close and reopen.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_manifest_write_that_fails_once_is_retried_and_the_database_resumes() {
	let dir = TempDir::new("manifest_fail_once").unwrap();
	let images = TempDir::new("manifest_fail_once_image").unwrap();
	let tree = open(dir.path());
	let mut expected = put(&tree, 0..300).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());
	let log_before = log_number(&tree);

	// The retry is the second flush to reach the hook; it waits there for the test.
	let retry = park_attempt(&tree, 2);
	fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, 1);
	flush_in_background(&tree);
	assert!(retry.is_reached(), "the failed flush is retried");

	stopped_by(&tree).await;
	let failure = tree.core.inner.error_handler.get_error().unwrap();
	assert_eq!(failure.severity, ErrorSeverity::HardError);
	assert_eq!(immutables(&tree), 1, "the memtable is still queued");
	assert!(table_ids(&tree).is_empty(), "no table is listed");
	assert_eq!(log_number(&tree), log_before, "log_number did not move");
	assert!(temp_files(&manifest).is_empty(), "the failed write left a file behind");
	assert_contents(&tree, &expected, "the stopped tree");

	retry.release();
	wait_until("the error to clear", || tree.core.inner.error_handler.check_error().is_ok()).await;
	assert!(!tree.core.inner.error_handler.is_db_stopped());
	assert_eq!(immutables(&tree), 0);
	assert_eq!(table_ids(&tree), vec![1], "the table is listed once");
	assert!(log_number(&tree) > log_before);

	expected.extend(put(&tree, 300..400).await);
	assert_contents(&tree, &expected, "the tree after the retry");

	let image = images.path().join("crash");
	copy_dir_all(dir.path(), &image);
	let recovered = open(&image);
	assert_contents(&recovered, &expected, "the crash image");
	assert_eq!(table_ids(&recovered), vec![1], "the crash image lists the table once");
	recovered.close().await.unwrap();

	tree.close().await.unwrap();
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	assert_eq!(table_ids(&reopened), vec![1, 2], "one table per memtable");
	reopened.close().await.unwrap();
}

/// A manifest write that never succeeds: the flush keeps being retried, the database stays stopped
/// with an error that names the manifest, reads keep working, and `close` returns instead of
/// waiting for a retry. Every acknowledged commit is there after the disk is back and the
/// database reopened.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_manifest_write_that_keeps_failing_keeps_the_database_stopped_and_close_returns() {
	let dir = TempDir::new("manifest_fail_always").unwrap();
	let tree = open(dir.path());
	let expected = put(&tree, 0..300).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	let attempts = count_attempts(&tree);
	fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, usize::MAX);
	flush_in_background(&tree);
	wait_until("the flush to be retried", || attempts.load(Ordering::SeqCst) >= 3).await;

	stopped_by(&tree).await;
	assert!(tree.core.inner.error_handler.is_db_stopped());
	assert_eq!(immutables(&tree), 1);
	assert!(table_ids(&tree).is_empty());
	assert!(temp_files(&manifest).is_empty(), "the failed writes left files behind");
	assert_contents(&tree, &expected, "the stopped tree");

	let closed = tokio::time::timeout(STUCK, tree.close()).await.expect("close returns");
	assert!(closed.is_err(), "the shutdown flush cannot write the manifest either");
	drop(tree);

	heal_replacements(&manifest);
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	reopened.close().await.unwrap();
}

/// The failure is not a transient one: a failure at the rename or after it may have replaced the
/// manifest, so the database stops for good. The flush is not run again by the background task,
/// the in-memory manifest is as before the flush, and the directory, which may hold either
/// manifest, reopens with every commit and the table listed once.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_manifest_write_that_may_have_replaced_the_manifest_stays_fatal() {
	let dir = TempDir::new("manifest_fail_after").unwrap();
	let images = TempDir::new("manifest_fail_after_image").unwrap();
	let tree = open(dir.path());
	let expected = put(&tree, 0..300).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());
	let log_before = log_number(&tree);

	let attempts = count_attempts(&tree);
	fail_replacements(&manifest, ReplaceFailure::AfterRename, 0, 1);
	flush_in_background(&tree);
	wait_until("the failure to be recorded", || {
		tree.core.inner.error_handler.check_error().is_err()
	})
	.await;
	tokio::time::sleep(GRACE).await;

	let failure = tree.core.inner.error_handler.get_error().unwrap();
	assert_eq!(failure.severity, ErrorSeverity::FatalError);
	stopped_by(&tree).await;
	assert_eq!(attempts.load(Ordering::SeqCst), 1, "a fatal failure is not retried");
	assert_eq!(immutables(&tree), 1);
	assert!(table_ids(&tree).is_empty(), "the in-memory manifest is as it was");
	assert_eq!(log_number(&tree), log_before);
	assert_contents(&tree, &expected, "the stopped tree");

	// The renamed manifest is on disk: a crash now leaves the table listed, with its data.
	let image = images.path().join("crash");
	copy_dir_all(dir.path(), &image);
	let recovered = open(&image);
	assert_contents(&recovered, &expected, "the crash image");
	assert_eq!(table_ids(&recovered), vec![1], "the crash image lists the table once");
	recovered.close().await.unwrap();

	tree.close().await.unwrap();
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	assert_eq!(table_ids(&reopened), vec![1], "the table is listed once");
	reopened.close().await.unwrap();
}

/// An error that stopped the database and is not the flush's stays when the flush recovers: one
/// recorded before the flush failed, however severe it is, and one that came while it was failing.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_flush_that_recovers_does_not_clear_another_failure() {
	let dir = TempDir::new("manifest_fail_other").unwrap();
	let tree = open(dir.path());
	let mut expected = put(&tree, 0..200).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());
	let handler = Arc::clone(&tree.core.inner.error_handler);
	let hard = || Error::Other("injected compaction failure".into());
	let fatal = || Error::Io(Arc::new(std::io::Error::other("injected compaction failure")));

	// Each round: a compaction failure of the given kind comes `first`, or while the flush is
	// failing, and the flush recovers.
	for (round, (error, first)) in
		[(hard as fn() -> Error, true), (fatal, true), (hard, false), (fatal, false)]
			.into_iter()
			.enumerate()
	{
		handler.clear_error();
		expected.extend(put(&tree, 200 * (round + 1)..200 * (round + 2)).await);
		if first {
			handler.set_error(error(), BackgroundErrorReason::Compaction);
		}
		let retry = park_attempt(&tree, 2);
		fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, 1);
		flush_in_background(&tree);
		assert!(retry.is_reached(), "round {round}: the failed flush is retried");
		if !first {
			assert_eq!(handler.get_error().unwrap().reason, BackgroundErrorReason::MemtablaFlush);
			handler.set_error(error(), BackgroundErrorReason::Compaction);
		}
		retry.release();
		wait_until("the flush to succeed", || immutables(&tree) == 0).await;
		tokio::time::sleep(GRACE).await;

		let failure = handler.get_error().expect("the compaction failure is still there");
		assert_eq!(failure.reason, BackgroundErrorReason::Compaction, "round {round}");
		assert!(handler.check_error().is_err(), "round {round}: the database stays stopped");
	}
	assert_contents(&tree, &expected, "the stopped tree");

	tree.close().await.unwrap();
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	reopened.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// the flush a checkpoint runs
// ---------------------------------------------------------------------------

/// A checkpoint whose flush cannot write the manifest returns the error and leaves the database
/// running: nothing is recorded, commits go on, the memtable is still queued and the retry of the
/// checkpoint succeeds. A crash image taken meanwhile and the reopened tree hold everything.
async fn a_checkpoint_that_cannot_write_the_manifest_does_not_stop_the_database(
	at: ReplaceFailure,
) {
	let dir = TempDir::new("manifest_fail_checkpoint").unwrap();
	let images = TempDir::new("manifest_fail_checkpoint_image").unwrap();
	let out = TempDir::new("manifest_fail_checkpoint_out").unwrap();
	let tree = open(dir.path());
	let mut expected = put(&tree, 0..300).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());
	let log_before = log_number(&tree);

	let cp = out.path().join("cp");
	fail_replacements(&manifest, at, 0, 1);
	let failed = {
		let (tree, cp) = (Arc::clone(&tree), cp.clone());
		tokio::task::spawn_blocking(move || tree.create_checkpoint(&cp)).await.unwrap()
	};
	assert!(failed.is_err(), "{at:?}: the checkpoint reports the failure");
	assert!(
		tree.core.inner.error_handler.check_error().is_ok(),
		"{at:?}: the failure of a checkpoint stops nothing"
	);
	assert!(!tree.core.inner.error_handler.is_db_stopped());
	assert_eq!(immutables(&tree), 1, "{at:?}: the memtable is still queued");
	assert!(table_ids(&tree).is_empty(), "{at:?}: the in-memory manifest is as it was");
	assert_eq!(log_number(&tree), log_before);
	assert_contents(&tree, &expected, "the tree after the failed checkpoint");

	expected.extend(put(&tree, 300..400).await);
	assert_contents(&tree, &expected, "the tree takes commits");

	let image = images.path().join("crash");
	copy_dir_all(dir.path(), &image);
	let recovered = open(&image);
	assert_contents(&recovered, &expected, "the crash image");
	recovered.close().await.unwrap();

	let metadata = {
		let (tree, cp) = (Arc::clone(&tree), cp.clone());
		tokio::task::spawn_blocking(move || tree.create_checkpoint(&cp)).await.unwrap().unwrap()
	};
	assert!(metadata.is_compatible());
	assert_eq!(immutables(&tree), 0);
	assert_eq!(table_ids(&tree), vec![1, 2], "{at:?}: one table per memtable");
	let copy = open(&cp);
	assert_contents(&copy, &expected, "the checkpoint");
	copy.close().await.unwrap();

	tree.close().await.unwrap();
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	reopened.close().await.unwrap();
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_that_fails_before_the_rename_does_not_stop_the_database() {
	a_checkpoint_that_cannot_write_the_manifest_does_not_stop_the_database(
		ReplaceFailure::BeforeRename,
	)
	.await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_that_fails_after_the_rename_does_not_stop_the_database() {
	a_checkpoint_that_cannot_write_the_manifest_does_not_stop_the_database(
		ReplaceFailure::AfterRename,
	)
	.await;
}
