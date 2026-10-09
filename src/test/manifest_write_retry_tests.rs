//! The failed manifest write of a memtable flush, in the interleavings around it.
//!
//! `manifest_write_failure_tests` has the scenarios of the policy. These run it against the
//! things that can overlap or follow it: committers racing the retry, a close, a checkpoint or a
//! crash image meeting it, memtables queued behind the one that fails, other causes of a failed
//! flush, and the steps of the manifest replacement that fail without being injected.

use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;

use super::collect_transaction_all;
use super::manifest_write_failure_tests::{
	copy_dir_all,
	count_attempts,
	flush_in_background,
	immutables,
	manifest_path,
	park_attempt,
	table_ids,
	temp_files,
	wait_until,
	Contents,
	Healed,
	GRACE,
	REACHED,
	STUCK,
};
use crate::error::{BackgroundErrorReason, ErrorSeverity, Result};
use crate::levels::{fail_replacements, heal_replacements, ReplaceFailure};
use crate::lsm::FlushHook;
use crate::{Durability, Error, Mode, Options, Tree};

const NEVER_ROTATING: usize = 16 * 1024 * 1024;

fn key_of(i: usize) -> Vec<u8> {
	format!("key_{i:06}").into_bytes()
}

fn val_of(i: usize, version: usize) -> Vec<u8> {
	let mut v = format!("val_{version}_{i:06}_").into_bytes();
	v.resize(120, b'x');
	v
}

fn options(path: &Path) -> Options {
	Options {
		path: path.to_path_buf(),
		max_memtable_size: NEVER_ROTATING,
		memtable_stall_threshold: 1000,
		level0_max_files: 1000,
		l0_stall_threshold: 2000,
		..Default::default()
	}
}

fn open(path: &Path) -> Arc<Tree> {
	open_with(options(path))
}

fn open_with(opts: Options) -> Arc<Tree> {
	Arc::new(Tree::new(Arc::new(opts)).unwrap())
}

async fn commit_one(tree: &Tree, i: usize) -> Result<()> {
	let mut txn = tree.begin()?;
	txn.set_durability(Durability::Immediate);
	txn.set(key_of(i), val_of(i, 0))?;
	txn.commit().await
}

async fn put_version(tree: &Tree, range: Range<usize>, version: usize) -> Contents {
	let mut written = Contents::new();
	let keys: Vec<usize> = range.collect();
	for chunk in keys.chunks(25) {
		let mut txn = tree.begin().unwrap();
		txn.set_durability(Durability::Immediate);
		for &i in chunk {
			txn.set(key_of(i), val_of(i, version)).unwrap();
			written.insert(key_of(i), val_of(i, version));
		}
		txn.commit().await.unwrap();
	}
	written
}

async fn put(tree: &Tree, range: Range<usize>) -> Contents {
	put_version(tree, range, 0).await
}

fn scan(tree: &Tree) -> Vec<(Vec<u8>, Vec<u8>)> {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let all = collect_transaction_all(&mut txn.iter().unwrap()).unwrap();
	all
}

fn assert_contents(tree: &Tree, expected: &Contents, what: &str) {
	let all = scan(tree);
	assert_eq!(all.len(), expected.len(), "{what}: number of entries (a key may appear twice)");
	let actual: Contents = all.into_iter().collect();
	assert_eq!(actual.len(), expected.len(), "{what}: number of keys");
	for (key, value) in expected {
		assert_eq!(actual.get(key), Some(value), "{what}: {}", String::from_utf8_lossy(key));
	}
}

fn assert_unique(ids: &[u64], what: &str) {
	let mut dedup = ids.to_vec();
	dedup.dedup();
	assert_eq!(dedup.len(), ids.len(), "{what}: a table is listed twice: {ids:?}");
}

fn rotate(tree: &Tree) {
	tree.core.inner.rotate_memtable().unwrap();
}

fn wake(tree: &Tree) {
	tree.core.task_manager.lock().unwrap().as_ref().unwrap().wake_up_memtable();
}

fn error_is_clear(tree: &Tree) -> bool {
	tree.core.inner.error_handler.check_error().is_ok()
}

/// Committers keep going through a run of failed manifest writes while memtables rotate under
/// them and writers stall on the queue. Every acknowledged commit is in the live tree and after a
/// reopen; no table is listed twice; nothing hangs.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn concurrent_committers_ride_out_transient_manifest_failures() {
	let dir = TempDir::new("manifest_retry_concurrent").unwrap();
	let mut opts = options(dir.path());
	opts.max_memtable_size = 64 * 1024;
	opts.memtable_stall_threshold = 2;
	let tree = open_with(opts);
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	// The first flush gets through; the next three manifest writes fail.
	fail_replacements(&manifest, ReplaceFailure::BeforeRename, 1, 3);

	let mut tasks = Vec::new();
	for t in 0..4usize {
		let tree = Arc::clone(&tree);
		tasks.push(tokio::spawn(async move {
			let mut acked = Contents::new();
			for n in 0..150usize {
				let i = t * 1000 + n;
				let start = Instant::now();
				loop {
					let mut txn = tree.begin().unwrap();
					txn.set_durability(if t % 2 == 0 {
						Durability::Immediate
					} else {
						Durability::Eventual
					});
					txn.set(key_of(i), val_of(i, 0)).unwrap();
					match txn.commit().await {
						Ok(()) => {
							acked.insert(key_of(i), val_of(i, 0));
							break;
						}
						Err(_) => {
							assert!(start.elapsed() < STUCK, "a commit never got through");
							tokio::time::sleep(Duration::from_millis(3)).await;
						}
					}
				}
			}
			acked
		}));
	}
	let mut expected = Contents::new();
	for task in tasks {
		expected
			.extend(tokio::time::timeout(STUCK, task).await.expect("committers finish").unwrap());
	}
	assert_eq!(expected.len(), 600);

	wait_until("the queue to drain", || immutables(&tree) == 0).await;
	wait_until("the error to clear", || error_is_clear(&tree)).await;
	assert_contents(&tree, &expected, "the live tree");
	assert_unique(&table_ids(&tree), "the live tree");

	tree.close().await.unwrap();
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	assert_unique(&table_ids(&reopened), "the reopened tree");
	reopened.close().await.unwrap();
}

/// The retry on a current-thread runtime, where the flush task and the committers share one thread.
#[test(tokio::test(flavor = "current_thread"))]
async fn a_failed_manifest_write_is_retried_on_a_current_thread_runtime() {
	let dir = TempDir::new("manifest_retry_current_thread").unwrap();
	let tree = open(dir.path());
	let mut expected = put(&tree, 0..200).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	let attempts = count_attempts(&tree);
	fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, 2);
	flush_in_background(&tree);
	wait_until("the flush to succeed", || immutables(&tree) == 0).await;
	wait_until("the error to clear", || error_is_clear(&tree)).await;
	// Two failed attempts and the one that succeeded each reach the hook.
	assert_eq!(attempts.load(Ordering::SeqCst), 3);

	expected.extend(put(&tree, 200..250).await);
	assert_contents(&tree, &expected, "the live tree");
	assert_eq!(table_ids(&tree), vec![1]);
	tokio::time::timeout(STUCK, tree.close()).await.expect("close returns").unwrap();
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	reopened.close().await.unwrap();
}

/// After a fatal (uncertain) failure, a checkpoint whose flush succeeds drains the queue: the table
/// is listed once, the copy holds everything and so does a reopen. The database stays stopped.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn an_uncertain_failure_then_a_checkpoint_keeps_every_commit() {
	let dir = TempDir::new("manifest_retry_uncertain_ckpt").unwrap();
	let out = TempDir::new("manifest_retry_uncertain_ckpt_out").unwrap();
	let tree = open(dir.path());
	let expected = put(&tree, 0..300).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	fail_replacements(&manifest, ReplaceFailure::AfterRename, 0, 1);
	flush_in_background(&tree);
	wait_until("the failure to be recorded", || !error_is_clear(&tree)).await;
	tokio::time::sleep(GRACE).await;
	assert_eq!(
		tree.core.inner.error_handler.get_error().unwrap().severity,
		ErrorSeverity::FatalError
	);

	let cp = out.path().join("cp");
	let made = {
		let (tree, cp) = (Arc::clone(&tree), cp.clone());
		tokio::task::spawn_blocking(move || tree.create_checkpoint(&cp)).await.unwrap()
	};
	made.expect("the checkpoint flush succeeds once the failpoint is spent");
	assert_eq!(immutables(&tree), 0);
	assert_eq!(table_ids(&tree), vec![1], "the table is listed once");
	assert!(!error_is_clear(&tree), "a success of the flush does not clear a fatal failure");
	assert!(commit_one(&tree, 9_999).await.is_err());

	let copy = open(&cp);
	assert_contents(&copy, &expected, "the checkpoint copy");
	copy.close().await.unwrap();

	tree.close().await.unwrap();
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	assert_eq!(table_ids(&reopened), vec![1]);
	reopened.close().await.unwrap();
}

/// A checkpoint that drains the queue while the retry is failing lets the retry find nothing to
/// do; that counts as a success, so the database resumes.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_that_drains_the_queue_lets_the_retry_recover() {
	let dir = TempDir::new("manifest_retry_ckpt_drains").unwrap();
	let out = TempDir::new("manifest_retry_ckpt_drains_out").unwrap();
	let tree = open(dir.path());
	let mut expected = put(&tree, 0..300).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	let attempts = count_attempts(&tree);
	fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, usize::MAX);
	flush_in_background(&tree);
	wait_until("two failed attempts", || {
		// The hook fires before the manifest write, so it counts the attempts.
		attempts.load(Ordering::SeqCst) >= 2
	})
	.await;
	assert!(!error_is_clear(&tree));

	heal_replacements(&manifest);
	let cp = out.path().join("cp");
	{
		let (tree, cp) = (Arc::clone(&tree), cp.clone());
		tokio::task::spawn_blocking(move || tree.create_checkpoint(&cp)).await.unwrap().unwrap();
	}
	wait_until("the error to clear", || error_is_clear(&tree)).await;
	assert_eq!(immutables(&tree), 0);
	expected.extend(put(&tree, 300..350).await);
	assert_contents(&tree, &expected, "the live tree");
	assert_unique(&table_ids(&tree), "the live tree");
	tree.close().await.unwrap();
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	assert_unique(&table_ids(&reopened), "the reopened tree");
	reopened.close().await.unwrap();
}

/// A crash image cut while the retry is parked between its SST and its manifest write holds a
/// finished SST that no manifest lists. Reopening it loses nothing, and it keeps working: more
/// writes, a flush, another reopen.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_crash_image_cut_while_the_retry_is_parked_reopens_whole() {
	let dir = TempDir::new("manifest_retry_image_parked").unwrap();
	let images = TempDir::new("manifest_retry_image_parked_img").unwrap();
	let tree = open(dir.path());
	let expected = put(&tree, 0..300).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	let retry = park_attempt(&tree, 2);
	fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, 1);
	flush_in_background(&tree);
	assert!(retry.is_reached());

	let image = images.path().join("crash");
	copy_dir_all(dir.path(), &image);
	retry.release();
	wait_until("the error to clear", || error_is_clear(&tree)).await;

	let recovered = open(&image);
	assert_contents(&recovered, &expected, "the image");
	let mut all = expected.clone();
	all.extend(put(&recovered, 300..400).await);
	rotate(&recovered);
	recovered.core.inner.flush_all_immutables_sync().unwrap();
	assert_unique(&table_ids(&recovered), "the image after a flush");
	recovered.close().await.unwrap();
	let again = open(&image);
	assert_contents(&again, &all, "the image reopened");
	assert_unique(&table_ids(&again), "the image reopened");
	again.close().await.unwrap();

	tree.close().await.unwrap();
}

/// A crash image cut after an uncertain failure holds a manifest that lists the table. It reopens
/// with every commit, and it keeps working like the parked one.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_crash_image_cut_after_an_uncertain_failure_keeps_working() {
	let dir = TempDir::new("manifest_retry_image_uncertain").unwrap();
	let images = TempDir::new("manifest_retry_image_uncertain_img").unwrap();
	let tree = open(dir.path());
	let expected = put(&tree, 0..300).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	fail_replacements(&manifest, ReplaceFailure::AfterRename, 0, 1);
	flush_in_background(&tree);
	wait_until("the failure to be recorded", || !error_is_clear(&tree)).await;

	let image = images.path().join("crash");
	copy_dir_all(dir.path(), &image);

	let recovered = open(&image);
	assert_contents(&recovered, &expected, "the image");
	assert_unique(&table_ids(&recovered), "the image");
	let mut all = expected.clone();
	all.extend(put(&recovered, 300..400).await);
	rotate(&recovered);
	recovered.core.inner.flush_all_immutables_sync().unwrap();
	all.extend(put(&recovered, 400..450).await);
	assert_unique(&table_ids(&recovered), "the image after a flush");
	recovered.close().await.unwrap();
	let again = open(&image);
	assert_contents(&again, &all, "the image reopened");
	assert_unique(&table_ids(&again), "the image reopened");
	again.close().await.unwrap();

	tree.close().await.unwrap();
}

async fn overwrites_across_memtables(at: ReplaceFailure) {
	let dir = TempDir::new("manifest_retry_order").unwrap();
	let images = TempDir::new("manifest_retry_order_img").unwrap();
	let tree = open(dir.path());
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	put_version(&tree, 0..100, 1).await;
	rotate(&tree);
	put_version(&tree, 0..100, 2).await;
	rotate(&tree);
	let expected = put_version(&tree, 0..100, 3).await;
	assert_eq!(immutables(&tree), 2);

	// The first flush gets through, the second fails.
	fail_replacements(&manifest, at, 1, 1);
	wake(&tree);
	if at == ReplaceFailure::BeforeRename {
		wait_until("the queue to drain", || immutables(&tree) == 0).await;
		wait_until("the error to clear", || error_is_clear(&tree)).await;
		assert_eq!(table_ids(&tree), vec![1, 2]);
		assert_contents(&tree, &expected, "the live tree");
	} else {
		wait_until("the failure to be recorded", || !error_is_clear(&tree)).await;
		tokio::time::sleep(GRACE).await;
		assert_eq!(immutables(&tree), 1, "the second memtable is still queued");
		assert_eq!(table_ids(&tree), vec![1]);
		assert_contents(&tree, &expected, "the stopped tree");

		let image = images.path().join("crash");
		copy_dir_all(dir.path(), &image);
		let recovered = open(&image);
		assert_contents(&recovered, &expected, "the crash image");
		assert_unique(&table_ids(&recovered), "the crash image");
		recovered.close().await.unwrap();
	}

	tree.close().await.unwrap();
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	assert_eq!(table_ids(&reopened), vec![1, 2, 3], "one table per memtable");
	reopened.close().await.unwrap();
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_failed_middle_flush_keeps_the_order_of_overwrites() {
	overwrites_across_memtables(ReplaceFailure::BeforeRename).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn an_uncertain_middle_flush_keeps_the_order_of_overwrites() {
	overwrites_across_memtables(ReplaceFailure::AfterRename).await;
}

/// After an uncertain failure the manifest on disk may list a table whose memtable is still
/// queued. The flush that comes next first writes the manifest in memory over it, and only then
/// the table: if that write fails, the table's file is left as it is, so that a crash cannot find
/// it cut short under a manifest that lists it. A wake of the background task is such a flush; it
/// does not start a retry loop under the fatal error, and when it succeeds the error stays.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_flush_after_an_uncertain_failure_settles_the_manifest_before_it_writes_the_table() {
	let dir = TempDir::new("manifest_retry_settle").unwrap();
	let out = TempDir::new("manifest_retry_settle_out").unwrap();
	let tree = open(dir.path());
	let expected = put(&tree, 0..300).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	let attempts = count_attempts(&tree);
	fail_replacements(&manifest, ReplaceFailure::AfterRename, 0, 1);
	flush_in_background(&tree);
	wait_until("the failure to be recorded", || !error_is_clear(&tree)).await;
	tokio::time::sleep(GRACE).await;
	assert_eq!(attempts.load(Ordering::SeqCst), 1);

	// The manifest on disk lists the table, whose file is complete.
	let listed = std::fs::read(&manifest).unwrap();
	let sst = tree.core.inner.opts.sstable_file_path(1);
	let before = std::fs::metadata(&sst).unwrap();
	let unchanged = || {
		let after = std::fs::metadata(&sst).unwrap();
		after.len() == before.len() && after.modified().unwrap() == before.modified().unwrap()
	};

	// A wake while the disk fails again stops at the manifest: the table is not written, and the
	// task does not retry, since the error on record is fatal.
	fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, usize::MAX);
	wake(&tree);
	tokio::time::sleep(GRACE).await;
	heal_replacements(&manifest);
	tokio::time::sleep(Duration::from_secs(1)).await;
	assert_eq!(attempts.load(Ordering::SeqCst), 1, "no flush got as far as the table");
	assert!(unchanged(), "the table was rewritten under a manifest that may list it");
	assert_eq!(std::fs::read(&manifest).unwrap(), listed);
	assert_eq!(immutables(&tree), 1, "the failed wake is not retried");

	// The write of the manifest fails once more, now in a checkpoint, which reports it.
	fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, 1);
	let cp = out.path().join("cp");
	let failed = {
		let (tree, cp) = (Arc::clone(&tree), cp.clone());
		tokio::task::spawn_blocking(move || tree.create_checkpoint(&cp)).await.unwrap()
	};
	assert!(failed.is_err());
	assert!(unchanged());
	assert_eq!(attempts.load(Ordering::SeqCst), 1);

	// When the table is written, the manifest on disk is no longer the one that lists it.
	let seen = Arc::new(Mutex::new(None));
	let hook: FlushHook = {
		let (seen, manifest) = (Arc::clone(&seen), manifest.clone());
		Arc::new(move |_| {
			*seen.lock().unwrap() = Some(std::fs::read(&manifest).unwrap());
			Ok(())
		})
	};
	*tree.core.inner.flush_hook.lock() = Some(hook);
	let cp = out.path().join("cp_ok");
	{
		let (tree, cp) = (Arc::clone(&tree), cp.clone());
		tokio::task::spawn_blocking(move || tree.create_checkpoint(&cp)).await.unwrap().unwrap();
	}
	let seen = seen.lock().unwrap().take().expect("the flush reached the table");
	assert_ne!(seen, listed, "the table was written before the manifest was settled");
	assert_eq!(table_ids(&tree), vec![1], "the table is listed once");
	assert!(!error_is_clear(&tree), "a success of the flush does not clear a fatal failure");

	let copy = open(&cp);
	assert_contents(&copy, &expected, "the checkpoint copy");
	copy.close().await.unwrap();
	tree.close().await.unwrap();
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	assert_eq!(table_ids(&reopened), vec![1]);
	reopened.close().await.unwrap();
}

/// A flush whose SST cannot be written is retried and recovers when the obstacle goes.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn an_sst_write_failure_is_retried_too() {
	let dir = TempDir::new("manifest_retry_sst_obstacle").unwrap();
	let tree = open(dir.path());
	let mut expected = put(&tree, 0..200).await;

	rotate(&tree);
	let id = tree.core.inner.immutable_memtables.read().unwrap().first().unwrap().table_id;
	let obstacle = tree.core.inner.opts.sstable_file_path(id);
	std::fs::create_dir_all(&obstacle).unwrap();
	wake(&tree);
	wait_until("the failure to be recorded", || !error_is_clear(&tree)).await;
	tokio::time::sleep(GRACE).await;
	assert_eq!(
		tree.core.inner.error_handler.get_error().unwrap().severity,
		ErrorSeverity::HardError
	);
	assert!(commit_one(&tree, 9_999).await.is_err());
	assert_eq!(immutables(&tree), 1);

	std::fs::remove_dir_all(&obstacle).unwrap();
	wait_until("the error to clear", || error_is_clear(&tree)).await;
	assert_eq!(immutables(&tree), 0);
	expected.extend(put(&tree, 200..250).await);
	assert_contents(&tree, &expected, "the live tree");
	tree.close().await.unwrap();
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	assert_unique(&table_ids(&reopened), "the reopened tree");
	reopened.close().await.unwrap();
}

/// A flush whose value-log fsync fails is retried, and what it wrote is there after a reopen.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_value_log_sync_failure_in_the_flush_is_retried() {
	let dir = TempDir::new("manifest_retry_vlog_sync").unwrap();
	let mut opts = options(dir.path());
	opts.enable_vlog = true;
	opts.vlog_value_threshold = 64;
	let tree = open_with(opts);
	let mut expected = put(&tree, 0..200).await;

	rotate(&tree);
	let errors_before = tree.core.inner.error_handler.error_count();
	tree.core.inner.vlog.as_ref().expect("the value log is on").fail_next_sync();
	wake(&tree);
	wait_until("the failure to be recorded", || {
		tree.core.inner.error_handler.error_count() > errors_before
	})
	.await;
	wait_until("the error to clear", || error_is_clear(&tree)).await;
	wait_until("the queue to drain", || immutables(&tree) == 0).await;
	expected.extend(put(&tree, 200..250).await);
	assert_contents(&tree, &expected, "the live tree");
	tree.close().await.unwrap();
	let reopened = {
		let mut opts = options(dir.path());
		opts.enable_vlog = true;
		opts.vlog_value_threshold = 64;
		open_with(opts)
	};
	assert_contents(&reopened, &expected, "the reopened tree");
	reopened.close().await.unwrap();
}

/// A writer stalled on the memtable queue is let go with an error when the flush that would free
/// it fails, and writers after the recovery go through.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_writer_stalled_on_the_queue_is_released_by_a_failed_flush() {
	let dir = TempDir::new("manifest_retry_stalled").unwrap();
	let mut opts = options(dir.path());
	opts.memtable_stall_threshold = 2;
	let tree = open_with(opts);
	let mut expected = put(&tree, 0..100).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	let first = park_attempt(&tree, 1);
	fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, 1);
	flush_in_background(&tree);
	assert!(first.is_reached());
	// A second memtable queues behind the one being flushed: the queue is at its limit.
	expected.extend(put(&tree, 100..150).await);
	rotate(&tree);
	assert_eq!(immutables(&tree), 2);

	let writer = {
		let tree = Arc::clone(&tree);
		tokio::spawn(async move { commit_one(&tree, 5_000).await })
	};
	wait_until("the writer to stall", || tree.core.write_stall.is_stalled()).await;
	first.release();

	let result =
		tokio::time::timeout(STUCK, writer).await.expect("the writer is released").unwrap();
	assert!(
		matches!(result, Err(Error::PipelineStall)),
		"a stalled writer is released with an error: {result:?}"
	);
	wait_until("the error to clear", || error_is_clear(&tree)).await;
	wait_until("the queue to drain", || immutables(&tree) == 0).await;
	commit_one(&tree, 5_001).await.unwrap();
	expected.insert(key_of(5_001), val_of(5_001, 0));
	assert_contents(&tree, &expected, "the live tree");
	assert_unique(&table_ids(&tree), "the live tree");
	tree.close().await.unwrap();
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	reopened.close().await.unwrap();
}

/// A retry that fails the fatal way ends the retrying, replaces the ordinary failure it follows,
/// and leaves the database stopped, with no writer left waiting.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_retry_that_turns_fatal_ends_the_retrying_and_keeps_the_database_stopped() {
	let dir = TempDir::new("manifest_retry_turns_fatal").unwrap();
	let tree = open(dir.path());
	let expected = put(&tree, 0..300).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	let attempts = Arc::new(AtomicUsize::new(0));
	let (reached_tx, reached) = mpsc::channel();
	let (release, release_rx) = mpsc::channel::<()>();
	let release_rx = Mutex::new(release_rx);
	{
		let attempts = Arc::clone(&attempts);
		let manifest = manifest.clone();
		let hook: FlushHook = Arc::new(move |_| {
			if attempts.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
				// The retry: the disk fails the other way now.
				fail_replacements(&manifest, ReplaceFailure::AfterRename, 0, 1);
				let _ = reached_tx.send(());
				let _ = release_rx.lock().unwrap().recv_timeout(REACHED);
			}
			Ok(())
		});
		*tree.core.inner.flush_hook.lock() = Some(hook);
	}
	fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, 1);
	flush_in_background(&tree);
	reached.recv_timeout(REACHED).expect("the retry runs");
	assert_eq!(
		tree.core.inner.error_handler.get_error().unwrap().severity,
		ErrorSeverity::HardError
	);
	let _ = release.send(());

	wait_until("the failure to turn fatal", || {
		tree.core.inner.error_handler.get_error().is_some_and(|e| {
			e.severity == ErrorSeverity::FatalError
				&& e.reason == BackgroundErrorReason::MemtablaFlush
		})
	})
	.await;
	tokio::time::sleep(GRACE).await;
	assert_eq!(attempts.load(Ordering::SeqCst), 2, "a fatal failure is not retried");
	assert!(commit_one(&tree, 9_999).await.is_err());
	assert_eq!(immutables(&tree), 1);
	assert!(table_ids(&tree).is_empty());
	assert_contents(&tree, &expected, "the stopped tree");

	tree.close().await.unwrap();
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	assert_eq!(table_ids(&reopened), vec![1]);
	reopened.close().await.unwrap();
}

/// Failures that come and go: every one is recorded, every success ends it, and nothing is lost.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn repeated_failures_and_recoveries_lose_nothing() {
	let dir = TempDir::new("manifest_retry_flaps").unwrap();
	let tree = open(dir.path());
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());
	let mut expected = Contents::new();

	for cycle in 0..6usize {
		expected.extend(put_version(&tree, cycle * 50..cycle * 50 + 50, cycle).await);
		let errors_before = tree.core.inner.error_handler.error_count();
		fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, 1 + cycle % 3);
		flush_in_background(&tree);
		wait_until("the failure to be recorded", || {
			tree.core.inner.error_handler.error_count() > errors_before
		})
		.await;
		wait_until("the queue to drain", || immutables(&tree) == 0).await;
		wait_until("the error to clear", || error_is_clear(&tree)).await;
	}
	assert_eq!(table_ids(&tree), vec![1, 2, 3, 4, 5, 6]);
	assert_contents(&tree, &expected, "the live tree");
	tree.close().await.unwrap();
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	assert_eq!(table_ids(&reopened), vec![1, 2, 3, 4, 5, 6]);
	reopened.close().await.unwrap();
}

/// `close` does not wait out the pause between two attempts of a flush that keeps failing: after
/// enough failures the pause is long, and the shutdown must cut it short.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_cuts_short_the_wait_between_two_attempts() {
	let dir = TempDir::new("manifest_retry_close_wait").unwrap();
	let tree = open(dir.path());
	put(&tree, 0..200).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	let attempts = count_attempts(&tree);
	fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, usize::MAX);
	flush_in_background(&tree);
	// The waits so far add up to 50 + 100 + 200 + 400 + 800 ms, and the next one is 1.6 s.
	wait_until("six failed attempts", || attempts.load(Ordering::SeqCst) >= 6).await;

	let started = Instant::now();
	let closed = tokio::time::timeout(STUCK, tree.close()).await.expect("close returns");
	let took = started.elapsed();
	assert!(closed.is_err());
	assert!(took < Duration::from_millis(900), "close waited out the pause: {took:?}");
}

/// A close that races the retry loop at every point of it loses nothing: it may land during an
/// attempt, in a pause, or after the success.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_close_racing_the_retry_loses_nothing() {
	for round in 0..8u64 {
		let dir = TempDir::new("manifest_retry_close_race").unwrap();
		let tree = open(dir.path());
		let expected = put(&tree, 0..120).await;
		let manifest = manifest_path(&tree);
		let _healed = Healed(manifest.clone());

		fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, 1 + (round % 3) as usize);
		flush_in_background(&tree);
		tokio::time::sleep(Duration::from_millis(round * 27)).await;
		let closed = tokio::time::timeout(STUCK, tree.close()).await.expect("close returns");
		heal_replacements(&manifest);
		// The shutdown flush may hit a failure that is left, which is for it to report.
		let _ = closed;
		drop(tree);

		let reopened = open(dir.path());
		assert_contents(&reopened, &expected, &format!("round {round}: the reopened tree"));
		assert_unique(&table_ids(&reopened), "the reopened tree");
		reopened.close().await.unwrap();
	}
}

/// A tree dropped while its flush keeps failing is closed by the drop; the directory opens again
/// once the disk is back, and holds every commit.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_tree_dropped_while_the_flush_keeps_failing_releases_the_directory() {
	let dir = TempDir::new("manifest_retry_drop").unwrap();
	let tree = open(dir.path());
	let expected = put(&tree, 0..200).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	let attempts = count_attempts(&tree);
	fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, usize::MAX);
	flush_in_background(&tree);
	wait_until("two failed attempts", || attempts.load(Ordering::SeqCst) >= 2).await;
	drop(tree);
	// The drop's close cannot flush the memtable while the disk fails.
	tokio::time::sleep(GRACE).await;
	heal_replacements(&manifest);

	let start = Instant::now();
	let reopened = loop {
		match Tree::new(Arc::new(options(dir.path()))) {
			Ok(tree) => break Arc::new(tree),
			Err(e) => {
				assert!(start.elapsed() < REACHED, "the directory stays locked: {e}");
				tokio::time::sleep(Duration::from_millis(20)).await;
			}
		}
	};
	assert_contents(&reopened, &expected, "the reopened tree");
	reopened.close().await.unwrap();
}

/// A rename that really fails (a directory stands where the manifest is) is not retried: the
/// error is the uncertain kind, fatal, and the failed replacement leaves no file behind. The
/// memtable stays queued, the in-memory manifest is as it was, and once the manifest is back the
/// shutdown flush writes it and a reopen finds every commit.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_rename_that_fails_stays_fatal_and_leaves_no_file() {
	let dir = TempDir::new("manifest_rename_fails").unwrap();
	let tree = open(dir.path());
	let expected = put(&tree, 0..300).await;
	let manifest = manifest_path(&tree);
	let saved = std::fs::read(&manifest).unwrap();

	std::fs::remove_file(&manifest).unwrap();
	std::fs::create_dir(&manifest).unwrap();
	std::fs::write(manifest.join("held"), b"held").unwrap();

	let attempts = count_attempts(&tree);
	flush_in_background(&tree);
	wait_until("the failure to be recorded", || {
		tree.core.inner.error_handler.check_error().is_err()
	})
	.await;
	tokio::time::sleep(GRACE).await;

	let failure = tree.core.inner.error_handler.get_error().unwrap();
	assert_eq!(failure.severity, ErrorSeverity::FatalError);
	assert!(
		matches!(failure.error, Error::ManifestWriteUncertain(_)),
		"a failed rename is an uncertain write: {}",
		failure.error
	);
	assert_eq!(attempts.load(Ordering::SeqCst), 1, "a fatal failure is not retried");
	assert!(temp_files(&manifest).is_empty(), "the failed rename left a file behind");
	assert_eq!(immutables(&tree), 1);
	assert!(table_ids(&tree).is_empty(), "the in-memory manifest is as it was");
	assert_contents(&tree, &expected, "the stopped tree");

	std::fs::remove_dir_all(&manifest).unwrap();
	std::fs::write(&manifest, saved).unwrap();
	tree.close().await.unwrap();
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	assert_eq!(table_ids(&reopened), vec![1], "the table is listed once");
	reopened.close().await.unwrap();
}

/// A failure after the rename that nothing injects: the manifest, whose mode the new file takes
/// over, cannot be opened to be synced. The new manifest is in place and listed the table, the
/// failure is fatal and not retried, and memory is as it was.
#[cfg(unix)]
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_manifest_that_cannot_be_synced_after_the_rename_stays_fatal() {
	use std::fs::Permissions;
	use std::os::unix::fs::PermissionsExt;

	let dir = TempDir::new("manifest_unreadable").unwrap();
	let images = TempDir::new("manifest_unreadable_image").unwrap();
	let tree = open(dir.path());
	let expected = put(&tree, 0..300).await;
	let manifest = manifest_path(&tree);

	std::fs::set_permissions(&manifest, Permissions::from_mode(0o000)).unwrap();
	if std::fs::File::open(&manifest).is_ok() {
		// The mode does not stop this user (root), so there is no failure to observe.
		std::fs::set_permissions(&manifest, Permissions::from_mode(0o644)).unwrap();
		tree.close().await.unwrap();
		return;
	}

	let attempts = count_attempts(&tree);
	flush_in_background(&tree);
	wait_until("the failure to be recorded", || {
		tree.core.inner.error_handler.check_error().is_err()
	})
	.await;
	tokio::time::sleep(GRACE).await;

	let failure = tree.core.inner.error_handler.get_error().unwrap();
	assert_eq!(failure.severity, ErrorSeverity::FatalError);
	assert!(
		matches!(failure.error, Error::ManifestWriteUncertain(_)),
		"a failure after the rename is an uncertain write: {}",
		failure.error
	);
	assert_eq!(attempts.load(Ordering::SeqCst), 1, "a fatal failure is not retried");
	assert!(temp_files(&manifest).is_empty());
	assert_eq!(immutables(&tree), 1);
	assert!(table_ids(&tree).is_empty(), "the in-memory manifest is as it was");

	// The renamed manifest is on disk: a crash now leaves the table listed, with its data.
	std::fs::set_permissions(&manifest, Permissions::from_mode(0o644)).unwrap();
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

/// The most attempts that a retry loop waiting 50 ms and doubling to 2 s can have made when
/// `elapsed` has passed since the first: the wait before attempt n + 1 is at least the sum of the
/// first n waits, however slow the machine is.
fn most_attempts_within(elapsed: Duration) -> usize {
	let mut attempts = 1;
	let mut wait = Duration::from_millis(50);
	let mut at = wait;
	while at <= elapsed {
		attempts += 1;
		wait = (wait * 2).min(Duration::from_secs(2));
		at += wait;
	}
	attempts
}

/// A flush that keeps failing is not run in a loop: the waits between the attempts grow.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_flush_that_keeps_failing_is_retried_with_growing_waits() {
	let dir = TempDir::new("manifest_backoff").unwrap();
	let tree = open(dir.path());
	put(&tree, 0..300).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	let attempts = count_attempts(&tree);
	fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, usize::MAX);
	let start = Instant::now();
	flush_in_background(&tree);
	wait_until("the flush to be retried", || attempts.load(Ordering::SeqCst) >= 2).await;
	tokio::time::sleep(Duration::from_millis(700)).await;

	let made = attempts.load(Ordering::SeqCst);
	let elapsed = start.elapsed();
	assert!(
		made <= most_attempts_within(elapsed) + 1,
		"{made} attempts in {elapsed:?}: the waits between them do not grow"
	);

	let _ = tokio::time::timeout(REACHED, tree.close()).await.expect("close returns");
}

/// Two memtables are queued and the manifest write of the one numbered `failing` (0 or 1) fails
/// once in the background flush. The retry succeeds, the stop ends whichever memtable failed, both
/// tables are listed once and nothing is lost.
async fn a_failure_in_a_queue_of_memtables_is_retried_and_ends(failing: usize) {
	let dir = TempDir::new("manifest_queue").unwrap();
	let tree = open(dir.path());
	let mut expected = put(&tree, 0..200).await;
	tree.core.inner.rotate_memtable().unwrap();
	expected.extend(put(&tree, 200..400).await);
	tree.core.inner.rotate_memtable().unwrap();
	assert_eq!(immutables(&tree), 2);
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	fail_replacements(&manifest, ReplaceFailure::BeforeRename, failing, 1);
	tree.core.task_manager.lock().unwrap().as_ref().unwrap().wake_up_memtable();
	wait_until("both memtables to be flushed", || immutables(&tree) == 0).await;
	wait_until("the stop to end", || tree.core.inner.error_handler.check_error().is_ok()).await;
	assert!(!tree.core.inner.error_handler.is_db_stopped());
	assert_eq!(table_ids(&tree), vec![1, 2], "one table per memtable");

	expected.extend(put(&tree, 400..500).await);
	assert_contents(&tree, &expected, "the tree after the retry");
	tree.close().await.unwrap();
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	assert_eq!(table_ids(&reopened), vec![1, 2, 3], "one table per memtable");
	reopened.close().await.unwrap();
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_failure_of_the_first_queued_memtable_is_retried_and_ends() {
	a_failure_in_a_queue_of_memtables_is_retried_and_ends(0).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_failure_of_a_later_queued_memtable_is_retried_and_ends() {
	a_failure_in_a_queue_of_memtables_is_retried_and_ends(1).await;
}

/// The memtable whose flush the background task keeps failing is flushed by a checkpoint instead.
/// The task that finds the queue empty on its next attempt ends the stop, though it flushed
/// nothing itself.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_that_flushes_the_failed_memtable_ends_the_stop() {
	let dir = TempDir::new("manifest_drained").unwrap();
	let out = TempDir::new("manifest_drained_out").unwrap();
	let tree = open(dir.path());
	let mut expected = put(&tree, 0..300).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	let attempts = count_attempts(&tree);
	fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, usize::MAX);
	flush_in_background(&tree);
	// The sixth attempt is followed by a wait of 1.6 s: the checkpoint is done well inside it.
	wait_until("the sixth attempt", || attempts.load(Ordering::SeqCst) >= 6).await;
	wait_until("the failure to be recorded", || {
		tree.core.inner.error_handler.check_error().is_err()
	})
	.await;
	// Let the sixth attempt finish failing, so that the next is the one that finds the queue empty.
	tokio::time::sleep(Duration::from_millis(250)).await;
	heal_replacements(&manifest);

	let cp = out.path().join("cp");
	let checkpointed = {
		let (tree, cp) = (Arc::clone(&tree), cp.clone());
		tokio::task::spawn_blocking(move || tree.create_checkpoint(&cp)).await.unwrap()
	};
	checkpointed.expect("the checkpoint flushes the memtable");
	assert_eq!(immutables(&tree), 0);

	wait_until("the stop to end", || tree.core.inner.error_handler.check_error().is_ok()).await;
	expected.extend(put(&tree, 300..400).await);
	assert_contents(&tree, &expected, "the tree after the stop");
	assert_eq!(table_ids(&tree), vec![1], "the checkpoint made the table, once");

	tree.close().await.unwrap();
	let reopened = open(dir.path());
	assert_contents(&reopened, &expected, "the reopened tree");
	reopened.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// a recovered tree
// ---------------------------------------------------------------------------

/// The WAL segments of a database directory that hold records, by number.
fn non_empty_segments(dir: &Path) -> Vec<u64> {
	let mut ids: Vec<u64> = std::fs::read_dir(dir.join("wal"))
		.unwrap()
		.map(|e| e.unwrap())
		.filter(|e| e.file_name().to_string_lossy().ends_with(".wal"))
		.filter(|e| e.metadata().unwrap().len() > 0)
		.map(|e| e.file_name().to_string_lossy().trim_end_matches(".wal").parse().unwrap())
		.collect();
	ids.sort_unstable();
	ids
}

/// The state a reopened database starts from. Its active memtable holds rows of the segments that
/// were replayed, all older than the fresh segment it is tagged with and continues in, and no
/// table holds them yet. Returns the number of the fresh segment.
fn assert_recovered_state(tree: &Tree, image: &Path, what: &str) -> u64 {
	let replayed = non_empty_segments(image);
	assert!(!replayed.is_empty(), "{what}: the image holds no record to replay");
	let active = tree.core.inner.wal.read().get_active_log_number();
	assert!(
		replayed.iter().all(|&segment| segment < active),
		"{what}: the database continues in segment {active}, the image holds {replayed:?}"
	);
	assert_eq!(
		tree.core.inner.active_memtable.read().unwrap().get_wal_number(),
		active,
		"{what}: the recovered memtable is tagged with the fresh segment"
	);
	assert!(table_ids(tree).is_empty(), "{what}: the rows are in the memtable only");
	active
}

/// A flush whose manifest write fails, on a tree that was recovered from a crash image: the
/// memtable it flushes holds the rows of the crashed segments but is tagged with the fresh
/// segment, and `log_number` moves past that tag when the flush succeeds. Every commit that was
/// acknowledged, before the crash and after the recovery, is there in an image cut while the flush
/// has failed, in one cut after it, and in a third recovery of each.
async fn a_failed_flush_on_a_recovered_tree(at: ReplaceFailure) {
	let dir = TempDir::new("manifest_retry_recovered").unwrap();
	let images = TempDir::new("manifest_retry_recovered_img").unwrap();
	let tree = open(dir.path());
	let mut expected = put(&tree, 0..300).await;

	// The first crash: the rows are in the WAL only.
	let first = images.path().join("first");
	copy_dir_all(dir.path(), &first);
	let recovered = open(&first);
	let tag = assert_recovered_state(&recovered, &first, "the first recovery");
	assert_contents(&recovered, &expected, "the first recovery");
	expected.extend(put(&recovered, 300..400).await);

	// The recovered memtable is flushed and its manifest write fails.
	let manifest = manifest_path(&recovered);
	let _healed = Healed(manifest.clone());
	let log_before = recovered.core.inner.level_manifest.read().unwrap().get_log_number();
	assert!(log_before < tag, "the manifest does not yet record the segment of the tag as flushed");
	// A retry of the first kind of failure waits for the test between its table and its manifest.
	let retry = (at == ReplaceFailure::BeforeRename).then(|| park_attempt(&recovered, 2));
	fail_replacements(&manifest, at, 0, 1);
	flush_in_background(&recovered);

	// The image cut while the flush has failed: its rows are in the WAL only, as in the first.
	let stopped = images.path().join("stopped");
	let acknowledged = expected.clone();
	if let Some(retry) = retry {
		assert!(retry.is_reached(), "the failed flush is retried");
		assert_eq!(immutables(&recovered), 1, "the recovered memtable is still queued");
		assert!(table_ids(&recovered).is_empty());
		copy_dir_all(&first, &stopped);
		retry.release();
		wait_until("the error to clear", || error_is_clear(&recovered)).await;
		expected.extend(put(&recovered, 400..450).await);
	} else {
		wait_until("the failure to be recorded", || !error_is_clear(&recovered)).await;
		tokio::time::sleep(GRACE).await;
		assert_eq!(immutables(&recovered), 1, "the recovered memtable is still queued");
		copy_dir_all(&first, &stopped);
		// The next flush settles the manifest, then writes the table again.
		recovered.core.inner.flush_all_immutables_sync().unwrap();
	}
	assert_eq!(immutables(&recovered), 0);
	assert_eq!(table_ids(&recovered), vec![1], "the table is listed once");
	assert!(
		recovered.core.inner.level_manifest.read().unwrap().get_log_number() > tag,
		"the flush of the tagged memtable moved log_number past its tag"
	);
	assert_contents(&recovered, &expected, "the recovered tree after the flush");

	// The image cut while the flush had failed recovers every commit acknowledged by then, and
	// keeps working: another flush and another recovery.
	let early = open(&stopped);
	assert_contents(&early, &acknowledged, "the image cut while the flush had failed");
	rotate(&early);
	early.core.inner.flush_all_immutables_sync().unwrap();
	assert_unique(&table_ids(&early), "the image cut while the flush had failed");
	early.close().await.unwrap();
	let early = open(&stopped);
	assert_contents(&early, &acknowledged, "the image cut while the flush had failed, again");
	early.close().await.unwrap();

	// The second crash: after the flush. The table holds the crashed rows, the WAL the rest.
	let second = images.path().join("second");
	copy_dir_all(&first, &second);
	let again = open(&second);
	assert_contents(&again, &expected, "the image cut after the flush");
	assert_unique(&table_ids(&again), "the image cut after the flush");
	again.close().await.unwrap();
	let third = open(&second);
	assert_contents(&third, &expected, "the image cut after the flush, recovered again");
	third.close().await.unwrap();

	recovered.close().await.unwrap();
	tree.close().await.unwrap();
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_manifest_write_that_fails_once_on_a_recovered_tree_is_retried_and_loses_nothing() {
	a_failed_flush_on_a_recovered_tree(ReplaceFailure::BeforeRename).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn an_uncertain_manifest_write_on_a_recovered_tree_loses_nothing() {
	a_failed_flush_on_a_recovered_tree(ReplaceFailure::AfterRename).await;
}

/// Recovery flushes the memtables it rebuilds, all but the last, while it replays. When the
/// manifest write of one of those flushes fails, the open fails with that error; what it left on
/// disk then opens with every commit, and no table is listed twice.
async fn a_recovery_flush_that_cannot_write_the_manifest(at: ReplaceFailure) {
	let dir = TempDir::new("manifest_retry_recovery_flush").unwrap();
	let images = TempDir::new("manifest_retry_recovery_flush_img").unwrap();
	let tree = open(dir.path());
	put(&tree, 0..100).await;
	rotate(&tree);
	put(&tree, 100..200).await;
	rotate(&tree);
	let expected = put(&tree, 0..300).await;
	assert_eq!(immutables(&tree), 2, "two memtables wait in the queue");
	assert!(table_ids(&tree).is_empty());

	let image = images.path().join("crash");
	copy_dir_all(dir.path(), &image);
	let segments = non_empty_segments(&image);
	assert!(segments.len() >= 3, "the image holds a segment per memtable: {segments:?}");

	let manifest = options(&image).manifest_file_path(0);
	let _healed = Healed(manifest.clone());
	fail_replacements(&manifest, at, 0, 1);
	let failed = Tree::new(Arc::new(options(&image)));
	let error = failed.err().expect("the open reports the failed flush of the recovery");
	match at {
		ReplaceFailure::BeforeRename => {
			assert!(matches!(error, Error::Other(_)), "{error:?}");
			assert!(error.to_string().contains("manifest"), "{error}");
			assert!(temp_files(&manifest).is_empty(), "the failed write left a file behind");
		}
		ReplaceFailure::AfterRename => {
			assert!(matches!(error, Error::ManifestWriteUncertain(_)), "{error:?}");
		}
	}

	// What the failed open left on disk, opened again. The failpoint is spent: the directory opens
	// and holds everything, once. (A copy, because the background tasks that the failed open had
	// started still hold the lock of the directory.)
	let left = images.path().join("left");
	copy_dir_all(&image, &left);
	let recovered = open(&left);
	assert_contents(&recovered, &expected, "the directory after the failed open");
	assert_unique(&table_ids(&recovered), "the directory after the failed open");
	assert!(!table_ids(&recovered).is_empty(), "the recovery flushed the older memtables");
	assert!(error_is_clear(&recovered));
	recovered.close().await.unwrap();
	let again = open(&left);
	assert_contents(&again, &expected, "the directory reopened");
	again.close().await.unwrap();

	tree.close().await.unwrap();
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_recovery_flush_that_fails_before_the_rename_fails_the_open_and_loses_nothing() {
	a_recovery_flush_that_cannot_write_the_manifest(ReplaceFailure::BeforeRename).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_recovery_flush_that_fails_after_the_rename_fails_the_open_and_loses_nothing() {
	a_recovery_flush_that_cannot_write_the_manifest(ReplaceFailure::AfterRename).await;
}
