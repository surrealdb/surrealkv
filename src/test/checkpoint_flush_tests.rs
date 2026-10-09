//! A checkpoint flushes the immutable memtables on the caller's thread while the background
//! task flushes them too, so an immutable memtable must be flushed once however the two
//! interleave: one table, listed once in the manifest, with its data intact. A flush that
//! fails leaves the immutable memtable and its WAL segment in place for a retry, and a
//! checkpoint needs no Tokio runtime on its thread. The copy a checkpoint makes holds one
//! version of the manifest with the tables and value-log files it lists, and taking it does not
//! disturb the tree.

use std::collections::BTreeMap;
use std::ops::Range;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use tempdir::TempDir;
use test_log::test;

use super::collect_transaction_all;
use crate::batch::Batch;
use crate::checkpoint::{CheckpointMetadata, CheckpointStage};
use crate::error::Result;
use crate::lsm::{CompactionOperations, FlushHook};
use crate::memtable::{ImmutableMemtables, MemTable};
use crate::ring::PipelineHook;
use crate::vlog::ValueLocation;
use crate::wal::list_segment_ids;
use crate::{Durability, Error, Mode, Options, Tree};

/// Large enough that nothing rotates unless a test rotates it.
const NEVER_ROTATING: usize = 16 * 1024 * 1024;

/// How long a test waits for a flush it expects to reach the hook.
const REACHED: Duration = Duration::from_secs(30);

/// How long a test gives a second flusher to reach the hook while the first one is parked.
/// Reaching it takes microseconds, so waiting this long without an arrival means the second
/// flusher is excluded.
const GRACE: Duration = Duration::from_millis(500);

/// How long a test waits for a call that is expected to return.
const STUCK: Duration = Duration::from_secs(60);

type Contents = BTreeMap<Vec<u8>, Vec<u8>>;

fn key_of(i: usize) -> Vec<u8> {
	format!("key_{i:06}").into_bytes()
}

fn val_of(i: usize) -> Vec<u8> {
	// Every seventh value is large enough to be separated into the value log when it is on.
	let len = if i % 7 == 0 {
		3000
	} else {
		100
	};
	let mut v = format!("val_{i:06}_").into_bytes();
	v.resize(len, b'x');
	v
}

fn options(path: &Path, max_memtable_size: usize, vlog: bool) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		max_memtable_size,
		enable_vlog: vlog,
		// Tests that queue several immutable memtables by hand must not stall their own writes.
		memtable_stall_threshold: 1000,
		..Default::default()
	})
}

/// A tree to share between threads. Not a clone of the `Tree`: dropping a clone closes the
/// store under the other holders.
fn open_with(opts: Arc<Options>) -> Arc<Tree> {
	Arc::new(Tree::new(opts).unwrap())
}

fn open(path: &Path, max_memtable_size: usize) -> Arc<Tree> {
	open_with(options(path, max_memtable_size, false))
}

/// Commits the keys in `range`, 25 to a transaction, and returns what was written.
async fn put(tree: &Tree, range: Range<usize>) -> Contents {
	let mut written = Contents::new();
	let keys: Vec<usize> = range.collect();
	for chunk in keys.chunks(25) {
		let mut txn = tree.begin().unwrap();
		for &i in chunk {
			txn.set(key_of(i), val_of(i)).unwrap();
			written.insert(key_of(i), val_of(i));
		}
		txn.commit().await.unwrap();
	}
	written
}

/// Everything the tree holds, in key order.
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

fn immutables(tree: &Tree) -> usize {
	tree.core.inner.immutable_count()
}

/// The ids of the tables the manifest lists, one per listing.
fn table_ids(tree: &Tree) -> Vec<u64> {
	let mut ids: Vec<u64> =
		tree.core.inner.level_manifest.read().unwrap().iter().map(|t| t.id).collect();
	ids.sort_unstable();
	ids
}

fn log_number(tree: &Tree) -> u64 {
	tree.core.inner.level_manifest.read().unwrap().get_log_number()
}

fn wal_segments(path: &Path) -> Vec<u64> {
	list_segment_ids(&path.join("wal"), Some("wal")).unwrap()
}

/// The WAL segment the oldest queued memtable was written to.
fn oldest_wal_number(tree: &Tree) -> u64 {
	tree.core.inner.immutable_memtables.read().unwrap().first().unwrap().wal_number
}

/// The names and sizes of the files in the `sub` directory of `path`, in name order.
fn files_in(path: &Path, sub: &str) -> Vec<(String, u64)> {
	let mut files: Vec<(String, u64)> = std::fs::read_dir(path.join(sub))
		.unwrap()
		.map(|e| {
			let e = e.unwrap();
			(e.file_name().to_string_lossy().into_owned(), e.metadata().unwrap().len())
		})
		.collect();
	files.sort();
	files
}

fn names(files: &[(String, u64)]) -> Vec<&str> {
	files.iter().map(|(name, _)| name.as_str()).collect()
}

/// A copy of the data directory as a crash would leave it.
fn copy_dir_all(src: &Path, dst: &Path) {
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

/// A call on a thread of its own, without a runtime. One that never returns fails its test after
/// `STUCK`, leaving a parked thread behind, instead of hanging the test binary on the runtime's
/// shutdown.
struct Call<T> {
	done: Arc<AtomicBool>,
	result: mpsc::Receiver<std::thread::Result<T>>,
}

fn on_thread<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Call<T> {
	let (tx, result) = mpsc::channel();
	let done = Arc::new(AtomicBool::new(false));
	let finished = Arc::clone(&done);
	std::thread::spawn(move || {
		let result = catch_unwind(AssertUnwindSafe(f));
		finished.store(true, Ordering::SeqCst);
		let _ = tx.send(result);
	});
	Call {
		done,
		result,
	}
}

impl<T> Call<T> {
	fn is_finished(&self) -> bool {
		self.done.load(Ordering::SeqCst)
	}

	/// The call's result; a panic of the call is raised again here.
	fn wait(self) -> T {
		match self.result.recv_timeout(STUCK) {
			Ok(Ok(value)) => value,
			Ok(Err(panic)) => resume_unwind(panic),
			Err(_) => panic!("a call did not return within {STUCK:?}: a deadlock"),
		}
	}
}

/// Runs the flush a checkpoint runs.
fn flush_all(tree: &Tree) -> Call<Result<()>> {
	let inner = Arc::clone(&tree.core.inner);
	on_thread(move || inner.flush_all_immutables_sync())
}

/// Runs the flush the background task runs.
fn flush_in_background(tree: &Tree) -> Call<Result<()>> {
	let inner = Arc::clone(&tree.core.inner);
	on_thread(move || inner.compact_memtable())
}

fn checkpoint(tree: &Arc<Tree>, path: &Path) -> Call<Result<CheckpointMetadata>> {
	let (tree, path) = (Arc::clone(tree), path.to_path_buf());
	on_thread(move || tree.create_checkpoint(&path))
}

fn restore(tree: &Arc<Tree>, path: &Path) -> Call<Result<CheckpointMetadata>> {
	let (tree, path) = (Arc::clone(tree), path.to_path_buf());
	on_thread(move || tree.restore_from_checkpoint(&path))
}

/// Rotates the active memtable into the queue and flushes the queue.
fn flush_to_tables(tree: &Tree) {
	tree.core.inner.rotate_memtable().unwrap();
	flush_all(tree).wait().unwrap();
}

/// Opens a checkpoint directory as a database.
fn open_checkpoint(path: &Path) -> Arc<Tree> {
	open(path, NEVER_ROTATING)
}

/// Parks the first caller of the hook that `new` returns until `release`, and reports the id every
/// caller passes it.
struct Gate {
	reached: mpsc::Receiver<u64>,
	release: mpsc::Sender<()>,
}

impl Gate {
	fn new() -> (Self, Arc<dyn Fn(u64) + Send + Sync>) {
		let (reached_tx, reached) = mpsc::channel();
		let (release, release_rx) = mpsc::channel();
		let release_rx = Mutex::new(release_rx);
		let parked = AtomicBool::new(false);
		let pass: Arc<dyn Fn(u64) + Send + Sync> = Arc::new(move |id| {
			reached_tx.send(id).unwrap();
			if !parked.swap(true, Ordering::SeqCst) {
				// Bounded, so a broken test fails instead of hanging.
				let _ = release_rx.lock().unwrap().recv_timeout(REACHED);
			}
		});
		(
			Self {
				reached,
				release,
			},
			pass,
		)
	}

	/// The id of the next caller to reach the hook, if one does within `wait`.
	fn next(&self, wait: Duration) -> Option<u64> {
		self.reached.recv_timeout(wait).ok()
	}

	fn release(&self) {
		let _ = self.release.send(());
	}
}

/// Parks the first flush that reaches the flush hook (its SST is on disk and the manifest does not
/// list it yet), and reports the table id of every flush that reaches it.
fn gate_flushes(tree: &Tree) -> Gate {
	let (gate, pass) = Gate::new();
	let hook: FlushHook = Arc::new(move |table_id| {
		pass(table_id);
		Ok(())
	});
	*tree.core.inner.flush_hook.lock() = Some(hook);
	gate
}

/// Parks a checkpoint when it reaches `stage`; the id reported is 0.
fn gate_checkpoint(tree: &Tree, stage: CheckpointStage) -> Gate {
	let (gate, pass) = Gate::new();
	*tree.core.inner.checkpoint_hook.lock() = Some(Arc::new(move |at| {
		if at == stage {
			pass(0);
		}
	}));
	gate
}

fn clear_hooks(tree: &Tree) {
	*tree.core.inner.flush_hook.lock() = None;
	*tree.core.inner.checkpoint_hook.lock() = None;
}

// ---------------------------------------------------------------------------
// one immutable memtable, flushed once
// ---------------------------------------------------------------------------

/// A second flusher arrives while the first one has written the SST and not yet updated the
/// manifest. It must not flush the memtable again: it waits, and then finds nothing to do.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn two_flushes_of_one_immutable_memtable_make_one_table() {
	let dir = TempDir::new("ckpt_flush_two").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let expected = put(&tree, 0..600).await;
	tree.core.inner.rotate_memtable().unwrap();
	assert_eq!(immutables(&tree), 1);

	let gate = gate_flushes(&tree);
	let first = flush_all(&tree);
	assert_eq!(gate.next(REACHED), Some(1), "the first flush reaches the hook");
	let second = flush_all(&tree);
	let intruder = gate.next(GRACE);
	gate.release();
	let (first, second) = (first.wait(), second.wait());

	assert_eq!(intruder, None, "a second flush of an immutable memtable that is being flushed");
	first.unwrap();
	second.unwrap();
	assert_eq!(table_ids(&tree), vec![1], "the memtable became one table, listed once");
	assert_eq!(immutables(&tree), 0);
	assert_contents(&tree, &expected, "live tree");

	clear_hooks(&tree);
	tree.close().await.unwrap();
	let reopened = open(dir.path(), NEVER_ROTATING);
	assert_eq!(table_ids(&reopened), vec![1]);
	assert_contents(&reopened, &expected, "reopened tree");
	reopened.close().await.unwrap();
}

/// The same race with a checkpoint as the second flusher and the background task's own flush
/// as the first one: the checkpoint waits for the flush in flight and captures its table once.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn checkpoint_waits_for_the_flush_in_flight_and_captures_its_table_once() {
	let dir = TempDir::new("ckpt_flush_cp").unwrap();
	let checkpoints = TempDir::new("ckpt_flush_cp_out").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let expected = put(&tree, 0..600).await;
	tree.core.inner.rotate_memtable().unwrap();

	let gate = gate_flushes(&tree);
	let background = flush_in_background(&tree);
	assert_eq!(gate.next(REACHED), Some(1), "the background flush reaches the hook");

	let checkpoint_dir = checkpoints.path().join("cp");
	let taking = checkpoint(&tree, &checkpoint_dir);
	let intruder = gate.next(GRACE);
	gate.release();
	background.wait().unwrap();
	let metadata = taking.wait().unwrap();

	assert_eq!(
		intruder, None,
		"the checkpoint flushed an immutable memtable already being flushed"
	);
	assert_eq!(table_ids(&tree), vec![1]);
	assert_eq!(metadata.sstable_count, 1, "the checkpoint holds the table once");
	assert_contents(&tree, &expected, "live tree");

	clear_hooks(&tree);
	let copy = open_checkpoint(&checkpoint_dir);
	assert_eq!(table_ids(&copy), vec![1]);
	assert_contents(&copy, &expected, "checkpoint copy");
	copy.close().await.unwrap();
	tree.close().await.unwrap();
}

/// Several immutable memtables drained by two flushers: each becomes one table, in order, and
/// the log number ends past the newest of them.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn two_flushers_drain_several_immutable_memtables_once_each_in_order() {
	let dir = TempDir::new("ckpt_flush_many").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let mut expected = Contents::new();
	for round in 0..3 {
		expected.extend(put(&tree, round * 200..(round + 1) * 200).await);
		tree.core.inner.rotate_memtable().unwrap();
	}
	assert_eq!(immutables(&tree), 3);
	let newest_wal = {
		let queue = tree.core.inner.immutable_memtables.read().unwrap();
		queue.iter().map(|entry| entry.wal_number).max().unwrap()
	};

	let gate = gate_flushes(&tree);
	let first = flush_all(&tree);
	assert_eq!(gate.next(REACHED), Some(1), "the first flush reaches the hook");
	let second = flush_all(&tree);
	let intruder = gate.next(GRACE);
	gate.release();
	let (first, second) = (first.wait(), second.wait());

	assert_eq!(intruder, None, "a second flush of an immutable memtable that is being flushed");
	first.unwrap();
	second.unwrap();
	assert_eq!(table_ids(&tree), vec![1, 2, 3]);
	assert_eq!(immutables(&tree), 0);
	assert_eq!(log_number(&tree), newest_wal + 1);
	assert_contents(&tree, &expected, "live tree");

	clear_hooks(&tree);
	tree.close().await.unwrap();
}

/// A flush of the queue ends with the memtables that were queued when it began. One rotated
/// while it ran is left to the background task: under steady writes the queue is never empty, and
/// a checkpoint that waited for that would never end.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_flush_of_the_queue_does_not_wait_for_memtables_rotated_after_it_began() {
	let dir = TempDir::new("ckpt_flush_bound").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let mut expected = put(&tree, 0..200).await;
	tree.core.inner.rotate_memtable().unwrap();

	let gate = gate_flushes(&tree);
	let flush = flush_all(&tree);
	assert_eq!(gate.next(REACHED), Some(1), "the flush reaches the hook");
	expected.extend(put(&tree, 200..400).await);
	tree.core.inner.rotate_memtable().unwrap();
	assert_eq!(immutables(&tree), 2);
	gate.release();
	flush.wait().unwrap();

	assert_eq!(table_ids(&tree), vec![1], "only the memtable queued at the start was flushed");
	assert_eq!(immutables(&tree), 1, "the one rotated meanwhile is still queued");
	assert_contents(&tree, &expected, "live tree");

	clear_hooks(&tree);
	flush_all(&tree).wait().unwrap();
	assert_eq!(table_ids(&tree), vec![1, 2]);
	assert_eq!(immutables(&tree), 0);
	assert_contents(&tree, &expected, "after the next flush");
	tree.close().await.unwrap();
}

/// A background flush that finds another flush in flight does not return before it is done.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_background_flush_waits_for_the_flush_in_flight() {
	let dir = TempDir::new("ckpt_flush_wait").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let expected = put(&tree, 0..300).await;
	tree.core.inner.rotate_memtable().unwrap();

	let gate = gate_flushes(&tree);
	let first = flush_in_background(&tree);
	assert_eq!(gate.next(REACHED), Some(1), "the first flush reaches the hook");
	let second = flush_in_background(&tree);
	let intruder = gate.next(GRACE);
	let returned_early = second.is_finished();
	gate.release();
	first.wait().unwrap();
	second.wait().unwrap();

	assert_eq!(intruder, None, "the second flush reached the hook");
	assert!(!returned_early, "the second flush returned while the first was in flight");
	assert_eq!(table_ids(&tree), vec![1]);
	assert_eq!(immutables(&tree), 0);
	assert_contents(&tree, &expected, "live tree");
	clear_hooks(&tree);
	tree.close().await.unwrap();
}

/// A queued memtable that holds nothing must not end the drain before the memtables behind it.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn an_empty_immutable_memtable_does_not_stop_the_drain() {
	let dir = TempDir::new("ckpt_flush_empty").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let expected = put(&tree, 0..100).await;
	let wal_number = tree.core.inner.active_memtable.read().unwrap().get_wal_number();
	{
		// An empty entry ahead of a populated one.
		let inner = &tree.core.inner;
		let empty_id = inner.level_manifest.read().unwrap().next_table_id();
		inner.immutable_memtables.write().unwrap().add(
			empty_id,
			wal_number,
			Arc::new(MemTable::new(NEVER_ROTATING)),
		);
	}
	tree.core.inner.rotate_memtable().unwrap();
	assert_eq!(immutables(&tree), 2);

	flush_all(&tree).wait().unwrap();

	assert_eq!(immutables(&tree), 0, "every queued memtable was drained");
	assert_eq!(table_ids(&tree).len(), 1);
	assert_contents(&tree, &expected, "live tree");
	tree.close().await.unwrap();
}

/// The shutdown flush waits for a flush in flight on another thread instead of flushing the
/// same memtable again.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_waits_for_a_flush_in_flight_on_another_thread() {
	let dir = TempDir::new("ckpt_flush_close").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let expected = put(&tree, 0..600).await;
	tree.core.inner.rotate_memtable().unwrap();

	let gate = gate_flushes(&tree);
	let flush = flush_all(&tree);
	assert_eq!(gate.next(REACHED), Some(1), "the flush reaches the hook");
	let closing = {
		let tree = Arc::clone(&tree);
		tokio::spawn(async move { tree.close().await })
	};
	let intruder = gate.next(GRACE);
	gate.release();
	flush.wait().unwrap();
	closing.await.unwrap().unwrap();

	assert_eq!(intruder, None, "the shutdown flushed an immutable memtable already being flushed");
	assert_eq!(table_ids(&tree), vec![1]);
	assert_eq!(immutables(&tree), 0);

	clear_hooks(&tree);
	drop(tree);
	let reopened = open(dir.path(), NEVER_ROTATING);
	assert_eq!(table_ids(&reopened), vec![1]);
	assert_contents(&reopened, &expected, "reopened tree");
	reopened.close().await.unwrap();
}

/// The shutdown flush is the one in flight and a checkpoint's flush arrives: it waits for the
/// whole of the shutdown flush instead of flushing the memtable the shutdown is flushing.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_flush_waits_for_the_shutdown_flush_in_flight() {
	let dir = TempDir::new("ckpt_flush_shutdown").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let expected = put(&tree, 0..300).await;
	tree.core.inner.rotate_memtable().unwrap();

	let gate = gate_flushes(&tree);
	let closing = {
		let tree = Arc::clone(&tree);
		tokio::spawn(async move { tree.close().await })
	};
	assert_eq!(gate.next(REACHED), Some(1), "the shutdown flush reaches the hook");
	let other = flush_all(&tree);
	let intruder = gate.next(GRACE);
	gate.release();
	closing.await.unwrap().unwrap();
	other.wait().unwrap();

	assert_eq!(intruder, None, "a flush started while the shutdown flush was in flight");
	assert_eq!(table_ids(&tree), vec![1]);
	assert_eq!(immutables(&tree), 0);

	clear_hooks(&tree);
	drop(tree);
	let reopened = open(dir.path(), NEVER_ROTATING);
	assert_eq!(table_ids(&reopened), vec![1]);
	assert_contents(&reopened, &expected, "the reopened tree");
	reopened.close().await.unwrap();
}

/// A restore replaces the manifest, the queue and the table directory, so it waits for a flush
/// in flight instead of letting the flush list its table in the manifest it restored.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_restore_waits_for_a_flush_in_flight() {
	let dir = TempDir::new("ckpt_flush_restore").unwrap();
	let out = TempDir::new("ckpt_flush_restore_out").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let at_checkpoint = put(&tree, 0..300).await;
	let cp = out.path().join("cp");
	checkpoint(&tree, &cp).wait().unwrap();

	// More data, queued as an immutable memtable whose flush is parked after its SST is written.
	put(&tree, 300..600).await;
	tree.core.inner.rotate_memtable().unwrap();
	assert_eq!(immutables(&tree), 1);
	let gate = gate_flushes(&tree);
	let flush = flush_all(&tree);
	assert_eq!(gate.next(REACHED), Some(2), "the flush reaches the hook");

	let restoring = restore(&tree, &cp);
	tokio::time::sleep(GRACE).await;
	let restored_meanwhile = restoring.is_finished();
	gate.release();
	flush.wait().unwrap();
	restoring.wait().unwrap();
	clear_hooks(&tree);

	assert!(!restored_meanwhile, "the restore went ahead of a flush in flight");
	let on_disk: Vec<String> =
		files_in(dir.path(), "sstables").into_iter().map(|(name, _)| name).collect();
	for id in table_ids(&tree) {
		let name = format!("{id:020}.sst");
		assert!(
			on_disk.contains(&name),
			"the manifest lists table {id}, the files are {on_disk:?}"
		);
	}
	assert_eq!(table_ids(&tree), vec![1], "the restored tree lists the checkpoint's tables only");
	assert_contents(&tree, &at_checkpoint, "the restored tree");

	tree.close().await.unwrap();
	let reopened = open(dir.path(), NEVER_ROTATING);
	assert_contents(&reopened, &at_checkpoint, "the reopened tree");
	reopened.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// a failed flush leaves what a retry or a crash needs
// ---------------------------------------------------------------------------

/// How a flush of the oldest immutable memtable is made to fail.
#[derive(Clone, Copy, Debug)]
enum Failure {
	/// A directory sits where the SST belongs, so the table cannot be created.
	TableCreate,
	/// The SST is written, then the flush fails before the manifest is updated.
	AfterTableWritten,
	/// The SST is written, then the manifest cannot be replaced: a directory sits where it is.
	ManifestReplace,
}

impl Failure {
	/// Removes the obstacle the injection put in `image`, a copy of the data directory.
	fn heal(self, tree: &Tree, image: &Path) {
		let opts = &tree.core.inner.opts;
		let in_image =
			|path: std::path::PathBuf| image.join(path.strip_prefix(&opts.path).unwrap());
		match self {
			Failure::TableCreate => {
				std::fs::remove_dir_all(in_image(opts.sstable_file_path(1))).unwrap();
			}
			Failure::AfterTableWritten => {}
			Failure::ManifestReplace => {
				let manifest = in_image(opts.manifest_file_path(0));
				std::fs::remove_dir_all(&manifest).unwrap();
				std::fs::rename(manifest.with_extension("parked"), &manifest).unwrap();
			}
		}
	}
}

/// Makes the next flush fail as `failure` says; the returned closure removes the obstacle.
fn inject(tree: &Tree, failure: Failure) -> Box<dyn FnOnce()> {
	let opts = Arc::clone(&tree.core.inner.opts);
	match failure {
		Failure::TableCreate => {
			let blocker = opts.sstable_file_path(1);
			std::fs::create_dir_all(&blocker).unwrap();
			Box::new(move || std::fs::remove_dir_all(blocker).unwrap())
		}
		Failure::AfterTableWritten => {
			*tree.core.inner.flush_hook.lock() =
				Some(Arc::new(|_| Err(Error::Other("injected flush failure".into()))));
			let inner = Arc::clone(&tree.core.inner);
			Box::new(move || *inner.flush_hook.lock() = None)
		}
		Failure::ManifestReplace => {
			let manifest = opts.manifest_file_path(0);
			let parked = manifest.with_extension("parked");
			std::fs::rename(&manifest, &parked).unwrap();
			std::fs::create_dir_all(&manifest).unwrap();
			Box::new(move || {
				std::fs::remove_dir_all(&manifest).unwrap();
				std::fs::rename(&parked, &manifest).unwrap();
			})
		}
	}
}

/// Whichever way a flush fails, the memtable stays queued and readable, nothing is listed,
/// `log_number` stays, and so does the WAL segment the memtable was written to: a crash image
/// taken then recovers the data, and the retry flushes the memtable and only then removes the
/// segment.
async fn a_failed_flush_leaves_the_memtable_and_its_wal_segment(failure: Failure) {
	let dir = TempDir::new("ckpt_flush_fail").unwrap();
	let images = TempDir::new("ckpt_flush_fail_image").unwrap();
	let checkpoints = TempDir::new("ckpt_flush_fail_out").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let expected = put(&tree, 0..300).await;
	tree.core.inner.rotate_memtable().unwrap();
	let wal_number = oldest_wal_number(&tree);
	let log_before = log_number(&tree);
	assert!(wal_segments(dir.path()).contains(&wal_number));

	let remove_obstacle = inject(&tree, failure);
	assert!(flush_all(&tree).wait().is_err(), "{failure:?}: the flush fails");
	assert!(flush_in_background(&tree).wait().is_err(), "{failure:?}: so does the background one");
	let cp = checkpoints.path().join("cp");
	assert!(checkpoint(&tree, &cp).wait().is_err(), "{failure:?}: and a checkpoint");

	assert_eq!(immutables(&tree), 1, "{failure:?}: the memtable is still queued");
	assert!(table_ids(&tree).is_empty(), "{failure:?}: nothing is listed");
	assert_eq!(log_number(&tree), log_before, "{failure:?}: log_number did not move");
	assert!(
		wal_segments(dir.path()).contains(&wal_number),
		"{failure:?}: the WAL segment of a memtable that is still queued was removed"
	);
	if !matches!(failure, Failure::TableCreate) {
		assert!(
			tree.core.inner.opts.sstable_file_path(1).exists(),
			"{failure:?}: the SST is on disk"
		);
	}
	if !matches!(failure, Failure::ManifestReplace) {
		assert!(
			tree.core.inner.error_handler.check_error().is_ok(),
			"{failure:?}: a flush that fails before the manifest is touched records no error"
		);
	}
	assert_contents(&tree, &expected, "the live tree after the failed flushes");

	// A crash here: the files as they are, without the obstacle the test put there.
	let image = images.path().join("crash");
	copy_dir_all(dir.path(), &image);
	failure.heal(&tree, &image);
	let recovered = open(&image, NEVER_ROTATING);
	assert_contents(&recovered, &expected, "the crash image");
	recovered.close().await.unwrap();

	remove_obstacle();
	flush_all(&tree).wait().unwrap();
	assert_eq!(immutables(&tree), 0);
	assert_eq!(table_ids(&tree), vec![1], "{failure:?}: the retry lists the table once");
	assert!(log_number(&tree) > log_before);
	assert!(
		wal_segments(dir.path()).iter().all(|id| *id > wal_number),
		"{failure:?}: the segment is removed once its memtable is flushed"
	);
	assert_contents(&tree, &expected, "the live tree after the retry");

	tree.close().await.unwrap();
	let reopened = open(dir.path(), NEVER_ROTATING);
	assert_contents(&reopened, &expected, "the reopened tree");
	reopened.close().await.unwrap();
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_flush_that_cannot_create_its_table_leaves_the_memtable_and_its_wal_segment() {
	a_failed_flush_leaves_the_memtable_and_its_wal_segment(Failure::TableCreate).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_flush_that_fails_after_writing_its_table_leaves_the_memtable_and_its_wal_segment() {
	a_failed_flush_leaves_the_memtable_and_its_wal_segment(Failure::AfterTableWritten).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_flush_whose_manifest_update_fails_leaves_the_memtable_and_its_wal_segment() {
	a_failed_flush_leaves_the_memtable_and_its_wal_segment(Failure::ManifestReplace).await;
}

/// The flush of the oldest memtable fails: the memtables behind it are not flushed past it, or
/// `log_number` would move over the older one's WAL segment. Once it succeeds, all three follow.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_failed_flush_holds_back_the_memtables_queued_behind_it() {
	let dir = TempDir::new("ckpt_flush_order").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let mut expected = Contents::new();
	for round in 0..3 {
		expected.extend(put(&tree, round * 100..(round + 1) * 100).await);
		tree.core.inner.rotate_memtable().unwrap();
	}
	assert_eq!(immutables(&tree), 3);
	let log_before = log_number(&tree);

	// Only the oldest memtable's table fails.
	let hook: FlushHook = Arc::new(|id| {
		if id == 1 {
			Err(Error::Other("injected flush failure".into()))
		} else {
			Ok(())
		}
	});
	*tree.core.inner.flush_hook.lock() = Some(hook);

	assert!(flush_all(&tree).wait().is_err());
	assert!(flush_in_background(&tree).wait().is_err());
	assert_eq!(immutables(&tree), 3, "nothing behind the failed memtable was flushed");
	assert!(table_ids(&tree).is_empty());
	assert_eq!(log_number(&tree), log_before);
	assert_contents(&tree, &expected, "after the failed flushes");

	clear_hooks(&tree);
	flush_all(&tree).wait().unwrap();
	assert_eq!(table_ids(&tree), vec![1, 2, 3]);
	assert_eq!(immutables(&tree), 0);
	assert_contents(&tree, &expected, "after the retry");
	tree.close().await.unwrap();
}

/// The first immutable memtable is flushed and the second one fails: the checkpoint reports the
/// error, the second memtable stays queued with its data readable, nothing is recorded as a
/// background error, and the retry (into the same directory) succeeds and matches.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_whose_second_flush_fails_can_be_retried() {
	let dir = TempDir::new("ckpt_flush_half").unwrap();
	let out = TempDir::new("ckpt_flush_half_out").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let mut expected = put(&tree, 0..200).await;
	tree.core.inner.rotate_memtable().unwrap();
	expected.extend(put(&tree, 200..400).await);
	tree.core.inner.rotate_memtable().unwrap();
	expected.extend(put(&tree, 400..500).await);
	assert_eq!(immutables(&tree), 2);

	let hook: FlushHook = Arc::new(|id| {
		if id == 2 {
			Err(Error::Other("injected flush failure".into()))
		} else {
			Ok(())
		}
	});
	*tree.core.inner.flush_hook.lock() = Some(hook);

	let cp = out.path().join("cp");
	assert!(checkpoint(&tree, &cp).wait().is_err());
	assert_eq!(table_ids(&tree), vec![1]);
	assert_eq!(immutables(&tree), 2, "the second memtable and the one rotated after it are queued");
	assert!(tree.core.inner.error_handler.check_error().is_ok());
	assert_contents(&tree, &expected, "after the failed checkpoint");

	clear_hooks(&tree);
	checkpoint(&tree, &cp).wait().unwrap();
	assert_eq!(immutables(&tree), 0);
	assert_contents(&tree, &expected, "after the retry");
	let copy = open_checkpoint(&cp);
	assert_contents(&copy, &expected, "checkpoint copy");
	copy.close().await.unwrap();
	tree.close().await.unwrap();
}

/// The directory as a crash would leave it while a flush has written its SSTable and not yet
/// listed it, with a second memtable still queued: reopening replays the WAL, loses nothing and
/// lists no table twice.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_crash_image_taken_between_the_sstable_and_the_manifest_reopens_whole() {
	let dir = TempDir::new("ckpt_flush_image").unwrap();
	let image = TempDir::new("ckpt_flush_image_copy").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let mut expected = put(&tree, 0..300).await;
	tree.core.inner.rotate_memtable().unwrap();
	expected.extend(put(&tree, 300..600).await);
	tree.core.inner.rotate_memtable().unwrap();
	expected.extend(put(&tree, 600..650).await);

	let gate = gate_flushes(&tree);
	let flush = flush_all(&tree);
	assert_eq!(gate.next(REACHED), Some(1));
	let img = image.path().join("img");
	copy_dir_all(dir.path(), &img);
	gate.release();
	flush.wait().unwrap();
	clear_hooks(&tree);

	let reopened = open(&img, NEVER_ROTATING);
	assert_contents(&reopened, &expected, "the crash image");
	let ids = table_ids(&reopened);
	let mut dedup = ids.clone();
	dedup.dedup();
	assert_eq!(ids, dedup, "a table is listed twice: {ids:?}");
	reopened.close().await.unwrap();
	tree.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// the WAL segments of memtables that are not in a table yet
// ---------------------------------------------------------------------------

/// A flush records a `log_number` that does not pass the WAL segment of any memtable still
/// queued, whichever memtable it flushed.
#[test]
fn the_log_number_a_flush_records_stays_below_the_oldest_queued_segment() {
	let mut queue = ImmutableMemtables::default();
	assert_eq!(queue.log_number_after(None, 4), 5, "nothing is queued");
	for (table_id, wal_number) in [(1, 3), (2, 4), (3, 5)] {
		queue.add(table_id, wal_number, Arc::new(MemTable::new(1024)));
	}
	assert_eq!(queue.log_number_after(Some(1), 3), 4, "the oldest memtable: up to the next one");
	assert_eq!(queue.log_number_after(Some(2), 4), 3, "a newer memtable leaves the oldest one");
	assert_eq!(queue.log_number_after(Some(3), 5), 3);
	assert_eq!(queue.log_number_after(None, 6), 3, "a memtable that is not queued");
}

/// A checkpoint rotates the active memtable while `close` is flushing the queue it read earlier.
/// When `close` returns, every commit it acknowledged must be in an SSTable or in a WAL segment
/// that is still there: the image of the directory at that instant must hold all of them.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_keeps_the_wal_of_a_memtable_rotated_after_its_snapshot() {
	let dir = TempDir::new("ckpt_flush_close_rotate").unwrap();
	let image = TempDir::new("ckpt_flush_close_rotate_img").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let mut expected = put(&tree, 0..300).await;
	tree.core.inner.rotate_memtable().unwrap();
	expected.extend(put(&tree, 300..600).await);

	let gate = gate_flushes(&tree);
	let closing = {
		let tree = Arc::clone(&tree);
		tokio::spawn(async move { tree.close().await })
	};
	assert_eq!(gate.next(REACHED), Some(1), "the shutdown flush reaches the hook");

	// What a checkpoint does first: rotate the active memtable into the queue.
	tree.core.inner.rotate_memtable().unwrap();
	gate.release();
	closing.await.unwrap().unwrap();

	let queued = immutables(&tree);
	let img = image.path().join("img");
	copy_dir_all(dir.path(), &img);
	clear_hooks(&tree);

	let reopened = open(&img, NEVER_ROTATING);
	assert_contents(
		&reopened,
		&expected,
		&format!("the image taken when close returned with {queued} memtable(s) still queued"),
	);
	reopened.close().await.unwrap();
}

/// As above, with the shutdown flush going on to flush an active memtable that holds something
/// when it has done the queue it read: that table is flushed with the rotated memtable still
/// queued ahead of it, and must not carry `log_number` over the rotated memtable's segment.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_keeps_the_wal_of_a_memtable_rotated_ahead_of_the_active_one_it_flushes() {
	let dir = TempDir::new("ckpt_flush_close_ahead").unwrap();
	let image = TempDir::new("ckpt_flush_close_ahead_img").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let mut expected = put(&tree, 0..300).await;
	tree.core.inner.rotate_memtable().unwrap();
	expected.extend(put(&tree, 300..600).await);

	let gate = gate_flushes(&tree);
	let closing = {
		let tree = Arc::clone(&tree);
		tokio::spawn(async move { tree.close().await })
	};
	assert_eq!(gate.next(REACHED), Some(1), "the shutdown flush reaches the hook");

	// A checkpoint rotates, and the memtable that replaces the rotated one receives an entry.
	// Commits have stopped by now, so it is added to the memtable directly.
	tree.core.inner.rotate_memtable().unwrap();
	let mut batch = Batch::new(10_000);
	let value = ValueLocation::with_inline_value(b"direct".to_vec()).encode();
	batch.set(b"zz_direct".to_vec(), value, 0).unwrap();
	tree.core.inner.active_memtable.read().unwrap().add(&batch).unwrap();
	expected.insert(b"zz_direct".to_vec(), b"direct".to_vec());
	gate.release();
	closing.await.unwrap().unwrap();

	let img = image.path().join("img");
	copy_dir_all(dir.path(), &img);
	clear_hooks(&tree);
	let reopened = open(&img, NEVER_ROTATING);
	assert_contents(&reopened, &expected, "the image taken when close returned");
	reopened.close().await.unwrap();
}

/// A group is in its WAL segment, synced, and not applied when a checkpoint rotates the memtable
/// and flushes it: the flush removes the group's segment before the group is applied. The group
/// is appended again to the current segment and acknowledged, and a crash image taken after the
/// acknowledgement holds it.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_between_a_groups_wal_sync_and_its_apply_loses_nothing() {
	let dir = TempDir::new("ckpt_flush_stale").unwrap();
	let out = TempDir::new("ckpt_flush_stale_out").unwrap();
	let image = TempDir::new("ckpt_flush_stale_img").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let mut expected = put(&tree, 0..100).await;
	let segments_before = wal_segments(dir.path());

	let (reached_tx, reached) = mpsc::channel::<()>();
	let (release, release_rx) = mpsc::channel::<()>();
	let release_rx = Mutex::new(release_rx);
	let parked = AtomicBool::new(false);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::AfterWalSync {
			..
		} = point
		{
			if !parked.swap(true, Ordering::SeqCst) {
				reached_tx.send(()).unwrap();
				let _ = release_rx.lock().unwrap().recv_timeout(REACHED);
			}
		}
	})));

	let commit = {
		let tree = Arc::clone(&tree);
		tokio::spawn(async move {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(Durability::Immediate);
			txn.set(b"late".to_vec(), b"late value".to_vec()).unwrap();
			txn.commit().await
		})
	};
	reached.recv_timeout(REACHED).expect("the group reaches the hook");

	let cp = out.path().join("cp");
	let metadata = checkpoint(&tree, &cp).wait().unwrap();
	assert!(metadata.sstable_count >= 1);
	let segments_parked = wal_segments(dir.path());
	let _ = release.send(());
	commit.await.unwrap().unwrap();
	tree.core.commit_pipeline.set_hook(None);
	expected.insert(b"late".to_vec(), b"late value".to_vec());

	assert!(
		segments_before.iter().all(|id| !segments_parked.contains(id)),
		"the checkpoint's flush was meant to remove the group's segment: {segments_before:?} -> \
		 {segments_parked:?}"
	);
	let img = image.path().join("img");
	copy_dir_all(dir.path(), &img);
	let reopened = open(&img, NEVER_ROTATING);
	assert_contents(&reopened, &expected, "the crash image after the acknowledgement");
	reopened.close().await.unwrap();

	// The checkpoint was taken before the commit was applied: it holds the earlier data only.
	let copy = open_checkpoint(&cp);
	let held = contents(&copy);
	assert!(held.len() == 100 || held.len() == 101, "{}", held.len());
	copy.close().await.unwrap();
	assert_contents(&tree, &expected, "the live tree");
	tree.close().await.unwrap();
}

/// Recovery continues in a fresh WAL segment and tags the recovered memtable with it, although
/// the memtable holds what the crashed segment held. A checkpoint flushes that memtable and
/// removes the segments up to its tag, the crashed one included: the copy, and a crash image
/// taken after the checkpoint, hold the recovered keys and the ones committed after recovery.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_of_a_recovered_tree_holds_what_the_crashed_segment_held() {
	for vlog in [false, true] {
		let dir = TempDir::new("ckpt_flush_recovered").unwrap();
		let out = TempDir::new("ckpt_flush_recovered_out").unwrap();
		let image = TempDir::new("ckpt_flush_recovered_img").unwrap();

		// The first run is never closed: what it committed is in its WAL segment only.
		let first = dir.path().join("first");
		let run = open_with(options(&first, NEVER_ROTATING, vlog));
		let mut expected = put(&run, 0..100).await;
		let crashed = run.core.inner.wal.read().get_active_log_number();
		let live = dir.path().join("live");
		copy_dir_all(&first, &live);
		run.close().await.unwrap();

		let tree = open_with(options(&live, NEVER_ROTATING, vlog));
		assert_eq!(tree.core.inner.wal.read().get_active_log_number(), crashed + 1);
		assert_contents(&tree, &expected, "the recovered tree");
		expected.extend(put(&tree, 100..150).await);
		assert!(wal_segments(&live).contains(&crashed), "the crashed segment is still needed");

		let cp = out.path().join("cp");
		let metadata = checkpoint(&tree, &cp).wait().unwrap();
		assert!(metadata.sstable_count >= 1);
		assert_eq!(immutables(&tree), 0);
		assert!(
			wal_segments(&live).iter().all(|id| *id > crashed + 1),
			"the flush of the recovered memtable removes the segments up to its tag: {:?}",
			wal_segments(&live)
		);

		let img = image.path().join("img");
		copy_dir_all(&live, &img);
		let reopened = open_with(options(&img, NEVER_ROTATING, vlog));
		assert_contents(&reopened, &expected, "a crash image after the checkpoint");
		reopened.close().await.unwrap();

		let copy = open_with(options(&cp, NEVER_ROTATING, vlog));
		assert_contents(&copy, &expected, "the checkpoint");
		copy.close().await.unwrap();
		tree.close().await.unwrap();
	}
}

// ---------------------------------------------------------------------------
// what a checkpoint copies, and what it leaves alone
// ---------------------------------------------------------------------------

/// A checkpoint from a plain thread flushes the memtable, removes the WAL segments the flush
/// made obsolete before it returns, and copies a database that matches the source.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn create_checkpoint_works_on_a_thread_without_a_runtime() {
	for vlog in [false, true] {
		let dir = TempDir::new("ckpt_flush_no_rt").unwrap();
		let checkpoints = TempDir::new("ckpt_flush_no_rt_out").unwrap();
		let tree = open_with(options(dir.path(), NEVER_ROTATING, vlog));
		let expected = put(&tree, 0..500).await;
		let segments_before = wal_segments(dir.path());

		let checkpoint_dir = checkpoints.path().join("cp");
		let metadata = {
			let (tree, checkpoint_dir) = (Arc::clone(&tree), checkpoint_dir.clone());
			on_thread(move || {
				assert!(
					tokio::runtime::Handle::try_current().is_err(),
					"the thread is meant to have no runtime"
				);
				tree.create_checkpoint(&checkpoint_dir)
			})
			.wait()
			.unwrap()
		};

		assert!(metadata.sstable_count >= 1);
		assert_eq!(immutables(&tree), 0);
		let segments_after = wal_segments(dir.path());
		assert!(
			!segments_after.is_empty()
				&& segments_after.iter().all(|id| !segments_before.contains(id)),
			"the WAL segments the flush made obsolete are still there: {segments_before:?} -> \
			 {segments_after:?}"
		);
		assert!(segments_after.iter().all(|id| *id >= log_number(&tree)));
		assert_contents(&tree, &expected, "live tree");

		let copy = open_with(options(&checkpoint_dir, NEVER_ROTATING, vlog));
		assert_contents(&copy, &expected, "checkpoint copy");
		copy.close().await.unwrap();
		tree.close().await.unwrap();
	}
}

/// The copy a checkpoint makes of a tree with data in several memtables and tables opens as a
/// database and holds exactly what the source holds, with and without the value log.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn checkpoint_copy_opens_and_matches_the_source() {
	for vlog in [false, true] {
		let dir = TempDir::new("ckpt_flush_copy").unwrap();
		let checkpoints = TempDir::new("ckpt_flush_copy_out").unwrap();
		let tree = open_with(options(dir.path(), 32 * 1024, vlog));
		let expected = put(&tree, 0..600).await;

		let checkpoint_dir = checkpoints.path().join("cp");
		let metadata = checkpoint(&tree, &checkpoint_dir).wait().unwrap();
		assert!(metadata.sstable_count >= 1);
		assert_eq!(immutables(&tree), 0);

		// Writes after the checkpoint are not in it.
		let later = put(&tree, 5000..5100).await;
		assert_eq!(contents(&tree).len(), expected.len() + later.len());

		let copy = open_with(options(&checkpoint_dir, 32 * 1024, vlog));
		assert_contents(&copy, &expected, "checkpoint copy");
		copy.close().await.unwrap();
		tree.close().await.unwrap();
	}
}

/// A flush that would finish while a checkpoint copies its files waits for the copy: the manifest
/// the checkpoint holds lists exactly the SSTables it holds, and the checkpoint opens.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_flush_cannot_change_the_manifest_while_a_checkpoint_copies_it() {
	let dir = TempDir::new("ckpt_flush_manifest").unwrap();
	let checkpoints = TempDir::new("ckpt_flush_manifest_out").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let before = put(&tree, 0..300).await;
	flush_to_tables(&tree);
	assert_eq!(table_ids(&tree), vec![1]);

	let gate = gate_checkpoint(&tree, CheckpointStage::SstablesCopied);
	let checkpoint_dir = checkpoints.path().join("cp");
	let taking = checkpoint(&tree, &checkpoint_dir);
	assert_eq!(gate.next(REACHED), Some(0), "the checkpoint copied the SSTables");

	// A second memtable is flushed while the checkpoint is between the SSTables and the manifest.
	let mut after = before.clone();
	after.extend(put(&tree, 300..600).await);
	tree.core.inner.rotate_memtable().unwrap();
	let flush = flush_all(&tree);
	tokio::time::sleep(GRACE).await;
	let flushed_meanwhile = flush.is_finished();
	gate.release();
	let metadata = taking.wait().unwrap();
	flush.wait().unwrap();

	assert!(!flushed_meanwhile, "a flush changed the manifest while the checkpoint copied it");
	assert_eq!(metadata.sstable_count, 1);
	assert_eq!(table_ids(&tree), vec![1, 2]);
	assert_contents(&tree, &after, "live tree");

	clear_hooks(&tree);
	let copy = open_checkpoint(&checkpoint_dir);
	assert_eq!(table_ids(&copy), vec![1], "the checkpoint holds the tables it copied");
	assert_contents(&copy, &before, "checkpoint copy");
	copy.close().await.unwrap();
	tree.close().await.unwrap();
}

/// A second checkpoint into the same directory must not damage the database it copies. The
/// SSTables of the first are hard links of the live files, and the second finds them in the way
/// of its own hard links.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_second_checkpoint_into_the_same_directory_leaves_the_live_tables_intact() {
	let dir = TempDir::new("ckpt_flush_twice").unwrap();
	let out = TempDir::new("ckpt_flush_twice_out").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let first = put(&tree, 0..400).await;
	let cp = out.path().join("cp");

	checkpoint(&tree, &cp).wait().unwrap();
	let before = files_in(dir.path(), "sstables");
	assert!(before.iter().all(|(_, len)| *len > 0), "{before:?}");

	let mut expected = first;
	expected.extend(put(&tree, 400..500).await);
	let second = checkpoint(&tree, &cp).wait();
	let after = files_in(dir.path(), "sstables");
	assert!(
		after.iter().all(|(_, len)| *len > 0),
		"the second checkpoint ({second:?}) truncated live SSTables: {before:?} -> {after:?}"
	);
	second.unwrap();
	assert_contents(&tree, &expected, "live tree");

	let copy = open_checkpoint(&cp);
	assert_contents(&copy, &expected, "checkpoint copy");
	copy.close().await.unwrap();
	tree.close().await.unwrap();
}

/// A compaction finishes while a checkpoint has released the manifest and not yet copied the
/// value log. The files it made obsolete stay until the copy is done, so the copy holds every
/// file the tables it holds point into, and a flush afterwards deletes them.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn value_log_files_a_checkpoint_has_yet_to_copy_are_not_deleted() {
	let dir = TempDir::new("ckpt_flush_vlog").unwrap();
	let out = TempDir::new("ckpt_flush_vlog_out").unwrap();
	let opts = Arc::new(Options {
		path: dir.path().to_path_buf(),
		max_memtable_size: NEVER_ROTATING,
		memtable_stall_threshold: 1000,
		enable_vlog: true,
		vlog_value_threshold: 64,
		// A few values to a file, so each round of writes makes several files.
		vlog_max_file_size: 4096,
		level0_max_files: 2,
		l0_stall_threshold: 1000,
		..Default::default()
	});
	let tree = open_with(Arc::clone(&opts));
	put(&tree, 0..100).await;
	flush_to_tables(&tree);
	// The same keys again: once the tables are merged, the files of the first round are obsolete.
	let expected = put(&tree, 0..100).await;
	flush_to_tables(&tree);
	assert_eq!(table_ids(&tree), vec![1, 2]);

	let gate = gate_checkpoint(&tree, CheckpointStage::ManifestReleased);
	let cp = out.path().join("cp");
	let taking = checkpoint(&tree, &cp);
	assert_eq!(gate.next(REACHED), Some(0), "the checkpoint released the manifest");

	let files = files_in(dir.path(), "vlog");
	assert!(files.len() > 4, "{files:?}");
	let merge = {
		let (tree, opts) = (Arc::clone(&tree), Arc::clone(&opts));
		on_thread(move || {
			tree.compact(Arc::new(crate::compaction::leveled::Strategy::from_options(opts)))
		})
	};
	merge.wait().unwrap();
	assert!(!table_ids(&tree).contains(&1), "the compaction merged the tables");
	assert_eq!(
		names(&files_in(dir.path(), "vlog")),
		names(&files),
		"the compaction deleted value-log files while a checkpoint had yet to copy them"
	);
	gate.release();
	taking.wait().unwrap();

	assert_eq!(names(&files_in(&cp, "vlog")), names(&files), "the copy lacks value-log files");
	clear_hooks(&tree);
	let copy = open_with(options(&cp, NEVER_ROTATING, true));
	assert_contents(&copy, &expected, "checkpoint copy");
	copy.close().await.unwrap();

	// The pin is gone with the checkpoint: the next flush deletes what the compaction obsoleted.
	let mut all = expected;
	all.extend(put(&tree, 200..210).await);
	flush_to_tables(&tree);
	assert!(
		files_in(dir.path(), "vlog").len() < files.len(),
		"the obsolete value-log files are still there after the checkpoint"
	);
	assert_contents(&tree, &all, "live tree");
	tree.close().await.unwrap();
}

/// While a checkpoint is parked between its SSTable copy and its manifest copy, a flush is
/// queued behind it and a commit that rotates the memtable is in flight. Nothing may deadlock:
/// once the checkpoint is released everything finishes, and the copy matches.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_flush_queued_behind_a_checkpoint_does_not_deadlock_the_tree() {
	let dir = TempDir::new("ckpt_flush_parked").unwrap();
	let out = TempDir::new("ckpt_flush_parked_out").unwrap();
	let tree = open(dir.path(), 32 * 1024);
	let before = put(&tree, 0..300).await;
	flush_to_tables(&tree);

	let gate = gate_checkpoint(&tree, CheckpointStage::SstablesCopied);
	let cp = out.path().join("cp");
	let taking = checkpoint(&tree, &cp);
	assert_eq!(gate.next(REACHED), Some(0), "the checkpoint reaches its hook");
	// Released from a plain thread: with the manifest write-locked behind the checkpoint, the
	// runtime's workers can all be blocked, and then no timer of the runtime fires.
	let releaser = std::thread::spawn(move || {
		std::thread::sleep(Duration::from_secs(1));
		gate.release();
	});

	// A flush that wants the manifest for writing, a reader and a rotating committer.
	put(&tree, 300..320).await;
	tree.core.inner.rotate_memtable().unwrap();
	let flush = flush_all(&tree);
	let reader = {
		let tree = Arc::clone(&tree);
		on_thread(move || contents(&tree))
	};
	let committer = {
		let tree = Arc::clone(&tree);
		tokio::spawn(async move { put(&tree, 400..700).await })
	};

	let metadata = taking.wait().unwrap();
	flush.wait().unwrap();
	let read = reader.wait();
	tokio::time::timeout(STUCK, committer)
		.await
		.expect("a parked checkpoint deadlocked the tree")
		.unwrap();
	releaser.join().unwrap();
	assert!(read.len() >= before.len());
	assert!(metadata.sstable_count >= 1);

	clear_hooks(&tree);
	let copy = open_checkpoint(&cp);
	assert_contents(&copy, &before, "checkpoint copy");
	copy.close().await.unwrap();
	tree.close().await.unwrap();
}

/// On a runtime with one thread, a checkpoint from another thread while the background flush
/// task is woken must finish, and the runtime must come back.
#[test(tokio::test(flavor = "current_thread"))]
async fn a_checkpoint_beside_the_background_flush_on_a_single_threaded_runtime_finishes() {
	let dir = TempDir::new("ckpt_flush_current").unwrap();
	let out = TempDir::new("ckpt_flush_current_out").unwrap();
	let tree = open(dir.path(), 32 * 1024);
	let mut expected = Contents::new();
	for round in 0..4 {
		expected.extend(put(&tree, round * 100..(round + 1) * 100).await);
		tree.core.inner.rotate_memtable().unwrap();
		if let Some(tm) = tree.core.task_manager.lock().unwrap().as_ref() {
			tm.wake_up_memtable();
		}
	}
	let cp = out.path().join("cp");
	let finished = {
		let (tree, cp) = (Arc::clone(&tree), cp.clone());
		tokio::time::timeout(
			STUCK,
			tokio::task::spawn_blocking(move || tree.create_checkpoint(&cp)),
		)
		.await
	};
	finished.expect("the checkpoint did not finish").unwrap().unwrap();
	let copy = open_checkpoint(&cp);
	assert_contents(&copy, &expected, "checkpoint copy");
	copy.close().await.unwrap();
	assert_contents(&tree, &expected, "live tree");
	tree.close().await.unwrap();
}

/// Every checkpoint of a tree that is written to between checkpoints, but never fills a
/// memtable, flushes one more level-0 table on the caller's thread. Nothing else flushes, so
/// the compactor must be woken by the checkpoint: once the tables reach the write-stall
/// threshold every commit waits for a compaction nobody asks for.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn commits_still_succeed_after_many_checkpoints_of_a_tree_that_never_rotates_by_itself() {
	let dir = TempDir::new("ckpt_flush_l0").unwrap();
	let out = TempDir::new("ckpt_flush_l0_out").unwrap();
	let tree = open_with(Arc::new(Options {
		path: dir.path().to_path_buf(),
		level0_max_files: 2,
		l0_stall_threshold: 4,
		..Default::default()
	}));
	let stall_at = tree.core.inner.opts.l0_stall_threshold;

	let mut expected = Contents::new();
	for round in 0..stall_at + 3 {
		let committed = tokio::time::timeout(Duration::from_secs(10), async {
			put(&tree, round * 5..(round + 1) * 5).await
		})
		.await;
		match committed {
			Ok(written) => expected.extend(written),
			Err(_) => panic!(
				"round {round}: a commit waits for the compaction of {} level-0 tables",
				tree.core.inner.l0_file_count()
			),
		}
		checkpoint(&tree, &out.path().join(format!("cp{round}"))).wait().unwrap();
	}
	assert_contents(&tree, &expected, "live tree");
	tree.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// checkpoints racing the background flush
// ---------------------------------------------------------------------------

const WRITERS: usize = 4;
const ROUNDS: usize = 8;
const KEYS_PER_WRITER_PER_ROUND: usize = 50;

/// What the writers had acknowledged: (writer, key index).
type Acked = Arc<Mutex<Vec<(usize, usize)>>>;
type Failures = Arc<Mutex<Vec<String>>>;

fn stress_key(writer: usize, i: usize) -> Vec<u8> {
	format!("w{writer}_{i:06}").into_bytes()
}

fn stress_val(writer: usize, i: usize) -> Vec<u8> {
	let mut v = format!("v{writer}_{i:06}_").into_bytes();
	v.resize(120 + (i * 37 + writer * 11) % 300, b'z');
	v
}

/// How many of `acked` the tree does not hold with the right value.
fn missing_in(tree: &Tree, acked: &[(usize, usize)]) -> usize {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	acked
		.iter()
		.filter(
			|(w, i)| !matches!(txn.get(stress_key(*w, *i)), Ok(Some(v)) if v == stress_val(*w, *i)),
		)
		.count()
}

/// One writer per `WRITERS`, each committing the keys in `keys` one transaction at a time.
fn spawn_writers(
	tree: &Arc<Tree>,
	keys: Range<usize>,
	acked: &Acked,
	failures: &Failures,
) -> Vec<tokio::task::JoinHandle<()>> {
	(0..WRITERS)
		.map(|w| {
			let (tree, acked, failures) =
				(Arc::clone(tree), Arc::clone(acked), Arc::clone(failures));
			let keys = keys.clone();
			tokio::spawn(async move {
				for i in keys {
					let mut txn = tree.begin().unwrap();
					txn.set(stress_key(w, i), stress_val(w, i)).unwrap();
					match txn.commit().await {
						Ok(()) => acked.lock().unwrap().push((w, i)),
						Err(e) => {
							failures.lock().unwrap().push(format!("commit {w}/{i}: {e:?}"));
							return;
						}
					}
				}
			})
		})
		.collect()
}

/// Rotates the memtable from outside the commit pipeline, and wakes the background flush as the
/// pipeline does, until `stop`.
fn spawn_rotator(tree: &Arc<Tree>, stop: &Arc<AtomicBool>) -> tokio::task::JoinHandle<()> {
	let (tree, stop) = (Arc::clone(tree), Arc::clone(stop));
	tokio::task::spawn_blocking(move || {
		let mut n = 0u64;
		while !stop.load(Ordering::SeqCst) {
			// Not faster than the flush can follow, or the queue only grows.
			if immutables(&tree) < 2 {
				tree.core.inner.rotate_memtable().unwrap();
				if let Some(tm) = tree.core.task_manager.lock().unwrap().as_ref() {
					tm.wake_up_memtable();
				}
			}
			n += 1;
			std::thread::sleep(Duration::from_millis(15 + 5 * (n % 3)));
		}
	})
}

/// Opens the checkpoint at `path` and records a failure unless it holds every one of `before`.
async fn check_checkpoint(path: &Path, before: &[(usize, usize)], failures: &Failures) {
	match Tree::new(options(path, NEVER_ROTATING, false)) {
		Err(e) => failures.lock().unwrap().push(format!("open checkpoint: {e:?}")),
		Ok(copy) => {
			let missing = missing_in(&copy, before);
			if missing > 0 {
				failures.lock().unwrap().push(format!(
					"the checkpoint lacks {missing} of the {} commits acknowledged before it",
					before.len()
				));
			}
			copy.close().await.unwrap();
		}
	}
}

/// Rounds of writers that fill small memtables, so the pipeline rotates and wakes the background
/// flush all the time, while a thread rotates by hand and a checkpoint is taken. Nothing may
/// fail, every other checkpoint opens and holds every commit acknowledged before it started,
/// and the tree keeps every commit across a close and a reopen.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn checkpoints_racing_the_background_flush_never_fail() {
	let dir = TempDir::new("ckpt_flush_stress").unwrap();
	let checkpoints = TempDir::new("ckpt_flush_stress_out").unwrap();
	let opts = Arc::new(Options {
		path: dir.path().to_path_buf(),
		max_memtable_size: 32 * 1024,
		memtable_stall_threshold: 4,
		..Default::default()
	});
	let tree = open_with(Arc::clone(&opts));
	let (acked, failures): (Acked, Failures) = Default::default();

	for round in 0..ROUNDS {
		let keys = round * KEYS_PER_WRITER_PER_ROUND..(round + 1) * KEYS_PER_WRITER_PER_ROUND;
		let writers = spawn_writers(&tree, keys, &acked, &failures);
		let stop = Arc::new(AtomicBool::new(false));
		let rotator = spawn_rotator(&tree, &stop);

		// A checkpoint while the writers run, starting a little later each round.
		tokio::time::sleep(Duration::from_millis(5 * (round as u64 % 6))).await;
		let before = acked.lock().unwrap().clone();
		let path = checkpoints.path().join(format!("cp{round}"));
		let result = checkpoint(&tree, &path).wait();
		stop.store(true, Ordering::SeqCst);
		rotator.await.unwrap();
		let finished = tokio::time::timeout(STUCK, async {
			for writer in writers {
				writer.await.unwrap();
			}
		})
		.await;
		assert!(finished.is_ok(), "round {round}: the writers did not finish");

		match result {
			Err(e) => failures.lock().unwrap().push(format!("create_checkpoint: {e:?}")),
			Ok(_) if round % 2 == 0 => check_checkpoint(&path, &before, &failures).await,
			Ok(_) => {}
		}
		let _ = std::fs::remove_dir_all(&path);
		if !failures.lock().unwrap().is_empty() {
			break;
		}
	}

	let failures = failures.lock().unwrap().clone();
	assert!(
		failures.is_empty(),
		"{} failures, first few: {:?}",
		failures.len(),
		&failures[..failures.len().min(5)]
	);
	assert!(tree.core.inner.error_handler.check_error().is_ok(), "a background error was recorded");

	let acked = acked.lock().unwrap().clone();
	assert_eq!(missing_in(&tree, &acked), 0, "live tree: acknowledged commits are wrong");
	tree.close().await.unwrap();
	drop(tree);
	let reopened = Tree::new(opts).unwrap();
	assert_eq!(missing_in(&reopened, &acked), 0, "reopened tree: acknowledged commits are wrong");
	reopened.close().await.unwrap();
}

/// The version of the value `writer` last wrote to key `i`, from the value's own text.
fn version_of(value: &[u8]) -> Option<usize> {
	let s = std::str::from_utf8(value).ok()?;
	s.split('_').nth(2)?.parse().ok()
}

fn versioned_val(writer: usize, i: usize, version: usize) -> Vec<u8> {
	let mut v = format!("v{writer}_{i:06}_{version:06}_").into_bytes();
	v.resize(150 + (i * 31 + writer * 7) % 200, b'q');
	v
}

/// What the checkpoint copy at `path` holds for the keys of `floor`: every key must be there with
/// a version at least as new as the one acknowledged before the checkpoint began.
fn check_copy(path: &Path, floor: &BTreeMap<(usize, usize), usize>) -> Vec<String> {
	let mut problems = Vec::new();
	let copy = match Tree::new(options(path, NEVER_ROTATING, false)) {
		Ok(copy) => copy,
		Err(e) => return vec![format!("open: {e:?}")],
	};
	let txn = copy.begin_with_mode(Mode::ReadOnly).unwrap();
	for (&(w, i), &version) in floor {
		match txn.get(stress_key(w, i)) {
			Ok(Some(v)) => match version_of(&v) {
				Some(got) if got >= version => {}
				got => problems.push(format!("key {w}/{i}: version {got:?} < {version}")),
			},
			Ok(None) => problems.push(format!("key {w}/{i}: missing")),
			Err(e) => problems.push(format!("key {w}/{i}: {e:?}")),
		}
		if problems.len() > 5 {
			break;
		}
	}
	drop(txn);
	// Dropped off the runtime: a `Tree` closes its store when it is dropped on one.
	std::thread::spawn(move || drop(copy)).join().ok();
	problems
}

/// Several checkpoints taken at once while writers commit and the memtable keeps rotating: each
/// one succeeds, opens, and holds every commit acknowledged before it started.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn concurrent_checkpoints_each_hold_what_was_acknowledged_before_them() {
	let dir = TempDir::new("ckpt_flush_many").unwrap();
	let out = TempDir::new("ckpt_flush_many_out").unwrap();
	let tree = open_with(Arc::new(Options {
		path: dir.path().to_path_buf(),
		max_memtable_size: 32 * 1024,
		memtable_stall_threshold: 4,
		..Default::default()
	}));

	let acked: Arc<Mutex<BTreeMap<(usize, usize), usize>>> = Default::default();
	let stop = Arc::new(AtomicBool::new(false));
	let failures: Failures = Default::default();
	let writers: Vec<_> = (0..WRITERS)
		.map(|w| {
			let (tree, acked, stop, failures) =
				(Arc::clone(&tree), Arc::clone(&acked), Arc::clone(&stop), Arc::clone(&failures));
			tokio::spawn(async move {
				let mut version = 0;
				while !stop.load(Ordering::SeqCst) {
					version += 1;
					for i in 0..40 {
						let mut txn = tree.begin().unwrap();
						txn.set(stress_key(w, i), versioned_val(w, i, version)).unwrap();
						match txn.commit().await {
							Ok(()) => {
								acked.lock().unwrap().insert((w, i), version);
							}
							Err(e) => {
								failures.lock().unwrap().push(format!("commit: {e:?}"));
								return;
							}
						}
					}
				}
			})
		})
		.collect();

	for round in 0..4 {
		tokio::time::sleep(Duration::from_millis(150)).await;
		let floor = acked.lock().unwrap().clone();
		let takers: Vec<_> = (0..3)
			.map(|n| {
				let path = out.path().join(format!("cp{round}_{n}"));
				let call = checkpoint(&tree, &path);
				(path, call)
			})
			.collect();
		for (path, call) in takers {
			match call.wait() {
				Err(e) => failures.lock().unwrap().push(format!("create_checkpoint: {e:?}")),
				Ok(_) => {
					let floor = floor.clone();
					let problems = tokio::task::spawn_blocking(move || check_copy(&path, &floor))
						.await
						.unwrap();
					failures.lock().unwrap().extend(problems);
				}
			}
		}
		if !failures.lock().unwrap().is_empty() {
			break;
		}
	}
	stop.store(true, Ordering::SeqCst);
	for w in writers {
		w.await.unwrap();
	}
	let failures = failures.lock().unwrap().clone();
	assert!(
		failures.is_empty(),
		"{} failures, first: {:?}",
		failures.len(),
		&failures[..failures.len().min(5)]
	);
	assert!(tree.core.inner.error_handler.check_error().is_ok());
	tree.close().await.unwrap();
}
