//! A checkpoint and `close()` at the same time.
//!
//! `close()` closes the value log and the WAL and releases the directory lock, and a checkpoint
//! flushes memtables and copies files that those steps would pull from under it. So `close()`
//! waits for the checkpoints that are running, and a checkpoint that begins once `close()` has
//! begun is refused. The deterministic tests hold a checkpoint at a step of its path while
//! `close()` runs, or hold `close()` while a checkpoint begins. The stress test races both
//! with random pauses inside the checkpoint.
//!
//! The hooks park threads, so the tests run on the multi-threaded runtime unless they are about
//! the single-threaded one. Every wait is bounded, so a regression fails a test instead of
//! hanging the run.

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Range;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::task::{Context, Waker};
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;
use tokio::task::JoinHandle;

use super::collect_transaction_all;
use crate::checkpoint::{CheckpointGate, CheckpointMetadata, CheckpointStage};
use crate::lsm::Tree;
use crate::ring::PipelineHook;
use crate::{Durability, Error, Mode, Options, Result};

/// How long a test waits for something that has to happen before it calls it a hang.
const LONG: Duration = Duration::from_secs(30);

/// Long enough for a `close()` that is going to return to have returned.
const GRACE: Duration = Duration::from_millis(500);

type Contents = BTreeMap<Vec<u8>, Vec<u8>>;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

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

fn options(path: &Path, vlog: bool) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		enable_vlog: vlog,
		// A clean close must leave the WAL in place, so the reopen replays it.
		flush_on_close: false,
		..Default::default()
	})
}

fn open(path: &Path, vlog: bool) -> Arc<Tree> {
	Arc::new(Tree::new(options(path, vlog)).unwrap())
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

/// Opens the checkpoint in `path`, checks it holds exactly `expected`, and closes it.
async fn assert_copy(path: &Path, vlog: bool, expected: &Contents, what: &str) {
	let copy = Tree::new(options(path, vlog)).unwrap();
	assert_contents(&copy, expected, what);
	within("closing the copy", copy.close()).await.unwrap();
}

/// Reopens the closed tree from its directory.
fn reopen(tree: Arc<Tree>, opts: &Arc<Options>) -> Tree {
	// The tree is closed: dropping the last handle runs no second close.
	assert!(Arc::strong_count(&tree) == 1, "a task still holds the tree");
	drop(tree);
	Tree::new(Arc::clone(opts)).unwrap()
}

/// Parks every thread that calls `wait` until `open`, or until `LONG` has passed.
#[derive(Default)]
struct Gate {
	open: Mutex<bool>,
	opened: Condvar,
	/// How many callers reached `wait`.
	reached: AtomicUsize,
}

impl Gate {
	fn wait(&self) {
		self.reached.fetch_add(1, Ordering::SeqCst);
		let open = self.open.lock().unwrap();
		let _ = self.opened.wait_timeout_while(open, LONG, |open| !*open).unwrap();
	}

	fn open(&self) {
		*self.open.lock().unwrap() = true;
		self.opened.notify_all();
	}

	fn reached(&self) -> usize {
		self.reached.load(Ordering::SeqCst)
	}
}

/// Gates the test holds, opened when it ends, whichever way.
struct HeldAll(Vec<Arc<Gate>>);

impl Drop for HeldAll {
	fn drop(&mut self) {
		for gate in &self.0 {
			gate.open();
		}
	}
}

/// A gate the test holds. Dropping it opens the gate, so a test that fails does not leave a
/// thread parked inside a hook.
struct Held(Arc<Gate>);

impl std::ops::Deref for Held {
	type Target = Gate;

	fn deref(&self) -> &Gate {
		&self.0
	}
}

impl Drop for Held {
	fn drop(&mut self) {
		self.0.open();
	}
}

/// Where a checkpoint is held.
#[derive(Clone, Copy, Debug)]
enum Park {
	/// In the flush of its memtable: the SST is on disk and the manifest does not list it.
	InFlush,
	/// At a stage of the copy.
	At(CheckpointStage),
}

/// Holds every checkpoint that reaches `at`.
fn hold_checkpoints(tree: &Tree, at: Park) -> Held {
	let gate = Arc::new(Gate::default());
	let hook = Arc::clone(&gate);
	match at {
		Park::InFlush => {
			*tree.core.inner.flush_hook.lock() = Some(Arc::new(move |_| {
				hook.wait();
				Ok(())
			}));
		}
		Park::At(stage) => {
			*tree.core.inner.checkpoint_hook.lock() = Some(Arc::new(move |reached| {
				if reached == stage {
					hook.wait();
				}
			}));
		}
	}
	Held(gate)
}

/// A call on a thread of its own, without a runtime.
struct Call<T>(mpsc::Receiver<std::thread::Result<T>>);

fn on_thread<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Call<T> {
	let (tx, rx) = mpsc::channel();
	std::thread::spawn(move || {
		let _ = tx.send(catch_unwind(AssertUnwindSafe(f)));
	});
	Call(rx)
}

impl<T> Call<T> {
	/// The call's result, or the panic of the call, within `bound`.
	fn wait_for(self, bound: Duration) -> std::thread::Result<T> {
		match self.0.recv_timeout(bound) {
			Ok(result) => result,
			Err(_) => panic!("a call did not return within {bound:?}: a deadlock"),
		}
	}

	/// The call's result; a panic of the call is raised again here.
	fn wait(self) -> T {
		match self.wait_for(LONG) {
			Ok(value) => value,
			Err(panic) => resume_unwind(panic),
		}
	}

	/// Whether the call has returned, without taking its result.
	fn peek(&self) -> Option<std::thread::Result<T>> {
		self.0.try_recv().ok()
	}
}

fn checkpoint_on_thread(tree: &Arc<Tree>, path: &Path) -> Call<Result<CheckpointMetadata>> {
	let (tree, path) = (Arc::clone(tree), path.to_path_buf());
	on_thread(move || {
		assert!(
			tokio::runtime::Handle::try_current().is_err(),
			"the thread is meant to have no runtime"
		);
		tree.create_checkpoint(&path)
	})
}

fn spawn_close(tree: &Arc<Tree>) -> JoinHandle<Result<()>> {
	let tree = Arc::clone(tree);
	tokio::spawn(async move { tree.close().await })
}

async fn until(what: &str, mut condition: impl FnMut() -> bool) {
	let deadline = Instant::now() + LONG;
	while !condition() {
		assert!(Instant::now() < deadline, "timed out waiting for {what}");
		tokio::time::sleep(Duration::from_millis(2)).await;
	}
}

/// Awaits `future`, failing the test if it takes longer than `LONG`.
async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
	match tokio::time::timeout(LONG, future).await {
		Ok(value) => value,
		Err(_) => panic!("{what} did not finish within {LONG:?}"),
	}
}

/// Whether `error` is the refusal of a checkpoint on a tree that is closing or closed.
fn is_closed_refusal(error: &Error) -> bool {
	matches!(error, Error::Other(message) if message.contains("closed"))
}

// ---------------------------------------------------------------------------
// close() waits for the checkpoint
// ---------------------------------------------------------------------------

/// `close()` begins while a checkpoint is held at `at`. It does not return until the checkpoint
/// is over, the checkpoint completes with a copy that opens and matches the tree, and the tree
/// reopens with everything that was acknowledged.
async fn close_waits_for_the_checkpoint(at: Park, vlog: bool) {
	let root = TempDir::new("ckpt_close").unwrap();
	let (db, cp) = (root.path().join("db"), root.path().join("cp"));
	let opts = options(&db, vlog);
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let expected = put(&tree, 0..400).await;
	let gate = hold_checkpoints(&tree, at);

	let checkpoint = checkpoint_on_thread(&tree, &cp);
	until("the checkpoint to reach the hook", || gate.reached() == 1).await;
	let close = spawn_close(&tree);
	tokio::time::sleep(GRACE).await;
	assert!(
		!close.is_finished(),
		"close() returned while a checkpoint was held at {at:?} (vlog {vlog})"
	);
	gate.open();

	let metadata = checkpoint.wait();
	let metadata = metadata.unwrap_or_else(|e| panic!("checkpoint held at {at:?}: {e:?}"));
	assert!(metadata.sstable_count >= 1);
	within("close()", close).await.unwrap().unwrap();

	assert_copy(&cp, vlog, &expected, "checkpoint taken while close() waited").await;
	let reopened = reopen(tree, &opts);
	assert_contents(&reopened, &expected, "source after the reopen");
	reopened.close().await.unwrap();
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_waits_for_a_checkpoint_in_its_flush() {
	close_waits_for_the_checkpoint(Park::InFlush, false).await;
	close_waits_for_the_checkpoint(Park::InFlush, true).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_waits_for_a_checkpoint_copying_the_sstables() {
	close_waits_for_the_checkpoint(Park::At(CheckpointStage::SstablesCopied), false).await;
	close_waits_for_the_checkpoint(Park::At(CheckpointStage::SstablesCopied), true).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_waits_for_a_checkpoint_about_to_copy_the_value_log() {
	close_waits_for_the_checkpoint(Park::At(CheckpointStage::ManifestReleased), false).await;
	close_waits_for_the_checkpoint(Park::At(CheckpointStage::ManifestReleased), true).await;
}

/// The directory lock is still held while the checkpoint runs, so no other process can open the
/// database between the checkpoint's flush and the end of the close.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn the_directory_lock_is_held_until_the_checkpoint_is_over() {
	let root = TempDir::new("ckpt_close_lock").unwrap();
	let (db, cp) = (root.path().join("db"), root.path().join("cp"));
	let opts = options(&db, false);
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	put(&tree, 0..200).await;
	let gate = hold_checkpoints(&tree, Park::InFlush);

	let checkpoint = checkpoint_on_thread(&tree, &cp);
	until("the checkpoint to reach its flush", || gate.reached() == 1).await;
	let close = spawn_close(&tree);
	tokio::time::sleep(GRACE).await;
	assert!(
		Tree::new(Arc::clone(&opts)).is_err(),
		"the database could be opened while a checkpoint was still flushing it"
	);
	gate.open();
	checkpoint.wait().unwrap();
	within("close()", close).await.unwrap().unwrap();
	within("closing the reopened tree", reopen(tree, &opts).close()).await.unwrap();
}

/// Checkpoints do not exclude each other: one that is held does not keep a second one waiting.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_held_checkpoint_does_not_stop_another() {
	let root = TempDir::new("ckpt_close_two").unwrap();
	let opts = options(&root.path().join("db"), false);
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let expected = put(&tree, 0..300).await;

	let gate = Arc::new(Gate::default());
	let first = AtomicBool::new(true);
	let hook = Arc::clone(&gate);
	*tree.core.inner.checkpoint_hook.lock() = Some(Arc::new(move |stage| {
		if stage == CheckpointStage::ManifestReleased && first.swap(false, Ordering::SeqCst) {
			hook.wait();
		}
	}));
	let gate = Held(gate);

	let held = checkpoint_on_thread(&tree, &root.path().join("held"));
	until("the first checkpoint to be held", || gate.reached() == 1).await;
	checkpoint_on_thread(&tree, &root.path().join("other")).wait().unwrap();
	assert!(held.peek().is_none(), "the first checkpoint is still held");
	gate.open();
	held.wait().unwrap();

	for name in ["held", "other"] {
		assert_copy(&root.path().join(name), false, &expected, name).await;
	}
	within("close()", tree.close()).await.unwrap();
}

/// `close()` waiting for a checkpoint leaves the runtime free: on a runtime with one thread,
/// other tasks keep running, and the checkpoint, which needs no runtime, finishes.
#[test(tokio::test(flavor = "current_thread"))]
async fn close_waiting_for_a_checkpoint_does_not_block_the_runtime() {
	let root = TempDir::new("ckpt_close_rt").unwrap();
	let (db, cp) = (root.path().join("db"), root.path().join("cp"));
	let opts = options(&db, false);
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let expected = put(&tree, 0..300).await;
	let gate = hold_checkpoints(&tree, Park::At(CheckpointStage::SstablesCopied));

	let checkpoint = checkpoint_on_thread(&tree, &cp);
	until("the checkpoint to reach the hook", || gate.reached() == 1).await;
	let close = spawn_close(&tree);
	let ticks = Arc::new(AtomicUsize::new(0));
	let ticker = {
		let ticks = Arc::clone(&ticks);
		tokio::spawn(async move {
			loop {
				tokio::time::sleep(Duration::from_millis(5)).await;
				ticks.fetch_add(1, Ordering::SeqCst);
			}
		})
	};
	// Only reached if the waiting close() has handed the one runtime thread back.
	tokio::time::sleep(GRACE).await;
	let ticked = ticks.load(Ordering::SeqCst);
	ticker.abort();
	assert!(ticked >= 5, "the runtime made {ticked} ticks in {GRACE:?} while close() waited");
	assert!(!close.is_finished());
	gate.open();

	checkpoint.wait().unwrap();
	within("close()", close).await.unwrap().unwrap();
	assert_copy(&cp, false, &expected, "checkpoint taken while close() waited").await;
	let reopened = reopen(tree, &opts);
	assert_contents(&reopened, &expected, "source after the reopen");
	reopened.close().await.unwrap();
}

/// A `close()` future that is dropped while it waits for a checkpoint leaves the checkpoint to
/// finish, and a `close()` after it completes the shutdown.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_close_dropped_while_waiting_for_a_checkpoint_can_be_repeated() {
	let root = TempDir::new("ckpt_close_drop").unwrap();
	let (db, cp) = (root.path().join("db"), root.path().join("cp"));
	let opts = options(&db, true);
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let expected = put(&tree, 0..400).await;
	let gate = hold_checkpoints(&tree, Park::At(CheckpointStage::SstablesCopied));

	let checkpoint = checkpoint_on_thread(&tree, &cp);
	until("the checkpoint to reach the hook", || gate.reached() == 1).await;
	let close = spawn_close(&tree);
	tokio::time::sleep(GRACE).await;
	assert!(!close.is_finished(), "close() returned while a checkpoint was held");
	close.abort();
	assert!(close.await.unwrap_err().is_cancelled());
	gate.open();

	checkpoint.wait().unwrap();
	within("the second close()", tree.close()).await.unwrap();
	assert_copy(&cp, true, &expected, "checkpoint finished after the dropped close()").await;
	let reopened = reopen(tree, &opts);
	assert_contents(&reopened, &expected, "source after the reopen");
	reopened.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// a checkpoint that begins once close() has
// ---------------------------------------------------------------------------

/// `close()` is held waiting for the flusher, which is inside a group, when a checkpoint begins:
/// it fails at once with a clear error and leaves no destination behind, while `close()` is
/// still going. The commit in the group, and the data from before, survive.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_that_begins_while_close_is_running_is_refused() {
	let root = TempDir::new("ckpt_close_late").unwrap();
	let (db, cp) = (root.path().join("db"), root.path().join("cp"));
	let opts = options(&db, false);
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let mut expected = put(&tree, 0..200).await;

	let flusher = Arc::new(Gate::default());
	let hook = Arc::clone(&flusher);
	let first = AtomicBool::new(true);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if matches!(point, PipelineHook::AfterWalSync { .. }) && first.swap(false, Ordering::SeqCst)
		{
			hook.wait();
		}
	})));
	let flusher = Held(flusher);
	let commit = {
		let tree = Arc::clone(&tree);
		tokio::spawn(async move {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(Durability::Immediate);
			txn.set(b"late", b"late").unwrap();
			txn.commit().await
		})
	};
	until("the flusher to take the group", || flusher.reached() == 1).await;
	let close = spawn_close(&tree);
	until("the shutdown to begin", || tree.core.commit_pipeline.shutdown.load(Ordering::SeqCst))
		.await;

	let outcome = checkpoint_on_thread(&tree, &cp).wait();
	let error = outcome.expect_err("a checkpoint began after close() had");
	assert!(is_closed_refusal(&error), "the refusal does not say the tree is closed: {error:?}");
	assert!(!cp.exists(), "the refused checkpoint created its destination");
	assert!(!close.is_finished(), "the flusher is still held");

	flusher.open();
	within("the commit", commit).await.unwrap().unwrap();
	within("close()", close).await.unwrap().unwrap();
	// The commit that was in the group when close() began is acknowledged, so it is kept.
	expected.insert(b"late".to_vec(), b"late".to_vec());
	let reopened = reopen(tree, &opts);
	assert_contents(&reopened, &expected, "source after the reopen");
	reopened.close().await.unwrap();
}

/// A checkpoint on a closed tree fails with the same error, from any thread, every time, and
/// touches nothing: not the directory it was asked to write, not the closed database.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_on_a_closed_tree_is_refused() {
	for vlog in [false, true] {
		let root = TempDir::new("ckpt_close_after").unwrap();
		let (db, cp) = (root.path().join("db"), root.path().join("cp"));
		let opts = options(&db, vlog);
		let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
		let expected = put(&tree, 0..300).await;
		within("close()", tree.close()).await.unwrap();

		// Without a runtime, on a runtime thread, and into a directory that holds a file.
		let error = checkpoint_on_thread(&tree, &cp).wait().expect_err("on a thread");
		assert!(is_closed_refusal(&error), "{error:?}");
		let error = tree.create_checkpoint(&cp).expect_err("on the runtime");
		assert!(is_closed_refusal(&error), "{error:?}");
		let existing = root.path().join("existing");
		std::fs::create_dir(&existing).unwrap();
		std::fs::write(existing.join("keep"), b"keep").unwrap();
		let error = tree.create_checkpoint(&existing).expect_err("into an existing directory");
		assert!(is_closed_refusal(&error), "{error:?}");

		assert!(!cp.exists(), "a refused checkpoint created its destination");
		let names: Vec<_> =
			std::fs::read_dir(&existing).unwrap().map(|e| e.unwrap().file_name()).collect();
		assert_eq!(names, vec![std::ffi::OsString::from("keep")]);
		within("a second close()", tree.close()).await.unwrap();
		let reopened = reopen(tree, &opts);
		assert_contents(&reopened, &expected, "source after the reopen");
		reopened.close().await.unwrap();
	}
}

// ---------------------------------------------------------------------------
// the gate: several checkpoints, failures, and a close() that is given up
// ---------------------------------------------------------------------------

/// `close()` is waiting for a checkpoint that is held when a second one begins: the second is
/// refused at once, and neither it nor `close()` is allowed to starve the other.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_that_begins_while_close_waits_for_another_is_refused() {
	let root = TempDir::new("ckpt_close_second").unwrap();
	let (db, first, second) =
		(root.path().join("db"), root.path().join("first"), root.path().join("second"));
	let opts = options(&db, false);
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let expected = put(&tree, 0..300).await;

	let gate = Arc::new(Gate::default());
	let hook = Arc::clone(&gate);
	*tree.core.inner.checkpoint_hook.lock() = Some(Arc::new(move |stage| {
		if stage == CheckpointStage::SstablesCopied {
			hook.wait();
		}
	}));
	let held = HeldAll(vec![Arc::clone(&gate)]);

	let running = checkpoint_on_thread(&tree, &first);
	until("the first checkpoint to be held", || gate.reached() == 1).await;
	let close = spawn_close(&tree);
	tokio::time::sleep(GRACE).await;
	assert!(!close.is_finished(), "close() returned while a checkpoint was held");

	// Bounded far below `LONG`: an accepted checkpoint parks in the hook.
	let outcome = checkpoint_on_thread(&tree, &second)
		.wait_for(Duration::from_secs(5))
		.expect("the second checkpoint panicked");
	let error = outcome.expect_err("a checkpoint began while close() was waiting");
	assert!(is_closed_refusal(&error), "{error:?}");
	assert!(!second.exists(), "the refused checkpoint created its destination");
	assert_eq!(gate.reached(), 1, "the refused checkpoint ran its steps");

	drop(held);
	running.wait().unwrap();
	within("close()", close).await.unwrap().unwrap();
	assert_copy(&first, false, &expected, "the checkpoint close() waited for").await;
}

/// Three checkpoints are held; `close()` returns after the last of them ends, not the first.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_waits_for_the_last_of_several_checkpoints() {
	let root = TempDir::new("ckpt_close_many").unwrap();
	let opts = options(&root.path().join("db"), false);
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let expected = put(&tree, 0..300).await;

	// The n-th checkpoint to reach the hook parks on its own gate.
	let gates: Vec<Arc<Gate>> = (0..3).map(|_| Arc::new(Gate::default())).collect();
	let arrivals = Arc::new(AtomicUsize::new(0));
	let (hook_gates, hook_arrivals) = (gates.clone(), Arc::clone(&arrivals));
	*tree.core.inner.checkpoint_hook.lock() = Some(Arc::new(move |stage| {
		if stage == CheckpointStage::SstablesCopied {
			let n = hook_arrivals.fetch_add(1, Ordering::SeqCst);
			hook_gates[n].wait();
		}
	}));
	let _held = HeldAll(gates.clone());

	let paths: Vec<PathBuf> = (0..3).map(|i| root.path().join(format!("cp{i}"))).collect();
	let calls: Vec<_> = paths.iter().map(|p| checkpoint_on_thread(&tree, p)).collect();
	until("the checkpoints to be held", || arrivals.load(Ordering::SeqCst) == 3).await;
	let close = spawn_close(&tree);

	for (n, gate) in gates.iter().enumerate().take(2) {
		gate.open();
		tokio::time::sleep(GRACE).await;
		let running = 2 - n;
		assert!(!close.is_finished(), "close() returned with {running} checkpoints still running");
		// `close()` can also be held up later, by locks that the parked checkpoints keep, so it
		// is the start of its shutdown that shows it is still waiting for them.
		assert!(
			!tree.core.is_closed.load(Ordering::SeqCst)
				&& !tree.core.commit_pipeline.shutdown.load(Ordering::SeqCst),
			"close() went on to shut the tree down with {running} checkpoints still running"
		);
	}
	gates[2].open();
	for call in calls {
		call.wait().unwrap();
	}
	within("close()", close).await.unwrap().unwrap();
	for path in &paths {
		assert_copy(path, false, &expected, "one of the checkpoints close() waited for").await;
	}
}

/// A checkpoint that returns an error, or panics, gives up its place: `close()` does not wait
/// for it, whatever the way it ended. That includes the refusal of a database that a commit group
/// stopped, which happens after the checkpoint has entered the gate.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_that_fails_or_panics_does_not_keep_close_waiting() {
	for how in ["refused", "stopped", "file", "flush", "panic"] {
		let root = TempDir::new("ckpt_close_fail").unwrap();
		let (db, cp) = (root.path().join("db"), root.path().join("cp"));
		let opts = options(&db, false);
		let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
		let expected = put(&tree, 0..200).await;

		match how {
			"refused" => {
				let error = tree.create_checkpoint(&db).unwrap_err();
				assert!(matches!(error, Error::InvalidArgument(_)), "{error:?}");
			}
			"stopped" => {
				tree.core
					.inner
					.error_handler
					.stop_commit_group(Error::DatabaseStopped("a commit group failed".to_string()));
				let error = tree.create_checkpoint(&cp).unwrap_err();
				assert!(matches!(error, Error::DatabaseStopped(_)), "{error:?}");
				assert!(!cp.exists(), "a refused checkpoint created its destination");
			}
			"file" => {
				std::fs::write(&cp, b"a file").unwrap();
				tree.create_checkpoint(&cp).unwrap_err();
			}
			"flush" => {
				*tree.core.inner.flush_hook.lock() =
					Some(Arc::new(|_| Err(Error::Other("flush failed".to_string()))));
				tree.create_checkpoint(&cp).unwrap_err();
				*tree.core.inner.flush_hook.lock() = None;
			}
			"panic" => {
				*tree.core.inner.checkpoint_hook.lock() = Some(Arc::new(|stage| {
					if stage == CheckpointStage::SstablesCopied {
						panic!("the checkpoint hook panics");
					}
				}));
				let outcome = checkpoint_on_thread(&tree, &cp).wait_for(LONG);
				assert!(outcome.is_err(), "the hook did not panic");
				*tree.core.inner.checkpoint_hook.lock() = None;
			}
			_ => unreachable!(),
		}

		within(&format!("close() after a checkpoint that ended as {how}"), tree.close())
			.await
			.unwrap();
		let reopened = Tree::new(Arc::clone(&opts)).unwrap();
		assert_eq!(contents(&reopened), expected, "source after {how}");
		within("the second close()", reopened.close()).await.unwrap();
	}
}

/// REPRODUCTION (documented residual, fails on the fixed tree): a `close()` that is given up
/// while it waits for a checkpoint (a timeout, say) leaves a tree that is open, takes commits,
/// and refuses every later checkpoint as "closing or closed".
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_close_given_up_while_waiting_does_not_stop_later_checkpoints() {
	let root = TempDir::new("ckpt_close_timeout").unwrap();
	let (db, cp) = (root.path().join("db"), root.path().join("cp"));
	let opts = options(&db, false);
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let mut expected = put(&tree, 0..200).await;
	let gate = hold_checkpoints(&tree, Park::At(CheckpointStage::SstablesCopied));

	let held = checkpoint_on_thread(&tree, &root.path().join("held"));
	until("the checkpoint to be held", || gate.reached() == 1).await;
	let gave_up = tokio::time::timeout(Duration::from_millis(300), tree.close()).await;
	assert!(gave_up.is_err(), "close() was meant to be waiting");
	gate.open();
	held.wait().unwrap();

	assert!(!tree.core.is_closed.load(Ordering::SeqCst), "the tree was never closed");
	expected.extend(put(&tree, 200..230).await);
	let outcome = checkpoint_on_thread(&tree, &cp).wait();
	assert!(
		outcome.is_ok(),
		"a checkpoint of a tree that is open and takes commits was refused: {outcome:?}"
	);
	within("close()", tree.close()).await.unwrap();
}

/// The last checkpoint ends at the moment `close()` starts to wait for it: the wake-up is kept for
/// a `close()` that is not waiting yet, so it never sleeps through it. Both threads spin until
/// they are released together, and the checkpoint ends after a different number of spins each
/// round, so that it ends at every point of the first poll of `close()` in turn. `close()` is
/// polled by hand, so that nothing sits between the release and that poll.
#[test]
fn the_gate_does_not_lose_the_wake_up_of_a_checkpoint_that_ends_as_close_begins() {
	let env = |name: &str, default: u64| {
		std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
	};
	let (rounds, span) = (env("GATE_ROUNDS", 20_000), env("GATE_SPAN", 600));
	let mut cx = Context::from_waker(Waker::noop());
	for round in 0..rounds {
		let gate = CheckpointGate::default();
		let (ready, go) = (AtomicBool::new(false), AtomicBool::new(false));
		let guard = gate.enter().unwrap();
		let spins = (round * 37) % span;
		std::thread::scope(|scope| {
			scope.spawn(|| {
				ready.store(true, Ordering::SeqCst);
				while !go.load(Ordering::Acquire) {
					std::hint::spin_loop();
				}
				for _ in 0..spins {
					std::hint::spin_loop();
				}
				drop(guard);
			});
			let mut close = std::pin::pin!(gate.close());
			while !ready.load(Ordering::SeqCst) {
				std::hint::spin_loop();
			}
			go.store(true, Ordering::Release);
			let deadline = Instant::now() + Duration::from_secs(5);
			while close.as_mut().poll(&mut cx).is_pending() {
				assert!(
					Instant::now() < deadline,
					"round {round}: close() slept through the end of the checkpoint"
				);
				std::hint::spin_loop();
			}
		});
	}
}

// ---------------------------------------------------------------------------
// close() with commits, crashes and other callers
// ---------------------------------------------------------------------------

/// Commits that begin while `close()` waits for a checkpoint are acknowledged, and the ones
/// acknowledged survive the close and a reopen. (The pipeline stays up until the checkpoint is
/// over; the doc of `Tree::close` says a commit that begins after the close fails.)
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn commits_that_begin_while_close_waits_for_a_checkpoint_survive() {
	for vlog in [false, true] {
		let root = TempDir::new("ckpt_close_commits").unwrap();
		let (db, cp) = (root.path().join("db"), root.path().join("cp"));
		let opts = options(&db, vlog);
		let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
		let mut expected = put(&tree, 0..200).await;
		let gate = hold_checkpoints(&tree, Park::At(CheckpointStage::SstablesCopied));

		let checkpoint = checkpoint_on_thread(&tree, &cp);
		until("the checkpoint to be held", || gate.reached() == 1).await;
		let close = spawn_close(&tree);
		tokio::time::sleep(GRACE).await;

		let mut accepted = 0;
		for i in 200..260 {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(if i % 2 == 0 {
				Durability::Immediate
			} else {
				Durability::Eventual
			});
			txn.set(key_of(i), val_of(i)).unwrap();
			match txn.commit().await {
				Ok(()) => {
					expected.insert(key_of(i), val_of(i));
					accepted += 1;
				}
				Err(_) => break,
			}
		}
		eprintln!("{accepted} commits acknowledged while close() waited (vlog {vlog})");
		gate.open();
		checkpoint.wait().unwrap();
		within("close()", close).await.unwrap().unwrap();
		let reopened = reopen(tree, &opts);
		assert_contents(&reopened, &expected, "source after the reopen");
		reopened.close().await.unwrap();
	}
}

fn copy_dir(from: &Path, to: &Path) {
	std::fs::create_dir_all(to).unwrap();
	for entry in std::fs::read_dir(from).unwrap() {
		let entry = entry.unwrap();
		let target = to.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &target);
		} else {
			std::fs::copy(entry.path(), &target).unwrap();
		}
	}
}

/// A crash image taken while a checkpoint is parked in its flush (its table is on disk and the
/// manifest does not list it yet) and `close()` is waiting for it recovers every commit that had
/// been acknowledged, the ones made while the checkpoint was parked included.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_crash_image_while_close_waits_for_a_checkpoint_recovers_every_acknowledged_commit() {
	for vlog in [false, true] {
		let root = TempDir::new("ckpt_close_crashimage").unwrap();
		let (db, cp, image) =
			(root.path().join("db"), root.path().join("cp"), root.path().join("image"));
		let opts = options(&db, vlog);
		let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
		let mut expected = put(&tree, 0..300).await;
		let before_checkpoint = expected.clone();
		let gate = hold_checkpoints(&tree, Park::InFlush);

		let checkpoint = checkpoint_on_thread(&tree, &cp);
		until("the checkpoint to be parked in its flush", || gate.reached() == 1).await;
		let close = spawn_close(&tree);
		for i in 300..340 {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(Durability::Immediate);
			txn.set(key_of(i), val_of(i)).unwrap();
			txn.commit().await.unwrap();
			expected.insert(key_of(i), val_of(i));
		}
		tokio::time::sleep(GRACE).await;
		assert!(!close.is_finished());
		copy_dir(&db, &image);

		gate.open();
		checkpoint.wait().unwrap();
		within("close()", close).await.unwrap().unwrap();
		// The image is a database directory whose lock file says it is in use: a crash.
		let recovered = open(&image, vlog);
		assert_contents(&recovered, &expected, "the crash image");
		within("closing the image", recovered.close()).await.unwrap();
		// The checkpoint is the state at its start; the later commits may be in it or not.
		let copy = open(&cp, vlog);
		let found = contents(&copy);
		for (key, value) in &before_checkpoint {
			assert_eq!(found.get(key), Some(value), "{}", String::from_utf8_lossy(key));
		}
		for (key, value) in &found {
			assert_eq!(expected.get(key), Some(value), "{}", String::from_utf8_lossy(key));
		}
		within("closing the copy", copy.close()).await.unwrap();
	}
}

/// Recovery continues in a fresh WAL segment and tags the recovered memtable with it, although
/// the memtable holds what the crashed segment held. `close()` waits for a checkpoint of such a
/// tree that is parked in its flush, the commits made meanwhile are acknowledged, and a crash
/// image taken during the wait, the copy and the tree after the close hold what they must.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_waits_for_a_checkpoint_of_a_recovered_tree() {
	for vlog in [false, true] {
		let root = TempDir::new("ckpt_close_recovered").unwrap();
		let (first, db, cp) =
			(root.path().join("first"), root.path().join("db"), root.path().join("cp"));
		let image = root.path().join("image");

		// The first run is never closed before the copy: what it committed is in its WAL only.
		let run = open(&first, vlog);
		let mut expected = put(&run, 0..150).await;
		let crashed = run.core.inner.wal.read().get_active_log_number();
		copy_dir(&first, &db);
		within("closing the first run", run.close()).await.unwrap();
		drop(run);

		let opts = options(&db, vlog);
		let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
		assert_eq!(tree.core.inner.wal.read().get_active_log_number(), crashed + 1);
		assert_contents(&tree, &expected, "the recovered tree");
		expected.extend(put(&tree, 150..200).await);
		let before_checkpoint = expected.clone();
		let gate = hold_checkpoints(&tree, Park::InFlush);

		let checkpoint = checkpoint_on_thread(&tree, &cp);
		until("the checkpoint to be parked in its flush", || gate.reached() == 1).await;
		let close = spawn_close(&tree);
		for i in 200..230 {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(Durability::Immediate);
			txn.set(key_of(i), val_of(i)).unwrap();
			txn.commit().await.unwrap();
			expected.insert(key_of(i), val_of(i));
		}
		tokio::time::sleep(GRACE).await;
		assert!(!close.is_finished(), "close() returned while the checkpoint was parked");
		copy_dir(&db, &image);

		gate.open();
		checkpoint.wait().unwrap();
		within("close()", close).await.unwrap().unwrap();

		let recovered = open(&image, vlog);
		assert_contents(&recovered, &expected, "the crash image taken during the wait");
		within("closing the image", recovered.close()).await.unwrap();
		let copy = open(&cp, vlog);
		let found = contents(&copy);
		for (key, value) in &before_checkpoint {
			assert_eq!(found.get(key), Some(value), "{}", String::from_utf8_lossy(key));
		}
		for (key, value) in &found {
			assert_eq!(expected.get(key), Some(value), "{}", String::from_utf8_lossy(key));
		}
		within("closing the copy", copy.close()).await.unwrap();
		let reopened = reopen(tree, &opts);
		assert_contents(&reopened, &expected, "the tree after the close");
		reopened.close().await.unwrap();
	}
}

/// Closers queued behind one that is dropped while it waits still finish, one after another.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn closers_queued_behind_a_dropped_one_still_finish() {
	let root = TempDir::new("ckpt_close_queue").unwrap();
	let (db, cp) = (root.path().join("db"), root.path().join("cp"));
	let opts = options(&db, true);
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let expected = put(&tree, 0..300).await;
	let gate = hold_checkpoints(&tree, Park::InFlush);

	let checkpoint = checkpoint_on_thread(&tree, &cp);
	until("the checkpoint to be held", || gate.reached() == 1).await;
	let first = spawn_close(&tree);
	let second = spawn_close(&tree);
	let third = spawn_close(&tree);
	tokio::time::sleep(GRACE).await;
	first.abort();
	assert!(first.await.unwrap_err().is_cancelled());
	tokio::time::sleep(GRACE).await;
	assert!(!second.is_finished() && !third.is_finished());
	gate.open();

	checkpoint.wait().unwrap();
	within("the second close()", second).await.unwrap().unwrap();
	within("the third close()", third).await.unwrap().unwrap();
	assert_copy(&cp, true, &expected, "checkpoint finished while closers queued").await;
	let reopened = reopen(tree, &opts);
	assert_contents(&reopened, &expected, "source after the reopen");
	reopened.close().await.unwrap();
}

/// Dropping a clone of the tree spawns a close that waits for a checkpoint another clone is
/// running; neither panics, and everything is intact after.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn dropping_a_tree_handle_while_a_checkpoint_runs_waits_for_it() {
	let root = TempDir::new("ckpt_close_dropclone").unwrap();
	let (db, cp) = (root.path().join("db"), root.path().join("cp"));
	let opts = options(&db, true);
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	let expected = put(&tree, 0..300).await;
	let core = Arc::clone(&tree.core);
	let gate = hold_checkpoints(&tree, Park::At(CheckpointStage::SstablesCopied));

	let for_thread = tree.clone();
	let cp_for_thread = cp.clone();
	let checkpoint = on_thread(move || for_thread.create_checkpoint(&cp_for_thread));
	until("the checkpoint to be held", || gate.reached() == 1).await;

	// The drop spawns a close on the runtime, which waits for the checkpoint.
	drop(tree);
	tokio::time::sleep(GRACE).await;
	assert!(!core.is_closed.load(Ordering::SeqCst), "the tree closed under a running checkpoint");
	gate.open();

	checkpoint.wait().unwrap();
	until("the spawned close to finish", || core.is_closed.load(Ordering::SeqCst)).await;
	// The close holds the directory lock until its last step.
	let deadline = Instant::now() + LONG;
	let reopened = loop {
		match Tree::new(Arc::clone(&opts)) {
			Ok(tree) => break tree,
			Err(error) => {
				assert!(Instant::now() < deadline, "the directory stayed locked: {error:?}");
				tokio::time::sleep(Duration::from_millis(20)).await;
			}
		}
	};
	assert_contents(&reopened, &expected, "source after the spawned close");
	assert_copy(&cp, true, &expected, "checkpoint taken while the handle was dropped").await;
	reopened.close().await.unwrap();
}

/// A close whose own flush fails still ends the gate's wait and refuses what follows: a
/// checkpoint after it is the closed error, not a panic or a hang.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_after_a_close_that_failed_is_refused() {
	let root = TempDir::new("ckpt_close_failedclose").unwrap();
	let (db, cp) = (root.path().join("db"), root.path().join("cp"));
	let opts = Arc::new(Options {
		path: db.clone(),
		flush_on_close: true,
		..Default::default()
	});
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	put(&tree, 0..100).await;
	*tree.core.inner.flush_hook.lock() =
		Some(Arc::new(|_| Err(Error::Other("injected flush failure".into()))));
	let closed = tree.close().await;
	assert!(closed.is_err(), "the close was meant to fail: {closed:?}");
	let error = checkpoint_on_thread(&tree, &cp).wait().expect_err("a checkpoint after the close");
	assert!(is_closed_refusal(&error), "{error:?}");
	assert!(!cp.exists());
	assert!(tree.close().await.is_ok());
}

/// On a runtime with one thread a checkpoint is called from the runtime thread itself, between
/// awaits, while writers commit and `close()` is a task that has not run yet: the checkpoint
/// needs no other task to finish, so nothing deadlocks, and every one is a copy that opens or the
/// closed refusal.
#[test(tokio::test(flavor = "current_thread"))]
async fn checkpoints_called_on_a_single_thread_runtime_while_close_is_pending_do_not_deadlock() {
	for vlog in [false, true] {
		let root = TempDir::new("ckpt_close_current").unwrap();
		let db = root.path().join("db");
		let opts = options(&db, vlog);
		let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
		let acked = Arc::new(Mutex::new(Contents::new()));
		let mut writers = Vec::new();
		for w in 0..2usize {
			let (tree, acked) = (Arc::clone(&tree), Arc::clone(&acked));
			writers.push(tokio::spawn(async move {
				for i in 0.. {
					let (key, value) = (key_of(w * 100_000 + i), val_of(i));
					let mut txn = tree.begin().unwrap();
					txn.set_durability(if w == 0 {
						Durability::Immediate
					} else {
						Durability::Eventual
					});
					txn.set(key.clone(), value.clone()).unwrap();
					if txn.commit().await.is_err() {
						break;
					}
					acked.lock().unwrap().insert(key, value);
				}
			}));
		}
		let close = spawn_close(&tree);
		let mut outcomes = Vec::new();
		for n in 0..12 {
			let floor = acked.lock().unwrap().clone();
			let cp = root.path().join(format!("cp{n}"));
			outcomes.push((floor, cp.clone(), tree.create_checkpoint(&cp)));
			tokio::task::yield_now().await;
			if n == 4 {
				// Lets the close task run: from here on it is waiting, or done.
				tokio::time::sleep(Duration::from_millis(20)).await;
			}
		}
		within("close()", close).await.unwrap().unwrap();
		for writer in writers {
			within("a writer", writer).await.unwrap();
		}
		let mut copies = 0;
		for (floor, cp, outcome) in outcomes {
			match outcome {
				Ok(_) => {
					copies += 1;
					let copy = open(&cp, vlog);
					let found = contents(&copy);
					for (key, value) in &floor {
						assert_eq!(found.get(key), Some(value), "{}", cp.display());
					}
					within("closing the copy", copy.close()).await.unwrap();
				}
				Err(error) => {
					assert!(is_closed_refusal(&error), "{error:?}");
					assert!(!cp.exists());
				}
			}
		}
		assert!(copies >= 1, "no checkpoint was taken before close() ran");
		let reopened = reopen(tree, &opts);
		let expected = acked.lock().unwrap().clone();
		assert_contents(&reopened, &expected, "source after the reopen");
		reopened.close().await.unwrap();
	}
}

// ---------------------------------------------------------------------------
// the two at random
// ---------------------------------------------------------------------------

/// splitmix64: a small deterministic generator, so a round repeats.
struct Rng(u64);

impl Rng {
	fn next(&mut self) -> u64 {
		self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
		let mut z = self.0;
		z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
		z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
		z ^ (z >> 31)
	}

	fn below(&mut self, n: u64) -> u64 {
		self.next() % n
	}
}

/// How many rounds the stress test runs: `CHECKPOINT_CLOSE_ROUNDS`, or `default`.
fn rounds(default: u64) -> u64 {
	std::env::var("CHECKPOINT_CLOSE_ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// The value of `key`, a function of the key alone, between 40 and 1200 bytes so that some are
/// separated into the value log when it is on.
fn value_for(key: &str) -> Vec<u8> {
	let n = 1 + (xxhash_rust::xxh3::xxh3_64(key.as_bytes()) % 60) as usize;
	format!("value of {key};").repeat(n).into_bytes()
}

/// Pauses at random, to widen the windows between the steps of a checkpoint.
fn jitter(counter: &AtomicUsize, salt: u64) {
	let n = counter.fetch_add(1, Ordering::Relaxed) as u64;
	let mut rng = Rng(n.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ salt);
	match rng.below(8) {
		0..=2 => std::thread::sleep(Duration::from_micros(rng.below(2500))),
		3..=4 => std::thread::yield_now(),
		_ => {}
	}
}

/// Parks the caller until `flag` is set, or for a moment: whoever sets it does not wait for the
/// caller, so a caller that is never released only loses the pause.
fn pause_until(flag: &AtomicBool) {
	let deadline = Instant::now() + Duration::from_millis(200);
	while !flag.load(Ordering::SeqCst) && Instant::now() < deadline {
		std::thread::sleep(Duration::from_micros(100));
	}
}

/// A checkpoint from a thread without a runtime races `close()` while three writers commit, in
/// either order and with random pauses inside the checkpoint. Each round ends in one of two ways:
/// the checkpoint is a copy that opens, holds every commit acknowledged before it began and
/// nothing wrong, or it is refused as closed and created nothing. Never a panic, a hang, another
/// error or a half-written destination, and every acknowledged commit is there after a reopen of
/// the source.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 6))]
async fn a_checkpoint_racing_close_is_a_valid_copy_or_a_clean_refusal() {
	let (mut copies, mut refusals) = (0, 0);
	let (mut forced, pauses) = (0, Arc::new(AtomicUsize::new(0)));
	for round in 0..rounds(12) {
		let root = TempDir::new("ckpt_close_race").unwrap();
		let (db, cp) = (root.path().join("db"), root.path().join("cp"));
		let vlog = round % 2 == 1;
		let opts = Arc::new(Options {
			path: db.clone(),
			max_memtable_size: 128 << 10,
			enable_vlog: vlog,
			vlog_value_threshold: 256,
			flush_on_close: round % 3 == 2,
			..Default::default()
		});
		let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());

		let acked = Arc::new(Mutex::new(Vec::<String>::new()));
		for i in 0..120 {
			let key = format!("seed{i:04}");
			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value_for(&key).as_slice()).unwrap();
			txn.commit().await.unwrap();
			acked.lock().unwrap().push(key);
		}

		// When the checkpoint goes first, it pauses at one of its steps (its flush, or after
		// the tables, or after the manifest) until `close()` has been called, so that `close()`
		// begins while the checkpoint is running. Every step of a checkpoint looks at whether
		// the tree has begun to shut down: none may, as `close()` waits for the checkpoint. The
		// background flushes, and the flush `close()` makes itself, pass the flush hook too, so
		// the hook only looks at the flushes of the thread that checkpoints.
		let close_called = Arc::new(AtomicBool::new(false));
		let late_steps = Arc::new(AtomicUsize::new(0));
		let checkpoint_thread = Arc::new(Mutex::new(None));
		let pause_at = (round % 3 == 0).then_some((round / 3) % 3);
		forced += usize::from(pause_at.is_some());
		let counter = Arc::new(AtomicUsize::new(0));
		{
			let step = {
				let (called, late, paused) =
					(Arc::clone(&close_called), Arc::clone(&late_steps), Arc::clone(&pauses));
				let core = Arc::downgrade(&tree.core);
				move |index: u64| {
					let shutting_down =
						|| core.upgrade().is_some_and(|c| c.is_closed.load(Ordering::SeqCst));
					if shutting_down() {
						late.fetch_add(1, Ordering::SeqCst);
					}
					if pause_at == Some(index) {
						paused.fetch_add(1, Ordering::SeqCst);
						pause_until(&called);
						// Time for a `close()` that does not wait to show.
						std::thread::sleep(Duration::from_millis(1));
						if shutting_down() {
							late.fetch_add(1, Ordering::SeqCst);
						}
					}
				}
			};
			let (for_stages, at_stage) = (Arc::clone(&counter), step.clone());
			*tree.core.inner.checkpoint_hook.lock() = Some(Arc::new(move |stage| {
				jitter(&for_stages, round);
				at_stage(match stage {
					CheckpointStage::SstablesCopied => 1,
					CheckpointStage::ManifestReleased => 2,
				});
			}));
			let (for_flushes, on_checkpoint) =
				(Arc::clone(&counter), Arc::clone(&checkpoint_thread));
			*tree.core.inner.flush_hook.lock() = Some(Arc::new(move |_| {
				jitter(&for_flushes, !round);
				if *on_checkpoint.lock().unwrap() == Some(std::thread::current().id()) {
					step(0);
				}
				Ok(())
			}));
		}

		let mut writers = Vec::new();
		for w in 0..3usize {
			let (tree, acked) = (Arc::clone(&tree), Arc::clone(&acked));
			writers.push(tokio::spawn(async move {
				for i in 0.. {
					let key = format!("w{w}_{i:06}");
					let mut txn = tree.begin().unwrap();
					txn.set_durability(if w == 0 {
						Durability::Immediate
					} else {
						Durability::Eventual
					});
					txn.set(key.as_bytes(), value_for(&key).as_slice()).unwrap();
					match txn.commit().await {
						Ok(()) => acked.lock().unwrap().push(key),
						// The pipeline is down.
						Err(_) => break,
					}
				}
			}));
		}

		// One of the two starts at once, or both after a random delay.
		let mut rng = Rng(round + 1);
		let (checkpoint_delay, close_delay) = match round % 3 {
			0 => (0, rng.below(4)),
			1 => (rng.below(4), 0),
			_ => (rng.below(4), rng.below(4)),
		};
		let checkpoint = {
			let (tree, acked, cp) = (Arc::clone(&tree), Arc::clone(&acked), cp.clone());
			let on_checkpoint = Arc::clone(&checkpoint_thread);
			on_thread(move || {
				*on_checkpoint.lock().unwrap() = Some(std::thread::current().id());
				std::thread::sleep(Duration::from_millis(checkpoint_delay));
				let floor = acked.lock().unwrap().clone();
				(floor, tree.create_checkpoint(&cp))
			})
		};
		let close = {
			let (tree, called) = (Arc::clone(&tree), Arc::clone(&close_called));
			tokio::spawn(async move {
				tokio::time::sleep(Duration::from_millis(close_delay)).await;
				called.store(true, Ordering::SeqCst);
				tree.close().await
			})
		};

		within("close()", close).await.unwrap().unwrap();
		for writer in writers {
			within("a writer", writer).await.unwrap();
		}
		let (floor, outcome) = checkpoint.wait();
		match outcome {
			Ok(_) => {
				copies += 1;
				let copy = Tree::new(options(&cp, vlog)).unwrap();
				let found = contents(&copy);
				for key in &floor {
					assert_eq!(
						found.get(key.as_bytes()),
						Some(&value_for(key)),
						"round {round}: {key} was acknowledged before the checkpoint began"
					);
				}
				for (key, value) in &found {
					let key = String::from_utf8(key.clone()).unwrap();
					assert_eq!(value, &value_for(&key), "round {round}: {key} in the copy");
				}
				within("closing the copy", copy.close()).await.unwrap();
			}
			Err(error) => {
				refusals += 1;
				assert!(is_closed_refusal(&error), "round {round}: {error:?}");
				assert!(
					!cp.exists(),
					"round {round}: a refused checkpoint created its destination"
				);
			}
		}

		assert_eq!(
			late_steps.load(Ordering::SeqCst),
			0,
			"round {round}: a step of the checkpoint ran after close() had begun to shut the tree down"
		);
		let reopened = reopen(tree, &opts);
		let acked = acked.lock().unwrap().clone();
		let found = contents(&reopened);
		for key in &acked {
			assert_eq!(
				found.get(key.as_bytes()),
				Some(&value_for(key)),
				"round {round}: {key} was acknowledged and is not there after the reopen"
			);
		}
		reopened.close().await.unwrap();
	}
	// The forced orders did happen: a hook that is never reached would leave only random rounds.
	assert!(forced < 3 || pauses.load(Ordering::SeqCst) > 0, "no checkpoint was ever paused");
	eprintln!("checkpoint racing close: {copies} copies, {refusals} refusals");
}

// ---------------------------------------------------------------------------
// with the global affinity pool
// ---------------------------------------------------------------------------

/// With the process-wide affinity pool installed, as a server has it, the commit pipeline's
/// appends and fsyncs run on pool threads. A checkpoint from a thread without a runtime racing
/// `close()` and three writers still ends as a copy that opens and holds what was acknowledged
/// before it began, or as a clean refusal, and the source reopens with everything acknowledged.
/// The pool is process-wide, so the test runs itself again in a child process.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_racing_close_with_a_global_pool_is_a_valid_copy_or_a_clean_refusal() {
	const NAME: &str =
		"a_checkpoint_racing_close_with_a_global_pool_is_a_valid_copy_or_a_clean_refusal";
	if std::env::var_os("CHECKPOINT_CLOSE_POOL_CHILD").is_none() {
		let module = module_path!().split_once("::").unwrap().1;
		let output = std::process::Command::new(std::env::current_exe().unwrap())
			.args(["--exact", &format!("{module}::{NAME}"), "--nocapture", "--test-threads=1"])
			.env("CHECKPOINT_CLOSE_POOL_CHILD", "1")
			.output()
			.unwrap();
		assert!(
			output.status.success(),
			"the child failed:\n{}\n{}",
			String::from_utf8_lossy(&output.stdout),
			String::from_utf8_lossy(&output.stderr)
		);
		return;
	}

	affinitypool::Threadpool::new(2).build_global().expect("the pool is installed once");
	let (mut copies, mut refusals) = (0, 0);
	for round in 0..8u64 {
		let root = TempDir::new("ckpt_close_pool").unwrap();
		let (db, cp) = (root.path().join("db"), root.path().join("cp"));
		let vlog = round % 2 == 1;
		let opts = Arc::new(Options {
			path: db.clone(),
			max_memtable_size: 128 << 10,
			enable_vlog: vlog,
			vlog_value_threshold: 256,
			flush_on_close: round % 4 == 3,
			..Default::default()
		});
		let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
		let acked = Arc::new(Mutex::new(Vec::<String>::new()));
		for i in 0..60 {
			let key = format!("seed{i:04}");
			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value_for(&key).as_slice()).unwrap();
			txn.commit().await.unwrap();
			acked.lock().unwrap().push(key);
		}

		let mut writers = Vec::new();
		for w in 0..3usize {
			let (tree, acked) = (Arc::clone(&tree), Arc::clone(&acked));
			writers.push(tokio::spawn(async move {
				for i in 0.. {
					let key = format!("w{w}_{i:06}");
					let mut txn = tree.begin().unwrap();
					txn.set_durability(if w == 0 {
						Durability::Immediate
					} else {
						Durability::Eventual
					});
					txn.set(key.as_bytes(), value_for(&key).as_slice()).unwrap();
					match txn.commit().await {
						Ok(()) => acked.lock().unwrap().push(key),
						Err(_) => break,
					}
				}
			}));
		}
		let (checkpoint_delay, close_delay) = match round % 3 {
			0 => (0, 3 + round % 4),
			1 => (3 + round % 4, 0),
			_ => (2, 2),
		};
		let checkpoint = {
			let (tree, acked, cp) = (Arc::clone(&tree), Arc::clone(&acked), cp.clone());
			on_thread(move || {
				std::thread::sleep(Duration::from_millis(checkpoint_delay));
				let floor = acked.lock().unwrap().clone();
				(floor, tree.create_checkpoint(&cp))
			})
		};
		let close = {
			let tree = Arc::clone(&tree);
			tokio::spawn(async move {
				tokio::time::sleep(Duration::from_millis(close_delay)).await;
				tree.close().await
			})
		};
		within("close()", close).await.unwrap().unwrap();
		for writer in writers {
			within("a writer", writer).await.unwrap();
		}
		let (floor, outcome) = checkpoint.wait();
		match outcome {
			Ok(_) => {
				copies += 1;
				let copy = open(&cp, vlog);
				let found = contents(&copy);
				for key in &floor {
					assert_eq!(
						found.get(key.as_bytes()),
						Some(&value_for(key)),
						"round {round}: {key}"
					);
				}
				within("closing the copy", copy.close()).await.unwrap();
			}
			Err(error) => {
				refusals += 1;
				assert!(is_closed_refusal(&error), "round {round}: {error:?}");
				assert!(
					!cp.exists(),
					"round {round}: a refused checkpoint created its destination"
				);
			}
		}
		let reopened = reopen(tree, &opts);
		let found = contents(&reopened);
		for key in acked.lock().unwrap().iter() {
			assert_eq!(
				found.get(key.as_bytes()),
				Some(&value_for(key)),
				"round {round}: {key} was acknowledged and is not there after the reopen"
			);
		}
		reopened.close().await.unwrap();
	}
	eprintln!("global pool: {copies} copies, {refusals} refusals");
}
