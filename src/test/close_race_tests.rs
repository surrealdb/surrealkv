//! Shutting the commit pipeline down while commits are in flight.
//!
//! `close()` stops admitting commits and then has to decide every commit that was already
//! admitted: each one either completes with its real outcome, durable and acknowledged, or
//! fails with an error. None may wait forever, and `close()` must not return while one is
//! still undecided. The deterministic tests hold a commit at an exact step of its path
//! (`CommitStage`), or the flusher inside a group (`PipelineHook`), while `close()` runs. The
//! stress tests race writers against `close()` with the steps of every commit slowed at random.
//!
//! The tests hold threads inside the hooks, so every test that uses one runs on the
//! multi-threaded runtime. Every commit and every `close()` is awaited with a timeout, so a
//! regression fails a test instead of leaving the run hanging.

use std::collections::HashSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;
use tokio::task::JoinHandle;

use crate::lsm::Tree;
use crate::ring::{CommitStage, PipelineHook, ADMISSION_PERMITS};
use crate::{Durability, Error, Mode, Options, Result};

/// How long a test waits for something that has to happen before it calls it a hang.
const LONG: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn options(path: &Path) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		// A clean close must leave the WAL in place, so the reopen replays it.
		flush_on_close: false,
		..Default::default()
	})
}

/// Parks every thread that calls `wait` until `open`.
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
		let mut open = self.open.lock().unwrap();
		while !*open {
			open = self.opened.wait(open).unwrap();
		}
	}

	fn open(&self) {
		*self.open.lock().unwrap() = true;
		self.opened.notify_all();
	}

	fn reached(&self) -> usize {
		self.reached.load(Ordering::SeqCst)
	}
}

/// A gate the test holds. Dropping it opens the gate, so a test that fails does not leave a
/// worker thread parked inside a hook, which would keep the runtime from shutting down.
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

/// Holds every commit that reaches `stage`.
fn hold_commits_at(tree: &Tree, stage: CommitStage) -> Held {
	let gate = Arc::new(Gate::default());
	let hook = Arc::clone(&gate);
	tree.core.commit_pipeline.set_commit_hook(Some(Arc::new(move |at| {
		if at == stage {
			hook.wait();
		}
	})));
	Held(gate)
}

/// Holds the flusher after it logged its first group, before anything is applied. Commits
/// that arrive meanwhile queue up behind it.
fn hold_flusher(tree: &Tree) -> Held {
	let gate = Arc::new(Gate::default());
	let hook = Arc::clone(&gate);
	let first = AtomicBool::new(true);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if matches!(point, PipelineHook::AfterWalSync { .. }) && first.swap(false, Ordering::SeqCst)
		{
			hook.wait();
		}
	})));
	Held(gate)
}

/// Counts the commits that reach `stage`.
fn count_commits_at(tree: &Tree, stage: CommitStage) -> Arc<AtomicUsize> {
	let count = Arc::new(AtomicUsize::new(0));
	let sink = Arc::clone(&count);
	tree.core.commit_pipeline.set_commit_hook(Some(Arc::new(move |at| {
		if at == stage {
			sink.fetch_add(1, Ordering::SeqCst);
		}
	})));
	count
}

/// Panics at the first commit that reaches `stage`.
fn fail_first_commit_at(tree: &Tree, stage: CommitStage) {
	let first = AtomicBool::new(true);
	tree.core.commit_pipeline.set_commit_hook(Some(Arc::new(move |at| {
		if at == stage && first.swap(false, Ordering::SeqCst) {
			panic!("the committer fails at {at:?}");
		}
	})));
}

async fn until(what: &str, mut condition: impl FnMut() -> bool) {
	let deadline = Instant::now() + LONG;
	while !condition() {
		assert!(Instant::now() < deadline, "timed out waiting for {what}");
		tokio::time::sleep(Duration::from_millis(2)).await;
	}
}

/// Waits until the flusher has seen the shutdown, found nothing to drain and begun to wait for
/// the commits that were admitted before it, or until `close` is over.
async fn until_the_flusher_waits(tree: &Tree, close: &JoinHandle<Result<()>>) {
	until("the flusher to wait for the admitted commits", || {
		tree.core.commit_pipeline.shutdown_waits() > 0 || close.is_finished()
	})
	.await;
}

async fn until_the_shutdown_begins(tree: &Tree) {
	until("the shutdown to begin", || tree.core.commit_pipeline.shutdown.load(Ordering::SeqCst))
		.await;
}

/// Awaits `future`, failing the test if it takes longer than `LONG`.
async fn within<T>(what: &str, future: impl Future<Output = T>) -> T {
	match tokio::time::timeout(LONG, future).await {
		Ok(value) => value,
		Err(_) => panic!("{what} did not finish within {LONG:?}"),
	}
}

async fn resolves<T>(what: &str, handle: JoinHandle<T>) -> T {
	within(what, handle).await.unwrap()
}

fn spawn_commit(tree: &Arc<Tree>, key: String, durability: Durability) -> JoinHandle<Result<()>> {
	let tree = Arc::clone(tree);
	tokio::spawn(async move {
		let mut txn = tree.begin().unwrap();
		txn.set_durability(durability);
		txn.set(key.as_bytes(), key.as_bytes()).unwrap();
		txn.commit().await
	})
}

/// A commit that writes nothing and only validates a locked read of `key`, which must exist.
fn spawn_locked_read(tree: &Arc<Tree>, key: &'static str) -> JoinHandle<Result<()>> {
	let tree = Arc::clone(tree);
	tokio::spawn(async move {
		let mut txn = tree.begin().unwrap();
		assert!(txn.get_for_update(key.as_bytes()).unwrap().is_some());
		txn.commit().await
	})
}

fn spawn_close(tree: &Arc<Tree>) -> JoinHandle<Result<()>> {
	let tree = Arc::clone(tree);
	tokio::spawn(async move { tree.close().await })
}

async fn commit_key(tree: &Tree, key: &str) {
	let mut txn = tree.begin().unwrap();
	txn.set(key.as_bytes(), key.as_bytes()).unwrap();
	within("a commit", txn.commit()).await.unwrap();
}

fn read(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	txn.get(key.as_bytes()).unwrap()
}

/// Reopens the closed tree from its directory.
fn reopen(tree: Arc<Tree>, opts: &Arc<Options>) -> Tree {
	// The tree is closed: dropping the last handle runs no second close.
	assert!(Arc::strong_count(&tree) == 1, "a task still holds the tree");
	drop(tree);
	Tree::new(Arc::clone(opts)).unwrap()
}

fn assert_present(tree: &Tree, keys: impl IntoIterator<Item = String>) {
	for key in keys {
		assert_eq!(read(tree, &key).as_deref(), Some(key.as_bytes()), "{key} after the reopen");
	}
}

fn copy_dir(src: &Path, dst: &Path) {
	std::fs::create_dir_all(dst).unwrap();
	for entry in std::fs::read_dir(src).unwrap() {
		let entry = entry.unwrap();
		let to = dst.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &to);
		} else {
			std::fs::copy(entry.path(), to).unwrap();
		}
	}
}

// ---------------------------------------------------------------------------
// a commit that races close()
// ---------------------------------------------------------------------------

/// A commit that is held at `stage` while `close()` begins, and released once the flusher has
/// seen the shutdown, completes: it was admitted before the close, so it gets its real outcome.
async fn admitted_commit_completes_when_close_begins(stage: CommitStage, flush_on_close: bool) {
	let dir = TempDir::new("close_race").unwrap();
	let opts = Arc::new(Options {
		path: dir.path().to_path_buf(),
		flush_on_close,
		..Default::default()
	});
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let gate = hold_commits_at(&tree, stage);

	let commit = spawn_commit(&tree, "key".into(), Durability::Immediate);
	until("the commit to reach the stage", || gate.reached() == 1).await;
	let close = spawn_close(&tree);
	until_the_flusher_waits(&tree, &close).await;
	gate.open();

	let outcome = resolves("the commit that raced close()", commit).await;
	assert!(outcome.is_ok(), "an admitted commit gets its real outcome: {outcome:?}");
	resolves("close()", close).await.unwrap();

	let reopened = reopen(tree, &opts);
	assert_present(&reopened, ["key".to_string()]);
}

/// The commit has an admission permit but has not claimed a ring sequence when the flusher
/// first sees the shutdown.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_commit_admitted_before_close_completes() {
	admitted_commit_completes_when_close_begins(CommitStage::Admitted, false).await;
}

/// The commit sits in the ring, claimed and validated, but not yet accepted, when the flusher
/// first sees the shutdown.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_commit_validated_before_close_completes() {
	admitted_commit_completes_when_close_begins(CommitStage::Validated, false).await;
}

/// With `flush_on_close`, `close()` flushes the memtable once the flusher is gone, so a commit
/// admitted before the close has to be in the memtable by then.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_commit_admitted_before_a_flushing_close_is_flushed() {
	admitted_commit_completes_when_close_begins(CommitStage::Admitted, true).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_commit_validated_before_a_flushing_close_is_flushed() {
	admitted_commit_completes_when_close_begins(CommitStage::Validated, true).await;
}

/// `close()` does not return while a commit that was admitted is undecided.
async fn close_waits_for_the_admitted_commit(stage: CommitStage) {
	let dir = TempDir::new("close_race").unwrap();
	let tree = Arc::new(Tree::new(options(dir.path())).unwrap());
	let gate = hold_commits_at(&tree, stage);

	let commit = spawn_commit(&tree, "key".into(), Durability::Immediate);
	until("the commit to reach the stage", || gate.reached() == 1).await;
	let close = spawn_close(&tree);
	until_the_flusher_waits(&tree, &close).await;
	// Time for a wrong exit to show.
	tokio::time::sleep(Duration::from_millis(50)).await;
	assert!(!close.is_finished(), "close() returned while an admitted commit was undecided");

	gate.open();
	resolves("the commit", commit).await.unwrap();
	resolves("close()", close).await.unwrap();
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_waits_for_a_commit_holding_a_permit() {
	close_waits_for_the_admitted_commit(CommitStage::Admitted).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_waits_for_a_validated_commit() {
	close_waits_for_the_admitted_commit(CommitStage::Validated).await;
}

/// A commit that is validated but not yet accepted still holds its ring entry undecided. If the
/// entry were decided at that point (aborted by a guard released too early, say), the flusher
/// could drain it, and the commit that is accepted afterwards would be waited for by nobody.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_validated_commit_leaves_its_entry_undecided_until_it_is_accepted() {
	let dir = TempDir::new("close_race").unwrap();
	let tree = Arc::new(Tree::new(options(dir.path())).unwrap());
	let gate = hold_commits_at(&tree, CommitStage::Validated);

	let commit = spawn_commit(&tree, "key".into(), Durability::Immediate);
	until("the commit to be validated", || gate.reached() == 1).await;
	// The first entry of the ring is the commit's. The completed prefix stops short of it for
	// as long as it is undecided.
	let (completed, _, _) = tree.core.commit_pipeline.watermarks();
	assert_eq!(completed, 0, "the entry was decided before the commit was accepted");
	gate.open();

	resolves("the commit", commit).await.unwrap();
	resolves("close()", spawn_close(&tree)).await.unwrap();
}

/// A commit that holds an admission permit when `close()` begins, and then loses its conflict
/// check, leaves an aborted entry behind. The flusher is waiting for that permit, so the commit
/// must wake it, or `close()` waits forever.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_commit_that_conflicts_while_close_waits_lets_close_finish() {
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());

	// `loser` begins before `winner` commits, so `winner` lands in its conflict window.
	let mut loser = tree.begin().unwrap();
	loser.set(b"contended", b"loser").unwrap();
	let mut winner = tree.begin().unwrap();
	winner.set(b"contended", b"winner").unwrap();
	within("the winner", winner.commit()).await.unwrap();

	let gate = hold_commits_at(&tree, CommitStage::Admitted);
	let conflicting = tokio::spawn(async move { loser.commit().await });
	until("the commit to be admitted", || gate.reached() == 1).await;
	let close = spawn_close(&tree);
	until_the_flusher_waits(&tree, &close).await;
	gate.open();

	let outcome = resolves("the conflicting commit", conflicting).await;
	assert!(matches!(outcome, Err(Error::TransactionWriteConflict)), "the loser: {outcome:?}");
	resolves("close()", close).await.unwrap();

	let reopened = reopen(tree, &opts);
	assert_eq!(read(&reopened, "contended").as_deref(), Some(b"winner".as_slice()));
}

// ---------------------------------------------------------------------------
// a committer that dies at each step of its path
// ---------------------------------------------------------------------------

/// A commit that fails between claiming its ring sequence and its verdict (here a panic, the
/// only way out of a window that holds no await) must not leave its entry undecided: that
/// would stop the flusher at it, wedging every later commit and the shutdown.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_commit_that_panics_after_claiming_does_not_wedge_the_pipeline() {
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	fail_first_commit_at(&tree, CommitStage::Validated);

	let panicked = spawn_commit(&tree, "panicked".into(), Durability::Immediate);
	let joined = within("the panicking commit", panicked).await;
	assert!(joined.unwrap_err().is_panic());

	resolves(
		"the commit after the panic",
		spawn_commit(&tree, "next".into(), Durability::Immediate),
	)
	.await
	.unwrap();
	resolves("close()", spawn_close(&tree)).await.unwrap();

	let reopened = reopen(tree, &opts);
	assert_present(&reopened, ["next".to_string()]);
	assert!(read(&reopened, "panicked").is_none());
}

/// The flusher is spawned when the tree is opened, but it first runs when the runtime next polls
/// it. A commit that fails before then is aborted, and its guard advances the completed prefix over
/// its entry. The flusher must still take that entry's admission permit, or `close()` waits for
/// it forever. On a current-thread runtime the commit runs to its failure on the test's own task,
/// before the flusher has been polled.
#[test(tokio::test(flavor = "current_thread"))]
async fn a_commit_that_fails_before_the_flusher_first_runs_does_not_wedge_close() {
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	fail_first_commit_at(&tree, CommitStage::Validated);

	let mut txn = tree.begin().unwrap();
	txn.set(b"panicked", b"panicked").unwrap();
	let mut commit = Box::pin(txn.commit());
	let mut cx = Context::from_waker(Waker::noop());
	let polled =
		std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| commit.as_mut().poll(&mut cx)));
	assert!(polled.is_err(), "the commit must fail in its first poll, before the flusher runs");
	drop(commit);

	assert_eq!(tree.core.commit_pipeline.watermarks().0, 1, "the aborted entry is complete");
	resolves(
		"the commit after the panic",
		spawn_commit(&tree, "next".into(), Durability::Immediate),
	)
	.await
	.unwrap();
	resolves("close()", spawn_close(&tree)).await.unwrap();

	let reopened = reopen(tree, &opts);
	assert_present(&reopened, ["next".to_string()]);
	assert!(read(&reopened, "panicked").is_none());
}

/// A commit that holds an admission permit when `close()` begins and then dies before it claims
/// an entry gives its permit back to the semaphore and does nothing else. That alone has to be
/// enough for the flusher, which waits for every permit, to see that nothing is left.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_commit_that_dies_holding_a_permit_while_close_waits_lets_close_finish() {
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let gate = Arc::new(Gate::default());
	let held = Held(Arc::clone(&gate));
	tree.core.commit_pipeline.set_commit_hook(Some(Arc::new(move |at| {
		if at == CommitStage::Admitted {
			gate.wait();
			panic!("the committer dies holding its admission permit");
		}
	})));

	let commit = spawn_commit(&tree, "dies".into(), Durability::Immediate);
	until("the commit to be admitted", || held.reached() == 1).await;
	let close = spawn_close(&tree);
	until_the_flusher_waits(&tree, &close).await;
	tokio::time::sleep(Duration::from_millis(50)).await;
	assert!(!close.is_finished(), "close() returned while an admitted commit was undecided");
	held.open();

	let joined = within("the dying commit", commit).await;
	assert!(joined.unwrap_err().is_panic());
	resolves("close()", close).await.unwrap();

	let reopened = reopen(tree, &opts);
	assert!(read(&reopened, "dies").is_none());
}

/// A committer that dies before it asked for admission holds nothing.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_commit_that_dies_before_admission_leaves_close_free() {
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	fail_first_commit_at(&tree, CommitStage::Entered);

	let doomed = spawn_commit(&tree, "doomed".into(), Durability::Immediate);
	assert!(within("the dying commit", doomed).await.unwrap_err().is_panic());
	resolves("the next commit", spawn_commit(&tree, "next".into(), Durability::Immediate))
		.await
		.unwrap();
	resolves("close()", spawn_close(&tree)).await.unwrap();

	let reopened = reopen(tree, &opts);
	assert_present(&reopened, ["next".to_string()]);
	assert!(read(&reopened, "doomed").is_none());
}

/// A committer that dies after its entry was accepted: the flusher still applies the entry.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_commit_that_dies_after_accept_is_still_flushed() {
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	fail_first_commit_at(&tree, CommitStage::Accepted);

	let doomed = spawn_commit(&tree, "doomed".into(), Durability::Immediate);
	assert!(within("the dying commit", doomed).await.unwrap_err().is_panic());
	resolves("the next commit", spawn_commit(&tree, "next".into(), Durability::Immediate))
		.await
		.unwrap();
	resolves("close()", spawn_close(&tree)).await.unwrap();

	let reopened = reopen(tree, &opts);
	assert_present(&reopened, ["next".to_string(), "doomed".to_string()]);
}

/// A locked-read-only commit claims a ring entry and publishes nothing. Whether it passes or
/// loses its check, it must decide that entry: an undecided one stops the flusher at it.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn locked_read_commits_leave_no_entry_undecided() {
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let mut seed = tree.begin().unwrap();
	seed.set(b"locked", b"seed").unwrap();
	within("the seed", seed.commit()).await.unwrap();

	// A locked read nobody wrote to passes.
	let mut passes = tree.begin().unwrap();
	assert!(passes.get_for_update(b"locked").unwrap().is_some());
	within("the passing locked read", passes.commit()).await.unwrap();

	// A locked read of a key written after the transaction began fails.
	let mut fails = tree.begin().unwrap();
	assert!(fails.get_for_update(b"locked").unwrap().is_some());
	let mut writer = tree.begin().unwrap();
	writer.set(b"locked", b"changed").unwrap();
	within("the writer", writer.commit()).await.unwrap();
	let outcome = within("the failing locked read", fails.commit()).await;
	assert!(matches!(outcome, Err(Error::TransactionWriteConflict)), "{outcome:?}");

	// A write commit behind both still goes through, and so does the shutdown.
	let after = spawn_commit(&tree, "after".into(), Durability::Immediate);
	resolves("the commit behind the locked reads", after).await.unwrap();
	resolves("close()", spawn_close(&tree)).await.unwrap();

	let reopened = reopen(tree, &opts);
	assert_eq!(read(&reopened, "after").as_deref(), Some(b"after".as_slice()));
	assert_eq!(read(&reopened, "locked").as_deref(), Some(b"changed".as_slice()));
}

// ---------------------------------------------------------------------------
// commits that come after close()
// ---------------------------------------------------------------------------

/// A commit that passed the shutdown check but asks for admission only after `close()` has
/// finished is refused: nothing is left to flush it.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_commit_that_enters_before_close_but_is_admitted_after_it_fails() {
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let gate = hold_commits_at(&tree, CommitStage::Entered);

	let commit = spawn_commit(&tree, "late".into(), Durability::Immediate);
	until("the commit to enter", || gate.reached() == 1).await;
	resolves("close()", spawn_close(&tree)).await.unwrap();
	gate.open();

	let outcome = resolves("the late commit", commit).await;
	assert!(matches!(outcome, Err(Error::PipelineStall)), "the late commit: {outcome:?}");
	let reopened = reopen(tree, &opts);
	assert!(read(&reopened, "late").is_none());
}

/// While `close()` waits for the flusher, a new commit is refused at once instead of joining
/// the commits it waits for. That holds for a write and for a commit that only checks locked
/// reads.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_commit_while_close_waits_for_the_flusher_fails_fast() {
	let dir = TempDir::new("close_race").unwrap();
	let tree = Arc::new(Tree::new(options(dir.path())).unwrap());
	commit_key(&tree, "seed").await;
	let flusher = hold_flusher(&tree);

	let first = spawn_commit(&tree, "first".into(), Durability::Immediate);
	until("the flusher to take the first group", || flusher.reached() == 1).await;
	let close = spawn_close(&tree);
	until_the_shutdown_begins(&tree).await;

	let write = spawn_commit(&tree, "refused".into(), Durability::Immediate);
	let outcome = resolves("the write during the shutdown", write).await;
	assert!(
		matches!(outcome, Err(Error::PipelineStall)),
		"the write during the shutdown: {outcome:?}"
	);
	let locked = spawn_locked_read(&tree, "seed");
	let outcome = resolves("the locked read during the shutdown", locked).await;
	assert!(
		matches!(outcome, Err(Error::PipelineStall)),
		"the locked read during the shutdown: {outcome:?}"
	);
	assert!(!close.is_finished(), "the flusher is still held");

	flusher.open();
	resolves("the first commit", first).await.unwrap();
	resolves("close()", close).await.unwrap();
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_commit_after_close_fails_fast() {
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	commit_key(&tree, "before").await;

	// Transactions begun before and after the close, with writes, and with locked reads only.
	let mut open = tree.begin().unwrap();
	open.set(b"open", b"open").unwrap();
	let mut locked = tree.begin().unwrap();
	assert!(locked.get_for_update(b"before").unwrap().is_some());
	within("close()", tree.close()).await.unwrap();
	let mut after = tree.begin().unwrap();
	after.set(b"after", b"after").unwrap();

	for (what, mut txn) in [("open", open), ("locked read", locked), ("after", after)] {
		match tokio::time::timeout(Duration::from_secs(5), txn.commit()).await {
			Ok(Err(Error::PipelineStall)) => {}
			Ok(other) => panic!("the {what} commit after close() returned {other:?}"),
			Err(_) => panic!("the {what} commit after close() did not fail fast"),
		}
	}

	let reopened = reopen(tree, &opts);
	assert_present(&reopened, ["before".to_string()]);
	for key in ["open", "after"] {
		assert!(read(&reopened, key).is_none(), "{key} was refused, so it must not exist");
	}
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_is_idempotent() {
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	commit_key(&tree, "key").await;

	for _ in 0..3 {
		within("close()", tree.close()).await.unwrap();
	}

	let reopened = reopen(tree, &opts);
	assert_present(&reopened, ["key".to_string()]);
}

/// A `close()` that arrives while another is running does not return before it: it too must
/// not return while an admitted commit is undecided.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_second_concurrent_close_waits_for_the_first() {
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let gate = hold_commits_at(&tree, CommitStage::Validated);

	let commit = spawn_commit(&tree, "key".into(), Durability::Immediate);
	until("the commit to be held", || gate.reached() == 1).await;
	let first = spawn_close(&tree);
	until_the_flusher_waits(&tree, &first).await;
	assert!(!first.is_finished(), "the first close() waits for the held commit");

	let second = spawn_close(&tree);
	tokio::time::sleep(Duration::from_millis(100)).await;
	assert!(!second.is_finished(), "the second close() returned while a commit was undecided");
	gate.open();

	resolves("the commit", commit).await.unwrap();
	resolves("the first close()", first).await.unwrap();
	resolves("the second close()", second).await.unwrap();
	let reopened = reopen(tree, &opts);
	assert_present(&reopened, ["key".to_string()]);
}

// ---------------------------------------------------------------------------
// close() with commits queued
// ---------------------------------------------------------------------------

/// Commits queue up behind a flusher that is held inside its first group, and `close()` runs
/// while they wait: every one of them is flushed, acknowledged and on disk after the reopen.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_flushes_every_queued_commit() {
	const QUEUED: usize = 300;
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let flusher = hold_flusher(&tree);
	let accepted = count_commits_at(&tree, CommitStage::Accepted);

	let mut commits = Vec::new();
	commits.push(spawn_commit(&tree, "key-0".into(), Durability::Immediate));
	until("the flusher to take the first group", || flusher.reached() == 1).await;
	for i in 1..=QUEUED {
		let durability = if i % 2 == 0 {
			Durability::Immediate
		} else {
			Durability::Eventual
		};
		commits.push(spawn_commit(&tree, format!("key-{i}"), durability));
	}
	until("every commit to be accepted", || accepted.load(Ordering::SeqCst) > QUEUED).await;

	let close = spawn_close(&tree);
	until_the_shutdown_begins(&tree).await;
	assert!(!close.is_finished(), "close() returned with {QUEUED} commits queued");
	flusher.open();

	for (i, commit) in commits.into_iter().enumerate() {
		let outcome = resolves("a queued commit", commit).await;
		assert!(outcome.is_ok(), "queued commit {i}: {outcome:?}");
	}
	resolves("close()", close).await.unwrap();

	let reopened = reopen(tree, &opts);
	assert_present(&reopened, (0..=QUEUED).map(|i| format!("key-{i}")));
}

/// The flusher is held after it logged its first group while `close()` is pending; a rotation
/// then makes the group stale and the flusher logs it again. Every acknowledged commit is there
/// after the reopen, with and without the value log.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_while_a_rotation_makes_the_group_stale_keeps_every_commit() {
	for vlog in [false, true] {
		let dir = TempDir::new("close_race").unwrap();
		let opts = Arc::new(Options {
			path: dir.path().to_path_buf(),
			flush_on_close: false,
			enable_vlog: vlog,
			vlog_value_threshold: 64,
			..Default::default()
		});
		let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
		let gate = Arc::new(Gate::default());
		{
			let gate = Arc::clone(&gate);
			let inner = Arc::clone(&tree.core.inner);
			let first = AtomicBool::new(true);
			tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
				if matches!(point, PipelineHook::AfterWalSync { .. })
					&& first.swap(false, Ordering::SeqCst)
				{
					gate.wait();
					inner.rotate_memtable().unwrap();
				}
			})));
		}
		let held = Held(Arc::clone(&gate));
		let accepted = count_commits_at(&tree, CommitStage::Accepted);

		let mut commits = vec![spawn_commit(&tree, "key-0".into(), Durability::Immediate)];
		until("the flusher to take the first group", || held.reached() == 1).await;
		for i in 1..60 {
			let durability = if i % 2 == 0 {
				Durability::Immediate
			} else {
				Durability::Eventual
			};
			commits.push(spawn_commit(&tree, format!("key-{i}"), durability));
		}
		until("every commit to be accepted", || accepted.load(Ordering::SeqCst) >= 60).await;
		let close = spawn_close(&tree);
		until_the_shutdown_begins(&tree).await;
		held.open();

		for (i, commit) in commits.into_iter().enumerate() {
			let outcome = resolves("a commit", commit).await;
			assert!(outcome.is_ok(), "vlog={vlog}: commit {i}: {outcome:?}");
		}
		resolves("close()", close).await.unwrap();
		let reopened = reopen(tree, &opts);
		assert_present(&reopened, (0..60).map(|i| format!("key-{i}")));
	}
}

/// The group behind the held one fails to append and poisons the writer. Its commits fail,
/// their permits come back, the shutdown still finishes, and the held group is on disk.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_after_a_failed_group_append_finishes() {
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let flusher = hold_flusher(&tree);
	let accepted = count_commits_at(&tree, CommitStage::Accepted);

	let first = spawn_commit(&tree, "first".into(), Durability::Immediate);
	until("the flusher to take the first group", || flusher.reached() == 1).await;
	let mut doomed = Vec::new();
	for i in 0..50 {
		doomed.push(spawn_commit(&tree, format!("doomed-{i}"), Durability::Immediate));
	}
	until("the commits to be accepted", || accepted.load(Ordering::SeqCst) > 50).await;
	tree.core.inner.wal.write().fail_writes_after(0);
	let close = spawn_close(&tree);
	until_the_shutdown_begins(&tree).await;
	flusher.open();

	resolves("the first commit", first).await.unwrap();
	for (i, commit) in doomed.into_iter().enumerate() {
		let outcome = resolves("a doomed commit", commit).await;
		assert!(outcome.is_err(), "doomed commit {i} was acknowledged: {outcome:?}");
	}
	resolves("close()", close).await.unwrap();
	let reopened = reopen(tree, &opts);
	assert_present(&reopened, ["first".to_string()]);
	for i in 0..50 {
		assert!(read(&reopened, &format!("doomed-{i}")).is_none(), "doomed-{i} must not exist");
	}
}

/// A copy of the data directory, the keys acknowledged before it was taken, and whether the
/// shutdown had begun.
type Image = (PathBuf, HashSet<String>, bool);

/// A copy of the data directory is taken after every group the flusher logs while `close()`
/// drains. Whatever was acknowledged before the copy must be in the copy.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn images_taken_while_close_drains_hold_every_acknowledged_commit() {
	let dir = TempDir::new("close_race").unwrap();
	let images = TempDir::new("close_race_images").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());

	let acked: Arc<Mutex<HashSet<String>>> = Arc::default();
	let taken: Arc<Mutex<Vec<Image>>> = Arc::default();
	let gate = Arc::new(Gate::default());
	{
		let (gate, acked, taken) = (Arc::clone(&gate), Arc::clone(&acked), Arc::clone(&taken));
		let data = dir.path().to_path_buf();
		let images = images.path().to_path_buf();
		let pipeline = Arc::downgrade(&tree.core.commit_pipeline);
		let first = AtomicBool::new(true);
		tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
			if !matches!(point, PipelineHook::AfterWalSync { .. }) {
				return;
			}
			if first.swap(false, Ordering::SeqCst) {
				gate.wait();
			}
			let mut taken = taken.lock().unwrap();
			if taken.len() >= 8 {
				return;
			}
			let before = acked.lock().unwrap().clone();
			let shutdown = pipeline.upgrade().is_none_or(|p| p.shutdown.load(Ordering::SeqCst));
			let to = images.join(format!("image-{}", taken.len()));
			copy_dir(&data, &to);
			taken.push((to, before, shutdown));
		})));
	}
	let held = Held(Arc::clone(&gate));

	let stop = Arc::new(AtomicBool::new(false));
	let mut writers = Vec::new();
	for w in 0..8usize {
		let (tree, stop, acked) = (Arc::clone(&tree), Arc::clone(&stop), Arc::clone(&acked));
		writers.push(tokio::spawn(async move {
			let mut i = 0;
			while !stop.load(Ordering::SeqCst) {
				i += 1;
				let key = format!("w{w}-{i:05}");
				let mut txn = tree.begin().unwrap();
				txn.set_durability(Durability::Immediate);
				txn.set(key.as_bytes(), key.as_bytes()).unwrap();
				match txn.commit().await {
					Ok(()) => {
						acked.lock().unwrap().insert(key);
					}
					Err(_) => break,
				}
			}
		}));
	}
	until("the flusher to take a group", || held.reached() == 1).await;
	let close = spawn_close(&tree);
	until_the_shutdown_begins(&tree).await;
	tokio::time::sleep(Duration::from_millis(100)).await;
	held.open();
	resolves("close()", close).await.unwrap();
	stop.store(true, Ordering::SeqCst);
	for writer in writers {
		resolves("a writer", writer).await;
	}

	let taken = std::mem::take(&mut *taken.lock().unwrap());
	assert!(taken.iter().any(|(_, _, shutdown)| *shutdown), "no image was taken during the drain");
	for (n, (path, before, shutdown)) in taken.iter().enumerate() {
		let image = Tree::new(Arc::new(Options {
			path: path.clone(),
			flush_on_close: false,
			..Default::default()
		}))
		.unwrap();
		for key in before {
			assert_eq!(
				read(&image, key).as_deref(),
				Some(key.as_bytes()),
				"image {n} (taken after the shutdown began: {shutdown}) lost {key}"
			);
		}
	}
	let reopened = reopen(tree, &opts);
	let acked = acked.lock().unwrap().clone();
	assert_present(&reopened, acked);
}

// ---------------------------------------------------------------------------
// the runtime
// ---------------------------------------------------------------------------

/// No thread is blocked and no worker is spare: the flusher, the committers and `close()` all
/// run on one thread.
#[test(tokio::test(flavor = "current_thread"))]
async fn close_with_commits_in_flight_on_a_current_thread_runtime() {
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let mut commits = Vec::new();
	for i in 0..200 {
		let durability = if i % 3 == 0 {
			Durability::Immediate
		} else {
			Durability::Eventual
		};
		commits.push(spawn_commit(&tree, format!("key-{i}"), durability));
	}
	// A few turns of the scheduler, so some commits are claimed, some accepted, some queued.
	for _ in 0..3 {
		tokio::task::yield_now().await;
	}
	let close = spawn_close(&tree);
	let mut acked = Vec::new();
	for (i, commit) in commits.into_iter().enumerate() {
		match resolves("a commit", commit).await {
			Ok(()) => acked.push(format!("key-{i}")),
			Err(Error::PipelineStall) => {}
			Err(e) => panic!("commit {i}: {e:?}"),
		}
	}
	resolves("close()", close).await.unwrap();
	let reopened = reopen(tree, &opts);
	assert_present(&reopened, acked);
}

/// `join!` polls in order on one thread, so the interleaving is fixed: a commit polled before
/// `close()` is admitted and flushed, a commit polled after it is refused.
#[test(tokio::test(flavor = "current_thread"))]
async fn a_commit_polled_before_close_completes_and_one_polled_after_it_is_refused() {
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());

	let commit = |key: &'static str| {
		let tree = Arc::clone(&tree);
		async move {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(Durability::Immediate);
			txn.set(key.as_bytes(), key.as_bytes()).unwrap();
			txn.commit().await
		}
	};
	let close = || {
		let tree = Arc::clone(&tree);
		async move { tree.close().await }
	};
	let (before, closed, after) = within("the commits and close()", async {
		tokio::join!(commit("before"), close(), commit("after"))
	})
	.await;
	closed.unwrap();
	assert!(before.is_ok(), "polled before close(): {before:?}");
	assert!(matches!(after, Err(Error::PipelineStall)), "polled after close(): {after:?}");

	let reopened = reopen(tree, &opts);
	assert_present(&reopened, ["before".to_string()]);
	assert!(read(&reopened, "after").is_none());
}

// ---------------------------------------------------------------------------
// a commit future that is dropped, or that waits for admission
// ---------------------------------------------------------------------------

/// A commit future dropped while it waits for its verdict leaves its entry accepted, so the
/// flusher still applies it, and `close()` still finishes.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_commit_dropped_while_waiting_for_the_flusher_is_still_flushed() {
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let flusher = hold_flusher(&tree);

	let first = spawn_commit(&tree, "first".into(), Durability::Immediate);
	until("the flusher to take the first group", || flusher.reached() == 1).await;
	let mut txn = tree.begin().unwrap();
	txn.set(b"dropped", b"dropped").unwrap();
	// Accepted, waiting behind the held flusher, then dropped by the timeout.
	let timed_out = tokio::time::timeout(Duration::from_millis(200), txn.commit()).await;
	assert!(timed_out.is_err(), "the flusher is held, so the commit cannot have finished");

	let close = spawn_close(&tree);
	until_the_shutdown_begins(&tree).await;
	flusher.open();
	resolves("the first commit", first).await.unwrap();
	resolves("close()", close).await.unwrap();

	let reopened = reopen(tree, &opts);
	assert_present(&reopened, ["first".to_string(), "dropped".to_string()]);
}

/// A commit future dropped while it waits for an admission permit claims nothing and holds
/// nothing, and the commits still queued for a permit when `close()` begins, writes and locked
/// reads, each get an outcome.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_commit_dropped_while_waiting_for_admission_leaks_no_permit() {
	const QUEUED_EACH: usize = 5;
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	commit_key(&tree, "seed").await;
	let flusher = hold_flusher(&tree);
	let accepted = count_commits_at(&tree, CommitStage::Accepted);

	// One permit per commit: the held flusher keeps every one of them.
	let mut commits = vec![spawn_commit(&tree, "key-0".into(), Durability::Eventual)];
	until("the flusher to take the first group", || flusher.reached() == 1).await;
	for i in 1..ADMISSION_PERMITS {
		commits.push(spawn_commit(&tree, format!("key-{i}"), Durability::Eventual));
	}
	until("every permit to be taken", || accepted.load(Ordering::SeqCst) >= ADMISSION_PERMITS)
		.await;

	// No permit is left: this commit waits for one, and the timeout drops it there.
	let mut txn = tree.begin().unwrap();
	txn.set(b"dropped", b"dropped").unwrap();
	let timed_out = tokio::time::timeout(Duration::from_millis(200), txn.commit()).await;
	assert!(timed_out.is_err(), "no permit is free, so the commit cannot have finished");
	// And these are still waiting when the close begins.
	let mut waiting = Vec::new();
	for i in 0..QUEUED_EACH {
		waiting.push(spawn_commit(&tree, format!("waiting-{i}"), Durability::Eventual));
	}
	let waiting_reads: Vec<_> =
		(0..QUEUED_EACH).map(|_| spawn_locked_read(&tree, "seed")).collect();
	tokio::time::sleep(Duration::from_millis(100)).await;

	let close = spawn_close(&tree);
	until_the_shutdown_begins(&tree).await;
	flusher.open();

	for (i, commit) in commits.into_iter().enumerate() {
		let outcome = resolves("a queued commit", commit).await;
		assert!(outcome.is_ok(), "queued commit {i}: {outcome:?}");
	}
	let mut written = Vec::new();
	for (i, commit) in waiting.into_iter().enumerate() {
		let outcome = resolves("a commit waiting for a permit", commit).await;
		assert!(
			matches!(outcome, Ok(()) | Err(Error::PipelineStall)),
			"the commit waiting for a permit: {outcome:?}"
		);
		written.push((format!("waiting-{i}"), outcome.is_ok()));
	}
	for read in waiting_reads {
		let outcome = resolves("a locked read waiting for a permit", read).await;
		assert!(
			matches!(outcome, Ok(()) | Err(Error::PipelineStall)),
			"the locked read waiting for a permit: {outcome:?}"
		);
	}
	// A permit leaked by the dropped commit would keep close() waiting for it.
	resolves("close()", close).await.unwrap();

	let reopened = reopen(tree, &opts);
	assert_present(&reopened, (0..ADMISSION_PERMITS).map(|i| format!("key-{i}")));
	assert!(read(&reopened, "dropped").is_none(), "the dropped commit never started");
	for (key, acknowledged) in written {
		assert_eq!(read(&reopened, &key).is_some(), acknowledged, "{key}");
	}
}

/// A commit that queued for an admission permit before the shutdown is handed a permit as the
/// flusher drains, and `close()` waits for that permit to come back. A future that is polled
/// once, and then neither polled nor dropped, never gives it back.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_waits_for_a_queued_commit_future_that_nobody_polls() {
	let dir = TempDir::new("close_race").unwrap();
	let opts = options(dir.path());
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let flusher = hold_flusher(&tree);
	let accepted = count_commits_at(&tree, CommitStage::Accepted);

	let mut commits = vec![spawn_commit(&tree, "key-0".into(), Durability::Eventual)];
	until("the flusher to take the first group", || flusher.reached() == 1).await;
	for i in 1..ADMISSION_PERMITS {
		commits.push(spawn_commit(&tree, format!("key-{i}"), Durability::Eventual));
	}
	until("every permit to be taken", || accepted.load(Ordering::SeqCst) >= ADMISSION_PERMITS)
		.await;

	// Polled once: it queues for a permit, and its waker does nothing.
	let mut txn = tree.begin().unwrap();
	txn.set(b"stale", b"stale").unwrap();
	let mut stale: Pin<Box<dyn Future<Output = Result<()>> + Send>> =
		Box::pin(async move { txn.commit().await });
	let mut cx = Context::from_waker(Waker::noop());
	assert!(matches!(stale.as_mut().poll(&mut cx), Poll::Pending), "no permit is free");

	let close = spawn_close(&tree);
	until_the_shutdown_begins(&tree).await;
	flusher.open();
	for commit in commits {
		resolves("a queued commit", commit).await.unwrap();
	}
	tokio::time::sleep(Duration::from_millis(300)).await;
	assert!(!close.is_finished(), "close() returned while the queued commit held a permit");

	// Driving the future, or dropping it, lets the shutdown finish.
	drop(stale);
	resolves("close() after the future was dropped", close).await.unwrap();
}

// ---------------------------------------------------------------------------
// stress
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

/// How many rounds a stress test runs: `CLOSE_RACE_ROUNDS`, or `default`.
fn rounds(default: u64) -> u64 {
	std::env::var("CLOSE_RACE_ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

/// The value written for `key`, `len` bytes long: a function of both, so a record that is
/// replayed against the wrong key or cut short cannot look right.
fn value_for(key: &str, len: usize) -> Vec<u8> {
	let mut x = xxhash_rust::xxh3::xxh3_64(key.as_bytes()) | 1;
	(0..len)
		.map(|_| {
			x ^= x << 13;
			x ^= x >> 7;
			x ^= x << 17;
			x as u8
		})
		.collect()
}

/// One write transaction for iteration `i` of writer `w`: its entries as (key, value length).
fn plan(rng: &mut Rng, w: usize, i: usize) -> Vec<(String, usize)> {
	let key = |k: usize| format!("w{w:02}_i{i:06}_k{k:04}");
	match rng.below(100) {
		// One tiny key, the common single-batch commit.
		0..=64 => vec![(key(0), 16 + rng.below(200) as usize)],
		// A handful of keys.
		65..=84 => (0..8 + rng.below(25) as usize)
			.map(|k| (key(k), 50 + rng.below(450) as usize))
			.collect(),
		// One value of a few KiB up to 40 KiB (a value-log pointer when the log is on).
		85..=93 => vec![(key(0), 4096 + rng.below(36_000) as usize)],
		// A batch of about 150 KiB.
		94..=97 => (0..200).map(|k| (key(k), 700)).collect(),
		// One value of 3 MiB.
		_ => vec![(key(0), 3 << 20)],
	}
}

/// `close()` while sixteen writers are mid-commit (Immediate and Eventual, small and large
/// batches, with and without the value log). Every commit acknowledged before or during the
/// close is on disk after the reopen, `close()` returns, and every writer gets an outcome
/// instead of waiting forever once the pipeline is down. A round is sixteen writers and a
/// `close()` after 150 to 330 ms.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
#[ignore = "stress: run with --ignored, CLOSE_RACE_ROUNDS sets the number of rounds"]
async fn close_racing_commit_groups_keeps_every_acknowledged_commit() {
	for round in 0..rounds(8) {
		let dir = TempDir::new("close_race").unwrap();
		let opts = Arc::new(Options {
			path: dir.path().to_path_buf(),
			flush_on_close: false,
			max_memtable_size: 4 << 20,
			enable_vlog: round % 2 == 1,
			..Default::default()
		});
		let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
		let stop = Arc::new(AtomicBool::new(false));

		let mut writers = Vec::new();
		for w in 0..16usize {
			let (tree, stop) = (Arc::clone(&tree), Arc::clone(&stop));
			writers.push(tokio::spawn(async move {
				let mut rng = Rng(round * 31 + w as u64 + 5);
				// The keys the store acknowledged, with their value lengths.
				let mut acked: Vec<(String, usize)> = Vec::new();
				let mut i = 0;
				while !stop.load(Ordering::SeqCst) {
					i += 1;
					let entries = plan(&mut rng, w, i);
					let Ok(mut txn) = tree.begin() else {
						break;
					};
					txn.set_durability(if rng.below(2) == 0 {
						Durability::Immediate
					} else {
						Durability::Eventual
					});
					for (key, len) in &entries {
						txn.set(key.as_bytes(), value_for(key, *len)).unwrap();
					}
					match txn.commit().await {
						Ok(()) => acked.extend(entries),
						// Anything but success is "not acknowledged": the pipeline is down.
						Err(_) => break,
					}
				}
				acked
			}));
		}

		tokio::time::sleep(Duration::from_millis(150 + 60 * (round % 4))).await;
		within(&format!("round {round}: close()"), tree.close()).await.unwrap();
		stop.store(true, Ordering::SeqCst);
		let mut acked = Vec::new();
		for writer in writers {
			match tokio::time::timeout(LONG, writer).await {
				Ok(joined) => acked.extend(joined.unwrap()),
				Err(_) => panic!("round {round}: a writer is stuck in commit() after close()"),
			}
		}
		assert!(!acked.is_empty(), "round {round}: nothing was acknowledged before the close");

		let reopened = reopen(tree, &opts);
		let txn = reopened.begin_with_mode(Mode::ReadOnly).unwrap();
		let wrong: Vec<&str> = acked
			.iter()
			.filter(|(key, len)| {
				txn.get(key.as_bytes()).unwrap().as_deref() != Some(value_for(key, *len).as_slice())
			})
			.map(|(key, _)| key.as_str())
			.take(5)
			.collect();
		assert!(wrong.is_empty(), "round {round}: acknowledged commits lost, first few: {wrong:?}");
		drop(txn);
		within("closing the reopened tree", reopened.close()).await.unwrap();
	}
}

/// Makes every step of a commit but the last take a random time of up to `max_micros`, so that
/// the time between a commit's shutdown check and its acceptance is long enough for a `close()`
/// to land in it while the flusher is idle.
fn slow_commits(tree: &Tree, seed: u64, max_micros: u64) {
	let state = Arc::new(AtomicU64::new(seed | 1));
	tree.core.commit_pipeline.set_commit_hook(Some(Arc::new(move |stage| {
		if stage == CommitStage::Accepted {
			return;
		}
		// xorshift64 on a shared atomic: racy between threads, which only adds to the jitter.
		let mut x = state.load(Ordering::Relaxed);
		x ^= x << 13;
		x ^= x >> 7;
		x ^= x << 17;
		state.store(x, Ordering::Relaxed);
		let micros = x % max_micros;
		if micros > max_micros / 10 {
			std::thread::sleep(Duration::from_micros(micros));
		}
	})));
}

/// (writers, longest pause in a step in microseconds, whether some commits are Immediate). Few
/// writers and Eventual commits keep the flusher idle, which is when a commit in flight is lost
/// to an early exit; sixteen writers with Immediate commits keep it busy.
const SHAPES: [(usize, u64, bool); 3] = [(4, 2000, false), (2, 3000, false), (16, 400, true)];

/// `close()` while writers run small commits whose steps are slowed at random. Every writer
/// gets an outcome, `close()` returns, and every acknowledged commit is there after the reopen.
/// A commit lost to the shutdown shows as a writer that never returns. The window is narrow, so
/// this runs nine rounds by default (`CLOSE_RACE_ROUNDS` changes that), which catch a flusher
/// that exits while a commit is on its way to the ring in most runs.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn close_racing_slowed_commits_gives_every_writer_an_outcome() {
	for round in 0..rounds(9) {
		let (writer_count, max_micros, mixed) = SHAPES[round as usize % SHAPES.len()];
		let dir = TempDir::new("close_race").unwrap();
		let opts = options(dir.path());
		let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
		slow_commits(&tree, round + 1, max_micros);
		let stop = Arc::new(AtomicBool::new(false));
		let progress = Arc::new(AtomicUsize::new(0));

		let mut writers = Vec::new();
		for w in 0..writer_count {
			let (tree, stop, progress) =
				(Arc::clone(&tree), Arc::clone(&stop), Arc::clone(&progress));
			writers.push(tokio::spawn(async move {
				let mut acked = Vec::new();
				let mut i = 0;
				while !stop.load(Ordering::SeqCst) {
					i += 1;
					let key = format!("w{w:02}_i{i:06}");
					let Ok(mut txn) = tree.begin() else {
						break;
					};
					txn.set_durability(if mixed && (w + i) % 2 == 0 {
						Durability::Immediate
					} else {
						Durability::Eventual
					});
					txn.set(key.as_bytes(), key.as_bytes()).unwrap();
					match txn.commit().await {
						Ok(()) => {
							progress.fetch_add(1, Ordering::SeqCst);
							acked.push(key);
						}
						Err(_) => break,
					}
				}
				acked
			}));
		}

		until("a commit to be acknowledged", || progress.load(Ordering::SeqCst) > 0).await;
		tokio::time::sleep(Duration::from_millis(5 + 12 * (round % 6))).await;
		within(&format!("round {round}: close()"), tree.close()).await.unwrap();
		stop.store(true, Ordering::SeqCst);
		let mut acked = Vec::new();
		for writer in writers {
			match tokio::time::timeout(Duration::from_secs(20), writer).await {
				Ok(joined) => acked.extend(joined.unwrap()),
				Err(_) => panic!("round {round}: a writer never got an outcome for its commit"),
			}
		}
		assert!(!acked.is_empty(), "round {round}: nothing was acknowledged before the close");

		let reopened = reopen(tree, &opts);
		assert_present(&reopened, acked);
		within("closing the reopened tree", reopened.close()).await.unwrap();
	}
}

/// Sleeps or yields at random, to widen the windows between the steps of a commit and of the
/// flusher's group.
fn jitter(counter: &AtomicUsize, salt: u64) {
	let n = counter.fetch_add(1, Ordering::Relaxed) as u64;
	let mut rng = Rng(n.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ salt);
	match rng.below(16) {
		0..=2 => std::thread::sleep(Duration::from_micros(rng.below(3000))),
		3..=7 => std::thread::yield_now(),
		_ => {}
	}
}

/// Sixteen committers of four kinds (single-key Immediate, multi-key Eventual, read-modify-write
/// on a few hot keys so that conflicts abort entries, and locked-read-only commits) race
/// `close()`, with random pauses inside every step of a commit and of the flusher's group.
/// Every writer ends, `close()` returns, and every acknowledged write is there after the reopen.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
#[ignore = "stress: run with --ignored, CLOSE_RACE_ROUNDS sets the number of rounds"]
async fn close_racing_jittered_commits_of_every_kind_keeps_every_acknowledged_commit() {
	for round in 0..rounds(3) {
		let dir = TempDir::new("close_race").unwrap();
		let opts = Arc::new(Options {
			path: dir.path().to_path_buf(),
			flush_on_close: round % 3 == 2,
			max_memtable_size: 256 << 10,
			enable_vlog: round % 2 == 1,
			vlog_value_threshold: 256,
			..Default::default()
		});
		let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
		let counter = Arc::new(AtomicUsize::new(0));
		{
			let for_commits = Arc::clone(&counter);
			tree.core.commit_pipeline.set_commit_hook(Some(Arc::new(move |_| {
				jitter(&for_commits, round);
			})));
			let for_flusher = Arc::clone(&counter);
			tree.core.commit_pipeline.set_hook(Some(Arc::new(move |_| {
				jitter(&for_flusher, !round);
			})));
		}
		let stop = Arc::new(AtomicBool::new(false));
		let mut writers = Vec::new();
		for w in 0..16usize {
			let (tree, stop) = (Arc::clone(&tree), Arc::clone(&stop));
			writers.push(tokio::spawn(async move {
				let mut rng = Rng(round * 131 + w as u64);
				let mut acked: Vec<(String, Vec<u8>)> = Vec::new();
				let mut i = 0;
				while !stop.load(Ordering::SeqCst) {
					i += 1;
					let mut txn = tree.begin().unwrap();
					let mut writes: Vec<(String, Vec<u8>)> = Vec::new();
					match w % 4 {
						0 => {
							txn.set_durability(Durability::Immediate);
							let key = format!("a{w:02}-{i:06}");
							writes.push((
								key.clone(),
								key.into_bytes().repeat(1 + rng.below(40) as usize),
							));
						}
						1 => {
							txn.set_durability(Durability::Eventual);
							for k in 0..1 + rng.below(20) {
								let key = format!("b{w:02}-{i:06}-{k:02}");
								writes.push((
									key.clone(),
									key.into_bytes().repeat(1 + rng.below(30) as usize),
								));
							}
						}
						2 => {
							txn.set_durability(if rng.below(2) == 0 {
								Durability::Immediate
							} else {
								Durability::Eventual
							});
							let hot = format!("hot-{}", rng.below(3));
							let _ = txn.get_for_update(hot.as_bytes());
							let own = format!("c{w:02}-{i:06}");
							writes.push((own.clone(), own.into_bytes()));
							txn.set(hot.as_bytes(), format!("{w}-{i}").as_bytes()).unwrap();
						}
						_ => {
							let hot = format!("hot-{}", rng.below(3));
							let _ = txn.get_for_update(hot.as_bytes());
						}
					}
					for (k, v) in &writes {
						txn.set(k.as_bytes(), v.as_slice()).unwrap();
					}
					match txn.commit().await {
						Ok(()) => acked.extend(writes),
						Err(Error::TransactionWriteConflict) => {}
						// Anything else: the pipeline is down.
						Err(_) => break,
					}
				}
				acked
			}));
		}

		tokio::time::sleep(Duration::from_millis(100 + 70 * (round % 4))).await;
		within(&format!("round {round}: close()"), tree.close()).await.unwrap();
		stop.store(true, Ordering::SeqCst);
		let mut acked = Vec::new();
		for writer in writers {
			match tokio::time::timeout(LONG, writer).await {
				Ok(joined) => acked.extend(joined.unwrap()),
				Err(_) => panic!("round {round}: a writer is stuck in commit() after close()"),
			}
		}
		assert!(!acked.is_empty(), "round {round}: nothing was acknowledged");

		let reopened = reopen(tree, &opts);
		let txn = reopened.begin_with_mode(Mode::ReadOnly).unwrap();
		let wrong: Vec<&str> = acked
			.iter()
			.filter(|(k, v)| txn.get(k.as_bytes()).unwrap().as_deref() != Some(v.as_slice()))
			.map(|(k, _)| k.as_str())
			.take(5)
			.collect();
		assert!(wrong.is_empty(), "round {round}: acknowledged commits lost: {wrong:?}");
		drop(txn);
		within("closing the reopened tree", reopened.close()).await.unwrap();
	}
}
