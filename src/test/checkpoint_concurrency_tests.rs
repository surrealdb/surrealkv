//! `create_checkpoint` under concurrency, and what a checkpoint directory is worth afterwards.
//!
//! A checkpoint is consumed by `restore_from_checkpoint` into a tree, a fresh one in another
//! directory included. The checkpoint directory also opens as a database, but opening it adds a
//! lock file and a WAL segment and may rewrite files, so these tests restore wherever the
//! checkpoint has to stay as it was, and nearly everywhere else.
//!
//! The groups:
//!
//! 1. Point-in-time consistency under writers. Writers commit transactions that each write a group
//!    of keys, with `Durability::Immediate`, while checkpoints are taken into fresh directories.
//!    Every restored checkpoint holds whole transactions, a prefix of each writer's sequence, at
//!    least what was acknowledged before the call and at most what had started before it returned,
//!    and exactly the transactions up to its sequence number. The same checker serves a plain run,
//!    a memtable-rotation storm with value-log files, and two checkpoints taken at the same time
//!    into different directories.
//! 2. A checkpoint beside a flush and a compaction: nothing changes the manifest while the
//!    checkpoint holds it, everything proceeds once it lets go, and the files it copied are the
//!    files its manifest lists. On Linux a named pipe in the place of a file the checkpoint writes
//!    also holds it inside the copy of the manifest and of the value log, where no hook reaches.
//! 3. A checkpoint during value-log churn and background compaction.
//! 4. Two checkpoints into the same directory: nothing serializes them, so what is checked is that
//!    the directory they leave restores to a whole state, and a name that one of them links while
//!    the other has it free is not copied over the live table (Linux only, it races two threads).
//! 5. The directory of a checkpoint that was interrupted, or damaged afterwards: restore refuses
//!    it, and a refused restore leaves the target database alone.
//! 6. Hard links: the live tree deletes the tables a checkpoint links, and a tree restored from the
//!    checkpoint writes and compacts, without a byte of the checkpoint changing.
//! 7. A checkpoint into a directory that holds an older one is exactly the newer state, also when
//!    the two histories reuse table ids.
//!
//! Every wait is bounded, so a regression fails a test instead of hanging the run, and the
//! randomness is seeded; the seed is part of every assertion message.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier, Mutex};
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;

use super::collect_transaction_all;
use crate::checkpoint::{CheckpointMetadata, CheckpointStage};
use crate::compaction::compactor::CompactionStage;
use crate::compaction::leveled::Strategy;
use crate::compaction::CompactionStrategy;
use crate::error::Result;
use crate::levels::LevelManifest;
use crate::lsm::FlushHook;
use crate::{Durability, Error, LSMIterator, Mode, Options, Tree};

/// Large enough that nothing rotates unless a test rotates it.
const NEVER_ROTATING: usize = 16 * 1024 * 1024;

/// How long a test waits for something that is expected to happen.
const REACHED: Duration = Duration::from_secs(30);

/// How long a test gives something that must not happen while a checkpoint holds the manifest.
/// What it waits for takes microseconds when nothing is in the way.
const GRACE: Duration = Duration::from_millis(500);

/// How long a test waits for a call that is expected to return.
const STUCK: Duration = Duration::from_secs(60);

type Contents = BTreeMap<Vec<u8>, Vec<u8>>;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn key_of(i: usize) -> Vec<u8> {
	format!("key_{i:06}").into_bytes()
}

/// The value of key `i` in generation `generation`: long enough to go to the value log when it
/// is on, and different in every generation.
fn val_of(i: usize, generation: usize) -> Vec<u8> {
	let mut v = format!("val_{i:06}_g{generation}_").into_bytes();
	v.resize(120 + (i * 13 + generation * 7) % 100, b'x');
	v
}

/// The options of a database at `path`. `level0_max_files` is the number of level-0 tables that
/// asks the background task for a compaction.
fn options(
	path: &Path,
	max_memtable_size: usize,
	vlog: bool,
	level0_max_files: usize,
) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		max_memtable_size,
		enable_vlog: vlog,
		vlog_value_threshold: 64,
		// A few values to a file, so that a run makes many files and the obsolete ones can go.
		vlog_max_file_size: 4096,
		level0_max_files,
		// Tests that queue several immutable memtables by hand must not stall their own writes.
		memtable_stall_threshold: 1000,
		l0_stall_threshold: 1000,
		..Default::default()
	})
}

/// A tree that never compacts or rotates by itself.
fn open(path: &Path, vlog: bool) -> Arc<Tree> {
	Arc::new(Tree::new(options(path, NEVER_ROTATING, vlog, 1000)).unwrap())
}

/// Commits the keys of `range` in generation `generation`, 25 to a transaction, and records them
/// in `expected`.
async fn put(tree: &Tree, range: Range<usize>, generation: usize, expected: &mut Contents) {
	let keys: Vec<usize> = range.collect();
	for chunk in keys.chunks(25) {
		let mut txn = tree.begin().unwrap();
		for &i in chunk {
			txn.set(key_of(i), val_of(i, generation)).unwrap();
			expected.insert(key_of(i), val_of(i, generation));
		}
		txn.commit().await.unwrap();
	}
}

/// Deletes the keys of `range` and removes them from `expected`.
async fn remove(tree: &Tree, range: Range<usize>, expected: &mut Contents) {
	let mut txn = tree.begin().unwrap();
	for i in range {
		txn.delete(key_of(i)).unwrap();
		expected.remove(&key_of(i));
	}
	txn.commit().await.unwrap();
}

/// Everything the tree holds, in key order. A failing read of a value pointer is a failure.
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

/// Rotates the active memtable into the queue and flushes the queue.
fn flush_to_tables(tree: &Tree) {
	tree.core.inner.rotate_memtable().unwrap();
	tree.core.inner.flush_all_immutables_sync().unwrap();
}

/// The ids of the tables the manifest lists.
fn table_ids(tree: &Tree) -> Vec<u64> {
	let mut ids: Vec<u64> =
		tree.core.inner.level_manifest.read().unwrap().iter().map(|t| t.id).collect();
	ids.sort_unstable();
	ids
}

/// Every entry of every table the tree lists: the user key and the sequence number.
fn table_entries(tree: &Tree) -> Vec<(Vec<u8>, u64)> {
	let manifest = tree.core.inner.level_manifest.read().unwrap();
	let mut entries = Vec::new();
	for level in manifest.levels.get_levels() {
		for table in &level.tables {
			let mut iter = table.iter(None).unwrap();
			iter.seek_first().unwrap();
			while iter.valid() {
				let key = iter.key().to_owned();
				entries.push((key.user_key.clone(), key.seq_num()));
				iter.next().unwrap();
			}
		}
	}
	entries
}

fn sst_name(id: u64) -> String {
	format!("{id:020}.sst")
}

/// The names of the files in the `sub` directory of `path`, sorted; empty if there is none.
fn names_in(path: &Path, sub: &str) -> Vec<String> {
	let mut names: Vec<String> = match std::fs::read_dir(path.join(sub)) {
		Ok(entries) => {
			entries.map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect()
		}
		Err(_) => Vec::new(),
	};
	names.sort();
	names
}

/// The table ids the manifest file of the checkpoint at `path` lists, read without opening the
/// checkpoint.
fn manifest_tables_of(path: &Path) -> BTreeSet<u64> {
	let opts = options(path, NEVER_ROTATING, false, 1000);
	let file = opts.manifest_file_path(0);
	let manifest = LevelManifest::load_from_file(&file, Arc::clone(&opts))
		.unwrap_or_else(|e| panic!("the manifest of {} does not load: {e:?}", path.display()));
	manifest.iter().map(|t| t.id).collect()
}

/// A hash of every file under `root`, by relative name, and the directories, as the bytes of a
/// copy would be. A `LOCK` file at the top is left out.
fn digest(root: &Path) -> BTreeMap<String, u64> {
	fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, u64>) {
		for entry in std::fs::read_dir(dir).unwrap() {
			let entry = entry.unwrap();
			let path = entry.path();
			let rel = path.strip_prefix(root).unwrap().to_string_lossy().into_owned();
			if rel == "LOCK" {
				continue;
			}
			if entry.file_type().unwrap().is_dir() {
				out.insert(format!("{rel}/"), 0);
				walk(root, &path, out);
			} else {
				let bytes = std::fs::read(&path).unwrap();
				let mut hasher = std::collections::hash_map::DefaultHasher::new();
				bytes.hash(&mut hasher);
				out.insert(rel, hasher.finish());
			}
		}
	}
	let mut out = BTreeMap::new();
	walk(root, root, &mut out);
	out
}

/// What differs between two digests, for an assertion message.
fn differences(before: &BTreeMap<String, u64>, after: &BTreeMap<String, u64>) -> Vec<String> {
	let mut out = Vec::new();
	for (name, hash) in before {
		match after.get(name) {
			None => out.push(format!("{name}: removed")),
			Some(now) if now != hash => out.push(format!("{name}: changed")),
			Some(_) => {}
		}
	}
	for name in after.keys() {
		if !before.contains_key(name) {
			out.push(format!("{name}: added"));
		}
	}
	out
}

/// The size of the files under `path`, 0 if there is none.
fn dir_size(path: &Path) -> u64 {
	let Ok(entries) = std::fs::read_dir(path) else {
		return 0;
	};
	entries
		.map(|e| {
			let e = e.unwrap();
			if e.file_type().unwrap().is_dir() {
				dir_size(&e.path())
			} else {
				e.metadata().unwrap().len()
			}
		})
		.sum()
}

/// A named pipe at `path`: whoever opens it for writing waits until somebody reads it. Put where
/// a checkpoint is about to write a file, it holds the checkpoint at that file for as long as the
/// test likes, with no hook in the product code. The tests that use it run on Linux only: what
/// `std::fs::copy` does with a pipe as its target elsewhere is not something they were written
/// against.
#[cfg(target_os = "linux")]
fn make_pipe(path: &Path) {
	std::fs::create_dir_all(path.parent().unwrap()).unwrap();
	// `mkfifo` is on every unix this runs on; the `mknodat` of `rustix` is not on macOS.
	let status = std::process::Command::new("mkfifo").arg(path).status().expect("mkfifo");
	assert!(status.success(), "mkfifo {} failed: {status}", path.display());
}

/// Reads the pipe at `path` to its end, on a thread of its own: what the checkpoint wrote to it,
/// once the checkpoint has got there.
#[cfg(target_os = "linux")]
fn drain_pipe(path: &Path) -> Call<Vec<u8>> {
	let path = path.to_path_buf();
	on_thread(move || std::fs::read(&path).unwrap())
}

/// Puts what a pipe carried back in its place as a file, so that the directory is a whole
/// checkpoint again.
#[cfg(target_os = "linux")]
fn replace_pipe(path: &Path, bytes: &[u8]) {
	std::fs::remove_file(path).unwrap();
	std::fs::write(path, bytes).unwrap();
}

/// A copy of the directory `src` in `dst`, the way a crash would leave it: no lock file.
fn copy_dir(src: &Path, dst: &Path) {
	std::fs::create_dir_all(dst).unwrap();
	for entry in std::fs::read_dir(src).unwrap() {
		let entry = entry.unwrap();
		if entry.file_name() == "LOCK" {
			continue;
		}
		let to = dst.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &to);
		} else {
			std::fs::copy(entry.path(), &to).unwrap();
		}
	}
}

/// The inode and the number of names of the file at `path`.
#[cfg(unix)]
fn inode_and_links(path: &Path) -> (u64, u64) {
	use std::os::unix::fs::MetadataExt;
	let meta = std::fs::metadata(path).unwrap();
	(meta.ino(), meta.nlink())
}

/// A call on a thread of its own. One that never returns fails its test after `STUCK`, leaving a
/// parked thread behind, instead of hanging the test binary.
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

fn checkpoint(tree: &Arc<Tree>, path: &Path) -> Call<Result<CheckpointMetadata>> {
	let (tree, path) = (Arc::clone(tree), path.to_path_buf());
	on_thread(move || tree.create_checkpoint(&path))
}

fn restore_call(tree: &Arc<Tree>, path: &Path) -> Call<Result<CheckpointMetadata>> {
	let (tree, path) = (Arc::clone(tree), path.to_path_buf());
	on_thread(move || tree.restore_from_checkpoint(&path))
}

/// Restores the checkpoint at `cp` into a fresh tree at `target`.
async fn restored(cp: &Path, target: &Path, vlog: bool) -> (Arc<Tree>, Result<CheckpointMetadata>) {
	let tree = Arc::new(Tree::new(options(target, 256 * 1024, vlog, 1000)).unwrap());
	let outcome = restore_call(&tree, cp).wait();
	(tree, outcome)
}

fn xorshift(state: &mut u64) -> u64 {
	let mut x = *state;
	x ^= x << 13;
	x ^= x >> 7;
	x ^= x << 17;
	*state = x;
	x
}

/// Parks the first caller of the hook that `new` returns until `release`, and reports the id
/// every caller passes it.
struct Gate {
	reached: mpsc::Receiver<u64>,
	release: mpsc::Sender<()>,
}

impl Gate {
	fn new() -> (Self, Arc<dyn Fn(u64) + Send + Sync>) {
		let (reached_tx, reached) = mpsc::channel();
		let (release, release_rx) = mpsc::channel();
		let release_rx = Mutex::new(release_rx);
		let reached_tx = Mutex::new(reached_tx);
		let parked = AtomicBool::new(false);
		let pass: Arc<dyn Fn(u64) + Send + Sync> = Arc::new(move |id| {
			reached_tx.lock().unwrap().send(id).unwrap();
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

/// Parks the first checkpoint that reaches `stage`; the id reported is 0.
fn gate_checkpoint(tree: &Tree, stage: CheckpointStage) -> Gate {
	let (gate, pass) = Gate::new();
	*tree.core.inner.checkpoint_hook.lock() = Some(Arc::new(move |at| {
		if at == stage {
			pass(0);
		}
	}));
	gate
}

/// The stages the checkpoints of a tree reach, in a channel; nothing is parked.
#[cfg(target_os = "linux")]
fn record_checkpoint_stages(tree: &Tree) -> mpsc::Receiver<CheckpointStage> {
	let (tx, rx) = mpsc::channel();
	let tx = Mutex::new(tx);
	*tree.core.inner.checkpoint_hook.lock() = Some(Arc::new(move |stage| {
		let _ = tx.lock().unwrap().send(stage);
	}));
	rx
}

fn clear_hooks(tree: &Tree) {
	*tree.core.inner.flush_hook.lock() = None;
	*tree.core.inner.checkpoint_hook.lock() = None;
	*tree.core.inner.compaction_stage_hook.lock() = None;
}

/// Leaves a tree that a test is done with: closes it, so that nothing of it outlives the test.
async fn finish(tree: Arc<Tree>) {
	clear_hooks(&tree);
	tokio::time::timeout(STUCK, tree.close()).await.expect("close() hung").unwrap();
}

// ---------------------------------------------------------------------------
// 1. point-in-time consistency under writers
// ---------------------------------------------------------------------------

/// What one run of writers and checkpoints looks like.
#[derive(Clone, Copy, Debug)]
struct Shape {
	memtable: usize,
	vlog: bool,
	/// Level-0 tables that ask the background task for a compaction.
	level0_max_files: usize,
	writers: usize,
	/// Transactions of each writer.
	txns: usize,
	/// Checkpoints of each checkpointer while the writers run.
	checkpoints: usize,
	/// Checkpointers that take a checkpoint at the same time, each into directories of its own.
	checkpointers: usize,
	/// Threads that rotate the memtable by hand all the time.
	rotators: usize,
	seed: u64,
}

/// Where each writer is: the transaction it has begun, and the last one acknowledged.
struct Progress {
	started: Vec<AtomicUsize>,
	acked: Vec<AtomicUsize>,
}

impl Progress {
	fn new(writers: usize) -> Arc<Self> {
		Arc::new(Self {
			started: (0..writers).map(|_| AtomicUsize::new(0)).collect(),
			acked: (0..writers).map(|_| AtomicUsize::new(0)).collect(),
		})
	}

	fn started(&self) -> Vec<usize> {
		self.started.iter().map(|n| n.load(Ordering::SeqCst)).collect()
	}

	fn acked(&self) -> Vec<usize> {
		self.acked.iter().map(|n| n.load(Ordering::SeqCst)).collect()
	}

	fn total_acked(&self) -> usize {
		self.acked().iter().sum()
	}
}

type Failures = Arc<Mutex<Vec<String>>>;

fn group_key(kind: char, writer: usize, j: usize) -> Vec<u8> {
	format!("w{writer}_{kind}_{j:05}").into_bytes()
}

fn marker_key(writer: usize) -> Vec<u8> {
	format!("w{writer}_last").into_bytes()
}

/// The value the three keys of transaction `j` of `writer` hold.
fn group_val(writer: usize, j: usize) -> Vec<u8> {
	let mut v = format!("w{writer}_j{j:05}_").into_bytes();
	let len = 48 + (j * 37 + writer * 11) % 160;
	v.resize(len.max(v.len()), b'p');
	v
}

/// A checkpoint that was taken while the writers ran, with what was known around the call.
struct Taken {
	path: PathBuf,
	metadata: CheckpointMetadata,
	/// Transactions acknowledged before the call began, per writer.
	acked_before: Vec<usize>,
	/// Transactions begun when the call returned, per writer.
	started_after: Vec<usize>,
	/// The visible sequence number before the call began.
	visible_before: u64,
	/// The next sequence number to assign, after the call returned.
	log_seq_after: u64,
}

/// Takes a checkpoint of `tree` into `path`, noting what the writers had done around the call.
fn take(tree: &Tree, progress: &Progress, path: &Path) -> std::result::Result<Taken, String> {
	let acked_before = progress.acked();
	let visible_before = tree.core.seq_num();
	let metadata = tree.create_checkpoint(path).map_err(|e| format!("create_checkpoint: {e:?}"))?;
	let started_after = progress.started();
	let log_seq_after = tree.core.commit_pipeline.log_seq_num.load(Ordering::SeqCst);
	Ok(Taken {
		path: path.to_path_buf(),
		metadata,
		acked_before,
		started_after,
		visible_before,
		log_seq_after,
	})
}

/// One writer per `shape.writers`: each commits `shape.txns` transactions of four keys, three
/// that hold the same value and a marker that holds the number of the transaction.
fn spawn_writers(
	tree: &Arc<Tree>,
	shape: Shape,
	progress: &Arc<Progress>,
	failures: &Failures,
) -> Vec<tokio::task::JoinHandle<()>> {
	(0..shape.writers)
		.map(|w| {
			let (tree, progress, failures) =
				(Arc::clone(tree), Arc::clone(progress), Arc::clone(failures));
			tokio::spawn(async move {
				let mut rng = shape.seed ^ ((w as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15));
				for j in 1..=shape.txns {
					progress.started[w].store(j, Ordering::SeqCst);
					let mut txn = tree.begin().unwrap();
					txn.set_durability(Durability::Immediate);
					let value = group_val(w, j);
					for kind in ['a', 'b', 'c'] {
						txn.set(group_key(kind, w, j), value.clone()).unwrap();
					}
					txn.set(marker_key(w), j.to_string().into_bytes()).unwrap();
					match txn.commit().await {
						Ok(()) => progress.acked[w].store(j, Ordering::SeqCst),
						Err(e) => {
							failures.lock().unwrap().push(format!("commit {w}/{j}: {e:?}"));
							return;
						}
					}
					if xorshift(&mut rng) % 4 == 0 {
						tokio::task::yield_now().await;
					}
				}
			})
		})
		.collect()
}

/// Threads that rotate the memtable by hand, and wake the background flush as the pipeline does,
/// until `stop`.
fn spawn_rotators(
	tree: &Arc<Tree>,
	shape: Shape,
	stop: &Arc<AtomicBool>,
) -> Vec<std::thread::JoinHandle<()>> {
	(0..shape.rotators)
		.map(|r| {
			let (tree, stop) = (Arc::clone(tree), Arc::clone(stop));
			std::thread::spawn(move || {
				let mut rng = shape.seed ^ ((r as u64 + 101).wrapping_mul(0x2545_F491_4F6C_DD1D));
				while !stop.load(Ordering::SeqCst) {
					// Not faster than the flush can follow, or the queue only grows.
					if tree.core.inner.immutable_count() < 3 {
						tree.core.inner.rotate_memtable().unwrap();
						if let Some(tm) = tree.core.task_manager.lock().unwrap().as_ref() {
							tm.wake_up_memtable();
						}
					}
					std::thread::sleep(Duration::from_millis(2 + xorshift(&mut rng) % 6));
				}
			})
		})
		.collect()
}

/// Takes `shape.checkpoints` checkpoints, evenly spread over the writers' progress, into
/// directories of its own under `out`. With several checkpointers they take each one at the
/// same moment.
fn checkpointer(
	tree: Arc<Tree>,
	shape: Shape,
	progress: Arc<Progress>,
	out: PathBuf,
	id: usize,
	barrier: Arc<Barrier>,
) -> Vec<std::result::Result<Taken, String>> {
	let total = shape.writers * shape.txns;
	let mut taken = Vec::new();
	for n in 0..shape.checkpoints {
		let threshold = (n + 1) * total / (shape.checkpoints + 1);
		let deadline = Instant::now() + 2 * STUCK;
		while progress.total_acked() < threshold && Instant::now() < deadline {
			std::thread::sleep(Duration::from_millis(1));
		}
		if shape.checkpointers > 1 {
			barrier.wait();
		}
		taken.push(take(&tree, &progress, &out.join(format!("cp_{id}_{n}"))));
	}
	taken
}

/// Checks the state of `tree`, restored from the checkpoint `taken`: whole transactions, a prefix
/// of each writer's, bounded by what the writers did around the call, and exactly the
/// transactions with a sequence number up to the checkpoint's. `live_seqs` holds the sequence
/// number of transaction `j` of writer `w`, as the live tree recorded it.
fn check_state(
	tree: &Tree,
	taken: &Taken,
	restored_metadata: &CheckpointMetadata,
	shape: Shape,
	live_seqs: &BTreeMap<(usize, usize), u64>,
	label: &str,
) {
	let seed = shape.seed;
	let state = contents(tree);
	let mut expected_len = 0;
	let mut found = Vec::with_capacity(shape.writers);
	for w in 0..shape.writers {
		let m: usize = match state.get(&marker_key(w)) {
			None => 0,
			Some(v) => String::from_utf8_lossy(v)
				.parse()
				.unwrap_or_else(|_| panic!("{label} (seed {seed}): writer {w}'s marker is {v:?}")),
		};
		found.push(m);
		assert!(m <= shape.txns, "{label} (seed {seed}): writer {w}'s marker is {m}");
		if m > 0 {
			expected_len += 1;
		}
		for j in 1..=shape.txns {
			let values: Vec<Option<&Vec<u8>>> =
				['a', 'b', 'c'].iter().map(|&kind| state.get(&group_key(kind, w, j))).collect();
			if j <= m {
				for (kind, value) in ['a', 'b', 'c'].iter().zip(&values) {
					assert_eq!(
						*value,
						Some(&group_val(w, j)),
						"{label} (seed {seed}): writer {w}'s transaction {j} is torn: key {kind}, \
						 its marker says {m}"
					);
				}
				expected_len += 3;
			} else {
				assert!(
					values.iter().all(|v| v.is_none()),
					"{label} (seed {seed}): writer {w}'s transaction {j} is above its marker {m}, \
					 so the committed set is not a prefix: {values:?}"
				);
			}
		}
		assert!(
			taken.acked_before[w] <= m,
			"{label} (seed {seed}): writer {w}: {} transactions were acknowledged before the \
			 checkpoint began, and it holds {m}",
			taken.acked_before[w]
		);
		assert!(
			m <= taken.started_after[w],
			"{label} (seed {seed}): writer {w}: {} transactions had begun when the checkpoint \
			 returned, and it holds {m}",
			taken.started_after[w]
		);
	}
	assert_eq!(state.len(), expected_len, "{label} (seed {seed}): keys that no transaction wrote");

	// The sequence number: the restored tree is at the checkpoint's, which lies between what was
	// visible before the call and what had been assigned when it returned, nothing in the tables
	// is above it, and the transactions it holds are exactly the ones numbered up to it.
	let sequence = restored_metadata.sequence_number;
	assert_eq!(
		sequence, taken.metadata.sequence_number,
		"{label} (seed {seed}): the metadata file differs from what create_checkpoint returned"
	);
	assert_eq!(
		tree.core.seq_num(),
		sequence,
		"{label} (seed {seed}): the restored tree is not at the checkpoint's sequence number"
	);
	assert!(
		taken.visible_before <= sequence,
		"{label} (seed {seed}): the sequence number {sequence} is below {}, which was visible \
		 before the call",
		taken.visible_before
	);
	assert!(
		sequence < taken.log_seq_after,
		"{label} (seed {seed}): the sequence number {sequence} was never assigned: the next one \
		 was {} when the call returned",
		taken.log_seq_after
	);
	let entries = table_entries(tree);
	let highest = entries.iter().map(|(_, seq)| *seq).max().unwrap_or(0);
	assert_eq!(
		highest, sequence,
		"{label} (seed {seed}): the tables of the checkpoint reach {highest}, its metadata says \
		 {sequence}"
	);
	assert_eq!(
		table_ids(tree).len(),
		taken.metadata.sstable_count,
		"{label} (seed {seed}): the number of tables"
	);
	for (w, &holds) in found.iter().enumerate() {
		for j in 1..=shape.txns {
			let numbered = live_seqs
				.get(&(w, j))
				.copied()
				.unwrap_or_else(|| panic!("{label}: no sequence number for {w}/{j}"));
			assert_eq!(
				j <= holds,
				numbered <= sequence,
				"{label} (seed {seed}): writer {w}'s transaction {j} has sequence number \
				 {numbered}, the checkpoint's is {sequence}, and it holds {} of the writer's \
				 transactions",
				holds
			);
		}
	}
}

/// Restores every checkpoint of `taken` into a fresh tree, one at a time, and checks it.
async fn verify_all(
	taken: &[Taken],
	shape: Shape,
	live_seqs: &BTreeMap<(usize, usize), u64>,
	scratch: &Path,
	label: &str,
) {
	for (n, taken) in taken.iter().enumerate() {
		let target = scratch.join(format!("restored_{n}"));
		let (tree, outcome) = restored(&taken.path, &target, shape.vlog).await;
		let metadata = outcome.unwrap_or_else(|e| {
			panic!("{label} (seed {}): restoring {n} failed: {e:?}", shape.seed)
		});
		check_state(&tree, taken, &metadata, shape, live_seqs, &format!("{label}, checkpoint {n}"));
		finish(tree).await;
		let _ = std::fs::remove_dir_all(&target);
		let _ = std::fs::remove_dir_all(&taken.path);
	}
}

/// The sequence number of every transaction, as the tree records it: of the `a` key, which only
/// that transaction wrote. The tree is flushed first, so that the tables hold everything.
fn live_sequence_numbers(tree: &Tree, shape: Shape) -> BTreeMap<(usize, usize), u64> {
	tree.flush().unwrap();
	let by_key: BTreeMap<Vec<u8>, u64> = table_entries(tree).into_iter().collect();
	let mut out = BTreeMap::new();
	for w in 0..shape.writers {
		for j in 1..=shape.txns {
			let seq = by_key
				.get(&group_key('a', w, j))
				.unwrap_or_else(|| panic!("the tree lost transaction {w}/{j}"));
			out.insert((w, j), *seq);
		}
	}
	out
}

/// Runs the writers and the checkpointers of `shape`, and verifies every checkpoint taken.
async fn point_in_time(shape: Shape, name: &str) {
	let dir = TempDir::new(name).unwrap();
	let out = TempDir::new(&format!("{name}_out")).unwrap();
	let scratch = TempDir::new(&format!("{name}_scratch")).unwrap();
	let tree = Arc::new(
		Tree::new(options(dir.path(), shape.memtable, shape.vlog, shape.level0_max_files)).unwrap(),
	);
	let progress = Progress::new(shape.writers);
	let failures: Failures = Default::default();
	let stop = Arc::new(AtomicBool::new(false));

	let rotators = spawn_rotators(&tree, shape, &stop);
	let writers = spawn_writers(&tree, shape, &progress, &failures);
	let barrier = Arc::new(Barrier::new(shape.checkpointers));
	let checkpointers: Vec<_> = (0..shape.checkpointers)
		.map(|id| {
			let (tree, progress, out, barrier) = (
				Arc::clone(&tree),
				Arc::clone(&progress),
				out.path().to_path_buf(),
				Arc::clone(&barrier),
			);
			on_thread(move || checkpointer(tree, shape, progress, out, id, barrier))
		})
		.collect();

	let finished = tokio::time::timeout(2 * STUCK, async {
		for writer in writers {
			writer.await.unwrap();
		}
	})
	.await;
	stop.store(true, Ordering::SeqCst);
	for rotator in rotators {
		rotator.join().unwrap();
	}
	assert!(finished.is_ok(), "{name} (seed {}): the writers did not finish", shape.seed);
	let mut taken = Vec::new();
	for call in checkpointers {
		for result in call.wait() {
			match result {
				Ok(t) => taken.push(t),
				Err(e) => failures.lock().unwrap().push(e),
			}
		}
	}
	// One when the writers are done: it holds every transaction.
	let last = {
		let (tree, progress, path) =
			(Arc::clone(&tree), Arc::clone(&progress), out.path().join("last"));
		on_thread(move || take(&tree, &progress, &path)).wait()
	};
	match last {
		Ok(t) => taken.push(t),
		Err(e) => failures.lock().unwrap().push(e),
	}
	let failures = failures.lock().unwrap().clone();
	assert!(
		failures.is_empty(),
		"{name} (seed {}): {} failures, first few: {:?}",
		shape.seed,
		failures.len(),
		&failures[..failures.len().min(5)]
	);
	assert!(tree.core.inner.error_handler.check_error().is_ok(), "a background error was recorded");

	// Not vacuous: checkpoints were taken while the writers were between transactions.
	let total = shape.writers * shape.txns;
	let partial = taken
		.iter()
		.filter(|t| {
			let acked: usize = t.acked_before.iter().sum();
			acked > 0 && acked < total
		})
		.count();
	assert!(
		partial >= shape.checkpoints.min(3),
		"{name} (seed {}): only {partial} of {} checkpoints were taken while the writers ran",
		shape.seed,
		taken.len()
	);

	let live_seqs = live_sequence_numbers(&tree, shape);
	verify_all(&taken, shape, &live_seqs, scratch.path(), name).await;

	// The last one holds every transaction of every writer.
	let last = taken.last().unwrap();
	assert_eq!(last.acked_before, vec![shape.txns; shape.writers], "{name}: the final checkpoint");
	finish(tree).await;
}

/// Writers that commit groups of keys with fsync, checkpoints taken while they run, background
/// flush and compaction on: each checkpoint holds whole transactions, a prefix of every writer's,
/// no fewer than the ones acknowledged before it and no more than the ones begun before it
/// returned, and exactly the ones numbered up to its sequence number.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn checkpoints_under_group_writers_hold_whole_transactions_and_a_prefix_of_each_writer() {
	point_in_time(
		Shape {
			memtable: 16 * 1024,
			vlog: false,
			level0_max_files: 2,
			writers: 4,
			txns: 80,
			checkpoints: 6,
			checkpointers: 1,
			rotators: 0,
			seed: 0x5EED_0001,
		},
		"ckpt_conc_plain",
	)
	.await;
}

/// The same with a memtable so small that the commits rotate it and the WAL every few
/// transactions, and threads that rotate it by hand on top, with values in the value log.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn checkpoints_racing_a_rotation_storm_with_a_value_log_hold_whole_transactions() {
	point_in_time(
		Shape {
			memtable: 4 * 1024,
			vlog: true,
			level0_max_files: 2,
			writers: 6,
			txns: 50,
			checkpoints: 6,
			checkpointers: 1,
			rotators: 2,
			seed: 0x5EED_0002,
		},
		"ckpt_conc_storm",
	)
	.await;
}

/// Two checkpoints taken at the same moment into different directories, round after round, while
/// writers commit and the memtable rotates: both complete, and both restore to a state that holds
/// whole transactions at the sequence number they report.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn two_checkpoints_at_once_into_different_directories_both_restore_consistently() {
	point_in_time(
		Shape {
			memtable: 8 * 1024,
			vlog: true,
			level0_max_files: 3,
			writers: 4,
			txns: 60,
			checkpoints: 4,
			checkpointers: 2,
			rotators: 1,
			seed: 0x5EED_0003,
		},
		"ckpt_conc_pair",
	)
	.await;
}

// ---------------------------------------------------------------------------
// 2. a checkpoint beside a flush and a compaction
// ---------------------------------------------------------------------------

/// The compaction stages a tree's compactions reach, with a send for each.
fn record_compaction_stages(tree: &Tree) -> mpsc::Receiver<CompactionStage> {
	let (tx, rx) = mpsc::channel();
	let tx = Mutex::new(tx);
	*tree.core.inner.compaction_stage_hook.lock() = Some(Arc::new(move |stage| {
		let _ = tx.lock().unwrap().send(stage);
		Ok(())
	}));
	rx
}

/// The flush hook that reports the id of every table written, without parking.
fn record_flushes(tree: &Tree) -> mpsc::Receiver<u64> {
	let (tx, rx) = mpsc::channel();
	let tx = Mutex::new(tx);
	let hook: FlushHook = Arc::new(move |table_id| {
		let _ = tx.lock().unwrap().send(table_id);
		Ok(())
	});
	*tree.core.inner.flush_hook.lock() = Some(hook);
	rx
}

/// The strategy that merges every level-0 table of the database at `path`.
fn merging_strategy(path: &Path, vlog: bool) -> Arc<dyn CompactionStrategy> {
	Arc::new(Strategy::from_options(options(path, NEVER_ROTATING, vlog, 2)))
}

/// A tree with two flushed tables, a compaction that would merge them, and its strategy.
struct Merging {
	tree: Arc<Tree>,
	strategy: Arc<dyn CompactionStrategy>,
	/// What the two tables hold.
	before: Contents,
}

/// The tree holds no compaction trigger of its own, so only the compaction a test runs by hand
/// ever reaches the stage hook.
async fn two_tables(path: &Path, vlog: bool) -> Merging {
	let tree = open(path, vlog);
	let mut before = Contents::new();
	put(&tree, 0..200, 1, &mut before).await;
	flush_to_tables(&tree);
	// Every key again, so that merging the tables makes the whole first round obsolete, the
	// value-log files of it included.
	put(&tree, 0..200, 2, &mut before).await;
	flush_to_tables(&tree);
	assert_eq!(table_ids(&tree), vec![1, 2]);
	let strategy = merging_strategy(path, vlog);
	Merging {
		tree,
		strategy,
		before,
	}
}

/// A flush and a compaction that arrive while a checkpoint holds the manifest cannot change it:
/// the flush has written its table and the compaction waits to start, and both wait for the
/// checkpoint to let go. Then both finish, the checkpoint holds the tables and the manifest it
/// copied, which agree, and restores to exactly the state it had when it began.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_flush_and_a_compaction_wait_for_the_checkpoint_that_holds_the_manifest() {
	for vlog in [false, true] {
		let dir = TempDir::new("ckpt_conc_hold").unwrap();
		let out = TempDir::new("ckpt_conc_hold_out").unwrap();
		let Merging {
			tree,
			strategy,
			before,
		} = two_tables(dir.path(), vlog).await;
		let cp = out.path().join("cp");

		let gate = gate_checkpoint(&tree, CheckpointStage::SstablesCopied);
		let taking = checkpoint(&tree, &cp);
		assert_eq!(gate.next(REACHED), Some(0), "vlog {vlog}: the checkpoint copied the tables");

		// The checkpoint holds the manifest for reading, so nobody can write it.
		assert!(
			tree.core.inner.level_manifest.try_write().is_err(),
			"vlog {vlog}: the manifest is not held while the SSTables are copied"
		);

		// More data and a table to flush, written while the checkpoint is parked. Nothing queues
		// for the manifest yet, so writing and rotating are free to read it.
		let mut after = before.clone();
		put(&tree, 300..400, 3, &mut after).await;
		tree.core.inner.rotate_memtable().unwrap();
		let flushes = record_flushes(&tree);
		let stages = record_compaction_stages(&tree);

		// From here on a writer waits for the manifest, and a reader behind it does too: the test
		// does not touch the manifest until the checkpoint is released.
		let flushing = {
			let tree = Arc::clone(&tree);
			on_thread(move || tree.core.inner.flush_all_immutables_sync())
		};
		assert_eq!(
			flushes.recv_timeout(REACHED).ok(),
			Some(3),
			"vlog {vlog}: the flush wrote its table and is about to list it"
		);
		let compacting = {
			let (tree, strategy) = (Arc::clone(&tree), Arc::clone(&strategy));
			on_thread(move || tree.compact(strategy))
		};
		assert!(
			stages.recv_timeout(GRACE).is_err(),
			"vlog {vlog}: the compaction advanced while a checkpoint held the manifest"
		);
		assert!(!flushing.is_finished(), "vlog {vlog}: the flush listed its table meanwhile");
		assert!(!compacting.is_finished(), "vlog {vlog}: the compaction finished meanwhile");
		assert!(!taking.is_finished());
		assert!(!cp.join("manifest").exists(), "vlog {vlog}: the manifest was copied early");

		gate.release();
		let metadata = taking.wait().unwrap();
		flushing.wait().unwrap();
		compacting.wait().unwrap();

		// The compaction ran to the end once the manifest was free.
		let reached: Vec<_> = stages.try_iter().collect();
		assert_eq!(
			reached,
			vec![
				CompactionStage::OutputsDurable,
				CompactionStage::ManifestWritten,
				CompactionStage::InputsRemoved,
				CompactionStage::VlogCleaned,
			],
			"vlog {vlog}: the compaction after the checkpoint"
		);
		clear_hooks(&tree);

		// The checkpoint holds what it copied: tables 1 and 2, as its manifest lists them, and
		// the live tree has since merged them away.
		assert_eq!(metadata.sstable_count, 2, "vlog {vlog}");
		let listed = manifest_tables_of(&cp);
		assert_eq!(listed, BTreeSet::from([1, 2]), "vlog {vlog}: the tables the manifest lists");
		let files = names_in(&cp, "sstables");
		assert_eq!(files, vec![sst_name(1), sst_name(2)], "vlog {vlog}: the checkpoint's tables");
		// The size it reports is that of the tables, the manifest and the value log it copied.
		let on_disk: u64 =
			["sstables", "manifest", "vlog"].iter().map(|sub| dir_size(&cp.join(sub))).sum();
		assert!(on_disk > 0, "vlog {vlog}");
		assert_eq!(metadata.total_size, on_disk, "vlog {vlog}: the size the metadata reports");
		let live = table_ids(&tree);
		assert!(
			!live.contains(&1) && !live.contains(&2),
			"vlog {vlog}: the compaction did not merge the tables: {live:?}"
		);
		assert_contents(&tree, &after, "the live tree");

		let target = out.path().join("restored");
		let (copy, outcome) = restored(&cp, &target, vlog).await;
		let restored_metadata = outcome.unwrap();
		assert_eq!(restored_metadata.sequence_number, metadata.sequence_number);
		assert_eq!(copy.core.seq_num(), metadata.sequence_number, "vlog {vlog}");
		assert_contents(&copy, &before, &format!("vlog {vlog}: the restored checkpoint"));
		finish(copy).await;
		finish(tree).await;
	}
}

/// With the manifest released and the value log not yet copied, a flush and a compaction both
/// complete, and the files of the value log that the compaction made obsolete stay until the
/// checkpoint is done: the checkpoint's manifest lists tables whose values are all in its copy.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_flush_and_a_compaction_finish_while_a_checkpoint_has_yet_to_copy_the_value_log() {
	for vlog in [false, true] {
		let dir = TempDir::new("ckpt_conc_released").unwrap();
		let out = TempDir::new("ckpt_conc_released_out").unwrap();
		let Merging {
			tree,
			strategy,
			before,
		} = two_tables(dir.path(), vlog).await;
		let cp = out.path().join("cp");
		let vlog_before = names_in(dir.path(), "vlog");
		if vlog {
			assert!(vlog_before.len() > 4, "vlog files: {vlog_before:?}");
		}

		let gate = gate_checkpoint(&tree, CheckpointStage::ManifestReleased);
		let taking = checkpoint(&tree, &cp);
		assert_eq!(gate.next(REACHED), Some(0), "vlog {vlog}: the manifest is released");
		assert!(
			tree.core.inner.level_manifest.try_write().is_ok(),
			"vlog {vlog}: the manifest is still held after it was copied"
		);
		assert!(cp.join("manifest").exists(), "vlog {vlog}");
		assert!(!cp.join("CHECKPOINT_METADATA").exists(), "vlog {vlog}: metadata before the end");

		// The tree moves on while the checkpoint is parked: a flush, then the compaction.
		let mut after = before.clone();
		put(&tree, 300..400, 3, &mut after).await;
		flush_to_tables(&tree);
		let stages = record_compaction_stages(&tree);
		let (compacting_tree, compacting_strategy) = (Arc::clone(&tree), Arc::clone(&strategy));
		on_thread(move || compacting_tree.compact(compacting_strategy)).wait().unwrap();
		let reached: Vec<_> = stages.try_iter().collect();
		assert_eq!(
			reached.last(),
			Some(&CompactionStage::VlogCleaned),
			"vlog {vlog}: the compaction did not finish while the checkpoint was parked: {reached:?}"
		);
		assert!(!table_ids(&tree).contains(&1), "vlog {vlog}: the tables were not merged");
		assert!(!taking.is_finished());
		let live_vlog = names_in(dir.path(), "vlog");
		for name in &vlog_before {
			assert!(
				live_vlog.contains(name),
				"vlog {vlog}: the compaction deleted {name}, which the checkpoint has yet to copy"
			);
		}

		gate.release();
		let metadata = taking.wait().unwrap();
		clear_hooks(&tree);
		assert_eq!(
			metadata.sstable_count, 2,
			"vlog {vlog}: the checkpoint is of the earlier state"
		);
		assert_eq!(manifest_tables_of(&cp), BTreeSet::from([1, 2]), "vlog {vlog}");
		for name in &vlog_before {
			assert!(
				cp.join("vlog").join(name).exists(),
				"vlog {vlog}: the checkpoint lacks {name}"
			);
		}

		let target = out.path().join("restored");
		let (copy, outcome) = restored(&cp, &target, vlog).await;
		outcome.unwrap();
		assert_eq!(copy.core.seq_num(), metadata.sequence_number, "vlog {vlog}");
		assert_contents(&copy, &before, &format!("vlog {vlog}: the restored checkpoint"));
		finish(copy).await;
		assert_contents(&tree, &after, "the live tree");

		// Not vacuous: with the checkpoint done the next flush deletes the files the compaction
		// made obsolete, so there were files to keep.
		if vlog {
			put(&tree, 400..410, 4, &mut after).await;
			flush_to_tables(&tree);
			let now = names_in(dir.path(), "vlog");
			assert!(
				vlog_before.iter().any(|name| !now.contains(name)),
				"vlog {vlog}: no value-log file was obsolete, so keeping them proved nothing"
			);
			assert_contents(&tree, &after, "the live tree after the checkpoint");
		}
		finish(tree).await;
	}
}

/// The two copies that no hook of the product code covers: the manifest, and the value log. A
/// named pipe stands where the checkpoint writes one file of each, so the checkpoint waits at it
/// for the test to read it. While it copies the manifest nobody can write it, and the value-log
/// files, the manifest, and the metadata file that makes the directory a checkpoint, are in the
/// state they must be in while it copies the value log: the manifest free, every value-log file in
/// place, the metadata not written yet.
#[cfg(target_os = "linux")]
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_holds_the_manifest_while_it_copies_it_and_writes_its_metadata_after_the_value_log(
) {
	for vlog in [false, true] {
		let dir = TempDir::new("ckpt_conc_window").unwrap();
		let out = TempDir::new("ckpt_conc_window_out").unwrap();
		let Merging {
			tree,
			strategy,
			before,
		} = two_tables(dir.path(), vlog).await;
		let cp = out.path().join("cp");
		let vlog_before = names_in(dir.path(), "vlog");
		let opts = options(dir.path(), NEVER_ROTATING, vlog, 1000);
		let manifest_pipe =
			cp.join("manifest").join(opts.manifest_file_path(0).file_name().unwrap());
		make_pipe(&manifest_pipe);
		let vlog_pipe = vlog.then(|| {
			assert!(vlog_before.len() > 4, "vlog files: {vlog_before:?}");
			cp.join("vlog").join(&vlog_before[0])
		});
		if let Some(pipe) = &vlog_pipe {
			make_pipe(pipe);
		}
		let stages = record_checkpoint_stages(&tree);
		let taking = checkpoint(&tree, &cp);
		assert_eq!(
			stages.recv_timeout(REACHED).ok(),
			Some(CheckpointStage::SstablesCopied),
			"vlog {vlog}: the checkpoint copied the tables"
		);

		// The checkpoint is at the manifest, which it must hold until it has copied it. The wait is
		// long enough for a lock released too early to show, and nothing else takes it here.
		let until = Instant::now() + GRACE;
		while Instant::now() < until {
			assert!(
				tree.core.inner.level_manifest.try_write().is_err(),
				"vlog {vlog}: the manifest was free while the checkpoint had yet to copy it"
			);
			assert!(
				stages.try_recv().is_err(),
				"vlog {vlog}: the checkpoint released the manifest before it copied it"
			);
			assert!(!taking.is_finished(), "vlog {vlog}");
			std::thread::sleep(Duration::from_millis(5));
		}
		let manifest_bytes = drain_pipe(&manifest_pipe).wait();
		assert!(!manifest_bytes.is_empty(), "vlog {vlog}: the manifest the checkpoint wrote");
		assert_eq!(
			stages.recv_timeout(REACHED).ok(),
			Some(CheckpointStage::ManifestReleased),
			"vlog {vlog}: the checkpoint copied the manifest"
		);

		let mut vlog_bytes = None;
		if let Some(pipe) = &vlog_pipe {
			// The checkpoint is at a value-log file. The manifest is free, and a flush and a
			// compaction run to the end, but the files of the value log stay for the copy.
			let until = Instant::now() + GRACE;
			while Instant::now() < until {
				assert!(
					!cp.join("CHECKPOINT_METADATA").exists(),
					"vlog {vlog}: the metadata was written before the value log was copied"
				);
				assert!(!taking.is_finished(), "vlog {vlog}");
				std::thread::sleep(Duration::from_millis(5));
			}
			assert!(
				tree.core.inner.level_manifest.try_write().is_ok(),
				"vlog {vlog}: the manifest is held while the value log is copied"
			);
			let mut after = before.clone();
			put(&tree, 300..400, 3, &mut after).await;
			let compaction = record_compaction_stages(&tree);
			let moving_on = {
				let (tree, strategy) = (Arc::clone(&tree), Arc::clone(&strategy));
				on_thread(move || {
					flush_to_tables(&tree);
					tree.compact(strategy)
				})
			};
			moving_on.wait().unwrap();
			let reached: Vec<_> = compaction.try_iter().collect();
			assert_eq!(
				reached.last(),
				Some(&CompactionStage::VlogCleaned),
				"vlog {vlog}: the compaction did not finish while the checkpoint copied the value \
				 log: {reached:?}"
			);
			assert!(!table_ids(&tree).contains(&1), "vlog {vlog}: the tables were not merged");
			let live_vlog = names_in(dir.path(), "vlog");
			for name in &vlog_before {
				assert!(
					live_vlog.contains(name),
					"vlog {vlog}: {name} was deleted while the checkpoint was copying the value log"
				);
			}
			assert!(!taking.is_finished(), "vlog {vlog}");
			vlog_bytes = Some(drain_pipe(pipe).wait());
		}

		let metadata = taking.wait().unwrap();
		clear_hooks(&tree);
		assert!(cp.join("CHECKPOINT_METADATA").exists(), "vlog {vlog}");
		assert_eq!(metadata.sstable_count, 2, "vlog {vlog}");

		// The pipes carried what the files hold: put back, the directory is the checkpoint of the
		// state before the pipes, in full.
		replace_pipe(&manifest_pipe, &manifest_bytes);
		if let (Some(pipe), Some(bytes)) = (&vlog_pipe, &vlog_bytes) {
			replace_pipe(pipe, bytes);
		}
		assert_eq!(manifest_tables_of(&cp), BTreeSet::from([1, 2]), "vlog {vlog}");
		let target = out.path().join("restored");
		let (copy, outcome) = restored(&cp, &target, vlog).await;
		assert_eq!(outcome.unwrap().sequence_number, metadata.sequence_number);
		assert_contents(&copy, &before, &format!("vlog {vlog}: the restored checkpoint"));
		finish(copy).await;

		// Not vacuous: once the checkpoint is done the next flush deletes the files the compaction
		// made obsolete, so there were files to keep.
		if vlog {
			let mut now_state = Contents::new();
			put(&tree, 400..410, 4, &mut now_state).await;
			flush_to_tables(&tree);
			let now = names_in(dir.path(), "vlog");
			assert!(
				vlog_before.iter().any(|name| !now.contains(name)),
				"vlog {vlog}: no value-log file was obsolete, so keeping them proved nothing"
			);
		}
		finish(tree).await;
	}
}

// ---------------------------------------------------------------------------
// 3. value-log churn with background compaction
// ---------------------------------------------------------------------------

fn churn_key(writer: usize, i: usize) -> Vec<u8> {
	format!("c{writer}_k{i:03}").into_bytes()
}

/// The value of key `i` of `writer` at `version`: long, and whole only if every byte is there.
fn churn_val(writer: usize, i: usize, version: usize) -> Vec<u8> {
	let mut v = format!("c{writer}_k{i:03}_v{version:05}_").into_bytes();
	v.resize(180 + (i * 29 + version * 7 + writer) % 300, b'0' + (version % 10) as u8);
	v
}

/// Writers overwrite the same few keys with long values, so that the value-log files of the old
/// versions become obsolete and the background compaction deletes them, while checkpoints are
/// taken. Every checkpoint restores and reads every value through its pointer: each is the
/// version it was, or a later one, intact.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn checkpoints_during_value_log_churn_and_background_compaction_read_back_every_value() {
	const WRITERS: usize = 4;
	const KEYS: usize = 12;
	const VERSIONS: usize = 60;
	const CHECKPOINTS: usize = 6;
	let seed = 0x5EED_0010u64;

	let dir = TempDir::new("ckpt_conc_churn").unwrap();
	let out = TempDir::new("ckpt_conc_churn_out").unwrap();
	let scratch = TempDir::new("ckpt_conc_churn_scratch").unwrap();
	let tree = Arc::new(Tree::new(options(dir.path(), 6 * 1024, true, 2)).unwrap());

	// The newest version of each key that was acknowledged, and how many commits were.
	let acked: Arc<Mutex<BTreeMap<(usize, usize), usize>>> = Default::default();
	let commits = Arc::new(AtomicUsize::new(0));
	let failures: Failures = Default::default();
	let writers: Vec<_> = (0..WRITERS)
		.map(|w| {
			let (tree, acked, commits, failures) = (
				Arc::clone(&tree),
				Arc::clone(&acked),
				Arc::clone(&commits),
				Arc::clone(&failures),
			);
			tokio::spawn(async move {
				let mut rng = seed ^ ((w as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15));
				for version in 1..=VERSIONS {
					for i in 0..KEYS {
						let mut txn = tree.begin().unwrap();
						txn.set(churn_key(w, i), churn_val(w, i, version)).unwrap();
						match txn.commit().await {
							Ok(()) => {
								acked.lock().unwrap().insert((w, i), version);
								commits.fetch_add(1, Ordering::SeqCst);
							}
							Err(e) => {
								failures.lock().unwrap().push(format!("commit {w}/{i}: {e:?}"));
								return;
							}
						}
					}
					if xorshift(&mut rng) % 3 == 0 {
						tokio::task::yield_now().await;
					}
				}
			})
		})
		.collect();

	let seen_vlog: Arc<Mutex<BTreeSet<String>>> = Default::default();
	let checkpointing = {
		let (tree, acked, commits, out, seen_vlog, db_dir) = (
			Arc::clone(&tree),
			Arc::clone(&acked),
			Arc::clone(&commits),
			out.path().to_path_buf(),
			Arc::clone(&seen_vlog),
			dir.path().to_path_buf(),
		);
		on_thread(move || {
			let mut taken = Vec::new();
			for n in 0..CHECKPOINTS {
				let threshold = (n + 1) * WRITERS * KEYS * VERSIONS / (CHECKPOINTS + 1);
				let deadline = Instant::now() + 2 * STUCK;
				while commits.load(Ordering::SeqCst) < threshold && Instant::now() < deadline {
					std::thread::sleep(Duration::from_millis(1));
				}
				seen_vlog.lock().unwrap().extend(names_in(&db_dir, "vlog"));
				let floor = acked.lock().unwrap().clone();
				let path = out.join(format!("cp{n}"));
				taken.push((tree.create_checkpoint(&path).map(|m| (path, m)), floor));
			}
			taken
		})
	};

	let finished = tokio::time::timeout(2 * STUCK, async {
		for writer in writers {
			writer.await.unwrap();
		}
	})
	.await;
	assert!(finished.is_ok(), "the writers did not finish");
	let taken = checkpointing.wait();
	let failures = failures.lock().unwrap().clone();
	assert!(failures.is_empty(), "failures: {failures:?}");
	assert_eq!(taken.len(), CHECKPOINTS);

	for (n, (result, floor)) in taken.into_iter().enumerate() {
		let (path, metadata) = result.unwrap_or_else(|e| panic!("checkpoint {n} failed: {e:?}"));
		assert!(!floor.is_empty(), "checkpoint {n} was taken before anything was written");
		let target = scratch.path().join(format!("restored_{n}"));
		let (copy, outcome) = restored(&path, &target, true).await;
		outcome.unwrap_or_else(|e| panic!("restoring {n}: {e:?}"));
		assert_eq!(copy.core.seq_num(), metadata.sequence_number, "checkpoint {n}");
		// A full scan resolves every value pointer.
		let state = contents(&copy);
		assert!(
			state.len() >= floor.len() && state.len() <= WRITERS * KEYS,
			"checkpoint {n}: {} keys, {} acknowledged",
			state.len(),
			floor.len()
		);
		for (&(w, i), &version) in &floor {
			let value = state
				.get(&churn_key(w, i))
				.unwrap_or_else(|| panic!("checkpoint {n} lacks key {w}/{i} (floor {version})"));
			let text = String::from_utf8_lossy(&value[..value.len().min(24)]).into_owned();
			let got: usize = text
				.split("_v")
				.nth(1)
				.and_then(|rest| rest.split('_').next())
				.and_then(|v| v.parse().ok())
				.unwrap_or_else(|| panic!("checkpoint {n}: key {w}/{i} holds {text:?}"));
			assert!(
				got >= version && got <= VERSIONS,
				"checkpoint {n}: key {w}/{i} is at version {got}, {version} was acknowledged first"
			);
			assert_eq!(value, &churn_val(w, i, got), "checkpoint {n}: key {w}/{i} is damaged");
		}
		finish(copy).await;
		let _ = std::fs::remove_dir_all(&target);
		let _ = std::fs::remove_dir_all(&path);
	}

	// Not vacuous: value-log files that existed during the run are gone, so the background
	// compaction did collect them while the checkpoints were taken.
	tree.flush().unwrap();
	let remaining: BTreeSet<String> = names_in(dir.path(), "vlog").into_iter().collect();
	let seen = seen_vlog.lock().unwrap().clone();
	let deleted: Vec<_> = seen.difference(&remaining).collect();
	assert!(!deleted.is_empty(), "no value-log file was deleted: seen {seen:?}");
	finish(tree).await;
}

// ---------------------------------------------------------------------------
// 4. two checkpoints into the same directory
// ---------------------------------------------------------------------------

/// Nothing serializes checkpoints that write one directory, so what the directory holds after
/// two of them ran at the same time is the contract: a checkpoint that restores, whole
/// transactions, a prefix of each writer's, bounded by what the writers did around both calls,
/// at the sequence number of one of the two checkpoints. Repeated, with writers running. That the
/// metadata file may belong to the other checkpoint is the subject of the ignored test below.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn two_checkpoints_at_once_into_the_same_directory_leave_one_that_restores_whole() {
	let shape = Shape {
		memtable: 8 * 1024,
		vlog: true,
		level0_max_files: 3,
		writers: 4,
		txns: 150,
		checkpoints: 0,
		checkpointers: 2,
		rotators: 1,
		seed: 0x5EED_0020,
	};
	const ROUNDS: usize = 14;
	let dir = TempDir::new("ckpt_conc_same").unwrap();
	let out = TempDir::new("ckpt_conc_same_out").unwrap();
	let scratch = TempDir::new("ckpt_conc_same_scratch").unwrap();
	let tree = Arc::new(
		Tree::new(options(dir.path(), shape.memtable, shape.vlog, shape.level0_max_files)).unwrap(),
	);
	let progress = Progress::new(shape.writers);
	let failures: Failures = Default::default();
	let stop = Arc::new(AtomicBool::new(false));
	let rotators = spawn_rotators(&tree, shape, &stop);
	let writers = spawn_writers(&tree, shape, &progress, &failures);
	let cp = out.path().join("shared");

	let mut rounds = Vec::new();
	for round in 0..ROUNDS {
		// A round begins once the writers have moved on since the last one.
		let goal = progress.total_acked() + 8;
		let deadline = Instant::now() + REACHED;
		while progress.total_acked() < goal.min(shape.writers * shape.txns)
			&& Instant::now() < deadline
		{
			tokio::time::sleep(Duration::from_millis(1)).await;
		}
		let acked_before = progress.acked();
		let visible_before = tree.core.seq_num();
		let barrier = Arc::new(Barrier::new(2));
		let calls: Vec<_> = (0..2)
			.map(|_| {
				let (tree, cp, barrier) = (Arc::clone(&tree), cp.clone(), Arc::clone(&barrier));
				on_thread(move || {
					barrier.wait();
					tree.create_checkpoint(&cp)
				})
			})
			.collect();
		let results: Vec<_> = calls.into_iter().map(|c| c.wait()).collect();
		let started_after = progress.started();
		let log_seq_after = tree.core.commit_pipeline.log_seq_num.load(Ordering::SeqCst);
		let results: Vec<CheckpointMetadata> = results
			.into_iter()
			.map(|r| r.unwrap_or_else(|e| panic!("round {round}: a checkpoint failed: {e:?}")))
			.collect();

		let target = scratch.path().join(format!("restored_{round}"));
		let (copy, outcome) = restored(&cp, &target, shape.vlog).await;
		let metadata = outcome.unwrap_or_else(|e| {
			panic!(
				"round {round} (seed {}): the shared directory does not restore: {e:?}",
				shape.seed
			)
		});
		let state = contents(&copy);
		for w in 0..shape.writers {
			let m: usize = state
				.get(&marker_key(w))
				.map(|v| String::from_utf8_lossy(v).parse().unwrap())
				.unwrap_or(0);
			for j in 1..=shape.txns {
				for kind in ['a', 'b', 'c'] {
					let found = state.get(&group_key(kind, w, j));
					if j <= m {
						assert_eq!(
							found,
							Some(&group_val(w, j)),
							"round {round} (seed {}): writer {w}'s transaction {j} is torn",
							shape.seed
						);
					} else {
						assert!(
							found.is_none(),
							"round {round} (seed {}): writer {w}'s transaction {j} is above its \
							 marker {m}",
							shape.seed
						);
					}
				}
			}
			assert!(
				acked_before[w] <= m && m <= started_after[w],
				"round {round} (seed {}): writer {w} holds {m}, between {} and {}",
				shape.seed,
				acked_before[w],
				started_after[w]
			);
		}
		let restored_sequence = copy.core.seq_num();
		// The files are one checkpoint's, and the metadata file is one checkpoint's too, not
		// necessarily the same one: the slower writes its own last, see
		// `the_metadata_a_slow_checkpoint_leaves_in_a_shared_directory_describes_its_files`.
		let returned: Vec<u64> = results.iter().map(|m| m.sequence_number).collect();
		assert!(
			returned.contains(&restored_sequence),
			"round {round} (seed {}): the files hold a state at {restored_sequence}, and the two \
			 checkpoints returned {returned:?}",
			shape.seed
		);
		assert!(
			returned.contains(&metadata.sequence_number),
			"round {round} (seed {}): the metadata file reports {}, and the two checkpoints \
			 returned {returned:?}",
			shape.seed,
			metadata.sequence_number
		);
		assert!(
			visible_before <= restored_sequence && restored_sequence < log_seq_after,
			"round {round} (seed {}): sequence {restored_sequence} outside \
			 {visible_before}..{log_seq_after}",
			shape.seed
		);
		finish(copy).await;
		let _ = std::fs::remove_dir_all(&target);
		rounds.push(restored_sequence);
	}

	let finished = tokio::time::timeout(2 * STUCK, async {
		for writer in writers {
			writer.await.unwrap();
		}
	})
	.await;
	stop.store(true, Ordering::SeqCst);
	for rotator in rotators {
		rotator.join().unwrap();
	}
	assert!(finished.is_ok(), "the writers did not finish");
	assert!(failures.lock().unwrap().is_empty(), "failures: {:?}", failures.lock().unwrap());
	assert!(rounds.windows(2).any(|w| w[0] != w[1]), "the writers never moved: {rounds:?}");
	assert!(tree.core.inner.error_handler.check_error().is_ok());
	finish(tree).await;
}

/// What a checkpoint that finishes while another into the same directory is between its manifest
/// and its metadata leaves: the slower one writes its older metadata last, over the files of the
/// newer one. Returns the state of the newer one, both metadata, and the directory.
async fn same_directory_slow_and_fast(
	vlog: bool,
) -> (Contents, CheckpointMetadata, CheckpointMetadata, TempDir, PathBuf) {
	let dir = TempDir::new("ckpt_conc_slow").unwrap();
	let out = TempDir::new("ckpt_conc_slow_out").unwrap();
	let tree = open(dir.path(), vlog);
	let mut state = Contents::new();
	put(&tree, 0..150, 1, &mut state).await;
	let cp = out.path().join("shared");

	let gate = gate_checkpoint(&tree, CheckpointStage::ManifestReleased);
	let slow = checkpoint(&tree, &cp);
	assert_eq!(gate.next(REACHED), Some(0), "the slow checkpoint released the manifest");

	// More commits, and a second checkpoint that runs from start to end meanwhile.
	let mut newer = state.clone();
	put(&tree, 150..300, 2, &mut newer).await;
	remove(&tree, 0..30, &mut newer).await;
	let fast = checkpoint(&tree, &cp).wait().unwrap();
	assert!(
		gate.next(GRACE).is_some(),
		"the second checkpoint did not reach the stage the first is parked at"
	);

	gate.release();
	let slow = slow.wait().unwrap();
	assert!(slow.sequence_number < fast.sequence_number, "{slow:?} {fast:?}");
	finish(tree).await;
	drop(dir);
	(newer, slow, fast, out, cp)
}

/// The directory the slow and the fast checkpoint left restores to the state of the fast one,
/// every value readable, nothing of the older state mixed in.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_slow_checkpoint_over_a_faster_one_into_the_same_directory_leaves_a_whole_state() {
	for vlog in [false, true] {
		let (newer, _slow, _fast, out, cp) = same_directory_slow_and_fast(vlog).await;
		let target = out.path().join("restored");
		let (copy, outcome) = restored(&cp, &target, vlog).await;
		outcome.unwrap();
		assert_contents(&copy, &newer, &format!("vlog {vlog}: the shared directory"));
		finish(copy).await;
	}
}

/// The metadata file of the directory describes the files in it: the slower checkpoint writes its
/// metadata last, and it must not report an older sequence number than the manifest it sits
/// beside.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
#[ignore = "a checkpoint that finishes late into a directory another one already completed writes its older metadata file over the newer files, so the sequence number in CHECKPOINT_METADATA differs from the one of the manifest beside it"]
async fn the_metadata_a_slow_checkpoint_leaves_in_a_shared_directory_describes_its_files() {
	let (_newer, _slow, fast, out, cp) = same_directory_slow_and_fast(false).await;
	let target = out.path().join("restored");
	let (copy, outcome) = restored(&cp, &target, false).await;
	let metadata = outcome.unwrap();
	assert_eq!(copy.core.seq_num(), fast.sequence_number, "the files are the newer checkpoint's");
	assert_eq!(
		metadata.sequence_number, fast.sequence_number,
		"the metadata file describes an older state than the files beside it"
	);
	finish(copy).await;
}

// ---------------------------------------------------------------------------
// 5. interrupted and damaged checkpoint directories
// ---------------------------------------------------------------------------

/// A target database with data of its own, whose files and contents a refused restore must leave
/// as they were.
struct Target {
	tree: Arc<Tree>,
	dir: TempDir,
	state: Contents,
	vlog: bool,
}

async fn target_with_data(vlog: bool) -> Target {
	let dir = TempDir::new("ckpt_conc_target").unwrap();
	let tree = open(dir.path(), vlog);
	let mut state = Contents::new();
	// Keys no checkpoint of these tests holds, some flushed and some only in the memtable and WAL.
	put(&tree, 9000..9120, 9, &mut state).await;
	flush_to_tables(&tree);
	put(&tree, 9120..9200, 9, &mut state).await;
	Target {
		tree,
		dir,
		state,
		vlog,
	}
}

impl Target {
	/// Restores `bad`, which must be refused, and checks the target is as it was: its files byte
	/// for byte, and its contents.
	async fn assert_refused(&mut self, bad: &Path, what: &str) -> Error {
		let files = digest(self.dir.path());
		let error = match restore_call(&self.tree, bad).wait() {
			Ok(metadata) => {
				panic!("{what}: the restore of an unusable directory returned {metadata:?}")
			}
			Err(e) => e,
		};
		let now = digest(self.dir.path());
		assert!(
			differences(&files, &now).is_empty(),
			"{what}: a refused restore changed the target's files: {:?}",
			differences(&files, &now)
		);
		assert_contents(&self.tree, &self.state, &format!("{what}: the target after the refusal"));
		error
	}

	/// A commit after the refusals, and the data across a close and a reopen.
	async fn assert_alive(mut self) {
		put(&self.tree, 9500..9520, 9, &mut self.state).await;
		assert_contents(&self.tree, &self.state, "the target after a commit");
		let opts = options(self.dir.path(), NEVER_ROTATING, self.vlog, 1000);
		tokio::time::timeout(STUCK, self.tree.close()).await.expect("close hung").unwrap();
		let reopened = Tree::new(opts).unwrap();
		assert_contents(&reopened, &self.state, "the target after a reopen");
		tokio::time::timeout(STUCK, reopened.close()).await.expect("close hung").unwrap();
	}
}

/// The source of a checkpoint: a tree with two flushed tables and, with `vlog`, value-log files,
/// and more data in the memtable that the checkpoint flushes into a third.
async fn source_with_data(vlog: bool) -> (Arc<Tree>, TempDir, Contents) {
	let dir = TempDir::new("ckpt_conc_source").unwrap();
	let tree = open(dir.path(), vlog);
	let mut state = Contents::new();
	put(&tree, 0..150, 1, &mut state).await;
	flush_to_tables(&tree);
	put(&tree, 100..250, 2, &mut state).await;
	flush_to_tables(&tree);
	put(&tree, 250..300, 2, &mut state).await;
	(tree, dir, state)
}

/// The directory of a checkpoint at each stage of its creation, copied by the hook on the
/// checkpoint's own thread, as a crash at that instant would leave it.
struct Images {
	at_sstables: PathBuf,
	at_manifest: PathBuf,
	calls: Arc<AtomicUsize>,
}

fn image_hook(tree: &Tree, cp: &Path, into: &Path) -> Images {
	let at_sstables = into.join("at_sstables");
	let at_manifest = into.join("at_manifest");
	let calls = Arc::new(AtomicUsize::new(0));
	let (cp, sst, man, count) =
		(cp.to_path_buf(), at_sstables.clone(), at_manifest.clone(), Arc::clone(&calls));
	*tree.core.inner.checkpoint_hook.lock() = Some(Arc::new(move |stage| match stage {
		CheckpointStage::SstablesCopied => {
			copy_dir(&cp, &sst);
			count.fetch_add(1, Ordering::SeqCst);
		}
		CheckpointStage::ManifestReleased => {
			copy_dir(&cp, &man);
			count.fetch_add(1, Ordering::SeqCst);
		}
		_ => {}
	}));
	Images {
		at_sstables,
		at_manifest,
		calls,
	}
}

/// The checkpoint directory fills in an order: the tables, then the manifest, then the value log,
/// and the metadata file last. Until the metadata file is there restore refuses the directory,
/// and a refused restore leaves the target as it was.
#[test(tokio::test)]
async fn a_checkpoint_directory_without_its_metadata_is_refused_and_the_target_is_untouched() {
	for vlog in [false, true] {
		let (source, _dir, state) = source_with_data(vlog).await;
		let out = TempDir::new("ckpt_conc_images").unwrap();
		let cp = out.path().join("cp");
		let images = image_hook(&source, &cp, out.path());
		let metadata = source.create_checkpoint(&cp).unwrap();
		assert_eq!(images.calls.load(Ordering::SeqCst), 2, "vlog {vlog}: both stages were reached");
		clear_hooks(&source);
		assert_eq!(metadata.sstable_count, 3, "vlog {vlog}");

		// What each image holds: the order the checkpoint writes in.
		let tables = vec![sst_name(1), sst_name(2), sst_name(3)];
		assert_eq!(names_in(&images.at_sstables, "sstables"), tables, "vlog {vlog}");
		assert!(!images.at_sstables.join("manifest").exists(), "vlog {vlog}: manifest first");
		assert!(!images.at_sstables.join("vlog").exists(), "vlog {vlog}: value log first");
		assert!(!images.at_sstables.join("CHECKPOINT_METADATA").exists(), "vlog {vlog}");
		assert_eq!(names_in(&images.at_manifest, "sstables"), tables, "vlog {vlog}");
		assert!(images.at_manifest.join("manifest").exists(), "vlog {vlog}");
		assert!(!images.at_manifest.join("vlog").exists(), "vlog {vlog}: value log early");
		assert!(!images.at_manifest.join("CHECKPOINT_METADATA").exists(), "vlog {vlog}");
		assert!(cp.join("CHECKPOINT_METADATA").exists(), "vlog {vlog}");
		assert_eq!(cp.join("vlog").exists(), vlog, "vlog {vlog}");

		let mut target = target_with_data(vlog).await;
		target.assert_refused(&images.at_sstables, "image at the tables").await;
		target.assert_refused(&images.at_manifest, "image after the manifest").await;

		// The finished checkpoint with its metadata file taken away.
		let without = out.path().join("without");
		copy_dir(&cp, &without);
		std::fs::remove_file(without.join("CHECKPOINT_METADATA")).unwrap();
		target.assert_refused(&without, "complete directory without metadata").await;

		// A directory that does not exist.
		target.assert_refused(&out.path().join("nowhere"), "a missing directory").await;
		target.assert_alive().await;

		// The checkpoint itself was never damaged by any of it, and restores.
		let restore_target = out.path().join("restored");
		let (copy, outcome) = restored(&cp, &restore_target, vlog).await;
		assert_eq!(outcome.unwrap().sequence_number, metadata.sequence_number);
		assert_contents(&copy, &state, &format!("vlog {vlog}: the complete checkpoint"));
		finish(copy).await;
		finish(source).await;
	}
}

/// A metadata file that is cut short, or from a version this build does not know, is refused, and
/// the target keeps its data.
#[test(tokio::test)]
async fn a_torn_or_unsupported_metadata_file_is_refused_and_the_target_is_untouched() {
	let (source, _dir, state) = source_with_data(false).await;
	let out = TempDir::new("ckpt_conc_torn").unwrap();
	let cp = out.path().join("cp");
	let metadata = source.create_checkpoint(&cp).unwrap();
	let bytes = std::fs::read(cp.join("CHECKPOINT_METADATA")).unwrap();
	assert_eq!(bytes.len(), 36);
	let mut target = target_with_data(false).await;

	for len in [0usize, 1, 4, 12, 20, 35] {
		let torn = out.path().join(format!("torn_{len}"));
		copy_dir(&cp, &torn);
		std::fs::write(torn.join("CHECKPOINT_METADATA"), &bytes[..len]).unwrap();
		target.assert_refused(&torn, &format!("metadata cut to {len} bytes")).await;
	}
	for version in [2u32, 3, 999, u32::MAX] {
		let future = out.path().join(format!("version_{version}"));
		copy_dir(&cp, &future);
		let mut changed = bytes.clone();
		changed[..4].copy_from_slice(&version.to_be_bytes());
		std::fs::write(future.join("CHECKPOINT_METADATA"), &changed).unwrap();
		let error = target.assert_refused(&future, &format!("version {version}")).await;
		assert!(
			error.to_string().contains("Unsupported checkpoint version"),
			"version {version}: {error}"
		);
	}
	target.assert_alive().await;

	// The intact one restores into a fresh tree.
	let restore_target = out.path().join("restored");
	let (copy, outcome) = restored(&cp, &restore_target, false).await;
	assert_eq!(outcome.unwrap().sequence_number, metadata.sequence_number);
	assert_contents(&copy, &state, "the intact checkpoint");
	finish(copy).await;
	finish(source).await;
}

/// A metadata file that a power cut left zero-filled (it has its length, and its blocks were never
/// written) describes no checkpoint: restore refuses it, and the target keeps its data.
#[test(tokio::test)]
#[ignore = "restore_from_checkpoint accepts a zero-filled CHECKPOINT_METADATA file as version 0: from_bytes only rejects versions above 1 and restore never calls is_compatible"]
async fn a_zero_filled_metadata_file_is_refused_and_the_target_is_untouched() {
	let (source, _dir, _state) = source_with_data(false).await;
	let out = TempDir::new("ckpt_conc_zeroed").unwrap();
	let cp = out.path().join("cp");
	source.create_checkpoint(&cp).unwrap();
	std::fs::write(cp.join("CHECKPOINT_METADATA"), [0u8; 36]).unwrap();

	let mut target = target_with_data(false).await;
	target.assert_refused(&cp, "a zero-filled metadata file").await;
	target.assert_alive().await;
	finish(source).await;
}

/// A checkpoint whose manifest file was lost in a power cut (a checkpoint syncs nothing, so a
/// file that was never synced may come back empty) is refused by restore without taking the
/// target's data: the restore reads everything it needs before it replaces anything.
#[test(tokio::test)]
#[ignore = "restore_from_checkpoint clears the target's tables, WAL, manifest and value log before it \
            reads the checkpoint's manifest, so a checkpoint with an empty manifest file is refused \
            only after the target database was destroyed"]
async fn a_checkpoint_with_an_empty_manifest_is_refused_and_the_target_is_untouched() {
	let (source, _dir, _state) = source_with_data(false).await;
	let out = TempDir::new("ckpt_conc_manifest").unwrap();
	let cp = out.path().join("cp");
	source.create_checkpoint(&cp).unwrap();
	let lost = out.path().join("lost");
	copy_dir(&cp, &lost);
	let manifest = lost.join("manifest").join(format!("{:020}.manifest", 0));
	assert!(manifest.exists());
	std::fs::OpenOptions::new().write(true).open(&manifest).unwrap().set_len(0).unwrap();

	let mut target = target_with_data(false).await;
	target.assert_refused(&lost, "a checkpoint with an empty manifest").await;
	target.assert_alive().await;
	finish(source).await;
}

/// A checkpoint taken over an older one in the same directory keeps the older metadata file until
/// the new one replaces it, so a crash between the manifest and the metadata leaves a directory
/// that looks finished: the old metadata beside the new manifest and the old value log. Whatever
/// a restore of such an image does, it must not hand out a state that is neither the older
/// checkpoint nor the newer one: it is refused, or every value is there.
#[test(tokio::test)]
#[ignore = "an interrupted checkpoint over an older one in the same directory keeps the old metadata \
            file, so restore accepts a manifest that points into value-log files the interrupted \
            checkpoint did not copy yet"]
async fn an_interrupted_checkpoint_over_an_older_one_is_refused_or_restores_a_whole_state() {
	let dir = TempDir::new("ckpt_conc_reuse_crash").unwrap();
	let out = TempDir::new("ckpt_conc_reuse_crash_out").unwrap();
	let tree = open(dir.path(), true);
	let mut older = Contents::new();
	put(&tree, 0..120, 1, &mut older).await;
	flush_to_tables(&tree);
	let cp = out.path().join("cp");
	tree.create_checkpoint(&cp).unwrap();

	// New values for every key and some deletes, merged, so the value-log files of the first
	// round are obsolete and the second round has files of its own.
	let mut newer = older.clone();
	put(&tree, 0..120, 2, &mut newer).await;
	remove(&tree, 0..20, &mut newer).await;
	flush_to_tables(&tree);
	let strategy = merging_strategy(dir.path(), true);
	tree.compact(strategy).unwrap();
	assert!(!table_ids(&tree).contains(&1), "the tables were merged");

	let images = image_hook(&tree, &cp, out.path());
	tree.create_checkpoint(&cp).unwrap();
	assert_eq!(images.calls.load(Ordering::SeqCst), 2);
	clear_hooks(&tree);
	assert!(
		images.at_manifest.join("CHECKPOINT_METADATA").exists(),
		"the older metadata file is still there at the second stage"
	);

	for (name, image) in
		[("after_the_tables", &images.at_sstables), ("after_the_manifest", &images.at_manifest)]
	{
		let target = out.path().join(format!("restored_{name}"));
		let (copy, outcome) = restored(image, &target, true).await;
		if outcome.is_ok() {
			let state = catch_unwind(AssertUnwindSafe(|| contents(&copy)));
			match state {
				Ok(state) => assert!(
					state == older || state == newer,
					"image {name}: restore accepted a directory that is neither the older \
					 checkpoint nor the newer one: {} keys",
					state.len()
				),
				Err(_) => panic!(
					"image {name}: restore accepted the directory, and its values cannot be read"
				),
			}
		}
		finish(copy).await;
	}
	finish(tree).await;
}

// ---------------------------------------------------------------------------
// 6. hard links
// ---------------------------------------------------------------------------

/// A table that a checkpoint links is the live file under a second name. The live tree compacts
/// it away and deletes it; the checkpoint still holds the table, unchanged, and restores to the
/// state it had.
#[test(tokio::test)]
async fn a_checkpoint_stays_whole_after_the_live_tree_deletes_the_tables_it_links() {
	for vlog in [false, true] {
		let dir = TempDir::new("ckpt_conc_links").unwrap();
		let out = TempDir::new("ckpt_conc_links_out").unwrap();
		let tree = open(dir.path(), vlog);
		let mut state = Contents::new();
		put(&tree, 0..100, 1, &mut state).await;
		flush_to_tables(&tree);
		put(&tree, 50..150, 2, &mut state).await;
		flush_to_tables(&tree);
		remove(&tree, 0..20, &mut state).await;
		flush_to_tables(&tree);
		put(&tree, 200..250, 3, &mut state).await;
		flush_to_tables(&tree);
		let tables: Vec<u64> = table_ids(&tree);
		assert_eq!(tables, vec![1, 2, 3, 4]);

		let cp = out.path().join("cp");
		let metadata = tree.create_checkpoint(&cp).unwrap();
		assert_eq!(metadata.sstable_count, 4);
		#[cfg(unix)]
		for id in &tables {
			let live = inode_and_links(&dir.path().join("sstables").join(sst_name(*id)));
			let linked = inode_and_links(&cp.join("sstables").join(sst_name(*id)));
			assert_eq!(live.0, linked.0, "vlog {vlog}: table {id} is a copy, not a link");
			assert_eq!(live.1, 2, "vlog {vlog}: table {id} has {} names", live.1);
		}
		let before = digest(&cp);

		// The live tree merges every table and deletes the inputs.
		let strategy = merging_strategy(dir.path(), vlog);
		for _ in 0..4 {
			tree.compact(Arc::clone(&strategy)).unwrap();
		}
		let live = table_ids(&tree);
		assert!(
			tables.iter().all(|id| !live.contains(id)),
			"vlog {vlog}: the live tree still lists a table of the checkpoint: {live:?}"
		);
		for id in &tables {
			assert!(
				!dir.path().join("sstables").join(sst_name(*id)).exists(),
				"vlog {vlog}: the live tree still holds table {id}"
			);
			assert!(cp.join("sstables").join(sst_name(*id)).exists(), "vlog {vlog}: table {id}");
			#[cfg(unix)]
			assert_eq!(
				inode_and_links(&cp.join("sstables").join(sst_name(*id))).1,
				1,
				"vlog {vlog}: table {id} should be the checkpoint's alone now"
			);
		}
		assert_eq!(differences(&before, &digest(&cp)), Vec::<String>::new(), "vlog {vlog}");
		assert_contents(&tree, &state, "the live tree after the compactions");

		let target = out.path().join("restored");
		let (copy, outcome) = restored(&cp, &target, vlog).await;
		assert_eq!(outcome.unwrap().sequence_number, metadata.sequence_number);
		assert_contents(&copy, &state, &format!("vlog {vlog}: the checkpoint"));
		finish(copy).await;
		finish(tree).await;
	}
}

/// A checkpoint removes the name of a table in its directory and links it again. A name that
/// exists by then was linked by another checkpoint into the same directory, to the same live
/// table, and is left alone: copying over it would truncate the live table. Threads that link the
/// names as soon as they are free play the other checkpoint, in rounds until one of them has
/// won a name from the checkpoint; the live tables must come out of every round at the size they
/// had, and the directory must restore.
#[cfg(target_os = "linux")]
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_name_another_checkpoint_linked_meanwhile_is_not_copied_over_the_live_table() {
	const TABLES: usize = 120;
	const ROUNDS: usize = 12;
	let dir = TempDir::new("ckpt_conc_race").unwrap();
	let out = TempDir::new("ckpt_conc_race_out").unwrap();
	let tree = open(dir.path(), false);
	let mut state = Contents::new();
	for i in 0..TABLES {
		put(&tree, i * 5..i * 5 + 5, 1, &mut state).await;
		flush_to_tables(&tree);
	}
	let ids = table_ids(&tree);
	assert_eq!(ids.len(), TABLES);
	let live_dir = dir.path().join("sstables");
	let sizes: Vec<u64> = ids
		.iter()
		.map(|id| std::fs::metadata(live_dir.join(sst_name(*id))).unwrap().len())
		.collect();
	assert!(sizes.iter().all(|size| *size > 0));

	let cp = out.path().join("cp");
	let mut won = 0;
	for round in 0..ROUNDS {
		let cp_tables = cp.join("sstables");
		let _ = std::fs::remove_dir_all(&cp_tables);
		std::fs::create_dir_all(&cp_tables).unwrap();
		let stop = Arc::new(AtomicBool::new(false));
		let links = Arc::new(AtomicUsize::new(0));
		let pollers: Vec<_> = (0..2u64)
			.map(|half| {
				let (stop, links) = (Arc::clone(&stop), Arc::clone(&links));
				let (live_dir, cp_tables) = (live_dir.clone(), cp_tables.clone());
				let mine: Vec<u64> = ids.iter().copied().filter(|id| id % 2 == half).collect();
				std::thread::spawn(move || {
					while !stop.load(Ordering::SeqCst) {
						for id in &mine {
							let name = sst_name(*id);
							if std::fs::hard_link(live_dir.join(&name), cp_tables.join(&name))
								.is_ok()
							{
								links.fetch_add(1, Ordering::SeqCst);
							}
						}
					}
				})
			})
			.collect();
		// The directory holds a link to every table before the checkpoint begins.
		let until = Instant::now() + REACHED;
		while links.load(Ordering::SeqCst) < TABLES && Instant::now() < until {
			std::thread::sleep(Duration::from_millis(1));
		}
		assert_eq!(
			links.load(Ordering::SeqCst),
			TABLES,
			"round {round}: the names were not linked"
		);

		let outcome = checkpoint(&tree, &cp).wait();
		stop.store(true, Ordering::SeqCst);
		for poller in pollers {
			poller.join().unwrap();
		}
		// A link beyond the first of each name is one made after the checkpoint removed the name
		// and before it linked it.
		won += links.load(Ordering::SeqCst) - TABLES;
		let metadata =
			outcome.unwrap_or_else(|e| panic!("round {round}: the checkpoint failed: {e:?}"));
		assert_eq!(metadata.sstable_count, TABLES, "round {round}");
		for (id, size) in ids.iter().zip(&sizes) {
			let now = std::fs::metadata(live_dir.join(sst_name(*id))).unwrap().len();
			assert_eq!(now, *size, "round {round}: the live table {id} changed its size");
		}
		if won > 0 {
			break;
		}
	}
	assert!(won > 0, "no name was linked while the checkpoint had it free, so nothing was tested");

	let target = out.path().join("restored");
	let (copy, outcome) = restored(&cp, &target, false).await;
	outcome.unwrap();
	assert_contents(&copy, &state, "the checkpoint");
	assert_contents(&tree, &state, "the live tree");
	finish(copy).await;
	finish(tree).await;
}

/// A tree restored from a checkpoint hard-links its tables. It then writes, deletes and
/// compacts them away, and a second restore of the same checkpoint, into a fresh tree and into
/// the tree that wrote, yields exactly the checkpoint's content, from files that did not change
/// by a byte.
#[test(tokio::test)]
async fn restoring_a_checkpoint_again_after_the_first_target_compacted_yields_the_original() {
	for vlog in [false, true] {
		let out = TempDir::new("ckpt_conc_twice").unwrap();
		let (source, source_dir, state) = source_with_data(vlog).await;
		let cp = out.path().join("cp");
		let metadata = source.create_checkpoint(&cp).unwrap();
		let sealed = digest(&cp);
		let original_tables = names_in(&cp, "sstables");
		assert!(!original_tables.is_empty());

		// The first target restores it and then moves on.
		let first_dir = out.path().join("first");
		let (first, outcome) = restored(&cp, &first_dir, vlog).await;
		assert_eq!(outcome.unwrap().sequence_number, metadata.sequence_number);
		assert_contents(&first, &state, "the first restore");
		let restored_ids = table_ids(&first);
		#[cfg(unix)]
		for name in &original_tables {
			assert_eq!(
				inode_and_links(&cp.join("sstables").join(name)).0,
				inode_and_links(&first_dir.join("sstables").join(name)).0,
				"vlog {vlog}: {name} was copied, so the check below proves nothing about links"
			);
		}
		let mut moved_on = state.clone();
		put(&first, 1000..1100, 5, &mut moved_on).await;
		remove(&first, 100..160, &mut moved_on).await;
		put(&first, 140..200, 6, &mut moved_on).await;
		flush_to_tables(&first);
		put(&first, 1100..1140, 5, &mut moved_on).await;
		flush_to_tables(&first);
		let strategy = merging_strategy(&first_dir, vlog);
		for _ in 0..6 {
			first.compact(Arc::clone(&strategy)).unwrap();
		}
		let now = table_ids(&first);
		assert!(
			restored_ids.iter().all(|id| !now.contains(id)),
			"vlog {vlog}: the first target still lists a restored table: {now:?} of {restored_ids:?}"
		);
		for name in &original_tables {
			assert!(
				!first_dir.join("sstables").join(name).exists(),
				"vlog {vlog}: the first target still holds {name}"
			);
		}
		assert_contents(&first, &moved_on, "the first target after it moved on");
		assert_eq!(
			differences(&sealed, &digest(&cp)),
			Vec::<String>::new(),
			"vlog {vlog}: the checkpoint changed while its first target moved on"
		);

		// A second restore into a fresh tree.
		let second_dir = out.path().join("second");
		let (second, outcome) = restored(&cp, &second_dir, vlog).await;
		assert_eq!(outcome.unwrap().sequence_number, metadata.sequence_number);
		assert_contents(&second, &state, &format!("vlog {vlog}: the second restore"));
		finish(second).await;

		// And over the tree that wrote.
		assert_eq!(
			restore_call(&first, &cp).wait().unwrap().sequence_number,
			metadata.sequence_number
		);
		assert_contents(&first, &state, &format!("vlog {vlog}: the first target restored again"));
		assert_eq!(
			names_in(&first_dir, "sstables"),
			original_tables,
			"vlog {vlog}: the restore left tables of the state it replaced beside the checkpoint's"
		);
		assert_eq!(
			differences(&sealed, &digest(&cp)),
			Vec::<String>::new(),
			"vlog {vlog}: the checkpoint changed by being restored again"
		);
		finish(first).await;
		finish(source).await;
		drop(source_dir);
	}
}

// ---------------------------------------------------------------------------
// 7. a checkpoint into a directory that holds an older one
// ---------------------------------------------------------------------------

/// A second checkpoint into the directory of a first one, after deletes, overwrites and a
/// compaction, restores to exactly the newer state: nothing the first checkpoint held that the
/// tree has since deleted or replaced comes back.
#[test(tokio::test)]
async fn a_checkpoint_over_an_older_one_in_the_same_directory_is_exactly_the_newer_state() {
	for vlog in [false, true] {
		let dir = TempDir::new("ckpt_conc_reuse").unwrap();
		let out = TempDir::new("ckpt_conc_reuse_out").unwrap();
		let tree = open(dir.path(), vlog);
		let mut state = Contents::new();
		put(&tree, 0..200, 1, &mut state).await;
		flush_to_tables(&tree);
		put(&tree, 150..250, 2, &mut state).await;
		flush_to_tables(&tree);
		let cp = out.path().join("cp");
		let first = tree.create_checkpoint(&cp).unwrap();
		let first_state = state.clone();
		assert_eq!(manifest_tables_of(&cp), BTreeSet::from([1, 2]));

		// Deletes, overwrites and a compaction, which removes the first checkpoint's tables
		// from the live tree.
		remove(&tree, 0..60, &mut state).await;
		put(&tree, 200..300, 3, &mut state).await;
		flush_to_tables(&tree);
		let strategy = merging_strategy(dir.path(), vlog);
		for _ in 0..4 {
			tree.compact(Arc::clone(&strategy)).unwrap();
		}
		assert!(!table_ids(&tree).contains(&1), "vlog {vlog}: the tables were merged");

		let second = tree.create_checkpoint(&cp).unwrap();
		assert!(second.sequence_number > first.sequence_number);
		assert_eq!(second.sstable_count, table_ids(&tree).len(), "vlog {vlog}");
		let listed = manifest_tables_of(&cp);
		assert_eq!(
			listed,
			table_ids(&tree).into_iter().collect::<BTreeSet<_>>(),
			"vlog {vlog}: the manifest of the reused directory is the live tree's"
		);
		for id in &listed {
			assert!(cp.join("sstables").join(sst_name(*id)).exists(), "vlog {vlog}: table {id}");
		}

		let target = out.path().join("restored");
		let (copy, outcome) = restored(&cp, &target, vlog).await;
		assert_eq!(outcome.unwrap().sequence_number, second.sequence_number);
		assert_eq!(copy.core.seq_num(), second.sequence_number, "vlog {vlog}");
		assert_contents(&copy, &state, &format!("vlog {vlog}: the reused directory"));
		let held = contents(&copy);
		for i in 0..60 {
			assert!(
				!held.contains_key(&key_of(i)),
				"vlog {vlog}: key {i}, deleted since the first checkpoint, is back"
			);
		}
		assert_ne!(held, first_state, "vlog {vlog}");
		finish(copy).await;
		finish(tree).await;
	}
}

/// Two histories that reuse a table id: a checkpoint of the first lies in a directory, with
/// tables 1 and 2, and a tree restored from an earlier point of the same database writes its own
/// table 2 with other keys. The checkpoint of that tree into the same directory is exactly its
/// own state: the table 2 left by the other history is replaced, not kept.
#[test(tokio::test)]
async fn a_checkpoint_into_a_directory_that_holds_another_history_with_the_same_table_ids() {
	for vlog in [false, true] {
		let dir = TempDir::new("ckpt_conc_ids").unwrap();
		let out = TempDir::new("ckpt_conc_ids_out").unwrap();
		let tree = open(dir.path(), vlog);
		let mut base = Contents::new();
		put(&tree, 0..100, 1, &mut base).await;
		flush_to_tables(&tree);
		let early = out.path().join("early");
		tree.create_checkpoint(&early).unwrap();

		// The first history goes on: table 2 holds keys 100..200.
		let mut first_history = base.clone();
		put(&tree, 100..200, 1, &mut first_history).await;
		flush_to_tables(&tree);
		assert_eq!(table_ids(&tree), vec![1, 2]);
		let shared = out.path().join("shared");
		tree.create_checkpoint(&shared).unwrap();
		assert_eq!(manifest_tables_of(&shared), BTreeSet::from([1, 2]));
		let first_table = std::fs::read(shared.join("sstables").join(sst_name(2))).unwrap();

		// The second history restores the early checkpoint, and its table 2 holds other keys.
		let other_dir = out.path().join("other");
		let (other, outcome) = restored(&early, &other_dir, vlog).await;
		outcome.unwrap();
		let mut second_history = base.clone();
		put(&other, 300..400, 4, &mut second_history).await;
		remove(&other, 0..10, &mut second_history).await;
		flush_to_tables(&other);
		assert_eq!(table_ids(&other), vec![1, 2], "vlog {vlog}: the second history reuses id 2");
		let metadata = checkpoint(&other, &shared).wait().unwrap();
		assert_ne!(
			std::fs::read(shared.join("sstables").join(sst_name(2))).unwrap(),
			first_table,
			"vlog {vlog}: the directory kept the table of the other history under the same name"
		);

		let target = out.path().join("restored");
		let (copy, outcome) = restored(&shared, &target, vlog).await;
		assert_eq!(outcome.unwrap().sequence_number, metadata.sequence_number);
		assert_contents(&copy, &second_history, &format!("vlog {vlog}: the shared directory"));
		finish(copy).await;
		finish(other).await;
		finish(tree).await;
	}
}
