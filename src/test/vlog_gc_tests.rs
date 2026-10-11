//! Garbage collection of the value log: a file is deleted once no table and no memtable can
//! point into it, and not before, whatever reads, checkpoints and crashes are around it.
//!
//! The expectation of every test is computed from the data and not from the cleanup: the
//! pointers of every table and every memtable are read from the entries themselves, and a
//! cleanup must leave exactly the files at or above the smallest of them, and the active file.
//! The reads that decide whether a file was deleted too early go to the disk: the tests use a
//! block cache too small to hold a value, and a freshly opened tree where they can.
//!
//! The groups, in the order of the file:
//!
//! 1. liveness: churn over the oldest keys, memtables that no table covers, and the bound of
//!    `VLog::cleanup_obsolete_files` itself (its boundary, the active file, the pins);
//! 2. snapshots and open iterators keep the versions they can see, and the files, until they are
//!    dropped;
//! 3. readers and scans race writers, flushes, compactions and the cleanups they trigger;
//! 4. a checkpoint pins the files from the moment it holds the manifest until its copy is done;
//! 5. crash images at every stage of a compaction, after a flush, and with files no record points
//!    to;
//! 6. file ids only grow across reopens, the active file survives every cleanup, and one cleanup
//!    removes many files;
//! 7. a missing or truncated file makes the reads that need it fail, and only those;
//! 8. the log is bounded by a small multiple of the live data under churn.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;

use super::collect_transaction_all;
use super::wal_group_budget_tests::{mark_closed, release_lock};
use crate::checkpoint::{CheckpointMetadata, CheckpointStage};
use crate::compaction::compactor::CompactionStage;
use crate::compaction::leveled::Strategy;
use crate::lsm::cleanup_vlog;
use crate::vfs::sync_tracker;
use crate::vlog::{VLog, ValueLocation, ValuePointer, SYNCED_VLOG_LENGTHS};
use crate::{Durability, LSMIterator, Mode, Options, Transaction, Tree, VLogChecksumLevel};

/// How long a test waits for something it expects to happen.
const LONG: Duration = Duration::from_secs(30);

/// How long a test waits for a call that is expected to return before it calls it a deadlock.
const STUCK: Duration = Duration::from_secs(60);

/// How long a test gives a call that must stay blocked to return. Returning takes microseconds,
/// so waiting this long without a return means it is excluded.
const GRACE: Duration = Duration::from_millis(500);

/// Large enough that nothing rotates unless a test rotates it.
const NEVER_ROTATING: usize = 16 * 1024 * 1024;

/// The size of the values of `big`: four of them fill a file of `vlog_max_file_size` 4096.
const VALUE_LEN: usize = 1000;

type Contents = BTreeMap<Vec<u8>, Vec<u8>>;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Options with a small value log: values above 64 bytes go to it, four of the values of `big` to
/// a file. No background compaction: the tests compact when they want to. The block cache is too
/// small to hold a value, so a read of a value goes to the file.
fn base_options(path: &Path) -> Options {
	Options {
		path: path.to_path_buf(),
		max_memtable_size: NEVER_ROTATING,
		enable_vlog: true,
		vlog_value_threshold: 64,
		vlog_max_file_size: 4096,
		level0_max_files: 100,
		l0_stall_threshold: 1000,
		memtable_stall_threshold: 1000,
		// A reopen after a close then replays the WAL instead of finding everything in tables.
		flush_on_close: false,
		block_cache: Arc::new(crate::cache::BlockCache::with_capacity_bytes(256)),
		..Default::default()
	}
}

fn open_with(opts: Options) -> Arc<Tree> {
	Arc::new(Tree::new(Arc::new(opts)).unwrap())
}

async fn stop_background_tasks(tree: &Tree) {
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
}

/// A tree whose background tasks are stopped: nothing is flushed or compacted unless the test
/// does it.
async fn quiet_tree_with(opts: Options) -> Arc<Tree> {
	let tree = open_with(opts);
	stop_background_tasks(&tree).await;
	tree
}

async fn quiet_tree(path: &Path) -> Arc<Tree> {
	quiet_tree_with(base_options(path)).await
}

/// Ends a tree the way a crash does: it is not closed, and its tasks and its lock are gone.
fn crash(tree: &Tree) {
	mark_closed(tree);
	release_lock(tree);
}

fn key_of(i: usize) -> Vec<u8> {
	format!("key_{i:04}").into_bytes()
}

/// A value that goes to the value log, and that no other `(i, round)` shares.
fn big(i: usize, round: usize) -> Vec<u8> {
	let mut v = format!("big_{i:04}_{round:04}_").into_bytes();
	v.resize(VALUE_LEN, b'x');
	v
}

async fn commit(
	tree: &Tree,
	entries: &[(Vec<u8>, Vec<u8>)],
	durability: Durability,
) -> crate::Result<()> {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(durability);
	for (key, value) in entries {
		txn.set(key, value).unwrap();
	}
	txn.commit().await
}

/// Commits `keys` with a value in the value log each, in one batch.
async fn put_with(
	tree: &Tree,
	keys: Range<usize>,
	round: usize,
	durability: Durability,
) -> Contents {
	let entries: Vec<_> = keys.map(|i| (key_of(i), big(i, round))).collect();
	commit(tree, &entries, durability).await.unwrap();
	entries.into_iter().collect()
}

async fn put(tree: &Tree, keys: Range<usize>, round: usize) -> Contents {
	put_with(tree, keys, round, Durability::Eventual).await
}

async fn put_durable(tree: &Tree, keys: Range<usize>, round: usize) -> Contents {
	put_with(tree, keys, round, Durability::Immediate).await
}

async fn delete(tree: &Tree, keys: Range<usize>) {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(Durability::Immediate);
	for i in keys {
		txn.delete(key_of(i)).unwrap();
	}
	txn.commit().await.unwrap();
}

/// What the tree must hold: the keys with their values, and the keys that were deleted.
#[derive(Default, Clone)]
struct Model {
	live: Contents,
	gone: BTreeSet<Vec<u8>>,
}

impl Model {
	fn put(&mut self, written: Contents) {
		for (key, value) in written {
			self.gone.remove(&key);
			self.live.insert(key, value);
		}
	}

	fn delete(&mut self, keys: Range<usize>) {
		for i in keys {
			self.live.remove(&key_of(i));
			self.gone.insert(key_of(i));
		}
	}
}

/// Reads every key of the model one by one and scans the tree, and fails with what is wrong.
fn assert_model(tree: &Tree, model: &Model, what: &str) {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let mut wrong = Vec::new();
	for (key, value) in &model.live {
		let name = String::from_utf8_lossy(key);
		match txn.get(key) {
			Ok(Some(got)) if &got == value => {}
			Ok(Some(got)) => {
				wrong.push(format!("{name}: {} bytes, not the value written", got.len()))
			}
			Ok(None) => wrong.push(format!("{name}: missing")),
			Err(e) => wrong.push(format!("{name}: {e}")),
		}
	}
	for key in &model.gone {
		match txn.get(key) {
			Ok(None) => {}
			other => wrong.push(format!(
				"{}: deleted, but reads as {:?}",
				String::from_utf8_lossy(key),
				other.map(|v| v.map(|v| v.len()))
			)),
		}
	}
	match txn.iter().and_then(|mut it| collect_transaction_all(&mut it)) {
		Ok(all) => {
			let got: Contents = all.into_iter().collect();
			if got != model.live {
				wrong.push(format!(
					"a scan holds {} keys, expected {}",
					got.len(),
					model.live.len()
				));
			}
		}
		Err(e) => wrong.push(format!("a scan fails: {e}")),
	}
	assert!(
		wrong.is_empty(),
		"{what}: {} problems, for example {:?}",
		wrong.len(),
		&wrong[..wrong.len().min(3)]
	);
}

/// The ids of the value-log files in `path`, in order.
fn vlog_files(path: &Path) -> Vec<u32> {
	let mut ids: Vec<u32> = std::fs::read_dir(path.join("vlog"))
		.unwrap()
		.filter_map(|e| {
			let name = e.unwrap().file_name().to_string_lossy().into_owned();
			name.strip_suffix(".vlog").and_then(|id| id.parse().ok())
		})
		.collect();
	ids.sort_unstable();
	ids
}

fn vlog_file_path(path: &Path, id: u32) -> PathBuf {
	path.join("vlog").join(format!("{id:020}.vlog"))
}

/// The bytes the value-log files of `path` take.
fn vlog_bytes(path: &Path) -> u64 {
	vlog_files(path)
		.iter()
		.map(|&id| std::fs::metadata(vlog_file_path(path, id)).unwrap().len())
		.sum()
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

fn active_file(tree: &Tree) -> u32 {
	tree.core.inner.vlog.as_ref().unwrap().active_writer_id.load(Ordering::SeqCst)
}

/// Rotates the active memtable into the queue and flushes the queue.
fn flush_to_tables(tree: &Tree) {
	tree.core.inner.rotate_memtable().unwrap();
	tree.core.inner.flush_all_immutables_sync().unwrap();
}

fn l0_tables(tree: &Tree) -> usize {
	tree.core.inner.l0_file_count()
}

/// Merges the tables of level 0, which the tree itself would not do until there are many more.
fn compact(tree: &Tree) {
	assert!(l0_tables(tree) >= 2, "a compaction needs two tables in level 0");
	let opts = Arc::new(Options {
		level0_max_files: 2,
		..Default::default()
	});
	tree.compact(Arc::new(Strategy::from_options(opts))).unwrap();
	assert_eq!(l0_tables(tree), 0, "the compaction merged level 0");
}

/// What a cleanup does, without a compaction or a flush around it.
fn cleanup(tree: &Tree) {
	let inner = &tree.core.inner;
	cleanup_vlog(
		&inner.vlog,
		&inner.active_memtable,
		&inner.level_manifest,
		&inner.immutable_memtables,
		"test",
	);
}

/// Commits `keys` as `round`, and flushes them into a table of their own.
async fn put_and_flush(tree: &Tree, model: &mut Model, keys: Range<usize>, round: usize) {
	model.put(put(tree, keys, round).await);
	flush_to_tables(tree);
}

// ---------------------------------------------------------------------------
// what the data points to
// ---------------------------------------------------------------------------

/// The value pointers of every entry the iterator holds, whatever version of its key it is.
fn collect_pointers(iter: &mut impl LSMIterator, out: &mut Vec<(Vec<u8>, ValuePointer)>) {
	if !iter.seek_first().unwrap() {
		return;
	}
	loop {
		if let Some(payload) = ValueLocation::peek_pointer_payload(iter.value_encoded().unwrap()) {
			out.push((iter.key().user_key().to_vec(), ValuePointer::decode(payload).unwrap()));
		}
		if !iter.next().unwrap() {
			break;
		}
	}
}

/// Every pointer of every table and every memtable, with the key it belongs to, read from the
/// entries. Nothing the cleanup computes is used.
fn referenced(tree: &Tree) -> Vec<(Vec<u8>, ValuePointer)> {
	let inner = &tree.core.inner;
	let mut out = Vec::new();
	{
		let manifest = inner.level_manifest.read().unwrap();
		for table in manifest.iter() {
			collect_pointers(&mut table.iter(None).unwrap(), &mut out);
		}
	}
	collect_pointers(&mut inner.active_memtable.read().unwrap().iter(), &mut out);
	for entry in inner.immutable_memtables.read().unwrap().iter() {
		collect_pointers(&mut entry.memtable.iter(), &mut out);
	}
	out
}

fn referenced_files(tree: &Tree) -> BTreeSet<u32> {
	referenced(tree).into_iter().map(|(_, pointer)| pointer.file_id).collect()
}

/// The oldest file that anything points into.
fn oldest_pointer(tree: &Tree) -> u32 {
	*referenced_files(tree).iter().next().expect("some value points into the value log")
}

/// Every pointer resolves to its value: no file a table or a memtable points into is missing or
/// short, however old the version it belongs to is.
fn assert_all_pointers_resolve(tree: &Tree, what: &str) -> usize {
	let vlog = tree.core.inner.vlog.as_ref().unwrap();
	let pointers = referenced(tree);
	let bad: Vec<String> = pointers
		.iter()
		.filter_map(|(key, pointer)| {
			vlog.get(pointer).err().map(|e| {
				format!("{} (file {}): {e}", String::from_utf8_lossy(key), pointer.file_id)
			})
		})
		.collect();
	assert!(
		bad.is_empty(),
		"{what}: {} of {} pointers do not resolve, for example {:?}",
		bad.len(),
		pointers.len(),
		&bad[..bad.len().min(3)]
	);
	pointers.len()
}

/// The files of the value-log directory are those that something can read and the active one, and
/// nothing below the oldest pointer is left. Returns the oldest pointer's file.
fn assert_collected(tree: &Tree, dir: &Path, what: &str) -> u32 {
	let files = vlog_files(dir);
	let floor = oldest_pointer(tree);
	let active = active_file(tree);
	for file in referenced_files(tree) {
		assert!(files.contains(&file), "{what}: file {file} is pointed into and gone: {files:?}");
	}
	let left: Vec<_> = files.iter().filter(|&&f| f < floor && f != active).collect();
	assert!(left.is_empty(), "{what}: files {left:?} are below the oldest pointer ({floor})");
	floor
}

/// `before` was the listing of the directory right before a cleanup: it must now hold exactly the
/// files at or above the oldest pointer, and the active file. Returns the oldest pointer's file.
fn assert_gc_state(tree: &Tree, dir: &Path, before: &[u32], what: &str) -> u32 {
	let floor = assert_collected(tree, dir, what);
	let active = active_file(tree);
	let expected: Vec<u32> =
		before.iter().copied().filter(|&f| f >= floor || f == active).collect();
	assert_eq!(
		vlog_files(dir),
		expected,
		"{what}: the files left are those at or above {floor} (oldest pointer) and the active \
		 file {active}; before: {before:?}"
	);
	floor
}

/// Compares two sets of contents and fails with where they first differ, not with the values: a
/// failed `assert_eq!` of two maps of kilobyte values prints every byte.
fn assert_same_contents(got: &Contents, want: &Contents, what: &str) {
	let first = want
		.iter()
		.find(|(key, value)| got.get(*key) != Some(*value))
		.map(|(key, _)| String::from_utf8_lossy(key).into_owned());
	assert!(
		got == want,
		"{what}: {} keys instead of {}, the first difference is at {first:?}",
		got.len(),
		want.len()
	);
}

/// The value-log files of the database in `dir` that this process has deleted and still holds
/// open: a file is only given back to the disk once the last descriptor is closed, so a cleanup
/// that leaves one in a cache has not freed its space. The descriptors are listed in `/proc`,
/// which only Linux has.
#[cfg(target_os = "linux")]
fn deleted_files_held_open(dir: &Path) -> Vec<String> {
	let dir = dir.join("vlog").canonicalize().unwrap().to_string_lossy().into_owned();
	std::fs::read_dir("/proc/self/fd")
		.unwrap()
		.filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
		.map(|target| target.to_string_lossy().into_owned())
		.filter(|target| target.starts_with(&dir) && target.ends_with(" (deleted)"))
		.collect()
}

#[cfg(not(target_os = "linux"))]
fn deleted_files_held_open(_dir: &Path) -> Vec<String> {
	Vec::new()
}

fn assert_no_deleted_file_held_open(dir: &Path, what: &str) {
	let held = deleted_files_held_open(dir);
	assert!(held.is_empty(), "{what}: deleted files are still held open: {held:?}");
}

/// The files of the `sstables` directory are the tables the manifest lists.
fn assert_tables_match_manifest(tree: &Tree, dir: &Path, what: &str) {
	let mut listed: Vec<u64> =
		tree.core.inner.level_manifest.read().unwrap().iter().map(|t| t.id).collect();
	listed.sort_unstable();
	let mut on_disk: Vec<u64> = std::fs::read_dir(dir.join("sstables"))
		.unwrap()
		.filter_map(|e| {
			let name = e.unwrap().file_name().to_string_lossy().into_owned();
			name.strip_suffix(".sst").and_then(|id| id.parse().ok())
		})
		.collect();
	on_disk.sort_unstable();
	assert_eq!(on_disk, listed, "{what}: the table files are the tables of the manifest");
}

// ---------------------------------------------------------------------------
// calls on threads of their own
// ---------------------------------------------------------------------------

/// A call on a thread of its own. One that never returns fails its test after `STUCK`.
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

	/// Waits for the call to return, and fails if it does not within `LONG`.
	fn wait_finished(&self, what: &str) {
		let deadline = Instant::now() + LONG;
		while !self.is_finished() {
			assert!(Instant::now() < deadline, "timed out waiting for {what}");
			std::thread::yield_now();
		}
	}

	/// Fails if the call returns within `GRACE`.
	fn assert_blocked(&self, what: &str) {
		let deadline = Instant::now() + GRACE;
		while Instant::now() < deadline {
			assert!(!self.is_finished(), "{what}");
			std::thread::yield_now();
		}
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

fn checkpoint(tree: &Arc<Tree>, path: &Path) -> Call<crate::Result<CheckpointMetadata>> {
	let (tree, path) = (Arc::clone(tree), path.to_path_buf());
	on_thread(move || tree.create_checkpoint(&path))
}

/// Parks the checkpoint at the stages it is given, and reports each arrival.
struct Park {
	reached: mpsc::Receiver<CheckpointStage>,
	release: mpsc::Sender<()>,
	fired: Arc<AtomicUsize>,
}

impl Park {
	fn install(tree: &Tree, stages: &'static [CheckpointStage]) -> Park {
		let (reached_tx, reached) = mpsc::channel();
		let (release, release_rx) = mpsc::channel();
		let reached_tx = Mutex::new(reached_tx);
		let release_rx = Mutex::new(release_rx);
		let fired = Arc::new(AtomicUsize::new(0));
		let count = Arc::clone(&fired);
		let hook: crate::checkpoint::CheckpointHook = Arc::new(move |stage| {
			if stages.contains(&stage) {
				count.fetch_add(1, Ordering::SeqCst);
				reached_tx.lock().unwrap().send(stage).unwrap();
				// Bounded, so a broken test fails instead of hanging.
				release_rx
					.lock()
					.unwrap()
					.recv_timeout(STUCK)
					.expect("the test never released the checkpoint");
			}
		});
		*tree.core.inner.checkpoint_hook.lock() = Some(hook);
		Park {
			reached,
			release,
			fired,
		}
	}

	/// Waits until the checkpoint is parked at `stage`.
	fn reached(&self, stage: CheckpointStage) {
		let got = self
			.reached
			.recv_timeout(LONG)
			.unwrap_or_else(|_| panic!("the checkpoint never reached {stage:?}"));
		assert_eq!(got, stage, "the checkpoint stopped at another stage");
	}

	fn release(&self) {
		self.release.send(()).unwrap();
	}
}

// ---------------------------------------------------------------------------
// 1. liveness
// ---------------------------------------------------------------------------

/// Overwriting and deleting the keys that live in the oldest files and compacting deletes exactly
/// the files below the oldest pointer that is left: the files above it stay even when nothing
/// points into them, the file the oldest pointer is in stays, and every key reads back its newest
/// value. The phases make the oldest pointer move, stay, and move again.
#[test(tokio::test)]
async fn churn_over_the_oldest_keys_removes_exactly_the_files_nothing_references() {
	let dir = TempDir::new("vlog_gc_churn").unwrap();
	let tree = quiet_tree(dir.path()).await;
	let mut model = Model::default();

	// Round 1: 40 keys in about ten files, in file order.
	put_and_flush(&tree, &mut model, 0..40, 1).await;
	let files = vlog_files(dir.path());
	assert!(files.len() >= 9, "round 1 filled several files: {files:?}");
	assert_eq!(assert_collected(&tree, dir.path(), "round 1"), files[0]);

	// Phase 1: the first twenty keys are overwritten and the next four deleted: the pointers of
	// the first files are gone, and the compaction deletes those files.
	put_and_flush(&tree, &mut model, 0..20, 2).await;
	delete(&tree, 20..24).await;
	model.delete(20..24);
	flush_to_tables(&tree);
	let before = vlog_files(dir.path());
	compact(&tree);
	let floor = assert_gc_state(&tree, dir.path(), &before, "phase 1");
	assert!(floor > before[0], "the oldest files were dropped: {floor} vs {}", before[0]);
	assert!(vlog_files(dir.path()).len() < before.len(), "files were deleted");
	assert!(vlog_files(dir.path()).contains(&floor), "the file of the oldest pointer stays");
	assert_model(&tree, &model, "after phase 1");
	assert_all_pointers_resolve(&tree, "after phase 1");

	// Phase 2: only the newest keys are overwritten: the oldest pointer does not move, so no
	// file is deleted, whatever the compaction dropped above it.
	put_and_flush(&tree, &mut model, 36..38, 3).await;
	put_and_flush(&tree, &mut model, 38..40, 3).await;
	let before = vlog_files(dir.path());
	compact(&tree);
	let after_floor = assert_gc_state(&tree, dir.path(), &before, "phase 2");
	assert_eq!(after_floor, floor, "the oldest pointer stayed where it was");
	assert_eq!(vlog_files(dir.path()), before, "nothing is deleted while the oldest pointer stays");
	assert_model(&tree, &model, "after phase 2");

	// Phase 3: the keys that pointed into the oldest files left are overwritten: the bound moves
	// up and the files in between go.
	put_and_flush(&tree, &mut model, 24..36, 4).await;
	put_and_flush(&tree, &mut model, 24..36, 5).await;
	let before = vlog_files(dir.path());
	compact(&tree);
	let moved = assert_gc_state(&tree, dir.path(), &before, "phase 3");
	assert!(moved > floor, "the oldest pointer moved up: {moved} vs {floor}");
	assert_model(&tree, &model, "after phase 3");
	assert_all_pointers_resolve(&tree, "after phase 3");

	// Phase 4: everything that is left is deleted or overwritten: only the files of the newest
	// round stay.
	delete(&tree, 0..20).await;
	model.delete(0..20);
	flush_to_tables(&tree);
	put_and_flush(&tree, &mut model, 20..40, 6).await;
	let before = vlog_files(dir.path());
	compact(&tree);
	let last = assert_gc_state(&tree, dir.path(), &before, "phase 4");
	assert!(last > moved);
	assert_model(&tree, &model, "after phase 4");
	assert_no_deleted_file_held_open(dir.path(), "after phase 4");

	// What a fresh tree sees, with a cache of its own.
	crash(&tree);
	let again = quiet_tree(dir.path()).await;
	assert_model(&again, &model, "after a restart");
	assert_eq!(assert_collected(&again, dir.path(), "after a restart"), last);
	crash(&again);
}

/// Registers a table of `keys` with the values of `round`, each in a value-log file after every
/// file there is, as the table of an oversized commit is, and makes it visible, without the
/// memtables being flushed and without a commit. The keys must be in no memtable. Nothing is
/// logged to the WAL, and no WAL segment is claimed.
fn register_table_past_the_memtables(tree: &Tree, keys: Range<usize>, round: usize) -> Contents {
	use crate::batch::Batch;
	use crate::levels::write_manifest_to_disk;
	use crate::InternalKeyKind;

	let inner = &tree.core.inner;
	let vlog = inner.vlog.as_ref().unwrap();
	let count = keys.len() as u64;
	let seq = tree.core.commit_pipeline.log_seq_num.fetch_add(count, Ordering::SeqCst);
	let mut batch = Batch::new(seq);
	let mut written = Contents::new();
	for i in keys {
		let pointer = vlog.append(&key_of(i), &big(i, round)).unwrap();
		let value = ValueLocation::with_pointer(pointer).encode();
		batch.add_record(InternalKeyKind::Set, key_of(i), Some(value), 0).unwrap();
		written.insert(key_of(i), big(i, round));
	}
	let log_number = inner.level_manifest.read().unwrap().get_log_number();
	let table_id = inner.level_manifest.read().unwrap().next_table_id();
	inner
		.write_batch_direct_to_l0_sst(&batch, table_id, log_number.saturating_sub(1), u64::MAX)
		.unwrap();
	let mut manifest = inner.level_manifest.write().unwrap();
	if manifest.get_log_number() != log_number {
		manifest.log_number = log_number;
		write_manifest_to_disk(&manifest).unwrap();
	}
	drop(manifest);
	inner.visible_seq_num.fetch_max(seq + count - 1, Ordering::SeqCst);
	written
}

/// Memtables that no table covers: the files that only the active or a queued memtable points
/// into stay through a compaction whose tables point past them, and go once the memtable is
/// flushed and its keys overwritten.
async fn files_of_a_memtable_below_every_table(queued: bool) {
	let dir = TempDir::new("vlog_gc_memtable").unwrap();
	let tree = quiet_tree(dir.path()).await;
	let mut model = Model::default();

	// Two older rounds in two tables of keys 100..108 (files of their own).
	put_and_flush(&tree, &mut model, 100..108, 1).await;
	put_and_flush(&tree, &mut model, 100..108, 2).await;
	let dead = vlog_files(dir.path());

	// The memtable: keys 0..8 in the files after them.
	model.put(put(&tree, 0..8, 1).await);
	if queued {
		tree.core.inner.rotate_memtable().unwrap();
		assert_eq!(tree.core.inner.immutable_count(), 1, "the memtable is queued: nothing flushes");
	}
	let memtable_min = {
		let inner = &tree.core.inner;
		if queued {
			inner.immutable_memtables.read().unwrap().first().unwrap().memtable.min_vlog_file_id()
		} else {
			inner.active_memtable.read().unwrap().min_vlog_file_id()
		}
	}
	.expect("the memtable points into the value log");
	assert!(memtable_min >= *dead.last().unwrap(), "the memtable's files follow the tables'");

	// A third table past the memtable, over the same keys as the first two, as an oversized
	// commit writes one, and the compaction that drops the two older rounds.
	model.put(register_table_past_the_memtables(&tree, 100..108, 3));
	assert_eq!(l0_tables(&tree), 3);
	let before = vlog_files(dir.path());
	compact(&tree);
	let tables_min = tree.core.inner.level_manifest.read().unwrap().min_oldest_vlog_file_id();
	assert!(tables_min > memtable_min, "every table points past the memtable's files");
	let floor = assert_gc_state(&tree, dir.path(), &before, "with the memtable");
	assert_eq!(floor, memtable_min, "the memtable's oldest file is the bound");
	assert!(
		vlog_files(dir.path()).len() < before.len(),
		"the files of the dropped rounds were deleted: {:?} of {before:?}",
		vlog_files(dir.path())
	);
	assert_model(&tree, &model, "after the compaction");
	assert_all_pointers_resolve(&tree, "after the compaction");

	// What a restart makes of it: the memtable is rebuilt from the WAL, and its files are there.
	let image = TempDir::new("vlog_gc_memtable_image").unwrap();
	let crashed = image.path().join("crashed");
	copy_dir_all(dir.path(), &crashed);
	let reopened = quiet_tree(&crashed).await;
	assert_model(&reopened, &model, "after a crash");
	assert_all_pointers_resolve(&reopened, "after a crash");
	assert_eq!(assert_collected(&reopened, &crashed, "after a crash"), memtable_min);
	crash(&reopened);

	// The memtable is flushed, its keys overwritten, and a compaction lets its files go.
	tree.core.inner.rotate_memtable().unwrap();
	tree.core.inner.flush_all_immutables_sync().unwrap();
	put_and_flush(&tree, &mut model, 0..8, 4).await;
	let before = vlog_files(dir.path());
	compact(&tree);
	let moved = assert_gc_state(&tree, dir.path(), &before, "after the flush");
	assert!(moved > memtable_min, "the memtable's files are not needed any more");
	assert!(!vlog_files(dir.path()).contains(&memtable_min), "its oldest file is deleted");
	assert_model(&tree, &model, "after the second compaction");
	crash(&tree);
}

#[test(tokio::test)]
async fn a_compaction_keeps_the_files_of_the_active_memtable_that_every_table_points_past() {
	files_of_a_memtable_below_every_table(false).await;
}

#[test(tokio::test)]
async fn a_compaction_keeps_the_files_of_a_queued_memtable_that_every_table_points_past() {
	files_of_a_memtable_below_every_table(true).await;
}

/// A table that nothing in it points into the log (small values, deletes) must not hold the log
/// back: the oldest pointer is taken over the tables that have one. A flush that adds such a table
/// still deletes the files that the compaction before it left behind, because a pin was held.
#[test(tokio::test)]
async fn a_table_without_pointers_does_not_hold_the_value_log_back() {
	let dir = TempDir::new("vlog_gc_no_pointers").unwrap();
	let tree = quiet_tree(dir.path()).await;
	let mut model = two_rounds_in_two_tables(&tree).await;
	{
		// The pin defers the cleanup of the compaction, which leaves its obsolete files behind.
		let _pin = tree.core.inner.vlog.as_ref().unwrap().pin_files();
		compact(&tree);
	}
	let left = vlog_files(dir.path());
	let pointed_into = oldest_pointer(&tree);
	assert!(left[0] < pointed_into, "obsolete files are left: {left:?} below {pointed_into}");

	// Small values and deletes: the table of this memtable has no pointer at all.
	let small: Contents =
		(200..210).map(|i| (key_of(i), format!("small_{i:04}").into_bytes())).collect();
	let entries: Vec<_> = small.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
	commit(&tree, &entries, Durability::Immediate).await.unwrap();
	model.put(small);
	delete(&tree, 0..4).await;
	model.delete(0..4);
	flush_to_tables(&tree);
	let manifest = tree.core.inner.level_manifest.read().unwrap();
	assert!(
		manifest.iter().any(|table| table.meta.properties.oldest_vlog_file_id == 0),
		"a table without a pointer is in the tree"
	);
	assert_eq!(manifest.min_oldest_vlog_file_id(), pointed_into, "it does not lower the bound");
	drop(manifest);

	// The flush ran the cleanup with that table in the tree.
	let after = assert_gc_state(&tree, dir.path(), &left, "after the flush");
	assert_eq!(after, pointed_into);
	assert!(vlog_files(dir.path()).len() < left.len(), "the obsolete files were deleted");
	assert!(!vlog_files(dir.path()).contains(&left[0]), "the oldest file is gone");
	assert_model(&tree, &model, "after the flush");
	assert_all_pointers_resolve(&tree, "after the flush");
	crash(&tree);
}

/// The cleanup of the value log itself, with no tree around it: it removes the files below the
/// bound, exactly, never the active file, and nothing while a pin is held.
#[test(tokio::test)]
async fn the_cleanup_removes_exactly_the_files_below_its_bound_and_never_the_active_one() {
	let dir = TempDir::new("vlog_gc_bound").unwrap();
	let mut opts = base_options(dir.path());
	opts.vlog_max_file_size = 1024;
	std::fs::create_dir_all(opts.vlog_dir()).unwrap();
	let vlog = VLog::new(Arc::new(opts)).unwrap();

	// One value to a file: six values, files 1 to 6.
	let pointers: Vec<ValuePointer> =
		(0..6).map(|i| vlog.append(format!("key_{i}").as_bytes(), &big(i, 1)).unwrap()).collect();
	vlog.flush().unwrap();
	assert_eq!(vlog_files(dir.path()), vec![1, 2, 3, 4, 5, 6]);
	assert_eq!(vlog.active_writer_id.load(Ordering::SeqCst), 6);
	// Every file is read once, so the log holds a handle for it, and none was fsynced, so the log
	// holds one more for each file it rolled over from.
	for (i, pointer) in pointers.iter().enumerate() {
		assert_eq!(
			vlog.get(pointer).unwrap(),
			big(i, 1),
			"file {} reads before the cleanup",
			i + 1
		);
	}
	assert_no_deleted_file_held_open(dir.path(), "before the cleanup");

	// A bound of 0 or 1 removes nothing.
	vlog.cleanup_obsolete_files(0).unwrap();
	vlog.cleanup_obsolete_files(1).unwrap();
	assert_eq!(vlog_files(dir.path()), vec![1, 2, 3, 4, 5, 6], "no file is below 1");

	// Below 3: the file 3 stays, and it is the file at the bound.
	vlog.cleanup_obsolete_files(3).unwrap();
	assert_eq!(vlog_files(dir.path()), vec![3, 4, 5, 6], "the files below 3 are removed");
	assert_no_deleted_file_held_open(dir.path(), "after the cleanup below 3");
	assert_eq!(vlog.get(&pointers[2]).unwrap(), big(2, 1), "the file at the bound is readable");
	assert!(vlog.get(&pointers[0]).is_err(), "a removed file is an error, not a value");
	assert!(vlog.get(&pointers[1]).is_err(), "a removed file is an error, not a value");
	vlog.cleanup_obsolete_files(3).unwrap();
	assert_eq!(vlog_files(dir.path()), vec![3, 4, 5, 6], "a repeated cleanup changes nothing");

	// Pinned: nothing is removed however many pins and whatever the bound; the files go when the
	// last pin is dropped and the next cleanup runs.
	let first = vlog.pin_files();
	let second = vlog.pin_files();
	vlog.cleanup_obsolete_files(u32::MAX).unwrap();
	assert_eq!(vlog_files(dir.path()), vec![3, 4, 5, 6], "a pinned log deletes nothing");
	drop(first);
	vlog.cleanup_obsolete_files(u32::MAX).unwrap();
	assert_eq!(vlog_files(dir.path()), vec![3, 4, 5, 6], "one pin is still held");
	drop(second);

	// The active file is never removed, whatever the bound.
	vlog.cleanup_obsolete_files(6).unwrap();
	assert_eq!(vlog_files(dir.path()), vec![6], "the files below 6 are removed");
	assert_no_deleted_file_held_open(dir.path(), "after the cleanup below 6");
	vlog.cleanup_obsolete_files(u32::MAX).unwrap();
	assert_eq!(vlog_files(dir.path()), vec![6], "the active file is not removed");
	assert_eq!(vlog.get(&pointers[5]).unwrap(), big(5, 1), "its value is readable");

	// File ids only grow: the next file is after every file there ever was.
	let next = vlog.append(b"next", &big(7, 1)).unwrap();
	let after = vlog.append(b"next_2", &big(8, 1)).unwrap();
	assert_eq!((next.file_id, after.file_id), (7, 8), "the full active file is followed by 7, 8");
	assert_eq!(vlog_files(dir.path()), vec![6, 7, 8], "no older id came back");
	vlog.flush().unwrap();
	assert_eq!(vlog.get(&next).unwrap(), big(7, 1));
	assert_eq!(vlog.get(&after).unwrap(), big(8, 1));
}

// ---------------------------------------------------------------------------
// 2. snapshots and open iterators
// ---------------------------------------------------------------------------

fn snapshots(tree: &Tree) -> usize {
	tree.core.inner.snapshot_tracker.get_all_snapshots().len()
}

/// The files that hold only values of the round that took the active file from `active_before`
/// to `active_after`: the first and the last file of a round are shared with its neighbours.
fn files_of_round(active_before: u32, active_after: u32) -> Vec<u32> {
	(active_before + 1..active_after).collect()
}

/// A snapshot keeps the values it sees readable across compactions, and with them the files they
/// are in: the compaction keeps the versions the snapshot can see. When it is dropped, a later
/// compaction lets the files go.
#[test(tokio::test)]
async fn a_snapshot_keeps_its_values_and_their_files_across_compactions_until_it_is_dropped() {
	let dir = TempDir::new("vlog_gc_snapshot").unwrap();
	let tree = quiet_tree(dir.path()).await;
	let mut model = Model::default();

	let start = active_file(&tree);
	put_and_flush(&tree, &mut model, 0..24, 1).await;
	let round1 = model.clone();
	let round1_files = files_of_round(start, active_file(&tree));
	assert!(round1_files.len() >= 3, "round 1 has files of its own: {round1_files:?}");

	let snap = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	assert_eq!(snapshots(&tree), 1, "the transaction is a registered snapshot");

	// Round 2 overwrites everything, and a compaction merges the tables of 1 and 2.
	put_and_flush(&tree, &mut model, 0..24, 2).await;
	let before = vlog_files(dir.path());
	compact(&tree);
	let floor = assert_gc_state(&tree, dir.path(), &before, "after the first compaction");
	assert!(floor <= round1_files[0], "round 1 is still pointed to: {floor}");
	for file in &round1_files {
		assert!(vlog_files(dir.path()).contains(file), "round 1's file {file} is kept");
	}

	// More rounds and a compaction that would drop everything but the newest version.
	put_and_flush(&tree, &mut model, 0..24, 3).await;
	put_and_flush(&tree, &mut model, 0..24, 4).await;
	let before = vlog_files(dir.path());
	compact(&tree);
	assert_gc_state(&tree, dir.path(), &before, "after the second compaction");
	for file in &round1_files {
		assert!(vlog_files(dir.path()).contains(file), "round 1's file {file} is kept");
	}

	// The snapshot reads round 1, one key at a time and in a scan; a new transaction reads round 4.
	for (key, value) in &round1.live {
		assert_eq!(
			snap.get(key).unwrap().as_deref(),
			Some(value.as_slice()),
			"{}",
			String::from_utf8_lossy(key)
		);
	}
	let scanned: Contents =
		collect_transaction_all(&mut snap.iter().unwrap()).unwrap().into_iter().collect();
	assert_same_contents(&scanned, &round1.live, "the snapshot's scan is round 1");
	assert_model(&tree, &model, "a new transaction");
	assert_all_pointers_resolve(&tree, "with the snapshot");

	// Dropped: two more rounds overlap the keys, a compaction drops every old version, and the
	// files of rounds 1 to 3 go.
	drop(snap);
	assert_eq!(snapshots(&tree), 0, "the snapshot is gone");
	put_and_flush(&tree, &mut model, 0..24, 5).await;
	put_and_flush(&tree, &mut model, 0..24, 6).await;
	let before = vlog_files(dir.path());
	compact(&tree);
	let floor = assert_gc_state(&tree, dir.path(), &before, "after the snapshot was dropped");
	for file in &round1_files {
		assert!(!vlog_files(dir.path()).contains(file), "round 1's file {file} is deleted");
	}
	assert!(floor > *round1_files.last().unwrap());
	assert_model(&tree, &model, "after the snapshot was dropped");
	crash(&tree);
}

/// An iterator that is half way through its scan keeps reading the values it started with
/// while compactions rewrite the tables under it, and the files of those values stay until it is
/// dropped.
#[test(tokio::test)]
async fn an_open_iterator_keeps_reading_its_values_across_a_compaction() {
	let dir = TempDir::new("vlog_gc_iterator").unwrap();
	let tree = quiet_tree(dir.path()).await;
	let mut model = Model::default();
	let start = active_file(&tree);
	put_and_flush(&tree, &mut model, 0..24, 1).await;
	let round1 = model.clone();
	let round1_files = files_of_round(start, active_file(&tree));

	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let mut it = txn.iter().unwrap();
	assert!(it.seek_first().unwrap());
	assert_eq!(snapshots(&tree), 1, "the iterator's transaction is a registered snapshot");
	let mut seen = Contents::new();
	for _ in 0..8 {
		assert!(it.valid());
		seen.insert(it.key().user_key().to_vec(), it.value().unwrap());
		it.next().unwrap();
	}

	// The rest of the tree moves on and is compacted, with the files of round 1 at stake.
	put_and_flush(&tree, &mut model, 0..24, 2).await;
	put_and_flush(&tree, &mut model, 0..24, 3).await;
	assert_eq!(l0_tables(&tree), 3);
	let before = vlog_files(dir.path());
	compact(&tree);
	assert_gc_state(&tree, dir.path(), &before, "while the iterator is open");
	for file in &round1_files {
		assert!(vlog_files(dir.path()).contains(file), "round 1's file {file} is kept");
	}

	while it.valid() {
		seen.insert(it.key().user_key().to_vec(), it.value().unwrap());
		it.next().unwrap();
	}
	assert_same_contents(&seen, &round1.live, "the iterator read round 1 from start to end");
	drop(it);
	drop(txn);
	assert_eq!(snapshots(&tree), 0);

	// Released: the next compaction over those keys deletes the files.
	put_and_flush(&tree, &mut model, 0..24, 4).await;
	put_and_flush(&tree, &mut model, 0..24, 5).await;
	let before = vlog_files(dir.path());
	compact(&tree);
	assert_gc_state(&tree, dir.path(), &before, "after the iterator was dropped");
	for file in &round1_files {
		assert!(!vlog_files(dir.path()).contains(file), "round 1's file {file} is deleted");
	}
	assert_model(&tree, &model, "after the iterator was dropped");
	crash(&tree);
}

/// Three snapshots of three rounds: the files of a round stay while its snapshot, or an older
/// one, is open, and the oldest snapshot decides, since a cleanup deletes by a minimum.
#[test(tokio::test)]
async fn snapshots_of_three_rounds_release_the_files_in_the_order_the_oldest_allows() {
	let dir = TempDir::new("vlog_gc_snapshots").unwrap();
	let tree = quiet_tree(dir.path()).await;
	let mut model = Model::default();
	let mut contents = Vec::new();
	let mut files = Vec::new();
	let mut snaps: Vec<Option<Transaction>> = Vec::new();
	for round in 1..=3 {
		let start = active_file(&tree);
		put_and_flush(&tree, &mut model, 0..16, round).await;
		files.push(files_of_round(start, active_file(&tree)));
		contents.push(model.live.clone());
		snaps.push(Some(tree.begin_with_mode(Mode::ReadOnly).unwrap()));
	}
	assert_eq!(snapshots(&tree), 3);

	let check = |snaps: &[Option<Transaction>], what: &str| {
		for (n, snap) in snaps.iter().enumerate() {
			if let Some(snap) = snap {
				for (key, value) in &contents[n] {
					assert_eq!(
						snap.get(key).unwrap().as_deref(),
						Some(value.as_slice()),
						"{what}: snapshot of round {} reads {}",
						n + 1,
						String::from_utf8_lossy(key)
					);
				}
			}
		}
	};

	// Two rounds over every key, flushed, and the compaction that merges them with the rest.
	async fn churn(tree: &Tree, model: &mut Model, round: usize) {
		put_and_flush(tree, model, 0..16, round).await;
		put_and_flush(tree, model, 0..16, round + 1).await;
	}

	// Rounds 4 and 5 overwrite: every file stays, the oldest snapshot is open.
	churn(&tree, &mut model, 4).await;
	let before = vlog_files(dir.path());
	compact(&tree);
	assert_gc_state(&tree, dir.path(), &before, "all snapshots open");
	for file in files.iter().flatten() {
		assert!(vlog_files(dir.path()).contains(file), "file {file} is kept");
	}
	check(&snaps, "all open");

	// The snapshot of round 2 is dropped: round 1's snapshot still holds every file.
	snaps[1] = None;
	churn(&tree, &mut model, 6).await;
	let before = vlog_files(dir.path());
	compact(&tree);
	assert_gc_state(&tree, dir.path(), &before, "the middle snapshot dropped");
	for file in files.iter().flatten() {
		assert!(vlog_files(dir.path()).contains(file), "file {file} is kept by round 1");
	}
	check(&snaps, "the middle one dropped");

	// The snapshot of round 1 is dropped: rounds 1 and 2 are not needed, round 3 still is.
	snaps[0] = None;
	churn(&tree, &mut model, 8).await;
	let before = vlog_files(dir.path());
	compact(&tree);
	assert_gc_state(&tree, dir.path(), &before, "the oldest snapshot dropped");
	for file in files[0].iter().chain(files[1].iter()) {
		assert!(!vlog_files(dir.path()).contains(file), "file {file} of round 1 or 2 is deleted");
	}
	for file in &files[2] {
		assert!(vlog_files(dir.path()).contains(file), "file {file} of round 3 is kept");
	}
	check(&snaps, "only round 3 is open");

	snaps[2] = None;
	assert_eq!(snapshots(&tree), 0);
	churn(&tree, &mut model, 10).await;
	let before = vlog_files(dir.path());
	compact(&tree);
	assert_gc_state(&tree, dir.path(), &before, "no snapshot open");
	for file in &files[2] {
		assert!(!vlog_files(dir.path()).contains(file), "file {file} of round 3 is deleted");
	}
	assert_model(&tree, &model, "at the end");
	crash(&tree);
}

// ---------------------------------------------------------------------------
// 3. readers racing writers, flushes, compactions and their cleanups
// ---------------------------------------------------------------------------

const CONC_LEN: usize = 700;

/// The value of `key` at version `ver`: it names both, and its body is a function of them, so a
/// read of any other bytes is recognised.
fn conc_value(i: usize, ver: usize) -> Vec<u8> {
	let mut v = format!("v_{i:04}_{ver:06}_").into_bytes();
	v.resize(CONC_LEN, b'a' + ((i + ver) % 26) as u8);
	v
}

/// The version of `key` that `value` is, if it is a valid value of it.
fn conc_version(i: usize, value: &[u8]) -> Result<usize, String> {
	if value.len() != CONC_LEN {
		return Err(format!("key {i}: {} bytes instead of {CONC_LEN}", value.len()));
	}
	let head = std::str::from_utf8(&value[..14]).map_err(|_| format!("key {i}: no header"))?;
	if !head.starts_with(&format!("v_{i:04}_")) {
		return Err(format!("key {i}: the value of another key: {head}"));
	}
	let ver: usize = head[7..13].parse().map_err(|_| format!("key {i}: bad version {head}"))?;
	if value != conc_value(i, ver).as_slice() {
		return Err(format!("key {i}: the body is not that of version {ver}"));
	}
	Ok(ver)
}

/// One read of `key` in `txn`, which must hold a valid version of it.
fn read_valid(txn: &Transaction, i: usize) -> Result<(usize, Vec<u8>), String> {
	let value = txn
		.get(key_of(i))
		.map_err(|e| format!("get of key {i}: {e}"))?
		.ok_or_else(|| format!("key {i} is missing"))?;
	conc_version(i, &value).map(|ver| (ver, value))
}

/// What a reader thread does until `done`: `mode` 0 reads the keys one by one, each in a new
/// transaction, and a key never goes back to an older version; 1 scans them all; 2 reads them all
/// twice in a transaction of its own, with a pause between, and both reads must agree.
fn run_reader(
	tree: &Tree,
	mode: usize,
	keys: usize,
	done: &AtomicBool,
	ops: &AtomicUsize,
) -> Result<(), String> {
	let mut newest = vec![0usize; keys];
	while !done.load(Ordering::SeqCst) {
		match mode {
			0 => {
				for (i, seen) in newest.iter_mut().enumerate() {
					let txn = tree.begin_with_mode(Mode::ReadOnly).map_err(|e| e.to_string())?;
					let (ver, _) = read_valid(&txn, i)?;
					if ver < *seen {
						return Err(format!("key {i} went back from version {seen} to {ver}"));
					}
					*seen = ver;
					ops.fetch_add(1, Ordering::SeqCst);
				}
			}
			1 => {
				let txn = tree.begin_with_mode(Mode::ReadOnly).map_err(|e| e.to_string())?;
				let all = txn
					.iter()
					.and_then(|mut it| collect_transaction_all(&mut it))
					.map_err(|e| format!("scan: {e}"))?;
				if all.len() != keys {
					return Err(format!("a scan found {} keys instead of {keys}", all.len()));
				}
				for (i, (key, value)) in all.iter().enumerate() {
					if key != &key_of(i) {
						return Err(format!(
							"a scan found {:?} at {i}",
							String::from_utf8_lossy(key)
						));
					}
					conc_version(i, value)?;
				}
				ops.fetch_add(1, Ordering::SeqCst);
			}
			_ => {
				let txn = tree.begin_with_mode(Mode::ReadOnly).map_err(|e| e.to_string())?;
				let first: Result<Vec<_>, String> =
					(0..keys).map(|i| read_valid(&txn, i).map(|(_, v)| v)).collect();
				std::thread::yield_now();
				let second: Result<Vec<_>, String> =
					(0..keys).map(|i| read_valid(&txn, i).map(|(_, v)| v)).collect();
				if first? != second? {
					return Err("a transaction read two different states of the tree".to_string());
				}
				ops.fetch_add(1, Ordering::SeqCst);
			}
		}
	}
	Ok(())
}

/// The lowest value-log file id in `dir`, or 0 when there is none.
fn oldest_vlog_file(dir: &Path) -> u32 {
	let Ok(listing) = std::fs::read_dir(dir.join("vlog")) else {
		return 0;
	};
	listing
		.filter_map(|e| {
			let name = e.ok()?.file_name().to_string_lossy().into_owned();
			name.strip_suffix(".vlog")?.parse::<u32>().ok()
		})
		.min()
		.unwrap_or(0)
}

/// Wakes the tree's compaction task. A flush by hand does not: only a flush by the background
/// task does, and a hand flush that takes the memtable first leaves the task asleep, so that no
/// compaction runs (a checkpoint wakes it for the same reason).
fn wake_compactor(tree: &Tree) {
	if let Some(task_manager) = tree.core.task_manager.lock().unwrap().as_ref() {
		task_manager.wake_up_level();
	}
}

/// Reader threads hammer `get` and scans while writers overwrite the keys and the tree flushes
/// and compacts. With `manual_compaction` the tree does not compact by itself and a thread of
/// the test flushes and compacts by hand; otherwise the tree compacts in the background and the
/// thread flushes and runs the cleanup by hand. No read fails, and every value read is a valid
/// version of its key. The value log is collected while this goes on, and at the end it is
/// collected and every pointer resolves.
async fn readers_race_everything(manual_compaction: bool) {
	const KEYS: usize = 48;
	const WRITERS: usize = 2;
	// The writers go on for at least this many rounds, and until the value log was collected
	// while they ran and every thread did its share, but not for more than the last.
	const ROUNDS: usize = 100;
	const MAX_ROUNDS: usize = 400;
	const READERS: usize = 3;
	const PER_COMMIT: usize = 6;

	let dir = TempDir::new("vlog_gc_concurrent").unwrap();
	let tree = open_with(Options {
		max_memtable_size: 64 * 1024,
		// Files of 64 KiB: the cache of open files holds one handle for every file read, and a
		// writer that outruns the cleanup must not run the process out of them.
		vlog_max_file_size: 64 * 1024,
		// Only one compactor at a time: the tree's or the test's.
		level0_max_files: if manual_compaction {
			100
		} else {
			2
		},
		..base_options(dir.path())
	});

	// Every key has version 0 before anything reads.
	let initial: Vec<_> = (0..KEYS).map(|i| (key_of(i), conc_value(i, 0))).collect();
	commit(&tree, &initial, Durability::Immediate).await.unwrap();

	let done = Arc::new(AtomicBool::new(false));
	let ops: Vec<Arc<AtomicUsize>> = (0..READERS).map(|_| Arc::new(AtomicUsize::new(0))).collect();
	let readers: Vec<_> = (0..READERS)
		.map(|mode| {
			let (tree, done, ops) = (Arc::clone(&tree), Arc::clone(&done), Arc::clone(&ops[mode]));
			on_thread(move || {
				let result = run_reader(&tree, mode, KEYS, &done, &ops);
				if result.is_err() {
					done.store(true, Ordering::SeqCst);
				}
				result
			})
		})
		.collect();

	// The thread that flushes, compacts or cleans up by hand, and watches the oldest file move.
	let manual_done = Arc::new(AtomicBool::new(false));
	let collected = Arc::new(AtomicU32::new(0));
	let manual_rounds = Arc::new(AtomicUsize::new(0));
	let manual = {
		let (tree, stop, path, collected, count) = (
			Arc::clone(&tree),
			Arc::clone(&manual_done),
			dir.path().to_path_buf(),
			Arc::clone(&collected),
			Arc::clone(&manual_rounds),
		);
		on_thread(move || {
			let opts = Arc::new(Options {
				level0_max_files: 2,
				..Default::default()
			});
			let (mut rounds, mut oldest_seen) = (0usize, 0u32);
			while !stop.load(Ordering::SeqCst) {
				tree.flush().expect("a flush by hand");
				if manual_compaction {
					tree.compact(Arc::new(Strategy::from_options(Arc::clone(&opts))))
						.expect("a compaction by hand");
				} else {
					wake_compactor(&tree);
					cleanup(&tree);
				}
				oldest_seen = oldest_seen.max(oldest_vlog_file(&path));
				collected.fetch_max(oldest_seen, Ordering::SeqCst);
				rounds += 1;
				count.store(rounds, Ordering::SeqCst);
				// Pacing, not synchronization: a round with nothing to do is cheap.
				std::thread::sleep(Duration::from_millis(1));
			}
			// The oldest file only moves up, so a last look sees whatever the loop missed.
			oldest_seen = oldest_seen.max(oldest_vlog_file(&path));
			collected.fetch_max(oldest_seen, Ordering::SeqCst);
			(rounds, oldest_seen)
		})
	};

	// The writers, each over its own keys, with versions that only grow, a few keys to a commit.
	let per_writer = KEYS / WRITERS;
	let shared = (Arc::clone(&collected), Arc::clone(&manual_rounds), ops.clone());
	let ready = Arc::new(move || {
		let (collected, rounds, ops) = &shared;
		collected.load(Ordering::SeqCst) > 1
			&& rounds.load(Ordering::SeqCst) >= 5
			&& ops.iter().all(|count| count.load(Ordering::SeqCst) >= 3)
	});
	let writers: Vec<_> = (0..WRITERS)
		.map(|w| {
			let (tree, ready) = (Arc::clone(&tree), Arc::clone(&ready));
			tokio::spawn(async move {
				let mut ver = 1;
				loop {
					for first in (w * per_writer..(w + 1) * per_writer).step_by(PER_COMMIT) {
						let mut txn = tree.begin().unwrap();
						for i in first..(first + PER_COMMIT).min((w + 1) * per_writer) {
							txn.set(key_of(i), conc_value(i, ver)).unwrap();
						}
						txn.commit().await.unwrap();
					}
					if ver >= ROUNDS && ready() {
						return ver;
					}
					if ver >= MAX_ROUNDS {
						// No more writes, so that the value log does not grow without bound, but
						// the tree and the threads go on, and the compaction that this much
						// data needs gets the time it needs.
						let deadline = std::time::Instant::now() + STUCK / 2;
						while !ready() && std::time::Instant::now() < deadline {
							tokio::time::sleep(Duration::from_millis(5)).await;
						}
						return ver;
					}
					ver += 1;
				}
			})
		})
		.collect();
	let mut finals = Vec::new();
	for writer in writers {
		finals.push(tokio::time::timeout(STUCK, writer).await.expect("a writer is stuck").unwrap());
	}

	done.store(true, Ordering::SeqCst);
	manual_done.store(true, Ordering::SeqCst);
	for (n, reader) in readers.into_iter().enumerate() {
		reader.wait_finished("a reader to stop");
		reader.wait().unwrap_or_else(|e| panic!("reader {n}: {e}"));
	}
	manual.wait_finished("the manual thread to stop");
	let (rounds, oldest_seen) = manual.wait();
	for (n, count) in ops.iter().enumerate() {
		let count = count.load(Ordering::SeqCst);
		assert!(count >= 3, "reader {n} made {count} reads");
	}
	assert!(rounds >= 2, "the manual thread ran {rounds} rounds");
	assert!(
		oldest_seen > 1,
		"the value log was not collected while the writers ran: oldest file {oldest_seen}, writer \
		 rounds {finals:?}, {rounds} rounds by hand, {} level-0 tables, {} files in the value log, \
		 readers' reads {:?}",
		l0_tables(&tree),
		std::fs::read_dir(dir.path().join("vlog")).map(|d| d.count()).unwrap_or(0),
		ops.iter().map(|c| c.load(Ordering::SeqCst)).collect::<Vec<_>>(),
	);

	// Now: nothing runs but this test.
	stop_background_tasks(&tree).await;
	flush_to_tables(&tree);
	while l0_tables(&tree) >= 2 {
		compact(&tree);
	}
	cleanup(&tree);
	let mut model = Model::default();
	model.put((0..KEYS).map(|i| (key_of(i), conc_value(i, finals[i / per_writer]))).collect());
	assert_model(&tree, &model, "after the race");
	assert_collected(&tree, dir.path(), "after the race");
	assert_all_pointers_resolve(&tree, "after the race");
	crash(&tree);
	let again = quiet_tree(dir.path()).await;
	assert_model(&again, &model, "after a restart");
	crash(&again);
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn readers_see_only_valid_versions_while_the_tree_compacts_by_itself() {
	readers_race_everything(false).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn readers_see_only_valid_versions_while_the_test_flushes_and_compacts_by_hand() {
	readers_race_everything(true).await;
}

// ---------------------------------------------------------------------------
// 4. a checkpoint pins the value log
// ---------------------------------------------------------------------------

/// Two tables whose merge makes the first round's files obsolete.
async fn two_rounds_in_two_tables(tree: &Tree) -> Model {
	let mut model = Model::default();
	put_and_flush(tree, &mut model, 0..24, 1).await;
	put_and_flush(tree, &mut model, 0..24, 2).await;
	model
}

/// A compaction that runs after the checkpoint released the manifest and before it copied the
/// value log would delete files the checkpoint's tables point into: it must delete nothing, and
/// the copy must hold every file. The files go with the next cleanup once the checkpoint is done.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn a_compaction_after_the_manifest_is_released_deletes_no_file_before_the_copy() {
	static RELEASED: [CheckpointStage; 1] = [CheckpointStage::ManifestReleased];
	let dir = TempDir::new("vlog_gc_checkpoint").unwrap();
	let ckpt = TempDir::new("vlog_gc_checkpoint_copy").unwrap();
	let dest = ckpt.path().join("copy");
	let tree = quiet_tree(dir.path()).await;
	let model = two_rounds_in_two_tables(&tree).await;
	let before = vlog_files(dir.path());

	let park = Park::install(&tree, &RELEASED);
	let call = checkpoint(&tree, &dest);
	park.reached(CheckpointStage::ManifestReleased);

	// The manifest is free and the value log is not copied yet.
	compact(&tree);
	let floor = oldest_pointer(&tree);
	assert!(floor > before[0], "the merge made files obsolete: {floor} vs {before:?}");
	assert_eq!(vlog_files(dir.path()), before, "the compaction deleted nothing, a pin is held");
	cleanup(&tree);
	assert_eq!(vlog_files(dir.path()), before, "a cleanup deletes nothing either");

	park.release();
	call.wait().unwrap();
	assert_eq!(park.fired.load(Ordering::SeqCst), 1, "the hook fired once");

	// The pin is gone: the next cleanup deletes what the compaction made obsolete.
	cleanup(&tree);
	assert_gc_state(&tree, dir.path(), &before, "after the checkpoint");
	assert!(vlog_files(dir.path()).len() < before.len(), "files were deleted");
	assert_model(&tree, &model, "the live tree");

	// The copy holds every file, and a database opened on it reads every value, every version.
	assert_eq!(vlog_files(&dest), before, "the checkpoint copied the whole value log");
	let restored = quiet_tree(&dest).await;
	assert_model(&restored, &model, "the checkpoint");
	assert_all_pointers_resolve(&restored, "the checkpoint");
	assert_eq!(vlog_files(&dest), before, "opening the checkpoint deleted nothing it points into");
	crash(&restored);
	crash(&tree);
}

/// While the checkpoint holds the manifest, a cleanup deletes nothing (the pin is taken with the
/// manifest), a compaction waits for the manifest, and once the manifest is released and the
/// compaction has run, the copy still holds every file.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn a_checkpoint_that_holds_the_manifest_keeps_every_file_until_its_copy_is_done() {
	static STAGES: [CheckpointStage; 2] =
		[CheckpointStage::SstablesCopied, CheckpointStage::ManifestReleased];
	let dir = TempDir::new("vlog_gc_checkpoint_held").unwrap();
	let ckpt = TempDir::new("vlog_gc_checkpoint_held_copy").unwrap();
	let dest = ckpt.path().join("copy");
	let tree = quiet_tree(dir.path()).await;
	let model = two_rounds_in_two_tables(&tree).await;
	let before = vlog_files(dir.path());

	let park = Park::install(&tree, &STAGES);
	let call = checkpoint(&tree, &dest);
	park.reached(CheckpointStage::SstablesCopied);

	// The pin is taken: not even the largest bound deletes a file.
	let vlog = tree.core.inner.vlog.as_ref().unwrap();
	vlog.cleanup_obsolete_files(u32::MAX).unwrap();
	assert_eq!(vlog_files(dir.path()), before, "a pinned log deletes nothing");

	// A compaction needs the manifest, which the checkpoint holds.
	let compaction = {
		let tree = Arc::clone(&tree);
		on_thread(move || compact(&tree))
	};
	compaction.assert_blocked("the compaction ran while the checkpoint held the manifest");

	park.release();
	park.reached(CheckpointStage::ManifestReleased);
	compaction.wait_finished("the compaction, now that the manifest is released");
	compaction.wait();
	let floor = oldest_pointer(&tree);
	assert!(floor > before[0], "the merge made files obsolete");
	assert_eq!(vlog_files(dir.path()), before, "the compaction deleted nothing, a pin is held");

	park.release();
	call.wait().unwrap();
	assert_eq!(park.fired.load(Ordering::SeqCst), 2, "both stages were reached");

	cleanup(&tree);
	assert_gc_state(&tree, dir.path(), &before, "after the checkpoint");
	assert!(vlog_files(dir.path()).len() < before.len(), "files were deleted");
	assert_eq!(vlog_files(&dest), before, "the checkpoint copied the whole value log");
	let restored = quiet_tree(&dest).await;
	assert_model(&restored, &model, "the checkpoint");
	assert_all_pointers_resolve(&restored, "the checkpoint");
	crash(&restored);
	crash(&tree);
}

/// A checkpoint restored into a live tree of its own reads every value, and the values written
/// after it go to files that are not any of the restored ones.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
#[ignore = "restore_from_checkpoint replaces the value-log files but not the log's state (active \
            writer, next file id, handles): values written after a restore get pointers into \
            file ids that the restored files also have, and read back as other bytes"]
async fn a_checkpoint_restored_into_a_live_tree_reads_every_value_and_new_values_get_new_files() {
	let dir = TempDir::new("vlog_gc_restore_source").unwrap();
	let ckpt = TempDir::new("vlog_gc_restore_copy").unwrap();
	let target = TempDir::new("vlog_gc_restore_target").unwrap();
	let dest = ckpt.path().join("copy");
	let tree = quiet_tree(dir.path()).await;
	let mut model = two_rounds_in_two_tables(&tree).await;
	let files = vlog_files(dir.path());
	tree.create_checkpoint(&dest).unwrap();
	assert_eq!(vlog_files(&dest), files);

	// The target holds other data, which the restore replaces.
	let other = quiet_tree(target.path()).await;
	put_and_flush(&other, &mut Model::default(), 500..510, 7).await;
	other.restore_from_checkpoint(&dest).unwrap();
	assert_model(&other, &model, "after the restore");
	assert_all_pointers_resolve(&other, "after the restore");

	// New values are written after the restore, and everything reads.
	model.put(put(&other, 30..40, 8).await);
	flush_to_tables(&other);
	assert_model(&other, &model, "after new writes");
	let all = vlog_files(target.path());
	assert!(all.len() > files.len(), "the new values went to files of their own: {all:?}");
	assert_all_pointers_resolve(&other, "after new writes");
	crash(&other);
	let again = quiet_tree(target.path()).await;
	assert_model(&again, &model, "after a restart of the restored tree");
	assert_all_pointers_resolve(&again, "after a restart of the restored tree");
	crash(&again);
	crash(&tree);
}

// ---------------------------------------------------------------------------
// 5. crashes around the cleanup
// ---------------------------------------------------------------------------

/// Truncates what a power loss may leave empty in an image of `live`: the tables that were never
/// fsynced, and the value-log files after the length of their last fsync.
fn lose_what_was_never_synced(image: &Path, live: &Path) {
	for entry in std::fs::read_dir(image.join("sstables")).unwrap() {
		let entry = entry.unwrap();
		let live_path = live.join("sstables").join(entry.file_name());
		if !sync_tracker::was_synced(&live_path) {
			std::fs::OpenOptions::new().write(true).open(entry.path()).unwrap().set_len(0).unwrap();
		}
	}
	let lengths = SYNCED_VLOG_LENGTHS.lock().clone();
	for id in vlog_files(image) {
		let live_path = vlog_file_path(live, id);
		let synced = lengths
			.iter()
			.filter(|(path, _)| *path == live_path)
			.map(|(_, len)| *len)
			.max()
			.unwrap_or(0);
		let image_path = vlog_file_path(image, id);
		let len = std::fs::metadata(&image_path).unwrap().len();
		if synced < len {
			std::fs::OpenOptions::new()
				.write(true)
				.open(image_path)
				.unwrap()
				.set_len(synced)
				.unwrap();
		}
	}
}

/// What the stage hook took: the stage, the image, and the listing of its value log.
type Images = Arc<Mutex<Vec<(CompactionStage, PathBuf, Vec<u32>)>>>;

/// Installs the stage hook that takes an image of `live` at every stage of the compactions that
/// follow.
fn take_images(tree: &Tree, live: &Path, root: &Path, lossy: bool) -> Images {
	let images: Images = Arc::new(Mutex::new(Vec::new()));
	let (taken, live, root) = (Arc::clone(&images), live.to_path_buf(), root.to_path_buf());
	let hook: crate::compaction::compactor::CompactionStageHook = Arc::new(move |stage| {
		let image = root.join(format!("{stage:?}_{lossy}"));
		copy_dir_all(&live, &image);
		if lossy {
			lose_what_was_never_synced(&image, &live);
		}
		let files = vlog_files(&image);
		taken.lock().unwrap().push((stage, image, files));
		Ok(())
	});
	*tree.core.inner.compaction_stage_hook.lock() = Some(hook);
	images
}

/// A crash at `stage` of a compaction: the image recovers every acknowledged key, the files that
/// the compaction made obsolete are there until the cleanup of the compaction or of the restart
/// removes them, and the files that a table or the replayed WAL points into never go.
async fn crash_at_stage(stage: CompactionStage, lossy: bool) {
	let root = TempDir::new("vlog_gc_crash_stage").unwrap();
	let live = root.path().join("live");
	let tree = quiet_tree(&live).await;
	let mut model = Model::default();

	// Round 1 in one table; round 2 over part of the keys and deletes of others in another; and
	// values only in the WAL (and a delete), which a restart replays.
	model.put(put_durable(&tree, 0..24, 1).await);
	flush_to_tables(&tree);
	model.put(put_durable(&tree, 0..16, 2).await);
	delete(&tree, 16..24).await;
	model.delete(16..24);
	flush_to_tables(&tree);
	model.put(put_durable(&tree, 100..104, 1).await);
	delete(&tree, 5..6).await;
	model.delete(5..6);
	let before = vlog_files(&live);

	let images = take_images(&tree, &live, root.path(), lossy);
	compact(&tree);
	let after = vlog_files(&live);
	assert!(after.len() < before.len(), "the compaction deleted files: {before:?} -> {after:?}");
	assert_gc_state(&tree, &live, &before, "the live tree");
	assert_model(&tree, &model, "the live tree");

	let taken = images.lock().unwrap().clone();
	let stages: Vec<_> = taken.iter().map(|(stage, ..)| *stage).collect();
	assert_eq!(
		stages,
		vec![
			CompactionStage::OutputsDurable,
			CompactionStage::ManifestWritten,
			CompactionStage::InputsRemoved,
			CompactionStage::VlogCleaned
		],
		"the hook fired at every stage, in order"
	);
	let (_, image, listing) = taken.iter().find(|(s, ..)| *s == stage).unwrap().clone();

	// Before anything was recovered: the obsolete files are in the image until the stage that
	// removes them.
	if stage == CompactionStage::VlogCleaned {
		assert_eq!(listing, after, "{stage:?}: the cleanup of the compaction ran");
	} else {
		assert_eq!(listing, before, "{stage:?}: the obsolete files are still on disk");
	}

	// Recovery.
	let recovered = quiet_tree(&image).await;
	let what = format!("a crash at {stage:?} (data loss: {lossy})");
	assert_model(&recovered, &model, &what);
	assert_all_pointers_resolve(&recovered, &what);
	assert_tables_match_manifest(&recovered, &image, &what);
	if stage == CompactionStage::OutputsDurable {
		// The manifest is the one from before the compaction: round 1 is still pointed to.
		assert_eq!(vlog_files(&image), before, "{what}: nothing the old manifest points to goes");
		assert_collected(&recovered, &image, &what);
	} else {
		assert_gc_state(&recovered, &image, &before, &what);
		assert_eq!(vlog_files(&image), after, "{what}: the restart reclaims what was left");
	}
	let settled = vlog_files(&image);

	// And again: a second restart finds the same.
	crash(&recovered);
	let again = quiet_tree(&image).await;
	assert_model(&again, &model, &format!("{what}, restarted twice"));
	assert_eq!(vlog_files(&image), settled, "{what}: the second restart deletes nothing more");
	crash(&again);
	crash(&tree);
}

#[test(tokio::test)]
async fn a_crash_before_the_manifest_write_keeps_every_acknowledged_key() {
	crash_at_stage(CompactionStage::OutputsDurable, false).await;
	crash_at_stage(CompactionStage::OutputsDurable, true).await;
}

#[test(tokio::test)]
async fn a_crash_after_the_manifest_write_keeps_every_acknowledged_key() {
	crash_at_stage(CompactionStage::ManifestWritten, false).await;
	crash_at_stage(CompactionStage::ManifestWritten, true).await;
}

#[test(tokio::test)]
async fn a_crash_after_the_input_files_are_removed_keeps_every_acknowledged_key() {
	crash_at_stage(CompactionStage::InputsRemoved, false).await;
	crash_at_stage(CompactionStage::InputsRemoved, true).await;
}

#[test(tokio::test)]
async fn a_crash_after_the_value_log_cleanup_keeps_every_acknowledged_key() {
	crash_at_stage(CompactionStage::VlogCleaned, false).await;
	crash_at_stage(CompactionStage::VlogCleaned, true).await;
}

/// A cleanup that a pin skipped leaves the obsolete files, and a crash then loses nothing and a
/// restart reclaims them; the flush that comes after is a cleanup of its own, and a crash image
/// right after it holds the files it deleted no more.
#[test(tokio::test)]
async fn a_crash_after_a_flush_triggered_cleanup_keeps_every_acknowledged_key() {
	let root = TempDir::new("vlog_gc_crash_flush").unwrap();
	let live = root.path().join("live");
	let tree = quiet_tree(&live).await;
	let mut model = two_rounds_in_two_tables(&tree).await;
	{
		let _pin = tree.core.inner.vlog.as_ref().unwrap().pin_files();
		compact(&tree);
	}
	let left = vlog_files(&live);
	assert!(left[0] < oldest_pointer(&tree), "obsolete files are left");

	// A crash image before the flush: the restart reclaims them.
	let first = root.path().join("first");
	copy_dir_all(&live, &first);
	let recovered = quiet_tree(&first).await;
	assert_model(&recovered, &model, "a crash with obsolete files left");
	assert_gc_state(&recovered, &first, &left, "the restart reclaims the files left");
	assert!(vlog_files(&first).len() < left.len(), "the restart deleted files");
	crash(&recovered);

	// The flush does it on the live tree.
	model.put(put_durable(&tree, 30..34, 3).await);
	flush_to_tables(&tree);
	let flushed = vlog_files(&live);
	assert_collected(&tree, &live, "after the flush");
	assert!(!flushed.contains(&left[0]), "the flush deleted the oldest file");

	let second = root.path().join("second");
	copy_dir_all(&live, &second);
	let recovered = quiet_tree(&second).await;
	assert_model(&recovered, &model, "a crash after the flush");
	assert_all_pointers_resolve(&recovered, "a crash after the flush");
	assert_eq!(vlog_files(&second), flushed, "the restart has nothing more to delete");
	crash(&recovered);
	crash(&tree);
}

/// Files that no record points to (a batch that was written to the value log and never logged)
/// and that are older than every pointer are reclaimed at startup, once a table points into the
/// value log, and the files that the replayed WAL points into are not.
#[test(tokio::test)]
async fn files_no_record_points_to_are_reclaimed_by_the_startup_cleanup() {
	let root = TempDir::new("vlog_gc_orphans").unwrap();
	let live = root.path().join("live");
	let tree = quiet_tree(&live).await;
	let vlog = tree.core.inner.vlog.as_ref().unwrap();
	// Eight values, two full files, in the log and in no record.
	for i in 0..8 {
		vlog.append(format!("orphan_{i}").as_bytes(), &big(i, 0)).unwrap();
	}
	vlog.sync().unwrap();
	assert_eq!(vlog_files(&live), vec![1, 2], "the orphans fill two files");

	// Real values in a table, flushed with a pin that defers the cleanup of the flush, and more
	// values that are in the WAL only.
	let mut model = Model::default();
	model.put(put_durable(&tree, 0..8, 1).await);
	{
		let _pin = vlog.pin_files();
		flush_to_tables(&tree);
	}
	model.put(put_durable(&tree, 10..14, 2).await);
	let before = vlog_files(&live);
	assert!(before.len() >= 5 && before[..2] == [1, 2], "the orphans are still there: {before:?}");
	let image = root.path().join("image");
	copy_dir_all(&live, &image);
	crash(&tree);

	let recovered = quiet_tree(&image).await;
	assert_model(&recovered, &model, "after the restart");
	assert_all_pointers_resolve(&recovered, "after the restart");
	let floor = assert_gc_state(&recovered, &image, &before, "after the restart");
	assert_eq!(floor, 3, "the first file of the table's values is the bound");
	assert_eq!(
		vlog_files(&image),
		before[2..].to_vec(),
		"the orphans are gone, the files of the table and of the WAL stay"
	);
	crash(&recovered);
}

/// While no table points into the value log, the files of a replayed memtable are all that is
/// pointed into, and nothing a record points to is lost, whatever else is on disk.
#[test(tokio::test)]
async fn values_that_only_the_wal_points_to_survive_a_restart_next_to_orphans() {
	let root = TempDir::new("vlog_gc_wal_only").unwrap();
	let live = root.path().join("live");
	let tree = quiet_tree(&live).await;
	let vlog = tree.core.inner.vlog.as_ref().unwrap();
	for i in 0..8 {
		vlog.append(format!("orphan_{i}").as_bytes(), &big(i, 0)).unwrap();
	}
	let mut model = Model::default();
	model.put(put_durable(&tree, 0..8, 1).await);
	let before = vlog_files(&live);
	let image = root.path().join("image");
	copy_dir_all(&live, &image);
	crash(&tree);

	let recovered = quiet_tree(&image).await;
	assert_model(&recovered, &model, "after the restart");
	assert_all_pointers_resolve(&recovered, "after the restart");
	let now = vlog_files(&image);
	for file in referenced_files(&recovered) {
		assert!(now.contains(&file), "file {file} is pointed into by the replayed WAL and gone");
	}
	assert!(now.iter().all(|f| before.contains(f)), "the restart created no file: {now:?}");
	crash(&recovered);
}

/// Commits that are larger than a memtable and go straight to level 0, as a bulk load makes
/// them: the value log is collected as the older rounds are compacted away, like any other.
#[test(tokio::test)]
#[ignore = "a commit written straight to level 0 lowers the bound of the empty active memtable, \
            which is never rotated while every commit goes straight to level 0: the bound stays at \
            the first such commit's file and no value-log file is ever deleted"]
async fn commits_that_go_straight_to_level_0_do_not_pin_the_value_log() {
	let dir = TempDir::new("vlog_gc_oversized").unwrap();
	let tree = quiet_tree_with(Options {
		max_memtable_size: 64 * 1024,
		..base_options(dir.path())
	})
	.await;
	let mut model = Model::default();
	for round in 1..=6 {
		// Eight values and a hundred small entries: too large for a memtable of 64 KiB.
		let mut entries: Vec<_> = (0..8).map(|i| (key_of(i), big(i, round))).collect();
		for i in 0..100 {
			entries.push((format!("pad_{i:04}").into_bytes(), format!("pad_{round}").into_bytes()));
		}
		commit(&tree, &entries, Durability::Eventual).await.unwrap();
		model.put(entries.into_iter().collect());
		assert!(
			tree.core.inner.active_memtable.read().unwrap().is_empty(),
			"round {round}: the commit went straight to a table"
		);
		if l0_tables(&tree) >= 2 {
			compact(&tree);
		}
	}
	let inner = &tree.core.inner;
	let bound = crate::lsm::vlog_floor(
		&inner.active_memtable,
		&inner.level_manifest,
		&inner.immutable_memtables,
	)
	.unwrap();
	assert_eq!(
		bound,
		oldest_pointer(&tree),
		"the bound of the cleanup is the oldest file that anything points into"
	);
	assert_collected(&tree, dir.path(), "after six rounds");
	assert_model(&tree, &model, "after six rounds");
	crash(&tree);
}

// ---------------------------------------------------------------------------
// 6. ids, the active file, many files
// ---------------------------------------------------------------------------

/// File ids only grow across reopens: a file created in a session has an id above every file
/// that ever existed, and the highest file, the active one, is never removed by a cleanup.
#[test(tokio::test)]
async fn file_ids_keep_growing_across_reopens_and_the_active_file_survives_every_cleanup() {
	let dir = TempDir::new("vlog_gc_ids").unwrap();
	let mut model = Model::default();
	let mut highest = 0u32;
	for round in 1..=4usize {
		let tree = open_with(Options {
			flush_on_close: true,
			..base_options(dir.path())
		});
		if round > 1 {
			assert_eq!(
				vlog_files(dir.path()).last().copied(),
				Some(highest),
				"the highest file is there"
			);
			assert_eq!(active_file(&tree), highest, "and it is the active one");
			let next = tree.core.inner.vlog.as_ref().unwrap().next_file_id.load(Ordering::SeqCst);
			assert_eq!(next, highest + 1, "the next id follows every id there was");
		}
		let known = vlog_files(dir.path());
		model.put(put(&tree, 0..16, round).await);
		flush_to_tables(&tree);
		let created: Vec<u32> =
			vlog_files(dir.path()).into_iter().filter(|f| !known.contains(f)).collect();
		assert!(created.len() >= 3, "round {round} made files: {created:?}");
		assert!(created.iter().all(|&f| f > highest), "new files {created:?} are above {highest}");
		if l0_tables(&tree) >= 2 {
			compact(&tree);
		}
		cleanup(&tree);
		highest = *vlog_files(dir.path()).last().unwrap();
		assert_eq!(active_file(&tree), highest, "the active file is the highest");
		assert_collected(&tree, dir.path(), &format!("round {round}"));
		assert_model(&tree, &model, &format!("round {round}"));
		tree.close().await.unwrap();
	}
	let tree = quiet_tree(dir.path()).await;
	assert_model(&tree, &model, "at the end");
	assert_all_pointers_resolve(&tree, "at the end");
	crash(&tree);
}

/// A compaction that drops the first round of 150 files deletes them all in one cleanup, and
/// nothing else. A bound above everything then deletes all but the active file, which the next
/// write goes on in, with a new id.
#[test(tokio::test)]
async fn one_cleanup_removes_a_hundred_files_and_never_the_active_one() {
	let dir = TempDir::new("vlog_gc_many").unwrap();
	let tree = quiet_tree_with(Options {
		// One value to a file.
		vlog_max_file_size: 1024,
		..base_options(dir.path())
	})
	.await;
	let mut model = Model::default();
	put_and_flush(&tree, &mut model, 0..150, 1).await;
	put_and_flush(&tree, &mut model, 0..150, 2).await;
	let before = vlog_files(dir.path());
	assert!(before.len() >= 300, "one file to a value: {}", before.len());

	compact(&tree);
	let floor = assert_gc_state(&tree, dir.path(), &before, "after the compaction");
	let after = vlog_files(dir.path());
	assert!(
		before.len() - after.len() >= 148,
		"round 1's files went at once: {} left",
		after.len()
	);
	assert!(floor > before[0]);
	assert_model(&tree, &model, "after the compaction");
	assert_all_pointers_resolve(&tree, "after the compaction");
	assert_no_deleted_file_held_open(dir.path(), "after the compaction");

	// The largest bound: everything but the active file goes.
	let active = active_file(&tree);
	tree.core.inner.vlog.as_ref().unwrap().cleanup_obsolete_files(u32::MAX).unwrap();
	assert_eq!(vlog_files(dir.path()), vec![active], "only the active file is left");
	let old = tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key_of(0));
	assert!(old.is_err(), "a value in a deleted file is an error: {old:?}");

	// The log goes on: the next value is in the active file or a new one, never an old id.
	let written = put(&tree, 200..204, 1).await;
	let now = vlog_files(dir.path());
	assert!(now.iter().all(|&f| f >= active), "no older id is back: {now:?}");
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	for (key, value) in &written {
		assert_eq!(txn.get(key).unwrap().as_deref(), Some(value.as_slice()));
	}
	crash(&tree);
}

// ---------------------------------------------------------------------------
// 7. a missing or truncated file
// ---------------------------------------------------------------------------

/// A database with 16 values in four files, in tables, closed. Returns the values and the
/// pointer of every key.
async fn closed_database_with_four_files(
	dir: &Path,
) -> (Contents, BTreeMap<Vec<u8>, ValuePointer>) {
	let tree = open_with(Options {
		flush_on_close: true,
		..base_options(dir)
	});
	let mut model = Model::default();
	put_and_flush(&tree, &mut model, 0..16, 1).await;
	let pointers: BTreeMap<_, _> = referenced(&tree).into_iter().collect();
	assert_eq!(pointers.len(), 16);
	tree.close().await.unwrap();
	(model.live, pointers)
}

/// A value-log file that a table points into is missing: the reads that need it fail with an
/// error, the others read, and no scan returns a value it cannot have.
#[test(tokio::test)]
async fn a_missing_value_log_file_fails_the_reads_that_need_it_and_only_those() {
	let dir = TempDir::new("vlog_gc_missing").unwrap();
	let (contents, pointers) = closed_database_with_four_files(dir.path()).await;
	let files: BTreeSet<u32> = pointers.values().map(|p| p.file_id).collect();
	let victim = *files.iter().nth(1).unwrap();
	std::fs::remove_file(vlog_file_path(dir.path(), victim)).unwrap();

	let tree = quiet_tree(dir.path()).await;
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let (mut failed, mut read) = (0, 0);
	for (key, value) in &contents {
		let result = txn.get(key);
		if pointers[key].file_id == victim {
			let error = result.expect_err("a value in a missing file must not read");
			assert!(error.to_string().contains("VLog"), "the error names the value log: {error}");
			failed += 1;
		} else {
			assert_eq!(result.unwrap().as_deref(), Some(value.as_slice()), "an intact file reads");
			read += 1;
		}
	}
	assert!(failed >= 3 && read >= 8, "some reads failed ({failed}) and some did not ({read})");
	let scan = txn.iter().and_then(|mut it| collect_transaction_all(&mut it));
	assert!(scan.is_err(), "a scan over a missing value is an error, not a short result");
	drop(txn);
	crash(&tree);
}

/// A file cut in the middle of an entry: the entries before the cut read, the others are
/// errors, and not one read returns bytes that were not written.
async fn truncated_file_case(level: VLogChecksumLevel) {
	let dir = TempDir::new("vlog_gc_truncated").unwrap();
	let (contents, pointers) = closed_database_with_four_files(dir.path()).await;
	let files: BTreeSet<u32> = pointers.values().map(|p| p.file_id).collect();
	let victim = *files.iter().nth(1).unwrap();

	// Cut the second entry of the file in the middle of its value.
	let mut entries: Vec<&ValuePointer> =
		pointers.values().filter(|p| p.file_id == victim).collect();
	entries.sort_by_key(|p| p.offset);
	let cut_entry = entries[1];
	let cut = cut_entry.offset + 8 + cut_entry.key_size as u64 + cut_entry.value_size as u64 / 2;
	std::fs::OpenOptions::new()
		.write(true)
		.open(vlog_file_path(dir.path(), victim))
		.unwrap()
		.set_len(cut)
		.unwrap();

	let tree = quiet_tree_with(Options {
		vlog_checksum_verification: level,
		..base_options(dir.path())
	})
	.await;
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let (mut failed, mut read) = (0, 0);
	for (key, value) in &contents {
		let pointer = &pointers[key];
		let end = pointer.offset + pointer.total_entry_size();
		match txn.get(key) {
			Ok(Some(got)) => {
				assert!(
					&got == value,
					"{}: a read returned bytes that were not written: {} bytes, {} written, the first \
					 difference at byte {:?} (file {}, entry {}..{end}, file cut at {cut})",
					String::from_utf8_lossy(key),
					got.len(),
					value.len(),
					got.iter().zip(value.iter()).position(|(a, b)| a != b),
					pointer.file_id,
					pointer.offset
				);
				assert!(pointer.file_id != victim || end <= cut, "an entry beyond the cut read");
				read += 1;
			}
			Ok(None) => panic!("{}: missing", String::from_utf8_lossy(key)),
			Err(_) => {
				assert!(pointer.file_id == victim && end > cut, "a read of an intact entry failed");
				failed += 1;
			}
		}
	}
	assert!(failed >= 2, "the entries at and beyond the cut fail: {failed}");
	assert!(read >= 10, "the other entries read: {read}");
	drop(txn);
	crash(&tree);
}

#[test(tokio::test)]
async fn a_truncated_value_log_file_fails_the_reads_beyond_the_cut_with_checksums() {
	truncated_file_case(VLogChecksumLevel::Full).await;
}

/// The default level does not verify checksums, and a read of an entry that is cut short by the
/// end of its file takes the bytes that are missing from the buffer's zeros.
#[test(tokio::test)]
#[ignore = "a value-log entry cut short by the end of its file reads as the value with a zeroed \
            tail when checksums are not verified: the length that read_at returned is not checked"]
async fn a_truncated_value_log_file_fails_the_reads_beyond_the_cut_without_checksums() {
	truncated_file_case(VLogChecksumLevel::Disabled).await;
}

// ---------------------------------------------------------------------------
// 8. space is reclaimed
// ---------------------------------------------------------------------------

/// Sixteen rounds over the same forty keys, each flushed and compacted as soon as two tables
/// exist: the directory never takes more than a small multiple of the live data, however much
/// has been written, and the end state is the live data and little more.
#[test(tokio::test)]
async fn the_value_log_stays_within_a_small_multiple_of_the_live_data_under_churn() {
	const KEYS: usize = 40;
	const ROUNDS: usize = 16;
	let dir = TempDir::new("vlog_gc_space").unwrap();
	let tree = quiet_tree(dir.path()).await;
	let mut model = Model::default();
	let live = (KEYS * VALUE_LEN) as u64;
	let file = 4096u64;
	let mut peak = 0u64;
	for round in 1..=ROUNDS {
		put_and_flush(&tree, &mut model, 0..KEYS, round).await;
		if l0_tables(&tree) >= 2 {
			compact(&tree);
		}
		peak = peak.max(vlog_bytes(dir.path()));
	}
	let files = vlog_files(dir.path());
	let highest = *files.last().unwrap() as usize;
	assert!(
		highest >= 4 * files.len(),
		"ids went much further than the files left: {highest} vs {}",
		files.len()
	);
	assert!(peak <= 3 * live + 3 * file, "the log reached {peak} bytes for {live} bytes of data");
	assert!(
		vlog_bytes(dir.path()) <= 3 * live + 3 * file,
		"the log holds {} bytes for {live} bytes of live data",
		vlog_bytes(dir.path())
	);
	assert_collected(&tree, dir.path(), "at the end");
	assert_model(&tree, &model, "at the end");
	crash(&tree);
}

/// Deleting every key and compacting leaves no value that anything can read: the value log
/// shrinks to the active file.
#[test(tokio::test)]
#[ignore = "when every key is deleted no table points into the value log, the bound is 0 and the \
            cleanup does nothing: the files of the deleted values are never removed"]
async fn deleting_every_value_lets_the_value_log_shrink_to_the_active_file() {
	let dir = TempDir::new("vlog_gc_delete_all").unwrap();
	let tree = quiet_tree(dir.path()).await;
	let mut model = Model::default();
	put_and_flush(&tree, &mut model, 0..40, 1).await;
	let before = vlog_files(dir.path());
	assert!(before.len() >= 9);
	delete(&tree, 0..40).await;
	model.delete(0..40);
	flush_to_tables(&tree);
	compact(&tree);
	assert!(referenced(&tree).is_empty(), "nothing points into the value log any more");
	let files = vlog_files(dir.path());
	assert_eq!(
		files,
		vec![active_file(&tree)],
		"the files of the deleted values are removed; {} of {} left",
		files.len(),
		before.len()
	);
	assert_model(&tree, &model, "after deleting everything");
	crash(&tree);
}
