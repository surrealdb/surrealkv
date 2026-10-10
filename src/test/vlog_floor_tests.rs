//! A value-log file is deleted once nothing points into it, and a pointer lives in more places
//! than the tables: in the active memtable, in every immutable memtable still waiting for its
//! flush, and in the WAL records that recovery replays into a memtable. A table can point only
//! into newer files than a memtable that is not flushed yet (an oversized batch is written to a
//! table of its own), so a cleanup that works from the tables alone deletes files the memtable
//! still reads. These tests keep those files through every cleanup (a compaction, a flush, a
//! restart), and check the bound the cleanup works from, the order it reads and locks in, and the
//! cleanups themselves while commits, rotations, flushes and compactions run.
//!
//! The table past a queued memtable is registered the way an oversized commit registers it,
//! without the commit, so that the tests do not depend on what a commit does with the queue.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, TryLockError};
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;

use crate::batch::Batch;
use crate::compaction::leveled::Strategy;
use crate::levels::write_manifest_to_disk;
use crate::lsm::{cleanup_vlog, vlog_floor, CompactionOperations, FlushHook};
use crate::memtable::MemTable;
use crate::vlog::{ValueLocation, ValuePointer};
use crate::{Durability, InternalKeyKind, Mode, Options, Tree};

/// How long a test waits for a flush it expects to reach the hook, or for a background task.
const LONG: Duration = Duration::from_secs(30);

/// How long a test waits for the thread that computes the bound to reach, or to give back, a lock
/// the test holds. A thread that does neither fails the test after this long, so a deadlock fails
/// it instead of hanging.
const AT_THE_LOCK: Duration = Duration::from_secs(10);

/// Small enough that a batch of a hundred entries cannot go to a memtable.
const SMALL_MEMTABLE: usize = 64 * 1024;

/// Large enough that nothing rotates unless a test rotates it.
const NEVER_ROTATING: usize = 16 * 1024 * 1024;

type Contents = BTreeMap<Vec<u8>, Vec<u8>>;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn options(path: &Path, max_memtable_size: usize) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		max_memtable_size,
		enable_vlog: true,
		vlog_value_threshold: 64,
		// Two values to a file, so a few writes make several files.
		vlog_max_file_size: 4096,
		// No background compaction: the tests compact when they want to.
		level0_max_files: 100,
		l0_stall_threshold: 1000,
		// Tests that queue immutable memtables by hand must not stall their own writes.
		memtable_stall_threshold: 1000,
		// A reopen after a close then replays the WAL instead of finding everything in tables.
		flush_on_close: false,
		..Default::default()
	})
}

fn open(path: &Path, max_memtable_size: usize) -> Arc<Tree> {
	Arc::new(Tree::new(options(path, max_memtable_size)).unwrap())
}

async fn stop_background_tasks(tree: &Tree) {
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
}

/// A tree whose background tasks are stopped: nothing is flushed or compacted unless the test
/// does it, and it is never closed (a crash).
async fn quiet_tree(path: &Path) -> Arc<Tree> {
	let tree = open(path, SMALL_MEMTABLE);
	stop_background_tasks(&tree).await;
	tree.core.is_closed.store(true, Ordering::SeqCst);
	tree
}

/// Ends a tree the way a crash does.
fn crash(tree: &Tree) {
	tree.core.is_closed.store(true, Ordering::SeqCst);
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
}

fn key_of(i: usize) -> Vec<u8> {
	format!("key_{i:04}").into_bytes()
}

/// A value that goes to the value log, and that no other `(i, round)` shares.
fn big(i: usize, round: usize) -> Vec<u8> {
	let mut v = format!("big_{i:04}_{round:04}_").into_bytes();
	v.resize(3000, b'x');
	v
}

/// A value that stays in the memtable.
fn small(i: usize, round: usize) -> Vec<u8> {
	format!("small_{i:04}_{round:04}").into_bytes()
}

/// An encoded pointer to `file_id`.
fn pointer(file_id: u32) -> Vec<u8> {
	ValueLocation::with_pointer(ValuePointer::new(file_id, 31, 8, 100, 0xDEAD)).encode()
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

/// Commits `keys` with a value in the value log each, and returns what was written.
async fn put(tree: &Tree, keys: Range<usize>, round: usize) -> Contents {
	let entries: Vec<_> = keys.map(|i| (key_of(i), big(i, round))).collect();
	commit(tree, &entries, Durability::Immediate).await.unwrap();
	entries.into_iter().collect()
}

/// Commits one batch that cannot go to a memtable, so it is written straight to a table: `keys`
/// with a value in the value log each, and enough small entries to make it that large.
async fn put_oversized(tree: &Tree, keys: &[usize], round: usize) -> Contents {
	let mut entries: Vec<_> = keys.iter().map(|&i| (key_of(i), big(i, round))).collect();
	for i in 0..100 {
		entries.push((format!("pad_{i:04}").into_bytes(), small(i, round)));
	}
	commit(tree, &entries, Durability::Immediate).await.unwrap();
	entries.into_iter().collect()
}

fn get(tree: &Tree, key: &[u8]) -> crate::Result<Option<Vec<u8>>> {
	tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key)
}

/// Every key of `expected` read back one by one, with what is wrong for each key that does not.
fn mismatches(tree: &Tree, expected: &Contents) -> Vec<String> {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let mut wrong = Vec::new();
	for (key, value) in expected {
		match txn.get(key) {
			Ok(Some(got)) if &got == value => {}
			Ok(Some(got)) => wrong.push(format!(
				"{}: {} bytes, not the value written",
				String::from_utf8_lossy(key),
				got.len()
			)),
			Ok(None) => wrong.push(format!("{}: missing", String::from_utf8_lossy(key))),
			Err(e) => wrong.push(format!("{}: {e}", String::from_utf8_lossy(key))),
		}
	}
	wrong
}

fn assert_reads(tree: &Tree, expected: &Contents, what: &str) {
	let wrong = mismatches(tree, expected);
	assert!(
		wrong.is_empty(),
		"{what}: {} of {} values do not read back, for example {:?}",
		wrong.len(),
		expected.len(),
		&wrong[..wrong.len().min(3)]
	);
}

fn immutables(tree: &Tree) -> usize {
	tree.core.inner.immutable_count()
}

fn table_ids(tree: &Tree) -> Vec<u64> {
	let mut ids: Vec<u64> =
		tree.core.inner.level_manifest.read().unwrap().iter().map(|t| t.id).collect();
	ids.sort_unstable();
	ids
}

/// What the tables alone say: the smallest file any of them points into, 0 for none.
fn tables_min(tree: &Tree) -> u32 {
	tree.core.inner.level_manifest.read().unwrap().min_oldest_vlog_file_id()
}

/// The bound a cleanup works from.
fn floor(tree: &Tree) -> u32 {
	let inner = &tree.core.inner;
	vlog_floor(&inner.active_memtable, &inner.level_manifest, &inner.immutable_memtables).unwrap()
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

/// Rotates the active memtable into the queue and flushes the queue.
fn flush_to_tables(tree: &Tree) {
	tree.core.inner.rotate_memtable().unwrap();
	tree.core.inner.flush_all_immutables_sync().unwrap();
}

/// Merges the tables of level 0, which the tree itself would not do until there are many more.
fn compact(tree: &Tree) {
	let opts = Arc::new(Options {
		level0_max_files: 2,
		..Default::default()
	});
	tree.compact(Arc::new(Strategy::from_options(opts))).unwrap();
}

/// Waits for `condition`, which a background task makes true.
fn wait_until(what: &str, condition: impl Fn() -> bool) {
	let start = Instant::now();
	while !condition() {
		assert!(start.elapsed() < LONG, "timed out waiting for {what}");
		std::thread::sleep(Duration::from_millis(5));
	}
}

/// Pushes the value log on to a file after `file`, with values nothing points to.
fn roll_past(tree: &Tree, file: u32) {
	let vlog = tree.core.inner.vlog.as_ref().unwrap();
	let mut i = 0;
	while vlog.active_writer_id.load(Ordering::SeqCst) <= file + 1 {
		vlog.append(b"filler", &big(i, 99)).unwrap();
		i += 1;
	}
}

/// Registers `batch` as a table of its own, as an oversized commit does, but claims no WAL
/// segment: the table's `log_number` advance is undone, so no unflushed record is skipped by a
/// replay.
fn register_table(tree: &Tree, batch: &Batch) {
	let inner = &tree.core.inner;
	let log_number = inner.level_manifest.read().unwrap().get_log_number();
	let table_id = inner.level_manifest.read().unwrap().next_table_id();
	inner
		.write_batch_direct_to_l0_sst(batch, table_id, log_number.saturating_sub(1), u64::MAX)
		.unwrap();
	let mut manifest = inner.level_manifest.write().unwrap();
	if manifest.get_log_number() != log_number {
		manifest.log_number = log_number;
		write_manifest_to_disk(&manifest).unwrap();
	}
}

/// Registers a table whose only value is in a file written after every file there is, as an
/// oversized batch makes one without the memtables having been flushed, and returns that file's
/// id.
fn table_past_every_file(tree: &Tree) -> u32 {
	let vlog = tree.core.inner.vlog.as_ref().unwrap();
	let mut pointer = None;
	for i in 0..6 {
		pointer = Some(vlog.append(b"direct", &big(i, 9)).unwrap());
	}
	let pointer = pointer.unwrap();
	let mut batch = Batch::new(1000);
	let value = ValueLocation::with_pointer(pointer.clone()).encode();
	batch.add_record(InternalKeyKind::Set, b"direct".to_vec(), Some(value), 0).unwrap();
	register_table(tree, &batch);
	pointer.file_id
}

/// Registers a table of `keys` with the values of `round`, each in the value log after every
/// file there is, as the table of an oversized commit is, and makes it visible, without the
/// memtables being flushed and without a commit: it does not depend on what a commit waits for.
/// The keys must be in no memtable, since a table is read after the memtables. Nothing is logged
/// to the WAL.
fn register_table_past_the_memtables(tree: &Tree, keys: Range<usize>, round: usize) -> Contents {
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
	register_table(tree, &batch);
	inner.visible_seq_num.fetch_max(seq + count - 1, Ordering::SeqCst);
	written
}

/// Parks the first flush that reaches the flush hook (its table is on disk and the manifest does
/// not list it yet).
struct Gate {
	reached: mpsc::Receiver<u64>,
	release: mpsc::Sender<()>,
}

impl Gate {
	fn install(tree: &Tree) -> Self {
		let (reached_tx, reached) = mpsc::channel();
		let (release, release_rx) = mpsc::channel();
		let release_rx = Mutex::new(release_rx);
		let parked = AtomicBool::new(false);
		let hook: FlushHook = Arc::new(move |table_id| {
			let _ = reached_tx.send(table_id);
			if !parked.swap(true, Ordering::SeqCst) {
				// Bounded, so a broken test fails instead of hanging.
				let _ = release_rx.lock().unwrap().recv_timeout(LONG);
			}
			Ok(())
		});
		*tree.core.inner.flush_hook.lock() = Some(hook);
		Self {
			reached,
			release,
		}
	}

	/// The id of the next flush to reach the hook, if one does within `LONG`.
	fn reached(&self) -> Option<u64> {
		self.reached.recv_timeout(LONG).ok()
	}

	fn release(&self, tree: &Tree) {
		let _ = self.release.send(());
		*tree.core.inner.flush_hook.lock() = None;
	}
}

/// Two rounds of values in two tables, so that merging them drops the first round's pointers,
/// which are in the oldest files. Returns what was written, as the merged table holds it.
async fn two_rounds_in_two_tables(tree: &Tree) -> Contents {
	put(tree, 0..4, 1).await;
	flush_to_tables(tree);
	let expected = put(tree, 0..4, 2).await;
	flush_to_tables(tree);
	assert_eq!(table_ids(tree).len(), 2, "one table for each round");
	expected
}

/// Queues a memtable that holds one inline value and no pointer.
async fn queue_a_memtable_without_pointers(tree: &Tree) -> Contents {
	commit(tree, &[(b"inline".to_vec(), b"value".to_vec())], Durability::Immediate).await.unwrap();
	tree.core.inner.rotate_memtable().unwrap();
	Contents::from([(b"inline".to_vec(), b"value".to_vec())])
}

/// Queues a memtable whose values are in the value log.
async fn queue_a_memtable_with_pointers(tree: &Tree) -> Contents {
	let written = put(tree, 100..104, 1).await;
	tree.core.inner.rotate_memtable().unwrap();
	written
}

// ---------------------------------------------------------------------------
// what a memtable records
// ---------------------------------------------------------------------------

/// What does not point into the value log does not lower the bound: inline values, deletes and
/// the end key of a range delete, which is not a value at all.
#[test]
fn values_that_are_not_pointers_leave_the_memtable_bound_alone() {
	let batch = |entries: Vec<(InternalKeyKind, &str, Option<Vec<u8>>)>| {
		let mut batch = Batch::new(1);
		for (kind, key, value) in entries {
			batch.add_record(kind, key.as_bytes().to_vec(), value, 0).unwrap();
		}
		batch
	};
	// An end key that reads as a pointer to file 2: a pointer's first byte and its length.
	let mut looks_like_a_pointer = pointer(2);
	looks_like_a_pointer[0] = 1;

	let memtable = MemTable::new(NEVER_ROTATING);
	assert_eq!(memtable.min_vlog_file_id(), None);
	memtable
		.add(&batch(vec![
			(
				InternalKeyKind::Set,
				"inline",
				Some(ValueLocation::with_inline_value(vec![1; 8]).encode()),
			),
			(InternalKeyKind::Set, "empty", Some(Vec::new())),
			(InternalKeyKind::Delete, "deleted", None),
			(InternalKeyKind::RangeDelete, "a", Some(looks_like_a_pointer)),
		]))
		.unwrap();
	assert_eq!(memtable.min_vlog_file_id(), None, "nothing in the memtable is a pointer");

	memtable.add(&batch(vec![(InternalKeyKind::Set, "far", Some(pointer(9)))])).unwrap();
	assert_eq!(memtable.min_vlog_file_id(), Some(9));
	memtable
		.add(&batch(vec![
			(InternalKeyKind::Set, "near", Some(pointer(4))),
			(InternalKeyKind::Set, "other", Some(pointer(6))),
		]))
		.unwrap();
	assert_eq!(memtable.min_vlog_file_id(), Some(4));
	memtable.add(&batch(vec![(InternalKeyKind::Set, "later", Some(pointer(7)))])).unwrap();
	assert_eq!(memtable.min_vlog_file_id(), Some(4), "a later, larger file does not raise it");
}

/// The commit path moves a batch into the memtable instead of cloning it, and the memtable
/// accounts for what the batch points into either way, wherever the smallest file is in the
/// batch (the value log writes them in increasing order, so a test whose batches do the same
/// cannot tell the smallest from the first). A batch that is refused leaves the bound alone.
#[test]
fn a_batch_moved_into_a_memtable_is_accounted_for_and_a_refused_one_is_not() {
	let batch = || {
		let mut batch = Batch::new(1);
		for (key, file_id) in [("far", 9), ("near", 4), ("other", 6)] {
			batch
				.add_record(
					InternalKeyKind::Set,
					key.as_bytes().to_vec(),
					Some(pointer(file_id)),
					0,
				)
				.unwrap();
		}
		batch
	};

	let cloned = MemTable::new(NEVER_ROTATING);
	cloned.add(&batch()).unwrap();
	assert_eq!(cloned.min_vlog_file_id(), Some(4), "add");

	let moved = MemTable::new(NEVER_ROTATING);
	let mut moving = batch();
	let needed = moving.memtable_size_estimate();
	moved.add_owned(&mut moving, needed).unwrap();
	assert_eq!(moved.min_vlog_file_id(), Some(4), "add_owned");

	let mut refused = Batch::new(10);
	refused.add_record(InternalKeyKind::Set, b"c".to_vec(), Some(pointer(1)), 0).unwrap();
	let full = MemTable::new(8 * 1024);
	let too_much = refused.memtable_size_estimate() * 100;
	assert!(full.add_owned(&mut refused, too_much).is_err(), "the batch does not fit");
	assert_eq!(full.min_vlog_file_id(), None, "a batch that was refused is not accounted for");
}

/// Adds batches of one pointer each, every one into an older file than the one before, while the
/// calling thread reads the one that is going in. Whenever that value can be read, the memtable
/// must already say it points into a file that old: the bound is read by another thread, and the
/// value is in the memtable for a reader as soon as it can be read.
fn pointers_are_covered_as_soon_as_they_can_be_read(add: fn(&MemTable, Batch)) {
	const BATCHES: u32 = 20_000;
	let file_of = |i: u32| BATCHES + 10 - i;

	let memtable = MemTable::new(NEVER_ROTATING);
	let done = AtomicU32::new(0);
	let uncovered = std::thread::scope(|scope| {
		let writer = scope.spawn(|| {
			for i in 0..BATCHES {
				let mut batch = Batch::new(u64::from(i) + 1);
				batch
					.add_record(
						InternalKeyKind::Set,
						key_of(i as usize),
						Some(pointer(file_of(i))),
						0,
					)
					.unwrap();
				add(&memtable, batch);
				done.store(i + 1, Ordering::SeqCst);
			}
		});
		let mut uncovered = None;
		while done.load(Ordering::SeqCst) < BATCHES {
			// The batch after the last that was added: the one that is going in.
			let next = done.load(Ordering::SeqCst);
			if next < BATCHES && memtable.get(&key_of(next as usize), None).is_some() {
				let covered = memtable.min_vlog_file_id().is_some_and(|min| min <= file_of(next));
				if !covered {
					uncovered = Some((next, memtable.min_vlog_file_id()));
					break;
				}
			}
			std::hint::spin_loop();
		}
		writer.join().unwrap();
		uncovered
	});
	assert_eq!(
		uncovered, None,
		"a value was readable while the memtable's minimum was above the file it points into"
	);
}

#[test]
fn a_cloned_batch_is_accounted_for_before_its_values_can_be_read() {
	pointers_are_covered_as_soon_as_they_can_be_read(|memtable, batch| {
		memtable.add(&batch).unwrap();
	});
}

#[test]
fn a_moved_batch_is_accounted_for_before_its_values_can_be_read() {
	pointers_are_covered_as_soon_as_they_can_be_read(|memtable, mut batch| {
		let needed = batch.memtable_size_estimate();
		memtable.add_owned(&mut batch, needed).unwrap();
	});
}

// ---------------------------------------------------------------------------
// the bound
// ---------------------------------------------------------------------------

/// The bound is the smallest file any table or any memtable points into, whichever has it, and
/// nothing is deleted while no table points into the value log.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn the_bound_is_the_smallest_file_of_any_table_or_memtable() {
	let dir = TempDir::new("vlog_floor_bound").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let inner = &tree.core.inner;
	let active_min = || inner.active_memtable.read().unwrap().min_vlog_file_id();
	let queued_min = || {
		let queue = inner.immutable_memtables.read().unwrap();
		queue.iter().filter_map(|entry| entry.memtable.min_vlog_file_id()).min()
	};

	assert_eq!(floor(&tree), 0, "nothing points into the value log");

	// Memtables alone.
	put(&tree, 0..4, 1).await;
	let first = vlog_files(dir.path())[0];
	assert_eq!(active_min(), Some(first));
	assert_eq!(floor(&tree), 0, "an active memtable only: no table points into the value log");
	inner.rotate_memtable().unwrap();
	assert_eq!((active_min(), queued_min()), (None, Some(first)));
	assert_eq!(floor(&tree), 0, "an immutable memtable only");
	put(&tree, 10..14, 1).await;
	assert!(active_min().unwrap() > first);
	assert_eq!(floor(&tree), 0, "an active and an immutable memtable");

	// A table past both: the bound is the older memtable's.
	let table = table_past_every_file(&tree);
	assert_eq!(tables_min(&tree), table);
	assert!(table > active_min().unwrap());
	assert_eq!(floor(&tree), first, "a table past an active and an immutable memtable");

	// Tables alone.
	flush_to_tables(&tree);
	assert_eq!((active_min(), queued_min()), (None, None));
	assert_eq!(tables_min(&tree), first);
	assert_eq!(floor(&tree), first, "tables only");

	// A memtable whose files are newer than the tables' does not raise the bound.
	put(&tree, 20..24, 1).await;
	assert!(active_min().unwrap() > first);
	assert_eq!(floor(&tree), first, "a table below a memtable");
	tree.close().await.unwrap();
}

/// The active memtable is below a table too when a table is written without sealing it, which
/// the commit path does not do today: the bound does not rely on that.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn the_bound_stays_below_the_active_memtable_when_a_table_points_past_it() {
	let dir = TempDir::new("vlog_floor_active").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	put(&tree, 0..4, 1).await;
	let first = vlog_files(dir.path())[0];
	let table = table_past_every_file(&tree);

	assert!(tables_min(&tree) > first, "the table points past the active memtable");
	assert_eq!(tables_min(&tree), table);
	assert_eq!(floor(&tree), first, "the bound is the active memtable's");
	tree.close().await.unwrap();
}

/// A queued memtable below a table: the bound is the memtable's, which is lower than what the
/// tables say, until its table is in the manifest, and after that it is the table's.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn the_bound_stays_below_a_queued_memtable_until_its_table_is_registered() {
	let dir = TempDir::new("vlog_floor_queued").unwrap();
	let tree = quiet_tree(dir.path()).await;
	put(&tree, 100..104, 1).await;
	let first = vlog_files(dir.path())[0];
	tree.core.inner.rotate_memtable().unwrap();
	let gate = Gate::install(&tree);
	let flusher = {
		let tree = Arc::clone(&tree);
		std::thread::spawn(move || tree.core.inner.flush_all_immutables_sync())
	};
	assert!(gate.reached().is_some(), "the flush of the memtable wrote its table");
	register_table_past_the_memtables(&tree, 0..4, 2);
	assert_eq!(table_ids(&tree).len(), 1, "the memtable's table is on disk, not listed");
	assert!(tables_min(&tree) > first, "the table points past the memtable");
	assert_eq!(floor(&tree), first, "the memtable is queued, its flush is not done");

	gate.release(&tree);
	flusher.join().unwrap().unwrap();
	assert_eq!(table_ids(&tree).len(), 2);
	assert_eq!(floor(&tree), first, "the memtable's table points into its files");
	crash(&tree);
}

// ---------------------------------------------------------------------------
// the order the bound reads and locks in
// ---------------------------------------------------------------------------

/// A tree whose active memtable points into the oldest file and a table that points past it, and
/// the oldest file's id: the bound is that file's, and is the table's if the memtable is missed.
async fn memtable_below_a_table(dir: &Path) -> (Arc<Tree>, u32) {
	let tree = open(dir, NEVER_ROTATING);
	put(&tree, 0..4, 1).await;
	let first = vlog_files(dir)[0];
	assert!(table_past_every_file(&tree) > first);
	assert_eq!(floor(&tree), first, "the bound with nothing in flight");
	(tree, first)
}

/// Runs `vlog_floor` on a thread of its own.
fn start_floor(tree: &Tree) -> std::thread::JoinHandle<u32> {
	let inner = &tree.core.inner;
	let active = Arc::clone(&inner.active_memtable);
	let manifest = Arc::clone(&inner.level_manifest);
	let queue = Arc::clone(&inner.immutable_memtables);
	std::thread::spawn(move || vlog_floor(&active, &manifest, &queue).unwrap())
}

/// Waits until `lock` is held for reading, which a try for writing finds out: it is refused.
fn wait_until_read_locked<T>(lock: &std::sync::RwLock<T>, why: &str) {
	let start = Instant::now();
	loop {
		match lock.try_write() {
			Ok(guard) => {
				drop(guard);
				assert!(start.elapsed() < AT_THE_LOCK, "{why}");
				std::thread::sleep(Duration::from_millis(5));
			}
			Err(TryLockError::WouldBlock) => return,
			Err(TryLockError::Poisoned(_)) => panic!("poisoned"),
		}
	}
}

/// Gives the thread that computes the bound time to take every lock it can, whichever order it
/// takes them in, then waits until `lock` can be written. A bound that holds it fails the test
/// after `AT_THE_LOCK`. If the machine is so busy that the thread has not got that far, the test
/// does not see the order, it does not fail.
fn wait_until_writable<T>(lock: &std::sync::RwLock<T>, why: &str) {
	std::thread::sleep(Duration::from_millis(200));
	let start = Instant::now();
	loop {
		match lock.try_write() {
			Ok(guard) => {
				drop(guard);
				return;
			}
			Err(TryLockError::WouldBlock) => {
				assert!(start.elapsed() < AT_THE_LOCK, "{why}");
				std::thread::sleep(Duration::from_millis(5));
			}
			Err(TryLockError::Poisoned(_)) => panic!("poisoned"),
		}
	}
}

/// The bound holds the active slot for reading from the start to the end. A rotation, which needs
/// the slot exclusively, cannot move the memtable between the slot and the queue while the bound
/// is read, so the memtable is counted once whichever the bound reads first. The test holds the
/// manifest, so the bound is still waiting for it, and finds the slot read-locked.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn the_bound_holds_the_active_slot_until_it_has_read_everything() {
	let dir = TempDir::new("vlog_floor_slot").unwrap();
	let (tree, first) = memtable_below_a_table(dir.path()).await;
	let inner = &tree.core.inner;

	let manifest = inner.level_manifest.write().unwrap();
	let reader = start_floor(&tree);
	wait_until_read_locked(
		&inner.active_memtable,
		"the bound does not hold the active slot while it waits for the manifest",
	);
	drop(manifest);
	assert_eq!(reader.join().unwrap(), first);
	tree.close().await.unwrap();
}

/// The bound holds the manifest while it waits for the queue. A flush registers its table and
/// takes its memtable out of the queue while it holds both, so a bound that reads one, lets go,
/// and reads the other can read the manifest before the table is in it and the queue after the
/// memtable is out, and miss both.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn the_bound_holds_the_manifest_while_it_waits_for_the_queue() {
	let dir = TempDir::new("vlog_floor_pair").unwrap();
	let (tree, first) = memtable_below_a_table(dir.path()).await;
	let inner = &tree.core.inner;

	let queue = inner.immutable_memtables.write().unwrap();
	let reader = start_floor(&tree);
	wait_until_read_locked(
		&inner.level_manifest,
		"the bound lets go of the manifest before it has the queue",
	);
	drop(queue);
	assert_eq!(reader.join().unwrap(), first);
	tree.close().await.unwrap();
}

/// The bound takes the manifest before the queue, the order a flush takes them in. A bound that
/// holds the queue and then waits for the manifest deadlocks with a flush.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn the_bound_takes_no_lock_of_the_queue_while_it_waits_for_the_manifest() {
	let dir = TempDir::new("vlog_floor_lock_order").unwrap();
	let (tree, first) = memtable_below_a_table(dir.path()).await;
	let inner = &tree.core.inner;

	let manifest = inner.level_manifest.write().unwrap();
	let reader = start_floor(&tree);
	wait_until_writable(
		&inner.immutable_memtables,
		"the bound holds the queue while it waits for the manifest, against the order a flush \
		 takes them in",
	);
	drop(manifest);
	assert_eq!(reader.join().unwrap(), first);
	tree.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// a table past a memtable that is not flushed
// ---------------------------------------------------------------------------

/// A compaction drops the values a table written past a queued memtable overwrote, which are in
/// the oldest files. The memtable's files are older than that table's, and have to stay, in the
/// tree and in a crash image that replays the memtable from the WAL (where the memtable is the
/// one the restart keeps active, so the startup cleanup needs it too).
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_compaction_keeps_the_files_of_a_queued_memtable_below_a_table_past_it() {
	let dir = TempDir::new("vlog_floor_queued_compaction").unwrap();
	let image = TempDir::new("vlog_floor_queued_compaction_image").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);

	let mut expected = put(&tree, 0..4, 1).await;
	flush_to_tables(&tree);
	expected.extend(put(&tree, 100..104, 1).await);
	tree.core.inner.rotate_memtable().unwrap();
	assert_eq!(immutables(&tree), 1, "the memtable is queued, nothing flushes it");
	let memtable_files = tables_min(&tree) + 1;
	expected.extend(register_table_past_the_memtables(&tree, 0..4, 2));
	assert_eq!(table_ids(&tree).len(), 2);

	// The merge drops the first round's pointers: the table past the memtable is all that is left.
	compact(&tree);
	assert!(tables_min(&tree) > memtable_files, "the table points past the memtable's files");
	assert_reads(&tree, &expected, "after the compaction");

	let crashed = image.path().join("crashed");
	copy_dir_all(dir.path(), &crashed);
	let reopened = open(&crashed, NEVER_ROTATING);
	assert_reads(&reopened, &expected, "reopened after a crash");
	reopened.close().await.unwrap();

	tree.core.inner.flush_all_immutables_sync().unwrap();
	assert_reads(&tree, &expected, "after the memtable was flushed");
	tree.close().await.unwrap();
}

/// Two memtables are queued, and a compaction drops the values of the only table that points
/// into older files than the table written past them. A bound that takes the oldest queued
/// memtable for all of them, or the newest, misses the one with the pointers when the other has
/// none.
async fn compaction_with_two_queued_memtables(pointers_first: bool) {
	let dir = TempDir::new("vlog_floor_two_queued").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);

	let mut expected = put(&tree, 0..4, 1).await;
	flush_to_tables(&tree);
	if pointers_first {
		expected.extend(queue_a_memtable_with_pointers(&tree).await);
		expected.extend(queue_a_memtable_without_pointers(&tree).await);
	} else {
		expected.extend(queue_a_memtable_without_pointers(&tree).await);
		expected.extend(queue_a_memtable_with_pointers(&tree).await);
	}
	assert_eq!(immutables(&tree), 2);
	let before_the_memtables = tables_min(&tree);
	expected.extend(register_table_past_the_memtables(&tree, 0..4, 2));

	compact(&tree);
	assert!(
		tables_min(&tree) > before_the_memtables,
		"the merge dropped the first table's pointers"
	);
	assert_reads(&tree, &expected, "after the compaction");

	tree.core.inner.flush_all_immutables_sync().unwrap();
	assert_reads(&tree, &expected, "after the memtables were flushed");
	tree.close().await.unwrap();
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_compaction_keeps_the_files_of_a_queued_memtable_behind_one_without_pointers() {
	compaction_with_two_queued_memtables(false).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_compaction_keeps_the_files_of_a_queued_memtable_ahead_of_one_without_pointers() {
	compaction_with_two_queued_memtables(true).await;
}

/// The flush of a memtable without pointers is a cleanup too, and the memtable queued behind it
/// holds the values the only table points past. Nothing is read before both flushes: a value
/// that was read is in the block cache, and would still be there when its file is gone.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_flush_keeps_the_files_of_the_memtable_queued_behind_it() {
	let dir = TempDir::new("vlog_floor_queued_flush").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);

	let mut expected = queue_a_memtable_without_pointers(&tree).await;
	expected.extend(queue_a_memtable_with_pointers(&tree).await);
	assert_eq!(immutables(&tree), 2);
	expected.extend(register_table_past_the_memtables(&tree, 200..204, 2));

	tree.core.inner.compact_memtable().unwrap();
	assert_eq!(immutables(&tree), 1, "the first memtable is in a table");
	tree.core.inner.compact_memtable().unwrap();
	assert_eq!(immutables(&tree), 0);
	assert_reads(&tree, &expected, "after both flushes");
	tree.close().await.unwrap();
}

/// Recovery flushes the memtables it rebuilds from the WAL before the last, while the others are
/// held outside the queue. A flush that cleans up then works from the tables alone, which point
/// only into the files of the table past them. Startup cleans up after the replay, with the
/// memtable it keeps accounted for.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_restart_keeps_the_files_of_the_wal_it_replays() {
	let dir = TempDir::new("vlog_floor_restart").unwrap();
	let image = TempDir::new("vlog_floor_restart_image").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);

	let mut expected = queue_a_memtable_without_pointers(&tree).await;
	expected.extend(queue_a_memtable_with_pointers(&tree).await);
	expected.extend(register_table_past_the_memtables(&tree, 0..4, 2));
	// Written after the table, so in files that are newer still, and kept in memory.
	expected.extend(put(&tree, 200..202, 3).await);
	let files = vlog_files(dir.path());
	let active = tree.core.inner.active_memtable.read().unwrap().min_vlog_file_id();
	assert!(files[0] < tables_min(&tree), "the files of the queued memtable are the oldest");

	let crashed = image.path().join("crashed");
	copy_dir_all(dir.path(), &crashed);
	let reopened = open(&crashed, NEVER_ROTATING);
	assert_reads(&reopened, &expected, "reopened after a crash");
	// The memtable rebuilt from the WAL accounts for what it points into.
	assert!(active.is_some_and(|file| file > files[0]));
	assert_eq!(reopened.core.inner.active_memtable.read().unwrap().min_vlog_file_id(), active);
	assert_eq!(floor(&reopened), files[0], "the bound of the reopened tree");
	assert_eq!(vlog_files(&crashed)[0], files[0], "the oldest file is still there");
	reopened.close().await.unwrap();
	tree.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// records that no memtable holds
// ---------------------------------------------------------------------------

/// A memtable that holds inline values above the threshold (its WAL was written under a higher
/// one) separates them at flush time: the flush appends to the value log, and the files it writes
/// are in no table until its table is registered. A cleanup in that window works from tables that
/// point into newer files only.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_flush_that_moves_values_to_the_value_log_keeps_the_files_it_wrote() {
	let root = TempDir::new("vlog_floor_flush_separation").unwrap();
	let path = root.path().join("live");

	// Written under a threshold nothing reaches: the values stay inline, in the WAL and in the
	// memtable.
	let first = Arc::new(
		Tree::new(Arc::new(Options {
			vlog_value_threshold: 1 << 20,
			..(*options(&path, SMALL_MEMTABLE)).clone()
		}))
		.unwrap(),
	);
	stop_background_tasks(&first).await;
	let mut expected = Contents::new();
	for i in 0..4 {
		expected.extend(put(&first, i..i + 1, 1).await);
	}
	crash(&first);
	drop(first);

	// Reopened under the usual one: the flush separates them.
	let tree = quiet_tree(&path).await;
	assert!(vlog_files(&path).is_empty(), "nothing is in the value log yet");
	tree.core.inner.rotate_memtable().unwrap();
	let gate = Gate::install(&tree);
	let flusher = {
		let tree = Arc::clone(&tree);
		std::thread::spawn(move || tree.core.inner.flush_all_immutables_sync())
	};
	assert!(gate.reached().is_some(), "the flush wrote its table");
	let written = vlog_files(&path);
	assert!(!written.is_empty(), "the flush moved the values to the value log");

	// The flush is parked: its table is on disk and not in the manifest. Writes go on, past the
	// files it wrote, and a compaction drops what the second table overwrote.
	roll_past(&tree, *written.last().unwrap());
	register_table_past_the_memtables(&tree, 100..120, 2);
	register_table_past_the_memtables(&tree, 100..120, 3);
	compact(&tree);
	let kept = vlog_files(&path);

	gate.release(&tree);
	flusher.join().unwrap().unwrap();

	// Nothing was read before, so no value is in the block cache to hide a deleted file.
	let wrong = mismatches(&tree, &expected);
	assert!(
		wrong.is_empty(),
		"after the flush: {} of {} values do not read back, for example {:?}; the files the flush \
		 wrote were {written:?}, and {kept:?} were left by the compaction while it ran",
		wrong.len(),
		expected.len(),
		&wrong[..wrong.len().min(3)]
	);
	crash(&tree);
}

/// An oversized commit flushes the memtables that are queued before it writes its table, and
/// the batch's values are in the value log by then. A queued memtable that holds inline values
/// above the threshold separates them in that flush, into files newer than the batch's, and the
/// table the flush registers points into those files only: the cleanup that follows must keep the
/// files the batch is still to point into.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn an_oversized_commit_keeps_its_files_while_it_flushes_a_queued_memtable() {
	let root = TempDir::new("vlog_floor_oversized_flush").unwrap();
	let path = root.path().join("live");

	// Written under a threshold nothing reaches: the values stay inline, in the WAL and in the
	// memtable that a restart rebuilds from it.
	let first = Arc::new(
		Tree::new(Arc::new(Options {
			vlog_value_threshold: 1 << 20,
			..(*options(&path, SMALL_MEMTABLE)).clone()
		}))
		.unwrap(),
	);
	stop_background_tasks(&first).await;
	let mut expected = Contents::new();
	for i in 0..4 {
		expected.extend(put(&first, i..i + 1, 1).await);
	}
	crash(&first);
	drop(first);

	let tree = quiet_tree(&path).await;
	assert!(vlog_files(&path).is_empty(), "nothing is in the value log yet");
	tree.core.inner.rotate_memtable().unwrap();
	assert_eq!(immutables(&tree), 1, "the rebuilt memtable waits in the queue");

	// Its values are moved to the value log by the flush the commit makes, after the batch's own.
	expected.extend(put_oversized(&tree, &[10, 11, 12], 2).await);
	assert_eq!(immutables(&tree), 0, "the commit flushed the queue");

	// Nothing was read before, so no value is in the block cache to hide a deleted file.
	assert_reads(&tree, &expected, "after the oversized commit");
	crash(&tree);
}

/// What a group that failed after its record was logged leaves behind, then a table written past
/// it twice, a compaction that drops the first one's values and so every file older than the
/// second one's, and a crash. `fail` says whether the group's fsync fails or the group is
/// acknowledged (the control). Returns the image of the data directory and the file the group's
/// value is in.
async fn crash_image_after_cleanup(root: &Path, fail: bool) -> (PathBuf, u32) {
	let live = root.join("live");
	let tree = quiet_tree(&live).await;

	// A memtable with an acknowledged inline value, so that a rotation has something to rotate.
	commit(&tree, &[(b"acked".to_vec(), b"value".to_vec())], Durability::Immediate).await.unwrap();

	// The group: its value goes to the value log, its record to the WAL, its fsync fails.
	if fail {
		tree.core.inner.wal.write().fail_syncs_after(0);
	}
	let outcome =
		commit(&tree, &[(b"doomed".to_vec(), big(0, 1))], Durability::Immediate).await.is_ok();
	assert_eq!(outcome, !fail, "the group is acknowledged only when it is not made to fail");
	let first = vlog_files(&live)[0];

	// A rotation ends the poisoned segment and queues the memtable: its flush is not running, so
	// the WAL segment holding the group's record stays.
	tree.core.inner.rotate_memtable().unwrap();
	roll_past(&tree, first);
	register_table_past_the_memtables(&tree, 0..20, 2);
	register_table_past_the_memtables(&tree, 0..20, 3);
	compact(&tree);

	let image = root.join("image");
	copy_dir_all(&live, &image);
	crash(&tree);
	(image, first)
}

/// The record of a group whose fsync failed is in the WAL, and recovery replays it: a crash image
/// holds the whole group. Its value is in a value-log file that no table and no memtable points
/// into, since the group was never applied, so a cleanup that works from them alone deletes it.
/// Recovery then replays a pointer into a file that is gone, and the key fails to read for good.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_group_that_failed_after_it_was_logged_keeps_the_file_its_record_points_into() {
	let root = TempDir::new("vlog_floor_failed_group").unwrap();
	let (image, first) = crash_image_after_cleanup(root.path(), true).await;
	let files = vlog_files(&image);

	let reopened = quiet_tree(&image).await;
	assert_eq!(get(&reopened, b"acked").unwrap().as_deref(), Some(b"value".as_slice()));
	// The write was not acknowledged, but its record was written before the fsync failed and the
	// image holds it: the restart replays it, and the key must read, not fail.
	match get(&reopened, b"doomed") {
		Ok(Some(value)) => assert_eq!(value, big(0, 1)),
		Ok(None) => {
			panic!("the record of the failed group was not replayed, so nothing is checked")
		}
		Err(e) => panic!(
			"the replayed record of the failed group points into value-log file {first}, which \
			 the cleanup deleted (files left: {files:?}): {e}"
		),
	}
	crash(&reopened);
}

/// The same sequence with the group acknowledged: the memtable it is in holds the pointer, so
/// the file stays and the value reads back after the crash.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn an_acknowledged_group_in_the_same_sequence_keeps_its_file() {
	let root = TempDir::new("vlog_floor_acked_group").unwrap();
	let (image, first) = crash_image_after_cleanup(root.path(), false).await;
	assert!(vlog_files(&image).contains(&first), "the file of the queued memtable is kept");

	let reopened = quiet_tree(&image).await;
	assert_eq!(get(&reopened, b"doomed").unwrap(), Some(big(0, 1)));
	crash(&reopened);
}

/// Another way for a group to fail after its record was logged: the table of an oversized batch
/// cannot be created (the directory refuses it), so the batch is never applied anywhere, and its
/// record stays in the WAL segment the write sealed.
#[cfg(unix)]
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_batch_whose_table_could_not_be_written_keeps_the_file_its_record_points_into() {
	use std::os::unix::fs::PermissionsExt;

	let root = TempDir::new("vlog_floor_failed_table").unwrap();
	let live = root.path().join("live");
	let tree = quiet_tree(&live).await;

	// A queued memtable that is not flushed, so the segments after it are kept.
	commit(&tree, &[(b"acked".to_vec(), b"value".to_vec())], Durability::Immediate).await.unwrap();
	tree.core.inner.rotate_memtable().unwrap();

	// The oversized batch: its values go to the value log and its record to the WAL, then its
	// table cannot be created.
	let sstables = live.join("sstables");
	std::fs::set_permissions(&sstables, std::fs::Permissions::from_mode(0o555)).unwrap();
	let outcome = put_oversized_outcome(&tree, &[500, 501]).await;
	std::fs::set_permissions(&sstables, std::fs::Permissions::from_mode(0o755)).unwrap();
	assert!(outcome.is_err(), "the table of the oversized batch cannot be created");

	let first = vlog_files(&live)[0];
	let last = tree.core.inner.vlog.as_ref().unwrap().active_writer_id.load(Ordering::SeqCst);
	roll_past(&tree, last);
	register_table_past_the_memtables(&tree, 0..20, 2);
	register_table_past_the_memtables(&tree, 0..20, 3);
	compact(&tree);

	let image = root.path().join("image");
	copy_dir_all(&live, &image);
	crash(&tree);
	let files = vlog_files(&image);

	let reopened = quiet_tree(&image).await;
	assert_eq!(get(&reopened, b"acked").unwrap().as_deref(), Some(b"value".as_slice()));
	for key in [key_of(500), key_of(501)] {
		match get(&reopened, &key) {
			Ok(Some(value)) => assert_eq!(value.len(), 3000),
			Ok(None) => {
				panic!("the record of the failed batch was not replayed, nothing is checked")
			}
			Err(e) => panic!(
				"the replayed record of the failed batch points into a value-log file the cleanup \
				 deleted (first file {first}, files left: {files:?}): {e}"
			),
		}
	}
	crash(&reopened);
}

#[cfg(unix)]
async fn put_oversized_outcome(tree: &Tree, keys: &[usize]) -> crate::Result<()> {
	let mut entries: Vec<_> = keys.iter().map(|&i| (key_of(i), big(i, 1))).collect();
	for i in 0..100 {
		entries.push((format!("pad_{i:04}").into_bytes(), small(i, 1)));
	}
	commit(tree, &entries, Durability::Immediate).await
}

// ---------------------------------------------------------------------------
// a tree that was recovered
// ---------------------------------------------------------------------------

/// Opens the crash image `image` and checks what recovery built, which the tests below go on
/// from: an active memtable that holds the rows of the crashed segment and is tagged with the
/// fresh segment the writer continues in, a segment newer than any row in it. A bound that came
/// from that tag instead of from what the memtable points into would be wrong for it.
async fn recovered_tree(image: &Path) -> Arc<Tree> {
	let tree = quiet_tree(image).await;
	let inner = &tree.core.inner;
	let active = inner.active_memtable.read().unwrap();
	assert!(!active.is_empty(), "recovery rebuilt the rows of the crashed segment");
	let fresh = inner.wal.read().get_active_log_number();
	assert_eq!(active.get_wal_number(), fresh, "the memtable is tagged with the fresh segment");
	assert!(
		inner.level_manifest.read().unwrap().get_log_number() < fresh,
		"the crashed segment is not flushed"
	);
	drop(active);
	tree
}

/// A crash image whose tables hold `put(0..4, 1)` and whose last WAL segment holds
/// `put(100..104, 1)` (values in the value log) and an inline value.
async fn crash_image_with_values_in_the_wal(root: &Path) -> (PathBuf, Contents) {
	let live = root.join("live");
	let tree = quiet_tree(&live).await;
	let mut expected = put(&tree, 0..4, 1).await;
	flush_to_tables(&tree);
	expected.extend(put(&tree, 100..104, 1).await);
	commit(&tree, &[(b"inline".to_vec(), b"value".to_vec())], Durability::Immediate).await.unwrap();
	expected.insert(b"inline".to_vec(), b"value".to_vec());
	let image = root.join("image");
	copy_dir_all(&live, &image);
	crash(&tree);
	(image, expected)
}

/// A restart keeps the memtable it rebuilt from the last crashed segment as the active one. A
/// table written past it and a compaction that drops the older table's pointers are a cleanup
/// that must keep the memtable's files, and everything acknowledged reads back after another
/// restart.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_cleanup_on_a_recovered_tree_keeps_the_files_of_the_memtable_recovery_kept() {
	let root = TempDir::new("vlog_floor_recovered_cleanup").unwrap();
	let (image, mut expected) = crash_image_with_values_in_the_wal(root.path()).await;

	let tree = recovered_tree(&image).await;
	let active_min = tree.core.inner.active_memtable.read().unwrap().min_vlog_file_id();
	let active_min = active_min.expect("the recovered memtable points into the value log");
	assert!(tables_min(&tree) < active_min, "the table's files are older than the memtable's");

	// A table past the recovered memtable, and the merge that drops the older table's pointers.
	expected.extend(register_table_past_the_memtables(&tree, 0..4, 2));
	compact(&tree);
	assert!(tables_min(&tree) > active_min, "the table points past the recovered memtable's files");
	assert!(vlog_files(&image).contains(&active_min), "the memtable's file is still there");
	assert_reads(&tree, &expected, "the recovered tree after the cleanup");

	let second = root.path().join("second");
	copy_dir_all(&image, &second);
	crash(&tree);
	let again = recovered_tree(&second).await;
	assert_reads(&again, &expected, "after the second restart");
	crash(&again);
}

/// An oversized commit on a recovered tree seals the segment the recovered memtable is tagged
/// with, flushes the memtable before its own table is registered, and overwrites keys of the
/// older table and of the memtable. A compaction follows, and a second restart.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn an_oversized_commit_on_a_recovered_tree_reads_back_after_another_restart() {
	let root = TempDir::new("vlog_floor_recovered_oversized").unwrap();
	let (image, mut expected) = crash_image_with_values_in_the_wal(root.path()).await;

	let tree = recovered_tree(&image).await;
	expected.extend(put_oversized(&tree, &[0, 1, 2, 3, 100], 2).await);
	assert_eq!(immutables(&tree), 0, "the commit flushed the recovered memtable");
	compact(&tree);
	assert_reads(&tree, &expected, "the recovered tree after the commit and a compaction");

	let second = root.path().join("second");
	copy_dir_all(&image, &second);
	crash(&tree);
	let again = quiet_tree(&second).await;
	assert_reads(&again, &expected, "after the second restart");
	crash(&again);
}

/// A group that fails after its record was logged, on a recovered tree whose memtable holds no
/// pointer: the group's file is the oldest anything points into, the recovered memtable is the
/// one that accounts for it, and it is queued, not flushed, while a table past it and a
/// compaction run. The record is in a segment the second restart replays.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_group_that_fails_on_a_recovered_tree_keeps_the_file_its_record_points_into() {
	let root = TempDir::new("vlog_floor_recovered_failed_group").unwrap();
	let live = root.path().join("live");
	let tree = quiet_tree(&live).await;
	let mut expected = put(&tree, 0..4, 1).await;
	flush_to_tables(&tree);
	commit(&tree, &[(b"inline".to_vec(), b"value".to_vec())], Durability::Immediate).await.unwrap();
	expected.insert(b"inline".to_vec(), b"value".to_vec());
	let image = root.path().join("image");
	copy_dir_all(&live, &image);
	crash(&tree);

	let tree = recovered_tree(&image).await;
	let inner = &tree.core.inner;
	assert_eq!(
		inner.active_memtable.read().unwrap().min_vlog_file_id(),
		None,
		"the recovered memtable holds no pointer"
	);

	// The group: its value goes to the value log, its record to the fresh segment, its fsync fails.
	inner.wal.write().fail_syncs_after(0);
	let outcome = commit(&tree, &[(b"doomed".to_vec(), big(0, 7))], Durability::Immediate).await;
	assert!(outcome.is_err(), "the group is made to fail");
	let doomed_file = *vlog_files(&image).last().unwrap();
	assert!(tables_min(&tree) < doomed_file, "the group's file is newer than the table's");
	assert_eq!(
		inner.active_memtable.read().unwrap().min_vlog_file_id(),
		Some(doomed_file),
		"the recovered memtable accounts for the group"
	);

	// The memtable is queued, and with it the segment of the group. A table is written past it
	// twice, and the compaction that merges them drops every file older than the second one's.
	inner.rotate_memtable().unwrap();
	roll_past(&tree, doomed_file);
	expected.extend(register_table_past_the_memtables(&tree, 0..4, 2));
	register_table_past_the_memtables(&tree, 20..24, 3);
	expected.extend(register_table_past_the_memtables(&tree, 20..24, 4));
	compact(&tree);
	assert!(tables_min(&tree) > doomed_file);
	assert!(vlog_files(&image).contains(&doomed_file), "the group's file is still there");

	let second = root.path().join("second");
	copy_dir_all(&image, &second);
	crash(&tree);
	let again = quiet_tree(&second).await;
	assert_reads(&again, &expected, "after the second restart");
	// The record was written before its fsync failed, so the restart replays it, whole.
	assert_eq!(get(&again, b"doomed").unwrap(), Some(big(0, 7)), "the group is replayed");
	crash(&again);
}

// ---------------------------------------------------------------------------
// liveness: what nothing points into is deleted
// ---------------------------------------------------------------------------

/// A compaction that drops the first round's values deletes the files they were in.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_compaction_deletes_the_files_nothing_points_into() {
	let dir = TempDir::new("vlog_floor_compaction").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let expected = two_rounds_in_two_tables(&tree).await;
	let oldest = vlog_files(dir.path())[0];
	assert_eq!(tables_min(&tree), oldest, "the files of the first round are there");

	compact(&tree);

	let bound = tables_min(&tree);
	assert!(bound > oldest, "the compaction dropped the first round's pointers");
	let files = vlog_files(dir.path());
	assert!(files.iter().all(|&file| file >= bound), "files below {bound} are left: {files:?}");
	assert!(!files.is_empty());
	assert_reads(&tree, &expected, "after the compaction");
	tree.close().await.unwrap();
}

/// A cleanup that is skipped because the files are pinned leaves them for the next one, which is
/// the flush that follows.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_flush_deletes_the_files_a_skipped_cleanup_left() {
	let dir = TempDir::new("vlog_floor_flush").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let mut expected = two_rounds_in_two_tables(&tree).await;
	let all = vlog_files(dir.path());

	{
		let _pin = tree.core.inner.vlog.as_ref().unwrap().pin_files();
		compact(&tree);
	}
	assert_eq!(vlog_files(dir.path()), all, "a pinned cleanup deletes nothing");
	let bound = tables_min(&tree);
	assert!(bound > all[0], "the compaction dropped the first round's pointers");

	expected.extend(put(&tree, 10..12, 3).await);
	flush_to_tables(&tree);

	let files = vlog_files(dir.path());
	assert!(files.iter().all(|&file| file >= bound), "files below {bound} are left: {files:?}");
	assert_reads(&tree, &expected, "after the flush");
	tree.close().await.unwrap();
}

/// A restart cleans up what a cleanup that was skipped left behind.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_restart_deletes_the_files_nothing_points_into() {
	let dir = TempDir::new("vlog_floor_restart_cleanup").unwrap();
	let image = TempDir::new("vlog_floor_restart_cleanup_image").unwrap();
	let tree = open(dir.path(), NEVER_ROTATING);
	let expected = two_rounds_in_two_tables(&tree).await;
	{
		let _pin = tree.core.inner.vlog.as_ref().unwrap().pin_files();
		compact(&tree);
	}
	let bound = tables_min(&tree);
	assert!(vlog_files(dir.path())[0] < bound, "the files are left by the pinned cleanup");

	let crashed = image.path().join("crashed");
	copy_dir_all(dir.path(), &crashed);
	let reopened = open(&crashed, NEVER_ROTATING);

	let files = vlog_files(&crashed);
	assert!(files.iter().all(|&file| file >= bound), "files below {bound} are left: {files:?}");
	assert_reads(&reopened, &expected, "after the restart");
	reopened.close().await.unwrap();
	tree.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// cleanups racing everything else
// ---------------------------------------------------------------------------

/// Writers commit batches (rewriting a few keys, and an oversized batch now and then), a thread
/// rotates the memtable, another compacts, two do nothing but clean up in a loop, and the
/// background flush runs. Every acknowledged value must read back from the live tree and from a
/// crash image taken once the writers are done, and no lock is taken in two orders.
async fn cleanups_racing_everything(rounds: usize) {
	const WRITERS: usize = 3;

	let root = TempDir::new("vlog_floor_race").unwrap();
	let live = root.path().join("live");
	let tree = open(&live, SMALL_MEMTABLE);

	let stop = Arc::new(AtomicBool::new(false));
	let cleanups = Arc::new(AtomicUsize::new(0));
	let mut threads = Vec::new();
	for _ in 0..2 {
		let (tree, stop, cleanups) = (Arc::clone(&tree), Arc::clone(&stop), Arc::clone(&cleanups));
		threads.push(std::thread::spawn(move || {
			while !stop.load(Ordering::Relaxed) {
				cleanup(&tree);
				cleanups.fetch_add(1, Ordering::Relaxed);
			}
		}));
	}
	{
		let (tree, stop) = (Arc::clone(&tree), Arc::clone(&stop));
		threads.push(std::thread::spawn(move || {
			while !stop.load(Ordering::Relaxed) {
				let _ = tree.core.inner.rotate_memtable();
				std::thread::sleep(Duration::from_millis(7));
			}
		}));
	}
	{
		let (tree, stop) = (Arc::clone(&tree), Arc::clone(&stop));
		threads.push(std::thread::spawn(move || {
			while !stop.load(Ordering::Relaxed) {
				let opts = Arc::new(Options {
					level0_max_files: 2,
					..Default::default()
				});
				let _ = tree.compact(Arc::new(Strategy::from_options(opts)));
				std::thread::sleep(Duration::from_millis(11));
			}
		}));
	}

	let mut writers = Vec::new();
	for w in 0..WRITERS {
		let tree = Arc::clone(&tree);
		writers.push(tokio::spawn(async move {
			let mut expected = Contents::new();
			for round in 0..rounds {
				let mut entries = Vec::new();
				for k in 0..1 + (round + w) % 3 {
					let i = w * 10 + (round * 7 + k) % 6;
					let value = if (round + k) % 5 == 0 {
						small(i, round)
					} else {
						big(i, round)
					};
					entries.push((key_of(i), value));
				}
				commit(&tree, &entries, Durability::Immediate).await.unwrap();
				expected.extend(entries);
				if round % 10 == 9 {
					// An oversized batch of its own, with keys of its own: it overwrites nothing
					// a memtable may hold, since a table written past a memtable is read after
					// it.
					let mut entries = Vec::new();
					for j in 0..3 {
						entries.push((format!("ov_{w}_{round}_{j}").into_bytes(), big(j, round)));
					}
					for j in 0..100 {
						entries.push((
							format!("pad_{w}_{round}_{j:03}").into_bytes(),
							small(j, round),
						));
					}
					commit(&tree, &entries, Durability::Immediate).await.unwrap();
					expected.extend(entries);
				}
			}
			expected
		}));
	}

	let mut expected = Contents::new();
	let all = async {
		for writer in writers {
			expected.extend(writer.await.unwrap());
		}
	};
	tokio::time::timeout(Duration::from_secs(240), all)
		.await
		.expect("the writers finished: no commit, rotation, flush or cleanup is stuck");
	stop.store(true, Ordering::Relaxed);
	for thread in threads {
		thread.join().unwrap();
	}
	assert!(cleanups.load(Ordering::Relaxed) > 0);

	assert_reads(&tree, &expected, "live tree");
	stop_background_tasks(&tree).await;
	let image = root.path().join("image");
	copy_dir_all(&live, &image);
	let reopened = quiet_tree(&image).await;
	assert_reads(&reopened, &expected, "crash image");
	crash(&reopened);
	crash(&tree);
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn cleanups_racing_commits_rotations_flushes_and_compactions_lose_nothing() {
	cleanups_racing_everything(10).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
#[ignore = "slow: 60 rounds of three writers against rotations, compactions and cleanups, about 20 s"]
async fn cleanups_racing_commits_rotations_flushes_and_compactions_for_long_lose_nothing() {
	cleanups_racing_everything(60).await;
}

// ---------------------------------------------------------------------------
// random mixes
// ---------------------------------------------------------------------------

/// A deterministic generator, so a failing seed can be replayed.
struct Rng(u64);

impl Rng {
	fn new(seed: u64) -> Self {
		Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
	}

	fn next(&mut self) -> u64 {
		self.0 ^= self.0 << 13;
		self.0 ^= self.0 >> 7;
		self.0 ^= self.0 << 17;
		self.0
	}

	fn below(&mut self, n: usize) -> usize {
		(self.next() % n as u64) as usize
	}
}

/// Random writes, oversized batches (committed, and registered the way a commit does), rotations,
/// flushes, compactions and cleanups, with a crash and a reopen of the image now and then: the
/// reopened tree carries on, so what recovery rebuilt (memtables, tables made while replaying)
/// goes through the later cleanups. Every value is read back after each step.
///
/// An oversized batch overwrites keys that are in tables only, so that a compaction can drop the
/// old values and the files they point into while a memtable written in between is still
/// queued. A key a memtable holds, or that a restart may replay into one, is left alone until
/// everything has been flushed: a table written past the memtable would be read after it, which
/// is a matter of read order, not of value-log files.
async fn crashing_mix(seed: u64, steps: usize) {
	const KEYS: usize = 30;

	let root = TempDir::new("vlog_floor_crashing").unwrap();
	let mut generation = 0;
	let mut path = root.path().join(format!("gen{generation}"));
	let mut tree = quiet_tree(&path).await;
	let mut rng = Rng::new(seed);
	let mut expected = Contents::new();
	let mut dirty = BTreeSet::new();

	// Where the files of a memtable are most at risk: its values are the oldest in the value log,
	// and the only table points past them. Nothing has been read yet, so no value is cached.
	expected.extend(put(&tree, 0..3, 0).await);
	dirty.extend(0..3);
	tree.core.inner.rotate_memtable().unwrap();
	expected.extend(register_table_past_the_memtables(&tree, 3..5, 0));
	cleanup(&tree);
	assert_reads(&tree, &expected, &format!("seed {seed}, after the first cleanup"));

	for step in 0..steps {
		let what = format!("seed {seed}, step {step}, generation {generation}");
		match rng.below(15) {
			0..=3 => {
				let mut entries = Vec::new();
				for _ in 0..1 + rng.below(5) {
					let i = rng.below(KEYS);
					let value = if rng.below(5) == 0 {
						small(i, step)
					} else {
						big(i, step)
					};
					entries.push((key_of(i), value));
					dirty.insert(i);
				}
				commit(&tree, &entries, Durability::Immediate).await.unwrap();
				expected.extend(entries);
			}
			4..=6 => {
				let clean: Vec<usize> = (0..KEYS).filter(|i| !dirty.contains(i)).collect();
				if clean.is_empty() {
					continue;
				}
				let keys: Vec<usize> =
					(0..1 + rng.below(5)).map(|_| clean[rng.below(clean.len())]).collect();
				if rng.below(2) == 0 {
					// Its record is in the WAL, and a restart replays it into a memtable.
					dirty.extend(keys.iter().copied());
					expected.extend(put_oversized(&tree, &keys, step).await);
				} else {
					// Strictly after every file there is, and in a table of its own, whatever a
					// commit waits for first.
					for &i in &keys {
						expected.extend(register_table_past_the_memtables(&tree, i..i + 1, step));
					}
				}
			}
			7 => {
				tree.core.inner.rotate_memtable().unwrap();
			}
			8 => {
				flush_to_tables(&tree);
				dirty.clear();
			}
			9 => {
				// One flush, the oldest memtable: the others stay queued.
				let _ = tree.core.inner.compact_memtable();
			}
			10 | 11 => compact(&tree),
			12 => cleanup(&tree),
			_ => {
				let image = root.path().join(format!("gen{}", generation + 1));
				copy_dir_all(&path, &image);
				crash(&tree);
				generation += 1;
				path = image;
				tree = quiet_tree(&path).await;
			}
		}
		assert_reads(&tree, &expected, &what);
	}

	// One more crash, and everything must read back from a cold start.
	let image = root.path().join("final");
	copy_dir_all(&path, &image);
	crash(&tree);
	let tree = quiet_tree(&image).await;
	assert_reads(&tree, &expected, &format!("seed {seed}, after the last crash"));
	crash(&tree);
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn random_mixes_with_repeated_crashes_read_back() {
	for seed in 1..=2 {
		crashing_mix(seed, 40).await;
	}
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
#[ignore = "slow: eight seeds of 60 steps, with a crash and a reopen every few steps, about 60 s"]
async fn random_mixes_with_repeated_crashes_for_long_read_back() {
	for seed in 1..=8 {
		crashing_mix(seed, 60).await;
	}
}

fn wake_the_flush_task(tree: &Tree) {
	if let Some(task_manager) = tree.core.task_manager.lock().unwrap().as_ref() {
		task_manager.wake_up_memtable();
	}
}

/// The steps of `crashing_mix` with the tree's own tasks running: the background task flushes
/// the memtables that are rotated, and the compaction it wakes merges the tables while the next
/// steps commit, so a cleanup runs at whatever moment the threads happen to reach. Every value is
/// read back after each step, and after a restart that replays the WAL.
async fn background_mix(seed: u64, steps: usize) {
	const KEYS: usize = 40;

	let dir = TempDir::new("vlog_floor_background").unwrap();
	let opts = Arc::new(Options {
		level0_max_files: 2,
		..(*options(dir.path(), SMALL_MEMTABLE)).clone()
	});
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
	let mut rng = Rng::new(seed);
	let mut expected = Contents::new();
	let mut dirty = BTreeSet::new();

	for step in 0..steps {
		let what = format!("seed {seed}, step {step}");
		match rng.below(10) {
			0..=3 => {
				let mut entries = Vec::new();
				for _ in 0..1 + rng.below(6) {
					let i = rng.below(KEYS);
					let value = if rng.below(5) == 0 {
						small(i, step)
					} else {
						big(i, step)
					};
					entries.push((key_of(i), value));
					dirty.insert(i);
				}
				commit(&tree, &entries, Durability::Eventual).await.unwrap();
				expected.extend(entries);
			}
			4..=6 => {
				let clean: Vec<usize> = (0..KEYS).filter(|i| !dirty.contains(i)).collect();
				if clean.is_empty() {
					continue;
				}
				let keys: Vec<usize> =
					(0..1 + rng.below(6)).map(|_| clean[rng.below(clean.len())]).collect();
				dirty.extend(keys.iter().copied());
				expected.extend(put_oversized(&tree, &keys, step).await);
			}
			7 => {
				tree.core.inner.rotate_memtable().unwrap();
				wake_the_flush_task(&tree);
			}
			8 => {
				flush_to_tables(&tree);
				dirty.clear();
			}
			_ => tokio::time::sleep(Duration::from_millis(rng.below(4) as u64)).await,
		}
		assert_reads(&tree, &expected, &what);
	}

	wait_until("the queue to drain", || {
		wake_the_flush_task(&tree);
		immutables(&tree) == 0
	});
	assert_reads(&tree, &expected, &format!("seed {seed}, before the restart"));

	// More writes that stay in the WAL, then a restart that does not flush them.
	expected.extend(put(&tree, 0..4, 1000).await);
	tree.close().await.unwrap();
	drop(tree);
	let reopened = Arc::new(Tree::new(opts).unwrap());
	assert_reads(&reopened, &expected, &format!("seed {seed}, after the restart"));
	reopened.close().await.unwrap();
}

/// Runs `work` on a thread of its own and fails if it has not finished within `limit`. A tree
/// whose locks are taken in two orders deadlocks the threads of its runtime, and a runtime that
/// is dropped waits for them for ever, so the test's own thread must not be the one that drops it.
fn finishes_within(limit: Duration, what: &str, work: impl FnOnce() + Send + 'static) {
	let (done, finished) = mpsc::channel();
	std::thread::spawn(move || {
		let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work));
		let _ = done.send(outcome);
	});
	match finished.recv_timeout(limit) {
		Ok(Ok(())) => {}
		Ok(Err(panic)) => std::panic::resume_unwind(panic),
		Err(_) => panic!("{what} did not finish within {limit:?}: the tree is deadlocked"),
	}
}

fn background_mixes(seeds: Range<u64>, steps: usize) {
	finishes_within(Duration::from_secs(300), "the random mixes", move || {
		let runtime = tokio::runtime::Builder::new_multi_thread()
			.worker_threads(4)
			.enable_all()
			.build()
			.unwrap();
		runtime.block_on(async {
			for seed in seeds {
				background_mix(seed, steps).await;
			}
		});
	});
}

#[test]
fn random_mixes_read_back_while_the_tree_flushes_and_compacts_by_itself() {
	background_mixes(1..3, 30);
}

#[test]
#[ignore = "slow: six seeds of 60 steps with the background tasks running, about 60 s"]
fn random_mixes_for_long_read_back_while_the_tree_flushes_and_compacts_by_itself() {
	background_mixes(1..7, 60);
}
