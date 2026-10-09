//! A failed fsync of a WAL segment poisons its writer.
//!
//! After an fsync error the kernel drops the dirty pages it could not write, and a later fsync
//! of the same file can report success for them. A commit acknowledged by such an fsync would sit
//! behind a hole, and recovery stops at the first hole. So the first failed fsync ends the
//! segment: every later append and sync on it fails, and only a rotation (a new segment, a new
//! writer) lets commits through again, exactly as for a failed append.
//!
//! Every fsync call site is covered: the flusher's group sync, the sync of re-appended stale
//! records, `flush_wal(true)` (an fsync outside the WAL lock), `close`, the sync of the old
//! segment in a rotation and the one that makes the cut after a failed append durable. The
//! failpoint is `Wal::fail_syncs_after`, which fails one fsync without writing anything, so the
//! bytes stay in the file the way the page cache keeps them.

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;

use crate::batch::Batch;
use crate::ring::PipelineHook;
use crate::wal::manager::{SyncHandle, Wal};
use crate::wal::reader::Reader;
use crate::wal::writer::Writer;
use crate::wal::{
	BufferedFileWriter,
	CompressionType,
	Error as WalError,
	Options as WalOptions,
	BLOCK_SIZE,
};
use crate::{Durability, InternalKeyKind, Mode, Options, Tree};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

const BIG: usize = 100 * 1024 * 1024;

fn options(path: &Path, max_memtable_size: usize, flush_on_close: bool) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		max_memtable_size,
		flush_on_close,
		// The background flush is stopped in most tests, so a rotation that queues a memtable
		// must not stall the commits behind it.
		memtable_stall_threshold: 1000,
		..Default::default()
	})
}

/// Crash semantics: nothing is flushed on drop or close.
fn opts(path: &Path) -> Arc<Options> {
	options(path, BIG, false)
}

fn seg_path(dir: &Path, id: u64) -> PathBuf {
	dir.join("wal").join(format!("{id:020}.wal"))
}

/// The segment `tree`'s commits go to. Opening a tree continues in a fresh segment, so it is not
/// segment 0.
fn active(tree: &Tree) -> u64 {
	tree.core.inner.wal.read().get_active_log_number()
}

fn file_len(path: &Path) -> u64 {
	fs::metadata(path).unwrap().len()
}

fn copy_dir(src: &Path, dst: &Path) {
	fs::create_dir_all(dst).unwrap();
	for entry in fs::read_dir(src).unwrap() {
		let entry = entry.unwrap();
		if entry.file_name() == "LOCK" {
			continue;
		}
		let to = dst.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &to);
		} else {
			fs::copy(entry.path(), &to).unwrap();
		}
	}
}

fn mark_closed(tree: &Tree) {
	tree.core.is_closed.store(true, Ordering::SeqCst);
}

fn release_lock(tree: &Tree) {
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
}

async fn stop_background_tasks(tree: &Tree) {
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
}

/// A tree whose background tasks are stopped, so no segment is flushed and removed behind the
/// test's back, and that is never closed: a test that fails half way must not leave a spawned
/// `close()` to stop the task manager a second time. Shared through an `Arc`, never
/// `Tree::clone`: dropping any clone of a `Tree` spawns `core.close()`.
async fn open_tree(dir: &Path) -> Arc<Tree> {
	open_tree_with(dir, BIG).await
}

async fn open_tree_with(dir: &Path, max_memtable_size: usize) -> Arc<Tree> {
	let tree = Arc::new(Tree::new(options(dir, max_memtable_size, false)).unwrap());
	stop_background_tasks(&tree).await;
	mark_closed(&tree);
	tree
}

/// Ends a tree the way a crash does: no close, no flush.
fn finish(tree: &Tree) {
	mark_closed(tree);
	release_lock(tree);
}

fn get(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key.as_bytes()).unwrap()
}

fn pairs(prefix: &str, count: usize, fill: u8) -> Vec<(String, Vec<u8>)> {
	(0..count).map(|i| (format!("{prefix}{i}"), vec![fill; 40])).collect()
}

/// One single-key transaction per entry, all spawned before the flusher is polled: on the
/// current-thread runtime that is one commit group, in order.
async fn commit_group(
	tree: &Arc<Tree>,
	entries: &[(String, Vec<u8>)],
	durability: Durability,
) -> Vec<crate::Result<()>> {
	let mut handles = Vec::new();
	for (key, value) in entries {
		let (tree, key, value) = (Arc::clone(tree), key.clone(), value.clone());
		handles.push(tokio::spawn(async move {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(durability);
			txn.set(key.as_bytes(), &value).unwrap();
			txn.commit().await
		}));
	}
	let mut out = Vec::new();
	for h in handles {
		out.push(h.await.unwrap());
	}
	out
}

async fn commit_ok(tree: &Arc<Tree>, entries: &[(String, Vec<u8>)], durability: Durability) {
	for result in commit_group(tree, entries, durability).await {
		result.unwrap();
	}
}

async fn commit_one(tree: &Tree, key: &str, durability: Durability) -> crate::Result<()> {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(durability);
	txn.set(key.as_bytes(), &[b'v'; 40]).unwrap();
	txn.commit().await
}

fn all_failed(results: &[crate::Result<()>]) -> Vec<String> {
	results
		.iter()
		.map(|r| match r {
			Ok(()) => panic!("a commit of the failed group was acknowledged: {results:?}"),
			Err(e) => e.to_string(),
		})
		.collect()
}

/// Every record of a segment with the offset it ends at, decoded as a batch. A segment that is
/// not whole stops the read: callers only use it on segments they have not damaged.
fn records_with_ends(path: &Path) -> Vec<(u64, Batch)> {
	let mut reader = Reader::new(File::open(path).unwrap());
	let mut out = Vec::new();
	loop {
		match reader.read() {
			Ok((record, end)) => out.push((end, Batch::decode(record).unwrap())),
			Err(WalError::IO(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return out,
			Err(e) => panic!("{} must read end to end: {e}", path.display()),
		}
	}
}

fn keys_of(batch: &Batch) -> Vec<String> {
	batch.entries.iter().map(|e| String::from_utf8(e.key.clone()).unwrap()).collect()
}

fn set_len(path: &Path, len: u64) {
	fs::OpenOptions::new().write(true).open(path).unwrap().set_len(len).unwrap();
}

/// Overwrites `[from, to)` with zeros, as a page the kernel dropped reads back.
fn zero_range(path: &Path, from: u64, to: u64) {
	let mut bytes = fs::read(path).unwrap();
	bytes[from as usize..to as usize].fill(0);
	fs::write(path, bytes).unwrap();
}

fn arm(tree: &Tree, syncs: usize) {
	tree.core.inner.wal.write().fail_syncs_after(syncs);
}

fn sync_failed(tree: &Tree) -> bool {
	tree.core.inner.wal.read().sync_failed()
}

/// Runs `f` on a thread of its own. Its result is waited for with `finished`, which fails the
/// test after a time instead of blocking it for good if `f` is stuck.
fn spawn_bounded<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> mpsc::Receiver<T> {
	let (tx, rx) = mpsc::channel();
	std::thread::spawn(move || {
		let _ = tx.send(f());
	});
	rx
}

fn finished<T>(result: mpsc::Receiver<T>) -> T {
	result.recv_timeout(Duration::from_secs(60)).expect("a thread of the test is stuck")
}

/// Sets the flag when dropped, so threads that loop until it is set end when a test fails.
struct SetOnDrop(Arc<AtomicBool>);

impl Drop for SetOnDrop {
	fn drop(&mut self) {
		self.0.store(true, Ordering::Release);
	}
}

fn pending_sync(tree: &Tree) -> bool {
	tree.core.inner.wal.read().pending_sync()
}

// ---------------------------------------------------------------------------
// the writer
// ---------------------------------------------------------------------------

fn new_writer(path: &Path) -> Writer {
	let buffered = BufferedFileWriter::new(File::create(path).unwrap(), BLOCK_SIZE);
	Writer::new(buffered, false, CompressionType::None, 0)
}

fn wal_record(seq: u64, key: &str) -> Vec<u8> {
	let mut batch = Batch::new(seq);
	batch
		.add_record(InternalKeyKind::Set, key.as_bytes().to_vec(), Some(vec![b'v'; 50]), 0)
		.unwrap();
	batch.encode().unwrap()
}

/// Keys of every record in every segment of a WAL directory, in order.
fn wal_keys(dir: &Path) -> Vec<String> {
	let mut segments: Vec<PathBuf> = fs::read_dir(dir)
		.unwrap()
		.map(|e| e.unwrap().path())
		.filter(|p| p.extension().is_some_and(|ext| ext == "wal"))
		.collect();
	segments.sort();
	segments
		.iter()
		.flat_map(|segment| records_with_ends(segment))
		.flat_map(|(_, batch)| keys_of(&batch))
		.collect()
}

fn assert_poisoned_by_sync<T: std::fmt::Debug>(result: crate::wal::Result<T>, at: &str) {
	match result {
		Err(e) => assert!(
			e.to_string().contains("poisoned by an earlier failed sync"),
			"{at}: the error must say why, got: {e}"
		),
		Ok(v) => panic!("{at}: expected a poisoned writer to refuse, got Ok({v:?})"),
	}
}

/// The first failed fsync is the one that happened. After it the writer takes no append and no
/// sync, however the append is made, and a second sync is not a retry: it fails without
/// touching the file, and what was pending stays pending.
#[test]
fn a_writer_whose_fsync_failed_refuses_every_append_and_sync() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let mut writer = new_writer(&dir.path().join("a.wal"));
	writer.add_record(&wal_record(1, "a")).unwrap();
	writer.sync().unwrap();
	writer.add_record(&wal_record(2, "b")).unwrap();
	assert!(writer.pending_sync());

	writer.sync_gate().fail_after(0);
	let first = writer.sync().unwrap_err();
	assert!(first.to_string().contains("injected fsync failure"), "the cause is kept: {first}");
	assert!(writer.sync_failed());
	assert!(writer.pending_sync(), "a failed fsync leaves the data pending");

	assert_poisoned_by_sync(writer.sync(), "sync");
	assert!(writer.pending_sync(), "the second sync must not be a retry that clears it");
	assert_poisoned_by_sync(writer.add_record(&wal_record(3, "c")), "add_record");
	let (buf, ends) = (wal_record(3, "c"), vec![wal_record(3, "c").len()]);
	assert_poisoned_by_sync(writer.add_records(&buf, &ends), "add_records");
	assert_poisoned_by_sync(writer.close(), "close");
}

/// A writer with nothing pending still refuses to sync after a failure: a sync that had
/// nothing to do must not read as success for a segment whose fsync failed. Here the fsync that
/// fails is the one outside the WAL lock, which does not touch what the writer has pending.
#[test]
fn a_poisoned_writer_with_nothing_pending_still_refuses_to_sync() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
	wal.append(&wal_record(1, "a")).unwrap();
	wal.sync().unwrap();
	assert!(!wal.pending_sync());

	let handle = wal.sync_handle();
	handle.fail_after(0);
	handle.sync().unwrap_err();
	assert!(wal.sync_failed());
	assert!(!wal.pending_sync(), "the failed fsync was not the writer's own");

	assert_poisoned_by_sync(wal.sync(), "sync");
	assert!(wal.flush().is_ok(), "a flush to the OS cache is not an fsync");
	assert_poisoned_by_sync(wal.close(), "close");
}

/// A segment poisoned by a failed fsync is replaced by a rotation, the way one poisoned by a
/// failed append is, and the new segment takes appends and syncs.
#[test]
fn a_rotation_replaces_a_writer_poisoned_by_a_failed_fsync() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
	wal.append(&wal_record(1, "acked")).unwrap();
	wal.sync().unwrap();
	wal.append(&wal_record(2, "unsynced")).unwrap();

	wal.fail_syncs_after(0);
	assert!(wal.sync().is_err());
	assert!(wal.sync_failed());
	assert!(wal.append(&wal_record(3, "later")).is_err());
	assert!(wal.append_group(&wal_record(3, "later"), &[wal_record(3, "later").len()]).is_err());
	assert!(wal.sync().is_err(), "a retried fsync must not succeed");

	let calls = Arc::new(Mutex::new(0usize));
	let counter = Arc::clone(&calls);
	wal.set_sync_observer(Some(Arc::new(move || *counter.lock().unwrap() += 1)));
	assert_eq!(wal.rotate().unwrap(), 1, "a poisoned writer is replaced by a rotation");
	assert_eq!(*calls.lock().unwrap(), 0, "and the segment that failed is not fsynced again");

	assert!(!wal.sync_failed());
	wal.append(&wal_record(4, "after")).unwrap();
	wal.sync().unwrap();
	wal.close().unwrap();
}

/// The old segment's fsync in a rotation: a failure fails the rotation and leaves the WAL as it
/// was, with the writer poisoned. The rotation after it replaces the writer.
#[test]
fn a_failed_fsync_of_the_old_segment_fails_the_rotation_once() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
	wal.append(&wal_record(1, "a")).unwrap();

	wal.fail_syncs_after(0);
	let err = wal.rotate().unwrap_err();
	assert!(err.to_string().contains("injected fsync failure"), "{err}");
	assert_eq!(wal.get_active_log_number(), 0, "a failed rotation changes nothing");
	assert!(!dir.path().join("00000000000000000001.wal").exists(), "and creates nothing");
	assert!(wal.sync_failed());
	assert!(wal.append(&wal_record(2, "b")).is_err(), "the segment is poisoned now");

	assert_eq!(wal.rotate().unwrap(), 1);
	wal.append(&wal_record(3, "c")).unwrap();
	wal.sync().unwrap();
	wal.close().unwrap();
}

/// A failed append is cut off the segment and the cut is fsynced. If that fsync fails, it is a
/// failed fsync like any other: the segment is poisoned for syncs too, so the rotation that
/// replaces it does not retry it.
#[test]
fn a_failed_fsync_of_the_cut_after_a_failed_append_poisons_the_segment() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
	wal.append(&wal_record(1, "acked")).unwrap();
	wal.sync().unwrap();

	wal.fail_writes_after(0);
	wal.fail_syncs_after(0);
	assert!(wal.append(&wal_record(2, "failed")).is_err());
	assert!(wal.sync_failed(), "the fsync of the cut failed");
	assert_poisoned_by_sync(wal.sync(), "sync");

	let calls = Arc::new(Mutex::new(0usize));
	let counter = Arc::clone(&calls);
	wal.set_sync_observer(Some(Arc::new(move || *counter.lock().unwrap() += 1)));
	assert_eq!(wal.rotate().unwrap(), 1);
	assert_eq!(*calls.lock().unwrap(), 0, "the segment that failed is not fsynced again");
	wal.append(&wal_record(3, "after")).unwrap();
	wal.close().unwrap();
	assert_eq!(wal_keys(dir.path()), ["acked", "after"]);
}

/// `close` syncs, so it fails on a poisoned segment, and it fails when its own fsync does.
#[test]
fn close_fails_on_a_segment_whose_fsync_failed() {
	for before_close in [true, false] {
		let dir = TempDir::new("wal_fsync").unwrap();
		let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
		wal.append(&wal_record(1, "a")).unwrap();
		wal.fail_syncs_after(0);
		if before_close {
			wal.sync().unwrap_err();
		}
		let err = wal.close().unwrap_err();
		assert!(
			err.to_string().contains(if before_close {
				"poisoned by an earlier failed sync"
			} else {
				"injected fsync failure"
			}),
			"before_close={before_close}: {err}"
		);
		assert!(wal.close().is_ok(), "a WAL that is closed stays closed");
	}
}

/// The out-of-lock fsync of `WalManager::sync` belongs to the segment it was prepared for. A
/// rotation between its two phases has synced that segment already, so when the late fsync
/// fails, the caller gets the error and the new segment is not poisoned.
#[test]
fn a_late_failing_fsync_of_a_rotated_segment_does_not_poison_the_new_one() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
	wal.append(&wal_record(1, "a")).unwrap();

	let handle = wal.sync_handle();
	assert_eq!(wal.rotate().unwrap(), 1);
	handle.fail_after(0);
	assert!(handle.sync().unwrap_err().to_string().contains("injected fsync failure"));
	assert!(handle.sync().is_err(), "the old segment stays poisoned");

	assert!(!wal.sync_failed());
	wal.append(&wal_record(2, "b")).unwrap();
	wal.sync().unwrap();
	wal.close().unwrap();
}

// ---------------------------------------------------------------------------
// the flusher's group sync
// ---------------------------------------------------------------------------

/// A group whose fsync fails fails as a whole. Every waiter gets an error that carries the
/// cause, nothing of the group is readable, and what was acknowledged before it still is.
#[test(tokio::test)]
async fn a_failed_group_fsync_fails_every_waiter_and_applies_nothing() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = open_tree(dir.path()).await;

	let acked = pairs("acked", 3, b'a');
	commit_ok(&tree, &acked, Durability::Immediate).await;
	let visible_before = tree.core.inner.visible_seq_num.load(Ordering::SeqCst);

	let doomed = pairs("doomed", 5, b'd');
	arm(&tree, 0);
	let results = commit_group(&tree, &doomed, Durability::Immediate).await;
	for error in all_failed(&results) {
		assert!(error.contains("Group commit failed"), "{error}");
		assert!(error.contains("injected fsync failure"), "the cause is carried: {error}");
	}

	assert_eq!(tree.core.inner.visible_seq_num.load(Ordering::SeqCst), visible_before);
	for (key, _) in &doomed {
		assert_eq!(get(&tree, key), None, "{key} was never acknowledged");
	}
	for (key, value) in &acked {
		assert_eq!(get(&tree, key).as_deref(), Some(value.as_slice()));
	}
	finish(&tree);
}

/// After the failure later commits fail instead of being acknowledged by a retried fsync, with
/// either durability, and so does `flush_wal(true)`. The segment is not written to again and
/// the data the failed fsync left pending stays pending.
#[test(tokio::test)]
async fn later_commits_fail_instead_of_retrying_the_fsync() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = open_tree(dir.path()).await;
	let segment = seg_path(dir.path(), active(&tree));

	commit_ok(&tree, &pairs("acked", 2, b'a'), Durability::Immediate).await;
	arm(&tree, 0);
	all_failed(&commit_group(&tree, &pairs("doomed", 3, b'd'), Durability::Immediate).await);
	let len_after_failure = file_len(&segment);
	assert!(len_after_failure > 0, "the failed group is in the segment");
	assert!(pending_sync(&tree), "the fsync that failed left its data pending");

	for round in 0..3 {
		for durability in [Durability::Immediate, Durability::Eventual] {
			let key = format!("later{round}{durability:?}");
			let results = commit_group(&tree, &[(key.clone(), vec![b'l'; 40])], durability).await;
			let errors = all_failed(&results);
			assert!(
				errors[0].contains("poisoned by an earlier failed sync"),
				"{durability:?}: {}",
				errors[0]
			);
			assert_eq!(get(&tree, &key), None);
		}
		let err = tree.flush_wal(true).unwrap_err();
		assert!(err.to_string().contains("poisoned by an earlier failed sync"), "{err}");
	}
	assert!(tree.flush_wal(false).is_ok(), "a flush to the OS cache is not an fsync");
	assert_eq!(file_len(&segment), len_after_failure, "a poisoned segment takes no bytes");
	assert!(pending_sync(&tree), "and no retry cleared what the failed fsync left pending");
	finish(&tree);
}

/// An Eventual commit never fsyncs, so it does not reach the failpoint and is acknowledged; the
/// fsync that fails is the next Immediate one, which also covers the Eventual commit's bytes. The
/// Eventual commit stays acknowledged and readable, because it never promised durability.
#[test(tokio::test)]
async fn eventual_commits_never_reach_the_fsync_and_stay_readable_after_a_failure() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = open_tree(dir.path()).await;

	arm(&tree, 0);
	let eventual = pairs("eventual", 3, b'e');
	commit_ok(&tree, &eventual, Durability::Eventual).await;
	assert!(!tree.core.inner.wal.read().sync_failed(), "no fsync happened, so none failed");
	assert!(pending_sync(&tree));

	all_failed(&commit_group(&tree, &pairs("doomed", 2, b'd'), Durability::Immediate).await);
	assert!(tree.core.inner.wal.read().sync_failed());
	for (key, value) in &eventual {
		assert_eq!(get(&tree, key).as_deref(), Some(value.as_slice()), "{key}");
	}
	finish(&tree);
}

/// A rotation clears the poison of a failed fsync as it clears the one of a failed append: the
/// next group goes into the new segment and is acknowledged, and flush_wal(true) works again.
#[test(tokio::test)]
async fn a_rotation_clears_the_poison_of_a_failed_fsync() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = open_tree(dir.path()).await;
	let first = active(&tree);

	let acked = pairs("acked", 3, b'a');
	commit_ok(&tree, &acked, Durability::Immediate).await;
	arm(&tree, 0);
	let doomed = pairs("doomed", 4, b'd');
	all_failed(&commit_group(&tree, &doomed, Durability::Immediate).await);
	all_failed(&commit_group(&tree, &pairs("later", 1, b'l'), Durability::Immediate).await);
	assert!(tree.flush_wal(true).is_err());

	tree.core.inner.rotate_memtable().unwrap();
	assert!(!tree.core.inner.wal.read().sync_failed());
	let again = pairs("again", 3, b'g');
	commit_ok(&tree, &again, Durability::Immediate).await;
	commit_ok(&tree, &pairs("eventual", 2, b'e'), Durability::Eventual).await;
	tree.flush_wal(true).unwrap();

	for (key, value) in acked.iter().chain(&again) {
		assert_eq!(get(&tree, key).as_deref(), Some(value.as_slice()), "{key}");
	}
	for (key, _) in &doomed {
		assert_eq!(get(&tree, key), None, "{key}");
	}
	// The new segment holds its own groups only, whole.
	let next: Vec<String> = records_with_ends(&seg_path(dir.path(), first + 1))
		.iter()
		.flat_map(|(_, b)| keys_of(b))
		.collect();
	assert_eq!(next, ["again0", "again1", "again2", "eventual0", "eventual1"]);
	finish(&tree);
}

/// `fail_syncs_after(n)` lets `n` fsyncs through and fails the next, once. A sync with nothing
/// pending is not an fsync and does not count, and the failpoint is spent by the failure.
#[test(tokio::test)]
async fn the_failpoint_fails_exactly_the_nth_fsync() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = open_tree(dir.path()).await;

	arm(&tree, 2);
	commit_one(&tree, "k0", Durability::Immediate).await.unwrap();
	// Nothing is pending: this is not an fsync.
	tree.core.inner.wal.write().sync().unwrap();
	commit_one(&tree, "k1", Durability::Immediate).await.unwrap();
	assert!(commit_one(&tree, "k2", Durability::Immediate).await.is_err());
	assert!(sync_failed(&tree));
	assert_eq!(get(&tree, "k2"), None);
	assert!(get(&tree, "k1").is_some());

	// The failpoint is spent: the segment stays poisoned, but nothing fails a second time on its
	// own, so a rotation and the commits after it go through.
	tree.core.inner.rotate_memtable().unwrap();
	for i in 3..6 {
		commit_one(&tree, &format!("k{i}"), Durability::Immediate).await.unwrap();
	}
	finish(&tree);
}

/// A group is applied whole or not at all, so an Eventual commit that shares a group with an
/// Immediate one fails with it when the group's fsync fails: it is not applied and not
/// acknowledged either.
#[test(tokio::test)]
async fn an_eventual_commit_in_a_group_with_an_immediate_one_fails_with_it() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = open_tree(dir.path()).await;
	commit_one(&tree, "acked", Durability::Immediate).await.unwrap();

	arm(&tree, 0);
	let mut handles = Vec::new();
	for (key, durability) in
		[("eventual", Durability::Eventual), ("immediate", Durability::Immediate)]
	{
		let tree = Arc::clone(&tree);
		handles.push(tokio::spawn(async move { commit_one(&tree, key, durability).await }));
	}
	for handle in handles {
		let error = handle.await.unwrap().expect_err("the group's fsync failed").to_string();
		assert!(error.contains("injected fsync failure"), "{error}");
	}
	assert_eq!(get(&tree, "eventual"), None);
	assert_eq!(get(&tree, "immediate"), None);
	assert!(get(&tree, "acked").is_some());
	finish(&tree);
}

/// The flusher rotates before it logs a batch that does not fit what is left of a non-empty
/// memtable, and a rotation replaces a poisoned segment. So the first commit that does not fit is
/// acknowledged in a new segment with nobody asking for a rotation. Besides a checkpoint, it is
/// the only way a running database leaves the poison: with an empty memtable, or a batch that is
/// larger than the whole memtable (it is logged first), the database stays write-dead until a
/// restart.
#[test(tokio::test)]
async fn a_commit_that_does_not_fit_the_memtable_replaces_the_poisoned_segment() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = open_tree_with(dir.path(), 4096).await;
	let first = active(&tree);
	let prefill = pairs("pre", 10, b'p');
	commit_ok(&tree, &prefill, Durability::Immediate).await;

	arm(&tree, 0);
	assert!(commit_one(&tree, "doomed", Durability::Immediate).await.is_err());
	assert!(commit_one(&tree, "doomed2", Durability::Immediate).await.is_err());
	assert!(sync_failed(&tree));
	assert_eq!(active(&tree), first);

	let mut txn = tree.begin().unwrap();
	txn.set(b"big", &[b'b'; 1800]).unwrap();
	txn.commit().await.unwrap();
	assert_eq!(active(&tree), first + 1, "rotated by the commit");
	assert!(!sync_failed(&tree));
	assert_eq!(get(&tree, "big"), Some(vec![b'b'; 1800]));
	assert_eq!(get(&tree, "doomed"), None);
	for (key, value) in &prefill {
		assert_eq!(get(&tree, key).as_deref(), Some(value.as_slice()), "{key}");
	}
	finish(&tree);
}

/// A checkpoint rotates the active memtable, so it replaces a poisoned segment: the checkpoint
/// is made, holds what was acknowledged (Eventual included) and not the failed group, and commits
/// work afterwards.
#[test(tokio::test)]
async fn a_checkpoint_replaces_a_poisoned_segment_and_leaves_out_the_failed_group() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = open_tree(&dir.path().join("db")).await;
	let acked = pairs("acked", 3, b'a');
	commit_ok(&tree, &acked, Durability::Immediate).await;
	let eventual = pairs("eventual", 1, b'e');
	commit_ok(&tree, &eventual, Durability::Eventual).await;
	arm(&tree, 0);
	all_failed(&commit_group(&tree, &pairs("doomed", 1, b'd'), Durability::Immediate).await);
	assert!(sync_failed(&tree));

	let checkpoint = dir.path().join("checkpoint");
	tree.create_checkpoint(&checkpoint).unwrap();
	assert!(!sync_failed(&tree), "the checkpoint rotated the WAL");
	commit_one(&tree, "after", Durability::Immediate).await.unwrap();

	let restored_dir = dir.path().join("restored");
	copy_dir(&checkpoint, &restored_dir);
	let restored = reopen(&restored_dir);
	for (key, value) in acked.iter().chain(&eventual) {
		assert_eq!(get(&restored, key).as_deref(), Some(value.as_slice()), "{key}");
	}
	assert_eq!(get(&restored, "doomed0"), None);
	assert_eq!(get(&restored, "after"), None, "made after the checkpoint");
	finish(&restored);
	finish(&tree);
}

/// `flush_wal(true)` fsyncs outside the WAL lock. A failure there poisons the segment just the
/// same: the retry fails instead of succeeding, and so does every commit.
#[test(tokio::test)]
async fn a_failed_flush_wal_poisons_the_segment() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = open_tree(dir.path()).await;

	commit_ok(&tree, &pairs("acked", 2, b'a'), Durability::Immediate).await;
	commit_ok(&tree, &pairs("eventual", 2, b'e'), Durability::Eventual).await;
	arm(&tree, 0);
	let err = tree.flush_wal(true).unwrap_err();
	assert!(err.to_string().contains("injected fsync failure"), "{err}");

	let err = tree.flush_wal(true).unwrap_err();
	assert!(err.to_string().contains("poisoned by an earlier failed sync"), "retry: {err}");
	for durability in [Durability::Immediate, Durability::Eventual] {
		all_failed(&commit_group(&tree, &pairs("later", 1, b'l'), durability).await);
	}

	tree.core.inner.rotate_memtable().unwrap();
	tree.flush_wal(true).unwrap();
	commit_ok(&tree, &pairs("again", 2, b'g'), Durability::Immediate).await;
	finish(&tree);
}

/// The sync of re-appended stale records is an fsync too. A rotation between the append and the
/// apply makes the group stale; its records are appended again to the new segment and synced
/// there. That fsync fails: the group fails, nothing of it is applied, and the new segment is
/// poisoned.
#[test(tokio::test)]
async fn a_failed_fsync_of_a_reappended_group_fails_it_and_poisons_the_new_segment() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = open_tree(dir.path()).await;
	let first = active(&tree);

	let acked = pairs("acked", 2, b'a');
	commit_ok(&tree, &acked, Durability::Immediate).await;

	let inner = Arc::clone(&tree.core.inner);
	let fired = Arc::new(AtomicBool::new(false));
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::BeforeApplyRound {
			round: 0,
		} = point
		{
			if !fired.swap(true, Ordering::SeqCst) {
				inner.rotate_memtable().unwrap();
				inner.wal.write().fail_syncs_after(0);
			}
		}
	})));

	let doomed = pairs("doomed", 3, b'd');
	let results = commit_group(&tree, &doomed, Durability::Immediate).await;
	for error in all_failed(&results) {
		assert!(error.contains("injected fsync failure"), "{error}");
	}
	assert_eq!(active(&tree), first + 1);
	assert!(tree.core.inner.wal.read().sync_failed());
	for (key, _) in &doomed {
		assert_eq!(get(&tree, key), None, "{key}");
	}
	tree.core.commit_pipeline.set_hook(None);
	all_failed(&commit_group(&tree, &pairs("later", 1, b'l'), Durability::Immediate).await);

	// Nothing was applied to the new memtable, so `rotate_memtable` is a no-op and leaves the
	// poison, as it does for a failed append; sealing the segment rotates it anyway.
	tree.core.inner.rotate_memtable().unwrap();
	assert!(tree.core.inner.wal.read().sync_failed());
	tree.core.inner.seal_active_wal_segment().unwrap();
	commit_ok(&tree, &pairs("again", 1, b'g'), Durability::Immediate).await;
	for (key, value) in &acked {
		assert_eq!(get(&tree, key).as_deref(), Some(value.as_slice()), "{key}");
	}
	finish(&tree);
}

/// A rotation whose fsync of the old segment fails is an error, leaves the active memtable
/// where it was, and poisons the writer. The next rotation goes ahead.
#[test(tokio::test)]
async fn a_failed_fsync_in_a_memtable_rotation_leaves_the_memtable_in_place() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = open_tree(dir.path()).await;
	let first = active(&tree);

	let eventual = pairs("eventual", 3, b'e');
	commit_ok(&tree, &eventual, Durability::Eventual).await;

	arm(&tree, 0);
	let err = tree.core.inner.rotate_memtable().unwrap_err();
	assert!(err.to_string().contains("injected fsync failure"), "{err}");
	assert_eq!(active(&tree), first);
	assert_eq!(tree.core.inner.active_memtable.read().unwrap().get_wal_number(), first);
	assert_eq!(tree.core.inner.immutable_count(), 0);
	all_failed(&commit_group(&tree, &pairs("doomed", 1, b'd'), Durability::Immediate).await);

	tree.core.inner.rotate_memtable().unwrap();
	assert_eq!(active(&tree), first + 1);
	assert_eq!(tree.core.inner.active_memtable.read().unwrap().get_wal_number(), first + 1);
	assert_eq!(tree.core.inner.immutable_count(), 1);
	commit_ok(&tree, &pairs("again", 2, b'g'), Durability::Immediate).await;
	for (key, value) in &eventual {
		assert_eq!(get(&tree, key).as_deref(), Some(value.as_slice()), "{key}");
	}
	finish(&tree);
}

/// `Tree::close` syncs the WAL, so it reports a poisoned segment.
#[test(tokio::test)]
async fn close_reports_a_poisoned_segment() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = Arc::new(Tree::new(opts(dir.path())).unwrap());
	commit_ok(&tree, &pairs("acked", 2, b'a'), Durability::Immediate).await;
	arm(&tree, 0);
	all_failed(&commit_group(&tree, &pairs("doomed", 2, b'd'), Durability::Immediate).await);

	let err = tree.close().await.unwrap_err();
	assert!(err.to_string().contains("poisoned by an earlier failed sync"), "{err}");
}

/// With `flush_on_close` everything is in tables before the WAL is closed, so a poisoned WAL fails
/// the close once the data is safe. The rest of the shutdown still runs: the obsolete segments go
/// and the lock is released, so the directory opens again while the tree that failed to close
/// is still alive, and it holds what was acknowledged.
#[test(tokio::test)]
async fn close_on_a_poisoned_wal_reports_it_after_the_rest_of_the_shutdown() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = Arc::new(Tree::new(options(dir.path(), BIG, true)).unwrap());
	let segment = seg_path(dir.path(), active(&tree));
	let acked = pairs("acked", 3, b'a');
	commit_ok(&tree, &acked, Durability::Immediate).await;
	let eventual = pairs("eventual", 1, b'e');
	commit_ok(&tree, &eventual, Durability::Eventual).await;
	arm(&tree, 0);
	let doomed = pairs("doomed", 2, b'd');
	all_failed(&commit_group(&tree, &doomed, Durability::Immediate).await);
	assert!(segment.exists(), "the commits are logged in the segment");

	let err = tree.close().await.unwrap_err();
	assert!(err.to_string().contains("poisoned by an earlier failed sync"), "{err}");
	assert!(!segment.exists(), "the segment is in tables and was cleaned up");

	let reopened = reopen(dir.path());
	for (key, value) in acked.iter().chain(&eventual) {
		assert_eq!(get(&reopened, key).as_deref(), Some(value.as_slice()), "{key}");
	}
	for (key, _) in &doomed {
		assert_eq!(get(&reopened, key), None, "{key}");
	}
	finish(&reopened);
}

// ---------------------------------------------------------------------------
// crash images
// ---------------------------------------------------------------------------

/// Reopens a copy of a data directory the way a restart does, and returns the tree.
fn reopen(image: &Path) -> Tree {
	Tree::new(opts(image)).unwrap()
}

/// What each image of the damaged segment is: the copy of the live segment, or the segment with
/// what the failed fsync left behind dropped in one way or another.
#[derive(Debug, Clone, Copy)]
enum Damage {
	/// The live files: every byte the writer wrote is there, as the page cache would keep it.
	None,
	/// The segment ends at `at`.
	CutAt(u64),
	/// `[from, end of segment)` reads back as zeros, the length being what the inode says.
	ZeroFrom(u64),
}

struct Failed {
	/// Commit-ordered keys of the failed group, as the live segment holds them.
	group: Vec<String>,
	/// Where in the damaged segment the acknowledged Immediate commits end.
	acked_end: u64,
	/// Where the Eventual commit ends (it is not durable, so it may be gone).
	eventual_end: u64,
	/// Where the records of the failed group end.
	record_ends: Vec<u64>,
	/// Commits made after the failure that were acknowledged. There must be none: they would sit
	/// behind the failed group in its segment, and a hole there takes them with it.
	later_acked: Vec<String>,
}

/// An acknowledged Immediate group, an Eventual commit and then an Immediate group whose fsync
/// fails, on a tree that is then left as it is. `rotate` rotates it afterwards and acknowledges
/// another group in the new segment. Returns the id of the segment that was damaged.
async fn fail_a_group(live: &Path, rotate: bool) -> (Arc<Tree>, u64, Failed) {
	let tree = open_tree(live).await;
	let damaged = active(&tree);
	let segment = seg_path(live, damaged);
	commit_ok(&tree, &pairs("acked", 3, b'a'), Durability::Immediate).await;
	let acked_end = file_len(&segment);
	commit_ok(&tree, &pairs("eventual", 1, b'e'), Durability::Eventual).await;
	let eventual_end = file_len(&segment);

	arm(&tree, 0);
	all_failed(&commit_group(&tree, &pairs("doomed", 4, b'd'), Durability::Immediate).await);
	let records = records_with_ends(&segment);
	let group_records: Vec<&(u64, Batch)> =
		records.iter().filter(|(end, _)| *end > eventual_end).collect();
	let failed = Failed {
		group: group_records.iter().flat_map(|(_, b)| keys_of(b)).collect(),
		acked_end,
		eventual_end,
		record_ends: group_records.iter().map(|(end, _)| *end).collect(),
		later_acked: Vec::new(),
	};
	assert_eq!(failed.group, ["doomed0", "doomed1", "doomed2", "doomed3"]);

	let later = pairs("later", 2, b'l');
	let results = commit_group(&tree, &later, Durability::Immediate).await;
	let failed = Failed {
		later_acked: later
			.iter()
			.zip(&results)
			.filter(|(_, result)| result.is_ok())
			.map(|((key, _), _)| key.clone())
			.collect(),
		..failed
	};

	if rotate {
		tree.core.inner.rotate_memtable().unwrap();
		commit_ok(&tree, &pairs("again", 3, b'g'), Durability::Immediate).await;
	}
	(tree, damaged, failed)
}

/// Every acknowledged commit is recovered with its value, and of the failed group a prefix of
/// whole records: nothing torn, nothing out of order.
fn assert_recovered(
	image: &Tree,
	failed: &Failed,
	acknowledged_later: bool,
	eventual_kept: bool,
	at: &str,
) {
	for (key, value) in pairs("acked", 3, b'a') {
		assert_eq!(get(image, &key), Some(value), "{at}: {key} was acknowledged");
	}
	if acknowledged_later {
		for (key, value) in pairs("again", 3, b'g') {
			assert_eq!(get(image, &key), Some(value), "{at}: {key} was acknowledged");
		}
	}
	for key in &failed.later_acked {
		assert_eq!(get(image, key), Some(vec![b'l'; 40]), "{at}: {key} was acknowledged");
	}
	if eventual_kept {
		assert_eq!(get(image, "eventual0"), Some(vec![b'e'; 40]), "{at}");
	}
	let present: Vec<bool> = failed.group.iter().map(|key| get(image, key).is_some()).collect();
	let prefix = present.iter().take_while(|p| **p).count();
	assert!(
		present.iter().skip(prefix).all(|p| !p),
		"{at}: the failed group must be a prefix of whole records, got {present:?}"
	);
	for key in &failed.group[..prefix] {
		assert_eq!(get(image, key), Some(vec![b'd'; 40]), "{at}: {key} is not what was written");
	}
}

/// A crash after the failed fsync, with every image of what the disk can hold: the live files,
/// the segment cut or zeroed at every record boundary of the failed group and inside each
/// record, and the Eventual commit gone with it. Every acknowledged group is recovered in all of
/// them; the failed group is wholly there or wholly absent in the live copy and in the images
/// that drop all of it, and a prefix of whole records otherwise. Run with the failed segment
/// last, and with a rotation and an acknowledged group after it.
#[test(tokio::test)]
async fn a_crash_after_a_failed_fsync_recovers_every_acknowledged_group() {
	for rotate in [false, true] {
		let root = TempDir::new("wal_fsync").unwrap();
		let live = root.path().join("live");
		let (tree, damaged, failed) = fail_a_group(&live, rotate).await;

		let mut damages = vec![
			Damage::None,
			Damage::CutAt(failed.eventual_end),
			Damage::CutAt(failed.acked_end),
			Damage::ZeroFrom(failed.eventual_end),
			Damage::ZeroFrom(failed.acked_end),
		];
		let mut start = failed.eventual_end;
		for end in &failed.record_ends {
			damages.push(Damage::CutAt(*end));
			damages.push(Damage::CutAt(start + (end - start) / 2));
			damages.push(Damage::ZeroFrom(*end));
			start = *end;
		}

		for (n, damage) in damages.into_iter().enumerate() {
			let image = root.path().join(format!("image{n}"));
			copy_dir(&live, &image);
			let segment = seg_path(&image, damaged);
			let at = format!("rotate={rotate}, {damage:?}");
			match damage {
				Damage::None => {}
				Damage::CutAt(at) => set_len(&segment, at),
				Damage::ZeroFrom(from) => zero_range(&segment, from, file_len(&segment)),
			}
			let eventual_kept = match damage {
				Damage::None => true,
				Damage::CutAt(at) | Damage::ZeroFrom(at) => at >= failed.eventual_end,
			};

			let reopened = reopen(&image);
			assert_recovered(&reopened, &failed, rotate, eventual_kept, &at);
			let present = failed.group.iter().filter(|k| get(&reopened, k).is_some()).count();
			match damage {
				// Nothing was dropped: every byte of the failed group is in the segment.
				Damage::None => assert_eq!(present, failed.group.len(), "{at}"),
				Damage::CutAt(cut) | Damage::ZeroFrom(cut) if cut <= failed.eventual_end => {
					assert_eq!(present, 0, "{at}: the group was dropped whole")
				}
				_ => {}
			}

			// The recovered database takes commits again, and keeps them across another restart.
			if !rotate {
				let tree = Arc::new(reopened);
				commit_ok(&tree, &pairs("fresh", 2, b'f'), Durability::Immediate).await;
				finish(&tree);
				let again = reopen(&image);
				for (key, value) in pairs("fresh", 2, b'f') {
					assert_eq!(get(&again, &key), Some(value), "{at}: {key}");
				}
				assert_recovered(&again, &failed, false, eventual_kept, &at);
				finish(&again);
			} else {
				finish(&reopened);
			}
		}
		finish(&tree);
	}
}

fn assert_all(tree: &Tree, entries: &[(String, Vec<u8>)], at: &str) {
	for (key, value) in entries {
		assert_eq!(get(tree, key).as_ref(), Some(value), "{at}: {key} was acknowledged");
	}
}

/// What a failed fsync of a group can leave in a segment that was rotated away: the tail cut,
/// the tail zeroed, or a hole of zeros with valid bytes after it (pages dropped one by one).
#[derive(Debug, Clone, Copy)]
enum Dropped {
	Cut(u64),
	ZeroTail(u64),
	Hole(u64, u64),
}

/// An image of a failed group followed by a rotation and acknowledged groups, damaged as above,
/// reopened with the default recovery: everything acknowledged is back, the database takes new
/// commits, and a second restart still has all of it. The first restart repairs the damaged
/// segment, which is not the last one.
#[test(tokio::test)]
async fn a_damaged_segment_that_is_not_the_last_is_repaired_once_and_stays_repaired() {
	let root = TempDir::new("wal_fsync").unwrap();
	let live = root.path().join("live");
	let tree = open_tree(&live).await;
	let damaged_id = active(&tree);
	let segment = seg_path(&live, damaged_id);

	let acked = pairs("acked", 3, b'a');
	commit_ok(&tree, &acked, Durability::Immediate).await;
	let acked_end = file_len(&segment);
	commit_ok(&tree, &pairs("eventual", 2, b'e'), Durability::Eventual).await;
	let eventual_end = file_len(&segment);
	arm(&tree, 0);
	all_failed(&commit_group(&tree, &pairs("doomed", 4, b'd'), Durability::Immediate).await);
	let doomed_end = file_len(&segment);
	tree.core.inner.rotate_memtable().unwrap();
	let again = pairs("again", 3, b'g');
	commit_ok(&tree, &again, Durability::Immediate).await;
	// An Eventual commit and an Immediate one in the new segment, behind the damaged one.
	commit_ok(&tree, &pairs("tail", 2, b't'), Durability::Eventual).await;
	let last = pairs("last", 1, b'z');
	commit_ok(&tree, &last, Durability::Immediate).await;

	let damages = [
		Dropped::Cut(acked_end),
		Dropped::Cut(eventual_end),
		Dropped::Cut(eventual_end + 11),
		Dropped::Cut(doomed_end - 3),
		Dropped::ZeroTail(acked_end),
		Dropped::ZeroTail(eventual_end + 11),
		Dropped::Hole(acked_end, eventual_end),
		Dropped::Hole(acked_end + 5, acked_end + 300),
		Dropped::Hole(eventual_end, doomed_end - 1),
		Dropped::Hole(eventual_end + 20, eventual_end + 21),
	];
	for (n, damage) in damages.into_iter().enumerate() {
		let at = format!("{damage:?}");
		let image = root.path().join(format!("image{n}"));
		copy_dir(&live, &image);
		let damaged = seg_path(&image, damaged_id);
		match damage {
			Dropped::Cut(len) => set_len(&damaged, len),
			Dropped::ZeroTail(from) => zero_range(&damaged, from, file_len(&damaged)),
			Dropped::Hole(from, to) => zero_range(&damaged, from, to),
		}

		let first = Arc::new(reopen(&image));
		mark_closed(&first);
		assert_all(&first, &acked, &at);
		assert_all(&first, &again, &at);
		assert_all(&first, &last, &at);
		let fresh = pairs("fresh", 3, b'f');
		commit_ok(&first, &fresh, Durability::Immediate).await;
		finish(&first);
		drop(first);

		let second = reopen(&image);
		assert_all(&second, &acked, &at);
		assert_all(&second, &again, &at);
		assert_all(&second, &fresh, &at);
		assert_all(&second, &last, &at);
		finish(&second);
	}
	finish(&tree);
}

// ---------------------------------------------------------------------------
// concurrent appends and rotations around the out-of-lock fsync
// ---------------------------------------------------------------------------

/// Holds the next fsync inside the gate until `release` is called, and reports when it got
/// there. `calls` counts the fsyncs that reached the file, the held one included.
struct Pause {
	entered: mpsc::Receiver<()>,
	release: mpsc::Sender<()>,
	calls: Arc<AtomicUsize>,
	handle: SyncHandle,
	/// How many fsyncs of the segment had been asked for when the pause was set.
	asked_before: usize,
}

fn pause_next_fsync(tree: &Tree) -> Pause {
	let (entered_tx, entered) = mpsc::channel();
	let (release, release_rx) = mpsc::channel::<()>();
	let (entered_tx, release_rx) = (Mutex::new(entered_tx), Mutex::new(release_rx));
	let calls = Arc::new(AtomicUsize::new(0));
	let counter = Arc::clone(&calls);
	let mut wal = tree.core.inner.wal.write();
	wal.set_sync_observer(Some(Arc::new(move || {
		if counter.fetch_add(1, Ordering::SeqCst) == 0 {
			entered_tx.lock().unwrap().send(()).unwrap();
			// A test that fails before it releases must not leave this thread parked for good.
			let _ = release_rx.lock().unwrap().recv_timeout(Duration::from_secs(60));
		}
	})));
	let handle = wal.sync_handle();
	Pause {
		entered,
		release,
		calls,
		asked_before: handle.arrivals(),
		handle,
	}
}

impl Pause {
	/// Waits until the fsync is held inside the gate.
	fn wait_until_held(&self) {
		self.entered.recv_timeout(Duration::from_secs(20)).expect("an fsync reached the gate");
	}

	/// Waits until `n` fsyncs of the segment were asked for since the pause was set, which is when
	/// `n - 1` of them are waiting behind the held one. Read without the WAL lock, which a
	/// waiting fsync may hold.
	fn wait_for_fsyncs_asked(&self, n: usize) {
		let started = Instant::now();
		while self.handle.arrivals() - self.asked_before < n {
			assert!(
				started.elapsed() < Duration::from_secs(20),
				"fewer than {n} fsyncs were asked for"
			);
			std::thread::sleep(Duration::from_millis(1));
		}
	}
}

/// While `flush_wal(true)` is inside its fsync, outside the WAL lock, an Immediate commit appends
/// to the same segment: the lock is free, so it does. Its own fsync cannot run before the one in
/// flight has finished, and when that fails the commit fails too: it is not acknowledged behind
/// the hole the failed fsync may have left.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_appended_during_a_failing_out_of_lock_fsync_is_not_acknowledged() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = open_tree(dir.path()).await;
	let segment = seg_path(dir.path(), active(&tree));
	commit_ok(&tree, &pairs("acked", 2, b'a'), Durability::Immediate).await;
	commit_ok(&tree, &pairs("eventual", 2, b'e'), Durability::Eventual).await;
	let visible = tree.core.inner.visible_seq_num.load(Ordering::SeqCst);

	arm(&tree, 0);
	let pause = pause_next_fsync(&tree);
	let flush = {
		let tree = Arc::clone(&tree);
		spawn_bounded(move || tree.flush_wal(true))
	};
	pause.wait_until_held();

	let before = file_len(&segment);
	let commit = {
		let tree = Arc::clone(&tree);
		tokio::spawn(async move {
			commit_group(&tree, &pairs("during", 3, b'u'), Durability::Immediate).await
		})
	};
	// The append is not held up by the fsync in flight, which holds no WAL lock, and the fsync
	// of the group waits for it at the gate.
	let started = Instant::now();
	while file_len(&segment) == before {
		assert!(started.elapsed() < Duration::from_secs(20), "the append was blocked by the fsync");
		tokio::time::sleep(Duration::from_millis(5)).await;
	}
	pause.wait_for_fsyncs_asked(2);
	pause.release.send(()).unwrap();

	let err = finished(flush).unwrap_err();
	assert!(err.to_string().contains("injected fsync failure"), "{err}");
	let results = tokio::time::timeout(Duration::from_secs(60), commit)
		.await
		.expect("the commit is stuck")
		.unwrap();
	let errors = all_failed(&results);
	assert!(errors[0].contains("poisoned by an earlier failed sync"), "{}", errors[0]);
	assert_eq!(pause.calls.load(Ordering::SeqCst), 1, "the commit's fsync never reached the file");
	assert_eq!(tree.core.inner.visible_seq_num.load(Ordering::SeqCst), visible);
	for (key, _) in pairs("during", 3, b'u') {
		assert_eq!(get(&tree, &key), None, "{key}");
	}
	finish(&tree);
}

/// Two `flush_wal(true)` calls overlap while the fsync of the first fails. The second waits at
/// the gate and then fails without a second fsync reaching the file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_racing_flush_wal_calls_make_one_fsync_and_both_fail() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = open_tree(dir.path()).await;
	commit_ok(&tree, &pairs("acked", 2, b'a'), Durability::Immediate).await;
	commit_ok(&tree, &pairs("eventual", 2, b'e'), Durability::Eventual).await;

	arm(&tree, 0);
	let pause = pause_next_fsync(&tree);
	let first = {
		let tree = Arc::clone(&tree);
		spawn_bounded(move || tree.flush_wal(true))
	};
	pause.wait_until_held();
	let second = {
		let tree = Arc::clone(&tree);
		spawn_bounded(move || tree.flush_wal(true))
	};
	pause.wait_for_fsyncs_asked(2);
	pause.release.send(()).unwrap();

	let first = finished(first).unwrap_err().to_string();
	let second = finished(second).unwrap_err().to_string();
	assert!(first.contains("injected fsync failure"), "{first}");
	assert!(second.contains("poisoned by an earlier failed sync"), "{second}");
	assert_eq!(pause.calls.load(Ordering::SeqCst), 1, "a second fsync reached the file");
	finish(&tree);
}

/// A rotation that needs the old segment's fsync while `flush_wal(true)` is failing it does not
/// retry it: the rotation fails once, with the poison, or, if it only looks after the failure,
/// goes ahead without the fsync. Either way the next rotation replaces the writer and commits
/// work again, and the caller of `flush_wal` got the error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rotation_racing_a_failing_out_of_lock_fsync_never_retries_it() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = open_tree(dir.path()).await;
	let before = active(&tree);
	commit_ok(&tree, &pairs("acked", 2, b'a'), Durability::Immediate).await;
	commit_ok(&tree, &pairs("eventual", 2, b'e'), Durability::Eventual).await;

	arm(&tree, 0);
	let pause = pause_next_fsync(&tree);
	let flush = {
		let tree = Arc::clone(&tree);
		spawn_bounded(move || tree.flush_wal(true))
	};
	pause.wait_until_held();

	let rotation = {
		let tree = Arc::clone(&tree);
		spawn_bounded(move || tree.core.inner.rotate_memtable())
	};
	// The segment has data that was not fsynced, so the rotation needs the gate.
	pause.wait_for_fsyncs_asked(2);
	pause.release.send(()).unwrap();

	assert!(finished(flush).is_err());
	let first = finished(rotation);
	assert_eq!(
		pause.calls.load(Ordering::SeqCst),
		1,
		"the rotation did not fsync the segment again"
	);
	if first.is_err() {
		tree.core.inner.rotate_memtable().unwrap();
	}
	assert_eq!(active(&tree), before + 1);
	assert!(!sync_failed(&tree));
	commit_ok(&tree, &pairs("again", 2, b'g'), Durability::Immediate).await;
	tree.flush_wal(true).unwrap();
	finish(&tree);
}

/// The fsync of `flush_wal(true)` runs outside the WAL lock. If it fails after a rotation has
/// replaced its segment, the failure belongs to the segment it was made for. That segment has
/// nothing pending, so the rotation needs no fsync of it and does not wait for the one in flight;
/// the new segment must take commits and fsyncs as if nothing had happened.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rotation_that_finishes_inside_a_failing_out_of_lock_fsync_leaves_the_new_segment_alone()
{
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = open_tree(dir.path()).await;
	let first = active(&tree);
	let acked = pairs("acked", 3, b'a');
	commit_ok(&tree, &acked, Durability::Immediate).await;
	assert!(!pending_sync(&tree), "an Immediate commit leaves nothing pending");

	arm(&tree, 0);
	let pause = pause_next_fsync(&tree);
	let late = {
		let tree = Arc::clone(&tree);
		spawn_bounded(move || tree.flush_wal(true))
	};
	pause.wait_until_held();

	tree.core.inner.rotate_memtable().unwrap();
	assert_eq!(active(&tree), first + 1);
	pause.release.send(()).unwrap();

	let err = finished(late).unwrap_err();
	assert!(err.to_string().contains("injected fsync failure"), "{err}");
	assert!(!sync_failed(&tree), "the failure belongs to the segment that was replaced");
	let again = pairs("again", 3, b'g');
	commit_ok(&tree, &again, Durability::Immediate).await;
	tree.flush_wal(true).unwrap();
	for (key, value) in acked.iter().chain(&again) {
		assert_eq!(get(&tree, key).as_deref(), Some(value.as_slice()), "{key}");
	}
	finish(&tree);
}

#[derive(Default)]
struct Outcomes {
	/// Commit-ordered `(key, acknowledged)`.
	commits: Vec<(String, bool)>,
	/// The distinct errors seen, with how often.
	errors: BTreeMap<String, usize>,
}

impl Outcomes {
	fn acknowledged_after_first_failure(&self) -> usize {
		self.commits.iter().skip_while(|(_, ok)| *ok).filter(|(_, ok)| *ok).count()
	}

	fn failures(&self) -> usize {
		self.commits.iter().filter(|(_, ok)| !*ok).count()
	}
}

/// Single-key Immediate commits from several tasks, `flush_wal(true)` from a thread, and a thread
/// that replaces the WAL segment whenever its fsync failed and arms the next two segments to fail
/// too, all against failing fsyncs. Whatever the interleaving:
///
/// - a commit that failed is not readable, and one that was acknowledged is;
/// - in no segment is a record that was acknowledged behind one that was not, which is the hole
///   recovery would stop at;
/// - every acknowledged commit is recovered from the live files and from images in which each
///   segment is cut, or zero-filled, from its first record that was not acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledged_commits_never_sit_behind_a_failed_one_under_concurrent_failures() {
	// `(first fsync to fail, flush_wal(true) thread running)`. Without that thread the first
	// failure is always a group's, so the failed group has records in the segment.
	let rounds = [(0usize, false), (1, false), (0, true)];
	let mut failed_records_seen = 0;
	for (round, (fail_at, with_flush)) in rounds.into_iter().enumerate() {
		let root = TempDir::new("wal_fsync").unwrap();
		let live = root.path().join("live");
		let tree = open_tree(&live).await;
		let outcomes = Arc::new(Mutex::new(Outcomes::default()));
		let base = pairs("base", 3, b'a');
		commit_ok(&tree, &base, Durability::Immediate).await;
		arm(&tree, fail_at);

		let stop = Arc::new(AtomicBool::new(false));
		let _stop_on_failure = SetOnDrop(Arc::clone(&stop));
		let mut writers = Vec::new();
		for writer in 0..4 {
			let (tree, outcomes, stop) =
				(Arc::clone(&tree), Arc::clone(&outcomes), Arc::clone(&stop));
			writers.push(tokio::spawn(async move {
				let mut i = 0;
				while !stop.load(Ordering::Acquire) {
					let key = format!("w{round}-{writer}-{i}");
					let result = commit_one(&tree, &key, Durability::Immediate).await;
					{
						let mut outcomes = outcomes.lock().unwrap();
						if let Err(e) = &result {
							*outcomes.errors.entry(e.to_string()).or_default() += 1;
						}
						outcomes.commits.push((key, result.is_ok()));
					}
					i += 1;
					tokio::task::yield_now().await;
				}
			}));
		}
		let mut threads = Vec::new();
		if with_flush {
			let (tree, stop) = (Arc::clone(&tree), Arc::clone(&stop));
			threads.push(spawn_bounded(move || {
				while !stop.load(Ordering::Acquire) {
					let _ = tree.flush_wal(true);
					std::thread::sleep(Duration::from_millis(1));
				}
			}));
		}
		{
			let (tree, stop) = (Arc::clone(&tree), Arc::clone(&stop));
			threads.push(spawn_bounded(move || {
				// Three segments are replaced, the last of them armed to fail never.
				let mut next = [Some(2usize), Some(0), None].into_iter();
				while !stop.load(Ordering::Acquire) {
					if sync_failed(&tree) && tree.core.inner.seal_active_wal_segment().is_ok() {
						if let Some(Some(syncs)) = next.next() {
							arm(&tree, syncs);
						}
					}
					std::thread::sleep(Duration::from_millis(2));
				}
			}));
		}

		let started = Instant::now();
		loop {
			{
				let outcomes = outcomes.lock().unwrap();
				if outcomes.acknowledged_after_first_failure() >= 12 && outcomes.failures() >= 2 {
					break;
				}
			}
			if started.elapsed() > Duration::from_secs(30) {
				let outcomes = outcomes.lock().unwrap();
				panic!(
					"round {round}: commits did not resume after a failed fsync: {} commits, {} \
					 acknowledged, errors {:?}, writer poisoned: {}, log {}",
					outcomes.commits.len(),
					outcomes.commits.iter().filter(|(_, ok)| *ok).count(),
					outcomes.errors,
					sync_failed(&tree),
					active(&tree),
				);
			}
			tokio::time::sleep(Duration::from_millis(5)).await;
		}
		stop.store(true, Ordering::Release);
		for writer in writers {
			tokio::time::timeout(Duration::from_secs(30), writer)
				.await
				.expect("a writer is stuck")
				.unwrap();
		}
		for thread in threads {
			finished(thread);
		}

		let outcomes = Arc::try_unwrap(outcomes).ok().unwrap().into_inner().unwrap();
		let mut acknowledged: Vec<String> = base.iter().map(|(key, _)| key.clone()).collect();
		acknowledged.extend(outcomes.commits.iter().filter(|(_, ok)| *ok).map(|(k, _)| k.clone()));

		// Live tree: nothing of a failed commit is visible, everything acknowledged is.
		for (key, ok) in &outcomes.commits {
			match ok {
				true => assert!(get(&tree, key).is_some(), "round {round}: {key}"),
				false => assert_eq!(get(&tree, key), None, "round {round}: {key} failed"),
			}
		}

		// Segments: no acknowledged record behind a failed one.
		let failed: HashSet<&str> =
			outcomes.commits.iter().filter(|(_, ok)| !*ok).map(|(k, _)| k.as_str()).collect();
		let mut segments: Vec<PathBuf> = fs::read_dir(live.join("wal"))
			.unwrap()
			.map(|e| e.unwrap().path())
			.filter(|p| p.extension().is_some_and(|ext| ext == "wal"))
			.collect();
		segments.sort();
		let mut first_failed: Vec<(u64, u64)> = Vec::new();
		for segment in &segments {
			let mut start = 0;
			let mut hole = None;
			for (end, batch) in records_with_ends(segment) {
				let keys = keys_of(&batch);
				let any_failed = keys.iter().any(|k| failed.contains(k.as_str()));
				if hole.is_none() && any_failed {
					hole = Some(start);
				}
				if hole.is_some() && !any_failed {
					panic!(
						"round {round}: {keys:?} was acknowledged behind a failed record in {}",
						segment.display()
					);
				}
				start = end;
			}
			if let Some(hole) = hole {
				let id: u64 =
					segment.file_stem().unwrap().to_str().unwrap().parse().expect("segment id");
				first_failed.push((id, hole));
			}
		}
		failed_records_seen += first_failed.len();
		if !with_flush {
			assert!(!first_failed.is_empty(), "round {round}: a failed group left no record");
		}

		// Images: the live files, and each segment cut or zero-filled from its first failed record.
		let images: &[&str] = if round == 1 {
			&["live", "cut", "zero"]
		} else {
			&["live", "zero"]
		};
		for (n, damage) in images.iter().enumerate() {
			let image = root.path().join(format!("image{n}"));
			copy_dir(&live, &image);
			for (id, hole) in &first_failed {
				let segment = seg_path(&image, *id);
				match *damage {
					"cut" => set_len(&segment, *hole),
					"zero" => zero_range(&segment, *hole, file_len(&segment)),
					_ => {}
				}
			}
			let reopened = reopen(&image);
			for key in &acknowledged {
				assert!(
					get(&reopened, key).is_some(),
					"round {round}, {damage}: {key} was acknowledged"
				);
			}
			finish(&reopened);
		}
		finish(&tree);
	}
	assert!(failed_records_seen > 0, "no round left a failed record behind");
}

// ---------------------------------------------------------------------------
// a group that fails after a part of it was applied
// ---------------------------------------------------------------------------

/// A memtable of about 21 commits, so that a group of 30 fills it part-way: the first batches are
/// applied, the memtable rotates, and the rest is logged again in the new segment.
async fn open_small_tree(dir: &Path) -> Arc<Tree> {
	open_tree_with(dir, 4096).await
}

fn readable(tree: &Tree, group: &[(String, Vec<u8>)]) -> Vec<String> {
	group.iter().filter(|(key, _)| get(tree, key).is_some()).map(|(key, _)| key.clone()).collect()
}

/// A group that straddles a memtable fill is applied up to the fill, the memtable rotates, and the
/// rest is logged again in the new segment and synced there. If that fsync fails, the whole group
/// is reported failed, but the part that was applied already sits in the memtable, and the next
/// commit that advances the visible sequence number makes it readable. The same goes for a failed
/// append in the second log: it is not specific to fsync.
#[test(tokio::test)]
#[ignore = "pre-existing: a group that fails after part of it was applied is reported failed in \
            full, and the part that was applied becomes readable"]
async fn nothing_of_a_group_that_failed_while_being_logged_again_becomes_readable() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = open_small_tree(dir.path()).await;

	// Round 0 applies the group up to the fill and rotates; the segment that round 1 logs the
	// rest in is the one whose fsync fails.
	let inner = Arc::clone(&tree.core.inner);
	let fired = Arc::new(AtomicBool::new(false));
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::BeforeApplyRound {
			round: 1,
		} = point
		{
			if !fired.swap(true, Ordering::SeqCst) {
				inner.wal.write().fail_syncs_after(0);
			}
		}
	})));

	let group = pairs("group", 30, b'g');
	all_failed(&commit_group(&tree, &group, Durability::Immediate).await);
	tree.core.commit_pipeline.set_hook(None);
	assert!(sync_failed(&tree), "the fsync of the new segment failed");
	assert_eq!(readable(&tree, &group), Vec::<String>::new(), "right after the failure");

	// A commit in a new segment publishes a sequence number above the failed group's.
	tree.core.inner.seal_active_wal_segment().unwrap();
	commit_one(&tree, "later", Durability::Immediate).await.unwrap();
	assert_eq!(readable(&tree, &group), Vec::<String>::new(), "after a later commit");
	finish(&tree);
}

/// The same with an Eventual group, which has no fsync of its own: the rotation in the middle of
/// the group fsyncs the old segment, and that fsync fails.
#[test(tokio::test)]
#[ignore = "pre-existing: a group that fails after part of it was applied is reported failed in \
            full, and the part that was applied becomes readable"]
async fn nothing_of_a_group_whose_mid_group_rotation_failed_becomes_readable() {
	let dir = TempDir::new("wal_fsync").unwrap();
	let tree = open_small_tree(dir.path()).await;

	arm(&tree, 0);
	let group = pairs("group", 30, b'g');
	all_failed(&commit_group(&tree, &group, Durability::Eventual).await);
	assert_eq!(readable(&tree, &group), Vec::<String>::new(), "right after the failure");

	// The next rotation goes ahead without fsyncing the segment that failed.
	tree.core.inner.rotate_memtable().unwrap();
	commit_one(&tree, "later", Durability::Immediate).await.unwrap();
	assert_eq!(readable(&tree, &group), Vec::<String>::new(), "after a later commit");
	finish(&tree);
}
