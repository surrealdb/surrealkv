//! A WAL segment that failed is replaced by the next commit.
//!
//! A failed append or fsync ends a segment: its writer refuses every later append and sync. The
//! commit pipeline replaces such a segment before it logs the next group, so the database keeps
//! accepting commits without a restart, whether the active memtable is empty, holds data or is
//! full. The failed group's commits stay failed, and the segment stays on disk until the
//! memtables tagged with it are in tables.
//!
//! A segment whose fsync failed is not fsynced again, so what it holds past its last good fsync
//! is of unknown durability. The segment that replaces it therefore refuses appends and syncs
//! until those memtables are in tables: no later commit is logged, or acknowledged, ahead of
//! data that a crash may drop.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;

use crate::batch::Batch;
use crate::ring::PipelineHook;
use crate::wal::manager::Wal;
use crate::wal::reader::Reader;
use crate::wal::{Error as WalError, Options as WalOptions};
use crate::{Durability, Error, InternalKeyKind, Mode, Options, Tree};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

const BIG: usize = 100 * 1024 * 1024;

fn options(path: &Path, max_memtable_size: usize) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		max_memtable_size,
		flush_on_close: false,
		// The background flush is stopped in most tests, so a rotation that queues a memtable
		// must not stall the commits behind it.
		memtable_stall_threshold: 1000,
		..Default::default()
	})
}

fn seg_path(dir: &Path, id: u64) -> PathBuf {
	dir.join("wal").join(format!("{id:020}.wal"))
}

fn active(tree: &Tree) -> u64 {
	tree.core.inner.wal.read().get_active_log_number()
}

fn tag(tree: &Tree) -> u64 {
	tree.core.inner.active_memtable.read().unwrap().get_wal_number()
}

fn log_number(tree: &Tree) -> u64 {
	tree.core.inner.level_manifest.read().unwrap().get_log_number()
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

/// A tree whose background tasks are stopped, so no segment is flushed and removed behind the
/// test's back, and that is never closed. Shared through an `Arc`, never `Tree::clone`: dropping
/// any clone of a `Tree` spawns `core.close()`.
async fn open_tree(dir: &Path) -> Arc<Tree> {
	open_tree_with(dir, BIG).await
}

async fn open_tree_with(dir: &Path, max_memtable_size: usize) -> Arc<Tree> {
	let tree = Arc::new(Tree::new(options(dir, max_memtable_size)).unwrap());
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
	tree.core.is_closed.store(true, Ordering::SeqCst);
	tree
}

/// Ends a tree the way a crash does: no close, no flush.
fn finish(tree: &Tree) {
	tree.core.is_closed.store(true, Ordering::SeqCst);
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
}

/// Reopens a copy of a data directory the way a restart does.
fn reopen(image: &Path) -> Tree {
	Tree::new(options(image, BIG)).unwrap()
}

fn get(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key.as_bytes()).unwrap()
}

fn pairs(prefix: &str, count: usize, fill: u8) -> Vec<(String, Vec<u8>)> {
	(0..count).map(|i| (format!("{prefix}{i}"), vec![fill; 40])).collect()
}

async fn commit_one(tree: &Tree, key: &str, durability: Durability) -> crate::Result<()> {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(durability);
	txn.set(key.as_bytes(), &[b'v'; 40]).unwrap();
	txn.commit().await
}

/// Commits each entry in its own transaction, in order, and expects each to be acknowledged.
async fn commit_ok(tree: &Tree, entries: &[(String, Vec<u8>)], durability: Durability) {
	for (key, value) in entries {
		let mut txn = tree.begin().unwrap();
		txn.set_durability(durability);
		txn.set(key.as_bytes(), value).unwrap();
		txn.commit().await.unwrap();
	}
}

fn assert_all(tree: &Tree, entries: &[(String, Vec<u8>)], at: &str) {
	for (key, value) in entries {
		assert_eq!(get(tree, key).as_ref(), Some(value), "{at}: {key}");
	}
}

/// Keys of every record of a segment, in order. A segment that is not whole stops the read:
/// callers only use it on segments they have not damaged.
fn segment_keys(path: &Path) -> Vec<String> {
	let mut reader = Reader::new(File::open(path).unwrap());
	let mut keys = Vec::new();
	loop {
		match reader.read() {
			Ok((record, _)) => {
				let batch = Batch::decode(record).unwrap();
				keys.extend(
					batch.entries.iter().map(|e| String::from_utf8(e.key.clone()).unwrap()),
				);
			}
			Err(WalError::IO(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return keys,
			Err(e) => panic!("{} must read end to end: {e}", path.display()),
		}
	}
}

/// How the WAL of a tree is made to fail.
#[derive(Debug, Clone, Copy)]
enum Fault {
	/// The next fsync of the segment fails.
	Fsync,
	/// The write after the next `n` of them fails, and the segment is cut back.
	Write(usize),
}

fn inject(tree: &Tree, fault: Fault) {
	let mut wal = tree.core.inner.wal.write();
	match fault {
		Fault::Fsync => wal.fail_syncs_after(0),
		Fault::Write(n) => wal.fail_writes_after(n),
	}
}

/// Fails an Immediate commit by injecting `fault` first, and returns its error.
async fn fail_a_commit(tree: &Tree, fault: Fault, key: &str) -> String {
	inject(tree, fault);
	commit_one(tree, key, Durability::Immediate)
		.await
		.expect_err("the commit was meant to fail")
		.to_string()
}

fn held(tree: &Tree) -> bool {
	tree.core.inner.wal.read().is_held()
}

/// Lets every replacement through at once, whatever came before.
fn no_backoff(tree: &Tree) {
	tree.core.commit_pipeline.set_replace_backoff(Duration::ZERO, Duration::ZERO);
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

/// What an image of a segment whose fsync failed can hold of it.
#[derive(Debug, Clone, Copy)]
enum Damage {
	/// Every byte the writer wrote.
	None,
	/// The segment ends at the offset.
	CutAt(u64),
	/// The segment keeps its length and reads as zeros from the offset.
	ZeroFrom(u64),
}

fn damage_segment(image: &Path, id: u64, damage: Damage) {
	let segment = seg_path(image, id);
	if !segment.exists() {
		return;
	}
	match damage {
		Damage::None => {}
		Damage::CutAt(at) => set_len(&segment, at),
		Damage::ZeroFrom(from) => zero_range(&segment, from, file_len(&segment)),
	}
}

/// The names in the WAL directory of a data directory, sorted.
fn wal_listing(dir: &Path) -> Vec<String> {
	let mut names: Vec<String> = fs::read_dir(dir.join("wal"))
		.unwrap()
		.map(|e| e.unwrap().file_name().to_string_lossy().to_string())
		.collect();
	names.sort();
	names
}

/// What holds the flusher at the start of a heal until `release` is called.
struct Park {
	entered: mpsc::Receiver<()>,
	release: mpsc::Sender<()>,
}

impl Park {
	/// Waits until the flusher is held.
	fn wait_until_parked(&self) {
		self.entered.recv_timeout(Duration::from_secs(20)).expect("the flusher began a heal");
	}
}

/// Holds the flusher the first time it begins to heal the WAL.
fn park_before_heal(tree: &Tree) -> Park {
	let (entered_tx, entered) = mpsc::channel();
	let (release, release_rx) = mpsc::channel::<()>();
	let (entered_tx, release_rx) = (Mutex::new(entered_tx), Mutex::new(release_rx));
	let first = AtomicBool::new(true);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if matches!(point, PipelineHook::BeforeWalHeal) && first.swap(false, Ordering::SeqCst) {
			entered_tx.lock().unwrap().send(()).unwrap();
			// A test that fails before it releases must not leave the flusher parked for good.
			let _ = release_rx.lock().unwrap().recv_timeout(Duration::from_secs(60));
		}
	})));
	Park {
		entered,
		release,
	}
}

/// Runs `f` on a thread of its own, and waits for its result for a bounded time.
fn on_a_thread<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
	let (tx, rx) = mpsc::channel();
	std::thread::spawn(move || {
		let _ = tx.send(f());
	});
	rx.recv_timeout(Duration::from_secs(60)).expect("a thread of the test is stuck")
}

/// Waits until `done` holds, for a bounded time.
async fn until(what: &str, done: impl Fn() -> bool) {
	let started = Instant::now();
	while !done() {
		assert!(started.elapsed() < Duration::from_secs(30), "timed out waiting for {what}");
		tokio::time::sleep(Duration::from_millis(5)).await;
	}
}

// ---------------------------------------------------------------------------
// the wedge: an empty memtable, a memtable with room
// ---------------------------------------------------------------------------

/// With nothing in the active memtable no rotation was ever due, so a failed segment was never
/// replaced. The commit after the failure is acknowledged in the next segment, whatever failed
/// and whatever the durability of the commits are. The failed commit is not readable, the
/// segment that failed is not touched by the replacement, the new one holds exactly the commit
/// that follows, and a crash image taken once it is acknowledged recovers it.
#[test(tokio::test)]
async fn an_empty_memtable_does_not_wedge_after_a_failed_fsync_or_append() {
	for fault in [Fault::Fsync, Fault::Write(0), Fault::Write(1), Fault::Write(2)] {
		for durability in [Durability::Immediate, Durability::Eventual] {
			let at = format!("{fault:?}, {durability:?}");
			let root = TempDir::new("wal_replace").unwrap();
			let live = root.path().join("live");
			let tree = open_tree(&live).await;
			let first = active(&tree);
			assert!(tree.core.inner.active_memtable.read().unwrap().is_empty());

			inject(&tree, fault);
			let error = commit_one(&tree, "failed", Durability::Immediate)
				.await
				.expect_err("the group's append or fsync failed")
				.to_string();
			assert!(error.contains("Group commit failed"), "{at}: {error}");
			assert_eq!(active(&tree), first, "{at}: nothing replaced the segment yet");

			let failed_segment = fs::read(seg_path(&live, first)).unwrap();
			commit_one(&tree, "next", durability)
				.await
				.unwrap_or_else(|e| panic!("{at}: the commit after a failure must succeed: {e}"));
			commit_one(&tree, "after", durability).await.unwrap();

			assert_eq!(active(&tree), first + 1, "{at}");
			assert_eq!(tag(&tree), first + 1, "{at}: the memtable moved with the WAL");
			assert_eq!(
				fs::read(seg_path(&live, first)).unwrap(),
				failed_segment,
				"{at}: the segment that failed is as it was"
			);
			assert_eq!(get(&tree, "failed"), None, "{at}: its commit stays failed");
			assert!(get(&tree, "next").is_some(), "{at}");
			assert!(get(&tree, "after").is_some(), "{at}");
			assert_eq!(segment_keys(&seg_path(&live, first + 1)), ["next", "after"], "{at}");

			let image = root.path().join("image");
			copy_dir(&live, &image);
			let recovered = reopen(&image);
			assert!(get(&recovered, "next").is_some(), "{at}: acknowledged, recovered");
			assert!(get(&recovered, "after").is_some(), "{at}: acknowledged, recovered");
			finish(&recovered);
			finish(&tree);
		}
	}
}

/// A memtable that holds data and has room never fills, so it never rotated either. After a
/// failed fsync the next commit replaces the segment, the memtable tagged with the segment that
/// failed is flushed to a table before the commit is acknowledged, and the segment is removed once
/// the manifest passed it. What was acknowledged, Eventual commits included, reads back from the
/// table.
#[test(tokio::test)]
async fn a_non_empty_memtable_is_flushed_before_the_next_commit_is_acknowledged() {
	let root = TempDir::new("wal_replace").unwrap();
	let live = root.path().join("live");
	let tree = open_tree(&live).await;
	let first = active(&tree);

	let prefill = pairs("pre", 3, b'p');
	commit_ok(&tree, &prefill, Durability::Immediate).await;
	let eventual = pairs("eventual", 2, b'e');
	commit_ok(&tree, &eventual, Durability::Eventual).await;

	inject(&tree, Fault::Fsync);
	assert!(commit_one(&tree, "failed", Durability::Immediate).await.is_err());
	assert_eq!(tree.core.inner.l0_file_count(), 0);

	commit_one(&tree, "next", Durability::Immediate).await.unwrap();

	assert_eq!(tree.core.inner.l0_file_count(), 1, "the memtable of the failed segment is a table");
	assert_eq!(tree.core.inner.immutable_count(), 0);
	assert_eq!(log_number(&tree), first + 1);
	assert!(!seg_path(&live, first).exists(), "the flushed segment was removed");
	assert_eq!(active(&tree), first + 1);
	assert_eq!(tag(&tree), first + 1);
	assert_all(&tree, &prefill, "from the table");
	assert_all(&tree, &eventual, "from the table");
	assert!(get(&tree, "next").is_some());
	assert_eq!(get(&tree, "failed"), None);
	assert_eq!(segment_keys(&seg_path(&live, first + 1)), ["next"]);
	finish(&tree);
}

/// Poison that no commit group caused: a failed `flush_wal(true)` is replaced by the next commit
/// the same way, and the Eventual commits before it are in a table by then.
#[test(tokio::test)]
async fn a_failed_flush_wal_is_replaced_by_the_next_commit() {
	let root = TempDir::new("wal_replace").unwrap();
	let live = root.path().join("live");
	let tree = open_tree(&live).await;
	let first = active(&tree);

	let eventual = pairs("eventual", 3, b'e');
	commit_ok(&tree, &eventual, Durability::Eventual).await;
	inject(&tree, Fault::Fsync);
	let error = tree.flush_wal(true).unwrap_err().to_string();
	assert!(error.contains("injected fsync failure"), "{error}");
	assert_eq!(active(&tree), first);

	commit_one(&tree, "next", Durability::Immediate).await.unwrap();
	assert_eq!(active(&tree), first + 1);
	assert_eq!(tree.core.inner.l0_file_count(), 1);
	assert!(!seg_path(&live, first).exists());
	assert_all(&tree, &eventual, "from the table");
	assert!(get(&tree, "next").is_some());
	finish(&tree);
}

// ---------------------------------------------------------------------------
// the hold
// ---------------------------------------------------------------------------

fn wal_record(seq: u64, key: &str) -> Vec<u8> {
	let mut batch = Batch::new(seq);
	batch
		.add_record(InternalKeyKind::Set, key.as_bytes().to_vec(), Some(vec![b'v'; 50]), 0)
		.unwrap();
	batch.encode().unwrap()
}

/// The segment that replaces one whose fsync failed refuses every append and sync, and writes
/// nothing: it is not cut and not poisoned by the refusal. The hold ends only for the bound it
/// was set with.
#[test]
fn a_rotation_after_a_failed_fsync_gives_a_held_writer_that_refuses_appends_and_syncs() {
	let dir = TempDir::new("wal_replace").unwrap();
	let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
	wal.append(&wal_record(1, "acked")).unwrap();
	wal.sync().unwrap();
	wal.append(&wal_record(2, "unsynced")).unwrap();
	wal.fail_syncs_after(0);
	assert!(wal.sync().is_err());
	assert!(wal.needs_replacement());
	assert!(!wal.is_held());

	assert_eq!(wal.rotate().unwrap(), 1);
	assert!(!wal.needs_replacement(), "the new segment is healthy");
	assert!(wal.is_held());
	assert_eq!(wal.hold_below(), Some(1));
	let segment = dir.path().join("00000000000000000001.wal");

	let rec = wal_record(3, "refused");
	for result in
		[wal.append(&rec).map(|_| ()), wal.append_group(&rec, &[rec.len()]).map(|_| ()), wal.sync()]
	{
		let error = result.expect_err("a held writer takes nothing").to_string();
		assert!(error.contains("WAL segment is held"), "{error}");
	}
	assert_eq!(file_len(&segment), 0, "a refusal writes nothing");
	assert!(wal.flush().is_ok(), "a flush to the OS cache is not an fsync");

	wal.release_hold(0);
	assert!(wal.is_held(), "a bound that is not the hold's leaves it alone");
	wal.release_hold(1);
	assert!(!wal.is_held());
	wal.append(&wal_record(3, "after")).unwrap();
	wal.sync().unwrap();
}

/// A held writer has nothing to sync, but closing it is not a success: the segment that failed
/// was not made durable, and the close says so.
#[test]
fn closing_a_held_writer_reports_the_hold() {
	let dir = TempDir::new("wal_replace").unwrap();
	let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
	wal.append(&wal_record(1, "unsynced")).unwrap();
	wal.fail_syncs_after(0);
	assert!(wal.sync().is_err());
	wal.rotate().unwrap();

	let error = wal.close().unwrap_err().to_string();
	assert!(error.contains("WAL segment is held"), "{error}");
}

/// A segment that failed only to append has its tail cut off, and the cut was fsynced: nothing is
/// in doubt, so the segment that replaces it is not held.
#[test]
fn a_failed_append_alone_does_not_hold_the_next_writer() {
	let dir = TempDir::new("wal_replace").unwrap();
	let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
	wal.append(&wal_record(1, "acked")).unwrap();
	wal.fail_writes_after(0);
	assert!(wal.append(&wal_record(2, "failed")).is_err());
	assert!(wal.needs_replacement());

	wal.rotate().unwrap();
	assert!(!wal.is_held());
	wal.append(&wal_record(3, "after")).unwrap();
	wal.sync().unwrap();
}

/// A rotation of a writer that is held makes a writer that is held, with the bound of the first,
/// and does not fsync: there is nothing in it to sync.
#[test]
fn a_held_writer_is_replaced_by_one_with_the_same_bound_and_is_not_synced() {
	let dir = TempDir::new("wal_replace").unwrap();
	let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
	wal.append(&wal_record(1, "unsynced")).unwrap();
	wal.fail_syncs_after(0);
	assert!(wal.sync().is_err());
	assert_eq!(wal.rotate().unwrap(), 1);

	let fsyncs = Arc::new(AtomicUsize::new(0));
	let counter = Arc::clone(&fsyncs);
	wal.set_sync_observer(Some(Arc::new(move || {
		counter.fetch_add(1, Ordering::SeqCst);
	})));
	let mut before_syncs = 0;
	assert_eq!(
		wal.rotate_with(|| {
			before_syncs += 1;
			Ok(())
		})
		.unwrap(),
		2
	);
	assert_eq!(before_syncs, 0, "nothing to make durable before it");
	assert_eq!(fsyncs.load(Ordering::SeqCst), 0);
	assert_eq!(wal.hold_below(), Some(1), "the bound is the one of the segment that failed");
	wal.release_hold(2);
	assert!(wal.is_held());
	wal.release_hold(1);
	assert!(!wal.is_held());
}

// ---------------------------------------------------------------------------
// the barrier
// ---------------------------------------------------------------------------

/// Commits made after a failed fsync never sit in a crash image ahead of data of the failed
/// segment that the image can lose. The Eventual commit `e` is in the segment whose fsync failed,
/// and what the segment holds past its last good fsync can be gone. The commit `i` that follows is
/// acknowledged only once `e` is in a table, so no image holds `i` without `e`.
///
/// Images: right after the failed group, inside the flush of the memtable of the failed segment
/// (the new segment exists, the table is written, the manifest is not updated), and once `i` is
/// acknowledged. Each is checked as it is and with the failed segment cut or zeroed.
#[test(tokio::test)]
async fn no_crash_image_holds_a_commit_acknowledged_after_a_failed_fsync_without_the_eventual_commits_before_it(
) {
	let root = TempDir::new("wal_replace").unwrap();
	let live = root.path().join("live");
	let tree = open_tree(&live).await;
	let first = active(&tree);
	let segment = seg_path(&live, first);

	commit_one(&tree, "e", Durability::Eventual).await.unwrap();
	let e_end = file_len(&segment);
	fail_a_commit(&tree, Fault::Fsync, "failed").await;

	let image_a = root.path().join("image_a");
	copy_dir(&live, &image_a);
	let image_b = root.path().join("image_b");
	let calls = Arc::new(AtomicUsize::new(0));
	{
		let (live, image_b, calls) = (live.clone(), image_b.clone(), Arc::clone(&calls));
		*tree.core.inner.flush_hook.lock() = Some(Arc::new(move |_| {
			assert_eq!(calls.fetch_add(1, Ordering::SeqCst), 0, "one flush releases the segment");
			copy_dir(&live, &image_b);
			Ok(())
		}));
	}
	commit_one(&tree, "i", Durability::Immediate).await.unwrap();
	*tree.core.inner.flush_hook.lock() = None;
	assert_eq!(calls.load(Ordering::SeqCst), 1, "the memtable of the failed segment was flushed");
	let image_c = root.path().join("image_c");
	copy_dir(&live, &image_c);

	assert!(!seg_path(&image_a, first + 1).exists());
	assert_eq!(file_len(&seg_path(&image_b, first + 1)), 0, "the held segment holds no record");
	assert!(!seg_path(&image_c, first).exists(), "the segment that failed is gone once flushed");

	for (name, image) in [("a", &image_a), ("b", &image_b), ("c", &image_c)] {
		for damage in [
			Damage::None,
			Damage::CutAt(0),
			Damage::CutAt(e_end),
			Damage::ZeroFrom(0),
			Damage::ZeroFrom(e_end),
		] {
			let at = format!("image {name}, {damage:?}");
			let copy = root.path().join(format!("{name}-{damage:?}").replace(['(', ')'], "_"));
			copy_dir(image, &copy);
			damage_segment(&copy, first, damage);
			let recovered = reopen(&copy);
			let (e, i) = (get(&recovered, "e"), get(&recovered, "i"));
			assert!(i.is_none() || e.is_some(), "{at}: `i` is there without `e`");
			match (name, damage) {
				("a" | "b", _) => assert!(i.is_none(), "{at}: `i` was not logged yet"),
				("c", _) => assert!(e.is_some() && i.is_some(), "{at}: both are in tables"),
				_ => unreachable!(),
			}
			finish(&recovered);
		}
	}
	finish(&tree);
}

/// While a memtable of the failed segment is not in a table, no group reaches the new segment, so
/// every commit fails, and `flush_wal(true)` fails with the hold error without touching the disk.
/// Nothing is recorded as a background error, so nothing depends on a background task to recover:
/// once the flush works, the next commit is acknowledged. Replacements that follow each other are
/// spaced out by the backoff, and a commit inside the window fails without a flush.
#[test(tokio::test)]
async fn a_failed_barrier_flush_fails_the_group_logs_nothing_and_flush_wal_until_a_flush_succeeds()
{
	let root = TempDir::new("wal_replace").unwrap();
	let live = root.path().join("live");
	let tree = open_tree(&live).await;
	no_backoff(&tree);
	let first = active(&tree);

	commit_one(&tree, "e", Durability::Eventual).await.unwrap();
	let calls = Arc::new(AtomicUsize::new(0));
	let failing = Arc::new(AtomicBool::new(true));
	{
		let (calls, failing) = (Arc::clone(&calls), Arc::clone(&failing));
		*tree.core.inner.flush_hook.lock() = Some(Arc::new(move |_| {
			calls.fetch_add(1, Ordering::SeqCst);
			match failing.load(Ordering::SeqCst) {
				true => Err(Error::Other("injected flush failure".into())),
				false => Ok(()),
			}
		}));
	}
	fail_a_commit(&tree, Fault::Fsync, "failed").await;

	let error = commit_one(&tree, "i", Durability::Immediate).await.unwrap_err().to_string();
	assert!(error.contains("Failed to flush the memtables of the failed WAL segment"), "{error}");
	assert!(error.contains("injected flush failure"), "{error}");
	assert_eq!(calls.load(Ordering::SeqCst), 1);
	assert_eq!(active(&tree), first + 1, "the segment was replaced");
	assert!(held(&tree));
	assert_eq!(file_len(&seg_path(&live, first + 1)), 0, "nothing was logged");
	assert!(tree.core.inner.error_handler.get_error().is_none(), "nothing was recorded");

	let fsyncs = tree.core.inner.wal.fsyncs();
	let error = tree.flush_wal(true).unwrap_err().to_string();
	assert!(error.contains("WAL segment is held"), "{error}");
	assert_eq!(tree.core.inner.wal.fsyncs(), fsyncs, "a refused flush_wal does no fsync");

	assert!(commit_one(&tree, "i", Durability::Immediate).await.is_err());
	assert_eq!(calls.load(Ordering::SeqCst), 2, "every commit runs the flush again");

	// Inside the window a commit fails without touching the memtables.
	tree.core
		.commit_pipeline
		.set_replace_backoff(Duration::from_secs(3600), Duration::from_secs(3600));
	assert!(commit_one(&tree, "i", Durability::Immediate).await.is_err());
	assert_eq!(calls.load(Ordering::SeqCst), 3);
	let error = commit_one(&tree, "i", Durability::Immediate).await.unwrap_err().to_string();
	assert!(error.contains("the next replacement is due"), "{error}");
	assert_eq!(calls.load(Ordering::SeqCst), 3, "no flush inside the window");
	assert_eq!(file_len(&seg_path(&live, first + 1)), 0);

	failing.store(false, Ordering::SeqCst);
	no_backoff(&tree);
	commit_one(&tree, "i", Durability::Immediate).await.unwrap();
	assert!(!held(&tree));
	assert_eq!(tree.core.inner.l0_file_count(), 1);
	assert!(get(&tree, "e").is_some() && get(&tree, "i").is_some());
	tree.flush_wal(true).unwrap();
	finish(&tree);
}

/// If creating the new segment fails, the commit fails with an error that names the replacement,
/// and the WAL, the memtable and its tag are as they were. The next commit tries again, and
/// succeeds once the cause is gone. With an empty memtable and with one that holds data.
#[test(tokio::test)]
async fn a_failed_replacement_fails_the_commit_changes_nothing_and_the_next_commit_retries() {
	for non_empty in [false, true] {
		let root = TempDir::new("wal_replace").unwrap();
		let live = root.path().join("live");
		let tree = open_tree(&live).await;
		no_backoff(&tree);
		let first = active(&tree);
		if non_empty {
			commit_one(&tree, "e", Durability::Eventual).await.unwrap();
		}
		fail_a_commit(&tree, Fault::Fsync, "failed").await;

		// A directory where the new segment goes: the file cannot be created.
		let blocker = seg_path(&live, first + 1);
		fs::create_dir(&blocker).unwrap();
		let listing = wal_listing(&live);
		let error = commit_one(&tree, "next", Durability::Immediate).await.unwrap_err().to_string();
		assert!(error.contains("replace the failed WAL segment"), "{non_empty}: {error}");
		assert_eq!(active(&tree), first, "{non_empty}");
		assert_eq!(tag(&tree), first, "{non_empty}");
		assert_eq!(tree.core.inner.immutable_count(), 0, "{non_empty}");
		assert!(!held(&tree), "{non_empty}");
		assert_eq!(tree.core.inner.active_memtable.read().unwrap().is_empty(), !non_empty);
		assert_eq!(wal_listing(&live), listing, "{non_empty}: no file was created");

		fs::remove_dir(&blocker).unwrap();
		commit_one(&tree, "next", Durability::Immediate).await.unwrap();
		assert_eq!(active(&tree), first + 1, "{non_empty}");
		assert_eq!(tag(&tree), first + 1, "{non_empty}");
		assert!(get(&tree, "next").is_some());
		assert_eq!(get(&tree, "e").is_some(), non_empty);
		finish(&tree);
	}
}

/// A disk that keeps failing costs a bounded rate of replacements. The first replacement after an
/// acknowledged commit is immediate. If the new segment fails before a commit is acknowledged,
/// the commits inside the window fail without creating a file or touching the WAL. Once the window
/// is open again the next commit replaces the segment and is acknowledged.
#[test(tokio::test)]
async fn consecutive_heals_back_off_and_create_no_file_inside_the_window() {
	let root = TempDir::new("wal_replace").unwrap();
	let live = root.path().join("live");
	let tree = open_tree(&live).await;
	let first = active(&tree);
	tree.core
		.commit_pipeline
		.set_replace_backoff(Duration::from_secs(3600), Duration::from_secs(3600));

	let heals = Arc::new(AtomicUsize::new(0));
	{
		let (heals, inner) = (Arc::clone(&heals), Arc::clone(&tree.core.inner));
		tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| match point {
			PipelineHook::BeforeWalHeal => {
				heals.fetch_add(1, Ordering::SeqCst);
			}
			// The segment that replaces the failed one fails to append, too.
			PipelineHook::AfterWalReplace => inner.wal.write().fail_writes_after(0),
			_ => {}
		})));
	}
	fail_a_commit(&tree, Fault::Fsync, "failed").await;
	assert_eq!(heals.load(Ordering::SeqCst), 0);

	let error = commit_one(&tree, "second", Durability::Immediate).await.unwrap_err().to_string();
	assert!(error.contains("injected write failure"), "the replacement failed too: {error}");
	assert_eq!(heals.load(Ordering::SeqCst), 1, "the first replacement is immediate");
	assert_eq!(active(&tree), first + 1);

	let listing = wal_listing(&live);
	for round in 0..10 {
		let error =
			commit_one(&tree, "later", Durability::Immediate).await.unwrap_err().to_string();
		assert!(error.contains("the next replacement is due"), "{round}: {error}");
	}
	assert_eq!(heals.load(Ordering::SeqCst), 1, "no heal began inside the window");
	assert_eq!(wal_listing(&live), listing, "no file was created inside the window");
	assert_eq!(active(&tree), first + 1);

	tree.core.commit_pipeline.set_hook(None);
	no_backoff(&tree);
	commit_one(&tree, "third", Durability::Immediate).await.unwrap();
	assert_eq!(active(&tree), first + 2);
	assert!(get(&tree, "third").is_some());
	finish(&tree);
}

// ---------------------------------------------------------------------------
// concurrent rotations
// ---------------------------------------------------------------------------

/// A checkpoint rotates the failed segment while the flusher is about to heal it. The writer that
/// replaces the segment is held by the rotation itself, so the barrier does not depend on the
/// checkpoint remembering anything: the flusher finds a healthy, held writer, releases it, and
/// the commit is acknowledged in the segment the checkpoint created. The checkpoint holds what was
/// acknowledged before the failure and nothing else. If the flush of the checkpoint fails, the hold
/// stays and the group's heal flushes the memtable.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_checkpoint_that_rotates_a_failed_segment_first_leaves_a_held_writer_that_the_flusher_releases(
) {
	for checkpoint_fails in [false, true] {
		let root = TempDir::new("wal_replace").unwrap();
		let tree = open_tree(&root.path().join("db")).await;
		let first = active(&tree);
		commit_one(&tree, "primer", Durability::Eventual).await.unwrap();
		fail_a_commit(&tree, Fault::Fsync, "failed").await;

		let park = park_before_heal(&tree);
		let commit = {
			let tree = Arc::clone(&tree);
			tokio::spawn(async move { commit_one(&tree, "next", Durability::Immediate).await })
		};
		park.wait_until_parked();

		if checkpoint_fails {
			*tree.core.inner.flush_hook.lock() =
				Some(Arc::new(|_| Err(Error::Other("injected flush failure".into()))));
		}
		let checkpoint = root.path().join("checkpoint");
		let made = {
			let (tree, checkpoint) = (Arc::clone(&tree), checkpoint.clone());
			on_a_thread(move || tree.create_checkpoint(&checkpoint))
		};
		assert_eq!(made.is_err(), checkpoint_fails);
		*tree.core.inner.flush_hook.lock() = None;
		assert!(held(&tree), "the rotation held the writer");
		assert_eq!(active(&tree), first + 1);
		park.release.send(()).unwrap();

		tokio::time::timeout(Duration::from_secs(60), commit)
			.await
			.expect("the commit is stuck")
			.unwrap()
			.unwrap();
		assert_eq!(active(&tree), first + 1, "the commit did not rotate again");
		assert!(!held(&tree));
		assert_eq!(tree.core.inner.immutable_count(), 0);
		assert!(get(&tree, "next").is_some() && get(&tree, "primer").is_some());

		if !checkpoint_fails {
			let restored_dir = root.path().join("restored");
			copy_dir(&checkpoint, &restored_dir);
			let restored = reopen(&restored_dir);
			assert!(get(&restored, "primer").is_some());
			assert_eq!(get(&restored, "failed"), None);
			assert_eq!(get(&restored, "next"), None, "made after the checkpoint");
			finish(&restored);
		}
		finish(&tree);
	}
}

/// What fails the gate between the check that finds a healthy segment and the append: a rotation
/// by the commit itself (the batch does not fit the memtable), or one by a checkpoint.
#[derive(Debug, Clone, Copy)]
enum Between {
	/// The group's own rotation, for a batch that does not fit what is left of the memtable.
	TheGroupsRotation,
	/// A rotation by someone else.
	AnotherRotation,
}

/// The flusher finds a healthy segment, and then an fsync of it fails and the segment is rotated
/// before the group is logged: the rotation skips the sealing fsync, so the writer it creates is
/// held, and the group is not logged in it. It is not acknowledged ahead of the Eventual commits
/// in the segment that failed, no crash image holds it, and the next commit heals the segment,
/// flushes the memtable and is acknowledged.
#[test(tokio::test)]
async fn a_rotation_between_heal_and_the_append_with_a_failed_gate_is_refused_by_the_held_writer() {
	for between in [Between::TheGroupsRotation, Between::AnotherRotation] {
		let at = format!("{between:?}");
		let root = TempDir::new("wal_replace").unwrap();
		let live = root.path().join("live");
		let tree = open_tree_with(&live, 4096).await;
		let first = active(&tree);
		let prefill = pairs("pre", 10, b'p');
		commit_ok(&tree, &prefill, Durability::Eventual).await;

		{
			let inner = Arc::clone(&tree.core.inner);
			let fired = AtomicBool::new(false);
			tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
				if matches!(point, PipelineHook::AfterWalHeal)
					&& !fired.swap(true, Ordering::SeqCst)
				{
					inner.wal.write().fail_syncs_after(0);
					assert!(inner.wal.sync().is_err());
					if let Between::AnotherRotation = between {
						inner.rotate_memtable().unwrap();
					}
				}
			})));
		}
		let mut txn = tree.begin().unwrap();
		txn.set(b"big", &[b'b'; 1800]).unwrap();
		let error = txn.commit().await.unwrap_err().to_string();
		assert!(error.contains("WAL segment is held"), "{at}: {error}");
		assert_eq!(active(&tree), first + 1, "{at}");
		assert!(held(&tree), "{at}");
		assert_eq!(file_len(&seg_path(&live, first + 1)), 0, "{at}: the group was not logged");
		assert_eq!(get(&tree, "big"), None, "{at}");

		let image = root.path().join("image");
		copy_dir(&live, &image);
		let recovered = reopen(&image);
		assert_eq!(get(&recovered, "big"), None, "{at}: no image holds the group");
		finish(&recovered);

		tree.core.commit_pipeline.set_hook(None);
		no_backoff(&tree);
		let mut txn = tree.begin().unwrap();
		txn.set(b"big", &[b'b'; 1800]).unwrap();
		txn.commit().await.unwrap();
		assert!(!held(&tree), "{at}");
		assert_eq!(tree.core.inner.l0_file_count(), 1, "{at}");
		assert_all(&tree, &prefill, &at);
		assert_eq!(get(&tree, "big"), Some(vec![b'b'; 1800]), "{at}");
		finish(&tree);
	}
}

/// The documented exception: a group that is part way through its apply when its segment fails.
/// The fsync fails after the group was logged and the first batches were applied. The rotation in
/// the middle of the group skips the sealing fsync and makes a held writer, which refuses the
/// records the group logs again, so the group fails after part of it was applied and the database
/// stops. Nothing of the group is acknowledged, and the commits after it get the stop error
/// without a heal ever starting.
#[test(tokio::test)]
async fn a_mid_group_rotation_with_a_failing_gate_stops_the_database_and_acknowledges_nothing() {
	let root = TempDir::new("wal_replace").unwrap();
	let live = root.path().join("live");
	let tree = open_tree_with(&live, 4096).await;

	let heals = Arc::new(AtomicUsize::new(0));
	{
		let (inner, heals) = (Arc::clone(&tree.core.inner), Arc::clone(&heals));
		let fired = AtomicBool::new(false);
		tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| match point {
			PipelineHook::BeforeApplyRound {
				round: 0,
			} if !fired.swap(true, Ordering::SeqCst) => {
				inner.wal.write().fail_syncs_after(0);
				assert!(inner.wal.sync().is_err());
			}
			PipelineHook::BeforeWalHeal => {
				heals.fetch_add(1, Ordering::SeqCst);
			}
			_ => {}
		})));
	}
	let group = pairs("group", 30, b'g');
	let mut handles = Vec::new();
	for (key, value) in group.clone() {
		let tree = Arc::clone(&tree);
		handles.push(tokio::spawn(async move {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(Durability::Eventual);
			txn.set(key.as_bytes(), &value).unwrap();
			txn.commit().await
		}));
	}
	for handle in handles {
		match handle.await.unwrap() {
			Err(Error::DatabaseStopped(message)) => {
				assert!(message.contains("WAL segment is held"), "{message}")
			}
			other => panic!("{other:?}"),
		}
	}
	assert!(group.iter().all(|(key, _)| get(&tree, key).is_none()), "nothing is readable");
	assert!(matches!(
		commit_one(&tree, "later", Durability::Immediate).await,
		Err(Error::DatabaseStopped(_))
	));
	assert_eq!(heals.load(Ordering::SeqCst), 0, "a stopped database is never healed");

	// What recovery makes of the group is decided there: whole or not at all.
	let image = root.path().join("image");
	copy_dir(&live, &image);
	let recovered = reopen(&image);
	let present = group.iter().filter(|(key, _)| get(&recovered, key).is_some()).count();
	assert!(present == 0 || present == group.len(), "{present} of the group recovered");
	finish(&recovered);
	finish(&tree);
}

/// A rotation of the WAL alone after a failed `flush_wal(true)` skips the sealing fsync, so it
/// makes a held writer; the next commit flushes the memtable of the failed segment, releases the
/// hold and is acknowledged. The Eventual commits before the failure are in a table by then.
#[test(tokio::test)]
async fn a_rotation_after_a_failed_flush_wal_makes_a_held_writer_that_a_commit_releases() {
	let root = TempDir::new("wal_replace").unwrap();
	let live = root.path().join("live");
	let tree = open_tree(&live).await;
	let first = active(&tree);

	let eventual = pairs("eventual", 3, b'e');
	commit_ok(&tree, &eventual, Durability::Eventual).await;
	inject(&tree, Fault::Fsync);
	assert!(tree.flush_wal(true).is_err());
	tree.core.inner.rotate_memtable().unwrap();
	assert!(held(&tree));
	assert_eq!(tree.core.inner.immutable_count(), 1);
	assert!(tree.flush_wal(true).unwrap_err().to_string().contains("WAL segment is held"));

	commit_one(&tree, "next", Durability::Immediate).await.unwrap();
	assert!(!held(&tree));
	assert_eq!(active(&tree), first + 1);
	assert_eq!(tree.core.inner.l0_file_count(), 1);
	assert!(!seg_path(&live, first).exists());
	assert_all(&tree, &eventual, "from the table");
	tree.flush_wal(true).unwrap();
	finish(&tree);
}

// ---------------------------------------------------------------------------
// close
// ---------------------------------------------------------------------------

/// Opens a tree whose background tasks are stopped and that `close_tree` can close.
async fn open_closable(dir: &Path, flush_on_close: bool) -> Arc<Tree> {
	let tree = Arc::new(Tree::new(options_with(dir, BIG, flush_on_close)).unwrap());
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
	tree.core.is_closed.store(true, Ordering::SeqCst);
	tree
}

fn options_with(path: &Path, max_memtable_size: usize, flush_on_close: bool) -> Arc<Options> {
	Arc::new(Options {
		flush_on_close,
		..Arc::unwrap_or_clone(options(path, max_memtable_size))
	})
}

/// Closes a tree opened by `open_closable`.
async fn close_tree(tree: &Tree) -> crate::Result<()> {
	tree.core.is_closed.store(false, Ordering::SeqCst);
	tree.core.task_manager.lock().unwrap().take();
	tokio::time::timeout(Duration::from_secs(30), tree.core.close())
		.await
		.expect("close does not end")
}

/// A commit that races `close` finishes first, with the heal it needs: the flusher drains the
/// commits that were admitted before the WAL is closed. The close returns Ok, and a reopened
/// database holds every acknowledged commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_commit_racing_close_heals_first_and_close_succeeds() {
	let root = TempDir::new("wal_replace").unwrap();
	let live = root.path().join("live");
	let tree = open_closable(&live, false).await;
	commit_one(&tree, "acked", Durability::Immediate).await.unwrap();
	fail_a_commit(&tree, Fault::Fsync, "failed").await;

	let park = park_before_heal(&tree);
	let commit = {
		let tree = Arc::clone(&tree);
		tokio::spawn(async move { commit_one(&tree, "next", Durability::Immediate).await })
	};
	park.wait_until_parked();
	tree.core.is_closed.store(false, Ordering::SeqCst);
	tree.core.task_manager.lock().unwrap().take();
	let close = {
		let tree = Arc::clone(&tree);
		tokio::spawn(async move { tree.core.close().await })
	};
	// The close has begun once the pipeline refuses new commits.
	until("the pipeline to shut down", || {
		tree.core.commit_pipeline.shutdown.load(Ordering::SeqCst)
	})
	.await;
	park.release.send(()).unwrap();

	commit.await.unwrap().expect("the commit admitted before the close is acknowledged");
	tokio::time::timeout(Duration::from_secs(60), close)
		.await
		.expect("close does not end")
		.unwrap()
		.expect("a close after a heal succeeds");
	let reopened = reopen(&live);
	assert!(get(&reopened, "acked").is_some() && get(&reopened, "next").is_some());
	assert_eq!(get(&reopened, "failed"), None);
	finish(&reopened);
}

/// Without a commit after the failure nothing replaced the segment, and the close reports the
/// poison as before. A held writer whose memtables are not in tables, because the database does not
/// flush on close, reports the hold after the rest of the shutdown. With `flush_on_close` the
/// memtables are in tables by the time the WAL is closed, the hold is released first, and the close
/// returns Ok.
#[test(tokio::test)]
async fn close_with_a_pending_hold_or_poison_reports_it_and_a_flush_on_close_releases_it() {
	// Poisoned, not replaced.
	let root = TempDir::new("wal_replace").unwrap();
	let tree = open_closable(&root.path().join("poisoned"), false).await;
	commit_one(&tree, "acked", Durability::Immediate).await.unwrap();
	fail_a_commit(&tree, Fault::Fsync, "failed").await;
	let error = close_tree(&tree).await.unwrap_err().to_string();
	assert!(error.contains("poisoned by an earlier failed sync"), "{error}");

	// Held, memtables still queued.
	let live = root.path().join("held");
	let tree = open_closable(&live, false).await;
	commit_one(&tree, "eventual", Durability::Eventual).await.unwrap();
	inject(&tree, Fault::Fsync);
	assert!(tree.flush_wal(true).is_err());
	tree.core.inner.rotate_memtable().unwrap();
	assert!(held(&tree));
	let error = close_tree(&tree).await.unwrap_err().to_string();
	assert!(error.contains("WAL segment is held"), "{error}");

	// The same with a flush on close.
	let live = root.path().join("flushed");
	let tree = open_closable(&live, true).await;
	commit_one(&tree, "eventual", Durability::Eventual).await.unwrap();
	inject(&tree, Fault::Fsync);
	assert!(tree.flush_wal(true).is_err());
	tree.core.inner.rotate_memtable().unwrap();
	assert!(held(&tree));
	close_tree(&tree).await.expect("the hold is released once the memtables are in tables");
	let reopened = reopen(&live);
	assert!(get(&reopened, "eventual").is_some());
	finish(&reopened);
}

// ---------------------------------------------------------------------------
// the value log
// ---------------------------------------------------------------------------

/// Values of the value log of Eventual commits in the failed segment are made durable by the
/// flush that releases the replacement, before the manifest is written. After the replacement the
/// commits that use the value log are acknowledged and read back live, from an image taken inside
/// the flush, and from one taken afterwards.
#[test(tokio::test)]
async fn a_failed_group_with_value_log_values_is_replaced_and_every_acknowledged_value_reads_back()
{
	let root = TempDir::new("wal_replace").unwrap();
	let live = root.path().join("live");
	let vlog_options = |path: &Path| {
		Arc::new(Options {
			enable_vlog: true,
			vlog_value_threshold: 64,
			..Arc::unwrap_or_clone(options(path, BIG))
		})
	};
	let tree = Arc::new(Tree::new(vlog_options(&live)).unwrap());
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
	tree.core.is_closed.store(true, Ordering::SeqCst);

	let blobs = |prefix: &str, fill: u8| -> Vec<(String, Vec<u8>)> {
		(0..4).map(|i| (format!("{prefix}{i}"), vec![fill; 300])).collect()
	};
	let eventual = blobs("eventual", b'e');
	commit_ok(&tree, &eventual, Durability::Eventual).await;
	{
		let mut txn = tree.begin().unwrap();
		txn.set_durability(Durability::Immediate);
		for (key, value) in blobs("failed", b'f') {
			txn.set(key.as_bytes(), &value).unwrap();
		}
		inject(&tree, Fault::Fsync);
		assert!(txn.commit().await.is_err());
	}

	let image_b = root.path().join("image_b");
	{
		let (live, image_b) = (live.clone(), image_b.clone());
		*tree.core.inner.flush_hook.lock() = Some(Arc::new(move |_| {
			copy_dir(&live, &image_b);
			Ok(())
		}));
	}
	let next = blobs("next", b'n');
	{
		let mut txn = tree.begin().unwrap();
		txn.set_durability(Durability::Immediate);
		for (key, value) in &next {
			txn.set(key.as_bytes(), value).unwrap();
		}
		txn.commit().await.unwrap();
	}
	*tree.core.inner.flush_hook.lock() = None;
	let image_c = root.path().join("image_c");
	copy_dir(&live, &image_c);

	assert_all(&tree, &eventual, "live");
	assert_all(&tree, &next, "live");
	for (name, image) in [("b", &image_b), ("c", &image_c)] {
		let recovered = Tree::new(vlog_options(image)).unwrap();
		assert_all(&recovered, &eventual, name);
		assert_eq!(get(&recovered, "next0"), (name == "c").then(|| vec![b'n'; 300]), "{name}");
		finish(&recovered);
	}
	finish(&tree);
}

// ---------------------------------------------------------------------------
// restore and stop
// ---------------------------------------------------------------------------

/// A restore that begins after a group was admitted fails the group before the heal touches the
/// WAL, and fails it again if the restore began during the heal. Neither leaves the WAL moved
/// before the check or the group logged, and once the restore is over the next commit heals.
#[test(tokio::test)]
async fn heal_does_not_run_during_a_restore() {
	for after_replace in [false, true] {
		let root = TempDir::new("wal_replace").unwrap();
		let live = root.path().join("live");
		let tree = open_tree(&live).await;
		let first = active(&tree);
		fail_a_commit(&tree, Fault::Fsync, "failed").await;

		let replaced = Arc::new(AtomicUsize::new(0));
		{
			let (pipeline, replaced) =
				(Arc::downgrade(&tree.core.commit_pipeline), Arc::clone(&replaced));
			tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
				let restore = |pipeline: &std::sync::Weak<crate::ring::CommitPipeline>| {
					pipeline.upgrade().unwrap().restoring.store(true, Ordering::SeqCst)
				};
				match point {
					PipelineHook::BeforeWalHeal if !after_replace => restore(&pipeline),
					PipelineHook::AfterWalReplace => {
						replaced.fetch_add(1, Ordering::SeqCst);
						if after_replace {
							restore(&pipeline);
						}
					}
					_ => {}
				}
			})));
		}
		let error = commit_one(&tree, "next", Durability::Immediate).await.unwrap_err().to_string();
		assert!(error.contains("Group commit failed"), "{after_replace}: {error}");
		assert_eq!(replaced.load(Ordering::SeqCst), after_replace as usize);
		assert_eq!(active(&tree), first + after_replace as u64, "{after_replace}");
		assert_eq!(seg_path(&live, first + 1).exists(), after_replace, "{after_replace}");

		tree.core.commit_pipeline.set_hook(None);
		tree.core.commit_pipeline.restoring.store(false, Ordering::SeqCst);
		no_backoff(&tree);
		commit_one(&tree, "next", Durability::Immediate).await.unwrap();
		assert_eq!(active(&tree), first + 1, "{after_replace}");
		assert!(get(&tree, "next").is_some());
		finish(&tree);
	}
}

// ---------------------------------------------------------------------------
// the background task
// ---------------------------------------------------------------------------

/// With the background tasks running, the memtable of the failed segment that the replacement
/// queued is flushed by the memtable task as well as by the heal. A flush that fails once is
/// retried, and does not stop the database: the commits that fail meanwhile succeed again once a
/// flush has, without anything being recorded that a restart would have to clear.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_failed_barrier_flush_is_retried_when_the_background_task_runs() {
	let root = TempDir::new("wal_replace").unwrap();
	let live = root.path().join("live");
	let tree = Arc::new(Tree::new(options(&live, BIG)).unwrap());
	no_backoff(&tree);
	commit_one(&tree, "e", Durability::Eventual).await.unwrap();

	let failures = Arc::new(AtomicUsize::new(0));
	{
		let failures = Arc::clone(&failures);
		*tree.core.inner.flush_hook.lock() =
			Some(Arc::new(move |_| match failures.fetch_add(1, Ordering::SeqCst) {
				0 => Err(Error::Other("injected flush failure".into())),
				_ => Ok(()),
			}));
	}
	fail_a_commit(&tree, Fault::Fsync, "failed").await;

	let started = Instant::now();
	let mut attempts = 0;
	loop {
		attempts += 1;
		match commit_one(&tree, "next", Durability::Immediate).await {
			Ok(()) => break,
			Err(e) => {
				let error = e.to_string();
				assert!(
					error.contains("flush the memtables of the failed WAL segment")
						|| error.contains("not yet in tables"),
					"{error}"
				);
			}
		}
		assert!(started.elapsed() < Duration::from_secs(30), "the commits did not resume");
		tokio::time::sleep(Duration::from_millis(5)).await;
	}
	assert!(attempts <= 1000);
	until("the memtable to be in a table", || {
		tree.core.inner.immutable_count() == 0 && tree.core.inner.l0_file_count() == 1
	})
	.await;
	assert!(!held(&tree));
	assert!(get(&tree, "e").is_some() && get(&tree, "next").is_some());
	finish(&tree);
}
