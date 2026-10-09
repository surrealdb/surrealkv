//! What the order of the value log's fsync and a sealed WAL segment's rests on.
//!
//! The tests in `wal_group_crash_tests` and `vlog_tests` check that the value log is fsynced
//! before the segment. These check what that order depends on, which a test of the order alone
//! cannot see:
//!
//! * the value log is fsynced with the WAL lock held, so no record can be logged between that fsync
//!   and the segment's (a rotation that synced the value log before taking the lock passes every
//!   ordering test, and loses the race against the flusher);
//! * a sync covers the entries appended before it took its fds, not the ones appended while it was
//!   fsyncing (a sync that settled what is appended now would let a rotation skip them);
//! * opening a tree continues in a fresh segment by rotating the WAL, and that rotation goes
//!   through the same hook as every other.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;

use crate::ring::PipelineHook;
use crate::vlog::{VLog, SYNCED_VLOG_FILES};
use crate::{Durability, Mode, Options, Tree};

fn payload(index: usize, len: usize) -> Vec<u8> {
	let mut x = (index as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
	(0..len)
		.map(|_| {
			x ^= x << 13;
			x ^= x >> 7;
			x ^= x << 17;
			x as u8
		})
		.collect()
}

fn vlog_opts(path: &Path) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		flush_on_close: false,
		enable_vlog: true,
		vlog_value_threshold: 100,
		vlog_max_file_size: 64 * 1024,
		..Default::default()
	})
}

fn vlog_of(tree: &Tree) -> &Arc<VLog> {
	tree.core.inner.vlog.as_ref().expect("the tree has a value log")
}

fn fsynced_vlog_files(under: &Path) -> Vec<PathBuf> {
	SYNCED_VLOG_FILES.lock().iter().filter(|p| p.starts_with(under)).cloned().collect()
}

fn vlog_files(under: &Path) -> Vec<PathBuf> {
	let mut files: Vec<PathBuf> = fs::read_dir(under.join("vlog"))
		.map(|rd| rd.map(|e| e.unwrap().path()).collect())
		.unwrap_or_default();
	files.sort();
	files
}

async fn stop_background_tasks(tree: &Tree) {
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
}

async fn put(tree: &Tree, key: &str, value: &[u8], durability: Durability) {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(durability);
	txn.set(key.as_bytes(), value).unwrap();
	txn.commit().await.unwrap();
}

fn get(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key.as_bytes()).unwrap()
}

/// Kills a tree: releases the lock file and drops it without `close()`.
fn crash(tree: Tree) {
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
	tree.core.is_closed.store(true, Ordering::SeqCst);
	drop(tree);
}

/// A sync fsyncs what was appended before it took its fds. A value appended while it fsyncs sits in
/// the writer's buffer, not covered, so the next `sync_dirty` has to fsync again: a sync that
/// settled what is appended by the time it finishes would let a rotation skip the value and seal
/// the segment of the record that points to it.
#[test(tokio::test)]
async fn a_value_appended_while_a_sync_runs_is_not_covered_by_it() {
	let dir = TempDir::new("rotate_vlog_sync_prove").unwrap();
	let opts = vlog_opts(dir.path());
	fs::create_dir_all(opts.vlog_dir()).unwrap();
	let vlog = Arc::new(VLog::new(opts).unwrap());
	let first = vlog.append(b"key-0", &payload(0, 100)).unwrap();
	let file = vlog.vlog_file_path(first.file_id);

	let appended = Arc::new(AtomicBool::new(false));
	let log = Arc::downgrade(&vlog);
	vlog.set_sync_gap(Some(Arc::new(move || {
		if !appended.swap(true, Ordering::SeqCst) {
			log.upgrade().unwrap().append(b"key-1", &payload(1, 100)).unwrap();
		}
	})));
	vlog.sync().unwrap();
	vlog.set_sync_gap(None);
	assert_eq!(
		fsynced_vlog_files(dir.path()),
		std::slice::from_ref(&file),
		"the sync fsynced the active file"
	);

	vlog.sync_dirty().unwrap();
	assert_eq!(
		fsynced_vlog_files(dir.path()),
		[file.clone(), file],
		"the value appended during the sync was not covered by its fsync"
	);
	vlog.sync_dirty().unwrap();
	assert_eq!(fsynced_vlog_files(dir.path()).len(), 2, "and is covered by the next one");
}

/// The value log is fsynced with the WAL lock held, however a segment is sealed. A record is
/// logged under that lock, so none can enter the segment between the fsync of the value log and
/// the fsync of the segment, which would make it durable ahead of the values it points to.
#[test(tokio::test)]
async fn the_vlog_is_fsynced_under_the_wal_lock_when_a_segment_is_sealed() {
	for by_memtable_rotation in [true, false] {
		let dir = TempDir::new("rotate_vlog_sync_prove").unwrap();
		let live = dir.path().join("live");
		let tree = Arc::new(Tree::new(vlog_opts(&live)).unwrap());
		stop_background_tasks(&tree).await;
		let inner = Arc::clone(&tree.core.inner);
		put(&tree, "e0", &payload(0, 40_000), Durability::Eventual).await;
		if !by_memtable_rotation {
			// An empty memtable over a value log that holds something unsynced: the segment is
			// sealed on its own.
			inner.rotate_memtable().unwrap();
			vlog_of(&tree).append(b"unsynced", &payload(1, 300)).unwrap();
		}

		let lock_free = Arc::new(Mutex::new(Vec::new()));
		let (core, seen) = (Arc::downgrade(&inner), Arc::clone(&lock_free));
		vlog_of(&tree).set_sync_gap(Some(Arc::new(move || {
			let core = core.upgrade().unwrap();
			let free = core.wal.inner.try_write().is_some();
			seen.lock().unwrap().push(free);
		})));
		let sealed = inner.wal.read().get_active_log_number();
		if by_memtable_rotation {
			inner.rotate_memtable().unwrap();
		} else {
			assert_eq!(inner.seal_active_wal_segment().unwrap(), sealed);
		}
		vlog_of(&tree).set_sync_gap(None);

		assert_eq!(
			*lock_free.lock().unwrap(),
			[false],
			"memtable rotation: {by_memtable_rotation}: the value log was fsynced once, and not \
			 under the WAL lock"
		);
		assert_eq!(inner.wal.read().get_active_log_number(), sealed + 1);
		drop(inner);
		crash(Arc::try_unwrap(tree).ok().expect("no other owner"));
	}
}

/// A commit that reaches the WAL while a rotation fsyncs the value log has to wait for the
/// rotation: its values are appended after the fsync took its fds, so a record logged into the
/// segment before the segment's fsync would be durable ahead of them. The commit is held at the
/// WAL lock and logged afterwards, into the segment that follows.
///
/// The commit is too big for the memtable, so the flusher logs it without a look at the memtable's
/// lock: only the WAL lock keeps it out of the segment while the rotation holds it.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_commit_cannot_log_its_record_while_a_rotation_fsyncs_the_vlog() {
	let dir = TempDir::new("rotate_vlog_sync_prove").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(
		Tree::new(Arc::new(Options {
			max_memtable_size: 64 * 1024,
			..Arc::try_unwrap(vlog_opts(&live)).ok().unwrap()
		}))
		.unwrap(),
	);
	stop_background_tasks(&tree).await;
	for i in 0..3 {
		put(&tree, &format!("e{i}"), &payload(i, 40_000), Durability::Eventual).await;
	}
	assert_eq!(vlog_files(&live).len(), 2);
	let inner = Arc::clone(&tree.core.inner);

	let logged = Arc::new(AtomicUsize::new(0));
	let counted = Arc::clone(&logged);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if matches!(point, PipelineHook::AfterWalSync { .. }) {
			counted.fetch_add(1, Ordering::SeqCst);
		}
	})));

	let committer = Arc::new(Mutex::new(None));
	let logged_in_the_gap = Arc::new(AtomicUsize::new(0));
	let started = Arc::new(AtomicBool::new(false));
	let (runtime, racer, parked) =
		(tokio::runtime::Handle::current(), Arc::clone(&tree), Arc::clone(&committer));
	let (counted, seen) = (Arc::clone(&logged), Arc::clone(&logged_in_the_gap));
	vlog_of(&tree).set_sync_gap(Some(Arc::new(move || {
		if started.swap(true, Ordering::SeqCst) {
			return;
		}
		// A commit starts while the rotation holds the fds of its fsync and has not made it. Give
		// it as long as it needs to reach the WAL: a lock keeps it out, nothing else does.
		let racer = Arc::clone(&racer);
		*parked.lock().unwrap() = Some(runtime.spawn(async move {
			let mut txn = racer.begin().unwrap();
			txn.set_durability(Durability::Eventual);
			for i in 0..1_000 {
				txn.set(format!("racer_{i:06}").as_bytes(), payload(i, 300)).unwrap();
			}
			txn.commit().await.unwrap();
		}));
		let start = Instant::now();
		while counted.load(Ordering::SeqCst) == 0 && start.elapsed() < Duration::from_millis(500) {
			std::thread::sleep(Duration::from_millis(2));
		}
		seen.store(counted.load(Ordering::SeqCst), Ordering::SeqCst);
	})));
	inner.rotate_memtable().unwrap();
	vlog_of(&tree).set_sync_gap(None);
	let during_the_sync = logged_in_the_gap.load(Ordering::SeqCst);

	let committer = committer.lock().unwrap().take().expect("the commit was started");
	tokio::time::timeout(Duration::from_secs(30), committer)
		.await
		.expect("the commit did not finish")
		.unwrap();
	assert_eq!(during_the_sync, 0, "a commit logged its record while the rotation fsynced");
	assert_eq!(logged.load(Ordering::SeqCst), 1, "the commit was logged after the rotation");
	assert_eq!(get(&tree, "racer_000999"), Some(payload(999, 300)));
	drop(inner);
	crash(Arc::try_unwrap(tree).ok().expect("no other owner"));
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

/// Opening a tree continues in a fresh segment by rotating the WAL, through the same hook as every
/// other rotation. A log reopened on files an earlier run left holds its active file's bytes as
/// unsynced, so the hook fsyncs that file once; a tree that has no value log file has nothing to
/// fsync. The files older than the active one are not covered (a known gap, which this does not
/// assert either way).
#[test(tokio::test)]
async fn opening_a_tree_rotates_through_the_value_log_sync_and_fsyncs_its_active_file_once() {
	let dir = TempDir::new("rotate_vlog_sync_prove").unwrap();
	let live = dir.path().join("live");

	// A fresh tree has no value log file: nothing is dirty and opening it fsyncs none.
	let tree = Tree::new(vlog_opts(&live)).unwrap();
	assert!(vlog_files(&live).is_empty());
	assert!(fsynced_vlog_files(&live).is_empty(), "a fresh tree fsynced a value log file");
	stop_background_tasks(&tree).await;

	// Eventual commits across a rollover: the records are in the active segment, their values in
	// two value log files, and nothing has been fsynced.
	let values: Vec<_> = (0..3).map(|i| (format!("e{i}"), payload(i, 40_000))).collect();
	for (key, value) in &values {
		put(&tree, key, value, Durability::Eventual).await;
	}
	let files = vlog_files(&live);
	assert_eq!(files.len(), 2, "the third value rolled the file over");
	assert!(fsynced_vlog_files(&live).is_empty(), "an Eventual commit fsyncs nothing");
	let segment = live
		.join("wal")
		.join(format!("{:020}.wal", tree.core.inner.wal.read().get_active_log_number()));
	assert!(fs::metadata(&segment).unwrap().len() > 0, "the commits are in the active segment");

	// The tree dies and an image of what the OS held is opened.
	let image = dir.path().join("image");
	copy_dir(&live, &image);
	crash(tree);
	assert!(fsynced_vlog_files(&image).is_empty());

	let recovered = Tree::new(vlog_opts(&image)).unwrap();
	let active_file = vlog_files(&image).pop().unwrap();
	let fsyncs = fsynced_vlog_files(&image).iter().filter(|file| **file == active_file).count();
	assert_eq!(fsyncs, 1, "opening the tree fsynced the active value log file {fsyncs} times");
	for (key, value) in &values {
		assert_eq!(get(&recovered, key).as_ref(), Some(value), "{key} was not recovered");
	}
	assert!(recovered.core.inner.wal.read().get_active_log_number() > 1, "a fresh segment");
	crash(recovered);
}
