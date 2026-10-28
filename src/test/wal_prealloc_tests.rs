//! The active WAL segment keeps written zeros ahead of its end (`Options::with_wal_preallocation`).
//!
//! * The file is longer than its records only by zeros, in whole blocks, while the segment is
//!   active, and a segment that is closed or rotated is cut back to its records.
//! * A crash image of an active segment, zeros and all, opens with every synced commit, even under
//!   `WalRecoveryMode::AbsoluteConsistency`, which refuses any damage: the replay reads the zeros
//!   as the padding of blocks.
//! * The commits made after that open survive another crash.
//!
//! These look at the files on the disk, so they run over `std` only: a pool keeps no zeros ahead.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use super::tfs;
use crate::wal::BLOCK_SIZE;
use crate::{Durability, Mode, Options, Tree, WalRecoveryMode};

fn opts(path: &Path, recovery: WalRecoveryMode) -> Arc<Options> {
	Arc::new(
		Options {
			path: path.to_path_buf(),
			// Crash semantics: nothing may be flushed on drop or close.
			flush_on_close: false,
			wal_recovery_mode: recovery,
			..Default::default()
		}
		.with_wal_preallocation(true),
	)
}

fn seg_path(dir: &Path, id: u64) -> PathBuf {
	dir.join("wal").join(format!("{id:020}.wal"))
}

fn active_segment(tree: &Tree) -> u64 {
	tree.core.inner.wal.read().get_active_log_number()
}

fn segments(dir: &Path) -> Vec<PathBuf> {
	let mut paths: Vec<PathBuf> = fs::read_dir(dir.join("wal"))
		.unwrap()
		.map(|entry| entry.unwrap().path())
		.filter(|path| path.extension().is_some_and(|ext| ext == "wal"))
		.collect();
	paths.sort();
	paths
}

fn copy_dir(src: &Path, dst: &Path) {
	fs::create_dir_all(dst).unwrap();
	for entry in fs::read_dir(src).unwrap() {
		let entry = entry.unwrap();
		let to = dst.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &to);
		} else {
			fs::copy(entry.path(), &to).unwrap();
		}
	}
}

/// Kills a tree: drops it without `close()`, with the lock released.
fn crash(tree: Tree) {
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
	tree.core.is_closed.store(true, Ordering::SeqCst);
	drop(tree);
}

async fn put(tree: &Tree, key: &str, value: &[u8]) {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(Durability::Immediate);
	txn.set(key.as_bytes(), value).unwrap();
	txn.commit().await.unwrap();
}

fn get(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key.as_bytes()).unwrap()
}

fn value(i: usize) -> Vec<u8> {
	format!("value-{i}-").repeat(1 + i % 40).into_bytes()
}

/// The length of the file at `path` without the zero bytes it ends with.
fn without_zero_tail(path: &Path) -> (u64, u64) {
	let bytes = fs::read(path).unwrap();
	let end = bytes.iter().rposition(|&b| b != 0).map_or(0, |at| at + 1);
	(bytes.len() as u64, end as u64)
}

#[test(tokio::test(flavor = "current_thread"))]
async fn the_active_segment_keeps_zeros_ahead_and_a_closed_one_is_cut_back() {
	if tfs::ambient().is_some() {
		return;
	}
	let dir = TempDir::new("wal_prealloc").unwrap();
	let tree = Tree::new(opts(dir.path(), WalRecoveryMode::default())).unwrap();
	for i in 0..200 {
		put(&tree, &format!("k{i:04}"), &value(i)).await;
	}
	let active = seg_path(dir.path(), active_segment(&tree));
	let (len, records) = without_zero_tail(&active);
	assert!(
		len >= records + 1024 * 1024,
		"zeros are kept ahead: {len} bytes, records end {records}"
	);
	assert_eq!(len % BLOCK_SIZE as u64, 0, "the zeros end on a block boundary");

	tree.close().await.unwrap();
	for path in segments(dir.path()) {
		let (len, records) = without_zero_tail(&path);
		assert!(
			len - records < BLOCK_SIZE as u64,
			"{path:?} is cut back to its records: {len} bytes, records end {records}"
		);
	}
	let tree = Tree::new(opts(dir.path(), WalRecoveryMode::AbsoluteConsistency)).unwrap();
	for i in 0..200 {
		assert_eq!(get(&tree, &format!("k{i:04}")), Some(value(i)), "k{i:04}");
	}
	tree.close().await.unwrap();
}

#[test(tokio::test(flavor = "current_thread"))]
async fn a_crash_image_with_the_zeros_ahead_opens_with_every_commit() {
	if tfs::ambient().is_some() {
		return;
	}
	for recovery in [WalRecoveryMode::AbsoluteConsistency, WalRecoveryMode::TolerateTornTail] {
		let dir = TempDir::new("wal_prealloc").unwrap();
		let live = dir.path().join("live");
		let tree = Tree::new(opts(&live, recovery)).unwrap();
		for i in 0..300 {
			put(&tree, &format!("k{i:04}"), &value(i)).await;
		}
		// The image is taken while the tree is open: the active segment ends in zeros, as a
		// power cut would leave it.
		let image = dir.path().join("image");
		copy_dir(&live, &image);
		let (len, records) = without_zero_tail(&seg_path(&image, active_segment(&tree)));
		assert!(len > records + BLOCK_SIZE as u64, "the image holds the zeros ahead");
		crash(tree);

		let tree = Tree::new(opts(&image, recovery)).unwrap();
		for i in 0..300 {
			assert_eq!(get(&tree, &format!("k{i:04}")), Some(value(i)), "{recovery:?} k{i:04}");
		}
		// What is committed after that open survives the next crash too.
		for i in 300..400 {
			put(&tree, &format!("k{i:04}"), &value(i)).await;
		}
		let second = dir.path().join("second");
		copy_dir(&image, &second);
		crash(tree);

		let tree = Tree::new(opts(&second, recovery)).unwrap();
		for i in 0..400 {
			assert_eq!(get(&tree, &format!("k{i:04}")), Some(value(i)), "{recovery:?} k{i:04}");
		}
		tree.close().await.unwrap();
	}
}

#[test(tokio::test(flavor = "current_thread"))]
async fn by_default_the_segment_is_as_long_as_its_records() {
	if tfs::ambient().is_some() {
		return;
	}
	let dir = TempDir::new("wal_prealloc").unwrap();
	let options = Options {
		path: dir.path().to_path_buf(),
		flush_on_close: false,
		..Default::default()
	};
	let tree = Tree::new(Arc::new(options)).unwrap();
	for i in 0..50 {
		put(&tree, &format!("k{i:04}"), &value(i)).await;
	}
	let (len, records) = without_zero_tail(&seg_path(dir.path(), active_segment(&tree)));
	assert!(
		len - records < BLOCK_SIZE as u64,
		"no zeros ahead: {len} bytes, records end {records}"
	);
	tree.close().await.unwrap();
}

/// A commit whose append fails part-way into the zeros ahead is cut back and reported, the next
/// commit replaces the segment, and a crash image recovers exactly the acknowledged commits, with
/// no part of the failed one, under `AbsoluteConsistency`.
#[test(tokio::test(flavor = "current_thread"))]
async fn a_failed_append_into_the_zeros_ahead_leaves_only_the_acknowledged_commits() {
	if tfs::ambient().is_some() {
		return;
	}
	let dir = TempDir::new("wal_prealloc").unwrap();
	let live = dir.path().join("live");
	let tree = Tree::new(opts(&live, WalRecoveryMode::AbsoluteConsistency)).unwrap();
	for i in 0..5 {
		put(&tree, &format!("acked{i}"), &value(i)).await;
	}
	let segment = seg_path(&live, active_segment(&tree));
	let (_, acked_end) = without_zero_tail(&segment);

	// A commit large enough to span blocks, failing inside the record.
	tree.core.inner.wal.write().fail_writes_after(8);
	let mut txn = tree.begin().unwrap();
	txn.set_durability(Durability::Immediate);
	txn.set(b"failed", vec![b'y'; 100_000]).unwrap();
	assert!(txn.commit().await.is_err(), "the commit reports the failed append");
	let (_, after_failure) = without_zero_tail(&segment);
	assert_eq!(after_failure, acked_end, "no part of the failed record stays");

	for i in 0..5 {
		put(&tree, &format!("later{i}"), &value(i)).await;
	}
	let image = dir.path().join("image");
	copy_dir(&live, &image);
	crash(tree);

	let tree = Tree::new(opts(&image, WalRecoveryMode::AbsoluteConsistency)).unwrap();
	for i in 0..5 {
		assert_eq!(get(&tree, &format!("acked{i}")), Some(value(i)), "acked{i}");
		assert_eq!(get(&tree, &format!("later{i}")), Some(value(i)), "later{i}");
	}
	assert_eq!(get(&tree, "failed"), None);
	tree.close().await.unwrap();
}
