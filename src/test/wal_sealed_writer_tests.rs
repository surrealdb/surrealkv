//! The life of the writer of a sealed segment: what a failed append or fsync leaves, that a
//! position of a segment's key is never sealed twice, and that a replacement or a rotation always
//! starts a segment of its own with a header and a salt of its own.
//!
//! A position is a nonce slot. Sealing two different batches at one position under one key is the
//! one failure that cannot be undone, so every way a write can end half way is tried, and what is
//! asked is where the count of positions stands afterwards and what a later write does.

use std::fs;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use test_log::test;

use super::sstable_encryption_tests::FlakyManager;
use super::wal_sealed_tests::{
	context,
	names,
	read_records,
	record,
	sealed_context,
	sealed_options,
	segment_path,
	COMPRESSIONS,
	HEADER_RECORD_LEN,
	SUITES,
};
use crate::batch::Batch;
use crate::error::{EncryptionError, KeyProviderFailure};
use crate::tde::{CipherSuite, DbId, EncryptionContext, KeyManager};
use crate::wal::manager::Wal;
use crate::wal::{
	segment_name,
	CompressionType,
	CreationFault,
	Error as WalError,
	WalContext,
	HEADER_SIZE,
};
use crate::{Durability, Mode, Options, Tree};

/// The salt in the header of a segment, as it is in the file.
fn salt_of(dir: &Path, id: u64) -> Vec<u8> {
	let bytes = fs::read(segment_path(dir, id)).unwrap();
	bytes[HEADER_SIZE + 40..HEADER_SIZE + 72].to_vec()
}

// ---------------------------------------------------------------------------
// a write that fails part of the way
// ---------------------------------------------------------------------------

/// Every write of an append is made to fail in turn, once for each. The append is a group of
/// records of which one is longer than a block, so the writes are the headers and the payloads of
/// several physical records, a padding, and the flush at the end.
///
/// After the failure the batches of the group have used their indices, whether or not a byte of
/// them reached the file: the writer and the cipher both stand past them, the writer refuses every
/// further append without moving, and the next one goes to a segment of its own, under another key,
/// where numbering starts again.
#[test]
fn a_write_that_fails_anywhere_never_gives_an_index_out_twice() {
	let sizes = [300usize, 33_000, 20, 5000];
	for suite in SUITES {
		for compression in COMPRESSIONS {
			for n in 0.. {
				assert!(n < 200, "the sweep does not end");
				let dir = TempDir::new().unwrap();
				let enc = context(suite);
				let mut wal = Wal::open(dir.path(), sealed_options(&enc, compression)).unwrap();

				let acknowledged = record(0, 50);
				wal.append(&acknowledged).unwrap();
				let mut buf = Vec::new();
				let mut ends = Vec::new();
				for (i, size) in sizes.iter().enumerate() {
					buf.extend_from_slice(&record(1 + i, *size));
					ends.push(buf.len());
				}
				let last_index = 1 + sizes.len() as u64;

				wal.fail_writes_after(n);
				if wal.append_group(&buf, &ends).is_ok() {
					// Past the last write of the append: it went through, and so did all of it.
					assert!(n > 5, "only {n} writes were failed");
					assert_eq!(wal.next_index(), Some(last_index + 1));
					wal.close().unwrap();
					let read = read_records(&sealed_context(&enc), &segment_path(dir.path(), 0), 0)
						.unwrap();
					assert_eq!(read.len(), 1 + sizes.len());
					break;
				}
				let at = format!("{suite:?} {compression:?} write {n}");

				// The writer is ended, and every index of the group is used up.
				assert!(wal.needs_replacement(), "{at}");
				assert_eq!(wal.next_index(), Some(last_index + 1), "{at}");
				assert_eq!(wal.cipher_next_position(), Some(last_index + 1), "{at}");

				// What the group left in the segment was cut off, so that commits that were not
				// acknowledged are not in it.
				let path = segment_path(dir.path(), 0);
				let kept = fs::read(&path).unwrap();
				let read = read_records(&sealed_context(&enc), &path, 0).unwrap();
				assert_eq!(read, vec![acknowledged.clone()], "{at}");

				// Every further append is refused, takes no index and writes nothing.
				assert!(wal.append(b"again").is_err(), "{at}");
				assert!(wal.append_group(&buf, &ends).is_err(), "{at}");
				assert_eq!(wal.next_index(), Some(last_index + 1), "{at}");
				assert_eq!(fs::read(&path).unwrap(), kept, "{at}");

				// The next one goes to a segment of its own, and numbering starts again under a
				// salt that is not the failed segment's.
				assert_eq!(wal.rotate().unwrap(), 1, "{at}");
				assert_eq!(wal.next_index(), Some(1), "{at}");
				let later = record(9, 70);
				wal.append(&later).unwrap();
				wal.close().unwrap();
				assert_ne!(salt_of(dir.path(), 0), salt_of(dir.path(), 1), "{at}");
				let read =
					read_records(&sealed_context(&enc), &segment_path(dir.path(), 1), 1).unwrap();
				assert_eq!(read, vec![later], "{at}");
				assert_eq!(
					fs::read(&path).unwrap(),
					kept,
					"{at}: the failed segment is not touched"
				);
			}
		}
	}
}

/// The same for a single record that is longer than a block, which `BufWriter` writes straight to
/// the file and which a failure can therefore leave half written.
#[test]
fn a_record_longer_than_a_block_that_fails_leaves_nothing_and_ends_the_writer() {
	for suite in SUITES {
		for n in 0.. {
			assert!(n < 200, "the sweep does not end");
			let dir = TempDir::new().unwrap();
			let enc = context(suite);
			let mut wal =
				Wal::open(dir.path(), sealed_options(&enc, CompressionType::None)).unwrap();
			let acknowledged = record(0, 50);
			wal.append(&acknowledged).unwrap();

			wal.fail_writes_after(n);
			if wal.append(&record(1, 70_000)).is_ok() {
				assert!(n > 3, "only {n} writes were failed");
				break;
			}
			assert!(wal.needs_replacement());
			assert_eq!(wal.next_index(), Some(3));
			let read =
				read_records(&sealed_context(&enc), &segment_path(dir.path(), 0), 0).unwrap();
			assert_eq!(read, vec![acknowledged.clone()], "{suite:?} write {n}");
			assert!(wal.append(&record(2, 10)).is_err());
			assert_eq!(wal.next_index(), Some(3));
		}
	}
}

// ---------------------------------------------------------------------------
// a failed fsync
// ---------------------------------------------------------------------------

/// A segment whose fsync failed is not fsynced again, and what replaces it is held, as a plaintext
/// one is. What is different is the header: the replacement has one of its own, with a salt of its
/// own, made before it is held.
#[test]
fn a_failed_fsync_ends_the_segment_and_the_replacement_is_held_under_a_new_salt() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let enc = context(suite);
		let mut wal = Wal::open(dir.path(), sealed_options(&enc, CompressionType::None)).unwrap();
		wal.append(b"unknown durability").unwrap();
		wal.fail_syncs_after(0);
		assert!(wal.sync().is_err());
		assert!(wal.sync_failed());
		assert!(wal.needs_replacement());

		// Nothing is sealed in a segment that cannot take it.
		assert!(wal.append(b"refused").is_err());
		assert_eq!(wal.next_index(), Some(2));

		assert_eq!(wal.rotate().unwrap(), 1);
		assert_eq!(wal.hold_below(), Some(1), "the replacement is held");
		assert_eq!(wal.next_index(), Some(1));
		assert!(wal.append(b"held").is_err(), "a held writer takes nothing");
		assert_eq!(wal.next_index(), Some(1));
		wal.release_hold(1);
		wal.append(b"released").unwrap();
		assert_eq!(wal.next_index(), Some(2));
		wal.close().unwrap();

		assert_ne!(salt_of(dir.path(), 0), salt_of(dir.path(), 1));
		let read = |id| read_records(&sealed_context(&enc), &segment_path(dir.path(), id), id);
		assert_eq!(read(0).unwrap(), vec![b"unknown durability".to_vec()]);
		assert_eq!(read(1).unwrap(), vec![b"released".to_vec()]);
	}
}

// ---------------------------------------------------------------------------
// the guard behind the count
// ---------------------------------------------------------------------------

/// The writer counts the indices itself, and the cipher counts the positions it has sealed. If the
/// count of the writer were ever wrong, the cipher refuses: the seal fails, nothing is written, and
/// the writer is ended, so the next batch goes elsewhere.
#[test]
fn a_count_that_goes_back_is_refused_by_the_cipher_and_ends_the_writer() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let enc = context(suite);
		let mut wal = Wal::open(dir.path(), sealed_options(&enc, CompressionType::None)).unwrap();
		wal.append(&record(1, 40)).unwrap();
		wal.append(&record(2, 40)).unwrap();
		assert_eq!(wal.cipher_next_position(), Some(3));
		let path = segment_path(dir.path(), 0);
		let before = fs::read(&path).unwrap();

		wal.set_next_index(2);
		let refused = wal.append(&record(3, 40)).unwrap_err();
		assert_eq!(refused, WalError::Encryption(EncryptionError::PositionReuse));
		assert!(wal.needs_replacement(), "{suite:?}: a failed seal ends the writer");
		assert_eq!(wal.cipher_next_position(), Some(3), "{suite:?}: the cipher did not move");
		assert_eq!(fs::read(&path).unwrap(), before, "{suite:?}: nothing was written");
		assert!(wal.append(&record(4, 40)).is_err());

		// The same in the middle of a group: the first seal is refused, and none of it is written.
		let mut other = Wal::open(dir.path(), sealed_options(&enc, CompressionType::None)).unwrap();
		other.append(&record(5, 40)).unwrap();
		other.append(&record(6, 40)).unwrap();
		other.set_next_index(2);
		let buf = [record(7, 10), record(8, 10), record(9, 10)].concat();
		assert!(other.append_group(&buf, &[10, 20, 30]).is_err());
		assert!(other.needs_replacement());
		let kept = fs::read(segment_path(dir.path(), 1)).unwrap();
		other.close().unwrap();
		assert_eq!(fs::read(segment_path(dir.path(), 1)).unwrap(), kept);
		let read = read_records(&sealed_context(&enc), &segment_path(dir.path(), 1), 1).unwrap();
		assert_eq!(read, vec![record(5, 40), record(6, 40)]);
	}
}

// ---------------------------------------------------------------------------
// making a segment fails
// ---------------------------------------------------------------------------

/// A segment that could not be made is not left behind, so the number is free for the next attempt,
/// which takes it with a header and a salt of its own. A rotation that fails leaves the segment in
/// use as it was, and still taking appends.
#[test]
fn a_segment_that_fails_to_start_is_removed_and_its_number_is_taken_again() {
	for suite in SUITES {
		for fault in
			[CreationFault::HeaderWrite, CreationFault::HeaderSync, CreationFault::Directory]
		{
			let dir = TempDir::new().unwrap();
			let enc = context(suite);
			let mut wal =
				Wal::open(dir.path(), sealed_options(&enc, CompressionType::None)).unwrap();

			// The first segment.
			wal.fail_next_creation(fault);
			assert!(wal.append(b"first").is_err(), "{suite:?} {fault:?}");
			assert!(names(dir.path()).is_empty(), "{fault:?}: the file was removed");
			assert!(!wal.has_segment());
			assert_eq!(wal.get_active_log_number(), 0);
			wal.append(b"first").unwrap();
			assert_eq!(names(dir.path()), [segment_name(0, "wal")], "{fault:?}");

			// A rotation.
			wal.fail_next_creation(fault);
			assert!(wal.rotate().is_err(), "{suite:?} {fault:?}");
			assert_eq!(names(dir.path()), [segment_name(0, "wal")], "{fault:?}");
			assert_eq!(wal.get_active_log_number(), 0);
			assert!(!wal.needs_replacement(), "{fault:?}: the segment in use was not touched");
			wal.append(b"second").unwrap();
			assert_eq!(wal.rotate().unwrap(), 1);
			assert_eq!(wal.get_active_log_number(), 1);
			wal.append(b"third").unwrap();
			wal.close().unwrap();

			let read = |id| read_records(&sealed_context(&enc), &segment_path(dir.path(), id), id);
			assert_eq!(read(0).unwrap(), vec![b"first".to_vec(), b"second".to_vec()], "{fault:?}");
			assert_eq!(read(1).unwrap(), vec![b"third".to_vec()], "{fault:?}");
			assert_ne!(salt_of(dir.path(), 0), salt_of(dir.path(), 1));
		}
	}
}

/// A key that cannot be had is the error of the call that needed it, which says that trying again
/// may work, and nothing is created: the segment in use keeps taking appends and the number is
/// free.
#[test]
fn a_key_that_cannot_be_had_fails_the_rotation_and_leaves_the_wal_as_it_was() {
	let dir = TempDir::new().unwrap();
	let manager = FlakyManager::new();
	let enc = Arc::new(EncryptionContext::new(
		Arc::clone(&manager) as Arc<dyn KeyManager>,
		DbId::from_bytes([0xD5; 16]),
		CipherSuite::XChaCha20Poly1305,
	));
	let mut wal = Wal::open(dir.path(), sealed_options(&enc, CompressionType::None)).unwrap();
	wal.append(b"first").unwrap();

	manager.set_unavailable(true);
	let error = wal.rotate().unwrap_err();
	let WalError::Encryption(encryption) = &error else {
		panic!("expected an encryption error, got {error}");
	};
	assert_eq!(*encryption, EncryptionError::KeyProvider(KeyProviderFailure::Unavailable));
	assert!(encryption.is_retryable());
	assert_eq!(names(dir.path()), [segment_name(0, "wal")]);
	assert_eq!(wal.get_active_log_number(), 0);

	wal.append(b"second").unwrap();
	manager.set_unavailable(false);
	assert_eq!(wal.rotate().unwrap(), 1);
	wal.append(b"third").unwrap();
	wal.close().unwrap();
	let read = |id| read_records(&sealed_context(&enc), &segment_path(dir.path(), id), id);
	assert_eq!(read(0).unwrap(), vec![b"first".to_vec(), b"second".to_vec()]);
	assert_eq!(read(1).unwrap(), vec![b"third".to_vec()]);
}

/// The first segment of a sealed WAL is made after the key is asked for: a WAL that cannot have it
/// has no file at all, and makes one when it can.
#[test]
fn the_first_segment_waits_for_a_key_that_is_not_there_yet() {
	let dir = TempDir::new().unwrap();
	let manager = FlakyManager::new();
	let enc = Arc::new(EncryptionContext::new(
		Arc::clone(&manager) as Arc<dyn KeyManager>,
		DbId::from_bytes([0xD5; 16]),
		CipherSuite::Aes256Gcm,
	));
	let mut wal = Wal::open(dir.path(), sealed_options(&enc, CompressionType::None)).unwrap();
	manager.set_unavailable(true);
	assert!(wal.append(b"first").is_err());
	assert!(names(dir.path()).is_empty());
	assert!(wal.rotate().is_err());
	assert!(names(dir.path()).is_empty());

	manager.set_unavailable(false);
	wal.append(b"first").unwrap();
	assert_eq!(names(dir.path()), [segment_name(0, "wal")]);
}

// ---------------------------------------------------------------------------
// the order of a rotation
// ---------------------------------------------------------------------------

/// The hook that makes what the records point to durable runs before the segment is fsynced, and
/// the new segment is made after both, as for a plaintext one.
#[test]
fn a_rotation_runs_its_hook_before_it_fsyncs_the_segment_and_makes_the_next_one_after() {
	let dir = TempDir::new().unwrap();
	let enc = context(CipherSuite::Aes256Gcm);
	let mut wal = Wal::open(dir.path(), sealed_options(&enc, CompressionType::None)).unwrap();
	wal.append(b"a record").unwrap();

	let events = Arc::new(parking_lot::Mutex::new(Vec::new()));
	let observed = Arc::clone(&events);
	wal.set_sync_observer(Some(Arc::new(move || observed.lock().push("fsync of the segment"))));
	let hooked = Arc::clone(&events);
	let directory = dir.path().to_path_buf();
	let rotated = wal.rotate_with(|| {
		hooked.lock().push("hook");
		// The next segment does not exist while the hook runs.
		assert_eq!(names(&directory), [segment_name(0, "wal")]);
		Ok(())
	});
	assert_eq!(rotated.unwrap(), 1);
	assert_eq!(*events.lock(), ["hook", "fsync of the segment"]);
	assert_eq!(names(dir.path()), [segment_name(0, "wal"), segment_name(1, "wal")]);
	wal.close().unwrap();
}

/// A hook that fails fails the rotation before the segment is fsynced and before the next one is
/// made, and the WAL is as it was.
#[test]
fn a_hook_that_fails_leaves_a_sealed_wal_as_it_was() {
	let dir = TempDir::new().unwrap();
	let enc = context(CipherSuite::XChaCha20Poly1305);
	let mut wal = Wal::open(dir.path(), sealed_options(&enc, CompressionType::None)).unwrap();
	wal.append(b"a record").unwrap();
	let rotated = wal.rotate_with(|| {
		Err(WalError::IO(crate::wal::IOError::new(std::io::ErrorKind::Other, "injected")))
	});
	assert!(rotated.is_err());
	assert_eq!(names(dir.path()), [segment_name(0, "wal")]);
	assert_eq!(wal.get_active_log_number(), 0);
	assert!(!wal.needs_replacement());
	wal.append(b"another").unwrap();
	assert_eq!(wal.rotate().unwrap(), 1);
	wal.close().unwrap();
	let read = read_records(&sealed_context(&enc), &segment_path(dir.path(), 0), 0).unwrap();
	assert_eq!(read, vec![b"a record".to_vec(), b"another".to_vec()]);
}

// ---------------------------------------------------------------------------
// a tree
// ---------------------------------------------------------------------------

fn tree_options(path: &Path, enc: &Arc<EncryptionContext>) -> Arc<Options> {
	Arc::new(
		Options {
			path: path.to_path_buf(),
			flush_on_close: false,
			// The background flush is stopped, so a rotation that queues a memtable must not stall
			// the commits behind it.
			memtable_stall_threshold: 1000,
			..Default::default()
		}
		.with_encryption_context(Arc::clone(enc)),
	)
}

/// A tree whose background tasks are stopped, so that no segment is flushed and removed behind the
/// test's back. Shared through an `Arc`, never `Tree::clone`: dropping any clone spawns a close.
async fn open_tree(path: &Path, enc: &Arc<EncryptionContext>) -> Arc<Tree> {
	let tree = Arc::new(Tree::new(tree_options(path, enc)).unwrap());
	let tasks = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tasks.stop().await;
	tree.core.is_closed.store(true, Ordering::SeqCst);
	tree.core.commit_pipeline.set_replace_backoff(Duration::ZERO, Duration::ZERO);
	tree
}

/// Ends a tree the way a crash does: no close, no flush.
fn crash(tree: &Tree) {
	tree.core.is_closed.store(true, Ordering::SeqCst);
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
}

async fn commit_one(tree: &Tree, key: &str) -> crate::Result<()> {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(Durability::Immediate);
	txn.set(key.as_bytes(), &[b'v'; 40]).unwrap();
	txn.commit().await
}

fn get(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key.as_bytes()).unwrap()
}

fn active(tree: &Tree) -> u64 {
	tree.core.inner.wal.read().get_active_log_number()
}

/// The keys of the batches of a segment, in order.
fn segment_keys(enc: &Arc<EncryptionContext>, path: &Path, id: u64) -> Vec<String> {
	read_records(&WalContext::sealed(Arc::clone(enc)), path, id)
		.unwrap()
		.iter()
		.flat_map(|record| {
			Batch::decode(record)
				.unwrap()
				.entries
				.iter()
				.map(|entry| String::from_utf8(entry.key.clone()).unwrap())
				.collect::<Vec<_>>()
		})
		.collect()
}

/// The commit pipeline replaces a segment that failed before it logs the next group, and the
/// replacement is a sealed segment of its own: a header, a salt, numbering from one. The commits
/// of the group that failed are not readable, and a crash image recovers the rest.
#[test(tokio::test)]
async fn a_tree_replaces_a_failed_sealed_segment_with_one_that_has_its_own_salt() {
	for suite in [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305] {
		for write_fault in [false, true] {
			let root = TempDir::new().unwrap();
			let live = root.path().join("live");
			let enc = context(suite);
			let tree = open_tree(&live, &enc).await;
			let first = active(&tree);
			let wal_dir = live.join("wal");
			commit_one(&tree, "before").await.unwrap();
			let failed_salt = salt_of(&wal_dir, first);

			{
				let mut wal = tree.core.inner.wal.write();
				if write_fault {
					wal.fail_writes_after(1);
				} else {
					wal.fail_syncs_after(0);
				}
			}
			assert!(commit_one(&tree, "failed").await.is_err());
			assert!(get(&tree, "failed").is_none());

			commit_one(&tree, "next").await.unwrap();
			commit_one(&tree, "after").await.unwrap();
			assert_eq!(active(&tree), first + 1, "{suite:?} {write_fault}");

			// The replacement is a sealed segment under a salt of its own. The segment that failed
			// is still there if the memtable tagged with it was not flushed, which only
			// a held replacement, after a failed fsync, waits for.
			let mut expected = vec![segment_name(first + 1, "wal")];
			if write_fault {
				expected.insert(0, segment_name(first, "wal"));
			}
			assert_eq!(names(&wal_dir), expected, "{suite:?} {write_fault}");
			assert_ne!(failed_salt, salt_of(&wal_dir, first + 1), "{suite:?} {write_fault}");
			let replacement = segment_keys(&enc, &segment_path(&wal_dir, first + 1), first + 1);
			assert_eq!(replacement, ["next", "after"], "{suite:?} {write_fault}");

			// A crash image: what was acknowledged is there. A commit that failed on a write was
			// cut off the segment; one that failed on an fsync may or may not be, which
			// is why it is not asserted.
			crash(&tree);
			let image = root.path().join("image");
			copy_dir(&live, &image);
			drop(tree);
			let recovered = Tree::new(tree_options(&image, &enc)).unwrap();
			for key in ["before", "next", "after"] {
				assert_eq!(get(&recovered, key), Some(vec![b'v'; 40]), "{suite:?}: {key}");
			}
			if write_fault {
				assert!(get(&recovered, "failed").is_none(), "{suite:?}");
			}
			recovered.close().await.unwrap();
		}
	}
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

/// Opening a tree makes one segment for its commits, a segment that is new, with a header of its
/// own, and never appends to the one a crash left.
#[test(tokio::test)]
async fn a_tree_that_is_opened_again_writes_to_a_new_segment_and_leaves_the_old_one_alone() {
	for suite in [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305] {
		let root = TempDir::new().unwrap();
		let live = root.path().join("live");
		let enc = context(suite);
		let tree = open_tree(&live, &enc).await;
		let first = active(&tree);
		commit_one(&tree, "one").await.unwrap();
		commit_one(&tree, "two").await.unwrap();
		crash(&tree);
		drop(tree);

		let wal_dir = live.join("wal");
		let left = fs::read(segment_path(&wal_dir, first)).unwrap();
		assert!(left.len() > HEADER_RECORD_LEN);

		let tree = open_tree(&live, &enc).await;
		assert_eq!(active(&tree), first + 1, "{suite:?}: a segment of its own");
		assert_eq!(fs::read(segment_path(&wal_dir, first)).unwrap(), left, "{suite:?}");
		commit_one(&tree, "three").await.unwrap();
		assert_eq!(segment_keys(&enc, &segment_path(&wal_dir, first + 1), first + 1), ["three"]);
		assert_eq!(segment_keys(&enc, &segment_path(&wal_dir, first), first), ["one", "two"]);
		assert_ne!(salt_of(&wal_dir, first), salt_of(&wal_dir, first + 1));
		for key in ["one", "two", "three"] {
			assert!(get(&tree, key).is_some(), "{suite:?}: {key}");
		}
		crash(&tree);
	}
}

/// A compression is a choice of the segment: it is in the header, so what a segment was written
/// with is how it is read, whatever the options say now.
#[test]
fn the_compression_of_a_segment_is_the_one_in_its_header() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let enc = context(suite);
		let mut wal = Wal::open(dir.path(), sealed_options(&enc, CompressionType::Lz4)).unwrap();
		wal.append(&record(1, 500)).unwrap();
		wal.close().unwrap();

		// A WAL that is opened with another setting starts a segment of its own with that one, and
		// both are read.
		let mut wal = Wal::open(dir.path(), sealed_options(&enc, CompressionType::None)).unwrap();
		wal.append(&record(2, 500)).unwrap();
		wal.close().unwrap();

		let read = |id| read_records(&sealed_context(&enc), &segment_path(dir.path(), id), id);
		assert_eq!(read(0).unwrap(), vec![record(1, 500)]);
		assert_eq!(read(1).unwrap(), vec![record(2, 500)]);
		let compressed = fs::metadata(segment_path(dir.path(), 0)).unwrap().len();
		let plain = fs::metadata(segment_path(dir.path(), 1)).unwrap().len();
		assert!(compressed < plain, "{suite:?}: {compressed} against {plain}");
	}
}
