//! A segment of the write-ahead log of an encrypted database that is not the database's.
//!
//! The segments from the log number on are read by the replay, which refuses one of another
//! database. The ones below it are never read: they are deleted by their number once everything in
//! them is in a table. So a segment that someone put there, or that a restore of another database
//! left behind, would have been deleted at the next flush or close without anyone having looked at
//! it. Each of them is read from its first record now, before the open does anything and before it
//! is deleted, and one that is not this database's is refused and kept.
//!
//! What is refused is only what is certain: a first record that passes its checksum and names
//! another database, or is in the clear. Damage, a write that was cut short and a segment that was
//! never written are the leftovers of this database's own crashes, and are deleted as they always
//! were.

use std::path::Path;
use std::sync::Arc;

use tempfile::TempDir;

use super::encryption_open_tests::{
	encryption_error,
	files_with_extension,
	manager,
	open_error,
	options,
	snapshot,
	write,
	KEYS,
	SUITES,
};
use super::{sfs, sfs as fs, tfs};
use crate::error::{EncryptionError, Error};
use crate::tde::{CipherSuite, DbId, EncryptionContext, SoftwareKeyManager};
use crate::wal::{calculate_crc32, sealed, segment_name, CompressionType, RecordType, HEADER_SIZE};
use crate::{Options, Tree};

const OURS: [u8; 16] = [0xA1; 16];
const THEIRS: [u8; 16] = [0xB2; 16];

fn context(db: [u8; 16]) -> EncryptionContext {
	EncryptionContext::new(
		Arc::new(SoftwareKeyManager::new([0x31; 32])),
		DbId::from_bytes(db),
		CipherSuite::XChaCha20Poly1305,
	)
}

/// A record of the type `kind` and the payload `payload` as a segment frames it.
fn framed(kind: u8, payload: &[u8]) -> Vec<u8> {
	let mut record = calculate_crc32(&[kind], payload).to_be_bytes().to_vec();
	record.extend_from_slice(&(payload.len() as u16).to_be_bytes());
	record.push(kind);
	record.extend_from_slice(payload);
	record
}

/// The header record of the segment `id` of the database `db`.
fn header_record(db: [u8; 16], id: u64) -> Vec<u8> {
	let (payload, _cipher) = sealed::new_header(&context(db), id, CompressionType::None).unwrap();
	framed(RecordType::SegmentHeader as u8, &payload)
}

/// What is said of the first record `record` by the database `OURS`, `None` if nothing is.
fn verdict(record: &[u8]) -> Option<EncryptionError> {
	sealed::check_first_record(DbId::from_bytes(OURS), record).err()
}

#[test]
fn a_header_record_that_names_another_database_is_refused_and_this_databases_is_not() {
	assert_eq!(verdict(&header_record(OURS, 7)), None);
	// The number is not the check's to say: the reader compares it with the name of the file.
	assert_eq!(verdict(&header_record(OURS, 8)), None);
	assert_eq!(verdict(&header_record(THEIRS, 7)), Some(EncryptionError::DbIdMismatch));
	assert_eq!(verdict(&header_record(THEIRS, 0)), Some(EncryptionError::DbIdMismatch));

	// Nothing after the first record changes the verdict.
	let mut longer = header_record(THEIRS, 7);
	longer.extend_from_slice(&[0xEE; 40]);
	assert_eq!(verdict(&longer), Some(EncryptionError::DbIdMismatch));
}

#[test]
fn damage_and_a_cut_are_not_a_verdict_since_they_are_what_a_crash_of_this_database_leaves() {
	let theirs = header_record(THEIRS, 7);

	// The checksum of the record does not hold: not a record, whoever's it was.
	let mut bad_checksum = theirs.clone();
	bad_checksum[0] ^= 1;
	assert_eq!(verdict(&bad_checksum), None);

	// A byte of the payload changed, the checksum as it was.
	let mut bad_payload = theirs.clone();
	bad_payload[HEADER_SIZE + 20] ^= 1;
	assert_eq!(verdict(&bad_payload), None);

	// The checksum of the record made again over a header that is not well formed: the checksum of
	// the encryption header, which is the last four bytes of it, no longer holds. That is for the
	// reader to say, in the order that it checks a header.
	let mut payload = theirs[HEADER_SIZE..].to_vec();
	payload[crate::tde::HEADER_LEN - 1] ^= 1;
	assert_eq!(verdict(&framed(RecordType::SegmentHeader as u8, &payload)), None);

	// A record that ends where the file does, at every length that leaves it incomplete.
	for len in [0, 3, HEADER_SIZE - 1, HEADER_SIZE, HEADER_SIZE + 1, theirs.len() - 1] {
		assert_eq!(verdict(&theirs[..len]), None, "{len} bytes");
	}

	// What a crash right after the file was made leaves: nothing, or zeros.
	assert_eq!(verdict(&[]), None);
	assert_eq!(verdict(&[0u8; sealed::HEADER_RECORD_LEN]), None);

	// A header record that is too short to hold an encryption header says nothing of a database.
	assert_eq!(verdict(&framed(RecordType::SegmentHeader as u8, &[1, 2, 3])), None);
}

#[test]
fn a_first_record_in_the_clear_is_refused_and_a_record_of_no_kind_is_not() {
	for kind in [
		RecordType::Full,
		RecordType::First,
		RecordType::Middle,
		RecordType::Last,
		RecordType::SetCompressionType,
		RecordType::SyncMark,
	] {
		// Short and long: the first record of a segment in the clear is often longer than the
		// header of a sealed segment is.
		for len in [1, 3, 94, 300] {
			let record = framed(kind as u8, &vec![b'x'; len]);
			assert_eq!(
				verdict(&record),
				Some(EncryptionError::PlaintextInEncryptedDb),
				"{kind:?}, {len} bytes"
			);

			// With a checksum that does not hold it is damage, which is not told apart from the
			// start of a sealed segment that was changed.
			let mut damaged = record.clone();
			damaged[0] ^= 1;
			assert_eq!(verdict(&damaged), None, "{kind:?}, {len} bytes, damaged");
		}
	}

	// A kind that no segment starts with, with a checksum that holds.
	for kind in [0u8, 5, 7, 12, 200] {
		assert_eq!(verdict(&framed(kind, b"abc")), None, "kind {kind}");
	}
}

fn write_segment(dir: &Path, id: u64, bytes: &[u8]) {
	fs::create_dir_all(dir).unwrap();
	fs::write(dir.join(segment_name(id, "wal")), bytes).unwrap();
}

#[test]
fn the_segments_of_a_directory_are_each_read_and_the_first_that_is_not_ours_is_the_answer() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let wal = dir.path().join("wal");
	let ours = DbId::from_bytes(OURS);

	// A directory that is not there has nothing to refuse, and nor has an empty one.
	assert!(sealed::check_directory(ours, &wal).is_ok());
	fs::create_dir_all(&wal).unwrap();
	assert!(sealed::check_directory(ours, &wal).is_ok());

	let mut own = header_record(OURS, 3);
	own.extend_from_slice(&[0xEE; 50]);
	write_segment(&wal, 3, &own);
	write_segment(&wal, 4, &[]);
	write_segment(&wal, 5, &[0u8; sealed::HEADER_RECORD_LEN]);
	write_segment(&wal, 6, &header_record(OURS, 6)[..20]);
	assert!(sealed::check_directory(ours, &wal).is_ok());
	for id in [3, 4, 5, 6] {
		assert!(
			sealed::check_segment_file(ours, &wal.join(segment_name(id, "wal"))).is_ok(),
			"{id}"
		);
	}

	// A file that is not there says nothing.
	assert!(sealed::check_segment_file(ours, &wal.join(segment_name(99, "wal"))).is_ok());

	// One of another database among them, below the others or above them.
	for id in [0, 4_000] {
		write_segment(&wal, id, &header_record(THEIRS, id));
		let err = sealed::check_directory(ours, &wal).unwrap_err();
		assert_eq!(encryption_error(err, "foreign"), EncryptionError::DbIdMismatch, "{id}");
		fs::remove_file(wal.join(segment_name(id, "wal"))).unwrap();
		assert!(sealed::check_directory(ours, &wal).is_ok(), "{id}");
	}

	// And one in the clear, whose first record is longer than a sealed header is.
	write_segment(&wal, 2, &framed(RecordType::Full as u8, &[b'p'; 300]));
	let err = sealed::check_directory(ours, &wal).unwrap_err();
	assert_eq!(encryption_error(err, "plain"), EncryptionError::PlaintextInEncryptedDb);
}

// ---------------------------------------------------------------------------
// a database that is opened and closed
// ---------------------------------------------------------------------------

/// Options of a database in `dir` that has no value log, so that the log of the write-ahead log
/// is the only place that a file of another database could be taken for the database's.
fn encrypted(dir: &Path, suite: CipherSuite) -> Arc<Options> {
	let opts = Options {
		enable_vlog: false,
		..options(dir)
	};
	Arc::new(opts.with_encryption(manager(), suite))
}

/// The first segment of a database of its own in `dir`, which shares the key of the others.
async fn segment_of_another_database(dir: &Path, suite: CipherSuite) -> Vec<u8> {
	let opts = encrypted(dir, suite);
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	write(&tree, '9', 0..4).await;
	tree.close().await.unwrap();
	let segments = files_with_extension(&opts.wal_dir(), "wal");
	fs::read(segments.first().expect("the database made a segment")).unwrap()
}

/// A database in `root`, written and flushed once, so that the number of its log is past its first
/// segment, and closed. The bytes of its first segment, which a flush deleted, are returned.
async fn database_that_has_flushed(root: &Path, suite: CipherSuite) -> (Arc<Options>, Vec<u8>) {
	let opts = encrypted(root, suite);
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	write(&tree, '1', 0..KEYS).await;
	// The segment is the one that the first flush makes obsolete. What is in it now is its header,
	// which is all that the checks look at.
	let first = files_with_extension(&opts.wal_dir(), "wal");
	let own = fs::read(first.first().expect("the database made a segment")).unwrap();
	tree.flush().unwrap();
	let log_number = tree.core.inner.level_manifest.read().unwrap().get_log_number();
	assert!(log_number >= 1, "the first flush moved the log number past the first segment");
	tree.close().await.unwrap();
	(opts, own)
}

fn first_segment(opts: &Options) -> std::path::PathBuf {
	opts.wal_dir().join(segment_name(0, "wal"))
}

#[tokio::test]
async fn a_segment_of_another_database_below_the_log_number_refuses_the_open_and_changes_nothing() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let foreign = segment_of_another_database(&dir.path().join("other"), suite).await;
		let root = dir.path().join("db");
		let (opts, _) = database_that_has_flushed(&root, suite).await;

		write_segment(&opts.wal_dir(), 0, &foreign);
		let before = snapshot(&root);

		let err = open_error(&opts);
		assert_eq!(encryption_error(err, "foreign"), EncryptionError::DbIdMismatch, "{suite:?}");
		assert!(snapshot(&root) == before, "{suite:?}: a refused open changed the database");
	}
}

#[tokio::test]
async fn a_segment_in_the_clear_below_the_log_number_refuses_the_open_and_changes_nothing() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let root = dir.path().join("db");
		let (opts, _) = database_that_has_flushed(&root, suite).await;

		write_segment(&opts.wal_dir(), 0, &framed(RecordType::Full as u8, &[b'p'; 300]));
		let before = snapshot(&root);

		let err = open_error(&opts);
		assert_eq!(
			encryption_error(err, "plain"),
			EncryptionError::PlaintextInEncryptedDb,
			"{suite:?}"
		);
		assert!(snapshot(&root) == before, "{suite:?}: a refused open changed the database");
	}
}

#[tokio::test]
async fn what_this_database_left_behind_below_the_log_number_is_opened_over_and_deleted() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let root = dir.path().join("db");
		let (opts, own) = database_that_has_flushed(&root, suite).await;

		let mut damaged = own.clone();
		damaged[HEADER_SIZE + 20] ^= 1;
		let leftovers: Vec<(&str, Vec<u8>)> = vec![
			("a segment of this database", own.clone()),
			("one with a byte changed", damaged),
			("one that was cut short", own[..20].to_vec()),
			("one with no byte in it", Vec::new()),
			("one of zeros", vec![0u8; own.len()]),
		];
		for (what, bytes) in leftovers {
			write_segment(&opts.wal_dir(), 0, &bytes);

			// The database opens and is closed, and what was there is removed as an old segment is.
			let tree = Tree::new(Arc::clone(&opts)).unwrap();
			tree.close().await.unwrap();
			assert!(!sfs::exists(first_segment(&opts)), "{suite:?}: {what} was not removed");
		}
	}
}

#[tokio::test]
async fn a_segment_of_another_database_that_appears_while_the_database_is_open_is_kept() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let foreign = segment_of_another_database(&dir.path().join("other"), suite).await;
		let root = dir.path().join("db");
		let (opts, own) = database_that_has_flushed(&root, suite).await;

		// Opened with nothing in the way, then a segment of another database below the log number,
		// and then the close, which deletes what the log number has passed.
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		write_segment(&opts.wal_dir(), 0, &foreign);
		tree.close().await.unwrap();
		assert_eq!(
			fs::read(first_segment(&opts)).ok().as_deref(),
			Some(&foreign[..]),
			"{suite:?}: a segment of another database was deleted"
		);

		// The control: the same place, with a segment of this database, is cleaned as it was.
		fs::remove_file(first_segment(&opts)).unwrap();
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		write_segment(&opts.wal_dir(), 0, &own);
		tree.close().await.unwrap();
		assert!(
			!sfs::exists(first_segment(&opts)),
			"{suite:?}: an old segment of this database stayed"
		);
	}
}

#[tokio::test]
async fn a_manifest_of_another_database_does_not_have_the_segments_of_this_one_deleted() {
	if tfs::skip_listed() {
		return;
	}
	// A database that has not flushed holds its acknowledged writes in the log alone. The manifest
	// of another database that shares the key, and the table that it lists, are put in its place: a
	// restore by copying, which a checkpoint's empty log does not clear. The segment of the first
	// database is below the log number of the manifest that is there now, so nothing reads it, and
	// the close deleted it.
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let other = dir.path().join("other");
		let (other_opts, _) = database_that_has_flushed(&other, suite).await;

		let root = dir.path().join("db");
		let opts = encrypted(&root, suite);
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		write(&tree, '1', 0..KEYS).await;
		tree.close().await.unwrap();
		let segments = files_with_extension(&opts.wal_dir(), "wal");
		assert_eq!(segments.len(), 1, "{suite:?}: the log of a database that has not flushed");

		// The manifest and the table of the other database take their place.
		fs::copy(other_opts.manifest_file_path(0), opts.manifest_file_path(0)).unwrap();
		for table in files_with_extension(&other_opts.sstable_dir(), "sst") {
			fs::copy(&table, opts.sstable_dir().join(table.file_name().unwrap())).unwrap();
		}
		let segment = fs::read(&segments[0]).unwrap();
		let before = snapshot(&root);

		let err = open_error(&opts);
		assert!(
			matches!(err, Error::Encryption(EncryptionError::DbIdMismatch)),
			"{suite:?}: {err:?}"
		);
		assert!(snapshot(&root) == before, "{suite:?}: a refused open changed the database");
		assert_eq!(fs::read(&segments[0]).unwrap(), segment, "{suite:?}");
	}
}
