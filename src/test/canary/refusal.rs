//! A wrong key, a file of the wrong kind and a damaged file, each met by a whole database: the
//! open, or the first read that touches the damage, says the right thing, and nothing on disk is
//! repaired, replaced, renamed or removed because of it.
//!
//! This is the other half of the canary. The matrix shows that nothing is readable without the
//! key; this shows that what is done with a file that is not what it should be is safe: the key
//! is not at fault for a missing file and the file is not at fault for a missing key, and the one
//! thing that must never follow from either is a repair, which for a WAL means deleting the
//! acknowledged writes that it holds.
//!
//! # How a case is run
//!
//! A database is made once for each suite and kept as it is. Each case takes a copy, changes one
//! thing in the copy, and opens it, in both recovery modes of the WAL wherever a WAL is there,
//! which is wherever a database was not closed after a flush. What must follow:
//!
//! - an open that is refused returns the typed error of the case, and an image of the directory
//!   taken before and after, which is the name and the bytes of every file and every directory but
//!   the lock, is the same;
//! - a file whose damage the open does not reach (a data block, an index partition, a filter
//!   partition, an entry of the value log) lets the open succeed, and then the first read that
//!   touches it fails with the typed error, no other read returns a wrong value or none, and
//!   nothing that was in the directory has changed or gone: all that is new is the segment of the
//!   WAL that every open makes.
//!
//! # Which error is the right one
//!
//! `AuthenticationFailed` for a table block, a record of the WAL or the body of the manifest that
//! was changed with the checksum made again, which is what someone who has no key can do and
//! what only the tag can find. `WrongKey` for a root that does not open the manifest, and for a
//! salt that was changed with the checksum of the header made again. `DbIdMismatch` for a file of
//! another database that has the same key. `PlaintextInEncryptedDb` and `NotConfigured` for a
//! file of the other kind, `NotEncrypted` for a database of the other kind. `UnknownKeyId` and
//! `KeyRetired` for a key that the manager does not have or no longer has. An index gap for a
//! block that was zeroed, a segment that is missing and a cut in a segment that others follow;
//! a tear, which is repaired only when the mode says so, for a cut in the last one.
//!
//! The entry of the value log is the one place where the damage is not an authentication
//! failure: its checksum is held by the pointer to it, inside a sealed table, so no one without
//! the key can make it agree with a changed entry, and a changed entry is found by the checksum
//! first, as a corrupt value. The tag of an entry is tested where a pointer can be made to
//! agree, in the tests of the value log.
//!
//! # What is not in here
//!
//! A manifest of another database under the same key is accepted as the database, since the
//! identity of a database is its manifest's. What is refused is each file that names a database
//! other than the manifest's. The foreign manifest below lists tables that exist, which the open
//! refuses for their database; the tables that a manifest does not list, and the segments of the
//! WAL below its log number, are refused for theirs in `orphan_cleanup_tests` and
//! `wal_foreign_segment_tests`.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use super::files::{assert_holds_no_key, copy_tree, files_of, last_file, DirImage};
use super::matrix::{crash, open_stopped, set_len};
use crate::error::{EncryptionError, Error};
use crate::sstable::block_io::{BlockKind, TrailerLayout};
use crate::tde::{
	CipherSuite,
	DbId,
	EncryptionContext,
	KeyManager,
	SoftwareKeyManager,
	HEADER_LEN,
	MANIFEST_PREFIX_LEN,
};
use crate::test::sstable_encryption_tests::{first_of, flip, walk};
use crate::test::tfs;
use crate::test::wal_sealed_tests::{fix_checksum, physical_records};
use crate::wal::{BLOCK_SIZE, HEADER_SIZE};
use crate::{Durability, Mode, Options, Tree, WalRecoveryMode};

const SUITES: [CipherSuite; 2] = [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];

const MODES: [WalRecoveryMode; 2] =
	[WalRecoveryMode::AbsoluteConsistency, WalRecoveryMode::TolerateCorruptedWithRepair];

/// The root keys of the databases. They are not runs of one byte, so that a file that holds one
/// was written with it, and the databases are scanned for them.
const ROOT: [u8; 32] = *b"root key of the refusal matrix 1";
const ROOT_2: [u8; 32] = *b"second root key of the refusal 2";

/// What a database is made with: whether it has a value log.
#[derive(Clone, Copy)]
struct Shape {
	vlog: bool,
}

const WITH_LOG: Shape = Shape {
	vlog: true,
};

const WITHOUT_LOG: Shape = Shape {
	vlog: false,
};

/// How a database is opened.
#[derive(Clone)]
enum Auth {
	/// With no key manager.
	None,
	/// With the root that the database was made with.
	Right,
	/// With another root behind the same key id.
	WrongRoot,
	/// With the right root and a context that names another database.
	ForeignDatabase,
	/// With a key manager that the test made.
	With(Arc<dyn KeyManager>),
}

fn manager(root: [u8; 32]) -> Arc<dyn KeyManager> {
	Arc::new(SoftwareKeyManager::new(root))
}

fn options(
	path: &Path,
	shape: Shape,
	mode: WalRecoveryMode,
	suite: CipherSuite,
	auth: &Auth,
) -> Arc<Options> {
	let opts = Options {
		path: path.to_path_buf(),
		enable_vlog: shape.vlog,
		vlog_value_threshold: 64,
		// What was committed stays where it is put: nothing is flushed by a close, and nothing
		// is flushed or compacted unless a test says so.
		flush_on_close: false,
		max_memtable_size: 16 * 1024 * 1024,
		memtable_stall_threshold: 1000,
		level0_max_files: 100,
		l0_stall_threshold: 1000,
		// Small blocks and partitions, so that a table of a few dozen keys has every kind of block
		// more than once.
		block_size: 256,
		block_restart_interval: 4,
		index_partition_size: 100,
		wal_recovery_mode: mode,
		..Default::default()
	};
	Arc::new(match auth {
		Auth::None => opts,
		Auth::Right => opts.with_encryption(manager(ROOT), suite),
		Auth::WrongRoot => opts.with_encryption(manager([0x43; 32]), suite),
		Auth::ForeignDatabase => {
			let foreign =
				EncryptionContext::new(manager(ROOT), DbId::from_bytes([0xEE; 16]), suite);
			opts.with_encryption_context(Arc::new(foreign))
		}
		Auth::With(key_manager) => opts.with_encryption(Arc::clone(key_manager), suite),
	})
}

// ---------------------------------------------------------------------------
// what is expected
// ---------------------------------------------------------------------------

/// What an error is expected to be.
#[derive(Clone, Debug)]
enum Says {
	Encryption(EncryptionError),
	/// A table that does not load: the error of a damaged file or of a key, whichever the loader
	/// reaches first.
	LoadFailedOrKey,
	IndexGap,
	Tear,
	ManifestDamaged,
	ManifestMissing,
	RestoreInterrupted,
}

impl Says {
	fn holds(&self, error: &Error) -> bool {
		match self {
			Says::Encryption(expected) => {
				matches!(error, Error::Encryption(found) if found == expected)
			}
			Says::LoadFailedOrKey => {
				matches!(error, Error::LoadManifestFail(_) | Error::Encryption(_))
			}
			Says::IndexGap => {
				matches!(error, Error::Wal(message) if message.contains("Index gap"))
			}
			Says::Tear => matches!(error, Error::WalCorruption { .. }),
			Says::ManifestDamaged => matches!(error, Error::ManifestCorruption(_)),
			Says::ManifestMissing => matches!(error, Error::ManifestMissing(_)),
			Says::RestoreInterrupted => matches!(error, Error::RestoreInterrupted(_)),
		}
	}
}

fn encryption(error: EncryptionError) -> Says {
	Says::Encryption(error)
}

/// A copy of the files of `pristine`, to be changed. The lock is not copied.
fn work_copy(pristine: &Path) -> TempDir {
	let work = TempDir::new().unwrap();
	copy_tree(pristine, work.path());
	work
}

/// The first of them.
fn first_file(root: &Path, dir: &str, ext: &str) -> PathBuf {
	files_of(root, dir, ext).into_iter().next().unwrap_or_else(|| panic!("no .{ext} file in {dir}"))
}

fn manifest_path(root: &Path) -> PathBuf {
	root.join("manifest").join(format!("{:020}.manifest", 0))
}

/// A database that is kept as it is, and what it is made with.
struct Pristine<'a> {
	path: &'a Path,
	shape: Shape,
	suite: CipherSuite,
}

impl Pristine<'_> {
	/// Every mode in `modes` refuses the copy of the database that `spoil` has changed, with an
	/// error that `says`, and leaves every file and directory of it as it was.
	fn refuse(
		&self,
		what: &str,
		auth: &Auth,
		modes: &[WalRecoveryMode],
		spoil: impl Fn(&Path),
		says: &Says,
	) {
		for &mode in modes {
			let work = work_copy(self.path);
			spoil(work.path());
			let before = DirImage::of(work.path());
			let error = match Tree::new(options(work.path(), self.shape, mode, self.suite, auth)) {
				Ok(_) => panic!("{what}, {mode:?}: the database was opened"),
				Err(error) => error,
			};
			assert!(
				says.holds(&error),
				"{what}, {mode:?}: {says:?} was expected, and it is {error:?}"
			);
			DirImage::of(work.path())
				.assert_unchanged_since(&before, &format!("{what}, {mode:?}, {:?}", self.suite));
		}
	}
}

// ---------------------------------------------------------------------------
// the databases
// ---------------------------------------------------------------------------

fn key_of(i: usize) -> Vec<u8> {
	format!("key-{i:03}").into_bytes()
}

/// A value of 100 bytes, which the value log takes when there is one.
fn value_of(round: char, i: usize) -> Vec<u8> {
	let mut value = format!("val-{round}-{i:03}-").into_bytes();
	value.resize(100, b'x');
	value
}

async fn commit(tree: &Tree, entries: &[(Vec<u8>, Vec<u8>)]) {
	let mut txn = tree.begin().unwrap();
	for (key, value) in entries {
		txn.set(key.clone(), value.clone()).unwrap();
	}
	txn.commit().await.unwrap();
}

/// What a database holds, and the context that its files are sealed with.
struct Db {
	dir: TempDir,
	context: Option<Arc<EncryptionContext>>,
	expected: BTreeMap<Vec<u8>, Vec<u8>>,
}

/// A database with a value log, two tables that overlap, and five commits that are in the WAL
/// and in no table: closed, with nothing flushed by the close.
async fn tables_db(suite: CipherSuite, auth: &Auth) -> Db {
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path(), WITH_LOG, WalRecoveryMode::AbsoluteConsistency, suite, auth);
	let tree = Tree::new(opts).unwrap();
	let mut expected = BTreeMap::new();
	for (round, range) in [('1', 0..60), ('2', 30..90)] {
		let entries: Vec<(Vec<u8>, Vec<u8>)> =
			range.map(|i| (key_of(i), value_of(round, i))).collect();
		commit(&tree, &entries).await;
		expected.extend(entries);
		tree.flush().unwrap();
	}
	let unflushed: Vec<(Vec<u8>, Vec<u8>)> =
		(0..5).map(|i| (format!("w-{i}").into_bytes(), value_of('w', i))).collect();
	commit(&tree, &unflushed).await;
	expected.extend(unflushed);
	let context = tree.core.inner.opts.encryption_context().cloned();
	tree.close().await.unwrap();
	Db {
		dir,
		context,
		expected,
	}
}

const WAL_KEYS: [&str; 6] = ["a0", "m0", "mb", "m1", "c0", "c1"];

/// The value of a key of the WAL database: one of them is long enough to fill several blocks.
fn wal_value(key: &str) -> Vec<u8> {
	if key == "mb" {
		vec![b'M'; 150_000]
	} else {
		format!("small-value-of-{key}").into_bytes()
	}
}

async fn put(tree: &Tree, key: &str, value: Vec<u8>) {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(Durability::Immediate);
	txn.set(key.as_bytes().to_vec(), value).unwrap();
	txn.commit().await.unwrap();
}

/// A database with no value log and three segments of WAL, nothing flushed, ended with a crash:
/// the first has one commit, the second three of which one fills several blocks, and the third
/// two.
async fn wal_db(suite: CipherSuite) -> TempDir {
	let dir = TempDir::new().unwrap();
	let opts =
		options(dir.path(), WITHOUT_LOG, WalRecoveryMode::AbsoluteConsistency, suite, &Auth::Right);
	let tree = open_stopped(opts).await;
	put(&tree, "a0", wal_value("a0")).await;
	tree.core.inner.rotate_memtable().unwrap();
	for key in ["m0", "mb", "m1"] {
		put(&tree, key, wal_value(key)).await;
	}
	tree.core.inner.rotate_memtable().unwrap();
	for key in ["c0", "c1"] {
		put(&tree, key, wal_value(key)).await;
	}
	tree.flush_wal(false).unwrap();
	crash(&tree);
	drop(tree);
	dir
}

/// The first, the longest and the last segment of the WAL in `root`: the longest is the one with
/// the long commit, which is between the other two.
fn wal_segments(root: &Path) -> (PathBuf, PathBuf, PathBuf) {
	let segments = files_of(root, "wal", "wal");
	assert!(segments.len() >= 3, "the WAL has {} segment(s)", segments.len());
	let longest =
		segments.iter().max_by_key(|segment| fs::metadata(segment).unwrap().len()).unwrap().clone();
	let (first, last) = (segments[0].clone(), segments[segments.len() - 1].clone());
	assert!(longest != first && longest != last, "the long segment is between the others");
	(first, longest, last)
}

// ---------------------------------------------------------------------------
// opening a database and reading it
// ---------------------------------------------------------------------------

struct Reads {
	failures: Vec<Error>,
	wrong: Vec<Vec<u8>>,
	missing: Vec<Vec<u8>>,
}

enum Outcome {
	Refused(Error),
	Opened(Reads),
}

/// Fails if the text of `error`, as it is shown and as it is debugged, which is what a log line or
/// a caller that did not ask for the data gets, holds a key or a value of the database. Keys that
/// are too short to be told from the words of a message are left out.
fn assert_names_no_data(error: &Error, expected: &BTreeMap<Vec<u8>, Vec<u8>>) {
	const SHORTEST: usize = 6;
	let text = format!("{error}\n{error:?}").into_bytes();
	for (key, value) in expected {
		for datum in [key, value] {
			assert!(
				datum.len() < SHORTEST || !text.windows(datum.len()).any(|window| window == datum),
				"an error names the data of the database: {error:?}"
			);
		}
	}
}

/// Opens the database, and, if it opens, reads every key of `expected`.
async fn open_and_read(
	path: &Path,
	shape: Shape,
	mode: WalRecoveryMode,
	suite: CipherSuite,
	auth: &Auth,
	expected: &BTreeMap<Vec<u8>, Vec<u8>>,
) -> Outcome {
	let opts = options(path, shape, mode, suite, auth);
	let tree = match Tree::new(opts) {
		Ok(tree) => tree,
		Err(error) => {
			assert_names_no_data(&error, expected);
			return Outcome::Refused(error);
		}
	};
	let mut reads = Reads {
		failures: Vec::new(),
		wrong: Vec::new(),
		missing: Vec::new(),
	};
	{
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		for (key, value) in expected {
			match txn.get(key.as_slice()) {
				Ok(Some(found)) if found == *value => {}
				Ok(Some(_)) => reads.wrong.push(key.clone()),
				Ok(None) => reads.missing.push(key.clone()),
				Err(error) => {
					assert_names_no_data(&error, expected);
					reads.failures.push(error);
				}
			}
		}
	}
	tree.close().await.unwrap();
	Outcome::Opened(reads)
}

/// What a damaged file leaves behind: an open that was refused changed nothing at all; an open
/// that was not refused read what it could, and each read that met the damage failed with an
/// error that `says`, no read gave a wrong value or none, and the only thing in the directory
/// that is new is a segment of the WAL.
fn assert_caught(what: &str, work: &Path, before: &DirImage, outcome: Outcome, says: &Says) {
	let after = DirImage::of(work);
	match outcome {
		Outcome::Refused(error) => {
			assert!(
				says.holds(&error),
				"{what}: refused with {error:?}, and {says:?} was expected"
			);
			after.assert_unchanged_since(before, what);
		}
		Outcome::Opened(reads) => {
			assert!(
				reads.wrong.is_empty(),
				"{what}: a wrong value for {} key(s)",
				reads.wrong.len()
			);
			assert!(reads.missing.is_empty(), "{what}: {} key(s) disappeared", reads.missing.len());
			assert!(!reads.failures.is_empty(), "{what}: the damage was never noticed");
			for error in &reads.failures {
				assert!(says.holds(error), "{what}: a read failed with {error:?}");
			}
			let changes = after.changes_since(before);
			assert!(
				changes.iter().all(|change| change.starts_with("file added: wal")),
				"{what}: more than a segment of the WAL changed: {changes:?}"
			);
		}
	}
}

// ---------------------------------------------------------------------------
// a key that does not fit
// ---------------------------------------------------------------------------

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn a_key_that_does_not_fit_is_refused_with_its_error_and_no_file_changes() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let tables = tables_db(suite, &Auth::Right).await;
		let wal = wal_db(suite).await;
		let plain = tables_db(suite, &Auth::None).await;
		// The key of these databases is in no file of either.
		for (name, path) in [("tables", tables.dir.path()), ("wal", wal.path())] {
			assert_holds_no_key(
				path,
				&[("the root key", &ROOT[..])],
				&format!("{suite:?}, {name}"),
			);
		}

		for (name, path, shape) in
			[("tables", tables.dir.path(), WITH_LOG), ("wal", wal.path(), WITHOUT_LOG)]
		{
			let db = Pristine {
				path,
				shape,
				suite,
			};
			let nothing = |_: &Path| {};
			let cases = [
				("another root behind the key id", Auth::WrongRoot, EncryptionError::WrongKey),
				("no key manager", Auth::None, EncryptionError::NotConfigured),
				(
					"the right key for another database",
					Auth::ForeignDatabase,
					EncryptionError::DbIdMismatch,
				),
			];
			for (what, auth, error) in cases {
				db.refuse(&format!("{name}: {what}"), &auth, &MODES, nothing, &encryption(error));
			}

			// A restore that did not finish is the answer whatever the key, and no key is tried.
			let marker =
				|work: &Path| fs::write(work.join("RESTORE_IN_PROGRESS"), b"a checkpoint").unwrap();
			for auth in [Auth::Right, Auth::WrongRoot, Auth::None] {
				db.refuse(
					&format!("{name}: a restore that did not finish"),
					&auth,
					&MODES,
					marker,
					&Says::RestoreInterrupted,
				);
			}

			// The manifest is gone and the data is not: with a key and without one.
			let gone = |work: &Path| fs::remove_file(manifest_path(work)).unwrap();
			for auth in [Auth::Right, Auth::None] {
				db.refuse(
					&format!("{name}: no manifest"),
					&auth,
					&MODES,
					gone,
					&Says::ManifestMissing,
				);
			}
		}

		// A key manager for a database that is not encrypted.
		let db = Pristine {
			path: plain.dir.path(),
			shape: WITH_LOG,
			suite,
		};
		db.refuse(
			"a plaintext database opened with a key",
			&Auth::Right,
			&MODES,
			|_| {},
			&encryption(EncryptionError::NotEncrypted),
		);
	}
}

// ---------------------------------------------------------------------------
// a file of the wrong kind or of another database
// ---------------------------------------------------------------------------

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn a_file_of_the_other_kind_is_refused_in_either_direction_and_nothing_changes() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let sealed = tables_db(suite, &Auth::Right).await;
		let plain = tables_db(suite, &Auth::None).await;
		let (sealed_path, plain_path) = (sealed.dir.path(), plain.dir.path());

		// The kinds of file that a database has, and where the file of the other database is.
		let replacements: [(&str, &str, &str); 3] = [
			("table", "sstables", "sst"),
			("value-log file", "vlog", "vlog"),
			("segment", "wal", "wal"),
		];
		for (name, dir, ext) in replacements {
			// A plaintext file where the database is encrypted: it is not read as if it were
			// sealed, nor repaired, and nothing is created in place of it.
			let from = last_file(plain_path, dir, ext);
			let in_the_clear = |work: &Path| {
				fs::copy(&from, last_file(work, dir, ext)).unwrap();
			};
			Pristine {
				path: sealed_path,
				shape: WITH_LOG,
				suite,
			}
			.refuse(
				&format!("a plaintext {name} in an encrypted database"),
				&Auth::Right,
				&MODES,
				in_the_clear,
				&encryption(EncryptionError::PlaintextInEncryptedDb),
			);

			// A sealed file where the database is not encrypted.
			let from = last_file(sealed_path, dir, ext);
			let sealed_file = |work: &Path| {
				fs::copy(&from, last_file(work, dir, ext)).unwrap();
			};
			Pristine {
				path: plain_path,
				shape: WITH_LOG,
				suite,
			}
			.refuse(
				&format!("a sealed {name} in a database that is not encrypted"),
				&Auth::None,
				&MODES,
				sealed_file,
				&encryption(EncryptionError::NotConfigured),
			);
		}
	}
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn a_file_of_another_database_with_the_same_key_is_refused_for_its_database() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let ours = tables_db(suite, &Auth::Right).await;
		// Made the same way, so its files have the names of ours, and its own identity.
		let foreign = tables_db(suite, &Auth::Right).await;
		let db = Pristine {
			path: ours.dir.path(),
			shape: WITH_LOG,
			suite,
		};

		for (name, dir, ext) in [
			("table", "sstables", "sst"),
			("value-log file", "vlog", "vlog"),
			("segment", "wal", "wal"),
		] {
			let from = last_file(foreign.dir.path(), dir, ext);
			let theirs = |work: &Path| {
				fs::copy(&from, last_file(work, dir, ext)).unwrap();
			};
			db.refuse(
				&format!("a {name} of another database"),
				&Auth::Right,
				&MODES,
				theirs,
				&encryption(EncryptionError::DbIdMismatch),
			);
		}

		// The manifest of another database: it says that the database is that one, and the
		// tables that it lists are ours, which are of the database that is ours.
		let from = manifest_path(foreign.dir.path());
		let theirs = |work: &Path| {
			fs::copy(&from, manifest_path(work)).unwrap();
		};
		db.refuse(
			"the manifest of another database",
			&Auth::Right,
			&MODES,
			theirs,
			&encryption(EncryptionError::DbIdMismatch),
		);
	}
}

// ---------------------------------------------------------------------------
// the manifest
// ---------------------------------------------------------------------------

/// The start of the crypto header in a sealed manifest.
const HEADER_AT: usize = MANIFEST_PREFIX_LEN - HEADER_LEN;

/// `bytes` with the checksum of the header made again.
fn with_header_checksum(mut bytes: Vec<u8>) -> Vec<u8> {
	let end = MANIFEST_PREFIX_LEN - 4;
	let crc = crc32fast::hash(&bytes[HEADER_AT..end]);
	bytes[end..MANIFEST_PREFIX_LEN].copy_from_slice(&crc.to_be_bytes());
	bytes
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn a_changed_manifest_is_the_error_of_the_part_that_changed_and_no_file_changes() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let db = tables_db(suite, &Auth::Right).await;
		let pristine = Pristine {
			path: db.dir.path(),
			shape: WITH_LOG,
			suite,
		};
		let sealed = fs::read(manifest_path(db.dir.path())).unwrap();
		let flip_at = |at: usize| {
			let mut bytes = sealed.clone();
			bytes[at] ^= 0x01;
			bytes
		};
		let last = sealed.len() - 1;

		let cases: Vec<(&str, Vec<u8>, Says)> = vec![
			(
				"the first byte of the body",
				flip_at(MANIFEST_PREFIX_LEN),
				encryption(EncryptionError::AuthenticationFailed),
			),
			(
				"the last byte of the tag",
				flip_at(last),
				encryption(EncryptionError::AuthenticationFailed),
			),
			(
				"the salt, with the checksum of the header made again",
				with_header_checksum(flip_at(HEADER_AT + 40)),
				encryption(EncryptionError::WrongKey),
			),
			(
				"the checksum of the header",
				flip_at(MANIFEST_PREFIX_LEN - 1),
				encryption(EncryptionError::MalformedHeader),
			),
			("a file cut inside its body", sealed[..last].to_vec(), Says::ManifestDamaged),
			("a file cut inside its header", sealed[..100].to_vec(), Says::ManifestDamaged),
		];
		for (what, bytes, says) in cases {
			pristine.refuse(
				&format!("a manifest with {what}"),
				&Auth::Right,
				&MODES,
				|work| fs::write(manifest_path(work), &bytes).unwrap(),
				&says,
			);
		}
	}
}

// ---------------------------------------------------------------------------
// a block of each kind
// ---------------------------------------------------------------------------

const KINDS: [BlockKind; 6] = [
	BlockKind::Data,
	BlockKind::IndexPartition,
	BlockKind::TopLevelIndex,
	BlockKind::FilterPartition,
	BlockKind::FilterIndex,
	BlockKind::MetaIndex,
];

fn table_id(path: &Path) -> u64 {
	path.file_stem().unwrap().to_str().unwrap().parse().unwrap()
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn a_changed_byte_in_a_block_of_each_kind_fails_authentication_where_the_block_is_read() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let db = tables_db(suite, &Auth::Right).await;
		let context = db.context.clone().expect("an encrypted database has a context");
		let reader = Arc::new(Options::default().with_encryption_context(context));
		let table = first_file(db.dir.path(), "sstables", "sst");
		let id = table_id(&table);
		let bytes = fs::read(&table).unwrap();
		let blocks = walk(&reader, &bytes, id);

		for kind in KINDS {
			// The first block of the kind, which holds the first keys of the table, the ones that
			// the later table does not have again. A byte of it is changed and the checksum of the
			// block is made again, as someone with no key can.
			let handle = first_of(&blocks, kind);
			for mode in MODES {
				let work = work_copy(db.dir.path());
				let path = first_file(work.path(), "sstables", "sst");
				let changed = flip(&bytes, &handle, handle.size / 2, TrailerLayout::Sealed, true);
				fs::write(&path, changed).unwrap();
				let before = DirImage::of(work.path());
				let outcome =
					open_and_read(work.path(), WITH_LOG, mode, suite, &Auth::Right, &db.expected)
						.await;
				assert_caught(
					&format!("{suite:?}, {mode:?}, a block of kind {kind:?}"),
					work.path(),
					&before,
					outcome,
					&encryption(EncryptionError::AuthenticationFailed),
				);
			}
		}

		// A table that was cut short is not a table, and is not repaired.
		let pristine = Pristine {
			path: db.dir.path(),
			shape: WITH_LOG,
			suite,
		};
		for cut in [10, 60, bytes.len() / 2] {
			pristine.refuse(
				&format!("a table with its last {cut} bytes cut off"),
				&Auth::Right,
				&MODES,
				|work| set_len(&first_file(work, "sstables", "sst"), (bytes.len() - cut) as u64),
				&Says::LoadFailedOrKey,
			);
		}
	}
}

// ---------------------------------------------------------------------------
// the value log
// ---------------------------------------------------------------------------

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn a_changed_entry_of_the_value_log_is_a_corrupt_value_and_never_a_wrong_one() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let db = tables_db(suite, &Auth::Right).await;
		let log = first_file(db.dir.path(), "vlog", "vlog");
		let bytes = fs::read(&log).unwrap();
		// The header of a sealed file is 127 bytes, and an entry of the first value that was
		// written is 140 bytes or more: this byte is inside it.
		assert!(bytes.len() > 127 + 140);

		for mode in MODES {
			let what = format!("{suite:?}, {mode:?}");

			let work = work_copy(db.dir.path());
			let mut changed = bytes.clone();
			changed[127 + 40] ^= 0x01;
			fs::write(first_file(work.path(), "vlog", "vlog"), changed).unwrap();
			let before = DirImage::of(work.path());
			let outcome =
				open_and_read(work.path(), WITH_LOG, mode, suite, &Auth::Right, &db.expected).await;
			// The checksum of an entry is held by the pointer to it, which is in a sealed table, so
			// nobody without the key can make a changed entry agree with it: it is found first.
			let Outcome::Opened(reads) = outcome else {
				panic!("{what}: the open of a database whose log has a changed entry was refused");
			};
			assert!(reads.wrong.is_empty() && reads.missing.is_empty(), "{what}");
			assert!(!reads.failures.is_empty(), "{what}: the change was never noticed");
			for error in &reads.failures {
				assert!(
					matches!(error, Error::Other(message) if message.contains("CRC32 mismatch")),
					"{what}: {error:?}"
				);
			}
			let changes = DirImage::of(work.path()).changes_since(&before);
			assert!(
				changes.iter().all(|change| change.starts_with("file added: wal")),
				"{what}: {changes:?}"
			);

			// A file that was cut short. The commits that were not flushed are in the last segment
			// of the WAL, and the open holds that segment to the value log: with the
			// last entry cut, the commit that it belongs to is not replayed, and the
			// mode that refuses damage refuses it. Every other entry that is cut is an
			// error that says so, where it is read.
			for cut in [bytes.len() - 20, 127 + 20] {
				let what = format!("{what}, the value log cut to {cut} of {} bytes", bytes.len());
				let work = work_copy(db.dir.path());
				set_len(&first_file(work.path(), "vlog", "vlog"), cut as u64);
				let before = DirImage::of(work.path());
				let outcome =
					open_and_read(work.path(), WITH_LOG, mode, suite, &Auth::Right, &db.expected)
						.await;
				if mode == WalRecoveryMode::AbsoluteConsistency {
					let Outcome::Refused(error) = outcome else {
						panic!("{what}: the open of a database whose log was cut short succeeded");
					};
					assert!(matches!(error, Error::WalCorruption { .. }), "{what}: {error:?}");
					DirImage::of(work.path()).assert_unchanged_since(&before, &what);
					continue;
				}
				let Outcome::Opened(reads) = outcome else {
					panic!("{what}: the open of a database whose log was cut short was refused");
				};
				assert!(reads.wrong.is_empty(), "{what}");
				let unflushed: Vec<Vec<u8>> =
					db.expected.keys().filter(|key| key.starts_with(b"w-")).cloned().collect();
				assert_eq!(reads.missing, unflushed, "{what}: the commit whose value is cut");
				for error in &reads.failures {
					assert!(
						matches!(error, Error::Other(message) if message.contains("cut short")),
						"{what}: {error:?}"
					);
				}
				if cut == 127 + 20 {
					assert!(!reads.failures.is_empty(), "{what}: the cut was never noticed");
				}
				let changes = DirImage::of(work.path()).changes_since(&before);
				assert!(
					changes.iter().all(|change| change.starts_with("file added: wal")
						|| change.starts_with("file changed: wal")),
					"{what}: {changes:?}"
				);
			}
		}
	}
}

// ---------------------------------------------------------------------------
// the WAL
// ---------------------------------------------------------------------------

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn a_damaged_wal_is_never_repaired_unless_it_is_a_tear_in_the_last_segment() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let pristine_dir = wal_db(suite).await;
		let pristine = Pristine {
			path: pristine_dir.path(),
			shape: WITHOUT_LOG,
			suite,
		};

		// A record of the long segment changed, with the checksum of the record made again, and
		// the second record of the second segment is the first commit of it.
		pristine.refuse(
			"a record that was changed, with its checksum made again",
			&Auth::Right,
			&MODES,
			|work| {
				let (_, middle, _) = wal_segments(work);
				let mut bytes = fs::read(&middle).unwrap();
				let (at, _, _) = physical_records(&bytes)[1];
				bytes[at + HEADER_SIZE + 3] ^= 0x01;
				fix_checksum(&mut bytes, at);
				fs::write(&middle, bytes).unwrap();
			},
			&encryption(EncryptionError::AuthenticationFailed),
		);

		// A block in the middle of the long segment that was zeroed, with records after it.
		pristine.refuse(
			"a block in the middle of a segment that was zeroed",
			&Auth::Right,
			&MODES,
			|work| {
				let (_, middle, _) = wal_segments(work);
				let mut bytes = fs::read(&middle).unwrap();
				let blocks = bytes.len().div_ceil(BLOCK_SIZE);
				assert!(blocks >= 5, "the long segment is {} bytes, {blocks} blocks", bytes.len());
				bytes[2 * BLOCK_SIZE..3 * BLOCK_SIZE].fill(0);
				fs::write(&middle, bytes).unwrap();
			},
			&Says::IndexGap,
		);

		// A segment between two others that is missing, and one that was cut with others after it.
		pristine.refuse(
			"a segment in the middle that is missing",
			&Auth::Right,
			&MODES,
			|work| fs::remove_file(wal_segments(work).1).unwrap(),
			&Says::IndexGap,
		);
		pristine.refuse(
			"a segment that was cut, with segments after it that hold commits",
			&Auth::Right,
			&MODES,
			|work| {
				let (_, middle, _) = wal_segments(work);
				set_len(&middle, fs::metadata(&middle).unwrap().len() - 10);
			},
			&Says::IndexGap,
		);

		// The last segment cut in the middle of its last record is the tear that a crash leaves.
		// The mode that refuses damage refuses it and changes nothing.
		let cut = |work: &Path| {
			let (_, _, last) = wal_segments(work);
			set_len(&last, fs::metadata(&last).unwrap().len() - 10);
		};
		pristine.refuse(
			"a tear in the last segment",
			&Auth::Right,
			&[WalRecoveryMode::AbsoluteConsistency],
			cut,
			&Says::Tear,
		);

		// The mode that repairs keeps every commit before the one that was cut, and keeps what it
		// found: the segment, as it was, is still in the directory.
		let work = work_copy(pristine_dir.path());
		cut(work.path());
		let (_, _, last) = wal_segments(work.path());
		let found = fs::read(&last).unwrap();
		let opts = options(
			work.path(),
			WITHOUT_LOG,
			WalRecoveryMode::TolerateCorruptedWithRepair,
			suite,
			&Auth::Right,
		);
		let tree = Tree::new(opts).unwrap();
		{
			let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
			for key in WAL_KEYS {
				let got = txn.get(key.as_bytes()).unwrap();
				if key == "c1" {
					assert!(got.is_none(), "{suite:?}: the commit that was cut came back");
				} else {
					assert_eq!(got, Some(wal_value(key)), "{suite:?}: {key}");
				}
			}
		}
		tree.close().await.unwrap();
		let kept: Vec<PathBuf> = fs::read_dir(work.path().join("wal"))
			.unwrap()
			.map(|entry| entry.unwrap().path())
			.filter(|path| path.file_name().unwrap().to_string_lossy().contains(".wal.corrupt"))
			.collect();
		assert!(
			kept.iter().any(|path| fs::read(path).unwrap() == found),
			"{suite:?}: the segment that was repaired was not kept as it was found"
		);
	}
}

// ---------------------------------------------------------------------------
// keys that rotate
// ---------------------------------------------------------------------------

/// A database written under key 1, then under key 2 after a rotation: its manifest and its newest
/// files are under 2 and its first files under 1.
async fn rotated_db(suite: CipherSuite) -> Db {
	let dir = TempDir::new().unwrap();
	let keys = Arc::new(SoftwareKeyManager::new(ROOT));
	let auth = Auth::With(Arc::clone(&keys) as Arc<dyn KeyManager>);
	let mut expected = BTreeMap::new();

	let opts = options(dir.path(), WITH_LOG, WalRecoveryMode::AbsoluteConsistency, suite, &auth);
	let tree = Tree::new(opts).unwrap();
	let first: Vec<(Vec<u8>, Vec<u8>)> = (0..30).map(|i| (key_of(i), value_of('1', i))).collect();
	commit(&tree, &first).await;
	expected.extend(first);
	tree.flush().unwrap();
	let second: Vec<(Vec<u8>, Vec<u8>)> = (0..5).map(|i| (key_of(i), value_of('2', i))).collect();
	commit(&tree, &second).await;
	expected.extend(second);
	tree.close().await.unwrap();

	keys.add_key(2, ROOT_2).unwrap();
	keys.set_active_key_id(2).unwrap();
	let opts = options(dir.path(), WITH_LOG, WalRecoveryMode::AbsoluteConsistency, suite, &auth);
	let tree = Tree::new(opts).unwrap();
	let third: Vec<(Vec<u8>, Vec<u8>)> = (30..40).map(|i| (key_of(i), value_of('3', i))).collect();
	commit(&tree, &third).await;
	expected.extend(third);
	tree.flush().unwrap();
	let context = tree.core.inner.opts.encryption_context().cloned();
	tree.close().await.unwrap();
	Db {
		dir,
		context,
		expected,
	}
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn a_key_that_rotated_is_needed_until_it_is_retired_and_then_the_database_is_refused() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let db = rotated_db(suite).await;
		assert_holds_no_key(
			db.dir.path(),
			&[("the root of key 1", &ROOT[..]), ("the root of key 2", &ROOT_2[..])],
			&format!("{suite:?}, written under two keys"),
		);
		let pristine = Pristine {
			path: db.dir.path(),
			shape: WITH_LOG,
			suite,
		};

		// The manager has only key 1, and the manifest is under key 2.
		pristine.refuse(
			"a manager that does not know the key of the manifest",
			&Auth::With(manager(ROOT)),
			&MODES,
			|_| {},
			&encryption(EncryptionError::UnknownKeyId),
		);

		// Key 1 is retired: the manifest opens, and the first table does not.
		let retired = Arc::new(SoftwareKeyManager::new(ROOT));
		retired.add_key(2, ROOT_2).unwrap();
		retired.set_active_key_id(2).unwrap();
		retired.retire_key(1).unwrap();
		pristine.refuse(
			"a retired key that the first table is under",
			&Auth::With(retired as Arc<dyn KeyManager>),
			&MODES,
			|_| {},
			&encryption(EncryptionError::KeyRetired),
		);

		// Key 1 is decrypt-only, which is what it is after a rotation: every file is read, the
		// ones under key 1 and the ones under key 2.
		let both = Arc::new(SoftwareKeyManager::new(ROOT));
		both.add_key(2, ROOT_2).unwrap();
		both.set_active_key_id(2).unwrap();
		for mode in MODES {
			let work = work_copy(db.dir.path());
			let outcome = open_and_read(
				work.path(),
				WITH_LOG,
				mode,
				suite,
				&Auth::With(Arc::clone(&both) as Arc<dyn KeyManager>),
				&db.expected,
			)
			.await;
			match outcome {
				Outcome::Opened(reads) => {
					assert!(reads.failures.is_empty(), "{suite:?}, {mode:?}: {:?}", reads.failures);
					assert!(
						reads.wrong.is_empty() && reads.missing.is_empty(),
						"{suite:?}, {mode:?}"
					);
				}
				Outcome::Refused(error) => {
					panic!("{suite:?}, {mode:?}: a decrypt-only key is refused: {error:?}")
				}
			}
		}
	}
}
