//! Opening a database that is encrypted, or is meant to be: how a tree decides, from its manifest
//! and the key manager it is given, whether the database is encrypted and which key it needs, and
//! that a refusal never changes a file.
//!
//! The surfaces that are sealed are the manifest, the tables, the value log and the write-ahead
//! log, and every check of the files here looks at all of them.

use std::collections::BTreeMap;
use std::fs;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tempfile::TempDir;

use super::manifest_encryption_tests::frozen_old_decode;
use super::tfs;
use crate::error::EncryptionError;
use crate::levels::{fail_replacements, ReplaceFailure};
use crate::tde::{
	describe_manifest_file,
	is_sealed_manifest,
	CipherPreference,
	CipherSuite,
	CryptoHeader,
	DbId,
	EncryptionContext,
	KeyManager,
	KeyState,
	SoftwareKeyManager,
	Zeroizing,
	ROOT_KEY_LEN,
};
use crate::{Error, Mode, Options, Tree, TreeBuilder};

pub(super) const SUITES: [CipherSuite; 2] =
	[CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];

pub(super) const ROOT: [u8; ROOT_KEY_LEN] = [0x42; ROOT_KEY_LEN];

pub(super) const KEYS: usize = 24;

pub(super) fn key(i: usize) -> Vec<u8> {
	format!("ek-{i:03}").into_bytes()
}

/// A value that is long enough to be moved to the value log, and holds a marker.
pub(super) fn value(round: char, i: usize) -> Vec<u8> {
	format!("ev-canary-{round}-{i:03}-{}", "x".repeat(80)).into_bytes()
}

pub(super) fn manager() -> Arc<SoftwareKeyManager> {
	Arc::new(SoftwareKeyManager::new(ROOT))
}

/// Options of a database in `dir` whose values of 64 bytes or more go to the value log, and which
/// does not flush when it is closed, so that a close leaves what was committed to the log.
pub(super) fn options(dir: &Path) -> Options {
	Options {
		path: dir.to_path_buf(),
		enable_vlog: true,
		vlog_value_threshold: 64,
		vlog_max_file_size: 4096,
		flush_on_close: false,
		memtable_stall_threshold: 1000,
		level0_max_files: 100,
		l0_stall_threshold: 1000,
		..Default::default()
	}
}

pub(super) fn encrypted(
	dir: &Path,
	key_manager: Arc<dyn KeyManager>,
	preference: impl Into<CipherPreference>,
) -> Arc<Options> {
	Arc::new(options(dir).with_encryption(key_manager, preference))
}

pub(super) fn plain(dir: &Path) -> Arc<Options> {
	Arc::new(options(dir))
}

pub(super) async fn write(tree: &Tree, round: char, range: Range<usize>) {
	let mut txn = tree.begin().unwrap();
	for i in range {
		txn.set(key(i), value(round, i)).unwrap();
	}
	txn.commit().await.unwrap();
}

/// Reads every key of `expected`, which says in which round each was last written.
pub(super) fn read(tree: &Tree, expected: &BTreeMap<usize, char>) {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	for (&i, &round) in expected {
		let found = txn.get(key(i).as_slice()).unwrap();
		assert_eq!(found, Some(value(round, i)), "key {i}");
	}
}

pub(super) fn rounds(round: char, range: Range<usize>) -> BTreeMap<usize, char> {
	range.map(|i| (i, round)).collect()
}

/// A database in `dir` that is created with `key_manager` and `suite`, holds `KEYS` keys in a
/// table and in the value log, and some more in the log of the write-ahead log, and is closed.
pub(super) async fn create(
	dir: &Path,
	key_manager: Arc<dyn KeyManager>,
	suite: CipherSuite,
) -> Arc<Options> {
	let opts = encrypted(dir, key_manager, suite);
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	write(&tree, '1', 0..KEYS).await;
	tree.flush().unwrap();
	write(&tree, '2', 0..KEYS / 2).await;
	tree.close().await.unwrap();
	opts
}

/// Every file and directory under `root`, with the bytes of the files.
pub(super) fn snapshot(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
	fn visit(dir: &Path, found: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
		for entry in fs::read_dir(dir).unwrap().flatten() {
			let path = entry.path();
			if entry.file_type().unwrap().is_dir() {
				found.insert(path.clone(), None);
				visit(&path, found);
			} else if path.file_name().is_some_and(|name| name == "LOCK") {
				// The file that a tree that is open holds a lock on, which some systems do not let
				// anyone else read, and which holds a process id and no data: it is there, or not.
				found.insert(path, Some(Vec::new()));
			} else {
				found.insert(path.clone(), Some(fs::read(&path).unwrap()));
			}
		}
	}
	let mut found = BTreeMap::new();
	visit(root, &mut found);
	found
}

pub(super) fn files_with_extension(dir: &Path, extension: &str) -> Vec<PathBuf> {
	let Ok(entries) = fs::read_dir(dir) else {
		return Vec::new();
	};
	let mut files: Vec<PathBuf> = entries
		.flatten()
		.map(|entry| entry.path())
		.filter(|path| path.extension().is_some_and(|ext| ext == extension))
		.collect();
	files.sort();
	files
}

pub(super) fn contains(haystack: &[u8], needle: &[u8]) -> bool {
	haystack.windows(needle.len()).any(|window| window == needle)
}

pub(super) fn open_error(opts: &Arc<Options>) -> Error {
	match Tree::new(Arc::clone(opts)) {
		Ok(_) => panic!("the database was opened"),
		Err(err) => err,
	}
}

pub(super) fn encryption_error(err: Error, what: &str) -> EncryptionError {
	match err {
		Error::Encryption(inner) => inner,
		other => panic!("{what}: expected an encryption error, got {other:?}"),
	}
}

/// The key id that a table's header names.
fn table_key_id(path: &Path) -> u32 {
	let bytes = fs::read(path).unwrap();
	CryptoHeader::decode(&bytes).unwrap().identity.key_id
}

fn table_suite(path: &Path) -> CipherSuite {
	let bytes = fs::read(path).unwrap();
	CryptoHeader::decode(&bytes).unwrap().identity.suite
}

#[tokio::test]
async fn a_database_created_with_a_key_manager_is_encrypted_and_opens_again_with_the_key() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let root = dir.path().join("db");
		let opts = encrypted(&root, manager(), suite);
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// The descriptor is in the manifest and is read without a key: the database, the suite it
		// was created with and the key that wrote the manifest.
		let manifest = opts.manifest_file_path(0);
		let described = describe_manifest_file(&manifest).unwrap();
		assert!(described.encrypted, "{suite:?}");
		assert_eq!(described.suite, Some(suite));
		assert_eq!(described.key_id, Some(1));
		let db_id = described.db_id.expect("a database has an identity");
		assert_ne!(db_id.as_bytes(), &[0u8; 16], "{suite:?}");

		// The tree runs with the context that the manifest and the key manager gave it.
		let context = tree.core.inner.opts.encryption_context().cloned().unwrap();
		assert_eq!((context.db_id(), context.suite()), (db_id, suite));

		write(&tree, '1', 0..KEYS).await;
		tree.flush().unwrap();
		write(&tree, '2', 0..KEYS / 2).await;
		let mut expected = rounds('1', 0..KEYS);
		expected.extend(rounds('2', 0..KEYS / 2));
		read(&tree, &expected);
		tree.close().await.unwrap();

		// What is sealed: the manifest, the table and the value log, and no marker is in them.
		assert!(is_sealed_manifest(&fs::read(&manifest).unwrap()), "{suite:?}");
		let tables = files_with_extension(&opts.sstable_dir(), "sst");
		let logs = files_with_extension(&opts.vlog_dir(), "vlog");
		assert!(!tables.is_empty() && !logs.is_empty(), "{suite:?}");
		for table in &tables {
			assert!(CryptoHeader::has_magic(&fs::read(table).unwrap()), "{table:?}");
		}
		for file in tables.iter().chain(&logs).chain([&manifest]) {
			let bytes = fs::read(file).unwrap();
			assert!(!contains(&bytes, b"ev-canary") && !contains(&bytes, b"ek-00"), "{file:?}");
		}
		for log in &logs {
			assert_eq!(fs::read(log).unwrap()[4..6], [0x80, 0x01], "{log:?} is not sealed");
		}

		// Another manager with the same key opens it, and it is the same database.
		let tree = Tree::new(encrypted(&root, manager(), suite)).unwrap();
		read(&tree, &expected);
		let context = tree.core.inner.opts.encryption_context().cloned().unwrap();
		assert_eq!(context.db_id(), db_id);
		tree.close().await.unwrap();
		assert_eq!(describe_manifest_file(&manifest).unwrap().db_id, Some(db_id));
	}
}

#[tokio::test]
async fn a_suite_that_is_left_to_the_library_is_resolved_once_and_written_to_the_manifest() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let opts = encrypted(&dir.path().join("db"), manager(), CipherPreference::Auto);
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	tree.close().await.unwrap();
	let described = describe_manifest_file(opts.manifest_file_path(0)).unwrap();
	assert_eq!(described.suite, Some(CipherSuite::resolve_auto()));
}

#[tokio::test]
async fn the_suite_a_database_was_created_with_is_the_one_it_keeps() {
	if tfs::skip_listed() {
		return;
	}
	for (created, asked) in [
		(CipherSuite::XChaCha20Poly1305, CipherSuite::Aes256Gcm),
		(CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305),
	] {
		let dir = TempDir::new().unwrap();
		let opts = create(dir.path(), manager(), created).await;
		let before = files_with_extension(&opts.sstable_dir(), "sst");
		assert!(!before.is_empty());

		// Opened with a preference for the other suite: the manifest has the say, and what is
		// written afterwards is written in the suite of the database.
		let tree = Tree::new(encrypted(dir.path(), manager(), asked)).unwrap();
		let context = tree.core.inner.opts.encryption_context().cloned().unwrap();
		assert_eq!(context.suite(), created);
		write(&tree, '3', 0..KEYS).await;
		tree.flush().unwrap();
		tree.close().await.unwrap();

		let after = files_with_extension(&opts.sstable_dir(), "sst");
		assert!(after.len() > before.len(), "a table was written");
		for table in &after {
			assert_eq!(table_suite(table), created, "{table:?}");
		}
		assert_eq!(
			describe_manifest_file(opts.manifest_file_path(0)).unwrap().suite,
			Some(created)
		);
	}
}

/// A key manager that looks at the directory of the database each time a root is asked for, which
/// is when a file is sealed: whether the manifest is a sealed one by then, and how many files the
/// database has apart from it.
struct Watching {
	inner: SoftwareKeyManager,
	root: PathBuf,
	log: Mutex<Vec<(bool, usize)>>,
}

impl KeyManager for Watching {
	fn active_key_id(&self) -> u32 {
		self.inner.active_key_id()
	}

	fn key_state(&self, key_id: u32) -> KeyState {
		self.inner.key_state(key_id)
	}

	fn root_key(&self, key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
		let manifest = self.root.join("manifest").join(format!("{:020}.manifest", 0));
		let sealed = fs::read(manifest).is_ok_and(|bytes| is_sealed_manifest(&bytes));
		let others = ["sstables", "wal", "vlog"]
			.iter()
			.map(|dir| fs::read_dir(self.root.join(dir)).map_or(0, |entries| entries.count()))
			.sum();
		self.log.lock().unwrap().push((sealed, others));
		self.inner.root_key(key_id)
	}
}

#[tokio::test]
async fn no_file_is_made_under_the_identity_of_a_database_before_its_manifest_is_on_disk() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let root = dir.path().join("db");
		let watching = Arc::new(Watching {
			inner: SoftwareKeyManager::new(ROOT),
			root: root.clone(),
			log: Mutex::new(Vec::new()),
		});
		let opts = encrypted(&root, Arc::clone(&watching) as Arc<dyn KeyManager>, suite);
		let tree = Tree::new(opts).unwrap();
		write(&tree, '1', 0..KEYS).await;
		tree.flush().unwrap();
		tree.close().await.unwrap();

		// The first key that is asked for is the one that the manifest is sealed with, when
		// nothing exists. After that, whenever a key is asked for, the manifest is a sealed one,
		// which is the only way that the database can have a file: a table, a value file.
		let log = watching.log.lock().unwrap().clone();
		assert_eq!(log[0], (false, 0), "{suite:?}");
		let first_sealed = log.iter().position(|&(sealed, _)| sealed).expect("a sealed manifest");
		assert!(log[..first_sealed].iter().all(|&entry| entry == (false, 0)), "{suite:?}: {log:?}");
		assert!(log.iter().all(|&(sealed, others)| sealed || others == 0), "{suite:?}: {log:?}");
		assert!(
			log.iter().any(|&(sealed, others)| sealed && others > 0),
			"{suite:?}: no file was sealed after the manifest: {log:?}"
		);

		// The first manifest is made durable with the directory that holds it and then the one
		// above, before anything else of the database is synced.
		let canonical = root.canonicalize().unwrap();
		let synced: Vec<PathBuf> = crate::lsm::SYNCED_DIRECTORIES
			.lock()
			.iter()
			.filter(|dir| dir.starts_with(&canonical))
			.cloned()
			.collect();
		assert_eq!(synced[0], canonical.join("manifest"), "{suite:?}: {synced:?}");
		assert_eq!(synced[1], canonical, "{suite:?}: {synced:?}");
	}
}

#[tokio::test]
async fn a_creation_that_fails_at_its_manifest_leaves_no_file_under_an_identity_and_can_be_repeated(
) {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let root = dir.path().join("db");
		let opts = encrypted(&root, manager(), suite);
		let manifest = opts.manifest_file_path(0);

		fail_replacements(&manifest, ReplaceFailure::BeforeRename, 0, 1);
		let err = open_error(&opts);
		assert!(matches!(err, Error::Io(_)), "{suite:?}: {err:?}");

		// There is no manifest, no temporary file of it, and not a table, a segment of the log or
		// a value file: nothing that carries the identity that was made for the attempt.
		assert!(files_with_extension(&opts.manifest_dir(), "manifest").is_empty(), "{suite:?}");
		assert!(snapshot(&opts.manifest_dir()).is_empty(), "{suite:?}");
		for dir in [opts.sstable_dir(), opts.wal_dir(), opts.vlog_dir()] {
			assert!(snapshot(&dir).is_empty(), "{suite:?}: {dir:?}");
		}

		// Tried again, it is a new database with an identity of its own.
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		assert!(describe_manifest_file(&manifest).unwrap().encrypted);
		write(&tree, '1', 0..KEYS).await;
		read(&tree, &rounds('1', 0..KEYS));
		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_database_that_is_opened_with_the_wrong_configuration_is_refused_and_not_one_byte_changes(
) {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let root = dir.path().join("db");
		let km = manager();
		let opts = create(&root, Arc::clone(&km) as Arc<dyn KeyManager>, suite).await;
		let before = snapshot(&root);
		let described = describe_manifest_file(opts.manifest_file_path(0)).unwrap();

		// No key manager: the data is encrypted.
		let err = open_error(&Arc::new(options(&root)));
		assert_eq!(encryption_error(err, "none"), EncryptionError::NotConfigured, "{suite:?}");

		// Another root behind the same key id: the manifest does not open.
		let wrong = Arc::new(SoftwareKeyManager::new([0x43; ROOT_KEY_LEN]));
		let err = open_error(&encrypted(&root, wrong, suite));
		assert_eq!(encryption_error(err, "wrong"), EncryptionError::WrongKey, "{suite:?}");

		// The right key for another database: a context that names it, which the engine has
		// no other way to be told.
		let foreign = EncryptionContext::new(
			Arc::clone(&km) as Arc<dyn KeyManager>,
			DbId::from_bytes([0xEE; 16]),
			suite,
		);
		let foreign = Arc::new(options(&root).with_encryption_context(Arc::new(foreign)));
		let err = open_error(&foreign);
		assert_eq!(encryption_error(err, "foreign"), EncryptionError::DbIdMismatch, "{suite:?}");

		assert!(snapshot(&root) == before, "{suite:?}: a refused open changed the database");

		// The manifest is gone and the data is not: the open is refused as it always was, with
		// a key and without one, and nothing is created in its place.
		let manifest = opts.manifest_file_path(0);
		let saved = fs::read(&manifest).unwrap();
		fs::remove_file(&manifest).unwrap();
		let without_manifest = snapshot(&root);
		let keyed = encrypted(&root, Arc::clone(&km) as Arc<dyn KeyManager>, suite);
		for opts in [&keyed, &Arc::new(options(&root))] {
			let err = open_error(opts);
			assert!(matches!(err, Error::ManifestMissing(_)), "{suite:?}: {err:?}");
		}
		assert!(snapshot(&root) == without_manifest, "{suite:?}: a refusal created a manifest");
		fs::write(&manifest, &saved).unwrap();
		assert!(snapshot(&root) == before, "{suite:?}");

		// It is the database that it was: the right key opens it, and it is the same one.
		let tree = Tree::new(keyed).unwrap();
		let mut expected = rounds('1', 0..KEYS);
		expected.extend(rounds('2', 0..KEYS / 2));
		read(&tree, &expected);
		let context = tree.core.inner.opts.encryption_context().cloned().unwrap();
		assert_eq!(Some(context.db_id()), described.db_id);
		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_restore_that_did_not_finish_is_reported_before_any_key_is_looked_at() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let km = manager();
		let opts = create(dir.path(), Arc::clone(&km) as Arc<dyn KeyManager>, suite).await;
		fs::write(dir.path().join("RESTORE_IN_PROGRESS"), b"a checkpoint").unwrap();
		let before = snapshot(dir.path());

		// Whatever the key, a restore that was interrupted is what the open says, and it changes
		// no file: its files are neither the database nor the checkpoint, and the key is of no
		// use to it.
		let wrong = Arc::new(SoftwareKeyManager::new([0x43; ROOT_KEY_LEN]));
		for (opts, keyed) in [
			(Arc::clone(&opts), true),
			(encrypted(dir.path(), wrong, suite), true),
			(Arc::new(options(dir.path())), false),
		] {
			let err = open_error(&opts);
			// Where a key manager is configured the advice is not to restore into an empty
			// directory, which would be a database of its own that the checkpoint is not of.
			match &err {
				Error::RestoreInterrupted(message) => {
					assert_eq!(message.contains("copy the directory of the checkpoint"), keyed);
					assert_eq!(message.contains("into an empty directory instead"), !keyed);
				}
				other => panic!("{suite:?}: {other:?}"),
			}
			assert!(snapshot(dir.path()) == before, "{suite:?}: a refused open changed the files");
		}

		fs::remove_file(dir.path().join("RESTORE_IN_PROGRESS")).unwrap();
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_plaintext_database_is_not_opened_with_a_key_manager_and_not_one_byte_changes() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let opts = plain(dir.path());
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		write(&tree, '1', 0..KEYS).await;
		tree.flush().unwrap();
		write(&tree, '2', 0..KEYS / 2).await;
		tree.close().await.unwrap();
		let before = snapshot(dir.path());
		assert!(!is_sealed_manifest(&fs::read(opts.manifest_file_path(0)).unwrap()));

		// Encryption is chosen when a database is created: a key manager does not make this one
		// encrypted, and is refused before a table, a segment or a file of the log is read.
		let err = open_error(&encrypted(dir.path(), manager(), suite));
		assert_eq!(encryption_error(err, "plain"), EncryptionError::NotEncrypted, "{suite:?}");
		assert!(snapshot(dir.path()) == before, "{suite:?}: a refused open changed the database");

		// And it is a plaintext database still, as it was.
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let mut expected = rounds('1', 0..KEYS);
		expected.extend(rounds('2', 0..KEYS / 2));
		read(&tree, &expected);
		tree.close().await.unwrap();
		assert!(!describe_manifest_file(opts.manifest_file_path(0)).unwrap().encrypted);
	}
}

#[tokio::test]
async fn a_database_with_no_tables_and_no_value_file_is_not_opened_with_a_root_that_does_not_fit() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		// Values stay in the write-ahead log: the database has a manifest and a log, and no table
		// and no value file that a key could be tried on.
		let opts = Arc::new(
			Options {
				vlog_value_threshold: 1 << 30,
				..options(dir.path())
			}
			.with_encryption(manager(), suite),
		);
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		write(&tree, '1', 0..KEYS).await;
		tree.close().await.unwrap();
		assert!(files_with_extension(&opts.sstable_dir(), "sst").is_empty());
		assert!(files_with_extension(&opts.vlog_dir(), "vlog").is_empty());
		let before = snapshot(dir.path());

		// Another root behind the right key id. Nothing but the manifest could say that it is
		// not the key, and the open is refused by it, before the log is read and before anything
		// that the replay writes is sealed under it.
		let wrong = Arc::new(SoftwareKeyManager::new([0x43; ROOT_KEY_LEN]));
		let refused = Arc::new((*opts).clone().with_encryption(wrong, suite));
		let err = open_error(&refused);
		assert_eq!(encryption_error(err, "wrong root"), EncryptionError::WrongKey, "{suite:?}");
		assert!(snapshot(dir.path()) == before, "{suite:?}: a refused open changed the database");

		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		read(&tree, &rounds('1', 0..KEYS));
		tree.flush().unwrap();
		tree.close().await.unwrap();
		assert!(!files_with_extension(&opts.sstable_dir(), "sst").is_empty());
	}
}

/// A key manager that says which key ids it was asked to prepare.
struct Spy {
	inner: Arc<SoftwareKeyManager>,
	prepared: Mutex<Vec<Vec<u32>>>,
}

impl KeyManager for Spy {
	fn active_key_id(&self) -> u32 {
		self.inner.active_key_id()
	}

	fn key_state(&self, key_id: u32) -> KeyState {
		self.inner.key_state(key_id)
	}

	fn root_key(&self, key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
		self.inner.root_key(key_id)
	}

	fn prepare(&self, key_ids: &[u32]) -> Result<(), EncryptionError> {
		self.prepared.lock().unwrap().push(key_ids.to_vec());
		self.inner.prepare(key_ids)
	}
}

#[tokio::test]
async fn a_database_written_under_two_keys_needs_both_and_a_key_that_was_retired_is_refused() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let root = dir.path().join("db");

		// Written under key 1.
		let opts = create(&root, manager(), suite).await;
		let manifest = opts.manifest_file_path(0);
		assert_eq!(describe_manifest_file(&manifest).unwrap().key_id, Some(1));

		// Key 2 becomes the active key. Opened with both, it is read, and what is written is
		// sealed under key 2: a table, and the manifest that lists it.
		let both = manager();
		both.add_key(2, [0x22; ROOT_KEY_LEN]).unwrap();
		both.set_active_key_id(2).unwrap();
		let tree =
			Tree::new(encrypted(&root, Arc::clone(&both) as Arc<dyn KeyManager>, suite)).unwrap();
		let mut expected = rounds('1', 0..KEYS);
		expected.extend(rounds('2', 0..KEYS / 2));
		read(&tree, &expected);
		write(&tree, '3', KEYS..KEYS + 6).await;
		expected.extend(rounds('3', KEYS..KEYS + 6));
		tree.flush().unwrap();
		tree.close().await.unwrap();
		assert_eq!(describe_manifest_file(&manifest).unwrap().key_id, Some(2));
		let key_ids: Vec<u32> = files_with_extension(&opts.sstable_dir(), "sst")
			.iter()
			.map(|table| table_key_id(table))
			.collect();
		assert!(key_ids.contains(&1) && key_ids.contains(&2), "{suite:?}: {key_ids:?}");
		let before = snapshot(&root);

		// The manifest names key 2, which a manager that only has key 1 does not know.
		let only_one = manager();
		let err = open_error(&encrypted(&root, only_one, suite));
		assert_eq!(encryption_error(err, "only 1"), EncryptionError::UnknownKeyId, "{suite:?}");

		// Key 1 is retired, and a table is sealed under it: the data cannot be read, which is
		// said, and nothing is repaired or removed.
		let retired = manager();
		retired.add_key(2, [0x22; ROOT_KEY_LEN]).unwrap();
		retired.set_active_key_id(2).unwrap();
		retired.retire_key(1).unwrap();
		let err = open_error(&encrypted(&root, retired, suite));
		assert_eq!(encryption_error(err, "retired"), EncryptionError::KeyRetired, "{suite:?}");
		assert!(snapshot(&root) == before, "{suite:?}: a refused open changed the database");

		// A third key is active when the database is opened: the key of the manifest and the
		// active key are both made available, and what is written is sealed under key 3.
		let three = manager();
		three.add_key(2, [0x22; ROOT_KEY_LEN]).unwrap();
		three.add_key(3, [0x33; ROOT_KEY_LEN]).unwrap();
		three.set_active_key_id(3).unwrap();
		let spy = Arc::new(Spy {
			inner: three,
			prepared: Mutex::new(Vec::new()),
		});
		let tree =
			Tree::new(encrypted(&root, Arc::clone(&spy) as Arc<dyn KeyManager>, suite)).unwrap();
		assert!(spy.prepared.lock().unwrap().contains(&vec![2, 3]), "{:?}", spy.prepared);
		read(&tree, &expected);
		write(&tree, '4', 0..4).await;
		tree.flush().unwrap();
		tree.close().await.unwrap();
		assert_eq!(describe_manifest_file(&manifest).unwrap().key_id, Some(3));
	}
}

#[tokio::test]
async fn a_loader_that_does_not_know_encryption_stops_at_the_manifest_and_reaches_no_log() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let opts = create(dir.path(), manager(), suite).await;
		let manifest = fs::read(opts.manifest_file_path(0)).unwrap();
		let log_before = snapshot(&opts.wal_dir());
		assert!(!log_before.is_empty(), "the control: the database has a log");

		// The manifest is the first file of a database that a binary of that time reads, and what
		// it reads first of it is a version that is not this one.
		let err = frozen_old_decode(&manifest).unwrap_err();
		assert!(err.contains("Unsupported manifest format version"), "{suite:?}: {err}");
		assert_ne!(manifest[..2], [0, 1]);
		assert!(snapshot(&opts.wal_dir()) == log_before, "{suite:?}");
	}
}

#[tokio::test]
async fn opening_an_encrypted_database_never_leaves_a_copy_of_its_data_in_the_clear() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let root = dir.path().join("db");
		let km = manager();
		let opts = create(&root, Arc::clone(&km) as Arc<dyn KeyManager>, suite).await;
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		write(&tree, '3', 0..KEYS / 3).await;
		tree.flush().unwrap();
		tree.close().await.unwrap();
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		tree.close().await.unwrap();

		// A migration moves what it found into a backup directory of the database, in the clear,
		// and a database that is encrypted is never one: there is no such directory, and what is
		// in any file of the database, the write-ahead log included, is free of any marker.
		for (path, content) in snapshot(&root) {
			let name = path.file_name().unwrap().to_string_lossy().into_owned();
			assert!(!(name.starts_with('_') && name.ends_with("_backup")), "{path:?}");
			if let Some(bytes) = content {
				assert!(!contains(&bytes, b"ev-canary") && !contains(&bytes, b"ek-00"), "{path:?}");
			}
		}
	}

	// The control: the same data in a plaintext database is in the clear in the tables and the
	// value log, so the scan above means something.
	let dir = TempDir::new().unwrap();
	let opts = plain(dir.path());
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	write(&tree, '1', 0..KEYS).await;
	tree.flush().unwrap();
	tree.close().await.unwrap();
	let files: Vec<PathBuf> = files_with_extension(&opts.sstable_dir(), "sst")
		.into_iter()
		.chain(files_with_extension(&opts.vlog_dir(), "vlog"))
		.collect();
	assert!(files.iter().any(|file| contains(&fs::read(file).unwrap(), b"ev-canary")));
}

#[tokio::test]
async fn the_options_that_a_build_returns_do_not_carry_one_database_into_another() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let first = options(&dir.path().join("first")).with_encryption(manager(), suite);
		let (tree, kept) = TreeBuilder::with_options(first).build_with_options().unwrap();
		tree.close().await.unwrap();

		// What comes back is what was given, with no identity in it.
		assert!(kept.encryption_context().is_none(), "{suite:?}");
		let one = describe_manifest_file(kept.manifest_file_path(0)).unwrap().db_id.unwrap();

		// Used again for another directory, it makes another database, with an identity of its own.
		let second = (*kept).clone().with_path(dir.path().join("second"));
		let second_manifest = second.manifest_file_path(0);
		let (tree, _) = TreeBuilder::with_options(second).build_with_options().unwrap();
		tree.close().await.unwrap();
		let two = describe_manifest_file(second_manifest).unwrap().db_id.unwrap();
		assert_ne!(one, two, "{suite:?}");
	}
}

#[tokio::test]
async fn an_open_that_the_value_log_refuses_makes_no_segment_of_the_log_and_changes_no_file() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let root = dir.path().join("db");
		let opts = Arc::new(
			Options {
				flush_on_close: true,
				..options(&root)
			}
			.with_encryption(manager(), suite),
		);
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		write(&tree, '1', 0..KEYS).await;
		tree.close().await.unwrap();

		// The close flushed everything into tables. A segment of the write-ahead log that is left
		// is removed, so that an open has to make one, which an open that is refused must not.
		for segment in files_with_extension(&opts.wal_dir(), "wal") {
			fs::remove_file(segment).unwrap();
		}

		// A file of the value log in the clear, taken from a database that is not encrypted, is
		// one that an encrypted database refuses. Its id is a newer one than any the log has.
		let plain_dir = TempDir::new().unwrap();
		let plain_opts = plain(plain_dir.path());
		let tree = Tree::new(Arc::clone(&plain_opts)).unwrap();
		write(&tree, '1', 0..KEYS).await;
		tree.close().await.unwrap();
		let in_the_clear = files_with_extension(&plain_opts.vlog_dir(), "vlog");
		assert!(!in_the_clear.is_empty(), "{suite:?}");
		fs::copy(&in_the_clear[0], opts.vlog_file_path(99)).unwrap();

		let before = snapshot(&root);
		let err = open_error(&opts);
		assert_eq!(
			encryption_error(err, "a file in the clear"),
			EncryptionError::PlaintextInEncryptedDb,
			"{suite:?}"
		);
		assert!(snapshot(&root) == before, "{suite:?}: a refused open changed the database");
		assert!(files_with_extension(&opts.wal_dir(), "wal").is_empty(), "{suite:?}");
	}
}
