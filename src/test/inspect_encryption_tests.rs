//! The inspection of encrypted files and databases with a key, and the refusal without one.
//!
//! Databases here are written by a real tree and read by the functions of `inspect` that a tool
//! calls, so what is checked is the whole way from a path and a key manager to what is said.
//! Every refusal is an error of a kind that a caller can match on, and none of them writes a file:
//! the directory of a database is compared byte for byte before and after.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tempfile::TempDir;

use super::encryption_open_tests::{
	create,
	encryption_error,
	files_with_extension,
	manager,
	plain,
	snapshot,
	write,
	KEYS,
	ROOT,
	SUITES,
};
use super::tfs;
use crate::error::{EncryptionError, KeyProviderFailure};
use crate::inspect::{
	dump_manifest,
	dump_manifest_with_options,
	dump_sst,
	dump_sst_with_options,
	dump_wal,
	dump_wal_with_options,
	inspect_db,
	inspect_db_with_options,
};
use crate::tde::{
	describe_manifest_file,
	CipherSuite,
	DbId,
	EncryptionContext,
	KeyManager,
	KeyState,
	SoftwareKeyManager,
	Zeroizing,
	ROOT_KEY_LEN,
};
use crate::{Error, Options, Tree};

/// Options that carry the key manager of the database that `create` makes.
fn keyed(suite: CipherSuite) -> Options {
	Options::new().with_encryption(manager(), suite)
}

/// A key manager whose only key is not the one that the databases here were made with.
fn another_manager() -> Arc<SoftwareKeyManager> {
	Arc::new(SoftwareKeyManager::new([0x43; ROOT_KEY_LEN]))
}

/// A key manager that still has the key of the databases here, but has retired it, and has
/// another one active.
fn retired_manager() -> Arc<SoftwareKeyManager> {
	let km = SoftwareKeyManager::new(ROOT);
	km.add_key(2, [0x44; ROOT_KEY_LEN]).unwrap();
	km.set_active_key_id(2).unwrap();
	km.retire_key(1).unwrap();
	Arc::new(km)
}

fn id_of(path: &Path) -> u64 {
	path.file_name().unwrap().to_str().unwrap()[..20].parse().unwrap()
}

fn tables_of(root: &Path) -> Vec<PathBuf> {
	let opts = Options {
		path: root.to_path_buf(),
		..Default::default()
	};
	let tables = files_with_extension(&opts.sstable_dir(), "sst");
	assert!(!tables.is_empty(), "the database has no table");
	tables
}

fn segments_of(root: &Path) -> Vec<PathBuf> {
	let opts = Options {
		path: root.to_path_buf(),
		..Default::default()
	};
	let segments = files_with_extension(&opts.wal_dir(), "wal");
	assert!(!segments.is_empty(), "the database has no log segment");
	segments
}

fn manifest_of(root: &Path) -> PathBuf {
	Options {
		path: root.to_path_buf(),
		..Default::default()
	}
	.manifest_file_path(0)
}

/// What an error says, which for each of these refusals is an encryption error.
fn refusal<T: std::fmt::Debug>(result: crate::Result<T>, what: &str) -> EncryptionError {
	match result {
		Ok(found) => panic!("{what}: nothing was refused: {found:?}"),
		Err(err) => encryption_error(err, what),
	}
}

/// A database in the clear that holds the same data as the one `create` makes.
async fn plain_database(root: &Path) {
	let tree = Tree::new(plain(root)).unwrap();
	write(&tree, '1', 0..KEYS).await;
	tree.flush().unwrap();
	write(&tree, '2', 0..KEYS / 2).await;
	tree.close().await.unwrap();
}

#[tokio::test]
async fn a_sealed_table_is_refused_without_a_key_and_dumped_with_it() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		create(dir.path(), manager(), suite).await;
		for table in tables_of(dir.path()) {
			let before = snapshot(dir.path());

			// Nothing here has a key, and the answer is the typed error, in words that say what is
			// the matter and none that name a key.
			let err = dump_sst(&table, false, false).unwrap_err();
			assert!(matches!(err, Error::Encryption(EncryptionError::NotConfigured)), "{err:?}");
			assert!(err.to_string().contains("no key manager"), "{err}");

			let info = dump_sst_with_options(&table, true, true, &keyed(suite)).unwrap();
			let id = id_of(&table);
			assert_eq!(info.table_id, Some(id));
			assert_eq!(info.checksum_type, 2, "the footer says that the blocks are sealed");
			let header = info.encryption.expect("a sealed table says so");
			assert_eq!((header.suite, header.key_id, header.file_id), (suite, 1, id));
			assert!(info.total_entries > 0);
			let keys = info.keys.expect("the keys were asked for");
			assert!(!keys.is_empty());
			assert!(keys.iter().all(|entry| entry.user_key.starts_with(b"ek-")), "{keys:?}");
			assert!(info.verified_blocks.is_some_and(|blocks| blocks > 0));
			assert_eq!(snapshot(dir.path()), before, "an inspection writes nothing");
		}
	}
}

#[tokio::test]
async fn a_key_that_does_not_fit_is_refused_by_name_and_changes_nothing() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		create(dir.path(), manager(), suite).await;
		let before = snapshot(dir.path());

		let wrong = Options::new().with_encryption(another_manager(), suite);
		let retired = Options::new().with_encryption(retired_manager(), suite);
		for table in tables_of(dir.path()) {
			let err = refusal(dump_sst_with_options(&table, false, false, &wrong), "wrong root");
			assert_eq!(err, EncryptionError::WrongKey, "{suite:?}");
			let err = refusal(dump_sst_with_options(&table, false, false, &retired), "retired");
			assert_eq!(err, EncryptionError::KeyRetired, "{suite:?}");
		}
		for (options, expected, what) in [
			(&wrong, EncryptionError::WrongKey, "wrong root"),
			(&retired, EncryptionError::KeyRetired, "retired key"),
		] {
			let found = refusal(inspect_db_with_options(dir.path(), options), what);
			assert_eq!(found, expected, "inspect: {what}");
			let found = refusal(dump_manifest_with_options(dir.path(), options), what);
			assert_eq!(found, expected, "manifest: {what}");
			let found = refusal(dump_wal_with_options(dir.path(), options), what);
			assert_eq!(found, expected, "log: {what}");
		}
		assert_eq!(snapshot(dir.path()), before, "{suite:?}");
	}
}

#[tokio::test]
async fn a_sealed_table_is_read_as_the_table_that_its_name_says() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	create(&dir.path().join("db"), manager(), CipherSuite::XChaCha20Poly1305).await;
	let table = tables_of(&dir.path().join("db")).remove(0);
	let id = id_of(&table);
	let options = keyed(CipherSuite::XChaCha20Poly1305);

	// The same bytes under the name of another table are not that table.
	let renamed = dir.path().join(format!("{:020}.sst", id + 1000));
	fs::copy(&table, &renamed).unwrap();
	let err = refusal(dump_sst_with_options(&renamed, false, false, &options), "renamed");
	assert_eq!(err, EncryptionError::FileIdMismatch);

	// A name that says no table at all leaves nothing to read it as, and the refusal says so.
	let unnamed = dir.path().join("copy.sst");
	fs::copy(&table, &unnamed).unwrap();
	let err = dump_sst_with_options(&unnamed, false, false, &options).unwrap_err();
	assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");
	assert!(err.to_string().contains("cannot tell which table"), "{err}");
	// Without a key the name does not matter: the footer refuses first.
	let err = dump_sst(&unnamed, false, false).unwrap_err();
	assert!(matches!(err, Error::Encryption(EncryptionError::NotConfigured)), "{err:?}");
}

#[tokio::test]
async fn a_database_that_the_caller_expects_is_the_one_a_table_must_belong_to() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	create(dir.path(), manager(), CipherSuite::Aes256Gcm).await;
	let table = tables_of(dir.path()).remove(0);
	let db_id = describe_manifest_file(manifest_of(dir.path())).unwrap().db_id.unwrap();

	let expecting = |db: DbId| {
		let km: Arc<dyn KeyManager> = manager();
		Options::new().with_encryption_context(Arc::new(EncryptionContext::new(
			km,
			db,
			CipherSuite::Aes256Gcm,
		)))
	};
	let err = refusal(
		dump_sst_with_options(&table, false, false, &expecting(DbId::from_bytes([0x11; 16]))),
		"another database",
	);
	assert_eq!(err, EncryptionError::DbIdMismatch);
	let info = dump_sst_with_options(&table, false, false, &expecting(db_id)).unwrap();
	assert_eq!(info.table_id, Some(id_of(&table)));
}

#[tokio::test]
async fn a_database_in_the_clear_is_inspected_as_before_and_refused_when_a_key_is_given() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	plain_database(dir.path()).await;
	let before = snapshot(dir.path());

	let info = inspect_db(dir.path()).unwrap();
	assert!(info.total_tables >= 1 && info.encryption.is_none());
	assert!(dump_manifest(dir.path()).unwrap().levels.iter().any(|level| !level.tables.is_empty()));
	let log = dump_wal(dir.path()).unwrap();
	assert!(!log.segment_files.is_empty());
	assert!(log.segment_files.iter().all(|segment| {
		segment.encryption.is_none() && segment.batches.is_none() && !segment.torn
	}));
	for table in tables_of(dir.path()) {
		let info = dump_sst(&table, true, true).unwrap();
		assert_eq!((info.table_id, info.checksum_type), (Some(id_of(&table)), 1));
		assert!(info.encryption.is_none());
	}

	// A key for a database that is not encrypted: encryption is never added to one that exists.
	let options = keyed(CipherSuite::Aes256Gcm);
	let not_encrypted = EncryptionError::NotEncrypted;
	assert_eq!(refusal(inspect_db_with_options(dir.path(), &options), "inspect"), not_encrypted);
	assert_eq!(
		refusal(dump_manifest_with_options(dir.path(), &options), "manifest"),
		not_encrypted
	);
	assert_eq!(refusal(dump_wal_with_options(dir.path(), &options), "log"), not_encrypted);
	for table in tables_of(dir.path()) {
		let found = refusal(dump_sst_with_options(&table, false, false, &options), "table");
		assert_eq!(found, not_encrypted);
	}
	assert_eq!(snapshot(dir.path()), before);
}

#[tokio::test]
async fn the_manifest_and_the_state_of_an_encrypted_database_are_read_with_its_key() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		create(dir.path(), manager(), suite).await;
		let before = snapshot(dir.path());

		// Without a key, both are refused before they say anything.
		for found in [
			refusal(inspect_db(dir.path()), "inspect"),
			refusal(dump_manifest(dir.path()), "manifest"),
		] {
			assert_eq!(found, EncryptionError::NotConfigured, "{suite:?}");
		}

		let options = keyed(suite);
		let state = inspect_db_with_options(dir.path(), &options).unwrap();
		let header = state.encryption.expect("the manifest says that it is encrypted");
		assert_eq!((header.suite, header.key_id, header.file_id), (suite, 1, 0));
		assert_eq!(state.total_tables, tables_of(dir.path()).len());

		// The manifest is read from the database, and from the file of the manifest.
		let mut listed = Vec::new();
		for path in [dir.path().to_path_buf(), manifest_of(dir.path())] {
			let manifest = dump_manifest_with_options(&path, &options).unwrap();
			listed = manifest
				.levels
				.iter()
				.flat_map(|level| level.tables.iter().map(|table| table.id))
				.collect::<Vec<_>>();
			listed.sort_unstable();
			let mut on_disk: Vec<u64> = tables_of(dir.path()).iter().map(|t| id_of(t)).collect();
			on_disk.sort_unstable();
			assert_eq!(listed, on_disk, "{suite:?}: {}", path.display());
		}
		assert!(!listed.is_empty());
		assert_eq!(snapshot(dir.path()), before, "{suite:?}");
	}
}

#[tokio::test]
async fn the_log_of_an_encrypted_database_is_refused_without_a_key_and_decoded_with_it() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		create(dir.path(), manager(), suite).await;
		let before = snapshot(dir.path());
		let options = keyed(suite);

		for path in [dir.path().to_path_buf(), dir.path().join("wal")] {
			let err = refusal(dump_wal(&path), "no key");
			assert_eq!(err, EncryptionError::NotConfigured, "{suite:?}");

			let log = dump_wal_with_options(&path, &options).unwrap();
			assert_eq!(log.segment_files.len(), segments_of(dir.path()).len(), "{suite:?}");
			let mut batches = 0;
			for segment in &log.segment_files {
				let header = segment.encryption.expect("a sealed segment says so");
				assert_eq!((header.suite, header.key_id), (suite, 1));
				assert!(!segment.torn, "{}", segment.file_name);
				batches += segment.batches.expect("a segment that was opened was counted");
			}
			// The commit that follows the last flush is in the log, and nothing else is.
			assert!(batches >= 1, "{suite:?}: the log holds no batch");
			assert!(log.total_bytes > 0);
		}
		assert_eq!(snapshot(dir.path()), before, "{suite:?}");
	}
}

#[tokio::test]
async fn a_segment_that_is_cut_short_is_reported_torn_and_is_left_as_it_is() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	create(dir.path(), manager(), CipherSuite::XChaCha20Poly1305).await;

	// The segment that holds the last commit is the longest: the others hold only a header.
	let segments = segments_of(dir.path());
	let longest = segments.iter().max_by_key(|path| fs::metadata(path).unwrap().len()).unwrap();
	let len = fs::metadata(longest).unwrap().len();
	fs::OpenOptions::new().write(true).open(longest).unwrap().set_len(len - 3).unwrap();
	let before = snapshot(dir.path());

	let options = keyed(CipherSuite::XChaCha20Poly1305);
	let log = dump_wal_with_options(dir.path(), &options).unwrap();
	let torn: Vec<&str> = log
		.segment_files
		.iter()
		.filter(|segment| segment.torn)
		.map(|segment| segment.file_name.as_str())
		.collect();
	assert_eq!(torn, [longest.file_name().unwrap().to_str().unwrap()]);
	assert_eq!(snapshot(dir.path()), before, "a dump repairs nothing");

	// Without a key it is still not said to be anything: it is encrypted, and that is refused.
	assert_eq!(refusal(dump_wal(dir.path()), "no key"), EncryptionError::NotConfigured);
}

#[tokio::test]
async fn a_log_that_is_alone_takes_its_database_from_its_first_segment() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let ours = dir.path().join("ours");
	let theirs = dir.path().join("theirs");
	create(&ours, manager(), CipherSuite::Aes256Gcm).await;
	create(&theirs, manager(), CipherSuite::Aes256Gcm).await;

	// A directory of segments with no manifest next to it, from the one database.
	let alone = dir.path().join("alone");
	fs::create_dir_all(&alone).unwrap();
	for segment in segments_of(&ours) {
		fs::copy(&segment, alone.join(segment.file_name().unwrap())).unwrap();
	}
	let options = keyed(CipherSuite::Aes256Gcm);
	let log = dump_wal_with_options(&alone, &options).unwrap();
	assert!(log.segment_files.iter().all(|segment| segment.batches.is_some()));

	// And a segment of another database in it is that, named: the first segment says which
	// database the log is of, and this one is not.
	let stranger = segments_of(&theirs).remove(0);
	fs::copy(&stranger, alone.join(format!("{:020}.wal", 9_999))).unwrap();
	let err = refusal(dump_wal_with_options(&alone, &options), "a segment of another database");
	assert_eq!(err, EncryptionError::DbIdMismatch);
}

/// A key manager in the shape of one that is backed by a provider: `root_key` answers only for the
/// ids that `prepare` fetched, and fails as a provider that is away does for the others.
struct Provider {
	inner: SoftwareKeyManager,
	prepared: Mutex<Vec<u32>>,
}

impl KeyManager for Provider {
	fn active_key_id(&self) -> u32 {
		self.inner.active_key_id()
	}

	fn key_state(&self, key_id: u32) -> KeyState {
		self.inner.key_state(key_id)
	}

	fn root_key(&self, key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
		if self.prepared.lock().unwrap().contains(&key_id) {
			self.inner.root_key(key_id)
		} else {
			Err(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable))
		}
	}

	fn prepare(&self, key_ids: &[u32]) -> Result<(), EncryptionError> {
		self.inner.prepare(key_ids)?;
		let mut prepared = self.prepared.lock().unwrap();
		for &key_id in key_ids {
			if !prepared.contains(&key_id) {
				prepared.push(key_id);
			}
		}
		Ok(())
	}
}

#[tokio::test]
async fn a_key_that_a_provider_has_to_fetch_is_asked_for_before_a_table_is_opened() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		create(dir.path(), manager(), suite).await;
		let provider = Arc::new(Provider {
			inner: SoftwareKeyManager::new(ROOT),
			prepared: Mutex::new(Vec::new()),
		});
		let key_manager: Arc<dyn KeyManager> = Arc::<Provider>::clone(&provider);
		let options = Options::new().with_encryption(key_manager, suite);

		// A table that is looked at alone has nothing that fetched its key, so the tool does.
		for table in tables_of(dir.path()) {
			let info = dump_sst_with_options(&table, false, true, &options).unwrap();
			assert!(info.verified_blocks.is_some_and(|blocks| blocks > 0), "{suite:?}");
		}
		assert_eq!(*provider.prepared.lock().unwrap(), vec![1], "{suite:?}");
	}
}
