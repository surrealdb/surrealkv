//! A database that has an encryption context: the tables that its flushes, its compactions and its
//! writes straight to level zero make are sealed, a reopen reads them with the key, and a
//! database is never opened with a key that is not its own.
//!
//! What is checked of the files is the files of the tables, and what is checked of a refused
//! open is that it changed nothing. The manifest and the value log of these databases are sealed
//! too, and the write-ahead log is not yet.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;

use crate::compaction::leveled::Strategy;
use crate::error::{EncryptionError, KeyProviderFailure};
use crate::sstable::block_io::{BlockKind, TrailerLayout};
use crate::sstable::table::{ChecksumType, Footer, TABLE_FULL_FOOTER_LENGTH};
use crate::tde::{CipherSuite, CryptoHeader, DbId, EncryptionContext, SoftwareKeyManager};
use crate::test::sstable_encryption_tests::{first_of, flip, walk, FlakyManager};
use crate::{Error, Options, Tree};

const SUITES: [CipherSuite; 2] = [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];
const KEYS: usize = 60;
const DEFAULT_MEMTABLE: usize = 16 * 1024 * 1024;

fn key(i: usize) -> Vec<u8> {
	format!("tk-{i:04}").into_bytes()
}

/// The value of key `i` as written in `round`. Every value holds "tv-canary".
fn value(round: char, i: usize) -> Vec<u8> {
	format!("tv-canary-{round}-{i:04}-{}", "x".repeat(24)).into_bytes()
}

fn context(suite: CipherSuite) -> Arc<EncryptionContext> {
	let key_manager = Arc::new(SoftwareKeyManager::new([0x42; 32]));
	Arc::new(EncryptionContext::new(key_manager, DbId::from_bytes([0xD5; 16]), suite))
}

/// Options for a database in `dir`, whose tables are sealed if a context is given, and which
/// compacts only when a test says so: the number of tables of level zero that makes a compaction
/// start by itself is out of reach, so that no background task removes a table that a test is
/// about to read.
fn options(
	dir: &Path,
	context: Option<&Arc<EncryptionContext>>,
	max_memtable_size: usize,
) -> Arc<Options> {
	let opts = Options {
		path: dir.to_path_buf(),
		flush_on_close: false,
		max_memtable_size,
		memtable_stall_threshold: 1000,
		level0_max_files: 100,
		l0_stall_threshold: 1000,
		..Default::default()
	};
	Arc::new(match context {
		Some(context) => opts.with_encryption_context(Arc::clone(context)),
		None => opts,
	})
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
	haystack.windows(needle.len()).any(|window| window == needle)
}

/// Commits the keys in `from..to` with the values of `round` and flushes them to a table.
async fn write_round(tree: &Tree, round: char, from: usize, to: usize) {
	let mut txn = tree.begin().unwrap();
	for i in from..to {
		txn.set(key(i), value(round, i)).unwrap();
	}
	txn.commit().await.unwrap();
	tree.flush().unwrap();
}

/// Reads every key: the first half was written again in round 2.
fn read_every_key(tree: &Tree) {
	let txn = tree.begin().unwrap();
	for i in 0..KEYS {
		let round = if i < KEYS / 2 {
			'2'
		} else {
			'1'
		};
		let found = txn.get(key(i).as_slice()).unwrap().expect("the key is in the database");
		assert_eq!(found, value(round, i), "key {i}");
	}
}

fn table_ids(tree: &Tree) -> Vec<u64> {
	let mut ids: Vec<u64> =
		tree.core.inner.level_manifest.read().unwrap().iter().map(|t| t.id).collect();
	ids.sort_unstable();
	ids
}

/// The files of the tables that the manifest of `tree` lists.
fn table_files(tree: &Tree, opts: &Options) -> Vec<PathBuf> {
	table_ids(tree).into_iter().map(|id| opts.sstable_file_path(id)).collect()
}

/// A table file that is sealed: it begins with the header of an encrypted file, its footer says
/// that its blocks are sealed, and nothing of what was written to it can be read in it.
fn assert_sealed(path: &Path) {
	let bytes = fs::read(path).unwrap();
	assert!(CryptoHeader::has_magic(&bytes), "{} has no header", path.display());
	let footer = Footer::decode(&bytes[bytes.len() - TABLE_FULL_FOOTER_LENGTH..]).unwrap();
	assert_eq!(footer.checksum, ChecksumType::SealedCRC32c, "{}", path.display());
	for plain in ["tv-canary", "tk-00", "direct-canary"] {
		assert!(!contains(&bytes, plain.as_bytes()), "{} holds {plain}", path.display());
	}
}

/// Every file of the directories of the database, with its bytes.
fn snapshot(opts: &Options) -> BTreeMap<PathBuf, Vec<u8>> {
	fn visit(dir: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
		let Ok(entries) = fs::read_dir(dir) else {
			return;
		};
		for entry in entries.flatten() {
			let path = entry.path();
			if path.is_dir() {
				visit(&path, files);
			} else {
				files.insert(path.clone(), fs::read(&path).unwrap());
			}
		}
	}
	let mut files = BTreeMap::new();
	for dir in [opts.sstable_dir(), opts.wal_dir(), opts.manifest_dir(), opts.vlog_dir()] {
		visit(&dir, &mut files);
	}
	files
}

/// The error that opening the database with `opts` fails with.
fn open_error(opts: &Arc<Options>) -> Error {
	match Tree::new(Arc::clone(opts)) {
		Ok(_) => panic!("the database was opened"),
		Err(err) => err,
	}
}

#[tokio::test]
async fn the_tables_that_a_flush_and_a_compaction_write_are_sealed_and_read_back_after_a_reopen() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite);
		let opts = options(dir.path(), Some(&ctx), DEFAULT_MEMTABLE);
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		write_round(&tree, '1', 0, KEYS).await;
		write_round(&tree, '2', 0, KEYS / 2).await;
		read_every_key(&tree);
		let flushed = table_ids(&tree);
		assert!(!flushed.is_empty());
		for path in table_files(&tree, &opts) {
			assert_sealed(&path);
		}

		// A compaction merges the tables into new ones, which are sealed as well. The strategy is
		// told that two tables are enough, and the tables are written with the options of the
		// database.
		let eager = Arc::new(Options {
			level0_max_files: 2,
			..(*opts).clone()
		});
		tree.compact(Arc::new(Strategy::from_options(eager))).unwrap();
		read_every_key(&tree);
		let level_one = tree
			.core
			.inner
			.level_manifest
			.read()
			.unwrap()
			.levels
			.get_levels()
			.get(1)
			.map_or(0, |level| level.tables.len());
		assert!(
			table_ids(&tree) != flushed && level_one > 0,
			"{suite:?}: the compaction wrote no table"
		);
		for path in table_files(&tree, &opts) {
			assert_sealed(&path);
		}
		tree.close().await.unwrap();

		// Every table is opened again from its file, with the key.
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		read_every_key(&tree);
		for path in table_files(&tree, &opts) {
			assert_sealed(&path);
		}
		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_key_that_cannot_be_had_is_the_error_of_the_flush_and_the_flush_can_be_tried_again() {
	let dir = TempDir::new().unwrap();
	let key_manager = FlakyManager::new();
	let ctx = Arc::new(EncryptionContext::new(
		Arc::<FlakyManager>::clone(&key_manager),
		DbId::from_bytes([0xD5; 16]),
		CipherSuite::Aes256Gcm,
	));
	let opts = options(dir.path(), Some(&ctx), DEFAULT_MEMTABLE);
	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	let mut txn = tree.begin().unwrap();
	for i in 0..KEYS {
		txn.set(key(i), value('1', i)).unwrap();
	}
	txn.commit().await.unwrap();
	let check = |tree: &Tree| {
		let txn = tree.begin().unwrap();
		for i in 0..KEYS {
			assert_eq!(txn.get(key(i).as_slice()).unwrap(), Some(value('1', i)), "key {i}");
		}
	};

	// The provider is away: the flush fails with that error, as it is, and not with a text that
	// says that a write failed. The data is where it was, and no table was registered.
	key_manager.set_unavailable(true);
	let err = tree.flush().unwrap_err();
	assert!(
		matches!(
			&err,
			Error::Encryption(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable))
		),
		"{err:?}"
	);
	assert!(err.to_string().contains("unavailable"), "{err}");
	assert!(table_ids(&tree).is_empty());
	check(&tree);

	// It is back, and the same flush is tried again: the table is written, sealed, and read.
	key_manager.set_unavailable(false);
	tree.flush().unwrap();
	let files = table_files(&tree, &opts);
	assert_eq!(files.len(), 1);
	assert_sealed(&files[0]);
	check(&tree);
	tree.close().await.unwrap();
}

#[tokio::test]
async fn a_batch_that_goes_straight_to_level_zero_is_sealed_too() {
	const COUNT: usize = 250;
	let direct_value = |i: usize| format!("direct-canary-{i:06}-{}", "y".repeat(380)).into_bytes();

	let dir = TempDir::new().unwrap();
	let ctx = context(CipherSuite::XChaCha20Poly1305);
	let opts = options(dir.path(), Some(&ctx), 64 * 1024);
	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// A batch that does not fit in a memtable is written as a table of its own.
	let mut txn = tree.begin().unwrap();
	for i in 0..COUNT {
		txn.set(key(i), direct_value(i)).unwrap();
	}
	txn.commit().await.unwrap();

	let files = table_files(&tree, &opts);
	assert!(!files.is_empty(), "the batch was not written as a table");
	for path in &files {
		assert_sealed(path);
	}
	let check = |tree: &Tree| {
		let txn = tree.begin().unwrap();
		for i in 0..COUNT {
			assert_eq!(txn.get(key(i).as_slice()).unwrap(), Some(direct_value(i)), "key {i}");
		}
	};
	check(&tree);
	tree.close().await.unwrap();

	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	check(&tree);
	tree.close().await.unwrap();
}

#[tokio::test]
async fn a_database_is_not_opened_with_a_key_that_is_not_its_own_and_nothing_is_changed() {
	let dir = TempDir::new().unwrap();
	let ctx = context(CipherSuite::Aes256Gcm);
	let opts = options(dir.path(), Some(&ctx), DEFAULT_MEMTABLE);
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	write_round(&tree, '1', 0, KEYS).await;
	write_round(&tree, '2', 0, KEYS / 2).await;
	tree.close().await.unwrap();
	let before = snapshot(&opts);
	assert!(before.keys().any(|path| path.extension().is_some_and(|ext| ext == "sst")));

	// Another key behind the same key id.
	let other_key = Arc::new(EncryptionContext::new(
		Arc::new(SoftwareKeyManager::new([0x99; 32])),
		DbId::from_bytes([0xD5; 16]),
		CipherSuite::Aes256Gcm,
	));
	let err = open_error(&options(dir.path(), Some(&other_key), DEFAULT_MEMTABLE));
	assert!(matches!(err, Error::Encryption(EncryptionError::WrongKey)), "{err:?}");

	// The right key, in another database.
	let other_db = Arc::new(EncryptionContext::new(
		Arc::new(SoftwareKeyManager::new([0x42; 32])),
		DbId::from_bytes([0xD6; 16]),
		CipherSuite::Aes256Gcm,
	));
	let err = open_error(&options(dir.path(), Some(&other_db), DEFAULT_MEMTABLE));
	assert!(matches!(err, Error::Encryption(EncryptionError::DbIdMismatch)), "{err:?}");

	// No key at all.
	let err = open_error(&options(dir.path(), None, DEFAULT_MEMTABLE));
	assert!(matches!(err, Error::Encryption(EncryptionError::NotConfigured)), "{err:?}");

	// Not a table, not a segment of the log and not the manifest was written, repaired or
	// removed by any of those.
	assert!(snapshot(&opts) == before, "a refused open changed the files of the database");

	// And the database is as it was: the right key opens it.
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	read_every_key(&tree);
	tree.close().await.unwrap();
}

#[tokio::test]
async fn a_plain_database_is_not_opened_as_an_encrypted_one_and_nothing_is_changed() {
	let dir = TempDir::new().unwrap();
	let plain = options(dir.path(), None, DEFAULT_MEMTABLE);
	let tree = Tree::new(Arc::clone(&plain)).unwrap();
	write_round(&tree, '1', 0, KEYS).await;
	write_round(&tree, '2', 0, KEYS / 2).await;
	tree.close().await.unwrap();
	let before = snapshot(&plain);
	for path in before.keys().filter(|path| path.extension().is_some_and(|ext| ext == "sst")) {
		let bytes = &before[path];
		assert!(!CryptoHeader::has_magic(bytes));
		assert!(contains(bytes, b"tv-canary"), "the control: a plain table holds its values");
	}

	// The manifest says that the database is not encrypted, which is found before any table is
	// read: encryption is chosen when a database is created and is never added to one.
	for suite in SUITES {
		let encrypted = options(dir.path(), Some(&context(suite)), DEFAULT_MEMTABLE);
		let err = open_error(&encrypted);
		assert!(
			matches!(err, Error::Encryption(EncryptionError::NotEncrypted)),
			"{suite:?}: {err:?}"
		);
	}
	assert!(snapshot(&plain) == before, "a refused open changed the files of the database");

	let tree = Tree::new(Arc::clone(&plain)).unwrap();
	read_every_key(&tree);
	tree.close().await.unwrap();
}

// ========== Neighbours of the tables ==========

#[tokio::test]
async fn a_foreign_database_is_not_migrated_into_an_encrypted_one() {
	use surrealkv_compat_v1::{V1_FOOTER_TOTAL_LEN, V1_MAGIC_FOOTER};

	let ctx = context(CipherSuite::Aes256Gcm);
	let key_manager: Arc<dyn crate::KeyManager> = Arc::new(SoftwareKeyManager::new([0x42; 32]));

	// The directory of each store that is migrated, with what the migration would move.
	let rocksdb = TempDir::new().unwrap();
	fs::write(rocksdb.path().join("CURRENT"), b"MANIFEST-000001\n").unwrap();
	fs::write(rocksdb.path().join("MANIFEST-000001"), b"not a real rocksdb manifest").unwrap();

	let v1 = TempDir::new().unwrap();
	let mut table = vec![0u8; V1_FOOTER_TOTAL_LEN];
	table[V1_FOOTER_TOTAL_LEN - V1_MAGIC_FOOTER.len()..].copy_from_slice(&V1_MAGIC_FOOTER);
	fs::write(v1.path().join("000001.sst"), &table).unwrap();

	let indxdb = TempDir::new().unwrap();
	surrealkv_compat_indxdb::export_dump(
		indxdb.path().join("indxdb_dump.bin"),
		&[(b"user:001".to_vec(), b"Alice".to_vec())],
	)
	.unwrap();

	let cases = [
		(&rocksdb, "CURRENT", "_rocksdb_backup", "RocksDB"),
		(&v1, "000001.sst", "_v1_backup", "SurrealKV v1"),
		(&indxdb, "indxdb_dump.bin", "_indxdb_backup", "IndexedDB"),
	];
	for (dir, original, backup, store) in cases {
		// Configured with a resolved context, and with a key manager alone.
		let with_context = options(dir.path(), Some(&ctx), DEFAULT_MEMTABLE);
		let with_key_manager = Arc::new(
			Options {
				path: dir.path().to_path_buf(),
				..Default::default()
			}
			.with_encryption(Arc::clone(&key_manager), CipherSuite::Aes256Gcm),
		);
		for opts in [with_context, with_key_manager] {
			let err = open_error(&opts);
			assert!(
				matches!(&err, Error::InvalidArgument(message)
					if message.contains(store) && message.contains("encrypted")),
				"{store}: {err:?}"
			);
			// Nothing was read, moved or created.
			assert!(dir.path().join(original).exists(), "{store}: {original} was moved");
			assert!(!dir.path().join(backup).exists(), "{store}: a backup was made");
			assert!(!opts.sstable_dir().exists(), "{store}: the database was created");
			assert!(!opts.manifest_dir().exists(), "{store}: the database was created");
		}
	}
}

/// The names of the files of `dir` with their bytes.
fn files_of(dir: &Path) -> BTreeMap<String, Vec<u8>> {
	fs::read_dir(dir)
		.unwrap()
		.flatten()
		.map(|entry| {
			(entry.file_name().to_string_lossy().into_owned(), fs::read(entry.path()).unwrap())
		})
		.collect()
}

#[tokio::test]
async fn a_checkpoint_of_an_encrypted_database_holds_its_sealed_tables_and_restores_with_the_key() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let checkpoints = TempDir::new().unwrap();
		let checkpoint = checkpoints.path().join("checkpoint");
		let ctx = context(suite);
		let opts = options(dir.path(), Some(&ctx), DEFAULT_MEMTABLE);
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		write_round(&tree, '1', 0, KEYS).await;
		write_round(&tree, '2', 0, KEYS / 2).await;
		tree.create_checkpoint(&checkpoint).unwrap();

		// The tables are copied as they are: the same bytes, sealed under the same keys.
		let copied = files_of(&checkpoint.join("sstables"));
		assert!(!copied.is_empty());
		for path in table_files(&tree, &opts) {
			let name = path.file_name().unwrap().to_string_lossy().into_owned();
			assert!(copied[&name] == fs::read(&path).unwrap(), "{suite:?}: {name}");
			assert_sealed(&checkpoint.join("sstables").join(&name));
		}

		// Restoring it into the database that it was taken from brings back what was there.
		write_round(&tree, '3', 0, KEYS).await;
		tree.restore_from_checkpoint(&checkpoint).unwrap();
		read_every_key(&tree);
		for path in table_files(&tree, &opts) {
			assert_sealed(&path);
		}
		tree.close().await.unwrap();

		// The checkpoint is a database that opens with the key. It is opened after the restore
		// has read it, since opening a database may write in its directory.
		let copy_opts = options(&checkpoint, Some(&ctx), DEFAULT_MEMTABLE);
		let copy = Tree::new(Arc::clone(&copy_opts)).unwrap();
		read_every_key(&copy);
		copy.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_plain_checkpoint_is_not_restored_into_an_encrypted_database() {
	let plain_dir = TempDir::new().unwrap();
	let checkpoints = TempDir::new().unwrap();
	let checkpoint = checkpoints.path().join("checkpoint");
	let plain = Tree::new(options(plain_dir.path(), None, DEFAULT_MEMTABLE)).unwrap();
	write_round(&plain, '1', 0, KEYS).await;
	plain.create_checkpoint(&checkpoint).unwrap();
	plain.close().await.unwrap();

	let dir = TempDir::new().unwrap();
	let ctx = context(CipherSuite::Aes256Gcm);
	let opts = options(dir.path(), Some(&ctx), DEFAULT_MEMTABLE);
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	write_round(&tree, '2', 0, KEYS).await;
	let before = snapshot(&opts);

	// The checkpoint is refused before anything of the database is touched.
	let err = tree.restore_from_checkpoint(&checkpoint).unwrap_err();
	assert!(
		matches!(&err, Error::InvalidArgument(message) if message.contains("not a usable checkpoint")),
		"{err:?}"
	);
	assert!(snapshot(&opts) == before, "a refused restore changed the files of the database");
	let txn = tree.begin().unwrap();
	assert_eq!(txn.get(key(0).as_slice()).unwrap(), Some(value('2', 0)));
	drop(txn);
	tree.close().await.unwrap();
}

#[tokio::test]
async fn a_table_that_was_changed_on_disk_is_an_error_that_repairs_and_removes_nothing() {
	let dir = TempDir::new().unwrap();
	let ctx = context(CipherSuite::XChaCha20Poly1305);
	let opts = options(dir.path(), Some(&ctx), DEFAULT_MEMTABLE);
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	write_round(&tree, '1', 0, KEYS).await;
	write_round(&tree, '2', 0, KEYS / 2).await;
	let ids = table_ids(&tree);
	tree.close().await.unwrap();

	// A bit of the meta-index of a table is changed, and the checksum of the block is made again,
	// by someone who does not have the key.
	let id = ids[0];
	let path = opts.sstable_file_path(id);
	let bytes = fs::read(&path).unwrap();
	let meta = first_of(&walk(&opts, &bytes, id), BlockKind::MetaIndex);
	fs::write(&path, flip(&bytes, &meta, meta.size / 2, TrailerLayout::Sealed, true)).unwrap();
	let before = snapshot(&opts);

	let err = open_error(&opts);
	assert!(matches!(err, Error::Encryption(EncryptionError::AuthenticationFailed)), "{err:?}");
	// The table is not repaired, replaced or removed, and nor is anything else.
	assert!(snapshot(&opts) == before, "a refused open changed the files of the database");

	// The table as it was is the database again.
	fs::write(&path, &bytes).unwrap();
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	read_every_key(&tree);
	tree.close().await.unwrap();
}
