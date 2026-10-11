//! Counterpart of `encryption_guard.rs`: the non-default `encryption-preview` feature compiles
//! the guard out, so a key manager is accepted. Run in CI so the feature-gated path keeps
//! building and the feature keeps doing what it says.
//!
//! The first test only pins that the guard is removed, for every way of asking for a cipher. The
//! rest take a database with a key manager through the public interface alone and look at the
//! files it leaves: a database is created encrypted, opens again with its key and with nothing
//! else, and a refused open changes no file.
//!
//! The manifest, the tables and the value log are sealed at this point. The write-ahead log is
//! not, so nothing here looks for a marker in it, and a database is only meant to be read back
//! after a clean close, which flushes what the log holds into tables.

#![cfg(all(not(target_arch = "wasm32"), feature = "encryption-preview"))]

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use surrealkv::tde::{describe_manifest_file, CryptoHeader, MANIFEST_MAGIC};
use surrealkv::{
	CipherPreference,
	CipherSuite,
	EncryptionError,
	Error,
	KeyManager,
	Options,
	SoftwareKeyManager,
	Tree,
	TreeBuilder,
};

#[tokio::test]
async fn build_accepts_key_manager() {
	let preferences = [
		CipherPreference::Auto,
		CipherPreference::Suite(CipherSuite::Aes256Gcm),
		CipherPreference::Suite(CipherSuite::XChaCha20Poly1305),
	];
	for preference in preferences {
		let dir = tempfile::tempdir().unwrap();
		let db_path = dir.path().join("db");

		let key_manager: Arc<dyn KeyManager> = Arc::new(SoftwareKeyManager::new([0x5a; 32]));
		let opts =
			Options::new().with_path(db_path.clone()).with_encryption(key_manager, preference);
		assert!(opts.validate().is_ok());

		let tree = TreeBuilder::with_options(opts).build().unwrap();
		assert!(db_path.join("manifest").exists());
		tree.close().await.unwrap();
	}
}

const ROOT: [u8; 32] = [0x5a; 32];
const KEYS: usize = 20;
const SUITES: [CipherSuite; 2] = [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];

fn key(i: usize) -> Vec<u8> {
	format!("pk-{i:03}").into_bytes()
}

/// A value that is long enough to go to the value log, and holds a marker.
fn value(i: usize) -> Vec<u8> {
	format!("pv-canary-{i:03}-{}", "y".repeat(90)).into_bytes()
}

fn options(path: &Path) -> Options {
	Options::new()
		.with_path(path.to_path_buf())
		.with_enable_vlog(true)
		.with_vlog_value_threshold(64)
}

fn encrypted(path: &Path, root: [u8; 32], suite: CipherSuite) -> Options {
	let key_manager: Arc<dyn KeyManager> = Arc::new(SoftwareKeyManager::new(root));
	options(path).with_encryption(key_manager, suite)
}

async fn fill(tree: &Tree) {
	let mut txn = tree.begin().unwrap();
	for i in 0..KEYS {
		txn.set(key(i), value(i)).unwrap();
	}
	txn.commit().await.unwrap();
}

fn check(tree: &Tree) {
	let txn = tree.begin().unwrap();
	for i in 0..KEYS {
		assert_eq!(txn.get(key(i).as_slice()).unwrap(), Some(value(i)), "key {i}");
	}
}

fn files(dir: &Path, extension: &str) -> Vec<PathBuf> {
	let Ok(entries) = fs::read_dir(dir) else {
		return Vec::new();
	};
	entries
		.flatten()
		.map(|entry| entry.path())
		.filter(|path| path.extension().is_some_and(|ext| ext == extension))
		.collect()
}

fn snapshot(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
	fn visit(dir: &Path, found: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
		for entry in fs::read_dir(dir).unwrap().flatten() {
			let path = entry.path();
			if entry.file_type().unwrap().is_dir() {
				found.insert(path.clone(), None);
				visit(&path, found);
			} else if path.file_name().is_some_and(|name| name == "LOCK") {
				// A lock file holds a process id and no data, and may not be readable while held.
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

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
	haystack.windows(needle.len()).any(|window| window == needle)
}

fn refusal(opts: Options) -> Error {
	match TreeBuilder::with_options(opts).build() {
		Ok(_) => panic!("the database was opened"),
		Err(err) => err,
	}
}

#[tokio::test]
async fn an_encrypted_database_is_sealed_where_it_can_be_and_opens_again_only_with_its_key() {
	for suite in SUITES {
		let dir = tempfile::tempdir().unwrap();
		let db = dir.path().join("db");

		// Created, written, and closed, which flushes the log into tables.
		let tree = TreeBuilder::with_options(encrypted(&db, ROOT, suite)).build().unwrap();
		fill(&tree).await;
		check(&tree);
		tree.close().await.unwrap();

		// The manifest names the database and the suite without a key, and is the encrypted
		// container, so that a reader that does not know it stops there.
		let manifest = files(&db.join("manifest"), "manifest");
		assert_eq!(manifest.len(), 1, "{suite:?}");
		let described = describe_manifest_file(&manifest[0]).unwrap();
		assert!(described.encrypted);
		assert_eq!(described.suite, Some(suite));
		assert_eq!(described.key_id, Some(1));
		assert!(described.db_id.is_some());
		let bytes = fs::read(&manifest[0]).unwrap();
		assert!(bytes.starts_with(&MANIFEST_MAGIC));
		assert_ne!(bytes[..2], [0, 1], "a plaintext manifest begins with its version");

		// Every table and every value-log file is sealed, and no key or value is in any of them.
		let tables = files(&db.join("sstables"), "sst");
		let logs = files(&db.join("vlog"), "vlog");
		assert!(!tables.is_empty() && !logs.is_empty(), "{suite:?}");
		for table in &tables {
			let bytes = fs::read(table).unwrap();
			assert!(CryptoHeader::has_magic(&bytes), "{table:?}");
			assert_eq!(CryptoHeader::decode(&bytes).unwrap().identity.suite, suite);
		}
		for log in &logs {
			assert_eq!(fs::read(log).unwrap()[4..6], [0x80, 0x01], "{log:?}");
		}
		for file in tables.iter().chain(&logs).chain(&manifest) {
			let bytes = fs::read(file).unwrap();
			assert!(!contains(&bytes, b"pv-canary") && !contains(&bytes, b"pk-00"), "{file:?}");
		}

		// Another key manager with the same key opens it. The suite that is asked for is not the
		// database's: it is the one it was created with.
		let other = if suite == CipherSuite::Aes256Gcm {
			CipherSuite::XChaCha20Poly1305
		} else {
			CipherSuite::Aes256Gcm
		};
		let tree = TreeBuilder::with_options(encrypted(&db, ROOT, other)).build().unwrap();
		check(&tree);
		tree.close().await.unwrap();
		assert_eq!(describe_manifest_file(&manifest[0]).unwrap().suite, Some(suite));

		// A refused open leaves every file as it was.
		let before = snapshot(&db);
		let err = refusal(encrypted(&db, [0x99; 32], suite));
		assert!(matches!(err, Error::Encryption(EncryptionError::WrongKey)), "{suite:?}: {err:?}");
		let err = refusal(options(&db));
		assert!(
			matches!(err, Error::Encryption(EncryptionError::NotConfigured)),
			"{suite:?}: {err:?}"
		);
		assert!(snapshot(&db) == before, "{suite:?}: a refused open changed the database");

		let tree = TreeBuilder::with_options(encrypted(&db, ROOT, suite)).build().unwrap();
		check(&tree);
		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_database_that_is_not_encrypted_is_not_made_encrypted_by_a_key_manager() {
	for suite in SUITES {
		let dir = tempfile::tempdir().unwrap();
		let db = dir.path().join("db");
		let tree = TreeBuilder::with_options(options(&db)).build().unwrap();
		fill(&tree).await;
		tree.close().await.unwrap();
		let before = snapshot(&db);
		let manifest = files(&db.join("manifest"), "manifest");
		assert!(!describe_manifest_file(&manifest[0]).unwrap().encrypted);

		let err = refusal(encrypted(&db, ROOT, suite));
		assert!(matches!(err, Error::Encryption(EncryptionError::NotEncrypted)), "{err:?}");
		assert!(snapshot(&db) == before, "{suite:?}: a refused open changed the database");

		let tree = TreeBuilder::with_options(options(&db)).build().unwrap();
		check(&tree);
		tree.close().await.unwrap();
	}
}
