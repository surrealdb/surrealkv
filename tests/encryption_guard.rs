//! A configured key manager must make `build` fail instead of silently writing plaintext.
//!
//! Encryption at rest is not wired into any SSTable, WAL or value-log path yet. These tests
//! link against the library the way a downstream crate does, so `cfg(test)` is not set on it
//! and the guard is active. They are compiled out when the non-default `encryption-preview`
//! feature removes the guard.

#![cfg(all(not(target_arch = "wasm32"), not(feature = "encryption-preview")))]

use std::path::Path;
use std::sync::Arc;

use surrealkv::{CipherSuite, Error, KeyManager, Options, SoftwareKeyManager, Tree, TreeBuilder};

const SUITES: [CipherSuite; 2] = [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];

fn options_with_key_manager(path: &Path, suite: CipherSuite) -> Options {
	let key_manager: Arc<dyn KeyManager> = Arc::new(SoftwareKeyManager::new([0x5a; 32]));
	Options::new().with_path(path.to_path_buf()).with_encryption(key_manager, suite)
}

fn assert_encryption_refused(err: Error) {
	let Error::InvalidArgument(message) = err else {
		panic!("expected Error::InvalidArgument, got {err:?}");
	};
	assert!(
		message.to_lowercase().contains("encryption"),
		"message should name the feature: {message}"
	);
	assert!(message.contains("key manager"), "message should name the option: {message}");
}

fn expect_err(result: surrealkv::Result<Tree>) -> Error {
	match result {
		Ok(_) => panic!("build succeeded with a key manager configured"),
		Err(err) => err,
	}
}

// The tests run on a Tokio runtime so that a build that wrongly succeeds fails on the
// assertions below rather than on a missing reactor.
#[tokio::test]
async fn build_rejects_key_manager_and_creates_nothing() {
	for suite in SUITES {
		let dir = tempfile::tempdir().unwrap();
		let db_path = dir.path().join("db");

		let opts = options_with_key_manager(&db_path, suite);
		assert_encryption_refused(expect_err(TreeBuilder::with_options(opts).build()));
		assert!(!db_path.exists(), "{suite:?}: a rejected build must not create the directory");
	}
}

#[tokio::test]
async fn build_with_options_rejects_key_manager_and_creates_nothing() {
	for suite in SUITES {
		let dir = tempfile::tempdir().unwrap();
		let db_path = dir.path().join("db");

		let opts = options_with_key_manager(&db_path, suite);
		let result = TreeBuilder::with_options(opts).build_with_options();
		assert_encryption_refused(match result {
			Ok(_) => panic!("build_with_options succeeded with a key manager configured"),
			Err(err) => err,
		});
		assert!(!db_path.exists(), "{suite:?}: a rejected build must not create the directory");
	}
}

#[tokio::test]
async fn rejected_build_leaves_an_existing_directory_untouched() {
	let dir = tempfile::tempdir().unwrap();
	let db_path = dir.path().join("db");
	std::fs::create_dir(&db_path).unwrap();

	let opts = options_with_key_manager(&db_path, CipherSuite::default()).with_enable_vlog(true);
	assert_encryption_refused(expect_err(TreeBuilder::with_options(opts).build()));

	let entries = std::fs::read_dir(&db_path).unwrap().count();
	assert_eq!(
		entries, 0,
		"a rejected build must not create anything inside an existing directory"
	);
}

#[test]
fn validate_rejects_key_manager() {
	for suite in SUITES {
		let opts = options_with_key_manager(Path::new("unused"), suite);
		assert_encryption_refused(opts.validate().unwrap_err());
	}
}

#[tokio::test]
async fn rejected_build_does_not_block_a_plain_build_at_the_same_path() {
	let dir = tempfile::tempdir().unwrap();
	let db_path = dir.path().join("db");

	let opts = options_with_key_manager(&db_path, CipherSuite::default());
	assert_encryption_refused(expect_err(TreeBuilder::with_options(opts).build()));
	assert!(!db_path.exists());

	let tree = TreeBuilder::new().with_path(db_path.clone()).build().unwrap();
	assert!(db_path.join("manifest").exists());
	tree.close().await.unwrap();
}

/// Control: without a key manager the same builder, path and options open and work normally,
/// so the rejections above are caused by the key manager and not by the path or the harness.
/// This passes on code without the guard as well.
#[tokio::test]
async fn build_without_key_manager_succeeds() {
	let dir = tempfile::tempdir().unwrap();
	let db_path = dir.path().join("db");

	assert!(Options::new().validate().is_ok());
	let (tree, opts) = TreeBuilder::new().with_path(db_path.clone()).build_with_options().unwrap();
	assert!(opts.key_manager.is_none());
	assert!(db_path.join("manifest").exists());

	let mut txn = tree.begin().unwrap();
	txn.set(b"key", b"value").unwrap();
	txn.commit().await.unwrap();
	let txn = tree.begin().unwrap();
	assert_eq!(txn.get(b"key").unwrap(), Some(b"value".to_vec()));
	drop(txn);

	tree.close().await.unwrap();
}
