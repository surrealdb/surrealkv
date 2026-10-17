//! `TreeBuilder::with_encryption`: the entry point of an embedder that configures a tree with a
//! builder, which must do what `Options::with_encryption` does and nothing else.

use std::fs;

use tempfile::TempDir;

use super::encryption_open_tests::{
	encryption_error,
	files_with_extension,
	key,
	manager,
	snapshot,
	value,
	KEYS,
	SUITES,
};
use super::tfs;
use crate::error::EncryptionError;
use crate::tde::{describe_manifest_file, is_sealed_manifest, CipherPreference, CipherSuite};
use crate::{Mode, Options, TreeBuilder};

fn builder(dir: &std::path::Path) -> TreeBuilder {
	TreeBuilder::new()
		.with_path(dir.to_path_buf())
		.with_enable_vlog(true)
		.with_vlog_value_threshold(64)
		.with_flush_on_close(true)
}

#[tokio::test]
async fn a_tree_built_with_a_key_manager_is_encrypted_and_opens_with_the_same_keys() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let root = dir.path().join("db");

		let tree = builder(&root).with_encryption(manager(), suite).build().unwrap();
		let mut txn = tree.begin().unwrap();
		for i in 0..KEYS {
			txn.set(key(i), value('1', i)).unwrap();
		}
		txn.commit().await.unwrap();
		tree.close().await.unwrap();

		// The manifest says what the builder asked for, and it is sealed.
		let opts = Options {
			path: root.clone(),
			..Default::default()
		};
		let manifest = opts.manifest_file_path(0);
		assert!(is_sealed_manifest(&fs::read(&manifest).unwrap()), "{suite:?}");
		let described = describe_manifest_file(&manifest).unwrap();
		assert!(described.encrypted);
		assert_eq!((described.suite, described.key_id), (Some(suite), Some(1)));
		assert!(!files_with_extension(&opts.sstable_dir(), "sst").is_empty(), "{suite:?}");

		// With the same keys it opens, and the suite that was asked for at the second open does
		// not change the database's own.
		let other = if suite == CipherSuite::Aes256Gcm {
			CipherSuite::XChaCha20Poly1305
		} else {
			CipherSuite::Aes256Gcm
		};
		let tree = builder(&root).with_encryption(manager(), other).build().unwrap();
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		for i in 0..KEYS {
			assert_eq!(txn.get(key(i).as_slice()).unwrap(), Some(value('1', i)), "{suite:?} {i}");
		}
		drop(txn);
		tree.close().await.unwrap();
		assert_eq!(describe_manifest_file(&manifest).unwrap().suite, Some(suite));
	}
}

#[tokio::test]
async fn a_tree_that_is_encrypted_is_refused_without_a_key_and_nothing_is_changed() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let root = dir.path().join("db");
	let tree = builder(&root).with_encryption(manager(), CipherPreference::Auto).build().unwrap();
	tree.close().await.unwrap();
	let before = snapshot(&root);

	let err = match builder(&root).build() {
		Ok(_) => panic!("an encrypted database was opened without a key"),
		Err(err) => err,
	};
	assert_eq!(encryption_error(err, "no key"), EncryptionError::NotConfigured);
	assert_eq!(snapshot(&root), before);
}

#[tokio::test]
async fn a_tree_in_the_clear_is_refused_when_a_key_is_given_and_nothing_is_changed() {
	if tfs::skip_listed() {
		return;
	}
	let dir = TempDir::new().unwrap();
	let root = dir.path().join("db");
	let tree = builder(&root).build().unwrap();
	tree.close().await.unwrap();
	let before = snapshot(&root);

	let err = match builder(&root).with_encryption(manager(), CipherPreference::Auto).build() {
		Ok(_) => panic!("a database in the clear was opened with a key"),
		Err(err) => err,
	};
	assert_eq!(
		encryption_error(err, "a key for a database in the clear"),
		EncryptionError::NotEncrypted
	);
	assert_eq!(snapshot(&root), before);
}
