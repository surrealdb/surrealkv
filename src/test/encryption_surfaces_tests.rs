//! What an encrypted database does with a key that is not to be had when it writes its manifest or
//! shuts down, and what a foreign database in its directory does not make of it: the error of the
//! key is the error that the caller gets, and nothing in the clear is made of the data.

use std::fs;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use tempfile::TempDir;

use super::encryption_open_tests::{
	create,
	encrypted,
	manager,
	open_error,
	options,
	read,
	rounds,
	snapshot,
	write,
	KEYS,
	SUITES,
};
use super::sstable_encryption_tests::FlakyManager;
use crate::error::{EncryptionError, KeyProviderFailure};
use crate::tde::{CipherSuite, KeyManager, KeyState, SoftwareKeyManager, Zeroizing, ROOT_KEY_LEN};
use crate::{Error, Options, Tree};

fn unavailable(err: &Error) -> bool {
	matches!(err, Error::Encryption(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable)))
}

#[tokio::test]
async fn a_manifest_that_cannot_be_sealed_is_the_error_of_the_flush_and_is_not_written() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let flaky = FlakyManager::new();
		let opts = encrypted(dir.path(), Arc::clone(&flaky) as Arc<dyn KeyManager>, suite);
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		write(&tree, '1', 0..KEYS).await;
		let manifest = opts.manifest_file_path(0);
		let before = fs::read(&manifest).unwrap();

		// The provider goes away once the table is on disk, which is when the manifest is sealed.
		let provider = Arc::clone(&flaky);
		*tree.core.inner.flush_hook.lock() = Some(Arc::new(move |_| {
			provider.set_unavailable(true);
			Ok(())
		}));
		let err = tree.flush().unwrap_err();
		assert!(unavailable(&err), "{suite:?}: {err:?}");

		// The manifest on disk is the one it was, no table is listed, and nothing of the write is
		// left beside it. The data is still there.
		assert_eq!(fs::read(&manifest).unwrap(), before, "{suite:?}");
		assert!(tree.core.inner.level_manifest.read().unwrap().iter().next().is_none());
		let strays = fs::read_dir(opts.manifest_dir())
			.unwrap()
			.flatten()
			.filter(|entry| entry.file_name().to_string_lossy().starts_with(".tmp_"))
			.count();
		assert_eq!(strays, 0, "{suite:?}");
		// The provider is back before the data is read, because reading a value opens its
		// value-log file, which asks the provider for the root. The flush that is tried again then
		// writes the table and the manifest.
		*tree.core.inner.flush_hook.lock() = None;
		flaky.set_unavailable(false);
		read(&tree, &rounds('1', 0..KEYS));
		tree.flush().unwrap();
		assert_ne!(fs::read(&manifest).unwrap(), before, "{suite:?}");
		assert!(tree.core.inner.level_manifest.read().unwrap().iter().next().is_some());
		read(&tree, &rounds('1', 0..KEYS));
		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn the_flush_at_shutdown_keeps_the_error_of_a_key_that_could_not_be_had() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let flaky = FlakyManager::new();
		let opts = Arc::new(
			Options {
				flush_on_close: true,
				..options(dir.path())
			}
			.with_encryption(Arc::clone(&flaky) as Arc<dyn KeyManager>, suite),
		);
		let tree = Tree::new(opts).unwrap();
		write(&tree, '1', 0..KEYS).await;

		// The key is not to be had when the memtable is flushed: that is what `close` says, and
		// not a text that a failed flush was the reason.
		flaky.set_unavailable(true);
		let err = tree.close().await.unwrap_err();
		assert!(unavailable(&err), "{suite:?}: {err:?}");
	}
}

/// A key manager that serves one root after it is armed and none after that.
struct AfterOne {
	inner: SoftwareKeyManager,
	armed: AtomicBool,
	served: AtomicUsize,
}

impl KeyManager for AfterOne {
	fn active_key_id(&self) -> u32 {
		self.inner.active_key_id()
	}

	fn key_state(&self, key_id: u32) -> KeyState {
		self.inner.key_state(key_id)
	}

	fn root_key(&self, key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
		if self.armed.load(Ordering::SeqCst) && self.served.fetch_add(1, Ordering::SeqCst) >= 1 {
			return Err(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable));
		}
		self.inner.root_key(key_id)
	}
}

#[tokio::test]
async fn the_manifest_that_follows_the_last_flush_of_a_shutdown_keeps_the_error_of_the_key() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let key_manager = Arc::new(AfterOne {
			inner: SoftwareKeyManager::new(super::encryption_open_tests::ROOT),
			armed: AtomicBool::new(false),
			served: AtomicUsize::new(0),
		});
		let opts = Arc::new(
			Options {
				flush_on_close: true,
				..options(dir.path())
			}
			.with_encryption(Arc::clone(&key_manager) as Arc<dyn KeyManager>, suite),
		);
		let tree = Tree::new(opts).unwrap();
		write(&tree, '1', 0..KEYS).await;
		// The memtable is queued, and the active one is empty: the shutdown flushes the queue, and
		// then writes the manifest once more for the log number.
		tree.core.inner.rotate_memtable().unwrap();

		// The first manifest that the flush writes is sealed, and the second is not: the key is
		// served once after the table is on disk.
		let armed = Arc::clone(&key_manager);
		*tree.core.inner.flush_hook.lock() = Some(Arc::new(move |_| {
			armed.armed.store(true, Ordering::SeqCst);
			Ok(())
		}));
		let err = tree.close().await.unwrap_err();
		assert!(unavailable(&err), "{suite:?}: {err:?}");
	}
}

#[tokio::test]
async fn a_database_of_another_store_in_the_directory_of_an_encrypted_one_is_not_migrated_into_it()
{
	use surrealkv_compat_v1::{V1_FOOTER_TOTAL_LEN, V1_MAGIC_FOOTER};

	let dir = TempDir::new().unwrap();
	let km = manager();
	let opts =
		create(dir.path(), Arc::clone(&km) as Arc<dyn KeyManager>, CipherSuite::Aes256Gcm).await;

	// What each migration would take for a database of its own, and the directory it would move it
	// to, in the clear.
	let mut v1_table = vec![0u8; V1_FOOTER_TOTAL_LEN];
	v1_table[V1_FOOTER_TOTAL_LEN - V1_MAGIC_FOOTER.len()..].copy_from_slice(&V1_MAGIC_FOOTER);
	type Place = Box<dyn Fn(&std::path::Path)>;
	let cases: Vec<(&str, &str, Place)> = vec![
		(
			"RocksDB",
			"_rocksdb_backup",
			Box::new(|root| {
				fs::write(root.join("CURRENT"), b"MANIFEST-000001\n").unwrap();
				fs::write(root.join("MANIFEST-000001"), b"not a real rocksdb manifest").unwrap();
			}),
		),
		(
			"SurrealKV v1",
			"_v1_backup",
			Box::new(move |root| fs::write(root.join("000001.sst"), &v1_table).unwrap()),
		),
		(
			"IndexedDB",
			"_indxdb_backup",
			Box::new(|root| {
				surrealkv_compat_indxdb::export_dump(
					root.join("indxdb_dump.bin"),
					&[(b"user:001".to_vec(), b"Alice".to_vec())],
				)
				.unwrap();
			}),
		),
	];
	for (store, backup, place) in cases {
		place(dir.path());
		let before = snapshot(dir.path());
		let err = open_error(&opts);
		assert!(
			matches!(&err, Error::InvalidArgument(message)
				if message.contains(store) && message.contains("encrypted")),
			"{store}: {err:?}"
		);
		// Nothing was read, moved or copied: the files are as they were and there is no backup.
		assert!(snapshot(dir.path()) == before, "{store}: a refused open changed the directory");
		assert!(!dir.path().join(backup).exists(), "{store}: a backup was made");

		// Without the foreign files, which are removed again, the database opens as it was.
		for name in ["CURRENT", "MANIFEST-000001", "000001.sst", "indxdb_dump.bin"] {
			let _ = fs::remove_file(dir.path().join(name));
		}
		let tree = Tree::new(encrypted(
			dir.path(),
			Arc::clone(&km) as Arc<dyn KeyManager>,
			CipherSuite::Aes256Gcm,
		))
		.unwrap();
		read(&tree, &{
			let mut expected = rounds('1', 0..KEYS);
			expected.extend(rounds('2', 0..KEYS / 2));
			expected
		});
		tree.close().await.unwrap();
	}
}
