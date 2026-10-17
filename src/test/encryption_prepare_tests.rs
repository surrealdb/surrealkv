//! The keys of a database are made available when it is opened, and are not asked for again by
//! anything that a write does.
//!
//! A manager that is backed by a provider fetches the roots in `prepare` and serves them from what
//! it fetched. So every key that a file of the database was written under has to be prepared when
//! the database is opened or restored, the newest key and the older ones, and nothing that happens
//! after that, a commit, a rotation of the log, a flush or a compaction, may reach the provider.

use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tempfile::TempDir;

use super::encryption_commit_path_tests::{
	active_segment,
	assert_running,
	commit,
	crash,
	get,
	is_unavailable,
	SMALL,
};
use super::encryption_open_tests::{files_with_extension, read as read_all, rounds, KEYS, SUITES};
use super::tfs;
use crate::error::{EncryptionError, KeyProviderFailure};
use crate::tde::{CipherSuite, CryptoHeader, KeyManager, KeyState, Zeroizing, ROOT_KEY_LEN};
use crate::{Error, Options, Tree};

/// A key manager in the shape that one backed by a provider has: the roots live with the provider,
/// `prepare` fetches them into a cache, and `root_key` answers from the cache and from nothing
/// else. While the provider is down `prepare` fails, for an id that is cached too, so that whatever
/// asks for a preparation then is found out.
struct PreparedOnly {
	/// What the provider holds: two keys.
	roots: [(u32, [u8; ROOT_KEY_LEN]); 2],
	active: AtomicU32,
	cache: Mutex<Vec<u32>>,
	provider_up: AtomicBool,
	/// The ids that every call of `prepare` was asked for.
	asked: Mutex<Vec<Vec<u32>>>,
	/// The preparations that were refused because the provider was down.
	refused_while_down: AtomicUsize,
	/// The roots that were asked for and were not prepared.
	misses: AtomicUsize,
}

impl PreparedOnly {
	fn new(active: u32) -> Arc<Self> {
		Arc::new(Self {
			roots: [(1, [0x21; ROOT_KEY_LEN]), (2, [0x22; ROOT_KEY_LEN])],
			active: AtomicU32::new(active),
			cache: Mutex::new(Vec::new()),
			provider_up: AtomicBool::new(true),
			asked: Mutex::new(Vec::new()),
			refused_while_down: AtomicUsize::new(0),
			misses: AtomicUsize::new(0),
		})
	}

	fn set_provider(&self, up: bool) {
		self.provider_up.store(up, Ordering::SeqCst);
	}

	fn rotate_to(&self, key_id: u32) {
		self.active.store(key_id, Ordering::SeqCst);
	}

	fn root_of(&self, key_id: u32) -> Option<[u8; ROOT_KEY_LEN]> {
		self.roots.iter().find(|(id, _)| *id == key_id).map(|(_, root)| *root)
	}

	fn asked(&self) -> Vec<Vec<u32>> {
		self.asked.lock().unwrap().clone()
	}
}

impl KeyManager for PreparedOnly {
	fn active_key_id(&self) -> u32 {
		self.active.load(Ordering::SeqCst)
	}

	fn key_state(&self, key_id: u32) -> KeyState {
		match (self.root_of(key_id), key_id == self.active_key_id()) {
			(None, _) => KeyState::Unknown,
			(Some(_), true) => KeyState::Active,
			(Some(_), false) => KeyState::DecryptOnly,
		}
	}

	fn root_key(&self, key_id: u32) -> Result<Zeroizing<[u8; ROOT_KEY_LEN]>, EncryptionError> {
		let root = self.root_of(key_id).ok_or(EncryptionError::UnknownKeyId)?;
		if self.cache.lock().unwrap().contains(&key_id) {
			return Ok(Zeroizing::new(root));
		}
		self.misses.fetch_add(1, Ordering::SeqCst);
		Err(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable))
	}

	fn prepare(&self, key_ids: &[u32]) -> Result<(), EncryptionError> {
		self.asked.lock().unwrap().push(key_ids.to_vec());
		if !self.provider_up.load(Ordering::SeqCst) {
			self.refused_while_down.fetch_add(1, Ordering::SeqCst);
			return Err(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable));
		}
		for &key_id in key_ids {
			if self.root_of(key_id).is_none() {
				return Err(EncryptionError::UnknownKeyId);
			}
			let mut cache = self.cache.lock().unwrap();
			if !cache.contains(&key_id) {
				cache.push(key_id);
			}
		}
		Ok(())
	}
}

fn prepared_options(path: &Path, km: &Arc<PreparedOnly>, suite: CipherSuite) -> Arc<Options> {
	Arc::new(
		Options {
			path: path.to_path_buf(),
			flush_on_close: false,
			memtable_stall_threshold: 1000,
			l0_stall_threshold: 1000,
			level0_max_files: 100,
			..Default::default()
		}
		.with_encryption(Arc::clone(km) as Arc<dyn KeyManager>, suite),
	)
}

#[tokio::test]
async fn a_table_under_an_older_key_opens_with_a_manager_that_serves_only_what_it_was_asked_for() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let what = format!("{suite:?}");
		let dir = TempDir::new().unwrap();

		// A table, and the manifest, are written under key 1.
		let first = PreparedOnly::new(1);
		let tree = Tree::new(prepared_options(dir.path(), &first, suite)).unwrap();
		super::encryption_open_tests::write(&tree, '1', 0..KEYS).await;
		tree.flush().unwrap();
		tree.close().await.unwrap();

		// Key 2 is active: a table and the manifest are written under it, and the manifest names
		// the newest key while the first table is under the older one.
		let second = PreparedOnly::new(2);
		let tree = Tree::new(prepared_options(dir.path(), &second, suite)).unwrap();
		super::encryption_open_tests::write(&tree, '2', 0..KEYS / 2).await;
		tree.flush().unwrap();
		tree.close().await.unwrap();

		// A manager that has been asked for nothing: it holds neither root. The manifest names key
		// 2, which is also the active key, so key 2 is asked for once, and the table under key 1 is
		// found when its file is opened, and asks for key 1 and for nothing else.
		let third = PreparedOnly::new(2);
		let tree = Tree::new(prepared_options(dir.path(), &third, suite)).unwrap();
		let mut ids: Vec<u32> = third.asked().into_iter().flatten().collect();
		ids.sort_unstable();
		assert_eq!(ids, vec![1, 2], "{what}: each key is asked for once: {:?}", third.asked());
		assert_eq!(third.misses.load(Ordering::SeqCst), 0, "{what}");

		let mut expected = rounds('1', 0..KEYS);
		expected.extend(rounds('2', 0..KEYS / 2));
		read_all(&tree, &expected);
		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn once_the_keys_are_prepared_a_provider_that_is_away_is_not_reached_by_any_write() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let what = format!("{suite:?}");
		let dir = TempDir::new().unwrap();
		let km = PreparedOnly::new(1);
		// Values that go to the value log, and ones that fill the memtable, so that the writes
		// below rotate the write-ahead log, start value-log files, flush memtables into tables and
		// compact them, each of which writes a manifest.
		let opts = Arc::new(
			Options {
				path: dir.path().to_path_buf(),
				max_memtable_size: SMALL,
				flush_on_close: false,
				enable_vlog: true,
				vlog_value_threshold: 2000,
				vlog_max_file_size: 1024,
				memtable_stall_threshold: 1000,
				l0_stall_threshold: 1000,
				level0_max_files: 2,
				..Default::default()
			}
			.with_encryption(Arc::clone(&km) as Arc<dyn KeyManager>, suite),
		);
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		assert_eq!(km.refused_while_down.load(Ordering::SeqCst), 0, "{what}");

		km.set_provider(false);
		for i in 0..20usize {
			let len = if i % 2 == 0 {
				1500
			} else {
				2500
			};
			commit(&tree, &format!("key{i:02}"), len).await.unwrap();
		}
		let queued = || tree.core.inner.immutable_count();
		let waited = tokio::time::timeout(std::time::Duration::from_secs(120), async {
			while queued() > 0 {
				tokio::time::sleep(std::time::Duration::from_millis(10)).await;
			}
		})
		.await;
		assert!(waited.is_ok(), "{what}: the flushes did not finish");

		// Nothing asked the manager for a key that it had not been asked to prepare, nothing asked
		// it to prepare anything, and no failure was recorded by any task.
		assert_eq!(km.refused_while_down.load(Ordering::SeqCst), 0, "{what}");
		assert_eq!(km.misses.load(Ordering::SeqCst), 0, "{what}");
		assert_eq!(tree.core.inner.error_handler.error_count(), 0, "{what}");
		assert!(!files_with_extension(&opts.sstable_dir(), "sst").is_empty(), "{what}");
		assert!(files_with_extension(&opts.vlog_dir(), "vlog").len() >= 2, "{what}");
		for i in 0..20usize {
			let len = if i % 2 == 0 {
				1500
			} else {
				2500
			};
			assert_eq!(get(&tree, &format!("key{i:02}")), Some(vec![b'v'; len]), "{what}: {i}");
		}
		tree.close().await.unwrap();
		assert_eq!(km.refused_while_down.load(Ordering::SeqCst), 0, "{what}: at the close");
		assert_eq!(tree.core.inner.error_handler.error_count(), 0, "{what}: at the close");

		// And the database opens again once the provider is back, and holds what was written.
		km.set_provider(true);
		let tree = Tree::new(opts).unwrap();
		for i in 0..20usize {
			let len = if i % 2 == 0 {
				1500
			} else {
				2500
			};
			assert_eq!(get(&tree, &format!("key{i:02}")), Some(vec![b'v'; len]), "{what}: {i}");
		}
		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_key_that_became_active_is_made_available_by_preparing_the_keys_again() {
	if tfs::skip_listed() {
		return;
	}
	for suite in SUITES {
		let what = format!("{suite:?}");
		let dir = TempDir::new().unwrap();
		let km = PreparedOnly::new(1);
		let opts = Arc::new(
			Options {
				path: dir.path().to_path_buf(),
				max_memtable_size: SMALL,
				flush_on_close: false,
				memtable_stall_threshold: 1000,
				..Default::default()
			}
			.with_encryption(Arc::clone(&km) as Arc<dyn KeyManager>, suite),
		);
		let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
		let tasks = tree.core.task_manager.lock().unwrap().clone().unwrap();
		tasks.stop().await;
		tree.core.is_closed.store(true, Ordering::SeqCst);
		commit(&tree, "first", 2500).await.unwrap();

		// The manager makes key 2 the active key, and has not fetched it. The next commit needs a
		// new segment of the log, and a key to start it with.
		km.rotate_to(2);
		let refused = commit(&tree, "second", 2500).await;
		assert!(is_unavailable(&refused), "{what}: {refused:?}");
		assert_running(&tree, &what);

		// Asking again while the provider is away fails with the same error and changes nothing.
		km.set_provider(false);
		let err = tree.prepare_keys().unwrap_err();
		assert!(
			matches!(
				err,
				Error::Encryption(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable))
			),
			"{what}: {err:?}"
		);
		assert!(is_unavailable(&commit(&tree, "second", 2500).await), "{what}");

		// The provider is back and the keys are prepared again: the commit goes through, and the
		// segment that it starts is sealed under the new key.
		km.set_provider(true);
		tree.prepare_keys().unwrap();
		commit(&tree, "second", 2500).await.unwrap();
		assert_running(&tree, &what);
		let segment = opts.wal_dir().join(format!("{:020}.wal", active_segment(&tree)));
		let bytes = fs::read(segment).unwrap();
		let header = CryptoHeader::decode(&bytes[crate::wal::HEADER_SIZE..]).unwrap();
		assert_eq!(header.identity.key_id, 2, "{what}");
		assert_eq!(get(&tree, "first"), Some(vec![b'v'; 2500]), "{what}");
		assert_eq!(get(&tree, "second"), Some(vec![b'v'; 2500]), "{what}");
		crash(&tree);
	}
}

#[tokio::test]
async fn preparing_the_keys_of_a_database_that_is_not_encrypted_does_nothing() {
	let dir = TempDir::new().unwrap();
	let tree = Tree::new(Arc::new(Options {
		path: dir.path().to_path_buf(),
		..Default::default()
	}))
	.unwrap();
	tree.prepare_keys().unwrap();
	tree.close().await.unwrap();
}
