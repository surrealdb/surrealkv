//! A tree whose open fails leaves nothing behind that keeps the database locked, so the same
//! directory can be opened again in the same process, as an embedder that retries after a key
//! provider was away, or after a mode was refused, does.

use std::fs;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tempfile::TempDir;

use super::encryption_commit_path_tests::{active_segment, commit, crash, get};
use crate::lsm::{Core, CoreInner};
use crate::tde::{CipherSuite, KeyManager, SoftwareKeyManager};
use crate::{Error, Options, Tree, WalRecoveryMode};

#[tokio::test]
async fn a_tree_that_cannot_be_opened_does_not_keep_the_database_locked() {
	let dir = TempDir::new().unwrap();
	let strict = Options {
		path: dir.path().to_path_buf(),
		flush_on_close: false,
		wal_recovery_mode: WalRecoveryMode::AbsoluteConsistency,
		..Default::default()
	};
	let tree = Arc::new(Tree::new(Arc::new(strict.clone())).unwrap());
	let tasks = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tasks.stop().await;
	tree.core.is_closed.store(true, Ordering::SeqCst);
	for key in ["a", "b", "c"] {
		commit(&tree, key, 40).await.unwrap();
	}
	let segment = dir.path().join("wal").join(format!("{:020}.wal", active_segment(&tree)));
	crash(&tree);
	drop(tree);
	let len = fs::metadata(&segment).unwrap().len();
	fs::OpenOptions::new().write(true).open(&segment).unwrap().set_len(len - 5).unwrap();

	// The tail is damaged, and this mode does not repair it: the open fails, after the WAL was
	// read, which is after the lock of the database was taken.
	let refused = Tree::new(Arc::new(strict.clone())).err().expect("the open is refused");
	assert!(matches!(refused, Error::WalCorruption { .. }), "{refused:?}");

	// Nothing of the failed open is left holding the lock, so the same directory opens at once.
	let repairing = Options {
		wal_recovery_mode: WalRecoveryMode::TolerateCorruptedWithRepair,
		..strict
	};
	let tree = Tree::new(Arc::new(repairing)).unwrap();
	assert_eq!(get(&tree, "a"), Some(vec![b'v'; 40]));
	assert_eq!(get(&tree, "b"), Some(vec![b'v'; 40]));
	assert_eq!(get(&tree, "c"), None);
	tree.close().await.unwrap();
}

/// Options that name a key manager and carry no context are not a database that is meant to be
/// encrypted and has not been settled yet: they are a path that skipped the settling, and every
/// file below would be written in the clear. `Tree::new` settles the context before it builds a
/// core, so the cores are tried here.
#[tokio::test]
async fn options_with_a_key_manager_and_no_context_open_nothing_and_create_nothing() {
	let dir = TempDir::new().unwrap();
	let root = dir.path().join("db");
	let key_manager: Arc<dyn KeyManager> = Arc::new(SoftwareKeyManager::new([0x12; 32]));
	let opts = Arc::new(
		Options {
			path: root.clone(),
			..Default::default()
		}
		.with_encryption(key_manager, CipherSuite::Aes256Gcm),
	);
	assert!(opts.encryption_context().is_none());

	let refused = CoreInner::new(Arc::clone(&opts)).err().expect("the inner core is refused");
	assert!(matches!(&refused, Error::InvalidArgument(message) if message.contains("plaintext")));
	let refused = Core::new(opts).err().expect("the core is refused");
	assert!(matches!(&refused, Error::InvalidArgument(message) if message.contains("plaintext")));

	// Not a directory, not a lock file, not a segment of the log.
	assert!(!root.exists(), "a refused core created {}", root.display());
}
