//! A tree whose open fails leaves nothing behind that keeps the database locked, so the same
//! directory can be opened again in the same process, as an embedder that retries after a key
//! provider was away, or after a mode was refused, does.

use std::fs;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tempfile::TempDir;

use super::encryption_commit_path_tests::{active_segment, commit, crash, get};
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
