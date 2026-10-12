//! A crash while an encrypted database is being created, or while its manifest is being replaced.
//!
//! The manifest is the first file of an encrypted database and the one that says whose the others
//! are, so what a crash can leave is the manifest alone, the manifest and a log segment that has
//! nothing in it, or an old manifest with the temporary file of the one that was being written
//! beside it. Each is a database that opens with its key, works, and holds what is written to it
//! afterwards, and one that a key that does not fit refuses without a byte of it changed.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use tempfile::TempDir;

use super::encryption_open_tests::{
	encrypted,
	encryption_error,
	files_with_extension,
	manager,
	open_error,
	read,
	rounds,
	snapshot,
	write,
	KEYS,
	SUITES,
};
use crate::error::EncryptionError;
use crate::tde::{CipherSuite, SoftwareKeyManager, ROOT_KEY_LEN};
use crate::{Options, Tree};

/// A database in `root` that was created and closed with nothing written to it: its manifest, and
/// the segment of the log that the open made, which holds a header and no record.
async fn created_and_closed(root: &Path, suite: CipherSuite) -> Arc<Options> {
	let opts = encrypted(root, manager(), suite);
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	tree.close().await.unwrap();
	opts
}

/// Checks that the database `opts` refuses a key that does not fit and leaves the files as they
/// were, and then that it opens with its own key, takes writes, and holds them when it is opened
/// again.
async fn refused_without_its_key_and_working_with_it(
	opts: &Arc<Options>,
	suite: CipherSuite,
	what: &str,
) {
	let root = opts.path.clone();
	let before = snapshot(&root);
	let wrong = Arc::new(SoftwareKeyManager::new([0x43; ROOT_KEY_LEN]));
	let err = open_error(&encrypted(&root, wrong, suite));
	assert_eq!(encryption_error(err, what), EncryptionError::WrongKey, "{suite:?}: {what}");
	assert!(snapshot(&root) == before, "{suite:?}: {what}: a refused open changed the database");

	let tree = Tree::new(Arc::clone(opts)).unwrap();
	write(&tree, '1', 0..KEYS).await;
	read(&tree, &rounds('1', 0..KEYS));
	tree.close().await.unwrap();

	let tree = Tree::new(Arc::clone(opts)).unwrap();
	read(&tree, &rounds('1', 0..KEYS));
	tree.close().await.unwrap();
}

#[tokio::test]
async fn a_database_that_has_its_manifest_and_nothing_else_opens_with_its_key_and_not_without() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let opts = created_and_closed(&dir.path().join("db"), suite).await;

		// The crash came before the first segment of the log was made.
		let segments = files_with_extension(&opts.wal_dir(), "wal");
		assert!(!segments.is_empty(), "{suite:?}: the open makes a segment");
		for segment in segments {
			fs::remove_file(segment).unwrap();
		}
		assert!(files_with_extension(&opts.sstable_dir(), "sst").is_empty(), "{suite:?}");
		assert!(files_with_extension(&opts.vlog_dir(), "vlog").is_empty(), "{suite:?}");

		refused_without_its_key_and_working_with_it(&opts, suite, "no segment").await;
	}
}

#[tokio::test]
async fn a_database_whose_first_segment_is_empty_opens_with_its_key_and_not_without() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let opts = created_and_closed(&dir.path().join("db"), suite).await;

		// The crash came after the file of the segment was made and before its header was written.
		let segments = files_with_extension(&opts.wal_dir(), "wal");
		assert!(!segments.is_empty(), "{suite:?}: the open makes a segment");
		for segment in segments {
			fs::write(segment, b"").unwrap();
		}

		refused_without_its_key_and_working_with_it(&opts, suite, "an empty segment").await;
	}
}

#[tokio::test]
async fn the_temporary_file_of_a_manifest_write_that_did_not_finish_is_not_read_and_not_removed() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let opts = created_and_closed(&dir.path().join("db"), suite).await;

		// The write was cut off before the rename: the old manifest is in place, and beside it a
		// file that holds a manifest of its own, sealed under a salt of its own, or only a part of
		// one.
		let manifest = opts.manifest_file_path(0);
		let sealed = fs::read(&manifest).unwrap();
		let whole = manifest.with_file_name(".tmp_1_1");
		let part = manifest.with_file_name(".tmp_2_2");
		fs::write(&whole, &sealed).unwrap();
		fs::write(&part, &sealed[..sealed.len() / 2]).unwrap();

		refused_without_its_key_and_working_with_it(&opts, suite, "a temporary manifest").await;

		// Nothing here reads it, and nothing deletes what it did not write.
		assert_eq!(fs::read(&whole).unwrap(), sealed, "{suite:?}");
		assert_eq!(fs::read(&part).unwrap(), &sealed[..sealed.len() / 2], "{suite:?}");
	}
}
