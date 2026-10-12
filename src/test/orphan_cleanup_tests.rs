//! The clean-up of the table files that the manifest does not list, which an open makes by their
//! names. In a database that is encrypted it reads the start of each of them first: what is not a
//! file that this database could have left is refused, with the open, and nothing is deleted.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;

use super::encryption_open_tests::{
	encrypted,
	encryption_error,
	files_with_extension,
	manager,
	open_error,
	plain,
	read,
	rounds,
	snapshot,
	write,
	KEYS,
	SUITES,
};
use crate::error::EncryptionError;
use crate::tde::CipherSuite;
use crate::{Options, Tree};

/// A database in `root` with two tables, closed.
async fn closed_database(root: &Path, suite: CipherSuite) -> Arc<Options> {
	let opts = encrypted(root, manager(), suite);
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	write(&tree, '1', 0..KEYS).await;
	tree.flush().unwrap();
	write(&tree, '2', 0..KEYS / 2).await;
	tree.flush().unwrap();
	tree.close().await.unwrap();
	opts
}

fn table_file(dir: &Path, id: u64) -> PathBuf {
	dir.join(format!("{id:020}.sst"))
}

#[tokio::test]
async fn a_table_of_another_database_refuses_the_open_and_nothing_is_deleted() {
	for suite in SUITES {
		let what = format!("{suite:?}");
		let dir = TempDir::new().unwrap();
		let live = dir.path().join("live");
		let opts = closed_database(&live, suite).await;
		let sstables = opts.sstable_dir();
		let own = fs::read(&files_with_extension(&sstables, "sst")[0]).unwrap();

		// Another database that shares the key and has an identity of its own, and one that is not
		// encrypted.
		let foreign_opts = closed_database(&dir.path().join("foreign"), suite).await;
		let foreign =
			fs::read(&files_with_extension(&foreign_opts.sstable_dir(), "sst")[0]).unwrap();
		let plain_root = dir.path().join("plain");
		let plain_opts = plain(&plain_root);
		let tree = Tree::new(Arc::clone(&plain_opts)).unwrap();
		write(&tree, '1', 0..KEYS).await;
		tree.flush().unwrap();
		tree.close().await.unwrap();
		let in_the_clear =
			fs::read(&files_with_extension(&plain_opts.sstable_dir(), "sst")[0]).unwrap();

		// What this database leaves when a flush or a compaction does not finish: an older copy of
		// one of its tables, an empty file, a file of zeros and the start of a table.
		fs::write(table_file(&sstables, 9001), &own).unwrap();
		fs::write(table_file(&sstables, 9002), b"").unwrap();
		fs::write(table_file(&sstables, 9003), vec![0u8; 300]).unwrap();
		fs::write(table_file(&sstables, 9004), &own[..40]).unwrap();
		let leftovers = [9001, 9002, 9003, 9004];
		let before = snapshot(&live);

		// Each of these is something else, and refuses the open, in the order of the files or not:
		// the leftovers of this database that are there too are not deleted.
		let mut rotted = own.clone();
		rotted[40] ^= 1;
		let cases: Vec<(&str, Vec<u8>, EncryptionError)> = vec![
			("another database", foreign.clone(), EncryptionError::DbIdMismatch),
			("another database, cut short", foreign[..40].to_vec(), EncryptionError::DbIdMismatch),
			("in the clear", in_the_clear, EncryptionError::PlaintextInEncryptedDb),
			(
				"not a table",
				b"not a table of any database".to_vec(),
				EncryptionError::PlaintextInEncryptedDb,
			),
			("a header that does not read", rotted, EncryptionError::MalformedHeader),
		];
		for (label, bytes, wanted) in cases {
			let stranger = table_file(&sstables, 9100);
			fs::write(&stranger, &bytes).unwrap();
			let mut expected_files = before.clone();
			expected_files.insert(stranger.clone(), Some(bytes));

			let err = open_error(&opts);
			assert_eq!(encryption_error(err, label), wanted, "{what}: {label}");
			assert!(
				snapshot(&live) == expected_files,
				"{what}: {label}: a refused open changed or removed a file"
			);
			for id in leftovers {
				assert!(table_file(&sstables, id).exists(), "{what}: {label}: {id} was deleted");
			}
			fs::remove_file(&stranger).unwrap();
		}

		// Nothing else is in the way: the open goes on, deletes what this database left, as it
		// always did, and keeps what is its own.
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		for id in leftovers {
			assert!(!table_file(&sstables, id).exists(), "{what}: {id} is still there");
		}
		assert_eq!(files_with_extension(&sstables, "sst").len(), 2, "{what}");
		let mut expected = rounds('1', 0..KEYS);
		expected.extend(rounds('2', 0..KEYS / 2));
		read(&tree, &expected);
		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_database_that_is_not_encrypted_cleans_up_by_name_as_it_always_did() {
	let dir = TempDir::new().unwrap();
	let root = dir.path().join("plain");
	let opts = plain(&root);
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	write(&tree, '1', 0..KEYS).await;
	tree.flush().unwrap();
	tree.close().await.unwrap();
	let sstables = opts.sstable_dir();
	let own = files_with_extension(&sstables, "sst");

	// A sealed table of a database that is encrypted, garbage and an empty file: no name decides
	// anything but the number, and all of them go.
	let sealed_dir = dir.path().join("sealed");
	let sealed_opts = closed_database(&sealed_dir, CipherSuite::Aes256Gcm).await;
	let sealed = fs::read(&files_with_extension(&sealed_opts.sstable_dir(), "sst")[0]).unwrap();
	fs::write(table_file(&sstables, 9001), sealed).unwrap();
	fs::write(table_file(&sstables, 9002), b"garbage").unwrap();
	fs::write(table_file(&sstables, 9003), b"").unwrap();

	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	for id in [9001, 9002, 9003] {
		assert!(!table_file(&sstables, id).exists(), "{id} is still there");
	}
	assert_eq!(files_with_extension(&sstables, "sst"), own);
	read(&tree, &rounds('1', 0..KEYS));
	tree.close().await.unwrap();
}
