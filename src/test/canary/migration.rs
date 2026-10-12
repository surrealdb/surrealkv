//! The backup that a migration leaves, and an encrypted database that refuses to leave one.
//!
//! Opening a directory that holds the files of another store migrates it: the old files are moved
//! into a directory of their own inside the database, in the clear, and the data is written
//! again. For a database that is not encrypted that is what happens, and the control below shows
//! the marker in the backup and in the table that the migration wrote. For one that is meant to
//! be encrypted it would leave every key and value in the directory next to the files that are
//! sealed, so the open is refused before anything is read or moved: the marker is where it was
//! put and nowhere else, and not one file of the directory changes.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use super::files::DirImage;
use super::scan::{scan_dir, Markers};
use crate::tde::{CipherSuite, KeyManager, SoftwareKeyManager};
use crate::{Error, Mode, Options, Tree};

const DUMP: &str = "indxdb_dump.bin";

/// A dump of one entry, whose key and value are the markers `key` and `value`.
fn plant_dump(dir: &Path, markers: &Markers, key: usize, value: usize) -> Vec<u8> {
	let mut key_bytes = b"user:".to_vec();
	key_bytes.extend_from_slice(&markers.get(key).text());
	surrealkv_compat_indxdb::export_dump(
		dir.join(DUMP),
		&[(key_bytes.clone(), markers.get(value).text())],
	)
	.unwrap();
	key_bytes
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn a_migration_leaves_a_backup_in_the_clear_and_an_encrypted_database_refuses_to() {
	let mut markers = Markers::from_environment();
	let key = markers.mint("migrated-key");
	let value = markers.mint("migrated-value");
	let needles = markers.needles();
	let both = BTreeSet::from([key, value]);

	// The control: a database that is not encrypted migrates the dump. The backup holds both
	// markers, so does the table that the migration wrote, and the data is in the database.
	let plain = TempDir::new().unwrap();
	let key_bytes = plant_dump(plain.path(), &markers, key, value);
	let opts = Options {
		path: plain.path().to_path_buf(),
		..Default::default()
	};
	let tree = Tree::new(Arc::new(opts)).unwrap();
	{
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		assert_eq!(txn.get(key_bytes.as_slice()).unwrap(), Some(markers.get(value).text()));
	}
	tree.close().await.unwrap();
	let scan = scan_dir(plain.path(), &needles).unwrap();
	assert!(plain.path().join("_indxdb_backup").join(DUMP).exists());
	assert_eq!(scan.groups_in(&needles, &["_indxdb_backup"]), both, "{}", scan.describe(&needles));
	assert_eq!(scan.groups_in(&needles, &["sstables"]), both, "{}", scan.describe(&needles));

	// An encrypted database is refused, and the marker is only in the file that it was put in.
	for suite in [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305] {
		let dir = TempDir::new().unwrap();
		plant_dump(dir.path(), &markers, key, value);
		let before = DirImage::of(dir.path());
		let manager: Arc<dyn KeyManager> = Arc::new(SoftwareKeyManager::new([0x5A; 32]));
		let opts = Options {
			path: dir.path().to_path_buf(),
			..Default::default()
		}
		.with_encryption(manager, suite);
		let error = match Tree::new(Arc::new(opts)) {
			Ok(_) => panic!("{suite:?}: a store was migrated into an encrypted database"),
			Err(error) => error,
		};
		assert!(
			matches!(&error, Error::InvalidArgument(message)
				if message.contains("IndexedDB") && message.contains("encrypted")),
			"{suite:?}: {error:?}"
		);
		DirImage::of(dir.path()).assert_unchanged_since(&before, &format!("{suite:?}"));
		assert!(!dir.path().join("_indxdb_backup").exists(), "{suite:?}: a backup was made");

		let scan = scan_dir(dir.path(), &needles).unwrap();
		let files: BTreeSet<PathBuf> =
			scan.findings.iter().map(|finding| finding.file.clone()).collect();
		assert_eq!(files, BTreeSet::from([PathBuf::from(DUMP)]), "{suite:?}");
		assert_eq!(scan.groups(&needles), both, "{suite:?}");
	}
}
