//! The write-ahead log of a checkpoint. A checkpoint that this build takes has none, because it
//! flushes first, but one made by hand or by another build may hold segments, and a restore
//! replays them after it has replaced the files of the database. What the segments are is read
//! from their first bytes before anything is cleared: a segment of another database, of another
//! file, or of another mode is refused, and a refusal changes no file.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;

use super::encryption_checkpoint_tests::{copy_dir, tree_with_checkpoint};
use super::encryption_open_tests::{
	encrypted,
	encryption_error,
	files_with_extension,
	manager,
	options,
	plain,
	read,
	rounds,
	snapshot,
	write,
	KEYS,
	SUITES,
};
use crate::error::EncryptionError;
use crate::tde::{CipherSuite, KeyManager, SoftwareKeyManager};
use crate::Tree;

/// A sealed database in `root` that holds a few commits in its log, closed without flushing them:
/// the first segment of its log, with the bytes it holds.
async fn sealed_segment(root: &Path, suite: CipherSuite) -> (String, Vec<u8>) {
	let key_manager: Arc<dyn KeyManager> = Arc::new(SoftwareKeyManager::new([0x77; 32]));
	let opts = Arc::new(options(root).with_encryption(key_manager, suite));
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	write(&tree, '1', 0..4).await;
	tree.close().await.unwrap();
	let segments = files_with_extension(&opts.wal_dir(), "wal");
	let first = segments.first().expect("the log has a segment");
	(first.file_name().unwrap().to_string_lossy().into_owned(), fs::read(first).unwrap())
}

/// A copy of the checkpoint `source` with the file `name` of `bytes` in its log.
fn with_segment(
	checkpoints: &Path,
	source: &Path,
	copy: &str,
	name: &str,
	bytes: &[u8],
) -> PathBuf {
	let target = checkpoints.join(copy);
	copy_dir(source, &target);
	let wal = target.join("wal");
	fs::create_dir_all(&wal).unwrap();
	fs::write(wal.join(name), bytes).unwrap();
	target
}

/// The bytes of a record in the clear: a checksum that nothing checks here, a length, the type of
/// a complete record and a payload. What a database that is not encrypted writes to its log, as
/// far as its first bytes are concerned.
fn segment_in_the_clear() -> Vec<u8> {
	vec![0x12, 0x34, 0x56, 0x78, 0x00, 0x03, 0x01, b'a', b'b', b'c']
}

#[tokio::test]
async fn a_segment_of_the_wal_of_a_checkpoint_that_is_not_of_this_database_is_refused_before_anything_is_cleared(
) {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let checkpoints = TempDir::new().unwrap();
		let live_root = dir.path().join("live");
		let live_cp = checkpoints.path().join("live");
		let live = tree_with_checkpoint(&encrypted(&live_root, manager(), suite), &live_cp).await;

		// A segment of another database that has its own identity, and a segment of this one under
		// the number of another: both are sealed and both open with the key of this database.
		let (foreign_name, foreign) = sealed_segment(&dir.path().join("foreign"), suite).await;
		let own = {
			let opts = encrypted(&live_root, manager(), suite);
			let segments = files_with_extension(&opts.wal_dir(), "wal");
			fs::read(segments.last().expect("the live log has a segment")).unwrap()
		};

		let before = snapshot(&live_root);
		let expected = rounds('1', 0..KEYS);
		let cases: Vec<(&str, String, Vec<u8>, EncryptionError)> = vec![
			("foreign", foreign_name, foreign, EncryptionError::DbIdMismatch),
			("renumbered", format!("{:020}.wal", 4242), own, EncryptionError::FileIdMismatch),
			(
				"clear",
				format!("{:020}.wal", 4243),
				segment_in_the_clear(),
				EncryptionError::PlaintextInEncryptedDb,
			),
		];
		for (what, name, bytes, wanted) in cases {
			let mixed = with_segment(checkpoints.path(), &live_cp, what, &name, &bytes);
			let err = live.restore_from_checkpoint(&mixed).unwrap_err();
			assert_eq!(encryption_error(err, what), wanted, "{suite:?}: {what}");
			assert!(
				snapshot(&live_root) == before,
				"{suite:?}: {what}: a refused restore changed the files"
			);
			read(&live, &expected);
			fs::remove_dir_all(&mixed).unwrap();
		}
		live.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_sealed_segment_in_the_wal_of_a_checkpoint_of_a_database_that_is_not_encrypted_is_refused(
) {
	let dir = TempDir::new().unwrap();
	let checkpoints = TempDir::new().unwrap();
	let live_root = dir.path().join("live");
	let live_cp = checkpoints.path().join("live");
	let live = tree_with_checkpoint(&plain(&live_root), &live_cp).await;

	let (name, sealed) =
		sealed_segment(&dir.path().join("sealed"), CipherSuite::XChaCha20Poly1305).await;
	let mixed = with_segment(checkpoints.path(), &live_cp, "mixed", &name, &sealed);
	let before = snapshot(&live_root);
	let err = live.restore_from_checkpoint(&mixed).unwrap_err();
	assert_eq!(encryption_error(err, "restore"), EncryptionError::NotConfigured);
	assert!(snapshot(&live_root) == before, "a refused restore changed the files");
	read(&live, &rounds('1', 0..KEYS));
	live.close().await.unwrap();
}
