//! Checkpoints and restores of databases that are, or are not, encrypted: a checkpoint is the
//! files of its database as they are, a restore reads from the start of the checkpoint's manifest
//! and value-log files whether it is of this database before it clears anything, and a refusal
//! changes no file.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;

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
	ROOT,
	SUITES,
};
use crate::error::EncryptionError;
use crate::tde::{
	describe_manifest_file,
	is_sealed_manifest,
	CipherSuite,
	DbId,
	EncryptionContext,
	KeyManager,
	SoftwareKeyManager,
};
use crate::{Error, Options, Tree};

/// A copy of the directory `from` at `to`: the files are copied and not linked, so changing the
/// copy never changes the database that a checkpoint linked its tables from.
fn copy_dir(from: &Path, to: &Path) {
	fs::create_dir_all(to).unwrap();
	for entry in fs::read_dir(from).unwrap().flatten() {
		let target = to.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &target);
		} else {
			fs::copy(entry.path(), target).unwrap();
		}
	}
}

/// A tree of `opts` that holds `KEYS` keys of round 1 in tables, and a checkpoint of it in
/// `checkpoint`.
async fn tree_with_checkpoint(opts: &Arc<Options>, checkpoint: &Path) -> Tree {
	let tree = Tree::new(Arc::clone(opts)).unwrap();
	write(&tree, '1', 0..KEYS).await;
	tree.flush().unwrap();
	tree.create_checkpoint(checkpoint).unwrap();
	tree
}

/// The path of file `id` of the value log in the checkpoint at `checkpoint`.
fn vlog_path(checkpoint: &Path, id: u32) -> PathBuf {
	checkpoint.join("vlog").join(format!("{id:020}.vlog"))
}

/// The ids of the value-log files of a checkpoint, in order.
fn vlog_ids(checkpoint: &Path) -> Vec<u32> {
	let mut ids: Vec<u32> = files_with_extension(&checkpoint.join("vlog"), "vlog")
		.iter()
		.map(|path| path.file_stem().unwrap().to_string_lossy().parse().unwrap())
		.collect();
	ids.sort_unstable();
	ids
}

#[tokio::test]
async fn a_checkpoint_of_an_encrypted_database_holds_the_sealed_manifest_as_it_is_and_restores_with_the_key(
) {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let checkpoints = TempDir::new().unwrap();
		let checkpoint = checkpoints.path().join("checkpoint");
		let opts = encrypted(&dir.path().join("db"), manager(), suite);
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		write(&tree, '1', 0..KEYS).await;
		tree.flush().unwrap();
		write(&tree, '2', 0..KEYS / 2).await;
		tree.create_checkpoint(&checkpoint).unwrap();

		// The manifest is copied as it is, sealed, and says what the live one does. The value-log
		// files are copied as they are too.
		let name = opts.manifest_file_path(0).file_name().unwrap().to_owned();
		let live = fs::read(opts.manifest_file_path(0)).unwrap();
		let copied = fs::read(checkpoint.join("manifest").join(&name)).unwrap();
		assert!(is_sealed_manifest(&copied), "{suite:?}");
		assert!(live == copied, "{suite:?}: the manifest was not copied as it is");
		let described = describe_manifest_file(checkpoint.join("manifest").join(&name)).unwrap();
		assert_eq!(described.suite, Some(suite));
		assert_eq!(
			described.db_id,
			tree.core.inner.opts.encryption_context().map(|context| context.db_id())
		);
		let logs = files_with_extension(&opts.vlog_dir(), "vlog");
		assert!(!logs.is_empty());
		for log in logs {
			let copy = checkpoint.join("vlog").join(log.file_name().unwrap());
			assert!(fs::read(&log).unwrap() == fs::read(copy).unwrap(), "{suite:?}: {log:?}");
		}

		// More is written, then the checkpoint is restored with the key of the database.
		write(&tree, '3', 0..KEYS).await;
		tree.restore_from_checkpoint(&checkpoint).unwrap();
		let mut expected = rounds('1', 0..KEYS);
		expected.extend(rounds('2', 0..KEYS / 2));
		read(&tree, &expected);

		// And the database goes on, and opens again with the key.
		write(&tree, '4', KEYS..KEYS + 4).await;
		tree.flush().unwrap();
		expected.extend(rounds('4', KEYS..KEYS + 4));
		read(&tree, &expected);
		tree.close().await.unwrap();
		let tree = Tree::new(encrypted(&dir.path().join("db"), manager(), suite)).unwrap();
		read(&tree, &expected);
		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_checkpoint_of_another_kind_or_database_is_refused_before_anything_is_cleared() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let checkpoints = TempDir::new().unwrap();
		let live_root = dir.path().join("live");
		let live_opts = encrypted(&live_root, manager(), suite);
		let live = Tree::new(Arc::clone(&live_opts)).unwrap();
		write(&live, '1', 0..KEYS).await;
		live.flush().unwrap();

		// A database that is not encrypted, and one that is, with the same key and suite and an
		// identity of its own.
		let plain_cp = checkpoints.path().join("plain");
		let other = tree_with_checkpoint(&plain(&dir.path().join("plain")), &plain_cp).await;
		other.close().await.unwrap();
		let foreign_cp = checkpoints.path().join("foreign");
		let foreign_opts = encrypted(&dir.path().join("foreign"), manager(), suite);
		let other = tree_with_checkpoint(&foreign_opts, &foreign_cp).await;
		other.close().await.unwrap();

		let before = snapshot(&live_root);
		let expected = rounds('1', 0..KEYS);
		for (checkpoint, wanted) in [
			(&plain_cp, EncryptionError::NotEncrypted),
			(&foreign_cp, EncryptionError::DbIdMismatch),
		] {
			let err = live.restore_from_checkpoint(checkpoint).unwrap_err();
			assert_eq!(encryption_error(err, "restore"), wanted, "{suite:?}");
			assert!(
				snapshot(&live_root) == before,
				"{suite:?}: a refused restore changed the files"
			);
			read(&live, &expected);
		}
		live.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_checkpoint_of_the_same_database_in_another_suite_is_refused() {
	for (suite, other) in [
		(CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305),
		(CipherSuite::XChaCha20Poly1305, CipherSuite::Aes256Gcm),
	] {
		let dir = TempDir::new().unwrap();
		let checkpoints = TempDir::new().unwrap();
		// The same identity, which the engine would only ever draw once, in two suites.
		let in_suite = |root: &str, suite: CipherSuite| {
			let key_manager: Arc<dyn KeyManager> = manager();
			let context =
				Arc::new(EncryptionContext::new(key_manager, DbId::from_bytes([0xD5; 16]), suite));
			Arc::new(options(&dir.path().join(root)).with_encryption_context(context))
		};

		let source_cp = checkpoints.path().join("source");
		let source = tree_with_checkpoint(&in_suite("source", other), &source_cp).await;
		source.close().await.unwrap();

		let live_root = dir.path().join("live");
		let live = Tree::new(in_suite("live", suite)).unwrap();
		write(&live, '1', 0..KEYS).await;
		live.flush().unwrap();
		let before = snapshot(&live_root);
		let err = live.restore_from_checkpoint(&source_cp).unwrap_err();
		assert_eq!(encryption_error(err, "restore"), EncryptionError::SuiteMismatch, "{suite:?}");
		assert!(snapshot(&live_root) == before, "{suite:?}");
		read(&live, &rounds('1', 0..KEYS));
		live.close().await.unwrap();
	}
}

#[tokio::test]
async fn an_encrypted_checkpoint_is_not_restored_into_a_database_that_is_not_encrypted() {
	let dir = TempDir::new().unwrap();
	let checkpoints = TempDir::new().unwrap();
	let sealed_cp = checkpoints.path().join("sealed");
	let source = tree_with_checkpoint(
		&encrypted(&dir.path().join("source"), manager(), CipherSuite::Aes256Gcm),
		&sealed_cp,
	)
	.await;
	source.close().await.unwrap();

	let live_root = dir.path().join("live");
	let live = Tree::new(plain(&live_root)).unwrap();
	write(&live, '1', 0..KEYS).await;
	live.flush().unwrap();
	let before = snapshot(&live_root);
	let err = live.restore_from_checkpoint(&sealed_cp).unwrap_err();
	assert_eq!(encryption_error(err, "restore"), EncryptionError::NotConfigured);
	assert!(snapshot(&live_root) == before);
	read(&live, &rounds('1', 0..KEYS));
	live.close().await.unwrap();
}

#[tokio::test]
async fn the_value_log_of_a_checkpoint_is_checked_from_its_first_bytes_before_anything_is_cleared()
{
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let checkpoints = TempDir::new().unwrap();
		let live_root = dir.path().join("live");
		let live_cp = checkpoints.path().join("live");
		let live = tree_with_checkpoint(&encrypted(&live_root, manager(), suite), &live_cp).await;

		// The value log of a plaintext database, and of another encrypted one.
		let plain_cp = checkpoints.path().join("plain");
		let plain_tree = tree_with_checkpoint(&plain(&dir.path().join("plain")), &plain_cp).await;
		let foreign_cp = checkpoints.path().join("foreign");
		let foreign_opts = encrypted(&dir.path().join("foreign"), manager(), suite);
		let foreign = tree_with_checkpoint(&foreign_opts, &foreign_cp).await;
		foreign.close().await.unwrap();

		// A checkpoint that is the live one but for its value log, which is another's.
		let with_log_of = |name: &str, log: &Path| {
			let copy = checkpoints.path().join(name);
			copy_dir(&live_cp, &copy);
			fs::remove_dir_all(copy.join("vlog")).unwrap();
			copy_dir(&log.join("vlog"), &copy.join("vlog"));
			copy
		};
		let before = snapshot(&live_root);
		let expected = rounds('1', 0..KEYS);
		for (log_of, wanted) in [
			(&plain_cp, EncryptionError::PlaintextInEncryptedDb),
			(&foreign_cp, EncryptionError::DbIdMismatch),
		] {
			let mixed = with_log_of("mixed", log_of);
			let err = live.restore_from_checkpoint(&mixed).unwrap_err();
			assert_eq!(encryption_error(err, "restore"), wanted, "{suite:?}");
			assert!(
				snapshot(&live_root) == before,
				"{suite:?}: a refused restore changed the files"
			);
			read(&live, &expected);
			fs::remove_dir_all(&mixed).unwrap();
		}
		live.close().await.unwrap();

		// The other way round: the files of a sealed log in the checkpoint of a database that is
		// not encrypted.
		let plain_root = dir.path().join("plain");
		let mixed = with_log_of("mixed-plain", &live_cp);
		let plain_manifest = plain_cp.join("manifest");
		fs::remove_dir_all(mixed.join("manifest")).unwrap();
		copy_dir(&plain_manifest, &mixed.join("manifest"));
		fs::remove_dir_all(mixed.join("sstables")).unwrap();
		copy_dir(&plain_cp.join("sstables"), &mixed.join("sstables"));
		let before = snapshot(&plain_root);
		let err = plain_tree.restore_from_checkpoint(&mixed).unwrap_err();
		assert_eq!(encryption_error(err, "restore"), EncryptionError::NotConfigured, "{suite:?}");
		assert!(snapshot(&plain_root) == before, "{suite:?}");
		plain_tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_checkpoint_that_was_taken_while_the_value_log_was_written_is_valid() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let checkpoints = TempDir::new().unwrap();
		let root = dir.path().join("db");
		let cp = checkpoints.path().join("cp");
		let live = tree_with_checkpoint(&encrypted(&root, manager(), suite), &cp).await;
		let expected = rounds('1', 0..KEYS);
		let newest = *vlog_ids(&cp).last().expect("the values went to the value log");

		// The file that the writer appends to is copied as it is at that moment: here with the
		// start of an entry after the last one that a table points to.
		let tail = checkpoints.path().join("tail");
		copy_dir(&cp, &tail);
		let mut bytes = fs::read(vlog_path(&tail, newest)).unwrap();
		bytes.extend_from_slice(&[0xAB; 11]);
		fs::write(vlog_path(&tail, newest), bytes).unwrap();
		live.restore_from_checkpoint(&tail).unwrap();
		read(&live, &expected);

		// And a file that was created and had not got its header yet, which holds no entry: the
		// start of a header, then nothing at all.
		let cut = checkpoints.path().join("cut");
		copy_dir(&cp, &cut);
		let header = fs::read(vlog_path(&cut, newest)).unwrap();
		fs::write(vlog_path(&cut, newest + 1), &header[..100]).unwrap();
		live.restore_from_checkpoint(&cut).unwrap();
		read(&live, &expected);
		let empty = checkpoints.path().join("empty");
		copy_dir(&cp, &empty);
		fs::write(vlog_path(&empty, newest + 1), b"").unwrap();
		live.restore_from_checkpoint(&empty).unwrap();
		read(&live, &expected);

		// The database goes on after any of them.
		write(&live, '2', KEYS..KEYS + 4).await;
		live.flush().unwrap();
		live.close().await.unwrap();

		// Only the newest file can be that: one with a newer file after it is a file that is
		// damaged, and the checkpoint is refused as one that is not usable, with nothing cleared.
		let live = Tree::new(encrypted(&root, manager(), suite)).unwrap();
		let before = snapshot(&root);
		let damaged = checkpoints.path().join("damaged");
		copy_dir(&cp, &damaged);
		fs::write(vlog_path(&damaged, newest + 1), &header[..100]).unwrap();
		fs::write(vlog_path(&damaged, newest + 2), &header[..100]).unwrap();
		let err = live.restore_from_checkpoint(&damaged).unwrap_err();
		assert!(
			matches!(&err, Error::InvalidArgument(message) if message.contains("not a usable checkpoint")),
			"{suite:?}: {err:?}"
		);
		assert!(snapshot(&root) == before, "{suite:?}: a refused restore changed the files");
		live.close().await.unwrap();
	}
}

#[tokio::test]
async fn a_table_that_does_not_open_for_want_of_a_key_is_a_key_error_and_not_a_damaged_checkpoint()
{
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let checkpoints = TempDir::new().unwrap();
		let root = dir.path().join("db");
		let cp = checkpoints.path().join("cp");

		// Written under key 1, then under key 2, with no value log: the manifest of the checkpoint
		// is sealed under key 2, and one of its tables under key 1.
		let km = Arc::new(SoftwareKeyManager::new(ROOT));
		let opts = Arc::new(
			Options {
				enable_vlog: false,
				..options(&root)
			}
			.with_encryption(Arc::clone(&km) as Arc<dyn KeyManager>, suite),
		);
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		write(&tree, '1', 0..KEYS).await;
		tree.flush().unwrap();
		km.add_key(2, [0x22; 32]).unwrap();
		km.set_active_key_id(2).unwrap();
		write(&tree, '2', 0..4).await;
		tree.create_checkpoint(&cp).unwrap();
		let mut expected = rounds('1', 0..KEYS);
		expected.extend(rounds('2', 0..4));
		read(&tree, &expected);

		// Key 1 is retired while the database runs: its tables are open and are read, and the
		// checkpoint cannot be opened.
		km.retire_key(1).unwrap();
		let before = snapshot(&root);
		let err = tree.restore_from_checkpoint(&cp).unwrap_err();
		assert_eq!(encryption_error(err, "restore"), EncryptionError::KeyRetired, "{suite:?}");
		assert!(snapshot(&root) == before, "{suite:?}: a refused restore changed the files");
		read(&tree, &expected);
		tree.close().await.unwrap();
	}
}
