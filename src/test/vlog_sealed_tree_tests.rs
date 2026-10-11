//! A tree whose value log is sealed: what the commit path and the flush path separate into the
//! log, what a checkpoint copies and a restore brings back, and that none of the keys and values
//! are in any file of the log.
//!
//! The context is put in the options directly, as the engine will when it has read a database's
//! descriptor, so only the value log is sealed in these trees: the tables and the WAL are not
//! covered here, and what is looked at is the directory of the log. The data is kept small: AEAD
//! runs at a few MB/s in the debug profile.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use crate::error::{EncryptionError, Error};
use crate::tde::{CipherSuite, DbId, EncryptionContext, SoftwareKeyManager};
use crate::{CompressionType, Mode, Options, Tree};

const SUITES: [CipherSuite; 2] = [CipherSuite::Aes256Gcm, CipherSuite::XChaCha20Poly1305];

const COMPRESSIONS: [CompressionType; 2] =
	[CompressionType::None, CompressionType::SnappyCompression];

const KEY_MARKER: &[u8] = b"CANARY-KEY-";
const VALUE_MARKER: &[u8] = b"CANARY-VALUE-";

fn context(suite: CipherSuite) -> Arc<EncryptionContext> {
	Arc::new(EncryptionContext::new(
		Arc::new(SoftwareKeyManager::new([0x11; 32])),
		DbId::from_bytes([0xD0; 16]),
		suite,
	))
}

/// Keys and values that carry a marker each and are long enough to be moved to the log.
fn canaries(range: Range<usize>) -> Vec<(Vec<u8>, Vec<u8>)> {
	range
		.map(|i| {
			let key = format!("CANARY-KEY-{i:03}-ae07c1").into_bytes();
			let value = format!("CANARY-VALUE-{i:03}-9d2b44-").repeat(6).into_bytes();
			(key, value)
		})
		.collect()
}

fn open_tree(
	path: &Path,
	ctx: Option<&Arc<EncryptionContext>>,
	compression: CompressionType,
	threshold: usize,
) -> Tree {
	try_open_tree(path, ctx, compression, threshold).unwrap()
}

fn try_open_tree(
	path: &Path,
	ctx: Option<&Arc<EncryptionContext>>,
	compression: CompressionType,
	threshold: usize,
) -> crate::Result<Tree> {
	let mut opts = Options {
		path: path.to_path_buf(),
		enable_vlog: true,
		vlog_value_threshold: threshold,
		vlog_max_file_size: 4096,
		flush_on_close: false,
		compression_per_level: vec![compression],
		..Default::default()
	};
	opts.encryption_context = ctx.cloned();
	Tree::new(Arc::new(opts))
}

async fn commit_all(tree: &Tree, entries: &[(Vec<u8>, Vec<u8>)]) {
	let mut txn = tree.begin().unwrap();
	for (key, value) in entries {
		txn.set(key, value).unwrap();
	}
	txn.commit().await.unwrap();
}

fn assert_reads(tree: &Tree, entries: &[(Vec<u8>, Vec<u8>)], what: &str) {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	for (key, value) in entries {
		assert_eq!(
			txn.get(key).unwrap().as_ref(),
			Some(value),
			"{what}: {}",
			String::from_utf8_lossy(key)
		);
	}
}

/// The files of the value log of the database at `path`, by name, with their bytes.
fn vlog_files(path: &Path) -> BTreeMap<String, Vec<u8>> {
	std::fs::read_dir(path.join("vlog"))
		.unwrap()
		.map(|entry| {
			let entry = entry.unwrap();
			(entry.file_name().to_string_lossy().into_owned(), std::fs::read(entry.path()).unwrap())
		})
		.collect()
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
	haystack.windows(needle.len()).any(|window| window == needle)
}

/// Every file is a sealed one, and no marker of any key or value is in any of them.
fn assert_sealed_and_free_of_markers(files: &BTreeMap<String, Vec<u8>>, what: &str) {
	for (name, bytes) in files {
		assert_eq!(bytes[..4], *b"VLOG", "{what}: {name}");
		assert_eq!(bytes[4..6], [0x80, 0x01], "{what}: {name} is not a sealed file");
		assert!(!contains(bytes, b"CANARY"), "{what}: {name} holds a marker");
		assert!(!contains(bytes, KEY_MARKER) && !contains(bytes, VALUE_MARKER), "{what}: {name}");
	}
}

fn salts(files: &BTreeMap<String, Vec<u8>>) -> BTreeSet<Vec<u8>> {
	files.values().map(|bytes| bytes[75..107].to_vec()).collect()
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn the_values_a_commit_separates_are_sealed_and_no_key_or_value_is_in_the_log() {
	for suite in SUITES {
		for compression in COMPRESSIONS {
			let dir = TempDir::new().unwrap();
			let ctx = context(suite);
			let first = canaries(0..6);
			let tree = open_tree(dir.path(), Some(&ctx), compression, 64);
			commit_all(&tree, &first).await;
			// From the memtable, whose entries point into the log, and then from a table.
			assert_reads(&tree, &first, "memtable");
			tree.flush().unwrap();
			assert_reads(&tree, &first, "table");
			let files = vlog_files(dir.path());
			assert!(!files.is_empty(), "the values went to the log");
			assert_sealed_and_free_of_markers(&files, "first run");
			tree.close().await.unwrap();
			drop(tree);

			// A reopen reads the files of the first run, leaves them as they are, and writes the
			// next values to a file of its own, under a salt of its own.
			let tree = open_tree(dir.path(), Some(&ctx), compression, 64);
			assert_reads(&tree, &first, "reopened");
			let second = canaries(6..9);
			commit_all(&tree, &second).await;
			tree.flush().unwrap();
			assert_reads(&tree, &first, "second run, first values");
			assert_reads(&tree, &second, "second run, second values");
			let after = vlog_files(dir.path());
			for (name, bytes) in &files {
				assert_eq!(after.get(name), Some(bytes), "{suite:?}: the reopen changed {name}");
			}
			assert!(after.len() > files.len(), "the second run wrote a file of its own");
			assert_eq!(salts(&after).len(), after.len(), "no two files share a salt");
			assert_sealed_and_free_of_markers(&after, "second run");
			tree.close().await.unwrap();
		}
	}
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn the_markers_are_in_a_plaintext_log_so_that_their_absence_in_a_sealed_one_means_something()
{
	for compression in COMPRESSIONS {
		let dir = TempDir::new().unwrap();
		let entries = canaries(0..6);
		let tree = open_tree(dir.path(), None, compression, 64);
		commit_all(&tree, &entries).await;
		tree.flush().unwrap();
		let files = vlog_files(dir.path());
		assert!(!files.is_empty());
		for (name, bytes) in &files {
			assert_eq!(bytes[4..6], [0, 1], "{name} is a plaintext file");
		}
		// Every key and every value is there in the clear.
		for (key, value) in &entries {
			assert!(files.values().any(|bytes| contains(bytes, key)), "{key:?}");
			assert!(files.values().any(|bytes| contains(bytes, value)), "{value:?}");
		}
		assert!(files.values().any(|bytes| contains(bytes, KEY_MARKER)));
		assert!(files.values().any(|bytes| contains(bytes, VALUE_MARKER)));
		tree.close().await.unwrap();
	}
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn the_values_a_flush_separates_are_sealed_like_the_ones_a_commit_separates() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite);
		let entries = canaries(0..5);

		// A threshold above every value: nothing is separated when the batch is committed, and
		// the values are in the WAL as they are.
		let tree = open_tree(dir.path(), Some(&ctx), CompressionType::None, 1 << 30);
		commit_all(&tree, &entries).await;
		assert!(vlog_files(dir.path()).is_empty(), "nothing went to the log");
		tree.close().await.unwrap();
		drop(tree);

		// A threshold below them: the flush of the memtable that the WAL rebuilds moves them.
		let tree = open_tree(dir.path(), Some(&ctx), CompressionType::None, 64);
		tree.flush().unwrap();
		assert_reads(&tree, &entries, "after the flush");
		let files = vlog_files(dir.path());
		assert!(!files.is_empty(), "{suite:?}: the flush separated the values");
		assert_sealed_and_free_of_markers(&files, "flush");
		tree.close().await.unwrap();
		drop(tree);

		let tree = open_tree(dir.path(), Some(&ctx), CompressionType::None, 64);
		assert_reads(&tree, &entries, "reopened");
		tree.close().await.unwrap();
	}
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_copies_sealed_files_as_they_are_and_a_restore_brings_them_back() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let db = dir.path().join("db");
		let checkpoint = dir.path().join("checkpoint");
		let ctx = context(suite);
		let first = canaries(0..6);
		let tree = open_tree(&db, Some(&ctx), CompressionType::None, 64);
		commit_all(&tree, &first).await;
		tree.create_checkpoint(&checkpoint).unwrap();

		// The copy needs no key and changes nothing: it is the files of the log, byte for byte.
		let copy = vlog_files(&checkpoint);
		assert!(!copy.is_empty());
		assert_eq!(copy, vlog_files(&db), "{suite:?}");
		assert_sealed_and_free_of_markers(&copy, "checkpoint");

		// More is committed after the checkpoint, and then the checkpoint is restored.
		commit_all(&tree, &canaries(6..9)).await;
		tree.restore_from_checkpoint(&checkpoint).unwrap();
		assert_reads(&tree, &first, "restored");
		let restored = vlog_files(&db);
		for (name, bytes) in &copy {
			assert_eq!(restored.get(name), Some(bytes), "{suite:?}: {name}");
		}

		// The restored files are only read: the next values go to a file that is new, whose id is
		// past every restored file and whose salt is none of theirs.
		let third = canaries(9..12);
		commit_all(&tree, &third).await;
		tree.flush().unwrap();
		assert_reads(&tree, &first, "after the restore, first values");
		assert_reads(&tree, &third, "after the restore, new values");
		let after = vlog_files(&db);
		for (name, bytes) in &copy {
			assert_eq!(after.get(name), Some(bytes), "{suite:?}: {name} was changed");
		}
		assert!(after.len() > copy.len(), "a file of its own");
		assert_eq!(salts(&after).len(), after.len(), "no two files share a salt");
		assert_sealed_and_free_of_markers(&after, "after the restore");
		tree.close().await.unwrap();
	}
}

/// What a reader of the files meets once the log is sealed: a byte that changed is an error, for
/// the one entry that holds it, and never a value that is not the one that was written.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_changed_byte_in_a_sealed_log_fails_that_read_and_returns_no_wrong_value() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite);
		let entries = canaries(0..3);
		let tree = open_tree(dir.path(), Some(&ctx), CompressionType::None, 64);
		commit_all(&tree, &entries).await;
		tree.flush().unwrap();
		tree.close().await.unwrap();
		drop(tree);

		// One byte in the ciphertext of the first entry of the first file, which starts where the
		// header ends, at 127, and is longer than 40 bytes: a value of 144 bytes and a key.
		let path = dir.path().join("vlog").join("00000000000000000001.vlog");
		let mut bytes = std::fs::read(&path).unwrap();
		assert!(bytes.len() > 127 + 3 * 144, "three values in one file");
		bytes[127 + 40] ^= 0x01;
		std::fs::write(&path, &bytes).unwrap();

		let tree = open_tree(dir.path(), Some(&ctx), CompressionType::None, 64);
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		let mut failed = 0;
		for (key, value) in &entries {
			match txn.get(key) {
				Ok(found) => assert_eq!(found.as_ref(), Some(value), "{suite:?}: a wrong value"),
				Err(_) => failed += 1,
			}
		}
		assert_eq!(failed, 1, "{suite:?}: the entry that holds the byte, and no other");
		drop(txn);
		tree.close().await.unwrap();
	}
}

/// A key that does not fit is an error of its kind at the tree, and the files of the log are left
/// as they were.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_tree_opened_with_a_key_that_does_not_fit_is_refused_and_leaves_the_log_alone() {
	for suite in SUITES {
		let dir = TempDir::new().unwrap();
		let ctx = context(suite);
		let entries = canaries(0..3);
		let tree = open_tree(dir.path(), Some(&ctx), CompressionType::None, 64);
		commit_all(&tree, &entries).await;
		tree.flush().unwrap();
		tree.close().await.unwrap();
		drop(tree);
		let before = vlog_files(dir.path());
		assert!(!before.is_empty());

		let wrong = Arc::new(EncryptionContext::new(
			Arc::new(SoftwareKeyManager::new([0x99; 32])),
			DbId::from_bytes([0xD0; 16]),
			suite,
		));
		let refused = try_open_tree(dir.path(), Some(&wrong), CompressionType::None, 64).err();
		assert!(
			matches!(refused, Some(Error::Encryption(EncryptionError::WrongKey))),
			"{suite:?}: {refused:?}"
		);
		assert_eq!(vlog_files(dir.path()), before, "{suite:?}: the log was changed");

		// Without any key at all the files are the other kind's.
		let none = try_open_tree(dir.path(), None, CompressionType::None, 64).err();
		assert!(
			matches!(none, Some(Error::Encryption(EncryptionError::NotConfigured))),
			"{suite:?}: {none:?}"
		);
		assert_eq!(vlog_files(dir.path()), before, "{suite:?}: the log was changed");
	}
}
