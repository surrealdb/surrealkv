//! The on-disk format markers: `TableFormat::LSMV2`, manifest version 2, and the gate that keeps
//! this engine from reading a table of LSMV1, the format of SurrealKV up to v0.21.4 (16-byte key
//! suffix), as if it were its own (8-byte suffix). A table of LSMV1 is converted by `Tree::new`,
//! with the help of `surrealkv-compat-v1`, and refused everywhere else.
//!
//! The LSMV1 tables come from real databases written by SurrealKV v0.21.4 (see
//! `tests/fixtures/legacy`), because a hand-built table would only prove that the gate agrees with
//! its own idea of the format.

use std::fs;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use crate::levels::{
	MANIFEST_FORMAT_VERSION,
	MANIFEST_FORMAT_VERSION_V1,
	MANIFEST_FORMAT_VERSION_V2,
};
use crate::sstable::table::{refuse_legacy_table, Table, TableFormat, TABLE_FULL_FOOTER_LENGTH};
use crate::vfs::File;
use crate::{Error, Options, Tree};

const KEYS: usize = 60;

fn key(i: usize) -> Vec<u8> {
	format!("key_{i:04}").into_bytes()
}

fn value(i: usize) -> Vec<u8> {
	format!("value_{i:04}_{}", "y".repeat(48)).into_bytes()
}

fn options(path: &Path) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		..Default::default()
	})
}

/// Writes `KEYS` keys and closes the database cleanly, which flushes one SSTable.
async fn write_database(opts: &Arc<Options>) {
	let tree = Tree::new(Arc::clone(opts)).unwrap();
	let mut txn = tree.begin().unwrap();
	for i in 0..KEYS {
		txn.set(key(i), value(i)).unwrap();
	}
	txn.commit().await.unwrap();
	tree.close().await.unwrap();
}

fn assert_all_keys(tree: &Tree) {
	let txn = tree.begin().unwrap();
	for i in 0..KEYS {
		assert_eq!(txn.get(key(i)).unwrap(), Some(value(i)), "key {i}");
	}
}

fn sstables(opts: &Options) -> Vec<PathBuf> {
	let mut paths: Vec<PathBuf> = fs::read_dir(opts.sstable_dir())
		.unwrap()
		.flatten()
		.map(|e| e.path())
		.filter(|p| p.extension().is_some_and(|e| e == "sst"))
		.collect();
	paths.sort();
	paths
}

fn open_file(path: &Path) -> (Arc<dyn File>, usize) {
	let file = std::fs::File::open(path).unwrap();
	let size = file.metadata().unwrap().len() as usize;
	(Arc::new(file), size)
}

fn footer_format_byte(path: &Path) -> u8 {
	let bytes = fs::read(path).unwrap();
	bytes[bytes.len() - TABLE_FULL_FOOTER_LENGTH]
}

/// Overwrites the footer's format byte. The footer carries a magic number but no checksum.
fn set_footer_format_byte(path: &Path, format: u8) {
	let len = fs::metadata(path).unwrap().len();
	let mut file = fs::OpenOptions::new().write(true).open(path).unwrap();
	file.seek(SeekFrom::Start(len - TABLE_FULL_FOOTER_LENGTH as u64)).unwrap();
	file.write_all(&[format]).unwrap();
	file.sync_all().unwrap();
}

fn manifest_version(opts: &Options) -> u16 {
	let bytes = fs::read(opts.manifest_file_path(0)).unwrap();
	u16::from_be_bytes([bytes[0], bytes[1]])
}

fn set_manifest_version(opts: &Options, version: u16) {
	let path = opts.manifest_file_path(0);
	let mut bytes = fs::read(&path).unwrap();
	bytes[..2].copy_from_slice(&version.to_be_bytes());
	fs::write(&path, bytes).unwrap();
}

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

/// A copy of a database written by SurrealKV v0.21.4.
fn legacy_database(name: &str) -> (TempDir, Arc<Options>) {
	let source = Path::new(env!("CARGO_MANIFEST_DIR"))
		.join("tests/fixtures/legacy/v0.21.4")
		.join(name)
		.join("db");
	let dir = TempDir::new().unwrap();
	copy_dir(&source, dir.path());
	let opts = options(dir.path());
	(dir, opts)
}

#[test(tokio::test)]
async fn a_new_database_is_stamped_lsmv2_with_flat_keys_and_a_v2_manifest() {
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path());
	write_database(&opts).await;

	let tables = sstables(&opts);
	assert!(!tables.is_empty(), "expected an SSTable on disk");
	for (id, path) in tables.iter().enumerate() {
		assert_eq!(footer_format_byte(path), TableFormat::LSMV2 as u8);
		let (file, size) = open_file(path);
		// Opens, which a table with another layout of keys does not.
		let table = Table::new(id as u64 + 1, Arc::clone(&opts), file, size as u64).unwrap();
		assert_eq!(table.format, TableFormat::LSMV2);
	}
	assert_eq!(manifest_version(&opts), MANIFEST_FORMAT_VERSION_V2);
	assert_eq!(MANIFEST_FORMAT_VERSION, MANIFEST_FORMAT_VERSION_V2);
}

/// What is refused is the layout of the keys, which is what makes a table LSMV1: a table with flat
/// keys that was stamped LSMV1 by a development build of this engine, before the format had a
/// name of its own, is read as it always was.
#[test(tokio::test)]
async fn a_flat_table_stamped_lsmv1_by_a_development_build_still_opens() {
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path());
	write_database(&opts).await;
	for (id, path) in sstables(&opts).iter().enumerate() {
		set_footer_format_byte(path, TableFormat::LSMV1 as u8);
		let (file, size) = open_file(path);
		refuse_legacy_table(id as u64 + 1, &opts, file, size as u64).unwrap();
	}
	set_manifest_version(&opts, MANIFEST_FORMAT_VERSION_V1);

	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	assert_all_keys(&tree);
	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn the_next_write_upgrades_a_v1_manifest_to_v2() {
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path());
	write_database(&opts).await;
	set_manifest_version(&opts, MANIFEST_FORMAT_VERSION_V1);
	assert_eq!(manifest_version(&opts), MANIFEST_FORMAT_VERSION_V1);

	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	// Opening only reads: the file still says V1.
	assert_eq!(manifest_version(&opts), MANIFEST_FORMAT_VERSION_V1);
	let mut txn = tree.begin().unwrap();
	txn.set(b"another".to_vec(), b"value".to_vec()).unwrap();
	txn.commit().await.unwrap();
	tree.flush().unwrap();
	assert_eq!(manifest_version(&opts), MANIFEST_FORMAT_VERSION_V2);
	assert_all_keys(&tree);
	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn a_manifest_version_from_the_future_is_refused() {
	let dir = TempDir::new().unwrap();
	let opts = options(dir.path());
	write_database(&opts).await;
	set_manifest_version(&opts, 3);
	let before = fs::read(opts.manifest_file_path(0)).unwrap();

	let err = Tree::new(Arc::clone(&opts)).err().expect("version 3 must be refused");
	assert!(matches!(err, Error::ManifestCorruption(_)), "got {err:?}");
	assert_eq!(fs::read(opts.manifest_file_path(0)).unwrap(), before);
}

/// Both LSMV1 fixtures: unversioned and versioned. Every table of both has the 16-byte layout and
/// says LSMV1 in its footer, which is what the reader of LSMV1 classifies a table by, and the gate
/// of this engine refuses them.
#[test]
fn real_v0_21_4_tables_are_lsmv1_and_refused() {
	for name in ["tiny-plain", "tiny-versioned"] {
		let (_dir, opts) = legacy_database(name);
		let tables = sstables(&opts);
		assert_eq!(tables.len(), 1, "{name}: expected one table");
		for path in tables {
			assert_eq!(footer_format_byte(&path), TableFormat::LSMV1 as u8, "{name}");
			let (file, size) = open_file(&path);
			let err = refuse_legacy_table(1, &opts, file, size as u64)
				.expect_err("a table of LSMV1 must be refused");
			assert!(matches!(err, Error::LegacyFormat(_)), "{name}: got {err:?}");
		}
	}
}

/// Opening an LSMV1 table directly used to hit `assert_eq!` and panic. It is a typed error now,
/// for the paths that do not convert first.
#[test]
fn opening_a_legacy_table_directly_is_a_legacy_format_error_not_a_panic() {
	let (_dir, opts) = legacy_database("tiny-plain");
	let path = sstables(&opts).remove(0);
	let (file, size) = open_file(&path);
	let err = Table::new(1, Arc::clone(&opts), file, size as u64)
		.err()
		.expect("a legacy table must not open");
	assert!(matches!(err, Error::LegacyFormat(_)), "got {err:?}");
}

/// Nothing writes LSMV2 over the keys of LSMV1: a file that says so was changed, and is not
/// converted or read.
#[test]
fn an_lsmv2_footer_over_legacy_keys_is_corruption() {
	let (_dir, opts) = legacy_database("tiny-versioned");
	let path = sstables(&opts).remove(0);
	set_footer_format_byte(&path, TableFormat::LSMV2 as u8);
	let (file, size) = open_file(&path);
	let err = Table::new(1, Arc::clone(&opts), file, size as u64)
		.err()
		.expect("LSMV2 over legacy keys must be refused");
	assert!(matches!(err, Error::CorruptedTableMetadata(_)), "got {err:?}");
}

#[test]
fn an_unknown_footer_format_is_refused() {
	let (_dir, opts) = legacy_database("tiny-plain");
	let path = sstables(&opts).remove(0);
	set_footer_format_byte(&path, 9);
	let (file, size) = open_file(&path);
	let err = Table::new(1, Arc::clone(&opts), file, size as u64)
		.err()
		.expect("format 9 must be refused");
	assert!(matches!(err, Error::InvalidTableFormat), "got {err:?}");
}
