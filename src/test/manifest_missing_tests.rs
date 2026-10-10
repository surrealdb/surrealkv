//! A directory that holds database files but no usable manifest must not be
//! opened as a new database.
//!
//! `LevelManifest::new` used to treat an absent manifest as "create a new
//! database": it wrote a fresh manifest, WAL replay could overwrite table 1,
//! and the startup orphan cleanup then deleted every SSTable the fresh
//! manifest did not list. Deleting or losing `manifest/` therefore emptied the
//! database without any error. These tests pin the replacement behaviour: the
//! open fails, and nothing under the directory is created, written or
//! deleted, so restoring the manifest brings the database back.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use crate::lsm::Core;
use crate::{Error, Options, Tree};

const KEY_COUNT: usize = 120;

fn key(i: usize) -> Vec<u8> {
	format!("key_{i:04}").into_bytes()
}

fn value(i: usize) -> Vec<u8> {
	format!("value_{i:04}_{}", "x".repeat(64)).into_bytes()
}

/// Options for a database with a value log, so that SSTables, WAL segments and
/// value log files can all exist side by side. `flush_on_close` is off so a
/// cleanly closed database still has live WAL records next to its SSTables.
fn db_options(path: &Path) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		enable_vlog: true,
		vlog_value_threshold: 0,
		flush_on_close: false,
		..Default::default()
	})
}

/// Options without a value log, for the checkpoint tests.
fn plain_options(path: &Path) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		flush_on_close: false,
		..Default::default()
	})
}

async fn commit_range(tree: &Tree, range: Range<usize>) {
	let mut txn = tree.begin().unwrap();
	for i in range {
		txn.set(key(i), value(i)).unwrap();
	}
	txn.commit().await.unwrap();
}

fn assert_all_keys_present(tree: &Tree) {
	let txn = tree.begin().unwrap();
	for i in 0..KEY_COUNT {
		assert_eq!(
			txn.get(key(i)).unwrap(),
			Some(value(i)),
			"key {i} must survive: the data files were never touched"
		);
	}
}

fn count_files(dir: PathBuf, extension: &str) -> usize {
	fs::read_dir(dir)
		.map(|entries| {
			entries
				.flatten()
				.filter(|e| e.path().extension().is_some_and(|ext| ext == extension))
				.count()
		})
		.unwrap_or(0)
}

/// Writes a database with two SSTables, live WAL records and value log
/// entries, then closes it cleanly.
async fn write_database(opts: &Arc<Options>) {
	let tree = Tree::new(Arc::clone(opts)).unwrap();
	commit_range(&tree, 0..40).await;
	tree.flush().unwrap();
	commit_range(&tree, 40..80).await;
	tree.flush().unwrap();
	commit_range(&tree, 80..KEY_COUNT).await;
	tree.close().await.unwrap();

	// The control tests below are only meaningful if every kind of data file
	// exists, so a layout change must fail loudly here.
	assert!(count_files(opts.sstable_dir(), "sst") >= 2, "expected SSTables on disk");
	assert!(count_files(opts.wal_dir(), "wal") >= 1, "expected a WAL segment on disk");
	assert!(count_files(opts.vlog_dir(), "vlog") >= 1, "expected a value log file on disk");
	assert!(opts.manifest_file_path(0).exists(), "expected a manifest on disk");
}

async fn closed_database() -> (TempDir, Arc<Options>) {
	let dir = TempDir::new().unwrap();
	let opts = db_options(dir.path());
	write_database(&opts).await;
	(dir, opts)
}

/// Opens the database, checks that every key is readable, and closes it.
async fn reopen_and_verify(opts: &Arc<Options>) {
	let tree = Tree::new(Arc::clone(opts)).unwrap();
	assert_all_keys_present(&tree);
	tree.close().await.unwrap();
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Node {
	Dir,
	File {
		len: u64,
		hash: u64,
	},
}

fn digest(bytes: &[u8]) -> u64 {
	let mut hasher = DefaultHasher::new();
	bytes.hash(&mut hasher);
	hasher.finish()
}

/// Every directory and file under `root`, with a length and content hash for
/// each file.
fn snapshot(root: &Path) -> BTreeMap<PathBuf, Node> {
	fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, Node>) {
		for entry in fs::read_dir(dir).unwrap() {
			let path = entry.unwrap().path();
			let rel = path.strip_prefix(root).unwrap().to_path_buf();
			if path.is_dir() {
				out.insert(rel, Node::Dir);
				walk(root, &path, out);
			} else {
				let bytes = fs::read(&path).unwrap();
				out.insert(
					rel,
					Node::File {
						len: bytes.len() as u64,
						hash: digest(&bytes),
					},
				);
			}
		}
	}

	let mut out = BTreeMap::new();
	if root.exists() {
		walk(root, root, &mut out);
	}
	out
}

fn changes(before: &BTreeMap<PathBuf, Node>, after: &BTreeMap<PathBuf, Node>) -> Vec<String> {
	let mut out = Vec::new();
	for (path, node) in before {
		match after.get(path) {
			None => out.push(format!("removed {}", path.display())),
			Some(now) if now != node => out.push(format!("modified {}", path.display())),
			Some(_) => {}
		}
	}
	for path in after.keys() {
		if !before.contains_key(path) {
			out.push(format!("created {}", path.display()));
		}
	}
	out
}

/// Opens the directory and requires the open to fail without having changed
/// anything under it. Returns the error.
///
/// The LOCK file is overwritten with a sentinel first: acquiring the lock
/// rewrites it with the process id, so an open that takes the lock before
/// failing shows up as a modification.
async fn open_refused(opts: &Arc<Options>) -> Error {
	fs::create_dir_all(&opts.path).unwrap();
	fs::write(opts.path.join("LOCK"), b"sentinel\n").unwrap();
	let before = snapshot(&opts.path);

	match Tree::new(Arc::clone(opts)) {
		Ok(tree) => {
			tree.close().await.unwrap();
			let after = snapshot(&opts.path);
			panic!(
				"the open must be refused, but it succeeded and changed the directory: {:#?}",
				changes(&before, &after)
			);
		}
		Err(err) => {
			let after = snapshot(&opts.path);
			assert_eq!(
				changes(&before, &after),
				Vec::<String>::new(),
				"a refused open must not create, modify or delete anything (error: {err})"
			);
			err
		}
	}
}

fn expect_manifest_missing(err: Error, opts: &Options) {
	match err {
		Error::ManifestMissing(msg) => {
			assert!(
				msg.contains(opts.path.to_str().unwrap()),
				"the message must name the directory: {msg}"
			);
			assert!(msg.contains("SSTable"), "the message must say what was found: {msg}");
		}
		other => panic!("expected ManifestMissing, got: {other}"),
	}
}

fn expect_manifest_corrupt(err: Error, opts: &Options) {
	match err {
		Error::ManifestCorruption(msg) => {
			let manifest = opts.manifest_file_path(0);
			assert!(
				msg.contains(manifest.to_str().unwrap()),
				"the message must name the manifest: {msg}"
			);
		}
		other => panic!("expected ManifestCorruption, got: {other}"),
	}
}

fn copy_dir(src: &Path, dst: &Path) {
	fs::create_dir_all(dst).unwrap();
	for entry in fs::read_dir(src).unwrap() {
		let entry = entry.unwrap();
		let target = dst.join(entry.file_name());
		if entry.path().is_dir() {
			copy_dir(&entry.path(), &target);
		} else {
			fs::copy(entry.path(), target).unwrap();
		}
	}
}

// ---------------------------------------------------------------------------
// The manifest is gone or unusable: refuse, touch nothing
// ---------------------------------------------------------------------------

#[test(tokio::test)]
async fn missing_manifest_directory_is_refused() {
	let (_dir, opts) = closed_database().await;
	let manifest = fs::read(opts.manifest_file_path(0)).unwrap();
	fs::remove_dir_all(opts.manifest_dir()).unwrap();

	let err = open_refused(&opts).await;
	expect_manifest_missing(err, &opts);
	assert!(!opts.manifest_dir().exists(), "the refused open must not recreate manifest/");

	// Nothing else was lost: putting the manifest back brings every key back.
	fs::create_dir_all(opts.manifest_dir()).unwrap();
	fs::write(opts.manifest_file_path(0), manifest).unwrap();
	reopen_and_verify(&opts).await;
}

#[test(tokio::test)]
async fn missing_manifest_file_is_refused() {
	let (_dir, opts) = closed_database().await;
	let manifest = fs::read(opts.manifest_file_path(0)).unwrap();
	fs::remove_file(opts.manifest_file_path(0)).unwrap();

	let err = open_refused(&opts).await;
	expect_manifest_missing(err, &opts);
	assert!(!opts.manifest_file_path(0).exists(), "the refused open must not write a manifest");

	fs::write(opts.manifest_file_path(0), manifest).unwrap();
	reopen_and_verify(&opts).await;
}

/// Applies `damage` to the manifest of a healthy database and requires the
/// open to be refused as corruption. The original manifest is then put back to
/// show the data files were never touched.
async fn refuses_damaged_manifest(damage: impl FnOnce(&mut Vec<u8>)) {
	let (_dir, opts) = closed_database().await;
	let manifest = fs::read(opts.manifest_file_path(0)).unwrap();
	assert!(manifest.len() > 40, "the manifest must be large enough to truncate mid-file");
	let mut damaged = manifest.clone();
	damage(&mut damaged);
	assert_ne!(damaged, manifest);
	fs::write(opts.manifest_file_path(0), &damaged).unwrap();

	let err = open_refused(&opts).await;
	expect_manifest_corrupt(err, &opts);

	fs::write(opts.manifest_file_path(0), &manifest).unwrap();
	reopen_and_verify(&opts).await;
}

#[test(tokio::test)]
async fn zero_length_manifest_is_refused() {
	refuses_damaged_manifest(|bytes| bytes.clear()).await;
}

#[test(tokio::test)]
async fn manifest_cut_off_inside_its_version_is_refused() {
	refuses_damaged_manifest(|bytes| bytes.truncate(1)).await;
}

#[test(tokio::test)]
async fn manifest_cut_off_inside_its_header_is_refused() {
	refuses_damaged_manifest(|bytes| bytes.truncate(10)).await;
}

#[test(tokio::test)]
async fn manifest_cut_off_mid_file_is_refused() {
	refuses_damaged_manifest(|bytes| bytes.truncate(bytes.len() / 2)).await;
}

/// The version is the first big-endian u16 of the file.
#[test(tokio::test)]
async fn manifest_with_a_flipped_low_version_byte_is_refused() {
	refuses_damaged_manifest(|bytes| bytes[1] ^= 0xFF).await;
}

#[test(tokio::test)]
async fn manifest_with_a_flipped_high_version_byte_is_refused() {
	refuses_damaged_manifest(|bytes| bytes[0] ^= 0x01).await;
}

/// A single data file in an otherwise empty directory is enough to refuse.
/// The file is a placeholder: the check looks at names, not contents.
async fn lone_data_file_is_refused(relative: &str) {
	let dir = TempDir::new().unwrap();
	let opts = db_options(dir.path());
	let file = dir.path().join(relative);
	fs::create_dir_all(file.parent().unwrap()).unwrap();
	fs::write(&file, b"placeholder").unwrap();

	match open_refused(&opts).await {
		Error::ManifestMissing(msg) => assert!(msg.contains(dir.path().to_str().unwrap()), "{msg}"),
		other => panic!("expected ManifestMissing, got: {other}"),
	}
	assert_eq!(fs::read(&file).unwrap(), b"placeholder", "{relative} must be left alone");
}

#[test(tokio::test)]
async fn lone_sstable_without_a_manifest_is_refused() {
	lone_data_file_is_refused("sstables/00000000000000000001.sst").await;
}

#[test(tokio::test)]
async fn lone_wal_segment_without_a_manifest_is_refused() {
	lone_data_file_is_refused("wal/00000000000000000000.wal").await;
}

#[test(tokio::test)]
async fn lone_value_log_file_without_a_manifest_is_refused() {
	lone_data_file_is_refused("vlog/00000000000000000001.vlog").await;
}

// ---------------------------------------------------------------------------
// Directories that may be initialised, and healthy databases (controls)
// ---------------------------------------------------------------------------

/// Opens `opts`, writes the full key set, closes, then reopens and verifies.
async fn initialise_and_use(opts: &Arc<Options>) {
	let tree = Tree::new(Arc::clone(opts)).unwrap();
	commit_range(&tree, 0..KEY_COUNT).await;
	tree.close().await.unwrap();
	reopen_and_verify(opts).await;
}

#[test(tokio::test)]
async fn empty_directory_opens_as_a_new_database() {
	let dir = TempDir::new().unwrap();
	initialise_and_use(&db_options(dir.path())).await;
}

#[test(tokio::test)]
async fn nonexistent_directory_opens_as_a_new_database() {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("not").join("created").join("yet");
	initialise_and_use(&db_options(&path)).await;
}

#[test(tokio::test)]
async fn directory_with_only_a_lock_file_opens_as_a_new_database() {
	let dir = TempDir::new().unwrap();
	fs::write(dir.path().join("LOCK"), b"12345\n").unwrap();
	initialise_and_use(&db_options(dir.path())).await;
}

/// The state left by a crash after the directories were created but before
/// the first manifest was written.
#[test(tokio::test)]
async fn empty_data_directories_open_as_a_new_database() {
	let dir = TempDir::new().unwrap();
	let opts = db_options(dir.path());
	for sub in [opts.sstable_dir(), opts.wal_dir(), opts.manifest_dir(), opts.vlog_dir()] {
		fs::create_dir_all(sub).unwrap();
	}
	initialise_and_use(&opts).await;
}

#[test(tokio::test)]
async fn healthy_database_reopens_and_keeps_its_data() {
	let (_dir, opts) = closed_database().await;
	reopen_and_verify(&opts).await;
	reopen_and_verify(&opts).await;
}

/// The orphan cleanup must keep working when the manifest is loaded from disk:
/// an SSTable the manifest does not list is leftover from an interrupted
/// flush, and its data is still in the WAL.
#[test(tokio::test)]
async fn orphan_cleanup_still_runs_against_a_loaded_manifest() {
	let (_dir, opts) = closed_database().await;
	let orphan = opts.sstable_file_path(9_999);
	fs::write(&orphan, b"left behind by an interrupted flush").unwrap();

	reopen_and_verify(&opts).await;
	assert!(!orphan.exists(), "an unlisted SSTable must be cleaned up at startup");
}

/// Defence in depth below `Tree::new`: even if something reaches `Core::new`
/// with data files and no manifest, the manifest it creates lists nothing, so
/// "not in the manifest" says nothing about whether an SSTable is an orphan
/// and the cleanup must not run.
#[test(tokio::test)]
async fn orphan_cleanup_is_skipped_for_a_manifest_created_in_this_run() {
	let dir = TempDir::new().unwrap();
	let opts = Arc::new(Options {
		path: dir.path().to_path_buf(),
		..Default::default()
	});
	for sub in [opts.sstable_dir(), opts.wal_dir(), opts.manifest_dir()] {
		fs::create_dir_all(sub).unwrap();
	}
	let sstable = opts.sstable_file_path(7);
	fs::write(&sstable, b"an SSTable some earlier manifest listed").unwrap();

	if let Ok(core) = Core::new(Arc::clone(&opts)) {
		core.close().await.unwrap();
	}

	assert_eq!(
		fs::read(&sstable).ok().as_deref(),
		Some(&b"an SSTable some earlier manifest listed"[..]),
		"the cleanup deleted an SSTable that a freshly created manifest cannot vouch for"
	);
}

// ---------------------------------------------------------------------------
// Foreign-format directories are migrated, not refused
// ---------------------------------------------------------------------------

/// Their files sit at the top level, where the manifest check does not look,
/// and the migration moves them aside before it creates the manifest itself.
/// The foreign databases here hold no records: only the detection, the move
/// into the backup directory and the hand-over to a normal open are exercised.
#[test(tokio::test)]
async fn rocksdb_directory_is_still_migrated() {
	let dir = TempDir::new().unwrap();
	fs::write(dir.path().join("CURRENT"), b"MANIFEST-000001\n").unwrap();
	fs::write(dir.path().join("MANIFEST-000001"), b"not a real rocksdb manifest").unwrap();

	initialise_and_use(&plain_options(dir.path())).await;

	assert!(dir.path().join("_rocksdb_backup/CURRENT").exists());
	assert!(dir.path().join("_rocksdb_backup/MANIFEST-000001").exists());
	assert!(!dir.path().join("CURRENT").exists());
}

#[test(tokio::test)]
async fn surrealkv_v1_directory_is_still_migrated() {
	use surrealkv_compat_v1::{V1_FOOTER_TOTAL_LEN, V1_MAGIC_FOOTER};

	let dir = TempDir::new().unwrap();
	// The detector only looks for the footer magic at the end of an SSTable.
	let mut table = vec![0u8; V1_FOOTER_TOTAL_LEN];
	table[V1_FOOTER_TOTAL_LEN - V1_MAGIC_FOOTER.len()..].copy_from_slice(&V1_MAGIC_FOOTER);
	fs::write(dir.path().join("000001.sst"), &table).unwrap();

	initialise_and_use(&plain_options(dir.path())).await;

	assert_eq!(fs::read(dir.path().join("_v1_backup/000001.sst")).unwrap(), table);
	assert!(!dir.path().join("000001.sst").exists());
}

/// The migrated database is an ordinary database afterwards: losing its
/// manifest is refused like any other.
#[test(tokio::test)]
async fn migrated_indxdb_directory_is_protected_like_any_other() {
	let dir = TempDir::new().unwrap();
	let entries = vec![(b"user:001".to_vec(), b"Alice".to_vec())];
	surrealkv_compat_indxdb::export_dump(dir.path().join("indxdb_dump.bin"), &entries).unwrap();
	let opts = plain_options(dir.path());

	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	assert_eq!(tree.begin().unwrap().get(b"user:001").unwrap(), Some(b"Alice".to_vec()));
	tree.close().await.unwrap();
	assert!(dir.path().join("_indxdb_backup/indxdb_dump.bin").exists());
	assert!(count_files(opts.sstable_dir(), "sst") >= 1, "the migration writes an SSTable");

	fs::remove_dir_all(opts.manifest_dir()).unwrap();
	let err = open_refused(&opts).await;
	expect_manifest_missing(err, &opts);
}

// ---------------------------------------------------------------------------
// Checkpoints
// ---------------------------------------------------------------------------

/// A legitimate restore must keep working, and the restored database must
/// reopen.
#[test(tokio::test)]
async fn restore_from_checkpoint_survives_a_reopen() {
	let dir = TempDir::new().unwrap();
	let checkpoints = TempDir::new().unwrap();
	let checkpoint = checkpoints.path().join("checkpoint");
	let opts = plain_options(dir.path());

	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	commit_range(&tree, 0..KEY_COUNT).await;
	tree.flush().unwrap();
	tree.create_checkpoint(&checkpoint).unwrap();

	// Overwrite everything after the checkpoint, so a restore has work to do.
	let mut txn = tree.begin().unwrap();
	for i in 0..KEY_COUNT {
		txn.set(key(i), b"written after the checkpoint").unwrap();
	}
	txn.commit().await.unwrap();
	tree.flush().unwrap();

	tree.restore_from_checkpoint(&checkpoint).unwrap();
	assert_all_keys_present(&tree);
	tree.close().await.unwrap();

	reopen_and_verify(&opts).await;
}

/// A restore copies the SSTables and WAL first and the manifest after them,
/// so a crash in between leaves data files and no manifest. That directory
/// used to open as an empty database and have its SSTables deleted as
/// orphans; now the open is refused and finishing the restore completes it.
#[test(tokio::test)]
async fn directory_left_by_an_interrupted_restore_is_refused() {
	let source = TempDir::new().unwrap();
	let checkpoints = TempDir::new().unwrap();
	let checkpoint = checkpoints.path().join("checkpoint");
	let source_opts = plain_options(source.path());

	let tree = Tree::new(Arc::clone(&source_opts)).unwrap();
	commit_range(&tree, 0..KEY_COUNT).await;
	tree.flush().unwrap();
	tree.create_checkpoint(&checkpoint).unwrap();
	tree.close().await.unwrap();

	let target = TempDir::new().unwrap();
	let opts = plain_options(target.path());
	copy_dir(&checkpoint.join("sstables"), &opts.sstable_dir());
	copy_dir(&checkpoint.join("wal"), &opts.wal_dir());
	assert!(count_files(opts.sstable_dir(), "sst") >= 1, "the checkpoint must hold SSTables");

	let err = open_refused(&opts).await;
	expect_manifest_missing(err, &opts);

	// The restore's last copy step finishes the job.
	copy_dir(&checkpoint.join("manifest"), &opts.manifest_dir());
	reopen_and_verify(&opts).await;
}
