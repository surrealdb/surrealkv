//! What the open replays from the log is durable before the database takes a commit.
//!
//! A process that is killed after it appended a commit to a segment of the log, and before it
//! synced it, leaves bytes that every later process can read and that are not on the disk. The open
//! that follows replays them, and a commit made on top of them (one that read them) is logged in
//! a new segment and synced. A power loss then takes the bytes that were never synced and keeps
//! the commit that was: the database comes back in a state that no order of its commits makes.
//!
//! The file system here is `std` with a ledger of what was synced: every fsync of a file, through a
//! handle or by name, records the length the file has then as durable. A power loss cuts every
//! segment of the log back to that length, which is what a file system that kept only what was
//! synced leaves. A process is killed with `Tree::abandon` and the tree is forgotten, so that no
//! destructor syncs what the process left.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tempfile::TempDir;
use test_log::test;

use super::wal_sealed_tests::{context, SUITES};
use crate::error::Result;
use crate::lockfile::LockFile;
use crate::tde::EncryptionContext;
use crate::vfs::{CreateOpts, DirEntry, EntryKind, File, Fs, FsFile, FsKind};
use crate::{Durability, FileSystem, Mode, Options, Tree};

/// The length of each file as of its last fsync, by canonical path.
type Ledger = Arc<Mutex<HashMap<PathBuf, u64>>>;

fn ledger_key(path: &Path) -> PathBuf {
	path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

/// `std`, with the ledger of what each fsync made durable.
#[derive(Debug)]
struct SyncLedgerFs {
	inner: Arc<dyn Fs>,
	durable: Ledger,
}

/// A file of [`SyncLedgerFs`]: an fsync of it records its length.
struct LedgerFile {
	inner: Arc<dyn FsFile>,
	path: PathBuf,
	durable: Ledger,
}

impl File for LedgerFile {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		self.inner.read_at(offset, buf)
	}

	fn size(&self) -> Result<u64> {
		self.inner.size()
	}
}

impl FsFile for LedgerFile {
	fn len(&self) -> io::Result<u64> {
		self.inner.len()
	}

	fn append(&self, buf: &[u8]) -> io::Result<()> {
		self.inner.append(buf)
	}

	fn set_len(&self, len: u64) -> io::Result<()> {
		self.inner.set_len(len)
	}

	fn sync(&self) -> io::Result<()> {
		self.inner.sync()?;
		let len = self.inner.len()?;
		self.durable.lock().unwrap().insert(ledger_key(&self.path), len);
		Ok(())
	}
}

impl SyncLedgerFs {
	fn new() -> (Arc<Self>, FileSystem) {
		let fs = Arc::new(Self {
			inner: Arc::clone(&FileSystem::std().0),
			durable: Ledger::default(),
		});
		let handle = FileSystem(Arc::clone(&fs) as Arc<dyn Fs>);
		(fs, handle)
	}

	fn wrap(&self, path: &Path, file: Arc<dyn FsFile>) -> Arc<dyn FsFile> {
		Arc::new(LedgerFile {
			inner: file,
			path: path.to_path_buf(),
			durable: Arc::clone(&self.durable),
		})
	}

	/// What a power loss leaves of the segments of the log in `wal_dir`: each is cut back to the
	/// length it had at its last fsync, and one that was never synced is empty.
	fn lose_power(&self, wal_dir: &Path) {
		let durable = self.durable.lock().unwrap();
		for entry in std::fs::read_dir(wal_dir).unwrap() {
			let path = entry.unwrap().path();
			if path.extension().is_none_or(|extension| extension != "wal") {
				continue;
			}
			let len = std::fs::metadata(&path).unwrap().len();
			let kept = durable.get(&ledger_key(&path)).copied().unwrap_or(0).min(len);
			std::fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(kept).unwrap();
		}
	}
}

impl Fs for SyncLedgerFs {
	fn kind(&self) -> FsKind {
		self.inner.kind()
	}

	fn entry(&self, path: &Path) -> io::Result<Option<EntryKind>> {
		self.inner.entry(path)
	}

	fn create_dir_all(&self, path: &Path) -> io::Result<()> {
		self.inner.create_dir_all(path)
	}

	fn read_dir(&self, path: &Path) -> io::Result<Vec<DirEntry>> {
		self.inner.read_dir(path)
	}

	fn remove_file(&self, path: &Path) -> io::Result<()> {
		self.inner.remove_file(path)
	}

	fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
		self.inner.remove_dir_all(path)
	}

	fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
		self.inner.rename(from, to)
	}

	fn sync_dir(&self, path: &Path) -> io::Result<()> {
		self.inner.sync_dir(path)
	}

	fn open_read(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
		Ok(self.wrap(path, self.inner.open_read(path)?))
	}

	fn open_append(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		Ok(self.wrap(path, self.inner.open_append(path, opts)?))
	}

	fn open_append_existing(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
		Ok(self.wrap(path, self.inner.open_append_existing(path)?))
	}

	fn create_new(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		Ok(self.wrap(path, self.inner.create_new(path, opts)?))
	}

	fn create_truncate(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		Ok(self.wrap(path, self.inner.create_truncate(path, opts)?))
	}

	fn sync_file(&self, path: &Path) -> io::Result<()> {
		self.inner.sync_file(path)?;
		let len = self.inner.file_len(path)?;
		self.durable.lock().unwrap().insert(ledger_key(path), len);
		Ok(())
	}

	fn replace_atomic(&self, path: &Path, content: &[u8]) -> Result<()> {
		self.inner.replace_atomic(path, content)
	}

	fn lock_file(&self, dir: &Path) -> LockFile {
		self.inner.lock_file(dir)
	}
}

fn options(path: &Path, fs: &FileSystem, enc: Option<&Arc<EncryptionContext>>) -> Arc<Options> {
	let opts = Options {
		path: path.to_path_buf(),
		enable_vlog: false,
		max_memtable_size: 8 * 1024,
		flush_on_close: false,
		..Default::default()
	}
	.with_file_system(fs.clone());
	Arc::new(match enc {
		Some(enc) => opts.with_encryption_context(Arc::clone(enc)),
		None => opts,
	})
}

/// Ends a tree the way a killed process does: nothing is flushed or synced, and no destructor
/// runs.
fn kill(tree: Tree) {
	tree.abandon();
	std::mem::forget(tree);
}

async fn put(tree: &Tree, key: &str, value: &[u8], durability: Durability) {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(durability);
	txn.set(key.as_bytes(), value).unwrap();
	txn.commit().await.unwrap();
}

fn get(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key.as_bytes()).unwrap()
}

/// A commit that was logged and not synced, and that the process did not live to sync, is
/// replayed by the next open. A commit made on top of it and synced must not survive a power loss
/// that the replayed one does not: the open makes what it replayed durable first.
async fn a_commit_on_top_of_a_replayed_one_does_not_outlive_it(
	enc: Option<Arc<EncryptionContext>>,
	what: &str,
) {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("db");
	let (ledger, fs) = SyncLedgerFs::new();
	let opts = options(&path, &fs, enc.as_ref());

	// The first process: a synced commit, then one that is logged and not synced, and the
	// process is killed before anything syncs it.
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	put(&tree, "seed", b"0", Durability::Immediate).await;
	put(&tree, "first", b"1", Durability::Eventual).await;
	kill(tree);

	// The second process replays the commit that was not synced, and commits on top of it.
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	let first = get(&tree, "first");
	assert_eq!(first.as_deref(), Some(&b"1"[..]), "{what}: the replay did not find the commit");
	let mut second = first.unwrap();
	second.push(b'2');
	put(&tree, "second", &second, Durability::Immediate).await;
	kill(tree);

	// The power is lost: of the log, only what was synced is left.
	ledger.lose_power(&opts.wal_dir());

	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	assert_eq!(get(&tree, "seed").as_deref(), Some(&b"0"[..]), "{what}: a synced commit was lost");
	let first = get(&tree, "first");
	let second = get(&tree, "second");
	assert_eq!(
		second.as_deref(),
		Some(&b"12"[..]),
		"{what}: the commit that was synced after the replay was lost"
	);
	assert_eq!(
		first.as_deref(),
		Some(&b"1"[..]),
		"{what}: the commit that the synced one read was lost, and the synced one was kept"
	);
	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn a_commit_on_top_of_a_replayed_plaintext_segment_does_not_outlive_it() {
	a_commit_on_top_of_a_replayed_one_does_not_outlive_it(None, "plaintext").await;
}

#[test(tokio::test)]
async fn a_commit_on_top_of_a_replayed_sealed_segment_does_not_outlive_it() {
	for suite in SUITES {
		a_commit_on_top_of_a_replayed_one_does_not_outlive_it(
			Some(context(suite)),
			&format!("sealed {suite:?}"),
		)
		.await;
	}
}
