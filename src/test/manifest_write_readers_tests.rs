//! Readers are not held up by a write of the manifest.
//!
//! A flush and a compaction write the manifest file and fsync it, which can take as long as the
//! disk takes. Neither holds a lock that a read needs while it does: the change is encoded under
//! the read lock of the manifest, written with only `manifest_io` held, and applied in memory
//! once it is on disk. The file system here parks the write of the manifest until the test
//! releases it, and a read has to complete while it is parked.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use tempfile::TempDir;
use test_log::test;

use crate::error::Result;
use crate::lockfile::LockFile;
use crate::vfs::{CreateOpts, DirEntry, EntryKind, Fs, FsFile, FsKind};
use crate::{CompactRangeOptions, Durability, FileSystem, Mode, Options, Tree};

/// How long a read may take while the manifest write is parked.
const READ: Duration = Duration::from_secs(10);

/// How long the test waits for the write of the manifest to be reached.
const REACHED: Duration = Duration::from_secs(15);

/// `std`, with the replacement of the manifest parked while the test holds it.
#[derive(Debug)]
struct ParkingFs {
	inner: Arc<dyn Fs>,
	armed: AtomicBool,
	reached: Mutex<Option<mpsc::Sender<()>>>,
	release: Mutex<Option<mpsc::Receiver<()>>>,
}

impl ParkingFs {
	fn new() -> (Arc<Self>, FileSystem) {
		let fs = Arc::new(Self {
			inner: Arc::clone(&FileSystem::std().0),
			armed: AtomicBool::new(false),
			reached: Mutex::new(None),
			release: Mutex::new(None),
		});
		let handle = FileSystem(Arc::clone(&fs) as Arc<dyn Fs>);
		(fs, handle)
	}

	/// Parks the next replacement of a manifest. The first receiver says it was reached, and
	/// the sender releases it.
	fn arm(&self) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
		let (reached_tx, reached) = mpsc::channel();
		let (release, release_rx) = mpsc::channel();
		*self.reached.lock().unwrap() = Some(reached_tx);
		*self.release.lock().unwrap() = Some(release_rx);
		self.armed.store(true, Ordering::SeqCst);
		(reached, release)
	}

	fn park(&self) {
		if !self.armed.swap(false, Ordering::SeqCst) {
			return;
		}
		if let Some(reached) = self.reached.lock().unwrap().take() {
			let _ = reached.send(());
		}
		if let Some(release) = self.release.lock().unwrap().take() {
			// Bounded, so a broken test fails instead of hanging.
			let _ = release.recv_timeout(REACHED);
		}
	}
}

impl Fs for ParkingFs {
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
		self.inner.open_read(path)
	}

	fn open_append(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.inner.open_append(path, opts)
	}

	fn open_append_existing(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
		self.inner.open_append_existing(path)
	}

	fn create_new(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.inner.create_new(path, opts)
	}

	fn create_truncate(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.inner.create_truncate(path, opts)
	}

	fn sync_file(&self, path: &Path) -> io::Result<()> {
		self.inner.sync_file(path)
	}

	fn replace_atomic(&self, path: &Path, content: &[u8]) -> Result<()> {
		if path.extension().is_some_and(|extension| extension == "manifest") {
			self.park();
		}
		self.inner.replace_atomic(path, content)
	}

	fn lock_file(&self, dir: &Path) -> LockFile {
		self.inner.lock_file(dir)
	}
}

fn open(path: PathBuf, fs: &FileSystem) -> Arc<Tree> {
	let opts = Options {
		path,
		enable_vlog: false,
		max_memtable_size: 16 * 1024 * 1024,
		level0_max_files: 1000,
		l0_stall_threshold: 2000,
		..Default::default()
	}
	.with_file_system(fs.clone());
	Arc::new(Tree::new(Arc::new(opts)).unwrap())
}

async fn put(tree: &Tree, key: &str, value: &str) {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(Durability::Immediate);
	txn.set(key.as_bytes(), value.as_bytes()).unwrap();
	txn.commit().await.unwrap();
}

fn get(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key.as_bytes()).unwrap()
}

fn table_count(tree: &Tree) -> usize {
	let manifest = tree.core.inner.level_manifest.read().unwrap();
	manifest.levels.get_levels().iter().map(|level| level.tables.len()).sum()
}

/// Reads `keys` on another thread and fails the test unless every read returns `expected` within
/// [`READ`].
fn read_while_parked(tree: &Arc<Tree>, keys: &[(&'static str, &'static str)], what: &str) {
	let (done_tx, done) = mpsc::channel();
	let reader = Arc::clone(tree);
	let names: Vec<&'static str> = keys.iter().map(|(key, _)| *key).collect();
	std::thread::spawn(move || {
		let read: Vec<_> = names.iter().map(|key| get(&reader, key)).collect();
		let _ = done_tx.send(read);
	});
	let read = done
		.recv_timeout(READ)
		.unwrap_or_else(|_| panic!("{what}: a read waited for the manifest write to finish"));
	for ((key, value), read) in keys.iter().zip(read) {
		assert_eq!(read.as_deref(), Some(value.as_bytes()), "{what}: wrong value for {key}");
	}
}

/// Runs `step` on another thread with the manifest write it makes parked, reads `keys` while it
/// is, and then lets it finish.
fn with_parked_manifest_write(
	tree: &Arc<Tree>,
	fs: &ParkingFs,
	keys: &[(&'static str, &'static str)],
	what: &str,
	step: impl FnOnce(&Tree) -> Result<()> + Send + 'static,
) {
	let (reached, release) = fs.arm();
	let worker = Arc::clone(tree);
	let handle = std::thread::spawn(move || step(&worker));
	reached.recv_timeout(REACHED).unwrap_or_else(|_| panic!("{what}: no manifest write"));
	read_while_parked(tree, keys, what);
	release.send(()).unwrap();
	handle.join().unwrap().unwrap();
}

/// A flush publishes its table once the manifest that lists it is on disk: while it is being
/// written, a read finds the flushed keys in the memtable that is still queued.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn a_read_completes_while_a_flush_writes_the_manifest() {
	let dir = TempDir::new().unwrap();
	let (fs, handle) = ParkingFs::new();
	let tree = open(dir.path().join("db"), &handle);

	put(&tree, "a", "1").await;
	tree.flush().unwrap();
	put(&tree, "b", "2").await;
	let tables = table_count(&tree);

	with_parked_manifest_write(&tree, &fs, &[("a", "1"), ("b", "2")], "flush", |tree| tree.flush());

	assert_eq!(table_count(&tree), tables + 1, "the flushed table is not in the manifest");
	assert_eq!(get(&tree, "a").as_deref(), Some(&b"1"[..]));
	assert_eq!(get(&tree, "b").as_deref(), Some(&b"2"[..]));
	tree.close().await.unwrap();
}

/// A compaction removes its inputs from the manifest in memory once the manifest without them is
/// on disk: while it is being written, a read finds the keys in the inputs.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn a_read_completes_while_a_compaction_writes_the_manifest() {
	let dir = TempDir::new().unwrap();
	let (fs, handle) = ParkingFs::new();
	let tree = open(dir.path().join("db"), &handle);

	put(&tree, "a", "1").await;
	tree.flush().unwrap();
	put(&tree, "b", "2").await;
	tree.flush().unwrap();
	assert_eq!(table_count(&tree), 2);

	with_parked_manifest_write(&tree, &fs, &[("a", "1"), ("b", "2")], "compaction", |tree| {
		tree.compact_range(None, None, &CompactRangeOptions::default())
	});

	assert_eq!(table_count(&tree), 1, "the compaction did not replace its inputs");
	assert_eq!(get(&tree, "a").as_deref(), Some(&b"1"[..]));
	assert_eq!(get(&tree, "b").as_deref(), Some(&b"2"[..]));
	tree.close().await.unwrap();
}
