//! Test-only: a file system that does what `StdFs` does and writes down each call, so that a test
//! can say in what order the engine used it.
//!
//! The tables, the manifest and the open path call a file system and not `std::fs`, and the order
//! of the calls is what makes a table durable before the manifest names it. On a native file
//! system each call is the syscall it was before the calls were moved, so the order that is
//! recorded here is the order of the syscalls. A name in the log is the last two parts of the path
//! (`sstables/00000000000000000001.sst`), so that a log does not depend on where the test ran.

use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::lockfile::LockFile;
use crate::vfs::{
	CreateOpts,
	DirEntry,
	EntryKind,
	Fs,
	FsFile,
	FsKind,
	SpareClass,
	SpareWait,
	StdFs,
};

/// A file system over `StdFs` with a log.
#[derive(Debug)]
pub(crate) struct RecordingFs {
	inner: StdFs,
	kind: FsKind,
	log: Mutex<Vec<String>>,
}

impl RecordingFs {
	/// A file system that is `std` for everything the engine asks of it, `kind` included.
	pub(crate) fn new() -> Arc<Self> {
		Self::of_kind(FsKind::Std)
	}

	/// The same files, with `kind` as the answer to what the file system is: what the engine
	/// tests where a pool must behave differently from `std`.
	pub(crate) fn of_kind(kind: FsKind) -> Arc<Self> {
		Arc::new(Self {
			inner: StdFs,
			kind,
			log: Mutex::new(Vec::new()),
		})
	}

	fn record(&self, entry: String) {
		self.log.lock().unwrap().push(entry);
	}

	/// Everything recorded so far.
	pub(crate) fn entries(&self) -> Vec<String> {
		self.log.lock().unwrap().clone()
	}

	/// What was recorded, and forgets it.
	pub(crate) fn take(&self) -> Vec<String> {
		std::mem::take(&mut *self.log.lock().unwrap())
	}
}

/// The last two parts of `path`.
pub(crate) fn short(path: &Path) -> String {
	let mut parts: Vec<_> =
		path.components().rev().take(2).map(|c| c.as_os_str().to_string_lossy()).collect();
	parts.reverse();
	parts.join("/")
}

fn class(opts: CreateOpts) -> &'static str {
	match opts.class {
		SpareClass::Reserved => "reserved",
		SpareClass::Opportunistic => "opportunistic",
	}
}

/// An open file of a `RecordingFs`: it records what is written and synced through it, and not
/// what is read.
struct RecordingFile {
	name: String,
	inner: Arc<dyn FsFile>,
	fs: Arc<RecordingFs>,
}

impl crate::vfs::File for RecordingFile {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		self.inner.read_at(offset, buf)
	}

	fn size(&self) -> Result<u64> {
		self.inner.size()
	}
}

impl FsFile for RecordingFile {
	fn len(&self) -> io::Result<u64> {
		self.inner.len()
	}

	fn append(&self, buf: &[u8]) -> io::Result<()> {
		self.fs.record(format!("append {} {}", self.name, buf.len()));
		self.inner.append(buf)
	}

	fn set_len(&self, len: u64) -> io::Result<()> {
		self.fs.record(format!("set_len {} {len}", self.name));
		self.inner.set_len(len)
	}

	fn sync(&self) -> io::Result<()> {
		self.fs.record(format!("sync {}", self.name));
		self.inner.sync()
	}
}

/// `Fs` takes `&self`, and the files it opens need the log: they hold the `Arc` that the test made,
/// which is why the trait is implemented for the `Arc`.
impl Fs for Arc<RecordingFs> {
	fn kind(&self) -> FsKind {
		self.kind
	}

	fn check_available(&self) -> io::Result<()> {
		self.inner.check_available()
	}

	fn entry(&self, path: &Path) -> io::Result<Option<EntryKind>> {
		self.inner.entry(path)
	}

	fn try_exists(&self, path: &Path) -> io::Result<bool> {
		self.record(format!("try_exists {}", short(path)));
		self.inner.try_exists(path)
	}

	fn exists(&self, path: &Path) -> bool {
		self.record(format!("exists {}", short(path)));
		self.inner.exists(path)
	}

	fn is_dir(&self, path: &Path) -> bool {
		self.record(format!("is_dir {}", short(path)));
		self.inner.is_dir(path)
	}

	fn is_file(&self, path: &Path) -> bool {
		self.record(format!("is_file {}", short(path)));
		self.inner.is_file(path)
	}

	fn create_dir_all(&self, path: &Path) -> io::Result<()> {
		self.record(format!("create_dir_all {}", short(path)));
		self.inner.create_dir_all(path)
	}

	fn read_dir(&self, path: &Path) -> io::Result<Vec<DirEntry>> {
		self.record(format!("read_dir {}", short(path)));
		self.inner.read_dir(path)
	}

	fn remove_file(&self, path: &Path) -> io::Result<()> {
		self.record(format!("remove_file {}", short(path)));
		self.inner.remove_file(path)
	}

	fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
		self.record(format!("remove_dir_all {}", short(path)));
		self.inner.remove_dir_all(path)
	}

	fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
		self.record(format!("rename {} {}", short(from), short(to)));
		self.inner.rename(from, to)
	}

	fn sync_dir(&self, path: &Path) -> io::Result<()> {
		self.record(format!("sync_dir {}", short(path)));
		self.inner.sync_dir(path)
	}

	fn open_read(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
		self.record(format!("open_read {}", short(path)));
		Ok(Arc::new(RecordingFile {
			name: short(path),
			inner: self.inner.open_read(path)?,
			fs: Arc::clone(self),
		}))
	}

	fn open_append_existing(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
		self.record(format!("open_append_existing {}", short(path)));
		Ok(Arc::new(RecordingFile {
			name: short(path),
			inner: self.inner.open_append_existing(path)?,
			fs: Arc::clone(self),
		}))
	}

	fn open_append(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.record(format!("open_append {} {}", short(path), class(opts)));
		Ok(Arc::new(RecordingFile {
			name: short(path),
			inner: self.inner.open_append(path, opts)?,
			fs: Arc::clone(self),
		}))
	}

	fn create_new(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.record(format!("create_new {} {}", short(path), class(opts)));
		Ok(Arc::new(RecordingFile {
			name: short(path),
			inner: self.inner.create_new(path, opts)?,
			fs: Arc::clone(self),
		}))
	}

	fn create_truncate(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.record(format!("create_truncate {} {}", short(path), class(opts)));
		Ok(Arc::new(RecordingFile {
			name: short(path),
			inner: self.inner.create_truncate(path, opts)?,
			fs: Arc::clone(self),
		}))
	}

	fn sync_file(&self, path: &Path) -> io::Result<()> {
		self.record(format!("sync_file {}", short(path)));
		self.inner.sync_file(path)
	}

	fn replace_atomic(&self, path: &Path, content: &[u8]) -> Result<()> {
		self.record(format!("replace_atomic {} {}", short(path), content.len()));
		self.inner.replace_atomic(path, content)
	}

	fn file_len(&self, path: &Path) -> io::Result<u64> {
		self.record(format!("file_len {}", short(path)));
		self.inner.file_len(path)
	}

	fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
		self.record(format!("read {}", short(path)));
		self.inner.read(path)
	}

	fn read_prefix(&self, path: &Path, n: usize) -> io::Result<Option<Vec<u8>>> {
		self.record(format!("read_prefix {} {n}", short(path)));
		self.inner.read_prefix(path, n)
	}

	fn copy(&self, from: &Path, to: &Path) -> io::Result<u64> {
		self.record(format!("copy {} {}", short(from), short(to)));
		self.inner.copy(from, to)
	}

	fn lock_file(&self, dir: &Path) -> LockFile {
		self.inner.lock_file(dir)
	}

	fn ensure_spares(&self, n: usize) -> Option<SpareWait> {
		self.inner.ensure_spares(n)
	}

	fn pool_exhaustions(&self) -> u64 {
		self.inner.pool_exhaustions()
	}
}
