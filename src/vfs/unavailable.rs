//! The file system of a build that has none mounted.
//!
//! `Options::default()` starts from it in a browser, where `std::fs` does not work and a file
//! system has to be mounted before a tree can be built. Every operation fails with
//! `ErrorKind::Unsupported` and a text that says what to do, so a tree that was built without a
//! mount fails at its first call and creates nothing. `exists`, `is_dir` and `is_file` cannot
//! fail, so they read `false`: this is why `build_async` asks `check_available` first, before any
//! of them can be taken for "the database is not there".

use std::io;
use std::path::Path;
use std::sync::Arc;

use super::{unavailable_error, CreateOpts, DirEntry, EntryKind, Fs, FsFile, FsKind, SpareWait};
use crate::error::{Error, Result};
use crate::lockfile::LockFile;

/// What a browser build says until a file system is mounted.
#[cfg_attr(
	not(all(target_arch = "wasm32", target_os = "unknown")),
	allow(dead_code, reason = "the default file system of a browser build")
)]
const NO_FILE_SYSTEM: &str = "no file system is mounted: use \
	`TreeBuilder::with_file_system(FileSystem::opfs(..).await?)`";

/// A file system that refuses every operation, with the reason it was given.
#[derive(Clone, Debug)]
pub(crate) struct UnavailableFs {
	reason: &'static str,
}

impl UnavailableFs {
	pub(crate) const fn new(reason: &'static str) -> Self {
		Self {
			reason,
		}
	}

	/// The file system of a browser build that has not mounted one.
	#[cfg_attr(
		not(all(target_arch = "wasm32", target_os = "unknown")),
		allow(dead_code, reason = "the default file system of a browser build")
	)]
	pub(crate) const fn no_file_system() -> Self {
		Self::new(NO_FILE_SYSTEM)
	}

	fn refuse<T>(&self) -> io::Result<T> {
		Err(unavailable_error(self.reason))
	}
}

impl Fs for UnavailableFs {
	fn kind(&self) -> FsKind {
		FsKind::Unavailable
	}

	fn check_available(&self) -> io::Result<()> {
		self.refuse()
	}

	fn entry(&self, _path: &Path) -> io::Result<Option<EntryKind>> {
		self.refuse()
	}

	fn create_dir_all(&self, _path: &Path) -> io::Result<()> {
		self.refuse()
	}

	fn read_dir(&self, _path: &Path) -> io::Result<Vec<DirEntry>> {
		self.refuse()
	}

	fn remove_file(&self, _path: &Path) -> io::Result<()> {
		self.refuse()
	}

	fn remove_dir_all(&self, _path: &Path) -> io::Result<()> {
		self.refuse()
	}

	fn rename(&self, _from: &Path, _to: &Path) -> io::Result<()> {
		self.refuse()
	}

	fn sync_dir(&self, _path: &Path) -> io::Result<()> {
		self.refuse()
	}

	fn open_read(&self, _path: &Path) -> io::Result<Arc<dyn FsFile>> {
		self.refuse()
	}

	fn open_append(&self, _path: &Path, _opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.refuse()
	}

	fn create_new(&self, _path: &Path, _opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.refuse()
	}

	fn create_truncate(&self, _path: &Path, _opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.refuse()
	}

	fn replace_atomic(&self, _path: &Path, _content: &[u8]) -> Result<()> {
		self.refuse().map_err(Error::from)
	}

	fn lock_file(&self, _dir: &Path) -> LockFile {
		LockFile::refused(self.reason)
	}

	fn ensure_spares(&self, _n: usize) -> Option<SpareWait> {
		let reason = self.reason;
		Some(Box::pin(async move { Err(unavailable_error(reason)) }))
	}
}

/// Asserts that `fs` refuses everything with the typed error. For the native and the browser
/// tests, which run the same check.
#[cfg(test)]
pub(crate) fn assert_refuses_everything(fs: &dyn Fs, expect_in_message: &str) {
	use std::task::{Context, Poll, Waker};

	let path = Path::new("a/b");
	let refused = |what: &str, error: io::Error| {
		assert_eq!(error.kind(), io::ErrorKind::Unsupported, "{what}: {error}");
		assert!(error.to_string().contains(expect_in_message), "{what}: {error}");
	};

	assert_eq!(fs.kind(), FsKind::Unavailable);
	refused("check_available", fs.check_available().unwrap_err());
	refused("entry", fs.entry(path).unwrap_err());
	refused("try_exists", fs.try_exists(path).unwrap_err());
	refused("create_dir_all", fs.create_dir_all(path).unwrap_err());
	refused("read_dir", fs.read_dir(path).unwrap_err());
	refused("remove_file", fs.remove_file(path).unwrap_err());
	refused("remove_dir_all", fs.remove_dir_all(path).unwrap_err());
	refused("rename", fs.rename(path, Path::new("c")).unwrap_err());
	refused("sync_dir", fs.sync_dir(path).unwrap_err());
	refused("open_read", fs.open_read(path).err().unwrap());
	refused("open_append", fs.open_append(path, CreateOpts::default()).err().unwrap());
	refused("create_new", fs.create_new(path, CreateOpts::default()).err().unwrap());
	refused("create_truncate", fs.create_truncate(path, CreateOpts::default()).err().unwrap());
	refused("sync_file", fs.sync_file(path).unwrap_err());
	refused("file_len", fs.file_len(path).unwrap_err());
	refused("read", fs.read(path).unwrap_err());
	refused("read_prefix", fs.read_prefix(path, 4).unwrap_err());
	refused("copy", fs.copy(path, Path::new("c")).unwrap_err());
	match fs.replace_atomic(path, b"x") {
		Err(Error::Io(error)) => {
			refused("replace_atomic", io::Error::new(error.kind(), error.to_string()))
		}
		other => panic!("replace_atomic: {other:?}"),
	}

	// The questions that cannot fail read `false`, which is why `check_available` comes first.
	assert!(!fs.exists(path) && !fs.is_dir(path) && !fs.is_file(path));

	// The lock cannot be had.
	let mut lock = fs.lock_file(Path::new("a"));
	match lock.acquire() {
		Err(Error::Io(error)) => refused("lock", io::Error::new(error.kind(), error.to_string())),
		other => panic!("lock: {other:?}"),
	}

	// Waiting for spares fails, at once.
	let mut wait = fs.ensure_spares(1).expect("an unavailable file system never has spares");
	let mut context = Context::from_waker(Waker::noop());
	match wait.as_mut().poll(&mut context) {
		Poll::Ready(Err(error)) => refused("ensure_spares", error),
		other => panic!("ensure_spares: {other:?}"),
	}
	assert_eq!(fs.pool_exhaustions(), 0);
}
