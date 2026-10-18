//! A file system that watches the engine and can fail it: the tests of the engine over a pool use
//! it to say what the engine asked of its file system, to refuse one call at an exact place, to
//! make a pool look empty, and to take a crash image of the files before a call.
//!
//! It wraps the pool (or any other file system) and delegates every call to it, so the engine
//! runs on the pool underneath; what it adds is the hooks.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::{fmt, io};

use crate::error::Result;
use crate::lockfile::LockFile;
use crate::vfs::{
	CreateOpts,
	DirEntry,
	EntryKind,
	Fs,
	FsFile,
	FsKind,
	PoolControl,
	SpareClass,
	SpareWait,
};
use crate::FileSystem;

/// A call of the engine that creates, replaces or removes a file or a name.
#[derive(Clone, Debug)]
pub(crate) enum Call {
	CreateDirAll(PathBuf),
	RemoveFile(PathBuf),
	/// The path that is renamed.
	Rename(PathBuf),
	SyncDir(PathBuf),
	SyncFile(PathBuf),
	OpenAppend(PathBuf),
	CreateNew(PathBuf),
	CreateTruncate(PathBuf, SpareClass),
	ReplaceAtomic(PathBuf),
}

impl Call {
	pub(crate) fn path(&self) -> &Path {
		match self {
			Call::CreateDirAll(p)
			| Call::RemoveFile(p)
			| Call::Rename(p)
			| Call::SyncDir(p)
			| Call::SyncFile(p)
			| Call::OpenAppend(p)
			| Call::CreateNew(p)
			| Call::CreateTruncate(p, _)
			| Call::ReplaceAtomic(p) => p,
		}
	}

	/// Whether the call makes a file that is a table of the database.
	pub(crate) fn makes_a_table(&self) -> bool {
		matches!(self, Call::CreateTruncate(p, _) if p.extension().is_some_and(|e| e == "sst"))
	}

	/// Whether the call makes a segment of the log: a rotation opens the next segment to append to,
	/// which is made if it is not there.
	pub(crate) fn makes_a_segment(&self) -> bool {
		matches!(self, Call::CreateNew(p) | Call::OpenAppend(p) if p.extension().is_some_and(|e| e == "wal"))
	}

	pub(crate) fn replaces_the_manifest(&self) -> bool {
		matches!(self, Call::ReplaceAtomic(_))
	}
}

/// What a hook says about a call.
pub(crate) enum Verdict {
	/// Let it through.
	Go,
	/// Fail it with this error, and do not make it.
	Fail(io::Error),
}

type Before = dyn Fn(&Call) -> Verdict + Send + Sync;
type After = dyn Fn(&Call) + Send + Sync;
type Spares = dyn Fn(usize) -> Option<Wait> + Send + Sync;

/// What a request for spare files is answered with, instead of the pool's own wait.
pub(crate) enum Wait {
	/// A wait that ends at once with this result.
	Now(io::Result<()>),
	/// A wait that never ends: a refill that is stuck.
	Never,
}

/// The wrapper. Everything about it can be changed while the engine runs.
pub(crate) struct ProbeFs {
	inner: Arc<dyn Fs>,
	before: Mutex<Option<Arc<Before>>>,
	after: Mutex<Option<Arc<After>>>,
	spares: Mutex<Option<Arc<Spares>>>,
	/// The requests for spare files, in order.
	asked: Mutex<Vec<usize>>,
	/// Refusals that this wrapper made and counted as the pool's own, so that the witness of the
	/// background tasks sees them.
	exhaustions: AtomicU64,
}

impl fmt::Debug for ProbeFs {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ProbeFs").field("inner", &self.inner).finish()
	}
}

impl ProbeFs {
	/// The wrapper around `inner`, and the file system to give to the engine.
	pub(crate) fn wrap(inner: &FileSystem) -> (Arc<ProbeFs>, FileSystem) {
		let probe = Arc::new(ProbeFs {
			inner: Arc::clone(&inner.0),
			before: Mutex::new(None),
			after: Mutex::new(None),
			spares: Mutex::new(None),
			asked: Mutex::new(Vec::new()),
			exhaustions: AtomicU64::new(0),
		});
		let fs = FileSystem(Arc::clone(&probe) as Arc<dyn Fs>);
		(probe, fs)
	}

	/// Sets what is said about every call before it is made.
	pub(crate) fn before(&self, hook: impl Fn(&Call) -> Verdict + Send + Sync + 'static) {
		*self.before.lock().unwrap() = Some(Arc::new(hook));
	}

	/// Sets what is told of every call that was made and went through.
	pub(crate) fn after(&self, hook: impl Fn(&Call) + Send + Sync + 'static) {
		*self.after.lock().unwrap() = Some(Arc::new(hook));
	}

	/// Sets the answer to a request for `n` spare files: `None` to ask the pool, `Some(wait)` for
	/// a wait of the kind that `wait` says.
	pub(crate) fn spares_wait(&self, hook: impl Fn(usize) -> Option<Wait> + Send + Sync + 'static) {
		*self.spares.lock().unwrap() = Some(Arc::new(hook));
	}

	/// Sets the answer to a request for `n` spare files: `None` to ask the pool, `Some(result)`
	/// for a wait that ends at once with `result`.
	pub(crate) fn spares(
		&self,
		hook: impl Fn(usize) -> Option<io::Result<()>> + Send + Sync + 'static,
	) {
		self.spares_wait(move |n| hook(n).map(Wait::Now));
	}

	pub(crate) fn clear_hooks(&self) {
		*self.before.lock().unwrap() = None;
		*self.after.lock().unwrap() = None;
		*self.spares.lock().unwrap() = None;
	}

	/// How many spare files each request asked for, in order.
	pub(crate) fn asked(&self) -> Vec<usize> {
		self.asked.lock().unwrap().clone()
	}

	/// Counts a refusal as the pool's own: a call that this wrapper failed with an error that is
	/// not the pool's, to see what the tasks make of a failure that came with an exhaustion.
	pub(crate) fn count_exhaustion(&self) {
		self.exhaustions.fetch_add(1, Ordering::AcqRel);
	}

	/// What the pool underneath would have left after a crash now.
	pub(crate) fn crash_image(&self, mode: crate::CrashMode) -> FileSystem {
		FileSystem(Arc::clone(&self.inner)).simulate_crash(mode).expect("a memory pool crashes")
	}

	fn gate(&self, call: Call) -> io::Result<()> {
		let hook = self.before.lock().unwrap().clone();
		match hook.map_or(Verdict::Go, |hook| hook(&call)) {
			Verdict::Go => Ok(()),
			Verdict::Fail(error) => Err(error),
		}
	}

	fn done(&self, call: Call) {
		let hook = self.after.lock().unwrap().clone();
		if let Some(hook) = hook {
			hook(&call);
		}
	}
}

impl Fs for ProbeFs {
	fn kind(&self) -> FsKind {
		self.inner.kind()
	}

	fn check_available(&self) -> io::Result<()> {
		self.inner.check_available()
	}

	fn entry(&self, path: &Path) -> io::Result<Option<EntryKind>> {
		self.inner.entry(path)
	}

	fn try_exists(&self, path: &Path) -> io::Result<bool> {
		self.inner.try_exists(path)
	}

	fn exists(&self, path: &Path) -> bool {
		self.inner.exists(path)
	}

	fn is_dir(&self, path: &Path) -> bool {
		self.inner.is_dir(path)
	}

	fn is_file(&self, path: &Path) -> bool {
		self.inner.is_file(path)
	}

	fn create_dir_all(&self, path: &Path) -> io::Result<()> {
		self.gate(Call::CreateDirAll(path.to_path_buf()))?;
		self.inner.create_dir_all(path)
	}

	fn read_dir(&self, path: &Path) -> io::Result<Vec<DirEntry>> {
		self.inner.read_dir(path)
	}

	fn remove_file(&self, path: &Path) -> io::Result<()> {
		self.gate(Call::RemoveFile(path.to_path_buf()))?;
		let result = self.inner.remove_file(path);
		if result.is_ok() {
			self.done(Call::RemoveFile(path.to_path_buf()));
		}
		result
	}

	fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
		self.inner.remove_dir_all(path)
	}

	fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
		self.gate(Call::Rename(from.to_path_buf()))?;
		self.inner.rename(from, to)
	}

	fn sync_dir(&self, path: &Path) -> io::Result<()> {
		self.gate(Call::SyncDir(path.to_path_buf()))?;
		self.inner.sync_dir(path)
	}

	fn open_read(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
		self.inner.open_read(path)
	}

	fn open_append(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.gate(Call::OpenAppend(path.to_path_buf()))?;
		self.inner.open_append(path, opts)
	}

	fn create_new(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.gate(Call::CreateNew(path.to_path_buf()))?;
		self.inner.create_new(path, opts)
	}

	fn create_truncate(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		self.gate(Call::CreateTruncate(path.to_path_buf(), opts.class))?;
		self.inner.create_truncate(path, opts)
	}

	fn sync_file(&self, path: &Path) -> io::Result<()> {
		self.gate(Call::SyncFile(path.to_path_buf()))?;
		self.inner.sync_file(path)
	}

	fn replace_atomic(&self, path: &Path, content: &[u8]) -> Result<()> {
		self.gate(Call::ReplaceAtomic(path.to_path_buf())).map_err(crate::Error::from)?;
		let result = self.inner.replace_atomic(path, content);
		if result.is_ok() {
			self.done(Call::ReplaceAtomic(path.to_path_buf()));
		}
		result
	}

	fn file_len(&self, path: &Path) -> io::Result<u64> {
		self.inner.file_len(path)
	}

	fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
		self.inner.read(path)
	}

	fn read_prefix(&self, path: &Path, n: usize) -> io::Result<Option<Vec<u8>>> {
		self.inner.read_prefix(path, n)
	}

	fn copy(&self, from: &Path, to: &Path) -> io::Result<u64> {
		self.inner.copy(from, to)
	}

	fn lock_file(&self, dir: &Path) -> LockFile {
		self.inner.lock_file(dir)
	}

	fn ensure_spares(&self, n: usize) -> Option<SpareWait> {
		self.asked.lock().unwrap().push(n);
		let hook = self.spares.lock().unwrap().clone();
		match hook.and_then(|hook| hook(n)) {
			Some(Wait::Now(answer)) => Some(Box::pin(async move { answer })),
			Some(Wait::Never) => Some(Box::pin(std::future::pending())),
			None => self.inner.ensure_spares(n),
		}
	}

	fn pool_exhaustions(&self) -> u64 {
		self.inner.pool_exhaustions() + self.exhaustions.load(Ordering::Acquire)
	}

	fn pool(&self) -> Option<&dyn PoolControl> {
		self.inner.pool()
	}
}
