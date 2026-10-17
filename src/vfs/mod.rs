//! Files and the file system.
//!
//! [`File`] is the read seam of a table: positional reads of one open file. [`Fs`] is the
//! directory-level seam beside it, and [`FsFile`] the open file it hands out. Tables, the manifest,
//! the write-ahead log and the value log make their file calls through them; what an item here is
//! for that nothing calls yet is said at the item, with an `allow` that gives the reason.
//!
//! A file system is one of three kinds. [`StdFs`] is `std::fs` and is what a native build uses. A
//! pool ([`pool`]) keeps its files in memory or in a browser's origin private file system.
//! [`UnavailableFs`] is what a browser build has until a file system is mounted, and it refuses
//! every operation with an error that says so.

pub(crate) mod header;
pub(crate) mod mem;
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) mod opfs_raw;
pub(crate) mod pool;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
mod std_fs;
pub(crate) mod unavailable;

#[cfg(test)]
pub(crate) mod contract;

use std::ffi::OsString;
use std::fmt;
use std::fs::File as SysFile;
use std::future::Future;
use std::io::{self, SeekFrom};
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};

#[doc(hidden)]
pub use self::mem::CrashMode;
pub(crate) use self::mem::MemRaw;
#[doc(hidden)]
pub use self::pool::MountReport;
pub(crate) use self::pool::{PoolConfig, PoolControl, PoolFs};
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) use self::std_fs::{StdFile, StdFs};
pub(crate) use self::unavailable::UnavailableFs;
use crate::error::{Error, Result};
use crate::lockfile::LockFile;

/// Read-only random-access file abstraction for SSTable blocks.
pub trait File: Send + Sync {
	/// Reads bytes from the given offset into the buffer.
	///
	/// Like `pread`, this may return fewer bytes than the buffer holds without being at the end
	/// of the file. Callers that need the whole buffer use `read_exact_at`.
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;

	/// Returns total file size in bytes.
	fn size(&self) -> Result<u64>;

	/// Reads until the buffer is full or the file ends, and returns how many bytes were read.
	/// The count is short of the buffer only at the end of the file. A read that was interrupted
	/// is retried.
	fn read_up_to(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		let mut filled = 0;
		while filled < buf.len() {
			let at = offset.checked_add(filled as u64).ok_or_else(|| {
				Error::Io(Arc::new(std::io::Error::new(
					std::io::ErrorKind::InvalidInput,
					"read offset overflows",
				)))
			})?;
			match self.read_at(at, &mut buf[filled..]) {
				Ok(0) => break,
				// A reader that claims more than it was asked for cannot move the position
				// past the buffer.
				Ok(n) => filled += n.min(buf.len() - filled),
				Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::Interrupted => {}
				Err(e) => return Err(e),
			}
		}
		Ok(filled)
	}

	/// Fills the buffer from the given offset, or fails: a file that ends first is an
	/// `UnexpectedEof` I/O error and the contents of the buffer are unspecified.
	fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
		let read = self.read_up_to(offset, buf)?;
		if read < buf.len() {
			return Err(short_read(offset, buf.len(), read));
		}
		Ok(())
	}
}

/// The error for a read that found the file ending before the bytes that were asked for.
pub(crate) fn short_read(offset: u64, wanted: usize, got: usize) -> Error {
	Error::Io(Arc::new(std::io::Error::new(
		std::io::ErrorKind::UnexpectedEof,
		format!("short read: wanted {wanted} bytes at offset {offset}, the file has {got}"),
	)))
}

pub type InMemoryFile = Vec<u8>;

impl File for InMemoryFile {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		// An offset at or past the end reads nothing, as `pread` does, and a request near
		// `u64::MAX` cannot overflow.
		let Some(rest) = usize::try_from(offset).ok().and_then(|start| self.get(start..)) else {
			return Ok(0);
		};
		let bytes_read = rest.len().min(buf.len());
		buf[..bytes_read].copy_from_slice(&rest[..bytes_read]);
		Ok(bytes_read)
	}

	fn size(&self) -> Result<u64> {
		Ok(self.len() as u64)
	}
}

impl File for SysFile {
	#[cfg_attr(target_arch = "wasm32", allow(unused_variables))]
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		#[cfg(unix)]
		{
			std::os::unix::prelude::FileExt::read_at(self, buf, offset)
				.map_err(|e| Error::Io(e.into()))
		}

		#[cfg(windows)]
		{
			std::os::windows::prelude::FileExt::seek_read(self, buf, offset)
				.map_err(|e| Error::Io(e.into()))
		}

		#[cfg(target_os = "wasi")]
		{
			use std::os::fd::AsRawFd;
			let fd = self.as_raw_fd();
			let res = unsafe {
				libc::pread(
					fd,
					buf.as_mut_ptr() as *mut libc::c_void,
					buf.len(),
					offset as libc::off_t,
				)
			};
			if res < 0 {
				Err(Error::Io(std::io::Error::last_os_error().into()))
			} else {
				Ok(res as usize)
			}
		}

		#[cfg(all(target_arch = "wasm32", not(target_os = "wasi")))]
		{
			Err(Error::Io(
				std::io::Error::new(
					std::io::ErrorKind::Unsupported,
					"read_at is not supported on WASM without OPFS or WASI",
				)
				.into(),
			))
		}
	}

	#[allow(
		clippy::disallowed_methods,
		reason = "this is the std::fs::File implementation of the file trait, which StdFs hands out"
	)]
	fn size(&self) -> Result<u64> {
		self.metadata().map(|m| m.len()).map_err(|e| Error::Io(e.into()))
	}
}

/// Opens a file with the access mode required for `sync_all()` to work
/// on all platforms.
///
/// On Windows, `FlushFileBuffers` (called by `sync_all`) requires
/// `GENERIC_WRITE` — a read-only handle returns `ERROR_ACCESS_DENIED`.
/// On Unix, read-only access is sufficient for fsync.
#[allow(
	clippy::disallowed_methods,
	reason = "opens a std::fs file by path for the syncs of StdFs, of the migrators and of the tools; no other file system calls it"
)]
pub fn open_for_sync<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<SysFile> {
	#[cfg(target_os = "windows")]
	{
		std::fs::OpenOptions::new().read(true).write(true).open(path)
	}
	#[cfg(not(target_os = "windows"))]
	{
		SysFile::open(path)
	}
}

/// Fsyncs an existing file's data and metadata to disk (durability barrier).
///
/// Uses `sync_all` deliberately: readers depend on the durable file *size*
/// (e.g. SST footers are located relative to the end of the file), so syncing
/// data alone is not enough.
#[cfg_attr(
	all(target_arch = "wasm32", target_os = "unknown"),
	allow(dead_code, reason = "only `StdFs` syncs a file by its path")
)]
#[allow(
	clippy::disallowed_methods,
	reason = "called only by StdFs, so that the crash-test ledgers see the syncs of a native build exactly as before"
)]
pub(crate) fn fsync_file<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<()> {
	let file = open_for_sync(&path)?;
	file.sync_all()?;
	#[cfg(test)]
	sync_tracker::record(path.as_ref());
	Ok(())
}

/// Test-only ledger of files made durable via [`fsync_file`].
///
/// Lets crash-consistency tests simulate power loss faithfully: files NOT in
/// the ledger may legally lose their data (truncate to 0), while files in it
/// must survive intact.
#[cfg(test)]
pub(crate) mod sync_tracker {
	use std::collections::HashSet;
	use std::path::{Path, PathBuf};
	use std::sync::{LazyLock, Mutex};

	static SYNCED: LazyLock<Mutex<HashSet<PathBuf>>> = LazyLock::new(Default::default);

	// Canonicalize symmetrically in record() and was_synced(): on macOS,
	// temp dirs under /var/folders canonicalize to /private/var/folders, so
	// one-sided canonicalization would make lookups miss.
	fn key(path: &Path) -> PathBuf {
		path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
	}

	pub(crate) fn record(path: &Path) {
		SYNCED.lock().unwrap().insert(key(path));
	}

	pub(crate) fn was_synced(path: &Path) -> bool {
		SYNCED.lock().unwrap().contains(&key(path))
	}
}

// ---------------------------------------------------------------------------------------------
// The file system seam
// ---------------------------------------------------------------------------------------------

/// Whether the engine may use a pool file system. A pool's files live in a registry in memory, and
/// the parts of the engine that decide from the file system whether a database exists (the open
/// path, the manifest, the orphan sweep) have to go through it together: a database half-ported
/// would write an empty manifest over the real one. Until every one of them does, a pool is
/// refused when the options are validated, so that no build, release or pull request in between
/// ships a `with_file_system` that opens a database in the wrong place.
pub(crate) const POOL_FS_PORT_COMPLETE: bool = true;

/// What a file system is, which is what the engine tests when it must behave differently.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FsKind {
	/// `std::fs`: the files are the operating system's, and a native tool can read them.
	Std,
	/// A pool of pre-opened files with the names in a registry.
	Pool,
	/// Nothing is mounted: every operation fails.
	Unavailable,
}

/// What a path names, or what an entry of a directory is.
///
/// [`Fs::entry`] follows links, as `std::fs::metadata` does, so it answers `File`, `Dir`, or (of
/// `StdFs` only) `Other`. [`Fs::read_dir`] does not follow them, as `DirEntry::file_type` does, and
/// is the only call that answers `Symlink`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EntryKind {
	File,
	Dir,
	/// A symbolic link, whatever it points to, or to nothing. Only a listing of `StdFs` has one: a
	/// pool has no links. A caller that has to treat a link as what it points to asks
	/// [`DirEntry::resolved_kind`]; one that has to treat it as not being a file compares the kind
	/// with `File`, which a link never is.
	Symlink,
	/// Neither a regular file, a directory nor a link: a socket, a pipe, a device. `StdFs` only.
	#[cfg_attr(
		all(target_arch = "wasm32", target_os = "unknown"),
		allow(dead_code, reason = "only `StdFs` lists one, and there is none in a browser")
	)]
	Other,
}

/// One name in a directory listing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DirEntry {
	pub(crate) name: OsString,
	/// What the entry is itself: a link is a `Symlink`, not what it points to.
	pub(crate) kind: EntryKind,
}

impl DirEntry {
	/// What the entry is with a link followed to what it points to, which is what
	/// `std::fs::metadata` of its path says: the answer of the call sites that listed a
	/// directory and then asked for the metadata of each name. A link that points to nothing is
	/// a `NotFound` error, as `metadata` of it is, and so is any other failure to tell.
	pub(crate) fn resolved_kind(&self, fs: &dyn Fs, dir: &Path) -> io::Result<EntryKind> {
		if self.kind != EntryKind::Symlink {
			return Ok(self.kind);
		}
		fs.entry(&dir.join(&self.name))?.ok_or_else(|| {
			io::Error::new(io::ErrorKind::NotFound, "a link points to a file that is not there")
		})
	}
}

/// How a creation treats the spare files of a pool. A file system that has no spares ignores it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum SpareClass {
	/// May use every spare file. The default.
	#[default]
	Reserved,
	/// Fails with a pool-exhausted error unless more files are free than the commit path keeps for
	/// itself. For the outputs of a compaction, which must not eat the files that a commit needs.
	Opportunistic,
}

/// What a creation is given besides the path.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CreateOpts {
	/// The permission bits of a file that is created. Only a Unix `StdFs` reads it.
	pub(crate) mode: Option<u32>,
	pub(crate) class: SpareClass,
}

impl CreateOpts {
	pub(crate) const fn new(mode: Option<u32>) -> Self {
		Self {
			mode,
			class: SpareClass::Reserved,
		}
	}

	/// The same options, for a file that may only use the spare files that are not kept back.
	pub(crate) const fn opportunistic(mut self) -> Self {
		self.class = SpareClass::Opportunistic;
		self
	}
}

/// A wait for a pool to hold at least `n` free files. `Send`, because it only awaits a
/// notification.
pub(crate) type SpareWait = Pin<Box<dyn Future<Output = io::Result<()>> + Send + 'static>>;

/// The directory-level seam. Synchronous and object-safe: the engine calls it under its own locks,
/// so nothing in it may wait, and a call never calls back into the engine. The one exception is
/// the future of [`Fs::ensure_spares`], which an `async` context that holds no lock awaits.
///
/// Every backend honours this, and every call site is correct under all of it.
///
/// * `open_*` and `create_*` of a path that is open may return the same shared handle (a pool) or
///   an independent descriptor (`std`). A caller never relies on either: it keeps its own cursor
///   ([`FsCursor`]), and never assumes that two handles cannot see each other's writes, or that an
///   append is atomic across handles.
/// * `remove_file` unlinks: a handle that is open keeps reading the old bytes.
/// * `rename(from, to)` replaces `to`. On a pool it is crash-atomic and durable when it returns; on
///   `std` it is `std::fs::rename`, durable at the next `sync_dir`, as it is today.
/// * `create_new` is `AlreadyExists` if the path is present. `create_truncate` of a path that is
///   present gives an empty file, and open readers of the old one may or may not keep their bytes.
/// * `read_dir` is `NotFound` if the path is not there, and an error if it is not a directory. The
///   order is unspecified, so a caller sorts. `create_dir_all` is idempotent. A creation in a
///   directory that does not exist is `NotFound`.
/// * `replace_atomic` is `Error::Io` if it failed before the commit point, and the old content is
///   intact; from the commit point on it is `Error::ManifestWriteUncertain`.
/// * `exists`, `is_dir` and `is_file` read an error as `false`, as `Path` does. A caller that must
///   tell "absent" from "cannot tell" uses `entry` or `try_exists`.
///
/// Directory permissions and the symlink and hard link calls of a checkpoint are not part of this
/// seam: they stay on `std::fs`, and the code that uses them refuses a file system that is not
/// `std`.
pub(crate) trait Fs: Send + Sync + fmt::Debug + 'static {
	fn kind(&self) -> FsKind;

	/// The first call of `build_async`: a file system that has nothing mounted answers with a typed
	/// error here, before any `exists` can answer `false` and make an empty database of it.
	fn check_available(&self) -> io::Result<()> {
		Ok(())
	}

	// ---- namespace --------------------------------------------------------------------------

	/// What `path` names, a link followed to what it points to: `Ok(None)` if nothing (a link to
	/// nothing too), an error if it cannot be told. The primitive that the other questions about a
	/// path are answered from.
	fn entry(&self, path: &Path) -> io::Result<Option<EntryKind>>;

	/// `Path::try_exists`: the three-way answer, for the sites that read `try_exists()?`.
	fn try_exists(&self, path: &Path) -> io::Result<bool> {
		Ok(self.entry(path)?.is_some())
	}

	/// `Path::exists`: an error reads as `false`. `StdFs` is `Path::exists` itself, so that no
	/// decision on a native build changes.
	fn exists(&self, path: &Path) -> bool {
		self.try_exists(path).unwrap_or(false)
	}

	#[cfg_attr(
		not(test),
		allow(dead_code, reason = "the engine asks `entry` or `exists`, and only tests ask this")
	)]
	fn is_dir(&self, path: &Path) -> bool {
		matches!(self.entry(path), Ok(Some(EntryKind::Dir)))
	}

	#[cfg_attr(
		not(test),
		allow(dead_code, reason = "the engine asks `entry` or `exists`, and only tests ask this")
	)]
	fn is_file(&self, path: &Path) -> bool {
		matches!(self.entry(path), Ok(Some(EntryKind::File)))
	}

	fn create_dir_all(&self, path: &Path) -> io::Result<()>;

	/// The names in a directory, each with what it is itself: a link is a [`EntryKind::Symlink`],
	/// not what it points to (the kind that `std::fs::DirEntry::file_type` gives, which costs
	/// no call). A caller that used to look at each name with `std::fs::metadata` follows a
	/// link with [`DirEntry::resolved_kind`]; one that used `DirEntry::file_type` compares the
	/// kind as it is.
	fn read_dir(&self, path: &Path) -> io::Result<Vec<DirEntry>>;

	/// `NotFound` if `path` is not there.
	fn remove_file(&self, path: &Path) -> io::Result<()>;

	/// `NotFound` if `path` is not there.
	fn remove_dir_all(&self, path: &Path) -> io::Result<()>;

	/// `NotFound` if `from` is not there.
	fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

	/// Makes the names in a directory durable. `StdFs` is `lsm::fsync_directory`, which also keeps
	/// the test ledger of synced directories.
	fn sync_dir(&self, path: &Path) -> io::Result<()>;

	// ---- files ------------------------------------------------------------------------------

	/// An open file to read, `NotFound` if there is none. `FsFile::append` of it fails on a Unix
	/// `std` (a Windows read open is writable, because syncing needs it).
	fn open_read(&self, path: &Path) -> io::Result<Arc<dyn FsFile>>;

	/// A file to read and append to, created if it is absent. What the write-ahead log opens.
	fn open_append(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>>;

	/// A file to read and append to that must be there already: `NotFound` if the path is absent,
	/// and nothing is created. What the repair of a log segment and the newest file of the
	/// value log open, which both used to open an existing file and fail if it was gone. The
	/// provided method asks first and opens after, which is not atomic: every file system that
	/// is not a wrapper of another overrides it.
	fn open_append_existing(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
		if !self.try_exists(path)? {
			return Err(io::Error::new(io::ErrorKind::NotFound, "no such file"));
		}
		self.open_append(path, CreateOpts::default())
	}

	/// A new file, `AlreadyExists` if the path is present. What a new log segment is.
	fn create_new(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>>;

	/// An empty file to write and read back, truncated if it existed. What a table is: the handle
	/// that wrote it can serve as its reader.
	fn create_truncate(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>>;

	/// Makes the bytes of the file at `path` and its place in its directory durable. `StdFs` is
	/// `vfs::fsync_file`, which feeds the ledger of synced files that the crash tests read.
	fn sync_file(&self, path: &Path) -> io::Result<()> {
		self.open_read(path)?.sync()
	}

	// ---- composites the engine needs --------------------------------------------------------

	/// Replaces the file at `path` with `content`, all or nothing: the manifest. `StdFs` is
	/// `levels::replace_file_content` unchanged, failpoints included.
	fn replace_atomic(&self, path: &Path, content: &[u8]) -> Result<()>;

	/// The length of the file at `path`. What a listing does not say.
	fn file_len(&self, path: &Path) -> io::Result<u64> {
		self.open_read(path)?.len()
	}

	/// The whole file.
	fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
		let file = self.open_read(path)?;
		let len = usize::try_from(file.len()?).map_err(|_| {
			io::Error::new(io::ErrorKind::OutOfMemory, "the file does not fit in memory")
		})?;
		let mut content = vec![0u8; len];
		let read = File::read_up_to(&*file, 0, &mut content).map_err(into_io_error)?;
		content.truncate(read);
		Ok(content)
	}

	/// The first `n` bytes of the file, fewer if it is shorter, `None` if it is not there.
	fn read_prefix(&self, path: &Path, n: usize) -> io::Result<Option<Vec<u8>>> {
		let file = match self.open_read(path) {
			Ok(file) => file,
			Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
			Err(e) => return Err(e),
		};
		let want = usize::try_from(file.len()?).map_or(n, |len| len.min(n));
		let mut head = vec![0u8; want];
		let read = File::read_up_to(&*file, 0, &mut head).map_err(into_io_error)?;
		head.truncate(read);
		Ok(Some(head))
	}

	/// Copies the file at `from` over `to` and makes the copy durable, and returns its length.
	/// `StdFs` is `std::fs::copy`, which keeps the permissions of the source and does not sync.
	fn copy(&self, from: &Path, to: &Path) -> io::Result<u64> {
		let source = self.open_read(from)?;
		let copy = self.create_truncate(to, CreateOpts::default())?;
		let mut chunk = vec![0u8; 64 * 1024];
		let mut copied = 0u64;
		loop {
			let read = File::read_up_to(&*source, copied, &mut chunk).map_err(into_io_error)?;
			if read == 0 {
				break;
			}
			copy.append(&chunk[..read])?;
			copied += read as u64;
			if read < chunk.len() {
				break;
			}
		}
		copy.sync()?;
		Ok(copied)
	}

	// ---- lifecycle --------------------------------------------------------------------------

	/// The lock of the database in `dir`, not yet acquired. `StdFs` is the existing `flock` of a
	/// `LOCK` file; a pool registers the directory in its own set, so that one tree owns it.
	fn lock_file(&self, dir: &Path) -> LockFile;

	/// `None` when at least `n` files can be created now, which is always the case of `StdFs`.
	/// Otherwise a future that completes when they can. Awaited only where no lock is held.
	fn ensure_spares(&self, n: usize) -> Option<SpareWait> {
		let _ = n;
		None
	}

	/// How many creations a pool has refused for want of a file, ever. A background task reads it
	/// before and after a unit of work: if it advanced, a failure of that unit is not permanent,
	/// whatever the failure looks like after the layers it went through.
	fn pool_exhaustions(&self) -> u64 {
		0
	}

	/// What only a pool has: cleaning, the reserve, closing and the hooks of the tests. `None` of
	/// every other file system.
	fn pool(&self) -> Option<&dyn PoolControl> {
		None
	}
}

/// An open file that can be written as well as read, positional like the read seam it extends.
/// `Arc<dyn FsFile>` coerces to `Arc<dyn File>`, which is what a table and a block source take.
pub(crate) trait FsFile: File {
	/// The length now, as the file system has it.
	fn len(&self) -> io::Result<u64>;

	/// Writes all of `buf` at the end of the file, or fails. A failed append may have written a
	/// prefix of `buf`: `len` says how much, truthfully, and the write-ahead log cuts such a tail
	/// back with `set_len`. "The end" is the end as of the last `append`, `set_len` or open, so
	/// the caller never seeks.
	fn append(&self, buf: &[u8]) -> io::Result<()>;

	/// Shrinks or grows the file to `len` (a grown part reads as zeros) and makes `len` the end.
	fn set_len(&self, len: u64) -> io::Result<()>;

	/// Makes the bytes and the length of the file durable (`fsync`, or `flush` of a browser's
	/// access handle, which is a barrier for ordering and not a promise about a power cut).
	fn sync(&self) -> io::Result<()>;
}

/// What the write-ahead log and the tests pass where they used to pass a `std::fs::File`.
pub(crate) trait IntoFsFile {
	fn into_fs_file(self) -> Arc<dyn FsFile>;
}

impl IntoFsFile for Arc<dyn FsFile> {
	fn into_fs_file(self) -> Arc<dyn FsFile> {
		self
	}
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl IntoFsFile for SysFile {
	fn into_fs_file(self) -> Arc<dyn FsFile> {
		Arc::new(StdFile::from_file(self))
	}
}

/// `io::Write` over [`FsFile::append`]: what a table writer and the buffer of the write-ahead log
/// are given. `flush` does nothing, as it does for `std::fs::File`.
pub(crate) struct FsWriter(pub(crate) Arc<dyn FsFile>);

impl io::Write for FsWriter {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		self.0.append(buf)?;
		Ok(buf.len())
	}

	fn flush(&mut self) -> io::Result<()> {
		Ok(())
	}
}

/// A private cursor over a shared positional file: `Read` and `Seek` for the reader of the
/// write-ahead log, and `File` for its look-ahead.
pub(crate) struct FsCursor {
	file: Arc<dyn FsFile>,
	pos: u64,
}

impl FsCursor {
	pub(crate) fn new(file: Arc<dyn FsFile>) -> Self {
		Self::at(file, 0)
	}

	pub(crate) fn at(file: Arc<dyn FsFile>, pos: u64) -> Self {
		Self {
			file,
			pos,
		}
	}
}

impl io::Read for FsCursor {
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		let read = self.file.read_at(self.pos, buf).map_err(into_io_error)?;
		self.pos += read as u64;
		Ok(read)
	}
}

impl io::Seek for FsCursor {
	fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
		let target = match from {
			SeekFrom::Start(at) => Some(at),
			SeekFrom::Current(by) => self.pos.checked_add_signed(by),
			SeekFrom::End(by) => self.file.len()?.checked_add_signed(by),
		};
		let at = target.ok_or_else(|| {
			io::Error::new(
				io::ErrorKind::InvalidInput,
				"invalid seek to a negative or overflowing position",
			)
		})?;
		self.pos = at;
		Ok(at)
	}
}

impl File for FsCursor {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		self.file.read_at(offset, buf)
	}

	fn size(&self) -> Result<u64> {
		self.file.size()
	}
}

/// The opaque, cloneable file system that a `Tree` is built over. The traits behind it are private
/// so that they can change.
///
/// `FileSystem::std` is the default of a native build, and the files are the operating system's.
/// A build for a browser has none until one is mounted, and building a tree without one fails with
/// an error that says so.
#[derive(Clone, Debug)]
pub struct FileSystem(pub(crate) Arc<dyn Fs>);

impl FileSystem {
	/// The operating system's files, through `std::fs`. The default of a native build.
	#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
	pub fn std() -> Self {
		Self(Arc::new(StdFs))
	}

	/// A file system that lives in memory and is lost with the process, on any target.
	///
	/// It is a pool over memory, the same code that keeps the files of a browser, so it needs no
	/// runtime and no thread: it is made and used from a plain function, and a file is made
	/// when it is asked for. It keeps what a simulated crash would lose (see
	/// [`FileSystem::simulate_crash`]) at the cost of a second copy of what has not been synced.
	pub fn memory() -> Self {
		match PoolFs::<MemRaw>::mount_sync(MemRaw::new(), PoolConfig::memory()) {
			Ok((fs, _)) => Self(fs),
			Err(_) => {
				Self(Arc::new(UnavailableFs::new("the in-memory file system could not be mounted")))
			}
		}
	}

	/// The database `name` in the origin private file system of the browser: mounts it, or makes
	/// it if it is not there. If another page has it open, it waits a few seconds for the lock,
	/// which covers the usual release after a reload (see
	/// [`opfs_with_lock_wait`](Self::opfs_with_lock_wait) for another wait).
	///
	/// This is the persistent file system of a browser. Give it to a tree with
	/// [`TreeBuilder::with_file_system`](crate::TreeBuilder::with_file_system) and build the tree
	/// with [`TreeBuilder::build_async`](crate::TreeBuilder::build_async).
	///
	/// # What the browser allows
	///
	/// * **A dedicated worker only.** The files are opened through synchronous access handles,
	///   which exist only in a dedicated worker. On the main thread of a page, in a shared worker
	///   or in a service worker, this fails with `Unsupported` before anything is made. Use
	///   [`FileSystem::memory`] there, which is not persistent. There is no threads support: the
	///   engine runs on the one thread of the worker.
	/// * **One page at a time.** Opening a database takes an exclusive lock of its directory, so a
	///   second tab, page or worker that mounts it fails once its wait for the lock is over, with
	///   an error for which [`Error::is_database_locked`] is true. A page that is reloaded may
	///   leave the lock held for a few seconds after it went, which is what the wait is for. If the
	///   old page holds it for longer, the mount fails with the same error and a later mount
	///   succeeds; [`opfs_with_lock_wait`](Self::opfs_with_lock_wait) sets a longer wait.
	/// * **Close before the page goes away.** A database that was not closed is opened by replaying
	///   its log, which is what happens after a crash, and the lock of the old page may still be
	///   held when the new one asks. Await [`Tree::close`](crate::Tree::close) and then
	///   [`FileSystem::close`] before the worker is terminated.
	/// * **`flush` orders, it does not promise.** What a commit with
	///   [`Durability::Immediate`](crate::Durability::Immediate) waits for is the flush of the
	///   access handle of the browser, which puts the writes before it ahead of the writes after
	///   it. What the browser promises about a power cut, on any platform, is not known, and this
	///   crate does not claim it. [`Durability::Eventual`](crate::Durability::Eventual), the
	///   default, does not wait for it at all.
	/// * **The storage may be evicted** unless the browser has been told to keep it: see
	///   [`FileSystem::persisted`]. A full origin fails a write with an error of kind
	///   `StorageFull`. The background flush and compaction try again after it, and a commit that
	///   meets it fails, and so may stop the database if part of its group was already applied.
	///
	/// # Errors
	///
	/// All are [`Error::Io`]: `Unsupported` where there are no synchronous access handles or no
	/// file system for the origin (some private windows); `PermissionDenied`, for which
	/// [`Error::is_database_locked`] is true, if the database is open elsewhere; `StorageFull` if
	/// the origin has no room for the files of a new database; `InvalidInput` if `name` is not one
	/// path component, or the directory of that name holds entries that are not a database's
	/// (it is left as it is).
	#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
	pub async fn opfs(name: &str) -> Result<Self> {
		Self::mount_opfs(name, PoolConfig::default()).await
	}

	/// [`FileSystem::opfs`] with the time that it waits for a database that another page has open
	/// before it gives up: it tries every few tens of milliseconds until `wait` has passed. A
	/// `wait` of zero tries once.
	#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
	pub async fn opfs_with_lock_wait(name: &str, wait: std::time::Duration) -> Result<Self> {
		Self::mount_opfs(
			name,
			PoolConfig {
				lock_deadline: wait,
				..PoolConfig::default()
			},
		)
		.await
	}

	#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
	async fn mount_opfs(name: &str, cfg: PoolConfig) -> Result<Self> {
		// Before anything is made: a context that cannot open the files must not leave a directory
		// behind.
		if !crate::storage::opfs::sync_access_handles_available() {
			return Err(Error::Io(Arc::new(io::Error::new(
				io::ErrorKind::Unsupported,
				"the origin private file system can be used only in a dedicated worker: a page, a \
				 shared worker and a service worker have no synchronous access handles",
			))));
		}
		let raw = opfs_raw::OpfsRaw::open_pool(name).await?;
		let (fs, _report) = PoolFs::mount(raw, cfg).await?;
		Ok(Self(fs))
	}

	/// Removes the database `name` of the origin private file system, with everything in it. A
	/// database that is not there is already removed, so this can be repeated.
	///
	/// The database must be closed ([`FileSystem::close`]) here and open nowhere else. If it is
	/// open, this fails with an error for which [`Error::is_database_locked`] is true, and nothing
	/// is removed. A directory that holds entries that are not a database's is refused with
	/// `InvalidInput` and left as it is: only the lock and the files of a pool are removed. The
	/// browser does not promise that a removal that fails changed nothing.
	#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
	pub async fn destroy_opfs(name: &str) -> Result<()> {
		match opfs_raw::OpfsRaw::destroy(name).await {
			Ok(()) => Ok(()),
			Err(e) if crate::storage::opfs::is_locked(&e) => {
				Err(Error::Io(Arc::new(database_locked(e))))
			}
			Err(e) => Err(Error::from(e)),
		}
	}

	/// Whether the browser keeps the storage of this origin until the user clears it, which is what
	/// stops it from evicting the databases when the disk fills. It is `false` until a page has
	/// asked for it with `navigator.storage.persist()`, which a worker cannot call: the page asks,
	/// and a database can check here that it was granted. `Unsupported` where the browser has no
	/// such question.
	#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
	pub async fn persisted() -> Result<bool> {
		opfs_raw::storage_persisted().await.map_err(Error::from)
	}

	/// Flushes what has to be flushed, cleans the files that were removed, and releases every
	/// handle of the file system, and with them the lock of a database in a browser. Calling it
	/// again does nothing. A file that is read or written after it fails with `NotConnected`, so
	/// close the trees first: a tree whose file system is closed under it fails what it does next.
	/// Nothing to do for a file system that is not a pool.
	///
	/// The flushes are made in one go, and take longer the more files the database has, so a page
	/// that is going away calls this early and does not leave it to the last moment. The last
	/// reference to a file system that was not closed releases the handles too, without the flush.
	pub async fn close(&self) -> Result<()> {
		match self.0.pool() {
			Some(pool) => pool.close().map_err(Error::from),
			None => Ok(()),
		}
	}

	/// Releases every handle without flushing anything: what is left of a worker that died.
	#[doc(hidden)]
	pub fn abandon(&self) {
		if let Some(pool) = self.0.pool() {
			pool.abandon();
		}
	}

	/// A file system that holds what a crash would have left of this one, mounted: the memory file
	/// system only. This one is not changed, so a test can crash one state in every way.
	#[doc(hidden)]
	pub fn simulate_crash(&self, mode: CrashMode) -> Result<FileSystem> {
		match self.0.pool() {
			Some(pool) => pool.simulate_crash(mode).map(Self).map_err(Error::from),
			None => Err(Error::InvalidArgument(
				"only the memory file system can simulate a crash".to_string(),
			)),
		}
	}

	/// The names in a directory, sorted. Empty if there is no such directory.
	#[doc(hidden)]
	pub fn list_for_test(&self, dir: &str) -> Vec<String> {
		let mut names: Vec<String> = self
			.0
			.read_dir(Path::new(dir))
			.map(|entries| {
				entries.into_iter().map(|entry| entry.name.to_string_lossy().into_owned()).collect()
			})
			.unwrap_or_default();
		names.sort();
		names
	}

	/// The length of a file, if it is there.
	#[doc(hidden)]
	pub fn file_len_for_test(&self, path: &str) -> Option<u64> {
		self.0.file_len(Path::new(path)).ok()
	}

	/// Cuts a file to `len` without flushing, as a crash in the middle of a write would: for a
	/// pool.
	#[doc(hidden)]
	pub fn raw_truncate_for_test(&self, path: &str, len: u64) -> Result<()> {
		match self.0.pool() {
			Some(pool) => pool.raw_truncate(Path::new(path), len).map_err(Error::from),
			None => {
				Err(Error::InvalidArgument("only a pool cuts a file without flushing".to_string()))
			}
		}
	}

	/// What the mount of a pool found: for a pool, the files, the spares, the orphans.
	#[doc(hidden)]
	pub fn mount_report(&self) -> Option<MountReport> {
		self.0.pool().and_then(|pool| pool.mount_report())
	}
}

/// The file system that `Options::default()` and the write-ahead log start from: `std::fs` where
/// there is one, and a file system that refuses everything in a browser, so that a tree built there
/// without a mount fails loudly and creates nothing.
pub(crate) fn default_fs() -> Arc<dyn Fs> {
	static DEFAULT: LazyLock<Arc<dyn Fs>> = LazyLock::new(make_default_fs);
	Arc::clone(&DEFAULT)
}

/// The file system of `Options::default()`: `default_fs()`, except in the tests of the crate that
/// are run over a pool (`SURREALKV_TEST_FS`, see `test::tfs`). Only `Options` is switched; the
/// code that has no `Options` keeps `default_fs()`.
pub(crate) fn options_default_fs() -> Arc<dyn Fs> {
	#[cfg(all(test, not(target_arch = "wasm32")))]
	if let Some(fs) = crate::test::tfs::options_fs() {
		return fs;
	}
	default_fs()
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
fn make_default_fs() -> Arc<dyn Fs> {
	Arc::new(StdFs)
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
fn make_default_fs() -> Arc<dyn Fs> {
	Arc::new(UnavailableFs::no_file_system())
}

// ---------------------------------------------------------------------------------------------
// Errors that a file system raises on purpose
// ---------------------------------------------------------------------------------------------

/// The payload of the `io::Error` that a pool answers a creation with when it has no free file.
/// It crosses every layer that takes an `io::Error`, and the conversions into [`Error`] and
/// `wal::Error` look for it (`is_pool_exhausted`) and make the typed error of it, which the
/// background tasks treat as something that passes once the pool is refilled.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PoolExhausted;

impl fmt::Display for PoolExhausted {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("the file pool has no free file")
	}
}

impl std::error::Error for PoolExhausted {}

/// The error of a creation that found no free file in a pool.
pub(crate) fn pool_exhausted() -> io::Error {
	io::Error::other(PoolExhausted)
}

/// The payload of the `io::Error` that a pool answers a creation with when it already holds as many
/// files as it may (`PoolConfig::max_files`). Unlike [`PoolExhausted`] it does not pass by waiting
/// for a refill: files have to be released first (a table that a snapshot pins, a log that is not
/// removed yet), so it has a type of its own, and its kind is `StorageFull`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PoolFull {
	pub(crate) max_files: usize,
}

impl fmt::Display for PoolFull {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "the file pool holds its limit of {} files and none is free", self.max_files)
	}
}

impl std::error::Error for PoolFull {}

/// The error of a creation that found the pool at its cap.
pub(crate) fn pool_full(max_files: usize) -> io::Error {
	io::Error::new(
		io::ErrorKind::StorageFull,
		PoolFull {
			max_files,
		},
	)
}

/// Whether `error` is the one that [`pool_full`] makes.
pub(crate) fn is_pool_full(error: &io::Error) -> bool {
	error.get_ref().is_some_and(|inner| inner.is::<PoolFull>())
}

/// Whether `error` is the one that [`pool_exhausted`] makes.
pub(crate) fn is_pool_exhausted(error: &io::Error) -> bool {
	error.get_ref().is_some_and(|inner| inner.is::<PoolExhausted>())
}

/// The payload of the error of a mount that found the lock of the pool held by somebody else, and
/// went on trying until its deadline: another tab or worker has the database open, or the page that
/// had it is gone and the browser has not released what it held yet. The kind is
/// `PermissionDenied`. The error of the browser that said so is kept as the source of this one, so
/// that a caller that wants the exception finds it.
#[derive(Debug)]
pub(crate) struct DatabaseLocked {
	cause: io::Error,
}

impl DatabaseLocked {
	/// What the file system answered when the lock was asked for the last time.
	#[allow(dead_code, reason = "read by the browser tests of the mount")]
	pub(crate) fn cause(&self) -> &io::Error {
		&self.cause
	}
}

impl fmt::Display for DatabaseLocked {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"the database is open in another tab or worker (the page that holds it may be in a long \
			 synchronous operation): {}",
			self.cause
		)
	}
}

impl std::error::Error for DatabaseLocked {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		Some(&self.cause)
	}
}

/// The error of a mount that could not take the lock of the pool before its deadline.
pub(crate) fn database_locked(cause: io::Error) -> io::Error {
	io::Error::new(
		io::ErrorKind::PermissionDenied,
		DatabaseLocked {
			cause,
		},
	)
}

/// Whether `error` is the one that [`database_locked`] makes.
pub(crate) fn is_database_locked(error: &io::Error) -> bool {
	error.get_ref().is_some_and(|inner| inner.is::<DatabaseLocked>())
}

impl Error {
	/// Whether this is the error of a file system that could not be mounted, or a database that
	/// could not be removed, because another tab, page or worker (or the page that was there
	/// before a reload, which the browser has not finished releasing) has it open. Only one page
	/// can use a database in a browser at a time. Waiting and trying again is the right answer.
	pub fn is_database_locked(&self) -> bool {
		matches!(self, Error::Io(e) if is_database_locked(e))
	}
}

/// The payload of the error that a mount or a removal answers with for a directory that holds
/// entries that are not a pool's: `InvalidInput`. They belong to somebody else, so a mount does not
/// put a pool among them and a removal does not delete them.
#[cfg_attr(
	not(all(target_arch = "wasm32", target_os = "unknown")),
	allow(
		dead_code,
		reason = "the directory of a pool is only checked over a browser's file system"
	)
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct NotAPool {
	pub(crate) name: String,
	/// The entries that are not a pool's, or the first few of them.
	pub(crate) entries: Vec<String>,
}

impl fmt::Display for NotAPool {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"the directory {:?} is not a database: it holds entries that are not a pool's ({}), which are left as they are",
			self.name,
			self.entries.join(", ")
		)
	}
}

impl std::error::Error for NotAPool {}

/// The error for a directory that holds `entries` that are not a pool's.
#[cfg_attr(
	not(all(target_arch = "wasm32", target_os = "unknown")),
	allow(
		dead_code,
		reason = "the directory of a pool is only checked over a browser's file system"
	)
)]
pub(crate) fn not_a_pool(name: &str, mut entries: Vec<String>) -> io::Error {
	entries.sort();
	entries.truncate(8);
	io::Error::new(
		io::ErrorKind::InvalidInput,
		NotAPool {
			name: name.to_string(),
			entries,
		},
	)
}

/// Whether `error` is the one that [`not_a_pool`] makes.
#[allow(dead_code, reason = "read by the browser tests of the mount")]
pub(crate) fn is_not_a_pool(error: &io::Error) -> bool {
	error.get_ref().is_some_and(|inner| inner.is::<NotAPool>())
}

/// The error of an operation on a file system that has nothing mounted: `Unsupported`, with a
/// text that names what to do.
pub(crate) fn unavailable_error(reason: &str) -> io::Error {
	io::Error::new(io::ErrorKind::Unsupported, reason.to_string())
}

/// The `io::Error` of an engine `Error`, for the places where a file API that speaks `io` calls
/// one that speaks `Error`. An I/O error comes back as it was (so does a pool that is exhausted),
/// and anything else is wrapped.
pub(crate) fn into_io_error(error: Error) -> io::Error {
	match error {
		Error::Io(shared) => match Arc::try_unwrap(shared) {
			Ok(error) => error,
			Err(shared) if is_pool_exhausted(&shared) => pool_exhausted(),
			Err(shared) => io::Error::new(shared.kind(), shared.to_string()),
		},
		Error::FilePoolExhausted => pool_exhausted(),
		other => io::Error::other(other),
	}
}
