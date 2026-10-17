//! The file system of a native build: `std::fs`, with the semantics the engine relies on today.
//!
//! Nothing here is new behaviour. Each operation is the call that the engine makes at the site it
//! will be moved from, so that moving the site changes no decision and no syscall:
//!
//! * `open_append` is the options of `Wal::open_wal_file`, and `create_new` those of
//!   `Wal::create_new_wal_file`;
//! * `create_truncate` is `File::create` of a table, opened for reading as well so that the handle
//!   that wrote it can serve as its reader;
//! * `exists`, `try_exists`, `is_dir` and `is_file` are the `Path` methods of the same names, and
//!   `entry` answers the way `try_exists` does, so that "not found" is the only error that reads as
//!   "absent";
//! * `sync_dir`, `sync_file`, `replace_atomic` and `lock_file` are `lsm::fsync_directory`,
//!   `vfs::fsync_file`, `levels::replace_file_content` and `LockFile::new`, which keep the
//!   failpoints and the ledgers of the crash tests.
//!
//! A file here is a descriptor of its own: two `open_*` calls of one path give two descriptors,
//! unlike a pool, which gives views of one handle. The engine is written to be correct under both.

use std::fs::{File as SysFile, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use super::{CreateOpts, DirEntry, EntryKind, Fs, FsFile, FsKind};
use crate::error::Result;
use crate::lockfile::LockFile;
use crate::vfs::{self, File};

/// `std::fs`. A unit: there is nothing to configure and nothing to keep.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct StdFs;

/// An open `std::fs::File`.
///
/// Reads are the `pread` of the existing `impl File for std::fs::File`, so the read path is the
/// same call. There is no end tracked here: a file opened to append has `O_APPEND`, and a file
/// made with `create_truncate` is written sequentially from its start, as `File::create` is today.
///
/// `sync` is `sync_all` and records nothing in the crash tests' ledger of synced files. That
/// ledger is fed by `Fs::sync_file`, as it is today, so moving a call site to `FsFile::sync` does
/// not change what a test takes to be durable.
pub(crate) struct StdFile {
	file: SysFile,
	/// Whether `set_len` must move the cursor of the descriptor to the new end. A descriptor
	/// opened with `O_APPEND` writes at the end whatever its cursor is, and any other one writes
	/// at its cursor, so a file cut back (or grown) without moving it would leave a hole.
	reposition: bool,
}

impl StdFile {
	/// A file that was opened by something other than `StdFs`: it may or may not append, so the
	/// cursor is moved after `set_len`.
	pub(crate) fn from_file(file: SysFile) -> Self {
		Self {
			file,
			reposition: true,
		}
	}

	fn appending(file: SysFile) -> Self {
		Self {
			file,
			reposition: false,
		}
	}
}

impl File for StdFile {
	#[inline]
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		<SysFile as File>::read_at(&self.file, offset, buf)
	}

	#[inline]
	fn size(&self) -> Result<u64> {
		<SysFile as File>::size(&self.file)
	}
}

impl FsFile for StdFile {
	fn len(&self) -> io::Result<u64> {
		self.file.metadata().map(|metadata| metadata.len())
	}

	#[inline]
	fn append(&self, buf: &[u8]) -> io::Result<()> {
		(&self.file).write_all(buf)
	}

	fn set_len(&self, len: u64) -> io::Result<()> {
		self.file.set_len(len)?;
		if self.reposition {
			(&self.file).seek(SeekFrom::Start(len))?;
		}
		Ok(())
	}

	fn sync(&self) -> io::Result<()> {
		self.file.sync_all()
	}
}

/// The options a creation starts from: the permission bits, where the platform has them.
#[cfg_attr(not(unix), allow(unused_variables))]
fn apply_mode(options: &mut OpenOptions, opts: CreateOpts) {
	#[cfg(unix)]
	{
		use std::os::unix::fs::OpenOptionsExt;
		if let Some(mode) = opts.mode {
			options.mode(mode);
		}
	}
}

impl Fs for StdFs {
	fn kind(&self) -> FsKind {
		FsKind::Std
	}

	fn entry(&self, path: &Path) -> io::Result<Option<EntryKind>> {
		// `metadata` follows a link: what the path leads to is what it names, and a link to
		// nothing names nothing.
		match std::fs::metadata(path) {
			Ok(metadata) if metadata.is_dir() => Ok(Some(EntryKind::Dir)),
			Ok(metadata) if metadata.is_file() => Ok(Some(EntryKind::File)),
			Ok(_) => Ok(Some(EntryKind::Other)),
			Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
			Err(e) => Err(e),
		}
	}

	fn try_exists(&self, path: &Path) -> io::Result<bool> {
		path.try_exists()
	}

	fn exists(&self, path: &Path) -> bool {
		path.exists()
	}

	fn is_dir(&self, path: &Path) -> bool {
		path.is_dir()
	}

	fn is_file(&self, path: &Path) -> bool {
		path.is_file()
	}

	fn create_dir_all(&self, path: &Path) -> io::Result<()> {
		std::fs::create_dir_all(path)
	}

	/// Each entry is what the directory says it is, which costs no call (`d_type`), and a link is
	/// not followed: the engine's call sites that listed a directory with `std::fs::read_dir` took
	/// the type of an entry from `DirEntry::file_type`, which does not follow a link, or asked
	/// `std::fs::metadata` of each path, which does, and the two differ for a link. A link is a
	/// `Symlink`, so that a site of the first kind skips it by comparing the kind with `File` and a
	/// site of the second kind follows it with `DirEntry::resolved_kind`: the table of who does
	/// which is in the notes of `wal::list_segment_ids_in`.
	fn read_dir(&self, path: &Path) -> io::Result<Vec<DirEntry>> {
		let mut entries = Vec::new();
		for entry in std::fs::read_dir(path)? {
			let entry = entry?;
			let file_type = entry.file_type()?;
			let kind = if file_type.is_symlink() {
				EntryKind::Symlink
			} else if file_type.is_dir() {
				EntryKind::Dir
			} else if file_type.is_file() {
				EntryKind::File
			} else {
				EntryKind::Other
			};
			entries.push(DirEntry {
				name: entry.file_name(),
				kind,
			});
		}
		Ok(entries)
	}

	fn remove_file(&self, path: &Path) -> io::Result<()> {
		std::fs::remove_file(path)
	}

	fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
		std::fs::remove_dir_all(path)
	}

	fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
		std::fs::rename(from, to)
	}

	fn sync_dir(&self, path: &Path) -> io::Result<()> {
		crate::lsm::fsync_directory(path)
	}

	fn open_read(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
		// `File::open`, except on Windows, where `FlushFileBuffers` needs write access and a read
		// handle could not be synced.
		Ok(Arc::new(StdFile::from_file(vfs::open_for_sync(path)?)))
	}

	fn open_append(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		let mut options = OpenOptions::new();
		options.read(true).write(true).create(true).append(true);
		apply_mode(&mut options, opts);
		Ok(Arc::new(StdFile::appending(options.open(path)?)))
	}

	fn open_append_existing(&self, path: &Path) -> io::Result<Arc<dyn FsFile>> {
		// Read, write and append, with no `create`: a path that is not there is `NotFound`, and a
		// link to nothing is not followed to make its target. The repair of a log segment used to
		// open `write(true)` alone, and the newest file of the value log `File::open`; this is the
		// descriptor that either needs to cut a file and to read it, which is one that appends.
		let mut options = OpenOptions::new();
		options.read(true).write(true).append(true);
		Ok(Arc::new(StdFile::appending(options.open(path)?)))
	}

	fn create_new(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		let mut options = OpenOptions::new();
		options.read(true).write(true).create_new(true).append(true);
		apply_mode(&mut options, opts);
		Ok(Arc::new(StdFile::appending(options.open(path)?)))
	}

	fn create_truncate(&self, path: &Path, opts: CreateOpts) -> io::Result<Arc<dyn FsFile>> {
		let mut options = OpenOptions::new();
		options.read(true).write(true).create(true).truncate(true);
		apply_mode(&mut options, opts);
		// Written from its start by a cursor, so `set_len` has to move that cursor.
		Ok(Arc::new(StdFile::from_file(options.open(path)?)))
	}

	fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
		std::fs::read(path)
	}

	fn read_prefix(&self, path: &Path, n: usize) -> io::Result<Option<Vec<u8>>> {
		use std::io::Read;

		let file = match SysFile::open(path) {
			Ok(file) => file,
			Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
			Err(e) => return Err(e),
		};
		let mut head = Vec::with_capacity(n);
		file.take(n as u64).read_to_end(&mut head)?;
		Ok(Some(head))
	}

	fn sync_file(&self, path: &Path) -> io::Result<()> {
		vfs::fsync_file(path)
	}

	fn replace_atomic(&self, path: &Path, content: &[u8]) -> Result<()> {
		crate::levels::replace_file_content(path, content)
	}

	fn file_len(&self, path: &Path) -> io::Result<u64> {
		std::fs::metadata(path).map(|metadata| metadata.len())
	}

	fn copy(&self, from: &Path, to: &Path) -> io::Result<u64> {
		std::fs::copy(from, to)
	}

	fn lock_file(&self, dir: &Path) -> LockFile {
		LockFile::new(dir)
	}
}
