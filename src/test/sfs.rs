//! `std::fs` for the tests: the same names and the same types of answer, over the file system the
//! test runs in.
//!
//! A test that looks into the directory of a database (reads a table, cuts a segment, lists the
//! files, makes a directory) used to call `std::fs`, which is the disk. When the engine runs in a
//! pool the files are not on the disk, so such a test says `use super::sfs as fs` in place of
//! `use std::fs` and goes on calling `fs::read`, `fs::write`, `fs::read_dir`, `fs::File` and
//! `fs::OpenOptions`. Over `std` (the default of every run) each call is the `std::fs` call it
//! stands for, so a test that is moved here is the test it was. In a pool run each call goes to
//! the file system that `Options::default()` has, which is the one that the tree of the test
//! lives in.
//!
//! What a pool does not have (permissions, links) is not here: a test that needs it stays on
//! `std::fs` and is run over `std` (see `tfs::EXCLUDED`).

use std::ffi::OsString;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::tfs;
use crate::vfs::{self, CreateOpts, EntryKind, Fs, FsFile};

/// The pool of the test, or `None` for the disk.
fn pool() -> Option<Arc<dyn Fs>> {
	tfs::ambient()
}

/// Makes the directory that a file is to be made in, in a pool: a `TempDir` is on the disk and
/// not in the pool, and the tests that put a file straight into one never made it there.
fn ensure_parent(fs: &dyn Fs, path: &Path) -> io::Result<()> {
	match path.parent() {
		Some(parent) if !parent.as_os_str().is_empty() && !fs.exists(parent) => {
			fs.create_dir_all(parent)
		}
		_ => Ok(()),
	}
}

/// Whether the files of the test are in a pool and not on the disk (only a Linux-only helper of the
/// tests asks).
#[cfg(target_os = "linux")]
pub(crate) fn on_pool() -> bool {
	pool().is_some()
}

fn not_found(what: &str) -> io::Error {
	io::Error::new(io::ErrorKind::NotFound, format!("no such file or directory: {what}"))
}

// ---------------------------------------------------------------------------------------------
// Directories and whole files
// ---------------------------------------------------------------------------------------------

pub(crate) fn create_dir_all<P: AsRef<Path>>(path: P) -> io::Result<()> {
	match pool() {
		None => std::fs::create_dir_all(path),
		Some(fs) => fs.create_dir_all(path.as_ref()),
	}
}

/// One directory: its parent has to be there, and it must not be.
pub(crate) fn create_dir<P: AsRef<Path>>(path: P) -> io::Result<()> {
	match pool() {
		None => std::fs::create_dir(path),
		Some(fs) => {
			let path = path.as_ref();
			if fs.exists(path) {
				return Err(io::Error::new(io::ErrorKind::AlreadyExists, "the path exists"));
			}
			if let Some(parent) = path.parent() {
				if !parent.as_os_str().is_empty() && !fs.exists(parent) {
					return Err(not_found(&parent.display().to_string()));
				}
			}
			fs.create_dir_all(path)
		}
	}
}

pub(crate) fn remove_dir_all<P: AsRef<Path>>(path: P) -> io::Result<()> {
	match pool() {
		None => std::fs::remove_dir_all(path),
		Some(fs) => fs.remove_dir_all(path.as_ref()),
	}
}

/// An empty directory.
pub(crate) fn remove_dir<P: AsRef<Path>>(path: P) -> io::Result<()> {
	match pool() {
		None => std::fs::remove_dir(path),
		Some(fs) => {
			if !fs.read_dir(path.as_ref())?.is_empty() {
				return Err(io::Error::new(
					io::ErrorKind::DirectoryNotEmpty,
					"the directory is not empty",
				));
			}
			fs.remove_dir_all(path.as_ref())
		}
	}
}

pub(crate) fn remove_file<P: AsRef<Path>>(path: P) -> io::Result<()> {
	match pool() {
		None => std::fs::remove_file(path),
		Some(fs) => fs.remove_file(path.as_ref()),
	}
}

pub(crate) fn rename<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> io::Result<()> {
	match pool() {
		None => std::fs::rename(from, to),
		Some(fs) => {
			ensure_parent(&*fs, to.as_ref())?;
			fs.rename(from.as_ref(), to.as_ref())
		}
	}
}

pub(crate) fn read<P: AsRef<Path>>(path: P) -> io::Result<Vec<u8>> {
	match pool() {
		None => std::fs::read(path),
		Some(fs) => fs.read(path.as_ref()),
	}
}

pub(crate) fn write<P: AsRef<Path>, C: AsRef<[u8]>>(path: P, contents: C) -> io::Result<()> {
	match pool() {
		None => std::fs::write(path, contents),
		Some(fs) => {
			ensure_parent(&*fs, path.as_ref())?;
			let file = fs.create_truncate(path.as_ref(), CreateOpts::default())?;
			if !contents.as_ref().is_empty() {
				file.append(contents.as_ref())?;
			}
			Ok(())
		}
	}
}

pub(crate) fn copy<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> io::Result<u64> {
	match pool() {
		None => std::fs::copy(from, to),
		Some(fs) => {
			ensure_parent(&*fs, to.as_ref())?;
			fs.copy(from.as_ref(), to.as_ref())
		}
	}
}

pub(crate) use std::fs::Permissions;

/// Permissions are the disk's: a test that sets them runs over `std`.
pub(crate) fn set_permissions<P: AsRef<Path>>(path: P, perm: Permissions) -> io::Result<()> {
	std::fs::set_permissions(path, perm)
}

pub(crate) fn exists<P: AsRef<Path>>(path: P) -> bool {
	match pool() {
		None => path.as_ref().exists(),
		Some(fs) => fs.exists(path.as_ref()),
	}
}

pub(crate) fn is_dir<P: AsRef<Path>>(path: P) -> bool {
	match pool() {
		None => path.as_ref().is_dir(),
		Some(fs) => fs.is_dir(path.as_ref()),
	}
}

// ---------------------------------------------------------------------------------------------
// What a name is
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FileType(EntryKind);

impl FileType {
	pub(crate) fn is_dir(self) -> bool {
		self.0 == EntryKind::Dir
	}

	pub(crate) fn is_file(self) -> bool {
		self.0 == EntryKind::File
	}
}

fn kind_of(file_type: std::fs::FileType) -> EntryKind {
	if file_type.is_dir() {
		EntryKind::Dir
	} else if file_type.is_symlink() {
		EntryKind::Symlink
	} else if file_type.is_file() {
		EntryKind::File
	} else {
		EntryKind::Other
	}
}

#[derive(Clone, Debug)]
pub(crate) struct Metadata {
	len: u64,
	kind: EntryKind,
	/// What the disk said, for the tests that ask what a pool cannot answer.
	std: Option<std::fs::Metadata>,
}

impl Metadata {
	pub(crate) fn len(&self) -> u64 {
		self.len
	}

	pub(crate) fn is_file(&self) -> bool {
		self.kind == EntryKind::File
	}

	/// Only the disk has permissions.
	pub(crate) fn permissions(&self) -> std::fs::Permissions {
		self.std.as_ref().expect("a pool has no permissions").permissions()
	}

	/// Only the disk has times.
	pub(crate) fn modified(&self) -> io::Result<std::time::SystemTime> {
		self.std
			.as_ref()
			.ok_or_else(|| io::Error::new(io::ErrorKind::Unsupported, "a pool has no times"))?
			.modified()
	}
}

impl From<std::fs::Metadata> for Metadata {
	fn from(meta: std::fs::Metadata) -> Self {
		Self {
			len: meta.len(),
			kind: kind_of(meta.file_type()),
			std: Some(meta),
		}
	}
}

pub(crate) fn metadata<P: AsRef<Path>>(path: P) -> io::Result<Metadata> {
	match pool() {
		None => std::fs::metadata(path).map(Metadata::from),
		Some(fs) => {
			let path = path.as_ref();
			match fs.entry(path)? {
				None => Err(not_found(&path.display().to_string())),
				Some(EntryKind::File) => Ok(Metadata {
					len: fs.file_len(path)?,
					kind: EntryKind::File,
					std: None,
				}),
				Some(kind) => Ok(Metadata {
					len: 0,
					kind,
					std: None,
				}),
			}
		}
	}
}

pub(crate) struct DirEntry {
	dir: PathBuf,
	name: OsString,
	kind: EntryKind,
}

impl DirEntry {
	pub(crate) fn path(&self) -> PathBuf {
		self.dir.join(&self.name)
	}

	pub(crate) fn file_name(&self) -> OsString {
		self.name.clone()
	}

	pub(crate) fn file_type(&self) -> io::Result<FileType> {
		Ok(FileType(self.kind))
	}

	pub(crate) fn metadata(&self) -> io::Result<Metadata> {
		metadata(self.path())
	}
}

pub(crate) type ReadDir = std::vec::IntoIter<io::Result<DirEntry>>;

pub(crate) fn read_dir<P: AsRef<Path>>(path: P) -> io::Result<ReadDir> {
	let dir = path.as_ref();
	let entries: Vec<io::Result<DirEntry>> = match pool() {
		None => std::fs::read_dir(dir)?
			.map(|entry| {
				let entry = entry?;
				Ok(DirEntry {
					dir: dir.to_path_buf(),
					name: entry.file_name(),
					kind: kind_of(entry.file_type()?),
				})
			})
			.collect(),
		Some(fs) => fs
			.read_dir(dir)?
			.into_iter()
			.map(|entry| {
				Ok(DirEntry {
					dir: dir.to_path_buf(),
					name: entry.name,
					kind: entry.kind,
				})
			})
			.collect(),
	};
	Ok(entries.into_iter())
}

// ---------------------------------------------------------------------------------------------
// Files
// ---------------------------------------------------------------------------------------------

/// An open file: the disk's, or a handle of the pool with a cursor of its own.
pub(crate) enum File {
	Std(std::fs::File),
	Pool(PoolFile),
}

pub(crate) struct PoolFile {
	file: Arc<dyn FsFile>,
	pos: u64,
	append: bool,
	read: bool,
	write: bool,
}

impl PoolFile {
	fn new(file: Arc<dyn FsFile>, read: bool, write: bool, append: bool) -> Self {
		Self {
			file,
			pos: 0,
			append,
			read,
			write,
		}
	}
}

impl File {
	pub(crate) fn create<P: AsRef<Path>>(path: P) -> io::Result<File> {
		OpenOptions::new().write(true).create(true).truncate(true).open(path)
	}

	pub(crate) fn open<P: AsRef<Path>>(path: P) -> io::Result<File> {
		OpenOptions::new().read(true).open(path)
	}

	pub(crate) fn set_len(&self, len: u64) -> io::Result<()> {
		match self {
			File::Std(file) => file.set_len(len),
			File::Pool(pool) => {
				if !pool.write {
					return Err(io::Error::new(
						io::ErrorKind::PermissionDenied,
						"the file is not open to write",
					));
				}
				pool.file.set_len(len)
			}
		}
	}

	pub(crate) fn sync_all(&self) -> io::Result<()> {
		match self {
			File::Std(file) => file.sync_all(),
			File::Pool(pool) => pool.file.sync(),
		}
	}

	pub(crate) fn metadata(&self) -> io::Result<Metadata> {
		match self {
			File::Std(file) => file.metadata().map(Metadata::from),
			File::Pool(pool) => Ok(Metadata {
				len: pool.file.len()?,
				kind: EntryKind::File,
				std: None,
			}),
		}
	}
}

impl Write for File {
	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
		match self {
			File::Std(file) => file.write(buf),
			File::Pool(pool) => {
				if !pool.write {
					return Err(io::Error::new(
						io::ErrorKind::PermissionDenied,
						"the file is not open to write",
					));
				}
				if buf.is_empty() {
					return Ok(0);
				}
				let len = pool.file.len()?;
				if pool.append || pool.pos >= len {
					if pool.pos > len && !pool.append {
						// A write past the end: the gap reads as zeros.
						pool.file.set_len(pool.pos)?;
					}
					pool.file.append(buf)?;
				} else {
					// A write over bytes that are there: cut the file where it starts, write, and
					// put back what the write did not cover. The seam only appends.
					let end = pool.pos + buf.len() as u64;
					let mut tail = Vec::new();
					if end < len {
						tail = vec![0u8; (len - end) as usize];
						vfs::File::read_exact_at(&*pool.file, end, &mut tail)
							.map_err(vfs::into_io_error)?;
					}
					pool.file.set_len(pool.pos)?;
					pool.file.append(buf)?;
					if !tail.is_empty() {
						pool.file.append(&tail)?;
					}
				}
				pool.pos = if pool.append {
					pool.file.len()?
				} else {
					pool.pos + buf.len() as u64
				};
				Ok(buf.len())
			}
		}
	}

	fn flush(&mut self) -> io::Result<()> {
		match self {
			File::Std(file) => file.flush(),
			File::Pool(_) => Ok(()),
		}
	}
}

impl Read for File {
	fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
		match self {
			File::Std(file) => file.read(buf),
			File::Pool(pool) => {
				if !pool.read {
					return Err(io::Error::new(
						io::ErrorKind::PermissionDenied,
						"the file is not open to read",
					));
				}
				let read =
					vfs::File::read_at(&*pool.file, pool.pos, buf).map_err(vfs::into_io_error)?;
				pool.pos += read as u64;
				Ok(read)
			}
		}
	}
}

impl Seek for File {
	fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
		match self {
			File::Std(file) => file.seek(from),
			File::Pool(pool) => {
				let target = match from {
					SeekFrom::Start(at) => Some(at),
					SeekFrom::Current(by) => pool.pos.checked_add_signed(by),
					SeekFrom::End(by) => pool.file.len()?.checked_add_signed(by),
				};
				pool.pos = target.ok_or_else(|| {
					io::Error::new(io::ErrorKind::InvalidInput, "seek before the start")
				})?;
				Ok(pool.pos)
			}
		}
	}
}

impl vfs::File for File {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> crate::Result<usize> {
		match self {
			File::Std(file) => vfs::File::read_at(file, offset, buf),
			File::Pool(pool) => vfs::File::read_at(&*pool.file, offset, buf),
		}
	}

	fn size(&self) -> crate::Result<u64> {
		match self {
			File::Std(file) => vfs::File::size(file),
			File::Pool(pool) => vfs::File::size(&*pool.file),
		}
	}
}

/// `std::fs::OpenOptions`.
#[derive(Clone, Debug, Default)]
pub(crate) struct OpenOptions {
	read: bool,
	write: bool,
	append: bool,
	truncate: bool,
	create: bool,
}

impl OpenOptions {
	pub(crate) fn new() -> Self {
		Self::default()
	}

	pub(crate) fn read(&mut self, on: bool) -> &mut Self {
		self.read = on;
		self
	}

	pub(crate) fn write(&mut self, on: bool) -> &mut Self {
		self.write = on;
		self
	}

	pub(crate) fn append(&mut self, on: bool) -> &mut Self {
		self.append = on;
		self
	}

	pub(crate) fn truncate(&mut self, on: bool) -> &mut Self {
		self.truncate = on;
		self
	}

	pub(crate) fn create(&mut self, on: bool) -> &mut Self {
		self.create = on;
		self
	}

	pub(crate) fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<File> {
		match pool() {
			None => {
				let mut options = std::fs::OpenOptions::new();
				options
					.read(self.read)
					.write(self.write)
					.append(self.append)
					.truncate(self.truncate)
					.create(self.create);
				options.open(path).map(File::Std)
			}
			Some(fs) => {
				let path = path.as_ref();
				let writes = self.write || self.append;
				let opts = CreateOpts::default();
				if self.create || self.truncate {
					ensure_parent(&*fs, path)?;
				}
				let file = if self.truncate && writes {
					if !self.create && !fs.exists(path) {
						return Err(not_found(&path.display().to_string()));
					}
					fs.create_truncate(path, opts)?
				} else if self.create && writes {
					fs.open_append(path, opts)?
				} else if writes {
					fs.open_append_existing(path)?
				} else {
					fs.open_read(path)?
				};
				Ok(File::Pool(PoolFile::new(file, self.read || !writes, writes, self.append)))
			}
		}
	}
}

impl vfs::IntoFsFile for File {
	fn into_fs_file(self) -> Arc<dyn FsFile> {
		match self {
			File::Std(file) => file.into_fs_file(),
			File::Pool(pool) => pool.file,
		}
	}
}

#[cfg(test)]
mod tests {
	use std::io::{Read, Seek, SeekFrom, Write};

	use tempfile::TempDir;

	use super::*;

	// These run in every mode: over `std` each call is the call of `std::fs` it stands for, and in
	// a pool it is the pool's, and the two answer the same.

	#[test]
	fn a_file_is_written_read_listed_renamed_copied_and_removed_as_std_does() {
		let dir = TempDir::new().unwrap();
		let a = dir.path().join("a/b");
		create_dir_all(&a).unwrap();
		write(a.join("one"), b"hello").unwrap();
		assert_eq!(read(a.join("one")).unwrap(), b"hello");
		assert_eq!(metadata(a.join("one")).unwrap().len(), 5);
		assert!(metadata(a.join("one")).unwrap().is_file());
		assert!(is_dir(&a) && exists(&a) && !exists(a.join("two")));
		assert_eq!(copy(a.join("one"), a.join("two")).unwrap(), 5);
		rename(a.join("two"), a.join("three")).unwrap();
		let mut names: Vec<String> = read_dir(&a)
			.unwrap()
			.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
			.collect();
		names.sort();
		assert_eq!(names, ["one", "three"]);
		let kinds: Vec<bool> = read_dir(dir.path())
			.unwrap()
			.map(|e| e.unwrap().file_type().unwrap().is_dir())
			.collect();
		assert_eq!(kinds, [true]);
		remove_file(a.join("one")).unwrap();
		assert_eq!(read(a.join("one")).unwrap_err().kind(), io::ErrorKind::NotFound);
		remove_dir_all(dir.path().join("a")).unwrap();
		assert!(!exists(&a));
	}

	#[test]
	fn a_file_is_opened_to_append_overwrite_cut_and_read_at_a_position_as_std_does() {
		let dir = TempDir::new().unwrap();
		let path = dir.path().join("f");
		write(&path, b"0123456789").unwrap();

		// Over bytes that are there, without growing the file.
		let mut file = OpenOptions::new().write(true).open(&path).unwrap();
		file.seek(SeekFrom::Start(3)).unwrap();
		file.write_all(b"abc").unwrap();
		drop(file);
		assert_eq!(read(&path).unwrap(), b"012abc6789");

		// Across the end.
		let mut file = OpenOptions::new().write(true).open(&path).unwrap();
		file.seek(SeekFrom::End(-2)).unwrap();
		file.write_all(b"XYZ").unwrap();
		drop(file);
		assert_eq!(read(&path).unwrap(), b"012abc67XYZ");

		// At the end, whatever the position.
		let mut file = OpenOptions::new().append(true).open(&path).unwrap();
		file.write_all(b"!").unwrap();
		assert_eq!(file.metadata().unwrap().len(), 12);
		drop(file);

		let file = OpenOptions::new().write(true).open(&path).unwrap();
		file.set_len(4).unwrap();
		drop(file);
		let mut text = String::new();
		File::open(&path).unwrap().read_to_string(&mut text).unwrap();
		assert_eq!(text, "012a");

		// A file that is not there is not made by a write that does not create, and is by one that
		// does.
		let missing = dir.path().join("file");
		assert!(OpenOptions::new().write(true).open(&missing).is_err());
		let mut made =
			OpenOptions::new().write(true).create(true).truncate(true).open(&missing).unwrap();
		made.write_all(b"x").unwrap();
		drop(made);
		assert_eq!(read(&missing).unwrap(), b"x");
		let size = vfs::File::size(&File::open(&missing).unwrap()).unwrap();
		assert_eq!(size, 1);
	}
}
