//! The backend of the pool over a browser's origin private file system (OPFS).
//!
//! [`OpfsRaw`] is the directory `<opfs root>/<name>/` and implements [`Raw`]: the lock file
//! `LOCK`, and the slot files `p{n:08x}` that [`super::pool::PoolFs`] makes and opens. A slot is
//! open through a sync access handle ([`OpfsSyncFile`]), which exists in a dedicated worker only,
//! is exclusive, and stays locked until it is closed. What this file adds to the handle is what the
//! pool asks of a backend and a browser makes awkward:
//!
//! * the four asynchronous operations (list, open, create, take the lock) are safe to drop. A
//!   handle that arrives after its future was dropped is closed on arrival
//!   (`create_sync_access_handle_io`), because one that is not closed locks its file until the
//!   browser collects it, which it has not done within seconds in any measurement;
//! * `create` is `AlreadyExists` on a file that is there, which `getFileHandle({create: true})` is
//!   not;
//! * an exception becomes an `io::Error` whose kind says what to do about it
//!   ([`crate::storage::opfs::kind_of_exception`]): `PermissionDenied` is the lock of another
//!   holder, which the mount of the pool polls for, and nothing else is `PermissionDenied`;
//! * the size of a file is the browser's, asked every time, and the error of a failed question is
//!   passed on.
//!
//! The write of a file is one browser call that may write fewer bytes than it is given; the pool
//! loops over that, with the count of the bytes that landed, so [`RawFile::write_at`] here is one
//! call and does not loop.
//!
//! What this file does not do: decide how many slots a pool has, wait for the lock (the mount does,
//! at 50 to 100 ms for at least five seconds, which is how long the previous page of a reload may
//! hold it), or open the public `FileSystem` (that is a later change).

use std::io;

use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{FileSystemDirectoryHandle, FileSystemGetDirectoryOptions, FileSystemRemoveOptions};

use super::pool::{Raw, RawFile, RawFut, LOCK_NAME};
use crate::storage::opfs::{
	create_sync_access_handle_io,
	get_file_handle_io,
	map_js_error,
	opfs_root_io,
	OpfsSyncFile,
};

/// The longest name of a database: what a browser allows of one path component, with room.
const MAX_NAME_LEN: usize = 200;

/// One open slot (or the lock file) of an OPFS pool.
#[derive(Debug)]
pub(crate) struct OpfsRawFile {
	file: OpfsSyncFile,
}

impl OpfsRawFile {
	pub(crate) fn from_sync_file(file: OpfsSyncFile) -> Self {
		Self {
			file,
		}
	}
}

impl RawFile for OpfsRawFile {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
		self.file.read_at_io(offset, buf)
	}

	/// One browser call. It may write fewer bytes than `buf` holds, and the pool goes on from
	/// there.
	fn write_at(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
		self.file.write_some_at_io(offset, buf)
	}

	fn set_len(&self, len: u64) -> io::Result<()> {
		self.file.truncate_io(len)
	}

	/// The size the browser has now (`getSize`), and its error if there is one.
	fn size(&self) -> io::Result<u64> {
		self.file.size_io()
	}

	/// Ordering only: `FileSystemSyncAccessHandle.flush()` is a barrier of the file system of the
	/// browser, and what it promises about a power cut is not known.
	fn flush(&self) -> io::Result<()> {
		self.file.flush_io()
	}

	fn close(&self) -> io::Result<()> {
		self.file.close();
		Ok(())
	}
}

/// The directory of a pool in the origin private file system.
pub(crate) struct OpfsRaw {
	dir: FileSystemDirectoryHandle,
	name: String,
}

// SAFETY: the engine is built for one thread on this target (`compile_error!` in `rt.rs` refuses
// the `atomics` target feature), so a JavaScript value is never used from a second one.
unsafe impl Send for OpfsRaw {}
unsafe impl Sync for OpfsRaw {}

impl std::fmt::Debug for OpfsRaw {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("OpfsRaw").field("name", &self.name).finish()
	}
}

/// What a database may be called: one directory name, which no browser has a problem with.
fn check_name(name: &str) -> io::Result<()> {
	let bad = name.is_empty()
		|| name == "."
		|| name == ".."
		|| name.len() > MAX_NAME_LEN
		|| name.chars().any(|c| c == '/' || c == '\\' || c == '\0' || c.is_control());
	if bad {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			format!(
				"{name:?} is not a name for a database in the origin private file system: it \
				 must be one non-empty path component"
			),
		));
	}
	Ok(())
}

/// The directory `name` below the root, made if `create`.
async fn directory_io(name: &str, create: bool) -> io::Result<FileSystemDirectoryHandle> {
	check_name(name)?;
	let root = opfs_root_io().await?;
	let options = FileSystemGetDirectoryOptions::new();
	options.set_create(create);
	let dir = JsFuture::from(root.get_directory_handle_with_options(name, &options))
		.await
		.map_err(|e| map_js_error(&format!("get the directory {name}"), &e))?;
	Ok(dir.unchecked_into())
}

/// Every entry of `dir`, from its async iterator.
async fn list_names(dir: &FileSystemDirectoryHandle) -> io::Result<Vec<String>> {
	let iterator = dir.keys();
	let mut names = Vec::new();
	loop {
		let next = iterator.next().map_err(|e| map_js_error("list the directory", &e))?;
		let step: js_sys::IteratorNext = JsFuture::from(next)
			.await
			.map_err(|e| map_js_error("list the directory", &e))?
			.unchecked_into();
		if step.done() {
			return Ok(names);
		}
		match step.value().as_string() {
			Some(name) => names.push(name),
			None => {
				return Err(io::Error::new(
					io::ErrorKind::InvalidData,
					"the browser listed an entry whose name is not a string",
				));
			}
		}
	}
}

/// Opens the file `name` of `dir` exclusively, making it first if `create`.
async fn open_file(
	dir: &FileSystemDirectoryHandle,
	name: &str,
	create: bool,
) -> io::Result<OpfsRawFile> {
	let handle = get_file_handle_io(dir, name, create).await?;
	let sync = create_sync_access_handle_io(&handle, name).await?;
	Ok(OpfsRawFile::from_sync_file(OpfsSyncFile::new(sync)))
}

impl OpfsRaw {
	/// The pool directory `name`, made if it is not there.
	pub(crate) async fn open(name: &str) -> io::Result<Self> {
		Ok(Self {
			dir: directory_io(name, true).await?,
			name: name.to_string(),
		})
	}

	/// The pool directory `name` if it is there, and `None` if it is not.
	pub(crate) async fn open_existing(name: &str) -> io::Result<Option<Self>> {
		match directory_io(name, false).await {
			Ok(dir) => Ok(Some(Self {
				dir,
				name: name.to_string(),
			})),
			Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
			Err(e) => Err(e),
		}
	}

	pub(crate) fn name(&self) -> &str {
		&self.name
	}

	/// Removes the pool directory `name` with everything in it, which is how a database is
	/// deleted. Removing what is not there is `Ok`, so that a delete can be repeated.
	///
	/// A pool that is mounted holds `LOCK`, and so does a page that has it open: this takes the
	/// lock first, and if it cannot, fails with `PermissionDenied` (see
	/// [`crate::storage::opfs::is_locked`]) and removes nothing. A handle on a slot that is still
	/// open somewhere fails the removal of the directory in the same way. Nothing is removed in
	/// part on purpose, but the browser does not promise that either.
	pub(crate) async fn destroy(name: &str) -> io::Result<()> {
		let Some(pool) = Self::open_existing(name).await? else {
			return Ok(());
		};
		// The lock, without making it: a directory that was never mounted has none.
		match get_file_handle_io(&pool.dir, LOCK_NAME, false).await {
			Ok(handle) => {
				let sync = create_sync_access_handle_io(&handle, LOCK_NAME).await?;
				OpfsSyncFile::new(sync).close();
			}
			Err(e) if e.kind() == io::ErrorKind::NotFound => {}
			Err(e) => return Err(e),
		}
		let root = opfs_root_io().await?;
		let options = FileSystemRemoveOptions::new();
		options.set_recursive(true);
		match JsFuture::from(root.remove_entry_with_options(name, &options)).await {
			Ok(_) => Ok(()),
			Err(e) => {
				let error = map_js_error(&format!("remove the directory {name}"), &e);
				if error.kind() == io::ErrorKind::NotFound {
					Ok(())
				} else {
					Err(error)
				}
			}
		}
	}
}

impl Raw for OpfsRaw {
	type File = OpfsRawFile;

	fn list(&self) -> RawFut<'_, io::Result<Vec<String>>> {
		let dir = self.dir.clone();
		Box::pin(async move {
			let mut names = list_names(&dir).await?;
			names.retain(|name| name != LOCK_NAME);
			Ok(names)
		})
	}

	fn open(&self, name: &str) -> RawFut<'_, io::Result<OpfsRawFile>> {
		let dir = self.dir.clone();
		let name = name.to_string();
		Box::pin(async move { open_file(&dir, &name, false).await })
	}

	fn create(&self, name: &str) -> RawFut<'_, io::Result<OpfsRawFile>> {
		let dir = self.dir.clone();
		let name = name.to_string();
		Box::pin(async move {
			// `getFileHandle({create: true})` opens a file that is there, and a pool never means
			// that: it makes a slot under a name that nothing has.
			match get_file_handle_io(&dir, &name, false).await {
				Ok(_) => {
					return Err(io::Error::new(
						io::ErrorKind::AlreadyExists,
						format!("the slot {name} is already there"),
					));
				}
				Err(e) if e.kind() == io::ErrorKind::NotFound => {}
				Err(e) => return Err(e),
			}
			open_file(&dir, &name, true).await
		})
	}

	/// One try: `PermissionDenied` if another handle holds `LOCK`, at once. The mount polls.
	fn open_lock(&self) -> RawFut<'_, io::Result<OpfsRawFile>> {
		let dir = self.dir.clone();
		Box::pin(async move { open_file(&dir, LOCK_NAME, true).await })
	}
}

#[cfg(all(test, target_arch = "wasm32"))]
#[path = "opfs_raw_tests.rs"]
mod tests;
