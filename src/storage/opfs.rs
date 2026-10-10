//! Origin Private File System (OPFS) storage backend for WebAssembly.
//!
//! Utilizes `FileSystemSyncAccessHandle` inside Web Worker contexts for synchronous,
//! zero-copy block reads and writes directly into WebAssembly linear memory.

#![cfg(target_arch = "wasm32")]

use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::JsFuture;
use web_sys::{
	FileSystemDirectoryHandle,
	FileSystemFileHandle,
	FileSystemGetFileOptions,
	FileSystemReadWriteOptions,
	FileSystemSyncAccessHandle,
};

use crate::error::{Error, Result};
use crate::storage::{BoxFuture, LogStore, ObjectStore};
use crate::vfs::File;

/// Synchronous file handle wrapping OPFS `FileSystemSyncAccessHandle`.
///
/// The browser gives the handle exclusive access to the file until it is closed, so the file is
/// closed when this value is dropped, whether or not `close` was called before.
pub struct OpfsSyncFile {
	handle: FileSystemSyncAccessHandle,
	size_cache: AtomicU64,
	closed: AtomicBool,
}

unsafe impl Send for OpfsSyncFile {}
unsafe impl Sync for OpfsSyncFile {}

impl Debug for OpfsSyncFile {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("OpfsSyncFile")
			.field("size", &self.size_cache.load(Ordering::Relaxed))
			.finish()
	}
}

impl OpfsSyncFile {
	/// Creates a new `OpfsSyncFile` from a browser `FileSystemSyncAccessHandle`. The file takes
	/// the handle over: it is closed when the file is closed or dropped.
	pub fn new(handle: FileSystemSyncAccessHandle) -> Self {
		let initial_size = handle.get_size().unwrap_or(0.0) as u64;
		Self {
			handle,
			size_cache: AtomicU64::new(initial_size),
			closed: AtomicBool::new(false),
		}
	}

	/// Reads bytes from the given offset into `buf`.
	pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		let options = FileSystemReadWriteOptions::new();
		options.set_at(offset as f64);
		let bytes_read = self
			.handle
			.read_with_u8_array_and_options(buf, &options)
			.map_err(|e| Error::Other(format!("OPFS read error: {:?}", e)))?;
		Ok(bytes_read as usize)
	}

	/// Writes bytes at `offset`.
	pub fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
		let options = FileSystemReadWriteOptions::new();
		options.set_at(offset as f64);
		let bytes_written = self
			.handle
			.write_with_u8_array_and_options(buf, &options)
			.map_err(|e| Error::Other(format!("OPFS write error: {:?}", e)))?;
		// The browser ignores a write of nothing, so it does not extend the file up to `offset`
		// and the cached size must not move either.
		if bytes_written > 0.0 {
			let new_end = offset + bytes_written as u64;
			self.size_cache.fetch_max(new_end, Ordering::Release);
		}
		Ok(bytes_written as usize)
	}

	/// Flushes buffered writes to persistent browser storage.
	pub fn flush(&self) -> Result<()> {
		self.handle.flush().map_err(|e| Error::Other(format!("OPFS flush error: {:?}", e)))?;
		Ok(())
	}

	/// Returns current file size.
	pub fn size(&self) -> Result<u64> {
		Ok(self.size_cache.load(Ordering::Acquire))
	}

	/// Closes the sync access handle. Closing a file again, or dropping it after it was closed,
	/// does nothing.
	pub fn close(&self) {
		if !self.closed.swap(true, Ordering::AcqRel) {
			self.handle.close();
		}
	}
}

impl Drop for OpfsSyncFile {
	/// An exclusive handle that is never closed stays locked in the browser, and then the file
	/// can neither be opened nor removed again.
	fn drop(&mut self) {
		self.close();
	}
}

impl File for OpfsSyncFile {
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		self.read_at(offset, buf)
	}

	fn size(&self) -> Result<u64> {
		self.size()
	}
}

/// Append-only sequential log store backed by OPFS (WAL).
#[derive(Debug)]
pub struct OpfsLogStore {
	file: Arc<OpfsSyncFile>,
	/// Where the log ends: the offset after the last record that was written. A record takes its
	/// place in the log when its append is first polled, and the end only moves once the record is
	/// in the file.
	end: Mutex<u64>,
}

impl OpfsLogStore {
	pub fn new(file: Arc<OpfsSyncFile>) -> Self {
		let initial_size = file.size().unwrap_or(0);
		Self {
			file,
			end: Mutex::new(initial_size),
		}
	}
}

impl LogStore for OpfsLogStore {
	fn append(&self, data: &[u8]) -> BoxFuture<'_, u64> {
		let data = data.to_vec();
		Box::pin(async move {
			// The record is placed and written in one step, with no await in between, under a lock
			// that serializes the appends. A future that is dropped before it is polled has not
			// touched the log, so it leaves no hole behind, and a write that fails leaves the end
			// where it was, so no later record lands after bytes that were never written.
			let mut end = self.end.lock();
			let start = *end;
			self.file.write_at(start, &data)?;
			*end = start + data.len() as u64;
			Ok(*end)
		})
	}

	fn sync(&self) -> BoxFuture<'_, ()> {
		let file = Arc::clone(&self.file);
		Box::pin(async move {
			file.flush()?;
			Ok(())
		})
	}

	fn size(&self) -> BoxFuture<'_, u64> {
		let cur = *self.end.lock();
		Box::pin(async move { Ok(cur) })
	}
}

/// Immutable block-level object store backed by OPFS (SSTables).
#[derive(Debug)]
pub struct OpfsObjectStore {
	file: Arc<OpfsSyncFile>,
}

impl OpfsObjectStore {
	pub fn new(file: Arc<OpfsSyncFile>) -> Self {
		Self {
			file,
		}
	}
}

impl ObjectStore for OpfsObjectStore {
	fn read_at(&self, offset: u64, len: usize) -> BoxFuture<'_, Bytes> {
		let file = Arc::clone(&self.file);
		Box::pin(async move {
			let mut buf = vec![0u8; len];
			let read_bytes = file.read_at(offset, &mut buf)?;
			buf.truncate(read_bytes);
			Ok(Bytes::from(buf))
		})
	}

	fn size(&self) -> BoxFuture<'_, u64> {
		let file = Arc::clone(&self.file);
		Box::pin(async move { file.size() })
	}
}

/// Retrieves the root Origin Private File System directory handle.
pub async fn get_opfs_root() -> Result<FileSystemDirectoryHandle> {
	let global = js_sys::global();
	let storage = if let Ok(window) = global.clone().dyn_into::<web_sys::Window>() {
		window.navigator().storage()
	} else if let Ok(worker) = global.dyn_into::<web_sys::WorkerGlobalScope>() {
		worker.navigator().storage()
	} else {
		return Err(Error::Other("Not running in browser window or worker context".into()));
	};

	let dir_val = JsFuture::from(storage.get_directory())
		.await
		.map_err(|e| Error::Other(format!("Failed to get OPFS directory: {:?}", e)))?;

	Ok(dir_val.unchecked_into())
}

/// Whether this context can create sync access handles. `createSyncAccessHandle` exists only in
/// a dedicated worker. Calling it anywhere else throws a `TypeError` that the generated binding
/// does not catch, and that aborts the WebAssembly instance.
fn sync_access_handles_available() -> bool {
	let global = js_sys::global();
	let Ok(constructor) = js_sys::Reflect::get(&global, &"FileSystemFileHandle".into()) else {
		return false;
	};
	let Ok(prototype) = js_sys::Reflect::get(&constructor, &"prototype".into()) else {
		return false;
	};
	js_sys::Reflect::has(&prototype, &"createSyncAccessHandle".into()).unwrap_or(false)
}

/// Opens or creates an OPFS synchronous access file handle.
///
/// Fails, without creating the file, when this context cannot open sync access handles, for
/// example on the main thread of a page.
pub async fn open_opfs_sync_file(
	dir: &FileSystemDirectoryHandle,
	file_name: &str,
	create: bool,
) -> Result<OpfsSyncFile> {
	if !sync_access_handles_available() {
		return Err(Error::Other(format!(
			"Failed to open {file_name}: sync access handles exist only in a dedicated worker"
		)));
	}

	let options = FileSystemGetFileOptions::new();
	options.set_create(create);

	let file_val = JsFuture::from(dir.get_file_handle_with_options(file_name, &options))
		.await
		.map_err(|e| Error::Other(format!("Failed to get file handle for {file_name}: {:?}", e)))?;
	let file_handle: FileSystemFileHandle = file_val.unchecked_into();

	let sync_val = JsFuture::from(file_handle.create_sync_access_handle()).await.map_err(|e| {
		Error::Other(format!("Failed to create sync access handle for {file_name}: {:?}", e))
	})?;
	let sync_handle: FileSystemSyncAccessHandle = sync_val.unchecked_into();

	Ok(OpfsSyncFile::new(sync_handle))
}
