//! Origin Private File System (OPFS) storage backend for WebAssembly.
//!
//! Utilizes `FileSystemSyncAccessHandle` inside Web Worker contexts for synchronous,
//! zero-copy block reads and writes directly into WebAssembly linear memory.

#![cfg(target_arch = "wasm32")]

use std::fmt::{self, Debug};
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use wasm_bindgen::{JsCast, JsValue};
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

// ---------------------------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------------------------

/// The payload of every `io::Error` made from an exception of the browser: which exception it was,
/// so that a caller can tell the lock of another page from a full disk without parsing a message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DomError {
	/// What was being done: `"open p00000001"`.
	pub(crate) context: String,
	/// The name of the exception: `NoModificationAllowedError`.
	pub(crate) name: String,
	pub(crate) message: String,
}

impl fmt::Display for DomError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}: {}: {}", self.context, self.name, self.message)
	}
}

impl std::error::Error for DomError {}

/// The `io::ErrorKind` of an exception, by its name.
///
/// * `NoModificationAllowedError` is `PermissionDenied`: the file is held by another sync access
///   handle (another page, or a handle that was dropped and not yet collected). It is what the
///   mount of a pool polls on, and nothing else is `PermissionDenied`, so that a failure that
///   waiting cannot cure is not waited for.
/// * `QuotaExceededError` is `StorageFull`, thrown by `write` and `truncate` and not by `flush`.
/// * `NotFoundError` is `NotFound`.
/// * `TypeMismatchError` (a directory where a file was asked for, or the other way) and `TypeError`
///   (a name the browser refuses) are `InvalidInput`.
/// * `InvalidStateError` is what a handle that was closed throws: `NotConnected`.
/// * `SecurityError`, `NotAllowedError` and `NotSupportedError` are `Unsupported`: the origin may
///   not use the file system (an insecure context, a private window), which is not a lock.
/// * Anything else is `Other`. Never `Interrupted`, which a caller retries without end.
pub(crate) fn kind_of_exception(name: &str) -> io::ErrorKind {
	match name {
		"NoModificationAllowedError" => io::ErrorKind::PermissionDenied,
		"QuotaExceededError" => io::ErrorKind::StorageFull,
		"NotFoundError" => io::ErrorKind::NotFound,
		"TypeMismatchError" | "TypeError" => io::ErrorKind::InvalidInput,
		"InvalidStateError" => io::ErrorKind::NotConnected,
		"SecurityError" | "NotAllowedError" | "NotSupportedError" => io::ErrorKind::Unsupported,
		_ => io::ErrorKind::Other,
	}
}

/// The `io::Error` of an exception that is known by name and message. Its payload is a
/// [`DomError`].
pub(crate) fn map_exception(context: &str, name: &str, message: &str) -> io::Error {
	io::Error::new(
		kind_of_exception(name),
		DomError {
			context: context.to_string(),
			name: name.to_string(),
			message: message.to_string(),
		},
	)
}

/// The `io::Error` of whatever a promise rejected with or a call threw: a `DOMException`, an
/// `Error`, or something else, which is `Other`.
pub(crate) fn map_js_error(context: &str, value: &JsValue) -> io::Error {
	if let Some(exception) = value.dyn_ref::<web_sys::DomException>() {
		return map_exception(context, &exception.name(), &exception.message());
	}
	if let Some(error) = value.dyn_ref::<js_sys::Error>() {
		return map_exception(context, &String::from(error.name()), &String::from(error.message()));
	}
	map_exception(context, "", &format!("{value:?}"))
}

/// The exception that an `io::Error` was made from, if it was made from one.
// Read by the tests today, and by the public mount and the migration that follow.
#[allow(dead_code)]
pub(crate) fn dom_error(error: &io::Error) -> Option<&DomError> {
	error.get_ref().and_then(|inner| inner.downcast_ref::<DomError>())
}

/// Whether the error is the lock of another holder: the file has an open sync access handle that
/// is not ours.
pub(crate) fn is_locked(error: &io::Error) -> bool {
	dom_error(error).is_some_and(|e| e.name == "NoModificationAllowedError")
}

fn io_error(error: io::Error) -> Error {
	Error::Io(Arc::new(error))
}

// ---------------------------------------------------------------------------------------------
// The handle
// ---------------------------------------------------------------------------------------------

/// The largest offset or length that a JavaScript number holds exactly.
const MAX_EXACT: u64 = 1 << 53;

/// What a sync access handle does, as `io` results. The real handle is the browser's; a test puts
/// a fake behind the file, to make a write short or a call fail the way a real one cannot be made
/// to.
pub(crate) trait SyncHandleOps: 'static {
	/// One read: how many bytes were read, `0` at the end of the file.
	fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize>;
	/// One write: how many bytes were written, which may be fewer than given.
	fn write(&self, offset: u64, buf: &[u8]) -> io::Result<usize>;
	fn truncate(&self, len: u64) -> io::Result<()>;
	fn get_size(&self) -> io::Result<u64>;
	fn flush(&self) -> io::Result<()>;
	/// Releases the file. A browser does not say if that fails.
	fn close(&self);
}

fn exact(value: u64, what: &str) -> io::Result<f64> {
	if value > MAX_EXACT {
		return Err(io::Error::new(
			io::ErrorKind::InvalidInput,
			format!("{what} {value} is beyond what the browser can address exactly"),
		));
	}
	Ok(value as f64)
}

/// A count that the browser returned, checked.
fn count(value: f64, what: &str) -> io::Result<u64> {
	if value.is_finite() && value >= 0.0 && value <= MAX_EXACT as f64 {
		Ok(value as u64)
	} else {
		Err(io::Error::new(
			io::ErrorKind::InvalidData,
			format!("the browser answered {value} for {what}"),
		))
	}
}

impl SyncHandleOps for FileSystemSyncAccessHandle {
	fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
		let options = FileSystemReadWriteOptions::new();
		options.set_at(exact(offset, "the offset")?);
		let read = self
			.read_with_u8_array_and_options(buf, &options)
			.map_err(|e| map_js_error("OPFS read", &e))?;
		Ok(count(read, "a read")? as usize)
	}

	fn write(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
		let options = FileSystemReadWriteOptions::new();
		options.set_at(exact(offset, "the offset")?);
		let written = self
			.write_with_u8_array_and_options(buf, &options)
			.map_err(|e| map_js_error("OPFS write", &e))?;
		Ok(count(written, "a write")? as usize)
	}

	fn truncate(&self, len: u64) -> io::Result<()> {
		self.truncate_with_f64(exact(len, "the length")?)
			.map_err(|e| map_js_error("OPFS truncate", &e))
	}

	fn get_size(&self) -> io::Result<u64> {
		let size = FileSystemSyncAccessHandle::get_size(self)
			.map_err(|e| map_js_error("OPFS getSize", &e))?;
		count(size, "getSize")
	}

	fn flush(&self) -> io::Result<()> {
		FileSystemSyncAccessHandle::flush(self).map_err(|e| map_js_error("OPFS flush", &e))
	}

	fn close(&self) {
		FileSystemSyncAccessHandle::close(self);
	}
}

/// Writes all of `buf` at `offset` with `write`, which may write fewer bytes than it is given, and
/// says how many bytes landed. A write that makes no progress is `WriteZero`, an error from `write`
/// ends it, and the bytes before it stay where they are. An empty buffer is passed on as it is,
/// once: the browser ignores it, and does not extend the file up to `offset`.
pub(crate) fn write_all_with(
	offset: u64,
	buf: &[u8],
	mut write: impl FnMut(u64, &[u8]) -> io::Result<usize>,
) -> io::Result<usize> {
	if buf.is_empty() {
		return write(offset, buf);
	}
	let mut done = 0usize;
	while done < buf.len() {
		let at = offset.checked_add(done as u64).ok_or_else(|| {
			io::Error::new(io::ErrorKind::InvalidInput, "a write overflows the offset")
		})?;
		match write(at, &buf[done..])? {
			0 => {
				return Err(io::Error::new(
					io::ErrorKind::WriteZero,
					"the browser wrote nothing and made no error",
				));
			}
			n => done += n.min(buf.len() - done),
		}
	}
	Ok(done)
}

/// Synchronous file handle wrapping OPFS `FileSystemSyncAccessHandle`.
///
/// The browser gives the handle exclusive access to the file until it is closed, so the file is
/// closed when this value is dropped, whether or not `close` was called before. A handle that is
/// dropped without being closed stays locked until the browser collects it, which it did not do
/// within seconds in any measurement; that is why `Drop` closes.
///
/// The size is the browser's (`getSize`), asked every time: a write, a `truncate` and a reopen all
/// move it, and a cache of it would have to be kept in step with each of them. The one value that
/// is kept is the size at the moment of the close, which [`OpfsSyncFile::size`] goes on to answer
/// once the handle is gone (a closed handle cannot be asked).
pub struct OpfsSyncFile {
	handle: Box<dyn SyncHandleOps>,
	closed: AtomicBool,
	/// The size when `close` was called, or `UNKNOWN_SIZE` if it was not asked for or failed.
	closed_size: AtomicU64,
}

const UNKNOWN_SIZE: u64 = u64::MAX;

// SAFETY: the engine is built for one thread on this target (`compile_error!` in `rt.rs` refuses
// the `atomics` target feature), so the handle, a JavaScript value, is never used from a second
// one.
unsafe impl Send for OpfsSyncFile {}
unsafe impl Sync for OpfsSyncFile {}

impl Debug for OpfsSyncFile {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let mut debug = f.debug_struct("OpfsSyncFile");
		match self.handle.get_size() {
			Ok(size) => debug.field("size", &size),
			Err(_) => debug.field("size", &"unavailable"),
		};
		debug.field("closed", &self.closed.load(Ordering::Relaxed)).finish()
	}
}

impl OpfsSyncFile {
	/// Creates a new `OpfsSyncFile` from a browser `FileSystemSyncAccessHandle`. The file takes
	/// the handle over: it is closed when the file is closed or dropped.
	pub fn new(handle: FileSystemSyncAccessHandle) -> Self {
		Self::from_ops(Box::new(handle))
	}

	/// A file over any handle: the real one, or a fake of a test.
	pub(crate) fn from_ops(handle: Box<dyn SyncHandleOps>) -> Self {
		Self {
			handle,
			closed: AtomicBool::new(false),
			closed_size: AtomicU64::new(UNKNOWN_SIZE),
		}
	}

	// ---- `io` forms: what the pool uses, with the kind of the exception kept ------------------

	/// One read. The count is short of `buf` at the end of the file, and may be short before it.
	pub(crate) fn read_at_io(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
		self.handle.read(offset, buf)
	}

	/// One write: how many bytes were written, which may be fewer than `buf` holds.
	pub(crate) fn write_some_at_io(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
		self.handle.write(offset, buf)
	}

	/// All of `buf`, looping over short writes.
	pub(crate) fn write_all_at_io(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
		write_all_with(offset, buf, |at, rest| self.handle.write(at, rest))
	}

	/// Shrinks the file, or grows it with zeros.
	pub(crate) fn truncate_io(&self, len: u64) -> io::Result<()> {
		self.handle.truncate(len)
	}

	pub(crate) fn flush_io(&self) -> io::Result<()> {
		self.handle.flush()
	}

	/// The size the browser says the file has now, with the error of the browser if it fails: that
	/// of a closed handle is an `InvalidStateError`, which is `NotConnected`.
	pub(crate) fn size_io(&self) -> io::Result<u64> {
		self.handle.get_size()
	}

	// ---- engine `Result` forms ----------------------------------------------------------------

	/// Reads bytes from the given offset into `buf`.
	pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
		self.read_at_io(offset, buf).map_err(io_error)
	}

	/// Writes all of `buf` at `offset`, and returns its length. A write that the browser makes
	/// short is continued until it is whole, and one that fails is an error, with the bytes that
	/// landed before it left in the file.
	pub fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
		self.write_all_at_io(offset, buf).map_err(io_error)
	}

	/// Shrinks the file to `len` bytes, or grows it to `len` with zeros.
	pub fn truncate(&self, len: u64) -> Result<()> {
		self.truncate_io(len).map_err(io_error)
	}

	/// Flushes buffered writes to persistent browser storage. An ordering barrier, which is all
	/// that is known of it: not a promise about a power cut.
	pub fn flush(&self) -> Result<()> {
		self.flush_io().map_err(io_error)
	}

	/// Returns the current file size, as the browser has it. Of a file that was closed it is the
	/// size that the file had when it was closed.
	pub fn size(&self) -> Result<u64> {
		if self.is_closed() {
			let size = self.closed_size.load(Ordering::Acquire);
			if size != UNKNOWN_SIZE {
				return Ok(size);
			}
		}
		self.size_io().map_err(io_error)
	}

	/// Whether `close` was called.
	pub fn is_closed(&self) -> bool {
		self.closed.load(Ordering::Acquire)
	}

	/// Closes the sync access handle. Closing a file again, or dropping it after it was closed,
	/// does nothing.
	pub fn close(&self) {
		if !self.closed.swap(true, Ordering::AcqRel) {
			if let Ok(size) = self.handle.get_size() {
				self.closed_size.store(size, Ordering::Release);
			}
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
	/// A log over `file`, which ends where the file does. If the browser cannot say how long the
	/// file is, the log starts at zero, and the next append would go over what is there: use
	/// [`OpfsLogStore::try_new`] where that must be an error.
	pub fn new(file: Arc<OpfsSyncFile>) -> Self {
		let initial_size = file.size().unwrap_or(0);
		Self {
			file,
			end: Mutex::new(initial_size),
		}
	}

	/// A log over `file`, which ends where the file does, or the error of the browser if it
	/// cannot say where that is.
	pub fn try_new(file: Arc<OpfsSyncFile>) -> Result<Self> {
		let end = file.size()?;
		Ok(Self {
			file,
			end: Mutex::new(end),
		})
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

/// The root directory of the origin private file system, with the error of the browser kept: a
/// context that has no file system at all is `Unsupported`, and so is one that refuses it
/// (`SecurityError`, as in some private windows).
pub(crate) async fn opfs_root_io() -> io::Result<FileSystemDirectoryHandle> {
	let global = js_sys::global();
	let storage = if let Ok(window) = global.clone().dyn_into::<web_sys::Window>() {
		window.navigator().storage()
	} else if let Ok(worker) = global.dyn_into::<web_sys::WorkerGlobalScope>() {
		worker.navigator().storage()
	} else {
		return Err(io::Error::new(
			io::ErrorKind::Unsupported,
			"not running in a browser window or worker context",
		));
	};

	let dir_val = JsFuture::from(storage.get_directory())
		.await
		.map_err(|e| map_js_error("get the OPFS root", &e))?;

	Ok(dir_val.unchecked_into())
}

/// Retrieves the root Origin Private File System directory handle.
pub async fn get_opfs_root() -> Result<FileSystemDirectoryHandle> {
	opfs_root_io().await.map_err(io_error)
}

/// Whether this context can create sync access handles. `createSyncAccessHandle` exists only in
/// a dedicated worker. Calling it anywhere else throws a `TypeError` that the generated binding
/// does not catch, and that aborts the WebAssembly instance.
pub(crate) fn sync_access_handles_available() -> bool {
	let global = js_sys::global();
	let Ok(constructor) = js_sys::Reflect::get(&global, &"FileSystemFileHandle".into()) else {
		return false;
	};
	let Ok(prototype) = js_sys::Reflect::get(&constructor, &"prototype".into()) else {
		return false;
	};
	js_sys::Reflect::has(&prototype, &"createSyncAccessHandle".into()).unwrap_or(false)
}

/// Closes a sync access handle that arrives after the call that asked for it was dropped.
///
/// A handle that nobody closes keeps its file locked until the browser collects it, which is not
/// within seconds. A `createSyncAccessHandle` that was asked for and whose future was dropped
/// before the handle arrived, or after it arrived and before it was taken, is such a handle, and
/// the file it locks can then neither be opened nor removed (26 of 26 and 22 of 22 attempts failed
/// in the measurements). So the promise stays in this guard until its value is taken, and a guard
/// that is dropped before that hangs a closer on the promise.
struct CloseOnArrival {
	promise: Option<js_sys::Promise>,
}

impl CloseOnArrival {
	fn new(promise: js_sys::Promise) -> Self {
		Self {
			promise: Some(promise),
		}
	}

	/// The value was taken, and whoever took it closes it.
	fn disarm(&mut self) {
		self.promise = None;
	}
}

impl Drop for CloseOnArrival {
	fn drop(&mut self) {
		let Some(promise) = self.promise.take() else {
			return;
		};
		let close = wasm_bindgen::closure::Closure::once_into_js(|handle: JsValue| {
			if let Some(handle) = handle.dyn_ref::<FileSystemSyncAccessHandle>() {
				handle.close();
			}
		});
		let ignore = wasm_bindgen::closure::Closure::once_into_js(|_error: JsValue| {});
		// `then` with both callbacks, so that a rejection is handled here and does not surface as
		// an unhandled rejection of the page. It cannot fail on a promise.
		if let Ok(then) = js_sys::Reflect::get(&promise, &"then".into()) {
			if let Some(then) = then.dyn_ref::<js_sys::Function>() {
				let _ = then.call2(&promise, &close, &ignore);
			}
		}
	}
}

/// Looks up the file `name` in `dir`, creating it if `create`.
pub(crate) async fn get_file_handle_io(
	dir: &FileSystemDirectoryHandle,
	name: &str,
	create: bool,
) -> io::Result<FileSystemFileHandle> {
	let options = FileSystemGetFileOptions::new();
	options.set_create(create);
	let value = JsFuture::from(dir.get_file_handle_with_options(name, &options))
		.await
		.map_err(|e| map_js_error(&format!("get the file {name}"), &e))?;
	Ok(value.unchecked_into())
}

/// Opens a sync access handle on `file`. Safe to drop at any point: a handle that arrives after
/// the future was dropped is closed on arrival.
///
/// Fails with `PermissionDenied` (see [`kind_of_exception`]) if the file is held by another
/// handle, at once and without waiting.
pub(crate) async fn create_sync_access_handle_io(
	file: &FileSystemFileHandle,
	name: &str,
) -> io::Result<FileSystemSyncAccessHandle> {
	if !sync_access_handles_available() {
		return Err(io::Error::new(
			io::ErrorKind::Unsupported,
			format!("failed to open {name}: sync access handles exist only in a dedicated worker"),
		));
	}
	let promise = file.create_sync_access_handle();
	let mut guard = CloseOnArrival::new(promise.clone());
	let outcome = JsFuture::from(promise).await;
	guard.disarm();
	outcome
		.map(|handle| handle.unchecked_into())
		.map_err(|e| map_js_error(&format!("open a sync access handle on {name}"), &e))
}

/// Opens or creates an OPFS synchronous access file handle.
///
/// Fails, without creating the file, when this context cannot open sync access handles, for
/// example on the main thread of a page. Safe to drop while it is waiting: a handle that
/// arrives late is closed.
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
	let file_handle = get_file_handle_io(dir, file_name, create).await.map_err(io_error)?;
	let sync_handle =
		create_sync_access_handle_io(&file_handle, file_name).await.map_err(io_error)?;
	Ok(OpfsSyncFile::new(sync_handle))
}
