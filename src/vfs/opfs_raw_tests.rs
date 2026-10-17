//! Tests of the pool's backend over the origin private file system, which run in a dedicated worker
//! of a real headless Chrome (they are tests of `pub(crate)` items, so they are unit tests that
//! `wasm-pack` runs through the library target; the crate's one `wasm_bindgen_test_configure!`
//! is in `wasm_tests.rs`). Run them, from the repository root, with
//!
//! ```text
//! export CC_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/clang
//! export AR_wasm32_unknown_unknown=/opt/homebrew/opt/llvm/bin/llvm-ar
//! WASM_BINDGEN_TEST_TIMEOUT=300 WASM_BINDGEN_USE_DEDICATED_WORKER=1 \
//!     wasm-pack test --headless --chrome -- --lib
//! ```
//!
//! `wasm-pack` passes what follows `--` to `cargo build`, so it cannot filter. To run these tests
//! alone, call `cargo test` with the runner that `wasm-pack` sets (the paths are those of its
//! cache, and `chromedriver` is the one it downloaded):
//!
//! ```text
//! CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=~/Library/Caches/.wasm-pack/wasm-bindgen-cargo-install-<version>/wasm-bindgen-test-runner \
//! CHROMEDRIVER=~/Library/Caches/.wasm-pack/chromedriver-<hash>/chromedriver \
//! WASM_BINDGEN_TEST_ONLY_WEB=1 WASM_BINDGEN_TEST_TIMEOUT=300 WASM_BINDGEN_USE_DEDICATED_WORKER=1 \
//!     cargo test --target wasm32-unknown-unknown --lib -- opfs_raw --nocapture
//! ```
//!
//! The origin private file system persists inside the profile of the browser, so every test works
//! on a name that no earlier test or run has used, and removes it with `OpfsRaw::destroy`, which
//! also proves that no handle of the test was left open (a directory with an open handle cannot be
//! removed).

use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::{console_log, wasm_bindgen_test};

use super::*;
use crate::error::Error;
use crate::rt;
use crate::storage::opfs::{
	dom_error,
	is_locked,
	map_exception,
	write_all_with,
	OpfsLogStore,
	SyncHandleOps,
};
use crate::vfs::header::{Header, HEADER_LEN, PAGE_LEN};
use crate::vfs::{self, contract, CreateOpts, Fs, MountReport, PoolConfig, PoolControl, PoolFs};

type Pool = PoolFs<OpfsRaw>;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A database name that no earlier test or run uses.
fn unique(label: &str) -> String {
	let n = COUNTER.fetch_add(1, Ordering::Relaxed);
	format!("skv-m10a-{label}-{}-{n}", js_sys::Date::now() as u64)
}

/// A pool with `initial` spares, that only its callers refill, and that waits `300 ms` for a lock.
fn config(initial: usize) -> PoolConfig {
	PoolConfig {
		lock_deadline: Duration::from_millis(300),
		lock_poll: Duration::from_millis(25),
		..PoolConfig::strict(initial, 2048)
	}
}

async fn mount_with(name: &str, cfg: PoolConfig) -> crate::Result<(Arc<Pool>, MountReport)> {
	let raw = OpfsRaw::open(name).await?;
	PoolFs::mount(raw, cfg).await
}

async fn mount(name: &str, initial: usize) -> (Arc<Pool>, MountReport) {
	mount_with(name, config(initial)).await.unwrap_or_else(|e| panic!("mount {name}: {e}"))
}

fn close(fs: &Arc<Pool>) {
	PoolControl::close(&**fs).expect("close");
}

fn p(path: &str) -> &Path {
	Path::new(path)
}

/// Makes a file with this content, synced.
fn put(fs: &Pool, path: &str, content: &[u8]) {
	let file = fs.create_truncate(p(path), CreateOpts::default()).expect("create_truncate");
	file.append(content).expect("append");
	file.sync().expect("sync");
}

/// Lets the event loop run until what a dropped future left in flight has arrived: an open needs
/// about three macrotask turns.
async fn settle() {
	for _ in 0..30 {
		rt::relax().await;
	}
	rt::sleep(Duration::from_millis(30)).await;
}

/// Removes the database `name`, which fails if a handle of it is still open, and checks that it is
/// gone.
async fn destroy_and_verify(name: &str) {
	OpfsRaw::destroy(name).await.unwrap_or_else(|e| panic!("destroy {name}: {e}"));
	assert!(OpfsRaw::open_existing(name).await.expect("open_existing").is_none(), "{name} is gone");
}

async fn names(name: &str) -> Vec<String> {
	let mut names = OpfsRaw::open(name).await.unwrap().list().await.unwrap();
	names.sort();
	names
}

fn is_permission_denied(error: &Error) -> bool {
	matches!(error, Error::Io(e) if e.kind() == io::ErrorKind::PermissionDenied)
}

/// Polls `future` once from inside this task, and says whether it finished.
async fn poll_once<F: Future + Unpin>(future: &mut F) -> bool {
	std::future::poll_fn(|cx| Poll::Ready(Pin::new(&mut *future).poll(cx).is_ready())).await
}

// ---------------------------------------------------------------------------------------------
// The exceptions of the browser
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
fn exceptions_map_to_the_kind_that_says_what_to_do() {
	let table = [
		("NoModificationAllowedError", io::ErrorKind::PermissionDenied),
		("QuotaExceededError", io::ErrorKind::StorageFull),
		("NotFoundError", io::ErrorKind::NotFound),
		("TypeMismatchError", io::ErrorKind::InvalidInput),
		("InvalidStateError", io::ErrorKind::NotConnected),
		("SecurityError", io::ErrorKind::Unsupported),
		("NotAllowedError", io::ErrorKind::Unsupported),
		("NotSupportedError", io::ErrorKind::Unsupported),
		("AbortError", io::ErrorKind::Other),
		("UnknownError", io::ErrorKind::Other),
		("DataError", io::ErrorKind::Other),
	];
	for (name, kind) in table {
		let exception = web_sys::DomException::new_with_message_and_name("a message", name)
			.expect("a DOMException can be made");
		let error = crate::storage::opfs::map_js_error("a context", &exception);
		assert_eq!(error.kind(), kind, "{name}");
		let payload = dom_error(&error).unwrap_or_else(|| panic!("{name} keeps its exception"));
		assert_eq!(payload.name, name);
		assert_eq!(payload.message, "a message");
		assert_eq!(payload.context, "a context");
		assert!(error.to_string().contains(name) && error.to_string().contains("a context"));
		// Nothing is `Interrupted`, which a caller retries without end, and the lock is the only
		// `PermissionDenied`, which the mount polls for.
		assert_ne!(error.kind(), io::ErrorKind::Interrupted);
		assert_eq!(
			error.kind() == io::ErrorKind::PermissionDenied,
			name == "NoModificationAllowedError"
		);
		assert_eq!(is_locked(&error), name == "NoModificationAllowedError", "{name}");
		// A full disk is not a pool that holds its limit of files: the engine tells them apart.
		assert!(!vfs::is_pool_full(&error) && !vfs::is_pool_exhausted(&error), "{name}");
	}
	// An `Error` that is not a `DOMException` has a name too, and anything else is `Other`.
	let type_error = js_sys::TypeError::new("a name the browser refuses");
	let error = crate::storage::opfs::map_js_error("a context", &type_error);
	assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
	assert_eq!(dom_error(&error).unwrap().name, "TypeError");
	let error = crate::storage::opfs::map_js_error("a context", &JsValue::from_f64(42.0));
	assert_eq!(error.kind(), io::ErrorKind::Other);
	assert!(dom_error(&error).unwrap().name.is_empty());
	// An error that is not from a browser is not the lock.
	assert!(!is_locked(&io::Error::new(io::ErrorKind::PermissionDenied, "nope")));
}

/// The names above are the ones that the browser throws, checked against Chrome and not against
/// the text of a specification.
#[wasm_bindgen_test]
async fn the_exceptions_that_chrome_throws_map_as_the_table_says() {
	let name = unique("real-errors");
	let raw = OpfsRaw::open(&name).await.unwrap();

	// NotFoundError.
	let error = raw.open("p00000000").await.expect_err("a slot that is not there");
	assert_eq!(error.kind(), io::ErrorKind::NotFound, "{error}");
	assert_eq!(dom_error(&error).unwrap().name, "NotFoundError");

	// TypeMismatchError: a directory where a file is asked for.
	let options = web_sys::FileSystemGetDirectoryOptions::new();
	options.set_create(true);
	JsFuture::from(raw.dir.get_directory_handle_with_options("sub", &options)).await.unwrap();
	let error = raw.open("sub").await.expect_err("a directory is not a slot");
	assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{error}");
	assert_eq!(dom_error(&error).unwrap().name, "TypeMismatchError");

	// TypeError: a name the browser refuses.
	let error = raw.open("a/b").await.expect_err("a name with a slash");
	assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{error}");

	// NoModificationAllowedError: a second handle on a file that is held.
	let first = raw.create("p00000000").await.unwrap();
	let error = raw.open("p00000000").await.expect_err("a slot that is held");
	assert!(is_locked(&error), "{error}");
	assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);

	// InvalidStateError: a handle that was closed.
	first.close().unwrap();
	let error = first.size().expect_err("a closed handle has no size");
	assert_eq!(error.kind(), io::ErrorKind::NotConnected, "{error}");
	assert_eq!(dom_error(&error).unwrap().name, "InvalidStateError");
	let error = first.write_at(0, b"x").expect_err("a closed handle is not written");
	assert_eq!(error.kind(), io::ErrorKind::NotConnected, "{error}");
	let error = first.set_len(0).expect_err("a closed handle is not truncated");
	assert_eq!(error.kind(), io::ErrorKind::NotConnected, "{error}");
	let error = first.flush().expect_err("a closed handle is not flushed");
	assert_eq!(error.kind(), io::ErrorKind::NotConnected, "{error}");
	let mut buf = [0u8; 1];
	let error = first.read_at(0, &mut buf).expect_err("a closed handle is not read");
	assert_eq!(error.kind(), io::ErrorKind::NotConnected, "{error}");
	first.close().unwrap();
	drop(first);

	// A name that is not one is refused before the browser is asked.
	for bad in ["", ".", "..", "a/b", "a\\b", "nul\0"] {
		let error = OpfsRaw::open(bad).await.expect_err("a bad name");
		assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{bad:?}");
	}
	let error = OpfsRaw::destroy("a/b").await.expect_err("a bad name");
	assert_eq!(error.kind(), io::ErrorKind::InvalidInput);

	// The directory that the test made is not a pool's, so the removal of the database leaves it
	// and says so; the test takes it away itself.
	let error = OpfsRaw::destroy(&name).await.expect_err("an entry that is not a pool's");
	assert!(vfs::is_not_a_pool(&error), "{error}");
	JsFuture::from(raw.dir.remove_entry("sub")).await.unwrap();

	drop(raw);
	destroy_and_verify(&name).await;
}

// ---------------------------------------------------------------------------------------------
// The handle: a real file, and a fake one
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn truncate_and_the_size_are_the_browsers_and_are_live() {
	let name = unique("truncate");
	let raw = OpfsRaw::open(&name).await.unwrap();
	let file = raw.create("p00000000").await.unwrap();
	assert_eq!(file.size().unwrap(), 0);

	assert_eq!(file.write_at(0, &[7u8; 100]).unwrap(), 100);
	assert_eq!(file.size().unwrap(), 100, "a write moves the size");

	file.set_len(10).unwrap();
	assert_eq!(file.size().unwrap(), 10, "a shrink moves it down");
	let mut buf = [0u8; 20];
	assert_eq!(file.read_at(0, &mut buf).unwrap(), 10, "a read stops where the file ends");
	assert_eq!(&buf[..10], &[7u8; 10]);

	file.set_len(50).unwrap();
	assert_eq!(file.size().unwrap(), 50, "a grow moves it up");
	let mut buf = [1u8; 50];
	assert_eq!(file.read_at(0, &mut buf).unwrap(), 50);
	assert_eq!(&buf[..10], &[7u8; 10]);
	assert!(buf[10..].iter().all(|b| *b == 0), "the bytes that were cut off do not come back");

	// A write past the end fills the gap with zeros.
	assert_eq!(file.write_at(60, b"tail").unwrap(), 4);
	assert_eq!(file.size().unwrap(), 64);
	let mut buf = [1u8; 64];
	assert_eq!(file.read_at(0, &mut buf).unwrap(), 64);
	assert!(buf[50..60].iter().all(|b| *b == 0));
	assert_eq!(&buf[60..], b"tail");

	// A read at and after the end is empty, and not an error.
	assert_eq!(file.read_at(64, &mut buf).unwrap(), 0);
	assert_eq!(file.read_at(1000, &mut buf).unwrap(), 0);

	file.flush().unwrap();
	file.close().unwrap();
	drop(file);

	// The size and the content are the same after a reopen.
	let file = raw.open("p00000000").await.unwrap();
	assert_eq!(file.size().unwrap(), 64);
	let mut buf = [0u8; 64];
	assert_eq!(file.read_at(0, &mut buf).unwrap(), 64);
	assert_eq!(&buf[60..], b"tail");
	file.set_len(0).unwrap();
	assert_eq!(file.size().unwrap(), 0);
	drop(file);

	drop(raw);
	destroy_and_verify(&name).await;
}

/// A handle behind an `OpfsSyncFile` that is made to write short, to fail, or to answer wrongly, as
/// the real one cannot be made to.
#[derive(Default)]
struct FakeState {
	data: Vec<u8>,
	/// The offset and length of every write call.
	writes: Vec<(u64, usize)>,
	/// The most bytes one write call takes; `0` is no limit.
	max_write: usize,
	/// From this call on (counted from 0), a write returns `Ok(0)`.
	zero_from: Option<usize>,
	/// The call (counted from 0) that fails with this error, once.
	fail_write: Option<(usize, io::Error)>,
	fail_truncate: Option<io::Error>,
	fail_size: Option<io::Error>,
	closes: usize,
}

struct Fake(Rc<RefCell<FakeState>>);

impl Fake {
	fn file(state: FakeState) -> (OpfsSyncFile, Rc<RefCell<FakeState>>) {
		let state = Rc::new(RefCell::new(state));
		(OpfsSyncFile::from_ops(Box::new(Fake(Rc::clone(&state)))), state)
	}
}

impl SyncHandleOps for Fake {
	fn read(&self, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
		let state = self.0.borrow();
		let start = (offset as usize).min(state.data.len());
		let n = buf.len().min(state.data.len() - start);
		buf[..n].copy_from_slice(&state.data[start..start + n]);
		Ok(n)
	}

	fn write(&self, offset: u64, buf: &[u8]) -> io::Result<usize> {
		let mut state = self.0.borrow_mut();
		let call = state.writes.len();
		state.writes.push((offset, buf.len()));
		if matches!(&state.fail_write, Some((at, _)) if *at == call) {
			return Err(state.fail_write.take().unwrap().1);
		}
		if state.zero_from.is_some_and(|from| call >= from) {
			return Ok(0);
		}
		let n = if state.max_write == 0 {
			buf.len()
		} else {
			buf.len().min(state.max_write)
		};
		if n == 0 {
			return Ok(0);
		}
		let end = offset as usize + n;
		if state.data.len() < end {
			state.data.resize(end, 0);
		}
		state.data[offset as usize..end].copy_from_slice(&buf[..n]);
		Ok(n)
	}

	fn truncate(&self, len: u64) -> io::Result<()> {
		let mut state = self.0.borrow_mut();
		if let Some(error) = state.fail_truncate.take() {
			return Err(error);
		}
		state.data.resize(len as usize, 0);
		Ok(())
	}

	fn get_size(&self) -> io::Result<u64> {
		let mut state = self.0.borrow_mut();
		if let Some(error) = state.fail_size.take() {
			return Err(error);
		}
		Ok(state.data.len() as u64)
	}

	fn flush(&self) -> io::Result<()> {
		Ok(())
	}

	fn close(&self) {
		self.0.borrow_mut().closes += 1;
	}
}

fn quota() -> io::Error {
	map_exception("OPFS write", "QuotaExceededError", "the quota is used up")
}

#[wasm_bindgen_test]
fn a_short_write_is_continued_until_it_is_whole() {
	let (file, state) = Fake::file(FakeState {
		max_write: 3,
		..FakeState::default()
	});
	let data: Vec<u8> = (0..10).collect();
	assert_eq!(file.write_at(5, &data).unwrap(), 10);
	assert_eq!(state.borrow().writes, vec![(5, 10), (8, 7), (11, 4), (14, 1)]);
	assert_eq!(&state.borrow().data[5..], &data[..]);
	assert_eq!(file.size().unwrap(), 15);

	// A write that is not short is one call.
	state.borrow_mut().writes.clear();
	state.borrow_mut().max_write = 0;
	assert_eq!(file.write_at(0, b"abcd").unwrap(), 4);
	assert_eq!(state.borrow().writes, vec![(0, 4)]);

	// An empty buffer is passed on once, as it is, and does not loop.
	state.borrow_mut().writes.clear();
	assert_eq!(file.write_at(100, b"").unwrap(), 0);
	assert_eq!(state.borrow().writes, vec![(100, 0)]);
	assert_eq!(file.size().unwrap(), 15, "an empty write does not extend the file");

	// The loop on its own, with a count that is longer than the buffer: it is clamped.
	let mut seen = Vec::new();
	let done = write_all_with(0, b"abcdef", |at, rest| {
		seen.push((at, rest.len()));
		Ok(rest.len() + 4)
	})
	.unwrap();
	assert_eq!((done, seen), (6, vec![(0, 6)]));
}

#[wasm_bindgen_test]
fn a_write_that_stops_making_progress_or_fails_is_an_error_and_keeps_what_landed() {
	let (file, state) = Fake::file(FakeState {
		max_write: 4,
		zero_from: Some(2),
		..FakeState::default()
	});
	let error = file.write_at(0, &[9u8; 20]).expect_err("no progress");
	let Error::Io(error) = error else {
		panic!("an I/O error");
	};
	assert_eq!(error.kind(), io::ErrorKind::WriteZero);
	assert_eq!(state.borrow().data, vec![9u8; 8], "the two calls that wrote stay in the file");

	// A failure in the middle is the failure of the browser, with its kind: the quota.
	let (file, state) = Fake::file(FakeState {
		max_write: 4,
		fail_write: Some((1, quota())),
		..FakeState::default()
	});
	let error = file.write_at(0, &[1u8; 10]).expect_err("the quota");
	let Error::Io(error) = error else {
		panic!("an I/O error");
	};
	assert_eq!(error.kind(), io::ErrorKind::StorageFull);
	assert!(!vfs::is_pool_full(&error));
	assert_eq!(dom_error(&error).unwrap().name, "QuotaExceededError");
	assert_eq!(state.borrow().data.len(), 4, "the first call landed");
}

#[wasm_bindgen_test]
fn the_raw_file_passes_a_short_write_on_for_the_pool_to_continue() {
	let (file, state) = Fake::file(FakeState {
		max_write: 3,
		..FakeState::default()
	});
	let raw = OpfsRawFile::from_sync_file(file);
	assert_eq!(raw.write_at(0, b"abcdefgh").unwrap(), 3, "one call, and its count");
	assert_eq!(state.borrow().writes, vec![(0, 8)]);
	assert_eq!(raw.size().unwrap(), 3);
	raw.set_len(1).unwrap();
	assert_eq!(raw.size().unwrap(), 1);
	raw.close().unwrap();
	raw.close().unwrap();
	assert_eq!(state.borrow().closes, 1, "a file is closed once, however often it is asked to");
	drop(raw);
	assert_eq!(state.borrow().closes, 1, "and its drop does not close it again");

	// A file that is dropped without a close closes its handle.
	let (file, state) = Fake::file(FakeState::default());
	drop(file);
	assert_eq!(state.borrow().closes, 1);
}

#[wasm_bindgen_test]
fn the_quota_is_a_full_disk_at_a_write_and_at_a_truncate_and_is_not_a_full_pool() {
	let (file, _) = Fake::file(FakeState {
		fail_write: Some((0, quota())),
		fail_truncate: Some(map_exception("OPFS truncate", "QuotaExceededError", "full")),
		..FakeState::default()
	});
	let raw = OpfsRawFile::from_sync_file(file);
	let error = raw.write_at(0, b"x").expect_err("a write over the quota");
	assert_eq!(error.kind(), io::ErrorKind::StorageFull);
	let error = raw.set_len(1 << 20).expect_err("a truncate over the quota");
	assert_eq!(error.kind(), io::ErrorKind::StorageFull);
	assert!(!vfs::is_pool_full(&error) && !vfs::is_pool_exhausted(&error));
	// The conversion into the engine's error keeps the kind, and is not the typed pool error.
	let engine: Error = error.into();
	assert!(
		matches!(&engine, Error::Io(e) if e.kind() == io::ErrorKind::StorageFull),
		"{engine:?}"
	);
	// Nothing else is broken by it: the next write goes through.
	assert_eq!(raw.write_at(0, b"x").unwrap(), 1);
}

#[wasm_bindgen_test]
fn a_size_that_the_browser_cannot_give_is_an_error_and_not_zero() {
	let failing = || FakeState {
		fail_size: Some(map_exception("OPFS getSize", "InvalidStateError", "closed")),
		..FakeState::default()
	};
	let (file, _) = Fake::file(failing());
	let error = file.size().expect_err("no size");
	assert!(matches!(&error, Error::Io(e) if e.kind() == io::ErrorKind::NotConnected));

	// A log over a file whose size is not known must not start at zero.
	let (file, _) = Fake::file(failing());
	assert!(OpfsLogStore::try_new(Arc::new(file)).is_err());
	let (file, state) = Fake::file(FakeState {
		data: vec![1u8; 12],
		..FakeState::default()
	});
	drop(state);
	assert!(OpfsLogStore::try_new(Arc::new(file)).is_ok());

	let (file, _) = Fake::file(failing());
	let raw = OpfsRawFile::from_sync_file(file);
	assert_eq!(raw.size().expect_err("no size").kind(), io::ErrorKind::NotConnected);
	assert_eq!(raw.size().unwrap(), 0, "and the next question is asked again, not cached");
}

// ---------------------------------------------------------------------------------------------
// The directory
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn the_backend_lists_creates_opens_and_locks_the_way_the_pool_asks() {
	let name = unique("raw");
	let raw = OpfsRaw::open(&name).await.unwrap();
	assert_eq!(raw.name(), name);
	assert!(raw.list().await.unwrap().is_empty());

	// `create` makes an empty slot, and is `AlreadyExists` for one that is there.
	let first = raw.create("p00000000").await.unwrap();
	assert_eq!(first.size().unwrap(), 0);
	let second = raw.create("p00000001").await.unwrap();
	let error = raw.create("p00000000").await.expect_err("it is there");
	assert_eq!(error.kind(), io::ErrorKind::AlreadyExists, "{error}");
	first.close().unwrap();
	let error = raw.create("p00000000").await.expect_err("closed, and still there");
	assert_eq!(error.kind(), io::ErrorKind::AlreadyExists, "{error}");

	// The lock is a file that the listing does not show, and one holder at a time.
	let lock = raw.open_lock().await.unwrap();
	let error = raw.open_lock().await.expect_err("a held lock");
	assert!(is_locked(&error), "{error}");
	assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
	let mut listed = raw.list().await.unwrap();
	listed.sort();
	assert_eq!(listed, ["p00000000", "p00000001"], "the listing has no LOCK");

	// An open slot is exclusive: a second open is the lock of another holder, the first goes on.
	let error = raw.open("p00000001").await.expect_err("held");
	assert!(is_locked(&error), "{error}");
	assert_eq!(second.write_at(0, b"still open").unwrap(), 10);

	// A closed one is open again, and has what was written.
	second.close().unwrap();
	let again = raw.open("p00000001").await.unwrap();
	assert_eq!(again.size().unwrap(), 10);
	lock.close().unwrap();
	let lock = raw.open_lock().await.expect("the lock is free once it is closed");
	lock.close().unwrap();
	again.close().unwrap();

	drop((first, second, again, lock));
	drop(raw);
	destroy_and_verify(&name).await;
}

#[wasm_bindgen_test]
async fn a_dropped_open_closes_the_handle_that_arrives_late() {
	let name = unique("late");
	let raw = OpfsRaw::open(&name).await.unwrap();
	drop(raw.create("p00000000").await.unwrap());
	// Dropped at every stage: before the first poll, while the file is looked up, and while the
	// sync access handle is being made.
	for polls in 0..8 {
		let mut open = raw.open("p00000000");
		for _ in 0..polls {
			if poll_once(&mut open).await {
				break;
			}
			rt::relax().await;
		}
		drop(open);
		settle().await;
		let file = raw
			.open("p00000000")
			.await
			.unwrap_or_else(|e| panic!("open after an open dropped after {polls} polls: {e}"));
		drop(file);
	}
	// The same for the lock and for a creation.
	for polls in 0..8 {
		let mut lock = raw.open_lock();
		for _ in 0..polls {
			if poll_once(&mut lock).await {
				break;
			}
			rt::relax().await;
		}
		drop(lock);
		settle().await;
		drop(raw.open_lock().await.unwrap_or_else(|e| panic!("lock after {polls} polls: {e}")));
	}
	for polls in 0..8 {
		let slot = format!("p{:08x}", polls + 1);
		let mut create = raw.create(&slot);
		for _ in 0..polls {
			if poll_once(&mut create).await {
				break;
			}
			rt::relax().await;
		}
		drop(create);
		settle().await;
		// The file may or may not have been made. If it was, it is open again at once.
		match raw.create(&slot).await {
			Ok(file) => drop(file),
			Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
				drop(raw.open(&slot).await.unwrap_or_else(|e| {
					panic!("open of a creation dropped after {polls} polls: {e}")
				}))
			}
			Err(e) => panic!("create after {polls} polls: {e}"),
		}
	}
	drop(raw);
	destroy_and_verify(&name).await;
}

/// The control of the test above: an open that is dropped without the closer does lock the file,
/// for longer than this test waits. It is what the guard is there for, and what makes the test
/// above able to fail.
#[wasm_bindgen_test]
async fn control_an_open_that_is_dropped_without_the_closer_locks_its_file() {
	let name = unique("control");
	let raw = OpfsRaw::open(&name).await.unwrap();
	drop(raw.create("p00000000").await.unwrap());
	let handle = get_file_handle_io(&raw.dir, "p00000000", false).await.unwrap();
	let promise = handle.create_sync_access_handle();
	drop(JsFuture::from(promise.clone()));
	settle().await;
	let error = raw.open("p00000000").await.expect_err("a handle that nobody closed");
	assert!(is_locked(&error), "{error}");
	// Release it, so that the directory can be removed.
	let late: web_sys::FileSystemSyncAccessHandle =
		JsFuture::from(promise).await.unwrap().unchecked_into();
	late.close();
	drop(raw.open("p00000000").await.expect("open once it is closed"));
	drop(raw);
	destroy_and_verify(&name).await;
}

#[wasm_bindgen_test]
async fn destroy_removes_a_pool_and_is_repeatable_and_refuses_a_pool_that_is_held() {
	let name = unique("destroy");

	// What is not there is already destroyed.
	OpfsRaw::destroy(&name).await.unwrap();
	OpfsRaw::destroy(&name).await.unwrap();

	let (fs, _) = mount(&name, 4).await;
	fs.create_dir_all(p("/db")).unwrap();
	put(&fs, "/db/f", b"kept");

	// A pool that is mounted holds the lock: destroy fails with the typed lock error, and removes
	// nothing.
	let error = OpfsRaw::destroy(&name).await.expect_err("the pool is held");
	assert!(is_locked(&error), "{error}");
	assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
	assert_eq!(fs.read(p("/db/f")).unwrap(), b"kept");
	put(&fs, "/db/g", b"and this works too");
	assert_eq!(names(&name).await.len(), 4, "the slots are still there");

	close(&fs);
	drop(fs);

	// A slot that is open somewhere makes the removal of the directory fail, in the same way.
	let raw = OpfsRaw::open(&name).await.unwrap();
	let slot = raw.open("p00000000").await.unwrap();
	let error = OpfsRaw::destroy(&name).await.expect_err("a slot is held");
	assert!(is_locked(&error), "{error}");
	slot.close().unwrap();
	drop((slot, raw));

	// Once every handle is closed it goes, with its files.
	destroy_and_verify(&name).await;
	OpfsRaw::destroy(&name).await.unwrap();

	// And a database of that name can be made again, empty.
	let (fs, report) = mount(&name, 2).await;
	assert_eq!(report.files, 0);
	assert!(fs.read(p("/db/f")).is_err());
	close(&fs);
	drop(fs);
	destroy_and_verify(&name).await;
}

// ---------------------------------------------------------------------------------------------
// Mounting
// ---------------------------------------------------------------------------------------------

#[wasm_bindgen_test]
async fn an_empty_pool_is_mounted_and_holds_its_lock_until_it_is_closed() {
	let name = unique("mount");
	let (fs, report) = mount(&name, 5).await;
	assert_eq!((report.files, report.spares), (0, 5), "{report:?}");
	assert_eq!(names(&name).await.len(), 5, "five slots, and no LOCK among them");

	let raw = OpfsRaw::open(&name).await.unwrap();
	let error = raw.open_lock().await.expect_err("the mount holds the lock");
	assert!(is_locked(&error), "{error}");

	close(&fs);
	drop(fs);
	drop(raw.open_lock().await.expect("the lock is free after the close"));
	drop(raw);
	destroy_and_verify(&name).await;
}

#[wasm_bindgen_test]
async fn a_second_mount_fails_fast_with_the_lock_error_and_leaves_nothing_open() {
	let name = unique("double");
	let (first, _) = mount(&name, 3).await;
	first.create_dir_all(p("/db")).unwrap();

	let started = rt::Instant::now();
	let error = mount_with(&name, config(3)).await.expect_err("the second mount");
	let waited = started.elapsed();
	assert!(is_permission_denied(&error), "{error:?}");
	assert!(error.to_string().contains("another tab"), "{error}");
	assert!(
		waited >= Duration::from_millis(250) && waited < Duration::from_secs(3),
		"it polls for its deadline, and no longer: {waited:?}"
	);

	// The first mount did not feel it.
	put(&first, "/db/f", b"fine");
	assert_eq!(first.read(p("/db/f")).unwrap(), b"fine");
	close(&first);
	drop(first);

	// And the failed one left no handle: it mounts at once, and a close lets the directory go.
	let started = rt::Instant::now();
	let (second, report) = mount(&name, 3).await;
	assert!(started.elapsed() < Duration::from_millis(250), "no wait: {:?}", started.elapsed());
	assert_eq!(report.files, 1);
	assert_eq!(second.read(p("/db/f")).unwrap(), b"fine");
	close(&second);
	drop(second);
	destroy_and_verify(&name).await;
}

#[wasm_bindgen_test]
async fn a_pool_remounts_after_a_close_with_its_files_and_after_an_abandon_with_the_synced_ones() {
	let name = unique("remount");
	let (fs, _) = mount(&name, 6).await;
	fs.create_dir_all(p("/db/wal")).unwrap();
	put(&fs, "/db/wal/000001.wal", b"first log");
	put(&fs, "/db/MANIFEST", b"manifest one");
	fs.replace_atomic(p("/db/MANIFEST"), b"manifest two").unwrap();
	let removed = fs.create_new(p("/db/wal/gone"), CreateOpts::default()).unwrap();
	removed.append(b"to be removed").unwrap();
	removed.sync().unwrap();
	drop(removed);
	fs.remove_file(p("/db/wal/gone")).unwrap();
	close(&fs);
	drop(fs);

	let (fs, report) = mount(&name, 6).await;
	assert_eq!(report.files, 2, "{report:?}");
	assert!(report.orphans_kept.is_empty(), "{report:?}");
	assert_eq!(fs.read(p("/db/wal/000001.wal")).unwrap(), b"first log");
	assert_eq!(fs.read(p("/db/MANIFEST")).unwrap(), b"manifest two");
	assert!(!fs.exists(p("/db/wal/gone")));
	assert!(fs.is_dir(p("/db/wal")));

	// A crash that releases the handles and flushes nothing: what was synced is there.
	put(&fs, "/db/wal/000002.wal", b"second log");
	PoolControl::abandon(&*fs);
	drop(fs);
	let (fs, report) = mount(&name, 6).await;
	assert_eq!(report.files, 3, "{report:?}");
	assert_eq!(fs.read(p("/db/wal/000002.wal")).unwrap(), b"second log");
	assert_eq!(fs.read(p("/db/MANIFEST")).unwrap(), b"manifest two");

	// A pool that is dropped without a close or an abandon releases its handles too.
	drop(fs);
	let (fs, _) = mount(&name, 6).await;
	close(&fs);
	drop(fs);
	destroy_and_verify(&name).await;
}

/// JavaScript of a worker that takes a pool's lock and one of its slots, as the previous page of a
/// reload does, and says so. With `spin` it then loops in JavaScript, and with `write` it loops
/// writing and flushing, which a browser stops about two seconds after it was asked to (or later,
/// see the test below).
const HOLDER: &str = r#"
self.onmessage = async (e) => {
  const { name, mode } = e.data;
  try {
    const root = await navigator.storage.getDirectory();
    const dir = await root.getDirectoryHandle(name, { create: true });
    const lock = await (await dir.getFileHandle('LOCK', { create: true })).createSyncAccessHandle();
    const slot = await (await dir.getFileHandle('p00000000', { create: true })).createSyncAccessHandle();
    slot.write(new Uint8Array([1, 2, 3]), { at: 0 });
    self.postMessage('ready');
    if (mode === 'spin') {
      const end = Date.now() + 60000;
      while (Date.now() < end) {}
    } else if (mode === 'write') {
      const bytes = new Uint8Array(4096);
      const end = Date.now() + 60000;
      while (Date.now() < end) {
        slot.write(bytes, { at: 8192 });
        slot.flush();
      }
    }
  } catch (err) {
    self.postMessage('error: ' + err.name + ': ' + err.message);
  }
};
"#;

/// Starts a worker that holds the pool `name`, in `mode`: `idle`, `spin` or `write`.
async fn spawn_holder(name: &str, mode: &str) -> JsValue {
	let make = js_sys::Function::new_with_args(
		"src, name, mode",
		r#"return new Promise((resolve, reject) => {
		  const url = URL.createObjectURL(new Blob([src], { type: 'text/javascript' }));
		  const worker = new Worker(url);
		  worker.onmessage = (e) => {
		    if (e.data === 'ready') resolve(worker); else reject(new Error(String(e.data)));
		  };
		  worker.onerror = (e) => reject(new Error('worker error: ' + e.message));
		  worker.postMessage({ name, mode });
		})"#,
	);
	let promise = make
		.call3(&JsValue::NULL, &HOLDER.into(), &name.into(), &mode.into())
		.expect("the helper worker starts");
	JsFuture::from(js_sys::Promise::from(promise)).await.expect("the helper worker holds the pool")
}

fn terminate(worker: &JsValue) {
	let terminate = js_sys::Reflect::get(worker, &"terminate".into()).unwrap();
	terminate.unchecked_into::<js_sys::Function>().call0(worker).unwrap();
}

/// A mount from this worker while another one holds the pool fails, and once that worker is
/// terminated it succeeds within the polling window: the case of a reload.
#[wasm_bindgen_test]
async fn a_pool_mounts_after_the_worker_that_held_it_was_terminated() {
	let name = unique("restart");
	let holder = spawn_holder(&name, "idle").await;

	let error = mount_with(&name, config(2)).await.expect_err("held by the other worker");
	assert!(is_permission_denied(&error), "{error:?}");

	terminate(&holder);
	let cfg = PoolConfig {
		lock_deadline: Duration::from_secs(8),
		lock_poll: Duration::from_millis(50),
		..config(2)
	};
	let started = rt::Instant::now();
	let (fs, report) = mount_with(&name, cfg).await.expect("mount after the holder was terminated");
	console_log!("mount after the termination of an idle holder: {:?}", started.elapsed());
	// The slot that the dead worker had: three bytes, and no header, so it is blank.
	assert_eq!(report.blank_initialised, 1, "{report:?}");
	assert!(report.orphans_kept.is_empty(), "{report:?}");
	assert_eq!(report.files, 0);
	close(&fs);
	drop(fs);
	destroy_and_verify(&name).await;
}

/// The same with a worker that is in the middle of a loop, which the browser stops late. The
/// lock of a page that was killed in the middle of a write may be held for much longer than the
/// five seconds that the mount polls for by default. The mount then fails with the lock error and
/// a later one succeeds, so the test polls for up to 90 s, prints how long it took, and holds the
/// mount to no time.
#[wasm_bindgen_test]
async fn a_pool_mounts_after_a_busy_worker_that_held_it_was_terminated() {
	for mode in ["spin", "write"] {
		let name = unique("busy-restart");
		let holder = spawn_holder(&name, mode).await;
		rt::sleep(Duration::from_millis(100)).await;
		terminate(&holder);

		let cfg = PoolConfig {
			lock_deadline: Duration::from_secs(90),
			lock_poll: Duration::from_millis(75),
			..config(2)
		};
		let started = rt::Instant::now();
		let (fs, report) =
			mount_with(&name, cfg).await.expect("mount after the busy holder was terminated");
		let waited = started.elapsed();
		console_log!(
			"mount after the termination of a holder that is in a {mode} loop: {waited:?}"
		);
		if mode == "spin" {
			assert_eq!(report.blank_initialised, 1, "{report:?}");
		} else {
			// The loop had written a payload into a slot that has no header: kept, not reused.
			assert_eq!(report.orphans_kept, ["p00000000"], "{report:?}");
		}
		close(&fs);
		drop(fs);
		destroy_and_verify(&name).await;
	}
}

/// A slot of a pool that was hand-built the way a crash leaves it: a header page that was torn,
/// one that lost to a newer path, one that is not a header and has a payload, and a free one.
#[wasm_bindgen_test]
async fn a_torn_header_in_a_real_slot_is_recovered() {
	let name = unique("torn");
	let raw = OpfsRaw::open(&name).await.unwrap();

	async fn write_slot(raw: &OpfsRaw, slot: &str, page_a: &[u8], page_b: &[u8], payload: &[u8]) {
		let file = raw.create(slot).await.unwrap();
		let mut bytes = Vec::new();
		bytes.extend_from_slice(page_a);
		bytes.extend_from_slice(page_b);
		bytes.extend_from_slice(payload);
		assert_eq!(file.write_at(0, &bytes).unwrap(), bytes.len());
		file.flush().unwrap();
		file.close().unwrap();
	}

	let valid = |seq: u64, path: &str| Header::named(seq, path).encode().unwrap();
	let mut torn = valid(9, "/db/g");
	torn[100] ^= 0xff;
	assert_eq!(torn.len(), PAGE_LEN);
	let blank = [0u8; PAGE_LEN];

	// The current header is the valid page of the highest sequence, the torn page does not count,
	// and the payload is the file.
	write_slot(&raw, "p00000000", &valid(5, "/db/f"), &torn, b"hello").await;
	// A second claimant of the path with a lower sequence: the loser, cleaned.
	write_slot(&raw, "p00000001", &valid(4, "/db/f"), &blank, b"old data").await;
	// No valid page and a payload: kept and never used.
	let garbage = {
		let mut page = [0u8; PAGE_LEN];
		page[..19].copy_from_slice(b"not a header at all");
		page
	};
	write_slot(&raw, "p00000002", &garbage, &blank, b"xyz").await;
	// A free slot.
	write_slot(&raw, "p00000003", &Header::free(2).encode().unwrap(), &blank, b"").await;
	drop(raw);

	let (fs, report) = mount(&name, 4).await;
	assert_eq!(report.files, 1, "{report:?}");
	assert_eq!(report.losers_cleaned, 1, "{report:?}");
	assert_eq!(report.orphans_kept, ["p00000002"], "{report:?}");
	assert_eq!(report.spares, 2, "the free slot and the cleaned loser: {report:?}");
	assert!(report.max_seq >= 5, "{report:?}");
	assert_eq!(fs.read(p("/db/f")).unwrap(), b"hello");
	assert!(!fs.exists(p("/db/g")), "the torn page does not name a file");
	let listing: Vec<String> = fs
		.read_dir(p("/db"))
		.unwrap()
		.into_iter()
		.map(|e| e.name.to_string_lossy().into_owned())
		.collect();
	assert_eq!(listing, ["f"]);
	close(&fs);
	drop(fs);

	// What the mount did to the files: the loser is cut to its headers, the orphan is as it was.
	let raw = OpfsRaw::open(&name).await.unwrap();
	let loser = raw.open("p00000001").await.unwrap();
	assert_eq!(loser.size().unwrap(), HEADER_LEN, "the loser is cleaned");
	loser.close().unwrap();
	let orphan = raw.open("p00000002").await.unwrap();
	assert_eq!(orphan.size().unwrap(), HEADER_LEN + 3, "the orphan is untouched");
	let mut head = [0u8; 19];
	orphan.read_at(0, &mut head).unwrap();
	assert_eq!(&head, b"not a header at all");
	orphan.close().unwrap();
	drop((loser, orphan, raw));

	// A mount again settles the same way, with nothing left to clean.
	let (fs, report) = mount(&name, 4).await;
	assert_eq!((report.files, report.losers_cleaned), (1, 0), "{report:?}");
	assert_eq!(report.orphans_kept, ["p00000002"]);
	assert_eq!(fs.read(p("/db/f")).unwrap(), b"hello");
	close(&fs);
	drop(fs);
	destroy_and_verify(&name).await;
}

// ---------------------------------------------------------------------------------------------
// Handle accounting
// ---------------------------------------------------------------------------------------------

/// A mount whose future is dropped part of the way: polled `k` times, with the event loop let
/// through between, so that it is in the lock, the listing, or the opens. Whatever it had in
/// flight arrives late and is closed, so the next mount, which does not wait, takes the lock and
/// opens every slot, and the directory can be removed at the end.
#[wasm_bindgen_test]
async fn a_dropped_mount_leaves_no_handle_open() {
	let name = unique("dropped-mount");
	{
		let (fs, _) = mount(&name, 40).await;
		put(&fs, "/kept", b"kept");
		close(&fs);
	}
	let cfg = PoolConfig {
		lock_deadline: Duration::ZERO,
		..config(40)
	};
	for polls in 0..14 {
		let raw = OpfsRaw::open(&name).await.unwrap();
		let mut mounting = Box::pin(PoolFs::mount(raw, cfg.clone()));
		let mut finished = false;
		for _ in 0..polls {
			if poll_once(&mut mounting).await {
				finished = true;
				break;
			}
			rt::relax().await;
		}
		drop(mounting);
		settle().await;
		let (fs, report) = mount_with(&name, cfg.clone())
			.await
			.unwrap_or_else(|e| panic!("mount after a mount dropped after {polls} polls: {e}"));
		assert_eq!(report.files, 1, "after {polls} polls (finished: {finished}): {report:?}");
		assert_eq!(fs.read(p("/kept")).unwrap(), b"kept");
		close(&fs);
	}
	destroy_and_verify(&name).await;
}

#[wasm_bindgen_test]
async fn a_dropped_mount_that_was_cancelled_while_waiting_for_the_lock_leaves_nothing_open() {
	let name = unique("dropped-wait");
	let (held, _) = mount(&name, 3).await;
	// The second mount is polling for the lock when it is dropped.
	let cfg = PoolConfig {
		lock_deadline: Duration::from_secs(30),
		lock_poll: Duration::from_millis(25),
		..config(3)
	};
	let raw = OpfsRaw::open(&name).await.unwrap();
	let mut waiting = Box::pin(PoolFs::mount(raw, cfg));
	for _ in 0..10 {
		assert!(!poll_once(&mut waiting).await, "the lock is held");
		rt::sleep(Duration::from_millis(30)).await;
	}
	drop(waiting);
	close(&held);
	drop(held);
	settle().await;
	let (fs, _) = mount(&name, 3).await;
	close(&fs);
	drop(fs);
	destroy_and_verify(&name).await;
}

#[wasm_bindgen_test]
async fn close_abandon_and_drop_each_release_every_handle() {
	for how in ["close", "abandon", "drop"] {
		let name = unique(how);
		let (fs, _) = mount(&name, 12).await;
		fs.create_dir_all(p("/db")).unwrap();
		for i in 0..4 {
			put(&fs, &format!("/db/f{i}"), b"x");
		}
		match how {
			"close" => close(&fs),
			"abandon" => PoolControl::abandon(&*fs),
			_ => {}
		}
		drop(fs);
		// Every handle is closed or this fails: a directory with an open handle cannot go.
		destroy_and_verify(&name).await;
	}
}

// ---------------------------------------------------------------------------------------------
// The contract that every file system passes
// ---------------------------------------------------------------------------------------------

/// The same function that the pool over memory and the native file system pass.
#[wasm_bindgen_test]
async fn the_pool_over_opfs_honours_the_contract() {
	let name = unique("contract");
	let cfg = PoolConfig {
		lock_deadline: Duration::from_millis(300),
		..PoolConfig::strict(64, 64)
	};
	let (fs, report) = mount_with(&name, cfg).await.unwrap();
	assert!(report.orphans_kept.is_empty(), "{report:?}");
	let root = Path::new("/root");
	fs.create_dir_all(root).unwrap();
	let started = rt::Instant::now();
	contract::run_all(&*fs, root);
	console_log!("the contract over OPFS: {:?}", started.elapsed());
	assert_eq!(PoolControl::stats(&*fs).total, 64, "no slot was made beyond the initial ones");
	close(&fs);
	drop(fs);

	// What was left in the pool by the contract is there after a remount.
	let cfg = PoolConfig {
		lock_deadline: Duration::from_millis(300),
		..PoolConfig::strict(64, 64)
	};
	let (fs, report) = mount_with(&name, cfg).await.unwrap();
	assert!(report.orphans_kept.is_empty(), "{report:?}");
	assert!(report.files > 0, "{report:?}");
	close(&fs);
	drop(fs);
	destroy_and_verify(&name).await;
}

#[wasm_bindgen_test]
async fn the_pool_over_opfs_honours_what_only_a_pool_promises() {
	let name = unique("semantics");
	let cfg = PoolConfig {
		lock_deadline: Duration::from_millis(300),
		..PoolConfig::strict(64, 64)
	};
	let (fs, _) = mount_with(&name, cfg).await.unwrap();
	fs.create_dir_all(p("/pool")).unwrap();
	contract::pool_semantics(&*fs, p("/pool"));
	close(&fs);
	drop(fs);
	destroy_and_verify(&name).await;
}

/// More slots than one batch of opens: the mount opens them together, in batches, and awaits every
/// one of them to its end. The time is the figure of the plan (500 files in under a second).
#[wasm_bindgen_test]
async fn a_pool_of_hundreds_of_slots_mounts_in_batches_and_closes_every_handle() {
	let name = unique("many");
	let started = rt::Instant::now();
	let (fs, report) = mount(&name, 600).await;
	console_log!("making a pool of 600 slots: {:?}", started.elapsed());
	assert_eq!((report.files, report.spares), (0, 600), "{report:?}");
	close(&fs);
	drop(fs);

	let started = rt::Instant::now();
	let (fs, report) = mount(&name, 600).await;
	let mounted = started.elapsed();
	console_log!("mounting a pool of 600 slots: {mounted:?}");
	assert_eq!((report.files, report.spares), (0, 600), "{report:?}");
	assert!(mounted < Duration::from_secs(5), "{mounted:?}");
	assert_eq!(names(&name).await.len(), 600);
	let started = rt::Instant::now();
	close(&fs);
	drop(fs);
	console_log!("closing a pool of 600 slots: {:?}", started.elapsed());
	destroy_and_verify(&name).await;
}

/// The refill task of the pool is not a future that is dropped: it is a task that keeps waiting for
/// its creation to arrive. A pool that is abandoned or closed while it has a creation in flight,
/// and mounted again at once, must not find a slot that the task left locked (the gap of the
/// reviews about `abandon` followed by a remount).
#[wasm_bindgen_test]
async fn abandon_and_close_during_a_background_refill_leave_no_handle_open() {
	for how in ["abandon", "close", "drop"] {
		for (micro, turns) in [(0, 0), (1, 0), (3, 0), (6, 0), (0, 1), (3, 1), (0, 2), (0, 3)] {
			let name = unique(&format!("refill-{how}"));
			// Two slots to start with, below the low mark, so that the task starts at once and
			// makes slots until there are 48.
			let cfg = PoolConfig {
				lock_deadline: Duration::from_secs(1),
				lock_poll: Duration::from_millis(25),
				initial: 2,
				..PoolConfig::default()
			};
			let (fs, report) = mount_with(&name, cfg.clone()).await.unwrap();
			assert_eq!(report.spares, 2, "{report:?}");
			for _ in 0..micro {
				rt::yield_now().await;
			}
			for _ in 0..turns {
				rt::relax().await;
			}
			match how {
				"abandon" => PoolControl::abandon(&*fs),
				"close" => close(&fs),
				_ => {}
			}
			drop(fs);
			let slots = names(&name).await.len();
			console_log!(
				"refill {how}, {micro} yields and {turns} turns: {slots} slots at the remount"
			);

			// At once: no settling, which is when a handle that arrives late would be found.
			let mut config = cfg.clone();
			config.background_refill = false;
			let (fs, report) = mount_with(&name, config).await.unwrap_or_else(|e| {
				panic!("remount after {how} with {micro} yields and {turns} turns of the refill task: {e}")
			});
			assert!(report.spares >= 2, "{how} {micro} {turns}: {report:?}");
			assert!(report.orphans_kept.is_empty(), "{how} {micro} {turns}: {report:?}");
			close(&fs);
			drop(fs);
			settle().await;
			destroy_and_verify(&name).await;
		}
	}
}

// ---------------------------------------------------------------------------------------------
// The late handle, the lock, and what is in the directory
// ---------------------------------------------------------------------------------------------

/// An open that is dropped after the call to the browser was issued, which is when a handle is on
/// its way and nothing holds it. The test above drops opens at the points of their own polls, and
/// passes without the closer of a late handle, so it cannot tell. This one issues the browser's
/// call itself (the first poll of the open does), drops the future before the handle came and after
/// it came and was not taken, and requires the file to be open again within half a second: without
/// the closer it stays locked for seconds (the control above shows it).
#[wasm_bindgen_test]
async fn an_open_that_is_dropped_after_the_browser_call_was_issued_closes_the_handle_when_it_arrives(
) {
	let name = unique("late-issued");
	let raw = OpfsRaw::open(&name).await.unwrap();
	drop(raw.create("p00000000").await.unwrap());
	let handle = get_file_handle_io(&raw.dir, "p00000000", false).await.unwrap();

	async fn opens_within_half_a_second(raw: &OpfsRaw, what: &str) {
		let started = rt::Instant::now();
		loop {
			match raw.open("p00000000").await {
				Ok(file) => {
					drop(file);
					return;
				}
				Err(e) if is_locked(&e) && started.elapsed() < Duration::from_millis(500) => {
					rt::sleep(Duration::from_millis(10)).await;
				}
				Err(e) => {
					panic!("{what}: the file is still locked after {:?}: {e}", started.elapsed())
				}
			}
		}
	}

	for round in 0..8 {
		// Dropped while the handle is on its way.
		let mut open = Box::pin(create_sync_access_handle_io(&handle, "p00000000"));
		assert!(!poll_once(&mut open).await, "a handle does not arrive within one poll");
		drop(open);
		for _ in 0..30 {
			rt::relax().await;
		}
		opens_within_half_a_second(&raw, &format!("dropped on its way, round {round}")).await;

		// Dropped after it arrived and before the future was polled again to take it.
		let mut open = Box::pin(create_sync_access_handle_io(&handle, "p00000000"));
		assert!(!poll_once(&mut open).await);
		rt::sleep(Duration::from_millis(40)).await;
		drop(open);
		opens_within_half_a_second(&raw, &format!("dropped after it arrived, round {round}")).await;
	}
	drop(raw);
	destroy_and_verify(&name).await;
}

/// A backend that counts how often the lock was asked for.
struct CountingLock {
	inner: OpfsRaw,
	tries: Arc<std::sync::atomic::AtomicUsize>,
}

impl Raw for CountingLock {
	type File = OpfsRawFile;

	fn list(&self) -> RawFut<'_, io::Result<Vec<String>>> {
		self.inner.list()
	}

	fn open(&self, name: &str) -> RawFut<'_, io::Result<OpfsRawFile>> {
		self.inner.open(name)
	}

	fn create(&self, name: &str) -> RawFut<'_, io::Result<OpfsRawFile>> {
		self.inner.create(name)
	}

	fn open_lock(&self) -> RawFut<'_, io::Result<OpfsRawFile>> {
		self.tries.fetch_add(1, Ordering::SeqCst);
		self.inner.open_lock()
	}
}

/// A mount of a pool whose lock another worker holds asks for it every few tens of milliseconds,
/// for its deadline: a loop that does not wait would ask thousands of times and keep the worker
/// busy, and a wait that is too long would ask a few times. The error says what the browser said.
#[wasm_bindgen_test]
async fn the_lock_that_another_worker_holds_is_asked_for_every_few_tens_of_milliseconds() {
	let name = unique("cadence");
	let holder = spawn_holder(&name, "idle").await;
	let tries = Arc::new(std::sync::atomic::AtomicUsize::new(0));
	let counting = CountingLock {
		inner: OpfsRaw::open(&name).await.unwrap(),
		tries: Arc::clone(&tries),
	};
	let cfg = PoolConfig {
		lock_deadline: Duration::from_millis(300),
		lock_poll: Duration::from_millis(25),
		..config(2)
	};
	let started = rt::Instant::now();
	let error = PoolFs::mount(counting, cfg).await.err().expect("another worker holds the lock");
	let waited = started.elapsed();
	let tries = tries.load(Ordering::SeqCst);
	assert!(waited >= Duration::from_millis(300), "it kept trying for its deadline: {waited:?}");
	// One try, and one more per 25 ms: 13 at the most; each takes a moment, so fewer. A
	// loop that does not wait tries hundreds of times, so the floor can be low for a slow machine.
	assert!((2..=14).contains(&tries), "{tries} tries in {waited:?}");
	let Error::Io(io_error) = error else {
		panic!("an I/O error was expected");
	};
	assert_eq!(io_error.kind(), io::ErrorKind::PermissionDenied);
	assert!(vfs::is_database_locked(&io_error), "{io_error}");
	assert!(Error::Io(Arc::clone(&io_error)).is_database_locked());
	let cause = io_error
		.get_ref()
		.and_then(|inner| inner.downcast_ref::<vfs::DatabaseLocked>())
		.expect("the payload says what the lock error is")
		.cause();
	assert!(is_locked(cause), "the exception of the browser is kept: {cause}");
	assert_eq!(dom_error(cause).unwrap().name, "NoModificationAllowedError");

	terminate(&holder);
	let cfg = PoolConfig {
		lock_deadline: Duration::from_secs(8),
		lock_poll: Duration::from_millis(50),
		..config(2)
	};
	let (fs, _) = mount_with(&name, cfg).await.expect("mount after the holder was terminated");
	close(&fs);
	drop(fs);
	destroy_and_verify(&name).await;
}

/// A directory of the origin private file system that has the entries of a pool and, among them,
/// ones that are not a pool's.
async fn make_dir(
	parent: &web_sys::FileSystemDirectoryHandle,
	name: &str,
) -> web_sys::FileSystemDirectoryHandle {
	let options = web_sys::FileSystemGetDirectoryOptions::new();
	options.set_create(true);
	JsFuture::from(parent.get_directory_handle_with_options(name, &options))
		.await
		.unwrap_or_else(|e| panic!("make the directory {name}: {e:?}"))
		.unchecked_into()
}

async fn make_file(parent: &web_sys::FileSystemDirectoryHandle, name: &str) {
	get_file_handle_io(parent, name, true).await.unwrap();
}

async fn remove_entry(parent: &web_sys::FileSystemDirectoryHandle, name: &str) {
	JsFuture::from(parent.remove_entry(name))
		.await
		.unwrap_or_else(|e| panic!("remove {name}: {e:?}"));
}

#[wasm_bindgen_test]
async fn a_directory_that_is_not_a_pool_is_not_mounted_and_not_removed() {
	let name = unique("foreign");
	let root = opfs_root_io().await.unwrap();
	let dir = make_dir(&root, &name).await;
	make_file(&dir, "notes.txt").await;
	make_dir(&dir, "photos").await;

	// Not a pool: refused, and nothing is put in it, not even the lock.
	let error = OpfsRaw::open_pool(&name).await.err().expect("it is somebody else's directory");
	assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{error}");
	assert!(vfs::is_not_a_pool(&error), "{error}");
	assert!(error.to_string().contains("notes.txt") && error.to_string().contains("photos"));
	let mut listed = list_names(&dir).await.unwrap();
	listed.sort();
	assert_eq!(listed, ["notes.txt", "photos"], "nothing was added");
	// And not removed.
	let error = OpfsRaw::destroy(&name).await.expect_err("it is not a pool");
	assert!(vfs::is_not_a_pool(&error), "{error}");
	let mut listed = list_names(&dir).await.unwrap();
	listed.sort();
	assert_eq!(listed, ["notes.txt", "photos"], "nothing was removed");

	// With a pool in it, the other entries are left alone and reported.
	remove_entry(&dir, "photos").await;
	let (fs, report) = mount(&name, 2).await;
	assert_eq!(report.ignored, ["notes.txt"], "{report:?}");
	assert_eq!(report.spares, 2);
	close(&fs);
	drop(fs);
	// And removing it is still refused, with the entries named: they are not the pool's.
	let error = OpfsRaw::destroy(&name).await.expect_err("a foreign entry is there");
	assert!(vfs::is_not_a_pool(&error) && error.to_string().contains("notes.txt"), "{error}");
	assert_eq!(list_names(&dir).await.unwrap().len(), 4, "the lock, two slots and the note");
	remove_entry(&dir, "notes.txt").await;
	destroy_and_verify(&name).await;

	// A name that is a file is not a directory.
	make_file(&root, &name).await;
	let error = OpfsRaw::open_pool(&name).await.err().expect("a file");
	assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{error}");
	remove_entry(&root, &name).await;
}

#[wasm_bindgen_test]
async fn a_lock_or_a_slot_that_is_a_directory_does_not_stop_a_mount_or_a_removal_for_good() {
	let name = unique("hostile");
	let root = opfs_root_io().await.unwrap();
	{
		let (fs, _) = mount(&name, 3).await;
		put(&fs, "/kept", b"kept");
		close(&fs);
	}
	let dir = make_dir(&root, &name).await;

	// A directory by the name of a slot: skipped, and its name is never used.
	let hostile = make_dir(&dir, "p00000009").await;
	make_file(&hostile, "inside").await;
	let (fs, report) = mount(&name, 3).await;
	assert_eq!(report.skipped, ["p00000009"], "{report:?}");
	assert_eq!(fs.read(p("/kept")).unwrap(), b"kept", "the files of the pool are there");
	refill_to(&fs, 6).await;
	let listed = list_names(&dir).await.unwrap();
	assert!(listed.contains(&"p0000000a".to_string()), "numbered above the entry: {listed:?}");
	close(&fs);
	drop(fs);

	// A lock that is a directory: no lock can be taken, so no pool is mounted, and the error says
	// so; the mount does not hang, and holds nothing.
	remove_entry(&dir, "LOCK").await;
	let lock = make_dir(&dir, "LOCK").await;
	make_file(&lock, "inside").await;
	let started = rt::Instant::now();
	let error = mount_with(&name, config(3)).await.err().expect("the lock is a directory");
	assert!(matches!(&error, Error::Io(e) if e.kind() == io::ErrorKind::InvalidInput), "{error:?}");
	assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());

	// Removing the database works all the same: the names of the pool are the pool's, whatever
	// they are.
	OpfsRaw::destroy(&name).await.expect("destroy a pool whose lock is a directory");
	assert!(OpfsRaw::open_existing(&name).await.unwrap().is_none());
}

async fn refill_to(fs: &Arc<Pool>, target: usize) {
	fs.refill(target).await.expect("a refill");
}

/// A slot that has the highest name there is: nothing numbered after it can be read back, so none
/// is made, and the file of a pool in that directory is still there at the next mount.
#[wasm_bindgen_test]
async fn a_slot_with_the_highest_name_makes_no_slot_that_the_next_mount_could_not_read() {
	let name = unique("highest");
	let raw = OpfsRaw::open(&name).await.unwrap();
	for slot in ["pffffffff", "p00000000"] {
		let file = raw.create(slot).await.unwrap();
		let page = Header::free(1).encode().unwrap();
		assert_eq!(file.write_at(0, &page).unwrap(), PAGE_LEN);
		assert_eq!(file.write_at(PAGE_LEN as u64, &page).unwrap(), PAGE_LEN);
		file.flush().unwrap();
		file.close().unwrap();
	}
	drop(raw);
	let (fs, report) = mount(&name, 0).await;
	assert_eq!(report.spares, 2, "{report:?}");
	put(&fs, "/important", b"a named file");
	put(&fs, "/another", b"and another");
	let error = fs.refill(4).await.expect_err("no name is left");
	assert!(error.to_string().contains("every slot name"), "{error}");
	let error = fs.create_new(p("/more"), CreateOpts::default()).err().expect("no slot");
	assert!(vfs::is_pool_exhausted(&error), "{error}");
	let mut listed = names(&name).await;
	listed.sort();
	assert_eq!(listed, ["p00000000", "pffffffff"], "nothing was made with a name past the end");
	close(&fs);
	drop(fs);
	let (fs, _) = mount(&name, 0).await;
	assert_eq!(fs.read(p("/important")).unwrap(), b"a named file");
	assert_eq!(fs.read(p("/another")).unwrap(), b"and another");
	close(&fs);
	drop(fs);
	destroy_and_verify(&name).await;
}

/// A slot that a handle of this worker still holds when the pool is mounted: the lock is the
/// pool's, so the handle is one that was closed and has not been released (the browser does that a
/// little after the fact, as the late handle does), and the mount waits for it until its deadline,
/// as it does for the lock.
#[wasm_bindgen_test]
async fn a_slot_that_is_still_held_is_tried_again_until_it_is_released() {
	let name = unique("held-slot");
	{
		let (fs, _) = mount(&name, 4).await;
		put(&fs, "/kept", b"kept");
		close(&fs);
	}
	let raw = OpfsRaw::open(&name).await.unwrap();
	let held = raw.open("p00000002").await.unwrap();
	drop(raw);
	rt::spawn(async move {
		rt::sleep(Duration::from_millis(200)).await;
		held.close().unwrap();
	});
	let cfg = PoolConfig {
		lock_deadline: Duration::from_secs(5),
		lock_poll: Duration::from_millis(25),
		..config(0)
	};
	let started = rt::Instant::now();
	let (fs, report) = mount_with(&name, cfg).await.expect("the mount waits for the slot");
	assert!(started.elapsed() >= Duration::from_millis(150), "{:?}", started.elapsed());
	assert_eq!(report.files, 1, "{report:?}");
	assert_eq!(fs.read(p("/kept")).unwrap(), b"kept");
	close(&fs);
	drop(fs);

	// One that stays held ends the mount at the deadline, with the lock error, and nothing is left
	// open.
	let raw = OpfsRaw::open(&name).await.unwrap();
	let held = raw.open("p00000002").await.unwrap();
	let cfg = PoolConfig {
		lock_deadline: Duration::from_millis(200),
		lock_poll: Duration::from_millis(25),
		..config(0)
	};
	let error = mount_with(&name, cfg).await.err().expect("a slot that is held for good");
	assert!(matches!(&error, Error::Io(e) if vfs::is_database_locked(e)), "{error:?}");
	held.close().unwrap();
	drop((held, raw));
	let (fs, _) = mount(&name, 0).await;
	close(&fs);
	drop(fs);
	destroy_and_verify(&name).await;
}

#[wasm_bindgen_test]
async fn whether_the_storage_is_persistent_can_be_asked_in_a_worker() {
	// The answer depends on the profile; what matters is that the question is answered, in a
	// worker, without a feature of the browser that a worker does not have.
	let persisted = storage_persisted().await.expect("persisted() works in a worker");
	console_log!("the storage of this origin is persistent: {persisted}");
}
