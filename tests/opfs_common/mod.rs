//! Helpers shared by the OPFS browser tests (`tests/opfs_*.rs`).
//!
//! Every target that needs them declares `mod opfs_common;`. The directory form
//! (`opfs_common/mod.rs`) keeps this file from being picked up as a test target of its own.
//!
//! The origin private file system persists inside the browser profile, so every test works on
//! names that no earlier test or run has used, and removes what it created through
//! [`remove_and_verify`], which also proves that the file was closed (an open file cannot be
//! removed).

#![allow(dead_code)]

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use surrealkv::storage::opfs::{get_opfs_root, open_opfs_sync_file, OpfsSyncFile};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::FileSystemDirectoryHandle;

/// How long any wait that could in principle never finish may take before the test fails with a
/// message instead of hanging the whole browser run.
pub const WATCHDOG_MS: f64 = 30_000.0;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A file name that no earlier test or run uses: `<label>-<time>-<counter>.<ext>`.
pub fn unique_name(label: &str, ext: &str) -> String {
	let n = COUNTER.fetch_add(1, Ordering::Relaxed);
	let run = js_sys::Date::now() as u64;
	format!("{label}-{run}-{n}.{ext}")
}

/// The OPFS root, failing the test with the browser's error if it cannot be obtained.
pub async fn root() -> FileSystemDirectoryHandle {
	get_opfs_root().await.unwrap_or_else(|e| panic!("get_opfs_root failed: {e:?}"))
}

/// Creates (or opens, keeping its contents) the file `name` and returns its sync handle.
pub async fn create(root: &FileSystemDirectoryHandle, name: &str) -> OpfsSyncFile {
	open_opfs_sync_file(root, name, true)
		.await
		.unwrap_or_else(|e| panic!("open_opfs_sync_file({name:?}, create = true) failed: {e:?}"))
}

/// Opens the existing file `name` with `create = false`.
pub async fn reopen(root: &FileSystemDirectoryHandle, name: &str) -> OpfsSyncFile {
	open_opfs_sync_file(root, name, false)
		.await
		.unwrap_or_else(|e| panic!("open_opfs_sync_file({name:?}, create = false) failed: {e:?}"))
}

/// Removes the file `name` and checks that it is gone.
///
/// The browser refuses to remove a file that still has an open sync access handle, so this
/// doubles as the proof that `close()` really released the file. The test must have closed
/// every handle on `name` before calling it.
pub async fn remove_and_verify(root: &FileSystemDirectoryHandle, name: &str) {
	JsFuture::from(root.remove_entry(name)).await.unwrap_or_else(|e| {
		panic!("removeEntry({name:?}) failed, a handle on it is probably still open: {e:?}")
	});
	let again = open_opfs_sync_file(root, name, false).await;
	assert!(again.is_err(), "{name:?} can still be opened after it was removed");
}

/// The text of an error, for asserting on the DOMException name the browser reports.
pub fn text<E: Debug>(e: &E) -> String {
	format!("{e:?}")
}

/// Unwraps the error of a result that must be an error, naming what was expected to fail.
pub fn expect_err<T: Debug, E: Debug>(r: Result<T, E>, what: &str) -> String {
	match r {
		Ok(v) => panic!("{what} succeeded but must fail, it returned {v:?}"),
		Err(e) => text(&e),
	}
}

/// Reads the whole file by reading until a read returns nothing, so the length it finds is the
/// real length of the file and does not come from `size()`. A read that never stops returning
/// data (an ignored offset, for example) fails the test instead of looping forever.
pub fn read_all(file: &OpfsSyncFile) -> Vec<u8> {
	const CAP: usize = 64 * 1024 * 1024;
	let mut out = Vec::new();
	let mut chunk = vec![0u8; 64 * 1024];
	loop {
		let n = file.read_at(out.len() as u64, &mut chunk).expect("read_at while reading all");
		if n == 0 {
			return out;
		}
		out.extend_from_slice(&chunk[..n]);
		assert!(out.len() <= CAP, "reading the file did not reach its end within {CAP} bytes");
	}
}

/// Reads exactly `len` bytes at `offset`, failing unless the full range was returned.
pub fn read_exact(file: &OpfsSyncFile, offset: u64, len: usize) -> Vec<u8> {
	let mut buf = vec![0u8; len];
	let n = file.read_at(offset, &mut buf).expect("read_at");
	assert_eq!(n, len, "short read of {len} bytes at offset {offset}");
	buf
}

/// Checks inside a spawned task. A panic there would abort the whole WebAssembly instance and
/// leave the test waiting for a task that can no longer finish (until the runner's timeout), so a
/// task reports a failed check as an `Err` that the test then turns into a panic of its own.
pub type Check = Result<(), String>;

/// `Err(what)` unless `cond` holds.
pub fn ensure(cond: bool, what: impl FnOnce() -> String) -> Check {
	if cond {
		Ok(())
	} else {
		Err(what())
	}
}

/// `Err` naming both values unless they are equal.
pub fn ensure_eq<T: PartialEq + Debug>(left: T, right: T, what: impl FnOnce() -> String) -> Check {
	if left == right {
		Ok(())
	} else {
		Err(format!("{}: left {left:?}, right {right:?}", what()))
	}
}

/// Like `ensure_eq` for byte strings: names the first difference instead of printing megabytes.
pub fn ensure_bytes(left: &[u8], right: &[u8], what: impl FnOnce() -> String) -> Check {
	if left == right {
		return Ok(());
	}
	let first = left.iter().zip(right).position(|(a, b)| a != b);
	Err(format!(
		"{}: {} bytes against {} bytes, first difference at {first:?}",
		what(),
		left.len(),
		right.len()
	))
}

/// Turns the error of a call into the message of a failed check: `call().map_err(failed("what"))?`.
pub fn failed<E: Debug>(what: &'static str) -> impl FnOnce(E) -> String {
	move |e| format!("{what} failed: {e:?}")
}

/// Reads exactly `len` bytes at `offset` or returns the reason it could not.
pub fn try_read_exact(file: &OpfsSyncFile, offset: u64, len: usize) -> Result<Vec<u8>, String> {
	let mut buf = vec![0u8; len];
	let n = file.read_at(offset, &mut buf).map_err(failed("read_at"))?;
	ensure_eq(n, len, || format!("bytes read at offset {offset}"))?;
	Ok(buf)
}

/// Waits for a task started with [`spawn`] that returns a check, failing the test with its message.
pub async fn finished<T>(what: &str, task: impl Future<Output = Result<T, String>>) -> T {
	task.await.unwrap_or_else(|e| panic!("{what}: {e}"))
}

/// A small xorshift64* generator: deterministic from its seed, no dependencies.
pub struct Rng(u64);

impl Rng {
	pub fn new(seed: u64) -> Self {
		Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
	}

	pub fn next_u64(&mut self) -> u64 {
		let mut x = self.0;
		x ^= x >> 12;
		x ^= x << 25;
		x ^= x >> 27;
		self.0 = x;
		x.wrapping_mul(0x2545_F491_4F6C_DD1D)
	}

	/// A value in `0..n`.
	pub fn below(&mut self, n: u64) -> u64 {
		self.next_u64() % n
	}

	pub fn fill(&mut self, buf: &mut [u8]) {
		for chunk in buf.chunks_mut(8) {
			let bytes = self.next_u64().to_le_bytes();
			chunk.copy_from_slice(&bytes[..chunk.len()]);
		}
	}
}

/// `len` pseudo-random bytes derived from `seed`. Random content (not a repeating byte) makes a
/// wrong offset, a wrong length or a mixed-up file show up as different bytes.
pub fn pattern(seed: u64, len: usize) -> Vec<u8> {
	let mut buf = vec![0u8; len];
	Rng::new(seed).fill(&mut buf);
	buf
}

/// A future that returns `Pending` once and wakes itself, so that other tasks run in between.
struct YieldNow(bool);

impl Future for YieldNow {
	type Output = ();

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
		if self.0 {
			Poll::Ready(())
		} else {
			self.0 = true;
			cx.waker().wake_by_ref();
			Poll::Pending
		}
	}
}

/// Lets every other ready task run once before continuing.
pub async fn yield_now() {
	YieldNow(false).await;
}

/// A promise that resolves after `ms` milliseconds, using the timer of whichever global scope
/// (window or worker) the test runs in.
fn timer(ms: f64) -> JsFuture {
	let promise = js_sys::Promise::new(&mut |resolve, _reject| {
		let global = js_sys::global();
		let set_timeout: js_sys::Function = js_sys::Reflect::get(&global, &"setTimeout".into())
			.expect("the global scope has no setTimeout")
			.unchecked_into();
		set_timeout.call2(&global, &resolve, &JsValue::from_f64(ms)).expect("setTimeout failed");
	});
	JsFuture::from(promise)
}

/// Awaits `fut`, failing the test with a message if it takes longer than [`WATCHDOG_MS`].
pub async fn watchdog<T>(what: &str, fut: impl Future<Output = T>) -> T {
	tokio::select! {
		biased;
		v = fut => v,
		_ = timer(WATCHDOG_MS) => panic!("watchdog: {what} did not finish within {WATCHDOG_MS} ms"),
	}
}

/// Runs `fut` as its own task on the browser's event loop and returns a future for its result,
/// so that several of them can make progress at once, interleaved at their `yield_now` points.
pub fn spawn<T: 'static>(fut: impl Future<Output = T> + 'static) -> impl Future<Output = T> {
	let (tx, rx) = tokio::sync::oneshot::channel();
	wasm_bindgen_futures::spawn_local(async move {
		let _ = tx.send(fut.await);
	});
	async move {
		watchdog("a spawned task", async {
			rx.await.expect("a spawned task ended without a result")
		})
		.await
	}
}
