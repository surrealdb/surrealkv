//! The runtime shim: the one place that names the clock, the timers, the task spawner and the
//! blocking pool, so that the rest of the engine runs unchanged on a target that has none of them.
//!
//! On every target but `wasm32-unknown-unknown` (WASI included) each item is the item the engine
//! called before this module existed, re-exported or forwarded with `#[inline]`: the same function,
//! the same future, the same behaviour, and no work of its own.
//!
//! `scripts/check-rt-guard.sh` fails the build if `tokio::spawn`, `tokio::time`, `std::time`'s
//! `Instant` or `SystemTime`, `affinitypool::spawn`, `std::thread::spawn` and `sleep` and the other
//! calls that panic or never complete in a browser are named anywhere else in `src`, outside test
//! code and code that is compiled out of a browser build.
//!
//! Nothing outside this module calls the shim yet in this commit: the call sites move over next.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) use native::*;

/// `Send` where there are threads to send to, and nothing where there are none. A bound of
/// generic code only: it cannot be put on a trait object.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) trait MaybeSend: Send {}
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl<T: Send + ?Sized> MaybeSend for T {}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
mod native {
	use std::future::Future;
	/// The monotonic clock of the platform.
	pub(crate) use std::time::Instant;
	use std::time::{SystemTime, UNIX_EPOCH};

	/// Runs a closure on the worker pool the embedding application installed, and on the
	/// calling thread when it installed none.
	pub(crate) use affinitypool::spawn as run_blocking;
	/// Spawns a task on the current tokio runtime. Panics outside of one, as it always did.
	pub(crate) use tokio::spawn;
	/// The handle of a spawned task.
	pub(crate) use tokio::task::{yield_now, JoinHandle};
	/// The error of a `timeout` that elapsed.
	pub(crate) use tokio::time::error::Elapsed;
	pub(crate) use tokio::time::{sleep, timeout};

	use super::MaybeSend;

	/// Spawns `task` on the tokio runtime the calling thread is in, and says whether there was
	/// one. For `Drop`, which must not panic outside of a runtime.
	#[inline]
	pub(crate) fn try_spawn<F>(task: F) -> bool
	where
		F: Future<Output = ()> + MaybeSend + 'static,
	{
		match tokio::runtime::Handle::try_current() {
			Ok(handle) => {
				handle.spawn(task);
				true
			}
			Err(_) => false,
		}
	}

	/// The wall clock, as nanoseconds since the Unix epoch. A clock set before the epoch reads 0.
	#[inline]
	pub(crate) fn now_unix_ns() -> u64 {
		SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64
	}

	/// Lets the host's event loop run. A thread of its own has nothing to give way to, so this is
	/// ready at once.
	#[inline(always)]
	pub(crate) fn relax() -> std::future::Ready<()> {
		std::future::ready(())
	}
}

#[cfg(all(test, not(all(target_arch = "wasm32", target_os = "unknown"))))]
mod tests {
	use std::sync::atomic::{AtomicBool, Ordering};
	use std::sync::Arc;
	use std::time::Duration;

	use super::*;

	#[tokio::test]
	async fn spawn_runs_the_task_and_its_handle_resolves() {
		let ran = Arc::new(AtomicBool::new(false));
		let flag = Arc::clone(&ran);
		let handle: JoinHandle<()> = spawn(async move {
			flag.store(true, Ordering::SeqCst);
		});
		handle.await.expect("the task completes");
		assert!(ran.load(Ordering::SeqCst));
	}

	#[tokio::test]
	async fn an_aborted_task_resolves_its_handle_with_an_error() {
		let handle = spawn(async {
			sleep(Duration::from_secs(3600)).await;
		});
		handle.abort();
		let error = handle.await.expect_err("an aborted task does not complete");
		assert!(error.is_cancelled());
	}

	#[tokio::test]
	async fn sleep_and_timeout_use_the_runtime_timers() {
		let started = Instant::now();
		sleep(Duration::from_millis(30)).await;
		assert!(started.elapsed() >= Duration::from_millis(30));

		let finished = timeout(Duration::from_secs(5), async { 7 }).await;
		assert_eq!(finished.ok(), Some(7));

		let elapsed: Result<(), Elapsed> =
			timeout(Duration::from_millis(10), std::future::pending::<()>()).await;
		assert!(elapsed.is_err(), "a future that never finishes times out");
	}

	#[tokio::test]
	async fn yield_now_and_relax_return() {
		yield_now().await;
		relax().await;
	}

	#[tokio::test]
	async fn run_blocking_runs_the_closure_and_returns_its_value() {
		assert_eq!(run_blocking(|| 40 + 2).await, 42);
	}

	#[test]
	fn instants_are_monotonic() {
		let first = Instant::now();
		let second = Instant::now();
		assert!(second >= first);
		assert!(second.duration_since(first) < Duration::from_secs(60));
		assert!(first + Duration::from_secs(1) > first);
	}

	#[test]
	fn the_unix_clock_is_plausible_and_does_not_go_back() {
		// After 2020-01-01 and before 2100-01-01, in nanoseconds.
		let now = now_unix_ns();
		assert!(now > 1_577_836_800_000_000_000, "{now}");
		assert!(now < 4_102_444_800_000_000_000, "{now}");
		assert!(now_unix_ns() >= now - 1_000_000_000);
	}

	#[test]
	fn try_spawn_says_whether_there_is_a_runtime() {
		assert!(!try_spawn(async {}), "no runtime on a plain test thread");

		let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
		let ran = Arc::new(AtomicBool::new(false));
		let flag = Arc::clone(&ran);
		runtime.block_on(async move {
			assert!(try_spawn(async move {
				flag.store(true, Ordering::SeqCst);
			}));
			yield_now().await;
		});
		assert!(ran.load(Ordering::SeqCst));
	}
}
