//! The runtime shim: the one place that names the clock, the timers, the task spawner and the
//! blocking pool, so that the rest of the engine runs unchanged on a target that has none of them.
//!
//! On every target but `wasm32-unknown-unknown` (WASI included) each item is the item the engine
//! called before this module existed, re-exported or forwarded with `#[inline]`: the same function,
//! the same future, the same behaviour, and no work of its own.
//!
//! On `wasm32-unknown-unknown` (a browser, in a dedicated worker) there is one thread and no
//! tokio runtime, and these are the facts the second arm is built on:
//!
//! * `std::time::{Instant, SystemTime}` and `std::thread::{spawn, sleep}` panic, a panic aborts the
//!   instance (`panic = abort`), and so does anything that waits for another thread.
//! * `tokio::spawn` and `tokio::time::{sleep, timeout}` panic outside a runtime, and there is none.
//!   `tokio::sync::{Notify, Semaphore, oneshot}` and `tokio::select!` work under any executor.
//! * The one executor is `wasm_bindgen_futures::spawn_local`. A future that is backed by a
//!   JavaScript promise must never be driven by a blocking `block_on`, which traps; a tokio
//!   `LocalSet` is run with `run_until(..).await`.
//! * `affinitypool::spawn` runs inline when no pool is installed and never completes once an
//!   embedder installs one, so the shim never calls it there: [`run_blocking`] runs inline.
//! * A loop that yields through the microtask queue ([`yield_now`]) starves the event loop: the
//!   completion of an OPFS call, a timer or a message waits until the loop ends. [`relax`] yields
//!   through a macrotask, over one `MessageChannel` port. `setTimeout(0)` is not an option for it:
//!   browsers clamp a chain of them to a delay of several milliseconds each.
//!
//! `scripts/check-rt-guard.sh` fails if `tokio::spawn`, `tokio::time`, `std::time`'s `Instant`
//! or `SystemTime`, `affinitypool::spawn`, `std::thread::spawn` and `sleep` and the other calls
//! that panic or never complete in a browser are named anywhere else in `src`, outside test code
//! and code that is compiled out of a browser build.
#[cfg(all(target_arch = "wasm32", target_os = "unknown", target_feature = "atomics"))]
compile_error!(
	"the browser arm of the runtime shim assumes one thread: it makes the JavaScript-backed \
	 futures `Send` by hand. Do not build the engine for wasm32-unknown-unknown with the \
	 `atomics` target feature."
);

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub use browser::*;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) use native::*;

/// `Send` where there are threads to send to, and nothing where there are none. A bound of
/// generic code only: it cannot be put on a trait object.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) trait MaybeSend: Send {}
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl<T: Send + ?Sized> MaybeSend for T {}

/// `Send` where there are threads to send to, and nothing where there are none. A bound of
/// generic code only: it cannot be put on a trait object.
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
pub(crate) trait MaybeSend {}
#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
impl<T: ?Sized> MaybeSend for T {}

// ---------------------------------------------------------------------------------------------
// Every target but a browser
// ---------------------------------------------------------------------------------------------

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
mod native {
	use std::future::Future;
	/// The monotonic clock of the platform.
	pub(crate) use std::time::Instant;
	use std::time::{Duration, SystemTime, UNIX_EPOCH};

	/// Runs a closure on the worker pool the embedding application installed, and on the
	/// calling thread when it installed none.
	pub(crate) use affinitypool::spawn as run_blocking;
	/// Spawns a task on the current tokio runtime. Panics outside of one, as it always did.
	pub(crate) use tokio::spawn;
	/// The handle of a spawned task.
	pub(crate) use tokio::task::{yield_now, JoinHandle};
	/// The error of a `timeout` that elapsed.
	#[allow(
		unused_imports,
		reason = "the surface of the shim: a caller of `timeout` names it"
	)]
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

	/// Waits for a pacing delay, which is a few hundred microseconds at most.
	///
	/// The wait happens on a thread of the blocking pool and not on a tokio timer, so that a
	/// runtime that was built without the time driver, which is what `tokio::runtime::Builder`
	/// gives unless `enable_time` or `enable_all` is called, does not panic in the commit path.
	#[cfg(not(target_arch = "wasm32"))]
	pub(crate) async fn pace(delay: Duration) {
		let _ = tokio::task::spawn_blocking(move || std::thread::sleep(delay)).await;
	}

	/// There are no threads to block on WASI, so the delay is a timer there, as it was.
	#[cfg(target_arch = "wasm32")]
	pub(crate) async fn pace(delay: Duration) {
		sleep(delay).await;
	}
}

// ---------------------------------------------------------------------------------------------
// A browser: wasm32-unknown-unknown, one dedicated worker
// ---------------------------------------------------------------------------------------------

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod browser {
	use std::cell::{Cell, RefCell};
	use std::collections::VecDeque;
	use std::fmt;
	use std::future::Future;
	use std::ops::{Add, Sub};
	use std::pin::Pin;
	use std::rc::Rc;
	use std::sync::atomic::{AtomicBool, Ordering};
	use std::sync::Arc;
	use std::task::{Context, Poll, Waker};
	use std::time::Duration;

	use js_sys::{Function, Promise, Reflect};
	use tokio::sync::{oneshot, Notify};
	use wasm_bindgen::prelude::*;
	use wasm_bindgen::JsCast;
	use wasm_bindgen_futures::JsFuture;

	use super::MaybeSend;

	#[wasm_bindgen]
	extern "C" {
		#[wasm_bindgen(js_namespace = performance, js_name = now)]
		fn performance_now() -> f64;
		#[wasm_bindgen(js_name = setTimeout)]
		fn set_timeout(handler: &Function, milliseconds: i32) -> JsValue;
		#[wasm_bindgen(js_name = clearTimeout)]
		fn clear_timeout(handle: &JsValue);
	}

	// -----------------------------------------------------------------------------------------
	// Time
	// -----------------------------------------------------------------------------------------

	/// A point on the monotonic clock of the worker (`performance.now()`), in nanoseconds: opaque,
	/// `Copy`, ordered, and the same operations as `std::time::Instant`, which panics here.
	///
	/// Public, as `std::time::Instant` is, because it is the type of the public field
	/// `BackgroundError::timestamp`.
	#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
	pub struct Instant(u64);

	impl Instant {
		/// The current instant.
		pub fn now() -> Self {
			Self((performance_now() * 1_000_000.0) as u64)
		}

		/// The time since this instant.
		pub fn elapsed(self) -> Duration {
			Self::now().duration_since(self)
		}

		/// The time from `earlier` to this instant, which is zero if `earlier` is later.
		pub fn duration_since(self, earlier: Self) -> Duration {
			self.saturating_duration_since(earlier)
		}

		/// The time from `earlier` to this instant, or nothing if `earlier` is later.
		pub fn checked_duration_since(self, earlier: Self) -> Option<Duration> {
			self.0.checked_sub(earlier.0).map(Duration::from_nanos)
		}

		/// The time from `earlier` to this instant, which is zero if `earlier` is later.
		pub fn saturating_duration_since(self, earlier: Self) -> Duration {
			self.checked_duration_since(earlier).unwrap_or_default()
		}

		/// This instant moved on by `duration`, or nothing if that overflows.
		pub fn checked_add(self, duration: Duration) -> Option<Self> {
			let nanos = u64::try_from(duration.as_nanos()).ok()?;
			self.0.checked_add(nanos).map(Self)
		}

		/// This instant moved back by `duration`, or nothing if that goes before the clock began.
		pub fn checked_sub(self, duration: Duration) -> Option<Self> {
			let nanos = u64::try_from(duration.as_nanos()).ok()?;
			self.0.checked_sub(nanos).map(Self)
		}
	}

	impl Add<Duration> for Instant {
		type Output = Self;

		fn add(self, duration: Duration) -> Self {
			self.checked_add(duration).expect("an instant overflowed")
		}
	}

	impl Sub<Duration> for Instant {
		type Output = Self;

		fn sub(self, duration: Duration) -> Self {
			self.checked_sub(duration).expect("an instant went before the clock began")
		}
	}

	impl Sub for Instant {
		type Output = Duration;

		fn sub(self, earlier: Self) -> Duration {
			self.duration_since(earlier)
		}
	}

	/// The wall clock, as nanoseconds since the Unix epoch, to the millisecond (`Date.now()`).
	pub(crate) fn now_unix_ns() -> u64 {
		(js_sys::Date::now() as u64).saturating_mul(1_000_000)
	}

	// -----------------------------------------------------------------------------------------
	// Timers
	// -----------------------------------------------------------------------------------------

	/// The error of a [`timeout`] that elapsed.
	#[derive(Clone, Copy, Debug, PartialEq, Eq)]
	pub struct Elapsed(());

	impl fmt::Display for Elapsed {
		fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
			f.write_str("deadline has elapsed")
		}
	}

	impl std::error::Error for Elapsed {}

	/// A `setTimeout` as a future: ready when the timer fires, and the timer is cleared if the
	/// future is dropped first.
	struct Timer {
		fired: JsFuture,
		handle: JsValue,
		/// Resolves the promise behind `fired`. A cleared timer never would, and the callbacks
		/// that `JsFuture` hangs on a promise are freed only when it settles: about 90 bytes of
		/// linear memory, which never shrinks, for every timer that lost a race or was dropped.
		resolve: Function,
	}

	// SAFETY: the engine is built for one thread on this target (`compile_error!` above refuses
	// the `atomics` target feature), so a JavaScript value is never used from a second one.
	unsafe impl Send for Timer {}

	impl Timer {
		fn new(milliseconds: i32) -> Self {
			let mut handle = JsValue::UNDEFINED;
			let mut settle = None;
			let promise = Promise::new(&mut |resolve, _reject| {
				handle = set_timeout(&resolve, milliseconds);
				settle = Some(resolve);
			});
			Self {
				fired: JsFuture::from(promise),
				handle,
				resolve: settle.expect("the promise executor runs at once"),
			}
		}
	}

	impl Future for Timer {
		type Output = ();

		fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
			Pin::new(&mut self.fired).poll(cx).map(|_| ())
		}
	}

	impl Drop for Timer {
		fn drop(&mut self) {
			clear_timeout(&self.handle);
			// Settle the promise (a no-op if the timer fired), so that its callbacks are freed.
			let _ = self.resolve.call0(&JsValue::UNDEFINED);
		}
	}

	/// Waits for `duration` on a real timer. The shortest wait is one macrotask, so that a zero
	/// duration also lets the event loop run.
	pub(crate) async fn sleep(duration: Duration) {
		// `setTimeout` takes a 32-bit signed count of milliseconds and fires at once for more, so
		// a longer wait is made of several.
		const LONGEST: u128 = i32::MAX as u128;
		// Rounded up: a timer must not fire before its time.
		let mut remaining = duration.as_nanos().div_ceil(1_000_000);
		loop {
			let chunk = remaining.min(LONGEST);
			Timer::new(chunk as i32).await;
			remaining -= chunk;
			if remaining == 0 {
				break;
			}
		}
	}

	/// Runs `future` for at most `duration`. A future that is ready when it is first polled wins
	/// over the timer, as it does under tokio.
	pub(crate) async fn timeout<F: Future>(
		duration: Duration,
		future: F,
	) -> Result<F::Output, Elapsed> {
		tokio::select! {
			biased;
			output = future => Ok(output),
			() = sleep(duration) => Err(Elapsed(())),
		}
	}

	// -----------------------------------------------------------------------------------------
	// Yielding
	// -----------------------------------------------------------------------------------------

	/// A future that is pending once and wakes itself, so that every other ready task runs once
	/// before it continues. It goes through the microtask queue only, so it lets other tasks of
	/// this executor run but not the event loop: see [`relax`].
	pub(crate) struct YieldNow(bool);

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

	/// Lets the other ready tasks of the executor run, but not the event loop.
	pub(crate) fn yield_now() -> YieldNow {
		YieldNow(false)
	}

	/// A task that waits for the macrotask that [`relax`] asked for.
	struct Waiting {
		done: Cell<bool>,
		waker: RefCell<Option<Waker>>,
	}

	impl Waiting {
		fn finish(&self) {
			self.done.set(true);
			if let Some(waker) = self.waker.borrow_mut().take() {
				waker.wake();
			}
		}
	}

	/// The one `MessageChannel` that every [`relax`] posts to. Each message releases the oldest
	/// task that is waiting, so `n` calls post `n` messages and release `n` tasks, in order.
	struct Relaxer {
		port: JsValue,
		post_message: Function,
		waiting: Rc<RefCell<VecDeque<Rc<Waiting>>>>,
		// Kept alive for as long as the port can deliver to it: it is never dropped.
		_on_message: Closure<dyn FnMut()>,
	}

	impl Relaxer {
		/// Opens the channel, or says that the global scope has no `MessageChannel`.
		fn open() -> Option<Self> {
			let global = js_sys::global();
			let constructor: Function = Reflect::get(&global, &JsValue::from_str("MessageChannel"))
				.ok()?
				.dyn_into()
				.ok()?;
			let channel = Reflect::construct(&constructor, &js_sys::Array::new()).ok()?;
			let receiver = Reflect::get(&channel, &JsValue::from_str("port1")).ok()?;
			let port = Reflect::get(&channel, &JsValue::from_str("port2")).ok()?;
			let post_message: Function =
				Reflect::get(&port, &JsValue::from_str("postMessage")).ok()?.dyn_into().ok()?;

			let waiting: Rc<RefCell<VecDeque<Rc<Waiting>>>> = Rc::default();
			let queue = Rc::clone(&waiting);
			let on_message = Closure::<dyn FnMut()>::new(move || {
				// Popped before it is woken: a woken task may call `relax` again.
				let next = queue.borrow_mut().pop_front();
				if let Some(next) = next {
					next.finish();
				}
			});
			// Setting `onmessage` starts the port.
			Reflect::set(&receiver, &JsValue::from_str("onmessage"), on_message.as_ref()).ok()?;
			// Node keeps its event loop alive for a port that listens, and a test runner there
			// would never exit. A browser has no such method.
			if let Ok(unref) = Reflect::get(&receiver, &JsValue::from_str("unref")) {
				if let Some(unref) = unref.dyn_ref::<Function>() {
					let _ = unref.call0(&receiver);
				}
			}
			Some(Self {
				port,
				post_message,
				waiting,
				_on_message: on_message,
			})
		}

		fn post(&self, waiting: Rc<Waiting>) {
			self.waiting.borrow_mut().push_back(waiting);
			let _ = self.post_message.call1(&self.port, &JsValue::from_f64(0.0));
		}
	}

	thread_local! {
		static RELAXER: Option<Relaxer> = Relaxer::open();
	}

	/// Where there is no `MessageChannel`: a timer, which is slow to chain but never traps.
	fn relax_with_a_timer(waiting: Rc<Waiting>) {
		let on_timer = Closure::once_into_js(move || waiting.finish());
		set_timeout(on_timer.unchecked_ref(), 0);
	}

	/// The future of [`relax`].
	pub(crate) struct Relax {
		waiting: Rc<Waiting>,
		posted: bool,
	}

	// SAFETY: as for `Timer`: one thread.
	unsafe impl Send for Relax {}

	impl Future for Relax {
		type Output = ();

		fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
			if self.waiting.done.get() {
				return Poll::Ready(());
			}
			*self.waiting.waker.borrow_mut() = Some(cx.waker().clone());
			if !self.posted {
				self.posted = true;
				let waiting = Rc::clone(&self.waiting);
				let timer_fallback = RELAXER.with(|relaxer| match relaxer {
					Some(relaxer) => {
						relaxer.post(Rc::clone(&waiting));
						false
					}
					None => true,
				});
				if timer_fallback {
					relax_with_a_timer(waiting);
				}
			}
			Poll::Pending
		}
	}

	/// Lets the event loop run: the completions of OPFS calls, timers and messages that are
	/// waiting are delivered before this returns, which a loop that yields with [`yield_now`]
	/// alone never lets happen. A macrotask yield over one persistent `MessageChannel` port, which
	/// is cheap enough to call in a loop.
	pub(crate) fn relax() -> Relax {
		Relax {
			waiting: Rc::new(Waiting {
				done: Cell::new(false),
				waker: RefCell::new(None),
			}),
			posted: false,
		}
	}

	/// Waits for a pacing delay: there is nothing to block, and a write that is paced is one that
	/// should let the background tasks run, which a yield does.
	pub(crate) async fn pace(_delay: Duration) {
		yield_now().await;
	}

	// -----------------------------------------------------------------------------------------
	// Tasks
	// -----------------------------------------------------------------------------------------

	/// What `abort` tells a task: set once, and a permit that `notified` finds even if the task has
	/// not been polled yet.
	struct AbortState {
		aborted: AtomicBool,
		finished: AtomicBool,
		notify: Notify,
	}

	/// The error of a task that did not return a value.
	#[derive(Debug)]
	pub struct JoinError {
		#[allow(dead_code, reason = "read by `is_cancelled`, which the tokio arm has as well")]
		cancelled: bool,
	}

	impl JoinError {
		/// Whether the task was aborted, or was dropped before it finished.
		#[allow(dead_code, reason = "the surface of a task handle: the tokio arm has it")]
		pub fn is_cancelled(&self) -> bool {
			self.cancelled
		}
	}

	impl fmt::Display for JoinError {
		fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
			f.write_str("task was cancelled")
		}
	}

	impl std::error::Error for JoinError {}

	/// The handle of a task spawned with [`spawn`]: a future of its output. Dropping it detaches
	/// the task, which keeps running.
	pub(crate) struct JoinHandle<T> {
		result: oneshot::Receiver<Result<T, JoinError>>,
		state: Arc<AbortState>,
	}

	impl<T> JoinHandle<T> {
		/// Ends the task at its next poll, or before its first one, and drops its future. Awaiting
		/// the handle then gives a cancelled `JoinError`, unless the task had already finished.
		pub(crate) fn abort(&self) {
			self.state.aborted.store(true, Ordering::SeqCst);
			self.state.notify.notify_one();
		}

		/// Whether the task has ended, by finishing or by being aborted.
		#[allow(dead_code, reason = "the surface of a task handle: the tokio arm has it")]
		pub(crate) fn is_finished(&self) -> bool {
			self.state.finished.load(Ordering::SeqCst)
		}
	}

	impl<T> Future for JoinHandle<T> {
		type Output = Result<T, JoinError>;

		fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
			match Pin::new(&mut self.result).poll(cx) {
				Poll::Ready(Ok(result)) => Poll::Ready(result),
				// The task was dropped without sending: its executor went away.
				Poll::Ready(Err(_)) => Poll::Ready(Err(JoinError {
					cancelled: true,
				})),
				Poll::Pending => Poll::Pending,
			}
		}
	}

	/// Runs `future` as a task on the executor of the page: `spawn_local`, callable from
	/// synchronous code. It is first polled from the microtask queue, after the caller yields.
	pub(crate) fn spawn<F, T>(future: F) -> JoinHandle<T>
	where
		F: Future<Output = T> + 'static,
		T: 'static,
	{
		let (sender, result) = oneshot::channel();
		let state = Arc::new(AbortState {
			aborted: AtomicBool::new(false),
			finished: AtomicBool::new(false),
			notify: Notify::new(),
		});
		let task_state = Arc::clone(&state);
		wasm_bindgen_futures::spawn_local(async move {
			let outcome = tokio::select! {
				biased;
				() = task_state.notify.notified() => Err(JoinError { cancelled: true }),
				output = future => Ok(output),
			};
			task_state.finished.store(true, Ordering::SeqCst);
			// A dropped handle is a detached task: nobody is left to tell.
			let _ = sender.send(outcome);
		});
		JoinHandle {
			result,
			state,
		}
	}

	/// Spawns `task`, and says whether it could: always, since there is no runtime to be missing.
	pub(crate) fn try_spawn<F>(task: F) -> bool
	where
		F: Future<Output = ()> + MaybeSend + 'static,
	{
		drop(spawn(task));
		true
	}

	/// Runs `blocking` at once, on the only thread there is. Never `affinitypool`, which never
	/// completes on this target once an embedder installs a pool.
	pub(crate) async fn run_blocking<F, R>(blocking: F) -> R
	where
		F: FnOnce() -> R + MaybeSend + 'static,
		R: MaybeSend + 'static,
	{
		blocking()
	}
}

// ---------------------------------------------------------------------------------------------
// Tests: the same ones run natively under tokio and in a dedicated worker of Chrome. What only a
// browser can show (a pending OPFS call that `relax` lets through and `yield_now` does not) is in
// `crate::wasm_tests`.
// ---------------------------------------------------------------------------------------------

#[cfg(test)]
mod tests {
	use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
	use std::sync::{Arc, Mutex};
	use std::time::Duration;

	#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
	use wasm_bindgen_test::wasm_bindgen_test;

	use super::*;

	macro_rules! rt_test {
		($(async fn $name:ident() $body:block)*) => {
			$(
				#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), tokio::test)]
				#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
				async fn $name() $body
			)*
		};
	}

	macro_rules! rt_sync_test {
		($(fn $name:ident() $body:block)*) => {
			$(
				#[cfg_attr(not(all(target_arch = "wasm32", target_os = "unknown")), test)]
				#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), wasm_bindgen_test)]
				fn $name() $body
			)*
		};
	}

	/// How far a timer may fire before its time on a clock that is not the timer's own: browsers
	/// round timers and `performance.now()` independently.
	const TIMER_SLACK: Duration = Duration::from_millis(2);

	rt_test! {
		async fn spawn_runs_the_task_and_its_handle_resolves() {
			let ran = Arc::new(AtomicBool::new(false));
			let flag = Arc::clone(&ran);
			let handle: JoinHandle<()> = spawn(async move {
				flag.store(true, Ordering::SeqCst);
			});
			handle.await.expect("the task completes");
			assert!(ran.load(Ordering::SeqCst));
		}

		async fn a_task_hands_back_its_value() {
			let handle = spawn(async { 6 * 7 });
			assert_eq!(handle.await.expect("the task completes"), 42);
		}

		async fn a_task_that_is_aborted_before_its_first_poll_never_runs() {
			let ran = Arc::new(AtomicBool::new(false));
			let flag = Arc::clone(&ran);
			let handle = spawn(async move {
				flag.store(true, Ordering::SeqCst);
			});
			handle.abort();
			let error = handle.await.expect_err("an aborted task does not complete");
			assert!(error.is_cancelled());
			assert!(!ran.load(Ordering::SeqCst), "the task body ran after the abort");
		}

		async fn a_task_that_waits_is_ended_by_abort() {
			let dropped = Arc::new(AtomicBool::new(false));
			struct SetOnDrop(Arc<AtomicBool>);
			impl Drop for SetOnDrop {
				fn drop(&mut self) {
					self.0.store(true, Ordering::SeqCst);
				}
			}
			let guard = SetOnDrop(Arc::clone(&dropped));
			let handle = spawn(async move {
				let _guard = guard;
				sleep(Duration::from_secs(3600)).await;
			});
			// Let the task run to its first wait.
			yield_now().await;
			yield_now().await;
			assert!(!handle.is_finished());
			handle.abort();
			let error = handle.await.expect_err("an aborted task does not complete");
			assert!(error.is_cancelled());
			assert!(dropped.load(Ordering::SeqCst), "the future of the task was not dropped");
		}

		async fn aborting_a_task_that_has_finished_keeps_its_value() {
			let handle = spawn(async { 7 });
			while !handle.is_finished() {
				yield_now().await;
			}
			handle.abort();
			assert_eq!(handle.await.expect("the task had finished"), 7);
		}

		async fn a_dropped_handle_detaches_the_task() {
			let ran = Arc::new(AtomicBool::new(false));
			let flag = Arc::clone(&ran);
			drop(spawn(async move {
				flag.store(true, Ordering::SeqCst);
			}));
			while !ran.load(Ordering::SeqCst) {
				yield_now().await;
			}
		}

		async fn sleep_waits_for_the_duration() {
			let started = Instant::now();
			sleep(Duration::from_millis(30)).await;
			assert!(started.elapsed() + TIMER_SLACK >= Duration::from_millis(30));
			assert!(started.elapsed() < Duration::from_secs(10));
			// Zero is one turn of the event loop and no more.
			sleep(Duration::ZERO).await;
		}

		async fn timeout_returns_the_output_of_a_future_that_is_in_time() {
			let finished = timeout(Duration::from_secs(60), async { 7 }).await;
			assert_eq!(finished.ok(), Some(7));
			let slow = timeout(Duration::from_secs(60), async {
				sleep(Duration::from_millis(5)).await;
				8
			})
			.await;
			assert_eq!(slow.ok(), Some(8));
		}

		async fn timeout_elapses_for_a_future_that_never_finishes() {
			let started = Instant::now();
			let result: Result<(), Elapsed> =
				timeout(Duration::from_millis(25), std::future::pending::<()>()).await;
			assert!(result.is_err(), "a future that never finishes times out");
			assert!(started.elapsed() + TIMER_SLACK >= Duration::from_millis(25));
			assert!(started.elapsed() < Duration::from_secs(10));
		}

		async fn a_future_that_is_ready_wins_over_a_timeout_of_zero() {
			let result = timeout(Duration::ZERO, async { 1 }).await;
			assert_eq!(result.ok(), Some(1));
		}

		async fn yield_now_lets_the_other_ready_tasks_run() {
			let order = Arc::new(Mutex::new(Vec::new()));
			let other = Arc::clone(&order);
			let handle = spawn(async move {
				other.lock().unwrap().push("other");
			});
			order.lock().unwrap().push("before");
			for _ in 0..3 {
				yield_now().await;
			}
			order.lock().unwrap().push("after");
			handle.await.unwrap();
			assert_eq!(*order.lock().unwrap(), ["before", "other", "after"]);
		}

		async fn relax_returns_and_lets_the_other_ready_tasks_run() {
			let ran = Arc::new(AtomicUsize::new(0));
			let counter = Arc::clone(&ran);
			let handle = spawn(async move {
				counter.fetch_add(1, Ordering::SeqCst);
			});
			// A native `relax` is ready at once and gives way to nothing; a browser's lets every
			// ready task run. A yield after it makes the two agree.
			relax().await;
			yield_now().await;
			assert_eq!(ran.load(Ordering::SeqCst), 1);
			handle.await.unwrap();
			// Several in a row, and several waiting at once.
			for _ in 0..50 {
				relax().await;
			}
			let waiting: Vec<_> = (0..8).map(|_| spawn(async { relax().await })).collect();
			for handle in waiting {
				handle.await.unwrap();
			}
		}

		async fn run_blocking_runs_the_closure_once_and_returns_its_value() {
			let calls = Arc::new(AtomicUsize::new(0));
			let counter = Arc::clone(&calls);
			let value = run_blocking(move || {
				counter.fetch_add(1, Ordering::SeqCst);
				40 + 2
			})
			.await;
			assert_eq!(value, 42);
			assert_eq!(calls.load(Ordering::SeqCst), 1);
		}

		async fn try_spawn_spawns_inside_a_runtime() {
			let ran = Arc::new(AtomicBool::new(false));
			let flag = Arc::clone(&ran);
			assert!(try_spawn(async move {
				flag.store(true, Ordering::SeqCst);
			}));
			while !ran.load(Ordering::SeqCst) {
				yield_now().await;
			}
		}

		async fn the_futures_and_handles_of_the_shim_are_send() {
			// The engine puts them in `Send` futures (`BoxFuture`, `tokio::spawn`), and on a browser
			// they are made of JavaScript values that are not `Send` unless the shim says so.
			fn assert_send<T: Send>(_: T) {}
			assert_send(sleep(Duration::from_millis(1)));
			assert_send(timeout(Duration::from_millis(1), async {}));
			assert_send(yield_now());
			assert_send(relax());
			assert_send(pace(Duration::from_micros(1)));
			assert_send(run_blocking(|| 1));
			assert_send(Instant::now());
			let handle: Option<JoinHandle<u32>> = None;
			assert_send(handle);
		}

		async fn pace_returns() {
			let started = Instant::now();
			pace(Duration::from_micros(300)).await;
			assert!(started.elapsed() < Duration::from_secs(10));
		}
	}

	rt_sync_test! {
		fn instants_are_monotonic_and_do_arithmetic() {
			let first = Instant::now();
			let second = Instant::now();
			assert!(second >= first);
			assert!(second.duration_since(first) < Duration::from_secs(60));

			let later = first + Duration::from_secs(1);
			assert!(later > first);
			assert_eq!(later - first, Duration::from_secs(1));
			assert_eq!(later.duration_since(first), Duration::from_secs(1));
			assert_eq!(later - Duration::from_secs(1), first);
			assert_eq!(later.checked_duration_since(first), Some(Duration::from_secs(1)));
			assert_eq!(first.checked_duration_since(later), None);
			// An earlier instant minus a later one is zero, as with `std`'s since 1.60.
			assert_eq!(first - later, Duration::ZERO);
			assert_eq!(first.saturating_duration_since(later), Duration::ZERO);
		}

		fn elapsed_does_not_go_backwards() {
			let started = Instant::now();
			let mut last = Duration::ZERO;
			for _ in 0..1000 {
				let elapsed = started.elapsed();
				assert!(elapsed >= last);
				last = elapsed;
			}
		}

		fn instants_are_copy_ord_debug_and_hash() {
			fn takes<T: Copy + Ord + std::fmt::Debug + std::hash::Hash>(_: T) {}
			takes(Instant::now());
			assert!(!format!("{:?}", Instant::now()).is_empty());
		}

		fn the_unix_clock_is_plausible() {
			// After 2020-01-01 and before 2100-01-01, in nanoseconds.
			let now = now_unix_ns();
			assert!(now > 1_577_836_800_000_000_000, "{now}");
			assert!(now < 4_102_444_800_000_000_000, "{now}");
			assert!(now_unix_ns() + 1_000_000_000 >= now);
		}
	}

	/// What only a native target can ask: no runtime, and no timer driver.
	#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
	mod native {
		use super::*;

		#[test]
		fn try_spawn_says_there_is_no_runtime() {
			assert!(!try_spawn(async {}), "no runtime on a plain test thread");
		}

		#[test]
		fn pace_does_not_need_the_time_driver_of_the_runtime() {
			// `Builder` without `enable_time`: a tokio timer would panic.
			let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
			runtime.block_on(async {
				let started = Instant::now();
				pace(Duration::from_micros(300)).await;
				assert!(started.elapsed() >= Duration::from_micros(300));
			});
		}

		#[test]
		fn the_unix_clock_matches_the_system_clock() {
			let expected = std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos() as u64;
			let actual = now_unix_ns();
			assert!(actual >= expected && actual - expected < 1_000_000_000);
		}
	}
}
