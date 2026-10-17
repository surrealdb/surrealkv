use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;

use crate::compaction::leveled::Strategy;
use crate::compaction::CompactionStrategy;
use crate::error::{BackgroundErrorHandler, BackgroundErrorReason, ErrorSeverity};
use crate::lsm::CompactionOperations;
use crate::stall::WriteStallController;
use crate::vfs::{FsKind, SpareWait};
use crate::{rt, Error, Options};

/// The wait before a failed memtable flush is run again; it doubles with every failure. The
/// commit pipeline spaces out the replacements of a failed WAL segment the same way.
pub(crate) const FLUSH_RETRY_MIN: Duration = Duration::from_millis(50);
/// The longest wait between two attempts at a memtable flush.
pub(crate) const FLUSH_RETRY_MAX: Duration = Duration::from_secs(2);

/// The wait before a failed level compaction is run again; it doubles with every failure.
pub(crate) const COMPACTION_RETRY_MIN: Duration = Duration::from_millis(50);
/// The longest wait between two attempts at a level compaction.
pub(crate) const COMPACTION_RETRY_MAX: Duration = Duration::from_secs(2);

/// Manages background tasks for the LSM tree
pub(crate) struct TaskManager {
	/// Flag to signal tasks to stop
	stop_flag: Arc<AtomicBool>,

	/// Notification for memtable compaction task
	memtable_notify: Arc<Notify>,

	/// Notification for level compaction task
	level_notify: Arc<Notify>,

	/// Flag indicating if memtable compaction is running
	memtable_running: Arc<AtomicBool>,

	/// Flag indicating if level compaction is running
	level_running: Arc<AtomicBool>,

	/// Task handles for cleanup
	task_handles: Mutex<Option<Vec<rt::JoinHandle<()>>>>,

	/// Told when the tasks are to stop, so that one that waits for the spare files of a pool is
	/// not waited for: `stop` would otherwise wait for a refill that may never come. Only a pool
	/// has such a wait, and a database on `std` has none of this: no allocation at open and no
	/// lock at close.
	stop_notify: Option<Arc<Notify>>,
}

/// How a wait for the spare files of a pool ended, for a task.
enum Spares {
	/// The files are there.
	Ready,
	/// The task was told to stop while it waited.
	Stopped,
	/// The pool could not make them: it is closed, at its limit, or its refill failed for good.
	Failed(Error),
}

/// Waits for `wait`, unless the tasks are told to stop first. Registered for the stop signal
/// before the flag is read, so a stop that comes between the two is not missed.
async fn await_spares(
	wait: SpareWait,
	stop_flag: &AtomicBool,
	stop_notify: Option<&Notify>,
) -> Spares {
	// A wait for spare files is only made on a pool, and a pool has the signal. Without one
	// there is nothing to hear a stop with, and the wait is the whole of it.
	let Some(stop_notify) = stop_notify else {
		return match wait.await {
			Ok(()) => Spares::Ready,
			Err(e) => Spares::Failed(Error::from(e)),
		};
	};
	let stopped = stop_notify.notified();
	tokio::pin!(stopped);
	stopped.as_mut().enable();
	if stop_flag.load(Ordering::SeqCst) {
		return Spares::Stopped;
	}
	tokio::select! {
		done = wait => match done {
			Ok(()) => Spares::Ready,
			Err(e) => Spares::Failed(Error::from(e)),
		},
		() = &mut stopped => Spares::Stopped,
	}
}

impl fmt::Debug for TaskManager {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("TaskManager")
			.field("memtable_running", &self.memtable_running.load(Ordering::Acquire))
			.field("level_running", &self.level_running.load(Ordering::Acquire))
			.finish()
	}
}

impl TaskManager {
	pub(crate) fn new(
		core: Arc<dyn CompactionOperations>,
		opts: Arc<Options>,
		write_stall: Arc<WriteStallController>,
	) -> Self {
		let stop_flag = Arc::new(AtomicBool::new(false));
		let memtable_notify = Arc::new(Notify::new());
		let level_notify = Arc::new(Notify::new());
		let memtable_running = Arc::new(AtomicBool::new(false));
		let level_running = Arc::new(AtomicBool::new(false));
		let task_handles = Mutex::new(Some(Vec::new()));
		// Everything that only a pool needs is done behind this, so that the tasks of a database on
		// `std` do exactly what they did.
		let pooled = opts.fs.kind() == FsKind::Pool;
		let stop_notify = pooled.then(|| Arc::new(Notify::new()));

		// Spawn memtable compaction task
		{
			let core = Arc::clone(&core);
			let stop_flag = Arc::clone(&stop_flag);
			let notify = Arc::clone(&memtable_notify);
			let running = Arc::clone(&memtable_running);
			let level_notify = Arc::clone(&level_notify);
			let write_stall = Arc::clone(&write_stall);
			let stop_notify = stop_notify.clone();

			let handle = rt::spawn(async move {
				let error_handler = core.error_handler();
				'task: loop {
					// Wait for notification
					notify.notified().await;

					if stop_flag.load(Ordering::SeqCst) {
						break;
					}

					running.store(true, Ordering::SeqCst);
					tracing::debug!("Memtable flush task starting");

					// Flush ALL pending immutable memtables in a loop
					let mut flush_count = 0;
					let mut retry_wait = COMPACTION_RETRY_MIN;
					loop {
						// A pool has a fixed number of files: the flush waits until it has the ones
						// it creates, and a failure of the unit during which the pool refused a
						// creation is not taken for a permanent one (see
						// `set_error_after_exhaustion`).
						let exhaustions = if pooled {
							core.pool_exhaustions()
						} else {
							0
						};
						let spares = match pooled.then(|| core.spares_for_flush()).flatten() {
							Some(wait) if core.has_pending_immutables() => {
								await_spares(wait, &stop_flag, stop_notify.as_deref()).await
							}
							_ => Spares::Ready,
						};
						let result = match spares {
							Spares::Ready => core.compact_memtable(),
							Spares::Stopped => {
								running.store(false, Ordering::SeqCst);
								break 'task;
							}
							Spares::Failed(e) => Err(e),
						};
						match result {
							Ok(()) => {
								flush_count += 1;
								retry_wait = FLUSH_RETRY_MIN;
								// A flush that succeeds ends the failure of the one before it
								error_handler.recover(BackgroundErrorReason::MemtablaFlush);
								write_stall.signal_work_done();
								if pooled {
									core.after_background_unit();
								}
								// Gives the event loop of a browser its turn between two flushes: a
								// worker has no other thread to serve what is waiting.
								rt::relax().await;
								// Check if there are more immutables to flush
								if !core.has_pending_immutables() {
									break;
								}
							}
							Err(e) => {
								tracing::error!("Memtable compaction task error: {e:?}");
								let severity = error_handler.set_error_after_exhaustion(
									e.clone(),
									BackgroundErrorReason::MemtablaFlush,
									pooled && core.exhausted_since(exhaustions),
								);
								// A database a commit group stopped refuses every flush for good,
								// so there is nothing to retry.
								if severity >= ErrorSeverity::FatalError
									|| error_handler.commit_group_error().is_some()
								{
									write_stall.signal_shutdown();
									break;
								}
								// The memtable stays queued and the manifest is as it was.
								// Commits fail meanwhile if the error stops writes, and the
								// writers stalled on the flush are let go.
								write_stall.signal_failure();
								// A key that does not fit, or bytes that did not authenticate,
								// will not be any better the next time: the memtable stays
								// queued, writes stay stopped, and nothing is attempted again
								// until something asks for a flush.
								if !BackgroundErrorHandler::retries(
									&e,
									severity,
									BackgroundErrorReason::MemtablaFlush,
								) {
									break;
								}
								// Otherwise the flush is run again until it succeeds. A key
								// provider that was away may have to be asked for its keys again
								// first, which is slow, and is done here and not on the commit
								// path.
								if matches!(&e, Error::Encryption(_)) {
									if let Err(prepare_error) = core.prepare_keys() {
										tracing::debug!(
											"The keys could not be prepared again: {prepare_error:?}"
										);
									}
								}
								let _ = rt::timeout(retry_wait, notify.notified()).await;
								if stop_flag.load(Ordering::SeqCst) {
									running.store(false, Ordering::SeqCst);
									break 'task;
								}
								retry_wait = (retry_wait * 2).min(FLUSH_RETRY_MAX);
							}
						}
					}

					if flush_count > 0 {
						tracing::debug!(
							"Memtable flush task completed: flushed {} memtables",
							flush_count
						);
						// Trigger level compaction after successful flushes
						level_notify.notify_one();
					} else {
						tracing::debug!("Memtable flush task: no immutables to flush");
					}

					running.store(false, Ordering::SeqCst);
				}
			});
			task_handles.lock().unwrap().as_mut().unwrap().push(handle);
		}

		// Spawn level compaction task
		{
			let core = Arc::clone(&core);
			let stop_flag = Arc::clone(&stop_flag);
			let notify = Arc::clone(&level_notify);
			let running = Arc::clone(&level_running);
			let write_stall = Arc::clone(&write_stall);
			let stop_notify = stop_notify.clone();

			let handle = rt::spawn(async move {
				'task: loop {
					// Wait for notification
					notify.notified().await;

					if stop_flag.load(Ordering::SeqCst) {
						break;
					}

					running.store(true, Ordering::SeqCst);
					tracing::debug!("Level compaction task starting");

					// Use leveled compaction strategy
					let strategy: Arc<dyn CompactionStrategy> =
						Arc::new(Strategy::from_options(Arc::clone(&opts)));
					let error_handler = core.error_handler();
					let mut retry_wait = COMPACTION_RETRY_MIN;
					loop {
						// See the flush task. The outputs of a compaction are created only while
						// the pool has more files than the commit path
						// keeps back, so the wait is for those and the ones
						// that the compaction itself needs.
						let exhaustions = if pooled {
							core.pool_exhaustions()
						} else {
							0
						};
						let spares = match pooled.then(|| core.spares_for_compaction()).flatten() {
							Some(wait) => {
								await_spares(wait, &stop_flag, stop_notify.as_deref()).await
							}
							None => Spares::Ready,
						};
						let result = match spares {
							Spares::Ready => core.compact(Arc::clone(&strategy)),
							Spares::Stopped => {
								running.store(false, Ordering::SeqCst);
								break 'task;
							}
							Spares::Failed(e) => Err(e),
						};
						if pooled {
							core.compaction_ran(&result);
						}
						match result {
							Ok(()) => {
								tracing::debug!("Level compaction completed successfully");
								// A compaction that works ends the wait of the one before it for a
								// key provider. Any other failure of a compaction stays on record.
								error_handler.recover_soft(BackgroundErrorReason::Compaction);
								error_handler.recover_soft(BackgroundErrorReason::ManifestWrite);
								write_stall.signal_work_done();
								if pooled {
									core.after_background_unit();
								}
								rt::relax().await;
								break;
							}
							Err(e) => {
								tracing::error!("Level compaction task error: {e:?}");
								let severity = error_handler.set_error_after_exhaustion(
									e.clone(),
									BackgroundErrorReason::Compaction,
									pooled && core.exhausted_since(exhaustions),
								);
								if !BackgroundErrorHandler::retries(
									&e,
									severity,
									BackgroundErrorReason::Compaction,
								) || error_handler.commit_group_error().is_some()
								{
									write_stall.signal_shutdown();
									break;
								}
								// A key provider that is away: nothing was changed, so the
								// compaction is picked again after the wait that a failed flush
								// makes, and the writers stalled on it are let go meanwhile.
								write_stall.signal_failure();
								if let Err(prepare_error) = core.prepare_keys() {
									tracing::debug!(
										"The keys could not be prepared again: {prepare_error:?}"
									);
								}
								let _ = rt::timeout(retry_wait, notify.notified()).await;
								if stop_flag.load(Ordering::SeqCst) {
									running.store(false, Ordering::SeqCst);
									break 'task;
								}
								retry_wait = (retry_wait * 2).min(COMPACTION_RETRY_MAX);
							}
						}
					}
					running.store(false, Ordering::SeqCst);
				}
			});
			task_handles.lock().unwrap().as_mut().unwrap().push(handle);
		}

		Self {
			stop_flag,
			memtable_notify,
			level_notify,
			memtable_running,
			level_running,
			task_handles,
			stop_notify,
		}
	}

	/// Test observer: whether the tasks have a signal for a stop that a wait for spare files is
	/// to hear, which only a pool needs.
	#[cfg(test)]
	pub(crate) fn has_stop_signal(&self) -> bool {
		self.stop_notify.is_some()
	}

	pub(crate) fn wake_up_memtable(&self) {
		// Only notify if not already running
		if !self.memtable_running.load(Ordering::Acquire) {
			self.memtable_notify.notify_one();
		}
	}

	pub(crate) fn wake_up_level(&self) {
		// Only notify if not already running
		if !self.level_running.load(Ordering::Acquire) {
			self.level_notify.notify_one();
		}
	}

	/// Ends the background tasks without waiting for them, the way a crash does, so that a tree
	/// that is abandoned does not stay alive until the runtime is dropped. Nothing outside the
	/// tests calls this yet: `Tree::abandon` will.
	pub(crate) fn abort(&self) {
		self.stop_flag.store(true, Ordering::SeqCst);
		if let Some(notify) = &self.stop_notify {
			notify.notify_waiters();
		}
		if let Some(handles) = self.task_handles.lock().unwrap().take() {
			for handle in handles {
				handle.abort();
			}
		}
	}

	pub async fn stop(&self) {
		// Set the stop flag to prevent new operations from starting
		self.stop_flag.store(true, Ordering::SeqCst);

		// Wake up any waiting tasks so they can check the stop flag and exit, and the ones that
		// wait for the spare files of a pool, which may never come.
		self.memtable_notify.notify_one();
		self.level_notify.notify_one();
		if let Some(notify) = &self.stop_notify {
			notify.notify_waiters();
		}

		// Wait for any in-progress compactions to complete (no timeout - wait
		// indefinitely)
		while self.memtable_running.load(Ordering::Acquire)
			|| self.level_running.load(Ordering::Acquire)
		{
			// Yield to other tasks and wait a short time before checking again
			rt::sleep(Duration::from_millis(50)).await;
		}

		// Now it's safe to wait for all tasks to complete
		let task_handles = self.task_handles.lock().unwrap().take().unwrap();
		for handle in task_handles {
			if let Err(e) = handle.await {
				tracing::error!("Error shutting down task: {e:?}");
			}
		}
	}
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
	use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
	use std::sync::Arc;
	use std::time::Duration;

	use test_log::test;
	use tokio::time;

	use crate::compaction::CompactionStrategy;
	use crate::error::{BackgroundErrorHandler, Result};
	use crate::lsm::CompactionOperations;
	use crate::stall::{
		StallCounts,
		StallThresholds,
		WriteStallController,
		WriteStallCountProvider,
	};
	use crate::task::TaskManager;
	use crate::{BackgroundErrorReason, Error, ErrorSeverity, Options};

	struct NoopStallProvider;

	impl WriteStallCountProvider for NoopStallProvider {
		fn get_stall_counts(&self) -> StallCounts {
			StallCounts {
				immutable_memtables: 0,
				l0_files: 0,
			}
		}
	}

	fn test_write_stall() -> Arc<WriteStallController> {
		let provider: Arc<dyn WriteStallCountProvider> = Arc::new(NoopStallProvider);
		let thresholds = StallThresholds::new(2, 12);
		Arc::new(WriteStallController::new(provider, thresholds))
	}

	// Mock CoreInner for testing
	struct MockCoreInner {
		memtable_compactions: Arc<AtomicUsize>,
		level_compactions: Arc<AtomicUsize>,
		// Add delay configuration to test different scenarios
		memtable_delay_ms: u64,
		level_delay_ms: u64,
		fail_memtable: Arc<AtomicBool>,
		fail_level: Arc<AtomicBool>,
		/// The next calls of `compact_memtable` that fail, as `fail_memtable` does
		memtable_failures: AtomicUsize,
		/// Whether a failed `compact_memtable` is an I/O error, which is fatal
		memtable_failure_is_io: AtomicBool,
		memtable_attempts: AtomicUsize,
		/// The errors that the next calls of `compact_memtable` fail with, the first one first
		memtable_errors: std::sync::Mutex<Vec<Error>>,
		/// An error that every call of `compact_memtable` fails with while it is set
		memtable_error: std::sync::Mutex<Option<Error>>,
		/// The same two for `compact`
		level_errors: std::sync::Mutex<Vec<Error>>,
		level_error: std::sync::Mutex<Option<Error>>,
		/// Every call of `compact`, whether it failed or not
		level_attempts: AtomicUsize,
		/// The times the tasks asked for the keys again
		prepares: AtomicUsize,
		/// The next calls of `compact` that fail, as `fail_level` does
		level_failures: AtomicUsize,
		/// Whether a failed `compact` is fatal
		level_failure_is_fatal: AtomicBool,
		error_handler: Arc<BackgroundErrorHandler>,
	}

	impl MockCoreInner {
		fn new() -> Self {
			Self::with_delays(20, 20)
		}

		fn with_delays(memtable_delay_ms: u64, level_delay_ms: u64) -> Self {
			Self {
				memtable_compactions: Arc::new(AtomicUsize::new(0)),
				level_compactions: Arc::new(AtomicUsize::new(0)),
				memtable_delay_ms,
				level_delay_ms,
				fail_memtable: Arc::new(AtomicBool::new(false)),
				fail_level: Arc::new(AtomicBool::new(false)),
				memtable_failures: AtomicUsize::new(0),
				memtable_failure_is_io: AtomicBool::new(false),
				memtable_attempts: AtomicUsize::new(0),
				memtable_errors: std::sync::Mutex::new(Vec::new()),
				memtable_error: std::sync::Mutex::new(None),
				level_errors: std::sync::Mutex::new(Vec::new()),
				level_error: std::sync::Mutex::new(None),
				level_attempts: AtomicUsize::new(0),
				prepares: AtomicUsize::new(0),
				level_failures: AtomicUsize::new(0),
				level_failure_is_fatal: AtomicBool::new(false),
				error_handler: Arc::new(BackgroundErrorHandler::new()),
			}
		}

		/// The error that the next call of `compact_memtable` is to fail with, if one is set.
		fn next_memtable_error(&self) -> Option<Error> {
			if let Some(error) = self.memtable_error.lock().unwrap().clone() {
				return Some(error);
			}
			let mut queued = self.memtable_errors.lock().unwrap();
			(!queued.is_empty()).then(|| queued.remove(0))
		}

		/// The error that the next call of `compact` is to fail with, if one is set.
		fn next_level_error(&self) -> Option<Error> {
			if let Some(error) = self.level_error.lock().unwrap().clone() {
				return Some(error);
			}
			let mut queued = self.level_errors.lock().unwrap();
			(!queued.is_empty()).then(|| queued.remove(0))
		}
	}

	impl CompactionOperations for MockCoreInner {
		fn compact_memtable(&self) -> Result<()> {
			let start = std::time::Instant::now();
			while start.elapsed() < Duration::from_millis(self.memtable_delay_ms) {
				std::hint::spin_loop();
			}

			self.memtable_attempts.fetch_add(1, Ordering::SeqCst);

			if let Some(error) = self.next_memtable_error() {
				return Err(error);
			}

			// Check if should fail
			let mut left = self.memtable_failures.load(Ordering::SeqCst);
			let failing = loop {
				if left == 0 {
					break false;
				}
				match self.memtable_failures.compare_exchange(
					left,
					left - 1,
					Ordering::SeqCst,
					Ordering::SeqCst,
				) {
					Ok(_) => break true,
					Err(now) => left = now,
				}
			};
			if failing || self.fail_memtable.load(Ordering::SeqCst) {
				if self.memtable_failure_is_io.load(Ordering::SeqCst) {
					return Err(Error::Io(Arc::new(std::io::Error::other("memtable error"))));
				}
				return Err(Error::Other("memtable error".into()));
			}

			// Only increment counter on success
			self.memtable_compactions.fetch_add(1, Ordering::SeqCst);
			Ok(())
		}

		fn compact(&self, _strategy: Arc<dyn CompactionStrategy>) -> Result<()> {
			let start = std::time::Instant::now();
			while start.elapsed() < Duration::from_millis(self.level_delay_ms) {
				std::hint::spin_loop();
			}

			self.level_attempts.fetch_add(1, Ordering::SeqCst);
			if let Some(error) = self.next_level_error() {
				return Err(error);
			}

			// Check if should fail
			let mut left = self.level_failures.load(Ordering::SeqCst);
			let failing = loop {
				if left == 0 {
					break false;
				}
				match self.level_failures.compare_exchange(
					left,
					left - 1,
					Ordering::SeqCst,
					Ordering::SeqCst,
				) {
					Ok(_) => break true,
					Err(now) => left = now,
				}
			};
			if failing || self.fail_level.load(Ordering::SeqCst) {
				if self.level_failure_is_fatal.load(Ordering::SeqCst) {
					return Err(Error::ManifestWriteUncertain("level fatal error".into()));
				}
				return Err(Error::Other("level error".into()));
			}

			// Only increment counter on success
			self.level_compactions.fetch_add(1, Ordering::SeqCst);
			Ok(())
		}

		fn error_handler(&self) -> Arc<BackgroundErrorHandler> {
			Arc::clone(&self.error_handler)
		}

		fn has_pending_immutables(&self) -> bool {
			false // Mock always returns false (no immutables)
		}

		fn prepare_keys(&self) -> Result<()> {
			self.prepares.fetch_add(1, Ordering::SeqCst);
			Ok(())
		}
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_wake_up_memtable() {
		let opts = Arc::new(Options::default());
		let core = Arc::new(MockCoreInner::new());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			test_write_stall(),
		);

		task_manager.wake_up_memtable();
		time::sleep(Duration::from_millis(100)).await; // Allow time for task to complete

		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 1);
		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 1); // Level compaction should
																	  // follow

		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_multiple_wake_up_memtable() {
		let opts = Arc::new(Options::default());
		let core = Arc::new(MockCoreInner::new());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			test_write_stall(),
		);

		for _ in 0..3 {
			task_manager.wake_up_memtable();
			time::sleep(Duration::from_millis(100)).await; // Allow time for task to
			                                               // complete
		}

		// Wait for all operations to complete
		time::sleep(Duration::from_millis(300)).await;

		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 3);
		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 3); // Each memtable compaction
																	  // triggers a level compaction

		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_wake_up_level() {
		let opts = Arc::new(Options::default());
		let core = Arc::new(MockCoreInner::new());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			test_write_stall(),
		);

		task_manager.wake_up_level();
		time::sleep(Duration::from_millis(100)).await; // Allow time for task to complete

		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 1);
		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 0); // Memtable should not be
																		 // affected

		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_multiple_wake_up_level() {
		let opts = Arc::new(Options::default());
		let core = Arc::new(MockCoreInner::new());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			test_write_stall(),
		);

		for _ in 0..3 {
			task_manager.wake_up_level();
			time::sleep(Duration::from_millis(100)).await; // Allow time for task to
			                                               // complete
		}

		// Wait for all operations to complete
		time::sleep(Duration::from_millis(300)).await;

		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 3);
		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 0); // Memtable should not be
																		 // affected

		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_alternating_compactions() {
		let opts = Arc::new(Options::default());
		let core = Arc::new(MockCoreInner::new());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			test_write_stall(),
		);

		// Alternating between memtable and level compactions
		for i in 0..4 {
			if i % 2 == 0 {
				task_manager.wake_up_memtable();
			} else {
				task_manager.wake_up_level();
			}
			time::sleep(Duration::from_millis(100)).await;
		}

		// Wait for all operations to complete
		time::sleep(Duration::from_millis(300)).await;

		// 2 direct memtable compactions + 2 triggered by level (which is 2)
		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 2);
		// 2 direct level compactions + 2 triggered by memtable
		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 4);

		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_already_running_tasks() {
		let opts = Arc::new(Options::default());
		// Create core with longer delays to ensure tasks are still running when we try
		// to wake them again
		let core = Arc::new(MockCoreInner::with_delays(100, 100));
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			test_write_stall(),
		);

		// Wake up memtable and immediately try again while it's still running
		task_manager.wake_up_memtable();
		task_manager.wake_up_memtable(); // This should be ignored as task is already running

		// Wait for first task to complete but not the second (if it were scheduled)
		time::sleep(Duration::from_millis(150)).await;

		// Only one compaction should have occurred
		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 1);

		// Do the same test with level compaction
		time::sleep(Duration::from_millis(150)).await; // Ensure previous level compaction is done

		task_manager.wake_up_level();
		task_manager.wake_up_level(); // This should be ignored

		time::sleep(Duration::from_millis(150)).await;

		// 1 from memtable + 1 directly triggered = 2
		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 2);

		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_concurrent_wake_up_memtable() {
		let opts = Arc::new(Options::default());
		let core = Arc::new(MockCoreInner::new());
		let task_manager = Arc::new(TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			test_write_stall(),
		));

		let mut handles = vec![];
		for _ in 0..10 {
			let tm = Arc::clone(&task_manager);
			handles.push(tokio::spawn(async move {
				tm.wake_up_memtable();
			}));
			// Small sleep to ensure some interleaving
			time::sleep(Duration::from_millis(100)).await; // Allow time for all tasks to
			                                               // complete
		}

		for handle in handles {
			handle.await.unwrap();
		}

		// Allow time for all tasks to complete
		time::sleep(Duration::from_millis(1000)).await;

		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 10);
		assert_eq!(core.level_compactions.load(Ordering::SeqCst), 10);

		Arc::try_unwrap(task_manager).expect("Task manager still has references").stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_concurrent_wake_up_level() {
		let opts = Arc::new(Options::default());
		let core = Arc::new(MockCoreInner::new());
		let task_manager = Arc::new(TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			test_write_stall(),
		));

		let mut handles = vec![];
		for _ in 0..5 {
			let tm = Arc::clone(&task_manager);
			handles.push(tokio::spawn(async move {
				tm.wake_up_level();
				// Small sleep to ensure some interleaving
				time::sleep(Duration::from_millis(5)).await;
			}));
		}

		for handle in handles {
			handle.await.unwrap();
		}

		// Allow time for all tasks to complete
		time::sleep(Duration::from_millis(500)).await;

		// Due to the running flag mechanism, we expect fewer compactions than wake-ups
		let level_count = core.level_compactions.load(Ordering::SeqCst);

		assert!(
			level_count > 0 && level_count <= 5,
			"Expected between 1-5 level compactions, got {level_count}"
		);

		Arc::try_unwrap(task_manager).expect("Task manager still has references").stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_error_handling() {
		// This test verifies the correct error handling behavior

		// Create a mock with error simulation capability
		let core = Arc::new(MockCoreInner::new());

		// First, check how the mock behaves with a direct call to verify our
		// understanding
		core.fail_memtable.store(true, Ordering::SeqCst);
		let direct_result = core.compact_memtable();
		assert!(direct_result.is_err(), "Should return an error when fail_memtable is true");
		let memtable_count_after_direct = core.memtable_compactions.load(Ordering::SeqCst);
		println!("Memtable count after direct call: {memtable_count_after_direct}");

		// Reset counters
		core.memtable_compactions.store(0, Ordering::SeqCst);
		core.level_compactions.store(0, Ordering::SeqCst);

		// Now create the task manager and run the actual test
		let opts = Arc::new(Options::default());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			test_write_stall(),
		);

		// Trigger memtable compaction that will fail
		task_manager.wake_up_memtable();
		time::sleep(Duration::from_millis(100)).await;

		// Read the actual counts to understand the real behavior
		let memtable_count = core.memtable_compactions.load(Ordering::SeqCst);
		let level_count = core.level_compactions.load(Ordering::SeqCst);

		println!(
            "After wake_up_memtable with failure: memtable_count={memtable_count}, level_count={level_count}"
        );

		assert!(
			level_count == 0,
			"Level compaction was triggered after memtable failure. Expected 0, got {level_count}"
		);

		// Reset counters
		core.memtable_compactions.store(0, Ordering::SeqCst);
		core.level_compactions.store(0, Ordering::SeqCst);

		// Set memtable to succeed but level to fail
		core.fail_memtable.store(false, Ordering::SeqCst);
		core.fail_level.store(true, Ordering::SeqCst);

		// Trigger memtable compaction (which should succeed)
		task_manager.wake_up_memtable();
		time::sleep(Duration::from_millis(100)).await;

		// Read counts again
		let memtable_count = core.memtable_compactions.load(Ordering::SeqCst);
		let level_count = core.level_compactions.load(Ordering::SeqCst);
		println!("After wake_up_memtable with success but level failure: memtable_count={memtable_count}, level_count={level_count}");

		// Reset counters
		core.memtable_compactions.store(0, Ordering::SeqCst);
		core.level_compactions.store(0, Ordering::SeqCst);

		// Fix level compaction and try direct level compaction
		core.fail_level.store(false, Ordering::SeqCst);
		task_manager.wake_up_level();
		time::sleep(Duration::from_millis(100)).await;

		// Read final counts
		let level_count = core.level_compactions.load(Ordering::SeqCst);
		println!("After wake_up_level with success: level_count={level_count}");

		stop_within_a_bound(task_manager).await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_task_from_fail() {
		let opts = Arc::new(Options::default());
		// Create a core that will fail on the first attempt but succeed on subsequent
		// attempts
		let core = Arc::new(MockCoreInner::new());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			test_write_stall(),
		);

		// Make first memtable compaction fail
		core.fail_memtable.store(true, Ordering::SeqCst);

		// Trigger a failing compaction
		task_manager.wake_up_memtable();
		time::sleep(Duration::from_millis(100)).await;

		assert_eq!(core.memtable_compactions.load(Ordering::SeqCst), 0);

		// Make next memtable compaction succeed
		core.fail_memtable.store(false, Ordering::SeqCst);

		// The task runs the failed flush again by itself, and that succeeds
		wait_for(|| core.memtable_compactions.load(Ordering::SeqCst) == 1).await;

		// Task should still be responsive after error
		stop_within_a_bound(task_manager).await;
	}

	/// Waits for `condition`, which the tasks make true.
	async fn wait_for(condition: impl Fn() -> bool) {
		time::timeout(Duration::from_secs(10), async {
			while !condition() {
				time::sleep(Duration::from_millis(10)).await;
			}
		})
		.await
		.expect("timed out waiting for the background task");
	}

	/// Stops the tasks, failing the test if a flush that keeps failing is not let go of.
	async fn stop_within_a_bound(task_manager: TaskManager) {
		time::timeout(Duration::from_secs(10), task_manager.stop())
			.await
			.expect("stop returns while a flush is waiting to be retried");
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_failed_memtable_flush_is_retried_until_it_succeeds() {
		let core = Arc::new(MockCoreInner::new());
		core.memtable_failures.store(2, Ordering::SeqCst);
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			Arc::new(Options::default()),
			test_write_stall(),
		);

		task_manager.wake_up_memtable();
		wait_for(|| core.memtable_compactions.load(Ordering::SeqCst) == 1).await;

		assert_eq!(core.memtable_attempts.load(Ordering::SeqCst), 3);
		// The failure was recorded once, and the success ended it.
		assert_eq!(core.error_handler.error_count(), 1);
		assert!(core.error_handler.check_error().is_ok());
		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_fatal_memtable_flush_failure_is_not_retried() {
		let write_stall = test_write_stall();
		let core = Arc::new(MockCoreInner::new());
		core.fail_memtable.store(true, Ordering::SeqCst);
		core.memtable_failure_is_io.store(true, Ordering::SeqCst);
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			Arc::new(Options::default()),
			Arc::clone(&write_stall),
		);

		task_manager.wake_up_memtable();
		wait_for(|| core.error_handler.is_db_stopped()).await;
		time::sleep(Duration::from_millis(300)).await;

		assert_eq!(core.memtable_attempts.load(Ordering::SeqCst), 1);
		// A fatal failure refuses every writer after it, and no success ends it.
		assert!(write_stall.check().await.is_err());
		core.fail_memtable.store(false, Ordering::SeqCst);
		task_manager.wake_up_memtable();
		wait_for(|| core.memtable_compactions.load(Ordering::SeqCst) == 1).await;
		assert!(core.error_handler.check_error().is_err());
		task_manager.stop().await;
	}

	/// A flush that fails once a commit group stopped the database is refused for good, not a
	/// failure that passes: it is not run again, and a later success does not end the stop.
	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_flush_failing_on_a_database_a_commit_group_stopped_is_not_retried() {
		let write_stall = test_write_stall();
		let core = Arc::new(MockCoreInner::new());
		core.error_handler.stop_commit_group(Error::DatabaseStopped("group".into()));
		core.fail_memtable.store(true, Ordering::SeqCst);
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			Arc::new(Options::default()),
			Arc::clone(&write_stall),
		);

		task_manager.wake_up_memtable();
		wait_for(|| core.memtable_attempts.load(Ordering::SeqCst) >= 1).await;
		// The first wait of a retry is 50 ms: a retry would have run again by now.
		time::sleep(Duration::from_millis(400)).await;
		assert_eq!(core.memtable_attempts.load(Ordering::SeqCst), 1);

		// A flush that succeeds on a later wake ends its own failure, not the stop.
		core.fail_memtable.store(false, Ordering::SeqCst);
		task_manager.wake_up_memtable();
		wait_for(|| core.memtable_compactions.load(Ordering::SeqCst) == 1).await;
		assert!(matches!(core.error_handler.check_error(), Err(Error::DatabaseStopped(_))));
		assert!(core.error_handler.commit_group_error().is_some());
		assert!(write_stall.check().await.is_err(), "writers stay refused");
		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_failed_level_compaction_is_retried_until_it_succeeds() {
		let core = Arc::new(MockCoreInner::new());
		core.level_failures.store(2, Ordering::SeqCst);
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			Arc::new(Options::default()),
			test_write_stall(),
		);

		task_manager.wake_up_level();
		wait_for(|| core.level_compactions.load(Ordering::SeqCst) == 1).await;

		assert_eq!(core.level_attempts.load(Ordering::SeqCst), 3);
		// The failure was recorded once, and the success recovered from it.
		assert_eq!(core.error_handler.error_count(), 1);
		assert!(core.error_handler.check_error().is_ok());
		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_fatal_level_compaction_failure_is_not_retried() {
		let write_stall = test_write_stall();
		let core = Arc::new(MockCoreInner::new());
		core.fail_level.store(true, Ordering::SeqCst);
		core.level_failure_is_fatal.store(true, Ordering::SeqCst);
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			Arc::new(Options::default()),
			Arc::clone(&write_stall),
		);

		task_manager.wake_up_level();
		wait_for(|| core.error_handler.is_db_stopped()).await;
		time::sleep(Duration::from_millis(400)).await;
		assert_eq!(core.level_attempts.load(Ordering::SeqCst), 1);

		assert!(matches!(core.error_handler.check_error(), Err(Error::ManifestWriteUncertain(_))));
		assert!(write_stall.check().await.is_err(), "writers stay refused");
		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_stop_does_not_wait_for_a_flush_that_keeps_failing() {
		let core = Arc::new(MockCoreInner::new());
		core.fail_memtable.store(true, Ordering::SeqCst);
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			Arc::new(Options::default()),
			test_write_stall(),
		);

		task_manager.wake_up_memtable();
		wait_for(|| core.memtable_attempts.load(Ordering::SeqCst) >= 3).await;

		stop_within_a_bound(task_manager).await;
	}

	struct AboveThresholdProvider {
		l0_files: usize,
	}

	impl WriteStallCountProvider for AboveThresholdProvider {
		fn get_stall_counts(&self) -> StallCounts {
			StallCounts {
				immutable_memtables: 0,
				l0_files: self.l0_files,
			}
		}
	}

	fn stalled_write_stall(l0_files: usize) -> Arc<WriteStallController> {
		let provider: Arc<dyn WriteStallCountProvider> = Arc::new(AboveThresholdProvider {
			l0_files,
		});
		let thresholds = StallThresholds::new(2, 12);
		Arc::new(WriteStallController::new(provider, thresholds))
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_stalled_writer_unblocked_on_level_compaction_failure() {
		let write_stall = stalled_write_stall(20);
		let core = Arc::new(MockCoreInner::new());
		core.fail_level.store(true, Ordering::SeqCst);

		let opts = Arc::new(Options::default());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			Arc::clone(&write_stall),
		);

		let stall_clone = Arc::clone(&write_stall);
		let writer_handle = tokio::spawn(async move { stall_clone.check().await });

		// Let the writer enter the stall loop
		time::sleep(Duration::from_millis(50)).await;

		// Trigger level compaction that will fail, which should call signal_shutdown()
		task_manager.wake_up_level();

		let result = time::timeout(Duration::from_secs(2), writer_handle).await;
		let writer_result = result
			.expect("Writer should not timeout (was it unblocked?)")
			.expect("Writer task should not panic");

		assert!(
			writer_result.is_err(),
			"Stalled writer should return Err(PipelineStall) after compaction failure"
		);

		stop_within_a_bound(task_manager).await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_stalled_writer_unblocked_on_memtable_flush_failure() {
		let write_stall = stalled_write_stall(20);
		let core = Arc::new(MockCoreInner::new());
		core.fail_memtable.store(true, Ordering::SeqCst);

		let opts = Arc::new(Options::default());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			Arc::clone(&write_stall),
		);

		let stall_clone = Arc::clone(&write_stall);
		let writer_handle = tokio::spawn(async move { stall_clone.check().await });

		// Let the writer enter the stall loop
		time::sleep(Duration::from_millis(50)).await;

		// Trigger memtable flush that will fail, which should call signal_shutdown()
		task_manager.wake_up_memtable();

		let result = time::timeout(Duration::from_secs(2), writer_handle).await;
		let writer_result = result
			.expect("Writer should not timeout (was it unblocked?)")
			.expect("Writer task should not panic");

		assert!(
			writer_result.is_err(),
			"Stalled writer should return Err(PipelineStall) after flush failure"
		);

		stop_within_a_bound(task_manager).await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_multiple_stalled_writers_unblocked_on_failure() {
		let write_stall = stalled_write_stall(20);
		let core = Arc::new(MockCoreInner::new());
		core.fail_level.store(true, Ordering::SeqCst);

		let opts = Arc::new(Options::default());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			opts,
			Arc::clone(&write_stall),
		);

		let mut writer_handles = Vec::new();
		for _ in 0..5 {
			let stall_clone = Arc::clone(&write_stall);
			writer_handles.push(tokio::spawn(async move { stall_clone.check().await }));
		}

		// Let all writers enter the stall loop
		time::sleep(Duration::from_millis(50)).await;

		// Trigger level compaction failure — signal_shutdown uses notify_waiters
		// which must wake ALL 5 writers, not just one
		task_manager.wake_up_level();

		for (i, handle) in writer_handles.into_iter().enumerate() {
			let result = time::timeout(Duration::from_secs(2), handle).await;
			let writer_result = result
				.unwrap_or_else(|_| panic!("Writer {i} timed out (not unblocked)"))
				.unwrap_or_else(|e| panic!("Writer {i} panicked: {e}"));
			assert!(writer_result.is_err(), "Writer {i} should return Err(PipelineStall)");
		}

		stop_within_a_bound(task_manager).await;
	}

	// ===== Encryption errors =====

	use crate::{EncryptionError, KeyProviderFailure};

	fn unavailable() -> Error {
		Error::Encryption(EncryptionError::KeyProvider(KeyProviderFailure::Unavailable))
	}

	/// An error of each class that trying again does not cure.
	fn errors_that_are_not_retried() -> Vec<(Error, ErrorSeverity)> {
		vec![
			(Error::Encryption(EncryptionError::WrongKey), ErrorSeverity::HardError),
			(Error::Encryption(EncryptionError::KeyRetired), ErrorSeverity::HardError),
			(
				Error::Encryption(EncryptionError::KeyProvider(KeyProviderFailure::Failed)),
				ErrorSeverity::HardError,
			),
			(
				Error::Encryption(EncryptionError::AuthenticationFailed),
				ErrorSeverity::Unrecoverable,
			),
			(Error::Encryption(EncryptionError::PositionReuse), ErrorSeverity::Unrecoverable),
			(Error::Encryption(EncryptionError::InvalidInput("a test")), ErrorSeverity::FatalError),
		]
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_a_flush_that_waits_for_a_key_provider_is_tried_again_and_stops_nothing() {
		let write_stall = test_write_stall();
		let core = Arc::new(MockCoreInner::new());
		core.memtable_errors.lock().unwrap().extend([unavailable(), unavailable(), unavailable()]);
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			Arc::new(Options::default()),
			Arc::clone(&write_stall),
		);

		task_manager.wake_up_memtable();
		wait_for(|| core.memtable_compactions.load(Ordering::SeqCst) == 1).await;
		// The success ends the failure on record, which the task does once the flush returns.
		wait_for(|| core.error_handler.get_error().is_none()).await;

		assert_eq!(core.memtable_attempts.load(Ordering::SeqCst), 4);
		// The keys were asked for again before each of the three attempts that followed a failure.
		assert_eq!(core.prepares.load(Ordering::SeqCst), 3);
		// One failure on record, and a success that ended it. Writes were never stopped.
		assert_eq!(core.error_handler.error_count(), 1);
		assert!(!core.error_handler.is_db_stopped());
		// And no writer is refused afterwards, as it would be after a shutdown.
		assert!(write_stall.check().await.is_ok());
		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_a_flush_that_waits_for_a_key_provider_does_not_stop_writes_while_it_waits() {
		let core = Arc::new(MockCoreInner::new());
		*core.memtable_error.lock().unwrap() = Some(unavailable());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			Arc::new(Options::default()),
			test_write_stall(),
		);

		task_manager.wake_up_memtable();
		wait_for(|| core.memtable_attempts.load(Ordering::SeqCst) >= 3).await;

		// The failure is on record, soft, and nothing refuses a commit because of it.
		let recorded = core.error_handler.get_error().expect("the failure is recorded");
		assert_eq!(recorded.severity, ErrorSeverity::SoftError);
		assert_eq!(recorded.reason, BackgroundErrorReason::MemtablaFlush);
		assert!(matches!(recorded.error, Error::Encryption(_)));
		assert!(!core.error_handler.is_db_stopped());
		assert!(core.error_handler.check_error().is_ok());

		// Not retried for ever: the task lets go when it is asked to stop.
		stop_within_a_bound(task_manager).await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_a_flush_that_fails_for_another_key_error_is_not_tried_again() {
		for (error, severity) in errors_that_are_not_retried() {
			let write_stall = test_write_stall();
			let core = Arc::new(MockCoreInner::new());
			*core.memtable_error.lock().unwrap() = Some(error.clone());
			let task_manager = TaskManager::new(
				Arc::clone(&core) as Arc<dyn CompactionOperations>,
				Arc::new(Options::default()),
				Arc::clone(&write_stall),
			);

			task_manager.wake_up_memtable();
			wait_for(|| core.error_handler.is_db_stopped()).await;
			// The first wait of a retry is 50 ms: a retry would have run again by now.
			time::sleep(Duration::from_millis(400)).await;

			assert_eq!(core.memtable_attempts.load(Ordering::SeqCst), 1, "{error:?}");
			assert_eq!(core.prepares.load(Ordering::SeqCst), 0, "{error:?}");
			let recorded = core.error_handler.get_error().unwrap();
			assert_eq!(recorded.severity, severity, "{error:?}");
			assert_eq!(recorded.reason, BackgroundErrorReason::MemtablaFlush);
			// Writes stop, and a commit is told the error as it is.
			match core.error_handler.check_error() {
				Err(Error::Encryption(told)) => {
					assert!(
						matches!(&error, Error::Encryption(sent) if *sent == told),
						"{error:?}"
					);
				}
				other => panic!("{error:?}: {other:?}"),
			}
			task_manager.stop().await;
		}
	}

	/// A hard key error ends when a flush that is asked for later works, like any hard error, and
	/// one that is worse than that does not.
	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_a_flush_that_works_ends_a_key_error_that_was_hard_and_no_other() {
		for (error, severity) in errors_that_are_not_retried() {
			let core = Arc::new(MockCoreInner::new());
			*core.memtable_error.lock().unwrap() = Some(error.clone());
			let task_manager = TaskManager::new(
				Arc::clone(&core) as Arc<dyn CompactionOperations>,
				Arc::new(Options::default()),
				test_write_stall(),
			);
			task_manager.wake_up_memtable();
			wait_for(|| core.error_handler.is_db_stopped()).await;
			// A wake-up is ignored while the task is still running, which it is for a moment after
			// it recorded the failure.
			wait_for(|| !task_manager.memtable_running.load(Ordering::Acquire)).await;

			*core.memtable_error.lock().unwrap() = None;
			task_manager.wake_up_memtable();
			wait_for(|| core.memtable_compactions.load(Ordering::SeqCst) == 1).await;
			// The task ends a hard error once the flush returns. The others stay.
			let ends = severity == ErrorSeverity::HardError;
			wait_for(|| core.error_handler.is_db_stopped() != ends).await;
			assert_eq!(core.error_handler.is_db_stopped(), !ends, "{error:?}");
			task_manager.stop().await;
		}
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_a_compaction_that_waits_for_a_key_provider_is_tried_again_and_stops_nothing() {
		let write_stall = test_write_stall();
		let core = Arc::new(MockCoreInner::new());
		core.level_errors.lock().unwrap().extend([unavailable(), unavailable()]);
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			Arc::new(Options::default()),
			Arc::clone(&write_stall),
		);

		task_manager.wake_up_level();
		wait_for(|| core.level_compactions.load(Ordering::SeqCst) == 1).await;
		// The success ends the failure on record, which the task does once the compaction returns.
		wait_for(|| core.error_handler.get_error().is_none()).await;

		assert_eq!(core.level_attempts.load(Ordering::SeqCst), 3);
		assert_eq!(core.prepares.load(Ordering::SeqCst), 2);
		// One failure on record, ended by the success, and writes were not stopped or refused.
		assert_eq!(core.error_handler.error_count(), 1);
		assert!(!core.error_handler.is_db_stopped());
		assert!(write_stall.check().await.is_ok());
		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_a_compaction_that_waits_for_a_key_provider_lets_the_stalled_writers_go() {
		let write_stall = stalled_write_stall(20);
		let core = Arc::new(MockCoreInner::new());
		*core.level_error.lock().unwrap() = Some(unavailable());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			Arc::new(Options::default()),
			Arc::clone(&write_stall),
		);

		let stalled = Arc::clone(&write_stall);
		let writer = tokio::spawn(async move { stalled.check().await });
		time::sleep(Duration::from_millis(50)).await;

		task_manager.wake_up_level();
		let told = time::timeout(Duration::from_secs(10), writer)
			.await
			.expect("the stalled writer is let go")
			.unwrap();
		assert!(told.is_err());
		wait_for(|| core.level_attempts.load(Ordering::SeqCst) >= 2).await;

		// The provider being away stops nothing.
		assert!(!core.error_handler.is_db_stopped());
		stop_within_a_bound(task_manager).await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_a_compaction_that_fails_for_another_key_error_stops_writes_and_is_not_tried_again(
	) {
		for (error, severity) in errors_that_are_not_retried() {
			let write_stall = test_write_stall();
			let core = Arc::new(MockCoreInner::new());
			*core.level_error.lock().unwrap() = Some(error.clone());
			let task_manager = TaskManager::new(
				Arc::clone(&core) as Arc<dyn CompactionOperations>,
				Arc::new(Options::default()),
				Arc::clone(&write_stall),
			);

			task_manager.wake_up_level();
			wait_for(|| core.error_handler.is_db_stopped()).await;
			time::sleep(Duration::from_millis(400)).await;

			assert_eq!(core.level_attempts.load(Ordering::SeqCst), 1, "{error:?}");
			assert_eq!(core.prepares.load(Ordering::SeqCst), 0, "{error:?}");
			let recorded = core.error_handler.get_error().unwrap();
			assert_eq!(recorded.severity, severity, "{error:?}");
			assert_eq!(recorded.reason, BackgroundErrorReason::Compaction);
			// As every compaction failure that is not retried, it refuses the writers that follow.
			assert!(write_stall.check().await.is_err(), "{error:?}");
			task_manager.stop().await;
		}
	}

	/// A fatal compaction failure keeps what it had: it stops the database, and a later compaction
	/// that works does not end that.
	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_a_compaction_failure_that_is_fatal_still_stops_the_database_for_good() {
		let write_stall = test_write_stall();
		let core = Arc::new(MockCoreInner::new());
		core.fail_level.store(true, Ordering::SeqCst);
		core.level_failure_is_fatal.store(true, Ordering::SeqCst);
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			Arc::new(Options::default()),
			Arc::clone(&write_stall),
		);

		task_manager.wake_up_level();
		wait_for(|| core.error_handler.is_db_stopped()).await;
		time::sleep(Duration::from_millis(400)).await;
		assert_eq!(core.level_attempts.load(Ordering::SeqCst), 1);
		assert!(write_stall.check().await.is_err());

		core.fail_level.store(false, Ordering::SeqCst);
		wait_for(|| !task_manager.level_running.load(Ordering::Acquire)).await;
		task_manager.wake_up_level();
		wait_for(|| core.level_compactions.load(Ordering::SeqCst) == 1).await;
		// The compaction has returned, so the task has decided what its success ends.
		wait_for(|| !task_manager.level_running.load(Ordering::Acquire)).await;
		assert!(core.error_handler.is_db_stopped(), "a compaction that works does not end it");
		assert!(core.error_handler.check_error().is_err());
		task_manager.stop().await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_stop_does_not_wait_for_a_compaction_that_keeps_waiting_for_a_key_provider() {
		let core = Arc::new(MockCoreInner::new());
		*core.level_error.lock().unwrap() = Some(unavailable());
		let task_manager = TaskManager::new(
			Arc::clone(&core) as Arc<dyn CompactionOperations>,
			Arc::new(Options::default()),
			test_write_stall(),
		);

		task_manager.wake_up_level();
		wait_for(|| core.level_attempts.load(Ordering::SeqCst) >= 3).await;

		stop_within_a_bound(task_manager).await;
	}
}
