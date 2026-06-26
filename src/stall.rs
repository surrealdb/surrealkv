//! Write stall controller for backpressure management.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::error::{Error, Result, WriteStallReason};

/// Current counts of resources that can trigger write stalls.
#[derive(Debug, Clone, Copy)]
pub struct StallCounts {
	/// Number of immutable memtables queued for flush
	pub immutable_memtables: usize,
	/// Number of L0 SSTable files awaiting compaction
	pub l0_files: usize,
}

/// Thresholds that trigger write stalls when exceeded.
#[derive(Debug, Clone, Copy)]
pub struct StallThresholds {
	/// Maximum immutable memtable count before stalling writes
	pub memtable_limit: usize,
	/// Maximum L0 file count before stalling writes
	pub l0_file_limit: usize,
}

/// Trait for getting current stall condition counts.
/// Implementors provide live resource counts that the controller
/// checks against its configured thresholds.
pub trait WriteStallCountProvider: Send + Sync + 'static {
	/// Get current immutable memtable count and L0 file count.
	fn get_stall_counts(&self) -> StallCounts;
}

/// Information about a write stall event.
/// Fields are used for diagnostics, testing, and potential future logging/metrics.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct WriteStallInfo {
	/// The reason for the stall
	pub reason: WriteStallReason,
	/// The current value that triggered the stall
	pub current_value: usize,
	/// The threshold that was exceeded
	pub threshold: usize,
	/// Duration spent stalled
	pub duration: Duration,
}

/// Controller that manages write stall state and signaling.
///
/// Owns its count provider and thresholds, providing a self-contained
/// API for the commit path to check backpressure conditions.
///
/// Synchronous (no tokio): the commit hot path calls `check()` directly. The
/// overwhelmingly common "not stalled" case takes no lock (a couple of atomic
/// reads); only an actual stall falls into the Condvar wait loop. This is sound
/// now that the caller is synchronous — a stalled committer blocks its own
/// thread, and the flush/compaction that clears the stall runs on a dedicated
/// `std::thread` (see `task.rs`), so there is no runtime worker to starve.
pub struct WriteStallController {
	/// Notification for when stall conditions clear.
	stall_cleared: Condvar,

	/// Mutex paired with `stall_cleared`. Held while a waiter checks stall
	/// conditions and transitions to waiting; `signal_*` take the same mutex
	/// before `notify_all`, which makes wakeups lost-wakeup-safe.
	stall_mutex: Mutex<()>,

	/// Current stall state (for fast-path check / metrics).
	is_stalled: AtomicBool,

	/// Shutdown flag - checked in stall loop for graceful exit.
	shutdown: AtomicBool,

	/// Provider for live stall count readings.
	provider: Arc<dyn WriteStallCountProvider>,

	/// Static thresholds configured at startup.
	thresholds: StallThresholds,
}

impl WriteStallController {
	pub fn new(provider: Arc<dyn WriteStallCountProvider>, thresholds: StallThresholds) -> Self {
		Self {
			stall_cleared: Condvar::new(),
			stall_mutex: Mutex::new(()),
			is_stalled: AtomicBool::new(false),
			shutdown: AtomicBool::new(false),
			provider,
			thresholds,
		}
	}

	/// Check stall conditions and block if stalled. Called before each write.
	///
	/// Returns `Ok(Some(WriteStallInfo))` if was stalled and then cleared,
	/// `Ok(None)` if not stalled, `Err(Error::PipelineStall)` on shutdown.
	///
	/// Lost-wakeup safety: the paired `stall_mutex` is held while we read the
	/// counts and transition into `wait`. `signal_work_done`/`signal_shutdown`
	/// take the same mutex before `notify_all`, so a clear that races our check
	/// cannot be missed — either we observe the cleared counts under the lock,
	/// or the signaller blocks until we are parked and then wakes us.
	pub fn check(&self) -> Result<Option<WriteStallInfo>> {
		// Fast path: the overwhelmingly common case is "not stalled". Read the
		// counts WITHOUT taking `stall_mutex`; only fall into the locking/parking
		// loop when actually stalled or shutting down.
		if !self.shutdown.load(Ordering::Acquire) {
			let counts = self.provider.get_stall_counts();
			if counts.immutable_memtables < self.thresholds.memtable_limit
				&& counts.l0_files < self.thresholds.l0_file_limit
			{
				return Ok(None);
			}
		}

		let mut stall_start: Option<Instant> = None;
		let mut stall_reason: Option<WriteStallReason> = None;
		let mut stall_value: usize = 0;
		let mut stall_threshold: usize = 0;

		loop {
			// Hold the paired mutex across the condition check and the wait.
			let mut guard = self.stall_mutex.lock();

			// Check shutdown
			if self.shutdown.load(Ordering::Acquire) {
				if stall_reason.is_some() {
					self.is_stalled.store(false, Ordering::Release);
				}
				return Err(Error::PipelineStall);
			}

			// Re-read counts each iteration (background work may clear the stall).
			let counts = self.provider.get_stall_counts();

			// Check if NOT stalled - return without waiting
			if counts.immutable_memtables < self.thresholds.memtable_limit
				&& counts.l0_files < self.thresholds.l0_file_limit
			{
				drop(guard);
				if let Some(reason) = stall_reason {
					self.is_stalled.store(false, Ordering::Release);
					let duration = stall_start.map(|s| s.elapsed()).unwrap_or(Duration::ZERO);
					log::info!("Write stall cleared after {:?}", duration);
					return Ok(Some(WriteStallInfo {
						reason,
						current_value: stall_value,
						threshold: stall_threshold,
						duration,
					}));
				}
				return Ok(None);
			}

			// Stalled - determine which condition triggered it
			let (reason, value, threshold) =
				if counts.immutable_memtables >= self.thresholds.memtable_limit {
					(
						WriteStallReason::MemtableLimit,
						counts.immutable_memtables,
						self.thresholds.memtable_limit,
					)
				} else {
					(WriteStallReason::L0FileLimit, counts.l0_files, self.thresholds.l0_file_limit)
				};

			// Record stall reason if first time
			if stall_reason.is_none() {
				stall_reason = Some(reason);
				stall_value = value;
				stall_threshold = threshold;
				stall_start = Some(Instant::now());
				self.is_stalled.store(true, Ordering::Release);
				log::warn!("Write stall: {:?} ({} >= {})", reason, value, threshold);
			}

			// Wait until signaled (atomically releases `guard` while parked).
			self.stall_cleared.wait(&mut guard);
		}
	}

	/// Non-blocking check of whether stall conditions are currently met.
	#[allow(dead_code)]
	pub fn should_stall(&self) -> bool {
		let counts = self.provider.get_stall_counts();
		counts.immutable_memtables >= self.thresholds.memtable_limit
			|| counts.l0_files >= self.thresholds.l0_file_limit
	}

	/// Get current counts from the provider (for determining stall reason).
	#[allow(dead_code)]
	pub fn provider_counts(&self) -> StallCounts {
		self.provider.get_stall_counts()
	}

	/// Get the configured memtable stall limit.
	#[allow(dead_code)]
	pub fn memtable_limit(&self) -> usize {
		self.thresholds.memtable_limit
	}

	/// Signal that stall conditions may have changed.
	/// Called after flush or compaction completes.
	pub fn signal_work_done(&self) {
		// Take the paired mutex before notifying so a waiter mid-check cannot
		// miss this wakeup (see `check`).
		let _guard = self.stall_mutex.lock();
		self.stall_cleared.notify_all();
	}

	/// Signal shutdown - wakes all stalled writers to exit.
	pub fn signal_shutdown(&self) {
		self.shutdown.store(true, Ordering::Release);
		let _guard = self.stall_mutex.lock();
		self.stall_cleared.notify_all();
	}

	/// Fast check if currently stalled (for metrics).
	#[cfg(test)]
	pub fn is_stalled(&self) -> bool {
		self.is_stalled.load(Ordering::Acquire)
	}
}
