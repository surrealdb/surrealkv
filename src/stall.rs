//! Write stall controller for backpressure management.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;

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
	/// Maximum immutable memtable count before hard stalling writes
	pub memtable_limit: usize,
	/// Soft threshold where proactive pacing delays start for memtables
	pub memtable_soft_limit: usize,
	/// Maximum L0 file count before hard stalling writes
	pub l0_file_limit: usize,
	/// Soft threshold where proactive pacing delays start for L0 files
	pub l0_file_soft_limit: usize,
}

impl StallThresholds {
	pub fn new(memtable_limit: usize, l0_file_limit: usize) -> Self {
		Self {
			memtable_limit,
			memtable_soft_limit: memtable_limit.saturating_sub(2).max(1),
			l0_file_limit,
			l0_file_soft_limit: l0_file_limit.saturating_sub(4).max(1),
		}
	}
}

/// Trait for getting current stall condition counts.
/// Implementors provide live resource counts that the controller
/// checks against its configured thresholds.
pub trait WriteStallCountProvider: Send + Sync + 'static {
	/// Get current immutable memtable count and L0 file count.
	fn get_stall_counts(&self) -> StallCounts;
}

/// Information about a write stall event.
#[derive(Debug, Clone)]
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
pub struct WriteStallController {
	/// Notification for when stall conditions clear
	stall_cleared: Notify,

	/// Current stall state (for fast-path check)
	is_stalled: AtomicBool,

	/// Shutdown flag - checked in stall loop for graceful exit
	shutdown: AtomicBool,

	/// Provider for live stall count readings
	provider: Arc<dyn WriteStallCountProvider>,

	/// Static thresholds configured at startup
	thresholds: StallThresholds,
}

impl WriteStallController {
	pub fn new(provider: Arc<dyn WriteStallCountProvider>, thresholds: StallThresholds) -> Self {
		Self {
			stall_cleared: Notify::new(),
			is_stalled: AtomicBool::new(false),
			shutdown: AtomicBool::new(false),
			provider,
			thresholds,
		}
	}

	/// Check stall conditions and wait if stalled. Called before each write.
	///
	/// Returns `Ok(Some(WriteStallInfo))` if was stalled and then cleared,
	/// `Ok(None)` if not stalled, `Err(Error::PipelineStall)` on shutdown.
	///
	/// Re-reads counts each iteration because background work (flushes,
	/// compactions) may complete while waiting.
	///
	/// IMPORTANT: The `Notified` future is created at the start of each loop
	/// iteration, BEFORE checking conditions. This ensures we receive any
	/// `notify_waiters()` calls that happen after we register but before we
	/// check. Per tokio docs: "The Notified future is guaranteed to receive
	/// wakeups from notify_waiters() as soon as it has been created."
	pub async fn check(&self) -> Result<Option<WriteStallInfo>> {
		let mut stall_start: Option<Instant> = None;
		let mut stall_reason: Option<WriteStallReason> = None;
		let mut stall_value: usize = 0;
		let mut stall_threshold: usize = 0;

		loop {
			// Create Notified FIRST to register for wakeups.
			// Any notify_waiters() call after this point will wake us.
			let notified = self.stall_cleared.notified();

			// Check shutdown
			if self.shutdown.load(Ordering::Acquire) {
				if stall_reason.is_some() {
					self.is_stalled.store(false, Ordering::Release);
				}
				return Err(Error::PipelineStall);
			}

			// Re-read counts (now any notify_waiters() after notified creation will wake us)
			let counts = self.provider.get_stall_counts();

			// Check if NOT hard-stalled
			if counts.immutable_memtables < self.thresholds.memtable_limit
				&& counts.l0_files < self.thresholds.l0_file_limit
			{
				// If we were stalled, clean up and return
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

				// Proactive Write Pacing: apply microsecond delays if above soft limits
				let mut pacing_micros: u64 = 0;
				if counts.immutable_memtables > self.thresholds.memtable_soft_limit {
					let excess = counts.immutable_memtables - self.thresholds.memtable_soft_limit;
					pacing_micros = pacing_micros.max((excess as u64) * 200);
				}
				if counts.l0_files > self.thresholds.l0_file_soft_limit {
					let excess = counts.l0_files - self.thresholds.l0_file_soft_limit;
					pacing_micros = pacing_micros.max((excess as u64) * 100);
				}

				if pacing_micros > 0 {
					tokio::time::sleep(Duration::from_micros(pacing_micros)).await;
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

			// Wait
			notified.await;
		}
	}

	/// Signal that stall conditions may have changed.
	/// Called after flush or compaction completes.
	pub fn signal_work_done(&self) {
		self.stall_cleared.notify_waiters();
	}

	/// Signal shutdown - wakes all stalled writers to exit.
	pub fn signal_shutdown(&self) {
		self.shutdown.store(true, Ordering::Release);
		self.stall_cleared.notify_waiters();
	}

	/// Fast check if currently stalled (for metrics).
	#[cfg(test)]
	pub fn is_stalled(&self) -> bool {
		self.is_stalled.load(Ordering::Acquire)
	}
}
