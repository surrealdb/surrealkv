//! Write stall controller for managing backpressure when compaction can't keep up.
//!
//! Provides adaptive rate limiting through RAII guards that automatically release
//! when dropped, similar to Mutex/RwLock guards in Rust's standard library.

use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Constants for rate limiting
const MICROS_PER_SECOND: u64 = 1_000_000;
const MICROS_PER_REFILL: u64 = 1_000; // 1ms

/// Rate adjustment ratios (verified from RocksDB)
pub const INC_SLOWDOWN_RATIO: f64 = 0.8; // Slow by 20%
pub const NEAR_STOP_SLOWDOWN_RATIO: f64 = 0.6; // Aggressive slow by 40%

/// Write stall condition
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStallCondition {
	/// Normal operation, no stall
	Normal,
	/// Writes are being rate-limited
	Delayed,
	/// Writes are completely stopped
	Stopped,
}

/// Cause of write stall
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteStallCause {
	/// Too many immutable memtables waiting for flush
	MemtableLimit,
	/// Too many L0 files waiting for compaction
	L0FileCountLimit,
	/// No stall condition
	None,
}

/// Recalculate write stall conditions and update guard.
///
/// This is the SINGLE function that both lsm.rs and compactor.rs call after
/// flush or compaction to check if stall conditions have cleared or changed.
///
/// # Parameters
/// - `num_immutable_memtables`: Current count of immutable memtables waiting to flush
/// - `num_l0_files`: Current count of L0 SSTable files
/// - `max_write_buffer_number`: Maximum allowed immutable memtables before stopping
/// - `level0_slowdown_writes_trigger`: L0 file count that triggers write slowdown
/// - `level0_stop_writes_trigger`: L0 file count that stops writes completely
/// - `write_controller`: The write controller to get/set guards
/// - `guard_holder`: The guard holder to update
///
/// # Returns
/// Ok(WriteStallCondition) indicating the new stall state
pub fn recalculate_write_stall_conditions(
	num_immutable_memtables: usize,
	num_l0_files: usize,
	max_write_buffer_number: usize,
	level0_slowdown_writes_trigger: usize,
	level0_stop_writes_trigger: usize,
	write_controller: &Arc<WriteController>,
	guard_holder: &mut parking_lot::MutexGuard<Option<WriteStallGuard>>,
) -> WriteStallCondition {
	// Check STOP conditions first (most severe)
	let (condition, cause) = if num_immutable_memtables >= max_write_buffer_number {
		(WriteStallCondition::Stopped, WriteStallCause::MemtableLimit)
	} else if num_l0_files >= level0_stop_writes_trigger {
		(WriteStallCondition::Stopped, WriteStallCause::L0FileCountLimit)
	} else if max_write_buffer_number > 2 && num_immutable_memtables >= max_write_buffer_number - 1
	{
		(WriteStallCondition::Delayed, WriteStallCause::MemtableLimit)
	} else if num_l0_files >= level0_slowdown_writes_trigger {
		(WriteStallCondition::Delayed, WriteStallCause::L0FileCountLimit)
	} else {
		(WriteStallCondition::Normal, WriteStallCause::None)
	};

	let was_stopped = write_controller.is_stopped();

	// Update guard based on condition
	match (condition, cause) {
		(WriteStallCondition::Stopped, WriteStallCause::MemtableLimit) => {
			**guard_holder = Some(WriteStallGuard::Stop(write_controller.acquire_stop_guard()));
			log::warn!(
				"Stopping writes: {} immutable memtables (max: {})",
				num_immutable_memtables,
				max_write_buffer_number
			);
		}
		(WriteStallCondition::Stopped, WriteStallCause::L0FileCountLimit) => {
			**guard_holder = Some(WriteStallGuard::Stop(write_controller.acquire_stop_guard()));
			log::warn!(
				"Stopping writes: {} L0 files (max: {})",
				num_l0_files,
				level0_stop_writes_trigger
			);
		}
		(WriteStallCondition::Delayed, _cause) => {
			let write_rate = calculate_delayed_write_rate(write_controller, was_stopped);
			**guard_holder =
				Some(WriteStallGuard::Delay(write_controller.acquire_delay_guard(write_rate)));
			log::warn!("Delaying writes at {} bytes/sec", write_rate);
		}
		(WriteStallCondition::Stopped, WriteStallCause::None) => {
			// This shouldn't happen in practice, but handle it for completeness
			log::warn!("Unexpected: WriteStallCondition::Stopped with WriteStallCause::None");
			**guard_holder = None;
		}
		(WriteStallCondition::Normal, _) => {
			**guard_holder = None; // Release guard (drops and clears stall)
		}
	}

	condition
}

/// Calculate delayed write rate with adaptive adjustment
fn calculate_delayed_write_rate(write_controller: &Arc<WriteController>, was_stopped: bool) -> u64 {
	let max_rate = write_controller.max_delayed_write_rate();
	let current_rate = write_controller.delayed_write_rate();
	const MIN_WRITE_RATE: u64 = 16 * 1024; // 16KB/s

	if was_stopped {
		// Aggressive slowdown if just recovered from stop
		((current_rate as f64 * NEAR_STOP_SLOWDOWN_RATIO) as u64).max(MIN_WRITE_RATE)
	} else if write_controller.needs_delay() {
		// Adaptive adjustment: gradually slow down
		((current_rate as f64 * INC_SLOWDOWN_RATIO) as u64).max(MIN_WRITE_RATE)
	} else {
		max_rate
	}
}

/// Credit state for rate limiting
struct CreditState {
	credit_in_bytes: u64,
	next_refill_time: u64,
}

impl CreditState {
	fn new() -> Self {
		Self {
			credit_in_bytes: 0,
			next_refill_time: 0,
		}
	}
}

/// Write controller managing write stalls
pub struct WriteController {
	total_stopped: AtomicI32,
	total_delayed: AtomicI32,
	delayed_write_rate: AtomicU64,
	max_delayed_write_rate: AtomicU64,
	credit_state: Mutex<CreditState>,
	// Statistics
	total_stops: AtomicU64,
	total_delays: AtomicU64,
	total_delay_micros: AtomicU64,
}

impl WriteController {
	/// Create a new WriteController
	pub fn new(delayed_write_rate: u64, max_delayed_write_rate: u64) -> Self {
		Self {
			total_stopped: AtomicI32::new(0),
			total_delayed: AtomicI32::new(0),
			delayed_write_rate: AtomicU64::new(delayed_write_rate),
			max_delayed_write_rate: AtomicU64::new(max_delayed_write_rate),
			credit_state: Mutex::new(CreditState::new()),
			total_stops: AtomicU64::new(0),
			total_delays: AtomicU64::new(0),
			total_delay_micros: AtomicU64::new(0),
		}
	}

	/// Acquire a stop guard that blocks all writes
	pub fn acquire_stop_guard(self: &Arc<Self>) -> StopGuard {
		self.total_stopped.fetch_add(1, Ordering::Relaxed);
		self.total_stops.fetch_add(1, Ordering::Relaxed);
		StopGuard {
			_controller: Arc::clone(self),
		}
	}

	/// Acquire a delay guard for rate-limited writes
	pub fn acquire_delay_guard(self: &Arc<Self>, write_rate: u64) -> DelayGuard {
		if self.total_delayed.fetch_add(1, Ordering::Relaxed) == 0 {
			// Starting delay, reset counters
			let mut state = self.credit_state.lock().unwrap();
			state.next_refill_time = 0;
			state.credit_in_bytes = 0;
		}
		self.set_delayed_write_rate(write_rate);
		self.total_delays.fetch_add(1, Ordering::Relaxed);

		DelayGuard {
			_controller: Arc::clone(self),
		}
	}

	/// Check if writes are stopped
	pub fn is_stopped(&self) -> bool {
		self.total_stopped.load(Ordering::Relaxed) > 0
	}

	/// Check if writes need delay
	pub fn needs_delay(&self) -> bool {
		self.total_delayed.load(Ordering::Relaxed) > 0
	}

	/// Calculate delay in microseconds for given number of bytes
	/// Credit-based rate limiter algorithm adapted from RocksDB
	pub fn calculate_delay(&self, num_bytes: u64) -> u64 {
		if self.total_stopped.load(Ordering::Relaxed) > 0 {
			return 0;
		}
		if self.total_delayed.load(Ordering::Relaxed) == 0 {
			return 0;
		}

		let mut state = self.credit_state.lock().unwrap();

		// Check if we have enough credits
		if state.credit_in_bytes >= num_bytes {
			state.credit_in_bytes -= num_bytes;
			return 0;
		}

		let time_now = Self::now_micros_monotonic();
		let delayed_write_rate = self.delayed_write_rate.load(Ordering::Relaxed);

		if state.next_refill_time == 0 {
			// Start with initial allotment
			state.next_refill_time = time_now;
		}

		if state.next_refill_time <= time_now {
			// Refill based on time interval plus any extra elapsed
			let elapsed = time_now - state.next_refill_time + MICROS_PER_REFILL;
			state.credit_in_bytes += ((elapsed as f64 / MICROS_PER_SECOND as f64
				* delayed_write_rate as f64)
				+ 0.999999) as u64;
			state.next_refill_time = time_now + MICROS_PER_REFILL;

			if state.credit_in_bytes >= num_bytes {
				// Avoid delay if possible
				state.credit_in_bytes -= num_bytes;
				return 0;
			}
		}

		// We need to delay to avoid exceeding write rate
		let bytes_over_budget = num_bytes.saturating_sub(state.credit_in_bytes);
		let needed_delay = ((bytes_over_budget as f64 / delayed_write_rate as f64)
			* MICROS_PER_SECOND as f64) as u64;

		state.credit_in_bytes = 0;
		state.next_refill_time += needed_delay;

		// Minimum delay of refill interval
		let delay =
			std::cmp::max(state.next_refill_time.saturating_sub(time_now), MICROS_PER_REFILL);

		self.total_delay_micros.fetch_add(delay, Ordering::Relaxed);
		delay
	}

	/// Set delayed write rate
	pub fn set_delayed_write_rate(&self, write_rate: u64) {
		let rate = if write_rate == 0 {
			1
		} else {
			let max_rate = self.max_delayed_write_rate.load(Ordering::Relaxed);
			std::cmp::min(write_rate, max_rate)
		};
		self.delayed_write_rate.store(rate, Ordering::Relaxed);
	}

	/// Get current delayed write rate
	pub fn delayed_write_rate(&self) -> u64 {
		self.delayed_write_rate.load(Ordering::Relaxed)
	}

	/// Get max delayed write rate
	pub fn max_delayed_write_rate(&self) -> u64 {
		self.max_delayed_write_rate.load(Ordering::Relaxed)
	}

	/// Get total number of stops
	#[cfg(test)]
	pub fn get_total_stops(&self) -> u64 {
		self.total_stops.load(Ordering::Relaxed)
	}

	/// Get total number of delays
	#[cfg(test)]
	pub fn get_total_delays(&self) -> u64 {
		self.total_delays.load(Ordering::Relaxed)
	}

	fn now_micros_monotonic() -> u64 {
		SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
	}

	fn release_stop(&self) {
		self.total_stopped.fetch_sub(1, Ordering::Relaxed);
	}

	fn release_delay(&self) {
		self.total_delayed.fetch_sub(1, Ordering::Relaxed);
	}
}

/// RAII guard for write stall conditions (released when dropped)
pub enum WriteStallGuard {
	Stop(StopGuard),
	Delay(DelayGuard),
}

/// Guard that stops all writes until dropped
pub struct StopGuard {
	_controller: Arc<WriteController>,
}

impl Drop for StopGuard {
	fn drop(&mut self) {
		self._controller.release_stop();
	}
}

/// Guard that rate-limits writes until dropped
pub struct DelayGuard {
	_controller: Arc<WriteController>,
}

impl Drop for DelayGuard {
	fn drop(&mut self) {
		self._controller.release_delay();
	}
}

#[cfg(test)]
mod tests {
	use std::thread;
	use std::time::Duration;

	use parking_lot::Mutex;

	use super::*;

	#[test]
	fn test_write_controller_basic() {
		let controller = Arc::new(WriteController::new(1024 * 1024, 1024 * 1024));

		assert!(!controller.is_stopped());
		assert!(!controller.needs_delay());

		let _guard = controller.acquire_stop_guard();
		assert!(controller.is_stopped());
		drop(_guard);
		assert!(!controller.is_stopped());
	}

	#[test]
	fn test_delay_guard() {
		let controller = Arc::new(WriteController::new(1024 * 1024, 1024 * 1024));

		assert!(!controller.needs_delay());

		let _guard = controller.acquire_delay_guard(512 * 1024);
		assert!(controller.needs_delay());
		assert_eq!(controller.delayed_write_rate(), 512 * 1024);

		drop(_guard);
		assert!(!controller.needs_delay());
	}

	#[test]
	fn test_calculate_delay() {
		let controller = Arc::new(WriteController::new(1024 * 1024, 1024 * 1024));

		// No delay without guard
		let delay = controller.calculate_delay(1024);
		assert_eq!(delay, 0);

		// With delay guard
		let _guard = controller.acquire_delay_guard(100 * 1024); // 100KB/s
		thread::sleep(Duration::from_millis(10));

		// Writing 10KB should cause delay
		let delay = controller.calculate_delay(10 * 1024);
		assert!(delay > 0);
	}

	#[test]
	fn test_statistics() {
		let controller = Arc::new(WriteController::new(1024 * 1024, 1024 * 1024));

		assert_eq!(controller.get_total_stops(), 0);
		assert_eq!(controller.get_total_delays(), 0);

		let _stop_guard = controller.acquire_stop_guard();
		assert_eq!(controller.get_total_stops(), 1);

		let _delay_guard = controller.acquire_delay_guard(512 * 1024);
		assert_eq!(controller.get_total_delays(), 1);
	}

	#[test]
	fn test_rate_adjustment() {
		let controller = Arc::new(WriteController::new(1024 * 1024, 1024 * 1024));

		// Acquire delay guard and verify rate
		let _guard = controller.acquire_delay_guard(800 * 1024);
		assert_eq!(controller.delayed_write_rate(), 800 * 1024);
		assert!(controller.needs_delay());

		drop(_guard);
		assert!(!controller.needs_delay());
	}

	#[test]
	fn test_stop_supersedes_delay() {
		let controller = Arc::new(WriteController::new(1024 * 1024, 1024 * 1024));

		let _delay_guard = controller.acquire_delay_guard(512 * 1024);
		assert!(controller.needs_delay());
		assert!(!controller.is_stopped());

		let _stop_guard = controller.acquire_stop_guard();
		assert!(controller.is_stopped());
		// Stop supersedes delay - calculate_delay returns 0 when stopped
		assert_eq!(controller.calculate_delay(1024), 0);
	}

	#[test]
	fn test_adaptive_rate_no_delay() {
		let controller = Arc::new(WriteController::new(1_000_000, 2_000_000));

		// When no delay active, should return max rate
		let rate = calculate_delayed_write_rate(&controller, false);
		assert_eq!(rate, 2_000_000);
	}

	#[test]
	fn test_adaptive_rate_during_delay() {
		let controller = Arc::new(WriteController::new(1_000_000, 1_000_000));
		let _guard = controller.acquire_delay_guard(1_000_000);

		// When already delayed, should slow down by 20%
		let rate = calculate_delayed_write_rate(&controller, false);
		assert_eq!(rate, 800_000); // 1MB * 0.8
	}

	#[test]
	fn test_adaptive_rate_minimum_enforcement() {
		let controller = Arc::new(WriteController::new(20_000, 20_000));
		let _guard = controller.acquire_delay_guard(20_000);

		// Even with 20% slowdown (would be 16KB), enforce minimum
		let rate = calculate_delayed_write_rate(&controller, false);
		assert_eq!(rate, 16 * 1024); // MIN_WRITE_RATE

		// Aggressive slowdown would go below minimum
		let rate_after_stop = calculate_delayed_write_rate(&controller, true);
		assert_eq!(rate_after_stop, 16 * 1024); // MIN_WRITE_RATE
	}

	#[test]
	fn test_adaptive_rate_cascading_slowdown() {
		let controller = Arc::new(WriteController::new(10_000_000, 10_000_000));

		// Simulate cascading slowdown
		let _guard = controller.acquire_delay_guard(10_000_000);

		// First slowdown: 10MB * 0.8 = 8MB
		let rate1 = calculate_delayed_write_rate(&controller, false);
		assert_eq!(rate1, 8_000_000);

		// Update rate and check again
		controller.set_delayed_write_rate(rate1);
		let rate2 = calculate_delayed_write_rate(&controller, false);
		assert_eq!(rate2, 6_400_000); // 8MB * 0.8

		// Third slowdown: 6.4MB * 0.8 = 5.12MB
		controller.set_delayed_write_rate(rate2);
		let rate3 = calculate_delayed_write_rate(&controller, false);
		assert_eq!(rate3, 5_120_000);
	}

	#[test]
	fn test_normal_no_stall() {
		let controller = Arc::new(WriteController::new(1_000_000, 1_000_000));
		let guard_holder = Arc::new(Mutex::new(None));
		let mut guard = guard_holder.lock();

		let condition = recalculate_write_stall_conditions(
			0,  // num_immutable_memtables
			0,  // num_l0_files
			5,  // max_write_buffer_number
			8,  // level0_slowdown_writes_trigger
			12, // level0_stop_writes_trigger
			&controller,
			&mut guard,
		);

		assert_eq!(condition, WriteStallCondition::Normal);
		assert!(guard.is_none());
		assert!(!controller.is_stopped());
		assert!(!controller.needs_delay());
	}

	#[test]
	fn test_stop_memtable_limit_exact() {
		let controller = Arc::new(WriteController::new(1_000_000, 1_000_000));
		let guard_holder = Arc::new(Mutex::new(None));
		let mut guard = guard_holder.lock();

		let condition = recalculate_write_stall_conditions(
			5,  // num_immutable_memtables == max
			0,  // num_l0_files
			5,  // max_write_buffer_number
			8,  // level0_slowdown_writes_trigger
			12, // level0_stop_writes_trigger
			&controller,
			&mut guard,
		);

		assert_eq!(condition, WriteStallCondition::Stopped);
		assert!(guard.is_some());
		assert!(controller.is_stopped());
		assert!(!controller.needs_delay());
	}

	#[test]
	fn test_stop_memtable_limit_exceeded() {
		let controller = Arc::new(WriteController::new(1_000_000, 1_000_000));
		let guard_holder = Arc::new(Mutex::new(None));
		let mut guard = guard_holder.lock();

		let condition = recalculate_write_stall_conditions(
			10, // num_immutable_memtables > max
			0,  // num_l0_files
			5,  // max_write_buffer_number
			8,  // level0_slowdown_writes_trigger
			12, // level0_stop_writes_trigger
			&controller,
			&mut guard,
		);

		assert_eq!(condition, WriteStallCondition::Stopped);
		assert!(controller.is_stopped());
	}

	#[test]
	fn test_stop_l0_limit_exact() {
		let controller = Arc::new(WriteController::new(1_000_000, 1_000_000));
		let guard_holder = Arc::new(Mutex::new(None));
		let mut guard = guard_holder.lock();

		let condition = recalculate_write_stall_conditions(
			0,  // num_immutable_memtables
			12, // num_l0_files == stop trigger
			5,  // max_write_buffer_number
			8,  // level0_slowdown_writes_trigger
			12, // level0_stop_writes_trigger
			&controller,
			&mut guard,
		);

		assert_eq!(condition, WriteStallCondition::Stopped);
		assert!(controller.is_stopped());
	}

	#[test]
	fn test_stop_l0_limit_exceeded() {
		let controller = Arc::new(WriteController::new(1_000_000, 1_000_000));
		let guard_holder = Arc::new(Mutex::new(None));
		let mut guard = guard_holder.lock();

		let condition = recalculate_write_stall_conditions(
			0,  // num_immutable_memtables
			20, // num_l0_files > stop trigger
			5,  // max_write_buffer_number
			8,  // level0_slowdown_writes_trigger
			12, // level0_stop_writes_trigger
			&controller,
			&mut guard,
		);

		assert_eq!(condition, WriteStallCondition::Stopped);
		assert!(controller.is_stopped());
	}

	#[test]
	fn test_memtable_takes_priority_over_l0() {
		// When both conditions met, memtable should take priority
		let controller = Arc::new(WriteController::new(1_000_000, 1_000_000));
		let guard_holder = Arc::new(Mutex::new(None));
		let mut guard = guard_holder.lock();

		let condition = recalculate_write_stall_conditions(
			5,  // num_immutable_memtables == max (STOP)
			12, // num_l0_files == stop trigger (STOP)
			5,  // max_write_buffer_number
			8,  // level0_slowdown_writes_trigger
			12, // level0_stop_writes_trigger
			&controller,
			&mut guard,
		);

		assert_eq!(condition, WriteStallCondition::Stopped);
		// Both trigger stop, memtable checked first in code
	}

	#[test]
	fn test_delay_memtable_limit() {
		let controller = Arc::new(WriteController::new(1_000_000, 1_000_000));
		let guard_holder = Arc::new(Mutex::new(None));
		let mut guard = guard_holder.lock();

		let condition = recalculate_write_stall_conditions(
			4,  // num_immutable_memtables == max - 1
			0,  // num_l0_files
			5,  // max_write_buffer_number (> 2)
			8,  // level0_slowdown_writes_trigger
			12, // level0_stop_writes_trigger
			&controller,
			&mut guard,
		);

		assert_eq!(condition, WriteStallCondition::Delayed);
		assert!(guard.is_some());
		assert!(!controller.is_stopped());
		assert!(controller.needs_delay());
	}

	#[test]
	fn test_delay_memtable_not_triggered_when_max_le_2() {
		// Special case: delay not triggered when max_write_buffer_number <= 2
		let controller = Arc::new(WriteController::new(1_000_000, 1_000_000));
		let guard_holder = Arc::new(Mutex::new(None));
		let mut guard = guard_holder.lock();

		let condition = recalculate_write_stall_conditions(
			1,  // num_immutable_memtables == max - 1
			0,  // num_l0_files
			2,  // max_write_buffer_number == 2
			8,  // level0_slowdown_writes_trigger
			12, // level0_stop_writes_trigger
			&controller,
			&mut guard,
		);

		// Should NOT delay because max_write_buffer_number <= 2
		assert_eq!(condition, WriteStallCondition::Normal);
		assert!(!controller.needs_delay());
	}

	#[test]
	fn test_delay_l0_slowdown() {
		let controller = Arc::new(WriteController::new(1_000_000, 1_000_000));
		let guard_holder = Arc::new(Mutex::new(None));
		let mut guard = guard_holder.lock();

		let condition = recalculate_write_stall_conditions(
			0,  // num_immutable_memtables
			8,  // num_l0_files == slowdown trigger
			5,  // max_write_buffer_number
			8,  // level0_slowdown_writes_trigger
			12, // level0_stop_writes_trigger
			&controller,
			&mut guard,
		);

		assert_eq!(condition, WriteStallCondition::Delayed);
		assert!(controller.needs_delay());
	}

	#[test]
	fn test_delay_l0_above_slowdown() {
		let controller = Arc::new(WriteController::new(1_000_000, 1_000_000));
		let guard_holder = Arc::new(Mutex::new(None));
		let mut guard = guard_holder.lock();

		let condition = recalculate_write_stall_conditions(
			0,  // num_immutable_memtables
			10, // num_l0_files > slowdown trigger
			5,  // max_write_buffer_number
			8,  // level0_slowdown_writes_trigger
			12, // level0_stop_writes_trigger
			&controller,
			&mut guard,
		);

		assert_eq!(condition, WriteStallCondition::Delayed);
		assert!(controller.needs_delay());
	}

	#[test]
	fn test_transition_from_stop_to_normal() {
		let controller = Arc::new(WriteController::new(1_000_000, 1_000_000));
		let guard_holder = Arc::new(Mutex::new(None));

		// First, trigger stop
		{
			let mut guard = guard_holder.lock();
			recalculate_write_stall_conditions(5, 0, 5, 8, 12, &controller, &mut guard);
			assert!(controller.is_stopped());
		}

		// Then, clear condition
		{
			let mut guard = guard_holder.lock();
			let condition =
				recalculate_write_stall_conditions(0, 0, 5, 8, 12, &controller, &mut guard);
			assert_eq!(condition, WriteStallCondition::Normal);
			assert!(!controller.is_stopped());
			assert!(guard.is_none());
		}
	}

	#[test]
	fn test_transition_from_delay_to_normal() {
		let controller = Arc::new(WriteController::new(1_000_000, 1_000_000));
		let guard_holder = Arc::new(Mutex::new(None));

		// First, trigger delay
		{
			let mut guard = guard_holder.lock();
			recalculate_write_stall_conditions(4, 0, 5, 8, 12, &controller, &mut guard);
			assert!(controller.needs_delay());
		}

		// Then, clear condition
		{
			let mut guard = guard_holder.lock();
			let condition =
				recalculate_write_stall_conditions(0, 0, 5, 8, 12, &controller, &mut guard);
			assert_eq!(condition, WriteStallCondition::Normal);
			assert!(!controller.needs_delay());
			assert!(guard.is_none());
		}
	}

	#[test]
	fn test_adaptive_rate_on_first_delay() {
		let controller = Arc::new(WriteController::new(10_000_000, 10_000_000));
		let guard_holder = Arc::new(Mutex::new(None));
		let mut guard = guard_holder.lock();

		// Trigger delay for first time (not previously stopped)
		recalculate_write_stall_conditions(4, 0, 5, 8, 12, &controller, &mut guard);

		// Should use max_rate on first delay
		assert_eq!(controller.delayed_write_rate(), 10_000_000);
	}

	#[test]
	fn test_adaptive_rate_after_stop() {
		let controller = Arc::new(WriteController::new(10_000_000, 10_000_000));
		let guard_holder = Arc::new(Mutex::new(None));

		// First stop
		{
			let mut guard = guard_holder.lock();
			recalculate_write_stall_conditions(5, 0, 5, 8, 12, &controller, &mut guard);
			assert!(controller.is_stopped());
		}

		// Then delay (was_stopped = true)
		{
			let mut guard = guard_holder.lock();
			recalculate_write_stall_conditions(4, 0, 5, 8, 12, &controller, &mut guard);

			// Should apply aggressive slowdown: 10MB * 0.6 = 6MB
			assert_eq!(controller.delayed_write_rate(), 6_000_000);
		}
	}

	#[test]
	fn test_edge_case_all_zeros() {
		let controller = Arc::new(WriteController::new(1_000_000, 1_000_000));
		let guard_holder = Arc::new(Mutex::new(None));
		let mut guard = guard_holder.lock();

		let condition = recalculate_write_stall_conditions(0, 0, 0, 0, 0, &controller, &mut guard);

		// With max_write_buffer_number=0, even 0 immutable memtables triggers stop (0 >= 0)
		// This matches RocksDB behavior - it's a degenerate config that shouldn't happen in
		// practice
		assert_eq!(condition, WriteStallCondition::Stopped);
		assert!(controller.is_stopped());
	}

	#[test]
	fn test_edge_case_boundary_values() {
		let controller = Arc::new(WriteController::new(1_000_000, 1_000_000));
		let guard_holder = Arc::new(Mutex::new(None));
		let mut guard = guard_holder.lock();

		// One below stop threshold
		let condition = recalculate_write_stall_conditions(
			4,  // max - 1
			11, // stop - 1
			5,
			8,
			12,
			&controller,
			&mut guard,
		);

		// Should delay (memtable) not stop
		assert_eq!(condition, WriteStallCondition::Delayed);
	}
}
