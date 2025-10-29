use std::fmt::Debug;
#[cfg(test)]
use std::sync::atomic::AtomicI64;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use arc_swap::ArcSwap;

/// Defines the logical clock that SurrealKV will use to measure
/// commit timestamps and retention periods checks.
pub trait LogicalClock: Debug + Send + Sync {
	/// Returns a timestamp (typically measured in nanoseconds since the unix epoch).
	/// Must return monotonically increasing numbers.
	fn now(&self) -> u64;
}

/// A logical clock implementation that wraps the system clock
/// and returns the number of nanoseconds since the Unix epoch.
///
/// Uses a background thread to periodically sync with system time,
/// and fast monotonic `Instant::now()` for high-performance time queries
/// between syncs to avoid expensive syscalls on every commit.
pub struct DefaultLogicalClock {
	/// Monotonic counter for logical time
	timestamp: Arc<AtomicU64>,
	/// Reference time when oracle was last synced with system clock
	reference: Arc<ArcSwap<(u64, Instant)>>,
	/// Background sync control
	resync_enabled: Arc<AtomicBool>,
	resync_handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
	resync_interval: Duration,
}

impl std::fmt::Debug for DefaultLogicalClock {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("DefaultLogicalClock")
			.field("timestamp", &self.timestamp.load(AtomicOrdering::Relaxed))
			.field("resync_interval", &self.resync_interval)
			.finish()
	}
}

impl Default for DefaultLogicalClock {
	fn default() -> Self {
		Self::new()
	}
}

impl DefaultLogicalClock {
	pub fn new() -> Self {
		let reference_unix = Self::current_unix_ns();
		let reference_time = Instant::now();

		let clock = Self {
			timestamp: Arc::new(AtomicU64::new(reference_unix)),
			reference: Arc::new(ArcSwap::new(Arc::new((reference_unix, reference_time)))),
			resync_enabled: Arc::new(AtomicBool::new(true)),
			resync_handle: std::sync::Mutex::new(None),
			resync_interval: Duration::from_secs(1), // Resync every 1s
		};

		clock.start_resync_thread();
		clock
	}

	#[inline]
	fn current_unix_ns() -> u64 {
		SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_nanos()
			as u64
	}

	fn start_resync_thread(&self) {
		let timestamp = Arc::clone(&self.timestamp);
		let reference = Arc::clone(&self.reference);
		let resync_enabled = Arc::clone(&self.resync_enabled);
		let interval = self.resync_interval;

		let handle = std::thread::spawn(move || {
			while resync_enabled.load(AtomicOrdering::Acquire) {
				std::thread::park_timeout(interval);

				// Check flag again after waking to ensure immediate shutdown
				if !resync_enabled.load(AtomicOrdering::Acquire) {
					break;
				}

				let reference_unix = Self::current_unix_ns();
				let reference_time = Instant::now();

				// Update reference point
				reference.store(Arc::new((reference_unix, reference_time)));

				// Ensure timestamp is monotonic
				timestamp.fetch_max(reference_unix, AtomicOrdering::SeqCst);
			}
		});

		*self.resync_handle.lock().unwrap() = Some(handle);
	}
}

impl LogicalClock for DefaultLogicalClock {
	#[inline]
	fn now(&self) -> u64 {
		// Load the reference point
		let reference = self.reference.load();

		// Calculate elapsed time using monotonic clock (no syscall!)
		let estimated_ts = reference.0 + reference.1.elapsed().as_nanos() as u64;

		// Ensure monotonicity
		self.timestamp.fetch_max(estimated_ts, AtomicOrdering::SeqCst);
		self.timestamp.load(AtomicOrdering::SeqCst)
	}
}

impl Drop for DefaultLogicalClock {
	fn drop(&mut self) {
		self.resync_enabled.store(false, AtomicOrdering::Release);
		if let Some(handle) = self.resync_handle.lock().unwrap().take() {
			handle.thread().unpark();
			let _ = handle.join();
		}
	}
}

/// A mock logical clock implementation.
#[cfg(test)]
#[derive(Debug)]
pub struct MockLogicalClock {
	current_tick: AtomicI64,
}

#[cfg(test)]
impl Default for MockLogicalClock {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
impl MockLogicalClock {
	pub fn new() -> Self {
		Self {
			current_tick: AtomicI64::new(i64::MIN),
		}
	}

	pub fn with_timestamp(timestamp: u64) -> Self {
		Self {
			current_tick: AtomicI64::new(timestamp as i64),
		}
	}

	pub fn set_time(&self, timestamp: u64) {
		self.current_tick.store(timestamp as i64, AtomicOrdering::SeqCst);
	}
}

#[cfg(test)]
impl LogicalClock for MockLogicalClock {
	fn now(&self) -> u64 {
		self.current_tick.load(AtomicOrdering::SeqCst) as u64
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_default_clock_immediate_shutdown() {
		// Test that the clock shuts down quickly without waiting for full resync interval
		for _ in 0..5 {
			let start = Instant::now();
			{
				let _clock = DefaultLogicalClock::new();
				// Clock is dropped here
			}
			let elapsed = start.elapsed();

			// Should drop in well under 100ms (the resync interval)
			// Allow 50ms for slow CI systems
			assert!(
				elapsed < Duration::from_millis(50),
				"Clock took too long to drop: {:?}. Thread may not be shutting down properly.",
				elapsed
			);
		}
	}

	#[test]
	fn test_default_clock_monotonicity() {
		let clock = DefaultLogicalClock::new();

		let mut last_ts = clock.now();
		for _ in 0..100 {
			let current_ts = clock.now();
			assert!(current_ts >= last_ts, "Clock is not monotonic: {} < {}", current_ts, last_ts);
			last_ts = current_ts;
		}
	}

	#[test]
	fn test_default_clock_progresses() {
		let clock = DefaultLogicalClock::new();
		let ts1 = clock.now();

		std::thread::sleep(Duration::from_millis(10));

		let ts2 = clock.now();
		assert!(ts2 > ts1, "Clock did not progress over time: {} >= {}", ts1, ts2);
	}
}
