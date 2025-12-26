use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;

/// Defines the logical clock that SurrealKV will use to measure
/// commit timestamps and retention periods checks.
pub trait LogicalClock: Debug + Send + Sync {
	/// Returns a timestamp (typically measured in nanoseconds since the unix
	/// epoch). Must return monotonically increasing numbers.
	fn now(&self) -> u64;
}

/// A logical clock implementation that wraps the system clock
/// and returns the number of nanoseconds since the Unix epoch.
///
/// Uses a background thread to periodically sync with system time,
/// and fast monotonic `Instant::now()` for high-performance time queries
/// between syncs to avoid expensive syscalls on every commit.
pub struct DefaultLogicalClock {
	/// The inner strcuture of this clock
	inner: Arc<DefaultLogicalClockInner>,
}

impl Drop for DefaultLogicalClock {
	fn drop(&mut self) {
		self.shutdown();
	}
}

/// The inner structure of the timestamp oracle
pub struct DefaultLogicalClockInner {
	/// The latest monotonic counter for this oracle
	timestamp: AtomicU64,
	/// Reference time when this clock was last synced with system clock
	reference: ArcSwap<(u64, Instant)>,
	/// Specifies whether timestamp syncing is enabled in the background
	resync_enabled: AtomicBool,
	/// Stores a handle to the current timestamp syncing background thread
	resync_handle: Mutex<Option<JoinHandle<()>>>,
	/// Interval at which the clock resyncs with the system clock
	resync_interval: Duration,
}

impl Default for DefaultLogicalClock {
	fn default() -> Self {
		Self::new()
	}
}

impl std::fmt::Debug for DefaultLogicalClock {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("DefaultLogicalClock")
			.field("current_timestamp", &self.current_timestamp())
			.field("current_unix_ns", &Self::current_unix_ns())
			.field("current_time_ns", &self.current_time_ns())
			.finish()
	}
}

impl DefaultLogicalClock {
	pub fn new() -> Self {
		// Get the current unix time in nanoseconds
		let reference_unix = Self::current_unix_ns();
		// Get a new monotonically increasing clock
		let reference_time = Instant::now();
		// Return the current timestamp clock
		let clock = Self {
			inner: Arc::new(DefaultLogicalClockInner {
				timestamp: AtomicU64::new(reference_unix),
				reference: ArcSwap::new(Arc::new((reference_unix, reference_time))),
				resync_enabled: AtomicBool::new(true),
				resync_handle: Mutex::new(None),
				resync_interval: Duration::from_secs(1),
			}),
		};
		// Start up the resyncing thread
		clock.spawn_clock_sync();
		// Return the clock
		clock
	}

	/// Returns the current timestamp for this oracle
	#[inline]
	pub fn current_timestamp(&self) -> u64 {
		self.inner.timestamp.load(Ordering::Acquire)
	}

	/// Gets the current system time in nanoseconds since the Unix epoch
	#[inline]
	pub(crate) fn current_unix_ns() -> u64 {
		// Get the current system time
		let timestamp = SystemTime::now().duration_since(UNIX_EPOCH);
		// Count the nanoseconds since the Unix epoch
		timestamp.unwrap_or_default().as_nanos() as u64
	}

	/// Gets the current estimated time in nanoseconds since the Unix epoch
	#[inline]
	pub(crate) fn current_time_ns(&self) -> u64 {
		// Get the current reference time
		let reference = self.inner.reference.load();
		// Calculate the nanoseconds since the Unix epoch
		reference.0 + reference.1.elapsed().as_nanos() as u64
	}

	/// Shutdown the oracle resync, waiting for background threads to exit
	fn shutdown(&self) {
		// Disable timestamp resyncing
		self.inner.resync_enabled.store(false, Ordering::Release);
		// Wait for the timestamp resyncing thread to exit
		if let Some(handle) = self.inner.resync_handle.lock().unwrap().take() {
			handle.thread().unpark();
			handle.join().unwrap();
		}
	}

	/// Start the resyncing thread after creating the oracle
	fn spawn_clock_sync(&self) {
		// Clone the underlying clock inner
		let inner = Arc::clone(&self.inner);
		// Store the resync interval for the thread
		let interval = inner.resync_interval;
		// Spawn a new thread to handle timestamp resyncing
		let handle = std::thread::spawn(move || {
			// Check whether the timestamp resync process is enabled
			while inner.resync_enabled.load(Ordering::Acquire) {
				// Wait for a specified time interval
				std::thread::park_timeout(interval);
				// Get the current unix time in nanoseconds
				let reference_unix = Self::current_unix_ns();
				// Get a new monotonically increasing clock
				let reference_time = Instant::now();
				// Store the timestamp and monotonic instant
				inner.reference.store(Arc::new((reference_unix, reference_time)));
			}
		});
		// Store and track the thread handle
		*self.inner.resync_handle.lock().unwrap() = Some(handle);
	}
}

impl LogicalClock for DefaultLogicalClock {
	#[inline]
	fn now(&self) -> u64 {
		// Store the number of spins
		let mut spins = 0;
		// Loop until we reach the next incremental timestamp
		loop {
			// Get current time estimate
			let mut version = self.current_time_ns();
			// Ensure monotonicity via compare-and-swap loop
			let current = self.inner.timestamp.load(Ordering::Acquire);
			// Ensure version is greater than last timestamp
			if version <= current {
				version = current + 1;
			}
			// Try to update the timestamp
			if self
				.inner
				.timestamp
				.compare_exchange_weak(current, version, Ordering::AcqRel, Ordering::Acquire)
				.is_ok()
			{
				return version;
			}
			// Ensure the thread backs off when under contention
			if spins < 10 {
				std::hint::spin_loop();
			} else if spins < 100 {
				std::thread::yield_now();
			} else {
				std::thread::park_timeout(Duration::from_micros(10));
			}
			// Increase the number loop spins we have attempted
			spins += 1;
		}
	}
}

/// A mock logical clock implementation.
#[cfg(test)]
#[derive(Debug)]
pub struct MockLogicalClock {
	current_tick: std::sync::atomic::AtomicI64,
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
			current_tick: std::sync::atomic::AtomicI64::new(i64::MIN),
		}
	}

	pub fn with_timestamp(timestamp: u64) -> Self {
		Self {
			current_tick: std::sync::atomic::AtomicI64::new(timestamp as i64),
		}
	}

	pub fn set_time(&self, timestamp: u64) {
		self.current_tick.store(timestamp as i64, Ordering::SeqCst);
	}
}

#[cfg(test)]
impl LogicalClock for MockLogicalClock {
	fn now(&self) -> u64 {
		self.current_tick.load(Ordering::SeqCst) as u64
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_default_clock_immediate_shutdown() {
		// Test that the clock shuts down quickly without waiting for full resync
		// interval
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

	#[test]
	fn test_default_clock_concurrent_monotonicity() {
		use std::sync::Mutex as StdMutex;

		let clock = Arc::new(DefaultLogicalClock::new());
		let all_values = Arc::new(StdMutex::new(Vec::new()));
		let mut handles = vec![];

		// Spawn 10 threads, each getting 100 timestamps
		for i in 0..10 {
			let clock = Arc::clone(&clock);
			let all_values = Arc::clone(&all_values);

			let handle = std::thread::spawn(move || {
				for _ in 0..100 {
					let ts = clock.now();
					all_values.lock().unwrap().push((i, ts));
				}
			});

			handles.push(handle);
		}

		// Wait for all threads
		for handle in handles {
			handle.join().unwrap();
		}

		// Check that all values are unique and monotonically increasing when sorted
		let mut values = all_values.lock().unwrap();
		values.sort_by_key(|(_, ts)| *ts);

		let mut last = 0;
		for (thread_id, ts) in values.iter() {
			assert!(
				*ts > last,
				"Clock not monotonic: thread {} got {}, but previous was {}",
				thread_id,
				ts,
				last
			);
			last = *ts;
		}
	}

	#[test]
	fn test_default_clock_no_duplicates() {
		use std::collections::HashSet;

		let clock = Arc::new(DefaultLogicalClock::new());
		let seen = Arc::new(Mutex::new(HashSet::new()));
		let mut handles = vec![];

		// Spawn multiple threads to detect any duplicate timestamps
		for _ in 0..5 {
			let clock = Arc::clone(&clock);
			let seen = Arc::clone(&seen);

			let handle = std::thread::spawn(move || {
				for _ in 0..1000 {
					let ts = clock.now();
					let mut seen_guard = seen.lock().unwrap();
					assert!(seen_guard.insert(ts), "Duplicate timestamp: {}", ts);
				}
			});

			handles.push(handle);
		}

		for handle in handles {
			handle.join().unwrap();
		}
	}

	#[test]
	fn test_default_clock_stays_close_to_wall_time() {
		let clock = DefaultLogicalClock::new();

		// Get a timestamp
		let ts1 = clock.now();
		let wall1 = DefaultLogicalClock::current_unix_ns();

		// Clock should be within 1 second of wall time
		let diff1 = ts1.abs_diff(wall1);
		assert!(diff1 < 1_000_000_000, "Clock too far from wall time: {} ns difference", diff1);

		// Even after many calls under contention
		for _ in 0..10000 {
			clock.now();
		}

		let ts2 = clock.now();
		let wall2 = DefaultLogicalClock::current_unix_ns();
		let diff2 = ts2.abs_diff(wall2);

		// Should still be reasonably close (within 100ms even with forced increments)
		assert!(
			diff2 < 100_000_000,
			"Clock drifted too far from wall time: {} ns difference",
			diff2
		);
	}
}
