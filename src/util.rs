use std::fmt::Debug;
use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
use std::time::SystemTime;

/// Defines the logical clock that SurrealKV will use to measure
/// commit timestamps and retention periods checks.
pub trait LogicalClock: Debug + Send + Sync {
	/// Returns a timestamp (typically measured in nanoseconds since the unix epoch).
	/// Must return monotonically increasing numbers.
	fn now(&self) -> u64;
}

/// A logical clock implementation that wraps the system clock
/// and returns the number of nanoseconds since the Unix epoch.
#[derive(Debug)]
pub struct DefaultLogicalClock {
	last_ts: AtomicI64,
}

impl Default for DefaultLogicalClock {
	fn default() -> Self {
		Self::new()
	}
}

impl DefaultLogicalClock {
	pub fn new() -> Self {
		Self {
			last_ts: AtomicI64::new(i64::MIN),
		}
	}
}

impl LogicalClock for DefaultLogicalClock {
	fn now(&self) -> u64 {
		let current_ts =
			SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_nanos()
				as i64;
		self.last_ts.fetch_max(current_ts, AtomicOrdering::SeqCst);
		self.last_ts.load(AtomicOrdering::SeqCst) as u64
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
}

#[cfg(test)]
impl LogicalClock for MockLogicalClock {
	fn now(&self) -> u64 {
		self.current_tick.load(AtomicOrdering::SeqCst) as u64
	}
}
