//! Zero-allocation telemetry and latency flight recorder.
//!
//! Provides lock-free atomic latency tracking and metrics without
//! allocation or locking on critical database read/write paths.

use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonically increasing atomic latency bucket counters.
/// Uses powers of two exponential bucketing: <1us, <2us, <4us, <8us, ..., <65ms, >=65ms.
#[derive(Debug, Default)]
pub struct LatencyHistogram {
	/// Buckets covering 1us, 2us, 4us, 8us, 16us, 32us, 64us, 128us, 256us, 512us,
	/// 1ms, 2ms, 4ms, 8ms, 16ms, 32ms, >=64ms (17 buckets)
	buckets: [AtomicU64; 17],
	count: AtomicU64,
	sum_us: AtomicU64,
}

impl LatencyHistogram {
	pub const fn new() -> Self {
		// Initialize atomic arrays
		#[allow(clippy::declare_interior_mutable_const)]
		const ZERO: AtomicU64 = AtomicU64::new(0);
		Self {
			buckets: [ZERO; 17],
			count: AtomicU64::new(0),
			sum_us: AtomicU64::new(0),
		}
	}

	/// Records an observed duration in microseconds.
	#[inline]
	pub fn record(&self, duration_us: u64) {
		self.count.fetch_add(1, Ordering::Relaxed);
		self.sum_us.fetch_add(duration_us, Ordering::Relaxed);

		let bucket_idx = if duration_us == 0 {
			0
		} else {
			let leading_zeros = duration_us.leading_zeros();
			// 64 - leading_zeros gives bit position (1..=64)
			let bit = 64 - leading_zeros;
			(bit as usize).min(16)
		};

		self.buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
	}

	/// Returns total recorded samples.
	#[inline]
	pub fn count(&self) -> u64 {
		self.count.load(Ordering::Relaxed)
	}

	/// Returns total sum in microseconds.
	#[inline]
	pub fn sum_us(&self) -> u64 {
		self.sum_us.load(Ordering::Relaxed)
	}

	/// Estimates percentile (e.g. 50.0, 99.0, 99.9) in microseconds.
	pub fn percentile(&self, p: f64) -> u64 {
		let total = self.count.load(Ordering::Relaxed);
		if total == 0 {
			return 0;
		}

		let rank = ((p / 100.0) * total as f64).round() as u64;
		let mut cumulative = 0u64;

		for (i, b) in self.buckets.iter().enumerate() {
			cumulative += b.load(Ordering::Relaxed);
			if cumulative >= rank {
				return 1u64 << i;
			}
		}

		1u64 << 16
	}
}

/// Global telemetry flight recorder tracking key operational metrics.
#[derive(Debug, Default)]
pub struct FlightRecorder {
	/// Commit latency histogram in microseconds
	pub commit_latency: LatencyHistogram,
	/// Point read latency histogram in microseconds
	pub read_latency: LatencyHistogram,
	/// Background WAL flush latency in microseconds
	pub wal_flush_latency: LatencyHistogram,
	/// Total bytes written to WAL
	pub wal_bytes_written: AtomicU64,
	/// Total bytes read from SSTables
	pub sst_bytes_read: AtomicU64,
	/// Cache hits in block cache
	pub cache_hits: AtomicU64,
	/// Cache misses in block cache
	pub cache_misses: AtomicU64,
	/// Corrupted blocks detected by scrubber
	pub corrupted_blocks_detected: AtomicU64,
}

impl FlightRecorder {
	pub fn new() -> Self {
		Self::default()
	}

	#[inline]
	pub fn record_cache_hit(&self) {
		self.cache_hits.fetch_add(1, Ordering::Relaxed);
	}

	#[inline]
	pub fn record_cache_miss(&self) {
		self.cache_misses.fetch_add(1, Ordering::Relaxed);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_latency_histogram() {
		let hist = LatencyHistogram::new();
		assert_eq!(hist.count(), 0);

		hist.record(10);
		hist.record(20);
		hist.record(30);

		assert_eq!(hist.count(), 3);
		assert_eq!(hist.sum_us(), 60);

		let p50 = hist.percentile(50.0);
		assert!(p50 > 0);
	}
}
