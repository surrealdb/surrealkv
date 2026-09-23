//! Global memory accounting and dynamic budget coordination.
//!
//! Prevents Out-Of-Memory (OOM) crashes by tracking memory consumption across
//! the active memtable, immutable memtable queue, commit ring buffer, and block cache,
//! enforcing a strict unified memory budget.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub const DEFAULT_MAX_MEMORY_BUDGET: usize = 512 * 1024 * 1024; // 512MB default

/// Controller coordinating global memory allocations across database subsystems.
#[derive(Debug)]
pub struct MemoryController {
	max_budget: usize,
	active_memtable_bytes: AtomicUsize,
	immutable_memtable_bytes: AtomicUsize,
	block_cache_bytes: AtomicUsize,
	ring_buffer_bytes: AtomicUsize,
}

impl Default for MemoryController {
	fn default() -> Self {
		Self::new(DEFAULT_MAX_MEMORY_BUDGET)
	}
}

impl MemoryController {
	pub fn new(max_budget: usize) -> Self {
		Self {
			max_budget: max_budget.max(16 * 1024 * 1024), // At least 16MB minimum
			active_memtable_bytes: AtomicUsize::new(0),
			immutable_memtable_bytes: AtomicUsize::new(0),
			block_cache_bytes: AtomicUsize::new(0),
			ring_buffer_bytes: AtomicUsize::new(0),
		}
	}

	/// Returns the maximum allowed memory budget in bytes.
	#[inline]
	pub fn max_budget(&self) -> usize {
		self.max_budget
	}

	/// Total memory currently allocated across all subsystems in bytes.
	#[inline]
	pub fn total_used(&self) -> usize {
		self.active_memtable_bytes.load(Ordering::Relaxed)
			+ self.immutable_memtable_bytes.load(Ordering::Relaxed)
			+ self.block_cache_bytes.load(Ordering::Relaxed)
			+ self.ring_buffer_bytes.load(Ordering::Relaxed)
	}

	/// Remaining memory budget in bytes before reaching `max_budget`.
	#[inline]
	pub fn remaining_budget(&self) -> usize {
		self.max_budget.saturating_sub(self.total_used())
	}

	/// Sets active memtable memory usage.
	#[inline]
	pub fn set_active_memtable_bytes(&self, bytes: usize) {
		self.active_memtable_bytes.store(bytes, Ordering::Relaxed);
	}

	/// Adds to active memtable memory usage.
	#[inline]
	pub fn add_active_memtable_bytes(&self, bytes: usize) {
		self.active_memtable_bytes.fetch_add(bytes, Ordering::Relaxed);
	}

	/// Sets immutable memtable memory usage.
	#[inline]
	pub fn set_immutable_memtable_bytes(&self, bytes: usize) {
		self.immutable_memtable_bytes.store(bytes, Ordering::Relaxed);
	}

	/// Sets block cache memory usage.
	#[inline]
	pub fn set_block_cache_bytes(&self, bytes: usize) {
		self.block_cache_bytes.store(bytes, Ordering::Relaxed);
	}

	/// Sets ring buffer memory usage.
	#[inline]
	pub fn set_ring_buffer_bytes(&self, bytes: usize) {
		self.ring_buffer_bytes.store(bytes, Ordering::Relaxed);
	}

	/// Calculates dynamic target block cache size based on available budget.
	/// If write queues balloon, cache capacity automatically shrinks to protect against OOM.
	pub fn dynamic_cache_target(&self, configured_cache_size: usize) -> usize {
		let non_cache_used = self.active_memtable_bytes.load(Ordering::Relaxed)
			+ self.immutable_memtable_bytes.load(Ordering::Relaxed)
			+ self.ring_buffer_bytes.load(Ordering::Relaxed);

		let headroom = self.max_budget.saturating_sub(non_cache_used);
		// Reserve at least 2MB for block cache
		headroom.min(configured_cache_size).max(2 * 1024 * 1024)
	}

	/// Checks whether the engine has exceeded the maximum memory budget (requiring write stall).
	#[inline]
	pub fn is_over_budget(&self) -> bool {
		self.total_used() >= self.max_budget
	}
}

pub type SharedMemoryController = Arc<MemoryController>;

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_memory_controller_budget_tracking() {
		let mc = MemoryController::new(100 * 1024 * 1024); // 100MB
		assert_eq!(mc.total_used(), 0);
		assert_eq!(mc.remaining_budget(), 100 * 1024 * 1024);

		mc.set_active_memtable_bytes(20 * 1024 * 1024);
		mc.set_immutable_memtable_bytes(30 * 1024 * 1024);
		mc.set_block_cache_bytes(40 * 1024 * 1024);

		assert_eq!(mc.total_used(), 90 * 1024 * 1024);
		assert!(!mc.is_over_budget());
		assert_eq!(mc.remaining_budget(), 10 * 1024 * 1024);

		// Balloon memtables past max budget: 20MB active + 99MB immutable = 119MB > 100MB
		mc.set_immutable_memtable_bytes(99 * 1024 * 1024);
		assert!(mc.is_over_budget());

		// Cache automatically shrinks to minimum floor
		let target = mc.dynamic_cache_target(40 * 1024 * 1024);
		assert_eq!(target, 2 * 1024 * 1024);
	}
}
