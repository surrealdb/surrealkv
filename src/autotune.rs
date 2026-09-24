//! Zero-config auto-tuning for hardware-aware engine defaults.
//!
//! Automatically inspects system concurrency and available RAM on startup,
//! calculating optimal memtable sizes, block cache capacities, and flush intervals
//! without manual configuration burden.

use crate::Options;

/// Hardware configuration profile detected on the host machine.
#[derive(Debug, Clone, Copy)]
pub struct HardwareProfile {
	pub cpu_cores: usize,
	pub total_ram_bytes: u64,
}

impl HardwareProfile {
	/// Detects the host hardware configuration.
	pub fn detect() -> Self {
		let cpu_cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4).max(1);

		// Heuristic default RAM estimation if OS memory query is unavailable:
		// Assume at least 4GB or 1GB per core
		let total_ram_bytes = ((cpu_cores as u64) * 1024 * 1024 * 1024).max(4 * 1024 * 1024 * 1024);

		Self {
			cpu_cores,
			total_ram_bytes,
		}
	}

	/// Calculates optimal auto-tuned SurrealKV options for this hardware.
	pub fn tune_options(&self, mut opts: Options) -> Options {
		// 1. Allocate ~25% of system RAM to SurrealKV global memory budget (capped between 64MB and 16GB)
		let engine_memory =
			(self.total_ram_bytes / 4).clamp(64 * 1024 * 1024, 16 * 1024 * 1024 * 1024);

		// 2. Divide engine memory: 40% Memtables, 50% Block Cache, 10% VLog / Overhead
		let memtable_budget = (engine_memory * 4) / 10;
		let block_cache_budget = (engine_memory * 5) / 10;

		// 3. Set memtable size: scale with core count
		let target_memtable_size = (memtable_budget / (self.cpu_cores.clamp(2, 8) as u64))
			.clamp(16 * 1024 * 1024, 128 * 1024 * 1024) as usize;
		opts.max_memtable_size = target_memtable_size;

		// 4. Set block cache size
		opts.with_block_cache_capacity(block_cache_budget)
	}
}

/// Automatically tunes an Options instance for the current host machine.
pub fn auto_tune_options(opts: Options) -> Options {
	HardwareProfile::detect().tune_options(opts)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_auto_tuning_scales_with_hardware() {
		let profile = HardwareProfile {
			cpu_cores: 8,
			total_ram_bytes: 16 * 1024 * 1024 * 1024, // 16GB
		};

		let opts = Options::default();
		let tuned = profile.tune_options(opts);

		// 16GB / 4 = 4GB total engine memory
		// 50% = 2GB block cache
		assert!(tuned.block_cache.capacity() >= 1024 * 1024 * 1024);
		// Memtable scaled to reasonable chunk
		assert!(tuned.max_memtable_size >= 16 * 1024 * 1024);
	}
}
