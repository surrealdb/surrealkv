use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use rand::RngCore;

use super::{MaintenanceHint, MemoryBudget, Platform};
use crate::api::{KernelResult, MonotonicTime};

pub(crate) struct NativePlatform {
	last_time: AtomicU64,
	memory_budget: MemoryBudget,
}

impl NativePlatform {
	pub(crate) fn new(memory_budget: u64) -> Self {
		Self {
			last_time: AtomicU64::new(0),
			memory_budget: MemoryBudget {
				bytes: memory_budget,
			},
		}
	}
}

#[async_trait]
impl Platform for NativePlatform {
	fn now(&self) -> MonotonicTime {
		let wall = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map_or(0, |duration| duration.as_micros().min(u64::MAX as u128) as u64);
		let mut observed = self.last_time.load(Ordering::Relaxed);
		loop {
			let next = wall.max(observed);
			match self.last_time.compare_exchange_weak(
				observed,
				next,
				Ordering::SeqCst,
				Ordering::Relaxed,
			) {
				Ok(_) => return MonotonicTime(next),
				Err(actual) => observed = actual,
			}
		}
	}

	fn fill_random(&self, out: &mut [u8]) -> KernelResult<()> {
		rand::rng().fill_bytes(out);
		Ok(())
	}

	fn memory_budget(&self) -> MemoryBudget {
		self.memory_budget
	}

	async fn schedule_maintenance(&self, _hint: MaintenanceHint) -> KernelResult<()> {
		// P3 exposes explicit flush/compaction. P5 installs the restartable
		// native maintenance scheduler behind this existing platform seam.
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn native_platform_owner_regression_produces_time_and_entropy() {
		let platform = NativePlatform::new(1024);
		let first = platform.now();
		let second = platform.now();
		assert!(second >= first);
		let mut bytes = [0; 32];
		platform.fill_random(&mut bytes).unwrap();
		assert_ne!(bytes, [0; 32]);
		assert_eq!(platform.memory_budget().bytes, 1024);
	}
}
