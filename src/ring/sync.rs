//! Synchronisation primitives for the commit ring.
//!
//! Kept behind one module so the ring can later be model checked by swapping
//! these for `loom` equivalents, as SurrealMX does.

pub(crate) use std::sync::atomic::AtomicU64;

pub(crate) use parking_lot::RwLock;

/// Progressive backoff while waiting on another committer's claim-to-publish
/// window, which never spans an `.await` and so is only ever a few
/// instructions long unless that thread is descheduled.
#[inline(always)]
pub(crate) fn backoff(spins: usize) {
	if spins < 10 {
		std::hint::spin_loop();
	} else {
		#[cfg(not(target_arch = "wasm32"))]
		if spins < 100 {
			std::thread::yield_now();
		} else {
			std::thread::park_timeout(std::time::Duration::from_micros(10));
		}
		#[cfg(target_arch = "wasm32")]
		std::hint::spin_loop();
	}
}

/// Pads and aligns a value to its own cache line pair, so the ring's
/// watermarks do not false-share with each other or with the slots.
#[repr(align(128))]
pub(crate) struct CachePadded<T>(T);

impl<T> CachePadded<T> {
	pub(crate) const fn new(value: T) -> Self {
		Self(value)
	}
}

impl<T> std::ops::Deref for CachePadded<T> {
	type Target = T;

	#[inline(always)]
	fn deref(&self) -> &T {
		&self.0
	}
}
