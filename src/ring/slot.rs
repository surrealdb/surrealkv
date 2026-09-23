use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::Mutex;

/// Status flags embedded in the atomic `state` of a Slot.
pub(crate) const SLOT_EMPTY: u64 = 0;
pub(crate) const SLOT_PUBLISHED: u64 = 1;
pub(crate) const SLOT_DRAINED: u64 = 2;

pub(crate) struct SlotData {
	/// The sequence number this slot currently represents.
	pub seq: u64,
	/// Pre-allocated buffer for encoding the transaction batch.
	/// Ensures zero-allocation on the hot write path.
	pub buffer: Vec<u8>,
	/// Tracks whether this transaction was rolled back (aborted)
	/// after claiming the slot.
	pub aborted: bool,
}

impl Default for SlotData {
	fn default() -> Self {
		Self {
			seq: 0,
			// Pre-allocate a reasonable default batch size (e.g., 4KB)
			// to avoid allocations for typical transactions.
			buffer: Vec::with_capacity(4096),
			aborted: false,
		}
	}
}

/// A single slot in the Ring Buffer.
pub(crate) struct Slot {
	/// The atomic state of this slot.
	/// Stores the sequence number, and potentially status flags.
	pub(crate) state: AtomicU64,
	/// The underlying data. A writer and flusher only collide here
	/// if the ring is completely full (lap overlap).
	pub(crate) data: Mutex<SlotData>,
}

impl Slot {
	pub(crate) fn new() -> Self {
		Self {
			state: AtomicU64::new(SLOT_EMPTY),
			data: Mutex::new(SlotData::default()),
		}
	}

	/// Marks the slot as published and ready for the flusher.
	#[inline]
	pub(crate) fn publish(&self, seq: u64) {
		self.state.store(seq, Ordering::Release);
	}
}
