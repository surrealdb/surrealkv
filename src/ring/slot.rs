use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::Mutex;
use tokio::sync::oneshot;
use crate::batch::Batch;
use crate::error::Result;

pub(crate) struct SlotData {
	/// The slot id this slot currently represents.
	pub seq: u64,
	/// The highest sequence number assigned to records in this batch.
	pub max_seq: u64,
	/// The batch to be written and applied.
	pub batch: Option<Batch>,
	/// Whether this commit requires an immediate fsync.
	pub sync: bool,
	/// Notification channel back to the committing transaction.
	pub complete_tx: Option<oneshot::Sender<Result<()>>>,
	/// Tracks whether this transaction was aborted (e.g. conflict).
	pub aborted: bool,
}

impl Default for SlotData {
	fn default() -> Self {
		Self {
			seq: 0,
			max_seq: 0,
			batch: None,
			sync: false,
			complete_tx: None,
			aborted: false,
		}
	}
}

/// A single slot in the Ring Buffer.
pub(crate) struct Slot {
	/// The atomic state of this slot (stores the sequence number when published).
	pub(crate) state: AtomicU64,
	/// The underlying slot data.
	pub(crate) data: Mutex<SlotData>,
}

impl Slot {
	pub(crate) fn new() -> Self {
		Self {
			state: AtomicU64::new(0),
			data: Mutex::new(SlotData::default()),
		}
	}

	/// Marks the slot as published and ready for the flusher.
	#[inline]
	pub(crate) fn publish(&self, seq: u64) {
		self.state.store(seq, Ordering::Release);
	}

	/// Checks if the slot has been published with the given sequence number.
	#[inline]
	pub(crate) fn is_published(&self, seq: u64) -> bool {
		self.state.load(Ordering::Acquire) == seq
	}
}
