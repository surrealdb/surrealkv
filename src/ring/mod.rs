use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

mod slot;
mod bloom;

pub(crate) use slot::{Slot, SlotData};
pub(crate) use bloom::BloomFilter;

/// A lock-free, Multi-Producer Single-Consumer (MPSC) Ring Buffer.
/// Used for the Optimistic Concurrency Control (OCC) commit pipeline.
///
/// Writers atomically claim a slot, validate conflicts, write their
/// batch into the pre-allocated buffer, and mark the slot as published.
/// A single background flusher consumes published slots and writes them to the WAL.
pub(crate) struct Ring {
	slots: Box<[Slot]>,

	/// The next sequence number to hand out to a writer.
	next: AtomicU64,

	/// The highest sequence number where all slots at or below it are filled and ready to flush.
	published: AtomicU64,

	/// The highest sequence number that has been successfully drained (flushed).
	/// Slots at or below this sequence are free to be claimed for the next lap.
	taken: AtomicU64,
}

impl Ring {
	/// Creates a new Ring Buffer with a capacity rounded up to the next power of two.
	pub(crate) fn new(capacity: usize, first_seq: u64) -> Self {
		let cap = capacity.max(2).next_power_of_two();
		let mut slots = Vec::with_capacity(cap);

		for _ in 0..cap {
			slots.push(Slot::new());
		}

		Self {
			slots: slots.into_boxed_slice(),
			next: AtomicU64::new(first_seq),
			published: AtomicU64::new(first_seq.saturating_sub(1)),
			taken: AtomicU64::new(first_seq.saturating_sub(1)),
		}
	}

	/// Returns the capacity of the ring.
	#[inline]
	pub(crate) fn capacity(&self) -> usize {
		self.slots.len()
	}

	/// Atomically claims the next available sequence number.
	/// The writer is responsible for waiting if the slot is still in use (lap collision).
	#[inline]
	pub(crate) fn claim(&self) -> u64 {
		self.next.fetch_add(1, Ordering::AcqRel)
	}

	/// Gets a reference to the slot for the given sequence number.
	#[inline]
	pub(crate) fn slot(&self, seq: u64) -> &Slot {
		&self.slots[(seq as usize) & (self.capacity() - 1)]
	}

	/// Returns true if the slot for `seq` is free (i.e. it has been drained).
	#[inline]
	pub(crate) fn has_room(&self, seq: u64) -> bool {
		seq <= self.taken.load(Ordering::Acquire) + self.capacity() as u64
	}

	/// Returns the highest drained sequence number.
	#[inline]
	pub(crate) fn taken(&self) -> u64 {
		self.taken.load(Ordering::Acquire)
	}
}
