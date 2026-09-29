use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use tokio::sync::{oneshot, OwnedSemaphorePermit};

use super::bloom::BloomFilter;
use super::commit_ring::RingEntry;
use crate::batch::Batch;
use crate::error::Result;
use crate::Key;

/// The committer is still validating: the entry may yet commit or abort.
const IN_FLIGHT: u64 = 0;
/// Validated and queued for the flusher, but not yet visible.
const ACCEPTED: u64 = u64::MAX - 1;
/// Aborted by a conflict, failed to flush, or a locked-read check that
/// publishes no writes.
const ABORTED: u64 = u64::MAX;
// Any other state is the highest LSM sequence number of a commit that is
// durable and visible (sequence numbers start at 1).

/// Where a commit entry is in its lifecycle, as the flusher sees it.
pub(crate) enum EntryState {
	InFlight,
	Accepted,
	Complete,
}

/// What the flusher needs to make an accepted commit durable.
pub(crate) struct Payload {
	pub(crate) batch: Batch,
	pub(crate) sync: bool,
	pub(crate) complete_tx: oneshot::Sender<Result<()>>,
	/// The restore epoch the commit was accepted in.
	pub(crate) epoch: u64,
}

/// One transaction's slot in the commit ring: its write set for conflict
/// detection, and its batch until the flusher takes it.
pub(crate) struct CommitEntry {
	/// Sorted, deduplicated write keys for exact conflict checks.
	keys: Box<[Key]>,
	/// Bloom filter over the write keys for fast pre-checks.
	bloom: BloomFilter,
	/// `IN_FLIGHT`, `ACCEPTED`, `ABORTED`, or the visible commit's highest
	/// sequence number.
	state: AtomicU64,
	/// Set before the entry is accepted, and taken by the flusher.
	payload: Mutex<Option<Payload>>,
	/// The committer's admission permit, released by the flusher only once
	/// the ring's completed prefix has passed this entry.
	permit: Mutex<Option<OwnedSemaphorePermit>>,
}

impl RingEntry for CommitEntry {
	fn is_complete(&self) -> bool {
		!matches!(self.state.load(Ordering::SeqCst), IN_FLIGHT | ACCEPTED)
	}
}

impl CommitEntry {
	pub(crate) fn new(mut keys: Vec<Key>, permit: OwnedSemaphorePermit) -> Self {
		keys.sort();
		keys.dedup();
		let mut bloom = BloomFilter::new();
		for k in &keys {
			bloom.insert(k);
		}
		Self {
			keys: keys.into_boxed_slice(),
			bloom,
			state: AtomicU64::new(IN_FLIGHT),
			payload: Mutex::new(None),
			permit: Mutex::new(Some(permit)),
		}
	}

	pub(crate) fn keys(&self) -> &[Key] {
		&self.keys
	}

	pub(crate) fn bloom(&self) -> &BloomFilter {
		&self.bloom
	}

	pub(crate) fn state(&self) -> EntryState {
		match self.state.load(Ordering::SeqCst) {
			IN_FLIGHT => EntryState::InFlight,
			ACCEPTED => EntryState::Accepted,
			_ => EntryState::Complete,
		}
	}

	/// Whether a transaction whose snapshot is `snapshot_seq` can ignore this
	/// entry: it aborted, or it was already visible to the snapshot.
	pub(crate) fn is_ignorable_at(&self, snapshot_seq: u64) -> bool {
		match self.state.load(Ordering::SeqCst) {
			ABORTED => true,
			IN_FLIGHT | ACCEPTED => false,
			max_seq => max_seq <= snapshot_seq,
		}
	}

	/// Hands the batch to the flusher. The payload is stored before the state,
	/// so a flusher that observes `ACCEPTED` always finds it.
	pub(crate) fn accept(&self, payload: Payload) {
		*self.payload.lock() = Some(payload);
		self.state.store(ACCEPTED, Ordering::SeqCst);
	}

	pub(crate) fn abort(&self) {
		self.state.store(ABORTED, Ordering::SeqCst);
	}

	/// Marks a durable, applied commit visible at `max_seq`. Must only be
	/// called after `visible_seq_num` has covered `max_seq`.
	pub(crate) fn make_visible(&self, max_seq: u64) {
		debug_assert!(max_seq != IN_FLIGHT && max_seq < ACCEPTED);
		self.state.store(max_seq, Ordering::SeqCst);
	}

	pub(crate) fn take_payload(&self) -> Option<Payload> {
		self.payload.lock().take()
	}

	pub(crate) fn take_permit(&self) -> Option<OwnedSemaphorePermit> {
		self.permit.lock().take()
	}

	/// Checks if this commit's write set is disjoint from another write set.
	pub(crate) fn is_disjoint_writeset(
		&self,
		other_keys: &[Key],
		other_bloom: &BloomFilter,
	) -> bool {
		if self.bloom.is_empty() || other_bloom.is_empty() {
			return true;
		}

		// Fast path: check if any of our keys are in the other's bloom filter
		let mut any_possible = false;
		for k in self.keys.iter() {
			if other_bloom.may_contain(k) {
				any_possible = true;
				break;
			}
		}
		if !any_possible {
			return true;
		}

		// Exact path: binary search on sorted keys
		for k in other_keys {
			if self.keys.binary_search(k).is_ok() {
				return false;
			}
		}

		true
	}

	/// Checks if this commit's write set is disjoint from a read set.
	pub(crate) fn is_disjoint_readset(&self, read_keys: &[Key], read_bloom: &BloomFilter) -> bool {
		self.is_disjoint_writeset(read_keys, read_bloom)
	}
}
