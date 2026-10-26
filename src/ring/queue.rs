use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use tokio::sync::{oneshot, OwnedSemaphorePermit};

use super::commit_ring::RingEntry;
use super::index::key_hash;
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
	/// Each write key's hash, by which the write index records it and the
	/// committer looks it up.
	hashes: Box<[u64]>,
	/// `IN_FLIGHT`, `ACCEPTED`, `ABORTED`, or the visible commit's highest
	/// sequence number.
	state: AtomicU64,
	/// Set before the entry is accepted, and taken by the flusher.
	payload: Mutex<Option<Payload>>,
	/// What the payload's batch costs a commit group, stored before the entry is accepted: its
	/// encoded size as `Batch::encoded_len_hint` estimates it in the low 32 bits, and its
	/// `Batch::gather_estimate` in the high 32 bits, each saturated at `u32::MAX`. The flusher
	/// reads both with one load to bound a group without locking the payload. Zero until then.
	///
	/// A saturated size is over `MAX_GROUP_BYTES` and a saturated estimate is at least the
	/// budget, so either keeps the entry from sharing a group, which is what its true value
	/// would do.
	cost: AtomicU64,
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
		let hashes = keys.iter().map(|k| key_hash(k)).collect();
		Self {
			keys: keys.into_boxed_slice(),
			hashes,
			state: AtomicU64::new(IN_FLIGHT),
			payload: Mutex::new(None),
			cost: AtomicU64::new(0),
			permit: Mutex::new(Some(permit)),
		}
	}

	pub(crate) fn keys(&self) -> &[Key] {
		&self.keys
	}

	pub(crate) fn hashes(&self) -> &[u64] {
		&self.hashes
	}

	/// Whether this commit writes `key`.
	pub(crate) fn writes(&self, key: &[u8]) -> bool {
		self.keys.binary_search_by(|k| k.as_slice().cmp(key)).is_ok()
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

	/// Whether the entry aborted, which it never undoes.
	pub(crate) fn is_aborted(&self) -> bool {
		self.state.load(Ordering::Relaxed) == ABORTED
	}

	/// Hands the batch to the flusher. The payload and its cost are stored before the state,
	/// so a flusher that observes `ACCEPTED` always finds them.
	pub(crate) fn accept(&self, payload: Payload) {
		let bytes = u32::try_from(payload.batch.encoded_len_hint()).unwrap_or(u32::MAX);
		let estimate = u32::try_from(payload.batch.gather_estimate()).unwrap_or(u32::MAX);
		self.cost.store(u64::from(bytes) | u64::from(estimate) << 32, Ordering::Relaxed);
		*self.payload.lock() = Some(payload);
		self.state.store(ACCEPTED, Ordering::SeqCst);
	}

	pub(crate) fn abort(&self) {
		self.state.store(ABORTED, Ordering::SeqCst);
	}

	/// Aborts the entry unless it was decided, and returns whether it did.
	pub(crate) fn abort_if_in_flight(&self) -> bool {
		self.state.load(Ordering::SeqCst) == IN_FLIGHT
			&& self
				.state
				.compare_exchange(IN_FLIGHT, ABORTED, Ordering::SeqCst, Ordering::SeqCst)
				.is_ok()
	}

	/// Marks a durable, applied commit visible at `max_seq`. Must only be
	/// called after `visible_seq_num` has covered `max_seq`.
	pub(crate) fn make_visible(&self, max_seq: u64) {
		debug_assert!(max_seq != IN_FLIGHT && max_seq < ACCEPTED);
		self.state.store(max_seq, Ordering::SeqCst);
	}

	/// What an accepted entry's batch costs a group: its estimated encoded size, and the
	/// estimate of the memtable bytes it takes, see `cost`.
	pub(crate) fn payload_cost(&self) -> (usize, u64) {
		let cost = self.cost.load(Ordering::Relaxed);
		((cost & u64::from(u32::MAX)) as usize, cost >> 32)
	}

	pub(crate) fn take_payload(&self) -> Option<Payload> {
		self.payload.lock().take()
	}

	pub(crate) fn take_permit(&self) -> Option<OwnedSemaphorePermit> {
		self.permit.lock().take()
	}
}
