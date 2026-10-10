// Tracks the pin of every live mutating transaction, for the commit ring's retire.
//
// A mutating transaction (ReadWrite or WriteOnly) registers the completed prefix of the commit
// ring that it read when it began (`CommitPipeline::commit_pin`). The flusher's `retire` scans
// the oldest pin to decide how far the ring's retired watermark may advance. Read-only
// transactions open no conflict window and do not register.
//
// Distinct from `SnapshotTracker`, which registers only read-bearing snapshots and drives
// compaction's MVCC retention. The two watermarks advance independently, so the trackers stay
// separate.
//
// A sharded set of `(pin, unique_id)`. `unique_id` differentiates concurrent transactions that
// share a pin, so that `remove` in `Drop` does not collide.
//
// What the retired watermark is held to:
//   It stays at or below the conflict WINDOW of every live mutating transaction, which is what
//   validation needs, since a window reaches every entry above it. It does not stay at or below
//   the oldest pin. `Transaction::new` reads the pin, registers it, then reads the window, with
//   nothing spanning the three steps. A `retire` whose scan runs before the registration does
//   not see the transaction, and may pass its pin as far as the completed prefix it read before
//   the scan. The window is read after the registration from the same monotonic prefix, so it
//   is at least that high. A scan that does see the pin is bounded by it, and the pin is read
//   before the window, so it is at most the window.
//
//   An entry the watermark has not passed is kept where validation looks for it, in its ring
//   slot or, once a lap has overwritten the slot, in the overflow map. A missing one counts as
//   a conflict, so a watermark that lags costs memory and never correctness.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

const NUM_TXN_SHARDS: usize = 64;

type TxnShard = RwLock<BTreeSet<(u64, u64)>>;

fn get_thread_shard_index() -> usize {
	static SHARD_COUNTER: AtomicUsize = AtomicUsize::new(0);
	thread_local! {
		static SHARD_ID: usize = SHARD_COUNTER.fetch_add(1, Ordering::Relaxed);
	}
	SHARD_ID.with(|id| *id % NUM_TXN_SHARDS)
}

pub(crate) struct ActiveTxnTracker {
	shards: Arc<[TxnShard; NUM_TXN_SHARDS]>,
	next_id: AtomicU64,
}

impl Default for ActiveTxnTracker {
	fn default() -> Self {
		Self::new()
	}
}

impl ActiveTxnTracker {
	pub(crate) fn new() -> Self {
		let shards: Vec<TxnShard> =
			(0..NUM_TXN_SHARDS).map(|_| RwLock::new(BTreeSet::new())).collect();
		let shards: Box<[TxnShard; NUM_TXN_SHARDS]> =
			shards.into_boxed_slice().try_into().unwrap_or_else(|_| panic!("size mismatch"));
		Self {
			shards: Arc::from(shards),
			next_id: AtomicU64::new(0),
		}
	}

	/// Register a transaction's pin. Returns an RAII guard whose `Drop`
	/// unregisters the entry.
	pub(crate) fn register(self: &Arc<Self>, pin: u64) -> ActiveTxnGuard {
		let id = self.next_id.fetch_add(1, Ordering::Relaxed);
		let entry = (pin, id);
		let shard_idx = get_thread_shard_index();
		self.shards[shard_idx].write().insert(entry);
		ActiveTxnGuard {
			tracker: Arc::clone(self),
			entry,
			shard_idx,
			released: false,
		}
	}

	/// Smallest pin currently registered. `None` if empty.
	pub(crate) fn oldest(&self) -> Option<u64> {
		self.shards.iter().filter_map(|s| s.read().first().map(|e| e.0)).min()
	}

	#[cfg(test)]
	pub(crate) fn len(&self) -> usize {
		self.shards.iter().map(|s| s.read().len()).sum()
	}
}

/// RAII handle. Owned by `Transaction`; dropped automatically (or explicitly
/// via `release`) on commit / rollback / drop / panic.
pub(crate) struct ActiveTxnGuard {
	tracker: Arc<ActiveTxnTracker>,
	entry: (u64, u64),
	shard_idx: usize,
	released: bool,
}

impl ActiveTxnGuard {
	/// Release the slot eagerly. Idempotent.
	pub(crate) fn release(&mut self) {
		if !self.released {
			self.tracker.shards[self.shard_idx].write().remove(&self.entry);
			self.released = true;
		}
	}
}

impl Drop for ActiveTxnGuard {
	fn drop(&mut self) {
		self.release();
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn empty_tracker_has_no_oldest() {
		let t = Arc::new(ActiveTxnTracker::new());
		assert_eq!(t.oldest(), None);
		assert_eq!(t.len(), 0);
	}

	#[test]
	fn registers_and_drops() {
		let t = Arc::new(ActiveTxnTracker::new());
		{
			let _g = t.register(10);
			assert_eq!(t.oldest(), Some(10));
			assert_eq!(t.len(), 1);
		}
		assert_eq!(t.oldest(), None);
		assert_eq!(t.len(), 0);
	}

	#[test]
	fn oldest_picks_min() {
		let t = Arc::new(ActiveTxnTracker::new());
		let _g1 = t.register(20);
		let _g2 = t.register(10);
		let _g3 = t.register(15);
		assert_eq!(t.oldest(), Some(10));
	}

	#[test]
	fn duplicate_start_seqs_dont_collide() {
		let t = Arc::new(ActiveTxnTracker::new());
		let g1 = t.register(5);
		let g2 = t.register(5);
		assert_eq!(t.len(), 2);
		drop(g1);
		assert_eq!(t.oldest(), Some(5));
		assert_eq!(t.len(), 1);
		drop(g2);
		assert_eq!(t.oldest(), None);
	}

	#[test]
	fn explicit_release_is_idempotent() {
		let t = Arc::new(ActiveTxnTracker::new());
		let mut g = t.register(7);
		g.release();
		assert_eq!(t.oldest(), None);
		// Calling release again is fine; Drop also calls release.
		g.release();
		drop(g);
		assert_eq!(t.oldest(), None);
	}
}
