// Tracks every live transaction's `start_seq_num` for the commit oracle's GC.
//
// Distinct from `SnapshotTracker`:
//   - `SnapshotTracker` registers only read-bearing snapshots; it drives compaction MVCC retention.
//   - `ActiveTxnTracker` registers every transaction including write-only ones (which have no
//     `Snapshot`); it drives oracle map GC. The oracle's required watermark can advance faster than
//     compaction's, so the two stay separate.
//
// Lock-free `SkipSet<(start_seq, unique_id)>`. `unique_id` differentiates
// concurrent transactions that happen to share a `start_seq` so `remove`
// in `Drop` doesn't collide.
//
// Registration race safety: `Transaction::new` does
// `start_seq = visible_seq_num.load(); tracker.register(start_seq);` with
// no lock spanning the two operations. This is sound because
// `visible_seq_num` is strictly monotonic (`CommitPipeline::publish` only
// advances it via CAS), so any later `core.seq_num()` returns at least the
// `start_seq` of any currently-registered transaction. A GC threshold
// computed from `oldest()` can therefore never exceed a future transaction's
// `start_seq`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crossbeam_skiplist::SkipSet;

pub(crate) struct ActiveTxnTracker {
	seqs: Arc<SkipSet<(u64, u64)>>,
	next_id: AtomicU64,
}

impl Default for ActiveTxnTracker {
	fn default() -> Self {
		Self::new()
	}
}

impl ActiveTxnTracker {
	pub(crate) fn new() -> Self {
		Self {
			seqs: Arc::new(SkipSet::new()),
			next_id: AtomicU64::new(0),
		}
	}

	/// Register a transaction's start_seq. Returns an RAII guard whose `Drop`
	/// unregisters the entry.
	pub(crate) fn register(self: &Arc<Self>, start_seq: u64) -> ActiveTxnGuard {
		let id = self.next_id.fetch_add(1, Ordering::Relaxed);
		let entry = (start_seq, id);
		self.seqs.insert(entry);
		ActiveTxnGuard {
			tracker: Arc::clone(self),
			entry,
			released: false,
		}
	}

	/// Smallest `start_seq` currently registered. `None` if empty.
	///
	/// Cheap (O(log N) via `SkipSet::front`). Safe to call concurrently with
	/// `register` and unregister. See the module-level comment for the
	/// monotonicity-based race proof.
	pub(crate) fn oldest(&self) -> Option<u64> {
		self.seqs.front().map(|e| e.value().0)
	}

	#[cfg(test)]
	pub(crate) fn len(&self) -> usize {
		self.seqs.len()
	}
}

/// RAII handle. Owned by `Transaction`; dropped automatically (or explicitly
/// via `release`) on commit / rollback / drop / panic.
pub(crate) struct ActiveTxnGuard {
	tracker: Arc<ActiveTxnTracker>,
	entry: (u64, u64),
	released: bool,
}

impl ActiveTxnGuard {
	/// Release the slot eagerly. Idempotent.
	pub(crate) fn release(&mut self) {
		if !self.released {
			self.tracker.seqs.remove(&self.entry);
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
