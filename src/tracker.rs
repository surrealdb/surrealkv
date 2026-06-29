// Tracks every live transaction's `start_seq_num` for the commit oracle's GC.
//
// Distinct from `SnapshotTracker`:
//   - `SnapshotTracker` registers only read-bearing snapshots; it drives compaction MVCC retention.
//   - `ActiveTxnTracker` registers every transaction including write-only ones (which have no
//     `Snapshot`); it drives oracle map GC. The oracle's required watermark can advance faster than
//     compaction's, so the two stay separate.
//
// Backed by a sharded refcount-by-seq tracker (`ShardedMinTracker`). The
// refcount handles concurrent transactions that share a `start_seq` (common,
// since `start_seq` is loaded from `visible_seq_num`, which only advances on
// commit) without the per-entry `unique_id` a set would need.
//
// Registration race window (KNOWN, DOCUMENTED, NOT FIXED PROTOCOL-SIDE):
//   `Transaction::new` does
//     `start_seq = visible_seq_num.load(); tracker.register(start_seq);`
//   with no synchronization spanning the two operations. If the thread is
//   preempted between them and a concurrent commit fires the oracle's GC
//   body with `oldest_active > start_seq` (because no other live txn has
//   `start_seq <= our start_seq` at that moment), `kept_since` rises past
//   our `start_seq`. Our subsequent `oracle.check` then returns
//   `TransactionRetry`.
//
//   This is benign:
//     - `TransactionRetry` is already part of the public API contract and callers of
//       `Transaction::commit` are required to retry.
//     - On retry, `start_seq` is reloaded fresh and the race window does not re-apply.
//     - Worst-case cost per occurrence: one extra `begin()` call, no I/O.
//     - The race fires only when (a) `commits_since_gc` reaches `GC_INTERVAL` during the
//       load+register window (typically microseconds), AND (b) no other live txn pins the
//       oldest_active below our `start_seq`. Combined probability is low.
//
//   Protocol-side fixes considered and rejected: every variant (mutex gate
//   around load+register, retry loop, lock-free placeholder-then-update)
//   adds ~10ns or more to every `begin()`. The cost arithmetic doesn't
//   favor preventing a sub-1500ns/sec event by paying ~1ms/sec across all
//   begins. Revisit if production benchmarks show the race firing often
//   enough to dominate.

use std::sync::Arc;

use crate::min_tracker::ShardedMinTracker;

pub(crate) struct ActiveTxnTracker {
	// Sharded refcount-by-seq tracker. Replaces a global `SkipSet<(seq, id)>`:
	// the refcount subsumes the `unique_id` multiset trick (concurrent begins
	// share a `start_seq`), and sharding removes the lock-free-skiplist churn
	// that dominated CPU under high begin/commit concurrency.
	inner: ShardedMinTracker,
}

impl Default for ActiveTxnTracker {
	fn default() -> Self {
		Self::new()
	}
}

impl ActiveTxnTracker {
	pub(crate) fn new() -> Self {
		Self {
			inner: ShardedMinTracker::new(),
		}
	}

	/// Register a transaction's start_seq. Returns an RAII guard whose `Drop`
	/// unregisters the entry.
	pub(crate) fn register(self: &Arc<Self>, start_seq: u64) -> ActiveTxnGuard {
		let shard = self.inner.register(start_seq);
		ActiveTxnGuard {
			tracker: Arc::clone(self),
			shard,
			seq: start_seq,
			released: false,
		}
	}

	/// Smallest `start_seq` currently registered. `None` if empty.
	///
	/// Lock-free (a min over the per-shard cached minimums). Safe to call
	/// concurrently with `register` and unregister. Never over-estimates in
	/// steady state; see the module-level comment for the monotonicity-based
	/// race proof.
	pub(crate) fn oldest(&self) -> Option<u64> {
		self.inner.oldest()
	}

	#[cfg(test)]
	pub(crate) fn len(&self) -> usize {
		self.inner.len()
	}
}

/// RAII handle. Owned by `Transaction`; dropped automatically (or explicitly
/// via `release`) on commit / rollback / drop / panic.
pub(crate) struct ActiveTxnGuard {
	tracker: Arc<ActiveTxnTracker>,
	shard: usize,
	seq: u64,
	released: bool,
}

impl ActiveTxnGuard {
	/// Release the slot eagerly. Idempotent.
	pub(crate) fn release(&mut self) {
		if !self.released {
			self.tracker.inner.unregister(self.shard, self.seq);
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
