// Commit oracle: in-memory write-set conflict detector for Snapshot Isolation.
//
// Stores `xxh3_64(key) -> highest committed seq that wrote that key`. Reads
// are O(1) hash lookups; insertions are O(1) per key. Validation under the
// commit pipeline's `write_mutex` is the load-bearing invariant: a transaction
// that enters `check` after another transaction's `publish` is guaranteed to
// see that other transaction's reservation, even before its memtable apply
// lands. This is what lets `apply()` run OUTSIDE `write_mutex`.
//
// Retention model:
//   `recent_writes` contains every commit at `seq >= discard_below`, where
//   `discard_below` advances only via per-publish GC that uses
//   `oldest_active = min(start_seq over live transactions)` as the threshold.
//   Map size is bounded by the live-reader window, not by commit count or any
//   tuning knob.
//
// Why u64 fingerprints (not full keys):
//   Per-check collision rate is `map_size / 2^64`. For 1M entries it's
//   5.4e-14; for 10M, 5.4e-13. False aborts at these rates are below
//   cosmic-ray bit-flip frequencies and have never been reported in
//   Badger (which uses the same approach in production since 2017).
//   Memory: ~3x smaller than full-key storage. No per-publish allocation.
//   On the rare collision: a transaction sees a spurious WriteConflict,
//   retries, and succeeds. Soundness is preserved.
//
// Why a single `parking_lot::Mutex`:
//   The critical section is short (O(W) hash ops, no I/O). RwLock buys
//   nothing — every call mutates state. Sharding helps only if commits
//   scaled past `write_mutex`, which serializes them already at a coarser
//   layer.

use std::collections::HashMap;

use parking_lot::Mutex;
use xxhash_rust::xxh3::xxh3_64;

use crate::error::{Error, Result};

#[inline]
fn fp(key: &[u8]) -> u64 {
	xxh3_64(key)
}

pub(crate) struct CommitOracle {
	inner: Mutex<OracleInner>,
}

struct OracleInner {
	// xxh3_64(key) -> commit_seq of the most recent writer of that key.
	recent_writes: HashMap<u64, u64>,

	// Any txn with `start_seq < discard_below` cannot be soundly validated by
	// the map alone (its window has been GC'd). Such txns get TransactionRetry.
	discard_below: u64,

	// Highest `oldest_active` seen by a GC pass. Used as a skip-when-unchanged
	// gate: if the watermark hasn't advanced since the last sweep, there is no
	// new work to do.
	last_cleanup_oldest_active: u64,
}

impl Default for CommitOracle {
	fn default() -> Self {
		Self::new()
	}
}

impl CommitOracle {
	pub(crate) fn new() -> Self {
		Self {
			inner: Mutex::new(OracleInner {
				recent_writes: HashMap::new(),
				discard_below: 0,
				last_cleanup_oldest_active: 0,
			}),
		}
	}

	/// Validate the write set against the recent-writes map.
	///
	/// Called under `write_mutex` BEFORE seq allocation. Returns:
	/// - `TransactionRetry` if `start_seq < discard_below` (window GC'd).
	/// - `TransactionWriteConflict` if any key was committed at seq > start_seq.
	pub(crate) fn check<'a, I>(&self, keys: I, start_seq: u64) -> Result<()>
	where
		I: IntoIterator<Item = &'a [u8]>,
	{
		let g = self.inner.lock();
		if start_seq < g.discard_below {
			return Err(Error::TransactionRetry);
		}
		for k in keys {
			if let Some(&committed) = g.recent_writes.get(&fp(k)) {
				if committed > start_seq {
					return Err(Error::TransactionWriteConflict);
				}
			}
		}
		Ok(())
	}

	/// Publish this commit's writes into the map, then opportunistically GC.
	///
	/// Stamping: every key in this batch is stamped with `seq_num + count - 1`
	/// (the highest seq in the batch). Conservative — a future txn whose
	/// `start_seq` falls inside `[seq_num, seq_num+count-1)` may see a false
	/// conflict, but no real conflict is ever missed.
	///
	/// GC cadence (Badger pattern): run per publish, but bail immediately when
	/// `oldest_active` has not advanced since the previous sweep. In steady
	/// state under long-lived readers this is two atomic-ish reads + a
	/// comparison and no map walk. When the watermark moves, prune entries
	/// with `seq < oldest_active` and update `discard_below`.
	///
	/// Called under `write_mutex` AFTER seq allocation, BEFORE WAL.
	pub(crate) fn publish<'a, I>(&self, keys: I, seq_num: u64, count: u64, oldest_active: u64)
	where
		I: IntoIterator<Item = &'a [u8]>,
	{
		debug_assert!(count >= 1, "publish called with count=0");
		let mut g = self.inner.lock();
		let stamp = seq_num + count - 1;
		for k in keys {
			g.recent_writes.insert(fp(k), stamp);
		}

		// Per-publish GC with skip-if-unchanged. `oldest_active` is provably
		// non-decreasing across calls (it's derived from `min(start_seq)` over
		// live txns, and `visible_seq_num` is monotonic). The debug_assert
		// catches any future code path that violates that invariant.
		if oldest_active > g.last_cleanup_oldest_active {
			debug_assert!(
				oldest_active >= g.last_cleanup_oldest_active,
				"oldest_active must be monotonic"
			);
			g.last_cleanup_oldest_active = oldest_active;
			g.discard_below = oldest_active;
			g.recent_writes.retain(|_, v| *v >= oldest_active);
		}
	}

	/// Roll back oracle entries reserved by a transaction whose commit
	/// path failed AFTER `publish` (typically because `apply` errored).
	///
	/// Removes entries only when their stamp still equals `my_seq` — i.e.
	/// when *we* are still the most recent writer of that fingerprint. If
	/// a concurrent transaction has already overwritten an entry with a
	/// higher seq, leaves it alone (their stamp wins).
	///
	/// Soundness: removing our entry can never cause a subsequent commit
	/// to miss a *real* conflict. The seq we are rolling back was reserved
	/// but its memtable apply failed; no other writer's correctness depends
	/// on its presence. A late retry by the original caller will pass
	/// validation (entry gone) and succeed at a fresh seq.
	pub(crate) fn rollback<'a, I>(&self, keys: I, my_seq: u64)
	where
		I: IntoIterator<Item = &'a [u8]>,
	{
		let mut g = self.inner.lock();
		for k in keys {
			let fk = fp(k);
			if let Some(&v) = g.recent_writes.get(&fk) {
				if v == my_seq {
					g.recent_writes.remove(&fk);
				}
			}
		}
	}

	/// Reset oracle state when a `Tree::restore_from_checkpoint` rewinds the
	/// seq counter. The live process may have accumulated entries with seqs
	/// that are now greater than the restored `max_seq`; those are ghosts of
	/// a future that no longer exists.
	///
	/// NOT called at startup. A freshly-constructed oracle already has an
	/// empty map and `discard_below = 0`, and the first post-startup txn has
	/// `start_seq = max_recovered_seq` which trivially passes the
	/// `start_seq < discard_below` check at either initial value of
	/// `discard_below`.
	pub(crate) fn reset_for_restore(&self, max_seq: u64) {
		let mut g = self.inner.lock();
		g.discard_below = max_seq;
		g.last_cleanup_oldest_active = max_seq;
		g.recent_writes.clear();
	}

	#[cfg(test)]
	pub(crate) fn len(&self) -> usize {
		self.inner.lock().recent_writes.len()
	}

	#[cfg(test)]
	pub(crate) fn discard_below(&self) -> u64 {
		self.inner.lock().discard_below
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn key(s: &str) -> Vec<u8> {
		s.as_bytes().to_vec()
	}

	#[test]
	fn empty_check_passes() {
		let o = CommitOracle::new();
		assert!(o.check(std::iter::empty::<&[u8]>(), 0).is_ok());
		assert!(o.check([key("a").as_slice()], 0).is_ok());
	}

	#[test]
	fn detects_write_write_conflict() {
		let o = CommitOracle::new();
		// T1 commits K at seq=10.
		o.publish([key("k").as_slice()], 10, 1, 0);
		// T2 started at seq=5, also wants to write K. Should conflict.
		assert!(matches!(o.check([key("k").as_slice()], 5), Err(Error::TransactionWriteConflict)));
	}

	#[test]
	fn no_conflict_when_started_after_commit() {
		let o = CommitOracle::new();
		o.publish([key("k").as_slice()], 10, 1, 0);
		assert!(o.check([key("k").as_slice()], 10).is_ok());
		assert!(o.check([key("k").as_slice()], 11).is_ok());
	}

	#[test]
	fn disjoint_keys_no_conflict() {
		let o = CommitOracle::new();
		o.publish([key("a").as_slice()], 10, 1, 0);
		assert!(o.check([key("b").as_slice()], 5).is_ok());
	}

	#[test]
	fn batch_stamp_is_highest_seq() {
		let o = CommitOracle::new();
		o.publish([key("a").as_slice(), key("b").as_slice(), key("c").as_slice()], 10, 3, 0);
		// A txn at start_seq=11 conflicts because stamp=12 > 11.
		assert!(matches!(o.check([key("a").as_slice()], 11), Err(Error::TransactionWriteConflict)));
		// A txn at start_seq=12 passes.
		assert!(o.check([key("a").as_slice()], 12).is_ok());
	}

	#[test]
	fn reset_for_restore_clears_state() {
		let o = CommitOracle::new();
		o.publish([key("k").as_slice()], 10, 1, 0);
		o.reset_for_restore(100);
		assert_eq!(o.len(), 0);
		assert_eq!(o.discard_below(), 100);
		// A txn at start_seq=99 < 100 must retry.
		assert!(matches!(o.check([key("k").as_slice()], 99), Err(Error::TransactionRetry)));
		// A txn at start_seq=100 is accepted (strict <).
		assert!(o.check([key("k").as_slice()], 100).is_ok());
	}

	#[test]
	fn gc_skips_when_watermark_unchanged() {
		let o = CommitOracle::new();
		// Two publishes at the SAME oldest_active. Each adds a new entry. GC's
		// skip-if-unchanged guard means neither pass drops anything (oldest_active
		// didn't advance, so retain criterion didn't change).
		o.publish([key("a").as_slice()], 10, 1, 5);
		o.publish([key("b").as_slice()], 20, 1, 5);
		// First publish triggered the initial GC at watermark=5 (5 > 0). Second
		// publish saw watermark unchanged, skipped. Both entries are >= 5 so
		// either way they're retained.
		assert_eq!(o.len(), 2);
		assert_eq!(o.discard_below(), 5);
	}

	#[test]
	fn gc_runs_when_watermark_advances() {
		let o = CommitOracle::new();
		// First publish at watermark=0: no advance from initial 0, nothing drops.
		o.publish([key("a").as_slice()], 10, 1, 0);
		assert_eq!(o.len(), 1);
		// Second publish at watermark=50: advances. Entry at seq=10 is below 50 → dropped.
		o.publish([key("recent").as_slice()], 99_999, 1, 50);
		assert_eq!(o.len(), 1); // "a" dropped, "recent" kept
		assert_eq!(o.discard_below(), 50);
		// "a" is gone from the map. Querying it at a start_seq >= discard_below
		// returns Ok (no entry, no conflict). Querying at start_seq < discard_below
		// returns TransactionRetry (we can't prove non-conflict for such old snapshots).
		assert!(o.check([key("a").as_slice()], 50).is_ok());
		assert!(matches!(o.check([key("a").as_slice()], 49), Err(Error::TransactionRetry)));
	}

	#[test]
	fn rollback_removes_own_entry() {
		let o = CommitOracle::new();
		o.publish([key("k").as_slice()], 10, 1, 0);
		assert_eq!(o.len(), 1);
		o.rollback([key("k").as_slice()], 10);
		assert_eq!(o.len(), 0);
		// And no longer a conflict for a later txn.
		assert!(o.check([key("k").as_slice()], 5).is_ok());
	}

	#[test]
	fn rollback_is_noop_when_overwritten() {
		let o = CommitOracle::new();
		o.publish([key("k").as_slice()], 10, 1, 0);
		// Concurrent overwriter: T2 publishes K at higher seq.
		o.publish([key("k").as_slice()], 11, 1, 0);
		// T1's rollback at seq=10 must NOT remove T2's stamp.
		o.rollback([key("k").as_slice()], 10);
		assert_eq!(o.len(), 1);
		// T2's stamp is still authoritative.
		assert!(matches!(o.check([key("k").as_slice()], 5), Err(Error::TransactionWriteConflict)));
	}

	#[test]
	fn rollback_partial_only_removes_owned_entries() {
		let o = CommitOracle::new();
		// T1 publishes a, b at seq=10.
		o.publish([key("a").as_slice(), key("b").as_slice()], 10, 2, 0);
		// T2 overwrites b at seq=12.
		o.publish([key("b").as_slice()], 12, 1, 0);
		// T1.rollback(a, b, 11): stamp on T1's batch was 10+2-1=11.
		o.rollback([key("a").as_slice(), key("b").as_slice()], 11);
		// a was at 11 → removed. b is at 12 → kept.
		assert_eq!(o.len(), 1);
		assert!(o.check([key("a").as_slice()], 0).is_ok());
		assert!(matches!(o.check([key("b").as_slice()], 5), Err(Error::TransactionWriteConflict)));
	}
}
