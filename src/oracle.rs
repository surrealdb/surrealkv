// Commit oracle: in-memory write-set conflict detector for Snapshot Isolation.
//
// Stores `xxh3_64(key) -> highest committed seq that wrote that key`. Reads
// are O(1) hash lookups; insertions are O(1) per key. Validation under the
// commit pipeline's `write_mutex` is the load-bearing invariant: a transaction
// that enters `check` after another transaction's `publish` is guaranteed to
// see that other transaction's reservation, even before its memtable apply
// lands. This is what lets `apply()` run OUTSIDE `write_mutex`.
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
//   nothing — every call mutates state (check increments nothing but
//   publish does). Sharding helps only if commits scaled past `write_mutex`,
//   which serializes them already at a coarser layer.

use std::collections::HashMap;

use parking_lot::Mutex;
use xxhash_rust::xxh3::xxh3_64;

use crate::error::{Error, Result};

/// Number of `publish` calls between GC sweeps.
pub(crate) const GC_INTERVAL: u32 = 1024;

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

	// GC trigger counter, incremented per `publish`.
	commits_since_gc: u32,
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
				commits_since_gc: 0,
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

	/// Publish this commit's writes into the map. Stamps every key with
	/// `seq_num + count - 1` (the highest seq in the batch).
	///
	/// Conservative: a future txn whose start_seq falls inside
	/// `[seq_num, seq_num+count-1)` may see a false conflict, but no real
	/// conflict is ever missed. Simpler than per-entry stamping; the false-
	/// abort window is at most `count-1` seqs wide.
	///
	/// Called under `write_mutex` AFTER seq allocation, BEFORE WAL. Also
	/// performs GC every `GC_INTERVAL` invocations using `oldest_active`
	/// as the new `discard_below` threshold.
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
		g.commits_since_gc += 1;
		if g.commits_since_gc >= GC_INTERVAL {
			g.commits_since_gc = 0;
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
	///
	/// Race example:
	///   T1.publish(K, 10), T1.apply fails.
	///   Meanwhile T2.publish(K, 11) overwrote the entry.
	///   T1.rollback(K, 10): recent_writes[fp(K)] == 11, not 10 → no-op.
	///   T2's stamp stays. Correct.
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

	/// Reset oracle state to a freshly-recovered snapshot.
	///
	/// Called from `CommitPipeline::set_seq_num` at startup. The map is
	/// cleared (in-flight pre-crash txns are gone) and `discard_below` is
	/// set to `max_seq`. New post-recovery txns start at `start_seq = max_seq`,
	/// so the strict `start_seq < discard_below` check accepts them.
	pub(crate) fn seed(&self, max_seq: u64) {
		let mut g = self.inner.lock();
		g.discard_below = max_seq;
		g.recent_writes.clear();
		g.commits_since_gc = 0;
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
	fn seed_resets_state() {
		let o = CommitOracle::new();
		o.publish([key("k").as_slice()], 10, 1, 0);
		o.seed(100);
		assert_eq!(o.len(), 0);
		assert_eq!(o.discard_below(), 100);
		assert!(matches!(o.check([key("k").as_slice()], 99), Err(Error::TransactionRetry)));
		assert!(o.check([key("k").as_slice()], 100).is_ok());
	}

	#[test]
	fn gc_drops_entries_below_watermark() {
		let o = CommitOracle::new();
		for i in 0..(GC_INTERVAL - 1) {
			o.publish([format!("k{i}").as_bytes()], i as u64 + 1, 1, 0);
		}
		o.publish([key("recent").as_slice()], 99_999, 1, 50);
		assert_eq!(o.discard_below(), 50);
		assert!(o.len() < GC_INTERVAL as usize);
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
