// Commit oracle for Snapshot-Isolation write-write conflict detection.
//
// The oracle keeps an in-memory record of recent committed writes
// (key -> highest commit seq that wrote it) plus a GC watermark.
// Validation reads the map; insertion happens atomically with seq allocation
// under the commit pipeline's `write_mutex`. This lets memtable `apply()`
// stay outside the lock so the commit pipeline retains its Pebble-style
// overlap between WAL of batch N+1 and apply of batch N.
//
// See plan: /Users/kfarhan/.claude/plans/plan-properly-and-why-wise-crane.md

use std::collections::HashMap;

use parking_lot::Mutex;

use crate::error::{Error, Result};

/// Number of `publish` calls between GC sweeps.
pub(crate) const GC_INTERVAL: u32 = 1024;

pub(crate) struct CommitOracle {
	inner: Mutex<OracleInner>,
}

struct OracleInner {
	// key bytes -> commit_seq of the most recent writer of that key.
	// Full keys (not hashes): correctness over memory; deterministic test behavior.
	recent_writes: HashMap<Vec<u8>, u64>,

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
			if let Some(&committed) = g.recent_writes.get(k) {
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
	/// conflict is ever missed. This trades a marginal false-abort window
	/// for simpler code than per-entry stamping.
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
			g.recent_writes.insert(k.to_vec(), stamp);
		}
		g.commits_since_gc += 1;
		if g.commits_since_gc >= GC_INTERVAL {
			g.commits_since_gc = 0;
			g.discard_below = oldest_active;
			g.recent_writes.retain(|_, v| *v >= oldest_active);
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
		// T2 started at seq=10 — has snapshot >= committed_at, no conflict.
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
		// Batch of 3 starting at seq=10: keys stamped at 10+3-1 = 12.
		o.publish([key("a").as_slice(), key("b").as_slice(), key("c").as_slice()], 10, 3, 0);
		// A txn at start_seq=11 conflicts because stamp=12 > 11.
		assert!(matches!(o.check([key("a").as_slice()], 11), Err(Error::TransactionWriteConflict)));
		// A txn at start_seq=12 passes (12 > 12 is false).
		assert!(o.check([key("a").as_slice()], 12).is_ok());
	}

	#[test]
	fn seed_resets_state() {
		let o = CommitOracle::new();
		o.publish([key("k").as_slice()], 10, 1, 0);
		o.seed(100);
		assert_eq!(o.len(), 0);
		assert_eq!(o.discard_below(), 100);
		// A txn at start_seq=99 < 100 must retry.
		assert!(matches!(o.check([key("k").as_slice()], 99), Err(Error::TransactionRetry)));
		// A txn at start_seq=100 is accepted (strict <).
		assert!(o.check([key("k").as_slice()], 100).is_ok());
	}

	#[test]
	fn gc_drops_entries_below_watermark() {
		let o = CommitOracle::new();
		// Hit GC_INTERVAL-1 publishes at low seqs.
		for i in 0..(GC_INTERVAL - 1) {
			o.publish([format!("k{i}").as_bytes()], i as u64 + 1, 1, 0);
		}
		// One more publish with a high oldest_active to trigger GC.
		o.publish([key("recent").as_slice()], 99_999, 1, 50);
		// After GC at threshold=50, only entries with seq>=50 remain.
		// k0..k48 had seqs 1..49 → dropped. k49.. and "recent" remain.
		assert!(o.discard_below() == 50);
		assert!(o.len() < GC_INTERVAL as usize); // map shrank
	}
}
