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
//   `recent_writes` contains every commit at `seq >= discard_below`. GC runs
//   inside `publish` under two gates:
//     (a) a commit-count throttle (`commits_since_gc >= GC_INTERVAL`) to
//         amortize the O(N) HashMap walk, and
//     (b) a watermark advance check (`oldest_active > last_cleanup`) to skip
//         walks that would do no work.
//   The map's size is bounded by `max(live-reader window, GC_INTERVAL × W)`
//   times the average keys-per-commit.

use std::collections::HashMap;

use parking_lot::Mutex;
use xxhash_rust::xxh3::xxh3_64;

use crate::error::{Error, Result};

/// Throttle on GC frequency inside `publish`. GC runs at most once per
/// `GC_INTERVAL` publishes, and only when `oldest_active` has advanced since
/// the last sweep. Internal constant — not exposed via `Options`.
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

	// Highest `oldest_active` seen by a GC pass. Used as the skip-if-unchanged
	// gate: if the watermark hasn't advanced since the last sweep there is no
	// new work to do.
	last_cleanup_oldest_active: u64,

	// Publishes since the last GC sweep. Bounds GC frequency to amortize the
	// O(N) `HashMap::retain` cost. Reset to 0 inside the GC body.
	// `saturating_add` on increment so it doesn't overflow during long stretches
	// where the watermark is pinned (e.g. one long reader holding GC back).
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
				last_cleanup_oldest_active: 0,
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

	/// Publish this commit's writes into the map; opportunistically GC.
	///
	/// Stamping: every key in this batch is stamped with `seq_num + count - 1`
	/// (the highest seq in the batch). Conservative — a future txn whose
	/// `start_seq` falls inside `[seq_num, seq_num+count-1)` may see a false
	/// conflict, but no real conflict is ever missed.
	///
	/// GC cadence: throttled by `commits_since_gc >= GC_INTERVAL` AND gated by
	/// `oldest_active > last_cleanup_oldest_active`. Most calls do nothing
	/// beyond an increment and a comparison — the O(N) `retain` runs at most
	/// once per `GC_INTERVAL` publishes, and only when there's actually work to
	/// do (the watermark has advanced since the previous sweep).
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

		// `saturating_add` so the counter doesn't overflow if the watermark
		// stays pinned across billions of commits (theoretical edge case under
		// a stuck long-running reader). Once the watermark moves, the GC body
		// runs and resets the counter to 0.
		g.commits_since_gc = g.commits_since_gc.saturating_add(1);

		// Two gates, both required:
		//   (a) Enough commits since the last sweep — perf throttle.
		//   (b) Watermark has advanced — skip fruitless walks.
		// `oldest_active` is provably non-decreasing across calls (it's derived
		// from `min(start_seq)` over live txns, and `visible_seq_num` is
		// monotonic). The debug_assert catches any future code path that
		// violates that invariant.
		if g.commits_since_gc >= GC_INTERVAL && oldest_active > g.last_cleanup_oldest_active {
			debug_assert!(
				oldest_active >= g.last_cleanup_oldest_active,
				"oldest_active must be monotonic"
			);
			g.commits_since_gc = 0;
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
	/// Resets the GC bookkeeping (`commits_since_gc`, `last_cleanup_oldest_active`)
	/// along with the map so post-restore behavior starts from a clean slate.
	///
	/// NOT called at startup. A freshly-constructed oracle already has an
	/// empty map and `discard_below = 0`, and the first post-startup txn has
	/// `start_seq = max_recovered_seq` which trivially passes the
	/// `start_seq < discard_below` check.
	pub(crate) fn reset_for_restore(&self, max_seq: u64) {
		let mut g = self.inner.lock();
		g.discard_below = max_seq;
		g.last_cleanup_oldest_active = max_seq;
		g.commits_since_gc = 0;
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

		// Counter was also reset. Verify indirectly: GC_INTERVAL-1 publishes
		// after the reset must NOT trigger another GC (counter starts at 0
		// post-reset, so we can do GC_INTERVAL-1 publishes without hitting
		// the threshold). If counter weren't reset, GC could fire prematurely
		// and disturb discard_below.
		for i in 0..(GC_INTERVAL - 1) {
			// Stamp >= discard_below(=100) so entries are kept; watermark
			// advancing would change discard_below if GC fires.
			o.publish([format!("k{i}").as_bytes()], 200 + i as u64, 1, 150);
		}
		// GC did NOT fire — discard_below still 100 (the reset value), not 150.
		assert_eq!(o.discard_below(), 100);
		assert_eq!(o.len(), (GC_INTERVAL - 1) as usize);
	}

	#[test]
	fn gc_skips_when_watermark_unchanged() {
		let o = CommitOracle::new();

		// Phase 1: push GC_INTERVAL publishes at watermark=5. At the last one,
		// counter hits GC_INTERVAL and watermark=5 > last_cleanup(=0), so GC
		// fires. All entries have seq >= 10, all retained. discard_below=5.
		for i in 0..GC_INTERVAL {
			o.publish([format!("a{i}").as_bytes()], 10 + i as u64, 1, 5);
		}
		assert_eq!(o.len(), GC_INTERVAL as usize, "all entries kept (all seq >= 5)");
		assert_eq!(o.discard_below(), 5);

		// Phase 2: push another GC_INTERVAL publishes at the SAME watermark=5.
		// Counter reaches GC_INTERVAL again, but watermark(5) > last_cleanup(5)
		// is false → GC body skipped. Map grows; discard_below unchanged.
		for i in 0..GC_INTERVAL {
			o.publish([format!("b{i}").as_bytes()], 10_000 + i as u64, 1, 5);
		}
		assert_eq!(o.len(), (2 * GC_INTERVAL) as usize, "second batch retained, no GC fired");
		assert_eq!(o.discard_below(), 5, "discard_below unchanged when watermark unchanged");
	}

	#[test]
	fn gc_runs_when_watermark_advances() {
		let o = CommitOracle::new();

		// Push GC_INTERVAL - 1 entries at watermark=0. No GC (counter not
		// yet at threshold). All entries retained.
		for i in 0..(GC_INTERVAL - 1) {
			o.publish([format!("low{i}").as_bytes()], 10 + i as u64, 1, 0);
		}
		assert_eq!(o.len(), (GC_INTERVAL - 1) as usize);
		assert_eq!(o.discard_below(), 0);

		// One more publish at watermark=50. Counter hits GC_INTERVAL AND
		// watermark advanced → GC fires. Entries with seq < 50 are dropped.
		// The "low" entries are at seqs 10..(10 + GC_INTERVAL - 1) = 10..1033;
		// those below 50 (10..49) get dropped, the rest stay. Plus the new
		// entry "recent" at seq 99_999.
		o.publish([key("recent").as_slice()], 99_999, 1, 50);
		assert_eq!(o.discard_below(), 50);
		// 40 entries below threshold (seqs 10..49) dropped; (GC_INTERVAL-1)-40
		// "low" entries kept + "recent" = GC_INTERVAL - 40.
		assert_eq!(o.len() as u32, GC_INTERVAL - 40);
		// Old txns retry; new ones see "recent" and conflict only if their
		// start_seq < 99_999.
		assert!(matches!(o.check([key("recent").as_slice()], 49), Err(Error::TransactionRetry)));
	}

	#[test]
	fn gc_throttle_skips_below_interval() {
		// Fewer than GC_INTERVAL publishes with monotonically advancing
		// watermark MUST NOT trigger GC. Counter must reach the threshold
		// even when the watermark advances on every call.
		let o = CommitOracle::new();
		for i in 0..(GC_INTERVAL - 1) {
			o.publish([format!("k{i}").as_bytes()], 10 + i as u64, 1, i as u64 + 1);
		}
		assert_eq!(o.len(), (GC_INTERVAL - 1) as usize);
		// discard_below stays at its initial 0 — GC never ran despite watermark
		// advancing on every call.
		assert_eq!(o.discard_below(), 0);
	}

	#[test]
	fn gc_throttle_resets_counter_after_sweep() {
		let o = CommitOracle::new();

		// Trigger one GC sweep: GC_INTERVAL publishes with the last one at a
		// higher watermark.
		for i in 0..(GC_INTERVAL - 1) {
			o.publish([format!("a{i}").as_bytes()], 10 + i as u64, 1, 0);
		}
		o.publish([key("trigger").as_slice()], 99_999, 1, 50);
		assert_eq!(o.discard_below(), 50, "GC fired on first batch");

		// After the sweep, counter is back at 0. Push GC_INTERVAL - 1 more
		// publishes with a further-advancing watermark; GC must NOT fire again
		// (counter < threshold).
		for i in 0..(GC_INTERVAL - 1) {
			o.publish([format!("b{i}").as_bytes()], 200_000 + i as u64, 1, 100 + i as u64);
		}
		// discard_below still 50 — second GC didn't fire because counter
		// hadn't hit the threshold again.
		assert_eq!(o.discard_below(), 50, "second GC did not fire after partial batch");
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
