// Sharded minimum-seq tracker for MVCC active-set bookkeeping.
//
// Replaces the global `crossbeam_skiplist::SkipSet` previously used by
// `SnapshotTracker` and `ActiveTxnTracker`. Those sets are inserted on every
// transaction begin and removed on every commit/drop, but only ever read for
// their MINIMUM (the GC watermark) plus a rare full scan for compaction. Under
// high create/destroy churn a lock-free skiplist accumulates logically-deleted
// nodes faster than epoch reclamation frees them, so its traversals
// (`search_position`) and cleanup (`help_unlink`) dominated CPU (~40% in a
// crud-bench flamegraph).
//
// Design (see plan): `SHARDS` independent shards, each a small
// `Mutex<BTreeMap<seq, refcount>>` plus an `AtomicU64` cache of that shard's
// current minimum (`EMPTY` when the shard is empty). A transaction registers
// into a shard chosen by a per-thread index (assigned once per thread,
// round-robin), so mutation contention is divided across shards instead of
// hammering one global lock. `oldest()` is lock-free: the min over the shard
// `AtomicU64`s. Refcounts make duplicate seqs correct (concurrent begins share
// `start_seq` because it is loaded from `visible_seq_num`, which only advances
// on commit).
//
// Soundness of `oldest()` as a GC watermark: an under-estimate (returning a
// value <= the true minimum active seq) is always safe — the oracle just GCs
// less and retains more. The true minimum is monotonically non-decreasing (new
// transactions get higher `start_seq`), so the only staleness window — a
// register whose `Release` store of `shard.min` is not yet observed by a
// concurrent `oldest()` — is the same load-then-register race already
// documented and accepted in `tracker.rs`, and is further bounded by the
// `.min(start_seq)` clamp at the commit site. It never over-estimates in steady
// state.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use parking_lot::Mutex;

/// Number of independent shards. Chosen >= the box's core count so that, even
/// with more OS threads than cores, only a couple of threads share any shard.
const SHARDS: usize = 64;

/// Sentinel stored in a shard's `min` atomic when that shard is empty.
const EMPTY: u64 = u64::MAX;

struct Shard {
	map: Mutex<BTreeMap<u64, u32>>,
	/// Cached copy of `map`'s smallest key (or `EMPTY`). Maintained under
	/// `map`'s lock with `Release`; read lock-free with `Acquire`.
	min: AtomicU64,
}

/// Global round-robin source for per-thread shard assignment. Touched at most
/// once per thread (the result is cached in a thread-local), so it is not a
/// hot-path atom.
static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
	/// This thread's shard index, assigned lazily on first use. Shared across
	/// all `ShardedMinTracker` instances (each has its own `SHARDS`-sized array,
	/// so the index is just a position).
	static SHARD_IDX: Cell<usize> = const { Cell::new(usize::MAX) };
}

#[inline]
fn my_shard() -> usize {
	SHARD_IDX.with(|c| {
		let v = c.get();
		if v == usize::MAX {
			let s = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % SHARDS;
			c.set(s);
			s
		} else {
			v
		}
	})
}

#[doc(hidden)] // Exposed for `benches/tracker_bench.rs`; not a supported public API.
pub struct ShardedMinTracker {
	shards: Box<[Shard]>,
}

impl Default for ShardedMinTracker {
	fn default() -> Self {
		Self::new()
	}
}

impl ShardedMinTracker {
	pub fn new() -> Self {
		let shards = (0..SHARDS)
			.map(|_| Shard {
				map: Mutex::new(BTreeMap::new()),
				min: AtomicU64::new(EMPTY),
			})
			.collect::<Vec<_>>()
			.into_boxed_slice();
		Self {
			shards,
		}
	}

	/// Register one transaction at `seq`. Returns the shard index, which the
	/// caller MUST pass back to `unregister` (it identifies which shard holds
	/// the entry; shard choice is per-thread, not derivable from `seq`).
	pub fn register(&self, seq: u64) -> usize {
		let idx = my_shard();
		let shard = &self.shards[idx];
		let mut map = shard.map.lock();
		*map.entry(seq).or_insert(0) += 1;
		// `map` is non-empty (we just inserted); the smallest key is the new min.
		let new_min = *map.keys().next().unwrap();
		shard.min.store(new_min, Ordering::Release);
		idx
	}

	/// Unregister one transaction previously registered at `(shard, seq)`.
	/// Safe if the entry is missing (no-op), so an unbalanced caller cannot
	/// underflow.
	pub fn unregister(&self, shard: usize, seq: u64) {
		let shard = &self.shards[shard];
		let mut map = shard.map.lock();
		if let Some(c) = map.get_mut(&seq) {
			*c -= 1;
			if *c == 0 {
				map.remove(&seq);
			}
		}
		let new_min = map.keys().next().copied().unwrap_or(EMPTY);
		shard.min.store(new_min, Ordering::Release);
	}

	/// Smallest registered seq, or `None` if no transactions are registered.
	/// Lock-free: a min over the per-shard cached minimums.
	pub fn oldest(&self) -> Option<u64> {
		let mut m = EMPTY;
		for s in self.shards.iter() {
			let v = s.min.load(Ordering::Acquire);
			if v < m {
				m = v;
			}
		}
		if m == EMPTY {
			None
		} else {
			Some(m)
		}
	}

	/// All distinct registered seqs, sorted ascending. Used by compaction only
	/// (rare). Point-in-time: it may include a concurrently-removed seq (safe —
	/// retains extra) and cannot miss a seq already present in a locked shard.
	pub fn get_all(&self) -> Vec<u64> {
		let mut out: Vec<u64> = Vec::new();
		for s in self.shards.iter() {
			let map = s.map.lock();
			out.extend(map.keys().copied());
		}
		out.sort_unstable();
		out.dedup();
		out
	}

	/// Total active registrations (sum of refcounts) — matches the old
	/// `SkipSet::len()` semantics (each registration counted once).
	#[cfg(test)]
	pub fn len(&self) -> usize {
		self.shards.iter().map(|s| s.map.lock().values().map(|&c| c as usize).sum::<usize>()).sum()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn empty_is_none() {
		let t = ShardedMinTracker::new();
		assert_eq!(t.oldest(), None);
		assert_eq!(t.len(), 0);
		assert!(t.get_all().is_empty());
	}

	#[test]
	fn min_picks_smallest() {
		let t = ShardedMinTracker::new();
		// Single-threaded: all land in this thread's one shard, so the BTreeMap
		// min is the global min.
		let a = t.register(20);
		let b = t.register(10);
		let c = t.register(15);
		assert_eq!(t.oldest(), Some(10));
		assert_eq!(t.len(), 3);
		t.unregister(b, 10);
		assert_eq!(t.oldest(), Some(15));
		t.unregister(c, 15);
		t.unregister(a, 20);
		assert_eq!(t.oldest(), None);
	}

	#[test]
	fn dup_seq_refcount() {
		let t = ShardedMinTracker::new();
		let g1 = t.register(5);
		let g2 = t.register(5);
		assert_eq!(t.len(), 2, "two registrations at the same seq are both counted");
		t.unregister(g1, 5);
		assert_eq!(t.oldest(), Some(5), "still one live registration at 5");
		assert_eq!(t.len(), 1);
		t.unregister(g2, 5);
		assert_eq!(t.oldest(), None);
		assert_eq!(t.len(), 0);
	}

	#[test]
	fn unregister_missing_is_safe() {
		let t = ShardedMinTracker::new();
		let g = t.register(7);
		// Unregister twice: the second is a no-op (no underflow).
		t.unregister(g, 7);
		t.unregister(g, 7);
		assert_eq!(t.oldest(), None);
		assert_eq!(t.len(), 0);
	}

	#[test]
	fn get_all_distinct_sorted() {
		let t = ShardedMinTracker::new();
		let _a = t.register(100);
		let _b = t.register(50);
		let _c = t.register(200);
		let _d = t.register(50); // duplicate seq
		assert_eq!(t.get_all(), vec![50, 100, 200], "distinct, sorted");
	}

	#[test]
	fn concurrent_register_unregister() {
		use std::sync::Arc;
		use std::thread;
		let t = Arc::new(ShardedMinTracker::new());
		let mut handles = Vec::new();
		for tid in 0..8u64 {
			let t = Arc::clone(&t);
			handles.push(thread::spawn(move || {
				for i in 0..10_000u64 {
					let seq = tid * 100_000 + i;
					let s = t.register(seq);
					// oldest() must always be <= our just-registered seq
					// (under-estimate-safe) and never None while we hold one.
					let o = t.oldest().unwrap();
					assert!(o <= seq);
					t.unregister(s, seq);
				}
			}));
		}
		for h in handles {
			h.join().unwrap();
		}
		assert_eq!(t.oldest(), None, "all unregistered");
		assert_eq!(t.len(), 0);
	}
}
