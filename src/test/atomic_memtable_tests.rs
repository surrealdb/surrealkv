//! Tests for the atomic `MemTable::add` contract and arena-size accounting.
//!
//! These tests verify three properties of the preflight reservation:
//!
//! 1. **Formula correctness**: `max_entry_bytes(key_len, value_len)` returns the expected `199 +
//!    key_len + value_len`.
//! 2. **Upper-bound invariant**: actual arena allocation for any batch is always `≤` the estimate
//!    computed from `max_entry_bytes`. This is what makes `try_reserve` safe: if a reservation
//!    succeeds, the subsequent inserts cannot run out of arena space.
//! 3. **Utilization gap**: the estimate is pessimistic — actual allocation uses less arena than the
//!    estimate. These tests quantify the gap for common record shapes so regressions in the formula
//!    (e.g. someone changing the skiplist node layout without recomputing `MAX_NODE_SIZE`) are
//!    caught loudly.
//!
//! They also cover the atomicity properties of `MemTable::add`:
//! batch-on-arena-full leaves the memtable unchanged; concurrent `try_reserve`
//! calls correctly serialize via CAS.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

use test_log::test;

use crate::batch::Batch;
use crate::memtable::{max_entry_bytes, MemTable};
use crate::Error;

/// Per-entry arena cost upper bound, as exposed by `max_entry_bytes`.
/// Equal to `MAX_NODE_SIZE (192) + NODE_ALIGNMENT-1 (7)`.
const ENTRY_OVERHEAD: u64 = 199;

/// Bytes the skiplist sentinels consume in the arena at MemTable construction
/// (two full-height nodes + 1 reserved offset). 199 per sentinel + 1.
const SENTINEL_OVERHEAD: usize = 2 * (ENTRY_OVERHEAD as usize) + 1;

// ============================================================================
// 1. Formula correctness
// ============================================================================

#[test]
fn max_entry_bytes_basic_inputs() {
	// Zero-length key and value: just the worst-case node + alignment padding.
	assert_eq!(max_entry_bytes(0, 0), ENTRY_OVERHEAD);
	// Small entry.
	assert_eq!(max_entry_bytes(10, 100), ENTRY_OVERHEAD + 110);
	// Larger entry.
	assert_eq!(max_entry_bytes(100, 1000), ENTRY_OVERHEAD + 1100);
}

#[test]
fn max_entry_bytes_is_linear_in_key_len() {
	let base = max_entry_bytes(0, 64);
	for k in [1usize, 10, 64, 256, 4096] {
		assert_eq!(max_entry_bytes(k, 64), base + k as u64);
	}
}

#[test]
fn max_entry_bytes_is_linear_in_value_len() {
	let base = max_entry_bytes(16, 0);
	for v in [1usize, 10, 64, 256, 4096] {
		assert_eq!(max_entry_bytes(16, v), base + v as u64);
	}
}

#[test]
fn batch_memtable_size_estimate_sums_per_entry() {
	let mut batch = Batch::new(1);
	batch.set(vec![0u8; 5], vec![0u8; 10], 0).unwrap();
	batch.set(vec![0u8; 10], vec![0u8; 20], 0).unwrap();
	batch.set(vec![0u8; 15], vec![0u8; 30], 0).unwrap();

	let expected = max_entry_bytes(5, 10) + max_entry_bytes(10, 20) + max_entry_bytes(15, 30);
	assert_eq!(batch.memtable_size_estimate(), expected);
}

#[test]
fn batch_memtable_size_estimate_treats_delete_as_zero_value() {
	let mut batch = Batch::new(1);
	batch.set(vec![0u8; 10], vec![0u8; 50], 0).unwrap();
	batch.delete(vec![0u8; 8], 0).unwrap();

	let expected = max_entry_bytes(10, 50) + max_entry_bytes(8, 0);
	assert_eq!(batch.memtable_size_estimate(), expected);
}

// ============================================================================
// 2. Upper-bound invariant: actual allocation ≤ estimate, always
// ============================================================================

/// Insert N entries of fixed shape into a fresh memtable and return
/// (estimate_bytes, actual_arena_bytes_consumed).
fn measure(key_len: usize, value_len: usize, n_entries: usize) -> (u64, usize) {
	let estimate = max_entry_bytes(key_len, value_len) * (n_entries as u64);
	// Provision plenty of headroom so the reservation succeeds regardless of
	// how unrealistically pessimistic the estimate is for this shape.
	let arena_size = estimate as usize + SENTINEL_OVERHEAD + 64 * 1024;

	let memtable = MemTable::new(arena_size);
	let initial = memtable.size();

	let mut batch = Batch::new(1);
	for i in 0..n_entries {
		let mut key = vec![0u8; key_len];
		// Stamp the index into the first few bytes of the key so entries
		// are distinct (otherwise the skiplist deduplicates them).
		for (j, b) in (i as u64).to_le_bytes().iter().enumerate() {
			if j < key.len() {
				key[j] = *b;
			}
		}
		batch.set(key, vec![0u8; value_len], 0).unwrap();
	}
	memtable.add(&batch).unwrap();
	let actual = memtable.size() - initial;
	(estimate, actual)
}

#[test]
fn estimate_is_upper_bound_on_actual_tiny_entries() {
	for n in [1, 10, 100, 1000] {
		let (estimate, actual) = measure(8, 16, n);
		assert!(
			(actual as u64) <= estimate,
			"upper bound violated: tiny entries (k=8,v=16), n={}, estimate={}, actual={}",
			n,
			estimate,
			actual
		);
	}
}

#[test]
fn estimate_is_upper_bound_on_actual_medium_entries() {
	for n in [1, 10, 100, 500] {
		let (estimate, actual) = measure(16, 256, n);
		assert!(
			(actual as u64) <= estimate,
			"upper bound violated: medium entries (k=16,v=256), n={}, estimate={}, actual={}",
			n,
			estimate,
			actual
		);
	}
}

#[test]
fn estimate_is_upper_bound_on_actual_large_values() {
	for n in [1, 10, 100] {
		let (estimate, actual) = measure(32, 8192, n);
		assert!(
			(actual as u64) <= estimate,
			"upper bound violated: large values (k=32,v=8192), n={}, estimate={}, actual={}",
			n,
			estimate,
			actual
		);
	}
}

#[test]
fn estimate_is_upper_bound_under_varied_shapes() {
	// A grid of shapes. The invariant must hold for every combination — if
	// any cell fails, MAX_NODE_SIZE or NODE_ALIGNMENT has drifted from the
	// real skiplist node layout.
	let key_lens = [1usize, 8, 32, 128, 512];
	let value_lens = [0usize, 1, 16, 256, 4096];
	for &k in &key_lens {
		for &v in &value_lens {
			let (estimate, actual) = measure(k, v, 200);
			assert!(
				(actual as u64) <= estimate,
				"upper bound violated: k={}, v={}, estimate={}, actual={}",
				k,
				v,
				estimate,
				actual
			);
		}
	}
}

// ============================================================================
// 3. Utilization gap: actual vs estimate, by record shape
// ============================================================================
//
// These tests measure (actual / estimate) and assert it falls in a documented
// range for each record shape. The expected ranges come from the geometric
// height distribution: E[height] = 1 / (1 − 1/e) ≈ 1.58, so average node
// overhead is ~45 bytes vs the worst-case 192. The gap closes as value size
// grows because the fixed node overhead is amortized.
//
// These ranges are wide enough to absorb the variance from random heights
// across the entry counts used (200–2000). If they ever fail, either the
// estimator has changed or the skiplist's node layout has changed — both
// require manual review.

fn assert_utilization_in_range(
	label: &str,
	key_len: usize,
	value_len: usize,
	n_entries: usize,
	min_ratio: f64,
	max_ratio: f64,
) {
	let (estimate, actual) = measure(key_len, value_len, n_entries);
	let ratio = (actual as f64) / (estimate as f64);
	log::info!(
		"utilization[{}] k={} v={} n={}: estimate={} actual={} ratio={:.3}",
		label,
		key_len,
		value_len,
		n_entries,
		estimate,
		actual,
		ratio
	);
	assert!(
		ratio >= min_ratio && ratio <= max_ratio,
		"utilization for {} (k={},v={},n={}) out of expected range [{:.2}, {:.2}]: \
		 estimate={} actual={} ratio={:.3}",
		label,
		key_len,
		value_len,
		n_entries,
		min_ratio,
		max_ratio,
		estimate,
		actual,
		ratio
	);
}

#[test]
fn utilization_tiny_kv_around_one_third() {
	// 8B key + 16B value: per-entry estimate 223, actual ≈ 76 ⇒ ~34%.
	assert_utilization_in_range("tiny", 8, 16, 2000, 0.25, 0.45);
}

#[test]
fn utilization_small_kv_around_one_half() {
	// 16B key + 100B value: per-entry estimate 315, actual ≈ 168 ⇒ ~53%.
	assert_utilization_in_range("small", 16, 100, 1000, 0.40, 0.65);
}

#[test]
fn utilization_medium_kv_around_three_quarters() {
	// 32B key + 512B value: per-entry estimate 743, actual ≈ 596 ⇒ ~80%.
	assert_utilization_in_range("medium", 32, 512, 500, 0.70, 0.90);
}

#[test]
fn utilization_large_value_near_one() {
	// 16B key + 4096B value: per-entry estimate 4311, actual ≈ 4160 ⇒ ~96%.
	assert_utilization_in_range("large", 16, 4096, 200, 0.93, 1.00);
}

// ============================================================================
// 4. Atomicity of `MemTable::add` on overflow
// ============================================================================

#[test]
fn add_atomic_on_arena_full_leaves_memtable_unchanged() {
	// Pick an arena that can't fit even one of these huge entries.
	let memtable = MemTable::new(2 * 1024);
	let pre_size = memtable.size();
	assert!(memtable.is_empty());

	let mut batch = Batch::new(1);
	batch.set(vec![0u8; 8], vec![0u8; 4 * 1024], 0).unwrap();
	let result = memtable.add(&batch);

	assert!(matches!(result, Err(Error::ArenaFull)), "expected ArenaFull, got {:?}", result);
	assert_eq!(
		memtable.size(),
		pre_size,
		"memtable size changed after ArenaFull — partial application leaked"
	);
	assert!(memtable.is_empty(), "memtable contains entries after a failed add");
}

#[test]
fn add_atomic_after_partial_prefill_preserves_prior_entries() {
	// Arena big enough for the prefill, too small for the second batch.
	let arena_size = 16 * 1024;
	let memtable = MemTable::new(arena_size);

	// Prefill with 8 entries of ~250 bytes each (estimate ≈ 2 KiB).
	let mut prefill = Batch::new(1);
	for i in 0..8u8 {
		prefill.set(vec![i; 8], vec![0u8; 50], 0).unwrap();
	}
	memtable.add(&prefill).unwrap();
	let post_prefill_size = memtable.size();

	// Second batch sized to overflow remaining capacity but fit a fresh memtable.
	let mut big = Batch::new(100);
	for i in 0..200u16 {
		big.set(i.to_le_bytes().to_vec(), vec![0u8; 200], 0).unwrap();
	}
	let result = memtable.add(&big);

	assert!(matches!(result, Err(Error::ArenaFull)));
	assert_eq!(
		memtable.size(),
		post_prefill_size,
		"memtable grew despite ArenaFull — partial application leaked"
	);

	// Prefill entries must still be readable.
	for i in 0..8u8 {
		let key = vec![i; 8];
		let got = memtable.get(&key, None);
		assert!(got.is_some(), "prefill entry {} missing after failed add", i);
	}

	// No entry from the second batch should be visible.
	for i in 0..200u16 {
		let key = i.to_le_bytes().to_vec();
		assert!(
			memtable.get(&key, None).is_none(),
			"entry from failed batch leaked into memtable: i={}",
			i
		);
	}
}

#[test]
fn add_releases_reservation_after_success() {
	// After a successful add returns, the reservation must be released — so a
	// subsequent reservation of the entire remaining capacity must succeed.
	let memtable = MemTable::new(64 * 1024);

	let mut batch = Batch::new(1);
	for i in 0..10u8 {
		batch.set(vec![i; 8], vec![0u8; 100], 0).unwrap();
	}
	memtable.add(&batch).unwrap();

	// Reserve every byte still available. If the previous reservation leaked,
	// this would falsely report ArenaFull.
	let capacity = memtable.arena_capacity() as u64;
	let used = memtable.size() as u64;
	let still_available = capacity - used;
	memtable.try_reserve(still_available).expect("reservation leak after successful add");
	memtable.release_reservation(still_available);
}

#[test]
fn add_releases_reservation_after_failure() {
	// Same property after a failed add: reservation must be released so a
	// later (fitting) batch can claim the space.
	let memtable = MemTable::new(2 * 1024);

	// First batch too large to fit — should fail cleanly.
	let mut huge = Batch::new(1);
	huge.set(vec![0u8; 8], vec![0u8; 4 * 1024], 0).unwrap();
	assert!(matches!(memtable.add(&huge), Err(Error::ArenaFull)));

	// A small batch must still work.
	let mut small = Batch::new(10);
	small.set(vec![1u8; 8], vec![0u8; 100], 0).unwrap();
	memtable.add(&small).expect("small batch failed after a prior ArenaFull");
}

// ============================================================================
// 5. CAS-correctness of `try_reserve` under concurrent contention
// ============================================================================

#[test]
fn try_reserve_serializes_concurrent_callers() {
	// Capacity sized for exactly 8 reservations of RESERVE_BYTES each, plus
	// sentinel overhead and a small slack. Spawn many more threads than slots
	// and assert the count of Ok results doesn't exceed what's actually available.
	const RESERVE_BYTES: u64 = 1024;
	const SLOTS: u64 = 8;
	let capacity = SENTINEL_OVERHEAD + (RESERVE_BYTES * SLOTS) as usize + 256;
	let memtable = Arc::new(MemTable::new(capacity));

	let success_count = Arc::new(AtomicUsize::new(0));
	let fail_count = Arc::new(AtomicUsize::new(0));

	let mut handles = Vec::new();
	for _ in 0..32 {
		let mt = Arc::clone(&memtable);
		let succ = Arc::clone(&success_count);
		let fail = Arc::clone(&fail_count);
		handles.push(thread::spawn(move || match mt.try_reserve(RESERVE_BYTES) {
			Ok(()) => {
				succ.fetch_add(1, Ordering::Relaxed);
			}
			Err(_) => {
				fail.fetch_add(1, Ordering::Relaxed);
			}
		}));
	}
	for h in handles {
		h.join().unwrap();
	}

	let succ = success_count.load(Ordering::Relaxed);
	let fail = fail_count.load(Ordering::Relaxed);
	assert_eq!(succ + fail, 32, "every thread should report a single outcome");
	assert!(
		(succ as u64) <= SLOTS,
		"more reservations succeeded ({}) than the arena had slots for ({})",
		succ,
		SLOTS
	);

	// Total reserved must equal succ * RESERVE_BYTES. Release everything and
	// verify a fresh reservation of the entire reserved budget succeeds.
	let released = (succ as u64) * RESERVE_BYTES;
	for _ in 0..succ {
		memtable.release_reservation(RESERVE_BYTES);
	}
	// After release, the original budget must again be reservable.
	memtable.try_reserve(released).expect("budget didn't return after releases");
}

#[test]
fn try_reserve_zero_always_succeeds() {
	// Edge case: reserving zero bytes is a no-op and must always succeed.
	let memtable = MemTable::new(1024);
	memtable.try_reserve(0).expect("try_reserve(0) failed on empty memtable");
	// And after the arena is full.
	let mut batch = Batch::new(1);
	batch.set(vec![0u8; 8], vec![0u8; 4096], 0).unwrap();
	let _ = memtable.add(&batch); // expected to fail
	memtable.try_reserve(0).expect("try_reserve(0) failed on full memtable");
}
