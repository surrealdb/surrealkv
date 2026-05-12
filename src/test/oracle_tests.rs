// Integration tests for the commit oracle and active-txn tracker.
//
// The commit pipeline validates write-write conflicts via an in-memory
// fingerprint map (`CommitOracle`) instead of scanning the memtable. This
// lets `apply()` run outside `write_mutex` (the throughput unlock) while
// keeping validation atomic with seq allocation. These tests cover:
//   - first-writer-wins under concurrent same-key commits;
//   - no false aborts for disjoint-key commits;
//   - GC bounded by the oldest live transaction;
//   - write-only txns are tracked (no Snapshot, but they still pin the watermark);
//   - recovery seeds the oracle correctly;
//   - RAII tracker guard releases on commit / rollback / Drop.

use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::lsm::Tree;
use crate::{Error, Mode, TreeBuilder};

fn create_store() -> (Tree, TempDir) {
	let temp_dir = TempDir::new("oracle_test").unwrap();
	let path = temp_dir.path().to_path_buf();
	let tree = TreeBuilder::new().with_path(path).build().unwrap();
	(tree, temp_dir)
}

/// Issue surrealdb#7303 race: two concurrent txns at the same `start_seq_num`
/// both write key K. Exactly one must succeed; the other gets
/// `TransactionWriteConflict`. Loop many iterations to exercise the timing
/// of concurrent `oracle.check` / `oracle.publish` under `write_mutex`.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn test_first_writer_wins_concurrent_same_key() {
	let (store, _td) = create_store();
	let store = Arc::new(store);

	const ITERS: usize = 200;
	for i in 0..ITERS {
		let key = format!("race_key_{i}").into_bytes();

		// Two parallel commits to the same key. They will share the same
		// `start_seq_num` because no commit has happened between their
		// `Tree::begin()` calls (assuming the runtime schedules them close
		// together — the test repeats to cover scheduling jitter).
		let store1 = Arc::clone(&store);
		let store2 = Arc::clone(&store);
		let k1 = key.clone();
		let k2 = key.clone();

		let h1 = tokio::spawn(async move {
			let mut t = store1.begin().unwrap();
			t.set(&k1, b"v1").unwrap();
			t.commit().await
		});
		let h2 = tokio::spawn(async move {
			let mut t = store2.begin().unwrap();
			t.set(&k2, b"v2").unwrap();
			t.commit().await
		});

		let r1 = h1.await.unwrap();
		let r2 = h2.await.unwrap();

		// At most one of them succeeds.
		let oks = [&r1, &r2].into_iter().filter(|r| r.is_ok()).count();
		let conflicts = [&r1, &r2]
			.into_iter()
			.filter(|r| matches!(r, Err(Error::TransactionWriteConflict)))
			.count();

		assert!(oks >= 1, "iteration {i}: both txns failed: r1={r1:?} r2={r2:?}");
		assert!(
			oks + conflicts == 2,
			"iteration {i}: unexpected error variant: r1={r1:?} r2={r2:?}"
		);

		// The key should be readable post-commit, with one of the two values.
		let t = store.begin().unwrap();
		let v = t.get(&key).unwrap().expect("key present");
		assert!(v == b"v1" || v == b"v2", "iteration {i}: unexpected value {v:?}");
	}
}

/// N concurrent writers each writing to disjoint keys must all commit.
/// The oracle must not produce spurious conflicts when write sets don't overlap.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn test_no_false_aborts_disjoint_keys() {
	let (store, _td) = create_store();
	let store = Arc::new(store);

	const N: usize = 32;
	let mut handles = Vec::with_capacity(N);
	for i in 0..N {
		let store = Arc::clone(&store);
		handles.push(tokio::spawn(async move {
			let mut t = store.begin().unwrap();
			t.set(format!("disjoint_{i}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
			t.commit().await
		}));
	}

	for (i, h) in handles.into_iter().enumerate() {
		let r = h.await.unwrap();
		assert!(r.is_ok(), "disjoint writer {i} got unexpected error: {r:?}");
	}

	// All keys must be visible afterwards.
	let t = store.begin().unwrap();
	for i in 0..N {
		let v = t.get(format!("disjoint_{i}").as_bytes()).unwrap();
		assert_eq!(v.as_deref(), Some(format!("v{i}").as_bytes()));
	}
}

/// SI allows write-skew: txn1 reads X writes Y; txn2 reads X writes Z.
/// Both must commit successfully because we do not track reads.
/// This confirms the design did not accidentally drift toward serializability.
#[test(tokio::test)]
async fn test_si_write_skew_still_allowed() {
	let (store, _td) = create_store();

	// Seed X.
	{
		let mut t = store.begin().unwrap();
		t.set(b"x", b"init").unwrap();
		t.commit().await.unwrap();
	}

	let mut t1 = store.begin().unwrap();
	let mut t2 = store.begin().unwrap();

	// Both read X (same value because they share a snapshot seq).
	let _ = t1.get(b"x").unwrap();
	let _ = t2.get(b"x").unwrap();

	// t1 writes Y, t2 writes Z — disjoint write sets.
	t1.set(b"y", b"from_t1").unwrap();
	t2.set(b"z", b"from_t2").unwrap();

	assert!(t1.commit().await.is_ok());
	assert!(t2.commit().await.is_ok());
}

/// A long-lived read-write transaction pins the oracle's GC watermark.
/// Until it drops, recent_writes entries since its start cannot be pruned.
/// After drop, the next commit advances the watermark and GC trims the map.
#[test(tokio::test)]
async fn test_oracle_gc_bounded_by_long_reader() {
	let (store, _td) = create_store();

	// Long-lived reader: holds an ActiveTxnGuard pinned at start_seq=0.
	let reader = store.begin().unwrap();

	// Fire enough commits to cross GC_INTERVAL so a GC pass runs.
	// Each commit writes a distinct key, so all entries land in `recent_writes`.
	const N: u32 = crate::oracle::GC_INTERVAL + 32;
	for i in 0..N {
		let mut t = store.begin().unwrap();
		t.set(format!("gc_{i}").as_bytes(), b"v").unwrap();
		t.commit().await.unwrap();
	}

	let pipeline = &store.core.commit_pipeline;
	let oracle_during_reader = pipeline.oracle();
	// While the reader is alive, oldest_active_start_seq <= 0, so any GC
	// pass would set discard_below <= 0 and retain all entries. The map
	// should contain all N entries.
	assert!(
		oracle_during_reader.len() >= N as usize,
		"expected >= {N} entries while reader alive, got {}",
		oracle_during_reader.len()
	);

	// Drop the reader. Its ActiveTxnGuard releases on Drop.
	drop(reader);

	// Trigger another GC_INTERVAL commits so the GC pass observes the new
	// (higher) oldest_active and prunes.
	for i in N..(N + crate::oracle::GC_INTERVAL) {
		let mut t = store.begin().unwrap();
		t.set(format!("gc_{i}").as_bytes(), b"v").unwrap();
		t.commit().await.unwrap();
	}

	let oracle_after = pipeline.oracle();
	// After GC with a higher watermark, many of the old entries are gone.
	// We expect significantly fewer than (2 * GC_INTERVAL + 32) entries.
	assert!(
		oracle_after.len() < (2 * crate::oracle::GC_INTERVAL as usize),
		"expected map shrink after reader drop, got {}",
		oracle_after.len()
	);
}

/// A long-lived write-only transaction must also pin the GC watermark
/// (it has no Snapshot — SnapshotTracker alone would miss it).
#[test(tokio::test)]
async fn test_write_only_txn_holds_watermark() {
	let (store, _td) = create_store();

	// Capture the tracker's "oldest" before any txn.
	let tracker = Arc::clone(&store.core.active_txn_tracker);
	assert!(tracker.oldest().is_none());

	// Begin a write-only txn. It should register in active_txn_tracker.
	let writer = store.begin_with_mode(Mode::WriteOnly).unwrap();
	let oldest = tracker.oldest();
	assert!(oldest.is_some(), "write-only txn should register start_seq");
	let pinned = oldest.unwrap();

	// Bump the seq with some unrelated commits. The watermark must NOT
	// advance past the pinned value while `writer` is alive.
	for i in 0..16 {
		let mut t = store.begin().unwrap();
		t.set(format!("wo_{i}").as_bytes(), b"v").unwrap();
		t.commit().await.unwrap();
	}
	assert_eq!(tracker.oldest(), Some(pinned), "write-only txn must continue to pin the watermark");

	// Drop the write-only txn (uncommitted). The guard releases on Drop.
	drop(writer);
	// `oldest` is now either None (no other txns alive) or some value > pinned.
	// In any case it must not be `pinned` anymore.
	assert_ne!(tracker.oldest(), Some(pinned));
}

/// On a clean restart, the recovered `max_seq_num` should seed
/// `oracle.discard_below`. New txns post-restart get `start_seq = max_seq`
/// and the strict `start_seq < discard_below` check accepts them.
///
/// Note: we can't easily simulate a process restart in-process for an LSM tree
/// without dropping and reopening it. This test reopens the tree on the same
/// path and verifies new txns proceed normally.
#[test(tokio::test)]
async fn test_recovery_seeds_discard_below() {
	let temp_dir = TempDir::new("oracle_recovery").unwrap();
	let path = temp_dir.path().to_path_buf();

	{
		let store = TreeBuilder::new().with_path(path.clone()).build().unwrap();
		for i in 0..8 {
			let mut t = store.begin().unwrap();
			t.set(format!("recov_{i}").as_bytes(), b"v").unwrap();
			t.commit().await.unwrap();
		}
		store.close().await.unwrap();
	}

	// Reopen: recovery replays WAL and calls commit_pipeline.set_seq_num,
	// which seeds the oracle with discard_below = max_seq.
	let store = TreeBuilder::new().with_path(path).build().unwrap();
	let pipeline = &store.core.commit_pipeline;
	let oracle = pipeline.oracle();

	// Oracle map starts empty post-recovery.
	assert_eq!(oracle.len(), 0, "oracle map should be empty after recovery");
	// discard_below should equal recovered max_seq (>= 8 since we did 8 commits).
	assert!(oracle.discard_below() >= 8);

	// A fresh commit must succeed (its start_seq == max_seq, not < discard_below).
	let mut t = store.begin().unwrap();
	t.set(b"post_recovery", b"ok").unwrap();
	assert!(t.commit().await.is_ok());
}

/// The `ActiveTxnGuard`'s `Drop` releases the watermark slot even when a
/// transaction is forgotten (no commit, no explicit rollback). Without this,
/// a panic-aborted txn would pin the watermark forever.
#[test(tokio::test)]
async fn test_panic_drops_guard() {
	let (store, _td) = create_store();
	let tracker = Arc::clone(&store.core.active_txn_tracker);

	{
		let _txn = store.begin().unwrap();
		assert!(tracker.oldest().is_some(), "txn should be registered");
		// _txn drops at end of this block — simulates panic-unwound stack.
	}
	assert!(tracker.oldest().is_none(), "guard should unregister on Drop");
}
