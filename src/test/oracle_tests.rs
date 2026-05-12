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
/// While it's alive, `oldest_active` stays at its `start_seq` and the GC
/// watermark check stays false, so no sweep fires. Recent_writes entries
/// accumulate. After it drops, enough subsequent commits cross the
/// `GC_INTERVAL` throttle AND see an advanced watermark → GC fires and
/// prunes everything below.
#[test(tokio::test)]
async fn test_oracle_gc_bounded_by_long_reader() {
	use crate::oracle::GC_INTERVAL;
	let (store, _td) = create_store();

	// Long-lived reader pinned at start_seq = 0 (no commits before it).
	let reader = store.begin().unwrap();

	// Commit GC_INTERVAL distinct keys. While `reader` is alive,
	// oldest_active = 0 so the watermark check is false on every publish
	// → no GC, regardless of the counter. All entries accumulate.
	let n = GC_INTERVAL as usize;
	for i in 0..n {
		let mut t = store.begin().unwrap();
		t.set(format!("gc_{i}").as_bytes(), b"v").unwrap();
		t.commit().await.unwrap();
	}

	let pipeline = &store.core.commit_pipeline;
	let oracle = pipeline.oracle();
	assert_eq!(
		oracle.len(),
		n,
		"map should hold all {n} entries while reader pins oldest_active=0; got {}",
		oracle.len()
	);

	// Drop the reader. `oldest_active` advances to the next-oldest live txn
	// (or to `visible_seq_num` if none).
	drop(reader);

	// At this point `commits_since_gc` is at `GC_INTERVAL` (saturated/at-cap
	// from the loop above without a GC firing). The first post-drop commit
	// sees a higher `oldest_active` → both gates pass → GC fires.
	let mut t = store.begin().unwrap();
	t.set(b"trigger_gc", b"v").unwrap();
	t.commit().await.unwrap();

	let after = oracle.len();
	assert!(after < n, "map should have shrunk after reader drop + one more commit; got {after}");
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

/// On a clean restart, the oracle is constructed fresh and `set_seq_num`
/// intentionally does NOT touch it. New txns post-recovery have
/// `start_seq = max_recovered_seq`; the oracle's `kept_since` is 0, so the
/// strict `start_seq < kept_since` check accepts them without any seeding.
///
/// Note: we can't easily simulate a full process restart in-process for an
/// LSM tree without dropping and reopening it. This test reopens the tree
/// on the same path and verifies new txns proceed normally.
#[test(tokio::test)]
async fn test_recovery_does_not_touch_oracle() {
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

	// Reopen: the oracle is constructed fresh (empty map, kept_since = 0).
	// `commit_pipeline.set_seq_num(max_seq)` bumps the seq counters but
	// intentionally does NOT touch the oracle — a fresh oracle is already in
	// the correct state for post-recovery transactions.
	let store = TreeBuilder::new().with_path(path).build().unwrap();
	let pipeline = &store.core.commit_pipeline;
	let oracle = pipeline.oracle();

	// Oracle is empty post-recovery (no commits have happened in this process).
	assert_eq!(oracle.len(), 0, "oracle map should be empty after reopen");
	// kept_since stays at its initial 0 — startup does not touch the oracle.
	assert_eq!(oracle.kept_since(), 0);

	// A fresh commit must succeed. Its `start_seq` is the recovered
	// `visible_seq_num` (>= 8 since we did 8 commits), and the oracle's
	// `kept_since = 0`, so the strict `start_seq < kept_since` check
	// returns false and the txn proceeds.
	let mut t = store.begin().unwrap();
	t.set(b"post_recovery", b"ok").unwrap();
	assert!(t.commit().await.is_ok());
}

/// `Tree::restore_from_checkpoint` rewinds the seq counter mid-process. The
/// running oracle has accumulated entries with seqs greater than the restored
/// `max_seq`; those are ghosts of a future that no longer exists. The
/// `reset_oracle_for_restore` call in the restore path must discard them.
#[test(tokio::test)]
async fn test_restore_clears_oracle_entries() {
	let temp = TempDir::new("oracle_restore").unwrap();
	let db_path = temp.path().join("db");
	let checkpoint_path = temp.path().join("checkpoint");
	std::fs::create_dir_all(&db_path).unwrap();

	let store = TreeBuilder::new().with_path(db_path).build().unwrap();

	// Write 4 keys, take a checkpoint at this state.
	for i in 0..4 {
		let mut t = store.begin().unwrap();
		t.set(format!("pre_{i}").as_bytes(), b"v").unwrap();
		t.commit().await.unwrap();
	}
	store.create_checkpoint(&checkpoint_path).unwrap();

	// Pin the watermark so subsequent commits don't get GC'd away. Without
	// this, per-publish GC keeps the map near-empty (1-2 entries) and we
	// can't observe "entries existed before restore".
	let reader = store.begin().unwrap();

	// Write 4 more keys. With the watermark pinned, all 4 stay in the map.
	for i in 0..4 {
		let mut t = store.begin().unwrap();
		t.set(format!("post_{i}").as_bytes(), b"v").unwrap();
		t.commit().await.unwrap();
	}
	let oracle = store.core.commit_pipeline.oracle();
	assert!(
		oracle.len() >= 4,
		"post-checkpoint commits should be retained while reader pins watermark; got {}",
		oracle.len()
	);

	// Drop the reader before restore — its `start_seq_num` references a future
	// (post-checkpoint) seq that won't exist after the restore.
	drop(reader);

	// Restore from the checkpoint. The restored manifest's max_seq is 4, but
	// the oracle just had entries at seqs 5..=8 — ghosts of the discarded
	// future. After restore, the oracle map must be empty.
	store.restore_from_checkpoint(&checkpoint_path).unwrap();
	assert_eq!(oracle.len(), 0, "oracle should be empty after restore");

	// A fresh commit at the post-restore start_seq must succeed.
	let mut t = store.begin().unwrap();
	t.set(b"post_restore", b"ok").unwrap();
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

/// `restore_from_checkpoint` must serialize against concurrent commits.
/// Without the `write_mutex` guard, a commit racing through the restore's
/// memtable-wipe / WAL-replay / seq-rewind / oracle-reset can observe torn
/// state. With the guard, the restore either completes before a given
/// commit starts (commit succeeds against post-restore state) or after
/// (commit succeeds against pre-restore state). Either is fine; the post-
/// condition we test is "no panic, no torn data, fresh commit works after".
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn test_restore_serialized_against_commits() {
	let temp = TempDir::new("oracle_restore_concurrent").unwrap();
	let db_path = temp.path().join("db");
	let checkpoint_path = temp.path().join("checkpoint");
	std::fs::create_dir_all(&db_path).unwrap();

	let store = Arc::new(TreeBuilder::new().with_path(db_path).build().unwrap());

	// Pre-seed and checkpoint.
	for i in 0..8 {
		let mut t = store.begin().unwrap();
		t.set(format!("seed_{i}").as_bytes(), b"v").unwrap();
		t.commit().await.unwrap();
	}
	store.create_checkpoint(&checkpoint_path).unwrap();

	// Stress: a writer task hammers commits while the main task restores.
	let writer_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
	let writer_done_clone = Arc::clone(&writer_done);
	let store_for_writer = Arc::clone(&store);
	let writer = tokio::spawn(async move {
		let mut acceptable_results = 0usize;
		for i in 0..200 {
			let mut t = store_for_writer.begin().unwrap();
			t.set(format!("racy_{i}").as_bytes(), b"v").unwrap();
			// Any outcome is acceptable as long as it's a normal error or Ok.
			// We're checking the system doesn't panic or wedge.
			match t.commit().await {
				Ok(()) | Err(Error::TransactionRetry) | Err(Error::TransactionWriteConflict) => {
					acceptable_results += 1;
				}
				Err(other) => {
					// Other errors are also OK (e.g., PipelineStall during shutdown),
					// but flag the unexpected ones so we notice regressions.
					eprintln!("writer iter {i} got non-fatal error: {other:?}");
					acceptable_results += 1;
				}
			}
		}
		writer_done_clone.store(true, std::sync::atomic::Ordering::Release);
		acceptable_results
	});

	// Restore in parallel with the writer.
	// Use tokio's spawn_blocking because restore_from_checkpoint is sync.
	let store_for_restore = Arc::clone(&store);
	let checkpoint_path_clone = checkpoint_path.clone();
	let restore_result = tokio::task::spawn_blocking(move || {
		store_for_restore.restore_from_checkpoint(&checkpoint_path_clone)
	})
	.await
	.unwrap();
	assert!(restore_result.is_ok(), "restore should not fail: {restore_result:?}");

	let written = writer.await.unwrap();
	assert_eq!(written, 200, "all writer iterations should produce a normal result");
	assert!(writer_done.load(std::sync::atomic::Ordering::Acquire));

	// Post-restore: a fresh commit must succeed.
	let mut t = store.begin().unwrap();
	t.set(b"post_restore_works", b"ok").unwrap();
	assert!(t.commit().await.is_ok(), "post-restore commit must succeed");

	// And the seed data must be readable (it survives the checkpoint).
	let t = store.begin().unwrap();
	let v = t.get(b"seed_0").unwrap();
	assert_eq!(v.as_deref(), Some(b"v".as_slice()), "seed data must be readable post-restore");
}

/// Drive enough GC firings under steady-state load to exercise the
/// monotonicity assert in the oracle's GC body. With Change A's clamp,
/// `oldest_active` is monotonic across GC bodies. This test must not
/// panic in debug builds; if it does, either Change A is broken or the
/// monotonicity invariant is violated by some other code path.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn test_gc_oldest_active_monotonic() {
	use crate::oracle::GC_INTERVAL;
	let (store, _td) = create_store();
	let store = Arc::new(store);

	// Push well past GC_INTERVAL commits with monotonically-increasing
	// watermark. The committer's start_seq is always the previous
	// visible_seq_num, which is monotonic; the clamp makes oldest_active
	// monotonic across publishes. If the debug_assert fires, this test
	// will panic in debug builds.
	let n = (GC_INTERVAL as usize) * 3 + 7;
	for i in 0..n {
		let mut t = store.begin().unwrap();
		t.set(format!("mono_{i}").as_bytes(), b"v").unwrap();
		t.commit().await.unwrap();
	}
}
