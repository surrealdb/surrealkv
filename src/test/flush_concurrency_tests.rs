//! G1b: the coverage the `Tree::flush` gap was really about.
//!
//! `KNOWN_GAPS.md` §1 used to say the suite's durability path was "a synchronous
//! path production never runs", implying the tests exercised different code.
//! Measured, that was overstated: the background worker's `compact_memtable` is
//! literally `flush_oldest_immutable_to_sst`, which is what the synchronous
//! drain calls in a loop. **The work is identical.**
//!
//! What the synchronous drain removes is *everything else happening at the same
//! time*. The caller is blocked inside the flush, so no read, no rotation, no
//! branch operation and no compaction can be in flight while it runs. That is
//! the blind spot, and converting 207 call sites would not have covered it —
//! only tests that deliberately put something *alongside* a flush do.
//!
//! So these five run the flush through the real worker
//! ([`Tree::flush_and_wait`]) with something else racing it, on a multi-threaded
//! runtime, and assert **invariants rather than schedules**: the outcome must be
//! one of a named set and the store consistent whichever it was. A flake here is
//! a finding, not a test to loosen.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::compaction::leveled::Strategy;
use crate::lsm::{CompactionOperations as _, Tree};
use crate::test::support::wait_until;
use crate::{Error, ForkPoint, TreeBuilder};

/// Small memtables and a shallow level count, so a handful of writes is enough
/// to make a flush real and a compaction reachable.
fn flush_test_store() -> (Arc<Tree>, TempDir) {
	let temp_dir = TempDir::new("flush-concurrency").unwrap();
	let tree = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_level_count(2)
		.with_max_memtable_size(16 * 1024)
		.build()
		.unwrap();
	(Arc::new(tree), temp_dir)
}

async fn write(store: &Tree, key: &[u8], value: &[u8]) {
	let mut txn = store.begin().unwrap();
	txn.set(key, value).unwrap();
	txn.commit().await.unwrap();
}

async fn write_branch(store: &Tree, branch: &str, key: &[u8], value: &[u8]) {
	let handle = store.branch(branch).unwrap();
	let mut txn = handle.begin().unwrap();
	txn.set(key, value).unwrap();
	txn.commit().await.unwrap();
}

/// Deleting a branch while that branch's own flush is in flight.
///
/// This is the V4 neighbourhood: `close()` used to fail after
/// *write-branch → delete-branch → close*, because the shared flush loop stalled
/// on an entry belonging to a fenced owner. That was found by a test that did
/// not go through the synchronous drain, which is the whole argument for this
/// file.
///
/// Either outcome is legal — the delete lands before the flush and the flush
/// discards a fenced memtable, or after and the table is reclaimed. What is not
/// legal is a store that will not close, or one whose other branches lost data.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn concurrent_branch_delete_and_its_own_flush_leave_a_closable_store() {
	let (store, _temp) = flush_test_store();
	write(&store, b"main-key", b"main-value").await;
	store.fork_branch("main", "doomed", ForkPoint::Head).unwrap();
	store.fork_branch("main", "survivor", ForkPoint::Head).unwrap();

	// Enough on the doomed branch that its flush is real work.
	for i in 0..64 {
		write_branch(&store, "doomed", format!("d{i:04}").as_bytes(), &[b'x'; 256]).await;
	}
	write_branch(&store, "survivor", b"s", b"survives").await;
	// The survivor is durable before the race, so a later read of it is a
	// statement about tables rather than about a memtable that happened to live.
	crate::test::support::flush_branch_and_wait(&store, "survivor").await;

	// Put the DOOMED branch's own memtable in front of the worker and do not
	// wait — the delete has to race a flush that is actually in flight. Using
	// `Tree::flush_and_wait` here would rotate only the default runtime, so this
	// test would have raced `main`'s flush while claiming to race the branch's.
	crate::test::support::rotate_branch_and_signal(&store, "doomed");

	let deleter = {
		let store = Arc::clone(&store);
		tokio::task::spawn_blocking(move || store.delete_branch("doomed"))
	};

	let deleted = deleter.await.expect("the delete task must not panic");
	assert!(deleted.is_ok(), "deleting a branch mid-flush must not error: {deleted:?}");

	// Let the worker finish whatever it was doing with the fenced memtable.
	assert!(
		wait_until(|| !store.core.inner.has_pending_immutables()).await,
		"the flush worker never drained after its branch was deleted underneath it"
	);

	// The invariant: the untouched branch is intact, and the store closes.
	let txn = store.begin_on("survivor").unwrap();
	assert_eq!(txn.get(b"s").unwrap(), Some(b"survives".to_vec()));
	assert_eq!(txn.get(b"main-key").unwrap(), Some(b"main-value".to_vec()));
	drop(txn);

	Arc::try_unwrap(store)
		.map_err(|_| "a task still holds the store")
		.unwrap()
		.close()
		.await
		.expect("the store must close after a branch was deleted mid-flush");
}

/// A compaction running against a flush that has not published yet.
///
/// The flush worker notifies `level_notify` only *after* its pass completes, but
/// nothing stops a compaction started by other means from sampling the manifest
/// mid-flush. It must not install an output that loses the flushed table, and it
/// must not observe a half-published state.
///
/// `CompactionPinRaced` is an accepted outcome — that variant exists to describe
/// exactly this — but a lost key is not.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_compaction_racing_an_unpublished_flush_never_loses_a_key() {
	let (store, _temp) = flush_test_store();

	let mut expected = Vec::new();
	for i in 0..128 {
		let key = format!("k{i:04}");
		write(&store, key.as_bytes(), &[b'v'; 128]).await;
		expected.push(key);
	}

	let flusher = {
		let store = Arc::clone(&store);
		tokio::spawn(async move { store.flush_and_wait().await })
	};
	let compactor = {
		let store = Arc::clone(&store);
		tokio::task::spawn_blocking(move || store.compact(Arc::new(Strategy::default())))
	};

	let _ = flusher.await.expect("the flush task must not panic");
	match compactor.await.expect("the compaction task must not panic") {
		Ok(()) => {}
		// The named outcome for exactly this race.
		Err(Error::CompactionPinRaced {
			..
		}) => {}
		Err(other) => panic!("a compaction racing a flush failed unexpectedly: {other}"),
	}

	let txn = store.begin().unwrap();
	for key in &expected {
		assert!(
			txn.get(key.as_bytes()).unwrap().is_some(),
			"key {key} was lost when a compaction raced an unpublished flush"
		);
	}
}

/// WAL reclamation must not advance past a memtable still in flight.
///
/// The replay floor is derived from every live memtable's WAL dependency. If a
/// flush publishes and the floor advances while another memtable still depends
/// on that segment, a crash would replay short of the data. Here the store keeps
/// writing while flushes run, and then must reopen with everything intact.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn wal_reclamation_during_concurrent_flushes_never_loses_a_commit() {
	let temp_dir = TempDir::new("flush-wal-reclaim").unwrap();
	let path = temp_dir.path().to_path_buf();
	let build = || {
		TreeBuilder::new()
			.with_path(path.clone())
			.with_level_count(2)
			.with_max_memtable_size(16 * 1024)
			.build()
			.unwrap()
	};

	let store = Arc::new(build());
	let stop = Arc::new(AtomicBool::new(false));
	// Published so the flusher can wait for real work to exist before starting.
	// Without this the flushes finish against an empty store before the writer is
	// ever scheduled, and the test races its own setup.
	let committed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

	// A writer that keeps producing new WAL dependencies while flushes run.
	let writer = {
		let store = Arc::clone(&store);
		let stop = Arc::clone(&stop);
		let committed = Arc::clone(&committed);
		tokio::spawn(async move {
			let mut written = Vec::new();
			let mut i = 0usize;
			while !stop.load(Ordering::Relaxed) {
				let key = format!("w{i:05}");
				let mut txn = store.begin().unwrap();
				txn.set(key.as_bytes(), &[b'p'; 128]).unwrap();
				if txn.commit().await.is_ok() {
					written.push(key);
					committed.fetch_add(1, Ordering::Relaxed);
				}
				i += 1;
				tokio::task::yield_now().await;
			}
			written
		})
	};

	// Interleave: let the writer get ahead, flush, repeat — so each flush has a
	// live memtable depending on a WAL segment behind it.
	for round in 1..=4 {
		assert!(
			wait_until(|| committed.load(Ordering::Relaxed) >= round * 8).await,
			"the writer stalled; nothing to race the flush against"
		);
		let _ = store.flush_and_wait().await;
	}
	stop.store(true, Ordering::Relaxed);
	let written = writer.await.expect("the writer task must not panic");
	assert!(written.len() > 8, "the writer produced too little to be a test: {}", written.len());

	Arc::try_unwrap(store)
		.map_err(|_| "a task still holds the store")
		.unwrap()
		.close()
		.await
		.unwrap();

	// Every committed key must survive the reopen.
	let reopened = build();
	let txn = reopened.begin().unwrap();
	for key in &written {
		assert!(
			txn.get(key.as_bytes()).unwrap().is_some(),
			"committed key {key} did not survive a reopen after concurrent flushes"
		);
	}
	drop(txn);
	reopened.close().await.unwrap();
}

/// Back-pressure engages when flushes queue, and clears when they drain.
///
/// `WriteStall` was asserted nowhere in the suite before this (a V7 finding),
/// because the synchronous drain never let a backlog build. Writing hard against
/// a small memtable and a low stall threshold is what makes it reachable.
///
/// The invariant is not "a stall happened" — that is schedule-dependent — but
/// that the writes all land and the controller is not left stalled afterwards.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn writes_under_queued_flushes_all_land_and_leave_no_stall_behind() {
	let temp_dir = TempDir::new("flush-backpressure").unwrap();
	let store = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_level_count(2)
		.with_max_memtable_size(8 * 1024)
		// 2 is the documented minimum; the engine refuses 1.
		.with_memtable_stall_threshold(2)
		.build()
		.unwrap();

	let mut written = Vec::new();
	for i in 0..256 {
		let key = format!("b{i:05}");
		let mut txn = store.begin().unwrap();
		txn.set(key.as_bytes(), &[b'z'; 512]).unwrap();
		txn.commit().await.expect("a stalled write must block, not fail");
		written.push(key);
	}

	// Whatever back-pressure engaged must clear once the backlog drains.
	assert!(
		wait_until(|| !store.core.write_stall.is_stalled()).await,
		"the store is still stalled after the writes finished"
	);

	let txn = store.begin().unwrap();
	for key in &written {
		assert!(txn.get(key.as_bytes()).unwrap().is_some(), "key {key} was lost under stall");
	}
	drop(txn);
	store.close().await.unwrap();
}

/// A read racing a memtable rotation sees a consistent view.
///
/// The cheapest of these five and the most frequently executed path in the
/// engine: a rotation swaps the active memtable for a fresh one and pushes the
/// old onto the immutable list. A reader that catches the swap mid-flight must
/// still see every committed key — the rotation is not a visibility event.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn reads_racing_a_rotation_never_miss_a_committed_key() {
	let (store, _temp) = flush_test_store();

	let mut expected = Vec::new();
	for i in 0..64 {
		let key = format!("r{i:04}");
		write(&store, key.as_bytes(), b"v").await;
		expected.push(key);
	}

	let stop = Arc::new(AtomicBool::new(false));
	let reader = {
		let store = Arc::clone(&store);
		let stop = Arc::clone(&stop);
		let expected = expected.clone();
		tokio::task::spawn_blocking(move || {
			let mut reads = 0usize;
			while !stop.load(Ordering::Relaxed) {
				let txn = store.begin().unwrap();
				for key in &expected {
					assert!(
						txn.get(key.as_bytes()).unwrap().is_some(),
						"key {key} vanished during a rotation"
					);
				}
				reads += 1;
			}
			reads
		})
	};

	for _ in 0..8 {
		let _ = store.flush_and_wait().await;
		write(&store, b"extra", b"v").await;
	}
	stop.store(true, Ordering::Relaxed);

	let reads = reader.await.expect("the reader must not panic — a vanished key panics here");
	assert!(reads > 0, "the reader never ran, so it asserted nothing");
}

/// The barrier means what it says: when it returns `true`, the data is in a
/// table.
///
/// The five tests above deliberately do *not* depend on this — they assert
/// invariants that must hold under any interleaving, so they keep passing even
/// if the barrier returns early (probed, 2026-08-15). That makes them tests of
/// concurrency, not of the barrier. This one is the test of the barrier, and it
/// is the one that reddens when the wait is removed.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn the_barrier_returns_only_once_the_flush_has_produced_a_table() {
	let (store, _temp) = flush_test_store();

	let before = store.metrics().unwrap().memtable_flushes;
	for i in 0..32 {
		write(&store, format!("t{i:04}").as_bytes(), &[b'q'; 256]).await;
	}

	assert!(store.flush_and_wait().await.unwrap(), "the barrier timed out");

	let after = store.metrics().unwrap().memtable_flushes;
	assert!(
		after > before,
		"the barrier returned but no memtable was flushed ({before} -> {after})"
	);
	assert!(
		!store.core.inner.has_pending_immutables(),
		"the barrier returned with a memtable still pending"
	);
	assert!(
		crate::test::support::owner_table_count(&store, "main") > 0,
		"the barrier returned but `main` has no table"
	);
}

/// Nothing to flush is not a failure, and must not hang.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn the_barrier_returns_immediately_on_an_empty_store() {
	let (store, _temp) = flush_test_store();
	assert!(store.flush_and_wait().await.unwrap(), "an empty store is already flushed");
	assert_eq!(store.metrics().unwrap().memtable_flushes, 0, "nothing should have been flushed");
}

/// A pathologically short fork-drain timeout does not break forking.
///
/// `ForkFenceTimeout` was the last branch-reachable error with no test, and
/// chasing it produced a better answer than a test: **it is unreachable by
/// construction in the current commit path.**
///
/// Its doc comment says it guards against "a commit whose future was dropped
/// between enqueue and apply". In `CommitPipeline::commit` the awaits are at the
/// stall check, the semaphore acquire, and the completion receiver — the last of
/// which is *after* `publish()`. Between `pending.enqueue` and `publish()` there
/// is **no suspension point at all**: enqueue, apply and publish happen in one
/// synchronous run under a `parking_lot` mutex. A future cannot be dropped
/// there, so a batch cannot be stranded, so the fence always finds a drained
/// pipeline. An earlier version of this test asserted a timeout must occur and
/// caught zero in 200 attempts against 217 concurrent commits.
///
/// The guard stays: the async port introduces awaits into exactly this path, at
/// which point the state it defends against becomes producible. See
/// `docs/KNOWN_GAPS.md` §2.
///
/// What this test does assert is the invariant that holds regardless: with the
/// timeout at zero — the most hostile setting available — every fork either
/// succeeds or fails with exactly `ForkFenceTimeout`, never anything else, and a
/// fork that reported failure leaves no branch behind.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn forking_under_load_with_a_zero_drain_timeout_stays_consistent() {
	let temp_dir = TempDir::new("fork-fence-timeout").unwrap();
	let store = Arc::new(
		TreeBuilder::new()
			.with_path(temp_dir.path().to_path_buf())
			// Zero: the fence refuses to wait at all. If an in-flight commit
			// could ever be observed here, this is the setting that would see it.
			.with_fork_drain_timeout(std::time::Duration::ZERO)
			.build()
			.unwrap(),
	);
	write(&store, b"seed", b"v").await;

	let stop = Arc::new(AtomicBool::new(false));
	let writer = {
		let store = Arc::clone(&store);
		let stop = Arc::clone(&stop);
		tokio::spawn(async move {
			let mut i = 0usize;
			while !stop.load(Ordering::Relaxed) {
				let mut txn = store.begin().unwrap();
				txn.set(format!("f{i:05}").as_bytes(), b"v").unwrap();
				let _ = txn.commit().await;
				i += 1;
			}
			i
		})
	};

	let mut succeeded = 0usize;
	for attempt in 0..200 {
		match store.fork_branch("main", &format!("fence/{attempt}"), ForkPoint::Head) {
			Ok(_) => succeeded += 1,
			// Legal, and currently never seen — see the doc comment.
			Err(Error::ForkFenceTimeout) => {}
			Err(other) => panic!("a fork under load must succeed or time out, got: {other}"),
		}
	}

	stop.store(true, Ordering::Relaxed);
	let commits = writer.await.expect("the writer must not panic");
	assert!(commits > 0, "the writer never committed, so nothing contended the fence");

	// Whatever each fork decided, the catalog agrees with it.
	assert_eq!(
		store.list_branches().unwrap().len(),
		succeeded + 1,
		"a fork that reported failure must have left no branch behind"
	);
	let txn = store.begin().unwrap();
	assert_eq!(txn.get(b"seed").unwrap(), Some(b"v".to_vec()));
}
