//! FK2 gate tests: commit-timestamp stamping, timeline resolution, and
//! crash/flush coverage of the `ts -> seq` index.

use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::clock::MockLogicalClock;
use crate::error::Error;
use crate::lsm::{Tree, TreeBuilder};
use crate::Options;

fn store_with_clock(path: std::path::PathBuf, clock: Arc<MockLogicalClock>) -> Tree {
	let opts = Options {
		path,
		clock,
		flush_on_close: false,
		..Options::default()
	};
	TreeBuilder::with_options(opts).build().unwrap()
}

/// Commit timestamps are stamped under the write mutex with a strictly
/// monotone clamp: a backdated clock cannot regress the timeline, and every
/// stamped instant resolves to exactly its commit's sequence.
#[test(tokio::test)]
async fn fk2_stamps_survive_a_backdated_clock_and_resolve_exactly() {
	let temp_dir = TempDir::new("test").unwrap();
	let clock = Arc::new(MockLogicalClock::with_timestamp(1_000));
	let store = store_with_clock(temp_dir.path().to_path_buf(), Arc::clone(&clock));

	let mut txn = store.begin().unwrap();
	txn.set(b"k1", b"v1").unwrap();
	txn.commit().await.unwrap();

	// The wall clock steps BACKWARDS; stamps must keep strictly increasing.
	clock.set_time(10);
	let mut txn = store.begin().unwrap();
	txn.set(b"k2", b"v2").unwrap();
	txn.commit().await.unwrap();
	let mut txn = store.begin().unwrap();
	txn.set(b"k3", b"v3").unwrap();
	txn.commit().await.unwrap();

	let timeline = &store.core.inner.timeline;
	assert_eq!(timeline.last_commit_ts(), 1_002, "clamp: 1000, then 1001, then 1002");
	assert_eq!(timeline.resolve(1_000).unwrap(), 1);
	assert_eq!(timeline.resolve(1_001).unwrap(), 2);
	assert_eq!(timeline.resolve(1_002).unwrap(), 3);
	assert_eq!(timeline.resolve(50_000).unwrap(), 3, "beyond the head: newest commit");
	assert!(matches!(timeline.resolve(999), Err(Error::TimestampBelowHorizon { .. })));

	store.close().await.unwrap();
}

/// The timeline survives a crash: WAL replay rebuilds the fenceposts, the
/// clamp resumes above the recovered maximum even with a stale clock, and
/// resolutions are identical to pre-crash.
#[test(tokio::test)]
async fn fk2_timeline_survives_crash_reopen_via_replay() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	{
		let clock = Arc::new(MockLogicalClock::with_timestamp(1_000));
		let store = store_with_clock(path.clone(), clock);
		for (key, value) in [(b"k1", b"v1"), (b"k2", b"v2")] {
			let mut txn = store.begin().unwrap();
			txn.set(key, value).unwrap();
			txn.commit().await.unwrap();
		}
		store.close().await.unwrap(); // flush_on_close = false: WAL only
	}

	// Reopen with a clock far in the past: recovery must still resume the
	// timeline above the replayed stamps.
	let clock = Arc::new(MockLogicalClock::with_timestamp(5));
	let store = store_with_clock(path, Arc::clone(&clock));
	let timeline = &store.core.inner.timeline;
	assert_eq!(timeline.resolve(1_000).unwrap(), 1, "replayed fencepost resolves exactly");
	assert_eq!(timeline.resolve(1_001).unwrap(), 2);
	assert_eq!(timeline.last_commit_ts(), 1_001);

	let mut txn = store.begin().unwrap();
	txn.set(b"k3", b"v3").unwrap();
	txn.commit().await.unwrap();
	assert_eq!(
		timeline.resolve(1_002).unwrap(),
		3,
		"the post-crash stamp clamps strictly above the recovered maximum"
	);

	store.close().await.unwrap();
}

/// After a flush advances the replay floor past the early segments, the
/// pre-floor fenceposts are served from the durable root tail — the covered
/// horizon spans flushed history, not just the replayable WAL.
#[test(tokio::test)]
async fn fk2_root_tail_serves_resolutions_past_the_replay_floor() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	{
		let clock = Arc::new(MockLogicalClock::with_timestamp(1_000));
		let store = store_with_clock(path.clone(), clock);
		for (key, value) in [(b"k1", b"v1"), (b"k2", b"v2")] {
			let mut txn = store.begin().unwrap();
			txn.set(key, value).unwrap();
			txn.commit().await.unwrap();
		}
		// Close the segment and flush: the floor advances past it, and the
		// root version records the timeline tail.
		crate::test::support::rotate_wal(&store);
		store.core.inner.rotate_memtable().unwrap();
		while store.core.inner.flush_oldest_immutable_for_test().unwrap().is_some() {}
		let floor = crate::test::support::wal_log_number(&store);
		assert!(floor >= 1, "fixture: the flush must advance the replay floor");
		store.close().await.unwrap();
	}

	let clock = Arc::new(MockLogicalClock::with_timestamp(5));
	let store = store_with_clock(path, clock);
	let timeline = &store.core.inner.timeline;
	// Replay covered nothing before the floor; these come from the root tail.
	assert_eq!(timeline.resolve(1_000).unwrap(), 1);
	assert_eq!(timeline.resolve(1_001).unwrap(), 2);
	assert_eq!(timeline.last_commit_ts(), 1_001);

	store.close().await.unwrap();
}

/// Sabotage twin: a WAL whose commit timestamps regress must fail the open
/// closed, while the byte-identical fixture with increasing timestamps opens
/// fine (non-vacuity).
#[test(tokio::test)]
async fn fk2_replay_rejects_commit_timestamp_regression() {
	fn craft_wal(path: &std::path::Path, stamps: [(u64, u64); 2]) {
		let wal_dir = path.join("wal");
		std::fs::create_dir_all(&wal_dir).unwrap();
		let mut wal =
			crate::wal::manager::Wal::open(&wal_dir, crate::wal::Options::default()).unwrap();
		for (index, (seq, commit_ts)) in stamps.into_iter().enumerate() {
			let mut batch = crate::batch::Batch::new(seq);
			batch.set_commit_ts(commit_ts);
			batch.set(format!("k{index}").into_bytes(), b"v".to_vec(), 0).unwrap();
			wal.append(&batch.encode().unwrap()).unwrap();
		}
		wal.close().unwrap();
	}

	// Non-vacuity arm: increasing timestamps open fine.
	let temp_dir = TempDir::new("test").unwrap();
	craft_wal(temp_dir.path(), [(1, 100), (2, 150)]);
	let store = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_flush_on_close(false)
		.build()
		.unwrap();
	assert_eq!(store.core.inner.timeline.resolve(100).unwrap(), 1);
	store.close().await.unwrap();

	// Sabotage arm: a timestamp regression is corruption.
	let temp_dir = TempDir::new("test").unwrap();
	craft_wal(temp_dir.path(), [(1, 100), (2, 50)]);
	let result = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_flush_on_close(false)
		.build();
	let error = result.err().expect("timestamp regression must fail the open").to_string();
	assert!(error.contains("commit timestamp regressed"), "{error}");
}
