//! Versioned queries: every write of a versioned tree is stamped with its timestamp, kept with
//! its value, and read back by `Transaction::get_at` and `Transaction::history`, from the
//! memtable, from tables, after a restart and after compactions.
//!
//! The versions of a key are listed newest timestamp first, and the newest write first among
//! those of one timestamp. Timestamps are free to go backwards, as `set_at` allows:
//! `get_at(key, t)` is the newest write whose timestamp is at or before `t`.

use std::path::Path;
use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use super::tfs;
use crate::compaction::leveled::Strategy;
use crate::vlog::ValueLocation;
use crate::{HistoryOptions, LSMIterator, Options, Transaction, Tree, TreeBuilder};

/// One version as `history` reports it: `(key, timestamp, is tombstone, value)`.
type Version = (Vec<u8>, u64, bool, Option<Vec<u8>>);

fn options(path: &Path, retention_ns: u64) -> Options {
	Options {
		path: path.to_path_buf(),
		enable_versioning: true,
		versioned_history_retention_ns: retention_ns,
		// Nothing is flushed or compacted unless a test does it, and a restart replays the log.
		level0_max_files: 100,
		l0_stall_threshold: 1000,
		memtable_stall_threshold: 1000,
		flush_on_close: false,
		..Default::default()
	}
}

fn open(path: &Path, retention_ns: u64) -> Arc<Tree> {
	Arc::new(Tree::new(Arc::new(options(path, retention_ns))).unwrap())
}

async fn quiet(path: &Path, retention_ns: u64) -> Arc<Tree> {
	let tree = open(path, retention_ns);
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
	tree
}

/// Ends a tree the way a crash does: nothing is flushed, and the lock is released.
fn crash(tree: &Tree) {
	tree.core.is_closed.store(true, std::sync::atomic::Ordering::SeqCst);
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
}

fn flush(tree: &Tree) {
	tree.core.inner.rotate_memtable().unwrap();
	tree.core.inner.flush_all_immutables_sync().unwrap();
}

fn compact(tree: &Tree) {
	compact_with(tree, 2);
}

/// Pushes what is in the tables down, until the tables stop changing or 24 compactions have been
/// done: with a size of 1 byte for a level, every level is over its target, and each compaction
/// takes the shallowest of them. The target of a level is ten times that of the one before, so
/// a table of a few entries (under 1000 bytes) stops at the fifth level, L4, and one over it goes
/// on to the bottom. The tests that assert where tables are check that they are that small, with
/// `assert_deep`.
fn push_down(tree: &Tree) {
	let tables = |tree: &Tree| -> Vec<u64> {
		let manifest = tree.core.inner.level_manifest.read().unwrap();
		let mut ids: Vec<u64> = manifest.iter().map(|table| table.meta.properties.id).collect();
		ids.sort_unstable();
		ids
	};
	for _ in 0..24 {
		let before = tables(tree);
		// A tree of two levels has nowhere to push to: its first compaction is the bottom one
		let opts = Arc::new(Options {
			level0_max_files: 1,
			max_bytes_for_level: if tree.core.opts.level_count > 2 {
				1
			} else {
				Options::default().max_bytes_for_level
			},
			enable_versioning: true,
			..Default::default()
		});
		tree.compact(Arc::new(Strategy::from_options(opts))).unwrap();
		if tables(tree) == before {
			break;
		}
	}
}

/// Checks that the deepest table is in the fifth level, L4, and that the tables are small enough
/// to have stopped there: below the target of that level when every level has a size of 1 byte.
fn assert_deep(tree: &Tree, what: &str) {
	assert_eq!(levels_with_tables(tree).last().copied(), Some(4), "{what}: the deepest level");
	let manifest = tree.core.inner.level_manifest.read().unwrap();
	for table in manifest.iter() {
		assert!(
			table.file_size < 1000,
			"{what}: a table of {} bytes would go on below the fifth level",
			table.file_size
		);
	}
}

/// The ids of the tables that hold a range delete.
fn tables_with_range_deletes(tree: &Tree) -> Vec<u64> {
	let manifest = tree.core.inner.level_manifest.read().unwrap();
	let mut ids: Vec<u64> = manifest
		.iter()
		.filter(|table| !table.range_deletions.read().is_empty())
		.map(|table| table.meta.properties.id)
		.collect();
	ids.sort_unstable();
	ids
}

/// The levels that hold tables, deepest last.
fn levels_with_tables(tree: &Tree) -> Vec<usize> {
	let manifest = tree.core.inner.level_manifest.read().unwrap();
	manifest
		.levels
		.0
		.iter()
		.enumerate()
		.filter(|(_, level)| !level.tables.is_empty())
		.map(|(n, _)| n)
		.collect()
}

/// A compaction of the first level, once it holds that many tables.
fn compact_with(tree: &Tree, level0_max_files: usize) {
	let opts = Arc::new(Options {
		level0_max_files,
		enable_versioning: true,
		..Default::default()
	});
	tree.compact(Arc::new(Strategy::from_options(opts))).unwrap();
}

async fn set_at(tree: &Tree, writes: &[(&[u8], &[u8], u64)]) {
	let mut txn = tree.begin().unwrap();
	for (key, value, ts) in writes {
		txn.set_at(*key, *value, *ts).unwrap();
	}
	txn.commit().await.unwrap();
}

fn history_with(txn: &Transaction, opts: &HistoryOptions) -> Vec<Version> {
	let mut iter = txn.history_with_options(b"\x00".as_slice(), b"\xff".as_slice(), opts).unwrap();
	let mut out = Vec::new();
	let mut valid = iter.seek_first().unwrap();
	while valid {
		let key = iter.key();
		let tombstone = key.is_tombstone();
		out.push((
			key.user_key().to_vec(),
			key.timestamp(),
			tombstone,
			(!tombstone).then(|| iter.value().unwrap()),
		));
		valid = iter.next().unwrap();
	}
	out
}

/// Every version of every key, with tombstones, newest first within a key.
fn history(tree: &Tree) -> Vec<Version> {
	let txn = tree.begin().unwrap();
	history_with(&txn, &HistoryOptions::new().with_tombstones(true))
}

fn versions_of(tree: &Tree, key: &[u8]) -> Vec<(u64, Option<Vec<u8>>)> {
	history(tree)
		.into_iter()
		.filter(|(k, ..)| k == key)
		.map(|(_, ts, _, value)| (ts, value))
		.collect()
}

fn get_at(tree: &Tree, key: &[u8], ts: u64) -> Option<Vec<u8>> {
	tree.begin().unwrap().get_at(key, ts).unwrap()
}

fn v(bytes: &[u8]) -> Option<Vec<u8>> {
	Some(bytes.to_vec())
}

/// The three versions `get_at` and `history` are checked on, at times well apart.
async fn three_versions(tree: &Tree) {
	set_at(tree, &[(b"k", b"one", 100)]).await;
	set_at(tree, &[(b"k", b"two", 200)]).await;
	set_at(tree, &[(b"k", b"three", 300)]).await;
}

fn assert_three_versions(tree: &Tree, what: &str) {
	assert_eq!(get_at(tree, b"k", 99), None, "{what}: before the first");
	assert_eq!(get_at(tree, b"k", 100), v(b"one"), "{what}: at the first");
	assert_eq!(get_at(tree, b"k", 199), v(b"one"), "{what}: between");
	assert_eq!(get_at(tree, b"k", 200), v(b"two"), "{what}: at the second");
	assert_eq!(get_at(tree, b"k", 299), v(b"two"), "{what}");
	assert_eq!(get_at(tree, b"k", 300), v(b"three"), "{what}: at the last");
	assert_eq!(get_at(tree, b"k", u64::MAX), v(b"three"), "{what}: after it");
	assert_eq!(
		versions_of(tree, b"k"),
		vec![(300, v(b"three")), (200, v(b"two")), (100, v(b"one"))],
		"{what}: the history, newest first"
	);
	let txn = tree.begin().unwrap();
	assert_eq!(txn.get(b"k").unwrap(), v(b"three"), "{what}: a plain read is the newest");
}

#[test(tokio::test)]
async fn versions_read_back_with_their_timestamps_from_the_memtable() {
	let dir = TempDir::new("versioning_memtable").unwrap();
	let tree = quiet(dir.path(), 0).await;
	three_versions(&tree).await;
	assert_three_versions(&tree, "memtable");
	crash(&tree);
}

#[test(tokio::test)]
async fn versions_read_back_with_their_timestamps_from_tables_and_after_compactions() {
	let dir = TempDir::new("versioning_tables").unwrap();
	let tree = quiet(dir.path(), 0).await;
	set_at(&tree, &[(b"k", b"one", 100)]).await;
	flush(&tree);
	set_at(&tree, &[(b"k", b"two", 200)]).await;
	flush(&tree);
	set_at(&tree, &[(b"k", b"three", 300)]).await;
	assert_three_versions(&tree, "two tables and a memtable");

	flush(&tree);
	assert_three_versions(&tree, "three tables");
	compact(&tree);
	assert_three_versions(&tree, "after a compaction: retention 0 keeps every version");
	compact(&tree);
	assert_three_versions(&tree, "after another");
	crash(&tree);
}

#[test(tokio::test)]
async fn timestamps_survive_a_crash_and_the_replay_of_the_log() {
	let dir = TempDir::new("versioning_wal").unwrap();
	let tree = quiet(dir.path(), 0).await;
	three_versions(&tree).await;
	crash(&tree);

	let reopened = quiet(dir.path(), 0).await;
	assert_three_versions(&reopened, "replayed from the log");
	flush(&reopened);
	assert_three_versions(&reopened, "then flushed");
	crash(&reopened);

	let again = quiet(dir.path(), 0).await;
	assert_three_versions(&again, "and reopened from the table");
	crash(&again);
}

#[test(tokio::test)]
async fn a_write_without_a_timestamp_is_stamped_with_the_time_of_its_commit() {
	let dir = TempDir::new("versioning_commit_time").unwrap();
	let tree = quiet(dir.path(), 0).await;
	let before = tree.core.opts.clock.now();
	let mut txn = tree.begin().unwrap();
	txn.set(b"a", b"1").unwrap();
	txn.set(b"b", b"2").unwrap();
	txn.commit().await.unwrap();
	let after = tree.core.opts.clock.now();

	let all = history(&tree);
	assert_eq!(all.len(), 2);
	for (key, ts, _, _) in &all {
		assert!(
			*ts > before && *ts < after,
			"{key:?} is stamped with the commit time {ts}, in ({before}, {after})"
		);
	}
	assert_eq!(all[0].1, all[1].1, "one commit, one time");
	assert_eq!(get_at(&tree, b"a", after), v(b"1"));
	assert_eq!(get_at(&tree, b"a", before), None);
	crash(&tree);
}

/// The timestamp of a write can be older than an earlier write's: a version can be written for
/// a time in the past. `history` returns the versions of a key newest timestamp first, and
/// `get_at(key, t)` is the version with the newest timestamp at or before `t`, so a version
/// written for the past does not hide a version of a later time.
#[test(tokio::test)]
async fn a_version_written_for_a_time_in_the_past_takes_its_place_by_its_timestamp() {
	let dir = TempDir::new("versioning_backwards").unwrap();
	let tree = quiet(dir.path(), 0).await;
	set_at(&tree, &[(b"k", b"first", 200)]).await;
	set_at(&tree, &[(b"k", b"backfilled", 100)]).await;
	set_at(&tree, &[(b"k", b"third", 300)]).await;
	set_at(&tree, &[(b"k", b"late", 100)]).await;

	let check = |tree: &Tree, what: &str| {
		assert_eq!(get_at(tree, b"k", 99), None, "{what}");
		// Two versions at 100: the one written last.
		assert_eq!(get_at(tree, b"k", 100), v(b"late"), "{what}");
		assert_eq!(get_at(tree, b"k", 150), v(b"late"), "{what}");
		assert_eq!(get_at(tree, b"k", 200), v(b"first"), "{what}");
		assert_eq!(
			get_at(tree, b"k", 250),
			v(b"first"),
			"{what}: not hidden by the older versions"
		);
		assert_eq!(get_at(tree, b"k", 300), v(b"third"), "{what}");
		assert_eq!(
			versions_of(tree, b"k"),
			vec![
				(300, v(b"third")),
				(200, v(b"first")),
				(100, v(b"late")),
				(100, v(b"backfilled"))
			],
			"{what}: newest timestamp first, and the newest write first among versions of one"
		);
		let txn = tree.begin().unwrap();
		assert_eq!(txn.get(b"k").unwrap(), v(b"late"), "{what}: a plain read is the last write");
	};
	check(&tree, "memtable");
	flush(&tree);
	check(&tree, "table");
	compact(&tree);
	check(&tree, "compacted");
	crash(&tree);
}

#[test(tokio::test)]
async fn a_soft_delete_is_a_version_with_its_own_time() {
	let dir = TempDir::new("versioning_soft_delete").unwrap();
	let tree = quiet(dir.path(), 0).await;
	set_at(&tree, &[(b"k", b"alive", 100)]).await;
	let mut txn = tree.begin().unwrap();
	txn.soft_delete_with_options(b"k", &crate::WriteOptions::default().with_timestamp(Some(200)))
		.unwrap();
	txn.commit().await.unwrap();
	set_at(&tree, &[(b"k", b"again", 300)]).await;

	let check = |tree: &Tree, what: &str| {
		assert_eq!(get_at(tree, b"k", 150), v(b"alive"), "{what}");
		assert_eq!(get_at(tree, b"k", 250), None, "{what}: deleted at 200");
		assert_eq!(get_at(tree, b"k", 350), v(b"again"), "{what}");
		assert_eq!(
			versions_of(tree, b"k"),
			vec![(300, v(b"again")), (200, None), (100, v(b"alive"))],
			"{what}: the delete is a version"
		);
		let txn = tree.begin().unwrap();
		let without = history_with(&txn, &HistoryOptions::new());
		assert_eq!(without.len(), 2, "{what}: and is left out when tombstones are not asked for");
	};
	check(&tree, "memtable");
	crash(&tree);
	let reopened = quiet(dir.path(), 0).await;
	check(&reopened, "replayed");
	flush(&reopened);
	check(&reopened, "table");
	crash(&reopened);
}

/// A hard delete erases the history of the key: it is a barrier, not a version.
#[test(tokio::test)]
async fn a_hard_delete_erases_the_history_below_it() {
	let dir = TempDir::new("versioning_hard_delete").unwrap();
	let tree = quiet(dir.path(), 0).await;
	set_at(&tree, &[(b"k", b"old", 100), (b"other", b"kept", 100)]).await;
	flush(&tree);
	let mut txn = tree.begin().unwrap();
	txn.delete(b"k").unwrap();
	txn.commit().await.unwrap();
	set_at(&tree, &[(b"k", b"new", 300)]).await;

	let check = |tree: &Tree, what: &str| {
		assert_eq!(get_at(tree, b"k", 150), None, "{what}: nothing before the delete");
		assert_eq!(get_at(tree, b"k", 350), v(b"new"), "{what}");
		assert_eq!(versions_of(tree, b"k"), vec![(300, v(b"new"))], "{what}");
		assert_eq!(versions_of(tree, b"other"), vec![(100, v(b"kept"))], "{what}");
	};
	check(&tree, "memtable over a table");
	flush(&tree);
	check(&tree, "two tables");
	compact(&tree);
	check(&tree, "compacted");
	crash(&tree);
}

#[test(tokio::test)]
async fn the_timestamp_range_of_history_keeps_what_is_inside_it() {
	let dir = TempDir::new("versioning_ts_range").unwrap();
	let tree = quiet(dir.path(), 0).await;
	for (round, ts) in [(1u64, 100u64), (2, 200), (3, 300), (4, 400)] {
		set_at(&tree, &[(b"a", format!("a{round}").as_bytes(), ts)]).await;
		set_at(&tree, &[(b"b", format!("b{round}").as_bytes(), ts + 5)]).await;
		// A table for each round, so that tables are left out by their times.
		flush(&tree);
	}

	let in_range = |tree: &Tree, start: u64, end: u64| -> Vec<(Vec<u8>, u64)> {
		let txn = tree.begin().unwrap();
		history_with(&txn, &HistoryOptions::new().with_ts_range(start, end))
			.into_iter()
			.map(|(k, ts, ..)| (k, ts))
			.collect()
	};
	for _ in 0..2 {
		assert_eq!(
			in_range(&tree, 200, 300),
			vec![(b"a".to_vec(), 300), (b"a".to_vec(), 200), (b"b".to_vec(), 205)]
		);
		assert_eq!(in_range(&tree, 301, 399), vec![(b"b".to_vec(), 305)]);
		assert_eq!(in_range(&tree, 0, 99), vec![]);
		assert_eq!(in_range(&tree, 0, u64::MAX).len(), 8);
		assert_eq!(in_range(&tree, 400, 400), vec![(b"a".to_vec(), 400)]);
		compact(&tree);
	}
	crash(&tree);
}

#[test(tokio::test)]
async fn the_limit_of_history_counts_versions() {
	let dir = TempDir::new("versioning_limit").unwrap();
	let tree = quiet(dir.path(), 0).await;
	for ts in [100u64, 200, 300, 400, 500] {
		set_at(&tree, &[(b"a", format!("a{ts}").as_bytes(), ts)]).await;
	}
	let txn = tree.begin().unwrap();
	let limited = history_with(&txn, &HistoryOptions::new().with_limit(3));
	assert_eq!(limited.iter().map(|v| v.1).collect::<Vec<_>>(), vec![500, 400, 300]);
	crash(&tree);
}

/// `prev` walks the versions in the reverse order of `next`, key by key.
#[test(tokio::test)]
async fn history_walks_backwards_as_it_does_forwards() {
	let dir = TempDir::new("versioning_backwards_walk").unwrap();
	let tree = quiet(dir.path(), 0).await;
	set_at(&tree, &[(b"a", b"a1", 100), (b"b", b"b1", 110)]).await;
	flush(&tree);
	set_at(&tree, &[(b"a", b"a2", 200), (b"b", b"b2", 210), (b"c", b"c1", 220)]).await;
	flush(&tree);
	set_at(&tree, &[(b"a", b"a3", 300)]).await;

	let txn = tree.begin().unwrap();
	let forward: Vec<(Vec<u8>, u64)> =
		history_with(&txn, &HistoryOptions::new()).into_iter().map(|(k, ts, ..)| (k, ts)).collect();
	let mut iter = txn
		.history_with_options(b"\x00".as_slice(), b"\xff".as_slice(), &HistoryOptions::new())
		.unwrap();
	let mut backward = Vec::new();
	let mut valid = iter.seek_last().unwrap();
	while valid {
		let key = iter.key();
		backward.push((key.user_key().to_vec(), key.timestamp()));
		valid = iter.prev().unwrap();
	}
	// Backward, the versions of a key come oldest first: a key at a time, in reverse.
	let mut expected = Vec::new();
	let mut i = forward.len();
	while i > 0 {
		let key = &forward[i - 1].0;
		let mut start = i - 1;
		while start > 0 && &forward[start - 1].0 == key {
			start -= 1;
		}
		expected.extend(forward[start..i].iter().rev().cloned());
		i = start;
	}
	assert_eq!(backward, expected);
	crash(&tree);
}

/// What the transaction has written and not committed is the newest version of its key, and is
/// reported with the time it was given.
#[test(tokio::test)]
async fn a_transaction_sees_its_own_writes_in_the_history_as_the_newest_versions() {
	let dir = TempDir::new("versioning_ryow").unwrap();
	let tree = quiet(dir.path(), 0).await;
	set_at(&tree, &[(b"a", b"a1", 100), (b"b", b"b1", 100)]).await;
	flush(&tree);

	let mut txn = tree.begin().unwrap();
	txn.set_at(b"a", b"a2", 500).unwrap();
	txn.set_at(b"c", b"c1", 500).unwrap();
	txn.delete(b"b").unwrap();
	let seen = history_with(&txn, &HistoryOptions::new());
	assert_eq!(
		seen,
		vec![
			(b"a".to_vec(), 500, false, v(b"a2")),
			(b"a".to_vec(), 100, false, v(b"a1")),
			(b"c".to_vec(), 500, false, v(b"c1")),
		],
		"a hard delete erases the history of b, even of the committed one"
	);
	assert_eq!(txn.get_at(b"a", 400).unwrap(), v(b"a1"));
	assert_eq!(txn.get_at(b"a", 500).unwrap(), v(b"a2"));
	txn.rollback();

	// Nothing of it was committed.
	assert_eq!(versions_of(&tree, b"a"), vec![(100, v(b"a1"))]);
	crash(&tree);
}

/// A transaction reads at its snapshot: a version committed after it began is not in its history.
#[test(tokio::test)]
async fn a_snapshot_does_not_see_versions_committed_after_it_began() {
	let dir = TempDir::new("versioning_snapshot").unwrap();
	let tree = quiet(dir.path(), 0).await;
	set_at(&tree, &[(b"k", b"one", 100)]).await;
	let reader = tree.begin().unwrap();
	set_at(&tree, &[(b"k", b"two", 200)]).await;
	flush(&tree);
	compact(&tree);

	assert_eq!(reader.get_at(b"k", u64::MAX).unwrap(), v(b"one"));
	let seen = history_with(&reader, &HistoryOptions::new());
	assert_eq!(seen, vec![(b"k".to_vec(), 100, false, v(b"one"))]);
	drop(reader);
	assert_eq!(get_at(&tree, b"k", u64::MAX), v(b"two"));
	crash(&tree);
}

/// Values above the threshold go to the value log. The pointer carries the timestamp.
#[test(tokio::test)]
async fn versions_in_the_value_log_keep_their_timestamps() {
	let dir = TempDir::new("versioning_vlog").unwrap();
	let tree = Arc::new(
		TreeBuilder::with_options(Options {
			enable_vlog: true,
			vlog_value_threshold: 64,
			..options(dir.path(), 0)
		})
		.build()
		.unwrap(),
	);
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
	let big = |i: u8| vec![i; 500];
	set_at(&tree, &[(b"k", &big(1), 100)]).await;
	flush(&tree);
	set_at(&tree, &[(b"k", &big(2), 200), (b"small", b"inline", 200)]).await;

	let check = |tree: &Tree, what: &str| {
		assert_eq!(get_at(tree, b"k", 150), Some(big(1)), "{what}");
		assert_eq!(get_at(tree, b"k", 250), Some(big(2)), "{what}");
		assert_eq!(
			versions_of(tree, b"k"),
			vec![(200, Some(big(2))), (100, Some(big(1)))],
			"{what}"
		);
		assert_eq!(get_at(tree, b"small", 250), v(b"inline"), "{what}");
	};
	check(&tree, "pointers in a table and values in the memtable");
	flush(&tree);
	compact(&tree);
	check(&tree, "pointers in a compacted table");
	crash(&tree);
}

// ---------------------------------------------------------------------------
// what is stored, and what an unversioned tree does
// ---------------------------------------------------------------------------

/// The memtable holds a version's value with its time in front of it, and an unversioned tree
/// holds the value as it always did, and has no versions to ask for.
#[test(tokio::test)]
async fn a_versioned_tree_stores_the_timestamp_with_the_value_and_an_unversioned_one_does_not() {
	let stored = |tree: &Tree| -> Vec<u8> {
		let active = tree.core.inner.active_memtable.read().unwrap().clone();
		let mut iter = active.iter();
		assert!(iter.seek_first().unwrap());
		iter.value_encoded().unwrap().to_vec()
	};

	let dir = TempDir::new("versioning_stored_versioned").unwrap();
	let tree = quiet(dir.path(), 0).await;
	set_at(&tree, &[(b"k", b"value", 1234)]).await;
	let encoded = stored(&tree);
	assert_eq!(ValueLocation::peek_timestamp(&encoded), Some(1234));
	assert_eq!(encoded.len(), 2 + 8 + b"value".len());
	crash(&tree);

	let dir = TempDir::new("versioning_stored_plain").unwrap();
	let plain = Arc::new(
		TreeBuilder::with_options(Options {
			enable_versioning: false,
			..options(dir.path(), 0)
		})
		.build()
		.unwrap(),
	);
	let tm = plain.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
	let mut txn = plain.begin().unwrap();
	txn.set(b"k", b"value").unwrap();
	txn.commit().await.unwrap();
	let encoded = stored(&plain);
	assert_eq!(ValueLocation::peek_timestamp(&encoded), None);
	assert_eq!(encoded.len(), 2 + b"value".len());
	let txn = plain.begin().unwrap();
	assert!(txn.get_at(b"k", 1).is_err(), "an unversioned tree has no versions to ask for");
	assert!(txn.history(b"a", b"z").is_err());
	crash(&plain);
}

/// Versioning needs neither the value log nor an index: it works with the options of a plain
/// tree.
#[test(tokio::test)]
async fn versioning_needs_no_value_log() {
	let dir = TempDir::new("versioning_no_vlog").unwrap();
	let tree = Arc::new(
		TreeBuilder::new()
			.with_path(dir.path().to_path_buf())
			.with_versioning(true, 0)
			.with_enable_vlog(false)
			.build()
			.unwrap(),
	);
	set_at(&tree, &[(b"k", b"one", 100)]).await;
	set_at(&tree, &[(b"k", b"two", 200)]).await;
	assert_eq!(get_at(&tree, b"k", 150), v(b"one"));
	assert_eq!(get_at(&tree, b"k", 250), v(b"two"));
	tree.close().await.unwrap();
}

/// A tree that is opened versioned, then not, then versioned again: what was written while it was
/// not versioned has no time, and reads as written when the version before it was, and so as the
/// newest write. It is never read as older than the versions before it.
#[test(tokio::test)]
async fn a_tree_that_is_versioned_then_not_then_versioned_again_reads_its_newest_writes_as_newest()
{
	let dir = TempDir::new("versioning_toggle").unwrap();
	let tree = quiet(dir.path(), 0).await;
	set_at(&tree, &[(b"k", b"a", 100), (b"gone", b"here", 120)]).await;
	let mut txn = tree.begin().unwrap();
	txn.soft_delete_with_options(
		b"gone",
		&crate::WriteOptions::default().with_timestamp(Some(150)),
	)
	.unwrap();
	txn.commit().await.unwrap();
	crash(&tree);

	// Opened without versioning: writes that have no time, over the stamped ones.
	let plain = Arc::new(
		TreeBuilder::with_options(Options {
			enable_versioning: false,
			..options(dir.path(), 0)
		})
		.build()
		.unwrap(),
	);
	let tm = plain.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
	let mut txn = plain.begin().unwrap();
	txn.set(b"k", b"b").unwrap();
	txn.set(b"gone", b"back").unwrap();
	txn.commit().await.unwrap();
	crash(&plain);

	let check = |tree: &Tree, what: &str| {
		let txn = tree.begin().unwrap();
		assert_eq!(txn.get(b"k").unwrap(), v(b"b"), "{what}");
		assert_eq!(txn.get(b"gone").unwrap(), v(b"back"), "{what}");
		assert_eq!(get_at(tree, b"k", u64::MAX), v(b"b"), "{what}: the newest write is newest");
		assert_eq!(get_at(tree, b"gone", u64::MAX), v(b"back"), "{what}");
		assert_eq!(get_at(tree, b"k", 99), None, "{what}");
		assert_eq!(get_at(tree, b"gone", 149), v(b"here"), "{what}");
		// At the time of the version before it, the later write is the newer of the two.
		assert_eq!(get_at(tree, b"k", 100), v(b"b"), "{what}");
		assert_eq!(
			versions_of(tree, b"k"),
			vec![(100, v(b"b")), (100, v(b"a"))],
			"{what}: ordered by time, then by write"
		);
		assert_eq!(
			versions_of(tree, b"gone"),
			vec![(150, v(b"back")), (150, None), (120, v(b"here"))],
			"{what}"
		);
	};
	let tree = quiet(dir.path(), 0).await;
	check(&tree, "from the log");
	flush(&tree);
	check(&tree, "from a table");
	compact(&tree);
	check(&tree, "compacted");
	crash(&tree);
}

/// A version written while the database had no timestamps, as a database of before the stamps
/// has, counts as written at 0: the oldest version.
#[test(tokio::test)]
async fn what_a_database_without_timestamps_held_is_the_oldest_version() {
	let dir = TempDir::new("versioning_unstamped").unwrap();
	let tree = quiet(dir.path(), 0).await;

	// A table with a value that has no timestamp, as an older engine wrote it.
	let inner = &tree.core.inner;
	let seq =
		tree.core.commit_pipeline.log_seq_num.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
	let mut batch = crate::batch::Batch::new(seq);
	let unstamped = ValueLocation::with_inline_value(b"before".to_vec()).encode();
	batch.add_record(crate::InternalKeyKind::Set, b"k".to_vec(), Some(unstamped), 0).unwrap();
	let log_number = inner.level_manifest.read().unwrap().get_log_number();
	let table_id = inner.level_manifest.read().unwrap().next_table_id();
	inner
		.write_batch_direct_to_l0_sst(&batch, table_id, log_number.saturating_sub(1), u64::MAX)
		.unwrap();
	inner.visible_seq_num.fetch_max(seq, std::sync::atomic::Ordering::SeqCst);

	set_at(&tree, &[(b"k", b"after", 500)]).await;
	assert_eq!(get_at(&tree, b"k", 0), v(b"before"));
	assert_eq!(get_at(&tree, b"k", 499), v(b"before"));
	assert_eq!(get_at(&tree, b"k", 500), v(b"after"));
	assert_eq!(versions_of(&tree, b"k"), vec![(500, v(b"after")), (0, v(b"before"))]);
	crash(&tree);
}

// ---------------------------------------------------------------------------
// retention
// ---------------------------------------------------------------------------

/// Without retention nothing is dropped, however many times the tables are compacted.
#[test(tokio::test)]
async fn retention_zero_keeps_every_version_through_every_compaction() {
	let dir = TempDir::new("versioning_keep_all").unwrap();
	let tree = quiet(dir.path(), 0).await;
	let mut expected = Vec::new();
	for round in 1..=12u64 {
		let value = format!("v{round}").into_bytes();
		set_at(&tree, &[(b"k", &value, round * 10)]).await;
		expected.insert(0, (round * 10, Some(value)));
		flush(&tree);
		if round % 3 == 0 {
			compact(&tree);
		}
	}
	compact(&tree);
	compact(&tree);
	assert_eq!(versions_of(&tree, b"k"), expected);
	crash(&tree);
}

/// With a retention, a version is kept while it is younger than the period, and so is the newest
/// version that is older: it is what a read at the start of the period sees.
#[test(tokio::test)]
async fn a_retention_drops_what_is_older_than_the_period_but_the_version_it_starts_at() {
	const HOUR: u64 = 3_600_000_000_000;
	let dir = TempDir::new("versioning_retention").unwrap();
	let tree = quiet(dir.path(), HOUR).await;
	let now = tree.core.opts.clock.now();

	// Three versions older than the hour, one inside it, one a minute ago.
	let writes: [(&[u8], u64); 5] = [
		(b"v1", now - 5 * HOUR),
		(b"v2", now - 4 * HOUR),
		(b"v3", now - 3 * HOUR),
		(b"v4", now - HOUR / 2),
		(b"v5", now - 60_000_000_000),
	];
	for (value, ts) in writes {
		set_at(&tree, &[(b"k", value, ts)]).await;
		flush(&tree);
	}
	// A key with only old versions keeps the newest one: the value a read now sees.
	set_at(&tree, &[(b"old", b"o1", now - 9 * HOUR)]).await;
	set_at(&tree, &[(b"old", b"o2", now - 8 * HOUR)]).await;
	flush(&tree);

	compact(&tree);
	compact(&tree);
	let k: Vec<_> = versions_of(&tree, b"k").into_iter().map(|(_, value)| value).collect();
	assert_eq!(k, vec![v(b"v5"), v(b"v4"), v(b"v3")], "the hour's versions and the one before it");
	assert_eq!(get_at(&tree, b"k", now - 3 * HOUR), v(b"v3"), "the start of the period reads");
	assert_eq!(get_at(&tree, b"k", now), v(b"v5"));
	let old: Vec<_> = versions_of(&tree, b"old").into_iter().map(|(_, value)| value).collect();
	assert_eq!(old, vec![v(b"o2")]);
	crash(&tree);
}

/// A hard delete is a barrier that history does not look past, and a compaction keeps it for as
/// long as there are versions below it in tables it did not read. With every version kept, which is
/// the case here, the delete is kept as a version that is not hidden; the same with a retention is
/// `a_retention_keeps_a_hard_delete_above_versions_in_deeper_tables`.
#[test(tokio::test)]
async fn a_compaction_keeps_a_hard_delete_that_hides_versions_in_deeper_tables() {
	let dir = TempDir::new("versioning_barrier").unwrap();
	let tree = quiet(dir.path(), 0).await;
	set_at(&tree, &[(b"k", b"old1", 100)]).await;
	flush(&tree);
	set_at(&tree, &[(b"k", b"old2", 200)]).await;
	flush(&tree);
	// Push the old versions down, then delete and write again above them.
	push_down(&tree);
	assert_deep(&tree, "the old versions");
	let mut txn = tree.begin().unwrap();
	txn.delete(b"k").unwrap();
	txn.commit().await.unwrap();
	flush(&tree);
	set_at(&tree, &[(b"k", b"new", 300)]).await;
	flush(&tree);
	assert_eq!(versions_of(&tree, b"k"), vec![(300, v(b"new"))]);
	compact_with(&tree, 1);
	assert_eq!(levels_with_tables(&tree), vec![1, 4], "the compaction did not read the deep table");
	assert_eq!(versions_of(&tree, b"k"), vec![(300, v(b"new"))], "the delete still hides them");
	compact_with(&tree, 1);
	assert_eq!(versions_of(&tree, b"k"), vec![(300, v(b"new"))]);
	assert_eq!(get_at(&tree, b"k", 250), None);
	crash(&tree);
}

/// A range delete is in the table of its start key, and a compaction reads the tables that overlap
/// its keys, so the range delete that hides the versions it reads can be in a table it does not
/// read. Readers read it all the same: the version a read at the start of the period sees is the
/// newest that the range delete does not hide.
#[test(tokio::test)]
async fn a_retention_knows_the_range_deletes_of_tables_the_compaction_does_not_read() {
	let dir = TempDir::new("versioning_retention_range").unwrap();
	let clock = Arc::new(SetClock::default());
	clock.0.store(1100, std::sync::atomic::Ordering::SeqCst);
	let tree = Arc::new(
		Tree::new(Arc::new(Options {
			clock: Arc::<SetClock>::clone(&clock),
			..options(dir.path(), 1000)
		}))
		.unwrap(),
	);
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;

	// A version at 90 in a table of the first level, with a neighbour
	set_at(&tree, &[(b"m", b"v90", 90)]).await;
	flush(&tree);
	set_at(&tree, &[(b"n", b"n1", 10)]).await;
	flush(&tree);
	compact(&tree);
	// A range delete, from a key before it, in a table of its own next to it
	let mut txn = tree.begin().unwrap();
	txn.delete_range(b"a", b"z").unwrap();
	txn.commit().await.unwrap();
	flush(&tree);
	set_at(&tree, &[(b"b", b"b1", 10)]).await;
	flush(&tree);
	compact(&tree);
	// Versions written after it, at earlier times than the one it hides: 70 is the one at 100
	set_at(&tree, &[(b"m", b"v70", 70)]).await;
	flush(&tree);
	set_at(&tree, &[(b"m", b"v60", 60)]).await;
	set_at(&tree, &[(b"m", b"v150", 150)]).await;
	flush(&tree);
	assert_eq!(get_at(&tree, b"m", 100), v(b"v70"), "before the compaction");
	// Two tables in the first level next to the two of the second: the compaction reads the two
	// and the one of the second that holds `m`, and not the one that holds the range delete
	assert_eq!(levels_with_tables(&tree), vec![0, 1]);
	let range_delete_tables = tables_with_range_deletes(&tree);
	assert_eq!(range_delete_tables.len(), 1, "one table holds the range delete");
	compact(&tree);
	assert_eq!(levels_with_tables(&tree), vec![1]);
	assert_eq!(
		tables_with_range_deletes(&tree),
		range_delete_tables,
		"the table of the range delete was not an input of the compaction"
	);
	assert_eq!(get_at(&tree, b"m", 100), v(b"v70"), "after it");
	assert_eq!(get_at(&tree, b"m", 150), v(b"v150"));
	assert_eq!(get_at(&tree, b"m", 90), v(b"v70"));
	crash(&tree);
}

/// A version written while the tree was not versioned reads as written when the versions before
/// it were, and those can be in a table that lies after a timestamp range: leaving that table out
/// of a history of the range would read the version at a time inside it.
#[test(tokio::test)]
async fn a_timestamp_range_does_not_change_the_time_a_version_without_one_reads_at() {
	let dir = TempDir::new("versioning_toggle_range").unwrap();
	let tree = quiet(dir.path(), 0).await;
	set_at(&tree, &[(b"k", b"a", 100)]).await;
	flush(&tree);
	crash(&tree);

	let plain = Arc::new(
		TreeBuilder::with_options(Options {
			enable_versioning: false,
			..options(dir.path(), 0)
		})
		.build()
		.unwrap(),
	);
	let tm = plain.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
	let mut txn = plain.begin().unwrap();
	txn.set(b"k", b"b").unwrap();
	txn.commit().await.unwrap();
	crash(&plain);

	let tree = quiet(dir.path(), 0).await;
	for stage in ["from the log", "from tables"] {
		let txn = tree.begin().unwrap();
		let ranged = |start, end| {
			history_with(&txn, &HistoryOptions::new().with_ts_range(start, end))
				.into_iter()
				.map(|(_, time, _, value)| (time, value))
				.collect::<Vec<_>>()
		};
		assert_eq!(ranged(0, 99), vec![], "{stage}: nothing was written before 100");
		assert_eq!(ranged(0, 100), vec![(100, v(b"b")), (100, v(b"a"))], "{stage}");
		assert_eq!(ranged(100, 200), vec![(100, v(b"b")), (100, v(b"a"))], "{stage}");
		assert_eq!(ranged(101, 200), vec![], "{stage}");
		assert_eq!(txn.get_at(b"k", 99).unwrap(), None, "{stage}");
		assert_eq!(txn.get_at(b"k", 100).unwrap(), v(b"b"), "{stage}");
		flush(&tree);
	}
	crash(&tree);
}

/// A version without a time reads as written when the versions before it were, and a compaction
/// above the bottom level does not read all of them: one in a deeper table can make it newer than
/// the compaction can tell, and put it inside the period. It is kept, and the version a read at the
/// start of the period would see without it.
#[test(tokio::test)]
async fn a_retention_keeps_a_version_without_a_time_that_a_deeper_table_may_make_newer() {
	let dir = TempDir::new("versioning_retention_unstamped").unwrap();
	let clock = Arc::new(SetClock::default());
	clock.0.store(1100, std::sync::atomic::Ordering::SeqCst);
	let versioned = |versioning: bool| {
		Arc::new(
			Tree::new(Arc::new(Options {
				enable_versioning: versioning,
				clock: Arc::<SetClock>::clone(&clock),
				..options(dir.path(), 1000)
			}))
			.unwrap(),
		)
	};
	let quiet = |tree: Arc<Tree>| async move {
		let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
		tm.stop().await;
		tree
	};

	// A version at 120, pushed down to a deep level
	let tree = quiet(versioned(true)).await;
	set_at(&tree, &[(b"k", b"stamped", 120)]).await;
	flush(&tree);
	set_at(&tree, &[(b"other", b"x", 10)]).await;
	flush(&tree);
	push_down(&tree);
	assert_deep(&tree, "the version");
	crash(&tree);

	// A write while the tree was not versioned, which reads as written at 120
	let plain = quiet(versioned(false)).await;
	let mut txn = plain.begin().unwrap();
	txn.set(b"k", b"unstamped").unwrap();
	txn.commit().await.unwrap();
	crash(&plain);

	// A backfill, at a time before it and before the start of the period at 100
	let tree = quiet(versioned(true)).await;
	set_at(&tree, &[(b"k", b"backfill", 80)]).await;
	flush(&tree);
	assert_eq!(get_at(&tree, b"k", 130), v(b"unstamped"), "before the compaction");
	assert_eq!(get_at(&tree, b"k", 110), v(b"backfill"));
	compact_with(&tree, 1);
	assert_eq!(get_at(&tree, b"k", 130), v(b"unstamped"), "after it");
	assert_eq!(get_at(&tree, b"k", 110), v(b"backfill"));
	crash(&tree);
}

/// A version written while the tree was not versioned reads as written when the versions before it
/// were. A compaction at the bottom that keeps it as the version a read at the start of the period
/// sees, and drops the versions it was read from, must give it the time: a read after it answers
/// as a read before it.
#[test(tokio::test)]
async fn a_retention_gives_a_version_without_a_time_the_time_it_read_at_when_it_drops_the_versions_before_it(
) {
	let dir = TempDir::new("versioning_retention_stamp").unwrap();
	let clock = Arc::new(SetClock::default());
	clock.0.store(760, std::sync::atomic::Ordering::SeqCst);
	let open = |versioned: bool| {
		Arc::new(
			Tree::new(Arc::new(Options {
				enable_versioning: versioned,
				// A tree of two levels: a compaction of the first is the bottom one
				level_count: 2,
				clock: Arc::<SetClock>::clone(&clock),
				..options(dir.path(), 300)
			}))
			.unwrap(),
		)
	};
	let quiet = |tree: Arc<Tree>| async move {
		let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
		tm.stop().await;
		tree
	};

	let tree = quiet(open(true)).await;
	set_at(&tree, &[(b"k", b"a", 250)]).await;
	set_at(&tree, &[(b"k", b"b", 200)]).await;
	crash(&tree);
	// Written without a time: it reads as written at 250, with a, and after it
	let plain = quiet(open(false)).await;
	let mut txn = plain.begin().unwrap();
	txn.set(b"k", b"unstamped").unwrap();
	txn.commit().await.unwrap();
	crash(&plain);
	let tree = quiet(open(true)).await;
	set_at(&tree, &[(b"k", b"c", 650)]).await;
	// Written after it, at a time before it: the newest write, and older in time
	set_at(&tree, &[(b"k", b"d", 200)]).await;

	let check = |tree: &Tree, what: &str| {
		assert_eq!(get_at(tree, b"k", 575), v(b"unstamped"), "{what}: at 575");
		assert_eq!(get_at(tree, b"k", 250), v(b"unstamped"), "{what}: at 250");
		assert_eq!(get_at(tree, b"k", 249), v(b"d"), "{what}: at 249");
		assert_eq!(get_at(tree, b"k", 650), v(b"c"), "{what}: at 650");
	};
	check(&tree, "before the compaction");
	flush(&tree);
	compact_with(&tree, 1);
	check(&tree, "after it");
	// What it was written without is dropped: the versions it was read from, and the stamp remains
	assert_eq!(
		versions_of(&tree, b"k"),
		vec![(650, v(b"c")), (250, v(b"unstamped")), (200, v(b"d"))]
	);
	crash(&tree);
	let tree = quiet(open(true)).await;
	check(&tree, "after a restart");
	crash(&tree);
}

/// What a bottom compaction under a retention does with a version written without a time that it
/// keeps: the same tree, written to without versioning in between, the versions before and after
/// the compaction, and reads that must be the same before it, after it and after a restart.
/// `write` is what is written without versioning, and the versions `before` it are those of key
/// `k` once a write at 650 and, if `with_later`, one at 200 have been made.
async fn restamp_scenario(
	vlog: bool,
	write: impl FnOnce(&mut Transaction),
	with_later: bool,
	before: Vec<(u64, Option<Vec<u8>>)>,
	reads: Vec<(u64, Option<Vec<u8>>)>,
	after: Vec<(u64, Option<Vec<u8>>)>,
) {
	let dir = TempDir::new("versioning_retention_restamp_kinds").unwrap();
	let clock = Arc::new(SetClock::default());
	clock.0.store(760, std::sync::atomic::Ordering::SeqCst);
	let open = |versioned: bool| {
		Arc::new(
			Tree::new(Arc::new(Options {
				enable_versioning: versioned,
				enable_vlog: vlog,
				vlog_value_threshold: 64,
				// A tree of two levels: a compaction of the first is the bottom one
				level_count: 2,
				clock: Arc::<SetClock>::clone(&clock),
				..options(dir.path(), 300)
			}))
			.unwrap(),
		)
	};
	let quiet = |tree: Arc<Tree>| async move {
		let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
		tm.stop().await;
		tree
	};

	let tree = quiet(open(true)).await;
	set_at(&tree, &[(b"k", b"a", 250)]).await;
	set_at(&tree, &[(b"k", b"b", 200)]).await;
	crash(&tree);
	let plain = quiet(open(false)).await;
	let mut txn = plain.begin().unwrap();
	write(&mut txn);
	txn.commit().await.unwrap();
	crash(&plain);
	let tree = quiet(open(true)).await;
	set_at(&tree, &[(b"k", b"c", 650)]).await;
	if with_later {
		set_at(&tree, &[(b"k", b"d", 200)]).await;
	}

	let check = |tree: &Tree, what: &str| {
		for (time, value) in &reads {
			assert_eq!(get_at(tree, b"k", *time), value.clone(), "{what}: at {time}");
		}
	};
	assert_eq!(versions_of(&tree, b"k"), before, "before the compaction");
	check(&tree, "before the compaction");
	flush(&tree);
	compact_with(&tree, 1);
	// The versions it was read from are gone, and it reads as it did
	assert_eq!(versions_of(&tree, b"k"), after, "after the compaction");
	check(&tree, "after the compaction");
	crash(&tree);
	let tree = quiet(open(true)).await;
	assert_eq!(versions_of(&tree, b"k"), after, "after a restart");
	check(&tree, "after a restart");
	crash(&tree);
}

/// A soft delete written without versioning has no value, and is stamped with a header and the
/// time when a compaction keeps it.
#[test(tokio::test)]
async fn a_retention_stamps_a_soft_delete_without_a_time_that_it_keeps() {
	restamp_scenario(
		false,
		|txn| txn.soft_delete(b"k").unwrap(),
		true,
		vec![(650, v(b"c")), (250, None), (250, v(b"a")), (200, v(b"d")), (200, v(b"b"))],
		vec![(649, None), (250, None), (249, v(b"d")), (650, v(b"c"))],
		vec![(650, v(b"c")), (250, None), (200, v(b"d"))],
	)
	.await;
}

/// A value in the value log is stamped in its pointer, and is read back whole.
#[test(tokio::test)]
async fn a_retention_stamps_a_value_in_the_value_log_that_it_keeps() {
	let big = Some(vec![b'x'; 500]);
	restamp_scenario(
		true,
		|txn| txn.set(b"k", vec![b'x'; 500]).unwrap(),
		true,
		vec![(650, v(b"c")), (250, big.clone()), (250, v(b"a")), (200, v(b"d")), (200, v(b"b"))],
		vec![(649, big.clone()), (250, big.clone()), (249, v(b"d")), (650, v(b"c"))],
		vec![(650, v(b"c")), (250, big), (200, v(b"d"))],
	)
	.await;
}

/// A replace written without versioning reads at 0 and hides what is before it; it is stamped with
/// that time when a compaction keeps it, and its value is not touched.
#[test(tokio::test)]
async fn a_retention_stamps_a_replace_without_a_time_that_it_keeps() {
	restamp_scenario(
		false,
		|txn| txn.replace(b"k", b"r").unwrap(),
		false,
		vec![(650, v(b"c")), (0, v(b"r"))],
		vec![(0, v(b"r")), (649, v(b"r")), (650, v(b"c"))],
		vec![(650, v(b"c")), (0, v(b"r"))],
	)
	.await;
}

/// A hard delete that a newer one hides from every reader is not read, and is not kept above the
/// bottom: the newer one hides what is below as well. It is not in any history, so the table the
/// compaction writes is looked at.
#[test(tokio::test)]
async fn a_retention_does_not_keep_a_hard_delete_that_another_hides() {
	let dir = TempDir::new("versioning_retention_hidden_barrier").unwrap();
	let clock = Arc::new(SetClock::default());
	clock.0.store(1100, std::sync::atomic::Ordering::SeqCst);
	let tree = Arc::new(
		Tree::new(Arc::new(Options {
			clock: Arc::<SetClock>::clone(&clock),
			..options(dir.path(), 1000)
		}))
		.unwrap(),
	);
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;

	set_at(&tree, &[(b"k", b"a", 100)]).await;
	flush(&tree);
	set_at(&tree, &[(b"k", b"b", 200)]).await;
	flush(&tree);
	push_down(&tree);
	assert_deep(&tree, "the versions");
	for _ in 0..2 {
		let mut txn = tree.begin().unwrap();
		txn.delete(b"k").unwrap();
		txn.commit().await.unwrap();
		flush(&tree);
	}
	set_at(&tree, &[(b"k", b"c", 300)]).await;
	flush(&tree);
	compact_with(&tree, 1);
	assert_eq!(levels_with_tables(&tree), vec![1, 4], "the compaction did not read the deep table");
	assert_eq!(versions_of(&tree, b"k"), vec![(300, v(b"c"))]);
	assert_eq!(get_at(&tree, b"k", 250), None);
	let manifest = tree.core.inner.level_manifest.read().unwrap();
	let entries: Vec<u64> =
		manifest.levels.0[1].tables.iter().map(|table| table.meta.properties.num_entries).collect();
	drop(manifest);
	assert_eq!(entries, vec![2], "the write at 300, and the newer of the two deletes");
	crash(&tree);
}

/// A range delete of a transaction that is rolled back to a savepoint is not one it has any more,
/// and one that is not is: what the transaction reads of the keys it covers follows.
#[test(tokio::test)]
async fn a_rollback_to_a_savepoint_takes_its_range_deletes_away_from_every_read() {
	let dir = TempDir::new("versioning_range_savepoint").unwrap();
	let tree = quiet(dir.path(), 0).await;
	for (key, time) in [(b"a", 10), (b"b", 20), (b"c", 30)] {
		set_at(&tree, &[(key, b"stored", time)]).await;
	}
	let keys = |keys: &[&[u8]]| keys.iter().map(|key| key.to_vec()).collect::<Vec<_>>();

	// Two range deletes, one rolled back: the first still hides a and b
	let mut txn = tree.begin().unwrap();
	txn.delete_range(b"a", b"c").unwrap();
	txn.set_savepoint().unwrap();
	txn.delete_range(b"b", b"c").unwrap();
	txn.rollback_to_savepoint().unwrap();
	for key in [b"a", b"b"] {
		assert_eq!(txn.get(key).unwrap(), None);
		assert_eq!(txn.get_async(key).await.unwrap(), None);
		assert_eq!(txn.get_at(key, u64::MAX).unwrap(), None);
	}
	assert_eq!(txn.get(b"c").unwrap(), v(b"stored"));
	assert_eq!(scan_keys(&txn), (keys(&[b"c"]), keys(&[b"c"])));
	drop(txn);

	// The only one rolled back: nothing is hidden
	let mut txn = tree.begin().unwrap();
	txn.set_at(b"a", b"written", 500).unwrap();
	txn.set_savepoint().unwrap();
	txn.delete_range(b"a", b"z").unwrap();
	assert_eq!(txn.get(b"b").unwrap(), None);
	txn.rollback_to_savepoint().unwrap();
	assert_eq!(txn.get(b"a").unwrap(), v(b"written"));
	assert_eq!(txn.get_async(b"b").await.unwrap(), v(b"stored"));
	assert_eq!(txn.get_at(b"b", u64::MAX).unwrap(), v(b"stored"));
	assert_eq!(scan_keys(&txn), (keys(&[b"a", b"b", b"c"]), keys(&[b"a", b"b", b"c"])));
	let history_keys: Vec<_> =
		history_with(&txn, &HistoryOptions::new()).into_iter().map(|(key, ..)| key).collect();
	assert_eq!(history_keys, keys(&[b"a", b"a", b"b", b"c"]));
	crash(&tree);
}

/// A key that a transaction writes at many times in turn, more than it looks back over, still has
/// one version for each time: the last write of it.
#[test(tokio::test)]
async fn a_key_written_at_many_times_in_turn_has_the_last_write_of_each_time() {
	let dir = TempDir::new("versioning_many_times").unwrap();
	let tree = quiet(dir.path(), 0).await;
	let value = |round: u32, time: u64| format!("{round}:{time}").into_bytes();
	for times in [9u64, 17, 40] {
		let mut txn = tree.begin().unwrap();
		let key = format!("k{times}").into_bytes();
		for round in 0..3u32 {
			for time in 1..=times {
				txn.set_at(key.as_slice(), value(round, time).as_slice(), 1000 + time).unwrap();
			}
		}
		let versions = |txn: &Transaction| -> Vec<(u64, Option<Vec<u8>>)> {
			history_with(txn, &HistoryOptions::new().with_tombstones(true))
				.into_iter()
				.filter(|(k, ..)| *k == key)
				.map(|(_, time, _, value)| (time, value))
				.collect()
		};
		let expected: Vec<_> =
			(1..=times).rev().map(|time| (1000 + time, Some(value(2, time)))).collect();
		assert_eq!(versions(&txn), expected, "{times} times, before the commit");
		txn.commit().await.unwrap();
		let reader = tree.begin().unwrap();
		assert_eq!(versions(&reader), expected, "{times} times, after it");
	}
	crash(&tree);
}

/// A range delete in a memtable that has been rotated and not yet flushed is read by `get_at` and
/// `history` as one in a table is.
#[test(tokio::test)]
async fn a_range_delete_in_an_immutable_memtable_is_read_by_get_at_and_history() {
	let dir = TempDir::new("versioning_range_immutable").unwrap();
	let tree = quiet(dir.path(), 0).await;
	set_at(&tree, &[(b"k", b"v", 100)]).await;
	flush(&tree);
	let mut txn = tree.begin().unwrap();
	txn.delete_range(b"a", b"z").unwrap();
	txn.commit().await.unwrap();
	// Rotated, not flushed: the range delete is in an immutable memtable
	tree.core.inner.rotate_memtable().unwrap();
	assert_eq!(get_at(&tree, b"k", 150), None);
	assert_eq!(versions_of(&tree, b"k"), vec![]);
	assert_eq!(tree.begin().unwrap().get(b"k").unwrap(), None);
	// And once it is written to a table
	tree.core.inner.flush_all_immutables_sync().unwrap();
	assert_eq!(get_at(&tree, b"k", 150), None);
	assert_eq!(versions_of(&tree, b"k"), vec![]);
	crash(&tree);
}

/// A hard delete is a barrier that hides the versions below it, in tables the compaction does not
/// read as well. It is not history itself, and a period does not hold it, but it is kept for as
/// long as there can be versions below it.
#[test(tokio::test)]
async fn a_retention_keeps_a_hard_delete_above_versions_in_deeper_tables() {
	let dir = TempDir::new("versioning_retention_barrier").unwrap();
	let clock = Arc::new(SetClock::default());
	clock.0.store(1100, std::sync::atomic::Ordering::SeqCst);
	let tree = Arc::new(
		Tree::new(Arc::new(Options {
			clock: Arc::<SetClock>::clone(&clock),
			..options(dir.path(), 1000)
		}))
		.unwrap(),
	);
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;

	// Versions at 100 and 200, pushed down to a deep level
	set_at(&tree, &[(b"k", b"a", 100)]).await;
	flush(&tree);
	set_at(&tree, &[(b"k", b"b", 200)]).await;
	flush(&tree);
	push_down(&tree);
	assert_deep(&tree, "the versions");
	// A hard delete, and a write after it
	let mut txn = tree.begin().unwrap();
	txn.delete(b"k").unwrap();
	txn.commit().await.unwrap();
	flush(&tree);
	set_at(&tree, &[(b"k", b"c", 300)]).await;
	flush(&tree);
	assert_eq!(versions_of(&tree, b"k"), vec![(300, v(b"c"))], "before the compaction");
	compact_with(&tree, 1);
	assert_eq!(levels_with_tables(&tree), vec![1, 4], "the compaction did not read the deep table");
	assert_eq!(versions_of(&tree, b"k"), vec![(300, v(b"c"))], "the delete still hides them");
	assert_eq!(get_at(&tree, b"k", 250), None);
	assert_eq!(get_at(&tree, b"k", 150), None);
	assert_eq!(get_at(&tree, b"k", 300), v(b"c"));
	crash(&tree);
}

/// An unversioned tree is compacted as it always was: one version per key.
#[test(tokio::test)]
async fn an_unversioned_tree_keeps_one_version_per_key() {
	let dir = TempDir::new("versioning_plain_compaction").unwrap();
	let plain = Arc::new(
		TreeBuilder::with_options(Options {
			enable_versioning: false,
			..options(dir.path(), 0)
		})
		.build()
		.unwrap(),
	);
	let tm = plain.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
	for round in 1..=4u32 {
		let mut txn = plain.begin().unwrap();
		txn.set(b"k", round.to_string().as_bytes()).unwrap();
		txn.commit().await.unwrap();
		flush(&plain);
	}
	let plain_opts = Arc::new(Options {
		level0_max_files: 2,
		..Default::default()
	});
	plain.compact(Arc::new(Strategy::from_options(plain_opts))).unwrap();
	let manifest = plain.core.inner.level_manifest.read().unwrap();
	let entries: u64 = manifest.iter().map(|t| t.meta.properties.num_entries).sum();
	assert_eq!(entries, 1, "one version of the key is left");
	drop(manifest);
	crash(&plain);
}

// ---------------------------------------------------------------------------
// range deletes, pending writes and readers
// ---------------------------------------------------------------------------

/// A range delete erases the history of the keys it covers, as a hard delete does for one key:
/// `get_at` and `history` agree with a plain read, before and after a compaction.
#[test(tokio::test)]
async fn a_range_delete_erases_the_history_of_the_keys_it_covers() {
	let dir = TempDir::new("versioning_range_delete").unwrap();
	let tree = quiet(dir.path(), 0).await;
	set_at(
		&tree,
		&[(b"a", b"a1", 100), (b"b", b"b1", 100), (b"c", b"c1", 100), (b"d", b"d1", 100)],
	)
	.await;
	set_at(&tree, &[(b"b", b"b2", 200)]).await;
	flush(&tree);
	let mut txn = tree.begin().unwrap();
	txn.delete_range(b"b", b"d").unwrap();
	txn.commit().await.unwrap();
	set_at(&tree, &[(b"c", b"c2", 300)]).await;

	let check = |tree: &Tree, what: &str| {
		assert_eq!(get_at(tree, b"a", 150), v(b"a1"), "{what}");
		assert_eq!(get_at(tree, b"b", u64::MAX), None, "{what}: erased");
		assert_eq!(get_at(tree, b"b", 150), None, "{what}: and its history with it");
		assert_eq!(get_at(tree, b"c", 150), None, "{what}");
		assert_eq!(get_at(tree, b"c", 350), v(b"c2"), "{what}: written after the delete");
		assert_eq!(get_at(tree, b"d", 150), v(b"d1"), "{what}: the end is not covered");
		let seen: Vec<_> = history(tree).into_iter().map(|(k, ts, ..)| (k, ts)).collect();
		assert_eq!(
			seen,
			vec![(b"a".to_vec(), 100), (b"c".to_vec(), 300), (b"d".to_vec(), 100),],
			"{what}"
		);
	};
	check(&tree, "memtable over a table");
	flush(&tree);
	check(&tree, "tables");
	compact(&tree);
	check(&tree, "compacted");
	crash(&tree);
}

/// A range delete that the transaction has not committed hides the snapshot's versions of the
/// keys it covers, for `get_at` and `history`, as it does for a read.
#[test(tokio::test)]
async fn a_range_delete_of_the_transaction_hides_the_versions_the_snapshot_holds() {
	let dir = TempDir::new("versioning_pending_range_delete").unwrap();
	let tree = quiet(dir.path(), 0).await;
	set_at(&tree, &[(b"a", b"a1", 100), (b"b", b"b1", 100), (b"c", b"c1", 100)]).await;
	flush(&tree);

	let mut txn = tree.begin().unwrap();
	txn.delete_range(b"b", b"c").unwrap();
	assert_eq!(txn.get(b"b").unwrap(), None);
	assert_eq!(txn.get_at(b"b", 150).unwrap(), None);
	assert_eq!(txn.get_at(b"a", 150).unwrap(), v(b"a1"));
	assert_eq!(txn.get_at(b"c", 150).unwrap(), v(b"c1"));
	let seen: Vec<_> =
		history_with(&txn, &HistoryOptions::new()).into_iter().map(|v| v.0).collect();
	assert_eq!(seen, vec![b"a".to_vec(), b"c".to_vec()]);
	// A write after the range delete is visible, and one before it is not.
	txn.set_at(b"b", b"b2", 500).unwrap();
	assert_eq!(txn.get_at(b"b", 500).unwrap(), v(b"b2"));
	assert_eq!(txn.get_at(b"b", 150).unwrap(), None);
	txn.rollback();
	assert_eq!(get_at(&tree, b"b", 150), v(b"b1"), "nothing was committed");
	crash(&tree);
}

/// A write that is to be stamped with the time of its commit is newer than every version, and is
/// reported as such: the timestamps of `history` descend, and a read at a past time does not
/// see it, whatever the cursor of a caller does with the first version not after that time.
#[test(tokio::test)]
async fn a_pending_write_without_a_timestamp_is_the_newest_version_and_not_seen_in_the_past() {
	let dir = TempDir::new("versioning_pending_commit_time").unwrap();
	let tree = quiet(dir.path(), 0).await;
	set_at(&tree, &[(b"k", b"old", 100)]).await;
	set_at(&tree, &[(b"k", b"mid", 500)]).await;

	let mut txn = tree.begin().unwrap();
	txn.set(b"k", b"new").unwrap();
	let seen: Vec<_> = history_with(&txn, &HistoryOptions::new())
		.into_iter()
		.map(|(_, ts, _, value)| (ts, value))
		.collect();
	assert_eq!(seen, vec![(u64::MAX, v(b"new")), (500, v(b"mid")), (100, v(b"old"))]);
	// The first version not after a time is the version at that time.
	assert_eq!(txn.get_at(b"k", 300).unwrap(), v(b"old"));
	assert_eq!(txn.get_at(b"k", 600).unwrap(), v(b"mid"));
	assert_eq!(txn.get_at(b"k", u64::MAX).unwrap(), v(b"new"));
	// The timestamps ascend going backwards.
	let mut iter = txn
		.history_with_options(
			b"\x00".as_slice(),
			b"\xff".as_slice(),
			&HistoryOptions::new().with_tombstones(true),
		)
		.unwrap();
	let mut backward = Vec::new();
	let mut valid = iter.seek_last().unwrap();
	while valid {
		backward.push(iter.key().timestamp());
		valid = iter.prev().unwrap();
	}
	assert_eq!(backward, vec![100, 500, u64::MAX]);
	drop(iter);

	// A soft delete of the transaction: the key is deleted at the end of time, not before.
	let mut txn2 = tree.begin().unwrap();
	txn2.soft_delete(b"k").unwrap();
	assert_eq!(txn2.get_at(b"k", 600).unwrap(), v(b"mid"));
	assert_eq!(txn2.get_at(b"k", u64::MAX).unwrap(), None);
	// A range of times that ends before the commit leaves the pending write out.
	let bounded =
		history_with(&txn2, &HistoryOptions::new().with_tombstones(true).with_ts_range(0, 1000));
	assert_eq!(bounded.iter().map(|v| v.1).collect::<Vec<_>>(), vec![500, 100]);
	crash(&tree);
}

/// A reader that began before a hard delete still has the versions under it in its history, and a
/// compaction of the tables keeps them while it is open.
#[test(tokio::test)]
async fn a_compaction_keeps_the_history_an_open_reader_has_below_a_hard_delete() {
	let dir = TempDir::new("versioning_reader_barrier").unwrap();
	let tree = quiet(dir.path(), 0).await;
	set_at(&tree, &[(b"k", b"one", 100)]).await;
	set_at(&tree, &[(b"k", b"two", 200)]).await;
	flush(&tree);
	let reader = tree.begin().unwrap();

	let mut txn = tree.begin().unwrap();
	txn.delete(b"k").unwrap();
	txn.commit().await.unwrap();
	set_at(&tree, &[(b"k", b"three", 300)]).await;
	flush(&tree);
	compact(&tree);
	compact(&tree);

	// The reader's history is what it was.
	let seen: Vec<_> =
		history_with(&reader, &HistoryOptions::new()).into_iter().map(|v| v.1).collect();
	assert_eq!(seen, vec![200, 100]);
	assert_eq!(reader.get_at(b"k", 150).unwrap(), v(b"one"));
	drop(reader);

	// Everyone else's is not: the delete hides them.
	assert_eq!(versions_of(&tree, b"k"), vec![(300, v(b"three"))]);
	assert_eq!(get_at(&tree, b"k", 150), None);
	crash(&tree);
}

/// `get_at` of a key that has no versions does not read the history of its neighbour.
#[test(tokio::test)]
async fn get_at_of_a_key_that_is_not_there_is_none_next_to_a_key_with_a_long_history() {
	let dir = TempDir::new("versioning_absent_key").unwrap();
	let tree = quiet(dir.path(), 0).await;
	for ts in 1..=200u64 {
		set_at(&tree, &[(b"b", format!("v{ts}").as_bytes(), ts)]).await;
	}
	flush(&tree);
	assert_eq!(get_at(&tree, b"a", u64::MAX), None);
	assert_eq!(get_at(&tree, b"c", u64::MAX), None);
	assert_eq!(get_at(&tree, b"b", 100), v(b"v100"));
	crash(&tree);
}

// ---------------------------------------------------------------------------
// what a transaction writes to a key at one time is one version of it
// ---------------------------------------------------------------------------

/// The versions of the key `k` that a transaction has, as `history` lists them, with their times.
fn versions_in(txn: &Transaction) -> Vec<(u64, Option<Vec<u8>>)> {
	history_with(txn, &HistoryOptions::new().with_tombstones(true))
		.into_iter()
		.filter(|(key, ..)| key == b"k")
		.map(|(_, time, _, value)| (time, value))
		.collect()
}

/// A transaction is atomic: what it writes to a key at one time is one version of it, the last of
/// its writes, whether or not a savepoint came between them. The states in between are ones no
/// reader could have seen, and they are in no history, before the commit or after it.
#[test(tokio::test)]
async fn a_transaction_writes_one_version_of_a_key_for_a_time_whatever_the_savepoints() {
	let dir = TempDir::new("versioning_atomic").unwrap();
	let tree = quiet(dir.path(), 0).await;

	// At a time the transaction chose
	let mut txn = tree.begin().unwrap();
	txn.set_at(b"k", b"first", 100).unwrap();
	txn.set_savepoint().unwrap();
	txn.set_at(b"k", b"second", 100).unwrap();
	txn.set_savepoint().unwrap();
	txn.soft_delete_with_options(b"k", &crate::WriteOptions::default().with_timestamp(Some(100)))
		.unwrap();
	txn.set_savepoint().unwrap();
	txn.set_at(b"k", b"last", 100).unwrap();
	assert_eq!(versions_in(&txn), vec![(100, v(b"last"))], "before the commit");
	txn.commit().await.unwrap();
	assert_eq!(versions_of(&tree, b"k"), vec![(100, v(b"last"))], "after it");

	// At the time of the commit
	let mut txn = tree.begin().unwrap();
	txn.set(b"k", b"first").unwrap();
	txn.set_savepoint().unwrap();
	txn.set(b"k", b"second").unwrap();
	assert_eq!(
		versions_in(&txn),
		vec![(u64::MAX, v(b"second")), (100, v(b"last"))],
		"before the commit"
	);
	txn.commit().await.unwrap();
	let after = versions_of(&tree, b"k");
	assert_eq!(after.len(), 2, "after it: {after:?}");
	assert_eq!(after[0].1, v(b"second"));
	assert_eq!(after[1], (100, v(b"last")));
	crash(&tree);
}

/// Writes at different times are different versions, and so are those of different transactions
/// at one time.
#[test(tokio::test)]
async fn writes_at_different_times_and_of_different_transactions_are_different_versions() {
	let dir = TempDir::new("versioning_atomic_times").unwrap();
	let tree = quiet(dir.path(), 0).await;

	let mut txn = tree.begin().unwrap();
	txn.set_at(b"k", b"early", 100).unwrap();
	txn.set_savepoint().unwrap();
	txn.set_at(b"k", b"late", 200).unwrap();
	txn.set_at(b"k", b"early again", 100).unwrap();
	assert_eq!(versions_in(&txn), vec![(200, v(b"late")), (100, v(b"early again"))]);
	txn.commit().await.unwrap();
	assert_eq!(versions_of(&tree, b"k"), vec![(200, v(b"late")), (100, v(b"early again"))]);

	// A transaction of its own writes the same time, and both are kept, the newer first
	set_at(&tree, &[(b"k", b"another", 100)]).await;
	assert_eq!(
		versions_of(&tree, b"k"),
		vec![(200, v(b"late")), (100, v(b"another")), (100, v(b"early again"))]
	);
	// A write at the time of the commit is another time than one given a time
	let mut txn = tree.begin().unwrap();
	txn.set_at(b"k", b"timed", 300).unwrap();
	txn.set(b"k", b"now").unwrap();
	assert_eq!(versions_in(&txn)[0], (u64::MAX, v(b"now")));
	assert_eq!(versions_in(&txn)[1], (300, v(b"timed")));
	txn.commit().await.unwrap();
	assert_eq!(versions_of(&tree, b"k").len(), 5);
	crash(&tree);
}

/// A rollback to a savepoint gives back what was there before it, though a later write at the
/// same time supersedes it in the history of the transaction.
#[test(tokio::test)]
async fn a_rollback_to_a_savepoint_restores_what_a_later_write_of_the_same_time_superseded() {
	let dir = TempDir::new("versioning_atomic_rollback").unwrap();
	let tree = quiet(dir.path(), 0).await;
	let mut txn = tree.begin().unwrap();
	txn.set_at(b"k", b"kept", 100).unwrap();
	txn.set_savepoint().unwrap();
	txn.set_at(b"k", b"undone", 100).unwrap();
	assert_eq!(versions_in(&txn), vec![(100, v(b"undone"))]);
	txn.rollback_to_savepoint().unwrap();
	assert_eq!(versions_in(&txn), vec![(100, v(b"kept"))]);
	assert_eq!(txn.get_at(b"k", 100).unwrap(), v(b"kept"));
	txn.commit().await.unwrap();
	assert_eq!(versions_of(&tree, b"k"), vec![(100, v(b"kept"))]);
	crash(&tree);
}

/// A hard delete and a replace wipe the history of the key whatever is written after them in the
/// same transaction, at one time or another.
#[test(tokio::test)]
async fn a_hard_delete_or_a_replace_wipes_the_history_whatever_the_transaction_writes_after_it() {
	let dir = TempDir::new("versioning_atomic_barrier").unwrap();
	let tree = quiet(dir.path(), 0).await;
	set_at(&tree, &[(b"k", b"old1", 100)]).await;
	set_at(&tree, &[(b"k", b"old2", 200)]).await;

	// A delete, then a write at the time of the commit, then one at a time of its own
	let mut txn = tree.begin().unwrap();
	txn.delete(b"k").unwrap();
	txn.set(b"k", b"after").unwrap();
	assert_eq!(versions_in(&txn), vec![(u64::MAX, v(b"after"))], "delete, then a write");
	txn.commit().await.unwrap();
	let after = versions_of(&tree, b"k");
	assert_eq!(after.len(), 1, "{after:?}");
	assert_eq!(after[0].1, v(b"after"));

	set_at(&tree, &[(b"k", b"old3", 50)]).await;
	// A replace, then a write at the same time: the write wins, and the history is wiped
	let mut txn = tree.begin().unwrap();
	txn.replace(b"k", b"replaced").unwrap();
	txn.set(b"k", b"over it").unwrap();
	assert_eq!(versions_in(&txn), vec![(u64::MAX, v(b"over it"))], "replace, then a write");
	txn.commit().await.unwrap();
	let after = versions_of(&tree, b"k");
	assert_eq!(after.len(), 1, "{after:?}");
	assert_eq!(after[0].1, v(b"over it"));

	// A replace, a savepoint, then a soft delete at the same time: it is the version
	set_at(&tree, &[(b"k", b"old4", 50)]).await;
	let mut txn = tree.begin().unwrap();
	txn.replace(b"k", b"replaced").unwrap();
	txn.set_savepoint().unwrap();
	txn.soft_delete(b"k").unwrap();
	assert_eq!(versions_in(&txn), vec![(u64::MAX, None)], "replace, savepoint, soft delete");
	txn.commit().await.unwrap();
	let after = versions_of(&tree, b"k");
	assert_eq!(after.len(), 1, "{after:?}");
	assert_eq!(after[0].1, None);
	crash(&tree);
}

// ---------------------------------------------------------------------------
// a range delete of a transaction is read by everything the transaction reads with
// ---------------------------------------------------------------------------

/// The keys a scan of the whole key space gives a transaction, forwards and backwards.
fn scan_keys(txn: &Transaction) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
	let mut iter = txn.range(b"\x00".as_slice(), b"\xff".as_slice()).unwrap();
	let mut forward = Vec::new();
	let mut valid = iter.seek_first().unwrap();
	while valid {
		forward.push(iter.key().user_key().to_vec());
		valid = iter.next().unwrap();
	}
	let mut backward = Vec::new();
	let mut valid = iter.seek_last().unwrap();
	while valid {
		backward.push(iter.key().user_key().to_vec());
		valid = iter.prev().unwrap();
	}
	backward.reverse();
	(forward, backward)
}

/// What a transaction deleted with a range, it no longer reads: with `get`, with `get_async`,
/// with a scan in either direction, and in the history it has of the keys.
#[test(tokio::test)]
async fn a_transaction_reads_nothing_of_what_it_deleted_with_a_range() {
	let dir = TempDir::new("versioning_range_ryow").unwrap();
	let tree = quiet(dir.path(), 0).await;
	for (key, time) in [(b"a", 10), (b"b", 20), (b"c", 30), (b"d", 40)] {
		set_at(&tree, &[(key, b"stored", time)]).await;
	}
	let keys = |keys: &[&[u8]]| keys.iter().map(|key| key.to_vec()).collect::<Vec<_>>();

	let mut txn = tree.begin().unwrap();
	txn.delete_range(b"b", b"d").unwrap();
	// The start key is deleted, and so is every key after it that the range covers
	for key in [b"b", b"c"] {
		assert_eq!(txn.get(key).unwrap(), None);
		assert_eq!(txn.get_async(key).await.unwrap(), None);
		assert_eq!(txn.get_at(key, u64::MAX).unwrap(), None);
	}
	for key in [b"a", b"d"] {
		assert_eq!(txn.get(key).unwrap(), v(b"stored"));
		assert_eq!(txn.get_async(key).await.unwrap(), v(b"stored"));
	}
	let expected = keys(&[b"a", b"d"]);
	assert_eq!(scan_keys(&txn), (expected.clone(), expected));
	let history_keys: Vec<_> =
		history_with(&txn, &HistoryOptions::new()).into_iter().map(|(key, ..)| key).collect();
	assert_eq!(history_keys, keys(&[b"a", b"d"]));

	// A write after the range delete is read, and one before it is not
	txn.set(b"c", b"again").unwrap();
	assert_eq!(txn.get(b"c").unwrap(), v(b"again"));
	assert_eq!(txn.get_async(b"c").await.unwrap(), v(b"again"));
	let expected = keys(&[b"a", b"c", b"d"]);
	assert_eq!(scan_keys(&txn), (expected.clone(), expected));
	txn.commit().await.unwrap();
	let reader = tree.begin().unwrap();
	let expected = keys(&[b"a", b"c", b"d"]);
	assert_eq!(scan_keys(&reader), (expected.clone(), expected));
	crash(&tree);
}

/// A range delete that starts before the keys that are read, with writes of the transaction and
/// other range deletes between them, in whichever order the write set keeps them.
#[test(tokio::test)]
async fn a_transaction_orders_its_range_deletes_and_writes_by_when_it_did_them() {
	let dir = TempDir::new("versioning_range_ryow_order").unwrap();
	let tree = quiet(dir.path(), 0).await;
	for key in [b"b", b"m", b"p", b"q"] {
		set_at(&tree, &[(key, b"stored", 10)]).await;
	}
	let mut txn = tree.begin().unwrap();
	txn.delete_range(b"m", b"z").unwrap();
	txn.set(b"p", b"written").unwrap();
	txn.delete_range(b"a", b"z").unwrap();
	// The later range delete, which starts first among the keys, hides the write before it
	for key in [b"b", b"m", b"p", b"q"] {
		assert_eq!(txn.get(key).unwrap(), None, "get {key:?}");
		assert_eq!(txn.get_async(key).await.unwrap(), None, "get_async {key:?}");
	}
	assert_eq!(scan_keys(&txn), (vec![], vec![]));
	// A range delete that starts before the range read hides what is in it
	let mut iter = txn.range(b"n".as_slice(), b"z".as_slice()).unwrap();
	assert!(!iter.seek_first().unwrap());
	drop(iter);
	txn.set(b"q", b"last").unwrap();
	assert_eq!(txn.get_async(b"q").await.unwrap(), v(b"last"));
	assert_eq!(scan_keys(&txn), (vec![b"q".to_vec()], vec![b"q".to_vec()]));
	crash(&tree);
}

// ---------------------------------------------------------------------------
// versioning on an encrypted tree
// ---------------------------------------------------------------------------

/// The history of an encrypted tree: the stamps are in the values, which are sealed with the
/// tables, the log and the value log, so what is read back is the same as in the clear, from the
/// tables after a restart as well.
#[test(tokio::test)]
async fn a_versioned_encrypted_tree_keeps_and_reads_its_history() {
	if tfs::skip_listed() {
		return;
	}
	use super::encryption_open_tests::{manager, SUITES};

	let big = vec![b'x'; 500];
	for suite in SUITES {
		let dir = TempDir::new("versioning_encrypted").unwrap();
		let root = dir.path().join("db");
		let build = || {
			TreeBuilder::new()
				.with_path(root.clone())
				.with_enable_vlog(true)
				.with_vlog_value_threshold(64)
				.with_flush_on_close(true)
				.with_versioning(true, 0)
				.with_encryption(manager(), suite)
				.build()
				.unwrap()
		};

		let tree = build();
		set_at(&tree, &[(b"k", b"one", 100)]).await;
		set_at(&tree, &[(b"k", b"two", 200)]).await;
		set_at(&tree, &[(b"k", &big, 300)]).await;
		let mut txn = tree.begin().unwrap();
		txn.soft_delete_with_options(
			b"k",
			&crate::WriteOptions::default().with_timestamp(Some(400)),
		)
		.unwrap();
		txn.commit().await.unwrap();

		let check = |tree: &Tree, what: &str| {
			assert_eq!(get_at(tree, b"k", 150), v(b"one"), "{suite:?} {what}");
			assert_eq!(get_at(tree, b"k", 250), v(b"two"), "{suite:?} {what}");
			assert_eq!(get_at(tree, b"k", 350), Some(big.clone()), "{suite:?} {what}");
			assert_eq!(get_at(tree, b"k", 450), None, "{suite:?} {what}");
			assert_eq!(
				versions_of(tree, b"k"),
				vec![(400, None), (300, Some(big.clone())), (200, v(b"two")), (100, v(b"one"))],
				"{suite:?} {what}"
			);
		};
		check(&tree, "from the log and the memtable");
		tree.close().await.unwrap();

		// Closed with the memtable flushed: from sealed tables and the sealed value log
		let tree = build();
		check(&tree, "from the tables");
		tree.close().await.unwrap();
	}
}

// ---------------------------------------------------------------------------
// what a transaction keeps, and what a tree that is not versioned writes
// ---------------------------------------------------------------------------

/// A key that a transaction writes at a few times in turn keeps one write for each, however
/// often it is rewritten.
#[test(tokio::test)]
async fn a_key_written_at_a_few_times_in_turn_keeps_a_write_for_each() {
	let dir = TempDir::new("versioning_bounded").unwrap();
	let tree = quiet(dir.path(), 0).await;
	let mut txn = tree.begin().unwrap();
	for i in 0..1000u32 {
		txn.set(b"k", i.to_be_bytes().as_slice()).unwrap();
		txn.set_at(b"k", i.to_be_bytes().as_slice(), 100).unwrap();
		txn.set_at(b"k", i.to_be_bytes().as_slice(), 200).unwrap();
	}
	assert!(txn.write_set[b"k".as_slice()].len() <= 4, "{}", txn.write_set[b"k".as_slice()].len());
	assert_eq!(
		versions_in(&txn),
		vec![
			(u64::MAX, Some(999u32.to_be_bytes().to_vec())),
			(200, Some(999u32.to_be_bytes().to_vec())),
			(100, Some(999u32.to_be_bytes().to_vec()))
		]
	);
	txn.commit().await.unwrap();
	assert_eq!(versions_of(&tree, b"k").len(), 3);
	crash(&tree);
}

/// A tree that is not versioned logs what a transaction wrote as it wrote it: a write after a
/// savepoint is a write of its own, as it always was, and takes a sequence number.
#[test(tokio::test)]
async fn a_tree_that_is_not_versioned_logs_each_write_of_a_transaction() {
	let dir = TempDir::new("versioning_unversioned_log").unwrap();
	let tree = Arc::new(
		TreeBuilder::with_options(Options {
			enable_versioning: false,
			..options(dir.path(), 0)
		})
		.build()
		.unwrap(),
	);
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
	let before = tree.core.seq_num();
	let mut txn = tree.begin().unwrap();
	txn.set(b"k", b"a").unwrap();
	txn.set_savepoint().unwrap();
	txn.set(b"k", b"b").unwrap();
	txn.replace(b"r", b"x").unwrap();
	txn.set_savepoint().unwrap();
	txn.set(b"r", b"y").unwrap();
	txn.commit().await.unwrap();
	assert_eq!(tree.core.seq_num() - before, 4, "four writes, of two keys, each of its own");
	let reader = tree.begin().unwrap();
	assert_eq!(reader.get(b"k").unwrap(), v(b"b"));
	assert_eq!(reader.get(b"r").unwrap(), v(b"y"));
	crash(&tree);
}

// ---------------------------------------------------------------------------
// a model of the versions of a key, checked against random sequences of everything
// ---------------------------------------------------------------------------

/// A version in the model: a write, with the time it was given.
#[derive(Clone, Debug)]
struct ModelVersion {
	/// `None` for a write of a tree that was not versioned, which was given none.
	time: Option<u64>,
	/// `None` for a soft delete.
	value: Option<Vec<u8>>,
	/// The transaction that wrote it.
	txn: u64,
}

/// What `history` and `get_at` must answer, written from their definitions and nothing else: the
/// versions of a key in the order they were written in; a hard delete or a range delete erases
/// what was written before it; the answer at a time is the version with the newest time not
/// after it, and the newest write among those of that time. A transaction is atomic: what it
/// writes to a key at one time is one version, the last of its writes. A write of a tree that was
/// not versioned has no time: it reads as written at the newest time of the versions written
/// before it, and at 0 if there is none.
#[derive(Clone, Default)]
struct Model {
	keys: std::collections::BTreeMap<Vec<u8>, Vec<ModelVersion>>,
}

impl Model {
	/// A write of the transaction `txn`: a version, in place of one that the same transaction
	/// wrote at the same time.
	fn write(&mut self, key: &[u8], value: Option<&[u8]>, time: Option<u64>, txn: u64) {
		let versions = self.keys.entry(key.to_vec()).or_default();
		versions.retain(|v| !(v.txn == txn && v.time == time));
		versions.push(ModelVersion {
			time,
			value: value.map(<[u8]>::to_vec),
			txn,
		});
	}

	/// A replace is a version and hides every version written before it.
	fn replace(&mut self, key: &[u8], value: &[u8], time: Option<u64>, txn: u64) {
		self.keys.remove(key);
		self.write(key, Some(value), time, txn);
	}

	fn delete(&mut self, key: &[u8]) {
		self.keys.remove(key);
	}

	fn delete_range(&mut self, start: &[u8], end: &[u8]) {
		self.keys.retain(|key, _| !(key.as_slice() >= start && key.as_slice() < end));
	}

	/// The times the versions of a key read at, in the order they were written in.
	fn times(versions: &[ModelVersion]) -> Vec<u64> {
		let mut newest = 0;
		versions
			.iter()
			.map(|v| match v.time {
				Some(time) => {
					newest = newest.max(time);
					time
				}
				None => newest,
			})
			.collect()
	}

	/// The versions of a key as `history` returns them, with tombstones if asked for.
	fn history(
		&self,
		key: &[u8],
		tombstones: bool,
		range: Option<(u64, u64)>,
	) -> Vec<(u64, Option<Vec<u8>>)> {
		let Some(versions) = self.keys.get(key) else {
			return Vec::new();
		};
		let times = Self::times(versions);
		let mut indexed: Vec<(usize, &ModelVersion)> = versions.iter().enumerate().collect();
		// Newest time first, and the newest write first among versions of one time.
		indexed.sort_by(|a, b| times[b.0].cmp(&times[a.0]).then(b.0.cmp(&a.0)));
		indexed
			.into_iter()
			.filter(|(_, v)| tombstones || v.value.is_some())
			.filter(|(at, _)| range.is_none_or(|(s, e)| times[*at] >= s && times[*at] <= e))
			.map(|(at, v)| (times[at], v.value.clone()))
			.collect()
	}

	fn get_at(&self, key: &[u8], time: u64) -> Option<Vec<u8>> {
		self.history(key, true, None).into_iter().find(|(t, _)| *t <= time).and_then(|(_, v)| v)
	}

	/// What a plain read returns: the newest write.
	fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
		self.keys.get(key).and_then(|versions| versions.last()).and_then(|v| v.value.clone())
	}
}

/// A small deterministic generator.
struct Rng(u64);

impl Rng {
	fn next(&mut self) -> u64 {
		self.0 ^= self.0 << 13;
		self.0 ^= self.0 >> 7;
		self.0 ^= self.0 << 17;
		self.0
	}

	fn below(&mut self, n: u64) -> u64 {
		self.next() % n
	}
}

const KEYS: [&[u8]; 6] = [b"k0", b"k1", b"k2", b"k3", b"k4", b"k5"];
const PROBES: [u64; 12] = [0, 99, 100, 150, 250, 400, 575, 700, 851, 1000, 2000, u64::MAX];

fn check_against_model(txn: &Transaction, model: &Model, what: &str) {
	for key in KEYS {
		let name = String::from_utf8_lossy(key);
		assert_eq!(txn.get(key).unwrap(), model.get(key), "{what}: get {name}");
		for probe in PROBES {
			assert_eq!(
				txn.get_at(key, probe).unwrap(),
				model.get_at(key, probe),
				"{what}: get_at {name} at {probe}"
			);
		}
	}
	for (tombstones, range) in [
		(true, None),
		(false, None),
		(true, Some((200, 600))),
		(false, Some((0, 300))),
		// Ranges that leave out whole tables, those before them and those after them
		(true, Some((0, 120))),
		(true, Some((700, 1100))),
	] {
		let mut opts = HistoryOptions::new().with_tombstones(tombstones);
		if let Some((s, e)) = range {
			opts = opts.with_ts_range(s, e);
		}
		let forward = history_with(txn, &opts);
		let mut expected = Vec::new();
		for key in KEYS {
			for (time, value) in model.history(key, tombstones, range) {
				expected.push((key.to_vec(), time, value.is_none(), value));
			}
		}
		assert_eq!(forward, expected, "{what}: history, tombstones {tombstones}, range {range:?}");

		// Backwards is the reverse of forwards.
		let mut iter =
			txn.history_with_options(b"\x00".as_slice(), b"\xff".as_slice(), &opts).unwrap();
		let mut backward = Vec::new();
		let mut valid = iter.seek_last().unwrap();
		while valid {
			let key = iter.key();
			let tombstone = key.is_tombstone();
			backward.push((
				key.user_key().to_vec(),
				key.timestamp(),
				tombstone,
				(!tombstone).then(|| iter.value().unwrap()),
			));
			valid = iter.prev().unwrap();
		}
		backward.reverse();
		assert_eq!(backward, expected, "{what}: history backwards, tombstones {tombstones}");

		// A limit takes the first entries going forwards, the last going backwards, and is
		// one budget for a walk that turns round.
		for limit in [1usize, 3] {
			let limited = opts.clone().with_limit(limit);
			let mut iter =
				txn.history_with_options(b"\x00".as_slice(), b"\xff".as_slice(), &limited).unwrap();
			let mut got = Vec::new();
			let mut valid = iter.seek_first().unwrap();
			while valid {
				got.push(entry_of(&iter));
				valid = iter.next().unwrap();
			}
			let first: Vec<_> = expected.iter().take(limit).cloned().collect();
			assert_eq!(got, first, "{what}: history, limit {limit}, tombstones {tombstones}");

			let mut got = Vec::new();
			let mut valid = iter.seek_last().unwrap();
			while valid {
				got.push(entry_of(&iter));
				valid = iter.prev().unwrap();
			}
			got.reverse();
			let last: Vec<_> = expected.iter().rev().take(limit).rev().cloned().collect();
			assert_eq!(
				got, last,
				"{what}: history backwards, limit {limit}, tombstones {tombstones}"
			);
		}

		// Forwards, backwards, forwards: the entries at 0, 1, 0, 1, with a budget of four.
		if expected.len() >= 2 {
			let limited = opts.clone().with_limit(4);
			let mut iter =
				txn.history_with_options(b"\x00".as_slice(), b"\xff".as_slice(), &limited).unwrap();
			assert!(iter.seek_first().unwrap());
			let mut got = vec![entry_of(&iter)];
			for forward in [true, false, true] {
				let valid = if forward {
					iter.next()
				} else {
					iter.prev()
				}
				.unwrap();
				assert!(valid, "{what}: the walk turning round ended early");
				got.push(entry_of(&iter));
			}
			assert!(!iter.next().unwrap(), "{what}: the budget of a walk is spent");
			let expected_walk: Vec<_> =
				[0, 1, 0, 1].iter().map(|at| expected[*at].clone()).collect();
			assert_eq!(got, expected_walk, "{what}: history, a walk that turns round");
		}
	}
}

/// The version an iterator is at.
fn entry_of(iter: &impl LSMIterator) -> Version {
	let key = iter.key();
	let tombstone = key.is_tombstone();
	(
		key.user_key().to_vec(),
		key.timestamp(),
		tombstone,
		(!tombstone).then(|| iter.value().unwrap()),
	)
}

/// Walks a history iterator with random steps, and checks each against a cursor over what the
/// history must list: a start by `seek_first`, `seek_last` or `seek` of a key, and steps that
/// turn round anywhere, with and without a limit. A walk that has left the entries stays out of
/// them, a limit is one budget for the walk, and a seek begins it again.
fn check_walks(txn: &Transaction, model: &Model, seed: u64, range: Option<(u64, u64)>, what: &str) {
	const TARGETS: [&[u8]; 9] = [b"k", b"k0", b"k05", b"k1", b"k15", b"k25", b"k3", b"k5", b"z"];
	let mut rng = Rng(seed | 1);
	for tombstones in [true, false] {
		let mut expected = Vec::new();
		for key in KEYS {
			for (time, value) in model.history(key, tombstones, range) {
				expected.push((key.to_vec(), time, value.is_none(), value));
			}
		}
		let n = expected.len();
		for limit in [None, Some(3usize)] {
			let mut opts = HistoryOptions::new().with_tombstones(tombstones);
			if let Some((start, end)) = range {
				opts = opts.with_ts_range(start, end);
			}
			if let Some(limit) = limit {
				opts = opts.with_limit(limit);
			}
			let mut iter =
				txn.history_with_options(b"\x00".as_slice(), b"\xff".as_slice(), &opts).unwrap();
			let spent = std::cell::Cell::new(0usize);
			let spend = |position: Option<usize>| -> Option<usize> {
				let position = position?;
				if limit.is_some_and(|limit| spent.get() >= limit) {
					return None;
				}
				spent.set(spent.get() + 1);
				Some(position)
			};
			// The start
			let (valid, mut position, start) = match rng.below(3) {
				0 => (iter.seek_first().unwrap(), (n > 0).then_some(0), "seek_first".to_string()),
				1 => (iter.seek_last().unwrap(), n.checked_sub(1), "seek_last".to_string()),
				_ => {
					let target = TARGETS[rng.below(TARGETS.len() as u64) as usize];
					let at = expected.iter().position(|entry| entry.0.as_slice() >= target);
					(
						LSMIterator::seek(&mut iter, target).unwrap(),
						at,
						format!("seek({})", String::from_utf8_lossy(target)),
					)
				}
			};
			spent.set(0);
			position = spend(position);
			let mut trail = vec![start];
			let context = format!("{what}: walk, tombstones {tombstones}, limit {limit:?}");
			assert_eq!(valid, position.is_some(), "{context}: {trail:?}");
			if let Some(at) = position {
				assert_eq!(entry_of(&iter), expected[at], "{context}: {trail:?}");
			}
			// The steps
			for _ in 0..14 {
				let forward = rng.below(2) == 0;
				trail.push(
					if forward {
						"next"
					} else {
						"prev"
					}
					.to_string(),
				);
				let valid = if forward {
					iter.next()
				} else {
					iter.prev()
				}
				.unwrap();
				position = position.and_then(|at| {
					if forward {
						(at + 1 < n).then_some(at + 1)
					} else {
						at.checked_sub(1)
					}
				});
				position = spend(position);
				assert_eq!(valid, position.is_some(), "{context}: {trail:?}");
				if let Some(at) = position {
					assert_eq!(entry_of(&iter), expected[at], "{context}: {trail:?}");
				}
			}
		}
	}
}

/// The keys and values a transaction reads with a scan, which is the newest write of each key.
fn check_scans(txn: &Transaction, model: &Model, seed: u64, what: &str) {
	const TARGETS: [&[u8]; 9] = [b"k", b"k0", b"k05", b"k1", b"k15", b"k25", b"k3", b"k5", b"z"];
	let expected: Vec<(Vec<u8>, Vec<u8>)> =
		KEYS.iter().filter_map(|key| model.get(key).map(|value| (key.to_vec(), value))).collect();
	let n = expected.len();
	let mut iter = txn.range(b"\x00".as_slice(), b"\xff".as_slice()).unwrap();
	let mut rng = Rng(seed | 1);
	for _ in 0..3 {
		let (valid, mut position, mut trail) = match rng.below(3) {
			0 => (iter.seek_first().unwrap(), (n > 0).then_some(0), vec!["seek_first".to_string()]),
			1 => (iter.seek_last().unwrap(), n.checked_sub(1), vec!["seek_last".to_string()]),
			_ => {
				let target = TARGETS[rng.below(TARGETS.len() as u64) as usize];
				(
					LSMIterator::seek(&mut iter, target).unwrap(),
					expected.iter().position(|(key, _)| key.as_slice() >= target),
					vec![format!("seek({})", String::from_utf8_lossy(target))],
				)
			}
		};
		let context = format!("{what}: scan");
		assert_eq!(valid, position.is_some(), "{context}: {trail:?}");
		if let Some(at) = position {
			assert_eq!(
				(iter.key().user_key().to_vec(), iter.value().unwrap()),
				expected[at],
				"{context}: {trail:?}"
			);
		}
		for _ in 0..12 {
			let forward = rng.below(2) == 0;
			trail.push(
				if forward {
					"next"
				} else {
					"prev"
				}
				.to_string(),
			);
			let valid = if forward {
				iter.next()
			} else {
				iter.prev()
			}
			.unwrap();
			position = position.and_then(|at| {
				if forward {
					(at + 1 < n).then_some(at + 1)
				} else {
					at.checked_sub(1)
				}
			});
			assert_eq!(valid, position.is_some(), "{context}: {trail:?}");
			if let Some(at) = position {
				assert_eq!(
					(iter.key().user_key().to_vec(), iter.value().unwrap()),
					expected[at],
					"{context}: {trail:?}"
				);
			}
		}
	}
}

/// What a tree with a retention promises: what is read inside the period is read as it was
/// written, whatever compaction has dropped from before it.
fn check_within_window(txn: &Transaction, model: &Model, cutoff: u64, what: &str) {
	for key in KEYS {
		let name = String::from_utf8_lossy(key);
		assert_eq!(txn.get(key).unwrap(), model.get(key), "{what}: get {name}");
		for probe in PROBES.into_iter().chain([cutoff, cutoff.saturating_add(1)]) {
			if probe >= cutoff {
				let (got, expected) = (txn.get_at(key, probe).unwrap(), model.get_at(key, probe));
				if got != expected {
					// What is stored and what was written, to read the failure by
					let stored: Vec<_> =
						history_with(txn, &HistoryOptions::new().with_tombstones(true))
							.into_iter()
							.filter(|(k, ..)| k.as_slice() == key)
							.map(|(_, time, tombstone, _)| (time, tombstone))
							.collect();
					let versions: Vec<_> = model
						.history(key, true, None)
						.into_iter()
						.map(|(time, value)| (time, value.is_none()))
						.collect();
					panic!(
						"{what}: get_at {name} at {probe}, the period starts at {cutoff}: {got:?}, \
						 not {expected:?}; stored {stored:?}, model {versions:?}"
					);
				}
			}
		}
	}
	for tombstones in [true, false] {
		let range = Some((cutoff, u64::MAX));
		let opts =
			HistoryOptions::new().with_tombstones(tombstones).with_ts_range(cutoff, u64::MAX);
		let mut expected = Vec::new();
		for key in KEYS {
			for (time, value) in model.history(key, tombstones, range) {
				expected.push((key.to_vec(), time, value.is_none(), value));
			}
		}
		assert_eq!(
			history_with(txn, &opts),
			expected,
			"{what}: history, tombstones {tombstones}, from {cutoff}"
		);
	}
}

/// One operation of a transaction.
#[derive(Clone, Debug)]
enum Op {
	Set(Vec<u8>, Vec<u8>, u64),
	SoftDelete(Vec<u8>, u64),
	/// A write that is given the time of the commit.
	SetNow(Vec<u8>, Vec<u8>),
	SoftDeleteNow(Vec<u8>),
	Replace(Vec<u8>, Vec<u8>),
	Delete(Vec<u8>),
	DeleteRange(Vec<u8>, Vec<u8>),
}

/// The operations of a transaction, in the order they were done in, with the savepoint each was
/// done under. What they come to is worked out from the operations alone, one after the other:
/// it does not follow how a transaction keeps them.
#[derive(Default)]
struct Pending {
	ops: Vec<(u32, Op)>,
	savepoints: u32,
}

impl Pending {
	fn push(&mut self, op: Op) {
		self.ops.push((self.savepoints, op));
	}

	fn set_savepoint(&mut self) {
		self.savepoints += 1;
	}

	/// Undoes what was done since the latest savepoint.
	fn rollback_to_savepoint(&mut self) {
		let savepoints = self.savepoints;
		self.ops.retain(|(savepoint, _)| *savepoint != savepoints);
		self.savepoints -= 1;
	}

	/// What the versions of the database are once the operations of the transaction `txn` are
	/// committed, at the time `now`. Until then a write that is to be given the time of its
	/// commit is reported at the end of time.
	fn apply(&self, model: &mut Model, now: u64, txn: u64) {
		for (_, op) in &self.ops {
			match op {
				Op::Set(key, value, time) => model.write(key, Some(value), Some(*time), txn),
				Op::SoftDelete(key, time) => model.write(key, None, Some(*time), txn),
				Op::SetNow(key, value) => model.write(key, Some(value), Some(now), txn),
				Op::SoftDeleteNow(key) => model.write(key, None, Some(now), txn),
				Op::Replace(key, value) => model.replace(key, value, Some(now), txn),
				Op::Delete(key) => model.delete(key),
				Op::DeleteRange(start, end) => model.delete_range(start, end),
			}
		}
	}
}

/// Does one random operation on the transaction, and records it.
fn random_op(
	rng: &mut Rng,
	txn: &mut Transaction,
	pending: &mut Pending,
	writes: &mut u32,
	big: bool,
	focus: Option<(&[u8], u64)>,
) {
	// A transaction that is about one key writes it at one of two times, and makes savepoints
	// more often: it is what makes writes of one time to one key, kept apart by savepoints.
	let (key, time) = match focus {
		Some((key, time)) => {
			if rng.below(4) == 0 {
				txn.set_savepoint().unwrap();
				pending.set_savepoint();
				return;
			}
			(key.to_vec(), time + 50 * rng.below(2))
		}
		None => (
			KEYS[rng.below(KEYS.len() as u64) as usize].to_vec(),
			// Times in steps of 50, going backwards as often as forwards, with ties.
			100 + 50 * rng.below(19),
		),
	};
	let value = |rng: &mut Rng, writes: &mut u32| {
		*writes += 1;
		let mut value = format!("v{writes}").into_bytes();
		if big && rng.below(2) == 0 {
			value.resize(200, b'.');
		}
		value
	};
	let op = match rng.below(100) {
		0..=4 => {
			txn.set_savepoint().unwrap();
			pending.set_savepoint();
			return;
		}
		5..=8 => {
			if pending.savepoints > 0 {
				txn.rollback_to_savepoint().unwrap();
				pending.rollback_to_savepoint();
			}
			return;
		}
		9..=43 => {
			let value = value(rng, writes);
			txn.set_at(key.as_slice(), value.as_slice(), time).unwrap();
			Op::Set(key, value, time)
		}
		44..=57 => {
			let value = value(rng, writes);
			// A time of 0 is the time of the commit
			if rng.below(4) == 0 {
				txn.set_at(key.as_slice(), value.as_slice(), 0).unwrap();
			} else {
				txn.set(key.as_slice(), value.as_slice()).unwrap();
			}
			Op::SetNow(key, value)
		}
		58..=65 => {
			txn.soft_delete_with_options(
				key.as_slice(),
				&crate::WriteOptions::default().with_timestamp(Some(time)),
			)
			.unwrap();
			Op::SoftDelete(key, time)
		}
		66..=70 => {
			txn.soft_delete(key.as_slice()).unwrap();
			Op::SoftDeleteNow(key)
		}
		71..=75 => {
			let value = value(rng, writes);
			txn.replace(key.as_slice(), value.as_slice()).unwrap();
			Op::Replace(key, value)
		}
		76..=87 => {
			txn.delete(key.as_slice()).unwrap();
			Op::Delete(key)
		}
		_ => {
			let other = KEYS[rng.below(KEYS.len() as u64) as usize].to_vec();
			let (start, end) = if key <= other {
				(key, other)
			} else {
				(other, key)
			};
			if start == end {
				return;
			}
			txn.delete_range(start.as_slice(), end.as_slice()).unwrap();
			Op::DeleteRange(start, end)
		}
	};
	pending.push(op);
}

/// Prints what a random run does, when `MODEL_TRACE` is set, to read a failure by.
fn trace(what: &str) {
	if std::env::var_os("MODEL_TRACE").is_some() {
		let shortened: String = what.chars().take(400).collect();
		eprintln!("{shortened}");
	}
}

/// A clock that tells the time it was set to, so that the time of a commit is known.
#[derive(Debug, Default)]
pub(super) struct SetClock(pub(super) std::sync::atomic::AtomicU64);

impl crate::clock::LogicalClock for SetClock {
	fn now(&self) -> u64 {
		self.0.load(std::sync::atomic::Ordering::SeqCst)
	}
}

/// How a randomised run is set up.
#[derive(Clone, Copy)]
struct Setup {
	/// Values of 200 bytes, which are kept in the value log
	big: bool,
	/// The retention period: 0 keeps every version
	retention: u64,
	/// The levels of the tree: with 2, a compaction of the first level is the bottom one
	levels: u8,
}

/// The setup of the run of a seed, so that the seeds cover all of them.
fn setup_of(seed: u64) -> Setup {
	Setup {
		big: seed % 2 == 0,
		retention: if (seed / 2) % 2 == 1 {
			300
		} else {
			0
		},
		levels: if (seed / 4) % 2 == 1 {
			2
		} else {
			6
		},
	}
}

/// What a view of the tree is checked on: everything for a tree that keeps every version, and
/// what the period promises for one that does not.
fn check_view(txn: &Transaction, model: &Model, setup: Setup, cutoff: u64, walk: u64, what: &str) {
	if setup.retention == 0 {
		check_against_model(txn, model, what);
		check_walks(txn, model, walk, None, what);
	} else {
		check_within_window(txn, model, cutoff, what);
		check_walks(txn, model, walk, Some((cutoff, u64::MAX)), what);
	}
	// What a scan reads is the newest of each key, whatever the retention
	check_scans(txn, model, walk.rotate_left(7), what);
}

/// A transaction that begins on one state of the tree and is written to while others commit, with
/// the state it began on.
struct Writer {
	txn: Transaction,
	base: Model,
	ops: Pending,
	id: u64,
	/// How many commits there had been when it began
	began_after: u64,
}

async fn random_run(seed: u64, steps: usize, setup: Setup) {
	let Setup {
		big,
		retention,
		levels,
	} = setup;
	let dir = TempDir::new("versioning_model").unwrap();
	let clock = Arc::new(SetClock::default());
	let open = |path: &Path, versioned: bool| {
		Arc::new(
			Tree::new(Arc::new(Options {
				enable_versioning: versioned,
				enable_vlog: big,
				vlog_value_threshold: 64,
				// Small files, so that the log rotates and old files are collected
				vlog_max_file_size: 1024,
				level_count: levels,
				clock: Arc::<SetClock>::clone(&clock),
				..options(path, retention)
			}))
			.unwrap(),
		)
	};
	let quiet = |tree: Arc<Tree>| async move {
		let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
		tm.stop().await;
		tree
	};
	// The period of a retention starts at 100 and moves on from there, through the times that
	// are written
	let now_of = |commits: u64| 400 + 10 * commits;
	let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
	let mut model = Model::default();
	let mut tree = quiet(open(dir.path(), true)).await;
	let mut writes = 0u32;
	let mut commits = 0u64;
	let mut transactions = 0u64;
	// A reader that stays open through flushes, compactions and writes, with what it must see.
	let mut held: Option<(Transaction, Model)> = None;
	// A transaction that began some time ago, and is written to and committed later
	let mut writer: Option<Writer> = None;
	for step in 0..steps {
		let what = format!("seed {seed} step {step}");
		match rng.below(100) {
			0..=47 => {
				// A transaction of one to four operations, read back before and after the commit.
				let mut txn = tree.begin().unwrap();
				transactions += 1;
				let id = transactions;
				let mut ops = Pending::default();
				// One in three is about one key
				let focus = (rng.below(3) == 0).then(|| {
					(KEYS[rng.below(KEYS.len() as u64) as usize], 100 + 50 * rng.below(17))
				});
				let count = if focus.is_some() {
					3 + rng.below(6)
				} else {
					1 + rng.below(4)
				};
				for _ in 0..count {
					random_op(&mut rng, &mut txn, &mut ops, &mut writes, big, focus);
				}
				let mut pending = model.clone();
				ops.apply(&mut pending, u64::MAX, id);
				let uncommitted = format!("{what} (uncommitted) after {:?}", ops.ops);
				let cutoff =
					clock.0.load(std::sync::atomic::Ordering::SeqCst).saturating_sub(retention);
				check_view(&txn, &pending, setup, cutoff, rng.next(), &uncommitted);
				// Commits come one after another in time, a little apart
				commits += 1;
				let now = now_of(commits);
				trace(&format!("{step}: commit at {now}: {:?}", ops.ops));
				clock.0.store(now, std::sync::atomic::Ordering::SeqCst);
				txn.commit().await.unwrap();
				ops.apply(&mut model, now, id);
			}
			48..=57 => {
				trace(&format!("{step}: flush"));
				flush(&tree)
			}
			58..=66 => {
				trace(&format!(
					"{step}: compact at {}",
					clock.0.load(std::sync::atomic::Ordering::SeqCst)
				));
				compact(&tree)
			}
			67..=69 => {
				trace(&format!("{step}: hold a reader"));
				held = Some((tree.begin().unwrap(), model.clone()));
			}
			70..=71 => {
				trace(&format!("{step}: restart"));
				held = None;
				writer = None;
				crash(&tree);
				tree = quiet(open(dir.path(), true)).await;
			}
			72..=74 => {
				// The tree is opened without versioning, written to, and opened with it again:
				// what was written has no time
				trace(&format!("{step}: a window without versioning"));
				held = None;
				writer = None;
				crash(&tree);
				let plain = quiet(open(dir.path(), false)).await;
				for _ in 0..1 + rng.below(3) {
					let mut txn = plain.begin().unwrap();
					transactions += 1;
					let id = transactions;
					// Each key once in a transaction: a write that follows a hard delete of the
					// key in one, in a tree without versions, is the same as a write over it
					let mut used: Vec<Vec<u8>> = Vec::new();
					for _ in 0..1 + rng.below(3) {
						let key = KEYS[rng.below(KEYS.len() as u64) as usize];
						if used.iter().any(|k| k == key) {
							continue;
						}
						used.push(key.to_vec());
						match rng.below(10) {
							0..=4 => {
								writes += 1;
								let mut value = format!("w{writes}").into_bytes();
								if big && rng.below(2) == 0 {
									value.resize(200, b'.');
								}
								txn.set(key, value.as_slice()).unwrap();
								model.write(key, Some(&value), None, id);
							}
							5 => {
								writes += 1;
								let value = format!("r{writes}").into_bytes();
								txn.replace(key, value.as_slice()).unwrap();
								model.replace(key, &value, None, id);
							}
							6..=7 => {
								txn.soft_delete(key).unwrap();
								model.write(key, None, None, id);
							}
							8 => {
								txn.delete(key).unwrap();
								model.delete(key);
							}
							_ => {
								let end = KEYS[rng.below(KEYS.len() as u64) as usize];
								if key < end {
									txn.delete_range(key, end).unwrap();
									model.delete_range(key, end);
								}
							}
						}
					}
					txn.commit().await.unwrap();
					// What was written may be in a table when the tree is versioned again
					if rng.below(3) == 0 {
						flush(&plain);
					}
				}
				let reader = plain.begin().unwrap();
				for key in KEYS {
					assert_eq!(
						reader.get(key).unwrap(),
						model.get(key),
						"{what}: get {}, without versioning",
						String::from_utf8_lossy(key)
					);
				}
				drop(reader);
				crash(&plain);
				tree = quiet(open(dir.path(), true)).await;
			}
			75..=78 => {
				if writer.is_none() {
					trace(&format!("{step}: a transaction begins"));
					transactions += 1;
					writer = Some(Writer {
						txn: tree.begin().unwrap(),
						base: model.clone(),
						ops: Pending::default(),
						id: transactions,
						began_after: commits,
					});
				}
			}
			79..=85 => {
				if let Some(w) = &mut writer {
					for _ in 0..1 + rng.below(3) {
						random_op(&mut rng, &mut w.txn, &mut w.ops, &mut writes, big, None);
					}
					trace(&format!("{step}: the transaction writes: {:?}", w.ops.ops));
				}
			}
			86..=93 => {
				if let Some(mut w) = writer.take() {
					// With nothing committed since it began there is nothing to conflict with
					let alone = commits == w.began_after;
					commits += 1;
					let now = now_of(commits);
					clock.0.store(now, std::sync::atomic::Ordering::SeqCst);
					trace(&format!("{step}: the transaction commits at {now}: {:?}", w.ops.ops));
					// What it wrote goes over what the tree is now, or it conflicts with what
					// was written since it began
					match w.txn.commit().await {
						Ok(()) => {
							trace("   committed");
							w.ops.apply(&mut model, now, w.id)
						}
						Err(error) => {
							trace("   conflict");
							assert!(
								matches!(error, crate::Error::TransactionWriteConflict) && !alone,
								"{what}: {error:?}, alone: {alone}"
							)
						}
					}
				}
			}
			94..=96 => {
				// What is in the tables goes down, so that tables of deeper levels hold older
				// versions, and a compaction of the first levels does not read them
				trace(&format!("{step}: push down"));
				push_down(&tree);
			}
			_ => {}
		}
		let cutoff = clock.0.load(std::sync::atomic::Ordering::SeqCst).saturating_sub(retention);
		check_view(&tree.begin().unwrap(), &model, setup, cutoff, rng.next(), &what);
		if let Some((txn, expected)) = &held {
			check_view(txn, expected, setup, cutoff, rng.next(), &format!("{what} (held reader)"));
		}
		if let Some(w) = &writer {
			let mut view = w.base.clone();
			w.ops.apply(&mut view, u64::MAX, w.id);
			let context =
				format!("{what} (a transaction that began earlier, after {:?})", w.ops.ops);
			check_view(&w.txn, &view, setup, cutoff, rng.next(), &context);
		}
	}
	drop(held);
	drop(writer);

	// A tree with a retention reclaims what the period lets go of: a long time after everything
	// that was written, with nothing reading, a key of a tree of two levels (where the compaction
	// of the first is the bottom one) is left with two of its versions, however many it had.
	if retention > 0 {
		const LATER: u64 = 100_000;
		clock.0.store(LATER, std::sync::atomic::Ordering::SeqCst);
		// A version of every key, so that there is something to compact with what is stored
		let mut txn = tree.begin().unwrap();
		transactions += 1;
		for key in KEYS {
			txn.set_at(key, b"last".as_slice(), LATER).unwrap();
			model.write(key, Some(b"last"), Some(LATER), transactions);
		}
		txn.commit().await.unwrap();
		// A transaction is a reader until it is dropped, and a reader keeps what it can see
		drop(txn);
		flush(&tree);
		// Everything down to the bottom level, where every version of a key is read
		push_down(&tree);
		if std::env::var_os("MODEL_TRACE").is_some() {
			let manifest = tree.core.inner.level_manifest.read().unwrap();
			for (n, level) in manifest.levels.0.iter().enumerate() {
				for t in level.tables.iter() {
					eprintln!(
						"LAYOUT L{n} t{} [{:?}..{:?}] rds={} times={:?}..{:?}",
						t.id,
						t.meta
							.smallest_point
							.as_ref()
							.map(|k| String::from_utf8_lossy(&k.user_key).to_string()),
						t.meta
							.largest_point
							.as_ref()
							.map(|k| String::from_utf8_lossy(&k.user_key).to_string()),
						t.range_deletions.read().len(),
						t.meta.properties.oldest_key_time,
						t.meta.properties.newest_key_time,
					);
				}
			}
		}
		let txn = tree.begin().unwrap();
		check_within_window(&txn, &model, LATER - retention, "after the last compactions");
		let all = history_with(&txn, &HistoryOptions::new().with_tombstones(true));
		for key in KEYS {
			let stored: Vec<_> = all
				.iter()
				.filter(|(k, ..)| k.as_slice() == key)
				.map(|(_, time, tombstone, _)| (*time, *tombstone))
				.collect();
			// At the bottom level: the version a read at the start of the period sees, and the
			// newest write, which a plain read sees whatever its time. Only a tree of two levels
			// has every compaction at the bottom: in a deeper one what the strategy leaves in
			// two levels is not merged, and a compaction above the bottom keeps more.
			let allowed = if levels == 2 {
				2
			} else {
				usize::MAX
			};
			let versions: Vec<_> = model
				.history(key, true, None)
				.iter()
				.map(|(time, value)| (*time, value.is_none()))
				.collect();
			assert!(
				stored.len() <= allowed,
				"seed {seed}: {} keeps {stored:?}, and at most {allowed} are needed; written: \
				 {versions:?}",
				String::from_utf8_lossy(key),
			);
		}
	}
	crash(&tree);
}

#[test(tokio::test)]
async fn the_versions_of_random_sequences_of_writes_deletes_flushes_compactions_and_restarts_match_the_model(
) {
	// With and without the value log, a retention, and a tree of two levels: the first eight
	// seeds are every combination, and two more are a retention on a tree of two levels, which is
	// where a compaction reclaims all it can
	for seed in (1u64..=8).chain([14, 15]) {
		random_run(seed, 100, setup_of(seed)).await;
	}
}

/// One sequence of the long run, to read a failure of it by: `MODEL_SEED=7 MODEL_TRACE=1`.
#[test(tokio::test)]
#[ignore = "run on a seed of the long run"]
async fn one_random_sequence_matches_the_model() {
	let seed: u64 = std::env::var("MODEL_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
	let steps: usize =
		std::env::var("MODEL_STEPS").ok().and_then(|s| s.parse().ok()).unwrap_or(150);
	random_run(seed, steps, setup_of(seed)).await;
}

/// The same with far more sequences, for a deliberate run: `cargo test -- --ignored`.
#[test(tokio::test)]
#[ignore = "a long randomised run"]
async fn many_random_sequences_match_the_model() {
	for seed in 1u64..=300 {
		random_run(seed, 150, setup_of(seed)).await;
	}
}
