//! The rewrite of the newest segment at open: the segment is moved to the number that follows it,
//! and the commits go on in it.
//!
//! The move is one rename, so no state holds the records of a segment under two names. Each batch
//! that an open replays is counted once by the replay, and the count is checked in these tests.

use std::path::{Path, PathBuf};

use tempfile::TempDir;
use test_log::test;

use super::fs_engine_tests::tables_per_level;
use crate::{Durability, LSMIterator, Mode, Options, Tree, TreeBuilder};

/// Commits in the session that is abandoned: one batch each, so that the replay counts them.
const COMMITS: usize = 10;

fn options(path: &Path, vlog: bool) -> Options {
	Options {
		path: path.to_path_buf(),
		enable_vlog: vlog,
		vlog_value_threshold: 256,
		..Options::default()
	}
}

fn key(i: usize) -> Vec<u8> {
	format!("key-{i:03}").into_bytes()
}

/// A value above the threshold of the value log when there is one.
fn value(i: usize, vlog: bool) -> Vec<u8> {
	let mut value = format!("value-{i:03}:").into_bytes();
	value.resize(
		if vlog {
			600
		} else {
			48
		},
		b'v',
	);
	value
}

async fn reopen(path: &Path, vlog: bool) -> Tree {
	TreeBuilder::with_options(options(path, vlog)).build_async().await.expect("the database opens")
}

/// A session that commits `COMMITS` keys, each in an `Immediate` transaction of its own, and ends
/// without a close.
async fn abandoned_session(path: &Path, vlog: bool) {
	let tree = reopen(path, vlog).await;
	for i in 0..COMMITS {
		let mut txn = tree.begin().expect("a transaction begins");
		txn.set_durability(Durability::Immediate);
		txn.set(key(i), value(i, vlog)).expect("a write is buffered");
		txn.commit().await.expect("the commit is acknowledged");
	}
	tree.abandon();
	drop(tree);
}

/// The segments of the log in `path`, by number, and the path of the newest.
fn segments(path: &Path) -> Vec<(u64, PathBuf)> {
	let mut found: Vec<(u64, PathBuf)> = super::sfs::read_dir(path.join("wal"))
		.expect("the log directory is there")
		.flatten()
		.filter_map(|entry| {
			let name = entry.file_name().into_string().ok()?;
			let id = name.strip_suffix(".wal")?.parse::<u64>().ok()?;
			Some((id, entry.path()))
		})
		.collect();
	found.sort();
	found
}

/// The newest segment of the log in `path`.
fn newest(path: &Path) -> (u64, PathBuf) {
	segments(path).pop().expect("a segment of the log")
}

/// Asserts that the tree holds exactly the `COMMITS` keys of the abandoned session, with their
/// values, and no other key: each one once.
async fn assert_each_commit_once(tree: &Tree, vlog: bool, what: &str) {
	let txn = tree.begin_with_mode(Mode::ReadOnly).expect("a snapshot begins");
	let mut iter = txn.iter().expect("an iterator");
	let mut seen = 0;
	let mut valid = iter.seek_first().expect("a first entry");
	while valid {
		seen += 1;
		valid = iter.next().expect("the next entry");
	}
	assert_eq!(seen, COMMITS, "{what}: the entries of the tree");
	for i in 0..COMMITS {
		let got = txn.get(key(i)).expect("a read answers");
		assert_eq!(got, Some(value(i, vlog)), "{what}: key {i}");
	}
}

#[test(tokio::test)]
async fn the_newest_segment_is_rewritten_into_the_next_one_at_open() {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("db");
	abandoned_session(&path, false).await;
	let (before, _) = newest(&path);

	let tree = reopen(&path, false).await;
	let report = tree.recovery_report();
	assert_eq!(report.records_replayed, COMMITS, "each batch of the session is replayed");
	assert_eq!(tables_per_level(&tree).iter().sum::<usize>(), 0, "no table is made at open");
	let after = segments(&path);
	assert_eq!(
		after.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
		vec![before + 1],
		"the open leaves the next segment alone, and the one it replayed is removed"
	);
	assert_each_commit_once(&tree, false, "after the rewrite").await;
	tree.close().await.expect("the database closes");
}

#[test(tokio::test)]
async fn commits_after_the_rewrite_are_replayed_once_in_each_later_open() {
	for vlog in [false, true] {
		let dir = TempDir::new().unwrap();
		let path = dir.path().join("db");
		abandoned_session(&path, vlog).await;

		let tree = reopen(&path, vlog).await;
		assert_eq!(tree.recovery_report().records_replayed, COMMITS, "vlog={vlog}");
		for i in COMMITS..COMMITS + 3 {
			let mut txn = tree.begin().expect("a transaction begins");
			txn.set_durability(Durability::Immediate);
			txn.set(key(i), value(i, vlog)).expect("a write is buffered");
			txn.commit().await.expect("the commit is acknowledged");
		}
		tree.abandon();
		drop(tree);

		let tree = reopen(&path, vlog).await;
		assert_eq!(
			tree.recovery_report().records_replayed,
			COMMITS + 3,
			"vlog={vlog}: the commits of the session and the ones after the rewrite, each once"
		);
		let txn = tree.begin_with_mode(Mode::ReadOnly).expect("a snapshot begins");
		for i in 0..COMMITS + 3 {
			assert_eq!(txn.get(key(i)).expect("a read answers"), Some(value(i, vlog)), "key {i}");
		}
		drop(txn);
		tree.close().await.expect("the database closes");
	}
}

#[test(tokio::test)]
async fn sessions_that_end_without_a_close_leave_one_segment_and_no_table_at_open() {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("db");
	for _ in 0..5 {
		abandoned_session(&path, true).await;
		let tree = reopen(&path, true).await;
		assert_eq!(segments(&path).len(), 1, "one segment after each open");
		assert_eq!(tables_per_level(&tree).iter().sum::<usize>(), 0, "no table after each open");
		tree.abandon();
		drop(tree);
	}
}
