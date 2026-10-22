//! The rewrite of the newest segment at open: the segment is moved to the number that follows it,
//! and the commits go on in it.
//!
//! The move is one rename, so no state holds the records of a segment under two names. Each batch
//! that an open replays is counted once by the replay, and the count is checked in these tests.

use std::path::{Path, PathBuf};

use tempfile::TempDir;
use test_log::test;

use super::fs_engine_tests::tables_per_level;
use crate::wal::manager::tail_is_marks_and_padding;
use crate::wal::recovery::sync_mark_in;
use crate::wal::{RecordType, BLOCK_SIZE, HEADER_SIZE, SYNC_MARK_LEN};
use crate::{Durability, FileSystem, LSMIterator, Mode, Options, Tree, TreeBuilder};

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

/// A session that commits `COMMITS - 1` keys `Immediate` and then one key `Eventual`, and ends
/// without a close. Returns the length of the newest segment before the last commit.
async fn abandoned_session_ending_in_an_unsynced_commit(path: &Path, vlog: bool) -> u64 {
	let tree = reopen(path, vlog).await;
	let mut before_last = 0;
	for i in 0..COMMITS {
		if i == COMMITS - 1 {
			before_last = super::sfs::metadata(newest(path).1).expect("the segment").len();
		}
		let durability = if i == COMMITS - 1 {
			Durability::Eventual
		} else {
			Durability::Immediate
		};
		let mut txn = tree.begin().expect("a transaction begins");
		txn.set_durability(durability);
		txn.set(key(i), value(i, vlog)).expect("a write is buffered");
		txn.commit().await.expect("the commit is acknowledged");
	}
	tree.abandon();
	drop(tree);
	before_last
}

/// Cuts the newest segment to the end of its last sync mark, so that the record after the mark is
/// not in the file: the state of a session that ended while its last commit was being written,
/// with the mark on the disk and the record not. Returns the number of the segment and the offset
/// of the mark.
fn cut_to_last_mark(path: &Path) -> (u64, u64) {
	let (id, file) = newest(path);
	let data = super::sfs::read(&file).expect("the segment reads");
	let mark = (0..data.len())
		.rev()
		.find(|pos| sync_mark_in(&data, 0, *pos).is_some())
		.expect("the segment holds a sync mark");
	let end = mark + HEADER_SIZE + SYNC_MARK_LEN;
	assert!(end < data.len(), "a record follows the last mark");
	let cut = super::sfs::OpenOptions::new().write(true).open(&file).expect("the segment opens");
	cut.set_len(end as u64).expect("the segment is cut");
	cut.sync_all().expect("the cut is synced");
	(id, mark as u64)
}

/// A session can end while its last commit is written: its sync mark is on the disk and the record
/// is not, which the replay reads as the end of the log without an error. The open moves the
/// segment with its whole records, as it does for a session that ends at a record boundary.
#[test(tokio::test)]
async fn a_sync_mark_without_its_record_is_moved_with_the_segment_at_open() {
	for vlog in [false, true] {
		let dir = TempDir::new().unwrap();
		let path = dir.path().join("db");
		let before_last = abandoned_session_ending_in_an_unsynced_commit(&path, vlog).await;
		let (before, mark) = cut_to_last_mark(&path);
		assert!(mark >= before_last, "vlog={vlog}: the mark is the one before the last commit");

		let tree = reopen(&path, vlog).await;
		assert_eq!(
			tree.recovery_report().records_replayed,
			COMMITS - 1,
			"vlog={vlog}: the batches of the commits that are whole are replayed"
		);
		assert_eq!(
			segments(&path).iter().map(|(id, _)| *id).collect::<Vec<_>>(),
			vec![before + 1],
			"vlog={vlog}: the segment is moved, and no segment starts beside it"
		);
		assert_eq!(tables_per_level(&tree).iter().sum::<usize>(), 0, "vlog={vlog}: no table");
		let txn = tree.begin_with_mode(Mode::ReadOnly).expect("a snapshot begins");
		for i in 0..COMMITS - 1 {
			let got = txn.get(key(i)).expect("a read answers");
			assert_eq!(got, Some(value(i, vlog)), "vlog={vlog}: key {i}");
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

/// A sync mark of the writer, which records that `synced` bytes of the segment were synced.
fn sync_mark(synced: u64) -> Vec<u8> {
	let payload = synced.to_be_bytes();
	let crc = crate::wal::calculate_crc32(&[RecordType::SyncMark as u8], &payload);
	let mut mark = Vec::with_capacity(HEADER_SIZE + SYNC_MARK_LEN);
	mark.extend_from_slice(&crc.to_be_bytes());
	mark.extend_from_slice(&(SYNC_MARK_LEN as u16).to_be_bytes());
	mark.push(RecordType::SyncMark as u8);
	mark.extend_from_slice(&payload);
	mark
}

/// The tail that the rewrite at open accepts: sync marks and the zeros that pad a block, and
/// nothing else. A mark is accepted as the reader accepts one, in its block, with its checksum and
/// its length.
#[test]
fn a_tail_is_moved_only_past_sync_marks_and_block_padding() {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("tail");
	let fs = FileSystem::std();
	let check = |bytes: &[u8], from: usize| {
		std::fs::write(&path, bytes).unwrap();
		let file = fs.0.open_read(&path).unwrap();
		tail_is_marks_and_padding(&*file, from as u64, bytes.len() as u64).unwrap()
	};

	let mut bad_crc = sync_mark(0);
	bad_crc[0] ^= 0xff;
	let mut bad_length = sync_mark(0);
	bad_length[5] = SYNC_MARK_LEN as u8 + 1;
	let mut padded = vec![0u8; BLOCK_SIZE];
	padded.extend(sync_mark(0));
	let mut straddling = vec![0u8; BLOCK_SIZE - 7];
	straddling.extend(sync_mark(0));

	let cases: Vec<(&str, Vec<u8>, usize, bool)> = vec![
		("no tail", vec![], 0, true),
		("a mark", sync_mark(0), 0, true),
		("two marks", [sync_mark(5), sync_mark(9)].concat(), 0, true),
		("fewer zeros than a mark", vec![0; 10], 0, true),
		("zeros to the end of the tail", vec![0; 100], 0, true),
		("padding of a block, then a mark in the next", padded, BLOCK_SIZE - 10, true),
		("a mark that straddles a block end", straddling, BLOCK_SIZE - 7, false),
		("a mark with a bad checksum", bad_crc, 0, false),
		("a mark with another length", bad_length, 0, false),
		("a mark and a byte after it", [sync_mark(0), vec![1]].concat(), 0, false),
		("zeros and a byte after them", vec![0, 0, 1], 0, false),
	];
	for (what, bytes, from, accepted) in cases {
		assert_eq!(check(&bytes, from), accepted, "{what}");
	}
	// A tail that reaches past the end of the file is not read as marks and padding.
	std::fs::write(&path, sync_mark(0)).unwrap();
	let file = fs.0.open_read(&path).unwrap();
	let past_end = (HEADER_SIZE + SYNC_MARK_LEN + 5) as u64;
	assert!(!tail_is_marks_and_padding(&*file, 0, past_end).unwrap(), "a tail past the end");
}
