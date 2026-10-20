//! Sync marks (`RecordType::SyncMark`): what the WAL says of how far a segment was synced, and the
//! damage that recovery cuts or refuses by them.
//!
//! A power cut loses any of the pages of a segment that were written but not synced, in any order,
//! so it can zero an unsynced page and keep a later one: damage with intact records after it. Such
//! damage is cut when the marks say that it was never synced, and refused when they say it was, or
//! when the segment holds no marks.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tempfile::TempDir;

use super::{sfs as fs, tfs};
use crate::wal::manager::Wal;
use crate::wal::reader::Reader;
use crate::wal::recovery::{scan_after_in, AfterDamage};
use crate::wal::{BufferedFileWriter, WritableFile, BLOCK_SIZE};
use crate::{Durability, Options, Tree};

const VALUE: [u8; 64] = [b'v'; 64];

fn opts(path: &Path, marks: bool) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		enable_vlog: false,
		flush_on_close: false,
		wal_sync_marks: marks,
		..Default::default()
	})
}

fn segment_path(dir: &Path, id: u64) -> PathBuf {
	dir.join(format!("{id:020}.wal"))
}

/// The segment the tree's commits are going to.
fn active_segment(tree: &Tree, o: &Options) -> PathBuf {
	segment_path(&o.wal_dir(), tree.core.inner.wal.read().get_active_log_number())
}

/// Simulates a kill: releases the lock file and drops the tree without `close()`. Stays on a
/// current-thread runtime with no `.await` before the next open, so the best-effort close that
/// `Drop` spawns is never polled.
fn crash(tree: Tree) {
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	drop(tree);
}

async fn put(tree: &Tree, key: &str, durability: Durability) {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(durability);
	txn.set(key.as_bytes(), &VALUE[..]).unwrap();
	txn.commit().await.unwrap();
}

fn get(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	tree.begin().unwrap().get(key.as_bytes()).unwrap()
}

/// What recovery's scan finds in the segment at `path` from `from` on, in the file system of the
/// test.
fn scan(path: &Path, from: u64) -> AfterDamage {
	let fs = tfs::ambient().unwrap_or_else(crate::vfs::default_fs);
	scan_after_in(&*fs, path, from).unwrap()
}

fn len(path: &Path) -> u64 {
	fs::metadata(path).unwrap().len()
}

/// Overwrites `range` of the file at `path` with zeros, as a power cut that lost those pages does.
fn zero(path: &Path, range: std::ops::Range<u64>) {
	let mut bytes = fs::read(path).unwrap();
	bytes[range.start as usize..range.end as usize].fill(0);
	fs::write(path, bytes).unwrap();
}

/// Five commits that are synced, then three that are not, and the segment with the offsets of the
/// first unsynced commit and of the one after it. With marks on, the first unsynced commit is the
/// first append after the last fsync, so it starts with a mark of the synced length.
async fn synced_then_unsynced(o: &Arc<Options>) -> (PathBuf, u64, u64) {
	let tree = Tree::new(Arc::clone(o)).unwrap();
	let segment = active_segment(&tree, o);
	for i in 0..5 {
		put(&tree, &format!("acked{i}"), Durability::Immediate).await;
	}
	let synced = len(&segment);
	put(&tree, "unsynced0", Durability::Eventual).await;
	let second = len(&segment);
	put(&tree, "unsynced1", Durability::Eventual).await;
	put(&tree, "unsynced2", Durability::Eventual).await;
	crash(tree);
	(segment, synced, second)
}

/// The shape of the crash that garland found: the last fsync of the segment covered its first
/// bytes, and the power cut lost the page after them but kept a later one, so an intact record
/// follows damage. No mark after the damage claims that it was synced, so it is cut, and every
/// commit that was acknowledged as durable survives.
#[test_log::test(tokio::test)]
async fn damage_in_the_unsynced_tail_that_intact_records_follow_is_cut() {
	let dir = TempDir::new().unwrap();
	let o = opts(dir.path(), true);
	let (segment, synced, second) = synced_then_unsynced(&o).await;
	let before = scan(&segment, 0);
	assert_eq!(before.highest_mark, Some(synced), "the first unsynced commit starts with a mark");
	zero(&segment, synced..second);
	let after = scan(&segment, second);
	assert_eq!(after.intact_records, 2, "intact records follow the damage");
	assert_eq!(after.highest_mark, None, "and none of them is a mark");

	let tree = Tree::new(Arc::clone(&o)).unwrap();
	for i in 0..5 {
		assert_eq!(get(&tree, &format!("acked{i}")).as_deref(), Some(&VALUE[..]), "acked{i}");
	}
	for i in 0..3 {
		assert_eq!(get(&tree, &format!("unsynced{i}")), None, "unsynced{i} was cut");
	}
	put(&tree, "later", Durability::Immediate).await;
	crash(tree);
	let tree = Tree::new(o).unwrap();
	assert_eq!(get(&tree, "later").as_deref(), Some(&VALUE[..]));
	assert_eq!(get(&tree, "acked4").as_deref(), Some(&VALUE[..]));
	tree.close().await.unwrap();
}

/// A segment without marks, as a binary that writes none leaves it, with the same damage: nothing
/// says the damage was not synced, so the open refuses it, as it always did.
#[test_log::test(tokio::test)]
async fn the_same_damage_in_a_segment_without_marks_is_refused() {
	let dir = TempDir::new().unwrap();
	let (segment, synced, second) = synced_then_unsynced(&opts(dir.path(), false)).await;
	assert_eq!(scan(&segment, 0).highest_mark, None);
	zero(&segment, synced..second);

	let err = Tree::new(opts(dir.path(), true)).err().expect("the damage must be refused");
	assert!(err.to_string().contains("interior corruption"), "{err}");
}

/// Damage to bytes that a later mark says were synced is not what a power cut leaves, and is
/// refused although intact records follow it.
#[test_log::test(tokio::test)]
async fn damage_to_bytes_a_later_mark_says_were_synced_is_refused() {
	let dir = TempDir::new().unwrap();
	let o = opts(dir.path(), true);
	let tree = Tree::new(Arc::clone(&o)).unwrap();
	let segment = active_segment(&tree, &o);
	let mut ends = Vec::new();
	for i in 0..5 {
		put(&tree, &format!("acked{i}"), Durability::Immediate).await;
		ends.push(len(&segment));
	}
	crash(tree);

	// A byte inside the payload of the third commit, which the marks of the later ones cover.
	let mut bytes = fs::read(&segment).unwrap();
	bytes[ends[2] as usize - 5] ^= 0xff;
	fs::write(&segment, bytes).unwrap();
	let marks = scan(&segment, ends[2]).highest_mark;
	assert!(marks.is_some_and(|mark| mark >= ends[2]), "a later mark covers the damage: {marks:?}");

	let err = Tree::new(o).err().expect("synced damage must be refused");
	assert!(err.to_string().contains("interior corruption"), "{err}");
}

fn records(sizes: &[usize], fill: u8) -> (Vec<u8>, Vec<usize>) {
	let mut buf = Vec::new();
	let mut ends = Vec::new();
	for size in sizes {
		buf.extend(std::iter::repeat_n(fill, *size));
		ends.push(buf.len());
	}
	(buf, ends)
}

fn read_records(path: &Path) -> Vec<Vec<u8>> {
	let mut reader = Reader::new(fs::File::open(path).unwrap());
	let mut out = Vec::new();
	while let Ok((record, _)) = reader.read() {
		out.push(record.to_vec());
	}
	out
}

/// A segment holds no mark until an fsync completes, then one before the next append, of the
/// length that fsync covered, and no other until the synced length moves again. The reader returns
/// the records as if there were none.
#[test]
fn a_mark_follows_each_fsync_and_the_reader_skips_it() {
	let dir = TempDir::new().unwrap();
	let path = segment_path(dir.path(), 1);
	let mut wal =
		Wal::open_with_min_log_number(dir.path(), 1, tfs::wal_options_for(dir.path())).unwrap();

	let (a, a_ends) = records(&[100, 200], b'a');
	wal.append_group(&a, &a_ends).unwrap();
	assert_eq!(scan(&path, 0).highest_mark, None, "nothing synced");
	let len_a = len(&path);
	assert_eq!(len_a, 2 * 7 + 300, "two records and no mark");

	wal.sync().unwrap();
	let (b, b_ends) = records(&[BLOCK_SIZE + 10], b'b');
	wal.append_group(&b, &b_ends).unwrap();
	assert_eq!(scan(&path, 0).highest_mark, Some(len_a));
	assert_eq!(scan(&path, len_a).highest_mark, Some(len_a));

	let len_b = len(&path);
	let (c, c_ends) = records(&[50], b'c');
	wal.append_group(&c, &c_ends).unwrap();
	assert_eq!(len(&path), len_b + 7 + 50, "no fsync since the last mark, so no new one");

	wal.sync().unwrap();
	let synced = len(&path);
	wal.append_group(&c, &c_ends).unwrap();
	assert_eq!(scan(&path, len_b).highest_mark, Some(synced));
	wal.sync().unwrap();
	wal.close().unwrap();

	let read = read_records(&path);
	let want: Vec<Vec<u8>> = [(&a, &a_ends), (&b, &b_ends), (&c, &c_ends), (&c, &c_ends)]
		.iter()
		.flat_map(|(buf, ends)| {
			let mut start = 0;
			ends.iter().map(move |&end| {
				let record = buf[start..end].to_vec();
				start = end;
				record
			})
		})
		.collect();
	assert_eq!(read, want);
}

/// The synced length that a mark is written with is the one the last fsync to complete covered:
/// while an fsync is in flight it is still the length before, and once it completes it is the
/// length the file had when it began.
#[test]
fn the_synced_length_moves_only_once_an_fsync_completes() {
	let dir = TempDir::new().unwrap();
	let path = segment_path(dir.path(), 1);
	let file = fs::File::create(&path).unwrap();
	let mut writer = BufferedFileWriter::new(file, BLOCK_SIZE);
	let gate = writer.gate();
	let seen = Arc::new(Mutex::new(Vec::new()));
	{
		let (gate, seen) = (Arc::clone(&gate), Arc::clone(&seen));
		writer
			.gate()
			.set_observer(Some(Arc::new(move || seen.lock().unwrap().push(gate.synced()))));
	}

	writer.append(&[1; 100]).unwrap();
	writer.sync().unwrap();
	assert_eq!(gate.synced(), 100);
	writer.append(&[2; 50]).unwrap();
	writer.sync().unwrap();
	assert_eq!(gate.synced(), 150);
	assert_eq!(*seen.lock().unwrap(), vec![0, 100], "inside each fsync, the length before it");
}

/// A mark that does not fit in what is left of a block goes to the start of the next one, after
/// zeros that the readers take for the padding of the block.
#[test]
fn a_mark_that_does_not_fit_the_block_starts_the_next_one() {
	let dir = TempDir::new().unwrap();
	let path = segment_path(dir.path(), 1);
	let mut wal =
		Wal::open_with_min_log_number(dir.path(), 1, tfs::wal_options_for(dir.path())).unwrap();

	// One record that leaves 10 bytes of its block: room for a header, not for a mark.
	let (a, a_ends) = records(&[BLOCK_SIZE - 7 - 10], b'a');
	wal.append_group(&a, &a_ends).unwrap();
	assert_eq!(len(&path), (BLOCK_SIZE - 10) as u64);
	wal.sync().unwrap();
	let (b, b_ends) = records(&[30], b'b');
	wal.append_group(&b, &b_ends).unwrap();
	wal.sync().unwrap();
	wal.close().unwrap();

	assert_eq!(len(&path), (BLOCK_SIZE + 7 + 8 + 7 + 30) as u64);
	assert_eq!(scan(&path, BLOCK_SIZE as u64).highest_mark, Some((BLOCK_SIZE - 10) as u64));
	assert_eq!(read_records(&path), vec![a, b]);
}
