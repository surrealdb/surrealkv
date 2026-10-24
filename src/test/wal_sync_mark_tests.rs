//! Sync marks (`RecordType::SyncMark`): what the WAL says of how far a segment was synced, and the
//! damage that recovery cuts or refuses by them.
//!
//! A power cut loses any of the pages of a segment that were written but not synced, in any order,
//! so it can zero an unsynced page and keep a later one: damage with intact records after it. Such
//! damage is cut when the marks say that it was never synced, and refused when they say it was, or
//! when the segment holds no marks (it does not start with a leading mark).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use tempfile::TempDir;

use super::format_gate_tests::{manifest_version, set_manifest_version};
use super::{sfs as fs, tfs};
use crate::levels::MANIFEST_FORMAT_VERSION_V2;
use crate::vfs::IntoFsFile;
use crate::wal::manager::Wal;
use crate::wal::reader::Reader;
use crate::wal::recovery::{scan_after_in, segment_holds_marks_in, AfterDamage};
use crate::wal::{
	calculate_crc32,
	BufferedFileWriter,
	CompressionType,
	RecordType,
	WritableFile,
	BLOCK_SIZE,
	HEADER_SIZE,
	SYNC_MARK_LEN,
};
use crate::{Durability, Options, Tree, WalRecoveryMode};

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

/// The length of a sync mark on disk, its header included.
const MARK: u64 = (HEADER_SIZE + SYNC_MARK_LEN) as u64;

/// The file system of the test.
fn test_fs() -> Arc<dyn crate::vfs::Fs> {
	tfs::ambient().unwrap_or_else(crate::vfs::default_fs)
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
	put_value(tree, key, &VALUE, durability).await;
}

async fn put_value(tree: &Tree, key: &str, value: &[u8], durability: Durability) {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(durability);
	txn.set(key.as_bytes(), value).unwrap();
	txn.commit().await.unwrap();
}

fn get(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	tree.begin().unwrap().get(key.as_bytes()).unwrap()
}

/// What recovery's scan finds in the segment at `path` from `from` on, in the file system of the
/// test.
fn scan(path: &Path, from: u64) -> AfterDamage {
	scan_after_in(&*test_fs(), path, from).unwrap()
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

/// A new segment starts with its leading mark, of nothing synced. Then a mark follows each fsync,
/// before the next append, of the length that fsync covered, and no other until the synced length
/// moves again, and a close ends the segment in a trailing mark. The reader returns the records as
/// if there were none.
#[test]
fn a_segment_starts_with_a_mark_and_a_mark_follows_each_fsync() {
	let dir = TempDir::new().unwrap();
	let path = segment_path(dir.path(), 1);
	let mut wal =
		Wal::open_with_min_log_number(dir.path(), 1, tfs::wal_options_for(dir.path())).unwrap();

	let (a, a_ends) = records(&[100, 200], b'a');
	wal.append_group(&a, &a_ends).unwrap();
	assert_eq!(scan(&path, 0).highest_mark, Some(0), "the leading mark, of nothing synced");
	assert!(segment_holds_marks_in(&*test_fs(), &path).unwrap());
	let len_a = len(&path);
	assert_eq!(len_a, MARK + 2 * 7 + 300, "the leading mark and two records");

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
	let before_close = len(&path);
	wal.close().unwrap();
	assert_eq!(len(&path), before_close + MARK, "a trailing mark");
	assert_eq!(scan(&path, before_close).highest_mark, Some(before_close));

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

	// After the leading mark, one record that leaves 10 bytes of its block: room for a header,
	// not for a mark.
	let (a, a_ends) = records(&[BLOCK_SIZE - MARK as usize - 7 - 10], b'a');
	wal.append_group(&a, &a_ends).unwrap();
	assert_eq!(len(&path), (BLOCK_SIZE - 10) as u64);
	wal.sync().unwrap();
	let (b, b_ends) = records(&[30], b'b');
	wal.append_group(&b, &b_ends).unwrap();
	wal.sync().unwrap();

	assert_eq!(len(&path), (BLOCK_SIZE + 7 + 8 + 7 + 30) as u64);
	assert_eq!(scan(&path, BLOCK_SIZE as u64).highest_mark, Some((BLOCK_SIZE - 10) as u64));
	wal.close().unwrap();
	assert_eq!(read_records(&path), vec![a, b]);
}

/// The bytes of a sync mark that claims `synced`, as a writer frames one.
fn mark_bytes(synced: u64) -> Vec<u8> {
	let payload = synced.to_be_bytes();
	let mut bytes = calculate_crc32(&[RecordType::SyncMark as u8], &payload).to_be_bytes().to_vec();
	bytes.extend_from_slice(&(SYNC_MARK_LEN as u16).to_be_bytes());
	bytes.push(RecordType::SyncMark as u8);
	bytes.extend_from_slice(&payload);
	bytes
}

/// A value that holds the bytes of a sync mark that claims `synced`, inside other bytes.
fn value_with_a_mark(synced: u64) -> Vec<u8> {
	let mut value = vec![b'x'; 32];
	value.extend(mark_bytes(synced));
	value.extend([b'y'; 32]);
	value
}

/// A segment whose commits were never synced (the default durability) still starts with its
/// leading mark, so damage in it that intact records follow, which only a power cut to unsynced
/// bytes leaves, is cut.
#[test_log::test(tokio::test)]
async fn damage_in_a_segment_that_was_never_synced_is_cut() {
	let dir = TempDir::new().unwrap();
	let o = opts(dir.path(), true);
	let tree = Tree::new(Arc::clone(&o)).unwrap();
	let segment = active_segment(&tree, &o);
	let mut ends = Vec::new();
	for i in 0..6 {
		put(&tree, &format!("k{i}"), Durability::Eventual).await;
		ends.push(len(&segment));
	}
	crash(tree);
	assert!(segment_holds_marks_in(&*test_fs(), &segment).unwrap());
	assert_eq!(scan(&segment, ends[0]).highest_mark, None, "no fsync, no mark after the first");

	zero(&segment, ends[2]..ends[3]);
	let tree = Tree::new(Arc::clone(&o)).unwrap();
	for i in 0..3 {
		assert_eq!(get(&tree, &format!("k{i}")).as_deref(), Some(&VALUE[..]), "k{i}");
	}
	for i in 3..6 {
		assert_eq!(get(&tree, &format!("k{i}")), None, "k{i} was cut");
	}
	tree.close().await.unwrap();
}

/// The bytes of a mark inside a value are not taken for a mark, even where recovery steps through
/// the value byte by byte because its record is damaged: a mark that a writer framed is followed by
/// a record. Here such bytes claim that the damaged record was synced, which would have the open
/// refuse damage that the real marks say was never synced.
#[test_log::test(tokio::test)]
async fn the_bytes_of_a_mark_inside_a_value_are_not_taken_for_one() {
	let dir = TempDir::new().unwrap();
	let o = opts(dir.path(), true);
	let tree = Tree::new(Arc::clone(&o)).unwrap();
	let segment = active_segment(&tree, &o);
	put(&tree, "acked", Durability::Immediate).await;
	let synced = len(&segment);
	// The next append starts with a mark of `synced`, and the record after it claims a length
	// past `synced`, which is where recovery finds the damage.
	put_value(&tree, "forged", &value_with_a_mark(synced + 1), Durability::Eventual).await;
	let forged_end = len(&segment);
	put(&tree, "later", Durability::Eventual).await;
	crash(tree);

	let mut bytes = fs::read(&segment).unwrap();
	bytes[forged_end as usize - 1] ^= 0xff;
	fs::write(&segment, bytes).unwrap();
	// The real mark in front of the damaged record is not followed by a record either, and claims
	// no more than the damage's offset anyway.
	assert_eq!(scan(&segment, synced).highest_mark, None, "no mark counts");

	let tree = Tree::new(Arc::clone(&o)).unwrap();
	assert_eq!(get(&tree, "acked").as_deref(), Some(&VALUE[..]));
	assert_eq!(get(&tree, "forged"), None);
	assert_eq!(get(&tree, "later"), None);
	tree.close().await.unwrap();
}

/// A segment without marks stays one whatever its values hold: the bytes of a mark inside a damaged
/// record do not make recovery read it as a segment with marks and cut the records after the
/// damage.
#[test_log::test(tokio::test)]
async fn the_bytes_of_a_mark_inside_a_value_do_not_give_a_segment_marks() {
	let dir = TempDir::new().unwrap();
	let unmarked = opts(dir.path(), false);
	let tree = Tree::new(Arc::clone(&unmarked)).unwrap();
	let segment = active_segment(&tree, &unmarked);
	put(&tree, "acked0", Durability::Immediate).await;
	put_value(&tree, "forged", &value_with_a_mark(0), Durability::Immediate).await;
	let forged_end = len(&segment);
	put(&tree, "acked2", Durability::Immediate).await;
	crash(tree);

	let mut bytes = fs::read(&segment).unwrap();
	bytes[forged_end as usize - 1] ^= 0xff;
	fs::write(&segment, bytes).unwrap();
	assert!(!segment_holds_marks_in(&*test_fs(), &segment).unwrap());

	let err = Tree::new(opts(dir.path(), true)).err().expect("the damage must be refused");
	assert!(err.to_string().contains("interior corruption"), "{err}");
}

/// The synced length that an fsync records is the end of the last append that had reached the
/// file whole when it began. An fsync outside the WAL lock can begin while an append longer than
/// the buffer is being written, with part of it in the file: that part is not counted.
#[test]
fn the_synced_length_is_the_end_of_the_last_whole_append() {
	let dir = TempDir::new().unwrap();
	let path = segment_path(dir.path(), 1);
	let file = fs::File::create(&path).unwrap().into_fs_file();
	let mut writer = BufferedFileWriter::new(Arc::clone(&file), BLOCK_SIZE);
	let gate = writer.gate();

	writer.append(&[1; 100]).unwrap();
	writer.flush().unwrap();
	writer.append(&[2; 2 * BLOCK_SIZE]).unwrap();
	assert_eq!(file.len().unwrap(), 100 + 2 * BLOCK_SIZE as u64, "part of the append is out");

	gate.sync(&*file).unwrap();
	assert_eq!(gate.synced(), 100, "only the append that is whole");
	writer.flush().unwrap();
	gate.sync(&*file).unwrap();
	assert_eq!(gate.synced(), 100 + 2 * BLOCK_SIZE as u64);
}

/// A segment that was closed ends in a trailing mark of its whole length, so damage anywhere in it
/// is refused, whether records follow it or not: a power cut does not undo the fsync of a close.
#[test_log::test(tokio::test)]
async fn damage_in_a_closed_segment_is_refused() {
	for (name, damaged) in [("records follow", 1), ("nothing follows", 4)] {
		let dir = TempDir::new().unwrap();
		let o = opts(dir.path(), true);
		let tree = Tree::new(Arc::clone(&o)).unwrap();
		let segment = active_segment(&tree, &o);
		let mut ends = Vec::new();
		for i in 0..5 {
			put(&tree, &format!("k{i}"), Durability::Eventual).await;
			ends.push(len(&segment));
		}
		tree.close().await.unwrap();
		let closed = len(&segment);
		assert_eq!(closed, ends[4] + MARK, "{name}: a trailing mark");
		assert_eq!(scan(&segment, ends[4]).highest_mark, Some(ends[4]), "{name}");

		zero(&segment, ends[damaged - 1]..ends[damaged]);
		let err = Tree::new(Arc::clone(&o)).err().expect("closed damage must be refused");
		assert!(err.to_string().contains("interior corruption"), "{name}: {err}");
	}
}

/// A compressed segment holds the record of its compression first and its leading mark after it,
/// and its records read back as they were written.
#[test]
fn a_compressed_segment_starts_with_its_compression_and_then_its_leading_mark() {
	let dir = TempDir::new().unwrap();
	let path = segment_path(dir.path(), 1);
	let options = tfs::wal_options_for(dir.path()).with_compression(CompressionType::Lz4);
	let mut wal = Wal::open_with_min_log_number(dir.path(), 1, options).unwrap();

	let (a, a_ends) = records(&[100, 3_000], b'a');
	wal.append_group(&a, &a_ends).unwrap();
	assert!(segment_holds_marks_in(&*test_fs(), &path).unwrap());
	wal.sync().unwrap();
	let (b, b_ends) = records(&[BLOCK_SIZE + 10], b'b');
	wal.append_group(&b, &b_ends).unwrap();
	wal.sync().unwrap();
	let before_close = len(&path);
	wal.close().unwrap();

	assert_eq!(fs::read(&path).unwrap()[6], RecordType::SetCompressionType as u8);
	assert_eq!(scan(&path, 0).highest_mark, Some(before_close));
	let want: Vec<Vec<u8>> =
		vec![a[..a_ends[0]].to_vec(), a[a_ends[0]..a_ends[1]].to_vec(), b[..b_ends[0]].to_vec()];
	assert_eq!(read_records(&path), want);
}

/// The modes that salvage cut an unsynced tail as the default mode does, and count the intact
/// records they cut with it.
#[test_log::test(tokio::test)]
async fn salvaging_cuts_an_unsynced_tail_and_counts_its_records() {
	for mode in [WalRecoveryMode::SalvageValidPrefix, WalRecoveryMode::TolerateCorruptedWithRepair]
	{
		let dir = TempDir::new().unwrap();
		let o = Arc::new(Options {
			wal_recovery_mode: mode,
			..(*opts(dir.path(), true)).clone()
		});
		let (segment, synced, second) = synced_then_unsynced(&o).await;
		zero(&segment, synced..second);

		let tree = Tree::new(Arc::clone(&o)).unwrap();
		assert_eq!(tree.core.inner.recovery_report.read().records_dropped, 2, "{mode:?}");
		assert_eq!(get(&tree, "acked4").as_deref(), Some(&VALUE[..]), "{mode:?}");
		assert_eq!(get(&tree, "unsynced2"), None, "{mode:?}");
		tree.close().await.unwrap();
	}
}

/// An open that refuses the WAL leaves the manifest of the version it found, so that the binary
/// the directory came from still opens it: the manifest is made V3 only once the WAL is replayed.
#[test_log::test(tokio::test)]
async fn an_open_that_refuses_the_wal_leaves_the_manifest_as_it_was() {
	let dir = TempDir::new().unwrap();
	let unmarked = opts(dir.path(), false);
	let (segment, synced, second) = synced_then_unsynced(&unmarked).await;
	zero(&segment, synced..second);
	set_manifest_version(&unmarked, MANIFEST_FORMAT_VERSION_V2);

	Tree::new(opts(dir.path(), true)).err().expect("the damage must be refused");
	assert_eq!(manifest_version(&unmarked), MANIFEST_FORMAT_VERSION_V2);
}
