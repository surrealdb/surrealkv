//! Torn WAL tails across restarts.
//!
//! A kill or power loss in the middle of a WAL append leaves a partial record at the end of the
//! last segment. Recovery drops it and continues in a fresh segment, so the damaged segment is
//! never appended to again and is no longer the last one at the next restart. These tests cut the
//! last segment at points inside a record, recover, commit again, crash again and recover again,
//! and check that:
//!
//! * every commit acknowledged before the crash is present after each recovery, and the commit that
//!   was cut short never is;
//! * commits made after a recovery go to the fresh segment and are present after the next restart,
//!   although the damaged segment is by then followed by one or two others;
//! * `AbsoluteConsistency` refuses a tail the reader reports as damage and accepts one it takes for
//!   a clean end of file, at every restart.
//!
//! Each shape: 10 durable commits, one more record that is then cut short, recovery, 5 new
//! commits, a second crash, recovery, a third crash, recovery.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tempfile::TempDir;

use crate::batch::Batch;
use crate::vlog::ValueLocation;
use crate::wal::manager::Wal;
use crate::wal::parallel_recovery::decode_segment_batches;
use crate::wal::{CompressionType, Options as WalOptions, BLOCK_SIZE};
use crate::{Error, InternalKeyKind, Options, Tree, WalRecoveryMode};

const BLOCK: u64 = BLOCK_SIZE as u64;
const GOOD: [u8; 64] = [b'x'; 64];
const NEW: [u8; 64] = [b'z'; 64];

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn opts(path: &Path, mode: WalRecoveryMode) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		vlog_value_threshold: 0,
		enable_vlog: false,
		max_memtable_size: 64 * 1024 * 1024,
		flush_on_close: false,
		wal_recovery_mode: mode,
		// The multi-segment tests rotate memtables without waking the flusher, so the immutable
		// queue must be allowed to grow instead of stalling writes.
		memtable_stall_threshold: 1000,
		..Default::default()
	})
}

fn wal_path(o: &Options, id: u64) -> PathBuf {
	o.wal_dir().join(format!("{id:020}.wal"))
}

/// The segment the tree's commits are going to.
fn active_segment(tree: &Tree) -> u64 {
	tree.core.inner.wal.read().get_active_log_number()
}

/// Simulates a kill: releases the lock file and drops the tree without
/// `close()`. Must stay on a current-thread runtime with no `.await` before
/// the next open, so the best-effort close that `Drop` spawns is never polled.
fn crash(tree: Tree) {
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	drop(tree);
}

async fn put(tree: &Tree, key: &str, value: &[u8]) {
	let mut txn = tree.begin().unwrap();
	txn.set(key.as_bytes(), value).unwrap();
	txn.commit().await.unwrap();
}

fn get(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	tree.begin().unwrap().get(key.as_bytes()).unwrap()
}

fn file_len(path: &Path) -> u64 {
	fs::metadata(path).unwrap().len()
}

fn copy_dir(src: &Path, dst: &Path) {
	fs::create_dir_all(dst).unwrap();
	for entry in fs::read_dir(src).unwrap() {
		let entry = entry.unwrap();
		let to = dst.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &to);
		} else {
			fs::copy(entry.path(), &to).unwrap();
		}
	}
}

fn set_len(path: &Path, len: u64) {
	fs::OpenOptions::new().write(true).open(path).unwrap().set_len(len).unwrap();
}

/// Decodes a segment the way recovery does and fails on any damage, so a
/// misframed record cannot hide behind the repair that the next open performs.
fn strict_decode(path: &Path) -> Vec<Batch> {
	let id: u64 = path.file_stem().unwrap().to_str().unwrap().parse().unwrap();
	decode_segment_batches(path, id)
		.unwrap_or_else(|e| panic!("{} must parse end to end: {e}", path.display()))
		.1
}

fn assert_keys(tree: &Tree, prefix: &str, count: usize, value: &[u8], when: &str) {
	for i in 0..count {
		let key = format!("{prefix}{i}");
		assert_eq!(get(tree, &key).as_deref(), Some(value), "{when}: lost {key}");
	}
}

/// One WAL record for `key`, framed like the commit pipeline does.
fn wal_record(seq: u64, key: &str, value: &[u8]) -> Vec<u8> {
	let mut batch = Batch::new(seq);
	let location = ValueLocation::with_inline_value(value.to_vec()).encode();
	batch.add_record(InternalKeyKind::Set, key.as_bytes().to_vec(), Some(location), 0).unwrap();
	batch.encode().unwrap()
}

// ---------------------------------------------------------------------------
// the tear shapes
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Cut {
	/// Keep this many bytes of the torn record.
	Bytes(u64),
	/// Keep the torn record up to the end of the first block, plus this many
	/// bytes of the next one.
	Block(u64),
}

#[derive(Clone, Copy)]
struct Shape {
	/// Size of the value in the torn commit (64 B is one small record, 100 KB
	/// spans four blocks).
	value_len: usize,
	cut: Cut,
}

/// How the reader classifies the tear, which decides whether
/// `AbsoluteConsistency` refuses to open at the first recovery.
#[derive(Clone, Copy)]
enum Class {
	/// The reader reports a clean end of file: complete fragments of a record whose next
	/// fragment starts in a block that was never written.
	CleanEof,
	/// The reader reports corruption: a partial header, or an intact header whose payload is
	/// cut.
	Corruption,
}

struct Torn {
	/// The segment the commits went to and that was cut.
	segment: PathBuf,
	/// The length of the segment after the cut.
	cut_len: u64,
}

/// Writes 10 good commits and one torn commit, crashes, and cuts the segment
/// `shape.cut` bytes into the torn record.
async fn write_and_tear(o: &Arc<Options>, shape: Shape) -> Torn {
	let tree = Tree::new(Arc::clone(o)).unwrap();
	let wal = wal_path(o, active_segment(&tree));
	for i in 0..10 {
		put(&tree, &format!("good{i}"), &GOOD).await;
	}
	let good_len = file_len(&wal);
	put(&tree, "torn", &vec![b'y'; shape.value_len]).await;
	crash(tree);

	let full_len = file_len(&wal);
	let cut = match shape.cut {
		Cut::Bytes(n) => n,
		Cut::Block(n) => BLOCK - good_len + n,
	};
	assert!(good_len + cut < full_len, "cut {cut} out of range (good={good_len} full={full_len})");
	set_len(&wal, good_len + cut);
	Torn {
		segment: wal,
		cut_len: good_len + cut,
	}
}

/// Opens, checks what recovery kept, commits 5 more keys under `new`, and crashes. Returns the
/// segment those commits went to and its length.
async fn recover_and_commit(o: &Arc<Options>, when: &str, torn: &Torn) -> (u64, u64) {
	let tree = Tree::new(Arc::clone(o)).unwrap_or_else(|e| panic!("{when} must open: {e}"));
	assert_keys(&tree, "good", 10, &GOOD, when);
	assert!(get(&tree, "torn").is_none(), "{when} exposed the torn commit");
	// The damaged segment holds the acknowledged records and nothing after them.
	assert_eq!(strict_decode(&torn.segment).len(), 10, "{when}: the damaged segment");
	let damaged_len = file_len(&torn.segment);

	let segment = active_segment(&tree);
	let damaged: u64 = torn.segment.file_stem().unwrap().to_str().unwrap().parse().unwrap();
	assert!(segment > damaged, "{when}: commits must not go to the damaged segment");
	for i in 0..5 {
		put(&tree, &format!("new{i}"), &NEW).await;
	}
	assert_eq!(file_len(&torn.segment), damaged_len, "{when}: the damaged segment was appended to");
	let len = file_len(&wal_path(o, segment));
	assert!(len > 0, "{when}: the commits did not reach segment {segment}");
	crash(tree);
	(segment, len)
}

/// Default mode: every key must survive the tear and every later restart.
async fn tolerate_scenario(shape: Shape) {
	let dir = TempDir::new().unwrap();
	let o = opts(dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair);
	let torn = write_and_tear(&o, shape).await;

	// Recovery 1: the torn commit was never acknowledged, the rest was.
	let (segment, _) = recover_and_commit(&o, "recovery #1", &torn).await;

	// The new commits are in the fresh segment, framed cleanly.
	assert_eq!(strict_decode(&wal_path(&o, segment)).len(), 5);

	// Recovery 2: the damaged segment is no longer the last one.
	let tree = Tree::new(Arc::clone(&o)).expect("recovery #2 must open");
	assert_keys(&tree, "good", 10, &GOOD, "recovery #2");
	assert_keys(&tree, "new", 5, &NEW, "recovery #2");
	assert!(get(&tree, "torn").is_none(), "recovery #2 exposed the torn commit");
	for i in 0..5 {
		put(&tree, &format!("later{i}"), &NEW).await;
	}
	crash(tree);

	// Recovery 3.
	let tree = Tree::new(Arc::clone(&o)).expect("recovery #3 must open");
	assert_keys(&tree, "good", 10, &GOOD, "recovery #3");
	assert_keys(&tree, "new", 5, &NEW, "recovery #3");
	assert_keys(&tree, "later", 5, &NEW, "recovery #3");
	assert!(get(&tree, "torn").is_none(), "recovery #3 exposed the torn commit");
	crash(tree);
}

/// `AbsoluteConsistency` keeps refusing a tear the reader reports as
/// corruption. A tear the reader accepts as a clean end of file opens, and the
/// commits that follow it survive the next restarts.
async fn absolute_scenario(shape: Shape, class: Class) {
	let dir = TempDir::new().unwrap();
	let tolerate = opts(dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair);
	let o = opts(dir.path(), WalRecoveryMode::AbsoluteConsistency);
	let torn = write_and_tear(&tolerate, shape).await;
	let damaged: u64 = torn.segment.file_stem().unwrap().to_str().unwrap().parse().unwrap();

	match class {
		Class::Corruption => {
			// A refused open leaves its background tasks, and with them the lock file, alive
			// until the runtime ends, so the refusal is tried on a copy of the directory.
			let copy = TempDir::new().unwrap();
			copy_dir(dir.path(), copy.path());
			let refused = opts(copy.path(), WalRecoveryMode::AbsoluteConsistency);
			let refused_wal = wal_path(&refused, damaged);
			let before = fs::read(&refused_wal).unwrap();
			match Tree::new(Arc::clone(&refused)) {
				Err(Error::WalCorruption {
					segment_id,
					..
				}) if segment_id as u64 == damaged => {}
				Err(e) => panic!("expected WalCorruption in segment {damaged}, got: {e}"),
				Ok(_) => panic!("AbsoluteConsistency must refuse a corrupted tail"),
			}
			assert_eq!(
				fs::read(&refused_wal).unwrap(),
				before,
				"a refused open must not touch the WAL"
			);
			assert!(
				!refused.wal_dir().join("repair_temp").exists(),
				"a refused open must not start a repair"
			);

			// The default mode still recovers the same bytes.
			let tree = Tree::new(Arc::clone(&tolerate)).expect("default mode must recover");
			assert_keys(&tree, "good", 10, &GOOD, "default mode");
			crash(tree);
		}
		Class::CleanEof => {
			recover_and_commit(&o, "recovery #1", &torn).await;
			assert_eq!(
				file_len(&torn.segment),
				torn.cut_len,
				"a tail the reader accepts is not repaired"
			);

			// The unrepaired tail is in a segment that is no longer the last one.
			for round in 2..=3 {
				let when = format!("recovery #{round}");
				let tree = Tree::new(Arc::clone(&o)).unwrap_or_else(|e| panic!("{when}: {e}"));
				assert_keys(&tree, "good", 10, &GOOD, &when);
				assert_keys(&tree, "new", 5, &NEW, &when);
				assert!(get(&tree, "torn").is_none(), "{when} exposed the torn commit");
				put(&tree, &format!("round{round}"), &NEW).await;
				crash(tree);
			}
		}
	}
}

macro_rules! tear_shape {
	($name:ident, $value_len:expr, $cut:expr, $class:expr) => {
		mod $name {
			use super::*;

			const SHAPE: Shape = Shape {
				value_len: $value_len,
				cut: $cut,
			};

			#[test_log::test(tokio::test)]
			async fn tolerate_keeps_new_commits_across_restart() {
				tolerate_scenario(SHAPE).await;
			}

			#[test_log::test(tokio::test)]
			async fn absolute_consistency_semantics() {
				absolute_scenario(SHAPE, $class).await;
			}
		}
	};
}

// A header cut short is damage to the reader whenever the file ends in the middle of a block.
tear_shape!(partial_header_3b, 64, Cut::Bytes(3), Class::Corruption);
tear_shape!(partial_header_6b, 64, Cut::Bytes(6), Class::Corruption);
tear_shape!(header_only_7b, 64, Cut::Bytes(7), Class::Corruption);
tear_shape!(partial_payload_20b, 64, Cut::Bytes(20), Class::Corruption);
tear_shape!(large_record_partial_header_3b, 100_000, Cut::Bytes(3), Class::Corruption);
tear_shape!(large_record_partial_first_fragment, 100_000, Cut::Bytes(1000), Class::Corruption);
// The first fragment is complete and ends exactly on the block boundary: the file is a whole
// number of blocks, so nothing says that a fragment is missing.
tear_shape!(large_record_first_fragment_to_block_end, 100_000, Cut::Block(0), Class::CleanEof);
// Tears just past the block boundary: a partial and a header-only second fragment.
tear_shape!(large_record_partial_second_header, 100_000, Cut::Block(3), Class::Corruption);
tear_shape!(large_record_header_only_second_fragment, 100_000, Cut::Block(7), Class::Corruption);

// ---------------------------------------------------------------------------
// segments that are not torn
// ---------------------------------------------------------------------------

/// Value length for a commit of the key `fill`, as the first record of a fresh
/// database, that makes the segment exactly `target` bytes long. Found by
/// measuring a probe, since the record's overhead is not worth hard-coding.
async fn fill_value_len(target: u64) -> usize {
	let probe_len = 20_000usize;
	let dir = TempDir::new().unwrap();
	let o = opts(dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair);
	let tree = Tree::new(Arc::clone(&o)).unwrap();
	let wal = wal_path(&o, active_segment(&tree));
	put(&tree, "fill", &vec![b'f'; probe_len]).await;
	let overhead = file_len(&wal) - probe_len as u64;
	crash(tree);
	assert!(target > overhead + 16_384, "target is outside the probe's varint size class");
	(target - overhead) as usize
}

/// Control: a segment that ends exactly on a block boundary after a complete
/// record is not torn. Nothing is repaired, and the commits after recovery go
/// to the next segment.
#[test_log::test(tokio::test)]
async fn segment_ending_on_a_block_boundary_is_left_alone() {
	let dir = TempDir::new().unwrap();
	let o = opts(dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair);
	let fill_len = fill_value_len(BLOCK).await;

	let tree = Tree::new(Arc::clone(&o)).unwrap();
	let wal = wal_path(&o, active_segment(&tree));
	put(&tree, "fill", &vec![b'f'; fill_len]).await;
	assert_eq!(file_len(&wal), BLOCK, "calibration: the record must end on the block boundary");
	let before = fs::read(&wal).unwrap();
	crash(tree);

	let tree = Tree::new(Arc::clone(&o)).unwrap();
	assert_eq!(fs::read(&wal).unwrap(), before, "a clean segment must not be rewritten");
	assert!(!o.wal_dir().join("repair_temp").exists());
	for i in 0..5 {
		put(&tree, &format!("new{i}"), &NEW).await;
	}
	crash(tree);

	assert_eq!(strict_decode(&wal).len(), 1);
	let tree = Tree::new(Arc::clone(&o)).unwrap();
	assert_eq!(get(&tree, "fill").map(|v| v.len()), Some(fill_len));
	assert_keys(&tree, "new", 5, &NEW, "after restart");
	crash(tree);
}

/// Writes 10 good commits (one record each), crashes, and returns the segment they are in and
/// the length of one record.
async fn write_ten_good(o: &Arc<Options>) -> (PathBuf, u64) {
	let tree = Tree::new(Arc::clone(o)).unwrap();
	let wal = wal_path(o, active_segment(&tree));
	for i in 0..10 {
		put(&tree, &format!("good{i}"), &GOOD).await;
	}
	crash(tree);
	let len = file_len(&wal);
	assert_eq!(len % 10, 0, "records are not equal-sized");
	(wal, len / 10)
}

/// A power loss can leave the file extended with zeros past the last record
/// that reached the disk. The reader takes them for padding and reports a
/// clean end of file. Commits made after recovery must not follow them into the
/// same segment, where the reader would skip them as padding.
#[test_log::test(tokio::test)]
async fn zero_filled_tail_does_not_hide_later_commits() {
	// Within the block, and running over two block boundaries.
	for zeros in [100usize, 70_000] {
		for mode in
			[WalRecoveryMode::TolerateCorruptedWithRepair, WalRecoveryMode::AbsoluteConsistency]
		{
			let dir = TempDir::new().unwrap();
			let o = opts(dir.path(), mode);
			let (wal, record_len) = write_ten_good(&o).await;
			fs::OpenOptions::new()
				.append(true)
				.open(&wal)
				.unwrap()
				.write_all(&vec![0u8; zeros])
				.unwrap();

			let tree = Tree::new(Arc::clone(&o)).unwrap();
			assert_keys(&tree, "good", 10, &GOOD, "recovery #1");
			assert!(active_segment(&tree) > 1, "{zeros} zeros, {mode:?}");
			for i in 0..5 {
				put(&tree, &format!("new{i}"), &NEW).await;
			}
			crash(tree);

			assert_eq!(strict_decode(&wal).len(), 10, "{zeros} zeros, {mode:?}");
			assert!(file_len(&wal) >= record_len * 10, "{zeros} zeros, {mode:?}");
			let tree = Tree::new(Arc::clone(&o)).unwrap();
			assert_keys(&tree, "good", 10, &GOOD, "recovery #2");
			assert_keys(&tree, "new", 5, &NEW, "recovery #2");
			crash(tree);
		}
	}
}

/// A record that ends fewer than 7 bytes before a block boundary is followed by padding. A
/// crash that lets the start of the next block's header reach the disk leaves a partial header
/// after the padding. The record before the padding is kept, and commits made after recovery
/// survive, including a record that spans blocks.
#[test_log::test(tokio::test)]
async fn partial_header_after_block_padding() {
	for leftover in [1u64, 3, 6] {
		let dir = TempDir::new().unwrap();
		let o = opts(dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair);
		let target = BLOCK - leftover;
		let fill_len = fill_value_len(target).await;

		// A record that ends `leftover` bytes before the block boundary, then one
		// more whose padding and first 3 header bytes reach the disk.
		let tree = Tree::new(Arc::clone(&o)).unwrap();
		let wal = wal_path(&o, active_segment(&tree));
		put(&tree, "fill", &vec![b'f'; fill_len]).await;
		assert_eq!(file_len(&wal), target, "calibration for leftover {leftover}");
		put(&tree, "torn", &[b'y'; 64]).await;
		crash(tree);
		set_len(&wal, BLOCK + 3);

		let tree = Tree::new(Arc::clone(&o)).unwrap();
		assert_eq!(get(&tree, "fill").map(|v| v.len()), Some(fill_len));
		assert!(get(&tree, "torn").is_none());
		assert_eq!(strict_decode(&wal).len(), 1, "leftover {leftover}: only fill remains");
		for i in 0..5 {
			put(&tree, &format!("new{i}"), &NEW).await;
		}
		let big = vec![b'B'; 40_000];
		put(&tree, "big", &big).await;
		crash(tree);

		let tree = Tree::new(Arc::clone(&o)).unwrap();
		assert_eq!(get(&tree, "fill").map(|v| v.len()), Some(fill_len), "leftover {leftover}");
		assert_keys(&tree, "new", 5, &NEW, "after restart");
		assert_eq!(get(&tree, "big"), Some(big), "leftover {leftover}");
		crash(tree);
	}
}

// ---------------------------------------------------------------------------
// corruption that is not a tail
// ---------------------------------------------------------------------------

/// Flips a byte inside the payload of record `n`.
fn flip_byte_in_record(path: &Path, record_len: u64, n: u64) {
	let mut bytes = fs::read(path).unwrap();
	bytes[(record_len * n + 20) as usize] ^= 0xff;
	fs::write(path, bytes).unwrap();
}

/// Damage with valid records after it is not a tear. The default mode keeps the valid prefix and
/// continues, and what is committed afterwards survives the next restart.
#[test_log::test(tokio::test)]
async fn mid_segment_corruption_keeps_the_prefix() {
	let dir = TempDir::new().unwrap();
	let o = opts(dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair);
	let (wal, record_len) = write_ten_good(&o).await;
	flip_byte_in_record(&wal, record_len, 5);

	let tree = Tree::new(Arc::clone(&o)).expect("the default mode repairs and opens");
	assert_keys(&tree, "good", 5, &GOOD, "recovery #1");
	for i in 5..10 {
		assert!(get(&tree, &format!("good{i}")).is_none(), "good{i} follows the damage");
	}
	assert_eq!(strict_decode(&wal).len(), 5, "the segment keeps the valid prefix");
	for i in 0..5 {
		put(&tree, &format!("new{i}"), &NEW).await;
	}
	crash(tree);

	let tree = Tree::new(Arc::clone(&o)).unwrap();
	assert_keys(&tree, "good", 5, &GOOD, "recovery #2");
	assert_keys(&tree, "new", 5, &NEW, "recovery #2");
	crash(tree);
}

#[test_log::test(tokio::test)]
async fn mid_segment_corruption_is_refused_by_absolute_consistency() {
	let dir = TempDir::new().unwrap();
	let tolerate = opts(dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair);
	let o = opts(dir.path(), WalRecoveryMode::AbsoluteConsistency);
	let (wal, record_len) = write_ten_good(&tolerate).await;
	flip_byte_in_record(&wal, record_len, 5);
	let before = fs::read(&wal).unwrap();

	match Tree::new(Arc::clone(&o)) {
		Err(Error::WalCorruption {
			segment_id: 1,
			..
		}) => {}
		Err(e) => panic!("expected WalCorruption in segment 1, got: {e}"),
		Ok(_) => panic!("AbsoluteConsistency must refuse mid-segment corruption"),
	}
	assert_eq!(fs::read(&wal).unwrap(), before);
}

/// Garbage that is not a record at all (an invalid record type) after the last
/// valid record is repaired like any other corruption.
#[test_log::test(tokio::test)]
async fn trailing_garbage_is_dropped_and_later_commits_survive() {
	let dir = TempDir::new().unwrap();
	let o = opts(dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair);
	let (wal, _) = write_ten_good(&o).await;
	fs::OpenOptions::new()
		.append(true)
		.open(&wal)
		.unwrap()
		.write_all(b"CORRUPTED_DATA_AT_END")
		.unwrap();

	let tree = Tree::new(Arc::clone(&o)).unwrap();
	assert_keys(&tree, "good", 10, &GOOD, "recovery #1");
	assert_eq!(strict_decode(&wal).len(), 10);
	for i in 0..5 {
		put(&tree, &format!("new{i}"), &NEW).await;
	}
	crash(tree);

	let tree = Tree::new(Arc::clone(&o)).unwrap();
	assert_keys(&tree, "good", 10, &GOOD, "recovery #2");
	assert_keys(&tree, "new", 5, &NEW, "recovery #2");
	crash(tree);
}

// ---------------------------------------------------------------------------
// a segment with no valid record
// ---------------------------------------------------------------------------

/// The only record of the segment is torn. Whatever repair does with a segment that has nothing
/// valid left, commits made afterwards must reach a segment recovery reads.
#[test_log::test(tokio::test)]
async fn segment_with_no_valid_record_does_not_lose_later_commits() {
	// 20 bytes: a partial payload. 3 bytes: a partial header.
	for cut in [20u64, 3] {
		let dir = TempDir::new().unwrap();
		let o = opts(dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair);

		let tree = Tree::new(Arc::clone(&o)).unwrap();
		let wal = wal_path(&o, active_segment(&tree));
		put(&tree, "torn", &[b'y'; 64]).await;
		crash(tree);
		set_len(&wal, cut);

		let tree = Tree::new(Arc::clone(&o)).unwrap();
		assert!(get(&tree, "torn").is_none());
		for i in 0..5 {
			put(&tree, &format!("new{i}"), &NEW).await;
		}
		crash(tree);

		let tree = Tree::new(Arc::clone(&o)).unwrap();
		assert_keys(&tree, "new", 5, &NEW, &format!("cut {cut}: after restart"));
		assert!(get(&tree, "torn").is_none(), "cut {cut}");
		crash(tree);
	}
}

// ---------------------------------------------------------------------------
// several segments
// ---------------------------------------------------------------------------

/// Starts a new WAL segment by rotating the active memtable together with the WAL, so the
/// new memtable's tag follows the new segment (a bare `Wal::rotate` would leave the live
/// tree inconsistent and the next commit is refused). Nothing wakes the flush task, so the
/// old memtable stays in the immutable queue and every segment survives for replay, which
/// is exactly a crash after a rotation but before its flush.
fn rotate_wal(tree: &Tree) {
	tree.core.inner.rotate_memtable().unwrap();
}

/// Three segments, the last one torn: replay takes the parallel path, and only
/// the last segment ends in a torn write.
async fn multi_segment_scenario(shape: Shape) {
	let dir = TempDir::new().unwrap();
	let o = opts(dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair);

	let tree = Tree::new(Arc::clone(&o)).unwrap();
	let first = active_segment(&tree);
	for i in 0..5 {
		put(&tree, &format!("a{i}"), &GOOD).await;
	}
	rotate_wal(&tree);
	for i in 0..5 {
		put(&tree, &format!("b{i}"), &GOOD).await;
	}
	rotate_wal(&tree);
	let last = wal_path(&o, first + 2);
	for i in 0..10 {
		put(&tree, &format!("good{i}"), &GOOD).await;
	}
	let good_len = file_len(&last);
	put(&tree, "torn", &vec![b'y'; shape.value_len]).await;
	crash(tree);
	let cut = match shape.cut {
		Cut::Bytes(n) => n,
		Cut::Block(n) => BLOCK - good_len + n,
	};
	set_len(&last, good_len + cut);
	let earlier =
		[fs::read(wal_path(&o, first)).unwrap(), fs::read(wal_path(&o, first + 1)).unwrap()];

	let tree = Tree::new(Arc::clone(&o)).expect("recovery #1 must open");
	assert_keys(&tree, "a", 5, &GOOD, "recovery #1");
	assert_keys(&tree, "b", 5, &GOOD, "recovery #1");
	assert_keys(&tree, "good", 10, &GOOD, "recovery #1");
	assert!(get(&tree, "torn").is_none());
	assert_eq!(strict_decode(&last).len(), 10, "the last segment holds what was acknowledged");
	assert_eq!(fs::read(wal_path(&o, first)).unwrap(), earlier[0], "an earlier segment changed");
	assert_eq!(
		fs::read(wal_path(&o, first + 1)).unwrap(),
		earlier[1],
		"an earlier segment changed"
	);
	assert_eq!(active_segment(&tree), first + 3, "recovery continues in a fresh segment");
	for i in 0..5 {
		put(&tree, &format!("new{i}"), &NEW).await;
	}
	crash(tree);

	assert_eq!(strict_decode(&wal_path(&o, first + 3)).len(), 5);
	let tree = Tree::new(Arc::clone(&o)).expect("recovery #2 must open");
	assert_keys(&tree, "a", 5, &GOOD, "recovery #2");
	assert_keys(&tree, "b", 5, &GOOD, "recovery #2");
	assert_keys(&tree, "good", 10, &GOOD, "recovery #2");
	assert_keys(&tree, "new", 5, &NEW, "recovery #2");
	crash(tree);
}

#[test_log::test(tokio::test)]
async fn multi_segment_partial_header_in_the_last_segment() {
	multi_segment_scenario(Shape {
		value_len: 64,
		cut: Cut::Bytes(3),
	})
	.await;
}

#[test_log::test(tokio::test)]
async fn multi_segment_partial_payload_in_the_last_segment() {
	multi_segment_scenario(Shape {
		value_len: 64,
		cut: Cut::Bytes(20),
	})
	.await;
}

#[test_log::test(tokio::test)]
async fn multi_segment_partial_first_fragment_in_the_last_segment() {
	multi_segment_scenario(Shape {
		value_len: 100_000,
		cut: Cut::Bytes(1000),
	})
	.await;
}

/// Corruption in an earlier segment: the default mode keeps that segment's valid prefix and
/// still replays the later segments; `AbsoluteConsistency` refuses.
#[test_log::test(tokio::test)]
async fn multi_segment_corruption_in_an_earlier_segment() {
	let dir = TempDir::new().unwrap();
	let tolerate = opts(dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair);

	let tree = Tree::new(Arc::clone(&tolerate)).unwrap();
	let first_id = active_segment(&tree);
	let first = wal_path(&tolerate, first_id);
	for i in 0..10 {
		put(&tree, &format!("a{i}"), &GOOD).await;
	}
	let record_len = file_len(&first) / 10;
	rotate_wal(&tree);
	for i in 0..10 {
		put(&tree, &format!("b{i}"), &GOOD).await;
	}
	crash(tree);
	flip_byte_in_record(&first, record_len, 5);
	let damaged = fs::read(&first).unwrap();
	let second = fs::read(wal_path(&tolerate, first_id + 1)).unwrap();

	// A refused open leaves its lock file held, so it is tried on a copy.
	let copy = TempDir::new().unwrap();
	copy_dir(dir.path(), copy.path());
	let refused = opts(copy.path(), WalRecoveryMode::AbsoluteConsistency);
	match Tree::new(Arc::clone(&refused)) {
		Err(Error::WalCorruption {
			segment_id,
			..
		}) if segment_id as u64 == first_id => {}
		Err(e) => panic!("expected WalCorruption in segment {first_id}, got: {e}"),
		Ok(_) => panic!("AbsoluteConsistency must refuse corruption in an earlier segment"),
	}
	assert_eq!(fs::read(wal_path(&refused, first_id)).unwrap(), damaged);

	let tree = Tree::new(Arc::clone(&tolerate)).expect("the default mode repairs and opens");
	assert_keys(&tree, "a", 5, &GOOD, "recovery #1");
	for i in 5..10 {
		assert!(get(&tree, &format!("a{i}")).is_none(), "a{i} follows the damage");
	}
	assert_keys(&tree, "b", 10, &GOOD, "recovery #1");
	assert_eq!(strict_decode(&first).len(), 5, "the earlier segment keeps its valid prefix");
	assert_eq!(
		fs::read(wal_path(&tolerate, first_id + 1)).unwrap(),
		second,
		"the last segment is untouched"
	);
	for i in 0..5 {
		put(&tree, &format!("new{i}"), &NEW).await;
	}
	crash(tree);

	let tree = Tree::new(Arc::clone(&tolerate)).unwrap();
	assert_keys(&tree, "a", 5, &GOOD, "recovery #2");
	assert_keys(&tree, "b", 10, &GOOD, "recovery #2");
	assert_keys(&tree, "new", 5, &NEW, "recovery #2");
	crash(tree);
}

// ---------------------------------------------------------------------------
// compressed segments
// ---------------------------------------------------------------------------

/// A database whose only WAL segment is LZ4-compressed. The tree never writes
/// compressed segments itself (it opens the WAL with default options), but it
/// must read them, and repair must handle them.
///
/// Returns the segment and the end of each of the `records` crafted records.
fn compressed_database(o: &Options, records: usize) -> (PathBuf, Vec<u64>) {
	// The tree creates the manifest and its empty segments, which are replaced.
	let wal_dir = o.wal_dir();
	for entry in fs::read_dir(&wal_dir).unwrap() {
		fs::remove_file(entry.unwrap().path()).unwrap();
	}
	let mut wal =
		Wal::open(&wal_dir, WalOptions::default().with_compression(CompressionType::Lz4)).unwrap();
	let segment = wal_path(o, wal.get_active_log_number());
	let mut ends = Vec::new();
	for i in 0..records {
		wal.append(&wal_record(i as u64 + 1, &format!("good{i}"), &GOOD)).unwrap();
		ends.push(file_len(&segment));
	}
	wal.close().unwrap();
	(segment, ends)
}

async fn create_empty_database(o: &Arc<Options>) {
	let tree = Tree::new(Arc::clone(o)).unwrap();
	crash(tree);
}

async fn compressed_scenario(cut: u64) {
	let dir = TempDir::new().unwrap();
	let o = opts(dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair);
	create_empty_database(&o).await;
	// 10 good records and an 11th that is cut `cut` bytes in.
	let (wal, ends) = compressed_database(&o, 11);
	let good_len = ends[9];
	assert!(ends[10] > good_len + cut);
	set_len(&wal, good_len + cut);
	assert_eq!(fs::read(&wal).unwrap()[6], 9, "the segment starts with a compression record");

	let tree = Tree::new(Arc::clone(&o)).expect("recovery #1 must open");
	assert_keys(&tree, "good", 10, &GOOD, "recovery #1");
	assert!(get(&tree, "good10").is_none());
	assert_eq!(strict_decode(&wal).len(), 10);
	for i in 0..5 {
		put(&tree, &format!("new{i}"), &NEW).await;
	}
	crash(tree);

	let tree = Tree::new(Arc::clone(&o)).expect("recovery #2 must open");
	assert_keys(&tree, "good", 10, &GOOD, "recovery #2");
	assert_keys(&tree, "new", 5, &NEW, "recovery #2");
	crash(tree);
}

#[test_log::test(tokio::test)]
async fn compressed_segment_partial_header() {
	compressed_scenario(3).await;
}

#[test_log::test(tokio::test)]
async fn compressed_segment_partial_payload() {
	compressed_scenario(20).await;
}

/// The only data record is torn. Nothing valid remains, and the commits made after recovery are
/// in an uncompressed segment of their own.
#[test_log::test(tokio::test)]
async fn compressed_segment_with_no_valid_record_does_not_lose_later_commits() {
	for cut in [3u64, 20] {
		let dir = TempDir::new().unwrap();
		let o = opts(dir.path(), WalRecoveryMode::TolerateCorruptedWithRepair);
		create_empty_database(&o).await;
		let (wal, ends) = compressed_database(&o, 1);
		// Keep the compression record and `cut` bytes of the one data record.
		set_len(&wal, 8 + cut);
		assert!(ends[0] > 8 + cut);

		let tree = Tree::new(Arc::clone(&o)).expect("recovery #1 must open");
		assert!(get(&tree, "good0").is_none());
		for i in 0..5 {
			put(&tree, &format!("new{i}"), &NEW).await;
		}
		crash(tree);

		let tree = Tree::new(Arc::clone(&o)).expect("recovery #2 must open");
		assert_keys(&tree, "new", 5, &NEW, &format!("cut {cut}: recovery #2"));
		assert!(get(&tree, "good0").is_none(), "cut {cut}");
		crash(tree);
	}
}
