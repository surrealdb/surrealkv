//! Crash-safety and durability tests for the coalesced WAL group append.
//!
//! A commit group is encoded into one buffer and appended with one call: one WAL lock, one
//! hand-off to the pool, one `guard_append` around every record and one flush. These tests try
//! to break the promises that rest on that:
//!
//! * a failed append, wherever in the group it fails, leaves exactly the bytes that were in the
//!   segment before the group, and poisons the writer;
//! * the bytes of a group are the bytes one `add_record` per batch writes, for any mix of record
//!   sizes, block positions and compression;
//! * a crash image cut at any byte of a coalesced group recovers a prefix of whole records, and the
//!   commits made after that recovery survive another crash;
//! * Immediate groups are fsynced before anyone is told, on the re-append path as well;
//! * the flusher's reused buffers never carry bytes from one group into the next.
//!
//! Test-only instrumentation these tests need: `BufferedFileWriter::pending_sync` (and the
//! `Writer` / `Wal` forwarders), and the failpoint `fail_after_ops` and the write counter
//! `file_writes`.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use tempdir::TempDir;
use test_log::test;

use crate::batch::Batch;
use crate::ring::PipelineHook;
use crate::vlog::ValueLocation;
use crate::wal::manager::Wal;
use crate::wal::reader::Reader;
use crate::wal::writer::Writer;
use crate::wal::{
	BufferedFileWriter,
	CompressionType,
	Error as WalError,
	Options as WalOptions,
	WritableFile,
	BLOCK_SIZE,
	HEADER_SIZE,
};
use crate::{Durability, InternalKeyKind, Mode, Options, Tree};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// A deterministic generator, so a failing iteration can be replayed.
struct Rng(u64);

impl Rng {
	fn next(&mut self) -> u64 {
		self.0 ^= self.0 << 13;
		self.0 ^= self.0 >> 7;
		self.0 ^= self.0 << 17;
		self.0
	}

	fn below(&mut self, n: usize) -> usize {
		(self.next() % n as u64) as usize
	}
}

/// A record of `len` bytes that no other `index` shares and that does not compress.
fn payload(index: usize, len: usize) -> Vec<u8> {
	let mut x = (index as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
	(0..len)
		.map(|_| {
			x ^= x << 13;
			x ^= x >> 7;
			x ^= x << 17;
			x as u8
		})
		.collect()
}

fn join(records: &[Vec<u8>]) -> (Vec<u8>, Vec<usize>) {
	let mut buf = Vec::new();
	let mut ends = Vec::new();
	for record in records {
		buf.extend_from_slice(record);
		ends.push(buf.len());
	}
	(buf, ends)
}

fn new_writer(path: &Path, compression: CompressionType) -> Writer {
	let buffered = BufferedFileWriter::new(File::create(path).unwrap(), BLOCK_SIZE);
	let mut writer = Writer::new(buffered, false, compression, 0);
	if compression != CompressionType::None {
		writer.add_compression_type_record().unwrap();
	}
	writer
}

/// Every record in `path`, the way recovery reads them.
fn read_records(path: &Path) -> Vec<Vec<u8>> {
	let mut reader = Reader::new(File::open(path).unwrap());
	let mut records = Vec::new();
	loop {
		match reader.read() {
			Ok((record, _)) => records.push(record.to_vec()),
			Err(WalError::IO(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
				return records
			}
			Err(e) => panic!("{} must read end to end: {e}", path.display()),
		}
	}
}

/// Every record of a segment with the offset it ends at, decoded as a batch.
fn records_with_ends(path: &Path) -> Vec<(u64, Batch)> {
	let mut reader = Reader::new(File::open(path).unwrap());
	let mut out = Vec::new();
	loop {
		match reader.read() {
			Ok((record, end)) => out.push((end, Batch::decode(record).unwrap())),
			Err(WalError::IO(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return out,
			Err(e) => panic!("{} must read end to end: {e}", path.display()),
		}
	}
}

fn user_keys(batch: &Batch) -> Vec<String> {
	batch.entries.iter().map(|e| String::from_utf8(e.key.clone()).unwrap()).collect()
}

fn copy_dir(src: &Path, dst: &Path) {
	fs::create_dir_all(dst).unwrap();
	for entry in fs::read_dir(src).unwrap() {
		let entry = entry.unwrap();
		if entry.file_name() == "LOCK" {
			continue;
		}
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

fn seg_path(dir: &Path, id: u64) -> PathBuf {
	dir.join("wal").join(format!("{id:020}.wal"))
}

/// The segment `tree`'s commits go to: opening a tree continues in a fresh segment, so this is
/// not segment 0.
fn active_segment(tree: &Tree) -> u64 {
	tree.core.inner.wal.read().get_active_log_number()
}

fn segment_ids(dir: &Path) -> Vec<u64> {
	let mut ids: Vec<u64> = fs::read_dir(dir.join("wal"))
		.unwrap()
		.filter_map(|e| {
			let name = e.unwrap().file_name().to_string_lossy().to_string();
			name.strip_suffix(".wal").and_then(|s| s.parse().ok())
		})
		.collect();
	ids.sort();
	ids
}

/// The highest-numbered segment in `dir/wal`.
fn last_segment(dir: &Path) -> PathBuf {
	let ids = segment_ids(dir);
	seg_path(dir, *ids.last().expect("a segment"))
}

fn opts(path: &Path, max_memtable_size: usize) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		max_memtable_size,
		// Crash semantics: nothing may be flushed on drop or close.
		flush_on_close: false,
		memtable_stall_threshold: 100_000,
		level0_max_files: 10_000,
		l0_stall_threshold: 10_000,
		..Default::default()
	})
}

const BIG: usize = 100 * 1024 * 1024;

fn mark_closed(tree: &Tree) {
	tree.core.is_closed.store(true, Ordering::SeqCst);
}

fn release_lock(tree: &Tree) {
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
}

/// Kills a tree: releases the lock file and drops it without `close()`. Current-thread
/// runtime, and no await before the next open, so the spawned best-effort close never runs.
fn crash(tree: Tree) {
	release_lock(&tree);
	mark_closed(&tree);
	drop(tree);
}

async fn stop_background_tasks(tree: &Tree) {
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
}

fn get(tree: &Tree, key: &str) -> Option<Vec<u8>> {
	tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key.as_bytes()).unwrap()
}

async fn put(tree: &Tree, key: &str, value: &[u8], durability: Durability) {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(durability);
	txn.set(key.as_bytes(), value).unwrap();
	txn.commit().await.unwrap();
}

/// One single-key transaction per entry, all spawned before the flusher is polled: on the
/// current-thread runtime that is one commit group, in order.
async fn commit_group(
	tree: &Arc<Tree>,
	entries: &[(String, Vec<u8>)],
	durability: Durability,
) -> Vec<crate::Result<()>> {
	let mut handles = Vec::new();
	for (key, value) in entries {
		let (tree, key, value) = (Arc::clone(tree), key.clone(), value.clone());
		handles.push(tokio::spawn(async move {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(durability);
			txn.set(key.as_bytes(), &value).unwrap();
			txn.commit().await
		}));
	}
	let mut out = Vec::new();
	for h in handles {
		out.push(h.await.unwrap());
	}
	out
}

/// A raw batch of `Set`s starting at the next sequence numbers (timestamp 0), exactly as
/// `flush_entries` hands them to `flush_group`.
fn raw_batch(tree: &Tree, entries: &[(&str, &[u8])]) -> Batch {
	let start =
		tree.core.commit_pipeline.log_seq_num.fetch_add(entries.len() as u64, Ordering::SeqCst);
	let mut batch = Batch::new(start);
	for (k, v) in entries {
		batch.add_record(InternalKeyKind::Set, k.as_bytes().to_vec(), Some(v.to_vec()), 0).unwrap();
	}
	batch
}

/// Where in its block a record of `len` bytes that starts at `offset` in its block ends.
fn block_offset_after(mut offset: usize, mut len: usize) -> usize {
	loop {
		if BLOCK_SIZE - offset < HEADER_SIZE {
			offset = 0;
		}
		let fragment = len.min(BLOCK_SIZE - offset - HEADER_SIZE);
		offset += HEADER_SIZE + fragment;
		len -= fragment;
		if len == 0 {
			return offset;
		}
	}
}

/// The value length at which a one-key raw batch ends with `left` bytes to spare in its block
/// when appended to a segment of `segment_len` bytes (timestamp 0, as `raw_batch` builds it).
fn value_len_leaving(tree: &Tree, key: &str, segment_len: usize, left: usize) -> usize {
	let encoded_len = |len: usize| {
		let start = tree.core.commit_pipeline.log_seq_num.load(Ordering::SeqCst);
		let mut batch = Batch::new(start);
		let value = ValueLocation::with_inline_value(vec![b'p'; len]).encode();
		batch.add_record(InternalKeyKind::Set, key.as_bytes().to_vec(), Some(value), 0).unwrap();
		batch.encode().unwrap().len()
	};
	let overhead = encoded_len(20_000) - 20_000;
	let want = BLOCK_SIZE - left;
	(20_000..20_000 + 2 * BLOCK_SIZE)
		.find(|len| block_offset_after(segment_len % BLOCK_SIZE, len + overhead) == want)
		.expect("some length in two blocks' worth ends the record where asked")
}

// ---------------------------------------------------------------------------
// writer level: a group that fails anywhere leaves exactly the bytes before it
// ---------------------------------------------------------------------------

/// Filler sizes that leave the group starting at block offsets all over the block, including
/// each number of bytes left (1..=6) that makes the first record of the group start with
/// padding, 7 (room for a header and no data), and none.
const FILLERS: [usize; 17] = [
	1, 6, 33, 1_000, 16_384, 32_000, 32_752, 32_753, 32_754, 32_755, 32_756, 32_757, 32_758,
	32_759, 32_760, 32_761, 70_000,
];

fn shapes() -> Vec<Vec<usize>> {
	vec![
		vec![8; 8],
		vec![40; 50],
		vec![10_000; 5],
		// records that fill a block exactly, and one byte either side
		vec![32_754, 32_761, 32_762, 32_760],
		vec![5, 100_000, 5, 40_000, 5],
		vec![150_000],
		// every record leaves the next one starting with padding
		vec![32_754, 1, 32_761, 2, 32_700, 3],
		// more than a buffer's worth of bytes, none of it in one record
		vec![8_000; 10],
	]
}

/// For every filler, group shape and compression, and for the append failing at every one of
/// its `append`s and its final `flush` (the failpoint counts both): the segment holds exactly
/// the bytes it held before the group, byte for byte, and not a prefix of the group; the writer
/// is poisoned and takes nothing more; and the segment reads back as the filler alone. When the
/// failpoint is past the last write the group goes through and is byte-identical to one
/// `add_record` per record, which is the control.
#[test]
fn a_group_that_fails_at_any_write_leaves_exactly_the_bytes_before_it() {
	let dir = TempDir::new("wal_group_crash").unwrap();
	let path = dir.path().join("w.wal");
	let mut iterations = 0usize;
	for compression in [CompressionType::None, CompressionType::Lz4] {
		for (f, filler_len) in FILLERS.iter().enumerate() {
			// LZ4 frames the same way whatever the bytes are: a third of the fillers is enough,
			// and the shapes of several blocks, which cost the most, run on every other filler.
			if compression == CompressionType::Lz4 && f % 3 != 0 {
				continue;
			}
			let filler = payload(500 + f, *filler_len);
			for (s, shape) in shapes().iter().enumerate() {
				if shape.iter().sum::<usize>() > 120_000 && f % 2 == 1 {
					continue;
				}
				let records: Vec<Vec<u8>> =
					shape.iter().enumerate().map(|(i, len)| payload(1000 * s + i, *len)).collect();
				let (buf, ends) = join(&records);

				{
					let mut w = new_writer(&path, compression);
					w.add_record(&filler).unwrap();
				}
				let pre = fs::read(&path).unwrap();
				{
					let mut w = new_writer(&path, compression);
					w.add_record(&filler).unwrap();
					for r in &records {
						w.add_record(r).unwrap();
					}
				}
				let want = fs::read(&path).unwrap();

				let at = format!("{compression:?}, filler {filler_len}, shape #{s}");
				let mut went_through = false;
				for fail_after in 0..100_000 {
					iterations += 1;
					let mut w = new_writer(&path, compression);
					w.add_record(&filler).unwrap();
					w.fail_after_ops(fail_after);
					match w.add_records(&buf, &ends) {
						Ok(()) => {
							assert_eq!(
								fs::read(&path).unwrap(),
								want,
								"{at}: the control group must equal one add_record per record"
							);
							assert!(fail_after > 0, "{at}: the failpoint at 0 must fail");
							went_through = true;
							break;
						}
						Err(_) => {
							assert_eq!(
								fs::read(&path).unwrap(),
								pre,
								"{at}, fail_after={fail_after}: the segment must be cut back to \
								 the bytes before the group"
							);
							assert!(
								w.add_records(&buf, &ends).is_err(),
								"{at}, fail_after={fail_after}: a poisoned writer takes no group"
							);
							assert!(
								w.add_record(b"x").is_err(),
								"{at}, fail_after={fail_after}: a poisoned writer takes no record"
							);
							assert_eq!(fs::read(&path).unwrap(), pre);
							assert_eq!(read_records(&path), vec![filler.clone()]);
						}
					}
				}
				assert!(went_through, "{at}: the group never went through");
			}
		}
	}
	assert!(iterations > 3_000, "only {iterations} iterations ran");
}

/// A writer replaced after a failure (what a rotation does, and what recovery does with the
/// segment) must take the next group at the right place: cut back, then appended to again from
/// the segment's own length, the segment reads end to end and is the bytes of the records that
/// were acknowledged.
#[test]
fn a_segment_cut_back_after_a_failed_group_takes_the_next_group_cleanly() {
	let dir = TempDir::new("wal_group_crash").unwrap();
	let path = dir.path().join("w.wal");
	for compression in [CompressionType::None, CompressionType::Lz4] {
		for filler_len in [1usize, 32_754, 32_755, 32_760, 40_000] {
			for fail_after in [0usize, 1, 2, 3, 5, 8, 13, 21] {
				let filler = payload(1, filler_len);
				let doomed: Vec<Vec<u8>> = (0..6).map(|i| payload(10 + i, 9_000)).collect();
				let (buf, ends) = join(&doomed);
				let mut w = new_writer(&path, compression);
				w.add_record(&filler).unwrap();
				w.fail_after_ops(fail_after);
				if w.add_records(&buf, &ends).is_ok() {
					// Past the last write of the group: nothing failed, nothing to cut back.
					continue;
				}
				drop(w);

				// A new writer on the same file at its length, as `create_writer` does.
				let len = fs::metadata(&path).unwrap().len();
				let file = fs::OpenOptions::new().append(true).read(true).open(&path).unwrap();
				let mut w2 = Writer::new(
					BufferedFileWriter::new(file, BLOCK_SIZE),
					false,
					compression,
					(len as usize) % BLOCK_SIZE,
				);
				let good: Vec<Vec<u8>> =
					(0..4).map(|i| payload(100 + i, 3_000 * (i + 1))).collect();
				let (buf, ends) = join(&good);
				w2.add_records(&buf, &ends).unwrap();
				drop(w2);

				let mut want = vec![filler];
				want.extend(good);
				assert_eq!(
					read_records(&path),
					want,
					"{compression:?}, filler {filler_len}, fail_after={fail_after}"
				);
			}
		}
	}
}

/// Boundary-biased record lengths, so that records start with 0..=6 bytes of block left, end
/// exactly at a block's end, and span several blocks.
fn biased_len(rng: &mut Rng) -> usize {
	let frag = BLOCK_SIZE - HEADER_SIZE;
	match rng.below(9) {
		0 => 1 + rng.below(64),
		1 => 1 + rng.below(2_000),
		2 => 1 + rng.below(40_000),
		3 => frag - 3 + rng.below(7),
		4 => 2 * frag - 3 + rng.below(7),
		5 => frag - 20 + rng.below(21),
		6 => 1 + rng.below(100_000),
		7 => frag - 14 + rng.below(8),
		_ => 1 + rng.below(300),
	}
}

/// Random groups at random block positions: the group's bytes are the bytes of one
/// `add_record` per record, and read back as one record per input.
#[test]
fn random_groups_are_byte_identical_to_one_append_per_record() {
	let dir = TempDir::new("wal_group_crash").unwrap();
	let (one, grouped) = (dir.path().join("one.wal"), dir.path().join("group.wal"));
	for (compression, iterations) in [(CompressionType::None, 1_500), (CompressionType::Lz4, 500)] {
		let mut rng = Rng(0x1234_5678_9ABC_DEF1 ^ iterations as u64);
		for it in 0..iterations {
			// 1 to 3 fillers (one add_record each, to start the group anywhere in a block) and
			// then a group of 1 to 8 records.
			let fillers: Vec<Vec<u8>> =
				(0..1 + rng.below(3)).map(|i| payload(it * 31 + i, biased_len(&mut rng))).collect();
			let records: Vec<Vec<u8>> = (0..1 + rng.below(8))
				.map(|i| payload(it * 31 + 10 + i, biased_len(&mut rng)))
				.collect();
			{
				let mut w = new_writer(&one, compression);
				for r in fillers.iter().chain(&records) {
					w.add_record(r).unwrap();
				}
			}
			{
				let mut w = new_writer(&grouped, compression);
				for r in &fillers {
					w.add_record(r).unwrap();
				}
				let (buf, ends) = join(&records);
				w.add_records(&buf, &ends).unwrap();
			}
			assert_eq!(
				fs::read(&one).unwrap(),
				fs::read(&grouped).unwrap(),
				"{compression:?} iteration {it}: fillers {:?}, records {:?}",
				fillers.iter().map(Vec::len).collect::<Vec<_>>(),
				records.iter().map(Vec::len).collect::<Vec<_>>()
			);
			let mut want = fillers.clone();
			want.extend(records);
			assert_eq!(read_records(&grouped), want, "{compression:?} iteration {it}");
		}
	}
}

/// `Wal::append_group`: the whole group is in the segment it returns, rotating moves the next
/// group to the next segment, and a malformed group writes nothing and does not poison.
#[test]
fn wal_append_group_lands_in_one_segment_and_a_bad_group_neither_writes_nor_poisons() {
	let dir = TempDir::new("wal_group_crash").unwrap();
	let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
	let seg0 = dir.path().join("00000000000000000000.wal");
	let seg1 = dir.path().join("00000000000000000001.wal");
	let buf: Vec<u8> = payload(7, 100);

	let before = fs::read(&seg0).unwrap();
	for ends in [vec![0usize], vec![50, 50], vec![50, 40], vec![101], vec![30, 60, 101]] {
		assert!(wal.append_group(&buf, &ends).is_err(), "{ends:?}");
		assert_eq!(fs::read(&seg0).unwrap(), before, "{ends:?}: nothing may be written");
	}
	assert_eq!(wal.append_group(&buf, &[]).unwrap(), 0, "an empty group is a no-op");
	assert_eq!(fs::read(&seg0).unwrap(), before);

	assert_eq!(wal.append_group(&buf, &[30, 100]).unwrap(), 0);
	wal.rotate().unwrap();
	assert_eq!(wal.append_group(&buf, &[10, 20, 100]).unwrap(), 1);
	drop(wal);
	assert_eq!(read_records(&seg0), vec![buf[..30].to_vec(), buf[30..].to_vec()]);
	assert_eq!(
		read_records(&seg1),
		vec![buf[..10].to_vec(), buf[10..20].to_vec(), buf[20..].to_vec()]
	);
}

// ---------------------------------------------------------------------------
// a real short write in the middle of a group (RLIMIT_FSIZE in a child process)
// ---------------------------------------------------------------------------

#[cfg(all(unix, target_pointer_width = "64"))]
mod short_write {
	use test_log::test;

	use super::*;

	#[repr(C)]
	struct Rlimit {
		cur: u64,
		max: u64,
	}

	extern "C" {
		fn setrlimit(resource: i32, rlp: *const Rlimit) -> i32;
		fn signal(signum: i32, handler: usize) -> usize;
	}

	const RLIMIT_FSIZE: i32 = 1;
	const SIGXFSZ: i32 = 25;
	const SIG_IGN: usize = 1;

	/// Caps the size of every file this process writes. A `write(2)` that crosses the cap
	/// writes up to it and returns the short count, and the next one fails with EFBIG.
	fn limit_file_size(bytes: u64) {
		// SAFETY: plain libc calls with valid arguments; only ever run in a child process
		// started by `a_real_short_write_...`, never in the shared test process.
		unsafe {
			signal(SIGXFSZ, SIG_IGN);
			let limit = Rlimit {
				cur: bytes,
				max: bytes,
			};
			assert_eq!(setrlimit(RLIMIT_FSIZE, &limit), 0);
		}
	}

	pub const CHILD_DIR: &str = "SKV_SHORT_WRITE_CHILD_DIR";
	pub const WRITER_LIMIT: u64 = 300_000;
	pub const TREE_LIMIT: u64 = 1_000_000;
	pub const TREE_VALUE: usize = 100_000;

	/// Child, writer level. Proves the premise (the kernel really writes part of a write and
	/// then fails), shows the bare buffered writer leaves a partial group behind, and then
	/// that `add_records` cuts exactly that back.
	#[test]
	fn short_write_child_writer() {
		let Some(dir) = std::env::var_os(CHILD_DIR) else {
			return;
		};
		let dir = PathBuf::from(dir);
		limit_file_size(WRITER_LIMIT);

		{
			use std::io::Write;
			let mut raw = File::create(dir.join("raw")).unwrap();
			let n = raw.write(&vec![7u8; 400_000]).unwrap();
			assert_eq!(n as u64, WRITER_LIMIT, "the kernel must short-write at the limit");
			assert!(raw.write(&[1u8; 10]).is_err(), "and refuse the next write");
		}

		let records: Vec<Vec<u8>> = (0..5).map(|i| payload(10 + i, 100_000)).collect();
		let (buf, ends) = join(&records);

		// Without the guard: the group is partly in the file when the write fails.
		{
			let mut bare =
				BufferedFileWriter::new(File::create(dir.join("bare.wal")).unwrap(), 32 * 1024);
			assert!(bare.append(&buf).is_err());
			let len = fs::metadata(dir.join("bare.wal")).unwrap().len();
			assert!(len > 0 && (len as usize) < buf.len(), "a partial group of {len} bytes landed");
		}

		let path = dir.join("short.wal");
		let mut w = new_writer(&path, CompressionType::None);
		w.add_record(&payload(1, 50_000)).unwrap();
		let pre = fs::read(&path).unwrap();
		assert!(w.add_records(&buf, &ends).is_err());
		assert_eq!(fs::read(&path).unwrap(), pre, "the partial group must be cut off");
		assert!(w.add_record(b"x").is_err(), "poisoned");
		assert_eq!(read_records(&path), vec![payload(1, 50_000)]);
	}

	/// Child, tree level: acknowledged commits, then a group that crosses the size limit in the
	/// middle of its append.
	#[test(tokio::test)]
	async fn short_write_child_tree() {
		let Some(dir) = std::env::var_os(CHILD_DIR) else {
			return;
		};
		let live = PathBuf::from(dir).join("tree");
		let tree = Arc::new(Tree::new(opts(&live, BIG)).unwrap());
		let acked: Vec<(String, Vec<u8>)> =
			(0..5).map(|i| (format!("acked{i}"), payload(i, TREE_VALUE))).collect();
		for result in commit_group(&tree, &acked, Durability::Immediate).await {
			result.unwrap();
		}
		let segment = seg_path(&live, active_segment(&tree));
		let acked_bytes = fs::read(&segment).unwrap();
		assert!(acked_bytes.len() as u64 > 5 * TREE_VALUE as u64);

		limit_file_size(TREE_LIMIT);
		let doomed: Vec<(String, Vec<u8>)> =
			(0..8).map(|i| (format!("doomed{i}"), payload(50 + i, TREE_VALUE))).collect();
		assert!(
			acked_bytes.len() as u64 + 8 * TREE_VALUE as u64 > TREE_LIMIT,
			"the group must cross the limit"
		);
		assert!(
			(acked_bytes.len() + 2 * TREE_VALUE) as u64 <= TREE_LIMIT,
			"and part of it must fit under the limit, so a partial group is written first"
		);
		let results = commit_group(&tree, &doomed, Durability::Immediate).await;
		assert!(results.iter().all(|r| r.is_err()), "no commit of the group is acknowledged");
		assert_eq!(fs::read(&segment).unwrap(), acked_bytes, "the segment is cut back");
		for (k, _) in &doomed {
			assert_eq!(get(&tree, k), None, "{k} was never acknowledged");
		}
		let later =
			commit_group(&tree, &[("later".to_string(), vec![1; 10])], Durability::Immediate).await;
		assert!(later[0].is_err(), "the poisoned writer takes no more");
		assert_eq!(fs::read(&segment).unwrap(), acked_bytes);

		let tree = Arc::try_unwrap(tree).ok().expect("the writers are done");
		crash(tree);
	}

	/// Runs the two children under a file size limit in a process of their own (the limit is
	/// process-wide, so it cannot be set in the shared test process), then opens what the
	/// killed child left, with no limit.
	#[test(tokio::test)]
	async fn a_real_short_write_in_the_middle_of_a_group_is_cut_back_and_recovered() {
		if std::env::var_os(CHILD_DIR).is_some() {
			return;
		}
		let dir = TempDir::new("wal_group_crash").unwrap();
		let out = std::process::Command::new(std::env::current_exe().unwrap())
			.args([
				"--exact",
				"test::wal_group_crash_tests::short_write::short_write_child_writer",
				"test::wal_group_crash_tests::short_write::short_write_child_tree",
				"--test-threads=1",
			])
			.env(CHILD_DIR, dir.path())
			.output()
			.unwrap();
		let stdout = String::from_utf8_lossy(&out.stdout);
		let stderr = String::from_utf8_lossy(&out.stderr);
		assert!(out.status.success(), "child failed:\n{stdout}\n{stderr}");
		assert!(
			stdout.contains("2 passed"),
			"both children must have run, and not been filtered out:\n{stdout}"
		);

		let live = dir.path().join("tree");
		let segment = last_segment(&live);
		let records = records_with_ends(&segment);
		assert_eq!(records.len(), 5, "only the acknowledged commits are in the segment");
		assert_eq!(fs::metadata(&segment).unwrap().len(), records.last().unwrap().0);
		let reopened = Tree::new(opts(&live, BIG)).unwrap();
		for i in 0..5 {
			assert_eq!(
				get(&reopened, &format!("acked{i}")).as_deref(),
				Some(payload(i, TREE_VALUE).as_slice())
			);
		}
		for i in 0..8 {
			assert_eq!(get(&reopened, &format!("doomed{i}")), None);
		}
		assert_eq!(get(&reopened, "later"), None);
		mark_closed(&reopened);
		release_lock(&reopened);
	}
}

// ---------------------------------------------------------------------------
// torn tail: a crash image cut at every byte of a coalesced group
// ---------------------------------------------------------------------------

/// Which cut offsets of the group a sweep tries.
#[derive(Clone, Copy)]
enum Cuts {
	/// Every byte from the start of the group to the end of the segment.
	Every,
	/// The bytes next to every record end, block boundary and the start, and a stride.
	Sampled,
}

/// What the crash leaves of the bytes after the cut.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Damage {
	/// The file ends at the cut: a write that stopped half way.
	Truncate,
	/// The file keeps its length and is zero from the cut: the file size was updated and the
	/// data blocks were not.
	ZeroTail,
}

/// A segment with a few durable records and a leaver that ends `left` bytes short of its block,
/// then one coalesced group (`flush_group`, so one append) of `group_lens` single-key batches.
/// The segment is cut at each offset of the group in turn: recovery must keep every record that
/// is whole and nothing of the rest, and the tree must take new commits, and a group, and
/// survive another crash.
async fn torn_group_sweep(left: usize, group_lens: &[usize], cuts: Cuts, damage: Damage) {
	let base = TempDir::new("wal_group_crash").unwrap();
	let live = base.path().join("live");
	let tree = Arc::new(Tree::new(opts(&live, BIG)).unwrap());
	let pipeline = &tree.core.commit_pipeline;

	// Durable records the cut must never touch.
	for i in 0..3 {
		let batches = vec![raw_batch(&tree, &[(format!("pre{i}").as_str(), &[b'x'; 60])])];
		pipeline.flush_group(&batches, true).await.unwrap();
	}
	let first = active_segment(&tree);
	let segment = seg_path(&live, first);
	let segment_len = || fs::metadata(&segment).unwrap().len() as usize;
	let len = value_len_leaving(&tree, "leaver", segment_len(), left);
	let leaver = vec![b'l'; len];
	pipeline
		.flush_group(&[raw_batch(&tree, &[("leaver", leaver.as_slice())])], true)
		.await
		.unwrap();
	assert_eq!(segment_len() % BLOCK_SIZE, (BLOCK_SIZE - left) % BLOCK_SIZE);
	let pre_len = segment_len() as u64;

	// The group under test: one append, one record per batch.
	let group: Vec<(String, Vec<u8>)> = group_lens
		.iter()
		.enumerate()
		.map(|(i, len)| (format!("g{i}"), payload(900 + i, *len)))
		.collect();
	let batches: Vec<Batch> =
		group.iter().map(|(k, v)| raw_batch(&tree, &[(k.as_str(), v.as_slice())])).collect();
	pipeline.flush_group(&batches, true).await.unwrap();

	let image = base.path().join("image");
	copy_dir(&live, &image);
	let tree = Arc::try_unwrap(tree).ok().expect("no other owner");
	crash(tree);

	let image_segment = seg_path(&image, first);
	let image_len = fs::metadata(&image_segment).unwrap().len();
	let records = records_with_ends(&image_segment);
	assert_eq!(records.len(), 3 + 1 + group.len());
	let group_ends: Vec<u64> = records[4..].iter().map(|(end, _)| *end).collect();
	assert!(group_ends[0] > pre_len);

	let offsets: Vec<u64> = match cuts {
		Cuts::Every => (pre_len..=image_len).collect(),
		Cuts::Sampled => {
			let mut set = std::collections::BTreeSet::new();
			for c in pre_len..(pre_len + 12).min(image_len + 1) {
				set.insert(c);
			}
			for end in group_ends.iter().copied().chain([image_len]) {
				for d in 0..=6u64 {
					set.insert(end.saturating_sub(d).max(pre_len));
					set.insert((end + d).min(image_len));
				}
			}
			let mut block = (pre_len / BLOCK_SIZE as u64 + 1) * BLOCK_SIZE as u64;
			while block <= image_len {
				for d in 0..=6u64 {
					set.insert(block.saturating_sub(d).max(pre_len));
					set.insert((block + d).min(image_len));
				}
				block += BLOCK_SIZE as u64;
			}
			let mut c = pre_len;
			while c <= image_len {
				set.insert(c);
				c += 4_999;
			}
			set.into_iter().collect()
		}
	};

	// A record survives a cut if it ends before it, or, for a zeroed tail, if every byte of it
	// after the cut was a zero already (the last bytes of a batch with timestamp 0 and no value
	// pointer are zeros, so a record can survive a cut in its last two bytes).
	let image_bytes = fs::read(&image_segment).unwrap();
	let group_starts: Vec<u64> =
		std::iter::once(pre_len).chain(group_ends.iter().copied()).take(group.len()).collect();
	let survives = |i: usize, cut: u64, damage: Damage| match damage {
		Damage::Truncate => group_ends[i] <= cut,
		Damage::ZeroTail => {
			let from = cut.max(group_starts[i]).min(group_ends[i]);
			image_bytes[from as usize..group_ends[i] as usize].iter().all(|b| *b == 0)
		}
	};

	let work = base.path().join("work");
	for &cut in &offsets {
		if work.exists() {
			fs::remove_dir_all(&work).unwrap();
		}
		copy_dir(&image, &work);
		match damage {
			Damage::Truncate => set_len(&seg_path(&work, first), cut),
			Damage::ZeroTail => {
				let mut bytes = fs::read(seg_path(&work, first)).unwrap();
				bytes[cut as usize..].fill(0);
				fs::write(seg_path(&work, first), bytes).unwrap();
			}
		}
		let at = format!(
			"{damage:?}, left={left}, cut at {cut} of {image_len} (group starts at {pre_len})"
		);

		let tree = Tree::new(opts(&work, BIG)).unwrap();
		let whole: Vec<&(String, Vec<u8>)> = group
			.iter()
			.enumerate()
			.filter(|(i, _)| survives(*i, cut, damage))
			.map(|(_, g)| g)
			.collect();
		let n_whole = whole.len();
		assert!(
			(0..group.len()).all(|i| survives(i, cut, damage) == (i < n_whole)),
			"{damage:?} cut at {cut}: surviving records must be a prefix"
		);
		for i in 0..3 {
			assert_eq!(get(&tree, &format!("pre{i}")).as_deref(), Some(&[b'x'; 60][..]), "{at}");
		}
		assert_eq!(get(&tree, "leaver").as_deref(), Some(leaver.as_slice()), "{at}");
		for (i, (k, v)) in group.iter().enumerate() {
			let expect = survives(i, cut, damage);
			assert_eq!(
				get(&tree, k).as_deref(),
				expect.then_some(v.as_slice()),
				"{at}: record {i} is {} but was {}",
				if expect {
					"whole"
				} else {
					"cut"
				},
				if expect {
					"lost"
				} else {
					"recovered"
				}
			);
		}
		// The damaged segment holds the records that were whole and nothing a reader would take
		// for another one, and recovery continues in a fresh segment.
		let fresh = active_segment(&tree);
		assert!(fresh > first, "{at}: commits must not go to the damaged segment");
		assert_eq!(
			records_with_ends(&seg_path(&work, first)).len(),
			3 + 1 + whole.len(),
			"{at}: the damaged segment parses"
		);

		// New commits after the recovery: singles, then a group with a record that spans blocks.
		for j in 0..3 {
			put(&tree, &format!("post{j}"), &[b'p'; 70], Durability::Immediate).await;
		}
		let tree = Arc::new(tree);
		let more: Vec<(String, Vec<u8>)> = vec![
			("m0".to_string(), payload(1, 10)),
			("m1".to_string(), payload(2, 40_000)),
			("m2".to_string(), payload(3, 10)),
		];
		for r in commit_group(&tree, &more, Durability::Immediate).await {
			r.unwrap();
		}
		let tree = Arc::try_unwrap(tree).ok().expect("no other owner");
		crash(tree);

		assert_eq!(
			records_with_ends(&seg_path(&work, fresh)).len(),
			3 + 3,
			"{at}: the new commits are in the fresh segment"
		);
		assert_eq!(
			records_with_ends(&seg_path(&work, first)).len(),
			3 + 1 + whole.len(),
			"{at}: the damaged segment was not appended to"
		);
		let tree = Tree::new(opts(&work, BIG)).unwrap();
		for (k, v) in whole.iter().map(|g| (&g.0, &g.1)).chain(more.iter().map(|g| (&g.0, &g.1))) {
			assert_eq!(get(&tree, k).as_deref(), Some(v.as_slice()), "{at}: {k} after 2nd crash");
		}
		for j in 0..3 {
			assert_eq!(get(&tree, &format!("post{j}")).as_deref(), Some(&[b'p'; 70][..]), "{at}");
		}
		mark_closed(&tree);
		release_lock(&tree);
	}
}

#[test(tokio::test)]
async fn torn_group_every_byte_with_padding_before_the_group() {
	torn_group_sweep(3, &[40, 40, 40], Cuts::Every, Damage::Truncate).await;
}

#[test(tokio::test)]
async fn torn_group_every_byte_starting_a_block() {
	torn_group_sweep(0, &[40, 40], Cuts::Every, Damage::Truncate).await;
}

#[test(tokio::test)]
async fn torn_group_every_byte_with_exactly_a_header_left() {
	torn_group_sweep(7, &[40, 40], Cuts::Every, Damage::Truncate).await;
}

#[test(tokio::test)]
async fn torn_group_every_byte_with_one_byte_of_padding() {
	torn_group_sweep(1, &[40, 40], Cuts::Every, Damage::Truncate).await;
}

#[test(tokio::test)]
async fn torn_block_crossing_group_sampled_with_padding_before_it() {
	torn_group_sweep(5, &[20_000, 20_000, 20_000], Cuts::Sampled, Damage::Truncate).await;
}

#[test(tokio::test)]
async fn torn_block_crossing_group_sampled_starting_a_block() {
	torn_group_sweep(0, &[40_000, 5, 70_000, 5], Cuts::Sampled, Damage::Truncate).await;
}

#[test(tokio::test)]
async fn torn_block_crossing_group_sampled_with_a_header_left() {
	torn_group_sweep(7, &[33_000, 33_000], Cuts::Sampled, Damage::Truncate).await;
}

#[test(tokio::test)]
async fn zeroed_tail_of_a_group_every_byte_with_padding_before_the_group() {
	torn_group_sweep(3, &[40, 40], Cuts::Every, Damage::ZeroTail).await;
}

#[test(tokio::test)]
async fn zeroed_tail_of_a_group_every_byte_with_exactly_a_header_left() {
	torn_group_sweep(7, &[40, 40], Cuts::Every, Damage::ZeroTail).await;
}

#[test(tokio::test)]
async fn zeroed_tail_of_a_block_crossing_group_sampled() {
	torn_group_sweep(5, &[20_000, 20_000, 20_000], Cuts::Sampled, Damage::ZeroTail).await;
}

// ---------------------------------------------------------------------------
// the flusher: a failure anywhere in a big group, and the buffer it leaves behind
// ---------------------------------------------------------------------------

/// Through the real flusher, a group of large values fails at every possible write: every
/// waiter gets the error, the segment is byte-for-byte what it was before the group, nothing of
/// the group is readable, and the writer is poisoned. Replacing the writer (a rotation) lets
/// the next group through, and that group's segment holds exactly its own records: nothing of
/// the failed group (its buffer is dropped, not retained) and nothing of the group before it.
/// A restart recovers exactly the acknowledged commits.
#[test(tokio::test)]
async fn a_failed_big_group_through_the_flusher_leaves_the_acked_bytes_and_the_next_group_clean() {
	let lens = [40_000usize, 5, 33_000, 70_000, 7];
	let mut went_through = false;
	for fail_after in 0..200 {
		let dir = TempDir::new("wal_group_crash").unwrap();
		let live = dir.path().join("live");
		let tree = Arc::new(Tree::new(opts(&live, BIG)).unwrap());
		stop_background_tasks(&tree).await;

		let acked: Vec<(String, Vec<u8>)> = [100usize, 32_000, 20]
			.iter()
			.enumerate()
			.map(|(i, len)| (format!("acked{i}"), payload(i, *len)))
			.collect();
		for r in commit_group(&tree, &acked, Durability::Immediate).await {
			r.unwrap();
		}
		let first = active_segment(&tree);
		let segment = seg_path(&live, first);
		let acked_bytes = fs::read(&segment).unwrap();

		let doomed: Vec<(String, Vec<u8>)> = lens
			.iter()
			.enumerate()
			.map(|(i, len)| (format!("doomed{i}"), payload(40 + i, *len)))
			.collect();
		tree.core.inner.wal.write().fail_writes_after(fail_after);
		let results = commit_group(&tree, &doomed, Durability::Immediate).await;
		if results.iter().all(|r| r.is_ok()) {
			assert!(fail_after > 3, "the failpoint was not reached before {fail_after} ops");
			let strict = records_with_ends(&segment);
			assert_eq!(strict.len(), acked.len() + doomed.len());
			went_through = true;
			let tree = Arc::try_unwrap(tree).ok().expect("no other owner");
			crash(tree);
			break;
		}
		assert!(results.iter().all(|r| r.is_err()), "fail_after={fail_after}: {results:?}");
		assert_eq!(
			fs::read(&segment).unwrap(),
			acked_bytes,
			"fail_after={fail_after}: the segment must be what it was before the group"
		);
		for (k, _) in &doomed {
			assert_eq!(get(&tree, k), None, "fail_after={fail_after}: {k}");
		}
		// Poisoned: later groups fail too, and leave the segment alone.
		for round in 0..3 {
			let later = commit_group(
				&tree,
				&[(format!("later{round}"), payload(7, 50))],
				Durability::Immediate,
			)
			.await;
			assert!(later[0].is_err(), "fail_after={fail_after}: poisoned");
		}
		assert_eq!(fs::read(&segment).unwrap(), acked_bytes);

		// A rotation replaces the writer.
		tree.core.inner.rotate_memtable().unwrap();
		let again: Vec<(String, Vec<u8>)> = (0..3)
			.map(|i| (format!("again{i}"), payload(70 + i, [12usize, 9_000, 3][i])))
			.collect();
		for r in commit_group(&tree, &again, Durability::Immediate).await {
			r.unwrap();
		}
		let next = records_with_ends(&seg_path(&live, first + 1));
		let got: Vec<(String, Vec<u8>)> = next
			.iter()
			.map(|(_, b)| {
				let e = &b.entries[0];
				(
					String::from_utf8(e.key.clone()).unwrap(),
					ValueLocation::decode(e.value.as_ref().unwrap()).unwrap().value,
				)
			})
			.collect();
		assert_eq!(
			got, again,
			"fail_after={fail_after}: the next segment holds its own group only"
		);
		assert_eq!(fs::read(&segment).unwrap(), acked_bytes);

		let tree = Arc::try_unwrap(tree).ok().expect("no other owner");
		crash(tree);
		let reopened = Tree::new(opts(&live, BIG)).unwrap();
		for (k, v) in acked.iter().chain(&again) {
			assert_eq!(get(&reopened, k).as_deref(), Some(v.as_slice()), "fail_after={fail_after}");
		}
		for (k, _) in doomed.iter() {
			assert_eq!(get(&reopened, k), None, "fail_after={fail_after}: {k}");
		}
		mark_closed(&reopened);
		release_lock(&reopened);
	}
	assert!(went_through, "the group never went through, so there is no control");
}

/// The flusher keeps one set of buffers from group to group. Groups of every size in an order
/// that follows a large one with a tiny one (the large buffer is dropped, the tiny group must
/// not see any of its bytes), and a small one with a bigger one, committed with both
/// durabilities. The segment holds exactly the commits, in order, one record each, and is
/// byte-identical to the same records framed one `add_record` at a time.
#[test(tokio::test)]
async fn flusher_buffers_never_leak_bytes_between_groups_and_the_segment_is_the_per_record_framing()
{
	let dir = TempDir::new("wal_group_crash").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(opts(&live, BIG)).unwrap());
	let seen = Arc::new(Mutex::new(Vec::new()));
	{
		let seen = Arc::clone(&seen);
		tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
			if let PipelineHook::AfterWalSync {
				batches,
			} = point
			{
				seen.lock().unwrap().push(batches);
			}
		})));
	}

	// (commits in the group, value length): the 1.2 MB and 1.4 MB groups are within what the
	// flusher keeps buffers at, and are followed by tiny ones that must not see their bytes. The
	// 6 MB commit is a group of its own and past it, so its buffer is dropped and a tiny group
	// follows it too.
	let plan: [(usize, usize); 15] = [
		(64, 40),
		(1, 40),
		(3, 400_000),
		(1, 6_000_000),
		(1, 10),
		(200, 20),
		(2, 700_000),
		(1, 1),
		(5, 5),
		(7, 70_000),
		(1, 3),
		(32, 300),
		(4, 33_000),
		(1, 2),
		(9, 9),
	];
	let mut expected: Vec<(String, Vec<u8>)> = Vec::new();
	for (g, (n, len)) in plan.iter().enumerate() {
		let entries: Vec<(String, Vec<u8>)> =
			(0..*n).map(|i| (format!("g{g:02}_{i:03}"), payload(g * 1000 + i, *len))).collect();
		let durability = if g % 2 == 0 {
			Durability::Immediate
		} else {
			Durability::Eventual
		};
		for r in commit_group(&tree, &entries, durability).await {
			r.unwrap();
		}
		expected.extend(entries);
	}
	assert!(
		seen.lock().unwrap().iter().copied().max().unwrap() > 100,
		"some group must have been large: {:?}",
		seen.lock().unwrap()
	);

	let segment = seg_path(&live, active_segment(&tree));
	let records = records_with_ends(&segment);
	assert_eq!(records.len(), expected.len(), "one record per commit, no extra, none missing");
	let mut raw = Vec::new();
	{
		let mut reader = Reader::new(File::open(&segment).unwrap());
		while let Ok((rec, _)) = reader.read() {
			raw.push(rec.to_vec());
		}
	}
	for ((_, batch), (k, v)) in records.iter().zip(&expected) {
		assert_eq!(batch.entries.len(), 1);
		assert_eq!(&user_keys(batch)[0], k, "the order of the commits");
		let location = ValueLocation::decode(batch.entries[0].value.as_ref().unwrap()).unwrap();
		assert_eq!(&location.value, v, "{k}");
	}

	// The same records through `add_record`: the same bytes.
	let reference = dir.path().join("reference.wal");
	{
		let mut w = new_writer(&reference, CompressionType::None);
		for rec in &raw {
			w.add_record(rec).unwrap();
		}
	}
	assert_eq!(fs::read(&segment).unwrap(), fs::read(&reference).unwrap());

	// And a crash image recovers all of it.
	let image = dir.path().join("image");
	copy_dir(&live, &image);
	let recovered = Tree::new(opts(&image, BIG)).unwrap();
	for (k, v) in &expected {
		assert_eq!(get(&recovered, k).as_deref(), Some(v.as_slice()), "{k}");
	}
	mark_closed(&recovered);
	release_lock(&recovered);
	let tree = Arc::try_unwrap(tree).ok().expect("no other owner");
	crash(tree);
}

// ---------------------------------------------------------------------------
// the re-append path
// ---------------------------------------------------------------------------

/// Appends the same group the way the straddling-group test in `wal_rotation_tests` does: a
/// segment-filling prefill and then 24 commits as one group, which fills the arena part-way
/// and so is appended once and its tail appended again after the rotation.
async fn straddling_setup(path: &Path) -> (Arc<Tree>, Vec<(String, Vec<u8>)>, u64) {
	let tree = Arc::new(Tree::new(opts(path, 4096)).unwrap());
	stop_background_tasks(&tree).await;
	let prefill: Vec<(String, Vec<u8>)> =
		(0..10).map(|i| (format!("pre{i:03}"), payload(i, 100))).collect();
	for (k, v) in &prefill {
		put(&tree, k, v, Durability::Immediate).await;
	}
	let first = tree.core.inner.active_memtable.read().unwrap().get_wal_number();
	(tree, prefill, first)
}

fn straddling_group() -> Vec<(String, Vec<u8>)> {
	(0..24).map(|i| (format!("grp{i:03}"), payload(100 + i, 100))).collect()
}

/// Where the failure of a straddling group's appends is injected.
#[derive(Clone, Copy, Debug)]
enum Inject {
	/// Into the first append (the whole group), before the group starts.
	FirstAppend,
	/// Into the second append (the stale tail, logged again in the new segment after the
	/// rotation): the failpoint is armed on the new segment's writer when the repair round
	/// starts.
	SecondAppend,
}

/// A failure at every write of the two appends a straddling group makes (the whole group, then
/// its tail after the rotation): the group fails as a unit, a failure in the first append
/// leaves the first segment as it was and a failure in the second leaves the first segment
/// holding the whole group and the second empty, and a record is never in a segment twice. A
/// restart recovers every acknowledged commit; a group that failed in the second append is
/// recovered whole (its first append was durable) and one that failed in the first not at all.
async fn straddling_failure_sweep(inject: Inject) {
	let mut went_through = false;
	let mut failed_in_second = 0;
	for fail_after in 0..400 {
		let dir = TempDir::new("wal_group_crash").unwrap();
		let live = dir.path().join("live");
		let (tree, prefill, first) = straddling_setup(&live).await;
		let pre_bytes = fs::read(seg_path(&live, first)).unwrap();
		let group = straddling_group();

		match inject {
			Inject::FirstAppend => tree.core.inner.wal.write().fail_writes_after(fail_after),
			Inject::SecondAppend => {
				let inner = Arc::clone(&tree.core.inner);
				let armed = Arc::new(Mutex::new(false));
				tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
					if let PipelineHook::BeforeApplyRound {
						round: 1,
					} = point
					{
						if !std::mem::replace(&mut *armed.lock().unwrap(), true) {
							inner.wal.write().fail_writes_after(fail_after);
						}
					}
				})));
			}
		}
		let results = commit_group(&tree, &group, Durability::Immediate).await;
		if results.iter().all(|r| r.is_ok()) {
			assert!(
				fail_after > 3,
				"{inject:?}: the failpoint was not reached before {fail_after}"
			);
			went_through = true;
			let tree = Arc::try_unwrap(tree).ok().expect("no other owner");
			crash(tree);
			break;
		}
		assert!(results.iter().all(|r| r.is_err()), "{inject:?} {fail_after}: {results:?}");

		let ids = segment_ids(&live);
		let first_records = records_with_ends(&seg_path(&live, first));
		let in_first: Vec<String> = first_records.iter().flat_map(|(_, b)| user_keys(b)).collect();
		let group_keys: Vec<String> = group.iter().map(|(k, _)| k.clone()).collect();
		let second_failed = ids.contains(&(first + 1));
		if second_failed {
			failed_in_second += 1;
			assert_eq!(
				first_records.len(),
				prefill.len() + group.len(),
				"{inject:?} {fail_after}: the first append was whole"
			);
			assert_eq!(
				fs::metadata(seg_path(&live, first + 1)).unwrap().len(),
				0,
				"{inject:?} {fail_after}: a failed re-append leaves the new segment empty"
			);
			assert!(in_first.ends_with(&group_keys));
		} else {
			assert_eq!(
				fs::read(seg_path(&live, first)).unwrap(),
				pre_bytes,
				"{inject:?} {fail_after}: a failed first append leaves the segment as it was"
			);
		}
		// Never twice in a segment.
		for id in &ids {
			let mut keys: Vec<String> = records_with_ends(&seg_path(&live, *id))
				.iter()
				.flat_map(|(_, b)| user_keys(b))
				.collect();
			let n = keys.len();
			keys.sort();
			keys.dedup();
			assert_eq!(keys.len(), n, "{inject:?} {fail_after}: a record twice in segment {id}");
		}
		// The writer that failed is poisoned: nothing more is logged.
		let later =
			commit_group(&tree, &[("later".to_string(), payload(9, 50))], Durability::Immediate)
				.await;
		assert!(later[0].is_err(), "{inject:?} {fail_after}: poisoned");

		let tree = Arc::try_unwrap(tree).ok().expect("no other owner");
		crash(tree);
		let reopened = Tree::new(opts(&live, 4096)).unwrap();
		for (k, v) in &prefill {
			assert_eq!(get(&reopened, k).as_deref(), Some(v.as_slice()), "{inject:?} {fail_after}");
		}
		// Whole or nothing: a failed group is never half recovered.
		let recovered =
			group.iter().filter(|(k, v)| get(&reopened, k).as_deref() == Some(v)).count();
		assert!(
			recovered == 0 || recovered == group.len(),
			"{inject:?} {fail_after}: {recovered} of {} recovered",
			group.len()
		);
		assert_eq!(recovered == group.len(), second_failed, "{inject:?} {fail_after}");
		assert_eq!(get(&reopened, "later"), None);
		mark_closed(&reopened);
		release_lock(&reopened);
	}
	assert!(went_through, "{inject:?}: the group never went through, so there is no control");
	match inject {
		Inject::FirstAppend => assert_eq!(failed_in_second, 0),
		Inject::SecondAppend => assert!(failed_in_second > 0, "no failure landed in the re-append"),
	}
}

#[test(tokio::test)]
async fn a_failure_in_the_first_append_of_a_straddling_group_fails_it_as_a_unit() {
	straddling_failure_sweep(Inject::FirstAppend).await;
}

#[test(tokio::test)]
async fn a_failure_in_the_re_append_of_a_straddling_group_fails_it_as_a_unit() {
	straddling_failure_sweep(Inject::SecondAppend).await;
}

/// Every apply round of an Immediate group starts with nothing appended since the last fsync:
/// at the group's WAL sync and at the start of every later round, which includes the rounds
/// that follow a rotation and a re-append of stale records, a direct-to-L0 write, and a
/// rotation by someone else. The Eventual control proves the probe can see an unsynced append.
#[test(tokio::test)]
async fn immediate_groups_are_fsynced_before_apply_on_every_path() {
	for (name, foreign_rotation, oversized_in_the_middle) in [
		("straddle", false, false),
		("foreign rotation", true, false),
		("direct to L0 mid group", false, true),
	] {
		for durability in [Durability::Immediate, Durability::Eventual] {
			let dir = TempDir::new("wal_group_crash").unwrap();
			let live = dir.path().join("live");
			let (tree, _prefill, _first) = straddling_setup(&live).await;
			let inner = Arc::clone(&tree.core.inner);

			let unsynced = Arc::new(Mutex::new(Vec::new()));
			let rounds = Arc::new(Mutex::new(0usize));
			{
				let (inner, unsynced, rounds) =
					(Arc::clone(&inner), Arc::clone(&unsynced), Arc::clone(&rounds));
				let rotated = Arc::new(Mutex::new(false));
				tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
					let wal = inner.wal.read();
					match point {
						PipelineHook::AfterWalSync {
							..
						} => {
							unsynced.lock().unwrap().push(wal.pending_sync());
							drop(wal);
							if foreign_rotation
								&& !std::mem::replace(&mut *rotated.lock().unwrap(), true)
							{
								inner.rotate_memtable().unwrap();
							}
						}
						PipelineHook::BeforeApplyRound {
							..
						} => {
							*rounds.lock().unwrap() += 1;
							unsynced.lock().unwrap().push(wal.pending_sync());
						}
						PipelineHook::BeforeFencedApply => {}
					}
				})));
			}

			let mut group = straddling_group();
			if oversized_in_the_middle {
				group.truncate(6);
				group[2].1 = vec![b'O'; 4096 + 600];
			} else if foreign_rotation {
				group.truncate(6);
			}
			for r in commit_group(&tree, &group, durability).await {
				r.unwrap();
			}
			let seen = unsynced.lock().unwrap().clone();
			assert!(*rounds.lock().unwrap() >= 2, "{name}: the group was repaired at least once");
			match durability {
				Durability::Immediate => assert!(
					seen.iter().all(|pending| !pending),
					"{name}: appended data was not fsynced when the group moved on: {seen:?}"
				),
				Durability::Eventual => assert!(
					seen.iter().any(|pending| *pending),
					"{name}: control: an Eventual group leaves data unsynced: {seen:?}"
				),
			}
			let tree = Arc::try_unwrap(tree).ok().expect("no other owner");
			crash(tree);
		}
	}
}

// ---------------------------------------------------------------------------
// concurrent writers and rotations: no record twice in a segment, sequence order kept
// ---------------------------------------------------------------------------

/// Writers commit Immediate singles while another task keeps rotating the memtable and a
/// small arena fills by itself. Whatever interleaving: no segment holds a record twice, the
/// sequence numbers in a segment only ever increase (re-appended records follow the group they
/// belong to), and a crash image recovers every acknowledged commit.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn racing_writers_and_rotations_never_log_a_record_twice_into_one_segment() {
	let dir = TempDir::new("wal_group_crash").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(opts(&live, 8192)).unwrap());
	stop_background_tasks(&tree).await;

	let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
	let rotator = {
		let (inner, stop) = (Arc::clone(&tree.core.inner), Arc::clone(&stop));
		tokio::task::spawn_blocking(move || {
			let mut n = 0u32;
			while !stop.load(Ordering::Relaxed) {
				inner.rotate_memtable().unwrap();
				n += 1;
				std::thread::sleep(std::time::Duration::from_micros(300));
			}
			n
		})
	};
	let mut writers = Vec::new();
	for w in 0..6usize {
		let tree = Arc::clone(&tree);
		writers.push(tokio::spawn(async move {
			let mut acked = Vec::new();
			for i in 0..120usize {
				let key = format!("w{w}_{i:04}");
				let value = payload(w * 1000 + i, 60 + (i % 7) * 200);
				let mut txn = tree.begin().unwrap();
				txn.set_durability(Durability::Immediate);
				txn.set(key.as_bytes(), &value).unwrap();
				if txn.commit().await.is_ok() {
					acked.push((key, value));
				}
			}
			acked
		}));
	}
	let mut acked = Vec::new();
	for w in writers {
		acked.extend(w.await.unwrap());
	}
	stop.store(true, Ordering::Relaxed);
	let rotations = rotator.await.unwrap();
	assert!(rotations > 5, "only {rotations} rotations happened");
	assert_eq!(acked.len(), 6 * 120, "no commit may fail in this test");

	for id in segment_ids(&live) {
		let records = records_with_ends(&seg_path(&live, id));
		let mut seqs: Vec<u64> = records.iter().map(|(_, b)| b.starting_seq_num).collect();
		let n = seqs.len();
		assert!(
			seqs.windows(2).all(|w| w[0] < w[1]),
			"segment {id}: sequence numbers must increase: {seqs:?}"
		);
		seqs.dedup();
		assert_eq!(seqs.len(), n, "segment {id}: a record twice");
	}

	let image = dir.path().join("image");
	copy_dir(&live, &image);
	let recovered = Tree::new(opts(&image, 8192)).unwrap();
	for (k, v) in &acked {
		assert_eq!(get(&recovered, k).as_deref(), Some(v.as_slice()), "{k} after a crash");
	}
	mark_closed(&recovered);
	release_lock(&recovered);
	mark_closed(&tree);
	release_lock(&tree);
}

// ---------------------------------------------------------------------------
// differential: the segment a random commit sequence writes
// ---------------------------------------------------------------------------

/// Runs a fixed pseudo-random sequence of commit groups (1 to 12 batches of 1 to 3 entries,
/// sizes on both sides of every block boundary, a random mix of syncing and not) through
/// `flush_group` and returns the length, CRC-32 and XXH3 of the segment. Timestamps are 0, so
/// the bytes depend on nothing else.
async fn random_sequence_digest() -> (u64, u32, u64) {
	let dir = TempDir::new("wal_group_crash").unwrap();
	let tree = Tree::new(opts(dir.path(), BIG)).unwrap();
	let mut rng = Rng(0xD1FF_E4E7_1A11_0001);
	for g in 0..150usize {
		let batches: Vec<Batch> = (0..1 + rng.below(12))
			.map(|b| {
				let entries: Vec<(String, Vec<u8>)> = (0..1 + rng.below(3))
					.map(|e| {
						let len = biased_len(&mut rng).min(60_000);
						(format!("k{g:03}_{b:02}_{e}"), payload(g * 100 + b * 10 + e, len))
					})
					.collect();
				let refs: Vec<(&str, &[u8])> =
					entries.iter().map(|(k, v)| (k.as_str(), v.as_slice())).collect();
				raw_batch(&tree, &refs)
			})
			.collect();
		let sync = rng.below(2) == 0;
		tree.core.commit_pipeline.flush_group(&batches, sync).await.unwrap();
	}
	let bytes = fs::read(seg_path(dir.path(), active_segment(&tree))).unwrap();
	let digest = (bytes.len() as u64, crc32fast::hash(&bytes), xxhash_rust::xxh3::xxh3_64(&bytes));
	mark_closed(&tree);
	release_lock(&tree);
	digest
}

/// The digest below was captured from the tree that appends a group one `Wal::append` per
/// batch (Phase 0 without the group append): a coalesced group must write the same bytes.
#[test(tokio::test)]
async fn a_random_commit_sequence_writes_the_bytes_the_per_batch_path_wrote() {
	let digest = random_sequence_digest().await;
	println!("RANDOM_SEQUENCE_DIGEST={digest:?}");
	assert_eq!(digest, RANDOM_SEQUENCE_DIGEST);
}

/// Captured from the pre-G tree (Phase 0 alone, `Wal::append` per batch): 46 MB over 150 groups.
const RANDOM_SEQUENCE_DIGEST: (u64, u32, u64) =
	(46_173_282, 3_409_075_246, 2_779_549_630_035_894_836);
