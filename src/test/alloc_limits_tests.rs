//! A single enormous batch neither aborts the process nor overflows a length field.
//!
//! The allocations that a batch of any size reaches before its WAL record is written (wrapping
//! a value, compressing a record) return an error when the allocator refuses them, instead of
//! aborting: the commit fails, nothing reaches the segment and the writer is not poisoned. The
//! tests make the allocator of the test thread refuse the size of one allocation, so nothing of
//! that size ever has to exist.
//!
//! A batch is also refused up front, by `Batch::grow`, when its encoding would not fit what one
//! WAL record holds. The boundaries are reached by setting `Batch::size`, which `grow` tracks,
//! instead of by building gigabytes.
//!
//! A batch that a memtable cannot hold is written to an L0 table after its record is logged,
//! which borrows its values; one test pins that no second copy of a value is made.

use std::fs::File;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use tempdir::TempDir;

use super::compaction_memory_proofs::{count_allocations, RefuseAllocations};
use super::wal_group_budget_tests::{
	assert_recovers,
	commit as spawn_commit,
	mark_closed,
	options,
	outcome,
	release_lock,
	value_of,
	wal_bytes,
	WATCHDOG,
};
use crate::batch::{Batch, ENCODED_ENTRY_SLACK, ENCODED_HEADER_SIZE, MAX_BATCH_SIZE};
use crate::varint::{put_varint_u32, put_varint_u64, varint_len_u64};
use crate::vlog::entry_lens;
use crate::wal::reader::Reader;
use crate::wal::writer::Writer;
use crate::wal::{
	segment_name,
	BufferedFileWriter,
	CompressionType,
	Error as WalError,
	RecordType,
	BLOCK_SIZE,
	HEADER_SIZE,
};
use crate::{Error, InternalKeyKind, Options, Tree};

// ---------------------------------------------------------------------------
// the allocations of a commit
// ---------------------------------------------------------------------------

async fn commit(tree: &Tree, key: &str, value: &[u8]) -> crate::Result<()> {
	let mut txn = tree.begin().unwrap();
	// `&[u8]` is copied into a vector that has no room to spare, as most values arrive.
	txn.set(key.as_bytes(), value).unwrap();
	tokio::time::timeout(WATCHDOG, txn.commit()).await.expect("a commit hung")
}

/// The bytes of the WAL segment that the tree appends to now. A tree always continues in a
/// segment of its own after it is opened, so a test that wants the segment its commits go to asks
/// the WAL for it and does not name one.
fn active_segment(tree: &Tree, live: &Path) -> Vec<u8> {
	let number = tree.core.inner.wal.read().get_active_log_number();
	std::fs::read(live.join("wal").join(segment_name(number, "wal"))).unwrap()
}

/// The flusher grows a value by the two bytes that wrap it, which allocates when the value has no
/// room to spare. When the allocator refuses, the commit fails with an error, nothing of it
/// reaches the WAL and the commits after it go through.
#[tokio::test]
async fn a_value_the_allocator_cannot_grow_fails_its_commit_and_leaves_the_wal_alone() {
	const LEN: usize = 1 << 20;
	let dir = TempDir::new("alloc_limits").unwrap();
	let live = dir.path().join("live");
	let tree = Tree::new(options(&live, false)).unwrap();

	let kept = ("kept".to_string(), value_of(0, 200));
	let empty = active_segment(&tree, &live).len();
	commit(&tree, &kept.0, &kept.1).await.unwrap();
	let segment = wal_bytes(&live);
	let active = active_segment(&tree, &live);
	assert!(active.len() >= empty + HEADER_SIZE + 200, "control: the active segment holds it");

	// The one allocation of `LEN + 2` bytes is the grown value.
	let value = value_of(1, LEN);
	let refusal = RefuseAllocations::of_size(LEN + 2..=LEN + 2);
	let result = commit(&tree, "doomed", &value).await;
	drop(refusal);
	let Err(Error::Io(error)) = &result else {
		panic!("the commit fails with an I/O error, got {result:?}");
	};
	assert!(error.to_string().contains("out of memory"), "{error}");
	assert!(wal_bytes(&live) == segment, "the segments are byte-identical");
	assert!(active_segment(&tree, &live) == active, "the active segment is byte-identical");

	// The same commit goes through once the allocator gives the bytes, and so do others.
	let later: Vec<(String, Vec<u8>)> =
		(0..3).map(|i| (format!("later_{i}"), value_of(10 + i, 1000))).collect();
	for (key, value) in &later {
		commit(&tree, key, value).await.unwrap();
	}
	commit(&tree, "doomed", &value).await.unwrap();

	let mut all = vec![kept];
	all.extend(later);
	all.push(("doomed".to_string(), value));
	assert_recovers(&live, &dir.path().join("image"), false, &all, &[], "after the refusal");
	mark_closed(&tree);
	release_lock(&tree);
}

// ---------------------------------------------------------------------------
// compressing a record
// ---------------------------------------------------------------------------

fn new_writer(path: &Path, compression: CompressionType) -> Writer {
	let buffered = BufferedFileWriter::new(File::create(path).unwrap(), BLOCK_SIZE);
	let mut writer = Writer::new(buffered, false, compression, 0);
	writer.add_compression_type_record().unwrap();
	writer
}

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

/// A record that cannot be compressed for want of memory is refused before the append is
/// guarded: the segment is byte-identical and the writer is not poisoned, so the records after it
/// are logged. Both entry points compress the same way.
#[test]
fn a_record_the_allocator_cannot_compress_is_refused_and_the_writer_stays_usable() {
	const LEN: usize = 1 << 20;
	let dir = TempDir::new("alloc_limits").unwrap();
	let path = dir.path().join("lz4.wal");
	let mut writer = new_writer(&path, CompressionType::Lz4);
	let small = vec![b'a'; 100];
	writer.add_records(&small, &[small.len()]).unwrap();
	let before = std::fs::read(&path).unwrap();

	// Incompressible, so its block is as large as the record and needs `LEN` bytes at least.
	let huge: Vec<u8> = value_of(1, LEN);
	let group = [&small[..], &huge[..]].concat();
	let ends = [small.len(), group.len()];
	{
		// The block takes about `LEN + LEN / 255` bytes, which is below the buffer that the abort
		// handler of the process needs to print its message.
		let _refusal = RefuseAllocations::of_size(LEN..=LEN + LEN / 4);
		assert!(writer.add_records(&group, &ends).is_err(), "a group with a record too big");
		assert!(writer.add_record(&huge).is_err(), "a single record");
	}
	assert!(std::fs::read(&path).unwrap() == before, "the segment is byte-identical");

	let more = vec![b'b'; 100];
	writer.add_records(&more, &[more.len()]).unwrap();
	writer.add_record(&huge).unwrap();
	assert_eq!(read_records(&path), [small, more, huge], "the records after the refusal");
}

// ---------------------------------------------------------------------------
// the size of a batch
// ---------------------------------------------------------------------------

/// A batch that would encode to more than one WAL record holds is refused by `grow`, for the
/// bytes of the header and of every entry that the tracked size does not count.
#[test]
fn a_batch_whose_encoding_would_not_fit_a_wal_record_is_refused_up_front() {
	// kind, key length, key, value length, value and timestamp, as `Batch::grow` counts them.
	const RECORD: u64 = 1 + 1 + 1 + 1 + 10 + 8;
	// What an encoded batch of one entry has over the tracked size, at most.
	const OVERHEAD: u64 = ENCODED_HEADER_SIZE + ENCODED_ENTRY_SLACK;
	let last_fit = MAX_BATCH_SIZE - RECORD - OVERHEAD;
	for (size, fits) in
		[(last_fit - 1, true), (last_fit, true), (last_fit + 1, false), (MAX_BATCH_SIZE, false)]
	{
		let mut batch = Batch::new(0);
		batch.size = size;
		let result = batch.add_record(InternalKeyKind::Set, b"k".to_vec(), Some(vec![0; 10]), 0);
		assert_eq!(result.is_ok(), fits, "a batch of {size} bytes takes a record of {RECORD}");
		if let Err(e) = result {
			assert!(matches!(e, Error::BatchTooLarge), "got {e:?}");
			assert_eq!(batch.size, size, "a refused record is not counted");
			assert!(batch.entries.is_empty(), "and not added");
		}
	}
}

/// What the limit counts on is true: no batch encodes to more than its tracked size, the header
/// and the slack per entry, even with the longest varints, which every timestamp and the
/// sequence number of this one have.
#[test]
fn the_bound_on_the_encoded_length_covers_the_longest_varints() {
	for entries in [1usize, 2, 3, 127, 128, 1000] {
		let mut batch = Batch::new(u64::MAX);
		for i in 0..entries {
			let key = vec![b'k'; 1 + i % 200];
			let value = (i % 3 != 0).then(|| vec![7u8; (i * 37) % 20_000]);
			let kind = if value.is_some() {
				InternalKeyKind::Set
			} else {
				InternalKeyKind::Delete
			};
			batch.add_record(kind, key, value, u64::MAX).unwrap();
		}
		let encoded = batch.encode().unwrap();
		let hint = batch.encoded_len_hint();
		assert!(encoded.len() <= hint, "{entries} entries: {} bytes, hint {hint}", encoded.len());
		assert!(!batch.exceeds_max_size());
		assert!(
			hint - encoded.len() <= ENCODED_HEADER_SIZE as usize,
			"{entries} entries: the entries' slack is exact for the longest varints"
		);
	}
}

/// A key or a value that the header and the pointer of a value log entry cannot say the length of
/// is refused, where a cast would store a length that wrapped around. Checked on the lengths
/// themselves, as the entry would need gigabytes.
#[test]
fn a_value_log_entry_whose_lengths_do_not_fit_a_u32_is_refused() {
	let max = u32::MAX as usize;
	assert_eq!(entry_lens(0, 0).unwrap(), (0, 0));
	assert_eq!(entry_lens(1, max).unwrap(), (1, u32::MAX));
	assert_eq!(entry_lens(max, 1).unwrap(), (u32::MAX, 1));
	#[cfg(target_pointer_width = "64")]
	for (key, value) in [(max + 1, 0), (0, max + 1), (max + 1, max + 1), (1, 1 << 40)] {
		let result = entry_lens(key, value);
		assert!(matches!(result, Err(Error::InvalidArgument(_))), "got {result:?}");
	}
}

// ---------------------------------------------------------------------------
// the limit and its slack
// ---------------------------------------------------------------------------

/// The header and the slack of an entry are the longest encodings of what they stand for, so
/// that the bound holds for the batches that the longest varints make. A batch at the limit
/// takes the longest entry count there is room for, which is checked next to the constant.
#[test]
fn the_slack_is_the_longest_varints() {
	let len64 = |value: u64| {
		let mut buf = Vec::new();
		put_varint_u64(&mut buf, value);
		buf.len() as u64
	};
	let len32 = |value: u32| {
		let mut buf = Vec::new();
		put_varint_u32(&mut buf, value);
		buf.len() as u64
	};
	// The most entries a batch at the limit holds: each takes 11 bytes at the least.
	let most_entries = (MAX_BATCH_SIZE / 11) as u32;
	assert_eq!(len32(most_entries), len32(u32::MAX), "a batch can have the longest count");
	assert_eq!(ENCODED_HEADER_SIZE, 1 + len64(u64::MAX) + len32(u32::MAX));
	// `Batch::grow` counts 8 bytes for the timestamp, and none for the value pointer flag.
	assert_eq!(ENCODED_ENTRY_SLACK, len64(u64::MAX) - 8 + 1);
}

/// `Batch::grow` counts the slack of every entry, the new one included, and
/// `Batch::exceeds_max_size` counts it the same way: the boundary moves with the entry count.
#[test]
fn the_limit_counts_the_slack_of_every_entry() {
	// kind, key length, key, value length, value and timestamp, as `Batch::grow` counts them.
	const RECORD: u64 = 1 + 1 + 1 + 1 + 10 + 8;
	for entries in [0usize, 1, 2, 10, 1000] {
		let last_fit = MAX_BATCH_SIZE
			- RECORD
			- ENCODED_HEADER_SIZE
			- ENCODED_ENTRY_SLACK * (entries as u64 + 1);
		for (size, fits) in [(last_fit, true), (last_fit + 1, false)] {
			let mut batch = Batch::new(0);
			for i in 0..entries {
				batch
					.add_record(InternalKeyKind::Set, vec![b'k'], Some(vec![0; 10]), i as u64)
					.unwrap();
			}
			batch.size = size;
			let result = batch.add_record(InternalKeyKind::Set, vec![b'k'], Some(vec![0; 10]), 0);
			assert_eq!(result.is_ok(), fits, "{entries} entries before a batch of {size} bytes");
			if let Err(e) = result {
				assert!(matches!(e, Error::BatchTooLarge), "got {e:?}");
				assert_eq!(batch.entries.len(), entries, "a refused record is not added");
			} else {
				assert!(!batch.exceeds_max_size(), "{entries} entries: what grow let in fits");
				batch.size += 1;
				assert!(batch.exceeds_max_size(), "{entries} entries: one byte more does not");
			}
		}
	}
}

/// What `test_batch_max_size` does with a value of nearly 4 GiB, on the sizes alone: a record of
/// nearly the limit is taken, and the next one is refused.
#[test]
fn a_record_of_nearly_the_limit_is_taken_and_the_next_one_refused() {
	let record = |key: usize, value: usize| {
		(1 + varint_len_u64(key as u64) + key + varint_len_u64(value as u64) + value + 8) as u64
	};
	let mut batch = Batch::new(0);
	batch.grow(record(1000, MAX_BATCH_SIZE as usize - 2000)).unwrap();
	assert!(matches!(batch.grow(record(1000, 1)), Err(Error::BatchTooLarge)));
}

// ---------------------------------------------------------------------------
// the LZ4 step of the writer
// ---------------------------------------------------------------------------

/// Records of every kind of size: tiny, a few that fit a block, some that fragment over several,
/// compressible and not, and in a few groups of different sizes.
fn records() -> Vec<Vec<u8>> {
	let mut records = Vec::new();
	for (i, len) in [1usize, 5, 100, 70_000, 33_000, 2, 32_760, 300].into_iter().enumerate() {
		records.push(if i % 2 == 0 {
			value_of(i, len)
		} else {
			vec![b'a' + i as u8; len]
		});
	}
	records
}

fn add_in_groups(writer: &mut Writer, records: &[Vec<u8>], group: usize) {
	for chunk in records.chunks(group) {
		let buf = chunk.concat();
		let ends: Vec<usize> = chunk
			.iter()
			.scan(0, |end, record| {
				*end += record.len();
				Some(*end)
			})
			.collect();
		writer.add_records(&buf, &ends).unwrap();
	}
}

/// A compressed group of several records reads back record by record, whatever the grouping,
/// and the single-record call writes what a group of one does.
#[test]
fn a_compressed_group_of_several_records_reads_back_record_by_record() {
	let dir = TempDir::new("alloc_limits").unwrap();
	let records = records();
	for group in [1, 2, 3, records.len()] {
		let path = dir.path().join(format!("group_{group}.wal"));
		let mut writer = new_writer(&path, CompressionType::Lz4);
		add_in_groups(&mut writer, &records, group);
		assert_eq!(read_records(&path), records, "groups of {group}");
	}
	let single = dir.path().join("single.wal");
	let mut writer = new_writer(&single, CompressionType::Lz4);
	for record in &records {
		writer.add_record(record).unwrap();
	}
	assert_eq!(read_records(&single), records);
	assert_eq!(
		std::fs::read(&single).unwrap(),
		std::fs::read(dir.path().join("group_1.wal")).unwrap(),
		"a record added alone is framed as a group of one"
	);
}

/// The allocations of one `add_records` call, after a first call that sizes whatever the writer
/// keeps.
fn allocations_of_a_group(compression: CompressionType, buf: &[u8], ends: &[usize]) -> u64 {
	let dir = TempDir::new("alloc_limits").unwrap();
	let mut writer = new_writer(&dir.path().join("count.wal"), compression);
	writer.add_records(buf, ends).unwrap();
	let (result, allocations) = count_allocations(|| writer.add_records(buf, ends));
	result.unwrap();
	allocations
}

/// The allocations `lz4_flex` makes of its own for a record (its hash table), which are not the
/// writer's to take.
fn allocations_of_lz4_flex(record: &[u8]) -> u64 {
	let mut out = vec![0; lz4_flex::block::get_maximum_output_size(record.len())];
	let (result, allocations) =
		count_allocations(|| lz4_flex::block::compress_into(record, &mut out));
	result.unwrap();
	allocations
}

/// Compressing a group takes the room for its blocks in one allocation and the room for where
/// they end in another, however many records it has, and `lz4_flex` takes what it needs for each
/// record: nothing else in the compression grows a vector, which would be an allocation that
/// cannot fail cleanly.
#[test]
fn compressing_a_group_allocates_its_room_once() {
	for count in [1usize, 2, 8, 40] {
		let records: Vec<Vec<u8>> = (0..count)
			.map(|i| {
				if i % 2 == 0 {
					value_of(i, 3000)
				} else {
					vec![7u8; 3000]
				}
			})
			.collect();
		let buf = records.concat();
		let ends: Vec<usize> = (1..=count).map(|i| i * 3000).collect();
		let plain = allocations_of_a_group(CompressionType::None, &buf, &ends);
		let lz4 = allocations_of_a_group(CompressionType::Lz4, &buf, &ends);
		let theirs: u64 = records.iter().map(|record| allocations_of_lz4_flex(record)).sum();
		assert_eq!(
			lz4,
			plain + 2 + theirs,
			"{count} records: the blocks, where they end, lz4_flex"
		);
	}
}

/// The room for where the blocks end is reserved fallibly too.
#[test]
fn a_group_whose_block_ends_cannot_be_allocated_is_refused() {
	// 37 records of 8 bytes each: the ends are 37 words, which nothing else here allocates.
	const RECORDS: usize = 37;
	let dir = TempDir::new("alloc_limits").unwrap();
	let path = dir.path().join("ends.wal");
	let mut writer = new_writer(&path, CompressionType::Lz4);
	let before = std::fs::read(&path).unwrap();
	let records: Vec<Vec<u8>> = (0..RECORDS).map(|i| value_of(i, 8)).collect();
	let buf = records.concat();
	let ends: Vec<usize> = (1..=RECORDS).map(|i| i * 8).collect();
	{
		let _refusal = RefuseAllocations::of_size(
			RECORDS * std::mem::size_of::<usize>()..=RECORDS * std::mem::size_of::<usize>(),
		);
		assert!(writer.add_records(&buf, &ends).is_err());
	}
	assert!(std::fs::read(&path).unwrap() == before, "the segment is byte-identical");
	writer.add_records(&buf, &ends).unwrap();
	assert_eq!(read_records(&path), records, "the writer is not poisoned");
}

// ---------------------------------------------------------------------------
// a group with a refused neighbour
// ---------------------------------------------------------------------------

/// The batches of a group are wrapped one after the other. When a later value cannot be, the
/// whole group fails before it is logged: the neighbours that were wrapped, or are yet to be,
/// are in the WAL no more than the one that failed, and the same group goes through afterwards.
#[tokio::test]
async fn a_group_with_a_value_the_allocator_cannot_grow_logs_none_of_it() {
	const LEN: usize = 1 << 20;
	let dir = TempDir::new("alloc_limits").unwrap();
	let live = dir.path().join("live");
	let tree = Tree::new(options(&live, false)).unwrap();
	let pipeline = &tree.core.commit_pipeline;
	let batch = |entries: &[(&str, &[u8])]| {
		let start = pipeline
			.log_seq_num
			.fetch_add(entries.len() as u64, std::sync::atomic::Ordering::SeqCst);
		let mut batch = Batch::new(start);
		for (key, value) in entries {
			batch
				.add_record(InternalKeyKind::Set, key.as_bytes().to_vec(), Some(value.to_vec()), 0)
				.unwrap();
		}
		batch
	};

	let kept = ("kept".to_string(), value_of(0, 200));
	let empty = active_segment(&tree, &live).len();
	pipeline.flush_group(&[batch(&[(&kept.0, &kept.1)])], true).await.unwrap();
	let segment = wal_bytes(&live);
	let active = active_segment(&tree, &live);
	assert!(active.len() >= empty + HEADER_SIZE + 200, "control: the active segment holds it");

	let first = ("first".to_string(), value_of(1, 300));
	let doomed = ("doomed".to_string(), value_of(2, LEN));
	let last = ("last".to_string(), value_of(3, 400));
	let group = [
		batch(&[(&first.0, &first.1)]),
		batch(&[(&doomed.0, &doomed.1)]),
		batch(&[(&last.0, &last.1)]),
	];
	let refusal = RefuseAllocations::of_size(LEN + 2..=LEN + 2);
	let result = pipeline.flush_group(&group, true).await;
	drop(refusal);
	let Err(Error::Io(error)) = &result else {
		panic!("the group fails with an I/O error, got {result:?}");
	};
	assert_eq!(error.kind(), std::io::ErrorKind::OutOfMemory, "{error}");
	assert!(wal_bytes(&live) == segment, "the segments are byte-identical");
	assert!(active_segment(&tree, &live) == active, "the active segment is byte-identical");
	assert_recovers(
		&live,
		&dir.path().join("image_failed"),
		false,
		std::slice::from_ref(&kept),
		&[first.0.clone(), doomed.0.clone(), last.0.clone()],
		"after the refused group",
	);

	// The group goes through once the allocator gives the bytes.
	pipeline.flush_group(&group, true).await.unwrap();
	assert_recovers(
		&live,
		&dir.path().join("image_retried"),
		false,
		&[kept, first, doomed, last],
		&[],
		"after the group is retried",
	);
	mark_closed(&tree);
	release_lock(&tree);
}

// ---------------------------------------------------------------------------
// an LZ4 segment through the commit pipeline
// ---------------------------------------------------------------------------

/// A tree whose active WAL segment is compressed, and the number of that segment: after a first
/// commit, the WAL is told to compress the segments it creates and the memtable is rotated, which
/// rotates the WAL.
async fn lz4_tree(live: &Path) -> (Tree, u64) {
	let tree = Tree::new(options(live, false)).unwrap();
	commit(&tree, "first", b"first").await.unwrap();
	tree.core.inner.wal.write().set_compression_of_new_segments(CompressionType::Lz4);
	tree.core.inner.rotate_memtable().unwrap();
	// The segment the commits go to now starts by saying that its records are LZ4 blocks.
	let segment = active_segment(&tree, live);
	assert!(is_lz4(&segment), "control: the active segment is LZ4");
	let number = tree.core.inner.wal.read().get_active_log_number();
	(tree, number)
}

/// Whether a segment starts with the record that says its records are LZ4 blocks.
fn is_lz4(segment: &[u8]) -> bool {
	segment.get(6) == Some(&(RecordType::SetCompressionType as u8))
		&& segment.get(HEADER_SIZE) == Some(&(CompressionType::Lz4 as u8))
}

/// The bytes of the block of a record of `len` bytes that the writer reserves.
fn block_capacity(len: usize) -> usize {
	lz4_flex::block::get_maximum_output_size(len) + 4
}

/// A commit whose LZ4 block cannot be allocated fails through the pipeline and leaves the segment
/// as it was, and the commits after it are logged and read back.
#[tokio::test]
async fn a_compressed_segment_refuses_a_commit_whose_block_cannot_be_allocated() {
	const LEN: usize = 1 << 20;
	let dir = TempDir::new("alloc_limits").unwrap();
	let live = dir.path().join("live");
	let (tree, _) = lz4_tree(&live).await;

	let kept = ("kept".to_string(), value_of(0, 200));
	let empty = active_segment(&tree, &live).len();
	commit(&tree, &kept.0, &kept.1).await.unwrap();
	let segment = wal_bytes(&live);
	let active = active_segment(&tree, &live);
	assert!(active.len() >= empty + HEADER_SIZE + 4, "control: the active segment holds it");

	// The record is the batch with its value wrapped: a little more than `LEN` bytes.
	let value = value_of(1, LEN);
	let record = LEN + 2 + "doomed".len() + 40;
	let refusal =
		RefuseAllocations::of_size(block_capacity(record - 100)..=block_capacity(record + 100));
	let result = commit(&tree, "doomed", &value).await;
	drop(refusal);
	assert!(result.is_err(), "the commit fails, got {result:?}");
	assert!(wal_bytes(&live) == segment, "the segments are byte-identical");
	assert!(active_segment(&tree, &live) == active, "the active segment is byte-identical");

	let later: Vec<(String, Vec<u8>)> =
		(0..3).map(|i| (format!("later_{i}"), value_of(10 + i, 1000))).collect();
	for (key, value) in &later {
		commit(&tree, key, value).await.unwrap();
	}
	commit(&tree, "doomed", &value).await.unwrap();
	let mut all = vec![kept];
	all.extend(later);
	all.push(("doomed".to_string(), value));
	assert_recovers(&live, &dir.path().join("image"), false, &all, &[], "after the refusal");
	mark_closed(&tree);
	release_lock(&tree);
}

/// Groups of commits of every size reach a compressed segment as one block per commit: the
/// segment reads back, in a crash image, as what was acknowledged.
#[tokio::test]
async fn groups_of_commits_in_a_compressed_segment_recover() {
	let dir = TempDir::new("alloc_limits").unwrap();
	let live = dir.path().join("live");
	let (tree, first) = lz4_tree(&live).await;
	let tree = Arc::new(tree);

	let mut expected = Vec::new();
	let mut handles = Vec::new();
	for round in 0..6usize {
		for i in 0..40usize {
			let len = [10, 3_000, 40_000, 70_000, 1][i % 5];
			let key = format!("r{round}_k{i:03}");
			let value = if i % 2 == 0 {
				value_of(round * 100 + i, len)
			} else {
				vec![b'z'; len]
			};
			expected.push((key.clone(), value.clone()));
			handles.push(spawn_commit(&tree, &key, value));
		}
		for handle in handles.drain(..) {
			outcome(handle).await.unwrap();
		}
	}
	// The commits went to compressed segments only, from the first of them on, and each is a record
	// of its own whatever it compresses to: what the crash image replays is LZ4 blocks.
	let segments: Vec<Vec<u8>> = wal_bytes(&live)
		.into_iter()
		.filter(|(name, _)| *name >= segment_name(first, "wal"))
		.map(|(_, bytes)| bytes)
		.collect();
	assert!(segments.iter().all(|segment| is_lz4(segment)), "control: every segment is LZ4");
	let bytes: usize = segments.iter().map(Vec::len).sum();
	assert!(
		bytes >= segments.len() * (HEADER_SIZE + 1) + expected.len() * HEADER_SIZE,
		"control: the compressed segments hold the commits"
	);
	assert_recovers(&live, &dir.path().join("image"), false, &expected, &[], "lz4 groups");
	mark_closed(&tree);
	release_lock(&tree);
}

// ---------------------------------------------------------------------------
// the direct-to-L0 write of a batch that a memtable cannot hold
// ---------------------------------------------------------------------------

const CHILD: &str = "SKV_ALLOC_LIMITS_CHILD";

/// A batch above `max_memtable_size` is written to an L0 table after its record is logged, and
/// that write borrows the values of the batch. The test refuses the one allocation of the size of
/// a value, which would abort the process had the write copied it, so it runs in a process of its
/// own, where an abort fails this test and not the ones after it.
#[test]
fn a_batch_written_straight_to_l0_does_not_copy_its_values() {
	if std::env::var_os(CHILD).is_some() {
		return;
	}
	let out = Command::new(std::env::current_exe().unwrap())
		.args([
			"--exact",
			"test::alloc_limits_tests::child_a_batch_written_straight_to_l0",
			"--test-threads=1",
			"--nocapture",
		])
		.env(CHILD, "1")
		.output()
		.unwrap();
	let stdout = String::from_utf8_lossy(&out.stdout);
	let stderr = String::from_utf8_lossy(&out.stderr);
	assert!(out.status.success(), "the child died ({:?}):\n{stdout}\n{stderr}", out.status);
	assert!(stdout.contains("1 passed"), "the child must have run:\n{stdout}");
}

#[tokio::test]
async fn child_a_batch_written_straight_to_l0() {
	if std::env::var_os(CHILD).is_none() {
		return;
	}
	const LEN: usize = 1 << 20;
	let dir = TempDir::new("alloc_limits").unwrap();
	let tree = Tree::new(Arc::new(Options {
		path: dir.path().join("live"),
		flush_on_close: false,
		max_memtable_size: 64 << 10,
		..Default::default()
	}))
	.unwrap();

	// The value has the two bytes that wrap it in its capacity, so the flusher allocates nothing
	// for it, and the wrapped value is exactly `LEN + 2` bytes long.
	let mut value = Vec::with_capacity(LEN + 2);
	value.resize(LEN, 7u8);
	let mut batch = Batch::new(0);
	batch.add_record(InternalKeyKind::Set, b"giant".to_vec(), Some(value), 1).unwrap();

	let pipeline = &tree.core.commit_pipeline;
	let window = pipeline.commit_window();
	let start = tree.core.seq_num();
	let refusal = RefuseAllocations::of_size(LEN + 2..=LEN + 2);
	let result = tree.core.commit(batch, true, start, window, &[]).await;
	drop(refusal);
	result.unwrap();
	let read = tree.begin().unwrap().get(b"giant").unwrap().expect("the value is readable");
	assert_eq!(read.len(), LEN);
}
