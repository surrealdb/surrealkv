//! Tests of the layout of an SSTable file: its bytes against a reference writer, and the
//! tiling of its blocks.
//!
//! The reader used here is written out in this file, from the format, so that what the tests
//! say about a file does not depend on the code that wrote it.

use std::sync::Arc;

use crate::sstable::block::{Block, BlockHandle};
use crate::sstable::meta::TableMetadata;
use crate::sstable::table::{TableWriter, TABLE_FULL_FOOTER_LENGTH};
use crate::test::reference_writer::ReferenceTableWriter;
use crate::vlog::{ValueLocation, ValuePointer};
use crate::{CompressionType, InternalKey, InternalKeyKind, Options};

/// Bytes after a block in a plaintext table: its compression type and its masked checksum.
const PLAIN_TRAILER_LEN: usize = 5;

/// Bytes of the created-at time in the properties of a table.
const CREATED_AT_LEN: usize = 16;

// ========== Inputs ==========

/// What is written to a table: points in order, and range deletions.
#[derive(Clone, Default)]
struct Input {
	points: Vec<(InternalKey, Vec<u8>)>,
	range_deletions: Vec<(Vec<u8>, Vec<u8>, u64)>,
}

fn point(i: usize, value: Vec<u8>) -> (InternalKey, Vec<u8>) {
	(
		InternalKey::new(format!("key{i:05}").into_bytes(), (i + 1) as u64, InternalKeyKind::Set),
		value,
	)
}

/// `n` points whose values are inline and about 40 bytes.
fn inline_points(n: usize) -> Input {
	Input {
		points: (0..n)
			.map(|i| {
				let value = format!("value-{i:05}-{}", "x".repeat(24)).into_bytes();
				point(i, ValueLocation::with_inline_value(value).encode())
			})
			.collect(),
		range_deletions: Vec::new(),
	}
}

fn options(block_size: usize, index_partition_size: usize, filter: bool) -> Arc<Options> {
	let mut opts = Options {
		block_size,
		block_restart_interval: 4,
		index_partition_size,
		compression_per_level: vec![CompressionType::None],
		..Default::default()
	};
	if !filter {
		opts.filter_policy = None;
	}
	Arc::new(opts)
}

/// Every configuration that the tests below run: one partition of each kind, and many; with a
/// filter and without; with range deletions and with value pointers; and a table of range
/// deletions alone.
fn configurations() -> Vec<(&'static str, Arc<Options>, Input)> {
	let mut with_range_deletions = inline_points(40);
	with_range_deletions.range_deletions = vec![
		(b"key00010".to_vec(), b"key00020".to_vec(), 100),
		(b"key00030".to_vec(), b"key00035".to_vec(), 101),
	];
	let mut with_pointers = inline_points(60);
	for (i, (_, value)) in with_pointers.points.iter_mut().enumerate() {
		if i % 7 == 0 {
			let pointer = ValuePointer::new(9 - (i % 5) as u32, 4096 * i as u64, 8, 5000, 0xabcd);
			*value = ValueLocation::with_pointer(pointer).encode();
		}
	}
	let range_deletions_only = Input {
		points: Vec::new(),
		range_deletions: vec![(b"a".to_vec(), b"z".to_vec(), 7)],
	};

	vec![
		("one partition of each kind", options(64 * 1024, 16 * 1024, true), inline_points(30)),
		("many blocks and partitions", options(256, 100, true), inline_points(200)),
		("many blocks and partitions, no filter", options(256, 100, false), inline_points(200)),
		("one partition, no filter", options(64 * 1024, 16 * 1024, false), inline_points(30)),
		("range deletions and points", options(256, 100, true), with_range_deletions),
		("value pointers", options(256, 100, true), with_pointers),
		("range deletions only", options(256, 100, true), range_deletions_only.clone()),
		("range deletions only, no filter", options(256, 100, false), range_deletions_only),
		("one point", options(256, 100, true), inline_points(1)),
	]
}

// ========== Writers ==========

fn write_table(opts: &Arc<Options>, input: &Input) -> Vec<u8> {
	let mut buf = Vec::new();
	let size = {
		let mut writer = TableWriter::new(&mut buf, 7, Arc::clone(opts), 0);
		for (key, value) in &input.points {
			writer.add(key.clone(), value).unwrap();
		}
		for (start, end, seq) in &input.range_deletions {
			writer.add_range_deletion(start.clone(), end.clone(), *seq);
		}
		writer.finish().unwrap()
	};
	assert_eq!(size, buf.len(), "finish() must report the size of the file");
	buf
}

fn write_reference_table(opts: &Arc<Options>, input: &Input) -> Vec<u8> {
	let mut buf = Vec::new();
	let size = {
		let mut writer = ReferenceTableWriter::new(&mut buf, 7, Arc::clone(opts), 0);
		for (key, value) in &input.points {
			writer.add(key.clone(), value).unwrap();
		}
		for (start, end, seq) in &input.range_deletions {
			writer.add_range_deletion(start.clone(), end.clone(), *seq);
		}
		writer.finish().unwrap()
	};
	assert_eq!(size, buf.len(), "finish() must report the size of the file");
	buf
}

// ========== A reader of the format ==========

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
	Data,
	IndexPartition,
	TopLevelIndex,
	FilterPartition,
	FilterIndex,
	MetaIndex,
}

type Extent = (Kind, BlockHandle);

/// The contents of the block at `handle`, after its checksum was checked. Every table of these
/// tests is uncompressed.
fn block_contents(bytes: &[u8], handle: &BlockHandle) -> Vec<u8> {
	let end = handle.offset + handle.size;
	let payload = &bytes[handle.offset..end];
	let compression = bytes[end];
	assert_eq!(compression, 0, "block at {} is compressed", handle.offset);
	let stored = u32::from_le_bytes(bytes[end + 1..end + PLAIN_TRAILER_LEN].try_into().unwrap());
	// A stored checksum is the CRC32 of the block and its compression byte, rotated right by 15
	// bits with a constant added.
	let crc = stored.wrapping_sub(0xa282_ead8).rotate_left(15);
	assert_eq!(
		crc32fast::hash(&bytes[handle.offset..=end]),
		crc,
		"checksum of the block at {}",
		handle.offset
	);
	payload.to_vec()
}

/// The `(key, value)` entries of a block of sorted entries.
fn entries(bytes: &[u8], handle: &BlockHandle, opts: &Options) -> Vec<(Vec<u8>, Vec<u8>)> {
	let block = Block::new(block_contents(bytes, handle), Arc::clone(&opts.internal_comparator));
	let mut iter = block.iter().unwrap();
	iter.seek_to_first().unwrap();
	let mut found = Vec::new();
	while iter.is_valid() {
		found.push((iter.key_bytes().to_vec(), iter.value_bytes().to_vec()));
		iter.advance().unwrap();
	}
	found
}

/// The footer, and the handles of the meta-index and of the top-level index that it holds.
fn read_footer(bytes: &[u8]) -> (BlockHandle, BlockHandle) {
	let footer = &bytes[bytes.len() - TABLE_FULL_FOOTER_LENGTH..];
	assert_eq!(&footer[42..], &[0x57, 0xfb, 0x80, 0x8b, 0x24, 0x75, 0x47, 0xdb]);
	assert_eq!((footer[0], footer[1]), (2, 1), "format (LSMV2) and checksum type");
	let (meta_index, meta_len) = BlockHandle::decode(&footer[2..]).unwrap();
	let (index, index_len) = BlockHandle::decode(&footer[2 + meta_len..]).unwrap();
	assert!(footer[2 + meta_len + index_len..42].iter().all(|b| *b == 0), "footer padding");
	(meta_index, index)
}

/// Every block of the file, found by following the handles from the footer.
fn walk(bytes: &[u8], opts: &Options) -> Vec<Extent> {
	let (meta_index, index) = read_footer(bytes);
	let mut blocks =
		vec![(Kind::TopLevelIndex, index.clone()), (Kind::MetaIndex, meta_index.clone())];

	for (_, partition) in entries(bytes, &index, opts) {
		let (partition, _) = BlockHandle::decode(&partition).unwrap();
		for (_, data) in entries(bytes, &partition, opts) {
			blocks.push((Kind::Data, BlockHandle::decode(&data).unwrap().0));
		}
		blocks.push((Kind::IndexPartition, partition));
	}

	for (key, value) in entries(bytes, &meta_index, opts) {
		let name = InternalKey::decode(&key).user_key;
		if name.starts_with(b"partitionedfilter.") {
			let (filter_index, _) = BlockHandle::decode(&value).unwrap();
			for (_, partition) in entries(bytes, &filter_index, opts) {
				blocks.push((Kind::FilterPartition, BlockHandle::decode(&partition).unwrap().0));
			}
			blocks.push((Kind::FilterIndex, filter_index));
		}
	}
	blocks
}

/// The properties of the table, from its meta-index.
fn read_metadata(bytes: &[u8], opts: &Options) -> TableMetadata {
	let (meta_index, _) = read_footer(bytes);
	let (_, value) = entries(bytes, &meta_index, opts)
		.into_iter()
		.find(|(key, _)| InternalKey::decode(key).user_key == b"meta")
		.expect("the meta-index holds the properties");
	TableMetadata::decode(&value).unwrap()
}

/// The file with the bytes that depend on the time it was written at set to zero: the time in
/// the properties, which is in the meta-index, and the checksum of the meta-index.
fn without_wall_clock(bytes: &[u8], opts: &Options) -> Vec<u8> {
	let (meta_index, _) = read_footer(bytes);
	let created_at = read_metadata(bytes, opts).properties.created_at.to_be_bytes();
	let contents = &bytes[meta_index.offset..meta_index.offset + meta_index.size];
	let at: Vec<usize> = contents
		.windows(CREATED_AT_LEN)
		.enumerate()
		.filter(|(_, window)| *window == created_at)
		.map(|(i, _)| i)
		.collect();
	assert_eq!(at.len(), 1, "the time the table was created at is in the meta-index once");

	let mut masked = bytes.to_vec();
	let at = meta_index.offset + at[0];
	masked[at..at + CREATED_AT_LEN].fill(0);
	let checksum = meta_index.offset + meta_index.size + 1;
	masked[checksum..checksum + 4].fill(0);
	masked
}

fn assert_same_bytes(actual: &[u8], expected: &[u8], what: &str) {
	let differs = actual.iter().zip(expected).position(|(a, e)| a != e);
	assert!(
		differs.is_none() && actual.len() == expected.len(),
		"{what}: the files differ, first at byte {differs:?}, lengths {} and {}",
		actual.len(),
		expected.len()
	);
}

/// Asserts that the blocks are one after the other from the start of the file to the footer,
/// each followed by its trailer and by nothing else, and returns them in file order.
fn assert_tiled(bytes: &[u8], blocks: &[Extent], trailer_len: usize, what: &str) -> Vec<Extent> {
	let mut ordered = blocks.to_vec();
	ordered.sort_by_key(|(_, handle)| handle.offset);
	let mut expected = 0;
	for (kind, handle) in &ordered {
		assert_eq!(
			handle.offset, expected,
			"{what}: the {kind:?} block at {} should start at {expected}",
			handle.offset
		);
		expected = handle.offset + handle.size + trailer_len;
	}
	assert_eq!(
		expected,
		bytes.len() - TABLE_FULL_FOOTER_LENGTH,
		"{what}: the last block must end where the footer begins"
	);
	ordered
}

// ========== Tests ==========

#[test]
fn a_plaintext_table_is_what_the_reference_writer_writes() {
	for (name, opts, input) in configurations() {
		let written = write_table(&opts, &input);
		let reference = write_reference_table(&opts, &input);

		assert_same_bytes(
			&without_wall_clock(&written, &opts),
			&without_wall_clock(&reference, &opts),
			name,
		);
	}
}

#[test]
fn the_blocks_of_a_table_tile_the_file_up_to_the_footer() {
	for (name, opts, input) in configurations() {
		let bytes = write_table(&opts, &input);
		let blocks = walk(&bytes, &opts);
		let ordered = assert_tiled(&bytes, &blocks, PLAIN_TRAILER_LEN, name);

		// The blocks are in the order that they are written in: the data blocks as they fill,
		// then the filter, then the index, then the meta-index.
		let kinds: Vec<Kind> = ordered.iter().map(|(kind, _)| *kind).collect();
		let count = |wanted: Kind| kinds.iter().filter(|kind| **kind == wanted).count();
		let mut expected = vec![Kind::Data; count(Kind::Data)];
		expected.extend(vec![Kind::FilterPartition; count(Kind::FilterPartition)]);
		if count(Kind::FilterIndex) == 1 {
			expected.push(Kind::FilterIndex);
		}
		expected.extend(vec![Kind::IndexPartition; count(Kind::IndexPartition)]);
		expected.extend([Kind::TopLevelIndex, Kind::MetaIndex]);
		assert_eq!(kinds, expected, "{name}: order of the blocks");

		// The filter has a block for each of its partitions and an index of them, or has none.
		assert!(count(Kind::FilterIndex) <= 1, "{name}");
		assert_eq!(count(Kind::FilterIndex) == 1, count(Kind::FilterPartition) > 0, "{name}");
		assert_eq!(count(Kind::TopLevelIndex), 1, "{name}");
		assert_eq!(count(Kind::MetaIndex), 1, "{name}");
	}
}

#[test]
fn the_tests_cover_every_kind_of_block_in_many_numbers() {
	// A guard against the tests above passing because the tables are too small to have what
	// they check.
	let (_, opts, input) = configurations().remove(1);
	let bytes = write_table(&opts, &input);
	let blocks = walk(&bytes, &opts);
	let count = |wanted: Kind| blocks.iter().filter(|(kind, _)| *kind == wanted).count();

	assert!(count(Kind::Data) > 10, "data blocks: {}", count(Kind::Data));
	assert!(count(Kind::IndexPartition) > 2, "index partitions: {}", count(Kind::IndexPartition));
	assert!(
		count(Kind::FilterPartition) > 2,
		"filter partitions: {}",
		count(Kind::FilterPartition)
	);
	assert_eq!(count(Kind::FilterIndex), 1);
	assert_eq!(count(Kind::TopLevelIndex), 1);
	assert_eq!(count(Kind::MetaIndex), 1);

	// And the properties agree with what the walk found.
	let metadata = read_metadata(&bytes, &opts);
	assert_eq!(metadata.properties.num_data_blocks as usize, count(Kind::Data));
	assert_eq!(metadata.properties.index_partitions as usize, count(Kind::IndexPartition));
}

#[test]
fn a_table_of_range_deletions_alone_has_an_empty_index_and_a_meta_index() {
	let (name, opts, input) = configurations().remove(6);
	assert!(input.points.is_empty());
	let bytes = write_table(&opts, &input);
	let blocks = walk(&bytes, &opts);
	let ordered = assert_tiled(&bytes, &blocks, PLAIN_TRAILER_LEN, name);

	let kinds: Vec<Kind> = ordered.iter().map(|(kind, _)| *kind).collect();
	assert_eq!(kinds, vec![Kind::TopLevelIndex, Kind::MetaIndex]);
}
