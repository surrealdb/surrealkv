//! Reading every block of a table, to check each against its checksum and, if the table is
//! sealed, against its tag.
//!
//! This is what a scrub does, and it is not a lookup. It reads the blocks from the file, through
//! the same path that every read takes (the trailer, the seal and only then the decompressor), and
//! never through the block cache, which can hold a block that the file no longer has. It finds
//! the blocks by walking the table from its footer, with the kind of each given by where it was
//! found and not by anything the file says about it, so a block that is the right bytes in the
//! wrong place fails to open as the kind that it is read as.

use std::sync::Arc;

use crate::error::Result;
use crate::sstable::block::{Block, BlockHandle};
use crate::sstable::block_io::BlockKind;
use crate::sstable::table::Table;

/// The prefix of the meta-index entry that names the filter index of a table with partitioned
/// filters, followed by the name of the policy.
const PARTITIONED_FILTER_PREFIX: &[u8] = b"partitionedfilter.";

/// How many blocks of each of the six kinds were read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct BlockCounts {
	/// Blocks of sorted entries: the keys and values of the table.
	pub data: usize,
	/// Partitions of the index, each of which names data blocks.
	pub index_partitions: usize,
	/// The top-level index, which names the partitions of the index.
	pub top_level_index: usize,
	/// Partitions of the filter.
	pub filter_partitions: usize,
	/// The index of the partitions of the filter.
	pub filter_index: usize,
	/// The meta-index, which holds the properties of the table and names its filter.
	pub meta_index: usize,
}

impl BlockCounts {
	/// The number of blocks of every kind together.
	pub fn total(&self) -> usize {
		self.data
			+ self.index_partitions
			+ self.top_level_index
			+ self.filter_partitions
			+ self.filter_index
			+ self.meta_index
	}

	/// Adds the counts of another table.
	pub fn add(&mut self, other: &Self) {
		self.data += other.data;
		self.index_partitions += other.index_partitions;
		self.top_level_index += other.top_level_index;
		self.filter_partitions += other.filter_partitions;
		self.filter_index += other.filter_index;
		self.meta_index += other.meta_index;
	}
}

/// The handles that the entries of `block` hold as their values, in order: what an index and a
/// filter index are.
fn handles_of(block: &Block) -> Result<Vec<BlockHandle>> {
	let mut handles = Vec::new();
	let mut iter = block.iter()?;
	iter.seek_to_first()?;
	while iter.is_valid() {
		let (handle, _) = BlockHandle::decode(iter.value_bytes())?;
		handles.push(handle);
		iter.advance()?;
	}
	Ok(handles)
}

/// The filter indexes that the meta-index names, which is one for a table with partitioned
/// filters under any policy name, and none for a table without a filter. A monolithic filter, which
/// no writer makes any more, is not a block with a trailer and is not read here.
fn filter_indexes_of(meta_index: &Block) -> Result<Vec<BlockHandle>> {
	let mut handles = Vec::new();
	let mut iter = meta_index.iter()?;
	iter.seek_to_first()?;
	while iter.is_valid() {
		if iter.user_key().starts_with(PARTITIONED_FILTER_PREFIX) {
			let (handle, _) = BlockHandle::decode(iter.value_bytes())?;
			if handle.size() > 0 {
				handles.push(handle);
			}
		}
		iter.advance()?;
	}
	Ok(handles)
}

/// Reads every block of `table` from its file and checks it: the meta-index, the top-level index
/// and each partition of the index and the data blocks it names, and each filter index that the
/// meta-index names and the partitions of the filter that it names. A sealed table authenticates
/// each of them, and the first block that does not is the error of the call, with the error that
/// says why: a checksum that does not match, or a block that does not authenticate.
///
/// Returns how many blocks of each kind were read. No block is kept, and none is taken from the
/// block cache.
pub(crate) fn verify_blocks(table: &Table) -> Result<BlockCounts> {
	let source = &table.source;
	let comparator = || Arc::clone(&table.opts.internal_comparator);
	let footer = source.read_footer()?;
	let mut counts = BlockCounts::default();

	let meta_index = source.read_block(BlockKind::MetaIndex, &footer.meta_index, comparator())?;
	counts.meta_index += 1;

	let top_level = source.read_block(BlockKind::TopLevelIndex, &footer.index, comparator())?;
	counts.top_level_index += 1;
	for partition in handles_of(&top_level)? {
		let block = source.read_block(BlockKind::IndexPartition, &partition, comparator())?;
		counts.index_partitions += 1;
		for data in handles_of(&block)? {
			source.read_contents(BlockKind::Data, &data)?;
			counts.data += 1;
		}
	}

	for filter_index in filter_indexes_of(&meta_index)? {
		let block = source.read_block(BlockKind::FilterIndex, &filter_index, comparator())?;
		counts.filter_index += 1;
		for partition in handles_of(&block)? {
			source.read_contents(BlockKind::FilterPartition, &partition)?;
			counts.filter_partitions += 1;
		}
	}
	Ok(counts)
}
