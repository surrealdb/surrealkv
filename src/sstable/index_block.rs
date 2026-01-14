// Note: Needs to be tested if a top-level index improves performance as such.
// TODO: Replace the current non-partitioned index block writer with this
use std::cmp::Ordering;
use std::io::Write;
use std::sync::Arc;

use crate::comparator::Comparator;
use crate::error::{Error, Result};
use crate::sstable::block::{Block, BlockData, BlockHandle, BlockWriter};
use crate::sstable::error::SSTableError;
use crate::sstable::table::{compress_block, read_table_block, write_block_at_offset};
use crate::vfs::File;
use crate::{CompressionType, Options};

/// Points to a block on file
#[derive(Clone, Debug)]
pub(crate) struct BlockHandleWithKey {
	/// Full encoded separator key (internal key with seq_num)
	pub separator_key: Vec<u8>,

	/// Position of block in file
	pub handle: BlockHandle,
}

impl BlockHandleWithKey {
	#[cfg(test)]
	pub(crate) fn new(separator_key: Vec<u8>, handle: BlockHandle) -> BlockHandleWithKey {
		BlockHandleWithKey {
			separator_key,
			handle,
		}
	}

	pub(crate) fn offset(&self) -> u64 {
		self.handle.offset as u64
	}
}

// Represents a top-level index block that contains pointers to other index
// blocks Link: https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
//
// [index block - partition 1]
// [index block - partition 2]
// ...
// [index block - partition N]
// [index block - top-level index]
pub(crate) struct TopLevelIndexWriter {
	opts: Arc<Options>,
	index_blocks: Vec<BlockWriter>,
	current_block: BlockWriter,
	max_block_size: usize,

	// Stats tracked during finish()
	index_size: u64,
	num_partitions: u64,
	top_level_index_size: u64,
}

impl TopLevelIndexWriter {
	pub(crate) fn new(opts: Arc<Options>, max_block_size: usize) -> TopLevelIndexWriter {
		TopLevelIndexWriter {
			opts: Arc::clone(&opts),
			index_blocks: Vec::new(),
			current_block: BlockWriter::new(
				opts.block_size,
				opts.block_restart_interval,
				Arc::clone(&opts.internal_comparator),
			),
			max_block_size,
			index_size: 0,
			num_partitions: 0,
			top_level_index_size: 0,
		}
	}

	// Query methods - called after finish()
	pub(crate) fn index_size(&self) -> u64 {
		self.index_size
	}

	pub(crate) fn num_partitions(&self) -> u64 {
		self.num_partitions
	}

	pub(crate) fn top_level_index_size(&self) -> u64 {
		self.top_level_index_size
	}

	pub(crate) fn add(&mut self, key: &[u8], handle: &[u8]) -> Result<()> {
		if self.current_block.size_estimate() >= self.max_block_size {
			self.finish_current_block();
		}
		self.current_block.add(key, handle)
	}

	fn finish_current_block(&mut self) {
		let new_block = BlockWriter::new(
			self.opts.block_size,
			self.opts.block_restart_interval,
			Arc::clone(&self.opts.internal_comparator),
		);
		let finished_block = std::mem::replace(&mut self.current_block, new_block);
		self.index_blocks.push(finished_block);
	}

	fn write_compressed_block<W: Write>(
		writer: &mut W,
		block: BlockData,
		compression_type: CompressionType,
		offset: usize,
	) -> Result<(BlockHandle, usize)> {
		let compressed_block = compress_block(block, compression_type)?;
		write_block_at_offset(writer, compressed_block, compression_type, offset)
	}

	pub(crate) fn finish<W: Write>(
		&mut self,
		writer: &mut W,
		compression_type: CompressionType,
		mut offset: usize,
	) -> Result<(BlockHandle, usize)> {
		let start_offset = offset;

		// Only finish current block if it has entries
		if self.current_block.entries() > 0 {
			self.finish_current_block();
		}

		// If no blocks were created, move current_block to index_blocks
		if self.index_blocks.is_empty() {
			let new_block = BlockWriter::new(
				self.opts.block_size,
				self.opts.block_restart_interval,
				Arc::clone(&self.opts.internal_comparator),
			);
			let old_block = std::mem::replace(&mut self.current_block, new_block);
			self.index_blocks.push(old_block);
		}

		let mut top_level_index = BlockWriter::new(
			self.opts.block_size,
			self.opts.block_restart_interval,
			Arc::clone(&self.opts.internal_comparator),
		);

		// Track number of partitions
		self.num_partitions = self.index_blocks.len() as u64;

		// Take ownership of index_blocks to iterate and consume
		let index_blocks = std::mem::take(&mut self.index_blocks);
		for block in index_blocks {
			let separator_key = block.last_key.clone();
			let block_data = block.finish()?;

			// Skip empty blocks or blocks with empty keys
			if separator_key.is_empty() {
				continue;
			}

			let (block_handle, new_offset) =
				Self::write_compressed_block(writer, block_data, compression_type, offset)?;
			offset = new_offset; // Update the offset for the next iteration

			// Use the last key of each block as the separator key
			top_level_index.add(&separator_key, &block_handle.encode())?;
		}

		let top_level_data = top_level_index.finish()?;
		self.top_level_index_size = top_level_data.len() as u64;
		let (handle, final_offset) =
			Self::write_compressed_block(writer, top_level_data, compression_type, offset)?;

		// Track total index size
		self.index_size = (final_offset - start_offset) as u64;

		Ok((handle, final_offset))
	}
}

// TODO: use block_cache to store top-level index blocks
#[derive(Clone)]
pub(crate) struct TopLevelIndex {
	pub(crate) id: u64,
	pub(crate) opts: Arc<Options>,
	pub(crate) blocks: Vec<BlockHandleWithKey>,
	// TODO: Fix this, as this could be problematic if the file is being shared across without any
	// mutex
	pub(crate) file: Arc<dyn File>,
}

impl TopLevelIndex {
	pub(crate) fn new(
		id: u64,
		opt: Arc<Options>,
		f: Arc<dyn File>,
		location: &BlockHandle,
	) -> Result<Self> {
		let block =
			read_table_block(Arc::clone(&opt.internal_comparator), Arc::clone(&f), location)?;
		let mut iter = block.iter()?;
		let mut blocks = Vec::new();
		iter.seek_to_first()?;
		while iter.is_valid() {
			let key = iter.key_bytes().to_vec();
			let handle_bytes = iter.value_bytes();
			// Store full encoded internal key for correct partition lookup
			let (handle, _) = BlockHandle::decode(handle_bytes)?;
			blocks.push(BlockHandleWithKey {
				separator_key: key,
				handle,
			});
			iter.advance()?;
		}
		Ok(TopLevelIndex {
			id,
			opts: opt,
			blocks,
			file: Arc::clone(&f),
		})
	}

	pub(crate) fn find_block_handle_by_key(
		&self,
		target: &[u8],
	) -> Result<Option<(usize, &BlockHandleWithKey)>> {
		// Guard against empty/corrupt partitioned index
		if self.blocks.is_empty() {
			let err = Error::from(SSTableError::EmptyCorruptPartitionedIndex {
				table_id: self.id,
			});
			log::error!("[INDEX] {}", err);
			return Err(err);
		}

		let internal_cmp = &self.opts.internal_comparator;

		// Find the partition point in the blocks where the key would fit.
		// Uses full internal key comparison for correct partition lookup.
		let index = self.blocks.partition_point(|block| {
			internal_cmp.compare(&block.separator_key, target) == Ordering::Less
		});

		// Attempt to retrieve the block at the found index.
		Ok(self
			.blocks
			.get(index)
			.filter(|block| internal_cmp.compare(target, &block.separator_key) != Ordering::Greater)
			.map(|block| (index, block)))
	}

	pub(crate) fn load_block(&self, block_handle: &BlockHandleWithKey) -> Result<Arc<Block>> {
		if let Some(block) = self.opts.block_cache.get_index_block(self.id, block_handle.offset()) {
			return Ok(block);
		}

		let block_data = read_table_block(
			Arc::clone(&self.opts.internal_comparator),
			Arc::clone(&self.file),
			&block_handle.handle,
		)?;
		let block = Arc::new(block_data);
		self.opts.block_cache.insert_index_block(
			self.id,
			block_handle.offset(),
			Arc::clone(&block),
		);

		Ok(block)
	}

	pub(crate) fn get(&self, target: &[u8]) -> Result<Arc<Block>> {
		match self.find_block_handle_by_key(target)? {
			Some((_index, block_handle)) => {
				let block = self.load_block(block_handle)?;
				Ok(block)
			}
			None => Err(Error::BlockNotFound),
		}
	}
}

use crate::sstable::block::BlockIterator;

/// Iterates over ALL partition index entries across all partition blocks.
/// This flattens the two-level partition structure (blocks[] + entries within each block)
/// into a single iterator that can be used as the first level of TwoLevelIterator.
pub(crate) struct PartitionedIndexIterator<'a> {
	index: &'a TopLevelIndex,
	/// Current partition block index (index into blocks[])
	partition_index: usize,
	/// Iterator within the current partition block
	partition_iter: Option<BlockIterator>,
}

impl<'a> PartitionedIndexIterator<'a> {
	pub(crate) fn new(index: &'a TopLevelIndex) -> Self {
		Self {
			index,
			partition_index: 0,
			partition_iter: None,
		}
	}

	/// Check if the iterator is positioned on a valid entry
	pub(crate) fn valid(&self) -> bool {
		self.partition_iter.as_ref().map_or(false, |iter| iter.is_valid())
	}

	/// Get the current key (separator key from the partition index entry)
	#[allow(dead_code)]
	pub(crate) fn key(&self) -> &[u8] {
		debug_assert!(self.valid());
		self.partition_iter.as_ref().unwrap().key_bytes()
	}

	/// Get the current value as a BlockHandle (data block handle)
	pub(crate) fn block_handle(&self) -> Result<BlockHandle> {
		debug_assert!(self.valid());
		let val = self.partition_iter.as_ref().unwrap().value_bytes();
		let (handle, _) = BlockHandle::decode(val)?;
		Ok(handle)
	}

	/// Seek to the first entry across all partitions
	pub(crate) fn seek_to_first(&mut self) -> Result<()> {
		if self.index.blocks.is_empty() {
			self.partition_iter = None;
			let err = Error::from(SSTableError::EmptyCorruptPartitionedIndex {
				table_id: self.index.id,
			});
			log::error!("[INDEX] {}", err);
			return Err(err);
		}

		// Load first partition block
		self.partition_index = 0;
		let block = self.index.load_block(&self.index.blocks[0])?;
		let mut iter = block.iter()?;
		iter.seek_to_first()?;
		self.partition_iter = Some(iter);

		// If first partition is empty, skip to next
		if !self.valid() {
			self.next()?;
		}
		Ok(())
	}

	/// Seek to the last entry across all partitions
	pub(crate) fn seek_to_last(&mut self) -> Result<()> {
		if self.index.blocks.is_empty() {
			self.partition_iter = None;
			let err = Error::from(SSTableError::EmptyCorruptPartitionedIndex {
				table_id: self.index.id,
			});
			log::error!("[INDEX] {}", err);
			return Err(err);
		}

		// Load last partition block
		self.partition_index = self.index.blocks.len() - 1;
		let block = self.index.load_block(&self.index.blocks[self.partition_index])?;
		let mut iter = block.iter()?;
		iter.seek_to_last()?;
		self.partition_iter = Some(iter);

		// If last partition is empty, skip to prev
		if !self.valid() {
			self.prev()?;
		}
		Ok(())
	}

	/// Seek to the first entry >= target
	pub(crate) fn seek(&mut self, target: &[u8]) -> Result<()> {
		if self.index.blocks.is_empty() {
			self.partition_iter = None;
			return Ok(());
		}

		// Use find_block_handle_by_key to locate the right partition
		match self.index.find_block_handle_by_key(target)? {
			Some((idx, block_handle)) => {
				self.partition_index = idx;
				let block = self.index.load_block(block_handle)?;
				let mut iter = block.iter()?;
				iter.seek_internal(target)?;
				self.partition_iter = Some(iter);

				// If seek went past end of this partition, move to next
				if !self.valid() {
					self.next()?;
				}
			}
			None => {
				// Target is past all partitions
				self.partition_iter = None;
			}
		}
		Ok(())
	}

	/// Move to the next entry, crossing partition boundaries if needed
	pub(crate) fn next(&mut self) -> Result<bool> {
		if let Some(ref mut iter) = self.partition_iter {
			// Try to advance within current partition
			if iter.advance()? {
				return Ok(true);
			}
		}

		// Current partition exhausted, move to next partition
		loop {
			self.partition_index += 1;
			if self.partition_index >= self.index.blocks.len() {
				self.partition_iter = None;
				return Ok(false);
			}

			let block = self.index.load_block(&self.index.blocks[self.partition_index])?;
			let mut iter = block.iter()?;
			iter.seek_to_first()?;
			self.partition_iter = Some(iter);

			if self.valid() {
				return Ok(true);
			}
			// Empty partition, continue to next
		}
	}

	/// Move to the previous entry, crossing partition boundaries if needed
	pub(crate) fn prev(&mut self) -> Result<bool> {
		if let Some(ref mut iter) = self.partition_iter {
			// Try to retreat within current partition
			if iter.prev_internal()? {
				return Ok(true);
			}
		}

		// Current partition exhausted going backward, move to previous partition
		loop {
			if self.partition_index == 0 {
				self.partition_iter = None;
				return Ok(false);
			}
			self.partition_index -= 1;

			let block = self.index.load_block(&self.index.blocks[self.partition_index])?;
			let mut iter = block.iter()?;
			iter.seek_to_last()?;
			self.partition_iter = Some(iter);

			if self.valid() {
				return Ok(true);
			}
			// Empty partition, continue to prev
		}
	}
}
