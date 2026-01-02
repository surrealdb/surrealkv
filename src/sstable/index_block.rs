// Note: Needs to be tested if a top-level index improves performance as such.
// TODO: Replace the current non-partitioned index block writer with this
use std::cmp::Ordering;
use std::collections::HashMap;
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
	pub(crate) partition_map: HashMap<u64, Arc<Block>>,
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
		let iter = block.iter(false)?;
		let mut blocks = Vec::new();
		let mut partition_map = HashMap::new();
		for item in iter {
			let (key, handle) = item?;
			// Store full encoded internal key for correct partition lookup
			let (handle, _) = BlockHandle::decode(&handle)?;
			let block_handle = BlockHandleWithKey {
				separator_key: key,
				handle,
			};
			let block = Arc::new(read_table_block(
				Arc::clone(&opt.internal_comparator),
				Arc::clone(&f),
				&block_handle.handle,
			)?);

			partition_map.insert(block_handle.offset(), block);
			blocks.push(block_handle);
		}
		Ok(TopLevelIndex {
			id,
			opts: opt,
			blocks,
			partition_map,
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
		if let Some(block) = self.partition_map.get(&block_handle.offset()) {
			return Ok(Arc::clone(block));
		}

		Err(Error::PartitionBlockExpectedButNotFound)
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

/// Handles readahead for sequential block access patterns.
pub(crate) struct BlockPrefetcher {
	// Current readahead size for FS prefetching
	pub(crate) readahead_size: usize,
	// Readahead limit for tracking what has been prefetched
	readahead_limit: u64,
	// Initial auto readahead size for internal prefetch buffer
	initial_auto_readahead_size: usize,
	// Maximum auto readahead size to cap exponential growth
	max_auto_readahead_size: usize,
	// Number of sequential file reads for auto readahead
	num_sequential_reads: usize,
	// Previous access pattern for sequential detection
	prev_offset: u64,
	prev_len: usize,
}

impl BlockPrefetcher {
	pub(crate) fn new(initial_auto_readahead_size: usize, max_auto_readahead_size: usize) -> Self {
		let sanitized_initial = if initial_auto_readahead_size > max_auto_readahead_size {
			max_auto_readahead_size
		} else {
			initial_auto_readahead_size
		};

		Self {
			readahead_size: sanitized_initial,
			readahead_limit: 0,
			initial_auto_readahead_size: sanitized_initial,
			max_auto_readahead_size,
			num_sequential_reads: 0,
			prev_offset: 0,
			prev_len: 0,
		}
	}

	/// Update read pattern for sequential detection
	pub(crate) fn update_read_pattern(&mut self, offset: u64, len: usize) {
		self.prev_offset = offset;
		self.prev_len = len;
	}

	/// Check if block access is sequential
	pub(crate) fn is_block_sequential(&self, offset: u64) -> bool {
		self.prev_len == 0 || (self.prev_offset + self.prev_len as u64 == offset)
	}

	pub(crate) fn reset_values(&mut self, initial_auto_readahead_size: usize) {
		self.num_sequential_reads = 1;
		// Sanitize the initial size against max_auto_readahead_size
		let sanitized_initial = initial_auto_readahead_size.min(self.max_auto_readahead_size);
		self.initial_auto_readahead_size = sanitized_initial;
		self.readahead_size = sanitized_initial;
		self.readahead_limit = 0;
	}

	pub(crate) fn prefetch_if_needed(
		&mut self,
		handle: &BlockHandle,
		file: &dyn crate::vfs::File,
	) -> bool {
		let len = handle.size() as u64;
		let offset = handle.offset() as u64;

		// Check if already prefetched
		if offset + len <= self.readahead_limit {
			self.update_read_pattern(offset, len as usize);
			return false;
		}

		// Check if access is sequential
		if !self.is_block_sequential(offset) {
			self.update_read_pattern(offset, len as usize);
			self.reset_values(self.initial_auto_readahead_size);
			return false;
		}

		self.update_read_pattern(offset, len as usize);

		// Auto readahead logic - try FS prefetch
		// Disable prefetching if either initial or max readahead size is 0
		if self.initial_auto_readahead_size == 0 || self.max_auto_readahead_size == 0 {
			return false;
		}

		self.num_sequential_reads += 1;
		if self.num_sequential_reads <= 2 {
			// Default num_sequential_reads_for_auto_readahead
			return false;
		}

		// Try FS-level prefetch for auto readahead
		if file.supports_prefetch() {
			let prefetch_result = file.prefetch(offset, len as usize + self.readahead_size);
			if prefetch_result.is_ok() {
				self.readahead_limit = offset + len + self.readahead_size as u64;
				self.grow_readahead_size();
				return false; // FS prefetch succeeded
			}
		}

		true
	}

	pub(crate) fn grow_readahead_size(&mut self) {
		if self.readahead_size < self.max_auto_readahead_size {
			self.readahead_size = (self.readahead_size * 2).min(self.max_auto_readahead_size);
		}
	}
}
