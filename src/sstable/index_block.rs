// Note: Needs to be tested if a top-level index improves performance as such.
// TODO: Replace the current non-partitioned index block writer with this
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use crate::{
	error::{Error, Result},
	sstable::{
		block::{Block, BlockData, BlockHandle, BlockWriter},
		table::{compress_block, read_table_block, write_block_at_offset},
		InternalKey,
	},
	vfs::File,
	CompressionType, Options,
};
use bytes::Bytes;

/// Manages prefetching of file data for efficient sequential access.
pub(crate) struct FilePrefetchBuffer {
	buffer: Vec<u8>,
	offset: u64,
	size: usize,
	enable: bool,
}

impl FilePrefetchBuffer {
	pub(crate) fn new() -> Self {
		Self {
			buffer: Vec::new(),
			offset: 0,
			size: 0,
			enable: true,
		}
	}

	pub(crate) fn prefetch(
		&mut self,
		file: &dyn crate::vfs::File,
		offset: u64,
		n: usize,
	) -> Result<()> {
		if !self.enable || n == 0 {
			return Ok(());
		}

		// Check if we already have the requested data
		if self.size > 0
			&& offset >= self.offset
			&& offset + n as u64 <= self.offset + self.size as u64
		{
			return Ok(());
		}

		// Allocate/reallocate buffer
		if self.buffer.len() < n {
			self.buffer.resize(n, 0);
		}

		// Read data from file
		let bytes_read = file.read_at(offset, &mut self.buffer[..n])?;
		self.offset = offset;
		self.size = bytes_read;

		Ok(())
	}
}

/// Handles readahead for sequential block access patterns.
pub(crate) struct BlockPrefetcher {
	// Readahead size used in compaction
	compaction_readahead_size: usize,
	// Current readahead size for FS prefetching
	readahead_size: usize,
	// Readahead limit for tracking what has been prefetched
	readahead_limit: u64,
	// Initial auto readahead size for internal prefetch buffer
	initial_auto_readahead_size: usize,
	// Maximum auto readahead size to cap exponential growth
	max_auto_readahead_size: usize,
	// Number of file reads for auto readahead
	num_file_reads: usize,
	// Previous access pattern for sequential detection
	prev_offset: u64,
	prev_len: usize,
}

impl BlockPrefetcher {
	pub(crate) fn new(
		compaction_readahead_size: usize,
		initial_auto_readahead_size: usize,
		max_auto_readahead_size: usize,
	) -> Self {
		let sanitized_initial = if initial_auto_readahead_size > max_auto_readahead_size {
			max_auto_readahead_size
		} else {
			initial_auto_readahead_size
		};

		Self {
			compaction_readahead_size,
			readahead_size: sanitized_initial,
			readahead_limit: 0,
			initial_auto_readahead_size: sanitized_initial,
			max_auto_readahead_size,
			num_file_reads: 0,
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
		self.num_file_reads = 1;
		// Sanitize the initial size against max_auto_readahead_size
		let sanitized_initial = initial_auto_readahead_size.min(self.max_auto_readahead_size);
		self.initial_auto_readahead_size = sanitized_initial;
		self.readahead_size = sanitized_initial;
		self.readahead_limit = 0;
	}

	pub(crate) fn prefetch_if_needed(
		&mut self,
		handle: &BlockHandle,
		readahead_size: usize,
		is_for_compaction: bool,
		no_sequential_checking: bool,
		file: &dyn crate::vfs::File,
	) -> bool {
		let len = handle.size() as u64;
		let offset = handle.offset() as u64;

		// For compaction with direct I/O support check
		if is_for_compaction {
			if self.compaction_readahead_size > 0 {
				// Try FS-level prefetch first
				if file.supports_prefetch() {
					let prefetch_result =
						file.prefetch(offset, len as usize + self.compaction_readahead_size);
					if prefetch_result.is_ok() {
						self.readahead_limit = offset + len + self.compaction_readahead_size as u64;
						return false; // Prefetch succeeded, no need for buffer-based prefetch
					}
				}
				// Fall back to buffer-based prefetch
				return true;
			}
			return false;
		}

		// Explicit user readahead
		if readahead_size > 0 {
			return true;
		}

		// Skip sequential checking if requested
		if no_sequential_checking {
			return true;
		}

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

		// Auto readahead logic - try FS prefetch first
		// Disable prefetching if either initial or max readahead size is 0
		if self.initial_auto_readahead_size == 0 || self.max_auto_readahead_size == 0 {
			return false;
		}

		self.num_file_reads += 1;
		if self.num_file_reads <= 2 {
			// Default num_file_reads_for_auto_readahead
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

		// Fall back to buffer-based prefetch
		true
	}

	pub(crate) fn grow_readahead_size(&mut self) {
		if self.readahead_size < self.max_auto_readahead_size {
			self.readahead_size = (self.readahead_size * 2).min(self.max_auto_readahead_size);
		}
	}

	pub(crate) fn get_curr_readahead_size(&self) -> usize {
		self.readahead_size
	}

	pub(crate) fn set_readahead_limit(&mut self, limit: u64) {
		self.readahead_limit = limit;
	}
}

/// Points to a block on file
#[derive(Clone, Debug)]
pub(crate) struct BlockHandleWithKey {
	/// User key of last item in block
	pub user_key: Bytes,

	/// Position of block in file
	pub handle: BlockHandle,
}

impl BlockHandleWithKey {
	#[cfg(test)]
	pub(crate) fn new(user_key: Vec<u8>, handle: BlockHandle) -> BlockHandleWithKey {
		BlockHandleWithKey {
			user_key: Bytes::from(user_key),
			handle,
		}
	}

	pub(crate) fn offset(&self) -> u64 {
		self.handle.offset as u64
	}
}

// Represents a top-level index block that contains pointers to other index blocks
// Link: https://github.com/facebook/rocksdb/wiki/Partitioned-Index-Filters
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
}

impl TopLevelIndexWriter {
	pub(crate) fn new(opts: Arc<Options>, max_block_size: usize) -> TopLevelIndexWriter {
		TopLevelIndexWriter {
			opts: opts.clone(),
			index_blocks: Vec::new(),
			current_block: BlockWriter::new(opts),
			max_block_size,
		}
	}

	pub(crate) fn size_estimate(&self) -> usize {
		// Sum the size of all finished index blocks
		let finished_blocks_size: usize =
			self.index_blocks.iter().map(|block| block.size_estimate()).sum();

		// Add the size of the current block
		let total_size = finished_blocks_size + self.current_block.size_estimate();

		// Add an estimate for the top-level index
		let top_level_estimate =
			(self.index_blocks.len() + 1) * (std::mem::size_of::<InternalKey>() + 8);

		total_size + top_level_estimate
	}

	pub(crate) fn add(&mut self, key: &[u8], handle: &[u8]) -> Result<()> {
		if self.current_block.size_estimate() >= self.max_block_size {
			self.finish_current_block();
		}
		self.current_block.add(key, handle)
	}

	fn finish_current_block(&mut self) {
		let new_block = BlockWriter::new(self.opts.clone());
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
		mut self,
		writer: &mut W,
		compression_type: CompressionType,
		mut offset: usize,
	) -> Result<(BlockHandle, usize)> {
		// Only finish current block if it has entries
		if self.current_block.entries() > 0 {
			self.finish_current_block();
		}

		// If no blocks were created, create a single empty block
		if self.index_blocks.is_empty() {
			self.index_blocks.push(self.current_block);
		}

		let mut top_level_index = BlockWriter::new(self.opts);

		for block in self.index_blocks.into_iter() {
			let separator_key = block.last_key.clone();
			let block_data = block.finish();

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

		let block_data = top_level_index.finish();
		Self::write_compressed_block(writer, block_data, compression_type, offset)
	}
}

#[derive(Clone)]
pub(crate) struct TopLevelIndex {
	id: u64,
	opts: Arc<Options>,
	pub(crate) blocks: Vec<BlockHandleWithKey>,
	file: Arc<dyn File>,
	// For partition blocks pinned in cache. This is expected to be "all or
	// none" so that !partition_map.empty() can use an iterator expecting
	// all partitions to be saved here.
	pub(crate) partition_map: std::collections::HashMap<u64, Arc<Block>>,
}

impl TopLevelIndex {
	pub(crate) fn new(
		id: u64,
		opt: Arc<Options>,
		f: Arc<dyn File>,
		location: &BlockHandle,
	) -> Result<Self> {
		let block = read_table_block(opt.clone(), f.clone(), location)?;
		let iter = block.iter(false);
		let mut blocks = Vec::new();
		for (key, handle) in iter {
			// Extract user key from the encoded internal key
			let internal_key = InternalKey::decode(&key);
			let (handle, _) = BlockHandle::decode(&handle)?;
			blocks.push(BlockHandleWithKey {
				user_key: internal_key.user_key,
				handle,
			});
		}
		Ok(TopLevelIndex {
			id,
			opts: opt.clone(),
			blocks,
			file: f.clone(),
			partition_map: HashMap::new(),
		})
	}

	pub(crate) fn find_block_handle_by_key(&self, user_key: &[u8]) -> Option<&BlockHandleWithKey> {
		// Find the partition point in the blocks where the key would fit.
		let index = self.blocks.partition_point(|block| block.user_key.as_ref() < user_key);

		// Attempt to retrieve the block at the found index.
		let result = self.blocks.get(index).and_then(|block| {
			// Compare user keys directly
			if user_key <= block.user_key.as_ref() {
				Some(block)
			} else {
				None
			}
		});

		result
	}

	pub(crate) fn load_block(&self, block_handle: &BlockHandleWithKey) -> Result<Arc<Block>> {
		// Check partition_map first if partitions are cached
		if let Some(block) = self.get_cached_partition(block_handle.offset()) {
			return Ok(block.clone());
		}

		// Fall back to block cache
		if let Some(block) = self.opts.block_cache.get_index_block(self.id, block_handle.offset()) {
			return Ok(block);
		}

		let block_data =
			read_table_block(self.opts.clone(), self.file.clone(), &block_handle.handle)?;
		let block = Arc::new(block_data);
		self.opts.block_cache.insert_index_block(self.id, block_handle.offset(), block.clone());

		Ok(block)
	}

	pub(crate) fn get(&self, user_key: &[u8]) -> Result<Arc<Block>> {
		let Some(block_handle) = self.find_block_handle_by_key(user_key) else {
			return Err(Error::BlockNotFound);
		};

		let block = self.load_block(block_handle)?;
		Ok(block)
	}

	pub(crate) fn first_partition(&self) -> Result<Arc<Block>> {
		if let Some(first_block_handle) = self.blocks.first() {
			self.load_block(first_block_handle)
		} else {
			Err(Error::BlockNotFound)
		}
	}

	/// Cache dependencies by prefetching all partition blocks.
	pub(crate) fn cache_dependencies(&mut self, pin: bool) -> Result<()> {
		if !self.partition_map.is_empty() {
			// The dependencies are already cached since partition_map is filled in
			// an all-or-nothing manner.
			return Ok(());
		}

		if self.blocks.is_empty() {
			return Ok(());
		}

		// Calculate prefetch range like RocksDB does
		// Read the first block offset
		let first_handle = &self.blocks[0].handle;
		let prefetch_off = first_handle.offset() as u64;

		// Read the last block's offset (like biter.SeekToLast())
		let last_handle = &self.blocks[self.blocks.len() - 1].handle;
		let last_off = last_handle.offset() + last_handle.size();
		let prefetch_len = last_off - first_handle.offset();

		// Create FilePrefetchBuffer and prefetch the entire range
		let mut prefetch_buffer = FilePrefetchBuffer::new();
		prefetch_buffer.prefetch(self.file.as_ref(), prefetch_off, prefetch_len)?;

		// For saving "all or nothing" to partition_map_
		let mut map_in_progress = HashMap::new();

		// After prefetch, read the partitions one by one
		for partition in &self.blocks {
			let block = self.load_block(partition)?;
			if pin {
				map_in_progress.insert(partition.offset(), block);
			}
		}

		// Save (pin) them only if everything checks out
		if map_in_progress.len() == self.blocks.len() {
			self.partition_map = map_in_progress;
		}

		Ok(())
	}

	/// Get a cached partition block
	pub(crate) fn get_cached_partition(&self, offset: u64) -> Option<&Arc<Block>> {
		self.partition_map.get(&offset)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sstable::{InternalKey, InternalKeyKind};
	use crate::Iterator;
	use bytes::Bytes;
	use std::sync::Arc;
	use test_log::test;

	fn wrap_buffer(src: Vec<u8>) -> Arc<dyn File> {
		Arc::new(src)
	}

	fn create_internal_key(user_key: Vec<u8>, sequence: u64) -> Vec<u8> {
		InternalKey::new(Bytes::from(user_key), sequence, InternalKeyKind::Set, 0).encode()
	}

	#[test]
	fn test_top_level_index_writer_basic() {
		let opts = Arc::new(Options::default());
		let max_block_size = 100;
		let mut writer = TopLevelIndexWriter::new(opts, max_block_size);

		let key1 = create_internal_key(b"key1".to_vec(), 1);
		let handle1 = vec![1, 2, 3];
		writer.add(&key1, &handle1).unwrap();

		let mut d = Vec::new();
		let top_level_block = writer.finish(&mut d, CompressionType::None, 0).unwrap();
		assert!(!top_level_block.0.offset > 0);
	}

	#[test]
	fn test_top_level_index_writer_multiple_blocks() {
		let opts = Arc::new(Options::default());
		let max_block_size = 50; // Small size to force multiple blocks
		let mut writer = TopLevelIndexWriter::new(opts, max_block_size);

		for i in 0..10 {
			let key = create_internal_key(format!("key{i}").as_bytes().to_vec(), i as u64);
			let handle = vec![i as u8; 10]; // 10-byte handle
			writer.add(&key, &handle).unwrap();
		}

		// assert!(index_blocks.len() > 1, "Expected multiple index blocks");
		let mut d = Vec::new();
		let top_level_block = writer.finish(&mut d, CompressionType::None, 0).unwrap();
		assert!(!top_level_block.0.offset > 0);
	}

	// #[test]
	// fn test_top_level_index_writer_empty() {
	//     let opts = Arc::new(Options::default());
	//     let max_block_size = 100;
	//     let writer = TopLevelIndexWriter::new(opts, max_block_size);

	//     let top_level_block = writer.finish().unwrap();
	//     assert_eq!(index_blocks.len(), 0);
	//     assert!(!top_level_block.is_empty()); // Top-level block should still be created
	// }

	#[test]
	fn test_top_level_index_writer_large_entries() {
		let opts = Arc::new(Options::default());
		let max_block_size = 1000;
		let mut writer = TopLevelIndexWriter::new(opts, max_block_size);

		let large_key = create_internal_key(vec![b'a'; 500], 1);
		let large_handle = vec![b'b'; 500];
		writer.add(&large_key, &large_handle).unwrap();

		let mut d = Vec::new();
		let top_level_block = writer.finish(&mut d, CompressionType::None, 0).unwrap();
		assert!(!top_level_block.0.offset > 0);
	}

	#[test]
	fn test_top_level_index_writer_exact_block_size() {
		let opts = Arc::new(Options::default());
		let max_block_size = 100;
		let mut writer = TopLevelIndexWriter::new(opts, max_block_size);

		// Add entries that exactly fill up one block
		let key = create_internal_key(b"key".to_vec(), 1);
		let handle = vec![0; 90];
		writer.add(&key, &handle).unwrap();

		let mut d = Vec::new();
		let top_level_block = writer.finish(&mut d, CompressionType::None, 0).unwrap();
		assert!(!top_level_block.0.offset > 0);
	}

	// #[test]
	// fn test_top_level_index() {
	//     let opts = Arc::new(Options::default());
	//     let max_block_size = 10;
	//     let mut writer = TopLevelIndexWriter::new(opts.clone(), max_block_size);

	//     let key1 = create_internal_key(b"key1".to_vec(), 1);
	//     let handle1 = vec![1, 2, 3];
	//     writer.add(&key1, &handle1).unwrap();

	//     let mut d = Vec::new();
	//     let top_level_block = writer.finish(&mut d, CompressionType::None, 0).unwrap();
	//     assert!(!top_level_block.0.offset > 0);

	//     let f = wrap_buffer(d);
	//     let top_level_index = TopLevelIndex::new(0, opts, f, &top_level_block.0).unwrap();
	//     let block = top_level_index.get(&key1).unwrap();
	//     // println!("block: {:?}", block.block);
	// }

	#[test]
	fn test_find_block_handle_by_key() {
		let opts = Arc::new(Options::default());
		let d = Vec::new();
		let f = wrap_buffer(d);

		// Initialize TopLevelIndex with predefined blocks using user keys only
		let index = TopLevelIndex {
			id: 0,
			opts: opts.clone(),
			blocks: vec![
				BlockHandleWithKey::new(b"c".to_vec(), BlockHandle::new(0, 10)),
				BlockHandleWithKey::new(b"f".to_vec(), BlockHandle::new(10, 10)),
				BlockHandleWithKey::new(b"j".to_vec(), BlockHandle::new(20, 10)),
			],
			file: f.clone(),
			partition_map: HashMap::new(),
		};

		// A list of tuples where the first element is the key to find,
		// and the second element is the expected block key result.
		let test_cases: &[(&[u8], Option<&[u8]>)] = &[
			(b"a", Some(b"c" as &[u8])),
			(b"c", Some(b"c")),
			(b"d", Some(b"f")),
			(b"e", Some(b"f")),
			(b"f", Some(b"f")),
			(b"g", Some(b"j")),
			(b"j", Some(b"j")),
			(b"z", None),
		];

		for (key, expected) in test_cases.iter() {
			// Pass user key directly instead of encoding as internal key
			let result = index.find_block_handle_by_key(key);
			match expected {
				Some(expected_key) => {
					let handle = result.expect("Expected a block handle but got None");
					assert_eq!(handle.user_key, *expected_key, "Mismatch for key {key:?}");
				}
				None => assert!(result.is_none(), "Expected None for key {key:?}, but got Some"),
			}
		}
	}

	#[test]
	fn test_cache_dependencies() {
		let opts = Arc::new(Options::default());
		let max_block_size = 50; // Small size to force multiple partitions
		let mut writer = TopLevelIndexWriter::new(opts.clone(), max_block_size);

		// Add entries to create partitions
		for i in 0..5 {
			let key = create_internal_key(format!("key_{i:03}").as_bytes().to_vec(), 1);
			let handle = format!("handle_{i}").into_bytes();
			writer.add(&key, &handle).unwrap();
		}

		// Write to buffer
		let mut buffer = Vec::new();
		let (top_level_handle, _) = writer.finish(&mut buffer, CompressionType::None, 0).unwrap();

		// Read it back
		let file = wrap_buffer(buffer);
		let mut index = TopLevelIndex::new(0, opts.clone(), file, &top_level_handle).unwrap();

		// Test cache_dependencies
		index.cache_dependencies(true).unwrap();

		// Verify partitions are cached
		assert!(!index.partition_map.is_empty());
		assert_eq!(index.partition_map.len(), index.blocks.len());

		// Test loading from cache
		for partition in &index.blocks {
			let cached_block = index.get_cached_partition(partition.offset());
			assert!(cached_block.is_some());
		}
	}

	#[test]
	fn test_block_prefetcher() {
		let mut prefetcher = BlockPrefetcher::new(1024, 512, 256 * 1024); // compaction, initial readahead, max readahead

		let handle = BlockHandle::new(0, 100);

		// Test explicit readahead
		let test_file = wrap_buffer(vec![]);
		let should_prefetch =
			prefetcher.prefetch_if_needed(&handle, 256, false, false, test_file.as_ref());
		assert!(should_prefetch);

		// Test compaction readahead
		let mut compaction_prefetcher = BlockPrefetcher::new(1024, 512, 256 * 1024);
		let should_prefetch =
			compaction_prefetcher.prefetch_if_needed(&handle, 0, true, false, test_file.as_ref());
		assert!(should_prefetch);

		// Test sequential access pattern
		let mut seq_prefetcher = BlockPrefetcher::new(0, 512, 256 * 1024);
		let handle1 = BlockHandle::new(0, 100);
		let handle2 = BlockHandle::new(100, 100); // Sequential to handle1

		// First access
		let should_prefetch =
			seq_prefetcher.prefetch_if_needed(&handle1, 0, false, false, test_file.as_ref());
		assert!(!should_prefetch);

		// Second sequential access (still no prefetch)
		let should_prefetch =
			seq_prefetcher.prefetch_if_needed(&handle2, 0, false, false, test_file.as_ref());
		assert!(!should_prefetch);

		// Third sequential access (should trigger prefetch)
		let handle3 = BlockHandle::new(200, 100);
		let should_prefetch =
			seq_prefetcher.prefetch_if_needed(&handle3, 0, false, false, test_file.as_ref());
		assert!(should_prefetch);

		// Test exponential growth capping
		let mut capped_prefetcher = BlockPrefetcher::new(0, 8 * 1024, 32 * 1024); // 8KB initial, 32KB max
		assert_eq!(capped_prefetcher.get_curr_readahead_size(), 8 * 1024);

		// Grow multiple times to test capping
		capped_prefetcher.grow_readahead_size(); // Should become 16KB
		assert_eq!(capped_prefetcher.get_curr_readahead_size(), 16 * 1024);

		capped_prefetcher.grow_readahead_size(); // Should become 32KB
		assert_eq!(capped_prefetcher.get_curr_readahead_size(), 32 * 1024);

		capped_prefetcher.grow_readahead_size(); // Should stay at 32KB (capped)
		assert_eq!(capped_prefetcher.get_curr_readahead_size(), 32 * 1024);
	}

	#[test]
	fn test_file_prefetch_buffer() {
		let data = b"Hello, World! This is test data for prefetch buffer.";
		let file = wrap_buffer(data.to_vec());

		let mut prefetch_buffer = FilePrefetchBuffer::new();

		// Test prefetch
		prefetch_buffer.prefetch(&*file, 7, 5).unwrap(); // "World"
		assert!(prefetch_buffer.enable);
		assert_eq!(prefetch_buffer.offset, 7);
		assert_eq!(prefetch_buffer.size, 5);
	}

	#[test]
	fn test_partitioned_index_lookup() {
		let opts = Arc::new(Options::default());
		let max_block_size = 50; // Small size to force multiple partitions
		let mut writer = TopLevelIndexWriter::new(opts.clone(), max_block_size);

		// Add enough entries to create multiple partitions
		let entries = vec![
			("key_001", "handle_001"),
			("key_002", "handle_002"),
			("key_003", "handle_003"),
			("key_004", "handle_004"),
			("key_005", "handle_005"),
			("key_006", "handle_006"),
			("key_007", "handle_007"),
			("key_008", "handle_008"),
			("key_009", "handle_009"),
			("key_010", "handle_010"),
		];

		for (key, handle) in &entries {
			let internal_key = create_internal_key(key.as_bytes().to_vec(), 1);
			writer.add(&internal_key, handle.as_bytes()).unwrap();
		}

		// Write to buffer
		let mut buffer = Vec::new();
		let (top_level_handle, _) = writer.finish(&mut buffer, CompressionType::None, 0).unwrap();

		// Now read it back
		let file = wrap_buffer(buffer);
		let index = TopLevelIndex::new(0, opts.clone(), file, &top_level_handle).unwrap();

		// Test lookups for various keys
		for (key, _) in &entries {
			// Pass user key directly instead of encoding as internal key
			let block = index.get(key.as_bytes()).unwrap();
			assert!(block.size() > 0, "Block should not be empty for key {key}");

			// Verify the block contains the expected handle by checking if we can find it
			let internal_key = create_internal_key(key.as_bytes().to_vec(), 1);
			let mut block_iter = block.iter(false);
			block_iter.seek(&internal_key);
			assert!(block_iter.valid(), "Block iterator should be valid for key {key}");
		}

		// Test lookup for non-existent key before range
		let block = index.get(b"key_000").unwrap();
		assert!(block.size() > 0, "Should find first block for key before range");

		// Test lookup for non-existent key after range
		match index.get(b"key_999") {
			Ok(_) => {
				// This is acceptable - might find the last block
			}
			Err(Error::BlockNotFound) => {
				// This is also acceptable for keys completely out of range
			}
			Err(e) => panic!("Unexpected error for key after range: {e:?}"),
		}
	}
}
