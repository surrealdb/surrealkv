// Note: Needs to be tested if a top-level index improves performance as such.
// TODO: Replace the current non-partitioned index block writer with this
use std::cmp::Ordering;
use std::io::Write;
use std::sync::Arc;

use crate::comparator::{Comparator, InternalKeyComparator};
use crate::error::{Error, Result};
use crate::sstable::block::{Block, BlockData, BlockHandle, BlockWriter};
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
				Arc::clone(&opts.comparator),
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
			Arc::clone(&self.opts.comparator),
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
				Arc::clone(&self.opts.comparator),
			);
			let old_block = std::mem::replace(&mut self.current_block, new_block);
			self.index_blocks.push(old_block);
		}

		let mut top_level_index = BlockWriter::new(
			self.opts.block_size,
			self.opts.block_restart_interval,
			Arc::clone(&self.opts.comparator),
		);

		// Track number of partitions
		self.num_partitions = self.index_blocks.len() as u64;

		// Take ownership of index_blocks to iterate and consume
		let index_blocks = std::mem::take(&mut self.index_blocks);
		for block in index_blocks {
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

		let top_level_data = top_level_index.finish();
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
	id: u64,
	opts: Arc<Options>,
	pub(crate) blocks: Vec<BlockHandleWithKey>,
	// TODO: Fix this, as this could be problematic if the file is being shared across without any
	// mutex
	file: Arc<dyn File>,
}

impl TopLevelIndex {
	pub(crate) fn new(
		id: u64,
		opt: Arc<Options>,
		f: Arc<dyn File>,
		location: &BlockHandle,
	) -> Result<Self> {
		let block = read_table_block(Arc::clone(&opt.comparator), Arc::clone(&f), location)?;
		let iter = block.iter(false);
		let mut blocks = Vec::new();
		for (key, handle) in iter {
			// Store full encoded internal key for correct partition lookup
			let (handle, _) = BlockHandle::decode(&handle)?;
			blocks.push(BlockHandleWithKey {
				separator_key: key,
				handle,
			});
		}
		Ok(TopLevelIndex {
			id,
			opts: opt,
			blocks,
			file: Arc::clone(&f),
		})
	}

	pub(crate) fn find_block_handle_by_key(&self, target: &[u8]) -> Option<&BlockHandleWithKey> {
		let internal_cmp = InternalKeyComparator::new(Arc::clone(&self.opts.comparator));

		// Find the partition point in the blocks where the key would fit.
		// Uses full internal key comparison for correct partition lookup.
		let index = self.blocks.partition_point(|block| {
			internal_cmp.compare(&block.separator_key, target) == Ordering::Less
		});

		// Attempt to retrieve the block at the found index.
		self.blocks
			.get(index)
			.filter(|block| internal_cmp.compare(target, &block.separator_key) != Ordering::Greater)
	}

	pub(crate) fn load_block(&self, block_handle: &BlockHandleWithKey) -> Result<Arc<Block>> {
		if let Some(block) = self.opts.block_cache.get_index_block(self.id, block_handle.offset()) {
			return Ok(block);
		}

		let block_data = read_table_block(
			Arc::clone(&self.opts.comparator),
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
		let Some(block_handle) = self.find_block_handle_by_key(target) else {
			return Err(Error::BlockNotFound);
		};

		let block = self.load_block(block_handle)?;
		Ok(block)
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use test_log::test;

	use super::*;
	use crate::sstable::{InternalKey, InternalKeyKind};

	fn wrap_buffer(src: Vec<u8>) -> Arc<dyn File> {
		Arc::new(src)
	}

	fn create_internal_key(user_key: Vec<u8>, sequence: u64) -> Vec<u8> {
		InternalKey::new(user_key, sequence, InternalKeyKind::Set, 0).encode()
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
	//     assert!(!top_level_block.is_empty()); // Top-level block should still be
	// created }

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
	//     let top_level_block = writer.finish(&mut d, CompressionType::None,
	// 0).unwrap();     assert!(!top_level_block.0.offset > 0);

	//     let f = wrap_buffer(d);
	//     let top_level_index = TopLevelIndex::new(0, opts, f,
	// &top_level_block.0).unwrap();     let block =
	// top_level_index.get(&key1).unwrap();     // println!("block: {:?}",
	// block.block); }

	#[test]
	fn test_find_block_handle_by_key() {
		let opts = Arc::new(Options::default());
		let d = Vec::new();
		let f = wrap_buffer(d);

		// Create separator keys as full encoded internal keys
		let sep_c = create_internal_key(b"c".to_vec(), 1);
		let sep_f = create_internal_key(b"f".to_vec(), 1);
		let sep_j = create_internal_key(b"j".to_vec(), 1);

		// Initialize TopLevelIndex with predefined blocks using encoded internal keys
		let index = TopLevelIndex {
			id: 0,
			opts,
			blocks: vec![
				BlockHandleWithKey::new(sep_c.clone(), BlockHandle::new(0, 10)),
				BlockHandleWithKey::new(sep_f.clone(), BlockHandle::new(10, 10)),
				BlockHandleWithKey::new(sep_j.clone(), BlockHandle::new(20, 10)),
			],
			file: f.clone(),
		};

		// A list of tuples where the first element is the encoded internal key to find,
		// and the second element is the expected separator key result.
		let test_cases: Vec<(Vec<u8>, Option<Vec<u8>>)> = vec![
			(create_internal_key(b"a".to_vec(), 1), Some(sep_c.clone())),
			(create_internal_key(b"c".to_vec(), 1), Some(sep_c.clone())),
			(create_internal_key(b"d".to_vec(), 1), Some(sep_f.clone())),
			(create_internal_key(b"e".to_vec(), 1), Some(sep_f.clone())),
			(create_internal_key(b"f".to_vec(), 1), Some(sep_f.clone())),
			(create_internal_key(b"g".to_vec(), 1), Some(sep_j.clone())),
			(create_internal_key(b"j".to_vec(), 1), Some(sep_j.clone())),
			(create_internal_key(b"z".to_vec(), 1), None),
		];

		for (key, expected) in test_cases.iter() {
			let result = index.find_block_handle_by_key(key);
			match expected {
				Some(expected_sep_key) => {
					let handle = result.expect("Expected a block handle but got None");
					assert_eq!(&handle.separator_key, expected_sep_key, "Mismatch for key {key:?}");
				}
				None => assert!(result.is_none(), "Expected None for key {key:?}, but got Some"),
			}
		}
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
		let index = TopLevelIndex::new(0, opts, file, &top_level_handle).unwrap();

		// Test lookups for various keys using encoded internal keys
		for (key, _) in &entries {
			let internal_key = create_internal_key(key.as_bytes().to_vec(), 1);
			let block = index.get(&internal_key).unwrap();
			assert!(block.size() > 0, "Block should not be empty for key {key}");

			// Verify the block contains the expected handle by checking if we can find it
			let mut block_iter = block.iter(false);
			block_iter.seek(&internal_key);
			assert!(block_iter.valid(), "Block iterator should be valid for key {key}");
		}

		// Test lookup for non-existent key before range
		let key_before = create_internal_key(b"key_000".to_vec(), 1);
		let block = index.get(&key_before).unwrap();
		assert!(block.size() > 0, "Should find first block for key before range");

		// Test lookup for non-existent key after range
		let key_after = create_internal_key(b"key_999".to_vec(), 1);
		match index.get(&key_after) {
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
