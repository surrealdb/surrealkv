//! # Partitioned Index Implementation
//!
//! This module implements a two-level partitioned index for SSTable files.
//! The partitioned index improves cache efficiency for large tables by splitting
//! the index into smaller, cacheable partition blocks.
//!
//! ## Index Structure Overview
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                     Partitioned Index Structure                          │
//! ├─────────────────────────────────────────────────────────────────────────┤
//! │                                                                          │
//! │  Top-Level Index Block:                                                  │
//! │  ┌─────────────────────────────────────────────────────────────────┐    │
//! │  │ Entry: last_key_P1 → Partition Block 1 handle                   │    │
//! │  │ Entry: last_key_P2 → Partition Block 2 handle                   │    │
//! │  │ ...                                                              │    │
//! │  │ Entry: last_key_PN → Partition Block N handle                   │    │
//! │  └─────────────────────────────────────────────────────────────────┘    │
//! │           │                    │                    │                    │
//! │           ▼                    ▼                    ▼                    │
//! │  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐             │
//! │  │ Partition 1 │      │ Partition 2 │      │ Partition N │             │
//! │  ├─────────────┤      ├─────────────┤      ├─────────────┤             │
//! │  │ sep_1 → D1  │      │ sep_4 → D4  │      │ sep_7 → D7  │             │
//! │  │ sep_2 → D2  │      │ sep_5 → D5  │      │ sep_8 → D8  │             │
//! │  │ sep_3 → D3  │      │ sep_6 → D6  │      │ sep_9 → D9  │             │
//! │  └─────────────┘      └─────────────┘      └─────────────┘             │
//! │                                                                          │
//! │  Where: sep_N = separator key (upper bound for data block N)            │
//! │         D_N   = data block N handle (offset + size)                     │
//! │                                                                          │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Lookup Example
//!
//! Given this structure:
//! ```text
//! Top-Level: [("c", P1), ("fig", P2)]
//! P1: [("b", D1), ("c", D2)]
//! P2: [("date", D3), ("fig", D4)]
//! D1: ["apple", "apricot"]
//! D2: ["banana", "cherry"]  -- Note: actual keys, not separators!
//! D3: ["date", "elderberry"]
//! D4: ["fig", "grape"]
//! ```
//!
//! Looking up "cherry":
//! 1. Top-level: "cherry" <= "c"? No. "cherry" <= "fig"? Yes → P2 Wait, that's wrong! Let me
//!    reconsider...
//!
//! Actually "cherry" > "c" alphabetically? Let's check:
//! - 'c' = 99, 'c' = 99, 'h' = 104 vs nothing
//! - "cherry" > "c" because it's longer and starts with same char
//!
//! So: "cherry" <= "c"? No → "cherry" <= "fig"? Yes → P2
//!
//! Hmm, but "cherry" should be in D2 which is under P1...
//!
//! This is why the example needs to be corrected. Let me fix:
//!
//! ## Corrected Lookup Example
//!
//! ```text
//! Top-Level: [("cherry", P1), ("grape", P2)]
//!                  ↑ last key in P1
//! P1: [("apricot", D1), ("cherry", D2)]
//! P2: [("elderberry", D3), ("grape", D4)]
//! D1: ["apple", "apricot"]
//! D2: ["banana", "cherry"]
//! D3: ["date", "elderberry"]
//! D4: ["fig", "grape"]
//! ```
//!
//! Looking up "banana":
//! 1. Top-level: "banana" <= "cherry"? Yes → P1
//! 2. P1: "banana" <= "apricot"? No → "banana" <= "cherry"? Yes → D2
//! 3. D2: seek("banana") → found!

use std::cmp::Ordering;
use std::io::Write;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::sstable::block::{Block, BlockData, BlockHandle, BlockIterator, BlockWriter};
use crate::sstable::error::SSTableError;
use crate::sstable::table::{compress_block, read_table_block, write_block_at_offset};
use crate::vfs::File;
use crate::{CompressionType, Options};

// =============================================================================
// BLOCK HANDLE WITH KEY
// =============================================================================

/// A block handle paired with its separator key.
///
/// Used in the top-level index to map separator keys to partition blocks.
///
/// ## Fields
///
/// - `separator_key`: The largest key that could be in this partition (actually the last key of the
///   partition block, which is itself a separator)
/// - `handle`: File offset and size of the partition block
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

// =============================================================================
// TOP-LEVEL INDEX WRITER
// =============================================================================

/// Writes partitioned index blocks to an SSTable.
///
/// ## Writing Process
///
/// ```text
/// For each data block written:
///   1. add(separator_key, data_block_handle)
///   2. If current partition is full, start a new one
///
/// On finish():
///   1. Write all partition blocks to file
///   2. Write top-level index pointing to partitions
///   3. Return top-level index handle for footer
/// ```
///
/// ## Example: Building the Index
///
/// ```text
/// add("apricot", handle_D1)  → Added to Partition 1
/// add("cherry", handle_D2)   → Added to Partition 1
///                              Partition 1 full! Start Partition 2.
/// add("elderberry", handle_D3) → Added to Partition 2
/// add("grape", handle_D4)    → Added to Partition 2
///
/// finish():
///   Write Partition 1 → at offset 0x1000
///   Write Partition 2 → at offset 0x1100
///   Write Top-Level:
///     Entry: "cherry" → 0x1000 (P1's last key)
///     Entry: "grape"  → 0x1100 (P2's last key)
/// ```
pub(crate) struct IndexWriter {
	opts: Arc<Options>,

	/// Completed partition blocks waiting to be written
	index_blocks: Vec<BlockWriter>,

	/// Current partition block being built
	current_block: BlockWriter,

	/// Maximum size for a partition block
	max_block_size: usize,

	// Statistics (populated during finish())
	index_size: u64,
	num_partitions: u64,
	top_level_index_size: u64,
}

impl IndexWriter {
	pub(crate) fn new(opts: Arc<Options>, max_block_size: usize) -> IndexWriter {
		IndexWriter {
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

	pub(crate) fn index_size(&self) -> u64 {
		self.index_size
	}

	pub(crate) fn num_partitions(&self) -> u64 {
		self.num_partitions
	}

	pub(crate) fn top_level_index_size(&self) -> u64 {
		self.top_level_index_size
	}

	/// Adds an index entry: separator_key → data_block_handle
	///
	/// ## Parameters
	///
	/// - `key`: Separator key (upper bound for keys in the data block)
	/// - `handle`: Encoded BlockHandle of the data block
	pub(crate) fn add(&mut self, key: &[u8], handle: &[u8]) -> Result<()> {
		// Start new partition if current one is full
		if self.current_block.size_estimate() >= self.max_block_size {
			self.finish_current_block();
		}
		self.current_block.add(key, handle)
	}

	/// Moves current block to completed list and starts a new empty block.
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

	/// Writes all partition blocks and the top-level index to the file.
	///
	/// ## Process
	///
	/// ```text
	/// 1. Finish current partition if it has entries
	/// 2. For each partition block:
	///    a. Write partition to file
	///    b. Add entry to top-level: partition.last_key → partition.handle
	/// 3. Write top-level index block
	/// 4. Return top-level index handle
	/// ```
	///
	/// ## Why last_key for Top-Level Entries?
	///
	/// The top-level index uses the LAST KEY of each partition block.
	/// This works because:
	/// - Partition entries are sorted by separator key
	/// - The last entry's key is the largest in that partition
	/// - Any key <= last_key might be in this partition
	pub(crate) fn finish<W: Write>(
		&mut self,
		writer: &mut W,
		compression_type: CompressionType,
		mut offset: usize,
	) -> Result<(BlockHandle, usize)> {
		let start_offset = offset;

		// Finish current block if it has entries
		if self.current_block.entries() > 0 {
			self.finish_current_block();
		}

		// Handle edge case: no blocks created
		if self.index_blocks.is_empty() {
			let new_block = BlockWriter::new(
				self.opts.block_size,
				self.opts.block_restart_interval,
				Arc::clone(&self.opts.internal_comparator),
			);
			let old_block = std::mem::replace(&mut self.current_block, new_block);
			self.index_blocks.push(old_block);
		}

		// Build top-level index
		let mut top_level_index = BlockWriter::new(
			self.opts.block_size,
			self.opts.block_restart_interval,
			Arc::clone(&self.opts.internal_comparator),
		);

		self.num_partitions = self.index_blocks.len() as u64;

		// Write each partition block and record in top-level
		let index_blocks = std::mem::take(&mut self.index_blocks);
		for block in index_blocks {
			// Use last key of partition as separator for top-level
			let separator_key = block.last_key.clone();
			let block_data = block.finish()?;

			// Skip empty blocks
			if separator_key.is_empty() {
				continue;
			}

			// Write partition block to file
			let (block_handle, new_offset) =
				Self::write_compressed_block(writer, block_data, compression_type, offset)?;
			offset = new_offset;

			// Add to top-level: separator_key → partition_handle
			top_level_index.add(&separator_key, &block_handle.encode())?;
		}

		// Write top-level index block
		let top_level_data = top_level_index.finish()?;
		self.top_level_index_size = top_level_data.len() as u64;
		let (handle, final_offset) =
			Self::write_compressed_block(writer, top_level_data, compression_type, offset)?;

		self.index_size = (final_offset - start_offset) as u64;

		Ok((handle, final_offset))
	}
}

// =============================================================================
// TOP-LEVEL INDEX (Reader)
// =============================================================================

/// Reads and navigates the partitioned index structure.
///
/// ## Structure in Memory
///
/// ```text
/// TopLevelIndex {
///     blocks: [
///         BlockHandleWithKey { separator: "cherry", handle: P1 },
///         BlockHandleWithKey { separator: "grape",  handle: P2 },
///     ]
/// }
/// ```
///
/// The `blocks` array is loaded from the top-level index block on disk.
/// Each entry points to a partition block that can be loaded on demand.
#[derive(Clone)]
pub(crate) struct Index {
	pub(crate) id: u64,
	pub(crate) opts: Arc<Options>,

	/// Partition block handles with their separator keys
	/// Sorted by separator key in ascending order
	pub(crate) blocks: Vec<BlockHandleWithKey>,

	pub(crate) file: Arc<dyn File>,
}

impl Index {
	/// Loads the top-level index from disk.
	///
	/// ## Process
	///
	/// 1. Read the top-level index block at `location`
	/// 2. Iterate through all entries
	/// 3. For each entry: decode (separator_key, partition_handle)
	/// 4. Store in `blocks` vector for binary search during lookups
	pub(crate) fn new(
		id: u64,
		opt: Arc<Options>,
		f: Arc<dyn File>,
		location: &BlockHandle,
	) -> Result<Self> {
		// Read and parse the top-level index block
		let block =
			read_table_block(Arc::clone(&opt.internal_comparator), Arc::clone(&f), location)?;

		let mut iter = block.iter()?;
		let mut blocks = Vec::new();

		// Extract all partition entries
		iter.seek_to_first()?;
		while iter.is_valid() {
			let key = iter.key_bytes().to_vec();
			let handle_bytes = iter.value_bytes();
			let (handle, _) = BlockHandle::decode(handle_bytes)?;

			blocks.push(BlockHandleWithKey {
				separator_key: key,
				handle,
			});
			iter.advance()?;
		}

		Ok(Index {
			id,
			opts: opt,
			blocks,
			file: Arc::clone(&f),
		})
	}

	/// Finds the partition block that could contain the target key.
	///
	/// ## Algorithm
	///
	/// Uses binary search (partition_point) to find the first partition
	/// where separator_key >= target.
	///
	/// ## Example
	///
	/// ```text
	/// blocks: [("cherry", P1), ("grape", P2)]
	///
	/// find_block_handle_by_key("banana"):
	///   partition_point: find first where NOT (separator < target)
	///   - "cherry" < "banana"? No (c > b) → predicate FALSE
	///   - Result: index 0
	///   - Check: "banana" <= "cherry"? Yes → Return P1
	///
	/// find_block_handle_by_key("date"):
	///   - "cherry" < "date"? Yes (c < d) → predicate TRUE
	///   - "grape" < "date"? No (g > d) → predicate FALSE
	///   - Result: index 1
	///   - Check: "date" <= "grape"? Yes → Return P2
	///
	/// find_block_handle_by_key("zebra"):
	///   - "cherry" < "zebra"? Yes → TRUE
	///   - "grape" < "zebra"? Yes → TRUE
	///   - Result: index 2 (past end)
	///   - blocks.get(2) returns None → Return None
	/// ```
	///
	/// ## Return Value
	///
	/// - `Some((index, handle))`: Partition that could contain the key
	/// - `None`: Key is beyond all partitions
	pub(crate) fn find_block_handle_by_key(
		&self,
		target: &[u8],
	) -> Result<Option<(usize, &BlockHandleWithKey)>> {
		// Guard against empty index
		if self.blocks.is_empty() {
			let err = Error::from(SSTableError::EmptyCorruptPartitionedIndex {
				table_id: self.id,
			});
			log::error!("[INDEX] {}", err);
			return Err(err);
		}

		let internal_cmp = &self.opts.internal_comparator;

		// Binary search: find first block where separator >= target
		// partition_point returns index of first element where predicate is FALSE
		// Predicate: separator < target
		let index = self.blocks.partition_point(|block| {
			internal_cmp.compare(&block.separator_key, target) == Ordering::Less
		});

		// Check if we found a valid partition
		// The key must be <= separator for this partition to potentially contain it
		Ok(self
			.blocks
			.get(index)
			.filter(|block| internal_cmp.compare(target, &block.separator_key) != Ordering::Greater)
			.map(|block| (index, block)))
	}

	/// Loads a partition block from disk (with caching).
	pub(crate) fn load_block(&self, block_handle: &BlockHandleWithKey) -> Result<Arc<Block>> {
		// Check cache first
		if let Some(block) = self.opts.block_cache.get_index_block(self.id, block_handle.offset()) {
			return Ok(block);
		}

		// Cache miss: read from disk
		let block_data = read_table_block(
			Arc::clone(&self.opts.internal_comparator),
			Arc::clone(&self.file),
			&block_handle.handle,
		)?;
		let block = Arc::new(block_data);

		// Insert into cache
		self.opts.block_cache.insert_index_block(
			self.id,
			block_handle.offset(),
			Arc::clone(&block),
		);

		Ok(block)
	}
}

// =============================================================================
// PARTITIONED INDEX ITERATOR
// =============================================================================

/// Iterates over ALL index entries across all partition blocks.
///
/// ## Purpose
///
/// IndexIterator does the following:
/// 1. Iterating through partition blocks sequentially
/// 2. Within each partition, iterating through entries
/// 3. Automatically crossing partition boundaries
///
/// ## Structure
///
/// ```text
/// IndexIterator
/// ├── index: &Index          (for loading partition blocks)
/// ├── partition_index: usize          (current partition block index)
/// └── partition_iter: BlockIterator   (iterator within current partition)
/// ```
///
/// ## Iteration Example
///
/// ```text
/// Partitions:
///   P1: [("apricot", D1), ("cherry", D2)]
///   P2: [("elderberry", D3), ("grape", D4)]
///
/// Iterator sequence:
///   1. partition_index=0, partition_iter at "apricot"
///   2. next() → partition_iter at "cherry"
///   3. next() → partition_iter exhausted!
///              → partition_index=1, load P2
///              → partition_iter at "elderberry"
///   4. next() → partition_iter at "grape"
///   5. next() → partition_iter exhausted, partition_index=2 (past end)
///              → iterator invalid
/// ```
pub(crate) struct IndexIterator<'a> {
	index: &'a Index,

	/// Current partition block index in index.blocks[]
	partition_index: usize,

	/// Iterator within the current partition block
	/// None if not yet positioned or exhausted
	partition_iter: Option<BlockIterator>,
}

impl<'a> IndexIterator<'a> {
	pub(crate) fn new(index: &'a Index) -> Self {
		Self {
			index,
			partition_index: 0,
			partition_iter: None,
		}
	}

	/// Returns true if positioned on a valid entry.
	pub(crate) fn valid(&self) -> bool {
		self.partition_iter.as_ref().is_some_and(|iter| iter.is_valid())
	}

	/// Returns the current key (separator key from the index entry).
	#[allow(dead_code)]
	pub(crate) fn key(&self) -> &[u8] {
		debug_assert!(self.valid());
		self.partition_iter.as_ref().unwrap().key_bytes()
	}

	/// Returns the current value as a BlockHandle (data block handle).
	///
	/// ## Note
	///
	/// The "value" of an index entry is the encoded BlockHandle
	/// pointing to the corresponding data block.
	pub(crate) fn block_handle(&self) -> Result<BlockHandle> {
		debug_assert!(self.valid());
		let val = self.partition_iter.as_ref().unwrap().value_bytes();
		let (handle, _) = BlockHandle::decode(val)?;
		Ok(handle)
	}

	/// Seeks to the first entry across all partitions.
	pub(crate) fn seek_to_first(&mut self) -> Result<()> {
		if self.index.blocks.is_empty() {
			self.partition_iter = None;
			let err = Error::from(SSTableError::EmptyCorruptPartitionedIndex {
				table_id: self.index.id,
			});
			log::error!("[INDEX] {}", err);
			return Err(err);
		}

		// Load first partition
		self.partition_index = 0;
		let block = self.index.load_block(&self.index.blocks[0])?;
		let mut iter = block.iter()?;
		iter.seek_to_first()?;
		self.partition_iter = Some(iter);

		// Handle empty first partition
		if !self.valid() {
			self.next()?;
		}
		Ok(())
	}

	/// Seeks to the last entry across all partitions.
	pub(crate) fn seek_to_last(&mut self) -> Result<()> {
		if self.index.blocks.is_empty() {
			self.partition_iter = None;
			let err = Error::from(SSTableError::EmptyCorruptPartitionedIndex {
				table_id: self.index.id,
			});
			log::error!("[INDEX] {}", err);
			return Err(err);
		}

		// Load last partition
		self.partition_index = self.index.blocks.len() - 1;
		let block = self.index.load_block(&self.index.blocks[self.partition_index])?;
		let mut iter = block.iter()?;
		iter.seek_to_last()?;
		self.partition_iter = Some(iter);

		// Handle empty last partition
		if !self.valid() {
			self.prev()?;
		}
		Ok(())
	}

	/// Seeks to the first entry >= target.
	///
	/// ## Algorithm
	///
	/// 1. Use top-level index to find the right partition
	/// 2. Load that partition block
	/// 3. Seek within the partition for target
	/// 4. If past end of partition, advance to next
	///
	/// ## Example
	///
	/// ```text
	/// Top-level: [("cherry", P1), ("grape", P2)]
	/// P1: [("apricot", D1), ("cherry", D2)]
	/// P2: [("elderberry", D3), ("grape", D4)]
	///
	/// seek("date"):
	///   1. find_block_handle_by_key("date"):
	///      - "cherry" < "date"? Yes
	///      - "grape" < "date"? No
	///      - Result: index 1 (P2)
	///   2. Load P2, seek for "date":
	///      - "elderberry" >= "date"? Yes → found!
	///   3. Iterator at ("elderberry", D3)
	/// ```
	pub(crate) fn seek(&mut self, target: &[u8]) -> Result<()> {
		if self.index.blocks.is_empty() {
			self.partition_iter = None;
			let err = Error::from(SSTableError::EmptyCorruptPartitionedIndex {
				table_id: self.index.id,
			});
			log::error!("[INDEX] {}", err);
			return Err(err);
		}

		// Find the right partition using top-level index
		match self.index.find_block_handle_by_key(target)? {
			Some((idx, block_handle)) => {
				self.partition_index = idx;
				let block = self.index.load_block(block_handle)?;
				let mut iter = block.iter()?;

				// Seek within partition
				iter.seek_internal(target)?;
				self.partition_iter = Some(iter);

				// If past end of this partition, move to next
				// This handles the "gap" case where target is between
				// this partition's actual entries and its separator
				if !self.valid() {
					self.next()?;
				}
			}
			None => {
				// Target is beyond all partitions
				self.partition_iter = None;
			}
		}
		Ok(())
	}

	/// Moves to the next entry, crossing partition boundaries if needed.
	///
	/// ## Algorithm
	///
	/// ```text
	/// 1. Try to advance within current partition
	/// 2. If partition exhausted:
	///    a. Move to next partition
	///    b. Seek to first entry
	///    c. Repeat if that partition is also empty
	/// ```
	pub(crate) fn next(&mut self) -> Result<bool> {
		// Try advancing within current partition
		if let Some(ref mut iter) = self.partition_iter {
			if iter.advance()? {
				return Ok(true);
			}
		}

		// Current partition exhausted, move to next
		loop {
			self.partition_index += 1;

			// Check if we've exhausted all partitions
			if self.partition_index >= self.index.blocks.len() {
				self.partition_iter = None;
				return Ok(false);
			}

			// Load next partition and position at start
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

	/// Moves to the previous entry, crossing partition boundaries if needed.
	pub(crate) fn prev(&mut self) -> Result<bool> {
		// Try retreating within current partition
		if let Some(ref mut iter) = self.partition_iter {
			if iter.prev_internal()? {
				return Ok(true);
			}
		}

		// Current partition exhausted going backward, move to previous
		loop {
			if self.partition_index == 0 {
				self.partition_iter = None;
				return Ok(false);
			}
			self.partition_index -= 1;

			// Load previous partition and position at end
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
