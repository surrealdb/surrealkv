//! # Block Implementation
//!
//! This module implements the basic building block of SSTables: the Block.
//! Blocks are the fundamental unit of I/O and caching in the storage engine.
//!
//! ## Block Format Overview
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────────────────┐
//! │                           Block Layout                                  │
//! ├────────────────────────────────────────────────────────────────────────┤
//! │                                                                         │
//! │  ┌─────────────────────────────────────────────────────────────────┐   │
//! │  │                     Key-Value Entries                            │   │
//! │  │  ┌───────────────────────────────────────────────────────────┐  │   │
//! │  │  │ Entry 0: shared=0 | unshared=5 | vlen=6 | "apple" | "fruit" │  │   │
//! │  │  ├───────────────────────────────────────────────────────────┤  │   │
//! │  │  │ Entry 1: shared=2 | unshared=5 | vlen=6 | "ricot" | "fruit" │  │   │
//! │  │  │          (full key = "ap" + "ricot" = "apricot")            │  │   │
//! │  │  ├───────────────────────────────────────────────────────────┤  │   │
//! │  │  │ Entry 2: shared=0 | unshared=6 | vlen=9 | "banana"| "yellow" │  │   │
//! │  │  │          ↑ RESTART POINT (shared=0, full key stored)        │  │   │
//! │  │  └───────────────────────────────────────────────────────────┘  │   │
//! │  └─────────────────────────────────────────────────────────────────┘   │
//! │                                                                         │
//! │  ┌─────────────────────────────────────────────────────────────────┐   │
//! │  │                     Restart Points Array                         │   │
//! │  │  [0x0000]  ← Offset of entry 0 (always 0)                       │   │
//! │  │  [0x001A]  ← Offset of entry 2 (first entry after restart)      │   │
//! │  └─────────────────────────────────────────────────────────────────┘   │
//! │                                                                         │
//! │  ┌─────────────────────────────────────────────────────────────────┐   │
//! │  │  Number of Restarts: 2                                          │   │
//! │  └─────────────────────────────────────────────────────────────────┘   │
//! │                                                                         │
//! └────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Entry Format
//!
//! Each key-value entry is encoded as:
//!
//! ```text
//! +----------------+------------------+------------------+
//! | shared (varint)| unshared (varint)| value_len (varint)|
//! +----------------+------------------+------------------+
//! | key_suffix (unshared bytes)       | value            |
//! +-----------------------------------+------------------+
//! ```
//!
//! - `shared`: Number of bytes shared with previous key
//! - `unshared`: Number of new bytes in this key
//! - `value_len`: Length of the value
//! - `key_suffix`: The non-shared portion of the key
//! - `value`: The value bytes
//!
//! ## Prefix Compression Example
//!
//! ```text
//! Key sequence: "apple", "apricot", "banana"
//! Restart interval: 2
//!
//! Entry 0 (restart point):
//!   shared=0, unshared=5, key_suffix="apple"
//!   Full key: "" + "apple" = "apple"
//!
//! Entry 1:
//!   shared=2, unshared=5, key_suffix="ricot"
//!   Full key: "ap" + "ricot" = "apricot"
//!
//! Entry 2 (new restart point because interval=2):
//!   shared=0, unshared=6, key_suffix="banana"
//!   Full key: "" + "banana" = "banana"
//! ```
//!
//! ## Why Restart Points?
//!
//! Restart points enable efficient binary search within the block:
//!
//! 1. Binary search on restart points (O(log R) where R = restart count)
//! 2. Linear scan from closest restart point (O(I) where I = restart interval)
//!
//! Without restart points, we'd need to scan from the beginning
//! to reconstruct keys due to prefix compression.

use std::cmp::Ordering;
use std::sync::Arc;

use integer_encoding::{FixedInt, FixedIntWriter, VarInt, VarIntWriter};

use crate::error::{Error, Result};
use crate::sstable::error::SSTableError;
use crate::{Comparator, InternalKey, InternalKeyRef, LSMIterator};

/// Raw block data as a byte vector.
pub(crate) type BlockData = Vec<u8>;

// =============================================================================
// BLOCK HANDLE
// =============================================================================

/// Points to a block's location in a file.
///
/// ## Fields
///
/// - `offset`: Byte offset from start of file
/// - `size`: Size of the block data (excluding trailer)
///
/// ## Encoding
///
/// BlockHandles are varint-encoded for space efficiency:
/// ```text
/// [varint: offset] [varint: size]
/// ```
#[derive(Eq, PartialEq, Debug, Clone, Default)]
pub(crate) struct BlockHandle {
	pub(crate) offset: usize,
	pub(crate) size: usize,
}

impl BlockHandle {
	pub(crate) fn new(offset: usize, size: usize) -> BlockHandle {
		BlockHandle {
			offset,
			size,
		}
	}

	pub(crate) fn offset(&self) -> usize {
		self.offset
	}

	pub(crate) fn size(&self) -> usize {
		self.size
	}

	/// Encodes the handle into a byte slice, returns bytes written.
	#[inline]
	pub(crate) fn encode_into(&self, dst: &mut [u8]) -> usize {
		assert!(dst.len() >= self.offset.required_space() + self.size.required_space());

		let off = self.offset.encode_var(dst);
		let size = self.size.encode_var(&mut dst[off..]);
		off + size
	}

	/// Encodes the handle into a new byte vector.
	#[inline]
	pub(crate) fn encode(&self) -> Vec<u8> {
		let cap = self.offset.required_space() + self.size.required_space();
		let mut v = vec![0; cap];
		self.encode_into(&mut v);
		v
	}

	/// Decodes a handle from bytes, returns (handle, bytes_read).
	pub(crate) fn decode(src: &[u8]) -> Result<(Self, usize)> {
		let (off, offsize) = usize::decode_var(src).ok_or(SSTableError::CorruptedBlockHandle)?;
		let (sz, szsize) =
			usize::decode_var(&src[offsize..]).ok_or(SSTableError::CorruptedBlockHandle)?;

		Ok((
			BlockHandle {
				offset: off,
				size: sz,
			},
			offsize + szsize,
		))
	}
}

// =============================================================================
// BLOCK
// =============================================================================

/// An immutable block of sorted key-value pairs.
///
/// ## Structure
///
/// A block consists of:
/// 1. Key-value entries with prefix compression
/// 2. Restart points array (4 bytes each)
/// 3. Number of restart points (4 bytes)
///
/// ## Usage
///
/// ```ignore
/// let block = Block::new(data, comparator);
/// let mut iter = block.iter()?;
/// iter.seek_internal(target)?;
/// if iter.is_valid() {
///     println!("Found: {:?} = {:?}", iter.key(), iter.value());
/// }
/// ```
#[derive(Clone)]
pub(crate) struct Block {
	/// Raw block data
	pub(crate) block: BlockData,
	/// Comparator for key ordering
	comparator: Arc<dyn Comparator>,
}

impl Block {
	/// Creates a new Block from raw data.
	///
	/// ## Panics
	///
	/// Panics if data is too small (< 4 bytes for restart count).
	pub(crate) fn new(data: BlockData, comparator: Arc<dyn Comparator>) -> Block {
		assert!(data.len() > 4);
		Block {
			block: data,
			comparator,
		}
	}

	/// Creates an iterator over this block.
	pub(crate) fn iter(&self) -> Result<BlockIterator> {
		BlockIterator::new(Arc::clone(&self.comparator), self.block.clone())
	}

	pub(crate) fn size(&self) -> usize {
		self.block.len()
	}
}

// =============================================================================
// BLOCK WRITER
// =============================================================================

/// Builds a block from key-value pairs.
///
/// ## Usage Example
///
/// ```ignore
/// let mut writer = BlockWriter::new(4096, 16, comparator);
/// writer.add(b"apple", b"fruit")?;
/// writer.add(b"apricot", b"fruit")?;
/// writer.add(b"banana", b"yellow")?;
/// let data = writer.finish()?;
/// ```
///
/// ## Prefix Compression
///
/// Keys are prefix-compressed against the previous key:
///
/// ```text
/// add("apple"):   shared=0, suffix="apple"   (restart point)
/// add("apricot"): shared=2, suffix="ricot"   (shares "ap")
/// add("banana"):  shared=0, suffix="banana"  (new restart point)
/// ```
///
/// ## Restart Interval
///
/// Every `restart_interval` entries, compression restarts (shared=0).
/// This enables binary search within the block.
pub(crate) struct BlockWriter {
	/// How often to create restart points
	restart_interval: usize,
	/// Destination buffer for encoded entries
	buffer: Vec<u8>,
	/// Offsets of restart points
	restart_points: Vec<u32>,
	/// Entries since last restart
	restart_counter: usize,
	/// Last key added (for prefix calculation)
	pub(crate) last_key: Vec<u8>,
	/// Total entries in block
	num_entries: usize,
	/// Key comparator
	internal_cmp: Arc<dyn Comparator>,
}

impl BlockWriter {
	pub(crate) fn new(
		size: usize,
		restart_interval: usize,
		internal_cmp: Arc<dyn Comparator>,
	) -> Self {
		BlockWriter {
			internal_cmp,
			buffer: Vec::with_capacity(size),
			restart_interval,
			restart_points: vec![0], // First entry is always a restart point
			last_key: Vec::new(),
			restart_counter: 0,
			num_entries: 0,
		}
	}

	/// Adds a key-value pair to the block.
	///
	/// ## Requirements
	///
	/// - Keys MUST be added in strictly ascending order
	/// - Violating order returns an error
	///
	/// ## Process
	///
	/// ```text
	/// 1. Validate key > last_key
	/// 2. If restart_counter >= restart_interval:
	///    - Record new restart point
	///    - Reset restart_counter
	///    - shared_prefix = 0
	/// 3. Else:
	///    - Calculate shared_prefix with last_key
	/// 4. Encode and append entry
	/// 5. Update last_key and counters
	/// ```
	///
	/// ## Example
	///
	/// ```text
	/// Initial: buffer=[], restarts=[0], counter=0, last_key=""
	///
	/// add("apple", "fruit"):
	///   counter=0 < interval=2, so use prefix compression
	///   shared=0 (no previous key)
	///   Encode: [0, 5, 5, "apple", "fruit"]
	///   buffer=[...22 bytes...], restarts=[0], counter=1
	///
	/// add("apricot", "fruit"):
	///   counter=1 < interval=2, use prefix compression
	///   shared=2 ("ap" shared with "apple")
	///   Encode: [2, 5, 5, "ricot", "fruit"]
	///   buffer=[...44 bytes...], restarts=[0], counter=2
	///
	/// add("banana", "yellow"):
	///   counter=2 >= interval=2, NEW RESTART POINT
	///   Record restart at current offset (44)
	///   shared=0 (restart means no prefix sharing)
	///   Encode: [0, 6, 6, "banana", "yellow"]
	///   buffer=[...66 bytes...], restarts=[0, 44], counter=1
	/// ```
	pub(crate) fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
		assert!(self.restart_counter <= self.restart_interval);

		// Validate key ordering
		if !self.buffer.is_empty() {
			let cmp_result = self.internal_cmp.compare(self.last_key.as_slice(), key);
			if cmp_result != Ordering::Less {
				// Detailed logging for debugging
				let last_internal_key = InternalKey::decode(self.last_key.as_slice());
				let current_internal_key = InternalKey::decode(key);

				log::error!(
					"[BLOCK] Key ordering violation detected!\n\
                    Last Key:\n\
                      User Key (UTF-8): {:?}\n\
                      User Key (bytes): {:?}\n\
                      Seq Num: {}\n\
                      Kind: {:?}\n\
                      Timestamp: {}\n\
                      Full InternalKey: {:?}\n\
                    Current Key:\n\
                      User Key (UTF-8): {:?}\n\
                      User Key (bytes): {:?}\n\
                      Seq Num: {}\n\
                      Kind: {:?}\n\
                      Timestamp: {}\n\
                      Full InternalKey: {:?}\n\
                    Comparison result: {:?} (expected: Less)\n\
                    Block state:\n\
                      Entries in block: {}\n\
                      Buffer size: {} bytes",
					String::from_utf8_lossy(&last_internal_key.user_key),
					last_internal_key.user_key.as_slice(),
					last_internal_key.seq_num(),
					last_internal_key.kind(),
					last_internal_key.timestamp,
					self.last_key.as_slice(),
					String::from_utf8_lossy(&current_internal_key.user_key),
					current_internal_key.user_key.as_slice(),
					current_internal_key.seq_num(),
					current_internal_key.kind(),
					current_internal_key.timestamp,
					key,
					cmp_result,
					self.num_entries,
					self.buffer.len()
				);
				return Err(Error::KeyNotInOrder);
			}
			assert!(cmp_result == Ordering::Less);
		}

		// Determine shared prefix length
		let mut shared_prefix_length = 0;
		if self.restart_counter < self.restart_interval {
			// Continue compression from last key
			shared_prefix_length = self.calculate_shared_prefix_length(&self.last_key, key);
		} else {
			// New restart point: no prefix sharing
			self.restart_points.push(self.buffer.len() as u32);
			self.restart_counter = 0;
		}

		// Encode and write the entry
		self.write_key_value_pair_to_buffer(shared_prefix_length, key, value)?;

		// Update state
		self.last_key.clear();
		self.last_key.extend_from_slice(key);
		self.restart_counter += 1;
		self.num_entries += 1;

		Ok(())
	}

	/// Calculates the number of shared prefix bytes between two keys.
	fn calculate_shared_prefix_length(&self, a: &[u8], b: &[u8]) -> usize {
		a.iter().zip(b.iter()).take_while(|&(a, b)| a == b).count()
	}

	/// Encodes and writes a key-value entry to the buffer.
	///
	/// ## Entry Format
	///
	/// ```text
	/// [shared: varint] [unshared: varint] [value_len: varint]
	/// [key_suffix: unshared bytes] [value: value_len bytes]
	/// ```
	fn write_key_value_pair_to_buffer(
		&mut self,
		shared_prefix_length: usize,
		key: &[u8],
		value: &[u8],
	) -> Result<()> {
		let non_shared_key_length = key.len() - shared_prefix_length;

		// Write header (all varints)
		self.buffer.write_varint(shared_prefix_length as u64)?;
		self.buffer.write_varint(non_shared_key_length as u64)?;
		self.buffer.write_varint(value.len() as u64)?;

		// Write key suffix (non-shared part)
		self.buffer.extend_from_slice(&key[shared_prefix_length..]);

		// Write value
		self.buffer.extend_from_slice(value);

		Ok(())
	}

	/// Finalizes the block by appending restart points and count.
	///
	/// ## Final Layout
	///
	/// ```text
	/// [entries...] [restart_0: u32] [restart_1: u32] ... [num_restarts: u32]
	/// ```
	pub(crate) fn finish(mut self) -> Result<BlockData> {
		// Append restart point offsets (fixed 4 bytes each)
		for &r in self.restart_points.iter() {
			self.buffer.write_fixedint(r).map_err(|e| {
				let err = Error::Io(Arc::new(std::io::Error::other(format!(
					"Failed to write restart point {}: {}",
					r, e
				))));
				log::error!("[BLOCK] {}", err);
				err
			})?;
		}

		// Append number of restart points
		self.buffer.write_fixedint(self.restart_points.len() as u32).expect("block write failed");

		Ok(self.buffer)
	}

	/// Estimates the current size including restart array.
	pub(crate) fn size_estimate(&self) -> usize {
		self.buffer.len() + self.restart_points.len() * 4 + 4
	}

	/// Returns the number of entries in the block.
	pub(crate) fn entries(&self) -> usize {
		self.num_entries
	}
}

// =============================================================================
// BLOCK ITERATOR
// =============================================================================

/// Iterates over entries in a block.
///
/// ## Seeking Algorithm
///
/// ```text
/// seek("banana"):
///
/// Step 1: Binary search on restart points
///   restarts = [0, 44]  (offsets of restart entries)
///
///   Decode key at restart[0]=0: "apple"
///   "apple" < "banana"? Yes → search right
///
///   Decode key at restart[1]=44: "date"
///   "date" < "banana"? No → search left
///
///   Binary search converges to restart[0]
///
/// Step 2: Linear scan from restart[0]
///   Position at offset 0, decode "apple"
///   "apple" < "banana"? Yes → continue
///
///   Advance to offset 22, decode "apricot"
///   "apricot" < "banana"? Yes → continue
///
///   Advance to offset 44, decode "date"
///   "date" < "banana"? No → STOP
///
/// Result: Iterator positioned at "date"
/// ```
///
/// ## Key Reconstruction
///
/// Due to prefix compression, keys must be reconstructed:
///
/// ```text
/// current_key starts empty: []
///
/// At entry (shared=0, unshared=5, suffix="apple"):
///   current_key.truncate(0) → []
///   current_key.extend("apple") → [apple]
///
/// At entry (shared=2, unshared=5, suffix="ricot"):
///   current_key.truncate(2) → [ap]
///   current_key.extend("ricot") → [apricot]
/// ```
pub(crate) struct BlockIterator {
	/// Raw block data
	block: BlockData,
	/// Decoded restart point offsets
	restart_points: Vec<u32>,
	/// Current position in block
	offset: usize,
	/// Reconstructed current key
	current_key: Vec<u8>,
	/// Offset where current entry starts (for prev())
	current_entry_offset: usize,
	/// Current restart point index
	current_restart_index: usize,
	/// Offset where entries end (start of restart array)
	restart_offset: usize,
	/// Current value start offset
	current_value_offset_start: usize,
	/// Current value end offset
	current_value_offset_end: usize,
	/// Key comparator
	internal_cmp: Arc<dyn Comparator>,
}

impl BlockIterator {
	/// Creates a new iterator over a block.
	///
	/// ## Initialization
	///
	/// 1. Read number of restarts from last 4 bytes
	/// 2. Calculate restart array offset
	/// 3. Decode all restart point offsets
	pub(crate) fn new(comparator: Arc<dyn Comparator>, block: BlockData) -> Result<Self> {
		if block.len() < 4 {
			let err = Error::from(SSTableError::BlockTooSmall {
				size: block.len(),
				min_size: 4,
			});
			log::error!("[BLOCK] {}", err);
			return Err(err);
		}

		// Decode number of restarts from last 4 bytes
		let num_restarts = u32::decode_fixed(&block[block.len() - 4..]).ok_or_else(|| {
			Error::from(SSTableError::FailedToDecodeRestartCount {
				block_size: block.len(),
			})
		})? as usize;

		// Calculate where entry data ends (before restart array)
		let restart_offset = block.len().checked_sub(4 * (num_restarts + 1)).ok_or_else(|| {
			Error::from(SSTableError::InvalidRestartCount {
				count: num_restarts,
				block_size: block.len(),
			})
		})?;

		// Decode all restart point offsets
		let mut restart_points = vec![0; num_restarts];
		for (i, restart_point) in restart_points.iter_mut().enumerate().take(num_restarts) {
			let start_point = restart_offset + (i * 4);
			let end_point = start_point + 4;
			if end_point > block.len() {
				let err = Error::from(SSTableError::RestartPointExceedsBounds {
					index: i,
					offset: start_point,
					block_size: block.len(),
				});
				log::error!("[BLOCK] {}", err);
				return Err(err);
			}
			*restart_point =
				u32::decode_fixed(&block[start_point..end_point]).ok_or_else(|| {
					Error::from(SSTableError::FailedToDecodeRestartPoint {
						index: i,
						offset: start_point,
					})
				})?;
		}

		Ok(BlockIterator {
			block,
			restart_points,
			current_key: Vec::new(),
			offset: 0,
			current_entry_offset: 0,
			current_restart_index: 0,
			restart_offset,
			current_value_offset_start: 0,
			current_value_offset_end: 0,
			internal_cmp: comparator,
		})
	}

	/// Gets a restart point offset by index.
	fn get_restart_point(&self, index: usize) -> usize {
		self.restart_points[index] as usize
	}

	/// Positions the iterator at a restart point.
	fn seek_to_restart_point(&mut self, restart_index: usize) {
		self.current_restart_index = restart_index;
		let offset = self.restart_points[restart_index] as usize;
		self.offset = offset;
		self.current_entry_offset = offset;
	}

	/// Decodes entry header: (shared, unshared, value_len, header_size).
	fn decode_entry_lengths(&self, offset: usize) -> Result<(usize, usize, usize, usize)> {
		let mut i = 0;

		let (shared_prefix_length, shared_prefix_length_size) =
			usize::decode_var(&self.block[offset..]).ok_or_else(|| {
				Error::from(SSTableError::FailedToDecodeSharedPrefix {
					offset,
				})
			})?;
		i += shared_prefix_length_size;

		let (non_shared_key_length, non_shared_key_length_size) =
			usize::decode_var(&self.block[offset + i..]).ok_or_else(|| {
				Error::from(SSTableError::FailedToDecodeNonSharedKeyLength {
					offset: offset + i,
				})
			})?;
		i += non_shared_key_length_size;

		let (value_size, value_size_size) = usize::decode_var(&self.block[offset + i..])
			.ok_or_else(|| {
				Error::from(SSTableError::FailedToDecodeValueSize {
					offset: offset + i,
				})
			})?;
		i += value_size_size;

		Ok((shared_prefix_length, non_shared_key_length, value_size, i))
	}

	/// Decodes current entry and advances offset to next entry.
	///
	/// ## Key Reconstruction
	///
	/// ```text
	/// current_key = [previous key, possibly truncated]
	/// shared_prefix = 2
	/// key_suffix = "ricot"
	///
	/// current_key.truncate(2) → keeps first 2 bytes
	/// current_key.extend("ricot") → appends suffix
	///
	/// Result: current_key = "apricot"
	/// ```
	fn seek_next_entry(&mut self) -> Result<()> {
		if self.offset >= self.restart_offset {
			return Err(Error::from(SSTableError::OffsetExceedsRestartOffset {
				offset: self.offset,
				restart_offset: self.restart_offset,
			}));
		}

		let (shared_prefix, non_shared_key, value_size, i) =
			self.decode_entry_lengths(self.offset)?;

		// Bounds validation
		let key_end =
			self.offset.checked_add(i).and_then(|sum| sum.checked_add(non_shared_key)).ok_or_else(
				|| {
					Error::from(SSTableError::IntegerOverflowKeyEnd {
						offset: self.offset,
					})
				},
			)?;
		let value_end = key_end.checked_add(value_size).ok_or_else(|| {
			Error::from(SSTableError::IntegerOverflowValueEnd {
				offset: self.offset,
			})
		})?;

		if key_end > self.restart_offset || value_end > self.restart_offset {
			let err = Error::from(SSTableError::DecodedLengthsExceedBounds {
				offset: self.offset,
				i,
				non_shared_key,
				value_size,
				key_end,
				value_end,
				restart_offset: self.restart_offset,
			});
			log::error!("[BLOCK] {}", err);
			return Err(err);
		}

		// Reconstruct key: truncate to shared prefix, then append suffix
		self.current_key.truncate(shared_prefix);
		self.current_key.extend_from_slice(&self.block[self.offset + i..key_end]);

		self.offset = key_end;

		// Track value location
		self.current_value_offset_start = self.offset;
		self.current_value_offset_end = self.offset + value_size;
		self.offset = value_end;

		Ok(())
	}

	/// Resets the iterator to initial state.
	pub(crate) fn reset(&mut self) {
		self.offset = 0;
		self.current_restart_index = 0;
		self.current_key.clear();
		self.current_value_offset_start = 0;
		self.current_value_offset_end = 0;
	}
}

// =============================================================================
// INTERNAL ITERATOR IMPLEMENTATION
// =============================================================================

impl LSMIterator for BlockIterator {
	/// Seeks to the first entry >= target.
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.seek_internal(target)?;
		Ok(self.is_valid())
	}

	/// Seeks to the first entry.
	fn seek_first(&mut self) -> Result<bool> {
		self.seek_to_first()?;
		Ok(self.is_valid())
	}

	/// Seeks to the last entry.
	fn seek_last(&mut self) -> Result<bool> {
		self.seek_to_last()?;
		Ok(self.is_valid())
	}

	/// Advances to the next entry.
	fn next(&mut self) -> Result<bool> {
		if !self.is_valid() {
			return Ok(false);
		}
		self.advance()
	}

	/// Moves to the previous entry.
	fn prev(&mut self) -> Result<bool> {
		if !self.is_valid() {
			return Ok(false);
		}
		self.prev_internal()
	}

	fn valid(&self) -> bool {
		self.is_valid()
	}

	/// Returns the current key as an InternalKeyRef.
	fn key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.valid());
		InternalKeyRef::from_encoded(&self.current_key)
	}

	/// Returns the current value.
	fn value_encoded(&self) -> Result<&[u8]> {
		debug_assert!(self.valid());
		Ok(&self.block[self.current_value_offset_start..self.current_value_offset_end])
	}
}

impl BlockIterator {
	/// Checks if the iterator is positioned on a valid entry.
	pub(crate) fn is_valid(&self) -> bool {
		!self.current_key.is_empty()
			&& self.current_value_offset_start != 0
			&& self.current_value_offset_end != 0
			&& self.offset <= self.restart_offset
	}

	/// Positions at the first entry.
	pub(crate) fn seek_to_first(&mut self) -> Result<()> {
		if self.restart_points.is_empty() {
			let err = Error::from(SSTableError::BlockHasNoRestartPoints);
			log::error!("[BLOCK] {}", err);
			return Err(err);
		}
		self.seek_to_restart_point(0);

		// Handle empty block
		if self.offset >= self.restart_offset {
			self.reset();
			return Ok(());
		}

		self.seek_next_entry()
	}

	/// Positions at the last entry.
	///
	/// ## Algorithm
	///
	/// 1. Go to last restart point
	/// 2. Scan forward to find last entry before restart array
	pub(crate) fn seek_to_last(&mut self) -> Result<()> {
		if self.restart_points.is_empty() {
			self.reset();
			let err = Error::from(SSTableError::BlockHasNoRestartPoints);
			log::error!("[BLOCK] {}", err);
			return Err(err);
		}

		// Start from last restart point
		self.seek_to_restart_point(self.restart_points.len() - 1);

		// Scan to find the last entry
		let mut last_entry_start = self.offset;
		while self.offset < self.restart_offset {
			last_entry_start = self.offset;
			self.seek_next_entry()?;
		}

		self.current_entry_offset = last_entry_start;
		Ok(())
	}

	/// Seeks to first entry >= target using binary search + linear scan.
	///
	/// ## Algorithm
	///
	/// ```text
	/// 1. Binary search restart points to find closest one < target
	/// 2. Linear scan from that restart point until key >= target
	/// ```
	///
	/// ## Example: seek("banana")
	///
	/// ```text
	/// Block contents:
	///   Entry 0 @ offset 0:  "apple"
	///   Entry 1 @ offset 22: "apricot"
	///   Entry 2 @ offset 44: "date"     ← restart point
	///   Entry 3 @ offset 66: "elderberry"
	///
	/// restart_points = [0, 44]
	///
	/// Binary search:
	///   left=0, right=1
	///   mid=1, key at restart[1]="date"
	///   "date" < "banana"? No → right=0
	///   left=0, right=0 → converged at restart[0]
	///
	/// Linear scan from restart[0]:
	///   "apple" < "banana"? Yes → continue
	///   "apricot" < "banana"? Yes → continue
	///   "date" < "banana"? No → STOP
	///
	/// Result: positioned at "date" (first key >= "banana")
	/// ```
	pub(crate) fn seek_internal(&mut self, target: &[u8]) -> Result<Option<()>> {
		self.reset();

		if self.restart_points.is_empty() {
			let err = Error::from(SSTableError::EmptyCorruptBlockSeek {
				block_size: self.block.len(),
				restart_offset: self.restart_offset,
			});
			log::error!("[BLOCK] {}", err);
			return Err(err);
		}

		// Binary search on restart points
		let mut left = 0;
		let mut right = self.restart_points.len() - 1;

		while left < right {
			let mid = (left + right).div_ceil(2);
			self.seek_to_restart_point(mid);

			// Decode key at this restart point
			let (shared_prefix, non_shared_key, _, i) = self.decode_entry_lengths(self.offset)?;
			let key_start = self.offset + i;
			let key_end = key_start + shared_prefix + non_shared_key;

			if key_end > self.block.len() {
				let err = Error::from(SSTableError::KeyExtendsBeyondBounds {
					offset: self.offset,
					key_end,
					block_len: self.block.len(),
				});
				log::error!("[BLOCK] {}", err);
				return Err(err);
			}

			let current_key = &self.block[key_start..key_end];

			match self.internal_cmp.compare(current_key, target) {
				Ordering::Less => left = mid,
				_ => right = mid - 1,
			}
		}

		assert_eq!(left, right);
		self.seek_to_restart_point(left);

		// Linear scan from restart point
		while self.advance()? {
			if self.internal_cmp.compare(&self.current_key, target) != Ordering::Less {
				break;
			}
		}

		Ok(Some(()))
	}

	/// Advances to the next entry.
	pub(crate) fn advance(&mut self) -> Result<bool> {
		if self.offset >= self.restart_offset {
			self.reset();
			return Ok(false);
		}
		self.current_entry_offset = self.offset;

		match self.seek_next_entry() {
			Ok(()) => Ok(true),
			Err(e) => {
				log::error!("[BLOCK] Failed to advance: {}", e);
				Err(e)
			}
		}
	}

	/// Moves to the previous entry.
	///
	/// ## Challenge
	///
	/// Due to prefix compression, we can't decode entries backward.
	/// We must re-scan from a restart point.
	///
	/// ## Algorithm
	///
	/// ```text
	/// 1. Find the restart point at or before current position
	/// 2. Scan forward from restart point
	/// 3. Stop at entry just before original position
	/// ```
	pub(crate) fn prev_internal(&mut self) -> Result<bool> {
		let original = self.current_entry_offset;
		if original == 0 {
			self.reset();
			return Ok(false);
		}

		// Find restart point before current position
		while self.get_restart_point(self.current_restart_index) >= original {
			if self.current_restart_index == 0 {
				self.offset = self.restart_points[self.current_restart_index] as usize;
				self.current_restart_index = self.restart_points.len();
				return Ok(false);
			}
			self.current_restart_index -= 1
		}

		self.seek_to_restart_point(self.current_restart_index);

		// Scan forward to find entry just before original position
		let mut prev_offset = self.offset;
		loop {
			match self.seek_next_entry() {
				Ok(()) => {
					if self.offset >= original {
						// Overshot: prev_offset is the entry we want
						self.offset = prev_offset;
						self.current_entry_offset = prev_offset;
						self.seek_next_entry()?;
						return Ok(true);
					}
					prev_offset = self.offset;
				}
				Err(e) => {
					if let Error::SSTable(SSTableError::OffsetExceedsRestartOffset {
						..
					}) = e
					{
						// Expected EOF
						return Ok(false);
					}
					return Err(e);
				}
			}
		}
	}

	/// Returns the raw encoded key bytes.
	#[inline]
	pub(crate) fn key_bytes(&self) -> &[u8] {
		&self.current_key
	}

	/// Returns the raw value bytes.
	#[inline]
	pub(crate) fn value_bytes(&self) -> &[u8] {
		&self.block[self.current_value_offset_start..self.current_value_offset_end]
	}

	/// Returns just the user key portion.
	#[inline]
	pub(crate) fn user_key(&self) -> &[u8] {
		InternalKey::user_key_from_encoded(&self.current_key)
	}
}
