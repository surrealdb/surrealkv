use std::cmp::Ordering;
use std::sync::Arc;

use integer_encoding::{FixedInt, FixedIntWriter, VarInt, VarIntWriter};

use crate::error::{Error, Result};
use crate::sstable::error::SSTableError;
use crate::sstable::{InternalIterator, InternalKey, InternalKeyRef};
use crate::{Comparator, InternalKeyComparator, Key, Value};

pub(crate) type BlockData = Vec<u8>;

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

	/// Appends varint encoded offset and size into given `dst`
	#[inline]
	pub(crate) fn encode_into(&self, dst: &mut [u8]) -> usize {
		assert!(dst.len() >= self.offset.required_space() + self.size.required_space());

		let off = self.offset.encode_var(dst);
		let size = self.size.encode_var(&mut dst[off..]);
		off + size
	}

	/// Returns bytes for a encoded BlockHandle
	#[inline]
	pub(crate) fn encode(&self) -> Vec<u8> {
		let cap = self.offset.required_space() + self.size.required_space();
		let mut v = vec![0; cap]; // Initialize v with zeros
		self.encode_into(&mut v);
		v
	}

	/// Decodes a block handle from `from` and returns a block handle
	/// together with how many bytes were read from the slice.
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

/// `Block` is consist of one or more key/value entries and a block trailer.
/// Block entry shares key prefix with its preceding key until a `restart`
/// point reached. A block should contains at least one restart point.
/// First restart point are always zero.
///
/// Block Key/value entry:
///
/// ```text
/// 
///     +-------+---------+-----------+---------+--------------------+--------------+----------------+
///     | shared (varint) | not shared (varint) | value len (varint) | key (varlen) | value (varlen) |
///     +-----------------+---------------------+--------------------+--------------+----------------+
/// ```
#[derive(Clone)]
pub(crate) struct Block {
	pub(crate) block: BlockData,
	comparator: Arc<InternalKeyComparator>,
}

impl Block {
	pub(crate) fn iter(&self, keys_only: bool) -> Result<BlockIterator> {
		BlockIterator::new(Arc::clone(&self.comparator), self.block.clone(), keys_only)
	}

	pub(crate) fn new(data: BlockData, comparator: Arc<InternalKeyComparator>) -> Block {
		assert!(data.len() > 4);
		Block {
			block: data,
			comparator,
		}
	}

	pub(crate) fn size(&self) -> usize {
		self.block.len()
	}
}

pub(crate) struct BlockWriter {
	restart_interval: usize,
	// Destination buffer
	buffer: Vec<u8>,
	// Restart points
	restart_points: Vec<u32>,
	// Number of entries since last restart
	restart_counter: usize,
	pub(crate) last_key: Vec<u8>,
	num_entries: usize,
	/// internal key comparator
	internal_cmp: Arc<InternalKeyComparator>,
}

// Block writer logic:
//
// 1. Initial state:
// buffer: []
// restart_points: [0]
// restart_counter: 0
// last_key: []
//
// 2. Add key-value pair ("apple", "fruit"):
// - No shared prefix with previous key.
// - Serialized entry: [0, 5, 5, "apple", "fruit"].
//
// buffer: [0, 5, 5, "apple", "fruit"]
// restart_points: [0]
// restart_counter: 1
// last_key: "apple"
//
// Buffer length: 22 (4 byte for shared prefix, 4 byte for non-shared key
// length, 4 byte for value length, 5 bytes for key, 5 bytes for value)
//
// 3. Add key-value pair ("apricot", "fruit"):
// - Shared prefix with previous key "apple" is "ap" (2 characters).
// - Serialized entry: [2, 4, 5, "ricot", "fruit"].
//
// buffer: [0, 5, 5, "apple", "fruit", 2, 4, 5, "ricot", "fruit"]
// restart_points: [0]
// restart_counter: 2
// last_key: "apricot"
//
// Buffer length: 44 (22 bytes + 4 byte for shared prefix, 4 byte for non-shared
// key length, 4 byte for value length, 5 bytes for key suffix, 5 bytes for
// value)
//
// 4. Add key-value pair ("banana", "fruit"):
// - No shared prefix with previous key.
// - Restart compression (new restart point at this position).
// - Serialized entry: [0, 6, 5, "banana", "fruit"].
//
// buffer: [0, 5, 5, "apple", "fruit", 2, 4, 5, "ricot", "fruit", 0, 6, 5,
// "banana", "fruit"] restart_points: [0, 26]  // new restart point at offset 25
// restart_counter: 1
// last_key: "banana"
//
// Buffer length: 67 (44 bytes + 4 byte for shared prefix, 4 byte for non-shared
// key length, 4 byte for value length, 6 bytes for key, 5 bytes for value)
//
//
// Finalize:
//
// 67 + 4 * restart_points.len() + 4 = 67 + 4 * 2 + 4 = 79 bytes
//
impl BlockWriter {
	// Constructor for BlockWriter
	pub(crate) fn new(
		size: usize,
		restart_interval: usize,
		internal_cmp: Arc<InternalKeyComparator>,
	) -> Self {
		BlockWriter {
			internal_cmp,
			buffer: Vec::with_capacity(size),
			restart_interval,
			restart_points: vec![0],
			last_key: Vec::new(),
			restart_counter: 0,
			num_entries: 0,
		}
	}

	// Adds a key-value pair to the block
	pub(crate) fn add(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
		// println!("key: {:?}", key);
		// Ensure the restart counter is within the interval limit
		assert!(self.restart_counter <= self.restart_interval);

		// Ensure keys are added in sorted order
		if !self.buffer.is_empty() {
			let cmp_result = self.internal_cmp.compare(self.last_key.as_slice(), key);
			if cmp_result != Ordering::Less {
				// Decode both keys for detailed logging
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

		let mut shared_prefix_length = 0;
		if self.restart_counter < self.restart_interval {
			// Calculate shared prefix length with previous key
			shared_prefix_length = self.calculate_shared_prefix_length(&self.last_key, key);
		} else {
			// Create a new restart point
			self.restart_points.push(self.buffer.len() as u32);
			self.restart_counter = 0;
		}

		// Write the key-value pair to the buffer
		self.write_key_value_pair_to_buffer(shared_prefix_length, key, value)?;

		// Update previous key to current key
		self.last_key.clear();
		self.last_key.extend_from_slice(key);

		// Update counters
		self.restart_counter += 1;
		self.num_entries += 1;

		Ok(())
	}

	// Calculates the number of shared bytes between the last key and the new key
	fn calculate_shared_prefix_length(&self, a: &[u8], b: &[u8]) -> usize {
		a.iter().zip(b.iter()).take_while(|&(a, b)| a == b).count()
	}

	// Writes the key-value pair to the buffer
	fn write_key_value_pair_to_buffer(
		&mut self,
		shared_prefix_length: usize,
		key: &[u8],
		value: &[u8],
	) -> Result<()> {
		let non_shared_key_length = key.len() - shared_prefix_length;

		// Write shared prefix length, non-shared key length, and value length as
		// varints
		self.buffer.write_varint(shared_prefix_length as u64)?;
		self.buffer.write_varint(non_shared_key_length as u64)?;
		self.buffer.write_varint(value.len() as u64)?;
		// Write non-shared part of the key and the value
		self.buffer.extend_from_slice(&key[shared_prefix_length..]);
		self.buffer.extend_from_slice(value);

		Ok(())
	}

	// Finalizes the block and returns the block data
	pub(crate) fn finish(mut self) -> Result<BlockData> {
		// 1. Append RESTARTS
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

		// 2. Append N_RESTARTS
		self.buffer.write_fixedint(self.restart_points.len() as u32).expect("block write failed");

		Ok(self.buffer)
	}

	// Estimates the current size of the block
	pub(crate) fn size_estimate(&self) -> usize {
		self.buffer.len() + self.restart_points.len() * 4 + 4
	}

	// Returns the number of entries in the block
	pub(crate) fn entries(&self) -> usize {
		self.num_entries
	}
}

pub(crate) struct BlockIterator {
	block: BlockData,
	restart_points: Vec<u32>,
	offset: usize,
	current_key: Vec<u8>,
	/// offset of the current entry used for prev iteration
	current_entry_offset: usize,
	current_restart_index: usize,
	restart_offset: usize,
	/// offset of value
	current_value_offset_start: usize,
	current_value_offset_end: usize,
	/// internal key comparator
	internal_cmp: Arc<InternalKeyComparator>,
	/// When true, only return keys without allocating values
	keys_only: bool,
}

impl BlockIterator {
	// Constructor for BlockIterator
	pub(crate) fn new(
		comparator: Arc<InternalKeyComparator>,
		block: BlockData,
		keys_only: bool,
	) -> Result<Self> {
		if block.len() < 4 {
			let err = Error::from(SSTableError::BlockTooSmall {
				size: block.len(),
				min_size: 4,
			});
			log::error!("[BLOCK] {}", err);
			return Err(err);
		}

		let num_restarts = u32::decode_fixed(&block[block.len() - 4..]).ok_or_else(|| {
			Error::from(SSTableError::FailedToDecodeRestartCount {
				block_size: block.len(),
			})
		})? as usize;

		let restart_offset = block.len().checked_sub(4 * (num_restarts + 1)).ok_or_else(|| {
			Error::from(SSTableError::InvalidRestartCount {
				count: num_restarts,
				block_size: block.len(),
			})
		})?;

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
			keys_only,
		})
	}

	fn get_restart_point(&self, index: usize) -> usize {
		self.restart_points[index] as usize
	}

	fn seek_to_restart_point(&mut self, restart_index: usize) {
		self.current_restart_index = restart_index;
		let offset = self.restart_points[restart_index] as usize;
		self.offset = offset;
		self.current_entry_offset = offset;
	}

	// Decodes the shared prefix length, non-shared key length, and value size from
	// the block
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

	// Read the current entry and seek to the next entry
	fn seek_next_entry(&mut self) -> Result<()> {
		if self.offset >= self.restart_offset {
			return Err(Error::from(SSTableError::OffsetExceedsRestartOffset {
				offset: self.offset,
				restart_offset: self.restart_offset,
			}));
		}

		let (shared_prefix, non_shared_key, value_size, i) =
			self.decode_entry_lengths(self.offset)?;

		// Bounds validation: ensure we won't read past the restart_offset
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

		self.current_key.truncate(shared_prefix);
		self.current_key.extend_from_slice(&self.block[self.offset + i..key_end]);

		self.offset = key_end;

		self.current_value_offset_start = self.offset;
		self.current_value_offset_end = self.offset + value_size;
		self.offset = value_end;

		Ok(())
	}

	pub(crate) fn reset(&mut self) {
		self.offset = 0;
		self.current_restart_index = 0;
		self.current_key.clear();
		self.current_value_offset_start = 0;
		self.current_value_offset_end = 0;
	}
}

impl InternalIterator for BlockIterator {
	/// Seek to first entry >= target using binary search on restart points.
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		// Use existing seek logic which handles binary search and block navigation
		self.seek_internal(target)?;
		Ok(self.is_valid())
	}

	/// Seek to first entry in block.
	fn seek_first(&mut self) -> Result<bool> {
		self.seek_to_first()?;
		Ok(self.is_valid())
	}

	/// Seek to last entry in block.
	fn seek_last(&mut self) -> Result<bool> {
		self.seek_to_last()?;
		Ok(self.is_valid())
	}

	/// Advance to next entry in block.
	fn next(&mut self) -> Result<bool> {
		if !self.is_valid() {
			return Ok(false);
		}
		self.advance()
	}

	/// Move to previous entry in block.
	/// Complex due to prefix compression - must rescan from restart point.
	fn prev(&mut self) -> Result<bool> {
		if !self.is_valid() {
			return Ok(false);
		}
		self.prev_internal()
	}

	fn valid(&self) -> bool {
		self.is_valid()
	}

	/// Zero-copy key access. current_key holds reconstructed key.
	fn key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.valid());
		InternalKeyRef::from_encoded(&self.current_key)
	}

	/// Zero-copy value access. Returns slice into block data.
	fn value(&self) -> &[u8] {
		debug_assert!(self.valid());
		if self.keys_only {
			&[]
		} else {
			&self.block[self.current_value_offset_start..self.current_value_offset_end]
		}
	}
}

impl BlockIterator {
	// Checks if the iterator is valid (has a current entry)
	pub(crate) fn is_valid(&self) -> bool {
		!self.current_key.is_empty()
			&& self.current_value_offset_start != 0
			&& self.current_value_offset_end != 0
			&& self.offset <= self.restart_offset
	}

	// Move to the first entry
	pub(crate) fn seek_to_first(&mut self) -> Result<()> {
		if self.restart_points.is_empty() {
			let err = Error::from(SSTableError::BlockHasNoRestartPoints);
			log::error!("[BLOCK] {}", err);
			return Err(err);
		}
		self.seek_to_restart_point(0);

		// Check if block has any entries before trying to decode
		if self.offset >= self.restart_offset {
			self.reset();
			return Ok(()); // Empty block
		}

		self.seek_next_entry()
	}

	// Move to the last entry
	pub(crate) fn seek_to_last(&mut self) -> Result<()> {
		if self.restart_points.is_empty() {
			self.reset();
			let err = Error::from(SSTableError::BlockHasNoRestartPoints);
			log::error!("[BLOCK] {}", err);
			return Err(err);
		}
		self.seek_to_restart_point(self.restart_points.len() - 1);

		let mut last_entry_start = self.offset;
		while self.offset < self.restart_offset {
			last_entry_start = self.offset;
			self.seek_next_entry()?;
		}

		// Set current_entry_offset to the start of the last entry
		self.current_entry_offset = last_entry_start;
		Ok(())
	}

	// Move to a specific key or the next larger key
	pub(crate) fn seek_internal(&mut self, target: &[u8]) -> Result<Option<()>> {
		self.reset();

		// Guard against empty blocks (corrupt or malformed)
		if self.restart_points.is_empty() {
			let err = Error::from(SSTableError::EmptyCorruptBlockSeek {
				block_size: self.block.len(),
				restart_offset: self.restart_offset,
			});
			log::error!("[BLOCK] {}", err);
			return Err(err);
		}

		let mut left = 0;
		let mut right = self.restart_points.len() - 1;

		// Binary search to find the closest restart point
		while left < right {
			let mid = (left + right).div_ceil(2);
			self.seek_to_restart_point(mid);
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

		while self.advance()? {
			if self.internal_cmp.compare(&self.current_key, target) != Ordering::Less {
				break;
			}
		}

		Ok(Some(()))
	}

	// Move to the next entry
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

	// Move to the previous entry
	pub(crate) fn prev_internal(&mut self) -> Result<bool> {
		let original = self.current_entry_offset;
		if original == 0 {
			self.reset();
			return Ok(false);
		}

		// Find the first restart point that is just less than the current offset
		while self.get_restart_point(self.current_restart_index) >= original {
			if self.current_restart_index == 0 {
				self.offset = self.restart_points[self.current_restart_index] as usize;
				self.current_restart_index = self.restart_points.len();
				return Ok(false);
			}
			self.current_restart_index -= 1
		}

		self.seek_to_restart_point(self.current_restart_index);
		// Iterate forward to find the entry just before the original position
		let mut prev_offset = self.offset;
		loop {
			match self.seek_next_entry() {
				Ok(()) => {
					if self.offset >= original {
						// We overshot, so the previous entry starts at prev_offset
						// Position at the previous entry and decode it
						self.offset = prev_offset;
						self.current_entry_offset = prev_offset;
						self.seek_next_entry()?; // Decode the previous entry
						return Ok(true);
					}
					prev_offset = self.offset;
				}
				Err(e) => {
					// Check if this is the expected "end of block" error
					// vs corruption errors that should be propagated
					if let Error::SSTable(SSTableError::OffsetExceedsRestartOffset {
						..
					}) = e
					{
						// Expected EOF - no previous entry found
						// Validate invariant: we should have been iterating forward from a restart
						// point and haven't found an entry where offset >= original
						debug_assert!(
							prev_offset < original,
							"OffsetExceedsRestartOffset should only occur when we haven't found previous entry"
						);
						return Ok(false);
					}
					// Corruption or other error - propagate it
					return Err(e);
				}
			}
		}
	}

	// Get the current key (owned version)
	#[inline]
	pub(crate) fn key_owned(&self) -> InternalKey {
		InternalKey::decode(&self.current_key)
	}

	// Get the current value (owned version)
	#[inline]
	pub(crate) fn value_owned(&self) -> Value {
		if self.keys_only {
			Vec::new()
		} else {
			self.block[self.current_value_offset_start..self.current_value_offset_end].to_vec()
		}
	}

	/// Returns the raw encoded key bytes without allocation
	#[inline]
	pub(crate) fn key_bytes(&self) -> &[u8] {
		&self.current_key
	}

	/// Returns the raw value bytes without allocation
	#[inline]
	pub(crate) fn value_bytes(&self) -> &[u8] {
		&self.block[self.current_value_offset_start..self.current_value_offset_end]
	}

	/// Returns user key slice from current key without allocation
	#[inline]
	pub(crate) fn user_key(&self) -> &[u8] {
		InternalKey::user_key_from_encoded(&self.current_key)
	}
}
