use std::cmp::Ordering;
use std::sync::Arc;

use crate::{
	error::{Error, Result},
	sstable::InternalKey,
	Comparator, InternalKeyComparator, Iterator as LSMIterator, Key, Options, Value,
};
use bytes::Bytes;
use integer_encoding::{FixedInt, FixedIntWriter, VarInt, VarIntWriter};

pub(crate) type BlockData = Bytes;

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
		let (off, offsize) = usize::decode_var(src)
			.ok_or(Error::CorruptedBlock("corrupted block handle".to_owned()))?;
		let (sz, szsize) = usize::decode_var(&src[offsize..])
			.ok_or(Error::CorruptedBlock("corrupted block handle".to_owned()))?;

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
///
/// ```
///
#[derive(Clone)]
pub(crate) struct Block {
	pub(crate) block: BlockData,
	opts: Arc<Options>,
}

impl Block {
	pub(crate) fn iter(&self, keys_only: bool) -> BlockIterator {
		BlockIterator::new(self.opts.clone(), self.block.clone(), keys_only)
	}

	pub(crate) fn new(data: BlockData, opts: Arc<Options>) -> Block {
		assert!(data.len() > 4);
		Block {
			block: data,
			opts,
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
	internal_cmp: Arc<dyn Comparator>,
}

/*
Block writer logic:

1. Initial state:
   buffer: []
   restart_points: [0]
   restart_counter: 0
   last_key: []

2. Add key-value pair ("apple", "fruit"):
   - No shared prefix with previous key.
   - Serialized entry: [0, 5, 5, "apple", "fruit"].

   buffer: [0, 5, 5, "apple", "fruit"]
   restart_points: [0]
   restart_counter: 1
   last_key: "apple"

   Buffer length: 22 (4 byte for shared prefix, 4 byte for non-shared key length, 4 byte for value length, 5 bytes for key, 5 bytes for value)

3. Add key-value pair ("apricot", "fruit"):
   - Shared prefix with previous key "apple" is "ap" (2 characters).
   - Serialized entry: [2, 4, 5, "ricot", "fruit"].

   buffer: [0, 5, 5, "apple", "fruit", 2, 4, 5, "ricot", "fruit"]
   restart_points: [0]
   restart_counter: 2
   last_key: "apricot"

   Buffer length: 44 (22 bytes + 4 byte for shared prefix, 4 byte for non-shared key length, 4 byte for value length, 5 bytes for key suffix, 5 bytes for value)

4. Add key-value pair ("banana", "fruit"):
   - No shared prefix with previous key.
   - Restart compression (new restart point at this position).
   - Serialized entry: [0, 6, 5, "banana", "fruit"].

   buffer: [0, 5, 5, "apple", "fruit", 2, 4, 5, "ricot", "fruit", 0, 6, 5, "banana", "fruit"]
   restart_points: [0, 26]  // new restart point at offset 25
   restart_counter: 1
   last_key: "banana"

   Buffer length: 67 (44 bytes + 4 byte for shared prefix, 4 byte for non-shared key length, 4 byte for value length, 6 bytes for key, 5 bytes for value)


   Finalize:

   67 + 4 * restart_points.len() + 4 = 67 + 4 * 2 + 4 = 79 bytes

*/
impl BlockWriter {
	// Constructor for BlockWriter
	pub(crate) fn new(opt: Arc<Options>) -> Self {
		BlockWriter {
			internal_cmp: Arc::new(InternalKeyComparator::new(opt.comparator.clone())),
			buffer: Vec::with_capacity(opt.block_size),
			restart_interval: opt.block_restart_interval,
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
					last_internal_key.user_key.as_ref(),
					last_internal_key.seq_num(),
					last_internal_key.kind(),
					last_internal_key.timestamp,
					self.last_key.as_slice(),
					String::from_utf8_lossy(&current_internal_key.user_key),
					current_internal_key.user_key.as_ref(),
					current_internal_key.seq_num(),
					current_internal_key.kind(),
					current_internal_key.timestamp,
					key,
					cmp_result,
					self.num_entries,
					self.buffer.len()
				);
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

		// Write shared prefix length, non-shared key length, and value length as varints
		self.buffer.write_varint(shared_prefix_length as u64)?;
		self.buffer.write_varint(non_shared_key_length as u64)?;
		self.buffer.write_varint(value.len() as u64)?;
		// Write non-shared part of the key and the value
		self.buffer.extend_from_slice(&key[shared_prefix_length..]);
		self.buffer.extend_from_slice(value);

		Ok(())
	}

	// Finalizes the block and returns the block data
	pub(crate) fn finish(mut self) -> BlockData {
		// 1. Append RESTARTS
		for &r in self.restart_points.iter() {
			self.buffer.write_fixedint(r).expect("block write failed");
		}

		// 2. Append N_RESTARTS
		self.buffer.write_fixedint(self.restart_points.len() as u32).expect("block write failed");

		Bytes::from(self.buffer)
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
	internal_cmp: Arc<dyn Comparator>,
	/// When true, only return keys without allocating values
	keys_only: bool,
}

impl BlockIterator {
	// Constructor for BlockIterator
	pub(crate) fn new(options: Arc<Options>, block: BlockData, keys_only: bool) -> Self {
		let num_restarts = u32::decode_fixed(&block[block.len() - 4..]).unwrap() as usize;
		let mut restart_points = vec![0; num_restarts];
		let restart_offset = block.len() - 4 * (num_restarts + 1);

		for (i, restart_point) in restart_points.iter_mut().enumerate().take(num_restarts) {
			let start_point = restart_offset + (i * 4);
			let end_point = start_point + 4;
			*restart_point = u32::decode_fixed(&block[start_point..end_point]).unwrap();
		}

		let internal_comparator = Arc::new(InternalKeyComparator::new(options.comparator.clone()));

		BlockIterator {
			block,
			restart_points,
			current_key: Vec::new(),
			offset: 0,
			current_entry_offset: 0,
			current_restart_index: 0,
			restart_offset,
			current_value_offset_start: 0,
			current_value_offset_end: 0,
			internal_cmp: internal_comparator,
			keys_only,
		}
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

	// Decodes the shared prefix length, non-shared key length, and value size from the block
	fn decode_entry_lengths(&self, offset: usize) -> Option<(usize, usize, usize, usize)> {
		let mut i = 0;
		let (shared_prefix_length, shared_prefix_length_size) =
			usize::decode_var(&self.block[offset..])?;
		i += shared_prefix_length_size;

		let (non_shared_key_length, non_shared_key_length_size) =
			usize::decode_var(&self.block[offset + i..])?;
		i += non_shared_key_length_size;

		let (value_size, value_size_size) = usize::decode_var(&self.block[offset + i..])?;
		i += value_size_size;

		Some((shared_prefix_length, non_shared_key_length, value_size, i))
	}

	// Read the current entry and seek to the next entry
	fn seek_next_entry(&mut self) -> Option<()> {
		if self.offset >= self.restart_offset {
			return None;
		}

		let (shared_prefix, non_shared_key, value_size, i) =
			self.decode_entry_lengths(self.offset)?;

		self.current_key.truncate(shared_prefix);
		self.current_key
			.extend_from_slice(&self.block[self.offset + i..self.offset + i + non_shared_key]);

		self.offset += i + non_shared_key;

		self.current_value_offset_start = self.offset;
		self.current_value_offset_end = self.offset + value_size;
		self.offset += value_size;

		Some(())
	}

	pub(crate) fn reset(&mut self) {
		self.offset = 0;
		self.current_restart_index = 0;
		self.current_key.clear();
		self.current_value_offset_start = 0;
		self.current_value_offset_end = 0;
	}
}

impl Iterator for BlockIterator {
	type Item = (Key, Value);
	fn next(&mut self) -> Option<Self::Item> {
		if !self.advance() {
			return None;
		}
		let value = if self.keys_only {
			Bytes::new()
		} else {
			self.block.slice(self.current_value_offset_start..self.current_value_offset_end)
		};
		Some((Bytes::copy_from_slice(&self.current_key[..]), value))
	}
}

impl DoubleEndedIterator for BlockIterator {
	fn next_back(&mut self) -> Option<Self::Item> {
		if !self.prev() {
			return None;
		}

		let value = if self.keys_only {
			Bytes::new()
		} else {
			self.block.slice(self.current_value_offset_start..self.current_value_offset_end)
		};
		Some((Bytes::copy_from_slice(&self.current_key[..]), value))
	}
}

impl LSMIterator for BlockIterator {
	// Checks if the iterator is valid (has a current entry)
	fn valid(&self) -> bool {
		!self.current_key.is_empty()
			&& self.current_value_offset_start != 0
			&& self.current_value_offset_end != 0
			&& self.offset <= self.restart_offset
	}

	// Move to the first entry
	fn seek_to_first(&mut self) {
		self.seek_to_restart_point(0);
		self.seek_next_entry();
	}

	// Move to the last entry
	fn seek_to_last(&mut self) {
		if self.restart_points.is_empty() {
			self.reset();
		} else {
			self.seek_to_restart_point(self.restart_points.len() - 1);
		}

		let mut last_entry_start = self.offset;
		while self.offset < self.restart_offset {
			last_entry_start = self.offset;
			self.seek_next_entry().expect(&format!(
				"Block corruption detected: failed to decode entry at offset {} (restart_offset: {})",
				self.offset,
				self.restart_offset
			));
		}

		// Set current_entry_offset to the start of the last entry
		self.current_entry_offset = last_entry_start;
	}

	// Move to a specific key or the next larger key
	fn seek(&mut self, target: &[u8]) -> Option<()> {
		self.reset();

		let mut left = 0;
		let mut right = self.restart_points.len() - 1;

		// Binary search to find the closest restart point
		while left < right {
			let mid = (left + right).div_ceil(2);
			self.seek_to_restart_point(mid);
			let (shared_prefix, non_shared_key, _, i) = self.decode_entry_lengths(self.offset)?;
			let current_key = self.block
				[self.offset + i..self.offset + i + shared_prefix + non_shared_key]
				.to_vec();

			match self.internal_cmp.compare(&current_key, target) {
				Ordering::Less => left = mid,
				_ => right = mid - 1,
			}
		}

		assert_eq!(left, right);
		self.seek_to_restart_point(left);

		while self.advance() {
			if self.internal_cmp.compare(&self.current_key, target) != Ordering::Less {
				break;
			}
		}

		Some(())
	}

	// Move to the next entry
	fn advance(&mut self) -> bool {
		if self.offset >= self.restart_offset {
			self.reset();
			return false;
		}
		self.current_entry_offset = self.offset;

		self.seek_next_entry().is_some()
	}

	// Move to the previous entry
	fn prev(&mut self) -> bool {
		let original = self.current_entry_offset;
		if original == 0 {
			self.reset();
			return false;
		}

		// Find the first restart point that is just less than the current offset
		while self.get_restart_point(self.current_restart_index) >= original {
			if self.current_restart_index == 0 {
				self.offset = self.restart_points[self.current_restart_index] as usize;
				self.current_restart_index = self.restart_points.len();
				return false;
			}
			self.current_restart_index -= 1
		}

		self.seek_to_restart_point(self.current_restart_index);
		// Iterate forward to find the entry just before the original position
		let mut prev_offset = self.offset;
		while self.seek_next_entry().is_some() {
			if self.offset >= original {
				// We overshot, so the previous entry starts at prev_offset
				// Position at the previous entry and decode it
				self.offset = prev_offset;
				self.current_entry_offset = prev_offset;
				let _ = self.seek_next_entry(); // Decode the previous entry
				return true;
			}
			prev_offset = self.offset;
		}
		false
	}

	// Get the current key
	fn key(&self) -> Arc<InternalKey> {
		Arc::new(InternalKey::decode(&self.current_key))
	}

	// Get the current value
	fn value(&self) -> Value {
		if self.keys_only {
			Bytes::new()
		} else {
			self.block.slice(self.current_value_offset_start..self.current_value_offset_end)
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::sstable::{InternalKey, InternalKeyKind};
	use test_log::test;

	use super::*;

	fn generate_data() -> Vec<(&'static [u8], &'static [u8])> {
		vec![
			("key1".as_bytes(), "value1".as_bytes()),
			("loooongkey1".as_bytes(), "value2".as_bytes()),
			("medium_key2".as_bytes(), "value3".as_bytes()),
			("pkey1".as_bytes(), "value".as_bytes()),
			("pkey2".as_bytes(), "value".as_bytes()),
			("pkey3".as_bytes(), "value".as_bytes()),
		]
	}

	fn make_opts(block_restart_interval: Option<usize>) -> Arc<Options> {
		let mut opt = Options::default();
		if let Some(interval) = block_restart_interval {
			opt.block_restart_interval = interval;
		}
		Arc::new(opt)
	}

	fn make_internal_key(key: &[u8], kind: InternalKeyKind) -> Vec<u8> {
		InternalKey::new(Bytes::copy_from_slice(key), 0, kind, 0).encode()
	}

	#[test]
	fn test_block_empty() {
		let o = make_opts(None);
		let builder = BlockWriter::new(o.clone());

		let blockc = builder.finish();
		assert_eq!(blockc.len(), 8);
		assert_eq!(blockc.as_ref(), &[0, 0, 0, 0, 1, 0, 0, 0]);

		let mut block_iter = Block::new(blockc, o).iter(false);

		let mut i = 0;
		while block_iter.advance() {
			i += 1;
		}

		assert_eq!(i, 0);
	}

	#[test]
	fn test_block_iter() {
		let data = generate_data();
		let o = make_opts(None);
		let mut builder = BlockWriter::new(o.clone());

		for &(k, v) in data.iter() {
			builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
		}

		let block_contents = builder.finish();

		let mut block_iter = Block::new(block_contents, o.clone()).iter(false);

		let mut i = 0;
		while block_iter.advance() {
			assert_eq!(block_iter.key().user_key.as_ref(), data[i].0);
			assert_eq!(block_iter.value(), Bytes::copy_from_slice(data[i].1));
			i += 1;
		}

		assert_eq!(i, data.len());
	}

	#[test]
	fn test_block_iter_reverse() {
		let data = generate_data();
		let o = make_opts(Some(3));
		let mut builder = BlockWriter::new(o.clone());

		for &(k, v) in data.iter() {
			builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
		}

		let block_contents = builder.finish();
		let mut iter = Block::new(block_contents, o.clone()).iter(false);

		iter.next();
		assert_eq!(iter.key().user_key.as_ref(), "key1".as_bytes());
		assert_eq!(iter.value(), Bytes::from_static(b"value1"));

		iter.next();
		assert!(iter.valid());

		iter.prev();
		assert!(iter.valid());
		assert_eq!(iter.key().user_key.as_ref(), "key1".as_bytes());
		assert_eq!(iter.value(), Bytes::from_static(b"value1"));

		// Go to the last entry
		while iter.advance() {}

		iter.prev();
		assert!(iter.valid());
		assert_eq!(iter.key().user_key.as_ref(), "pkey2".as_bytes());
		assert_eq!(iter.value(), Bytes::from_static(b"value"));
	}

	#[test]
	fn test_block_seek() {
		let data = generate_data();
		let o = make_opts(Some(3));
		let mut builder = BlockWriter::new(o.clone());

		for &(k, v) in data.iter() {
			builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
		}

		let block_contents = builder.finish();

		let mut block_iter = Block::new(block_contents, o.clone()).iter(false);

		let key = InternalKey::new(Bytes::from_static(b"pkey2"), 1, InternalKeyKind::Set, 0);
		block_iter.seek(&key.encode());
		assert!(block_iter.valid());
		assert_eq!(
			Some((block_iter.key().user_key.to_vec(), block_iter.value().to_vec(),)),
			Some(("pkey2".as_bytes().to_vec(), "value".as_bytes().to_vec()))
		);

		let key = InternalKey::new(Bytes::from_static(b"pkey0"), 1, InternalKeyKind::Set, 0);
		block_iter.seek(&key.encode());
		assert!(block_iter.valid());
		assert_eq!(
			Some((block_iter.key().user_key.to_vec(), block_iter.value().to_vec(),)),
			Some(("pkey1".as_bytes().to_vec(), "value".as_bytes().to_vec()))
		);

		let key = InternalKey::new(Bytes::from_static(b"key1"), 1, InternalKeyKind::Set, 0);
		block_iter.seek(&key.encode());
		assert!(block_iter.valid());
		assert_eq!(
			Some((block_iter.key().user_key.to_vec(), block_iter.value().to_vec(),)),
			Some(("key1".as_bytes().to_vec(), "value1".as_bytes().to_vec()))
		);

		let key = InternalKey::new(Bytes::from_static(b"pkey3"), 1, InternalKeyKind::Set, 0);
		block_iter.seek(&key.encode());
		assert!(block_iter.valid());
		assert_eq!(
			Some((block_iter.key().user_key.to_vec(), block_iter.value().to_vec(),)),
			Some(("pkey3".as_bytes().to_vec(), "value".as_bytes().to_vec()))
		);

		let key = InternalKey::new(Bytes::from_static(b"pkey8"), 1, InternalKeyKind::Set, 0);
		block_iter.seek(&key.encode());
		assert!(!block_iter.valid());
	}

	#[test]
	fn test_block_seek_to_last() {
		// Test with different number of restarts
		for block_restart_interval in [2, 6, 10] {
			let data = generate_data();
			let o = make_opts(Some(block_restart_interval));
			let mut builder = BlockWriter::new(o.clone());

			for &(k, v) in data.iter() {
				builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
			}

			let block_contents = builder.finish();

			let mut block_iter = Block::new(block_contents, o.clone()).iter(false);

			block_iter.seek_to_last();
			assert!(block_iter.valid());
			assert_eq!(block_iter.key().user_key.as_ref(), "pkey3".as_bytes());
			assert_eq!(block_iter.value(), Bytes::from_static(b"value"));

			block_iter.seek_to_first();
			assert!(block_iter.valid());
			assert_eq!(block_iter.key().user_key.as_ref(), "key1".as_bytes());
			assert_eq!(block_iter.value(), Bytes::from_static(b"value1"));

			block_iter.next();
			assert!(block_iter.valid());
			block_iter.next();
			assert!(block_iter.valid());
			block_iter.next();
			assert!(block_iter.valid());

			assert_eq!(block_iter.key().user_key.as_ref(), "pkey1".as_bytes());
			assert_eq!(block_iter.value(), Bytes::from_static(b"value"));
		}
	}

	#[test]
	fn test_block_prev() {
		// Test backward iteration using prev()
		let data = generate_data();
		let o = make_opts(Some(2)); // Small restart interval to test restart logic
		let mut builder = BlockWriter::new(o.clone());

		for &(k, v) in data.iter() {
			builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
		}

		let block_contents = builder.finish();
		let mut block_iter = Block::new(block_contents, o.clone()).iter(false);

		// Test prev() from the end
		block_iter.seek_to_last();
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key.as_ref(), "pkey3".as_bytes());

		// Go backward one step
		assert!(block_iter.prev());
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key.as_ref(), "pkey2".as_bytes());

		// Go backward another step
		assert!(block_iter.prev());
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key.as_ref(), "pkey1".as_bytes());

		// Go backward one more step
		assert!(block_iter.prev());
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key.as_ref(), "medium_key2".as_bytes());

		// Go backward to the beginning
		assert!(block_iter.prev());
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key.as_ref(), "loooongkey1".as_bytes());

		// Go backward to the first key
		assert!(block_iter.prev());
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key.as_ref(), "key1".as_bytes());

		// Try to go past the beginning
		assert!(!block_iter.prev());
		assert!(!block_iter.valid());
	}

	#[test]
	fn test_block_double_ended_iteration() {
		// Test using both next() and next_back() (DoubleEndedIterator)
		let data = generate_data();
		let o = make_opts(Some(3));
		let mut builder = BlockWriter::new(o.clone());

		for &(k, v) in data.iter() {
			builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
		}

		let block_contents = builder.finish();

		// Test forward iteration with next()
		let forward_iter = Block::new(block_contents.clone(), o.clone()).iter(false);
		let mut forward_keys = Vec::new();
		for item in forward_iter {
			let internal_key = InternalKey::decode(&item.0);
			forward_keys.push(String::from_utf8(internal_key.user_key.to_vec()).unwrap());
		}
		assert_eq!(
			forward_keys,
			vec!["key1", "loooongkey1", "medium_key2", "pkey1", "pkey2", "pkey3"]
		);

		// Test backward iteration using prev()
		let mut backward_iter = Block::new(block_contents.clone(), o.clone()).iter(false);
		backward_iter.seek_to_last();
		let mut backward_keys = Vec::new();
		while backward_iter.valid() {
			let key = backward_iter.key().user_key.clone();
			backward_keys.push(String::from_utf8(key.to_vec()).unwrap());
			if !backward_iter.prev() {
				break;
			}
		}
		assert_eq!(
			backward_keys,
			vec!["pkey3", "pkey2", "pkey1", "medium_key2", "loooongkey1", "key1"]
		);

		// Verify they are complementary
		assert_eq!(forward_keys, backward_keys.iter().rev().cloned().collect::<Vec<_>>());
	}

	#[test]
	fn test_block_prev_from_middle() {
		// Test prev() starting from the middle of the block
		let data = generate_data();
		let o = make_opts(Some(2));
		let mut builder = BlockWriter::new(o.clone());

		for &(k, v) in data.iter() {
			builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
		}

		let block_contents = builder.finish();
		let mut block_iter = Block::new(block_contents, o.clone()).iter(false);

		// Seek to "pkey1"
		let seek_key = InternalKey::new(Bytes::from_static(b"pkey1"), 1, InternalKeyKind::Set, 0);
		block_iter.seek(&seek_key.encode());
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key.as_ref(), "pkey1".as_bytes());

		// Go backward from here
		assert!(block_iter.prev());
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key.as_ref(), "medium_key2".as_bytes());

		assert!(block_iter.prev());
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key.as_ref(), "loooongkey1".as_bytes());

		assert!(block_iter.prev());
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key.as_ref(), "key1".as_bytes());

		// Can't go further back
		assert!(!block_iter.prev());
		assert!(!block_iter.valid());
	}

	#[test]
	fn test_block_mixed_next_prev() {
		// Test basic prev() functionality after positioning
		let data = generate_data();
		let o = make_opts(Some(3));
		let mut builder = BlockWriter::new(o.clone());

		for &(k, v) in data.iter() {
			builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
		}

		let block_contents = builder.finish();
		let mut block_iter = Block::new(block_contents, o.clone()).iter(false);

		// Position at "pkey1"
		let seek_key = InternalKey::new(Bytes::from_static(b"pkey1"), 1, InternalKeyKind::Set, 0);
		block_iter.seek(&seek_key.encode());
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key.as_ref(), "pkey1".as_bytes());

		// Go backward
		assert!(block_iter.prev());
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key.as_ref(), "medium_key2".as_bytes());

		assert!(block_iter.prev());
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key.as_ref(), "loooongkey1".as_bytes());

		assert!(block_iter.prev());
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key.as_ref(), "key1".as_bytes());

		// Can't go further
		assert!(!block_iter.prev());
		assert!(!block_iter.valid());
	}

	#[test]
	fn test_block_prev_edge_cases() {
		// Test edge cases for prev()
		let data = generate_data();
		let o = make_opts(Some(5)); // Large restart interval
		let mut builder = BlockWriter::new(o.clone());

		for &(k, v) in data.iter() {
			builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
		}

		let block_contents = builder.finish();
		let mut block_iter = Block::new(block_contents, o.clone()).iter(false);

		// Test prev() on empty block (shouldn't happen but let's test robustness)
		let empty_block = BlockWriter::new(o.clone()).finish();
		let mut empty_iter = Block::new(empty_block, o.clone()).iter(false);
		assert!(!empty_iter.prev());
		assert!(!empty_iter.valid());

		// Test prev() when at first entry
		block_iter.seek_to_first();
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key.as_ref(), "key1".as_bytes());

		assert!(!block_iter.prev()); // Can't go before first
		assert!(!block_iter.valid());

		// Test prev() after seek_to_last()
		block_iter.seek_to_last();
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key.as_ref(), "pkey3".as_bytes());

		// Should be able to go backward
		assert!(block_iter.prev());
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key.as_ref(), "pkey2".as_bytes());
	}
}
