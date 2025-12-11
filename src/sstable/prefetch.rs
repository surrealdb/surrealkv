//! Prefetch buffer and adaptive readahead for SSTable block reads.
//!
//! This module provides two main components:
//! 1. `PrefetchBuffer` - An in-memory buffer for bulk prefetched data
//! 2. `BlockPrefetcher` - Tracks sequential access and manages adaptive readahead

use crate::{error::Result, sstable::block::BlockHandle, vfs::File};

use super::table::{BLOCK_CKSUM_LEN, BLOCK_COMPRESS_LEN};

/// Block trailer size (checksum + compression type byte)
pub(crate) const BLOCK_TRAILER_SIZE: usize = BLOCK_CKSUM_LEN + BLOCK_COMPRESS_LEN;

/// In-memory buffer for bulk prefetched data.
///
/// Used to read a contiguous range of data from a file into memory,
/// allowing multiple blocks to be read from the buffer without
/// additional I/O operations.
#[derive(Debug)]
pub(crate) struct PrefetchBuffer {
	/// The prefetched data
	data: Vec<u8>,
	/// Starting offset of this buffer in the file
	offset: u64,
}

impl PrefetchBuffer {
	/// Read a range from file into memory.
	///
	/// # Arguments
	/// * `file` - The file to read from
	/// * `offset` - Starting offset in the file
	/// * `len` - Number of bytes to read
	pub fn prefetch(file: &dyn File, offset: u64, len: usize) -> Result<Self> {
		let mut data = vec![0u8; len];
		file.read_at(offset, &mut data)?;
		Ok(Self {
			data,
			offset,
		})
	}

	/// Check if the given block handle is fully contained within this buffer.
	pub fn contains(&self, handle: &BlockHandle) -> bool {
		let block_start = handle.offset as u64;
		let block_end = block_start + handle.size as u64 + BLOCK_TRAILER_SIZE as u64;
		block_start >= self.offset && block_end <= self.offset + self.data.len() as u64
	}

	/// Read a block from the buffer.
	///
	/// Returns the block data including trailer, or None if the block
	/// is not fully contained in the buffer.
	pub fn read_block(&self, handle: &BlockHandle) -> Option<Vec<u8>> {
		if !self.contains(handle) {
			return None;
		}

		let start = (handle.offset as u64 - self.offset) as usize;
		let end = start + handle.size + BLOCK_TRAILER_SIZE;
		Some(self.data[start..end].to_vec())
	}
}

/// Tracks sequential access and manages adaptive readahead.
///
/// This is modeled after RocksDB's `BlockPrefetcher` in `block_prefetcher.cc`.
/// It detects sequential read patterns and automatically prefetches
/// additional data when sequential access is detected.
pub(crate) struct BlockPrefetcher {
	/// Number of sequential reads observed
	num_file_reads: u64,
	/// Previous read offset
	prev_offset: u64,
	/// Previous read length
	prev_len: usize,
	/// Current readahead size (grows exponentially)
	readahead_size: usize,
	/// Limit of current prefetch (offset + len + readahead)
	readahead_limit: u64,
	/// Current prefetch buffer
	buffer: Option<PrefetchBuffer>,

	// Configuration
	/// Initial readahead size
	initial_readahead_size: usize,
	/// Maximum readahead size
	max_readahead_size: usize,
	/// Number of sequential reads before enabling readahead
	num_reads_for_auto_readahead: u64,
}

impl BlockPrefetcher {
	/// Create a new BlockPrefetcher with custom auto-readahead threshold.
	pub fn with_auto_readahead_threshold(
		initial_size: usize,
		max_size: usize,
		num_reads_threshold: u64,
	) -> Self {
		Self {
			num_file_reads: 0,
			prev_offset: 0,
			prev_len: 0,
			readahead_size: initial_size,
			readahead_limit: 0,
			buffer: None,
			initial_readahead_size: initial_size,
			max_readahead_size: max_size,
			num_reads_for_auto_readahead: num_reads_threshold,
		}
	}

	/// Check if offset is sequential with previous read.
	fn is_sequential(&self, offset: u64) -> bool {
		self.prev_len == 0 || (self.prev_offset + self.prev_len as u64 == offset)
	}

	/// Update tracking after a read.
	fn update_pattern(&mut self, offset: u64, len: usize) {
		self.prev_offset = offset;
		self.prev_len = len;
	}

	/// Reset on non-sequential access.
	fn reset(&mut self) {
		self.num_file_reads = 1;
		self.readahead_size = self.initial_readahead_size;
		self.readahead_limit = 0;
		self.buffer = None;
	}

	/// Check if the block is already in the prefetch buffer.
	pub fn is_in_buffer(&self, handle: &BlockHandle) -> bool {
		if let Some(ref buf) = self.buffer {
			buf.contains(handle)
		} else {
			false
		}
	}

	/// Prefetch if needed, based on access pattern.
	///
	/// This method:
	/// 1. Checks if the block is already in the prefetch buffer
	/// 2. Detects non-sequential access and resets if needed
	/// 3. Counts sequential reads and triggers prefetch after threshold
	/// 4. Exponentially grows readahead size up to max
	///
	/// Returns `true` if prefetching was performed.
	pub fn prefetch_if_needed(&mut self, file: &dyn File, handle: &BlockHandle) -> Result<bool> {
		let offset = handle.offset as u64;
		let len = handle.size + BLOCK_TRAILER_SIZE;

		// Already in prefetch buffer?
		if offset + len as u64 <= self.readahead_limit {
			if let Some(ref buf) = self.buffer {
				if buf.contains(handle) {
					self.update_pattern(offset, len);
					return Ok(false); // No new prefetch needed
				}
			}
		}

		// Non-sequential access?
		if !self.is_sequential(offset) {
			self.update_pattern(offset, len);
			self.reset();
			return Ok(false);
		}

		self.update_pattern(offset, len);
		self.num_file_reads += 1;

		// Not enough sequential reads yet?
		if self.num_file_reads <= self.num_reads_for_auto_readahead {
			return Ok(false);
		}

		// Trigger prefetch
		let prefetch_len = len + self.readahead_size;
		self.buffer = Some(PrefetchBuffer::prefetch(file, offset, prefetch_len)?);
		self.readahead_limit = offset + prefetch_len as u64;

		// Exponential growth
		self.readahead_size = std::cmp::min(self.max_readahead_size, self.readahead_size * 2);

		Ok(true)
	}

	/// Read a block, using the prefetch buffer if available.
	///
	/// If the block is in the prefetch buffer, returns it directly.
	/// Otherwise, returns None and the caller should read from file.
	pub fn read_from_buffer(&self, handle: &BlockHandle) -> Option<Vec<u8>> {
		self.buffer.as_ref().and_then(|buf| buf.read_block(handle))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn make_test_file(size: usize) -> Vec<u8> {
		(0..size).map(|i| (i % 256) as u8).collect()
	}

	#[test]
	fn test_prefetch_buffer_basic() {
		let data = make_test_file(1000);
		let buf = PrefetchBuffer::prefetch(&data, 100, 500).unwrap();

		assert_eq!(buf.offset, 100);
		assert_eq!(buf.data.len(), 500);
	}

	#[test]
	fn test_prefetch_buffer_contains() {
		let data = make_test_file(1000);
		let buf = PrefetchBuffer::prefetch(&data, 100, 500).unwrap();

		// Block fully contained
		let handle = BlockHandle::new(150, 50);
		assert!(buf.contains(&handle));

		// Block starts before buffer
		let handle = BlockHandle::new(50, 100);
		assert!(!buf.contains(&handle));

		// Block extends beyond buffer
		let handle = BlockHandle::new(550, 100);
		assert!(!buf.contains(&handle));
	}

	#[test]
	fn test_prefetch_buffer_read_block() {
		let data = make_test_file(1000);
		let buf = PrefetchBuffer::prefetch(&data, 100, 500).unwrap();

		let handle = BlockHandle::new(150, 50);
		let block_data = buf.read_block(&handle).unwrap();

		// Block size + trailer
		assert_eq!(block_data.len(), 50 + BLOCK_TRAILER_SIZE);

		// Verify data content
		for (i, &byte) in block_data.iter().enumerate() {
			let expected = ((150 + i) % 256) as u8;
			assert_eq!(byte, expected);
		}
	}

	#[test]
	fn test_block_prefetcher_sequential_detection() {
		let data = make_test_file(10000);
		let mut prefetcher = BlockPrefetcher::with_auto_readahead_threshold(1000, 8000, 2);

		// First read - no prefetch yet
		let handle1 = BlockHandle::new(0, 100);
		let result = prefetcher.prefetch_if_needed(&data, &handle1).unwrap();
		assert!(!result);
		assert_eq!(prefetcher.num_file_reads, 1);

		// Second sequential read - still no prefetch (need 2 reads first)
		let handle2 = BlockHandle::new(100 + BLOCK_TRAILER_SIZE, 100);
		let result = prefetcher.prefetch_if_needed(&data, &handle2).unwrap();
		assert!(!result);
		assert_eq!(prefetcher.num_file_reads, 2);

		// Third sequential read - triggers prefetch
		let handle3 = BlockHandle::new(200 + 2 * BLOCK_TRAILER_SIZE, 100);
		let result = prefetcher.prefetch_if_needed(&data, &handle3).unwrap();
		assert!(result);
		assert!(prefetcher.buffer.is_some());
	}

	#[test]
	fn test_block_prefetcher_non_sequential_reset() {
		let data = make_test_file(10000);
		let mut prefetcher = BlockPrefetcher::with_auto_readahead_threshold(1000, 8000, 2);

		// Sequential reads
		let handle1 = BlockHandle::new(0, 100);
		prefetcher.prefetch_if_needed(&data, &handle1).unwrap();
		let handle2 = BlockHandle::new(100 + BLOCK_TRAILER_SIZE, 100);
		prefetcher.prefetch_if_needed(&data, &handle2).unwrap();

		assert_eq!(prefetcher.num_file_reads, 2);

		// Non-sequential read - should reset
		let handle3 = BlockHandle::new(5000, 100);
		prefetcher.prefetch_if_needed(&data, &handle3).unwrap();

		assert_eq!(prefetcher.num_file_reads, 1);
		assert!(prefetcher.buffer.is_none());
	}

	#[test]
	fn test_block_prefetcher_exponential_growth() {
		let data = make_test_file(100000);
		let mut prefetcher = BlockPrefetcher::with_auto_readahead_threshold(1000, 16000, 2);

		// Trigger prefetch with sequential reads
		let mut offset = 0;
		for _ in 0..5 {
			let handle = BlockHandle::new(offset, 100);
			prefetcher.prefetch_if_needed(&data, &handle).unwrap();
			offset += 100 + BLOCK_TRAILER_SIZE;
		}

		// Readahead should have grown
		assert!(prefetcher.readahead_size > 1000);
		assert!(prefetcher.readahead_size <= 16000);
	}
}
