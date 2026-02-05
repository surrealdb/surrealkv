use std::fs::{File, OpenOptions};
use std::path::Path;

use memmap2::{MmapMut, MmapOptions};
use parking_lot::Mutex;

use crate::error::Result;

const DISCARD_STATS_FILENAME: &str = "DISCARD";
const ENTRY_SIZE: usize = 12; // 4 bytes for file_id + 8 bytes for discard_bytes
const INITIAL_SIZE: usize = 1 << 20; // 1MB

// Header format: Magic (4 bytes) + Version (4 bytes) + Entry count (4 bytes) = 12 bytes
const HEADER_SIZE: usize = 12;
const MAGIC: [u8; 4] = [0x44, 0x49, 0x53, 0x43]; // "DISC"
const VERSION: u32 = 1;

/// Memory-mapped discard statistics for efficient persistence and lookup
///
/// Each entry is 12 bytes:
/// - 4 bytes: file_id (big-endian u32)
/// - 8 bytes: discard_bytes (big-endian u64)
///
/// Entries are kept sorted by file_id for binary search
#[derive(Debug)]
pub(crate) struct DiscardStats {
	/// Memory-mapped file
	mmap: MmapMut,
	/// File handle
	file: File,
	/// Next empty slot index (not byte offset) - protected by mutex
	next_empty_slot: Mutex<usize>,
}

impl DiscardStats {
	/// Creates or opens a memory-mapped discard stats file
	pub(crate) fn new<P: AsRef<Path>>(dir: P) -> Result<Self> {
		let path = dir.as_ref().join(DISCARD_STATS_FILENAME);
		let file_exists = path.exists();

		// Open or create the file
		let file =
			OpenOptions::new().read(true).write(true).create(true).truncate(false).open(&path)?;

		// Set initial size if new file
		if !file_exists {
			file.set_len(INITIAL_SIZE as u64)?;
		}

		// Create memory map
		let mmap = unsafe { MmapOptions::new().map_mut(&file)? };

		let mut stats = Self {
			mmap,
			file,
			next_empty_slot: Mutex::new(0),
		};

		if !file_exists {
			// New file: initialize header with zero entries
			stats.write_header(0);
		} else if stats.has_valid_magic() {
			// Existing file with valid header: read entry count
			let count = stats.read_entry_count();
			*stats.next_empty_slot.lock() = count;
		} else {
			// Invalid/corrupt file: treat as new (no backward compat)
			stats.write_header(0);
		}

		// Sort entries to ensure binary search works
		stats.sort_entries();

		Ok(stats)
	}

	/// Returns the maximum number of slots available
	fn max_slots(&self) -> usize {
		(self.mmap.len() - HEADER_SIZE) / ENTRY_SIZE
	}

	/// Gets the file ID at the given slot
	fn get_file_id(&self, slot: usize) -> u32 {
		let offset = HEADER_SIZE + slot * ENTRY_SIZE;
		if offset + 4 > self.mmap.len() {
			return 0;
		}
		u32::from_be_bytes([
			self.mmap[offset],
			self.mmap[offset + 1],
			self.mmap[offset + 2],
			self.mmap[offset + 3],
		])
	}

	/// Gets the discard bytes at the given slot
	fn get_discard_bytes(&self, slot: usize) -> u64 {
		let offset = HEADER_SIZE + slot * ENTRY_SIZE + 4;
		if offset + 8 > self.mmap.len() {
			return 0;
		}
		u64::from_be_bytes([
			self.mmap[offset],
			self.mmap[offset + 1],
			self.mmap[offset + 2],
			self.mmap[offset + 3],
			self.mmap[offset + 4],
			self.mmap[offset + 5],
			self.mmap[offset + 6],
			self.mmap[offset + 7],
		])
	}

	/// Sets the file ID at the given slot
	fn set_file_id(&mut self, slot: usize, file_id: u32) {
		let offset = HEADER_SIZE + slot * ENTRY_SIZE;
		self.mmap[offset..offset + 4].copy_from_slice(&file_id.to_be_bytes());
	}

	/// Sets the discard bytes at the given slot
	fn set_discard_bytes(&mut self, slot: usize, discard_bytes: u64) {
		let offset = HEADER_SIZE + slot * ENTRY_SIZE + 4;
		self.mmap[offset..offset + 8].copy_from_slice(&discard_bytes.to_be_bytes());
	}

	/// Reads the entry count from the header
	fn read_entry_count(&self) -> usize {
		u32::from_be_bytes([self.mmap[8], self.mmap[9], self.mmap[10], self.mmap[11]]) as usize
	}

	/// Writes the entry count to the header
	fn write_entry_count(&mut self, count: usize) {
		self.mmap[8..12].copy_from_slice(&(count as u32).to_be_bytes());
	}

	/// Reads the version from the header
	#[allow(dead_code)]
	fn read_version(&self) -> u32 {
		u32::from_be_bytes([self.mmap[4], self.mmap[5], self.mmap[6], self.mmap[7]])
	}

	/// Writes the version to the header
	fn write_version(&mut self, version: u32) {
		self.mmap[4..8].copy_from_slice(&version.to_be_bytes());
	}

	/// Checks if the file has a valid magic header
	fn has_valid_magic(&self) -> bool {
		self.mmap[0..4] == MAGIC
	}

	/// Writes the magic bytes to the header
	fn write_magic(&mut self) {
		self.mmap[0..4].copy_from_slice(&MAGIC);
	}

	/// Writes the complete header (magic, version, count)
	fn write_header(&mut self, count: usize) {
		self.write_magic();
		self.write_version(VERSION);
		self.write_entry_count(count);
	}

	/// Sorts entries by file_id
	fn sort_entries(&mut self) {
		let next_empty_slot = *self.next_empty_slot.lock();

		// Create a vector of (file_id, discard_bytes) tuples
		let mut entries: Vec<(u32, u64)> = Vec::with_capacity(next_empty_slot);
		for slot in 0..next_empty_slot {
			let file_id = self.get_file_id(slot);
			let discard_bytes = self.get_discard_bytes(slot);
			entries.push((file_id, discard_bytes));
		}

		// Sort by file_id
		entries.sort_by_key(|(file_id, _)| *file_id);

		// Write back sorted entries
		for (slot, (file_id, discard_bytes)) in entries.into_iter().enumerate() {
			self.set_file_id(slot, file_id);
			self.set_discard_bytes(slot, discard_bytes);
		}
	}

	/// Updates the discard statistics for a file
	///
	/// # Arguments
	/// * `file_id` - The VLog file ID (any u32 value is valid, including 0)
	/// * `discard_bytes` - 0: query; negative: reset to 0; positive: add
	///
	/// # Returns
	/// The current discard bytes after the operation
	pub(crate) fn update(&mut self, file_id: u32, discard_bytes: i64) -> Result<u64> {
		let next_empty_slot = *self.next_empty_slot.lock();

		// Try to find existing entry
		if let Some(slot) = self.binary_search(file_id, next_empty_slot) {
			let current_discard = self.get_discard_bytes(slot);

			if discard_bytes == 0 {
				return Ok(current_discard);
			}

			if discard_bytes < 0 {
				self.set_discard_bytes(slot, 0);
				return Ok(0);
			}

			let new_discard = current_discard.saturating_add(discard_bytes as u64);
			self.set_discard_bytes(slot, new_discard);
			return Ok(new_discard);
		}

		// Not found - if not adding, return 0
		if discard_bytes <= 0 {
			return Ok(0);
		}

		// Need to add new entry
		if next_empty_slot >= self.max_slots() {
			self.expand()?;
			return self.update(file_id, discard_bytes);
		}

		// Find insertion point to maintain sorted order
		let insert_pos = self.binary_search_insert_point(file_id, next_empty_slot);

		// Shift entries right to make room
		for i in (insert_pos..next_empty_slot).rev() {
			let fid = self.get_file_id(i);
			let db = self.get_discard_bytes(i);
			self.set_file_id(i + 1, fid);
			self.set_discard_bytes(i + 1, db);
		}

		// Insert new entry at correct position
		self.set_file_id(insert_pos, file_id);
		self.set_discard_bytes(insert_pos, discard_bytes as u64);

		// Update count in memory and header
		let new_count = next_empty_slot + 1;
		*self.next_empty_slot.lock() = new_count;
		self.write_entry_count(new_count);

		Ok(discard_bytes as u64)
	}

	/// Binary search for insertion point - returns the index where file_id should be inserted
	/// to maintain sorted order
	fn binary_search_insert_point(&self, file_id: u32, count: usize) -> usize {
		let mut left = 0;
		let mut right = count;

		while left < right {
			let mid = left + (right - left) / 2;
			if self.get_file_id(mid) < file_id {
				left = mid + 1;
			} else {
				right = mid;
			}
		}
		left
	}

	/// Binary search for a file_id, returns the slot index if found
	fn binary_search(&self, file_id: u32, next_empty_slot: usize) -> Option<usize> {
		if next_empty_slot == 0 {
			return None;
		}

		let pos = self.binary_search_insert_point(file_id, next_empty_slot);
		if pos < next_empty_slot && self.get_file_id(pos) == file_id {
			Some(pos)
		} else {
			None
		}
	}

	/// Expands the memory-mapped file by doubling its size
	fn expand(&mut self) -> Result<()> {
		let current_size = self.mmap.len();
		let new_size = current_size * 2;

		// Flush current mmap
		self.mmap.flush()?;

		// Create a new mmap with double size
		// First, we need to drop the current mmap
		let mmap_copy: Vec<u8> = self.mmap.to_vec();
		drop(std::mem::replace(&mut self.mmap, MmapMut::map_anon(1)?));

		// Resize the file
		self.file.set_len(new_size as u64)?;

		// Recreate the mmap
		self.mmap = unsafe { MmapOptions::new().map_mut(&self.file)? };

		// Copy old data
		self.mmap[..current_size].copy_from_slice(&mmap_copy);

		Ok(())
	}

	/// Returns all files with discard bytes, sorted by discard bytes
	/// (descending) Returns a vector of (file_id, discard_bytes) tuples
	pub(crate) fn get_gc_candidates(&self) -> Vec<(u32, u64)> {
		let next_empty_slot = *self.next_empty_slot.lock();

		let mut candidates = Vec::new();

		for slot in 0..next_empty_slot {
			let file_id = self.get_file_id(slot);
			let discard_bytes = self.get_discard_bytes(slot);

			if discard_bytes > 0 {
				candidates.push((file_id, discard_bytes));
			}
		}

		// Sort by discard_bytes in descending order
		candidates.sort_by(|a, b| b.1.cmp(&a.1));

		candidates
	}

	/// Gets discard bytes for a specific file
	pub(crate) fn get_file_stats(&self, file_id: u32) -> u64 {
		let next_empty_slot = *self.next_empty_slot.lock();

		if let Some(slot) = self.binary_search(file_id, next_empty_slot) {
			self.get_discard_bytes(slot)
		} else {
			0
		}
	}

	/// Removes statistics for a file
	pub(crate) fn remove_file(&mut self, file_id: u32) {
		let (slot_to_remove, next_empty_slot) = {
			let next_empty_slot_guard = self.next_empty_slot.lock();
			let next_empty_slot = *next_empty_slot_guard;
			let slot_to_remove = self.binary_search(file_id, next_empty_slot);
			(slot_to_remove, next_empty_slot)
		};

		if let Some(slot) = slot_to_remove {
			// Shift all entries after this slot to the left
			for i in slot..next_empty_slot - 1 {
				let next_file_id = self.get_file_id(i + 1);
				let next_discard = self.get_discard_bytes(i + 1);
				self.set_file_id(i, next_file_id);
				self.set_discard_bytes(i, next_discard);
			}

			// Update count in memory and header
			let new_count = next_empty_slot - 1;
			*self.next_empty_slot.lock() = new_count;
			self.write_entry_count(new_count);
		}
	}

	/// Syncs the memory-mapped file to disk
	pub(crate) fn sync(&self) -> Result<()> {
		self.mmap.flush()?;
		Ok(())
	}

	/// Returns the number of entries (for testing)
	#[cfg(test)]
	fn entry_count(&self) -> usize {
		*self.next_empty_slot.lock()
	}
}

impl Drop for DiscardStats {
	fn drop(&mut self) {
		// Best effort flush on drop
		let _ = self.mmap.flush();
	}
}

#[cfg(test)]
mod tests {
	use tempfile::TempDir;
	use test_log::test;

	use super::*;

	#[test]
	fn test_discard_stats_basic() {
		let temp_dir = TempDir::new().unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		// Test basic operations
		assert_eq!(stats.update(1, 100).unwrap(), 100);
		assert_eq!(stats.update(1, 50).unwrap(), 150);
		assert_eq!(stats.update(1, 0).unwrap(), 150); // Query current value
		assert_eq!(stats.update(1, -1).unwrap(), 0); // Reset to 0

		// Test multiple files
		assert_eq!(stats.update(2, 200).unwrap(), 200);
		assert_eq!(stats.update(3, 300).unwrap(), 300);

		// Test max discard
		let candidates = stats.get_gc_candidates();
		let (file_id, discard) = candidates[0];
		assert_eq!(file_id, 3);
		assert_eq!(discard, 300);
	}

	#[test]
	fn test_discard_stats_persistence() {
		let temp_dir = TempDir::new().unwrap();

		// Create and populate stats
		{
			let mut stats = DiscardStats::new(temp_dir.path()).unwrap();
			stats.update(1, 100).unwrap();
			stats.update(2, 200).unwrap();
			stats.update(3, 300).unwrap();
			stats.sync().unwrap();
		}

		// Reopen and verify data persisted
		{
			let stats = DiscardStats::new(temp_dir.path()).unwrap();
			assert_eq!(stats.get_file_stats(1), 100);
			assert_eq!(stats.get_file_stats(2), 200);
			assert_eq!(stats.get_file_stats(3), 300);

			let candidates = stats.get_gc_candidates();
			let (file_id, discard) = candidates[0];
			assert_eq!(file_id, 3);
			assert_eq!(discard, 300);
		}
	}

	#[test]
	fn test_discard_stats_remove() {
		let temp_dir = TempDir::new().unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		// Add some entries
		stats.update(1, 100).unwrap();
		stats.update(2, 200).unwrap();
		stats.update(3, 300).unwrap();

		// Remove middle entry
		stats.remove_file(2);

		// Verify removal
		assert_eq!(stats.get_file_stats(1), 100);
		assert_eq!(stats.get_file_stats(2), 0); // Removed
		assert_eq!(stats.get_file_stats(3), 300);

		// Verify max discard updated
		let candidates = stats.get_gc_candidates();
		let (file_id, discard) = candidates[0];

		assert_eq!(file_id, 3);
		assert_eq!(discard, 300);
	}

	#[test]
	fn test_discard_stats_expansion() {
		let temp_dir = TempDir::new().unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		// Add enough entries to trigger expansion
		let entries_to_add = 1000;

		// Start from 0 since file_id=0 is now valid
		for i in 0..entries_to_add {
			stats.update(i as u32, 100).unwrap();
		}

		// Verify all entries exist
		for i in 0..entries_to_add {
			assert_eq!(stats.get_file_stats(i as u32), 100);
		}
	}

	// ========================================================================
	// BUG FIX VERIFICATION TESTS
	// ========================================================================

	/// Verify file_id=0 is now valid (was previously incorrectly rejected)
	#[test]
	fn test_file_id_zero_valid() {
		let temp_dir = TempDir::new().unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		// file_id=0 should now work
		assert_eq!(stats.update(0, 500).unwrap(), 500);
		assert_eq!(stats.update(1, 100).unwrap(), 100);
		assert_eq!(stats.update(2, 200).unwrap(), 200);

		// Verify all entries including 0
		assert_eq!(stats.entry_count(), 3);
		assert_eq!(stats.get_file_stats(0), 500);
		assert_eq!(stats.get_file_stats(1), 100);
		assert_eq!(stats.get_file_stats(2), 200);

		// Verify sorted order: 0, 1, 2
		assert_eq!(stats.get_file_id(0), 0);
		assert_eq!(stats.get_file_id(1), 1);
		assert_eq!(stats.get_file_id(2), 2);
	}

	/// Verify data persists correctly after reload
	#[test]
	fn test_persistence_no_data_loss() {
		let temp_dir = TempDir::new().unwrap();

		{
			let mut stats = DiscardStats::new(temp_dir.path()).unwrap();
			stats.update(0, 50).unwrap(); // Test file_id=0 persistence
			stats.update(5, 500).unwrap();
			stats.update(10, 1000).unwrap();
			stats.update(15, 1500).unwrap();
			stats.sync().unwrap();
		}

		{
			let stats = DiscardStats::new(temp_dir.path()).unwrap();
			assert_eq!(stats.entry_count(), 4);
			assert_eq!(stats.get_file_stats(0), 50);
			assert_eq!(stats.get_file_stats(5), 500);
			assert_eq!(stats.get_file_stats(10), 1000);
			assert_eq!(stats.get_file_stats(15), 1500);
		}
	}

	/// Verify header (magic, version, count) persists correctly
	#[test]
	fn test_header_persistence() {
		let temp_dir = TempDir::new().unwrap();

		{
			let mut stats = DiscardStats::new(temp_dir.path()).unwrap();
			stats.update(0, 100).unwrap();
			stats.update(1, 200).unwrap();
			stats.update(2, 300).unwrap();
			stats.sync().unwrap();

			// Verify header values
			assert!(stats.has_valid_magic());
			assert_eq!(stats.read_version(), VERSION);
			assert_eq!(stats.read_entry_count(), 3);
		}

		{
			let stats = DiscardStats::new(temp_dir.path()).unwrap();
			// Verify header persisted
			assert!(stats.has_valid_magic());
			assert_eq!(stats.read_version(), VERSION);
			assert_eq!(stats.read_entry_count(), 3);
			// Verify data
			assert_eq!(stats.get_file_stats(0), 100);
			assert_eq!(stats.get_file_stats(1), 200);
			assert_eq!(stats.get_file_stats(2), 300);
		}
	}

	/// Verify Fix #2: Insertions maintain sorted order without full sort
	#[test]
	fn test_binary_insertion_maintains_order() {
		let temp_dir = TempDir::new().unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		// Insert in reverse order (worst case for naive append)
		let ids = [50, 40, 30, 20, 10, 5, 45, 35, 25, 15];
		for &id in &ids {
			stats.update(id, id as i64 * 10).unwrap();
		}

		// Verify sorted
		let mut prev = 0;
		for i in 0..stats.entry_count() {
			let fid = stats.get_file_id(i);
			assert!(fid > prev, "Not sorted: {} should be > {}", fid, prev);
			prev = fid;
		}
	}

	// ========================================================================
	// EDGE CASES
	// ========================================================================

	#[test]
	fn test_saturating_add() {
		let temp_dir = TempDir::new().unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		// Start with a large value
		stats.update(1, i64::MAX).unwrap();
		let first_val = stats.get_file_stats(1);
		assert_eq!(first_val, i64::MAX as u64);

		// Adding another large value should saturate at u64::MAX
		stats.update(1, i64::MAX).unwrap();
		let saturated_val = stats.get_file_stats(1);

		// i64::MAX + i64::MAX = 2 * i64::MAX which is u64::MAX - 1
		// The saturating_add works on u64, so this tests that overflow is handled
		assert_eq!(saturated_val, (i64::MAX as u64).saturating_add(i64::MAX as u64));

		// Now test actual saturation at u64::MAX
		stats.update(1, i64::MAX).unwrap();
		assert_eq!(stats.get_file_stats(1), u64::MAX);
	}

	#[test]
	fn test_remove_nonexistent() {
		let temp_dir = TempDir::new().unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		stats.update(1, 100).unwrap();
		stats.remove_file(999);

		assert_eq!(stats.entry_count(), 1);
	}

	#[test]
	fn test_query_nonexistent() {
		let temp_dir = TempDir::new().unwrap();
		let stats = DiscardStats::new(temp_dir.path()).unwrap();

		assert_eq!(stats.get_file_stats(999), 0);
	}

	#[test]
	fn test_negative_on_nonexistent() {
		let temp_dir = TempDir::new().unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		assert_eq!(stats.update(1, -100).unwrap(), 0);
		assert_eq!(stats.entry_count(), 0);
	}

	#[test]
	fn test_large_file_ids() {
		let temp_dir = TempDir::new().unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		stats.update(u32::MAX, 100).unwrap();
		stats.update(0, 50).unwrap(); // Include file_id=0
		stats.update(1, 200).unwrap();
		stats.update(u32::MAX / 2, 300).unwrap();

		// Verify sorted order: 0, 1, MAX/2, MAX
		assert_eq!(stats.get_file_id(0), 0);
		assert_eq!(stats.get_file_id(1), 1);
		assert_eq!(stats.get_file_id(2), u32::MAX / 2);
		assert_eq!(stats.get_file_id(3), u32::MAX);
	}
}
