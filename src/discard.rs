use std::fs::{File, OpenOptions};
use std::path::Path;

use memmap2::{MmapMut, MmapOptions};

use crate::error::Result;
use crate::Error;

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
	next_empty_slot: usize,
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
			next_empty_slot: 0,
		};

		if !file_exists {
			// New file: initialize header with zero entries
			stats.write_header(0);
		} else if stats.has_valid_magic() {
			// Existing file with valid header: read entry count
			let count = stats.read_entry_count();
			let max_slots = stats.max_slots();

			// Validate entry count doesn't exceed max slots
			if count > max_slots {
				return Err(Error::DiscardCorruptedEntryCount {
					count,
					max: max_slots,
				});
			}

			stats.next_empty_slot = count;

			// Validate entries for data integrity
			stats.validate_entries()?;
		} else {
			// Invalid/corrupt file: treat as new (no backward compat)
			stats.write_header(0);
		}

		// Sort entries to ensure binary search works
		stats.sort_entries()?;

		Ok(stats)
	}

	/// Returns the maximum number of slots available
	fn max_slots(&self) -> usize {
		(self.mmap.len() - HEADER_SIZE) / ENTRY_SIZE
	}

	/// Returns the byte offset for the file_id field at the given slot.
	/// Returns error if slot is out of bounds.
	fn file_id_offset(&self, slot: usize) -> Result<usize> {
		let max = self.max_slots();
		if slot >= max {
			return Err(Error::DiscardSlotOutOfBounds {
				slot,
				max,
			});
		}
		Ok(HEADER_SIZE + slot * ENTRY_SIZE)
	}

	/// Returns the byte offset for the discard_bytes field at the given slot.
	/// Returns error if slot is out of bounds.
	fn discard_bytes_offset(&self, slot: usize) -> Result<usize> {
		Ok(self.file_id_offset(slot)? + 4)
	}

	/// Gets the file ID at the given slot
	fn get_file_id(&self, slot: usize) -> Result<u32> {
		let offset = self.file_id_offset(slot)?;
		Ok(u32::from_be_bytes([
			self.mmap[offset],
			self.mmap[offset + 1],
			self.mmap[offset + 2],
			self.mmap[offset + 3],
		]))
	}

	/// Gets the discard bytes at the given slot
	fn get_discard_bytes(&self, slot: usize) -> Result<u64> {
		let offset = self.discard_bytes_offset(slot)?;
		Ok(u64::from_be_bytes([
			self.mmap[offset],
			self.mmap[offset + 1],
			self.mmap[offset + 2],
			self.mmap[offset + 3],
			self.mmap[offset + 4],
			self.mmap[offset + 5],
			self.mmap[offset + 6],
			self.mmap[offset + 7],
		]))
	}

	/// Sets the file ID at the given slot
	fn set_file_id(&mut self, slot: usize, file_id: u32) -> Result<()> {
		let offset = self.file_id_offset(slot)?;
		self.mmap[offset..offset + 4].copy_from_slice(&file_id.to_be_bytes());
		Ok(())
	}

	/// Sets the discard bytes at the given slot
	fn set_discard_bytes(&mut self, slot: usize, discard_bytes: u64) -> Result<()> {
		let offset = self.discard_bytes_offset(slot)?;
		self.mmap[offset..offset + 8].copy_from_slice(&discard_bytes.to_be_bytes());
		Ok(())
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

	/// Validates all entries for data integrity
	/// Checks that entries are sorted by file_id with no duplicates
	fn validate_entries(&self) -> Result<()> {
		if self.next_empty_slot == 0 {
			return Ok(());
		}

		let mut prev_file_id: Option<u32> = None;

		for slot in 0..self.next_empty_slot {
			let file_id = self.get_file_id(slot)?;

			if let Some(prev) = prev_file_id {
				if file_id <= prev {
					return Err(Error::DiscardCorruptedData {
						slot,
						reason: format!(
							"entries not sorted or duplicate: file_id {} <= previous {}",
							file_id, prev
						),
					});
				}
			}

			prev_file_id = Some(file_id);
		}

		Ok(())
	}

	/// Sorts entries by file_id
	fn sort_entries(&mut self) -> Result<()> {
		if self.next_empty_slot == 0 {
			return Ok(());
		}

		// Create a vector of (file_id, discard_bytes) tuples
		let mut entries: Vec<(u32, u64)> = Vec::with_capacity(self.next_empty_slot);
		for slot in 0..self.next_empty_slot {
			let file_id = self.get_file_id(slot)?;
			let discard_bytes = self.get_discard_bytes(slot)?;
			entries.push((file_id, discard_bytes));
		}

		// Sort by file_id
		entries.sort_by_key(|(file_id, _)| *file_id);

		// Write back sorted entries
		for (slot, (file_id, discard_bytes)) in entries.into_iter().enumerate() {
			self.set_file_id(slot, file_id)?;
			self.set_discard_bytes(slot, discard_bytes)?;
		}

		Ok(())
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
		let next_empty_slot = self.next_empty_slot;

		let (slot, found) = self.binary_search_with_position(file_id, next_empty_slot)?;

		if found {
			let current_discard = self.get_discard_bytes(slot)?;

			if discard_bytes == 0 {
				return Ok(current_discard);
			}

			if discard_bytes < 0 {
				self.set_discard_bytes(slot, 0)?;
				return Ok(0);
			}

			let new_discard = current_discard.saturating_add(discard_bytes as u64);
			self.set_discard_bytes(slot, new_discard)?;
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

		let insert_pos = slot;

		// Shift entries right to make room
		for i in (insert_pos..next_empty_slot).rev() {
			let fid = self.get_file_id(i)?;
			let db = self.get_discard_bytes(i)?;
			self.set_file_id(i + 1, fid)?;
			self.set_discard_bytes(i + 1, db)?;
		}

		// Insert new entry at correct position
		self.set_file_id(insert_pos, file_id)?;
		self.set_discard_bytes(insert_pos, discard_bytes as u64)?;

		// Update count in memory and header
		let new_count = next_empty_slot + 1;
		self.next_empty_slot = new_count;
		self.write_entry_count(new_count);

		Ok(discard_bytes as u64)
	}

	/// Unified binary search that returns (position, found)
	/// - If found: returns (slot_index, true)
	/// - If not found: returns (insertion_point, false)
	fn binary_search_with_position(&self, file_id: u32, count: usize) -> Result<(usize, bool)> {
		if count == 0 {
			return Ok((0, false));
		}

		let mut left = 0;
		let mut right = count;

		while left < right {
			let mid = left + (right - left) / 2;
			let mid_id = self.get_file_id(mid)?;

			match mid_id.cmp(&file_id) {
				std::cmp::Ordering::Less => left = mid + 1,
				std::cmp::Ordering::Greater => right = mid,
				std::cmp::Ordering::Equal => return Ok((mid, true)),
			}
		}

		Ok((left, false))
	}

	/// Binary search for a file_id, returns the slot index if found
	fn binary_search(&self, file_id: u32, next_empty_slot: usize) -> Result<Option<usize>> {
		let (pos, found) = self.binary_search_with_position(file_id, next_empty_slot)?;
		Ok(if found {
			Some(pos)
		} else {
			None
		})
	}

	/// Expands the memory-mapped file by doubling its size
	fn expand(&mut self) -> Result<()> {
		let current_size = self.mmap.len();

		// ADDED: Overflow check
		let new_size = current_size
			.checked_mul(2)
			.ok_or_else(|| crate::error::Error::Corruption("mmap size overflow".into()))?;

		self.mmap.flush()?;
		let mmap_copy: Vec<u8> = self.mmap[..current_size].to_vec();

		let temp_mmap = MmapMut::map_anon(1)?;
		let _ = std::mem::replace(&mut self.mmap, temp_mmap);

		self.file.set_len(new_size as u64)?;
		self.mmap = unsafe { MmapOptions::new().map_mut(&self.file)? };
		self.mmap[..current_size].copy_from_slice(&mmap_copy);

		// ADDED: Zero new region
		self.mmap[current_size..].fill(0);

		Ok(())
	}

	/// Returns all files with discard bytes, sorted by discard bytes
	/// (descending) Returns a vector of (file_id, discard_bytes) tuples
	pub(crate) fn get_gc_candidates(&self) -> Result<Vec<(u32, u64)>> {
		let mut candidates = Vec::new();

		for slot in 0..self.next_empty_slot {
			let file_id = self.get_file_id(slot)?;
			let discard_bytes = self.get_discard_bytes(slot)?;

			if discard_bytes > 0 {
				candidates.push((file_id, discard_bytes));
			}
		}

		// Sort by discard_bytes in descending order
		candidates.sort_by(|a, b| b.1.cmp(&a.1));

		Ok(candidates)
	}

	/// Gets discard bytes for a specific file
	/// Returns 0 if file not found
	pub(crate) fn get_file_stats(&self, file_id: u32) -> Result<u64> {
		match self.binary_search(file_id, self.next_empty_slot)? {
			Some(slot) => self.get_discard_bytes(slot),
			None => Ok(0),
		}
	}

	/// Removes statistics for a file
	/// Returns true if file was found and removed, false if not found
	pub(crate) fn remove_file(&mut self, file_id: u32) -> Result<bool> {
		let slot_to_remove = self.binary_search(file_id, self.next_empty_slot)?;

		if let Some(slot) = slot_to_remove {
			// Shift all entries after this slot to the left
			for i in slot..self.next_empty_slot.saturating_sub(1) {
				let next_file_id = self.get_file_id(i + 1)?;
				let next_discard = self.get_discard_bytes(i + 1)?;
				self.set_file_id(i, next_file_id)?;
				self.set_discard_bytes(i, next_discard)?;
			}

			// Update count in memory and header
			let new_count = self.next_empty_slot.saturating_sub(1);
			self.next_empty_slot = new_count;
			self.write_entry_count(new_count);
			Ok(true)
		} else {
			Ok(false)
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
		self.next_empty_slot
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
		let candidates = stats.get_gc_candidates().unwrap();
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
			assert_eq!(stats.get_file_stats(1).unwrap(), 100);
			assert_eq!(stats.get_file_stats(2).unwrap(), 200);
			assert_eq!(stats.get_file_stats(3).unwrap(), 300);

			let candidates = stats.get_gc_candidates().unwrap();
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
		assert!(stats.remove_file(2).unwrap());
		assert!(!stats.remove_file(999).unwrap()); // Not found

		// Verify removal
		assert_eq!(stats.get_file_stats(1).unwrap(), 100);
		assert_eq!(stats.get_file_stats(2).unwrap(), 0); // Removed
		assert_eq!(stats.get_file_stats(3).unwrap(), 300);

		// Verify max discard updated
		let candidates = stats.get_gc_candidates().unwrap();
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
			assert_eq!(stats.get_file_stats(i as u32).unwrap(), 100);
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
		assert_eq!(stats.get_file_stats(0).unwrap(), 500);
		assert_eq!(stats.get_file_stats(1).unwrap(), 100);
		assert_eq!(stats.get_file_stats(2).unwrap(), 200);

		// Verify sorted order: 0, 1, 2
		assert_eq!(stats.get_file_id(0).unwrap(), 0);
		assert_eq!(stats.get_file_id(1).unwrap(), 1);
		assert_eq!(stats.get_file_id(2).unwrap(), 2);
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
			assert_eq!(stats.get_file_stats(0).unwrap(), 50);
			assert_eq!(stats.get_file_stats(5).unwrap(), 500);
			assert_eq!(stats.get_file_stats(10).unwrap(), 1000);
			assert_eq!(stats.get_file_stats(15).unwrap(), 1500);
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
			assert_eq!(stats.get_file_stats(0).unwrap(), 100);
			assert_eq!(stats.get_file_stats(1).unwrap(), 200);
			assert_eq!(stats.get_file_stats(2).unwrap(), 300);
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
			let fid = stats.get_file_id(i).unwrap();
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
		let first_val = stats.get_file_stats(1).unwrap();
		assert_eq!(first_val, i64::MAX as u64);

		// Adding another large value should saturate at u64::MAX
		stats.update(1, i64::MAX).unwrap();
		let saturated_val = stats.get_file_stats(1).unwrap();

		// i64::MAX + i64::MAX = 2 * i64::MAX which is u64::MAX - 1
		// The saturating_add works on u64, so this tests that overflow is handled
		assert_eq!(saturated_val, (i64::MAX as u64).saturating_add(i64::MAX as u64));

		// Now test actual saturation at u64::MAX
		stats.update(1, i64::MAX).unwrap();
		assert_eq!(stats.get_file_stats(1).unwrap(), u64::MAX);
	}

	#[test]
	fn test_remove_nonexistent() {
		let temp_dir = TempDir::new().unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		stats.update(1, 100).unwrap();
		assert!(!stats.remove_file(999).unwrap()); // Should return false

		assert_eq!(stats.entry_count(), 1);
	}

	#[test]
	fn test_query_nonexistent() {
		let temp_dir = TempDir::new().unwrap();
		let stats = DiscardStats::new(temp_dir.path()).unwrap();

		assert_eq!(stats.get_file_stats(999).unwrap(), 0);
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
		assert_eq!(stats.get_file_id(0).unwrap(), 0);
		assert_eq!(stats.get_file_id(1).unwrap(), 1);
		assert_eq!(stats.get_file_id(2).unwrap(), u32::MAX / 2);
		assert_eq!(stats.get_file_id(3).unwrap(), u32::MAX);
	}

	#[test]
	fn test_binary_search() {
		let temp_dir = TempDir::new().unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		stats.update(10, 100).unwrap();
		stats.update(20, 200).unwrap();
		stats.update(30, 300).unwrap();

		// Test found cases
		assert_eq!(stats.binary_search_with_position(10, 3).unwrap(), (0, true));
		assert_eq!(stats.binary_search_with_position(20, 3).unwrap(), (1, true));
		assert_eq!(stats.binary_search_with_position(30, 3).unwrap(), (2, true));

		// Test not-found cases (returns insertion point)
		assert_eq!(stats.binary_search_with_position(5, 3).unwrap(), (0, false));
		assert_eq!(stats.binary_search_with_position(15, 3).unwrap(), (1, false));
		assert_eq!(stats.binary_search_with_position(25, 3).unwrap(), (2, false));
		assert_eq!(stats.binary_search_with_position(35, 3).unwrap(), (3, false));
	}

	#[test]
	fn test_expand_zeroes_new_region() {
		let temp_dir = TempDir::new().unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		let initial_size = stats.mmap.len();

		// Fill to trigger expansion
		let slots_needed = stats.max_slots() + 1;
		for i in 0..slots_needed {
			stats.update(i as u32, 1).unwrap();
		}

		// Verify expansion happened
		assert!(stats.mmap.len() > initial_size);

		// Verify unused region is zeroed
		let used_bytes = HEADER_SIZE + slots_needed * ENTRY_SIZE;
		for i in used_bytes..stats.mmap.len() {
			assert_eq!(stats.mmap[i], 0, "Byte {} should be zero", i);
		}
	}

	// ========================================================================
	// ERROR HANDLING TESTS
	// ========================================================================

	#[test]
	fn test_slot_out_of_bounds_error() {
		let temp_dir = TempDir::new().unwrap();
		let stats = DiscardStats::new(temp_dir.path()).unwrap();

		let max = stats.max_slots();

		// Accessing slot beyond max should return error
		let result = stats.get_file_id(max);
		assert!(result.is_err());
		match result.unwrap_err() {
			Error::DiscardSlotOutOfBounds {
				slot,
				max: m,
			} => {
				assert_eq!(slot, max);
				assert_eq!(m, max);
			}
			e => panic!("Expected DiscardSlotOutOfBounds, got {:?}", e),
		}

		let result = stats.get_discard_bytes(max + 10);
		assert!(result.is_err());
		match result.unwrap_err() {
			Error::DiscardSlotOutOfBounds {
				slot,
				max: m,
			} => {
				assert_eq!(slot, max + 10);
				assert_eq!(m, max);
			}
			e => panic!("Expected DiscardSlotOutOfBounds, got {:?}", e),
		}
	}

	#[test]
	fn test_corrupted_entry_count_detection() {
		let temp_dir = TempDir::new().unwrap();

		// Create a valid stats file first
		{
			let mut stats = DiscardStats::new(temp_dir.path()).unwrap();
			stats.update(1, 100).unwrap();
			stats.update(2, 200).unwrap();
		}

		// Now corrupt the entry count in the file
		let path = temp_dir.path().join(DISCARD_STATS_FILENAME);
		let file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
		let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };

		// Get max_slots for this mmap
		let max_slots = (mmap.len() - HEADER_SIZE) / ENTRY_SIZE;

		// Set entry count to an invalid value (larger than max_slots)
		// Count offset is 8 (after 4-byte magic + 4-byte version)
		let count_offset = 8;
		let corrupted_count = (max_slots + 100) as u32;
		mmap[count_offset..count_offset + 4].copy_from_slice(&corrupted_count.to_be_bytes());
		mmap.flush().unwrap();
		drop(mmap);

		// Opening should detect corruption
		let result = DiscardStats::new(temp_dir.path());
		assert!(result.is_err());
		match result.unwrap_err() {
			Error::DiscardCorruptedEntryCount {
				count,
				max,
			} => {
				assert_eq!(count, corrupted_count as usize);
				assert_eq!(max, max_slots);
			}
			e => panic!("Expected DiscardCorruptedEntryCount, got {:?}", e),
		}
	}

	#[test]
	fn test_corrupted_data_unsorted_detection() {
		let temp_dir = TempDir::new().unwrap();

		// Create a valid stats file first
		{
			let mut stats = DiscardStats::new(temp_dir.path()).unwrap();
			stats.update(1, 100).unwrap();
			stats.update(2, 200).unwrap();
			stats.update(3, 300).unwrap();
		}

		// Now corrupt the data by making entries unsorted
		let path = temp_dir.path().join(DISCARD_STATS_FILENAME);
		let file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
		let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };

		// Swap file_id at slot 0 and slot 2 to break sorted order
		// Slot 0 has file_id=1, slot 2 has file_id=3
		// We'll set slot 1 to have file_id=5 (which breaks order: 1, 5, 3)
		let slot1_offset = HEADER_SIZE + ENTRY_SIZE;
		let bad_file_id: u32 = 5;
		mmap[slot1_offset..slot1_offset + 4].copy_from_slice(&bad_file_id.to_be_bytes());
		mmap.flush().unwrap();
		drop(mmap);

		// Opening should detect corruption
		let result = DiscardStats::new(temp_dir.path());
		assert!(result.is_err());
		match result.unwrap_err() {
			Error::DiscardCorruptedData {
				slot,
				reason,
			} => {
				assert_eq!(slot, 2); // Slot 2 is where the order violation is detected
				assert!(reason.contains("not sorted"));
			}
			e => panic!("Expected DiscardCorruptedData, got {:?}", e),
		}
	}

	#[test]
	fn test_corrupted_data_duplicate_detection() {
		let temp_dir = TempDir::new().unwrap();

		// Create a valid stats file first
		{
			let mut stats = DiscardStats::new(temp_dir.path()).unwrap();
			stats.update(1, 100).unwrap();
			stats.update(2, 200).unwrap();
			stats.update(3, 300).unwrap();
		}

		// Now corrupt the data by creating duplicate file_ids
		let path = temp_dir.path().join(DISCARD_STATS_FILENAME);
		let file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
		let mut mmap = unsafe { memmap2::MmapMut::map_mut(&file).unwrap() };

		// Set slot 1 to have the same file_id as slot 0 (duplicate: 1, 1, 3)
		let slot1_offset = HEADER_SIZE + ENTRY_SIZE;
		let duplicate_file_id: u32 = 1;
		mmap[slot1_offset..slot1_offset + 4].copy_from_slice(&duplicate_file_id.to_be_bytes());
		mmap.flush().unwrap();
		drop(mmap);

		// Opening should detect corruption
		let result = DiscardStats::new(temp_dir.path());
		assert!(result.is_err());
		match result.unwrap_err() {
			Error::DiscardCorruptedData {
				slot,
				reason,
			} => {
				assert_eq!(slot, 1); // Duplicate detected at slot 1
				assert!(reason.contains("duplicate"));
			}
			e => panic!("Expected DiscardCorruptedData, got {:?}", e),
		}
	}

	#[test]
	fn test_get_gc_candidates_returns_result() {
		let temp_dir = TempDir::new().unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		stats.update(1, 100).unwrap();
		stats.update(2, 200).unwrap();

		// Normal operation should return Ok
		let candidates = stats.get_gc_candidates().unwrap();
		assert_eq!(candidates.len(), 2);
		// Sorted by discard bytes descending
		assert_eq!(candidates[0], (2, 200));
		assert_eq!(candidates[1], (1, 100));
	}

	#[test]
	fn test_remove_file_returns_result() {
		let temp_dir = TempDir::new().unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		stats.update(1, 100).unwrap();
		stats.update(2, 200).unwrap();

		// Remove existing file should return Ok(true)
		assert!(stats.remove_file(1).unwrap());
		assert_eq!(stats.entry_count(), 1);

		// Remove non-existing file should return Ok(false)
		assert!(!stats.remove_file(999).unwrap());
		assert_eq!(stats.entry_count(), 1);
	}
}
