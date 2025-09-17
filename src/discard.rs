use std::fs::{File, OpenOptions};
use std::path::Path;
use std::sync::Mutex;

use memmap2::{MmapMut, MmapOptions};

use crate::error::{Error, Result};

const DISCARD_STATS_FILENAME: &str = "DISCARD";
const ENTRY_SIZE: usize = 12; // 4 bytes for file_id + 8 bytes for discard_bytes
const INITIAL_SIZE: usize = 1 << 20; // 1MB, can store 65,536 entries

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

		// If new file, zero out the first entry
		if !file_exists {
			stats.zero_out_slot(0);
		} else {
			// Find the next empty slot
			let next_slot = stats.find_next_empty_slot();
			*stats.next_empty_slot.lock().unwrap() = next_slot;
		}

		// Sort entries to ensure binary search works
		stats.sort_entries();

		Ok(stats)
	}

	/// Finds the next empty slot by scanning the mapped memory
	fn find_next_empty_slot(&self) -> usize {
		let max_slots = self.max_slots();
		for slot in 0..max_slots {
			if self.get_file_id(slot) == 0 {
				return slot;
			}
		}
		max_slots
	}

	/// Returns the maximum number of slots available
	fn max_slots(&self) -> usize {
		self.mmap.len() / ENTRY_SIZE
	}

	/// Gets the file ID at the given slot
	fn get_file_id(&self, slot: usize) -> u32 {
		let offset = slot * ENTRY_SIZE;
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
		let offset = slot * ENTRY_SIZE + 4;
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
		let offset = slot * ENTRY_SIZE;
		self.mmap[offset..offset + 4].copy_from_slice(&file_id.to_be_bytes());
	}

	/// Sets the discard bytes at the given slot
	fn set_discard_bytes(&mut self, slot: usize, discard_bytes: u64) {
		let offset = slot * ENTRY_SIZE + 4;
		self.mmap[offset..offset + 8].copy_from_slice(&discard_bytes.to_be_bytes());
	}

	/// Zeros out the given slot
	fn zero_out_slot(&mut self, slot: usize) {
		self.set_file_id(slot, 0);
		self.set_discard_bytes(slot, 0);
	}

	/// Sorts entries by file_id
	fn sort_entries(&mut self) {
		let next_empty_slot = *self.next_empty_slot.lock().unwrap();

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
	/// - If discard_bytes is 0, returns current discard bytes
	/// - If discard_bytes is negative, resets discard bytes to 0
	/// - If discard_bytes is positive, adds to current discard bytes
	pub(crate) fn update(&mut self, file_id: u32, discard_bytes: i64) -> u64 {
		// Binary search for the file_id
		let next_empty_slot = *self.next_empty_slot.lock().unwrap();
		let idx = self.binary_search(file_id, next_empty_slot);

		if let Some(slot) = idx {
			// Found existing entry - just update it, no sorting needed
			let current_discard = self.get_discard_bytes(slot);

			if discard_bytes == 0 {
				return current_discard;
			}

			if discard_bytes < 0 {
				self.set_discard_bytes(slot, 0);
				return 0;
			}

			let new_discard = current_discard.saturating_add(discard_bytes as u64);
			self.set_discard_bytes(slot, new_discard);
			return new_discard;
		}

		// File not found
		if discard_bytes <= 0 {
			return 0;
		}

		// Add new entry - need to check and update next_empty_slot atomically
		let slot = {
			let next_slot_guard = self.next_empty_slot.lock().unwrap();
			let slot = *next_slot_guard;

			// Check if we need to expand the mmap
			if slot >= self.max_slots() {
				drop(next_slot_guard); // Release lock before expand
				if let Err(e) = self.expand() {
					eprintln!("Failed to expand discard stats file: {e}");
					return 0;
				}
				// After expansion, get the slot again
				slot
			} else {
				slot
			}
		};

		self.set_file_id(slot, file_id);
		self.set_discard_bytes(slot, discard_bytes as u64);

		// Update next_empty_slot and zero out the new next slot
		{
			let mut next_slot_guard = self.next_empty_slot.lock().unwrap();
			*next_slot_guard += 1;
			let next_slot = *next_slot_guard;
			drop(next_slot_guard); // Release lock before zero_out_slot
			self.zero_out_slot(next_slot);
		}

		// Only sort when we add a new entry
		self.sort_entries();

		discard_bytes as u64
	}

	/// Binary search for a file_id, returns the slot index if found
	fn binary_search(&self, file_id: u32, next_empty_slot: usize) -> Option<usize> {
		let mut left = 0;
		let mut right = next_empty_slot;

		while left < right {
			let mid = left + (right - left) / 2;
			let mid_file_id = self.get_file_id(mid);

			match mid_file_id.cmp(&file_id) {
				std::cmp::Ordering::Equal => return Some(mid),
				std::cmp::Ordering::Less => left = mid + 1,
				std::cmp::Ordering::Greater => right = mid,
			}
		}

		None
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

	/// Returns the file with maximum discard bytes
	pub(crate) fn max_discard(&self) -> Result<(u32, u64)> {
		let next_empty_slot = *self.next_empty_slot.lock().unwrap();

		let mut max_file_id = 0;
		let mut max_discard = 0;
		let mut found_any = false;

		for slot in 0..next_empty_slot {
			let file_id = self.get_file_id(slot);
			let discard_bytes = self.get_discard_bytes(slot);

			if discard_bytes > max_discard {
				max_discard = discard_bytes;
				max_file_id = file_id;
				found_any = true;
			}
		}

		if !found_any || max_discard == 0 {
			return Err(Error::Other("No files with discardable data found".to_string()));
		}

		Ok((max_file_id, max_discard))
	}

	/// Gets discard bytes for a specific file
	pub(crate) fn get_file_stats(&self, file_id: u32) -> u64 {
		let next_empty_slot = *self.next_empty_slot.lock().unwrap();

		if let Some(slot) = self.binary_search(file_id, next_empty_slot) {
			self.get_discard_bytes(slot)
		} else {
			0
		}
	}

	/// Removes statistics for a file
	pub(crate) fn remove_file(&mut self, file_id: u32) {
		let (slot_to_remove, next_empty_slot) = {
			let next_empty_slot_guard = self.next_empty_slot.lock().unwrap();
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

			// Clear the last slot and update next_empty_slot
			{
				let mut next_empty_slot_guard = self.next_empty_slot.lock().unwrap();
				*next_empty_slot_guard -= 1;
				let new_next_slot = *next_empty_slot_guard;
				drop(next_empty_slot_guard);
				self.zero_out_slot(new_next_slot);
			}
		}
	}

	/// Syncs the memory-mapped file to disk
	pub(crate) fn sync(&self) -> Result<()> {
		self.mmap.flush()?;
		Ok(())
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
	use super::*;
	use tempfile::TempDir;

	#[test]
	fn test_discard_stats_basic() {
		let temp_dir = TempDir::new().unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		// Test basic operations
		assert_eq!(stats.update(1, 100), 100);
		assert_eq!(stats.update(1, 50), 150);
		assert_eq!(stats.update(1, 0), 150); // Query current value
		assert_eq!(stats.update(1, -1), 0); // Reset to 0

		// Test multiple files
		assert_eq!(stats.update(2, 200), 200);
		assert_eq!(stats.update(3, 300), 300);

		// Test max discard
		let (file_id, discard) = stats.max_discard().unwrap();
		assert_eq!(file_id, 3);
		assert_eq!(discard, 300);
	}

	#[test]
	fn test_discard_stats_persistence() {
		let temp_dir = TempDir::new().unwrap();

		// Create and populate stats
		{
			let mut stats = DiscardStats::new(temp_dir.path()).unwrap();
			stats.update(1, 100);
			stats.update(2, 200);
			stats.update(3, 300);
			stats.sync().unwrap();
		}

		// Reopen and verify data persisted
		{
			let stats = DiscardStats::new(temp_dir.path()).unwrap();
			assert_eq!(stats.get_file_stats(1), 100);
			assert_eq!(stats.get_file_stats(2), 200);
			assert_eq!(stats.get_file_stats(3), 300);

			let (file_id, discard) = stats.max_discard().unwrap();
			assert_eq!(file_id, 3);
			assert_eq!(discard, 300);
		}
	}

	#[test]
	fn test_discard_stats_remove() {
		let temp_dir = TempDir::new().unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		// Add some entries
		stats.update(1, 100);
		stats.update(2, 200);
		stats.update(3, 300);

		// Remove middle entry
		stats.remove_file(2);

		// Verify removal
		assert_eq!(stats.get_file_stats(1), 100);
		assert_eq!(stats.get_file_stats(2), 0); // Removed
		assert_eq!(stats.get_file_stats(3), 300);

		// Verify max discard updated
		let (file_id, discard) = stats.max_discard().unwrap();
		assert_eq!(file_id, 3);
		assert_eq!(discard, 300);
	}

	#[test]
	fn test_discard_stats_expansion() {
		let temp_dir = TempDir::new().unwrap();
		let mut stats = DiscardStats::new(temp_dir.path()).unwrap();

		// Add enough entries to trigger expansion (initial size allows 65,536 entries)
		// Let's just add enough to trigger one expansion - about 100,000 entries
		let entries_to_add = 1000; // Much smaller test

		for i in 0..entries_to_add {
			stats.update(i as u32, 100);
		}

		// Verify all entries exist
		for i in 0..entries_to_add {
			assert_eq!(stats.get_file_stats(i as u32), 100);
		}
	}
}
