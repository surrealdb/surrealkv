//! LevelIterator provides lazy, single-table-at-a-time iteration over a level.
//!
//! Instead of creating iterators for all tables upfront, LevelIterator:
//! - Uses binary search to find the correct table for seeks
//! - Opens only one table iterator at a time
//! - Advances to the next table only when the current one is exhausted
//!
//! This reduces heap size from O(total_tables) to O(num_levels) and avoids
//! opening files that are never accessed.

use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;

use crate::sstable::meta::KeyRange;
use crate::sstable::table::{Table, TableIterator};
use crate::sstable::{InternalKey, InternalKeyKind, INTERNAL_KEY_SEQ_NUM_MAX};
use crate::Iterator as LSMIterator;

/// Direction of iteration
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Direction {
	/// Not yet positioned
	Uninitialized,
	/// Iterating forward (Next/Seek)
	Forward,
	/// Iterating backward (Prev/SeekToLast)
	Backward,
}

/// An iterator over a single level that lazily opens tables.
///
/// Key design decisions:
/// - Only one TableIterator is active at a time
/// - Tables are opened lazily when needed
/// - For Level 0 (overlapping tables), we need all tables in merge
/// - For Level 1+ (non-overlapping), we can use single-table iteration
pub(crate) struct LevelIterator {
	/// Index of this level (0 = L0, 1 = L1, etc.)
	level_idx: usize,

	/// All tables in this level (sorted by key for L1+, by seqno for L0)
	tables: Vec<Arc<Table>>,

	/// Indices of tables that overlap with the query range [start_idx, end_idx)
	overlapping_start: usize,
	overlapping_end: usize,

	/// Current table index within the overlapping range
	current_table_idx: usize,

	/// Current table iterator (only one active at a time)
	current_iter: Option<TableIterator>,

	/// Query range for seeking
	range_start: Bound<Vec<u8>>,

	/// Whether to skip values (for count queries)
	keys_only: bool,

	/// Current direction of iteration
	direction: Direction,
}

impl LevelIterator {
	/// Create a new LevelIterator for the given level and query range.
	///
	/// # Arguments
	/// * `level_idx` - Index of the level (0 for L0, 1 for L1, etc.)
	/// * `tables` - All tables in this level
	/// * `query_range` - Key range for filtering tables
	/// * `range_start` - Start bound for seeking
	/// * `keys_only` - Whether to skip value loading
	pub(crate) fn new(
		level_idx: usize,
		tables: Vec<Arc<Table>>,
		query_range: &KeyRange,
		range_start: Bound<Vec<u8>>,
		keys_only: bool,
	) -> Self {
		// Find overlapping tables based on level type
		let (overlapping_start, overlapping_end) = if level_idx == 0 {
			// L0: Tables can overlap, check all but skip those completely outside range
			let mut start = 0;
			let mut end = tables.len();

			// Find first overlapping table
			for (i, table) in tables.iter().enumerate() {
				if !table.is_before_range(query_range) && !table.is_after_range(query_range) {
					start = i;
					break;
				}
			}

			// Find last overlapping table
			for i in (start..tables.len()).rev() {
				if !tables[i].is_before_range(query_range) && !tables[i].is_after_range(query_range)
				{
					end = i + 1;
					break;
				}
			}

			// If no overlap found, set empty range
			if start >= tables.len() {
				(0, 0)
			} else {
				(start, end)
			}
		} else {
			// L1+: Tables have non-overlapping key ranges, use binary search
			let start = tables.partition_point(|t| t.is_before_range(query_range));
			let end = tables.partition_point(|t| !t.is_after_range(query_range));
			(start, end)
		};

		Self {
			level_idx,
			tables,
			overlapping_start,
			overlapping_end,
			current_table_idx: overlapping_start,
			current_iter: None,
			range_start,
			keys_only,
			direction: Direction::Uninitialized,
		}
	}

	/// Check if there are no overlapping tables
	fn is_empty(&self) -> bool {
		self.overlapping_start >= self.overlapping_end
	}

	/// Get the current table, if any
	fn current_table(&self) -> Option<&Arc<Table>> {
		if self.current_table_idx >= self.overlapping_start
			&& self.current_table_idx < self.overlapping_end
		{
			Some(&self.tables[self.current_table_idx])
		} else {
			None
		}
	}

	/// Open the iterator for the current table and seek to start
	fn open_current_table_forward(&mut self) {
		if let Some(table) = self.current_table() {
			let mut iter = table.iter(self.keys_only);

			// Seek to the range start
			match &self.range_start {
				Bound::Included(key) => {
					let ikey = InternalKey::new(
						Bytes::copy_from_slice(key),
						INTERNAL_KEY_SEQ_NUM_MAX,
						InternalKeyKind::Max,
						0,
					);
					iter.seek(&ikey.encode());
				}
				Bound::Excluded(key) => {
					let ikey =
						InternalKey::new(Bytes::copy_from_slice(key), 0, InternalKeyKind::Delete, 0);
					iter.seek(&ikey.encode());
					// If we're at the excluded key, advance once
					if iter.valid() {
						let current_user_key = InternalKey::extract_user_key(iter.key_bytes());
						if current_user_key == key.as_slice() {
							iter.advance();
						}
					}
				}
				Bound::Unbounded => {
					iter.seek_to_first();
				}
			}

			self.current_iter = Some(iter);
		} else {
			self.current_iter = None;
		}
	}

	/// Open the iterator for the current table positioned at last entry
	fn open_current_table_backward(&mut self) {
		if let Some(table) = self.current_table() {
			let mut iter = table.iter(self.keys_only);
			iter.seek_to_last();
			self.current_iter = Some(iter);
		} else {
			self.current_iter = None;
		}
	}

	/// Advance to the next table in the overlapping range
	fn advance_to_next_table(&mut self) -> bool {
		self.current_table_idx += 1;
		if self.current_table_idx >= self.overlapping_end {
			self.current_iter = None;
			return false;
		}

		self.open_current_table_forward();

		// Check if the new iterator is valid
		if let Some(ref iter) = self.current_iter {
			if iter.valid() {
				return true;
			}
		}

		// This table is empty or exhausted, try next
		self.advance_to_next_table()
	}

	/// Move to the previous table in the overlapping range
	fn move_to_prev_table(&mut self) -> bool {
		if self.current_table_idx <= self.overlapping_start {
			self.current_iter = None;
			return false;
		}

		self.current_table_idx -= 1;
		self.open_current_table_backward();

		// Check if the new iterator is valid
		if let Some(ref iter) = self.current_iter {
			if iter.valid() {
				return true;
			}
		}

		// This table is empty or exhausted, try previous
		self.move_to_prev_table()
	}

	/// Find the table containing the target key using binary search (L1+ only)
	fn find_table_for_key(&self, target: &[u8]) -> Option<usize> {
		if self.is_empty() {
			return None;
		}

		let user_key = InternalKey::extract_user_key(target);

		// Binary search to find the table that might contain this key
		let tables_slice = &self.tables[self.overlapping_start..self.overlapping_end];

		// Find the first table whose key range could contain this key
		let idx = tables_slice.partition_point(|table| {
			if let Some(ref range) = table.meta.properties.key_range {
				range.high.as_ref() < user_key
			} else {
				false
			}
		});

		if idx < tables_slice.len() {
			Some(self.overlapping_start + idx)
		} else {
			None
		}
	}
}

impl LSMIterator for LevelIterator {
	fn valid(&self) -> bool {
		if let Some(ref iter) = self.current_iter {
			iter.valid()
		} else {
			false
		}
	}

	fn seek_to_first(&mut self) {
		self.direction = Direction::Forward;

		if self.is_empty() {
			self.current_iter = None;
			return;
		}

		self.current_table_idx = self.overlapping_start;
		self.open_current_table_forward();

		// If current table has no valid entries, advance to next
		if !self.valid() {
			self.advance_to_next_table();
		}
	}

	fn seek_to_last(&mut self) {
		self.direction = Direction::Backward;

		if self.is_empty() {
			self.current_iter = None;
			return;
		}

		self.current_table_idx = self.overlapping_end - 1;
		self.open_current_table_backward();

		// If current table has no valid entries, move to previous
		if !self.valid() {
			self.move_to_prev_table();
		}
	}

	fn seek(&mut self, target: &[u8]) -> Option<()> {
		self.direction = Direction::Forward;

		if self.is_empty() {
			self.current_iter = None;
			return Some(());
		}

		// For L1+, use binary search to find the right table
		if self.level_idx > 0 {
			if let Some(table_idx) = self.find_table_for_key(target) {
				self.current_table_idx = table_idx;
			} else {
				// Key is beyond all tables
				self.current_iter = None;
				return Some(());
			}
		} else {
			// For L0, start from the first overlapping table
			self.current_table_idx = self.overlapping_start;
		}

		// Open the table and seek
		if let Some(table) = self.current_table() {
			let mut iter = table.iter(self.keys_only);
			iter.seek(target);
			self.current_iter = Some(iter);

			// If not valid, advance to next table
			if !self.valid() {
				self.advance_to_next_table();
			}
		} else {
			self.current_iter = None;
		}

		Some(())
	}

	fn advance(&mut self) -> bool {
		if self.direction == Direction::Uninitialized {
			self.seek_to_first();
			return self.valid();
		}

		// Try to advance within current table
		if let Some(ref mut iter) = self.current_iter {
			if iter.advance() {
				return true;
			}
		}

		// Current table exhausted, move to next
		self.advance_to_next_table()
	}

	fn prev(&mut self) -> bool {
		if self.direction == Direction::Uninitialized {
			self.seek_to_last();
			return self.valid();
		}

		// Try to move backward within current table
		if let Some(ref mut iter) = self.current_iter {
			if iter.prev() {
				return true;
			}
		}

		// Current table exhausted, move to previous
		self.move_to_prev_table()
	}

	fn key_bytes(&self) -> &[u8] {
		self.current_iter.as_ref().map(|i| i.key_bytes()).unwrap_or(&[])
	}

	fn value_bytes(&self) -> &[u8] {
		self.current_iter.as_ref().map(|i| i.value_bytes()).unwrap_or(&[])
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_level_iterator_empty_tables() {
		// Create a LevelIterator with no tables
		let query_range = KeyRange::new((Bytes::from("a"), Bytes::from("z")));
		let iter = LevelIterator::new(1, vec![], &query_range, Bound::Unbounded, false);

		// Empty iterator should not be valid
		assert!(!iter.valid());
	}

	#[test]
	fn test_level_iterator_direction_uninitialized() {
		// Create a LevelIterator with no tables
		let query_range = KeyRange::new((Bytes::from("a"), Bytes::from("z")));
		let mut iter = LevelIterator::new(1, vec![], &query_range, Bound::Unbounded, false);

		// advance() on uninitialized should call seek_to_first()
		let result = iter.advance();
		assert!(!result); // No tables, so advance returns false

		// prev() on uninitialized should call seek_to_last()
		let mut iter2 = LevelIterator::new(1, vec![], &query_range, Bound::Unbounded, false);
		let result2 = iter2.prev();
		assert!(!result2); // No tables, so prev returns false
	}

	#[test]
	fn test_level_iterator_key_value_empty() {
		// Create a LevelIterator with no tables
		let query_range = KeyRange::new((Bytes::from("a"), Bytes::from("z")));
		let iter = LevelIterator::new(1, vec![], &query_range, Bound::Unbounded, false);

		// Empty key and value for invalid iterator
		assert_eq!(iter.key_bytes(), &[]);
		assert_eq!(iter.value_bytes(), &[]);
	}

	#[test]
	fn test_level_iterator_seek_empty() {
		let query_range = KeyRange::new((Bytes::from("a"), Bytes::from("z")));
		let mut iter = LevelIterator::new(1, vec![], &query_range, Bound::Unbounded, false);

		// Seek on empty iterator should work but not position
		iter.seek(b"test");
		assert!(!iter.valid());

		// seek_to_first on empty
		iter.seek_to_first();
		assert!(!iter.valid());

		// seek_to_last on empty
		iter.seek_to_last();
		assert!(!iter.valid());
	}
}

