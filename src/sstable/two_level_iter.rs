//! Two-level iterator for SSTable with built-in bounds support.
//!
//! This follows RocksDB's TwoLevelIterator pattern but with bounds checking
//! built-in rather than using a separate BoundedIterator wrapper.

use std::cmp::Ordering;
use std::ops::Bound;

use crate::error::Result;
use crate::sstable::block::{BlockHandle, BlockIterator};
use crate::sstable::index_block::PartitionedIndexIterator;
use crate::sstable::table::{IndexType, Table};
use crate::sstable::{
	InternalIterator,
	InternalKey,
	InternalKeyKind,
	InternalKeyRef,
	INTERNAL_KEY_SEQ_NUM_MAX,
	INTERNAL_KEY_TIMESTAMP_MAX,
};
use crate::InternalKeyRange;

/// Two-level iterator over SSTable with built-in bounds support.
///
/// Follows RocksDB's TwoLevelIterator pattern:
/// - first_level: PartitionedIndexIterator (navigates partition index entries)
/// - second_level: BlockIterator (navigates data block entries)
///
/// The iterator uses SkipEmptyDataBlocksForward/Backward pattern to handle
/// empty blocks and partition boundaries.
pub(crate) struct TwoLevelIterator<'a> {
	table: &'a Table,
	/// First level: iterates over partition index entries
	first_level: PartitionedIndexIterator<'a>,
	/// Second level: iterates over data block entries
	second_level: Option<BlockIterator>,
	/// Cached data block handle (to avoid reloading same block)
	data_block_handle: Option<BlockHandle>,
	/// Range bounds for filtering
	range: InternalKeyRange,
	/// Whether the iterator has been exhausted
	exhausted: bool,
}

impl<'a> TwoLevelIterator<'a> {
	/// Create a new TwoLevelIterator over the given table with the specified range.
	pub(crate) fn new(table: &'a Table, range: InternalKeyRange) -> Result<Self> {
		let IndexType::Partitioned(ref partitioned_index) = table.index_block;

		Ok(Self {
			table,
			first_level: PartitionedIndexIterator::new(partitioned_index),
			second_level: None,
			data_block_handle: None,
			range,
			exhausted: false,
		})
	}

	/// Check if the iterator is positioned on a valid entry
	fn is_valid(&self) -> bool {
		!self.exhausted && self.second_level.as_ref().map_or(false, |iter| iter.is_valid())
	}

	/// Mark the iterator as exhausted
	fn mark_exhausted(&mut self) {
		self.exhausted = true;
		self.second_level = None;
	}

	/// Initialize the data block from the current first-level position.
	fn init_data_block(&mut self) -> Result<()> {
		if !self.first_level.valid() {
			self.second_level = None;
			self.data_block_handle = None;
			return Ok(());
		}

		let handle = self.first_level.block_handle()?;

		// Check if we already have this block loaded (caching optimization)
		if let Some(ref current_handle) = self.data_block_handle {
			if current_handle.offset() == handle.offset() {
				// Already loaded, skip
				return Ok(());
			}
		}

		// Load the new data block
		let block = self.table.read_block(&handle)?;
		let iter = block.iter()?;
		self.second_level = Some(iter);
		self.data_block_handle = Some(handle);
		Ok(())
	}

	/// Skip empty data blocks forward until we find a valid entry or exhaust.
	fn skip_empty_data_blocks_forward(&mut self) -> Result<()> {
		loop {
			// Check if current second_level is valid
			if self.second_level.as_ref().map_or(false, |iter| iter.is_valid()) {
				return Ok(());
			}

			// Need to move to next block
			if !self.first_level.valid() {
				self.second_level = None;
				return Ok(());
			}

			self.first_level.next()?;
			self.init_data_block()?;

			if let Some(ref mut iter) = self.second_level {
				iter.seek_to_first()?;
			}
		}
	}

	/// Skip empty data blocks backward until we find a valid entry or exhaust.
	fn skip_empty_data_blocks_backward(&mut self) -> Result<()> {
		loop {
			// Check if current second_level is valid
			if self.second_level.as_ref().map_or(false, |iter| iter.is_valid()) {
				return Ok(());
			}

			// Need to move to previous block
			if !self.first_level.valid() {
				self.second_level = None;
				return Ok(());
			}

			self.first_level.prev()?;
			self.init_data_block()?;

			if let Some(ref mut iter) = self.second_level {
				iter.seek_to_last()?;
			}
		}
	}

	/// Check if a user key satisfies the lower bound constraint
	fn satisfies_lower_bound(&self, user_key: &[u8]) -> bool {
		match &self.range.0 {
			Bound::Included(start) => {
				self.table.opts.comparator.compare(user_key, &start.user_key) != Ordering::Less
			}
			Bound::Excluded(start) => {
				self.table.opts.comparator.compare(user_key, &start.user_key) == Ordering::Greater
			}
			Bound::Unbounded => true,
		}
	}

	/// Check if a user key satisfies the upper bound constraint
	fn satisfies_upper_bound(&self, user_key: &[u8]) -> bool {
		match &self.range.1 {
			Bound::Included(end) => {
				self.table.opts.comparator.compare(user_key, &end.user_key) != Ordering::Greater
			}
			Bound::Excluded(end) => {
				self.table.opts.comparator.compare(user_key, &end.user_key) == Ordering::Less
			}
			Bound::Unbounded => true,
		}
	}

	/// Get the current user key (requires valid iterator)
	fn current_user_key(&self) -> &[u8] {
		self.second_level.as_ref().unwrap().user_key()
	}

	/// Seek to first entry within bounds
	pub(crate) fn seek_to_first(&mut self) -> Result<()> {
		self.exhausted = false;

		// Clone bound to avoid borrow conflict
		let lower_bound = self.range.0.clone();

		match lower_bound {
			Bound::Unbounded => {
				// Raw seek to first
				self.first_level.seek_to_first()?;
				self.init_data_block()?;
				if let Some(ref mut iter) = self.second_level {
					iter.seek_to_first()?;
				}
				self.skip_empty_data_blocks_forward()?;
			}
			Bound::Included(ref internal_key) => {
				// Seek to the lower bound key
				self.seek_internal(&internal_key.encode())?;
			}
			Bound::Excluded(ref internal_key) => {
				// Seek to (user_key, seq_num=0) which sorts AFTER all versions
				// because keys sort by (user_key ASC, seq_num DESC)
				let seek_key = InternalKey::new(
					internal_key.user_key.clone(),
					0, // min seq_num sorts last for this user_key
					InternalKeyKind::Set,
					0,
				);
				self.seek_internal(&seek_key.encode())?;

				// If we landed on the excluded key, advance once
				if self.is_valid() && self.current_user_key() == internal_key.user_key.as_slice() {
					self.advance_internal()?;
				}
			}
		}

		// Verify within upper bound
		if self.is_valid() && !self.satisfies_upper_bound(self.current_user_key()) {
			self.mark_exhausted();
		}
		Ok(())
	}

	/// Seek to last entry within bounds
	pub(crate) fn seek_to_last(&mut self) -> Result<()> {
		self.exhausted = false;

		// Clone bound to avoid borrow conflict
		let upper_bound = self.range.1.clone();

		match upper_bound {
			Bound::Unbounded => {
				// Raw seek to last
				self.first_level.seek_to_last()?;
				self.init_data_block()?;
				if let Some(ref mut iter) = self.second_level {
					iter.seek_to_last()?;
				}
				self.skip_empty_data_blocks_backward()?;
			}
			Bound::Included(ref internal_key) => {
				// SeekForPrev semantics: find last entry <= bound
				let bound_user_key = internal_key.user_key.clone();
				let seek_key = InternalKey::new(
					internal_key.user_key.clone(),
					INTERNAL_KEY_SEQ_NUM_MAX,
					InternalKeyKind::Max,
					INTERNAL_KEY_TIMESTAMP_MAX,
				);
				self.seek_internal(&seek_key.encode())?;

				if !self.is_valid() {
					// Went past end, position at absolute last
					self.position_to_absolute_last()?;
				} else {
					// Check if we're past the bound
					let cmp = self
						.table
						.opts
						.comparator
						.compare(self.current_user_key(), bound_user_key.as_slice());
					if cmp == Ordering::Greater {
						self.prev_internal()?;
					}
				}
			}
			Bound::Excluded(ref internal_key) => {
				// SeekForPrev for excluded: find last entry < bound
				let bound_user_key = internal_key.user_key.clone();
				let seek_key = InternalKey::new(
					internal_key.user_key.clone(),
					INTERNAL_KEY_SEQ_NUM_MAX,
					InternalKeyKind::Max,
					INTERNAL_KEY_TIMESTAMP_MAX,
				);
				self.seek_internal(&seek_key.encode())?;

				if !self.is_valid() {
					self.position_to_absolute_last()?;
				}

				// For excluded: if at or past bound, back up
				if self.is_valid() {
					let cmp = self
						.table
						.opts
						.comparator
						.compare(self.current_user_key(), bound_user_key.as_slice());
					if cmp != Ordering::Less {
						self.prev_internal()?;
					}
				}
			}
		}

		// Verify within lower bound
		if self.is_valid() && !self.satisfies_lower_bound(self.current_user_key()) {
			self.mark_exhausted();
		}
		Ok(())
	}

	/// Internal seek to target key
	fn seek_internal(&mut self, target: &[u8]) -> Result<()> {
		self.first_level.seek(target)?;
		self.init_data_block()?;

		if let Some(ref mut iter) = self.second_level {
			iter.seek_internal(target)?;
		}

		self.skip_empty_data_blocks_forward()?;
		Ok(())
	}

	/// Position to absolute last entry (for SeekForPrev fallback)
	fn position_to_absolute_last(&mut self) -> Result<()> {
		self.first_level.seek_to_last()?;
		self.init_data_block()?;

		if let Some(ref mut iter) = self.second_level {
			iter.seek_to_last()?;
		}

		self.skip_empty_data_blocks_backward()?;
		Ok(())
	}

	/// Advance to next entry (internal, no bounds check)
	fn advance_internal(&mut self) -> Result<bool> {
		if let Some(ref mut iter) = self.second_level {
			if iter.advance()? {
				return Ok(true);
			}
		}
		self.skip_empty_data_blocks_forward()?;
		Ok(self.is_valid())
	}

	/// Move to previous entry (internal, no bounds check)
	fn prev_internal(&mut self) -> Result<bool> {
		if let Some(ref mut iter) = self.second_level {
			if iter.prev_internal()? {
				return Ok(true);
			}
		}
		self.skip_empty_data_blocks_backward()?;
		Ok(self.is_valid())
	}
}

impl InternalIterator for TwoLevelIterator<'_> {
	/// Seek to first entry >= target within bounds
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.exhausted = false;
		self.seek_internal(target)?;

		// Check upper bound
		if self.is_valid() && !self.satisfies_upper_bound(self.current_user_key()) {
			self.mark_exhausted();
		}
		Ok(self.is_valid())
	}

	/// Seek to first entry within bounds
	fn seek_first(&mut self) -> Result<bool> {
		self.seek_to_first()?;
		Ok(self.is_valid())
	}

	/// Seek to last entry within bounds
	fn seek_last(&mut self) -> Result<bool> {
		self.seek_to_last()?;
		Ok(self.is_valid())
	}

	/// Move to next entry within bounds
	fn next(&mut self) -> Result<bool> {
		// If not positioned yet and not exhausted, seek to first entry
		// This matches the old TableIterator behavior where next() on a fresh
		// iterator positions on the first item
		if !self.is_valid() && !self.exhausted {
			return self.seek_first();
		}

		if !self.is_valid() {
			return Ok(false);
		}

		self.advance_internal()?;

		// Check upper bound or mark exhausted if no more entries
		if !self.is_valid() {
			self.mark_exhausted();
		} else if !self.satisfies_upper_bound(self.current_user_key()) {
			self.mark_exhausted();
		}
		Ok(self.is_valid())
	}

	/// Move to previous entry within bounds
	fn prev(&mut self) -> Result<bool> {
		// If not positioned yet and not exhausted, seek to last entry
		// This matches the old TableIterator behavior
		if !self.is_valid() && !self.exhausted {
			return self.seek_last();
		}

		if !self.is_valid() {
			return Ok(false);
		}

		self.prev_internal()?;

		// Check lower bound or mark exhausted if no more entries
		if !self.is_valid() {
			self.mark_exhausted();
		} else if !self.satisfies_lower_bound(self.current_user_key()) {
			self.mark_exhausted();
		}
		Ok(self.is_valid())
	}

	fn valid(&self) -> bool {
		self.is_valid()
	}

	fn key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.valid());
		InternalKeyRef::from_encoded(self.second_level.as_ref().unwrap().key_bytes())
	}

	fn value(&self) -> &[u8] {
		debug_assert!(self.valid());
		self.second_level.as_ref().unwrap().value_bytes()
	}
}
