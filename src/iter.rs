use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use crate::clock::LogicalClock;
use crate::error::Result;
use crate::vlog::{VLog, ValueLocation, ValuePointer};
use crate::{Comparator, InternalIterator, InternalKey, InternalKeyRef, Value};

/// Boxed internal iterator type for dynamic dispatch
pub type BoxedInternalIterator<'a> = Box<dyn InternalIterator + 'a>;

/// Binary heap
struct BinaryHeap<T, F> {
	data: Vec<T>,
	cmp: F,
}

impl<T, F: Fn(&T, &T) -> Ordering> BinaryHeap<T, F> {
	fn with_capacity(capacity: usize, cmp: F) -> Self {
		Self {
			data: Vec::with_capacity(capacity),
			cmp,
		}
	}

	#[inline]
	fn len(&self) -> usize {
		self.data.len()
	}

	#[inline]
	fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	/// Push an item onto the heap.
	fn push(&mut self, item: T) {
		self.data.push(item);
		self.sift_up(self.data.len() - 1);
	}

	/// Remove and return the top (highest priority) item.
	fn pop(&mut self) -> Option<T> {
		if self.data.is_empty() {
			return None;
		}
		let len = self.data.len();
		if len == 1 {
			return self.data.pop();
		}
		self.data.swap(0, len - 1);
		let result = self.data.pop();
		if !self.data.is_empty() {
			self.sift_down(0);
		}
		result
	}

	/// Get a reference to the top item without removing it. O(1)
	#[inline]
	fn peek(&self) -> Option<&T> {
		self.data.first()
	}

	/// Get a mutable reference to the top item. O(1)
	/// After modification, call `sift_down_root()` to restore heap property.
	#[inline]
	fn peek_mut(&mut self) -> Option<&mut T> {
		self.data.first_mut()
	}

	/// Restore heap property after modifying the root via peek_mut().
	fn sift_down_root(&mut self) {
		if !self.data.is_empty() {
			self.sift_down(0);
		}
	}

	/// Drain all items from the heap, returning them as a Vec.
	fn drain(&mut self) -> Vec<T> {
		std::mem::take(&mut self.data)
	}

	#[inline]
	fn sift_up(&mut self, mut pos: usize) {
		while pos > 0 {
			let parent = (pos - 1) / 2;
			if (self.cmp)(&self.data[pos], &self.data[parent]) == Ordering::Less {
				self.data.swap(pos, parent);
				pos = parent;
			} else {
				break;
			}
		}
	}

	#[inline]
	fn sift_down(&mut self, mut pos: usize) {
		let len = self.data.len();
		loop {
			let left = 2 * pos + 1;
			let right = 2 * pos + 2;

			// Find the smallest among pos, left, right
			let mut smallest = pos;

			if left < len && (self.cmp)(&self.data[left], &self.data[smallest]) == Ordering::Less {
				smallest = left;
			}
			if right < len && (self.cmp)(&self.data[right], &self.data[smallest]) == Ordering::Less
			{
				smallest = right;
			}

			if smallest != pos {
				self.data.swap(pos, smallest);
				pos = smallest;
			} else {
				break;
			}
		}
	}
}

// ============================================================================
// MergingIterator - k-way merge using binary heap (RocksDB-style)
// ============================================================================

/// Direction of iteration
#[derive(Clone, Copy, PartialEq)]
enum Direction {
	Forward,
	Backward,
}

/// Entry in the merge heap - wraps an iterator
struct HeapEntry<'a> {
	iter: BoxedInternalIterator<'a>,
	/// Original index for stable ordering on equal keys
	level_idx: usize,
}

/// K-way merge iterator using binary heaps for operations.
///
/// Uses dual heaps for bidirectional iteration:
/// - min_heap for forward iteration (smallest key wins)
/// - max_heap for backward iteration (largest key wins)
pub(crate) struct MergingIterator<'a> {
	/// Entries for forward iteration (min-heap)
	min_heap: BinaryHeap<HeapEntry<'a>, Box<dyn Fn(&HeapEntry<'a>, &HeapEntry<'a>) -> Ordering>>,
	/// Entries for backward iteration (max-heap), lazily initialized
	max_heap:
		Option<BinaryHeap<HeapEntry<'a>, Box<dyn Fn(&HeapEntry<'a>, &HeapEntry<'a>) -> Ordering>>>,
	/// Exhausted iterators stored here when not in current heap
	exhausted: Vec<HeapEntry<'a>>,
	/// Current direction
	direction: Direction,
	/// Comparator for keys
	cmp: Arc<dyn Comparator>,
}

impl<'a> MergingIterator<'a> {
	pub fn new(iterators: Vec<BoxedInternalIterator<'a>>, cmp: Arc<dyn Comparator>) -> Self {
		let capacity = iterators.len();

		// Create min-heap comparator (smaller key = higher priority)
		let cmp_clone = Arc::clone(&cmp);
		let min_cmp: Box<dyn Fn(&HeapEntry<'a>, &HeapEntry<'a>) -> Ordering> =
			Box::new(move |a: &HeapEntry, b: &HeapEntry| {
				let key_a = a.iter.key().encoded();
				let key_b = b.iter.key().encoded();
				match cmp_clone.compare(key_a, key_b) {
					Ordering::Equal => a.level_idx.cmp(&b.level_idx), // Stable sort by level
					ord => ord,
				}
			});

		let min_heap = BinaryHeap::with_capacity(capacity, min_cmp);

		// Store iterators as exhausted initially (they need seek_first/seek_last)
		let exhausted: Vec<_> = iterators
			.into_iter()
			.enumerate()
			.map(|(idx, iter)| HeapEntry {
				iter,
				level_idx: idx,
			})
			.collect();

		Self {
			min_heap,
			max_heap: None,
			exhausted,
			direction: Direction::Forward,
			cmp,
		}
	}

	/// Create max-heap comparator (larger key = higher priority)
	fn create_max_heap(
		&self,
		capacity: usize,
	) -> BinaryHeap<HeapEntry<'a>, Box<dyn Fn(&HeapEntry<'a>, &HeapEntry<'a>) -> Ordering>> {
		let cmp_clone = Arc::clone(&self.cmp);
		let max_cmp: Box<dyn Fn(&HeapEntry<'a>, &HeapEntry<'a>) -> Ordering> =
			Box::new(move |a: &HeapEntry, b: &HeapEntry| {
				let key_a = a.iter.key().encoded();
				let key_b = b.iter.key().encoded();
				match cmp_clone.compare(key_a, key_b) {
					Ordering::Equal => b.level_idx.cmp(&a.level_idx), /* Reverse for max-heap
					                                                    * stability */
					ord => ord.reverse(), // Reverse for max-heap
				}
			});
		BinaryHeap::with_capacity(capacity, max_cmp)
	}

	/// Initialize for forward iteration
	fn init_forward(&mut self) -> Result<()> {
		self.direction = Direction::Forward;

		// Collect all entries from min_heap, max_heap, and exhausted
		let mut all_entries: Vec<HeapEntry<'a>> = self.min_heap.drain();
		if let Some(ref mut max_heap) = self.max_heap {
			all_entries.append(&mut max_heap.drain());
		}
		all_entries.append(&mut self.exhausted);

		// Position all iterators at first and push valid ones to min_heap
		for mut entry in all_entries {
			if entry.iter.seek_first()? {
				self.min_heap.push(entry);
			} else {
				self.exhausted.push(entry);
			}
		}

		Ok(())
	}

	/// Initialize for backward iteration
	fn init_backward(&mut self) -> Result<()> {
		self.direction = Direction::Backward;

		// Ensure max_heap exists
		let capacity = self.min_heap.len() + self.exhausted.len();
		if self.max_heap.is_none() {
			self.max_heap = Some(self.create_max_heap(capacity));
		}
		let max_heap = self.max_heap.as_mut().unwrap();

		// Collect all entries
		let mut all_entries: Vec<HeapEntry<'a>> = self.min_heap.drain();
		all_entries.append(&mut max_heap.drain());
		all_entries.append(&mut self.exhausted);

		// Position all iterators at last and push valid ones to max_heap
		for mut entry in all_entries {
			if entry.iter.seek_last()? {
				max_heap.push(entry);
			} else {
				self.exhausted.push(entry);
			}
		}

		Ok(())
	}

	/// Get the current heap based on direction
	#[inline]
	fn current_heap(
		&self,
	) -> &BinaryHeap<HeapEntry<'a>, Box<dyn Fn(&HeapEntry<'a>, &HeapEntry<'a>) -> Ordering>> {
		match self.direction {
			Direction::Forward => &self.min_heap,
			Direction::Backward => self.max_heap.as_ref().unwrap(),
		}
	}

	/// Get the current heap mutably based on direction
	#[inline]
	fn current_heap_mut(
		&mut self,
	) -> &mut BinaryHeap<HeapEntry<'a>, Box<dyn Fn(&HeapEntry<'a>, &HeapEntry<'a>) -> Ordering>> {
		match self.direction {
			Direction::Forward => &mut self.min_heap,
			Direction::Backward => self.max_heap.as_mut().unwrap(),
		}
	}

	/// Advance the current
	fn advance_current(&mut self) -> Result<bool> {
		// Copy direction before borrowing heap mutably
		let direction = self.direction;
		let heap = self.current_heap_mut();
		if heap.is_empty() {
			return Ok(false);
		}

		// Get mutable access to the winner and advance it
		let winner = heap.peek_mut().unwrap();
		let still_valid = match direction {
			Direction::Forward => winner.iter.next()?,
			Direction::Backward => winner.iter.prev()?,
		};

		if still_valid {
			// Iterator still valid - sift down to restore heap property
			heap.sift_down_root();
		} else {
			// Iterator exhausted - remove from heap
			let entry = heap.pop().unwrap();
			self.exhausted.push(entry);
		}

		Ok(!self.current_heap().is_empty())
	}

	/// Check if iterator is valid
	#[inline]
	pub fn is_valid(&self) -> bool {
		!self.current_heap().is_empty()
	}

	/// Get current winner's key
	#[inline]
	pub fn current_key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.is_valid());
		self.current_heap().peek().unwrap().iter.key()
	}

	/// Get current winner's value
	#[inline]
	pub fn current_value(&self) -> &[u8] {
		debug_assert!(self.is_valid());
		self.current_heap().peek().unwrap().iter.value()
	}
}

impl InternalIterator for MergingIterator<'_> {
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.direction = Direction::Forward;

		// Collect all entries
		let mut all_entries: Vec<HeapEntry<'_>> = self.min_heap.drain();
		if let Some(ref mut max_heap) = self.max_heap {
			all_entries.append(&mut max_heap.drain());
		}
		all_entries.append(&mut self.exhausted);

		// Seek all iterators and push valid ones to min_heap
		for mut entry in all_entries {
			if entry.iter.seek(target)? {
				self.min_heap.push(entry);
			} else {
				self.exhausted.push(entry);
			}
		}

		Ok(self.is_valid())
	}

	fn seek_first(&mut self) -> Result<bool> {
		self.init_forward()?;
		Ok(self.is_valid())
	}

	fn seek_last(&mut self) -> Result<bool> {
		self.init_backward()?;
		Ok(self.is_valid())
	}

	fn next(&mut self) -> Result<bool> {
		if !self.is_valid() {
			return Ok(false);
		}
		// Switch direction if needed
		if self.direction != Direction::Forward {
			// Save current position, switch to forward, seek to current key
			let current_key = self.current_key().encoded().to_vec();
			self.init_forward()?;
			// Seek past entries <= current_key to avoid duplicates
			while self.is_valid()
				&& self.cmp.compare(self.current_key().encoded(), &current_key) != Ordering::Greater
			{
				if !self.advance_current()? {
					break;
				}
			}
			return Ok(self.is_valid());
		}
		self.advance_current()
	}

	fn prev(&mut self) -> Result<bool> {
		if !self.is_valid() {
			return Ok(false);
		}
		// Switch direction if needed
		if self.direction != Direction::Backward {
			// Save current position, switch to backward, seek to current key
			let current_key = self.current_key().encoded().to_vec();
			self.init_backward()?;
			// Seek past entries >= current_key to avoid duplicates
			while self.is_valid()
				&& self.cmp.compare(self.current_key().encoded(), &current_key) != Ordering::Less
			{
				if !self.advance_current()? {
					break;
				}
			}
			return Ok(self.is_valid());
		}
		self.advance_current()
	}

	fn valid(&self) -> bool {
		self.is_valid()
	}

	fn key(&self) -> InternalKeyRef<'_> {
		self.current_key()
	}

	fn value(&self) -> &[u8] {
		self.current_value()
	}
}

// ============================================================================
// CompactionIterator - Uses MergingIterator for compaction
// ============================================================================

fn collect_vlog_discard_stats(discard_stats: &mut HashMap<u32, i64>, value: &[u8]) -> Result<()> {
	// Skip empty values (e.g., hard delete entries)
	if value.is_empty() {
		return Ok(());
	}

	// Check if this is a ValueLocation
	let location = ValueLocation::decode(value)?;
	if location.is_value_pointer() {
		let pointer = ValuePointer::decode(&location.value)?;
		let value_data_size = pointer.total_entry_size() as i64;
		*discard_stats.entry(pointer.file_id).or_insert(0) += value_data_size;
	}
	Ok(())
}

pub(crate) struct CompactionIterator<'a> {
	merge_iter: MergingIterator<'a>,
	is_bottom_level: bool,

	// Track the current key being processed
	current_user_key: Vec<u8>,

	// Buffer for accumulating all versions of the current key
	accumulated_versions: Vec<(InternalKey, Value)>,

	// Buffer for outputting the versions of the current key
	output_versions: Vec<(InternalKey, Value)>,

	// Compaction state
	/// Collected discard statistics: file_id -> total_discarded_bytes
	pub discard_stats: HashMap<u32, i64>,

	/// Reference to VLog for populating delete-list
	vlog: Option<Arc<VLog>>,

	/// Batch of stale entries to add to delete-list: (sequence_number,
	/// value_size)
	pub(crate) delete_list_batch: Vec<(u64, u64)>,

	/// Versioning configuration
	enable_versioning: bool,
	retention_period_ns: u64,

	/// Logical clock for time-based operations
	clock: Arc<dyn LogicalClock>,

	initialized: bool,
}

impl<'a> CompactionIterator<'a> {
	pub(crate) fn new(
		iterators: Vec<BoxedInternalIterator<'a>>,
		cmp: Arc<dyn Comparator>,
		is_bottom_level: bool,
		vlog: Option<Arc<VLog>>,
		enable_versioning: bool,
		retention_period_ns: u64,
		clock: Arc<dyn LogicalClock>,
	) -> Self {
		let merge_iter = MergingIterator::new(iterators, cmp);

		Self {
			merge_iter,
			is_bottom_level,
			current_user_key: Vec::new(),
			accumulated_versions: Vec::new(),
			output_versions: Vec::new(),
			discard_stats: HashMap::new(),
			vlog,
			delete_list_batch: Vec::new(),
			enable_versioning,
			retention_period_ns,
			clock,
			initialized: false,
		}
	}

	fn initialize(&mut self) -> Result<()> {
		self.merge_iter.seek_first()?;
		self.initialized = true;
		Ok(())
	}

	/// Flushes the batched delete-list entries to the VLog
	pub(crate) fn flush_delete_list_batch(&mut self) -> Result<()> {
		if let Some(ref vlog) = self.vlog {
			if !self.delete_list_batch.is_empty() {
				vlog.add_batch_to_delete_list(std::mem::take(&mut self.delete_list_batch))?;
			}
		}
		Ok(())
	}

	/// Process all accumulated versions of the current key
	/// Filters out stale entries and populates output_versions with valid
	/// entries
	fn process_accumulated_versions(&mut self) {
		if self.accumulated_versions.is_empty() {
			return;
		}

		// Sort by sequence number (descending) to get the latest version first
		self.accumulated_versions.sort_by(|a, b| b.0.seq_num().cmp(&a.0.seq_num()));

		// Check if latest version is DELETE at bottom level - if so, discard entire key
		let latest_is_delete_at_bottom = self.is_bottom_level
			&& !self.accumulated_versions.is_empty()
			&& self.accumulated_versions[0].0.is_hard_delete_marker();

		// Check if any version is Replace - if so, mark all older versions as stale
		let has_set_with_delete = self.accumulated_versions.iter().any(|(key, _)| key.is_replace());

		// Process all versions for delete list and determine which to keep
		for (i, (key, value)) in self.accumulated_versions.iter().enumerate() {
			let is_hard_delete = key.is_hard_delete_marker();
			let is_replace = key.is_replace();
			let is_latest = i == 0;

			// Determine if this entry should be marked as stale in VLog
			let should_mark_stale = if latest_is_delete_at_bottom {
				// If latest version is DELETE at bottom level, mark ALL versions as stale
				true
			} else if is_latest && !is_hard_delete && !is_replace {
				// Latest version of a regular SET operation: never mark as stale (it's being
				// returned)
				false
			} else if is_latest && is_hard_delete && self.is_bottom_level {
				// Latest version of a DELETE operation at bottom level: mark as stale (not
				// returned)
				true
			} else if is_latest && is_hard_delete && !self.is_bottom_level {
				// Latest version of a DELETE operation at non-bottom level: don't mark as stale
				// (it's being returned)
				false
			} else if is_latest && is_replace {
				// Latest version of a Replace operation: don't mark as stale (it's being
				// returned)
				false
			} else if is_hard_delete {
				// For older DELETE operations (hard delete entries): always mark as stale since
				// they don't have VLog values
				true
			} else if has_set_with_delete && !is_replace {
				// If there's a Replace operation, mark all older non-Replace versions as stale
				true
			} else {
				// Older version of a SET operation: check retention period
				if !self.enable_versioning {
					// Versioning disabled: mark older versions as stale
					true
				} else if self.retention_period_ns > 0 {
					// Versioning enabled with retention period: check if within retention
					let current_time = self.clock.now();
					let key_timestamp = key.timestamp;
					let age = current_time - key_timestamp;
					let is_within_retention = age <= self.retention_period_ns;
					// Mark as stale only if NOT within retention period
					!is_within_retention
				} else {
					// Versioning enabled with retention_period_ns == 0: keep all versions forever
					false
				}
			};

			// Add to delete list and collect discard stats if needed
			if should_mark_stale && self.vlog.is_some() {
				if key.is_tombstone() {
					// Hard Delete: add key size to delete list
					self.delete_list_batch.push((key.seq_num(), key.size() as u64));
				} else {
					let location = ValueLocation::decode(value).unwrap();
					if location.is_value_pointer() {
						let pointer = ValuePointer::decode(&location.value).unwrap();
						let value_size = pointer.total_entry_size();
						self.delete_list_batch.push((key.seq_num(), value_size));
					}
				}

				// Collect discard statistics
				if let Err(e) = collect_vlog_discard_stats(&mut self.discard_stats, value) {
					log::warn!("Error collecting discard stats: {e:?}");
				}
			}

			// Determine if this version should be kept for output
			let should_output = if latest_is_delete_at_bottom {
				// If latest version is DELETE at bottom level, output NOTHING for this key
				false
			} else if should_mark_stale {
				// Stale entries: don't output
				false
			} else if self.enable_versioning {
				// Versioning enabled: output all non-stale versions
				true
			} else {
				// Versioning disabled: only output the latest version
				is_latest
			};

			if should_output {
				self.output_versions.push((key.clone(), value.clone()));
			}
		}

		// Clear accumulated versions for the next key
		self.accumulated_versions.clear();
	}

	/// Advance to the next output entry
	pub fn advance(&mut self) -> Result<Option<(InternalKey, Value)>> {
		if !self.initialized {
			self.initialize()?;
		}

		loop {
			// First, return any pending output versions
			if !self.output_versions.is_empty() {
				// Remove from front to maintain sequence number order (already sorted
				// descending)
				return Ok(Some(self.output_versions.remove(0)));
			}

			// Get the next item from the merge iterator
			if !self.merge_iter.is_valid() {
				// No more items, process any remaining accumulated versions
				if !self.accumulated_versions.is_empty() {
					self.process_accumulated_versions();
					// Return first output version if any
					if !self.output_versions.is_empty() {
						return Ok(Some(self.output_versions.remove(0)));
					}
				}
				return Ok(None);
			}

			// Get current key and value from merge iterator (extract to owned to avoid borrow
			// issues)
			let key_owned = self.merge_iter.current_key().to_owned();
			let user_key_owned = key_owned.user_key.clone();
			let value_owned = self.merge_iter.current_value().to_vec();

			// Check if this is a new user key
			let is_new_key =
				self.current_user_key.is_empty() || user_key_owned != self.current_user_key;

			if is_new_key {
				// Process accumulated versions of the previous key
				if !self.accumulated_versions.is_empty() {
					self.process_accumulated_versions();

					// Start accumulating the new key
					self.current_user_key = user_key_owned;
					self.accumulated_versions.push((key_owned, value_owned));

					// Advance merge iterator for next iteration
					self.merge_iter.next()?;

					// Return first output version from processed key if any
					if !self.output_versions.is_empty() {
						return Ok(Some(self.output_versions.remove(0)));
					}
				} else {
					// First key - start accumulating
					self.current_user_key = user_key_owned;
					self.accumulated_versions.push((key_owned, value_owned));

					// Advance merge iterator for next iteration
					self.merge_iter.next()?;
				}
			} else {
				// Same user key - add to accumulated versions
				self.accumulated_versions.push((key_owned, value_owned));

				// Advance merge iterator for next iteration
				self.merge_iter.next()?;
			}
		}
	}
}

impl Iterator for CompactionIterator<'_> {
	type Item = Result<(InternalKey, Value)>;

	fn next(&mut self) -> Option<Self::Item> {
		match self.advance() {
			Ok(Some(item)) => Some(Ok(item)),
			Ok(None) => None,
			Err(e) => Some(Err(e)),
		}
	}
}
