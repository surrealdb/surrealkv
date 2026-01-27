use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use crate::clock::LogicalClock;
use crate::error::Result;
use crate::vlog::{VLog, ValueLocation, ValuePointer};
use crate::{Comparator, InternalIterator, InternalKey, InternalKeyRef, Value};

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

// ============================================================================
// Proptest-based Property Tests
// ============================================================================

#[cfg(test)]
mod proptest_tests {
	use super::*;
	use proptest::prelude::*;
	use std::cmp::Ordering;

	// ========================================================================
	// BinaryHeap Property Tests
	// ========================================================================

	/// Verify the min-heap invariant: parent <= children for all nodes
	fn verify_heap_invariant<T, F: Fn(&T, &T) -> Ordering>(heap: &BinaryHeap<T, F>) -> bool {
		let data = &heap.data;
		for i in 0..data.len() {
			let left = 2 * i + 1;
			let right = 2 * i + 2;

			if left < data.len() && (heap.cmp)(&data[i], &data[left]) == Ordering::Greater {
				return false;
			}
			if right < data.len() && (heap.cmp)(&data[i], &data[right]) == Ordering::Greater {
				return false;
			}
		}
		true
	}

	proptest! {
		/// Property: After any sequence of pushes, the heap invariant holds
		#[test]
		fn prop_heap_invariant_after_pushes(values in prop::collection::vec(0i32..1000, 0..100)) {
			let mut heap = BinaryHeap::with_capacity(values.len(), |a: &i32, b: &i32| a.cmp(b));

			for v in values {
				heap.push(v);
				prop_assert!(verify_heap_invariant(&heap), "Heap invariant violated after push");
			}
		}

		/// Property: peek() always returns the minimum element
		#[test]
		fn prop_peek_returns_minimum(values in prop::collection::vec(0i32..1000, 1..100)) {
			let mut heap = BinaryHeap::with_capacity(values.len(), |a: &i32, b: &i32| a.cmp(b));

			for v in &values {
				heap.push(*v);
			}

			let expected_min = values.iter().min().unwrap();
			prop_assert_eq!(heap.peek(), Some(expected_min));
		}

		/// Property: pop() returns elements in ascending order (sorted extraction)
		#[test]
		fn prop_pop_returns_sorted(values in prop::collection::vec(0i32..1000, 0..100)) {
			let mut heap = BinaryHeap::with_capacity(values.len(), |a: &i32, b: &i32| a.cmp(b));

			for v in &values {
				heap.push(*v);
			}

			let mut sorted = values.clone();
			sorted.sort();

			let mut extracted = Vec::new();
			while let Some(v) = heap.pop() {
				extracted.push(v);
				prop_assert!(verify_heap_invariant(&heap), "Heap invariant violated after pop");
			}

			prop_assert_eq!(extracted, sorted);
		}

		/// Property: len() correctly tracks the number of elements
		#[test]
		fn prop_len_tracks_correctly(
			pushes in prop::collection::vec(0i32..1000, 0..50),
			pop_count in 0usize..30
		) {
			let mut heap = BinaryHeap::with_capacity(pushes.len(), |a: &i32, b: &i32| a.cmp(b));

			for v in &pushes {
				heap.push(*v);
			}

			prop_assert_eq!(heap.len(), pushes.len());

			let actual_pops = pop_count.min(pushes.len());
			for _ in 0..actual_pops {
				heap.pop();
			}

			prop_assert_eq!(heap.len(), pushes.len() - actual_pops);
		}

		/// Property: After modifying root via peek_mut and calling sift_down_root,
		/// the heap invariant is restored
		#[test]
		fn prop_sift_down_root_restores_invariant(
			values in prop::collection::vec(0i32..1000, 2..50),
			new_root in 0i32..2000
		) {
			let mut heap = BinaryHeap::with_capacity(values.len(), |a: &i32, b: &i32| a.cmp(b));

			for v in &values {
				heap.push(*v);
			}

			if let Some(root) = heap.peek_mut() {
				*root = new_root;
			}
			heap.sift_down_root();

			prop_assert!(verify_heap_invariant(&heap), "Heap invariant violated after sift_down_root");
		}
	}

	// ========================================================================
	// MergingIterator Property Tests
	// ========================================================================

	/// A simple mock iterator for testing MergingIterator
	struct PropMockIterator {
		items: Vec<(Vec<u8>, Vec<u8>)>, // (encoded_key, value)
		index: Option<usize>,
	}

	impl PropMockIterator {
		fn new(mut items: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
			// Sort by encoded key to simulate a proper iterator
			items.sort_by(|a, b| a.0.cmp(&b.0));
			Self { items, index: None }
		}
	}

	impl InternalIterator for PropMockIterator {
		fn seek(&mut self, target: &[u8]) -> Result<bool> {
			for i in 0..self.items.len() {
				if self.items[i].0.as_slice() >= target {
					self.index = Some(i);
					return Ok(true);
				}
			}
			self.index = None;
			Ok(false)
		}

		fn seek_first(&mut self) -> Result<bool> {
			if self.items.is_empty() {
				self.index = None;
				Ok(false)
			} else {
				self.index = Some(0);
				Ok(true)
			}
		}

		fn seek_last(&mut self) -> Result<bool> {
			if self.items.is_empty() {
				self.index = None;
				Ok(false)
			} else {
				self.index = Some(self.items.len() - 1);
				Ok(true)
			}
		}

		fn next(&mut self) -> Result<bool> {
			match self.index {
				Some(i) if i + 1 < self.items.len() => {
					self.index = Some(i + 1);
					Ok(true)
				}
				_ => {
					self.index = None;
					Ok(false)
				}
			}
		}

		fn prev(&mut self) -> Result<bool> {
			match self.index {
				Some(i) if i > 0 => {
					self.index = Some(i - 1);
					Ok(true)
				}
				_ => {
					self.index = None;
					Ok(false)
				}
			}
		}

		fn valid(&self) -> bool {
			self.index.is_some()
		}

		fn key(&self) -> InternalKeyRef<'_> {
			debug_assert!(self.valid());
			InternalKeyRef::from_encoded(&self.items[self.index.unwrap()].0)
		}

		fn value(&self) -> &[u8] {
			debug_assert!(self.valid());
			&self.items[self.index.unwrap()].1
		}
	}

	/// Generate a random encoded internal key
	fn gen_encoded_key(user_key: &[u8], seq: u64) -> Vec<u8> {
		let key = InternalKey::new(user_key.to_vec(), seq, crate::InternalKeyKind::Set, 0);
		key.encode()
	}

	/// Simple comparator for testing
	#[derive(Clone)]
	struct TestComparator;

	impl Comparator for TestComparator {
		fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
			a.cmp(b)
		}

		fn separator(&self, from: &[u8], _to: &[u8]) -> Vec<u8> {
			from.to_vec()
		}

		fn successor(&self, key: &[u8]) -> Vec<u8> {
			let mut result = key.to_vec();
			result.push(0);
			result
		}

		fn name(&self) -> &str {
			"TestComparator"
		}
	}

	proptest! {
		/// Property: MergingIterator outputs keys in sorted order
		#[test]
		fn prop_merging_iter_sorted_output(
			// Generate 1-5 iterators, each with 0-20 keys
			iter_data in prop::collection::vec(
				prop::collection::vec(
					(prop::collection::vec(prop::num::u8::ANY, 1..10), 1u64..1000),
					0..20
				),
				1..5
			)
		) {
			let cmp: Arc<dyn Comparator> = Arc::new(TestComparator);

			// Build mock iterators
			let iterators: Vec<BoxedInternalIterator<'_>> = iter_data
				.iter()
				.map(|items| {
					let encoded_items: Vec<(Vec<u8>, Vec<u8>)> = items
						.iter()
						.map(|(user_key, seq)| {
							(gen_encoded_key(user_key, *seq), b"value".to_vec())
						})
						.collect();
					Box::new(PropMockIterator::new(encoded_items)) as BoxedInternalIterator<'_>
				})
				.collect();

			let mut merge_iter = MergingIterator::new(iterators, Arc::clone(&cmp));

			// Collect all keys
			let mut keys: Vec<Vec<u8>> = Vec::new();
			if merge_iter.seek_first().unwrap() {
				keys.push(merge_iter.current_key().encoded().to_vec());
				while merge_iter.next().unwrap() {
					keys.push(merge_iter.current_key().encoded().to_vec());
				}
			}

			// Verify sorted order
			for i in 1..keys.len() {
				prop_assert!(
					keys[i - 1] <= keys[i],
					"Keys not in sorted order at index {}: {:?} > {:?}",
					i,
					keys[i - 1],
					keys[i]
				);
			}
		}

		/// Property: MergingIterator contains all keys from input iterators
		#[test]
		fn prop_merging_iter_completeness(
			iter_data in prop::collection::vec(
				prop::collection::vec(
					(prop::collection::vec(prop::num::u8::ANY, 1..5), 1u64..100),
					0..10
				),
				1..4
			)
		) {
			let cmp: Arc<dyn Comparator> = Arc::new(TestComparator);

			// Collect all expected keys
			let mut all_expected: Vec<Vec<u8>> = iter_data
				.iter()
				.flat_map(|items| {
					items.iter().map(|(user_key, seq)| gen_encoded_key(user_key, *seq))
				})
				.collect();
			all_expected.sort();
			all_expected.dedup();

			// Build mock iterators
			let iterators: Vec<BoxedInternalIterator<'_>> = iter_data
				.iter()
				.map(|items| {
					let encoded_items: Vec<(Vec<u8>, Vec<u8>)> = items
						.iter()
						.map(|(user_key, seq)| {
							(gen_encoded_key(user_key, *seq), b"value".to_vec())
						})
						.collect();
					Box::new(PropMockIterator::new(encoded_items)) as BoxedInternalIterator<'_>
				})
				.collect();

			let mut merge_iter = MergingIterator::new(iterators, Arc::clone(&cmp));

			// Collect merged keys
			let mut merged_keys: Vec<Vec<u8>> = Vec::new();
			if merge_iter.seek_first().unwrap() {
				merged_keys.push(merge_iter.current_key().encoded().to_vec());
				while merge_iter.next().unwrap() {
					merged_keys.push(merge_iter.current_key().encoded().to_vec());
				}
			}

			prop_assert_eq!(
				merged_keys.len(),
				all_expected.len(),
				"Merged output has different count than expected"
			);
		}

		/// Property: seek(target) positions on first key >= target
		#[test]
		fn prop_merging_iter_seek_correctness(
			keys in prop::collection::vec(
				prop::collection::vec(prop::num::u8::ANY, 1..5),
				1..20
			),
			target in prop::collection::vec(prop::num::u8::ANY, 0..5)
		) {
			let cmp: Arc<dyn Comparator> = Arc::new(TestComparator);

			// Build encoded keys
			let encoded_items: Vec<(Vec<u8>, Vec<u8>)> = keys
				.iter()
				.enumerate()
				.map(|(i, user_key)| {
					(gen_encoded_key(user_key, i as u64 + 1), b"value".to_vec())
				})
				.collect();

			let encoded_target = gen_encoded_key(&target, 0);

			let iterators: Vec<BoxedInternalIterator<'_>> = vec![
				Box::new(PropMockIterator::new(encoded_items.clone())) as BoxedInternalIterator<'_>
			];

			let mut merge_iter = MergingIterator::new(iterators, Arc::clone(&cmp));

			let found = merge_iter.seek(&encoded_target).unwrap();

			if found {
				let key = merge_iter.current_key().encoded().to_vec();
				prop_assert!(
					key >= encoded_target,
					"seek() positioned on key < target"
				);

				// Verify it's the first key >= target
				let mut all_keys: Vec<Vec<u8>> = encoded_items.iter().map(|(k, _)| k.clone()).collect();
				all_keys.sort();
				let expected_first = all_keys.iter().find(|k| **k >= encoded_target);
				prop_assert_eq!(Some(&key), expected_first);
			} else {
				// No key >= target exists
				let all_keys: Vec<Vec<u8>> = encoded_items.iter().map(|(k, _)| k.clone()).collect();
				let has_ge = all_keys.iter().any(|k| *k >= encoded_target);
				prop_assert!(!has_ge, "seek() returned false but a key >= target exists");
			}
		}
	}

	// ========================================================================
	// CompactionIterator Property Tests
	// ========================================================================

	use crate::clock::MockLogicalClock;
	use crate::comparator::{BytewiseComparator, InternalKeyComparator};

	/// Generate test entries for CompactionIterator testing
	fn create_test_entry(user_key: &[u8], seq: u64, is_delete: bool) -> (InternalKey, Vec<u8>) {
		let kind = if is_delete {
			crate::InternalKeyKind::Delete
		} else {
			crate::InternalKeyKind::Set
		};
		let key = InternalKey::new(user_key.to_vec(), seq, kind, 0);
		let value = if is_delete {
			Vec::new()
		} else {
			format!("value-{seq}").into_bytes()
		};
		(key, value)
	}

	/// MockIterator that works with InternalKey entries
	struct CompactionMockIterator {
		items: Vec<(InternalKey, Vec<u8>)>,
		encoded_keys: Vec<Vec<u8>>,
		index: Option<usize>,
	}

	impl CompactionMockIterator {
		fn new(mut items: Vec<(InternalKey, Vec<u8>)>) -> Self {
			// Sort by encoded key
			items.sort_by(|a, b| a.0.encode().cmp(&b.0.encode()));
			let encoded_keys: Vec<Vec<u8>> = items.iter().map(|(k, _)| k.encode()).collect();
			Self {
				items,
				encoded_keys,
				index: None,
			}
		}
	}

	impl InternalIterator for CompactionMockIterator {
		fn seek(&mut self, target: &[u8]) -> Result<bool> {
			for i in 0..self.encoded_keys.len() {
				if self.encoded_keys[i].as_slice() >= target {
					self.index = Some(i);
					return Ok(true);
				}
			}
			self.index = None;
			Ok(false)
		}

		fn seek_first(&mut self) -> Result<bool> {
			if self.items.is_empty() {
				self.index = None;
				Ok(false)
			} else {
				self.index = Some(0);
				Ok(true)
			}
		}

		fn seek_last(&mut self) -> Result<bool> {
			if self.items.is_empty() {
				self.index = None;
				Ok(false)
			} else {
				self.index = Some(self.items.len() - 1);
				Ok(true)
			}
		}

		fn next(&mut self) -> Result<bool> {
			match self.index {
				Some(i) if i + 1 < self.items.len() => {
					self.index = Some(i + 1);
					Ok(true)
				}
				_ => {
					self.index = None;
					Ok(false)
				}
			}
		}

		fn prev(&mut self) -> Result<bool> {
			match self.index {
				Some(i) if i > 0 => {
					self.index = Some(i - 1);
					Ok(true)
				}
				_ => {
					self.index = None;
					Ok(false)
				}
			}
		}

		fn valid(&self) -> bool {
			self.index.is_some()
		}

		fn key(&self) -> InternalKeyRef<'_> {
			debug_assert!(self.valid());
			InternalKeyRef::from_encoded(&self.encoded_keys[self.index.unwrap()])
		}

		fn value(&self) -> &[u8] {
			debug_assert!(self.valid());
			&self.items[self.index.unwrap()].1
		}
	}

	fn create_test_comparator() -> Arc<InternalKeyComparator> {
		Arc::new(InternalKeyComparator::new(Arc::new(BytewiseComparator::default())))
	}

	proptest! {
		/// Property: CompactionIterator output is sorted by user key
		#[test]
		fn prop_compaction_iter_sorted_output(
			// Generate entries: (user_key_suffix, seq_num, is_delete)
			entries in prop::collection::vec(
				(0u8..10, 1u64..100, prop::bool::ANY),
				1..30
			)
		) {
			let items: Vec<(InternalKey, Vec<u8>)> = entries
				.iter()
				.map(|(suffix, seq, is_delete)| {
					let user_key = format!("key-{suffix:02}").into_bytes();
					create_test_entry(&user_key, *seq, *is_delete)
				})
				.collect();

			let iter = Box::new(CompactionMockIterator::new(items)) as BoxedInternalIterator<'_>;

			let mut comp_iter = CompactionIterator::new(
				vec![iter],
				create_test_comparator(),
				false,
				None,
				false,
				0,
				Arc::new(MockLogicalClock::new()),
			);

			let mut prev_user_key: Option<Vec<u8>> = None;
			for result in comp_iter.by_ref() {
				let (key, _) = result.unwrap();
				if let Some(prev) = &prev_user_key {
					prop_assert!(
						*prev <= key.user_key,
						"Output not sorted: {:?} > {:?}",
						prev,
						key.user_key
					);
				}
				prev_user_key = Some(key.user_key);
			}
		}

		/// Property: CompactionIterator deduplicates by user key (no duplicate user keys)
		#[test]
		fn prop_compaction_iter_deduplication(
			// Generate entries with potential duplicates
			entries in prop::collection::vec(
				(0u8..5, 1u64..50, prop::bool::ANY),
				1..40
			)
		) {
			let items: Vec<(InternalKey, Vec<u8>)> = entries
				.iter()
				.map(|(suffix, seq, is_delete)| {
					let user_key = format!("key-{suffix:02}").into_bytes();
					create_test_entry(&user_key, *seq, *is_delete)
				})
				.collect();

			let iter = Box::new(CompactionMockIterator::new(items)) as BoxedInternalIterator<'_>;

			let mut comp_iter = CompactionIterator::new(
				vec![iter],
				create_test_comparator(),
				false, // non-bottom level
				None,
				false, // no versioning
				0,
				Arc::new(MockLogicalClock::new()),
			);

			let mut seen_user_keys: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
			for result in comp_iter.by_ref() {
				let (key, _) = result.unwrap();
				prop_assert!(
					seen_user_keys.insert(key.user_key.clone()),
					"Duplicate user key in output: {:?}",
					key.user_key
				);
			}
		}

		/// Property: CompactionIterator returns latest version (highest seq) for each user key
		#[test]
		fn prop_compaction_iter_latest_wins(
			// Generate entries with same user key but different seq nums
			seqs in prop::collection::vec(1u64..1000, 2..10)
		) {
			let user_key = b"test_key".to_vec();
			let items: Vec<(InternalKey, Vec<u8>)> = seqs
				.iter()
				.map(|seq| create_test_entry(&user_key, *seq, false))
				.collect();

			let expected_max_seq = *seqs.iter().max().unwrap();

			let iter = Box::new(CompactionMockIterator::new(items)) as BoxedInternalIterator<'_>;

			let mut comp_iter = CompactionIterator::new(
				vec![iter],
				create_test_comparator(),
				false,
				None,
				false,
				0,
				Arc::new(MockLogicalClock::new()),
			);

			let results: Vec<_> = comp_iter.by_ref().map(|r| r.unwrap()).collect();

			prop_assert_eq!(results.len(), 1, "Should have exactly one output for one user key");
			prop_assert_eq!(
				results[0].0.seq_num(),
				expected_max_seq,
				"Should return the entry with highest sequence number"
			);
		}

		/// Property: At bottom level, delete markers are not emitted
		#[test]
		fn prop_compaction_iter_bottom_level_deletes(
			// Generate mix of sets and deletes
			entries in prop::collection::vec(
				(0u8..5, 1u64..100),
				1..20
			)
		) {
			// Create entries where latest is always a delete
			let items: Vec<(InternalKey, Vec<u8>)> = entries
				.iter()
				.flat_map(|(suffix, seq)| {
					let user_key = format!("key-{suffix:02}").into_bytes();
					vec![
						create_test_entry(&user_key, *seq, false),      // SET
						create_test_entry(&user_key, *seq + 1000, true), // DELETE (higher seq)
					]
				})
				.collect();

			let iter = Box::new(CompactionMockIterator::new(items)) as BoxedInternalIterator<'_>;

			let mut comp_iter = CompactionIterator::new(
				vec![iter],
				create_test_comparator(),
				true, // BOTTOM LEVEL
				None,
				false,
				0,
				Arc::new(MockLogicalClock::new()),
			);

			let results: Vec<_> = comp_iter.by_ref().map(|r| r.unwrap()).collect();

			// At bottom level with delete as latest, all entries should be filtered out
			prop_assert_eq!(
				results.len(),
				0,
				"At bottom level, keys with delete as latest should be dropped"
			);
		}

		/// Property: At non-bottom level, delete markers ARE emitted
		#[test]
		fn prop_compaction_iter_non_bottom_preserves_deletes(
			user_key_count in 1usize..5
		) {
			// Create one delete entry per user key
			let items: Vec<(InternalKey, Vec<u8>)> = (0..user_key_count)
				.map(|i| {
					let user_key = format!("key-{i:02}").into_bytes();
					create_test_entry(&user_key, 100, true) // DELETE
				})
				.collect();

			let iter = Box::new(CompactionMockIterator::new(items)) as BoxedInternalIterator<'_>;

			let mut comp_iter = CompactionIterator::new(
				vec![iter],
				create_test_comparator(),
				false, // NON-BOTTOM LEVEL
				None,
				false,
				0,
				Arc::new(MockLogicalClock::new()),
			);

			let results: Vec<_> = comp_iter.by_ref().map(|r| r.unwrap()).collect();

			prop_assert_eq!(
				results.len(),
				user_key_count,
				"At non-bottom level, delete markers should be preserved"
			);

			for (key, _) in &results {
				prop_assert!(
					key.is_hard_delete_marker(),
					"Expected delete marker in output"
				);
			}
		}
	}
}
