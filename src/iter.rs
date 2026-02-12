use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use crate::clock::LogicalClock;
use crate::error::{Error, Result};
use crate::vlog::{VLog, ValueLocation, ValuePointer};
use crate::{Comparator, InternalIterator, InternalKey, InternalKeyRef, Value};

// ============================================================================
// SNAPSHOT VISIBILITY
// ============================================================================

/// Represents the visibility state of a version with respect to active snapshots.
///
/// This enum is used during compaction to determine whether a version must be
/// preserved for snapshot isolation or can be garbage collected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SnapshotVisibility {
	/// Version is bounded by a specific snapshot boundary.
	///
	/// The contained value is the sequence number of the earliest snapshot
	/// that can see this version. This version is actually visible to ALL
	/// snapshots >= this value, but the earliest one defines the visibility
	/// boundary used for compaction grouping.
	///
	/// This version must be preserved unless a newer version exists in the
	/// same visibility boundary (in which case the newer version supersedes it).
	BoundedBySnapshot(u64),

	/// No active snapshots exist.
	///
	/// When there are no snapshots, visibility is determined solely by
	/// retention rules. Only the latest version needs to be kept (unless
	/// versioning is enabled).
	NoActiveSnapshots,

	/// Version has a sequence number higher than all active snapshots.
	///
	/// This version was written after all currently active snapshots were created,
	/// so it is not visible to any existing snapshot. It can be dropped
	/// if a newer version exists in the same visibility boundary.
	NewerThanAllSnapshots,
}

/// Boxed internal iterator type for dynamic dispatch.
///
/// This allows us to store different iterator types (MemTable iterators,
/// SSTable iterators, etc.) in the same collection.
pub type BoxedInternalIterator<'a> = Box<dyn InternalIterator + 'a>;

// ============================================================================
// BINARY HEAP
// ============================================================================
//
// A binary heap is a complete binary tree stored in a flat array.
//
// ## Array-to-Tree Mapping
//
// The tree structure is implicit in the array indices:
//
// ```
// Array:   [0, 2, 1, 3, 4]   (these are indices into children array)
// Heap idx: 0  1  2  3  4
//
// Tree visualization:
//
//              [0]           ← heap index 0 (root)
//             /   \
//          [2]     [1]       ← heap index 1, 2
//          / \
//       [3]  [4]             ← heap index 3, 4
// ```
//
// ## Index Formulas
//
// For any element at heap index `i`:
// - Parent index:      `(i - 1) / 2`  (integer division)
// - Left child index:  `2 * i + 1`
// - Right child index: `2 * i + 2`
//
// ## Heap Property
//
// The comparator returns `Ordering::Less` if the first argument should be
// closer to the root. This means:
// - Min-heap: smaller elements return Less, so smallest is at root
// - Max-heap: larger elements return Less (by reversing comparison), so largest is at root
//

struct BinaryHeap<T> {
	/// The underlying storage representing the tree level-by-level.
	data: Vec<T>,
}

impl<T: Copy> BinaryHeap<T> {
	fn with_capacity(capacity: usize) -> Self {
		Self {
			data: Vec::with_capacity(capacity),
		}
	}

	#[inline]
	fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	#[inline]
	fn peek(&self) -> Option<T> {
		self.data.first().copied()
	}

	fn clear(&mut self) {
		self.data.clear();
	}

	/// Push an item onto the heap.
	///
	/// 1. Add element at end of array
	/// 2. Sift up: swap with parent while heap property is violated
	fn push<F>(&mut self, item: T, cmp: F)
	where
		F: Fn(T, T) -> Ordering,
	{
		self.data.push(item);
		self.sift_up(self.data.len() - 1, cmp);
	}

	/// Remove and return the root (highest priority) item.
	///
	/// 1. Swap root with last element
	/// 2. Remove last element (the original root)
	/// 3. Sift down: restore heap property from root
	fn pop<F>(&mut self, cmp: F) -> Option<T>
	where
		F: Fn(T, T) -> Ordering,
	{
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
			self.sift_down(0, cmp);
		}
		result
	}

	/// Restore heap property after modifying the root element.
	///
	/// This is an optimization for the merging iterator: instead of pop + push
	/// (2 × O(log N)), we modify in place + sift_down (1 × O(log N)).
	fn sift_down_root<F>(&mut self, cmp: F)
	where
		F: Fn(T, T) -> Ordering,
	{
		if !self.data.is_empty() {
			self.sift_down(0, cmp);
		}
	}

	/// Move element at `pos` towards root while it has higher priority than parent.
	fn sift_up<F>(&mut self, mut pos: usize, cmp: F)
	where
		F: Fn(T, T) -> Ordering,
	{
		while pos > 0 {
			let parent = (pos - 1) / 2;
			if cmp(self.data[pos], self.data[parent]) == Ordering::Less {
				self.data.swap(pos, parent);
				pos = parent;
			} else {
				break;
			}
		}
	}

	/// Move element at `pos` towards leaves while children have higher priority.
	fn sift_down<F>(&mut self, mut pos: usize, cmp: F)
	where
		F: Fn(T, T) -> Ordering,
	{
		let len = self.data.len();
		loop {
			let left = 2 * pos + 1;
			let right = 2 * pos + 2;
			let mut smallest = pos;

			if left < len && cmp(self.data[left], self.data[smallest]) == Ordering::Less {
				smallest = left;
			}
			if right < len && cmp(self.data[right], self.data[smallest]) == Ordering::Less {
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
// MERGING ITERATOR
// ============================================================================
//
// K-way merge iterator using binary heaps. This is the core data structure for
// LSM-tree iteration, merging sorted runs from multiple levels.
//
// ```
// MergingIterator
// ├── children: Vec<HeapEntry>     ← Permanent storage for all child iterators
// ├── min_heap: BinaryHeap<usize>  ← Indices into children (forward iteration)
// ├── max_heap: BinaryHeap<usize>  ← Indices into children (backward iteration)
// └── direction: Forward|Backward
// ```
//
// ## Ordering and Tiebreaking
//
// When keys are equal, we use `level_idx` as tiebreaker:
// - Lower level_idx = higher priority (wins the comparison)
// - This ensures newer data (lower levels in LSM) shadows older data
//
// For min-heap (forward iteration):
// - Smaller keys have higher priority (appear first)
// - Equal keys: lower level_idx wins
//
// For max-heap (backward iteration):
// - Larger keys have higher priority (appear first)
// - Equal keys: lower level_idx wins (same tiebreaker, NOT reversed)
//
// ## Direction Switching
//
// When switching directions (e.g., forward to backward), we:
// 1. Clear the current heap
// 2. Seek all iterators to the target position
// 3. Rebuild the appropriate heap with valid iterators
#[derive(Clone, Copy, PartialEq)]
enum Direction {
	Forward,
	Backward,
}

/// Entry holding a child iterator and its level index for tiebreaking.
struct HeapEntry<'a> {
	iter: BoxedInternalIterator<'a>,
	/// Lower level_idx = newer data = higher priority when keys are equal.
	level_idx: usize,
}

pub(crate) struct MergingIterator<'a> {
	/// Permanent storage for all child iterators.
	/// Iterators stay here for the lifetime of the MergingIterator.
	children: Vec<HeapEntry<'a>>,

	/// Min-heap storing indices into `children` for forward iteration.
	/// The smallest key is at the root (index 0).
	min_heap: BinaryHeap<usize>,

	/// Max-heap storing indices into `children` for backward iteration.
	/// The largest key is at the root (index 0).
	/// Lazily initialized on first backward operation.
	max_heap: Option<BinaryHeap<usize>>,

	/// Current iteration direction.
	direction: Direction,

	/// User-provided key comparator.
	cmp: Arc<dyn Comparator>,
}

impl<'a> MergingIterator<'a> {
	pub fn new(iterators: Vec<BoxedInternalIterator<'a>>, cmp: Arc<dyn Comparator>) -> Self {
		let capacity = iterators.len();
		let children: Vec<_> = iterators
			.into_iter()
			.enumerate()
			.map(|(idx, iter)| HeapEntry {
				iter,
				level_idx: idx,
			})
			.collect();

		Self {
			children,
			min_heap: BinaryHeap::with_capacity(capacity),
			max_heap: None,
			direction: Direction::Forward,
			cmp,
		}
	}

	// -------------------------------------------------------------------------
	// Comparison Functions
	// -------------------------------------------------------------------------
	//
	/// Min-heap comparison: smaller key wins, then lower level_idx.
	///
	/// Returns `Ordering::Less` if `a` should be closer to root than `b`.
	/// This means: a.key < b.key, or (a.key == b.key && a.level_idx < b.level_idx)
	#[inline]
	fn cmp_min(children: &[HeapEntry<'_>], cmp: &dyn Comparator, a: usize, b: usize) -> Ordering {
		let key_a = children[a].iter.key().encoded();
		let key_b = children[b].iter.key().encoded();
		cmp.compare(key_a, key_b).then_with(|| children[a].level_idx.cmp(&children[b].level_idx))
	}

	/// Max-heap comparison: larger key wins, then lower level_idx.
	///
	/// Returns `Ordering::Less` if `a` should be closer to root than `b`.
	/// This means: a.key > b.key, or (a.key == b.key && a.level_idx < b.level_idx)
	///
	/// Note: The key comparison is reversed, but the level_idx tiebreaker is NOT.
	/// This ensures that when iterating backward, newer data (lower level_idx)
	/// still shadows older data with the same key.
	#[inline]
	fn cmp_max(children: &[HeapEntry<'_>], cmp: &dyn Comparator, a: usize, b: usize) -> Ordering {
		let key_a = children[a].iter.key().encoded();
		let key_b = children[b].iter.key().encoded();
		cmp.compare(key_a, key_b)
			.reverse()
			.then_with(|| children[a].level_idx.cmp(&children[b].level_idx))
	}

	/// Lazily initialize max_heap on first backward iteration.
	fn init_max_heap(&mut self) {
		if self.max_heap.is_none() {
			self.max_heap = Some(BinaryHeap::with_capacity(self.children.len()));
		}
	}

	/// Clear both heaps (used when switching directions or seeking).
	fn clear_heaps(&mut self) {
		self.min_heap.clear();
		if let Some(ref mut h) = self.max_heap {
			h.clear();
		}
	}

	/// Rebuild min_heap with all currently valid iterators.
	fn rebuild_min_heap(&mut self) {
		let children = &self.children;
		let cmp = self.cmp.as_ref();
		for i in 0..children.len() {
			if children[i].iter.valid() {
				self.min_heap.push(i, |a, b| Self::cmp_min(children, cmp, a, b));
			}
		}
	}

	/// Rebuild max_heap with all currently valid iterators.
	fn rebuild_max_heap(&mut self) {
		let children = &self.children;
		let cmp = self.cmp.as_ref();
		let max_heap = self.max_heap.as_mut().unwrap();
		for i in 0..children.len() {
			if children[i].iter.valid() {
				max_heap.push(i, |a, b| Self::cmp_max(children, cmp, a, b));
			}
		}
	}

	/// Initialize for forward iteration from the beginning.
	fn init_forward(&mut self) -> Result<()> {
		self.direction = Direction::Forward;
		self.clear_heaps();

		for child in &mut self.children {
			child.iter.seek_first()?;
		}
		self.rebuild_min_heap();
		Ok(())
	}

	/// Initialize for backward iteration from the end.
	fn init_backward(&mut self) -> Result<()> {
		self.direction = Direction::Backward;
		self.init_max_heap();
		self.clear_heaps();

		for child in &mut self.children {
			child.iter.seek_last()?;
		}
		self.rebuild_max_heap();
		Ok(())
	}

	// -------------------------------------------------------------------------
	// Direction Switching
	// -------------------------------------------------------------------------

	/// Switch from backward to forward, positioning just after `target`.
	///
	/// When switching directions, we need to handle duplicate keys correctly.
	/// The current iterator (heap top) should simply move forward one position.
	/// Non-current iterators need to be positioned at a key strictly greater than
	/// `target` to ensure correct ordering.
	fn switch_to_forward(&mut self, target: &[u8]) -> Result<()> {
		let current_idx = self.max_heap.as_ref().and_then(|h| h.peek());

		self.direction = Direction::Forward;
		self.clear_heaps();

		for (idx, child) in self.children.iter_mut().enumerate() {
			if Some(idx) == current_idx {
				// Current iterator: just call next() once
				child.iter.next()?;
			} else {
				// Non-current: position at key strictly > target
				if child.iter.seek(target)? {
					// Iterate forward until key > target
					while child.iter.valid()
						&& self.cmp.compare(child.iter.key().encoded(), target) != Ordering::Greater
					{
						if !child.iter.next()? {
							break;
						}
					}
				}
				// If seek returned false, iterator is exhausted
			}
		}
		self.rebuild_min_heap();
		Ok(())
	}

	/// Switch from forward to backward, positioning just before `target`.
	///
	/// When switching directions, we need to handle duplicate keys correctly.
	/// The current iterator (heap top) should simply move back one position.
	/// Non-current iterators need to be positioned at a key strictly less than
	/// `target` to ensure correct ordering.
	fn switch_to_backward(&mut self, target: &[u8]) -> Result<()> {
		let current_idx = self.min_heap.peek();

		self.direction = Direction::Backward;
		self.init_max_heap();
		self.clear_heaps();

		for (idx, child) in self.children.iter_mut().enumerate() {
			if Some(idx) == current_idx {
				// Current iterator: just call prev() once
				child.iter.prev()?;
			} else {
				// Non-current: position at key strictly < target
				if child.iter.seek(target)? {
					// Iterate backward until key < target
					while child.iter.valid()
						&& self.cmp.compare(child.iter.key().encoded(), target) != Ordering::Less
					{
						if !child.iter.prev()? {
							break;
						}
					}
				} else {
					// Iterator positioned past all keys, go to last
					child.iter.seek_last()?;
				}
			}
		}
		self.rebuild_max_heap();
		Ok(())
	}

	// -------------------------------------------------------------------------
	// Advancing
	// -------------------------------------------------------------------------

	/// Advance the current winner iterator and restore heap property.
	///
	/// This is the hot path for iteration. After advancing the winner:
	/// - If still valid: sift_down to restore heap property (O(log K))
	/// - If exhausted: pop from heap (O(log K))
	fn advance_winner(&mut self) -> Result<bool> {
		match self.direction {
			Direction::Forward => {
				if self.min_heap.is_empty() {
					return Ok(false);
				}
				let winner = self.min_heap.peek().unwrap();
				let valid = self.children[winner].iter.next()?;
				let children = &self.children;
				let cmp = self.cmp.as_ref();
				if valid {
					// Winner still valid, restore heap property
					self.min_heap.sift_down_root(|a, b| Self::cmp_min(children, cmp, a, b));
				} else {
					// Winner exhausted, remove from heap
					self.min_heap.pop(|a, b| Self::cmp_min(children, cmp, a, b));
				}
				Ok(!self.min_heap.is_empty())
			}
			Direction::Backward => {
				let max_heap = self.max_heap.as_mut().unwrap();
				if max_heap.is_empty() {
					return Ok(false);
				}
				let winner = max_heap.peek().unwrap();
				let valid = self.children[winner].iter.prev()?;
				let children = &self.children;
				let cmp = self.cmp.as_ref();
				if valid {
					self.max_heap
						.as_mut()
						.unwrap()
						.sift_down_root(|a, b| Self::cmp_max(children, cmp, a, b));
				} else {
					self.max_heap.as_mut().unwrap().pop(|a, b| Self::cmp_max(children, cmp, a, b));
				}
				Ok(!self.max_heap.as_ref().unwrap().is_empty())
			}
		}
	}

	// -------------------------------------------------------------------------
	// Accessors
	// -------------------------------------------------------------------------

	#[inline]
	pub fn is_valid(&self) -> bool {
		match self.direction {
			Direction::Forward => !self.min_heap.is_empty(),
			Direction::Backward => self.max_heap.as_ref().is_some_and(|h| !h.is_empty()),
		}
	}

	#[inline]
	pub fn current_key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.is_valid());
		let idx = match self.direction {
			Direction::Forward => self.min_heap.peek().unwrap(),
			Direction::Backward => self.max_heap.as_ref().unwrap().peek().unwrap(),
		};
		self.children[idx].iter.key()
	}

	#[inline]
	pub fn current_value(&self) -> crate::error::Result<&[u8]> {
		debug_assert!(self.is_valid());
		let idx = match self.direction {
			Direction::Forward => self.min_heap.peek().unwrap(),
			Direction::Backward => self.max_heap.as_ref().unwrap().peek().unwrap(),
		};
		self.children[idx].iter.value()
	}
}

// -----------------------------------------------------------------------------
// InternalIterator Implementation
// -----------------------------------------------------------------------------

impl InternalIterator for MergingIterator<'_> {
	/// Seek to the first key >= target.
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.direction = Direction::Forward;
		self.clear_heaps();

		for child in &mut self.children {
			child.iter.seek(target)?;
		}
		self.rebuild_min_heap();
		Ok(self.is_valid())
	}

	/// Seek to the first key.
	fn seek_first(&mut self) -> Result<bool> {
		self.init_forward()?;
		Ok(self.is_valid())
	}

	/// Seek to the last key.
	fn seek_last(&mut self) -> Result<bool> {
		self.init_backward()?;
		Ok(self.is_valid())
	}

	/// Move to the next key.
	///
	/// If currently iterating backward, switches direction first.
	fn next(&mut self) -> Result<bool> {
		if !self.is_valid() {
			return Ok(false);
		}
		if self.direction != Direction::Forward {
			let target = self.current_key().encoded().to_vec();
			self.switch_to_forward(&target)?;
			return Ok(self.is_valid());
		}
		self.advance_winner()
	}

	/// Move to the previous key.
	///
	/// If currently iterating forward, switches direction first.
	fn prev(&mut self) -> Result<bool> {
		if !self.is_valid() {
			return Ok(false);
		}
		if self.direction != Direction::Backward {
			let target = self.current_key().encoded().to_vec();
			self.switch_to_backward(&target)?;
			return Ok(self.is_valid());
		}
		self.advance_winner()
	}

	fn valid(&self) -> bool {
		self.is_valid()
	}

	fn key(&self) -> InternalKeyRef<'_> {
		self.current_key()
	}

	fn value(&self) -> crate::error::Result<&[u8]> {
		self.current_value()
	}
}

// ============================================================================
// COMPACTION ITERATOR - Deduplication and Garbage Collection
// ============================================================================
//
// ## What is Compaction?
//
// Compaction is the background process in LSM-trees that:
// 1. Merges data from multiple levels
// 2. Removes old versions of keys (keeps only latest)
// 3. Removes deleted keys (tombstones) when safe
// 4. Tracks garbage for Value Log cleanup
//
// ## How it Works
//
// ```text
// Input (from MergingIterator):
//   ("apple", seq=100, PUT, "red")      ← newest
//   ("apple", seq=50,  PUT, "green")    ← older
//   ("apple", seq=20,  PUT, "blue")     ← oldest
//   ("banana", seq=60, PUT, "yellow")
//   ("cherry", seq=95, DELETE)
//   ("cherry", seq=40, PUT, "dark")
//
// Output (after compaction, versioning=false, bottom_level=true):
//   ("apple", seq=100, "red")           ← only latest version kept
//   ("banana", seq=60, "yellow")
//   // "cherry" completely removed (DELETE at bottom level)
//
// Side effects:
//   - Older versions of "apple" added to delete_list
//   - All versions of "cherry" added to delete_list
//   - discard_stats updated for VLog garbage collection
// ```
//
// ## Version Retention
//
// With versioning enabled, older versions can be kept based on retention period:
//
// ```text
// versioning=true, retention=1hour:
//   ("apple", seq=100, age=0)      → keep (latest)
//   ("apple", seq=50,  age=30min)  → keep (within retention)
//   ("apple", seq=20,  age=2hours) → discard (outside retention)
// ```
//

/// Helper function to collect VLog discard statistics.
///
/// When a value is discarded during compaction, we need to track
/// how much space can be reclaimed from each VLog file.
///
/// # Arguments
/// * `discard_stats` - Map of file_id → bytes_to_discard
/// * `value` - The value being discarded (may contain VLog pointer)
fn collect_vlog_discard_stats(discard_stats: &mut HashMap<u32, i64>, value: &[u8]) -> Result<()> {
	// Skip empty values (e.g., hard delete entries have no value)
	if value.is_empty() {
		return Ok(());
	}

	// Check if this is a ValueLocation (pointer to VLog)
	// Small values are stored inline, large values have pointers
	let location = ValueLocation::decode(value)?;
	if location.is_value_pointer() {
		let pointer = ValuePointer::decode(&location.value)?;
		let value_data_size = pointer.total_entry_size() as i64;
		*discard_stats.entry(pointer.file_id).or_insert(0) += value_data_size;
	}
	Ok(())
}

/// Compaction iterator that wraps MergingIterator to perform deduplication
/// and garbage collection tracking.
///
/// ## Processing Flow
///
/// ```text
///                      ┌─────────────────────┐
///                      │  MergingIterator    │
///                      │  (sorted stream)    │
///                      └─────────┬───────────┘
///                                │
///                                ▼
///        ┌───────────────────────────────────────────┐
///        │          CompactionIterator               │
///        │                                           │
///        │  1. Group by user_key                     │
///        │  2. Sort versions by seq_num (desc)       │
///        │  3. Apply retention/deletion rules        │
///        │  4. Track garbage for VLog               │
///        │                                           │
///        └─────────────────────┬─────────────────────┘
///                              │
///                              ▼
///                    ┌─────────────────────┐
///                    │   Output versions   │
///                    │   (deduplicated)    │
///                    └─────────────────────┘
/// ```
pub(crate) struct CompactionIterator<'a> {
	/// The underlying merge iterator that provides sorted input.
	merge_iter: MergingIterator<'a>,

	/// Whether this compaction is at the bottom level of the LSM-tree.
	///
	/// At the bottom level:
	/// - DELETE tombstones can be dropped (no older data below)
	/// - All versions of deleted keys can be removed
	///
	/// At non-bottom levels:
	/// - Tombstones must be preserved to mask data in lower levels
	is_bottom_level: bool,

	// ========== Current Key Processing ==========
	/// The user key currently being processed.
	/// All versions of this key are accumulated before processing.
	current_user_key: Vec<u8>,

	/// Buffer for accumulating all versions of the current key.
	///
	/// As the merge iterator produces entries, we collect all entries
	/// with the same user key here before deciding what to keep.
	///
	/// ```text
	/// MergingIterator produces:
	///   ("apple", seq=100, PUT)  → accumulate
	///   ("apple", seq=50, PUT)   → accumulate
	///   ("banana", seq=60, PUT)  → new key! process "apple", start "banana"
	/// ```
	accumulated_versions: Vec<(InternalKey, Value)>,

	/// Buffer for versions that passed the filter and should be output.
	///
	/// After processing accumulated_versions, valid entries are moved here.
	/// The advance() method drains this buffer before processing more input.
	output_versions: Vec<(InternalKey, Value)>,

	// ========== Garbage Collection State ==========
	/// Collected discard statistics: file_id → total_discarded_bytes.
	///
	/// This tells the VLog garbage collector how much space can be
	/// reclaimed from each VLog file.
	pub discard_stats: HashMap<u32, i64>,

	/// Reference to VLog for populating delete-list.
	vlog: Option<Arc<VLog>>,

	/// Batch of stale entries to add to delete-list: (sequence_number, value_size).
	///
	/// These entries are flushed periodically to the VLog's delete list,
	/// which is used during garbage collection.
	pub(crate) delete_list_batch: Vec<(u64, u64)>,

	// ========== Versioning Configuration ==========
	/// Whether to keep multiple versions of keys.
	///
	/// - false: Only keep the latest version (point-in-time database)
	/// - true: Keep versions based on retention_period_ns
	enable_versioning: bool,

	/// How long to keep old versions (in nanoseconds).
	///
	/// - 0: Keep all versions forever
	/// - >0: Keep versions newer than (current_time - retention_period_ns)
	retention_period_ns: u64,

	/// Logical clock for time-based operations.
	/// Used to determine if versions are within retention period.
	clock: Arc<dyn LogicalClock>,

	/// Whether the iterator has been initialized.
	initialized: bool,

	// ========== Snapshot-Aware Compaction ==========
	/// Sorted list of active snapshot sequence numbers.
	///
	/// Used to implement snapshot-aware compaction. Versions visible to any
	/// active snapshot must be preserved. The list is sorted in ascending
	/// order for efficient binary search.
	snapshots: Vec<u64>,
}

impl<'a> CompactionIterator<'a> {
	/// Create a new compaction iterator.
	///
	/// # Arguments
	/// * `iterators` - Source iterators to merge (one per level/file)
	/// * `cmp` - Key comparator
	/// * `is_bottom_level` - Whether compacting to the bottom level
	/// * `vlog` - Optional VLog for garbage tracking
	/// * `enable_versioning` - Whether to keep multiple versions
	/// * `retention_period_ns` - How long to keep old versions
	/// * `clock` - Time source for retention calculations
	/// * `snapshots` - Sorted list of active snapshot sequence numbers
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		iterators: Vec<BoxedInternalIterator<'a>>,
		cmp: Arc<dyn Comparator>,
		is_bottom_level: bool,
		vlog: Option<Arc<VLog>>,
		enable_versioning: bool,
		retention_period_ns: u64,
		clock: Arc<dyn LogicalClock>,
		snapshots: Vec<u64>,
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
			snapshots,
		}
	}

	/// Initialize the iterator by seeking to the first entry.
	fn initialize(&mut self) -> Result<()> {
		self.merge_iter.seek_first()?;
		self.initialized = true;
		Ok(())
	}

	/// Flushes the batched delete-list entries to the VLog.
	///
	/// Called periodically during compaction to avoid building up
	/// too much state in memory.
	pub(crate) fn flush_delete_list_batch(&mut self) -> Result<()> {
		if let Some(ref vlog) = self.vlog {
			if !self.delete_list_batch.is_empty() {
				vlog.add_batch_to_delete_list(std::mem::take(&mut self.delete_list_batch))?;
			}
		}
		Ok(())
	}

	// ========== Snapshot Visibility Methods ==========

	/// Find the earliest snapshot that can see a version with the given sequence number.
	///
	/// This implements the core snapshot visibility check for snapshot-aware compaction.
	/// For a version to be visible to a snapshot, its sequence number must be less
	/// than or equal to the snapshot's sequence number.
	///
	/// # Arguments
	/// * `seq_num` - The sequence number of the version to check
	///
	/// # Returns
	/// * `Ok(SnapshotVisibility::BoundedBySnapshot(snap_seq))` - Version is bounded by snapshot
	///   `snap_seq` (the earliest snapshot that can see it). The version is actually visible to all
	///   snapshots >= `snap_seq`, but `snap_seq` defines the visibility boundary.
	/// * `Ok(SnapshotVisibility::NoActiveSnapshots)` - No snapshots exist
	/// * `Ok(SnapshotVisibility::NewerThanAllSnapshots)` - Version is newer than all snapshots
	///
	/// # Errors
	/// Returns an error if the sequence number is invalid (zero).
	fn find_earliest_visible_snapshot(&self, seq_num: u64) -> Result<SnapshotVisibility> {
		// Fast path: no active snapshots
		if self.snapshots.is_empty() {
			return Ok(SnapshotVisibility::NoActiveSnapshots);
		}

		// Validate seq_num is reasonable (not zero, which is invalid)
		if seq_num == 0 {
			return Err(Error::InvalidArgument(
				"Sequence number 0 is invalid for snapshot visibility check".to_string(),
			));
		}

		// Binary search to find earliest snapshot >= seq_num
		// A snapshot S can see version V if V.seq_num <= S.seq_num
		// So the earliest snapshot that can see V is the smallest S where S >= V.seq_num
		match self.snapshots.binary_search(&seq_num) {
			// Exact match: a snapshot exists at exactly this sequence number.
			// That snapshot can see this version (since snap.seq >= version.seq).
			Ok(idx) => Ok(SnapshotVisibility::BoundedBySnapshot(self.snapshots[idx])),

			// No exact match found. Rust's binary_search returns Err(idx) where idx
			// is the insertion point - the index where seq_num would be inserted
			// to maintain sorted order. This means:
			//   - All snapshots before idx have seq < seq_num (can't see this version)
			//   - All snapshots at/after idx have seq > seq_num (can see this version)
			Err(idx) => {
				if idx < self.snapshots.len() {
					// There's at least one snapshot with seq > seq_num.
					// The snapshot at idx is the earliest one that can see this version.
					Ok(SnapshotVisibility::BoundedBySnapshot(self.snapshots[idx]))
				} else {
					// idx == len means seq_num is greater than ALL snapshot sequence numbers.
					// No existing snapshot can see this version.
					Ok(SnapshotVisibility::NewerThanAllSnapshots)
				}
			}
		}
	}

	/// Determines if two versions of the same key are in the same visibility boundary.
	///
	/// # What is a Visibility Boundary?
	///
	/// A visibility boundary groups versions that are indistinguishable from the
	/// perspective of active snapshots. Two versions in the same boundary are visible
	/// to exactly the same set of snapshots, meaning:
	/// - If any snapshot can see the older version, it can also see the newer version
	/// - Therefore, the newer version "hides" the older one for all observers
	/// - The older version can be safely dropped during compaction
	///
	/// # Visual Example
	///
	///
	/// Snapshots:        S1=50         S2=100        S3=150
	///                    |             |             |
	/// Timeline:    ─────┼─────────────┼─────────────┼─────────────>
	///                    |             |             |
	/// Versions:    v1=30 |  v2=75      |  v3=120     |  v4=180
	///                    |             |             |
	/// Boundaries:  [─────]  [─────────]  [──────────]  [────────>
	///              visible  visible to   visible to   newer than
	///              to S1+   S2 & S3      S3 only      all snapshots
	fn same_visibility_boundary(
		&self,
		newer_vis: SnapshotVisibility,
		older_vis: SnapshotVisibility,
	) -> bool {
		match (newer_vis, older_vis) {
			// Both bounded by the same snapshot boundary - older is superseded by newer
			(
				SnapshotVisibility::BoundedBySnapshot(s1),
				SnapshotVisibility::BoundedBySnapshot(s2),
			) => s1 == s2,

			// Both newer than all snapshots (no snapshot sees either) - older is hidden
			(
				SnapshotVisibility::NewerThanAllSnapshots,
				SnapshotVisibility::NewerThanAllSnapshots,
			) => true,

			// No active snapshots - all versions in same "boundary" (only keep latest)
			(SnapshotVisibility::NoActiveSnapshots, SnapshotVisibility::NoActiveSnapshots) => true,

			// Different visibility states = different boundaries, must keep both
			_ => false,
		}
	}

	/// Check if a version must be preserved due to snapshot visibility.
	///
	/// A version must be preserved if it's the visible version for some active
	/// snapshot (unless hidden by a newer version in the same visibility boundary).
	#[inline]
	fn must_preserve_for_snapshot(&self, visibility: SnapshotVisibility) -> bool {
		matches!(visibility, SnapshotVisibility::BoundedBySnapshot(_))
	}

	/// Process all accumulated versions of the current key.
	///
	/// This is the heart of compaction logic. It decides:
	/// - Which versions to keep (output_versions)
	/// - Which versions to discard (delete_list, discard_stats)
	///
	/// # Snapshot-Aware Compaction
	///
	/// Before applying retention rules, we check snapshot visibility:
	/// - A version visible to an active snapshot MUST be preserved
	/// - Exception: A version can be dropped if a newer version exists in the same snapshot
	///   "boundary" (both visible to the same earliest snapshot). The newer version supersedes the
	///   older one.
	///
	/// # Decision Matrix
	///
	/// ```text
	/// ┌─────────────────┬───────────────┬────────────────┬──────────────────┐
	/// │ Scenario        │ Bottom Level? │ Versioning?    │ Action           │
	/// ├─────────────────┼───────────────┼────────────────┼──────────────────┤
	/// │ Visible to snap │ any           │ any            │ KEEP (unless hidden) │
	/// │ Hidden by newer │ any           │ any            │ DROP             │
	/// │ Latest PUT      │ any           │ any            │ KEEP             │
	/// │ Latest DELETE   │ YES           │ any            │ DROP ALL         │
	/// │ Latest DELETE   │ NO            │ any            │ KEEP (tombstone) │
	/// │ Latest REPLACE  │ any           │ any            │ KEEP, drop older │
	/// │ Older PUT       │ any           │ NO             │ DROP             │
	/// │ Older PUT       │ any           │ YES, in window │ KEEP             │
	/// │ Older PUT       │ any           │ YES, expired   │ DROP             │
	/// └─────────────────┴───────────────┴────────────────┴──────────────────┘
	/// ```
	///
	/// # Example: Snapshot-Aware Compaction
	///
	/// ```text
	/// Snapshots: [50, 150]
	///
	/// Input:
	///   ("key1", seq=200, PUT, "v4")  → NewerThanAllSnapshots
	///   ("key1", seq=100, PUT, "v3")  → BoundedBySnapshot(150)
	///   ("key1", seq=80,  PUT, "v2")  → BoundedBySnapshot(150) - same boundary as v3!
	///   ("key1", seq=30,  PUT, "v1")  → BoundedBySnapshot(50)
	///
	/// Snapshot visibility analysis:
	///   - v4: Latest, keep
	///   - v3: Bounded by snap 150, different boundary from v4, KEEP
	///   - v2: Same boundary as v3 (both bounded by 150), DROP (superseded by v3)
	///   - v1: Bounded by snap 50, different boundary, KEEP
	///
	/// Output: [v4, v3, v1]
	/// ```
	///
	/// # Example: DELETE at Bottom Level
	///
	/// ```text
	/// Input:
	///   ("key1", seq=100, DELETE)
	///   ("key1", seq=50,  PUT, "value")
	///
	/// At bottom level:
	///   - Latest is DELETE → can drop everything
	///   - Output: []
	///   - delete_list: [seq=100, seq=50]
	///
	/// At non-bottom level:
	///   - Must keep tombstone to mask lower levels
	///   - Output: [("key1", seq=100, DELETE)]
	///   - delete_list: [seq=50]
	/// ```
	///
	/// # Example: Versioning with Retention
	///
	/// ```text
	/// Input:
	///   ("key1", seq=100, PUT, "v3", timestamp=now)
	///   ("key1", seq=50,  PUT, "v2", timestamp=now-30min)
	///   ("key1", seq=20,  PUT, "v1", timestamp=now-2hours)
	///
	/// With versioning=true, retention=1hour, no snapshots:
	///   - seq=100: age=0, KEEP (latest)
	///   - seq=50:  age=30min < 1hour, KEEP (within retention)
	///   - seq=20:  age=2hours > 1hour, DROP (expired)
	///   
	/// Output: [seq=100, seq=50]
	/// delete_list: [seq=20]
	/// ```
	fn process_accumulated_versions(&mut self) -> Result<()> {
		if self.accumulated_versions.is_empty() {
			return Ok(());
		}

		// Sort by sequence number (descending) to get the latest version first
		// Higher sequence number = more recent write
		self.accumulated_versions.sort_by(|a, b| b.0.seq_num().cmp(&a.0.seq_num()));

		// Check if latest version is DELETE at bottom level
		// If so, we can completely remove this key from the database
		let latest_is_delete_at_bottom = self.is_bottom_level
			&& !self.accumulated_versions.is_empty()
			&& self.accumulated_versions[0].0.is_hard_delete_marker();

		// Check if any version is REPLACE
		// REPLACE semantics: delete all older versions regardless of retention
		let has_set_with_delete = self.accumulated_versions.iter().any(|(key, _)| key.is_replace());

		// Track the visibility of the previous (newer) version we processed.
		// Used to detect when a newer version supersedes an older one.
		let mut newer_version_visibility: Option<SnapshotVisibility> = None;

		// We need to iterate with indices to access accumulated_versions
		let len = self.accumulated_versions.len();
		for i in 0..len {
			let (key, value) = &self.accumulated_versions[i];
			let is_hard_delete = key.is_hard_delete_marker();
			let is_replace = key.is_replace();
			let is_latest = i == 0;
			let seq_num = key.seq_num();

			// ===== SNAPSHOT-AWARE COMPACTION =====
			//
			// Goal: Drop old versions that no snapshot needs to see.
			//
			// A version is "superseded" when:
			//   1. A newer version of the same key exists
			//   2. Both versions have the same visibility boundary (i.e., visible to the same
			//      earliest snapshot, or both invisible to all snapshots)
			//   3. Therefore, any snapshot that could see the old version will see the newer one
			//      instead - the old version is redundant
			//
			// Exception: When versioning is enabled and no snapshots exist, we keep
			// old versions based on retention policy, not snapshot visibility.

			let current_visibility = self.find_earliest_visible_snapshot(seq_num)?;

			// Check if this version is superseded by a newer version
			let superseded = if let Some(newer_vis) = newer_version_visibility {
				// Can we drop superseded versions in this scenario?
				let snapshot_allows_drop = match current_visibility {
					// Active snapshots exist - use visibility boundaries to decide
					SnapshotVisibility::BoundedBySnapshot(_) => true,
					SnapshotVisibility::NewerThanAllSnapshots => true,
					// No snapshots - only drop if versioning is disabled
					// (with versioning enabled, retention policy decides instead)
					SnapshotVisibility::NoActiveSnapshots => !self.enable_versioning,
				};

				// Superseded = not latest AND in same visibility boundary AND allowed to drop
				snapshot_allows_drop
					&& !is_latest && self.same_visibility_boundary(newer_vis, current_visibility)
			} else {
				// This is the first (newest) version - can't be superseded
				false
			};

			// Is this version required by an active snapshot?
			// (Only matters if not already superseded by a newer version)
			let required_by_snapshot =
				!superseded && self.must_preserve_for_snapshot(current_visibility);

			// ===== DETERMINE IF ENTRY IS STALE =====
			// Stale entries are added to delete_list for VLog garbage collection

			let should_mark_stale = if superseded {
				// Superseded: a newer version in the same visibility boundary
				// makes this version redundant - safe to drop
				true
			} else if required_by_snapshot {
				// Required by snapshot: an active snapshot needs this version - keep it
				false
			} else if latest_is_delete_at_bottom {
				// DELETE at bottom level: mark ALL versions as stale
				// The entire key is being removed from the database
				true
			} else if is_latest && !is_hard_delete && !is_replace {
				// Latest PUT: never stale (will be output)
				false
			} else if is_latest && is_hard_delete && self.is_bottom_level {
				// Latest DELETE at bottom: stale (won't be output)
				true
			} else if is_latest && is_hard_delete && !self.is_bottom_level {
				// Latest DELETE at non-bottom: not stale (tombstone preserved)
				false
			} else if is_latest && is_replace {
				// Latest REPLACE: not stale (will be output)
				false
			} else if is_hard_delete {
				// Older DELETE: always stale (only latest tombstone matters)
				true
			} else if has_set_with_delete && !is_replace {
				// REPLACE found: all older non-REPLACE versions are stale
				true
			} else {
				// Older PUT: check versioning and retention
				if !self.enable_versioning {
					// No versioning enabled: only the latest version matters,
					// all older versions are stale
					true
				} else if self.retention_period_ns > 0 {
					// Versioning enabled with retention period:
					// Keep versions within the retention window, drop older ones
					let current_time = self.clock.now();
					let age = current_time.saturating_sub(key.timestamp);
					age > self.retention_period_ns
				} else {
					// Versioning enabled, retention_period_ns == 0:
					// Keep all versions forever
					false
				}
			};

			// ===== RECORD STALE ENTRIES FOR GARBAGE COLLECTION =====

			if should_mark_stale && self.vlog.is_some() {
				if key.is_tombstone() {
					// Tombstone: record key size
					self.delete_list_batch.push((key.seq_num(), key.size() as u64));
				} else {
					// Regular value: record VLog pointer size
					let location = ValueLocation::decode(value)?;
					if location.is_value_pointer() {
						let pointer = ValuePointer::decode(&location.value)?;
						let value_size = pointer.total_entry_size();
						self.delete_list_batch.push((key.seq_num(), value_size));
					}
				}

				// Update per-file discard statistics
				if let Err(e) = collect_vlog_discard_stats(&mut self.discard_stats, value) {
					log::error!("Error collecting discard stats: {e:?}");
					return Err(e);
				}
			}

			// ===== DETERMINE IF ENTRY SHOULD BE OUTPUT =====

			let should_output = if superseded {
				// Superseded by newer version: don't output
				false
			} else if latest_is_delete_at_bottom {
				// DELETE at bottom: output NOTHING
				false
			} else if should_mark_stale {
				// Stale entries: don't output
				false
			} else if self.enable_versioning || required_by_snapshot {
				// Versioning enabled or snapshot requires it: output
				true
			} else {
				// No versioning, no snapshot requirement: only output latest
				is_latest
			};

			if should_output {
				self.output_versions.push((key.clone(), value.clone()));
			}

			// Update for next iteration (this version becomes the "newer" one)
			newer_version_visibility = Some(current_visibility);
		}

		// Clear accumulated versions for the next key
		self.accumulated_versions.clear();
		Ok(())
	}

	/// Advance to the next output entry.
	///
	/// # Algorithm
	///
	/// ```text
	/// loop:
	///   1. If output_versions is not empty:
	///      → Return next output entry
	///      
	///   2. If merge_iter is exhausted:
	///      → Process remaining accumulated versions
	///      → Return next output entry or None
	///      
	///   3. Get next entry from merge_iter
	///   
	///   4. If new user key:
	///      → Process accumulated versions of previous key
	///      → Start accumulating new key
	///      → Return output if any
	///      
	///   5. If same user key:
	///      → Add to accumulated versions
	///      → Continue loop
	/// ```
	///
	/// # Example Trace
	///
	/// ```text
	/// MergingIterator produces:
	///   ("apple", seq=100, PUT)
	///   ("apple", seq=50, PUT)
	///   ("banana", seq=60, PUT)
	///
	/// advance() call 1:
	///   - output_versions: []
	///   - merge_iter → ("apple", 100)
	///   - new key: accumulate, current_user_key = "apple"
	///   - merge_iter.next()
	///   - loop continues...
	///   
	/// advance() call 1 (continued):
	///   - merge_iter → ("apple", 50)
	///   - same key: accumulate
	///   - merge_iter.next()
	///   - loop continues...
	///   
	/// advance() call 1 (continued):
	///   - merge_iter → ("banana", 60)
	///   - NEW key! Process "apple" accumulated versions
	///   - output_versions = [("apple", 100)]  // only latest
	///   - start accumulating "banana"
	///   - return ("apple", 100)
	///   
	/// advance() call 2:
	///   - output_versions: []
	///   - merge_iter exhausted
	///   - process "banana" accumulated versions
	///   - return ("banana", 60)
	///   
	/// advance() call 3:
	///   - output_versions: []
	///   - merge_iter exhausted
	///   - accumulated_versions: []
	///   - return None
	/// ```
	pub fn advance(&mut self) -> Result<Option<(InternalKey, Value)>> {
		if !self.initialized {
			self.initialize()?;
		}

		loop {
			// Priority 1: Return any pending output versions
			if !self.output_versions.is_empty() {
				// Remove from front to maintain sequence number order
				// (already sorted descending by seq_num)
				return Ok(Some(self.output_versions.remove(0)));
			}

			// Priority 2: Check if merge iterator is exhausted
			if !self.merge_iter.is_valid() {
				// Process any remaining accumulated versions
				if !self.accumulated_versions.is_empty() {
					self.process_accumulated_versions()?;
					// Return first output version if any
					if !self.output_versions.is_empty() {
						return Ok(Some(self.output_versions.remove(0)));
					}
				}
				return Ok(None);
			}

			// Priority 3: Get next entry from merge iterator
			// Extract to owned values to avoid borrow checker issues
			let key_owned = self.merge_iter.current_key().to_owned();
			let user_key_owned = key_owned.user_key.clone();
			let value_owned = self.merge_iter.current_value()?.to_vec();

			// Check if this is a new user key
			let is_new_key =
				self.current_user_key.is_empty() || user_key_owned != self.current_user_key;

			if is_new_key {
				// Process accumulated versions of the previous key
				if !self.accumulated_versions.is_empty() {
					self.process_accumulated_versions()?;

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

/// Implement Iterator trait for convenient use in for loops and iterators.
///
/// # Example
/// ```text
/// let compaction_iter = CompactionIterator::new(...);
/// for result in compaction_iter {
///     let (key, value) = result?;
///     // Write to output SSTable
/// }
/// ```
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
