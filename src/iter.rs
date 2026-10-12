use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::vlog::ValueLocation;
use crate::{Comparator, InternalKey, InternalKeyRef, Key, LSMIterator, Value};

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
	/// same visibility boundary and is published (in which case the newer version
	/// supersedes it).
	BoundedBySnapshot(u64),

	/// No active snapshots exist.
	///
	/// When there are no snapshots, only readers that are not snapshots remain: they
	/// read at the horizon or above, so the newest version at or below the horizon is the
	/// last one that is needed.
	NoActiveSnapshots,

	/// Version has a sequence number higher than all active snapshots.
	///
	/// This version was written after all currently active snapshots were created,
	/// so it is not visible to any existing snapshot. It can be dropped
	/// if a newer, published version exists in the same visibility boundary.
	NewerThanAllSnapshots,
}

/// Boxed internal iterator type for dynamic dispatch.
///
/// This allows us to store different iterator types (MemTable iterators,
/// SSTable iterators, etc.) in the same collection.
pub type BoxedLSMIterator<'a> = Box<dyn LSMIterator + 'a>;

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
	iter: BoxedLSMIterator<'a>,
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
	pub fn new(iterators: Vec<BoxedLSMIterator<'a>>, cmp: Arc<dyn Comparator>) -> Self {
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
	pub fn current_value(&self) -> Result<&[u8]> {
		debug_assert!(self.is_valid());
		let idx = match self.direction {
			Direction::Forward => self.min_heap.peek().unwrap(),
			Direction::Backward => self.max_heap.as_ref().unwrap().peek().unwrap(),
		};
		self.children[idx].iter.value_encoded()
	}
}

// -----------------------------------------------------------------------------
// LSMIterator Implementation
// -----------------------------------------------------------------------------

impl LSMIterator for MergingIterator<'_> {
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

	fn value_encoded(&self) -> Result<&[u8]> {
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
//   ("apple", seq=20,  age=2hours) → keep (the newest older than the period: what a read at
//                                    the start of the period sees)
//   ("apple", seq=10,  age=3hours) → discard (outside retention, and not that version)
// ```
//

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
///        │                                           │
///        └─────────────────────┬─────────────────────┘
///                              │
///                              ▼
///                    ┌─────────────────────┐
///                    │   Output versions   │
///                    │   (deduplicated)    │
///                    └─────────────────────┘
/// ```
/// The version a read at `since` sees, among the `versions` of a key, which are newest write
/// first, for a reader at sequence number `bound`: the one with the newest timestamp before
/// `since`, and the newest write among those of that timestamp.
///
/// The reader sees what was written at or before it. A range deletion it sees, of which
/// `range_deletes` are the sequence numbers of those that cover the key, hides what was written
/// before it. A barrier it sees hides what is written before the barrier: a hard delete is not a
/// version and ends the search, and a replace is one that does. `unknown` are the versions whose
/// time is only known to be at least what `times` says.
fn history_base(
	versions: &[(InternalKey, Value)],
	since: u64,
	times: &[u64],
	unknown: &[bool],
	bound: u64,
	range_deletes: &[u64],
) -> Option<usize> {
	let mut base: Option<(u64, usize)> = None;
	for (idx, (key, _)) in versions.iter().enumerate() {
		let seq = key.seq_num();
		if seq > bound || range_deletes.iter().any(|&rseq| rseq > seq && rseq <= bound) {
			continue;
		}
		let kind = key.kind();
		if matches!(kind, crate::InternalKeyKind::Delete | crate::InternalKeyKind::RangeDelete) {
			break;
		}
		let ts = times[idx];
		// Newest write first, so a version of the same timestamp as the best is not newer. One
		// whose time is not known is kept for being in the period, and is no one's base.
		if !unknown[idx] && ts < since && base.is_none_or(|(best, _)| ts > best) {
			base = Some((ts, idx));
		}
		if kind == crate::InternalKeyKind::Replace {
			break;
		}
	}
	base.map(|(_, idx)| idx)
}

/// The encoded value of a version that was written without a time, stamped with `time`, or `None`
/// if it cannot be: the value is not one that this engine encoded.
///
/// A version without a time reads as written when the versions before it were, so what it reads
/// at changes if a compaction drops them. At the bottom level every older version is read, the
/// time it reads at is known, and it is made the version's own: a later compaction can then drop
/// what it was inferred from and read the same.
fn stamp_value(kind: crate::InternalKeyKind, value: &[u8], time: u64) -> Option<Value> {
	if kind == crate::InternalKeyKind::SoftDelete && value.is_empty() {
		// A soft delete of a tree that was not versioned has no value at all
		let mut stamped = Vec::with_capacity(2 + crate::vlog::VALUE_TIMESTAMP_SIZE);
		stamped.extend_from_slice(&[
			crate::vlog::BIT_HAS_TIMESTAMP,
			crate::vlog::VALUE_LOCATION_VERSION,
		]);
		stamped.extend_from_slice(&time.to_be_bytes());
		return Some(stamped);
	}
	ValueLocation::decode(value).ok().map(|location| location.with_timestamp(Some(time)).encode())
}

/// Which versions of a key a compaction keeps, beyond the ones that readers can see.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum History {
	/// The versions readers can see, and no more: the newest, and what a snapshot reads.
	Latest,
	/// Every version, for a versioned tree that keeps its history for ever.
	All,
	/// The versions written at or after a time, and the newest one before it: what a read at
	/// that time sees. For a versioned tree that keeps its history for a period.
	Since(u64),
}

/// Buffers that `CompactionIterator::decide_history` fills for each key, kept between keys so
/// that deciding what history holds costs no allocation once they have grown.
#[derive(Default)]
struct HistoryScratch {
	hidden: Vec<bool>,
	stamps: Vec<Option<u64>>,
	times: Vec<u64>,
	unknown: Vec<bool>,
	range_deletes: Vec<u64>,
	events: Vec<u64>,
	bounds: Vec<u64>,
	bases: Vec<usize>,
	/// The time to stamp each sorted version with, if it is kept: set for a version without one
	/// when the compaction reads every version older than it, see `stamp_value`.
	restamp: Vec<Option<u64>>,
}

pub(crate) struct CompactionIterator<'a> {
	/// The underlying merge iterator that provides sorted input.
	merge_iter: MergingIterator<'a>,

	/// Whether this compaction is at the bottom level of the LSM-tree.
	///
	/// At the bottom level:
	/// - A DELETE tombstone that is the oldest version kept can be dropped (no older data below)
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
	/// The advance() method drains this buffer front-first before processing
	/// more input (a deque: Vec::remove(0) would memmove per pop).
	output_versions: VecDeque<(InternalKey, Value)>,

	/// Whether the iterator has been initialized.
	initialized: bool,

	// ========== Snapshot-Aware Compaction ==========
	/// Sorted list of active snapshot sequence numbers.
	///
	/// Used to implement snapshot-aware compaction. Versions visible to any
	/// active snapshot must be preserved. The list is sorted in ascending
	/// order for efficient binary search. It must be read after the horizon.
	snapshots: Vec<u64>,

	/// The visibility horizon: the sequence number of the newest version that readers can see.
	///
	/// A reader that is not registered as a snapshot, or that has not started yet, reads at
	/// this sequence number or above. A version above it was not published, so no reader can
	/// see it, and it must neither be dropped nor allowed to supersede the versions below it.
	horizon: u64,

	/// Scratch space: whether each of the sorted versions of the current key is kept.
	keep: Vec<bool>,

	/// Scratch space: whether a period of history holds each of the sorted versions of the
	/// current key, see `decide_history`. Empty for a key of one version, or without history.
	retained: Vec<bool>,

	/// Scratch space: whether a range deletion hides each of the sorted versions of the key.
	covered: Vec<bool>,

	/// Scratch space of `decide_history`.
	scratch: HistoryScratch,

	/// Active range tombstones accumulated during compaction: (start_key, end_key, seq_num)
	active_range_deletions: Vec<(Key, Key, u64)>,

	/// Range deletions of tables the compaction does not read. They are not the compaction's to
	/// drop or to carry, but a reader reads them, so they decide which versions of a key that it
	/// reads are history: they are only looked at to choose the versions a period keeps.
	history_range_deletions: Vec<(Key, Key, u64)>,

	/// The history of a key that is kept, see [`History`].
	history: History,
}

impl<'a> CompactionIterator<'a> {
	/// Create a new compaction iterator.
	///
	/// # Arguments
	/// * `iterators` - Source iterators to merge (one per level/file)
	/// * `cmp` - Key comparator
	/// * `is_bottom_level` - Whether compacting to the bottom level
	/// * `enable_versioning` - Whether to keep multiple versions
	/// * `retention_period_ns` - How long to keep old versions
	/// * `clock` - Time source for retention calculations
	/// * `snapshots` - Sorted list of active snapshot sequence numbers
	/// * `horizon` - The visible sequence number, read before `snapshots` was
	pub(crate) fn new(
		iterators: Vec<BoxedLSMIterator<'a>>,
		cmp: Arc<dyn Comparator>,
		is_bottom_level: bool,
		snapshots: Vec<u64>,
		horizon: u64,
	) -> Self {
		let merge_iter = MergingIterator::new(iterators, cmp);

		Self {
			merge_iter,
			is_bottom_level,
			current_user_key: Vec::new(),
			accumulated_versions: Vec::new(),
			output_versions: VecDeque::new(),
			initialized: false,
			snapshots,
			horizon,
			keep: Vec::new(),
			retained: Vec::new(),
			covered: Vec::new(),
			scratch: HistoryScratch::default(),
			active_range_deletions: Vec::new(),
			history_range_deletions: Vec::new(),
			history: History::Latest,
		}
	}

	/// Keeps the history of every key as `history` says, as well as what readers can see.
	pub(crate) fn with_history(mut self, history: History) -> Self {
		self.history = history;
		self
	}

	/// A compaction iterator for input that is entirely published.
	#[cfg(all(test, not(target_arch = "wasm32")))]
	pub(crate) fn all_published(
		iterators: Vec<BoxedLSMIterator<'a>>,
		cmp: Arc<dyn Comparator>,
		is_bottom_level: bool,
		snapshots: Vec<u64>,
	) -> Self {
		Self::new(iterators, cmp, is_bottom_level, snapshots, u64::MAX)
	}

	/// Sets the range deletions of tables that this compaction does not read, see
	/// `history_range_deletions`.
	pub(crate) fn with_history_range_deletions(
		mut self,
		range_deletions: Vec<(Key, Key, u64)>,
	) -> Self {
		self.history_range_deletions = range_deletions;
		self
	}

	pub(crate) fn with_range_deletions(mut self, range_deletions: Vec<(Key, Key, u64)>) -> Self {
		self.active_range_deletions.extend(range_deletions);
		self
	}

	pub(crate) fn active_range_deletions(&self) -> &[(Key, Key, u64)] {
		&self.active_range_deletions
	}

	/// Initialize the iterator by seeking to the first entry.
	fn initialize(&mut self) -> Result<()> {
		self.merge_iter.seek_first()?;
		self.initialized = true;
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

	/// Whether some snapshot has a sequence number in `[lo, hi)`.
	///
	/// Such a snapshot reads the version at `lo` rather than the one at `hi` or above.
	fn snapshot_in(&self, lo: u64, hi: u64) -> bool {
		let idx = self.snapshots.partition_point(|&s| s < lo);
		self.snapshots.get(idx).is_some_and(|&s| s < hi)
	}

	/// Whether a range deletion hides the version of `user_key` at `seq_num` from every reader.
	///
	/// A range deletion applies to a reader at its sequence number or above, and the horizon is
	/// the lowest sequence number a reader that is not a snapshot reads at, so a range deletion
	/// above the horizon hides nothing yet. A snapshot below the range deletion and not below
	/// the version reads the version, and the range deletion does not apply to it.
	fn is_covered_by_range(&self, user_key: &[u8], seq_num: u64) -> bool {
		self.active_range_deletions.iter().any(|(start, end, rseq)| {
			*rseq >= seq_num
				&& *rseq <= self.horizon
				&& user_key >= start.as_slice()
				&& user_key < end.as_slice()
				&& !self.snapshot_in(seq_num, *rseq)
		})
	}

	/// Process all accumulated versions of the current key.
	///
	/// This is the heart of compaction logic. It decides which versions to keep
	/// (`output_versions`) and which to discard.
	///
	/// # Who reads a version
	///
	/// A version is read by every reader whose sequence number lies in `[version, newer)`, where
	/// `newer` is the sequence number of the next newer version of the key. A reader is either
	/// a registered snapshot, or it reads at the horizon or above: the readers that have not
	/// registered, or have not started. A version is therefore dropped only when
	///
	/// - no snapshot lies in `[version, newer)`: it shares its visibility boundary with the newer
	///   version, and
	/// - the newer version is at or below the horizon: no other reader can read the older one.
	///
	/// Every version above the horizon is kept: nothing can see it yet, and the version below it
	/// is still the one readers see. The newest version at or below the horizon is kept in
	/// turn, and it is that version that drops the ones under it.
	///
	/// # Decision Matrix
	///
	/// ```text
	/// ┌────────────────────────────┬───────────────┬──────────────────────────────┐
	/// │ Scenario                   │ Bottom Level? │ Action                       │
	/// ├────────────────────────────┼───────────────┼──────────────────────────────┤
	/// │ Newer version above horizon│ any           │ KEEP                         │
	/// │ Snapshot in [version,newer)│ any           │ KEEP                         │
	/// │ Newer version at or below  │ any           │ DROP                         │
	/// │ horizon, same boundary     │               │                              │
	/// │ Covered by range deletion  │ any           │ DROP                         │
	/// │ Oldest kept, is a delete   │ YES           │ DROP                         │
	/// │ Oldest kept, is a delete   │ NO            │ KEEP (masks lower levels)    │
	/// └────────────────────────────┴───────────────┴──────────────────────────────┘
	/// ```
	///
	/// A range deletion entry counts as a delete here. The range deletion itself is never
	/// dropped: it is registered in `active_range_deletions` before its entry is considered,
	/// and every output carries all of them.
	///
	/// # Example: Snapshot-Aware Compaction
	///
	/// ```text
	/// Snapshots: [50, 150], horizon: 200
	///
	/// Input:
	///   ("key1", seq=200, PUT, "v4")  → newest, KEEP
	///   ("key1", seq=100, PUT, "v3")  → BoundedBySnapshot(150), KEEP
	///   ("key1", seq=80,  PUT, "v2")  → BoundedBySnapshot(150), same boundary as v3: DROP
	///   ("key1", seq=30,  PUT, "v1")  → BoundedBySnapshot(50), KEEP
	///
	/// Output: [v4, v3, v1]
	/// ```
	///
	/// # Example: Versions Above the Horizon
	///
	/// ```text
	/// Snapshots: [], horizon: 10
	///
	/// Input:
	///   ("key1", seq=12, PUT, "v3")  → above the horizon, KEEP
	///   ("key1", seq=11, PUT, "v2")  → newer version is above the horizon, KEEP
	///   ("key1", seq=8,  PUT, "v1")  → newer version is above the horizon, KEEP
	///   ("key1", seq=5,  PUT, "v0")  → v1 is at or below the horizon, DROP
	///
	/// Output: [v3, v2, v1]
	/// ```
	///
	/// # Example: DELETE at Bottom Level
	///
	/// ```text
	/// Input:
	///   ("key1", seq=100, DELETE)
	///   ("key1", seq=50,  PUT, "value")
	///
	/// At bottom level, no snapshots, horizon: 100:
	///   - the PUT is dropped, the DELETE is then the oldest version kept and is dropped
	///   - Output: []
	///
	/// At bottom level, snapshot: 60:
	///   - the snapshot reads the PUT, so it is kept, and so is the DELETE above it
	///   - Output: [DELETE, PUT]
	///
	/// At non-bottom level:
	///   - Must keep tombstone to mask lower levels
	///   - Output: [("key1", seq=100, DELETE)]
	/// ```
	fn process_accumulated_versions(&mut self) -> Result<()> {
		if self.accumulated_versions.is_empty() {
			return Ok(());
		}

		// Take ownership of the batch so entries can be MOVED into
		// output_versions below instead of cloning every key and value; the
		// buffer (and its capacity) is handed back at the end.
		let mut versions = std::mem::take(&mut self.accumulated_versions);

		// Sort by sequence number (descending) to get the latest version first
		// Higher sequence number = more recent write
		versions.sort_by_key(|b| std::cmp::Reverse(b.0.seq_num()));

		// SAFETY NET: dedup physical duplicates that share an InternalKey
		// (same user_key implied by accumulation pass + same seq_num).
		//
		// `MemTable::add` is atomic via `try_reserve` (see memtable/mod.rs),
		// so two SSTs containing rows with identical `(user_key, seq_num)`
		// should no longer be possible. Were there such rows, the visibility
		// checks below would keep both of them whenever the horizon is below
		// their sequence number, and `BlockBuilder::add` would abort the SST
		// flush with `Error::KeyNotInOrder`.
		//
		// This dedup remains in place as defense-in-depth against any future
		// code path that re-introduces equal-seq duplicates. After sorting by
		// descending seq_num, equal-seq rows are adjacent; `dedup_by_key`
		// keeps the first (sort is stable, so this is the row from the lowest
		// level_idx — the newer source). In correct operation it deduplicates
		// zero rows.
		versions.dedup_by_key(|b| b.0.seq_num());

		// Register any range tombstones in accumulated versions
		for (k, v) in &versions {
			if k.kind() == crate::InternalKeyKind::RangeDelete {
				self.active_range_deletions.push((k.user_key.clone(), v.clone(), k.seq_num()));
			}
		}

		let mut keep = std::mem::take(&mut self.keep);
		keep.clear();

		// The sequence number and visibility of the previous (newer) version.
		let mut newer: Option<(u64, SnapshotVisibility)> = None;

		// What a period of history holds. A key of one version is its newest: it is kept, as it
		// is exposed, and there is nothing to decide.
		let mut retained = std::mem::take(&mut self.retained);
		retained.clear();
		self.scratch.restamp.clear();
		self.scratch.hidden.clear();
		if self.history != History::Latest && versions.len() > 1 {
			self.decide_history(&versions, &mut retained);
		}
		let mut covered_flags = std::mem::take(&mut self.covered);
		covered_flags.clear();

		for (idx, (key, _)) in versions.iter().enumerate() {
			let seq_num = key.seq_num();
			let visibility = self.find_earliest_visible_snapshot(seq_num)?;

			// A version is exposed when it is the newest, or the version above it is not
			// published: a reader that is not a snapshot reads it. Otherwise it is superseded
			// by the newer version when no snapshot lies between the two.
			let (exposed, superseded) = match newer {
				Some((newer_seq, newer_vis)) if newer_seq <= self.horizon => {
					(false, self.same_visibility_boundary(newer_vis, visibility))
				}
				_ => (true, false),
			};

			// What is neither exposed nor superseded has a snapshot between it and the newer
			// version, as the visibility of the newer version differs.
			let required = !superseded && self.must_preserve_for_snapshot(visibility);
			debug_assert!(exposed || superseded || required);

			let covered = key.kind() != crate::InternalKeyKind::RangeDelete
				&& self.is_covered_by_range(&key.user_key, seq_num);

			let kind = key.kind();
			let barrier = matches!(
				kind,
				crate::InternalKeyKind::Delete
					| crate::InternalKeyKind::RangeDelete
					| crate::InternalKeyKind::Replace
			);
			let retained = retained.get(idx).copied().unwrap_or(false);
			// A barrier that is dropped shows what is below it, in the tables that this
			// compaction does not read, as history again: it stays where there is anything below.
			// (one that a newer barrier hides from every reader is not read, and what is kept of
			// what lies below it is decided by the rule that follows)
			let hidden_barrier = self.scratch.hidden.get(idx).copied().unwrap_or(false);
			let barrier_kept = self.history != History::Latest
				&& barrier
				&& !self.is_bottom_level
				&& !hidden_barrier;

			keep.push(!covered && (exposed || required || retained || barrier_kept));
			covered_flags.push(covered);
			newer = Some((seq_num, visibility));
		}

		// A barrier stays for as long as a version below it is kept, whichever rule kept that
		// version: a snapshot that reads it, or a period of history that holds it. Without the
		// barrier the readers above it would see what it hides.
		if self.history != History::Latest {
			let mut kept_below = false;
			for (idx, (key, _)) in versions.iter().enumerate().rev() {
				let barrier = matches!(
					key.kind(),
					crate::InternalKeyKind::Delete
						| crate::InternalKeyKind::RangeDelete
						| crate::InternalKeyKind::Replace
				);
				if barrier && kept_below && !keep[idx] && !covered_flags[idx] {
					keep[idx] = true;
				}
				kept_below |= keep[idx];
			}
		}

		// At the bottom level nothing lies below, so a delete that is the oldest version kept
		// hides nothing and goes, and so does the next one it uncovers. A delete above a version
		// that is kept stays: it hides that version from the readers above it.
		if self.is_bottom_level {
			for (idx, (key, _)) in versions.iter().enumerate().rev() {
				if !keep[idx] {
					continue;
				}
				if !key.is_hard_delete_marker() {
					break;
				}
				keep[idx] = false;
			}
		}

		// Entries are moved out of the batch; kept ones go to output_versions.
		for (idx, (mut entry, kept)) in versions.drain(..).zip(keep.iter().copied()).enumerate() {
			if kept {
				if let Some(Some(time)) = self.scratch.restamp.get(idx).copied() {
					if let Some(stamped) = stamp_value(entry.0.kind(), &entry.1, time) {
						entry.1 = stamped;
					}
				}
				self.output_versions.push_back(entry);
			}
		}

		// Hand the (now empty) buffers back so their capacity is reused for the next key's
		// versions.
		self.accumulated_versions = versions;
		self.keep = keep;
		self.retained = retained;
		self.covered = covered_flags;
		Ok(())
	}

	/// Decides which of the `versions` of a key, which are newest first, a period of history
	/// holds, and sets `retained` to one flag for each.
	///
	/// A hard delete or a replace hides what is below it from `history`, so a version below one
	/// is not history, for the readers that read the barrier. A reader whose sequence number is
	/// below the barrier's still has the version in its history, and so does one that reads at
	/// the horizon when the barrier is above it: only the closest barrier above a version
	/// matters, as a snapshot that lies below a farther one lies below the nearer as well, or
	/// reads the nearer first.
	///
	/// With `History::All` everything that is not hidden is held. With `History::Since` that is
	/// what was written at or after the time the period starts at, and the version a read at that
	/// time sees, for every reader there is: the readers that read the newest versions, one at the
	/// horizon, and each snapshot, which sees only the versions written at or before it, and the
	/// range deletions at or before it, so the newest in time of what it sees is not always the
	/// same one.
	fn decide_history(&mut self, versions: &[(InternalKey, Value)], retained: &mut Vec<bool>) {
		let mut sc = std::mem::take(&mut self.scratch);
		let is_barrier = |kind: crate::InternalKeyKind| {
			matches!(
				kind,
				crate::InternalKeyKind::Delete
					| crate::InternalKeyKind::RangeDelete
					| crate::InternalKeyKind::Replace
			)
		};

		sc.hidden.clear();
		let mut closest_barrier: Option<u64> = None;
		for (key, _) in versions {
			let seq = key.seq_num();
			sc.hidden.push(
				closest_barrier.is_some_and(|barrier| {
					barrier <= self.horizon && !self.snapshot_in(seq, barrier)
				}),
			);
			if is_barrier(key.kind()) {
				closest_barrier = Some(seq);
			}
		}
		retained.extend(sc.hidden.iter().map(|hidden| !hidden));

		if let History::Since(since) = self.history {
			// The range deletions that cover this key, whichever table they are in: a reader
			// reads them all, so what they hide is not history for it.
			sc.range_deletes.clear();
			let user_key = versions[0].0.user_key.as_slice();
			sc.range_deletes.extend(
				self.active_range_deletions
					.iter()
					.chain(self.history_range_deletions.iter())
					.filter(|(start, end, _)| {
						user_key >= start.as_slice() && user_key < end.as_slice()
					})
					.map(|(_, _, rseq)| *rseq),
			);
			// The same range deletion is in every table that a compaction wrote it to
			sc.range_deletes.sort_unstable();
			sc.range_deletes.dedup();

			// The time of each version: what it was stamped with, or, for one written while the
			// tree was not versioned, that of the versions written before it that a reader of it
			// reads.
			sc.stamps.clear();
			sc.stamps.extend(
				versions.iter().map(|(key, value)| ValueLocation::entry_time(key.kind(), value)),
			);
			if sc.range_deletes.is_empty() {
				ValueLocation::infer_times_into(
					versions.len(),
					|idx| (sc.stamps[idx], is_barrier(versions[idx].0.kind())),
					&mut sc.times,
				);
			} else {
				// A range deletion that a reader of the version reads hides what is written
				// before it from the reader, so it hides it from the inference as well.
				sc.times.clear();
				for idx in 0..versions.len() {
					let time = match sc.stamps[idx] {
						Some(time) => time,
						// A barrier takes what is written before it away before it is given a
						// time, so one without a time reads at 0, and the time of a hard delete
						// is never used
						None if is_barrier(versions[idx].0.kind()) => 0,
						None => {
							let reader = versions[idx].0.seq_num();
							let mut newest = 0u64;
							for older in (idx + 1..versions.len()).rev() {
								let seq = versions[older].0.seq_num();
								if sc.range_deletes.iter().any(|&rseq| rseq > seq && rseq <= reader)
								{
									continue;
								}
								if is_barrier(versions[older].0.kind()) {
									newest = 0;
								}
								if let Some(time) = sc.stamps[older] {
									newest = newest.max(time);
								}
							}
							newest
						}
					};
					sc.times.push(time);
				}
			}

			// A version without a time is given that of the versions written before it, and a
			// compaction above the bottom level does not read all of them: the time is not less
			// than what it makes of it, and can be any more.
			sc.unknown.clear();
			let bottom = self.is_bottom_level;
			sc.unknown.extend(sc.stamps.iter().zip(versions).map(|(stamp, (key, _))| {
				!bottom
					&& stamp.is_none()
					&& !matches!(
						key.kind(),
						crate::InternalKeyKind::Delete | crate::InternalKeyKind::RangeDelete
					)
			}));

			// Two readers between the same two events see the same, so one stands for both: of
			// the events (a version, or a range deletion that covers the key), those that a
			// reader reads exactly up to. The readers are the newest versions, the horizon and
			// the snapshots.
			sc.events.clear();
			sc.events.extend(versions.iter().map(|(key, _)| key.seq_num()));
			sc.events.extend(sc.range_deletes.iter().copied());
			sc.events.sort_unstable();
			sc.events.dedup();
			sc.bounds.clear();
			for (at, &event) in sc.events.iter().enumerate() {
				let read = match sc.events.get(at + 1) {
					None => true,
					Some(&next) => {
						self.snapshot_in(event, next)
							|| (event <= self.horizon && self.horizon < next)
					}
				};
				if read {
					sc.bounds.push(event);
				}
			}
			sc.bases.clear();
			for &bound in &sc.bounds {
				if let Some(base) =
					history_base(versions, since, &sc.times, &sc.unknown, bound, &sc.range_deletes)
				{
					sc.bases.push(base);
				}
			}

			// At the bottom level every older version is read, and the time of a version without
			// one is known: it is stamped if it is kept.
			sc.restamp.clear();
			if self.is_bottom_level {
				sc.restamp.extend(sc.stamps.iter().zip(&sc.times).zip(versions).map(
					|((stamp, time), (key, _))| {
						(stamp.is_none()
							&& matches!(
								key.kind(),
								crate::InternalKeyKind::Set
									| crate::InternalKeyKind::SoftDelete
									| crate::InternalKeyKind::Replace
							))
						.then_some(*time)
					},
				));
			}

			for (idx, kept) in retained.iter_mut().enumerate() {
				*kept =
					*kept && (sc.times[idx] >= since || sc.unknown[idx] || sc.bases.contains(&idx));
			}
		}
		self.scratch = sc;
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
			// (front-first to maintain descending seq_num order)
			if let Some(entry) = self.output_versions.pop_front() {
				return Ok(Some(entry));
			}

			// Priority 2: Check if merge iterator is exhausted
			if !self.merge_iter.is_valid() {
				// Process any remaining accumulated versions
				if !self.accumulated_versions.is_empty() {
					self.process_accumulated_versions()?;
					// Return first output version if any
					if let Some(entry) = self.output_versions.pop_front() {
						return Ok(Some(entry));
					}
				}
				return Ok(None);
			}

			// Priority 3: Get next entry from merge iterator
			// Extract to owned values to avoid borrow checker issues
			let key_owned = self.merge_iter.current_key().to_owned();
			let value = self.merge_iter.current_value()?.to_vec();

			// Check if this is a new user key (bytewise, matching the
			// grouping the rest of this iterator relies on). The user key is
			// copied into current_user_key only when it changes, not per entry.
			let is_new_key =
				self.current_user_key.is_empty() || key_owned.user_key != self.current_user_key;

			if is_new_key {
				// Process accumulated versions of the previous key
				if !self.accumulated_versions.is_empty() {
					self.process_accumulated_versions()?;

					// Start accumulating the new key
					self.current_user_key.clone_from(&key_owned.user_key);
					self.accumulated_versions.push((key_owned, value));

					// Advance merge iterator for next iteration
					self.merge_iter.next()?;

					// Return first output version from processed key if any
					if let Some(entry) = self.output_versions.pop_front() {
						return Ok(Some(entry));
					}
				} else {
					// First key - start accumulating
					self.current_user_key.clone_from(&key_owned.user_key);
					self.accumulated_versions.push((key_owned, value));

					// Advance merge iterator for next iteration
					self.merge_iter.next()?;
				}
			} else {
				// Same user key - add to accumulated versions
				self.accumulated_versions.push((key_owned, value));

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
