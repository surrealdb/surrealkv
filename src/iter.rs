use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use crate::clock::LogicalClock;
use crate::error::Result;
use crate::vlog::{VLog, ValueLocation, ValuePointer};
use crate::{Comparator, InternalIterator, InternalKey, InternalKeyRef, Value};

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
// We use a custom implementation instead of std::collections::BinaryHeap because:
//   1. We need custom comparators (not just Ord trait)
//   2. We need more control of the data structure for future optimisations
//
// ## Array-to-Tree Mapping
//
// The tree structure is implicit in the array indices:
//
// ```
// Array:   [10, 20, 30, 40, 50]
// Index:     0   1   2   3   4
//
// Tree visualization:
//
//               [10]          ← index 0 (root)
//              /    \
//           [20]    [30]      ← index 1, 2
//           /  \
//        [40]  [50]           ← index 3, 4
// ```
//
// ## Index Formulas
//
// For any element at index `i`:
// - Parent index:      `(i - 1) / 2`  (integer division)
// - Left child index:  `2 * i + 1`
// - Right child index: `2 * i + 2`
//
// Example calculations:
// | Index | Value | Parent = (i-1)/2 | Left = 2i+1 | Right = 2i+2 |
// |-------|-------|------------------|-------------|--------------|
// |   0   |  10   |  none            |      1      |      2       |
// |   1   |  20   |  0 (value 10)    |      3      |      4       |
// |   2   |  30   |  0 (value 10)    |      5      |      6       |
// |   3   |  40   |  1 (value 20)    |      7      |      8       |
// |   4   |  50   |  1 (value 20)    |      9      |     10       |
//
// ## Min-Heap Property
//
// Every parent must be ≤ its children. This ensures the minimum
// element is always at the root (index 0).
//
struct BinaryHeap<T, F> {
	/// The underlying storage - a Vec that represents the tree structure.
	///
	/// The tree is stored level-by-level:
	/// - Index 0: root (level 0)
	/// - Index 1-2: level 1
	/// - Index 3-6: level 2
	/// - Index 7-14: level 3
	/// - etc.
	data: Vec<T>,

	/// Custom comparison function.
	///
	/// For a min-heap: returns Ordering::Less if `a` should be closer to root than `b`
	/// For a max-heap: returns Ordering::Less if `a` should be closer to root than `b`
	///                 (achieved by reversing the comparison)
	cmp: F,
}

impl<T, F: Fn(&T, &T) -> Ordering> BinaryHeap<T, F> {
	/// Create a new heap with pre-allocated capacity.
	///
	/// # Arguments
	/// * `capacity` - Initial capacity for the underlying Vec
	/// * `cmp` - Comparison function that defines heap ordering
	///
	/// # Example
	/// ```
	/// // Min-heap of integers
	/// let min_heap = |a: &i32, b: &i32| a.cmp(b);
	///
	/// // Max-heap of integers (reverse comparison)
	/// let max_heap = |a: &i32, b: &i32| b.cmp(a);
	/// ```
	fn with_capacity(capacity: usize, cmp: F) -> Self {
		Self {
			data: Vec::with_capacity(capacity),
			cmp,
		}
	}

	/// Returns the number of elements in the heap.
	#[inline]
	fn len(&self) -> usize {
		self.data.len()
	}

	/// Returns true if the heap contains no elements.
	#[inline]
	fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	/// Push an item onto the heap.
	///
	/// # Algorithm
	/// 1. Add the new element at the end of the array (last position in tree)
	/// 2. "Sift up": repeatedly swap with parent until heap property is restored
	///
	/// # Example: Push 15 into [10, 20, 30, 40, 50]
	///
	/// ```text
	/// Initial:
	///               [10]
	///              /    \
	///           [20]    [30]
	///           /  \
	///        [40]  [50]
	///
	/// Step 1: Add 15 at end (index 5)
	/// Array: [10, 20, 30, 40, 50, 15]
	///                            ↑ new
	///
	///               [10]
	///              /    \
	///           [20]    [30]
	///           /  \    /
	///        [40] [50] [15] ← new element
	///
	/// Step 2: Sift up
	///   pos = 5, parent = (5-1)/2 = 2
	///   Compare: 15 < 30? YES → swap!
	///   
	/// Array: [10, 20, 15, 40, 50, 30]
	///                 ↑           ↑ swapped
	///
	///               [10]
	///              /    \
	///           [20]    [15] ← moved up
	///           /  \    /
	///        [40] [50] [30] ← moved down
	///
	/// Step 3: Continue sift up
	///   pos = 2, parent = (2-1)/2 = 0
	///   Compare: 15 < 10? NO → stop!
	///
	/// Final: [10, 20, 15, 40, 50, 30]
	/// ```
	fn push(&mut self, item: T) {
		self.data.push(item);
		self.sift_up(self.data.len() - 1);
	}

	/// Remove and return the top (highest priority) item.
	///
	/// # Algorithm
	/// 1. Swap root with last element
	/// 2. Remove last element (the original root)
	/// 3. "Sift down": restore heap property by swapping root down
	///
	/// # Example: Pop from [10, 20, 15, 40, 50, 30]
	///
	/// ```text
	/// Initial:
	///               [10] ← minimum (we want to remove this)
	///              /    \
	///           [20]    [15]
	///           /  \    /
	///        [40] [50] [30]
	///
	/// Step 1: Swap root (index 0) with last (index 5)
	/// Array: [30, 20, 15, 40, 50, 10]
	///         ↑                   ↑ swapped
	///
	///               [30] ← was at end
	///              /    \
	///           [20]    [15]
	///           /  \    /
	///        [40] [50] [10] ← was root
	///
	/// Step 2: Remove last element
	/// Array: [30, 20, 15, 40, 50]  → returns 10
	///
	///               [30] ← violates heap property!
	///              /    \
	///           [20]    [15]
	///           /  \
	///        [40] [50]
	///
	/// Step 3: Sift down
	///   pos = 0, left = 1, right = 2
	///   smallest among {30, 20, 15} = 15 (index 2)
	///   Swap positions 0 and 2:
	///
	/// Array: [15, 20, 30, 40, 50]
	///         ↑       ↑ swapped
	///
	///               [15] ← moved up
	///              /    \
	///           [20]    [30] ← moved down
	///           /  \
	///        [40] [50]
	///
	/// Step 4: Continue sift down from pos=2
	///   left = 5, right = 6 (both out of bounds)
	///   No children → stop!
	///
	/// Final: [15, 20, 30, 40, 50]
	/// ```
	fn pop(&mut self) -> Option<T> {
		if self.data.is_empty() {
			return None;
		}
		let len = self.data.len();
		if len == 1 {
			return self.data.pop();
		}
		// Swap root with last element
		self.data.swap(0, len - 1);
		// Remove last (original root)
		let result = self.data.pop();
		// Restore heap property
		if !self.data.is_empty() {
			self.sift_down(0);
		}
		result
	}

	/// Get a reference to the top item without removing it.
	///
	/// For a min-heap, this is the smallest element.
	/// For a max-heap, this is the largest element.
	#[inline]
	fn peek(&self) -> Option<&T> {
		self.data.first()
	}

	/// Get a mutable reference to the top item.
	///
	/// # IMPORTANT
	/// After modifying the element, you MUST call `sift_down_root()`
	/// to restore the heap property!
	///
	/// # Why This Exists
	/// This is a key optimization for the merging iterator. Instead of:
	/// ```text
	/// let item = heap.pop();     // O(log N)
	/// item.advance_iterator();
	/// heap.push(item);           // O(log N)
	/// // Total: O(2 log N)
	/// ```
	///
	/// We can do:
	/// ```text
	/// let item = heap.peek_mut(); // O(1)
	/// item.advance_iterator();
	/// heap.sift_down_root();      // O(log N)
	/// // Total: O(log N)
	/// ```
	#[inline]
	fn peek_mut(&mut self) -> Option<&mut T> {
		self.data.first_mut()
	}

	/// Restore heap property after modifying the root via peek_mut().
	///
	/// # Example
	/// ```text
	/// Before (heap property violated at root):
	///               [50] ← modified, too large!
	///              /    \
	///           [20]    [30]
	///
	/// After sift_down_root():
	///               [20]
	///              /    \
	///           [50]    [30]
	/// ```
	fn sift_down_root(&mut self) {
		if !self.data.is_empty() {
			self.sift_down(0);
		}
	}

	/// Drain all items from the heap, returning them as a Vec.
	/// The returned Vec is NOT in sorted order - it's the raw heap array.
	fn drain(&mut self) -> Vec<T> {
		std::mem::take(&mut self.data)
	}

	/// Sift up: move element at `pos` towards the root until heap property is satisfied.
	///
	/// Used after inserting a new element at the end of the heap.
	///
	/// # Algorithm
	/// ```text
	/// while pos is not root:
	///     parent = (pos - 1) / 2
	///     if element[pos] < element[parent]:  // For min-heap
	///         swap(pos, parent)
	///         pos = parent
	///     else:
	///         break  // Heap property satisfied
	/// ```
	#[inline]
	fn sift_up(&mut self, mut pos: usize) {
		while pos > 0 {
			let parent = (pos - 1) / 2;
			// If current element should be higher priority than parent, swap
			if (self.cmp)(&self.data[pos], &self.data[parent]) == Ordering::Less {
				self.data.swap(pos, parent);
				pos = parent;
			} else {
				break;
			}
		}
	}

	/// Sift down: move element at `pos` towards the leaves until heap property is satisfied.
	///
	/// Used after:
	/// - Removing the root (pop operation)
	/// - Modifying the root (via peek_mut + sift_down_root)
	///
	/// # Algorithm
	/// ```text
	/// loop:
	///     left = 2 * pos + 1
	///     right = 2 * pos + 2
	///     smallest = pos
	///     
	///     if left exists and element[left] < element[smallest]:
	///         smallest = left
	///     if right exists and element[right] < element[smallest]:
	///         smallest = right
	///     
	///     if smallest != pos:
	///         swap(pos, smallest)
	///         pos = smallest
	///     else:
	///         break  // Heap property satisfied
	/// ```
	///
	/// # Example: Sift down 30 from root
	/// ```text
	/// pos=0:  [30, 20, 15, 40, 50]
	///         left=1 (20), right=2 (15)
	///         smallest = 2 (value 15)
	///         swap(0, 2) → [15, 20, 30, 40, 50]
	///
	/// pos=2:  left=5 (none), right=6 (none)
	///         No children, smallest stays 2
	///         Done!
	/// ```
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
// MERGING ITERATOR - K-Way Merge Using Binary Heap
// ============================================================================
//
// ## Problem
//
// Given K sorted iterators, produce a single sorted stream of all elements.
// This is the core operation for LSM-tree reads and compaction.
//
// ## Example
//
// ```text
// Input iterators (each sorted):
//   Iter0: ["apple", "cherry", "fig"]
//   Iter1: ["banana", "date", "grape"]
//   Iter2: ["avocado", "coconut"]
//
// Output (merged, sorted):
//   "apple" → "avocado" → "banana" → "cherry" → "coconut" → "date" → "fig" → "grape"
// ```
//
// ## Algorithm (using min-heap)
//
// 1. Initialize: Put all iterators in a min-heap, ordered by their current key
// 2. To get next element: a. The heap root has the iterator with the smallest current key b. Output
//    that key c. Advance that iterator to its next element d. If iterator is exhausted, remove from
//    heap e. Otherwise, sift down to restore heap property
//
// ## Bidirectional Iteration
//
// This implementation supports both forward and backward iteration:
// - Forward (next): uses min-heap (smallest key at top)
// - Backward (prev): uses max-heap (largest key at top)
//

/// Direction of iteration
#[derive(Clone, Copy, PartialEq)]
enum Direction {
	Forward,  // Using min-heap, moving towards larger keys
	Backward, // Using max-heap, moving towards smaller keys
}

/// Entry in the merge heap - wraps an iterator with metadata.
///
/// # Fields
/// - `iter`: The actual iterator pointing to sorted data
/// - `level_idx`: Original index for stable ordering when keys are equal
///
/// # Why level_idx Matters
///
/// In an LSM-tree, lower level numbers contain newer data:
/// - Level 0 (MemTable): newest writes
/// - Level 1: older data
/// - Level 2: even older data
///
/// When two iterators have the same key, we want the newer version (lower level).
/// The `level_idx` serves as a tiebreaker in the heap comparison.
///
/// ```text
/// Example:
///   Iter0 (level_idx=0): current_key = "apple", seq=100 (newer)
///   Iter1 (level_idx=1): current_key = "apple", seq=50  (older)
///   
/// Heap comparison for same user key:
///   "apple" vs "apple" → Equal
///   Tiebreaker: level_idx 0 < 1
///   Winner: Iter0 (newer version)
/// ```
struct HeapEntry<'a> {
	iter: BoxedInternalIterator<'a>,
	/// Original index for stable ordering on equal keys.
	/// Lower index = higher priority (newer data in LSM-tree).
	level_idx: usize,
}

/// K-way merge iterator using binary heaps for O(log K) operations.
///
/// ## Architecture
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────┐
/// │                    MergingIterator                          │
/// │                                                             │
/// │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
/// │  │  min_heap   │  │  max_heap   │  │     exhausted       │ │
/// │  │ (forward)   │  │ (backward)  │  │  (empty iterators)  │ │
/// │  │             │  │  (lazy)     │  │                     │ │
/// │  │ [Iter0]     │  │             │  │ [Iter3, Iter4]      │ │
/// │  │ [Iter1]     │  │             │  │                     │ │
/// │  │ [Iter2]     │  │             │  │                     │ │
/// │  └─────────────┘  └─────────────┘  └─────────────────────┘ │
/// │                                                             │
/// │  direction: Forward                                         │
/// │  cmp: Arc<dyn Comparator>                                   │
/// └─────────────────────────────────────────────────────────────┘
/// ```
///
/// ## State Transitions
///
/// ```text
/// seek_first() → Forward mode, all valid iters in min_heap
/// seek_last()  → Backward mode, all valid iters in max_heap
/// next()       → Advance winner in min_heap (or switch direction)
/// prev()       → Advance winner in max_heap (or switch direction)
/// ```
pub(crate) struct MergingIterator<'a> {
	/// Min-heap for forward iteration.
	///
	/// Ordering: smallest key at root (highest priority).
	///
	/// Comparator logic:
	/// ```text
	/// compare(a, b):
	///   if a.key < b.key: return Less (a wins)
	///   if a.key > b.key: return Greater (b wins)
	///   if a.key == b.key: return a.level_idx.cmp(b.level_idx)
	///                      (lower level_idx wins = newer data)
	/// ```
	min_heap: BinaryHeap<HeapEntry<'a>, Box<dyn Fn(&HeapEntry<'a>, &HeapEntry<'a>) -> Ordering>>,

	/// Max-heap for backward iteration, lazily initialized.
	///
	/// Ordering: largest key at root (highest priority).
	///
	/// Comparator logic (reversed):
	/// ```text
	/// compare(a, b):
	///   if a.key > b.key: return Less (a wins - note reversal!)
	///   if a.key < b.key: return Greater (b wins)
	///   if a.key == b.key: return b.level_idx.cmp(a.level_idx)
	///                      (lower level_idx wins, but reversed comparison)
	/// ```
	max_heap:
		Option<BinaryHeap<HeapEntry<'a>, Box<dyn Fn(&HeapEntry<'a>, &HeapEntry<'a>) -> Ordering>>>,

	/// Storage for iterators that have been exhausted (no more elements).
	///
	/// When an iterator runs out of elements in one direction, it's moved here.
	/// When switching directions, these iterators are repositioned and may
	/// become valid again.
	exhausted: Vec<HeapEntry<'a>>,

	/// Current iteration direction.
	direction: Direction,

	/// Comparator for comparing user keys.
	/// Usually lexicographic byte comparison, but could be custom.
	cmp: Arc<dyn Comparator>,
}

impl<'a> MergingIterator<'a> {
	/// Create a new merging iterator from multiple source iterators.
	///
	/// # Arguments
	/// * `iterators` - Source iterators to merge. Each should be sorted.
	/// * `cmp` - Comparator for ordering keys.
	///
	/// # Initial State
	/// All iterators start in the `exhausted` list because they haven't
	/// been positioned yet. Call `seek_first()`, `seek_last()`, or `seek()`
	/// to initialize.
	///
	/// # Example
	/// ```text
	/// let iters = vec![
	///     memtable.iter(),      // level_idx = 0 (newest)
	///     l1_sst.iter(),        // level_idx = 1
	///     l2_sst.iter(),        // level_idx = 2 (oldest)
	/// ];
	/// let merger = MergingIterator::new(iters, comparator);
	/// merger.seek_first()?;  // Position all at their first elements
	/// ```
	pub fn new(iterators: Vec<BoxedInternalIterator<'a>>, cmp: Arc<dyn Comparator>) -> Self {
		let capacity = iterators.len();

		// Create min-heap comparator (smaller key = higher priority = Less)
		//
		// The heap's sift_up/sift_down use `Less` to determine priority,
		// so we return Less when `a` should be closer to the root.
		let cmp_clone = Arc::clone(&cmp);
		let min_cmp: Box<dyn Fn(&HeapEntry<'a>, &HeapEntry<'a>) -> Ordering> =
			Box::new(move |a: &HeapEntry, b: &HeapEntry| {
				let key_a = a.iter.key().encoded();
				let key_b = b.iter.key().encoded();
				match cmp_clone.compare(key_a, key_b) {
					Ordering::Equal => a.level_idx.cmp(&b.level_idx), // Stable sort: lower
					// level wins
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
			max_heap: None, // Lazy initialization
			exhausted,
			direction: Direction::Forward,
			cmp,
		}
	}

	/// Create max-heap comparator (larger key = higher priority).
	///
	/// For max-heap, we reverse the comparison so that larger keys
	/// return `Ordering::Less` (which the heap treats as higher priority).
	///
	/// # Comparator Logic
	/// ```text
	/// For keys: "apple" vs "banana"
	///   Normal:  "apple" < "banana" → Less
	///   Reversed: "apple" < "banana" → Greater (so "banana" wins)
	///
	/// For equal keys with level_idx:
	///   level_idx 0 vs 1
	///   We still want lower level_idx to win, but since we're reversing,
	///   we compare b.level_idx with a.level_idx
	/// ```
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
					Ordering::Equal => a.level_idx.cmp(&b.level_idx),
					ord => ord.reverse(), // Reverse for max-heap
				}
			});
		BinaryHeap::with_capacity(capacity, max_cmp)
	}

	/// Initialize for forward iteration - position all iterators at their first element.
	///
	/// # Algorithm
	/// 1. Collect all iterators from min_heap, max_heap, and exhausted
	/// 2. Call seek_first() on each iterator
	/// 3. Valid iterators go into min_heap, exhausted ones go to exhausted list
	///
	/// # Example
	/// ```text
	/// Before:
	///   min_heap: []
	///   max_heap: [Iter0("zebra"), Iter1("yak")]  // from backward iteration
	///   exhausted: [Iter2]
	///
	/// After seek_first() on each:
	///   Iter0.seek_first() → "apple"    ✓
	///   Iter1.seek_first() → "banana"   ✓
	///   Iter2.seek_first() → (empty)    ✗
	///
	/// After init_forward():
	///   min_heap: [Iter0("apple"), Iter1("banana")]
	///   max_heap: []
	///   exhausted: [Iter2]
	/// ```
	fn init_forward(&mut self) -> Result<()> {
		self.direction = Direction::Forward;

		// Collect all entries from everywhere
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

	/// Initialize for backward iteration - position all iterators at their last element.
	///
	/// Similar to init_forward(), but:
	/// - Uses max_heap instead of min_heap
	/// - Calls seek_last() instead of seek_first()
	///
	/// # Example
	/// ```text
	/// Iter0: ["apple", "cherry", "fig"]
	///                              ↑ seek_last()
	/// Iter1: ["banana", "date"]
	///                     ↑ seek_last()
	///
	/// max_heap: [Iter0("fig"), Iter1("date")]
	/// Winner: Iter0 ("fig" > "date")
	/// ```
	fn init_backward(&mut self) -> Result<()> {
		self.direction = Direction::Backward;

		// Ensure max_heap exists (lazy initialization)
		let capacity = self.min_heap.len() + self.exhausted.len();
		if self.max_heap.is_none() {
			self.max_heap = Some(self.create_max_heap(capacity));
		}
		let max_heap = self.max_heap.as_mut().unwrap();

		// Collect all entries
		let mut all_entries: Vec<HeapEntry<'_>> = self.min_heap.drain();
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

	/// Switch from backward to forward iteration, positioning all iterators past target.
	///
	/// 1. For each child iterator: seek(target)
	/// 2. If key == target: next() once to skip past it
	/// 3. Rebuild the min_heap
	fn switch_to_forward(&mut self, target: &[u8]) -> Result<()> {
		self.direction = Direction::Forward;

		// Collect all entries from both heaps and exhausted
		let mut all_entries: Vec<HeapEntry<'_>> = self.min_heap.drain();
		if let Some(ref mut max_heap) = self.max_heap {
			all_entries.append(&mut max_heap.drain());
		}
		all_entries.append(&mut self.exhausted);

		// Seek each child to target, then next() if equal
		for mut entry in all_entries {
			// seek to first key >= target
			if entry.iter.seek(target)? {
				// If positioned exactly at target, advance past it
				if self.cmp.compare(entry.iter.key().encoded(), target) == Ordering::Equal {
					if !entry.iter.next()? {
						self.exhausted.push(entry);
						continue;
					}
				}
				// key > target, add to heap
				self.min_heap.push(entry);
			} else {
				// No keys >= target in this child
				self.exhausted.push(entry);
			}
		}
		Ok(())
	}

	/// Switch from forward to backward iteration, positioning all iterators before target.
	///
	/// 1. For each child iterator: seek(target)
	/// 2. If found: prev() to get last key < target
	/// 3. If not found (all keys < target): seek_last() to get largest key
	/// 4. Rebuild the max_heap
	///
	/// # Critical Edge Case
	/// When seek() returns false, the iterator is invalid and prev() would fail.
	/// We must use seek_last() to get the last (largest) key, which is guaranteed
	/// to be < target since seek() found no keys >= target.
	fn switch_to_backward(&mut self, target: &[u8]) -> Result<()> {
		self.direction = Direction::Backward;

		// Ensure max_heap exists (lazy initialization)
		let capacity = self.min_heap.len() + self.exhausted.len();
		if self.max_heap.is_none() {
			self.max_heap = Some(self.create_max_heap(capacity));
		}
		let max_heap = self.max_heap.as_mut().unwrap();

		// Collect all entries
		let mut all_entries: Vec<HeapEntry<'_>> = self.min_heap.drain();
		all_entries.append(&mut max_heap.drain());
		all_entries.append(&mut self.exhausted);

		for mut entry in all_entries {
			if entry.iter.seek(target)? {
				// Found key >= target, need to go before it
				// prev() moves to last key < current position
				if !entry.iter.prev()? {
					// No key before current position (exhausted backward)
					self.exhausted.push(entry);
					continue;
				}
				max_heap.push(entry);
			} else {
				// seek() returned false: ALL keys in this child are < target
				// Use seek_last() to get the largest key (which is < target)
				if entry.iter.seek_last()? {
					max_heap.push(entry);
				} else {
					// Empty iterator
					self.exhausted.push(entry);
				}
			}
		}
		Ok(())
	}

	/// Get the current heap based on direction.
	#[inline]
	fn current_heap(
		&self,
	) -> &BinaryHeap<HeapEntry<'a>, Box<dyn Fn(&HeapEntry<'a>, &HeapEntry<'a>) -> Ordering>> {
		match self.direction {
			Direction::Forward => &self.min_heap,
			Direction::Backward => self.max_heap.as_ref().unwrap(),
		}
	}

	/// Get the current heap mutably based on direction.
	#[inline]
	fn current_heap_mut(
		&mut self,
	) -> &mut BinaryHeap<HeapEntry<'a>, Box<dyn Fn(&HeapEntry<'a>, &HeapEntry<'a>) -> Ordering>> {
		match self.direction {
			Direction::Forward => &mut self.min_heap,
			Direction::Backward => self.max_heap.as_mut().unwrap(),
		}
	}

	/// Advance the current winner iterator.
	///
	/// This is the core operation of the merging iterator.
	///
	/// # Algorithm
	/// 1. Get mutable reference to heap root (the winner)
	/// 2. Advance the winner's iterator (next or prev)
	/// 3. If still valid: sift_down to restore heap property
	/// 4. If exhausted: pop from heap, move to exhausted list
	///
	/// # Example: Forward iteration
	/// ```text
	/// Before:
	///   min_heap:
	///         [Iter0 → "apple"]     ← winner (smallest)
	///        /                \
	///   [Iter1 → "banana"]  [Iter2 → "cherry"]
	///
	/// Step 1: Advance Iter0
	///   Iter0.next() → now points to "date"
	///
	/// Step 2: Heap property violated!
	///         [Iter0 → "date"]      ← "date" > "banana"!
	///        /                \
	///   [Iter1 → "banana"]  [Iter2 → "cherry"]
	///
	/// Step 3: sift_down_root()
	///         [Iter1 → "banana"]    ← restored!
	///        /                \
	///   [Iter0 → "date"]    [Iter2 → "cherry"]
	/// ```
	fn advance_winner(&mut self) -> Result<bool> {
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

	/// Check if iterator is valid (positioned on a valid element).
	#[inline]
	pub fn is_valid(&self) -> bool {
		!self.current_heap().is_empty()
	}

	/// Get current winner's key.
	///
	/// # Panics
	/// Panics if iterator is not valid. Always check `is_valid()` first.
	#[inline]
	pub fn current_key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.is_valid());
		self.current_heap().peek().unwrap().iter.key()
	}

	/// Get current winner's value.
	///
	/// # Panics
	/// Panics if iterator is not valid. Always check `is_valid()` first.
	#[inline]
	pub fn current_value(&self) -> &[u8] {
		debug_assert!(self.is_valid());
		self.current_heap().peek().unwrap().iter.value()
	}
}

impl InternalIterator for MergingIterator<'_> {
	/// Seek to the first entry with key >= target.
	///
	/// # Example
	/// ```text
	/// Iter0: ["apple", "cherry", "fig"]
	/// Iter1: ["banana", "date"]
	///
	/// seek("cat"):
	///   Iter0.seek("cat") → "cherry" (first >= "cat")
	///   Iter1.seek("cat") → "date"   (first >= "cat")
	///   
	/// min_heap: [Iter0("cherry"), Iter1("date")]
	/// Result: "cherry"
	/// ```
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

	/// Position at the first (smallest) key across all iterators.
	fn seek_first(&mut self) -> Result<bool> {
		self.init_forward()?;
		Ok(self.is_valid())
	}

	/// Position at the last (largest) key across all iterators.
	fn seek_last(&mut self) -> Result<bool> {
		self.init_backward()?;
		Ok(self.is_valid())
	}

	/// Move to the next entry (in forward direction).
	///
	/// If currently in backward mode, this switches direction.
	///
	/// # Direction Switch Example
	/// ```text
	/// Current state (backward mode):
	///   Position: "cherry"
	///   
	/// User calls next():
	///   1. Save current key: "cherry"
	///   2. switch_to_forward("cherry") - seeks all iterators to "cherry", then next()
	///   3. Result: first entry > "cherry" (e.g., "date")
	/// ```
	fn next(&mut self) -> Result<bool> {
		if !self.is_valid() {
			return Ok(false);
		}
		if self.direction != Direction::Forward {
			let current_key = self.current_key().encoded().to_vec();
			self.switch_to_forward(&current_key)?;
			return Ok(self.is_valid());
		}
		self.advance_winner()
	}

	/// Move to the previous entry (in backward direction).
	///
	/// If currently in forward mode, this switches direction.
	fn prev(&mut self) -> Result<bool> {
		if !self.is_valid() {
			return Ok(false);
		}
		if self.direction != Direction::Backward {
			let current_key = self.current_key().encoded().to_vec();
			self.switch_to_backward(&current_key)?;
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

	fn value(&self) -> &[u8] {
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

	/// Process all accumulated versions of the current key.
	///
	/// This is the heart of compaction logic. It decides:
	/// - Which versions to keep (output_versions)
	/// - Which versions to discard (delete_list, discard_stats)
	///
	/// # Decision Matrix
	///
	/// ```text
	/// ┌─────────────────┬───────────────┬────────────────┬──────────────────┐
	/// │ Scenario        │ Bottom Level? │ Versioning?    │ Action           │
	/// ├─────────────────┼───────────────┼────────────────┼──────────────────┤
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
	/// With versioning=true, retention=1hour:
	///   - seq=100: age=0, KEEP (latest)
	///   - seq=50:  age=30min < 1hour, KEEP (within retention)
	///   - seq=20:  age=2hours > 1hour, DROP (expired)
	///   
	/// Output: [seq=100, seq=50]
	/// delete_list: [seq=20]
	/// ```
	fn process_accumulated_versions(&mut self) {
		if self.accumulated_versions.is_empty() {
			return;
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

		// Process each version
		for (i, (key, value)) in self.accumulated_versions.iter().enumerate() {
			let is_hard_delete = key.is_hard_delete_marker();
			let is_replace = key.is_replace();
			let is_latest = i == 0;

			// ===== DETERMINE IF ENTRY SHOULD BE MARKED STALE =====
			// Stale entries are added to delete_list for VLog garbage collection

			let should_mark_stale = if latest_is_delete_at_bottom {
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
					// No versioning: only latest matters
					true
				} else if self.retention_period_ns > 0 {
					// Check retention period
					let current_time = self.clock.now();
					let key_timestamp = key.timestamp;
					let age = current_time - key_timestamp;
					let is_within_retention = age <= self.retention_period_ns;
					// Stale if OUTSIDE retention period
					!is_within_retention
				} else {
					// retention_period_ns == 0: keep forever
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
					let location = ValueLocation::decode(value).unwrap();
					if location.is_value_pointer() {
						let pointer = ValuePointer::decode(&location.value).unwrap();
						let value_size = pointer.total_entry_size();
						self.delete_list_batch.push((key.seq_num(), value_size));
					}
				}

				// Update per-file discard statistics
				if let Err(e) = collect_vlog_discard_stats(&mut self.discard_stats, value) {
					log::warn!("Error collecting discard stats: {e:?}");
				}
			}

			// ===== DETERMINE IF ENTRY SHOULD BE OUTPUT =====

			let should_output = if latest_is_delete_at_bottom {
				// DELETE at bottom: output NOTHING
				false
			} else if should_mark_stale {
				// Stale entries: don't output
				false
			} else if self.enable_versioning {
				// Versioning: output all non-stale versions
				true
			} else {
				// No versioning: only output latest
				is_latest
			};

			if should_output {
				self.output_versions.push((key.clone(), value.clone()));
			}
		}

		// Clear accumulated versions for the next key
		self.accumulated_versions.clear();
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
					self.process_accumulated_versions();
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
