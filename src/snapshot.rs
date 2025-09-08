use std::cmp::Ordering;
use std::ops::{Bound, RangeBounds};
use std::ptr::NonNull;
use std::sync::{atomic::AtomicU32, Arc};

use crate::error::Result;
use crate::iter::BoxedIterator;
use crate::levels::Levels;
use crate::lsm::Core;
use crate::memtable::MemTable;
use crate::sstable::{meta::KeyRange, InternalKey, InternalKeyKind};
use crate::{IterResult, Iterator as LSMIterator, INTERNAL_KEY_SEQ_NUM_MAX};
use crate::{Key, Value};

use double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt};
use interval_heap::IntervalHeap;

#[derive(Eq)]
struct HeapItem {
	key: Arc<InternalKey>,
	value: Value,
	iterator_index: usize,
}

impl PartialEq for HeapItem {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == Ordering::Equal
	}
}

impl Ord for HeapItem {
	fn cmp(&self, other: &Self) -> Ordering {
		// First compare by user key
		match self.key.user_key.cmp(&other.key.user_key) {
			Ordering::Equal => {
				// Same user key, compare by sequence number in DESCENDING order
				// (higher sequence number = more recent)
				match other.key.seq_num().cmp(&self.key.seq_num()) {
					Ordering::Equal => self.iterator_index.cmp(&other.iterator_index),
					ord => ord,
				}
			}
			ord => ord, // Different user keys
		}
	}
}

impl PartialOrd for HeapItem {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

// ===== Snapshot Counter =====
/// Tracks the number of active snapshots in the system.
///
/// Important for garbage collection: old versions can only be removed
/// when no snapshot needs them. This counter helps determine when it's
/// safe to compact away old versions during compaction.
///
/// TODO: This check needs to be implemented in the compaction logic.
#[derive(Clone, Debug, Default)]
pub(crate) struct Counter(Arc<AtomicU32>);

impl std::ops::Deref for Counter {
	type Target = Arc<AtomicU32>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Counter {
	/// Increments when a new snapshot is created
	pub(crate) fn increment(&self) -> u32 {
		self.fetch_add(1, std::sync::atomic::Ordering::Release)
	}

	/// Decrements when a snapshot is dropped
	pub(crate) fn decrement(&self) -> u32 {
		self.fetch_sub(1, std::sync::atomic::Ordering::Release)
	}
}

// ===== Iterator State =====
/// Holds references to all LSM tree components needed for iteration.
pub(crate) struct IterState {
	/// The active memtable receiving current writes
	pub active: Arc<MemTable>,
	/// Immutable memtables waiting to be flushed
	pub immutable: Vec<Arc<MemTable>>,
	/// All levels containing SSTables
	pub levels: Levels,
}

// ===== Snapshot Implementation =====
/// A consistent point-in-time view of the LSM tree.
///
/// # Snapshot Isolation in LSM Trees
///
/// Snapshots provide consistent reads by fixing a sequence number at creation time.
/// All reads through the snapshot only see data with sequence numbers less than
/// or equal to the snapshot's sequence number.
#[derive(Clone)]
pub(crate) struct Snapshot {
	/// Reference to the LSM tree core
	core: Arc<Core>,

	/// Sequence number defining this snapshot's view of the data
	/// Only data with seq_num <= this value is visible
	seq_num: u64,
}

impl Snapshot {
	/// Creates a new snapshot at the current sequence number
	pub(crate) fn new(core: Arc<Core>, seq_num: u64) -> Self {
		// Increment counter so compaction knows to preserve old versions
		core.snapshot_counter.increment();
		Self {
			core,
			seq_num,
		}
	}

	/// Gets a single key from the snapshot.
	///
	/// # Read Path in LSM Trees
	///
	/// The read path checks multiple locations in order:
	/// 1. **Active Memtable**: Most recent writes, in memory
	/// 2. **Immutable Memtables**: Recent writes being flushed
	/// 3. **Level**: From SSTables
	///
	/// The search stops at the first version found with seq_num <= snapshot seq_num.
	pub(crate) fn get(&self, key: &[u8]) -> crate::Result<Option<(Value, u64)>> {
		// self.core.get_internal(key, self.seq_num)
		// Read lock on the active memtable
		let memtable_lock = self.core.active_memtable.read().unwrap();

		// Check the active memtable for the key
		if let Some(item) = memtable_lock.get(key.as_ref(), Some(self.seq_num)) {
			if item.0.is_tombstone() {
				return Ok(None); // Key is a tombstone, return None
			}
			return Ok(Some((item.1, item.0.seq_num()))); // Key found, return the value
		}
		drop(memtable_lock); // Release the lock on the active memtable

		// Read lock on the immutable memtables
		let memtable_lock = self.core.immutable_memtables.read().unwrap();

		// Check the immutable memtables for the key
		for (_, memtable) in memtable_lock.iter().rev() {
			if let Some(item) = memtable.get(key.as_ref(), Some(self.seq_num)) {
				if item.0.is_tombstone() {
					return Ok(None); // Key is a tombstone, return None
				}
				return Ok(Some((item.1, item.0.seq_num()))); // Key found, return the value
			}
		}
		drop(memtable_lock); // Release the lock on the immutable memtables

		// Read lock on the level manifest
		if let Some(ref level_manifest) = self.core.level_manifest {
			let level_manifest = level_manifest.read().unwrap();

			// Check the tables in each level for the key
			for level in &level_manifest.levels {
				for table in level.tables.iter() {
					let ikey =
						InternalKey::new(key.as_ref().to_vec(), self.seq_num, InternalKeyKind::Set);

					if !table.is_key_in_key_range(&ikey) {
						continue; // Skip this table if the key is not in its range
					}

					let maybe_item = table.get(ikey)?;

					if let Some(item) = maybe_item {
						let ikey = &item.0;
						if ikey.is_tombstone() {
							return Ok(None); // Key is a tombstone, return None
						}
						return Ok(Some((item.1, ikey.seq_num()))); // Key found, return the value
					}
				}
			}
		}

		Ok(None) // Key not found in any memtable or table, return None
	}

	/// Creates an iterator for a range scan within the snapshot
	pub(crate) fn range<K: AsRef<[u8]>>(
		&self,
		start: K,
		end: K,
		keys_only: bool,
	) -> Result<impl DoubleEndedIterator<Item = IterResult>> {
		// Create a range from start (inclusive) to end (inclusive)
		let range = start.as_ref().to_vec()..=end.as_ref().to_vec();
		SnapshotIterator::new_from(self.core.clone(), self.seq_num, range, keys_only)
	}
}

impl Drop for Snapshot {
	fn drop(&mut self) {
		// Decrement counter so compaction can clean up old versions
		self.core.snapshot_counter.decrement();
	}
}

/// A merge iterator that sorts by key+seqno.
pub(crate) struct KMergeIterator<'a> {
	// Owned state
	_iter_state: Box<IterState>,

	/// Array of iterators to merge over
	iterators: NonNull<Vec<BoxedIterator<'a>>>,

	/// Interval heap of items ordered by their current key
	heap: IntervalHeap<HeapItem>,

	/// Range bounds for filtering
	range_end: Bound<Vec<u8>>,

	/// Whether the iterator has been initialized for forward iteration
	initialized_lo: bool,

	/// Whether the iterator has been initialized for backward iteration
	initialized_hi: bool,
}

impl<'a> KMergeIterator<'a> {
	fn new_from(
		iter_state: IterState,
		snapshot_seq_num: u64,
		range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
	) -> Self {
		let boxed_state = Box::new(iter_state);
		let mut iterators: Vec<BoxedIterator<'a>> = Vec::new();

		unsafe {
			let state_ref: &'a IterState = &*(&*boxed_state as *const IterState);
			// Compute query key range for table overlap checks
			let query_range = {
				let low = match &range.0 {
					Bound::Included(v) | Bound::Excluded(v) => v.clone(),
					Bound::Unbounded => Vec::new(),
				};
				let high = match &range.1 {
					Bound::Included(v) | Bound::Excluded(v) => v.clone(),
					Bound::Unbounded => vec![0xff],
				};
				KeyRange::new((Arc::from(low), Arc::from(high)))
			};

			// Active memtable with range
			let active_iter = state_ref.active.range(range.clone()).filter(move |item| {
				// Filter out items that are not visible in this snapshot
				item.0.seq_num() <= snapshot_seq_num
			});
			iterators.push(Box::new(active_iter));

			// Immutable memtables with range
			for memtable in &state_ref.immutable {
				let iter = memtable.range(range.clone()).filter(move |item| {
					// Filter out items that are not visible in this snapshot
					item.0.seq_num() <= snapshot_seq_num
				});
				iterators.push(Box::new(iter));
			}

			// Tables - these have native seek support
			for level in &state_ref.levels {
				for table in &level.tables {
					if !table.overlaps_with_range(&query_range) {
						continue;
					}
					let mut table_iter = table.iter();
					// Seek to start if bounded
					match &range.0 {
						Bound::Included(key) => {
							let ikey = InternalKey::new(
								key.clone(),
								INTERNAL_KEY_SEQ_NUM_MAX,
								InternalKeyKind::Max,
							);
							table_iter.seek(&ikey.encode());
						}
						Bound::Excluded(key) => {
							let ikey = InternalKey::new(key.clone(), 0, InternalKeyKind::Set);
							table_iter.seek(&ikey.encode());
							// If we're at the excluded key, advance once
							if table_iter.valid() {
								let current = &table_iter.key();
								if current.user_key.as_ref() == key.as_slice() {
									table_iter.advance();
								}
							}
						}
						Bound::Unbounded => {
							table_iter.seek_to_first();
						}
					}

					if table_iter.valid() {
						iterators.push(Box::new(table_iter.filter(move |item| {
							// Filter out items that are not visible in this snapshot
							item.0.seq_num() <= snapshot_seq_num
						})));
					}
				}
			}
		}

		let heap = IntervalHeap::with_capacity(iterators.len());

		// Box the iterators and get a raw pointer
		let boxed_iterators = Box::new(iterators);
		let iterators_ptr = NonNull::new(Box::into_raw(boxed_iterators))
			.expect("Box::into_raw should never return null");

		Self {
			_iter_state: boxed_state,
			iterators: iterators_ptr,
			heap,
			range_end: range.1,
			initialized_lo: false,
			initialized_hi: false,
		}
	}

	fn initialize_lo(&mut self) {
		// Pull the first item from each iterator and add to heap
		unsafe {
			let iterators = self.iterators.as_mut();
			for (idx, iter) in iterators.iter_mut().enumerate() {
				if let Some((key, value)) = iter.next() {
					self.heap.push(HeapItem {
						key,
						value,
						iterator_index: idx,
					});
				}
			}
		}
		self.initialized_lo = true;
	}

	fn initialize_hi(&mut self) {
		// Pull the last item from each iterator and add to heap
		unsafe {
			let iterators = self.iterators.as_mut();
			for (idx, iter) in iterators.iter_mut().enumerate() {
				if let Some((key, value)) = iter.next_back() {
					self.heap.push(HeapItem {
						key,
						value,
						iterator_index: idx,
					});
				}
			}
		}
		self.initialized_hi = true;
	}
}

impl Drop for KMergeIterator<'_> {
	fn drop(&mut self) {
		// Must drop the iterators before iter_state
		// because the iterators contain references to iter_state
		unsafe {
			let _ = Box::from_raw(self.iterators.as_ptr());
		}
		// iter_state is dropped automatically after this
	}
}

impl Iterator for KMergeIterator<'_> {
	type Item = (Arc<InternalKey>, Value);

	fn next(&mut self) -> Option<Self::Item> {
		if !self.initialized_lo {
			self.initialize_lo();
		}

		loop {
			if self.heap.is_empty() {
				return None;
			}

			// Get the smallest item from the heap
			let heap_item = self.heap.pop_min()?;

			// Pull the next item from the same iterator and add back to heap if valid
			unsafe {
				let iterators = self.iterators.as_mut();
				if let Some((key, value)) = iterators[heap_item.iterator_index].next() {
					self.heap.push(HeapItem {
						key,
						value,
						iterator_index: heap_item.iterator_index,
					});
				}
			}

			// Check if the key exceeds the range end
			let user_key = &heap_item.key.user_key;
			match &self.range_end {
				Bound::Included(end) => {
					if user_key.as_ref() > end.as_slice() {
						continue;
					}
				}
				Bound::Excluded(end) => {
					if user_key.as_ref() >= end.as_slice() {
						continue;
					}
				}
				Bound::Unbounded => {
					// No end bound to check
				}
			}

			// Return the key-value pair
			return Some((heap_item.key, heap_item.value));
		}
	}
}

impl DoubleEndedIterator for KMergeIterator<'_> {
	fn next_back(&mut self) -> Option<Self::Item> {
		if !self.initialized_hi {
			self.initialize_hi();
		}

		loop {
			if self.heap.is_empty() {
				return None;
			}

			// Get the largest item from the heap
			let heap_item = self.heap.pop_max()?;

			// Pull the previous item from the same iterator and add back to heap if valid
			unsafe {
				let iterators = self.iterators.as_mut();
				if let Some((key, value)) = iterators[heap_item.iterator_index].next_back() {
					self.heap.push(HeapItem {
						key,
						value,
						iterator_index: heap_item.iterator_index,
					});
				}
			}

			// Check if the key exceeds the range end
			let user_key = &heap_item.key.user_key;
			match &self.range_end {
				Bound::Included(end) => {
					if user_key.as_ref() > end.as_slice() {
						continue;
					}
				}
				Bound::Excluded(end) => {
					if user_key.as_ref() >= end.as_slice() {
						continue;
					}
				}
				Bound::Unbounded => {
					// No end bound to check
				}
			}

			// Return the key-value pair
			return Some((heap_item.key, heap_item.value));
		}
	}
}

/// Consumes a stream of KVs and emits a new stream to the filter rules
pub struct FilterIter<I: DoubleEndedIterator<Item = (Arc<InternalKey>, Value)>> {
	inner: DoubleEndedPeekable<I>,
}

impl<I: DoubleEndedIterator<Item = (Arc<InternalKey>, Value)>> FilterIter<I> {
	pub fn new(iter: I) -> Self {
		let iter = iter.double_ended_peekable();
		Self {
			inner: iter,
		}
	}

	fn skip_to_latest(&mut self, key: &Key) -> Result<()> {
		loop {
			let Some(next) = self.inner.peek() else {
				return Ok(());
			};

			// Consume version
			if next.0.user_key == *key {
				self.inner.next().expect("should not be empty");
			} else {
				return Ok(());
			}
		}
	}
}

impl<I: DoubleEndedIterator<Item = (Arc<InternalKey>, Value)>> Iterator for FilterIter<I> {
	type Item = (Arc<InternalKey>, Value);

	fn next(&mut self) -> Option<Self::Item> {
		let head = self.inner.next()?;

		// Keep only latest version of the key
		let _ = self.skip_to_latest(&head.0.user_key);

		Some(head)
	}
}

impl<I: DoubleEndedIterator<Item = (Arc<InternalKey>, Value)>> DoubleEndedIterator
	for FilterIter<I>
{
	fn next_back(&mut self) -> Option<Self::Item> {
		loop {
			let tail = self.inner.next_back()?;

			let prev = match self.inner.peek_back() {
				Some(prev) => prev,
				None => {
					return Some(tail);
				}
			};

			if prev.0.user_key < tail.0.user_key {
				return Some(tail);
			}
		}
	}
}

pub(crate) struct SnapshotIterator<'a> {
	// The complete pipeline: Data Sources → KMergeIterator → FilterIter → .filter()
	pipeline: Box<dyn DoubleEndedIterator<Item = (Arc<InternalKey>, Value)> + 'a>,
	core: Arc<Core>,
	/// When true, only return keys without resolving values
	keys_only: bool,
}

impl SnapshotIterator<'_> {
	/// Creates a new iterator over a specific key range
	fn new_from<R>(core: Arc<Core>, seq_num: u64, range: R, keys_only: bool) -> Result<Self>
	where
		R: RangeBounds<Vec<u8>>,
	{
		// Collect iterator from active memtable
		let active = guardian::ArcRwLockReadGuardian::take(core.active_memtable.clone()).unwrap();

		// Collect iterators from immutable memtables
		let immutable =
			guardian::ArcRwLockReadGuardian::take(core.immutable_memtables.clone()).unwrap();

		// Collect iterators from all tables in all levels
		let iter_state = if let Some(ref level_manifest) = core.level_manifest {
			let manifest = guardian::ArcRwLockReadGuardian::take(level_manifest.clone()).unwrap();
			IterState {
				active: active.clone(),
				immutable: immutable.iter().map(|(_, mt)| mt.clone()).collect(),
				levels: manifest.levels.clone(),
			}
		} else {
			// In memory-only mode, only use memtables
			IterState {
				active: active.clone(),
				immutable: immutable.iter().map(|(_, mt)| mt.clone()).collect(),
				levels: Levels(vec![]), // Empty levels
			}
		};

		// Convert bounds to owned for passing to merge iterator
		let start = range.start_bound().map(|v| v.clone());
		let end = range.end_bound().map(|v| v.clone());

		// Increment VLog iterator count
		if let Some(ref vlog) = core.vlog {
			vlog.incr_iterator_count();
		}

		// Build the pipeline: Data Sources → KMergeIterator → FilterIter → .filter()
		let merge_iter = KMergeIterator::new_from(iter_state, seq_num, (start, end));
		let filter_iter = FilterIter::new(merge_iter);
		let pipeline =
			Box::new(filter_iter.filter(|(key, _value)| key.kind() != InternalKeyKind::Delete));

		Ok(Self {
			pipeline,
			core: core.clone(),
			keys_only,
		})
	}
}

impl Iterator for SnapshotIterator<'_> {
	type Item = IterResult;

	fn next(&mut self) -> Option<Self::Item> {
		if let Some((key, value)) = self.pipeline.next() {
			if self.keys_only {
				// For keys-only mode, return None for the value to avoid allocations
				Some(Ok((key.user_key.clone(), None)))
			} else {
				// Resolve value pointers to actual values
				match self.core.resolve_value(&value) {
					Ok(resolved_value) => Some(Ok((key.user_key.clone(), Some(resolved_value)))),
					Err(e) => Some(Err(e)),
				}
			}
		} else {
			None
		}
	}
}

impl DoubleEndedIterator for SnapshotIterator<'_> {
	fn next_back(&mut self) -> Option<Self::Item> {
		if let Some((key, value)) = self.pipeline.next_back() {
			if self.keys_only {
				// For keys-only mode, return None for the value to avoid allocations
				Some(Ok((key.user_key.clone(), None)))
			} else {
				// Resolve value pointers to actual values
				match self.core.resolve_value(&value) {
					Ok(resolved_value) => Some(Ok((key.user_key.clone(), Some(resolved_value)))),
					Err(e) => Some(Err(e)),
				}
			}
		} else {
			None
		}
	}
}

impl Drop for SnapshotIterator<'_> {
	fn drop(&mut self) {
		// Decrement VLog iterator count when iterator is dropped
		if let Some(ref vlog) = self.core.vlog {
			if let Err(e) = vlog.decr_iterator_count() {
				eprintln!("Warning: Failed to decrement VLog iterator count: {e}");
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::{Options, Tree};
	use std::collections::HashSet;
	use std::sync::Arc;

	use super::{IterState, KMergeIterator};
	use crate::levels::Level;
	use crate::levels::Levels;
	use crate::memtable::MemTable;
	use crate::sstable::table::{Table, TableWriter};
	use crate::sstable::{InternalKey, InternalKeyKind};
	use crate::vfs::File;
	use std::ops::Bound;

	use tempdir::TempDir;

	fn create_temp_directory() -> TempDir {
		TempDir::new("test").unwrap()
	}

	// Common setup logic for creating a store
	fn create_store() -> (Tree, TempDir) {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = Options {
			path: path.clone(),
			..Default::default()
		};

		let tree = Tree::new(Arc::new(opts)).unwrap();
		(tree, temp_dir)
	}

	#[tokio::test]
	async fn test_empty_snapshot() {
		let (store, _temp_dir) = create_store();

		// Create a transaction without any data
		let tx = store.begin().unwrap();

		// Range scan should return empty
		let range: Vec<_> =
			tx.range(b"a", b"z", None).unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert!(range.is_empty());
	}

	#[tokio::test]
	async fn test_basic_snapshot_visibility() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.commit().await.unwrap();
		}

		// Start a read transaction (captures snapshot)
		let read_tx = store.begin().unwrap();

		// Insert more data in a new transaction
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key4", b"value4").unwrap();
			tx.commit().await.unwrap();
		}

		// The read transaction should only see the initial data
		let range: Vec<_> =
			read_tx.range(b"key0", b"key9", None).unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(range.len(), 2);
		assert_eq!(range[0].0.as_ref(), b"key1");
		assert_eq!(range[1].0.as_ref(), b"key2");
	}

	#[tokio::test]
	async fn test_snapshot_isolation_with_updates() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1_v1").unwrap();
			tx.set(b"key2", b"value2_v1").unwrap();
			tx.commit().await.unwrap();
		}

		// Start a read transaction
		let read_tx = store.begin().unwrap();

		// Update the data in a new transaction
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1_v2").unwrap();
			tx.set(b"key2", b"value2_v2").unwrap();
			tx.commit().await.unwrap();
		}

		// The read transaction should see the old values
		let range: Vec<_> =
			read_tx.range(b"key0", b"key9", None).unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(range.len(), 2);
		assert_eq!(range[0].1.as_ref().unwrap().as_ref(), b"value1_v1");
		assert_eq!(range[1].1.as_ref().unwrap().as_ref(), b"value2_v1");

		// A new transaction should see the updated values
		let new_tx = store.begin().unwrap();
		let range: Vec<_> =
			new_tx.range(b"key0", b"key9", None).unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(range.len(), 2);
		assert_eq!(range[0].1.as_ref().unwrap().as_ref(), b"value1_v2");
		assert_eq!(range[1].1.as_ref().unwrap().as_ref(), b"value2_v2");
	}

	#[tokio::test]
	async fn test_tombstone_handling() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.commit().await.unwrap();
		}

		// Start a read transaction
		let read_tx1 = store.begin().unwrap();

		// Delete key2
		{
			let mut tx = store.begin().unwrap();
			tx.delete(b"key2").unwrap();
			tx.commit().await.unwrap();
		}

		// The first read transaction should still see all three keys
		let range: Vec<_> =
			read_tx1.range(b"key0", b"key9", None).unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(range.len(), 3);
		assert_eq!(range[0].0.as_ref(), b"key1");
		assert_eq!(range[1].0.as_ref(), b"key2");
		assert_eq!(range[2].0.as_ref(), b"key3");

		// A new transaction should not see the deleted key
		let read_tx2 = store.begin().unwrap();
		let range: Vec<_> =
			read_tx2.range(b"key0", b"key9", None).unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(range.len(), 2);
		assert_eq!(range[0].0.as_ref(), b"key1");
		assert_eq!(range[1].0.as_ref(), b"key3");
	}

	#[tokio::test]
	async fn test_version_resolution() {
		let (store, _temp_dir) = create_store();

		// Insert multiple versions of the same key
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"version1").unwrap();
			tx.commit().await.unwrap();
		}

		let snapshot1 = store.begin().unwrap();

		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"version2").unwrap();
			tx.commit().await.unwrap();
		}

		let snapshot2 = store.begin().unwrap();

		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"version3").unwrap();
			tx.commit().await.unwrap();
		}

		let snapshot3 = store.begin().unwrap();

		// Each snapshot should see its corresponding version
		let value1 = snapshot1.get(b"key1").unwrap().unwrap();
		assert_eq!(value1.as_ref(), b"version1");

		let value2 = snapshot2.get(b"key1").unwrap().unwrap();
		assert_eq!(value2.as_ref(), b"version2");

		let value3 = snapshot3.get(b"key1").unwrap().unwrap();
		assert_eq!(value3.as_ref(), b"version3");
	}

	#[tokio::test]
	async fn test_range_with_random_operations() {
		let (store, _temp_dir) = create_store();

		// Initial data
		{
			let mut tx = store.begin().unwrap();
			for i in 1..=10 {
				let key = format!("key{i:02}");
				let value = format!("value{i}");
				tx.set(key.as_bytes(), value.as_bytes()).unwrap();
			}
			tx.commit().await.unwrap();
		}

		let snapshot1 = store.begin().unwrap();

		// Update some keys and delete others
		{
			let mut tx = store.begin().unwrap();
			// Update even keys
			for i in (2..=10).step_by(2) {
				let key = format!("key{i:02}");
				let value = format!("value{i}_updated");
				tx.set(key.as_bytes(), value.as_bytes()).unwrap();
			}
			// Delete keys 3, 6, 9
			tx.delete(b"key03").unwrap();
			tx.delete(b"key06").unwrap();
			tx.delete(b"key09").unwrap();
			tx.commit().await.unwrap();
		}

		let snapshot2 = store.begin().unwrap();

		// Snapshot1 should see all original data
		let range1: Vec<_> = snapshot1
			.range(b"key00", b"key99", None)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();

		assert_eq!(range1.len(), 10);
		for (i, item) in range1.iter().enumerate() {
			let (key, value) = item;
			let expected_key = format!("key{:02}", i + 1);
			let expected_value = format!("value{}", i + 1);
			assert_eq!(key.as_ref(), expected_key.as_bytes());
			assert_eq!(value.as_ref().unwrap().as_ref(), expected_value.as_bytes());
		}

		// Snapshot2 should see updated data with deletions
		let range2: Vec<_> = snapshot2
			.range(b"key00", b"key99", None)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();

		assert_eq!(range2.len(), 7); // 10 - 3 deleted

		// Check that deleted keys are not present
		let keys: HashSet<_> = range2.iter().map(|item| item.0.as_ref()).collect();
		assert!(!keys.contains(&b"key03".as_ref()));
		assert!(!keys.contains(&b"key06".as_ref()));
		assert!(!keys.contains(&b"key09".as_ref()));

		// Check that even keys are updated
		for item in &range2 {
			let (key, value) = item;
			let key_str = String::from_utf8_lossy(key.as_ref());
			if let Ok(num) = key_str.trim_start_matches("key").parse::<i32>() {
				if num % 2 == 0 {
					let expected_value = format!("value{num}_updated");
					assert_eq!(value.as_ref().unwrap().as_ref(), expected_value.as_bytes());
				}
			}
		}
	}

	#[tokio::test]
	async fn test_concurrent_snapshots() {
		let (store, _temp_dir) = create_store();

		// Initial state
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"counter", b"0").unwrap();
			tx.commit().await.unwrap();
		}

		// Create multiple snapshots at different points
		let mut snapshots = Vec::new();

		for i in 1..=5 {
			// Take a snapshot
			snapshots.push(store.begin().unwrap());

			// Update the counter
			{
				let mut tx = store.begin().unwrap();
				tx.set(b"counter", i.to_string().as_bytes()).unwrap();
				tx.commit().await.unwrap();
			}
		}

		// Each snapshot should see its corresponding counter value
		for (i, snapshot) in snapshots.iter().enumerate() {
			let value = snapshot.get(b"counter").unwrap().unwrap();
			let expected = i.to_string();
			assert_eq!(value.as_ref(), expected.as_bytes());
		}
	}

	#[tokio::test]
	async fn test_snapshot_with_complex_key_patterns() {
		let (store, _temp_dir) = create_store();

		// Insert data with different key patterns
		{
			let mut tx = store.begin().unwrap();
			// Numeric keys
			for i in 0..10 {
				let key = format!("{i:03}");
				tx.set(key.as_bytes(), b"numeric").unwrap();
			}
			// Alpha keys
			for c in 'a'..='j' {
				tx.set(c.to_string().as_bytes(), b"alpha").unwrap();
			}
			// Mixed keys
			for i in 0..5 {
				let key = format!("mix{i}key");
				tx.set(key.as_bytes(), b"mixed").unwrap();
			}
			tx.commit().await.unwrap();
		}

		let snapshot = store.begin().unwrap();

		// Test different range queries
		let numeric_range: Vec<_> =
			snapshot.range(b"000", b"999", None).unwrap().collect::<Vec<_>>();
		assert_eq!(numeric_range.len(), 10);

		let alpha_range: Vec<_> = snapshot.range(b"a", b"z", None).unwrap().collect::<Vec<_>>();
		assert_eq!(alpha_range.len(), 15); // 10 numeric + 10 alpha + 5 mixed

		let mixed_range: Vec<_> = snapshot.range(b"mix", b"miy", None).unwrap().collect::<Vec<_>>();
		assert_eq!(mixed_range.len(), 5);
	}

	#[tokio::test]
	async fn test_snapshot_ordering_invariants() {
		let (store, _temp_dir) = create_store();

		// Insert data in random order
		{
			let mut tx = store.begin().unwrap();
			let keys = vec![
				"key05", "key01", "key09", "key03", "key07", "key02", "key08", "key04", "key06",
			];
			for key in keys {
				tx.set(key.as_bytes(), key.as_bytes()).unwrap();
			}
			tx.commit().await.unwrap();
		}

		let snapshot = store.begin().unwrap();

		// Range scan should return keys in sorted order
		let range: Vec<_> = snapshot
			.range(b"key00", b"key99", None)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();

		assert_eq!(range.len(), 9);

		// Verify ordering
		for i in 1..range.len() {
			assert!(range[i - 1].0.as_ref() < range[i].0.as_ref());
		}

		// Verify no duplicates
		let keys: HashSet<_> = range.iter().map(|item| item.0.as_ref()).collect();
		assert_eq!(keys.len(), range.len());
	}

	#[tokio::test]
	async fn test_snapshot_keys_only() {
		let (store, _temp_dir) = create_store();

		// Insert test data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key4", b"value4").unwrap();
			tx.set(b"key5", b"value5").unwrap();
			tx.commit().await.unwrap();
		}

		// Create snapshot via transaction
		let tx = store.begin().unwrap();
		let snapshot = tx.snapshot.as_ref().unwrap();

		// Get keys only
		let keys_only_iter = snapshot.range(b"key1", b"key5", true).unwrap();
		let keys_only: Vec<_> = keys_only_iter.collect::<Result<Vec<_>, _>>().unwrap();

		// Verify we got all 5 keys
		assert_eq!(keys_only.len(), 5);

		// Check keys are correct and values are None
		for (i, (key, value)) in keys_only.iter().enumerate().take(5) {
			let expected_key = format!("key{}", i + 1);
			assert_eq!(key.as_ref(), expected_key.as_bytes());

			// Values should be None for keys-only scan
			assert!(value.is_none(), "Value should be None for keys-only scan");
		}

		// Compare with regular range scan
		let regular_range_iter = snapshot.range(b"key1", b"key5", false).unwrap();
		let regular_range: Vec<_> = regular_range_iter.collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(regular_range.len(), keys_only.len());

		// Keys should match but values should be different
		for i in 0..keys_only.len() {
			// Keys should be identical
			assert_eq!(keys_only[i].0, regular_range[i].0, "Keys should match");

			// Regular range should have actual values
			let expected_value = format!("value{}", i + 1);
			assert!(regular_range[i].1.is_some(), "Regular range should have values");
			assert_eq!(
				regular_range[i].1.as_ref().unwrap().as_ref(),
				expected_value.as_bytes(),
				"Regular range should have correct values"
			);
		}
	}

	#[test]
	fn test_range_skips_non_overlapping_tables() {
		fn build_table(data: Vec<(&'static [u8], &'static [u8])>) -> Arc<Table> {
			let opts = Arc::new(Options::new());
			let mut buf = Vec::new();
			{
				let mut w = TableWriter::new(&mut buf, 0, opts.clone());
				for (k, v) in data {
					let ikey = InternalKey::new(k.to_vec(), 1, InternalKeyKind::Set);
					w.add(ikey.into(), v).unwrap();
				}
				w.finish().unwrap();
			}
			let size = buf.len();
			let file = Arc::new(buf) as Arc<dyn File>;
			Arc::new(Table::new(1, opts, file, size as u64).unwrap())
		}

		// Build two tables with disjoint key ranges
		let table1 = build_table(vec![(b"a1", b"v1"), (b"a2", b"v2")]);
		let table2 = build_table(vec![(b"z1", b"v3"), (b"z2", b"v4")]);

		let mut level0 = Level::with_capacity(10);
		level0.insert(table1);
		level0.insert(table2.clone());

		let levels = Levels(vec![Arc::new(level0)]);

		let iter_state = IterState {
			active: Arc::new(MemTable::new()),
			immutable: Vec::new(),
			levels,
		};

		// Range that only overlaps with table2
		let range = (Bound::Included(b"z0".to_vec()), Bound::Included(b"zz".to_vec()));

		let merge_iter = KMergeIterator::new_from(iter_state, 1, range);

		let items: Vec<_> = merge_iter.collect();
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].0.user_key.as_ref(), b"z1");
		assert_eq!(items[1].0.user_key.as_ref(), b"z2");
	}

	#[tokio::test]
	async fn test_double_ended_iteration() {
		let (store, _temp_dir) = create_store();

		// Insert test data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key4", b"value4").unwrap();
			tx.set(b"key5", b"value5").unwrap();
			tx.commit().await.unwrap();
		}

		// Create snapshot via transaction
		let tx = store.begin().unwrap();
		let snapshot = tx.snapshot.as_ref().unwrap();

		// Test forward iteration
		let forward_iter = snapshot.range(b"key1", b"key5", false).unwrap();
		let forward_items: Vec<_> = forward_iter.collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(forward_items.len(), 5);
		assert_eq!(forward_items[0].0.as_ref(), b"key1");
		assert_eq!(forward_items[4].0.as_ref(), b"key5");

		// Test backward iteration
		let backward_iter = snapshot.range(b"key1", b"key5", false).unwrap();
		let backward_items: Vec<_> = backward_iter.rev().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(backward_items.len(), 5);
		assert_eq!(backward_items[0].0.as_ref(), b"key5");
		assert_eq!(backward_items[4].0.as_ref(), b"key1");

		// Verify both iterations produce the same items in reverse order
		for i in 0..5 {
			assert_eq!(forward_items[i].0, backward_items[4 - i].0);
		}
	}

	#[tokio::test]
	async fn test_double_ended_iteration_with_tombstones() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.commit().await.unwrap();
		}

		// Take a snapshot before deletion
		let snapshot1 = store.begin().unwrap();

		// Delete key2
		{
			let mut tx = store.begin().unwrap();
			tx.delete(b"key2").unwrap();
			tx.commit().await.unwrap();
		}

		// Take another snapshot after deletion
		let snapshot2 = store.begin().unwrap();

		// Test forward iteration on first snapshot (should see all keys)
		let snapshot1_ref = snapshot1.snapshot.as_ref().unwrap();
		let forward_iter1 = snapshot1_ref.range(b"key1", b"key3", false).unwrap();
		let forward_items1: Vec<_> = forward_iter1.collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(forward_items1.len(), 3);

		// Test backward iteration on first snapshot
		let backward_iter1 = snapshot1_ref.range(b"key1", b"key3", false).unwrap();
		let backward_items1: Vec<_> = backward_iter1.rev().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(backward_items1.len(), 3);

		// Test forward iteration on second snapshot (should not see deleted key)
		let snapshot2_ref = snapshot2.snapshot.as_ref().unwrap();
		let forward_iter2 = snapshot2_ref.range(b"key1", b"key3", false).unwrap();
		let forward_items2: Vec<_> = forward_iter2.collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(forward_items2.len(), 2);

		// Test backward iteration on second snapshot
		let backward_iter2 = snapshot2_ref.range(b"key1", b"key3", false).unwrap();
		let backward_items2: Vec<_> = backward_iter2.rev().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(backward_items2.len(), 2);

		// Verify both iterations produce the same items in reverse order
		for i in 0..forward_items2.len() {
			assert_eq!(forward_items2[i].0, backward_items2[forward_items2.len() - 1 - i].0);
		}
	}

	#[tokio::test]
	async fn test_double_ended_iteration_mixed_operations() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			for i in 1..=10 {
				let key = format!("key{i:02}");
				let value = format!("value{i}");
				tx.set(key.as_bytes(), value.as_bytes()).unwrap();
			}
			tx.commit().await.unwrap();
		}

		// Take a snapshot
		let snapshot = store.begin().unwrap();

		// Test forward iteration
		let snapshot_ref = snapshot.snapshot.as_ref().unwrap();
		let forward_iter = snapshot_ref.range(b"key01", b"key10", false).unwrap();
		let forward_items: Vec<_> = forward_iter.collect::<Result<Vec<_>, _>>().unwrap();

		// Test backward iteration
		let backward_iter = snapshot_ref.range(b"key01", b"key10", false).unwrap();
		let backward_items: Vec<_> = backward_iter.rev().collect::<Result<Vec<_>, _>>().unwrap();

		// Both should have 10 items
		assert_eq!(forward_items.len(), 10);
		assert_eq!(backward_items.len(), 10);

		// Verify ordering
		for i in 1..=10 {
			let expected_key = format!("key{i:02}");
			assert_eq!(forward_items[i - 1].0.as_ref(), expected_key.as_bytes());
			assert_eq!(backward_items[10 - i].0.as_ref(), expected_key.as_bytes());
		}

		// Verify both iterations produce the same items in reverse order
		for i in 0..10 {
			assert_eq!(forward_items[i].0, backward_items[9 - i].0);
		}
	}
}
