use std::collections::BinaryHeap;
use std::ops::{Bound, RangeBounds};
use std::ptr::NonNull;
use std::sync::{atomic::AtomicU32, Arc};

use crate::error::Result;
use crate::iter::{BoxedIterator, HeapItem};
use crate::levels::Levels;
use crate::lsm::Core;
use crate::memtable::MemTable;
use crate::sstable::{meta::KeyRange, InternalKey, InternalKeyKind};
use crate::{IterResult, Iterator as LSMIterator, INTERNAL_KEY_SEQ_NUM_MAX};
use crate::{Key, Value};

// ===== Snapshot Counter =====
/// Tracks the number of active snapshots in the system.
///
/// Important for garbage collection: old versions can only be removed
/// when no snapshot needs them. This counter helps determine when it's
/// safe to compact away old versions during compaction.
///
/// TODO: This check needs to be implemented in the compaction logic.
#[derive(Clone, Debug, Default)]
pub struct Counter(Arc<AtomicU32>);

impl std::ops::Deref for Counter {
	type Target = Arc<AtomicU32>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Counter {
	/// Increments when a new snapshot is created
	pub fn increment(&self) -> u32 {
		self.fetch_add(1, std::sync::atomic::Ordering::Release)
	}

	/// Decrements when a snapshot is dropped
	pub fn decrement(&self) -> u32 {
		self.fetch_sub(1, std::sync::atomic::Ordering::Release)
	}
}

// ===== Iterator State =====
/// Holds references to all LSM tree components needed for iteration.
pub struct IterState {
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
pub struct Snapshot {
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
	pub fn get(&self, key: &[u8]) -> crate::Result<Option<(Value, u64)>> {
		// self.core.get_internal(key, self.seq_num)
		// Read lock on the active memtable
		let memtable_lock = self.core.active_memtable.read().unwrap();

		// Check the active memtable for the key
		if let Some(item) = memtable_lock.get(key.as_ref(), Some(self.seq_num)) {
			if item.0.is_tombstone() {
				return Ok(None); // Key is a tombstone, return None
			} else {
				return Ok(Some((item.1, item.0.seq_num()))); // Key found, return the value
			}
		}
		drop(memtable_lock); // Release the lock on the active memtable

		// Read lock on the immutable memtables
		let memtable_lock = self.core.immutable_memtables.read().unwrap();

		// Check the immutable memtables for the key
		for (_, memtable) in memtable_lock.iter().rev() {
			if let Some(item) = memtable.get(key.as_ref(), Some(self.seq_num)) {
				if item.0.is_tombstone() {
					return Ok(None); // Key is a tombstone, return None
				} else {
					return Ok(Some((item.1, item.0.seq_num()))); // Key found, return the value
				}
			}
		}
		drop(memtable_lock); // Release the lock on the immutable memtables

		// Read lock on the level manifest
		let level_manifest = self.core.levels.read().unwrap();

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
					} else {
						return Ok(Some((item.1, ikey.seq_num()))); // Key found, return the value
					}
				}
			}
		}

		Ok(None) // Key not found in any memtable or table, return None
	}

	/// Creates an iterator for a range scan within the snapshot
	pub fn range<K: AsRef<[u8]>>(
		&self,
		start: K,
		end: K,
		keys_only: bool,
	) -> Result<impl Iterator<Item = IterResult>> {
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

pub struct SnapshotIterator<'a> {
	snapshot_merge_iter: SnapshotMergeIterator<'a>,
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
		let manifest = guardian::ArcRwLockReadGuardian::take(core.levels.clone()).unwrap();

		let iter_state = IterState {
			active: active.clone(),
			immutable: immutable.iter().map(|(_, mt)| mt.clone()).collect(),
			levels: manifest.levels.clone(),
		};

		// Convert bounds to owned for passing to merge iterator
		let start = range.start_bound().map(|v| v.clone());
		let end = range.end_bound().map(|v| v.clone());

		// Increment VLog iterator count
		if let Some(ref vlog) = core.vlog {
			vlog.incr_iterator_count();
		}

		Ok(Self {
			snapshot_merge_iter: SnapshotMergeIterator::new_from(iter_state, seq_num, (start, end)),
			core: core.clone(),
			keys_only,
		})
	}
}

impl Iterator for SnapshotIterator<'_> {
	type Item = IterResult;

	fn next(&mut self) -> Option<Self::Item> {
		if let Some((key, value)) = self.snapshot_merge_iter.next() {
			if self.keys_only {
				// For keys-only mode, return None for the value to avoid allocations
				Some(Ok((key, None)))
			} else {
				// Resolve value pointers to actual values
				match self.core.resolve_value(&value) {
					Ok(resolved_value) => Some(Ok((key, Some(resolved_value)))),
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

/// A merge iterator that respects snapshot isolation by filtering entries based on sequence numbers
pub struct SnapshotMergeIterator<'a> {
	// Owned state
	_iter_state: Box<IterState>,

	/// Array of iterators to merge over
	iterators: NonNull<Vec<BoxedIterator<'a>>>,

	/// Min-heap of items ordered by their current key
	heap: BinaryHeap<HeapItem>,

	/// Snapshot sequence number for version visibility
	snapshot_seq_num: u64,

	// Current user key being processed
	current_user_key: Option<Key>,

	/// Whether we've found a valid version for the current key
	current_key_resolved: bool,

	/// Range bounds for filtering
	range_end: Bound<Vec<u8>>,

	/// Whether the iterator has been initialized
	initialized: bool,
}

impl<'a> SnapshotMergeIterator<'a> {
	pub fn new_from(
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
						iterators.push(Box::new(table_iter));
					}
				}
			}
		}

		let heap = BinaryHeap::with_capacity(iterators.len());

		// Box the iterators and get a raw pointer
		let boxed_iterators = Box::new(iterators);
		let iterators_ptr = NonNull::new(Box::into_raw(boxed_iterators))
			.expect("Box::into_raw should never return null");

		Self {
			_iter_state: boxed_state,
			iterators: iterators_ptr,
			heap,
			snapshot_seq_num,
			current_user_key: None,
			current_key_resolved: false,
			range_end: range.1,
			initialized: false,
		}
	}

	fn initialize(&mut self) {
		// Pull the first item from each iterator and add to heap
		unsafe {
			let iterators = self.iterators.as_mut();
			for (idx, iter) in iterators.iter_mut().enumerate() {
				// Keep trying to get the next valid item from this iterator
				for (key, value) in iter.by_ref() {
					// Filter by snapshot sequence number
					if key.seq_num() <= self.snapshot_seq_num {
						self.heap.push(HeapItem {
							key,
							value,
							iterator_index: idx,
						});
						break; // Found a valid item, add to heap and move to next iterator
					}
					// Continue to next item if filtered out by sequence number
				}
			}
		}
		self.initialized = true;
	}
}

impl Drop for SnapshotMergeIterator<'_> {
	fn drop(&mut self) {
		// Must drop the iterators before iter_state
		// because the iterators contain references to iter_state
		unsafe {
			let _ = Box::from_raw(self.iterators.as_ptr());
		}
		// iter_state is dropped automatically after this
	}
}

impl Iterator for SnapshotMergeIterator<'_> {
	type Item = (Key, Value);

	fn next(&mut self) -> Option<Self::Item> {
		if !self.initialized {
			self.initialize();
		}

		loop {
			if self.heap.is_empty() {
				return None;
			}

			// Get the smallest item from the heap
			let heap_item = self.heap.pop()?;

			// Pull the next item from the same iterator and add back to heap if valid
			unsafe {
				let iterators = self.iterators.as_mut();
				for (key, value) in iterators[heap_item.iterator_index].by_ref() {
					// Filter by snapshot sequence number
					if key.seq_num() <= self.snapshot_seq_num {
						self.heap.push(HeapItem {
							key,
							value,
							iterator_index: heap_item.iterator_index,
						});
						break;
					}
					// Continue to next item if filtered out
				}
			}

			// Process the key-value pair
			let user_key = heap_item.key.user_key.clone();
			let kind = heap_item.key.kind();

			// Check if this is a new user key
			let is_new_key = match &self.current_user_key {
				None => true,
				Some(current) => &user_key != current,
			};

			if is_new_key {
				self.current_user_key = Some(user_key.clone());
				self.current_key_resolved = false;
			}

			// Skip if we've already resolved this key
			if self.current_key_resolved {
				continue;
			}

			// This is the latest visible version of this key
			self.current_key_resolved = true;

			// If it's a tombstone, the key is deleted in this snapshot
			if kind == InternalKeyKind::Delete {
				continue;
			}

			// Skip if the key exceeds the range end
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

			// Return the user key and value
			return Some((user_key, heap_item.value));
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::{Options, Tree};
	use std::collections::HashSet;
	use std::sync::Arc;

	use super::{IterState, SnapshotMergeIterator};
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

		let merge_iter = SnapshotMergeIterator::new_from(iter_state, 1, range);

		let items: Vec<_> = merge_iter.collect();
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].0.as_ref(), b"z1");
		assert_eq!(items[1].0.as_ref(), b"z2");
	}
}
