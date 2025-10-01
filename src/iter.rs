use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::{cmp::Ordering, sync::Arc};

use crate::error::Result;
use crate::util::LogicalClock;
use crate::vlog::{VLog, ValueLocation, ValuePointer};

use crate::{sstable::InternalKey, Key, Value};

pub type BoxedIterator<'a> = Box<dyn DoubleEndedIterator<Item = (Arc<InternalKey>, Value)> + 'a>;

// Holds a key-value pair and the iterator index
#[derive(Eq)]
pub(crate) struct HeapItem {
	pub(crate) key: Arc<InternalKey>,
	pub(crate) value: Value,
	pub(crate) iterator_index: usize,
}

impl PartialEq for HeapItem {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == Ordering::Equal
	}
}

impl Ord for HeapItem {
	fn cmp(&self, other: &Self) -> Ordering {
		// Invert for min-heap behavior
		other.cmp_internal(self)
	}
}

impl HeapItem {
	fn cmp_internal(&self, other: &Self) -> Ordering {
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

pub(crate) struct MergeIterator<'a> {
	iterators: Vec<BoxedIterator<'a>>,
	// Heap of iterators, ordered by their current key
	heap: BinaryHeap<HeapItem>,
	// Current key we're processing, to skip duplicate versions
	current_user_key: Option<Key>,
	initialized: bool,
	// If true, skip hard delete entries at the bottom level
	is_bottom_level: bool,
}

impl<'a> MergeIterator<'a> {
	pub(crate) fn new(iterators: Vec<BoxedIterator<'a>>, is_bottom_level: bool) -> Self {
		let heap = BinaryHeap::with_capacity(iterators.len());

		Self {
			iterators,
			heap,
			current_user_key: None,
			initialized: false,
			is_bottom_level,
		}
	}

	fn initialize(&mut self) {
		// Pull the first item from each iterator and add to heap
		for (idx, iter) in self.iterators.iter_mut().enumerate() {
			if let Some((key, value)) = iter.next() {
				self.heap.push(HeapItem {
					key,
					value,
					iterator_index: idx,
				});
			}
		}
		self.initialized = true;
	}
}

impl Iterator for MergeIterator<'_> {
	type Item = (Arc<InternalKey>, Value);

	fn next(&mut self) -> Option<Self::Item> {
		if !self.initialized {
			self.initialize();
		}

		loop {
			// Get the smallest item from the heap
			let heap_item = self.heap.pop()?;

			// Pull the next item from the same iterator and add back to heap
			if let Some((key, value)) = self.iterators[heap_item.iterator_index].next() {
				self.heap.push(HeapItem {
					key,
					value,
					iterator_index: heap_item.iterator_index,
				});
			}

			let user_key = heap_item.key.user_key.clone();
			let is_hard_delete = heap_item.key.is_hard_delete_marker();

			// Check if this is a new user key
			let is_new_key = match &self.current_user_key {
				None => true,
				Some(current) => &user_key != current,
			};

			if is_new_key {
				// New user key - update tracking
				self.current_user_key = Some(user_key);

				// At the bottom level, skip hard delete entries since there are no older entries below
				if is_hard_delete && self.is_bottom_level {
					continue;
				}

				// Return this item (most recent version of this user key)
				return Some((heap_item.key, heap_item.value));
			}
			// Same user key - this is an older version, skip it
			continue;
		}
	}
}

fn collect_vlog_discard_stats(discard_stats: &mut HashMap<u32, i64>, value: &Value) -> Result<()> {
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
	iterators: Vec<BoxedIterator<'a>>,
	// Heap of iterators, ordered by their current key
	heap: BinaryHeap<HeapItem>,
	is_bottom_level: bool,

	// Track the current key being processed
	current_user_key: Option<Key>,

	// Collected versions of the current key
	current_key_versions: Vec<(Arc<InternalKey>, Value)>,

	// Compaction state
	/// Collected discard statistics: file_id -> total_discarded_bytes
	pub discard_stats: HashMap<u32, i64>,

	/// Reference to VLog for populating delete-list
	vlog: Option<Arc<VLog>>,

	/// Batch of stale entries to add to delete-list: (sequence_number, value_size)
	delete_list_batch: Vec<(u64, u64)>,

	/// Versioning configuration
	enable_versioning: bool,
	retention_period_ns: u64,

	/// Keys that are being deleted during compaction (for versioned index cleanup)
	deleted_keys: Vec<Key>,

	/// Logical clock for time-based operations
	clock: Arc<dyn LogicalClock>,

	initialized: bool,
}

impl<'a> CompactionIterator<'a> {
	pub(crate) fn new(
		iterators: Vec<BoxedIterator<'a>>,
		is_bottom_level: bool,
		vlog: Option<Arc<VLog>>,
		enable_versioning: bool,
		retention_period_ns: u64,
		clock: Arc<dyn LogicalClock>,
	) -> Self {
		let heap = BinaryHeap::with_capacity(iterators.len());

		Self {
			iterators,
			heap,
			is_bottom_level,
			current_user_key: None,
			current_key_versions: Vec::new(),
			discard_stats: HashMap::new(),
			vlog,
			delete_list_batch: Vec::new(),
			enable_versioning,
			retention_period_ns,
			deleted_keys: Vec::new(),
			clock,
			initialized: false,
		}
	}

	fn initialize(&mut self) {
		// Pull the first item from each iterator and add to heap
		for (idx, iter) in self.iterators.iter_mut().enumerate() {
			if let Some((key, value)) = iter.next() {
				self.heap.push(HeapItem {
					key,
					value,
					iterator_index: idx,
				});
			}
		}
		self.initialized = true;
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

	/// Returns the keys that were deleted during compaction (for versioned index cleanup)
	pub(crate) fn take_deleted_keys(&mut self) -> Vec<Key> {
		std::mem::take(&mut self.deleted_keys)
	}

	/// Process all collected versions of the current key and return the one to output
	fn process_current_key_versions(&mut self) -> Option<(Arc<InternalKey>, Value)> {
		if self.current_key_versions.is_empty() {
			return None;
		}

		// Sort by sequence number (descending) to get the latest version first
		self.current_key_versions.sort_by(|a, b| b.0.seq_num().cmp(&a.0.seq_num()));

		// Clone the latest version data to avoid borrow conflicts
		let latest_key = self.current_key_versions[0].0.clone();
		let latest_value = self.current_key_versions[0].1.clone();
		let is_latest_delete_marker = latest_key.is_hard_delete_marker();

		// If the latest version is a hard delete marker and we're at bottom level, collect the key for versioned index cleanup
		if is_latest_delete_marker && self.is_bottom_level && self.enable_versioning {
			self.deleted_keys.push(latest_key.user_key.clone());
		}

		// Process all versions for delete list and discard stats
		for (i, (key, value)) in self.current_key_versions.iter().enumerate() {
			let is_hard_delete = key.is_hard_delete_marker();
			let is_latest = i == 0;

			// Determine if this entry should be marked as stale in VLog
			let should_mark_stale = if is_latest && !is_hard_delete {
				// Latest version of a SET operation: never mark as stale (it's being returned)
				false
			} else if is_latest && is_hard_delete && self.is_bottom_level {
				// Latest version of a DELETE operation at bottom level: mark as stale (not returned)
				true
			} else if is_latest && is_hard_delete && !self.is_bottom_level {
				// Latest version of a DELETE operation at non-bottom level: don't mark as stale (it's being returned)
				false
			} else if is_hard_delete {
				// For older DELETE operations (hard delete entries): always mark as stale since they don't have VLog values
				// The B+ tree index entry will remain for versioned queries, but VLog doesn't store delete values
				true
			} else {
				// Older version of a SET operation: check retention period
				if self.enable_versioning && self.retention_period_ns > 0 {
					// Get current time for retention period check
					let current_time = self.clock.now();
					let key_timestamp = key.timestamp;
					let age = current_time - key_timestamp;
					let is_within_retention = age <= self.retention_period_ns;
					// Mark as stale only if NOT within retention period
					!is_within_retention
				} else {
					// If versioning is disabled, mark older versions as stale
					true
				}
			};

			// Add to delete list and collect discard stats if needed
			if should_mark_stale && self.vlog.is_some() {
				if is_hard_delete {
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
					eprintln!("Error collecting discard stats: {e:?}");
				}
			}
		}

		// Clear the versions for the next key
		self.current_key_versions.clear();

		// Return the latest version if it should be returned
		if is_latest_delete_marker && self.is_bottom_level {
			// At bottom level, don't return hard delete entries
			None
		} else {
			Some((latest_key, latest_value))
		}
	}
}

impl Iterator for CompactionIterator<'_> {
	type Item = (Arc<InternalKey>, Value);

	fn next(&mut self) -> Option<Self::Item> {
		if !self.initialized {
			self.initialize();
		}

		loop {
			// Get the next item from the heap
			let heap_item = match self.heap.pop() {
				Some(item) => item,
				None => {
					// No more items in heap, process any remaining key versions
					return self.process_current_key_versions();
				}
			};

			// Pull the next item from the same iterator and add back to heap
			if let Some((key, value)) = self.iterators[heap_item.iterator_index].next() {
				self.heap.push(HeapItem {
					key,
					value,
					iterator_index: heap_item.iterator_index,
				});
			}

			let user_key = heap_item.key.user_key.clone();

			// Check if this is a new user key
			let is_new_key = match &self.current_user_key {
				None => true,
				Some(current) => &user_key != current,
			};

			if is_new_key {
				// Process any accumulated versions of the previous key
				if let Some(result) = self.process_current_key_versions() {
					// Add the current item to the new key's versions
					self.current_user_key = Some(user_key);
					self.current_key_versions.push((heap_item.key, heap_item.value));
					return Some(result);
				}

				// Start collecting versions for the new key
				self.current_user_key = Some(user_key);
				self.current_key_versions.push((heap_item.key, heap_item.value));
			} else {
				// Same user key - add to current key's versions
				self.current_key_versions.push((heap_item.key, heap_item.value));
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{
		sstable::{InternalKey, InternalKeyKind},
		util::MockLogicalClock,
		Options, VLogChecksumLevel, Value,
	};
	use std::sync::Arc;
	use tempfile::TempDir;

	fn create_internal_key(
		user_key: &str,
		sequence: u64,
		kind: InternalKeyKind,
	) -> Arc<InternalKey> {
		InternalKey::new(user_key.as_bytes().to_vec(), sequence, kind, 0).into()
	}

	fn create_internal_key_with_timestamp(
		user_key: &str,
		sequence: u64,
		kind: InternalKeyKind,
		timestamp: u64,
	) -> Arc<InternalKey> {
		InternalKey::new(user_key.as_bytes().to_vec(), sequence, kind, timestamp).into()
	}

	fn create_test_vlog() -> (Arc<VLog>, TempDir) {
		let temp_dir = TempDir::new().unwrap();
		let opts = Options {
			vlog_checksum_verification: VLogChecksumLevel::Full,
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		std::fs::create_dir_all(opts.vlog_dir()).unwrap();
		std::fs::create_dir_all(opts.discard_stats_dir()).unwrap();
		std::fs::create_dir_all(opts.delete_list_dir()).unwrap();

		let vlog = Arc::new(VLog::new(Arc::new(opts)).unwrap());
		(vlog, temp_dir)
	}

	fn create_vlog_value(vlog: &Arc<VLog>, key: &[u8], value: &[u8]) -> Value {
		let pointer = vlog.append(key, value).unwrap();
		ValueLocation::with_pointer(pointer).encode().into()
	}

	// Creates a mock iterator with predefined entries
	struct MockIterator {
		items: Vec<(Arc<InternalKey>, Value)>,
		index: usize,
	}

	impl MockIterator {
		fn new(items: Vec<(Arc<InternalKey>, Value)>) -> Self {
			Self {
				items,
				index: 0,
			}
		}
	}

	impl Iterator for MockIterator {
		type Item = (Arc<InternalKey>, Value);

		fn next(&mut self) -> Option<Self::Item> {
			if self.index < self.items.len() {
				let item = self.items[self.index].clone();
				self.index += 1;
				Some(item)
			} else {
				None
			}
		}
	}

	impl DoubleEndedIterator for MockIterator {
		fn next_back(&mut self) -> Option<Self::Item> {
			if self.index < self.items.len() {
				let item = self.items.pop()?;
				Some(item)
			} else {
				None
			}
		}
	}

	#[test]
	fn test_merge_iterator_sequence_ordering() {
		// First iterator (L0) with hard delete entries for even keys
		let mut items1 = Vec::new();
		for i in 0..10 {
			if i % 2 == 0 {
				// hard_delete for even keys
				let key = create_internal_key(&format!("key-{i:03}"), 200, InternalKeyKind::Delete);
				let empty_value: Vec<u8> = Vec::new();
				items1.push((key, empty_value.into()));
			}
		}

		// Second iterator (L1) with values for all keys
		let mut items2 = Vec::new();
		for i in 0..20 {
			let key = create_internal_key(&format!("key-{i:03}"), 100, InternalKeyKind::Set);
			let value_str = format!("value-{i}");
			let value_vec: Vec<u8> = value_str.into_bytes();
			items2.push((key, value_vec.into()));
		}

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));

		// Create the merge iterator
		let merge_iter = MergeIterator::new(vec![iter1, iter2], false);

		// Collect all items
		let mut result = Vec::new();
		for (key, _) in merge_iter {
			let key_str = String::from_utf8_lossy(&key.user_key).to_string();
			let seq = key.seq_num();
			let kind = key.kind();
			result.push((key_str, seq, kind));
		}

		// Now verify the output

		// 1. First, check that all keys are in ascending order by user key
		for i in 1..result.len() {
			assert!(
				result[i - 1].0 <= result[i].0,
				"Keys not in ascending order: {} vs {}",
				result[i - 1].0,
				result[i].0
			);
		}

		// 2. Check that for keys with hard delete entries, the hard_delete comes first
		for i in 0..10 {
			if i % 2 == 0 {
				// Find this key in the result
				let key = format!("key-{i:03}");
				let entries: Vec<_> = result.iter().filter(|(k, _, _)| k == &key).collect();

				// Should only have one entry per key due to deduplication
				assert_eq!(entries.len(), 1, "Key {key} has multiple entries");

				// And it should be the hard_delete (seq=200, kind=Delete)
				let (_, seq, kind) = entries[0];
				assert_eq!(*seq, 200, "Key {key} has wrong sequence");
				assert_eq!(*kind, InternalKeyKind::Delete, "Key {key} has wrong kind");
			}
		}

		// 3. Check that we have the correct total number of entries
		// All keys (20) because we get 5 keys with hard delete entries from items1 and 15 other keys from items2
		assert_eq!(result.len(), 20, "Wrong number of entries");
	}

	#[test]
	fn test_compaction_iterator_hard_delete_filtering() {
		let mut items1 = Vec::new();
		for i in 0..10 {
			if i % 2 == 0 {
				// hard_delete for even keys
				let key = create_internal_key(&format!("key-{i:03}"), 200, InternalKeyKind::Delete);
				let empty_value: Vec<u8> = Vec::new();
				items1.push((key, empty_value.into()));
			}
		}

		let mut items2 = Vec::new();
		for i in 0..20 {
			let key = create_internal_key(&format!("key-{i:03}"), 100, InternalKeyKind::Set);
			let value_str = format!("value-{i}");
			let value_vec: Vec<u8> = value_str.into_bytes();
			items2.push((key, value_vec.into()));
		}

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));

		// Test non-bottom level (should keep hard delete entries)
		let comp_iter = MergeIterator::new(vec![iter1, iter2], false);

		// Collect all items
		let mut result = Vec::new();
		for (key, _) in comp_iter {
			let key_str = String::from_utf8_lossy(&key.user_key).to_string();
			let seq = key.seq_num();
			let kind = key.kind();
			result.push((key_str, seq, kind));
		}

		// Now verify the output

		// 1. Verify all keys are in ascending order (important for block writer)
		for i in 1..result.len() {
			assert!(
				result[i - 1].0 <= result[i].0,
				"Keys not in ascending order: {} vs {}",
				result[i - 1].0,
				result[i].0
			);
		}

		// 2. For even keys (0, 2, 4...), we should have hard delete entries
		for entry in &result {
			let (key, seq, kind) = entry;

			// Extract key number
			let key_num = key.strip_prefix("key-").unwrap().parse::<i32>().unwrap();

			if key_num % 2 == 0 && key_num < 10 {
				// Even keys under 10 should be hard delete entries with seq=200
				assert_eq!(*seq, 200, "Even key {key} has wrong sequence");
				assert_eq!(*kind, InternalKeyKind::Delete, "Even key {key} has wrong kind");
			} else {
				// Odd keys and even keys >= 10 should be values with seq=100
				assert_eq!(*seq, 100, "Key {key} has wrong sequence");
				assert_eq!(*kind, InternalKeyKind::Set, "Key {key} has wrong kind");
			}
		}

		// 3. Test bottom level (should drop hard delete entries)
		let mut items1 = Vec::new();
		for i in 0..10 {
			if i % 2 == 0 {
				// hard_delete for even keys
				let key = create_internal_key(&format!("key-{i:03}"), 200, InternalKeyKind::Delete);
				// Create empty Vec<u8> and wrap it in Arc for hard_delete value
				let empty_value: Vec<u8> = Vec::new();
				items1.push((key, empty_value.into()));
			}
		}

		let mut items2 = Vec::new();
		for i in 0..20 {
			let key = create_internal_key(&format!("key-{i:03}"), 100, InternalKeyKind::Set);
			let value_str = format!("value-{i}");
			let value_vec: Vec<u8> = value_str.into_bytes();
			items2.push((key, value_vec.into()));
		}

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));

		// Use bottom level
		let comp_iter = MergeIterator::new(vec![iter1, iter2], true);

		// Collect all items
		let mut bottom_result = Vec::new();
		for (key, _) in comp_iter {
			let key_str = String::from_utf8_lossy(&key.user_key).to_string();
			bottom_result.push(key_str);
		}

		// At bottom level, hard delete entries should be removed
		for i in 0..20 {
			let key = format!("key-{i:03}");

			if i % 2 == 0 && i < 10 {
				// Even keys under 10 should be removed due to hard delete entries
				assert!(
					!bottom_result.contains(&key),
					"Key {key} should be removed at bottom level"
				);
			} else {
				// Other keys should remain
				assert!(bottom_result.contains(&key), "Key {key} should exist at bottom level");
			}
		}
	}

	#[tokio::test]
	async fn test_combined_iterator_returns_latest_version() {
		let (vlog, _temp_dir) = create_test_vlog();

		// Create multiple versions of the same key with different sequence numbers
		let user_key = "key1";
		let key_v1 = create_internal_key(user_key, 100, InternalKeyKind::Set);
		let key_v2 = create_internal_key(user_key, 200, InternalKeyKind::Set);
		let key_v3 = create_internal_key(user_key, 300, InternalKeyKind::Set);

		let value_v1 = create_vlog_value(&vlog, user_key.as_bytes(), b"value1");
		let value_v2 = create_vlog_value(&vlog, user_key.as_bytes(), b"value2");
		let value_v3 = create_vlog_value(&vlog, user_key.as_bytes(), b"value3");

		// Put them in different iterators to test different levels
		let items1 = vec![(key_v1.clone(), value_v1.clone())]; // L0 - oldest
		let items2 = vec![(key_v2.clone(), value_v2.clone())]; // L1 - middle
		let items3 = vec![(key_v3.clone(), value_v3.clone())]; // L2 - newest

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));
		let iter3 = Box::new(MockIterator::new(items3));

		// Create compaction iterator (non-bottom level)
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2, iter3],
			false, // not bottom level
			Some(vlog.clone()),
			false,
			0,
			Arc::new(MockLogicalClock::default()),
		);

		// Should return only the latest version (seq=300)
		let result: Vec<_> = comp_iter.by_ref().collect();
		assert_eq!(result.len(), 1);

		let (returned_key, returned_value) = &result[0];
		assert_eq!(returned_key.user_key.as_ref(), user_key.as_bytes());
		assert_eq!(returned_key.seq_num(), 300);
		assert_eq!(returned_key.kind(), InternalKeyKind::Set);
		// Verify the correct value is returned (should be value_v3)
		assert_eq!(
			returned_value, &value_v3,
			"Should return the value corresponding to the latest version (seq=300)"
		);

		// Flush any remaining delete list batch
		comp_iter.flush_delete_list_batch().unwrap();

		// Verify that older versions are marked as stale, but latest is not
		assert!(vlog.is_stale(100).unwrap(), "Older version (seq=100) should be marked as stale");
		assert!(vlog.is_stale(200).unwrap(), "Older version (seq=200) should be marked as stale");
		assert!(
			!vlog.is_stale(300).unwrap(),
			"Latest version (seq=300) should NOT be marked as stale since it was returned"
		);
	}

	#[tokio::test]
	async fn test_combined_iterator_adds_older_versions_to_delete_list() {
		let (vlog, _temp_dir) = create_test_vlog();

		let user_key = "key1";
		let key_v1 = create_internal_key(user_key, 100, InternalKeyKind::Set);
		let key_v2 = create_internal_key(user_key, 200, InternalKeyKind::Set);
		let key_v3 = create_internal_key(user_key, 300, InternalKeyKind::Set);

		let value_v1 = create_vlog_value(&vlog, user_key.as_bytes(), b"value1");
		let value_v2 = create_vlog_value(&vlog, user_key.as_bytes(), b"value2");
		let value_v3 = create_vlog_value(&vlog, user_key.as_bytes(), b"value3");

		let items1 = vec![(key_v1.clone(), value_v1.clone())];
		let items2 = vec![(key_v2.clone(), value_v2.clone())];
		let items3 = vec![(key_v3.clone(), value_v3.clone())];

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));
		let iter3 = Box::new(MockIterator::new(items3));

		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2, iter3],
			false, // not bottom level
			Some(vlog.clone()),
			false,
			0,
			Arc::new(MockLogicalClock::default()),
		);

		// Consume the iterator
		let result: Vec<_> = comp_iter.by_ref().collect();

		// Verify we get the latest version with correct value
		assert_eq!(result.len(), 1);
		let (returned_key, returned_value) = &result[0];
		assert_eq!(returned_key.seq_num(), 300);
		assert_eq!(
			returned_value, &value_v3,
			"Should return the value corresponding to the latest version (seq=300)"
		);

		// Flush delete list batch to VLog
		comp_iter.flush_delete_list_batch().unwrap();

		// Verify that the delete list batch was properly flushed
		assert_eq!(
			comp_iter.delete_list_batch.len(),
			0,
			"Delete list batch should be empty after flush"
		);

		// Verify that older versions are marked as stale
		assert!(vlog.is_stale(100).unwrap(), "Older version (seq=100) should be marked as stale");
		assert!(vlog.is_stale(200).unwrap(), "Older version (seq=200) should be marked as stale");
		// Latest version (seq=300) should NOT be marked as stale since it was returned
		assert!(
			!vlog.is_stale(300).unwrap(),
			"Latest version (seq=300) should NOT be marked as stale since it was returned"
		);
	}

	#[tokio::test]
	async fn test_hard_delete_at_bottom_level() {
		let (vlog, _temp_dir) = create_test_vlog();

		let user_key = "key1";
		let hard_delete_key = create_internal_key(user_key, 200, InternalKeyKind::Delete);
		let value_key = create_internal_key(user_key, 100, InternalKeyKind::Set);

		let empty_value: Vec<u8> = Vec::new();
		let actual_value = create_vlog_value(&vlog, user_key.as_bytes(), b"value1");

		let items1 = vec![(hard_delete_key.clone(), empty_value.into())];
		let items2 = vec![(value_key.clone(), actual_value.clone())];

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));

		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			true, // bottom level
			Some(vlog.clone()),
			false,
			0,
			Arc::new(MockLogicalClock::default()),
		);

		// At bottom level, hard_delete should NOT be returned
		let result: Vec<_> = comp_iter.by_ref().collect();
		assert_eq!(result.len(), 0, "At bottom level, hard_delete should not be returned");

		// Flush delete list batch
		comp_iter.flush_delete_list_batch().unwrap();

		// Both hard_delete and older value should be added to delete list
		// At bottom level, both the hard_delete and the older value should be marked as stale
		assert!(
			vlog.is_stale(200).unwrap(),
			"hard_delete (seq=200) should be marked as stale at bottom level"
		);
		assert!(vlog.is_stale(100).unwrap(), "Older value (seq=100) should be marked as stale");
	}

	#[tokio::test]
	async fn test_hard_delete_at_non_bottom_level() {
		let (vlog, _temp_dir) = create_test_vlog();

		let user_key = "key1";
		let hard_delete_key = create_internal_key(user_key, 200, InternalKeyKind::Delete);
		let value_key = create_internal_key(user_key, 100, InternalKeyKind::Set);

		let empty_value: Vec<u8> = Vec::new();
		let actual_value = create_vlog_value(&vlog, user_key.as_bytes(), b"value1");

		let items1 = vec![(hard_delete_key.clone(), empty_value.into())];
		let items2 = vec![(value_key.clone(), actual_value.clone())];

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));

		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			false, // not bottom level
			Some(vlog.clone()),
			false,
			0,
			Arc::new(MockLogicalClock::default()),
		);

		// At non-bottom level, hard_delete SHOULD be returned
		let result: Vec<_> = comp_iter.by_ref().collect();
		assert_eq!(result.len(), 1, "At non-bottom level, hard_delete should be returned");

		let (returned_key, returned_value) = &result[0];
		assert_eq!(returned_key.user_key.as_ref(), user_key.as_bytes());
		assert_eq!(returned_key.seq_num(), 200);
		assert_eq!(returned_key.kind(), InternalKeyKind::Delete);
		// Verify hard_delete has empty value
		assert_eq!(returned_value.len(), 0, "hard_delete should have empty value");

		// Flush delete list batch
		comp_iter.flush_delete_list_batch().unwrap();

		// Only the older value should be added to delete list (not the hard_delete)
		// Check that the older value (seq=100) is marked as stale
		assert!(vlog.is_stale(100).unwrap(), "Older value (seq=100) should be marked as stale");
		// Check that the hard_delete (seq=200) is NOT marked as stale (since it was returned)
		assert!(
			!vlog.is_stale(200).unwrap(),
			"hard_delete (seq=200) should NOT be marked as stale since it was returned"
		);
	}

	#[tokio::test]
	async fn test_multiple_keys_with_mixed_scenarios() {
		let (vlog, _temp_dir) = create_test_vlog();

		// Key1: Multiple versions (latest is value)
		let key1_v1 = create_internal_key("key1", 100, InternalKeyKind::Set);
		let key1_v2 = create_internal_key("key1", 200, InternalKeyKind::Set);
		let key1_val1 = create_vlog_value(&vlog, b"key1", b"value1_old");
		let key1_val2 = create_vlog_value(&vlog, b"key1", b"value1_new");

		// Key2: Multiple versions (latest is hard_delete)
		let key2_v1 = create_internal_key("key2", 110, InternalKeyKind::Set);
		let key2_v2 = create_internal_key("key2", 210, InternalKeyKind::Delete);
		let key2_val1 = create_vlog_value(&vlog, b"key2", b"value2");
		let key2_val2: Vec<u8> = Vec::new();

		// Key3: Single version (value)
		let key3_v1 = create_internal_key("key3", 150, InternalKeyKind::Set);
		let key3_val1 = create_vlog_value(&vlog, b"key3", b"value3");

		// Distribute across multiple iterators
		let items1 =
			vec![(key1_v2.clone(), key1_val2.clone()), (key2_v2.clone(), key2_val2.into())];
		let items2 =
			vec![(key1_v1.clone(), key1_val1.clone()), (key3_v1.clone(), key3_val1.clone())];
		let items3 = vec![(key2_v1.clone(), key2_val1.clone())];

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));
		let iter3 = Box::new(MockIterator::new(items3));

		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2, iter3],
			false, // not bottom level
			Some(vlog.clone()),
			false,
			0,
			Arc::new(MockLogicalClock::default()),
		);

		let result: Vec<_> = comp_iter.by_ref().collect();

		// Should get 3 keys back (latest version of each)
		assert_eq!(result.len(), 3);

		// Verify results are in order and contain latest versions
		let keys: Vec<String> =
			result.iter().map(|(k, _)| String::from_utf8_lossy(&k.user_key).to_string()).collect();
		assert_eq!(keys, vec!["key1", "key2", "key3"]);

		// Verify sequence numbers (latest versions)
		assert_eq!(result[0].0.seq_num(), 200); // key1 latest
		assert_eq!(result[1].0.seq_num(), 210); // key2 latest (hard_delete)
		assert_eq!(result[2].0.seq_num(), 150); // key3 only version

		// Verify kinds
		assert_eq!(result[0].0.kind(), InternalKeyKind::Set);
		assert_eq!(result[1].0.kind(), InternalKeyKind::Delete);
		assert_eq!(result[2].0.kind(), InternalKeyKind::Set);

		// Verify values match the latest versions
		assert_eq!(result[0].1, key1_val2, "key1 should have the latest value (key1_val2)");
		assert_eq!(result[1].1.len(), 0, "key2 hard_delete should have empty value");
		assert_eq!(result[2].1, key3_val1, "key3 should have its only value (key3_val1)");

		comp_iter.flush_delete_list_batch().unwrap();

		// Verify delete list behavior for complex scenario:
		// Key1: seq=100 should be stale (older version), seq=200 should NOT be stale (latest, returned)
		assert!(
			vlog.is_stale(100).unwrap(),
			"Key1 older version (seq=100) should be marked as stale"
		);
		assert!(
			!vlog.is_stale(200).unwrap(),
			"Key1 latest version (seq=200) should NOT be marked as stale since it was returned"
		);

		// Key2: seq=110 should be stale (older version), seq=210 should NOT be stale (latest hard_delete, returned)
		assert!(
			vlog.is_stale(110).unwrap(),
			"Key2 older version (seq=110) should be marked as stale"
		);
		assert!(
			!vlog.is_stale(210).unwrap(),
			"Key2 latest hard_delete (seq=210) should NOT be marked as stale since it was returned"
		);

		// Key3: seq=150 should NOT be stale (only version, returned)
		assert!(
			!vlog.is_stale(150).unwrap(),
			"Key3 only version (seq=150) should NOT be marked as stale since it was returned"
		);
	}

	#[test]
	fn test_no_vlog_no_delete_list() {
		// Test CompactionIterator without VLog - should work but not track delete list

		let user_key = "key1";
		let key_v1 = create_internal_key(user_key, 100, InternalKeyKind::Set);
		let key_v2 = create_internal_key(user_key, 200, InternalKeyKind::Set);

		let value_v1: Vec<u8> = b"value1".to_vec();
		let value_v2: Vec<u8> = b"value2".to_vec();

		let items1 = vec![(key_v2.clone(), value_v2.into())];
		let items2 = vec![(key_v1.clone(), value_v1.into())];

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));

		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			false, // not bottom level
			None,  // no vlog
			false,
			0,
			Arc::new(MockLogicalClock::default()),
		);

		// Should still work and return latest version
		let result: Vec<_> = comp_iter.by_ref().collect();
		assert_eq!(result.len(), 1);

		let (returned_key, returned_value) = &result[0];
		assert_eq!(returned_key.user_key.as_ref(), user_key.as_bytes());
		assert_eq!(returned_key.seq_num(), 200);
		// Verify the correct value is returned (should be value_v2)
		assert_eq!(
			returned_value.as_ref(),
			b"value2",
			"Should return the value corresponding to the latest version (seq=200)"
		);

		// Delete list batch should be empty since no VLog
		assert_eq!(comp_iter.delete_list_batch.len(), 0);
	}

	#[tokio::test]
	async fn test_sequence_ordering_across_iterators() {
		let (vlog, _temp_dir) = create_test_vlog();

		// Create entries across multiple iterators with overlapping keys
		// and verify they are merged in correct order

		// Iterator 1 (L0): Latest versions
		let key_a_v3 = create_internal_key("key_a", 300, InternalKeyKind::Set);
		let key_c_v3 = create_internal_key("key_c", 350, InternalKeyKind::Delete);
		let val_a_v3 = create_vlog_value(&vlog, b"key_a", b"value_a_latest");
		let val_c_v3: Vec<u8> = Vec::new();

		// Iterator 2 (L1): Middle versions
		let key_a_v2 = create_internal_key("key_a", 200, InternalKeyKind::Set);
		let key_b_v2 = create_internal_key("key_b", 250, InternalKeyKind::Set);
		let key_c_v2 = create_internal_key("key_c", 220, InternalKeyKind::Set);
		let val_a_v2 = create_vlog_value(&vlog, b"key_a", b"value_a_middle");
		let val_b_v2 = create_vlog_value(&vlog, b"key_b", b"value_b");
		let val_c_v2 = create_vlog_value(&vlog, b"key_c", b"value_c_middle");

		// Iterator 3 (L2): Oldest versions
		let key_a_v1 = create_internal_key("key_a", 100, InternalKeyKind::Set);
		let key_c_v1 = create_internal_key("key_c", 120, InternalKeyKind::Set);
		let key_d_v1 = create_internal_key("key_d", 150, InternalKeyKind::Set);
		let val_a_v1 = create_vlog_value(&vlog, b"key_a", b"value_a_oldest");
		let val_c_v1 = create_vlog_value(&vlog, b"key_c", b"value_c_oldest");
		let val_d_v1 = create_vlog_value(&vlog, b"key_d", b"value_d");

		let items1 =
			vec![(key_a_v3.clone(), val_a_v3.clone()), (key_c_v3.clone(), val_c_v3.into())];
		let items2 = vec![
			(key_a_v2.clone(), val_a_v2.clone()),
			(key_b_v2.clone(), val_b_v2.clone()),
			(key_c_v2.clone(), val_c_v2.clone()),
		];
		let items3 = vec![
			(key_a_v1.clone(), val_a_v1.clone()),
			(key_c_v1.clone(), val_c_v1.clone()),
			(key_d_v1.clone(), val_d_v1.clone()),
		];

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));
		let iter3 = Box::new(MockIterator::new(items3));

		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2, iter3],
			false, // not bottom level
			Some(vlog.clone()),
			false,
			0,
			Arc::new(MockLogicalClock::default()),
		);

		let result: Vec<_> = comp_iter.by_ref().collect();

		// Should get 4 keys in alphabetical order
		assert_eq!(result.len(), 4);

		let keys: Vec<String> =
			result.iter().map(|(k, _)| String::from_utf8_lossy(&k.user_key).to_string()).collect();
		assert_eq!(keys, vec!["key_a", "key_b", "key_c", "key_d"]);

		// Verify we get the latest versions
		assert_eq!(result[0].0.seq_num(), 300); // key_a latest
		assert_eq!(result[1].0.seq_num(), 250); // key_b only version
		assert_eq!(result[2].0.seq_num(), 350); // key_c latest (hard_delete)
		assert_eq!(result[3].0.seq_num(), 150); // key_d only version

		// Verify kinds
		assert_eq!(result[0].0.kind(), InternalKeyKind::Set);
		assert_eq!(result[1].0.kind(), InternalKeyKind::Set);
		assert_eq!(result[2].0.kind(), InternalKeyKind::Delete);
		assert_eq!(result[3].0.kind(), InternalKeyKind::Set);

		// Verify values match the latest versions of each key
		assert_eq!(result[0].1, val_a_v3, "key_a should have the latest value (val_a_v3)");
		assert_eq!(result[1].1, val_b_v2, "key_b should have its only value (val_b_v2)");
		assert_eq!(result[2].1.len(), 0, "key_c hard_delete should have empty value");
		assert_eq!(result[3].1, val_d_v1, "key_d should have its only value (val_d_v1)");

		comp_iter.flush_delete_list_batch().unwrap();

		// Verify delete list behavior for sequence ordering test:
		// key_a: seq=100,200 should be stale (older versions), seq=300 should NOT be stale (latest, returned)
		assert!(
			vlog.is_stale(100).unwrap(),
			"key_a oldest version (seq=100) should be marked as stale"
		);
		assert!(
			vlog.is_stale(200).unwrap(),
			"key_a middle version (seq=200) should be marked as stale"
		);
		assert!(
			!vlog.is_stale(300).unwrap(),
			"key_a latest version (seq=300) should NOT be marked as stale since it was returned"
		);

		// key_b: seq=250 should NOT be stale (only version, returned)
		assert!(
			!vlog.is_stale(250).unwrap(),
			"key_b only version (seq=250) should NOT be marked as stale since it was returned"
		);

		// key_c: seq=120,220 should be stale (older versions), seq=350 should NOT be stale (latest hard_delete, returned)
		assert!(
			vlog.is_stale(120).unwrap(),
			"key_c oldest version (seq=120) should be marked as stale"
		);
		assert!(
			vlog.is_stale(220).unwrap(),
			"key_c middle version (seq=220) should be marked as stale"
		);
		assert!(
			!vlog.is_stale(350).unwrap(),
			"key_c latest hard_delete (seq=350) should NOT be marked as stale since it was returned"
		);

		// key_d: seq=150 should NOT be stale (only version, returned)
		assert!(
			!vlog.is_stale(150).unwrap(),
			"key_d only version (seq=150) should NOT be marked as stale since it was returned"
		);
	}

	#[tokio::test]
	async fn test_compaction_iterator_versioning_retention_logic() {
		// Create test VLog
		let (vlog, _temp_dir) = create_test_vlog();

		// Use fixed current time for consistent testing
		let current_time = 1000000000000; // Fixed current time
		let retention_period_ns = 5_000_000_000; // 5 seconds

		// Create keys with different timestamps and operations
		let recent_time = current_time - 1_000_000_000; // 1 second ago (within retention)
		let old_time = current_time - 3_000_000_000; // 3 seconds ago (within retention)

		// Test case 1: Recent SET, Old SET, Recent DELETE
		// Expected: Recent SET kept, Old SET kept (within retention), Recent DELETE marked stale
		let key1_recent_set =
			create_internal_key_with_timestamp("key1", 100, InternalKeyKind::Set, recent_time);
		let key1_old_set =
			create_internal_key_with_timestamp("key1", 200, InternalKeyKind::Set, old_time);
		let key1_recent_delete =
			create_internal_key_with_timestamp("key1", 300, InternalKeyKind::Delete, recent_time);

		// Test case 2: Old SET, Recent SET, Old DELETE
		// Expected: Recent SET kept, Old SET kept (within retention), Old DELETE marked stale
		let key2_old_set =
			create_internal_key_with_timestamp("key2", 400, InternalKeyKind::Set, old_time);
		let key2_recent_set =
			create_internal_key_with_timestamp("key2", 500, InternalKeyKind::Set, recent_time);
		let key2_old_delete =
			create_internal_key_with_timestamp("key2", 600, InternalKeyKind::Delete, old_time);

		let val1_recent = create_vlog_value(&vlog, b"key1", b"value1_recent");
		let val1_old = create_vlog_value(&vlog, b"key1", b"value1_old");
		let val2_old = create_vlog_value(&vlog, b"key2", b"value2_old");
		let val2_recent = create_vlog_value(&vlog, b"key2", b"value2_recent");

		// Create items for testing
		let items1 = vec![
			(key1_recent_delete, Arc::from(Vec::new())), // DELETE operation
			(key1_old_set, val1_old),
			(key1_recent_set, val1_recent),
		];
		let items2 = vec![
			(key2_old_delete, Arc::from(Vec::new())), // DELETE operation
			(key2_recent_set, val2_recent),
			(key2_old_set, val2_old),
		];

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));

		// Test with versioning enabled and 5-second retention period
		let clock = Arc::new(MockLogicalClock::with_timestamp(current_time));
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			false, // not bottom level
			Some(vlog.clone()),
			true, // enable versioning
			retention_period_ns,
			clock,
		);

		let result: Vec<_> = comp_iter.by_ref().collect();

		// Should get 2 keys (latest versions only)
		assert_eq!(result.len(), 2);

		// Verify we get the latest versions
		let keys: Vec<String> =
			result.iter().map(|(k, _)| String::from_utf8_lossy(&k.user_key).to_string()).collect();
		assert!(keys.contains(&"key1".to_string()));
		assert!(keys.contains(&"key2".to_string()));

		comp_iter.flush_delete_list_batch().unwrap();

		// Verify retention behavior for key1:
		// - Recent DELETE (seq=300): NOT stale (latest, returned)
		// - Old SET (seq=200): NOT stale (within retention)
		// - Recent SET (seq=100): NOT stale (within retention)
		assert!(
			!vlog.is_stale(300).unwrap(),
			"key1 recent DELETE (seq=300) should NOT be marked as stale (latest, returned)"
		);
		assert!(
			!vlog.is_stale(200).unwrap(),
			"key1 old SET (seq=200) should NOT be marked as stale (within retention)"
		);
		assert!(
			!vlog.is_stale(100).unwrap(),
			"key1 recent SET (seq=100) should NOT be marked as stale (within retention)"
		);

		// Verify retention behavior for key2:
		// - Recent SET (seq=500): NOT stale (latest, returned)
		// - Old SET (seq=400): NOT stale (within retention)
		// - Old DELETE (seq=600): NOT stale (within retention)
		assert!(
			!vlog.is_stale(500).unwrap(),
			"key2 recent SET (seq=500) should NOT be marked as stale (latest, returned)"
		);
		assert!(
			!vlog.is_stale(400).unwrap(),
			"key2 old SET (seq=400) should NOT be marked as stale (within retention)"
		);
		assert!(
			!vlog.is_stale(600).unwrap(),
			"key2 old DELETE (seq=600) should NOT be marked as stale (within retention)"
		);
	}

	#[tokio::test]
	async fn test_delete_list_logic() {
		let current_time = 1000000000000; // Fixed current time for testing
		let retention_period = 1000000; // 1 millisecond

		// Test case 1: Soft delete should respect retention period
		{
			let (vlog, _temp_dir) = create_test_vlog();
			let user_key = "soft_delete_key";

			// Create a soft delete within retention period
			let soft_delete_key = create_internal_key_with_timestamp(
				user_key,
				200,
				InternalKeyKind::SoftDelete,
				current_time,
			);

			// Create old value with timestamp beyond retention
			let old_value_key = create_internal_key_with_timestamp(
				user_key,
				100,
				InternalKeyKind::Set,
				current_time - retention_period - 1,
			);

			let empty_value: Vec<u8> = Vec::new();
			let actual_value = create_vlog_value(&vlog, user_key.as_bytes(), b"value1");

			let items = vec![
				(soft_delete_key.clone(), empty_value.into()),
				(old_value_key.clone(), actual_value.clone()),
			];

			let iter = Box::new(MockIterator::new(items));
			let clock = Arc::new(MockLogicalClock::with_timestamp(current_time));
			let mut comp_iter = CompactionIterator::new(
				vec![iter],
				true, // bottom level
				Some(vlog.clone()),
				true, // enable versioning
				retention_period,
				clock,
			);

			// Process the entries
			let _result: Vec<_> = comp_iter.by_ref().collect();
			comp_iter.flush_delete_list_batch().unwrap();

			// Soft delete should NOT be marked as stale (it's not a hard delete marker)
			assert!(
				!vlog.is_stale(200).unwrap(),
				"Soft delete (seq=200) should NOT be marked as stale - it respects retention period"
			);

			// Old value should be marked as stale (older version, respects retention period)
			assert!(
				vlog.is_stale(100).unwrap(),
				"Old value (seq=100) should be marked as stale - older version beyond retention period"
			);
		}

		// Test case 2: Range delete should always be marked as stale
		{
			let (vlog, _temp_dir) = create_test_vlog();
			let user_key = "range_delete_key";

			// Create a range delete
			let range_delete_key = create_internal_key_with_timestamp(
				user_key,
				200,
				InternalKeyKind::RangeDelete,
				current_time,
			);

			// Create old value with timestamp beyond retention
			let old_value_key = create_internal_key_with_timestamp(
				user_key,
				100,
				InternalKeyKind::Set,
				current_time - retention_period - 1,
			);

			let empty_value: Vec<u8> = Vec::new();
			let actual_value = create_vlog_value(&vlog, user_key.as_bytes(), b"value1");

			let items = vec![
				(range_delete_key.clone(), empty_value.into()),
				(old_value_key.clone(), actual_value.clone()),
			];

			let iter = Box::new(MockIterator::new(items));
			let clock = Arc::new(MockLogicalClock::with_timestamp(current_time));
			let mut comp_iter = CompactionIterator::new(
				vec![iter],
				true, // bottom level
				Some(vlog.clone()),
				true, // enable versioning
				retention_period,
				clock,
			);

			// Process the entries
			let _result: Vec<_> = comp_iter.by_ref().collect();
			comp_iter.flush_delete_list_batch().unwrap();

			// Range delete should be marked as stale (it's a hard delete marker)
			assert!(
				vlog.is_stale(200).unwrap(),
				"Range delete (seq=200) should be marked as stale - it's a hard delete marker"
			);

			// Old value should be marked as stale (older version)
			assert!(vlog.is_stale(100).unwrap(), "Old value (seq=100) should be marked as stale");
		}

		// Test case 3: Hard delete should always be marked as stale
		{
			let (vlog, _temp_dir) = create_test_vlog();
			let user_key = "hard_delete_key";

			// Create a hard delete
			let hard_delete_key = create_internal_key_with_timestamp(
				user_key,
				200,
				InternalKeyKind::Delete,
				current_time,
			);

			// Create old value with timestamp beyond retention
			let old_value_key = create_internal_key_with_timestamp(
				user_key,
				100,
				InternalKeyKind::Set,
				current_time - retention_period - 1,
			);

			let empty_value: Vec<u8> = Vec::new();
			let actual_value = create_vlog_value(&vlog, user_key.as_bytes(), b"value1");

			let items = vec![
				(hard_delete_key.clone(), empty_value.into()),
				(old_value_key.clone(), actual_value.clone()),
			];

			let iter = Box::new(MockIterator::new(items));
			let clock = Arc::new(MockLogicalClock::with_timestamp(current_time));
			let mut comp_iter = CompactionIterator::new(
				vec![iter],
				true, // bottom level
				Some(vlog.clone()),
				true, // enable versioning
				retention_period,
				clock,
			);

			// Process the entries
			let _result: Vec<_> = comp_iter.by_ref().collect();
			comp_iter.flush_delete_list_batch().unwrap();

			// Hard delete should be marked as stale (it's a hard delete marker)
			assert!(
				vlog.is_stale(200).unwrap(),
				"Hard delete (seq=200) should be marked as stale - it's a hard delete marker"
			);

			// Old value should be marked as stale (older version)
			assert!(vlog.is_stale(100).unwrap(), "Old value (seq=100) should be marked as stale");
		}

		// Test case 4: Soft delete with versioning disabled
		{
			let (vlog, _temp_dir) = create_test_vlog();
			let user_key = "soft_delete_no_versioning";

			// Create a soft delete
			let soft_delete_key = create_internal_key_with_timestamp(
				user_key,
				200,
				InternalKeyKind::SoftDelete,
				current_time,
			);

			// Create old value with timestamp beyond retention
			let old_value_key = create_internal_key_with_timestamp(
				user_key,
				100,
				InternalKeyKind::Set,
				current_time - retention_period - 1,
			);

			let empty_value: Vec<u8> = Vec::new();
			let actual_value = create_vlog_value(&vlog, user_key.as_bytes(), b"value1");

			let items = vec![
				(soft_delete_key.clone(), empty_value.into()),
				(old_value_key.clone(), actual_value.clone()),
			];

			let iter = Box::new(MockIterator::new(items));
			let clock = Arc::new(MockLogicalClock::with_timestamp(current_time));
			let mut comp_iter = CompactionIterator::new(
				vec![iter],
				true, // bottom level
				Some(vlog.clone()),
				false, // disable versioning
				retention_period,
				clock,
			);

			// Process the entries
			let _result: Vec<_> = comp_iter.by_ref().collect();
			comp_iter.flush_delete_list_batch().unwrap();

			// Soft delete should NOT be marked as stale (it's the latest version)
			assert!(
				!vlog.is_stale(200).unwrap(),
				"Soft delete (seq=200) should NOT be marked as stale - it's the latest version"
			);

			// Old value should be marked as stale (older version, versioning disabled)
			assert!(
				vlog.is_stale(100).unwrap(),
				"Old value (seq=100) should be marked as stale when versioning is disabled"
			);
		}

		// Test case 5: Soft delete beyond retention period
		{
			let (vlog, _temp_dir) = create_test_vlog();
			let user_key = "soft_delete_old";

			// Create a soft delete with old timestamp (beyond retention)
			let soft_delete_key = create_internal_key_with_timestamp(
				user_key,
				200,
				InternalKeyKind::SoftDelete,
				current_time - retention_period - 1,
			);

			// Create old value with timestamp within retention
			let old_value_key = create_internal_key_with_timestamp(
				user_key,
				100,
				InternalKeyKind::Set,
				current_time - retention_period + 1,
			);

			let empty_value: Vec<u8> = Vec::new();
			let actual_value = create_vlog_value(&vlog, user_key.as_bytes(), b"value1");

			let items = vec![
				(soft_delete_key.clone(), empty_value.into()),
				(old_value_key.clone(), actual_value.clone()),
			];

			let iter = Box::new(MockIterator::new(items));
			let clock = Arc::new(MockLogicalClock::with_timestamp(current_time));
			let mut comp_iter = CompactionIterator::new(
				vec![iter],
				true, // bottom level
				Some(vlog.clone()),
				true, // enable versioning
				retention_period,
				clock,
			);

			// Process the entries
			let _result: Vec<_> = comp_iter.by_ref().collect();
			comp_iter.flush_delete_list_batch().unwrap();

			// Soft delete should NOT be marked as stale (it's the latest version)
			assert!(
				!vlog.is_stale(200).unwrap(),
				"Soft delete (seq=200) should NOT be marked as stale - it's the latest version"
			);

			// Old value should NOT be marked as stale (within retention period)
			assert!(
				!vlog.is_stale(100).unwrap(),
				"Old value (seq=100) should NOT be marked as stale - within retention period"
			);
		}
	}
}
