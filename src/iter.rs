use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

use crate::clock::LogicalClock;
use crate::error::Result;
use crate::sstable::InternalKey;
use crate::vlog::{VLog, ValueLocation, ValuePointer};
use crate::{Key, Value};

pub type BoxedIterator<'a> = Box<dyn DoubleEndedIterator<Item = Result<(InternalKey, Value)>> + 'a>;

// Holds a key-value pair and the iterator index
#[derive(Eq)]
pub(crate) struct HeapItem {
	pub(crate) key: InternalKey,
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

fn collect_vlog_discard_stats(discard_stats: &mut HashMap<u32, i64>, value: &Value) -> Result<()> {
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
	iterators: Vec<BoxedIterator<'a>>,
	// Heap of iterators, ordered by their current key
	heap: BinaryHeap<HeapItem>,
	is_bottom_level: bool,

	// Track the current key being processed
	current_user_key: Option<Key>,

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
	delete_list_batch: Vec<(u64, u64)>,

	/// Versioning configuration
	enable_versioning: bool,
	retention_period_ns: u64,

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
		// Pull the first item from each iterator and add to heap
		for (idx, iter) in self.iterators.iter_mut().enumerate() {
			if let Some(item_result) = iter.next() {
				let (key, value) = item_result?;
				self.heap.push(HeapItem {
					key,
					value,
					iterator_index: idx,
				});
			}
		}
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
}

impl Iterator for CompactionIterator<'_> {
	type Item = Result<(InternalKey, Value)>;

	fn next(&mut self) -> Option<Self::Item> {
		if !self.initialized {
			if let Err(e) = self.initialize() {
				log::error!("[COMPACTION_ITER] Error initializing: {}", e);
				return Some(Err(e));
			}
		}

		loop {
			// First, return any pending output versions
			if !self.output_versions.is_empty() {
				// Remove from front to maintain sequence number order (already sorted
				// descending)
				return Some(Ok(self.output_versions.remove(0)));
			}

			// Get the next item from the heap
			let heap_item = match self.heap.pop() {
				Some(item) => item,
				None => {
					// No more items in heap, process any remaining accumulated versions
					if !self.accumulated_versions.is_empty() {
						self.process_accumulated_versions();
						// Return first output version if any
						if !self.output_versions.is_empty() {
							return Some(Ok(self.output_versions.remove(0)));
						}
					}
					return None;
				}
			};

			// Pull the next item from the same iterator and add back to heap
			if let Some(item_result) = self.iterators[heap_item.iterator_index].next() {
				match item_result {
					Ok((key, value)) => {
						self.heap.push(HeapItem {
							key,
							value,
							iterator_index: heap_item.iterator_index,
						});
					}
					Err(e) => {
						log::error!(
							"[COMPACTION_ITER] Error from iterator {}: {}",
							heap_item.iterator_index,
							e
						);
						return Some(Err(e));
					}
				}
			}

			let user_key = heap_item.key.user_key.clone();

			// Check if this is a new user key
			let is_new_key = match &self.current_user_key {
				None => true,
				Some(current) => user_key != *current,
			};

			if is_new_key {
				// Process accumulated versions of the previous key
				if !self.accumulated_versions.is_empty() {
					self.process_accumulated_versions();

					// Start accumulating the new key
					self.current_user_key = Some(user_key);
					self.accumulated_versions.push((heap_item.key, heap_item.value));

					// Return first output version from processed key if any
					if !self.output_versions.is_empty() {
						return Some(Ok(self.output_versions.remove(0)));
					}
				} else {
					// First key - start accumulating
					self.current_user_key = Some(user_key);
					self.accumulated_versions.push((heap_item.key, heap_item.value));
				}
			} else {
				// Same user key - add to accumulated versions
				self.accumulated_versions.push((heap_item.key, heap_item.value));
			}
		}
	}
}
#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use tempfile::TempDir;
	use test_log::test;

	use super::*;
	use crate::clock::MockLogicalClock;
	use crate::sstable::{InternalKey, InternalKeyKind};
	use crate::{Options, VLogChecksumLevel, Value};

	fn create_internal_key(user_key: &str, sequence: u64, kind: InternalKeyKind) -> InternalKey {
		InternalKey::new(user_key.as_bytes().to_vec(), sequence, kind, 0)
	}

	fn create_internal_key_with_timestamp(
		user_key: &str,
		sequence: u64,
		kind: InternalKeyKind,
		timestamp: u64,
	) -> InternalKey {
		InternalKey::new(user_key.as_bytes().to_vec(), sequence, kind, timestamp)
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

		let vlog = Arc::new(VLog::new(Arc::new(opts), None).unwrap());
		(vlog, temp_dir)
	}

	fn create_vlog_value(vlog: &Arc<VLog>, key: &[u8], value: &[u8]) -> Value {
		let pointer = vlog.append(key, value).unwrap();
		ValueLocation::with_pointer(pointer).encode()
	}

	// Creates a mock iterator with predefined entries
	struct MockIterator {
		items: Vec<(InternalKey, Value)>,
		index: usize,
	}

	impl MockIterator {
		fn new(items: Vec<(InternalKey, Value)>) -> Self {
			Self {
				items,
				index: 0,
			}
		}
	}

	impl Iterator for MockIterator {
		type Item = Result<(InternalKey, Value)>;

		fn next(&mut self) -> Option<Self::Item> {
			if self.index < self.items.len() {
				let item = self.items[self.index].clone();
				self.index += 1;
				Some(Ok(item))
			} else {
				None
			}
		}
	}

	impl DoubleEndedIterator for MockIterator {
		fn next_back(&mut self) -> Option<Self::Item> {
			if self.index < self.items.len() {
				let item = self.items.pop()?;
				Some(Ok(item))
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
				items1.push((key, empty_value));
			}
		}

		// Second iterator (L1) with values for all keys
		let mut items2 = Vec::new();
		for i in 0..20 {
			let key = create_internal_key(&format!("key-{i:03}"), 100, InternalKeyKind::Set);
			let value_str = format!("value-{i}");
			let value_vec: Vec<u8> = value_str.into_bytes();
			items2.push((key, value_vec));
		}

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));

		// Create the merge iterator
		let mut merge_iter = CompactionIterator::new(
			vec![iter1, iter2],
			false,
			None,
			false, // versioning disabled
			0,
			Arc::new(MockLogicalClock::new()),
		);

		// Collect all items
		let mut result = Vec::new();
		for item in merge_iter.by_ref() {
			let (key, _) = item.unwrap();
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
		// All keys (20) because we get 5 keys with hard delete entries from items1 and
		// 15 other keys from items2
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
				items1.push((key, empty_value));
			}
		}

		let mut items2 = Vec::new();
		for i in 0..20 {
			let key = create_internal_key(&format!("key-{i:03}"), 100, InternalKeyKind::Set);
			let value_str = format!("value-{i}");
			let value_vec: Vec<u8> = value_str.into_bytes();
			items2.push((key, value_vec));
		}

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));

		// Test non-bottom level (should keep hard delete entries)
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			false,
			None,
			false,
			0,
			Arc::new(MockLogicalClock::new()),
		);

		// Collect all items
		let mut result = Vec::new();
		for item in comp_iter.by_ref() {
			let (key, _) = item.unwrap();
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
				items1.push((key, empty_value));
			}
		}

		let mut items2 = Vec::new();
		for i in 0..20 {
			let key = create_internal_key(&format!("key-{i:03}"), 100, InternalKeyKind::Set);
			let value_str = format!("value-{i}");
			let value_vec: Vec<u8> = value_str.into_bytes();
			items2.push((key, value_vec));
		}

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));

		// Use bottom level
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			true, // bottom level
			None,
			false,
			0,
			Arc::new(MockLogicalClock::new()),
		);

		// Collect all items
		let mut bottom_result = Vec::new();
		for item in comp_iter.by_ref() {
			let (key, _) = item.unwrap();
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

	#[test(tokio::test)]
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
		let items1 = vec![(key_v1, value_v1)]; // L0 - oldest
		let items2 = vec![(key_v2, value_v2)]; // L1 - middle
		let items3 = vec![(key_v3, value_v3.clone())]; // L2 - newest

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
		let result: Vec<_> = comp_iter.by_ref().map(|r| r.unwrap()).collect();
		assert_eq!(result.len(), 1);

		let (returned_key, returned_value) = &result[0];
		assert_eq!(&returned_key.user_key, user_key.as_bytes());
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

	#[test(tokio::test)]
	async fn test_combined_iterator_adds_older_versions_to_delete_list() {
		let (vlog, _temp_dir) = create_test_vlog();

		let user_key = "key1";
		let key_v1 = create_internal_key(user_key, 100, InternalKeyKind::Set);
		let key_v2 = create_internal_key(user_key, 200, InternalKeyKind::Set);
		let key_v3 = create_internal_key(user_key, 300, InternalKeyKind::Set);

		let value_v1 = create_vlog_value(&vlog, user_key.as_bytes(), b"value1");
		let value_v2 = create_vlog_value(&vlog, user_key.as_bytes(), b"value2");
		let value_v3 = create_vlog_value(&vlog, user_key.as_bytes(), b"value3");

		let items1 = vec![(key_v1, value_v1)];
		let items2 = vec![(key_v2, value_v2)];
		let items3 = vec![(key_v3, value_v3.clone())];

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
		let result: Vec<_> = comp_iter.by_ref().map(|r| r.unwrap()).collect();

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

	#[test(tokio::test)]
	async fn test_hard_delete_at_bottom_level() {
		let (vlog, _temp_dir) = create_test_vlog();

		let user_key = "key1";
		let hard_delete_key = create_internal_key(user_key, 200, InternalKeyKind::Delete);
		let value_key = create_internal_key(user_key, 100, InternalKeyKind::Set);

		let empty_value: Vec<u8> = Vec::new();
		let actual_value = create_vlog_value(&vlog, user_key.as_bytes(), b"value1");

		let items1 = vec![(hard_delete_key, empty_value)];
		let items2 = vec![(value_key, actual_value)];

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
		let result: Vec<_> = comp_iter.by_ref().map(|r| r.unwrap()).collect();
		assert_eq!(result.len(), 0, "At bottom level, hard_delete should not be returned");

		// Flush delete list batch
		comp_iter.flush_delete_list_batch().unwrap();

		// Both hard_delete and older value should be added to delete list
		// At bottom level, both the hard_delete and the older value should be marked as
		// stale
		assert!(
			vlog.is_stale(200).unwrap(),
			"hard_delete (seq=200) should be marked as stale at bottom level"
		);
		assert!(vlog.is_stale(100).unwrap(), "Older value (seq=100) should be marked as stale");
	}

	#[test(tokio::test)]
	async fn test_hard_delete_at_non_bottom_level() {
		let (vlog, _temp_dir) = create_test_vlog();

		let user_key = "key1";
		let hard_delete_key = create_internal_key(user_key, 200, InternalKeyKind::Delete);
		let value_key = create_internal_key(user_key, 100, InternalKeyKind::Set);

		let empty_value: Vec<u8> = Vec::new();
		let actual_value = create_vlog_value(&vlog, user_key.as_bytes(), b"value1");

		let items1 = vec![(hard_delete_key, empty_value)];
		let items2 = vec![(value_key, actual_value)];

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
		let result: Vec<_> = comp_iter.by_ref().map(|r| r.unwrap()).collect();
		assert_eq!(result.len(), 1, "At non-bottom level, hard_delete should be returned");

		let (returned_key, returned_value) = &result[0];
		assert_eq!(&returned_key.user_key, user_key.as_bytes());
		assert_eq!(returned_key.seq_num(), 200);
		assert_eq!(returned_key.kind(), InternalKeyKind::Delete);
		// Verify hard_delete has empty value
		assert_eq!(returned_value.len(), 0, "hard_delete should have empty value");

		// Flush delete list batch
		comp_iter.flush_delete_list_batch().unwrap();

		// Only the older value should be added to delete list (not the hard_delete)
		// Check that the older value (seq=100) is marked as stale
		assert!(vlog.is_stale(100).unwrap(), "Older value (seq=100) should be marked as stale");
		// Check that the hard_delete (seq=200) is NOT marked as stale (since it was
		// returned)
		assert!(
			!vlog.is_stale(200).unwrap(),
			"hard_delete (seq=200) should NOT be marked as stale since it was returned"
		);
	}

	#[test(tokio::test)]
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
		let items1 = vec![(key1_v2, key1_val2.clone()), (key2_v2, key2_val2)];
		let items2 = vec![(key1_v1, key1_val1), (key3_v1, key3_val1.clone())];
		let items3 = vec![(key2_v1, key2_val1)];

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

		let result: Vec<_> = comp_iter.by_ref().map(|r| r.unwrap()).collect();

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
		// Key1: seq=100 should be stale (older version), seq=200 should NOT be stale
		// (latest, returned)
		assert!(
			vlog.is_stale(100).unwrap(),
			"Key1 older version (seq=100) should be marked as stale"
		);
		assert!(
			!vlog.is_stale(200).unwrap(),
			"Key1 latest version (seq=200) should NOT be marked as stale since it was returned"
		);

		// Key2: seq=110 should be stale (older version), seq=210 should NOT be stale
		// (latest hard_delete, returned)
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

		let items1 = vec![(key_v2, value_v2)];
		let items2 = vec![(key_v1, value_v1)];

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
		let result: Vec<_> = comp_iter.by_ref().map(|r| r.unwrap()).collect();
		assert_eq!(result.len(), 1);

		let (returned_key, returned_value) = &result[0];
		assert_eq!(&returned_key.user_key, user_key.as_bytes());
		assert_eq!(returned_key.seq_num(), 200);
		// Verify the correct value is returned (should be value_v2)
		assert_eq!(
			returned_value.as_slice(),
			b"value2",
			"Should return the value corresponding to the latest version (seq=200)"
		);

		// Delete list batch should be empty since no VLog
		assert_eq!(comp_iter.delete_list_batch.len(), 0);
	}

	#[test(tokio::test)]
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

		let items1 = vec![(key_a_v3, val_a_v3.clone()), (key_c_v3, val_c_v3)];
		let items2 = vec![(key_a_v2, val_a_v2), (key_b_v2, val_b_v2.clone()), (key_c_v2, val_c_v2)];
		let items3 = vec![(key_a_v1, val_a_v1), (key_c_v1, val_c_v1), (key_d_v1, val_d_v1.clone())];

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

		let result: Vec<_> = comp_iter.by_ref().map(|r| r.unwrap()).collect();

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
		// key_a: seq=100,200 should be stale (older versions), seq=300 should NOT be
		// stale (latest, returned)
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

		// key_c: seq=120,220 should be stale (older versions), seq=350 should NOT be
		// stale (latest hard_delete, returned)
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

	#[test(tokio::test)]
	async fn test_compaction_iterator_versioning_retention_logic() {
		// Create test VLog
		let (vlog, _temp_dir) = create_test_vlog();

		// Use fixed current time for consistent testing
		let current_time = 1000000000000; // Fixed current time
		let retention_period_ns = 5_000_000_000; // 5 seconds

		// Create keys with different timestamps and operations
		let recent_time = current_time - 1_000_000_000; // 1 second ago (within retention)
		let old_time = current_time - 3_000_000_000; // 3 seconds ago (within retention)
		let very_old_time = current_time - 10_000_000_000; // 10 seconds ago (OUTSIDE retention)

		// Test case 1: Recent SET, Old SET, Recent DELETE
		// Expected: All 3 versions kept (all within retention)
		let key1_recent_set =
			create_internal_key_with_timestamp("key1", 100, InternalKeyKind::Set, recent_time);
		let key1_old_set =
			create_internal_key_with_timestamp("key1", 200, InternalKeyKind::Set, old_time);
		let key1_recent_delete =
			create_internal_key_with_timestamp("key1", 300, InternalKeyKind::Delete, recent_time);

		// Test case 2: Old SET, Recent SET, Old DELETE
		// Expected: All 3 versions kept (all within retention)
		let key2_old_set =
			create_internal_key_with_timestamp("key2", 400, InternalKeyKind::Set, old_time);
		let key2_recent_set =
			create_internal_key_with_timestamp("key2", 500, InternalKeyKind::Set, recent_time);
		let key2_old_delete =
			create_internal_key_with_timestamp("key2", 600, InternalKeyKind::Delete, old_time);

		// Test case 3: Recent SET with very old versions outside retention
		// Expected: Only recent SET kept, very old versions marked as stale
		let key3_very_old_set1 =
			create_internal_key_with_timestamp("key3", 700, InternalKeyKind::Set, very_old_time);
		let key3_very_old_set2 =
			create_internal_key_with_timestamp("key3", 800, InternalKeyKind::Set, very_old_time);
		let key3_recent_set =
			create_internal_key_with_timestamp("key3", 900, InternalKeyKind::Set, recent_time);

		let val1_recent = create_vlog_value(&vlog, b"key1", b"value1_recent");
		let val1_old = create_vlog_value(&vlog, b"key1", b"value1_old");
		let val2_old = create_vlog_value(&vlog, b"key2", b"value2_old");
		let val2_recent = create_vlog_value(&vlog, b"key2", b"value2_recent");
		let val3_very_old1 = create_vlog_value(&vlog, b"key3", b"value3_very_old1");
		let val3_very_old2 = create_vlog_value(&vlog, b"key3", b"value3_very_old2");
		let val3_recent = create_vlog_value(&vlog, b"key3", b"value3_recent");

		// Create items for testing
		let items1 = vec![
			(key1_recent_delete, Vec::new()), // DELETE operation
			(key1_old_set, val1_old),
			(key1_recent_set, val1_recent),
		];
		let items2 = vec![
			(key2_old_delete, Vec::new()), // DELETE operation
			(key2_recent_set, val2_recent),
			(key2_old_set, val2_old),
		];
		let items3 = vec![
			(key3_recent_set, val3_recent),
			(key3_very_old_set2, val3_very_old2),
			(key3_very_old_set1, val3_very_old1),
		];

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));
		let iter3 = Box::new(MockIterator::new(items3));

		// Test with versioning enabled and 5-second retention period
		let clock = Arc::new(MockLogicalClock::with_timestamp(current_time));
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2, iter3],
			false, // not bottom level
			Some(vlog.clone()),
			true, // enable versioning
			retention_period_ns,
			clock,
		);

		let result: Vec<_> = comp_iter.by_ref().map(|r| r.unwrap()).collect();

		// With versioning enabled and retention period of 5 seconds, should get 7 items
		// total:
		// - key1: 3 versions (all within retention)
		// - key2: 3 versions (all within retention)
		// - key3: 1 version (only recent one, very old versions filtered out due to retention)
		assert_eq!(result.len(), 7, "Expected 7 items: 3 from key1, 3 from key2, 1 from key3");

		// Verify we get all versions of key1 (all within retention)
		let key1_versions: Vec<_> = result.iter().filter(|(k, _)| &k.user_key == b"key1").collect();
		assert_eq!(key1_versions.len(), 3, "key1 should have 3 versions within retention");

		// Verify we get all versions of key2 (all within retention)
		let key2_versions: Vec<_> = result.iter().filter(|(k, _)| &k.user_key == b"key2").collect();
		assert_eq!(key2_versions.len(), 3, "key2 should have 3 versions within retention");

		// Verify we get only the recent version of key3 (very old versions outside
		// retention)
		let key3_versions: Vec<_> = result.iter().filter(|(k, _)| &k.user_key == b"key3").collect();
		assert_eq!(
			key3_versions.len(),
			1,
			"key3 should have only 1 version (recent), very old versions filtered"
		);

		// Verify sequence numbers for key1 (sorted descending: latest first)
		assert_eq!(key1_versions[0].0.seq_num(), 300); // DELETE
		assert_eq!(key1_versions[1].0.seq_num(), 200); // SET
		assert_eq!(key1_versions[2].0.seq_num(), 100); // SET

		// Verify sequence numbers for key2 (sorted descending: latest first)
		assert_eq!(key2_versions[0].0.seq_num(), 600); // DELETE
		assert_eq!(key2_versions[1].0.seq_num(), 500); // SET
		assert_eq!(key2_versions[2].0.seq_num(), 400); // SET

		// Verify sequence number for key3 (only recent version)
		assert_eq!(key3_versions[0].0.seq_num(), 900); // Recent SET only

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

		// Verify retention behavior for key3:
		// - Recent SET (seq=900): NOT stale (latest, returned)
		// - Very old SET (seq=800): STALE (outside retention period)
		// - Very old SET (seq=700): STALE (outside retention period)
		assert!(
			!vlog.is_stale(900).unwrap(),
			"key3 recent SET (seq=900) should NOT be marked as stale (latest, returned)"
		);
		assert!(
			vlog.is_stale(800).unwrap(),
			"key3 very old SET (seq=800) SHOULD be marked as stale (outside retention period)"
		);
		assert!(
			vlog.is_stale(700).unwrap(),
			"key3 very old SET (seq=700) SHOULD be marked as stale (outside retention period)"
		);
	}

	#[test(tokio::test)]
	async fn test_compaction_iterator_versioning_retention_bottom_level() {
		// Create test VLog
		let (vlog, _temp_dir) = create_test_vlog();

		// Use fixed current time for consistent testing
		let current_time = 1000000000000; // Fixed current time
		let retention_period_ns = 5_000_000_000; // 5 seconds

		// Create keys with different timestamps and operations
		let recent_time = current_time - 1_000_000_000; // 1 second ago (within retention)
		let old_time = current_time - 3_000_000_000; // 3 seconds ago (within retention)
		let very_old_time = current_time - 10_000_000_000; // 10 seconds ago (OUTSIDE retention)

		// Test case 1: DELETE marker is latest at bottom level - entire key should be
		// discarded Expected: All versions (DELETE + SETs) marked as stale and
		// discarded
		let key1_recent_set =
			create_internal_key_with_timestamp("key1", 100, InternalKeyKind::Set, recent_time);
		let key1_old_set =
			create_internal_key_with_timestamp("key1", 200, InternalKeyKind::Set, old_time);
		let key1_recent_delete =
			create_internal_key_with_timestamp("key1", 300, InternalKeyKind::Delete, recent_time);

		// Test case 2: Only DELETE marker at bottom level - entire key should disappear
		// Expected: Nothing returned for this key
		let key2_delete =
			create_internal_key_with_timestamp("key2", 400, InternalKeyKind::Delete, recent_time);

		// Test case 3: Recent SET with very old versions outside retention
		// Expected: Only recent SET kept, very old versions marked as stale
		let key3_very_old_set1 =
			create_internal_key_with_timestamp("key3", 500, InternalKeyKind::Set, very_old_time);
		let key3_very_old_set2 =
			create_internal_key_with_timestamp("key3", 600, InternalKeyKind::Set, very_old_time);
		let key3_recent_set =
			create_internal_key_with_timestamp("key3", 700, InternalKeyKind::Set, recent_time);

		// Test case 4: Very old DELETE marker is latest at bottom level - entire key
		// discarded Expected: All versions (DELETE + SET) marked as stale and
		// discarded
		let key4_old_set =
			create_internal_key_with_timestamp("key4", 800, InternalKeyKind::Set, old_time);
		let key4_very_old_delete =
			create_internal_key_with_timestamp("key4", 900, InternalKeyKind::Delete, very_old_time);

		let val1_recent = create_vlog_value(&vlog, b"key1", b"value1_recent");
		let val1_old = create_vlog_value(&vlog, b"key1", b"value1_old");
		let val3_very_old1 = create_vlog_value(&vlog, b"key3", b"value3_very_old1");
		let val3_very_old2 = create_vlog_value(&vlog, b"key3", b"value3_very_old2");
		let val3_recent = create_vlog_value(&vlog, b"key3", b"value3_recent");
		let val4_old = create_vlog_value(&vlog, b"key4", b"value4_old");

		// Create items for testing
		let items1 = vec![
			(key1_recent_delete, Vec::new()), // DELETE operation - should be dropped
			(key1_old_set, val1_old),
			(key1_recent_set, val1_recent),
		];
		let items2 = vec![
			(key2_delete, Vec::new()), // DELETE only - entire key disappears
		];
		let items3 = vec![
			(key3_recent_set, val3_recent),
			(key3_very_old_set2, val3_very_old2),
			(key3_very_old_set1, val3_very_old1),
		];
		let items4 = vec![
			(key4_very_old_delete, Vec::new()), // Very old DELETE - dropped
			(key4_old_set, val4_old),
		];

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));
		let iter3 = Box::new(MockIterator::new(items3));
		let iter4 = Box::new(MockIterator::new(items4));

		// Test with versioning enabled, 5-second retention period, and BOTTOM LEVEL
		let clock = Arc::new(MockLogicalClock::with_timestamp(current_time));
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2, iter3, iter4],
			true, // BOTTOM LEVEL - delete markers should be dropped
			Some(vlog.clone()),
			true, // enable versioning
			retention_period_ns,
			clock,
		);

		let result: Vec<_> = comp_iter.by_ref().map(|r| r.unwrap()).collect();

		// With versioning enabled at bottom level:
		// - key1: 0 versions (DELETE is latest, entire key discarded)
		// - key2: 0 versions (DELETE-only key discarded)
		// - key3: 1 version (only recent SET kept, very old versions filtered)
		// - key4: 0 versions (DELETE is latest, entire key discarded)
		// Total: 1 item
		assert_eq!(result.len(), 1, "Expected 1 item: only key3 with recent SET");

		// Verify key1: Entire key discarded (DELETE is latest at bottom level)
		let key1_versions: Vec<_> = result.iter().filter(|(k, _)| &k.user_key == b"key1").collect();
		assert_eq!(
			key1_versions.len(),
			0,
			"key1 should have 0 versions (DELETE is latest at bottom level, entire key discarded)"
		);

		// Verify key2: DELETE-only key should completely disappear
		let key2_versions: Vec<_> = result.iter().filter(|(k, _)| &k.user_key == b"key2").collect();
		assert_eq!(
			key2_versions.len(),
			0,
			"key2 should have 0 versions (DELETE-only at bottom level)"
		);

		// Verify key3: only recent version kept (very old versions outside retention)
		let key3_versions: Vec<_> = result.iter().filter(|(k, _)| &k.user_key == b"key3").collect();
		assert_eq!(
			key3_versions.len(),
			1,
			"key3 should have only 1 version (recent), very old versions filtered"
		);
		assert_eq!(key3_versions[0].0.seq_num(), 700); // Recent SET only

		// Verify key4: Entire key discarded (DELETE is latest at bottom level)
		let key4_versions: Vec<_> = result.iter().filter(|(k, _)| &k.user_key == b"key4").collect();
		assert_eq!(
			key4_versions.len(),
			0,
			"key4 should have 0 versions (DELETE is latest at bottom level, entire key discarded)"
		);

		comp_iter.flush_delete_list_batch().unwrap();

		// Verify staleness behavior:

		// key1: DELETE is latest at bottom level, so ALL versions should be marked as
		// stale
		assert!(
			vlog.is_stale(300).unwrap(),
			"key1 DELETE (seq=300) SHOULD be marked as stale (latest DELETE at bottom level)"
		);
		assert!(
			vlog.is_stale(200).unwrap(),
			"key1 old SET (seq=200) SHOULD be marked as stale (entire key discarded)"
		);
		assert!(
			vlog.is_stale(100).unwrap(),
			"key1 recent SET (seq=100) SHOULD be marked as stale (entire key discarded)"
		);

		// key2: DELETE (seq=400) should be marked as stale (dropped at bottom level)
		assert!(
			vlog.is_stale(400).unwrap(),
			"key2 DELETE (seq=400) SHOULD be marked as stale (dropped at bottom level)"
		);

		// key3: Recent version NOT stale, very old versions STALE
		assert!(
			!vlog.is_stale(700).unwrap(),
			"key3 recent SET (seq=700) should NOT be marked as stale (latest, returned)"
		);
		assert!(
			vlog.is_stale(600).unwrap(),
			"key3 very old SET (seq=600) SHOULD be marked as stale (outside retention period)"
		);
		assert!(
			vlog.is_stale(500).unwrap(),
			"key3 very old SET (seq=500) SHOULD be marked as stale (outside retention period)"
		);

		// key4: DELETE is latest at bottom level, so ALL versions should be marked as
		// stale
		assert!(
		vlog.is_stale(900).unwrap(),
		"key4 very old DELETE (seq=900) SHOULD be marked as stale (latest DELETE at bottom level)"
	);
		assert!(
			vlog.is_stale(800).unwrap(),
			"key4 old SET (seq=800) SHOULD be marked as stale (entire key discarded)"
		);
	}

	#[test(tokio::test)]
	async fn test_compaction_iterator_no_versioning_non_bottom_level() {
		// Create test VLog
		let (vlog, _temp_dir) = create_test_vlog();

		// Use fixed current time for consistent testing
		let current_time = 1000000000000; // Fixed current time
		let retention_period_ns = 0; // No retention period (versioning disabled)

		// Create keys with different timestamps and operations
		let recent_time = current_time - 1_000_000_000; // 1 second ago
		let old_time = current_time - 3_000_000_000; // 3 seconds ago
		let very_old_time = current_time - 10_000_000_000; // 10 seconds ago

		// Test case 1: Multiple SET versions - only latest should be kept
		// Expected: Only seq=300 (latest SET) kept, older versions marked stale
		let key1_very_old_set =
			create_internal_key_with_timestamp("key1", 100, InternalKeyKind::Set, very_old_time);
		let key1_old_set =
			create_internal_key_with_timestamp("key1", 200, InternalKeyKind::Set, old_time);
		let key1_recent_set =
			create_internal_key_with_timestamp("key1", 300, InternalKeyKind::Set, recent_time);

		// Test case 2: Latest is DELETE at non-bottom level - DELETE should be
		// preserved Expected: Only DELETE (seq=600) kept, older SETs marked stale
		let key2_old_set =
			create_internal_key_with_timestamp("key2", 400, InternalKeyKind::Set, old_time);
		let key2_recent_set =
			create_internal_key_with_timestamp("key2", 500, InternalKeyKind::Set, recent_time);
		let key2_delete =
			create_internal_key_with_timestamp("key2", 600, InternalKeyKind::Delete, recent_time);

		// Test case 3: Only DELETE marker - should be preserved at non-bottom level
		// Expected: DELETE (seq=700) kept
		let key3_delete =
			create_internal_key_with_timestamp("key3", 700, InternalKeyKind::Delete, recent_time);

		let val1_very_old = create_vlog_value(&vlog, b"key1", b"value1_very_old");
		let val1_old = create_vlog_value(&vlog, b"key1", b"value1_old");
		let val1_recent = create_vlog_value(&vlog, b"key1", b"value1_recent");
		let val2_old = create_vlog_value(&vlog, b"key2", b"value2_old");
		let val2_recent = create_vlog_value(&vlog, b"key2", b"value2_recent");

		// Create items for testing
		let items1 = vec![
			(key1_recent_set, val1_recent),
			(key1_old_set, val1_old),
			(key1_very_old_set, val1_very_old),
		];
		let items2 = vec![
			(key2_delete, Vec::new()), // DELETE operation
			(key2_recent_set, val2_recent),
			(key2_old_set, val2_old),
		];
		let items3 = vec![(key3_delete, Vec::new())]; // DELETE only

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));
		let iter3 = Box::new(MockIterator::new(items3));

		// Test with versioning DISABLED at NON-BOTTOM level
		let clock = Arc::new(MockLogicalClock::with_timestamp(current_time));
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2, iter3],
			false, // NON-BOTTOM LEVEL
			Some(vlog.clone()),
			false, // VERSIONING DISABLED
			retention_period_ns,
			clock,
		);

		let result: Vec<_> = comp_iter.by_ref().map(|r| r.unwrap()).collect();

		// Without versioning at non-bottom level, should get 3 items (only latest
		// versions):
		// - key1: 1 version (latest SET, seq=300)
		// - key2: 1 version (latest DELETE, seq=600)
		// - key3: 1 version (DELETE, seq=700)
		assert_eq!(result.len(), 3, "Expected 3 items: latest version of each key");

		// Verify key1: Only latest SET kept
		let key1_versions: Vec<_> = result.iter().filter(|(k, _)| &k.user_key == b"key1").collect();
		assert_eq!(
			key1_versions.len(),
			1,
			"key1 should have only 1 version (latest, versioning disabled)"
		);
		assert_eq!(key1_versions[0].0.seq_num(), 300); // Latest SET

		// Verify key2: Only latest DELETE kept
		let key2_versions: Vec<_> = result.iter().filter(|(k, _)| &k.user_key == b"key2").collect();
		assert_eq!(
			key2_versions.len(),
			1,
			"key2 should have only 1 version (latest DELETE, versioning disabled)"
		);
		assert_eq!(key2_versions[0].0.seq_num(), 600); // DELETE

		// Verify key3: Only DELETE kept
		let key3_versions: Vec<_> = result.iter().filter(|(k, _)| &k.user_key == b"key3").collect();
		assert_eq!(key3_versions.len(), 1, "key3 should have only 1 version (DELETE)");
		assert_eq!(key3_versions[0].0.seq_num(), 700); // DELETE

		comp_iter.flush_delete_list_batch().unwrap();

		// Verify staleness behavior:

		// key1: Latest SET (seq=300) NOT stale, older versions STALE
		assert!(
			!vlog.is_stale(300).unwrap(),
			"key1 latest SET (seq=300) should NOT be marked as stale (returned)"
		);
		assert!(
			vlog.is_stale(200).unwrap(),
			"key1 old SET (seq=200) SHOULD be marked as stale (older version)"
		);
		assert!(
			vlog.is_stale(100).unwrap(),
			"key1 very old SET (seq=100) SHOULD be marked as stale (older version)"
		);

		// key2: Latest DELETE (seq=600) NOT stale (returned), older SETs STALE
		assert!(
			!vlog.is_stale(600).unwrap(),
			"key2 DELETE (seq=600) should NOT be marked as stale (returned at non-bottom level)"
		);
		assert!(
			vlog.is_stale(500).unwrap(),
			"key2 recent SET (seq=500) SHOULD be marked as stale (older version)"
		);
		assert!(
			vlog.is_stale(400).unwrap(),
			"key2 old SET (seq=400) SHOULD be marked as stale (older version)"
		);

		// key3: DELETE (seq=700) NOT stale (returned at non-bottom level)
		assert!(
			!vlog.is_stale(700).unwrap(),
			"key3 DELETE (seq=700) should NOT be marked as stale (returned at non-bottom level)"
		);
	}

	#[test(tokio::test)]
	async fn test_compaction_iterator_no_versioning_bottom_level() {
		// Create test VLog
		let (vlog, _temp_dir) = create_test_vlog();

		// Use fixed current time for consistent testing
		let current_time = 1000000000000; // Fixed current time
		let retention_period_ns = 0; // No retention period (versioning disabled)

		// Create keys with different timestamps and operations
		let recent_time = current_time - 1_000_000_000; // 1 second ago
		let old_time = current_time - 3_000_000_000; // 3 seconds ago
		let very_old_time = current_time - 10_000_000_000; // 10 seconds ago

		// Test case 1: Multiple SET versions - only latest should be kept
		// Expected: Only seq=300 (latest SET) kept, older versions marked stale
		let key1_very_old_set =
			create_internal_key_with_timestamp("key1", 100, InternalKeyKind::Set, very_old_time);
		let key1_old_set =
			create_internal_key_with_timestamp("key1", 200, InternalKeyKind::Set, old_time);
		let key1_recent_set =
			create_internal_key_with_timestamp("key1", 300, InternalKeyKind::Set, recent_time);

		// Test case 2: Latest is DELETE at bottom level - entire key should be
		// discarded Expected: All versions marked stale, nothing returned
		let key2_old_set =
			create_internal_key_with_timestamp("key2", 400, InternalKeyKind::Set, old_time);
		let key2_recent_set =
			create_internal_key_with_timestamp("key2", 500, InternalKeyKind::Set, recent_time);
		let key2_delete =
			create_internal_key_with_timestamp("key2", 600, InternalKeyKind::Delete, recent_time);

		// Test case 3: Only DELETE marker at bottom level - entire key should disappear
		// Expected: DELETE marked stale, nothing returned
		let key3_delete =
			create_internal_key_with_timestamp("key3", 700, InternalKeyKind::Delete, recent_time);

		let val1_very_old = create_vlog_value(&vlog, b"key1", b"value1_very_old");
		let val1_old = create_vlog_value(&vlog, b"key1", b"value1_old");
		let val1_recent = create_vlog_value(&vlog, b"key1", b"value1_recent");
		let val2_old = create_vlog_value(&vlog, b"key2", b"value2_old");
		let val2_recent = create_vlog_value(&vlog, b"key2", b"value2_recent");

		// Create items for testing
		let items1 = vec![
			(key1_recent_set, val1_recent),
			(key1_old_set, val1_old),
			(key1_very_old_set, val1_very_old),
		];
		let items2 = vec![
			(key2_delete, Vec::new()), // DELETE operation
			(key2_recent_set, val2_recent),
			(key2_old_set, val2_old),
		];
		let items3 = vec![(key3_delete, Vec::new())]; // DELETE only

		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));
		let iter3 = Box::new(MockIterator::new(items3));

		// Test with versioning DISABLED at BOTTOM level
		let clock = Arc::new(MockLogicalClock::with_timestamp(current_time));
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2, iter3],
			true, // BOTTOM LEVEL
			Some(vlog.clone()),
			false, // VERSIONING DISABLED
			retention_period_ns,
			clock,
		);

		let result: Vec<_> = comp_iter.by_ref().map(|r| r.unwrap()).collect();

		// Without versioning at bottom level, should get 1 item:
		// - key1: 1 version (latest SET, seq=300)
		// - key2: 0 versions (DELETE is latest, entire key discarded)
		// - key3: 0 versions (DELETE-only, entire key discarded)
		assert_eq!(result.len(), 1, "Expected 1 item: only key1 with latest SET");

		// Verify key1: Only latest SET kept
		let key1_versions: Vec<_> = result.iter().filter(|(k, _)| &k.user_key == b"key1").collect();
		assert_eq!(
			key1_versions.len(),
			1,
			"key1 should have only 1 version (latest SET, versioning disabled)"
		);
		assert_eq!(key1_versions[0].0.seq_num(), 300); // Latest SET

		// Verify key2: Entire key discarded (DELETE is latest at bottom level)
		let key2_versions: Vec<_> = result.iter().filter(|(k, _)| &k.user_key == b"key2").collect();
		assert_eq!(
			key2_versions.len(),
			0,
			"key2 should have 0 versions (DELETE is latest at bottom level)"
		);

		// Verify key3: Entire key discarded (DELETE-only at bottom level)
		let key3_versions: Vec<_> = result.iter().filter(|(k, _)| &k.user_key == b"key3").collect();
		assert_eq!(
			key3_versions.len(),
			0,
			"key3 should have 0 versions (DELETE-only at bottom level)"
		);

		comp_iter.flush_delete_list_batch().unwrap();

		// Verify staleness behavior:

		// key1: Latest SET (seq=300) NOT stale, older versions STALE
		assert!(
			!vlog.is_stale(300).unwrap(),
			"key1 latest SET (seq=300) should NOT be marked as stale (returned)"
		);
		assert!(
			vlog.is_stale(200).unwrap(),
			"key1 old SET (seq=200) SHOULD be marked as stale (older version)"
		);
		assert!(
			vlog.is_stale(100).unwrap(),
			"key1 very old SET (seq=100) SHOULD be marked as stale (older version)"
		);

		// key2: DELETE is latest at bottom level, ALL versions should be marked as
		// stale
		assert!(
			vlog.is_stale(600).unwrap(),
			"key2 DELETE (seq=600) SHOULD be marked as stale (latest DELETE at bottom level)"
		);
		assert!(
			vlog.is_stale(500).unwrap(),
			"key2 recent SET (seq=500) SHOULD be marked as stale (entire key discarded)"
		);
		assert!(
			vlog.is_stale(400).unwrap(),
			"key2 old SET (seq=400) SHOULD be marked as stale (entire key discarded)"
		);

		// key3: DELETE marked as stale (dropped at bottom level)
		assert!(
			vlog.is_stale(700).unwrap(),
			"key3 DELETE (seq=700) SHOULD be marked as stale (dropped at bottom level)"
		);
	}

	#[test(tokio::test)]
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

			let items = vec![(soft_delete_key, empty_value), (old_value_key, actual_value)];

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

			// Old value should be marked as stale (older version, respects retention
			// period)
			assert!(
				vlog.is_stale(100).unwrap(),
				"Old value (seq=100) should be marked as stale - older version beyond retention period"
			);
		}

		// Test case 2: Hard delete should always be marked as stale
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

			let items = vec![(hard_delete_key, empty_value), (old_value_key, actual_value)];

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

		// Test case 3: Soft delete with versioning disabled
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

			let items = vec![(soft_delete_key, empty_value), (old_value_key, actual_value)];

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

		// Test case 4: Soft delete beyond retention period
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

			let items = vec![(soft_delete_key, empty_value), (old_value_key, actual_value)];

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

	#[test(tokio::test)]
	async fn test_compaction_iterator_set_with_delete_behavior() {
		let (vlog, _tmp_dir) = create_test_vlog();
		let clock = Arc::new(MockLogicalClock::new());

		// Create test data with Replace operations
		let mut items1 = Vec::new();
		let mut items2 = Vec::new();

		// Table 1: Regular SET operations
		let key1 = create_internal_key("test_key", 100, InternalKeyKind::Set);
		let value1 = create_vlog_value(&vlog, b"test_key", b"value1");
		items1.push((key1, value1));

		let key2 = create_internal_key("test_key", 200, InternalKeyKind::Set);
		let value2 = create_vlog_value(&vlog, b"test_key", b"value2");
		items1.push((key2, value2));

		// Table 2: Replace operation (newer sequence number)
		let key3 = create_internal_key("test_key", 300, InternalKeyKind::Replace);
		let value3 = create_vlog_value(&vlog, b"test_key", b"set_with_delete_value");
		items2.push((key3, value3));

		// Create iterators
		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));

		// Test non-bottom level compaction (should preserve Replace)
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			false, // non-bottom level
			Some(vlog.clone()),
			true, // enable versioning
			1000, // retention period
			clock,
		);

		let mut result = Vec::new();
		for item in comp_iter.by_ref() {
			result.push(item.unwrap());
		}

		// Should return only the Replace version (latest)
		assert_eq!(result.len(), 1);
		let (returned_key, _) = &result[0];
		assert_eq!(returned_key.seq_num(), 300);
		assert!(returned_key.is_replace());

		// Flush the delete list batch to actually mark entries as stale
		comp_iter.flush_delete_list_batch().unwrap();

		// Verify that older versions were marked as stale in VLog
		assert!(vlog.is_stale(100).unwrap());
		assert!(vlog.is_stale(200).unwrap());
		assert!(!vlog.is_stale(300).unwrap());
	}

	#[test(tokio::test)]
	async fn test_compaction_iterator_set_with_delete_marks_older_versions_stale() {
		let (vlog, _tmp_dir) = create_test_vlog();
		let clock = Arc::new(MockLogicalClock::new());

		// Create test data with multiple versions and Replace
		let mut items1 = Vec::new();
		let mut items2 = Vec::new();

		// Table 1: Multiple regular SET operations
		let key1 = create_internal_key("test_key", 100, InternalKeyKind::Set);
		let value1 = create_vlog_value(&vlog, b"test_key", b"value1");
		items1.push((key1, value1));

		let key2 = create_internal_key("test_key", 200, InternalKeyKind::Set);
		let value2 = create_vlog_value(&vlog, b"test_key", b"value2");
		items1.push((key2, value2));

		let key3 = create_internal_key("test_key", 250, InternalKeyKind::Set);
		let value3 = create_vlog_value(&vlog, b"test_key", b"value3");
		items1.push((key3, value3));

		// Table 2: Replace operation (not the latest)
		let key4 = create_internal_key("test_key", 300, InternalKeyKind::Replace);
		let value4 = create_vlog_value(&vlog, b"test_key", b"set_with_delete_value");
		items2.push((key4, value4));

		// Table 3: Regular SET after Replace (latest)
		let key5 = create_internal_key("test_key", 400, InternalKeyKind::Set);
		let value5 = create_vlog_value(&vlog, b"test_key", b"final_value");
		items2.push((key5, value5));

		// Create iterators
		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));

		// Test compaction
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			false, // non-bottom level
			Some(vlog.clone()),
			true, // enable versioning
			1000, // retention period
			clock,
		);

		let mut result = Vec::new();
		for item in comp_iter.by_ref() {
			result.push(item.unwrap());
		}

		comp_iter.flush_delete_list_batch().unwrap();

		// Should return only the latest version (regular SET, not Replace)
		assert_eq!(result.len(), 1);
		let (returned_key, _) = &result[0];
		assert_eq!(returned_key.seq_num(), 400);
		assert!(!returned_key.is_replace());

		// The Replace and all older regular SET versions should be marked as stale
		assert!(vlog.is_stale(100).unwrap());
		assert!(vlog.is_stale(200).unwrap());
		assert!(vlog.is_stale(250).unwrap());
		assert!(vlog.is_stale(300).unwrap());
		assert!(!vlog.is_stale(400).unwrap());
	}

	#[test(tokio::test)]
	async fn test_compaction_iterator_set_with_delete_latest_version() {
		let (vlog, _tmp_dir) = create_test_vlog();
		let clock = Arc::new(MockLogicalClock::new());

		// Create test data where Replace is the latest version
		let mut items1 = Vec::new();
		let mut items2 = Vec::new();

		// Table 1: Regular SET operations
		let key1 = create_internal_key("test_key", 100, InternalKeyKind::Set);
		let value1 = create_vlog_value(&vlog, b"test_key", b"value1");
		items1.push((key1, value1));

		let key2 = create_internal_key("test_key", 200, InternalKeyKind::Set);
		let value2 = create_vlog_value(&vlog, b"test_key", b"value2");
		items1.push((key2, value2));

		// Table 2: Replace as the latest version
		let key3 = create_internal_key("test_key", 300, InternalKeyKind::Replace);
		let value3 = create_vlog_value(&vlog, b"test_key", b"set_with_delete_final");
		items2.push((key3, value3));

		// Create iterators
		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));

		// Test compaction
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			false, // non-bottom level
			Some(vlog.clone()),
			true, // enable versioning
			1000, // retention period
			clock,
		);

		let mut result = Vec::new();
		for item in comp_iter.by_ref() {
			result.push(item.unwrap());
		}

		comp_iter.flush_delete_list_batch().unwrap();

		// Should return the Replace version (latest)
		assert_eq!(result.len(), 1);
		let (returned_key, _) = &result[0];
		assert_eq!(returned_key.seq_num(), 300);
		assert!(returned_key.is_replace());

		// All older regular SET versions should be marked as stale
		assert!(vlog.is_stale(100).unwrap());
		assert!(vlog.is_stale(200).unwrap());
		assert!(!vlog.is_stale(300).unwrap());
	}

	#[test(tokio::test)]
	async fn test_compaction_iterator_set_with_delete_mixed_with_hard_delete() {
		let (vlog, _tmp_dir) = create_test_vlog();
		let clock = Arc::new(MockLogicalClock::new());

		// Create test data with Replace and hard delete operations
		let mut items1 = Vec::new();
		let mut items2 = Vec::new();

		// Table 1: Regular SET and hard delete
		let key1 = create_internal_key("test_key", 100, InternalKeyKind::Set);
		let value1 = create_vlog_value(&vlog, b"test_key", b"value1");
		items1.push((key1, value1));

		let key2 = create_internal_key("test_key", 200, InternalKeyKind::Delete);
		let value2 = Value::from(vec![]); // Empty value for delete
		items1.push((key2, value2));

		// Table 2: Replace operation
		let key3 = create_internal_key("test_key", 300, InternalKeyKind::Replace);
		let value3 = create_vlog_value(&vlog, b"test_key", b"set_with_delete_value");
		items2.push((key3, value3));

		// Create iterators
		let iter1 = Box::new(MockIterator::new(items1));
		let iter2 = Box::new(MockIterator::new(items2));

		// Test non-bottom level compaction
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			false, // non-bottom level
			Some(vlog.clone()),
			true, // enable versioning
			1000, // retention period
			clock,
		);

		let mut result = Vec::new();
		for item in comp_iter.by_ref() {
			result.push(item.unwrap());
		}

		comp_iter.flush_delete_list_batch().unwrap();

		// Should return the Replace version (latest)
		assert_eq!(result.len(), 1);
		let (returned_key, _) = &result[0];
		assert_eq!(returned_key.seq_num(), 300);
		assert!(returned_key.is_replace());

		// The hard delete and regular SET should be marked as stale
		assert!(vlog.is_stale(100).unwrap());
		assert!(vlog.is_stale(200).unwrap());
		assert!(!vlog.is_stale(300).unwrap());
	}

	#[test(tokio::test)]
	async fn test_compaction_iterator_multiple_replace_operations() {
		let (vlog, _tmp_dir) = create_test_vlog();
		let clock = Arc::new(MockLogicalClock::new());

		// Test case 1: Multiple Replace operations for the same key
		// Only the latest Replace should be preserved
		{
			let mut items1 = Vec::new();
			let mut items2 = Vec::new();

			// Table 1: Multiple Replace operations
			let key1 = create_internal_key("test_key", 100, InternalKeyKind::Replace);
			let value1 = create_vlog_value(&vlog, b"test_key", b"replace_value1");
			items1.push((key1, value1));

			let key2 = create_internal_key("test_key", 200, InternalKeyKind::Replace);
			let value2 = create_vlog_value(&vlog, b"test_key", b"replace_value2");
			items1.push((key2, value2));

			// Table 2: Another Replace operation (latest)
			let key3 = create_internal_key("test_key", 300, InternalKeyKind::Replace);
			let value3 = create_vlog_value(&vlog, b"test_key", b"replace_value3");
			items2.push((key3, value3));

			// Create iterators
			let iter1 = Box::new(MockIterator::new(items1));
			let iter2 = Box::new(MockIterator::new(items2));

			// Test non-bottom level compaction
			let mut comp_iter = CompactionIterator::new(
				vec![iter1, iter2],
				false, // non-bottom level
				Some(vlog.clone()),
				true, // enable versioning
				1000, // retention period
				clock.clone(),
			);

			let mut result = Vec::new();
			for item in comp_iter.by_ref() {
				result.push(item.unwrap());
			}

			// Flush to mark entries as stale
			comp_iter.flush_delete_list_batch().unwrap();

			// Should return only the latest Replace version
			assert_eq!(result.len(), 1);
			let (returned_key, _) = &result[0];
			assert_eq!(returned_key.seq_num(), 300);
			assert!(returned_key.is_replace());

			// Older Replace versions should be marked as stale
			assert!(vlog.is_stale(100).unwrap());
			assert!(vlog.is_stale(200).unwrap());
			assert!(!vlog.is_stale(300).unwrap());
		}

		// Test case 2: Multiple Replace operations at bottom level
		// Should still preserve only the latest Replace
		{
			let mut items1 = Vec::new();
			let mut items2 = Vec::new();

			// Table 1: Multiple Replace operations
			let key1 = create_internal_key("test_key2", 400, InternalKeyKind::Replace);
			let value1 = create_vlog_value(&vlog, b"test_key2", b"replace_value4");
			items1.push((key1, value1));

			let key2 = create_internal_key("test_key2", 500, InternalKeyKind::Replace);
			let value2 = create_vlog_value(&vlog, b"test_key2", b"replace_value5");
			items1.push((key2, value2));

			// Table 2: Another Replace operation (latest)
			let key3 = create_internal_key("test_key2", 600, InternalKeyKind::Replace);
			let value3 = create_vlog_value(&vlog, b"test_key2", b"replace_value6");
			items2.push((key3, value3));

			// Create iterators
			let iter1 = Box::new(MockIterator::new(items1));
			let iter2 = Box::new(MockIterator::new(items2));

			// Test bottom level compaction
			let mut comp_iter = CompactionIterator::new(
				vec![iter1, iter2],
				true, // bottom level
				Some(vlog.clone()),
				true, // enable versioning
				1000, // retention period
				clock.clone(),
			);

			let mut result = Vec::new();
			for item in comp_iter.by_ref() {
				result.push(item.unwrap());
			}

			// Flush to mark entries as stale
			comp_iter.flush_delete_list_batch().unwrap();

			// Should return only the latest Replace version
			assert_eq!(result.len(), 1);
			let (returned_key, _) = &result[0];
			assert_eq!(returned_key.seq_num(), 600);
			assert!(returned_key.is_replace());

			// Older Replace versions should be marked as stale
			assert!(vlog.is_stale(400).unwrap());
			assert!(vlog.is_stale(500).unwrap());
			assert!(!vlog.is_stale(600).unwrap());
		}

		// Test case 3: Multiple SET operations followed by Replace
		// Only the Replace should be preserved
		{
			let mut items1 = Vec::new();
			let mut items2 = Vec::new();

			// Table 1: Multiple regular SET operations
			let key1 = create_internal_key("test_key3", 700, InternalKeyKind::Set);
			let value1 = create_vlog_value(&vlog, b"test_key3", b"set_value1");
			items1.push((key1, value1));

			let key2 = create_internal_key("test_key3", 800, InternalKeyKind::Set);
			let value2 = create_vlog_value(&vlog, b"test_key3", b"set_value2");
			items1.push((key2, value2));

			let key3 = create_internal_key("test_key3", 850, InternalKeyKind::Set);
			let value3 = create_vlog_value(&vlog, b"test_key3", b"set_value3");
			items1.push((key3, value3));

			// Table 2: Replace operation (latest)
			let key4 = create_internal_key("test_key3", 900, InternalKeyKind::Replace);
			let value4 = create_vlog_value(&vlog, b"test_key3", b"replace_value7");
			items2.push((key4, value4));

			// Create iterators
			let iter1 = Box::new(MockIterator::new(items1));
			let iter2 = Box::new(MockIterator::new(items2));

			// Test compaction
			let mut comp_iter = CompactionIterator::new(
				vec![iter1, iter2],
				false, // non-bottom level
				Some(vlog.clone()),
				true, // enable versioning
				1000, // retention period
				clock.clone(),
			);

			let mut result = Vec::new();
			for item in comp_iter.by_ref() {
				result.push(item.unwrap());
			}

			// Flush to mark entries as stale
			comp_iter.flush_delete_list_batch().unwrap();

			// Should return only the Replace version
			assert_eq!(result.len(), 1);
			let (returned_key, _) = &result[0];
			assert_eq!(returned_key.seq_num(), 900);
			assert!(returned_key.is_replace());

			// All older SET versions should be marked as stale
			assert!(vlog.is_stale(700).unwrap());
			assert!(vlog.is_stale(800).unwrap());
			assert!(vlog.is_stale(850).unwrap());
			assert!(!vlog.is_stale(900).unwrap());
		}

		// Test case 4: Multiple Replace operations for different keys
		// Each key should preserve only its latest Replace
		{
			let mut items1 = Vec::new();
			let mut items2 = Vec::new();

			// Table 1: Replace operations for different keys
			let key1 = create_internal_key("key_a", 1000, InternalKeyKind::Replace);
			let value1 = create_vlog_value(&vlog, b"key_a", b"replace_a1");
			items1.push((key1, value1));

			let key2 = create_internal_key("key_b", 1100, InternalKeyKind::Replace);
			let value2 = create_vlog_value(&vlog, b"key_b", b"replace_b1");
			items1.push((key2, value2));

			// Table 2: Newer Replace operations for the same keys
			let key3 = create_internal_key("key_a", 1200, InternalKeyKind::Replace);
			let value3 = create_vlog_value(&vlog, b"key_a", b"replace_a2");
			items2.push((key3, value3));

			let key4 = create_internal_key("key_b", 1300, InternalKeyKind::Replace);
			let value4 = create_vlog_value(&vlog, b"key_b", b"replace_b2");
			items2.push((key4, value4));

			// Create iterators
			let iter1 = Box::new(MockIterator::new(items1));
			let iter2 = Box::new(MockIterator::new(items2));

			// Test compaction
			let mut comp_iter = CompactionIterator::new(
				vec![iter1, iter2],
				false, // non-bottom level
				Some(vlog.clone()),
				true, // enable versioning
				1000, // retention period
				clock.clone(),
			);

			let mut result = Vec::new();
			for item in comp_iter.by_ref() {
				result.push(item.unwrap());
			}

			// Flush to mark entries as stale
			comp_iter.flush_delete_list_batch().unwrap();

			// Should return 2 entries (one for each key)
			assert_eq!(result.len(), 2);

			// Sort results by key for consistent testing
			result.sort_by(|a, b| a.0.user_key.cmp(&b.0.user_key));

			// Check key_a
			let (returned_key_a, _) = &result[0];
			assert_eq!(&returned_key_a.user_key, b"key_a");
			assert_eq!(returned_key_a.seq_num(), 1200);
			assert!(returned_key_a.is_replace());

			// Check key_b
			let (returned_key_b, _) = &result[1];
			assert_eq!(&returned_key_b.user_key, b"key_b");
			assert_eq!(returned_key_b.seq_num(), 1300);
			assert!(returned_key_b.is_replace());

			// Older versions should be marked as stale
			assert!(vlog.is_stale(1000).unwrap());
			assert!(vlog.is_stale(1100).unwrap());
			assert!(!vlog.is_stale(1200).unwrap());
			assert!(!vlog.is_stale(1300).unwrap());
		}

		// Test case 5: Multiple SET operations followed by Replace followed by SET
		// Only the final SET should be preserved
		{
			let mut items1 = Vec::new();
			let mut items2 = Vec::new();
			let mut items3 = Vec::new();

			// Table 1: Multiple regular SET operations
			let key1 = create_internal_key("test_key4", 1400, InternalKeyKind::Set);
			let value1 = create_vlog_value(&vlog, b"test_key4", b"set_value1");
			items1.push((key1, value1));

			let key2 = create_internal_key("test_key4", 1500, InternalKeyKind::Set);
			let value2 = create_vlog_value(&vlog, b"test_key4", b"set_value2");
			items1.push((key2, value2));

			// Table 2: Replace operation
			let key3 = create_internal_key("test_key4", 1600, InternalKeyKind::Replace);
			let value3 = create_vlog_value(&vlog, b"test_key4", b"replace_value");
			items2.push((key3, value3));

			// Table 3: Final SET operation (latest)
			let key4 = create_internal_key("test_key4", 1700, InternalKeyKind::Set);
			let value4 = create_vlog_value(&vlog, b"test_key4", b"final_set_value");
			items3.push((key4, value4));

			// Create iterators
			let iter1 = Box::new(MockIterator::new(items1));
			let iter2 = Box::new(MockIterator::new(items2));
			let iter3 = Box::new(MockIterator::new(items3));

			// Test compaction
			let mut comp_iter = CompactionIterator::new(
				vec![iter1, iter2, iter3],
				false, // non-bottom level
				Some(vlog.clone()),
				true, // enable versioning
				1000, // retention period
				clock.clone(),
			);

			let mut result = Vec::new();
			for item in comp_iter.by_ref() {
				result.push(item.unwrap());
			}

			// Flush to mark entries as stale
			comp_iter.flush_delete_list_batch().unwrap();

			// Should return only the final SET version
			assert_eq!(result.len(), 1);
			let (returned_key, _) = &result[0];
			assert_eq!(returned_key.seq_num(), 1700);
			assert!(!returned_key.is_replace());
			assert_eq!(returned_key.kind(), InternalKeyKind::Set);

			// All older versions (SET and Replace) should be marked as stale
			assert!(vlog.is_stale(1400).unwrap());
			assert!(vlog.is_stale(1500).unwrap());
			assert!(vlog.is_stale(1600).unwrap());
			assert!(!vlog.is_stale(1700).unwrap());
		}

		// Test case 6: Replace followed by Delete (non-bottom level)
		// Should preserve only the Delete
		{
			let mut items1 = Vec::new();
			let mut items2 = Vec::new();

			// Table 1: Replace operation
			let key1 = create_internal_key("test_key5", 1800, InternalKeyKind::Replace);
			let value1 = create_vlog_value(&vlog, b"test_key5", b"replace_value");
			items1.push((key1, value1));

			// Table 2: Delete operation (latest)
			let key2 = create_internal_key("test_key5", 1900, InternalKeyKind::Delete);
			let value2 = Vec::new(); // Empty value for delete
			items2.push((key2, value2));

			// Create iterators
			let iter1 = Box::new(MockIterator::new(items1));
			let iter2 = Box::new(MockIterator::new(items2));

			// Test non-bottom level compaction
			let mut comp_iter = CompactionIterator::new(
				vec![iter1, iter2],
				false, // non-bottom level
				Some(vlog.clone()),
				true, // enable versioning
				1000, // retention period
				clock.clone(),
			);

			let mut result = Vec::new();
			for item in comp_iter.by_ref() {
				result.push(item.unwrap());
			}

			// Flush to mark entries as stale
			comp_iter.flush_delete_list_batch().unwrap();

			// Should return only the Delete version
			assert_eq!(result.len(), 1);
			let (returned_key, _) = &result[0];
			assert_eq!(returned_key.seq_num(), 1900);
			assert!(returned_key.is_hard_delete_marker());
			assert_eq!(returned_key.kind(), InternalKeyKind::Delete);

			// The Replace should be marked as stale
			assert!(vlog.is_stale(1800).unwrap());
			assert!(!vlog.is_stale(1900).unwrap());
		}

		// Test case 7: Replace followed by Delete (bottom level)
		// Should return nothing (Delete at bottom level discards everything)
		{
			let mut items1 = Vec::new();
			let mut items2 = Vec::new();

			// Table 1: Replace operation
			let key1 = create_internal_key("test_key6", 2000, InternalKeyKind::Replace);
			let value1 = create_vlog_value(&vlog, b"test_key6", b"replace_value");
			items1.push((key1, value1));

			// Table 2: Delete operation (latest)
			let key2 = create_internal_key("test_key6", 2100, InternalKeyKind::Delete);
			let value2 = Vec::new(); // Empty value for delete
			items2.push((key2, value2));

			// Create iterators
			let iter1 = Box::new(MockIterator::new(items1));
			let iter2 = Box::new(MockIterator::new(items2));

			// Test bottom level compaction
			let mut comp_iter = CompactionIterator::new(
				vec![iter1, iter2],
				true, // bottom level
				Some(vlog.clone()),
				true, // enable versioning
				1000, // retention period
				clock.clone(),
			);

			let mut result = Vec::new();
			for item in comp_iter.by_ref() {
				result.push(item.unwrap());
			}

			// Flush to mark entries as stale
			comp_iter.flush_delete_list_batch().unwrap();

			// Should return nothing (Delete at bottom level discards everything)
			assert_eq!(result.len(), 0);

			// The Replace should be marked as stale
			assert!(vlog.is_stale(2000).unwrap());
			// Note: Delete at bottom level doesn't get marked as stale since
			// it's not returned
		}

		// Test case 8: Multiple Replace operations followed by Delete
		// Should preserve only the Delete (non-bottom) or nothing (bottom)
		{
			let mut items1 = Vec::new();
			let mut items2 = Vec::new();
			let mut items3 = Vec::new();

			// Table 1: Multiple Replace operations
			let key1 = create_internal_key("test_key7", 2200, InternalKeyKind::Replace);
			let value1 = create_vlog_value(&vlog, b"test_key7", b"replace_value1");
			items1.push((key1, value1));

			let key2 = create_internal_key("test_key7", 2300, InternalKeyKind::Replace);
			let value2 = create_vlog_value(&vlog, b"test_key7", b"replace_value2");
			items1.push((key2, value2));

			// Table 2: Another Replace operation
			let key3 = create_internal_key("test_key7", 2400, InternalKeyKind::Replace);
			let value3 = create_vlog_value(&vlog, b"test_key7", b"replace_value3");
			items2.push((key3, value3));

			// Table 3: Delete operation (latest)
			let key4 = create_internal_key("test_key7", 2500, InternalKeyKind::Delete);
			let value4 = Vec::new(); // Empty value for delete
			items3.push((key4, value4));

			// Create iterators
			let iter1 = Box::new(MockIterator::new(items1));
			let iter2 = Box::new(MockIterator::new(items2));
			let iter3 = Box::new(MockIterator::new(items3));

			// Test non-bottom level compaction
			let mut comp_iter = CompactionIterator::new(
				vec![iter1, iter2, iter3],
				false, // non-bottom level
				Some(vlog.clone()),
				true, // enable versioning
				1000, // retention period
				clock.clone(),
			);

			let mut result = Vec::new();
			for item in comp_iter.by_ref() {
				result.push(item.unwrap());
			}

			// Flush to mark entries as stale
			comp_iter.flush_delete_list_batch().unwrap();

			// Should return only the Delete version
			assert_eq!(result.len(), 1);
			let (returned_key, _) = &result[0];
			assert_eq!(returned_key.seq_num(), 2500);
			assert!(returned_key.is_hard_delete_marker());
			assert_eq!(returned_key.kind(), InternalKeyKind::Delete);

			// All Replace versions should be marked as stale
			assert!(vlog.is_stale(2200).unwrap());
			assert!(vlog.is_stale(2300).unwrap());
			assert!(vlog.is_stale(2400).unwrap());
			assert!(!vlog.is_stale(2500).unwrap());
		}

		// Test case 9: Multiple Replace operations without versioning enabled
		// Should still preserve only the latest version
		{
			let mut items1 = Vec::new();
			let mut items2 = Vec::new();

			// Table 1: Multiple Replace operations
			let key1 = create_internal_key("test_key8", 2600, InternalKeyKind::Replace);
			let value1 = create_vlog_value(&vlog, b"test_key8", b"replace_value1");
			items1.push((key1, value1));

			let key2 = create_internal_key("test_key8", 2700, InternalKeyKind::Replace);
			let value2 = create_vlog_value(&vlog, b"test_key8", b"replace_value2");
			items1.push((key2, value2));

			// Table 2: Another Replace operation (latest)
			let key3 = create_internal_key("test_key8", 2800, InternalKeyKind::Replace);
			let value3 = create_vlog_value(&vlog, b"test_key8", b"replace_value3");
			items2.push((key3, value3));

			// Create iterators
			let iter1 = Box::new(MockIterator::new(items1));
			let iter2 = Box::new(MockIterator::new(items2));

			// Test compaction without versioning
			let mut comp_iter = CompactionIterator::new(
				vec![iter1, iter2],
				false, // non-bottom level
				Some(vlog.clone()),
				false, // versioning disabled
				1000,  // retention period (ignored when versioning is disabled)
				clock.clone(),
			);

			let mut result = Vec::new();
			for item in comp_iter.by_ref() {
				result.push(item.unwrap());
			}

			// Flush to mark entries as stale
			comp_iter.flush_delete_list_batch().unwrap();

			// Should return only the latest Replace version
			assert_eq!(result.len(), 1);
			let (returned_key, _) = &result[0];
			assert_eq!(returned_key.seq_num(), 2800);
			assert!(returned_key.is_replace());

			// Older Replace versions should be marked as stale
			assert!(vlog.is_stale(2600).unwrap());
			assert!(vlog.is_stale(2700).unwrap());
			assert!(!vlog.is_stale(2800).unwrap());
		}

		// Test case 10: SET -> Replace -> SET without versioning
		// Should preserve only the latest SET
		{
			let mut items1 = Vec::new();
			let mut items2 = Vec::new();
			let mut items3 = Vec::new();

			// Table 1: SET operation
			let key1 = create_internal_key("test_key9", 2900, InternalKeyKind::Set);
			let value1 = create_vlog_value(&vlog, b"test_key9", b"set_value1");
			items1.push((key1, value1));

			// Table 2: Replace operation
			let key2 = create_internal_key("test_key9", 3000, InternalKeyKind::Replace);
			let value2 = create_vlog_value(&vlog, b"test_key9", b"replace_value");
			items2.push((key2, value2));

			// Table 3: Final SET operation (latest)
			let key3 = create_internal_key("test_key9", 3100, InternalKeyKind::Set);
			let value3 = create_vlog_value(&vlog, b"test_key9", b"final_set_value");
			items3.push((key3, value3));

			// Create iterators
			let iter1 = Box::new(MockIterator::new(items1));
			let iter2 = Box::new(MockIterator::new(items2));
			let iter3 = Box::new(MockIterator::new(items3));

			// Test compaction without versioning
			let mut comp_iter = CompactionIterator::new(
				vec![iter1, iter2, iter3],
				false, // non-bottom level
				Some(vlog.clone()),
				false, // versioning disabled
				1000,  // retention period (ignored when versioning is disabled)
				clock.clone(),
			);

			let mut result = Vec::new();
			for item in comp_iter.by_ref() {
				result.push(item.unwrap());
			}

			// Flush to mark entries as stale
			comp_iter.flush_delete_list_batch().unwrap();

			// Should return only the latest SET version
			assert_eq!(result.len(), 1);
			let (returned_key, _) = &result[0];
			assert_eq!(returned_key.seq_num(), 3100);
			assert!(!returned_key.is_replace());
			assert_eq!(returned_key.kind(), InternalKeyKind::Set);

			// All older versions should be marked as stale
			assert!(vlog.is_stale(2900).unwrap());
			assert!(vlog.is_stale(3000).unwrap());
			assert!(!vlog.is_stale(3100).unwrap());
		}

		// Test case 11: Replace -> Delete without versioning
		// Should preserve only the Delete (non-bottom) or nothing (bottom)
		{
			let mut items1 = Vec::new();
			let mut items2 = Vec::new();

			// Table 1: Replace operation
			let key1 = create_internal_key("test_key10", 3200, InternalKeyKind::Replace);
			let value1 = create_vlog_value(&vlog, b"test_key10", b"replace_value");
			items1.push((key1, value1));

			// Table 2: Delete operation (latest)
			let key2 = create_internal_key("test_key10", 3300, InternalKeyKind::Delete);
			let value2 = Vec::new(); // Empty value for delete
			items2.push((key2, value2));

			// Create iterators
			let iter1 = Box::new(MockIterator::new(items1));
			let iter2 = Box::new(MockIterator::new(items2));

			// Test non-bottom level compaction without versioning
			let mut comp_iter = CompactionIterator::new(
				vec![iter1, iter2],
				false, // non-bottom level
				Some(vlog.clone()),
				false, // versioning disabled
				1000,  // retention period (ignored when versioning is disabled)
				clock.clone(),
			);

			let mut result = Vec::new();
			for item in comp_iter.by_ref() {
				result.push(item.unwrap());
			}

			// Flush to mark entries as stale
			comp_iter.flush_delete_list_batch().unwrap();

			// Should return only the Delete version
			assert_eq!(result.len(), 1);
			let (returned_key, _) = &result[0];
			assert_eq!(returned_key.seq_num(), 3300);
			assert!(returned_key.is_hard_delete_marker());
			assert_eq!(returned_key.kind(), InternalKeyKind::Delete);

			// The Replace should be marked as stale
			assert!(vlog.is_stale(3200).unwrap());
			assert!(!vlog.is_stale(3300).unwrap());
		}

		// Test case 12: Replace -> Delete at bottom level without versioning
		// Should return nothing (Delete at bottom level discards everything)
		{
			let mut items1 = Vec::new();
			let mut items2 = Vec::new();

			// Table 1: Replace operation
			let key1 = create_internal_key("test_key11", 3400, InternalKeyKind::Replace);
			let value1 = create_vlog_value(&vlog, b"test_key11", b"replace_value");
			items1.push((key1, value1));

			// Table 2: Delete operation (latest)
			let key2 = create_internal_key("test_key11", 3500, InternalKeyKind::Delete);
			let value2 = Vec::new(); // Empty value for delete
			items2.push((key2, value2));

			// Create iterators
			let iter1 = Box::new(MockIterator::new(items1));
			let iter2 = Box::new(MockIterator::new(items2));

			// Test bottom level compaction without versioning
			let mut comp_iter = CompactionIterator::new(
				vec![iter1, iter2],
				true, // bottom level
				Some(vlog.clone()),
				false, // versioning disabled
				1000,  // retention period (ignored when versioning is disabled)
				clock,
			);

			let mut result = Vec::new();
			for item in comp_iter.by_ref() {
				result.push(item.unwrap());
			}

			// Flush to mark entries as stale
			comp_iter.flush_delete_list_batch().unwrap();

			// Should return nothing (Delete at bottom level discards everything)
			assert_eq!(result.len(), 0);

			// The Replace should be marked as stale
			assert!(vlog.is_stale(3400).unwrap());
			// Note: Delete at bottom level doesn't get marked as stale since
			// it's not returned
		}
	}
}
