use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tempfile::TempDir;
use test_log::test;

use crate::clock::{LogicalClock, MockLogicalClock};
use crate::comparator::{BytewiseComparator, InternalKeyComparator};
use crate::iter::{BoxedLSMIterator, CompactionIterator, MergingIterator};
use crate::sstable::table::{Table, TableWriter};
use crate::vfs::File;
use crate::vlog::{VLog, ValueLocation};
use crate::{InternalKey, InternalKeyKind, LSMIterator, Options, VLogChecksumLevel, Value};

/// Global counter for generating unique table IDs in tests
static TEST_TABLE_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

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

	let vlog = Arc::new(VLog::new(Arc::new(opts)).unwrap());
	(vlog, temp_dir)
}

fn create_vlog_value(vlog: &Arc<VLog>, key: &[u8], value: &[u8]) -> Value {
	let pointer = vlog.append(key, value).unwrap();
	ValueLocation::with_pointer(pointer).encode()
}

fn create_comparator() -> Arc<InternalKeyComparator> {
	Arc::new(InternalKeyComparator::new(Arc::new(BytewiseComparator::default())))
}

/// Wraps a Vec<u8> as a File for in-memory table reading
fn wrap_buffer(src: Vec<u8>) -> Arc<dyn File> {
	Arc::new(src)
}

/// Builds an in-memory SSTable and returns its iterator.
/// This uses real SSTable seek behavior with InternalKeyComparator.
///
/// Unlike MockIterator, this properly handles internal key ordering
/// where higher sequence numbers come before lower ones for the same user key.
fn build_table_iterator(entries: Vec<(InternalKey, Value)>) -> BoxedLSMIterator<'static> {
	let table_id = TEST_TABLE_ID_COUNTER.fetch_add(1, Ordering::Relaxed);

	let mut buf = Vec::with_capacity(512);
	let opts = Arc::new(Options::new());

	{
		let mut writer = TableWriter::new(&mut buf, table_id, Arc::clone(&opts), 0);
		for (key, value) in entries {
			writer.add(key, &value).unwrap();
		}
		writer.finish().unwrap();
	}

	let size = buf.len() as u64;
	let table = Table::new(table_id, opts, wrap_buffer(buf), size).unwrap();

	// Leak the table to get 'static lifetime (acceptable for tests)
	let table: &'static Table = Box::leak(Box::new(table));
	Box::new(table.iter(None).unwrap())
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

	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);

	// Create the merge iterator
	let mut merge_iter = CompactionIterator::new(
		vec![iter1, iter2],
		create_comparator(),
		false,
		false, // versioning disabled
		0,
		Arc::new(MockLogicalClock::new()),
		vec![],
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

	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);

	// Test non-bottom level (should keep hard delete entries)
	let mut comp_iter = CompactionIterator::new(
		vec![iter1, iter2],
		create_comparator(),
		false,
		false,
		0,
		Arc::new(MockLogicalClock::new()),
		vec![],
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

	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);

	// Use bottom level
	let mut comp_iter = CompactionIterator::new(
		vec![iter1, iter2],
		create_comparator(),
		true, // bottom level
		false,
		0,
		Arc::new(MockLogicalClock::new()),
		vec![],
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
			assert!(!bottom_result.contains(&key), "Key {key} should be removed at bottom level");
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

	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);
	let iter3 = build_table_iterator(items3);

	// Create compaction iterator (non-bottom level)
	let mut comp_iter = CompactionIterator::new(
		vec![iter1, iter2, iter3],
		create_comparator(),
		false, // not bottom level
		false,
		0,
		Arc::new(MockLogicalClock::default()),
		vec![],
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

	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);
	let iter3 = build_table_iterator(items3);

	let mut comp_iter = CompactionIterator::new(
		vec![iter1, iter2, iter3],
		create_comparator(),
		false, // not bottom level
		false,
		0,
		Arc::new(MockLogicalClock::default()),
		vec![],
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

	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);

	let mut comp_iter = CompactionIterator::new(
		vec![iter1, iter2],
		create_comparator(),
		true, // bottom level
		false,
		0,
		Arc::new(MockLogicalClock::default()),
		vec![],
	);

	// At bottom level, hard_delete should NOT be returned
	let result: Vec<_> = comp_iter.by_ref().map(|r| r.unwrap()).collect();
	assert_eq!(result.len(), 0, "At bottom level, hard_delete should not be returned");
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

	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);

	let mut comp_iter = CompactionIterator::new(
		vec![iter1, iter2],
		create_comparator(),
		false, // not bottom level
		false,
		0,
		Arc::new(MockLogicalClock::default()),
		vec![],
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

	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);
	let iter3 = build_table_iterator(items3);

	let mut comp_iter = CompactionIterator::new(
		vec![iter1, iter2, iter3],
		create_comparator(),
		false, // not bottom level
		false,
		0,
		Arc::new(MockLogicalClock::default()),
		vec![],
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

	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);

	let mut comp_iter = CompactionIterator::new(
		vec![iter1, iter2],
		create_comparator(),
		false, // not bottom level
		false,
		0,
		Arc::new(MockLogicalClock::default()),
		vec![],
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

	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);
	let iter3 = build_table_iterator(items3);

	let mut comp_iter = CompactionIterator::new(
		vec![iter1, iter2, iter3],
		create_comparator(),
		false, // not bottom level
		false,
		0,
		Arc::new(MockLogicalClock::default()),
		vec![],
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

	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);
	let iter3 = build_table_iterator(items3);

	// Test with versioning enabled and 5-second retention period
	let clock = Arc::new(MockLogicalClock::with_timestamp(current_time));
	let mut comp_iter = CompactionIterator::new(
		vec![iter1, iter2, iter3],
		create_comparator(),
		false, // not bottom level
		true,  // enable versioning
		retention_period_ns,
		clock,
		vec![],
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

	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);
	let iter3 = build_table_iterator(items3);
	let iter4 = build_table_iterator(items4);

	// Test with versioning enabled, 5-second retention period, and BOTTOM LEVEL
	let clock = Arc::new(MockLogicalClock::with_timestamp(current_time));
	let mut comp_iter = CompactionIterator::new(
		vec![iter1, iter2, iter3, iter4],
		create_comparator(),
		true, // BOTTOM LEVEL - delete markers should be dropped
		true, // enable versioning
		retention_period_ns,
		clock,
		vec![],
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
	assert_eq!(key2_versions.len(), 0, "key2 should have 0 versions (DELETE-only at bottom level)");

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

	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);
	let iter3 = build_table_iterator(items3);

	// Test with versioning DISABLED at NON-BOTTOM level
	let clock = Arc::new(MockLogicalClock::with_timestamp(current_time));
	let mut comp_iter = CompactionIterator::new(
		vec![iter1, iter2, iter3],
		create_comparator(),
		false, // NON-BOTTOM LEVEL
		false, // VERSIONING DISABLED
		retention_period_ns,
		clock,
		vec![],
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

	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);
	let iter3 = build_table_iterator(items3);

	// Test with versioning DISABLED at BOTTOM level
	let clock = Arc::new(MockLogicalClock::with_timestamp(current_time));
	let mut comp_iter = CompactionIterator::new(
		vec![iter1, iter2, iter3],
		create_comparator(),
		true,  // BOTTOM LEVEL
		false, // VERSIONING DISABLED
		retention_period_ns,
		clock,
		vec![],
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
	assert_eq!(key3_versions.len(), 0, "key3 should have 0 versions (DELETE-only at bottom level)");
}

#[test(tokio::test)]
async fn test_compaction_iterator_set_with_delete_behavior() {
	let (vlog, _tmp_dir) = create_test_vlog();
	let clock = Arc::new(MockLogicalClock::new());

	// Create test data with Replace operations
	let mut items1 = Vec::new();
	let mut items2 = Vec::new();

	// Table 1: Regular SET operations (higher seq first for same user key)
	let key2 = create_internal_key("test_key", 200, InternalKeyKind::Set);
	let value2 = create_vlog_value(&vlog, b"test_key", b"value2");
	items1.push((key2, value2));

	let key1 = create_internal_key("test_key", 100, InternalKeyKind::Set);
	let value1 = create_vlog_value(&vlog, b"test_key", b"value1");
	items1.push((key1, value1));

	// Table 2: Replace operation (newer sequence number)
	let key3 = create_internal_key("test_key", 300, InternalKeyKind::Replace);
	let value3 = create_vlog_value(&vlog, b"test_key", b"set_with_delete_value");
	items2.push((key3, value3));

	// Create iterators
	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);

	// Test non-bottom level compaction (should preserve Replace)
	let mut comp_iter = CompactionIterator::new(
		vec![iter1, iter2],
		create_comparator(),
		false, // non-bottom level
		true,  // enable versioning
		1000,  // retention period
		clock,
		vec![],
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
}

#[test(tokio::test)]
async fn test_compaction_iterator_set_with_delete_marks_older_versions_stale() {
	let (vlog, _tmp_dir) = create_test_vlog();
	let clock = Arc::new(MockLogicalClock::new());

	// Create test data with multiple versions and Replace
	let mut items1 = Vec::new();
	let mut items2 = Vec::new();

	// Table 1: Multiple regular SET operations (higher seq first for same user key)
	let key3 = create_internal_key("test_key", 250, InternalKeyKind::Set);
	let value3 = create_vlog_value(&vlog, b"test_key", b"value3");
	items1.push((key3, value3));

	let key2 = create_internal_key("test_key", 200, InternalKeyKind::Set);
	let value2 = create_vlog_value(&vlog, b"test_key", b"value2");
	items1.push((key2, value2));

	let key1 = create_internal_key("test_key", 100, InternalKeyKind::Set);
	let value1 = create_vlog_value(&vlog, b"test_key", b"value1");
	items1.push((key1, value1));

	// Table 2: Replace operation (not the latest) and Regular SET after Replace (latest)
	// Higher seq first for same user key
	let key5 = create_internal_key("test_key", 400, InternalKeyKind::Set);
	let value5 = create_vlog_value(&vlog, b"test_key", b"final_value");
	items2.push((key5, value5));

	let key4 = create_internal_key("test_key", 300, InternalKeyKind::Replace);
	let value4 = create_vlog_value(&vlog, b"test_key", b"set_with_delete_value");
	items2.push((key4, value4));

	// Create iterators
	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);

	// Test compaction
	let mut comp_iter = CompactionIterator::new(
		vec![iter1, iter2],
		create_comparator(),
		false, // non-bottom level
		true,  // enable versioning
		1000,  // retention period
		clock,
		vec![],
	);

	let mut result = Vec::new();
	for item in comp_iter.by_ref() {
		result.push(item.unwrap());
	}

	// Should return only the latest version (regular SET, not Replace)
	assert_eq!(result.len(), 1);
	let (returned_key, _) = &result[0];
	assert_eq!(returned_key.seq_num(), 400);
	assert!(!returned_key.is_replace());
}

#[test(tokio::test)]
async fn test_compaction_iterator_set_with_delete_latest_version() {
	let (vlog, _tmp_dir) = create_test_vlog();
	let clock = Arc::new(MockLogicalClock::new());

	// Create test data where Replace is the latest version
	let mut items1 = Vec::new();
	let mut items2 = Vec::new();

	// Table 1: Regular SET operations (higher seq first for same user key)
	let key2 = create_internal_key("test_key", 200, InternalKeyKind::Set);
	let value2 = create_vlog_value(&vlog, b"test_key", b"value2");
	items1.push((key2, value2));

	let key1 = create_internal_key("test_key", 100, InternalKeyKind::Set);
	let value1 = create_vlog_value(&vlog, b"test_key", b"value1");
	items1.push((key1, value1));

	// Table 2: Replace as the latest version
	let key3 = create_internal_key("test_key", 300, InternalKeyKind::Replace);
	let value3 = create_vlog_value(&vlog, b"test_key", b"set_with_delete_final");
	items2.push((key3, value3));

	// Create iterators
	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);

	// Test compaction
	let mut comp_iter = CompactionIterator::new(
		vec![iter1, iter2],
		create_comparator(),
		false, // non-bottom level
		true,  // enable versioning
		1000,  // retention period
		clock,
		vec![],
	);

	let mut result = Vec::new();
	for item in comp_iter.by_ref() {
		result.push(item.unwrap());
	}

	// Should return the Replace version (latest)
	assert_eq!(result.len(), 1);
	let (returned_key, _) = &result[0];
	assert_eq!(returned_key.seq_num(), 300);
	assert!(returned_key.is_replace());
}

#[test(tokio::test)]
async fn test_compaction_iterator_set_with_delete_mixed_with_hard_delete() {
	let (vlog, _tmp_dir) = create_test_vlog();
	let clock = Arc::new(MockLogicalClock::new());

	// Create test data with Replace and hard delete operations
	let mut items1 = Vec::new();
	let mut items2 = Vec::new();

	// Table 1: Regular SET and hard delete (higher seq first for same user key)
	let key2 = create_internal_key("test_key", 200, InternalKeyKind::Delete);
	let value2 = Value::from(vec![]); // Empty value for delete
	items1.push((key2, value2));

	let key1 = create_internal_key("test_key", 100, InternalKeyKind::Set);
	let value1 = create_vlog_value(&vlog, b"test_key", b"value1");
	items1.push((key1, value1));

	// Table 2: Replace operation
	let key3 = create_internal_key("test_key", 300, InternalKeyKind::Replace);
	let value3 = create_vlog_value(&vlog, b"test_key", b"set_with_delete_value");
	items2.push((key3, value3));

	// Create iterators
	let iter1 = build_table_iterator(items1);
	let iter2 = build_table_iterator(items2);

	// Test non-bottom level compaction
	let mut comp_iter = CompactionIterator::new(
		vec![iter1, iter2],
		create_comparator(),
		false, // non-bottom level
		true,  // enable versioning
		1000,  // retention period
		clock,
		vec![],
	);

	let mut result = Vec::new();
	for item in comp_iter.by_ref() {
		result.push(item.unwrap());
	}

	// Should return the Replace version (latest)
	assert_eq!(result.len(), 1);
	let (returned_key, _) = &result[0];
	assert_eq!(returned_key.seq_num(), 300);
	assert!(returned_key.is_replace());
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

		// Table 1: Multiple Replace operations (higher seq first for same user key)
		let key2 = create_internal_key("test_key", 200, InternalKeyKind::Replace);
		let value2 = create_vlog_value(&vlog, b"test_key", b"replace_value2");
		items1.push((key2, value2));

		let key1 = create_internal_key("test_key", 100, InternalKeyKind::Replace);
		let value1 = create_vlog_value(&vlog, b"test_key", b"replace_value1");
		items1.push((key1, value1));

		// Table 2: Another Replace operation (latest)
		let key3 = create_internal_key("test_key", 300, InternalKeyKind::Replace);
		let value3 = create_vlog_value(&vlog, b"test_key", b"replace_value3");
		items2.push((key3, value3));

		// Create iterators
		let iter1 = build_table_iterator(items1);
		let iter2 = build_table_iterator(items2);

		// Test non-bottom level compaction
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			create_comparator(),
			false, // non-bottom level
			true,  // enable versioning
			1000,  // retention period
			Arc::clone(&clock) as Arc<dyn LogicalClock>,
			vec![],
		);

		let mut result = Vec::new();
		for item in comp_iter.by_ref() {
			result.push(item.unwrap());
		}

		// Should return only the latest Replace version
		assert_eq!(result.len(), 1);
		let (returned_key, _) = &result[0];
		assert_eq!(returned_key.seq_num(), 300);
		assert!(returned_key.is_replace());
	}

	// Test case 2: Multiple Replace operations at bottom level
	// Should still preserve only the latest Replace
	{
		let mut items1 = Vec::new();
		let mut items2 = Vec::new();

		// Table 1: Multiple Replace operations (higher seq first for same user key)
		let key2 = create_internal_key("test_key2", 500, InternalKeyKind::Replace);
		let value2 = create_vlog_value(&vlog, b"test_key2", b"replace_value5");
		items1.push((key2, value2));

		let key1 = create_internal_key("test_key2", 400, InternalKeyKind::Replace);
		let value1 = create_vlog_value(&vlog, b"test_key2", b"replace_value4");
		items1.push((key1, value1));

		// Table 2: Another Replace operation (latest)
		let key3 = create_internal_key("test_key2", 600, InternalKeyKind::Replace);
		let value3 = create_vlog_value(&vlog, b"test_key2", b"replace_value6");
		items2.push((key3, value3));

		// Create iterators
		let iter1 = build_table_iterator(items1);
		let iter2 = build_table_iterator(items2);

		// Test bottom level compaction
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			create_comparator(),
			true, // bottom level
			true, // enable versioning
			1000, // retention period
			Arc::clone(&clock) as Arc<dyn LogicalClock>,
			vec![],
		);

		let mut result = Vec::new();
		for item in comp_iter.by_ref() {
			result.push(item.unwrap());
		}

		// Should return only the latest Replace version
		assert_eq!(result.len(), 1);
		let (returned_key, _) = &result[0];
		assert_eq!(returned_key.seq_num(), 600);
		assert!(returned_key.is_replace());
	}

	// Test case 3: Multiple SET operations followed by Replace
	// Only the Replace should be preserved
	{
		let mut items1 = Vec::new();
		let mut items2 = Vec::new();

		// Table 1: Multiple regular SET operations (higher seq first for same user key)
		let key3 = create_internal_key("test_key3", 850, InternalKeyKind::Set);
		let value3 = create_vlog_value(&vlog, b"test_key3", b"set_value3");
		items1.push((key3, value3));

		let key2 = create_internal_key("test_key3", 800, InternalKeyKind::Set);
		let value2 = create_vlog_value(&vlog, b"test_key3", b"set_value2");
		items1.push((key2, value2));

		let key1 = create_internal_key("test_key3", 700, InternalKeyKind::Set);
		let value1 = create_vlog_value(&vlog, b"test_key3", b"set_value1");
		items1.push((key1, value1));

		// Table 2: Replace operation (latest)
		let key4 = create_internal_key("test_key3", 900, InternalKeyKind::Replace);
		let value4 = create_vlog_value(&vlog, b"test_key3", b"replace_value7");
		items2.push((key4, value4));

		// Create iterators
		let iter1 = build_table_iterator(items1);
		let iter2 = build_table_iterator(items2);

		// Test compaction
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			create_comparator(),
			false, // non-bottom level
			true,  // enable versioning
			1000,  // retention period
			Arc::clone(&clock) as Arc<dyn LogicalClock>,
			vec![],
		);

		let mut result = Vec::new();
		for item in comp_iter.by_ref() {
			result.push(item.unwrap());
		}

		// Should return only the Replace version
		assert_eq!(result.len(), 1);
		let (returned_key, _) = &result[0];
		assert_eq!(returned_key.seq_num(), 900);
		assert!(returned_key.is_replace());
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
		let iter1 = build_table_iterator(items1);
		let iter2 = build_table_iterator(items2);

		// Test compaction
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			create_comparator(),
			false, // non-bottom level
			true,  // enable versioning
			1000,  // retention period
			Arc::clone(&clock) as Arc<dyn LogicalClock>,
			vec![],
		);

		let mut result = Vec::new();
		for item in comp_iter.by_ref() {
			result.push(item.unwrap());
		}

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
	}

	// Test case 5: Multiple SET operations followed by Replace followed by SET
	// Only the final SET should be preserved
	{
		let mut items1 = Vec::new();
		let mut items2 = Vec::new();
		let mut items3 = Vec::new();

		// Table 1: Multiple regular SET operations (higher seq first for same user key)
		let key2 = create_internal_key("test_key4", 1500, InternalKeyKind::Set);
		let value2 = create_vlog_value(&vlog, b"test_key4", b"set_value2");
		items1.push((key2, value2));

		let key1 = create_internal_key("test_key4", 1400, InternalKeyKind::Set);
		let value1 = create_vlog_value(&vlog, b"test_key4", b"set_value1");
		items1.push((key1, value1));

		// Table 2: Replace operation
		let key3 = create_internal_key("test_key4", 1600, InternalKeyKind::Replace);
		let value3 = create_vlog_value(&vlog, b"test_key4", b"replace_value");
		items2.push((key3, value3));

		// Table 3: Final SET operation (latest)
		let key4 = create_internal_key("test_key4", 1700, InternalKeyKind::Set);
		let value4 = create_vlog_value(&vlog, b"test_key4", b"final_set_value");
		items3.push((key4, value4));

		// Create iterators
		let iter1 = build_table_iterator(items1);
		let iter2 = build_table_iterator(items2);
		let iter3 = build_table_iterator(items3);

		// Test compaction
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2, iter3],
			create_comparator(),
			false, // non-bottom level
			true,  // enable versioning
			1000,  // retention period
			Arc::clone(&clock) as Arc<dyn LogicalClock>,
			vec![],
		);

		let mut result = Vec::new();
		for item in comp_iter.by_ref() {
			result.push(item.unwrap());
		}

		// Should return only the final SET version
		assert_eq!(result.len(), 1);
		let (returned_key, _) = &result[0];
		assert_eq!(returned_key.seq_num(), 1700);
		assert!(!returned_key.is_replace());
		assert_eq!(returned_key.kind(), InternalKeyKind::Set);
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
		let iter1 = build_table_iterator(items1);
		let iter2 = build_table_iterator(items2);

		// Test non-bottom level compaction
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			create_comparator(),
			false, // non-bottom level
			true,  // enable versioning
			1000,  // retention period
			Arc::clone(&clock) as Arc<dyn LogicalClock>,
			vec![],
		);

		let mut result = Vec::new();
		for item in comp_iter.by_ref() {
			result.push(item.unwrap());
		}

		// Should return only the Delete version
		assert_eq!(result.len(), 1);
		let (returned_key, _) = &result[0];
		assert_eq!(returned_key.seq_num(), 1900);
		assert!(returned_key.is_hard_delete_marker());
		assert_eq!(returned_key.kind(), InternalKeyKind::Delete);
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
		let iter1 = build_table_iterator(items1);
		let iter2 = build_table_iterator(items2);

		// Test bottom level compaction
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			create_comparator(),
			true, // bottom level
			true, // enable versioning
			1000, // retention period
			Arc::clone(&clock) as Arc<dyn LogicalClock>,
			vec![],
		);

		let mut result = Vec::new();
		for item in comp_iter.by_ref() {
			result.push(item.unwrap());
		}

		// Should return nothing (Delete at bottom level discards everything)
		assert_eq!(result.len(), 0);
	}

	// Test case 8: Multiple Replace operations followed by Delete
	// Should preserve only the Delete (non-bottom) or nothing (bottom)
	{
		let mut items1 = Vec::new();
		let mut items2 = Vec::new();
		let mut items3 = Vec::new();

		// Table 1: Multiple Replace operations (higher seq first for same user key)
		let key2 = create_internal_key("test_key7", 2300, InternalKeyKind::Replace);
		let value2 = create_vlog_value(&vlog, b"test_key7", b"replace_value2");
		items1.push((key2, value2));

		let key1 = create_internal_key("test_key7", 2200, InternalKeyKind::Replace);
		let value1 = create_vlog_value(&vlog, b"test_key7", b"replace_value1");
		items1.push((key1, value1));

		// Table 2: Another Replace operation
		let key3 = create_internal_key("test_key7", 2400, InternalKeyKind::Replace);
		let value3 = create_vlog_value(&vlog, b"test_key7", b"replace_value3");
		items2.push((key3, value3));

		// Table 3: Delete operation (latest)
		let key4 = create_internal_key("test_key7", 2500, InternalKeyKind::Delete);
		let value4 = Vec::new(); // Empty value for delete
		items3.push((key4, value4));

		// Create iterators
		let iter1 = build_table_iterator(items1);
		let iter2 = build_table_iterator(items2);
		let iter3 = build_table_iterator(items3);

		// Test non-bottom level compaction
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2, iter3],
			create_comparator(),
			false, // non-bottom level
			true,  // enable versioning
			1000,  // retention period
			Arc::clone(&clock) as Arc<dyn LogicalClock>,
			vec![],
		);

		let mut result = Vec::new();
		for item in comp_iter.by_ref() {
			result.push(item.unwrap());
		}

		// Should return only the Delete version
		assert_eq!(result.len(), 1);
		let (returned_key, _) = &result[0];
		assert_eq!(returned_key.seq_num(), 2500);
		assert!(returned_key.is_hard_delete_marker());
		assert_eq!(returned_key.kind(), InternalKeyKind::Delete);
	}

	// Test case 9: Multiple Replace operations without versioning enabled
	// Should still preserve only the latest version
	{
		let mut items1 = Vec::new();
		let mut items2 = Vec::new();

		// Table 1: Multiple Replace operations (higher seq first for same user key)
		let key2 = create_internal_key("test_key8", 2700, InternalKeyKind::Replace);
		let value2 = create_vlog_value(&vlog, b"test_key8", b"replace_value2");
		items1.push((key2, value2));

		let key1 = create_internal_key("test_key8", 2600, InternalKeyKind::Replace);
		let value1 = create_vlog_value(&vlog, b"test_key8", b"replace_value1");
		items1.push((key1, value1));

		// Table 2: Another Replace operation (latest)
		let key3 = create_internal_key("test_key8", 2800, InternalKeyKind::Replace);
		let value3 = create_vlog_value(&vlog, b"test_key8", b"replace_value3");
		items2.push((key3, value3));

		// Create iterators
		let iter1 = build_table_iterator(items1);
		let iter2 = build_table_iterator(items2);

		// Test compaction without versioning
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			create_comparator(),
			false, // non-bottom level
			false, // versioning disabled
			1000,  // retention period (ignored when versioning is disabled)
			Arc::clone(&clock) as Arc<dyn LogicalClock>,
			vec![],
		);

		let mut result = Vec::new();
		for item in comp_iter.by_ref() {
			result.push(item.unwrap());
		}

		// Should return only the latest Replace version
		assert_eq!(result.len(), 1);
		let (returned_key, _) = &result[0];
		assert_eq!(returned_key.seq_num(), 2800);
		assert!(returned_key.is_replace());
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
		let iter1 = build_table_iterator(items1);
		let iter2 = build_table_iterator(items2);
		let iter3 = build_table_iterator(items3);

		// Test compaction without versioning
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2, iter3],
			create_comparator(),
			false, // non-bottom level
			false, // versioning disabled
			1000,  // retention period (ignored when versioning is disabled)
			Arc::clone(&clock) as Arc<dyn LogicalClock>,
			vec![],
		);

		let mut result = Vec::new();
		for item in comp_iter.by_ref() {
			result.push(item.unwrap());
		}

		// Should return only the latest SET version
		assert_eq!(result.len(), 1);
		let (returned_key, _) = &result[0];
		assert_eq!(returned_key.seq_num(), 3100);
		assert!(!returned_key.is_replace());
		assert_eq!(returned_key.kind(), InternalKeyKind::Set);
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
		let iter1 = build_table_iterator(items1);
		let iter2 = build_table_iterator(items2);

		// Test non-bottom level compaction without versioning
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			create_comparator(),
			false, // non-bottom level
			false, // versioning disabled
			1000,  // retention period (ignored when versioning is disabled)
			Arc::clone(&clock) as Arc<dyn LogicalClock>,
			vec![],
		);

		let mut result = Vec::new();
		for item in comp_iter.by_ref() {
			result.push(item.unwrap());
		}

		// Should return only the Delete version
		assert_eq!(result.len(), 1);
		let (returned_key, _) = &result[0];
		assert_eq!(returned_key.seq_num(), 3300);
		assert!(returned_key.is_hard_delete_marker());
		assert_eq!(returned_key.kind(), InternalKeyKind::Delete);
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
		let iter1 = build_table_iterator(items1);
		let iter2 = build_table_iterator(items2);

		// Test bottom level compaction without versioning
		let mut comp_iter = CompactionIterator::new(
			vec![iter1, iter2],
			create_comparator(),
			true,  // bottom level
			false, // versioning disabled
			1000,  // retention period (ignored when versioning is disabled)
			clock,
			vec![],
		);

		let mut result = Vec::new();
		for item in comp_iter.by_ref() {
			result.push(item.unwrap());
		}

		// Should return nothing (Delete at bottom level discards everything)
		assert_eq!(result.len(), 0);
	}
}

// =============================================================================
// MergingIterator Direction Switching Tests
// =============================================================================

/// Test that direction switching correctly handles duplicate keys across levels.
///
/// This test verifies the fix for a bug where switching from forward to backward
/// iteration could skip entries when multiple levels have the same user key.
///
/// Scenario:
/// - Child 0 (level 0, newer): keys [1, 3(seq=200), 5]
/// - Child 1 (level 1, older): keys [2, 3(seq=100), 6]
///
/// In internal key ordering, higher seq comes first (smaller), so:
/// Forward iteration: key-1, key-2, key-3(seq=200), key-3(seq=100), key-5, key-6
/// When at key-3(seq=100) and calling prev(), we should get key-3(seq=200), NOT key-2.
#[test]
fn test_merging_iterator_direction_switch_with_duplicate_keys() {
	// Create two child iterators with overlapping key "3"
	// Child 0 (level 0, newer): key-3 with seq=200
	// Child 1 (level 1, older): key-3 with seq=100
	// Higher seq = smaller internal key = comes first in forward iteration

	let iter0 = build_table_iterator(vec![
		(create_internal_key("key-1", 100, InternalKeyKind::Set), Value::from(b"v1".to_vec())),
		(create_internal_key("key-3", 200, InternalKeyKind::Set), Value::from(b"v3-L0".to_vec())),
		(create_internal_key("key-5", 100, InternalKeyKind::Set), Value::from(b"v5".to_vec())),
	]);

	let iter1 = build_table_iterator(vec![
		(create_internal_key("key-2", 100, InternalKeyKind::Set), Value::from(b"v2".to_vec())),
		(create_internal_key("key-3", 100, InternalKeyKind::Set), Value::from(b"v3-L1".to_vec())),
		(create_internal_key("key-6", 100, InternalKeyKind::Set), Value::from(b"v6".to_vec())),
	]);

	let cmp = create_comparator();
	let mut merge_iter = MergingIterator::new(vec![iter0, iter1], cmp);

	// Forward iteration to key-3(seq=100)
	// Expected sequence: key-1, key-2, key-3(seq=200), key-3(seq=100)
	assert!(merge_iter.seek_first().unwrap());
	assert_eq!(String::from_utf8_lossy(merge_iter.current_key().user_key()), "key-1");

	assert!(merge_iter.next().unwrap());
	assert_eq!(String::from_utf8_lossy(merge_iter.current_key().user_key()), "key-2");

	assert!(merge_iter.next().unwrap());
	assert_eq!(String::from_utf8_lossy(merge_iter.current_key().user_key()), "key-3");
	assert_eq!(merge_iter.current_key().seq_num(), 200); // Higher seq comes first

	assert!(merge_iter.next().unwrap());
	assert_eq!(String::from_utf8_lossy(merge_iter.current_key().user_key()), "key-3");
	assert_eq!(merge_iter.current_key().seq_num(), 100); // Lower seq comes second

	// Now we're at key-3(seq=100). Call prev().
	// BUG (before fix): This would return key-2, skipping key-3(seq=200)
	// EXPECTED (after fix): This should return key-3(seq=200)
	assert!(merge_iter.prev().unwrap());
	assert_eq!(
		String::from_utf8_lossy(merge_iter.current_key().user_key()),
		"key-3",
		"After prev() from key-3(seq=100), should be at key-3(seq=200), not key-2"
	);
	assert_eq!(merge_iter.current_key().seq_num(), 200);

	// Continue prev() to verify we get key-2 next
	assert!(merge_iter.prev().unwrap());
	assert_eq!(String::from_utf8_lossy(merge_iter.current_key().user_key()), "key-2");

	// And then key-1
	assert!(merge_iter.prev().unwrap());
	assert_eq!(String::from_utf8_lossy(merge_iter.current_key().user_key()), "key-1");

	// No more keys
	assert!(!merge_iter.prev().unwrap());
}

/// Test direction switching from backward to forward with duplicate keys.
///
/// In internal key ordering, higher seq = smaller key, so:
/// Forward order:  key-1, key-2, key-3(seq=200), key-3(seq=100), key-5, key-6
/// Backward order: key-6, key-5, key-3(seq=100), key-3(seq=200), key-2, key-1
#[test]
fn test_merging_iterator_direction_switch_backward_to_forward() {
	// Create two child iterators with overlapping key "3"
	// Child 0 (level 0, newer): key-3 with seq=200
	// Child 1 (level 1, older): key-3 with seq=100

	let iter0 = build_table_iterator(vec![
		(create_internal_key("key-1", 100, InternalKeyKind::Set), Value::from(b"v1".to_vec())),
		(create_internal_key("key-3", 200, InternalKeyKind::Set), Value::from(b"v3-L0".to_vec())),
		(create_internal_key("key-5", 100, InternalKeyKind::Set), Value::from(b"v5".to_vec())),
	]);

	let iter1 = build_table_iterator(vec![
		(create_internal_key("key-2", 100, InternalKeyKind::Set), Value::from(b"v2".to_vec())),
		(create_internal_key("key-3", 100, InternalKeyKind::Set), Value::from(b"v3-L1".to_vec())),
		(create_internal_key("key-6", 100, InternalKeyKind::Set), Value::from(b"v6".to_vec())),
	]);

	let cmp = create_comparator();
	let mut merge_iter = MergingIterator::new(vec![iter0, iter1], cmp);

	// Start with backward iteration from the end
	// Backward order: key-6, key-5, key-3(seq=100), key-3(seq=200), key-2, key-1
	assert!(merge_iter.seek_last().unwrap());
	assert_eq!(String::from_utf8_lossy(merge_iter.current_key().user_key()), "key-6");

	assert!(merge_iter.prev().unwrap());
	assert_eq!(String::from_utf8_lossy(merge_iter.current_key().user_key()), "key-5");

	assert!(merge_iter.prev().unwrap());
	assert_eq!(String::from_utf8_lossy(merge_iter.current_key().user_key()), "key-3");
	assert_eq!(merge_iter.current_key().seq_num(), 100); // Lower seq comes first in backward

	// Now we're at key-3(seq=100). Call next() to switch direction.
	// In forward order, key-3(seq=100) is followed by key-5.
	// EXPECTED: Should return key-5
	assert!(merge_iter.next().unwrap());
	assert_eq!(
		String::from_utf8_lossy(merge_iter.current_key().user_key()),
		"key-5",
		"After next() from key-3(seq=100), should be at key-5"
	);

	// And then key-6
	assert!(merge_iter.next().unwrap());
	assert_eq!(String::from_utf8_lossy(merge_iter.current_key().user_key()), "key-6");

	// No more keys
	assert!(!merge_iter.next().unwrap());
}

/// Test multiple direction switches with duplicate keys.
///
/// Uses different sequence numbers for realistic scenario:
/// - key-3(seq=200) from L0 (newer)
/// - key-3(seq=100) from L1 (older)
#[test]
fn test_merging_iterator_multiple_direction_switches() {
	let iter0 = build_table_iterator(vec![
		(create_internal_key("key-1", 100, InternalKeyKind::Set), Value::from(b"v1".to_vec())),
		(create_internal_key("key-3", 200, InternalKeyKind::Set), Value::from(b"v3-L0".to_vec())),
	]);

	let iter1 = build_table_iterator(vec![
		(create_internal_key("key-2", 100, InternalKeyKind::Set), Value::from(b"v2".to_vec())),
		(create_internal_key("key-3", 100, InternalKeyKind::Set), Value::from(b"v3-L1".to_vec())),
	]);

	let cmp = create_comparator();
	let mut merge_iter = MergingIterator::new(vec![iter0, iter1], cmp);

	// Forward to key-3(seq=200)
	assert!(merge_iter.seek_first().unwrap()); // key-1
	assert!(merge_iter.next().unwrap()); // key-2
	assert!(merge_iter.next().unwrap()); // key-3(seq=200)
	assert_eq!(String::from_utf8_lossy(merge_iter.current_key().user_key()), "key-3");
	assert_eq!(merge_iter.current_key().seq_num(), 200);

	// Switch to backward
	assert!(merge_iter.prev().unwrap()); // Should be key-2
	assert_eq!(String::from_utf8_lossy(merge_iter.current_key().user_key()), "key-2");

	// Switch back to forward
	assert!(merge_iter.next().unwrap()); // Should be key-3(seq=200)
	assert_eq!(String::from_utf8_lossy(merge_iter.current_key().user_key()), "key-3");
	assert_eq!(merge_iter.current_key().seq_num(), 200);

	// Continue forward
	assert!(merge_iter.next().unwrap()); // Should be key-3(seq=100)
	assert_eq!(String::from_utf8_lossy(merge_iter.current_key().user_key()), "key-3");
	assert_eq!(merge_iter.current_key().seq_num(), 100);

	// Switch to backward again
	assert!(merge_iter.prev().unwrap()); // Should be key-3(seq=200)
	assert_eq!(String::from_utf8_lossy(merge_iter.current_key().user_key()), "key-3");
	assert_eq!(merge_iter.current_key().seq_num(), 200);
}

// ============================================================================
// SNAPSHOT-AWARE COMPACTION TESTS
// ============================================================================

/// Test: No snapshots - only latest version is kept (existing behavior)
#[test]
fn test_snapshot_compaction_no_snapshots_keeps_only_latest() {
	// Create multiple versions of the same key
	let items = vec![
		(create_internal_key("key1", 100, InternalKeyKind::Set), b"v3".to_vec()),
		(create_internal_key("key1", 50, InternalKeyKind::Set), b"v2".to_vec()),
		(create_internal_key("key1", 20, InternalKeyKind::Set), b"v1".to_vec()),
	];

	let iter = build_table_iterator(items);

	// No snapshots - should only keep latest version
	let mut comp_iter = CompactionIterator::new(
		vec![iter],
		create_comparator(),
		false, // not bottom level
		false, // versioning disabled
		0,
		Arc::new(MockLogicalClock::new()),
		vec![], // No snapshots
	);

	let mut result = Vec::new();
	for item in comp_iter.by_ref() {
		let (key, _) = item.unwrap();
		result.push((String::from_utf8_lossy(&key.user_key).to_string(), key.seq_num()));
	}

	// Only the latest version (seq=100) should be kept
	assert_eq!(result.len(), 1, "Only latest version should be kept with no snapshots");
	assert_eq!(result[0], ("key1".to_string(), 100));
}

/// Test: Single snapshot - preserves version visible to snapshot
#[test]
fn test_snapshot_compaction_single_snapshot_preserves_visible_version() {
	// Versions: seq=100 (visible to snapshot 50), seq=30 (visible to snapshot 50)
	// Snapshot at seq=50 means versions with seq <= 50 are visible to it
	let items = vec![
		(create_internal_key("key1", 100, InternalKeyKind::Set), b"v3".to_vec()),
		(create_internal_key("key1", 30, InternalKeyKind::Set), b"v2".to_vec()),
		(create_internal_key("key1", 10, InternalKeyKind::Set), b"v1".to_vec()),
	];

	let iter = build_table_iterator(items);

	// Snapshot at seq=50 - version seq=30 is the visible version for this snapshot
	let mut comp_iter = CompactionIterator::new(
		vec![iter],
		create_comparator(),
		false,
		false, // versioning disabled
		0,
		Arc::new(MockLogicalClock::new()),
		vec![50], // Snapshot at seq=50
	);

	let mut result = Vec::new();
	for item in comp_iter.by_ref() {
		let (key, _) = item.unwrap();
		result.push((String::from_utf8_lossy(&key.user_key).to_string(), key.seq_num()));
	}

	// seq=100: Latest, keep
	// seq=30: Visible to snapshot 50, keep
	// seq=10: Same boundary as seq=30 (both visible to snapshot 50), dropped (hidden by seq=30)
	assert_eq!(result.len(), 2, "Latest and snapshot-visible version should be kept");
	assert!(result.contains(&("key1".to_string(), 100)), "Latest should be kept");
	assert!(result.contains(&("key1".to_string(), 30)), "Snapshot-visible version should be kept");
}

/// Test: Multiple snapshots - each visibility boundary preserves one version
#[test]
fn test_snapshot_compaction_multiple_snapshots_different_boundaries() {
	// Versions at different seq_nums, two snapshots create different boundaries
	let items = vec![
		(create_internal_key("key1", 200, InternalKeyKind::Set), b"v4".to_vec()), /* Newer than
		                                                                           * all snapshots */
		(create_internal_key("key1", 120, InternalKeyKind::Set), b"v3".to_vec()), /* Visible to
		                                                                           * snap 150 */
		(create_internal_key("key1", 80, InternalKeyKind::Set), b"v2".to_vec()), /* Visible to
		                                                                          * snap 150 (same
		                                                                          * boundary as
		                                                                          * v3) */
		(create_internal_key("key1", 30, InternalKeyKind::Set), b"v1".to_vec()), /* Visible to
		                                                                          * snap 50 */
	];

	let iter = build_table_iterator(items);

	// Snapshots at seq=50 and seq=150
	let mut comp_iter = CompactionIterator::new(
		vec![iter],
		create_comparator(),
		false,
		false,
		0,
		Arc::new(MockLogicalClock::new()),
		vec![50, 150], // Two snapshots
	);

	let mut result = Vec::new();
	for item in comp_iter.by_ref() {
		let (key, _) = item.unwrap();
		result.push((String::from_utf8_lossy(&key.user_key).to_string(), key.seq_num()));
	}

	// seq=200: NewerThanAllSnapshots, different boundary from v3, keep
	// seq=120: BoundedBySnapshot(150), keep
	// seq=80: BoundedBySnapshot(150), same boundary as seq=120, dropped (hidden by seq=120)
	// seq=30: BoundedBySnapshot(50), different boundary, keep
	assert_eq!(result.len(), 3, "Three versions should be kept (one per boundary)");
	assert!(result.contains(&("key1".to_string(), 200)), "Latest should be kept");
	assert!(
		result.contains(&("key1".to_string(), 120)),
		"Version visible to snap 150 should be kept"
	);
	assert!(
		result.contains(&("key1".to_string(), 30)),
		"Version visible to snap 50 should be kept"
	);
	assert!(
		!result.contains(&("key1".to_string(), 80)),
		"Version hidden by newer version in same boundary should be dropped"
	);
}

/// Test: Older version in same visibility boundary is dropped when hidden by newer version
#[test]
fn test_snapshot_compaction_newer_version_hides_older_in_same_boundary() {
	// Two versions both visible to the same snapshot - older one should be dropped
	let items = vec![
		(create_internal_key("key1", 100, InternalKeyKind::Set), b"v2".to_vec()),
		(create_internal_key("key1", 50, InternalKeyKind::Set), b"v1".to_vec()),
	];

	let iter = build_table_iterator(items);

	// Snapshot at seq=150 - both versions are visible to it (same boundary)
	let mut comp_iter = CompactionIterator::new(
		vec![iter],
		create_comparator(),
		false,
		false,
		0,
		Arc::new(MockLogicalClock::new()),
		vec![150], // Snapshot at seq=150
	);

	let mut result = Vec::new();
	for item in comp_iter.by_ref() {
		let (key, _) = item.unwrap();
		result.push((String::from_utf8_lossy(&key.user_key).to_string(), key.seq_num()));
	}

	// Both seq=100 and seq=50 are visible to snapshot 150 (same boundary)
	// seq=100 is newer, so seq=50 is dropped (hidden by newer version)
	assert_eq!(result.len(), 1, "Only newer version in same boundary should be kept");
	assert_eq!(result[0], ("key1".to_string(), 100));
}

/// Test: Versions newer than all snapshots can be hidden by newer versions
#[test]
fn test_snapshot_compaction_versions_at_tip_hidden_by_newer() {
	// Multiple versions all newer than the snapshot
	let items = vec![
		(create_internal_key("key1", 300, InternalKeyKind::Set), b"v3".to_vec()),
		(create_internal_key("key1", 200, InternalKeyKind::Set), b"v2".to_vec()),
		(create_internal_key("key1", 150, InternalKeyKind::Set), b"v1".to_vec()),
	];

	let iter = build_table_iterator(items);

	// Snapshot at seq=50 - all versions are newer than all snapshots (NewerThanAllSnapshots)
	let mut comp_iter = CompactionIterator::new(
		vec![iter],
		create_comparator(),
		false,
		false,
		0,
		Arc::new(MockLogicalClock::new()),
		vec![50], // Snapshot at seq=50
	);

	let mut result = Vec::new();
	for item in comp_iter.by_ref() {
		let (key, _) = item.unwrap();
		result.push((String::from_utf8_lossy(&key.user_key).to_string(), key.seq_num()));
	}

	// All versions are NewerThanAllSnapshots (same boundary), only latest kept
	assert_eq!(result.len(), 1, "Only latest version should be kept when all newer than snapshots");
	assert_eq!(result[0], ("key1".to_string(), 300));
}

/// Test: Multiple keys with snapshots
#[test]
fn test_snapshot_compaction_multiple_keys() {
	let items = vec![
		// Key1: versions at 100, 30
		(create_internal_key("key1", 100, InternalKeyKind::Set), b"k1v2".to_vec()),
		(create_internal_key("key1", 30, InternalKeyKind::Set), b"k1v1".to_vec()),
		// Key2: versions at 80, 40
		(create_internal_key("key2", 80, InternalKeyKind::Set), b"k2v2".to_vec()),
		(create_internal_key("key2", 40, InternalKeyKind::Set), b"k2v1".to_vec()),
	];

	let iter = build_table_iterator(items);

	// Snapshot at seq=50
	let mut comp_iter = CompactionIterator::new(
		vec![iter],
		create_comparator(),
		false,
		false,
		0,
		Arc::new(MockLogicalClock::new()),
		vec![50], // Snapshot at seq=50
	);

	let mut result = Vec::new();
	for item in comp_iter.by_ref() {
		let (key, _) = item.unwrap();
		result.push((String::from_utf8_lossy(&key.user_key).to_string(), key.seq_num()));
	}

	// Key1: seq=100 (NewerThanAllSnapshots), seq=30 (BoundedBySnapshot(50)) - different boundaries,
	// keep both Key2: seq=80 (NewerThanAllSnapshots), seq=40 (BoundedBySnapshot(50)) - different
	// boundaries, keep both
	assert_eq!(result.len(), 4, "All versions in different boundaries should be kept");
}

/// Test: Tombstone with snapshot visibility
#[test]
fn test_snapshot_compaction_tombstone_visible_to_snapshot() {
	// Delete followed by older put - both visible to snapshot
	let items = vec![
		(create_internal_key("key1", 100, InternalKeyKind::Delete), b"".to_vec()),
		(create_internal_key("key1", 50, InternalKeyKind::Set), b"v1".to_vec()),
	];

	let iter = build_table_iterator(items);

	// Snapshot at seq=150 - tombstone is visible to snapshot
	let mut comp_iter = CompactionIterator::new(
		vec![iter],
		create_comparator(),
		false, // NOT bottom level - tombstone must be preserved
		false,
		0,
		Arc::new(MockLogicalClock::new()),
		vec![150],
	);

	let mut result = Vec::new();
	for item in comp_iter.by_ref() {
		let (key, _) = item.unwrap();
		result.push((
			String::from_utf8_lossy(&key.user_key).to_string(),
			key.seq_num(),
			key.kind(),
		));
	}

	// Both in same boundary (BoundedBySnapshot(150))
	// seq=100 Delete is kept (latest), seq=50 is dropped (hidden by seq=100)
	assert_eq!(result.len(), 1);
	assert_eq!(result[0], ("key1".to_string(), 100, InternalKeyKind::Delete));
}

/// Test: Tombstone at bottom level with snapshot
#[test]
fn test_snapshot_compaction_tombstone_at_bottom_with_snapshot() {
	// Delete at bottom level - even with snapshot, can drop if visible
	let items = vec![
		(create_internal_key("key1", 100, InternalKeyKind::Delete), b"".to_vec()),
		(create_internal_key("key1", 50, InternalKeyKind::Set), b"v1".to_vec()),
	];

	let iter = build_table_iterator(items);

	// Snapshot at seq=150 - both versions visible, but bottom level
	let mut comp_iter = CompactionIterator::new(
		vec![iter],
		create_comparator(),
		true, // Bottom level
		false,
		0,
		Arc::new(MockLogicalClock::new()),
		vec![150],
	);

	let mut result = Vec::new();
	for item in comp_iter.by_ref() {
		let (key, _) = item.unwrap();
		result.push((String::from_utf8_lossy(&key.user_key).to_string(), key.seq_num()));
	}

	// At bottom level with DELETE as latest, all versions can be dropped
	// (The tombstone doesn't need to mask anything below)
	assert_eq!(result.len(), 0, "Bottom level delete should drop all versions");
}

/// Test: Snapshot at exact sequence number
#[test]
fn test_snapshot_compaction_exact_sequence_match() {
	// Version at exact snapshot sequence number
	let items = vec![
		(create_internal_key("key1", 100, InternalKeyKind::Set), b"v2".to_vec()),
		(create_internal_key("key1", 50, InternalKeyKind::Set), b"v1".to_vec()),
	];

	let iter = build_table_iterator(items);

	// Snapshot at seq=50 (exact match with v1)
	let mut comp_iter = CompactionIterator::new(
		vec![iter],
		create_comparator(),
		false,
		false,
		0,
		Arc::new(MockLogicalClock::new()),
		vec![50], // Snapshot at exact seq=50
	);

	let mut result = Vec::new();
	for item in comp_iter.by_ref() {
		let (key, _) = item.unwrap();
		result.push((String::from_utf8_lossy(&key.user_key).to_string(), key.seq_num()));
	}

	// seq=100: NewerThanAllSnapshots (> snapshot 50)
	// seq=50: BoundedBySnapshot(50) - exact match
	// Different boundaries, both should be kept
	assert_eq!(result.len(), 2, "Both versions should be kept with exact snapshot match");
	assert!(result.contains(&("key1".to_string(), 100)));
	assert!(result.contains(&("key1".to_string(), 50)));
}

/// Test: Version older than all snapshots
#[test]
fn test_snapshot_compaction_version_older_than_all_snapshots() {
	// Version older than the oldest snapshot
	let items = vec![
		(create_internal_key("key1", 200, InternalKeyKind::Set), b"v3".to_vec()),
		(create_internal_key("key1", 100, InternalKeyKind::Set), b"v2".to_vec()),
		(create_internal_key("key1", 30, InternalKeyKind::Set), b"v1".to_vec()),
	];

	let iter = build_table_iterator(items);

	// Snapshots at seq=50 and seq=150
	let mut comp_iter = CompactionIterator::new(
		vec![iter],
		create_comparator(),
		false,
		false,
		0,
		Arc::new(MockLogicalClock::new()),
		vec![50, 150],
	);

	let mut result = Vec::new();
	for item in comp_iter.by_ref() {
		let (key, _) = item.unwrap();
		result.push((String::from_utf8_lossy(&key.user_key).to_string(), key.seq_num()));
	}

	// seq=200: NewerThanAllSnapshots
	// seq=100: BoundedBySnapshot(150)
	// seq=30: BoundedBySnapshot(50)
	// All in different boundaries, all should be kept
	assert_eq!(result.len(), 3, "All versions in different boundaries should be kept");
}

/// Test: SnapshotVisibility enum states
#[test]
fn test_snapshot_visibility_states() {
	// Test NoActiveSnapshots
	let items = vec![(create_internal_key("key1", 100, InternalKeyKind::Set), b"v1".to_vec())];
	let iter = build_table_iterator(items);
	let mut comp_iter = CompactionIterator::new(
		vec![iter],
		create_comparator(),
		false,
		false,
		0,
		Arc::new(MockLogicalClock::new()),
		vec![], // No snapshots
	);

	// Test via the iterator behavior - with no snapshots, should keep only latest
	let mut count = 0;
	for _ in comp_iter.by_ref() {
		count += 1;
	}
	assert_eq!(count, 1);
}

/// Test: Snapshot-aware compaction with versioning enabled
#[test]
fn test_snapshot_compaction_with_versioning_enabled() {
	let clock = Arc::new(MockLogicalClock::new());
	clock.set_time(1000); // Current time = 1000

	// Versions with different timestamps
	let items = vec![
		(
			create_internal_key_with_timestamp("key1", 100, InternalKeyKind::Set, 900),
			b"v3".to_vec(),
		),
		(create_internal_key_with_timestamp("key1", 50, InternalKeyKind::Set, 800), b"v2".to_vec()),
		(create_internal_key_with_timestamp("key1", 20, InternalKeyKind::Set, 500), b"v1".to_vec()),
	];

	let iter = build_table_iterator(items);

	// Snapshot at seq=60, versioning with 300ns retention
	let mut comp_iter = CompactionIterator::new(
		vec![iter],
		create_comparator(),
		false,
		true,  // versioning enabled
		300,   // retention period = 300ns
		clock, // current time = 1000
		vec![60],
	);

	let mut result = Vec::new();
	for item in comp_iter.by_ref() {
		let (key, _) = item.unwrap();
		result.push((String::from_utf8_lossy(&key.user_key).to_string(), key.seq_num()));
	}

	// seq=100: NewerThanAllSnapshots, keep (latest)
	// seq=50: BoundedBySnapshot(60), keep (snapshot visible)
	// seq=20: BoundedBySnapshot(60), same boundary as seq=50, dropped (hidden by seq=50)
	assert_eq!(result.len(), 2, "Latest and snapshot-visible should be kept");
	assert!(result.contains(&("key1".to_string(), 100)));
	assert!(result.contains(&("key1".to_string(), 50)));
}
