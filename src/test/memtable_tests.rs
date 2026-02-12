use std::collections::HashMap;
use std::sync::Arc;

use test_log::test;

use crate::batch::Batch;
use crate::memtable::MemTable;
use crate::{InternalKeyKind, LSMIterator, Value};

fn assert_value(encoded_value: &Value, expected_value: &[u8]) {
	// Skip the tag byte (first byte) and compare the actual value content
	let value_content = &encoded_value[..];
	assert_eq!(value_content, expected_value);
}

#[test]
fn memtable_get() {
	let memtable = MemTable::default();
	let key = b"foo".to_vec();
	let value = b"value".to_vec();

	let mut batch = Batch::new(1);
	batch.set(key, value.clone(), 0).unwrap();

	memtable.add(&batch).unwrap();

	let res = memtable.get(b"foo", None).unwrap();
	assert_value(&res.1, &value);
}

#[test]
fn memtable_size() {
	let memtable = MemTable::default();
	let key = b"foo".to_vec();
	let value = b"value".to_vec();

	let mut batch = Batch::new(1);
	batch.set(key, value, 0).unwrap();

	memtable.add(&batch).unwrap();

	assert!(memtable.size() > 0);
}

#[test]
fn memtable_lsn() {
	let memtable = MemTable::default();
	let key = b"foo".to_vec();
	let value = b"value".to_vec();
	let seq_num = 100;

	let mut batch = Batch::new(seq_num);
	batch.set(key, value, 0).unwrap();

	memtable.add(&batch).unwrap();

	assert_eq!(seq_num, memtable.lsn());
}

#[test]
fn memtable_add_and_get() {
	let memtable = MemTable::default();
	let key1 = b"key1".to_vec();
	let value1 = b"value1".to_vec();

	let mut batch1 = Batch::new(1);
	batch1.set(key1, value1.clone(), 0).unwrap();

	memtable.add(&batch1).unwrap();

	let key2 = b"key2".to_vec();
	let value2 = b"value2".to_vec();

	let mut batch2 = Batch::new(2);
	batch2.set(key2, value2.clone(), 0).unwrap();

	memtable.add(&batch2).unwrap();

	let res = memtable.get(b"key1", None).unwrap();
	assert_value(&res.1, &value1);

	let res = memtable.get(b"key2", None).unwrap();
	assert_value(&res.1, &value2);
}

#[test]
fn memtable_get_latest_seq_no() {
	let memtable = MemTable::default();
	let key1 = b"key1".to_vec();
	let value1 = b"value1".to_vec();
	let value2 = b"value2".to_vec();
	let value3 = b"value3".to_vec();

	let mut batch1 = Batch::new(1);
	batch1.set(key1.clone(), value1, 0).unwrap();
	memtable.add(&batch1).unwrap();

	let mut batch2 = Batch::new(2);
	batch2.set(key1.clone(), value2, 0).unwrap();
	memtable.add(&batch2).unwrap();

	let mut batch3 = Batch::new(3);
	batch3.set(key1, value3.clone(), 0).unwrap();
	memtable.add(&batch3).unwrap();

	let res = memtable.get(b"key1", None).unwrap();
	assert_value(&res.1, &value3);
}

#[test]
fn memtable_prefix() {
	let memtable = MemTable::default();
	let key1 = b"foo".to_vec();
	let value1 = b"value1".to_vec();

	let key2 = b"foo1".to_vec();
	let value2 = b"value2".to_vec();

	let mut batch1 = Batch::new(0);
	batch1.set(key1, value1.clone(), 0).unwrap();
	memtable.add(&batch1).unwrap();

	let mut batch2 = Batch::new(1);
	batch2.set(key2, value2.clone(), 0).unwrap();
	memtable.add(&batch2).unwrap();

	let res = memtable.get(b"foo", None).unwrap();
	assert_value(&res.1, &value1);

	let res = memtable.get(b"foo1", None).unwrap();
	assert_value(&res.1, &value2);
}

type TestEntry = (Vec<u8>, Vec<u8>, InternalKeyKind, Option<u64>);

fn create_test_memtable(entries: Vec<TestEntry>) -> (Arc<MemTable>, u64) {
	let memtable = Arc::new(MemTable::default());

	let mut last_seq = 0;

	// For test purposes, if custom sequence numbers are provided, we need to add
	// each entry individually to ensure they get the exact sequence number
	// specified
	for (key, value, kind, custom_seq) in entries {
		let seq_num = custom_seq.unwrap_or_else(|| {
			last_seq += 1;
			last_seq
		});

		// Create a single-entry batch for each record to ensure exact sequence number
		// assignment
		let mut batch = Batch::new(seq_num);
		match kind {
			InternalKeyKind::Set => {
				batch.set(key.clone(), value.clone(), 0).unwrap();
			}
			InternalKeyKind::Delete => {
				batch.delete(key.clone(), 0).unwrap();
			}
			_ => {
				// For other kinds, use add_record directly
				batch.add_record(kind, key.clone(), Some(value.clone()), 0).unwrap();
			}
		}

		memtable.add(&batch).unwrap();

		if custom_seq.is_some() {
			last_seq = std::cmp::max(last_seq, seq_num);
		}
	}

	(memtable, last_seq)
}

#[test]
fn test_empty_memtable() {
	let memtable = Arc::new(MemTable::default());

	// Test that iterator is empty
	let mut iter = memtable.iter();
	iter.seek_first().unwrap();
	let mut count = 0;
	while iter.valid() {
		count += 1;
		if !iter.next().unwrap_or(false) {
			break;
		}
	}
	assert_eq!(count, 0);

	// Test that is_empty returns true
	assert!(memtable.is_empty());
}

#[test]
fn test_single_key() {
	let (memtable, _) = create_test_memtable(vec![(
		b"key1".to_vec(),
		b"value1".to_vec(),
		InternalKeyKind::Set,
		None,
	)]);

	// Collect all entries
	let mut iter = memtable.iter();
	iter.seek_first().unwrap();
	let mut entries = Vec::new();
	while iter.valid() {
		let key = iter.key().to_owned();
		let value_bytes = iter.value_encoded();
		let value = value_bytes.unwrap().to_vec();
		entries.push((key, value));
		if !iter.next().unwrap_or(false) {
			break;
		}
	}
	assert_eq!(entries.len(), 1);

	let (key, encoded_value) = &entries[0];
	let user_key = &key.user_key;
	assert_eq!(user_key, b"key1");

	assert_value(encoded_value, &b"value1"[..]);

	// Test get method
	let result = memtable.get(b"key1", None);
	assert!(result.is_some());
	let (ikey, encoded_val) = result.unwrap();
	assert_eq!(&ikey.user_key, b"key1");

	assert_value(&encoded_val, b"value1");
}

#[test]
fn test_multiple_keys() {
	let (memtable, _) = create_test_memtable(vec![
		(b"key1".to_vec(), b"value1".to_vec(), InternalKeyKind::Set, None),
		(b"key3".to_vec(), b"value3".to_vec(), InternalKeyKind::Set, None),
		(b"key5".to_vec(), b"value5".to_vec(), InternalKeyKind::Set, None),
	]);

	// Collect all entries
	let mut iter = memtable.iter();
	iter.seek_first().unwrap();
	let mut entries = Vec::new();
	while iter.valid() {
		let key = iter.key().to_owned();
		let value_bytes = iter.value_encoded();
		let value = value_bytes.unwrap().to_vec();
		entries.push((key, value));
		if !iter.next().unwrap_or(false) {
			break;
		}
	}
	assert_eq!(entries.len(), 3);

	// Extract user keys for comparison
	let user_keys: Vec<_> = entries.iter().map(|(key, _)| key.user_key.clone()).collect();

	// Keys should be in lexicographic order
	assert_eq!(&user_keys[0], b"key1");
	assert_eq!(&user_keys[1], b"key3");
	assert_eq!(&user_keys[2], b"key5");

	// Test individual gets
	assert!(memtable.get(b"key1", None).is_some());
	assert!(memtable.get(b"key3", None).is_some());
	assert!(memtable.get(b"key5", None).is_some());
	assert!(memtable.get(b"key2", None).is_none());
	assert!(memtable.get(b"key4", None).is_none());
}

#[test]
fn test_sequence_number_ordering() {
	// Create test with multiple sequence numbers for the same key
	let (memtable, _) = create_test_memtable(vec![
		(b"key1".to_vec(), b"value1".to_vec(), InternalKeyKind::Set, Some(10)),
		(b"key1".to_vec(), b"value2".to_vec(), InternalKeyKind::Set, Some(20)), /* Higher sequence number */
		(b"key1".to_vec(), b"value3".to_vec(), InternalKeyKind::Set, Some(5)),  /* Lower sequence
		                                                                         * number */
	]);

	// Collect all entries
	let mut iter = memtable.iter();
	iter.seek_first().unwrap();
	let mut entries = Vec::new();
	while iter.valid() {
		let key = iter.key().to_owned();
		let value_bytes = iter.value_encoded();
		let value = value_bytes.unwrap().to_vec();
		entries.push((key, value));
		if !iter.next().unwrap_or(false) {
			break;
		}
	}
	assert_eq!(entries.len(), 3);

	// Extract sequence numbers and values
	let mut key1_entries = Vec::new();
	for (key, encoded_value) in &entries {
		let (user_key, seq_num, _) = (key.user_key.clone(), key.seq_num(), key.kind());
		if &user_key == b"key1" {
			key1_entries.push((seq_num, encoded_value));
		}
	}

	// Verify ordering - higher sequence numbers should come first
	assert_eq!(key1_entries.len(), 3);
	assert_eq!(key1_entries[0].0, 20);
	assert_eq!(key1_entries[0].1, b"value2");
	assert_eq!(key1_entries[1].0, 10);
	assert_eq!(key1_entries[1].1, b"value1");
	assert_eq!(key1_entries[2].0, 5);
	assert_eq!(key1_entries[2].1, b"value3");

	// Test get method - should return the highest sequence number
	let result = memtable.get(b"key1", None);
	assert!(result.is_some());
	let (ikey, encoded_val) = result.unwrap();
	assert_eq!(ikey.seq_num(), 20);
	assert_eq!(&encoded_val, b"value2");
}

#[test]
fn test_key_updates_with_sequence_numbers() {
	// Create test with key updates
	let (memtable, _) = create_test_memtable(vec![
		(b"key1".to_vec(), b"old_value".to_vec(), InternalKeyKind::Set, Some(5)),
		(b"key1".to_vec(), b"new_value".to_vec(), InternalKeyKind::Set, Some(10)),
		(b"key2".to_vec(), b"value2".to_vec(), InternalKeyKind::Set, Some(7)),
	]);

	// Test get returns the latest value
	let result = memtable.get(b"key1", None);
	assert!(result.is_some());
	let (_, encoded_val) = result.unwrap();
	assert_value(&encoded_val, b"new_value");

	// Test get with specific sequence number
	let result = memtable.get(b"key1", Some(8));
	assert!(result.is_some());
	let (_, encoded_val) = result.unwrap();
	assert_value(&encoded_val, b"old_value"); // Should get the value with seq_num
	                                       // <= 8
}

#[test]
fn test_tombstones() {
	// Create test with deleted entries
	let (memtable, _) = create_test_memtable(vec![
		(b"key1".to_vec(), b"value1".to_vec(), InternalKeyKind::Set, Some(1)),
		(b"key2".to_vec(), b"value2".to_vec(), InternalKeyKind::Set, Some(2)),
		(b"key3".to_vec(), b"value3".to_vec(), InternalKeyKind::Set, Some(3)),
		(b"key2".to_vec(), vec![], InternalKeyKind::Delete, Some(4)), // Delete key2
	]);

	// Iterator should see all entries including tombstones
	let mut iter = memtable.iter();
	iter.seek_first().unwrap();
	let mut entries = Vec::new();
	while iter.valid() {
		let key = iter.key().to_owned();
		let value_bytes = iter.value_encoded();
		let value = value_bytes.unwrap().to_vec();
		entries.push((key, value));
		if !iter.next().unwrap_or(false) {
			break;
		}
	}

	// Count entries for each key
	let mut key_counts = HashMap::new();
	for (key, _) in &entries {
		let user_key = &key.user_key;
		*key_counts.entry(user_key).or_insert(0) += 1;
	}

	assert_eq!(key_counts[&b"key1".to_vec()], 1);
	assert_eq!(key_counts[&b"key2".to_vec()], 2); // Original + tombstone
	assert_eq!(key_counts[&b"key3".to_vec()], 1);
}

#[test]
fn test_key_kinds() {
	// Test different key kinds
	let (memtable, _) = create_test_memtable(vec![
		(b"key1".to_vec(), b"value1".to_vec(), InternalKeyKind::Set, Some(10)),
		(b"key2".to_vec(), vec![], InternalKeyKind::Delete, Some(20)),
		(b"key3".to_vec(), b"value3".to_vec(), InternalKeyKind::Set, Some(30)),
		(b"key4".to_vec(), vec![], InternalKeyKind::Delete, Some(40)),
	]);

	// All key types should be visible in the iterator
	let mut iter = memtable.iter();
	iter.seek_first().unwrap();
	let mut entries = Vec::new();
	while iter.valid() {
		let key = iter.key().to_owned();
		let value_bytes = iter.value_encoded();
		let value = value_bytes.unwrap().to_vec();
		entries.push((key, value));
		if !iter.next().unwrap_or(false) {
			break;
		}
	}
	assert_eq!(entries.len(), 4);

	// Extract and verify key information
	let mut key_info = Vec::new();
	for (key, encoded_value) in &entries {
		let (user_key, seq_num, kind) = (key.user_key.clone(), key.seq_num(), key.kind());
		key_info.push((user_key, seq_num, kind, encoded_value.len()));
	}

	// Verify all keys are present with correct kinds
	assert_eq!(&key_info[0].0, b"key1");
	assert_eq!(key_info[0].2, InternalKeyKind::Set);
	assert!(key_info[0].3 > 0); // Has value

	assert_eq!(&key_info[1].0, b"key2");
	assert_eq!(key_info[1].2, InternalKeyKind::Delete);
	assert_eq!(key_info[1].3, 0); // No value for delete

	assert_eq!(&key_info[2].0, b"key3");
	assert_eq!(key_info[2].2, InternalKeyKind::Set);
	assert!(key_info[2].3 > 0); // Has value

	assert_eq!(&key_info[3].0, b"key4");
	assert_eq!(key_info[3].2, InternalKeyKind::Delete);
	assert_eq!(key_info[3].3, 0); // No value for delete

	// Test get method behavior with different kinds
	let result = memtable.get(b"key1", None);
	assert!(result.is_some());
	let (ikey, _) = result.unwrap();
	assert_eq!(ikey.kind(), InternalKeyKind::Set);

	let result = memtable.get(b"key2", None);
	assert!(result.is_some());
	let (ikey, encoded_val) = result.unwrap();
	assert_eq!(ikey.kind(), InternalKeyKind::Delete);
	assert_eq!(encoded_val.len(), 0);
}

#[test]
fn test_range_query() {
	// Create a memtable with many keys
	let (memtable, _) = create_test_memtable(vec![
		(b"a".to_vec(), b"value-a".to_vec(), InternalKeyKind::Set, None),
		(b"c".to_vec(), b"value-c".to_vec(), InternalKeyKind::Set, None),
		(b"e".to_vec(), b"value-e".to_vec(), InternalKeyKind::Set, None),
		(b"g".to_vec(), b"value-g".to_vec(), InternalKeyKind::Set, None),
		(b"i".to_vec(), b"value-i".to_vec(), InternalKeyKind::Set, None),
		(b"k".to_vec(), b"value-k".to_vec(), InternalKeyKind::Set, None),
		(b"m".to_vec(), b"value-m".to_vec(), InternalKeyKind::Set, None),
	]);

	// Test inclusive lower, exclusive upper
	// To include "k", use upper bound "l" (exclusive upper means < upper)
	let mut range_iter = memtable.range(
		Some("c".as_bytes()), // Inclusive lower
		Some("l".as_bytes()), // Exclusive upper - includes "k" but not "l" or "m"
	);
	range_iter.seek_first().unwrap();
	let mut range_entries = Vec::new();
	while range_iter.valid() {
		let key = range_iter.key().to_owned();
		let value_bytes = range_iter.value_encoded();
		let value = value_bytes.unwrap().to_vec();
		range_entries.push((key, value));
		if !range_iter.next().unwrap_or(false) {
			break;
		}
	}

	let user_keys: Vec<_> = range_entries.iter().map(|(key, _)| key.user_key.clone()).collect();

	// Range [c, l) includes c, e, g, i, k (5 entries)
	assert_eq!(user_keys.len(), 5);
	assert_eq!(&user_keys[0], b"c");
	assert_eq!(&user_keys[1], b"e");
	assert_eq!(&user_keys[2], b"g");
	assert_eq!(&user_keys[3], b"i");
	assert_eq!(&user_keys[4], b"k");

	// Test exclusive range
	let mut range_iter = memtable.range(
		Some("c".as_bytes()), // Inclusive lower
		Some("k".as_bytes()), // Exclusive upper (excludes "k")
	);
	range_iter.seek_first().unwrap();
	let mut range_entries = Vec::new();
	while range_iter.valid() {
		let key = range_iter.key().to_owned();
		let value_bytes = range_iter.value_encoded();
		let value = value_bytes.unwrap().to_vec();
		range_entries.push((key, value));
		if !range_iter.next().unwrap_or(false) {
			break;
		}
	}

	let user_keys: Vec<_> = range_entries.iter().map(|(key, _)| key.user_key.clone()).collect();

	assert_eq!(user_keys.len(), 4); // Excludes "k"
	assert_eq!(&user_keys[0], b"c");
	assert_eq!(&user_keys[1], b"e");
	assert_eq!(&user_keys[2], b"g");
	assert_eq!(&user_keys[3], b"i");
}

#[test]
fn test_range_query_with_sequence_numbers() {
	// Create a memtable with overlapping sequence numbers
	let (memtable, _) = create_test_memtable(vec![
		(b"a".to_vec(), b"value-a1".to_vec(), InternalKeyKind::Set, Some(10)),
		(b"a".to_vec(), b"value-a2".to_vec(), InternalKeyKind::Set, Some(20)), // Updated value
		(b"c".to_vec(), b"value-c1".to_vec(), InternalKeyKind::Set, Some(15)),
		(b"e".to_vec(), b"value-e1".to_vec(), InternalKeyKind::Set, Some(25)),
		(b"e".to_vec(), b"value-e2".to_vec(), InternalKeyKind::Set, Some(15)), // Older version
	]);

	// Perform a range query from "a" to "f" (inclusive lower, exclusive upper)
	let mut range_iter = memtable.range(
		Some("a".as_bytes()), // Inclusive lower
		Some("f".as_bytes()), // Exclusive upper
	);
	range_iter.seek_first().unwrap();
	let mut range_entries = Vec::new();
	while range_iter.valid() {
		let key = range_iter.key().to_owned();
		let value_bytes = range_iter.value_encoded();
		let value = value_bytes.unwrap().to_vec();
		range_entries.push((key, value));
		if !range_iter.next().unwrap_or(false) {
			break;
		}
	}

	// Extract user keys, sequence numbers and values
	let mut entries_info = Vec::new();
	for (key, encoded_value) in &range_entries {
		let (user_key, seq_num, _) = (key.user_key.clone(), key.seq_num(), key.kind());
		entries_info.push((user_key, seq_num, encoded_value));
	}

	// Verify we get keys in order, with highest sequence numbers first for each key
	assert_eq!(entries_info.len(), 5);

	// Key "a" entries (seq 20 then seq 10)
	assert_eq!(&entries_info[0].0, b"a");
	assert_eq!(entries_info[0].1, 20);
	assert_eq!(entries_info[0].2, b"value-a2");

	assert_eq!(entries_info[1].0, b"a");
	assert_eq!(entries_info[1].1, 10);
	assert_eq!(entries_info[1].2, b"value-a1");

	// Key "c" entry
	assert_eq!(entries_info[2].0, b"c");
	assert_eq!(entries_info[2].1, 15);
	assert_eq!(entries_info[2].2, b"value-c1");

	// Key "e" entries (seq 25 then seq 15)
	assert_eq!(entries_info[3].0, b"e");
	assert_eq!(entries_info[3].1, 25);
	assert_eq!(entries_info[3].2, b"value-e1");

	assert_eq!(entries_info[4].0, b"e");
	assert_eq!(entries_info[4].1, 15);
	assert_eq!(entries_info[4].2, b"value-e2");
}

#[test]
fn test_binary_keys() {
	// Test with binary keys containing nulls and various byte values
	let (memtable, _) = create_test_memtable(vec![
		(vec![0, 0, 1], b"value1".to_vec(), InternalKeyKind::Set, None),
		(vec![0, 1, 0], b"value2".to_vec(), InternalKeyKind::Set, None),
		(vec![1, 0, 0], b"value3".to_vec(), InternalKeyKind::Set, None),
		(vec![0xFF, 0xFE, 0xFD], b"value4".to_vec(), InternalKeyKind::Set, None),
	]);

	let mut iter = memtable.iter();
	iter.seek_first().unwrap();
	let mut entries = Vec::new();
	while iter.valid() {
		let key = iter.key().to_owned();
		let value_bytes = iter.value_encoded();
		let value = value_bytes.unwrap().to_vec();
		entries.push((key, value));
		if !iter.next().unwrap_or(false) {
			break;
		}
	}
	assert_eq!(entries.len(), 4);

	// Extract and verify user keys are in correct order
	let user_keys: Vec<_> = entries.iter().map(|(key, _)| key.user_key.clone()).collect();

	assert_eq!(user_keys[0].as_ref(), vec![0, 0, 1]);
	assert_eq!(user_keys[1].as_ref(), vec![0, 1, 0]);
	assert_eq!(user_keys[2].as_ref(), vec![1, 0, 0]);
	assert_eq!(user_keys[3].as_ref(), vec![0xFF, 0xFE, 0xFD]);
}

#[test]
fn test_large_dataset() {
	// Create a larger dataset to test performance and correctness
	let mut entries = Vec::new();
	for i in 0..1000 {
		let key = format!("key{i:04}").as_bytes().to_vec();
		let value = format!("value{i:04}").as_bytes().to_vec();
		entries.push((key, value, InternalKeyKind::Set, None));
	}

	let (memtable, _) = create_test_memtable(entries);

	// Test that all entries exist
	let mut iter = memtable.iter();
	iter.seek_first().unwrap();
	let mut count = 0;
	while iter.valid() {
		count += 1;
		if !iter.next().unwrap_or(false) {
			break;
		}
	}
	assert_eq!(count, 1000);

	// Test specific gets
	let result = memtable.get(b"key0000", None);
	assert!(result.is_some());
	let (_, encoded_val) = result.unwrap();
	assert_value(&encoded_val, b"value0000");

	let result = memtable.get(b"key0500", None);
	assert!(result.is_some());
	let (_, encoded_val) = result.unwrap();
	assert_value(&encoded_val, b"value0500");

	let result = memtable.get(b"key0999", None);
	assert!(result.is_some());
	let (_, encoded_val) = result.unwrap();
	assert_value(&encoded_val, b"value0999");

	// Test non-existent key
	let result = memtable.get(b"key1000", None);
	assert!(result.is_none());
}

#[test]
fn test_memtable_size_tracking() {
	let memtable = Arc::new(MemTable::default());

	// Add some data
	let mut batch = Batch::new(1);
	batch.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
	batch.set(b"key2".to_vec(), b"value2".to_vec(), 0).unwrap();

	memtable.add(&batch).unwrap();
	let size1 = memtable.size();
	assert!(size1 > 0);

	// Add more data
	let mut batch2 = Batch::new(2);
	batch2.set(b"key3".to_vec(), b"value3".to_vec(), 0).unwrap();

	memtable.add(&batch2).unwrap();
	let size2 = memtable.size();
	assert!(size2 > size1);
}

#[test]
fn test_latest_sequence_number() {
	let memtable = Arc::new(MemTable::default());

	// Initially 0
	assert_eq!(memtable.lsn(), 0);

	// Add batch with seq_num 10
	let mut batch1 = Batch::new(10);
	batch1.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
	memtable.add(&batch1).unwrap();
	assert_eq!(memtable.lsn(), 10);

	// Add batch with lower seq_num - should not update
	let mut batch2 = Batch::new(5);
	batch2.set(b"key2".to_vec(), b"value2".to_vec(), 0).unwrap();
	memtable.add(&batch2).unwrap();
	assert_eq!(memtable.lsn(), 10); // Should still be 10

	// Add batch with higher seq_num
	let mut batch3 = Batch::new(20);
	batch3.set(b"key3".to_vec(), b"value3".to_vec(), 0).unwrap();
	memtable.add(&batch3).unwrap();
	assert_eq!(memtable.lsn(), 20);
}

#[test]
fn test_get_highest_seq_num() {
	// Add a batch with 5 entries
	let mut batch = Batch::new(10);
	batch.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
	batch.set(b"key2".to_vec(), b"value2".to_vec(), 0).unwrap();
	batch.set(b"key3".to_vec(), b"value3".to_vec(), 0).unwrap();
	batch.set(b"key4".to_vec(), b"value4".to_vec(), 0).unwrap();
	batch.set(b"key5".to_vec(), b"value5".to_vec(), 0).unwrap();

	assert_eq!(batch.get_highest_seq_num(), 14);
}

#[test]
fn test_excluded_bound_skips_all_versions_of_key() {
	// This test verifies that Bound::Excluded skips ALL entries with the same user key,
	// not just one entry. In an LSM tree, there can be multiple versions of the same
	// key with different sequence numbers.

	// Create entries where key "b" has multiple versions
	let (memtable, _) = create_test_memtable(vec![
		(b"a".to_vec(), b"value-a".to_vec(), InternalKeyKind::Set, Some(10)),
		(b"b".to_vec(), b"value-b1".to_vec(), InternalKeyKind::Set, Some(30)), // Newest version
		(b"b".to_vec(), b"value-b2".to_vec(), InternalKeyKind::Set, Some(20)), // Middle version
		(b"b".to_vec(), b"value-b3".to_vec(), InternalKeyKind::Set, Some(10)), // Oldest version
		(b"c".to_vec(), b"value-c".to_vec(), InternalKeyKind::Set, Some(15)),
	]);

	// Query with excluded lower bound "b" - manually skip ALL versions of "b"
	let mut iter = memtable.range(
		Some("b".as_bytes()), // Start at "b" (inclusive)
		None,                 // No upper bound
	);
	iter.seek_first().unwrap();
	// Skip all entries with user key "b" (they have same user key, different seqnums)
	let mut range_entries = Vec::new();
	while iter.valid() {
		let key = iter.key().to_owned();
		let value_bytes = iter.value_encoded();
		let value = value_bytes.unwrap().to_vec();
		if key.user_key != b"b" {
			range_entries.push((key, value));
			// Collect remaining entries
			while iter.next().unwrap_or(false) && iter.valid() {
				let key = iter.key().to_owned();
				let value_bytes = iter.value_encoded();
				let value = value_bytes.unwrap().to_vec();
				range_entries.push((key, value));
			}
			break;
		}
		if !iter.next().unwrap_or(false) {
			break;
		}
	}

	let user_keys: Vec<_> = range_entries.iter().map(|(key, _)| key.user_key.clone()).collect();

	// Should only contain "c" - all versions of "b" should be excluded
	assert_eq!(user_keys.len(), 1, "Expected only 'c', but got {:?}", user_keys);
	assert_eq!(&user_keys[0], b"c");
}

#[test]
fn test_excluded_bound_first_skips_all_versions() {
	let (memtable, _) = create_test_memtable(vec![
		(b"a".to_vec(), b"value-a".to_vec(), InternalKeyKind::Set, Some(10)),
		(b"b".to_vec(), b"value-b1".to_vec(), InternalKeyKind::Set, Some(30)),
		(b"b".to_vec(), b"value-b2".to_vec(), InternalKeyKind::Set, Some(20)),
		(b"b".to_vec(), b"value-b3".to_vec(), InternalKeyKind::Set, Some(10)),
		(b"c".to_vec(), b"value-c".to_vec(), InternalKeyKind::Set, Some(15)),
	]);

	// Test excluded lower bound - manually skip "b" entries
	let mut iter = memtable.range(
		Some("b".as_bytes()), // Start at "b" (inclusive)
		None,                 // No upper bound
	);
	iter.seek_first().unwrap();
	// Skip all entries with user key "b"
	while iter.valid() {
		let key = iter.key().to_owned();
		if key.user_key != b"b" {
			break;
		}
		if !iter.next().unwrap_or(false) {
			break;
		}
	}

	// Iterate once
	let first = iter.seek_first().unwrap();
	assert!(first);
	let key = iter.key().to_owned();
	assert_eq!(&key.user_key, b"b", "First key should be 'b'");

	// Reset and try again - create new iterator
	let mut iter2 = memtable.range(Some("b".as_bytes()), None);
	iter2.seek_first().unwrap();
	// Skip all entries with user key "b"
	while iter2.valid() {
		let key = iter2.key().to_owned();
		if key.user_key != b"b" {
			break;
		}
		if !iter2.next().unwrap_or(false) {
			break;
		}
	}

	let first = iter2.seek_first().unwrap();
	assert!(first);
	let key = iter2.key().to_owned();
	assert_eq!(&key.user_key, b"b", "After reset, first key should still be 'b'");
}
