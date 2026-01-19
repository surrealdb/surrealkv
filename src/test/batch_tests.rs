use test_log::test;

use crate::batch::{Batch, BATCH_VERSION, MAX_BATCH_SIZE};
use crate::vlog::ValuePointer;
use crate::InternalKeyKind;

#[test]
fn test_batch_new() {
	let batch = Batch::new(0);
	assert_eq!(batch.entries.len(), 0);
	assert_eq!(batch.count(), 0);
}

#[test]
fn test_batch_grow() {
	let mut batch = Batch::new(0);
	assert!(batch.grow(10).is_ok());
	assert!(batch.grow(MAX_BATCH_SIZE).is_err());
}

#[test]
fn test_batch_encode() {
	let mut batch = Batch::new(1);
	batch.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
	let encoded = batch.encode().unwrap();
	assert!(!encoded.is_empty());
}

#[test]
fn test_batch_get_count() {
	let mut batch = Batch::new(0);
	assert_eq!(batch.count(), 0);
	batch.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
	assert_eq!(batch.count(), 1);
}

#[test]
fn test_batchreader_new() {
	let mut batch = Batch::new(100);
	batch.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
	let encoded = batch.encode().unwrap();
	let reader = Batch::decode(&encoded).unwrap();
	assert_eq!(reader.starting_seq_num, 100);
}

#[test]
fn test_batchreader_get_seq_num() {
	let batch = Batch::new(100);
	let encoded = batch.encode().unwrap();
	let reader = Batch::decode(&encoded).unwrap();
	assert_eq!(reader.starting_seq_num, 100);
}

#[test]
fn test_batch_read_record() {
	let mut batch = Batch::new(1);
	batch.set(b"key1".to_vec(), b"value1".to_vec(), 1).unwrap();
	let encoded = batch.encode().unwrap();
	let decoded_batch = Batch::decode(&encoded).unwrap();
	assert_eq!(decoded_batch.starting_seq_num, 1);

	let entries = decoded_batch.entries();
	assert_eq!(entries.len(), 1);
	assert_eq!(entries[0].kind, InternalKeyKind::Set);
	assert_eq!(entries[0].key.as_slice(), b"key1");
	assert_eq!(entries[0].value.as_ref().unwrap().as_slice(), b"value1");
	assert_eq!(entries[0].timestamp, 1);
}

#[test]
fn test_batch_empty() {
	let batch = Batch::new(1);
	let encoded = batch.encode().unwrap();
	let decoded_batch = Batch::decode(&encoded).unwrap();
	assert_eq!(decoded_batch.starting_seq_num, 1);
	assert!(decoded_batch.entries().is_empty());
}

#[test]
fn test_batch_multiple_operations() {
	let mut batch = Batch::new(1);
	batch.set(b"key1".to_vec(), b"value1".to_vec(), 1).unwrap();
	batch.delete(b"key2".to_vec(), 2).unwrap();
	batch.set(b"key3".to_vec(), b"value3".to_vec(), 3).unwrap();

	assert_eq!(batch.count(), 3);

	let encoded = batch.encode().unwrap();
	let decoded_batch = Batch::decode(&encoded).unwrap();
	assert_eq!(decoded_batch.starting_seq_num, 1);

	let entries = decoded_batch.entries();
	assert_eq!(entries.len(), 3);

	assert_eq!(entries[0].kind, InternalKeyKind::Set);
	assert_eq!(entries[0].key.as_slice(), b"key1");
	assert_eq!(entries[0].value.as_ref().unwrap().as_slice(), b"value1");
	assert_eq!(entries[0].timestamp, 1);

	assert_eq!(entries[1].kind, InternalKeyKind::Delete);
	assert_eq!(entries[1].key.as_slice(), b"key2");
	assert!(entries[1].value.is_none());
	assert_eq!(entries[1].timestamp, 2);

	assert_eq!(entries[2].kind, InternalKeyKind::Set);
	assert_eq!(entries[2].key.as_slice(), b"key3");
	assert_eq!(entries[2].value.as_ref().unwrap().as_slice(), b"value3");
	assert_eq!(entries[2].timestamp, 3);
}

#[test]
fn test_batch_large_key_value() {
	let large_key = vec![b'a'; 1000000];
	let large_value = vec![b'b'; 1000000];

	let mut batch = Batch::new(1);
	batch.set(large_key.clone(), large_value.clone(), 1).unwrap();

	let encoded = batch.encode().unwrap();
	let decoded_batch = Batch::decode(&encoded).unwrap();
	assert_eq!(decoded_batch.starting_seq_num, 1);

	let entries = decoded_batch.entries();
	assert_eq!(entries.len(), 1);
	assert_eq!(entries[0].kind, InternalKeyKind::Set);
	assert_eq!(entries[0].key.as_slice(), &large_key);
	assert_eq!(entries[0].value.as_ref().unwrap().as_slice(), &large_value);
	assert_eq!(entries[0].timestamp, 1);
}

#[test]
fn test_batch_max_size() {
	let mut batch = Batch::new(0);
	let key = vec![b'a'; 1000];
	let value = vec![b'b'; (MAX_BATCH_SIZE as usize) - 2000];

	assert!(batch.set(key.clone(), value, 0).is_ok());
	assert!(batch.set(key, vec![0], 0).is_err());
}

#[test]
fn test_batch_iteration() {
	let mut batch = Batch::new(1);
	batch.set(b"key1".to_vec(), b"value1".to_vec(), 1).unwrap();
	batch.delete(b"key2".to_vec(), 2).unwrap();
	batch.set(b"key3".to_vec(), b"value3".to_vec(), 3).unwrap();

	let encoded = batch.encode().unwrap();
	let decoded_batch = Batch::decode(&encoded).unwrap();
	assert_eq!(decoded_batch.starting_seq_num, 1);

	let entries = decoded_batch.entries();
	assert_eq!(entries.len(), 3);

	assert_eq!(entries[0].kind, InternalKeyKind::Set);
	assert_eq!(entries[0].key.as_slice(), b"key1");
	assert_eq!(entries[0].value.as_ref().unwrap().as_slice(), b"value1");
	assert_eq!(entries[0].timestamp, 1);

	assert_eq!(entries[1].kind, InternalKeyKind::Delete);
	assert_eq!(entries[1].key.as_slice(), b"key2");
	assert!(entries[1].value.is_none());
	assert_eq!(entries[1].timestamp, 2);

	assert_eq!(entries[2].kind, InternalKeyKind::Set);
	assert_eq!(entries[2].key.as_slice(), b"key3");
	assert_eq!(entries[2].value.as_ref().unwrap().as_slice(), b"value3");
	assert_eq!(entries[2].timestamp, 3);
}

#[test]
fn test_batch_invalid_data() {
	let invalid_data = vec![0];
	assert!(Batch::decode(&invalid_data).is_err());
}

#[test]
fn test_batch_empty_key_and_value() {
	let mut batch = Batch::new(1);
	batch.set(b"".to_vec(), b"".to_vec(), 0).unwrap();
	batch.delete(b"".to_vec(), 0).unwrap();

	let encoded = batch.encode().unwrap();
	let decoded_batch = Batch::decode(&encoded).unwrap();
	assert_eq!(decoded_batch.starting_seq_num, 1);

	let entries = decoded_batch.entries();
	assert_eq!(entries.len(), 2);

	assert_eq!(entries[0].kind, InternalKeyKind::Set);
	assert_eq!(entries[0].key.as_slice(), b"");
	assert!(entries[1].value.is_none());

	assert_eq!(entries[1].kind, InternalKeyKind::Delete);
	assert_eq!(entries[1].key.as_slice(), b"");
	assert!(entries[1].value.is_none());
}

#[test]
fn test_batch_unicode_keys_and_values() {
	let mut batch = Batch::new(1);
	batch.set("🔑".as_bytes().to_vec(), "🗝️".as_bytes().to_vec(), 1).unwrap();
	batch.set("こんにちは".as_bytes().to_vec(), "世界".as_bytes().to_vec(), 2).unwrap();

	let encoded = batch.encode().unwrap();
	let decoded_batch = Batch::decode(&encoded).unwrap();
	assert_eq!(decoded_batch.starting_seq_num, 1);

	let entries = decoded_batch.entries();
	assert_eq!(entries.len(), 2);

	assert_eq!(entries[0].kind, InternalKeyKind::Set);
	assert_eq!(entries[0].key.as_slice(), "🔑".as_bytes());
	assert_eq!(entries[0].value.as_ref().unwrap().as_slice(), "🗝️".as_bytes());
	assert_eq!(entries[0].timestamp, 1);

	assert_eq!(entries[1].kind, InternalKeyKind::Set);
	assert_eq!(entries[1].key.as_slice(), "こんにちは".as_bytes());
	assert_eq!(entries[1].value.as_ref().unwrap().as_slice(), "世界".as_bytes());
	assert_eq!(entries[1].timestamp, 2);
}

#[test]
fn test_batch_sequence_numbers() {
	let mut batch = Batch::new(100);
	batch.set(b"key1".to_vec(), b"value1".to_vec(), 1).unwrap();
	batch.set(b"key2".to_vec(), b"value2".to_vec(), 2).unwrap();

	let encoded = batch.encode().unwrap();
	let decoded_batch = Batch::decode(&encoded).unwrap();

	assert_eq!(decoded_batch.starting_seq_num, 100);
}

#[test]
fn test_batch_large_number_of_records() {
	const NUM_RECORDS: usize = 10000;
	let mut batch = Batch::new(1);

	for i in 0..NUM_RECORDS {
		let key = format!("key{i}");
		let value = format!("value{i}");
		if i % 2 == 0 {
			batch.set(key.as_bytes().to_vec(), value.as_bytes().to_vec(), i as u64).unwrap();
		} else {
			batch.delete(key.as_bytes().to_vec(), i as u64).unwrap();
		}
	}

	assert_eq!(batch.count() as usize, NUM_RECORDS);

	let encoded = batch.encode().unwrap();
	let decoded_batch = Batch::decode(&encoded).unwrap();
	assert_eq!(decoded_batch.starting_seq_num, 1);

	let entries = decoded_batch.entries();
	assert_eq!(entries.len(), NUM_RECORDS);

	for (i, entry) in entries.iter().enumerate().take(NUM_RECORDS) {
		let expected_key = format!("key{i}");

		if i % 2 == 0 {
			assert_eq!(entry.kind, InternalKeyKind::Set);
			assert_eq!(entry.key, expected_key.as_bytes());
			assert_eq!(entry.value.as_ref().unwrap(), format!("value{i}").as_bytes());
		} else {
			assert_eq!(entry.kind, InternalKeyKind::Delete);
			assert_eq!(entry.key, expected_key.as_bytes());
			assert!(entry.value.is_none());
		}
		assert_eq!(entry.timestamp, i as u64);
	}
}

#[test]
fn test_batch_version() {
	let batch = Batch::new(1);
	assert_eq!(batch.version, BATCH_VERSION);

	let encoded = batch.encode().unwrap();
	let decoded_batch = Batch::decode(&encoded).unwrap();
	assert_eq!(decoded_batch.starting_seq_num, 1);
	assert_eq!(decoded_batch.version, BATCH_VERSION);

	// Test with a batch that has data
	let mut batch_with_data = Batch::new(100);
	batch_with_data.set(b"key".to_vec(), b"value".to_vec(), 1).unwrap();
	let encoded = batch_with_data.encode().unwrap();
	let decoded_batch = Batch::decode(&encoded).unwrap();
	assert_eq!(decoded_batch.starting_seq_num, 100);
	assert_eq!(decoded_batch.version, BATCH_VERSION);
}

#[test]
fn test_add_record_consistent_encoding() {
	// Test that None and Some(&[]) now encode identically (both as value_len=0)
	let mut batch_none = Batch::new(100);
	batch_none.add_record(InternalKeyKind::Delete, b"test_key".to_vec(), None, 100).unwrap();

	let mut batch_empty = Batch::new(100);
	batch_empty
		.add_record(InternalKeyKind::Delete, b"test_key".to_vec(), Some(vec![]), 100)
		.unwrap();

	let encoded_none = batch_none.encode().unwrap();
	let encoded_empty = batch_empty.encode().unwrap();

	// Now they should encode identically (both write value_len=0)
	assert_eq!(encoded_none, encoded_empty, "None and Some(&[]) should now encode identically");

	// Test reading them back - both should return None (since both encode as
	// value_len=0)
	let decoded_batch_none = Batch::decode(&encoded_none).unwrap();
	assert_eq!(decoded_batch_none.starting_seq_num, 100);
	let entries_none = decoded_batch_none.entries();
	assert_eq!(entries_none.len(), 1);
	assert_eq!(entries_none[0].kind, InternalKeyKind::Delete);
	assert_eq!(entries_none[0].key, b"test_key");
	assert!(entries_none[0].value.is_none(), "None encodes as value_len=0, reads back as None");
	assert_eq!(entries_none[0].timestamp, 100);

	let decoded_batch_empty = Batch::decode(&encoded_empty).unwrap();
	assert_eq!(decoded_batch_empty.starting_seq_num, 100);
	let entries_empty = decoded_batch_empty.entries();
	assert_eq!(entries_empty.len(), 1);
	assert_eq!(entries_empty[0].kind, InternalKeyKind::Delete);
	assert_eq!(entries_empty[0].key, b"test_key");
	assert!(
		entries_empty[0].value.is_none(),
		"Some(&[]) also encodes as value_len=0, reads back as None"
	);
	assert_eq!(entries_empty[0].timestamp, 100);

	// Test with different operation types to ensure they all work
	let mut batch_merge = Batch::new(300);
	batch_merge
		.add_record(
			InternalKeyKind::Merge,
			b"merge_key".to_vec(),
			Some(b"merge_data".to_vec()),
			300,
		)
		.unwrap();

	let encoded_merge = batch_merge.encode().unwrap();
	let decoded_batch_merge = Batch::decode(&encoded_merge).unwrap();
	assert_eq!(decoded_batch_merge.starting_seq_num, 300);
	let entries_merge = decoded_batch_merge.entries();
	assert_eq!(entries_merge.len(), 1);
	assert_eq!(entries_merge[0].kind, InternalKeyKind::Merge);
	assert_eq!(entries_merge[0].key.as_slice(), b"merge_key");
	assert_eq!(entries_merge[0].value.as_ref().unwrap().as_slice(), b"merge_data");
	assert_eq!(entries_merge[0].timestamp, 300);

	// Test with Set operations (should still work as before)
	let mut batch_set = Batch::new(400);
	batch_set
		.add_record(InternalKeyKind::Set, b"set_key".to_vec(), Some(b"set_data".to_vec()), 400)
		.unwrap();

	let encoded_set = batch_set.encode().unwrap();
	let decoded_batch_set = Batch::decode(&encoded_set).unwrap();
	assert_eq!(decoded_batch_set.starting_seq_num, 400);
	let entries_set = decoded_batch_set.entries();
	assert_eq!(entries_set.len(), 1);
	assert_eq!(entries_set[0].kind, InternalKeyKind::Set);
	assert_eq!(entries_set[0].key.as_slice(), b"set_key");
	assert_eq!(entries_set[0].value.as_ref().unwrap().as_slice(), b"set_data");
	assert_eq!(entries_set[0].timestamp, 400);
}

#[test]
fn test_batch_encode_decode() {
	// Create a batch with all possible variations
	let mut batch = Batch::new(12345);

	// Add various types of records with different value pointer scenarios
	batch.add_record(InternalKeyKind::Set, b"key1".to_vec(), Some(b"value1".to_vec()), 1).unwrap();
	batch.add_record(InternalKeyKind::Delete, b"key2".to_vec(), None, 2).unwrap();
	batch
		.add_record(InternalKeyKind::Merge, b"key3".to_vec(), Some(b"merge_value".to_vec()), 3)
		.unwrap();

	// Add records with value pointers (simulating VLog pointers)
	let valueptr1 = ValuePointer::new(100, 200, 10, 20, 30);
	let valueptr2 = ValuePointer::new(400, 500, 15, 25, 35);

	batch
		.add_record_with_valueptr(
			InternalKeyKind::Set,
			b"key4".to_vec(),
			Some(b"large_value".to_vec()),
			Some(valueptr1),
			4,
		)
		.unwrap();
	batch
		.add_record_with_valueptr(InternalKeyKind::Set, b"key5".to_vec(), None, Some(valueptr2), 5)
		.unwrap();
	batch
		.add_record_with_valueptr(InternalKeyKind::Delete, b"key6".to_vec(), None, None, 6)
		.unwrap();

	// Add some edge cases
	batch
		.add_record(InternalKeyKind::Set, b"".to_vec(), Some(b"empty_key_value".to_vec()), 7)
		.unwrap();
	batch
		.add_record(InternalKeyKind::Set, b"empty_value_key".to_vec(), Some(b"".to_vec()), 8)
		.unwrap();
	batch.add_record(InternalKeyKind::Set, b"".to_vec(), Some(b"".to_vec()), 9).unwrap();

	// Add unicode data
	batch
		.add_record(
			InternalKeyKind::Set,
			"🔑".as_bytes().to_vec(),
			Some("🗝️".as_bytes().to_vec()),
			10,
		)
		.unwrap();
	batch
		.add_record(
			InternalKeyKind::Set,
			"こんにちは".as_bytes().to_vec(),
			Some("世界".as_bytes().to_vec()),
			11,
		)
		.unwrap();

	// Verify original batch properties
	assert_eq!(batch.starting_seq_num, 12345);
	assert_eq!(batch.count(), 11);
	assert!(!batch.is_empty());
	assert_eq!(batch.get_highest_seq_num(), 12345 + 10); // starting + count - 1

	// Encode the batch
	let encoded = batch.encode().unwrap();
	assert!(!encoded.is_empty());

	// Decode the batch
	let decoded_batch = Batch::decode(&encoded).unwrap();

	// Verify all properties are preserved
	assert_eq!(decoded_batch.version, BATCH_VERSION);
	assert_eq!(decoded_batch.starting_seq_num, 12345);
	assert_eq!(decoded_batch.count(), 11);
	assert!(!decoded_batch.is_empty());
	assert_eq!(decoded_batch.get_highest_seq_num(), 12345 + 10);

	// Verify entries are preserved correctly
	let entries = decoded_batch.entries();
	assert_eq!(entries.len(), 11);

	// Check first few entries
	assert_eq!(entries[0].kind, InternalKeyKind::Set);
	assert_eq!(entries[0].key.as_slice(), b"key1");
	assert_eq!(entries[0].value.as_ref().unwrap().as_slice(), b"value1");
	assert_eq!(entries[0].timestamp, 1);

	assert_eq!(entries[1].kind, InternalKeyKind::Delete);
	assert_eq!(entries[1].key.as_slice(), b"key2");
	assert!(entries[1].value.is_none());
	assert_eq!(entries[1].timestamp, 2);

	assert_eq!(entries[2].kind, InternalKeyKind::Merge);
	assert_eq!(entries[2].key.as_slice(), b"key3");
	assert_eq!(entries[2].value.as_ref().unwrap().as_slice(), b"merge_value");
	assert_eq!(entries[2].timestamp, 3);

	// Check entries with value pointers
	assert_eq!(entries[3].kind, InternalKeyKind::Set);
	assert_eq!(entries[3].key.as_slice(), b"key4");
	assert_eq!(entries[3].value.as_ref().unwrap().as_slice(), b"large_value");
	assert_eq!(entries[3].timestamp, 4);

	assert_eq!(entries[4].kind, InternalKeyKind::Set);
	assert_eq!(entries[4].key.as_slice(), b"key5");
	assert!(entries[4].value.is_none());
	assert_eq!(entries[4].timestamp, 5);

	assert_eq!(entries[5].kind, InternalKeyKind::Delete);
	assert_eq!(entries[5].key.as_slice(), b"key6");
	assert!(entries[5].value.is_none());
	assert_eq!(entries[5].timestamp, 6);

	// Check edge cases
	assert_eq!(entries[6].kind, InternalKeyKind::Set);
	assert_eq!(entries[6].key.as_slice(), b"");
	assert_eq!(entries[6].value.as_ref().unwrap().as_slice(), b"empty_key_value");
	assert_eq!(entries[6].timestamp, 7);

	assert_eq!(entries[7].kind, InternalKeyKind::Set);
	assert_eq!(entries[7].key.as_slice(), b"empty_value_key");
	// Empty string values decode as None (this is the intended behavior)
	assert!(entries[7].value.is_none());
	assert_eq!(entries[7].timestamp, 8);

	assert_eq!(entries[8].kind, InternalKeyKind::Set);
	assert_eq!(entries[8].key.as_slice(), b"");
	// Empty string values decode as None (this is the intended behavior)
	assert!(entries[8].value.is_none());
	assert_eq!(entries[8].timestamp, 9);

	// Check unicode entries
	assert_eq!(entries[9].kind, InternalKeyKind::Set);
	assert_eq!(entries[9].key.as_slice(), "🔑".as_bytes());
	assert_eq!(entries[9].value.as_ref().unwrap().as_slice(), "🗝️".as_bytes());
	assert_eq!(entries[9].timestamp, 10);

	assert_eq!(entries[10].kind, InternalKeyKind::Set);
	assert_eq!(entries[10].key.as_slice(), "こんにちは".as_bytes());
	assert_eq!(entries[10].value.as_ref().unwrap().as_slice(), "世界".as_bytes());
	assert_eq!(entries[10].timestamp, 11);

	// Verify value pointers are preserved correctly
	assert_eq!(decoded_batch.entries.len(), decoded_batch.valueptrs.len());

	// Test sequence number iteration
	let entries_with_seq_nums: Vec<_> = decoded_batch.entries_with_seq_nums().unwrap().collect();
	assert_eq!(entries_with_seq_nums.len(), 11);

	for (i, (entry_idx, entry, seq_num, timestamp)) in entries_with_seq_nums.iter().enumerate() {
		assert_eq!(*entry_idx, i);
		assert_eq!(*seq_num, 12345 + i as u64);
		assert_eq!(entry.kind, entries[i].kind);
		assert_eq!(entry.key, entries[i].key);
		assert_eq!(entry.value, entries[i].value);
		assert_eq!(entry.timestamp, *timestamp);
	}

	// Test that we can re-encode the decoded batch and get the same result
	let re_encoded = decoded_batch.encode().unwrap();
	assert_eq!(encoded, re_encoded, "Re-encoding should produce identical result");
}
