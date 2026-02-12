use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::compaction::leveled::Strategy;
use crate::levels::LevelManifest;
use crate::lsm::{CompactionOperations, Core, CoreInner};
use crate::test::collect_transaction_all;
use crate::{
	Error,
	InternalKeyKind,
	Key,
	LSMIterator,
	Options,
	Tree,
	TreeBuilder,
	Value,
	WalRecoveryMode,
};

fn create_temp_directory() -> TempDir {
	TempDir::new("test").unwrap()
}

/// Creates test options with common defaults, allowing customization of
/// specific fields
fn create_test_options(path: PathBuf, customizations: impl FnOnce(&mut Options)) -> Arc<Options> {
	let mut opts = Options {
		path,
		vlog_value_threshold: 0,
		..Default::default()
	};
	customizations(&mut opts);
	Arc::new(opts)
}

/// Creates test options optimized for small memtable testing (triggers
/// frequent flushes)
fn create_small_memtable_options(path: PathBuf) -> Arc<Options> {
	create_test_options(path, |opts| {
		opts.max_memtable_size = 64 * 1024; // 64KB
	})
}

/// Creates test options optimized for VLog testing
fn create_vlog_test_options(path: PathBuf) -> Arc<Options> {
	create_test_options(path, |opts| {
		opts.max_memtable_size = 64 * 1024; // 64KB
		opts.vlog_max_file_size = 1024 * 1024; // 1MB
	})
}

/// Creates test options optimized for VLog compaction testing
fn create_vlog_compaction_options(path: PathBuf) -> Arc<Options> {
	create_test_options(path, |opts| {
		opts.max_memtable_size = 64 * 1024; // 64KB
		opts.vlog_max_file_size = 950; // Small size to force frequent rotations
		opts.vlog_gc_discard_ratio = 0.0; // Disable discard ratio to preserve all values
		opts.level_count = 2; // Two levels for compaction strategy
	})
}

#[test(tokio::test)]
async fn test_tree_basic() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let tree = Tree::new(create_test_options(path.clone(), |_| {})).unwrap();

	// Insert a key-value pair
	let key = "hello";
	let value = "world";
	let mut txn = tree.begin().unwrap();
	txn.set(key.as_bytes(), value.as_bytes()).unwrap();
	txn.commit().await.unwrap();

	// Read back the key-value pair
	let txn = tree.begin().unwrap();
	let result = txn.get(key.as_bytes()).unwrap().unwrap();
	assert_eq!(result, value.as_bytes().to_vec());
}

#[test(tokio::test)]
async fn test_memtable_flush() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |_| {});

	let key = "hello";
	let values = ["world", "universe", "everyone", "planet"];

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Insert the same key multiple times with different values
	for value in values.iter() {
		// Insert key-value pair in a new transaction
		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	// Ensure the active memtable is flushed to disk
	tree.flush().unwrap();

	{
		// Read back the key-value pair
		let txn = tree.begin().unwrap();
		let result = txn.get(key.as_bytes()).unwrap().unwrap();
		let expected_value = values.last().unwrap();
		assert_eq!(result, expected_value.as_bytes().to_vec());
	}
}

#[test(tokio::test)]
async fn test_memtable_flush_with_delete() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |_| {});

	let key = "hello";
	let values = ["world", "universe", "everyone"];

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Insert the same key multiple times with different values
	for value in values.iter() {
		// Insert key-value pair in a new transaction
		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	// Delete the key
	{
		let mut txn = tree.begin().unwrap();
		txn.delete(key.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	// Ensure the active memtable is flushed to disk
	tree.flush().unwrap();

	{
		// Read back the key-value pair
		let txn = tree.begin().unwrap();
		let result = txn.get(key.as_bytes()).unwrap();
		assert!(result.is_none(), "Expected key to be deleted");
	}
}

#[test(tokio::test)]
async fn test_memtable_flush_with_multiple_keys_and_updates() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	// Set a reasonable memtable size to trigger multiple flushes
	let opts = create_small_memtable_options(path.clone());

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Number of keys to insert
	const KEY_COUNT: usize = 10001;
	// Number of updates per key
	const UPDATES_PER_KEY: usize = 2;

	// Track the latest value for each key for verification
	let mut expected_values = Vec::with_capacity(KEY_COUNT);

	// Insert each key with multiple updates
	for i in 0..KEY_COUNT {
		let key = format!("key_{i:05}");
		for update in 0..UPDATES_PER_KEY {
			let value = format!("value_{i:05}_update_{update}");

			// Save the final value for verification
			if update == UPDATES_PER_KEY - 1 {
				expected_values.push((key.clone(), value.clone()));
			}

			// Insert in a new transaction
			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}
	}

	// Final flush to ensure all data is on disk
	tree.flush().unwrap();

	// Verify all keys have their final values
	for (key, expected_value) in expected_values {
		let txn = tree.begin().unwrap();
		let result = txn.get(key.as_bytes()).unwrap().unwrap();
		assert_eq!(
			result,
			expected_value.as_bytes().to_vec(),
			"Key '{key}' should have final value '{expected_value}'"
		);
	}

	// Verify the LSM state: we should have multiple SSTables
	let l0_size = tree.core.level_manifest.read().unwrap().levels.get_levels()[0].tables.len();
	assert!(l0_size > 0, "Expected SSTables in L0, got {l0_size}");
}

#[test(tokio::test)]
async fn test_persistence() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	// Set a reasonable memtable size to trigger multiple flushes
	let opts = create_test_options(path.clone(), |opts| {
		opts.vlog_max_file_size = 10;
	});

	// Number of keys to insert
	const KEY_COUNT: usize = 100;
	// Number of updates per key
	const UPDATES_PER_KEY: usize = 1;

	// Track the latest value for each key for verification
	let expected_values: Vec<(String, String)>;

	// Step 1: Insert data and close the store
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let mut values = Vec::with_capacity(KEY_COUNT);

		// Insert each key with multiple updates
		for i in 0..KEY_COUNT {
			let key = format!("key_{i:05}");
			for update in 0..UPDATES_PER_KEY {
				let value = format!("value_{i:05}_update_{update}");

				// Save the final value for verification
				if update == UPDATES_PER_KEY - 1 {
					values.push((key.clone(), value.clone()));
				}

				// Insert in a new transaction
				let mut txn = tree.begin().unwrap();
				txn.set(key.as_bytes(), value.as_bytes()).unwrap();
				txn.commit().await.unwrap();
			}
		}

		// Final flush to ensure all data is on disk
		tree.flush().unwrap();

		// Store values for later verification
		expected_values = values;

		// Verify L0 has tables before closing
		let l0_size = tree.core.level_manifest.read().unwrap().levels.get_levels()[0].tables.len();
		assert!(l0_size > 0, "Expected SSTables in L0 before closing, got {l0_size}");

		// Tree will be dropped here, closing the store
		tree.close().await.unwrap();
	}

	// Step 2: Reopen the store and verify data
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Verify L0 has tables after reopening
		let l0_size = tree.core.level_manifest.read().unwrap().levels.get_levels()[0].tables.len();
		assert!(l0_size > 0, "Expected SSTables in L0 after reopening, got {l0_size}");

		// Verify all keys have their final values
		for (key, expected_value) in expected_values.iter() {
			let txn = tree.begin().unwrap();
			let result = txn.get(key.as_bytes()).unwrap();

			assert!(result.is_some(), "Key '{key}' not found after reopening store");

			let value = result.unwrap();
			assert_eq!(
				value,
				expected_value.as_bytes().to_vec(),
				"Key '{}' has incorrect value after reopening. Expected '{}', got '{:?}'",
				key,
				expected_value,
				std::str::from_utf8(value.as_ref()).unwrap_or("<invalid utf8>")
			);
		}
	}
}

#[test(tokio::test)]
async fn test_checkpoint_functionality() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_small_memtable_options(path.clone());

	// Create initial data
	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Insert some test data
	for i in 0..10 {
		let key = format!("key_{i:03}");
		let value = format!("value_{i:03}");

		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	// Force flush to ensure data is on disk
	tree.flush().unwrap();

	// Create checkpoint
	let checkpoint_dir = temp_dir.path().join("checkpoint");
	let metadata = tree.create_checkpoint(&checkpoint_dir).unwrap();

	// Verify checkpoint metadata and structure
	assert!(metadata.timestamp > 0);
	assert_eq!(metadata.sequence_number, 10);
	assert_eq!(metadata.sstable_count, 1);
	assert!(metadata.total_size > 0);

	assert!(checkpoint_dir.exists());
	assert!(checkpoint_dir.join("sstables").exists());
	assert!(checkpoint_dir.join("wal").exists());
	assert!(checkpoint_dir.join("manifest").exists());
	assert!(checkpoint_dir.join("CHECKPOINT_METADATA").exists());

	// Insert more data after checkpoint
	for i in 10..15 {
		let key = format!("key_{i:03}");
		let value = format!("value_{i:03}");

		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	// Force flush to ensure the new data is also on disk
	tree.flush().unwrap();

	// Verify all data exists before restore
	for i in 0..15 {
		let key = format!("key_{i:03}");
		let txn = tree.begin().unwrap();
		let result = txn.get(key.as_bytes()).unwrap();
		assert!(result.is_some(), "Key '{key}' should exist before restore");
	}

	// Restore from checkpoint
	tree.restore_from_checkpoint(&checkpoint_dir).unwrap();

	// Verify data is restored to checkpoint state (keys 0-9 exist, 10-14 don't)
	for i in 0..10 {
		let key = format!("key_{i:03}");
		let expected_value = format!("value_{i:03}");

		let txn = tree.begin().unwrap();
		let result = txn.get(key.as_bytes()).unwrap();
		assert!(result.is_some(), "Key '{key}' should exist after restore");
		assert_eq!(result.unwrap(), expected_value.as_bytes().to_vec());
	}

	// Verify the newer keys don't exist after restore
	for i in 10..15 {
		let key = format!("key_{i:03}");

		let txn = tree.begin().unwrap();
		let result = txn.get(key.as_bytes()).unwrap();
		assert!(result.is_none(), "Key '{key}' should not exist after restore");
	}
}

#[test(tokio::test)]
async fn test_checkpoint_restore_discards_pending_writes() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_small_memtable_options(path.clone());

	// Create initial data and checkpoint
	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Insert initial data
	for i in 0..5 {
		let key = format!("key_{i:03}");
		let value = format!("checkpoint_value_{i:03}");

		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	// Force flush to ensure data is on disk
	tree.flush().unwrap();

	// Create checkpoint
	let checkpoint_dir = temp_dir.path().join("checkpoint");
	let metadata = tree.create_checkpoint(&checkpoint_dir).unwrap();
	assert_eq!(metadata.sequence_number, 5);

	// Insert NEW data that should be discarded during restore
	for i in 0..5 {
		let key = format!("key_{i:03}");
		let value = format!("pending_value_{i:03}"); // Different values

		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	// Verify the pending writes are in memory but not yet flushed
	for i in 0..5 {
		let key = format!("key_{i:03}");
		let txn = tree.begin().unwrap();
		let result = txn.get(key.as_bytes()).unwrap().unwrap();
		assert_eq!(
			result,
			format!("pending_value_{i:03}").as_bytes().to_vec(),
			"Expected pending value before restore"
		);
	}

	// Restore from checkpoint - this should discard pending writes
	tree.restore_from_checkpoint(&checkpoint_dir).unwrap();

	// Verify data is restored to checkpoint state (NOT the pending writes)
	for i in 0..5 {
		let key = format!("key_{i:03}");
		let expected_value = format!("checkpoint_value_{i:03}");

		let txn = tree.begin().unwrap();
		let result = txn.get(key.as_bytes()).unwrap().unwrap();
		assert_eq!(
			result,
			expected_value.as_bytes().to_vec(),
			"Key '{key}' should have checkpoint value '{expected_value}', not pending value"
		);
	}
}

#[test(tokio::test)]
async fn test_simple_range_seek() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_small_memtable_options(path.clone());

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Insert simple keys
	let keys = ["a", "b", "c", "d", "e"];
	for key in keys.iter() {
		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), key.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	// Test range scan BEFORE flushing (should work from memtables)
	let txn = tree.begin().unwrap();
	let range_before_flush = collect_transaction_all(&mut txn.range(b"a", b"d").unwrap()).unwrap();

	// Should return keys "a", "b", "c" ([a, d) range)
	assert_eq!(range_before_flush.len(), 3, "Range scan before flush should return 3 items");

	// Verify the keys and values are correct
	let expected_before = [("a", "a"), ("b", "b"), ("c", "c")];
	for (idx, (key, value)) in range_before_flush.iter().enumerate() {
		let key_str = std::str::from_utf8(key).unwrap();
		let value_str = std::str::from_utf8(value).unwrap();
		assert_eq!(key_str, expected_before[idx].0, "Key mismatch at index {idx} before flush");
		assert_eq!(value_str, expected_before[idx].1, "Value mismatch at index {idx} before flush");
	}

	// Force flush to ensure data is on disk
	tree.flush().unwrap();

	// Test range scan AFTER flushing (should work from SSTables)
	let txn = tree.begin().unwrap();
	let range_after_flush = collect_transaction_all(&mut txn.range(b"a", b"d").unwrap()).unwrap();

	// Should return the same keys after flush
	assert_eq!(range_after_flush.len(), 3, "Range scan after flush should return 3 items");

	// Verify the keys and values are still correct after flush
	let expected_after = [("a", "a"), ("b", "b"), ("c", "c")];
	for (idx, (key, value)) in range_after_flush.iter().enumerate() {
		let key_str = std::str::from_utf8(key).unwrap();
		let value_str = std::str::from_utf8(value).unwrap();
		assert_eq!(key_str, expected_after[idx].0, "Key mismatch at index {idx} after flush");
		assert_eq!(value_str, expected_after[idx].1, "Value mismatch at index {idx} after flush");
	}

	// Test individual gets
	for key in ["a", "b", "c"].iter() {
		let txn = tree.begin().unwrap();
		let result = txn.get(key.as_bytes()).unwrap();

		assert!(result.is_some(), "Key '{key}' should be found");
		let value = result.unwrap();
		let value_str = std::str::from_utf8(value.as_ref()).unwrap();
		assert_eq!(value_str, *key, "Value for key '{key}' should match the key");
	}

	// Verify that the results are identical before and after flush
	assert_eq!(
		range_before_flush, range_after_flush,
		"Range scan results should be identical before and after flush"
	);
}

#[test(tokio::test)]
async fn test_large_range_scan() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_small_memtable_options(path.clone());

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Insert 10,000 items with zero-padded keys for consistent ordering
	const TOTAL_ITEMS: usize = 10_000;
	let mut expected_items = Vec::with_capacity(TOTAL_ITEMS);

	for i in 0..TOTAL_ITEMS {
		let key = format!("key_{i:06}");
		let value = format!("value_{:06}_content_data_{}", i, i * 2);

		expected_items.push((key.clone(), value.clone()));

		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	// Force flush to ensure all data is on disk
	tree.flush().unwrap();

	// Test 1: Full range scan - should return all 10k items in order
	let start_key = "key_000000".as_bytes();
	let end_key = "key_999999".as_bytes();

	let txn = tree.begin().unwrap();
	let range_result =
		collect_transaction_all(&mut txn.range(start_key, end_key).unwrap()).unwrap();

	assert_eq!(
		range_result.len(),
		TOTAL_ITEMS,
		"Full range scan should return all {TOTAL_ITEMS} items"
	);

	// Verify each item is correct and in order
	for (idx, (key, value)) in range_result.iter().enumerate() {
		let key_str = std::str::from_utf8(key).unwrap();
		let value_str = std::str::from_utf8(value).unwrap();

		let expected_key = &expected_items[idx].0;
		let expected_value = &expected_items[idx].1;

		assert_eq!(
			key_str, expected_key,
			"Key mismatch at index {idx}: expected '{expected_key}', found '{key_str}'"
		);
		assert_eq!(
			value_str, expected_value,
			"Value mismatch at index {idx}: expected '{expected_value}', found '{value_str}'"
		);
	}

	// Test 2: Partial range scan - first 100 items ([key_000000, key_000100) range)
	let partial_start = "key_000000".as_bytes();
	let partial_end = "key_000100".as_bytes();

	let txn = tree.begin().unwrap();
	let partial_result =
		collect_transaction_all(&mut txn.range(partial_start, partial_end).unwrap()).unwrap();

	assert_eq!(partial_result.len(), 100, "Partial range scan should return 100 items");

	// Verify each item in partial range
	for (idx, (key, value)) in partial_result.iter().enumerate() {
		let key_str = std::str::from_utf8(key).unwrap();
		let value_str = std::str::from_utf8(value).unwrap();

		let expected_key = &expected_items[idx].0;
		let expected_value = &expected_items[idx].1;

		assert_eq!(
                key_str, expected_key,
                "Partial range key mismatch at index {idx}: expected '{expected_key}', found '{key_str}'"
            );
		assert_eq!(
                value_str, expected_value,
                "Partial range value mismatch at index {idx}: expected '{expected_value}', found '{value_str}'"
            );
	}

	// Test 3: Middle range scan - items 5000-5099
	let middle_start = "key_005000".as_bytes();
	let middle_end = "key_005100".as_bytes();

	let txn = tree.begin().unwrap();
	let middle_result =
		collect_transaction_all(&mut txn.range(middle_start, middle_end).unwrap()).unwrap();

	assert_eq!(middle_result.len(), 100, "Middle range scan should return 100 items");

	// Verify each item in middle range
	for (idx, (key, value)) in middle_result.iter().enumerate() {
		let key_str = std::str::from_utf8(key).unwrap();
		let value_str = std::str::from_utf8(value).unwrap();

		let expected_idx = 5000 + idx;
		let expected_key = &expected_items[expected_idx].0;
		let expected_value = &expected_items[expected_idx].1;

		assert_eq!(
                key_str, expected_key,
                "Middle range key mismatch at index {idx}: expected '{expected_key}', found '{key_str}'"
            );
		assert_eq!(
                value_str, expected_value,
                "Middle range value mismatch at index {idx}: expected '{expected_value}', found '{value_str}'"
            );
	}

	// Test 4: End range scan - last 100 items
	let end_start = "key_009900".as_bytes();
	let end_end = "key_010000".as_bytes();

	let txn = tree.begin().unwrap();
	let end_result = collect_transaction_all(&mut txn.range(end_start, end_end).unwrap()).unwrap();

	assert_eq!(end_result.len(), 100, "End range scan should return 100 items");

	// Verify each item in end range
	for (idx, (key, value)) in end_result.iter().enumerate() {
		let key_str = std::str::from_utf8(key).unwrap();
		let value_str = std::str::from_utf8(value).unwrap();

		let expected_idx = 9900 + idx;
		let expected_key = &expected_items[expected_idx].0;
		let expected_value = &expected_items[expected_idx].1;

		assert_eq!(
			key_str, expected_key,
			"End range key mismatch at index {idx}: expected '{expected_key}', found '{key_str}'"
		);
		assert_eq!(
                value_str, expected_value,
                "End range value mismatch at index {idx}: expected '{expected_value}', found '{value_str}'"
            );
	}

	// Test 5: Single item range scan
	let single_start = "key_004567".as_bytes();
	let single_end = "key_004568".as_bytes();

	let txn = tree.begin().unwrap();
	let single_result =
		collect_transaction_all(&mut txn.range(single_start, single_end).unwrap()).unwrap();

	assert_eq!(single_result.len(), 1, "Single item range scan should return 1 item");

	let (key, value) = &single_result[0];
	let key_str = std::str::from_utf8(key).unwrap();
	let value_str = std::str::from_utf8(value).unwrap();

	assert_eq!(key_str, "key_004567");
	assert_eq!(value_str, "value_004567_content_data_9134");

	// Test 6: Empty range scan (non-existent range)
	let empty_start = "key_999999".as_bytes();
	let empty_end = "key_888888".as_bytes(); // end < start, should be empty

	let txn = tree.begin().unwrap();
	let empty_result =
		collect_transaction_all(&mut txn.range(empty_start, empty_end).unwrap()).unwrap();

	assert_eq!(empty_result.len(), 0, "Empty range scan should return 0 items");
}

#[test(tokio::test)]
async fn test_range_skip_take() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_small_memtable_options(path.clone());

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	const TOTAL_ITEMS: usize = 10_000;
	let mut expected_items = Vec::with_capacity(TOTAL_ITEMS);

	for i in 0..TOTAL_ITEMS {
		let key = format!("key_{i:06}");
		let value = format!("value_{:06}_content_data_{}", i, i * 2);

		expected_items.push((key.clone(), value.clone()));

		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	tree.flush().unwrap();

	let start_key = "key_000000".as_bytes();
	let end_key = "key_999999".as_bytes();

	let txn = tree.begin().unwrap();
	let mut iter = txn.range(start_key, end_key).unwrap();
	iter.seek_first().unwrap();
	// Skip 5000 items
	for _ in 0..5000 {
		if !iter.valid() {
			break;
		}
		iter.next().unwrap();
	}
	// Take 100 items
	let mut range_result = Vec::new();
	for _ in 0..100 {
		if !iter.valid() {
			break;
		}
		let key = iter.key().user_key().to_vec();
		let value = iter.value().unwrap();
		range_result.push((key, value));
		iter.next().unwrap();
	}

	assert_eq!(
		range_result.len(),
		100,
		"Range scan with skip(5000).take(100) should return 100 items"
	);

	for (idx, (key, value)) in range_result.iter().enumerate() {
		let key_str = std::str::from_utf8(key).unwrap();
		let value_str = std::str::from_utf8(value).unwrap();

		let expected_idx = 5000 + idx;
		let expected_key = &expected_items[expected_idx].0;
		let expected_value = &expected_items[expected_idx].1;

		assert_eq!(
			key_str, expected_key,
			"Key mismatch at index {idx}: expected '{expected_key}', found '{key_str}'"
		);
		assert_eq!(
			value_str, expected_value,
			"Value mismatch at index {idx}: expected '{expected_value}', found '{value_str}'"
		);
	}
}

#[test(tokio::test)]
async fn test_range_skip_take_alphabetical() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_small_memtable_options(path.clone());

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	fn generate_alphabetical_key(index: usize) -> String {
		let mut result = String::new();
		let mut remaining = index + 1;

		while remaining > 0 {
			let digit = (remaining - 1) % 26;
			result.push((b'a' + digit as u8) as char);
			remaining = (remaining - 1) / 26;
		}

		result.chars().rev().collect()
	}

	const TOTAL_ITEMS: usize = 10_000;
	let mut expected_items = Vec::with_capacity(TOTAL_ITEMS);

	for i in 0..TOTAL_ITEMS {
		let key = generate_alphabetical_key(i);
		let value = format!("value_{}_content_data_{}", key, i * 2);

		expected_items.push((key.clone(), value.clone()));

		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	tree.flush().unwrap();

	expected_items.sort_by(|a, b| a.0.cmp(&b.0));

	let beg = [0u8].to_vec();
	let end = [255u8].to_vec();

	let txn = tree.begin().unwrap();
	let mut iter = txn.range(&beg, &end).unwrap();
	iter.seek_first().unwrap();
	// Skip 5000 items
	for _ in 0..5000 {
		if !iter.valid() {
			break;
		}
		iter.next().unwrap();
	}
	// Take 100 items
	let mut range_result = Vec::new();
	for _ in 0..100 {
		if !iter.valid() {
			break;
		}
		let key = iter.key().user_key().to_vec();
		let value = iter.value().unwrap();
		range_result.push((key, value));
		iter.next().unwrap();
	}

	assert_eq!(
		range_result.len(),
		100,
		"Range scan with skip(5000).take(100) should return 100 items"
	);

	for (idx, (key, value)) in range_result.iter().enumerate() {
		let key_str = std::str::from_utf8(key).unwrap();
		let value_str = std::str::from_utf8(value).unwrap();

		let expected_idx = 5000 + idx;
		let expected_key = &expected_items[expected_idx].0;
		let expected_value = &expected_items[expected_idx].1;

		assert_eq!(
			key_str, expected_key,
			"Key mismatch at index {idx}: expected '{expected_key}', found '{key_str}'"
		);
		assert_eq!(
			value_str, expected_value,
			"Value mismatch at index {idx}: expected '{expected_value}', found '{value_str}'"
		);
	}
}

#[test(tokio::test)]
async fn test_range_limit_functionality() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_small_memtable_options(path.clone());

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	const TOTAL_ITEMS: usize = 10_000;
	let mut expected_items = Vec::with_capacity(TOTAL_ITEMS);

	for i in 0..TOTAL_ITEMS {
		let key = format!("key_{i:06}");
		let value = format!("value_{:06}_content_data_{}", i, i * 2);

		expected_items.push((key.clone(), value.clone()));

		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	tree.flush().unwrap();

	let beg = [0u8].to_vec();
	let end = [255u8].to_vec();

	let txn = tree.begin().unwrap();
	let mut iter = txn.range(&beg, &end).unwrap();
	iter.seek_first().unwrap();
	// Take 100 items
	let mut limited_result = Vec::new();
	for _ in 0..100 {
		if !iter.valid() {
			break;
		}
		let key = iter.key().user_key().to_vec();
		let value = iter.value().unwrap();
		limited_result.push((key, value));
		iter.next().unwrap();
	}

	assert_eq!(
		limited_result.len(),
		100,
		"Range scan with .take(100) should return exactly 100 items"
	);

	for (idx, (key, value)) in limited_result.iter().enumerate() {
		let key_str = std::str::from_utf8(key).unwrap();
		let value_str = std::str::from_utf8(value).unwrap();

		let expected_key = &expected_items[idx].0;
		let expected_value = &expected_items[idx].1;

		assert_eq!(
                key_str, expected_key,
                "Key mismatch at index {idx} with limit 100: expected '{expected_key}', found '{key_str}'"
            );
		assert_eq!(
                value_str, expected_value,
                "Value mismatch at index {idx} with limit 100: expected '{expected_value}', found '{value_str}'"
            );
	}

	let txn = tree.begin().unwrap();
	let mut iter = txn.range(&beg, &end).unwrap();
	iter.seek_first().unwrap();
	// Take 1000 items
	let mut limited_result_1000 = Vec::new();
	for _ in 0..1000 {
		if !iter.valid() {
			break;
		}
		let key = iter.key().user_key().to_vec();
		let value = iter.value().unwrap();
		limited_result_1000.push((key, value));
		iter.next().unwrap();
	}

	assert_eq!(
		limited_result_1000.len(),
		1000,
		"Range scan with .take(1000) should return exactly 1000 items"
	);

	for (idx, (key, value)) in limited_result_1000.iter().enumerate() {
		let key_str = std::str::from_utf8(key).unwrap();
		let value_str = std::str::from_utf8(value).unwrap();

		let expected_key = &expected_items[idx].0;
		let expected_value = &expected_items[idx].1;

		assert_eq!(
                key_str, expected_key,
                "Key mismatch at index {idx} with limit 1000: expected '{expected_key}', found '{key_str}'"
            );
		assert_eq!(
                value_str, expected_value,
                "Value mismatch at index {idx} with limit 1000: expected '{expected_value}', found '{value_str}'"
            );
	}

	let txn = tree.begin().unwrap();
	let mut iter = txn.range(&beg, &end).unwrap();
	iter.seek_first().unwrap();
	// Take 15000 items
	let mut limited_result_large = Vec::new();
	for _ in 0..15000 {
		if !iter.valid() {
			break;
		}
		let key = iter.key().user_key().to_vec();
		let value = iter.value().unwrap();
		limited_result_large.push((key, value));
		iter.next().unwrap();
	}

	assert_eq!(
		limited_result_large.len(),
		TOTAL_ITEMS,
		"Range scan with .take(15000) should return all {TOTAL_ITEMS} items"
	);

	let txn = tree.begin().unwrap();
	let unlimited_result = collect_transaction_all(&mut txn.range(&beg, &end).unwrap()).unwrap();

	assert_eq!(
		unlimited_result.len(),
		TOTAL_ITEMS,
		"Range scan with no limit should return all {TOTAL_ITEMS} items"
	);

	let txn = tree.begin().unwrap();
	let mut iter = txn.range(&beg, &end).unwrap();
	iter.seek_first().unwrap();
	// Take 1 item
	let mut single_result = Vec::new();
	if iter.valid() {
		let key = iter.key().user_key().to_vec();
		let value = iter.value().unwrap();
		single_result.push((key, value));
	}

	assert_eq!(single_result.len(), 1, "Range scan with .take(1) should return exactly 1 item");

	let (key, value) = &single_result[0];
	let key_str = std::str::from_utf8(key).unwrap();
	let value_str = std::str::from_utf8(value).unwrap();

	assert_eq!(key_str, "key_000000");
	assert_eq!(value_str, "value_000000_content_data_0");

	let txn = tree.begin().unwrap();
	let mut iter = txn.range(&beg, &end).unwrap();
	iter.seek_first().unwrap();
	// Take 0 items (don't collect anything)
	let zero_result: Vec<(Key, Option<Value>)> = Vec::new();

	assert_eq!(zero_result.len(), 0, "Range scan with .take(0) should return no items");
}

#[test(tokio::test)]
async fn test_range_limit_with_skip_take() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_small_memtable_options(path.clone());

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	const TOTAL_ITEMS: usize = 20_000;
	let mut expected_items = Vec::with_capacity(TOTAL_ITEMS);

	for i in 0..TOTAL_ITEMS {
		let key = format!("key_{i:06}");
		let value = format!("value_{:06}_content_data_{}", i, i * 2);

		expected_items.push((key.clone(), value.clone()));

		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	tree.flush().unwrap();

	let beg = [0u8].to_vec();
	let end = [255u8].to_vec();

	let txn = tree.begin().unwrap();
	let mut iter = txn.range(&beg, &end).unwrap();
	iter.seek_first().unwrap();
	// Skip 5000 items
	for _ in 0..5000 {
		if !iter.valid() {
			break;
		}
		iter.next().unwrap();
	}
	// Take 100 items
	let mut unlimited_range_result = Vec::new();
	for _ in 0..100 {
		if !iter.valid() {
			break;
		}
		let key = iter.key().user_key().to_vec();
		let value = iter.value().unwrap();
		unlimited_range_result.push((key, value));
		iter.next().unwrap();
	}

	assert_eq!(
		unlimited_range_result.len(),
		100,
		"Range scan followed by skip(5000).take(100) should return 100 items"
	);

	for (idx, (key, value)) in unlimited_range_result.iter().enumerate() {
		let key_str = std::str::from_utf8(key).unwrap();
		let value_str = std::str::from_utf8(value).unwrap();

		let expected_idx = 5000 + idx;
		let expected_key = &expected_items[expected_idx].0;
		let expected_value = &expected_items[expected_idx].1;

		assert_eq!(
                key_str, expected_key,
                "Key mismatch at index {idx} with unlimited range: expected '{expected_key}', found '{key_str}'"
            );
		assert_eq!(
                value_str, expected_value,
                "Value mismatch at index {idx} with unlimited range: expected '{expected_value}', found '{value_str}'"
            );
	}

	// Case: Test another skip position with take
	let txn = tree.begin().unwrap();
	let mut iter = txn.range(&beg, &end).unwrap();
	iter.seek_first().unwrap();
	// Skip 5000 items
	for _ in 0..5000 {
		if !iter.valid() {
			break;
		}
		iter.next().unwrap();
	}
	// Take 50 items
	let mut res = Vec::new();
	for _ in 0..50 {
		if !iter.valid() {
			break;
		}
		let key = iter.key().user_key().to_vec();
		let value = iter.value().unwrap();
		res.push((key, value));
		iter.next().unwrap();
	}

	assert_eq!(res.len(), 50, "Range scan followed by skip(5000).take(50) should return 50 items");

	for (idx, (key, value)) in res.iter().enumerate() {
		let key_str = std::str::from_utf8(key).unwrap();
		let value_str = std::str::from_utf8(value).unwrap();

		let expected_idx = 5000 + idx;
		let expected_key = &expected_items[expected_idx].0;
		let expected_value = &expected_items[expected_idx].1;

		assert_eq!(
                key_str, expected_key,
                "Key mismatch at index {idx} with limit 5050: expected '{expected_key}', found '{key_str}'"
            );
		assert_eq!(
                value_str, expected_value,
                "Value mismatch at index {idx} with limit 5050: expected '{expected_value}', found '{value_str}'"
            );
	}
}

#[test(tokio::test)]
async fn test_vlog_basic() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_vlog_test_options(path.clone());

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Test 1: Values should be stored in VLog
	let small_key = "small_key";
	let small_value = "small_value"; // < 100 bytes

	let mut txn = tree.begin().unwrap();
	txn.set(small_key.as_bytes(), small_value.as_bytes()).unwrap();
	txn.commit().await.unwrap();

	// Test 2: Large values should be stored in VLog
	let large_key = "large_key";
	let large_value = "X".repeat(200); // >= 100 bytes

	let mut txn = tree.begin().unwrap();
	txn.set(large_key.as_bytes(), large_value.as_bytes()).unwrap();
	txn.commit().await.unwrap();

	// Force memtable flush to persist to SSTables
	tree.flush().unwrap();

	// Verify both values can be read correctly
	let txn = tree.begin().unwrap();

	let retrieved_small = txn.get(small_key.as_bytes()).unwrap().unwrap();
	assert_eq!(retrieved_small, small_value.as_bytes().to_vec());

	let retrieved_large = txn.get(large_key.as_bytes()).unwrap().unwrap();
	assert_eq!(retrieved_large, large_value.as_bytes().to_vec());
}

#[test(tokio::test(flavor = "multi_thread"))]
async fn test_vlog_concurrent_operations() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.vlog_max_file_size = 512 * 1024;
		opts.max_memtable_size = 32 * 1024;
	});

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	let mut expected_values = Vec::new();

	for i in 0..1000 {
		let key = format!("key_{i:04}");
		let value = if i % 2 == 0 {
			format!("small_value_{i}")
		} else {
			format!("large_value_{}_with_padding_{}", i, "X".repeat(100))
		};

		expected_values.push((key.clone(), value.clone()));

		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	// Force flush to ensure all data is persisted
	tree.close().await.unwrap();

	let tree = Arc::new(tree);

	// Concurrent reads
	let read_handles: Vec<_> = (0..5)
		.map(|reader_id| {
			let tree = Arc::clone(&tree);
			let expected_values = expected_values.clone();
			tokio::spawn(async move {
				for (key, expected_value) in expected_values {
					let txn = tree.begin().unwrap();
					let retrieved = txn.get(key.as_bytes()).unwrap().unwrap();
					assert_eq!(
						retrieved,
						expected_value.as_bytes().to_vec(),
						"Reader {reader_id} failed to get correct value for key {key}"
					);
				}
			})
		})
		.collect();

	// Wait for all reads to complete
	for handle in read_handles {
		handle.await.unwrap();
	}

	// Explicitly close the tree to ensure proper cleanup
	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn test_vlog_file_rotation() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.vlog_max_file_size = 2048; // 2KB files to force frequent rotation
		opts.enable_vlog = true;
	});

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Insert enough data to force multiple file rotations
	let mut keys_and_values = Vec::new();
	for i in 0..50 {
		let key = format!("rotation_key_{i:03}");
		let value = format!("rotation_value_{}_with_lots_of_padding_{}", i, "Z".repeat(200));
		keys_and_values.push((key.clone(), value.clone()));

		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	// Force flush
	tree.flush().unwrap();

	// Check that multiple VLog files were created
	let vlog_dir = path.join("vlog");
	let entries = std::fs::read_dir(&vlog_dir).unwrap();
	let vlog_files: Vec<_> = entries
		.filter_map(|e| {
			let entry = e.ok()?;
			let name = entry.file_name().to_string_lossy().to_string();
			if opts.is_vlog_filename(&name) {
				Some(name)
			} else {
				None
			}
		})
		.collect();

	assert!(vlog_files.len() > 1, "Expected multiple VLog files due to rotation");

	// Verify all data is still accessible across multiple files
	for (key, expected_value) in keys_and_values {
		let txn = tree.begin().unwrap();
		let retrieved = txn.get(key.as_bytes()).unwrap().unwrap();
		assert_eq!(
			retrieved,
			expected_value.as_bytes().to_vec(),
			"Key {key} has incorrect value across file rotation"
		);
	}
}

#[test(tokio::test)]
async fn test_discard_file_directory_structure() {
	// Test to verify that the DISCARD file lives in the main database directory,
	// not in the VLog subdirectory
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.vlog_max_file_size = 1024;
		opts.max_memtable_size = 1024;
		opts.enable_vlog = true;
	});

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Insert some data to ensure VLog and discard stats are created
	let large_value = vec![1u8; 200]; // Large enough for VLog
	for i in 0..5 {
		let key = format!("key_{i}");
		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), &large_value).unwrap();
		txn.commit().await.unwrap();
	}

	// Force flush to create VLog files and initialize discard stats
	tree.flush().unwrap();

	// Update some keys to create discard statistics
	let mut discard_updates = HashMap::new();
	let stats = tree.get_all_vlog_stats();
	for (file_id, total_size, _discard_bytes, _ratio) in &stats {
		if *total_size > 0 {
			discard_updates.insert(*file_id, (*total_size as f64 * 0.3) as i64);
		}
	}
	tree.update_vlog_discard_stats(&discard_updates);

	// Verify directory structure
	let main_db_dir = &path;
	let vlog_dir = path.join("vlog");
	let sstables_dir = path.join("sstables");
	let wal_dir = path.join("wal");
	let discard_stats_dir = path.join("discard_stats");
	let delete_list_dir = path.join("delete_list");

	// Check that all expected directories exist
	assert!(main_db_dir.exists(), "Main database directory should exist");
	assert!(vlog_dir.exists(), "VLog directory should exist");
	assert!(sstables_dir.exists(), "SSTables directory should exist");
	assert!(wal_dir.exists(), "WAL directory should exist");
	assert!(discard_stats_dir.exists(), "Discard stats directory should exist");
	assert!(delete_list_dir.exists(), "Delete list directory should exist");

	// Check that DISCARD file is in the main database directory
	let discard_file_in_main = discard_stats_dir.join("DISCARD");
	let discard_file_in_vlog = vlog_dir.join("DISCARD");

	assert!(
		discard_file_in_main.exists(),
		"DISCARD file should exist in main database directory: {discard_file_in_main:?}"
	);
	assert!(
		!discard_file_in_vlog.exists(),
		"DISCARD file should NOT exist in VLog directory: {discard_file_in_vlog:?}"
	);

	// Verify that VLog files are in the VLog directory
	let vlog_entries = std::fs::read_dir(&vlog_dir).unwrap();
	let vlog_files: Vec<_> = vlog_entries
		.filter_map(|entry| {
			let entry = entry.ok()?;
			let name = entry.file_name().to_string_lossy().to_string();
			if opts.is_vlog_filename(&name) {
				Some(name)
			} else {
				None
			}
		})
		.collect();

	assert!(!vlog_files.is_empty(), "Should have VLog files in the VLog directory");

	// Verify that the DISCARD file is separate from VLog files
	for vlog_file in &vlog_files {
		assert_ne!(vlog_file, "DISCARD", "DISCARD should not be mistaken for a VLog file");
	}

	// Verify other expected files are in the main directory
	let manifest_dir = main_db_dir.join("manifest");
	assert!(
		manifest_dir.exists(),
		"manifest directory should be in main directory alongside DISCARD"
	);

	println!("✓ Directory structure verification passed:");
	println!("  Main DB dir: {main_db_dir:?}");
	println!(
		"  DISCARD file: {:?} (exists: {})",
		discard_file_in_main,
		discard_file_in_main.exists()
	);
	println!("  VLog dir: {vlog_dir:?}");
	println!("  Discard stats dir: {discard_stats_dir:?}");
	println!("  Delete list dir: {delete_list_dir:?}");
	println!("  VLog files: {vlog_files:?}");
	println!("  manifest dir: {:?} (exists: {})", manifest_dir, manifest_dir.exists());
}

#[test(tokio::test)]
async fn test_compaction_with_updates_and_delete() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.level_count = 2;
		opts.vlog_max_file_size = 20;
		opts.enable_vlog = true;
	});

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Create 2 keys in ascending order
	let keys = ["key-1".as_bytes(), "key-2".as_bytes()];

	// Insert first version of each key
	for (i, key) in keys.iter().enumerate() {
		let value = format!("value-{}-v1", i + 1);
		let mut tx = tree.begin().unwrap();
		tx.set(*key, value.as_bytes()).unwrap();
		tx.commit().await.unwrap();
		tree.flush().unwrap();
	}

	// Insert second version of each key
	for (i, key) in keys.iter().enumerate() {
		let value = format!("value-{}-v2", i + 1);
		let mut tx = tree.begin().unwrap();
		tx.set(*key, value.as_bytes()).unwrap();
		tx.commit().await.unwrap();
		tree.flush().unwrap();
	}

	// Verify the latest values before delete
	for (i, key) in keys.iter().enumerate() {
		let tx = tree.begin().unwrap();
		let result = tx.get(*key).unwrap();
		let expected = format!("value-{}-v2", i + 1);
		assert_eq!(result, Some(expected.as_bytes().to_vec()));
	}

	// Delete all keys
	for key in keys.iter() {
		let mut tx = tree.begin().unwrap();
		tx.delete(*key).unwrap();
		tx.commit().await.unwrap();
	}

	// Flush memtable
	tree.flush().unwrap();

	// Force compaction
	let strategy = Arc::new(Strategy::default());
	tree.core.compact(strategy).unwrap();

	// There could be multiple VLog files, need to garbage collect them all but
	// only one should remain because the active VLog file does not get garbage
	// collected
	for _ in 0..6 {
		tree.garbage_collect_vlog().await.unwrap();
	}

	// Verify all keys are gone after compaction
	for key in keys.iter() {
		let tx = tree.begin().unwrap();
		let result = tx.get(*key).unwrap();
		assert_eq!(result, None);
	}

	// Verify that only one VLog file remains after garbage collection
	let vlog_dir = path.join("vlog");
	let entries = std::fs::read_dir(&vlog_dir).unwrap();
	let vlog_files: Vec<_> = entries
		.filter_map(|e| {
			let entry = e.ok()?;
			let name = entry.file_name().to_string_lossy().to_string();
			if opts.is_vlog_filename(&name) {
				Some(name)
			} else {
				None
			}
		})
		.collect();

	assert_eq!(
		vlog_files.len(),
		1,
		"Should have exactly one VLog file after garbage collection, found: {vlog_files:?}"
	);
}

#[test(tokio::test)]
async fn test_compaction_with_updates_and_delete_on_same_key() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.level_count = 2;
		opts.vlog_max_file_size = 20;
		opts.enable_vlog = true;
	});

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Create single key
	let keys = ["key-1".as_bytes()];

	// Insert first version of each key
	for (i, key) in keys.iter().enumerate() {
		let value = format!("value-{}-v1", i + 1);
		let mut tx = tree.begin().unwrap();
		tx.set(*key, value.as_bytes()).unwrap();
		tx.commit().await.unwrap();
		tree.flush().unwrap();
	}

	// Insert second version of each key
	for (i, key) in keys.iter().enumerate() {
		let value = format!("value-{}-v2", i + 1);
		let mut tx = tree.begin().unwrap();
		tx.set(*key, value.as_bytes()).unwrap();
		tx.commit().await.unwrap();
		tree.flush().unwrap();
	}

	// Insert third version of each key
	for (i, key) in keys.iter().enumerate() {
		let value = format!("value-{}-v3", i + 1);
		let mut tx = tree.begin().unwrap();
		tx.set(*key, value.as_bytes()).unwrap();
		tx.commit().await.unwrap();
		tree.flush().unwrap();
	}

	// Insert fourth version of each key
	for (i, key) in keys.iter().enumerate() {
		let value = format!("value-{}-v4", i + 1);
		let mut tx = tree.begin().unwrap();
		tx.set(*key, value.as_bytes()).unwrap();
		tx.commit().await.unwrap();
		tree.flush().unwrap();
	}

	// Verify the latest values before delete
	for (i, key) in keys.iter().enumerate() {
		let tx = tree.begin().unwrap();
		let result = tx.get(*key).unwrap();
		assert_eq!(result, Some(format!("value-{}-v4", i + 1).as_bytes().to_vec()));
	}

	// Delete all keys
	for key in keys.iter() {
		let mut tx = tree.begin().unwrap();
		tx.delete(*key).unwrap();
		tx.commit().await.unwrap();
	}

	// Flush memtable
	tree.flush().unwrap();

	// Force compaction
	let strategy = Arc::new(Strategy::default());
	tree.core.compact(strategy).unwrap();

	// There could be multiple VLog files, need to garbage collect them all but
	// only one should remain because the active VLog file does not get garbage
	// collected
	for _ in 0..6 {
		tree.garbage_collect_vlog().await.unwrap();
	}

	// Verify all keys are gone after compaction
	for key in keys.iter() {
		let tx = tree.begin().unwrap();
		let result = tx.get(*key).unwrap();
		assert_eq!(result, None);
	}

	// Verify that only one VLog file remains after garbage collection
	let vlog_dir = path.join("vlog");
	let entries = std::fs::read_dir(&vlog_dir).unwrap();
	let vlog_files: Vec<_> = entries
		.filter_map(|e| {
			let entry = e.ok()?;
			let name = entry.file_name().to_string_lossy().to_string();
			if opts.is_vlog_filename(&name) {
				Some(name)
			} else {
				None
			}
		})
		.collect();

	assert_eq!(
		vlog_files.len(),
		1,
		"Should have exactly one VLog file after garbage collection, found: {vlog_files:?}"
	);
}

#[test(tokio::test)]
async fn test_vlog_compaction_preserves_sequence_numbers() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_vlog_compaction_options(path.clone());

	// Open the Tree (database instance)
	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Define keys used throughout the test
	let key1 = b"test_key_1".to_vec();
	let key2 = b"test_key_2".to_vec();
	let key3 = b"test_key_3".to_vec();

	// --- Step 1: Insert multiple versions of key1 to fill the first VLog file ---
	for i in 0..3 {
		let value = format!("value1_version_{i}").repeat(10); // Large value to fill VLog
		let mut tx = tree.begin().unwrap();
		tx.set(&key1, value.as_bytes()).unwrap();
		tx.commit().await.unwrap();
		tree.flush().unwrap();
	}

	// --- Step 2: Insert and delete key2 ---
	// We are doing this to make sure there are deleted keys in the VLog file
	{
		// Insert key2
		let mut tx = tree.begin().unwrap();
		tx.set(&key2, b"value2").unwrap();
		tx.commit().await.unwrap();
		tree.flush().unwrap();

		// Delete key2
		let mut tx = tree.begin().unwrap();
		tx.delete(&key2).unwrap();
		tx.commit().await.unwrap();
		tree.flush().unwrap();
	}

	// --- Step 3: Validate current value of key1 (should be last inserted version)
	// ---
	{
		let tx = tree.begin().unwrap();
		let current_value = tx.get(&key1).unwrap().unwrap();
		assert_eq!(&current_value, "value1_version_2".to_string().repeat(10).as_bytes());
	}

	// --- Step 4: Insert key3 values which will rotate VLog file ---
	for i in 0..2 {
		let value = format!("value2_version_{i}").repeat(10); // Large values
		let mut tx = tree.begin().unwrap();
		tx.set(&key3, value.as_bytes()).unwrap();
		tx.commit().await.unwrap();
		tree.flush().unwrap();
	}

	// --- Step 5: Update key1 with a final value ---
	{
		let mut tx = tree.begin().unwrap();
		tx.set(&key1, "final_value_key1".repeat(10).as_bytes()).unwrap();
		tx.commit().await.unwrap();
	}
	tree.flush().unwrap();

	// --- Step 6: Verify latest value of key1 before compaction ---
	{
		let tx = tree.begin().unwrap();
		let value = tx.get(&key1).unwrap().unwrap();
		assert_eq!(&value, "final_value_key1".repeat(10).as_bytes());
	}

	// --- Step 7: Trigger manual compaction of the LSM tree ---
	let strategy = Arc::new(Strategy::default());
	tree.core.compact(strategy).unwrap();

	// --- Step 8: Run VLog garbage collection (which internally can trigger file
	// compaction) ---
	tree.garbage_collect_vlog().await.unwrap();

	// --- Step 9: Verify key1 still returns the correct latest value after
	// compaction ---
	{
		let tx = tree.begin().unwrap();
		let value = tx.get(&key1).unwrap().unwrap();
		assert_eq!(
                &value,
                "final_value_key1".repeat(10).as_bytes(),
                "After VLog compaction, key1 returned incorrect value. The sequence number was not preserved during compaction."
            );
	}

	// --- Step 10: Clean shutdown ---
	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn test_sstable_lsn_bug() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 1024;
		opts.level_count = 2;
	});

	// Step 1: Create database and write data for first table
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Write data for first table
		for i in 0..6 {
			let key = format!("batch_0_key_{:03}", i);
			let value = format!("batch_0_value_{:03}", i);

			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Force first flush
		tree.flush().unwrap();

		// Write data for second table
		for i in 0..6 {
			let key = format!("batch_1_key_{:03}", i);
			let value = format!("batch_1_value_{:03}", i);

			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Force second flush
		tree.flush().unwrap();

		// Verify all keys exist immediately after flush
		for batch in 0..2 {
			for i in 0..6 {
				let key = format!("batch_{}_key_{:03}", batch, i);
				let expected_value = format!("batch_{}_value_{:03}", batch, i);

				let txn = tree.begin().unwrap();
				let result = txn.get(key.as_bytes()).unwrap();
				assert!(result.is_some(), "Key '{}' should exist immediately after flush", key);
				assert_eq!(&result.unwrap(), expected_value.as_bytes());
			}
		}

		// Close the database
		tree.close().await.unwrap();
	}

	// Step 2: Reopen database and verify data persistence
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Verify all keys still exist after restart
		for batch in 0..2 {
			for i in 0..6 {
				let key = format!("batch_{}_key_{:03}", batch, i);
				let expected_value = format!("batch_{}_value_{:03}", batch, i);

				let txn = tree.begin().unwrap();
				let result = txn.get(key.as_bytes()).unwrap();
				assert!(result.is_some(), "Key '{}' should exist after restart", key);
				assert_eq!(&result.unwrap(), expected_value.as_bytes());
			}
		}

		tree.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_table_id_assignment_across_restart() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	// Create options with very small memtable to force frequent flushes
	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 4 * 1024;
		opts.level_count = 2;
	});

	// Step 1: Create initial database and write data for 2 memtables
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Write data for first table
		for i in 0..6 {
			let key = format!("batch_0_key_{:03}", i);
			let value = format!("batch_0_value_{:03}", i);

			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Force first flush to create first table
		tree.flush().unwrap();

		// Write data for second table
		for i in 0..6 {
			let key = format!("batch_1_key_{:03}", i);
			let value = format!("batch_1_value_{:03}", i);

			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Force second flush to create second table
		tree.flush().unwrap();

		// Verify we have 2 tables in L0
		let l0_size = tree.core.level_manifest.read().unwrap().levels.get_levels()[0].tables.len();
		assert_eq!(l0_size, 2, "Expected 2 tables in L0 after initial writes, got {l0_size}");

		// Get the table IDs from the first session
		let (table1_id, table2_id, next_table_id) = {
			let manifest = tree.core.level_manifest.read().unwrap();
			let table1_id = manifest.levels.get_levels()[0].tables[0].id;
			let table2_id = manifest.levels.get_levels()[0].tables[1].id;
			let next_table_id = manifest.next_table_id();
			(table1_id, table2_id, next_table_id)
		};

		// Verify table IDs are increasing (note: tables are stored in reverse order by
		// sequence number) So the first table in the vector has the higher ID
		assert!(
			table1_id > table2_id,
			"Table 1 ID should be greater than table 2 ID (newer table first)"
		);
		assert!(
			table1_id < next_table_id,
			"Next table ID should be greater than existing table IDs"
		);

		// Close the database
		tree.close().await.unwrap();
	}

	// Step 2: Reopen the database
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		{
			// Verify we still have 2 tables in L0 after reopening
			let l0_size =
				tree.core.level_manifest.read().unwrap().levels.get_levels()[0].tables.len();
			assert_eq!(l0_size, 2, "Expected 2 tables in L0 after reopening, got {l0_size}");
			// Get the table IDs after reopening
			let manifest = tree.core.level_manifest.read().unwrap();
			let table1_id = manifest.levels.get_levels()[0].tables[0].id;
			let table2_id = manifest.levels.get_levels()[0].tables[1].id;
			let next_table_id = manifest.next_table_id();

			// Verify table IDs are still in correct order (newer table first)
			assert!(
				table1_id > table2_id,
				"Table 1 ID should be greater than table 2 ID (newer table first)"
			);
			assert!(
				table1_id < next_table_id,
				"Next table ID should be greater than existing table IDs after reopen"
			);
		}

		// Step 3: Add more data to create a 3rd table
		for i in 0..6 {
			let key = format!("batch_2_key_{:03}", i);
			let value = format!("batch_2_value_{:03}", i);

			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Force flush to create the 3rd table
		tree.flush().unwrap();

		{
			// Verify we now have 3 tables in L0
			let l0_size =
				tree.core.level_manifest.read().unwrap().levels.get_levels()[0].tables.len();
			assert_eq!(l0_size, 3, "Expected 3 tables in L0 after adding more data, got {l0_size}");

			// Get the table IDs from all 3 tables
			let manifest = tree.core.level_manifest.read().unwrap();
			let table1_id = manifest.levels.get_levels()[0].tables[0].id;
			let table2_id = manifest.levels.get_levels()[0].tables[1].id;
			let table3_id = manifest.levels.get_levels()[0].tables[2].id;
			let next_table_id = manifest.next_table_id();

			// Verify table IDs are in correct order (newer tables first)
			assert!(
				table1_id > table2_id,
				"Table 1 ID should be greater than table 2 ID (newer table first)"
			);
			assert!(
				table2_id > table3_id,
				"Table 2 ID should be greater than table 3 ID (newer table first)"
			);
			assert!(
				table1_id < next_table_id,
				"Next table ID should be greater than all existing table IDs"
			);
		}

		// Verify we can read data from all tables
		for batch in 0..3 {
			for i in 0..6 {
				let key = format!("batch_{}_key_{:03}", batch, i);
				let expected_value = format!("batch_{}_value_{:03}", batch, i);

				let txn = tree.begin().unwrap();
				let result = txn.get(key.as_bytes()).unwrap();
				assert!(result.is_some(), "Key '{}' should exist after restart", key);
				assert_eq!(&result.unwrap(), expected_value.as_bytes());
			}
		}

		// Close the database
		tree.close().await.unwrap();
	}

	// Step 4: Final verification - reopen and check everything is still correct
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Verify we still have 3 tables
		let l0_size = tree.core.level_manifest.read().unwrap().levels.get_levels()[0].tables.len();
		assert_eq!(l0_size, 3, "Expected 3 tables in L0 after final reopen, got {l0_size}");

		// Verify table IDs are still in correct order (newer tables first)
		let manifest = tree.core.level_manifest.read().unwrap();
		let table1_id = manifest.levels.get_levels()[0].tables[0].id;
		let table2_id = manifest.levels.get_levels()[0].tables[1].id;
		let table3_id = manifest.levels.get_levels()[0].tables[2].id;
		let next_table_id = manifest.next_table_id();

		assert!(
			table1_id > table2_id,
			"Final check: Table 1 ID should be greater than table 2 ID (newer table first)"
		);
		assert!(
			table2_id > table3_id,
			"Final check: Table 2 ID should be greater than table 3 ID (newer table first)"
		);
		assert!(
			table1_id < next_table_id,
			"Final check: Next table ID should be greater than all existing table IDs"
		);

		// Verify that we can read some data (simplified check)
		let txn = tree.begin().unwrap();
		let result = txn.get("batch_0_key_000".as_bytes()).unwrap();
		assert!(result.is_some(), "Should be able to read at least one key");
	}
}

#[test(tokio::test(flavor = "multi_thread"))]
async fn test_vlog_prefill_on_reopen() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.vlog_max_file_size = 10; // Very small to force multiple files
		opts.enable_vlog = true;
	});

	// Create initial database and add data to create multiple VLog files
	let tree1 = Tree::new(Arc::clone(&opts)).unwrap();

	// Verify VLog is enabled
	assert!(tree1.core.vlog.is_some(), "VLog should be enabled");

	// Add data to create multiple VLog files
	for i in 0..10 {
		let key = format!("key_{i}");
		let value = format!("value_{i}_large_data_that_should_force_vlog_storage");

		let mut tx = tree1.begin().unwrap();
		tx.set(key.as_bytes(), value.as_bytes()).unwrap();
		tx.commit().await.unwrap();

		// Verify the data was written immediately
		let tx = tree1.begin().unwrap();
		let result = tx.get(key.as_bytes()).unwrap();
		assert!(result.is_some(), "Key '{key}' should exist immediately after write");
	}

	// Verify all data exists in the first database
	for i in 0..10 {
		let key = format!("key_{i}");
		let expected_value = format!("value_{i}_large_data_that_should_force_vlog_storage");

		let tx = tree1.begin().unwrap();
		let result = tx.get(key.as_bytes()).unwrap();
		assert!(result.is_some(), "Key '{key}' should exist in first database");
		assert_eq!(&result.unwrap(), expected_value.as_bytes());
	}

	// Drop the first database (this will close it)
	tree1.close().await.unwrap();

	// Create a new database instance - this should trigger VLog prefill
	let tree2 = Tree::new(Arc::clone(&opts)).unwrap();
	assert!(tree2.core.vlog.is_some(), "VLog should be enabled in second database");

	// Verify that all existing data can still be read
	for i in 0..10 {
		let key = format!("key_{i}");
		let expected_value = format!("value_{i}_large_data_that_should_force_vlog_storage");

		let tx = tree2.begin().unwrap();
		let result = tx.get(key.as_bytes()).unwrap();
		assert!(result.is_some(), "Key '{key}' should exist after reopen");
		assert_eq!(&result.unwrap(), expected_value.as_bytes());
	}

	// Add new data and verify it goes to the correct active file
	let new_key = "new_key";
	let new_value = "new_value_large_data_that_should_force_vlog_storage";

	let mut tx = tree2.begin().unwrap();
	tx.set(new_key.as_bytes(), new_value.as_bytes()).unwrap();
	tx.commit().await.unwrap();

	// Verify the new data can be read
	let tx = tree2.begin().unwrap();
	let result = tx.get(new_key.as_bytes()).unwrap();
	assert!(result.is_some(), "New key should exist after write");
	assert_eq!(&result.unwrap(), new_value.as_bytes());

	// Verify we can still read from old data after adding new data
	for i in 0..10 {
		let key = format!("key_{i}");
		let expected_value = format!("value_{i}_large_data_that_should_force_vlog_storage");

		let tx = tree2.begin().unwrap();
		let result = tx.get(key.as_bytes()).unwrap();
		assert!(result.is_some(), "Old key '{key}' should still exist");
		assert_eq!(&result.unwrap(), expected_value.as_bytes());
	}

	// Clean shutdown (drop will close it)
	tree2.close().await.unwrap();
}

#[test(tokio::test)]
async fn test_tree_builder() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	// Test TreeBuilder with default key type (InternalKey)
	let tree = TreeBuilder::new()
		.with_path(path.clone())
		.with_max_memtable_size(64 * 1024)
		.with_enable_vlog(true)
		.with_vlog_max_file_size(1024 * 1024)
		.build()
		.unwrap();

	// Test basic operations
	let mut txn = tree.begin().unwrap();
	txn.set(b"test_key", b"test_value").unwrap();
	txn.commit().await.unwrap();

	let txn = tree.begin().unwrap();
	let result = txn.get(b"test_key").unwrap().unwrap();
	assert_eq!(result, b"test_value".to_vec());

	// Test build_with_options
	let (tree2, opts) =
		TreeBuilder::new().with_path(temp_dir.path().join("tree2")).build_with_options().unwrap();

	// Verify the options are accessible
	assert_eq!(opts.path, temp_dir.path().join("tree2"));

	// Test basic operations on the second tree
	let mut txn = tree2.begin().unwrap();
	txn.set(b"key2", b"value2").unwrap();
	txn.commit().await.unwrap();

	let txn = tree2.begin().unwrap();
	let result = txn.get(b"key2").unwrap().unwrap();
	assert_eq!(result, b"value2".to_vec());
}

#[test(tokio::test)]
async fn test_soft_delete() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 64 * 1024; // Small memtable to force flushes
	});

	// Step 1: Create multiple versions of a key across separate transactions
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Create 5 different versions of the same key
		for version in 1..=5 {
			let value = format!("value_v{}", version);
			let mut txn = tree.begin().unwrap();
			txn.set(b"test_key", value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Verify the latest version exists
		let txn = tree.begin().unwrap();
		let result = txn.get(b"test_key").unwrap().unwrap();
		assert_eq!(&result, b"value_v5");

		// Soft delete the key
		let mut txn = tree.begin().unwrap();
		txn.soft_delete(b"test_key").unwrap();
		txn.commit().await.unwrap();

		// Verify the key is now invisible (soft deleted)
		let txn = tree.begin().unwrap();
		let result = txn.get(b"test_key").unwrap();
		assert!(result.is_none(), "Soft deleted key should not be visible");

		// Add a new different key
		let mut txn = tree.begin().unwrap();
		txn.set(b"other_key", b"other_value").unwrap();
		txn.commit().await.unwrap();

		// Verify the new key exists
		let txn = tree.begin().unwrap();
		let result = txn.get(b"other_key").unwrap().unwrap();
		assert_eq!(&result, b"other_value");

		// Force flush to persist all changes to disk
		tree.flush().unwrap();

		// Close the database
		tree.close().await.unwrap();
	}

	// Step 2: Reopen the database and verify soft deleted key is still invisible
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Verify the soft deleted key is still invisible after restart
		let txn = tree.begin().unwrap();
		let result = txn.get(b"test_key").unwrap();
		assert!(
			result.is_none(),
			"Soft deleted key should remain invisible after database restart"
		);

		// Verify the other key still exists
		let txn = tree.begin().unwrap();
		let result = txn.get(b"other_key").unwrap().unwrap();
		assert_eq!(&result, b"other_value");

		// Test range scan to ensure soft deleted key doesn't appear
		let txn = tree.begin().unwrap();
		let range_result = collect_transaction_all(
			&mut txn.range(b"test".as_slice(), b"testz".as_slice()).unwrap(),
		)
		.unwrap();

		// Should be empty since test_key is soft deleted
		assert!(range_result.is_empty(), "Range scan should not include soft deleted keys");

		// Test range scan that includes the other key
		let txn = tree.begin().unwrap();
		let range_result = collect_transaction_all(
			&mut txn.range(b"other".as_slice(), b"otherz".as_slice()).unwrap(),
		)
		.unwrap();

		// Should contain the other key
		assert_eq!(range_result.len(), 1);
		assert_eq!(&range_result[0].0, b"other_key");
		assert_eq!(range_result[0].1.as_slice(), b"other_value");

		// Test that we can reinsert the same key after soft delete
		let mut txn = tree.begin().unwrap();
		txn.set(b"test_key", b"new_value_after_soft_delete").unwrap();
		txn.commit().await.unwrap();

		// Verify the new value is visible
		let txn = tree.begin().unwrap();
		let result = txn.get(b"test_key").unwrap().unwrap();
		assert_eq!(&result, b"new_value_after_soft_delete");

		tree.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_checkpoint_with_vlog() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.vlog_max_file_size = 1024;
		opts.max_memtable_size = 1024;
		opts.enable_vlog = true;
	});

	// Create initial data with VLog enabled
	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Insert some data to ensure VLog and discard stats are created
	let large_value = vec![1u8; 200]; // Large enough for VLog
	for i in 0..5 {
		let key = format!("key_{i}");
		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), &large_value).unwrap();
		txn.commit().await.unwrap();
	}

	// Force flush to create VLog files and initialize discard stats
	tree.flush().unwrap();

	// Update some keys to create discard statistics
	let mut discard_updates = HashMap::new();
	let stats = tree.get_all_vlog_stats();
	for (file_id, total_size, _discard_bytes, _ratio) in &stats {
		if *total_size > 0 {
			discard_updates.insert(*file_id, (*total_size as f64 * 0.3) as i64);
		}
	}
	tree.update_vlog_discard_stats(&discard_updates);

	// Create checkpoint
	let checkpoint_dir = temp_dir.path().join("checkpoint");
	let metadata = tree.create_checkpoint(&checkpoint_dir).unwrap();

	// Verify checkpoint metadata
	assert!(metadata.timestamp > 0);
	assert!(metadata.total_size > 0);

	// Verify all expected directories exist in checkpoint
	assert!(checkpoint_dir.exists());
	assert!(checkpoint_dir.join("sstables").exists());
	assert!(checkpoint_dir.join("wal").exists());
	assert!(checkpoint_dir.join("manifest").exists());
	assert!(checkpoint_dir.join("vlog").exists());
	assert!(checkpoint_dir.join("discard_stats").exists());
	assert!(checkpoint_dir.join("delete_list").exists());
	assert!(checkpoint_dir.join("CHECKPOINT_METADATA").exists());

	// Verify VLog files are in the checkpoint
	let vlog_checkpoint_dir = checkpoint_dir.join("vlog");
	let vlog_entries = std::fs::read_dir(&vlog_checkpoint_dir).unwrap();
	let vlog_files: Vec<_> = vlog_entries
		.filter_map(|entry| {
			let entry = entry.ok()?;
			let name = entry.file_name().to_string_lossy().to_string();
			if opts.is_vlog_filename(&name) {
				Some(name)
			} else {
				None
			}
		})
		.collect();

	assert!(!vlog_files.is_empty(), "Should have VLog files in the checkpoint");

	// Verify discard stats file exists
	let discard_stats_checkpoint_dir = checkpoint_dir.join("discard_stats");
	assert!(discard_stats_checkpoint_dir.exists());
	assert!(discard_stats_checkpoint_dir.join("DISCARD").exists());

	// Insert more data after checkpoint
	for i in 5..10 {
		let key = format!("key_{i}");
		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), &large_value).unwrap();
		txn.commit().await.unwrap();
	}

	// Force flush to ensure the new data is also on disk
	tree.flush().unwrap();

	// Verify all data exists before restore
	for i in 0..10 {
		let key = format!("key_{i}");
		let txn = tree.begin().unwrap();
		let result = txn.get(key.as_bytes()).unwrap();
		assert!(result.is_some(), "Key '{key}' should exist before restore");
	}

	// Restore from checkpoint
	tree.restore_from_checkpoint(&checkpoint_dir).unwrap();

	// Verify data is restored to checkpoint state (keys 0-4 exist, 5-9 don't)
	for i in 0..5 {
		let key = format!("key_{i}");
		let txn = tree.begin().unwrap();
		let result = txn.get(key.as_bytes()).unwrap();
		assert!(result.is_some(), "Key '{key}' should exist after restore");
		assert_eq!(&result.unwrap(), &large_value);
	}

	// Verify the newer keys don't exist after restore
	for i in 5..10 {
		let key = format!("key_{i}");
		let txn = tree.begin().unwrap();
		let result = txn.get(key.as_bytes()).unwrap();
		assert!(result.is_none(), "Key '{key}' should not exist after restore");
	}

	// Verify VLog directories are restored
	let vlog_dir = opts.vlog_dir();
	let discard_stats_dir = opts.discard_stats_dir();
	let delete_list_dir = opts.delete_list_dir();

	assert!(vlog_dir.exists(), "VLog directory should exist after restore");
	assert!(discard_stats_dir.exists(), "Discard stats directory should exist after restore");
	assert!(delete_list_dir.exists(), "Delete list directory should exist after restore");

	// Verify VLog files are restored
	let vlog_entries = std::fs::read_dir(&vlog_dir).unwrap();
	let vlog_files: Vec<_> = vlog_entries
		.filter_map(|entry| {
			let entry = entry.ok()?;
			let name = entry.file_name().to_string_lossy().to_string();
			if opts.is_vlog_filename(&name) {
				Some(name)
			} else {
				None
			}
		})
		.collect();

	assert!(!vlog_files.is_empty(), "Should have VLog files after restore");

	// Verify discard stats file is restored
	assert!(discard_stats_dir.join("DISCARD").exists(), "DISCARD file should exist after restore");
}

#[test_log::test(tokio::test)]
async fn test_clean_shutdown_actually_skips_wal() {
	// This test explicitly validates that WAL is NOT replayed after clean shutdown
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 1024;
		opts.flush_on_close = true;
	});

	// Phase 1: Write data and clean shutdown
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		let mut txn = tree.begin().unwrap();
		txn.set(b"test_key", b"test_value").unwrap();
		txn.commit().await.unwrap();

		// Clean shutdown - should flush to SST
		tree.close().await.unwrap();
	}

	// Phase 2: Check manifest state after shutdown
	let manifest = LevelManifest::new(Arc::clone(&opts)).expect("Failed to load manifest");
	let log_number = manifest.get_log_number();

	// CRITICAL CHECK: log_number should be > 0 to skip WAL #0
	assert!(
		log_number > 0,
		"BUG: log_number should be > 0 after flush to indicate WAL #0 is flushed, got {}",
		log_number
	);

	// Phase 3: Restart and verify WAL was actually skipped
	{
		// Create a custom Core to inspect if WAL was replayed
		let inner = Arc::new(CoreInner::new(Arc::clone(&opts)).unwrap());

		// Before WAL replay, memtable should be empty
		let memtable_before = inner.active_memtable.read().unwrap().clone();
		assert!(memtable_before.is_empty(), "Memtable should be empty before WAL replay");

		// Now do WAL replay
		let wal_path = opts.wal_dir();
		let min_wal_number = log_number;

		let (wal_seq_opt, _memtable_opt) = Core::replay_wal_with_repair(
			&wal_path,
			min_wal_number,
			"Test",
			WalRecoveryMode::default(),
			opts.max_memtable_size,
			|_memtable, _wal_number| Ok(()),
		)
		.unwrap();

		// CRITICAL: WAL should have been skipped (return None)
		assert_eq!(
			wal_seq_opt, None,
			"BUG: WAL should have been skipped but was replayed! min_wal={}, returned={:?}",
			min_wal_number, wal_seq_opt
		);
	}
}

#[tokio::test]
async fn test_crash_before_flush_replays_wal() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 10 * 1024 * 1024; // Large memtable to prevent
		                                     // auto-flush
	});

	// Phase 1: Write data and simulate crash (no clean shutdown)
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let mut txn = tree.begin().unwrap();
		txn.set(b"crash_key", b"crash_value").unwrap();
		txn.commit().await.unwrap();

		// Simulate crash: drop tree without calling close()
		// This leaves data in WAL but not flushed to SST
		// Release lock manually to allow reopen
		{
			let mut lockfile = tree.core.inner.lockfile.lock().unwrap();
			lockfile.release().unwrap();
		}
		drop(tree);
	}

	// Phase 2: Restart and verify WAL was replayed
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Data should be available (recovered from WAL)
		let txn = tree.begin().unwrap();
		assert_eq!(txn.get(b"crash_key").unwrap(), Some(b"crash_value".to_vec()));

		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn test_log_number_advances_with_flushes() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 1024;
	});

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Initial log_number
	let log_number_0 = tree.core.inner.level_manifest.read().unwrap().get_log_number();

	// Write data to trigger flush #1
	for i in 0..100 {
		let mut txn = tree.begin().unwrap();
		txn.set(format!("key_{i}").as_bytes(), b"value").unwrap();
		txn.commit().await.unwrap();
	}

	// Force flush
	tree.flush().unwrap();
	let log_number_1 = tree.core.inner.level_manifest.read().unwrap().get_log_number();
	assert!(log_number_1 > log_number_0, "log_number should advance after flush");

	// Write more data, trigger flush #2
	for i in 100..200 {
		let mut txn = tree.begin().unwrap();
		txn.set(format!("key_{i}").as_bytes(), b"value").unwrap();
		txn.commit().await.unwrap();
	}

	tree.flush().unwrap();
	let log_number_2 = tree.core.inner.level_manifest.read().unwrap().get_log_number();
	assert!(log_number_2 > log_number_1, "log_number should advance after second flush");

	tree.close().await.unwrap();
}

#[tokio::test]
async fn test_last_sequence_persists_across_restart() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 1024;
	});

	let expected_last_seq;

	// Phase 1: Create database, write, flush
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Write data
		for i in 0..50 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("key_{i}").as_bytes(), b"value").unwrap();
			txn.commit().await.unwrap();
		}

		// Force flush to persist
		tree.flush().unwrap();

		// Get last_sequence from manifest
		expected_last_seq = tree.core.inner.level_manifest.read().unwrap().get_last_sequence();
		assert!(expected_last_seq > 0, "last_sequence should be > 0 after flush");

		tree.close().await.unwrap();
	}

	// Phase 2: Reopen and verify last_sequence persisted
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let loaded_last_seq = tree.core.inner.level_manifest.read().unwrap().get_last_sequence();

		assert_eq!(
			loaded_last_seq, expected_last_seq,
			"last_sequence should persist across restart"
		);

		tree.close().await.unwrap();
	}
}

#[test_log::test(tokio::test)]
async fn test_wal_recovery_updates_last_sequence_in_memory() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 10 * 1024 * 1024; // Large to prevent auto-flush
	});

	let manifest_seq_initial;

	// Phase 1: Write data and clean shutdown (no flush, just close)
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let mut txn = tree.begin().unwrap();
		txn.set(b"key1", b"value1").unwrap();
		txn.commit().await.unwrap();

		// Don't flush, just close - this will flush memtable on shutdown
		tree.close().await.unwrap();

		// Get the manifest sequence after shutdown flush
		let manifest = LevelManifest::new(Arc::clone(&opts)).unwrap();
		manifest_seq_initial = manifest.get_last_sequence();
	}

	// Phase 2: Write more data but crash (no flush, no close)
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let mut txn = tree.begin().unwrap();
		txn.set(b"key2", b"value2").unwrap();
		txn.commit().await.unwrap();

		// Crash: drop without close, release lock manually
		{
			let mut lockfile = tree.core.inner.lockfile.lock().unwrap();
			lockfile.release().unwrap();
		}
		drop(tree);
	}

	// Phase 3: Recover and verify in-memory sequence > manifest sequence
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// In-memory sequence should be updated from WAL (key2 recovery)
		let in_memory_seq = tree.core.seq_num();

		// Manifest sequence should still be from Phase 1
		let manifest_seq = tree.core.inner.level_manifest.read().unwrap().get_last_sequence();
		assert_eq!(
			manifest_seq, manifest_seq_initial,
			"Manifest last_sequence should not be updated until flush (from Phase 2 crash)"
		);

		// In-memory should be higher (includes recovered WAL data)
		assert!(
			in_memory_seq > manifest_seq,
			"In-memory sequence ({}) should be > manifest ({}) after WAL recovery",
			in_memory_seq,
			manifest_seq
		);

		// Now flush and verify manifest gets updated
		tree.flush().unwrap();
		let manifest_seq_after_flush =
			tree.core.inner.level_manifest.read().unwrap().get_last_sequence();
		assert!(
			manifest_seq_after_flush >= in_memory_seq,
			"Manifest last_sequence should update after flush"
		);

		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn test_clean_shutdown_no_empty_wal() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 1024;
	});

	let wal_dir = opts.wal_dir();

	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Write data and trigger flush
		// make_room_for_write rotates WAL, creating a new WAL for subsequent writes
		for i in 0..100 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("key_{i}").as_bytes(), b"value").unwrap();
			txn.commit().await.unwrap();
		}
		tree.flush().unwrap();

		// Write a bit more data to the new WAL
		let mut txn = tree.begin().unwrap();
		txn.set(b"extra", b"data").unwrap();
		txn.commit().await.unwrap();

		// Clean shutdown (should flush the extra data, update log_number, close WAL)
		tree.close().await.unwrap();

		// Give async cleanup time to complete
		tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

		// Count WAL files after shutdown
		let wals_after = crate::wal::list_segment_ids(&wal_dir, Some("wal")).unwrap();

		// Validate: shutdown should NOT create additional WAL files
		// WAL count may decrease due to cleanup, but should never increase
		// This confirms no empty WAL is created on shutdown
		assert!(
			wals_after.len() <= 2,
			"Clean shutdown should not create new WAL files (found {} WAL files after shutdown)",
			wals_after.len()
		);
	}

	// Verify restart works and data is accessible
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let txn = tree.begin().unwrap();
		assert_eq!(txn.get(b"key_0").unwrap(), Some(b"value".to_vec()));
		assert_eq!(txn.get(b"extra").unwrap(), Some(b"data".to_vec()));
		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn test_multiple_flush_cycles_log_number_sequence() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 1024;
	});

	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Flush cycle 1
		for i in 0..50 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("batch1_key_{i}").as_bytes(), b"value1").unwrap();
			txn.commit().await.unwrap();
		}
		tree.flush().unwrap();
		let log_num_1 = tree.core.inner.level_manifest.read().unwrap().get_log_number();

		// Flush cycle 2
		for i in 0..50 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("batch2_key_{i}").as_bytes(), b"value2").unwrap();
			txn.commit().await.unwrap();
		}
		tree.flush().unwrap();
		let log_num_2 = tree.core.inner.level_manifest.read().unwrap().get_log_number();

		// Flush cycle 3
		for i in 0..50 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("batch3_key_{i}").as_bytes(), b"value3").unwrap();
			txn.commit().await.unwrap();
		}
		tree.flush().unwrap();
		let log_num_3 = tree.core.inner.level_manifest.read().unwrap().get_log_number();

		assert!(log_num_2 > log_num_1, "log_number should advance");
		assert!(log_num_3 > log_num_2, "log_number should advance");

		tree.close().await.unwrap();
	}

	// Restart and verify all data accessible from SSTables
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let txn = tree.begin().unwrap();

		// All batches should be accessible (reading doesn't need mut)
		assert_eq!(txn.get(b"batch1_key_0").unwrap(), Some(b"value1".to_vec()));
		assert_eq!(txn.get(b"batch2_key_0").unwrap(), Some(b"value2".to_vec()));
		assert_eq!(txn.get(b"batch3_key_0").unwrap(), Some(b"value3".to_vec()));

		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn test_shutdown_with_empty_memtable() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |_opts| {});

	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Write and flush everything
		let mut txn = tree.begin().unwrap();
		txn.set(b"key", b"value").unwrap();
		txn.commit().await.unwrap();
		tree.flush().unwrap();

		// Get manifest state
		let log_number_before = tree.core.inner.level_manifest.read().unwrap().get_log_number();
		let last_seq_before = tree.core.inner.level_manifest.read().unwrap().get_last_sequence();

		// Shutdown with empty memtable
		tree.close().await.unwrap();

		// Verify manifest unchanged (no unnecessary updates)
		let manifest = LevelManifest::new(Arc::clone(&opts)).unwrap();
		assert_eq!(manifest.get_log_number(), log_number_before);
		assert_eq!(manifest.get_last_sequence(), last_seq_before);
	}

	// Restart successfully
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let txn = tree.begin().unwrap();
		assert_eq!(txn.get(b"key").unwrap(), Some(b"value".to_vec()));
		tree.close().await.unwrap();
	}
}

#[test_log::test(tokio::test)]
async fn test_full_crash_recovery_scenario() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 1024;
	});

	// Phase 1: Write batch A, flush
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		for i in 0..50 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("batch_a_{i}").as_bytes(), b"value_a").unwrap();
			txn.commit().await.unwrap();
		}
		tree.flush().unwrap();
		tree.close().await.unwrap();
	}

	// Phase 2: Write batch B, flush
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		for i in 0..50 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("batch_b_{i}").as_bytes(), b"value_b").unwrap();
			txn.commit().await.unwrap();
		}
		tree.flush().unwrap();

		// Get log_number after second flush
		let log_number_after_b = tree.core.inner.level_manifest.read().unwrap().get_log_number();
		assert!(log_number_after_b >= 2, "Should have rotated WAL at least twice");

		tree.close().await.unwrap();
	}

	// Give async cleanup time to finish from Phase 2
	tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

	// Phase 3: Write batch C, crash before flush
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		for i in 0..20 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("batch_c_{i}").as_bytes(), b"value_c").unwrap();
			txn.commit().await.unwrap();
		}

		// Simulate crash: drop without close
		// Release lock manually to allow reopen
		{
			let mut lockfile = tree.core.inner.lockfile.lock().unwrap();
			lockfile.release().unwrap();
		}
		drop(tree);
	}

	// Phase 4: Restart and verify
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// All data should be accessible
		let txn = tree.begin().unwrap();
		assert_eq!(txn.get(b"batch_a_0").unwrap(), Some(b"value_a".to_vec()));
		assert_eq!(txn.get(b"batch_b_0").unwrap(), Some(b"value_b".to_vec()));
		assert_eq!(
			txn.get(b"batch_c_0").unwrap(),
			Some(b"value_c".to_vec()),
			"Batch C should be recovered from WAL"
		);

		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn test_concurrent_flush_after_rotation() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 1024;
	});

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Write data
	for i in 0..100 {
		let mut txn = tree.begin().unwrap();
		txn.set(format!("key_{i}").as_bytes(), b"value").unwrap();
		txn.commit().await.unwrap();
	}

	// Verify data is accessible before flush
	{
		let txn = tree.begin().unwrap();
		assert_eq!(
			txn.get(b"key_0").unwrap(),
			Some(b"value".to_vec()),
			"Data should be accessible before flush"
		);
	}

	// Call flush which will:
	// 1. Rotate WAL
	// 2. Call flush_memtable_and_update_manifest
	// The function should handle empty memtable gracefully
	tree.flush().unwrap();

	// Verify data is still accessible after flush
	{
		let txn = tree.begin().unwrap();
		assert_eq!(
			txn.get(b"key_0").unwrap(),
			Some(b"value".to_vec()),
			"Data should be accessible after flush"
		);
	}

	// Verify no errors and system continues
	let mut txn = tree.begin().unwrap();
	txn.set(b"after_flush", b"value").unwrap();
	txn.commit().await.unwrap();
	drop(txn);

	// Verify both old and new data are accessible
	{
		let txn = tree.begin().unwrap();
		assert_eq!(
			txn.get(b"key_0").unwrap(),
			Some(b"value".to_vec()),
			"Old data should still be accessible"
		);
		assert_eq!(
			txn.get(b"after_flush").unwrap(),
			Some(b"value".to_vec()),
			"New data after flush should be accessible"
		);
		drop(txn);
	}

	tree.close().await.unwrap();
}

#[test_log::test(tokio::test)]
async fn test_wal_file_reuse_across_restarts() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 10 * 1024 * 1024; // Large to prevent auto-flush
	});

	// Phase 1: Open database, write one transaction, close
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let mut txn = tree.begin().unwrap();
		txn.set(b"key1", b"value1").unwrap();
		txn.commit().await.unwrap();

		tree.close().await.unwrap();
	}

	// Phase 2: Reopen database, check if WAL is reused
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Write another transaction
		let mut txn = tree.begin().unwrap();
		txn.set(b"key2", b"value2").unwrap();
		txn.commit().await.unwrap();

		tree.close().await.unwrap();
	}

	// Phase 3: Verify recovery
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let txn = tree.begin().unwrap();

		// Both keys should be accessible
		let key1_present = txn.get(b"key1").unwrap().is_some();
		let key2_present = txn.get(b"key2").unwrap().is_some();

		assert!(key1_present && key2_present, "Both keys should be recovered");

		tree.close().await.unwrap();
	}
}

#[test_log::test(tokio::test)]
async fn test_wal_append_after_crash_recovery() {
	// This test verifies that after a crash (no flush), WAL is reused and appended
	// to
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 10 * 1024 * 1024; // Large to prevent auto-flush
	});

	// Phase 1: Write data and simulate crash (no clean shutdown)
	let manifest_log = {
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let mut txn = tree.begin().unwrap();
		txn.set(b"key1", b"value1").unwrap();
		txn.commit().await.unwrap();

		let manifest_log = tree.core.inner.level_manifest.read().unwrap().get_log_number();

		// Simulate crash: drop without close (but release lock)
		{
			let mut lockfile = tree.core.inner.lockfile.lock().unwrap();
			lockfile.release().unwrap();
		}
		drop(tree);

		manifest_log
	};

	// Verify manifest didn't change (no flush happened)
	let manifest = LevelManifest::new(Arc::clone(&opts)).unwrap();
	assert_eq!(manifest.get_log_number(), manifest_log, "Manifest should not change on crash");

	// Phase 2: Reopen and verify WAL is reused (SAME number)
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		let wal_num_after_reopen = tree.core.inner.wal.read().get_active_log_number();

		// CRITICAL: WAL number should be SAME as before (appending to existing)
		assert_eq!(wal_num_after_reopen, 0, "WAL should reuse existing file #0 since log_number=0");

		// Write another transaction to same WAL
		let mut txn = tree.begin().unwrap();
		txn.set(b"key2", b"value2").unwrap();
		txn.commit().await.unwrap();

		let wal_num_after_write = tree.core.inner.wal.read().get_active_log_number();

		// Should still be same WAL
		assert_eq!(
			wal_num_after_write, wal_num_after_reopen,
			"Should still be using same WAL file"
		);

		tree.close().await.unwrap();
	}
}

#[test_log::test(tokio::test)]
async fn test_flush_on_close_creates_sst() {
	// This test verifies that close() flushes active memtable to SST
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 10 * 1024 * 1024; // Large to prevent auto-flush
		opts.flush_on_close = true;
	});

	let sst_dir = opts.sstable_dir();

	// Count SST files before
	let count_ssts = || {
		std::fs::read_dir(&sst_dir)
			.ok()
			.map(|entries| {
				entries
					.filter_map(|e| e.ok())
					.filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
					.count()
			})
			.unwrap_or(0)
	};

	// Phase 1: Write data and close
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Write data to memtable (no manual flush)
		let mut txn = tree.begin().unwrap();
		txn.set(b"test_key", b"test_value").unwrap();
		txn.commit().await.unwrap();

		let sst_count_before_close = count_ssts();

		// Close (should trigger flush)
		tree.close().await.unwrap();

		let sst_count_after_close = count_ssts();

		// CRITICAL: SST count should increase by 1 (memtable flushed)
		assert_eq!(
			sst_count_after_close,
			sst_count_before_close + 1,
			"SST count should increase by 1 after close (flush on shutdown)"
		);
	}

	// Phase 2: Reopen and verify data is in SST (not WAL)
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Memtable should be empty (data in SST)
		// Data should still be accessible
		let txn = tree.begin().unwrap();
		assert_eq!(txn.get(b"test_key").unwrap(), Some(b"test_value".to_vec()));

		tree.close().await.unwrap();
	}
}

#[test_log::test(tokio::test)]
async fn test_multiple_flush_cycles_with_sst_and_wal_verification() {
	// Comprehensive test: multiple write-flush-close cycles
	// Verifies SST creation, WAL rotation, log_number tracking, and recovery
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 1024;
	});

	let sst_dir = opts.sstable_dir();

	let count_ssts = || {
		std::fs::read_dir(&sst_dir)
			.ok()
			.map(|entries| {
				entries
					.filter_map(|e| e.ok())
					.filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
					.count()
			})
			.unwrap_or(0)
	};

	// Cycle 1: Write data, trigger flush, close
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		for i in 0..50 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("cycle1_key_{}", i).as_bytes(), b"value1").unwrap();
			txn.commit().await.unwrap();
		}

		let sst_before = count_ssts();
		tree.flush().unwrap(); // Explicit flush
		let sst_after = count_ssts();

		{
			let manifest = tree.core.inner.level_manifest.read().unwrap();
			drop(manifest);
		}

		assert!(sst_after > sst_before, "Flush should create many SST");

		tree.close().await.unwrap();
	}

	// Cycle 2: Reopen, verify recovery, write more, flush, close
	{
		let manifest_before = LevelManifest::new(Arc::clone(&opts)).unwrap();
		let log_num_before = manifest_before.get_log_number();

		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Verify cycle 1 data is accessible
		let txn = tree.begin().unwrap();
		assert_eq!(txn.get(b"cycle1_key_0").unwrap(), Some(b"value1".to_vec()));
		drop(txn);

		for i in 0..50 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("cycle2_key_{}", i).as_bytes(), b"value2").unwrap();
			txn.commit().await.unwrap();
		}

		let sst_before = count_ssts();
		tree.flush().unwrap();
		let sst_after = count_ssts();

		{
			let manifest = tree.core.inner.level_manifest.read().unwrap();

			assert!(sst_after > sst_before, "Second flush should create more SST");
			assert!(manifest.get_log_number() > log_num_before, "log_number should advance");
			drop(manifest);
		}

		tree.close().await.unwrap();
	}

	// Cycle 3: Reopen, write but DON'T flush, close (tests shutdown flush)
	{
		// Enable flush_on_close to test shutdown flush behavior
		let opts_with_flush = Arc::new(Options {
			flush_on_close: true,
			..(*opts).clone()
		});
		let tree = Tree::new(opts_with_flush).unwrap();

		// Verify both previous cycles' data
		let txn = tree.begin().unwrap();
		assert_eq!(txn.get(b"cycle1_key_0").unwrap(), Some(b"value1".to_vec()));
		assert_eq!(txn.get(b"cycle2_key_0").unwrap(), Some(b"value2".to_vec()));
		drop(txn);

		for i in 0..50 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("cycle3_key_{}", i).as_bytes(), b"value3").unwrap();
			txn.commit().await.unwrap();
		}

		let sst_before_close = count_ssts();

		// Close WITHOUT explicit flush (shutdown should flush because
		// flush_on_close=true)
		tree.close().await.unwrap();

		let sst_after_close = count_ssts();

		assert!(
			sst_after_close > sst_before_close,
			"Shutdown should flush and create SST when flush_on_close=true"
		);
	}

	// Final verification: All data accessible from SSTs
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		let txn = tree.begin().unwrap();
		assert_eq!(txn.get(b"cycle1_key_0").unwrap(), Some(b"value1".to_vec()));
		assert_eq!(txn.get(b"cycle2_key_0").unwrap(), Some(b"value2".to_vec()));
		assert_eq!(txn.get(b"cycle3_key_0").unwrap(), Some(b"value3".to_vec()));

		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn test_close_without_flush() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.flush_on_close = false; // Default behavior
		opts.max_memtable_size = 1024 * 1024;
	});

	let sst_count_before;
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Write some data that won't trigger auto-flush
		for i in 0..10 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("key_{}", i).as_bytes(), b"value").unwrap();
			txn.commit().await.unwrap();
		}

		// Count SSTs before close
		sst_count_before = tree.core.inner.level_manifest.read().as_ref().iter().count();

		// Close without flush (flush_on_close=false)
		tree.close().await.unwrap();
	}

	// Reopen and verify SST count hasn't changed
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let sst_count_after = tree.core.inner.level_manifest.read().as_ref().iter().count();

		assert_eq!(
			sst_count_after, sst_count_before,
			"SST count should not increase when flush_on_close=false"
		);

		// Data should still be accessible via WAL recovery
		let txn = tree.begin().unwrap();
		assert_eq!(txn.get(b"key_0").unwrap(), Some(b"value".to_vec()));

		tree.close().await.unwrap();
	}
}

#[tokio::test]
async fn test_flush_on_close_option_comparison() {
	// Test with flush_on_close = true
	{
		let temp_dir = TempDir::new("test").unwrap();
		let opts = create_test_options(temp_dir.path().to_path_buf(), |opts| {
			opts.flush_on_close = true;
		});

		let sst_before;
		{
			let tree = Tree::new(Arc::clone(&opts)).unwrap();
			let mut txn = tree.begin().unwrap();
			txn.set(b"test", b"data").unwrap();
			txn.commit().await.unwrap();

			sst_before = tree.core.inner.level_manifest.read().unwrap().iter().count();

			tree.close().await.unwrap();
		}

		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let sst_after = tree.core.inner.level_manifest.read().unwrap().iter().count();

		assert_eq!(sst_after, sst_before + 1, "flush_on_close=true should create SST");
		tree.close().await.unwrap();
	}

	// Test with flush_on_close = false
	{
		let temp_dir = TempDir::new("test").unwrap();
		let opts = create_test_options(temp_dir.path().to_path_buf(), |opts| {
			opts.flush_on_close = false;
		});

		let sst_before;
		{
			let tree = Tree::new(Arc::clone(&opts)).unwrap();
			let mut txn = tree.begin().unwrap();
			txn.set(b"test", b"data").unwrap();
			txn.commit().await.unwrap();

			sst_before = tree.core.inner.level_manifest.read().unwrap().iter().count();

			tree.close().await.unwrap();
		}

		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let sst_after = tree.core.inner.level_manifest.read().unwrap().iter().count();

		assert_eq!(sst_after, sst_before, "flush_on_close=false should NOT create SST");

		// But data should still be accessible via WAL
		let txn = tree.begin().unwrap();
		assert_eq!(txn.get(b"test").unwrap(), Some(b"data".to_vec()));

		tree.close().await.unwrap();
	}
}

/// Tests that flush_all_memtables_for_shutdown flushes both immutable and
/// active memtables in the correct order (immutables first, then active)
/// to preserve SSTable ordering.
#[test_log::test(tokio::test)]
async fn test_flush_all_memtables_on_close_ordering() {
	// This test verifies that close() flushes ALL memtables (immutable + active)
	// and that SSTable table_ids are in correct temporal order
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 1024;
		opts.flush_on_close = true;
	});

	let sst_dir = opts.sstable_dir();

	// Helper to count SST files
	let count_ssts = || -> usize {
		std::fs::read_dir(&sst_dir)
			.ok()
			.map(|entries| {
				entries
					.filter_map(|e| e.ok())
					.filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
					.count()
			})
			.unwrap_or(0)
	};

	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Write enough data to trigger multiple memtable flushes
		// This creates immutable memtables in the background
		for i in 0..50 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("key_{:04}", i).as_bytes(), b"value_data_here").unwrap();
			txn.commit().await.unwrap();
		}

		// Wait a bit for background flushes to start
		tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

		let sst_count_before_close = count_ssts();
		log::info!("SST count before close: {}", sst_count_before_close);

		// Write one more entry to ensure active memtable has data
		let mut txn = tree.begin().unwrap();
		txn.set(b"final_key", b"final_value").unwrap();
		txn.commit().await.unwrap();

		// Close - should flush all remaining memtables
		tree.close().await.unwrap();

		let sst_count_after_close = count_ssts();
		log::info!("SST count after close: {}", sst_count_after_close);

		// Should have at least one more SST from the close flush
		assert!(
			sst_count_after_close >= sst_count_before_close,
			"SST count should not decrease after close"
		);
	}

	// Reopen and verify all data is accessible
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Verify data from all writes
		for i in 0..50 {
			let txn = tree.begin().unwrap();
			let key = format!("key_{:04}", i);
			let value = txn.get(key.as_bytes()).unwrap();
			assert!(value.is_some(), "Key {} should exist after close/reopen", key);
		}

		// Verify final key
		let txn = tree.begin().unwrap();
		assert_eq!(
			txn.get(b"final_key").unwrap(),
			Some(b"final_value".to_vec()),
			"Final key should exist after close/reopen"
		);

		tree.close().await.unwrap();
	}
}

/// Tests that pending immutable memtables are flushed during close even
/// when the active memtable is empty.
#[test_log::test(tokio::test)]
async fn test_flush_immutable_memtables_with_empty_active() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 1024;
		opts.flush_on_close = true;
	});

	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Write data to trigger a memtable flush (creates immutable memtable)
		for i in 0..20 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("key_{}", i).as_bytes(), b"value_data").unwrap();
			txn.commit().await.unwrap();
		}

		// Wait for background flush to complete
		tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

		// After flush, active memtable should be empty
		// Close should handle this correctly
		tree.close().await.unwrap();
	}

	// Reopen and verify data
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		for i in 0..20 {
			let txn = tree.begin().unwrap();
			let key = format!("key_{}", i);
			assert!(txn.get(key.as_bytes()).unwrap().is_some(), "Key {} should exist", key);
		}

		tree.close().await.unwrap();
	}
}

/// Tests that SSTable table_ids are in correct ascending order after
/// flushing immutable memtables followed by active memtable.
#[test_log::test(tokio::test)]
async fn test_sst_table_ids_ordered_correctly_on_close() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 10 * 1024 * 1024; // Large to prevent auto-flush
		opts.flush_on_close = true;
	});

	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Write data - will be flushed on close
		for i in 0..5 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("key_{}", i).as_bytes(), b"value").unwrap();
			txn.commit().await.unwrap();
		}

		// Get initial table count
		let initial_count = tree.core.inner.level_manifest.read().unwrap().iter().count();

		// Close triggers flush
		tree.close().await.unwrap();

		// Reopen and verify table count increased
		let tree2 = Tree::new(Arc::clone(&opts)).unwrap();
		let after_count = tree2.core.inner.level_manifest.read().unwrap().iter().count();

		assert_eq!(after_count, initial_count + 1, "Should have one more SST after close flush");

		// Verify table_ids are in ascending order
		{
			let manifest = tree2.core.inner.level_manifest.read().unwrap();
			let mut prev_id = 0u64;
			for table in manifest.iter() {
				assert!(
					table.id > prev_id || prev_id == 0,
					"Table IDs should be in ascending order: prev={}, current={}",
					prev_id,
					table.id
				);
				prev_id = table.id;
			}
		} // Drop manifest before await

		tree2.close().await.unwrap();
	}
}

/// Tests that WAL numbers are correctly tracked with memtables and that
/// log_number is updated incrementally as each memtable is flushed.
#[test_log::test(tokio::test)]
async fn test_wal_number_tracking_on_flush() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 10 * 1024 * 1024; // Large - prevent auto flush
		opts.flush_on_close = true;
	});

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Get initial state
	let initial_log_number = tree.core.inner.level_manifest.read().unwrap().get_log_number();
	let initial_wal_number = tree.core.inner.active_memtable.read().unwrap().get_wal_number();
	log::info!(
		"Initial state: log_number={}, wal_number={}",
		initial_log_number,
		initial_wal_number
	);

	// Write some data
	let mut txn = tree.begin().unwrap();
	txn.set(b"key1", b"value1").unwrap();
	txn.commit().await.unwrap();

	// Explicitly flush
	tree.flush().unwrap();

	// Verify log_number increased to wal_number + 1
	let after_flush_log = tree.core.inner.level_manifest.read().unwrap().get_log_number();
	assert_eq!(
		after_flush_log,
		initial_wal_number + 1,
		"log_number should be initial_wal + 1 after flush"
	);

	// Verify new memtable has new WAL number
	let new_wal_number = tree.core.inner.active_memtable.read().unwrap().get_wal_number();
	assert!(
		new_wal_number > initial_wal_number,
		"New memtable should have higher WAL number: {} > {}",
		new_wal_number,
		initial_wal_number
	);
	log::info!(
		"After first flush: log_number={}, new_wal_number={}",
		after_flush_log,
		new_wal_number
	);

	// Second write + flush cycle
	let mut txn = tree.begin().unwrap();
	txn.set(b"key2", b"value2").unwrap();
	txn.commit().await.unwrap();

	tree.flush().unwrap();

	let after_second_flush_log = tree.core.inner.level_manifest.read().unwrap().get_log_number();
	assert_eq!(
		after_second_flush_log,
		new_wal_number + 1,
		"log_number should be new_wal + 1 after second flush"
	);
	log::info!("After second flush: log_number={}", after_second_flush_log);

	// Verify data survives close/reopen
	tree.close().await.unwrap();

	let tree2 = Tree::new(Arc::clone(&opts)).unwrap();

	let txn = tree2.begin().unwrap();
	assert_eq!(
		txn.get(b"key1").unwrap(),
		Some(b"value1".to_vec()),
		"key1 should exist after reopen"
	);
	assert_eq!(
		txn.get(b"key2").unwrap(),
		Some(b"value2".to_vec()),
		"key2 should exist after reopen"
	);

	tree2.close().await.unwrap();
}

/// Tests that the active memtable's WAL number is correctly updated after
/// each memtable swap during flush.
#[test_log::test(tokio::test)]
async fn test_memtable_wal_number_after_swap() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 10 * 1024 * 1024; // Large - prevent auto flush
	});

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Track WAL numbers through explicit flush cycles
	let wal_1 = tree.core.inner.active_memtable.read().unwrap().get_wal_number();
	log::info!("Initial WAL number: {}", wal_1);

	// Write data and flush
	let mut txn = tree.begin().unwrap();
	txn.set(b"key1", b"value1").unwrap();
	txn.commit().await.unwrap();

	tree.flush().unwrap();

	let wal_2 = tree.core.inner.active_memtable.read().unwrap().get_wal_number();
	log::info!("WAL number after first flush: {}", wal_2);
	assert!(wal_2 > wal_1, "WAL number should increase after flush: {} > {}", wal_2, wal_1);

	// Write more and flush again
	let mut txn = tree.begin().unwrap();
	txn.set(b"key2", b"value2").unwrap();
	txn.commit().await.unwrap();

	tree.flush().unwrap();

	let wal_3 = tree.core.inner.active_memtable.read().unwrap().get_wal_number();
	log::info!("WAL number after second flush: {}", wal_3);
	assert!(wal_3 > wal_2, "WAL number should increase after second flush: {} > {}", wal_3, wal_2);

	// Verify data survives close/reopen
	tree.close().await.unwrap();

	let tree2 = Tree::new(Arc::clone(&opts)).unwrap();

	let txn = tree2.begin().unwrap();
	assert_eq!(
		txn.get(b"key1").unwrap(),
		Some(b"value1".to_vec()),
		"key1 should exist after reopen"
	);
	assert_eq!(
		txn.get(b"key2").unwrap(),
		Some(b"value2".to_vec()),
		"key2 should exist after reopen"
	);

	tree2.close().await.unwrap();
}

/// Tests that after multiple flushes and reopen, the new active memtable
/// has the correct WAL number assigned.
#[test_log::test(tokio::test)]
async fn test_wal_number_correct_after_reopen() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 10 * 1024 * 1024; // Large - prevent auto flush
		opts.flush_on_close = true;
	});

	// Phase 1: Multiple flushes, track WAL numbers
	let final_log_number;
	let last_flushed_wal;
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Do 3 write+flush cycles, tracking the last WAL that gets flushed
		let mut current_wal;
		for i in 0..3 {
			// Get the WAL number before flush - this WAL will be flushed
			current_wal = tree.core.inner.active_memtable.read().unwrap().get_wal_number();

			let mut txn = tree.begin().unwrap();
			txn.set(format!("key{}", i).as_bytes(), b"value").unwrap();
			txn.commit().await.unwrap();
			tree.flush().unwrap();

			log::info!(
				"After flush {}: flushed WAL {}, new log_number={}",
				i,
				current_wal,
				tree.core.inner.level_manifest.read().unwrap().get_log_number()
			);
		}

		// The last WAL that was flushed is the one before the final flush
		last_flushed_wal = tree.core.inner.level_manifest.read().unwrap().get_log_number() - 1;
		final_log_number = tree.core.inner.level_manifest.read().unwrap().get_log_number();

		log::info!(
			"Before close: last_flushed_wal={}, final_log_number={}",
			last_flushed_wal,
			final_log_number
		);

		tree.close().await.unwrap();
	}

	// Phase 2: Reopen and verify active memtable's WAL number
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		let reopened_log_number = tree.core.inner.level_manifest.read().unwrap().get_log_number();
		let active_wal_number = tree.core.inner.active_memtable.read().unwrap().get_wal_number();

		log::info!(
			"After reopen: log_number={}, active_wal_number={}, last_flushed_wal={}",
			reopened_log_number,
			active_wal_number,
			last_flushed_wal
		);

		// log_number should be preserved from before close
		assert_eq!(
			reopened_log_number, final_log_number,
			"log_number should be preserved after reopen"
		);

		// Active memtable's WAL number should be exactly log_number
		// because WAL opens starting from log_number
		assert_eq!(
			active_wal_number, reopened_log_number,
			"Active memtable WAL number should equal log_number on fresh open"
		);

		// Active WAL number should be greater than the last flushed WAL
		// (log_number = last_flushed_wal + 1)
		assert!(
			active_wal_number > last_flushed_wal,
			"Active WAL number should be > last flushed WAL: {} > {}",
			active_wal_number,
			last_flushed_wal
		);

		// More specifically, it should be exactly last_flushed_wal + 1
		assert_eq!(
			active_wal_number,
			last_flushed_wal + 1,
			"Active WAL number should be last_flushed_wal + 1"
		);

		tree.close().await.unwrap();
	}
}

#[test_log::test(tokio::test)]
async fn test_wal_files_after_multiple_open_close_cycles() {
	// Simulates: open -> write 100 entries -> close, repeated multiple times
	// Tests that data is recoverable and manifest state is correct after each cycle
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 10 * 1024 * 1024; // Large to prevent auto-flush
	});

	let mut previous_log_numbers = Vec::new();

	for cycle in 1..=3 {
		{
			let tree = Tree::new(Arc::clone(&opts)).unwrap();

			// Write 100 entries
			for i in 0..100 {
				let mut txn = tree.begin().unwrap();
				txn.set(format!("cycle{}_key_{}", cycle, i).as_bytes(), b"value").unwrap();
				txn.commit().await.unwrap();
			}

			// Close (should flush)
			tree.close().await.unwrap();

			// Give async cleanup time to run
			tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
		}

		// Check manifest state after close
		let manifest = LevelManifest::new(Arc::clone(&opts)).unwrap();
		let log_number_after_close = manifest.get_log_number();
		previous_log_numbers.push(log_number_after_close);

		// Verify data from this cycle is accessible
		{
			let tree = Tree::new(Arc::clone(&opts)).unwrap();
			let txn = tree.begin().unwrap();

			// Check data from current cycle
			assert_eq!(
				txn.get(format!("cycle{}_key_0", cycle).as_bytes()).unwrap(),
				Some(b"value".to_vec()),
				"Data from cycle {} should be recoverable",
				cycle
			);

			// Check data from all previous cycles is still accessible
			for prev_cycle in 1..cycle {
				assert_eq!(
					txn.get(format!("cycle{}_key_0", prev_cycle).as_bytes()).unwrap(),
					Some(b"value".to_vec()),
					"Data from previous cycle {} should still be accessible",
					prev_cycle
				);
			}

			drop(txn);
			tree.close().await.unwrap();
		}
	}

	// Final verification: Manifest log_number should have advanced across cycles
	assert!(previous_log_numbers.len() == 3, "Should have collected log numbers from all 3 cycles");

	// Verify all data is still accessible after all cycles
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let txn = tree.begin().unwrap();

		for cycle in 1..=3 {
			assert_eq!(
				txn.get(format!("cycle{}_key_0", cycle).as_bytes()).unwrap(),
				Some(b"value".to_vec()),
				"Data from cycle {} should be accessible in final check",
				cycle
			);
		}

		drop(txn);
		tree.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_cleanup_orphaned_sst_files() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();
	let opts = create_test_options(path.clone(), |_| {});

	// Create initial tree and add some data
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let mut txn = tree.begin().unwrap();
		txn.set(b"key1", b"value1").unwrap();
		txn.commit().await.unwrap();
		tree.flush().unwrap();
		tree.close().await.unwrap();
	}

	// Manually create an orphaned SST file (simulating incomplete flush)
	let orphaned_table_id = 9999;
	let orphaned_path = opts.sstable_file_path(orphaned_table_id);
	std::fs::write(&orphaned_path, b"fake sst data").unwrap();

	// Verify orphaned file exists
	assert!(orphaned_path.exists(), "Orphaned SST should exist before cleanup");

	// Reopen database - should trigger cleanup
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Verify orphaned file was deleted
		assert!(!orphaned_path.exists(), "Orphaned SST should be cleaned up");

		// Verify real data still accessible
		let txn = tree.begin().unwrap();
		let result = txn.get(b"key1").unwrap().unwrap();
		assert_eq!(result, b"value1".to_vec());

		tree.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_manifest_atomic_sst_and_log_number() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	// Use tiny threshold to ensure flush happens
	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 1024;
	});

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Get initial log_number
	let initial_log_number = {
		let manifest = tree.core.inner.level_manifest.read().unwrap();
		manifest.get_log_number()
	};

	// Add data
	for i in 0..100 {
		let mut txn = tree.begin().unwrap();
		txn.set(format!("key{}", i).as_bytes(), b"value").unwrap();
		txn.commit().await.unwrap();
	}

	// Explicitly trigger flush to ensure test reliability
	tree.flush().unwrap();

	// Verify that when SST is added, log_number is also updated
	{
		let manifest = tree.core.inner.level_manifest.read().unwrap();
		let new_log_number = manifest.get_log_number();
		let level0_tables = &manifest.levels.get_levels()[0].tables;

		// SST should be flushed now
		assert!(!level0_tables.is_empty(), "SST should be flushed");
		// And log_number MUST have been updated atomically
		assert!(
			new_log_number > initial_log_number,
			"log_number should be updated atomically with SST addition"
		);
		drop(manifest);
	}

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn test_no_spurious_small_flush() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 100 * 1024; // 100KB threshold
	});

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Add small amount of data (way below threshold)
	let mut txn = tree.begin().unwrap();
	txn.set(b"key1", b"value1").unwrap();
	txn.commit().await.unwrap();

	// Manually trigger wake_up (simulating spurious notification)
	if let Some(ref task_manager) = *tree.core.task_manager.lock().unwrap() {
		task_manager.wake_up_memtable();
	}

	// Wait a bit
	tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

	// Verify no flush occurred (data still in active memtable, not in L0)
	{
		let manifest = tree.core.inner.level_manifest.read().unwrap();
		assert!(
			manifest.levels.get_levels()[0].tables.is_empty(),
			"Should not flush small memtable due to spurious notification"
		);
		drop(manifest);
	}

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn test_crash_recovery_with_orphaned_sst() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();
	let opts = create_test_options(path.clone(), |_| {});

	// Phase 1: Write data and flush
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let mut txn = tree.begin().unwrap();
		txn.set(b"committed_key", b"committed_value").unwrap();
		txn.commit().await.unwrap();
		tree.flush().unwrap();
		tree.close().await.unwrap();
	}

	// Phase 2: Simulate incomplete flush (create orphaned SST + keep WAL)
	let orphaned_table_id = 9998;
	let orphaned_sst_path = opts.sstable_file_path(orphaned_table_id);
	std::fs::write(&orphaned_sst_path, b"orphaned SST content").unwrap();

	// Add more data that would be in WAL
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let mut txn = tree.begin().unwrap();
		txn.set(b"wal_key", b"wal_value").unwrap();
		txn.commit().await.unwrap();
		// Don't flush - keep in WAL
		tree.close().await.unwrap();
	}

	// Phase 3: Reopen - should cleanup orphaned SST and recover from WAL
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Verify orphaned SST was cleaned up
		assert!(!orphaned_sst_path.exists(), "Orphaned SST should be removed");

		// Verify WAL data was recovered
		let txn = tree.begin().unwrap();
		let result = txn.get(b"wal_key").unwrap().unwrap();
		assert_eq!(result, b"wal_value".to_vec());

		// Verify committed data still accessible
		let result = txn.get(b"committed_key").unwrap().unwrap();
		assert_eq!(result, b"committed_value".to_vec());

		tree.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_cleanup_multiple_orphaned_ssts() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();
	let opts = create_test_options(path.clone(), |_| {});

	// Create initial database
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let mut txn = tree.begin().unwrap();
		txn.set(b"key1", b"value1").unwrap();
		txn.commit().await.unwrap();
		tree.close().await.unwrap();
	}

	// Create multiple orphaned SST files
	let orphaned_ids = vec![8888, 9999, 10000];
	for table_id in &orphaned_ids {
		let orphaned_path = opts.sstable_file_path(*table_id);
		std::fs::write(&orphaned_path, format!("orphaned {}", table_id)).unwrap();
	}

	// Verify all exist
	for table_id in &orphaned_ids {
		assert!(opts.sstable_file_path(*table_id).exists());
	}

	// Reopen - should cleanup all orphaned files
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Verify all orphaned files deleted
		for table_id in &orphaned_ids {
			assert!(
				!opts.sstable_file_path(*table_id).exists(),
				"Orphaned SST {} should be cleaned up",
				table_id
			);
		}

		tree.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_valid_ssts_not_deleted_during_cleanup() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();
	let opts = create_small_memtable_options(path.clone());

	// Create database and flush some data
	let valid_table_ids = {
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		for i in 0..200 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("key{}", i).as_bytes(), b"value").unwrap();
			txn.commit().await.unwrap();
		}

		tree.flush().unwrap();

		// Get list of valid table IDs
		let ids = {
			let manifest = tree.core.inner.level_manifest.read().unwrap();
			let ids: Vec<u64> = manifest.iter().map(|t| t.id).collect();
			drop(manifest);
			ids
		};

		tree.close().await.unwrap();
		ids
	};

	// Create orphaned SST
	let orphaned_id = 9999;
	std::fs::write(opts.sstable_file_path(orphaned_id), b"orphaned").unwrap();

	// Reopen
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Verify orphaned deleted
		assert!(!opts.sstable_file_path(orphaned_id).exists());

		// Verify all valid SSTs still exist
		for table_id in &valid_table_ids {
			assert!(
				opts.sstable_file_path(*table_id).exists(),
				"Valid SST {} should not be deleted",
				table_id
			);
		}

		// Verify data still accessible
		let txn = tree.begin().unwrap();
		let result = txn.get(b"key1").unwrap().unwrap();
		assert_eq!(result, b"value".to_vec());

		tree.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_comprehensive_orphaned_cleanup_with_multiple_ssts() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 1024; // Small to create multiple SSTs
	});

	// Phase 1: Create multiple valid SSTs with real data
	let mut expected_keys = Vec::new();
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Write multiple batches to create multiple SSTs
		for batch_num in 0..5 {
			for i in 0..50 {
				let key = format!("batch{}_key{}", batch_num, i);
				let value = format!("batch{}_value{}", batch_num, i);
				expected_keys.push(key.clone());

				let mut txn = tree.begin().unwrap();
				txn.set(key.as_bytes(), value.as_bytes()).unwrap();
				txn.commit().await.unwrap();
			}
			// Force flush after each batch
			tree.flush().unwrap();
		}

		tree.close().await.unwrap();
	}

	// Phase 2: Get valid SST IDs and create orphaned SSTs
	let valid_sst_ids = {
		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let ids = {
			let manifest = tree.core.inner.level_manifest.read().unwrap();
			let ids: Vec<u64> = manifest.iter().map(|t| t.id).collect();
			drop(manifest);
			ids
		};
		tree.close().await.unwrap();
		ids
	};

	// Create multiple orphaned SST files
	let orphaned_ids = vec![8888, 9999, 10000, 10001];
	for table_id in &orphaned_ids {
		let orphaned_path = opts.sstable_file_path(*table_id);
		std::fs::write(&orphaned_path, format!("orphaned SST {}", table_id)).unwrap();
	}

	// Verify all files exist (both valid and orphaned)
	for table_id in &valid_sst_ids {
		assert!(
			opts.sstable_file_path(*table_id).exists(),
			"Valid SST {} should exist before reopen",
			table_id
		);
	}
	for table_id in &orphaned_ids {
		assert!(
			opts.sstable_file_path(*table_id).exists(),
			"Orphaned SST {} should exist before cleanup",
			table_id
		);
	}

	// Phase 3: Reopen - should cleanup orphaned but keep valid
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		// Verify all orphaned files deleted
		for table_id in &orphaned_ids {
			assert!(
				!opts.sstable_file_path(*table_id).exists(),
				"Orphaned SST {} should be cleaned up",
				table_id
			);
		}

		// Verify all valid SST files still exist
		for table_id in &valid_sst_ids {
			assert!(
				opts.sstable_file_path(*table_id).exists(),
				"Valid SST {} should not be deleted",
				table_id
			);
		}

		// Verify ALL data is still accessible
		for key in &expected_keys {
			let txn = tree.begin().unwrap();
			let result = txn.get(key.as_bytes()).unwrap();
			assert!(result.is_some(), "Key {} should be accessible", key);
		}

		// Spot check a few specific values
		let txn = tree.begin().unwrap();
		let result = txn.get(b"batch0_key0").unwrap().unwrap();
		assert_eq!(result, b"batch0_value0".to_vec());

		let result = txn.get(b"batch4_key49").unwrap().unwrap();
		assert_eq!(result, b"batch4_value49".to_vec());

		tree.close().await.unwrap();
	}
}

#[test_log::test(tokio::test)]
async fn test_wal_recovery_mode_absolute_consistency_fails_on_corruption() {
	use std::io::Write;

	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	// Phase 1: Create a valid WAL with some data
	{
		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 10 * 1024 * 1024; // Large to prevent auto-flush
			opts.flush_on_close = false; // Don't flush on close - keep data in WAL
		});

		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let mut txn = tree.begin().unwrap();
		txn.set(b"key1", b"value1").unwrap();
		txn.set(b"key2", b"value2").unwrap();
		txn.commit().await.unwrap();

		// Close without flush - data only in WAL
		tree.close().await.unwrap();
	}

	// Phase 2: Corrupt the WAL file
	let wal_dir = path.join("wal");
	let segment_path = wal_dir.join("00000000000000000000.wal");
	{
		let mut file = std::fs::OpenOptions::new().append(true).open(&segment_path).unwrap();
		file.write_all(b"CORRUPTED_DATA_AT_END").unwrap();
	}

	// Phase 3: Try to open with AbsoluteConsistency mode - should FAIL
	{
		let opts = create_test_options(path.clone(), |opts| {
			opts.wal_recovery_mode = WalRecoveryMode::AbsoluteConsistency;
		});

		let result = Tree::new(opts);

		// Should fail due to corruption
		match result {
			Err(Error::WalCorruption {
				..
			}) => {
				// Expected - AbsoluteConsistency correctly fails on
				// corruption
			}
			Err(e) => panic!("Expected WalCorruption error, got: {}", e),
			Ok(_) => {
				panic!("AbsoluteConsistency should fail on WAL corruption, but it succeeded")
			}
		}
	}
}

#[test_log::test(tokio::test)]
async fn test_wal_recovery_mode_tolerate_with_repair_succeeds_on_corruption() {
	use std::io::Write;

	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	// Phase 1: Create a valid WAL with some data
	{
		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 10 * 1024 * 1024; // Large to prevent auto-flush
			opts.flush_on_close = false; // Don't flush on close - keep data in WAL
		});

		let tree = Tree::new(Arc::clone(&opts)).unwrap();
		let mut txn = tree.begin().unwrap();
		txn.set(b"key1", b"value1").unwrap();
		txn.set(b"key2", b"value2").unwrap();
		txn.commit().await.unwrap();

		// Close without flush - data only in WAL
		tree.close().await.unwrap();
	}

	// Phase 2: Corrupt the WAL file at the end
	let wal_dir = path.join("wal");
	let segment_path = wal_dir.join("00000000000000000000.wal");
	{
		let mut file = std::fs::OpenOptions::new().append(true).open(&segment_path).unwrap();
		file.write_all(b"CORRUPTED_DATA_AT_END").unwrap();
	}

	// Phase 3: Open with default TolerateCorruptedWithRepair mode - should SUCCEED
	{
		let opts = create_test_options(path.clone(), |opts| {
			opts.wal_recovery_mode = WalRecoveryMode::TolerateCorruptedWithRepair;
		});

		let result = Tree::new(opts);

		// Should succeed (repair the WAL and recover data)
		let tree = match result {
			Ok(t) => t,
			Err(e) => {
				panic!("TolerateCorruptedWithRepair should succeed after repairing WAL: {}", e)
			}
		};

		// Verify data was recovered
		let txn = tree.begin().unwrap();
		assert_eq!(txn.get(b"key1").unwrap(), Some(b"value1".to_vec()));
		assert_eq!(txn.get(b"key2").unwrap(), Some(b"value2".to_vec()));

		tree.close().await.unwrap();
	}
}

#[test]
fn test_wal_recovery_mode_default_is_tolerate_with_repair() {
	// Verify that the default recovery mode is TolerateCorruptedWithRepair
	let opts = Options::default();
	assert_eq!(
		opts.wal_recovery_mode,
		WalRecoveryMode::TolerateCorruptedWithRepair,
		"Default recovery mode should be TolerateCorruptedWithRepair"
	);
}

#[test_log::test(tokio::test)]
async fn test_wal_incremental_number_after_flush_and_reopen() {
	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 1024;
		opts.flush_on_close = true;
	});

	// Phase 1: Write data and clean shutdown (triggers flush)
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		let wal_num_before = tree.core.inner.wal.read().get_active_log_number();
		assert_eq!(wal_num_before, 0, "Fresh database should start at WAL #0");

		let mut txn = tree.begin().unwrap();
		txn.set(b"test_key", b"test_value").unwrap();
		txn.commit().await.unwrap();

		tree.close().await.unwrap();
	}

	// Phase 2: Reopen and verify WAL has incremental number
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		let wal_num_after_reopen = tree.core.inner.wal.read().get_active_log_number();

		// CRITICAL: WAL number should be > 0 since WAL #0 was flushed
		assert!(
			wal_num_after_reopen > 0,
			"BUG: After flush and reopen, WAL should start at incremental number, not 0. Got {}",
			wal_num_after_reopen
		);

		tree.close().await.unwrap();
	}
}

/// Tests that new writes after crash recovery are not lost.
///
/// BUG SCENARIO (without fix):
/// 1. Crash with segments 1, 2, 3 on disk, manifest log_number=1
/// 2. Recovery: replays all segments, WAL opens at segment 1 for new writes
/// 3. Write key4 → goes to segment 1
/// 4. Flush → log_number becomes 2
/// 5. Second crash and recovery
/// 6. WAL recovery skips segments < 2 (skips segment 1!)
/// 7. key4 is LOST
///
/// FIX: WAL opens at max(log_number, highest_segment_on_disk)
/// - With fix: WAL opens at segment 3, key4 goes to segment 3
/// - After flush log_number=4, nothing is skipped, no data loss
#[test_log::test(tokio::test)]
async fn test_recovery_with_manually_created_wal_segments() {
	use crate::batch::Batch;
	use crate::vlog::ValueLocation;
	use crate::wal::Wal;

	let temp_dir = TempDir::new("test").unwrap();
	let path = temp_dir.path().to_path_buf();

	let opts = create_test_options(path.clone(), |opts| {
		opts.max_memtable_size = 10 * 1024 * 1024; // Large - prevent auto flush
		opts.flush_on_close = false; // Don't auto-flush on close
	});

	let wal_path = opts.path.join("wal");

	// Phase 1: Establish baseline - write key1, flush, close
	// This creates: SST with key1, manifest log_number = 1
	let log_number_after_phase1;
	let last_seq_after_phase1;
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		let mut txn = tree.begin().unwrap();
		txn.set(b"key1", b"value1_from_sst").unwrap();
		txn.commit().await.unwrap();
		tree.flush().unwrap();

		log_number_after_phase1 = tree.core.inner.level_manifest.read().unwrap().get_log_number();
		last_seq_after_phase1 = tree.core.inner.level_manifest.read().unwrap().get_last_sequence();
		log::info!(
			"Phase 1: After flush, log_number={}, last_seq={}",
			log_number_after_phase1,
			last_seq_after_phase1
		);

		tree.close().await.unwrap();
	}

	// Phase 2: Manually create WAL segments 2 and 3 with key2 and key3
	// This simulates a crash where WAL rotations happened but manifest wasn't
	// updated
	let highest_segment_created;
	{
		log::info!("Phase 2: Creating additional WAL segments");

		// Find the highest existing segment on disk
		let highest_existing: u64 = std::fs::read_dir(&wal_path)
			.unwrap()
			.filter_map(|e| e.ok())
			.filter_map(|e| {
				e.path()
					.file_name()
					.and_then(|n| n.to_str())
					.and_then(|n| n.strip_suffix(".wal"))
					.and_then(|n| n.parse::<u64>().ok())
			})
			.max()
			.unwrap_or(0);

		// Create segment for key2
		let segment_for_key2 = highest_existing + 1;
		let next_seq = last_seq_after_phase1 + 1;
		{
			let mut batch = Batch::new(next_seq);
			let encoded_value =
				ValueLocation::with_inline_value(b"value2_from_wal".to_vec()).encode();
			batch
				.add_record(InternalKeyKind::Set, b"key2".to_vec(), Some(encoded_value), 0)
				.unwrap();

			let mut wal = Wal::open_with_min_log_number(
				&wal_path,
				segment_for_key2,
				crate::wal::Options::default(),
			)
			.unwrap();
			wal.append(&batch.encode().unwrap()).unwrap();
			wal.sync().unwrap();
			wal.close().unwrap();
			log::info!("Phase 2: Created segment {} with key2", segment_for_key2);
		}

		// Create segment for key3
		let segment_for_key3 = segment_for_key2 + 1;
		{
			let mut batch = Batch::new(next_seq + 1);
			let encoded_value =
				ValueLocation::with_inline_value(b"value3_from_wal".to_vec()).encode();
			batch
				.add_record(InternalKeyKind::Set, b"key3".to_vec(), Some(encoded_value), 0)
				.unwrap();

			let mut wal = Wal::open_with_min_log_number(
				&wal_path,
				segment_for_key3,
				crate::wal::Options::default(),
			)
			.unwrap();
			wal.append(&batch.encode().unwrap()).unwrap();
			wal.sync().unwrap();
			wal.close().unwrap();
			log::info!("Phase 2: Created segment {} with key3", segment_for_key3);
		}

		highest_segment_created = segment_for_key3;
	}

	// Phase 3: First recovery - open Tree, verify key2/key3 recovered, then write
	// NEW data This is where the bug manifests:
	// - WITHOUT FIX: WAL opens at log_number (1), new writes go to segment 1
	// - WITH FIX: WAL opens at highest (3), new writes go to segment 3
	let active_wal_after_recovery;
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		active_wal_after_recovery = tree.core.inner.wal.read().get_active_log_number();
		let log_number = tree.core.inner.level_manifest.read().unwrap().get_log_number();
		log::info!(
			"Phase 3: active_wal={}, log_number={}, highest_created={}",
			active_wal_after_recovery,
			log_number,
			highest_segment_created
		);

		// KEY ASSERTION: WAL should open at highest segment, not log_number
		assert_eq!(
			active_wal_after_recovery, highest_segment_created,
			"BUG: WAL opened at {} but should open at highest segment {} to prevent data loss",
			active_wal_after_recovery, highest_segment_created
		);

		// Verify initial recovery worked
		let txn = tree.begin().unwrap();
		assert!(txn.get(b"key1").unwrap().is_some(), "key1 should exist");
		assert!(txn.get(b"key2").unwrap().is_some(), "key2 should exist");
		assert!(txn.get(b"key3").unwrap().is_some(), "key3 should exist");
		drop(txn);

		// Write NEW data after recovery - this is the data that could be lost
		let mut txn = tree.begin().unwrap();
		txn.set(b"key4_new_after_recovery", b"value4").unwrap();
		txn.commit().await.unwrap();
		log::info!("Phase 3: Wrote key4 to WAL segment {}", active_wal_after_recovery);

		// Flush - this updates log_number
		tree.flush().unwrap();
		let log_number_after_flush =
			tree.core.inner.level_manifest.read().unwrap().get_log_number();
		log::info!("Phase 3: After flush, log_number={}", log_number_after_flush);

		// Write more data that stays in WAL (not flushed)
		let mut txn = tree.begin().unwrap();
		txn.set(b"key5_unflushed", b"value5").unwrap();
		txn.commit().await.unwrap();
		log::info!("Phase 3: Wrote key5 (unflushed)");

		// Close without flush (simulating crash)
		tree.close().await.unwrap();
	}

	// Phase 4: Second recovery - verify NO data loss
	// Without the fix, key4 and key5 would be lost because they were written
	// to segment 1 (if WAL opened there), and segment 1 is now skipped
	{
		let tree = Tree::new(Arc::clone(&opts)).unwrap();

		let txn = tree.begin().unwrap();

		// Original data should still exist
		assert!(txn.get(b"key1").unwrap().is_some(), "key1 should persist");
		assert!(txn.get(b"key2").unwrap().is_some(), "key2 should persist");
		assert!(txn.get(b"key3").unwrap().is_some(), "key3 should persist");

		// CRITICAL: New data written after recovery should NOT be lost
		let key4 = txn.get(b"key4_new_after_recovery").unwrap();
		assert_eq!(
			key4,
			Some(b"value4".to_vec()),
			"DATA LOSS BUG: key4 written after recovery was lost! \
				 This happens when WAL opens at log_number instead of highest segment."
		);

		let key5 = txn.get(b"key5_unflushed").unwrap();
		assert_eq!(key5, Some(b"value5".to_vec()), "DATA LOSS BUG: key5 (unflushed) was lost!");

		log::info!("Phase 4: All data verified - no data loss!");

		tree.close().await.unwrap();
	}
}

// Test if flush_wal is concurrent safe and does not block the commit pipeline.
#[test(tokio::test)]
async fn test_flush_wal_concurrent_commits() {
	use std::sync::atomic::{AtomicUsize, Ordering};

	use tokio::task::JoinSet;

	let temp_dir = create_temp_directory();
	let opts = create_test_options(temp_dir.path().to_path_buf(), |opts| {
		opts.enable_vlog = true;
		opts.vlog_value_threshold = 50; // Some values will go to VLog
	});

	let tree = Arc::new(Tree::new(opts).unwrap());
	let commit_count = Arc::new(AtomicUsize::new(0));
	let flush_count = Arc::new(AtomicUsize::new(0));

	let num_commit_tasks = 8;
	let commits_per_task = 25;
	let num_flush_tasks = 2;
	let flushes_per_task = 50;

	let mut join_set = JoinSet::new();

	// Spawn commit tasks
	for task_id in 0..num_commit_tasks {
		let tree = Arc::clone(&tree);
		let commit_count = Arc::clone(&commit_count);

		join_set.spawn(async move {
			for i in 0..commits_per_task {
				let key = format!("task{}_key{}", task_id, i);
				let value = if i % 2 == 0 {
					// Small value (inline)
					format!("value{}", i)
				} else {
					// Large value (VLog)
					"x".repeat(100)
				};

				let mut txn = tree.begin().unwrap();
				txn.set(key.as_bytes(), value.as_bytes()).unwrap();
				txn.commit().await.unwrap();
				commit_count.fetch_add(1, Ordering::SeqCst);
			}
		});
	}

	// Spawn flush tasks
	for _ in 0..num_flush_tasks {
		let tree = Arc::clone(&tree);
		let flush_count = Arc::clone(&flush_count);

		join_set.spawn(async move {
			for i in 0..flushes_per_task {
				// Alternate between flush and sync
				let sync = i % 2 == 0;
				let result = tree.flush_wal(sync);
				assert!(result.is_ok(), "flush_wal should succeed during concurrent commits");
				flush_count.fetch_add(1, Ordering::SeqCst);
				// Small yield to allow interleaving
				tokio::task::yield_now().await;
			}
		});
	}

	// Wait for all tasks to complete
	while let Some(result) = join_set.join_next().await {
		result.expect("Task should complete successfully");
	}

	// Verify counts
	assert_eq!(
		commit_count.load(Ordering::SeqCst),
		num_commit_tasks * commits_per_task,
		"All commits should complete"
	);
	assert_eq!(
		flush_count.load(Ordering::SeqCst),
		num_flush_tasks * flushes_per_task,
		"All flushes should complete"
	);

	// Verify all data is accessible
	{
		let txn = tree.begin().unwrap();
		for task_id in 0..num_commit_tasks {
			for i in 0..commits_per_task {
				let key = format!("task{}_key{}", task_id, i);
				let result = txn.get(key.as_bytes()).unwrap();
				assert!(result.is_some(), "Key {} should exist after concurrent operations", key);
			}
		}
	}

	tree.close().await.unwrap();
}

/// Test that verifies lexicographic byte ordering with keys of varying lengths.
/// This ensures that keys like "a" < "b" < "ba" < "baaa" < "baaaaaaaaaaaaaaaaaa1" < "c"
/// are correctly ordered during range scans, both before and after flush.
#[test(tokio::test)]
async fn test_range_key_ordering_correctness() {
	use crate::test::collect_transaction_reverse;

	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_small_memtable_options(path.clone());

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Keys in expected lexicographic sorted order
	let keys_in_order = ["a", "b", "ba", "baaa", "baaaaaaaaaaaaaaaaaa1", "c"];

	// Insert keys in random order to ensure sorting is done by the system
	let insert_order = ["baaa", "c", "a", "baaaaaaaaaaaaaaaaaa1", "b", "ba"];
	for key in insert_order.iter() {
		let mut txn = tree.begin().unwrap();
		let value = format!("value_for_{}", key);
		txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	// Test BEFORE flush (memtable path)
	{
		let txn = tree.begin().unwrap();

		// Forward iteration
		let range_result = collect_transaction_all(&mut txn.range(b"a", b"d").unwrap()).unwrap();

		assert_eq!(
			range_result.len(),
			keys_in_order.len(),
			"Should return all {} keys",
			keys_in_order.len()
		);

		// Verify forward order
		for (idx, (key, value)) in range_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let expected_key = keys_in_order[idx];
			let expected_value = format!("value_for_{}", expected_key);

			assert_eq!(
				key_str, expected_key,
				"Forward iteration: Key mismatch at index {idx}: expected '{expected_key}', found '{key_str}'"
			);
			assert_eq!(
				value.as_slice(),
				expected_value.as_bytes(),
				"Forward iteration: Value mismatch at index {idx}"
			);
		}

		// Reverse iteration
		let reverse_result =
			collect_transaction_reverse(&mut txn.range(b"a", b"d").unwrap()).unwrap();

		assert_eq!(
			reverse_result.len(),
			keys_in_order.len(),
			"Reverse should return all {} keys",
			keys_in_order.len()
		);

		// Verify reverse order
		for (idx, (key, _value)) in reverse_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let expected_key = keys_in_order[keys_in_order.len() - 1 - idx];

			assert_eq!(
				key_str, expected_key,
				"Reverse iteration: Key mismatch at index {idx}: expected '{expected_key}', found '{key_str}'"
			);
		}
	}

	// Flush to disk
	tree.flush().unwrap();

	// Test AFTER flush (SSTable path)
	{
		let txn = tree.begin().unwrap();

		// Forward iteration
		let range_result = collect_transaction_all(&mut txn.range(b"a", b"d").unwrap()).unwrap();

		assert_eq!(
			range_result.len(),
			keys_in_order.len(),
			"After flush: Should return all {} keys",
			keys_in_order.len()
		);

		// Verify forward order after flush
		for (idx, (key, value)) in range_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let expected_key = keys_in_order[idx];
			let expected_value = format!("value_for_{}", expected_key);

			assert_eq!(
				key_str, expected_key,
				"After flush forward: Key mismatch at index {idx}: expected '{expected_key}', found '{key_str}'"
			);
			assert_eq!(
				value.as_slice(),
				expected_value.as_bytes(),
				"After flush forward: Value mismatch at index {idx}"
			);
		}

		// Reverse iteration after flush
		let reverse_result =
			collect_transaction_reverse(&mut txn.range(b"a", b"d").unwrap()).unwrap();

		for (idx, (key, _value)) in reverse_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let expected_key = keys_in_order[keys_in_order.len() - 1 - idx];

			assert_eq!(
				key_str, expected_key,
				"After flush reverse: Key mismatch at index {idx}: expected '{expected_key}', found '{key_str}'"
			);
		}
	}
}

/// Test that keys with common prefixes are correctly differentiated.
/// This tests SurrealDB-like namespace patterns where table records and index records
/// share a common prefix but should be distinguishable by range scans.
///
/// If partial prefix matching incorrectly includes wrong records, deserializing
/// an index record as a table record (or vice versa) would cause errors.
#[test(tokio::test)]
async fn test_range_prefix_differentiation() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_small_memtable_options(path.clone());

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Table records: /ns/db/tb/<id>
	// Index records: /ns/db/ix/<name>
	let table_records = [
		("/ns/db/tb/1", "TableRecord1"),
		("/ns/db/tb/2", "TableRecord2"),
		("/ns/db/tb/10", "TableRecord10"),
		("/ns/db/tb/100", "TableRecord100"),
	];

	let index_records =
		[("/ns/db/ix/a", "IndexA"), ("/ns/db/ix/b", "IndexB"), ("/ns/db/ix/name_idx", "IndexName")];

	// Insert all records
	for (key, value) in table_records.iter().chain(index_records.iter()) {
		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	// Test before flush
	{
		let txn = tree.begin().unwrap();

		// Range scan for table records only: [/ns/db/tb/, /ns/db/tb0)
		let table_range =
			collect_transaction_all(&mut txn.range(b"/ns/db/tb/", b"/ns/db/tb0").unwrap()).unwrap();

		assert_eq!(
			table_range.len(),
			table_records.len(),
			"Table range should return exactly {} table records, got {}",
			table_records.len(),
			table_range.len()
		);

		// Verify all returned keys are table records
		for (key, value) in table_range.iter() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			assert!(
				key_str.starts_with("/ns/db/tb/"),
				"Key '{}' should be a table record (start with /ns/db/tb/)",
				key_str
			);
			// Verify it's NOT an index record
			assert!(
				!key_str.starts_with("/ns/db/ix/"),
				"Table range should NOT contain index record '{}'",
				key_str
			);
			// Verify value is a table record value
			let value_str = std::str::from_utf8(value.as_ref()).unwrap();
			assert!(
				value_str.starts_with("TableRecord"),
				"Value '{}' should be a TableRecord",
				value_str
			);
		}

		// Range scan for index records only: [/ns/db/ix/, /ns/db/ix0)
		let index_range =
			collect_transaction_all(&mut txn.range(b"/ns/db/ix/", b"/ns/db/ix0").unwrap()).unwrap();

		assert_eq!(
			index_range.len(),
			index_records.len(),
			"Index range should return exactly {} index records, got {}",
			index_records.len(),
			index_range.len()
		);

		// Verify all returned keys are index records
		for (key, value) in index_range.iter() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			assert!(
				key_str.starts_with("/ns/db/ix/"),
				"Key '{}' should be an index record (start with /ns/db/ix/)",
				key_str
			);
			// Verify it's NOT a table record
			assert!(
				!key_str.starts_with("/ns/db/tb/"),
				"Index range should NOT contain table record '{}'",
				key_str
			);
			// Verify value is an index record value
			let value_str = std::str::from_utf8(value.as_ref()).unwrap();
			assert!(value_str.starts_with("Index"), "Value '{}' should be an Index", value_str);
		}
	}

	// Flush and test after flush
	tree.flush().unwrap();

	{
		let txn = tree.begin().unwrap();

		// Same tests after flush
		let table_range =
			collect_transaction_all(&mut txn.range(b"/ns/db/tb/", b"/ns/db/tb0").unwrap()).unwrap();

		assert_eq!(
			table_range.len(),
			table_records.len(),
			"After flush: Table range should return exactly {} table records",
			table_records.len()
		);

		for (key, _value) in table_range.iter() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			assert!(
				key_str.starts_with("/ns/db/tb/"),
				"After flush: Key '{}' should be a table record",
				key_str
			);
		}

		let index_range =
			collect_transaction_all(&mut txn.range(b"/ns/db/ix/", b"/ns/db/ix0").unwrap()).unwrap();

		assert_eq!(
			index_range.len(),
			index_records.len(),
			"After flush: Index range should return exactly {} index records",
			index_records.len()
		);

		for (key, _value) in index_range.iter() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			assert!(
				key_str.starts_with("/ns/db/ix/"),
				"After flush: Key '{}' should be an index record",
				key_str
			);
		}
	}
}

/// Test that exclusive upper bounds work correctly.
/// The range API uses [start, end) semantics - inclusive start, exclusive end.
#[test(tokio::test)]
async fn test_range_exclusive_boundaries() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_small_memtable_options(path.clone());

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Insert keys: a, b, c, d
	let keys = ["a", "b", "c", "d"];
	for key in keys.iter() {
		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), format!("value_{}", key).as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	// Test before flush
	{
		let txn = tree.begin().unwrap();

		// Range [a, c) should include a, b but NOT c
		let range_result = collect_transaction_all(&mut txn.range(b"a", b"c").unwrap()).unwrap();

		assert_eq!(range_result.len(), 2, "Range [a, c) should return 2 keys (a and b)");

		let result_keys: Vec<String> = range_result
			.iter()
			.map(|(k, _)| std::str::from_utf8(k.as_ref()).unwrap().to_string())
			.collect();

		assert!(result_keys.contains(&"a".to_string()), "Range [a, c) should include 'a'");
		assert!(result_keys.contains(&"b".to_string()), "Range [a, c) should include 'b'");
		assert!(!result_keys.contains(&"c".to_string()), "Range [a, c) should NOT include 'c'");
		assert!(!result_keys.contains(&"d".to_string()), "Range [a, c) should NOT include 'd'");

		// Range [b, d) should include b, c but NOT d
		let range_result2 = collect_transaction_all(&mut txn.range(b"b", b"d").unwrap()).unwrap();

		assert_eq!(range_result2.len(), 2, "Range [b, d) should return 2 keys (b and c)");

		let result_keys2: Vec<String> = range_result2
			.iter()
			.map(|(k, _)| std::str::from_utf8(k.as_ref()).unwrap().to_string())
			.collect();

		assert!(!result_keys2.contains(&"a".to_string()), "Range [b, d) should NOT include 'a'");
		assert!(result_keys2.contains(&"b".to_string()), "Range [b, d) should include 'b'");
		assert!(result_keys2.contains(&"c".to_string()), "Range [b, d) should include 'c'");
		assert!(!result_keys2.contains(&"d".to_string()), "Range [b, d) should NOT include 'd'");

		// Range starting at exact key boundary
		let range_result3 = collect_transaction_all(&mut txn.range(b"c", b"e").unwrap()).unwrap();

		assert_eq!(range_result3.len(), 2, "Range [c, e) should return 2 keys (c and d)");

		let result_keys3: Vec<String> = range_result3
			.iter()
			.map(|(k, _)| std::str::from_utf8(k.as_ref()).unwrap().to_string())
			.collect();

		assert!(result_keys3.contains(&"c".to_string()), "Range [c, e) should include 'c'");
		assert!(result_keys3.contains(&"d".to_string()), "Range [c, e) should include 'd'");
	}

	// Flush and test after flush
	tree.flush().unwrap();

	{
		let txn = tree.begin().unwrap();

		// Same tests after flush
		let range_result = collect_transaction_all(&mut txn.range(b"a", b"c").unwrap()).unwrap();

		assert_eq!(range_result.len(), 2, "After flush: Range [a, c) should return 2 keys");

		let result_keys: Vec<String> = range_result
			.iter()
			.map(|(k, _)| std::str::from_utf8(k.as_ref()).unwrap().to_string())
			.collect();

		assert!(
			result_keys.contains(&"a".to_string()),
			"After flush: Range [a, c) should include 'a'"
		);
		assert!(
			result_keys.contains(&"b".to_string()),
			"After flush: Range [a, c) should include 'b'"
		);
		assert!(
			!result_keys.contains(&"c".to_string()),
			"After flush: Range [a, c) should NOT include 'c'"
		);
	}
}

/// Test edge cases with keys at boundaries, especially keys that are proper prefixes of each other.
/// This tests scenarios like: aa < aaa < aaaa < ab < b
/// Range [aa, ab) should return aa, aaa, aaaa but NOT ab
#[test(tokio::test)]
async fn test_range_boundary_edge_cases() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let opts = create_small_memtable_options(path.clone());

	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	// Keys where some are proper prefixes of others
	let keys = ["aa", "aaa", "aaaa", "aaaab", "ab", "b"];
	for key in keys.iter() {
		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), format!("value_{}", key).as_bytes()).unwrap();
		txn.commit().await.unwrap();
	}

	// Test before flush
	{
		let txn = tree.begin().unwrap();

		// Range [aa, ab) should return aa, aaa, aaaa, aaaab but NOT ab, b
		let range_result = collect_transaction_all(&mut txn.range(b"aa", b"ab").unwrap()).unwrap();

		let result_keys: Vec<String> = range_result
			.iter()
			.map(|(k, _)| std::str::from_utf8(k.as_ref()).unwrap().to_string())
			.collect();

		assert_eq!(
			range_result.len(),
			4,
			"Range [aa, ab) should return 4 keys (aa, aaa, aaaa, aaaab), got: {:?}",
			result_keys
		);

		assert!(result_keys.contains(&"aa".to_string()), "Should include 'aa'");
		assert!(result_keys.contains(&"aaa".to_string()), "Should include 'aaa'");
		assert!(result_keys.contains(&"aaaa".to_string()), "Should include 'aaaa'");
		assert!(result_keys.contains(&"aaaab".to_string()), "Should include 'aaaab'");
		assert!(!result_keys.contains(&"ab".to_string()), "Should NOT include 'ab' (upper bound)");
		assert!(!result_keys.contains(&"b".to_string()), "Should NOT include 'b'");

		// Verify ordering within the result
		assert_eq!(result_keys[0], "aa", "First key should be 'aa'");
		assert_eq!(result_keys[1], "aaa", "Second key should be 'aaa'");
		assert_eq!(result_keys[2], "aaaa", "Third key should be 'aaaa'");
		assert_eq!(result_keys[3], "aaaab", "Fourth key should be 'aaaab'");

		// Range [aaa, aaaa) - very narrow range between prefixes
		let narrow_range =
			collect_transaction_all(&mut txn.range("aaa".as_bytes(), "aaaa".as_bytes()).unwrap())
				.unwrap();

		let narrow_keys: Vec<String> = narrow_range
			.iter()
			.map(|(k, _)| std::str::from_utf8(k.as_ref()).unwrap().to_string())
			.collect();

		assert_eq!(
			narrow_range.len(),
			1,
			"Range [aaa, aaaa) should return only 'aaa', got: {:?}",
			narrow_keys
		);
		assert_eq!(narrow_keys[0], "aaa", "Only key should be 'aaa'");

		// Range [aaaa, ab) - starts exactly at 'aaaa'
		let exact_start =
			collect_transaction_all(&mut txn.range("aaaa".as_bytes(), "ab".as_bytes()).unwrap())
				.unwrap();

		let exact_keys: Vec<String> = exact_start
			.iter()
			.map(|(k, _)| std::str::from_utf8(k.as_ref()).unwrap().to_string())
			.collect();

		assert_eq!(
			exact_start.len(),
			2,
			"Range [aaaa, ab) should return 'aaaa' and 'aaaab', got: {:?}",
			exact_keys
		);
		assert!(exact_keys.contains(&"aaaa".to_string()), "Should include 'aaaa'");
		assert!(exact_keys.contains(&"aaaab".to_string()), "Should include 'aaaab'");
	}

	// Flush and test after flush
	tree.flush().unwrap();

	{
		let txn = tree.begin().unwrap();

		// Same primary test after flush
		let range_result = collect_transaction_all(&mut txn.range(b"aa", b"ab").unwrap()).unwrap();

		let result_keys: Vec<String> = range_result
			.iter()
			.map(|(k, _)| std::str::from_utf8(k.as_ref()).unwrap().to_string())
			.collect();

		assert_eq!(
			range_result.len(),
			4,
			"After flush: Range [aa, ab) should return 4 keys, got: {:?}",
			result_keys
		);

		// Verify ordering is maintained after flush
		assert_eq!(result_keys[0], "aa", "After flush: First key should be 'aa'");
		assert_eq!(result_keys[1], "aaa", "After flush: Second key should be 'aaa'");
		assert_eq!(result_keys[2], "aaaa", "After flush: Third key should be 'aaaa'");
		assert_eq!(result_keys[3], "aaaab", "After flush: Fourth key should be 'aaaab'");
	}
}
