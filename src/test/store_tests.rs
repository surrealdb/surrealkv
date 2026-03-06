use std::collections::HashMap;

use tempdir::TempDir;
use test_log::test;

use crate::test::{
	collect_history_all,
	collect_transaction_all,
	collect_transaction_reverse,
	point_in_time_from_history,
	KeyVersionsMap,
};
use crate::{Key, LSMIterator, Options, Tree, TreeBuilder, Value};

fn create_temp_directory() -> TempDir {
	TempDir::new("test").unwrap()
}

// Common setup logic for creating a store
fn create_store() -> (Tree, TempDir) {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let tree = TreeBuilder::new().with_path(path).build().unwrap();
	(tree, temp_dir)
}

fn create_versioned_store() -> (Tree, TempDir) {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();
	let opts = Options::new().with_path(path).with_versioning(true, 0);
	let tree = TreeBuilder::with_options(opts).build().unwrap();
	(tree, temp_dir)
}

#[test(tokio::test)]
async fn basic_store_operations() {
	let (store, _temp_dir) = create_store();

	// Define key-value pairs for the test
	let key1 = b"foo1";
	let key2 = b"foo2";
	let value1 = b"baz";
	let value2 = b"bar";

	// Write initial values
	{
		let mut batch = store.new_batch();
		batch.set(key1, value1).unwrap();
		batch.set(key2, value1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	// Read back and verify
	{
		let val = store.get(key1).unwrap().unwrap();
		assert_eq!(val.as_slice(), value1);
	}

	// Overwrite with new values
	{
		let mut batch = store.new_batch();
		batch.set(key1, value2).unwrap();
		batch.set(key2, value2).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	// Read back and verify updated values
	let val = store.get(key1).unwrap().unwrap();
	assert_eq!(val.as_slice(), value2);
}

#[test(tokio::test)]
async fn sdb_delete_record_id_bug() {
	let (store, _) = create_store();

	let key1 = Vec::from(&[
		47, 33, 110, 100, 166, 192, 229, 30, 101, 24, 73, 242, 185, 36, 233, 242, 54, 96, 72, 52,
	]);
	let key2 = Vec::from(&[
		47, 33, 104, 98, 0, 0, 1, 141, 141, 42, 113, 8, 47, 166, 192, 229, 30, 101, 24, 73, 242,
		185, 36, 233, 242, 54, 96, 72, 52,
	]);
	let value1 = b"baz";

	// Write initial keys
	{
		let mut batch = store.new_batch();
		batch.set(&key1, value1).unwrap();
		batch.set(&key2, value1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let key3 = Vec::from(&[47, 33, 117, 115, 114, 111, 111, 116, 0]);
	store.set(&key3, value1).await.unwrap();

	// Verify key3 is readable
	let val = store.get(&key3).unwrap();
	assert!(val.is_some());

	// Write more complex keys
	{
		let ns_key = Vec::from(&[47, 33, 110, 115, 116, 101, 115, 116, 45, 110, 115, 0]);
		let db_key = Vec::from(&[
			47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72, 71,
			72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
		]);
		let user_key = Vec::from(&[
			47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72, 50,
			50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 42, 117, 115,
			101, 114, 0, 42, 0, 0, 0, 1, 106, 111, 104, 110, 0,
		]);

		let mut batch = store.new_batch();
		batch.set(&ns_key, value1).unwrap();
		batch.set(&db_key, value1).unwrap();
		batch.set(&user_key, value1).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	// Delete and re-set in another batch
	{
		let user_key = Vec::from(&[
			47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72, 50,
			50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 42, 117, 115,
			101, 114, 0, 42, 0, 0, 0, 1, 106, 111, 104, 110, 0,
		]);
		let db_key = Vec::from(&[
			47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72, 71,
			72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
		]);

		let mut batch = store.new_batch();
		batch.delete(&user_key).unwrap();
		batch.set(&db_key, value1).unwrap();
		store.apply(batch, false).await.unwrap();
	}
}

#[test(tokio::test)]
async fn store_delete_from_index() {
	let (store, _) = create_store();

	let key1 = b"foo1";
	let value = b"baz";
	let key2 = b"foo2";

	// Write initial data
	{
		let mut batch = store.new_batch();
		batch.set(key1, value).unwrap();
		batch.set(key2, value).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	// Delete key1
	store.delete(key1).await.unwrap();

	// Verify key1 is deleted and key2 remains
	assert!(store.get(key1).unwrap().is_none());
	assert_eq!(store.get(key2).unwrap().unwrap().as_slice(), value);
}

#[test(tokio::test)]
async fn test_insert_delete_read_key() {
	let (store, _) = create_store();

	let key = b"test_key";
	let value1 = b"test_value1";
	let value2 = b"test_value2";

	// Insert key-value pair
	store.set(key, value1).await.unwrap();

	// Update with new value
	store.set(key, value2).await.unwrap();

	// Delete the key
	store.delete(key).await.unwrap();

	// Verify it does not exist
	assert!(store.get(key).unwrap().is_none());
}

#[test(tokio::test)]
async fn test_range_basic_functionality() {
	let (store, _temp_dir) = create_store();

	// Insert some initial data
	{
		let mut batch = store.new_batch();
		batch.set(b"key1", b"value1").unwrap();
		batch.set(b"key2", b"value2").unwrap();
		batch.set(b"key3", b"value3").unwrap();
		batch.set(b"key4", b"value4").unwrap();
		batch.set(b"key5", b"value5").unwrap();
		store.apply(batch, false).await.unwrap();
	}

	// Test basic range scan
	{
		let snap = store.new_snapshot();
		let range = collect_transaction_all(&mut snap.range(Some(b"key2"), Some(b"key4")).unwrap())
			.unwrap();

		assert_eq!(range.len(), 2); // key2, key3 (key4 is exclusive)
		assert_eq!(&range[0].0, b"key2");
		assert_eq!(&range[0].1.as_slice(), b"value2");
		assert_eq!(&range[1].0, b"key3");
		assert_eq!(&range[1].1.as_slice(), b"value3");
	}
}

#[test(tokio::test)]
async fn test_range_with_bounds() {
	let (store, _temp_dir) = create_store();

	// Insert some initial data
	{
		let mut batch = store.new_batch();
		batch.set(b"key1", b"value1").unwrap();
		batch.set(b"key2", b"value2").unwrap();
		batch.set(b"key3", b"value3").unwrap();
		batch.set(b"key4", b"value4").unwrap();
		batch.set(b"key5", b"value5").unwrap();
		store.apply(batch, false).await.unwrap();
	}

	// Test range with start bound as empty (None = unbounded)
	{
		let snap = store.new_snapshot();
		let range = collect_transaction_all(&mut snap.range(None, Some(b"key4")).unwrap()).unwrap();
		assert_eq!(range.len(), 3); // key1, key2, key3 (key4 is exclusive)
		assert_eq!(&range[0].0, b"key1");
		assert_eq!(&range[1].0, b"key2");
		assert_eq!(&range[2].0, b"key3");
	}
}

#[test(tokio::test)]
async fn test_range_with_limit() {
	let (store, _temp_dir) = create_store();

	// Insert test data
	{
		let mut batch = store.new_batch();
		for i in 1..=10 {
			let key = format!("key{i:02}");
			let value = format!("value{i}");
			batch.set(key.as_bytes(), value.as_bytes()).unwrap();
		}
		store.apply(batch, false).await.unwrap();
	}

	// Test with .take()
	{
		let snap = store.new_snapshot();
		let all_range =
			collect_transaction_all(&mut snap.range(Some(b"key01"), Some(b"key10")).unwrap())
				.unwrap();
		let range: Vec<_> = all_range.into_iter().take(3).collect::<Vec<_>>();

		assert_eq!(range.len(), 3);
		assert_eq!(&range[0].0, b"key01");
		assert_eq!(&range[1].0, b"key02");
		assert_eq!(&range[2].0, b"key03");
	}
}

#[test(tokio::test)]
async fn test_range_empty_result() {
	let (store, _temp_dir) = create_store();

	// Insert data outside the range we'll query
	{
		let mut batch = store.new_batch();
		batch.set(b"a", b"1").unwrap();
		batch.set(b"z", b"26").unwrap();
		store.apply(batch, false).await.unwrap();
	}

	// Query range with no data
	{
		let snap = store.new_snapshot();
		let range =
			collect_transaction_all(&mut snap.range(Some(b"m"), Some(b"n")).unwrap()).unwrap();
		assert_eq!(range.len(), 0);
	}
}

#[test(tokio::test)]
async fn test_range_ordering() {
	let (store, _temp_dir) = create_store();

	// Insert data in non-sequential order
	{
		let mut batch = store.new_batch();
		batch.set(b"key5", b"value5").unwrap();
		batch.set(b"key1", b"value1").unwrap();
		batch.set(b"key3", b"value3").unwrap();
		batch.set(b"key2", b"value2").unwrap();
		batch.set(b"key4", b"value4").unwrap();
		store.apply(batch, false).await.unwrap();
	}

	// Verify correct ordering in range
	{
		let snap = store.new_snapshot();
		let range = collect_transaction_all(&mut snap.range(Some(b"key1"), Some(b"key6")).unwrap())
			.unwrap();

		assert_eq!(range.len(), 5);
		for (i, item) in range.iter().enumerate().take(5) {
			let expected_key = format!("key{}", i + 1);
			assert_eq!(&item.0, expected_key.as_bytes());
		}
	}
}

#[test(tokio::test)]
async fn test_range_boundary_conditions() {
	let (store, _temp_dir) = create_store();

	// Insert test data
	{
		let mut batch = store.new_batch();
		batch.set(b"key1", b"value1").unwrap();
		batch.set(b"key2", b"value2").unwrap();
		batch.set(b"key3", b"value3").unwrap();
		store.apply(batch, false).await.unwrap();
	}

	// Test range boundaries (inclusive start, exclusive end)
	{
		let snap = store.new_snapshot();
		let range = collect_transaction_all(&mut snap.range(Some(b"key1"), Some(b"key4")).unwrap())
			.unwrap();

		assert_eq!(range.len(), 3);
		assert_eq!(&range[0].0, b"key1");
		assert_eq!(&range[2].0, b"key3");
	}

	// Test single key range
	{
		let snap = store.new_snapshot();
		let range = collect_transaction_all(&mut snap.range(Some(b"key2"), Some(b"key3")).unwrap())
			.unwrap();

		assert_eq!(range.len(), 1);
		assert_eq!(&range[0].0, b"key2");
	}
}

#[test(tokio::test)]
async fn test_range_value_pointer_resolution_bug() {
	let temp_dir = create_temp_directory();

	let tree = TreeBuilder::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_max_memtable_size(64 * 1024)
		.build()
		.unwrap();

	// Create values that will be stored in VLog (> 50 bytes)
	let key1 = b"key1";
	let key2 = b"key2";
	let key3 = b"key3";

	let large_value1 = "X".repeat(100);
	let large_value2 = "Y".repeat(100);
	let large_value3 = "Z".repeat(100);

	// Insert the values
	{
		let mut batch = tree.new_batch();
		batch.set(key1, large_value1.as_bytes()).unwrap();
		batch.set(key2, large_value2.as_bytes()).unwrap();
		batch.set(key3, large_value3.as_bytes()).unwrap();
		tree.apply(batch, true).await.unwrap();
	}

	// Write enough data to trigger memtable flush
	for i in 0..100 {
		let k = format!("pad_{i:04}");
		let v = "P".repeat(1024);
		tree.set(k.as_bytes(), v.as_bytes()).await.unwrap();
	}

	// Test 1: Verify get() works correctly
	{
		let retrieved1 = tree.get(key1).unwrap().unwrap();
		let retrieved2 = tree.get(key2).unwrap().unwrap();
		let retrieved3 = tree.get(key3).unwrap().unwrap();

		assert_eq!(
			retrieved1.as_slice(),
			large_value1.as_bytes(),
			"get() should resolve value pointers correctly"
		);
		assert_eq!(
			retrieved2.as_slice(),
			large_value2.as_bytes(),
			"get() should resolve value pointers correctly"
		);
		assert_eq!(
			retrieved3.as_slice(),
			large_value3.as_bytes(),
			"get() should resolve value pointers correctly"
		);
	}

	// Test 2: Verify range() also works correctly
	{
		let snap = tree.new_snapshot();
		let range_results =
			collect_transaction_all(&mut snap.range(Some(b"key1"), Some(b"key4")).unwrap())
				.unwrap();

		assert_eq!(range_results.len(), 3, "Should get 3 items from range query");

		for (i, (returned_key, returned_value)) in range_results.iter().enumerate() {
			let expected_key = match i {
				0 => &key1[..],
				1 => &key2[..],
				2 => &key3[..],
				_ => panic!("Unexpected index"),
			};
			let expected_value = match i {
				0 => &large_value1,
				1 => &large_value2,
				2 => &large_value3,
				_ => panic!("Unexpected index"),
			};

			assert_eq!(returned_key.as_slice(), expected_key, "Key mismatch in range result");

			assert_eq!(
				returned_value.as_slice(),
				expected_value.as_bytes(),
				"Range should return resolved values, not value pointers. \
				Expected actual value of {} bytes, but got a different value",
				expected_value.len(),
			);
		}
	}
}

// Double-ended iterator tests
mod double_ended_iterator_tests {
	use test_log::test;

	use super::*;

	#[test(tokio::test)]
	async fn test_reverse_iteration_basic() {
		let (store, _temp_dir) = create_store();

		// Insert test data
		{
			let mut batch = store.new_batch();
			batch.set(b"key1", b"value1").unwrap();
			batch.set(b"key2", b"value2").unwrap();
			batch.set(b"key3", b"value3").unwrap();
			batch.set(b"key4", b"value4").unwrap();
			batch.set(b"key5", b"value5").unwrap();
			store.apply(batch, false).await.unwrap();
		}

		// Test reverse iteration
		{
			let snap = store.new_snapshot();
			let reverse_results =
				collect_transaction_reverse(&mut snap.range(Some(b"key1"), Some(b"key6")).unwrap())
					.unwrap();

			assert_eq!(reverse_results.len(), 5);

			let keys: Vec<Vec<u8>> = reverse_results.into_iter().map(|(k, _)| k).collect();

			assert_eq!(keys[0], b"key5");
			assert_eq!(keys[1], b"key4");
			assert_eq!(keys[2], b"key3");
			assert_eq!(keys[3], b"key2");
			assert_eq!(keys[4], b"key1");
		}
	}

	#[test(tokio::test)]
	async fn test_reverse_iteration_with_limits() {
		let (store, _temp_dir) = create_store();

		// Insert test data
		{
			let mut batch = store.new_batch();
			for i in 1..=10 {
				let key = format!("key{:02}", i);
				let value = format!("value{}", i);
				batch.set(key.as_bytes(), value.as_bytes()).unwrap();
			}
			store.apply(batch, false).await.unwrap();
		}

		// Test reverse iteration with take
		{
			let snap = store.new_snapshot();
			let reverse_results = collect_transaction_reverse(
				&mut snap.range(Some(b"key01"), Some(b"key11")).unwrap(),
			)
			.unwrap();

			let limited_results: Vec<_> = reverse_results.into_iter().take(3).collect();

			assert_eq!(limited_results.len(), 3);

			let keys: Vec<Vec<u8>> = limited_results.into_iter().map(|(k, _)| k).collect();

			assert_eq!(keys[0], b"key10");
			assert_eq!(keys[1], b"key09");
			assert_eq!(keys[2], b"key08");
		}
	}

	#[test(tokio::test)]
	async fn test_reverse_iteration_keys_only() {
		let (store, _temp_dir) = create_store();

		// Insert test data
		{
			let mut batch = store.new_batch();
			batch.set(b"key1", b"value1").unwrap();
			batch.set(b"key2", b"value2").unwrap();
			batch.set(b"key3", b"value3").unwrap();
			store.apply(batch, false).await.unwrap();
		}

		// Test reverse iteration with keys only
		{
			let snap = store.new_snapshot();
			let reverse_results =
				collect_transaction_reverse(&mut snap.range(Some(b"key1"), Some(b"key4")).unwrap())
					.unwrap();

			assert_eq!(reverse_results.len(), 3);

			let keys: Vec<Vec<u8>> = reverse_results.into_iter().map(|(k, _)| k).collect();

			assert_eq!(keys[0], b"key3");
			assert_eq!(keys[1], b"key2");
			assert_eq!(keys[2], b"key1");
		}
	}

	#[test(tokio::test)]
	async fn test_reverse_iteration_empty_range() {
		let (store, _temp_dir) = create_store();

		// Insert data outside the range we'll query
		{
			let mut batch = store.new_batch();
			batch.set(b"key1", b"value1").unwrap();
			batch.set(b"key5", b"value5").unwrap();
			store.apply(batch, false).await.unwrap();
		}

		// Test reverse iteration on empty range
		{
			let snap = store.new_snapshot();
			let reverse_results =
				collect_transaction_reverse(&mut snap.range(Some(b"key2"), Some(b"key5")).unwrap())
					.unwrap();

			assert_eq!(reverse_results.len(), 0);
		}
	}

	#[test(tokio::test)]
	async fn test_reverse_iteration_consistency_with_forward() {
		let (store, _temp_dir) = create_store();

		// Insert test data
		{
			let mut batch = store.new_batch();
			for i in 1..=5 {
				let key = format!("key{}", i);
				let value = format!("value{}", i);
				batch.set(key.as_bytes(), value.as_bytes()).unwrap();
			}
			store.apply(batch, false).await.unwrap();
		}

		// Test that reverse iteration gives same results as forward iteration reversed
		{
			let snap = store.new_snapshot();

			// Forward iteration
			let forward_results =
				collect_transaction_all(&mut snap.range(Some(b"key1"), Some(b"key5")).unwrap())
					.unwrap();

			// Reverse iteration
			let reverse_results =
				collect_transaction_reverse(&mut snap.range(Some(b"key1"), Some(b"key5")).unwrap())
					.unwrap();

			// Reverse the forward results
			let mut forward_reversed = forward_results;
			forward_reversed.reverse();

			// Results should be identical
			assert_eq!(forward_reversed.len(), reverse_results.len());
			for (forward, reverse) in forward_reversed.iter().zip(reverse_results.iter()) {
				assert_eq!(forward.0, reverse.0);
				assert_eq!(forward.1, reverse.1);
			}
		}
	}
}

// ============================================================================
// Versioned query tests
// ============================================================================

#[test(tokio::test)]
async fn test_versioned_queries_basic() {
	let (tree, _temp_dir) = create_versioned_store();

	// Use explicit timestamps for better testing
	let ts1 = 100;
	let ts2 = 200;

	// Insert data with explicit timestamps
	tree.set_at(b"key1", b"value1_v1", ts1).await.unwrap();
	tree.set_at(b"key1", b"value1_v2", ts2).await.unwrap();

	// Test regular get (should return latest)
	let value = tree.get(b"key1").unwrap();
	assert_eq!(value, Some(Vec::from(b"value1_v2")));

	// Get all versions using history_iter() to verify timestamps and values
	let snap = tree.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
	let versions = collect_history_all(&mut iter).unwrap();
	assert_eq!(versions.len(), 2);

	// Find versions by timestamp
	let v1 = versions.iter().find(|(_, _, timestamp, _)| *timestamp == ts1).unwrap();
	let v2 = versions.iter().find(|(_, _, timestamp, _)| *timestamp == ts2).unwrap();

	// Verify values match timestamps
	assert_eq!(&v1.1, b"value1_v1");
	assert_eq!(&v2.1, b"value1_v2");

	// Test get at specific timestamp (earlier version)
	let value_at_ts1 = snap.get_at(b"key1", ts1).unwrap();
	assert_eq!(value_at_ts1, Some(Vec::from(b"value1_v1")));

	// Test get at later timestamp (should return latest version as of that time)
	let value_at_ts2 = snap.get_at(b"key1", ts2).unwrap();
	assert_eq!(value_at_ts2, Some(Vec::from(b"value1_v2")));
}

#[test(tokio::test)]
async fn test_set_at_timestamp() {
	let (tree, _temp_dir) = create_versioned_store();

	// Set a value with a specific timestamp
	let custom_timestamp = 10;
	tree.set_at(b"key1", b"value1", custom_timestamp).await.unwrap();

	// Verify we can get the value at that timestamp
	let snap = tree.new_snapshot();
	let value = snap.get_at(b"key1", custom_timestamp).unwrap();
	assert_eq!(value, Some(Vec::from(b"value1")));

	// Verify we can get the value at a later timestamp
	let later_timestamp = custom_timestamp + 1000000;
	let value = snap.get_at(b"key1", later_timestamp).unwrap();
	assert_eq!(value, Some(Vec::from(b"value1")));

	// Verify we can't get the value at an earlier timestamp
	let earlier_timestamp = custom_timestamp - 5;
	let value = snap.get_at(b"key1", earlier_timestamp).unwrap();
	assert_eq!(value, None);

	// Verify using history_iter() to check the timestamp
	let mut iter = snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
	let versions = collect_history_all(&mut iter).unwrap();
	assert_eq!(versions.len(), 1);
	assert_eq!(versions[0].2, custom_timestamp);
	assert_eq!(&versions[0].1, b"value1");
}

#[test(tokio::test)]
async fn test_timestamp_via_batch_set_at() {
	let (tree, _temp_dir) = create_versioned_store();

	// Test setting a value with timestamp via batch set_at
	let custom_timestamp = 100;
	{
		let mut batch = tree.new_batch();
		batch.set_at(b"key1", b"value1", custom_timestamp).unwrap();
		tree.apply(batch, false).await.unwrap();
	}

	// Verify we can read it at that timestamp
	let snap = tree.new_snapshot();
	let value = snap.get_at(b"key1", custom_timestamp).unwrap();
	assert_eq!(value, Some(Vec::from(b"value1")));

	// Test delete_at with timestamp (uses hard delete)
	let delete_timestamp = 200;
	{
		let mut batch = tree.new_batch();
		batch.delete_at(b"key1", delete_timestamp).unwrap();
		tree.apply(batch, false).await.unwrap();
	}

	// With HARD_DELETE barrier semantics, all historical versions become inaccessible
	// after a hard delete. Both queries should return None.
	let snap = tree.new_snapshot();
	let value_before = snap.get_at(b"key1", custom_timestamp).unwrap();
	assert_eq!(value_before, None); // Key was hard-deleted, all versions inaccessible

	let value_after = snap.get_at(b"key1", delete_timestamp).unwrap();
	assert_eq!(value_after, None);
}

#[test(tokio::test)]
async fn test_commit_timestamp_consistency() {
	let (tree, _temp_dir) = create_versioned_store();

	// Set multiple values in a single batch
	{
		let mut batch = tree.new_batch();
		batch.set(b"key1", b"value1").unwrap();
		batch.set(b"key2", b"value2").unwrap();
		batch.set(b"key3", b"value3").unwrap();
		tree.apply(batch, false).await.unwrap();
	}

	// All keys should have the same timestamp
	{
		let snap = tree.new_snapshot();
		let mut iter1 = snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
		let versions1 = collect_history_all(&mut iter1).unwrap();
		let mut iter2 = snap.history_iter(Some(b"key2"), Some(b"key3"), false, None, None).unwrap();
		let versions2 = collect_history_all(&mut iter2).unwrap();
		let mut iter3 = snap.history_iter(Some(b"key3"), Some(b"key4"), false, None, None).unwrap();
		let versions3 = collect_history_all(&mut iter3).unwrap();

		assert_eq!(versions1.len(), 1);
		assert_eq!(versions2.len(), 1);
		assert_eq!(versions3.len(), 1);

		let ts1 = versions1[0].2;
		let ts2 = versions2[0].2;
		let ts3 = versions3[0].2;
		assert_eq!(ts1, ts2);
		assert_eq!(ts2, ts3);
	}

	// Test mixed explicit and implicit timestamps
	let custom_timestamp = 9876543210000000000;
	{
		let mut batch = tree.new_batch();
		batch.set(b"key4", b"value4").unwrap(); // Will get commit timestamp
		batch.set_at(b"key5", b"value5", custom_timestamp).unwrap(); // Explicit timestamp
		batch.set(b"key6", b"value6").unwrap(); // Will get commit timestamp
		tree.apply(batch, false).await.unwrap();
	}

	let snap = tree.new_snapshot();
	let mut iter4 = snap.history_iter(Some(b"key4"), Some(b"key5"), false, None, None).unwrap();
	let versions4 = collect_history_all(&mut iter4).unwrap();
	let mut iter5 = snap.history_iter(Some(b"key5"), Some(b"key6"), false, None, None).unwrap();
	let versions5 = collect_history_all(&mut iter5).unwrap();
	let mut iter6 = snap.history_iter(Some(b"key6"), Some(b"key7"), false, None, None).unwrap();
	let versions6 = collect_history_all(&mut iter6).unwrap();

	assert_eq!(versions4.len(), 1);
	assert_eq!(versions5.len(), 1);
	assert_eq!(versions6.len(), 1);

	let ts4 = versions4[0].2;
	let ts5 = versions5[0].2;
	let ts6 = versions6[0].2;

	// key4 and key6 should have the same timestamp (commit timestamp)
	assert_eq!(ts4, ts6);

	// key5 should have the custom timestamp
	assert_eq!(ts5, custom_timestamp);

	// key4/key6 timestamp should be different from key5 timestamp
	assert_ne!(ts4, ts5);
}

#[test(tokio::test)]
async fn test_range_at_version() {
	let (tree, _temp_dir) = create_versioned_store();

	let ts1 = 100;
	let ts2 = 200;

	// Insert data with first timestamp
	{
		let mut batch = tree.new_batch();
		batch.set_at(b"key1", b"value1", ts1).unwrap();
		batch.set_at(b"key2", b"value2", ts1).unwrap();
		batch.set_at(b"key3", b"value3", ts1).unwrap();
		tree.apply(batch, false).await.unwrap();
	}

	// Insert data with second timestamp
	{
		let mut batch = tree.new_batch();
		batch.set_at(b"key2", b"value2_updated", ts2).unwrap();
		batch.set_at(b"key4", b"value4", ts2).unwrap();
		tree.apply(batch, false).await.unwrap();
	}

	// Test point-in-time query at first timestamp
	let snap = tree.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key1"), Some(b"key5"), false, None, None).unwrap();
	let scan_at_ts1 = point_in_time_from_history(&mut iter, ts1).unwrap();
	assert_eq!(scan_at_ts1.len(), 3);

	let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
	for (key, value) in &scan_at_ts1 {
		found_keys.insert(key.as_ref());
		match key.as_slice() {
			b"key1" => assert_eq!(value.as_slice(), b"value1"),
			b"key2" => assert_eq!(value.as_slice(), b"value2"),
			b"key3" => assert_eq!(value.as_slice(), b"value3"),
			_ => panic!("Unexpected key: {:?}", key),
		}
	}
	assert!(found_keys.contains(&b"key1".as_ref()));
	assert!(found_keys.contains(&b"key2".as_ref()));
	assert!(found_keys.contains(&b"key3".as_ref()));
	assert!(!found_keys.contains(&b"key4".as_ref()));

	// Test point-in-time query at second timestamp
	let mut iter = snap.history_iter(Some(b"key1"), Some(b"key5"), false, None, None).unwrap();
	let scan_at_ts2 = point_in_time_from_history(&mut iter, ts2).unwrap();
	assert_eq!(scan_at_ts2.len(), 4);

	let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
	for (key, value) in &scan_at_ts2 {
		found_keys.insert(key.as_ref());
		match key.as_slice() {
			b"key1" => assert_eq!(value.as_slice(), b"value1"),
			b"key2" => assert_eq!(value.as_slice(), b"value2_updated"),
			b"key3" => assert_eq!(value.as_slice(), b"value3"),
			b"key4" => assert_eq!(value.as_slice(), b"value4"),
			_ => panic!("Unexpected key: {:?}", key),
		}
	}
	assert!(found_keys.contains(&b"key1".as_ref()));
	assert!(found_keys.contains(&b"key2".as_ref()));
	assert!(found_keys.contains(&b"key3".as_ref()));
	assert!(found_keys.contains(&b"key4".as_ref()));

	// Test with specific key range
	let mut iter = snap.history_iter(Some(b"key2"), Some(b"key4"), false, None, None).unwrap();
	let scan_range = point_in_time_from_history(&mut iter, ts2).unwrap();
	assert_eq!(scan_range.len(), 2);
	let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
	for (key, _) in &scan_range {
		found_keys.insert(key.as_ref());
	}
	assert!(found_keys.contains(&b"key2".as_ref()));
	assert!(found_keys.contains(&b"key3".as_ref()));
}

#[test(tokio::test)]
async fn test_versioned_range_bounds_edge_cases() {
	let (tree, _temp_dir) = create_versioned_store();

	let ts = 100;

	// Insert data: keys a, b, c, d, e
	{
		let mut batch = tree.new_batch();
		batch.set_at(b"a", b"value_a", ts).unwrap();
		batch.set_at(b"b", b"value_b", ts).unwrap();
		batch.set_at(b"c", b"value_c", ts).unwrap();
		batch.set_at(b"d", b"value_d", ts).unwrap();
		batch.set_at(b"e", b"value_e", ts).unwrap();
		tree.apply(batch, false).await.unwrap();
	}

	let snap = tree.new_snapshot();

	// Test 1: Range includes exact start key (b to e should include b, c, d but not e)
	let mut iter = snap.history_iter(Some(b"b"), Some(b"e"), false, None, None).unwrap();
	let scan1 = point_in_time_from_history(&mut iter, ts).unwrap();
	assert_eq!(scan1.len(), 3);
	let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
	for (key, _) in &scan1 {
		found_keys.insert(key.as_ref());
	}
	assert!(found_keys.contains(&b"b".as_ref()));
	assert!(found_keys.contains(&b"c".as_ref()));
	assert!(found_keys.contains(&b"d".as_ref()));
	assert!(!found_keys.contains(&b"e".as_ref()));

	// Test 2: Range with start key not in data
	let mut iter = snap.history_iter(Some(b"aa"), Some(b"cc"), false, None, None).unwrap();
	let scan2 = point_in_time_from_history(&mut iter, ts).unwrap();
	assert_eq!(scan2.len(), 2);
	let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
	for (key, _) in &scan2 {
		found_keys.insert(key.as_ref());
	}
	assert!(found_keys.contains(&b"b".as_ref()));
	assert!(found_keys.contains(&b"c".as_ref()));
	assert!(!found_keys.contains(&b"a".as_ref()));

	// Test 3: Range with end key not in data
	let mut iter = snap.history_iter(Some(b"b"), Some(b"dd"), false, None, None).unwrap();
	let scan3 = point_in_time_from_history(&mut iter, ts).unwrap();
	assert_eq!(scan3.len(), 3);
	let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
	for (key, _) in &scan3 {
		found_keys.insert(key.as_ref());
	}
	assert!(found_keys.contains(&b"b".as_ref()));
	assert!(found_keys.contains(&b"c".as_ref()));
	assert!(found_keys.contains(&b"d".as_ref()));

	// Test 4: Empty range (start >= end should return empty)
	let mut iter = snap.history_iter(Some(b"d"), Some(b"b"), false, None, None).unwrap();
	let scan4 = point_in_time_from_history(&mut iter, ts).unwrap();
	assert_eq!(scan4.len(), 0);

	// Test 5: Single key range (b to c should include only b)
	let mut iter = snap.history_iter(Some(b"b"), Some(b"c"), false, None, None).unwrap();
	let scan5 = point_in_time_from_history(&mut iter, ts).unwrap();
	assert_eq!(scan5.len(), 1);
	assert_eq!(scan5[0].0.as_slice(), b"b");
}

#[test(tokio::test)]
async fn test_range_at_version_with_deletes() {
	let temp_dir = create_temp_directory();
	let opts = Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
	let clock = std::sync::Arc::clone(&opts.clock);
	let tree = TreeBuilder::with_options(opts).build().unwrap();

	// Insert data
	{
		let mut batch = tree.new_batch();
		batch.set(b"key1", b"value1").unwrap();
		batch.set(b"key2", b"value2").unwrap();
		batch.set(b"key3", b"value3").unwrap();
		tree.apply(batch, false).await.unwrap();
	}
	let ts_after_insert = clock.now();

	{
		// Query at this point should show all three keys
		let snap = tree.new_snapshot();
		let mut iter = snap.history_iter(Some(b"key1"), Some(b"key4"), true, None, None).unwrap();
		let scan_before = point_in_time_from_history(&mut iter, ts_after_insert).unwrap();
		assert_eq!(scan_before.len(), 3, "Should have all 3 keys before deletes");
	}

	// Delete key2 (hard delete)
	tree.delete(b"key2").await.unwrap();
	let ts_after_deletes = clock.now();

	// Test point-in-time query at a time after the deletes
	let snap = tree.new_snapshot();

	// Verify key2 is completely gone (hard deleted)
	let mut iter = snap.history_iter(Some(b"key2"), Some(b"key3"), false, None, None).unwrap();
	let versions2 = collect_history_all(&mut iter).unwrap();
	assert_eq!(versions2.len(), 0, "Hard deleted key should have no versions");

	// Perform scan at timestamp after deletes
	let mut iter = snap.history_iter(Some(b"key1"), Some(b"key4"), true, None, None).unwrap();
	let scan_result = point_in_time_from_history(&mut iter, ts_after_deletes).unwrap();
	assert_eq!(scan_result.len(), 2, "Should have 2 keys after hard delete");

	let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
	for (key, _) in &scan_result {
		found_keys.insert(key.as_ref());
	}
	assert!(found_keys.contains(&b"key1".as_ref()));
	assert!(!found_keys.contains(&b"key2".as_ref())); // Hard deleted
	assert!(found_keys.contains(&b"key3".as_ref()));
}

#[test(tokio::test)]
async fn test_scan_all_versions() {
	let (tree, _temp_dir) = create_versioned_store();

	// Insert data at different times
	{
		let mut batch = tree.new_batch();
		batch.set(b"key1", b"value1_v1").unwrap();
		batch.set(b"key2", b"value2_v1").unwrap();
		tree.apply(batch, false).await.unwrap();
	}

	{
		let mut batch = tree.new_batch();
		batch.set(b"key1", b"value1_v2").unwrap();
		batch.set(b"key3", b"value3_v1").unwrap();
		tree.apply(batch, false).await.unwrap();
	}

	{
		let mut batch = tree.new_batch();
		batch.set(b"key2", b"value2_v2").unwrap();
		batch.set(b"key4", b"value4_v1").unwrap();
		tree.apply(batch, false).await.unwrap();
	}

	// Test using history_iter() to get all versions
	let snap = tree.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key1"), Some(b"key5"), false, None, None).unwrap();
	let all_versions = collect_history_all(&mut iter).unwrap();

	assert_eq!(all_versions.len(), 6);

	// Group by key to verify we have all versions
	let mut key_versions: KeyVersionsMap = HashMap::new();
	for (key, value, timestamp, is_tombstone) in all_versions {
		key_versions.entry(key).or_default().push((value.clone(), timestamp, is_tombstone));
	}

	// Verify key1 has 2 versions
	let key1_versions = key_versions.get_mut(&Vec::from(b"key1")).unwrap();
	assert_eq!(key1_versions.len(), 2);
	key1_versions.sort_by_key(|a| a.1);
	assert_eq!(key1_versions[0].0, b"value1_v1");
	assert_eq!(key1_versions[1].0, b"value1_v2");

	// Verify key2 has 2 versions
	let key2_versions = key_versions.get_mut(&Vec::from(b"key2")).unwrap();
	assert_eq!(key2_versions.len(), 2);
	key2_versions.sort_by_key(|a| a.1);
	assert_eq!(key2_versions[0].0, b"value2_v1");
	assert_eq!(key2_versions[1].0, b"value2_v2");

	// Verify key3 has 1 version
	let key3_versions = &key_versions[&Vec::from(b"key3")];
	assert_eq!(key3_versions.len(), 1);
	assert_eq!(key3_versions[0].0, b"value3_v1");

	// Verify key4 has 1 version
	let key4_versions = &key_versions[&Vec::from(b"key4")];
	assert_eq!(key4_versions.len(), 1);
	assert_eq!(key4_versions[0].0, b"value4_v1");

	// Test with specific key range
	let mut iter = snap.history_iter(Some(b"key2"), Some(b"key4"), false, None, None).unwrap();
	let range_versions = collect_history_all(&mut iter).unwrap();
	assert_eq!(range_versions.len(), 3); // 2 versions of key2 + 1 version of key3
}

#[test(tokio::test)]
async fn test_versioned_queries_without_versioning() {
	let (store, _temp_dir) = create_store(); // Non-versioned store

	// Test that versioned queries fail when versioning is disabled
	let snap = store.new_snapshot();
	assert!(snap.history_iter(Some(b"key1"), Some(b"key3"), false, None, None).is_err());
	assert!(snap.get_at(b"key1", 123456789).is_err());
}

// Version management tests
mod version_tests {
	use std::collections::HashSet;

	use test_log::test;

	use super::*;

	fn create_tree() -> (Tree, TempDir) {
		create_versioned_store()
	}

	/// Helper to collect all versions using history_iter() (returns sorted by timestamp ascending)
	fn scan_all_versions_via_history(
		snap: &crate::Snapshot,
		start: &[u8],
		end: &[u8],
	) -> crate::Result<Vec<(Key, Value, u64, bool)>> {
		let mut iter = snap.history_iter(Some(start), Some(end), false, None, None)?;
		let mut results = collect_history_all(&mut iter)?;
		// Sort by (key, timestamp) ascending
		results.sort_by(|a, b| match a.0.cmp(&b.0) {
			std::cmp::Ordering::Equal => a.2.cmp(&b.2),
			other => other,
		});
		Ok(results)
	}

	#[test(tokio::test)]
	async fn test_insert_multiple_versions_in_same_tx() {
		let (store, _tmp_dir) = create_tree();
		let key = Vec::from("key1");

		// Insert multiple versions of the same key
		let values = [Vec::from("value1"), Vec::from("value2"), Vec::from("value3")];

		for (i, value) in values.iter().enumerate() {
			let version = (i + 1) as u64;
			store.set_at(&key, value, version).await.unwrap();
		}

		let snap = store.new_snapshot();
		let mut end_key = key.clone();
		end_key.push(0);
		let results = scan_all_versions_via_history(&snap, key.as_ref(), &end_key).unwrap();

		assert_eq!(results.len(), values.len());
		for (i, (k, v, version, is_deleted)) in results.iter().enumerate() {
			assert_eq!(k, &key);
			assert_eq!(v, &values[i]);
			assert_eq!(*version, (i + 1) as u64);
			assert!(!(*is_deleted));
		}
	}

	#[test(tokio::test)]
	async fn test_same_key_multiple_sets_in_single_batch_creates_versions() {
		let (store, _tmp_dir) = create_tree();
		let key = vec![218u8];

		// Single batch: set same key twice with different values
		// In LSM stores, this creates two versions (no deduplication within batch)
		{
			let mut batch = store.new_batch();
			batch.set(&key, &[254u8]).unwrap();
			batch.set(&key, &[]).unwrap();
			store.apply(batch, false).await.unwrap();
		}

		// Read back with history iterator - should see both versions
		let snap = store.new_snapshot();
		let mut end_key = key.clone();
		end_key.push(0);

		// Forward iteration: should see both versions (each set creates a version)
		let results = scan_all_versions_via_history(&snap, &key, &end_key).unwrap();
		assert_eq!(results.len(), 2, "Expected two versions (no deduplication in batch)");
		assert_eq!(results[0].0, key, "Key mismatch");
		assert_eq!(results[1].0, key, "Key mismatch");

		// The latest version (highest seq_num) should be the last write (empty value)
		// Versions are returned in timestamp order (oldest first)
		// The second entry in the batch has higher seq_num
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_single_key_multiple_versions() {
		let (store, _tmp_dir) = create_tree();
		let key = Vec::from("key1");

		let values = [Vec::from("value1"), Vec::from("value2"), Vec::from("value3")];

		for (i, value) in values.iter().enumerate() {
			let version = (i + 1) as u64;
			store.set_at(&key, value, version).await.unwrap();
		}

		let snap = store.new_snapshot();
		let mut end_key = key.clone();
		end_key.push(0);
		let results = scan_all_versions_via_history(&snap, key.as_ref(), &end_key).unwrap();

		assert_eq!(results.len(), values.len());
		for (i, (k, v, version, is_deleted)) in results.iter().enumerate() {
			assert_eq!(k, &key);
			assert_eq!(v, &values[i]);
			assert_eq!(*version, (i + 1) as u64);
			assert!(!(*is_deleted));
		}
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_multiple_keys_single_version_each() {
		let (store, _tmp_dir) = create_tree();
		let keys = vec![Vec::from("key1"), Vec::from("key2"), Vec::from("key3")];
		let value = Vec::from("value1");

		for key in &keys {
			store.set_at(key, &value, 1).await.unwrap();
		}

		let snap = store.new_snapshot();
		let mut end_key = keys.last().unwrap().clone();
		end_key.push(0);
		let results =
			scan_all_versions_via_history(&snap, keys.first().unwrap().as_ref(), &end_key).unwrap();

		assert_eq!(results.len(), keys.len());
		for (i, (k, v, version, is_deleted)) in results.iter().enumerate() {
			assert_eq!(k, &keys[i]);
			assert_eq!(v, &value);
			assert_eq!(*version, 1);
			assert!(!(*is_deleted));
		}
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_multiple_keys_multiple_versions_each() {
		let (store, _tmp_dir) = create_tree();
		let keys = vec![Vec::from("key1"), Vec::from("key2"), Vec::from("key3")];
		let values = [Vec::from("value1"), Vec::from("value2"), Vec::from("value3")];

		for key in &keys {
			for (i, value) in values.iter().enumerate() {
				let version = (i + 1) as u64;
				store.set_at(key, value, version).await.unwrap();
			}
		}

		let snap = store.new_snapshot();
		let mut end_key = keys.last().unwrap().clone();
		end_key.push(0);
		let results =
			scan_all_versions_via_history(&snap, keys.first().unwrap().as_ref(), &end_key).unwrap();

		let mut expected_results = Vec::new();
		for key in &keys {
			for (i, value) in values.iter().enumerate() {
				expected_results.push((key.clone(), value.clone(), (i + 1) as u64, false));
			}
		}

		assert_eq!(results.len(), expected_results.len());
		for (result, expected) in results.iter().zip(expected_results.iter()) {
			let (k, v, version, is_deleted) = result;
			let (expected_key, expected_value, expected_version, expected_is_deleted) = expected;
			assert_eq!(k, expected_key);
			assert_eq!(&v, &expected_value);
			assert_eq!(*version, *expected_version);
			assert_eq!(*is_deleted, *expected_is_deleted);
		}
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_range_boundaries() {
		let (store, _tmp_dir) = create_tree();
		let keys = vec![Vec::from("key1"), Vec::from("key2"), Vec::from("key3")];
		let value = Vec::from("value1");

		for key in &keys {
			store.set_at(key, &value, 1).await.unwrap();
		}

		let snap = store.new_snapshot();
		let mut end_key = keys.last().unwrap().clone();
		end_key.push(0);
		let results =
			scan_all_versions_via_history(&snap, keys.first().unwrap().as_ref(), &end_key).unwrap();
		assert_eq!(results.len(), keys.len());
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_with_limit() {
		let (store, _tmp_dir) = create_tree();
		let keys = vec![Vec::from("key1"), Vec::from("key2"), Vec::from("key3")];
		let value = Vec::from("value1");

		for key in &keys {
			store.set_at(key, &value, 1).await.unwrap();
		}

		let snap = store.new_snapshot();
		let mut end_key = keys.last().unwrap().clone();
		end_key.push(0);
		let results =
			scan_all_versions_via_history(&snap, keys.first().unwrap().as_ref(), &end_key).unwrap();

		assert!(results.len() >= 2);
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_single_key_single_version() {
		let (store, _tmp_dir) = create_tree();
		let key = Vec::from("key1");
		let value = Vec::from("value1");

		store.set_at(&key, &value, 1).await.unwrap();

		let snap = store.new_snapshot();
		let mut end_key = key.clone();
		end_key.push(0);
		let results = scan_all_versions_via_history(&snap, key.as_ref(), &end_key).unwrap();

		assert_eq!(results.len(), 1);
		let (k, v, version, is_deleted) = &results[0];
		assert_eq!(k, &key);
		assert_eq!(v, &value);
		assert_eq!(*version, 1);
		assert!(!(*is_deleted));
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_with_limit_with_multiple_versions_per_key() {
		let (store, _tmp_dir) = create_tree();
		let keys = vec![Vec::from("key1"), Vec::from("key2"), Vec::from("key3")];
		let values = [Vec::from("value1"), Vec::from("value2"), Vec::from("value3")];

		for key in &keys {
			for (i, value) in values.iter().enumerate() {
				let version = (i + 1) as u64;
				store.set_at(key, value, version).await.unwrap();
			}
		}

		let snap = store.new_snapshot();
		let mut end_key = keys.last().unwrap().clone();
		end_key.push(0);
		let all_results =
			scan_all_versions_via_history(&snap, keys.first().unwrap().as_ref(), &end_key).unwrap();

		assert_eq!(all_results.len(), 9);

		let unique_keys: HashSet<_> = all_results.iter().map(|(k, _, _, _)| k.clone()).collect();
		assert_eq!(unique_keys.len(), 3);

		for key in unique_keys {
			let key_versions: Vec<_> =
				all_results.iter().filter(|(k, _, _, _)| k == &key).collect();
			assert_eq!(key_versions.len(), 3);

			let latest = key_versions.iter().max_by_key(|(_, _, version, _)| version).unwrap();
			assert_eq!(latest.1.as_ref(), *values.last().unwrap());
			assert_eq!(latest.2, values.len() as u64);
		}
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_with_subsets() {
		let (store, _tmp_dir) = create_tree();
		let keys = vec![
			Vec::from("key1"),
			Vec::from("key2"),
			Vec::from("key3"),
			Vec::from("key4"),
			Vec::from("key5"),
			Vec::from("key6"),
			Vec::from("key7"),
		];
		let values = [Vec::from("value1"), Vec::from("value2"), Vec::from("value3")];

		for key in &keys {
			for (i, value) in values.iter().enumerate() {
				let version = (i + 1) as u64;
				store.set_at(key, value, version).await.unwrap();
			}
		}

		let subsets = vec![
			(&keys[0], &keys[2]),
			(&keys[1], &keys[3]),
			(&keys[2], &keys[4]),
			(&keys[3], &keys[5]),
			(&keys[4], &keys[6]),
		];

		for subset in subsets {
			let snap = store.new_snapshot();
			let mut end_key = subset.1.clone();
			end_key.push(0);
			let results = scan_all_versions_via_history(&snap, subset.0, &end_key).unwrap();

			let unique_keys: HashSet<_> = results.iter().map(|(k, _, _, _)| k.clone()).collect();

			for key in unique_keys {
				for (i, value) in values.iter().enumerate() {
					let version = (i + 1) as u64;
					let result = results
						.iter()
						.find(|(k, v, ver, _)| k == &key && v == value && *ver == version)
						.unwrap();
					assert_eq!(result.1.as_ref(), *value);
					assert_eq!(result.2, version);
				}
			}
		}
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_range_bounds() {
		let (store, _tmp_dir) = create_tree();

		{
			let mut batch = store.new_batch();
			batch.set_at(b"key1", b"value1", 1).unwrap();
			batch.set_at(b"key1", b"value1_v2", 2).unwrap();
			batch.set_at(b"key2", b"value2", 1).unwrap();
			batch.set_at(b"key2", b"value2_v2", 2).unwrap();
			batch.set_at(b"key3", b"value3", 1).unwrap();
			batch.set_at(b"key4", b"value4", 1).unwrap();
			batch.set_at(b"key5", b"value5", 1).unwrap();
			store.apply(batch, false).await.unwrap();
		}

		let snap = store.new_snapshot();
		let results =
			scan_all_versions_via_history(&snap, &b""[..], &b"\xff\xff\xff\xff"[..]).unwrap();
		assert_eq!(results.len(), 7);

		let results =
			scan_all_versions_via_history(&snap, &b"key2"[..], &b"\xff\xff\xff\xff"[..]).unwrap();
		assert_eq!(results.len(), 5);

		let results =
			scan_all_versions_via_history(&snap, &b"key2\x00"[..], &b"\xff\xff\xff\xff"[..])
				.unwrap();
		assert_eq!(results.len(), 3);

		let results =
			scan_all_versions_via_history(&snap, &b"key5\x00"[..], &b"\xff\xff\xff\xff"[..])
				.unwrap();
		assert_eq!(results.len(), 0);
	}

	#[test(tokio::test)]
	async fn test_scan_all_versions_with_batches() {
		let (store, _tmp_dir) = create_tree();
		let keys = [
			Vec::from("key1"),
			Vec::from("key2"),
			Vec::from("key3"),
			Vec::from("key4"),
			Vec::from("key5"),
		];
		let versions = [
			vec![Vec::from("v1"), Vec::from("v2"), Vec::from("v3"), Vec::from("v4")],
			vec![Vec::from("v1"), Vec::from("v2")],
			vec![Vec::from("v1"), Vec::from("v2"), Vec::from("v3"), Vec::from("v4")],
			vec![Vec::from("v1")],
			vec![Vec::from("v1")],
		];

		for (key, key_versions) in keys.iter().zip(versions.iter()) {
			for (i, value) in key_versions.iter().enumerate() {
				let version = (i + 1) as u64;
				store.set_at(key, value, version).await.unwrap();
			}
		}

		let snap = store.new_snapshot();
		let all_results =
			scan_all_versions_via_history(&snap, &b""[..], &b"\xff\xff\xff\xff"[..]).unwrap();

		assert_eq!(all_results.len(), 12);

		let key1_results: Vec<_> = all_results.iter().filter(|(k, _, _, _)| k == b"key1").collect();
		assert_eq!(key1_results.len(), 4);

		let key2_results: Vec<_> = all_results.iter().filter(|(k, _, _, _)| k == b"key2").collect();
		assert_eq!(key2_results.len(), 2);

		let key3_results: Vec<_> = all_results.iter().filter(|(k, _, _, _)| k == b"key3").collect();
		assert_eq!(key3_results.len(), 4);

		let key4_results: Vec<_> = all_results.iter().filter(|(k, _, _, _)| k == b"key4").collect();
		assert_eq!(key4_results.len(), 1);

		let key5_results: Vec<_> = all_results.iter().filter(|(k, _, _, _)| k == b"key5").collect();
		assert_eq!(key5_results.len(), 1);
	}
}

// ============================================================================
// Test: Versions survive memtable flush (bug detector)
// ============================================================================

#[test(tokio::test)]
async fn test_versioned_range_survives_memtable_flush() {
	let temp_dir = create_temp_directory();
	let opts = Options::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_versioning(true, 0)
		.with_max_memtable_size(4 * 1024); // Small memtable to trigger flush
	let store = TreeBuilder::with_options(opts).build().unwrap();

	// Insert key1 three times with different values
	for i in 1..=3 {
		let value = format!("v{i}");
		store.set_at(b"key1", value.as_bytes(), i as u64 * 100).await.unwrap();
	}

	// Verify all 3 versions exist before forcing flush
	{
		let snap = store.new_snapshot();
		let mut iter = snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
		let mut results = collect_history_all(&mut iter).unwrap();
		results.sort_by_key(|a| a.2);
		assert_eq!(results.len(), 3, "Should have 3 versions before flush");
		assert_eq!(results[0].1, b"v1");
		assert_eq!(results[1].1, b"v2");
		assert_eq!(results[2].1, b"v3");
	}

	// Write enough data to force memtable rotation/flush
	for i in 0..50 {
		let k = format!("pad_{i:04}");
		let v = "P".repeat(256);
		store.set(k.as_bytes(), v.as_bytes()).await.unwrap();
	}

	// Query versions AFTER flush - this is the critical test
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
	let mut results = collect_history_all(&mut iter).unwrap();
	results.sort_by_key(|a| a.2);

	assert_eq!(results.len(), 3, "All 3 versions should survive memtable flush");
	assert_eq!(results[0].1, b"v1");
	assert_eq!(results[1].1, b"v2");
	assert_eq!(results[2].1, b"v3");
}

// =============================================================================
// SnapshotIterator Direction-Switching Tests
// =============================================================================

/// Test 1: Forward-to-backward direction switch
#[test(tokio::test)]
async fn test_direction_switch_forward_to_backward() {
	let (store, _temp_dir) = create_store();

	{
		let mut batch = store.new_batch();
		batch.set(b"a", b"val_a").unwrap();
		batch.set(b"b", b"val_b").unwrap();
		batch.set(b"c", b"val_c").unwrap();
		batch.set(b"d", b"val_d").unwrap();
		batch.set(b"e", b"val_e").unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	let mut iter = snap.range(Some(b"a"), Some(b"f")).unwrap();

	assert!(iter.seek_first().unwrap(), "seek_first should succeed");
	assert_eq!(iter.key().user_key(), b"a");

	assert!(iter.next().unwrap());
	assert_eq!(iter.key().user_key(), b"b");

	assert!(iter.next().unwrap());
	assert_eq!(iter.key().user_key(), b"c");

	assert!(iter.prev().unwrap(), "prev from 'c' should succeed");
	assert_eq!(iter.key().user_key(), b"b", "After prev() from 'c', should be at 'b'");

	assert!(iter.prev().unwrap());
	assert_eq!(iter.key().user_key(), b"a");

	assert!(!iter.prev().unwrap(), "prev from 'a' should return false");
}

/// Test 2: Forward-to-backward at end of range
#[test(tokio::test)]
async fn test_direction_switch_forward_to_backward_at_end() {
	let (store, _temp_dir) = create_store();

	{
		let mut batch = store.new_batch();
		batch.set(b"a", b"val_a").unwrap();
		batch.set(b"b", b"val_b").unwrap();
		batch.set(b"c", b"val_c").unwrap();
		batch.set(b"d", b"val_d").unwrap();
		batch.set(b"e", b"val_e").unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	let mut iter = snap.range(Some(b"a"), Some(b"f")).unwrap();

	iter.seek_first().unwrap();
	while iter.valid() && iter.key().user_key() != b"e" {
		iter.next().unwrap();
	}
	assert_eq!(iter.key().user_key(), b"e");

	assert!(iter.prev().unwrap());
	assert_eq!(iter.key().user_key(), b"d", "After prev() from 'e', should be at 'd'");
}

/// Test 3: Backward-to-forward direction switch
#[test(tokio::test)]
async fn test_direction_switch_backward_to_forward() {
	let (store, _temp_dir) = create_store();

	{
		let mut batch = store.new_batch();
		batch.set(b"a", b"val_a").unwrap();
		batch.set(b"b", b"val_b").unwrap();
		batch.set(b"c", b"val_c").unwrap();
		batch.set(b"d", b"val_d").unwrap();
		batch.set(b"e", b"val_e").unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	let mut iter = snap.range(Some(b"a"), Some(b"f")).unwrap();

	assert!(iter.seek_last().unwrap());
	assert_eq!(iter.key().user_key(), b"e");

	assert!(iter.prev().unwrap());
	assert_eq!(iter.key().user_key(), b"d");

	assert!(iter.prev().unwrap());
	assert_eq!(iter.key().user_key(), b"c");

	assert!(iter.next().unwrap());
	assert_eq!(iter.key().user_key(), b"d");

	assert!(iter.next().unwrap());
	assert_eq!(iter.key().user_key(), b"e");

	assert!(!iter.next().unwrap(), "next from 'e' should return false");
}

/// Test 4: Multiple direction switches
#[test(tokio::test)]
async fn test_multiple_direction_switches() {
	let (store, _temp_dir) = create_store();

	{
		let mut batch = store.new_batch();
		batch.set(b"a", b"val_a").unwrap();
		batch.set(b"b", b"val_b").unwrap();
		batch.set(b"c", b"val_c").unwrap();
		batch.set(b"d", b"val_d").unwrap();
		batch.set(b"e", b"val_e").unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	let mut iter = snap.range(Some(b"a"), Some(b"f")).unwrap();

	iter.seek_first().unwrap();
	assert_eq!(iter.key().user_key(), b"a");
	iter.next().unwrap();
	assert_eq!(iter.key().user_key(), b"b");
	iter.next().unwrap();
	assert_eq!(iter.key().user_key(), b"c");

	assert!(iter.prev().unwrap());
	assert_eq!(iter.key().user_key(), b"b");

	assert!(iter.next().unwrap());
	assert_eq!(iter.key().user_key(), b"c");

	assert!(iter.next().unwrap());
	assert_eq!(iter.key().user_key(), b"d");
	assert!(iter.next().unwrap());
	assert_eq!(iter.key().user_key(), b"e");

	assert!(iter.prev().unwrap());
	assert_eq!(iter.key().user_key(), b"d");

	assert!(iter.next().unwrap());
	assert_eq!(iter.key().user_key(), b"e");
}

/// Test 5: Direction switch after seek
#[test(tokio::test)]
async fn test_direction_switch_after_seek() {
	let (store, _temp_dir) = create_store();

	{
		let mut batch = store.new_batch();
		batch.set(b"a", b"val_a").unwrap();
		batch.set(b"b", b"val_b").unwrap();
		batch.set(b"c", b"val_c").unwrap();
		batch.set(b"d", b"val_d").unwrap();
		batch.set(b"e", b"val_e").unwrap();
		store.apply(batch, false).await.unwrap();
	}

	let snap = store.new_snapshot();
	let mut iter = snap.range(Some(b"a"), Some(b"f")).unwrap();

	assert!(iter.seek(b"c").unwrap());
	assert_eq!(iter.key().user_key(), b"c");

	assert!(iter.prev().unwrap());
	assert_eq!(iter.key().user_key(), b"b", "After prev() from seek('c'), should be at 'b'");
}
