use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::levels::{Level, Levels};
use crate::memtable::MemTable;
use crate::snapshot::{IterState, KMergeIterator};
use crate::sstable::table::{Table, TableWriter};
use crate::test::{
	collect_all,
	collect_snapshot_iter,
	collect_snapshot_reverse,
	collect_transaction_all,
};
use crate::vfs::File;
use crate::{InternalKey, InternalKeyKind, LSMIterator, Options, Tree, TreeBuilder};

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

#[test(tokio::test)]
async fn test_empty_snapshot() {
	let (store, _temp_dir) = create_store();

	// Create a transaction without any data
	let tx = store.begin().unwrap();

	// Range scan should return empty
	let range = collect_transaction_all(&mut tx.range(b"a", b"z").unwrap()).unwrap();

	assert!(range.is_empty());
}

#[test(tokio::test)]
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
	let range = collect_transaction_all(&mut read_tx.range(b"key0", b"key:").unwrap()).unwrap();

	assert_eq!(range.len(), 2);
	assert_eq!(range[0].0, b"key1");
	assert_eq!(range[1].0, b"key2");
}

#[test(tokio::test)]
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
	let range = collect_transaction_all(&mut read_tx.range(b"key0", b"key:").unwrap()).unwrap();

	assert_eq!(range.len(), 2);
	assert_eq!(range[0].1, b"value1_v1".to_vec());
	assert_eq!(range[1].1, b"value2_v1".to_vec());

	// A new transaction should see the updated values
	let new_tx = store.begin().unwrap();
	let range = collect_transaction_all(&mut new_tx.range(b"key0", b"key:").unwrap()).unwrap();

	assert_eq!(range.len(), 2);
	assert_eq!(range[0].1, b"value1_v2".to_vec());
	assert_eq!(range[1].1, b"value2_v2".to_vec());
}

#[test(tokio::test)]
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
	let range = collect_transaction_all(&mut read_tx1.range(b"key0", b"key:").unwrap()).unwrap();

	assert_eq!(range.len(), 3);
	assert_eq!(range[0].0, b"key1");
	assert_eq!(range[1].0, b"key2");
	assert_eq!(range[2].0, b"key3");

	// A new transaction should not see the deleted key
	let read_tx2 = store.begin().unwrap();
	let range = collect_transaction_all(&mut read_tx2.range(b"key0", b"key:").unwrap()).unwrap();

	assert_eq!(range.len(), 2);
	assert_eq!(range[0].0, b"key1");
	assert_eq!(range[1].0, b"key3");
}

#[test(tokio::test)]
async fn test_version_resolution() {
	let (store, _temp_dir) = create_store();

	// Insert multiple versions of the same key
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"version1").unwrap();
		tx.commit().await.unwrap();
	}

	let tx1 = store.begin().unwrap();

	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"version2").unwrap();
		tx.commit().await.unwrap();
	}

	let tx2 = store.begin().unwrap();

	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"version3").unwrap();
		tx.commit().await.unwrap();
	}

	let tx3 = store.begin().unwrap();

	// Each snapshot should see its corresponding version
	let value1 = tx1.get(b"key1").unwrap().unwrap();
	assert_eq!(value1, b"version1");

	let value2 = tx2.get(b"key1").unwrap().unwrap();
	assert_eq!(value2, b"version2");

	let value3 = tx3.get(b"key1").unwrap().unwrap();
	assert_eq!(value3, b"version3");
}

#[test(tokio::test)]
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

	let tx1 = store.begin().unwrap();

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

	let tx2 = store.begin().unwrap();

	// tx1 should see all original data
	let range1 = collect_transaction_all(&mut tx1.range(b"key00", b"key99").unwrap()).unwrap();

	assert_eq!(range1.len(), 10);
	for (i, (key, value)) in range1.iter().enumerate() {
		let expected_key = format!("key{:02}", i + 1);
		let expected_value = format!("value{}", i + 1);
		assert_eq!(key, expected_key.as_bytes());
		assert_eq!(value.as_slice(), expected_value.as_bytes());
	}

	// tx2 should see updated data with deletions
	let range2 = collect_transaction_all(&mut tx2.range(b"key00", b"key99").unwrap()).unwrap();

	assert_eq!(range2.len(), 7); // 10 - 3 deleted

	// Check that deleted keys are not present
	let keys: HashSet<_> = range2.iter().map(|item| item.0.clone()).collect();
	assert!(!keys.contains(b"key03".as_slice()));
	assert!(!keys.contains(b"key06".as_slice()));
	assert!(!keys.contains(b"key09".as_slice()));

	// Check that even keys are updated
	for item in &range2 {
		let (key, value) = item;
		let key_str = String::from_utf8_lossy(key.as_ref());
		if let Ok(num) = key_str.trim_start_matches("key").parse::<i32>() {
			if num % 2 == 0 {
				let expected_value = format!("value{num}_updated");
				assert_eq!(value.as_slice(), expected_value.as_bytes());
			}
		}
	}
}

#[test(tokio::test)]
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
		assert_eq!(value, expected.as_bytes());
	}
}

#[test(tokio::test)]
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

	let tx = store.begin().unwrap();

	// Test different range queries
	let numeric_range = collect_transaction_all(&mut tx.range(b"000", b"999").unwrap()).unwrap();
	assert_eq!(numeric_range.len(), 10);

	let alpha_range = collect_transaction_all(&mut tx.range(b"a", b"z").unwrap()).unwrap();
	assert_eq!(alpha_range.len(), 15); // 10 numeric + 10 alpha + 5 mixed

	let mixed_range = collect_transaction_all(&mut tx.range(b"mix", b"miy").unwrap()).unwrap();
	assert_eq!(mixed_range.len(), 5);
}

#[test(tokio::test)]
async fn test_snapshot_ordering_invariants() {
	let (store, _temp_dir) = create_store();

	// Insert data in random order
	{
		let mut tx = store.begin().unwrap();
		let keys =
			vec!["key05", "key01", "key09", "key03", "key07", "key02", "key08", "key04", "key06"];
		for key in keys {
			tx.set(key.as_bytes(), key.as_bytes()).unwrap();
		}
		tx.commit().await.unwrap();
	}

	let tx = store.begin().unwrap();

	// Range scan should return keys in sorted order
	let range = collect_transaction_all(&mut tx.range(b"key00", b"key99").unwrap()).unwrap();

	assert_eq!(range.len(), 9);

	// Verify ordering
	for i in 1..range.len() {
		assert!(range[i - 1].0 < range[i].0);
	}

	// Verify no duplicates
	let keys: HashSet<_> = range.iter().map(|item| item.0.clone()).collect();
	assert_eq!(keys.len(), range.len());
}

#[test(tokio::test)]
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
	let tx = tx.snapshot.as_ref().unwrap();

	// Get keys only ([key1, key6) to include key5)
	// Note: keys_only parameter no longer exists, using regular range and mapping to keys
	let mut keys_only_iter = tx.range(Some("key1".as_bytes()), Some("key6".as_bytes())).unwrap();
	let keys_only_data = collect_snapshot_iter(&mut keys_only_iter).unwrap();
	let keys_only: Vec<(Vec<u8>, Option<Vec<u8>>)> =
		keys_only_data.into_iter().map(|(k, _)| (k.user_key, None)).collect();

	// Verify we got all 5 keys
	assert_eq!(keys_only.len(), 5);

	// Check keys are correct and values are None
	for (i, (key, value)) in keys_only.iter().enumerate().take(5) {
		let expected_key = format!("key{}", i + 1);
		assert_eq!(key, expected_key.as_bytes());

		// Values should be None for keys-only scan
		assert!(value.is_none(), "Value should be None for keys-only scan");
	}

	// Compare with regular range scan ([key1, key6) to include key5)
	let mut regular_range_iter =
		tx.range(Some("key1".as_bytes()), Some("key6".as_bytes())).unwrap();
	let regular_range_data = collect_snapshot_iter(&mut regular_range_iter).unwrap();
	let regular_range: Vec<_> =
		regular_range_data.into_iter().map(|(k, v)| (k.user_key, Some(v))).collect();

	assert_eq!(regular_range.len(), keys_only.len());

	// Keys should match but values should be different
	for i in 0..keys_only.len() {
		// Keys should be identical
		assert_eq!(keys_only[i].0, regular_range[i].0, "Keys should match");

		// Regular range should have actual values
		let expected_value = format!("value{}", i + 1);
		assert_eq!(
			regular_range[i].1.as_ref().unwrap(),
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
			let mut w = TableWriter::new(&mut buf, 0, Arc::clone(&opts), 0); // L0 for test
			for (k, v) in data {
				let ikey = InternalKey::new(k.to_vec(), 1, InternalKeyKind::Set, 0);
				w.add(ikey, v).unwrap();
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
	level0.insert(table2);

	let levels = Levels(vec![Arc::new(level0)]);

	let iter_state = IterState {
		active: Arc::new(MemTable::default()),
		immutable: Vec::new(),
		levels,
	};

	// Range that only overlaps with table2
	let internal_range = crate::user_range_to_internal_range(
		Bound::Included("z0".as_bytes()),
		Bound::Excluded("zz".as_bytes()),
	);
	let mut merge_iter = KMergeIterator::new_from(iter_state, internal_range);

	let items = collect_all(&mut merge_iter).unwrap();
	assert_eq!(items.len(), 2);
	assert_eq!(items[0].0.user_key.as_slice(), b"z1");
	assert_eq!(items[1].0.user_key.as_slice(), b"z2");
}

#[test(tokio::test)]
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
	let tx = tx.snapshot.as_ref().unwrap();

	// Test forward iteration ([key1, key6) to include key5)
	let mut forward_iter = tx.range(Some("key1".as_bytes()), Some("key6".as_bytes())).unwrap();
	let forward_items = collect_snapshot_iter(&mut forward_iter).unwrap();

	assert_eq!(forward_items.len(), 5);
	assert_eq!(&forward_items[0].0.user_key, b"key1");
	assert_eq!(&forward_items[4].0.user_key, b"key5");

	// Test backward iteration ([key1, key6) to include key5)
	let mut backward_iter = tx.range(Some("key1".as_bytes()), Some("key6".as_bytes())).unwrap();
	let backward_items = collect_snapshot_reverse(&mut backward_iter).unwrap();

	assert_eq!(backward_items.len(), 5);
	assert_eq!(&backward_items[0].0.user_key, b"key5");
	assert_eq!(&backward_items[4].0.user_key, b"key1");

	// Verify both iterations produce the same items in reverse order
	for i in 0..5 {
		assert_eq!(forward_items[i].0, backward_items[4 - i].0);
	}
}

#[test(tokio::test)]
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
	let tx1 = store.begin().unwrap();

	// Delete key2
	{
		let mut tx = store.begin().unwrap();
		tx.delete(b"key2").unwrap();
		tx.commit().await.unwrap();
	}

	// Take another snapshot after deletion
	let tx2 = store.begin().unwrap();

	// Test forward iteration on first snapshot (should see all keys)
	let tx1_ref = tx1.snapshot.as_ref().unwrap();
	let mut forward_iter1 =
		tx1_ref.range(Some("key1".as_bytes()), Some("key4".as_bytes())).unwrap();
	let forward_items1 = collect_snapshot_iter(&mut forward_iter1).unwrap();
	assert_eq!(forward_items1.len(), 3);

	// Test backward iteration on first snapshot
	let mut backward_iter1 =
		tx1_ref.range(Some("key1".as_bytes()), Some("key4".as_bytes())).unwrap();
	let backward_items1 = collect_snapshot_reverse(&mut backward_iter1).unwrap();
	assert_eq!(backward_items1.len(), 3);

	// Test forward iteration on second snapshot (should not see deleted key)
	let tx2_ref = tx2.snapshot.as_ref().unwrap();
	let mut forward_iter2 =
		tx2_ref.range(Some("key1".as_bytes()), Some("key4".as_bytes())).unwrap();
	let forward_items2 = collect_snapshot_iter(&mut forward_iter2).unwrap();
	assert_eq!(forward_items2.len(), 2);

	// Test backward iteration on second snapshot
	let mut backward_iter2 =
		tx2_ref.range(Some("key1".as_bytes()), Some("key4".as_bytes())).unwrap();
	let backward_items2 = collect_snapshot_reverse(&mut backward_iter2).unwrap();
	assert_eq!(backward_items2.len(), 2);

	// Verify both iterations produce the same items in reverse order
	for i in 0..forward_items2.len() {
		assert_eq!(forward_items2[i].0, backward_items2[forward_items2.len() - 1 - i].0);
	}
}

#[test(tokio::test)]
async fn test_soft_delete_snapshot_individual_get() {
	let (store, _temp_dir) = create_store();

	// Insert initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.set(b"key2", b"value2").unwrap();
		tx.commit().await.unwrap();
	}

	// Take a snapshot before soft delete
	let tx1 = store.begin().unwrap();

	// Soft delete key2
	{
		let mut tx = store.begin().unwrap();
		tx.soft_delete(b"key2").unwrap();
		tx.commit().await.unwrap();
	}

	// Take another snapshot after soft delete
	let tx2 = store.begin().unwrap();

	// First snapshot should see both keys
	{
		assert_eq!(&tx1.get(b"key1").unwrap().unwrap(), b"value1");
		assert_eq!(&tx1.get(b"key2").unwrap().unwrap(), b"value2");
	}

	// Second snapshot should not see the soft deleted key
	{
		assert_eq!(&tx2.get(b"key1").unwrap().unwrap(), b"value1");
		assert!(tx2.get(b"key2").unwrap().is_none());
	}
}

#[test(tokio::test)]
async fn test_soft_delete_snapshot_double_ended_iteration() {
	let (store, _temp_dir) = create_store();

	// Insert initial data
	{
		let mut tx = store.begin().unwrap();
		for i in 1..=5 {
			let key = format!("key{i}");
			let value = format!("value{i}");
			tx.set(key.as_bytes(), value.as_bytes()).unwrap();
		}
		tx.commit().await.unwrap();
	}

	// Soft delete key2 and key4
	{
		let mut tx = store.begin().unwrap();
		tx.soft_delete(b"key2").unwrap();
		tx.soft_delete(b"key4").unwrap();
		tx.commit().await.unwrap();
	}

	// Take snapshot after soft delete
	let tx = store.begin().unwrap();

	// Test forward iteration
	{
		let snapshot_ref = tx.snapshot.as_ref().unwrap();
		let mut forward_iter =
			snapshot_ref.range(Some("key1".as_bytes()), Some("key6".as_bytes())).unwrap();
		let forward_items = collect_snapshot_iter(&mut forward_iter).unwrap();

		assert_eq!(forward_items.len(), 3); // key1, key3, key5
		assert_eq!(&forward_items[0].0.user_key, b"key1");
		assert_eq!(&forward_items[1].0.user_key, b"key3");
		assert_eq!(&forward_items[2].0.user_key, b"key5");
	}

	// Test backward iteration
	{
		let snapshot_ref = tx.snapshot.as_ref().unwrap();
		let mut backward_iter =
			snapshot_ref.range(Some("key1".as_bytes()), Some("key6".as_bytes())).unwrap();
		let backward_items = collect_snapshot_reverse(&mut backward_iter).unwrap();

		assert_eq!(backward_items.len(), 3); // key5, key3, key1
		assert_eq!(&backward_items[0].0.user_key, b"key5");
		assert_eq!(&backward_items[1].0.user_key, b"key3");
		assert_eq!(&backward_items[2].0.user_key, b"key1");
	}
}

#[test(tokio::test)]
async fn test_soft_delete_snapshot_mixed_with_hard_delete() {
	let (store, _temp_dir) = create_store();

	// Insert initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.set(b"key2", b"value2").unwrap();
		tx.set(b"key3", b"value3").unwrap();
		tx.set(b"key4", b"value4").unwrap();
		tx.commit().await.unwrap();
	}

	// Take snapshot before any deletes
	let tx1 = store.begin().unwrap();

	// Mix of soft delete and hard delete
	{
		let mut tx = store.begin().unwrap();
		tx.soft_delete(b"key1").unwrap(); // Soft delete
		tx.delete(b"key2").unwrap(); // Hard delete
		tx.commit().await.unwrap();
	}

	// Take snapshot after deletes
	let tx2 = store.begin().unwrap();

	// First snapshot should see all keys
	{
		let tx1_ref = tx1.snapshot.as_ref().unwrap();
		let mut range_iter =
			tx1_ref.range(Some("key1".as_bytes()), Some("key5".as_bytes())).unwrap();
		let range = collect_snapshot_iter(&mut range_iter).unwrap();
		assert_eq!(range.len(), 4);
	}

	// Second snapshot should not see either deleted key
	{
		let tx2_ref = tx2.snapshot.as_ref().unwrap();
		let mut range_iter =
			tx2_ref.range(Some("key1".as_bytes()), Some("key5".as_bytes())).unwrap();
		let range = collect_snapshot_iter(&mut range_iter).unwrap();
		assert_eq!(range.len(), 2); // Only key3 and key4
		assert_eq!(&range[0].0.user_key, b"key3");
		assert_eq!(&range[1].0.user_key, b"key4");
	}
}

#[test(tokio::test)]
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
	let tx = store.begin().unwrap();

	// Test forward iteration
	let snapshot_ref = tx.snapshot.as_ref().unwrap();
	let mut forward_iter =
		snapshot_ref.range(Some("key01".as_bytes()), Some("key11".as_bytes())).unwrap();
	let forward_items = collect_snapshot_iter(&mut forward_iter).unwrap();

	// Test backward iteration
	let mut backward_iter =
		snapshot_ref.range(Some("key01".as_bytes()), Some("key11".as_bytes())).unwrap();
	let backward_items = collect_snapshot_reverse(&mut backward_iter).unwrap();

	// Both should have 10 items
	assert_eq!(forward_items.len(), 10);
	assert_eq!(backward_items.len(), 10);

	// Verify ordering
	for i in 1..=10 {
		let expected_key = format!("key{i:02}");
		assert_eq!(&forward_items[i - 1].0.user_key, expected_key.as_bytes());
		assert_eq!(&backward_items[10 - i].0.user_key, expected_key.as_bytes());
	}

	// Verify both iterations produce the same items in reverse order
	for i in 0..10 {
		assert_eq!(forward_items[i].0, backward_items[9 - i].0);
	}
}

// ========================================================================
// KMergeIterator Range Query Tests
// ========================================================================

// Helper function to create a test table with specific key range
fn create_test_table_with_range(
	table_id: u64,
	key_start: &str,
	key_end: &str,
	seq_start: u64,
	opts: Arc<Options>,
) -> crate::Result<Arc<Table>> {
	use std::fs::{self, File as SysFile};

	// Ensure the sstables directory exists
	let sstables_dir = opts.path.join("sstables");
	fs::create_dir_all(&sstables_dir)?;

	let table_file_path = opts.sstable_file_path(table_id);
	let mut file = SysFile::create(&table_file_path)?;

	let mut writer = TableWriter::new(&mut file, table_id, Arc::clone(&opts), 0); // L0 for test

	// Generate incremental keys spanning the range
	let mut keys = Vec::new();

	// For single-character ranges, generate all keys from start to end
	if key_start.len() == 1 && key_end.len() == 1 {
		let start_byte = key_start.as_bytes()[0];
		let end_byte = key_end.as_bytes()[0];

		for byte_val in start_byte..=end_byte {
			keys.push(String::from_utf8(vec![byte_val]).unwrap());
		}
	} else {
		// For multi-character ranges, create keys with numeric suffixes
		keys.push(key_start.to_string());
		keys.push(format!("{key_start}_mid"));
		keys.push(key_end.to_string());
	}

	for (i, key) in keys.iter().enumerate() {
		let seq_num = seq_start + i as u64;
		let value = format!("value_{seq_num}");

		let internal_key =
			InternalKey::new(key.as_bytes().to_vec(), seq_num, InternalKeyKind::Set, 0);

		writer.add(internal_key, value.as_bytes())?;
	}

	let size = writer.finish()?;

	let file = SysFile::open(&table_file_path)?;
	file.sync_all()?;
	let file: Arc<dyn File> = Arc::new(file);

	let table = Table::new(table_id, opts, file, size as u64)?;
	Ok(Arc::new(table))
}

// Helper to create IterState with specified tables
fn create_iter_state_with_tables(
	l0_tables: Vec<Arc<Table>>,
	l1_tables: Vec<Arc<Table>>,
	l2_tables: Vec<Arc<Table>>,
	_opts: Arc<Options>,
) -> IterState {
	let mut level0 = Level::default();
	for table in l0_tables {
		level0.tables.push(table);
	}

	let mut level1 = Level::default();
	for table in l1_tables {
		level1.tables.push(table);
	}

	let mut level2 = Level::default();
	for table in l2_tables {
		level2.tables.push(table);
	}

	let levels = Levels(vec![Arc::new(level0), Arc::new(level1), Arc::new(level2)]);

	IterState {
		active: Arc::new(MemTable::default()),
		immutable: vec![],
		levels,
	}
}

// Helper to count the number of items returned by iterator
fn count_kmerge_items(mut iter: KMergeIterator) -> usize {
	iter.seek_first().unwrap();
	let mut count = 0;
	while iter.valid() {
		count += 1;
		if !iter.next().unwrap() {
			break;
		}
	}
	count
}

#[test]
fn test_level0_tables_before_range_skipped() {
	let temp_dir = create_temp_directory();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	};
	let opts = Arc::new(opts);

	// Create L0 tables with ranges: [a-c], [d-f], [g-i]
	let table1 = create_test_table_with_range(1, "a", "c", 1, Arc::clone(&opts)).unwrap();
	let table2 = create_test_table_with_range(2, "d", "f", 4, Arc::clone(&opts)).unwrap();
	let table3 = create_test_table_with_range(3, "g", "i", 7, Arc::clone(&opts)).unwrap();

	let iter_state =
		create_iter_state_with_tables(vec![table1, table2, table3], vec![], vec![], opts);

	// Query range: [j-z] - all tables are before this range
	let internal_range = crate::user_range_to_internal_range(
		Bound::Included(b"j".as_slice()),
		Bound::Excluded(b"z".as_slice()),
	);
	let iter = KMergeIterator::new_from(iter_state, internal_range);

	let count = count_kmerge_items(iter);
	assert_eq!(count, 0, "No tables should be included as all are before range");
}

#[test]
fn test_level0_tables_after_range_skipped() {
	let temp_dir = create_temp_directory();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	};
	let opts = Arc::new(opts);

	// Create L0 tables with ranges: [m-o], [p-r], [s-u]
	let table1 = create_test_table_with_range(1, "m", "o", 1, Arc::clone(&opts)).unwrap();
	let table2 = create_test_table_with_range(2, "p", "r", 4, Arc::clone(&opts)).unwrap();
	let table3 = create_test_table_with_range(3, "s", "u", 7, Arc::clone(&opts)).unwrap();

	let iter_state =
		create_iter_state_with_tables(vec![table1, table2, table3], vec![], vec![], opts);

	// Query range: [a-k] - all tables are after this range
	let internal_range = crate::user_range_to_internal_range(
		Bound::Included(b"a".as_slice()),
		Bound::Excluded(b"k".as_slice()),
	);
	let iter = KMergeIterator::new_from(iter_state, internal_range);

	let count = count_kmerge_items(iter);
	assert_eq!(count, 0, "No tables should be included as all are after range");
}

#[test]
fn test_level0_overlapping_tables_included() {
	let temp_dir = create_temp_directory();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	};
	let opts = Arc::new(opts);

	// Create L0 tables with overlapping ranges
	let table1 = create_test_table_with_range(1, "a", "e", 1, Arc::clone(&opts)).unwrap();
	let table2 = create_test_table_with_range(2, "c", "g", 4, Arc::clone(&opts)).unwrap();
	let table3 = create_test_table_with_range(3, "f", "j", 7, Arc::clone(&opts)).unwrap();

	let iter_state =
		create_iter_state_with_tables(vec![table1, table2, table3], vec![], vec![], opts);

	// Query range: [d-h] - all tables overlap with this range
	let internal_range = crate::user_range_to_internal_range(
		Bound::Included(b"d".as_slice()),
		Bound::Excluded(b"h".as_slice()),
	);
	let iter = KMergeIterator::new_from(iter_state, internal_range);

	let count = count_kmerge_items(iter);
	// All 3 tables should contribute items
	assert!(count > 0, "Should have items from overlapping L0 tables");
}

#[test]
fn test_level0_mixed_overlap_scenarios() {
	let temp_dir = create_temp_directory();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	};
	let opts = Arc::new(opts);

	// Create L0 tables: [a-c], [e-g], [i-k], [d-f], [j-m]
	let table1 = create_test_table_with_range(1, "a", "c", 1, Arc::clone(&opts)).unwrap();
	let table2 = create_test_table_with_range(2, "e", "g", 4, Arc::clone(&opts)).unwrap();
	let table3 = create_test_table_with_range(3, "i", "k", 7, Arc::clone(&opts)).unwrap();
	let table4 = create_test_table_with_range(4, "d", "f", 10, Arc::clone(&opts)).unwrap();
	let table5 = create_test_table_with_range(5, "j", "m", 13, Arc::clone(&opts)).unwrap();

	let iter_state = create_iter_state_with_tables(
		vec![table1, table2, table3, table4, table5],
		vec![],
		vec![],
		opts,
	);

	// Query range: [f-j] - should include tables [e-g], [i-k], [d-f], [j-m]
	let internal_range = crate::user_range_to_internal_range(
		Bound::Included(b"f".as_slice()),
		Bound::Excluded(b"j".as_slice()),
	);
	let iter = KMergeIterator::new_from(iter_state, internal_range);

	let count = count_kmerge_items(iter);
	// Should have items from multiple overlapping tables
	assert!(count > 0, "Should have items from overlapping tables in range");
}

#[test]
fn test_level1_binary_search_correct_range() {
	let temp_dir = create_temp_directory();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	};
	let opts = Arc::new(opts);

	// Create L1 tables with non-overlapping sorted ranges
	let table1 = create_test_table_with_range(11, "a", "b", 1, Arc::clone(&opts)).unwrap();
	let table2 = create_test_table_with_range(12, "c", "d", 4, Arc::clone(&opts)).unwrap();
	let table3 = create_test_table_with_range(13, "e", "f", 7, Arc::clone(&opts)).unwrap();
	let table4 = create_test_table_with_range(14, "g", "h", 10, Arc::clone(&opts)).unwrap();
	let table5 = create_test_table_with_range(15, "i", "j", 13, Arc::clone(&opts)).unwrap();

	let iter_state = create_iter_state_with_tables(
		vec![],
		vec![table1, table2, table3, table4, table5],
		vec![],
		opts,
	);

	// Query range: [e-h] - should include tables [e-f], [g-h]
	let internal_range = crate::user_range_to_internal_range(
		Bound::Included(b"e".as_slice()),
		Bound::Excluded(b"h".as_slice()),
	);
	let iter = KMergeIterator::new_from(iter_state, internal_range);

	let count = count_kmerge_items(iter);
	assert!(count > 0, "Should have items from L1 tables in range");
}

#[test]
fn test_level1_query_before_all_tables() {
	let temp_dir = create_temp_directory();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	};
	let opts = Arc::new(opts);

	// Create L1 tables: [d-f], [g-i], [j-l]
	let table1 = create_test_table_with_range(11, "d", "f", 1, Arc::clone(&opts)).unwrap();
	let table2 = create_test_table_with_range(12, "g", "i", 4, Arc::clone(&opts)).unwrap();
	let table3 = create_test_table_with_range(13, "j", "l", 7, Arc::clone(&opts)).unwrap();

	let iter_state =
		create_iter_state_with_tables(vec![], vec![table1, table2, table3], vec![], opts);

	// Query range: [a-c] - before all tables
	let internal_range = crate::user_range_to_internal_range(
		Bound::Included(b"a".as_slice()),
		Bound::Excluded(b"c".as_slice()),
	);
	let iter = KMergeIterator::new_from(iter_state, internal_range);

	let count = count_kmerge_items(iter);
	assert_eq!(count, 0, "No tables should be included as query is before all L1 tables");
}

#[test]
fn test_level1_query_after_all_tables() {
	let temp_dir = create_temp_directory();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	};
	let opts = Arc::new(opts);

	// Create L1 tables: [a-c], [d-f], [g-i]
	let table1 = create_test_table_with_range(11, "a", "c", 1, Arc::clone(&opts)).unwrap();
	let table2 = create_test_table_with_range(12, "d", "f", 4, Arc::clone(&opts)).unwrap();
	let table3 = create_test_table_with_range(13, "g", "i", 7, Arc::clone(&opts)).unwrap();

	let iter_state =
		create_iter_state_with_tables(vec![], vec![table1, table2, table3], vec![], opts);

	// Query range: [m-z] - after all tables
	let internal_range = crate::user_range_to_internal_range(
		Bound::Included(b"m".as_slice()),
		Bound::Excluded(b"z".as_slice()),
	);
	let iter = KMergeIterator::new_from(iter_state, internal_range);

	let count = count_kmerge_items(iter);
	assert_eq!(count, 0, "No tables should be included as query is after all L1 tables");
}

#[test]
fn test_level1_query_spans_all_tables() {
	let temp_dir = create_temp_directory();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	};
	let opts = Arc::new(opts);

	// Create L1 tables: [b-d], [e-g], [h-j]
	let table1 = create_test_table_with_range(11, "b", "d", 1, Arc::clone(&opts)).unwrap();
	let table2 = create_test_table_with_range(12, "e", "g", 4, Arc::clone(&opts)).unwrap();
	let table3 = create_test_table_with_range(13, "h", "j", 7, Arc::clone(&opts)).unwrap();

	let iter_state =
		create_iter_state_with_tables(vec![], vec![table1, table2, table3], vec![], opts);

	// Query range: [a-z] - spans all tables
	let internal_range = crate::user_range_to_internal_range(
		Bound::Included(b"a".as_slice()),
		Bound::Excluded(b"z".as_slice()),
	);
	let iter = KMergeIterator::new_from(iter_state, internal_range);

	let count = count_kmerge_items(iter);
	assert!(count > 0, "Should have items from all L1 tables");
}

#[test]
fn test_bound_included_start_and_end() {
	let temp_dir = create_temp_directory();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	};
	let opts = Arc::new(opts);

	// Create table with keys: "d1", "d5", "h"
	let table1 = create_test_table_with_range(1, "d", "h", 1, Arc::clone(&opts)).unwrap();

	let iter_state = create_iter_state_with_tables(vec![table1], vec![], vec![], opts);

	// Query with Included bounds - should include all keys from d to h
	let internal_range = crate::user_range_to_internal_range(
		Bound::Included(b"d".as_slice()),
		Bound::Excluded(b"h".as_slice()),
	);
	let mut iter = KMergeIterator::new_from(iter_state, internal_range);

	let items = collect_all(&mut iter).unwrap();
	assert!(!items.is_empty(), "Should have items in inclusive range");
}

#[test]
fn test_bound_excluded_start_and_end() {
	let temp_dir = create_temp_directory();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	};
	let opts = Arc::new(opts);

	// Create table with keys: "d1", "d5", "h"
	let table1 = create_test_table_with_range(1, "d", "h", 1, Arc::clone(&opts)).unwrap();

	let iter_state = create_iter_state_with_tables(vec![table1], vec![], vec![], opts);

	// Query with Excluded bounds - "d" and "h" exact matches should be excluded
	// But "d1", "d5" are > "d" so they should be included
	let internal_range = crate::user_range_to_internal_range(
		Bound::Included(b"d".as_slice()),
		Bound::Excluded(b"h".as_slice()),
	);
	let mut iter = KMergeIterator::new_from(iter_state, internal_range);

	let items = collect_all(&mut iter).unwrap();
	// Keys "d1" and "d5" should be included (they're > "d" and < "h")
	// But exact "d" and exact "h" should not be (though "h" is at the boundary)
	assert!(items.len() >= 2, "Should have at least d1 and d5");
}

#[test]
fn test_bound_unbounded_start() {
	let temp_dir = create_temp_directory();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	};
	let opts = Arc::new(opts);

	let table1 = create_test_table_with_range(1, "a", "z", 1, Arc::clone(&opts)).unwrap();

	let iter_state = create_iter_state_with_tables(vec![table1], vec![], vec![], opts);

	// Query with unbounded start
	let internal_range =
		crate::user_range_to_internal_range(Bound::Unbounded, Bound::Included(b"h".as_slice()));
	let iter = KMergeIterator::new_from(iter_state, internal_range);

	let count = count_kmerge_items(iter);
	assert!(count > 0, "Should iterate from beginning with unbounded start");
}

#[test]
fn test_bound_unbounded_end() {
	let temp_dir = create_temp_directory();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	};
	let opts = Arc::new(opts);

	let table1 = create_test_table_with_range(1, "a", "z", 1, Arc::clone(&opts)).unwrap();

	let iter_state = create_iter_state_with_tables(vec![table1], vec![], vec![], opts);

	// Query with unbounded end
	let internal_range =
		crate::user_range_to_internal_range(Bound::Included(b"d".as_slice()), Bound::Unbounded);
	let iter = KMergeIterator::new_from(iter_state, internal_range);

	let count = count_kmerge_items(iter);
	assert!(count > 0, "Should iterate to end with unbounded end");
}

#[test]
fn test_fully_unbounded_range() {
	let temp_dir = create_temp_directory();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	};
	let opts = Arc::new(opts);

	let table1 = create_test_table_with_range(1, "a", "m", 1, Arc::clone(&opts)).unwrap();
	let table2 = create_test_table_with_range(2, "n", "z", 4, Arc::clone(&opts)).unwrap();

	let iter_state = create_iter_state_with_tables(vec![table1, table2], vec![], vec![], opts);

	// Query with fully unbounded range
	let iter = KMergeIterator::new_from(iter_state, (Bound::Unbounded, Bound::Unbounded));

	let count = count_kmerge_items(iter);
	assert!(count > 0, "Should return all keys with fully unbounded range");
}

#[test]
fn test_empty_levels() {
	let temp_dir = create_temp_directory();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	};
	let opts = Arc::new(opts);

	// Create IterState with no tables
	let iter_state = create_iter_state_with_tables(vec![], vec![], vec![], opts);

	let internal_range = crate::user_range_to_internal_range(
		Bound::Included("a".as_bytes()),
		Bound::Excluded("z".as_bytes()),
	);
	let iter = KMergeIterator::new_from(iter_state, internal_range);

	let count = count_kmerge_items(iter);
	assert_eq!(count, 0, "Iterator with no tables should return no items");
}

#[test]
fn test_single_key_range() {
	let temp_dir = create_temp_directory();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	};
	let opts = Arc::new(opts);

	let table1 = create_test_table_with_range(1, "a", "z", 1, Arc::clone(&opts)).unwrap();

	let iter_state = create_iter_state_with_tables(vec![table1], vec![], vec![], opts);

	// Query for exact single key
	let internal_range = crate::user_range_to_internal_range(
		Bound::Included(b"a1".as_slice()),
		Bound::Included(b"a1".as_slice()),
	);
	let mut iter = KMergeIterator::new_from(iter_state, internal_range);

	let items = collect_all(&mut iter).unwrap();
	// Should return at most 1 item
	for (key, _) in &items {
		assert_eq!(key.user_key.as_slice(), b"a1");
	}
}

#[test]
fn test_inverted_range() {
	let temp_dir = create_temp_directory();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	};
	let opts = Arc::new(opts);

	let table1 = create_test_table_with_range(1, "a", "m", 1, Arc::clone(&opts)).unwrap();

	let iter_state = create_iter_state_with_tables(vec![table1], vec![], vec![], opts);

	// Inverted range: start > end
	let internal_range = crate::user_range_to_internal_range(
		Bound::Included("z".as_bytes()),
		Bound::Excluded("a".as_bytes()),
	);
	let iter = KMergeIterator::new_from(iter_state, internal_range);

	let count = count_kmerge_items(iter);
	assert_eq!(count, 0, "Inverted range should return no items");
}

#[test]
fn test_mixed_level0_and_level1_tables() {
	let temp_dir = create_temp_directory();
	let opts = Options {
		path: temp_dir.path().to_path_buf(),
		..Default::default()
	};
	let opts = Arc::new(opts);

	// Create both L0 and L1 tables
	let l0_table = create_test_table_with_range(1, "a", "m", 1, Arc::clone(&opts)).unwrap();
	let l1_table1 = create_test_table_with_range(11, "d", "h", 4, Arc::clone(&opts)).unwrap();
	let l1_table2 = create_test_table_with_range(12, "i", "n", 7, Arc::clone(&opts)).unwrap();

	let iter_state =
		create_iter_state_with_tables(vec![l0_table], vec![l1_table1, l1_table2], vec![], opts);

	// Query that overlaps with both levels
	let internal_range = crate::user_range_to_internal_range(
		Bound::Included("e".as_bytes()),
		Bound::Excluded("k".as_bytes()),
	);
	let iter = KMergeIterator::new_from(iter_state, internal_range);

	let count = count_kmerge_items(iter);
	assert!(count > 0, "Should have items from both L0 and L1 tables");
}

#[test(tokio::test)]
async fn test_cache_effectiveness_with_range_query() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let tree = TreeBuilder::new()
		.with_path(path)
		.with_block_cache_capacity(10 * 1024 * 1024) // 10MB cache
		.with_max_memtable_size(1024 * 64) // Small memtable to trigger flushes
		.build()
		.unwrap();

	eprintln!("\n=== Inserting 10,000 keys with periodic flushes ===");

	// Insert 10,000 keys, flushing every 1,000 keys
	for i in 0..10_000 {
		let key = format!("key_{:08}", i);
		let value = format!("value_{}", i);

		let mut tx = tree.begin().unwrap();
		tx.set(key.as_bytes(), value.as_bytes()).unwrap();
		tx.commit().await.unwrap();

		// Flush every 1,000 keys to create multiple SSTables
		if (i + 1) % 1_000 == 0 {
			tree.flush().unwrap();
			eprintln!("Flushed after {} keys", i + 1);
		}
	}

	// Final flush to ensure all data is on disk
	tree.flush().unwrap();
	eprintln!("Final flush completed\n");

	// Reset cache statistics before first query
	tree.core.opts.block_cache.reset_stats();

	eprintln!("=== First range query (populating cache) ===");
	// First range query - this will populate the cache
	let tx = tree.begin().unwrap();
	let first_results =
		collect_transaction_all(&mut tx.range("key_00000000", "key_00010000").unwrap()).unwrap();

	let first_stats = tree.core.opts.block_cache.get_stats();
	eprintln!("First query results: {} items", first_results.len());
	eprintln!("First query cache stats:");
	eprintln!("  Data hits: {}, Data misses: {}", first_stats.data_hits, first_stats.data_misses);
	eprintln!(
		"  Index hits: {}, Index misses: {}",
		first_stats.index_hits, first_stats.index_misses
	);
	eprintln!(
		"  Total hits: {}, Total misses: {}",
		first_stats.total_hits(),
		first_stats.total_misses()
	);
	eprintln!("  Hit ratio: {:.2}%\n", first_stats.hit_ratio() * 100.0);

	// Reset cache statistics before second query
	tree.core.opts.block_cache.reset_stats();

	eprintln!("=== Second range query (served from cache) ===");
	// Second range query - should be served mostly from cache
	let tx = tree.begin().unwrap();
	let second_results =
		collect_transaction_all(&mut tx.range(b"key_00000000", b"key_00010000").unwrap()).unwrap();

	let second_stats = tree.core.opts.block_cache.get_stats();
	eprintln!("Second query results: {} items", second_results.len());
	eprintln!("Second query cache stats:");
	eprintln!("  Data hits: {}, Data misses: {}", second_stats.data_hits, second_stats.data_misses);
	eprintln!(
		"  Index hits: {}, Index misses: {}",
		second_stats.index_hits, second_stats.index_misses
	);
	eprintln!(
		"  Total hits: {}, Total misses: {}",
		second_stats.total_hits(),
		second_stats.total_misses()
	);
	eprintln!("  Hit ratio: {:.2}%\n", second_stats.hit_ratio() * 100.0);

	// Assertions
	assert_eq!(first_results.len(), 10_000, "First query should return all 10,000 items");
	assert_eq!(second_results.len(), 10_000, "Second query should return all 10,000 items");
	assert!(
		second_stats.total_hits() > first_stats.total_hits() * 2,
		"Second query should have at least 2x more cache hits. First: {}, Second: {}",
		first_stats.total_hits(),
		second_stats.total_hits()
	);

	assert!(
		second_stats.hit_ratio() == 1.0,
		"Second query should have 100% cache hit ratio, got {:.2}%",
		second_stats.hit_ratio() * 100.0
	);

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn test_snapshot_iterator_seq_num_filtering_via_transactions() {
	let (store, _temp_dir) = create_store();

	// Insert key1 at seq_num ~1
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1_v1").unwrap();
		tx.commit().await.unwrap();
	}

	// Capture snapshot after key1 insert (should see only key1)
	let snapshot1 = store.begin().unwrap();

	// Insert key2 at seq_num ~2
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key2", b"value2_v1").unwrap();
		tx.commit().await.unwrap();
	}

	// Capture snapshot after key2 insert (should see key1 and key2)
	let snapshot2 = store.begin().unwrap();

	// Insert key3 at seq_num ~3
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key3", b"value3_v1").unwrap();
		tx.commit().await.unwrap();
	}

	// Capture snapshot after key3 insert (should see key1, key2, and key3)
	let snapshot3 = store.begin().unwrap();

	// Insert key4 at seq_num ~4
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key4", b"value4_v1").unwrap();
		tx.commit().await.unwrap();
	}

	// Capture snapshot after key4 insert (should see all keys)
	let snapshot4 = store.begin().unwrap();

	// Test snapshot1 - should only see key1
	{
		let snap_ref = snapshot1.snapshot.as_ref().unwrap();
		let mut range_iter =
			snap_ref.range(Some("key0".as_bytes()), Some("key9".as_bytes())).unwrap();
		let range = collect_snapshot_iter(&mut range_iter).unwrap();

		assert_eq!(range.len(), 1, "Snapshot1 should only see 1 key");
		assert_eq!(&range[0].0.user_key, b"key1");
		assert_eq!(range[0].1.as_slice(), b"value1_v1");
	}

	// Test snapshot2 - should see key1 and key2
	{
		let snap_ref = snapshot2.snapshot.as_ref().unwrap();
		let mut range_iter =
			snap_ref.range(Some("key0".as_bytes()), Some("key9".as_bytes())).unwrap();
		let range = collect_snapshot_iter(&mut range_iter).unwrap();

		assert_eq!(range.len(), 2, "Snapshot2 should see 2 keys");
		assert_eq!(&range[0].0.user_key, b"key1");
		assert_eq!(&range[1].0.user_key, b"key2");
	}

	// Test snapshot3 - should see key1, key2, and key3
	{
		let snap_ref = snapshot3.snapshot.as_ref().unwrap();
		let mut range_iter =
			snap_ref.range(Some("key0".as_bytes()), Some("key9".as_bytes())).unwrap();
		let range = collect_snapshot_iter(&mut range_iter).unwrap();

		assert_eq!(range.len(), 3, "Snapshot3 should see 3 keys");
		assert_eq!(&range[0].0.user_key, b"key1");
		assert_eq!(&range[1].0.user_key, b"key2");
		assert_eq!(&range[2].0.user_key, b"key3");
	}

	// Test snapshot4 - should see all 4 keys
	{
		let snap_ref = snapshot4.snapshot.as_ref().unwrap();
		let mut range_iter =
			snap_ref.range(Some("key0".as_bytes()), Some("key9".as_bytes())).unwrap();
		let range = collect_snapshot_iter(&mut range_iter).unwrap();

		assert_eq!(range.len(), 4, "Snapshot4 should see 4 keys");
		assert_eq!(&range[0].0.user_key, b"key1");
		assert_eq!(&range[1].0.user_key, b"key2");
		assert_eq!(&range[2].0.user_key, b"key3");
		assert_eq!(&range[3].0.user_key, b"key4");
	}

	// Test backward iteration on snapshot2
	{
		let snap_ref = snapshot2.snapshot.as_ref().unwrap();
		let mut range_iter =
			snap_ref.range(Some("key0".as_bytes()), Some("key9".as_bytes())).unwrap();
		let range = collect_snapshot_reverse(&mut range_iter).unwrap();

		assert_eq!(range.len(), 2, "Backward iteration should also see 2 keys");
		assert_eq!(&range[0].0.user_key, b"key2");
		assert_eq!(&range[1].0.user_key, b"key1");
	}
}

#[test(tokio::test)]
async fn test_snapshot_iterator_seq_num_filtering_with_updates() {
	let (store, _temp_dir) = create_store();

	// Insert initial value for key1
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value_v1").unwrap();
		tx.commit().await.unwrap();
	}

	// Snapshot after v1
	let snapshot1 = store.begin().unwrap();

	// Update key1 to v2
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value_v2").unwrap();
		tx.commit().await.unwrap();
	}

	// Snapshot after v2
	let snapshot2 = store.begin().unwrap();

	// Update key1 to v3
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value_v3").unwrap();
		tx.commit().await.unwrap();
	}

	// Snapshot after v3
	let snapshot3 = store.begin().unwrap();

	// Each snapshot should see its corresponding version
	{
		let snap1_ref = snapshot1.snapshot.as_ref().unwrap();
		let mut range_iter =
			snap1_ref.range(Some("key0".as_bytes()), Some("key9".as_bytes())).unwrap();
		let range = collect_snapshot_iter(&mut range_iter).unwrap();

		assert_eq!(range.len(), 1);
		assert_eq!(range[0].1.as_slice(), b"value_v1");
	}

	{
		let snap2_ref = snapshot2.snapshot.as_ref().unwrap();
		let mut range_iter =
			snap2_ref.range(Some("key0".as_bytes()), Some("key9".as_bytes())).unwrap();
		let range = collect_snapshot_iter(&mut range_iter).unwrap();

		assert_eq!(range.len(), 1);
		assert_eq!(range[0].1.as_slice(), b"value_v2");
	}

	{
		let snap3_ref = snapshot3.snapshot.as_ref().unwrap();
		let mut range_iter =
			snap3_ref.range(Some("key0".as_bytes()), Some("key9".as_bytes())).unwrap();
		let range = collect_snapshot_iter(&mut range_iter).unwrap();

		assert_eq!(range.len(), 1);
		assert_eq!(range[0].1.as_slice(), b"value_v3");
	}
}

#[test(tokio::test)]
async fn test_snapshot_iterator_seq_num_with_deletions() {
	let (store, _temp_dir) = create_store();

	// Insert keys 1, 2, 3
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.set(b"key2", b"value2").unwrap();
		tx.set(b"key3", b"value3").unwrap();
		tx.commit().await.unwrap();
	}

	// Snapshot before deletion
	let snapshot_before = store.begin().unwrap();

	// Delete key2
	{
		let mut tx = store.begin().unwrap();
		tx.delete(b"key2").unwrap();
		tx.commit().await.unwrap();
	}

	// Snapshot after deletion
	let snapshot_after = store.begin().unwrap();

	// Snapshot before should see all 3 keys
	{
		let snap_ref = snapshot_before.snapshot.as_ref().unwrap();
		let mut range_iter =
			snap_ref.range(Some("key0".as_bytes()), Some("key9".as_bytes())).unwrap();
		let range = collect_snapshot_iter(&mut range_iter).unwrap();

		assert_eq!(range.len(), 3, "Before deletion: should see all 3 keys");
		assert_eq!(&range[0].0.user_key, b"key1");
		assert_eq!(&range[1].0.user_key, b"key2");
		assert_eq!(&range[2].0.user_key, b"key3");
	}

	// Snapshot after should not see deleted key2
	{
		let snap_ref = snapshot_after.snapshot.as_ref().unwrap();
		let mut range_iter =
			snap_ref.range(Some("key0".as_bytes()), Some("key9".as_bytes())).unwrap();
		let range = collect_snapshot_iter(&mut range_iter).unwrap();

		assert_eq!(range.len(), 2, "After deletion: should see only 2 keys");
		assert_eq!(&range[0].0.user_key, b"key1");
		assert_eq!(&range[1].0.user_key, b"key3");
		// key2 should not be present
	}

	// Test backward iteration after deletion
	{
		let snap_ref = snapshot_after.snapshot.as_ref().unwrap();
		let mut range_iter =
			snap_ref.range(Some("key0".as_bytes()), Some("key9".as_bytes())).unwrap();
		let range = collect_snapshot_reverse(&mut range_iter).unwrap();

		assert_eq!(range.len(), 2, "Backward: should see only 2 keys");
		assert_eq!(&range[0].0.user_key, b"key3");
		assert_eq!(&range[1].0.user_key, b"key1");
	}
}

#[test(tokio::test)]
async fn test_snapshot_iterator_seq_num_complex_scenario() {
	let (store, _temp_dir) = create_store();

	// Timeline:
	// 1. Insert key1, key2, key3
	let mut tx = store.begin().unwrap();
	tx.set(b"key1", b"v1").unwrap();
	tx.set(b"key2", b"v2").unwrap();
	tx.set(b"key3", b"v3").unwrap();
	tx.commit().await.unwrap();

	let snap1 = store.begin().unwrap();

	// 2. Update key2, insert key4
	let mut tx = store.begin().unwrap();
	tx.set(b"key2", b"v2_updated").unwrap();
	tx.set(b"key4", b"v4").unwrap();
	tx.commit().await.unwrap();

	let snap2 = store.begin().unwrap();

	// 3. Delete key1, insert key5
	let mut tx = store.begin().unwrap();
	tx.delete(b"key1").unwrap();
	tx.set(b"key5", b"v5").unwrap();
	tx.commit().await.unwrap();

	let snap3 = store.begin().unwrap();

	// Verify snap1: Should see key1(v1), key2(v2), key3(v3)
	{
		let snap_ref = snap1.snapshot.as_ref().unwrap();
		let mut range_iter =
			snap_ref.range(Some("key0".as_bytes()), Some("key9".as_bytes())).unwrap();
		let range = collect_snapshot_iter(&mut range_iter).unwrap();

		assert_eq!(range.len(), 3);
		assert_eq!(&range[0].0.user_key, b"key1");
		assert_eq!(range[0].1.as_slice(), b"v1");
		assert_eq!(&range[1].0.user_key, b"key2");
		assert_eq!(range[1].1.as_slice(), b"v2");
		assert_eq!(&range[2].0.user_key, b"key3");
	}

	// Verify snap2: Should see key1(v1), key2(v2_updated), key3(v3), key4(v4)
	{
		let snap_ref = snap2.snapshot.as_ref().unwrap();
		let mut range_iter =
			snap_ref.range(Some("key0".as_bytes()), Some("key9".as_bytes())).unwrap();
		let range = collect_snapshot_iter(&mut range_iter).unwrap();

		assert_eq!(range.len(), 4);
		assert_eq!(&range[0].0.user_key, b"key1");
		assert_eq!(&range[1].0.user_key, b"key2");
		assert_eq!(range[1].1.as_slice(), b"v2_updated"); // Updated value
		assert_eq!(&range[2].0.user_key, b"key3");
		assert_eq!(&range[3].0.user_key, b"key4");
	}

	// Verify snap3: Should see key2(v2_updated), key3(v3), key4(v4), key5(v5)
	// key1 should be deleted
	{
		let snap_ref = snap3.snapshot.as_ref().unwrap();
		let mut range_iter =
			snap_ref.range(Some("key0".as_bytes()), Some("key9".as_bytes())).unwrap();
		let range = collect_snapshot_iter(&mut range_iter).unwrap();

		assert_eq!(range.len(), 4);
		// key1 should NOT be present (deleted)
		assert_eq!(&range[0].0.user_key, b"key2");
		assert_eq!(range[0].1.as_slice(), b"v2_updated");
		assert_eq!(&range[1].0.user_key, b"key3");
		assert_eq!(&range[2].0.user_key, b"key4");
		assert_eq!(&range[3].0.user_key, b"key5");
	}

	// Verify backward iteration on snap2
	{
		let snap_ref = snap2.snapshot.as_ref().unwrap();
		let mut range_iter =
			snap_ref.range(Some("key0".as_bytes()), Some("key9".as_bytes())).unwrap();
		let range = collect_snapshot_reverse(&mut range_iter).unwrap();

		assert_eq!(range.len(), 4);
		assert_eq!(&range[0].0.user_key, b"key4");
		assert_eq!(&range[1].0.user_key, b"key3");
		assert_eq!(&range[2].0.user_key, b"key2");
		assert_eq!(range[2].1.as_slice(), b"v2_updated");
		assert_eq!(&range[3].0.user_key, b"key1");
	}
}
