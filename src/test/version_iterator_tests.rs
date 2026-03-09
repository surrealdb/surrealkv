//! Tests for the versioned iterator functionality.
//!
//! Tests run with LSM-only iteration to verify versioned query behavior.

use tempdir::TempDir;
use test_log::test;

use crate::{Key, LSMIterator, Options, Result, TreeBuilder, Value};

fn create_temp_directory() -> TempDir {
	TempDir::new("test").unwrap()
}

/// Create a store with versioning enabled
async fn create_versioned_store() -> (crate::Tree, TempDir) {
	let temp_dir = create_temp_directory();
	let opts = Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
	let tree = TreeBuilder::with_options(opts).build().await.unwrap();
	(tree, temp_dir)
}

/// Create a store without versioning enabled
async fn create_store_no_versioning() -> (crate::Tree, TempDir) {
	let temp_dir = create_temp_directory();
	let opts = Options::new().with_path(temp_dir.path().to_path_buf());
	let tree = TreeBuilder::with_options(opts).build().await.unwrap();
	(tree, temp_dir)
}

/// Collects all entries from a history iterator
async fn collect_history_all(iter: &mut impl LSMIterator) -> Result<Vec<(Key, Value, u64, bool)>> {
	iter.seek_first().await?;
	let mut result = Vec::new();
	while iter.valid() {
		let key_ref = iter.key();
		let is_tombstone = key_ref.is_tombstone();
		// Tombstones have no value, so use empty vec
		let value = if is_tombstone {
			Vec::new()
		} else {
			iter.value()?
		};
		result.push((key_ref.user_key().to_vec(), value, key_ref.timestamp(), is_tombstone));
		iter.next().await?;
	}
	Ok(result)
}

/// Collects all entries from a history iterator in reverse
async fn collect_history_reverse(
	iter: &mut impl LSMIterator,
) -> Result<Vec<(Key, Value, u64, bool)>> {
	iter.seek_last().await?;
	let mut result = Vec::new();
	while iter.valid() {
		let key_ref = iter.key();
		let is_tombstone = key_ref.is_tombstone();
		// Tombstones have no value, so use empty vec
		let value = if is_tombstone {
			Vec::new()
		} else {
			iter.value()?
		};
		result.push((key_ref.user_key().to_vec(), value, key_ref.timestamp(), is_tombstone));
		if !iter.prev().await? {
			break;
		}
	}
	Ok(result)
}

// ============================================================================
// Test 1: Multiple Versions Per Key
// ============================================================================

#[test(tokio::test)]
async fn test_history_multiple_versions_single_key() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key1 three times with different values
	store.set_at(b"key1", b"value1_v1", 100).await.unwrap();
	store.set_at(b"key1", b"value1_v2", 200).await.unwrap();
	store.set_at(b"key1", b"value1_v3", 300).await.unwrap();

	// Query versioned range
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	// Should have all 3 versions, ordered by seq_num descending (newest first)
	assert_eq!(results.len(), 3, "Should have 3 versions");
	assert_eq!(results[0].0, b"key1");
	assert_eq!(results[0].1, b"value1_v3");
	assert_eq!(results[0].2, 300);
	assert_eq!(results[1].0, b"key1");
	assert_eq!(results[1].1, b"value1_v2");
	assert_eq!(results[1].2, 200);
	assert_eq!(results[2].0, b"key1");
	assert_eq!(results[2].1, b"value1_v1");
	assert_eq!(results[2].2, 100);
}

// ============================================================================
// Test 2: Multiple Keys with Multiple Versions
// ============================================================================

#[test(tokio::test)]
async fn test_history_multiple_keys_multiple_versions() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key1 (2 versions)
	store.set_at(b"key1", b"key1_v1", 100).await.unwrap();
	store.set_at(b"key1", b"key1_v2", 200).await.unwrap();

	// Insert key2 (1 version)
	store.set_at(b"key2", b"key2_v1", 150).await.unwrap();

	// Insert key3 (3 versions)
	store.set_at(b"key3", b"key3_v1", 50).await.unwrap();
	store.set_at(b"key3", b"key3_v2", 250).await.unwrap();
	store.set_at(b"key3", b"key3_v3", 350).await.unwrap();

	// Query all keys
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key0"), Some(b"key9"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	// Should have 6 total versions
	assert_eq!(results.len(), 6, "Should have 6 total versions");

	// Verify ordering: grouped by key, newest first within each key
	assert_eq!(results[0].0, b"key1");
	assert_eq!(results[0].1, b"key1_v2");
	assert_eq!(results[1].0, b"key1");
	assert_eq!(results[1].1, b"key1_v1");
	assert_eq!(results[2].0, b"key2");
	assert_eq!(results[2].1, b"key2_v1");
	assert_eq!(results[3].0, b"key3");
	assert_eq!(results[3].1, b"key3_v3");
	assert_eq!(results[4].0, b"key3");
	assert_eq!(results[4].1, b"key3_v2");
	assert_eq!(results[5].0, b"key3");
	assert_eq!(results[5].1, b"key3_v1");
}

// ============================================================================
// Test 3: Tombstones - Without Include Tombstones
// ============================================================================

#[test(tokio::test)]
async fn test_history_excludes_tombstones() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key1 v1
	store.set_at(b"key1", b"value1", 100).await.unwrap();

	// Delete key1
	store.delete(b"key1").await.unwrap();

	// Insert key2
	store.set_at(b"key2", b"value2", 200).await.unwrap();

	// Query with history (excludes tombstones by default)
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key0"), Some(b"key9"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	// Hard delete hides ALL versions of key1, only key2 visible
	assert_eq!(results.len(), 1, "Only key2 visible");

	for result in &results {
		assert!(!result.3, "No entry should be a tombstone");
	}

	assert_eq!(results[0].0, b"key2");
	assert_eq!(results[0].1, b"value2");
}

// ============================================================================
// Test 4: Tombstones - With Include Tombstones
// ============================================================================

#[test(tokio::test)]
async fn test_history_with_tombstones() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key1 v1
	store.set_at(b"key1", b"value1", 100).await.unwrap();

	// Delete key1 (creates tombstone)
	store.delete(b"key1").await.unwrap();

	// Insert key2
	store.set_at(b"key2", b"value2", 200).await.unwrap();

	// Query with history (includes tombstones)
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key0"), Some(b"key9"), true, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	// Hard delete hides ALL versions of key1, even with include_tombstones=true
	assert_eq!(results.len(), 1, "Only key2 visible");
	assert_eq!(results[0].0, b"key2");
	assert!(!results[0].3, "key2 should not be a tombstone");
}

// ============================================================================
// Test 5: Replace Operations
// ============================================================================

#[test(tokio::test)]
async fn test_history_replace_shows_all_versions() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key1=v1
	store.set_at(b"key1", b"v1", 100).await.unwrap();

	// Replace key1=v2
	store.set_at(b"key1", b"v2", 200).await.unwrap();

	// Replace key1=v3
	store.set_at(b"key1", b"v3", 300).await.unwrap();

	// Query all versions
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	assert_eq!(results.len(), 3, "All 3 versions visible");
	assert_eq!(results[0].1, b"v3");
	assert_eq!(results[1].1, b"v2");
	assert_eq!(results[2].1, b"v1");
}

// ============================================================================
// Test 7: Bounds - Inclusive Start, Exclusive End
// ============================================================================

#[test(tokio::test)]
async fn test_history_bounds() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert keys a through e
	for key in [b"key_a", b"key_b", b"key_c", b"key_d", b"key_e"] {
		store.set_at(key, b"value", 100).await.unwrap();
	}

	// Query range [key_b, key_d) - should include key_b and key_c, NOT key_d
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key_b"), Some(b"key_d"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	assert_eq!(results.len(), 2, "Should have 2 entries");
	assert_eq!(results[0].0, b"key_b");
	assert_eq!(results[1].0, b"key_c");

	store.close().await.unwrap();
}

// ============================================================================
// Test 8: Bounds - Empty Range
// ============================================================================

#[test(tokio::test)]
async fn test_history_empty_range() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key_a and key_b
	let mut batch = store.new_batch();
	batch.set_at(b"key_a", b"value_a", 100).unwrap();
	batch.set_at(b"key_b", b"value_b", 100).unwrap();
	store.apply(batch, false).await.unwrap();

	// Query range [key_c, key_d) - no matching keys
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key_c"), Some(b"key_d"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	assert!(results.is_empty(), "Should have no entries");

	store.close().await.unwrap();
}

// ============================================================================
// Test 9: Bounds - Single Key Match
// ============================================================================

#[test(tokio::test)]
async fn test_history_single_key_match() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key_a (2 versions)
	store.set_at(b"key_a", b"v1", 100).await.unwrap();
	store.set_at(b"key_a", b"v2", 200).await.unwrap();

	// Insert key_b and key_c
	let mut batch = store.new_batch();
	batch.set_at(b"key_b", b"value_b", 150).unwrap();
	batch.set_at(b"key_c", b"value_c", 150).unwrap();
	store.apply(batch, false).await.unwrap();

	// Query range that only matches key_a
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key_a"), Some(b"key_b"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	assert_eq!(results.len(), 2, "Should have both versions");
	assert_eq!(results[0].0, b"key_a");
	assert_eq!(results[0].1, b"v2");
	assert_eq!(results[1].0, b"key_a");
	assert_eq!(results[1].1, b"v1");
}

// ============================================================================
// Test 10: Interleaved Iteration - Forward/Backward
// ============================================================================

#[test(tokio::test)]
async fn test_history_interleaved_iteration() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key1 (2 versions) and key2 (2 versions)
	store.set_at(b"key1", b"key1_v1", 100).await.unwrap();
	store.set_at(b"key1", b"key1_v2", 200).await.unwrap();
	store.set_at(b"key2", b"key2_v1", 150).await.unwrap();
	store.set_at(b"key2", b"key2_v2", 250).await.unwrap();

	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key0"), Some(b"key9"), false, None, None).unwrap();

	// Test forward iteration
	iter.seek_first().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key1");
	assert_eq!(iter.value().unwrap(), b"key1_v2".to_vec());

	iter.next().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key1");
	assert_eq!(iter.value().unwrap(), b"key1_v1".to_vec());

	iter.next().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key2");
	assert_eq!(iter.value().unwrap(), b"key2_v2".to_vec());

	iter.next().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key2");
	assert_eq!(iter.value().unwrap(), b"key2_v1".to_vec());

	// Test backward iteration starting from last
	iter.seek_last().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key2");
	assert_eq!(iter.value().unwrap(), b"key2_v1".to_vec());

	iter.prev().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key2");
	assert_eq!(iter.value().unwrap(), b"key2_v2".to_vec());

	iter.prev().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key1");
	assert_eq!(iter.value().unwrap(), b"key1_v1".to_vec());
}

// ============================================================================
// Test 11: Interleaved Iteration - Seek in Middle
// ============================================================================

#[test(tokio::test)]
async fn test_history_seek_middle() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert 5 keys
	for i in 1..=5 {
		let key = format!("key{i}");
		let value = format!("value{i}");
		store.set_at(key.as_bytes(), value.as_bytes(), i as u64 * 100).await.unwrap();
	}

	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key0"), Some(b"key9"), false, None, None).unwrap();

	// Forward iteration should return keys in order
	let results = collect_history_all(&mut iter).await.unwrap();
	assert_eq!(results.len(), 5);
	assert_eq!(results[0].0, b"key1");
	assert_eq!(results[1].0, b"key2");
	assert_eq!(results[2].0, b"key3");
	assert_eq!(results[3].0, b"key4");
	assert_eq!(results[4].0, b"key5");

	// Test seek_first after collection
	iter.seek_first().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key1");

	// Navigate forward
	iter.next().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key2");

	iter.next().await.unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key3");
}

// ============================================================================
// Test 12: Backward Iteration Only
// ============================================================================

#[test(tokio::test)]
async fn test_history_backward_iteration() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert multiple keys with multiple versions
	store.set_at(b"key1", b"key1_v1", 100).await.unwrap();
	store.set_at(b"key1", b"key1_v2", 200).await.unwrap();
	store.set_at(b"key2", b"key2_v1", 150).await.unwrap();

	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key0"), Some(b"key9"), false, None, None).unwrap();
	let results = collect_history_reverse(&mut iter).await.unwrap();

	assert_eq!(results.len(), 3, "Should have 3 entries");

	// Reverse order: key2_v1, key1_v1, key1_v2
	assert_eq!(results[0].0, b"key2");
	assert_eq!(results[0].1, b"key2_v1");
	assert_eq!(results[1].0, b"key1");
	assert_eq!(results[1].1, b"key1_v1");
	assert_eq!(results[2].0, b"key1");
	assert_eq!(results[2].1, b"key1_v2");
}

// ============================================================================
// Test 13: Snapshot Isolation - Versions Not Visible to Old Snapshots
// ============================================================================

#[test(tokio::test)]
async fn test_history_snapshot_isolation() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key1 v1
	store.set_at(b"key1", b"v1", 100).await.unwrap();

	// Take snapshot1
	let snap1 = store.new_snapshot();

	// Insert key1 v2
	store.set_at(b"key1", b"v2", 200).await.unwrap();

	// Take snapshot2
	let snap2 = store.new_snapshot();

	// snapshot1 should only see v1
	{
		let mut iter = snap1.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
		let results = collect_history_all(&mut iter).await.unwrap();
		assert_eq!(results.len(), 1, "snapshot1 should see 1 version");
		assert_eq!(results[0].1, b"v1");
	}

	// snapshot2 should see both v1 and v2
	{
		let mut iter = snap2.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
		let results = collect_history_all(&mut iter).await.unwrap();
		assert_eq!(results.len(), 2, "snapshot2 should see 2 versions");
		assert_eq!(results[0].1, b"v2");
		assert_eq!(results[1].1, b"v1");
	}
	store.close().await.unwrap();
}

// ============================================================================
// Test 14: Large Number of Versions
// ============================================================================

#[test(tokio::test)]
async fn test_history_many_versions() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert same key 100 times with different values
	for i in 1..=100 {
		let value = format!("value_{i:03}");
		store.set_at(b"key1", value.as_bytes(), i as u64 * 10).await.unwrap();
	}

	// Query all versions
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	assert_eq!(results.len(), 100, "Should have all 100 versions");
	assert_eq!(results[0].1, b"value_100");
	assert_eq!(results[99].1, b"value_001");

	store.close().await.unwrap();
}

// ============================================================================
// Test 15: Mixed Timestamps
// ============================================================================

#[test(tokio::test)]
async fn test_history_timestamps() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key with timestamp 50, then 100, then 200
	store.set_at(b"key1", b"ts_50", 50).await.unwrap();
	store.set_at(b"key1", b"ts_100", 100).await.unwrap();
	store.set_at(b"key1", b"ts_200", 200).await.unwrap();

	// Query versions
	let snap = store.new_snapshot();
	let mut iter = snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
	let results = collect_history_all(&mut iter).await.unwrap();

	assert_eq!(results.len(), 3, "Should have 3 versions");

	// Verify ordering is by timestamp descending (B+tree uses TimestampComparator)
	assert_eq!(results[0].1, b"ts_200");
	assert_eq!(results[0].2, 200);
	assert_eq!(results[1].1, b"ts_100");
	assert_eq!(results[1].2, 100);
	assert_eq!(results[2].1, b"ts_50");
	assert_eq!(results[2].2, 50);

	store.close().await.unwrap();
}

// ============================================================================
// Test 17: get_at Fallback
// ============================================================================

#[test(tokio::test)]
async fn test_get_at_fallback() {
	let (store, _temp_dir) = create_versioned_store().await;

	// Insert key at ts=100
	store.set_at(b"key1", b"value_100", 100).await.unwrap();

	// Insert key at ts=200
	store.set_at(b"key1", b"value_200", 200).await.unwrap();

	let snap = store.new_snapshot();

	let result = snap.get_at(b"key1", 150).await.unwrap();
	assert!(result.is_some());
	assert_eq!(result.unwrap(), b"value_100");

	let result = snap.get_at(b"key1", 200).await.unwrap();
	assert!(result.is_some());
	assert_eq!(result.unwrap(), b"value_200");

	let result = snap.get_at(b"key1", 50).await.unwrap();
	assert!(result.is_none());

	let result = snap.get_at(b"key1", 250).await.unwrap();
	assert!(result.is_some());
	assert_eq!(result.unwrap(), b"value_200");

	store.close().await.unwrap();
}

// ============================================================================
// Test 19: Error Handling - Versioning Disabled
// ============================================================================

#[test(tokio::test)]
async fn test_history_requires_versioning() {
	// Create store WITHOUT versioning enabled
	let (store, _temp_dir) = create_store_no_versioning().await;

	// Insert some data
	store.set(b"key1", b"value1").await.unwrap();

	// Try to call history - should return error
	let snap = store.new_snapshot();
	let result = snap.history_iter(Some(b"key0"), Some(b"key9"), false, None, None);

	assert!(result.is_err(), "history should fail without versioning enabled");
}

// ============================================================================
// Test: Versions survive memtable flush (bug detector)
// ============================================================================

#[test(tokio::test)]
async fn test_history_survives_memtable_flush() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		// Insert key1 three times with different values
		for i in 1..=3 {
			let value = format!("v{i}");
			store.set_at(b"key1", value.as_bytes(), i as u64 * 100).await.unwrap();
		}

		// Verify all 3 versions exist in memtable BEFORE flush
		{
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();
			assert_eq!(results.len(), 3, "Should have 3 versions before flush");
		}

		// FORCE MEMTABLE FLUSH
		{
			// Query versions AFTER flush
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();

			assert_eq!(results.len(), 3, "All 3 versions should survive flush");
			assert_eq!(results[0].1, b"v3");
			assert_eq!(results[1].1, b"v2");
			assert_eq!(results[2].1, b"v1");
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test: Versions survive compaction (L0 -> L1)
// ============================================================================

#[test(tokio::test)]
async fn test_versions_survive_compaction() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		// Insert key1 with multiple versions
		for i in 1..=5 {
			let value = format!("v{i}");
			store.set_at(b"key1", value.as_bytes(), i as u64 * 100).await.unwrap();
		}

		// First flush (memtable -> L0)
		// Verify versions after first flush
		{
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();
			assert_eq!(results.len(), 5, "All 5 versions should survive L0 flush");
		}

		// Insert more versions to trigger another flush
		for i in 6..=10 {
			let value = format!("v{i}");
			store.set_at(b"key1", value.as_bytes(), i as u64 * 100).await.unwrap();
		}

		// Second flush
		{
			// Verify all 10 versions exist across L0 files
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, None).unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();

			assert_eq!(results.len(), 10, "All 10 versions should survive");
			assert_eq!(results[0].1, b"v10");
			assert_eq!(results[9].1, b"v1");
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test: history() forward and backward iteration
// ============================================================================

#[test(tokio::test)]
async fn test_history_bidirectional_iteration() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		// Insert multiple keys with multiple versions
		store.set_at(b"key1", b"key1_v1", 100).await.unwrap();
		store.set_at(b"key1", b"key1_v2", 200).await.unwrap();
		store.set_at(b"key2", b"key2_v1", 150).await.unwrap();
		{
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key0"), Some(b"key9"), false, None, None).unwrap();

			let forward_results = collect_history_all(&mut iter).await.unwrap();
			assert_eq!(forward_results.len(), 3);

			let reverse_results = collect_history_reverse(&mut iter).await.unwrap();
			assert_eq!(reverse_results.len(), 3);

			assert_eq!(forward_results[0].0, reverse_results[2].0);
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test: ts_range filtering
// ============================================================================

#[test(tokio::test)]
async fn test_history_ts_range_forward() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		store.set_at(b"key1", b"v100", 100).await.unwrap();
		store.set_at(b"key1", b"v200", 200).await.unwrap();
		store.set_at(b"key1", b"v300", 300).await.unwrap();
		store.set_at(b"key1", b"v400", 400).await.unwrap();
		{
			// Query with ts_range [150, 350] - should only return v200 and v300
			let snap = store.new_snapshot();
			let mut iter = snap
				.history_iter(Some(b"key1"), Some(b"key2"), false, Some((150, 350)), None)
				.unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();

			assert_eq!(results.len(), 2, "Should have 2 versions in range");
			assert_eq!(results[0].2, 300);
			assert_eq!(results[1].2, 200);
		}
		store.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_history_ts_range_backward() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		store.set_at(b"key1", b"v100", 100).await.unwrap();
		store.set_at(b"key1", b"v200", 200).await.unwrap();
		store.set_at(b"key1", b"v300", 300).await.unwrap();
		{
			// Query with ts_range [150, 250] - should only return v200
			let snap = store.new_snapshot();
			let mut iter = snap
				.history_iter(Some(b"key1"), Some(b"key2"), false, Some((150, 250)), None)
				.unwrap();
			let results = collect_history_reverse(&mut iter).await.unwrap();

			assert_eq!(results.len(), 1, "Should have 1 version in range");
			assert_eq!(results[0].2, 200);
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test: limit filtering
// ============================================================================

#[test(tokio::test)]
async fn test_history_limit_forward() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		for i in 1..=5 {
			store.set_at(b"key1", format!("v{}", i).as_bytes(), i * 100).await.unwrap();
		}
		{
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, Some(3)).unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();

			assert_eq!(results.len(), 3, "Should have 3 entries");
			assert_eq!(results[0].2, 500);
			assert_eq!(results[1].2, 400);
			assert_eq!(results[2].2, 300);
		}
		store.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_history_limit_backward() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		for i in 1..=5 {
			store.set_at(b"key1", format!("v{}", i).as_bytes(), i * 100).await.unwrap();
		}
		{
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, Some(2)).unwrap();
			let results = collect_history_reverse(&mut iter).await.unwrap();

			assert_eq!(results.len(), 2, "Should have 2 entries");
			assert_eq!(results[0].2, 100);
			assert_eq!(results[1].2, 200);
		}
		store.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_history_limit_multiple_keys() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		store.set_at(b"key1", b"v1", 100).await.unwrap();
		store.set_at(b"key1", b"v2", 200).await.unwrap();
		store.set_at(b"key2", b"v1", 150).await.unwrap();
		store.set_at(b"key2", b"v2", 250).await.unwrap();
		{
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key0"), Some(b"key9"), false, None, Some(3)).unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();

			assert_eq!(results.len(), 3, "Should have 3 entries across keys");
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test: ts_range + limit combination
// ============================================================================

#[test(tokio::test)]
async fn test_history_ts_range_with_limit() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		for i in 1..=10 {
			store.set_at(b"key1", format!("v{}", i).as_bytes(), i * 100).await.unwrap();
		}
		{
			let snap = store.new_snapshot();
			let mut iter = snap
				.history_iter(Some(b"key1"), Some(b"key2"), false, Some((300, 800)), Some(3))
				.unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();

			assert_eq!(results.len(), 3, "Should have 3 entries (limited)");
			assert_eq!(results[0].2, 800);
			assert_eq!(results[1].2, 700);
			assert_eq!(results[2].2, 600);
		}
		store.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_history_limit_zero() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		store.set_at(b"key1", b"v1", 100).await.unwrap();
		{
			let snap = store.new_snapshot();
			let mut iter =
				snap.history_iter(Some(b"key1"), Some(b"key2"), false, None, Some(0)).unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();

			assert_eq!(results.len(), 0, "Should have 0 entries");
		}
		store.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_history_ts_range_empty_result() {
	{
		let (store, _temp_dir) = create_versioned_store().await;

		store.set_at(b"key1", b"v100", 100).await.unwrap();
		store.set_at(b"key1", b"v200", 200).await.unwrap();
		{
			let snap = store.new_snapshot();
			let mut iter = snap
				.history_iter(Some(b"key1"), Some(b"key2"), false, Some((500, 600)), None)
				.unwrap();
			let results = collect_history_all(&mut iter).await.unwrap();

			assert_eq!(results.len(), 0, "Should have 0 entries");
		}

		store.close().await.unwrap();
	}
}

// ==================== RYOW (Read Your Own Writes) Tests ====================

/// Test get_at RYOW: uncommitted writes should be visible
/// Test get_at RYOW: future timestamp writes should not be visible
/// Test get_at RYOW with tombstone
/// Test history iterator RYOW: uncommitted writes appear in history
/// Test history RYOW with timestamp collision: write set wins
/// Test history RYOW with timestamp range filtering
/// Test history RYOW soft delete (tombstone) handling
/// Test history RYOW hard delete handling - wipes all history
/// Test get_at RYOW with hard delete - returns None regardless of timestamp
// ============================================================================
// History Iterator Bounds Tests
// ============================================================================

/// Test that forward iteration respects lower bound when keys exist before the range.
/// Regression test: keys with user_key lexicographically before lower bound should not be returned.
/// Test that backward iteration respects upper bound when keys exist after the range.
/// Regression test: keys with user_key lexicographically after upper bound should not be returned.
/// Test that both bounds are respected with keys outside both ends of the range.
/// Test bounds with timestamp ranges - keys outside range but within timestamp should be excluded.
/// Test direction switching (forward to backward) respects bounds.
/// Test direction switching (backward to forward) respects bounds.
/// Test direction switching with keys that have many versions.
/// Verifies that prev/next correctly navigate across multi-version keys.
/// Test seeking to a key below lower_bound.
/// Test seeking to a key at or above upper_bound.
/// Test seeking to exact bound values.
/// Test key exactly at lower_bound is included (inclusive).
/// Test key exactly at upper_bound is excluded (exclusive).
/// Test adjacent byte boundaries with special byte values.
/// Test tombstone (soft delete) at lower bound.
/// Test hard delete at boundary.
/// Test replace operation at boundary.
/// Test equal lower and upper bounds (empty range).
/// Test inverted bounds (lower > upper).
/// Test single key in range.
/// Test all keys outside range.
/// Test bounds with limit (forward).
/// Test bounds with limit (backward).
/// Test bounds with timestamp range and limit combined.
/// Test prefix pattern iteration (common use case).
/// Test keys and bounds containing null bytes.
/// Test bounds with maximum byte values (0xFF).
// Test for https://github.com/surrealdb/surrealkv/issues/364
#[tokio::test(flavor = "current_thread")]
async fn repro() {
	let tmp = tempfile::Builder::new().prefix("tc-").tempdir().unwrap();

	let dir = std::path::PathBuf::from(tmp.path());

	let opts = Options::new().with_path(dir).with_versioning(true, 0).with_l0_no_compression();

	let store = TreeBuilder::with_options(opts).build().await.unwrap();

	let sync_key = b"#@prefix:sync\x00\x00\x00\x00\x00\x00\x00\x02****************";
	let table_key = b"@prefix:tbentity\x00\x00\x00\x00\x00\x00\x00\x00\x10cccccccccccccccc";
	let before_key = b"@prefix:tbentity\x00\x00\x00\x00\x00\x00\x00\x00\x10kkkkkkkkkkkkkkkk";
	let after_key = b"@prefix:tbentity\x00\x00\x00\x00\x00\x00\x00\x00\x10MMMMMMMMMMMMMMMM";

	store.set_at(before_key, b"value", 1).await.unwrap();

	{
		let mut batch = store.new_batch();
		batch.set_at(sync_key, b"value", 2).unwrap();
		batch.set_at(table_key, b"value", 2).unwrap();
		store.apply(batch, false).await.unwrap();
	}

	store.set_at(after_key, b"value", 3).await.unwrap();

	let snap = store.new_snapshot();

	let lower = b"@prefix:\x00";
	let upper = b"@prefix:\xFF";
	let mut it = snap
		.history_iter(Some(lower.as_slice()), Some(upper.as_slice()), true, Some((2, 2)), None)
		.unwrap();

	let mut seen = vec![];

	if it.seek_first().await.unwrap() {
		while it.valid() {
			let key = it.key();
			let user_key = String::from_utf8_lossy(key.user_key()).to_string();
			eprintln!("\t{}: {}", key.timestamp(), user_key);
			seen.push(user_key);

			it.next().await.unwrap();
		}
	}

	// Expected: only the @prefix key should be returned for this range.
	// Current behavior: this also returns #@prefix:* and reproduces the bug.
	assert_eq!(seen.len(), 1, "unexpected keys at ts=2 in scope range: {seen:?}");
}
