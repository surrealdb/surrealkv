//! Tests for the LSM-based versioned iterator functionality.
//!
//! These tests verify that `TransactionHistoryIterator` correctly returns
//! all versions of keys from the LSM tree without relying on the B+tree index.

use tempdir::TempDir;
use test_log::test;

use crate::transaction::{HistoryOptions, Mode};
use crate::{Key, LSMIterator, Options, Result, TreeBuilder, Value};

fn create_temp_directory() -> TempDir {
	TempDir::new("test").unwrap()
}

/// Create a store with versioning enabled but B+tree index disabled
fn create_versioned_store_no_index() -> (crate::lsm::Tree, TempDir) {
	let temp_dir = create_temp_directory();
	let opts = Options::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_versioning(true, 0)
		.with_versioned_index(false); // Test LSM-based iteration without B+tree
	let tree = TreeBuilder::with_options(opts).build().unwrap();
	(tree, temp_dir)
}

/// Create a store with versioning and B+tree index both enabled
fn create_versioned_store_with_index() -> (crate::lsm::Tree, TempDir) {
	let temp_dir = create_temp_directory();
	let opts = Options::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_versioning(true, 0)
		.with_versioned_index(true);
	let tree = TreeBuilder::with_options(opts).build().unwrap();
	(tree, temp_dir)
}

/// Create a store without versioning enabled
fn create_store_no_versioning() -> (crate::lsm::Tree, TempDir) {
	let temp_dir = create_temp_directory();
	let opts = Options::new().with_path(temp_dir.path().to_path_buf());
	let tree = TreeBuilder::with_options(opts).build().unwrap();
	(tree, temp_dir)
}

/// Collects all entries from a history iterator
fn collect_history_all(iter: &mut impl LSMIterator) -> Result<Vec<(Key, Value, u64, bool)>> {
	iter.seek_first()?;
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
		iter.next()?;
	}
	Ok(result)
}

/// Collects all entries from a history iterator in reverse
fn collect_history_reverse(iter: &mut impl LSMIterator) -> Result<Vec<(Key, Value, u64, bool)>> {
	iter.seek_last()?;
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
		if !iter.prev()? {
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
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key1 three times with different values
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1_v1", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1_v2", 200).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1_v3", 300).unwrap();
		tx.commit().await.unwrap();
	}

	// Query versioned range
	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key1", b"key2").unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// Should have all 3 versions, ordered by seq_num descending (newest first)
	assert_eq!(results.len(), 3, "Should have 3 versions");
	assert_eq!(results[0].0, b"key1");
	assert_eq!(results[0].1, b"value1_v3");
	assert_eq!(results[0].2, 300); // timestamp
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
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key1 (2 versions)
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"key1_v1", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"key1_v2", 200).unwrap();
		tx.commit().await.unwrap();
	}

	// Insert key2 (1 version)
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key2", b"key2_v1", 150).unwrap();
		tx.commit().await.unwrap();
	}

	// Insert key3 (3 versions)
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key3", b"key3_v1", 50).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key3", b"key3_v2", 250).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key3", b"key3_v3", 350).unwrap();
		tx.commit().await.unwrap();
	}

	// Query all keys
	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key0", b"key9").unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// Should have 6 total versions
	assert_eq!(results.len(), 6, "Should have 6 total versions");

	// Verify ordering: grouped by key, newest first within each key
	// key1 should come first (2 versions)
	assert_eq!(results[0].0, b"key1");
	assert_eq!(results[0].1, b"key1_v2");
	assert_eq!(results[1].0, b"key1");
	assert_eq!(results[1].1, b"key1_v1");

	// key2 (1 version)
	assert_eq!(results[2].0, b"key2");
	assert_eq!(results[2].1, b"key2_v1");

	// key3 (3 versions)
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
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key1 v1
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Delete key1
	{
		let mut tx = store.begin().unwrap();
		tx.delete(b"key1").unwrap();
		tx.commit().await.unwrap();
	}

	// Insert key2
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key2", b"value2", 200).unwrap();
		tx.commit().await.unwrap();
	}

	// Query with history (excludes tombstones by default)
	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key0", b"key9").unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// Hard delete hides ALL versions of key1, only key2 visible
	assert_eq!(results.len(), 1, "Only key2 visible - key1 hard deleted");

	// Verify no tombstones in results
	for result in &results {
		assert!(!result.3, "No entry should be a tombstone");
	}

	// Only key2 should be present (key1 is hard deleted)
	assert_eq!(results[0].0, b"key2");
	assert_eq!(results[0].1, b"value2");
}

// ============================================================================
// Test 4: Tombstones - With Include Tombstones
// ============================================================================

#[test(tokio::test)]
async fn test_history_with_tombstones() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key1 v1
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Delete key1 (creates tombstone)
	{
		let mut tx = store.begin().unwrap();
		tx.delete(b"key1").unwrap();
		tx.commit().await.unwrap();
	}

	// Insert key2
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key2", b"value2", 200).unwrap();
		tx.commit().await.unwrap();
	}

	// Query with history_with_options (includes tombstones)
	let tx = store.begin().unwrap();
	let opts = HistoryOptions {
		include_tombstones: true,
		..Default::default()
	};
	let mut iter = tx.history_with_options(b"key0", b"key9", &opts).unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// Hard delete hides ALL versions of key1, even with include_tombstones=true
	// include_tombstones only affects SOFT deletes, not hard deletes
	assert_eq!(results.len(), 1, "Only key2 visible - hard delete hides all");

	// Only key2 should be present
	assert_eq!(results[0].0, b"key2");
	assert!(!results[0].3, "key2 should not be a tombstone");
}

// ============================================================================
// Test 5: Replace Operations
// ============================================================================

#[test(tokio::test)]
async fn test_history_replace_shows_all_versions() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key1=v1
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v1", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Replace key1=v2
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v2", 200).unwrap();
		tx.commit().await.unwrap();
	}

	// Replace key1=v3
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v3", 300).unwrap();
		tx.commit().await.unwrap();
	}

	// Query all versions
	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key1", b"key2").unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// All 3 versions should be visible
	assert_eq!(results.len(), 3, "All 3 replace operations should be visible");
	assert_eq!(results[0].1, b"v3");
	assert_eq!(results[1].1, b"v2");
	assert_eq!(results[2].1, b"v1");
}

// ============================================================================
// Test 6: Soft Delete vs Hard Delete
// ============================================================================

#[test(tokio::test)]
async fn test_history_soft_delete_vs_hard_delete() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key1 v1
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Soft delete key1
	{
		let mut tx = store.begin().unwrap();
		tx.soft_delete(b"key1").unwrap();
		tx.commit().await.unwrap();
	}

	// Insert key2
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key2", b"value2", 200).unwrap();
		tx.commit().await.unwrap();
	}

	// Hard delete key2
	{
		let mut tx = store.begin().unwrap();
		tx.delete(b"key2").unwrap();
		tx.commit().await.unwrap();
	}

	// Query with tombstones
	let tx = store.begin().unwrap();
	let opts = HistoryOptions {
		include_tombstones: true,
		..Default::default()
	};
	let mut iter = tx.history_with_options(b"key0", b"key9", &opts).unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// key1 (soft delete): tombstone + value = 2 entries
	// key2 (hard delete): hidden = 0 entries
	// include_tombstones only affects SOFT deletes, hard deletes are always hidden
	assert_eq!(results.len(), 2, "Only key1 (soft deleted) visible");

	// key1 tombstone and value
	assert_eq!(results[0].0, b"key1");
	assert!(results[0].3, "key1 first should be tombstone");
	assert_eq!(results[1].0, b"key1");
	assert!(!results[1].3, "key1 second should be value");

	// key2 is hard deleted - not visible at all
}

// ============================================================================
// Test 7: Bounds - Inclusive Start, Exclusive End
// ============================================================================

#[test(tokio::test)]
async fn test_history_bounds() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert keys a through e
	for key in [b"key_a", b"key_b", b"key_c", b"key_d", b"key_e"] {
		let mut tx = store.begin().unwrap();
		tx.set_at(key, b"value", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Query range [key_b, key_d) - should include key_b and key_c, NOT key_d
	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key_b", b"key_d").unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	assert_eq!(results.len(), 2, "Should have 2 entries");
	assert_eq!(results[0].0, b"key_b");
	assert_eq!(results[1].0, b"key_c");
}

// ============================================================================
// Test 8: Bounds - Empty Range
// ============================================================================

#[test(tokio::test)]
async fn test_history_empty_range() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key_a and key_b
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key_a", b"value_a", 100).unwrap();
		tx.set_at(b"key_b", b"value_b", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Query range [key_c, key_d) - no matching keys
	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key_c", b"key_d").unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	assert!(results.is_empty(), "Should have no entries for empty range");
}

// ============================================================================
// Test 9: Bounds - Single Key Match
// ============================================================================

#[test(tokio::test)]
async fn test_history_single_key_match() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key_a (2 versions)
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key_a", b"v1", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key_a", b"v2", 200).unwrap();
		tx.commit().await.unwrap();
	}

	// Insert key_b and key_c
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key_b", b"value_b", 150).unwrap();
		tx.set_at(b"key_c", b"value_c", 150).unwrap();
		tx.commit().await.unwrap();
	}

	// Query range that only matches key_a
	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key_a", b"key_b").unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// Both versions of key_a should be returned
	assert_eq!(results.len(), 2, "Should have both versions of key_a");
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
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key1 (2 versions) and key2 (2 versions)
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"key1_v1", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"key1_v2", 200).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key2", b"key2_v1", 150).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key2", b"key2_v2", 250).unwrap();
		tx.commit().await.unwrap();
	}

	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key0", b"key9").unwrap();

	// Test forward iteration
	iter.seek_first().unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key1");
	assert_eq!(iter.value().unwrap(), b"key1_v2".to_vec());

	iter.next().unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key1");
	assert_eq!(iter.value().unwrap(), b"key1_v1".to_vec());

	iter.next().unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key2");
	assert_eq!(iter.value().unwrap(), b"key2_v2".to_vec());

	iter.next().unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key2");
	assert_eq!(iter.value().unwrap(), b"key2_v1".to_vec());

	// Test backward iteration starting from last
	iter.seek_last().unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key2");
	assert_eq!(iter.value().unwrap(), b"key2_v1".to_vec());

	iter.prev().unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key2");
	assert_eq!(iter.value().unwrap(), b"key2_v2".to_vec());

	iter.prev().unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key1");
	assert_eq!(iter.value().unwrap(), b"key1_v1".to_vec());
}

// ============================================================================
// Test 11: Interleaved Iteration - Seek in Middle
// ============================================================================

#[test(tokio::test)]
async fn test_history_seek_middle() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert 5 keys
	for i in 1..=5 {
		let mut tx = store.begin().unwrap();
		let key = format!("key{i}");
		let value = format!("value{i}");
		tx.set_at(key.as_bytes(), value.as_bytes(), i as u64 * 100).unwrap();
		tx.commit().await.unwrap();
	}

	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key0", b"key9").unwrap();

	// Forward iteration should return keys in order
	let results = collect_history_all(&mut iter).unwrap();
	assert_eq!(results.len(), 5);
	assert_eq!(results[0].0, b"key1");
	assert_eq!(results[1].0, b"key2");
	assert_eq!(results[2].0, b"key3");
	assert_eq!(results[3].0, b"key4");
	assert_eq!(results[4].0, b"key5");

	// Test seek_first after collection
	iter.seek_first().unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key1");

	// Navigate forward
	iter.next().unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key2");

	iter.next().unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"key3");
}

// ============================================================================
// Test 12: Backward Iteration Only
// ============================================================================

#[test(tokio::test)]
async fn test_history_backward_iteration() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert multiple keys with multiple versions
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"key1_v1", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"key1_v2", 200).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key2", b"key2_v1", 150).unwrap();
		tx.commit().await.unwrap();
	}

	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key0", b"key9").unwrap();
	let results = collect_history_reverse(&mut iter).unwrap();

	// Should have 3 entries in reverse order
	assert_eq!(results.len(), 3);

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
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key1 v1
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v1", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Take snapshot1
	let tx1 = store.begin().unwrap();

	// Insert key1 v2
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v2", 200).unwrap();
		tx.commit().await.unwrap();
	}

	// Take snapshot2
	let tx2 = store.begin().unwrap();

	// snapshot1 should only see v1
	{
		let mut iter = tx1.history(b"key1", b"key2").unwrap();
		let results = collect_history_all(&mut iter).unwrap();
		assert_eq!(results.len(), 1, "snapshot1 should only see 1 version");
		assert_eq!(results[0].1, b"v1");
	}

	// snapshot2 should see both v1 and v2
	{
		let mut iter = tx2.history(b"key1", b"key2").unwrap();
		let results = collect_history_all(&mut iter).unwrap();
		assert_eq!(results.len(), 2, "snapshot2 should see 2 versions");
		assert_eq!(results[0].1, b"v2");
		assert_eq!(results[1].1, b"v1");
	}
}

// ============================================================================
// Test 14: Large Number of Versions
// ============================================================================

#[test(tokio::test)]
async fn test_history_many_versions() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert same key 100 times with different values
	for i in 0..100 {
		let mut tx = store.begin().unwrap();
		let value = format!("value_{i:03}");
		tx.set_at(b"key1", value.as_bytes(), i as u64 * 10).unwrap();
		tx.commit().await.unwrap();
	}

	// Query all versions
	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key1", b"key2").unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// Should have all 100 versions
	assert_eq!(results.len(), 100, "Should have all 100 versions");

	// Newest first (value_099, value_098, ..., value_000)
	assert_eq!(results[0].1, b"value_099");
	assert_eq!(results[99].1, b"value_000");
}

// ============================================================================
// Test 15: Mixed Timestamps
// ============================================================================

#[test(tokio::test)]
async fn test_history_timestamps() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key with timestamp 100, then 50, then 200
	// Note: seq_num order != timestamp order
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"ts_100", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"ts_50", 50).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"ts_200", 200).unwrap();
		tx.commit().await.unwrap();
	}

	// Query versions
	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key1", b"key2").unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// Should have 3 versions ordered by seq_num (insert order), not timestamp
	assert_eq!(results.len(), 3);

	// Verify ordering is by seq_num descending (newest insert first)
	assert_eq!(results[0].1, b"ts_200");
	assert_eq!(results[0].2, 200); // timestamp
	assert_eq!(results[1].1, b"ts_50");
	assert_eq!(results[1].2, 50);
	assert_eq!(results[2].1, b"ts_100");
	assert_eq!(results[2].2, 100);
}

// ============================================================================
// Test 16: Entry Convenience Method
// Uses soft_delete to test include_tombstones (hard delete hides all versions)
// ============================================================================

#[test(tokio::test)]
async fn test_history_entry_method() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert a key
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Soft delete the key (creates tombstone that can be shown with include_tombstones)
	{
		let mut tx = store.begin().unwrap();
		tx.soft_delete(b"key1").unwrap();
		tx.commit().await.unwrap();
	}

	// Query with tombstones
	let tx = store.begin().unwrap();
	let opts = HistoryOptions {
		include_tombstones: true,
		..Default::default()
	};
	let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();

	iter.seek_first().unwrap();
	assert!(iter.valid());

	// Use LSMIterator API
	let key_ref = iter.key();
	let is_tombstone = key_ref.is_tombstone();
	assert_eq!(key_ref.user_key(), b"key1");
	assert!(is_tombstone, "First entry should be tombstone");

	iter.next().unwrap();
	assert!(iter.valid());

	let key_ref = iter.key();
	let value = iter.value().unwrap();
	let timestamp = key_ref.timestamp();
	let is_tombstone = key_ref.is_tombstone();
	assert_eq!(key_ref.user_key(), b"key1");
	assert_eq!(value.as_slice(), b"value1");
	assert_eq!(timestamp, 100);
	assert!(!is_tombstone, "Second entry should not be tombstone");
}

// ============================================================================
// Test 17: get_at LSM Fallback
// ============================================================================

#[test(tokio::test)]
async fn test_get_at_lsm_fallback() {
	// Create store with versioning but WITHOUT B+tree index
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key at ts=100
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value_100", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Insert key at ts=200
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value_200", 200).unwrap();
		tx.commit().await.unwrap();
	}

	let tx = store.begin().unwrap();

	// get_at(key, 150) should return version at ts=100
	let result = tx.get_at(b"key1", 150).unwrap();
	assert!(result.is_some());
	assert_eq!(result.unwrap(), b"value_100");

	// get_at(key, 200) should return version at ts=200
	let result = tx.get_at(b"key1", 200).unwrap();
	assert!(result.is_some());
	assert_eq!(result.unwrap(), b"value_200");

	// get_at(key, 50) should return None (no version at or before ts=50)
	let result = tx.get_at(b"key1", 50).unwrap();
	assert!(result.is_none());

	// get_at(key, 250) should return latest (ts=200)
	let result = tx.get_at(b"key1", 250).unwrap();
	assert!(result.is_some());
	assert_eq!(result.unwrap(), b"value_200");
}

// ============================================================================
// Test 18: get_at with Tombstone in LSM
// ============================================================================

#[test(tokio::test)]
async fn test_get_at_lsm_tombstone() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key at ts=100
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value_100", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Delete at ts=200 (creates tombstone)
	{
		let mut tx = store.begin().unwrap();
		tx.soft_delete(b"key1").unwrap();
		tx.commit().await.unwrap();
	}

	// Insert at ts=300
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value_300", 300).unwrap();
		tx.commit().await.unwrap();
	}

	let tx = store.begin().unwrap();

	// get_at(key, 150) returns value at ts=100
	let result = tx.get_at(b"key1", 150).unwrap();
	assert!(result.is_some());
	assert_eq!(result.unwrap(), b"value_100");

	// get_at(key, 350) returns value at ts=300
	let result = tx.get_at(b"key1", 350).unwrap();
	assert!(result.is_some());
	assert_eq!(result.unwrap(), b"value_300");
}

// ============================================================================
// Test 19: Error Handling - Versioning Disabled
// ============================================================================

#[test(tokio::test)]
async fn test_history_requires_versioning() {
	// Create store WITHOUT versioning enabled
	let (store, _temp_dir) = create_store_no_versioning();

	// Insert some data
	{
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.commit().await.unwrap();
	}

	// Try to call history - should return error
	let tx = store.begin().unwrap();
	let result = tx.history(b"key0", b"key9");

	assert!(result.is_err(), "history should fail without versioning enabled");
}

// ============================================================================
// Test 20: Error Handling - Transaction States
// ============================================================================

#[test(tokio::test)]
async fn test_history_transaction_states() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert some data
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Test on closed transaction
	{
		let tx = store.begin().unwrap();
		drop(tx); // Transaction is dropped/closed

		// Cannot test closed transaction directly since drop consumes it
		// But we can test that a new transaction works fine
		let tx = store.begin().unwrap();
		let result = tx.history(b"key0", b"key9");
		assert!(result.is_ok(), "Fresh transaction should work");
	}

	// Test on write-only transaction
	{
		let tx = store.begin_with_mode(Mode::WriteOnly).unwrap();
		let result = tx.history(b"key0", b"key9");
		assert!(result.is_err(), "Write-only transaction should not allow history");
	}
}

// ============================================================================
// Test: Works with B+tree index enabled too
// ============================================================================

#[test(tokio::test)]
async fn test_history_with_btree_index() {
	let (store, _temp_dir) = create_versioned_store_with_index();

	// Insert key1 multiple times
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v1", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v2", 200).unwrap();
		tx.commit().await.unwrap();
	}

	// history should still work (uses LSM, not B+tree)
	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key1", b"key2").unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	assert_eq!(results.len(), 2, "Should have 2 versions");
	assert_eq!(results[0].1, b"v2");
	assert_eq!(results[1].1, b"v1");
}

// ============================================================================
// Test: Versions survive memtable flush (bug detector)
// This test catches the bug where versioning=false in memtable flush
// causes older versions to be dropped.
// ============================================================================

#[test(tokio::test)]
async fn test_history_survives_memtable_flush() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key1 three times with different values
	for i in 1..=3 {
		let mut tx = store.begin().unwrap();
		let value = format!("v{i}");
		tx.set_at(b"key1", value.as_bytes(), i as u64 * 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Verify all 3 versions exist in memtable BEFORE flush
	{
		let tx = store.begin().unwrap();
		let mut iter = tx.history(b"key1", b"key2").unwrap();
		let results = collect_history_all(&mut iter).unwrap();
		assert_eq!(results.len(), 3, "Should have 3 versions before flush");
	}

	// FORCE MEMTABLE FLUSH - this is where versions get dropped with the bug!
	store.flush().unwrap();

	// Query versions AFTER flush - this is the critical test
	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key1", b"key2").unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// BUG: With versioning=false in flush, only 1 version survives
	// EXPECTED: All 3 versions should survive
	assert_eq!(results.len(), 3, "All 3 versions should survive memtable flush");
	assert_eq!(results[0].1, b"v3");
	assert_eq!(results[1].1, b"v2");
	assert_eq!(results[2].1, b"v1");
}

// ============================================================================
// Test: Replace operation cuts off older versions (even with versioning)
// This is the expected behavior - Replace is a "destructive" operation
// ============================================================================

#[test(tokio::test)]
async fn test_replace_cuts_off_history_with_versioning() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key1 with SET operations
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"set_v1", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"set_v2", 200).unwrap();
		tx.commit().await.unwrap();
	}

	// Now use REPLACE - this should cut off the history
	{
		let mut tx = store.begin().unwrap();
		tx.replace_with_options(
			b"key1",
			b"replace_v3",
			&crate::WriteOptions::default().with_timestamp(Some(300)),
		)
		.unwrap();
		tx.commit().await.unwrap();
	}

	// Before flush - REPLACE filters immediately during iteration (not after compaction)
	{
		let tx = store.begin().unwrap();
		let mut iter = tx.history(b"key1", b"key2").unwrap();
		let results = collect_history_all(&mut iter).unwrap();
		// REPLACE cuts off history immediately during iteration
		assert_eq!(results.len(), 1, "Before flush: REPLACE filters immediately");
		assert_eq!(results[0].1, b"replace_v3");
		assert_eq!(results[0].2, 300);
	}

	// Force flush
	store.flush().unwrap();

	// After flush - same result (REPLACE already filtered during iteration)
	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key1", b"key2").unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// Replace cuts off history - only the Replace version survives
	assert_eq!(results.len(), 1, "After flush: only Replace version survives");
	assert_eq!(results[0].1, b"replace_v3");
	assert_eq!(results[0].2, 300);
}

// ============================================================================
// Test: Multiple Replace operations are all preserved with versioning
// ============================================================================

#[test(tokio::test)]
async fn test_multiple_replaces_preserved_with_versioning() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert with SET first
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"set_v1", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Multiple Replace operations
	{
		let mut tx = store.begin().unwrap();
		tx.replace_with_options(
			b"key1",
			b"replace_v2",
			&crate::WriteOptions::default().with_timestamp(Some(200)),
		)
		.unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.replace_with_options(
			b"key1",
			b"replace_v3",
			&crate::WriteOptions::default().with_timestamp(Some(300)),
		)
		.unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.replace_with_options(
			b"key1",
			b"replace_v4",
			&crate::WriteOptions::default().with_timestamp(Some(400)),
		)
		.unwrap();
		tx.commit().await.unwrap();
	}

	// Force flush
	store.flush().unwrap();

	// After flush - latest Replace version should survive, but not the original SET
	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key1", b"key2").unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// Latest Replace version is kept
	// Only the initial SET is discarded
	assert_eq!(results.len(), 1, "Latest Replace versions should survive");
	assert_eq!(results[0].1, b"replace_v4");
}

// ============================================================================
// Test: Replace after Delete with versioning
// ============================================================================

#[test(tokio::test)]
async fn test_replace_after_delete_with_versioning() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key1
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"set_v1", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Delete key1
	{
		let mut tx = store.begin().unwrap();
		tx.delete(b"key1").unwrap();
		tx.commit().await.unwrap();
	}

	// Replace key1 (after delete)
	{
		let mut tx = store.begin().unwrap();
		tx.replace_with_options(
			b"key1",
			b"replace_v3",
			&crate::WriteOptions::default().with_timestamp(Some(300)),
		)
		.unwrap();
		tx.commit().await.unwrap();
	}

	// Force flush
	store.flush().unwrap();

	// After flush - Replace should survive
	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key1", b"key2").unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// The Replace should survive (it's the latest non-tombstone)
	assert_eq!(results.len(), 1, "Replace should survive after flush");
	assert_eq!(results[0].1, b"replace_v3");
}

// ============================================================================
// Test: Versions survive compaction (L0 -> L1)
// ============================================================================

#[test(tokio::test)]
async fn test_versions_survive_compaction() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key1 with multiple versions
	for i in 1..=5 {
		let mut tx = store.begin().unwrap();
		let value = format!("v{i}");
		tx.set_at(b"key1", value.as_bytes(), i as u64 * 100).unwrap();
		tx.commit().await.unwrap();
	}

	// First flush (memtable -> L0)
	store.flush().unwrap();

	// Verify versions after first flush
	{
		let tx = store.begin().unwrap();
		let mut iter = tx.history(b"key1", b"key2").unwrap();
		let results = collect_history_all(&mut iter).unwrap();
		assert_eq!(results.len(), 5, "All 5 versions should survive L0 flush");
	}

	// Insert more versions to trigger another flush
	for i in 6..=10 {
		let mut tx = store.begin().unwrap();
		let value = format!("v{i}");
		tx.set_at(b"key1", value.as_bytes(), i as u64 * 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Second flush
	store.flush().unwrap();

	// Verify all 10 versions exist across L0 files
	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key1", b"key2").unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	assert_eq!(results.len(), 10, "All 10 versions should survive multiple flushes");
	// Verify order (newest first)
	assert_eq!(results[0].1, b"v10");
	assert_eq!(results[9].1, b"v1");
}

// ============================================================================
// Test: history() with LSM-based iteration (no B+tree index)
// ============================================================================

#[test(tokio::test)]
async fn test_history_lsm_multiple_versions() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key1 three times with different values
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1_v1", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1_v2", 200).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1_v3", 300).unwrap();
		tx.commit().await.unwrap();
	}

	// Query using history() API
	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key1", b"key2").unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// Should have all 3 versions, ordered by seq_num descending (newest first)
	assert_eq!(results.len(), 3, "Should have 3 versions");
	assert_eq!(results[0].0, b"key1");
	assert_eq!(results[0].1, b"value1_v3");
	assert_eq!(results[0].2, 300); // timestamp
	assert_eq!(results[1].0, b"key1");
	assert_eq!(results[1].1, b"value1_v2");
	assert_eq!(results[1].2, 200);
	assert_eq!(results[2].0, b"key1");
	assert_eq!(results[2].1, b"value1_v1");
	assert_eq!(results[2].2, 100);
}

// ============================================================================
// Test: history() with B+tree index enabled
// ============================================================================

#[test(tokio::test)]
async fn test_history_btree_multiple_versions() {
	let (store, _temp_dir) = create_versioned_store_with_index();

	// Insert key1 three times with different values
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1_v1", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1_v2", 200).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"value1_v3", 300).unwrap();
		tx.commit().await.unwrap();
	}

	// Query using history() API - should use B+tree streaming
	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key1", b"key2").unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// Should have all 3 versions
	assert_eq!(results.len(), 3, "Should have 3 versions via B+tree");
	assert_eq!(results[0].0, b"key1");
	assert_eq!(results[1].0, b"key1");
	assert_eq!(results[2].0, b"key1");
}

// ============================================================================
// Test: history() forward and backward iteration
// ============================================================================

#[test(tokio::test)]
async fn test_history_bidirectional_iteration() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert multiple keys with multiple versions
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"key1_v1", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"key1_v2", 200).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key2", b"key2_v1", 150).unwrap();
		tx.commit().await.unwrap();
	}

	let tx = store.begin().unwrap();
	let mut iter = tx.history(b"key0", b"key9").unwrap();

	// Test forward iteration
	let forward_results = collect_history_all(&mut iter).unwrap();
	assert_eq!(forward_results.len(), 3);

	// Test backward iteration
	let reverse_results = collect_history_reverse(&mut iter).unwrap();
	assert_eq!(reverse_results.len(), 3);

	// Forward and reverse should have opposite order
	assert_eq!(forward_results[0].0, reverse_results[2].0);
}

// ============================================================================
// Test: ts_range filtering
// ============================================================================

#[test(tokio::test)]
async fn test_history_ts_range_forward() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key1 with multiple versions at different timestamps
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v100", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v200", 200).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v300", 300).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v400", 400).unwrap();
		tx.commit().await.unwrap();
	}

	// Query with ts_range [150, 350] - should only return v200 and v300
	let tx = store.begin().unwrap();
	let opts = HistoryOptions::new().with_ts_range(150, 350);
	let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	assert_eq!(results.len(), 2, "Should have 2 versions in range [150, 350]");
	assert_eq!(results[0].2, 300); // timestamp
	assert_eq!(results[1].2, 200);
}

#[test(tokio::test)]
async fn test_history_ts_range_backward() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert key1 with multiple versions at different timestamps
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v100", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v200", 200).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v300", 300).unwrap();
		tx.commit().await.unwrap();
	}

	// Query with ts_range [150, 250] - should only return v200
	let tx = store.begin().unwrap();
	let opts = HistoryOptions::new().with_ts_range(150, 250);
	let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
	let results = collect_history_reverse(&mut iter).unwrap();

	assert_eq!(results.len(), 1, "Should have 1 version in range [150, 250]");
	assert_eq!(results[0].2, 200);
}

#[test(tokio::test)]
async fn test_history_ts_range_btree() {
	let (store, _temp_dir) = create_versioned_store_with_index();

	// Insert key1 with multiple versions at different timestamps
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v100", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v200", 200).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v300", 300).unwrap();
		tx.commit().await.unwrap();
	}

	// Query with ts_range [100, 200] - should return v100 and v200
	let tx = store.begin().unwrap();
	let opts = HistoryOptions::new().with_ts_range(100, 200);
	let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	assert_eq!(results.len(), 2, "Should have 2 versions in range [100, 200]");
	assert_eq!(results[0].2, 200);
	assert_eq!(results[1].2, 100);
}

// ============================================================================
// Test: limit filtering
// ============================================================================

#[test(tokio::test)]
async fn test_history_limit_forward() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert multiple versions
	for i in 1..=5 {
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", format!("v{}", i).as_bytes(), i * 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Query with limit of 3
	let tx = store.begin().unwrap();
	let opts = HistoryOptions::new().with_limit(3);
	let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	assert_eq!(results.len(), 3, "Should have exactly 3 entries due to limit");
	// Should be the 3 newest versions (highest timestamps first)
	assert_eq!(results[0].2, 500);
	assert_eq!(results[1].2, 400);
	assert_eq!(results[2].2, 300);
}

#[test(tokio::test)]
async fn test_history_limit_backward() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert multiple versions
	for i in 1..=5 {
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", format!("v{}", i).as_bytes(), i * 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Query with limit of 2 in reverse
	let tx = store.begin().unwrap();
	let opts = HistoryOptions::new().with_limit(2);
	let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
	let results = collect_history_reverse(&mut iter).unwrap();

	assert_eq!(results.len(), 2, "Should have exactly 2 entries due to limit");
	// Backward iteration starts from oldest, so should get v100 and v200
	assert_eq!(results[0].2, 100);
	assert_eq!(results[1].2, 200);
}

#[test(tokio::test)]
async fn test_history_limit_multiple_keys() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert versions for multiple keys
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v1", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v2", 200).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key2", b"v1", 150).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key2", b"v2", 250).unwrap();
		tx.commit().await.unwrap();
	}

	// Query with limit of 3 - should span across keys
	let tx = store.begin().unwrap();
	let opts = HistoryOptions::new().with_limit(3);
	let mut iter = tx.history_with_options(b"key0", b"key9", &opts).unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	assert_eq!(results.len(), 3, "Should have exactly 3 entries across keys");
}

// ============================================================================
// Test: ts_range + limit combination
// ============================================================================

#[test(tokio::test)]
async fn test_history_ts_range_with_limit() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Insert many versions
	for i in 1..=10 {
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", format!("v{}", i).as_bytes(), i * 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Query with ts_range [300, 800] and limit 3
	// Range includes v300, v400, v500, v600, v700, v800 (6 entries)
	// But limit is 3, so only first 3 should be returned
	let tx = store.begin().unwrap();
	let opts = HistoryOptions::new().with_ts_range(300, 800).with_limit(3);
	let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	assert_eq!(results.len(), 3, "Should have 3 entries (limited)");
	// Newest first within range
	assert_eq!(results[0].2, 800);
	assert_eq!(results[1].2, 700);
	assert_eq!(results[2].2, 600);
}

#[test(tokio::test)]
async fn test_history_limit_zero() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v1", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Query with limit of 0 - should return nothing
	let tx = store.begin().unwrap();
	let opts = HistoryOptions::new().with_limit(0);
	let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	assert_eq!(results.len(), 0, "Should have 0 entries with limit 0");
}

#[test(tokio::test)]
async fn test_history_ts_range_empty_result() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v100", 100).unwrap();
		tx.commit().await.unwrap();
	}
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v200", 200).unwrap();
		tx.commit().await.unwrap();
	}

	// Query with ts_range that doesn't match any entries
	let tx = store.begin().unwrap();
	let opts = HistoryOptions::new().with_ts_range(500, 600);
	let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	assert_eq!(results.len(), 0, "Should have 0 entries outside ts_range");
}

// ==================== RYOW (Read Your Own Writes) Tests ====================

/// Test get_at RYOW: uncommitted writes should be visible
#[test(tokio::test)]
async fn test_get_at_ryow() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Commit some initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"committed_v1", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Start a new transaction and write uncommitted data
	let mut tx = store.begin().unwrap();
	tx.set_at(b"key1", b"uncommitted_v2", 200).unwrap();
	tx.set_at(b"key2", b"uncommitted_new", 150).unwrap();

	// RYOW: Should see uncommitted write at matching timestamp
	let value = tx.get_at(b"key1", 200).unwrap();
	assert_eq!(value, Some(b"uncommitted_v2".to_vec()), "Should see uncommitted write at ts=200");

	// RYOW: Should see uncommitted write for new key
	let value = tx.get_at(b"key2", 150).unwrap();
	assert_eq!(value, Some(b"uncommitted_new".to_vec()), "Should see uncommitted new key");

	// Should see committed data at older timestamp
	let value = tx.get_at(b"key1", 100).unwrap();
	assert_eq!(value, Some(b"committed_v1".to_vec()), "Should see committed data at ts=100");
}

/// Test get_at RYOW: future timestamp writes should not be visible
#[test(tokio::test)]
async fn test_get_at_ryow_future_timestamp() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Start a transaction and write with future timestamp
	let mut tx = store.begin().unwrap();
	tx.set_at(b"key1", b"future_value", 500).unwrap();

	// Query at earlier timestamp should NOT see the future write
	let value = tx.get_at(b"key1", 400).unwrap();
	assert_eq!(value, None, "Should not see future write at ts=400 < write ts=500");

	// Query at same or later timestamp should see it
	let value = tx.get_at(b"key1", 500).unwrap();
	assert_eq!(value, Some(b"future_value".to_vec()), "Should see write at ts=500");

	let value = tx.get_at(b"key1", 600).unwrap();
	assert_eq!(value, Some(b"future_value".to_vec()), "Should see write at ts=600 > write ts=500");
}

/// Test get_at RYOW with tombstone
#[test(tokio::test)]
async fn test_get_at_ryow_tombstone() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Commit initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"initial", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Delete the key in a new transaction (uncommitted)
	let mut tx = store.begin().unwrap();
	tx.delete(b"key1").unwrap();

	// The delete timestamp should be after 100, so get_at at delete time should return None
	// Since delete uses current time, query at a very high timestamp to ensure we see the delete
	let value = tx.get_at(b"key1", u64::MAX).unwrap();
	assert_eq!(value, None, "Should see tombstone as None");
}

/// Test history iterator RYOW: uncommitted writes appear in history
#[test(tokio::test)]
async fn test_history_ryow() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Commit some initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"committed_v1", 100).unwrap();
		tx.set_at(b"key1", b"committed_v2", 200).unwrap();
		tx.commit().await.unwrap();
	}

	// Start a new transaction and write uncommitted data
	let mut tx = store.begin().unwrap();
	tx.set_at(b"key1", b"uncommitted_v3", 300).unwrap();
	tx.set_at(b"key2", b"uncommitted_new", 150).unwrap();

	// Get history - should include uncommitted writes
	let opts = HistoryOptions::new();
	let mut iter = tx.history_with_options(b"key1", b"key3", &opts).unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// Should have 4 entries: key1@300 (uncommitted), key1@200, key1@100, key2@150 (uncommitted)
	// Ordered by (key ASC, timestamp DESC)
	assert_eq!(results.len(), 4, "Should have 4 history entries");

	// key1@300 (uncommitted)
	assert_eq!(results[0].0, b"key1".to_vec());
	assert_eq!(results[0].1, b"uncommitted_v3".to_vec());
	assert_eq!(results[0].2, 300);

	// key1@200 (committed)
	assert_eq!(results[1].0, b"key1".to_vec());
	assert_eq!(results[1].1, b"committed_v2".to_vec());
	assert_eq!(results[1].2, 200);

	// key1@100 (committed)
	assert_eq!(results[2].0, b"key1".to_vec());
	assert_eq!(results[2].1, b"committed_v1".to_vec());
	assert_eq!(results[2].2, 100);

	// key2@150 (uncommitted)
	assert_eq!(results[3].0, b"key2".to_vec());
	assert_eq!(results[3].1, b"uncommitted_new".to_vec());
	assert_eq!(results[3].2, 150);
}

/// Test history RYOW with timestamp collision: write set wins
#[test(tokio::test)]
async fn test_history_ryow_timestamp_collision() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Commit some initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"committed_at_100", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Write uncommitted data with the SAME timestamp
	let mut tx = store.begin().unwrap();
	tx.set_at(b"key1", b"uncommitted_at_100", 100).unwrap();

	// Get history - uncommitted write should shadow committed one at same timestamp
	let opts = HistoryOptions::new();
	let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// Should have only 1 entry - the uncommitted one shadows the committed one
	assert_eq!(results.len(), 1, "Should have 1 entry (uncommitted shadows committed at same ts)");
	assert_eq!(results[0].0, b"key1".to_vec());
	assert_eq!(results[0].1, b"uncommitted_at_100".to_vec());
	assert_eq!(results[0].2, 100);
}

/// Test history RYOW with timestamp range filtering
#[test(tokio::test)]
async fn test_history_ryow_with_ts_range() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Commit initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"committed_100", 100).unwrap();
		tx.set_at(b"key1", b"committed_200", 200).unwrap();
		tx.set_at(b"key1", b"committed_300", 300).unwrap();
		tx.commit().await.unwrap();
	}

	// Write uncommitted data at various timestamps
	let mut tx = store.begin().unwrap();
	tx.set_at(b"key1", b"uncommitted_150", 150).unwrap();

	// Query with ts_range [100, 200] - should include uncommitted at 150
	let opts = HistoryOptions::new().with_ts_range(100, 200);
	let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// Should have: key1@200, key1@150 (uncommitted), key1@100
	assert_eq!(results.len(), 3, "Should have 3 entries in range [100, 200]");
	assert_eq!(results[0].2, 200);
	assert_eq!(results[1].2, 150);
	assert_eq!(results[1].1, b"uncommitted_150".to_vec());
	assert_eq!(results[2].2, 100);
}

/// Test history RYOW soft delete (tombstone) handling
#[test(tokio::test)]
async fn test_history_ryow_soft_delete() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Commit initial data at ts=100
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v1", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Soft delete key in new transaction (uncommitted)
	let mut tx = store.begin().unwrap();
	let write_opts = crate::transaction::WriteOptions {
		timestamp: Some(200),
	};
	tx.soft_delete_with_options(b"key1", &write_opts).unwrap();

	// Query without tombstones - should not see soft delete entry from write set
	let opts = HistoryOptions::new();
	let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// Should only see the committed entry (write set soft delete is filtered out)
	assert_eq!(results.len(), 1, "Should have 1 entry (soft delete filtered)");
	assert_eq!(results[0].1, b"v1".to_vec());
	assert_eq!(results[0].2, 100);

	// Query with tombstones included - should see soft delete entry from write set
	let opts = HistoryOptions::new().with_tombstones(true);
	let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// Should see both: the soft delete @200 (uncommitted) and the original value @100
	assert_eq!(results.len(), 2, "Should have 2 entries with tombstones");
	// The soft delete should be first (higher timestamp)
	assert_eq!(results[0].2, 200, "First entry should be ts=200");
	assert!(results[0].3, "First entry should be tombstone (is_tombstone=true)");
	// The original value should be second
	assert_eq!(results[1].2, 100, "Second entry should be ts=100");
	assert!(!results[1].3, "Second entry should not be tombstone");
}

/// Test history RYOW hard delete handling - wipes all history
#[test(tokio::test)]
async fn test_history_ryow_hard_delete() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Commit initial data at ts=100
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v1", 100).unwrap();
		tx.set_at(b"key2", b"v2", 100).unwrap();
		tx.commit().await.unwrap();
	}

	// Hard delete key1 in new transaction (uncommitted)
	let mut tx = store.begin().unwrap();
	tx.delete(b"key1").unwrap();

	// Query history - hard delete should wipe all history for key1
	let opts = HistoryOptions::new();
	let mut iter = tx.history_with_options(b"key1", b"key3", &opts).unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	// Should only see key2, key1's history is wiped by hard delete
	assert_eq!(results.len(), 1, "Should have 1 entry (key1 wiped by hard delete)");
	assert_eq!(results[0].0, b"key2".to_vec());
	assert_eq!(results[0].1, b"v2".to_vec());

	// Even with tombstones=true, hard delete entry itself should not appear
	// and all history for that key should be gone
	let opts = HistoryOptions::new().with_tombstones(true);
	let mut iter = tx.history_with_options(b"key1", b"key3", &opts).unwrap();
	let results = collect_history_all(&mut iter).unwrap();

	assert_eq!(results.len(), 1, "Hard delete wipes history even with tombstones=true");
	assert_eq!(results[0].0, b"key2".to_vec());
}

/// Test get_at RYOW with hard delete - returns None regardless of timestamp
#[test(tokio::test)]
async fn test_get_at_ryow_hard_delete() {
	let (store, _temp_dir) = create_versioned_store_no_index();

	// Commit initial data
	{
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"v1", 100).unwrap();
		tx.set_at(b"key1", b"v2", 200).unwrap();
		tx.commit().await.unwrap();
	}

	// Hard delete the key
	let mut tx = store.begin().unwrap();
	tx.delete(b"key1").unwrap();

	// get_at should return None for any timestamp - hard delete wipes all
	let value = tx.get_at(b"key1", 100).unwrap();
	assert_eq!(value, None, "Hard delete should wipe ts=100");

	let value = tx.get_at(b"key1", 200).unwrap();
	assert_eq!(value, None, "Hard delete should wipe ts=200");

	let value = tx.get_at(b"key1", u64::MAX).unwrap();
	assert_eq!(value, None, "Hard delete should wipe all timestamps");
}
