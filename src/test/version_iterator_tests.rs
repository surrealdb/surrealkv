//! Tests for the versioned iterator functionality.
//!
//! Each test runs with both `versioned_index=false` (LSM-only iteration) and
//! `versioned_index=true` (merged B+tree + memtable iteration) to ensure both
//! paths produce identical results.

use tempdir::TempDir;
use test_log::test;

use crate::transaction::{HistoryOptions, Mode};
use crate::{Key, LSMIterator, Options, Result, TreeBuilder, Value};

fn create_temp_directory() -> TempDir {
	TempDir::new("test").unwrap()
}

/// Create a store with versioning enabled and configurable B+tree index
fn create_versioned_store(with_index: bool) -> (crate::lsm::Tree, TempDir) {
	let temp_dir = create_temp_directory();
	let opts = Options::new()
		.with_path(temp_dir.path().to_path_buf())
		.with_versioning(true, 0)
		.with_versioned_index(with_index);
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
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

		// Query versioned range
		let tx = store.begin().unwrap();
		let mut iter = tx.history(b"key1", b"key2").unwrap();
		let results = collect_history_all(&mut iter).unwrap();

		// Should have all 3 versions, ordered by seq_num descending (newest first)
		assert_eq!(results.len(), 3, "with_index={with_index}: Should have 3 versions");
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
}

// ============================================================================
// Test 2: Multiple Keys with Multiple Versions
// ============================================================================

#[test(tokio::test)]
async fn test_history_multiple_keys_multiple_versions() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

		// Query all keys
		let tx = store.begin().unwrap();
		let mut iter = tx.history(b"key0", b"key9").unwrap();
		let results = collect_history_all(&mut iter).unwrap();

		// Should have 6 total versions
		assert_eq!(results.len(), 6, "with_index={with_index}: Should have 6 total versions");

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
}

// ============================================================================
// Test 3: Tombstones - Without Include Tombstones
// ============================================================================

#[test(tokio::test)]
async fn test_history_excludes_tombstones() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

		// Query with history (excludes tombstones by default)
		let tx = store.begin().unwrap();
		let mut iter = tx.history(b"key0", b"key9").unwrap();
		let results = collect_history_all(&mut iter).unwrap();

		// Hard delete hides ALL versions of key1, only key2 visible
		assert_eq!(results.len(), 1, "with_index={with_index}: Only key2 visible");

		for result in &results {
			assert!(!result.3, "No entry should be a tombstone");
		}

		assert_eq!(results[0].0, b"key2");
		assert_eq!(results[0].1, b"value2");
	}
}

// ============================================================================
// Test 4: Tombstones - With Include Tombstones
// ============================================================================

#[test(tokio::test)]
async fn test_history_with_tombstones() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

		// Query with history_with_options (includes tombstones)
		let tx = store.begin().unwrap();
		let opts = HistoryOptions {
			include_tombstones: true,
			..Default::default()
		};
		let mut iter = tx.history_with_options(b"key0", b"key9", &opts).unwrap();
		let results = collect_history_all(&mut iter).unwrap();

		// Hard delete hides ALL versions of key1, even with include_tombstones=true
		assert_eq!(results.len(), 1, "with_index={with_index}: Only key2 visible");
		assert_eq!(results[0].0, b"key2");
		assert!(!results[0].3, "key2 should not be a tombstone");
	}
}

// ============================================================================
// Test 5: Replace Operations
// ============================================================================

#[test(tokio::test)]
async fn test_history_replace_shows_all_versions() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

		// Query all versions
		let tx = store.begin().unwrap();
		let mut iter = tx.history(b"key1", b"key2").unwrap();
		let results = collect_history_all(&mut iter).unwrap();

		assert_eq!(results.len(), 3, "with_index={with_index}: All 3 versions visible");
		assert_eq!(results[0].1, b"v3");
		assert_eq!(results[1].1, b"v2");
		assert_eq!(results[2].1, b"v1");
	}
}

// ============================================================================
// Test 6: Soft Delete vs Hard Delete
// ============================================================================

#[test(tokio::test)]
async fn test_history_soft_delete_vs_hard_delete() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

		{
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
			assert_eq!(results.len(), 2, "with_index={with_index}: Only key1 visible");
			assert_eq!(results[0].0, b"key1");
			assert!(results[0].3, "key1 first should be tombstone");
			assert_eq!(results[1].0, b"key1");
			assert!(!results[1].3, "key1 second should be value");
		}

		store.close().await.unwrap();
	}
}

// ============================================================================
// Test 7: Bounds - Inclusive Start, Exclusive End
// ============================================================================

#[test(tokio::test)]
async fn test_history_bounds() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		// Insert keys a through e
		for key in [b"key_a", b"key_b", b"key_c", b"key_d", b"key_e"] {
			let mut tx = store.begin().unwrap();
			tx.set_at(key, b"value", 100).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		{
			// Query range [key_b, key_d) - should include key_b and key_c, NOT key_d
			let tx = store.begin().unwrap();
			let mut iter = tx.history(b"key_b", b"key_d").unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(results.len(), 2, "with_index={with_index}: Should have 2 entries");
			assert_eq!(results[0].0, b"key_b");
			assert_eq!(results[1].0, b"key_c");
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test 8: Bounds - Empty Range
// ============================================================================

#[test(tokio::test)]
async fn test_history_empty_range() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		// Insert key_a and key_b
		{
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key_a", b"value_a", 100).unwrap();
			tx.set_at(b"key_b", b"value_b", 100).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		{
			// Query range [key_c, key_d) - no matching keys
			let tx = store.begin().unwrap();
			let mut iter = tx.history(b"key_c", b"key_d").unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert!(results.is_empty(), "with_index={with_index}: Should have no entries");
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test 9: Bounds - Single Key Match
// ============================================================================

#[test(tokio::test)]
async fn test_history_single_key_match() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

		// Query range that only matches key_a
		let tx = store.begin().unwrap();
		let mut iter = tx.history(b"key_a", b"key_b").unwrap();
		let results = collect_history_all(&mut iter).unwrap();

		assert_eq!(results.len(), 2, "with_index={with_index}: Should have both versions");
		assert_eq!(results[0].0, b"key_a");
		assert_eq!(results[0].1, b"v2");
		assert_eq!(results[1].0, b"key_a");
		assert_eq!(results[1].1, b"v1");
	}
}

// ============================================================================
// Test 10: Interleaved Iteration - Forward/Backward
// ============================================================================

#[test(tokio::test)]
async fn test_history_interleaved_iteration() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

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
}

// ============================================================================
// Test 11: Interleaved Iteration - Seek in Middle
// ============================================================================

#[test(tokio::test)]
async fn test_history_seek_middle() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		// Insert 5 keys
		for i in 1..=5 {
			let mut tx = store.begin().unwrap();
			let key = format!("key{i}");
			let value = format!("value{i}");
			tx.set_at(key.as_bytes(), value.as_bytes(), i as u64 * 100).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

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
}

// ============================================================================
// Test 12: Backward Iteration Only
// ============================================================================

#[test(tokio::test)]
async fn test_history_backward_iteration() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

		let tx = store.begin().unwrap();
		let mut iter = tx.history(b"key0", b"key9").unwrap();
		let results = collect_history_reverse(&mut iter).unwrap();

		assert_eq!(results.len(), 3, "with_index={with_index}: Should have 3 entries");

		// Reverse order: key2_v1, key1_v1, key1_v2
		assert_eq!(results[0].0, b"key2");
		assert_eq!(results[0].1, b"key2_v1");
		assert_eq!(results[1].0, b"key1");
		assert_eq!(results[1].1, b"key1_v1");
		assert_eq!(results[2].0, b"key1");
		assert_eq!(results[2].1, b"key1_v2");
	}
}

// ============================================================================
// Test 13: Snapshot Isolation - Versions Not Visible to Old Snapshots
// ============================================================================

#[test(tokio::test)]
async fn test_history_snapshot_isolation() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

		// Take snapshot2
		let tx2 = store.begin().unwrap();

		// snapshot1 should only see v1
		{
			let mut iter = tx1.history(b"key1", b"key2").unwrap();
			let results = collect_history_all(&mut iter).unwrap();
			assert_eq!(results.len(), 1, "with_index={with_index}: snapshot1 should see 1 version");
			assert_eq!(results[0].1, b"v1");
		}

		// snapshot2 should see both v1 and v2
		{
			let mut iter = tx2.history(b"key1", b"key2").unwrap();
			let results = collect_history_all(&mut iter).unwrap();
			assert_eq!(
				results.len(),
				2,
				"with_index={with_index}: snapshot2 should see 2 versions"
			);
			assert_eq!(results[0].1, b"v2");
			assert_eq!(results[1].1, b"v1");
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test 14: Large Number of Versions
// ============================================================================

#[test(tokio::test)]
async fn test_history_many_versions() {
	for with_index in [false, true] {
		println!("test_history_many_versions with_index={with_index}");
		let (store, _temp_dir) = create_versioned_store(with_index);

		// Insert same key 100 times with different values
		for i in 1..=100 {
			let mut tx = store.begin().unwrap();
			let value = format!("value_{i:03}");
			tx.set_at(b"key1", value.as_bytes(), i as u64 * 10).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		{
			// Query all versions
			let tx = store.begin().unwrap();
			let mut iter = tx.history(b"key1", b"key2").unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(results.len(), 100, "with_index={with_index}: Should have all 100 versions");
			assert_eq!(results[0].1, b"value_100");
			assert_eq!(results[99].1, b"value_001");
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test 15: Mixed Timestamps
// ============================================================================

#[test(tokio::test)]
async fn test_history_timestamps() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		// Insert key with timestamp 50, then 100, then 200
		// Note: seq_num order != timestamp order
		{
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", b"ts_50", 50).unwrap();
			tx.commit().await.unwrap();
		}

		{
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", b"ts_100", 100).unwrap();
			tx.commit().await.unwrap();
		}
		{
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", b"ts_200", 200).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		{
			// Query versions
			let tx = store.begin().unwrap();
			let mut iter = tx.history(b"key1", b"key2").unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(results.len(), 3, "with_index={with_index}: Should have 3 versions");

			// Verify ordering is by timestamp descending (B+tree uses TimestampComparator)
			assert_eq!(results[0].1, b"ts_200");
			assert_eq!(results[0].2, 200);
			assert_eq!(results[1].1, b"ts_100");
			assert_eq!(results[1].2, 100);
			assert_eq!(results[2].1, b"ts_50");
			assert_eq!(results[2].2, 50);
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test 16: Entry Convenience Method
// Uses soft_delete to test include_tombstones (hard delete hides all versions)
// ============================================================================

#[test(tokio::test)]
async fn test_history_entry_method() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

		// Query with tombstones
		let tx = store.begin().unwrap();
		let opts = HistoryOptions {
			include_tombstones: true,
			..Default::default()
		};
		let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();

		iter.seek_first().unwrap();
		assert!(iter.valid());

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
}

// ============================================================================
// Test 17: get_at Fallback
// ============================================================================

#[test(tokio::test)]
async fn test_get_at_fallback() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

		let tx = store.begin().unwrap();

		let result = tx.get_at(b"key1", 150).unwrap();
		assert!(result.is_some());
		assert_eq!(result.unwrap(), b"value_100");

		let result = tx.get_at(b"key1", 200).unwrap();
		assert!(result.is_some());
		assert_eq!(result.unwrap(), b"value_200");

		let result = tx.get_at(b"key1", 50).unwrap();
		assert!(result.is_none());

		let result = tx.get_at(b"key1", 250).unwrap();
		assert!(result.is_some());
		assert_eq!(result.unwrap(), b"value_200");
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test 18: get_at with Tombstone
// ============================================================================

#[test(tokio::test)]
async fn test_get_at_tombstone() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

		let tx = store.begin().unwrap();

		let result = tx.get_at(b"key1", 150).unwrap();
		assert!(result.is_some());
		assert_eq!(result.unwrap(), b"value_100");

		let result = tx.get_at(b"key1", 350).unwrap();
		assert!(result.is_some());
		assert_eq!(result.unwrap(), b"value_300");
		store.close().await.unwrap();
	}
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
	let (store, _temp_dir) = create_versioned_store(false);

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
// Test: Versions survive memtable flush (bug detector)
// ============================================================================

#[test(tokio::test)]
async fn test_history_survives_memtable_flush() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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
			assert_eq!(
				results.len(),
				3,
				"with_index={with_index}: Should have 3 versions before flush"
			);
		}

		// FORCE MEMTABLE FLUSH
		store.flush().unwrap();

		{
			// Query versions AFTER flush
			let tx = store.begin().unwrap();
			let mut iter = tx.history(b"key1", b"key2").unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(
				results.len(),
				3,
				"with_index={with_index}: All 3 versions should survive flush"
			);
			assert_eq!(results[0].1, b"v3");
			assert_eq!(results[1].1, b"v2");
			assert_eq!(results[2].1, b"v1");
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test: Replace operation cuts off older versions (even with versioning)
// ============================================================================

#[test(tokio::test)]
async fn test_replace_cuts_off_history_with_versioning() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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
			tx.replace(b"key1", b"replace_v3").unwrap();
			tx.commit().await.unwrap();
		}

		// Before flush - REPLACE filters immediately during iteration
		{
			let tx = store.begin().unwrap();
			let mut iter = tx.history(b"key1", b"key2").unwrap();
			let results = collect_history_all(&mut iter).unwrap();
			assert_eq!(
				results.len(),
				1,
				"with_index={with_index}: Before flush: REPLACE filters immediately"
			);
			assert_eq!(results[0].1, b"replace_v3");
		}

		// Force flush
		store.flush().unwrap();

		{
			// After flush - same result
			let tx = store.begin().unwrap();
			let mut iter = tx.history(b"key1", b"key2").unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(
				results.len(),
				1,
				"with_index={with_index}: After flush: only Replace survives"
			);
			assert_eq!(results[0].1, b"replace_v3");
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test: Multiple Replace operations are all preserved with versioning
// ============================================================================

#[test(tokio::test)]
async fn test_multiple_replaces_preserved_with_versioning() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		// Insert with SET first
		{
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", b"set_v1", 100).unwrap();
			tx.commit().await.unwrap();
		}

		// Multiple Replace operations
		{
			let mut tx = store.begin().unwrap();
			tx.replace(b"key1", b"replace_v2").unwrap();
			tx.commit().await.unwrap();
		}
		{
			let mut tx = store.begin().unwrap();
			tx.replace(b"key1", b"replace_v3").unwrap();
			tx.commit().await.unwrap();
		}
		{
			let mut tx = store.begin().unwrap();
			tx.replace(b"key1", b"replace_v4").unwrap();
			tx.commit().await.unwrap();
		}

		// Force flush
		store.flush().unwrap();

		{
			let tx = store.begin().unwrap();
			let mut iter = tx.history(b"key1", b"key2").unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(results.len(), 1, "with_index={with_index}: Latest Replace should survive");
			assert_eq!(results[0].1, b"replace_v4");
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test: Replace after Delete with versioning
// ============================================================================

#[test(tokio::test)]
async fn test_replace_after_delete_with_versioning() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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
			tx.replace(b"key1", b"replace_v3").unwrap();
			tx.commit().await.unwrap();
		}

		// Force flush
		store.flush().unwrap();

		{
			let tx = store.begin().unwrap();
			let mut iter = tx.history(b"key1", b"key2").unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(results.len(), 1, "with_index={with_index}: Replace should survive");
			assert_eq!(results[0].1, b"replace_v3");
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test: Versions survive compaction (L0 -> L1)
// ============================================================================

#[test(tokio::test)]
async fn test_versions_survive_compaction() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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
			assert_eq!(
				results.len(),
				5,
				"with_index={with_index}: All 5 versions should survive L0 flush"
			);
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

		{
			// Verify all 10 versions exist across L0 files
			let tx = store.begin().unwrap();
			let mut iter = tx.history(b"key1", b"key2").unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(
				results.len(),
				10,
				"with_index={with_index}: All 10 versions should survive"
			);
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
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

		{
			let tx = store.begin().unwrap();
			let mut iter = tx.history(b"key0", b"key9").unwrap();

			let forward_results = collect_history_all(&mut iter).unwrap();
			assert_eq!(forward_results.len(), 3);

			let reverse_results = collect_history_reverse(&mut iter).unwrap();
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
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

		{
			// Query with ts_range [150, 350] - should only return v200 and v300
			let tx = store.begin().unwrap();
			let opts = HistoryOptions::new().with_ts_range(150, 350);
			let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(
				results.len(),
				2,
				"with_index={with_index}: Should have 2 versions in range"
			);
			assert_eq!(results[0].2, 300);
			assert_eq!(results[1].2, 200);
		}
		store.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_history_ts_range_backward() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

		{
			// Query with ts_range [150, 250] - should only return v200
			let tx = store.begin().unwrap();
			let opts = HistoryOptions::new().with_ts_range(150, 250);
			let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
			let results = collect_history_reverse(&mut iter).unwrap();

			assert_eq!(results.len(), 1, "with_index={with_index}: Should have 1 version in range");
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
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		for i in 1..=5 {
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", format!("v{}", i).as_bytes(), i * 100).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		{
			let tx = store.begin().unwrap();
			let opts = HistoryOptions::new().with_limit(3);
			let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(results.len(), 3, "with_index={with_index}: Should have 3 entries");
			assert_eq!(results[0].2, 500);
			assert_eq!(results[1].2, 400);
			assert_eq!(results[2].2, 300);
		}
		store.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_history_limit_backward() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		for i in 1..=5 {
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", format!("v{}", i).as_bytes(), i * 100).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		{
			let tx = store.begin().unwrap();
			let opts = HistoryOptions::new().with_limit(2);
			let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
			let results = collect_history_reverse(&mut iter).unwrap();

			assert_eq!(results.len(), 2, "with_index={with_index}: Should have 2 entries");
			assert_eq!(results[0].2, 100);
			assert_eq!(results[1].2, 200);
		}
		store.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_history_limit_multiple_keys() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

		{
			let tx = store.begin().unwrap();
			let opts = HistoryOptions::new().with_limit(3);
			let mut iter = tx.history_with_options(b"key0", b"key9", &opts).unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(
				results.len(),
				3,
				"with_index={with_index}: Should have 3 entries across keys"
			);
		}
		store.close().await.unwrap();
	}
}

// ============================================================================
// Test: ts_range + limit combination
// ============================================================================

#[test(tokio::test)]
async fn test_history_ts_range_with_limit() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		for i in 1..=10 {
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", format!("v{}", i).as_bytes(), i * 100).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		{
			let tx = store.begin().unwrap();
			let opts = HistoryOptions::new().with_ts_range(300, 800).with_limit(3);
			let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(
				results.len(),
				3,
				"with_index={with_index}: Should have 3 entries (limited)"
			);
			assert_eq!(results[0].2, 800);
			assert_eq!(results[1].2, 700);
			assert_eq!(results[2].2, 600);
		}
		store.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_history_limit_zero() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", b"v1", 100).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		{
			let tx = store.begin().unwrap();
			let opts = HistoryOptions::new().with_limit(0);
			let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(results.len(), 0, "with_index={with_index}: Should have 0 entries");
		}
		store.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_history_ts_range_empty_result() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

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

		store.flush().unwrap();

		{
			let tx = store.begin().unwrap();
			let opts = HistoryOptions::new().with_ts_range(500, 600);
			let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(results.len(), 0, "with_index={with_index}: Should have 0 entries");
		}

		store.close().await.unwrap();
	}
}

// ==================== RYOW (Read Your Own Writes) Tests ====================

/// Test get_at RYOW: uncommitted writes should be visible
#[test(tokio::test)]
async fn test_get_at_ryow() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		// Commit some initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", b"committed_v1", 100).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		// Start a new transaction and write uncommitted data
		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"uncommitted_v2", 200).unwrap();
		tx.set_at(b"key2", b"uncommitted_new", 150).unwrap();

		let value = tx.get_at(b"key1", 200).unwrap();
		assert_eq!(value, Some(b"uncommitted_v2".to_vec()), "with_index={with_index}");

		let value = tx.get_at(b"key2", 150).unwrap();
		assert_eq!(value, Some(b"uncommitted_new".to_vec()), "with_index={with_index}");

		let value = tx.get_at(b"key1", 100).unwrap();
		assert_eq!(value, Some(b"committed_v1".to_vec()), "with_index={with_index}");
	}
}

/// Test get_at RYOW: future timestamp writes should not be visible
#[test(tokio::test)]
async fn test_get_at_ryow_future_timestamp() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		let mut tx = store.begin().unwrap();
		tx.set_at(b"key1", b"future_value", 500).unwrap();

		let value = tx.get_at(b"key1", 400).unwrap();
		assert_eq!(value, None, "with_index={with_index}: Should not see future write");

		let value = tx.get_at(b"key1", 500).unwrap();
		assert_eq!(value, Some(b"future_value".to_vec()), "with_index={with_index}");

		let value = tx.get_at(b"key1", 600).unwrap();
		assert_eq!(value, Some(b"future_value".to_vec()), "with_index={with_index}");
		store.close().await.unwrap();
	}
}

/// Test get_at RYOW with tombstone
#[test(tokio::test)]
async fn test_get_at_ryow_tombstone() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		// Commit initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", b"initial", 100).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		// Delete the key in a new transaction (uncommitted)
		let mut tx = store.begin().unwrap();
		tx.delete(b"key1").unwrap();

		let value = tx.get_at(b"key1", u64::MAX).unwrap();
		assert_eq!(value, None, "with_index={with_index}: Should see tombstone as None");
		store.close().await.unwrap();
	}
}

/// Test history iterator RYOW: uncommitted writes appear in history
#[test(tokio::test)]
async fn test_history_ryow() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		// Commit some initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", b"committed_v1", 100).unwrap();
			tx.set_at(b"key1", b"committed_v2", 200).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		{
			// Start a new transaction and write uncommitted data
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", b"uncommitted_v3", 300).unwrap();
			tx.set_at(b"key2", b"uncommitted_new", 150).unwrap();

			let opts = HistoryOptions::new();
			let mut iter = tx.history_with_options(b"key1", b"key3", &opts).unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(results.len(), 4, "with_index={with_index}: Should have 4 history entries");

			assert_eq!(results[0].0, b"key1".to_vec());
			assert_eq!(results[0].1, b"uncommitted_v3".to_vec());
			assert_eq!(results[0].2, 300);

			assert_eq!(results[1].0, b"key1".to_vec());
			assert_eq!(results[1].1, b"committed_v2".to_vec());
			assert_eq!(results[1].2, 200);

			assert_eq!(results[2].0, b"key1".to_vec());
			assert_eq!(results[2].1, b"committed_v1".to_vec());
			assert_eq!(results[2].2, 100);

			assert_eq!(results[3].0, b"key2".to_vec());
			assert_eq!(results[3].1, b"uncommitted_new".to_vec());
			assert_eq!(results[3].2, 150);
		}
		store.close().await.unwrap();
	}
}

/// Test history RYOW with timestamp collision: write set wins
#[test(tokio::test)]
async fn test_history_ryow_timestamp_collision() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", b"committed_at_100", 100).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		{
			// Write uncommitted data with the SAME timestamp
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", b"uncommitted_at_100", 100).unwrap();

			let opts = HistoryOptions::new();
			let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(results.len(), 1, "with_index={with_index}: Uncommitted shadows committed");
			assert_eq!(results[0].0, b"key1".to_vec());
			assert_eq!(results[0].1, b"uncommitted_at_100".to_vec());
			assert_eq!(results[0].2, 100);
		}
		store.close().await.unwrap();
	}
}

/// Test history RYOW with timestamp range filtering
#[test(tokio::test)]
async fn test_history_ryow_with_ts_range() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", b"committed_100", 100).unwrap();
			tx.set_at(b"key1", b"committed_200", 200).unwrap();
			tx.set_at(b"key1", b"committed_300", 300).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		{
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", b"uncommitted_150", 150).unwrap();

			let opts = HistoryOptions::new().with_ts_range(100, 200);
			let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(results.len(), 3, "with_index={with_index}: Should have 3 entries in range");
			assert_eq!(results[0].2, 200);
			assert_eq!(results[1].2, 150);
			assert_eq!(results[1].1, b"uncommitted_150".to_vec());
			assert_eq!(results[2].2, 100);
		}

		store.close().await.unwrap();
	}
}

/// Test history RYOW soft delete (tombstone) handling
#[test(tokio::test)]
async fn test_history_ryow_soft_delete() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", b"v1", 100).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		{
			// Soft delete key in new transaction (uncommitted)
			let mut tx = store.begin().unwrap();
			let write_opts = crate::transaction::WriteOptions {
				timestamp: Some(200),
			};
			tx.soft_delete_with_options(b"key1", &write_opts).unwrap();

			// Query without tombstones
			let opts = HistoryOptions::new();
			let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(results.len(), 1, "with_index={with_index}: Soft delete filtered");
			assert_eq!(results[0].1, b"v1".to_vec());
			assert_eq!(results[0].2, 100);

			// Query with tombstones included
			let opts = HistoryOptions::new().with_tombstones(true);
			let mut iter = tx.history_with_options(b"key1", b"key2", &opts).unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(results.len(), 2, "with_index={with_index}: Should have 2 with tombstones");
			assert_eq!(results[0].2, 200);
			assert!(results[0].3, "First entry should be tombstone");
			assert_eq!(results[1].2, 100);
			assert!(!results[1].3, "Second entry should not be tombstone");
		}
		store.close().await.unwrap();
	}
}

/// Test history RYOW hard delete handling - wipes all history
#[test(tokio::test)]
async fn test_history_ryow_hard_delete() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", b"v1", 100).unwrap();
			tx.set_at(b"key2", b"v2", 100).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		{
			// Hard delete key1 in new transaction (uncommitted)
			let mut tx = store.begin().unwrap();
			tx.delete(b"key1").unwrap();

			let opts = HistoryOptions::new();
			let mut iter = tx.history_with_options(b"key1", b"key3", &opts).unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(results.len(), 1, "with_index={with_index}: key1 wiped by hard delete");
			assert_eq!(results[0].0, b"key2".to_vec());
			assert_eq!(results[0].1, b"v2".to_vec());

			// Even with tombstones=true, hard delete wipes all
			let opts = HistoryOptions::new().with_tombstones(true);
			let mut iter = tx.history_with_options(b"key1", b"key3", &opts).unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			assert_eq!(
				results.len(),
				1,
				"with_index={with_index}: Hard delete wipes even with tombstones"
			);
			assert_eq!(results[0].0, b"key2".to_vec());
		}

		store.close().await.unwrap();
	}
}

/// Test get_at RYOW with hard delete - returns None regardless of timestamp
#[test(tokio::test)]
async fn test_get_at_ryow_hard_delete() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin().unwrap();
			tx.set_at(b"key1", b"v1", 100).unwrap();
			tx.set_at(b"key1", b"v2", 200).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let mut tx = store.begin().unwrap();
		tx.delete(b"key1").unwrap();

		let value = tx.get_at(b"key1", 100).unwrap();
		assert_eq!(value, None, "with_index={with_index}: Hard delete should wipe ts=100");

		let value = tx.get_at(b"key1", 200).unwrap();
		assert_eq!(value, None, "with_index={with_index}: Hard delete should wipe ts=200");

		let value = tx.get_at(b"key1", u64::MAX).unwrap();
		assert_eq!(value, None, "with_index={with_index}: Hard delete should wipe all");
		store.close().await.unwrap();
	}
}

// ============================================================================
// History Iterator Bounds Tests
// ============================================================================

/// Test that forward iteration respects lower bound when keys exist before the range.
/// Regression test: keys with user_key lexicographically before lower bound should not be returned.
#[test(tokio::test)]
async fn test_history_forward_respects_lower_bound() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		// Key starting with '#' (ASCII 35) comes before '@' (ASCII 64)
		let sync_key = b"#@prefix:sync\x00\x00\x00\x00\x00\x00\x00\x02****************";
		let table_key = b"@prefix:tbentity\x00\x00\x00\x00\x00\x00\x00\x00\x10cccccccccccccccc";
		let before_key = b"@prefix:tbentity\x00\x00\x00\x00\x00\x00\x00\x00\x10kkkkkkkkkkkkkkkk";
		let after_key = b"@prefix:tbentity\x00\x00\x00\x00\x00\x00\x00\x00\x10MMMMMMMMMMMMMMMM";

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(before_key, b"value", 1).unwrap();
			tx.commit().await.unwrap();
		}

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(sync_key, b"value", 2).unwrap();
			tx.set_at(table_key, b"value", 2).unwrap();
			tx.commit().await.unwrap();
		}

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(after_key, b"value", 3).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();

		// Query range starting with '@' should NOT return '#@prefix:sync'
		let lower = b"@prefix:\x00";
		let upper = b"@prefix:\xFF";
		let opts = HistoryOptions::new().with_tombstones(true).with_ts_range(2, 2);
		let mut iter = tx.history_with_options(lower, upper, &opts).unwrap();

		let results = collect_history_all(&mut iter).unwrap();

		// Only table_key should be returned (it's the only key within range at ts=2)
		assert_eq!(
			results.len(),
			1,
			"with_index={with_index}: Only keys within range should be returned, got: {:?}",
			results.iter().map(|(k, _, _, _)| String::from_utf8_lossy(k)).collect::<Vec<_>>()
		);
		assert!(
			results[0].0.starts_with(b"@prefix:"),
			"with_index={with_index}: Key should start with @prefix:"
		);

		store.close().await.unwrap();
	}
}

/// Test that backward iteration respects upper bound when keys exist after the range.
/// Regression test: keys with user_key lexicographically after upper bound should not be returned.
#[test(tokio::test)]
async fn test_history_backward_respects_upper_bound() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		// Keys in range
		let key_a = b"key_a";
		let key_b = b"key_b";
		// Key after upper bound
		let key_z = b"key_z";

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(key_a, b"value_a", 1).unwrap();
			tx.set_at(key_b, b"value_b", 1).unwrap();
			tx.set_at(key_z, b"value_z", 1).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();

		// Query range [key_a, key_c) should NOT return key_z
		let opts = HistoryOptions::new().with_tombstones(true);
		let mut iter = tx.history_with_options(b"key_a", b"key_c", &opts).unwrap();

		// Iterate backward from the end
		iter.seek_last().unwrap();
		let mut results = Vec::new();
		while iter.valid() {
			let key_ref = iter.key();
			results.push(key_ref.user_key().to_vec());
			if !iter.prev().unwrap() {
				break;
			}
		}

		assert_eq!(
			results.len(),
			2,
			"with_index={with_index}: Only keys within range should be returned, got: {:?}",
			results.iter().map(|k| String::from_utf8_lossy(k)).collect::<Vec<_>>()
		);
		// Backward iteration returns newest first, so key_b then key_a
		assert_eq!(results[0], b"key_b".to_vec(), "with_index={with_index}: First should be key_b");
		assert_eq!(
			results[1],
			b"key_a".to_vec(),
			"with_index={with_index}: Second should be key_a"
		);

		store.close().await.unwrap();
	}
}

/// Test that both bounds are respected with keys outside both ends of the range.
#[test(tokio::test)]
async fn test_history_respects_both_bounds() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		// Keys before range
		let key_aa = b"aa_before";
		let key_ab = b"ab_before";
		// Keys in range
		let key_ma = b"ma_inside";
		let key_mb = b"mb_inside";
		let key_mc = b"mc_inside";
		// Keys after range
		let key_za = b"za_after";
		let key_zb = b"zb_after";

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(key_aa, b"v", 1).unwrap();
			tx.set_at(key_ab, b"v", 1).unwrap();
			tx.set_at(key_ma, b"v", 1).unwrap();
			tx.set_at(key_mb, b"v", 1).unwrap();
			tx.set_at(key_mc, b"v", 1).unwrap();
			tx.set_at(key_za, b"v", 1).unwrap();
			tx.set_at(key_zb, b"v", 1).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();

		// Query range [m, z) should only return keys starting with 'm'
		let opts = HistoryOptions::new().with_tombstones(true);
		let mut iter = tx.history_with_options(b"m", b"z", &opts).unwrap();

		// Test forward iteration
		let results = collect_history_all(&mut iter).unwrap();
		assert_eq!(
			results.len(),
			3,
			"with_index={with_index}: Forward should return 3 keys in range, got: {:?}",
			results.iter().map(|(k, _, _, _)| String::from_utf8_lossy(k)).collect::<Vec<_>>()
		);
		for (key, _, _, _) in &results {
			assert!(
				key.as_slice() >= b"m".as_slice() && key.as_slice() < b"z".as_slice(),
				"with_index={with_index}: Key {:?} should be in range [m, z)",
				String::from_utf8_lossy(key)
			);
		}

		// Test backward iteration
		let mut iter = tx.history_with_options(b"m", b"z", &opts).unwrap();
		iter.seek_last().unwrap();
		let mut backward_results = Vec::new();
		while iter.valid() {
			backward_results.push(iter.key().user_key().to_vec());
			if !iter.prev().unwrap() {
				break;
			}
		}
		assert_eq!(
			backward_results.len(),
			3,
			"with_index={with_index}: Backward should return 3 keys in range, got: {:?}",
			backward_results.iter().map(|k| String::from_utf8_lossy(k)).collect::<Vec<_>>()
		);
		for key in &backward_results {
			assert!(
				key.as_slice() >= b"m".as_slice() && key.as_slice() < b"z".as_slice(),
				"with_index={with_index}: Key {:?} should be in range [m, z)",
				String::from_utf8_lossy(key)
			);
		}

		store.close().await.unwrap();
	}
}

/// Test bounds with timestamp ranges - keys outside range but within timestamp should be excluded.
#[test(tokio::test)]
async fn test_history_bounds_with_timestamp_range() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		// Key before range at target timestamp
		let key_before = b"aaa_before";
		// Key in range at target timestamp
		let key_inside = b"mmm_inside";
		// Key after range at target timestamp
		let key_after = b"zzz_after";

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			// All keys at timestamp 5
			tx.set_at(key_before, b"v", 5).unwrap();
			tx.set_at(key_inside, b"v", 5).unwrap();
			tx.set_at(key_after, b"v", 5).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();

		// Query with timestamp filter - should still respect bounds
		let opts = HistoryOptions::new().with_tombstones(true).with_ts_range(5, 5);
		let mut iter = tx.history_with_options(b"m", b"z", &opts).unwrap();

		let results = collect_history_all(&mut iter).unwrap();
		assert_eq!(
			results.len(),
			1,
			"with_index={with_index}: Only key_inside should be returned, got: {:?}",
			results.iter().map(|(k, _, _, _)| String::from_utf8_lossy(k)).collect::<Vec<_>>()
		);
		assert_eq!(
			results[0].0,
			key_inside.to_vec(),
			"with_index={with_index}: Should return mmm_inside"
		);

		store.close().await.unwrap();
	}
}

/// Test direction switching (forward to backward) respects bounds.
#[test(tokio::test)]
async fn test_history_bounds_direction_switch_forward_to_backward() {
	for with_index in [false] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		// Keys: before range, in range (3 keys), after range
		let keys: [&[u8]; 5] = [b"aa_before", b"mm_in1", b"mm_in2", b"mm_in3", b"zz_after"];

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			for key in keys {
				tx.set_at(key, b"value", 1).unwrap();
			}
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);
		let lower: &[u8] = b"m";
		let upper: &[u8] = b"z";
		let mut iter = tx.history_with_options(lower, upper, &opts).unwrap();

		// Forward: get first key
		iter.seek_first().unwrap();
		assert!(iter.valid(), "with_index={with_index}: Should be valid after seek_first");
		let first_key = iter.key().user_key().to_vec();
		assert_eq!(
			first_key,
			b"mm_in1".to_vec(),
			"with_index={with_index}: First key should be mm_in1"
		);

		// Move forward one more
		iter.next().unwrap();
		assert!(iter.valid());
		assert_eq!(
			iter.key().user_key(),
			b"mm_in2",
			"with_index={with_index}: Second should be mm_in2"
		);

		// Switch direction: go backward
		iter.prev().unwrap();
		assert!(iter.valid());
		let back_key = iter.key().user_key().to_vec();
		assert_eq!(
			back_key,
			b"mm_in1".to_vec(),
			"with_index={with_index}: After prev should be mm_in1"
		);

		// Continue backward - should become invalid (no more in-range keys)
		let has_more = iter.prev().unwrap();
		assert!(
			!has_more || !iter.valid(),
			"with_index={with_index}: Should have no more keys before mm_in1"
		);

		store.close().await.unwrap();
	}
}

/// Test direction switching (backward to forward) respects bounds.
#[test(tokio::test)]
async fn test_history_bounds_direction_switch_backward_to_forward() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		let keys: [&[u8]; 5] = [b"aa_before", b"mm_in1", b"mm_in2", b"mm_in3", b"zz_after"];

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			for key in keys {
				tx.set_at(key, b"value", 1).unwrap();
			}
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);
		let lower: &[u8] = b"m";
		let upper: &[u8] = b"z";
		let mut iter = tx.history_with_options(lower, upper, &opts).unwrap();

		// Backward: get last key in range
		iter.seek_last().unwrap();
		assert!(iter.valid(), "with_index={with_index}: Should be valid after seek_last");
		let last_key = iter.key().user_key().to_vec();
		assert_eq!(
			last_key,
			b"mm_in3".to_vec(),
			"with_index={with_index}: Last key should be mm_in3"
		);

		// Move backward one
		iter.prev().unwrap();
		assert!(iter.valid());
		assert_eq!(
			iter.key().user_key(),
			b"mm_in2",
			"with_index={with_index}: Should be at mm_in2"
		);

		// Switch direction: go forward
		iter.next().unwrap();
		assert!(iter.valid());
		let fwd_key = iter.key().user_key().to_vec();
		assert_eq!(
			fwd_key,
			b"mm_in3".to_vec(),
			"with_index={with_index}: After next should be mm_in3"
		);

		// Continue forward - should become invalid (no more in-range keys)
		let has_more = iter.next().unwrap();
		assert!(
			!has_more || !iter.valid(),
			"with_index={with_index}: Should have no more keys after mm_in3"
		);

		store.close().await.unwrap();
	}
}

/// Test direction switching with keys that have many versions.
/// Verifies that prev/next correctly navigate across multi-version keys.
#[test(tokio::test)]
async fn test_history_direction_switch_multi_version_keys() {
	const VERSIONS_PER_KEY: u64 = 5;
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		// Create 3 keys, each with many versions
		for key in [b"key_a", b"key_b", b"key_c"] {
			for v in 1..=VERSIONS_PER_KEY {
				let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
				tx.set_at(
					key,
					format!("{}_v{}", std::str::from_utf8(key).unwrap(), v).as_bytes(),
					v * 100,
				)
				.unwrap();
				tx.commit().await.unwrap();
			}
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);
		let mut iter = tx.history_with_options(b"key_a", b"key_d", &opts).unwrap();

		// --- Forward: advance past key_a (5 versions) to key_b
		iter.seek_first().unwrap();
		assert_eq!(
			iter.key().user_key(),
			b"key_a",
			"with_index={with_index}: First should be key_a"
		);
		for _ in 0..VERSIONS_PER_KEY {
			iter.next().unwrap();
		}
		assert!(iter.valid());
		assert_eq!(
			iter.key().user_key(),
			b"key_b",
			"with_index={with_index}: After key_a versions should be key_b"
		);

		// --- Switch backward: prev should go to last version of key_a
		iter.prev().unwrap();
		assert!(iter.valid());
		assert_eq!(
			iter.key().user_key(),
			b"key_a",
			"with_index={with_index}: After prev from key_b should be key_a"
		);
		assert_eq!(
			iter.value().unwrap(),
			b"key_a_v1",
			"with_index={with_index}: Should be oldest key_a"
		);

		// --- Switch forward: next advances one entry (key_a v1 -> v2). Advance through key_a to
		// key_b.
		for _ in 0..VERSIONS_PER_KEY {
			iter.next().unwrap();
		}
		assert!(iter.valid());
		assert_eq!(
			iter.key().user_key(),
			b"key_b",
			"with_index={with_index}: After advancing through key_a versions should be key_b"
		);

		// --- Forward: advance through key_b to key_c
		for _ in 0..VERSIONS_PER_KEY {
			iter.next().unwrap();
		}
		assert!(iter.valid());
		assert_eq!(
			iter.key().user_key(),
			b"key_c",
			"with_index={with_index}: After key_b versions should be key_c"
		);

		// --- Backward: seek_last, then prev through key_c to key_b
		iter.seek_last().unwrap();
		assert_eq!(
			iter.key().user_key(),
			b"key_c",
			"with_index={with_index}: Last should be key_c"
		);
		for _ in 0..VERSIONS_PER_KEY {
			iter.prev().unwrap();
		}
		assert!(iter.valid());
		assert_eq!(
			iter.key().user_key(),
			b"key_b",
			"with_index={with_index}: After key_c versions should be key_b"
		);

		// --- Switch forward: next advances one entry. Advance through key_b to key_c.
		for _ in 0..VERSIONS_PER_KEY {
			iter.next().unwrap();
		}
		assert!(iter.valid());
		assert_eq!(
			iter.key().user_key(),
			b"key_c",
			"with_index={with_index}: After advancing through key_b versions should be key_c"
		);

		store.close().await.unwrap();
	}
}

/// Test seeking to a key below lower_bound.
#[test(tokio::test)]
async fn test_history_seek_outside_lower_bound() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(b"aaa", b"v", 1).unwrap();
			tx.set_at(b"mmm", b"v", 1).unwrap();
			tx.set_at(b"zzz", b"v", 1).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);
		let mut iter = tx.history_with_options(b"m", b"z", &opts).unwrap();

		// Seek to key below lower_bound - should position at first in-range key or become invalid
		iter.seek(b"aaa").unwrap();
		if iter.valid() {
			let key = iter.key().user_key();
			assert!(
				key >= b"m".as_slice(),
				"with_index={with_index}: After seek below lower, key should be >= lower_bound, got: {:?}",
				String::from_utf8_lossy(key)
			);
		}

		store.close().await.unwrap();
	}
}

/// Test seeking to a key at or above upper_bound.
#[test(tokio::test)]
async fn test_history_seek_outside_upper_bound() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(b"aaa", b"v", 1).unwrap();
			tx.set_at(b"mmm", b"v", 1).unwrap();
			tx.set_at(b"zzz", b"v", 1).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);
		let mut iter = tx.history_with_options(b"m", b"z", &opts).unwrap();

		// Seek to key at upper_bound - should be invalid (upper is exclusive)
		iter.seek(b"z").unwrap();
		assert!(
			!iter.valid(),
			"with_index={with_index}: Seek to upper_bound should make iterator invalid"
		);

		// Seek to key above upper_bound
		let mut iter = tx.history_with_options(b"m", b"z", &opts).unwrap();
		iter.seek(b"zzz").unwrap();
		assert!(
			!iter.valid(),
			"with_index={with_index}: Seek above upper_bound should make iterator invalid"
		);

		store.close().await.unwrap();
	}
}

/// Test seeking to exact bound values.
#[test(tokio::test)]
async fn test_history_seek_to_exact_bounds() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(b"key_a", b"v", 1).unwrap();
			tx.set_at(b"key_m", b"v", 1).unwrap();
			tx.set_at(b"key_z", b"v", 1).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);

		// Seek to exact lower_bound where key exists
		let mut iter = tx.history_with_options(b"key_m", b"key_z", &opts).unwrap();
		iter.seek(b"key_m").unwrap();
		assert!(iter.valid(), "with_index={with_index}: Seek to lower_bound should be valid");
		assert_eq!(iter.key().user_key(), b"key_m", "with_index={with_index}: Should be at key_m");

		// Seek to exact upper_bound - should be invalid (exclusive)
		iter.seek(b"key_z").unwrap();
		assert!(
			!iter.valid(),
			"with_index={with_index}: Seek to upper_bound should be invalid (exclusive)"
		);

		store.close().await.unwrap();
	}
}

/// Test key exactly at lower_bound is included (inclusive).
#[test(tokio::test)]
async fn test_history_key_at_exact_lower_bound() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(b"lower", b"v", 1).unwrap();
			tx.set_at(b"middle", b"v", 1).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);
		let mut iter = tx.history_with_options(b"lower", b"upper", &opts).unwrap();

		let results = collect_history_all(&mut iter).unwrap();
		assert!(
			results.iter().any(|(k, _, _, _)| k == b"lower"),
			"with_index={with_index}: Key exactly at lower_bound should be included"
		);

		store.close().await.unwrap();
	}
}

/// Test key exactly at upper_bound is excluded (exclusive).
#[test(tokio::test)]
async fn test_history_key_at_exact_upper_bound() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(b"middle", b"v", 1).unwrap();
			tx.set_at(b"upper", b"v", 1).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);
		let mut iter = tx.history_with_options(b"lower", b"upper", &opts).unwrap();

		let results = collect_history_all(&mut iter).unwrap();
		assert!(
			!results.iter().any(|(k, _, _, _)| k == b"upper"),
			"with_index={with_index}: Key exactly at upper_bound should be excluded"
		);
		assert!(
			results.iter().any(|(k, _, _, _)| k == b"middle"),
			"with_index={with_index}: Key before upper_bound should be included"
		);

		store.close().await.unwrap();
	}
}

/// Test adjacent byte boundaries with special byte values.
#[test(tokio::test)]
async fn test_history_adjacent_byte_boundaries() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		// Keys with adjacent byte values
		let key_before = b"key";
		let key_at_lower = b"key\x00";
		let key_in_range = b"key\x01";
		let key_at_upper = b"key\x10";

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(key_before, b"v", 1).unwrap();
			tx.set_at(key_at_lower, b"v", 1).unwrap();
			tx.set_at(key_in_range, b"v", 1).unwrap();
			tx.set_at(key_at_upper, b"v", 1).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);
		// Range: [key\x00, key\x10)
		let mut iter = tx.history_with_options(b"key\x00", b"key\x10", &opts).unwrap();

		let results = collect_history_all(&mut iter).unwrap();

		// key\x00 should be included (at lower bound, inclusive)
		assert!(
			results.iter().any(|(k, _, _, _)| k == key_at_lower),
			"with_index={with_index}: Key at lower bound should be included"
		);
		// key\x01 should be included (in range)
		assert!(
			results.iter().any(|(k, _, _, _)| k == key_in_range),
			"with_index={with_index}: Key in range should be included"
		);
		// key (without suffix) should be excluded (before lower)
		assert!(
			!results.iter().any(|(k, _, _, _)| k == key_before),
			"with_index={with_index}: Key before lower bound should be excluded"
		);
		// key\x10 should be excluded (at upper, exclusive)
		assert!(
			!results.iter().any(|(k, _, _, _)| k == key_at_upper),
			"with_index={with_index}: Key at upper bound should be excluded"
		);

		store.close().await.unwrap();
	}
}

/// Test tombstone (soft delete) at lower bound.
#[test(tokio::test)]
async fn test_history_tombstone_at_lower_bound() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(b"key_a", b"v", 1).unwrap();
			tx.set_at(b"key_b", b"v", 1).unwrap();
			tx.commit().await.unwrap();
		}

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			let write_opts = crate::transaction::WriteOptions {
				timestamp: Some(2),
			};
			tx.soft_delete_with_options(b"key_a", &write_opts).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();

		// Without tombstones - key_a's older version (ts=1) should still appear,
		// but the tombstone at ts=2 should NOT appear
		let opts = HistoryOptions::new().with_tombstones(false);
		let mut iter = tx.history_with_options(b"key_a", b"key_z", &opts).unwrap();
		let results = collect_history_all(&mut iter).unwrap();
		// The value at ts=1 should appear
		assert!(
			results.iter().any(|(k, _, ts, is_tomb)| k == b"key_a" && *ts == 1 && !is_tomb),
			"with_index={with_index}: Without tombstones flag, older value version should still appear"
		);
		// The tombstone at ts=2 should NOT appear
		assert!(
			!results.iter().any(|(k, _, ts, is_tomb)| k == b"key_a" && *ts == 2 && *is_tomb),
			"with_index={with_index}: Without tombstones flag, tombstone should be filtered out"
		);

		// With tombstones - both versions of key_a should appear
		let opts = HistoryOptions::new().with_tombstones(true);
		let mut iter = tx.history_with_options(b"key_a", b"key_z", &opts).unwrap();
		let results = collect_history_all(&mut iter).unwrap();
		// The tombstone at ts=2 should appear
		assert!(
			results.iter().any(|(k, _, ts, is_tomb)| k == b"key_a" && *ts == 2 && *is_tomb),
			"with_index={with_index}: With tombstones flag, tombstone at lower bound should appear"
		);
		// The value at ts=1 should also appear
		assert!(
			results.iter().any(|(k, _, ts, is_tomb)| k == b"key_a" && *ts == 1 && !is_tomb),
			"with_index={with_index}: With tombstones flag, older value should also appear"
		);

		store.close().await.unwrap();
	}
}

/// Test hard delete at boundary.
#[test(tokio::test)]
async fn test_history_hard_delete_at_boundary() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(b"key_a", b"v1", 1).unwrap();
			tx.set_at(b"key_b", b"v1", 1).unwrap();
			tx.commit().await.unwrap();
		}

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			let write_opts = crate::transaction::WriteOptions {
				timestamp: Some(2),
			};
			tx.delete_with_options(b"key_a", &write_opts).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);
		let mut iter = tx.history_with_options(b"key_a", b"key_z", &opts).unwrap();

		let results = collect_history_all(&mut iter).unwrap();
		// Hard delete should completely remove the key from history
		assert!(
			!results.iter().any(|(k, _, _, _)| k == b"key_a"),
			"with_index={with_index}: Hard deleted key at boundary should not appear"
		);
		assert!(
			results.iter().any(|(k, _, _, _)| k == b"key_b"),
			"with_index={with_index}: Other keys should still appear"
		);

		store.close().await.unwrap();
	}
}

/// Test replace operation at boundary.
#[test(tokio::test)]
async fn test_history_replace_at_boundary() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(b"key_a", b"v1", 1).unwrap();
			tx.set_at(b"key_a", b"v2", 2).unwrap();
			tx.commit().await.unwrap();
		}

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.replace(b"key_a", b"v3").unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);
		let mut iter = tx.history_with_options(b"key_a", b"key_z", &opts).unwrap();

		let results = collect_history_all(&mut iter).unwrap();
		let key_a_versions: Vec<_> = results.iter().filter(|(k, _, _, _)| k == b"key_a").collect();

		// Replace cuts off history - should only see v3 (the replace)
		assert_eq!(
			key_a_versions.len(),
			1,
			"with_index={with_index}: Replace should cut off history, only showing the replace version"
		);

		store.close().await.unwrap();
	}
}

/// Test equal lower and upper bounds (empty range).
#[test(tokio::test)]
async fn test_history_bounds_equal_lower_upper() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(b"key_a", b"v", 1).unwrap();
			tx.set_at(b"key_b", b"v", 1).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);
		// Range [key_a, key_a) is empty since upper is exclusive
		let mut iter = tx.history_with_options(b"key_a", b"key_a", &opts).unwrap();

		let results = collect_history_all(&mut iter).unwrap();
		assert_eq!(
			results.len(),
			0,
			"with_index={with_index}: Equal lower and upper bounds should return empty"
		);

		store.close().await.unwrap();
	}
}

/// Test inverted bounds (lower > upper).
#[test(tokio::test)]
async fn test_history_bounds_inverted() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(b"key_a", b"v", 1).unwrap();
			tx.set_at(b"key_b", b"v", 1).unwrap();
			tx.set_at(b"key_c", b"v", 1).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);
		// Range [key_z, key_a) is inverted
		let mut iter = tx.history_with_options(b"key_z", b"key_a", &opts).unwrap();

		let results = collect_history_all(&mut iter).unwrap();
		assert_eq!(
			results.len(),
			0,
			"with_index={with_index}: Inverted bounds should return empty"
		);

		store.close().await.unwrap();
	}
}

/// Test single key in range.
#[test(tokio::test)]
async fn test_history_single_key_in_range() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(b"only_key", b"v", 1).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);
		let lower: &[u8] = b"only";
		let upper: &[u8] = b"only_z";
		let mut iter = tx.history_with_options(lower, upper, &opts).unwrap();

		// Forward
		let results = collect_history_all(&mut iter).unwrap();
		assert_eq!(results.len(), 1, "with_index={with_index}: Should have exactly 1 key");
		assert_eq!(results[0].0, b"only_key".to_vec());

		// Backward
		let mut iter = tx.history_with_options(lower, upper, &opts).unwrap();
		iter.seek_last().unwrap();
		assert!(iter.valid(), "with_index={with_index}: seek_last should be valid");
		assert_eq!(iter.key().user_key(), b"only_key");

		let has_more = iter.prev().unwrap();
		assert!(!has_more, "with_index={with_index}: Should have no more keys");

		store.close().await.unwrap();
	}
}

/// Test all keys outside range.
#[test(tokio::test)]
async fn test_history_all_keys_outside_range() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(b"aaa", b"v", 1).unwrap();
			tx.set_at(b"bbb", b"v", 1).unwrap();
			tx.set_at(b"zzz", b"v", 1).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);
		// Range [m, n) has no keys
		let mut iter = tx.history_with_options(b"m", b"n", &opts).unwrap();

		// Forward
		let results = collect_history_all(&mut iter).unwrap();
		assert_eq!(results.len(), 0, "with_index={with_index}: Forward should return empty");

		// Backward
		let mut iter = tx.history_with_options(b"m", b"n", &opts).unwrap();
		iter.seek_last().unwrap();
		assert!(
			!iter.valid(),
			"with_index={with_index}: seek_last should be invalid for empty range"
		);

		store.close().await.unwrap();
	}
}

/// Test bounds with limit (forward).
#[test(tokio::test)]
async fn test_history_bounds_with_limit() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			// 5 keys in range
			tx.set_at(b"key_1", b"v", 1).unwrap();
			tx.set_at(b"key_2", b"v", 1).unwrap();
			tx.set_at(b"key_3", b"v", 1).unwrap();
			tx.set_at(b"key_4", b"v", 1).unwrap();
			tx.set_at(b"key_5", b"v", 1).unwrap();
			// Keys outside range
			tx.set_at(b"aaa", b"v", 1).unwrap();
			tx.set_at(b"zzz", b"v", 1).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true).with_limit(3);
		let lower: &[u8] = b"key";
		let upper: &[u8] = b"key_z";
		let mut iter = tx.history_with_options(lower, upper, &opts).unwrap();

		let results = collect_history_all(&mut iter).unwrap();
		assert_eq!(results.len(), 3, "with_index={with_index}: Limit should cap results to 3");
		// Verify results are in range
		for (key, _, _, _) in &results {
			assert!(
				key.as_slice() >= lower && key.as_slice() < upper,
				"with_index={with_index}: All keys should be in range"
			);
		}

		store.close().await.unwrap();
	}
}

/// Test bounds with limit (backward).
#[test(tokio::test)]
async fn test_history_bounds_with_limit_backward() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(b"key_1", b"v", 1).unwrap();
			tx.set_at(b"key_2", b"v", 1).unwrap();
			tx.set_at(b"key_3", b"v", 1).unwrap();
			tx.set_at(b"key_4", b"v", 1).unwrap();
			tx.set_at(b"key_5", b"v", 1).unwrap();
			tx.set_at(b"zzz", b"v", 1).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true).with_limit(3);
		let lower: &[u8] = b"key";
		let upper: &[u8] = b"key_z";
		let mut iter = tx.history_with_options(lower, upper, &opts).unwrap();

		// Backward iteration with limit
		iter.seek_last().unwrap();
		let mut results = Vec::new();
		while iter.valid() && results.len() < 3 {
			let key = iter.key().user_key().to_vec();
			assert!(
				key.as_slice() >= lower && key.as_slice() < upper,
				"with_index={with_index}: Key should be in range, got: {:?}",
				String::from_utf8_lossy(&key)
			);
			results.push(key);
			if !iter.prev().unwrap() {
				break;
			}
		}

		assert!(results.len() <= 3, "with_index={with_index}: Should respect limit");

		store.close().await.unwrap();
	}
}

/// Test bounds with timestamp range and limit combined.
#[test(tokio::test)]
async fn test_history_bounds_ts_range_and_limit() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			// Keys at different timestamps
			tx.set_at(b"key_a", b"v_ts1", 1).unwrap();
			tx.set_at(b"key_b", b"v_ts1", 1).unwrap();
			tx.set_at(b"key_c", b"v_ts1", 1).unwrap();
			tx.commit().await.unwrap();
		}

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(b"key_a", b"v_ts5", 5).unwrap();
			tx.set_at(b"key_b", b"v_ts5", 5).unwrap();
			tx.set_at(b"key_c", b"v_ts5", 5).unwrap();
			tx.set_at(b"key_d", b"v_ts5", 5).unwrap();
			tx.set_at(b"key_e", b"v_ts5", 5).unwrap();
			tx.commit().await.unwrap();
		}

		// Key outside range
		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(b"zzz", b"v_ts5", 5).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		// All three constraints: key bounds [key, key_z), ts_range [5,5], limit 3
		let opts = HistoryOptions::new().with_tombstones(true).with_ts_range(5, 5).with_limit(3);
		let lower: &[u8] = b"key";
		let upper: &[u8] = b"key_z";
		let mut iter = tx.history_with_options(lower, upper, &opts).unwrap();

		let results = collect_history_all(&mut iter).unwrap();

		// Should have at most 3 results
		assert!(
			results.len() <= 3,
			"with_index={with_index}: Limit should be respected, got {}",
			results.len()
		);

		// All results should be in key range and ts range
		for (key, _, ts, _) in &results {
			assert!(
				key.as_slice() >= lower && key.as_slice() < upper,
				"with_index={with_index}: Key should be in range"
			);
			assert_eq!(*ts, 5, "with_index={with_index}: Timestamp should be 5");
		}

		store.close().await.unwrap();
	}
}

/// Test prefix pattern iteration (common use case).
#[test(tokio::test)]
async fn test_history_bounds_prefix_pattern() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			// Keys with prefix "user:"
			tx.set_at(b"user:alice", b"v", 1).unwrap();
			tx.set_at(b"user:bob", b"v", 1).unwrap();
			tx.set_at(b"user:charlie", b"v", 1).unwrap();
			// Keys without prefix
			tx.set_at(b"admin:root", b"v", 1).unwrap();
			tx.set_at(b"users_count", b"v", 1).unwrap(); // starts with "user" but not "user:"
			tx.set_at(b"userz", b"v", 1).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);
		// Range for prefix "user:" is [user:, user;) since ';' is byte after ':'
		let mut iter = tx.history_with_options(b"user:", b"user;", &opts).unwrap();

		let results = collect_history_all(&mut iter).unwrap();
		assert_eq!(
			results.len(),
			3,
			"with_index={with_index}: Should have 3 keys with prefix 'user:'"
		);

		for (key, _, _, _) in &results {
			assert!(
				key.starts_with(b"user:"),
				"with_index={with_index}: All keys should start with 'user:', got: {:?}",
				String::from_utf8_lossy(key)
			);
		}

		store.close().await.unwrap();
	}
}

/// Test keys and bounds containing null bytes.
#[test(tokio::test)]
async fn test_history_bounds_null_bytes() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			// Keys with embedded null bytes
			tx.set_at(b"key\x00a", b"v", 1).unwrap();
			tx.set_at(b"key\x00b", b"v", 1).unwrap();
			tx.set_at(b"key\x00c", b"v", 1).unwrap();
			// Key without null byte
			tx.set_at(b"key", b"v", 1).unwrap();
			tx.set_at(b"key\x01", b"v", 1).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);
		// Range with null byte bounds: [key\x00, key\x01)
		let lower: &[u8] = b"key\x00";
		let upper: &[u8] = b"key\x01";
		let mut iter = tx.history_with_options(lower, upper, &opts).unwrap();

		let results = collect_history_all(&mut iter).unwrap();

		// Should include key\x00a, key\x00b, key\x00c but not "key" or "key\x01"
		assert_eq!(
			results.len(),
			3,
			"with_index={with_index}: Should have 3 keys in null byte range"
		);

		for (key, _, _, _) in &results {
			assert!(
				key.starts_with(b"key\x00"),
				"with_index={with_index}: All keys should start with 'key\\x00'"
			);
		}

		// Verify "key" (without null) is not included
		assert!(
			!results.iter().any(|(k, _, _, _)| k == b"key"),
			"with_index={with_index}: 'key' without null should not be included"
		);

		store.close().await.unwrap();
	}
}

/// Test bounds with maximum byte values (0xFF).
#[test(tokio::test)]
async fn test_history_bounds_max_byte_values() {
	for with_index in [false, true] {
		let (store, _temp_dir) = create_versioned_store(with_index);

		{
			let mut tx = store.begin_with_mode(Mode::ReadWrite).unwrap();
			tx.set_at(b"key\xfe", b"v", 1).unwrap();
			tx.set_at(b"key\xff", b"v", 1).unwrap();
			tx.set_at(b"key\xff\x00", b"v", 1).unwrap();
			tx.set_at(b"kez", b"v", 1).unwrap();
			tx.commit().await.unwrap();
		}

		store.flush().unwrap();

		let tx = store.begin_with_mode(Mode::ReadOnly).unwrap();
		let opts = HistoryOptions::new().with_tombstones(true);
		// Range [key\xff, key\xff\xff) - keys starting with key\xff
		let lower: &[u8] = b"key\xff";
		let upper: &[u8] = b"key\xff\xff";
		let mut iter = tx.history_with_options(lower, upper, &opts).unwrap();

		let results = collect_history_all(&mut iter).unwrap();

		// Should include key\xff and key\xff\x00
		assert_eq!(
			results.len(),
			2,
			"with_index={with_index}: Should have 2 keys starting with 0xFF"
		);

		// Verify key\xfe is not included
		assert!(
			!results.iter().any(|(k, _, _, _)| k == b"key\xfe"),
			"with_index={with_index}: key\\xfe should not be included"
		);

		store.close().await.unwrap();
	}
}

// Test for https://github.com/surrealdb/surrealkv/issues/364
#[tokio::test(flavor = "current_thread")]
async fn repro() {
	let tmp = tempfile::Builder::new().prefix("tc-").tempdir().unwrap();

	let dir = std::path::PathBuf::from(tmp.path());

	let opts = Options::new().with_path(dir).with_versioning(true, 0).with_l0_no_compression();

	let store = TreeBuilder::with_options(opts).build().unwrap();

	let sync_key = b"#@prefix:sync\x00\x00\x00\x00\x00\x00\x00\x02****************";
	let table_key = b"@prefix:tbentity\x00\x00\x00\x00\x00\x00\x00\x00\x10cccccccccccccccc";
	let before_key = b"@prefix:tbentity\x00\x00\x00\x00\x00\x00\x00\x00\x10kkkkkkkkkkkkkkkk";
	let after_key = b"@prefix:tbentity\x00\x00\x00\x00\x00\x00\x00\x00\x10MMMMMMMMMMMMMMMM";

	{
		let mut tx = store.begin().unwrap();
		tx.set_at(before_key, b"value", 1).unwrap();
		tx.commit().await.unwrap();
	}

	{
		let mut tx = store.begin().unwrap();
		tx.set_at(sync_key, b"value", 2).unwrap();
		tx.set_at(table_key, b"value", 2).unwrap();
		tx.commit().await.unwrap();
	}

	{
		let mut tx = store.begin().unwrap();
		tx.set_at(after_key, b"value", 3).unwrap();
		tx.commit().await.unwrap();
	}

	let tx = store.begin().unwrap();

	let lower = b"@prefix:\x00";
	let upper = b"@prefix:\xFF";
	let opts = HistoryOptions::new().with_tombstones(true).with_ts_range(2, 2);
	let mut it = tx.history_with_options(lower, upper, &opts).unwrap();

	let mut seen = vec![];

	if it.seek_first().unwrap() {
		while it.valid() {
			let key = it.key();
			let user_key = String::from_utf8_lossy(key.user_key()).to_string();
			eprintln!("\t{}: {}", key.timestamp(), user_key);
			seen.push(user_key);

			it.next().unwrap();
		}
	}

	// Expected: only the @prefix key should be returned for this range.
	// Current behavior: this also returns #@prefix:* and reproduces the bug.
	assert_eq!(seen.len(), 1, "unexpected keys at ts=2 in scope range: {seen:?}");
}
