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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
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

		if with_index {
			store.flush().unwrap();
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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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
		for i in 0..100 {
			let mut tx = store.begin().unwrap();
			let value = format!("value_{i:03}");
			tx.set_at(b"key1", value.as_bytes(), i as u64 * 10).unwrap();
			tx.commit().await.unwrap();
		}

		if with_index {
			store.flush().unwrap();
		}

		{
			// Query all versions
			let tx = store.begin().unwrap();
			let mut iter = tx.history(b"key1", b"key2").unwrap();
			let results = collect_history_all(&mut iter).unwrap();

			// #region agent log
			{
				use std::io::Write;
				let log_path = "/Users/kfarhan/workspace/surrealdb/surrealkv/.cursor/debug.log";
				if let Ok(mut file) =
					std::fs::OpenOptions::new().create(true).append(true).open(log_path)
				{
					let first_val_bytes = results.first().map(|r| r.1.clone()).unwrap_or_default();
					let first_val = String::from_utf8_lossy(&first_val_bytes);
					let first_ts = results.first().map(|r| r.2).unwrap_or(0);
					let second_val_bytes = results.get(1).map(|r| r.1.clone()).unwrap_or_default();
					let second_val = String::from_utf8_lossy(&second_val_bytes);
					let second_ts = results.get(1).map(|r| r.2).unwrap_or(0);
					let last_val_bytes = results
						.get(results.len().saturating_sub(1))
						.map(|r| r.1.clone())
						.unwrap_or_default();
					let last_val = String::from_utf8_lossy(&last_val_bytes);
					let last_ts =
						results.get(results.len().saturating_sub(1)).map(|r| r.2).unwrap_or(0);
					let _ = writeln!(
						file,
						r#"{{"hypothesisId":"A,B,C","location":"test_history_many_versions","message":"history results","data":{{"with_index":{},"results_len":{},"first_val":"{}","first_ts":{},"second_val":"{}","second_ts":{},"last_val":"{}","last_ts":{}}},"timestamp":{}}}"#,
						with_index,
						results.len(),
						first_val,
						first_ts,
						second_val,
						second_ts,
						last_val,
						last_ts,
						std::time::SystemTime::now()
							.duration_since(std::time::UNIX_EPOCH)
							.unwrap()
							.as_millis()
					);
				}
			}
			// #endregion

			assert_eq!(results.len(), 100, "with_index={with_index}: Should have all 100 versions");
			assert_eq!(results[0].1, b"value_099");
			assert_eq!(results[99].1, b"value_000");
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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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

		if with_index {
			store.flush().unwrap();
		}

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
