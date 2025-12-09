//! Test helper utilities for LSM recovery integration tests
//!
//! These helpers simplify common patterns in recovery testing, similar to
//! RocksDB's RecoveryTestHelper.

use std::fs;
use std::path::Path;

use crate::Tree;

pub struct RecoveryTestHelper;

impl RecoveryTestHelper {
	/// Fill data across multiple WAL segments by triggering memtable rotations
	///
	/// Returns the total number of keys written
	pub async fn fill_multiple_wal_segments(
		tree: &Tree,
		wal_count: usize,
		keys_per_wal: usize,
	) -> usize {
		let mut total_keys = 0;

		for wal_idx in 0..wal_count {
			for key_idx in 0..keys_per_wal {
				let key = format!("key_{:05}", total_keys);
				let value = format!("value_wal{}_key{}", wal_idx, key_idx);

				let mut txn = tree.begin().unwrap();
				txn.set(key.as_bytes(), value.as_bytes()).unwrap();
				txn.commit().await.unwrap();

				total_keys += 1;
			}

			// Trigger flush to rotate to next WAL (except for last iteration)
			if wal_idx < wal_count - 1 {
				tree.flush().unwrap();
			}
		}

		total_keys
	}

	/// Count WAL files in directory
	pub fn count_wal_files(wal_dir: &Path) -> usize {
		if !wal_dir.exists() {
			return 0;
		}

		fs::read_dir(wal_dir)
			.ok()
			.map(|entries| {
				entries
					.filter_map(|e| e.ok())
					.filter(|e| {
						e.path()
							.extension()
							.and_then(|s| s.to_str())
							.map(|ext| ext == "wal")
							.unwrap_or(false)
					})
					.count()
			})
			.unwrap_or(0)
	}

	/// Count SST files in directory
	pub fn count_sst_files(sst_dir: &Path) -> usize {
		if !sst_dir.exists() {
			return 0;
		}

		fs::read_dir(sst_dir)
			.ok()
			.map(|entries| {
				entries
					.filter_map(|e| e.ok())
					.filter(|e| {
						e.path()
							.extension()
							.and_then(|s| s.to_str())
							.map(|ext| ext == "sst")
							.unwrap_or(false)
					})
					.count()
			})
			.unwrap_or(0)
	}

	/// Verify a key exists with expected value
	pub async fn verify_key(tree: &Tree, key: &str, expected_value: &str) {
		let txn = tree.begin().unwrap();
		let result = txn.get(key.as_bytes()).unwrap();
		assert!(result.is_some(), "Key '{}' should exist but was not found", key);
		assert_eq!(
			result.unwrap().as_ref(),
			expected_value.as_bytes(),
			"Key '{}' has wrong value",
			key
		);
	}

	/// Verify key count in database
	pub async fn verify_key_count(tree: &Tree, expected: usize) {
		let txn = tree.begin().unwrap();
		let mut count = 0;

		// Count all keys - use keys() method with full range
		let start: Vec<u8> = Vec::new();
		let end: Vec<u8> = vec![0xFF; 32]; // Large upper bound
		if let Ok(mut keys_iter) = txn.keys(start, end) {
			while keys_iter.next().is_some() {
				count += 1;
			}
		}

		assert_eq!(count, expected, "Expected {} keys, found {}", expected, count);
	}

	/// Get manifest log number (using internal access for testing)
	pub fn get_manifest_log_number(tree: &Tree) -> u64 {
		// Access through the deref'd inner Core fields
		tree.core.level_manifest.read().unwrap().get_log_number()
	}

	/// Get manifest last sequence (using internal access for testing)
	pub fn get_manifest_last_sequence(tree: &Tree) -> u64 {
		tree.core.level_manifest.read().unwrap().get_last_sequence()
	}

	/// Get current in-memory sequence number
	pub fn get_current_sequence(tree: &Tree) -> u64 {
		tree.core.seq_num()
	}
}
