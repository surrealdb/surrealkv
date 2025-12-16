//! B+ Tree Migration Tests
//!
//! This module contains integration tests that read data from an LSM tree
//! (with vlog enabled) and migrate it to a B+ tree, verifying data integrity.

use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use test_log::test;

use crate::bplustree::tree::new_disk_tree;
use crate::lsm::TreeBuilder;
use crate::BytewiseComparator;

/// Test that reads all keys/values from the crud-bench LSM tree (with vlog enabled)
/// and inserts them into a new empty B+ tree, then verifies all data matches.
#[test(tokio::test)]
async fn test_lsm_to_bplustree_migration() {
    // Path to the existing crud-bench LSM tree data
    let crud_bench_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("crud-bench");

    // Skip test if crud-bench directory doesn't exist
    if !crud_bench_path.exists() {
        eprintln!(
            "Skipping test: crud-bench directory not found at {:?}",
            crud_bench_path
        );
        return;
    }

    // Step 1: Open the existing LSM tree with vlog enabled
    let lsm_tree = TreeBuilder::new()
        .with_path(crud_bench_path)
        .with_enable_vlog(true)
        .build()
        .expect("Failed to open LSM tree from crud-bench");

    // Step 2: Read all key-value pairs from the LSM tree
    let txn = lsm_tree.begin().expect("Failed to begin transaction");

    // Use range to scan all keys from empty to max
    let start_key: &[u8] = &[];
    let end_key: &[u8] = &[0xFF, 0xFF, 0xFF, 0xFF];

    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    let range_iter = txn
        .range(start_key, end_key)
        .expect("Failed to create range iterator");

    for result in range_iter {
        match result {
            Ok((key, value)) => {
                entries.push((key.to_vec(), value.to_vec()));
            }
            Err(e) => {
                panic!("Error reading from LSM tree: {:?}", e);
            }
        }
    }

    // Ensure we have data to work with
    assert!(
        !entries.is_empty(),
        "No entries found in crud-bench LSM tree. Expected data to migrate."
    );

    println!(
        "Read {} entries from LSM tree, migrating to B+ tree...",
        entries.len()
    );

    // Step 3: Create a new empty B+ tree in a temporary location
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let bptree_path = temp_dir.path().join("bplustree.db");

    let comparator = Arc::new(BytewiseComparator {});
    let mut bptree =
        new_disk_tree(&bptree_path, comparator).expect("Failed to create B+ tree");

    // Step 4: Insert all entries from LSM tree into B+ tree
    for (key, value) in &entries {
        bptree
            .insert(key, value)
            .expect("Failed to insert into B+ tree");
    }

    println!("Inserted {} entries into B+ tree", entries.len());

    // Step 5: Verify by reading back all values from B+ tree and comparing
    let mut verified_count = 0;
    let mut mismatches: Vec<String> = Vec::new();

    for (key, expected_value) in &entries {
        match bptree.get(key) {
            Ok(Some(actual_value)) => {
                if actual_value.as_ref() != expected_value.as_slice() {
                    mismatches.push(format!(
                        "Value mismatch for key {:?}: expected {:?}, got {:?}",
                        key, expected_value, actual_value
                    ));
                } else {
                    verified_count += 1;
                }
            }
            Ok(None) => {
                mismatches.push(format!("Key {:?} not found in B+ tree", key));
            }
            Err(e) => {
                mismatches.push(format!("Error reading key {:?} from B+ tree: {:?}", key, e));
            }
        }
    }

    // Report results
    if !mismatches.is_empty() {
        for mismatch in &mismatches[..std::cmp::min(10, mismatches.len())] {
            eprintln!("{}", mismatch);
        }
        if mismatches.len() > 10 {
            eprintln!("... and {} more mismatches", mismatches.len() - 10);
        }
        panic!(
            "Migration verification failed: {} mismatches out of {} entries",
            mismatches.len(),
            entries.len()
        );
    }

    assert_eq!(
        verified_count,
        entries.len(),
        "Not all entries were verified"
    );

    println!(
        "Successfully verified all {} entries in B+ tree match LSM tree",
        verified_count
    );
}

/// Test that verifies B+ tree range scan matches LSM tree data
#[test(tokio::test)]
async fn test_lsm_to_bplustree_range_verification() {
    // Path to the existing crud-bench LSM tree data
    let crud_bench_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("crud-bench");

    // Skip test if crud-bench directory doesn't exist
    if !crud_bench_path.exists() {
        eprintln!(
            "Skipping test: crud-bench directory not found at {:?}",
            crud_bench_path
        );
        return;
    }

    // Open the existing LSM tree with vlog enabled
    let lsm_tree = TreeBuilder::new()
        .with_path(crud_bench_path.clone())
        .with_enable_vlog(true)
        .build()
        .expect("Failed to open LSM tree from crud-bench");

    // Read all key-value pairs from the LSM tree using FULL range (empty to max)
    let txn = lsm_tree.begin().expect("Failed to begin transaction");
    // Use empty start key and max end key to capture ALL keys
    let beg: &[u8] = &[];
    let end: &[u8] = &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];

    let mut lsm_entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    for result in txn.range(beg, end).expect("Failed to create range iterator") {
        match result {
            Ok((key, value)) => {
                lsm_entries.push((key.to_vec(), value.to_vec()));
            }
            Err(e) => {
                panic!("Error reading from LSM tree: {:?}", e);
            }
        }
    }

    if lsm_entries.is_empty() {
        eprintln!("Skipping test: No entries in crud-bench LSM tree");
        return;
    }

    println!("LSM entries: {:?}", lsm_entries.len());

    // Create B+ tree and insert all entries
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let bptree_path = temp_dir.path().join("bplustree_range.db");

    let comparator = Arc::new(BytewiseComparator {});
    let mut bptree = new_disk_tree(&bptree_path, comparator).expect("Failed to create B+ tree");

    for (key, value) in &lsm_entries {
        bptree.insert(key, value).expect("Failed to insert into B+ tree");
    }

    // Verify using B+ tree range scan
    let bptree_range = bptree
        .range(beg, end)
        .expect("Failed to create B+ tree range iterator");

    let bptree_entries: Vec<(Vec<u8>, Vec<u8>)> = bptree_range
        .map(|result| {
            let (k, v) = result.expect("Failed to read from B+ tree range");
            (k.to_vec(), v.to_vec())
        })
        .collect();

    // Verify counts match
    assert_eq!(
        bptree_entries.len(),
        lsm_entries.len(),
        "Entry count mismatch: B+ tree has {}, LSM tree has {}",
        bptree_entries.len(),
        lsm_entries.len()
    );

    // Verify all entries match (both are sorted by key, so we can compare directly)
    for (i, ((lsm_key, lsm_value), (bpt_key, bpt_value))) in
        lsm_entries.iter().zip(bptree_entries.iter()).enumerate()
    {
        assert_eq!(
            lsm_key, bpt_key,
            "Key mismatch at index {}: LSM {:?} vs B+ tree {:?}",
            i, lsm_key, bpt_key
        );
        assert_eq!(
            lsm_value, bpt_value,
            "Value mismatch at index {} for key {:?}",
            i, lsm_key
        );
    }

    println!(
        "Range verification passed: {} entries match between LSM and B+ tree",
        lsm_entries.len()
    );
}

