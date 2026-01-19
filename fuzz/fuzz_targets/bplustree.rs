#![no_main]
use std::collections::HashMap;
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::bplustree::new_disk_tree;
use surrealkv::comparator::BytewiseComparator;
use tempfile::NamedTempFile;

/// Operation types for the B+ tree
#[derive(Arbitrary, Debug, Clone)]
enum Operation {
    Insert { key_idx: u8, value_size: u16 },
    Get { key_idx: u8 },
    Delete { key_idx: u8 },
}

/// Key size category for testing different storage paths
#[derive(Arbitrary, Debug, Clone)]
enum KeySizeCategory {
    Tiny,      // 1-32 bytes
    Small,     // 33-256 bytes  
    Medium,    // 257-1024 bytes (near overflow threshold)
    Large,     // 1025-4096 bytes (definitely overflows)
    Huge,      // 4097-16384 bytes (multiple overflow pages)
}

/// Fuzz input structure
#[derive(Arbitrary, Debug)]
struct FuzzInput {
    /// Seed data for generating keys
    key_seeds: Vec<(KeySizeCategory, Vec<u8>)>,
    /// Base value data (will be repeated for large values)
    value_seed: Vec<u8>,
    /// Sequence of operations to perform
    operations: Vec<Operation>,
}

/// Generate a key of the specified size category using seed data
fn make_key(category: &KeySizeCategory, seed: &[u8]) -> Vec<u8> {
    let target_size = match category {
        KeySizeCategory::Tiny => 1 + (seed.first().copied().unwrap_or(0) as usize % 32),
        KeySizeCategory::Small => 33 + (seed.first().copied().unwrap_or(0) as usize % 224),
        KeySizeCategory::Medium => 257 + (seed.first().copied().unwrap_or(0) as usize % 768),
        KeySizeCategory::Large => 1025 + (seed.first().copied().unwrap_or(0) as usize % 3072),
        KeySizeCategory::Huge => 4097 + (seed.first().copied().unwrap_or(0) as usize % 12288),
    };
    
    make_data(seed, target_size)
}

/// Generate data of the specified size using seed
fn make_data(seed: &[u8], size: usize) -> Vec<u8> {
    if size == 0 {
        return vec![];
    }
    if seed.is_empty() {
        return vec![0u8; size];
    }
    let mut data = Vec::with_capacity(size);
    while data.len() < size {
        let remaining = size - data.len();
        let chunk_size = remaining.min(seed.len());
        data.extend_from_slice(&seed[..chunk_size]);
    }
    data
}

fuzz_target!(|data: FuzzInput| {
    // Limit input sizes to prevent OOM
    if data.key_seeds.len() > 50 || data.operations.len() > 200 {
        return;
    }

    // Generate keys from seeds with varying sizes
    let keys: Vec<_> = data.key_seeds.iter()
        .map(|(cat, seed)| make_key(cat, seed))
        .filter(|k| !k.is_empty())
        .take(30)
        .collect();

    if keys.is_empty() {
        return;
    }

    // Create a temporary file for the tree
    let temp_file = match NamedTempFile::new() {
        Ok(f) => f,
        Err(_) => return,
    };

    let comparator = Arc::new(BytewiseComparator::default());
    let mut tree = match new_disk_tree(temp_file.path(), comparator) {
        Ok(t) => t,
        Err(_) => return,
    };

    // Track expected state
    let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

    // Execute operations
    for op in &data.operations {
        match op {
            Operation::Insert { key_idx, value_size } => {
                let key_idx = (*key_idx as usize) % keys.len();
                let key = &keys[key_idx];

                // Value size: 0 to 64KB (tests both inline and overflow storage)
                let size = (*value_size as usize) % 65536;
                let value = make_data(&data.value_seed, size);

                match tree.insert(key, &value) {
                    Ok(()) => {
                        expected.insert(key.clone(), value);
                    }
                    Err(e) => {
                        panic!("Insert failed for key len={}: {:?}", key.len(), e);
                    }
                }
            }

            Operation::Get { key_idx } => {
                let key_idx = (*key_idx as usize) % keys.len();
                let key = &keys[key_idx];

                match tree.get(key) {
                    Ok(result) => {
                        match (result, expected.get(key)) {
                            (Some(got), Some(exp)) => {
                                assert_eq!(
                                    got.as_ref(), exp.as_slice(),
                                    "Get returned wrong value for key len={}", key.len()
                                );
                            }
                            (None, None) => {}
                            (Some(_), None) => {
                                panic!("Get found key (len={}) that shouldn't exist", key.len());
                            }
                            (None, Some(_)) => {
                                panic!("Get failed to find key (len={}) that should exist", key.len());
                            }
                        }
                    }
                    Err(e) => {
                        panic!("Get failed for key len={}: {:?}", key.len(), e);
                    }
                }
            }

            Operation::Delete { key_idx } => {
                let key_idx = (*key_idx as usize) % keys.len();
                let key = &keys[key_idx];

                match tree.delete(key) {
                    Ok(result) => {
                        match (result.is_some(), expected.remove(key).is_some()) {
                            (true, true) | (false, false) => {}
                            (true, false) => {
                                panic!("Delete found key (len={}) that shouldn't exist", key.len());
                            }
                            (false, true) => {
                                panic!("Delete failed to find key (len={}) that should exist", key.len());
                            }
                        }
                    }
                    Err(e) => {
                        panic!("Delete failed for key len={}: {:?}", key.len(), e);
                    }
                }
            }
        }
    }

    // Final verification
    for (key, expected_value) in &expected {
        match tree.get(key) {
            Ok(Some(got)) => {
                assert_eq!(got.as_ref(), expected_value.as_slice());
            }
            Ok(None) => {
                panic!("Final check: key (len={}) missing", key.len());
            }
            Err(e) => {
                panic!("Final check: error for key (len={}): {:?}", key.len(), e);
            }
        }
    }
});