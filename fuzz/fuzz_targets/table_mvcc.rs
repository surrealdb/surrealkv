#![no_main]
use std::cmp::Ordering;
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::comparator::{BytewiseComparator, Comparator, InternalKeyComparator};
use surrealkv::sstable::table::{Table, TableWriter};
use surrealkv::sstable::{InternalKey, InternalKeyKind, INTERNAL_KEY_SEQ_NUM_MAX};
use surrealkv::Options;
use tempfile::NamedTempFile;

#[derive(Arbitrary, Debug)]
struct FuzzMultiVersionInput {
    /// Base user keys (will have multiple versions)
    base_keys: Vec<BaseKey>,
    /// Queries with specific seq_nums
    queries: Vec<VersionQuery>,
}

#[derive(Arbitrary, Debug)]
struct BaseKey {
    user_key: Vec<u8>,
    /// Multiple versions with different seq_nums
    versions: Vec<KeyVersion>,
}

#[derive(Arbitrary, Debug)]
struct KeyVersion {
    seq_num: u64,
    value: Vec<u8>,
    is_delete: bool,
}

#[derive(Arbitrary, Debug)]
struct VersionQuery {
    key_index: u8,
    query_seq_num: u64,
}

fuzz_target!(|data: FuzzMultiVersionInput| {
    // Limits
    if data.base_keys.len() > 20 || data.queries.len() > 50 {
        return;
    }

    // Filter and validate
    let base_keys: Vec<_> = data
        .base_keys
        .iter()
        .filter(|k| !k.user_key.is_empty() && k.user_key.len() <= 64 && !k.versions.is_empty())
        .collect();

    if base_keys.is_empty() {
        return;
    }

    let user_cmp = Arc::new(BytewiseComparator::default());
    let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp.clone()));

    // Build entries with multiple versions per key
    let mut entries: Vec<(InternalKey, Vec<u8>)> = Vec::new();

    for base in &base_keys {
        for version in &base.versions {
            if version.value.len() > 128 {
                continue;
            }

            let kind = if version.is_delete {
                InternalKeyKind::Delete
            } else {
                InternalKeyKind::Set
            };

            let key = InternalKey::new(
                base.user_key.clone(),
                version.seq_num.min(INTERNAL_KEY_SEQ_NUM_MAX),
                kind,
                0,
            );

            entries.push((key, version.value.clone()));
        }
    }

    if entries.is_empty() {
        return;
    }

    // Sort by internal key (user_key ASC, seq_num DESC)
    entries.sort_by(|a, b| {
        let key_a = a.0.encode();
        let key_b = b.0.encode();
        internal_cmp.compare(&key_a, &key_b)
    });

    // Deduplicate
    entries.dedup_by(|a, b| {
        let key_a = a.0.encode();
        let key_b = b.0.encode();
        internal_cmp.compare(&key_a, &key_b) == Ordering::Equal
    });

    // Build table
    let opts = Arc::new(Options::default());

    let mut temp_file = match NamedTempFile::new() {
        Ok(f) => f,
        Err(_) => return,
    };
    let file_path = temp_file.path().to_path_buf();

    let mut writer = TableWriter::new(&mut temp_file, 1, opts.clone(), 0);

    for (key, value) in &entries {
        if writer.add(key.clone(), value).is_err() {
            return;
        }
    }

    let file_size = match writer.finish() {
        Ok(size) => size,
        Err(_) => return,
    };

    // Read back
    use std::fs::File;
    use std::io::Read;
    let mut file = match File::open(&file_path) {
        Ok(f) => f,
        Err(_) => return,
    };
    let mut file_data = Vec::new();
    if file.read_to_end(&mut file_data).is_err() {
        return;
    }
    drop(temp_file);

    let file_arc: Arc<dyn surrealkv::vfs::File> = Arc::new(file_data);

    let table = match Table::new(1, opts.clone(), file_arc, file_size as u64) {
        Ok(t) => t,
        Err(_) => return,
    };

    // ========================================
    // TEST: Version visibility queries
    // ========================================
    for query in &data.queries {
        if base_keys.is_empty() {
            continue;
        }

        let key_idx = (query.key_index as usize) % base_keys.len();
        let base = &base_keys[key_idx];
        let query_seq = query.query_seq_num.min(INTERNAL_KEY_SEQ_NUM_MAX);

        let query_key = InternalKey::new(
            base.user_key.clone(),
            query_seq,
            InternalKeyKind::Set,
            0,
        );

        let result = table.get(&query_key);

        // Find expected result: highest seq_num <= query_seq for this user_key
        let expected_version = entries
            .iter()
            .filter(|(k, _)| {
                k.user_key == base.user_key && k.seq_num() <= query_seq
            })
            .max_by_key(|(k, _)| k.seq_num());

        match (result, expected_version) {
            (Ok(Some((found_key, found_value))), Some((expected_key, expected_value))) => {
                // INVARIANT: User key must match
                assert_eq!(
                    found_key.user_key.as_slice(),
                    expected_key.user_key.as_slice(),
                    "Version query returned wrong user_key"
                );

                // INVARIANT: Found seq_num should be the highest <= query_seq
                assert_eq!(
                    found_key.seq_num(),
                    expected_key.seq_num(),
                    "Version query returned wrong version. Expected seq={}, got seq={}",
                    expected_key.seq_num(),
                    found_key.seq_num()
                );

                // INVARIANT: Value should match (for non-deletes)
                if expected_key.kind() != InternalKeyKind::Delete {
                    assert_eq!(
                        &found_value, expected_value,
                        "Version query returned wrong value"
                    );
                }
            }
            (Ok(None), None) => {
                // Correct: no version visible at this seq_num
            }
            (Ok(None), Some((expected_key, _))) => {
                // get() returns deletes, so this should not happen
                panic!(
                    "Expected version with seq={} for user_key {:?} but got None",
                    expected_key.seq_num(),
                    expected_key.user_key
                );
            }
            (Ok(Some((found_key, _))), None) => {
                // No version should be visible, but we got one
                panic!(
                    "Got unexpected result with seq={} for user_key {:?} when no version expected (query_seq={})",
                    found_key.seq_num(),
                    found_key.user_key,
                    query_seq
                );
            }
            (Err(e), _) => {
                // With valid programmatically constructed data, errors indicate bugs
                panic!("Unexpected error in get(): {:?}", e);
            }
        }
    }

    // ========================================
    // TEST: Iteration returns all versions in order
    // ========================================
    let table_arc = Arc::new(table);
    let mut iter = table_arc.iter(false, None);

    let mut prev_user_key: Option<Vec<u8>> = None;
    let mut prev_seq_num: Option<u64> = None;
    let mut iter_count = 0;

    for item in &mut iter {
        let (key, _) = match item {
            Ok(kv) => kv,
            Err(_) => break,
        };

        if let Some(ref prev_uk) = prev_user_key {
            match user_cmp.compare(prev_uk, &key.user_key) {
                Ordering::Less => {
                    // New user key, reset seq tracking
                    prev_seq_num = None;
                }
                Ordering::Equal => {
                    // Same user key, seq_num must be strictly decreasing
                    if let Some(prev_seq) = prev_seq_num {
                        assert!(
                            key.seq_num() < prev_seq,
                            "Same user_key versions not in descending seq_num order"
                        );
                    }
                }
                Ordering::Greater => {
                    panic!("Iteration returned keys in wrong order (user_key decreased)");
                }
            }
        }

        prev_user_key = Some(key.user_key.clone());
        prev_seq_num = Some(key.seq_num());
        iter_count += 1;
    }

    assert_eq!(
        iter_count,
        entries.len(),
        "Iteration should return all versions"
    );
});