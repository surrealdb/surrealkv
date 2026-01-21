#![no_main]
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::comparator::{BytewiseComparator, Comparator, InternalKeyComparator};
use surrealkv::sstable::table::{Table, TableWriter};
use surrealkv::sstable::{InternalKey, InternalKeyKind, INTERNAL_KEY_SEQ_NUM_MAX};
use surrealkv::Options;
use tempfile::NamedTempFile;

#[derive(Arbitrary, Debug)]
struct FuzzGetCorrectnessInput {
    /// Keys that WILL be added to the table
    existing_keys: Vec<ExistingKey>,
    /// Keys to query (mix of existing and non-existing)
    queries: Vec<QueryKey>,
}

#[derive(Arbitrary, Debug)]
struct ExistingKey {
    user_key: Vec<u8>,
    seq_num: u64,
    value: Vec<u8>,
}

#[derive(Arbitrary, Debug)]
struct QueryKey {
    user_key: Vec<u8>,
    seq_num: u64,
}

fuzz_target!(|data: FuzzGetCorrectnessInput| {
    // Limits
    if data.existing_keys.is_empty() || data.existing_keys.len() > 50 {
        return;
    }
    if data.queries.is_empty() || data.queries.len() > 100 {
        return;
    }

    let user_cmp = Arc::new(BytewiseComparator::default());
    let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp.clone()));

    // Filter and validate existing keys
    let existing_keys: Vec<_> = data
        .existing_keys
        .iter()
        .filter(|k| !k.user_key.is_empty() && k.user_key.len() <= 64 && k.value.len() <= 128)
        .collect();

    if existing_keys.is_empty() {
        return;
    }

    // Build entries
    let mut entries: Vec<(InternalKey, Vec<u8>)> = Vec::new();

    for existing in &existing_keys {
        let key = InternalKey::new(
            existing.user_key.clone(),
            existing.seq_num.min(INTERNAL_KEY_SEQ_NUM_MAX),
            InternalKeyKind::Set,
            0,
        );
        entries.push((key, existing.value.clone()));
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

    // Track existing user keys and their latest values
    let mut latest_by_user_key: HashMap<Vec<u8>, (u64, Vec<u8>)> = HashMap::new();
    for (key, value) in &entries {
        latest_by_user_key
            .entry(key.user_key.clone())
            .and_modify(|(seq, val)| {
                if key.seq_num() > *seq {
                    *seq = key.seq_num();
                    *val = value.clone();
                }
            })
            .or_insert((key.seq_num(), value.clone()));
    }

    let existing_user_keys: HashSet<Vec<u8>> = latest_by_user_key.keys().cloned().collect();

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
    // TEST: Query correctness for all queries
    // ========================================
    for query in &data.queries {
        if query.user_key.is_empty() || query.user_key.len() > 64 {
            continue;
        }

        let query_seq = query.seq_num.min(INTERNAL_KEY_SEQ_NUM_MAX);
        let query_key = InternalKey::new(
            query.user_key.clone(),
            query_seq,
            InternalKeyKind::Set,
            0,
        );

        let result = table.get(&query_key);
        let key_exists = existing_user_keys.contains(&query.user_key);

        match result {
            Ok(Some((found_key, found_value))) => {
                // ========================================
                // REGRESSION CHECK: User key must match EXACTLY
                // ========================================
                // The bug from the regression test: using >= comparison
                // would return "key_bbb" when searching for "key_aaa"
                assert_eq!(
                    found_key.user_key.as_slice(),
                    query.user_key.as_slice(),
                    "REGRESSION BUG: get() returned wrong user_key!\n\
                     Queried: {:?}\n\
                     Got: {:?}\n\
                     This indicates a comparison bug (likely >= instead of ==)",
                    String::from_utf8_lossy(&query.user_key),
                    String::from_utf8_lossy(&found_key.user_key)
                );

                // Key must actually exist in our set
                assert!(
                    key_exists,
                    "get() returned a result for non-existent key {:?}",
                    String::from_utf8_lossy(&query.user_key)
                );

                // Verify seq_num is <= query seq_num (MVCC semantics)
                assert!(
                    found_key.seq_num() <= query_seq,
                    "get() returned version with seq={} which is > query seq={}",
                    found_key.seq_num(),
                    query_seq
                );

                // Verify it's the correct version
                if let Some((expected_seq, expected_value)) = latest_by_user_key.get(&query.user_key) {
                    if query_seq >= *expected_seq {
                        // Should get the latest version
                        assert_eq!(
                            found_key.seq_num(),
                            *expected_seq,
                            "get() returned wrong version for key {:?}",
                            String::from_utf8_lossy(&query.user_key)
                        );
                        assert_eq!(
                            &found_value,
                            expected_value,
                            "get() returned wrong value for key {:?}",
                            String::from_utf8_lossy(&query.user_key)
                        );
                    }
                }
            }
            Ok(None) => {
                if key_exists {
                    // Key exists, but maybe query seq_num is too low
                    if let Some((min_seq, _)) = latest_by_user_key.get(&query.user_key) {
                        // Find the minimum seq_num for this user_key
                        let min_seq_for_key = entries
                            .iter()
                            .filter(|(k, _)| k.user_key == query.user_key)
                            .map(|(k, _)| k.seq_num())
                            .min()
                            .unwrap_or(*min_seq);

                        if query_seq >= min_seq_for_key {
                            panic!(
                                "get() returned None for existing key {:?} with query_seq={}, \
                                 but key exists with seq_num={}",
                                String::from_utf8_lossy(&query.user_key),
                                query_seq,
                                min_seq_for_key
                            );
                        }
                        // Otherwise, it's correct - query seq is before any version
                    }
                }
                // Non-existent key correctly returns None
            }
            Err(e) => {
                panic!("Unexpected error in get(): {:?}", e);
            }
        }
    }

    // ========================================
    // TEST: Boundary keys (before/after/between)
    // ========================================
    let sorted_user_keys: Vec<_> = {
        let mut keys: Vec<_> = existing_user_keys.iter().cloned().collect();
        keys.sort();
        keys
    };

    // Test key before all existing keys
    if let Some(first_key) = sorted_user_keys.first() {
        if first_key.first().map_or(false, |&b| b > 0) {
            let mut before_key = first_key.clone();
            before_key[0] = before_key[0].saturating_sub(1);

            if !existing_user_keys.contains(&before_key) {
                let query_key = InternalKey::new(
                    before_key.clone(),
                    INTERNAL_KEY_SEQ_NUM_MAX,
                    InternalKeyKind::Set,
                    0,
                );

                let result = table.get(&query_key);
                assert!(
                    matches!(result, Ok(None)),
                    "Key before all entries should return None, got {:?}",
                    result
                );
            }
        }
    }

    // Test key after all existing keys
    if let Some(last_key) = sorted_user_keys.last() {
        let mut after_key = last_key.clone();
        after_key.push(0xFF);

        let query_key = InternalKey::new(
            after_key.clone(),
            INTERNAL_KEY_SEQ_NUM_MAX,
            InternalKeyKind::Set,
            0,
        );

        let result = table.get(&query_key);
        assert!(
            matches!(result, Ok(None)),
            "Key after all entries should return None, got {:?}",
            result
        );
    }

    // Test keys between existing keys
    for window in sorted_user_keys.windows(2) {
        let key_a = &window[0];
        let key_b = &window[1];

        // Try to create a key between key_a and key_b
        let mut between_key = key_a.clone();
        between_key.push(0x80); // Append a byte to make it > key_a

        // Only test if it's actually between and doesn't exist
        if user_cmp.compare(&between_key, key_a) == Ordering::Greater
            && user_cmp.compare(&between_key, key_b) == Ordering::Less
            && !existing_user_keys.contains(&between_key)
        {
            let query_key = InternalKey::new(
                between_key.clone(),
                INTERNAL_KEY_SEQ_NUM_MAX,
                InternalKeyKind::Set,
                0,
            );

            let result = table.get(&query_key);
            assert!(
                matches!(result, Ok(None)),
                "Key between {:?} and {:?} should return None, got {:?}",
                String::from_utf8_lossy(key_a),
                String::from_utf8_lossy(key_b),
                result
            );
        }
    }
});