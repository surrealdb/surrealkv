#![no_main]
use std::cmp::Ordering;
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::comparator::{BytewiseComparator, Comparator, InternalKeyComparator};
use surrealkv::sstable::block::BlockHandle;
use surrealkv::sstable::index_block::{BlockHandleWithKey, TopLevelIndex};
use surrealkv::sstable::{InternalKey, InternalKeyKind};
use surrealkv::Options;

#[path = "mod.rs"]
mod helpers;
use helpers::make_internal_key;

#[derive(Arbitrary, Debug)]
struct FuzzLookupInput {
    partitions: Vec<FuzzPartition>,
    queries: Vec<FuzzQuery>,
    /// Test mode for targeted testing
    test_mode: TestMode,
}

#[derive(Arbitrary, Debug)]
enum TestMode {
    /// Standard random testing
    Random,
    /// Focus on boundary conditions
    Boundaries,
    /// Dense keys (many keys with same prefix)
    DenseKeys { prefix: Vec<u8> },
    /// Sequential keys
    Sequential { start: u8, count: u8 },
}

#[derive(Arbitrary, Debug, Clone)]
struct FuzzPartition {
    separator_key: Vec<u8>,
    seq_num: u64,
}

#[derive(Arbitrary, Debug)]
struct FuzzQuery {
    user_key: Vec<u8>,
    seq_num: u64,
    /// Query kind - real queries typically use Set, not Separator
    use_set_kind: bool,
}

fuzz_target!(|data: FuzzLookupInput| {
    // Tighter limits
    if data.partitions.len() > 100 || data.queries.len() > 50 {
        return;
    }

    // Limit key sizes
    for p in &data.partitions {
        if p.separator_key.len() > 128 {
            return;
        }
    }
    for q in &data.queries {
        if q.user_key.len() > 128 {
            return;
        }
    }

    let user_cmp = Arc::new(BytewiseComparator::default());
    let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp.clone()));

    // Generate partitions based on test mode
    let partitions = match &data.test_mode {
        TestMode::Random => data.partitions.clone(),
        TestMode::Boundaries => generate_boundary_partitions(&data.partitions),
        TestMode::DenseKeys { prefix } => generate_dense_partitions(prefix, &data.partitions),
        TestMode::Sequential { start, count } => generate_sequential_partitions(*start, *count),
    };

    // Filter empty keys
    let mut partitions: Vec<_> = partitions
        .into_iter()
        .filter(|p| !p.separator_key.is_empty())
        .collect();

    if partitions.is_empty() {
        test_empty_index();
        return;
    }

    // Sort partitions using internal key comparison
    // Separators use InternalKeyKind::Separator
    partitions.sort_by(|a, b| {
        let key_a = make_internal_key(&a.separator_key, a.seq_num, InternalKeyKind::Separator);
        let key_b = make_internal_key(&b.separator_key, b.seq_num, InternalKeyKind::Separator);
        internal_cmp.compare(&key_a, &key_b)
    });

    // Deduplicate
    partitions.dedup_by(|a, b| {
        let key_a = make_internal_key(&a.separator_key, a.seq_num, InternalKeyKind::Separator);
        let key_b = make_internal_key(&b.separator_key, b.seq_num, InternalKeyKind::Separator);
        internal_cmp.compare(&key_a, &key_b) == Ordering::Equal
    });

    if partitions.is_empty() {
        return;
    }

    // Build separator keys
    let separator_keys: Vec<Vec<u8>> = partitions
        .iter()
        .map(|p| make_internal_key(&p.separator_key, p.seq_num, InternalKeyKind::Separator))
        .collect();

    // Create index
    let opts = Arc::new(Options::default());
    let file: Arc<dyn surrealkv::vfs::File> = Arc::new(Vec::new());

    let blocks: Vec<BlockHandleWithKey> = separator_keys
        .iter()
        .enumerate()
        .map(|(i, sep_key)| BlockHandleWithKey {
            separator_key: sep_key.clone(),
            handle: BlockHandle::new(i * 100, 100),
        })
        .collect();

    let index = TopLevelIndex {
        id: 0,
        opts: opts.clone(),
        blocks,
        file,
    };

    // ========================================
    // TEST 1: Query with correct kinds
    // ========================================
    for query in &data.queries {
        // Real queries use Set kind, not Separator
        let query_kind = if query.use_set_kind {
            InternalKeyKind::Set
        } else {
            InternalKeyKind::Separator
        };

        let query_key = make_internal_key(&query.user_key, query.seq_num, query_kind);
        let result = index.find_block_handle_by_key(&query_key).unwrap();

        verify_lookup_invariants(
            &result,
            &query_key,
            &separator_keys,
            &internal_cmp,
        );
    }

    // ========================================
    // TEST 2: Each separator finds itself
    // ========================================
    for (i, sep_key) in separator_keys.iter().enumerate() {
        let result = index.find_block_handle_by_key(sep_key).unwrap();
        assert!(result.is_some(), "Separator {} should find a partition", i);
        let (idx, block) = result.unwrap();
        assert_eq!(idx, i, "Separator {} should find partition {}, got {}", i, i, idx);
        assert_eq!(
            &block.separator_key, sep_key,
            "Returned block should have matching separator"
        );
    }

    // ========================================
    // TEST 3: Boundary queries
    // ========================================
    test_boundary_queries(&index, &separator_keys, &partitions, &internal_cmp);

    // ========================================
    // TEST 4: Between-partition queries
    // ========================================
    test_between_partition_queries(&index, &separator_keys, &partitions, &internal_cmp);

    // ========================================
    // TEST 5: Sequence number edge cases
    // ========================================
    test_seq_num_edge_cases(&index, &separator_keys, &partitions, &internal_cmp);

    // ========================================
    // TEST 6: Exhaustive small index verification
    // ========================================
    if partitions.len() <= 10 {
        exhaustive_verification(&index, &separator_keys, &partitions, &internal_cmp);
    }
});

fn verify_lookup_invariants(
    result: &Option<(usize, &BlockHandleWithKey)>,
    query_key: &[u8],
    separator_keys: &[Vec<u8>],
    internal_cmp: &Arc<InternalKeyComparator>,
) {
    match result {
        Some((idx, block)) => {
            // INVARIANT 1: Index in bounds
            assert!(
                *idx < separator_keys.len(),
                "Index {} out of bounds (len={})",
                idx,
                separator_keys.len()
            );

            // INVARIANT 2: query_key <= separator at idx
            let cmp = internal_cmp.compare(query_key, &block.separator_key);
            assert!(
                cmp != Ordering::Greater,
                "query_key must be <= separator at idx {}. Got {:?}",
                idx,
                cmp
            );

            // INVARIANT 3: All separators before idx are < query_key
            for i in 0..*idx {
                let sep_cmp = internal_cmp.compare(&separator_keys[i], query_key);
                assert!(
                    sep_cmp == Ordering::Less,
                    "separator[{}] should be < query_key, got {:?}",
                    i,
                    sep_cmp
                );
            }

            // INVARIANT 4: This is the FIRST partition where query_key <= separator
            // (binary search correctness)
            if *idx > 0 {
                let prev_sep = &separator_keys[*idx - 1];
                let prev_cmp = internal_cmp.compare(query_key, prev_sep);
                assert!(
                    prev_cmp == Ordering::Greater,
                    "query_key should be > separator[{}] (previous partition)",
                    idx - 1
                );
            }
        }
        None => {
            // INVARIANT 5: query_key > all separators
            for (i, sep) in separator_keys.iter().enumerate() {
                let cmp = internal_cmp.compare(query_key, sep);
                assert!(
                    cmp == Ordering::Greater,
                    "query_key should be > separator[{}] when result is None, got {:?}",
                    i,
                    cmp
                );
            }
        }
    }
}

fn test_empty_index() {
    let opts = Arc::new(Options::default());
    let file: Arc<dyn surrealkv::vfs::File> = Arc::new(Vec::new());

    let index = TopLevelIndex {
        id: 0,
        opts,
        blocks: vec![],
        file,
    };

    let query = make_internal_key(b"test", 100, InternalKeyKind::Set);
    let result = index.find_block_handle_by_key(&query);

    // Empty index should return error
    assert!(result.is_err(), "Empty index should return error");
}

fn test_boundary_queries(
    index: &TopLevelIndex,
    separator_keys: &[Vec<u8>],
    partitions: &[FuzzPartition],
    internal_cmp: &Arc<InternalKeyComparator>,
) {
    // Query before first partition
    if let Some(first) = partitions.first() {
        // Key lexicographically before first separator
        let before_first = if first.separator_key.first().map_or(false, |&b| b > 0) {
            let mut key = first.separator_key.clone();
            key[0] = key[0].saturating_sub(1);
            Some(make_internal_key(&key, u64::MAX, InternalKeyKind::Set))
        } else {
            None
        };

        if let Some(query_key) = before_first {
            let result = index.find_block_handle_by_key(&query_key).unwrap();
            verify_lookup_invariants(&result, &query_key, separator_keys, internal_cmp);

            // Should find first partition (or None if query > all)
            if let Some((idx, _)) = result {
                // If found, should be partition 0 or later
                assert!(idx == 0 || internal_cmp.compare(&query_key, &separator_keys[0]) == Ordering::Greater);
            }
        }
    }

    // Query after last partition
    if let Some(last) = partitions.last() {
        let mut after_key = last.separator_key.clone();
        after_key.push(0xFF);
        let query_key = make_internal_key(&after_key, 0, InternalKeyKind::Set);

        let result = index.find_block_handle_by_key(&query_key).unwrap();
        verify_lookup_invariants(&result, &query_key, separator_keys, internal_cmp);

        // Should return None (beyond all partitions)
        // Note: may return Some if query_key <= last separator due to seq_num ordering
    }

    // Query with empty key (edge case)
    let empty_query = make_internal_key(&[], u64::MAX, InternalKeyKind::Set);
    let result = index.find_block_handle_by_key(&empty_query).unwrap();
    verify_lookup_invariants(&result, &empty_query, separator_keys, internal_cmp);
}

fn test_between_partition_queries(
    index: &TopLevelIndex,
    separator_keys: &[Vec<u8>],
    partitions: &[FuzzPartition],
    internal_cmp: &Arc<InternalKeyComparator>,
) {
    // Test queries that fall between partitions
    for i in 0..partitions.len().saturating_sub(1) {
        let sep_i = &partitions[i].separator_key;
        let sep_next = &partitions[i + 1].separator_key;

        // Try to create a key between sep_i and sep_next
        let between_key = create_key_between(sep_i, sep_next);

        if let Some(between) = between_key {
            let query_key = make_internal_key(&between, u64::MAX / 2, InternalKeyKind::Set);
            let result = index.find_block_handle_by_key(&query_key).unwrap();
            verify_lookup_invariants(&result, &query_key, separator_keys, internal_cmp);

            // Should find partition i+1 (the next partition)
            if let Some((idx, _)) = result {
                // The key is > separator[i], so it should go to partition i+1 or later
                assert!(
                    idx > i || internal_cmp.compare(&query_key, &separator_keys[i]) != Ordering::Greater,
                    "Query between partitions {} and {} should find partition > {}",
                    i, i + 1, i
                );
            }
        }
    }
}

fn create_key_between(a: &[u8], b: &[u8]) -> Option<Vec<u8>> {
    // Find first differing byte
    for (i, (&byte_a, &byte_b)) in a.iter().zip(b.iter()).enumerate() {
        if byte_a < byte_b {
            // Can create key between by incrementing byte_a
            if byte_a < byte_b - 1 {
                let mut result = a[..=i].to_vec();
                result[i] = byte_a + 1;
                return Some(result);
            } else if i + 1 < a.len() {
                // Same prefix, try appending
                let mut result = a[..=i].to_vec();
                result.push(0x80); // Middle value
                return Some(result);
            }
        }
    }

    // If a is prefix of b, append something
    if a.len() < b.len() {
        let mut result = a.to_vec();
        result.push(0x00);
        return Some(result);
    }

    None
}

fn test_seq_num_edge_cases(
    index: &TopLevelIndex,
    separator_keys: &[Vec<u8>],
    partitions: &[FuzzPartition],
    internal_cmp: &Arc<InternalKeyComparator>,
) {
    for partition in partitions {
        // Same user key, different seq_nums
        // Higher seq_num should sort BEFORE lower seq_num (descending order)

        // Query with max seq_num (should find earliest version)
        let query_max_seq = make_internal_key(
            &partition.separator_key,
            u64::MAX,
            InternalKeyKind::Set,
        );
        let result_max = index.find_block_handle_by_key(&query_max_seq).unwrap();
        verify_lookup_invariants(&result_max, &query_max_seq, separator_keys, internal_cmp);

        // Query with min seq_num (should find latest version or next partition)
        let query_min_seq = make_internal_key(
            &partition.separator_key,
            0,
            InternalKeyKind::Set,
        );
        let result_min = index.find_block_handle_by_key(&query_min_seq).unwrap();
        verify_lookup_invariants(&result_min, &query_min_seq, separator_keys, internal_cmp);

        // Query with same seq_num as separator
        let query_same_seq = make_internal_key(
            &partition.separator_key,
            partition.seq_num,
            InternalKeyKind::Set, // Note: different kind than Separator
        );
        let result_same = index.find_block_handle_by_key(&query_same_seq).unwrap();
        verify_lookup_invariants(&result_same, &query_same_seq, separator_keys, internal_cmp);
    }
}

fn exhaustive_verification(
    index: &TopLevelIndex,
    separator_keys: &[Vec<u8>],
    partitions: &[FuzzPartition],
    internal_cmp: &Arc<InternalKeyComparator>,
) {
    // For small indexes, verify every possible query position
    
    // Collect all unique user keys
    let mut all_keys: Vec<Vec<u8>> = partitions.iter().map(|p| p.separator_key.clone()).collect();
    
    // Add keys before, between, and after
    if let Some(first) = all_keys.first().cloned() {
        if !first.is_empty() {
            let mut before = first.clone();
            before[0] = before[0].saturating_sub(1);
            all_keys.push(before);
        }
    }
    
    if let Some(last) = all_keys.last().cloned() {
        let mut after = last.clone();
        after.push(0xFF);
        all_keys.push(after);
    }

    // Test each key with various seq_nums
    let seq_nums = [0u64, 1, 100, u64::MAX / 2, u64::MAX - 1, u64::MAX];

    for key in &all_keys {
        for &seq_num in &seq_nums {
            let query_key = make_internal_key(key, seq_num, InternalKeyKind::Set);
            let result = index.find_block_handle_by_key(&query_key).unwrap();
            verify_lookup_invariants(&result, &query_key, separator_keys, internal_cmp);
        }
    }
}

fn generate_boundary_partitions(base: &[FuzzPartition]) -> Vec<FuzzPartition> {
    let mut partitions = Vec::new();

    // Single byte keys at boundaries
    partitions.push(FuzzPartition { separator_key: vec![0x00], seq_num: 0 });
    partitions.push(FuzzPartition { separator_key: vec![0x7F], seq_num: 0 });
    partitions.push(FuzzPartition { separator_key: vec![0x80], seq_num: 0 });
    partitions.push(FuzzPartition { separator_key: vec![0xFF], seq_num: 0 });

    // Add base partitions
    partitions.extend(base.iter().take(20).cloned());

    partitions
}

fn generate_dense_partitions(prefix: &[u8], base: &[FuzzPartition]) -> Vec<FuzzPartition> {
    if prefix.len() > 64 {
        return base.to_vec();
    }

    let mut partitions = Vec::new();

    // Keys with same prefix, different suffixes
    for i in 0u8..20 {
        let mut key = prefix.to_vec();
        key.push(i);
        partitions.push(FuzzPartition {
            separator_key: key,
            seq_num: i as u64,
        });
    }

    // Add some base partitions
    partitions.extend(base.iter().take(10).cloned());

    partitions
}

fn generate_sequential_partitions(start: u8, count: u8) -> Vec<FuzzPartition> {
    let count = count.min(50);

    (0..count)
        .map(|i| FuzzPartition {
            separator_key: vec![start.wrapping_add(i)],
            seq_num: i as u64,
        })
        .collect()
}