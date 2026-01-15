#![no_main]
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::comparator::{BytewiseComparator, Comparator, InternalKeyComparator};
use surrealkv::sstable::block::{Block, BlockWriter};
use surrealkv::sstable::{InternalKey, InternalKeyKind, INTERNAL_KEY_SEQ_NUM_MAX};
use surrealkv::Options;

#[path = "mod.rs"]
mod helpers;
use helpers::{sort_and_deduplicate_entries, to_internal_key_kind, FuzzEntry};

#[derive(Arbitrary, Debug)]
struct FuzzBlockInput {
    entries: Vec<FuzzEntryRaw>,
    restart_interval: u8,
    keys_only: bool,
    /// Additional seek targets (may not exist in entries)
    seek_targets: Vec<SeekTarget>,
}

#[derive(Arbitrary, Debug)]
struct FuzzEntryRaw {
    user_key: Vec<u8>,
    value: Vec<u8>,
    seq_num: u64,
    kind: u8,
}

#[derive(Arbitrary, Debug)]
struct SeekTarget {
    user_key: Vec<u8>,
    seq_num: u64,
}

impl FuzzEntryRaw {
    fn to_fuzz_entry(&self) -> FuzzEntry {
        FuzzEntry {
            user_key: self.user_key.clone(),
            value: self.value.clone(),
            seq_num: self.seq_num,
            kind: to_internal_key_kind(self.kind),
        }
    }
}

fuzz_target!(|data: FuzzBlockInput| {
    // Tighter limits for faster fuzzing
    if data.entries.len() > 100 || data.seek_targets.len() > 20 {
        return;
    }

    // Limit key/value sizes
    for entry in &data.entries {
        if entry.user_key.len() > 128 || entry.value.len() > 256 {
            return;
        }
    }

    // Convert and clamp sequence numbers
    let mut entries: Vec<FuzzEntry> = data
        .entries
        .iter()
        .map(|e| {
            let mut entry = e.to_fuzz_entry();
            entry.seq_num = entry.seq_num.min(INTERNAL_KEY_SEQ_NUM_MAX);
            entry
        })
        .collect();

    // Filter empty keys (known edge case)
    entries.retain(|e| !e.user_key.is_empty());

    // Sort and deduplicate
    sort_and_deduplicate_entries(&mut entries);

    if entries.is_empty() {
        test_empty_block();
        return;
    }

    // Setup
    let restart_interval = ((data.restart_interval % 16) as usize).max(1);
    let user_cmp = Arc::new(BytewiseComparator::default());
    let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp.clone()));

    // Build block
    let mut builder =
        BlockWriter::new(4096, restart_interval, Arc::clone(&internal_cmp));

    for entry in &entries {
        let internal_key = entry.to_internal_key().encode();
        if builder.add(&internal_key, &entry.value).is_err() {
            return;
        }
    }

    let block_data = match builder.finish() {
        Ok(data) => data,
        Err(_) => return,
    };

    let block = Block::new(block_data, Arc::clone(&internal_cmp));

    // ========================================
    // TEST 1: Forward iteration roundtrip
    // ========================================
    {
        let mut iter = block.iter(data.keys_only).unwrap();
        iter.seek_to_first().unwrap();

        let mut count = 0;
        let mut prev_key: Option<Vec<u8>> = None;

        while iter.valid() {
            let key = iter.key();
            let key_bytes = iter.key_bytes().to_vec();

            // Verify ordering
            if let Some(ref pk) = prev_key {
                assert!(
                    internal_cmp.compare(pk, &key_bytes) == std::cmp::Ordering::Less,
                    "Forward iteration must be strictly ordered"
                );
            }

            // Verify exact match with expected entry
            let expected = &entries[count];
            assert_eq!(
                key.user_key.as_slice(),
                expected.user_key.as_slice(),
                "User key mismatch at index {}", count
            );
            assert_eq!(
                key.seq_num(),
                expected.seq_num,
                "Sequence number mismatch at index {}", count
            );
            assert_eq!(
                key.kind(),
                expected.kind,
                "Kind mismatch at index {}", count
            );

            if !data.keys_only {
                assert_eq!(
                    iter.value(),
                    expected.value,
                    "Value mismatch at index {}", count
                );
            }

            prev_key = Some(key_bytes);
            count += 1;

            if iter.advance().is_err() {
                break;
            }
        }

        assert_eq!(count, entries.len(), "Forward iteration count mismatch");
    }

    // ========================================
    // TEST 2: Backward iteration roundtrip
    // ========================================
    {
        let mut iter = block.iter(data.keys_only).unwrap();
        iter.seek_to_last().unwrap();

        let mut count = 0;
        let mut next_key: Option<Vec<u8>> = None;

        while iter.valid() {
            let key = iter.key();
            let key_bytes = iter.key_bytes().to_vec();

            // Verify ordering (should be decreasing going backward)
            if let Some(ref nk) = next_key {
                assert!(
                    internal_cmp.compare(&key_bytes, nk) == std::cmp::Ordering::Less,
                    "Backward iteration must produce decreasing keys"
                );
            }

            // Verify exact match (in reverse order)
            let expected_idx = entries.len() - 1 - count;
            let expected = &entries[expected_idx];

            assert_eq!(
                key.user_key.as_slice(),
                expected.user_key.as_slice(),
                "User key mismatch at reverse index {}", count
            );
            assert_eq!(
                key.seq_num(),
                expected.seq_num,
                "Sequence number mismatch at reverse index {}", count
            );

            if !data.keys_only {
                assert_eq!(
                    iter.value(),
                    expected.value,
                    "Value mismatch at reverse index {}", count
                );
            }

            next_key = Some(key_bytes);
            count += 1;

            match iter.prev() {
                Ok(true) => continue,
                Ok(false) => break,
                Err(_) => break,
            }
        }

        assert_eq!(count, entries.len(), "Backward iteration count mismatch");
    }

    // ========================================
    // TEST 3: Seek to each existing entry
    // ========================================
    {
        let mut iter = block.iter(data.keys_only).unwrap();

        for (idx, entry) in entries.iter().enumerate() {
            let seek_key = entry.to_internal_key().encode();
            iter.seek(&seek_key).unwrap();

            assert!(
                iter.valid(),
                "Seek to existing key should succeed at index {}", idx
            );

            let found_key = iter.key();
            assert_eq!(
                found_key.user_key.as_slice(),
                entry.user_key.as_slice(),
                "Seek found wrong user_key at index {}", idx
            );
            assert_eq!(
                found_key.seq_num(),
                entry.seq_num,
                "Seek found wrong seq_num at index {}", idx
            );

            if !data.keys_only {
                assert_eq!(
                    iter.value(),
                    entry.value,
                    "Seek found wrong value at index {}", idx
                );
            }
        }
    }

    // ========================================
    // TEST 4: Seek to arbitrary targets
    // ========================================
    {
        let mut iter = block.iter(data.keys_only).unwrap();

        for target in &data.seek_targets {
            if target.user_key.len() > 128 {
                continue;
            }

            let seek_key = InternalKey::new(
                target.user_key.clone(),
                target.seq_num.min(INTERNAL_KEY_SEQ_NUM_MAX),
                InternalKeyKind::Set,
                0,
            )
            .encode();

            let _ = iter.seek(&seek_key);

            if iter.valid() {
                // INVARIANT: Found key must be >= seek key
                assert!(
                    internal_cmp.compare(iter.key_bytes(), &seek_key) != std::cmp::Ordering::Less,
                    "Seek result must be >= target"
                );

                // INVARIANT: Must be the FIRST key >= seek key
                let expected_idx = entries
                    .iter()
                    .enumerate()
                    .find(|(_, e)| {
                        let ik = e.to_internal_key().encode();
                        internal_cmp.compare(&ik, &seek_key) != std::cmp::Ordering::Less
                    })
                    .map(|(i, _)| i);

                if let Some(idx) = expected_idx {
                    assert_eq!(
                        iter.key_bytes(),
                        entries[idx].to_internal_key().encode().as_slice(),
                        "Seek should find first key >= target"
                    );
                }
            } else {
                // INVARIANT: If invalid, all keys must be < seek key
                for entry in &entries {
                    let ik = entry.to_internal_key().encode();
                    assert!(
                        internal_cmp.compare(&ik, &seek_key) == std::cmp::Ordering::Less,
                        "Iterator invalid but entry >= seek key exists"
                    );
                }
            }
        }
    }

    // ========================================
    // TEST 5: Seek boundary cases
    // ========================================
    {
        let mut iter = block.iter(data.keys_only).unwrap();

        // Seek before first key (empty key with max seq)
        let before_first = InternalKey::new(vec![], u64::MAX, InternalKeyKind::Set, 0).encode();
        let _ = iter.seek(&before_first);
        if iter.valid() {
            // Should land on first entry
            assert_eq!(
                iter.key_bytes(),
                entries[0].to_internal_key().encode().as_slice(),
                "Seek before first should land on first entry"
            );
        }

        // Seek after last key
        let mut after_last_key = entries.last().unwrap().user_key.clone();
        after_last_key.push(0xFF);
        let after_last = InternalKey::new(after_last_key, 0, InternalKeyKind::Set, 0).encode();
        let _ = iter.seek(&after_last);
        // May or may not be valid depending on key - just verify invariant holds
        if iter.valid() {
            assert!(
                internal_cmp.compare(iter.key_bytes(), &after_last) != std::cmp::Ordering::Less,
                "Seek result must be >= target"
            );
        }
    }

    // ========================================
    // TEST 6: seek_to_first / seek_to_last consistency
    // ========================================
    {
        let mut iter = block.iter(data.keys_only).unwrap();

        iter.seek_to_first().unwrap();
        assert!(iter.valid());
        let first_key = iter.key_bytes().to_vec();

        iter.seek_to_last().unwrap();
        assert!(iter.valid());
        let last_key = iter.key_bytes().to_vec();

        // First should be <= last (equal only if single entry)
        let cmp = internal_cmp.compare(&first_key, &last_key);
        if entries.len() == 1 {
            assert_eq!(cmp, std::cmp::Ordering::Equal);
        } else {
            assert_eq!(cmp, std::cmp::Ordering::Less);
        }

        // Verify first and last match expected
        assert_eq!(first_key, entries[0].to_internal_key().encode());
        assert_eq!(last_key, entries.last().unwrap().to_internal_key().encode());
    }

    // ========================================
    // TEST 7: Iterator exhaustion and reset
    // ========================================
    {
        let mut iter = block.iter(data.keys_only).unwrap();

        // Exhaust forward
        iter.seek_to_first().unwrap();
        while iter.valid() {
            if iter.advance().is_err() {
                break;
            }
        }
        assert!(!iter.valid(), "Should be invalid after exhaustion");

        // Reset and verify can iterate again
        iter.reset();
        iter.seek_to_first().unwrap();
        assert!(iter.valid(), "Should be valid after reset and seek_to_first");

        // Exhaust backward
        iter.seek_to_last().unwrap();
        while iter.valid() {
            match iter.prev() {
                Ok(true) => continue,
                _ => break,
            }
        }
        assert!(!iter.valid(), "Should be invalid after backward exhaustion");
    }
});

fn test_empty_block() {
    let user_cmp = Arc::new(BytewiseComparator::default());
    let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp));

    let builder = BlockWriter::new(4096, 16, Arc::clone(&internal_cmp));
    let block_data = builder.finish().expect("Empty block should finish");

    let block = Block::new(block_data, Arc::clone(&internal_cmp));
    let mut iter = block.iter(false).expect("Should create iterator");

    iter.seek_to_first().ok();
    assert!(!iter.valid());

    iter.seek_to_last().ok();
    assert!(!iter.valid());
}