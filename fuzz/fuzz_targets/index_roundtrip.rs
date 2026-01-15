#![no_main]
use std::io::Cursor;
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::comparator::{BytewiseComparator, Comparator, InternalKeyComparator};
use surrealkv::sstable::block::BlockHandle;
use surrealkv::sstable::index_block::{TopLevelIndex, TopLevelIndexWriter};
use surrealkv::sstable::InternalKeyKind;
use surrealkv::{CompressionType, Options};

#[path = "mod.rs"]
mod helpers;
use helpers::make_internal_key;

#[derive(Arbitrary, Debug)]
struct FuzzWriterInput {
    entries: Vec<IndexEntry>,
    max_block_size: u16,
    compression: u8,
}

#[derive(Arbitrary, Debug, Clone)]
struct IndexEntry {
    user_key: Vec<u8>,
    seq_num: u64,
    block_offset: u32,
    block_size: u16,
}

fuzz_target!(|data: FuzzWriterInput| {
    // Limits
    if data.entries.len() > 100 {
        return;
    }

    for entry in &data.entries {
        if entry.user_key.is_empty() || entry.user_key.len() > 128 {
            return;
        }
    }

    // Setup
    let mut opts = Options::default();
    let user_cmp = Arc::new(BytewiseComparator::default());
    let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp.clone()));
    opts.internal_comparator = internal_cmp.clone();
    let opts = Arc::new(opts);

    // Sort and deduplicate entries
    let mut entries = data.entries.clone();
    entries.sort_by(|a, b| {
        let key_a = make_internal_key(&a.user_key, a.seq_num, InternalKeyKind::Set);
        let key_b = make_internal_key(&b.user_key, b.seq_num, InternalKeyKind::Set);
        internal_cmp.compare(&key_a, &key_b)
    });
    entries.dedup_by(|a, b| {
        let key_a = make_internal_key(&a.user_key, a.seq_num, InternalKeyKind::Set);
        let key_b = make_internal_key(&b.user_key, b.seq_num, InternalKeyKind::Set);
        internal_cmp.compare(&key_a, &key_b) == std::cmp::Ordering::Equal
    });

    if entries.is_empty() {
        return;
    }

    // Create writer
    let max_block_size = (data.max_block_size as usize).max(64).min(4096);
    let mut writer = TopLevelIndexWriter::new(Arc::clone(&opts), max_block_size);

    // Add entries
    for entry in &entries {
        let key = make_internal_key(&entry.user_key, entry.seq_num, InternalKeyKind::Set);
        let handle = BlockHandle::new(entry.block_offset as usize, entry.block_size as usize);
        if writer.add(&key, &handle.encode()).is_err() {
            return;
        }
    }

    // Finish writing
    let compression = match data.compression % 3 {
        0 => CompressionType::None,
        1 => CompressionType::SnappyCompression,
        _ => CompressionType::None,
    };

    let mut buffer = Vec::new();
    let (top_level_handle, final_offset) = match writer.finish(&mut buffer, compression, 0) {
        Ok(result) => result,
        Err(_) => return,
    };

    // Verify stats
    assert!(writer.num_partitions() > 0, "Should have at least one partition");
    assert!(writer.index_size() > 0, "Index size should be > 0");
    assert!(writer.top_level_index_size() > 0, "Top level index size should be > 0");

    // Verify buffer structure
    assert!(!buffer.is_empty(), "Buffer should not be empty");
    assert_eq!(final_offset, buffer.len(), "Final offset should match buffer length");
    assert!(top_level_handle.offset < buffer.len(), "Handle offset should be in bounds");
    assert!(
        top_level_handle.offset + top_level_handle.size <= buffer.len(),
        "Handle should not exceed buffer"
    );

    // ========================================
    // Roundtrip verification: read back the index
    // ========================================
    let file_arc: Arc<dyn surrealkv::vfs::File> = Arc::new(buffer);

    let index = match TopLevelIndex::new(0, Arc::clone(&opts), file_arc, &top_level_handle) {
        Ok(idx) => idx,
        Err(e) => panic!("Failed to read back TopLevelIndex: {:?}", e),
    };

    // Verify partition count matches
    assert_eq!(
        index.blocks.len() as u64,
        writer.num_partitions(),
        "Partition count mismatch after roundtrip"
    );

    // ========================================
    // Verify each entry can be found via lookup
    // ========================================
    for entry in &entries {
        let query_key = make_internal_key(&entry.user_key, entry.seq_num, InternalKeyKind::Set);

        let result = index.find_block_handle_by_key(&query_key);

        match result {
            Ok(Some((idx, block_with_key))) => {
                // INVARIANT: Index in bounds
                assert!(
                    idx < index.blocks.len(),
                    "Lookup returned out-of-bounds index"
                );

                // INVARIANT: Query key should be <= separator key
                let cmp = internal_cmp.compare(&query_key, &block_with_key.separator_key);
                assert!(
                    cmp != std::cmp::Ordering::Greater,
                    "Query key should be <= separator at found partition"
                );
            }
            Ok(None) => {
                panic!(
                    "Lookup returned None for entry that should exist: {:?}",
                    entry.user_key
                );
            }
            Err(e) => {
                panic!("Lookup error for entry {:?}: {:?}", entry.user_key, e);
            }
        }
    }
});