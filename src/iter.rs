use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::{cmp::Ordering, sync::Arc};

use crate::error::Result;
use crate::{
    sstable::{InternalKey, InternalKeyKind},
    Key, Value,
};

pub type BoxedIterator<'a> = Box<dyn DoubleEndedIterator<Item = (Arc<InternalKey>, Value)> + 'a>;

// Holds a key-value pair and the iterator index
#[derive(Eq)]
pub(crate) struct HeapItem {
    pub(crate) key: Arc<InternalKey>,
    pub(crate) value: Value,
    pub(crate) iterator_index: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Invert for min-heap behavior
        other.cmp_internal(self)
    }
}

impl HeapItem {
    fn cmp_internal(&self, other: &Self) -> Ordering {
        // First compare by user key
        match self.key.user_key.cmp(&other.key.user_key) {
            Ordering::Equal => {
                // Same user key, compare by sequence number in DESCENDING order
                // (higher sequence number = more recent)
                match other.key.seq_num().cmp(&self.key.seq_num()) {
                    Ordering::Equal => self.iterator_index.cmp(&other.iterator_index),
                    ord => ord,
                }
            }
            ord => ord, // Different user keys
        }
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct MergeIterator<'a> {
    iterators: Vec<BoxedIterator<'a>>,
    // Heap of iterators, ordered by their current key
    heap: BinaryHeap<HeapItem>,
    // Current key we're processing, to skip duplicate versions
    current_user_key: Option<Key>,
    initialized: bool,
}

impl<'a> MergeIterator<'a> {
    pub fn new(iterators: Vec<BoxedIterator<'a>>) -> Self {
        let heap = BinaryHeap::with_capacity(iterators.len());

        Self {
            iterators,
            heap,
            current_user_key: None,
            initialized: false,
        }
    }

    fn initialize(&mut self) {
        // Pull the first item from each iterator and add to heap
        for (idx, iter) in self.iterators.iter_mut().enumerate() {
            if let Some((key, value)) = iter.next() {
                self.heap.push(HeapItem {
                    key,
                    value,
                    iterator_index: idx,
                });
            }
        }
        self.initialized = true;
    }
}

impl Iterator for MergeIterator<'_> {
    type Item = (Arc<InternalKey>, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if !self.initialized {
            self.initialize();
        }

        loop {
            // Get the smallest item from the heap
            let heap_item = self.heap.pop()?;

            // Pull the next item from the same iterator and add back to heap
            if let Some((key, value)) = self.iterators[heap_item.iterator_index].next() {
                self.heap.push(HeapItem {
                    key,
                    value,
                    iterator_index: heap_item.iterator_index,
                });
            }

            let user_key = heap_item.key.user_key.clone();

            // Check if this is a new user key
            let is_new_key = match &self.current_user_key {
                None => true,
                Some(current) => &user_key != current,
            };

            if is_new_key {
                // New user key - update tracking and return this item
                self.current_user_key = Some(user_key);
                return Some((heap_item.key, heap_item.value));
            } else {
                // Same user key - skip this older version and continue to next
                continue;
            }
        }
    }
}

pub struct CompactionIterator<'a> {
    merge_iter: MergeIterator<'a>,
    is_bottom_level: bool,

    // Track tombstone status
    current_user_key: Option<Key>,
    current_key_has_tombstone: bool,
    /// Collected discard statistics: file_id -> total_discarded_bytes
    pub discard_stats: HashMap<u64, i64>,
}

impl<'a> CompactionIterator<'a> {
    pub fn new(merge_iter: MergeIterator<'a>, is_bottom_level: bool) -> Self {
        Self {
            merge_iter,
            is_bottom_level,
            current_user_key: None,
            current_key_has_tombstone: false,
            discard_stats: HashMap::new(),
        }
    }
}

impl Iterator for CompactionIterator<'_> {
    type Item = (Arc<InternalKey>, Value);

    fn next(&mut self) -> Option<Self::Item> {
        for (key, value) in self.merge_iter.by_ref() {
            let user_key = key.user_key.clone();
            let is_tombstone = key.kind() == InternalKeyKind::Delete;

            // Check if this is a new key
            let is_new_key = match &self.current_user_key {
                None => true,
                Some(current) => &user_key != current,
            };

            // Determine if this entry should be marked as stale in VLog
            let should_mark_stale = if is_new_key {
                // New key: mark stale if it's a tombstone being removed at bottom level
                is_tombstone && self.is_bottom_level
            } else {
                // Same key, older version: mark stale if we've seen a tombstone for this key
                self.current_key_has_tombstone
            };

            // Collect discard statistics instead of immediately updating VLog
            if should_mark_stale {
                collect_vlog_discard_stats(&mut self.discard_stats, &value).unwrap();
            }

            if is_new_key {
                // Reset tracking for the new key
                self.current_user_key = Some(user_key.clone());
                self.current_key_has_tombstone = is_tombstone;

                // At the bottom level, skip tombstones
                if is_tombstone && self.is_bottom_level {
                    continue;
                }

                // Return this entry
                return Some((key, value));
            } else {
                // Update tombstone status for same key
                if is_tombstone {
                    self.current_key_has_tombstone = true;
                }

                // Skip this entry - older version
                continue;
            }
        }

        None
    }
}

fn collect_vlog_discard_stats(discard_stats: &mut HashMap<u64, i64>, value: &Value) -> Result<()> {
    // Check if this value is a VLog pointer
    if let Some(pointer) = crate::vlog::ValuePointer::try_decode(value)? {
        let value_data_size = pointer.total_entry_size() as i64;
        *discard_stats.entry(pointer.file_id).or_insert(0) += value_data_size;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        sstable::{InternalKey, InternalKeyKind},
        Value,
    };
    use std::sync::Arc;

    fn create_internal_key(
        user_key: &str,
        sequence: u64,
        kind: InternalKeyKind,
    ) -> Arc<InternalKey> {
        InternalKey::new(user_key.as_bytes().to_vec(), sequence, kind).into()
    }

    // Creates a mock iterator with predefined entries
    struct MockIterator {
        items: Vec<(Arc<InternalKey>, Value)>,
        index: usize,
    }

    impl MockIterator {
        fn new(items: Vec<(Arc<InternalKey>, Value)>) -> Self {
            Self { items, index: 0 }
        }
    }

    impl Iterator for MockIterator {
        type Item = (Arc<InternalKey>, Value);

        fn next(&mut self) -> Option<Self::Item> {
            if self.index < self.items.len() {
                let item = self.items[self.index].clone();
                self.index += 1;
                Some(item)
            } else {
                None
            }
        }
    }

    impl DoubleEndedIterator for MockIterator {
        fn next_back(&mut self) -> Option<Self::Item> {
            if self.index < self.items.len() {
                let item = self.items.pop()?;
                Some(item)
            } else {
                None
            }
        }
    }

    #[test]
    fn test_merge_iterator_sequence_ordering() {
        // First iterator (L0) with tombstones for even keys
        let mut items1 = Vec::new();
        for i in 0..10 {
            if i % 2 == 0 {
                // Tombstone for even keys
                let key =
                    create_internal_key(&format!("key-{:03}", i), 200, InternalKeyKind::Delete);
                let empty_value: Vec<u8> = Vec::new();
                items1.push((key, empty_value.into()));
            }
        }

        // Second iterator (L1) with values for all keys
        let mut items2 = Vec::new();
        for i in 0..20 {
            let key = create_internal_key(&format!("key-{:03}", i), 100, InternalKeyKind::Set);
            let value_str = format!("value-{}", i);
            let value_vec: Vec<u8> = value_str.into_bytes();
            items2.push((key, value_vec.into()));
        }

        let iter1 = Box::new(MockIterator::new(items1));
        let iter2 = Box::new(MockIterator::new(items2));

        // Create the merge iterator
        let merge_iter = MergeIterator::new(vec![iter1, iter2]);

        // Collect all items
        let mut result = Vec::new();
        for (key, _) in merge_iter {
            let key_str = String::from_utf8_lossy(&key.user_key).to_string();
            let seq = key.seq_num();
            let kind = key.kind();
            result.push((key_str, seq, kind));
        }

        // Now verify the output

        // 1. First, check that all keys are in ascending order by user key
        for i in 1..result.len() {
            assert!(
                result[i - 1].0 <= result[i].0,
                "Keys not in ascending order: {} vs {}",
                result[i - 1].0,
                result[i].0
            );
        }

        // 2. Check that for keys with tombstones, the tombstone comes first
        for i in 0..10 {
            if i % 2 == 0 {
                // Find this key in the result
                let key = format!("key-{:03}", i);
                let entries: Vec<_> = result.iter().filter(|(k, _, _)| k == &key).collect();

                // Should only have one entry per key due to deduplication
                assert_eq!(entries.len(), 1, "Key {} has multiple entries", key);

                // And it should be the tombstone (seq=200, kind=Delete)
                let (_, seq, kind) = entries[0];
                assert_eq!(*seq, 200, "Key {} has wrong sequence", key);
                assert_eq!(*kind, InternalKeyKind::Delete, "Key {} has wrong kind", key);
            }
        }

        // 3. Check that we have the correct total number of entries
        // All keys (20) because we get 5 keys with tombstones from items1 and 15 other keys from items2
        assert_eq!(result.len(), 20, "Wrong number of entries");
    }

    #[test]
    fn test_compaction_iterator_tombstone_filtering() {
        let mut items1 = Vec::new();
        for i in 0..10 {
            if i % 2 == 0 {
                // Tombstone for even keys
                let key =
                    create_internal_key(&format!("key-{:03}", i), 200, InternalKeyKind::Delete);
                let empty_value: Vec<u8> = Vec::new();
                items1.push((key, empty_value.into()));
            }
        }

        let mut items2 = Vec::new();
        for i in 0..20 {
            let key = create_internal_key(&format!("key-{:03}", i), 100, InternalKeyKind::Set);
            let value_str = format!("value-{}", i);
            let value_vec: Vec<u8> = value_str.into_bytes();
            items2.push((key, value_vec.into()));
        }

        let iter1 = Box::new(MockIterator::new(items1));
        let iter2 = Box::new(MockIterator::new(items2));

        // Create the merge iterator
        let merge_iter = MergeIterator::new(vec![iter1, iter2]);

        // Test non-bottom level (should keep tombstones)
        let comp_iter = CompactionIterator::new(merge_iter, false);

        // Collect all items
        let mut result = Vec::new();
        for (key, _) in comp_iter {
            let key_str = String::from_utf8_lossy(&key.user_key).to_string();
            let seq = key.seq_num();
            let kind = key.kind();
            result.push((key_str, seq, kind));
        }

        // Now verify the output

        // 1. Verify all keys are in ascending order (important for block writer)
        for i in 1..result.len() {
            assert!(
                result[i - 1].0 <= result[i].0,
                "Keys not in ascending order: {} vs {}",
                result[i - 1].0,
                result[i].0
            );
        }

        // 2. For even keys (0, 2, 4...), we should have tombstones
        for entry in &result {
            let (key, seq, kind) = entry;

            // Extract key number
            let key_num = key.strip_prefix("key-").unwrap().parse::<i32>().unwrap();

            if key_num % 2 == 0 && key_num < 10 {
                // Even keys under 10 should be tombstones with seq=200
                assert_eq!(*seq, 200, "Even key {} has wrong sequence", key);
                assert_eq!(
                    *kind,
                    InternalKeyKind::Delete,
                    "Even key {} has wrong kind",
                    key
                );
            } else {
                // Odd keys and even keys >= 10 should be values with seq=100
                assert_eq!(*seq, 100, "Key {} has wrong sequence", key);
                assert_eq!(*kind, InternalKeyKind::Set, "Key {} has wrong kind", key);
            }
        }

        // 3. Test bottom level (should drop tombstones)
        let mut items1 = Vec::new();
        for i in 0..10 {
            if i % 2 == 0 {
                // Tombstone for even keys
                let key =
                    create_internal_key(&format!("key-{:03}", i), 200, InternalKeyKind::Delete);
                // Create empty Vec<u8> and wrap it in Arc for tombstone value
                let empty_value: Vec<u8> = Vec::new();
                items1.push((key, empty_value.into()));
            }
        }

        let mut items2 = Vec::new();
        for i in 0..20 {
            let key = create_internal_key(&format!("key-{:03}", i), 100, InternalKeyKind::Set);
            let value_str = format!("value-{}", i);
            let value_vec: Vec<u8> = value_str.into_bytes();
            items2.push((key, value_vec.into()));
        }

        let iter1 = Box::new(MockIterator::new(items1));
        let iter2 = Box::new(MockIterator::new(items2));

        // Create the merge iterator
        let merge_iter = MergeIterator::new(vec![iter1, iter2]);

        // Use bottom level
        let comp_iter = CompactionIterator::new(merge_iter, true);

        // Collect all items
        let mut bottom_result = Vec::new();
        for (key, _) in comp_iter {
            let key_str = String::from_utf8_lossy(&key.user_key).to_string();
            bottom_result.push(key_str);
        }

        // At bottom level, tombstones should be removed
        for i in 0..20 {
            let key = format!("key-{:03}", i);

            if i % 2 == 0 && i < 10 {
                // Even keys under 10 should be removed due to tombstones
                assert!(
                    !bottom_result.contains(&key),
                    "Key {} should be removed at bottom level",
                    key
                );
            } else {
                // Other keys should remain
                assert!(
                    bottom_result.contains(&key),
                    "Key {} should exist at bottom level",
                    key
                );
            }
        }
    }
}
