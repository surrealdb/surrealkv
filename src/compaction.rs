use std::sync::atomic::{AtomicBool, Ordering};

use crate::entry::{Entry, Record};
use crate::error::{Error, Result};
use crate::log::SegmentRef;
use crate::store::Store;

struct CompactionGuard<'a> {
    is_compacting: &'a AtomicBool,
}

impl<'a> CompactionGuard<'a> {
    fn new(is_compacting: &'a AtomicBool) -> Self {
        is_compacting.store(true, Ordering::SeqCst);
        CompactionGuard { is_compacting }
    }
}

impl Drop for CompactionGuard<'_> {
    fn drop(&mut self) {
        self.is_compacting.store(false, Ordering::SeqCst);
    }
}

impl Store {
    pub fn compact(&self) -> Result<()> {
        // Early return if the store is closed or compaction is already in progress
        if self.is_closed.load(Ordering::SeqCst) || !self.core.opts.should_persist_data() {
            return Err(Error::InvalidOperation);
        }

        // Check if compaction is already in progress
        if self.is_compacting.load(Ordering::SeqCst) {
            return Err(Error::CompactionAlreadyInProgress);
        }

        // Clear compaction stats before starting compaction
        self.stats.compaction_stats.reset();

        // Acquire compaction guard
        let _guard = CompactionGuard::new(&self.is_compacting);

        // Check which segments need compaction
        let segments_to_compact = self.get_segments_for_compaction()?;

        // Early return if no segments need compaction
        if segments_to_compact.is_empty() {
            return Ok(());
        }

        println!(
            "Compacting {} segments with >{}% deleted keys",
            segments_to_compact.len(),
            self.core.opts.compaction_discard_threshold
        );

        // Process the segments that need compaction
        self.compact_selected_segments(&segments_to_compact)?;

        // Clean up discard stats for compacted segments
        {
            let mut discard_stats = self.core.discard_stats.write();
            for segment in &segments_to_compact {
                discard_stats.remove_file(segment.id);
            }
        }

        Ok(())
    }

    /// Gets list of segments that need compaction using discard statistics
    fn get_segments_for_compaction(&self) -> Result<Vec<SegmentRef>> {
        let deletion_threshold = self.core.opts.compaction_discard_threshold;

        let discard_stats = self.core.discard_stats.read();
        let clog_dir = self.core.opts.dir.join("clog");

        let segment_ids_to_compact =
            discard_stats.get_segments_exceeding_threshold(deletion_threshold, &clog_dir);

        // Read all segments from directory to get properly formatted SegmentRef objects
        let all_segments = SegmentRef::read_segments_from_directory(&clog_dir)?;

        // Filter to only include segments that need compaction
        let segments_to_compact: Vec<SegmentRef> = all_segments
            .into_iter()
            .filter(|segment| segment_ids_to_compact.contains(&segment.id))
            .collect();

        Ok(segments_to_compact)
    }

    /// Compacts the selected segments by reading them directly and writing live entries back to the store
    fn compact_selected_segments(&self, segments: &[SegmentRef]) -> Result<()> {
        use crate::log::MultiSegmentReader;
        use crate::reader::{Reader, RecordReader};

        let delete_list = self.core.global_delete_list.read();
        let mut live_entries = Vec::new();

        // Read all live entries from segments that need compaction
        for segment in segments {
            // Use the segment with its proper header offset (don't create a new one)
            let reader = MultiSegmentReader::new(vec![SegmentRef {
                file_path: segment.file_path.clone(),
                file_header_offset: segment.file_header_offset,
                id: segment.id,
            }])?;
            let reader = Reader::new_from(reader);
            let mut record_reader = RecordReader::new(reader);
            let mut record = Record::new();

            loop {
                record.reset();
                match record_reader.read_into(&mut record) {
                    Ok(_) => {
                        // Check if this key is NOT in the global delete list (i.e., it's still active)
                        if delete_list.search(&record.key).unwrap_or(None).is_none() {
                            // This key is still active, convert to Entry and add to live entries
                            let mut entry = Entry::new(&record.key, &record.value);
                            if let Some(metadata) = record.metadata.clone() {
                                entry.set_metadata(metadata);
                            }
                            entry.set_ts(record.ts);
                            live_entries.push(entry);
                            self.stats.compaction_stats.add_record();
                        } else {
                            // Key is deleted, increment deleted counter
                            self.stats.compaction_stats.delete_record();
                        }
                    }
                    Err(Error::LogError(crate::log::Error::Eof)) => break,
                    Err(e) => return Err(e),
                }
            }
        }

        // If we have live entries, write them back to the store
        if !live_entries.is_empty() {
            // Take the commit write lock to ensure atomic operation (similar to transaction commit)
            let _commit_lock = self.core.commit_write_lock.lock();

            // Get a proper transaction ID from the Oracle (like normal transactions do)
            let tx_id = self.core.oracle.new_commit_ts_internal();

            // Write the live entries back to the store using the same path as transaction commit
            self.core.write_entries(
                live_entries,
                tx_id,
                crate::transaction::Durability::Immediate,
            )?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rand::prelude::SliceRandom;
    use rand::{distributions::Alphanumeric, Rng};
    use std::collections::HashSet;

    use crate::option::Options;
    use crate::store::Store;

    use bytes::Bytes;
    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    #[test]
    fn basic_compaction() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_value_threshold = 0;
        opts.max_value_cache_size = 0;

        // Create a new store instance with VariableKey as the key type
        let store = Store::new(opts.clone()).expect("should create store");

        // Number of keys to generate and write
        let num_keys_to_write = 20;

        // Create a vector to store the generated keys
        let mut keys: Vec<Bytes> = Vec::new();

        for counter in 1usize..=num_keys_to_write {
            // Convert the counter to Bytes
            let key_bytes = Bytes::from(counter.to_le_bytes().to_vec());

            // Add the key to the vector
            keys.push(key_bytes);
        }

        let default_value = Bytes::from("default_value".to_string());

        // Write the keys to the store
        for key in keys.iter() {
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().unwrap();
        }

        // Number of keys to delete
        let num_keys_to_delete = 5;

        // Delete the first 5 keys from the store
        for key in keys.iter().take(num_keys_to_delete) {
            let mut txn = store.begin().unwrap();
            txn.delete(key).unwrap();
            txn.commit().unwrap();
        }

        store.compact().unwrap();
        store.close().unwrap();

        opts.max_value_threshold = 20;
        opts.max_value_cache_size = 20;
        let store = Store::new(opts).expect("should create store");

        // Read the keys to the store
        for key in keys.iter().skip(num_keys_to_delete) {
            // Start a new read transaction
            let mut txn = store.begin().unwrap();
            let val = txn.get(key).unwrap().unwrap();
            // Assert that the value retrieved in txn matches default_value
            assert_eq!(val, default_value.as_ref());
        }
    }

    #[test]
    fn compaction_with_mixed_operations() {
        let temp_dir = create_temp_directory();

        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_value_threshold = 0;
        opts.max_value_cache_size = 0;

        let store = Store::new(opts.clone()).expect("should create store");

        let num_keys_to_write = 100;
        let mut keys: Vec<Bytes> = Vec::new();

        // Generate keys and values
        for counter in 1usize..=num_keys_to_write {
            let key_bytes = Bytes::from(counter.to_le_bytes().to_vec());
            keys.push(key_bytes);
        }

        let default_value = Bytes::from("default_value".to_string());
        let updated_value = Bytes::from("updated_value".to_string());

        // Write initial values
        for key in keys.iter() {
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().unwrap();
        }

        // Update half of the keys
        for key in keys.iter().take(num_keys_to_write / 2) {
            let mut txn = store.begin().unwrap();
            txn.set(key, &updated_value).unwrap();
            txn.commit().unwrap();
        }

        // Delete a quarter of the keys
        let num_keys_to_delete = num_keys_to_write / 4;
        for key in keys.iter().take(num_keys_to_delete) {
            let mut txn = store.begin().unwrap();
            txn.delete(key).unwrap();
            txn.commit().unwrap();
        }

        store.compact().unwrap();
        store.close().unwrap();

        // Reopen the store to verify persistence
        let reopened_store = Store::new(opts).expect("should reopen store");

        // Verify that deleted keys are not present
        for key in keys.iter().take(num_keys_to_delete) {
            let mut txn = reopened_store.begin().unwrap();
            assert!(txn.get(key).unwrap().is_none());
        }

        // Verify that the first half of the remaining keys have the updated value
        for key in keys
            .iter()
            .skip(num_keys_to_delete)
            .take(num_keys_to_write / 2 - num_keys_to_delete)
        {
            let mut txn = reopened_store.begin().unwrap();
            let val = txn.get(key).unwrap().unwrap();
            assert_eq!(val, updated_value.as_ref());
        }

        // Verify that the second half of the keys still have the default value
        for key in keys.iter().skip(num_keys_to_write / 2) {
            let mut txn = reopened_store.begin().unwrap();
            let val = txn.get(key).unwrap().unwrap();
            assert_eq!(val, default_value.as_ref());
        }

        reopened_store.close().unwrap();
    }

    #[test]
    fn compaction_persistence_across_restarts() {
        let temp_dir = create_temp_directory();

        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_value_threshold = 0;
        opts.max_value_cache_size = 0;

        let store = Store::new(opts.clone()).expect("should create store");

        let num_keys_to_write = 50;
        let mut keys: Vec<Bytes> = Vec::new();

        for counter in 1usize..=num_keys_to_write {
            let key_bytes = Bytes::from(counter.to_le_bytes().to_vec());
            keys.push(key_bytes);
        }

        let default_value = Bytes::from("default_value".to_string());

        // Write keys to the store
        for key in keys.iter() {
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().unwrap();
        }

        store.compact().unwrap();
        store.close().unwrap();

        // Reopen the store to verify persistence
        let reopened_store = Store::new(opts).expect("should reopen store");

        // Verify that all keys still exist with the correct value
        for key in keys.iter() {
            let mut txn = reopened_store.begin().unwrap();
            let val = txn.get(key).unwrap().unwrap();
            assert_eq!(val, default_value.as_ref());
        }

        reopened_store.close().unwrap();
    }

    #[test]
    fn compaction_and_post_compaction_writes() {
        let temp_dir = create_temp_directory();

        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_value_threshold = 0;
        opts.max_value_cache_size = 0;

        let store = Store::new(opts.clone()).expect("should create store");

        let num_initial_keys = 50;
        let num_post_compaction_keys = 25;
        let mut initial_keys: Vec<Bytes> = Vec::new();
        let mut post_compaction_keys: Vec<Bytes> = Vec::new();

        // Generate initial keys and values
        for counter in 1usize..=num_initial_keys {
            let key_bytes = Bytes::from(counter.to_le_bytes().to_vec());
            initial_keys.push(key_bytes);
        }

        // Generate post-compaction keys and values
        for counter in (num_initial_keys + 1)..=(num_initial_keys + num_post_compaction_keys) {
            let key_bytes = Bytes::from(counter.to_le_bytes().to_vec());
            post_compaction_keys.push(key_bytes);
        }

        let default_value = Bytes::from("default_value".to_string());

        // Write initial values
        for key in initial_keys.iter() {
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().unwrap();
        }

        // Trigger compaction
        store.compact().unwrap();

        // Write post-compaction values
        for key in post_compaction_keys.iter() {
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().unwrap();
        }

        store.close().unwrap();

        // Reopen the store to verify persistence of all keys
        let reopened_store = Store::new(opts).expect("should reopen store");

        // Verify initial keys are present
        for key in initial_keys.iter() {
            let mut txn = reopened_store.begin().unwrap();
            let val = txn.get(key).unwrap().unwrap();
            assert_eq!(val, default_value.as_ref());
        }

        // Verify post-compaction keys are present
        for key in post_compaction_keys.iter() {
            let mut txn = reopened_store.begin().unwrap();
            let val = txn.get(key).unwrap().unwrap();
            assert_eq!(val, default_value.as_ref());
        }

        reopened_store.close().unwrap();
    }

    #[test]
    fn compaction_with_random_keys_and_deletes() {
        let temp_dir = create_temp_directory();

        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_value_threshold = 0;
        opts.max_value_cache_size = 0;

        let store = Store::new(opts.clone()).expect("should create store");

        let num_initial_keys = 50;
        let num_post_compaction_keys = 25;
        let num_keys_to_delete = 10;
        let mut initial_keys: Vec<Bytes> = Vec::new();
        let mut post_compaction_keys: Vec<Bytes> = Vec::new();
        let mut keys_to_delete: HashSet<Bytes> = HashSet::new();

        // Generate initial random keys
        for _ in 0..num_initial_keys {
            let key: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(10)
                .map(char::from)
                .collect();
            initial_keys.push(Bytes::from(key));
        }

        // Select random keys to delete
        while keys_to_delete.len() < num_keys_to_delete {
            let random_key = initial_keys
                .choose(&mut rand::thread_rng())
                .unwrap()
                .clone();
            keys_to_delete.insert(random_key.clone());
        }

        // Generate post-compaction random keys
        for _ in 0..num_post_compaction_keys {
            let key: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(10)
                .map(char::from)
                .collect();
            post_compaction_keys.push(Bytes::from(key));
        }

        let default_value = Bytes::from("default_value".to_string());

        // Write initial values
        for key in &initial_keys {
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().unwrap();
        }

        // Trigger compaction
        store.compact().unwrap();

        // Delete selected keys
        for key in &keys_to_delete {
            let mut txn = store.begin().unwrap();
            txn.delete(key).unwrap();
            txn.commit().unwrap();
        }

        // Write post-compaction values
        for key in &post_compaction_keys {
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().unwrap();
        }

        store.close().unwrap();

        // Reopen the store to verify persistence of all keys
        let reopened_store = Store::new(opts).expect("should reopen store");

        // Verify initial keys are present (except deleted ones)
        for key in &initial_keys {
            if !keys_to_delete.contains(key) {
                let mut txn = reopened_store.begin().unwrap();
                let val = txn.get(key).unwrap().unwrap();
                assert_eq!(val, default_value.as_ref());
            }
        }

        // Verify deleted keys are not present
        for key in &keys_to_delete {
            let mut txn = reopened_store.begin().unwrap();
            assert!(txn.get(key).unwrap().is_none());
        }

        // Verify post-compaction keys are present
        for key in &post_compaction_keys {
            let mut txn = reopened_store.begin().unwrap();
            let val = txn.get(key).unwrap().unwrap();
            assert_eq!(val, default_value.as_ref());
        }

        reopened_store.close().unwrap();
    }

    #[test]
    fn compaction_with_overlapping_keys() {
        let temp_dir = create_temp_directory();

        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_value_threshold = 0;
        opts.max_value_cache_size = 0;

        let store = Store::new(opts.clone()).expect("should create store");

        let num_keys = 50;
        let mut keys: Vec<Bytes> = Vec::new();
        let mut overlapping_keys: Vec<Bytes> = Vec::new();

        // Generate random keys
        for _ in 0..num_keys {
            let key: String = rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(10)
                .map(char::from)
                .collect();
            keys.push(Bytes::from(key));
        }

        // Select half of the keys to be overlapping
        overlapping_keys.extend_from_slice(&keys[0..(num_keys / 2)]);

        let default_value = Bytes::from("default_value".to_string());
        let overlapping_value = Bytes::from("overlapping_value".to_string());

        // Write initial values
        for key in &keys {
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().unwrap();
        }

        // Write overlapping values
        for key in &overlapping_keys {
            let mut txn = store.begin().unwrap();
            txn.set(key, &overlapping_value).unwrap();
            txn.commit().unwrap();
        }

        // Trigger compaction
        store.compact().unwrap();

        store.close().unwrap();

        // Reopen the store to verify persistence of all keys
        let reopened_store = Store::new(opts).expect("should reopen store");

        // Verify initial keys have default value or overlapping value
        for key in &keys {
            let mut txn = reopened_store.begin().unwrap();
            let val = txn.get(key).unwrap().unwrap();
            if overlapping_keys.contains(key) {
                assert_eq!(val, overlapping_value.as_ref());
            } else {
                assert_eq!(val, default_value.as_ref());
            }
        }

        reopened_store.close().unwrap();
    }

    #[test]
    fn compaction_with_all_keys_deleted() {
        let temp_dir = create_temp_directory();

        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_value_threshold = 0;
        opts.max_value_cache_size = 0;

        let store = Store::new(opts.clone()).expect("Failed to create store");

        let num_keys = 50;
        let keys: Vec<Bytes> = (0..num_keys)
            .map(|_| {
                let key: String = rand::thread_rng()
                    .sample_iter(&rand::distributions::Alphanumeric)
                    .take(10)
                    .map(char::from)
                    .collect();
                Bytes::from(key)
            })
            .collect();

        let value = Bytes::from("some_value");

        // Write keys
        for key in &keys {
            let mut txn = store.begin().expect("Failed to begin transaction");
            txn.set(key, &value).expect("Failed to set key");
            txn.commit().expect("Failed to commit transaction");
        }

        // Delete all keys
        for key in &keys {
            let mut txn = store
                .begin()
                .expect("Failed to begin transaction for deletion");
            txn.delete(key).expect("Failed to delete key");
            txn.commit().expect("Failed to commit deletion");
        }

        // Trigger compaction
        store.compact().expect("Failed to compact");

        store.close().expect("Failed to close store");

        // Reopen the store to verify all keys are deleted
        let reopened_store = Store::new(opts).expect("Failed to reopen store");

        for key in &keys {
            let mut txn = reopened_store
                .begin()
                .expect("Failed to begin transaction on reopened store");
            assert!(
                txn.get(key).expect("Failed to get key").is_none(),
                "Key {key:?} was not deleted"
            );
        }

        reopened_store
            .close()
            .expect("Failed to close reopened store");
    }

    #[test]
    fn compaction_with_sequential_writes_and_deletes() {
        let temp_dir = create_temp_directory();

        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_value_threshold = 0;
        opts.max_value_cache_size = 0;

        let store = Store::new(opts.clone()).expect("Failed to create store");

        let num_keys = 26;
        // Sequentially generate keys
        let keys: Vec<Bytes> = (0..num_keys)
            .map(|i| Bytes::from(format!("key{i:02}")))
            .collect();

        let value = Bytes::from("value");

        // Sequentially write keys
        for key in &keys {
            let mut txn = store.begin().expect("Failed to begin transaction");
            txn.set(key, &value).expect("Failed to set key");
            txn.commit().expect("Failed to commit transaction");
        }

        // Sequentially delete half of the keys
        for key in &keys[..(num_keys / 2)] {
            let mut txn = store
                .begin()
                .expect("Failed to begin transaction for deletion");
            txn.delete(key).expect("Failed to delete key");
            txn.commit().expect("Failed to commit deletion");
        }

        // Trigger compaction
        store.compact().expect("Failed to compact");

        store.close().expect("Failed to close store");

        // Reopen the store to verify the state of keys
        let reopened_store = Store::new(opts).expect("Failed to reopen store");

        // Verify the first half of the keys are deleted and the second half still exist
        for (i, key) in keys.iter().enumerate() {
            let mut txn = reopened_store
                .begin()
                .expect("Failed to begin transaction on reopened store");
            let result = txn.get(key).expect("Failed to get key");
            if i < num_keys / 2 {
                assert!(result.is_none(), "Key {key:?} should have been deleted");
            } else {
                assert!(result.is_some(), "Key {key:?} should exist");
            }
        }

        reopened_store
            .close()
            .expect("Failed to close reopened store");
    }

    #[test]
    fn compaction_with_random_reads_and_writes() {
        let temp_dir = create_temp_directory();

        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_value_threshold = 0;
        opts.max_value_cache_size = 0;

        let store = Store::new(opts.clone()).expect("Failed to create store");

        let num_keys = 100;
        let mut rng = rand::thread_rng();

        // Generate random keys and values
        let keys_and_values: Vec<(Bytes, Bytes)> = (0..num_keys)
            .map(|_| {
                let key: String = (0..10)
                    .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
                    .collect();
                let value: String = (0..10)
                    .map(|_| rng.sample(rand::distributions::Alphanumeric) as char)
                    .collect();
                (Bytes::from(key), Bytes::from(value))
            })
            .collect();

        // Randomly write or delete keys
        for (key, value) in &keys_and_values {
            if rng.gen_bool(0.5) {
                // 50% chance
                let mut txn = store.begin().expect("Failed to begin transaction");
                txn.set(key, value).expect("Failed to set key");
                txn.commit().expect("Failed to commit transaction");
            } else {
                let mut txn = store
                    .begin()
                    .expect("Failed to begin transaction for deletion");
                txn.delete(key).expect("Failed to delete key");
                txn.commit().expect("Failed to commit deletion");
            }
        }

        // Trigger compaction
        store.compact().expect("Failed to compact");

        store.close().expect("Failed to close store");

        // Reopen the store to verify the state of keys
        let reopened_store = Store::new(opts).expect("Failed to reopen store");

        // Randomly read keys to verify their integrity
        for (key, expected_value) in &keys_and_values {
            let mut txn = reopened_store
                .begin()
                .expect("Failed to begin transaction on reopened store");
            if let Some(value) = txn.get(key).expect("Failed to get key") {
                assert_eq!(&value, expected_value, "Value mismatch for key {key:?}")
            }
        }

        reopened_store
            .close()
            .expect("Failed to close reopened store");
    }

    #[test]
    fn insert_close_reopen_single_bulk_transaction() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        let num_ops = 10;

        for i in 1..=2 {
            // (Re)open the store
            let store = Store::new(opts.clone()).expect("should create store");

            // Append num_ops items to the store
            let mut txn = store.begin().unwrap();
            for j in 0..num_ops {
                let id = (i - 1) * num_ops + j;
                let key = format!("key{id}");
                let value = format!("value{id}");
                txn.set(key.as_bytes(), value.as_bytes()).unwrap();
            }
            txn.commit().unwrap();

            // Test that the items are still in the store
            for j in 0..(num_ops * i) {
                let key = format!("key{j}");
                let value = format!("value{j}");
                let value = value.into_bytes();
                let mut txn = store.begin().unwrap();
                let val = txn.get(key.as_bytes()).unwrap().unwrap();

                assert_eq!(val, value);
            }

            // Close the store again
            store.close().unwrap();
        }
    }

    #[test]
    fn compact_skips_all_versions_if_last_is_deleted() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.compaction_discard_threshold = 30.0; // Lower threshold for testing

        // Initialize the store
        let store = Store::new(opts.clone()).expect("should create store");

        // Create enough entries and deletions to exceed 30% discard threshold by bytes
        // We'll create multiple entries and delete most of them

        {
            let mut txn = store.begin().unwrap();
            // Insert multiple keys
            for i in 1..=10 {
                let key = format!("key{i}").as_bytes().to_vec();
                let value = format!("value{i}").as_bytes().to_vec();
                txn.set(&key, &value).unwrap();
            }
            txn.commit().unwrap();
        }

        {
            let mut txn = store.begin().unwrap();
            // Delete most of the keys (8 out of 10) to exceed 30% threshold
            for i in 1..=8 {
                let key = format!("key{i}").as_bytes().to_vec();
                txn.delete(&key).unwrap();
            }
            txn.commit().unwrap();
        }

        // Now we have: key1-key8 (deleted), key9-key10 (live)
        // That should be > 30% deletion rate by bytes

        // Perform compaction
        store.compact().expect("compaction should succeed");
        let stats = &store.stats;
        assert_eq!(stats.compaction_stats.get_records_deleted(), 16); // 8 original entries + 8 delete tombstones = 16 deleted records encountered
        assert_eq!(stats.compaction_stats.get_records_added(), 2); // key9 and key10 should remain

        // Reopen the store to ensure compaction changes are applied
        drop(store);
        let store = Store::new(opts).expect("should reopen store");

        // Begin a new transaction to verify compaction results
        let mut txn = store.begin().unwrap();

        // Check that deleted keys are not present
        for i in 1..=8 {
            let key = format!("key{i}").as_bytes().to_vec();
            assert!(
                txn.get(&key).unwrap().is_none(),
                "key{i} should be skipped by compaction"
            );
        }

        // Check that remaining keys are still present
        for i in 9..=10 {
            let key = format!("key{i}").as_bytes().to_vec();
            let expected_value = format!("value{i}").as_bytes().to_vec();
            let val = txn
                .get(&key)
                .unwrap()
                .unwrap_or_else(|| panic!("key{i} should exist after compaction"));
            assert_eq!(
                val, expected_value,
                "key{i}'s value should remain unchanged after compaction"
            );
        }

        // Close the store
        store.close().unwrap();
    }

    #[test]
    fn multiple_versions_stored_post_compaction() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.compaction_discard_threshold = 1.0; // Set low threshold to ensure compaction triggers

        // Initialize the store
        let store = Store::new(opts.clone()).expect("should create store");

        // Open a transaction to populate the store with multiple versions of a key
        // Insert multiple versions for the same key
        {
            let mut txn = store.begin().unwrap();
            txn.set(b"key1", b"value1").unwrap(); // First version
            txn.commit().unwrap();
        }

        {
            let mut txn = store.begin().unwrap();
            txn.set(b"key1", b"value2").unwrap(); // Second version
            txn.commit().unwrap();
        }

        // Add some keys that will be deleted to trigger compaction
        for i in 2..=5 {
            let key = format!("key{i}").as_bytes().to_vec();
            let value = format!("value{i}").as_bytes().to_vec();

            let mut txn = store.begin().unwrap();
            txn.set(&key, &value).unwrap();
            txn.commit().unwrap();
        }

        // Delete the keys to trigger compaction
        for i in 2..=5 {
            let key = format!("key{i}").as_bytes().to_vec();

            let mut txn = store.begin().unwrap();
            txn.delete(&key).unwrap();
            txn.commit().unwrap();
        }

        // Perform compaction
        store.compact().expect("compaction should succeed");
        let stats = &store.stats;
        assert_eq!(stats.compaction_stats.get_records_added(), 2);

        // Reopen the store to ensure compaction changes are applied
        drop(store);
        let store = Store::new(opts).expect("should reopen store");

        // Begin a new transaction to verify compaction results
        let mut txn = store.begin().unwrap();

        // Check that "key1" is present and its value is the last inserted value
        let val = txn
            .get(b"key1")
            .unwrap()
            .expect("key1 should exist after compaction");
        assert_eq!(
            val, b"value2",
            "key1's value should be the last version after compaction"
        );

        // Close the store
        store.close().unwrap();
    }

    #[test]
    fn multiple_versions_and_single_version_keys_post_compaction() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.compaction_discard_threshold = 1.0;

        let store = Store::new(opts.clone()).expect("should create store");

        let num_keys = 100; // Total number of keys
        let multiple_versions_threshold = 50; // Keys above this index will have multiple versions
        let num_keys_to_delete = 10; // Number of keys to delete to trigger compaction

        // Insert keys into the store:
        // - First 50 keys (1-50) have a single version
        // - Next 50 keys (51-100) have two versions each
        for key_index in 1..=num_keys {
            let key = format!("key{key_index}").as_bytes().to_vec();
            {
                let mut txn = store.begin().unwrap();
                let value1 = format!("value{key_index}_1").as_bytes().to_vec();

                // Insert first version for all keys
                txn.set(&key, &value1).unwrap();
                txn.commit().unwrap();
            }

            if key_index > multiple_versions_threshold {
                let value2 = format!("value{key_index}_2").as_bytes().to_vec();
                {
                    let mut txn = store.begin().unwrap();
                    // Insert a second version for keys above the multiple_versions_threshold
                    txn.set(&key, &value2).unwrap();
                    txn.commit().unwrap();
                }
            }
        }

        // Delete some keys to trigger compaction threshold
        for key_index in 1..=num_keys_to_delete {
            let key = format!("key{key_index}").as_bytes().to_vec();
            let mut txn = store.begin().unwrap();
            txn.delete(&key).unwrap();
            txn.commit().unwrap();
        }

        // Perform compaction
        store.compact().expect("compaction should succeed");

        // Verify compaction stats:
        // - Original records: 150 (50 single-version + 50*2 double-version)
        // - Deleted records: 10 keys
        // - Expected remaining records: 140
        let stats = &store.stats;
        assert_eq!(stats.compaction_stats.get_records_added(), 140);

        // Reopen the store to ensure changes persist
        drop(store);
        let store = Store::new(opts).expect("should reopen store");

        // Verify non-deleted keys (11-100) still exist with correct values
        for key_index in (num_keys_to_delete + 1)..=num_keys {
            let mut txn = store.begin().unwrap();
            let key = format!("key{key_index}").as_bytes().to_vec();
            let expected_value = if key_index > multiple_versions_threshold {
                format!("value{key_index}_2")
            } else {
                format!("value{key_index}_1")
            };

            let val = txn
                .get(&key)
                .unwrap()
                .expect("key should exist after compaction");
            assert_eq!(
                val,
                expected_value.as_bytes(),
                "key's value should be the expected version after compaction"
            );
        }

        // Verify deleted keys (1-10) are gone
        for key_index in 1..=num_keys_to_delete {
            let mut txn = store.begin().unwrap();
            let key = format!("key{key_index}").as_bytes().to_vec();
            assert!(
                txn.get(&key).unwrap().is_none(),
                "deleted key should not exist after compaction"
            );
        }

        // Close the store
        store.close().unwrap();
    }

    #[test]
    fn compact_handles_various_key_versions_correctly() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.compaction_discard_threshold = 1.0; // Set low threshold to ensure compaction triggers

        let store = Store::new(opts.clone()).expect("should create store");

        let num_keys = 100; // Total number of keys
        let multiple_versions_threshold = 50; // Keys above this index will have multiple versions
        let delete_threshold = 75; // Keys above this index will be marked as deleted in their last version

        // Add a few keys that will be deleted to ensure compaction threshold is met
        for i in 101..=110 {
            let key = format!("delete_me_key{i}").as_bytes().to_vec();
            let value = format!("temp_value{i}").as_bytes().to_vec();

            let mut txn = store.begin().unwrap();
            txn.set(&key, &value).unwrap();
            txn.commit().unwrap();
        }

        // Insert keys into the store with each operation in its own transaction
        for key_index in 1..=num_keys {
            let key = format!("key{key_index}").as_bytes().to_vec();
            let value1 = format!("value{key_index}_1").as_bytes().to_vec();

            // Insert first version for all keys in its own transaction
            {
                let mut txn = store.begin().unwrap();
                txn.set(&key, &value1).unwrap();
                txn.commit().unwrap();
            }

            // Insert a second version for keys above the multiple_versions_threshold in its own transaction
            if key_index > multiple_versions_threshold {
                let value2 = format!("value{key_index}_2").as_bytes().to_vec();
                {
                    let mut txn = store.begin().unwrap();
                    txn.set(&key, &value2).unwrap();
                    txn.commit().unwrap();
                }

                // Mark the last version as deleted for keys above the delete_threshold in its own transaction
                if key_index > delete_threshold {
                    let mut txn = store.begin().unwrap();
                    txn.delete(&key).unwrap();
                    txn.commit().unwrap();
                }
            }
        }

        // Delete the temporary keys to trigger compaction threshold
        for i in 101..=110 {
            let key = format!("delete_me_key{i}").as_bytes().to_vec();

            let mut txn = store.begin().unwrap();
            txn.delete(&key).unwrap();
            txn.commit().unwrap();
        }

        // Perform compaction
        store.compact().expect("compaction should succeed");
        let stats = &store.stats;
        assert_eq!(stats.compaction_stats.get_records_added(), 100);

        // Reopen the store
        drop(store);
        let store = Store::new(opts).expect("should reopen store");

        // Verify the results
        for key_index in 1..=num_keys {
            let mut txn = store.begin().unwrap();
            let key = format!("key{key_index}").as_bytes().to_vec();

            if key_index > delete_threshold {
                // Keys marked as deleted in their last version should not be present
                assert!(
                    txn.get(&key).unwrap().is_none(),
                    "Deleted key{key_index} should not be present after compaction"
                );
            } else if key_index > multiple_versions_threshold {
                // Keys with multiple versions should have their last version
                let expected_value = format!("value{key_index}_2");
                let val = txn
                    .get(&key)
                    .unwrap()
                    .expect("key should exist after compaction");
                assert_eq!(
                    val,
                    expected_value.as_bytes(),
                    "key{key_index}'s value should be the last version after compaction"
                );
            } else {
                // Keys with a single version should remain unchanged
                let expected_value = format!("value{key_index}_1");
                let val = txn
                    .get(&key)
                    .unwrap()
                    .expect("key should exist after compaction");
                assert_eq!(
                    val,
                    expected_value.as_bytes(),
                    "key{key_index}'s value should remain unchanged after compaction"
                );
            }
        }

        // Close the store
        store.close().unwrap();
    }

    #[test]
    fn compact_handles_various_key_versions_correctly_with_clear() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.compaction_discard_threshold = 1.0;

        let store = Store::new(opts.clone()).expect("should create store");

        let num_keys = 100; // Total number of keys
        let multiple_versions_threshold = 50; // Keys above this index will have multiple versions
        let clear_threshold = 75; // Keys above this index will be marked as deleted in their last version

        // Add a few keys that will be deleted to ensure compaction threshold is met
        for i in 101..=110 {
            let key = format!("delete_me_key{i}").as_bytes().to_vec();
            let value = format!("temp_value{i}").as_bytes().to_vec();

            let mut txn = store.begin().unwrap();
            txn.set(&key, &value).unwrap();
            txn.commit().unwrap();
        }

        // Insert keys into the store with each operation in its own transaction
        for key_index in 1..=num_keys {
            let key = format!("key{key_index}").as_bytes().to_vec();
            let value1 = format!("value{key_index}_1").as_bytes().to_vec();

            // Insert first version for all keys in its own transaction
            {
                let mut txn = store.begin().unwrap();
                txn.set(&key, &value1).unwrap();
                txn.commit().unwrap();
            }

            // Insert a second version for keys above the multiple_versions_threshold in its own transaction
            if key_index > multiple_versions_threshold {
                let value2 = format!("value{key_index}_2").as_bytes().to_vec();
                {
                    let mut txn = store.begin().unwrap();
                    txn.set(&key, &value2).unwrap();
                    txn.commit().unwrap();
                }

                // Mark the last version as deleted for keys above the clear_threshold in its own transaction
                if key_index > clear_threshold {
                    let mut txn = store.begin().unwrap();
                    txn.soft_delete(&key).unwrap();
                    txn.commit().unwrap();
                }
            }
        }

        // Delete the temporary keys to trigger compaction threshold
        for i in 101..=110 {
            let key = format!("delete_me_key{i}").as_bytes().to_vec();

            let mut txn = store.begin().unwrap();
            txn.delete(&key).unwrap();
            txn.commit().unwrap();
        }

        // Perform compaction
        store.compact().expect("compaction should succeed");
        let stats = &store.stats;
        assert_eq!(stats.compaction_stats.get_records_added(), 175);

        // Reopen the store
        drop(store);
        let store = Store::new(opts).expect("should reopen store");

        for key_index in 1..=num_keys {
            let mut txn = store.begin().unwrap();
            let key = format!("key{key_index}").as_bytes().to_vec();

            if key_index > clear_threshold {
                // Keys marked as deleted in their last version should not be present
                assert!(
                    txn.get(&key).unwrap().is_none(),
                    "Deleted key{key_index} should not be present after compaction"
                );
            }
        }

        // Verify the results
        for key_index in 1..=num_keys {
            let mut txn = store.begin().unwrap();
            let key = format!("key{key_index}").as_bytes().to_vec();

            if key_index > clear_threshold {
                // Keys marked as cleared in their last version should not be present
                assert!(
                    txn.get(&key).unwrap().is_none(),
                    "Cleared key{key_index} should not be present after compaction"
                );
            } else if key_index > multiple_versions_threshold {
                // Keys with multiple versions should have their last version
                let expected_value = format!("value{key_index}_2");
                let val = txn
                    .get(&key)
                    .unwrap()
                    .expect("key should exist after compaction");
                assert_eq!(
                    val,
                    expected_value.as_bytes(),
                    "key{key_index}'s value should be the last version after compaction"
                );
            } else {
                // Keys with a single version should remain unchanged
                let expected_value = format!("value{key_index}_1");
                let val = txn
                    .get(&key)
                    .unwrap()
                    .expect("key should exist after compaction");
                assert_eq!(
                    val,
                    expected_value.as_bytes(),
                    "key{key_index}'s value should remain unchanged after compaction"
                );
            }
        }

        // Close the store
        store.close().unwrap();
    }

    #[test]
    fn compaction_preserves_key_deletion() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_value_threshold = 0;
        opts.max_value_cache_size = 0;

        // Create a new store instance with VariableKey as the key type
        let store = Store::new(opts.clone()).expect("should create store");

        // Number of keys to generate and write
        let num_keys_to_write = 1;

        // Create a vector to store the generated keys
        let mut keys: Vec<Bytes> = Vec::new();

        for counter in 1usize..=num_keys_to_write {
            // Convert the counter to Bytes
            let key_bytes = Bytes::from(counter.to_le_bytes().to_vec());

            // Add the key to the vector
            keys.push(key_bytes);
        }

        let default_value = Bytes::from("default_value".to_string());
        let default_value2 = Bytes::from("default_value2".to_string());

        // Write the keys to the store
        for key in keys.iter() {
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().unwrap();
        }

        for key in keys.iter() {
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value2).unwrap();
            txn.commit().unwrap();
        }

        let key_bytes = Bytes::from(2usize.to_le_bytes().to_vec());
        let mut txn = store.begin().unwrap();
        txn.set(&key_bytes, &default_value2).unwrap();
        txn.commit().unwrap();

        // Delete the first 5 keys from the store
        for key in keys.iter() {
            let mut txn = store.begin().unwrap();
            txn.delete(key).unwrap();
            txn.commit().unwrap();
        }

        store.compact().unwrap();
        store.close().unwrap();

        let reopened_store = Store::new(opts).expect("should reopen store");
        for key in keys.iter() {
            let mut txn = reopened_store.begin().unwrap();
            assert!(txn.get(key).unwrap().is_none());
        }
    }
}
