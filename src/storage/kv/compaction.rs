use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::BytesMut;

use crate::storage::{
    kv::{
        entry::{Entry, Record, Value, ValueRef},
        error::{Error, Result},
        manifest::Manifest,
        meta::Metadata,
        option::Options,
        store::{Core, StoreInner},
    },
    log::{Aol, Options as LogOptions, SegmentRef},
};

struct CompactionGuard<'a> {
    is_compacting: &'a AtomicBool,
}

impl<'a> CompactionGuard<'a> {
    fn new(is_compacting: &'a AtomicBool) -> Self {
        is_compacting.store(true, Ordering::Relaxed);
        CompactionGuard { is_compacting }
    }
}

impl<'a> Drop for CompactionGuard<'a> {
    fn drop(&mut self) {
        self.is_compacting.store(false, Ordering::Relaxed);
    }
}

impl StoreInner {
    pub async fn compact(&self) -> Result<()> {
        // Early return if the store is closed or compaction is already in progress
        if self.is_closed.load(Ordering::SeqCst) || !self.core.opts.should_persist_data() {
            return Err(Error::InvalidOperation);
        }

        // Check if compaction is already in progress
        if self.is_compacting.load(Ordering::SeqCst) {
            return Err(Error::CompactionAlreadyInProgress);
        }

        // Clear files before starting compaction if a .merge or .tmp.merge directory exists
        let tmp_merge_dir = self.core.opts.dir.join(".tmp.merge");
        if tmp_merge_dir.exists() {
            fs::remove_dir_all(&tmp_merge_dir)?;
        }

        let merge_dir = self.core.opts.dir.join(".merge");
        if merge_dir.exists() {
            fs::remove_dir_all(&merge_dir)?;
        }

        // Clean recovery state before starting compaction
        RecoveryState::clear(&self.core.opts.dir)?;

        // Clear compaction stats before starting compaction
        self.stats.compaction_stats.reset();

        // Acquire compaction guard
        let _guard = CompactionGuard::new(&self.is_compacting);

        // Lock the oracle to prevent operations during compaction
        let oracle = self.core.oracle.clone();
        let oracle_lock = oracle.write_lock.lock().await;

        // Rotate the commit log and get the new segment ID
        let mut clog = self.core.clog.as_ref().unwrap().write();
        let new_segment_id = clog.rotate()?;
        let last_updated_segment_id = new_segment_id - 1;
        drop(clog); // Explicitly drop the lock

        // Create a temporary directory for compaction
        fs::create_dir_all(&tmp_merge_dir)?;

        // Initialize a new manifest in the temporary directory
        let mut manifest = Core::initialize_manifest(&tmp_merge_dir)?;
        // Add the last updated segment ID to the manifest
        let changeset = Manifest::with_compacted_up_to_segment(last_updated_segment_id);
        manifest.append(&changeset.serialize()?)?;
        manifest.close()?;

        // Prepare a temporary commit log directory
        let temp_clog_dir = tmp_merge_dir.join("clog");
        let tm_opts = LogOptions::default()
            .with_max_file_size(self.core.opts.max_compaction_segment_size)
            .with_file_extension("clog".to_string());
        let mut temp_writer = Aol::open(&temp_clog_dir, &tm_opts)?;

        // TODO: Check later to add a new way for compaction by reading from the files first and then
        // check in files for the keys that are not found in memory to handle deletion

        // Start compaction process
        let snapshot_lock = self.core.indexer.write();
        let snapshot = snapshot_lock.snapshot();
        let snapshot_versioned_iter = snapshot.iter_with_versions();
        drop(snapshot_lock); // Explicitly drop the lock
        drop(oracle_lock); // Release the oracle lock

        // Do compaction and write

        // Define a closure for writing entries to the temporary commit log
        let mut write_entry = |key: Vec<u8>,
                               value: Vec<u8>,
                               version: u64,
                               ts: u64,
                               metadata: Option<Metadata>|
         -> Result<()> {
            let mut key = key;
            key.truncate(key.len() - 1);
            let mut entry = Entry::new(&key, &value);
            entry.set_ts(ts);

            if let Some(md) = metadata {
                entry.set_metadata(md);
            }

            let tx_record = Record::from_entry(entry, version);
            let mut buf = BytesMut::new();
            tx_record.encode(&mut buf)?;

            let (segment_id, _, _) = temp_writer.append(&buf)?;
            if segment_id > last_updated_segment_id {
                eprintln!(
                    "Segment ID: {} exceeds last updated segment ID: {}",
                    segment_id, last_updated_segment_id
                );
                return Err(Error::SegmentIdExceedsLastUpdated);
            }

            // increment the compaction stats
            self.stats.compaction_stats.add_record();

            Ok(())
        };

        let mut current_key: Option<Vec<u8>> = None;
        let mut entries_buffer = Vec::new();
        let mut skip_current_key = false;

        for (key, value, version, ts) in snapshot_versioned_iter {
            let mut val_ref = ValueRef::new(self.core.clone());
            val_ref.decode(*version, value)?;

            // IMP!!! What happnes to keys with the swizzle bit set to 1?

            // Skip keys from segments newer than the last updated segment
            if val_ref.segment_id() > last_updated_segment_id {
                continue;
            }

            let metadata = val_ref.metadata();

            // If we've moved to a new key, decide whether to write the previous key's entries
            if Some(&key) != current_key.as_ref() {
                if !skip_current_key {
                    // Write buffered entries of the previous key to disk
                    for (key, value, version, ts, metadata) in entries_buffer.drain(..) {
                        write_entry(key, value, version, ts, metadata)?;
                    }
                } else {
                    // Clear the buffer without writing if the last version was marked as deleted
                    entries_buffer.clear();
                }

                // Reset flags for the new key
                current_key = Some(key.clone());
                skip_current_key = false;
            }

            // Determine if the current key should be skipped based on deletion status
            if let Some(md) = metadata {
                if md.is_deleted() {
                    skip_current_key = true;

                    let num_records_deleted = 1 + entries_buffer.len() as u64;
                    self.stats
                        .compaction_stats
                        .add_multiple_deleted_records(num_records_deleted);

                    entries_buffer.clear(); // Clear any previously buffered entries for this key
                    continue;
                }
            }

            // Buffer the current entry if not skipping
            if !skip_current_key {
                entries_buffer.push((key, val_ref.resolve()?, *version, *ts, metadata.cloned()));
            }
        }

        // Handle the last key
        if !skip_current_key {
            for (key, value, version, ts, metadata) in entries_buffer.drain(..) {
                write_entry(key, value, version, ts, metadata)?;
            }
        }

        temp_writer.close()?;

        // Finalize compaction by renaming the temporary directory
        fs::rename(tmp_merge_dir, merge_dir)?;

        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum RecoveryState {
    None,
    ClogDeleted,
}

impl RecoveryState {
    pub(crate) fn load(dir: &Path) -> Result<Self> {
        let path = dir.join(".recovery_state");
        if path.exists() {
            let mut file = File::open(path)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            match contents.as_str() {
                "ClogDeleted" => Ok(RecoveryState::ClogDeleted),
                _ => Ok(RecoveryState::None),
            }
        } else {
            Ok(RecoveryState::None)
        }
    }

    pub(crate) fn save(&self, dir: &Path) -> Result<()> {
        let path = dir.join(".recovery_state");
        let mut file = File::create(path)?;
        match self {
            RecoveryState::ClogDeleted => {
                write!(file, "ClogDeleted")
                    .map_err(|e| Error::CustomError(format!("recovery file write error: {}", e)))?;
                Ok(())
            }
            RecoveryState::None => Ok(()),
        }
    }

    pub(crate) fn clear(dir: &Path) -> Result<()> {
        let path = dir.join(".recovery_state");
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }
}

fn perform_recovery(opts: &Options) -> Result<()> {
    // Encapsulate operations in a closure for easier rollback
    let result = || -> Result<()> {
        let merge_dir = opts.dir.join(".merge");
        let clog_dir = opts.dir.join("clog");
        let merge_clog_subdir = merge_dir.join("clog");

        // If there is a .merge directory, try reading manifest from it
        let manifest = Core::initialize_manifest(&merge_dir)?;
        let existing_manifest = if manifest.size()? > 0 {
            Core::read_manifest(&merge_dir)?
        } else {
            return Err(Error::MergeManifestMissing);
        };

        let compacted_upto_segments = existing_manifest.extract_compacted_up_to_segments();
        if compacted_upto_segments.is_empty() || compacted_upto_segments.len() > 1 {
            return Err(Error::MergeManifestMissing);
        }

        let compacted_upto_segment_id = compacted_upto_segments[0];
        let segs = SegmentRef::read_segments_from_directory(&clog_dir)?;
        // Step 4: Copy files from clog dir to merge clog dir
        for seg in segs.iter() {
            if seg.id > compacted_upto_segment_id {
                // Check if the path points to a regular file
                match fs::metadata(&seg.file_path) {
                    Ok(metadata) => {
                        if metadata.is_file() {
                            // Proceed to copy the file
                            let dest_path =
                                merge_clog_subdir.join(seg.file_path.file_name().unwrap());
                            match fs::copy(&seg.file_path, &dest_path) {
                                Ok(_) => println!("File copied successfully"),
                                Err(e) => {
                                    println!("Error copying file: {:?}", e);
                                    return Err(Error::from(e));
                                }
                            }
                        } else {
                            println!("Path is not a regular file: {:?}", &seg.file_path);
                        }
                    }
                    Err(e) => {
                        println!("Error accessing file metadata: {:?}", e);
                        return Err(Error::from(e));
                    }
                }
            }
        }

        // Clear any previous recovery state before setting a new one
        RecoveryState::clear(&opts.dir)?;
        // After successful operation, update recovery state to indicate clog can be deleted
        RecoveryState::ClogDeleted.save(&opts.dir)?;

        // Delete the `clog` directory
        if let Err(e) = fs::remove_dir_all(&clog_dir) {
            println!("Error deleting clog directory: {:?}", e);
            return Err(Error::from(e));
        }

        // Rename `merge_clog_subdir` to `clog`
        if let Err(e) = fs::rename(&merge_clog_subdir, &clog_dir) {
            println!("Error renaming merge_clog_subdir to clog: {:?}", e);
            return Err(Error::from(e));
        }

        // Clear recovery state after successful completion
        RecoveryState::clear(&opts.dir)?;

        Ok(())
    };

    match result() {
        Ok(_) => Ok(()),
        Err(e) => {
            let merge_dir = opts.dir.join(".merge");
            let clog_dir = opts.dir.join("clog");
            rollback(&merge_dir, &clog_dir, RecoveryState::load(&opts.dir)?)?;
            Err(e)
        }
    }
}

fn cleanup_after_recovery(opts: &Options) -> Result<()> {
    let merge_dir = opts.dir.join(".merge");

    if merge_dir.exists() {
        fs::remove_dir_all(&merge_dir)?;
    }
    Ok(())
}

fn rollback(merge_dir: &Path, clog_dir: &Path, checkpoint: RecoveryState) -> Result<()> {
    if checkpoint == RecoveryState::ClogDeleted {
        // Restore the clog directory from merge directory if it exists
        // At this point the merge directory should exist and the clog directory should not
        // So, we can safely rename the merge clog directory to clog directory
        if !clog_dir.exists() && merge_dir.exists() {
            let merge_clog_subdir = merge_dir.join("clog");
            if merge_clog_subdir.exists() {
                fs::rename(&merge_clog_subdir, clog_dir)?;
            }
        }
    }

    Ok(())
}

fn needs_recovery(opts: &Options) -> Result<bool> {
    Ok(opts.dir.join(".merge").exists())
}

fn handle_clog_deleted_state(opts: &Options) -> Result<()> {
    let merge_dir = opts.dir.join(".merge");
    let clog_dir = opts.dir.join("clog");
    rollback(&merge_dir, &clog_dir, RecoveryState::ClogDeleted)
}

/// Restores the store from a compaction process by handling .tmp.merge and .merge directories.
/// TODO: This should happen post repair
pub fn restore_from_compaction(opts: &Options) -> Result<()> {
    let tmp_merge_dir = opts.dir.join(".tmp.merge");
    // 1) Check if there is a .tmp.merge directory, delete it
    if tmp_merge_dir.exists() {
        // This means there was a previous compaction process that failed
        // so we don't need to do anything here and just return
        fs::remove_dir_all(&tmp_merge_dir)?;
        return Ok(());
    }

    if !needs_recovery(opts)? {
        return Ok(());
    }

    match RecoveryState::load(&opts.dir)? {
        RecoveryState::ClogDeleted => handle_clog_deleted_state(opts)?,
        RecoveryState::None => (),
    }

    perform_recovery(opts)?;
    // Clean up merge directory after successful operation
    cleanup_after_recovery(opts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::prelude::SliceRandom;
    use rand::{distributions::Alphanumeric, Rng};
    use std::collections::HashSet;
    use std::fs::read_to_string;

    use crate::storage::kv::option::Options;
    use crate::storage::kv::store::Store;

    use bytes::Bytes;
    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    #[test]
    fn test_recovery_state_sequentially() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();
        let temp_dir = temp_dir.path();
        let path = temp_dir.join(".recovery_state");
        let temp_dir = &temp_dir.to_path_buf();

        // Clear state and re-setup for next test
        RecoveryState::clear(temp_dir).unwrap();
        assert!(!path.exists());

        // Test saving and loading ClogDeleted
        RecoveryState::ClogDeleted.save(temp_dir).unwrap();
        assert_eq!(
            RecoveryState::load(temp_dir).unwrap(),
            RecoveryState::ClogDeleted
        );

        // Clear state and re-setup for next test
        RecoveryState::clear(temp_dir).unwrap();
        assert!(!path.exists());

        // Test loading None when no state is saved
        assert_eq!(RecoveryState::load(temp_dir).unwrap(), RecoveryState::None);

        // Test save contents for ClogDeleted
        RecoveryState::ClogDeleted.save(temp_dir).unwrap();
        let contents = read_to_string(&path).unwrap();
        assert_eq!(contents, "ClogDeleted");

        // Final clear to clean up
        RecoveryState::clear(temp_dir).unwrap();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn basic_compaction() {
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
            txn.commit().await.unwrap();
        }

        // Number of keys to delete
        let num_keys_to_delete = 5;

        // Delete the first 5 keys from the store
        for key in keys.iter().take(num_keys_to_delete) {
            let mut txn = store.begin().unwrap();
            txn.delete(key).unwrap();
            txn.commit().await.unwrap();
        }

        store.inner.as_ref().unwrap().compact().await.unwrap();
        store.close().await.unwrap();

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

    #[tokio::test]
    async fn compaction_with_mixed_operations() {
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
            txn.commit().await.unwrap();
        }

        // Update half of the keys
        for key in keys.iter().take(num_keys_to_write / 2) {
            let mut txn = store.begin().unwrap();
            txn.set(key, &updated_value).unwrap();
            txn.commit().await.unwrap();
        }

        // Delete a quarter of the keys
        let num_keys_to_delete = num_keys_to_write / 4;
        for key in keys.iter().take(num_keys_to_delete) {
            let mut txn = store.begin().unwrap();
            txn.delete(key).unwrap();
            txn.commit().await.unwrap();
        }

        store.inner.as_ref().unwrap().compact().await.unwrap();
        store.close().await.unwrap();

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

        reopened_store.close().await.unwrap();
    }

    #[tokio::test]
    async fn compaction_persistence_across_restarts() {
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
            txn.commit().await.unwrap();
        }

        store.inner.as_ref().unwrap().compact().await.unwrap();
        store.close().await.unwrap();

        // Reopen the store to verify persistence
        let reopened_store = Store::new(opts).expect("should reopen store");

        // Verify that all keys still exist with the correct value
        for key in keys.iter() {
            let mut txn = reopened_store.begin().unwrap();
            let val = txn.get(key).unwrap().unwrap();
            assert_eq!(val, default_value.as_ref());
        }

        reopened_store.close().await.unwrap();
    }

    #[tokio::test]
    async fn compaction_and_post_compaction_writes() {
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
            txn.commit().await.unwrap();
        }

        // Trigger compaction
        store.inner.as_ref().unwrap().compact().await.unwrap();

        // Write post-compaction values
        for key in post_compaction_keys.iter() {
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().await.unwrap();
        }

        store.close().await.unwrap();

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

        reopened_store.close().await.unwrap();
    }

    #[tokio::test]
    async fn compaction_with_random_keys_and_deletes() {
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
            txn.commit().await.unwrap();
        }

        // Trigger compaction
        store.inner.as_ref().unwrap().compact().await.unwrap();

        // Delete selected keys
        for key in &keys_to_delete {
            let mut txn = store.begin().unwrap();
            txn.delete(key).unwrap();
            txn.commit().await.unwrap();
        }

        // Write post-compaction values
        for key in &post_compaction_keys {
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().await.unwrap();
        }

        store.close().await.unwrap();

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

        reopened_store.close().await.unwrap();
    }

    #[tokio::test]
    async fn compaction_with_overlapping_keys() {
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
            txn.commit().await.unwrap();
        }

        // Write overlapping values
        for key in &overlapping_keys {
            let mut txn = store.begin().unwrap();
            txn.set(key, &overlapping_value).unwrap();
            txn.commit().await.unwrap();
        }

        // Trigger compaction
        store.inner.as_ref().unwrap().compact().await.unwrap();

        store.close().await.unwrap();

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

        reopened_store.close().await.unwrap();
    }

    #[tokio::test]
    async fn compaction_with_all_keys_deleted() {
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
            txn.commit().await.expect("Failed to commit transaction");
        }

        // Delete all keys
        for key in &keys {
            let mut txn = store
                .begin()
                .expect("Failed to begin transaction for deletion");
            txn.delete(key).expect("Failed to delete key");
            txn.commit().await.expect("Failed to commit deletion");
        }

        // Trigger compaction
        store
            .inner
            .as_ref()
            .expect("Store inner is None")
            .compact()
            .await
            .expect("Failed to compact");

        store.close().await.expect("Failed to close store");

        // Reopen the store to verify all keys are deleted
        let reopened_store = Store::new(opts).expect("Failed to reopen store");

        for key in &keys {
            let mut txn = reopened_store
                .begin()
                .expect("Failed to begin transaction on reopened store");
            assert!(
                txn.get(key).expect("Failed to get key").is_none(),
                "Key {:?} was not deleted",
                key
            );
        }

        reopened_store
            .close()
            .await
            .expect("Failed to close reopened store");
    }

    #[tokio::test]
    async fn compaction_with_sequential_writes_and_deletes() {
        let temp_dir = create_temp_directory();

        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_value_threshold = 0;
        opts.max_value_cache_size = 0;

        let store = Store::new(opts.clone()).expect("Failed to create store");

        let num_keys = 26;
        // Sequentially generate keys
        let keys: Vec<Bytes> = (0..num_keys)
            .map(|i| Bytes::from(format!("key{:02}", i)))
            .collect();

        let value = Bytes::from("value");

        // Sequentially write keys
        for key in &keys {
            let mut txn = store.begin().expect("Failed to begin transaction");
            txn.set(key, &value).expect("Failed to set key");
            txn.commit().await.expect("Failed to commit transaction");
        }

        // Sequentially delete half of the keys
        for key in &keys[..(num_keys / 2)] {
            let mut txn = store
                .begin()
                .expect("Failed to begin transaction for deletion");
            txn.delete(key).expect("Failed to delete key");
            txn.commit().await.expect("Failed to commit deletion");
        }

        // Trigger compaction
        store
            .inner
            .as_ref()
            .expect("Store inner is None")
            .compact()
            .await
            .expect("Failed to compact");

        store.close().await.expect("Failed to close store");

        // Reopen the store to verify the state of keys
        let reopened_store = Store::new(opts).expect("Failed to reopen store");

        // Verify the first half of the keys are deleted and the second half still exist
        for (i, key) in keys.iter().enumerate() {
            let mut txn = reopened_store
                .begin()
                .expect("Failed to begin transaction on reopened store");
            let result = txn.get(key).expect("Failed to get key");
            if i < num_keys / 2 {
                assert!(result.is_none(), "Key {:?} should have been deleted", key);
            } else {
                assert!(result.is_some(), "Key {:?} should exist", key);
            }
        }

        reopened_store
            .close()
            .await
            .expect("Failed to close reopened store");
    }

    #[tokio::test]
    async fn compaction_with_random_reads_and_writes() {
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
                txn.commit().await.expect("Failed to commit transaction");
            } else {
                let mut txn = store
                    .begin()
                    .expect("Failed to begin transaction for deletion");
                txn.delete(key).expect("Failed to delete key");
                txn.commit().await.expect("Failed to commit deletion");
            }
        }

        // Trigger compaction
        store
            .inner
            .as_ref()
            .expect("Store inner is None")
            .compact()
            .await
            .expect("Failed to compact");

        store.close().await.expect("Failed to close store");

        // Reopen the store to verify the state of keys
        let reopened_store = Store::new(opts).expect("Failed to reopen store");

        // Randomly read keys to verify their integrity
        for (key, expected_value) in &keys_and_values {
            let mut txn = reopened_store
                .begin()
                .expect("Failed to begin transaction on reopened store");
            if let Some(value) = txn.get(key).expect("Failed to get key") {
                assert_eq!(&value, expected_value, "Value mismatch for key {:?}", key)
            }
        }

        reopened_store
            .close()
            .await
            .expect("Failed to close reopened store");
    }

    #[tokio::test]
    async fn insert_close_reopen_single_bulk_transaction() {
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
                let key = format!("key{}", id);
                let value = format!("value{}", id);
                txn.set(key.as_bytes(), value.as_bytes()).unwrap();
            }
            txn.commit().await.unwrap();

            // Test that the items are still in the store
            for j in 0..(num_ops * i) {
                let key = format!("key{}", j);
                let value = format!("value{}", j);
                let value = value.into_bytes();
                let mut txn = store.begin().unwrap();
                let val = txn.get(key.as_bytes()).unwrap().unwrap();

                assert_eq!(val, value);
            }

            // Close the store again
            store.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn compact_skips_all_versions_if_last_is_deleted() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Initialize the store
        let store = Store::new(opts.clone()).expect("should create store");

        // Open a transaction to populate the store with versions of keys
        {
            let mut txn = store.begin().unwrap();
            // Insert a key with two versions, where the last one is marked as deleted
            txn.set(b"key1", b"value1").unwrap(); // First version
            txn.commit().await.unwrap();
        }

        {
            let mut txn = store.begin().unwrap();
            // Insert a key with two versions, where the last one is marked as deleted
            txn.delete(b"key1").unwrap(); // Second version marked as deleted
            txn.commit().await.unwrap();
        }

        {
            let mut txn = store.begin().unwrap();
            // Insert another key with a single version not marked as deleted
            txn.set(b"key2", b"value2").unwrap();
            txn.commit().await.unwrap();
        }

        // Perform compaction
        store.compact().await.expect("compaction should succeed");
        let stats = &store.inner.as_ref().unwrap().stats;
        assert_eq!(stats.compaction_stats.get_records_deleted(), 0);
        assert_eq!(stats.compaction_stats.get_records_added(), 1);

        // Reopen the store to ensure compaction changes are applied
        drop(store);
        let store = Store::new(opts).expect("should reopen store");

        // Begin a new transaction to verify compaction results
        let mut txn = store.begin().unwrap();

        // Check that "key1" is not present because its last version was marked as deleted
        assert!(
            txn.get(b"key1").unwrap().is_none(),
            "key1 should be skipped by compaction"
        );

        // Check that "key2" is still present because it was not marked as deleted
        let val = txn
            .get(b"key2")
            .unwrap()
            .expect("key2 should exist after compaction");
        assert_eq!(
            val, b"value2",
            "key2's value should remain unchanged after compaction"
        );

        // Close the store
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn multiple_versions_stored_post_compaction() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Initialize the store
        let store = Store::new(opts.clone()).expect("should create store");

        // Open a transaction to populate the store with multiple versions of a key
        // Insert multiple versions for the same key
        {
            let mut txn = store.begin().unwrap();
            txn.set(b"key1", b"value1").unwrap(); // First version
            txn.commit().await.unwrap();
        }

        {
            let mut txn = store.begin().unwrap();
            txn.set(b"key1", b"value2").unwrap(); // Second version
            txn.commit().await.unwrap();
        }

        // Perform compaction
        store.compact().await.expect("compaction should succeed");
        let stats = &store.inner.as_ref().unwrap().stats;
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
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn multiple_versions_and_single_version_keys_post_compaction() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        let store = Store::new(opts.clone()).expect("should create store");

        let num_keys = 100; // Total number of keys
        let multiple_versions_threshold = 50; // Keys above this index will have multiple versions

        // Insert keys into the store
        for key_index in 1..=num_keys {
            let key = format!("key{}", key_index).as_bytes().to_vec();
            {
                let mut txn = store.begin().unwrap();
                let value1 = format!("value{}_1", key_index).as_bytes().to_vec();

                // Insert first version for all keys
                txn.set(&key, &value1).unwrap();
                txn.commit().await.unwrap();
            }

            if key_index > multiple_versions_threshold {
                let value2 = format!("value{}_2", key_index).as_bytes().to_vec();
                {
                    let mut txn = store.begin().unwrap();
                    // Insert a second version for keys above the multiple_versions_threshold
                    txn.set(&key, &value2).unwrap();
                    txn.commit().await.unwrap();
                }
            }
        }

        // Perform compaction
        store.compact().await.expect("compaction should succeed");
        let stats = &store.inner.as_ref().unwrap().stats;
        assert_eq!(stats.compaction_stats.get_records_added(), 150);

        // Reopen the store
        drop(store);
        let store = Store::new(opts).expect("should reopen store");

        // Verify the results
        for key_index in 1..=num_keys {
            let mut txn = store.begin().unwrap();
            let key = format!("key{}", key_index).as_bytes().to_vec();
            let expected_value = if key_index > multiple_versions_threshold {
                format!("value{}_2", key_index)
            } else {
                format!("value{}_1", key_index)
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

        // Close the store
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn compact_handles_various_key_versions_correctly() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        let store = Store::new(opts.clone()).expect("should create store");

        let num_keys = 100; // Total number of keys
        let multiple_versions_threshold = 50; // Keys above this index will have multiple versions
        let delete_threshold = 75; // Keys above this index will be marked as deleted in their last version

        // Insert keys into the store with each operation in its own transaction
        for key_index in 1..=num_keys {
            let key = format!("key{}", key_index).as_bytes().to_vec();
            let value1 = format!("value{}_1", key_index).as_bytes().to_vec();

            // Insert first version for all keys in its own transaction
            {
                let mut txn = store.begin().unwrap();
                txn.set(&key, &value1).unwrap();
                txn.commit().await.unwrap();
            }

            // Insert a second version for keys above the multiple_versions_threshold in its own transaction
            if key_index > multiple_versions_threshold {
                let value2 = format!("value{}_2", key_index).as_bytes().to_vec();
                {
                    let mut txn = store.begin().unwrap();
                    txn.set(&key, &value2).unwrap();
                    txn.commit().await.unwrap();
                }

                // Mark the last version as deleted for keys above the delete_threshold in its own transaction
                if key_index > delete_threshold {
                    let mut txn = store.begin().unwrap();
                    txn.delete(&key).unwrap();
                    txn.commit().await.unwrap();
                }
            }
        }

        // Perform compaction
        store.compact().await.expect("compaction should succeed");
        let stats = &store.inner.as_ref().unwrap().stats;
        assert_eq!(stats.compaction_stats.get_records_added(), 100);

        // Reopen the store
        drop(store);
        let store = Store::new(opts).expect("should reopen store");

        // Verify the results
        for key_index in 1..=num_keys {
            let mut txn = store.begin().unwrap();
            let key = format!("key{}", key_index).as_bytes().to_vec();

            if key_index > delete_threshold {
                // Keys marked as deleted in their last version should not be present
                assert!(
                    txn.get(&key).unwrap().is_none(),
                    "Deleted key{} should not be present after compaction",
                    key_index
                );
            } else if key_index > multiple_versions_threshold {
                // Keys with multiple versions should have their last version
                let expected_value = format!("value{}_2", key_index);
                let val = txn
                    .get(&key)
                    .unwrap()
                    .expect("key should exist after compaction");
                assert_eq!(
                    val,
                    expected_value.as_bytes(),
                    "key{}'s value should be the last version after compaction",
                    key_index
                );
            } else {
                // Keys with a single version should remain unchanged
                let expected_value = format!("value{}_1", key_index);
                let val = txn
                    .get(&key)
                    .unwrap()
                    .expect("key should exist after compaction");
                assert_eq!(
                    val,
                    expected_value.as_bytes(),
                    "key{}'s value should remain unchanged after compaction",
                    key_index
                );
            }
        }

        // Close the store
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn compact_handles_various_key_versions_correctly_with_clear() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        let store = Store::new(opts.clone()).expect("should create store");

        let num_keys = 100; // Total number of keys
        let multiple_versions_threshold = 50; // Keys above this index will have multiple versions
        let clear_threshold = 75; // Keys above this index will be marked as deleted in their last version

        // Insert keys into the store with each operation in its own transaction
        for key_index in 1..=num_keys {
            let key = format!("key{}", key_index).as_bytes().to_vec();
            let value1 = format!("value{}_1", key_index).as_bytes().to_vec();

            // Insert first version for all keys in its own transaction
            {
                let mut txn = store.begin().unwrap();
                txn.set(&key, &value1).unwrap();
                txn.commit().await.unwrap();
            }

            // Insert a second version for keys above the multiple_versions_threshold in its own transaction
            if key_index > multiple_versions_threshold {
                let value2 = format!("value{}_2", key_index).as_bytes().to_vec();
                {
                    let mut txn = store.begin().unwrap();
                    txn.set(&key, &value2).unwrap();
                    txn.commit().await.unwrap();
                }

                // Mark the last version as deleted for keys above the clear_threshold in its own transaction
                if key_index > clear_threshold {
                    let mut txn = store.begin().unwrap();
                    txn.clear(&key).unwrap();
                    txn.commit().await.unwrap();
                }
            }
        }

        // Perform compaction
        store.compact().await.expect("compaction should succeed");
        let stats = &store.inner.as_ref().unwrap().stats;
        assert_eq!(stats.compaction_stats.get_records_added(), 175);

        // Reopen the store
        drop(store);
        let store = Store::new(opts).expect("should reopen store");

        for key_index in 1..=num_keys {
            let mut txn = store.begin().unwrap();
            let key = format!("key{}", key_index).as_bytes().to_vec();

            if key_index > clear_threshold {
                // Keys marked as deleted in their last version should not be present
                assert!(
                    txn.get(&key).unwrap().is_none(),
                    "Deleted key{} should not be present after compaction",
                    key_index
                );
            }
        }

        // Verify the results
        for key_index in 1..=num_keys {
            let mut txn = store.begin().unwrap();
            let key = format!("key{}", key_index).as_bytes().to_vec();

            if key_index > clear_threshold {
                // Keys marked as cleared in their last version should not be present
                assert!(
                    txn.get(&key).unwrap().is_none(),
                    "Cleared key{} should not be present after compaction",
                    key_index
                );
            } else if key_index > multiple_versions_threshold {
                // Keys with multiple versions should have their last version
                let expected_value = format!("value{}_2", key_index);
                let val = txn
                    .get(&key)
                    .unwrap()
                    .expect("key should exist after compaction");
                assert_eq!(
                    val,
                    expected_value.as_bytes(),
                    "key{}'s value should be the last version after compaction",
                    key_index
                );
            } else {
                // Keys with a single version should remain unchanged
                let expected_value = format!("value{}_1", key_index);
                let val = txn
                    .get(&key)
                    .unwrap()
                    .expect("key should exist after compaction");
                assert_eq!(
                    val,
                    expected_value.as_bytes(),
                    "key{}'s value should remain unchanged after compaction",
                    key_index
                );
            }
        }

        // Close the store
        store.close().await.unwrap();
    }
}
