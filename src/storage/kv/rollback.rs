use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

use crate::storage::kv::util::copy_dir_all;
use crate::storage::log::SegmentRef;
use crate::{Error, Options, Result};

use super::store::Core;

#[derive(Debug, PartialEq)]
pub(crate) enum RecoveryState {
    None,
    ClogBackedUp,
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
                "ClogBackedUp" => Ok(RecoveryState::ClogBackedUp),
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
            RecoveryState::ClogBackedUp => {
                write!(file, "ClogBackedUp")
                    .map_err(|e| Error::CustomError(format!("recovery file write error: {}", e)))?;
                Ok(())
            }
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

        // Original operations with modifications for transactional integrity
        // For example, deleting the .tmp.merge directory is now safe to skip as it's already backed up

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
        // Step 4: Copy files from clog dir to merge clog dir and then copy it back to clog dir to avoid
        // creating a backup directory
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

        // Copy files from the `.merge/clog` directory into the `clog` directory

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
            let backup_dir = opts.dir.join(".backup");
            let clog_dir = opts.dir.join("clog");
            rollback(&backup_dir, &clog_dir, RecoveryState::load(&opts.dir)?)?;
            Err(e)
        }
    }
}

fn cleanup_after_recovery(opts: &Options) -> Result<()> {
    let backup_dir = opts.dir.join(".backup");
    let merge_dir = opts.dir.join(".merge");

    if backup_dir.exists() {
        fs::remove_dir_all(&backup_dir)?;
    }
    if merge_dir.exists() {
        fs::remove_dir_all(&merge_dir)?;
    }
    Ok(())
}

fn rollback(backup_dir: &Path, clog_dir: &Path, checkpoint: RecoveryState) -> Result<()> {
    match checkpoint {
        RecoveryState::ClogBackedUp => {
            // Restore the clog directory from backup
            let backup_clog_dir = backup_dir.join("clog");
            if backup_clog_dir.exists() && clog_dir.exists() {
                fs::remove_dir_all(clog_dir)?;
            }
            if backup_clog_dir.exists() {
                fs::rename(&backup_clog_dir, clog_dir)?;
            }
        }
        RecoveryState::ClogDeleted => {
            // Restore the clog directory from backup
            let backup_clog_dir = backup_dir.join("clog");
            if backup_clog_dir.exists() {
                fs::rename(&backup_clog_dir, clog_dir)?;
            }
        }
        _ => (),
    }

    // Clean up backup directory after rollback
    if backup_dir.exists() {
        fs::remove_dir_all(backup_dir)?;
    }

    Ok(())
}

fn needs_recovery(opts: &Options) -> Result<bool> {
    Ok(opts.dir.join(".merge").exists())
}

fn handle_clog_backed_up_state(opts: &Options) -> Result<()> {
    let backup_dir = opts.dir.join(".backup");
    let clog_dir = opts.dir.join("clog");
    rollback(&backup_dir, &clog_dir, RecoveryState::ClogBackedUp)
}

fn handle_clog_deleted_state(opts: &Options) -> Result<()> {
    let backup_dir = opts.dir.join(".backup");
    let clog_dir = opts.dir.join("clog");
    rollback(&backup_dir, &clog_dir, RecoveryState::ClogDeleted)
}

fn backup_and_prepare_for_recovery(opts: &Options) -> Result<()> {
    let backup_dir = opts.dir.join(".backup");
    let clog_dir = opts.dir.join("clog");
    let merge_dir = opts.dir.join(".merge");

    if merge_dir.exists() {
        fs::create_dir_all(&backup_dir)?;
        copy_dir_all(&clog_dir, &backup_dir.join("clog"))?;
        RecoveryState::ClogBackedUp.save(&opts.dir)?;
    }
    Ok(())
}

/// Restores the store from a compaction process by handling .tmp.merge and .merge directories.
/// TODO: This should happen post repair
pub fn restore_from_compaction(opts: &Options) -> Result<()> {
    let tmp_merge_dir = opts.dir.join(".tmp.merge");
    // 1) Check if there is a .tmp.merge directory, delete it
    if tmp_merge_dir.exists() {
        fs::remove_dir_all(&tmp_merge_dir).map_err(Error::from)?;
        return Ok(());
    }

    if !needs_recovery(opts)? {
        return Ok(());
    }

    match RecoveryState::load(&opts.dir)? {
        RecoveryState::ClogBackedUp => handle_clog_backed_up_state(opts)?,
        RecoveryState::ClogDeleted => handle_clog_deleted_state(opts)?,
        RecoveryState::None => backup_and_prepare_for_recovery(opts)?,
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

        // Test saving and loading ClogBackedUp
        RecoveryState::ClogBackedUp.save(temp_dir).unwrap();
        assert_eq!(
            RecoveryState::load(temp_dir).unwrap(),
            RecoveryState::ClogBackedUp
        );

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

        // Test save contents for ClogBackedUp
        RecoveryState::ClogBackedUp.save(temp_dir).unwrap();
        let contents = read_to_string(&path).unwrap();
        assert_eq!(contents, "ClogBackedUp");

        // Clear state and re-setup for next test
        RecoveryState::clear(temp_dir).unwrap();
        assert!(!path.exists());

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
            let txn = store.begin().unwrap();
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
            let txn = reopened_store.begin().unwrap();
            assert!(txn.get(key).unwrap().is_none());
        }

        // Verify that the first half of the remaining keys have the updated value
        for key in keys
            .iter()
            .skip(num_keys_to_delete)
            .take(num_keys_to_write / 2 - num_keys_to_delete)
        {
            let txn = reopened_store.begin().unwrap();
            let val = txn.get(key).unwrap().unwrap();
            assert_eq!(val, updated_value.as_ref());
        }

        // Verify that the second half of the keys still have the default value
        for key in keys.iter().skip(num_keys_to_write / 2) {
            let txn = reopened_store.begin().unwrap();
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
            let txn = reopened_store.begin().unwrap();
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
            let txn = reopened_store.begin().unwrap();
            let val = txn.get(key).unwrap().unwrap();
            assert_eq!(val, default_value.as_ref());
        }

        // Verify post-compaction keys are present
        for key in post_compaction_keys.iter() {
            let txn = reopened_store.begin().unwrap();
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
                let txn = reopened_store.begin().unwrap();
                let val = txn.get(key).unwrap().unwrap();
                assert_eq!(val, default_value.as_ref());
            }
        }

        // Verify deleted keys are not present
        for key in &keys_to_delete {
            let txn = reopened_store.begin().unwrap();
            assert!(txn.get(key).unwrap().is_none());
        }

        // Verify post-compaction keys are present
        for key in &post_compaction_keys {
            let txn = reopened_store.begin().unwrap();
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
            let txn = reopened_store.begin().unwrap();
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
            let txn = reopened_store
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
            let txn = reopened_store
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
            let txn = reopened_store
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
                let txn = store.begin().unwrap();
                let val = txn.get(key.as_bytes()).unwrap().unwrap();

                assert_eq!(val, value);
            }

            // Close the store again
            store.close().await.unwrap();
        }
    }
}
