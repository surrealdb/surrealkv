use std::path::PathBuf;

use crate::storage::{
    kv::error::{Error, Result},
    log::Metadata,
};

use vart::art::DEFAULT_MAX_ACTIVE_SNAPSHOTS;

// Defining constants for metadata keys
const META_KEY_ISOLATION_LEVEL: &str = "isolation_level";
const META_KEY_MAX_KEY_SIZE: &str = "max_key_size";
const META_KEY_MAX_VALUE_SIZE: &str = "max_value_size";
const META_KEY_MAX_VALUE_THRESHOLD: &str = "max_value_threshold";
const META_KEY_CREATE_IF_NOT_EXISTS: &str = "create_if_not_exists";
const META_KEY_MAX_TX_ENTRIES: &str = "max_tx_entries";
const META_KEY_MAX_ACTIVE_SNAPSHOTS: &str = "max_active_snapshots";
const META_KEY_MAX_FILE_SIZE: &str = "max_file_size";
const META_KEY_MAX_VALUE_CACHE_SIZE: &str = "max_value_cache_size";

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum IsolationLevel {
    SnapshotIsolation = 1,
    SerializableSnapshotIsolation = 2,
}

impl IsolationLevel {
    pub fn from_u64(value: u64) -> Option<Self> {
        match value {
            1 => Some(IsolationLevel::SnapshotIsolation),
            2 => Some(IsolationLevel::SerializableSnapshotIsolation),
            _ => None,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Options {
    // Required options.
    pub dir: PathBuf, // Directory path for storing the database files.

    // Usually modified options.
    pub isolation_level: IsolationLevel, // Isolation level for transactions.

    // Fine tuning options.
    pub max_key_size: u64,          // Maximum size in bytes for key.
    pub max_value_size: u64,        // Maximum size in bytes for value.
    pub max_value_threshold: usize, // Threshold to decide value should be stored and read from memory or from log value files.
    pub create_if_not_exists: bool, // Create the directory if the provided open path doesn't exist.
    pub max_tx_entries: u32,        // Maximum entries in a transaction.
    pub max_active_snapshots: u64,  // Maximum number of active snapshots.
    pub max_segment_size: u64,      // Maximum size of a single segment.
    pub max_value_cache_size: u64,  // Maximum size of the value cache.
}

impl Default for Options {
    /// Creates a new set of options with default values.
    fn default() -> Self {
        Self {
            dir: PathBuf::from(""),
            max_key_size: 1024,
            max_value_size: 1024 * 1024,
            create_if_not_exists: true,
            max_tx_entries: 1 << 10,
            max_value_threshold: 64, // 64 bytes
            isolation_level: IsolationLevel::SnapshotIsolation,
            max_active_snapshots: DEFAULT_MAX_ACTIVE_SNAPSHOTS,
            max_segment_size: 1 << 26, // 64 MB
            max_value_cache_size: 100000,
        }
    }
}

impl Options {
    /// Creates a new set of options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Convert Options to Metadata.
    pub fn to_metadata(&self) -> Metadata {
        let mut metadata = Metadata::new(None);
        metadata.put_uint(META_KEY_ISOLATION_LEVEL, self.isolation_level as u64);
        metadata.put_uint(META_KEY_MAX_KEY_SIZE, self.max_key_size);
        metadata.put_uint(META_KEY_MAX_VALUE_SIZE, self.max_value_size);
        metadata.put_uint(
            META_KEY_MAX_VALUE_THRESHOLD,
            self.max_value_threshold as u64,
        );
        metadata.put_bool(META_KEY_CREATE_IF_NOT_EXISTS, self.create_if_not_exists);
        metadata.put_uint(META_KEY_MAX_TX_ENTRIES, self.max_tx_entries as u64);
        metadata.put_uint(META_KEY_MAX_ACTIVE_SNAPSHOTS, self.max_active_snapshots);
        metadata.put_uint(META_KEY_MAX_FILE_SIZE, self.max_segment_size);
        metadata.put_uint(META_KEY_MAX_VALUE_CACHE_SIZE, self.max_value_cache_size);

        metadata
    }

    /// Convert Metadata to Options.
    pub fn from_metadata(metadata: Metadata, dir: PathBuf) -> Result<Self> {
        let isolation_level =
            IsolationLevel::from_u64(metadata.get_uint(META_KEY_ISOLATION_LEVEL)?)
                .ok_or(Error::CorruptedMetadata)?;

        Ok(Options {
            dir,
            isolation_level,
            max_key_size: metadata.get_uint(META_KEY_MAX_KEY_SIZE)?,
            max_value_size: metadata.get_uint(META_KEY_MAX_VALUE_SIZE)?,
            max_value_threshold: metadata.get_uint(META_KEY_MAX_VALUE_THRESHOLD)? as usize,
            create_if_not_exists: metadata.get_bool(META_KEY_CREATE_IF_NOT_EXISTS)?,
            max_tx_entries: metadata.get_uint(META_KEY_MAX_TX_ENTRIES)? as u32,
            max_active_snapshots: metadata.get_uint(META_KEY_MAX_ACTIVE_SNAPSHOTS)?,
            max_segment_size: metadata.get_uint(META_KEY_MAX_FILE_SIZE)?,
            max_value_cache_size: metadata.get_uint(META_KEY_MAX_VALUE_CACHE_SIZE)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn default_options() {
        let options = Options::default();

        assert_eq!(options.dir, PathBuf::from(""));
        assert_eq!(options.max_key_size, 1024);
        assert_eq!(options.max_value_size, 1024 * 1024);
        assert!(options.create_if_not_exists);
        assert_eq!(options.max_tx_entries, 1 << 10);
        assert_eq!(options.max_value_threshold, 64);
        assert_eq!(options.isolation_level, IsolationLevel::SnapshotIsolation);
        assert_eq!(options.max_active_snapshots, DEFAULT_MAX_ACTIVE_SNAPSHOTS);
        assert_eq!(options.max_segment_size, 1 << 26);
        assert_eq!(options.max_value_cache_size, 100000);
    }

    #[test]
    fn options_to_metadata() {
        let options = Options {
            dir: PathBuf::from("/test/dir"),
            max_key_size: 2048,
            max_value_size: 4096,
            create_if_not_exists: false,
            max_tx_entries: 500,
            max_value_threshold: 128,
            isolation_level: IsolationLevel::SerializableSnapshotIsolation,
            max_active_snapshots: 10,
            max_segment_size: 1 << 25, // 32 MB
            max_value_cache_size: 200000,
        };

        let metadata = options.to_metadata();

        assert_eq!(
            metadata.get_uint(META_KEY_ISOLATION_LEVEL).unwrap(),
            IsolationLevel::SerializableSnapshotIsolation as u64
        );
        assert_eq!(metadata.get_uint(META_KEY_MAX_KEY_SIZE).unwrap(), 2048);
        assert_eq!(metadata.get_uint(META_KEY_MAX_VALUE_SIZE).unwrap(), 4096);
        assert_eq!(
            metadata.get_uint(META_KEY_MAX_VALUE_THRESHOLD).unwrap(),
            128
        );
        assert!(!metadata.get_bool(META_KEY_CREATE_IF_NOT_EXISTS).unwrap());
        assert_eq!(metadata.get_uint(META_KEY_MAX_TX_ENTRIES).unwrap(), 500);
        assert_eq!(
            metadata.get_uint(META_KEY_MAX_ACTIVE_SNAPSHOTS).unwrap(),
            10
        );
        assert_eq!(metadata.get_uint(META_KEY_MAX_FILE_SIZE).unwrap(), 1 << 25);
        assert_eq!(
            metadata.get_uint(META_KEY_MAX_VALUE_CACHE_SIZE).unwrap(),
            200000
        );
    }

    #[test]
    fn options_from_metadata() {
        let mut metadata = Metadata::new(None);
        metadata.put_uint(
            META_KEY_ISOLATION_LEVEL,
            IsolationLevel::SerializableSnapshotIsolation as u64,
        );
        metadata.put_uint(META_KEY_MAX_KEY_SIZE, 2048);
        metadata.put_uint(META_KEY_MAX_VALUE_SIZE, 4096);
        metadata.put_uint(META_KEY_MAX_VALUE_THRESHOLD, 128);
        metadata.put_bool(META_KEY_CREATE_IF_NOT_EXISTS, false);
        metadata.put_uint(META_KEY_MAX_TX_ENTRIES, 500);
        metadata.put_uint(META_KEY_MAX_ACTIVE_SNAPSHOTS, 10);
        metadata.put_uint(META_KEY_MAX_FILE_SIZE, 1 << 25);
        metadata.put_uint(META_KEY_MAX_VALUE_CACHE_SIZE, 200000);

        let dir = PathBuf::from("/test/dir");
        let options_result = Options::from_metadata(metadata, dir.clone());

        assert!(options_result.is_ok());

        let options = options_result.unwrap();

        assert_eq!(options.dir, dir);
        assert_eq!(options.max_key_size, 2048);
        assert_eq!(options.max_value_size, 4096);
        assert_eq!(options.max_value_threshold, 128);
        assert!(!options.create_if_not_exists);
        assert_eq!(options.max_tx_entries, 500);
        assert_eq!(
            options.isolation_level,
            IsolationLevel::SerializableSnapshotIsolation
        );
        assert_eq!(options.max_active_snapshots, 10);
        assert_eq!(options.max_segment_size, 1 << 25);
        assert_eq!(options.max_value_cache_size, 200000);
    }
}
