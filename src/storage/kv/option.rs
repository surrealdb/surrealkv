use std::path::PathBuf;

use crate::storage::index::art::DEFAULT_MAX_ACTIVE_SNAPSHOTS;
use crate::storage::kv::error::{Error, Result};
use crate::storage::log::Metadata;

// Defining constants for metadata keys
const META_KEY_ISOLATION_LEVEL: &str = "isolation_level";
const META_KEY_MAX_KEY_SIZE: &str = "max_key_size";
const META_KEY_MAX_VALUE_SIZE: &str = "max_value_size";
const META_KEY_MAX_VALUE_THRESHOLD: &str = "max_value_threshold";
const META_KEY_DETECT_CONFLICTS: &str = "detect_conflicts";
const META_KEY_CREATE_IF_NOT_EXISTS: &str = "create_if_not_exists";
const META_KEY_MAX_TX_ENTRIES: &str = "max_tx_entries";
const META_KEY_WAL_DISABLED: &str = "wal_disabled";
const META_KEY_MAX_ACTIVE_SNAPSHOTS: &str = "max_active_snapshots";
const META_KEY_MAX_FILE_SIZE: &str = "max_file_size";

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
    pub max_value_threshold: usize, // Threshold to decide value storage in LSM tree or log value files.
    pub detect_conflicts: bool,     // Whether to check transactions for conflicts.
    pub create_if_not_exists: bool, // Create the directory if the provided open path doesn't exist.
    pub max_tx_entries: usize,      // Maximum entries in a transaction.
    pub wal_disabled: bool,         // Whether to disable the write-ahead log.
    pub max_active_snapshots: u64,  // Maximum number of active snapshots.
    pub max_segment_size: u64,      // Maximum size of a single segment.
}

impl Default for Options {
    /// Creates a new set of options with default values.
    fn default() -> Self {
        Self {
            dir: PathBuf::from(""),
            max_key_size: 1024,
            max_value_size: 1024 * 1024,
            detect_conflicts: true,
            create_if_not_exists: true,
            max_tx_entries: 1 << 10,
            wal_disabled: false,
            max_value_threshold: 64, // 64 bytes
            isolation_level: IsolationLevel::SnapshotIsolation,
            max_active_snapshots: DEFAULT_MAX_ACTIVE_SNAPSHOTS,
            max_segment_size: 1 << 26, // 64 MB
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
        metadata.put_int(META_KEY_ISOLATION_LEVEL, self.isolation_level as u64);
        metadata.put_int(META_KEY_MAX_KEY_SIZE, self.max_key_size);
        metadata.put_int(META_KEY_MAX_VALUE_SIZE, self.max_value_size);
        metadata.put_int(
            META_KEY_MAX_VALUE_THRESHOLD,
            self.max_value_threshold as u64,
        );
        metadata.put_bool(META_KEY_DETECT_CONFLICTS, self.detect_conflicts);
        metadata.put_bool(META_KEY_CREATE_IF_NOT_EXISTS, self.create_if_not_exists);
        metadata.put_int(META_KEY_MAX_TX_ENTRIES, self.max_tx_entries as u64);
        metadata.put_bool(META_KEY_WAL_DISABLED, self.wal_disabled);
        metadata.put_int(META_KEY_MAX_ACTIVE_SNAPSHOTS, self.max_active_snapshots);
        metadata.put_int(META_KEY_MAX_FILE_SIZE, self.max_segment_size);

        metadata
    }

    /// Convert Metadata to Options.
    pub fn from_metadata(metadata: Metadata, dir: PathBuf) -> Result<Self> {
        let isolation_level = IsolationLevel::from_u64(metadata.get_int(META_KEY_ISOLATION_LEVEL)?)
            .ok_or(Error::CorruptedMetadata)?;

        Ok(Options {
            dir,
            isolation_level,
            max_key_size: metadata.get_int(META_KEY_MAX_KEY_SIZE)?,
            max_value_size: metadata.get_int(META_KEY_MAX_VALUE_SIZE)?,
            max_value_threshold: metadata.get_int(META_KEY_MAX_VALUE_THRESHOLD)? as usize,
            detect_conflicts: metadata.get_bool(META_KEY_DETECT_CONFLICTS)?,
            create_if_not_exists: metadata.get_bool(META_KEY_CREATE_IF_NOT_EXISTS)?,
            max_tx_entries: metadata.get_int(META_KEY_MAX_TX_ENTRIES)? as usize,
            wal_disabled: metadata.get_bool(META_KEY_WAL_DISABLED)?,
            max_active_snapshots: metadata.get_int(META_KEY_MAX_ACTIVE_SNAPSHOTS)?,
            max_segment_size: metadata.get_int(META_KEY_MAX_FILE_SIZE)?,
        })
    }
}
