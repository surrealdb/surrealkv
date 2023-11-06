use std::path::PathBuf;

use crate::storage::index::art::DEFAULT_MAX_ACTIVE_SNAPSHOTS;

#[derive(Clone)]
pub struct Options {
    // Required options.
    pub dir: PathBuf,     // Directory path for storing the database files.
    pub wal_dir: PathBuf, // Directory path for storing the write-ahead log.

    // Usually modified options.
    pub isolation_level: IsolationLevel, // Isolation level for transactions.

    // Fine tuning options.
    pub max_key_size: u64,          // Maximum size in bytes for key.
    pub max_value_size: u64,        // Maximum size in bytes for value.
    pub max_value_threshold: usize, // Threshold to decide value storage in LSM tree or log value files.
    pub value_log_file_size: u64,   // Maximum size of a single value log file segment.
    pub detect_conflicts: bool,     // Whether to check transactions for conflicts.
    pub create_if_not_exists: bool, // Create the directory if the provided open path doesn't exist.
    pub max_tx_entries: usize,          // Maximum entries in a transaction.
    pub wal_disabled: bool,         // Whether to disable the write-ahead log.
    pub max_active_snapshots: u64,  // Maximum number of active snapshots.
}

impl Default for Options {
    /// Creates a new set of options with default values.
    fn default() -> Self {
        Self {
            dir: PathBuf::from(""),
            wal_dir: PathBuf::from("./wal"),
            max_key_size: 1024,
            max_value_size: 1024 * 1024,
            value_log_file_size: 1024 * 1024 * 1024,
            detect_conflicts: true,
            create_if_not_exists: true,
            max_tx_entries: 1 << 10,
            wal_disabled: false,
            max_value_threshold: 64, // 64 bytes
            isolation_level: IsolationLevel::SnapshotIsolation,
            max_active_snapshots: DEFAULT_MAX_ACTIVE_SNAPSHOTS,
        }
    }
}

impl Options {
    /// Creates a new set of options with default values.
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Clone)]
pub enum IsolationLevel {
    SnapshotIsolation = 1,
    SerializableSnapshotIsolation = 2,
}
