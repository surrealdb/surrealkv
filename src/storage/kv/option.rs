use std::path::PathBuf;

use super::entry::{MAX_KV_METADATA_SIZE, MAX_TX_METADATA_SIZE};

#[derive(Clone)]
pub struct Options {
    // Required options.
    pub dir: PathBuf,     // Directory path for storing the database files.
    pub wal_dir: PathBuf, // Directory path for storing the write-ahead log.

    // Usually modified options.
    pub sync_writes: bool,           // Whether to perform fsync after writes.
    pub num_versions_to_keep: usize, // Maximum versions to keep per key.

    // Fine tuning options.
    pub max_tx_entries: usize,
    pub max_key_size: u64,          // Maximum size in bytes for key.
    pub max_value_size: u64,        // Maximum size in bytes for value.
    pub value_threshold: usize, // Threshold to decide value storage in LSM tree or log value files.
    pub value_log_file_size: u64, // Maximum size of a single value log file segment.
    pub detect_conflicts: bool, // Whether to check transactions for conflicts.
    pub managed_txns: bool,     // Transaction timestamps managed by end-user.
    pub create_if_not_exists: bool, // Create the directory if the provided open path doesn't exist.
    pub max_batch_count: u64,   // Maximum entries in a batch.
    pub max_batch_size: u64,    // Maximum batch size in bytes.
    pub wal_disabled: bool,     // Whether to disable the write-ahead log.
}

impl Default for Options {
    /// Creates a new set of options with default values.
    fn default() -> Self {
        Self {
            dir: PathBuf::from(""),
            wal_dir: PathBuf::from("./wal"),
            sync_writes: false,
            num_versions_to_keep: 1,
            max_key_size: 1024,
            max_value_size: 1024 * 1024,
            value_threshold: 1024,
            value_log_file_size: 1 * 1024 * 1024 * 1024,
            detect_conflicts: true,
            managed_txns: true,
            create_if_not_exists: true,
            max_batch_count: 1000,
            max_batch_size: 4 * 1024 * 1024,
            wal_disabled: false,
            max_tx_entries: 1 << 10,
        }
    }
}

impl Options {
    /// Creates a new set of options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn max_tx_size(&self) -> usize {
        let u16_size = std::mem::size_of::<u16>();
        let u32_size = std::mem::size_of::<u32>();
        let u64_size = std::mem::size_of::<u64>();

        // fn max_tx_size(max_tx_entries: usize, max_key_len: usize, max_tx_metadata_len: usize, max_kv_metadata_len: usize) -> usize {
        //      let u16_size = std::mem::size_of::<u16>();
        //      let u32_size = std::mem::size_of::<u32>();
        //      let u64_size = std::mem::size_of::<u64>();
        // }
        //
        // tx_id + ts + version + tx_md_len + max_tx_md_len + entries_size + max_tx_entries * (kv_md_len + max_kv_md_len + key_len + max_key_len + value_len + value_offset_size)
        // u64_size + u64_size + u16_size + u16_size + max_tx_metadata_len + u32_size +
        //    max_tx_entries * (u16_size + max_kv_metadata_len + u16_size + max_key_len + u32_size + u64_size)

        // tx_id + ts + version + tx_md_len + max_tx_md_len + entries_size + max_tx_entries * (kv_md_len + max_kv_md_len + key_len + max_key_len + value_len + value_offset_size)
        u64_size
            + u64_size
            + u16_size
            + u16_size
            + MAX_TX_METADATA_SIZE
            + u32_size
            + self.max_tx_entries
                * (u16_size
                    + MAX_KV_METADATA_SIZE
                    + u16_size
                    + self.max_key_size as usize
                    + u32_size
                    + u64_size)
    }
}