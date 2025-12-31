//! Integration tests for SurrealKV
//!
//! This module contains integration tests that test the full LSM stack
//! including WAL recovery, memtable flush, SST interaction, and manifest
//! coordination.

#[cfg(test)]
pub mod batch_tests;
#[cfg(test)]
pub mod block_tests;
#[cfg(test)]
pub mod compaction_tests;
#[cfg(test)]
pub mod compression_tests;
#[cfg(test)]
pub mod index_block_tests;
#[cfg(test)]
pub mod iterator_tests;
#[cfg(test)]
pub mod lsm_tests;
#[cfg(test)]
pub mod manifest_tests;
#[cfg(test)]
pub mod memtable_tests;
#[cfg(test)]
pub mod recovery_integration_tests;
#[cfg(test)]
pub mod recovery_test_helpers;
#[cfg(test)]
pub mod recovery_tests;
#[cfg(test)]
pub mod snapshot_tests;
#[cfg(test)]
pub mod sstable_tests;
#[cfg(test)]
pub mod transaction_tests;
#[cfg(test)]
pub mod vlog_tests;
#[cfg(test)]
pub mod wal_tests;
