//! Integration tests for SurrealKV
//!
//! This module contains integration tests that test the full LSM stack including
//! WAL recovery, memtable flush, SST interaction, and manifest coordination.

#[cfg(test)]
pub mod compression_tests;
#[cfg(test)]
pub mod recovery_integration_tests;
#[cfg(test)]
pub mod recovery_test_helpers;
#[cfg(test)]
pub mod recovery_tests;
