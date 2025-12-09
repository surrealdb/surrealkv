//! Integration tests for SurrealKV
//!
//! This module contains integration tests that test the full LSM stack including
//! WAL recovery, memtable flush, SST interaction, and manifest coordination.

pub mod recovery_integration_tests;
pub mod recovery_test_helpers;
