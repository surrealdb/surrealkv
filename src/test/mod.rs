//! Integration tests for SurrealKV
//!
//! This module contains integration tests that test the full LSM stack
//! including WAL recovery, memtable flush, SST interaction, and manifest
//! coordination.

use crate::snapshot::SnapshotIterator;
use crate::vlog::ValueLocation;
use crate::{InternalKey, LSMIterator, Result};

#[cfg(test)]
pub mod alloc_limits_tests;
#[cfg(test)]
pub mod atomic_memtable_tests;
#[cfg(test)]
pub mod batch_tests;
#[cfg(test)]
pub mod block_io_tests;
#[cfg(test)]
pub mod block_read_tests;
#[cfg(test)]
pub mod block_tests;
#[cfg(test)]
pub mod cache_namespace_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod canary;
#[cfg(test)]
pub mod checkpoint_close_tests;
#[cfg(test)]
pub mod checkpoint_concurrency_tests;
#[cfg(test)]
pub mod checkpoint_destination_tests;
#[cfg(test)]
pub mod checkpoint_flush_tests;
#[cfg(test)]
pub mod close_race_tests;
#[cfg(test)]
pub mod commit_pipeline_gate_tests;
#[cfg(test)]
pub mod commit_pipeline_tests;
#[cfg(test)]
pub mod compact_range_tests;
#[cfg(test)]
pub mod compaction_crash_tests;
#[cfg(test)]
pub mod compaction_failure_recovery_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod compaction_horizon_tests;
#[cfg(test)]
pub mod compaction_memory_proofs;
#[cfg(test)]
pub mod compaction_tests;
#[cfg(test)]
pub mod compression_tests;
#[cfg(test)]
pub mod crash_consistency_tests;
#[cfg(test)]
pub mod describe_tests;
#[cfg(test)]
pub mod direct_l0_flush_tests;
#[cfg(test)]
pub mod direct_l0_group_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod direct_l0_manifest_failure_tests;
#[cfg(test)]
pub mod direct_l0_read_order_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod direct_l0_wal_cleanup_tests;
#[cfg(test)]
pub mod encryption_background_tests;
#[cfg(test)]
pub mod encryption_checkpoint_tests;
#[cfg(test)]
pub mod encryption_checkpoint_wal_tests;
#[cfg(test)]
pub mod encryption_commit_path_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod encryption_creation_crash_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod encryption_fuzz_tests;
#[cfg(test)]
pub mod encryption_open_tests;
#[cfg(test)]
pub mod encryption_prepare_tests;
#[cfg(test)]
pub mod encryption_status_tests;
#[cfg(test)]
pub mod encryption_surfaces_tests;
#[cfg(test)]
pub mod encryption_typed_errors_tests;
#[cfg(test)]
pub mod failed_open_tests;
#[cfg(test)]
pub mod flusher_alloc_diet_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod format_gate_tests;
#[cfg(test)]
pub mod fs_conformance_crash_tests;
#[cfg(test)]
pub mod fs_engine_crash_tests;
#[cfg(test)]
pub mod fs_engine_exhaustion_tests;
#[cfg(test)]
pub mod fs_engine_followup_tests;
#[cfg(test)]
pub mod fs_engine_tests;
#[cfg(test)]
pub mod fs_error_tests;
#[cfg(test)]
pub mod fs_pool_crash_tests;
#[cfg(test)]
pub mod fs_probe;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod fs_std_tests;
#[cfg(test)]
pub mod group_fail_stop_edge_tests;
#[cfg(test)]
pub mod group_fail_stop_tests;
#[cfg(test)]
pub mod index_block_tests;
#[cfg(test)]
pub mod indexeddb_migration_tests;
#[cfg(test)]
pub mod inspect_encryption_tests;
#[cfg(test)]
pub mod iterator_semantics_tests;
#[cfg(test)]
pub mod iterator_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod legacy_migration_tests;
#[cfg(test)]
pub mod level_order_crash_tests;
#[cfg(test)]
pub mod level_tests;
#[cfg(test)]
pub mod lsm_tests;
#[cfg(test)]
pub mod manifest_checksum_tests;
#[cfg(test)]
pub mod manifest_encryption_tests;
#[cfg(test)]
pub mod manifest_missing_tests;
#[cfg(test)]
pub mod manifest_tests;
#[cfg(test)]
pub mod manifest_write_failure_tests;
#[cfg(test)]
pub mod manifest_write_retry_tests;
#[cfg(test)]
pub mod manifest_write_stop_tests;
#[cfg(test)]
pub mod memtable_tests;
#[cfg(test)]
pub mod orphan_cleanup_tests;
#[cfg(test)]
pub mod recovery_integration_tests;
#[cfg(test)]
pub mod recovery_test_helpers;
#[cfg(test)]
pub mod recovery_tests;
#[cfg(test)]
pub mod reference_writer;
#[cfg(test)]
pub mod restore_concurrency_tests;
#[cfg(test)]
pub mod retire_free_tests;
#[cfg(test)]
pub mod retire_isolation_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod rocksdb_migration_tests;
#[cfg(test)]
pub mod rotate_vlog_order_tests;
#[cfg(test)]
pub mod rotate_vlog_race_tests;
#[cfg(test)]
pub mod scrub_encryption_tests;
#[cfg(test)]
pub mod sfs;
#[cfg(test)]
pub mod snapshot_tests;
#[cfg(test)]
pub mod sstable_encryption_tests;
#[cfg(test)]
pub mod sstable_encryption_tree_tests;
#[cfg(test)]
pub mod sstable_layout_tests;
#[cfg(test)]
pub mod sstable_tests;
#[cfg(test)]
pub mod stall_tests;
#[cfg(test)]
pub mod tde_tests;
#[cfg(test)]
pub mod tfs;
#[cfg(test)]
mod tfs_excluded;
#[cfg(test)]
pub mod transaction_tests;
#[cfg(test)]
pub mod tree_builder_encryption_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod value_location_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod versioning_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod vlog_check_entry_tests;
#[cfg(test)]
pub mod vlog_floor_tests;
#[cfg(test)]
pub mod vlog_gc_tests;
#[cfg(test)]
pub mod vlog_sealed_tests;
#[cfg(test)]
pub mod vlog_sealed_tree_tests;
#[cfg(test)]
pub mod vlog_sync_poison_tests;
#[cfg(test)]
pub mod vlog_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod wal_context_tests;
#[cfg(test)]
pub mod wal_decode_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod wal_foreign_segment_tests;
#[cfg(test)]
pub mod wal_fsync_poison_tests;
#[cfg(test)]
pub mod wal_group_budget_failure_tests;
#[cfg(test)]
pub mod wal_group_budget_tests;
#[cfg(test)]
pub mod wal_group_commit_tests;
#[cfg(test)]
pub mod wal_group_crash_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod wal_group_rotation_bound_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod wal_replace_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod wal_replay_durability_tests;
#[cfg(test)]
pub mod wal_rotation_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod wal_sealed_recovery_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod wal_sealed_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod wal_sealed_tree_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod wal_sealed_writer_tests;
#[cfg(test)]
pub mod wal_stale_rounds_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod wal_sync_mark_tests;
#[cfg(test)]
pub mod wal_tail_check_crash_tests;
#[cfg(test)]
pub mod wal_tail_check_replay_tests;
#[cfg(all(test, not(target_arch = "wasm32")))]
pub mod wal_tail_check_sealed_tests;
#[cfg(test)]
pub mod wal_tail_check_tests;
#[cfg(test)]
pub mod wal_tests;
#[cfg(test)]
pub mod wal_torn_tail_tests;

/// Collects all (key, value) pairs from an LSMIterator into a Vec
/// Assumes iterator is already positioned (e.g., after seek_first or seek)
fn collect_iter(iter: &mut impl LSMIterator) -> Vec<(InternalKey, Vec<u8>)> {
	let mut result = Vec::new();
	while iter.valid() {
		result.push((iter.key().to_owned(), iter.value_encoded().unwrap().to_vec()));
		if !iter.next().unwrap_or(false) {
			break;
		}
	}
	result
}

/// Collects all items from start (seek_first) to end
fn collect_all(iter: &mut impl LSMIterator) -> Result<Vec<(InternalKey, Vec<u8>)>> {
	iter.seek_first()?;
	Ok(collect_iter(iter))
}

/// Counts entries in an iterator
fn count_iter(iter: &mut impl LSMIterator) -> Result<usize> {
	iter.seek_first()?;
	let mut count = 0;
	while iter.valid() {
		count += 1;
		if !iter.next()? {
			break;
		}
	}
	Ok(count)
}

/// Collects all entries from a TransactionIterator
fn collect_transaction_iter(iter: &mut impl LSMIterator) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	let mut result = Vec::new();
	while iter.valid() {
		let key = iter.key().user_key().to_vec();
		let value = iter.value()?;
		result.push((key, value));
		iter.next()?;
	}
	Ok(result)
}

/// Collects all entries from a TransactionIterator starting from seek_first()
fn collect_transaction_all(iter: &mut impl LSMIterator) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	iter.seek_first()?;
	collect_transaction_iter(iter)
}

/// Collects all entries from a TransactionIterator in reverse order starting from seek_last()
fn collect_transaction_reverse(iter: &mut impl LSMIterator) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	iter.seek_last()?;
	let mut result = Vec::new();
	while iter.valid() {
		let key = iter.key().user_key().to_vec();
		let value = iter.value()?;
		result.push((key, value));
		if !iter.prev()? {
			break;
		}
	}
	Ok(result)
}

/// Collects all entries from a SnapshotIterator starting from seek_first()
/// Decodes ValueLocation encoding to return raw user values
fn collect_snapshot_iter(iter: &mut SnapshotIterator) -> Result<Vec<(InternalKey, Vec<u8>)>> {
	iter.seek_first()?;
	let mut result = Vec::new();
	while iter.valid() {
		let encoded_value = iter.value_encoded()?;
		let decoded_value = ValueLocation::decode(encoded_value)?.value;
		result.push((iter.key().to_owned(), decoded_value));
		if !iter.next()? {
			break;
		}
	}
	Ok(result)
}

/// Collects entries from a SnapshotIterator in reverse order
/// Decodes ValueLocation encoding to return raw user values
fn collect_snapshot_reverse(iter: &mut SnapshotIterator) -> Result<Vec<(InternalKey, Vec<u8>)>> {
	iter.seek_last()?;
	let mut result = Vec::new();
	while iter.valid() {
		let encoded_value = iter.value_encoded()?;
		let decoded_value = ValueLocation::decode(encoded_value)?.value;
		result.push((iter.key().to_owned(), decoded_value));
		if !iter.prev()? {
			break;
		}
	}
	Ok(result)
}
