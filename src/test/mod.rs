//! Integration tests for SurrealKV
//!
//! This module contains integration tests that test the full LSM stack
//! including WAL recovery, memtable flush, SST interaction, and manifest
//! coordination.

use std::collections::HashMap;

use crate::snapshot::SnapshotIterator;
use crate::vlog::ValueLocation;
use crate::{InternalKey, Key, LSMIterator, Result, Value};

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
pub mod level_tests;
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
pub mod version_iterator_tests;
#[cfg(test)]
pub mod vlog_tests;
#[cfg(test)]
pub mod wal_tests;

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

/// Type alias for a map of keys to their version information
/// Each key maps to a vector of (value, timestamp, is_tombstone) tuples
#[allow(dead_code)]
type KeyVersionsMap = HashMap<Key, Vec<(Vec<u8>, u64, bool)>>;

/// Collects all entries from a history iterator
/// Returns a vector of (key, value, timestamp, is_tombstone) tuples
#[allow(dead_code)]
fn collect_history_all(iter: &mut impl LSMIterator) -> crate::Result<Vec<(Key, Value, u64, bool)>> {
	iter.seek_first()?;
	let mut result = Vec::new();
	while iter.valid() {
		let key_ref = iter.key();
		let is_tombstone = key_ref.is_tombstone();
		// Tombstones have no value, so use empty vec
		let value = if is_tombstone {
			Vec::new()
		} else {
			iter.value()?
		};
		result.push((key_ref.user_key().to_vec(), value, key_ref.timestamp(), is_tombstone));
		iter.next()?;
	}
	Ok(result)
}

/// Gets a point-in-time snapshot of key-values from a history iterator
/// Returns the latest version of each key at or before the given timestamp
///
/// IMPORTANT: The iterator MUST be created with include_tombstones=true for this
/// function to correctly handle deleted keys. If tombstones are not included,
/// soft-deleted keys will incorrectly appear in the results.
#[allow(dead_code)]
fn point_in_time_from_history(
	iter: &mut impl LSMIterator,
	timestamp: u64,
) -> crate::Result<Vec<(Key, Value)>> {
	use std::collections::BTreeMap;

	iter.seek_first()?;
	// Track the latest entry for each key by timestamp (value, timestamp, is_tombstone)
	let mut latest_entries: BTreeMap<Key, (Option<Value>, u64, bool)> = BTreeMap::new();

	while iter.valid() {
		let key_ref = iter.key();
		let ts = key_ref.timestamp();
		let key = key_ref.user_key().to_vec();
		let is_tombstone = key_ref.is_tombstone();

		// Only consider versions at or before the requested timestamp
		if ts <= timestamp {
			// Check if we need to update this key's entry (higher timestamp wins)
			let should_update = match latest_entries.get(&key) {
				None => true,
				Some((_, existing_ts, _)) => ts > *existing_ts,
			};

			if should_update {
				let value = if is_tombstone {
					None
				} else {
					Some(iter.value()?)
				};
				latest_entries.insert(key.clone(), (value, ts, is_tombstone));
			}
		}

		iter.next()?;
	}

	// Filter out tombstones - keys whose latest entry is a delete
	Ok(latest_entries
		.into_iter()
		.filter_map(|(k, (v, _, is_tombstone))| {
			if is_tombstone {
				None
			} else {
				Some((k, v.unwrap()))
			}
		})
		.collect())
}
