//! Integration tests for SurrealKV
//!
//! This module contains integration tests that test the full LSM stack
//! including WAL recovery, memtable flush, SST interaction, and manifest
//! coordination.

use std::collections::HashMap;
use std::sync::Arc;

use crate::cache::BlockCache;
use crate::paths::PathResolver;
use crate::snapshot::SnapshotIterator;
use crate::sstable::sst_id::SstId;
use crate::sstable::table::Table;
use crate::tablestore::TableStore;
use crate::{InternalKey, Key, LSMIterator, Options, Result, Value};

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
pub mod store_tests;
#[cfg(test)]
pub mod version_iterator_tests;
#[cfg(test)]
pub mod wal_tests;

/// Creates a deterministic SstId from a u64 for use in tests.
pub(crate) fn test_sst_id(n: u64) -> SstId {
	ulid::Ulid::from_parts(n, 0)
}

/// Creates an in-memory TableStore for testing.
/// Uses object_store::memory::InMemory as the backing store.
pub(crate) fn test_table_store() -> Arc<TableStore> {
	let store = object_store::memory::InMemory::new();
	let path_resolver = PathResolver::new("");
	let block_cache = Arc::new(BlockCache::with_capacity_bytes(64 * 1024 * 1024));
	Arc::new(TableStore::new(Arc::new(store), path_resolver, block_cache))
}

/// Builds a Table from raw bytes (written by TableWriter) using an in-memory object store.
/// This is the async replacement for the old sync `Table::new(id, opts, wrap_buffer(buf), size)`.
pub(crate) async fn new_test_table(id: SstId, opts: Arc<Options>, data: Vec<u8>) -> Result<Table> {
	let file_size = data.len() as u64;
	let table_store = test_table_store();
	table_store.write_sst(&id, bytes::Bytes::from(data)).await?;
	Table::new(id, opts, table_store, file_size).await
}

/// Collects all (key, value) pairs from an LSMIterator into a Vec
/// Assumes iterator is already positioned (e.g., after seek_first or seek)
async fn collect_iter(iter: &mut impl LSMIterator) -> Vec<(InternalKey, Vec<u8>)> {
	let mut result = Vec::new();
	while iter.valid() {
		result.push((iter.key().to_owned(), iter.value_encoded().unwrap().to_vec()));
		if !iter.next().await.unwrap_or(false) {
			break;
		}
	}
	result
}

/// Collects all items from start (seek_first) to end
async fn collect_all(iter: &mut impl LSMIterator) -> Result<Vec<(InternalKey, Vec<u8>)>> {
	iter.seek_first().await?;
	Ok(collect_iter(iter).await)
}

/// Counts entries in an iterator
async fn count_iter(iter: &mut impl LSMIterator) -> Result<usize> {
	iter.seek_first().await?;
	let mut count = 0;
	while iter.valid() {
		count += 1;
		if !iter.next().await? {
			break;
		}
	}
	Ok(count)
}

/// Collects all entries from a TransactionIterator
async fn collect_transaction_iter(iter: &mut impl LSMIterator) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	let mut result = Vec::new();
	while iter.valid() {
		let key = iter.key().user_key().to_vec();
		let value = iter.value()?;
		result.push((key, value));
		iter.next().await?;
	}
	Ok(result)
}

/// Collects all entries from a TransactionIterator starting from seek_first()
async fn collect_transaction_all(iter: &mut impl LSMIterator) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	iter.seek_first().await?;
	collect_transaction_iter(iter).await
}

/// Collects all entries from a TransactionIterator in reverse order starting from seek_last()
async fn collect_transaction_reverse(
	iter: &mut impl LSMIterator,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
	iter.seek_last().await?;
	let mut result = Vec::new();
	while iter.valid() {
		let key = iter.key().user_key().to_vec();
		let value = iter.value()?;
		result.push((key, value));
		if !iter.prev().await? {
			break;
		}
	}
	Ok(result)
}

/// Collects all entries from a SnapshotIterator starting from seek_first()
async fn collect_snapshot_iter(
	iter: &mut SnapshotIterator<'_>,
) -> Result<Vec<(InternalKey, Vec<u8>)>> {
	iter.seek_first().await?;
	let mut result = Vec::new();
	while iter.valid() {
		let value = iter.value_encoded()?.to_vec();
		result.push((iter.key().to_owned(), value));
		if !iter.next().await? {
			break;
		}
	}
	Ok(result)
}

/// Collects entries from a SnapshotIterator in reverse order
async fn collect_snapshot_reverse(
	iter: &mut SnapshotIterator<'_>,
) -> Result<Vec<(InternalKey, Vec<u8>)>> {
	iter.seek_last().await?;
	let mut result = Vec::new();
	while iter.valid() {
		let value = iter.value_encoded()?.to_vec();
		result.push((iter.key().to_owned(), value));
		if !iter.prev().await? {
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
async fn collect_history_all(
	iter: &mut impl LSMIterator,
) -> crate::Result<Vec<(Key, Value, u64, bool)>> {
	iter.seek_first().await?;
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
		iter.next().await?;
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
async fn point_in_time_from_history(
	iter: &mut impl LSMIterator,
	timestamp: u64,
) -> crate::Result<Vec<(Key, Value)>> {
	use std::collections::BTreeMap;

	iter.seek_first().await?;
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

		iter.next().await?;
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
