//! Integration tests for SurrealKV
//!
//! This module contains integration tests that test the full LSM stack
//! including WAL recovery, memtable flush, SST interaction, and manifest
//! coordination.

use crate::snapshot::SnapshotIterator;
use crate::transaction::TransactionIterator;
use crate::vlog::ValueLocation;
use crate::{InternalIterator, InternalKey, Key, Result, Value};

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

/// Collects all (key, value) pairs from an InternalIterator into a Vec
/// Assumes iterator is already positioned (e.g., after seek_first or seek)
fn collect_iter(iter: &mut impl InternalIterator) -> Vec<(InternalKey, Vec<u8>)> {
	let mut result = Vec::new();
	while iter.valid() {
		result.push((iter.key().to_owned(), iter.value().to_vec()));
		if !iter.next().unwrap_or(false) {
			break;
		}
	}
	result
}

/// Collects all items from start (seek_first) to end
fn collect_all(iter: &mut impl InternalIterator) -> Result<Vec<(InternalKey, Vec<u8>)>> {
	iter.seek_first()?;
	Ok(collect_iter(iter))
}

/// Counts entries in an iterator
fn count_iter(iter: &mut impl InternalIterator) -> Result<usize> {
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
fn collect_transaction_iter(iter: &mut TransactionIterator) -> Result<Vec<(Key, Option<Value>)>> {
	let mut result = Vec::new();
	while iter.valid() {
		result.push((iter.key(), iter.value()?));
		iter.next()?;
	}
	Ok(result)
}

/// Collects all entries from a TransactionIterator starting from seek_first()
fn collect_transaction_all(iter: &mut TransactionIterator) -> Result<Vec<(Key, Option<Value>)>> {
	iter.seek_first()?;
	collect_transaction_iter(iter)
}

/// Collects all entries from a TransactionIterator in reverse order starting from seek_last()
fn collect_transaction_reverse(
	iter: &mut TransactionIterator,
) -> Result<Vec<(Key, Option<Value>)>> {
	iter.seek_last()?;
	let mut result = Vec::new();
	while iter.valid() {
		result.push((iter.key(), iter.value()?));
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
		let encoded_value = iter.value();
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
		let encoded_value = iter.value();
		let decoded_value = ValueLocation::decode(encoded_value)?.value;
		result.push((iter.key().to_owned(), decoded_value));
		if !iter.prev()? {
			break;
		}
	}
	Ok(result)
}
