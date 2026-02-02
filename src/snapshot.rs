use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::File;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use parking_lot::RwLockReadGuard;

use crate::bplustree::tree::{BPlusTreeIterator, DiskBPlusTree};
use crate::error::{Error, Result};
use crate::iter::BoxedInternalIterator;
use crate::levels::Levels;
use crate::lsm::Core;
use crate::memtable::MemTable;
use crate::{
	Comparator,
	InternalIterator,
	InternalKey,
	InternalKeyComparator,
	InternalKeyKind,
	InternalKeyRange,
	InternalKeyRef,
	IntoBytes,
	Key,
	Value,
};

/// Type alias for version scan results
pub type VersionScanResult = (Key, Value, u64, bool);

// ===== Snapshot Counter =====
/// Tracks the number of active snapshots in the system.
///
/// Important for garbage collection: old versions can only be removed
/// when no snapshot needs them. This counter helps determine when it's
/// safe to compact away old versions during compaction.
///
/// TODO: This check needs to be implemented in the compaction logic.
#[derive(Clone, Debug, Default)]
pub(crate) struct Counter(Arc<AtomicU32>);

impl std::ops::Deref for Counter {
	type Target = Arc<AtomicU32>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Counter {
	/// Increments when a new snapshot is created
	pub(crate) fn increment(&self) -> u32 {
		self.fetch_add(1, std::sync::atomic::Ordering::Release)
	}

	/// Decrements when a snapshot is dropped
	pub(crate) fn decrement(&self) -> u32 {
		self.fetch_sub(1, std::sync::atomic::Ordering::Release)
	}
}

// ===== Iterator State =====
/// Holds references to all LSM tree components needed for iteration.
pub(crate) struct IterState {
	/// The active memtable receiving current writes
	pub active: Arc<MemTable>,
	/// Immutable memtables waiting to be flushed
	pub immutable: Vec<Arc<MemTable>>,
	/// All levels containing SSTables
	pub levels: Levels,
}

/// Query parameters for versioned range queries
#[derive(Debug, Clone)]
pub(crate) struct VersionedRangeQueryParams<'a, R: RangeBounds<Vec<u8>> + 'a> {
	pub(crate) key_range: &'a R,
	pub(crate) start_ts: u64,
	pub(crate) end_ts: u64,
	pub(crate) snapshot_seq_num: u64,
	pub(crate) limit: Option<usize>,
	pub(crate) include_tombstones: bool,
	pub(crate) include_latest_only: bool, // When true, only include the latest version of each key
}

// ===== Snapshot Implementation =====
/// A consistent point-in-time view of the LSM tree.
///
/// # Snapshot Isolation in LSM Trees
///
/// Snapshots provide consistent reads by fixing a sequence number at creation
/// time. All reads through the snapshot only see data with sequence numbers
/// less than or equal to the snapshot's sequence number.
#[derive(Clone)]
pub(crate) struct Snapshot {
	/// Reference to the LSM tree core
	core: Arc<Core>,

	/// Sequence number defining this snapshot's view of the data
	/// Only data with seq_num <= this value is visible
	pub(crate) seq_num: u64,
}

impl Snapshot {
	/// Creates a new snapshot at the current sequence number
	pub(crate) fn new(core: Arc<Core>, seq_num: u64) -> Self {
		// Increment counter so compaction knows to preserve old versions
		core.snapshot_counter.increment();
		Self {
			core,
			seq_num,
		}
	}

	/// Collects the iterator state from all LSM components
	/// This is a helper method used by both iterators and optimized operations
	/// like count
	pub(crate) fn collect_iter_state(&self) -> Result<IterState> {
		let active = guardian::ArcRwLockReadGuardian::take(Arc::clone(&self.core.active_memtable))?;
		let immutable =
			guardian::ArcRwLockReadGuardian::take(Arc::clone(&self.core.immutable_memtables))?;
		let manifest =
			guardian::ArcRwLockReadGuardian::take(Arc::clone(&self.core.level_manifest))?;

		Ok(IterState {
			active: active.clone(),
			immutable: immutable.iter().map(|entry| Arc::clone(&entry.memtable)).collect(),
			levels: manifest.levels.clone(),
		})
	}

	/// Gets a single key from the snapshot.
	///
	/// # Read Path in LSM Trees
	///
	/// The read path checks multiple locations in order:
	/// 1. **Active Memtable**: Most recent writes, in memory
	/// 2. **Immutable Memtables**: Recent writes being flushed
	/// 3. **Level**: From SSTables
	///
	/// The search stops at the first version found with seq_num <= snapshot
	/// seq_num.
	pub(crate) fn get(&self, key: &[u8]) -> crate::Result<Option<(Value, u64)>> {
		// self.core.get_internal(key, self.seq_num)
		// Read lock on the active memtable
		let memtable_lock = self.core.active_memtable.read()?;

		// Check the active memtable for the key
		if let Some(item) = memtable_lock.get(key.as_ref(), Some(self.seq_num)) {
			if item.0.is_tombstone() {
				return Ok(None); // Key is a tombstone, return None
			}
			return Ok(Some((item.1, item.0.seq_num()))); // Key found, return the value
		}
		drop(memtable_lock); // Release the lock on the active memtable

		// Read lock on the immutable memtables
		let memtable_lock = self.core.immutable_memtables.read()?;

		// Check the immutable memtables for the key
		for entry in memtable_lock.iter().rev() {
			let memtable = &entry.memtable;
			if let Some(item) = memtable.get(key.as_ref(), Some(self.seq_num)) {
				if item.0.is_tombstone() {
					return Ok(None); // Key is a tombstone, return None
				}
				return Ok(Some((item.1, item.0.seq_num()))); // Key found, return the value
			}
		}
		drop(memtable_lock); // Release the lock on the immutable memtables

		// Read lock on the level manifest
		let level_manifest = self.core.level_manifest.read()?;

		let ikey = InternalKey::new(key.to_vec(), self.seq_num, InternalKeyKind::Set, 0);

		// Check the tables in each level for the key
		for (level_idx, level) in (&level_manifest.levels).into_iter().enumerate() {
			if level_idx == 0 {
				// Level 0: Tables can overlap, check all
				for table in level.tables.iter() {
					if !table.is_key_in_key_range(&ikey) {
						continue; // Skip this table if the key is not in its range
					}

					let maybe_item = table.get(&ikey)?;

					if let Some(item) = maybe_item {
						let ikey = &item.0;
						if ikey.is_tombstone() {
							return Ok(None); // Key is a tombstone, return None
						}
						return Ok(Some((item.1, ikey.seq_num()))); // Key found, return the value
					}
				}
			} else {
				// Level 1+: Non-overlapping, binary search for the one table
				let query_range =
					crate::user_range_to_internal_range(Bound::Included(key), Bound::Included(key));
				let start_idx = level.find_first_overlapping_table(&query_range);
				let end_idx = level.find_last_overlapping_table(&query_range);

				// At most one table can contain this exact key
				for table in &level.tables[start_idx..end_idx] {
					let maybe_item = table.get(&ikey)?;

					if let Some(item) = maybe_item {
						let ikey = &item.0;
						if ikey.is_tombstone() {
							return Ok(None); // Key is a tombstone, return None
						}
						return Ok(Some((item.1, ikey.seq_num()))); // Key found, return the value
					}
				}
			}
		}

		Ok(None) // Key not found in any memtable or table, return None
	}

	/// Creates an iterator for a range scan within the snapshot
	/// Returns a SnapshotIterator that implements InternalIterator
	pub(crate) fn range(
		&self,
		lower: Option<&[u8]>,
		upper: Option<&[u8]>,
	) -> Result<SnapshotIterator<'_>> {
		let internal_range = crate::user_range_to_internal_range(
			lower.map(Bound::Included).unwrap_or(Bound::Unbounded),
			upper.map(Bound::Excluded).unwrap_or(Bound::Unbounded),
		);
		SnapshotIterator::new_from(Arc::clone(&self.core), self.seq_num, internal_range)
	}

	/// Creates an iterator that returns ALL versions of keys in the range
	/// from the LSM tree directly (memtables + SSTables).
	/// Unlike `range()` which returns only the latest version per key,
	/// this returns every visible version in (user_key, seq_num descending) order.
	///
	/// This method queries the LSM index directly and does not require the
	/// B+tree versioned index to be enabled.
	pub(crate) fn versioned_range(
		&self,
		lower: Option<&[u8]>,
		upper: Option<&[u8]>,
		include_tombstones: bool,
	) -> Result<VersionedSnapshotIterator<'_>> {
		let internal_range = crate::user_range_to_internal_range(
			lower.map(Bound::Included).unwrap_or(Bound::Unbounded),
			upper.map(Bound::Excluded).unwrap_or(Bound::Unbounded),
		);
		VersionedSnapshotIterator::new_from(
			Arc::clone(&self.core),
			self.seq_num,
			internal_range,
			include_tombstones,
		)
	}

	/// Creates a streaming B+tree iterator for versioned queries.
	///
	/// This method returns a true streaming iterator over the B+tree versioned index,
	/// without collecting results into memory. Requires the B+tree versioned index
	/// to be enabled.
	///
	/// # Arguments
	/// * `lower` - Optional lower bound key (inclusive)
	/// * `upper` - Optional upper bound key (exclusive)
	///
	/// # Errors
	/// Returns an error if the B+tree versioned index is not enabled.
	pub(crate) fn btree_history_iter(
		&self,
		_lower: Option<&[u8]>,
		_upper: Option<&[u8]>,
	) -> Result<BPlusTreeIteratorWithGuard<'_>> {
		if !self.core.opts.enable_versioned_index {
			return Err(Error::InvalidArgument("B+tree versioned index not enabled".to_string()));
		}

		let versioned_index =
			self.core.versioned_index.as_ref().ok_or_else(|| {
				Error::InvalidArgument("No versioned index available".to_string())
			})?;

		BPlusTreeIteratorWithGuard::new(versioned_index)
	}

	/// Queries for a specific key at a specific timestamp.
	/// Only returns data visible to this snapshot (seq_num <= snapshot.seq_num).
	///
	/// If the B+tree versioned index is enabled, queries it directly.
	/// Otherwise, falls back to scanning the LSM tree.
	pub(crate) fn get_at_version(&self, key: &[u8], timestamp: u64) -> Result<Option<Value>> {
		// Try B+tree first if available
		if self.core.opts.enable_versioned_index {
			// Create a range that includes only the specific key
			let key_range = key.to_vec()..=key.to_vec();

			let mut versioned_iter = self.versioned_range_iter(VersionedRangeQueryParams {
				key_range: &key_range,
				start_ts: 0, // Start from beginning of time
				end_ts: timestamp,
				snapshot_seq_num: self.seq_num,
				limit: None,
				include_tombstones: false, // Don't include tombstones
				include_latest_only: true,
			})?;

			// Get the latest version (should be only one due to include_latest_only: true)
			if let Some((_internal_key, encoded_value)) = versioned_iter.next() {
				return Ok(Some(self.core.resolve_value(&encoded_value)?));
			} else {
				return Ok(None);
			}
		}

		// Fallback to LSM query if versioning is enabled but B+tree index is not
		if self.core.opts.enable_versioning {
			return self.get_at_version_from_lsm(key, timestamp);
		}

		Err(Error::InvalidArgument("Versioning not enabled".to_string()))
	}

	/// Queries the LSM tree directly for a specific key at a specific timestamp.
	/// Used as fallback when B+tree index is not available.
	fn get_at_version_from_lsm(&self, key: &[u8], timestamp: u64) -> Result<Option<Value>> {
		// Create a range for just this key
		// We need to find the version with timestamp <= requested timestamp
		let mut iter = self.versioned_range(Some(key), None, true)?;

		iter.seek_first()?;

		// Track the best match (latest version at or before requested timestamp)
		let mut best_value: Option<Value> = None;
		let mut best_timestamp: u64 = 0;

		while iter.valid() {
			let entry_key = iter.key();
			let entry_user_key = entry_key.user_key();

			// Stop if we've moved past our key
			if entry_user_key != key {
				break;
			}

			let entry_ts = entry_key.timestamp();

			// Only consider versions at or before the requested timestamp
			if entry_ts <= timestamp && entry_ts >= best_timestamp {
				if entry_key.is_tombstone() {
					// Key was deleted at this timestamp
					best_value = None;
					best_timestamp = entry_ts;
				} else {
					best_value = Some(self.core.resolve_value(iter.value())?);
					best_timestamp = entry_ts;
				}
			}

			iter.next()?;
		}

		Ok(best_value)
	}

	/// Gets keys in a key range at a specific timestamp
	/// Only returns data visible to this snapshot (seq_num <= snapshot.seq_num)
	/// Range is [start, end) - start is inclusive, end is exclusive.
	pub(crate) fn keys_at_version<Key: AsRef<[u8]>>(
		&self,
		start: Key,
		end: Key,
		timestamp: u64,
	) -> Result<impl DoubleEndedIterator<Item = Vec<u8>> + '_> {
		let key_range = start.as_ref().to_vec()..end.as_ref().to_vec();
		let versioned_iter = self.versioned_range_iter(VersionedRangeQueryParams {
			key_range: &key_range,
			start_ts: 0,
			end_ts: timestamp,
			snapshot_seq_num: self.seq_num,
			limit: None,
			include_tombstones: false, // Don't include tombstones
			include_latest_only: true,
		})?;

		Ok(KeysAtTimestampIterator {
			inner: versioned_iter,
		})
	}

	/// Scans key-value pairs in a key range at a specific timestamp
	/// Only returns data visible to this snapshot (seq_num <= snapshot.seq_num)
	/// Range is [start, end) - start is inclusive, end is exclusive.
	pub(crate) fn range_at_version<Key: AsRef<[u8]>>(
		&self,
		start: Key,
		end: Key,
		timestamp: u64,
	) -> Result<impl DoubleEndedIterator<Item = Result<(Vec<u8>, Value)>> + '_> {
		let key_range = start.as_ref().to_vec()..end.as_ref().to_vec();
		let versioned_iter = self.versioned_range_iter(VersionedRangeQueryParams {
			key_range: &key_range,
			start_ts: 0,
			end_ts: timestamp,
			snapshot_seq_num: self.seq_num,
			limit: None,
			include_tombstones: false, // Don't include tombstones
			include_latest_only: true,
		})?;

		Ok(ScanAtTimestampIterator {
			inner: versioned_iter,
			core: Arc::clone(&self.core),
		})
	}

	/// Gets all versions of keys in a key range using the B+tree versioned index.
	/// Only returns data visible to this snapshot (seq_num <= snapshot.seq_num)
	/// Range is [start, end) - start is inclusive, end is exclusive.
	///
	/// Note: This method requires the B+tree versioned index to be enabled.
	/// For LSM-based versioned iteration, use `versioned_range()` instead.
	///
	/// # Arguments
	/// * `start` - Start key (inclusive)
	/// * `end` - End key (exclusive)
	/// * `limit` - Optional maximum number of versions to return. If None, returns all versions.
	pub(crate) fn scan_all_versions<Key: IntoBytes>(
		&self,
		start: Key,
		end: Key,
		limit: Option<usize>,
	) -> Result<Vec<VersionScanResult>> {
		if !self.core.opts.enable_versioned_index {
			return Err(Error::InvalidArgument(
				"B+tree versioned index not enabled. Use versioned_range() for LSM-based iteration."
					.to_string(),
			));
		}

		let key_range = start.as_slice().to_vec()..end.as_slice().to_vec();
		let versioned_iter = self.versioned_range_iter(VersionedRangeQueryParams {
			key_range: &key_range,
			start_ts: 0,
			end_ts: u64::MAX,
			snapshot_seq_num: self.seq_num,
			limit,
			include_tombstones: true,
			include_latest_only: false, // Include all versions for scan_all_versions
		})?;

		let mut results = Vec::new();
		for (internal_key, encoded_value) in versioned_iter {
			let is_tombstone = internal_key.is_tombstone();
			let value = if is_tombstone {
				Value::default() // Use default value for soft delete markers
			} else {
				self.core.resolve_value(&encoded_value)?
			};
			results.push((
				internal_key.user_key.clone(),
				value,
				internal_key.timestamp,
				is_tombstone,
			));
		}

		Ok(results)
	}

	/// Encodes a user key bound into an InternalKey bound, preserving Included/Excluded/Unbounded
	fn encode_start_bound(
		bound: Bound<&Vec<u8>>,
		seq_num: u64,
		kind: InternalKeyKind,
		ts: u64,
	) -> Bound<Vec<u8>> {
		match bound {
			Bound::Included(key) => {
				Bound::Included(InternalKey::new(key.clone(), seq_num, kind, ts).encode())
			}
			Bound::Excluded(key) => {
				Bound::Excluded(InternalKey::new(key.clone(), seq_num, kind, ts).encode())
			}
			Bound::Unbounded => Bound::Unbounded,
		}
	}

	/// Encodes an end bound for versioned queries
	fn encode_end_bound(
		bound: Bound<&Vec<u8>>,
		seq_num: u64,
		kind: InternalKeyKind,
		ts: u64,
	) -> Bound<Vec<u8>> {
		match bound {
			Bound::Included(key) => {
				Bound::Included(InternalKey::new(key.clone(), seq_num, kind, ts).encode())
			}
			Bound::Excluded(key) => {
				// For excluded bounds, use minimal InternalKey properties so range stops just
				// before this key
				Bound::Excluded(InternalKey::new(key.clone(), 0, InternalKeyKind::Set, 0).encode())
			}
			Bound::Unbounded => Bound::Unbounded,
		}
	}

	/// Converts Bound<Vec<u8>> to Bound<&[u8]> for passing to B+tree
	fn bound_as_slice(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
		match bound {
			Bound::Included(v) => Bound::Included(v.as_slice()),
			Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
			Bound::Unbounded => Bound::Unbounded,
		}
	}

	/// Creates a versioned range iterator that implements DoubleEndedIterator.
	/// This method queries the B+tree versioned index.
	///
	/// Note: This method requires the B+tree versioned index to be enabled.
	/// For LSM-based versioned iteration, use `versioned_range()` instead.
	///
	/// TODO: This is a temporary solution to avoid the complexity of
	/// implementing a proper streaming double ended iterator, which will be
	/// fixed in the future.
	pub(crate) fn versioned_range_iter<R: RangeBounds<Vec<u8>>>(
		&self,
		params: VersionedRangeQueryParams<'_, R>,
	) -> Result<VersionedRangeIterator> {
		if !self.core.opts.enable_versioned_index {
			return Err(Error::InvalidArgument("B+tree versioned index not enabled".to_string()));
		}

		let mut results = Vec::new();

		if let Some(ref versioned_index) = self.core.versioned_index {
			let index_guard = versioned_index.read();

			// Step 1: Encode bounds (preserves bound type)
			let start_bound = Self::encode_start_bound(
				params.key_range.start_bound(),
				0,
				InternalKeyKind::Set,
				params.start_ts,
			);
			let end_bound = Self::encode_end_bound(
				params.key_range.end_bound(),
				params.snapshot_seq_num,
				InternalKeyKind::Max,
				params.end_ts,
			);

			// Step 2: Convert to slice bounds and call range
			let range = (Self::bound_as_slice(&start_bound), Self::bound_as_slice(&end_bound));
			let range_iter = index_guard.range(range)?;

			// Collect all versions by key (already in timestamp order from B+tree)
			// Store the complete InternalKey to preserve all original information
			// Use BTreeMap to maintain keys in sorted order
			let mut key_versions: BTreeMap<Vec<u8>, Vec<(InternalKey, Vec<u8>)>> = BTreeMap::new();

			for entry in range_iter {
				let (encoded_key, encoded_value) = entry?;
				let internal_key = InternalKey::decode(&encoded_key);
				assert!(internal_key.seq_num() <= params.snapshot_seq_num);

				if internal_key.timestamp > params.end_ts {
					continue;
				}

				let current_key = internal_key.user_key.clone();

				key_versions
					.entry(current_key)
					.or_default()
					.push((internal_key, encoded_value.to_vec()));
			}

			// Filter out keys where the latest version is a hard delete
			key_versions.retain(|_, versions| {
				let latest_version = versions.last().unwrap();
				!latest_version.0.is_hard_delete_marker()
			});

			// Process each key's versions
			let max_unique_keys = params.limit.unwrap_or(usize::MAX);

			for (unique_key_count, (_user_key, mut versions)) in
				key_versions.into_iter().enumerate()
			{
				// Check if we've reached the limit of unique keys
				if unique_key_count >= max_unique_keys {
					break;
				}

				// If include_latest_only is true, keep only the latest version (highest
				// timestamp)
				if params.include_latest_only && !versions.is_empty() {
					// B+tree already provides entries in timestamp order, so just take the last
					// element (highest timestamp)
					let latest_version = versions.pop().unwrap();
					versions = vec![latest_version];
				}

				// Determine which versions to output
				let versions_to_output: Vec<_> = if params.include_tombstones {
					// For scan_all_versions, include ALL versions (both values and tombstones)
					versions.into_iter().collect()
				} else {
					// Otherwise, only include non-tombstone versions
					versions.into_iter().filter(|version| !version.0.is_tombstone()).collect()
				};

				// Collect the filtered versions with original InternalKey preserved
				for (internal_key, encoded_value) in versions_to_output {
					results.push((internal_key, encoded_value));
				}
			}
		}

		Ok(VersionedRangeIterator {
			results,
			index: 0,
		})
	}
}

/// A DoubleEndedIterator for versioned range queries
pub(crate) struct VersionedRangeIterator {
	/// All collected results from the versioned query
	results: Vec<(InternalKey, Vec<u8>)>,
	/// Current position in the results (0-based index)
	index: usize,
}

impl Iterator for VersionedRangeIterator {
	type Item = (InternalKey, Vec<u8>);

	fn next(&mut self) -> Option<Self::Item> {
		if self.index < self.results.len() {
			let result = self.results[self.index].clone();
			self.index += 1;
			Some(result)
		} else {
			None
		}
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		let remaining = self.results.len().saturating_sub(self.index);
		(remaining, Some(remaining))
	}
}

impl DoubleEndedIterator for VersionedRangeIterator {
	fn next_back(&mut self) -> Option<Self::Item> {
		if self.index < self.results.len() {
			let result = self.results[self.results.len() - 1].clone();
			self.results.pop();
			Some(result)
		} else {
			None
		}
	}
}

impl ExactSizeIterator for VersionedRangeIterator {
	fn len(&self) -> usize {
		self.results.len().saturating_sub(self.index)
	}
}

/// Iterator for keys at a specific timestamp
pub(crate) struct KeysAtTimestampIterator {
	inner: VersionedRangeIterator,
}

impl Iterator for KeysAtTimestampIterator {
	type Item = Vec<u8>;

	fn next(&mut self) -> Option<Self::Item> {
		self.inner.next().map(|(internal_key, _)| internal_key.user_key)
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.inner.size_hint()
	}
}

impl DoubleEndedIterator for KeysAtTimestampIterator {
	fn next_back(&mut self) -> Option<Self::Item> {
		self.inner.next_back().map(|(internal_key, _)| internal_key.user_key)
	}
}

impl ExactSizeIterator for KeysAtTimestampIterator {
	fn len(&self) -> usize {
		self.inner.len()
	}
}

/// Iterator for key-value pairs at a specific timestamp
pub(crate) struct ScanAtTimestampIterator {
	inner: VersionedRangeIterator,
	core: Arc<Core>,
}

impl Iterator for ScanAtTimestampIterator {
	type Item = Result<(Vec<u8>, Value)>;

	fn next(&mut self) -> Option<Self::Item> {
		self.inner.next().map(|(internal_key, encoded_value)| {
			match self.core.resolve_value(&encoded_value) {
				Ok(resolved_value) => Ok((internal_key.user_key, resolved_value)),
				Err(e) => Err(e), // Return the error instead of skipping
			}
		})
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.inner.size_hint()
	}
}

impl DoubleEndedIterator for ScanAtTimestampIterator {
	fn next_back(&mut self) -> Option<Self::Item> {
		self.inner.next_back().map(|(internal_key, encoded_value)| {
			match self.core.resolve_value(&encoded_value) {
				Ok(resolved_value) => Ok((internal_key.user_key, resolved_value)),
				Err(e) => Err(e), // Return the error instead of skipping
			}
		})
	}
}

impl ExactSizeIterator for ScanAtTimestampIterator {
	fn len(&self) -> usize {
		self.inner.len()
	}
}

impl Drop for Snapshot {
	fn drop(&mut self) {
		// Decrement counter so compaction can clean up old versions
		self.core.snapshot_counter.decrement();
	}
}

/// Direction of iteration for KMergeIterator
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum MergeDirection {
	Forward,
	Backward,
}

/// A merge iterator that sorts by key+seqno.
/// Uses index-based tracking for zero-allocation iteration.
pub(crate) struct KMergeIterator<'iter> {
	/// Array of iterators to merge over.
	///
	/// IMPORTANT: Due to self-referential structs, this must be defined before
	/// `iter_state` in order to ensure it is dropped before `iter_state`.
	iterators: Vec<BoxedInternalIterator<'iter>>,

	// Owned state
	#[allow(dead_code)]
	iter_state: Box<IterState>,

	/// Current winner index (None if exhausted)
	winner: Option<usize>,

	/// Number of active (valid) iterators
	active_count: usize,

	/// Direction of iteration
	direction: MergeDirection,

	/// Whether the iterator has been initialized
	initialized: bool,

	/// Comparator for key comparison
	cmp: Arc<InternalKeyComparator>,
}

impl<'a> KMergeIterator<'a> {
	pub(crate) fn new_from(iter_state: IterState, internal_range: InternalKeyRange) -> Self {
		let boxed_state = Box::new(iter_state);

		let query_range = Arc::new(internal_range);

		// Pre-allocate capacity for the iterators.
		// 1 active memtable + immutable memtables + level tables.
		let mut iterators: Vec<BoxedInternalIterator<'a>> =
			Vec::with_capacity(1 + boxed_state.immutable.len() + boxed_state.levels.total_tables());

		let state_ref: &'a IterState = unsafe { &*(&*boxed_state as *const IterState) };

		// Extract user key bounds from InternalKeyRange (inclusive lower, exclusive
		// upper)
		let (start_bound, end_bound) = query_range.as_ref();
		let lower = match start_bound {
			Bound::Included(key) | Bound::Excluded(key) => Some(key.user_key.as_slice()),
			Bound::Unbounded => None,
		};
		let upper = match end_bound {
			Bound::Excluded(key) => Some(key.user_key.as_slice()),
			Bound::Included(_) | Bound::Unbounded => None, /* Included upper handled by table
			                                                * iterators */
		};

		// Active memtable
		let active_iter = state_ref.active.range(lower, upper);
		iterators.push(Box::new(active_iter) as BoxedInternalIterator<'a>);

		// Immutable memtables
		for memtable in &state_ref.immutable {
			let iter = memtable.range(lower, upper);
			iterators.push(Box::new(iter) as BoxedInternalIterator<'a>);
		}

		// Tables - these have native seek support
		for (level_idx, level) in (&state_ref.levels).into_iter().enumerate() {
			// Optimization: Skip tables that are completely outside the query range
			if level_idx == 0 {
				// Level 0: Tables can overlap, so we check all but skip those completely
				// outside range
				for table in &level.tables {
					// Skip tables completely before or after the range
					if table.is_before_range(&query_range) || table.is_after_range(&query_range) {
						continue;
					}
					if let Ok(table_iter) = table.iter(Some((*query_range).clone())) {
						iterators.push(Box::new(table_iter) as BoxedInternalIterator<'a>);
					}
				}
			} else {
				// Level 1+: Tables have non-overlapping key ranges, use binary search
				let start_idx = level.find_first_overlapping_table(&query_range);
				let end_idx = level.find_last_overlapping_table(&query_range);

				for table in &level.tables[start_idx..end_idx] {
					if let Ok(table_iter) = table.iter(Some((*query_range).clone())) {
						iterators.push(Box::new(table_iter) as BoxedInternalIterator<'a>);
					}
				}
			}
		}

		Self {
			iterators,
			iter_state: boxed_state,
			winner: None,
			active_count: 0,
			direction: MergeDirection::Forward,
			initialized: false,
			cmp: Arc::new(InternalKeyComparator::new(Arc::new(
				crate::BytewiseComparator::default(),
			))),
		}
	}

	/// Compare two iterators by their current key (zero-copy)
	#[inline]
	fn compare(&self, a: usize, b: usize) -> Ordering {
		let iter_a = &self.iterators[a];
		let iter_b = &self.iterators[b];

		let valid_a = iter_a.valid();
		let valid_b = iter_b.valid();

		match (valid_a, valid_b) {
			(false, false) => Ordering::Equal,
			(true, false) => Ordering::Less, // a wins (valid beats invalid)
			(false, true) => Ordering::Greater, // b wins
			(true, true) => {
				// Both valid - compare keys (zero-copy from iterators)
				let key_a = iter_a.key().encoded();
				let key_b = iter_b.key().encoded();
				let ord = self.cmp.compare(key_a, key_b);
				if self.direction == MergeDirection::Backward {
					ord.reverse()
				} else {
					ord
				}
			}
		}
	}

	/// Find the winner (min for forward, max for backward) among all valid iterators
	fn find_winner(&mut self) {
		if self.iterators.is_empty() || self.active_count == 0 {
			self.winner = None;
			return;
		}

		let mut best_idx = None;
		for i in 0..self.iterators.len() {
			if !self.iterators[i].valid() {
				continue;
			}
			match best_idx {
				None => best_idx = Some(i),
				Some(b) => {
					if self.compare(i, b) == Ordering::Less {
						best_idx = Some(i);
					}
				}
			}
		}

		self.winner = best_idx;
	}

	/// Initialize for forward iteration
	fn init_forward(&mut self) -> Result<()> {
		self.direction = MergeDirection::Forward;
		self.active_count = 0;

		// Position all iterators at first
		for iter in &mut self.iterators {
			if iter.seek_first()? {
				self.active_count += 1;
			}
		}

		self.find_winner();
		self.initialized = true;
		Ok(())
	}

	/// Initialize for backward iteration
	fn init_backward(&mut self) -> Result<()> {
		self.direction = MergeDirection::Backward;
		self.active_count = 0;

		// Position all iterators at last
		for iter in &mut self.iterators {
			if iter.seek_last()? {
				self.active_count += 1;
			}
		}

		self.find_winner();
		self.initialized = true;
		Ok(())
	}

	/// Advance the current winner and find new winner
	fn advance_winner(&mut self) -> Result<bool> {
		if self.active_count == 0 || self.winner.is_none() {
			return Ok(false);
		}

		let winner_idx = self.winner.unwrap();
		let iter = &mut self.iterators[winner_idx];

		// Advance the winning iterator
		let still_valid = if self.direction == MergeDirection::Forward {
			iter.next()?
		} else {
			iter.prev()?
		};

		if !still_valid {
			self.active_count = self.active_count.saturating_sub(1);
		}

		// Find new winner
		self.find_winner();

		Ok(self.winner.is_some())
	}

	/// Check if iterator is positioned on a valid entry
	#[inline]
	pub fn is_valid(&self) -> bool {
		self.winner.is_some() && self.iterators[self.winner.unwrap()].valid()
	}
}

impl InternalIterator for KMergeIterator<'_> {
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.active_count = 0;

		for iter in &mut self.iterators {
			if iter.seek(target)? {
				self.active_count += 1;
			}
		}

		self.find_winner();
		self.initialized = true;
		Ok(self.is_valid())
	}

	fn seek_first(&mut self) -> Result<bool> {
		self.init_forward()?;
		Ok(self.is_valid())
	}

	fn seek_last(&mut self) -> Result<bool> {
		self.init_backward()?;
		Ok(self.is_valid())
	}

	fn next(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_first();
		}
		if !self.is_valid() {
			return Ok(false);
		}
		self.advance_winner()
	}

	fn prev(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_last();
		}
		if !self.is_valid() {
			return Ok(false);
		}
		// If we were going forward, switch to backward
		if self.direction != MergeDirection::Backward {
			return self.seek_last();
		}
		self.advance_winner()
	}

	fn valid(&self) -> bool {
		self.is_valid()
	}

	fn key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.is_valid());
		self.iterators[self.winner.unwrap()].key()
	}

	fn value(&self) -> &[u8] {
		debug_assert!(self.is_valid());
		self.iterators[self.winner.unwrap()].value()
	}
}

pub(crate) struct SnapshotIterator<'a> {
	/// The merge iterator
	merge_iter: KMergeIterator<'a>,

	/// Sequence number for visibility
	snapshot_seq_num: u64,

	/// Core for resolving values
	#[allow(dead_code)]
	core: Arc<Core>,

	/// Last user key seen (forward direction) - reusable buffer
	last_key_fwd: Vec<u8>,

	/// For backward iteration: buffered key/value when we've read past current user key
	buffered_back_key: Vec<u8>,
	buffered_back_value: Vec<u8>,
	has_buffered_back: bool,

	/// For backward iteration: the current entry we're returning
	/// (stored because merge_iter has already moved past it)
	current_back_key: Vec<u8>,
	current_back_value: Vec<u8>,
	has_current_back: bool,

	/// Direction of iteration
	direction: MergeDirection,

	/// Whether the iterator has been initialized
	initialized: bool,
}

impl SnapshotIterator<'_> {
	/// Creates a new iterator over a specific key range
	fn new_from(core: Arc<Core>, seq_num: u64, range: InternalKeyRange) -> Result<Self> {
		// Create a temporary snapshot to use the helper method
		let snapshot = Snapshot {
			core: Arc::clone(&core),
			seq_num,
		};
		let iter_state = snapshot.collect_iter_state()?;

		if let Some(ref vlog) = core.vlog {
			vlog.incr_iterator_count();
		}

		let merge_iter = KMergeIterator::new_from(iter_state, range);

		Ok(Self {
			merge_iter,
			snapshot_seq_num: seq_num,
			core,
			last_key_fwd: Vec::new(),
			buffered_back_key: Vec::new(),
			buffered_back_value: Vec::new(),
			has_buffered_back: false,
			current_back_key: Vec::new(),
			current_back_value: Vec::new(),
			has_current_back: false,
			direction: MergeDirection::Forward,
			initialized: false,
		})
	}

	#[inline]
	fn is_visible_ref(&self, key: &InternalKeyRef<'_>) -> bool {
		key.seq_num() <= self.snapshot_seq_num
	}

	/// Skip to the next valid entry in forward direction.
	/// Valid = visible, latest version of user key, not a tombstone.
	fn skip_to_valid_forward(&mut self) -> Result<bool> {
		while self.merge_iter.valid() {
			let key_ref = self.merge_iter.key();

			// Skip invisible versions (seq_num > snapshot)
			if !self.is_visible_ref(&key_ref) {
				self.merge_iter.next()?;
				continue;
			}

			// Skip older versions of same user key
			let user_key = key_ref.user_key();
			if user_key == self.last_key_fwd.as_slice() {
				self.merge_iter.next()?;
				continue;
			}

			// New user key - remember it (reuses buffer capacity)
			self.last_key_fwd.clear();
			self.last_key_fwd.extend_from_slice(user_key);

			// Skip tombstones (but remember we saw this key)
			if key_ref.is_tombstone() {
				self.merge_iter.next()?;
				continue;
			}

			// Found valid entry
			return Ok(true);
		}
		Ok(false)
	}

	/// Skip to the next valid entry in backward direction.
	/// More complex because we see oldest version first, need to find latest visible.
	fn skip_to_valid_backward(&mut self) -> Result<bool> {
		// First check if we have a buffered entry from previous iteration
		if self.has_buffered_back {
			self.has_buffered_back = false;
			// The buffered entry is already the start of a new user key
			// We need to find the latest visible version of this key
			return self.find_latest_visible_backward();
		}

		if self.merge_iter.valid() {
			return self.find_latest_visible_backward();
		}
		self.has_current_back = false;
		Ok(false)
	}

	/// Find the latest visible version of the current user key going backward.
	/// Backward iteration sees oldest version first (lowest seq_num).
	fn find_latest_visible_backward(&mut self) -> Result<bool> {
		if !self.merge_iter.valid() {
			self.has_current_back = false;
			return Ok(false);
		}

		let first_key_ref = self.merge_iter.key();

		// Store the current user key we're examining
		let current_user_key: Vec<u8> = first_key_ref.user_key().to_vec();

		// Track the latest visible version
		let mut latest_key: Option<Vec<u8>> = None;
		let mut latest_value: Option<Vec<u8>> = None;

		// If first entry is visible, it's a candidate
		if self.is_visible_ref(&first_key_ref) {
			latest_key = Some(first_key_ref.encoded().to_vec());
			latest_value = Some(self.merge_iter.value().to_vec());
		}

		// Keep consuming entries with same user key, looking for newer visible versions
		loop {
			self.merge_iter.prev()?;

			if !self.merge_iter.valid() {
				break;
			}

			let key_ref = self.merge_iter.key();
			let user_key = key_ref.user_key();

			if user_key != current_user_key.as_slice() {
				// Different user key - buffer it for next call
				self.buffered_back_key.clear();
				self.buffered_back_key.extend_from_slice(key_ref.encoded());
				self.buffered_back_value.clear();
				self.buffered_back_value.extend_from_slice(self.merge_iter.value());
				self.has_buffered_back = true;
				break;
			}

			// Same user key - check if this is a newer visible version
			if self.is_visible_ref(&key_ref) {
				latest_key = Some(key_ref.encoded().to_vec());
				latest_value = Some(self.merge_iter.value().to_vec());
			}
		}

		// Check if we found a valid (non-tombstone) entry
		if let (Some(key_bytes), Some(value_bytes)) = (latest_key, latest_value) {
			let key_ref = InternalKeyRef::from_encoded(&key_bytes);
			if key_ref.is_tombstone() {
				// Latest visible is tombstone - skip this key, try next
				self.has_current_back = false;
				return self.skip_to_valid_backward();
			}
			// Store the found entry in current_back buffers so valid()/key()/value() work
			self.current_back_key.clear();
			self.current_back_key.extend_from_slice(&key_bytes);
			self.current_back_value.clear();
			self.current_back_value.extend_from_slice(&value_bytes);
			self.has_current_back = true;
			return Ok(true);
		}

		// No visible version found for this key, try next
		self.has_current_back = false;
		self.skip_to_valid_backward()
	}
}

impl InternalIterator for SnapshotIterator<'_> {
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.last_key_fwd.clear();
		self.has_buffered_back = false;
		self.has_current_back = false;
		self.merge_iter.seek(target)?;
		self.initialized = true;
		self.skip_to_valid_forward()
	}

	fn seek_first(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.last_key_fwd.clear();
		self.has_buffered_back = false;
		self.has_current_back = false;
		self.merge_iter.seek_first()?;
		self.initialized = true;
		self.skip_to_valid_forward()
	}

	fn seek_last(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Backward;
		self.has_buffered_back = false;
		self.has_current_back = false;
		self.merge_iter.seek_last()?;
		self.initialized = true;
		self.skip_to_valid_backward()
	}

	fn next(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_first();
		}
		if !self.merge_iter.valid() {
			return Ok(false);
		}
		self.merge_iter.next()?;
		self.skip_to_valid_forward()
	}

	fn prev(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_last();
		}
		// For backward iteration, we can continue if we have a buffered next entry
		// or if the merge_iter is still valid
		if !self.merge_iter.valid() && !self.has_buffered_back {
			self.has_current_back = false;
			return Ok(false);
		}
		// For backward, skip_to_valid_backward handles the logic
		self.skip_to_valid_backward()
	}

	fn valid(&self) -> bool {
		if self.direction == MergeDirection::Backward {
			self.has_current_back
		} else {
			self.merge_iter.valid()
		}
	}

	fn key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.valid());
		if self.direction == MergeDirection::Backward {
			InternalKeyRef::from_encoded(&self.current_back_key)
		} else {
			self.merge_iter.key()
		}
	}

	fn value(&self) -> &[u8] {
		debug_assert!(self.valid());
		if self.direction == MergeDirection::Backward {
			&self.current_back_value
		} else {
			self.merge_iter.value()
		}
	}
}

impl Drop for SnapshotIterator<'_> {
	fn drop(&mut self) {
		// Decrement VLog iterator count when iterator is dropped
		if let Some(ref vlog) = self.core.vlog {
			if let Err(e) = vlog.decr_iterator_count() {
				log::warn!("Failed to decrement VLog iterator count: {e}");
			}
		}
	}
}

/// A snapshot iterator that returns ALL versions of keys, not just the latest.
/// Unlike `SnapshotIterator` which deduplicates (returns only latest version per key),
/// this iterator returns every visible version in (user_key, seq_num descending) order.
///
/// Used for versioned range scans over the LSM tree directly.
pub(crate) struct VersionedSnapshotIterator<'a> {
	/// The merge iterator over all LSM components
	merge_iter: KMergeIterator<'a>,

	/// Sequence number for visibility
	snapshot_seq_num: u64,

	/// Core for resolving values
	#[allow(dead_code)]
	core: Arc<Core>,

	/// Whether to include tombstones in the iteration
	include_tombstones: bool,

	/// Direction of iteration
	direction: MergeDirection,

	/// Whether the iterator has been initialized
	initialized: bool,
}

impl VersionedSnapshotIterator<'_> {
	/// Creates a new versioned iterator over a specific key range
	fn new_from(
		core: Arc<Core>,
		seq_num: u64,
		range: crate::InternalKeyRange,
		include_tombstones: bool,
	) -> Result<Self> {
		// Create a temporary snapshot to use the helper method
		let snapshot = Snapshot {
			core: Arc::clone(&core),
			seq_num,
		};
		let iter_state = snapshot.collect_iter_state()?;

		if let Some(ref vlog) = core.vlog {
			vlog.incr_iterator_count();
		}

		let merge_iter = KMergeIterator::new_from(iter_state, range);

		Ok(Self {
			merge_iter,
			snapshot_seq_num: seq_num,
			core,
			include_tombstones,
			direction: MergeDirection::Forward,
			initialized: false,
		})
	}

	#[inline]
	fn is_visible_ref(&self, key: &InternalKeyRef<'_>) -> bool {
		key.seq_num() <= self.snapshot_seq_num
	}

	/// Skip to the next valid entry in forward direction.
	/// Valid = visible, optionally not a tombstone.
	/// NOTE: Does NOT deduplicate - returns every visible version.
	fn skip_to_valid_forward(&mut self) -> Result<bool> {
		while self.merge_iter.valid() {
			let key_ref = self.merge_iter.key();

			// Skip invisible versions (seq_num > snapshot)
			if !self.is_visible_ref(&key_ref) {
				self.merge_iter.next()?;
				continue;
			}

			// Skip tombstones if not included
			if !self.include_tombstones && key_ref.is_tombstone() {
				self.merge_iter.next()?;
				continue;
			}

			// Found valid entry - no deduplication check!
			return Ok(true);
		}
		Ok(false)
	}

	/// Skip to the next valid entry in backward direction.
	/// NOTE: Does NOT deduplicate - returns every visible version.
	fn skip_to_valid_backward(&mut self) -> Result<bool> {
		while self.merge_iter.valid() {
			let key_ref = self.merge_iter.key();

			// Skip invisible versions (seq_num > snapshot)
			if !self.is_visible_ref(&key_ref) {
				self.merge_iter.prev()?;
				continue;
			}

			// Skip tombstones if not included
			if !self.include_tombstones && key_ref.is_tombstone() {
				self.merge_iter.prev()?;
				continue;
			}

			// Found valid entry - no deduplication check!
			return Ok(true);
		}
		Ok(false)
	}
}

impl InternalIterator for VersionedSnapshotIterator<'_> {
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.merge_iter.seek(target)?;
		self.initialized = true;
		self.skip_to_valid_forward()
	}

	fn seek_first(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.merge_iter.seek_first()?;
		self.initialized = true;
		self.skip_to_valid_forward()
	}

	fn seek_last(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Backward;
		self.merge_iter.seek_last()?;
		self.initialized = true;
		self.skip_to_valid_backward()
	}

	fn next(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_first();
		}
		if !self.merge_iter.valid() {
			return Ok(false);
		}
		self.merge_iter.next()?;
		self.skip_to_valid_forward()
	}

	fn prev(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_last();
		}
		// If we were going forward, switch to backward
		if self.direction != MergeDirection::Backward {
			return self.seek_last();
		}
		if !self.merge_iter.valid() {
			return Ok(false);
		}
		self.merge_iter.prev()?;
		self.skip_to_valid_backward()
	}

	fn valid(&self) -> bool {
		self.merge_iter.valid()
	}

	fn key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.valid());
		self.merge_iter.key()
	}

	fn value(&self) -> &[u8] {
		debug_assert!(self.valid());
		self.merge_iter.value()
	}
}

impl Drop for VersionedSnapshotIterator<'_> {
	fn drop(&mut self) {
		// Decrement VLog iterator count when iterator is dropped
		if let Some(ref vlog) = self.core.vlog {
			if let Err(e) = vlog.decr_iterator_count() {
				log::warn!("Failed to decrement VLog iterator count: {e}");
			}
		}
	}
}

// ===== B+Tree History Iterator =====

/// A streaming iterator over the B+tree versioned index.
///
/// This struct holds both the RwLock read guard and the BPlusTreeIterator together,
/// allowing true streaming iteration without collecting results into memory.
///
/// # Safety
/// This is a self-referential struct. The iterator borrows from the guarded tree.
/// Field declaration order is critical: `iter` MUST be declared before `_guard`
/// to ensure the iterator is dropped before the guard.
pub struct BPlusTreeIteratorWithGuard<'a> {
	/// The iterator borrowing from the guarded tree.
	/// MUST be declared before _guard for correct drop order.
	iter: BPlusTreeIterator<'a, File>,

	/// The read guard that keeps the tree alive.
	/// Dropped AFTER iter due to field declaration order.
	#[allow(dead_code)]
	_guard: RwLockReadGuard<'a, DiskBPlusTree>,
}

impl<'a> BPlusTreeIteratorWithGuard<'a> {
	/// Creates a new streaming B+tree iterator.
	///
	/// # Safety
	/// Uses unsafe to create a self-referential struct. This is safe because:
	/// 1. The guard keeps the tree alive for the lifetime of this struct
	/// 2. The iterator is dropped before the guard (field declaration order)
	/// 3. The tree memory is stable (behind Arc<RwLock<>>)
	pub(crate) fn new(versioned_index: &'a parking_lot::RwLock<DiskBPlusTree>) -> Result<Self> {
		let guard = versioned_index.read();

		// SAFETY: The guard keeps the tree alive for the lifetime of this struct.
		// The iterator is dropped before the guard due to field declaration order.
		let tree_ref: &'a DiskBPlusTree = unsafe { &*(&*guard as *const DiskBPlusTree) };

		let iter = tree_ref.internal_iterator();

		Ok(Self {
			iter,
			_guard: guard,
		})
	}
}

impl InternalIterator for BPlusTreeIteratorWithGuard<'_> {
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.iter.seek(target)
	}

	fn seek_first(&mut self) -> Result<bool> {
		self.iter.seek_first()
	}

	fn seek_last(&mut self) -> Result<bool> {
		self.iter.seek_last()
	}

	fn next(&mut self) -> Result<bool> {
		self.iter.next()
	}

	fn prev(&mut self) -> Result<bool> {
		self.iter.prev()
	}

	fn valid(&self) -> bool {
		self.iter.valid()
	}

	fn key(&self) -> InternalKeyRef<'_> {
		self.iter.key()
	}

	fn value(&self) -> &[u8] {
		self.iter.value()
	}
}
