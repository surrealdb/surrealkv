use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};
use std::ptr::NonNull;
use std::sync::{atomic::AtomicU32, Arc};

use crate::error::{Error, Result};
use crate::iter::BoxedIterator;
use crate::levels::Levels;
use crate::lsm::Core;
use crate::memtable::MemTable;
use crate::sstable::{meta::KeyRange, InternalKey, InternalKeyKind};
use crate::{IntoBytes, Key, Value};
use crate::{IterResult, Iterator as LSMIterator, INTERNAL_KEY_SEQ_NUM_MAX};
use bytes::Bytes;

/// Type alias for version scan results
pub type VersionScanResult = (Key, Value, u64, bool);

use double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt};

/// Type alias for versioned entries with key, timestamp, and optional value
use interval_heap::IntervalHeap;

#[derive(Eq)]
struct HeapItem {
	key: Arc<InternalKey>,
	value: Value,
	iterator_index: usize,
}

impl PartialEq for HeapItem {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == Ordering::Equal
	}
}

impl Ord for HeapItem {
	fn cmp(&self, other: &Self) -> Ordering {
		// First compare by user key
		match self.key.user_key.cmp(&other.key.user_key) {
			Ordering::Equal => {
				// Same user key, compare by sequence number in DESCENDING order
				// (higher sequence number = more recent)
				match other.key.seq_num().cmp(&self.key.seq_num()) {
					Ordering::Equal => self.iterator_index.cmp(&other.iterator_index),
					ord => ord,
				}
			}
			ord => ord, // Different user keys
		}
	}
}

impl PartialOrd for HeapItem {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

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
struct VersionedRangeQueryParams<'a, R: RangeBounds<Vec<u8>> + 'a> {
	key_range: &'a R,
	start_ts: u64,
	end_ts: u64,
	snapshot_seq_num: u64,
	include_tombstones: bool,
	include_latest_only: bool, // When true, only include the latest version of each key
}

// ===== Snapshot Implementation =====
/// A consistent point-in-time view of the LSM tree.
///
/// # Snapshot Isolation in LSM Trees
///
/// Snapshots provide consistent reads by fixing a sequence number at creation time.
/// All reads through the snapshot only see data with sequence numbers less than
/// or equal to the snapshot's sequence number.
#[derive(Clone)]
pub(crate) struct Snapshot {
	/// Reference to the LSM tree core
	core: Arc<Core>,

	/// Sequence number defining this snapshot's view of the data
	/// Only data with seq_num <= this value is visible
	seq_num: u64,
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
	/// This is a helper method used by both iterators and optimized operations like count
	pub(crate) fn collect_iter_state(&self) -> Result<IterState> {
		let active = guardian::ArcRwLockReadGuardian::take(self.core.active_memtable.clone())?;
		let immutable =
			guardian::ArcRwLockReadGuardian::take(self.core.immutable_memtables.clone())?;
		let manifest = guardian::ArcRwLockReadGuardian::take(self.core.level_manifest.clone())?;

		Ok(IterState {
			active: active.clone(),
			immutable: immutable.iter().map(|(_, mt)| mt.clone()).collect(),
			levels: manifest.levels.clone(),
		})
	}

	/// Optimized count operation for a key range
	///
	/// This method efficiently counts keys in the range [start, end) without:
	/// - Creating a full iterator
	/// - Resolving values from the value log
	/// - Allocating result structures
	///
	/// It only counts the latest version of each key and skips tombstones.
	pub(crate) fn count_in_range(&self, start: Vec<u8>, end: Vec<u8>) -> Result<usize> {
		let mut count = 0usize;
		let mut last_key: Option<Vec<u8>> = None;

		let iter_state = self.collect_iter_state()?;
		let range = (Bound::Included(start), Bound::Excluded(end));
		let merge_iter = KMergeIterator::new_from(iter_state, self.seq_num, range, true);

		for (key, _value) in merge_iter {
			// Skip tombstones
			if key.kind() == InternalKeyKind::Delete || key.kind() == InternalKeyKind::SoftDelete {
				last_key = Some(key.user_key.as_ref().to_vec());
				continue;
			}

			// Only count latest version of each key
			match &last_key {
				Some(prev_key) if prev_key == key.user_key.as_ref() => {
					continue; // Skip older version
				}
				_ => {
					count += 1;
					last_key = Some(key.user_key.as_ref().to_vec());
				}
			}
		}

		Ok(count)
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
	/// The search stops at the first version found with seq_num <= snapshot seq_num.
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
		for (_, memtable) in memtable_lock.iter().rev() {
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

		// Check the tables in each level for the key
		for level in &level_manifest.levels {
			for table in level.tables.iter() {
				let ikey = InternalKey::new(
					Bytes::copy_from_slice(key),
					self.seq_num,
					InternalKeyKind::Set,
					0,
				);

				if !table.is_key_in_key_range(&ikey) {
					continue; // Skip this table if the key is not in its range
				}

				let maybe_item = table.get(ikey)?;

				if let Some(item) = maybe_item {
					let ikey = &item.0;
					if ikey.is_tombstone() {
						return Ok(None); // Key is a tombstone, return None
					}
					return Ok(Some((item.1, ikey.seq_num()))); // Key found, return the value
				}
			}
		}

		Ok(None) // Key not found in any memtable or table, return None
	}

	/// Creates an iterator for a range scan within the snapshot
	pub(crate) fn range<Key: AsRef<[u8]>>(
		&self,
		start: Key,
		end: Key,
		keys_only: bool,
	) -> Result<impl DoubleEndedIterator<Item = IterResult>> {
		// Create a range from start (inclusive) to end (exclusive)
		let range = start.as_ref().to_vec()..end.as_ref().to_vec();
		SnapshotIterator::new_from(self.core.clone(), self.seq_num, range, keys_only)
	}

	/// Queries the versioned index for a specific key at a specific timestamp
	/// Only returns data visible to this snapshot (seq_num <= snapshot.seq_num)
	pub(crate) fn get_at_version(&self, key: &[u8], timestamp: u64) -> Result<Option<Value>> {
		// Create a range that includes only the specific key
		let key_range = key.to_vec()..=key.to_vec();

		let mut versioned_iter = self.versioned_range_iter(VersionedRangeQueryParams {
			key_range: &key_range,
			start_ts: 0, // Start from beginning of time
			end_ts: timestamp,
			snapshot_seq_num: self.seq_num,
			include_tombstones: false, // Don't include tombstones
			include_latest_only: true,
		})?;

		// Get the latest version (should be only one due to include_latest_only: true)
		if let Some((_internal_key, encoded_value)) = versioned_iter.next() {
			Ok(Some(self.core.resolve_value(&encoded_value)?))
		} else {
			Ok(None)
		}
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
			include_tombstones: false, // Don't include tombstones
			include_latest_only: true,
		})?;

		Ok(ScanAtTimestampIterator {
			inner: versioned_iter,
			core: self.core.clone(),
		})
	}

	/// Gets all versions of keys in a key range
	/// Only returns data visible to this snapshot (seq_num <= snapshot.seq_num)
	/// Range is [start, end) - start is inclusive, end is exclusive.
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
		if !self.core.opts.enable_versioning {
			return Err(Error::InvalidArgument("Versioned queries not enabled".to_string()));
		}

		let key_range = start.as_slice().to_vec()..end.as_slice().to_vec();
		let versioned_iter = self.versioned_range_iter(VersionedRangeQueryParams {
			key_range: &key_range,
			start_ts: 0,
			end_ts: u64::MAX,
			snapshot_seq_num: self.seq_num,
			include_tombstones: true,
			include_latest_only: false, // Include all versions for scan_all_versions
		})?;

		let mut results = Vec::new();
		for (internal_key, encoded_value) in versioned_iter {
			if let Some(limit) = limit {
				if results.len() >= limit {
					break;
				}
			}
			let is_tombstone = internal_key.is_tombstone();
			let value = if is_tombstone {
				Value::default() // Use default value for soft delete markers
			} else {
				self.core.resolve_value(&encoded_value)?
			};
			results.push((
				Bytes::from(internal_key.user_key.as_ref().to_vec()),
				value,
				internal_key.timestamp,
				is_tombstone,
			));
		}

		Ok(results)
	}

	/// Creates a versioned range iterator that implements DoubleEndedIterator
	/// TODO: This is a temporary solution to avoid the complexity of implementing
	/// a proper streaming double ended iterator, which will be fixed in the future.
	fn versioned_range_iter<R: RangeBounds<Vec<u8>>>(
		&self,
		params: VersionedRangeQueryParams<'_, R>,
	) -> Result<VersionedRangeIterator> {
		if !self.core.opts.enable_versioning {
			return Err(Error::InvalidArgument("Versioned queries not enabled".to_string()));
		}

		let mut results = Vec::new();

		if let Some(ref versioned_index) = self.core.versioned_index {
			let index_guard = versioned_index.read();

			// Extract start and end bounds from the RangeBounds
			let start_bound = params.key_range.start_bound();
			let end_bound = params.key_range.end_bound();

			// Convert to InternalKey format for the B+ tree query
			let start_key = match start_bound {
				Bound::Included(key) => InternalKey::new(
					Bytes::copy_from_slice(key),
					0,
					InternalKeyKind::Set,
					params.start_ts,
				)
				.encode(),
				Bound::Excluded(key) => {
					// For excluded bounds, create a key that's lexicographically greater
					let mut next_key = key.to_vec();
					next_key.push(0); // Add null byte to make it greater
					InternalKey::new(
						Bytes::from(next_key),
						0,
						InternalKeyKind::Set,
						params.start_ts,
					)
					.encode()
				}
				Bound::Unbounded => {
					InternalKey::new(Bytes::new(), 0, InternalKeyKind::Set, params.start_ts)
						.encode()
				}
			};

			let end_key = match end_bound {
				Bound::Included(key) => InternalKey::new(
					Bytes::copy_from_slice(key),
					params.snapshot_seq_num,
					InternalKeyKind::Max,
					params.end_ts,
				)
				.encode(),
				Bound::Excluded(key) => {
					// For excluded bounds, use minimal InternalKey properties so range stops just before this key
					InternalKey::new(Bytes::copy_from_slice(key), 0, InternalKeyKind::Set, 0)
						.encode()
				}
				Bound::Unbounded => InternalKey::new(
					Bytes::from_static(&[0xff]),
					params.snapshot_seq_num,
					InternalKeyKind::Max,
					params.end_ts,
				)
				.encode(),
			};

			let range_iter = index_guard.range(&start_key, &end_key)?;

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

				let current_key = internal_key.user_key.as_ref().to_vec();

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
			for (_user_key, mut versions) in key_versions.into_iter() {
				// If include_latest_only is true, keep only the latest version (highest timestamp)
				if params.include_latest_only && !versions.is_empty() {
					// B+tree already provides entries in timestamp order, so just take the last element (highest timestamp)
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
				for (internal_key, encoded_value) in versions_to_output.into_iter() {
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
		self.inner.next().map(|(internal_key, _)| internal_key.user_key.as_ref().to_vec())
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.inner.size_hint()
	}
}

impl DoubleEndedIterator for KeysAtTimestampIterator {
	fn next_back(&mut self) -> Option<Self::Item> {
		self.inner.next_back().map(|(internal_key, _)| internal_key.user_key.as_ref().to_vec())
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
				Ok(resolved_value) => Ok((internal_key.user_key.as_ref().to_vec(), resolved_value)),
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
				Ok(resolved_value) => Ok((internal_key.user_key.as_ref().to_vec(), resolved_value)),
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

/// A merge iterator that sorts by key+seqno.
pub(crate) struct KMergeIterator<'a> {
	// Owned state
	_iter_state: Box<IterState>,

	/// Array of iterators to merge over
	iterators: NonNull<Vec<BoxedIterator<'a>>>,

	/// Interval heap of items ordered by their current key
	heap: IntervalHeap<HeapItem>,

	/// Range bounds for filtering
	range_end: Bound<Vec<u8>>,

	/// Whether the iterator has been initialized for forward iteration
	initialized_lo: bool,

	/// Whether the iterator has been initialized for backward iteration
	initialized_hi: bool,
}

impl<'a> KMergeIterator<'a> {
	fn new_from(
		iter_state: IterState,
		snapshot_seq_num: u64,
		range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
		keys_only: bool,
	) -> Self {
		let boxed_state = Box::new(iter_state);
		let mut iterators: Vec<BoxedIterator<'a>> = Vec::new();

		unsafe {
			let state_ref: &'a IterState = &*(&*boxed_state as *const IterState);
			// Compute query key range for table overlap checks
			let query_range = {
				let low = match &range.0 {
					Bound::Included(v) | Bound::Excluded(v) => v.clone(),
					Bound::Unbounded => Vec::new(),
				};
				let high = match &range.1 {
					Bound::Included(v) | Bound::Excluded(v) => v.clone(),
					Bound::Unbounded => vec![0xff],
				};
				KeyRange::new((Bytes::from(low), Bytes::from(high)))
			};

			// Active memtable with range
			let active_iter = state_ref.active.range(range.clone()).filter(move |item| {
				// Filter out items that are not visible in this snapshot
				item.0.seq_num() <= snapshot_seq_num
			});
			iterators.push(Box::new(active_iter));

			// Immutable memtables with range
			for memtable in &state_ref.immutable {
				let iter = memtable.range(range.clone()).filter(move |item| {
					// Filter out items that are not visible in this snapshot
					item.0.seq_num() <= snapshot_seq_num
				});
				iterators.push(Box::new(iter));
			}

			// Tables - these have native seek support
			for (level_idx, level) in (&state_ref.levels).into_iter().enumerate() {
				// Optimization: Skip tables that are completely outside the query range
				if level_idx == 0 {
					// Level 0: Tables can overlap, so we check all but skip those completely outside range
					for table in &level.tables {
						// Skip tables completely before or after the range
						if table.is_before_range(&query_range) || table.is_after_range(&query_range)
						{
							continue;
						}
						if !table.overlaps_with_range(&query_range) {
							continue;
						}
						let mut table_iter = table.iter(keys_only);
						// Seek to start if unbounded
						match &range.0 {
							Bound::Included(key) => {
								let ikey = InternalKey::new(
									Bytes::copy_from_slice(key),
									INTERNAL_KEY_SEQ_NUM_MAX,
									InternalKeyKind::Max,
									0,
								);
								table_iter.seek(&ikey.encode());
							}
							Bound::Excluded(key) => {
								let ikey = InternalKey::new(
									Bytes::copy_from_slice(key),
									0,
									InternalKeyKind::Delete,
									0,
								);
								table_iter.seek(&ikey.encode());
								// If we're at the excluded key, advance once
								if table_iter.valid() {
									let current = &table_iter.key();
									if current.user_key.as_ref() == key.as_slice() {
										table_iter.advance();
									}
								}
							}
							Bound::Unbounded => {
								table_iter.seek_to_first();
							}
						}

						if table_iter.valid() {
							iterators.push(Box::new(table_iter.filter(move |item| {
								// Filter out items that are not visible in this snapshot
								item.0.seq_num() <= snapshot_seq_num
							})));
						}
					}
				} else {
					// Level 1+: Tables have non-overlapping key ranges, use binary search
					let start_idx = level.find_first_overlapping_table(&query_range);
					let end_idx = level.find_last_overlapping_table(&query_range);

					for table in &level.tables[start_idx..end_idx] {
						if !table.overlaps_with_range(&query_range) {
							continue;
						}
						let mut table_iter = table.iter(keys_only);
						// Seek to start if unbounded
						match &range.0 {
							Bound::Included(key) => {
								let ikey = InternalKey::new(
									Bytes::copy_from_slice(key),
									INTERNAL_KEY_SEQ_NUM_MAX,
									InternalKeyKind::Max,
									0,
								);
								table_iter.seek(&ikey.encode());
							}
							Bound::Excluded(key) => {
								let ikey = InternalKey::new(
									Bytes::copy_from_slice(key),
									0,
									InternalKeyKind::Delete,
									0,
								);
								table_iter.seek(&ikey.encode());
								// If we're at the excluded key, advance once
								if table_iter.valid() {
									let current = &table_iter.key();
									if current.user_key.as_ref() == key.as_slice() {
										table_iter.advance();
									}
								}
							}
							Bound::Unbounded => {
								table_iter.seek_to_first();
							}
						}

						if table_iter.valid() {
							iterators.push(Box::new(table_iter.filter(move |item| {
								// Filter out items that are not visible in this snapshot
								item.0.seq_num() <= snapshot_seq_num
							})));
						}
					}
				}
			}
		}

		let heap = IntervalHeap::with_capacity(iterators.len());

		// Box the iterators and get a raw pointer
		let boxed_iterators = Box::new(iterators);
		let iterators_ptr = NonNull::new(Box::into_raw(boxed_iterators))
			.expect("Box::into_raw should never return null");

		Self {
			_iter_state: boxed_state,
			iterators: iterators_ptr,
			heap,
			range_end: range.1,
			initialized_lo: false,
			initialized_hi: false,
		}
	}

	fn initialize_lo(&mut self) {
		// Pull the first item from each iterator and add to heap
		unsafe {
			let iterators = self.iterators.as_mut();
			for (idx, iter) in iterators.iter_mut().enumerate() {
				if let Some((key, value)) = iter.next() {
					self.heap.push(HeapItem {
						key,
						value,
						iterator_index: idx,
					});
				}
			}
		}
		self.initialized_lo = true;
	}

	fn initialize_hi(&mut self) {
		// Pull the last item from each iterator and add to heap
		unsafe {
			let iterators = self.iterators.as_mut();
			for (idx, iter) in iterators.iter_mut().enumerate() {
				if let Some((key, value)) = iter.next_back() {
					self.heap.push(HeapItem {
						key,
						value,
						iterator_index: idx,
					});
				}
			}
		}
		self.initialized_hi = true;
	}
}

impl Drop for KMergeIterator<'_> {
	fn drop(&mut self) {
		// Must drop the iterators before iter_state
		// because the iterators contain references to iter_state
		unsafe {
			let _ = Box::from_raw(self.iterators.as_ptr());
		}
		// iter_state is dropped automatically after this
	}
}

impl Iterator for KMergeIterator<'_> {
	type Item = (Arc<InternalKey>, Value);

	fn next(&mut self) -> Option<Self::Item> {
		if !self.initialized_lo {
			self.initialize_lo();
		}

		loop {
			if self.heap.is_empty() {
				return None;
			}

			// Get the smallest item from the heap
			let heap_item = self.heap.pop_min()?;

			// Pull the next item from the same iterator and add back to heap if valid
			unsafe {
				let iterators = self.iterators.as_mut();
				if let Some((key, value)) = iterators[heap_item.iterator_index].next() {
					self.heap.push(HeapItem {
						key,
						value,
						iterator_index: heap_item.iterator_index,
					});
				}
			}

			// Check if the key exceeds the range end
			let user_key = heap_item.key.user_key.as_ref();
			match &self.range_end {
				Bound::Included(end) => {
					if user_key > end.as_slice() {
						continue;
					}
				}
				Bound::Excluded(end) => {
					if user_key >= end.as_slice() {
						continue;
					}
				}
				Bound::Unbounded => {
					// No end bound to check
				}
			}

			// Return the key-value pair
			return Some((heap_item.key, heap_item.value));
		}
	}
}

impl DoubleEndedIterator for KMergeIterator<'_> {
	fn next_back(&mut self) -> Option<Self::Item> {
		if !self.initialized_hi {
			self.initialize_hi();
		}

		loop {
			if self.heap.is_empty() {
				return None;
			}

			// Get the largest item from the heap
			let heap_item = self.heap.pop_max()?;

			// Pull the previous item from the same iterator and add back to heap if valid
			unsafe {
				let iterators = self.iterators.as_mut();
				if let Some((key, value)) = iterators[heap_item.iterator_index].next_back() {
					self.heap.push(HeapItem {
						key,
						value,
						iterator_index: heap_item.iterator_index,
					});
				}
			}

			// Check if the key exceeds the range end
			let user_key = heap_item.key.user_key.as_ref();
			match &self.range_end {
				Bound::Included(end) => {
					if user_key > end.as_slice() {
						continue;
					}
				}
				Bound::Excluded(end) => {
					if user_key >= end.as_slice() {
						continue;
					}
				}
				Bound::Unbounded => {
					// No end bound to check
				}
			}

			// Return the key-value pair
			return Some((heap_item.key, heap_item.value));
		}
	}
}

/// Consumes a stream of KVs and emits a new stream to the filter rules
pub(crate) struct FilterIter<I>
where
	I: DoubleEndedIterator<Item = (Arc<InternalKey>, Value)>,
{
	inner: DoubleEndedPeekable<I>,
}

impl<I> FilterIter<I>
where
	I: DoubleEndedIterator<Item = (Arc<InternalKey>, Value)>,
{
	pub fn new(iter: I) -> Self {
		let iter = iter.double_ended_peekable();
		Self {
			inner: iter,
		}
	}

	fn skip_to_latest(&mut self, key: &Key) -> Result<()> {
		loop {
			let Some(next) = self.inner.peek() else {
				return Ok(());
			};

			// Consume version
			if next.0.user_key == *key {
				self.inner.next().expect("should not be empty");
			} else {
				return Ok(());
			}
		}
	}
}

impl<I> Iterator for FilterIter<I>
where
	I: DoubleEndedIterator<Item = (Arc<InternalKey>, Value)>,
{
	type Item = (Arc<InternalKey>, Value);

	fn next(&mut self) -> Option<Self::Item> {
		let head = self.inner.next()?;

		// Keep only latest version of the key
		let _ = self.skip_to_latest(&head.0.user_key);

		Some(head)
	}
}

impl<I> DoubleEndedIterator for FilterIter<I>
where
	I: DoubleEndedIterator<Item = (Arc<InternalKey>, Value)>,
{
	fn next_back(&mut self) -> Option<Self::Item> {
		loop {
			let tail = self.inner.next_back()?;

			let prev = match self.inner.peek_back() {
				Some(prev) => prev,
				None => {
					return Some(tail);
				}
			};

			if prev.0.user_key < tail.0.user_key {
				return Some(tail);
			}
		}
	}
}

pub(crate) struct SnapshotIterator<'a> {
	// The complete pipeline: Data Sources → KMergeIterator → FilterIter → .filter()
	pipeline: Box<dyn DoubleEndedIterator<Item = (Arc<InternalKey>, Value)> + 'a>,
	core: Arc<Core>,
	/// When true, only return keys without resolving values
	keys_only: bool,
}

impl SnapshotIterator<'_> {
	/// Creates a new iterator over a specific key range
	fn new_from<R>(core: Arc<Core>, seq_num: u64, range: R, keys_only: bool) -> Result<Self>
	where
		R: RangeBounds<Vec<u8>>,
	{
		// Create a temporary snapshot to use the helper method
		let snapshot = Snapshot {
			core: core.clone(),
			seq_num,
		};
		let iter_state = snapshot.collect_iter_state()?;

		// Convert bounds to owned for passing to merge iterator
		let start = range.start_bound().map(|v| v.clone());
		let end = range.end_bound().map(|v| v.clone());

		// Increment VLog iterator count
		if let Some(ref vlog) = core.vlog {
			vlog.incr_iterator_count();
		}

		// Build the pipeline: Data Sources → KMergeIterator → FilterIter → .filter()
		let merge_iter: KMergeIterator<'_> =
			KMergeIterator::new_from(iter_state, seq_num, (start, end), keys_only);
		let filter_iter = FilterIter::new(merge_iter);
		let pipeline = Box::new(filter_iter.filter(|(key, _value)| {
			key.kind() != InternalKeyKind::Delete && key.kind() != InternalKeyKind::SoftDelete
		}));

		Ok(Self {
			pipeline,
			core,
			keys_only,
		})
	}
}

impl Iterator for SnapshotIterator<'_> {
	type Item = IterResult;

	fn next(&mut self) -> Option<Self::Item> {
		if let Some((key, value)) = self.pipeline.next() {
			if self.keys_only {
				// For keys-only mode, return None for the value to avoid allocations
				Some(Ok((key.user_key.clone(), None)))
			} else {
				// Resolve value pointers to actual values
				match self.core.resolve_value(&value) {
					Ok(resolved_value) => Some(Ok((key.user_key.clone(), Some(resolved_value)))),
					Err(e) => Some(Err(e)),
				}
			}
		} else {
			None
		}
	}
}

impl DoubleEndedIterator for SnapshotIterator<'_> {
	fn next_back(&mut self) -> Option<Self::Item> {
		if let Some((key, value)) = self.pipeline.next_back() {
			if self.keys_only {
				// For keys-only mode, return None for the value to avoid allocations
				Some(Ok((key.user_key.clone(), None)))
			} else {
				// Resolve value pointers to actual values
				match self.core.resolve_value(&value) {
					Ok(resolved_value) => Some(Ok((key.user_key.clone(), Some(resolved_value)))),
					Err(e) => Some(Err(e)),
				}
			}
		} else {
			None
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

#[cfg(test)]
mod tests {
	use crate::TreeBuilder;
	use crate::{Options, Tree};
	use bytes::Bytes;
	use std::collections::HashSet;
	use std::sync::Arc;
	use test_log::test;

	use super::{IterState, KMergeIterator};
	use crate::levels::Level;
	use crate::levels::Levels;
	use crate::memtable::MemTable;
	use crate::sstable::table::{Table, TableWriter};
	use crate::sstable::{InternalKey, InternalKeyKind};
	use crate::vfs::File;
	use std::ops::Bound;

	use tempdir::TempDir;

	fn create_temp_directory() -> TempDir {
		TempDir::new("test").unwrap()
	}

	// Common setup logic for creating a store
	fn create_store() -> (Tree, TempDir) {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let tree = TreeBuilder::new().with_path(path).build().unwrap();
		(tree, temp_dir)
	}

	#[test(tokio::test)]
	async fn test_empty_snapshot() {
		let (store, _temp_dir) = create_store();

		// Create a transaction without any data
		let tx = store.begin().unwrap();

		// Range scan should return empty
		let range: Vec<_> = tx.range(b"a", b"z").unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert!(range.is_empty());
	}

	#[test(tokio::test)]
	async fn test_basic_snapshot_visibility() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.commit().await.unwrap();
		}

		// Start a read transaction (captures snapshot)
		let read_tx = store.begin().unwrap();

		// Insert more data in a new transaction
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key4", b"value4").unwrap();
			tx.commit().await.unwrap();
		}

		// The read transaction should only see the initial data
		let range: Vec<_> =
			read_tx.range(b"key0", b"key:").unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(range.len(), 2);
		assert_eq!(range[0].0.as_ref(), b"key1");
		assert_eq!(range[1].0.as_ref(), b"key2");
	}

	#[test(tokio::test)]
	async fn test_snapshot_isolation_with_updates() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1_v1").unwrap();
			tx.set(b"key2", b"value2_v1").unwrap();
			tx.commit().await.unwrap();
		}

		// Start a read transaction
		let read_tx = store.begin().unwrap();

		// Update the data in a new transaction
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1_v2").unwrap();
			tx.set(b"key2", b"value2_v2").unwrap();
			tx.commit().await.unwrap();
		}

		// The read transaction should see the old values
		let range: Vec<_> =
			read_tx.range(b"key0", b"key:").unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(range.len(), 2);
		assert_eq!(range[0].1.as_ref(), b"value1_v1");
		assert_eq!(range[1].1.as_ref(), b"value2_v1");

		// A new transaction should see the updated values
		let new_tx = store.begin().unwrap();
		let range: Vec<_> =
			new_tx.range(b"key0", b"key:").unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(range.len(), 2);
		assert_eq!(range[0].1.as_ref(), b"value1_v2");
		assert_eq!(range[1].1.as_ref(), b"value2_v2");
	}

	#[test(tokio::test)]
	async fn test_tombstone_handling() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.commit().await.unwrap();
		}

		// Start a read transaction
		let read_tx1 = store.begin().unwrap();

		// Delete key2
		{
			let mut tx = store.begin().unwrap();
			tx.delete(b"key2").unwrap();
			tx.commit().await.unwrap();
		}

		// The first read transaction should still see all three keys
		let range: Vec<_> =
			read_tx1.range(b"key0", b"key:").unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(range.len(), 3);
		assert_eq!(range[0].0.as_ref(), b"key1");
		assert_eq!(range[1].0.as_ref(), b"key2");
		assert_eq!(range[2].0.as_ref(), b"key3");

		// A new transaction should not see the deleted key
		let read_tx2 = store.begin().unwrap();
		let range: Vec<_> =
			read_tx2.range(b"key0", b"key:").unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(range.len(), 2);
		assert_eq!(range[0].0.as_ref(), b"key1");
		assert_eq!(range[1].0.as_ref(), b"key3");
	}

	#[test(tokio::test)]
	async fn test_version_resolution() {
		let (store, _temp_dir) = create_store();

		// Insert multiple versions of the same key
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"version1").unwrap();
			tx.commit().await.unwrap();
		}

		let tx1 = store.begin().unwrap();

		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"version2").unwrap();
			tx.commit().await.unwrap();
		}

		let tx2 = store.begin().unwrap();

		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"version3").unwrap();
			tx.commit().await.unwrap();
		}

		let tx3 = store.begin().unwrap();

		// Each snapshot should see its corresponding version
		let value1 = tx1.get(b"key1").unwrap().unwrap();
		assert_eq!(value1.as_ref(), b"version1");

		let value2 = tx2.get(b"key1").unwrap().unwrap();
		assert_eq!(value2.as_ref(), b"version2");

		let value3 = tx3.get(b"key1").unwrap().unwrap();
		assert_eq!(value3.as_ref(), b"version3");
	}

	#[test(tokio::test)]
	async fn test_range_with_random_operations() {
		let (store, _temp_dir) = create_store();

		// Initial data
		{
			let mut tx = store.begin().unwrap();
			for i in 1..=10 {
				let key = format!("key{i:02}");
				let value = format!("value{i}");
				tx.set(key.as_bytes(), value.as_bytes()).unwrap();
			}
			tx.commit().await.unwrap();
		}

		let tx1 = store.begin().unwrap();

		// Update some keys and delete others
		{
			let mut tx = store.begin().unwrap();
			// Update even keys
			for i in (2..=10).step_by(2) {
				let key = format!("key{i:02}");
				let value = format!("value{i}_updated");
				tx.set(key.as_bytes(), value.as_bytes()).unwrap();
			}
			// Delete keys 3, 6, 9
			tx.delete(b"key03").unwrap();
			tx.delete(b"key06").unwrap();
			tx.delete(b"key09").unwrap();
			tx.commit().await.unwrap();
		}

		let tx2 = store.begin().unwrap();

		// tx1 should see all original data
		let range1: Vec<_> =
			tx1.range(b"key00", b"key99").unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(range1.len(), 10);
		for (i, item) in range1.iter().enumerate() {
			let (key, value) = item;
			let expected_key = format!("key{:02}", i + 1);
			let expected_value = format!("value{}", i + 1);
			assert_eq!(key.as_ref(), expected_key.as_bytes());
			assert_eq!(value.as_ref(), expected_value.as_bytes());
		}

		// tx2 should see updated data with deletions
		let range2: Vec<_> =
			tx2.range(b"key00", b"key99").unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(range2.len(), 7); // 10 - 3 deleted

		// Check that deleted keys are not present
		let keys: HashSet<_> = range2.iter().map(|item| item.0.as_ref()).collect();
		assert!(!keys.contains(&b"key03".as_ref()));
		assert!(!keys.contains(&b"key06".as_ref()));
		assert!(!keys.contains(&b"key09".as_ref()));

		// Check that even keys are updated
		for item in &range2 {
			let (key, value) = item;
			let key_str = String::from_utf8_lossy(key.as_ref());
			if let Ok(num) = key_str.trim_start_matches("key").parse::<i32>() {
				if num % 2 == 0 {
					let expected_value = format!("value{num}_updated");
					assert_eq!(value.as_ref(), expected_value.as_bytes());
				}
			}
		}
	}

	#[test(tokio::test)]
	async fn test_concurrent_snapshots() {
		let (store, _temp_dir) = create_store();

		// Initial state
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"counter", b"0").unwrap();
			tx.commit().await.unwrap();
		}

		// Create multiple snapshots at different points
		let mut snapshots = Vec::new();

		for i in 1..=5 {
			// Take a snapshot
			snapshots.push(store.begin().unwrap());

			// Update the counter
			{
				let mut tx = store.begin().unwrap();
				tx.set(b"counter", i.to_string().as_bytes()).unwrap();
				tx.commit().await.unwrap();
			}
		}

		// Each snapshot should see its corresponding counter value
		for (i, snapshot) in snapshots.iter().enumerate() {
			let value = snapshot.get(b"counter").unwrap().unwrap();
			let expected = i.to_string();
			assert_eq!(value.as_ref(), expected.as_bytes());
		}
	}

	#[test(tokio::test)]
	async fn test_snapshot_with_complex_key_patterns() {
		let (store, _temp_dir) = create_store();

		// Insert data with different key patterns
		{
			let mut tx = store.begin().unwrap();
			// Numeric keys
			for i in 0..10 {
				let key = format!("{i:03}");
				tx.set(key.as_bytes(), b"numeric").unwrap();
			}
			// Alpha keys
			for c in 'a'..='j' {
				tx.set(c.to_string().as_bytes(), b"alpha").unwrap();
			}
			// Mixed keys
			for i in 0..5 {
				let key = format!("mix{i}key");
				tx.set(key.as_bytes(), b"mixed").unwrap();
			}
			tx.commit().await.unwrap();
		}

		let tx = store.begin().unwrap();

		// Test different range queries
		let numeric_range: Vec<_> = tx.range(b"000", b"999").unwrap().collect::<Vec<_>>();
		assert_eq!(numeric_range.len(), 10);

		let alpha_range: Vec<_> = tx.range(b"a", b"z").unwrap().collect::<Vec<_>>();
		assert_eq!(alpha_range.len(), 15); // 10 numeric + 10 alpha + 5 mixed

		let mixed_range: Vec<_> = tx.range(b"mix", b"miy").unwrap().collect::<Vec<_>>();
		assert_eq!(mixed_range.len(), 5);
	}

	#[test(tokio::test)]
	async fn test_snapshot_ordering_invariants() {
		let (store, _temp_dir) = create_store();

		// Insert data in random order
		{
			let mut tx = store.begin().unwrap();
			let keys = vec![
				"key05", "key01", "key09", "key03", "key07", "key02", "key08", "key04", "key06",
			];
			for key in keys {
				tx.set(key.as_bytes(), key.as_bytes()).unwrap();
			}
			tx.commit().await.unwrap();
		}

		let tx = store.begin().unwrap();

		// Range scan should return keys in sorted order
		let range: Vec<_> =
			tx.range(b"key00", b"key99").unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(range.len(), 9);

		// Verify ordering
		for i in 1..range.len() {
			assert!(range[i - 1].0.as_ref() < range[i].0.as_ref());
		}

		// Verify no duplicates
		let keys: HashSet<_> = range.iter().map(|item| item.0.as_ref()).collect();
		assert_eq!(keys.len(), range.len());
	}

	#[test(tokio::test)]
	async fn test_snapshot_keys_only() {
		let (store, _temp_dir) = create_store();

		// Insert test data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key4", b"value4").unwrap();
			tx.set(b"key5", b"value5").unwrap();
			tx.commit().await.unwrap();
		}

		// Create snapshot via transaction
		let tx = store.begin().unwrap();
		let tx = tx.snapshot.as_ref().unwrap();

		// Get keys only ([key1, key6) to include key5)
		let keys_only_iter = tx.range(b"key1", b"key6", true).unwrap();
		let keys_only: Vec<_> = keys_only_iter.collect::<Result<Vec<_>, _>>().unwrap();

		// Verify we got all 5 keys
		assert_eq!(keys_only.len(), 5);

		// Check keys are correct and values are None
		for (i, (key, value)) in keys_only.iter().enumerate().take(5) {
			let expected_key = format!("key{}", i + 1);
			assert_eq!(key.as_ref(), expected_key.as_bytes());

			// Values should be None for keys-only scan
			assert!(value.is_none(), "Value should be None for keys-only scan");
		}

		// Compare with regular range scan ([key1, key6) to include key5)
		let regular_range_iter = tx.range(b"key1", b"key6", false).unwrap();
		let regular_range: Vec<_> = regular_range_iter.collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(regular_range.len(), keys_only.len());

		// Keys should match but values should be different
		for i in 0..keys_only.len() {
			// Keys should be identical
			assert_eq!(keys_only[i].0, regular_range[i].0, "Keys should match");

			// Regular range should have actual values
			let expected_value = format!("value{}", i + 1);
			assert_eq!(
				regular_range[i].1.as_ref().unwrap().as_ref(),
				expected_value.as_bytes(),
				"Regular range should have correct values"
			);
		}
	}

	#[test]
	fn test_range_skips_non_overlapping_tables() {
		fn build_table(data: Vec<(&'static [u8], &'static [u8])>) -> Arc<Table> {
			let opts = Arc::new(Options::new());
			let mut buf = Vec::new();
			{
				let mut w = TableWriter::new(&mut buf, 0, opts.clone());
				for (k, v) in data {
					let ikey =
						InternalKey::new(Bytes::copy_from_slice(k), 1, InternalKeyKind::Set, 0);
					w.add(ikey.into(), v).unwrap();
				}
				w.finish().unwrap();
			}
			let size = buf.len();
			let file = Arc::new(buf) as Arc<dyn File>;
			Arc::new(Table::new(1, opts, file, size as u64).unwrap())
		}

		// Build two tables with disjoint key ranges
		let table1 = build_table(vec![(b"a1", b"v1"), (b"a2", b"v2")]);
		let table2 = build_table(vec![(b"z1", b"v3"), (b"z2", b"v4")]);

		let mut level0 = Level::with_capacity(10);
		level0.insert(table1);
		level0.insert(table2.clone());

		let levels = Levels(vec![Arc::new(level0)]);

		let iter_state = IterState {
			active: Arc::new(MemTable::new()),
			immutable: Vec::new(),
			levels,
		};

		// Range that only overlaps with table2
		let range = (Bound::Included(b"z0".to_vec()), Bound::Included(b"zz".to_vec()));

		let merge_iter = KMergeIterator::new_from(iter_state, 1, range, false);

		let items: Vec<_> = merge_iter.collect();
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].0.user_key.as_ref(), b"z1");
		assert_eq!(items[1].0.user_key.as_ref(), b"z2");
	}

	#[test(tokio::test)]
	async fn test_double_ended_iteration() {
		let (store, _temp_dir) = create_store();

		// Insert test data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key4", b"value4").unwrap();
			tx.set(b"key5", b"value5").unwrap();
			tx.commit().await.unwrap();
		}

		// Create snapshot via transaction
		let tx = store.begin().unwrap();
		let tx = tx.snapshot.as_ref().unwrap();

		// Test forward iteration ([key1, key6) to include key5)
		let forward_iter = tx.range(b"key1", b"key6", false).unwrap();
		let forward_items: Vec<_> = forward_iter.collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(forward_items.len(), 5);
		assert_eq!(forward_items[0].0.as_ref(), b"key1");
		assert_eq!(forward_items[4].0.as_ref(), b"key5");

		// Test backward iteration ([key1, key6) to include key5)
		let backward_iter = tx.range(b"key1", b"key6", false).unwrap();
		let backward_items: Vec<_> = backward_iter.rev().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(backward_items.len(), 5);
		assert_eq!(backward_items[0].0.as_ref(), b"key5");
		assert_eq!(backward_items[4].0.as_ref(), b"key1");

		// Verify both iterations produce the same items in reverse order
		for i in 0..5 {
			assert_eq!(forward_items[i].0, backward_items[4 - i].0);
		}
	}

	#[test(tokio::test)]
	async fn test_double_ended_iteration_with_tombstones() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.commit().await.unwrap();
		}

		// Take a snapshot before deletion
		let tx1 = store.begin().unwrap();

		// Delete key2
		{
			let mut tx = store.begin().unwrap();
			tx.delete(b"key2").unwrap();
			tx.commit().await.unwrap();
		}

		// Take another snapshot after deletion
		let tx2 = store.begin().unwrap();

		// Test forward iteration on first snapshot (should see all keys)
		let tx1_ref = tx1.snapshot.as_ref().unwrap();
		let forward_iter1 = tx1_ref.range(b"key1", b"key4", false).unwrap();
		let forward_items1: Vec<_> = forward_iter1.collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(forward_items1.len(), 3);

		// Test backward iteration on first snapshot
		let backward_iter1 = tx1_ref.range(b"key1", b"key4", false).unwrap();
		let backward_items1: Vec<_> = backward_iter1.rev().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(backward_items1.len(), 3);

		// Test forward iteration on second snapshot (should not see deleted key)
		let tx2_ref = tx2.snapshot.as_ref().unwrap();
		let forward_iter2 = tx2_ref.range(b"key1", b"key4", false).unwrap();
		let forward_items2: Vec<_> = forward_iter2.collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(forward_items2.len(), 2);

		// Test backward iteration on second snapshot
		let backward_iter2 = tx2_ref.range(b"key1", b"key4", false).unwrap();
		let backward_items2: Vec<_> = backward_iter2.rev().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(backward_items2.len(), 2);

		// Verify both iterations produce the same items in reverse order
		for i in 0..forward_items2.len() {
			assert_eq!(forward_items2[i].0, backward_items2[forward_items2.len() - 1 - i].0);
		}
	}

	#[test(tokio::test)]
	async fn test_soft_delete_snapshot_individual_get() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.commit().await.unwrap();
		}

		// Take a snapshot before soft delete
		let tx1 = store.begin().unwrap();

		// Soft delete key2
		{
			let mut tx = store.begin().unwrap();
			tx.soft_delete(b"key2").unwrap();
			tx.commit().await.unwrap();
		}

		// Take another snapshot after soft delete
		let tx2 = store.begin().unwrap();

		// First snapshot should see both keys
		{
			assert_eq!(tx1.get(b"key1").unwrap().unwrap().as_ref(), b"value1");
			assert_eq!(tx1.get(b"key2").unwrap().unwrap().as_ref(), b"value2");
		}

		// Second snapshot should not see the soft deleted key
		{
			assert_eq!(tx2.get(b"key1").unwrap().unwrap().as_ref(), b"value1");
			assert!(tx2.get(b"key2").unwrap().is_none());
		}
	}

	#[test(tokio::test)]
	async fn test_soft_delete_snapshot_double_ended_iteration() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			for i in 1..=5 {
				let key = format!("key{i}");
				let value = format!("value{i}");
				tx.set(key.as_bytes(), value.as_bytes()).unwrap();
			}
			tx.commit().await.unwrap();
		}

		// Soft delete key2 and key4
		{
			let mut tx = store.begin().unwrap();
			tx.soft_delete(b"key2").unwrap();
			tx.soft_delete(b"key4").unwrap();
			tx.commit().await.unwrap();
		}

		// Take snapshot after soft delete
		let tx = store.begin().unwrap();

		// Test forward iteration
		{
			let snapshot_ref = tx.snapshot.as_ref().unwrap();
			let forward_iter = snapshot_ref.range(b"key1", b"key6", false).unwrap();
			let forward_items: Vec<_> = forward_iter.collect::<Result<Vec<_>, _>>().unwrap();

			assert_eq!(forward_items.len(), 3); // key1, key3, key5
			assert_eq!(forward_items[0].0.as_ref(), b"key1");
			assert_eq!(forward_items[1].0.as_ref(), b"key3");
			assert_eq!(forward_items[2].0.as_ref(), b"key5");
		}

		// Test backward iteration
		{
			let snapshot_ref = tx.snapshot.as_ref().unwrap();
			let backward_iter = snapshot_ref.range(b"key1", b"key6", false).unwrap();
			let backward_items: Vec<_> =
				backward_iter.rev().collect::<Result<Vec<_>, _>>().unwrap();

			assert_eq!(backward_items.len(), 3); // key5, key3, key1
			assert_eq!(backward_items[0].0.as_ref(), b"key5");
			assert_eq!(backward_items[1].0.as_ref(), b"key3");
			assert_eq!(backward_items[2].0.as_ref(), b"key1");
		}
	}

	#[test(tokio::test)]
	async fn test_soft_delete_snapshot_mixed_with_hard_delete() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key4", b"value4").unwrap();
			tx.commit().await.unwrap();
		}

		// Take snapshot before any deletes
		let tx1 = store.begin().unwrap();

		// Mix of soft delete and hard delete
		{
			let mut tx = store.begin().unwrap();
			tx.soft_delete(b"key1").unwrap(); // Soft delete
			tx.delete(b"key2").unwrap(); // Hard delete
			tx.commit().await.unwrap();
		}

		// Take snapshot after deletes
		let tx2 = store.begin().unwrap();

		// First snapshot should see all keys
		{
			let tx1_ref = tx1.snapshot.as_ref().unwrap();
			let range: Vec<_> = tx1_ref
				.range(b"key1", b"key5", false)
				.unwrap()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();
			assert_eq!(range.len(), 4);
		}

		// Second snapshot should not see either deleted key
		{
			let tx2_ref = tx2.snapshot.as_ref().unwrap();
			let range: Vec<_> = tx2_ref
				.range(b"key1", b"key5", false)
				.unwrap()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();
			assert_eq!(range.len(), 2); // Only key3 and key4
			assert_eq!(range[0].0.as_ref(), b"key3");
			assert_eq!(range[1].0.as_ref(), b"key4");
		}
	}

	#[test(tokio::test)]
	async fn test_double_ended_iteration_mixed_operations() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			for i in 1..=10 {
				let key = format!("key{i:02}");
				let value = format!("value{i}");
				tx.set(key.as_bytes(), value.as_bytes()).unwrap();
			}
			tx.commit().await.unwrap();
		}

		// Take a snapshot
		let tx = store.begin().unwrap();

		// Test forward iteration
		let snapshot_ref = tx.snapshot.as_ref().unwrap();
		let forward_iter = snapshot_ref.range(b"key01", b"key11", false).unwrap();
		let forward_items: Vec<_> = forward_iter.collect::<Result<Vec<_>, _>>().unwrap();

		// Test backward iteration
		let backward_iter = snapshot_ref.range(b"key01", b"key11", false).unwrap();
		let backward_items: Vec<_> = backward_iter.rev().collect::<Result<Vec<_>, _>>().unwrap();

		// Both should have 10 items
		assert_eq!(forward_items.len(), 10);
		assert_eq!(backward_items.len(), 10);

		// Verify ordering
		for i in 1..=10 {
			let expected_key = format!("key{i:02}");
			assert_eq!(forward_items[i - 1].0.as_ref(), expected_key.as_bytes());
			assert_eq!(backward_items[10 - i].0.as_ref(), expected_key.as_bytes());
		}

		// Verify both iterations produce the same items in reverse order
		for i in 0..10 {
			assert_eq!(forward_items[i].0, backward_items[9 - i].0);
		}
	}
}
