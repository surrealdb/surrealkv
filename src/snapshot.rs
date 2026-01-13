use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::iter::BoxedIterator;
use crate::levels::Levels;
use crate::lsm::Core;
use crate::memtable::MemTable;
use crate::sstable::{InternalKey, InternalKeyKind};
use crate::{InternalKeyRange, IntoBytes, IterResult, Key, Value};

/// Type alias for version scan results
pub type VersionScanResult = (Key, Value, u64, bool);

/// Type alias for versioned entries with key, timestamp, and optional value
use interval_heap::IntervalHeap;

#[derive(Eq)]
struct HeapItem {
	key: InternalKey,
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

	/// Optimized count operation for a key range
	///
	/// This method efficiently counts keys in the range [start, end) without:
	/// - Creating a full iterator
	/// - Resolving values from the value log
	/// - Allocating result structures
	///
	/// It only counts the latest version of each key and skips tombstones.
	pub(crate) fn count_in_range(
		&self,
		lower: Option<&[u8]>,
		upper: Option<&[u8]>,
	) -> Result<usize> {
		let mut count = 0usize;
		let mut last_key: Option<Key> = None;

		let iter_state = self.collect_iter_state()?;
		let internal_range = crate::user_range_to_internal_range(
			lower.map(Bound::Included).unwrap_or(Bound::Unbounded),
			upper.map(Bound::Excluded).unwrap_or(Bound::Unbounded),
		);

		// Filter out items that are not visible in this snapshot
		let merge_iter = KMergeIterator::new_from(iter_state, internal_range, true).filter_map(
			move |item_result| match item_result {
				Ok(item) if item.0.seq_num() <= self.seq_num => Some(Ok(item)),
				Ok(_) => None,
				Err(e) => Some(Err(e)),
			},
		);

		for item in merge_iter {
			let (key, _value) = item?;
			// Skip older versions of the same key
			if last_key.as_ref().is_some_and(|prev| prev == &key.user_key) {
				continue;
			}

			// Only count non-tombstone entries
			if !key.is_tombstone() {
				count += 1;
			}

			last_key = Some(key.user_key);
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
	pub(crate) fn range(
		&self,
		lower: Option<&[u8]>,
		upper: Option<&[u8]>,
		keys_only: bool,
	) -> Result<impl DoubleEndedIterator<Item = IterResult>> {
		let internal_range = crate::user_range_to_internal_range(
			lower.map(Bound::Included).unwrap_or(Bound::Unbounded),
			upper.map(Bound::Excluded).unwrap_or(Bound::Unbounded),
		);
		SnapshotIterator::new_from(Arc::clone(&self.core), self.seq_num, internal_range, keys_only)
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
			limit: None,
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

	/// Creates a versioned range iterator that implements DoubleEndedIterator
	/// TODO: This is a temporary solution to avoid the complexity of
	/// implementing a proper streaming double ended iterator, which will be
	/// fixed in the future.
	pub(crate) fn versioned_range_iter<R: RangeBounds<Vec<u8>>>(
		&self,
		params: VersionedRangeQueryParams<'_, R>,
	) -> Result<VersionedRangeIterator> {
		if !self.core.opts.enable_versioning {
			return Err(Error::InvalidArgument("Versioned queries not enabled".to_string()));
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

/// A merge iterator that sorts by key+seqno.
pub(crate) struct KMergeIterator<'iter> {
	/// Array of iterators to merge over.
	///
	/// IMPORTANT: Due to self-referential structs, this must be defined before
	/// `iter_state` in order to ensure it is dropped before `iter_state`.
	iterators: Vec<BoxedIterator<'iter>>,

	// Owned state
	#[allow(dead_code)]
	iter_state: Box<IterState>,

	/// Interval heap of items ordered by their current key
	heap: IntervalHeap<HeapItem>,

	/// Whether the iterator has been initialized for forward iteration
	initialized_lo: bool,

	/// Whether the iterator has been initialized for backward iteration
	initialized_hi: bool,
}

impl<'a> KMergeIterator<'a> {
	pub(crate) fn new_from(
		iter_state: IterState,
		internal_range: InternalKeyRange,
		keys_only: bool,
	) -> Self {
		let boxed_state = Box::new(iter_state);

		let query_range = Arc::new(internal_range);

		// Pre-allocate capacity for the iterators.
		// 1 active memtable + immutable memtables + level tables.
		let mut iterators: Vec<BoxedIterator<'a>> =
			Vec::with_capacity(1 + boxed_state.immutable.len() + boxed_state.levels.total_tables());

		let state_ref: &'a IterState = unsafe { &*(&*boxed_state as *const IterState) };

		// Extract user key bounds from InternalKeyRange (Pebble model: inclusive lower, exclusive
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
		let active_iter = state_ref.active.range(lower, upper, keys_only);
		iterators.push(Box::new(active_iter));

		// Immutable memtables
		for memtable in &state_ref.immutable {
			let iter = memtable.range(lower, upper, keys_only);
			iterators.push(Box::new(iter));
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
					let table_iter = table.iter(keys_only, Some((*query_range).clone()));
					iterators.push(Box::new(table_iter));
				}
			} else {
				// Level 1+: Tables have non-overlapping key ranges, use binary search
				let start_idx = level.find_first_overlapping_table(&query_range);
				let end_idx = level.find_last_overlapping_table(&query_range);

				for table in &level.tables[start_idx..end_idx] {
					let table_iter = table.iter(keys_only, Some((*query_range).clone()));
					iterators.push(Box::new(table_iter));
				}
			}
		}

		let heap = IntervalHeap::with_capacity(iterators.len());

		Self {
			iterators,
			iter_state: boxed_state,
			heap,
			initialized_lo: false,
			initialized_hi: false,
		}
	}

	fn initialize_lo(&mut self) -> Result<()> {
		// Pull the first item from each iterator and add to heap
		for (idx, iter) in self.iterators.iter_mut().enumerate() {
			if let Some(item_result) = iter.next() {
				let (key, value) = item_result?;
				self.heap.push(HeapItem {
					key,
					value,
					iterator_index: idx,
				});
			}
		}
		self.initialized_lo = true;
		Ok(())
	}

	fn initialize_hi(&mut self) -> Result<()> {
		// Pull the last item from each iterator and add to heap
		for (idx, iter) in self.iterators.iter_mut().enumerate() {
			if let Some(item_result) = iter.next_back() {
				let (key, value) = item_result?;
				self.heap.push(HeapItem {
					key,
					value,
					iterator_index: idx,
				});
			}
		}
		self.initialized_hi = true;
		Ok(())
	}
}

impl Iterator for KMergeIterator<'_> {
	type Item = Result<(InternalKey, Value)>;

	#[inline]
	fn next(&mut self) -> Option<Self::Item> {
		if !self.initialized_lo {
			if let Err(e) = self.initialize_lo() {
				log::error!("[KMERGE_ITER] Error initializing lo: {}", e);
				return Some(Err(e));
			}
		}

		let min_item = self.heap.pop_min()?;

		if let Some(item_result) = self.iterators[min_item.iterator_index].next() {
			match item_result {
				Ok((key, value)) => {
					self.heap.push(HeapItem {
						key,
						value,
						iterator_index: min_item.iterator_index,
					});
				}
				Err(e) => {
					log::error!(
						"[KMERGE_ITER] Error from iterator {}: {}",
						min_item.iterator_index,
						e
					);
					return Some(Err(e));
				}
			}
		}

		Some(Ok((min_item.key, min_item.value)))
	}
}

impl DoubleEndedIterator for KMergeIterator<'_> {
	#[inline]
	fn next_back(&mut self) -> Option<Self::Item> {
		if !self.initialized_hi {
			if let Err(e) = self.initialize_hi() {
				log::error!("[KMERGE_ITER] Error initializing hi: {}", e);
				return Some(Err(e));
			}
		}

		let max_item = self.heap.pop_max()?;

		if let Some(item_result) = self.iterators[max_item.iterator_index].next_back() {
			match item_result {
				Ok((key, value)) => {
					self.heap.push(HeapItem {
						key,
						value,
						iterator_index: max_item.iterator_index,
					});
				}
				Err(e) => {
					log::error!(
						"[KMERGE_ITER] Error from iterator {}: {}",
						max_item.iterator_index,
						e
					);
					return Some(Err(e));
				}
			}
		}

		Some(Ok((max_item.key, max_item.value)))
	}
}

pub(crate) struct SnapshotIterator<'a> {
	/// The merge iterator wrapped in peekable for efficient version skipping
	merge_iter: KMergeIterator<'a>,

	/// Sequence number for visibility
	snapshot_seq_num: u64,

	/// Core for resolving values
	core: Arc<Core>,

	/// When true, only return keys without resolving values
	keys_only: bool,

	/// Last user key returned (forward direction)
	last_key_fwd: Key,

	/// Buffered item for backward iteration (when we read one too many)
	buffered_back: Option<(InternalKey, Value)>,
}

impl SnapshotIterator<'_> {
	/// Creates a new iterator over a specific key range
	fn new_from(
		core: Arc<Core>,
		seq_num: u64,
		range: InternalKeyRange,
		keys_only: bool,
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

		let merge_iter = KMergeIterator::new_from(iter_state, range, keys_only);

		Ok(Self {
			merge_iter,
			snapshot_seq_num: seq_num,
			core,
			keys_only,
			last_key_fwd: Vec::new(),
			buffered_back: None,
		})
	}

	#[inline]
	fn is_visible(&self, key: &InternalKey) -> bool {
		key.seq_num() <= self.snapshot_seq_num
	}

	/// Resolves the value and constructs the result
	#[inline]
	fn resolve(&self, key: InternalKey, value: Value) -> IterResult {
		if self.keys_only {
			Ok((key.user_key, None))
		} else {
			match self.core.resolve_value(&value) {
				Ok(resolved) => Ok((key.user_key, Some(resolved))),
				Err(e) => Err(e),
			}
		}
	}

	/// Get next item from back, checking buffer first
	#[inline]
	fn next_back_raw(&mut self) -> Option<Result<(InternalKey, Value)>> {
		if let Some(buffered) = self.buffered_back.take() {
			return Some(Ok(buffered));
		}
		self.merge_iter.next_back()
	}
}

impl Iterator for SnapshotIterator<'_> {
	type Item = IterResult;

	#[inline(always)]
	fn next(&mut self) -> Option<Self::Item> {
		while let Some(item_result) = self.merge_iter.next() {
			let (key, value) = match item_result {
				Ok(kv) => kv,
				Err(e) => {
					log::error!("[SNAPSHOT_ITER] Error from merge iterator: {}", e);
					return Some(Err(e));
				}
			};
			// Skip invisible versions (seq_num > snapshot)
			// For forward: highest seq comes first, so first visible is latest
			if !self.is_visible(&key) {
				continue;
			}

			// Skip older versions of last returned key
			if key.user_key == *self.last_key_fwd {
				continue;
			}

			// New key - remember it
			self.last_key_fwd.clone_from(&key.user_key);

			// Latest version is tombstone? Skip entire key
			// (older versions already consumed above)
			if key.is_tombstone() {
				continue;
			}

			return Some(self.resolve(key, value));
		}
		None
	}
}

impl DoubleEndedIterator for SnapshotIterator<'_> {
	#[inline(always)]
	fn next_back(&mut self) -> Option<Self::Item> {
		while let Some(first_result) = self.next_back_raw() {
			let (first_key, first_value) = match first_result {
				Ok(kv) => kv,
				Err(e) => {
					log::error!("[SNAPSHOT_ITER] Error from merge iterator (back): {}", e);
					return Some(Err(e));
				}
			};
			// Skip invisible
			if !self.is_visible(&first_key) {
				continue;
			}

			let mut latest_key = first_key;
			let mut latest_value = first_value;

			// Consume all versions of this key, keeping latest visible
			loop {
				let Some(item_result) = self.next_back_raw() else {
					break;
				};

				let (key, value) = match item_result {
					Ok(kv) => kv,
					Err(e) => {
						log::error!("[SNAPSHOT_ITER] Error from merge iterator (back): {}", e);
						return Some(Err(e));
					}
				};

				if key.user_key != latest_key.user_key {
					// Different key - buffer it for next call
					self.buffered_back = Some((key, value));
					break;
				}

				// Same key - check if this version is newer and visible
				if self.is_visible(&key) {
					latest_key = key;
					latest_value = value;
				}
			}

			// Skip tombstones
			if latest_key.is_tombstone() {
				continue;
			}

			return Some(self.resolve(latest_key, latest_value));
		}
		None
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
