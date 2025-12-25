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
struct VersionedRangeQueryParams<'a, R: RangeBounds<Vec<u8>> + 'a> {
	key_range: &'a R,
	start_ts: u64,
	end_ts: u64,
	snapshot_seq_num: u64,
	limit: Option<usize>,
	include_tombstones: bool,
	include_latest_only: bool, // When true, only include the latest version of each key
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
		let merge_iter =
			KMergeIterator::new_from(iter_state, internal_range, true).filter(move |item| {
				// Filter out items that are not visible in this snapshot
				item.0.seq_num() <= self.seq_num
			});

		for (key, _value) in merge_iter {
			// Skip tombstones
			if key.kind() == InternalKeyKind::Delete || key.kind() == InternalKeyKind::SoftDelete {
				last_key = Some(key.user_key.clone());
				continue;
			}

			// Only count latest version of each key
			match &last_key {
				Some(prev_key) if prev_key == &key.user_key => {
					continue; // Skip older version
				}
				_ => {
					count += 1;
					last_key = Some(key.user_key.clone());
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

		// CREATE ONCE: Create InternalKey for all table lookups
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

	/// Creates a versioned range iterator that implements DoubleEndedIterator
	/// TODO: This is a temporary solution to avoid the complexity of
	/// implementing a proper streaming double ended iterator, which will be
	/// fixed in the future.
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
				Bound::Included(key) => {
					InternalKey::new(key.clone(), 0, InternalKeyKind::Set, params.start_ts).encode()
				}
				Bound::Excluded(key) => {
					// For excluded bounds, create a key that's lexicographically greater
					let mut next_key = key.clone();
					next_key.push(0); // Add null byte to make it greater
					InternalKey::new(next_key, 0, InternalKeyKind::Set, params.start_ts).encode()
				}
				Bound::Unbounded => {
					InternalKey::new(Key::new(), 0, InternalKeyKind::Set, params.start_ts).encode()
				}
			};

			let end_key = match end_bound {
				Bound::Included(key) => InternalKey::new(
					key.clone(),
					params.snapshot_seq_num,
					InternalKeyKind::Max,
					params.end_ts,
				)
				.encode(),
				Bound::Excluded(key) => {
					// For excluded bounds, use minimal InternalKey properties so range stops just
					// before this key
					InternalKey::new(key.clone(), 0, InternalKeyKind::Set, 0).encode()
				}
				Bound::Unbounded => InternalKey::new(
					[0xff].to_vec(),
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

				let current_key = internal_key.user_key.clone();

				key_versions.entry(current_key).or_default().push((internal_key, encoded_value));
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
	fn new_from(iter_state: IterState, internal_range: InternalKeyRange, keys_only: bool) -> Self {
		let boxed_state = Box::new(iter_state);

		// WRAP range in Arc to share it
		let shared_range = Arc::new(internal_range);

		// Pre-allocate capacity for the iterators.
		// 1 active memtable + immutable memtables + level tables.
		let mut iterators: Vec<BoxedIterator<'a>> =
			Vec::with_capacity(1 + boxed_state.immutable.len() + boxed_state.levels.total_tables());

		let state_ref: &'a IterState = unsafe { &*(&*boxed_state as *const IterState) };

		// Active memtable with shared range
		let active_iter = state_ref.active.range((*shared_range).clone(), keys_only);
		iterators.push(Box::new(active_iter));

		// Immutable memtables with range
		for memtable in &state_ref.immutable {
			let iter = memtable.range((*shared_range).clone(), keys_only);
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
					if table.is_before_range(&shared_range) || table.is_after_range(&shared_range) {
						continue;
					}
					let table_iter = table.iter(keys_only, Some((*shared_range).clone()));
					iterators.push(Box::new(table_iter));
				}
			} else {
				// Level 1+: Tables have non-overlapping key ranges, use binary search
				let start_idx = level.find_first_overlapping_table(&shared_range);
				let end_idx = level.find_last_overlapping_table(&shared_range);

				for table in &level.tables[start_idx..end_idx] {
					let table_iter = table.iter(keys_only, Some((*shared_range).clone()));
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

	fn initialize_lo(&mut self) {
		// Pull the first item from each iterator and add to heap
		for (idx, iter) in self.iterators.iter_mut().enumerate() {
			if let Some((key, value)) = iter.next() {
				self.heap.push(HeapItem {
					key,
					value,
					iterator_index: idx,
				});
			}
		}
		self.initialized_lo = true;
	}

	fn initialize_hi(&mut self) {
		// Pull the last item from each iterator and add to heap
		for (idx, iter) in self.iterators.iter_mut().enumerate() {
			if let Some((key, value)) = iter.next_back() {
				self.heap.push(HeapItem {
					key,
					value,
					iterator_index: idx,
				});
			}
		}
		self.initialized_hi = true;
	}
}

impl Iterator for KMergeIterator<'_> {
	type Item = (InternalKey, Value);

	#[inline]
	fn next(&mut self) -> Option<Self::Item> {
		if !self.initialized_lo {
			self.initialize_lo();
		}

		let min_item = self.heap.pop_min()?;
		if let Some(next_item) = self.iterators[min_item.iterator_index].next() {
			self.heap.push(HeapItem {
				key: next_item.0,
				value: next_item.1,
				iterator_index: min_item.iterator_index,
			});
		}

		Some((min_item.key, min_item.value))
	}
}

impl DoubleEndedIterator for KMergeIterator<'_> {
	#[inline]
	fn next_back(&mut self) -> Option<Self::Item> {
		if !self.initialized_hi {
			self.initialize_hi();
		}

		let max_item = self.heap.pop_max()?;
		if let Some(next_item) = self.iterators[max_item.iterator_index].next_back() {
			self.heap.push(HeapItem {
				key: next_item.0,
				value: next_item.1,
				iterator_index: max_item.iterator_index,
			});
		}

		Some((max_item.key, max_item.value))
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
	fn next_back_raw(&mut self) -> Option<(InternalKey, Value)> {
		self.buffered_back.take().or_else(|| self.merge_iter.next_back())
	}
}

impl Iterator for SnapshotIterator<'_> {
	type Item = IterResult;

	#[inline(always)]
	fn next(&mut self) -> Option<Self::Item> {
		while let Some((key, value)) = self.merge_iter.next() {
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
		while let Some((first_key, first_value)) = self.next_back_raw() {
			// Skip invisible
			if !self.is_visible(&first_key) {
				continue;
			}

			let mut latest_key = first_key;
			let mut latest_value = first_value;

			// Consume all versions of this key, keeping latest visible
			loop {
				let Some((key, value)) = self.next_back_raw() else {
					break;
				};

				// Compare without cloning - use reference
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

#[cfg(test)]
mod tests {
	use std::collections::HashSet;
	use std::ops::Bound;
	use std::sync::Arc;

	use tempdir::TempDir;
	use test_log::test;

	use super::{IterState, KMergeIterator};
	use crate::levels::{Level, Levels};
	use crate::memtable::MemTable;
	use crate::sstable::table::{Table, TableWriter};
	use crate::sstable::{InternalKey, InternalKeyKind};
	use crate::vfs::File;
	use crate::{Options, Tree, TreeBuilder};

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
		assert_eq!(range[0].0, b"key1");
		assert_eq!(range[1].0, b"key2");
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
		assert_eq!(range[0].1, b"value1_v1");
		assert_eq!(range[1].1, b"value2_v1");

		// A new transaction should see the updated values
		let new_tx = store.begin().unwrap();
		let range: Vec<_> =
			new_tx.range(b"key0", b"key:").unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(range.len(), 2);
		assert_eq!(range[0].1, b"value1_v2");
		assert_eq!(range[1].1, b"value2_v2");
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
		assert_eq!(range[0].0, b"key1");
		assert_eq!(range[1].0, b"key2");
		assert_eq!(range[2].0, b"key3");

		// A new transaction should not see the deleted key
		let read_tx2 = store.begin().unwrap();
		let range: Vec<_> =
			read_tx2.range(b"key0", b"key:").unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(range.len(), 2);
		assert_eq!(range[0].0, b"key1");
		assert_eq!(range[1].0, b"key3");
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
		assert_eq!(value1, b"version1");

		let value2 = tx2.get(b"key1").unwrap().unwrap();
		assert_eq!(value2, b"version2");

		let value3 = tx3.get(b"key1").unwrap().unwrap();
		assert_eq!(value3, b"version3");
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
		for (i, (key, value)) in range1.iter().enumerate() {
			let expected_key = format!("key{:02}", i + 1);
			let expected_value = format!("value{}", i + 1);
			assert_eq!(key, expected_key.as_bytes());
			assert_eq!(value, expected_value.as_bytes());
		}

		// tx2 should see updated data with deletions
		let range2: Vec<_> =
			tx2.range(b"key00", b"key99").unwrap().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(range2.len(), 7); // 10 - 3 deleted

		// Check that deleted keys are not present
		let keys: HashSet<_> = range2.iter().map(|item| item.0.clone()).collect();
		assert!(!keys.contains(b"key03".as_slice()));
		assert!(!keys.contains(b"key06".as_slice()));
		assert!(!keys.contains(b"key09".as_slice()));

		// Check that even keys are updated
		for item in &range2 {
			let (key, value) = item;
			let key_str = String::from_utf8_lossy(key.as_ref());
			if let Ok(num) = key_str.trim_start_matches("key").parse::<i32>() {
				if num % 2 == 0 {
					let expected_value = format!("value{num}_updated");
					assert_eq!(value, expected_value.as_bytes());
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
			assert_eq!(value, expected.as_bytes());
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
			assert!(range[i - 1].0 < range[i].0);
		}

		// Verify no duplicates
		let keys: HashSet<_> = range.iter().map(|item| item.0.clone()).collect();
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
		let keys_only_iter =
			tx.range(Some("key1".as_bytes()), Some("key6".as_bytes()), true).unwrap();
		let keys_only: Vec<_> = keys_only_iter.collect::<Result<Vec<_>, _>>().unwrap();

		// Verify we got all 5 keys
		assert_eq!(keys_only.len(), 5);

		// Check keys are correct and values are None
		for (i, (key, value)) in keys_only.iter().enumerate().take(5) {
			let expected_key = format!("key{}", i + 1);
			assert_eq!(key, expected_key.as_bytes());

			// Values should be None for keys-only scan
			assert!(value.is_none(), "Value should be None for keys-only scan");
		}

		// Compare with regular range scan ([key1, key6) to include key5)
		let regular_range_iter =
			tx.range(Some("key1".as_bytes()), Some("key6".as_bytes()), false).unwrap();
		let regular_range: Vec<_> = regular_range_iter.collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(regular_range.len(), keys_only.len());

		// Keys should match but values should be different
		for i in 0..keys_only.len() {
			// Keys should be identical
			assert_eq!(keys_only[i].0, regular_range[i].0, "Keys should match");

			// Regular range should have actual values
			let expected_value = format!("value{}", i + 1);
			assert_eq!(
				regular_range[i].1.as_ref().unwrap(),
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
				let mut w = TableWriter::new(&mut buf, 0, opts.clone(), 0); // L0 for test
				for (k, v) in data {
					let ikey = InternalKey::new(k.to_vec(), 1, InternalKeyKind::Set, 0);
					w.add(ikey, v).unwrap();
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
		level0.insert(table2);

		let levels = Levels(vec![Arc::new(level0)]);

		let iter_state = IterState {
			active: Arc::new(MemTable::new()),
			immutable: Vec::new(),
			levels,
		};

		// Range that only overlaps with table2
		let internal_range = crate::user_range_to_internal_range(
			Bound::Included("z0".as_bytes()),
			Bound::Excluded("zz".as_bytes()),
		);
		let merge_iter = KMergeIterator::new_from(iter_state, internal_range, false);

		let items: Vec<_> = merge_iter.collect();
		assert_eq!(items.len(), 2);
		assert_eq!(items[0].0.user_key.as_slice(), b"z1");
		assert_eq!(items[1].0.user_key.as_slice(), b"z2");
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
		let forward_iter =
			tx.range(Some("key1".as_bytes()), Some("key6".as_bytes()), false).unwrap();
		let forward_items: Vec<_> = forward_iter.collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(forward_items.len(), 5);
		assert_eq!(&forward_items[0].0, b"key1");
		assert_eq!(&forward_items[4].0, b"key5");

		// Test backward iteration ([key1, key6) to include key5)
		let backward_iter =
			tx.range(Some("key1".as_bytes()), Some("key6".as_bytes()), false).unwrap();
		let backward_items: Vec<_> = backward_iter.rev().collect::<Result<Vec<_>, _>>().unwrap();

		assert_eq!(backward_items.len(), 5);
		assert_eq!(&backward_items[0].0, b"key5");
		assert_eq!(&backward_items[4].0, b"key1");

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
		let forward_iter1 =
			tx1_ref.range(Some("key1".as_bytes()), Some("key4".as_bytes()), false).unwrap();
		let forward_items1: Vec<_> = forward_iter1.collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(forward_items1.len(), 3);

		// Test backward iteration on first snapshot
		let backward_iter1 =
			tx1_ref.range(Some("key1".as_bytes()), Some("key4".as_bytes()), false).unwrap();
		let backward_items1: Vec<_> = backward_iter1.rev().collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(backward_items1.len(), 3);

		// Test forward iteration on second snapshot (should not see deleted key)
		let tx2_ref = tx2.snapshot.as_ref().unwrap();
		let forward_iter2 =
			tx2_ref.range(Some("key1".as_bytes()), Some("key4".as_bytes()), false).unwrap();
		let forward_items2: Vec<_> = forward_iter2.collect::<Result<Vec<_>, _>>().unwrap();
		assert_eq!(forward_items2.len(), 2);

		// Test backward iteration on second snapshot
		let backward_iter2 =
			tx2_ref.range(Some("key1".as_bytes()), Some("key4".as_bytes()), false).unwrap();
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
			assert_eq!(&tx1.get(b"key1").unwrap().unwrap(), b"value1");
			assert_eq!(&tx1.get(b"key2").unwrap().unwrap(), b"value2");
		}

		// Second snapshot should not see the soft deleted key
		{
			assert_eq!(&tx2.get(b"key1").unwrap().unwrap(), b"value1");
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
			let forward_iter = snapshot_ref
				.range(Some("key1".as_bytes()), Some("key6".as_bytes()), false)
				.unwrap();
			let forward_items: Vec<_> = forward_iter.collect::<Result<Vec<_>, _>>().unwrap();

			assert_eq!(forward_items.len(), 3); // key1, key3, key5
			assert_eq!(&forward_items[0].0, b"key1");
			assert_eq!(&forward_items[1].0, b"key3");
			assert_eq!(&forward_items[2].0, b"key5");
		}

		// Test backward iteration
		{
			let snapshot_ref = tx.snapshot.as_ref().unwrap();
			let backward_iter = snapshot_ref
				.range(Some("key1".as_bytes()), Some("key6".as_bytes()), false)
				.unwrap();
			let backward_items: Vec<_> =
				backward_iter.rev().collect::<Result<Vec<_>, _>>().unwrap();

			assert_eq!(backward_items.len(), 3); // key5, key3, key1
			assert_eq!(&backward_items[0].0, b"key5");
			assert_eq!(&backward_items[1].0, b"key3");
			assert_eq!(&backward_items[2].0, b"key1");
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
				.range(Some("key1".as_bytes()), Some("key5".as_bytes()), false)
				.unwrap()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();
			assert_eq!(range.len(), 4);
		}

		// Second snapshot should not see either deleted key
		{
			let tx2_ref = tx2.snapshot.as_ref().unwrap();
			let range: Vec<_> = tx2_ref
				.range(Some("key1".as_bytes()), Some("key5".as_bytes()), false)
				.unwrap()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();
			assert_eq!(range.len(), 2); // Only key3 and key4
			assert_eq!(&range[0].0, b"key3");
			assert_eq!(&range[1].0, b"key4");
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
		let forward_iter =
			snapshot_ref.range(Some("key01".as_bytes()), Some("key11".as_bytes()), false).unwrap();
		let forward_items: Vec<_> = forward_iter.collect::<Result<Vec<_>, _>>().unwrap();

		// Test backward iteration
		let backward_iter =
			snapshot_ref.range(Some("key01".as_bytes()), Some("key11".as_bytes()), false).unwrap();
		let backward_items: Vec<_> = backward_iter.rev().collect::<Result<Vec<_>, _>>().unwrap();

		// Both should have 10 items
		assert_eq!(forward_items.len(), 10);
		assert_eq!(backward_items.len(), 10);

		// Verify ordering
		for i in 1..=10 {
			let expected_key = format!("key{i:02}");
			assert_eq!(&forward_items[i - 1].0, expected_key.as_bytes());
			assert_eq!(&backward_items[10 - i].0, expected_key.as_bytes());
		}

		// Verify both iterations produce the same items in reverse order
		for i in 0..10 {
			assert_eq!(forward_items[i].0, backward_items[9 - i].0);
		}
	}

	// ========================================================================
	// KMergeIterator Range Query Tests
	// ========================================================================

	// Helper function to create a test table with specific key range
	fn create_test_table_with_range(
		table_id: u64,
		key_start: &str,
		key_end: &str,
		seq_start: u64,
		opts: Arc<Options>,
	) -> crate::Result<Arc<Table>> {
		use std::fs::{self, File as SysFile};

		// Ensure the sstables directory exists
		let sstables_dir = opts.path.join("sstables");
		fs::create_dir_all(&sstables_dir)?;

		let table_file_path = opts.sstable_file_path(table_id);
		let mut file = SysFile::create(&table_file_path)?;

		let mut writer = TableWriter::new(&mut file, table_id, opts.clone(), 0); // L0 for test

		// Generate incremental keys spanning the range
		let mut keys = Vec::new();

		// For single-character ranges, generate all keys from start to end
		if key_start.len() == 1 && key_end.len() == 1 {
			let start_byte = key_start.as_bytes()[0];
			let end_byte = key_end.as_bytes()[0];

			for byte_val in start_byte..=end_byte {
				keys.push(String::from_utf8(vec![byte_val]).unwrap());
			}
		} else {
			// For multi-character ranges, create keys with numeric suffixes
			keys.push(key_start.to_string());
			keys.push(format!("{key_start}_mid"));
			keys.push(key_end.to_string());
		}

		for (i, key) in keys.iter().enumerate() {
			let seq_num = seq_start + i as u64;
			let value = format!("value_{seq_num}");

			let internal_key =
				InternalKey::new(key.as_bytes().to_vec(), seq_num, InternalKeyKind::Set, 0);

			writer.add(internal_key, value.as_bytes())?;
		}

		let size = writer.finish()?;

		let file = SysFile::open(&table_file_path)?;
		file.sync_all()?;
		let file: Arc<dyn File> = Arc::new(file);

		let table = Table::new(table_id, opts, file, size as u64)?;
		Ok(Arc::new(table))
	}

	// Helper to create IterState with specified tables
	fn create_iter_state_with_tables(
		l0_tables: Vec<Arc<Table>>,
		l1_tables: Vec<Arc<Table>>,
		l2_tables: Vec<Arc<Table>>,
		_opts: Arc<Options>,
	) -> IterState {
		let mut level0 = Level::default();
		for table in l0_tables {
			level0.tables.push(table);
		}

		let mut level1 = Level::default();
		for table in l1_tables {
			level1.tables.push(table);
		}

		let mut level2 = Level::default();
		for table in l2_tables {
			level2.tables.push(table);
		}

		let levels = Levels(vec![Arc::new(level0), Arc::new(level1), Arc::new(level2)]);

		IterState {
			active: Arc::new(MemTable::new()),
			immutable: vec![],
			levels,
		}
	}

	// Helper to count the number of items returned by iterator
	fn count_kmerge_items(mut iter: KMergeIterator) -> usize {
		let mut count = 0;
		while iter.next().is_some() {
			count += 1;
		}
		count
	}

	#[test]
	fn test_level0_tables_before_range_skipped() {
		let temp_dir = create_temp_directory();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		let opts = Arc::new(opts);

		// Create L0 tables with ranges: [a-c], [d-f], [g-i]
		let table1 = create_test_table_with_range(1, "a", "c", 1, opts.clone()).unwrap();
		let table2 = create_test_table_with_range(2, "d", "f", 4, opts.clone()).unwrap();
		let table3 = create_test_table_with_range(3, "g", "i", 7, opts.clone()).unwrap();

		let iter_state =
			create_iter_state_with_tables(vec![table1, table2, table3], vec![], vec![], opts);

		// Query range: [j-z] - all tables are before this range
		let internal_range = crate::user_range_to_internal_range(
			Bound::Included(b"j".as_slice()),
			Bound::Excluded(b"z".as_slice()),
		);
		let iter = KMergeIterator::new_from(iter_state, internal_range, false);

		let count = count_kmerge_items(iter);
		assert_eq!(count, 0, "No tables should be included as all are before range");
	}

	#[test]
	fn test_level0_tables_after_range_skipped() {
		let temp_dir = create_temp_directory();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		let opts = Arc::new(opts);

		// Create L0 tables with ranges: [m-o], [p-r], [s-u]
		let table1 = create_test_table_with_range(1, "m", "o", 1, opts.clone()).unwrap();
		let table2 = create_test_table_with_range(2, "p", "r", 4, opts.clone()).unwrap();
		let table3 = create_test_table_with_range(3, "s", "u", 7, opts.clone()).unwrap();

		let iter_state =
			create_iter_state_with_tables(vec![table1, table2, table3], vec![], vec![], opts);

		// Query range: [a-k] - all tables are after this range
		let internal_range = crate::user_range_to_internal_range(
			Bound::Included(b"a".as_slice()),
			Bound::Excluded(b"k".as_slice()),
		);
		let iter = KMergeIterator::new_from(iter_state, internal_range, false);

		let count = count_kmerge_items(iter);
		assert_eq!(count, 0, "No tables should be included as all are after range");
	}

	#[test]
	fn test_level0_overlapping_tables_included() {
		let temp_dir = create_temp_directory();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		let opts = Arc::new(opts);

		// Create L0 tables with overlapping ranges
		let table1 = create_test_table_with_range(1, "a", "e", 1, opts.clone()).unwrap();
		let table2 = create_test_table_with_range(2, "c", "g", 4, opts.clone()).unwrap();
		let table3 = create_test_table_with_range(3, "f", "j", 7, opts.clone()).unwrap();

		let iter_state =
			create_iter_state_with_tables(vec![table1, table2, table3], vec![], vec![], opts);

		// Query range: [d-h] - all tables overlap with this range
		let internal_range = crate::user_range_to_internal_range(
			Bound::Included(b"d".as_slice()),
			Bound::Excluded(b"h".as_slice()),
		);
		let iter = KMergeIterator::new_from(iter_state, internal_range, false);

		let count = count_kmerge_items(iter);
		// All 3 tables should contribute items
		assert!(count > 0, "Should have items from overlapping L0 tables");
	}

	#[test]
	fn test_level0_mixed_overlap_scenarios() {
		let temp_dir = create_temp_directory();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		let opts = Arc::new(opts);

		// Create L0 tables: [a-c], [e-g], [i-k], [d-f], [j-m]
		let table1 = create_test_table_with_range(1, "a", "c", 1, opts.clone()).unwrap();
		let table2 = create_test_table_with_range(2, "e", "g", 4, opts.clone()).unwrap();
		let table3 = create_test_table_with_range(3, "i", "k", 7, opts.clone()).unwrap();
		let table4 = create_test_table_with_range(4, "d", "f", 10, opts.clone()).unwrap();
		let table5 = create_test_table_with_range(5, "j", "m", 13, opts.clone()).unwrap();

		let iter_state = create_iter_state_with_tables(
			vec![table1, table2, table3, table4, table5],
			vec![],
			vec![],
			opts,
		);

		// Query range: [f-j] - should include tables [e-g], [i-k], [d-f], [j-m]
		let internal_range = crate::user_range_to_internal_range(
			Bound::Included(b"f".as_slice()),
			Bound::Excluded(b"j".as_slice()),
		);
		let iter = KMergeIterator::new_from(iter_state, internal_range, false);

		let count = count_kmerge_items(iter);
		// Should have items from multiple overlapping tables
		assert!(count > 0, "Should have items from overlapping tables in range");
	}

	#[test]
	fn test_level1_binary_search_correct_range() {
		let temp_dir = create_temp_directory();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		let opts = Arc::new(opts);

		// Create L1 tables with non-overlapping sorted ranges
		let table1 = create_test_table_with_range(11, "a", "b", 1, opts.clone()).unwrap();
		let table2 = create_test_table_with_range(12, "c", "d", 4, opts.clone()).unwrap();
		let table3 = create_test_table_with_range(13, "e", "f", 7, opts.clone()).unwrap();
		let table4 = create_test_table_with_range(14, "g", "h", 10, opts.clone()).unwrap();
		let table5 = create_test_table_with_range(15, "i", "j", 13, opts.clone()).unwrap();

		let iter_state = create_iter_state_with_tables(
			vec![],
			vec![table1, table2, table3, table4, table5],
			vec![],
			opts,
		);

		// Query range: [e-h] - should include tables [e-f], [g-h]
		let internal_range = crate::user_range_to_internal_range(
			Bound::Included(b"e".as_slice()),
			Bound::Excluded(b"h".as_slice()),
		);
		let iter = KMergeIterator::new_from(iter_state, internal_range, false);

		let count = count_kmerge_items(iter);
		assert!(count > 0, "Should have items from L1 tables in range");
	}

	#[test]
	fn test_level1_query_before_all_tables() {
		let temp_dir = create_temp_directory();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		let opts = Arc::new(opts);

		// Create L1 tables: [d-f], [g-i], [j-l]
		let table1 = create_test_table_with_range(11, "d", "f", 1, opts.clone()).unwrap();
		let table2 = create_test_table_with_range(12, "g", "i", 4, opts.clone()).unwrap();
		let table3 = create_test_table_with_range(13, "j", "l", 7, opts.clone()).unwrap();

		let iter_state =
			create_iter_state_with_tables(vec![], vec![table1, table2, table3], vec![], opts);

		// Query range: [a-c] - before all tables
		let internal_range = crate::user_range_to_internal_range(
			Bound::Included(b"a".as_slice()),
			Bound::Excluded(b"c".as_slice()),
		);
		let iter = KMergeIterator::new_from(iter_state, internal_range, false);

		let count = count_kmerge_items(iter);
		assert_eq!(count, 0, "No tables should be included as query is before all L1 tables");
	}

	#[test]
	fn test_level1_query_after_all_tables() {
		let temp_dir = create_temp_directory();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		let opts = Arc::new(opts);

		// Create L1 tables: [a-c], [d-f], [g-i]
		let table1 = create_test_table_with_range(11, "a", "c", 1, opts.clone()).unwrap();
		let table2 = create_test_table_with_range(12, "d", "f", 4, opts.clone()).unwrap();
		let table3 = create_test_table_with_range(13, "g", "i", 7, opts.clone()).unwrap();

		let iter_state =
			create_iter_state_with_tables(vec![], vec![table1, table2, table3], vec![], opts);

		// Query range: [m-z] - after all tables
		let internal_range = crate::user_range_to_internal_range(
			Bound::Included(b"m".as_slice()),
			Bound::Excluded(b"z".as_slice()),
		);
		let iter = KMergeIterator::new_from(iter_state, internal_range, false);

		let count = count_kmerge_items(iter);
		assert_eq!(count, 0, "No tables should be included as query is after all L1 tables");
	}

	#[test]
	fn test_level1_query_spans_all_tables() {
		let temp_dir = create_temp_directory();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		let opts = Arc::new(opts);

		// Create L1 tables: [b-d], [e-g], [h-j]
		let table1 = create_test_table_with_range(11, "b", "d", 1, opts.clone()).unwrap();
		let table2 = create_test_table_with_range(12, "e", "g", 4, opts.clone()).unwrap();
		let table3 = create_test_table_with_range(13, "h", "j", 7, opts.clone()).unwrap();

		let iter_state =
			create_iter_state_with_tables(vec![], vec![table1, table2, table3], vec![], opts);

		// Query range: [a-z] - spans all tables
		let internal_range = crate::user_range_to_internal_range(
			Bound::Included(b"a".as_slice()),
			Bound::Excluded(b"z".as_slice()),
		);
		let iter = KMergeIterator::new_from(iter_state, internal_range, false);

		let count = count_kmerge_items(iter);
		assert!(count > 0, "Should have items from all L1 tables");
	}

	#[test]
	fn test_bound_included_start_and_end() {
		let temp_dir = create_temp_directory();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		let opts = Arc::new(opts);

		// Create table with keys: "d1", "d5", "h"
		let table1 = create_test_table_with_range(1, "d", "h", 1, opts.clone()).unwrap();

		let iter_state = create_iter_state_with_tables(vec![table1], vec![], vec![], opts);

		// Query with Included bounds - should include all keys from d to h
		let internal_range = crate::user_range_to_internal_range(
			Bound::Included(b"d".as_slice()),
			Bound::Excluded(b"h".as_slice()),
		);
		let iter = KMergeIterator::new_from(iter_state, internal_range, false);

		let items: Vec<_> = iter.collect();
		assert!(!items.is_empty(), "Should have items in inclusive range");
	}

	#[test]
	fn test_bound_excluded_start_and_end() {
		let temp_dir = create_temp_directory();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		let opts = Arc::new(opts);

		// Create table with keys: "d1", "d5", "h"
		let table1 = create_test_table_with_range(1, "d", "h", 1, opts.clone()).unwrap();

		let iter_state = create_iter_state_with_tables(vec![table1], vec![], vec![], opts);

		// Query with Excluded bounds - "d" and "h" exact matches should be excluded
		// But "d1", "d5" are > "d" so they should be included
		let internal_range = crate::user_range_to_internal_range(
			Bound::Included(b"d".as_slice()),
			Bound::Excluded(b"h".as_slice()),
		);
		let iter = KMergeIterator::new_from(iter_state, internal_range, false);

		let items: Vec<_> = iter.collect();
		// Keys "d1" and "d5" should be included (they're > "d" and < "h")
		// But exact "d" and exact "h" should not be (though "h" is at the boundary)
		assert!(items.len() >= 2, "Should have at least d1 and d5");
	}

	#[test]
	fn test_bound_unbounded_start() {
		let temp_dir = create_temp_directory();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		let opts = Arc::new(opts);

		let table1 = create_test_table_with_range(1, "a", "z", 1, opts.clone()).unwrap();

		let iter_state = create_iter_state_with_tables(vec![table1], vec![], vec![], opts);

		// Query with unbounded start
		let internal_range =
			crate::user_range_to_internal_range(Bound::Unbounded, Bound::Included(b"h".as_slice()));
		let iter = KMergeIterator::new_from(iter_state, internal_range, false);

		let count = count_kmerge_items(iter);
		assert!(count > 0, "Should iterate from beginning with unbounded start");
	}

	#[test]
	fn test_bound_unbounded_end() {
		let temp_dir = create_temp_directory();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		let opts = Arc::new(opts);

		let table1 = create_test_table_with_range(1, "a", "z", 1, opts.clone()).unwrap();

		let iter_state = create_iter_state_with_tables(vec![table1], vec![], vec![], opts);

		// Query with unbounded end
		let internal_range =
			crate::user_range_to_internal_range(Bound::Included(b"d".as_slice()), Bound::Unbounded);
		let iter = KMergeIterator::new_from(iter_state, internal_range, false);

		let count = count_kmerge_items(iter);
		assert!(count > 0, "Should iterate to end with unbounded end");
	}

	#[test]
	fn test_fully_unbounded_range() {
		let temp_dir = create_temp_directory();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		let opts = Arc::new(opts);

		let table1 = create_test_table_with_range(1, "a", "m", 1, opts.clone()).unwrap();
		let table2 = create_test_table_with_range(2, "n", "z", 4, opts.clone()).unwrap();

		let iter_state = create_iter_state_with_tables(vec![table1, table2], vec![], vec![], opts);

		// Query with fully unbounded range
		let iter =
			KMergeIterator::new_from(iter_state, (Bound::Unbounded, Bound::Unbounded), false);

		let count = count_kmerge_items(iter);
		assert!(count > 0, "Should return all keys with fully unbounded range");
	}

	#[test]
	fn test_empty_levels() {
		let temp_dir = create_temp_directory();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		let opts = Arc::new(opts);

		// Create IterState with no tables
		let iter_state = create_iter_state_with_tables(vec![], vec![], vec![], opts);

		let internal_range = crate::user_range_to_internal_range(
			Bound::Included("a".as_bytes()),
			Bound::Excluded("z".as_bytes()),
		);
		let iter = KMergeIterator::new_from(iter_state, internal_range, false);

		let count = count_kmerge_items(iter);
		assert_eq!(count, 0, "Iterator with no tables should return no items");
	}

	#[test]
	fn test_single_key_range() {
		let temp_dir = create_temp_directory();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		let opts = Arc::new(opts);

		let table1 = create_test_table_with_range(1, "a", "z", 1, opts.clone()).unwrap();

		let iter_state = create_iter_state_with_tables(vec![table1], vec![], vec![], opts);

		// Query for exact single key
		let internal_range = crate::user_range_to_internal_range(
			Bound::Included(b"a1".as_slice()),
			Bound::Included(b"a1".as_slice()),
		);
		let iter = KMergeIterator::new_from(iter_state, internal_range, false);

		let items: Vec<_> = iter.collect();
		// Should return at most 1 item
		for (key, _) in &items {
			assert_eq!(key.user_key.as_slice(), b"a1");
		}
	}

	#[test]
	fn test_inverted_range() {
		let temp_dir = create_temp_directory();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		let opts = Arc::new(opts);

		let table1 = create_test_table_with_range(1, "a", "m", 1, opts.clone()).unwrap();

		let iter_state = create_iter_state_with_tables(vec![table1], vec![], vec![], opts);

		// Inverted range: start > end
		let internal_range = crate::user_range_to_internal_range(
			Bound::Included("z".as_bytes()),
			Bound::Excluded("a".as_bytes()),
		);
		let iter = KMergeIterator::new_from(iter_state, internal_range, false);

		let count = count_kmerge_items(iter);
		assert_eq!(count, 0, "Inverted range should return no items");
	}

	#[test]
	fn test_mixed_level0_and_level1_tables() {
		let temp_dir = create_temp_directory();
		let opts = Options {
			path: temp_dir.path().to_path_buf(),
			..Default::default()
		};
		let opts = Arc::new(opts);

		// Create both L0 and L1 tables
		let l0_table = create_test_table_with_range(1, "a", "m", 1, opts.clone()).unwrap();
		let l1_table1 = create_test_table_with_range(11, "d", "h", 4, opts.clone()).unwrap();
		let l1_table2 = create_test_table_with_range(12, "i", "n", 7, opts.clone()).unwrap();

		let iter_state =
			create_iter_state_with_tables(vec![l0_table], vec![l1_table1, l1_table2], vec![], opts);

		// Query that overlaps with both levels
		let internal_range = crate::user_range_to_internal_range(
			Bound::Included("e".as_bytes()),
			Bound::Excluded("k".as_bytes()),
		);
		let iter = KMergeIterator::new_from(iter_state, internal_range, false);

		let count = count_kmerge_items(iter);
		assert!(count > 0, "Should have items from both L0 and L1 tables");
	}

	#[test(tokio::test)]
	async fn test_cache_effectiveness_with_range_query() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let tree = TreeBuilder::new()
			.with_path(path)
			.with_block_cache_capacity(10 * 1024 * 1024) // 10MB cache
			.with_max_memtable_size(500) // Small memtable to trigger flushes
			.build()
			.unwrap();

		eprintln!("\n=== Inserting 10,000 keys with periodic flushes ===");

		// Insert 10,000 keys, flushing every 1,000 keys
		for i in 0..10_000 {
			let key = format!("key_{:08}", i);
			let value = format!("value_{}", i);

			let mut tx = tree.begin().unwrap();
			tx.set(key.as_bytes(), value.as_bytes()).unwrap();
			tx.commit().await.unwrap();

			// Flush every 1,000 keys to create multiple SSTables
			if (i + 1) % 1_000 == 0 {
				tree.flush().unwrap();
				eprintln!("Flushed after {} keys", i + 1);
			}
		}

		// Final flush to ensure all data is on disk
		tree.flush().unwrap();
		eprintln!("Final flush completed\n");

		// Reset cache statistics before first query
		tree.core.opts.block_cache.reset_stats();

		eprintln!("=== First range query (populating cache) ===");
		// First range query - this will populate the cache
		let tx = tree.begin().unwrap();
		let first_results: Vec<_> = tx
			.range("key_00000000", "key_00010000")
			.unwrap()
			.collect::<crate::Result<Vec<_>>>()
			.unwrap();

		let first_stats = tree.core.opts.block_cache.get_stats();
		eprintln!("First query results: {} items", first_results.len());
		eprintln!("First query cache stats:");
		eprintln!(
			"  Data hits: {}, Data misses: {}",
			first_stats.data_hits, first_stats.data_misses
		);
		eprintln!(
			"  Index hits: {}, Index misses: {}",
			first_stats.index_hits, first_stats.index_misses
		);
		eprintln!(
			"  Total hits: {}, Total misses: {}",
			first_stats.total_hits(),
			first_stats.total_misses()
		);
		eprintln!("  Hit ratio: {:.2}%\n", first_stats.hit_ratio() * 100.0);

		// Reset cache statistics before second query
		tree.core.opts.block_cache.reset_stats();

		eprintln!("=== Second range query (served from cache) ===");
		// Second range query - should be served mostly from cache
		let tx = tree.begin().unwrap();
		let second_results: Vec<_> = tx
			.range(b"key_00000000", b"key_00010000")
			.unwrap()
			.collect::<crate::Result<Vec<_>>>()
			.unwrap();

		let second_stats = tree.core.opts.block_cache.get_stats();
		eprintln!("Second query results: {} items", second_results.len());
		eprintln!("Second query cache stats:");
		eprintln!(
			"  Data hits: {}, Data misses: {}",
			second_stats.data_hits, second_stats.data_misses
		);
		eprintln!(
			"  Index hits: {}, Index misses: {}",
			second_stats.index_hits, second_stats.index_misses
		);
		eprintln!(
			"  Total hits: {}, Total misses: {}",
			second_stats.total_hits(),
			second_stats.total_misses()
		);
		eprintln!("  Hit ratio: {:.2}%\n", second_stats.hit_ratio() * 100.0);

		// Assertions
		assert_eq!(first_results.len(), 10_000, "First query should return all 10,000 items");
		assert_eq!(second_results.len(), 10_000, "Second query should return all 10,000 items");
		assert!(
			second_stats.total_hits() > first_stats.total_hits() * 2,
			"Second query should have at least 2x more cache hits. First: {}, Second: {}",
			first_stats.total_hits(),
			second_stats.total_hits()
		);

		assert!(
			second_stats.hit_ratio() == 1.0,
			"Second query should have 100% cache hit ratio, got {:.2}%",
			second_stats.hit_ratio() * 100.0
		);

		tree.close().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_snapshot_iterator_seq_num_filtering_via_transactions() {
		let (store, _temp_dir) = create_store();

		// Insert key1 at seq_num ~1
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1_v1").unwrap();
			tx.commit().await.unwrap();
		}

		// Capture snapshot after key1 insert (should see only key1)
		let snapshot1 = store.begin().unwrap();

		// Insert key2 at seq_num ~2
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key2", b"value2_v1").unwrap();
			tx.commit().await.unwrap();
		}

		// Capture snapshot after key2 insert (should see key1 and key2)
		let snapshot2 = store.begin().unwrap();

		// Insert key3 at seq_num ~3
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key3", b"value3_v1").unwrap();
			tx.commit().await.unwrap();
		}

		// Capture snapshot after key3 insert (should see key1, key2, and key3)
		let snapshot3 = store.begin().unwrap();

		// Insert key4 at seq_num ~4
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key4", b"value4_v1").unwrap();
			tx.commit().await.unwrap();
		}

		// Capture snapshot after key4 insert (should see all keys)
		let snapshot4 = store.begin().unwrap();

		// Test snapshot1 - should only see key1
		{
			let snap_ref = snapshot1.snapshot.as_ref().unwrap();
			let range: Vec<_> = snap_ref
				.range(Some("key0".as_bytes()), Some("key9".as_bytes()), false)
				.unwrap()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();

			assert_eq!(range.len(), 1, "Snapshot1 should only see 1 key");
			assert_eq!(&range[0].0, b"key1");
			assert_eq!(range[0].1.as_ref().unwrap().as_slice(), b"value1_v1");
		}

		// Test snapshot2 - should see key1 and key2
		{
			let snap_ref = snapshot2.snapshot.as_ref().unwrap();
			let range: Vec<_> = snap_ref
				.range(Some("key0".as_bytes()), Some("key9".as_bytes()), false)
				.unwrap()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();

			assert_eq!(range.len(), 2, "Snapshot2 should see 2 keys");
			assert_eq!(&range[0].0, b"key1");
			assert_eq!(&range[1].0, b"key2");
		}

		// Test snapshot3 - should see key1, key2, and key3
		{
			let snap_ref = snapshot3.snapshot.as_ref().unwrap();
			let range: Vec<_> = snap_ref
				.range(Some("key0".as_bytes()), Some("key9".as_bytes()), false)
				.unwrap()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();

			assert_eq!(range.len(), 3, "Snapshot3 should see 3 keys");
			assert_eq!(&range[0].0, b"key1");
			assert_eq!(&range[1].0, b"key2");
			assert_eq!(&range[2].0, b"key3");
		}

		// Test snapshot4 - should see all 4 keys
		{
			let snap_ref = snapshot4.snapshot.as_ref().unwrap();
			let range: Vec<_> = snap_ref
				.range(Some("key0".as_bytes()), Some("key9".as_bytes()), false)
				.unwrap()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();

			assert_eq!(range.len(), 4, "Snapshot4 should see 4 keys");
			assert_eq!(&range[0].0, b"key1");
			assert_eq!(&range[1].0, b"key2");
			assert_eq!(&range[2].0, b"key3");
			assert_eq!(&range[3].0, b"key4");
		}

		// Test backward iteration on snapshot2
		{
			let snap_ref = snapshot2.snapshot.as_ref().unwrap();
			let range: Vec<_> = snap_ref
				.range(Some("key0".as_bytes()), Some("key9".as_bytes()), false)
				.unwrap()
				.rev()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();

			assert_eq!(range.len(), 2, "Backward iteration should also see 2 keys");
			assert_eq!(&range[0].0, b"key2");
			assert_eq!(&range[1].0, b"key1");
		}
	}

	#[test(tokio::test)]
	async fn test_snapshot_iterator_seq_num_filtering_with_updates() {
		let (store, _temp_dir) = create_store();

		// Insert initial value for key1
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value_v1").unwrap();
			tx.commit().await.unwrap();
		}

		// Snapshot after v1
		let snapshot1 = store.begin().unwrap();

		// Update key1 to v2
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value_v2").unwrap();
			tx.commit().await.unwrap();
		}

		// Snapshot after v2
		let snapshot2 = store.begin().unwrap();

		// Update key1 to v3
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value_v3").unwrap();
			tx.commit().await.unwrap();
		}

		// Snapshot after v3
		let snapshot3 = store.begin().unwrap();

		// Each snapshot should see its corresponding version
		{
			let snap1_ref = snapshot1.snapshot.as_ref().unwrap();
			let range: Vec<_> = snap1_ref
				.range(Some("key0".as_bytes()), Some("key9".as_bytes()), false)
				.unwrap()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();

			assert_eq!(range.len(), 1);
			assert_eq!(range[0].1.as_ref().unwrap().as_slice(), b"value_v1");
		}

		{
			let snap2_ref = snapshot2.snapshot.as_ref().unwrap();
			let range: Vec<_> = snap2_ref
				.range(Some("key0".as_bytes()), Some("key9".as_bytes()), false)
				.unwrap()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();

			assert_eq!(range.len(), 1);
			assert_eq!(range[0].1.as_ref().unwrap().as_slice(), b"value_v2");
		}

		{
			let snap3_ref = snapshot3.snapshot.as_ref().unwrap();
			let range: Vec<_> = snap3_ref
				.range(Some("key0".as_bytes()), Some("key9".as_bytes()), false)
				.unwrap()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();

			assert_eq!(range.len(), 1);
			assert_eq!(range[0].1.as_ref().unwrap().as_slice(), b"value_v3");
		}
	}

	#[test(tokio::test)]
	async fn test_snapshot_iterator_seq_num_with_deletions() {
		let (store, _temp_dir) = create_store();

		// Insert keys 1, 2, 3
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.commit().await.unwrap();
		}

		// Snapshot before deletion
		let snapshot_before = store.begin().unwrap();

		// Delete key2
		{
			let mut tx = store.begin().unwrap();
			tx.delete(b"key2").unwrap();
			tx.commit().await.unwrap();
		}

		// Snapshot after deletion
		let snapshot_after = store.begin().unwrap();

		// Snapshot before should see all 3 keys
		{
			let snap_ref = snapshot_before.snapshot.as_ref().unwrap();
			let range: Vec<_> = snap_ref
				.range(Some("key0".as_bytes()), Some("key9".as_bytes()), false)
				.unwrap()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();

			assert_eq!(range.len(), 3, "Before deletion: should see all 3 keys");
			assert_eq!(&range[0].0, b"key1");
			assert_eq!(&range[1].0, b"key2");
			assert_eq!(&range[2].0, b"key3");
		}

		// Snapshot after should not see deleted key2
		{
			let snap_ref = snapshot_after.snapshot.as_ref().unwrap();
			let range: Vec<_> = snap_ref
				.range(Some("key0".as_bytes()), Some("key9".as_bytes()), false)
				.unwrap()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();

			assert_eq!(range.len(), 2, "After deletion: should see only 2 keys");
			assert_eq!(&range[0].0, b"key1");
			assert_eq!(&range[1].0, b"key3");
			// key2 should not be present
		}

		// Test backward iteration after deletion
		{
			let snap_ref = snapshot_after.snapshot.as_ref().unwrap();
			let range: Vec<_> = snap_ref
				.range(Some("key0".as_bytes()), Some("key9".as_bytes()), false)
				.unwrap()
				.rev()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();

			assert_eq!(range.len(), 2, "Backward: should see only 2 keys");
			assert_eq!(&range[0].0, b"key3");
			assert_eq!(&range[1].0, b"key1");
		}
	}

	#[test(tokio::test)]
	async fn test_snapshot_iterator_seq_num_complex_scenario() {
		let (store, _temp_dir) = create_store();

		// Timeline:
		// 1. Insert key1, key2, key3
		let mut tx = store.begin().unwrap();
		tx.set(b"key1", b"v1").unwrap();
		tx.set(b"key2", b"v2").unwrap();
		tx.set(b"key3", b"v3").unwrap();
		tx.commit().await.unwrap();

		let snap1 = store.begin().unwrap();

		// 2. Update key2, insert key4
		let mut tx = store.begin().unwrap();
		tx.set(b"key2", b"v2_updated").unwrap();
		tx.set(b"key4", b"v4").unwrap();
		tx.commit().await.unwrap();

		let snap2 = store.begin().unwrap();

		// 3. Delete key1, insert key5
		let mut tx = store.begin().unwrap();
		tx.delete(b"key1").unwrap();
		tx.set(b"key5", b"v5").unwrap();
		tx.commit().await.unwrap();

		let snap3 = store.begin().unwrap();

		// Verify snap1: Should see key1(v1), key2(v2), key3(v3)
		{
			let snap_ref = snap1.snapshot.as_ref().unwrap();
			let range: Vec<_> = snap_ref
				.range(Some("key0".as_bytes()), Some("key9".as_bytes()), false)
				.unwrap()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();

			assert_eq!(range.len(), 3);
			assert_eq!(&range[0].0, b"key1");
			assert_eq!(range[0].1.as_ref().unwrap().as_slice(), b"v1");
			assert_eq!(&range[1].0, b"key2");
			assert_eq!(range[1].1.as_ref().unwrap().as_slice(), b"v2");
			assert_eq!(&range[2].0, b"key3");
		}

		// Verify snap2: Should see key1(v1), key2(v2_updated), key3(v3), key4(v4)
		{
			let snap_ref = snap2.snapshot.as_ref().unwrap();
			let range: Vec<_> = snap_ref
				.range(Some("key0".as_bytes()), Some("key9".as_bytes()), false)
				.unwrap()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();

			assert_eq!(range.len(), 4);
			assert_eq!(&range[0].0, b"key1");
			assert_eq!(&range[1].0, b"key2");
			assert_eq!(range[1].1.as_ref().unwrap().as_slice(), b"v2_updated"); // Updated value
			assert_eq!(&range[2].0, b"key3");
			assert_eq!(&range[3].0, b"key4");
		}

		// Verify snap3: Should see key2(v2_updated), key3(v3), key4(v4), key5(v5)
		// key1 should be deleted
		{
			let snap_ref = snap3.snapshot.as_ref().unwrap();
			let range: Vec<_> = snap_ref
				.range(Some("key0".as_bytes()), Some("key9".as_bytes()), false)
				.unwrap()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();

			assert_eq!(range.len(), 4);
			// key1 should NOT be present (deleted)
			assert_eq!(&range[0].0, b"key2");
			assert_eq!(range[0].1.as_ref().unwrap().as_slice(), b"v2_updated");
			assert_eq!(&range[1].0, b"key3");
			assert_eq!(&range[2].0, b"key4");
			assert_eq!(&range[3].0, b"key5");
		}

		// Verify backward iteration on snap2
		{
			let snap_ref = snap2.snapshot.as_ref().unwrap();
			let range: Vec<_> = snap_ref
				.range(Some("key0".as_bytes()), Some("key9".as_bytes()), false)
				.unwrap()
				.rev()
				.collect::<Result<Vec<_>, _>>()
				.unwrap();

			assert_eq!(range.len(), 4);
			assert_eq!(&range[0].0, b"key4");
			assert_eq!(&range[1].0, b"key3");
			assert_eq!(&range[2].0, b"key2");
			assert_eq!(range[2].1.as_ref().unwrap().as_slice(), b"v2_updated");
			assert_eq!(&range[3].0, b"key1");
		}
	}
}
