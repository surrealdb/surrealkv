use std::cmp::Ordering;
use std::fs::File;
use std::ops::Bound;
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
	Value,
};

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

	/// Creates a unified history iterator that works with both LSM and B+tree backends.
	///
	/// This method returns a `HistoryIterator` enum that abstracts over the underlying
	/// storage implementation, providing a single interface for versioned queries.
	///
	/// # Arguments
	/// * `lower` - Optional lower bound key (inclusive)
	/// * `upper` - Optional upper bound key (exclusive)
	/// * `include_tombstones` - Whether to include tombstones in the iteration
	///
	/// # Errors
	/// Returns an error if versioning is not enabled.
	pub(crate) fn history_iter(
		&self,
		lower: Option<&[u8]>,
		upper: Option<&[u8]>,
		include_tombstones: bool,
	) -> Result<HistoryIterator<'_>> {
		if !self.core.opts.enable_versioning {
			return Err(Error::InvalidArgument("Versioning not enabled".to_string()));
		}

		if self.core.opts.enable_versioned_index {
			let btree_iter = self.btree_history_iter(lower, upper)?;
			Ok(HistoryIterator::new_btree(
				btree_iter,
				Arc::clone(&self.core),
				self.seq_num,
				include_tombstones,
				lower,
				upper,
			))
		} else {
			// Create range for KMergeIterator
			let range = crate::user_range_to_internal_range(
				lower.map(Bound::Included).unwrap_or(Bound::Unbounded),
				upper.map(Bound::Excluded).unwrap_or(Bound::Unbounded),
			);
			let iter_state = self.collect_iter_state()?;
			Ok(HistoryIterator::new_lsm(
				Arc::clone(&self.core),
				self.seq_num,
				iter_state,
				range,
				include_tombstones,
			))
		}
	}

	/// Queries for a specific key at a specific timestamp.
	/// Only returns data visible to this snapshot (seq_num <= snapshot.seq_num).
	///
	/// Uses the unified `history_iter()` for both B+tree and LSM backends.
	pub(crate) fn get_at(&self, key: &[u8], timestamp: u64) -> Result<Option<Value>> {
		// Use unified history iterator for both backends
		let mut iter = self.history_iter(Some(key), None, true)?;
		iter.seek_first()?;

		// Track the best match (latest version at or before requested timestamp)
		let mut best_value: Option<Value> = None;
		let mut best_timestamp: u64 = 0;

		while iter.valid() {
			let entry_key = iter.key();

			// Stop if we've moved past our key
			if entry_key.user_key() != key {
				break;
			}

			// Only consider versions visible to this snapshot
			if entry_key.seq_num() > self.seq_num {
				iter.next()?;
				continue;
			}

			let entry_ts = entry_key.timestamp();

			// Only consider versions at or before the requested timestamp
			if entry_ts <= timestamp && entry_ts >= best_timestamp {
				if entry_key.is_tombstone() {
					// Key was deleted at this timestamp
					best_value = None;
				} else {
					best_value = Some(self.core.resolve_value(iter.value())?);
				}
				best_timestamp = entry_ts;
			}

			iter.next()?;
		}

		Ok(best_value)
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

// ===== Unified History Iterator =====

/// Internal enum for the underlying iterator type.
enum HistoryIteratorInner<'a> {
	/// LSM-based iterator (KMergeIterator handles bounds via InternalKeyRange)
	Lsm(KMergeIterator<'a>),
	/// B+tree streaming iterator (bounds checked manually)
	BTree(BPlusTreeIteratorWithGuard<'a>),
}

/// Unified history iterator that works with both LSM and B+tree backends.
///
/// This struct provides:
/// - Visibility filtering (seq_num <= snapshot_seq_num)
/// - Tombstone filtering (optional)
/// - Bounds checking (for B+tree; LSM handles via KMergeIterator)
/// - Bidirectional iteration support
///
/// The filtering logic is unified for both backends, eliminating the need
/// for a separate `VersionedSnapshotIterator`.
pub struct HistoryIterator<'a> {
	/// The underlying iterator (LSM or B+tree)
	inner: HistoryIteratorInner<'a>,

	/// Sequence number for visibility filtering
	snapshot_seq_num: u64,

	/// Whether to include tombstones in iteration
	include_tombstones: bool,

	/// Current direction of iteration
	direction: MergeDirection,

	/// Whether the iterator has been initialized (seek called)
	initialized: bool,

	/// Lower bound for B+tree (LSM handles bounds via KMergeIterator)
	lower_bound: Option<Vec<u8>>,

	/// Upper bound for B+tree (LSM handles bounds via KMergeIterator)
	upper_bound: Option<Vec<u8>>,

	/// Core reference for VLog iterator counting
	core: Arc<Core>,

	// === Tracking fields for REPLACE/DELETE filtering ===
	/// Current user key being iterated (for detecting key changes)
	current_user_key: Vec<u8>,
	/// Whether we've seen the first visible version for current user key
	first_visible_seen: bool,
	/// Whether the latest visible version is a hard delete
	latest_is_hard_delete: bool,
	/// Whether we've seen a REPLACE operation for current user key
	seen_replace: bool,
}

impl<'a> HistoryIterator<'a> {
	/// Creates a new LSM-based history iterator.
	///
	/// The bounds are passed to `KMergeIterator` via `InternalKeyRange`,
	/// so bounds checking is handled internally by the merge iterator.
	pub(crate) fn new_lsm(
		core: Arc<Core>,
		seq_num: u64,
		iter_state: IterState,
		range: InternalKeyRange,
		include_tombstones: bool,
	) -> Self {
		if let Some(ref vlog) = core.vlog {
			vlog.incr_iterator_count();
		}

		let merge_iter = KMergeIterator::new_from(iter_state, range);

		Self {
			inner: HistoryIteratorInner::Lsm(merge_iter),
			snapshot_seq_num: seq_num,
			include_tombstones,
			direction: MergeDirection::Forward,
			initialized: false,
			lower_bound: None, // LSM handles bounds via KMergeIterator
			upper_bound: None,
			core,
			current_user_key: Vec::new(),
			first_visible_seen: false,
			latest_is_hard_delete: false,
			seen_replace: false,
		}
	}

	/// Creates a new B+tree-based history iterator.
	///
	/// Bounds are stored and checked manually since `BPlusTreeIteratorWithGuard`
	/// doesn't have built-in bounds support.
	pub(crate) fn new_btree(
		btree_iter: BPlusTreeIteratorWithGuard<'a>,
		core: Arc<Core>,
		seq_num: u64,
		include_tombstones: bool,
		lower: Option<&[u8]>,
		upper: Option<&[u8]>,
	) -> Self {
		if let Some(ref vlog) = core.vlog {
			vlog.incr_iterator_count();
		}

		Self {
			inner: HistoryIteratorInner::BTree(btree_iter),
			snapshot_seq_num: seq_num,
			include_tombstones,
			direction: MergeDirection::Forward,
			initialized: false,
			lower_bound: lower.map(|b| b.to_vec()),
			upper_bound: upper.map(|b| b.to_vec()),
			core,
			current_user_key: Vec::new(),
			first_visible_seen: false,
			latest_is_hard_delete: false,
			seen_replace: false,
		}
	}

	/// Checks if a key is visible to this snapshot.
	#[inline]
	fn is_visible(&self, key: &InternalKeyRef<'_>) -> bool {
		key.seq_num() <= self.snapshot_seq_num
	}

	/// Checks if current position is within upper bound.
	/// - LSM: Returns true always (KMergeIterator enforces bounds internally)
	/// - B+tree: Manually checks upper_bound since BPlusTreeIteratorWithGuard has no bounds support
	fn within_upper_bound(&self) -> bool {
		match &self.inner {
			HistoryIteratorInner::Lsm(_) => {
				// LSM path: KMergeIterator already enforces bounds via InternalKeyRange
				true
			}
			HistoryIteratorInner::BTree(iter) => {
				if let Some(ref upper) = self.upper_bound {
					if iter.valid() {
						iter.key().user_key() < upper.as_slice()
					} else {
						false
					}
				} else {
					true
				}
			}
		}
	}

	/// Checks if current position is within lower bound (for backward iteration).
	fn within_lower_bound(&self) -> bool {
		match &self.inner {
			HistoryIteratorInner::Lsm(_) => true,
			HistoryIteratorInner::BTree(iter) => {
				if let Some(ref lower) = self.lower_bound {
					if iter.valid() {
						iter.key().user_key() >= lower.as_slice()
					} else {
						false
					}
				} else {
					true
				}
			}
		}
	}

	/// Returns whether the inner iterator is valid.
	fn inner_valid(&self) -> bool {
		match &self.inner {
			HistoryIteratorInner::Lsm(iter) => iter.valid(),
			HistoryIteratorInner::BTree(iter) => iter.valid(),
		}
	}

	/// Gets the key from the inner iterator.
	fn inner_key(&self) -> InternalKeyRef<'_> {
		match &self.inner {
			HistoryIteratorInner::Lsm(iter) => iter.key(),
			HistoryIteratorInner::BTree(iter) => iter.key(),
		}
	}

	/// Advances the inner iterator forward.
	fn inner_next(&mut self) -> Result<bool> {
		match &mut self.inner {
			HistoryIteratorInner::Lsm(iter) => iter.next(),
			HistoryIteratorInner::BTree(iter) => iter.next(),
		}
	}

	/// Moves the inner iterator backward.
	fn inner_prev(&mut self) -> Result<bool> {
		match &mut self.inner {
			HistoryIteratorInner::Lsm(iter) => iter.prev(),
			HistoryIteratorInner::BTree(iter) => iter.prev(),
		}
	}

	/// Skip to the next valid entry in forward direction.
	/// Handles visibility, tombstones, REPLACE, DELETE filtering, and bounds for BOTH backends.
	///
	/// Filtering rules:
	/// - Hard DELETE as latest visible: skip ALL versions of this key
	/// - REPLACE seen: skip older non-REPLACE versions, output only REPLACE versions
	/// - Soft DELETE: affected by include_tombstones flag only
	/// - Normal SET: output all versions
	fn skip_to_valid_forward(&mut self) -> Result<bool> {
		while self.inner_valid() {
			// Check bounds first (B+tree only, LSM returns true)
			if !self.within_upper_bound() {
				return Ok(false);
			}

			// Extract data before mutations (borrow checker)
			let (user_key_owned, seq_num, is_hard_delete, is_replace, is_tombstone) = {
				let key_ref = self.inner_key();
				(
					key_ref.user_key().to_vec(),
					key_ref.seq_num(),
					key_ref.is_hard_delete_marker(),
					key_ref.is_replace(),
					key_ref.is_tombstone(),
				)
			};

			// Detect user_key change - reset tracking state
			let is_new_user_key = user_key_owned != self.current_user_key;
			if is_new_user_key {
				self.current_user_key = user_key_owned;
				self.first_visible_seen = false;
				self.latest_is_hard_delete = false;
				self.seen_replace = false;
			}

			// Skip invisible versions (seq_num > snapshot)
			if seq_num > self.snapshot_seq_num {
				self.inner_next()?;
				continue;
			}

			// First visible entry - check if it's hard delete
			if !self.first_visible_seen {
				self.first_visible_seen = true;
				if is_hard_delete {
					self.latest_is_hard_delete = true;
				}
			}

			// Hard DELETE as latest: skip ALL versions of this key
			if self.latest_is_hard_delete {
				self.inner_next()?;
				continue;
			}

			// Track REPLACE
			if is_replace {
				self.seen_replace = true;
			}

			// After REPLACE, skip non-REPLACE versions
			if self.seen_replace && !is_replace {
				self.inner_next()?;
				continue;
			}

			// Skip older hard deletes (not latest)
			if is_hard_delete {
				self.inner_next()?;
				continue;
			}

			// Skip soft delete tombstones if not included
			// (is_tombstone includes both soft and hard deletes, but hard deletes
			// are already handled above, so this only affects soft deletes)
			if !self.include_tombstones && is_tombstone {
				self.inner_next()?;
				continue;
			}

			// Found valid entry
			return Ok(true);
		}
		Ok(false)
	}

	/// Skip to the next valid entry in backward direction.
	///
	/// Note: Backward iteration uses simpler filtering (visibility + tombstones only).
	/// Full REPLACE/DELETE tracking would require looking ahead since versions
	/// appear in reverse order (oldest first for each user_key).
	fn skip_to_valid_backward(&mut self) -> Result<bool> {
		while self.inner_valid() {
			// Check bounds first (B+tree only, LSM returns true)
			if !self.within_lower_bound() {
				return Ok(false);
			}

			let key_ref = self.inner_key();

			// Skip invisible versions (seq_num > snapshot)
			if !self.is_visible(&key_ref) {
				self.inner_prev()?;
				continue;
			}

			// Skip tombstones if not included
			if !self.include_tombstones && key_ref.is_tombstone() {
				self.inner_prev()?;
				continue;
			}

			// Found valid entry
			return Ok(true);
		}
		Ok(false)
	}
}

impl InternalIterator for HistoryIterator<'_> {
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		match &mut self.inner {
			HistoryIteratorInner::Lsm(iter) => iter.seek(target)?,
			HistoryIteratorInner::BTree(iter) => iter.seek(target)?,
		};
		self.initialized = true;
		self.skip_to_valid_forward()
	}

	fn seek_first(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		match &mut self.inner {
			HistoryIteratorInner::Lsm(iter) => {
				iter.seek_first()?;
			}
			HistoryIteratorInner::BTree(iter) => {
				// Seek to lower bound if present, otherwise start of tree
				if let Some(ref lower) = self.lower_bound {
					// B+tree is ordered by (user_key ASC, timestamp DESC)
					// With DESC order, timestamp=u64::MAX is smallest (comes first)
					// So we seek with max timestamp to find the first entry for this user_key
					let seek_key =
						InternalKey::new(lower.clone(), u64::MAX, InternalKeyKind::Set, u64::MAX);
					iter.seek(&seek_key.encode())?;
				} else {
					iter.seek_first()?;
				}
			}
		}
		self.initialized = true;
		self.skip_to_valid_forward()
	}

	fn seek_last(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Backward;
		match &mut self.inner {
			HistoryIteratorInner::Lsm(iter) => {
				iter.seek_last()?;
			}
			HistoryIteratorInner::BTree(iter) => {
				// For B+tree, we need to seek to end and work backwards
				// If upper bound exists, seek to it and go back
				if let Some(ref upper) = self.upper_bound {
					iter.seek(upper)?;
					// If we're at or past upper, go back one
					if iter.valid() && iter.key().user_key() >= upper.as_slice() {
						iter.prev()?;
					}
				} else {
					iter.seek_last()?;
				}
			}
		}
		self.initialized = true;
		self.skip_to_valid_backward()
	}

	fn next(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_first();
		}
		if !self.inner_valid() {
			return Ok(false);
		}
		self.inner_next()?;
		self.skip_to_valid_forward()
	}

	fn prev(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_last();
		}
		// If we were going forward, need to reset for backward
		if self.direction != MergeDirection::Backward {
			self.direction = MergeDirection::Backward;
		}
		if !self.inner_valid() {
			return Ok(false);
		}
		self.inner_prev()?;
		self.skip_to_valid_backward()
	}

	fn valid(&self) -> bool {
		if !self.inner_valid() {
			return false;
		}
		// Check bounds based on direction
		match self.direction {
			MergeDirection::Forward => self.within_upper_bound(),
			MergeDirection::Backward => self.within_lower_bound(),
		}
	}

	fn key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.valid());
		self.inner_key()
	}

	fn value(&self) -> &[u8] {
		debug_assert!(self.valid());
		match &self.inner {
			HistoryIteratorInner::Lsm(iter) => iter.value(),
			HistoryIteratorInner::BTree(iter) => iter.value(),
		}
	}
}

impl Drop for HistoryIterator<'_> {
	fn drop(&mut self) {
		// Decrement VLog iterator count for BOTH paths (both increment in constructor)
		// B+tree also stores VLog pointers as values, so it needs VLog access
		if let Some(ref vlog) = self.core.vlog {
			if let Err(e) = vlog.decr_iterator_count() {
				log::warn!("Failed to decrement VLog iterator count: {e}");
			}
		}
		// B+tree guard (RwLockReadGuard) is automatically dropped when `inner` is dropped
	}
}
