use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::error::{Error, Result};
use crate::iter::BoxedLSMIterator;
use crate::levels::Levels;
use crate::lsm::Core;
use crate::memtable::MemTable;
use crate::sstable::table::Table;
use crate::vlog::ValueLocation;
use crate::{
	BytewiseComparator,
	Comparator,
	InternalKey,
	InternalKeyComparator,
	InternalKeyKind,
	InternalKeyRange,
	InternalKeyRef,
	Key,
	LSMIterator,
	Value,
};

// ===== Snapshot Tracker =====
/// Tracks active snapshot sequence numbers in the system.
///
/// This tracker maintains the actual sequence numbers of active snapshots,
/// enabling snapshot-aware compaction. During compaction, versions that are
/// visible to any active snapshot must be preserved.
///
/// # Compaction Integration
///
/// The compaction iterator uses `get_all_snapshots()` to obtain a sorted list
/// of active snapshot sequence numbers. For each version being considered for
/// removal, it checks if the version is visible to any snapshot using binary
/// search. Versions visible to snapshots are preserved unless hidden by a newer
/// version in the same visibility boundary.
const NUM_SNAPSHOT_SHARDS: usize = 64;

fn get_thread_shard_index() -> usize {
	static SHARD_COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
	thread_local! {
		static SHARD_ID: usize = SHARD_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	}
	SHARD_ID.with(|id| *id % NUM_SNAPSHOT_SHARDS)
}

#[derive(Clone)]
pub(crate) struct SnapshotTracker {
	shards: Arc<[RwLock<BTreeMap<u64, usize>>; NUM_SNAPSHOT_SHARDS]>,
}

impl Default for SnapshotTracker {
	fn default() -> Self {
		Self::new()
	}
}

impl std::fmt::Debug for SnapshotTracker {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("SnapshotTracker").field("snapshots", &self.get_all_snapshots()).finish()
	}
}

impl SnapshotTracker {
	/// Creates a new empty snapshot tracker with 64 shards.
	pub(crate) fn new() -> Self {
		let shards: Vec<RwLock<BTreeMap<u64, usize>>> =
			(0..NUM_SNAPSHOT_SHARDS).map(|_| RwLock::new(BTreeMap::new())).collect();
		let shards: Box<[RwLock<BTreeMap<u64, usize>>; NUM_SNAPSHOT_SHARDS]> =
			shards.into_boxed_slice().try_into().unwrap_or_else(|_| panic!("size mismatch"));
		Self {
			shards: Arc::from(shards),
		}
	}

	/// Registers a new snapshot with the given sequence number.
	/// Returns the shard index assigned to this registration.
	pub(crate) fn register(&self, seq_num: u64) -> usize {
		let shard_idx = get_thread_shard_index();
		let mut lock = self.shards[shard_idx].write();
		*lock.entry(seq_num).or_insert(0) += 1;
		shard_idx
	}

	/// Unregisters a snapshot with the given sequence number and optional shard index.
	pub(crate) fn unregister(&self, seq_num: u64, shard_idx: Option<usize>) {
		if let Some(idx) = shard_idx {
			let mut lock = self.shards[idx % NUM_SNAPSHOT_SHARDS].write();
			if let std::collections::btree_map::Entry::Occupied(mut entry) = lock.entry(seq_num) {
				if *entry.get() <= 1 {
					entry.remove();
				} else {
					*entry.get_mut() -= 1;
				}
			}
		} else {
			for shard in self.shards.iter() {
				let mut lock = shard.write();
				if let std::collections::btree_map::Entry::Occupied(mut entry) = lock.entry(seq_num)
				{
					if *entry.get() <= 1 {
						entry.remove();
					} else {
						*entry.get_mut() -= 1;
					}
					break;
				}
			}
		}
	}

	/// Returns all active snapshots as a sorted vector.
	pub(crate) fn get_all_snapshots(&self) -> Vec<u64> {
		let mut snapshots = Vec::new();
		for shard in self.shards.iter() {
			snapshots.extend(shard.read().keys().copied());
		}
		snapshots.sort_unstable();
		snapshots.dedup();
		snapshots
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

impl IterState {
	/// The range deletions of every memtable and table of the state, `(start, end, seq)`: those
	/// that cover `key` if it is given. Of the memtables and tables, only those that have any
	/// are looked in.
	pub(crate) fn range_deletions(&self, key: Option<&[u8]>) -> Vec<(Key, Key, u64)> {
		let mut range_deletions = Vec::new();
		let covers = |start: &Key, end: &Key| {
			key.is_none_or(|key| key >= start.as_slice() && key < end.as_slice())
		};
		let mut add = |list: &[(Key, Key, u64)]| {
			range_deletions
				.extend(list.iter().filter(|(start, end, _)| covers(start, end)).cloned());
		};
		for memtable in std::iter::once(&self.active).chain(&self.immutable) {
			if memtable.has_range_deletions() {
				add(&memtable.range_deletions.read());
			}
		}
		for level in &self.levels {
			for table in &level.tables {
				if table.has_range_deletions() {
					add(&table.range_deletions.read());
				}
			}
		}
		range_deletions
	}
}

/// Whether a range deletion that the snapshot at `snapshot_seq` reads hides the version of
/// `user_key` written at `seq`: one written before it, or with it.
fn covered_by_range(
	range_deletions: &[(Key, Key, u64)],
	snapshot_seq: u64,
	user_key: &[u8],
	seq: u64,
) -> bool {
	range_deletions.iter().any(|(start, end, rseq)| {
		seq <= *rseq
			&& *rseq <= snapshot_seq
			&& user_key >= start.as_slice()
			&& user_key < end.as_slice()
	})
}

// ===== Snapshot Implementation =====
/// A consistent point-in-time view of the LSM tree.
///
/// # Snapshot Isolation in LSM Trees
///
/// Snapshots provide consistent reads by fixing a sequence number at creation
/// time. All reads through the snapshot only see data with sequence numbers
/// less than or equal to the snapshot's sequence number.
pub(crate) struct Snapshot {
	/// Reference to the LSM tree core
	core: Arc<Core>,

	/// Sequence number defining this snapshot's view of the data
	/// Only data with seq_num <= this value is visible
	pub(crate) seq_num: u64,

	/// The shard index where this snapshot was registered
	shard_idx: usize,
}

impl Snapshot {
	/// Creates a new snapshot at the current sequence number
	pub(crate) fn new(core: Arc<Core>, seq_num: u64) -> Self {
		let shard_idx = core.snapshot_tracker.register(seq_num);

		Self {
			core,
			seq_num,
			shard_idx,
		}
	}

	/// Collects the iterator state from all LSM components
	/// This is a helper method used by both iterators and optimized operations
	/// like count
	pub(crate) fn collect_iter_state(&self) -> Result<IterState> {
		// The three locks are held together, in the order everything else takes them: a restore
		// replaces the memtables and the manifest under all of them, and a state read one lock
		// at a time could be the memtable from before it over the tables it restored.
		let active_guard = self.core.active_memtable.read()?;
		let levels_guard = self.core.level_manifest.read()?;
		let immutable_guard = self.core.immutable_memtables.read()?;
		let active = Arc::clone(&active_guard);
		let immutable = immutable_guard.iter().map(|entry| Arc::clone(&entry.memtable)).collect();
		let levels = levels_guard.levels.clone();
		drop((immutable_guard, levels_guard, active_guard));

		Ok(IterState {
			active,
			immutable,
			levels,
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
		let mut max_range_delete_seq: Option<u64> = None;

		// 1. Check active memtable
		{
			let active_lock = self.core.active_memtable.read()?;
			if active_lock.has_range_deletions() {
				for (start, end, seq) in active_lock.range_deletions.read().iter() {
					if *seq <= self.seq_num && key >= start.as_slice() && key < end.as_slice() {
						max_range_delete_seq =
							Some(max_range_delete_seq.map_or(*seq, |s| s.max(*seq)));
					}
				}
			}

			if let Some(item) = active_lock.get(key, Some(self.seq_num)) {
				if item.0.is_tombstone() {
					return Ok(None);
				}
				if let Some(rseq) = max_range_delete_seq {
					if item.0.seq_num() <= rseq {
						return Ok(None);
					}
				}
				return Ok(Some((item.1, item.0.seq_num())));
			}
		}

		// 2. Check immutable memtables
		{
			let imm_lock = self.core.immutable_memtables.read()?;
			for entry in imm_lock.iter().rev() {
				let memtable = &entry.memtable;
				if memtable.has_range_deletions() {
					for (start, end, seq) in memtable.range_deletions.read().iter() {
						if *seq <= self.seq_num && key >= start.as_slice() && key < end.as_slice() {
							max_range_delete_seq =
								Some(max_range_delete_seq.map_or(*seq, |s| s.max(*seq)));
						}
					}
				}
				if let Some(item) = memtable.get(key, Some(self.seq_num)) {
					if item.0.is_tombstone() {
						return Ok(None);
					}
					if let Some(rseq) = max_range_delete_seq {
						if item.0.seq_num() <= rseq {
							return Ok(None);
						}
					}
					return Ok(Some((item.1, item.0.seq_num())));
				}
			}
		}

		// 3. Check SSTables in level manifest
		let level_manifest = self.core.level_manifest.read()?;

		for level in &level_manifest.levels {
			for table in &level.tables {
				if table.has_range_deletions() {
					for (start, end, seq) in table.range_deletions.read().iter() {
						if *seq <= self.seq_num && key >= start.as_slice() && key < end.as_slice() {
							max_range_delete_seq =
								Some(max_range_delete_seq.map_or(*seq, |s| s.max(*seq)));
						}
					}
				}
			}
		}

		let ikey = InternalKey::new(key.to_vec(), self.seq_num, InternalKeyKind::Set);

		for (level_idx, level) in level_manifest.levels.get_levels().iter().enumerate() {
			if level_idx == 0 {
				// Level 0: Tables can overlap, check newest to oldest (tables are sorted
				// descending)
				for table in level.tables.iter() {
					if !table.is_user_key_in_range(key) {
						continue;
					}

					let maybe_item = table.get(&ikey)?;

					if let Some(item) = maybe_item {
						let ikey = &item.0;
						if ikey.is_tombstone() {
							return Ok(None);
						}
						if let Some(rseq) = max_range_delete_seq {
							if ikey.seq_num() <= rseq {
								return Ok(None);
							}
						}
						return Ok(Some((item.1, ikey.seq_num())));
					}
				}
			} else {
				// Level 1+: Non-overlapping, binary search for the one table with zero allocations
				if let Some(table) = level.find_table_for_user_key(key, &self.core.opts.comparator)
				{
					let maybe_item = table.get(&ikey)?;

					if let Some(item) = maybe_item {
						let ikey = &item.0;
						if ikey.is_tombstone() {
							return Ok(None);
						}
						if let Some(rseq) = max_range_delete_seq {
							if ikey.seq_num() <= rseq {
								return Ok(None);
							}
						}
						return Ok(Some((item.1, ikey.seq_num())));
					}
				}
			}
		}

		Ok(None)
	}

	/// Asynchronously gets a value from the snapshot.
	pub(crate) async fn get_async(&self, key: &[u8]) -> crate::Result<Option<(Value, u64)>> {
		let mut max_range_delete_seq: Option<u64> = None;

		// 1. Check active memtable and collect range deletions
		{
			let active_lock = self.core.active_memtable.read()?;
			if active_lock.has_range_deletions() {
				for (start, end, seq) in active_lock.range_deletions.read().iter() {
					if *seq <= self.seq_num && key >= start.as_slice() && key < end.as_slice() {
						max_range_delete_seq =
							Some(max_range_delete_seq.map_or(*seq, |s| s.max(*seq)));
					}
				}
			}
			if let Some(item) = active_lock.get(key, Some(self.seq_num)) {
				if item.0.is_tombstone() {
					return Ok(None);
				}
				if let Some(rseq) = max_range_delete_seq {
					if item.0.seq_num() <= rseq {
						return Ok(None);
					}
				}
				return Ok(Some((item.1, item.0.seq_num())));
			}
		}

		// 2. Check immutable memtables and collect range deletions
		{
			let imm_lock = self.core.immutable_memtables.read()?;
			for entry in imm_lock.iter().rev() {
				let memtable = &entry.memtable;
				if memtable.has_range_deletions() {
					for (start, end, seq) in memtable.range_deletions.read().iter() {
						if *seq <= self.seq_num && key >= start.as_slice() && key < end.as_slice() {
							max_range_delete_seq =
								Some(max_range_delete_seq.map_or(*seq, |s| s.max(*seq)));
						}
					}
				}
				if let Some(item) = memtable.get(key, Some(self.seq_num)) {
					if item.0.is_tombstone() {
						return Ok(None);
					}
					if let Some(rseq) = max_range_delete_seq {
						if item.0.seq_num() <= rseq {
							return Ok(None);
						}
					}
					return Ok(Some((item.1, item.0.seq_num())));
				}
			}
		}

		// 3. Collect level tables while holding level_manifest lock, then drop lock before awaiting
		let ikey = InternalKey::new(key.to_vec(), self.seq_num, InternalKeyKind::Set);
		let tables_to_check: Vec<Arc<Table>> = {
			let level_manifest = self.core.level_manifest.read()?;
			for level in &level_manifest.levels {
				for table in &level.tables {
					if table.has_range_deletions() {
						for (start, end, seq) in table.range_deletions.read().iter() {
							if *seq <= self.seq_num
								&& key >= start.as_slice()
								&& key < end.as_slice()
							{
								max_range_delete_seq =
									Some(max_range_delete_seq.map_or(*seq, |s| s.max(*seq)));
							}
						}
					}
				}
			}

			let mut tables = Vec::new();
			for (level_idx, level) in level_manifest.levels.get_levels().iter().enumerate() {
				if level_idx == 0 {
					for table in level.tables.iter() {
						if table.is_user_key_in_range(key) {
							tables.push(Arc::clone(table));
						}
					}
				} else if let Some(table) =
					level.find_table_for_user_key(key, &self.core.opts.comparator)
				{
					tables.push(Arc::clone(table));
				}
			}
			tables
		};

		// 4. Asynchronously search SSTables without holding any synchronous locks across await
		for table in tables_to_check {
			let maybe_item = table.get_async(&ikey).await?;
			if let Some(item) = maybe_item {
				let ikey = &item.0;
				if ikey.is_tombstone() {
					return Ok(None);
				}
				if let Some(rseq) = max_range_delete_seq {
					if ikey.seq_num() <= rseq {
						return Ok(None);
					}
				}
				return Ok(Some((item.1, ikey.seq_num())));
			}
		}

		Ok(None)
	}

	/// Creates an iterator for a range scan within the snapshot
	/// Returns a SnapshotIterator that implements LSMIterator
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

	/// Creates a history iterator: every version of every key in the range, newest first within
	/// a key, over the memtables and the tables.
	///
	/// A version's timestamp is stored with its value, and the iterator's `key()` reports it.
	///
	/// # Arguments
	/// * `lower` - Optional lower bound key (inclusive)
	/// * `upper` - Optional upper bound key (exclusive)
	/// * `include_tombstones` - Whether to include tombstones in the iteration
	/// * `ts_range` - Optional timestamp range filter (start_ts, end_ts) inclusive
	/// * `limit` - Optional limit on total entries returned
	///
	/// # Errors
	/// Returns an error if versioning is not enabled.
	pub(crate) fn history_iter(
		&self,
		lower: Option<&[u8]>,
		upper: Option<&[u8]>,
		include_tombstones: bool,
		ts_range: Option<(u64, u64)>,
		limit: Option<usize>,
	) -> Result<HistoryIterator<'_>> {
		if !self.core.opts.enable_versioning {
			return Err(Error::InvalidArgument("Versioning not enabled".to_string()));
		}

		let range = crate::user_range_to_internal_range(
			lower.map(Bound::Included).unwrap_or(Bound::Unbounded),
			upper.map(Bound::Excluded).unwrap_or(Bound::Unbounded),
		);
		let iter_state = self.collect_iter_state()?;
		let range_deletions = iter_state.range_deletions(None);

		Ok(HistoryIterator::new_lsm(
			self.seq_num,
			iter_state,
			range_deletions,
			range,
			include_tombstones,
			ts_range,
			limit,
			lower,
			upper,
		))
	}

	/// Queries for a specific key at a specific timestamp: the version of it that `get_at`
	/// answers with, with its time. `None` if the key has no version at or before `timestamp`,
	/// and a version without a value if that is a soft delete.
	/// Only returns data visible to this snapshot (seq_num <= snapshot.seq_num).
	///
	/// The answer is the version with the newest timestamp that is at or before `timestamp`, and
	/// the newest write among versions of one timestamp: the first version `history` returns
	/// that is not after `timestamp`. A version written without a timestamp, by a database that
	/// had none or while versioning was off, counts as written when the versions before it
	/// were. A hard delete and a range delete hide every version written before them, whatever
	/// their time: the key has no history before it.
	///
	/// The versions of the key are read once, oldest write first, and only the best so far is
	/// held: a version that has no time reads as written when the ones before it were, which is
	/// known as they come in that order.
	pub(crate) fn version_at(&self, key: &[u8], timestamp: u64) -> Result<Option<SnapshotVersion>> {
		if !self.core.opts.enable_versioning {
			return Err(Error::InvalidArgument("Versioning not enabled".to_string()));
		}

		let mut upper = key.to_vec();
		upper.push(0);
		let range = crate::user_range_to_internal_range(
			Bound::Included(key),
			Bound::Excluded(upper.as_slice()),
		);
		let iter_state = self.collect_iter_state()?;
		let range_deletions = iter_state.range_deletions(Some(key));
		let mut merge = KMergeIterator::new_from(iter_state, range);
		// After every version of the key: the next key's first entry, or the end. Going back from
		// there reads the versions of the key from the oldest write to the newest.
		merge.seek(&InternalKey::new(key.to_vec(), 0, InternalKeyKind::Set).encode())?;
		if merge.valid() {
			merge.prev()?;
		} else {
			merge.seek_last()?;
		}

		// `(time, is a tombstone, encoded value)` of the best version so far, and the newest time
		// of the versions read.
		let mut best: Option<(u64, bool, Vec<u8>)> = None;
		let mut newest = 0u64;
		while merge.valid() {
			let key_ref = merge.key();
			if key_ref.user_key() != key {
				break;
			}
			let seq = key_ref.seq_num();
			if seq > self.seq_num || covered_by_range(&range_deletions, self.seq_num, key, seq) {
				merge.prev()?;
				continue;
			}
			// A hard delete hides every version written before it.
			if key_ref.is_hard_delete_marker() {
				best = None;
				newest = 0;
				merge.prev()?;
				continue;
			}
			// So does a replace, which is a version too.
			if key_ref.is_replace() {
				best = None;
				newest = 0;
			}
			let value = merge.value_encoded()?;
			let time = match ValueLocation::entry_time(key_ref.kind(), value) {
				Some(time) => {
					newest = newest.max(time);
					time
				}
				None => newest,
			};
			// Oldest write first: a version of the same time as the best is newer.
			if time <= timestamp && best.as_ref().is_none_or(|(best_time, ..)| time >= *best_time) {
				best = Some((time, key_ref.is_tombstone(), value.to_vec()));
			}
			merge.prev()?;
		}

		match best {
			Some((time, false, value)) => Ok(Some(SnapshotVersion {
				time,
				value: Some(self.core.resolve_value(&value)?),
			})),
			Some((time, true, _)) => Ok(Some(SnapshotVersion {
				time,
				value: None,
			})),
			None => Ok(None),
		}
	}
}

/// A version of a key as a snapshot holds it: when it was written, and its value, or none if it
/// is a soft delete.
pub(crate) struct SnapshotVersion {
	pub(crate) time: u64,
	pub(crate) value: Option<Value>,
}

impl Drop for Snapshot {
	fn drop(&mut self) {
		// Unregister this snapshot's sequence number so compaction can
		// clean up versions no longer visible to any snapshot
		self.core.snapshot_tracker.unregister(self.seq_num, Some(self.shard_idx));
	}
}

/// Whether every version a table holds is older than the timestamp range, so that the table has
/// nothing to add to a history of that range.
///
/// Only the tables before the range can be left out, and not those after it: a version written
/// without a time reads as written when the versions before it were, which can be those of a table
/// that lies after the range, and without the table it would read as written at a time inside it.
/// A table before the range can only give a time that is before it as well, whatever is read of it.
/// A table that holds a hard delete, a replace, or a version without a time records no times, and
/// is never left out.
fn table_lies_before(table: &crate::sstable::table::Table, ts_range: Option<(u64, u64)>) -> bool {
	let Some((ts_start, _)) = ts_range else {
		return false;
	};
	let props = &table.meta.properties;
	matches!((props.newest_key_time, props.oldest_key_time), (Some(newest), Some(_)) if newest < ts_start)
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
	iterators: Vec<BoxedLSMIterator<'iter>>,

	// Owned state retained for drop order and backing buffer lifetime
	_iter_state: Box<IterState>,

	/// Current winner index (None if exhausted)
	winner: Option<usize>,

	/// Number of active (valid) iterators
	active_count: usize,

	/// Direction of iteration
	direction: MergeDirection,

	/// Whether the iterator has been initialized
	initialized: bool,

	/// Comparator for key comparison
	cmp: Arc<dyn Comparator>,
}

impl<'a> KMergeIterator<'a> {
	/// Creates a new KMergeIterator with InternalKeyComparator (default).
	/// Use this for normal queries where ordering is by seq_num.
	pub(crate) fn new_from(iter_state: IterState, internal_range: InternalKeyRange) -> Self {
		let cmp: Arc<dyn Comparator> =
			Arc::new(InternalKeyComparator::new(Arc::new(BytewiseComparator::default())));
		Self::new_with_comparator(iter_state, internal_range, cmp, None)
	}

	/// Creates a new KMergeIterator for history queries. The versions of a key are in the order
	/// of their sequence numbers, newest first, as everywhere else; tables whose timestamps all
	/// lie outside `ts_range` are not read.
	pub(crate) fn new_for_history(
		iter_state: IterState,
		internal_range: InternalKeyRange,
		ts_range: Option<(u64, u64)>,
	) -> Self {
		let cmp: Arc<dyn Comparator> =
			Arc::new(InternalKeyComparator::new(Arc::new(BytewiseComparator::default())));
		Self::new_with_comparator(iter_state, internal_range, cmp, ts_range)
	}

	/// Creates a new KMergeIterator with a configurable comparator.
	fn new_with_comparator(
		iter_state: IterState,
		internal_range: InternalKeyRange,
		cmp: Arc<dyn Comparator>,
		ts_range: Option<(u64, u64)>,
	) -> Self {
		let boxed_state = Box::new(iter_state);

		let query_range = Arc::new(internal_range);

		// Pre-allocate capacity for the iterators.
		// 1 active memtable + immutable memtables + level tables.
		let mut iterators: Vec<BoxedLSMIterator<'a>> =
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
		iterators.push(Box::new(active_iter) as BoxedLSMIterator<'a>);

		// Immutable memtables
		for memtable in &state_ref.immutable {
			let iter = memtable.range(lower, upper);
			iterators.push(Box::new(iter) as BoxedLSMIterator<'a>);
		}

		// Tables - these have native seek support
		for (level_idx, level) in (&state_ref.levels).into_iter().enumerate() {
			// Optimization: Skip tables that are completely outside the query range
			if level_idx == 0 {
				// Level 0: Tables can overlap, so we check all but skip those completely
				// outside range
				for table in &level.tables {
					// Skip tables completely before or after the range, and those without points
					if !table.has_points()
						|| table.is_before_range(&query_range)
						|| table.is_after_range(&query_range)
					{
						continue;
					}
					// Skip tables whose versions all lie before the timestamp range (if specified)
					if table_lies_before(table, ts_range) {
						continue;
					}
					// Use custom comparator for table iteration
					if let Ok(table_iter) =
						table.iter_with_comparator(Some((*query_range).clone()), Arc::clone(&cmp))
					{
						iterators.push(Box::new(table_iter) as BoxedLSMIterator<'a>);
					}
				}
			} else {
				// Level 1+: Tables have non-overlapping key ranges, use binary search
				let start_idx = level.find_first_overlapping_table(&query_range);
				let end_idx = level.find_last_overlapping_table(&query_range);

				for table in &level.tables[start_idx..end_idx] {
					// Skip tables whose versions all lie before the timestamp range (if specified)
					if table_lies_before(table, ts_range) {
						continue;
					}
					// Use custom comparator for table iteration
					if let Ok(table_iter) =
						table.iter_with_comparator(Some((*query_range).clone()), Arc::clone(&cmp))
					{
						iterators.push(Box::new(table_iter) as BoxedLSMIterator<'a>);
					}
				}
			}
		}

		Self {
			iterators,
			_iter_state: boxed_state,
			winner: None,
			active_count: 0,
			direction: MergeDirection::Forward,
			initialized: false,
			cmp,
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

	/// Switch from backward to forward, positioning just after `target`.
	///
	/// When switching directions, the current iterator just moves forward once.
	/// Non-current iterators need to be positioned at a key strictly greater than
	/// `target` to ensure correct ordering.
	fn switch_to_forward(&mut self, target: &[u8]) -> Result<()> {
		let current_idx = self.winner;
		self.direction = MergeDirection::Forward;
		self.active_count = 0;

		for (idx, iter) in self.iterators.iter_mut().enumerate() {
			if Some(idx) == current_idx {
				// Current iterator: just call next() once
				if iter.next()? {
					self.active_count += 1;
				}
			} else {
				// Non-current: seek to target, then advance past it
				if iter.seek(target)? {
					// Advance while key <= target (need to be strictly greater)
					while iter.valid()
						&& self.cmp.compare(iter.key().encoded(), target) != Ordering::Greater
					{
						if !iter.next()? {
							break;
						}
					}
					if iter.valid() {
						self.active_count += 1;
					}
				}
			}
		}

		self.find_winner();
		Ok(())
	}

	/// Switch from forward to backward, positioning just before `target`.
	///
	/// When switching directions, the current iterator just moves backward once.
	/// Non-current iterators need to be positioned at a key strictly less than
	/// `target` to ensure correct ordering.
	fn switch_to_backward(&mut self, target: &[u8]) -> Result<()> {
		let current_idx = self.winner;
		self.direction = MergeDirection::Backward;
		self.active_count = 0;

		for (idx, iter) in self.iterators.iter_mut().enumerate() {
			if Some(idx) == current_idx {
				// Current iterator: just call prev() once
				if iter.prev()? {
					self.active_count += 1;
				}
			} else {
				// Non-current: seek to target, then move before it
				if iter.seek(target)? {
					// Move backward while key >= target (need to be strictly less)
					while iter.valid()
						&& self.cmp.compare(iter.key().encoded(), target) != Ordering::Less
					{
						if !iter.prev()? {
							break;
						}
					}
					if iter.valid() {
						self.active_count += 1;
					}
				} else {
					// Iterator positioned past all keys, go to last
					if iter.seek_last()? {
						self.active_count += 1;
					}
				}
			}
		}

		self.find_winner();
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

impl LSMIterator for KMergeIterator<'_> {
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
		// If we were going backward, switch to forward
		if self.direction != MergeDirection::Forward {
			let target = self.key().encoded().to_vec();
			self.switch_to_forward(&target)?;
			return Ok(self.is_valid());
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
			let target = self.key().encoded().to_vec();
			self.switch_to_backward(&target)?;
			return Ok(self.is_valid());
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

	fn value_encoded(&self) -> Result<&[u8]> {
		debug_assert!(self.is_valid());
		self.iterators[self.winner.unwrap()].value_encoded()
	}
}

pub(crate) struct SnapshotIterator<'a> {
	/// The merge iterator
	merge_iter: KMergeIterator<'a>,

	/// Sequence number for visibility
	snapshot_seq_num: u64,

	/// Core handle retained for lifetime of in-flight iteration
	_core: Arc<Core>,

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

	/// Active range tombstones: (start_key, end_key, seq_num)
	range_deletions: Vec<(Key, Key, u64)>,

	/// Direction of iteration
	direction: MergeDirection,

	/// Whether the iterator has been initialized
	initialized: bool,
}

impl SnapshotIterator<'_> {
	/// Creates a new iterator over a specific key range
	fn new_from(core: Arc<Core>, seq_num: u64, range: InternalKeyRange) -> Result<Self> {
		// Create a temporary snapshot to use the helper method
		let snapshot = Snapshot::new(Arc::clone(&core), seq_num);
		let iter_state = snapshot.collect_iter_state()?;

		// From the state the iterator reads, not from the tree again: a restore in between would
		// give it the range deletions of the state that replaced it.
		let mut range_deletions = Vec::new();
		range_deletions.extend(iter_state.active.range_deletions.read().clone());
		for memtable in &iter_state.immutable {
			range_deletions.extend(memtable.range_deletions.read().clone());
		}
		for level in &iter_state.levels {
			for table in &level.tables {
				range_deletions.extend(table.range_deletions.read().clone());
			}
		}

		let merge_iter = KMergeIterator::new_from(iter_state, range);

		Ok(Self {
			merge_iter,
			snapshot_seq_num: seq_num,
			_core: core,
			range_deletions,
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

	#[inline]
	fn is_deleted_by_range(&self, user_key: &[u8], seq_num: u64) -> bool {
		for (start, end, rseq) in &self.range_deletions {
			if seq_num <= *rseq
				&& *rseq <= self.snapshot_seq_num
				&& user_key >= start.as_slice()
				&& user_key < end.as_slice()
			{
				return true;
			}
		}
		false
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

			// Skip tombstones and range-deleted keys (but remember we saw this key)
			if key_ref.is_tombstone() || self.is_deleted_by_range(user_key, key_ref.seq_num()) {
				self.merge_iter.next()?;
				continue;
			}

			// Found valid entry
			return Ok(true);
		}
		Ok(false)
	}

	/// Skip to the next valid entry in backward direction.
	/// Thin wrapper kept for symmetry with `skip_to_valid_forward`; all real
	/// work happens in `find_latest_visible_backward`, which loops internally.
	fn skip_to_valid_backward(&mut self) -> Result<bool> {
		self.find_latest_visible_backward()
	}

	/// Find the latest visible version of the next user key going backward.
	///
	/// Finds the latest visible entry for the current user key during backward iteration.
	///
	/// Iterates iteratively in O(1) stack space, skipping keys covered by tombstones
	/// or invisible to this snapshot until a valid live entry is found or the iterator exhausts.
	fn find_latest_visible_backward(&mut self) -> Result<bool> {
		loop {
			// Position merge_iter at the start of the next user key to examine.
			//
			// `has_buffered_back` is set when a prior iteration crossed into a
			// new user key while walking versions of the previous one; the
			// boundary key is already loaded into merge_iter, so we just clear
			// the flag and fall through.
			if self.has_buffered_back {
				self.has_buffered_back = false;
			} else if !self.merge_iter.valid() {
				self.has_current_back = false;
				return Ok(false);
			}

			let first_key_ref = self.merge_iter.key();
			let current_user_key: Vec<u8> = first_key_ref.user_key().to_vec();

			let mut latest_key: Option<Vec<u8>> = None;
			let mut latest_value: Option<Vec<u8>> = None;

			if self.is_visible_ref(&first_key_ref) {
				latest_key = Some(first_key_ref.encoded().to_vec());
				latest_value = Some(self.merge_iter.value_encoded()?.to_vec());
			}

			// Walk older versions of this same user key, picking up the latest
			// visible one. Stops when merge_iter goes invalid or crosses into
			// a different user key (which becomes the buffered start for the
			// next outer iteration).
			loop {
				self.merge_iter.prev()?;

				if !self.merge_iter.valid() {
					break;
				}

				let key_ref = self.merge_iter.key();
				let user_key = key_ref.user_key();

				if user_key != current_user_key.as_slice() {
					self.buffered_back_key.clear();
					self.buffered_back_key.extend_from_slice(key_ref.encoded());
					self.buffered_back_value.clear();
					self.buffered_back_value.extend_from_slice(self.merge_iter.value_encoded()?);
					self.has_buffered_back = true;
					break;
				}

				if self.is_visible_ref(&key_ref) {
					latest_key = Some(key_ref.encoded().to_vec());
					latest_value = Some(self.merge_iter.value_encoded()?.to_vec());
				}
			}

			// Decide the outcome for this user key.
			if let (Some(key_bytes), Some(value_bytes)) = (latest_key, latest_value) {
				let key_ref = InternalKeyRef::from_encoded(&key_bytes);
				if key_ref.is_tombstone()
					|| self.is_deleted_by_range(key_ref.user_key(), key_ref.seq_num())
				{
					// Latest visible is a tombstone -- skip this user key and
					// examine the next one.
					self.has_current_back = false;
					continue;
				}
				self.current_back_key.clear();
				self.current_back_key.extend_from_slice(&key_bytes);
				self.current_back_value.clear();
				self.current_back_value.extend_from_slice(&value_bytes);
				self.has_current_back = true;
				return Ok(true);
			}

			// No visible version found for this user key -- skip it. (Was:
			// recursive call.)
			self.has_current_back = false;
		}
	}

	/// Switch from backward to forward direction.
	fn reverse_to_forward(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Forward;

		// Get current user key from backward state (equivalent to saved_key_)
		let current_user_key = if self.has_current_back {
			InternalKeyRef::from_encoded(&self.current_back_key).user_key().to_vec()
		} else {
			// No current position in backward mode
			self.has_buffered_back = false;
			return Ok(false);
		};

		// Clear backward state
		self.has_current_back = false;
		self.has_buffered_back = false;

		// Set last_key_fwd so skip_to_valid_forward() will skip this user key
		// (equivalent to FindNextUserEntry(skipping_saved_key=true))
		self.last_key_fwd.clear();
		self.last_key_fwd.extend_from_slice(&current_user_key);

		// Seek to first entry >= current user key
		// Using (user_key, MAX_SEQ) positions at the start of this user key's entries
		let seek_key = InternalKey::new(current_user_key, u64::MAX, InternalKeyKind::Set);
		self.merge_iter.seek(&seek_key.encode())?;

		// skip_to_valid_forward() will skip entries with user_key == last_key_fwd
		// and return the first visible entry with a DIFFERENT user key
		self.skip_to_valid_forward()
	}

	/// Switch from forward to backward direction.
	fn forward_to_backward(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Backward;

		// Clear backward buffers
		self.has_buffered_back = false;
		self.has_current_back = false;

		// In forward mode, merge_iter is positioned at the current entry
		// We need to move to the previous user key
		if self.merge_iter.valid() {
			// Move backward from current position
			self.merge_iter.prev()?;
		}

		// find_latest_visible_backward will scan this user key's entries
		// to find the latest visible version, then buffer the next user key
		self.skip_to_valid_backward()
	}
}

impl LSMIterator for SnapshotIterator<'_> {
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

		// Direction change: backward → forward (ReverseToForward)
		if self.direction == MergeDirection::Backward {
			return self.reverse_to_forward();
		}

		// Normal forward iteration
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

		// Direction change: forward → backward (ReverseToBackward)
		if self.direction != MergeDirection::Backward {
			return self.forward_to_backward();
		}

		// Normal backward iteration
		if !self.merge_iter.valid() && !self.has_buffered_back {
			self.has_current_back = false;
			return Ok(false);
		}
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

	fn value_encoded(&self) -> Result<&[u8]> {
		debug_assert!(self.valid());
		if self.direction == MergeDirection::Backward {
			Ok(&self.current_back_value)
		} else {
			self.merge_iter.value_encoded()
		}
	}
}

// ===== History Iterator =====

/// A version in the history of a key, as the iterator holds it.
#[derive(Clone)]
struct BufferedEntry {
	/// The encoded internal key: the user key and the trailer.
	key: Vec<u8>,
	value: Vec<u8>,
	/// The time the version was written at, see `InternalKeyRef::timestamp`.
	timestamp: u64,
	/// The sequence number of the write.
	seq: u64,
}

/// A visible version of a key, as read from the tables and the memtables.
struct Version {
	is_hard_delete: bool,
	is_replace: bool,
	is_tombstone: bool,
	/// The time the version was stamped with, if it was.
	stamp: Option<u64>,
	/// Whether the version is to be returned, if no barrier hides it: it is in the timestamp
	/// range, and is not a tombstone that is not asked for. The value of one that is not is not
	/// copied. For a version that has no stamp, whose time is only known with the others of its
	/// key, `make_group` decides this.
	wanted: bool,
	entry: BufferedEntry,
}

/// Iterates the versions of the keys of a range: keys in order, and for each key its versions
/// from the newest timestamp to the oldest, and the newest write first among versions of the same
/// timestamp. This is the order a versioned read needs: the first version at or before a time is
/// the version at that time. It is not the order the versions are stored in, which is the order
/// of their sequence numbers; the timestamp of a write can be older than an earlier write's.
///
/// The versions of one key are read, ordered and held together, and the iterator moves through
/// them, forwards or backwards, and on to the neighbouring key.
///
/// A hard delete hides every version below it, whatever its time, and a replace hides every
/// version below it but is itself a version. Below means written before, by sequence number: the
/// barriers are found in storage order, before the timestamp range and the tombstones are taken
/// out of what is returned.
pub struct HistoryIterator<'a> {
	inner: KMergeIterator<'a>,
	snapshot_seq_num: u64,
	include_tombstones: bool,
	direction: MergeDirection,
	initialized: bool,
	lower_bound: Option<Vec<u8>>,
	upper_bound: Option<Vec<u8>>,

	/// The versions of the current key that are returned, in the order they are returned in
	/// going forwards.
	group: Vec<BufferedEntry>,
	/// The user key of `group`.
	group_key: Vec<u8>,
	/// The entry of `group` the iterator is on, if it is on one.
	pos: Option<usize>,

	/// The range deletions of the state the iterator reads: `(start, end, seq)`.
	range_deletions: Vec<(Key, Key, u64)>,

	// === Filtering options ===
	ts_range: Option<(u64, u64)>, // (start_ts, end_ts) inclusive
	limit: Option<usize>,
	entries_returned: usize,
	limit_reached: bool,
}

impl<'a> HistoryIterator<'a> {
	/// Creates a HistoryIterator from a pre-built KMergeIterator.
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		merge_iter: KMergeIterator<'a>,
		range_deletions: Vec<(Key, Key, u64)>,
		seq_num: u64,
		include_tombstones: bool,
		lower: Option<&[u8]>,
		upper: Option<&[u8]>,
		ts_range: Option<(u64, u64)>,
		limit: Option<usize>,
	) -> Self {
		Self {
			range_deletions,
			inner: merge_iter,
			snapshot_seq_num: seq_num,
			include_tombstones,
			direction: MergeDirection::Forward,
			initialized: false,
			lower_bound: lower.map(|b| b.to_vec()),
			upper_bound: upper.map(|b| b.to_vec()),
			group: Vec::new(),
			group_key: Vec::new(),
			pos: None,
			ts_range,
			limit,
			entries_returned: 0,
			limit_reached: false,
		}
	}

	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new_lsm(
		seq_num: u64,
		iter_state: IterState,
		range_deletions: Vec<(Key, Key, u64)>,
		range: InternalKeyRange,
		include_tombstones: bool,
		ts_range: Option<(u64, u64)>,
		limit: Option<usize>,
		lower: Option<&[u8]>,
		upper: Option<&[u8]>,
	) -> Self {
		// A timestamp range lets whole tables be skipped, see `KMergeIterator::new_for_history`
		let inner = if ts_range.is_some() {
			KMergeIterator::new_for_history(iter_state, range, ts_range)
		} else {
			KMergeIterator::new_from(iter_state, range)
		};

		Self::new(
			inner,
			range_deletions,
			seq_num,
			include_tombstones,
			lower,
			upper,
			ts_range,
			limit,
		)
	}

	/// Everything `reset_all_state` clears EXCEPT the `limit` budget counter.
	///
	/// `limit_reached` is cleared here rather than preserved because it is a
	/// derived flag: it is raised again from `entries_returned` against `limit` as soon as the
	/// iterator is asked to move on.
	fn reset_position_state(&mut self) {
		self.group.clear();
		self.group_key.clear();
		self.pos = None;
		self.limit_reached = false;
	}

	fn reset_all_state(&mut self) {
		self.reset_position_state();
		self.entries_returned = 0;
	}

	/// Shared body of `seek_first`/`seek_last` and of the `reanchor_*` methods a
	/// merge layer uses to reposition this iterator mid-traversal.
	///
	/// `reset_budget` is the sole difference between the two: a seek the user
	/// asked for restarts the `limit` budget, an internal re-anchor continues on
	/// whatever is left of it.
	fn anchor(&mut self, direction: MergeDirection, reset_budget: bool) -> Result<bool> {
		self.direction = direction;
		self.reset_position_state();
		if reset_budget {
			self.entries_returned = 0;
		}

		// NOTE: `initialized` is set AFTER the inner seek, so that a seek error leaves the
		// iterator uninitialized, and a later `next()` retries via `seek_first()` instead of
		// reporting `Ok(false)`.
		match direction {
			MergeDirection::Forward => {
				if let Some(ref lower) = self.lower_bound {
					let seek_key = InternalKey::new(lower.clone(), u64::MAX, InternalKeyKind::Set);
					self.inner.seek(&seek_key.encode())?;
				} else {
					self.inner.seek_first()?;
				}
				self.initialized = true;
				self.settle_forward()
			}
			MergeDirection::Backward => {
				self.inner.seek_last()?;
				self.initialized = true;
				self.settle_backward()
			}
		}
	}

	/// Reposition to the extreme entry in `direction` without refunding the
	/// `limit` budget already spent. See `LSMIterator::reanchor_last`.
	fn reanchor(&mut self, direction: MergeDirection) -> Result<bool> {
		self.anchor(direction, false)
	}

	// --- Inner iterator helpers ---

	fn inner_valid(&self) -> bool {
		self.inner.valid()
	}

	fn inner_key(&self) -> InternalKeyRef<'_> {
		self.inner.key()
	}

	fn inner_value(&self) -> Result<&[u8]> {
		self.inner.value_encoded()
	}

	fn inner_next(&mut self) -> Result<bool> {
		self.inner.next()
	}

	fn inner_prev(&mut self) -> Result<bool> {
		self.inner.prev()
	}

	fn in_ts_range(&self, timestamp: u64) -> bool {
		match self.ts_range {
			Some((ts_start, ts_end)) => timestamp >= ts_start && timestamp <= ts_end,
			None => true,
		}
	}

	// --- Bounds checking ---

	fn user_key_within_lower_bound(&self, user_key: &[u8]) -> bool {
		match &self.lower_bound {
			Some(lower) => user_key >= lower.as_slice(),
			None => true,
		}
	}

	fn user_key_within_upper_bound(&self, user_key: &[u8]) -> bool {
		match &self.upper_bound {
			Some(upper) => user_key < upper.as_slice(),
			None => true,
		}
	}

	// --- The versions of a key ---

	/// The version the inner iterator is at, if the snapshot can see it: a range deletion that
	/// the snapshot reads hides what was written before it, as it does for a read.
	fn read_version(&self) -> Result<Option<Version>> {
		let key_ref = self.inner_key();
		let seq = key_ref.seq_num();
		if seq > self.snapshot_seq_num
			|| covered_by_range(
				&self.range_deletions,
				self.snapshot_seq_num,
				key_ref.user_key(),
				seq,
			) {
			return Ok(None);
		}
		let value = self.inner_value()?;
		let stamp = ValueLocation::entry_time(key_ref.kind(), value);
		let is_tombstone = key_ref.is_tombstone();
		let wanted = match stamp {
			Some(timestamp) => {
				self.in_ts_range(timestamp) && (self.include_tombstones || !is_tombstone)
			}
			None => true,
		};
		Ok(Some(Version {
			is_hard_delete: key_ref.is_hard_delete_marker(),
			is_replace: key_ref.is_replace(),
			is_tombstone,
			stamp,
			wanted,
			entry: BufferedEntry {
				key: key_ref.encoded().to_vec(),
				value: if wanted {
					value.to_vec()
				} else {
					Vec::new()
				},
				timestamp: stamp.unwrap_or(0),
				seq,
			},
		}))
	}

	/// Makes the versions of `user_key`, which are given newest write first, the current group.
	///
	/// - A hard delete as the newest version erases the key: it has no history.
	/// - A hard delete below that hides every version under it and is not returned itself.
	/// - A replace is returned, and hides every version under it.
	///
	/// This holds whatever the timestamp range is, which only decides what is returned, and
	/// whether tombstones are asked for.
	fn make_group(&mut self, user_key: Vec<u8>, mut versions: Vec<Version>) {
		self.group.clear();
		self.group_key = user_key;

		// The same write can be in a table and in a memtable that recovery rebuilt from the log
		// of that table: it is one version.
		versions.dedup_by_key(|version| version.entry.seq);

		if versions.first().is_none_or(|latest| latest.is_hard_delete) {
			return;
		}
		// The time of a version that has no stamp is that of the versions written before it.
		let stamps: Vec<(Option<u64>, bool)> = versions
			.iter()
			.map(|version| (version.stamp, version.is_hard_delete || version.is_replace))
			.collect();
		let times = ValueLocation::infer_times(&stamps);
		for (version, time) in versions.into_iter().zip(times) {
			if version.is_hard_delete {
				break;
			}
			let replace = version.is_replace;
			let wanted = if version.stamp.is_some() {
				version.wanted
			} else {
				self.in_ts_range(time) && (self.include_tombstones || !version.is_tombstone)
			};
			if wanted {
				let mut entry = version.entry;
				entry.timestamp = time;
				self.group.push(entry);
			}
			if replace {
				break;
			}
		}

		// Newest timestamp first, and the newest write first among versions of one timestamp.
		// For versions written with increasing timestamps this is the order they were written
		// in, reversed, and the sort finds them in order.
		self.group.sort_by(|a, b| b.timestamp.cmp(&a.timestamp).then(b.seq.cmp(&a.seq)));
	}

	/// Reads the versions of the next key that has any to return, going forwards. The inner
	/// iterator is at the first entry of a key, or past the end; it is left at the first entry of
	/// the key after the one read. Returns whether there is such a key, and leaves `pos` unset.
	fn load_group_forward(&mut self) -> Result<bool> {
		self.group.clear();
		self.pos = None;
		while self.inner_valid() {
			let user_key = self.inner_key().user_key().to_vec();
			if !self.user_key_within_upper_bound(&user_key) {
				return Ok(false);
			}
			let wanted = self.user_key_within_lower_bound(&user_key);
			let mut versions = Vec::new();
			while self.inner_valid() && self.inner_key().user_key() == user_key.as_slice() {
				if wanted {
					if let Some(version) = self.read_version()? {
						versions.push(version);
					}
				}
				self.inner_next()?;
			}
			if wanted {
				self.make_group(user_key, versions);
				if !self.group.is_empty() {
					return Ok(true);
				}
			}
		}
		Ok(false)
	}

	/// Reads the versions of the previous key that has any to return, going backwards. The inner
	/// iterator is at the last entry of a key, or before the start; it is left at the last entry
	/// of the key before the one read.
	fn load_group_backward(&mut self) -> Result<bool> {
		self.group.clear();
		self.pos = None;
		while self.inner_valid() {
			let user_key = self.inner_key().user_key().to_vec();
			if !self.user_key_within_lower_bound(&user_key) {
				return Ok(false);
			}
			let wanted = self.user_key_within_upper_bound(&user_key);
			// Going backwards the versions of a key come oldest write first.
			let mut versions = Vec::new();
			while self.inner_valid() && self.inner_key().user_key() == user_key.as_slice() {
				if wanted {
					if let Some(version) = self.read_version()? {
						versions.push(version);
					}
				}
				self.inner_prev()?;
			}
			if wanted {
				versions.reverse();
				self.make_group(user_key, versions);
				if !self.group.is_empty() {
					return Ok(true);
				}
			}
		}
		Ok(false)
	}

	/// Moves onto `pos` of the group, if the `limit` budget has any left.
	fn step_to(&mut self, pos: usize) -> bool {
		if let Some(limit) = self.limit {
			if self.entries_returned >= limit {
				self.limit_reached = true;
				return false;
			}
		}
		self.entries_returned += 1;
		self.pos = Some(pos);
		true
	}

	fn settle_forward(&mut self) -> Result<bool> {
		if self.load_group_forward()? {
			Ok(self.step_to(0))
		} else {
			Ok(false)
		}
	}

	fn settle_backward(&mut self) -> Result<bool> {
		if self.load_group_backward()? {
			Ok(self.step_to(self.group.len() - 1))
		} else {
			Ok(false)
		}
	}

	/// Puts the inner iterator at the first entry of the key after the group's, which is where
	/// going forwards loads the next group from: every version of the key sorts before the
	/// internal key of sequence number 0.
	fn position_inner_after_group(&mut self) -> Result<()> {
		let target = InternalKey::new(self.group_key.clone(), 0, InternalKeyKind::Set);
		self.inner.seek(&target.encode())?;
		Ok(())
	}

	/// Puts the inner iterator at the last entry of the key before the group's, which is where
	/// going backwards loads the next group from.
	fn position_inner_before_group(&mut self) -> Result<()> {
		let target = InternalKey::new(self.group_key.clone(), u64::MAX, InternalKeyKind::Set);
		self.inner.seek(&target.encode())?;
		if self.inner_valid() {
			self.inner_prev()?;
		} else {
			self.inner.seek_last()?;
		}
		Ok(())
	}

	fn step_forward(&mut self) -> Result<bool> {
		let Some(pos) = self.pos else {
			return Ok(false);
		};
		if self.direction == MergeDirection::Backward {
			// The inner iterator is before the group: put it after.
			self.direction = MergeDirection::Forward;
			self.position_inner_after_group()?;
		}
		if pos + 1 < self.group.len() {
			return Ok(self.step_to(pos + 1));
		}
		// Past the last version of the key: on to the next key. Not on an entry meanwhile, so
		// that a spent budget leaves the iterator where it was.
		if let Some(limit) = self.limit {
			if self.entries_returned >= limit {
				self.limit_reached = true;
				return Ok(false);
			}
		}
		self.settle_forward()
	}

	fn step_backward(&mut self) -> Result<bool> {
		let Some(pos) = self.pos else {
			return Ok(false);
		};
		if self.direction != MergeDirection::Backward {
			// The inner iterator is after the group: put it before.
			self.direction = MergeDirection::Backward;
			self.position_inner_before_group()?;
		}
		if pos > 0 {
			return Ok(self.step_to(pos - 1));
		}
		if let Some(limit) = self.limit {
			if self.entries_returned >= limit {
				self.limit_reached = true;
				return Ok(false);
			}
		}
		self.settle_backward()
	}
}

impl LSMIterator for HistoryIterator<'_> {
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.reset_all_state();

		self.inner.seek(target)?;
		self.initialized = true;
		self.settle_forward()
	}

	fn seek_first(&mut self) -> Result<bool> {
		self.anchor(MergeDirection::Forward, true)
	}

	fn seek_last(&mut self) -> Result<bool> {
		self.anchor(MergeDirection::Backward, true)
	}

	fn next(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_first();
		}
		self.step_forward()
	}

	fn prev(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_last();
		}
		self.step_backward()
	}

	fn valid(&self) -> bool {
		!self.limit_reached && self.pos.is_some()
	}

	/// Overridden because `seek_first`/`seek_last` here begin with
	/// `reset_all_state()`, which zeroes `entries_returned` — refunding the whole
	/// `limit` budget. That is right for a seek the *user* asked for and wrong for
	/// an internal re-anchor, which continues a traversal that has already spent
	/// part of the budget.
	fn reanchor_first(&mut self) -> Result<bool> {
		self.reanchor(MergeDirection::Forward)
	}

	fn reanchor_last(&mut self) -> Result<bool> {
		self.reanchor(MergeDirection::Backward)
	}

	fn key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.valid());
		let entry = &self.group[self.pos.expect("a valid iterator is on an entry")];
		InternalKeyRef::from_encoded(&entry.key).with_timestamp(entry.timestamp)
	}

	fn value_encoded(&self) -> Result<&[u8]> {
		debug_assert!(self.valid());
		Ok(&self.group[self.pos.expect("a valid iterator is on an entry")].value)
	}
}

#[cfg(test)]
mod tests {
	use super::SnapshotTracker;

	#[test]
	fn test_snapshot_tracker_ordering() {
		let tracker = SnapshotTracker::new();

		// Insert snapshots in non-sorted order
		tracker.register(100);
		tracker.register(50);
		tracker.register(200);
		tracker.register(75);
		tracker.register(150);

		// Verify get_all_snapshots returns sorted order
		let snapshots = tracker.get_all_snapshots();
		assert_eq!(snapshots, vec![50, 75, 100, 150, 200]);

		// Unregister some and verify order is maintained
		tracker.unregister(100, None);
		tracker.unregister(50, None);

		let snapshots = tracker.get_all_snapshots();
		assert_eq!(snapshots, vec![75, 150, 200]);

		// Add more and verify
		tracker.register(25);
		tracker.register(300);

		let snapshots = tracker.get_all_snapshots();
		assert_eq!(snapshots, vec![25, 75, 150, 200, 300]);
	}

	#[test]
	fn test_snapshot_tracker_empty() {
		let tracker = SnapshotTracker::new();
		assert!(tracker.get_all_snapshots().is_empty());
	}

	#[test]
	fn test_snapshot_tracker_clone_shares_state() {
		let tracker1 = SnapshotTracker::new();
		tracker1.register(100);

		let tracker2 = tracker1.clone();
		tracker2.register(50);

		// Both should see the same snapshots
		assert_eq!(tracker1.get_all_snapshots(), vec![50, 100]);
		assert_eq!(tracker2.get_all_snapshots(), vec![50, 100]);
	}
}
