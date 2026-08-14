use std::cmp::Ordering;
use std::ops::Bound;
use std::sync::Arc;

use crossbeam_skiplist::SkipSet;

use crate::batch::BatchOwner;
use crate::branch_runtime::BranchRuntime;
use crate::error::{Error, Result};
use crate::iter::BoxedLSMIterator;
use crate::levels::{LevelManifest, Levels};
use crate::lsm::Core;
use crate::memtable::MemTable;
use crate::{
	BytewiseComparator,
	Comparator,
	InternalKey,
	InternalKeyComparator,
	InternalKeyKind,
	InternalKeyRange,
	InternalKeyRef,
	LSMIterator,
	TimestampComparator,
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
/// Entries are `(seq, unique_id)`, exactly like `ActiveTxnTracker`: the id
/// differentiates concurrent snapshots that share a sequence number (any two
/// read transactions started with no commit in between do), so dropping one
/// cannot strip the other's compaction protection. Registration returns an
/// RAII guard, making an unregister without a matching register
/// unrepresentable.
pub(crate) struct SnapshotTracker {
	snapshots: Arc<SkipSet<(u64, u64)>>,
	next_id: Arc<std::sync::atomic::AtomicU64>,
}

impl Clone for SnapshotTracker {
	fn clone(&self) -> Self {
		Self {
			snapshots: Arc::clone(&self.snapshots),
			next_id: Arc::clone(&self.next_id),
		}
	}
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
	/// Creates a new empty snapshot tracker.
	pub(crate) fn new() -> Self {
		Self {
			snapshots: Arc::new(SkipSet::new()),
			next_id: Arc::new(std::sync::atomic::AtomicU64::new(0)),
		}
	}

	/// Registers a new snapshot with the given sequence number.
	///
	/// The returned guard keeps compaction preserving versions visible to
	/// this snapshot; dropping the guard releases exactly this registration.
	pub(crate) fn register(&self, seq_num: u64) -> SnapshotGuard {
		let entry = (seq_num, self.next_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
		self.snapshots.insert(entry);
		SnapshotGuard {
			snapshots: Arc::clone(&self.snapshots),
			entry,
		}
	}

	/// Returns all active snapshots as a sorted, deduplicated vector.
	///
	/// This is the primary method used by compaction. The returned vector
	/// is sorted in ascending order.
	pub(crate) fn get_all_snapshots(&self) -> Vec<u64> {
		let mut seqs: Vec<u64> = self.snapshots.iter().map(|entry| entry.value().0).collect();
		seqs.dedup();
		seqs
	}

	/// Returns the smallest active snapshot seq, if any. O(log N) via
	/// `SkipSet::front`. Used by the commit oracle to compute its GC
	/// watermark on every commit.
	pub(crate) fn first(&self) -> Option<u64> {
		self.snapshots.front().map(|e| e.value().0)
	}
}

/// RAII registration handle: one guard = one tracked snapshot entry.
pub(crate) struct SnapshotGuard {
	snapshots: Arc<SkipSet<(u64, u64)>>,
	entry: (u64, u64),
}

impl Drop for SnapshotGuard {
	fn drop(&mut self) {
		self.snapshots.remove(&self.entry);
	}
}

// ===== Iterator State =====
/// Holds references to all LSM tree components needed for iteration.
/// One layer of a snapshot's read stack: a branch's own components with the
/// visibility cap that applies to them from the reader's position in the
/// fork chain.
pub(crate) struct IterLayer {
	/// The active memtable receiving current writes. `None` when the layer's
	/// owner has no runtime (idle branch) — never synthesized, because an
	/// empty stand-in memtable would allocate a full arena per read.
	pub active: Option<Arc<MemTable>>,
	/// Immutable memtables waiting to be flushed
	pub immutable: Vec<Arc<MemTable>>,
	/// The layer owner's own levels
	pub levels: Levels,
	/// Visibility cap: `min(snapshot seq, every fork anchor on the path to
	/// this ancestor)`. Rows above it are unreadable through this layer.
	pub cap: u64,
}

/// The full capture: the reader's own layer first, then ancestors
/// nearest-first (logical inherited views resolved through the catalog).
pub(crate) struct IterState {
	pub layers: Vec<IterLayer>,
}

impl IterState {
	/// Single-layer state (no inheritance) — the default-branch shape and
	/// the test-fixture constructor.
	#[cfg_attr(not(test), allow(dead_code))]
	pub(crate) fn single(
		active: Option<Arc<MemTable>>,
		immutable: Vec<Arc<MemTable>>,
		levels: Levels,
		cap: u64,
	) -> Self {
		Self {
			layers: vec![IterLayer {
				active,
				immutable,
				levels,
				cap,
			}],
		}
	}
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

	/// The read stack: the owner's layer first, then catalog-resolved
	/// ancestors nearest-first, each with its visibility cap. A layer's
	/// `runtime` is `None` when that owner has no runtime (idle branch) —
	/// which is a complete view, not a race: `seq_num` comes from the
	/// drained visible sequence and apply creates a runtime before
	/// publication advances. Reads never create runtimes.
	layers: Vec<SnapshotLayer>,

	/// Tracker registration; released on drop.
	_tracker_guard: SnapshotGuard,
}

/// A point-get hit inside one layer: a live value or a tombstone (which
/// must HIDE farther layers, not fall through to them).
enum LayerHit {
	Value(Value, u64),
	/// A delete is a version: its sequence is what tells a merge whether the
	/// branch moved, so it is carried rather than collapsed away.
	Tombstone {
		seq: u64,
		kind: InternalKeyKind,
	},
}

impl LayerHit {
	fn from_item(item: (InternalKey, Value)) -> Self {
		if item.0.is_tombstone() {
			Self::Tombstone {
				seq: item.0.seq_num(),
				kind: item.0.kind(),
			}
		} else {
			let seq = item.0.seq_num();
			Self::Value(item.1, seq)
		}
	}

	fn from_owned(item: (InternalKey, Value)) -> Self {
		Self::from_item(item)
	}
}

/// What a snapshot can say about a key without reading its value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct VersionMeta {
	pub(crate) seq: u64,
	pub(crate) kind: InternalKeyKind,
}

pub(crate) struct SnapshotLayer {
	owner: BatchOwner,
	runtime: Option<Arc<BranchRuntime>>,
	/// `min(snapshot seq, fork anchors on the path)`.
	cap: u64,
}

impl Snapshot {
	/// Creates a new snapshot of one physical owner's read stack: its own
	/// components plus catalog-resolved ancestor layers, nearest-first, each
	/// capped at `min(seq_num, fork anchors on the path)` (FK3 logical
	/// views).
	pub(crate) fn new_owned(core: Arc<Core>, seq_num: u64, owner: BatchOwner) -> Result<Self> {
		// Register this snapshot's sequence number so compaction knows
		// to preserve versions visible to this snapshot
		let tracker_guard = core.snapshot_tracker.register(seq_num);

		let mut layers = vec![SnapshotLayer {
			owner,
			runtime: core.inner.runtimes.get(owner),
			cap: seq_num,
		}];
		let chain = core.inner.branch_catalog.read()?.parent_chain(
			owner.branch,
			owner.generation,
			crate::branch::MAX_VIEW_DEPTH,
		)?;
		let mut cap = seq_num;
		for (branch, generation, fork_seq) in chain {
			cap = cap.min(fork_seq);
			let ancestor = BatchOwner {
				branch,
				generation,
			};
			layers.push(SnapshotLayer {
				owner: ancestor,
				runtime: core.inner.runtimes.get(ancestor),
				cap,
			});
		}
		Ok(Self {
			core,
			seq_num,
			layers,
			_tracker_guard: tracker_guard,
		})
	}

	/// A snapshot of only what `owner` OWNS: its own memtables and level set,
	/// with no ancestor layers at all.
	///
	/// Diff reads through this. Note that "owns" is not the same as "wrote":
	/// detach materializes inherited rows into the branch's own tables, so
	/// callers that mean "changed since the fork" must still filter by sequence.
	pub(crate) fn own_only(core: Arc<Core>, seq_num: u64, owner: BatchOwner) -> Result<Self> {
		let tracker_guard = core.snapshot_tracker.register(seq_num);
		let layers = vec![SnapshotLayer {
			owner,
			runtime: core.inner.runtimes.get(owner),
			cap: seq_num,
		}];
		Ok(Self {
			core,
			seq_num,
			layers,
			_tracker_guard: tracker_guard,
		})
	}

	/// A snapshot of only what `owner` INHERITS: the same ancestor layers with
	/// the same cumulative caps, minus the owner's own components.
	///
	/// Detach uses this to copy the inherited view into the branch's own tables.
	/// Including the owner's own layer would duplicate its rows at their
	/// original sequences — survivable, since compaction dedups equal sequences,
	/// but wasteful and it would blur what a materialized table contains.
	///
	/// Returns `None` when the owner inherits nothing, which is the honest
	/// answer for a root branch and the cheap exit for an already-detached one.
	pub(crate) fn inherited_only(
		core: Arc<Core>,
		seq_num: u64,
		owner: BatchOwner,
	) -> Result<Option<Self>> {
		let tracker_guard = core.snapshot_tracker.register(seq_num);
		let chain = core.inner.branch_catalog.read()?.parent_chain(
			owner.branch,
			owner.generation,
			crate::branch::MAX_VIEW_DEPTH,
		)?;
		if chain.is_empty() {
			return Ok(None);
		}
		let mut layers = Vec::with_capacity(chain.len());
		let mut cap = seq_num;
		for (branch, generation, fork_seq) in chain {
			cap = cap.min(fork_seq);
			let ancestor = BatchOwner {
				branch,
				generation,
			};
			layers.push(SnapshotLayer {
				owner: ancestor,
				runtime: core.inner.runtimes.get(ancestor),
				cap,
			});
		}
		Ok(Some(Self {
			core,
			seq_num,
			layers,
			_tracker_guard: tracker_guard,
		}))
	}

	/// Collects the iterator state from the owner's LSM components
	/// This is a helper method used by both iterators and optimized operations
	/// like count
	pub(crate) fn collect_iter_state(&self) -> Result<IterState> {
		let manifest =
			guardian::ArcRwLockReadGuardian::take(Arc::clone(&self.core.level_manifest))?;

		let mut layers = Vec::with_capacity(self.layers.len());
		for layer in &self.layers {
			let (active, immutable) = match &layer.runtime {
				Some(runtime) => {
					let active = guardian::ArcRwLockReadGuardian::take(Arc::clone(
						&runtime.active_memtable,
					))?;
					let immutable = guardian::ArcRwLockReadGuardian::take(Arc::clone(
						&runtime.immutable_memtables,
					))?;
					(
						Some(active.clone()),
						immutable.iter().map(|entry| Arc::clone(&entry.memtable)).collect(),
					)
				}
				None => (None, Vec::new()),
			};

			let levels = match manifest.levels_for(layer.owner) {
				Some(levels) => levels.clone(),
				// Manifest load fail-closes on a missing default set, so
				// this is unreachable except through corruption.
				None if layer.owner == BatchOwner::DEFAULT => {
					return Err(crate::error::Error::Corruption(format!(
						"snapshot owner {:?} has no level set",
						layer.owner
					)));
				}
				// The owner never flushed an SST: an empty durable view.
				None => Levels::new(self.core.opts.level_count as usize, 0),
			};

			layers.push(IterLayer {
				active,
				immutable,
				levels,
				cap: layer.cap,
			});
		}

		Ok(IterState {
			layers,
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
		// Walk the read stack nearest-first: the owner's layer, then
		// catalog-resolved ancestors, each capped at its fork-path cap. The
		// first visible version wins; a tombstone in a nearer layer hides
		// every farther layer (COW shadowing is structural under the global
		// clock: a child's sequences exceed every inherited visible one).
		let level_manifest = self.core.level_manifest.read()?;
		for layer in &self.layers {
			if let Some(found) = Self::get_in_layer(&level_manifest, layer, key)? {
				return Ok(match found {
					LayerHit::Tombstone {
						..
					} => None,
					LayerHit::Value(value, seq) => Some((value, seq)),
				});
			}
		}
		Ok(None)
	}

	/// The newest version of `key` visible to this snapshot, as metadata only —
	/// no value is loaded, and a delete is reported rather than hidden.
	///
	/// Merge planning uses this to ask "did the target move since the fork" for
	/// every key the source changed, which must stay a metadata question: the
	/// common answer is "no", and loading a value to discover that would make
	/// planning proportional to the target's data rather than its changes.
	pub(crate) fn get_latest_meta(&self, key: &[u8]) -> crate::Result<Option<VersionMeta>> {
		let level_manifest = self.core.level_manifest.read()?;
		for layer in &self.layers {
			if let Some(found) = Self::get_in_layer(&level_manifest, layer, key)? {
				return Ok(Some(match found {
					LayerHit::Tombstone {
						seq,
						kind,
					} => VersionMeta {
						seq,
						kind,
					},
					LayerHit::Value(_, seq) => VersionMeta {
						seq,
						kind: InternalKeyKind::Set,
					},
				}));
			}
		}
		Ok(None)
	}

	fn get_in_layer(
		level_manifest: &LevelManifest,
		layer: &SnapshotLayer,
		key: &[u8],
	) -> crate::Result<Option<LayerHit>> {
		// Memtable phases exist only when the layer's owner has a runtime;
		// an idle branch's view is durable tables at most.
		if let Some(runtime) = &layer.runtime {
			let memtable_lock = runtime.active_memtable.read()?;
			if let Some(item) = memtable_lock.get(key.as_ref(), Some(layer.cap)) {
				return Ok(Some(LayerHit::from_item(item)));
			}
			drop(memtable_lock);

			let memtable_lock = runtime.immutable_memtables.read()?;
			for entry in memtable_lock.iter().rev() {
				if let Some(item) = entry.memtable.get(key.as_ref(), Some(layer.cap)) {
					return Ok(Some(LayerHit::from_item(item)));
				}
			}
		}

		// The layer's own durable tables, capped at the layer cap.
		let Some(owner_levels) = level_manifest.levels_for(layer.owner) else {
			return Ok(None);
		};
		let ikey = InternalKey::new(key.to_vec(), layer.cap, InternalKeyKind::Set, 0);
		for (level_idx, level) in owner_levels.into_iter().enumerate() {
			if level_idx == 0 {
				for table in level.tables.iter() {
					if !table.is_key_in_key_range(&ikey) {
						continue;
					}
					if let Some(item) = table.get(&ikey)? {
						return Ok(Some(LayerHit::from_owned(item)));
					}
				}
			} else {
				let query_range =
					crate::user_range_to_internal_range(Bound::Included(key), Bound::Included(key));
				let start_idx = level.find_first_overlapping_table(&query_range);
				let end_idx = level.find_last_overlapping_table(&query_range);
				for table in &level.tables[start_idx..end_idx] {
					if let Some(item) = table.get(&ikey)? {
						return Ok(Some(LayerHit::from_owned(item)));
					}
				}
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
		SnapshotIterator::new_from(self, internal_range)
	}

	/// Creates a history iterator over memtables + SSTables via KMergeIterator.
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

		// Merge memtables + SSTables
		Ok(HistoryIterator::new_lsm(
			self.seq_num,
			iter_state,
			range,
			include_tombstones,
			ts_range,
			limit,
			lower,
			upper,
		))
	}

	/// Queries for a specific key at a specific timestamp.
	/// Only returns data visible to this snapshot (seq_num <= snapshot.seq_num).
	///
	/// Uses `history_iter()` over the LSM (memtables + SSTables).
	pub(crate) fn get_at(&self, key: &[u8], timestamp: u64) -> Result<Option<Value>> {
		let mut iter = self.history_iter(Some(key), None, true, None, None)?;
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
					best_value = Some(iter.value_encoded()?.to_vec());
				}
				best_timestamp = entry_ts;
			}

			iter.next()?;
		}

		Ok(best_value)
	}
}

/// Direction of iteration for KMergeIterator
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum MergeDirection {
	Forward,
	Backward,
}

/// Caps one layer's iterator at its fork-path visibility: entries with
/// `seq > cap` are skipped in whichever direction the wrapper is moving.
/// Internal order is key-ascending with seq DESCENDING inside a key, so a
/// forward skip walks from a key's newest (possibly above-cap) versions down
/// to its visible ones; backward is the mirror image.
struct SeqCappedIterator<'a> {
	inner: BoxedLSMIterator<'a>,
	cap: u64,
}

impl<'a> SeqCappedIterator<'a> {
	fn new(inner: BoxedLSMIterator<'a>, cap: u64) -> Self {
		Self {
			inner,
			cap,
		}
	}

	fn skip_forward(&mut self, mut valid: bool) -> Result<bool> {
		while valid && self.inner.key().seq_num() > self.cap {
			valid = self.inner.next()?;
		}
		Ok(valid)
	}

	fn skip_backward(&mut self, mut valid: bool) -> Result<bool> {
		while valid && self.inner.key().seq_num() > self.cap {
			valid = self.inner.prev()?;
		}
		Ok(valid)
	}
}

impl LSMIterator for SeqCappedIterator<'_> {
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		let valid = self.inner.seek(target)?;
		self.skip_forward(valid)
	}

	fn seek_first(&mut self) -> Result<bool> {
		let valid = self.inner.seek_first()?;
		self.skip_forward(valid)
	}

	fn seek_last(&mut self) -> Result<bool> {
		let valid = self.inner.seek_last()?;
		self.skip_backward(valid)
	}

	fn next(&mut self) -> Result<bool> {
		let valid = self.inner.next()?;
		self.skip_forward(valid)
	}

	fn prev(&mut self) -> Result<bool> {
		let valid = self.inner.prev()?;
		self.skip_backward(valid)
	}

	fn valid(&self) -> bool {
		self.inner.valid()
	}

	fn key(&self) -> InternalKeyRef<'_> {
		self.inner.key()
	}

	fn value_encoded(&self) -> Result<&[u8]> {
		self.inner.value_encoded()
	}
}

/// A merge iterator that sorts by key+seqno.
/// Uses index-based tracking for zero-allocation iteration.
pub(crate) struct KMergeIterator<'iter> {
	/// Array of iterators to merge over.
	///
	/// IMPORTANT: Due to self-referential structs, this must be defined before
	/// `iter_state` in order to ensure it is dropped before `iter_state`.
	iterators: Vec<BoxedLSMIterator<'iter>>,

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

	/// Creates a new KMergeIterator with TimestampComparator for history queries.
	/// This enables timestamp-based seek optimization when timestamps are monotonic with seq_nums.
	pub(crate) fn new_for_history(
		iter_state: IterState,
		internal_range: InternalKeyRange,
		ts_range: Option<(u64, u64)>,
	) -> Self {
		let cmp: Arc<dyn Comparator> =
			Arc::new(TimestampComparator::new(Arc::new(BytewiseComparator::default())));
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

		// Pre-allocate capacity for the iterators across every layer.
		let capacity: usize = boxed_state
			.layers
			.iter()
			.map(|layer| 1 + layer.immutable.len() + layer.levels.total_tables())
			.sum();
		let mut iterators: Vec<BoxedLSMIterator<'a>> = Vec::with_capacity(capacity);

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

		for layer in &state_ref.layers {
			// Every iterator of this layer is wrapped in a SeqCappedIterator
			// BEFORE the merge: per-layer fork caps cannot be expressed by
			// the merged stream's global snapshot filter.
			let mut layer_iterators: Vec<BoxedLSMIterator<'a>> = Vec::new();

			// Active memtable (absent for an idle branch's layer)
			if let Some(active) = &layer.active {
				let active_iter = active.range(lower, upper);
				layer_iterators.push(Box::new(active_iter) as BoxedLSMIterator<'a>);
			}

			// Immutable memtables
			for memtable in &layer.immutable {
				let iter = memtable.range(lower, upper);
				layer_iterators.push(Box::new(iter) as BoxedLSMIterator<'a>);
			}

			// Tables - these have native seek support
			for (level_idx, level) in (&layer.levels).into_iter().enumerate() {
				// Optimization: Skip tables that are completely outside the query range
				if level_idx == 0 {
					// Level 0: Tables can overlap, so we check all but skip those completely
					// outside range
					for table in &level.tables {
						// Skip tables completely before or after the range
						if table.is_before_range(&query_range) || table.is_after_range(&query_range)
						{
							continue;
						}
						// Skip tables outside timestamp range (if specified)
						if let Some((ts_start, ts_end)) = ts_range {
							let props = &table.meta.properties;
							if let (Some(newest), Some(oldest)) =
								(props.newest_key_time, props.oldest_key_time)
							{
								if newest < ts_start || oldest > ts_end {
									continue;
								}
							}
						}
						// Use custom comparator for table iteration
						if let Ok(table_iter) = table
							.iter_with_comparator(Some((*query_range).clone()), Arc::clone(&cmp))
						{
							layer_iterators.push(Box::new(table_iter) as BoxedLSMIterator<'a>);
						}
					}
				} else {
					// Level 1+: Tables have non-overlapping key ranges, use binary search
					let start_idx = level.find_first_overlapping_table(&query_range);
					let end_idx = level.find_last_overlapping_table(&query_range);

					for table in &level.tables[start_idx..end_idx] {
						// Skip tables outside timestamp range (if specified)
						if let Some((ts_start, ts_end)) = ts_range {
							let props = &table.meta.properties;
							if let (Some(newest), Some(oldest)) =
								(props.newest_key_time, props.oldest_key_time)
							{
								if newest < ts_start || oldest > ts_end {
									continue;
								}
							}
						}
						// Use custom comparator for table iteration
						if let Ok(table_iter) = table
							.iter_with_comparator(Some((*query_range).clone()), Arc::clone(&cmp))
						{
							layer_iterators.push(Box::new(table_iter) as BoxedLSMIterator<'a>);
						}
					}
				}
			}

			for inner in layer_iterators {
				iterators.push(
					Box::new(SeqCappedIterator::new(inner, layer.cap)) as BoxedLSMIterator<'a>
				);
			}
		}

		Self {
			iterators,
			iter_state: boxed_state,
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
	/// Creates a new iterator over a specific key range.
	///
	/// Takes the real snapshot: it must not construct a stand-in `Snapshot`
	/// to reuse `collect_iter_state`, because a stand-in was never registered
	/// with the tracker yet its drop unregistered the caller's sequence,
	/// silently stripping the live snapshot's compaction protection.
	fn new_from(snapshot: &Snapshot, range: InternalKeyRange) -> Result<Self> {
		let iter_state = snapshot.collect_iter_state()?;

		let merge_iter = KMergeIterator::new_from(iter_state, range);

		Ok(Self {
			merge_iter,
			snapshot_seq_num: snapshot.seq_num,
			core: Arc::clone(&snapshot.core),
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
	/// Thin wrapper kept for symmetry with `skip_to_valid_forward`; all real
	/// work happens in `find_latest_visible_backward`, which loops internally.
	fn skip_to_valid_backward(&mut self) -> Result<bool> {
		self.find_latest_visible_backward()
	}

	/// Find the latest visible version of the next user key going backward.
	///
	/// Backward iteration sees the oldest version of each user key first
	/// (lowest seq_num) and must walk back to the newest, picking the latest
	/// version that is visible to this snapshot. If that latest version turns
	/// out to be a tombstone (or no visible version exists at all), we must
	/// skip the user key entirely and examine the next one back.
	///
	/// This was previously implemented via mutual tail-recursion between
	/// `find_latest_visible_backward` and `skip_to_valid_backward`. Each
	/// fully-tombstoned (or fully-invisible) user key consumed two stack
	/// frames, so a backward range scan over a long run of such keys --- a
	/// pattern that occurs naturally after deletes against an MVCC store ---
	/// would overflow the thread stack. The loop below is semantically
	/// identical but uses O(1) stack regardless of how many user keys we skip.
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
				if key_ref.is_tombstone() {
					// Latest visible is a tombstone -- skip this user key and
					// examine the next one. (Was: recursive call.)
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
		let seek_key = InternalKey::new(
			current_user_key,
			crate::INTERNAL_KEY_SEQ_NUM_MAX,
			InternalKeyKind::Set,
			crate::INTERNAL_KEY_TIMESTAMP_MAX,
		);
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

// ===== Unified History Iterator =====

#[derive(Clone)]
struct BufferedEntry {
	key: Vec<u8>,
	value: Vec<u8>,
}

pub struct HistoryIterator<'a> {
	inner: KMergeIterator<'a>,
	snapshot_seq_num: u64,
	include_tombstones: bool,
	direction: MergeDirection,
	initialized: bool,
	lower_bound: Option<Vec<u8>>,
	upper_bound: Option<Vec<u8>>,
	// === Forward iteration state (streaming) ===
	current_user_key: Vec<u8>,
	first_visible_seen: bool,
	latest_is_hard_delete: bool,
	barrier_seen: bool, // True once we hit HARD_DELETE or REPLACE

	// === Backward iteration state (buffered) ===
	backward_buffer: Vec<BufferedEntry>,
	backward_buffer_index: Option<usize>,

	// === Filtering options ===
	ts_range: Option<(u64, u64)>, // (start_ts, end_ts) inclusive
	limit: Option<usize>,
	entries_returned: usize,
	limit_reached: bool,
}

impl<'a> HistoryIterator<'a> {
	/// Creates a HistoryIterator from a pre-built KMergeIterator over the
	/// LSM (memtables + SSTables).
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		merge_iter: KMergeIterator<'a>,
		seq_num: u64,
		include_tombstones: bool,
		lower: Option<&[u8]>,
		upper: Option<&[u8]>,
		ts_range: Option<(u64, u64)>,
		limit: Option<usize>,
	) -> Self {
		Self {
			inner: merge_iter,
			snapshot_seq_num: seq_num,
			include_tombstones,
			direction: MergeDirection::Forward,
			initialized: false,
			lower_bound: lower.map(|b| b.to_vec()),
			upper_bound: upper.map(|b| b.to_vec()),
			current_user_key: Vec::new(),
			first_visible_seen: false,
			latest_is_hard_delete: false,
			barrier_seen: false,
			backward_buffer: Vec::new(),
			backward_buffer_index: None,
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
		range: InternalKeyRange,
		include_tombstones: bool,
		ts_range: Option<(u64, u64)>,
		limit: Option<usize>,
		lower: Option<&[u8]>,
		upper: Option<&[u8]>,
	) -> Self {
		// Use TimestampComparator for history queries with timestamp range
		// This enables efficient timestamp-based seeks when timestamps are monotonic with seq_nums
		let inner = if ts_range.is_some() {
			KMergeIterator::new_for_history(iter_state, range, ts_range)
		} else {
			KMergeIterator::new_from(iter_state, range)
		};

		Self::new(inner, seq_num, include_tombstones, lower, upper, ts_range, limit)
	}

	fn reset_forward_state(&mut self) {
		self.current_user_key.clear();
		self.first_visible_seen = false;
		self.latest_is_hard_delete = false;
		self.barrier_seen = false;
	}

	fn clear_backward_buffer(&mut self) {
		self.backward_buffer.clear();
		self.backward_buffer_index = None;
	}

	fn reset_all_state(&mut self) {
		self.reset_forward_state();
		self.clear_backward_buffer();
		self.entries_returned = 0;
		self.limit_reached = false;
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

	/// Skip all remaining entries for the current user_key.
	/// Returns true if positioned on a new user_key, false if iterator exhausted.
	fn skip_to_next_user_key(&mut self) -> Result<bool> {
		let current = self.current_user_key.clone();
		while self.inner_valid() {
			if self.inner_key().user_key() != current.as_slice() {
				return Ok(true);
			}
			self.inner_next()?;
		}
		Ok(false)
	}

	/// With ts_range, seek to (next_user_key, ts_end) to skip entries above range.
	/// Without ts_range, linearly scan past entries with the same user_key.
	/// Returns true if positioned on a new user_key, false if iterator exhausted.
	fn advance_to_next_user_key(&mut self) -> Result<bool> {
		// Only optimize with ts_range
		let ts_end = match self.ts_range {
			Some((_, end)) => end,
			None => return self.skip_to_next_user_key(),
		};

		let current = self.current_user_key.clone();

		// Advance to find next user_key
		while self.inner_valid() {
			let next_key_vec = self.inner_key().user_key().to_vec();
			if next_key_vec != current {
				// Found next key - seek to (next_key, ts_end) to skip entries above range
				let seek_key = InternalKey::new(
					next_key_vec,
					crate::INTERNAL_KEY_SEQ_NUM_MAX,
					InternalKeyKind::Set,
					ts_end,
				);
				self.inner.seek(&seek_key.encode())?;
				return Ok(self.inner_valid());
			}
			self.inner_next()?;
		}
		Ok(false)
	}

	// --- Bounds checking ---
	// KMergeIterator handles bounds via InternalKeyRange, but upper_bound
	// is still needed for memtable iterators that don't clamp the upper bound.

	fn within_upper_bound(&self) -> bool {
		if let Some(ref upper) = self.upper_bound {
			if self.inner_valid() {
				self.inner_key().user_key() < upper.as_slice()
			} else {
				false
			}
		} else {
			true
		}
	}

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

	// === FORWARD ITERATION (Streaming) ===

	/// Skip to next valid entry in forward direction.
	///
	/// Barriers (first one wins):
	/// - HARD_DELETE: skip it and everything older
	/// - REPLACE: output it, skip everything older
	fn skip_to_valid_forward(&mut self) -> Result<bool> {
		while self.inner_valid() {
			// Check limit before returning any entry
			if let Some(limit) = self.limit {
				if self.entries_returned >= limit {
					self.limit_reached = true;
					return Ok(false);
				}
			}

			if !self.within_upper_bound() {
				return Ok(false);
			}

			let (user_key_vec, seq_num, timestamp, is_hard_delete, is_replace, is_tombstone) = {
				let key_ref = self.inner_key();
				(
					key_ref.user_key().to_vec(),
					key_ref.seq_num(),
					key_ref.timestamp(),
					key_ref.is_hard_delete_marker(),
					key_ref.is_replace(),
					key_ref.is_tombstone(),
				)
			};

			// Skip keys below lower_bound
			if !self.user_key_within_lower_bound(&user_key_vec) {
				self.inner_next()?;
				continue;
			}

			// Detect user_key change → reset state
			if user_key_vec != self.current_user_key {
				self.current_user_key = user_key_vec;
				self.first_visible_seen = false;
				self.latest_is_hard_delete = false;
				self.barrier_seen = false;
			}

			// Skip invisible versions
			if seq_num > self.snapshot_seq_num {
				self.inner_next()?;
				continue;
			}

			// Skip entries outside timestamp range
			if let Some((ts_start, ts_end)) = self.ts_range {
				if timestamp > ts_end {
					// Above range - skip, next entries might be in range
					self.inner_next()?;
					continue;
				}
				if timestamp < ts_start {
					// Below range - all remaining entries for this key are also below
					// (timestamps are ordered descending within a key).
					// Skip to next user_key.
					if !self.advance_to_next_user_key()? {
						return Ok(false);
					}
					continue;
				}
			}

			// First visible entry → check for HARD_DELETE as latest
			if !self.first_visible_seen {
				self.first_visible_seen = true;
				if is_hard_delete {
					self.latest_is_hard_delete = true;
				}
			}

			// Rule 1: HARD_DELETE as latest → skip entire key
			if self.latest_is_hard_delete {
				self.inner_next()?;
				continue;
			}

			// Rule 2: Already past a barrier → skip everything older
			if self.barrier_seen {
				self.inner_next()?;
				continue;
			}

			// Rule 3: Hit HARD_DELETE barrier (not latest)
			// Skip this entry and mark barrier
			if is_hard_delete {
				self.barrier_seen = true;
				self.inner_next()?;
				continue;
			}

			// Rule 4: Hit REPLACE barrier
			// Output this entry, then mark barrier for older entries
			if is_replace {
				self.barrier_seen = true;
				// Don't skip - fall through to output
			}

			// Rule 5: Soft DELETE (tombstone) filtering
			if !self.include_tombstones && is_tombstone {
				self.inner_next()?;
				continue;
			}

			// Found valid entry - increment counter
			self.entries_returned += 1;
			return Ok(true);
		}
		Ok(false)
	}

	// === BACKWARD ITERATION (Buffered) ===

	/// Collect all visible versions of current user key, apply filtering,
	/// and populate backward_buffer.
	///
	/// After this call, inner iterator is at previous user key (or invalid).
	fn collect_user_key_backward(&mut self) -> Result<bool> {
		self.backward_buffer.clear();

		if !self.inner_valid() {
			return Ok(false);
		}

		let user_key = self.inner_key().user_key().to_vec();

		if !self.user_key_within_lower_bound(&user_key) {
			return Ok(false);
		}

		if !self.user_key_within_upper_bound(&user_key) {
			while self.inner_valid() && self.inner_key().user_key() == user_key.as_slice() {
				self.inner_prev()?;
			}
			return self.collect_user_key_backward();
		}

		// Collect all visible versions
		// Backward storage order: (user_key DESC, seq_num ASC) → oldest first
		struct VersionInfo {
			is_hard_delete: bool,
			is_replace: bool,
			is_tombstone: bool,
			encoded_key: Vec<u8>,
			value: Vec<u8>,
		}
		let mut versions: Vec<VersionInfo> = Vec::new();

		while self.inner_valid() {
			let key_ref = self.inner_key();

			if key_ref.user_key() != user_key.as_slice() {
				break;
			}

			let seq_num = key_ref.seq_num();
			let timestamp = key_ref.timestamp();

			// Check visibility and timestamp range
			let visible = seq_num <= self.snapshot_seq_num;
			let in_ts_range = match self.ts_range {
				Some((ts_start, ts_end)) => timestamp >= ts_start && timestamp <= ts_end,
				None => true,
			};

			if visible && in_ts_range {
				versions.push(VersionInfo {
					is_hard_delete: key_ref.is_hard_delete_marker(),
					is_replace: key_ref.is_replace(),
					is_tombstone: key_ref.is_tombstone(),
					encoded_key: key_ref.encoded().to_vec(),
					value: self.inner_value()?.to_vec(),
				});
			}

			self.inner_prev()?;
		}

		if versions.is_empty() {
			return Ok(false);
		}

		// versions are in seq_num ASC order (oldest first, newest last)
		// Latest visible is the LAST element
		let latest = versions.last().unwrap();

		// Rule 1: HARD_DELETE as latest → skip entire key
		if latest.is_hard_delete {
			return Ok(false);
		}

		// Rule 2: Find first barrier from newest (search from end to start)
		// Barrier can be HARD_DELETE or REPLACE
		let mut barrier_idx: Option<usize> = None;
		let mut barrier_is_hard_delete = false;

		for i in (0..versions.len()).rev() {
			if versions[i].is_hard_delete {
				barrier_idx = Some(i);
				barrier_is_hard_delete = true;
				break;
			}
			if versions[i].is_replace {
				barrier_idx = Some(i);
				barrier_is_hard_delete = false;
				break;
			}
		}

		// Determine valid range based on barrier
		let valid_start_idx = match barrier_idx {
			Some(idx) if barrier_is_hard_delete => idx + 1, // Exclude HARD_DELETE and older
			Some(idx) => idx,                               // Include REPLACE, exclude older
			None => 0,                                      // No barrier, include all
		};

		// Output versions[valid_start_idx..] in ASC order (oldest first for backward)
		for v in versions.into_iter().skip(valid_start_idx) {
			// Skip HARD_DELETE markers (shouldn't happen after valid_start_idx, but be safe)
			if v.is_hard_delete {
				continue;
			}

			// Tombstone filtering
			if !self.include_tombstones && v.is_tombstone {
				continue;
			}

			self.backward_buffer.push(BufferedEntry {
				key: v.encoded_key,
				value: v.value,
			});
		}

		if self.backward_buffer.is_empty() {
			return Ok(false);
		}

		// Truncate buffer to respect limit
		if let Some(limit) = self.limit {
			let remaining = limit.saturating_sub(self.entries_returned);
			if remaining == 0 {
				self.backward_buffer.clear();
				self.limit_reached = true;
				return Ok(false);
			}
			if self.backward_buffer.len() > remaining {
				self.backward_buffer.truncate(remaining);
			}
		}

		// Pre-increment entries_returned by buffer size
		// (all buffered entries will be yielded before next collect)
		self.entries_returned += self.backward_buffer.len();

		// Start yielding from index 0 (oldest in valid range)
		self.backward_buffer_index = Some(0);

		Ok(true)
	}

	fn advance_backward(&mut self) -> Result<bool> {
		if let Some(idx) = self.backward_buffer_index {
			if idx + 1 < self.backward_buffer.len() {
				self.backward_buffer_index = Some(idx + 1);
				return Ok(true);
			}
		}

		// Buffer exhausted, load previous user key
		self.collect_user_key_backward()
	}

	fn buffered_key(&self) -> InternalKeyRef<'_> {
		let idx = self.backward_buffer_index.unwrap();
		InternalKeyRef::from_encoded(&self.backward_buffer[idx].key)
	}

	fn buffered_value(&self) -> &[u8] {
		let idx = self.backward_buffer_index.unwrap();
		&self.backward_buffer[idx].value
	}

	fn has_buffered_entry(&self) -> bool {
		matches!(self.backward_buffer_index, Some(idx) if idx < self.backward_buffer.len())
	}

	/// Switch from backward to forward direction.
	/// Uses seek-based repositioning to avoid KMergeIterator direction-switch complexity.
	fn reverse_to_forward(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.reset_forward_state();

		if !self.has_buffered_entry() {
			self.clear_backward_buffer();
			return Ok(false);
		}

		// Get current position from backward buffer
		let current_internal_key = self.buffered_key().encoded().to_vec();
		self.clear_backward_buffer();

		// Seek to current position - this resets KMergeIterator to Forward mode
		self.inner.seek(&current_internal_key)?;

		if !self.inner_valid() {
			return Ok(false);
		}

		// Move past current entry to get the NEXT entry in forward direction
		self.inner_next()?;

		// Find next valid entry
		self.skip_to_valid_forward()
	}

	/// Switch from forward to backward direction.
	///
	/// In forward mode, inner is positioned at the current entry. We call prev() to move
	/// to the previous user key, then collect that key's versions for backward iteration.
	/// This matches SnapshotIterator's forward_to_backward behavior.
	fn forward_to_backward(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Backward;
		self.clear_backward_buffer();

		if !self.inner_valid() {
			return Ok(false);
		}

		// Move backward from current position to previous user key.
		// SnapshotIterator does the same: merge_iter.prev() from current position.
		self.inner_prev()?;

		// Collect user key at new position
		self.collect_user_key_backward()
	}
}

impl LSMIterator for HistoryIterator<'_> {
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.reset_all_state();

		self.inner.seek(target)?;
		self.initialized = true;
		self.skip_to_valid_forward()
	}

	fn seek_first(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.reset_all_state();

		if self.ts_range.is_some() {
			// Seek to (lower_bound or empty, ts_end) to skip entries above range
			let ts = self.ts_range.map(|(_, end)| end).unwrap_or(crate::INTERNAL_KEY_TIMESTAMP_MAX);
			let seek_key = InternalKey::new(
				self.lower_bound.clone().unwrap_or_default(),
				u64::MAX,
				InternalKeyKind::Set,
				ts,
			);
			self.inner.seek(&seek_key.encode())?;
		} else if let Some(ref lower) = self.lower_bound {
			let seek_key =
				InternalKey::new(lower.clone(), u64::MAX, InternalKeyKind::Set, u64::MAX);
			self.inner.seek(&seek_key.encode())?;
		} else {
			self.inner.seek_first()?;
		}

		self.initialized = true;
		self.skip_to_valid_forward()
	}

	fn seek_last(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Backward;
		self.reset_all_state();

		self.inner.seek_last()?;
		self.initialized = true;
		self.collect_user_key_backward()
	}

	fn next(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_first();
		}

		// Direction change: backward → forward
		if self.direction == MergeDirection::Backward {
			return self.reverse_to_forward();
		}

		// Normal forward iteration
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

		// Direction change: forward → backward
		if self.direction != MergeDirection::Backward {
			return self.forward_to_backward();
		}

		// Normal backward iteration
		if !self.has_buffered_entry() && !self.inner_valid() {
			return Ok(false);
		}

		self.advance_backward()
	}

	fn valid(&self) -> bool {
		if self.limit_reached {
			return false;
		}
		match self.direction {
			MergeDirection::Forward => self.inner_valid() && self.within_upper_bound(),
			MergeDirection::Backward => self.has_buffered_entry(),
		}
	}

	fn key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.valid());
		match self.direction {
			MergeDirection::Forward => self.inner_key(),
			MergeDirection::Backward => self.buffered_key(),
		}
	}

	fn value_encoded(&self) -> Result<&[u8]> {
		debug_assert!(self.valid());
		match self.direction {
			MergeDirection::Forward => self.inner_value(),
			MergeDirection::Backward => Ok(self.buffered_value()),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::SnapshotTracker;

	#[test]
	fn test_snapshot_tracker_ordering() {
		let tracker = SnapshotTracker::new();

		// Insert snapshots in non-sorted order
		let g100 = tracker.register(100);
		let g50 = tracker.register(50);
		let _g200 = tracker.register(200);
		let _g75 = tracker.register(75);
		let _g150 = tracker.register(150);

		// Verify get_all_snapshots returns sorted order
		let snapshots = tracker.get_all_snapshots();
		assert_eq!(snapshots, vec![50, 75, 100, 150, 200]);

		// Release some and verify order is maintained
		drop(g100);
		drop(g50);

		let snapshots = tracker.get_all_snapshots();
		assert_eq!(snapshots, vec![75, 150, 200]);

		// Add more and verify
		let _g25 = tracker.register(25);
		let _g300 = tracker.register(300);

		let snapshots = tracker.get_all_snapshots();
		assert_eq!(snapshots, vec![25, 75, 150, 200, 300]);
	}

	/// Regression for the set-collision bug: two snapshots sharing one
	/// sequence (any two read transactions with no commit in between) must
	/// each hold an independent registration. Dropping one — or a stand-in
	/// created and dropped mid-read, as `SnapshotIterator::new_from` once
	/// did — must not strip the survivor's compaction protection.
	#[test]
	fn same_seq_registrations_do_not_collide() {
		let tracker = SnapshotTracker::new();

		let first = tracker.register(42);
		let second = tracker.register(42);
		assert_eq!(tracker.get_all_snapshots(), vec![42], "deduplicated view");

		drop(first);
		assert_eq!(
			tracker.get_all_snapshots(),
			vec![42],
			"the surviving snapshot must keep sequence 42 protected"
		);
		assert_eq!(tracker.first(), Some(42));

		drop(second);
		assert_eq!(tracker.get_all_snapshots(), Vec::<u64>::new());
		assert_eq!(tracker.first(), None);
	}

	#[test]
	fn test_snapshot_tracker_empty() {
		let tracker = SnapshotTracker::new();
		assert!(tracker.get_all_snapshots().is_empty());
	}

	#[test]
	fn test_snapshot_tracker_clone_shares_state() {
		let tracker1 = SnapshotTracker::new();
		let _g100 = tracker1.register(100);

		let tracker2 = tracker1.clone();
		let _g50 = tracker2.register(50);

		// Both should see the same snapshots
		assert_eq!(tracker1.get_all_snapshots(), vec![50, 100]);
		assert_eq!(tracker2.get_all_snapshots(), vec![50, 100]);
	}
}
