use std::collections::HashSet;
use std::fs::create_dir_all;
#[cfg(not(any(target_os = "windows", target_family = "wasm")))]
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};

use crate::batch::Batch;
use crate::checkpoint::{CheckpointGate, CheckpointMetadata, DatabaseCheckpoint};
use crate::compaction::compactor::{CompactionOptions, Compactor};
use crate::compaction::CompactionStrategy;
use crate::error::{BackgroundErrorHandler, Result};
use crate::levels::{
	check_manifest_before_open,
	write_manifest_to_disk,
	LevelManifest,
	ManifestChangeSet,
};
use crate::lockfile::LockFile;
use crate::memtable::{ImmutableEntry, ImmutableMemtables, MemTable};
use crate::snapshot::SnapshotTracker;
use crate::sstable::table::Table;
use crate::stall::{StallCounts, StallThresholds, WriteStallCountProvider};
use crate::task::TaskManager;
use crate::transaction::{Mode, Transaction, TransactionOptions};
use crate::vlog::{VLog, ValueLocation};
use crate::wal::recovery::{repair_corrupted_wal_segment, replay_wal};
use crate::wal::{self, cleanup_old_segments, Wal, WalManager};
use crate::{
	Comparator,
	Error,
	FilterPolicy,
	Key,
	LSMIterator,
	Options,
	VLogChecksumLevel,
	Value,
	WalRecoveryMode,
};

// ===== Compaction Operations Trait =====
/// Defines the compaction operations that can be performed on an LSM tree.
/// Compaction is essential for maintaining read performance by merging
/// overlapping SSTables and removing deleted entries.
pub trait CompactionOperations: Send + Sync {
	/// Flushes the active memtable to disk, converting it into an immutable
	/// SSTable. This is the first step in the LSM tree's write path.
	fn compact_memtable(&self) -> Result<()>;

	/// Performs compaction according to the specified strategy.
	/// Compaction merges SSTables to reduce read amplification and remove
	/// tombstones.
	fn compact(&self, strategy: Arc<dyn CompactionStrategy>) -> Result<()>;

	/// Returns a reference to the background error handler
	fn error_handler(&self) -> Arc<BackgroundErrorHandler>;

	/// Returns true if there are immutable memtables pending flush.
	fn has_pending_immutables(&self) -> bool;
}

// ===== Core LSM Tree Implementation =====
/// The core of an LSM (Log-Structured Merge) tree implementation.
///
/// # LSM Tree Overview
/// An LSM tree optimizes for write performance by buffering writes in memory
/// and periodically flushing them to disk as sorted, immutable files
/// (SSTables). Reads must check multiple locations: the active memtable,
/// immutable memtables, and multiple levels of SSTables.
///
/// # Components
/// - **Active Memtable**: An in-memory, mutable data structure (usually a skip list or B-tree) that
///   receives all new writes.
/// - **Immutable Memtables**: Former active memtables that are full and awaiting flush to disk.
///   They serve reads but accept no new writes.
/// - **SSTables**: Sorted String Tables on disk, organized into levels. Each level has
///   progressively larger SSTables with non-overlapping key ranges (except L0).
/// - **Compaction**: Background process that merges SSTables to maintain read performance and
///   remove deleted entries.
///
/// # Lock Ordering
///
/// To prevent deadlocks, locks must be acquired in this order:
/// 1. `active_memtable` - serializes writes
/// 2. `level_manifest` - SST metadata and table IDs
/// 3. `immutable_memtables` - pending flush queue
///
/// Read locks and write locks follow the same ordering.
/// If a function needs multiple locks, it must acquire them in this order.
/// See `rotate_memtable()`, `flush_immutable_to_sst()` for examples.
///
/// `flush_lock`, which serializes the flushes of the immutable queue, comes before all of
/// them: a flush, the shutdown flush and a restore hold it while they take the others, and
/// nothing waits for it while holding one of them.
///
/// The WAL's own lock (`wal`) sits below every lock above. Under it are taken only the segment's
/// fsync gate, which is a leaf, and, by a rotation (`rotate_wal()`), the value log's writer lock
/// and its list of unsynced files, which nothing holds while it waits for the WAL lock: the commit
/// pipeline appends to the value log before it takes the WAL lock and never holds both. The
/// rotation that opening a tree and a restore make to continue in a fresh segment takes them the
/// same way.
/// The WAL lock is taken under `active_memtable` by rotation (`rotate_memtable()`,
/// `seal_active_wal_segment()`, `replace_failed_wal()`), and so do the shutdown flush, the
/// release of a held writer (`try_release_wal_hold()`, which reads the immutable queue under
/// `active_memtable` too, never under the WAL lock) and the commit pipeline's fenced round.
/// Everything else takes it alone, and some of it from pool threads: the commit pipeline
/// for every append and every fsync of a commit group (`AffinityLogStore`), and `flush_wal`,
/// `close` and restore. It is held across the fsync of a commit group, but not across the one
/// `WalManager::sync` makes, which goes through the gate instead. The commit pipeline compares
/// the segment it appended to with the tag of the active memtable under
/// `active_memtable.read()`, which rotation needs exclusively, so a record is never applied to a
/// memtable tagged with a different segment. A group that rotations keep overtaking appends
/// under `active_memtable.read()` and then the WAL lock instead, in the same order as rotation,
/// and applies under the same read guard.
pub(crate) struct CoreInner {
	/// The active memtable (write buffer) that receives all new writes.
	///
	/// In LSM trees, all writes first go to an in-memory structure for fast
	/// insertion. This memtable is typically implemented as a skip list or
	/// balanced tree to maintain sorted order while supporting concurrent
	/// access.
	pub(crate) active_memtable: Arc<RwLock<Arc<MemTable>>>,

	/// Collection of immutable memtables waiting to be flushed to disk.
	///
	/// When the active memtable fills up (reaches max_memtable_size), it
	/// becomes immutable and a new active memtable is created. These immutable
	/// memtables continue serving reads while waiting for background threads
	/// to flush them to disk as SSTables.
	pub(crate) immutable_memtables: Arc<RwLock<ImmutableMemtables>>,

	/// Held for the whole of a flush of the immutable queue, so the background task, a
	/// checkpoint and the shutdown flush never flush one memtable twice and always take the
	/// oldest one first, and a restore never replaces the manifest under a flush.
	pub(crate) flush_lock: parking_lot::Mutex<()>,

	/// The level structure managing all SSTables on disk.
	///
	/// LSM trees organize SSTables into levels:
	/// - L0: Contains SSTables flushed directly from memtables. May have overlapping key ranges.
	/// - L1+: Each level is larger than the previous. SSTables have non-overlapping key ranges
	///   within a level, enabling efficient binary search.
	pub level_manifest: Arc<RwLock<LevelManifest>>,

	/// Configuration options controlling LSM tree behavior
	pub opts: Arc<Options>,

	/// Tracker for active snapshot sequence numbers for MVCC (Multi-Version
	/// Concurrency Control). Snapshots provide consistent point-in-time views
	/// of the data. The tracker stores actual sequence numbers to enable
	/// snapshot-aware compaction.
	pub(crate) snapshot_tracker: SnapshotTracker,

	/// Pins of every live mutating transaction, which hold the commit ring's
	/// retired watermark at or below their conflict windows, so the ring
	/// entries in those windows stay reachable. Separate from
	/// `snapshot_tracker` because write-only txns need this protection but
	/// don't hold MVCC snapshots, and read-only txns hold snapshots but never
	/// commit.
	pub(crate) active_txn_tracker: Arc<crate::tracker::ActiveTxnTracker>,

	/// Value Log (VLog)
	pub(crate) vlog: Option<Arc<VLog>>,

	/// Write-Ahead Log (WAL) for durability
	pub(crate) wal: WalManager,

	/// Lock file to prevent multiple processes from opening the same database
	pub(crate) lockfile: Mutex<LockFile>,

	/// Background error handler
	pub(crate) error_handler: Arc<BackgroundErrorHandler>,

	/// Visible sequence number - the highest sequence number that is visible to readers.
	/// Shared with CommitPipeline for coordinated updates.
	pub(crate) visible_seq_num: Arc<AtomicU64>,

	/// The oldest WAL segment that holds the record of a batch of the commit group in flight that
	/// is not applied yet, `u64::MAX` when there is none. Once part of a group was applied, such a
	/// record may be the only copy of its batch, until the batch is logged again in a newer
	/// segment. A flush that moved `log_number` past it would remove it, and a group that then
	/// failed would reach recovery in part.
	pub(crate) group_wal_pin: AtomicU64,

	/// Global memory controller accounting memory across memtables, ring buffer, and cache.
	pub(crate) memory_controller: Arc<crate::memory::MemoryController>,

	/// Whether the manifest was read from disk in this run, as opposed to created by it.
	/// A manifest created in this run lists no tables, so it cannot say which SSTables
	/// are orphans.
	pub(crate) manifest_loaded_from_disk: bool,

	/// Set when a flush's or a direct table's manifest write failed from the rename on: the
	/// manifest on disk may list a table the one in memory does not. The next flush, direct table
	/// or checkpoint writes the one in memory again first, which a flush would otherwise follow
	/// by rewriting the file of a table the disk may list.
	manifest_uncertain: AtomicBool,

	/// Test-only observer called by `flush_immutable_to_sst` once the SST is on disk and before
	/// the manifest is updated. An error from it fails the flush.
	#[cfg(test)]
	pub(crate) flush_hook: parking_lot::Mutex<Option<FlushHook>>,

	/// Test-only observer called by `write_batch_direct_to_l0_sst` once its table is open and
	/// before the manifest is updated. An error from it fails the write.
	#[cfg(test)]
	pub(crate) direct_table_hook: parking_lot::Mutex<Option<FlushHook>>,

	/// Test-only observer called by `create_checkpoint` at each of its stages.
	#[cfg(test)]
	pub(crate) checkpoint_hook: parking_lot::Mutex<Option<crate::checkpoint::CheckpointHook>>,
}

/// What a failed `write_batch_direct_to_l0_sst` leaves behind.
#[derive(Default)]
struct DirectL0Progress {
	/// The file of the table was created, so it is this write's to remove.
	created: bool,
	/// The manifest on disk may list the table, so its file must stay.
	table_may_be_listed: bool,
}

/// See `CoreInner::flush_hook`; the argument is the table id.
#[cfg(test)]
pub(crate) type FlushHook = Arc<dyn Fn(u64) -> Result<()> + Send + Sync>;

impl CoreInner {
	/// Creates a new LSM tree core instance
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		// Acquire database lock to prevent multiple processes from opening the same
		// database
		let mut lockfile = LockFile::new(&opts.path);
		lockfile.acquire()?;

		// Initialize immutable memtables
		let immutable_memtables = Arc::new(RwLock::new(ImmutableMemtables::default()));

		// Initialize level manifest FIRST to get log_number. Whether it exists must be read
		// first, because `LevelManifest::new` creates one when it does not.
		let manifest_loaded_from_disk = opts.manifest_file_path(0).exists();
		let manifest = LevelManifest::new(Arc::clone(&opts))?;
		let manifest_log_number = manifest.get_log_number();

		// Initialize WAL starting from manifest.log_number
		let wal_path = opts.wal_dir();
		// This avoids creating intermediate empty WAL files
		let wal_instance =
			Wal::open_with_min_log_number(&wal_path, manifest_log_number, wal::Options::default())?;

		// Starts at 0 since no commits have happened yet.
		let visible_seq_num = Arc::new(AtomicU64::new(0));

		// Initialize active memtable with its WAL number set to the initial WAL
		// This tracks which WAL the memtable's data belongs to for later flush
		let initial_memtable = Arc::new(MemTable::new(opts.max_memtable_size));
		initial_memtable.set_wal_number(wal_instance.get_active_log_number());
		let active_memtable = Arc::new(RwLock::new(initial_memtable));

		let level_manifest = Arc::new(RwLock::new(manifest));

		let vlog = if opts.enable_vlog {
			Some(Arc::new(VLog::new(Arc::clone(&opts))?))
		} else {
			None
		};
		let memory_controller = Arc::new(crate::memory::MemoryController::default());

		Ok(Self {
			opts,
			active_memtable,
			immutable_memtables,
			flush_lock: parking_lot::Mutex::new(()),
			level_manifest,
			snapshot_tracker: SnapshotTracker::new(),
			active_txn_tracker: Arc::new(crate::tracker::ActiveTxnTracker::new()),
			vlog,
			wal: WalManager::new(wal_instance),
			lockfile: Mutex::new(lockfile),
			error_handler: Arc::new(BackgroundErrorHandler::new()),
			visible_seq_num,
			group_wal_pin: AtomicU64::new(u64::MAX),
			memory_controller,
			manifest_loaded_from_disk,
			manifest_uncertain: AtomicBool::new(false),
			#[cfg(test)]
			flush_hook: parking_lot::Mutex::new(None),
			#[cfg(test)]
			direct_table_hook: parking_lot::Mutex::new(None),
			#[cfg(test)]
			checkpoint_hook: parking_lot::Mutex::new(None),
		})
	}

	pub(crate) fn immutable_count(&self) -> usize {
		self.immutable_memtables.read().map(|imm| imm.iter().count()).unwrap_or(0)
	}

	pub(crate) fn l0_file_count(&self) -> usize {
		self.level_manifest
			.read()
			.map(|m| m.levels.get_levels().first().map(|l| l.tables.len()).unwrap_or(0))
			.unwrap_or(0)
	}

	/// Holds `log_number`, the one a flush is about to record, at the segment `pin`, but never
	/// behind `current`, the one the manifest has: it only moves forward.
	fn hold_log_number(log_number: u64, current: u64, pin: u64) -> u64 {
		if log_number > pin {
			pin.max(current).min(log_number)
		} else {
			log_number
		}
	}

	/// Flushes a memtable to SST and atomically updates the manifest.
	///
	/// This is the core primitive used by all flush operations. It handles:
	/// 1. Flushing the memtable to disk as an SSTable
	/// 2. Creating a changeset with the new SST and log_number update
	/// 3. Atomically applying the changeset to the manifest
	/// 4. Removing the memtable from immutable_memtables tracking
	///
	/// A failure leaves the memtable tracked and the manifest in memory as it was, so the flush
	/// can be run again. It is for the caller to say whether the failure stops the database.
	/// A failure of the manifest write from the rename on is an `Error::ManifestWriteUncertain`.
	///
	/// It deletes no value-log files: recovery flushes the memtables it rebuilds one by one while
	/// the ones after them are held outside the queue, and the files those point into must stay.
	/// The flush of the queue does it, see `flush_oldest_immutable_to_sst`.
	///
	/// # Arguments
	/// * `memtable` - The memtable to flush
	/// * `table_id` - Table ID for the new SST
	/// * `wal_number` - WAL number to mark as flushed (log_number = wal_number + 1)
	///
	/// # Returns
	/// The flushed SSTable
	fn flush_immutable_to_sst(
		&self,
		memtable: Arc<MemTable>,
		table_id: u64,
		wal_number: u64,
	) -> Result<Arc<Table>> {
		self.flush_immutable_to_sst_holding(memtable, table_id, wal_number, u64::MAX)
	}

	/// Like `flush_immutable_to_sst`, but `log_number` is not moved past the segment `hold`: the
	/// oldest segment that still holds records which are in no table and in no queued memtable,
	/// `u64::MAX` if there is none. Recovery uses it for a memtable that is only the first part of
	/// a segment: the rest of the segment is replayed into a memtable that is not flushed yet.
	fn flush_immutable_to_sst_holding(
		&self,
		memtable: Arc<MemTable>,
		table_id: u64,
		wal_number: u64,
		hold: u64,
	) -> Result<Arc<Table>> {
		let collect_bptree = false;

		self.settle_manifest()?;

		// Step 1: Flush memtable to SST (with VLog separation for large values)
		let (table, _bptree_entries) = memtable
			.flush(
				table_id,
				Arc::clone(&self.opts),
				self.vlog.as_ref(),
				self.opts.vlog_value_threshold,
				collect_bptree,
			)
			.map_err(|e| {
				Error::Other(format!(
					"Failed to flush memtable to SST table_id={}: {}",
					table_id, e
				))
			})?;

		tracing::debug!("Created SST table_id={}, file_size={}", table.id, table.file_size);

		#[cfg(test)]
		{
			let hook = self.flush_hook.lock().clone();
			if let Some(hook) = hook {
				hook(table.id)?;
			}
		}

		// Step 2: Prepare atomic changeset
		let mut changeset = ManifestChangeSet::default();
		changeset.new_tables.push((0, Arc::clone(&table)));

		// Step 4: Apply changeset atomically
		// Lock order: level_manifest → immutable_memtables
		let mut manifest = self.level_manifest.write()?;
		let mut memtable_lock = self.immutable_memtables.write()?;

		// A memtable queued ahead of this one (a checkpoint can rotate while the shutdown flush
		// runs) keeps its WAL segment: `log_number` only moves forward, so passing the segment
		// would lose the memtable if the process died before its flush.
		let log_number = Self::hold_log_number(
			memtable_lock.log_number_after(Some(table_id), wal_number),
			manifest.get_log_number(),
			self.group_wal_pin.load(Ordering::Acquire).min(hold),
		);
		changeset.log_number = Some(log_number);

		tracing::debug!(
			"Changeset prepared: table_id={}, log_number={} (WAL #{:020} flushed)",
			table_id,
			log_number,
			wal_number
		);

		let rollback = manifest.apply_changeset(&changeset)?;
		if let Err(e) = write_manifest_to_disk(&manifest) {
			manifest.revert_changeset(rollback);
			return Err(self.manifest_write_error(
				e,
				format!(
					"Failed to atomically update manifest: table_id={}, log_number={}",
					table_id, log_number
				),
			));
		}

		// Remove successfully flushed memtable from immutables tracking. The
		// Arc<MemTable> in `memtable` (function parameter) falls out of scope at
		// end of function — its data is now available via the SST that was just
		// added to the manifest, and conflict detection uses the in-memory oracle
		// (independent of memtables), so dropping this Arc is safe.
		memtable_lock.remove(table_id);
		self.memory_controller.set_immutable_memtable_bytes(memtable_lock.total_size());
		drop(memtable);

		tracing::debug!(
			"Manifest updated atomically: table_id={}, log_number={}, last_sequence={}",
			table_id,
			log_number,
			manifest.get_last_sequence()
		);

		Ok(table)
	}

	/// Whether the manifest on disk may list a table the one in memory does not.
	pub(crate) fn manifest_uncertain(&self) -> bool {
		self.manifest_uncertain.load(Ordering::Acquire)
	}

	/// The error for a failed write of the manifest. An uncertain write keeps its kind, which
	/// the background task does not retry, and makes the next flush settle the manifest first.
	fn manifest_write_error(&self, error: Error, context: String) -> Error {
		match error {
			Error::ManifestWriteUncertain(_) => {
				self.manifest_uncertain.store(true, Ordering::Release);
				error
			}
			error => Error::Other(format!("{context}: {error}")),
		}
	}

	/// Writes the manifest in memory to disk again if the last write of it was uncertain, so that
	/// the disk holds exactly the tables in memory before a flush writes the file of one.
	pub(crate) fn settle_manifest(&self) -> Result<()> {
		if !self.manifest_uncertain.load(Ordering::Acquire) {
			return Ok(());
		}
		let manifest = self.level_manifest.write()?;
		write_manifest_to_disk(&manifest)
			.map_err(|e| self.manifest_write_error(e, "Failed to settle the manifest".into()))?;
		self.manifest_uncertain.store(false, Ordering::Release);
		Ok(())
	}

	/// Writes a batch directly to a new Level 0 SSTable without inserting into a memtable.
	///
	/// Used for batches that exceed `max_memtable_size` to avoid `ArenaFull` allocation
	/// failures and unnecessary in-memory buffering.
	///
	/// `batch_wal_number` is the WAL segment number that `batch` was durably appended to
	/// before this call (the caller must have already ensured that segment can never
	/// receive another write — see `seal_active_wal_segment`). It is used to safely
	/// advance the manifest's `log_number` so this WAL segment isn't replayed again on
	/// the next restart, without skipping any *other* not-yet-flushed data that also
	/// lives in that segment.
	///
	/// `rest_wal_number` is the oldest segment that holds the record of a batch of the same
	/// commit group that is not applied yet, `u64::MAX` if there is none: `log_number` is not
	/// moved past it either, see `group_wal_pin`.
	///
	/// A write that fails fails the commit, or its group, through the error of the group, and
	/// records no background error: the caller is a committer, which hears of it, like the one of
	/// a flush that a checkpoint runs. A group that failed after part of it was applied stops the
	/// database, which `flush_entries` decides. The manifest is written again first if the last
	/// write of it was uncertain, and a failure of that write fails this one before a table
	/// exists. The file of a failed write is removed, with one exception: a manifest write that
	/// failed from the rename on, whose manifest in memory could not be written again at once,
	/// may have been listed by the manifest on disk, and its file is kept until the manifest is
	/// written again or the database is opened. A failure leaves at most that one file, as
	/// nothing is created while the manifest cannot be written. The record of the batch stays in
	/// the segment that was sealed, so the commit that failed is whole or absent after a crash.
	///
	/// The values of `batch` are in files of the value log that the active memtable keeps
	/// (`note_vlog_pointers`) until it is flushed, and a flush writes the manifest from memory
	/// before it deletes any file, as the compaction does. A manifest on disk that lists a kept
	/// table is therefore replaced before the files that table points into can go.
	pub(crate) fn write_batch_direct_to_l0_sst(
		&self,
		batch: &Batch,
		table_id: u64,
		batch_wal_number: u64,
		rest_wal_number: u64,
	) -> Result<Arc<Table>> {
		self.settle_manifest()?;

		let mut progress = DirectL0Progress::default();
		let result = self.install_direct_l0_table(
			batch,
			table_id,
			batch_wal_number,
			rest_wal_number,
			&mut progress,
		);
		if result.is_err() && progress.created {
			if progress.table_may_be_listed {
				tracing::warn!(
					"Kept the table file of a failed direct L0 write that the manifest may list: table_id={}",
					table_id
				);
			} else if let Err(e) = std::fs::remove_file(self.opts.sstable_file_path(table_id)) {
				if e.kind() != std::io::ErrorKind::NotFound {
					tracing::warn!(
						"Failed to remove the table file of a failed direct L0 write: table_id={}: {}",
						table_id,
						e
					);
				}
			}
		}
		result
	}

	/// Writes the table of `write_batch_direct_to_l0_sst` and registers it in the manifest, and
	/// says in `progress` what a failure leaves behind.
	fn install_direct_l0_table(
		&self,
		batch: &Batch,
		table_id: u64,
		batch_wal_number: u64,
		rest_wal_number: u64,
		progress: &mut DirectL0Progress,
	) -> Result<Arc<Table>> {
		let table_file_path = self.opts.sstable_file_path(table_id);
		let mut range_deletions = Vec::new();
		let mut point_entries = Vec::new();

		// The values are borrowed from the batch, not copied: a batch that takes this path is
		// larger than a memtable, and a second copy of its values doubles what it needs.
		for (_, entry, seq_num, _) in batch.entries_with_seq_nums()? {
			let ikey = crate::InternalKey::new(entry.key.clone(), seq_num, entry.kind);
			let value = entry.value.as_deref().unwrap_or(&[]);
			if entry.kind == crate::InternalKeyKind::RangeDelete {
				range_deletions.push((entry.key.clone(), value.to_vec(), seq_num));
			}
			// Range-delete entries are ALSO added as a point entry (start_key ->
			// end_key), matching `MemTable::flush`'s behavior. Without this, the
			// table's `smallest_point`/`largest_point` bounds (which range-scan
			// pruning consults) would never cover the tombstone's start key, so a
			// scan could skip this table entirely and resurrect data the tombstone
			// was meant to hide.
			point_entries.push((ikey, value));
		}

		// Sort point entries by InternalKey comparator: user_key ASC, seq_num DESC
		point_entries
			.sort_by(|a, b| self.opts.internal_comparator.compare(&a.0.encode(), &b.0.encode()));

		{
			let file = std::fs::File::create(&table_file_path)?;
			progress.created = true;
			let mut table_writer =
				crate::sstable::table::TableWriter::new(file, table_id, Arc::clone(&self.opts), 0);

			for (key, val) in point_entries {
				table_writer.add(key, val)?;
			}

			for (start, end, seq) in &range_deletions {
				table_writer.add_range_deletion(start.clone(), end.clone(), *seq);
			}

			table_writer.finish()?;
		}

		if let Some(ref vlog) = self.vlog {
			vlog.sync()?;
		}

		crate::vfs::fsync_file(&table_file_path)?;
		crate::lsm::fsync_directory(self.opts.sstable_dir())?;
		let file: Arc<dyn crate::vfs::File> = Arc::new(std::fs::File::open(&table_file_path)?);
		let file_size = file.size()?;

		let created_table =
			Arc::new(Table::new(table_id, Arc::clone(&self.opts), file, file_size)?);
		if !range_deletions.is_empty() {
			created_table.range_deletions.write().extend(range_deletions);
			created_table.has_range_deletions.store(true, Ordering::Release);
		}

		#[cfg(test)]
		{
			let hook = self.direct_table_hook.lock().clone();
			if let Some(hook) = hook {
				hook(table_id)?;
			}
		}

		// Atomically commit new L0 table to manifest.
		let mut changeset = ManifestChangeSet::default();
		changeset.new_tables.push((0, Arc::clone(&created_table)));

		// Lock order: level_manifest -> immutable_memtables (matches flush_immutable_to_sst).
		let mut manifest = self.level_manifest.write()?;

		// Only advance `log_number` past `batch_wal_number` if no OLDER immutable
		// memtable is still waiting to be flushed. `flush_immutable_to_sst` can bump
		// `log_number` unconditionally because the immutable queue is always flushed
		// oldest-first (so anything older is already gone by the time it runs); this
		// path bypasses that queue entirely, so it must check explicitly. Getting this
		// wrong in the other direction — advancing `log_number` while older data is
		// still unflushed — would cause replay to silently skip that data after a
		// crash, which is a strictly worse outcome than the replay-again-on-restart
		// behavior this check exists to avoid.
		let immutable_memtables = self.immutable_memtables.read()?;
		let safe_to_advance_log_number =
			immutable_memtables.first().is_none_or(|entry| entry.wal_number > batch_wal_number);
		drop(immutable_memtables);
		if safe_to_advance_log_number {
			changeset.log_number = Some(Self::hold_log_number(
				batch_wal_number + 1,
				manifest.get_log_number(),
				rest_wal_number,
			));
		}

		let rollback = manifest.apply_changeset(&changeset)?;
		if let Err(e) = write_manifest_to_disk(&manifest) {
			manifest.revert_changeset(rollback);
			let error = self.manifest_write_error(
				e,
				format!(
					"Failed to update the manifest for the direct L0 table table_id={table_id}"
				),
			);
			drop(manifest);
			if matches!(error, Error::ManifestWriteUncertain(_)) {
				// The manifest on disk may list the table: write the one in memory again, which
				// does not. Until that works the file stays.
				progress.table_may_be_listed = self.settle_manifest().is_err();
			}
			return Err(error);
		}

		tracing::debug!(
			"Direct-to-L0 flush completed: table_id={}, file_size={}",
			created_table.id,
			created_table.file_size
		);

		Ok(created_table)
	}

	/// Rotates the active memtable to the immutable queue WITHOUT flushing to SST.
	/// This is a fast operation (no disk I/O) that:
	/// 1. Rotates WAL to a new file
	/// 2. Swaps active memtable with a fresh one
	/// 3. Adds old memtable to immutable queue
	///
	/// The actual SST flush happens asynchronously via background task.
	pub(crate) fn rotate_memtable(&self) -> Result<()> {
		// Step 1: Acquire WRITE lock upfront to prevent race conditions
		let active_memtable = self.active_memtable.write()?;

		if active_memtable.is_empty() {
			return Ok(());
		}

		self.rotate_memtable_locked(active_memtable)
	}

	/// Rotates the non-empty active memtable under the write guard the caller holds, which it
	/// keeps until the memtable is queued: whoever holds it sees the queue only shrink, see
	/// `try_release_wal_hold`.
	fn rotate_memtable_locked(
		&self,
		mut active_memtable: RwLockWriteGuard<'_, Arc<MemTable>>,
	) -> Result<()> {
		tracing::debug!("rotate_memtable: rotating memtable size={}", active_memtable.size());

		// Step 2: Rotate WAL while STILL holding memtable write lock
		let (flushed_wal_number, new_wal_number) = {
			let mut wal_guard = self.wal.write();
			let old_log_number = wal_guard.get_active_log_number();
			self.rotate_wal(&mut wal_guard).map_err(|e| {
				Error::Other(format!("Failed to rotate WAL before memtable rotation: {}", e))
			})?;
			let new_log_number = wal_guard.get_active_log_number();
			drop(wal_guard);

			tracing::debug!(
				"WAL rotated during memtable rotation: {} -> {}",
				old_log_number,
				new_log_number
			);
			(old_log_number, new_log_number)
		};

		// Step 3: Swap memtable while STILL holding write lock
		let flushed_memtable = std::mem::replace(
			&mut *active_memtable,
			Arc::new(MemTable::new(self.opts.max_memtable_size)),
		);

		// Set the WAL number on the new (empty) active memtable
		active_memtable.set_wal_number(new_wal_number);

		// LOCK ORDER: Get table_id from manifest BEFORE acquiring immutable_memtables lock.
		// This maintains consistent ordering: level_manifest -> immutable_memtables
		// which matches flush_immutable_to_sst() and prevents deadlock.
		//
		// Deadlock scenario prevented:
		//   Thread A (rotate): holds imm.write, waits manifest.read
		//   Thread B (flush):  holds manifest.write, waits imm.write
		// By acquiring manifest.read first, we ensure no circular wait.
		let table_id = self.level_manifest.read()?.next_table_id();
		let mut immutable_memtables = self.immutable_memtables.write()?;
		immutable_memtables.add(table_id, flushed_wal_number, Arc::clone(&flushed_memtable));

		// Update global memory accounting
		self.memory_controller.set_active_memtable_bytes(0);
		self.memory_controller.set_immutable_memtable_bytes(immutable_memtables.total_size());

		// Release locks
		drop(active_memtable);
		drop(immutable_memtables);

		tracing::debug!(
			"rotate_memtable: completed rotation, table_id={}, wal_number={}",
			table_id,
			flushed_wal_number
		);

		Ok(())
	}

	/// Rotates the WAL under the guard the caller holds, with the value log synced before the
	/// old segment is: its records point into the value log, and none may be durable ahead of its
	/// value. Nothing is fsynced if no value is unsynced.
	///
	/// The value log's locks are taken inside the WAL lock. Nothing that holds one of them waits
	/// for the WAL lock: the commit pipeline appends to the value log before it takes the WAL
	/// lock, and never holds both.
	fn rotate_wal(&self, wal: &mut Wal) -> wal::Result<u64> {
		wal.rotate_with(|| match self.vlog {
			Some(ref vlog) => {
				vlog.sync_dirty().map_err(|e| std::io::Error::other(e.to_string()).into())
			}
			None => Ok(()),
		})
	}

	/// Ensures the WAL segment currently backing the active memtable can never receive
	/// another write, and returns that WAL number.
	///
	/// Used before `write_batch_direct_to_l0_sst`, which advances `log_number` past the
	/// returned WAL number once the batch is durably captured in a new L0 table. That
	/// advance is only safe if no future write can land in the same WAL segment — this
	/// method establishes that precondition. When the active memtable is non-empty,
	/// `rotate_memtable` already does this as a side effect (it always rotates the WAL).
	/// When the active memtable is empty, `rotate_memtable` is a no-op by design (to
	/// avoid pointless WAL file churn), so this method rotates the WAL on its own instead.
	pub(crate) fn seal_active_wal_segment(&self) -> Result<u64> {
		// Hold the write lock for the whole check-and-rotate so no writer can land a
		// batch in the segment we're about to seal in between the emptiness check and
		// the rotation.
		let active_memtable = self.active_memtable.write()?;
		let sealed_wal_number = active_memtable.get_wal_number();

		if !active_memtable.is_empty() {
			drop(active_memtable);
			self.rotate_memtable()?;
			return Ok(sealed_wal_number);
		}

		let mut wal_guard = self.wal.write();
		self.rotate_wal(&mut wal_guard).map_err(|e| {
			Error::Other(format!("Failed to rotate WAL before direct-to-L0 flush: {}", e))
		})?;
		let new_wal_number = wal_guard.get_active_log_number();
		drop(wal_guard);
		active_memtable.set_wal_number(new_wal_number);

		Ok(sealed_wal_number)
	}

	/// Replaces the WAL segment if its writer refuses appends after a failed append or fsync, and
	/// returns whether it did. `Some(true)` means the active memtable was queued, tagged with the
	/// segment that failed; it is not flushed here.
	///
	/// The active memtable is held for the whole replacement, which excludes every other rotation.
	/// A memtable with data is rotated with the WAL. An empty one stays, and takes the tag of the
	/// new segment under the same guard, so a batch is still only applied to a memtable tagged
	/// with the segment of its record. A rotation that fails changes nothing: the next call tries
	/// again.
	///
	/// If the fsync of the segment failed, the new writer is held, see `Wal::rotate_with`.
	pub(crate) fn replace_failed_wal(&self) -> Result<Option<bool>> {
		let active_memtable = self.active_memtable.write()?;
		if !self.wal.read().needs_replacement() {
			return Ok(None);
		}

		let replace_error = |e: &dyn std::fmt::Display| {
			Error::Other(format!("Failed to replace the failed WAL segment: {e}"))
		};
		if active_memtable.is_empty() {
			let mut wal_guard = self.wal.write();
			self.rotate_wal(&mut wal_guard).map_err(|e| replace_error(&e))?;
			let new_wal_number = wal_guard.get_active_log_number();
			drop(wal_guard);
			active_memtable.set_wal_number(new_wal_number);
			return Ok(Some(false));
		}

		self.rotate_memtable_locked(active_memtable).map_err(|e| replace_error(&e))?;
		Ok(Some(true))
	}

	/// Ends the hold of the WAL writer once no memtable tagged with a segment below the hold's
	/// bound is queued, and returns whether the writer is free of it.
	///
	/// A rotation queues the memtable it retires while it holds the active memtable's write guard,
	/// which is held here too, so the queue can only shrink between the check and the release.
	pub(crate) fn try_release_wal_hold(&self) -> Result<bool> {
		let _active_memtable = self.active_memtable.write()?;
		let Some(below) = self.wal.read().hold_below() else {
			return Ok(true);
		};
		let pending = self.immutable_memtables.read()?.iter().any(|e| e.wal_number < below);
		if pending {
			return Ok(false);
		}
		self.wal.write().release_hold(below);
		Ok(true)
	}

	/// Flushes the oldest immutable memtable to an SSTable.
	/// Returns Ok(Some(table)) if a memtable was flushed, Ok(None) if queue was empty (or the
	/// oldest memtable was empty and was dropped).
	///
	/// This method:
	/// 1. Takes `flush_lock`, so a flush that is already running finishes first and its memtable is
	///    out of the queue when the oldest entry is read
	/// 2. Gets the oldest entry from immutable queue (lowest table_id)
	/// 3. Flushes it to SST via flush_immutable_to_sst (which also removes from queue)
	/// 4. Removes the WAL segments the flush made obsolete, on the caller's thread, so it needs no
	///    runtime
	/// 5. Deletes the VLog files that nothing points into any more
	///
	/// A failed flush leaves the memtable in the queue and releases the lock, so it can be retried.
	///
	/// Fails, flushing nothing, once a commit group stopped the database: see
	/// `BackgroundErrorHandler::commit_group_error`.
	fn flush_oldest_immutable_to_sst(&self) -> Result<Option<Arc<Table>>> {
		let _flush = self.flush_lock.lock();
		if let Some(error) = self.error_handler.commit_group_error() {
			return Err(error);
		}

		// Get the oldest immutable entry (clone to release lock before I/O)
		let entry = {
			let guard = self.immutable_memtables.read()?;
			guard.first().cloned()
		};

		let entry = match entry {
			Some(e) => e,
			None => {
				tracing::debug!("flush_oldest_immutable_to_sst: no immutables to flush");
				return Ok(None);
			}
		};

		// Skip empty memtables
		if entry.memtable.is_empty() {
			let mut guard = self.immutable_memtables.write()?;
			guard.remove(entry.table_id);
			tracing::debug!(
				"flush_oldest_immutable_to_sst: skipped empty memtable table_id={}",
				entry.table_id
			);
			return Ok(None);
		}

		tracing::debug!(
			"flush_oldest_immutable_to_sst: flushing table_id={}, wal_number={}",
			entry.table_id,
			entry.wal_number
		);

		// Flush to SST (this also removes from immutable queue and updates manifest)
		let table = self.flush_immutable_to_sst(
			Arc::clone(&entry.memtable),
			entry.table_id,
			entry.wal_number,
		)?;

		// Clean up the WAL segments the flush made obsolete
		let wal_dir = self.wal.read().get_dir_path().to_path_buf();
		let min_wal_to_keep = Self::hold_log_number(
			entry.wal_number + 1,
			self.level_manifest.read()?.get_log_number(),
			self.group_wal_pin.load(Ordering::Acquire),
		);

		match cleanup_old_segments(&wal_dir, min_wal_to_keep) {
			Ok(count) if count > 0 => {
				tracing::debug!(
					"Cleaned up {} old WAL segments (min_wal_to_keep={})",
					count,
					min_wal_to_keep
				);
			}
			Ok(_) => {}
			Err(e) => {
				tracing::warn!("Failed to clean up old WAL segments: {}", e);
			}
		}

		self.cleanup_vlog("flush");

		tracing::debug!(
			"flush_oldest_immutable_to_sst: flushed table_id={}, file_size={}",
			table.id,
			table.file_size
		);

		Ok(Some(table))
	}

	/// Flushes the immutable memtables that are queued when it is called, synchronously.
	/// Used by Tree::flush() and checkpoint for forced/sync flush.
	/// Blocks until they are all in tables, including one that another flush had started. Ones
	/// rotated meanwhile are left to the background task: waiting for them too would keep a
	/// checkpoint of a tree under steady writes from ever finishing.
	pub(crate) fn flush_all_immutables_sync(&self) -> Result<()> {
		let Some(newest) = self.immutable_memtables.read()?.iter().next_back().map(|e| e.table_id)
		else {
			return Ok(());
		};
		let mut count = 0;
		// `Ok(None)` can also mean an empty memtable was dropped, so the queue says when to stop.
		while self.immutable_memtables.read()?.first().is_some_and(|e| e.table_id <= newest) {
			if self.flush_oldest_immutable_to_sst()?.is_some() {
				count += 1;
			}
		}
		if count > 0 {
			tracing::debug!("flush_all_immutables_sync: flushed {} immutable memtables", count);
		}
		Ok(())
	}

	/// Flushes active memtable to SST and updates manifest log_number.
	///
	/// This is the core flush logic used by both shutdown and normal memtable
	/// rotation. Unlike `make_room_for_write`, this does NOT rotate the WAL -
	/// the caller is responsible for WAL rotation if needed.
	///
	/// # Arguments
	///
	/// - `flushed_wal_number`: Optional WAL number that was flushed. If provided, log_number will
	///   be set to `flushed_wal_number + 1`. If None, uses current active WAL number.
	///
	/// # Returns
	///
	/// - `Ok(Some(table))` if flush occurred successfully
	/// - `Ok(None)` if memtable was empty (nothing to flush)
	/// - `Err(_)` on failure
	///
	/// # Flush Process
	///
	/// The flush process follows these steps:
	/// 1. Memtable is swapped and marked as immutable
	/// 2. Immutable memtable is flushed to SST file
	/// 3. SST is added to Level 0
	/// 4. Manifest log_number is updated to mark flushed WALs
	/// 5. This marks all previous WALs as flushed
	fn flush_memtable_and_update_manifest(
		&self,
		flushed_wal_number: Option<u64>,
	) -> Result<Option<Arc<Table>>> {
		// Step 1: Atomically swap active memtable with a new empty one
		let mut active_memtable = self.active_memtable.write()?;

		// Don't flush an empty memtable
		if active_memtable.is_empty() {
			return Ok(None);
		}

		// LOCK ORDER: Get table_id from manifest BEFORE acquiring immutable_memtables lock.
		// This maintains consistent ordering: active -> level_manifest -> immutable_memtables
		// which matches flush_immutable_to_sst() and prevents deadlock with background flush.
		let table_id = self.level_manifest.read()?.next_table_id();

		let mut immutable_memtables = self.immutable_memtables.write()?;

		// Get the current WAL number for the new memtable
		let current_wal_number = self.wal.read().get_active_log_number();

		// Swap the active memtable with a new empty one
		// This allows writes to continue immediately
		let flushed_memtable = std::mem::replace(
			&mut *active_memtable,
			Arc::new(MemTable::new(self.opts.max_memtable_size)),
		);

		// Set the WAL number on the new active memtable
		active_memtable.set_wal_number(current_wal_number);

		// Get the WAL number from the memtable (set when it started receiving writes)
		// or use the current WAL if not set
		let memtable_wal_number = flushed_memtable.get_wal_number();

		// Track the immutable memtable until it's successfully flushed
		immutable_memtables.add(table_id, memtable_wal_number, Arc::clone(&flushed_memtable));

		// Release locks before the potentially slow flush operation
		drop(active_memtable);
		drop(immutable_memtables);

		// Step 2: Determine which WAL was flushed
		let wal_that_was_flushed = match flushed_wal_number {
			Some(num) => num,
			None => {
				// No explicit WAL provided, use the memtable's stored WAL number
				// This is the WAL that was active when the memtable started receiving writes
				memtable_wal_number
			}
		};

		// Step 3: Flush the immutable memtable to disk and update manifest
		let table = self.flush_immutable_to_sst(
			Arc::clone(&flushed_memtable),
			table_id,
			wal_that_was_flushed,
		)?;

		Ok(Some(table))
	}

	/// Flushes all memtables (immutable and active) during shutdown.
	///
	/// # Critical: Flush Ordering
	///
	/// Immutable memtables MUST be flushed BEFORE the active memtable to
	/// preserve SSTable ordering:
	/// - Immutable memtables contain OLDER data (were swapped out earlier)
	/// - Active memtable contains NEWEST data (currently receiving writes)
	/// - Table IDs must reflect temporal order (newer data = higher table_id)
	///
	/// # Flush Sequence
	///
	/// 1. Flush ALL immutable memtables FIRST using their pre-assigned table_ids
	/// 2. Flush active memtable LAST (gets new highest table_id)
	/// 3. Update manifest log_number to mark all WALs as flushed
	///
	/// # WAL Handling
	///
	/// This method does NOT rotate the WAL. The final log_number in manifest
	/// is set to current_wal + 1, indicating all data up to current WAL is
	/// persisted.
	fn flush_all_memtables_for_shutdown(&self) -> Result<()> {
		tracing::debug!("Flushing all memtables for shutdown...");

		// A checkpoint on another thread may still be flushing the same queue.
		let _flush = self.flush_lock.lock();

		// STEP 1: Flush ALL immutable memtables FIRST (older data, lower table_ids)
		// We need to collect them first to avoid holding the lock during I/O
		let immutables_to_flush: Vec<ImmutableEntry> = {
			let immutable_guard = self.immutable_memtables.read()?;
			immutable_guard.iter().cloned().collect()
		};

		let immutable_count = immutables_to_flush.len();
		if immutable_count > 0 {
			tracing::debug!(
				"Flushing {} immutable memtable(s) first (older data)",
				immutable_count
			);
		}

		// Flush each immutable memtable using its pre-assigned table_id and WAL number
		// These were assigned when the memtable was moved from active to immutable
		//
		// We use fail-fast because:
		// 1. Successfully flushed memtables already updated log_number (their WALs can be deleted)
		// 2. Failed memtable's WAL is preserved (its wal_number >= current log_number)
		// 3. On restart, WAL replay recovers all unflushed data
		let mut flushed_count = 0;

		for entry in immutables_to_flush {
			if entry.memtable.is_empty() {
				// Skip empty memtables - just remove from tracking
				let mut immutable_guard = self.immutable_memtables.write()?;
				immutable_guard.remove(entry.table_id);
				tracing::debug!("Skipped empty immutable memtable: table_id={}", entry.table_id);
				continue;
			}

			// Fail-fast: return immediately on error
			// WAL replay will recover this and subsequent memtables on restart
			self.flush_immutable_to_sst(
				Arc::clone(&entry.memtable),
				entry.table_id,
				entry.wal_number,
			)?;

			flushed_count += 1;
			tracing::debug!(
				"Flushed immutable memtable {}/{}: table_id={}, wal_number={}",
				flushed_count,
				immutable_count,
				entry.table_id,
				entry.wal_number
			);
		}

		if flushed_count > 0 {
			tracing::debug!("Flushed {} immutable memtable(s) successfully", flushed_count);
		}

		// STEP 2: Flush active memtable LAST (newest data, gets highest table_id)
		let active_memtable = self.active_memtable.read()?;
		let active_size = active_memtable.size();
		let active_is_empty = active_memtable.is_empty();
		drop(active_memtable);

		if !active_is_empty {
			tracing::debug!("Flushing active memtable last (newest data): size={}", active_size);

			// Use flush_memtable_and_update_manifest which:
			// - Gets a new (highest) table_id
			// - Updates log_number to mark WAL as flushed
			// - Does NOT rotate WAL (we pass None)
			// Fail-fast: return immediately on error
			match self.flush_memtable_and_update_manifest(None)? {
				Some(table) => {
					tracing::debug!(
						"Active memtable flushed: table_id={}, file_size={}",
						table.id,
						table.file_size
					);
				}
				None => {
					tracing::debug!("Active memtable was empty, skipped flush");
				}
			}
		} else {
			tracing::debug!("Active memtable is empty, skipping flush");

			// Even if active is empty, we should update log_number if we flushed immutables
			// This marks the WAL as safe to delete
			if flushed_count > 0 {
				let current_wal = self.wal.read().get_active_log_number();

				// A memtable rotated after the queue was read keeps its WAL segment.
				let mut manifest = self.level_manifest.write()?;
				let log_number =
					self.immutable_memtables.read()?.log_number_after(None, current_wal);
				let changeset = ManifestChangeSet {
					log_number: Some(log_number),
					..Default::default()
				};

				let rollback = manifest.apply_changeset(&changeset)?;
				if let Err(e) = write_manifest_to_disk(&manifest) {
					manifest.revert_changeset(rollback);
					return Err(Error::Other(format!(
						"Failed to update manifest log_number after immutable flush: {}",
						e
					)));
				}

				tracing::debug!(
					"Updated manifest log_number to {} after immutable flushes",
					log_number
				);
			}
		}

		tracing::debug!("All memtables flushed successfully for shutdown");
		Ok(())
	}

	/// Cleans up orphaned SST files not referenced in manifest
	/// Called during database startup to remove files from incomplete flushes
	///
	/// SAFETY: This is only safe because manifest updates are atomic.
	/// An SST file is orphaned if and only if the atomic manifest write
	/// (containing both SST addition and log_number update) never completed.
	/// In that case, the WAL is still alive and will replay the data.
	fn cleanup_orphaned_sst_files(&self) -> Result<()> {
		// Every SSTable on disk looks orphaned to a manifest that was just created, so
		// this would delete the whole database.
		if !self.manifest_loaded_from_disk {
			tracing::debug!("Manifest was created in this run, skipping orphaned SST cleanup");
			return Ok(());
		}

		let sstable_dir = self.opts.sstable_dir();

		if !sstable_dir.exists() {
			return Ok(());
		}

		// Get all table IDs from manifest
		let manifest = self.level_manifest.read()?;
		let live_tables = manifest.get_all_tables();
		let live_table_ids: HashSet<u64> = live_tables.keys().copied().collect();
		drop(manifest);

		// Scan SST directory for orphaned files
		let entries = std::fs::read_dir(&sstable_dir)?;
		let mut removed_count = 0;

		for entry in entries {
			let entry = entry?;
			let filename = entry.file_name();
			let filename_str = filename.to_string_lossy();

			// Parse table ID from filename (format: {id:020}.sst)
			if filename_str.ends_with(".sst") && filename_str.len() == 24 {
				if let Ok(table_id) = filename_str[..20].parse::<u64>() {
					// Delete if not in manifest
					if !live_table_ids.contains(&table_id) {
						let path = entry.path();
						match std::fs::remove_file(&path) {
							Ok(_) => {
								removed_count += 1;
								tracing::debug!("Removed orphaned SST file: table_id={}", table_id);
							}
							Err(e) => {
								tracing::warn!(
									"Failed to remove orphaned SST table_id={}: {}",
									table_id,
									e
								);
							}
						}
					}
				}
			}
		}

		if removed_count > 0 {
			tracing::debug!("Cleaned up {} orphaned SST files", removed_count);
		} else {
			tracing::debug!("No orphaned SST files found");
		}

		Ok(())
	}

	/// Deletes the VLog files that no table and no memtable points into, see [`cleanup_vlog`].
	///
	/// After a crash there may be files that no record points to (written, but their batch never
	/// logged), or that every table pointing into them was compacted away. At startup this runs
	/// after the manifest is loaded and the WAL is replayed, so the memtable rebuilt from the WAL
	/// accounts for the files its records point into.
	fn cleanup_vlog(&self, context: &str) {
		cleanup_vlog(
			&self.vlog,
			&self.active_memtable,
			&self.level_manifest,
			&self.immutable_memtables,
			context,
		);
	}

	/// Resolves a value, checking if it's a VLog pointer and retrieving from
	/// VLog if needed
	pub(crate) fn resolve_value(&self, value: &[u8]) -> Result<Value> {
		let location = ValueLocation::decode(value)?;
		location.resolve_value(self.vlog.as_ref())
	}
}

impl CompactionOperations for CoreInner {
	/// Flushes the oldest immutable memtable to SST.
	/// Called by background task. Returns Ok(()) even if nothing to flush.
	fn compact_memtable(&self) -> Result<()> {
		self.flush_oldest_immutable_to_sst().map(|_| ())
	}

	/// Performs compaction to merge SSTables and maintain read performance.
	///
	/// Compaction is crucial for LSM tree performance:
	/// - Merges overlapping SSTables to reduce read amplification
	/// - Removes deleted entries to reclaim space
	/// - Maintains the level invariants (size ratios and key ranges)
	fn compact(&self, strategy: Arc<dyn CompactionStrategy>) -> Result<()> {
		// A compaction keeps only the newest version of a key. If that is one that was never
		// published, it drops the version readers can see.
		if let Some(error) = self.error_handler.commit_group_error() {
			return Err(error);
		}

		// Create compaction options from the current LSM tree state
		let options = CompactionOptions::from(self);

		// Execute compaction according to the chosen strategy
		let compactor = Compactor::new(options, strategy);
		compactor.compact()?;

		// // Clean deleted versions from versioned index after compaction
		// self.clean_expired_versions()?;

		Ok(())
	}

	/// Returns a reference to the background error handler
	fn error_handler(&self) -> Arc<BackgroundErrorHandler> {
		Arc::clone(&self.error_handler)
	}

	fn has_pending_immutables(&self) -> bool {
		self.immutable_memtables.read().map(|guard| !guard.is_empty()).unwrap_or(false)
	}
}

impl WriteStallCountProvider for CoreInner {
	fn get_stall_counts(&self) -> StallCounts {
		StallCounts {
			immutable_memtables: self.immutable_count(),
			l0_files: self.l0_file_count(),
		}
	}
}

// ===== Core with Background Task Management =====
/// Wraps the LSM tree core with background task management.
///
/// LSM trees rely heavily on background operations:
/// - Memtable flushing: Converting full memtables to SSTables
/// - Compaction: Merging SSTables to maintain read performance
/// - Garbage collection: Removing obsolete SSTables
pub(crate) struct Core {
	/// The inner LSM tree implementation
	pub(crate) inner: Arc<CoreInner>,

	/// The commit pipeline for lock-free OCC transactions and group commit
	pub(crate) commit_pipeline: Arc<crate::ring::CommitPipeline>,

	/// Task manager for background operations (stored in Option so we can take
	/// it for shutdown)
	pub(crate) task_manager: Mutex<Option<Arc<TaskManager>>>,

	/// Write stall controller for backpressure management
	pub(crate) write_stall: Arc<crate::stall::WriteStallController>,

	/// Handle to the background flusher task
	pub(crate) flusher_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,

	/// Atomic flag indicating if the core has been closed
	pub(crate) is_closed: AtomicBool,

	/// Held while `close` runs, so that a second caller waits for the first to finish.
	closing: tokio::sync::Mutex<()>,

	/// Lets `close` wait for the checkpoints that are running and refuse the ones that follow. A
	/// checkpoint holds it for its whole run, before it takes any lock of `CoreInner`, and `close`
	/// waits on it holding nothing but `closing`.
	pub(crate) checkpoints: CheckpointGate,
}

impl std::ops::Deref for Core {
	type Target = CoreInner;

	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

impl Core {
	/// Replays WAL with configurable corruption handling.
	///
	/// Creates one memtable per WAL segment. Flushes all but the last memtable
	/// to SST via the provided callback. Returns the last memtable as active.
	///
	/// # Arguments
	/// * `wal_path` - Path to WAL directory
	/// * `min_wal_number` - Minimum WAL to replay
	/// * `context` - Context string for error messages
	/// * `recovery_mode` - How to handle corruption
	/// * `arena_size` - Size for memtable arenas
	/// * `flush_memtable` - Callback to flush intermediate memtables to SST. Its last argument is
	///   whether the next memtable holds more of the same WAL segment: the segment is then not
	///   flushed yet, and must not be recorded as such.
	///
	/// # Returns
	/// * `(Option<max_seq_num>, Option<active_memtable>, did_recovery)`
	pub(crate) fn replay_wal_with_repair<F>(
		wal_path: &Path,
		min_wal_number: u64,
		context: &str,
		recovery_mode: WalRecoveryMode,
		arena_size: usize,
		mut flush_memtable: F,
	) -> Result<(Option<u64>, Option<Arc<MemTable>>)>
	where
		F: FnMut(Arc<MemTable>, u64, bool) -> Result<()>,
	{
		// Replay WAL - returns memtables per segment
		let (wal_seq_num_opt, memtables) = match replay_wal(wal_path, min_wal_number, arena_size) {
			Ok(result) => result,
			Err(Error::WalCorruption {
				segment_id,
				offset,
				message,
			}) => {
				// Handle corruption based on recovery mode
				match recovery_mode {
					WalRecoveryMode::AbsoluteConsistency => {
						tracing::error!(
							"WAL corruption detected in segment {} at offset {}: {}. \
							AbsoluteConsistency mode: failing immediately without repair.",
							segment_id,
							offset,
							message
						);
						return Err(Error::WalCorruption {
							segment_id,
							offset,
							message,
						});
					}
					WalRecoveryMode::TolerateCorruptedWithRepair => {
						tracing::warn!(
							"Detected WAL corruption in segment {} at offset {}: {}. Attempting repair...",
							segment_id,
							offset,
							message
						);

						// Attempt repair
						if let Err(repair_err) = repair_corrupted_wal_segment(wal_path, segment_id)
						{
							tracing::error!("Failed to repair WAL segment: {repair_err}");
							return Err(Error::Other(format!(
								"{context} failed: WAL segment {segment_id} is corrupted and could not be repaired. {repair_err}"
							)));
						}

						// Retry after repair
						match replay_wal(wal_path, min_wal_number, arena_size) {
							Ok(result) => result,
							Err(Error::WalCorruption {
								segment_id: seg_id,
								offset: off,
								message,
							}) => {
								return Err(Error::Other(format!(
									"{context} failed: WAL segment {seg_id} still corrupted at offset {off} after repair: {message}"
								)));
							}
							Err(retry_err) => {
								return Err(Error::Other(format!(
									"{context} failed: WAL replay failed after repair. {retry_err}"
								)));
							}
						}
					}
				}
			}
			Err(e) => return Err(e),
		};

		// If no memtables, nothing was recovered
		if memtables.is_empty() {
			return Ok((None, None));
		}

		// Flush all memtables except the last to SST
		let memtable_count = memtables.len();
		if memtable_count > 1 {
			tracing::debug!(
				"Recovery: flushing {} intermediate memtables to SST",
				memtable_count - 1
			);
			for (at, (memtable, _, wal_number)) in
				memtables.iter().enumerate().take(memtable_count - 1)
			{
				if !memtable.is_empty() {
					// A segment that holds more than one memtable of records is replayed into
					// several. Until the last of them is flushed, the segment is still needed.
					let continues = memtables[at + 1].2 == *wal_number;
					flush_memtable(Arc::clone(memtable), *wal_number, continues)?;
				}
			}
		}

		// Return the last memtable as the active one — unless it's an oversized
		// memtable that `replay_wal` allocated specifically to absorb a single batch
		// that exceeded `arena_size` on its own (see the `ArenaFull` handling there).
		// Such a memtable must never become the live active memtable: its capacity has
		// nothing to do with `opts.max_memtable_size`, so it would silently blow
		// through the configured memory bound for however long it stays active. Flush
		// it immediately like the other recovered memtables instead.
		let (last_memtable, oversized, last_wal_number) = memtables.into_iter().last().unwrap();
		if oversized {
			tracing::debug!(
				"Recovery: last memtable (wal={}) is oversized (arena_capacity={} != {}), \
				flushing instead of activating",
				last_wal_number,
				last_memtable.arena_capacity(),
				arena_size
			);
			if !last_memtable.is_empty() {
				flush_memtable(last_memtable, last_wal_number, false)?;
			}
			return Ok((wal_seq_num_opt, None));
		}
		let entry_count = {
			let mut iter = last_memtable.iter();
			let mut count = 0;
			if iter.seek_first().unwrap_or(false) {
				count += 1;
				while iter.next().unwrap_or(false) {
					count += 1;
				}
			}
			count
		};
		tracing::debug!(
			"Recovery: setting last memtable (wal={}) as active with {} entries",
			last_wal_number,
			entry_count
		);

		Ok((wal_seq_num_opt, Some(last_memtable)))
	}

	/// Creates a new LSM tree with background task management
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		tracing::debug!("Initializing LSM tree at {:?}", opts.path);

		let inner = Arc::new(CoreInner::new(Arc::clone(&opts))?);

		// Create the write stall controller with the provider and thresholds
		let thresholds =
			StallThresholds::new(opts.memtable_stall_threshold, opts.l0_stall_threshold);
		let write_stall = Arc::new(crate::stall::WriteStallController::new(
			Arc::clone(&inner) as Arc<dyn WriteStallCountProvider>,
			thresholds,
		));

		// Initialize background task manager
		let task_manager = Arc::new(TaskManager::new(
			Arc::clone(&inner) as Arc<dyn CompactionOperations>,
			Arc::clone(&opts),
			Arc::clone(&write_stall),
		));

		// Path for the WAL directory
		let wal_path = opts.wal_dir();

		// Get min_wal_number from manifest to skip already-flushed WALs
		let min_wal_number = inner.level_manifest.read()?.get_log_number();
		let manifest_last_seq = inner.level_manifest.read()?.get_last_sequence();

		tracing::debug!(
			"Manifest state: log_number={}, last_sequence={}",
			min_wal_number,
			manifest_last_seq
		);

		// Replay WAL with configurable recovery mode (returns None if skipped/empty)
		let (wal_seq_num_opt, recovered_memtable) = Self::replay_wal_with_repair(
			&wal_path,
			min_wal_number,
			"Database startup",
			opts.wal_recovery_mode,
			opts.max_memtable_size,
			|memtable, wal_number, continues| {
				// Flush intermediate memtable to SST during recovery
				let table_id = inner.level_manifest.read()?.next_table_id();
				let hold = if continues {
					wal_number
				} else {
					u64::MAX
				};
				inner.flush_immutable_to_sst_holding(
					Arc::clone(&memtable),
					table_id,
					wal_number,
					hold,
				)?;
				tracing::debug!(
					"Recovery: flushed memtable to SST table_id={}, wal_number={}",
					table_id,
					wal_number
				);
				Ok(())
			},
		)?;

		// Always continue in a fresh segment: replay may have repaired (replaced) the
		// segment the writer was opened on, or flushed it and moved the manifest's
		// log number past it. Like every rotation it goes through `rotate_wal()`, which
		// syncs the value log first if it holds unsynced values.
		{
			let mut wal_guard = inner.wal.write();
			inner.rotate_wal(&mut wal_guard)?;
		}

		// Set recovered memtable as active (if any)
		if let Some(memtable) = recovered_memtable {
			let mut active_memtable = inner.active_memtable.write()?;
			*active_memtable = memtable;
		}

		// Ensure the active memtable has the correct WAL number set
		{
			let active_memtable = inner.active_memtable.read()?;
			let current_wal_number = inner.wal.read().get_active_log_number();
			active_memtable.set_wal_number(current_wal_number);
		}

		// Get last_sequence from manifest
		let manifest_last_seq = inner.level_manifest.read()?.get_last_sequence();

		// Determine effective sequence number:
		// - If WAL was replayed, use max(manifest, WAL)
		// - If WAL was skipped/empty, use manifest value
		let max_seq_num = match wal_seq_num_opt {
			Some(wal_seq) => {
				let effective = std::cmp::max(manifest_last_seq, wal_seq);
				tracing::debug!(
					"WAL replayed: manifest_last_seq={}, wal_seq={}, using max={}",
					manifest_last_seq,
					wal_seq,
					effective
				);
				effective
			}
			None => {
				tracing::debug!(
					"WAL skipped or empty, using manifest_last_seq={}",
					manifest_last_seq
				);
				manifest_last_seq
			}
		};

		// Set visible sequence number (in-memory, will be persisted on next flush)
		inner.visible_seq_num.store(max_seq_num, Ordering::Release);

		// Initialize the CommitPipeline for lock-free OCC commits and background flushing.
		let commit_pipeline = Arc::new(crate::ring::CommitPipeline::new(
			Arc::clone(&inner),
			Arc::clone(&write_stall),
			Some(Arc::clone(&task_manager)),
			max_seq_num + 1,
		));

		let flusher_handle = commit_pipeline.start_flusher();

		// Clean up any orphaned SST files from previous crashes
		// SAFETY: This must happen AFTER WAL replay so data is recovered
		// but BEFORE any new flushes that might create new SSTs
		inner.cleanup_orphaned_sst_files()?;

		// Clean up any orphaned VLog files that are no longer referenced by any SST
		// SAFETY: This must happen AFTER manifest is loaded and the WAL is replayed
		inner.cleanup_vlog("startup");

		// Trigger level compaction check at startup
		task_manager.wake_up_level();

		let core = Self {
			inner: Arc::clone(&inner),
			commit_pipeline,
			task_manager: Mutex::new(Some(task_manager)),
			write_stall,
			flusher_handle: Mutex::new(Some(flusher_handle)),
			is_closed: AtomicBool::new(false),
			closing: tokio::sync::Mutex::new(()),
			checkpoints: CheckpointGate::default(),
		};

		tracing::debug!("LSM tree initialization complete");

		Ok(core)
	}

	pub(crate) async fn commit(
		&self,
		batch: Batch,
		sync: bool,
		start_seq: u64,
		window: u64,
		read_set: &[Key],
	) -> Result<()> {
		// Commit the batch using the commit pipeline. `start_seq` is the
		// transaction's snapshot seq and `window` the start of its conflict
		// window in the commit ring. The write keys are derived from
		// `batch.entries` inside the pipeline — no duplicated parallel array.
		// `read_set` carries locked-read keys that join the conflict check
		// without being written.
		self.commit_pipeline.commit(batch, sync, start_seq, window, read_set).await
	}

	pub(crate) async fn check_conflicts(
		&self,
		keys: &[Key],
		start_seq: u64,
		window: u64,
	) -> Result<()> {
		// Validate locked-read keys against the commit ring without writing
		// anything. Used by transactions whose commit carries only locked
		// reads.
		self.commit_pipeline.check_conflicts(keys, start_seq, window).await
	}

	pub(crate) fn seq_num(&self) -> u64 {
		self.inner.visible_seq_num.load(Ordering::Acquire)
	}

	/// Flushes WAL and VLog buffers to OS cache.
	///
	/// If `sync` is true, also fsyncs to disk for durability.
	/// This is safe to call concurrently with ongoing transactions.
	///
	/// If the fsync fails, the WAL segment in use refuses every later append and sync: the
	/// kernel may have dropped the data it could not write, and a second fsync can report
	/// success for it. The next commit replaces the segment. The segment that replaces it is held
	/// until the memtables of the failed segment are in tables, and `flush_wal(true)` fails until
	/// then without touching the disk.
	///
	/// # Order of Operations
	///
	/// VLog is flushed (or synced) first (contains data referenced by WAL), then WAL.
	/// This ensures that if WAL contains a ValuePointer, the referenced
	/// VLog data is at least as durable.
	pub(crate) fn flush_wal(&self, sync: bool) -> Result<()> {
		if sync && self.wal.read().is_held() {
			return Err(wal::segment_held().into());
		}
		if let Some(ref vlog) = self.vlog {
			if sync {
				vlog.sync()?;
			} else {
				vlog.flush()?;
			}
		}
		if sync {
			self.wal.sync()?;
		} else {
			self.wal.flush()?;
		}
		Ok(())
	}

	/// Test-only: ends the flusher and the background tasks the way a crash does, so that a tree
	/// a test abandons does not stay alive until the runtime is dropped.
	#[cfg(all(test, not(target_arch = "wasm32")))]
	pub(crate) fn abort_background_tasks(&self) {
		if let Some(handle) = self.flusher_handle.lock().unwrap().take() {
			handle.abort();
		}
		if let Some(task_manager) = self.task_manager.lock().unwrap().take() {
			task_manager.abort();
		}
	}

	/// Safely closes the LSM tree by shutting down all components in the
	/// correct order. A call that arrives while another is closing waits for it.
	///
	/// # Shutdown Sequence
	///
	/// 0. Checkpoints - refuses new ones, and waits for those running, which flush memtables and
	///    copy files that the steps below close or release. Takes no thread, and a `close` that is
	///    dropped meanwhile can be called again.
	/// 1. Commit pipeline shutdown - refuses new commits, and waits until every commit already
	///    admitted has been decided
	/// 2. Background tasks stopped - waits for ongoing operations
	/// 3. Active memtable flush - if flush_on_close enabled AND memtable non-empty, flush to SST
	///    (NO WAL rotation)
	/// 4. WAL close - sync and close current WAL file
	/// 5. Directory sync - ensure all metadata is persisted
	/// 6. Lock release - allow other processes to open the database
	///
	/// # Critical: No Empty WAL Creation
	///
	/// Unlike `make_room_for_write`, this does NOT rotate the WAL before
	/// flushing. This prevents creating an empty WAL file on clean shutdown.
	pub async fn close(&self) -> Result<()> {
		let _closing = self.closing.lock().await;
		if self.is_closed.load(Ordering::SeqCst) {
			return Ok(());
		}

		// Step 0: Wait for the checkpoints that are running. Closing is marked after the wait,
		// so a `close` that is dropped during it leaves a tree that a second call closes.
		self.checkpoints.close().await;
		self.is_closed.store(true, Ordering::SeqCst);

		tracing::debug!("Shutting down LSM tree");

		// Step 1: Shutdown the commit pipeline to stop accepting new writes. The flusher exits
		// once every commit admitted before the shutdown is decided.
		self.commit_pipeline.shutdown();
		tracing::debug!("Commit pipeline shutdown complete");

		let handle = self.flusher_handle.lock().unwrap().take();
		if let Some(handle) = handle {
			let _ = handle.await;
		}

		// Step 2: Signal write stall controller - wake any stalled writers
		self.write_stall.signal_shutdown();
		tracing::debug!("Write stall shutdown signal sent");

		// Step 3: Wait for and stop all background tasks
		let task_manager = self.task_manager.lock().unwrap().take();
		if let Some(task_manager) = task_manager {
			tracing::debug!("Stopping background task manager...");
			task_manager.stop().await;
			tracing::debug!("Background task manager stopped");
		}

		// Close the VLog if present
		if let Some(ref vlog) = self.inner.vlog {
			tracing::debug!("Closing VLog...");
			vlog.close()?;
			tracing::debug!("VLog closed");
		}

		// Step 3: Conditionally flush ALL memtables based on flush_on_close option
		// CRITICAL ORDERING: Immutable memtables must be flushed BEFORE active memtable
		// to preserve SSTable ordering (older data = lower table_ids)
		// IMPORTANT: We do NOT rotate the WAL here to avoid creating an empty WAL file
		// A database a commit group stopped leaves its memtables to the WAL: a table made from
		// them would hold part of the group, and the segments that hold the rest would go.
		let stopped_by_commit_group = self.inner.error_handler.commit_group_error().is_some();
		if self.inner.opts.flush_on_close && !stopped_by_commit_group {
			tracing::debug!("Flushing all memtables on shutdown (flush_on_close=true)");

			// Flush ALL memtables: immutables first (older data), then active (newest data)
			self.inner.flush_all_memtables_for_shutdown().map_err(|e| {
				Error::Other(format!("Failed to flush memtables during shutdown: {}", e))
			})?;

			tracing::debug!("All memtables flushed successfully on shutdown");

			// The memtables of a failed segment are in tables now, so a held writer can be
			// closed. One that cannot be released reports the hold when the WAL is closed.
			let _ = self.inner.try_release_wal_hold();
		}

		// Step 4: Close the WAL to ensure all data is flushed
		// This is safe now because all background tasks that could write to WAL are
		// stopped NOTE: WAL must be closed BEFORE cleanup, otherwise cleanup may
		// delete the active WAL file
		let wal_log_number = self.inner.wal.read().get_active_log_number();
		tracing::debug!("Closing WAL: active_log_number={}", wal_log_number);

		// A WAL that cannot be closed (the fsync of its segment failed) does not keep the rest of
		// the shutdown from running: its error is returned at the end.
		let mut wal_guard = self.inner.wal.write();
		let wal_closed =
			wal_guard.close().map_err(|e| Error::Other(format!("Failed to close WAL: {}", e)));
		if wal_closed.is_ok() {
			tracing::debug!("WAL #{:020} closed and synced", wal_log_number);
		}
		drop(wal_guard);

		// Step 4.5: Clean up obsolete WAL files (synchronous cleanup)
		// This happens AFTER closing the WAL to prevent deleting the active WAL file.
		// When memtable flush sets log_number = current_wal + 1, cleanup would delete
		// the active WAL if done before closing it.
		let wal_dir = self.inner.wal.read().get_dir_path().to_path_buf();
		let min_wal_to_keep = self.inner.level_manifest.read()?.get_log_number();

		tracing::debug!("Cleaning up obsolete WAL files (min_wal_to_keep={})", min_wal_to_keep);

		match cleanup_old_segments(&wal_dir, min_wal_to_keep) {
			Ok(count) if count > 0 => {
				tracing::debug!("Cleaned up {} obsolete WAL files during shutdown", count);
			}
			Ok(_) => {
				tracing::debug!("No obsolete WAL files to clean up");
			}
			Err(e) => {
				tracing::warn!("Failed to clean up WAL files during shutdown: {}", e);
			}
		}

		// Step 5: Flush all directories to ensure durability
		tracing::debug!("Syncing directory structure...");
		sync_directory_structure(&self.inner.opts).map_err(|e| {
			Error::Other(format!("Failed to sync directories during shutdown: {}", e))
		})?;
		tracing::debug!("Directory sync complete");

		// Step 7: Release the database lock
		let mut lockfile = self.inner.lockfile.lock()?;
		lockfile.release()?;

		// Log final state
		let final_manifest = self.inner.level_manifest.read()?;
		tracing::debug!(
			"LSM tree shutdown complete: log_number={}, last_sequence={}",
			final_manifest.get_log_number(),
			final_manifest.get_last_sequence()
		);

		wal_closed
	}
}

#[derive(Clone)]
pub struct Tree {
	pub(crate) core: Arc<Core>,
}

impl Tree {
	/// Creates a new LSM tree with the specified options
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		// Validate options before creating the tree
		opts.validate()?;

		// Fail before anything is created or moved if the directory holds database files
		// but no usable manifest. This must stay ahead of the migrations, the directory
		// creation and the lock file.
		check_manifest_before_open(&opts)?;

		// If the path contains an existing RocksDB database, automatically migrate it in pure Rust
		if surrealkv_compat_rocksdb::is_rocksdb_dir(&opts.path) {
			Self::migrate_from_rocksdb(&opts)?;
		}

		// If the path contains an existing SurrealKV v1 database, automatically migrate it
		if surrealkv_compat_v1::is_v1_dir(&opts.path) {
			Self::migrate_from_v1(&opts)?;
		}

		// If the path contains an existing IndexedDB dump or store, automatically migrate it
		if surrealkv_compat_indxdb::is_indxdb_dump_dir(&opts.path) {
			Self::migrate_from_indxdb(&opts)?;
		}

		// Create all required directory structure
		Self::create_directory_structure(&opts)?;

		// Create the core LSM tree components
		let core = Core::new(Arc::clone(&opts))?;

		// Ensure directory changes are persisted
		sync_directory_structure(&opts)?;

		Ok(Self {
			core: Arc::new(core),
		})
	}

	/// Automatically migrates an existing RocksDB database to SurrealKV v2 format in pure Rust.
	fn migrate_from_rocksdb(opts: &Options) -> Result<()> {
		tracing::info!("Detected existing RocksDB database at {:?}. Starting automatic migration to SurrealKV v2...", opts.path);
		let records = surrealkv_compat_rocksdb::read_all_latest(&opts.path).map_err(|e| {
			Error::Other(format!("Failed to read RocksDB database for migration: {e}"))
		})?;

		tracing::info!(
			"Read {} live records from RocksDB. Creating backup and migrating...",
			records.len()
		);

		// Create backup directory
		let backup_dir = opts.path.join("_rocksdb_backup");
		create_dir_all(&backup_dir)?;

		// Move all RocksDB legacy files into backup
		if let Ok(entries) = std::fs::read_dir(&opts.path) {
			for entry in entries.flatten() {
				let p = entry.path();
				let name = entry.file_name().to_string_lossy().to_string();
				if name == "_rocksdb_backup" {
					continue;
				}
				// RocksDB files: CURRENT, MANIFEST-*, OPTIONS-*, IDENTITY, LOCK, *.sst, *.log,
				// *.dbtmp
				if name.starts_with("MANIFEST-")
					|| name.starts_with("OPTIONS-")
					|| name == "CURRENT"
					|| name == "IDENTITY"
					|| name == "LOCK"
					|| name.ends_with(".sst")
					|| name.ends_with(".log")
					|| name.ends_with(".dbtmp")
				{
					let dest = backup_dir.join(&name);
					let _ = std::fs::rename(&p, dest);
				}
			}
		}

		// Now initialize directory structure
		Self::create_directory_structure(opts)?;

		if !records.is_empty() {
			let table_id = 1;
			let sst_path = opts.sstable_file_path(table_id);
			let file = std::fs::File::create(&sst_path)?;
			let opts_arc = Arc::new(opts.clone());
			let mut writer =
				crate::sstable::table::TableWriter::new(file, table_id, Arc::clone(&opts_arc), 0);

			let mut last_seq = 0u64;
			for (k, v) in records {
				last_seq += 1;
				let ikey = crate::InternalKey::new(k, last_seq, crate::InternalKeyKind::Set);
				let val_encoded = crate::vlog::ValueLocation::with_inline_value(v).encode();
				writer.add(ikey, &val_encoded)?;
			}
			let file_size = writer.finish()? as u64;

			// Write manifest
			let file: Arc<dyn crate::vfs::File> = Arc::new(std::fs::File::open(&sst_path)?);
			let table = Arc::new(crate::sstable::table::Table::new(
				table_id,
				Arc::clone(&opts_arc),
				file,
				file_size,
			)?);

			let mut manifest = crate::levels::LevelManifest::new(Arc::clone(&opts_arc))?;
			manifest.next_table_id.store(table_id + 1, std::sync::atomic::Ordering::Release);
			manifest.last_sequence = last_seq;

			let mut changeset = crate::levels::ManifestChangeSet::default();
			changeset.new_tables.push((0, table));
			manifest.apply_changeset(&changeset)?;
			crate::levels::write_manifest_to_disk(&manifest)?;
		}

		tracing::info!("RocksDB to SurrealKV v2 automatic migration completed successfully!");
		Ok(())
	}

	/// Automatically migrates an existing SurrealKV v1 database to SurrealKV v2 format.
	fn migrate_from_v1(opts: &Options) -> Result<()> {
		tracing::info!("Detected existing SurrealKV v1 database at {:?}. Starting automatic migration to SurrealKV v2...", opts.path);
		let records = surrealkv_compat_v1::read_all_latest(&opts.path).map_err(|e| {
			Error::Other(format!("Failed to read SurrealKV v1 database for migration: {e}"))
		})?;

		tracing::info!(
			"Read {} live records from SurrealKV v1. Creating backup and migrating...",
			records.len()
		);

		// Create backup directory
		let backup_dir = opts.path.join("_v1_backup");
		create_dir_all(&backup_dir)?;

		// Move all v1 legacy files into backup
		if let Ok(entries) = std::fs::read_dir(&opts.path) {
			for entry in entries.flatten() {
				let p = entry.path();
				let name = entry.file_name().to_string_lossy().to_string();
				if name == "_v1_backup" {
					continue;
				}
				if name.ends_with(".sst")
					|| name.ends_with(".wal")
					|| name.starts_with("MANIFEST")
					|| name == "LOCK"
				{
					let dest = backup_dir.join(&name);
					let _ = std::fs::rename(&p, dest);
				}
			}
		}

		// Now initialize directory structure
		Self::create_directory_structure(opts)?;

		if !records.is_empty() {
			let table_id = 1;
			let sst_path = opts.sstable_file_path(table_id);
			let file = std::fs::File::create(&sst_path)?;
			let opts_arc = Arc::new(opts.clone());
			let mut writer =
				crate::sstable::table::TableWriter::new(file, table_id, Arc::clone(&opts_arc), 0);

			let mut last_seq = 0u64;
			for (k, v) in records {
				last_seq += 1;
				let ikey = crate::InternalKey::new(k, last_seq, crate::InternalKeyKind::Set);
				let val_encoded = crate::vlog::ValueLocation::with_inline_value(v).encode();
				writer.add(ikey, &val_encoded)?;
			}
			let file_size = writer.finish()? as u64;

			// Write manifest
			let file: Arc<dyn crate::vfs::File> = Arc::new(std::fs::File::open(&sst_path)?);
			let table = Arc::new(crate::sstable::table::Table::new(
				table_id,
				Arc::clone(&opts_arc),
				file,
				file_size,
			)?);

			let mut manifest = crate::levels::LevelManifest::new(Arc::clone(&opts_arc))?;
			manifest.next_table_id.store(table_id + 1, std::sync::atomic::Ordering::Release);
			manifest.last_sequence = last_seq;

			let mut changeset = crate::levels::ManifestChangeSet::default();
			changeset.new_tables.push((0, table));
			manifest.apply_changeset(&changeset)?;
			crate::levels::write_manifest_to_disk(&manifest)?;
		}

		tracing::info!("SurrealKV v1 to SurrealKV v2 automatic migration completed successfully!");
		Ok(())
	}

	/// Automatically migrates an existing IndexedDB dump to SurrealKV v2 format.
	fn migrate_from_indxdb(opts: &Options) -> Result<()> {
		tracing::info!("Detected existing IndexedDB dump at {:?}. Starting automatic migration to SurrealKV v2...", opts.path);
		let dump_file = if opts.path.is_dir() {
			opts.path.join("indxdb_dump.bin")
		} else {
			opts.path.clone()
		};

		let records = surrealkv_compat_indxdb::read_dump(&dump_file).map_err(|e| {
			Error::Other(format!("Failed to read IndexedDB dump for migration: {e}"))
		})?;

		tracing::info!(
			"Read {} live records from IndexedDB dump. Creating backup and migrating...",
			records.len()
		);

		// Create backup directory
		let backup_dir = opts.path.join("_indxdb_backup");
		create_dir_all(&backup_dir)?;

		if dump_file.exists() {
			let dest = backup_dir.join("indxdb_dump.bin");
			let _ = std::fs::rename(&dump_file, dest);
		}

		// Now initialize directory structure
		Self::create_directory_structure(opts)?;

		if !records.is_empty() {
			let table_id = 1;
			let sst_path = opts.sstable_file_path(table_id);
			let file = std::fs::File::create(&sst_path)?;
			let opts_arc = Arc::new(opts.clone());
			let mut writer =
				crate::sstable::table::TableWriter::new(file, table_id, Arc::clone(&opts_arc), 0);

			let mut last_seq = 0u64;
			for (k, v) in records {
				last_seq += 1;
				let ikey = crate::InternalKey::new(k, last_seq, crate::InternalKeyKind::Set);
				let val_encoded = crate::vlog::ValueLocation::with_inline_value(v).encode();
				writer.add(ikey, &val_encoded)?;
			}
			let file_size = writer.finish()? as u64;

			// Write manifest
			let file: Arc<dyn crate::vfs::File> = Arc::new(std::fs::File::open(&sst_path)?);
			let table = Arc::new(crate::sstable::table::Table::new(
				table_id,
				Arc::clone(&opts_arc),
				file,
				file_size,
			)?);

			let mut manifest = crate::levels::LevelManifest::new(Arc::clone(&opts_arc))?;
			manifest.next_table_id.store(table_id + 1, std::sync::atomic::Ordering::Release);
			manifest.last_sequence = last_seq;

			let mut changeset = crate::levels::ManifestChangeSet::default();
			changeset.new_tables.push((0, table));
			manifest.apply_changeset(&changeset)?;
			crate::levels::write_manifest_to_disk(&manifest)?;
		}

		tracing::info!("IndexedDB to SurrealKV v2 automatic migration completed successfully!");
		Ok(())
	}

	/// Creates all required directory structure for the LSM tree
	fn create_directory_structure(opts: &Options) -> Result<()> {
		// Create base directory
		create_dir_all(&opts.path)?;

		// Create all subdirectories
		create_dir_all(opts.sstable_dir())?;
		create_dir_all(opts.wal_dir())?;
		create_dir_all(opts.manifest_dir())?;

		// Create VLog directories
		if opts.enable_vlog {
			create_dir_all(opts.vlog_dir())?;
		}

		Ok(())
	}

	/// Transactions provide a consistent, atomic view of the database.
	pub fn begin(&self) -> Result<Transaction> {
		self.begin_with_opts(TransactionOptions::new())
	}

	/// Begins a new transaction with the specified mode
	pub fn begin_with_mode(&self, mode: Mode) -> Result<Transaction> {
		self.begin_with_opts(TransactionOptions::new_with_mode(mode))
	}

	/// Begins a new transaction with the provided options
	pub fn begin_with_opts(&self, opts: TransactionOptions) -> Result<Transaction> {
		let txn = Transaction::new(Arc::clone(&self.core), opts)?;
		Ok(txn)
	}

	/// Executes a read-only operation in a consistent snapshot.
	///
	/// This provides a consistent view of the database without blocking writes.
	pub fn view(&self, f: impl FnOnce(&mut Transaction) -> Result<()>) -> Result<()> {
		let mut txn = self.begin_with_mode(Mode::ReadOnly)?;
		f(&mut txn)?;
		Ok(())
	}

	/// Creates a database checkpoint at the specified directory.
	///
	/// This creates a consistent point-in-time snapshot that includes:
	/// - All SSTables from all levels
	/// - Current WAL segments
	/// - Level manifest
	/// - Checkpoint metadata
	///
	/// # Arguments
	/// * `checkpoint_dir` - Directory where the checkpoint will be created. It may exist, holding
	///   an earlier checkpoint for instance. It must not be the database directory, lie inside it
	///   or contain it, however it is spelled (links and relative paths are resolved), or the call
	///   fails with [`Error::InvalidArgument`] before anything is changed.
	///
	/// # Returns
	/// Metadata about the created checkpoint
	///
	/// # Closing
	/// [`Tree::close`] waits for the checkpoints that are running. One that begins after `close`
	/// has fails without touching `checkpoint_dir`.
	pub fn create_checkpoint<P: AsRef<Path>>(
		&self,
		checkpoint_dir: P,
	) -> Result<CheckpointMetadata> {
		let _running = self.core.checkpoints.enter()?;
		let checkpoint = DatabaseCheckpoint::new(Arc::clone(&self.core.inner));
		let result = checkpoint.create_checkpoint(checkpoint_dir);

		// The checkpoint flushed memtables on this thread, which the background tasks did not
		// see: the level-0 tables it added may need compacting, and writers may be waiting for
		// either.
		if let Some(task_manager) = self.core.task_manager.lock().unwrap().as_ref() {
			task_manager.wake_up_level();
		}
		self.core.write_stall.signal_work_done();

		result
	}

	/// Restores the database from a checkpoint directory.
	pub fn restore_from_checkpoint<P: AsRef<Path>>(
		&self,
		checkpoint_dir: P,
	) -> Result<CheckpointMetadata> {
		// Block new commits from entering the critical section for the duration
		// of the restore. The restore is a multi-step rewrite of nearly all
		// in-memory state (manifest, memtables, WAL, seq counters, oracle); a
		// concurrent commit racing through any one of those steps would observe
		// torn state. In-flight commits already past `write_mutex` (in their
		// apply phase) will finish against the soon-to-be-replaced memtable —
		// their data is intentionally discarded by the restore.
		let _write_guard = self.core.commit_pipeline.lock_writes();

		// A flush in flight would add its table to the manifest this replaces.
		let _flush = self.core.inner.flush_lock.lock();

		// Step 1: Restore files from checkpoint
		let checkpoint = DatabaseCheckpoint::new(Arc::clone(&self.core.inner));
		let metadata = checkpoint.restore_from_checkpoint(checkpoint_dir)?;

		// Step 2: Reload in-memory state to match restored files

		// Create a new LevelManifest from the current path
		let new_levels = LevelManifest::new(Arc::clone(&self.core.inner.opts))?;

		// Replace the current levels with the reloaded ones
		{
			let mut levels_guard = self.core.inner.level_manifest.write()?;
			*levels_guard = new_levels;
		}

		// Clear the current memtables since they would be stale after restore
		// This discards any pending writes, which is correct for restore operations
		{
			let mut active_memtable = self.core.inner.active_memtable.write()?;
			*active_memtable = Arc::new(MemTable::new(self.core.inner.opts.max_memtable_size));
		}

		{
			let mut immutable_memtables = self.core.inner.immutable_memtables.write()?;
			*immutable_memtables = ImmutableMemtables::default();
		}

		// Reopen the WAL from the restored directory
		let wal_path = self.core.inner.opts.path.join("wal");
		let manifest_log_number = self.core.inner.level_manifest.read()?.get_log_number();

		{
			let mut wal_guard = self.core.inner.wal.write();
			let new_wal = Wal::open_with_min_log_number(
				&wal_path,
				manifest_log_number,
				wal::Options::default(),
			)?;
			*wal_guard = new_wal;
		}

		// Replay any WAL entries that were restored
		let (wal_seq_num_opt, recovered_memtable) = Core::replay_wal_with_repair(
			&wal_path,
			manifest_log_number,
			"Database restore",
			self.core.inner.opts.wal_recovery_mode,
			self.core.inner.opts.max_memtable_size,
			|memtable, wal_number, continues| {
				// Flush intermediate memtable to SST during recovery
				let table_id = self.core.inner.level_manifest.read()?.next_table_id();
				let hold = if continues {
					wal_number
				} else {
					u64::MAX
				};
				self.core.inner.flush_immutable_to_sst_holding(
					Arc::clone(&memtable),
					table_id,
					wal_number,
					hold,
				)?;
				tracing::debug!(
					"Restore: flushed memtable to SST table_id={}, wal_number={}",
					table_id,
					wal_number
				);
				Ok(())
			},
		)?;

		// Always continue in a fresh segment: replay may have repaired (replaced) the
		// segment the writer was opened on, or flushed it and moved the manifest's
		// log number past it. Like every rotation it goes through `rotate_wal()`, which
		// syncs the value log first if it holds unsynced values.
		{
			let mut wal_guard = self.core.inner.wal.write();
			self.core.inner.rotate_wal(&mut wal_guard)?;
		}

		// Set recovered memtable as active (if any)
		if let Some(memtable) = recovered_memtable {
			let mut active_memtable = self.core.inner.active_memtable.write()?;
			*active_memtable = memtable;
		}

		// Ensure the active memtable has the correct WAL number set
		{
			let active_memtable = self.core.inner.active_memtable.read()?;
			let current_wal_number = self.core.inner.wal.read().get_active_log_number();
			active_memtable.set_wal_number(current_wal_number);
		}

		// Get last_sequence from manifest
		let manifest_last_seq = self.core.inner.level_manifest.read()?.get_last_sequence();

		// Determine effective sequence number (same logic as Core::new)
		let max_seq_num = match wal_seq_num_opt {
			Some(wal_seq) => std::cmp::max(manifest_last_seq, wal_seq),
			None => manifest_last_seq,
		};

		// Set visible sequence number AND reset the oracle. The live process
		// may have accumulated oracle entries from pre-restore commits whose
		// seqs are now ghosts of a future that no longer exists; clearing them
		// prevents false write-write conflicts for new post-restore txns.
		self.core.commit_pipeline.reset_for_restore(max_seq_num);

		Ok(metadata)
	}

	/// Closes the tree. Commits already admitted finish first, each with its real outcome, and a
	/// commit that begins once the commit pipeline is shut down fails with
	/// [`Error::PipelineStall`]. Checkpoints that are running finish first, before the pipeline is
	/// shut down, and one that begins after the close fails.
	pub async fn close(&self) -> Result<()> {
		self.core.close().await
	}

	/// Flushes all memtables to disk synchronously.
	/// This is a blocking operation that ensures all data is persisted before returning.
	#[cfg(test)]
	pub(crate) fn flush(&self) -> Result<()> {
		// Step 1: Rotate active memtable if it has data
		{
			let active = self.core.inner.active_memtable.read()?;
			if !active.is_empty() {
				drop(active); // Release read lock before acquiring write lock
				self.core.inner.rotate_memtable()?;
			}
		}

		// Step 2: Flush all immutable memtables synchronously
		self.core.inner.flush_all_immutables_sync()?;

		// Step 3: Signal stall controller that work completed
		self.core.write_stall.signal_work_done();

		Ok(())
	}

	#[cfg(test)]
	pub(crate) fn compact(&self, strategy: Arc<dyn CompactionStrategy>) -> Result<()> {
		self.core.inner.compact(strategy)?;
		self.core.write_stall.signal_work_done();
		Ok(())
	}

	/// Flushes WAL and VLog buffers to OS cache.
	///
	/// If `sync` is true, also fsyncs to disk, guaranteeing durability
	/// of all previously committed transactions.
	///
	/// If `sync` is false, only flushes to OS buffer cache (faster but
	/// not durable across power loss).
	///
	/// If the fsync fails, the WAL segment in use refuses every later write and sync, because
	/// the kernel may have dropped the data it could not write and a second fsync can report
	/// success for it. Commits fail until the WAL rotates or the database is reopened.
	///
	/// This is safe to call concurrently with ongoing transactions.
	pub fn flush_wal(&self, sync: bool) -> Result<()> {
		self.core.flush_wal(sync)
	}
}

impl Drop for Tree {
	fn drop(&mut self) {
		#[cfg(not(target_arch = "wasm32"))]
		{
			// Only attempt async shutdown if the core is not already closed
			if !self.core.is_closed.load(Ordering::SeqCst) {
				if let Ok(handle) = tokio::runtime::Handle::try_current() {
					// Clone the Arc to move into the async task
					let core = Arc::clone(&self.core);
					handle.spawn(async move {
						if let Err(err) = core.close().await {
							tracing::error!("Error closing store: {}", err);
						}
					});
				} else {
					tracing::warn!("No runtime available for closing the store correctly");
				}
			}
		}
	}
}

/// A builder for creating LSM trees with type-safe configuration.
pub struct TreeBuilder {
	opts: Options,
}

impl TreeBuilder {
	/// Creates a new TreeBuilder with default options for the specified key
	/// type.
	pub fn new() -> Self {
		Self {
			opts: Options::default(),
		}
	}

	/// Creates a new TreeBuilder with the specified options.
	///
	/// This method ensures type safety by requiring the options to use the same
	/// key type.
	pub fn with_options(opts: Options) -> Self {
		Self {
			opts,
		}
	}

	/// Sets the database path.
	pub fn with_path(mut self, path: std::path::PathBuf) -> Self {
		self.opts = self.opts.with_path(path);
		self
	}

	/// Sets the block size.
	pub fn with_block_size(mut self, size: usize) -> Self {
		self.opts = self.opts.with_block_size(size);
		self
	}

	/// Sets the block restart interval.
	pub fn with_block_restart_interval(mut self, interval: usize) -> Self {
		self.opts = self.opts.with_block_restart_interval(interval);
		self
	}

	/// Sets the filter policy.
	pub fn with_filter_policy(mut self, policy: Option<Arc<dyn FilterPolicy>>) -> Self {
		self.opts = self.opts.with_filter_policy(policy);
		self
	}

	/// Sets the comparator.
	pub fn with_comparator(mut self, comparator: Arc<dyn Comparator>) -> Self {
		self.opts = self.opts.with_comparator(comparator);
		self
	}

	/// Disables compression for data blocks in SSTables.
	///
	/// Use this when compression overhead is not desired or when
	/// data is already compressed at the application level.
	///
	/// # Example
	///
	/// ```no_run
	/// use surrealkv::TreeBuilder;
	///
	/// let tree = TreeBuilder::new()
	///     .with_path("./data".into())
	///     .without_compression()
	///     .build()
	///     .unwrap();
	/// ```
	pub fn without_compression(mut self) -> Self {
		self.opts = self.opts.without_compression();
		self
	}

	/// Sets the number of levels.
	pub fn with_level_count(mut self, count: u8) -> Self {
		self.opts = self.opts.with_level_count(count);
		self
	}

	/// Sets the maximum memtable size.
	pub fn with_max_memtable_size(mut self, size: usize) -> Self {
		self.opts = self.opts.with_max_memtable_size(size);
		self
	}

	/// Sets the unified block cache capacity (includes data blocks, index
	/// blocks, and VLog values).
	pub fn with_block_cache_capacity(mut self, capacity_bytes: u64) -> Self {
		self.opts = self.opts.with_block_cache_capacity(capacity_bytes);
		self
	}

	/// Sets the index partition size.
	pub fn with_index_partition_size(mut self, size: usize) -> Self {
		self.opts = self.opts.with_index_partition_size(size);
		self
	}

	/// Sets the VLog maximum file size.
	pub fn with_vlog_max_file_size(mut self, size: u64) -> Self {
		self.opts = self.opts.with_vlog_max_file_size(size);
		self
	}

	/// Sets the VLog checksum verification level.
	pub fn with_vlog_checksum_verification(mut self, level: VLogChecksumLevel) -> Self {
		self.opts = self.opts.with_vlog_checksum_verification(level);
		self
	}

	/// Enables or disables VLog.
	pub fn with_enable_vlog(mut self, enable: bool) -> Self {
		self.opts = self.opts.with_enable_vlog(enable);
		self
	}

	/// Sets the VLog value threshold in bytes.
	///
	/// Values smaller than this threshold are stored inline in SSTables.
	/// Values larger than or equal to this threshold are stored in VLog files.
	///
	/// Default: 1024 (1KB)
	///
	/// # Example
	///
	/// ```no_run
	/// use surrealkv::TreeBuilder;
	///
	/// let tree = TreeBuilder::new()
	///     .with_path("./data".into())
	///     .with_enable_vlog(true)
	///     .with_vlog_value_threshold(8192) // 8KB threshold
	///     .build()
	///     .unwrap();
	/// ```
	pub fn with_vlog_value_threshold(mut self, value: usize) -> Self {
		self.opts = self.opts.with_vlog_value_threshold(value);
		self
	}

	/// Enables or disables versioned queries with timestamp tracking
	pub fn with_versioning(mut self, enable: bool, retention_ns: u64) -> Self {
		self.opts = self.opts.with_versioning(enable, retention_ns);
		self
	}

	/// Enables or disables the B+tree versioned index for timestamp-based queries.
	/// When disabled, versioned queries will scan the LSM tree directly.
	/// Requires `with_versioning` to be called first with `enable = true`.
	pub fn with_versioned_index(mut self, enable: bool) -> Self {
		self.opts = self.opts.with_versioned_index(enable);
		self
	}

	/// Controls whether to flush the active memtable during database shutdown.
	pub fn with_flush_on_close(mut self, value: bool) -> Self {
		self.opts = self.opts.with_flush_on_close(value);
		self
	}

	/// Set the memtable stall threshold.
	pub fn with_memtable_stall_threshold(mut self, value: usize) -> Self {
		self.opts = self.opts.with_memtable_stall_threshold(value);
		self
	}

	/// Set the L0 stall threshold.
	pub fn with_l0_stall_threshold(mut self, value: usize) -> Self {
		self.opts = self.opts.with_l0_stall_threshold(value);
		self
	}

	/// Builds the LSM tree with the configured options.
	///
	/// This method ensures type safety by using the same key type K
	/// for both the builder and the resulting tree.
	pub fn build(self) -> Result<Tree> {
		Tree::new(Arc::new(self.opts))
	}

	/// Builds the LSM tree and returns both the tree and the options.
	///
	/// This is useful when you need to keep a reference to the options
	/// after creating the tree.
	pub fn build_with_options(self) -> Result<(Tree, Arc<Options>)> {
		let opts = Arc::new(self.opts);
		let tree = Tree::new(Arc::clone(&opts))?;
		Ok((tree, opts))
	}
}

impl Default for TreeBuilder {
	fn default() -> Self {
		Self::new()
	}
}

/// Test-only: the directories `fsync_directory` has synced, canonicalized, in order.
#[cfg(test)]
pub(crate) static SYNCED_DIRECTORIES: parking_lot::Mutex<Vec<std::path::PathBuf>> =
	parking_lot::Mutex::new(Vec::new());

/// Syncs a directory to ensure all changes are persisted to disk
pub(crate) fn fsync_directory<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
	let path = path.as_ref();

	// Check if the directory still exists before trying to sync it
	if !path.exists() {
		return Ok(());
	}

	// On Windows and WASM/WASI, calling sync_all() on a directory handle is
	// not supported by the underlying host/runtime.
	#[cfg(not(any(target_os = "windows", target_family = "wasm")))]
	{
		let file = File::open(path)?;
		debug_assert!(file.metadata()?.is_dir());
		file.sync_all()?;
	}

	#[cfg(test)]
	SYNCED_DIRECTORIES.lock().push(path.canonicalize()?);

	Ok(())
}

/// Syncs all directory structures for the LSM store to ensure durability
/// Returns explicit errors indicating which path failed
fn sync_directory_structure(opts: &Options) -> Result<()> {
	// Sync all subdirectories with explicit error handling
	fsync_directory(opts.sstable_dir()).map_err(|e| {
		Error::Other(format!(
			"Failed to sync SSTable directory '{}': {}",
			opts.sstable_dir().display(),
			e
		))
	})?;

	fsync_directory(opts.wal_dir()).map_err(|e| {
		Error::Other(format!("Failed to sync WAL directory '{}': {}", opts.wal_dir().display(), e))
	})?;

	fsync_directory(opts.manifest_dir()).map_err(|e| {
		Error::Other(format!(
			"Failed to sync manifest directory '{}': {}",
			opts.manifest_dir().display(),
			e
		))
	})?;

	// Sync VLog directories
	if opts.enable_vlog {
		fsync_directory(opts.vlog_dir()).map_err(|e| {
			Error::Other(format!(
				"Failed to sync vlog directory '{}': {}",
				opts.vlog_dir().display(),
				e
			))
		})?;
	}

	fsync_directory(&opts.path).map_err(|e| {
		Error::Other(format!("Failed to sync base directory '{}': {}", opts.path.display(), e))
	})?;

	Ok(())
}

// ===== VLog and Versioned Index Cleanup Helpers =====

/// The bound of a VLog cleanup: the smallest file id that a table or a memtable that is not
/// flushed yet points into. A file below it holds nothing that can be read any more. It is 0,
/// and nothing is deleted, while no table points into the VLog.
///
/// The active slot is read-locked first and stays locked until the end, then the manifest and the
/// queue, together, in the order a flush takes them in. The slot is read last: a batch applied
/// before a table was registered is counted, and a rotation, which needs the slot exclusively,
/// cannot move the memtable between the queue and the slot while the bound is read. A flush
/// registers its table and takes the memtable out of the queue under both locks, so the memtable
/// of a flush that is under way stays counted until its table is in the manifest.
///
/// The files of a batch that is encoded but not in a memtable or a table yet are newer than every
/// file the bound reads: the flusher encodes one group at a time and the VLog appends to its
/// newest file. A group that fails after it was logged lowers the active memtable's bound, since
/// recovery replays its records.
pub(crate) fn vlog_floor(
	active: &RwLock<Arc<MemTable>>,
	manifest: &RwLock<LevelManifest>,
	immutables: &RwLock<ImmutableMemtables>,
) -> Result<u32> {
	let active = active.read()?;
	let manifest = manifest.read()?;
	let immutables = immutables.read()?;
	let queued = immutables.iter().filter_map(|entry| entry.memtable.min_vlog_file_id());
	Ok(active
		.min_vlog_file_id()
		.into_iter()
		.chain(queued)
		.fold(manifest.min_oldest_vlog_file_id(), u32::min))
}

/// Cleans up obsolete VLog files.
///
/// This function should be called after compaction, flush, or during startup recovery, with no
/// lock held.
///
/// # Arguments
/// * `vlog` - The VLog instance (if value separation is enabled)
/// * `active`, `manifest`, `immutables` - What points into the VLog, see [`vlog_floor`]
/// * `context` - Description of the calling context (e.g., "flush", "compaction", "startup")
pub(crate) fn cleanup_vlog(
	vlog: &Option<Arc<VLog>>,
	active: &RwLock<Arc<MemTable>>,
	manifest: &RwLock<LevelManifest>,
	immutables: &RwLock<ImmutableMemtables>,
	context: &str,
) {
	let Some(vlog) = vlog else {
		return;
	};
	let floor = match vlog_floor(active, manifest, immutables) {
		Ok(floor) => floor,
		Err(e) => {
			tracing::warn!("Failed to cleanup obsolete vlog files during {}: {}", context, e);
			return;
		}
	};

	// Skip cleanup if no table references VLog files yet (fresh database case)
	if floor == 0 {
		return;
	}

	// Delete obsolete VLog files
	if let Err(e) = vlog.cleanup_obsolete_files(floor) {
		tracing::warn!("Failed to cleanup obsolete vlog files during {}: {}", context, e);
	}
}
