use std::collections::HashSet;
use std::fs::create_dir_all;
#[cfg(not(any(target_os = "windows", target_family = "wasm")))]
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};

use crate::batch::Batch;
use crate::checkpoint::{CheckpointGate, CheckpointMetadata, DatabaseCheckpoint};
use crate::compaction::compactor::{CompactionOptions, Compactor};
use crate::compaction::manual::RangeStrategy;
use crate::compaction::{CompactionChoice, CompactionStrategy};
use crate::error::{BackgroundErrorHandler, Result};
use crate::levels::{
	check_manifest_before_open,
	plan_orphan_cleanup,
	resolve_encryption_in,
	write_manifest_to_disk_in,
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
use crate::vfs::{CreateOpts, Fs, FsKind, FsWriter};
use crate::vlog::{VLog, ValueLocation};
use crate::wal::recovery::{
	classify_wal_damage_in,
	repair_corrupted_wal_segment_in,
	replay_wal_inner_with,
	WalDamage,
	WalRecoveryReport,
};
use crate::wal::tail::{self, TailCheck, TailEnd};
use crate::wal::{self, cleanup_old_segments_for_in, list_segment_ids_in, Wal, WalManager};
use crate::{
	rt,
	CompactRangeOptions,
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

	/// Whether `strategy` would move tables down a level now. The level task compacts until this
	/// is false, so a backlog left by a burst of flushes is worked off without waiting for the next
	/// flush. A compaction of the last level into itself does not count: it does not change the
	/// shape it was picked for, and running it again at once would only rewrite the same tables.
	fn compaction_pending(&self, strategy: &dyn CompactionStrategy) -> bool {
		let _ = strategy;
		false
	}

	/// Returns a reference to the background error handler
	fn error_handler(&self) -> Arc<BackgroundErrorHandler>;

	/// Returns true if there are immutable memtables pending flush.
	fn has_pending_immutables(&self) -> bool;

	/// Asks the key manager again for the keys the database uses, which a task that is about to
	/// try again after a key provider failed does first, and which may wait on the provider. Does
	/// nothing for a database that is not encrypted.
	fn prepare_keys(&self) -> Result<()> {
		Ok(())
	}

	/// What the memtable flush task awaits before it flushes: the spare files of a pool. `None`
	/// for every other file system, and for a tree that has nothing to wait for.
	fn spares_for_flush(&self) -> Option<crate::vfs::SpareWait> {
		None
	}

	/// What the compaction task awaits before it compacts.
	fn spares_for_compaction(&self) -> Option<crate::vfs::SpareWait> {
		None
	}

	/// How many creations the file system has refused for want of a file, ever. A task reads it
	/// before and after a unit of work: a failure of a unit during which it advanced is not
	/// permanent, whatever shape the failure took through the layers it came up.
	fn pool_exhaustions(&self) -> u64 {
		0
	}

	/// Called when a unit of background work is over: cleans some of the files that were removed.
	fn after_background_unit(&self) {}

	/// Whether a pool refused a creation since `exhaustions` was read from `pool_exhaustions`, and
	/// is not at its limit, which is what makes a failure one that passes.
	fn exhausted_since(&self, exhaustions: u64) -> bool {
		let _ = exhaustions;
		false
	}

	/// Called with the outcome of a level compaction, so that the next one waits for as many files
	/// as the last one needed.
	fn compaction_ran(&self, result: &Result<()>) {
		let _ = result;
	}
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
/// `compactions` comes before them too: a compaction holds it shared from the moment it picks its
/// tables to the removal of what it made obsolete, and takes the locks above under it, and a
/// restore holds it exclusively. Nothing waits for it while holding any of them.
///
/// `rotations`, which serializes the rotations of the memtable and the WAL with a restore, comes
/// before `active_memtable`, and after `flush_lock` and `compactions`, which a restore takes first
/// and a rotation never takes. A rotation takes it with nothing else held, except the checkpoint
/// gate when a checkpoint's flush rotates, and nothing waits for it while holding
/// `active_memtable` or a lock below it.
///
/// A checkpoint takes the checkpoint gate shared (`CheckpointGate::enter()`) before any of them
/// and a restore takes it exclusively before any of them, so the two never overlap and neither
/// waits for the gate while holding a lock the other needs.
///
/// The commit pipeline takes two of them outside the flush path: a commit group's direct level-0
/// write holds `flush_lock` with nothing else held and takes the others under it, as a flush
/// does, and a group's publication holds the `active_memtable` read guard alone, with no lock
/// under it.
///
/// The WAL's own lock (`wal`) sits below every lock above. Under it are taken only the segment's
/// fsync gate, which is a leaf, and, by a rotation (`rotate_wal()`), the value log's writer lock
/// and its list of unsynced files, which nothing holds while it waits for the WAL lock: the commit
/// pipeline appends to the value log before it takes the WAL lock and never holds both. The
/// rotation that opening a tree and a restore make to continue in a fresh segment takes them the
/// same way. A restore holds the value log's writer lock too, from before it removes the files
/// until it has reloaded the log, and takes no other lock in between.
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

	/// Held shared by every compaction for its whole run, and exclusively by a restore. A
	/// compaction names its inputs and takes the ids of its outputs from the manifest it picked
	/// from, writes the outputs, and then applies a changeset to the manifest and deletes the
	/// input files by id. A restore replaces the manifest, the table files and the ids: a
	/// compaction that went on would delete the restored tables that have the ids of its inputs,
	/// write over the ones that have the ids of its outputs, and list what it made in a manifest
	/// it was not computed from.
	pub(crate) compactions: parking_lot::RwLock<()>,

	/// Held by one compaction at a time, from the pick of its inputs to the removal of them. The
	/// background task and `Tree::compact_range` both pick from the manifest, and two picks that
	/// overlap would merge the same tables twice.
	compaction_serial: parking_lot::Mutex<()>,

	/// Held for the whole of a rotation of the memtable and the WAL (`rotate_memtable()`,
	/// `seal_active_wal_segment()`, `replace_failed_wal()`), taken before the active memtable's
	/// write lock, and by a restore from before it removes the files until it has reopened the
	/// WAL. A rotation creates the next WAL segment, and a restore removes the directory of the
	/// WAL and writes it again: a segment created between the restore's listing of the
	/// directory and its removal fails the restore with `DirectoryNotEmpty` after part of the
	/// database is gone, and one that the old WAL creates in the restored directory is
	/// replayed with the restored ones. The active memtable's lock cannot serve, a restore
	/// would hold it, against every reader, for as long as it copies files.
	pub(crate) rotations: parking_lot::Mutex<()>,

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

	/// The WAL segments below this one may hold the records of a commit group that failed with
	/// none of it applied, 0 when there is none. Its committers were told it failed, so no later
	/// group may be logged while recovery could still replay it: it would come back underneath
	/// commits that never saw it. See `drop_unapplied_group`.
	pub(crate) unapplied_group_below: AtomicU64,

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
	pub(crate) manifest_uncertain: Arc<AtomicBool>,

	/// Test-only observer called by `flush_immutable_to_sst` once the SST is on disk and before
	/// the manifest is updated. An error from it fails the flush.
	#[cfg(test)]
	pub(crate) flush_hook: parking_lot::Mutex<Option<FlushHook>>,

	/// Test-only observer called by `write_batch_direct_to_l0_sst` once its table is open and
	/// before the manifest is updated. An error from it fails the write.
	#[cfg(test)]
	pub(crate) direct_table_hook: parking_lot::Mutex<Option<FlushHook>>,

	/// Test-only observer called by `remove_obsolete_wal_segments` with the `log_number` of the
	/// manifest, after the direct table is registered and before any segment is removed.
	#[cfg(test)]
	pub(crate) direct_cleanup_hook: parking_lot::Mutex<Option<FlushHook>>,

	/// Test-only observer called by `create_checkpoint` at each of its stages.
	#[cfg(test)]
	pub(crate) checkpoint_hook: parking_lot::Mutex<Option<crate::checkpoint::CheckpointHook>>,

	/// Test-only observer that each compaction reads when it starts and calls at its stages; see
	/// `CompactionStage`.
	#[cfg(test)]
	pub(crate) compaction_stage_hook:
		parking_lot::Mutex<Option<crate::compaction::compactor::CompactionStageHook>>,

	/// Report of actions taken during WAL recovery at database open.
	pub(crate) recovery_report: parking_lot::RwLock<WalRecoveryReport>,

	/// Whether the file system is a pool, which has a fixed number of files to create from. Every
	/// call that only a pool needs (waiting for spare files, cleaning dead ones, reading the
	/// exhaustion counter) is made behind this, so that a database on `std` does nothing more than
	/// it did.
	pub(crate) pooled: bool,

	/// The spare files that one commit group may need to create, a bound that is derived once from
	/// the options (`ring::pipeline::group_need_of`, set when the commit pipeline is made) and not
	/// measured, and that the file system keeps back for the commit path from the compaction that
	/// would eat them (`PoolControl::set_reserve`). 0 until then.
	pub(crate) group_need: AtomicUsize,

	/// Whether the cap of the pool (`PoolConfig::max_files`) is too low for the full bound
	/// `group_need`: a pool that cannot hold it twice over, with `cap_band` to spare, could be
	/// made to refuse every commit, and every compaction, once its database has grown a little
	/// (see `commit_group_need`). The database then degrades, in the way that `commit_group_need`
	/// says, and does not wedge. False for a pool with the default cap, and for every file system
	/// that is not a pool.
	cap_limited: AtomicBool,

	/// With `cap_limited`: the files, above `cap_floor`, that only a delete-only commit group and
	/// a compaction may take. A group that holds anything else is not started while fewer than
	/// `cap_floor + cap_band` files are free, so that a compaction always has `cap_band` files to
	/// make its outputs in, and what it frees brings the commits back.
	cap_band: AtomicUsize,

	/// With `cap_limited`: the files that the commit path keeps back from the outputs of a
	/// compaction, which is what a group of the usual size needs (`small_group_need_of`), and
	/// never less than a flush does.
	cap_floor: AtomicUsize,

	/// How many spare files beyond `group_need` the next level compaction waits for. It starts at
	/// `COMPACTION_HINT_MIN`, doubles each time a compaction met an empty pool, up to
	/// `COMPACTION_HINT_MAX`, and starts again after a compaction that worked.
	compaction_hint: AtomicUsize,
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

/// `need`, or the files that `fs` can have at all if that is fewer: the free ones and what is left
/// of its limit. Always `need` for a file system that is not a pool.
pub(crate) fn clamp_to_pool(fs: &dyn Fs, need: usize) -> usize {
	match fs.pool() {
		Some(pool) => {
			let stats = pool.stats();
			let potential = stats.clean + stats.dirty + stats.max_files.saturating_sub(stats.total);
			need.min(potential)
		}
		None => need,
	}
}

/// The spare files that a flush of a memtable waits for: its table, the manifest that lists it (a
/// replaced manifest frees its old file only once it is clean), a roll of the value log, and room
/// to spare.
pub(crate) const FLUSH_NEED: usize = 8;

/// The least and the most files that `CoreInner::cap_band` keeps for the work that frees files in
/// a pool whose cap is below the bound of a commit group.
const CAP_BAND_MIN: usize = 4;
const CAP_BAND_MAX: usize = 32;

/// What a commit group is, as far as the files it needs go: see `CoreInner::commit_group_need`.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct GroupShape {
	/// What a group of this size needs if it is not a batch of its own: one segment of the log
	/// for each rotation of the memtable that it can make, and a margin
	/// (`ring::pipeline::small_group_need_of`).
	pub(crate) usual: usize,
	/// Every entry of every batch is a delete.
	pub(crate) delete_only: bool,
	/// A batch of it is bigger than a memtable.
	pub(crate) oversized: bool,
}

/// The first wait of a compaction for spare files, beyond what the commit path keeps back.
const COMPACTION_HINT_MIN: usize = 4;

/// The most that a compaction waits for beyond what the commit path keeps back.
const COMPACTION_HINT_MAX: usize = 256;

/// How long `close` waits for the pool to make the files of the shutdown flush before it flushes
/// with the files that the pool has.
const CLOSE_SPARES_WAIT: std::time::Duration = if cfg!(test) {
	std::time::Duration::from_millis(300)
} else {
	std::time::Duration::from_secs(5)
};

/// How many dead files of a pool a background unit cleans when it is done. A clean is a few writes
/// and one flush of a file, and the worker is the one that serves queries.
const MAINTAIN_BUDGET: usize = 32;

impl CoreInner {
	/// Creates a new LSM tree core instance
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		// Whether the database is encrypted was settled before this, from its manifest. A key
		// manager with no context here is a path that did not, and every file below, tables
		// included, would be written in the clear: nothing is created.
		opts.ensure_encryption_resolved()?;

		// Acquire database lock to prevent multiple processes from opening the same
		// database
		let mut lockfile = opts.fs.lock_file(&opts.path);
		lockfile.acquire()?;

		// Initialize immutable memtables
		let immutable_memtables = Arc::new(RwLock::new(ImmutableMemtables::default()));

		// Initialize level manifest FIRST to get log_number. Whether it exists must be read
		// first, because `LevelManifest::new` creates one when it does not.
		let manifest_loaded_from_disk = opts.fs.exists(&opts.manifest_file_path(0));
		let manifest = LevelManifest::new_in(&*opts.fs, Arc::clone(&opts))?;
		let manifest_log_number = manifest.get_log_number();

		// Whether the table files that the manifest does not list are this database's is read
		// before anything of the log or the value log is opened, so that a table of another
		// database, which the clean-up after the replay would delete, refuses the open with
		// nothing changed.
		if manifest_loaded_from_disk {
			if let Some(context) = opts.encryption_context() {
				let live: HashSet<u64> = manifest.get_all_tables().keys().copied().collect();
				plan_orphan_cleanup(&opts, &live)?;
				// The same for the segments of the log. The ones from the log number on are read by
				// the replay, which refuses another database's, but the ones below it are only
				// deleted by number, and a manifest that names another database, or a segment that
				// was put there, would have them deleted without anyone having read them.
				wal::sealed::check_directory(context.db_id(), &opts.wal_dir())?;
			}
		}

		// The value log is opened before the WAL is: opening the WAL makes a segment and removes
		// what a repair left, and an open that the value log refuses (a file in the clear in an
		// encrypted database, or one that the key does not open) must not have done either.
		let error_handler = Arc::new(BackgroundErrorHandler::new());
		let vlog = if opts.enable_vlog {
			let vlog = VLog::new_in(Arc::clone(&opts.fs), Arc::clone(&opts))?;
			vlog.set_error_handler(Arc::clone(&error_handler));
			Some(Arc::new(vlog))
		} else {
			None
		};

		// Initialize WAL starting from manifest.log_number
		let wal_path = opts.wal_dir();
		// This avoids creating intermediate empty WAL files
		let wal_instance = Wal::open_with_min_log_number(
			&wal_path,
			manifest_log_number,
			wal::Options::for_tree(&opts)?,
		)?;

		// Starts at 0 since no commits have happened yet.
		let visible_seq_num = Arc::new(AtomicU64::new(0));

		// Initialize active memtable with its WAL number set to the initial WAL
		// This tracks which WAL the memtable's data belongs to for later flush
		let initial_memtable = Arc::new(MemTable::new(opts.max_memtable_size));
		initial_memtable.set_wal_number(wal_instance.get_active_log_number());
		let active_memtable = Arc::new(RwLock::new(initial_memtable));

		let level_manifest = Arc::new(RwLock::new(manifest));

		let memory_controller = Arc::new(crate::memory::MemoryController::default());

		let pooled = opts.fs.kind() == FsKind::Pool;

		Ok(Self {
			opts,
			active_memtable,
			immutable_memtables,
			flush_lock: parking_lot::Mutex::new(()),
			compactions: parking_lot::RwLock::new(()),
			compaction_serial: parking_lot::Mutex::new(()),
			rotations: parking_lot::Mutex::new(()),
			level_manifest,
			snapshot_tracker: SnapshotTracker::new(),
			active_txn_tracker: Arc::new(crate::tracker::ActiveTxnTracker::new()),
			vlog,
			wal: WalManager::new(wal_instance),
			lockfile: Mutex::new(lockfile),
			error_handler,
			visible_seq_num,
			group_wal_pin: AtomicU64::new(u64::MAX),
			unapplied_group_below: AtomicU64::new(0),
			memory_controller,
			manifest_loaded_from_disk,
			manifest_uncertain: Arc::new(AtomicBool::new(false)),
			#[cfg(test)]
			flush_hook: parking_lot::Mutex::new(None),
			#[cfg(test)]
			direct_table_hook: parking_lot::Mutex::new(None),
			#[cfg(test)]
			direct_cleanup_hook: parking_lot::Mutex::new(None),
			#[cfg(test)]
			checkpoint_hook: parking_lot::Mutex::new(None),
			#[cfg(test)]
			compaction_stage_hook: parking_lot::Mutex::new(None),
			recovery_report: parking_lot::RwLock::new(WalRecoveryReport::default()),
			pooled,
			group_need: AtomicUsize::new(0),
			cap_limited: AtomicBool::new(false),
			cap_band: AtomicUsize::new(0),
			cap_floor: AtomicUsize::new(0),
			compaction_hint: AtomicUsize::new(COMPACTION_HINT_MIN),
		})
	}

	/// Records the files that one commit group may need (see `group_need_of`) and has the pool keep
	/// that many back from the outputs of a compaction. Called once, when the commit pipeline is
	/// made, and only for a pool.
	///
	/// `usual` is what the largest group that is not a batch of its own needs
	/// (`small_group_need_max`). A pool whose cap is below `2 * need + band` is `cap_limited`:
	/// what the pool keeps back is then `usual`, and the bound `need` is asked only of a batch that
	/// is bigger than a memtable, which the pool then refuses with `PoolFull` when it cannot be
	/// given.
	pub(crate) fn set_group_need(&self, need: usize, usual: usize) {
		self.group_need.store(need, Ordering::Release);
		let Some(pool) = self.opts.fs.pool() else {
			return;
		};
		let max_files = pool.stats().max_files;
		let band = (max_files / 8).clamp(CAP_BAND_MIN, CAP_BAND_MAX);
		if max_files < need.saturating_mul(2).saturating_add(band) {
			let floor = usual.max(FLUSH_NEED);
			self.cap_band.store(band, Ordering::Release);
			self.cap_floor.store(floor, Ordering::Release);
			self.cap_limited.store(true, Ordering::Release);
			pool.set_reserve(floor);
		} else {
			pool.set_reserve(need);
		}
	}

	/// Whether the cap of the pool is too low for the full bound of a commit group: see
	/// `cap_limited`.
	pub(crate) fn is_cap_limited(&self) -> bool {
		self.cap_limited.load(Ordering::Acquire)
	}

	/// The files that a commit group of this `shape` waits for, `vlog` of them for the rolls of
	/// the value log, in a pool.
	///
	/// With a cap that holds the full bound it is `group_need`, as it always was. In a
	/// `cap_limited` pool the bound could never be met once the database has grown, so what a group
	/// needs is what it can use: one log segment for each time it can rotate the memtable, and a
	/// margin (`small_group_need_of`), but at least `cap_floor`, which compaction leaves free. A
	/// group of puts and merges also asks for `cap_band` more, so that the last files of the pool
	/// are left to the work that frees files: a delete-only group (a delete does not grow the
	/// database, and is how a full one is made smaller) and the outputs of a compaction. A batch
	/// that is bigger than a memtable needs the full bound, because it writes a table of its own
	/// after flushing the queue, and a pool that cannot hold that refuses it with `PoolFull`.
	pub(crate) fn commit_group_need(&self, shape: GroupShape, vlog: usize) -> usize {
		let full = self.group_need.load(Ordering::Acquire);
		if !self.is_cap_limited() || shape.oversized {
			return full + vlog;
		}
		let floor = self.cap_floor.load(Ordering::Acquire);
		let base = (shape.usual + vlog).max(floor);
		if shape.delete_only {
			base
		} else {
			base + self.cap_band.load(Ordering::Acquire)
		}
	}

	/// The wait of the flush of a memtable for the files it creates.
	fn spares_for_flush(&self) -> Option<crate::vfs::SpareWait> {
		if !self.pooled {
			return None;
		}
		self.opts.fs.ensure_spares(FLUSH_NEED)
	}

	/// The wait of a level compaction for the files its outputs take, which are created only while
	/// more than `group_need` files are free (more than `cap_floor` in a `cap_limited` pool, and
	/// then at most `cap_band` of them are waited for, which is all that the commits leave it).
	/// Never for more than the pool can have: a wait for more would fail with `PoolFull` every
	/// time, before the compaction was tried.
	fn spares_for_compaction(&self) -> Option<crate::vfs::SpareWait> {
		if !self.pooled {
			return None;
		}
		let hint =
			self.compaction_hint.load(Ordering::Acquire).max(self.compaction_outputs_bound());
		let need = if self.is_cap_limited() {
			self.cap_floor.load(Ordering::Acquire) + hint.min(self.cap_band.load(Ordering::Acquire))
		} else {
			self.group_need.load(Ordering::Acquire) + hint
		};
		self.opts.fs.ensure_spares(self.clamped_to_pool(need))
	}

	/// How many output files the next compaction may make, as far as the tables that it can merge
	/// say: the two shallowest levels that hold tables, over `target_file_size`, and two more. An
	/// upper bound for the compaction that is likely, so that its first wait is for about what it
	/// needs, and the one that merges most of a database is not run again and again with a
	/// doubling hint, each run merging every input anew. At most `COMPACTION_HINT_MAX`, and never
	/// more than the pool can have.
	fn compaction_outputs_bound(&self) -> usize {
		let Ok(manifest) = self.level_manifest.read() else {
			return 0;
		};
		let bytes: u64 = manifest
			.levels
			.get_levels()
			.iter()
			.filter(|level| !level.tables.is_empty())
			.take(2)
			.flat_map(|level| level.tables.iter())
			.map(|table| table.file_size)
			.sum();
		drop(manifest);
		let target = self.opts.target_file_size.max(1);
		let outputs =
			usize::try_from(bytes.div_ceil(target)).unwrap_or(usize::MAX).saturating_add(2);
		clamp_to_pool(&*self.opts.fs, outputs.min(COMPACTION_HINT_MAX))
	}

	/// The wait of `close` for the files that the shutdown flush creates: a table and a manifest
	/// for every memtable that is queued and for the active one, and a roll of the value log.
	fn spares_for_close(&self) -> Option<crate::vfs::SpareWait> {
		if !self.pooled {
			return None;
		}
		let need = 3 * (self.immutable_count() + 1) + 4;
		self.opts.fs.ensure_spares(self.clamped_to_pool(need))
	}

	/// `need`, or what the pool can have if that is less: a wait for more than that would fail at
	/// once with the error of a pool that is at its limit, before the work that needs fewer was
	/// tried.
	pub(crate) fn clamped_to_pool(&self, need: usize) -> usize {
		clamp_to_pool(&*self.opts.fs, need)
	}

	/// Removes the segments of the log below `min_wal_to_keep`, at the close, when every memtable
	/// is in a table and the segment that was active goes too.
	///
	/// In a pool a removal is a write that a crash may keep or lose on its own: the removals that
	/// are not flushed are lost or kept in any combination, and a crash that keeps the removal of
	/// the newest segment and loses that of an older one leaves a database whose manifest is past
	/// every segment that is there, which the open refuses (`log_number exceeds WAL segments`),
	/// with all of its data in tables. So a pool removes all of them but the newest, makes those
	/// removals durable (`maintain_pool` cleans the files and flushes each), and removes the
	/// newest last: whatever a crash keeps, the newest segment is there or nothing is.
	///
	/// On `std` it is the one pass that it always was. A journaled file system keeps the removals
	/// of one directory in the order they were made, which is the case that this does not change
	/// (a file system that does not is not known to be one of those that are used: unverified).
	fn remove_obsolete_wal_segments_at_close(
		&self,
		wal_dir: &Path,
		min_wal_to_keep: u64,
	) -> Result<usize> {
		let context = self.opts.encryption_context().map(Arc::as_ref);
		let newest = if self.pooled {
			list_segment_ids_in(&*self.opts.fs, wal_dir, Some("wal"))
				.ok()
				.and_then(|ids| ids.into_iter().filter(|id| *id < min_wal_to_keep).max())
		} else {
			None
		};
		let mut removed = 0;
		if let Some(newest) = newest {
			removed += cleanup_old_segments_for_in(context, &*self.opts.fs, wal_dir, newest)?;
			self.maintain_pool(usize::MAX);
		}
		Ok(removed
			+ cleanup_old_segments_for_in(context, &*self.opts.fs, wal_dir, min_wal_to_keep)?)
	}

	/// Removes, at the open, the segments of the log that nothing needs, after the recovery has put
	/// the records that it replayed in tables (all but `data_segment`, which holds those of the
	/// recovered memtable) and a new segment, `active`, has been started.
	///
	/// A session that ends without a close (a browser tab cannot run one) is the usual end of a
	/// session on the web, and every open starts a segment: a recovery flush moves the manifest's
	/// `log_number` and removes nothing, and one that finds no records moves nothing, so without
	/// this each of those sessions leaves one more file, and a file pool at its limit then cannot
	/// open the database. The segments below `data_segment` are removed as a close does it, with
	/// the newest of them last; those between it and `active` hold no record at all, and a
	/// crash that keeps any of the removals leaves a log that opens, as the one that was there.
	///
	/// Best effort: what stays is removed by the next open, a flush or the close.
	fn remove_dead_wal_segments_at_open(&self, data_segment: Option<u64>, active: u64) {
		// A sealed log names, checks and numbers its segments in its own way, and is on `std`,
		// which has no limit of files: it keeps what it kept.
		if self.opts.encryption_context().is_some() {
			return;
		}
		let wal_dir = self.wal.read().get_dir_path().to_path_buf();
		let bound = data_segment.unwrap_or(active).min(active);
		if let Err(e) = self.remove_obsolete_wal_segments_at_close(&wal_dir, bound) {
			tracing::warn!("Failed to clean up old WAL segments at open: {}", e);
			return;
		}
		let Some(data_segment) = data_segment else {
			return;
		};
		let Ok(ids) = list_segment_ids_in(&*self.opts.fs, &wal_dir, Some("wal")) else {
			return;
		};
		for id in ids.into_iter().filter(|id| *id > data_segment && *id < active) {
			if let Err(e) = self.opts.fs.remove_file(&wal_dir.join(format!("{id:020}.wal"))) {
				tracing::warn!("Failed to remove the empty WAL segment {}: {}", id, e);
				return;
			}
		}
		self.maintain_pool(usize::MAX);
	}

	/// Cleans some of the files that were removed, so that they return to the free ones without
	/// waiting for the pool's own refill. A no-op for a file system that is not a pool.
	pub(crate) fn maintain_pool(&self, budget: usize) {
		if self.pooled {
			if let Some(pool) = self.opts.fs.pool() {
				pool.maintain(budget);
			}
		}
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
			.map_err(|e| match e {
				// An encryption error keeps its type: a key that could not be had may be asked for
				// again, and no other of them may, which a text does not say. A pool that has
				// no free file keeps its type too: it passes once the pool is refilled, which a
				// text does not say either, and one that is at its limit does not.
				Error::Encryption(_) | Error::FilePoolExhausted => e,
				e if e.is_storage_full() => e,
				e => Error::Other(format!(
					"Failed to flush memtable to SST table_id={}: {}",
					table_id, e
				)),
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
		if let Err(e) = write_manifest_to_disk_in(&*self.opts.fs, &manifest) {
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
		// A pool that is at its limit of files keeps its type: it does not pass by waiting, and
		// the layer below only lets through the one that does.
		if error.is_storage_full() {
			return error;
		}
		crate::levels::manifest_write_error(&self.manifest_uncertain, error, &context)
	}

	/// Writes the manifest in memory to disk again if the last write of it was uncertain, so that
	/// the disk holds exactly the tables in memory before a flush writes the file of one.
	pub(crate) fn settle_manifest(&self) -> Result<()> {
		if !self.manifest_uncertain.load(Ordering::Acquire) {
			return Ok(());
		}
		let manifest = self.level_manifest.write()?;
		write_manifest_to_disk_in(&*self.opts.fs, &manifest)
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
	/// The values of `batch` are in files of the value log that the pin of the commit group
	/// keeps until the table is registered (`VLog::pin_group`), and a flush writes the manifest
	/// from memory before it deletes any file, as the compaction does. A manifest on disk that
	/// lists a kept table is therefore replaced before the files that table points into can go.
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
			} else if let Err(e) = self.opts.fs.remove_file(&self.opts.sstable_file_path(table_id))
			{
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
			// The same order as the flush of a memtable (`MemTable::flush`): created empty, written
			// through the handle that is dropped when the writer finishes, made durable by path,
			// and opened again for reading.
			let file = self.opts.fs.create_truncate(&table_file_path, CreateOpts::default())?;
			progress.created = true;
			let mut table_writer = crate::sstable::table::TableWriter::new(
				FsWriter(file),
				table_id,
				Arc::clone(&self.opts),
				0,
			);

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

		let fs = &*self.opts.fs;
		fs.sync_file(&table_file_path)?;
		fs.sync_dir(&self.opts.sstable_dir())?;
		let file: Arc<dyn crate::vfs::File> = fs.open_read(&table_file_path)?;
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
		if let Err(e) = write_manifest_to_disk_in(&*self.opts.fs, &manifest) {
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
		// Before the memtable's lock, and held until the memtable is queued: see `rotations`.
		let _rotation = self.rotations.lock();

		// Step 1: Acquire WRITE lock upfront to prevent race conditions
		let active_memtable = self.active_memtable.write()?;

		if active_memtable.is_empty() {
			return Ok(());
		}

		self.rotate_memtable_locked(active_memtable)
	}

	/// Rotates the non-empty active memtable under the write guard the caller holds, which it
	/// keeps until the memtable is queued: whoever holds it sees the queue only shrink, see
	/// `try_release_wal_hold`. The caller holds `rotations` too.
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
				Self::rotation_error(
					self.pooled,
					"Failed to rotate WAL before memtable rotation",
					e,
				)
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

	/// The rewrite at open: see `Wal::rewrite_active_with`. Like `rotate_wal`, the value log is
	/// synced first, before the records that point to it are made durable.
	fn rewrite_wal(&self, wal: &mut Wal, valid_end: u64) -> wal::Result<Option<u64>> {
		wal.rewrite_active_with(valid_end, || match self.vlog {
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
		// Before the memtable's lock: see `rotations`.
		let _rotation = self.rotations.lock();

		// Hold the write lock for the whole check-and-rotate so no writer can land a
		// batch in the segment we're about to seal in between the emptiness check and
		// the rotation.
		let active_memtable = self.active_memtable.write()?;
		let sealed_wal_number = active_memtable.get_wal_number();

		if !active_memtable.is_empty() {
			self.rotate_memtable_locked(active_memtable)?;
			return Ok(sealed_wal_number);
		}

		let mut wal_guard = self.wal.write();
		self.rotate_wal(&mut wal_guard).map_err(|e| {
			Self::rotation_error(self.pooled, "Failed to rotate WAL before direct-to-L0 flush", e)
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
		// Before the memtable's lock: see `rotations`.
		let _rotation = self.rotations.lock();
		let active_memtable = self.active_memtable.write()?;
		if !self.wal.read().needs_replacement() {
			return Ok(None);
		}

		let replace_error = |e: &dyn std::fmt::Display| {
			Error::Other(format!("Failed to replace the failed WAL segment: {e}"))
		};
		if active_memtable.is_empty() {
			let mut wal_guard = self.wal.write();
			self.rotate_wal(&mut wal_guard).map_err(|e| {
				Self::rotation_error(self.pooled, "Failed to replace the failed WAL segment", e)
			})?;
			let new_wal_number = wal_guard.get_active_log_number();
			drop(wal_guard);
			active_memtable.set_wal_number(new_wal_number);
			return Ok(Some(false));
		}

		self.rotate_memtable_locked(active_memtable).map_err(|e| match e {
			Error::Encryption(_) | Error::FilePoolExhausted => e,
			e if e.is_storage_full() => e,
			e => replace_error(&e),
		})?;
		Ok(Some(true))
	}

	/// Moves `log_number` to `below`, the bound of `unapplied_group_below`, so that recovery
	/// replays none of the segments that may hold the records of the commit group that failed, and
	/// clears the bound. `current` says whether a restore has replaced the database since; it is
	/// asked under `flush_lock`, which a restore holds while it replaces the manifest.
	///
	/// Every memtable tagged with one of those segments is flushed first: the active one is
	/// rotated out if it is tagged below the bound, and the queue is flushed. A failure leaves the
	/// bound where it was, and the next group tries again before it logs anything.
	pub(crate) fn drop_unapplied_group(
		&self,
		below: u64,
		current: impl Fn() -> bool,
	) -> Result<()> {
		if self.active_memtable.read()?.get_wal_number() < below {
			self.seal_active_wal_segment()?;
		}
		self.flush_all_immutables_sync()?;

		let _flush = self.flush_lock.lock();
		if !current() {
			return Err(Error::PipelineStall);
		}
		// Lock order: level_manifest -> immutable_memtables (matches flush_immutable_to_sst).
		let mut manifest = self.level_manifest.write()?;
		if manifest.get_log_number() < below {
			if self.immutable_memtables.read()?.iter().any(|entry| entry.wal_number < below) {
				return Err(Error::Other(
					"a memtable of a WAL segment that holds the records of a failed commit group \
					 is not in a table yet"
						.into(),
				));
			}
			let changeset = ManifestChangeSet {
				log_number: Some(below),
				..Default::default()
			};
			let rollback = manifest.apply_changeset(&changeset)?;
			if let Err(e) = write_manifest_to_disk_in(&*self.opts.fs, &manifest) {
				manifest.revert_changeset(rollback);
				return Err(self.manifest_write_error(
					e,
					format!("Failed to move log_number past a failed commit group: {below}"),
				));
			}
			// The manifest in memory is on disk whole, whatever an earlier write left there.
			self.manifest_uncertain.store(false, Ordering::Release);
		}
		let _ = self.unapplied_group_below.compare_exchange(
			below,
			0,
			Ordering::AcqRel,
			Ordering::Acquire,
		);
		Ok(())
	}

	/// The error of a rotation of the WAL that failed. An encryption error keeps its type: a key
	/// that could not be had may be asked for again, and no other of them may, which a text does
	/// not say. So does a pool that had no free file for the new segment: it passes once the pool
	/// is refilled.
	///
	/// A pool that is at its limit of files, and a browser that is out of quota, fail the new
	/// segment with an I/O error of the kind `StorageFull`, which the log keeps only as a kind and
	/// a text. It is given its type again here (see `Error::is_storage_full`), so that it is
	/// soft and the committer can tell it from a disk that failed. Only on a pool: no file system
	/// of a pool gives an error of an operating system, and the full disk of a native one (the
	/// same kind, with a code) is as it always was.
	fn rotation_error(pooled: bool, what: &str, error: wal::Error) -> Error {
		match error {
			wal::Error::Encryption(error) => Error::Encryption(error),
			wal::Error::PoolExhausted => Error::FilePoolExhausted,
			wal::Error::IO(error) if pooled && error.kind() == std::io::ErrorKind::StorageFull => {
				Error::Io(Arc::new(std::io::Error::new(
					std::io::ErrorKind::StorageFull,
					format!("{what}: {error}"),
				)))
			}
			wal::Error::StorageFull(message) => Error::Io(Arc::new(std::io::Error::new(
				std::io::ErrorKind::StorageFull,
				format!("{what}: {message}"),
			))),
			error => Error::Other(format!("{what}: {error}")),
		}
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
		let min_wal_to_keep = Self::hold_log_number(
			entry.wal_number + 1,
			self.level_manifest.read()?.get_log_number(),
			self.group_wal_pin.load(Ordering::Acquire),
		);
		self.remove_wal_segments_below(min_wal_to_keep);

		self.cleanup_vlog("flush");

		tracing::debug!(
			"flush_oldest_immutable_to_sst: flushed table_id={}, file_size={}",
			table.id,
			table.file_size
		);

		Ok(Some(table))
	}

	/// Removes the WAL segments below `min_wal_to_keep`, which the manifest's `log_number` has
	/// moved past. Best effort: a segment that stays is removed by the next flush or by `close`,
	/// and recovery starts at `log_number`, so it is never read.
	///
	/// The caller holds `flush_lock`, which a restore holds while it replaces the WAL directory.
	fn remove_wal_segments_below(&self, min_wal_to_keep: u64) {
		let wal_dir = self.wal.read().get_dir_path().to_path_buf();
		match cleanup_old_segments_for_in(
			self.opts.encryption_context().map(Arc::as_ref),
			&*self.opts.fs,
			&wal_dir,
			min_wal_to_keep,
		) {
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
	}

	/// Removes the WAL segments that the manifest's `log_number` is past, after a direct L0 write
	/// moved it. The bound is the manifest's own value, which already holds back the segments
	/// that the rest of the commit group and the queued memtables still need, see
	/// `hold_log_number`. See `remove_wal_segments_below`.
	pub(crate) fn remove_obsolete_wal_segments(&self) {
		let Ok(manifest) = self.level_manifest.read() else {
			return;
		};
		let log_number = manifest.get_log_number();
		drop(manifest);

		#[cfg(test)]
		{
			let hook = self.direct_cleanup_hook.lock().clone();
			if let Some(hook) = hook {
				let _ = hook(log_number);
			}
		}

		self.remove_wal_segments_below(log_number);
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
				if let Err(e) = write_manifest_to_disk_in(&*self.opts.fs, &manifest) {
					manifest.revert_changeset(rollback);
					return Err(match e {
						// A manifest that could not be sealed keeps the error of the key, and one
						// that a pool had no file for keeps the type that says it passes.
						Error::Encryption(_) | Error::FilePoolExhausted => e,
						e if e.is_storage_full() => e,
						e => Error::Other(format!(
							"Failed to update manifest log_number after immutable flush: {}",
							e
						)),
					});
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
		let fs = &*self.opts.fs;

		if !fs.exists(&sstable_dir) {
			return Ok(());
		}

		// Get all table IDs from manifest
		let manifest = self.level_manifest.read()?;
		let live_tables = manifest.get_all_tables();
		let live_table_ids: HashSet<u64> = live_tables.keys().copied().collect();
		drop(manifest);

		// In a database that is encrypted, the files that the manifest does not list are read
		// first, and none is deleted if one of them is not this database's: see
		// `plan_orphan_cleanup`. One that is not is cleaned by the names, as it always was.
		let removable: Option<HashSet<u64>> = match self.opts.encryption_context() {
			Some(_) => Some(
				plan_orphan_cleanup(&self.opts, &live_table_ids)?
					.into_iter()
					.map(|(id, _)| id)
					.collect(),
			),
			None => None,
		};

		// Scan SST directory for orphaned files
		let entries = fs.read_dir(&sstable_dir)?;
		let mut removed_count = 0;

		for entry in entries {
			let filename = &entry.name;
			let filename_str = filename.to_string_lossy();

			// Parse table ID from filename (format: {id:020}.sst)
			if filename_str.ends_with(".sst") && filename_str.len() == 24 {
				if let Ok(table_id) = filename_str[..20].parse::<u64>() {
					// Delete if not in manifest
					if !live_table_ids.contains(&table_id)
						&& removable.as_ref().is_none_or(|removable| removable.contains(&table_id))
					{
						let path = sstable_dir.join(filename);
						match fs.remove_file(&path) {
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
		ValueLocation::resolve_raw_value(value, self.vlog.as_ref()).map(|cow| cow.into_owned())
	}

	/// Resolves a value, borrowing inline bytes directly without allocation.
	pub(crate) fn resolve_value_ref<'a>(
		&self,
		value: &'a [u8],
	) -> Result<std::borrow::Cow<'a, [u8]>> {
		ValueLocation::resolve_raw_value(value, self.vlog.as_ref())
	}

	/// Resolves an owned encoded value, stripping the header in-place for inline values.
	pub(crate) fn resolve_owned_value(&self, value: Value) -> Result<Value> {
		ValueLocation::resolve_owned_value(value, self.vlog.as_ref())
	}

	/// Resolves an encoded value into `out`, reusing its capacity.
	pub(crate) fn resolve_value_into(&self, value: &[u8], out: &mut Vec<u8>) -> Result<()> {
		ValueLocation::resolve_value_into(value, self.vlog.as_ref(), out)
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
		// A stopped database moves no table: a table may hold batches of the group that failed, and
		// recovery has to find the group as it was left. A compaction that was already running
		// keeps what readers can see, because it keeps every version above the visible sequence
		// number.
		if let Some(error) = self.error_handler.commit_group_error() {
			return Err(error);
		}

		// The manifest is not settled here: the compaction writes the manifest in memory itself,
		// which settles it if that works and leaves it unsettled, with the outputs kept, if not.
		// Settling first would let a compaction heal or fail a manifest that the flush that left it
		// unsettled has yet to see.

		// Kept for the whole run, so that a restore waits for it, see `compactions`.
		let _running = self.compactions.read();
		let _serial = self.compaction_serial.lock();

		// Create compaction options from the current LSM tree state
		let options = CompactionOptions::from(self);

		// Execute compaction according to the chosen strategy
		let compactor = Compactor::new(options, strategy);
		compactor.compact()?;

		Ok(())
	}

	fn compaction_pending(&self, strategy: &dyn CompactionStrategy) -> bool {
		if self.error_handler.commit_group_error().is_some() {
			return false;
		}
		let Ok(manifest) = self.level_manifest.read() else {
			return false;
		};
		matches!(
			strategy.pick_levels(&manifest),
			Ok(CompactionChoice::Merge(input)) if input.source_level != input.target_level
		)
	}

	/// Returns a reference to the background error handler
	fn error_handler(&self) -> Arc<BackgroundErrorHandler> {
		Arc::clone(&self.error_handler)
	}

	fn has_pending_immutables(&self) -> bool {
		self.immutable_memtables.read().map(|guard| !guard.is_empty()).unwrap_or(false)
	}

	fn prepare_keys(&self) -> Result<()> {
		match self.opts.encryption_context() {
			Some(context) => Ok(context.refresh_in_use()?),
			None => Ok(()),
		}
	}

	fn spares_for_flush(&self) -> Option<crate::vfs::SpareWait> {
		CoreInner::spares_for_flush(self)
	}

	fn spares_for_compaction(&self) -> Option<crate::vfs::SpareWait> {
		CoreInner::spares_for_compaction(self)
	}

	fn pool_exhaustions(&self) -> u64 {
		if self.pooled {
			self.opts.fs.pool_exhaustions()
		} else {
			0
		}
	}

	fn after_background_unit(&self) {
		self.maintain_pool(MAINTAIN_BUDGET);
	}

	fn exhausted_since(&self, exhaustions: u64) -> bool {
		if !self.pooled || self.opts.fs.pool_exhaustions() == exhaustions {
			return false;
		}
		// A pool at its limit is not a pool that passes: the files have to be released first.
		match self.opts.fs.pool() {
			Some(pool) => {
				let stats = pool.stats();
				stats.total < stats.max_files
			}
			None => false,
		}
	}

	fn compaction_ran(&self, result: &Result<()>) {
		if !self.pooled {
			return;
		}
		match result {
			Ok(()) => self.compaction_hint.store(COMPACTION_HINT_MIN, Ordering::Release),
			Err(Error::FilePoolExhausted) => {
				let hint = self.compaction_hint.load(Ordering::Acquire);
				self.compaction_hint
					.store(hint.saturating_mul(2).min(COMPACTION_HINT_MAX), Ordering::Release);
			}
			Err(_) => {}
		}
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
	pub(crate) flusher_handle: Mutex<Option<rt::JoinHandle<()>>>,

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

/// What the replay of the log leaves for the open: the highest sequence number, the memtable of
/// the records that are in no table, the report, and the newest segment with the offset where its
/// last complete record ends, when the open may rewrite it (see [`Core::replay_wal_in`]).
pub(crate) type ReplayedLog =
	(Option<u64>, Option<Arc<MemTable>>, WalRecoveryReport, Option<(u64, u64)>);

impl Core {
	/// Replays a plaintext WAL, see [`replay_wal_in`](Self::replay_wal_in).
	#[cfg(test)]
	pub(crate) fn replay_wal_with_repair<F>(
		wal_path: &Path,
		min_wal_number: u64,
		context: &str,
		recovery_mode: WalRecoveryMode,
		arena_size: usize,
		flush_memtable: F,
	) -> Result<(Option<u64>, Option<Arc<MemTable>>, WalRecoveryReport)>
	where
		F: FnMut(Arc<MemTable>, u64, bool) -> Result<()>,
	{
		let (max_seq, memtable, report, _) = Self::replay_wal_in(
			&wal::WalContext::plaintext(),
			wal_path,
			min_wal_number,
			context,
			recovery_mode,
			arena_size,
			None,
			flush_memtable,
		)?;
		Ok((max_seq, memtable, report))
	}

	/// Replays WAL with configurable corruption handling.
	///
	/// Creates one memtable per WAL segment. Flushes all but the last memtable
	/// to SST via the provided callback. Returns the last memtable as active.
	///
	/// Every replay of a tree (open and restore) goes through here with the context of the tree.
	///
	/// # Arguments
	/// * `wal_context` - Whether the segments are sealed, and under what
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
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn replay_wal_in<F>(
		wal_context: &wal::WalContext,
		wal_path: &Path,
		min_wal_number: u64,
		context: &str,
		recovery_mode: WalRecoveryMode,
		arena_size: usize,
		mut tail: Option<&mut TailCheck<'_>>,
		mut flush_memtable: F,
	) -> Result<ReplayedLog>
	where
		F: FnMut(Arc<MemTable>, u64, bool) -> Result<()>,
	{
		let mut report = WalRecoveryReport::default();
		let mut repair_attempts = 0;

		// Before anything of the log is replayed, so that no memtable is made of a record that the
		// segment is then cut before. Once: the records that are kept were all looked at.
		if let Some(tail) = tail.as_deref_mut() {
			Self::cut_tail_of_log(
				tail,
				wal_context,
				wal_path,
				min_wal_number,
				recovery_mode,
				&mut report,
			)?;
		}

		let (wal_seq_num_opt, memtables, newest_end) = loop {
			match replay_wal_inner_with(wal_context, wal_path, min_wal_number, arena_size) {
				Ok((seq, memtables, total_batches, segments_processed, newest_end)) => {
					report.records_replayed = total_batches;
					report.segments_replayed = segments_processed;
					break (seq, memtables, newest_end);
				}
				Err(Error::WalCorruption {
					segment_id,
					offset,
					message,
				}) => match recovery_mode {
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
					WalRecoveryMode::TolerateTornTail => {
						if repair_attempts >= 10 {
							return Err(Error::Other(format!(
									"{context} failed: exceeded maximum repair attempts for WAL segment {segment_id}"
								)));
						}
						repair_attempts += 1;

						// A tear at the end of a sealed segment is already tolerated by its replay,
						// so what reaches here is damage that is not one.
						// Its records cannot be told apart without the key,
						// and nothing is discarded unless asked for.
						if wal_context.is_sealed() {
							return Err(Error::Other(format!(
								"WAL segment {segment_id:020}.wal is damaged at offset {offset} and is not a torn tail: {message}. \
								Automatic repair refused to prevent data loss. Configure WalRecoveryMode::SalvageValidPrefix to explicitly salvage."
							)));
						}

						let segment_path = wal_path.join(format!("{segment_id:020}.wal"));
						let all_ids = list_segment_ids_in(&*wal_context.fs, wal_path, Some("wal"))?;
						let max_segment_id = all_ids.into_iter().max().unwrap_or(segment_id as u64);
						let is_last_segment = segment_id as u64 == max_segment_id;

						let damage = classify_wal_damage_in(
							&*wal_context.fs,
							&segment_path,
							offset,
							is_last_segment,
						)?;
						match damage {
							WalDamage::TornTail {
								last_valid_offset,
								file_size,
							} => {
								tracing::info!(
										"Torn tail detected at end of last WAL segment {segment_id} at offset {last_valid_offset} \
										(file size {file_size}). Trimming torn tail and quarantining original."
									);
								let (quarantine_path, bytes_trimmed) =
									repair_corrupted_wal_segment_in(
										&*wal_context.fs,
										wal_path,
										segment_id,
										last_valid_offset,
									)?;
								report.quarantined_files.push(quarantine_path);
								report.bytes_trimmed += bytes_trimmed;
							}
							WalDamage::UnsyncedTail {
								last_valid_offset,
								intact_records_after,
								file_size,
							} => {
								tracing::info!(
										"Damage in the unsynced tail of the last WAL segment {segment_id} at offset {last_valid_offset} \
										(file size {file_size}): no sync mark after it says it was synced. Trimming the tail, with \
										its {intact_records_after} intact record(s) that were never synced, and quarantining the original."
									);
								let (quarantine_path, bytes_trimmed) =
									repair_corrupted_wal_segment_in(
										&*wal_context.fs,
										wal_path,
										segment_id,
										last_valid_offset,
									)?;
								report.records_dropped += intact_records_after;
								report.quarantined_files.push(quarantine_path);
								report.bytes_trimmed += bytes_trimmed;
							}
							WalDamage::InteriorCorruption {
								last_valid_offset,
								intact_records_after,
								is_last_segment,
								..
							} => {
								let explanation = if !is_last_segment {
									format!(
											"WAL segment {segment_id} is an older segment (newest is {max_segment_id}); older segments cannot have torn tails"
										)
								} else if intact_records_after == 0 {
									"a sync mark after it says the damaged bytes had been synced"
										.to_string()
								} else {
									format!(
											"found {intact_records_after} intact record(s) after corruption offset {last_valid_offset}"
										)
								};
								tracing::error!(
										"Refusing automatic repair of interior corruption in WAL segment {segment_id} at offset {last_valid_offset}: {explanation}. \
										Configure WalRecoveryMode::SalvageValidPrefix to explicitly discard subsequent data."
									);
								return Err(Error::Other(format!(
										"WAL segment {segment_id:020}.wal has interior corruption at offset {last_valid_offset} ({explanation}). \
										Automatic repair refused to prevent data loss. Configure WalRecoveryMode::SalvageValidPrefix to explicitly salvage."
									)));
							}
						}
					}
					WalRecoveryMode::SalvageValidPrefix
					| WalRecoveryMode::TolerateCorruptedWithRepair => {
						if repair_attempts >= 10 {
							return Err(Error::Other(format!(
									"{context} failed: exceeded maximum repair attempts for WAL segment {segment_id}"
								)));
						}
						repair_attempts += 1;

						// A sealed segment is repaired by its own code, which keeps the records
						// before the damage and deletes nothing. A key that
						// could not be had keeps its type.
						if wal_context.is_sealed() {
							tracing::warn!(
								"Salvaging the valid prefix of sealed WAL segment {segment_id}: {message}"
							);
							wal::sealed_recovery::repair(wal_context, wal_path, segment_id)?;
							continue;
						}

						let segment_path = wal_path.join(format!("{segment_id:020}.wal"));
						let all_ids = list_segment_ids_in(&*wal_context.fs, wal_path, Some("wal"))?;
						let max_segment_id = all_ids.into_iter().max().unwrap_or(segment_id as u64);
						let is_last_segment = segment_id as u64 == max_segment_id;

						let damage = classify_wal_damage_in(
							&*wal_context.fs,
							&segment_path,
							offset,
							is_last_segment,
						)?;
						let (last_valid_offset, intact_records_after) = match damage {
							WalDamage::TornTail {
								last_valid_offset,
								..
							} => (last_valid_offset, 0),
							WalDamage::UnsyncedTail {
								last_valid_offset,
								intact_records_after,
								..
							} => (last_valid_offset, intact_records_after),
							WalDamage::InteriorCorruption {
								last_valid_offset,
								intact_records_after,
								..
							} => {
								tracing::warn!(
										"Salvaging valid prefix of WAL segment {segment_id} up to offset {last_valid_offset}. \
										Discarding subsequent data ({intact_records_after} intact records after corruption)."
									);
								(last_valid_offset, intact_records_after)
							}
						};

						report.records_dropped += intact_records_after;
						let (quarantine_path, bytes_trimmed) = repair_corrupted_wal_segment_in(
							&*wal_context.fs,
							wal_path,
							segment_id,
							last_valid_offset,
						)?;
						report.quarantined_files.push(quarantine_path);
						report.bytes_trimmed += bytes_trimmed;
					}
				},
				Err(e) => return Err(e),
			}
		};

		// What the records of the last segment point to is made durable before anything else is
		// started on top of them: the open starts the next segment once this returns.
		if let Some(tail) = tail.as_deref() {
			tail.make_durable()?;
		}

		// Every replayed record is durable before anything is built on it: a record that was
		// readable but never synced would otherwise be lost to a power cut that keeps the
		// commits made on top of it. After the values of the last segment, so that none of its
		// records reaches the disk ahead of its value.
		wal::recovery::sync_replayed_segments_in(&*wal_context.fs, wal_path, min_wal_number)?;

		// If no memtables, nothing was recovered
		if memtables.is_empty() {
			return Ok((None, None, report, None));
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
		// The records of the newest segment are moved at the open into the segment after it, when
		// they are one memtable that is kept active: the segment is then all of its records. The
		// result names the segment and where its records end, see `Wal::rewrite_active_with`. Never
		// for a sealed log, which is not rewritten.
		let rewrite = memtables
			.last()
			.filter(|(_, oversized, wal_number)| {
				!*oversized && memtables.iter().filter(|(_, _, w)| w == wal_number).count() == 1
			})
			.and(newest_end)
			.filter(|_| !wal_context.is_sealed());
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
			return Ok((wal_seq_num_opt, None, report, None));
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

		// Tagged with the segment that its records are in, until the caller gives it the segment
		// that it will be written to: the open reads the tag to know which segments hold
		// nothing that is not in a table.
		last_memtable.set_wal_number(last_wal_number);

		let rewrite = rewrite.filter(|(newest, _)| *newest == last_wal_number);
		Ok((wal_seq_num_opt, Some(last_memtable), report, rewrite))
	}

	/// Holds the records of the last segment of the log to the value log, and cuts the segment
	/// before the first record whose value is not in it, as `recovery_mode` allows. The segment as
	/// it was is kept, as for every repair of the log, and the report says what was cut.
	fn cut_tail_of_log(
		tail: &mut TailCheck<'_>,
		wal_context: &wal::WalContext,
		wal_path: &Path,
		min_wal_number: u64,
		recovery_mode: WalRecoveryMode,
		report: &mut WalRecoveryReport,
	) -> Result<()> {
		let fs = &*wal_context.fs;
		let Some(segment) = tail::last_segment(fs, wal_path, min_wal_number)? else {
			return Ok(());
		};
		let TailEnd::Cut {
			record,
			offset,
			dropped,
		} = tail.run(wal_context, &segment, recovery_mode)?
		else {
			return Ok(());
		};

		let segment_id = segment.id as usize;
		let (quarantined, trimmed) = if wal_context.is_sealed() {
			let before = fs.file_len(&segment.file_path)?;
			let kept = wal::sealed_recovery::keep_first_batches(
				wal_context,
				wal_path,
				segment_id,
				record,
			)?;
			(kept, before.saturating_sub(fs.file_len(&segment.file_path)?))
		} else {
			repair_corrupted_wal_segment_in(fs, wal_path, segment_id, offset)?
		};
		tracing::warn!(
			"WAL segment {segment_id} ends before record {record}, at offset {offset}: the value it \
			 points to is not in the value log. {dropped} record(s) and {trimmed} byte(s) were left \
			 out, and the segment as it was is kept as {quarantined:?}."
		);
		report.records_dropped += dropped;
		report.bytes_trimmed += trimmed;
		report.quarantined_files.push(quarantined);
		Ok(())
	}

	/// Creates a new LSM tree with background task management
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		tracing::debug!("Initializing LSM tree at {:?}", opts.path);

		let inner = Arc::new(CoreInner::new(Arc::clone(&opts))?);

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
		let mut tail = inner.vlog.as_deref().map(TailCheck::new);
		let (wal_seq_num_opt, recovered_memtable, startup_report, rewrite) = Self::replay_wal_in(
			&wal::WalContext::for_options(&opts)?,
			&wal_path,
			min_wal_number,
			"Database startup",
			opts.wal_recovery_mode,
			opts.max_memtable_size,
			tail.as_mut(),
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

		if startup_report.bytes_trimmed > 0 || startup_report.records_dropped > 0 {
			tracing::warn!(
				"WAL recovery completed with modifications: {} bytes trimmed, {} records dropped, {} quarantined files: {:?}",
				startup_report.bytes_trimmed,
				startup_report.records_dropped,
				startup_report.quarantined_files.len(),
				startup_report.quarantined_files
			);
		}
		*inner.recovery_report.write() = startup_report;

		// The segments that the open writes from here on hold sync marks, which a binary that
		// knows no marks must not read: the manifest is of the version that such a binary refuses
		// first. Not before: nothing up to here writes a mark, and an open that failed before
		// this leaves the directory to the binary it came from.
		inner.level_manifest.write()?.upgrade_format_in(&*opts.fs)?;

		// The records of the newest segment, when they are the recovered memtable and nothing else,
		// are rewritten into the segment after it, which then takes the commits: the open starts no
		// segment beside the one it replayed, so a session that ends without a close adds no
		// segment and no table (see `Wal::rewrite_active_with`). Otherwise a fresh segment is
		// started, as every open did: replay may have flushed the newest segment, or moved the
		// manifest's log number past it. A rotation goes through `rotate_wal()`, which syncs the
		// value log first if it holds unsynced values, and a rewrite does the same before its
		// rename.
		{
			let mut wal_guard = inner.wal.write();
			let recovered_in_newest = match (rewrite, recovered_memtable.as_ref()) {
				(Some((newest, valid_end)), Some(memtable))
					if memtable.get_wal_number() == newest
						&& wal_guard.get_active_log_number() == newest =>
				{
					Some(valid_end)
				}
				_ => None,
			};
			// A tail that the rewrite does not take is left as it is, and a segment starts after
			// it. A torn record at the end of the segment is not such a tail: the replay has
			// trimmed it already, with a copy of it kept, so the segment ends at the records it
			// read. What the rewrite accepts past them is sync marks and padding.
			let rewritten = match recovered_in_newest {
				Some(valid_end) => inner.rewrite_wal(&mut wal_guard, valid_end)?.is_some(),
				None => false,
			};
			if !rewritten {
				inner.rotate_wal(&mut wal_guard)?;
			}
		}

		// The segment that holds the records of the recovered memtable, which no table has: the
		// only one below the new segment that a later open still has to read. Every other one
		// holds records that are in tables, or none. Its number is kept when the segment is
		// rewritten, so that the quarantine copies of the segments below it stay under the
		// retention rule of the quarantine.
		let data_segment = recovered_memtable
			.as_ref()
			.filter(|memtable| !memtable.is_empty())
			.map(|memtable| memtable.get_wal_number());

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
			inner.remove_dead_wal_segments_at_open(data_segment, current_wal_number);
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

		// Clean up any orphaned SST files from previous crashes
		// SAFETY: This must happen AFTER WAL replay so data is recovered
		// but BEFORE any new flushes that might create new SSTs
		inner.cleanup_orphaned_sst_files()?;

		// Clean up any orphaned VLog files that are no longer referenced by any SST
		// SAFETY: This must happen AFTER manifest is loaded and the WAL is replayed
		inner.cleanup_vlog("startup");

		// Nothing that can fail is left, so the tasks and the flusher are started only now: a tree
		// whose open fails keeps none of them, which would hold the lock of the database and keep
		// it from being opened again in this process.

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

		// Initialize the CommitPipeline for lock-free OCC commits and background flushing.
		let commit_pipeline = Arc::new(crate::ring::CommitPipeline::new(
			Arc::clone(&inner),
			Arc::clone(&write_stall),
			Some(Arc::clone(&task_manager)),
			max_seq_num + 1,
		));

		let flusher_handle = commit_pipeline.start_flusher();

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
		let Some(ref vlog) = self.vlog else {
			if sync {
				self.wal.sync()?;
			} else {
				self.wal.flush()?;
			}
			return Ok(());
		};
		if !sync {
			vlog.flush()?;
			self.wal.flush()?;
			return Ok(());
		}
		// A record reaches the WAL after its values reach the value log, so the sync of the value
		// log covers the values of every record that the WAL holds before it. The commits that
		// land in between have values it may not cover: the fsync of the WAL makes their records
		// durable too, but the marks of the segment claim only what it held before.
		let bound = self.wal.sync_bound();
		vlog.sync()?;
		self.wal.sync_within(&bound)?;
		Ok(())
	}

	/// Ends the flusher and the background tasks the way a crash does, without waiting for them:
	/// a tree that is abandoned, or that a test abandons, does not stay alive until the runtime is
	/// dropped. Used by `Tree::abandon`, and by the tests that crash a tree.
	pub(crate) fn abort_background_tasks(&self) {
		if let Some(handle) = self.flusher_handle.lock().unwrap().take() {
			handle.abort();
		}
		if let Some(task_manager) = self.task_manager.lock().unwrap().take() {
			task_manager.abort();
		}
	}

	/// Ends the tree the way a crash does, from a function that is not async: it is marked closed
	/// (so that no drop of it flushes or closes anything afterwards), its three tasks are aborted,
	/// and its lock is released. Nothing is flushed, no log is synced and no manifest is written,
	/// and the commits that were in flight are not decided: they end with `Error::PipelineStall`,
	/// as they do after a close, whether or not they are in the log. What the file system holds
	/// is what a crash at this point would have left, as far as the file system is concerned.
	pub(crate) fn abandon(&self) {
		self.is_closed.store(true, Ordering::SeqCst);
		self.abort_background_tasks();
		self.write_stall.signal_shutdown();
		// Nothing more is logged: the flusher is gone, and a commit that arrives is refused. The
		// commits that were admitted are told so, and none waits for the flusher.
		self.commit_pipeline.abandon();
		if let Ok(mut lockfile) = self.inner.lockfile.lock() {
			let _ = lockfile.release();
		}
	}

	/// Safely closes the LSM tree by shutting down all components in the
	/// correct order. A call that arrives while another is closing waits for it.
	///
	/// # Shutdown Sequence
	///
	/// 0. Checkpoints and restores - refuses new ones, and waits for those running, which flush
	///    memtables and copy or replace files that the steps below close or release. Takes no
	///    thread, and a `close` that is dropped meanwhile can be called again.
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
		self.close_with(true).await
	}

	/// [`close`](Self::close), with or without the trailing sync mark of the active WAL segment
	/// (see `Wal::close`). The close that a drop spawns writes none: it runs whenever the runtime
	/// gets to it, and the segment may by then be another open's, which repaired or replayed it.
	pub(crate) async fn close_with(&self, trailing_mark: bool) -> Result<()> {
		let _closing = self.closing.lock().await;
		if self.is_closed.load(Ordering::SeqCst) {
			return Ok(());
		}

		// Step 0: Wait for the checkpoints and the restore that are running. Closing is marked
		// after the wait, so a `close` that is dropped during it leaves a tree that a second call
		// closes.
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

		// Close the VLog if present. A log that cannot be synced (one whose fsync failed refuses
		// every sync) ends the shutdown here, the way a crash ends the tree: nothing after it may
		// be made durable, as the WAL's records and the tables of a flush would point at values
		// that may be lost. The tree is closed already, so a second `close` does nothing: the
		// lock is released now, so that the database can be opened again and recovery decide what
		// survived.
		if let Some(ref vlog) = self.inner.vlog {
			tracing::debug!("Closing VLog...");
			if let Err(error) = vlog.close() {
				if let Ok(mut lockfile) = self.inner.lockfile.lock() {
					let _ = lockfile.release();
				}
				return Err(error);
			}
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

			// A pool has a fixed number of files: the tables and the manifests that the flush
			// creates are waited for first, with nothing held. The wait ends with an error if the
			// pool is closed or cannot make them, and `close` then fails with it instead of waiting
			// for ever. A refill that does not answer at all is waited for a while, and then the
			// flush is tried with what the pool has: it is the files that it may not find, and its
			// error says so.
			if let Some(wait) = self.inner.spares_for_close() {
				match rt::timeout(CLOSE_SPARES_WAIT, wait).await {
					Ok(done) => done.map_err(Error::from)?,
					Err(_) => tracing::warn!(
						"The pool did not make the files for the shutdown flush in {:?}: flushing with the \
						 files that it has",
						CLOSE_SPARES_WAIT
					),
				}
			}

			// The failures of the unit are read against the pool's counter, as a background unit's
			// are: an error that went through a layer that turned it into text is still the one
			// that a pool that has no free file made. Only a pool has one.
			let exhaustions = if self.inner.pooled {
				self.inner.opts.fs.pool_exhaustions()
			} else {
				0
			};

			// Flush ALL memtables: immutables first (older data), then active (newest data)
			self.inner.flush_all_memtables_for_shutdown().map_err(|e| match e {
				// A key that could not be had stays what it is, for the caller of `close`, and a
				// pool that has no free file stays one that passes.
				Error::Encryption(_) | Error::FilePoolExhausted => e,
				e if e.is_storage_full() => e,
				// Not a typed error that a layer turned into text, but the pool did refuse a file
				// during the flush: say that it passes.
				e if self.inner.pooled && self.inner.exhausted_since(exhaustions) => {
					tracing::debug!("The shutdown flush met a pool with no free file: {e}");
					Error::FilePoolExhausted
				}
				e => Error::Other(format!("Failed to flush memtables during shutdown: {}", e)),
			})?;

			tracing::debug!("All memtables flushed successfully on shutdown");

			// The memtables of a failed segment are in tables now, so a held writer can be
			// closed. One that cannot be released reports the hold when the WAL is closed.
			let _ = self.inner.try_release_wal_hold();
		}

		// A pool keeps one handle per file, and the log of values has handles of its own on the
		// files it wrote and read, which keep a file from being removed or opened again. The
		// flush above was the last thing to append to it, so they go now, with an fsync first.
		// The files stay, and a read that comes later opens its file again.
		let vlog_released = match self.inner.vlog {
			Some(ref vlog) if self.inner.pooled => vlog.release_files(),
			_ => Ok(()),
		};

		// Step 4: Close the WAL to ensure all data is flushed
		// This is safe now because all background tasks that could write to WAL are
		// stopped NOTE: WAL must be closed BEFORE cleanup, otherwise cleanup may
		// delete the active WAL file
		let wal_log_number = self.inner.wal.read().get_active_log_number();
		tracing::debug!("Closing WAL: active_log_number={}", wal_log_number);

		// A WAL that cannot be closed (the fsync of its segment failed) does not keep the rest of
		// the shutdown from running: its error is returned at the end.
		let mut wal_guard = self.inner.wal.write();
		let wal_closed = wal_guard
			.close_with(trailing_mark)
			.map_err(|e| Error::Other(format!("Failed to close WAL: {}", e)));
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

		match self.inner.remove_obsolete_wal_segments_at_close(&wal_dir, min_wal_to_keep) {
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
		drop(final_manifest);

		// The files that this shutdown removed (the log segments of the memtables that were just
		// flushed) are clean and free for the next open, or for the next database in this pool,
		// without waiting for its own refill.
		self.inner.maintain_pool(usize::MAX);

		vlog_released.map_err(|e| Error::Other(format!("Failed to release the value log: {e}")))?;
		wal_closed
	}
}

#[derive(Clone)]
pub struct Tree {
	pub(crate) core: Arc<Core>,
	/// What the `Tree` values of one database share, and nothing else does: the last of them to be
	/// dropped closes the database (see [`TreeHandle`]). A browser build only: a native `Tree`
	/// closes the database when any of its clones is dropped, as it always has.
	#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
	#[allow(
		dead_code,
		reason = "it is never read: dropping the last of the trees is what it is for"
	)]
	handle: Arc<TreeHandle>,
}

/// The close of a database that no `Tree` is left to close.
///
/// A transaction, a snapshot and an iterator keep the core of the tree alive, and a count of the
/// references to the core would never see the last `Tree` go while one of them lives: the
/// database would stay open for good, holding its lock and its files, and could not be opened
/// again in the worker. The `Tree` values of a database share one of these instead, and nothing
/// else holds it, so it is dropped when the last of them is, whatever else is alive. Dropping it
/// closes the database in the background, and a transaction that is still running finds the
/// commit pipeline shut down, and its commit fails with `PipelineStall`, as it does after `close`.
#[cfg_attr(not(any(test, all(target_arch = "wasm32", target_os = "unknown"))), allow(dead_code))]
pub(crate) struct TreeHandle {
	core: Arc<Core>,
}

#[cfg_attr(not(any(test, all(target_arch = "wasm32", target_os = "unknown"))), allow(dead_code))]
impl TreeHandle {
	pub(crate) fn new(core: Arc<Core>) -> Self {
		Self {
			core,
		}
	}
}

impl Drop for TreeHandle {
	fn drop(&mut self) {
		if self.core.is_closed.load(Ordering::SeqCst) {
			return;
		}
		let core = Arc::clone(&self.core);
		let spawned = rt::try_spawn(async move {
			if let Err(err) = core.close_with(false).await {
				tracing::error!("Error closing store: {}", err);
			}
		});
		if !spawned {
			tracing::warn!("No runtime available for closing the store correctly");
		}
	}
}

impl Tree {
	/// The attempts that [`TreeBuilder::build_async`] makes to open a database on a pool that has
	/// too few files for the recovery it needs.
	const OPEN_ATTEMPTS: usize = 16;

	/// Creates a new LSM tree with the specified options
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		// Validate options before creating the tree
		opts.validate()?;

		// A file system that has nothing mounted is refused before any call asks it whether a
		// database exists: on it every such call answers `false`, and a sync `build()` would
		// make an empty database of that, or (while the engine still reads `opts.path` itself)
		// write one to the operating system's disk. A no-op for every other file system.
		opts.fs.check_available()?;
		let fs = Arc::clone(&opts.fs);
		// The migrators, and everything else that reads the operating system's files by path, are
		// for a database that is in them: on another file system they would look at a directory
		// that is not there and take it for one that is empty.
		let native = fs.kind() == FsKind::Std;

		// What a file system that is not the operating system's cannot do yet is refused here, with
		// nothing created or read: see `refuse_what_the_file_system_cannot_do`.
		Self::refuse_what_the_file_system_cannot_do(&opts)?;

		// Fail before anything is created or moved if a restore into the directory did not
		// finish. This must stay ahead of the migrations, the directory creation and the lock
		// file.
		crate::checkpoint::check_restore_finished(&opts)?;

		// Whether the database is encrypted is read from its manifest, and the key checked by
		// opening it, before anything is created or moved. This writes nothing: an encrypted
		// database that is new writes its first manifest, sealed, before any other file.
		let opts = resolve_encryption_in(&*fs, opts)?;

		// A RocksDB database written by SurrealDB is migrated next, and a migration that was
		// interrupted is finished or undone: it has to come before anything looks for a manifest,
		// which a RocksDB directory does not have.
		if native {
			if let Some(report) = crate::migrate::rocksdb::migrate_if_needed(&opts)? {
				tracing::info!("migrated a RocksDB database: {report:?}");
			}
		}

		// Fail before anything is created or moved if the directory holds database files
		// but no usable manifest. This must stay ahead of the other migrations, the directory
		// creation and the lock file.
		check_manifest_before_open(&opts)?;

		// A database written by SurrealKV v0.x is migrated to the current format before
		// anything else reads it, with every version and timestamp it holds.
		if native {
			if let Some(report) = crate::migrate::legacy::migrate_if_needed(&opts)? {
				tracing::info!("migrated a SurrealKV v0.x database: {report:?}");
			}
		}

		// Values that live in the value log can only be read with it. A directory that holds
		// value-log files holds such values, whatever the options of this open say.
		let opts = Self::opts_with_existing_value_log(opts)?;

		// If the path contains an existing IndexedDB dump or store, automatically migrate it
		if native && surrealkv_compat_indxdb::is_indxdb_dump_dir(&opts.path) {
			Self::migrate_from_indxdb(&opts)?;
		}

		// Create all required directory structure
		Self::create_directory_structure(&opts)?;

		// Create the core LSM tree components. A manifest that says it is current but lists a
		// table in the legacy layout can only come from a migration whose marker was lost: finish
		// it, then try again.
		let core = match Core::new(Arc::clone(&opts)) {
			Err(Error::LegacyFormat(_))
				if native && crate::migrate::legacy::migrate_forced(&opts)?.is_some() =>
			{
				Core::new(Arc::clone(&opts))?
			}
			other => other?,
		};

		// Ensure directory changes are persisted
		sync_directory_structure(&opts)?;

		Ok(Self::from_core(core))
	}

	/// Opens the database, after waiting for the files of a pool that the open creates. A file
	/// system that is not a pool has nothing to wait for, and this is `new` behind an `async fn`.
	pub(crate) async fn open_async(opts: Arc<Options>) -> Result<Self> {
		// `new` validates the options and asks the file system whether it is there, in that order:
		// a file system that is not a pool goes straight to it, so that an open on `std` does each
		// of them once.
		if opts.fs.kind() != FsKind::Pool {
			return Self::new(opts);
		}
		opts.validate()?;
		let fs = Arc::clone(&opts.fs);
		// Before anything: a file system that has nothing mounted answers with its own error here,
		// and not with the `false` that a question about a manifest would make of it.
		fs.check_available()?;
		// What a pool cannot do is refused before it is asked for the files of an open.
		Self::refuse_what_the_file_system_cannot_do(&opts)?;

		let mut need = Self::open_need(&*fs, &opts);
		let mut attempt = 1;
		loop {
			if let Some(wait) = fs.ensure_spares(clamp_to_pool(&*fs, need)) {
				wait.await.map_err(Error::from)?;
			}
			let exhaustions = fs.pool_exhaustions();
			let error = match Self::new(Arc::clone(&opts)) {
				Ok(tree) => return Ok(tree),
				Err(error) => error,
			};
			// What passes is a pool that ran short: the typed error, or a pool that refused a
			// creation during the attempt, whatever the error looks like now. A pool that is at its
			// limit does not pass, and the attempts stop there.
			let passes = !error.is_pool_full()
				&& (matches!(error, Error::FilePoolExhausted)
					|| fs.pool_exhaustions() != exhaustions)
				&& fs.pool().is_none_or(|pool| {
					let stats = pool.stats();
					stats.total < stats.max_files
				});
			if !passes || attempt >= Self::OPEN_ATTEMPTS {
				return Err(error);
			}
			tracing::debug!(
				"Opening the database found the pool short of files (attempt {attempt}): {error}"
			);
			attempt += 1;
			need = need.saturating_mul(2);
		}
	}

	/// How many files the open of the database creates in a pool, as the files that are there say.
	///
	/// A recovery turns the records of every segment of the log that the manifest does not say is
	/// flushed (the ones numbered from its `log_number`) into a memtable, and flushes every one of
	/// them but the last, a table and a manifest each. The count is of segments that have bytes in
	/// them, not of bytes: how many memtables a segment becomes is known only when it has been
	/// read, which is what the attempts that follow are for. The rest is the open itself: the first
	/// manifest, the segments the open starts, the vlog's first file, a repair and its copy.
	fn open_need(fs: &dyn Fs, opts: &Options) -> usize {
		const OPEN_BASE: usize = 8;
		let log_number = fs
			.read(&opts.manifest_file_path(0))
			.ok()
			.and_then(|data| crate::levels::unseal_manifest(data, None).ok())
			.and_then(|body| crate::levels::DecodedManifest::decode(body).ok())
			.map_or(0, |manifest| manifest.log_number);
		let wal_dir = opts.wal_dir();
		let segments = fs
			.read_dir(&wal_dir)
			.map(|entries| {
				entries
					.iter()
					.filter(|entry| entry.kind == crate::vfs::EntryKind::File)
					.filter_map(|entry| {
						let (id, extension) =
							wal::parse_segment_name(&entry.name.to_string_lossy()).ok()?;
						wal::should_include_file(Some("wal"), extension).then_some((id, entry))
					})
					.filter(|(id, _)| *id >= log_number)
					.filter(|(_, entry)| {
						fs.file_len(&wal_dir.join(&entry.name)).map_or(true, |len| len > 0)
					})
					.count()
			})
			.unwrap_or(0);
		2 * segments + 1 + OPEN_BASE
	}

	/// A tree over a core that is open.
	fn from_core(core: Core) -> Self {
		let core = Arc::new(core);
		Self {
			#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
			handle: Arc::new(TreeHandle::new(Arc::clone(&core))),
			core,
		}
	}

	/// Refuses, before anything is read or created, what the engine does not do on a file system
	/// that is not the operating system's (spec 2.2 of the browser port): encryption at rest, the
	/// sealed log and the sealed value log that come with it, which read and repair their files
	/// by path. Checkpoints and restores are refused where they begin, and the migrators are not
	/// run (see `Tree::new`). A refusal is an `Error::InvalidArgument` that names the feature.
	///
	/// The migrators are not run in a pool, and a directory that one of them would have had to
	/// convert is refused too, with an `Error::MigrationFailed` that names the other store: a
	/// RocksDB directory, a migration of one that was cut short (its marker), an IndexedDB dump,
	/// and the marker of a v0.x migration. They are the files of other stores, which a pool holds
	/// only if somebody put them there. A v0.x directory whose tables are in the old layout is
	/// refused with `Error::LegacyFormat`, when its tables are opened.
	fn refuse_what_the_file_system_cannot_do(opts: &Options) -> Result<()> {
		if opts.fs.kind() != FsKind::Std {
			if opts.key_manager.is_some() || opts.encryption_context().is_some() {
				return Err(Self::unsupported_here("encryption at rest"));
			}
			Self::refuse_a_directory_that_needs_a_migration(&*opts.fs, opts)?;
		}
		Ok(())
	}

	/// Refuses to open a database in a file system that is not the operating system's if a copy
	/// of an IndexedDB database into it was begun and not finished: a file at the root of the
	/// pool says so. What the files hold then is part of a copy, which the next call of
	/// `migrate_from_indexeddb` removes and makes again, so whatever an application wrote there
	/// would be lost. It is an `Error::MigrationFailed` that names the unfinished copy, and the
	/// open is refused before anything is read or created. The copy opens the database itself
	/// without this check, with `open_async`: only what an application calls goes through it, which
	/// is the builder.
	fn refuse_an_unfinished_import(opts: &Options) -> Result<()> {
		let fs = &*opts.fs;
		if fs.kind() == FsKind::Std {
			return Ok(());
		}
		// A file system that has nothing mounted answers every question with `false`: it is asked
		// to say so first, as `new` does.
		fs.check_available()?;
		if fs.try_exists(Path::new(crate::migrate::INDEXEDDB_MARKER))? {
			return Err(Error::MigrationFailed(format!(
				"the copy of an IndexedDB database into {:?} was begun and not finished (the file \
				 {:?} is at the root of the file system), so what is there is not a database yet. \
				 Call migrate_from_indexeddb again, which makes the copy again, before the database \
				 is opened, or remove the files of the database to start without one.",
				opts.path,
				crate::migrate::INDEXEDDB_MARKER
			)));
		}
		Ok(())
	}

	/// Refuses, in a file system that is not the operating system's, the directory of another
	/// store that the migrators convert on `std`: see `refuse_what_the_file_system_cannot_do`. It
	/// reads the names in the directory, and the first bytes of the three files that a name alone
	/// does not settle, and nothing is created or changed.
	fn refuse_a_directory_that_needs_a_migration(fs: &dyn Fs, opts: &Options) -> Result<()> {
		let refuse = |store: &str| {
			Error::MigrationFailed(format!(
				"{:?} holds {store}, which is converted only in the files of the operating \
				 system, and not in this file system yet. Nothing was read, moved or created.",
				opts.path
			))
		};
		let is_a_dump = |path: &Path| {
			matches!(
				fs.read_prefix(path, surrealkv_compat_indxdb::INDXDB_DUMP_MAGIC.len()),
				Ok(Some(head)) if head == surrealkv_compat_indxdb::INDXDB_DUMP_MAGIC
			)
		};
		// A dump can be the path itself.
		match fs.entry(&opts.path)? {
			Some(crate::vfs::EntryKind::File) if is_a_dump(&opts.path) => {
				return Err(refuse("an IndexedDB dump"));
			}
			Some(crate::vfs::EntryKind::Dir) => {}
			_ => return Ok(()),
		}
		let entries = match fs.read_dir(&opts.path) {
			Ok(entries) => entries,
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
			Err(e) => return Err(e.into()),
		};
		for entry in entries.iter().filter(|entry| entry.kind == crate::vfs::EntryKind::File) {
			let path = opts.path.join(&entry.name);
			match entry.name.to_string_lossy().as_ref() {
				"MIGRATING_ROCKSDB" => {
					return Err(refuse("a RocksDB migration that was cut short"))
				}
				"MIGRATING" => return Err(refuse("a SurrealKV v0.x migration that was cut short")),
				"indxdb_dump.bin" if is_a_dump(&path) => {
					return Err(refuse("an IndexedDB dump"));
				}
				// What RocksDB does: `CURRENT` names the manifest, which is there.
				"CURRENT" => {
					let named = fs
						.read(&path)
						.ok()
						.and_then(|content| String::from_utf8(content).ok())
						.map(|text| text.trim_end_matches(['\n', '\r']).to_string())
						.filter(|name| name.starts_with("MANIFEST-"));
					if named.is_some_and(|name| fs.is_file(&opts.path.join(name))) {
						return Err(refuse("a RocksDB database"));
					}
				}
				_ => {}
			}
		}
		Ok(())
	}

	/// The error of a feature that this file system does not support yet.
	pub(crate) fn unsupported_here(feature: &str) -> Error {
		Error::InvalidArgument(format!("{feature} is not supported on this file system yet"))
	}

	/// Refuses to migrate a database of another store into one that is meant to be encrypted. A
	/// migration moves the old files into a backup directory inside this one, so that a copy of the
	/// old database stays there in the clear, and writes what it migrates without the key. It is
	/// refused before anything is read or moved.
	pub(crate) fn refuse_migration_into_an_encrypted_database(
		opts: &Options,
		source: &str,
	) -> Result<()> {
		if opts.key_manager.is_some() || opts.encryption_context().is_some() {
			return Err(Error::InvalidArgument(format!(
				"A {source} database cannot be migrated into an encrypted database: the migration \
				 keeps a plaintext copy of the old database in this directory and does not \
				 encrypt what it writes. Migrate it without a key manager."
			)));
		}
		Ok(())
	}

	/// Returns `opts` with the value log enabled if the directory already holds value-log files.
	/// Pointers into them would otherwise fail to resolve, and the files would never be collected.
	fn opts_with_existing_value_log(opts: Arc<Options>) -> Result<Arc<Options>> {
		if opts.enable_vlog {
			return Ok(opts);
		}
		let has_files = match opts.fs.read_dir(&opts.vlog_dir()) {
			Ok(entries) => entries
				.iter()
				.any(|e| Path::new(&e.name).extension().is_some_and(|ext| ext == "vlog")),
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
			Err(e) => return Err(e.into()),
		};
		if !has_files {
			return Ok(opts);
		}
		tracing::warn!(
			"{:?} holds value-log files, so the value log is enabled although the options disable it",
			opts.path
		);
		let mut enabled = (*opts).clone();
		enabled.enable_vlog = true;
		Ok(Arc::new(enabled))
	}

	/// Automatically migrates an existing IndexedDB dump to SurrealKV v2 format.
	#[allow(
		clippy::disallowed_methods,
		reason = "the migration of an IndexedDB dump runs only for a native build, behind the file system kind check in Tree::new"
	)]
	fn migrate_from_indxdb(opts: &Options) -> Result<()> {
		Self::refuse_migration_into_an_encrypted_database(opts, "IndexedDB")?;
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
		let fs = &*opts.fs;
		// Create base directory
		fs.create_dir_all(&opts.path)?;

		// Create all subdirectories
		fs.create_dir_all(&opts.sstable_dir())?;
		fs.create_dir_all(&opts.wal_dir())?;
		fs.create_dir_all(&opts.manifest_dir())?;

		// Create VLog directories
		if opts.enable_vlog {
			fs.create_dir_all(&opts.vlog_dir())?;
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
	///   fails with [`Error::InvalidArgument`] before anything is changed. Checkpoints of this
	///   process into the same directory run one after the other.
	///
	/// # Returns
	/// Metadata about the created checkpoint
	///
	/// # Closing
	/// [`Tree::close`] waits for the checkpoints that are running. One that begins after `close`
	/// has fails without touching `checkpoint_dir`.
	///
	/// # Restoring
	/// A checkpoint that begins while [`Tree::restore_from_checkpoint`] runs waits for it, and a
	/// restore waits for the checkpoints that are running. Restores of the same tree take turns.
	pub fn create_checkpoint<P: AsRef<Path>>(
		&self,
		checkpoint_dir: P,
	) -> Result<CheckpointMetadata> {
		// A checkpoint copies and links the files of the database by path.
		self.refuse_on_a_file_system_that_is_not_std("checkpoints")?;
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
	///
	/// Waits for the checkpoints of this tree that are running, and for the flushes, compactions
	/// and rotations of the memtable and the WAL that are, and keeps the ones that begin meanwhile
	/// waiting until the restore is over. [`Tree::close`] waits for a restore that runs, and a
	/// restore that begins once `close` has fails without touching the database.
	pub fn restore_from_checkpoint<P: AsRef<Path>>(
		&self,
		checkpoint_dir: P,
	) -> Result<CheckpointMetadata> {
		self.refuse_on_a_file_system_that_is_not_std("restoring from a checkpoint")?;
		// A checkpoint of this tree that is running copies the files this replaces, and takes the
		// locks below to flush: wait for it, before anything else so that commits are not refused
		// meanwhile, and keep the next one from starting until the restore is done.
		let _checkpoints = self.core.checkpoints.exclude()?;

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

		// A compaction in flight would delete, write over and list tables of the manifest this
		// replaces: wait for it, and keep the next one from starting until the restore is done.
		let _compactions = self.core.inner.compactions.write();

		// A rotation in flight would create a WAL segment in a directory this removes, or, once
		// the WAL is replaced, in the restored one. Held until the WAL is reopened and moved on to
		// a segment of its own.
		let _rotations = self.core.inner.rotations.lock();

		// A group that began before the restore appends its values to the value log, and rolls it
		// over to a new file, from the flusher: keep it from creating one while the directory is
		// removed and written again. Held until the log is reloaded, and no lock is taken in
		// between.
		let vlog_writer = self.core.inner.vlog.as_ref().map(|vlog| (vlog, vlog.hold_writer()));

		// Step 1: Restore files from checkpoint
		let checkpoint = DatabaseCheckpoint::new(Arc::clone(&self.core.inner));
		let metadata = checkpoint.restore_from_checkpoint(checkpoint_dir)?;

		// Step 2: Reload in-memory state to match restored files

		// Read from disk with none of the locks of the state held.
		let new_levels = LevelManifest::new(Arc::clone(&self.core.inner.opts))?;

		// The files of the value log were replaced: the log's writer, its handles and its next
		// file id belong to the ones that are gone. Before the swap below, so that a reader of the
		// restored tables never finds a handle of the old file with the same id.
		if let Some((vlog, writer)) = vlog_writer {
			vlog.reload(writer)?;
		}

		// Swap the manifest, the active memtable and the immutable queue in one step, under all
		// three locks in the order the rest of the code takes them: a reader, a flush or a
		// compaction sees the old state or the restored one, never the restored tables under a
		// memtable from before the restore (which is discarded, as the restore intends).
		let replaced = {
			let inner = &self.core.inner;
			let mut active_memtable = inner.active_memtable.write()?;
			let mut levels_guard = inner.level_manifest.write()?;
			let mut immutable_memtables = inner.immutable_memtables.write()?;
			// Under the active memtable's write lock: a commit group that applies or publishes
			// under its read guard did so before this, and anything it does after is refused.
			self.core.commit_pipeline.fence_inflight();
			let replaced = (
				std::mem::replace(&mut *levels_guard, new_levels),
				std::mem::replace(
					&mut *active_memtable,
					Arc::new(MemTable::new(inner.opts.max_memtable_size)),
				),
				std::mem::take(&mut *immutable_memtables),
			);
			inner.memory_controller.set_active_memtable_bytes(0);
			inner.memory_controller.set_immutable_memtable_bytes(0);
			// Under the locks, so that nothing reads a restored table before the blocks cached
			// for the tables and the value-log files that had its id are gone.
			inner.opts.block_cache.clear();
			replaced
		};
		// Dropped with no lock held
		drop(replaced);

		#[cfg(test)]
		{
			let hook = self.core.inner.checkpoint_hook.lock().clone();
			if let Some(hook) = hook {
				hook(crate::checkpoint::CheckpointStage::RestoreManifestSwapped);
			}
		}

		// Reopen the WAL from the restored directory
		let wal_path = self.core.inner.opts.path.join("wal");
		let manifest_log_number = self.core.inner.level_manifest.read()?.get_log_number();

		{
			let mut wal_guard = self.core.inner.wal.write();
			let new_wal = Wal::open_with_min_log_number(
				&wal_path,
				manifest_log_number,
				wal::Options::for_tree(&self.core.inner.opts)?,
			)?;
			*wal_guard = new_wal;
		}

		// Replay any WAL entries that were restored
		let mut tail = self.core.inner.vlog.as_deref().map(TailCheck::new);
		let (wal_seq_num_opt, recovered_memtable, restore_report, _) = Core::replay_wal_in(
			&wal::WalContext::for_options(&self.core.inner.opts)?,
			&wal_path,
			manifest_log_number,
			"Database restore",
			self.core.inner.opts.wal_recovery_mode,
			self.core.inner.opts.max_memtable_size,
			tail.as_mut(),
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

		*self.core.inner.recovery_report.write() = restore_report;

		// The restored directory gets segments with sync marks from here on, see `Core::new`.
		self.core.inner.level_manifest.write()?.upgrade_format_in(&*self.core.inner.opts.fs)?;

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

		// The queue of immutable memtables and the level-0 tables that a writer was stalled on are
		// the restored ones now, and nothing else wakes a writer on an idle tree. Woken with the
		// locks released, so that it commits instead of finding the restore still running.
		#[allow(clippy::used_underscore_binding)]
		{
			drop(_rotations);
			drop(_compactions);
			drop(_flush);
			drop(_write_guard);
		}
		self.core.write_stall.signal_work_done();

		Ok(metadata)
	}

	/// Refuses `feature`, which copies, links or replaces the files of the database by path, on a
	/// file system that is not the operating system's, before anything is touched.
	fn refuse_on_a_file_system_that_is_not_std(&self, feature: &str) -> Result<()> {
		if self.core.inner.opts.fs.kind() != FsKind::Std {
			return Err(Self::unsupported_here(feature));
		}
		Ok(())
	}

	/// Asks the key manager again for the keys this database uses: the key that new files are
	/// written under, and every key that its files were found to use.
	///
	/// A database asks for them when it is opened, and creating a file afterwards, a segment of the
	/// log, a file of the value log or a table, never waits on a key provider. Call this after the
	/// key manager changed which key new files are written under, so that the new key is there
	/// before a commit needs it, and after a commit failed with a key provider that was away, once
	/// it is back. It may wait on the provider, and does nothing for a database that is not
	/// encrypted.
	///
	/// Kept out of the documented interface until its shape is settled.
	#[doc(hidden)]
	pub fn prepare_keys(&self) -> Result<()> {
		self.core.inner.prepare_keys()
	}

	/// Says whether the database is encrypted, with which suite, and which keys cover how many
	/// files and bytes of each kind. It is made from the headers at the start of the files and
	/// from the key manager, and reads no block and no key: it opens each file, one after the
	/// other, to read its first bytes, so call it where a blocking call is allowed.
	///
	/// It is what to ask before a key is destroyed, see [`EncryptionStatus`]. The state of every
	/// key id that a file names, and of the active one, is the key manager's. A database that
	/// is not encrypted is answered at once, with nothing counted.
	///
	/// The shape of the answer is not settled, which is why it is
	/// hidden from the documentation with the types that it is made of.
	#[doc(hidden)]
	pub fn encryption_status(&self) -> Result<crate::EncryptionStatus> {
		let opts = &self.core.inner.opts;
		match opts.encryption_context() {
			None => Ok(crate::EncryptionStatus::not_encrypted()),
			Some(context) => crate::encryption_status::scan_database(
				&opts.path,
				Some(context.key_manager().as_ref()),
				Some((context.db_id(), context.suite())),
			),
		}
	}

	/// Closes the tree. Commits already admitted finish first, each with its real outcome, and a
	/// commit that begins once the commit pipeline is shut down fails with
	/// [`Error::PipelineStall`]. Checkpoints that are running, and a restore that runs, finish
	/// first, before the pipeline is shut down, and one that begins after the close fails.
	pub async fn close(&self) -> Result<()> {
		self.core.close().await
	}

	/// Ends the tree the way a crash does: its tasks are aborted, its lock is released, and nothing
	/// is flushed, no log is synced and no manifest is written. Commits that are in flight end with
	/// `Error::PipelineStall`, and whether they reached the log is not said. For the tests of a
	/// file system that crashes, which then remount it and open the database again: it is not
	/// part of the interface of the crate.
	#[doc(hidden)]
	pub fn abandon(&self) {
		self.core.abandon();
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

	/// Compacts the keys of `[start, end)` down the tree, blocking until it is done. `None` leaves
	/// a bound open, so `compact_range(None, None, ..)` compacts the whole tree.
	///
	/// Memtables are flushed first when [`CompactRangeOptions::flush_memtable`] is set, since only
	/// tables are compacted. Then, one level at a time from level 0, every table that overlaps the
	/// range is merged into the next level, down to [`CompactRangeOptions::target_level`]. A table
	/// that only partly overlaps is merged whole, as are the tables that share a user key with it.
	/// Merging into the last level drops the tombstones and the versions that no reader can see,
	/// which is what frees the space of deleted data.
	///
	/// The compaction runs between the steps of the background compactor, never at the same time
	/// as one, and writes and reads carry on meanwhile. Data that is written after the call began
	/// may be left uncompacted. It keeps every version a snapshot or a transaction can still see.
	///
	/// This is blocking file I/O: call it from `spawn_blocking` in an async context. A failure
	/// leaves the tree as it was before the step that failed, and the steps that finished stay
	/// done. A tree that is closing makes it return [`Error::DatabaseStopped`].
	///
	/// # Errors
	///
	/// [`Error::InvalidArgument`] if `start` is greater than `end`.
	pub fn compact_range(
		&self,
		start: Option<&[u8]>,
		end: Option<&[u8]>,
		options: &CompactRangeOptions,
	) -> Result<()> {
		let inner = &self.core.inner;
		if let (Some(start), Some(end)) = (start, end) {
			if inner.opts.comparator.compare(start, end) == std::cmp::Ordering::Greater {
				return Err(Error::InvalidArgument("compact_range: start is after end".into()));
			}
		}
		let stopped = || {
			self.core.is_closed.load(Ordering::SeqCst)
				|| self.core.inner.error_handler.commit_group_error().is_some()
		};
		let stopped_error = || Error::DatabaseStopped("compact_range: the tree is closing".into());
		if stopped() {
			return Err(stopped_error());
		}

		if options.flush_memtable {
			let has_data = !inner.active_memtable.read()?.is_empty();
			if has_data {
				inner.rotate_memtable()?;
			}
			inner.flush_all_immutables_sync()?;
		}

		let last = inner.opts.level_count.saturating_sub(1);
		let target = options.target_level.map_or(last, |level| level.min(last));
		let comparator = || Arc::clone(&inner.opts.comparator);
		let run = |source: u8, same_level: bool| -> Result<()> {
			if stopped() {
				return Err(stopped_error());
			}
			let strategy = RangeStrategy::new(source, same_level, start, end, comparator());
			self.core.inner.compact(Arc::new(strategy))?;
			self.core.write_stall.signal_work_done();
			Ok(())
		};
		for source in 0..target {
			run(source, false)?;
		}
		if target == last && options.rewrite_bottommost {
			run(last, true)?;
		}
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

	/// Returns the report of actions taken during WAL recovery at database open.
	pub fn recovery_report(&self) -> WalRecoveryReport {
		self.core.inner.recovery_report.read().clone()
	}

	/// Returns the current background error, if any.
	pub fn background_error(&self) -> Option<crate::error::BackgroundError> {
		self.core.inner.error_handler.get_error()
	}

	/// Returns the total count of background errors encountered.
	pub fn background_error_count(&self) -> u64 {
		self.core.inner.error_handler.error_count()
	}

	/// Returns true if the database is stopped due to a fatal or unrecoverable background error.
	pub fn is_stopped(&self) -> bool {
		self.core.inner.error_handler.is_db_stopped()
	}
}

impl Drop for Tree {
	fn drop(&mut self) {
		#[cfg(not(target_arch = "wasm32"))]
		{
			// Only attempt async shutdown if the core is not already closed
			if !self.core.is_closed.load(Ordering::SeqCst) {
				// Clone the Arc to move into the async task
				let core = Arc::clone(&self.core);
				let spawned = rt::try_spawn(async move {
					if let Err(err) = core.close_with(false).await {
						tracing::error!("Error closing store: {}", err);
					}
				});
				if !spawned {
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

	/// Puts the database in `fs`: see [`Options::with_file_system`].
	pub fn with_file_system(mut self, fs: crate::FileSystem) -> Self {
		self.opts = self.opts.with_file_system(fs);
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

	/// Enables or disables versioned queries: every write is stamped with its timestamp, and
	/// versions are kept for `Transaction::get_at` and `Transaction::history`, for `retention_ns`
	/// nanoseconds, or for ever if that is 0. See `Options::with_versioning`.
	pub fn with_versioning(mut self, enable: bool, retention_ns: u64) -> Self {
		self.opts = self.opts.with_versioning(enable, retention_ns);
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

	/// Creates the database encrypted at rest with the keys of `key_manager`, or opens the one that
	/// is. It is [`Options::with_encryption`] for a builder: every file of the database is sealed,
	/// the manifest, the tables, the value log and the write-ahead log.
	///
	/// # Creating and opening
	///
	/// Whether a database is encrypted is decided once, when it is created, and is read from its
	/// manifest every time it is opened.
	///
	/// - At a path that holds no database, `build` creates one that is encrypted. It is given an
	///   identity of its own and the cipher suite that `cipher` names, and `Auto` is resolved then
	///   and never again: AES-256-GCM on a machine with hardware support for it, XChaCha20-Poly1305
	///   on one without. Both are written to the manifest, which is the first file of the database,
	///   sealed under the key that `key_manager` says is active. No file exists under an identity
	///   before the manifest that names it is on disk.
	/// - At the path of a database that is encrypted, `build` opens it, with the suite it was
	///   created with, and `cipher` is not looked at. The manager must hold the key that sealed the
	///   manifest, and every other key that a file of the database names. A key that does not fit
	///   is refused by name (`Error::Encryption` with `WrongKey`, `UnknownKeyId` or `KeyRetired`)
	///   before anything is changed, and a database is never repaired or deleted because of a key.
	/// - At the path of a database that was created without a key, `build` fails with
	///   `NotEncrypted`: encryption is never added to a database that exists, and no migration from
	///   another store is done into an encrypted database.
	/// - A database that is encrypted cannot be opened without a key manager: `build` fails with
	///   `NotConfigured`.
	///
	/// # Keys
	///
	/// The key manager owns the keys, and the database keeps none: a root key is not written to any
	/// file. **A key that is lost is data that is lost.** There is no copy to recover it from, and
	/// every file that was written under a key is unreadable without it, the checkpoints and
	/// backups of the database included. New files are written under the key that the manager
	/// says is active, and an older key must stay with the manager for as long as any file or any
	/// checkpoint that was written under it is wanted: see `Tree::encryption_status`.
	///
	/// # Feature
	///
	/// Encryption needs the `encryption` feature, which is on by default. Without it,
	/// `build` with a key manager fails with `Error::InvalidArgument` and creates nothing, and the
	/// formats of the files are not final: do not use it for data that is meant to be kept.
	pub fn with_encryption(
		mut self,
		key_manager: Arc<dyn crate::tde::KeyManager>,
		cipher: impl Into<crate::tde::CipherPreference>,
	) -> Self {
		self.opts = self.opts.with_encryption(key_manager, cipher);
		self
	}

	/// Builds the LSM tree with the configured options.
	///
	/// This method ensures type safety by using the same key type K
	/// for both the builder and the resulting tree.
	pub fn build(self) -> Result<Tree> {
		Tree::refuse_an_unfinished_import(&self.opts)?;
		Tree::new(Arc::new(self.opts))
	}

	/// Builds the LSM tree: the entry point for a browser, and the same as [`build`](Self::build)
	/// on a native build for now.
	///
	/// A file system that has nothing mounted is refused first, with an error that says what to
	/// do, before anything is read or created.
	///
	/// On a pool of files (a browser's, or the memory file system), the open waits until the pool
	/// has the files that it creates: its first manifest, the log segments of the open, and a table
	/// and a manifest for each memtable that a recovery flushes, which is counted from the log
	/// that is there. If the pool turns out to have too few, the open is done again with twice as
	/// many, up to sixteen times, which is safe because a database that was opened and failed is
	/// what a crash during the recovery leaves. A pool that is at its limit of files fails the open
	/// at once, with an error that says so.
	pub async fn build_async(self) -> Result<Tree> {
		Tree::refuse_an_unfinished_import(&self.opts)?;
		Tree::open_async(Arc::new(self.opts)).await
	}

	/// [`build_async`](Self::build_async), returning the options as well, as
	/// [`build_with_options`](Self::build_with_options) does.
	pub async fn build_with_options_async(self) -> Result<(Tree, Arc<Options>)> {
		Tree::refuse_an_unfinished_import(&self.opts)?;
		let opts = Arc::new(self.opts);
		let tree = Tree::open_async(Arc::clone(&opts)).await?;
		Ok((tree, opts))
	}

	/// Builds the LSM tree and returns both the tree and the options.
	///
	/// This is useful when you need to keep a reference to the options
	/// after creating the tree.
	pub fn build_with_options(self) -> Result<(Tree, Arc<Options>)> {
		Tree::refuse_an_unfinished_import(&self.opts)?;
		// The options as they were given, and not the ones the tree runs with: those carry the
		// identity of the database they were opened for, and a caller that moves them to another
		// path would make a second database with the first one's identity.
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
#[allow(
	clippy::disallowed_methods,
	reason = "this is the body of StdFs::sync_dir and of the tools that run on std::fs; no other file system calls it"
)]
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
	let fs = &*opts.fs;
	fs.sync_dir(&opts.sstable_dir()).map_err(|e| {
		Error::Other(format!(
			"Failed to sync SSTable directory '{}': {}",
			opts.sstable_dir().display(),
			e
		))
	})?;

	fs.sync_dir(&opts.wal_dir()).map_err(|e| {
		Error::Other(format!("Failed to sync WAL directory '{}': {}", opts.wal_dir().display(), e))
	})?;

	fs.sync_dir(&opts.manifest_dir()).map_err(|e| {
		Error::Other(format!(
			"Failed to sync manifest directory '{}': {}",
			opts.manifest_dir().display(),
			e
		))
	})?;

	// Sync VLog directories
	if opts.enable_vlog {
		fs.sync_dir(&opts.vlog_dir()).map_err(|e| {
			Error::Other(format!(
				"Failed to sync vlog directory '{}': {}",
				opts.vlog_dir().display(),
				e
			))
		})?;
	}

	fs.sync_dir(&opts.path).map_err(|e| {
		Error::Other(format!("Failed to sync base directory '{}': {}", opts.path.display(), e))
	})?;

	Ok(())
}

// ===== VLog and Versioned Index Cleanup Helpers =====

/// The bound of a VLog cleanup: the smallest file id that a table or a memtable that is not
/// flushed yet points into. A file below it holds nothing that can be read any more. It is
/// `u32::MAX` when tables were written and nothing points into the VLog, any more or ever: every
/// file but the active one is below it, see [`cleanup_vlog`]. It is 0, and nothing is deleted,
/// while no table was written yet, or a table of an older engine opened from disk may point into
/// the VLog without saying so, see `LevelManifest::vlog_floor_of_tables`.
///
/// The active slot is read-locked first and stays locked until the end, then the manifest and the
/// queue, together, in the order a flush takes them in. The slot is read last: a batch applied
/// before a table was registered is counted, and a rotation, which needs the slot exclusively,
/// cannot move the memtable between the queue and the slot while the bound is read. A flush
/// registers its table and takes the memtable out of the queue under both locks, so the memtable
/// of a flush that is under way stays counted until its table is in the manifest.
///
/// The files of a batch that is encoded but not in a memtable or a table yet are not in the
/// bound: [`cleanup_vlog`] takes the pin of the commit group with it, which the flusher holds from
/// before the first value of the group is appended until the group is applied (see
/// `VLog::unregistered_floor`). A group that fails after it was logged lowers the active
/// memtable's bound, since recovery replays its records, before its pin is released.
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
		.fold(manifest.vlog_floor_of_tables(), u32::min))
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
	// Before the bound: it is what keeps the files of a commit group that no table and no
	// memtable points into yet, see `VLog::unregistered_floor`.
	let unregistered = vlog.unregistered_floor();
	let floor = match vlog_floor(active, manifest, immutables) {
		Ok(floor) => floor.min(unregistered),
		Err(e) => {
			tracing::warn!("Failed to cleanup obsolete vlog files during {}: {}", context, e);
			return;
		}
	};

	// Nothing is below 0: no table was written yet (fresh database), or one may point into the
	// VLog without saying so.
	if floor == 0 {
		return;
	}

	// Delete obsolete VLog files
	if let Err(e) = vlog.cleanup_obsolete_files(floor) {
		tracing::warn!("Failed to cleanup obsolete vlog files during {}: {}", context, e);
	}
}
