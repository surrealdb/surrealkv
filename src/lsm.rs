use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;

use crate::batch::Batch;
use crate::bplustree::tree::DiskBPlusTree;
use crate::checkpoint::{CheckpointMetadata, DatabaseCheckpoint};
use crate::commit::{CommitEnv, CommitPipeline};
use crate::compaction::compactor::{CompactionOptions, Compactor};
use crate::compaction::CompactionStrategy;
use crate::error::{BackgroundErrorHandler, BackgroundErrorReason, Result};
use crate::levels::{write_manifest_to_disk, LevelManifest, ManifestChangeSet};
use crate::lockfile::LockFile;
use crate::memtable::{ImmutableEntry, ImmutableMemtables, MemTable};
use crate::snapshot::SnapshotTracker;
use crate::sstable::table::Table;
use crate::task::TaskManager;
use crate::transaction::{Mode, Transaction, TransactionOptions};
use crate::vlog::{VLog, ValueLocation, ValuePointer};
use crate::wal::recovery::{repair_corrupted_wal_segment, replay_wal};
use crate::wal::{self, cleanup_old_segments, Wal, WalManager};
use crate::{
	BytewiseComparator,
	Comparator,
	Error,
	FilterPolicy,
	LSMIterator,
	Options,
	TimestampComparator,
	VLogChecksumLevel,
	Value,
	WalRecoveryMode,
};

// ===== Compaction Operations Trait =====
/// Defines the compaction operations that can be performed on an LSM tree.
/// Compaction is essential for maintaining read performance by merging
/// overlapping SSTables and removing deleted entries.
#[async_trait]
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

	/// Value Log (VLog)
	pub(crate) vlog: Option<Arc<VLog>>,

	/// Write-Ahead Log (WAL) for durability
	pub(crate) wal: WalManager,

	/// Versioned B+ tree index for timestamp-based queries
	/// Maps InternalKey -> Value for time-range queries
	pub(crate) versioned_index: Option<Arc<parking_lot::RwLock<DiskBPlusTree>>>,

	/// Lock file to prevent multiple processes from opening the same database
	pub(crate) lockfile: Mutex<LockFile>,

	/// Background error handler
	pub(crate) error_handler: Arc<BackgroundErrorHandler>,

	/// Visible sequence number - the highest sequence number that is visible to readers.
	/// Shared with CommitPipeline for coordinated updates.
	/// Used to set `earliest_seq` when creating new memtables for conflict detection.
	pub(crate) visible_seq_num: Arc<AtomicU64>,
}

impl CoreInner {
	/// Creates a new LSM tree core instance
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		// Acquire database lock to prevent multiple processes from opening the same
		// database
		let mut lockfile = LockFile::new(&opts.path);
		lockfile.acquire()?;

		// Initialize immutable memtables
		let immutable_memtables = Arc::new(RwLock::new(ImmutableMemtables::default()));

		// Initialize level manifest FIRST to get log_number
		let manifest = LevelManifest::new(Arc::clone(&opts))?;
		let manifest_log_number = manifest.get_log_number();

		// Initialize WAL starting from manifest.log_number
		// This avoids creating intermediate empty WAL files
		let wal_path = opts.wal_dir();
		let wal_instance =
			Wal::open_with_min_log_number(&wal_path, manifest_log_number, wal::Options::default())?;

		// Starts at 0 since no commits have happened yet.
		let visible_seq_num = Arc::new(AtomicU64::new(0));

		// Initialize active memtable with its WAL number set to the initial WAL
		// This tracks which WAL the memtable's data belongs to for later flush
		let initial_memtable = Arc::new(MemTable::new(opts.max_memtable_size, 0));
		initial_memtable.set_wal_number(wal_instance.get_active_log_number());
		let active_memtable = Arc::new(RwLock::new(initial_memtable));

		let level_manifest = Arc::new(RwLock::new(manifest));

		// Initialize versioned index if B+tree versioned index is enabled
		let versioned_index = if opts.enable_versioned_index {
			// Create the versioned index directory if it doesn't exist
			let versioned_index_dir = opts.versioned_index_dir();
			let versioned_index_path = versioned_index_dir.join("index.bpt");
			let comparator =
				Arc::new(TimestampComparator::new(Arc::new(BytewiseComparator::default())));
			let tree = DiskBPlusTree::disk(&versioned_index_path, comparator)?;
			Some(Arc::new(parking_lot::RwLock::new(tree)))
		} else {
			None
		};

		let vlog = if opts.enable_vlog {
			Some(Arc::new(VLog::new(Arc::clone(&opts))?))
		} else {
			None
		};

		Ok(Self {
			opts,
			active_memtable,
			immutable_memtables,
			level_manifest,
			snapshot_tracker: SnapshotTracker::new(),
			vlog,
			wal: WalManager::new(wal_instance),
			versioned_index,
			lockfile: Mutex::new(lockfile),
			error_handler: Arc::new(BackgroundErrorHandler::new()),
			visible_seq_num,
		})
	}

	/// Get the earliest sequence number across all memtables.
	/// Any key with seq >= this value is guaranteed to be in memtables.
	/// Keys modified with seq < this value may have been flushed to SST.
	pub(crate) fn get_earliest_memtable_seq(&self) -> Result<u64> {
		// Check immutable memtables - the first (oldest by table_id) has the earliest seq
		let immutables = self.immutable_memtables.read()?;
		if let Some(oldest) = immutables.first() {
			return Ok(oldest.memtable.earliest_seq());
		}
		drop(immutables);

		// No immutables - use active memtable
		let memtable = self.active_memtable.read()?;
		Ok(memtable.earliest_seq())
	}

	pub(crate) fn check_keys_conflict<'a, I>(&self, keys: I, start_seq: u64) -> Result<()>
	where
		I: Iterator<Item = &'a [u8]>,
	{
		// Acquire locks once for all keys - this is the key optimization
		let memtable = self.active_memtable.read()?;
		let immutables = self.immutable_memtables.read()?;

		for key in keys {
			// Check active memtable first (most recent writes)
			if let Some((ikey, _)) = memtable.get(key, None) {
				if ikey.seq_num() > start_seq {
					return Err(Error::TransactionWriteConflict);
				}
				// Key exists but was written before our transaction started - no conflict
				continue;
			}

			// Check immutable memtables (newest to oldest by iterating in reverse)
			for entry in immutables.iter().rev() {
				if let Some((ikey, _)) = entry.memtable.get(key, None) {
					if ikey.seq_num() > start_seq {
						return Err(Error::TransactionWriteConflict);
					}
					// Key exists but was written before our transaction started - no conflict
					break;
				}
			}
			// Key not found in any memtable - no conflict for this key
		}

		// No conflicts found for any key
		Ok(())
	}

	/// Flushes a memtable to SST and atomically updates the manifest.
	///
	/// This is the core primitive used by all flush operations. It handles:
	/// 1. Flushing the memtable to disk as an SSTable
	/// 2. Creating a changeset with the new SST and log_number update
	/// 3. Atomically applying the changeset to the manifest
	/// 4. Removing the memtable from immutable_memtables tracking
	///
	/// # Arguments
	/// * `memtable` - The memtable to flush
	/// * `table_id` - Table ID for the new SST
	/// * `wal_number` - WAL number to mark as flushed (log_number = wal_number + 1)
	///
	/// # Returns
	/// The flushed SSTable
	fn flush_and_update_manifest(
		&self,
		memtable: &MemTable,
		table_id: u64,
		wal_number: u64,
	) -> Result<Arc<Table>> {
		let collect_bptree = self.versioned_index.is_some();

		// Step 1: Flush memtable to SST (with VLog separation for large values)
		// Also collects entries for B+tree versioned index if enabled
		let (table, bptree_entries) = memtable
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

		log::debug!("Created SST table_id={}, file_size={}", table.id, table.file_size);

		// Step 2: Write to versioned index (B+tree) with vlog-separated values
		// Note: Replace entries are NOT cleaned up here. The HistoryIterator uses
		// barrier logic (barrier_seen) to skip older entries when it encounters a
		// Replace — same as how hard deletes work. Stale entries are eventually
		// removed by cleanup_stale_versioned_index when their vlog files are cleaned.
		if let Some(ref versioned_index) = self.versioned_index {
			let mut vi_guard = versioned_index.write();

			for (encoded_key, encoded_value) in &bptree_entries {
				vi_guard.insert(encoded_key.clone(), encoded_value.clone())?;
			}

			vi_guard.sync()?;
			log::debug!(
				"Versioned index updated: {} entries written for table_id={}",
				bptree_entries.len(),
				table_id
			);
		}

		// Step 3: Prepare atomic changeset
		let mut changeset = ManifestChangeSet::default();
		changeset.new_tables.push((0, Arc::clone(&table)));
		changeset.log_number = Some(wal_number + 1);

		log::debug!(
			"Changeset prepared: table_id={}, log_number={} (WAL #{:020} flushed)",
			table_id,
			wal_number + 1,
			wal_number
		);

		// Step 4: Apply changeset atomically
		let mut manifest = self.level_manifest.write()?;
		let mut memtable_lock = self.immutable_memtables.write()?;

		let rollback = manifest.apply_changeset(&changeset)?;
		if let Err(e) = write_manifest_to_disk(&manifest) {
			manifest.revert_changeset(rollback);
			let error = Error::Other(format!(
				"Failed to atomically update manifest: table_id={}, log_number={}: {}",
				table_id,
				wal_number + 1,
				e
			));
			self.error_handler.set_error(error.clone(), BackgroundErrorReason::ManifestWrite);
			return Err(error);
		}

		// Remove successfully flushed memtable from tracking
		memtable_lock.remove(table_id);

		log::info!(
			"Manifest updated atomically: table_id={}, log_number={}, last_sequence={}",
			table_id,
			wal_number + 1,
			manifest.get_last_sequence()
		);

		// After successful manifest commit, cleanup obsolete vlog files and stale index entries
		let min_oldest_vlog = manifest.min_oldest_vlog_file_id();
		cleanup_vlog_and_index(&self.vlog, &self.versioned_index, min_oldest_vlog, "flush");

		Ok(table)
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
		let mut active_memtable = self.active_memtable.write()?;

		if active_memtable.is_empty() {
			return Ok(());
		}

		log::debug!("rotate_memtable: rotating memtable size={}", active_memtable.size());

		// Step 2: Rotate WAL while STILL holding memtable write lock
		let (flushed_wal_number, new_wal_number) = {
			let mut wal_guard = self.wal.write();
			let old_log_number = wal_guard.get_active_log_number();
			wal_guard.rotate().map_err(|e| {
				Error::Other(format!("Failed to rotate WAL before memtable rotation: {}", e))
			})?;
			let new_log_number = wal_guard.get_active_log_number();
			drop(wal_guard);

			log::debug!(
				"WAL rotated during memtable rotation: {} -> {}",
				old_log_number,
				new_log_number
			);
			(old_log_number, new_log_number)
		};

		// Step 3: Swap memtable while STILL holding write lock
		let earliest_seq = self.visible_seq_num.load(Ordering::Acquire);
		let flushed_memtable = std::mem::replace(
			&mut *active_memtable,
			Arc::new(MemTable::new(self.opts.max_memtable_size, earliest_seq)),
		);

		// Set the WAL number on the new (empty) active memtable
		active_memtable.set_wal_number(new_wal_number);

		// Get table ID and track immutable memtable with its WAL number
		let mut immutable_memtables = self.immutable_memtables.write()?;
		let table_id = self.level_manifest.read()?.next_table_id();
		immutable_memtables.add(table_id, flushed_wal_number, Arc::clone(&flushed_memtable));

		// Release locks
		drop(active_memtable);
		drop(immutable_memtables);

		log::debug!(
			"rotate_memtable: completed rotation, table_id={}, wal_number={}",
			table_id,
			flushed_wal_number
		);

		Ok(())
	}

	/// Flushes the oldest immutable memtable to an SSTable.
	/// Returns Ok(Some(table)) if a memtable was flushed, Ok(None) if queue was empty.
	///
	/// This method:
	/// 1. Gets the oldest entry from immutable queue (lowest table_id)
	/// 2. Flushes it to SST via flush_and_update_manifest (which also removes from queue)
	/// 3. Schedules async WAL cleanup
	fn flush_oldest_immutable_to_sst(&self) -> Result<Option<Arc<Table>>> {
		// Get the oldest immutable entry (clone to release lock before I/O)
		let entry = {
			let guard = self.immutable_memtables.read()?;
			guard.first().cloned()
		};

		let entry = match entry {
			Some(e) => e,
			None => {
				log::debug!("flush_oldest_immutable_to_sst: no immutables to flush");
				return Ok(None);
			}
		};

		// Skip empty memtables
		if entry.memtable.is_empty() {
			let mut guard = self.immutable_memtables.write()?;
			guard.remove(entry.table_id);
			log::debug!(
				"flush_oldest_immutable_to_sst: skipped empty memtable table_id={}",
				entry.table_id
			);
			return Ok(None);
		}

		log::debug!(
			"flush_oldest_immutable_to_sst: flushing table_id={}, wal_number={}",
			entry.table_id,
			entry.wal_number
		);

		// Flush to SST (this also removes from immutable queue and updates manifest)
		let table =
			self.flush_and_update_manifest(&entry.memtable, entry.table_id, entry.wal_number)?;

		// Schedule async WAL cleanup
		let wal_dir = self.wal.read().get_dir_path().to_path_buf();
		let min_wal_to_keep = entry.wal_number + 1;

		tokio::spawn(async move {
			match cleanup_old_segments(&wal_dir, min_wal_to_keep) {
				Ok(count) if count > 0 => {
					log::info!(
						"Cleaned up {} old WAL segments (min_wal_to_keep={})",
						count,
						min_wal_to_keep
					);
				}
				Ok(_) => {}
				Err(e) => {
					log::warn!("Failed to clean up old WAL segments: {}", e);
				}
			}
		});

		log::debug!(
			"flush_oldest_immutable_to_sst: flushed table_id={}, file_size={}",
			table.id,
			table.file_size
		);

		Ok(Some(table))
	}

	/// Flushes ALL immutable memtables synchronously.
	/// Used by Tree::flush() and checkpoint for forced/sync flush.
	/// Blocks until all immutables are written to SST.
	pub(crate) fn flush_all_immutables_sync(&self) -> Result<()> {
		let mut count = 0;
		while self.flush_oldest_immutable_to_sst()?.is_some() {
			count += 1;
		}
		if count > 0 {
			log::debug!("flush_all_immutables_sync: flushed {} immutable memtables", count);
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

		let mut immutable_memtables = self.immutable_memtables.write()?;

		// Get the current WAL number for the new memtable
		let current_wal_number = self.wal.read().get_active_log_number();

		// Get the current visible_seq_num as earliest_seq for the new memtable
		let earliest_seq = self.visible_seq_num.load(Ordering::Acquire);

		// Swap the active memtable with a new empty one
		// This allows writes to continue immediately
		let flushed_memtable = std::mem::replace(
			&mut *active_memtable,
			Arc::new(MemTable::new(self.opts.max_memtable_size, earliest_seq)),
		);

		// Set the WAL number on the new active memtable
		active_memtable.set_wal_number(current_wal_number);

		// Get table ID for the SST file
		let table_id = self.level_manifest.read()?.next_table_id();

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
		let table =
			self.flush_and_update_manifest(&flushed_memtable, table_id, wal_that_was_flushed)?;

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
		log::info!("Flushing all memtables for shutdown...");

		// STEP 1: Flush ALL immutable memtables FIRST (older data, lower table_ids)
		// We need to collect them first to avoid holding the lock during I/O
		let immutables_to_flush: Vec<ImmutableEntry> = {
			let immutable_guard = self.immutable_memtables.read()?;
			immutable_guard.iter().cloned().collect()
		};

		let immutable_count = immutables_to_flush.len();
		if immutable_count > 0 {
			log::info!("Flushing {} immutable memtable(s) first (older data)", immutable_count);
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
				log::debug!("Skipped empty immutable memtable: table_id={}", entry.table_id);
				continue;
			}

			// Fail-fast: return immediately on error
			// WAL replay will recover this and subsequent memtables on restart
			self.flush_and_update_manifest(&entry.memtable, entry.table_id, entry.wal_number)?;

			flushed_count += 1;
			log::debug!(
				"Flushed immutable memtable {}/{}: table_id={}, wal_number={}",
				flushed_count,
				immutable_count,
				entry.table_id,
				entry.wal_number
			);
		}

		if flushed_count > 0 {
			log::info!("Flushed {} immutable memtable(s) successfully", flushed_count);
		}

		// STEP 2: Flush active memtable LAST (newest data, gets highest table_id)
		let active_memtable = self.active_memtable.read()?;
		let active_size = active_memtable.size();
		let active_is_empty = active_memtable.is_empty();
		drop(active_memtable);

		if !active_is_empty {
			log::info!("Flushing active memtable last (newest data): size={}", active_size);

			// Use flush_memtable_and_update_manifest which:
			// - Gets a new (highest) table_id
			// - Updates log_number to mark WAL as flushed
			// - Does NOT rotate WAL (we pass None)
			// Fail-fast: return immediately on error
			match self.flush_memtable_and_update_manifest(None)? {
				Some(table) => {
					log::info!(
						"Active memtable flushed: table_id={}, file_size={}",
						table.id,
						table.file_size
					);
				}
				None => {
					log::debug!("Active memtable was empty, skipped flush");
				}
			}
		} else {
			log::debug!("Active memtable is empty, skipping flush");

			// Even if active is empty, we should update log_number if we flushed immutables
			// This marks the WAL as safe to delete
			if flushed_count > 0 {
				let current_wal = self.wal.read().get_active_log_number();
				let changeset = ManifestChangeSet {
					log_number: Some(current_wal + 1),
					..Default::default()
				};

				let mut manifest = self.level_manifest.write()?;
				let rollback = manifest.apply_changeset(&changeset)?;
				if let Err(e) = write_manifest_to_disk(&manifest) {
					manifest.revert_changeset(rollback);
					let error = Error::Other(format!(
						"Failed to update manifest log_number after immutable flush: {}",
						e
					));
					self.error_handler
						.set_error(error.clone(), BackgroundErrorReason::ManifestWrite);
					return Err(error);
				}

				log::debug!(
					"Updated manifest log_number to {} after immutable flushes",
					current_wal + 1
				);
			}
		}

		log::info!("All memtables flushed successfully for shutdown");
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
								log::info!("Removed orphaned SST file: table_id={}", table_id);
							}
							Err(e) => {
								log::warn!(
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
			log::info!("Cleaned up {} orphaned SST files", removed_count);
		} else {
			log::debug!("No orphaned SST files found");
		}

		Ok(())
	}

	/// Cleans up orphaned VLog files that are not referenced by any SST.
	///
	/// After a crash, there may be VLog files that:
	/// 1. Were written but never referenced by an SST (write crashed before flush)
	/// 2. Are no longer referenced because all referencing SSTs were compacted away
	///
	/// This method computes the minimum oldest_vlog_file_id across all live SSTs
	/// and removes any VLog files below that threshold.
	///
	/// SAFETY: This must be called after manifest is loaded and SSTs are known.
	fn cleanup_orphaned_vlog_files(&self) -> Result<()> {
		if self.vlog.is_none() {
			return Ok(()); // No VLog, nothing to clean up
		}

		let manifest = self.level_manifest.read()?;
		let min_oldest_vlog = manifest.min_oldest_vlog_file_id();

		// If no SSTs reference VLog files yet, keep all files
		// (This handles the fresh database case)
		if min_oldest_vlog == 0 {
			log::debug!("No SSTs with VLog references found, skipping VLog orphan cleanup");
			return Ok(());
		}

		log::info!("Cleaning up orphaned VLog files below min_oldest_vlog={}", min_oldest_vlog);

		// Use the consolidated cleanup helper
		cleanup_vlog_and_index(&self.vlog, &self.versioned_index, min_oldest_vlog, "startup");

		Ok(())
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

struct LsmCommitEnv {
	core: Arc<CoreInner>,

	/// Manages background tasks like flushing and compaction
	task_manager: Option<Arc<TaskManager>>,
}

impl LsmCommitEnv {
	/// Creates a new commit environment for the LSM tree
	pub(crate) fn new(core: Arc<CoreInner>, task_manager: Arc<TaskManager>) -> Result<Self> {
		Ok(Self {
			core,
			task_manager: Some(task_manager),
		})
	}
}

impl CommitEnv for LsmCommitEnv {
	// Write batch to WAL with inline values (synchronous operation).
	// VLog separation is deferred to memtable flush time.
	fn write(&self, batch: &Batch, seq_num: u64, sync: bool) -> Result<Batch> {
		let mut processed_batch = Batch::new(seq_num);

		for (_, entry, _current_seq_num, timestamp) in batch.entries_with_seq_nums()? {
			// Always store values inline — VLog separation deferred to flush.
			// Versioned index (B+tree) writes are also deferred to flush time,
			// so the B+tree stores value pointers (consistent with SSTables).
			let encoded_value = match &entry.value {
				Some(value) => {
					let value_location = ValueLocation::with_inline_value(value.clone());
					Some(value_location.encode())
				}
				None => None,
			};

			processed_batch.add_record_with_valueptr(
				entry.kind,
				entry.key.clone(),
				encoded_value,
				None,
				timestamp,
			)?;
		}

		// Write to WAL for durability
		let enc_bytes = processed_batch.encode()?;
		let mut wal_guard = self.core.wal.write();
		wal_guard.append(&enc_bytes)?;
		if sync {
			wal_guard.sync()?;
		}
		drop(wal_guard);

		Ok(processed_batch)
	}

	/// Apply batch to memtable with retry on arena full.
	fn apply(&self, batch: &Batch) -> Result<()> {
		// Try to add to current memtable
		let result = {
			let active_memtable = self.core.active_memtable.read()?;
			active_memtable.add(batch)
		};

		match result {
			Ok(()) => Ok(()),
			Err(Error::ArenaFull) => {
				// Arena is full - rotate memtable and retry
				log::debug!("apply: arena full, rotating memtable");

				self.core.rotate_memtable()?;

				// Schedule background flush
				if let Some(ref task_manager) = self.task_manager {
					task_manager.wake_up_memtable();
				}

				// Retry on new memtable - must succeed
				let active_memtable = self.core.active_memtable.read()?;
				active_memtable.add(batch)
			}
			Err(e) => Err(e),
		}
	}

	// Check for background errors before committing
	fn check_background_error(&self) -> Result<()> {
		self.core.error_handler.check_error()
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

	/// The commit pipeline that handles write batches
	pub(crate) commit_pipeline: Arc<CommitPipeline>,

	/// Task manager for background operations (stored in Option so we can take
	/// it for shutdown)
	pub(crate) task_manager: Mutex<Option<Arc<TaskManager>>>,
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
	/// * `flush_memtable` - Callback to flush intermediate memtables to SST
	///
	/// # Returns
	/// * `(Option<max_seq_num>, Option<active_memtable>)`
	pub(crate) fn replay_wal_with_repair<F>(
		wal_path: &Path,
		min_wal_number: u64,
		context: &str,
		recovery_mode: WalRecoveryMode,
		arena_size: usize,
		mut flush_memtable: F,
	) -> Result<(Option<u64>, Option<Arc<MemTable>>)>
	where
		F: FnMut(Arc<MemTable>, u64) -> Result<()>,
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
						log::error!(
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
						log::warn!(
							"Detected WAL corruption in segment {} at offset {}: {}. Attempting repair...",
							segment_id,
							offset,
							message
						);

						// Attempt repair
						if let Err(repair_err) = repair_corrupted_wal_segment(wal_path, segment_id)
						{
							log::error!("Failed to repair WAL segment: {repair_err}");
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
			log::info!("Recovery: flushing {} intermediate memtables to SST", memtable_count - 1);
			for (memtable, wal_number) in memtables.iter().take(memtable_count - 1) {
				if !memtable.is_empty() {
					flush_memtable(Arc::clone(memtable), *wal_number)?;
				}
			}
		}

		// Return the last memtable as the active one
		let (last_memtable, last_wal_number) = memtables.into_iter().last().unwrap();
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
		log::info!(
			"Recovery: setting last memtable (wal={}) as active with {} entries",
			last_wal_number,
			entry_count
		);

		Ok((wal_seq_num_opt, Some(last_memtable)))
	}

	/// Creates a new LSM tree with background task management
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		log::info!("=== Starting LSM tree initialization ===");
		log::info!("Database path: {:?}", opts.path);

		let inner = Arc::new(CoreInner::new(Arc::clone(&opts))?);

		// Initialize background task manager
		let task_manager = Arc::new(TaskManager::new(
			Arc::clone(&inner) as Arc<dyn CompactionOperations>,
			Arc::clone(&opts),
		));

		let commit_env =
			Arc::new(LsmCommitEnv::new(Arc::clone(&inner), Arc::clone(&task_manager))?);

		// Pass the shared visible_seq_num from CoreInner to CommitPipeline
		// Both will use the same atomic for coordinated updates
		let commit_pipeline = CommitPipeline::new(commit_env, Arc::clone(&inner.visible_seq_num));

		// Path for the WAL directory
		let wal_path = opts.wal_dir();

		// Get min_wal_number from manifest to skip already-flushed WALs
		let min_wal_number = inner.level_manifest.read()?.get_log_number();
		let manifest_last_seq = inner.level_manifest.read()?.get_last_sequence();

		log::info!(
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
			|memtable, wal_number| {
				// Flush intermediate memtable to SST during recovery
				let table_id = inner.level_manifest.read()?.next_table_id();
				inner.flush_and_update_manifest(&memtable, table_id, wal_number)?;
				log::info!(
					"Recovery: flushed memtable to SST table_id={}, wal_number={}",
					table_id,
					wal_number
				);
				Ok(())
			},
		)?;

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
				log::debug!(
					"WAL replayed: manifest_last_seq={}, wal_seq={}, using max={}",
					manifest_last_seq,
					wal_seq,
					effective
				);
				effective
			}
			None => {
				log::debug!("WAL skipped or empty, using manifest_last_seq={}", manifest_last_seq);
				manifest_last_seq
			}
		};

		// Set visible sequence number (in-memory, will be persisted on next flush)
		commit_pipeline.set_seq_num(max_seq_num);

		// Clean up any orphaned SST files from previous crashes
		// SAFETY: This must happen AFTER WAL replay so data is recovered
		// but BEFORE any new flushes that might create new SSTs
		inner.cleanup_orphaned_sst_files()?;

		// Clean up any orphaned VLog files that are no longer referenced by any SST
		// SAFETY: This must happen AFTER manifest is loaded so we know which SSTs exist
		inner.cleanup_orphaned_vlog_files()?;

		let core = Self {
			inner: Arc::clone(&inner),
			commit_pipeline: Arc::clone(&commit_pipeline),
			task_manager: Mutex::new(Some(task_manager)),
		};

		log::info!("=== LSM tree initialization complete ===");

		Ok(core)
	}

	pub(crate) async fn commit(&self, batch: Batch, sync: bool) -> Result<()> {
		// Commit the batch using the commit pipeline
		self.commit_pipeline.commit(batch, sync).await
	}

	pub(crate) fn seq_num(&self) -> u64 {
		self.commit_pipeline.get_visible_seq_num()
	}

	/// Flushes WAL and VLog buffers to OS cache.
	///
	/// If `sync` is true, also fsyncs to disk for durability.
	/// This is safe to call concurrently with ongoing transactions.
	///
	/// # Order of Operations
	///
	/// VLog is flushed first (contains data referenced by WAL), then WAL.
	/// This ensures that if WAL contains a ValuePointer, the referenced
	/// VLog data is at least as durable.
	pub(crate) fn flush_wal(&self, sync: bool) -> Result<()> {
		// VLog is NOT synced here — VLog writes are deferred to memtable flush.
		if sync {
			self.wal.sync()?;
		} else {
			self.wal.flush()?;
		}
		Ok(())
	}

	/// Safely closes the LSM tree by shutting down all components in the
	/// correct order.
	///
	/// # Shutdown Sequence
	///
	/// 1. Commit pipeline shutdown - stops accepting new writes
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
		log::info!("Shutting down LSM tree...");

		// Step 1: Shutdown the commit pipeline to stop accepting new writes
		self.commit_pipeline.shutdown();
		log::debug!("Commit pipeline shutdown complete");

		// Step 2: Wait for and stop all background tasks
		let task_manager = self.task_manager.lock().unwrap().take();
		if let Some(task_manager) = task_manager {
			log::debug!("Stopping background task manager...");
			task_manager.stop().await;
			log::debug!("Background task manager stopped");
		}

		// Close the VLog if present
		if let Some(ref vlog) = self.inner.vlog {
			log::debug!("Closing VLog...");
			vlog.close()?;
			log::debug!("VLog closed");
		}

		// Close the versioned index if present
		if let Some(ref versioned_index) = self.inner.versioned_index {
			log::debug!("Closing versioned index...");
			versioned_index.write().close()?;
			log::debug!("Versioned index closed");
		}

		// Step 3: Conditionally flush ALL memtables based on flush_on_close option
		// CRITICAL ORDERING: Immutable memtables must be flushed BEFORE active memtable
		// to preserve SSTable ordering (older data = lower table_ids)
		// IMPORTANT: We do NOT rotate the WAL here to avoid creating an empty WAL file
		if self.inner.opts.flush_on_close {
			log::info!("Flushing all memtables on shutdown (flush_on_close=true)");

			// Flush ALL memtables: immutables first (older data), then active (newest data)
			self.inner.flush_all_memtables_for_shutdown().map_err(|e| {
				Error::Other(format!("Failed to flush memtables during shutdown: {}", e))
			})?;

			log::info!("All memtables flushed successfully on shutdown");
		}

		// Step 4: Close the WAL to ensure all data is flushed
		// This is safe now because all background tasks that could write to WAL are
		// stopped NOTE: WAL must be closed BEFORE cleanup, otherwise cleanup may
		// delete the active WAL file
		let wal_log_number = self.inner.wal.read().get_active_log_number();
		log::info!("Closing WAL: active_log_number={}", wal_log_number);

		let mut wal_guard = self.inner.wal.write();
		wal_guard.close().map_err(|e| Error::Other(format!("Failed to close WAL: {}", e)))?;
		log::debug!("WAL #{:020} closed and synced", wal_log_number);
		drop(wal_guard);

		// Step 4.5: Clean up obsolete WAL files (synchronous cleanup)
		// This happens AFTER closing the WAL to prevent deleting the active WAL file.
		// When memtable flush sets log_number = current_wal + 1, cleanup would delete
		// the active WAL if done before closing it.
		let wal_dir = self.inner.wal.read().get_dir_path().to_path_buf();
		let min_wal_to_keep = self.inner.level_manifest.read()?.get_log_number();

		log::debug!("Cleaning up obsolete WAL files (min_wal_to_keep={})", min_wal_to_keep);

		match cleanup_old_segments(&wal_dir, min_wal_to_keep) {
			Ok(count) if count > 0 => {
				log::info!("Cleaned up {} obsolete WAL files during shutdown", count);
			}
			Ok(_) => {
				log::debug!("No obsolete WAL files to clean up");
			}
			Err(e) => {
				log::warn!("Failed to clean up WAL files during shutdown: {}", e);
			}
		}

		// Step 5: Close the versioned index if present
		if let Some(ref versioned_index) = self.inner.versioned_index {
			log::debug!("Closing versioned index...");
			versioned_index.write().close()?;
			log::debug!("Versioned index closed");
		}

		// Step 6: Flush all directories to ensure durability
		log::debug!("Syncing directory structure...");
		sync_directory_structure(&self.inner.opts).map_err(|e| {
			Error::Other(format!("Failed to sync directories during shutdown: {}", e))
		})?;
		log::debug!("Directory sync complete");

		// Step 7: Release the database lock
		let mut lockfile = self.inner.lockfile.lock()?;
		lockfile.release()?;

		// Log final state
		let final_manifest = self.inner.level_manifest.read()?;
		log::info!(
			"=== LSM tree shutdown complete === log_number={}, last_sequence={}",
			final_manifest.get_log_number(),
			final_manifest.get_last_sequence()
		);

		Ok(())
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

		// Create all required directory structure
		Self::create_directory_structure(&opts)?;

		// Create the core LSM tree components
		let core = Core::new(Arc::clone(&opts))?;

		// TODO: Add file to write options manifest
		// TODO: Add version header in file similar to table in WAL

		// Ensure directory changes are persisted
		sync_directory_structure(&opts)?;

		Ok(Self {
			core: Arc::new(core),
		})
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

		if opts.enable_versioning {
			create_dir_all(opts.versioned_index_dir())?;
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
	/// * `checkpoint_dir` - Directory where the checkpoint will be created
	///
	/// # Returns
	/// Metadata about the created checkpoint
	pub fn create_checkpoint<P: AsRef<Path>>(
		&self,
		checkpoint_dir: P,
	) -> Result<CheckpointMetadata> {
		let checkpoint = DatabaseCheckpoint::new(Arc::clone(&self.core.inner));
		checkpoint.create_checkpoint(checkpoint_dir)
	}

	/// Restores the database from a checkpoint directory.
	pub fn restore_from_checkpoint<P: AsRef<Path>>(
		&self,
		checkpoint_dir: P,
	) -> Result<CheckpointMetadata> {
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
			let earliest_seq = self.core.inner.visible_seq_num.load(Ordering::Acquire);
			let mut active_memtable = self.core.inner.active_memtable.write()?;
			*active_memtable =
				Arc::new(MemTable::new(self.core.inner.opts.max_memtable_size, earliest_seq));
		}

		{
			let mut immutable_memtables = self.core.inner.immutable_memtables.write()?;
			*immutable_memtables = ImmutableMemtables::default();
		}

		// Reopen the WAL from the restored directory
		{
			let manifest_log_number = self.core.inner.level_manifest.read()?.get_log_number();
			let mut wal_guard = self.core.inner.wal.write();
			let wal_path = self.core.inner.opts.path.join("wal");
			let new_wal = Wal::open_with_min_log_number(
				&wal_path,
				manifest_log_number,
				wal::Options::default(),
			)?;
			*wal_guard = new_wal;
		}

		// Replay any WAL entries that were restored
		let wal_path = self.core.inner.opts.path.join("wal");
		let min_wal_number = self.core.inner.level_manifest.read()?.get_log_number();
		let (wal_seq_num_opt, recovered_memtable) = Core::replay_wal_with_repair(
			&wal_path,
			min_wal_number,
			"Database restore",
			self.core.inner.opts.wal_recovery_mode,
			self.core.inner.opts.max_memtable_size,
			|memtable, wal_number| {
				// Flush intermediate memtable to SST during recovery
				let table_id = self.core.inner.level_manifest.read()?.next_table_id();
				self.core.inner.flush_and_update_manifest(&memtable, table_id, wal_number)?;
				log::info!(
					"Restore: flushed memtable to SST table_id={}, wal_number={}",
					table_id,
					wal_number
				);
				Ok(())
			},
		)?;

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

		// Set visible sequence number
		self.core.commit_pipeline.set_seq_num(max_seq_num);

		Ok(metadata)
	}

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
		self.core.inner.flush_all_immutables_sync()
	}

	/// Flushes WAL and VLog buffers to OS cache.
	///
	/// If `sync` is true, also fsyncs to disk, guaranteeing durability
	/// of all previously committed transactions.
	///
	/// If `sync` is false, only flushes to OS buffer cache (faster but
	/// not durable across power loss).
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
			// Native environment - use tokio
			if let Ok(handle) = tokio::runtime::Handle::try_current() {
				// Clone the Arc to move into the async task
				let core = Arc::clone(&self.core);
				handle.spawn(async move {
					if let Err(err) = core.close().await {
						log::error!("Error closing store: {}", err);
					}
				});
			} else {
				log::warn!("No runtime available for closing the store correctly");
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
	/// Default: 4096 (4KB)
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
/// Syncs a directory to ensure all changes are persisted to disk
pub(crate) fn fsync_directory<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
	let path = path.as_ref();

	// Check if the directory still exists before trying to sync it
	if !path.exists() {
		return Ok(());
	}

	let file = File::open(path)?;
	debug_assert!(file.metadata()?.is_dir());
	file.sync_all()
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
				"Failed to sync VLog directory '{}': {}",
				opts.vlog_dir().display(),
				e
			))
		})?;
	}

	if opts.enable_versioning {
		fsync_directory(opts.versioned_index_dir()).map_err(|e| {
			Error::Other(format!(
				"Failed to sync versioned index directory '{}': {}",
				opts.versioned_index_dir().display(),
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

/// Cleans up stale versioned_index entries that reference deleted VLog files.
///
/// After VLog GC deletes files, the versioned_index (B+ tree) may contain
/// entries with ValuePointers referencing those deleted files. This function
/// removes those stale entries.
///
/// # Algorithm:
/// 1. Phase 1: Acquire READ lock, iterate all entries, collect stale keys
/// 2. Release READ lock
/// 3. Phase 2: For each batch of keys, acquire WRITE lock, delete, release
///
/// This design allows concurrent read/write operations between batches.
///
/// # Arguments
/// * `versioned_index` - The versioned B+ tree index
/// * `min_valid_file_id` - VLog files with file_id < this are considered deleted
///
/// # Returns
/// The number of stale entries deleted
pub(crate) fn cleanup_stale_versioned_index(
	versioned_index: &Option<Arc<parking_lot::RwLock<DiskBPlusTree>>>,
	min_valid_file_id: u32,
) -> Result<usize> {
	let versioned_index = match versioned_index {
		Some(idx) => idx,
		None => return Ok(0),
	};

	// Phase 1: Read lock - collect stale keys
	let keys_to_delete: Vec<Vec<u8>> = {
		let guard = versioned_index.read();
		let mut stale_keys = Vec::new();

		// Use range(..) to iterate all entries (RangeFull implements RangeBounds<T>)
		let empty: &[u8] = &[];
		let iter = guard.range(empty..)?;

		for entry in iter {
			let (key, value) = entry?;
			// Check if this entry has a VLog pointer to a deleted file
			if let Ok(loc) = ValueLocation::decode(&value) {
				if loc.is_value_pointer() {
					if let Ok(ptr) = ValuePointer::decode(&loc.value) {
						if ptr.file_id < min_valid_file_id {
							stale_keys.push(key.to_vec());
						}
					}
				}
			}
		}
		stale_keys
	}; // Read lock released here

	if keys_to_delete.is_empty() {
		return Ok(0);
	}

	log::debug!(
		"Cleaning up {} stale versioned_index entries for VLog files < {}",
		keys_to_delete.len(),
		min_valid_file_id
	);

	// Phase 2: Write lock per batch - delete
	let mut deleted_count = 0;
	const BATCH_SIZE: usize = 100;

	for batch in keys_to_delete.chunks(BATCH_SIZE) {
		let mut guard = versioned_index.write();
		for key in batch {
			// No re-verification needed:
			// - Keys are never updated (unique InternalKey)
			// - If deleted by concurrent Replace, delete() returns None (harmless)
			if guard.delete(key)?.is_some() {
				deleted_count += 1;
			}
		}
		// Write lock released here, allowing other operations between batches
	}

	Ok(deleted_count)
}

/// Cleans up obsolete VLog files and stale versioned_index entries.
///
/// This is the consolidated cleanup function that should be called after
/// compaction, flush, or during startup recovery. It:
/// 1. Removes VLog files that are no longer referenced by any SST
/// 2. Removes versioned_index entries pointing to deleted VLog files
///
/// # Arguments
/// * `vlog` - The VLog instance (if value separation is enabled)
/// * `versioned_index` - The versioned B+ tree index (if versioned reads are enabled)
/// * `min_oldest_vlog` - Minimum oldest_vlog_file_id across all live SSTs
/// * `context` - Description of the calling context (e.g., "flush", "compaction", "startup")
pub(crate) fn cleanup_vlog_and_index(
	vlog: &Option<Arc<VLog>>,
	versioned_index: &Option<Arc<parking_lot::RwLock<DiskBPlusTree>>>,
	min_oldest_vlog: u32,
	context: &str,
) {
	// Skip cleanup if no SSTs reference VLog files yet (fresh database case)
	if min_oldest_vlog == 0 {
		return;
	}

	// Clean stale versioned_index entries FIRST — remove references to
	// soon-to-be-deleted vlog files before actually deleting them, so
	// no history query can hit a dangling vlog pointer.
	if let Err(e) = cleanup_stale_versioned_index(versioned_index, min_oldest_vlog) {
		log::warn!("Failed to cleanup stale versioned_index entries during {}: {}", context, e);
		// Don't propagate error - cleanup failures shouldn't fail the primary operation
	}

	// THEN delete obsolete VLog files (safe: no live bplustree references remain)
	if let Some(ref vlog) = vlog {
		if let Err(e) = vlog.cleanup_obsolete_files(min_oldest_vlog) {
			log::warn!("Failed to cleanup obsolete vlog files during {}: {}", context, e);
			// Don't propagate error - cleanup failures shouldn't fail the primary operation
		}
	}
}
