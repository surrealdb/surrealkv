#[cfg(test)]
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::{create_dir_all, File};
use std::path::Path;
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
use crate::oracle::Oracle;
use crate::snapshot::Counter as SnapshotCounter;
use crate::sstable::table::Table;
use crate::sstable::{InternalKey, InternalKeyKind, INTERNAL_KEY_TIMESTAMP_MAX};
use crate::task::TaskManager;
use crate::transaction::{Mode, Transaction};
use crate::vlog::{VLog, VLogGCManager, ValueLocation};
use crate::wal::recovery::{repair_corrupted_wal_segment, replay_wal};
use crate::wal::{self, cleanup_old_segments, Wal};
use crate::{
	BytewiseComparator,
	Comparator,
	Error,
	FilterPolicy,
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

	/// Counter tracking active snapshots for MVCC (Multi-Version Concurrency
	/// Control). Snapshots provide consistent point-in-time views of the data.
	pub(crate) snapshot_counter: SnapshotCounter,

	/// Oracle managing transaction timestamps for MVCC.
	/// Provides monotonic timestamps for transaction ordering and conflict
	/// resolution.
	pub(crate) oracle: Arc<Oracle>,

	/// Value Log (VLog)
	pub(crate) vlog: Option<Arc<VLog>>,

	/// Write-Ahead Log (WAL) for durability
	pub(crate) wal: parking_lot::RwLock<Wal>,

	/// Versioned B+ tree index for timestamp-based queries
	/// Maps InternalKey -> Value for time-range queries
	pub(crate) versioned_index: Option<Arc<parking_lot::RwLock<DiskBPlusTree>>>,

	/// Lock file to prevent multiple processes from opening the same database
	pub(crate) lockfile: Mutex<LockFile>,

	/// Background error handler
	pub(crate) error_handler: Arc<BackgroundErrorHandler>,
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

		// TODO: Add a way to recover from the WAL or level manifest
		// Initialize the transaction oracle for MVCC support
		// The oracle provides monotonic timestamps for transaction ordering
		let oracle = Oracle::new(Arc::clone(&opts.clock));

		// Initialize level manifest FIRST to get log_number
		let manifest = LevelManifest::new(Arc::clone(&opts))?;
		let manifest_log_number = manifest.get_log_number();

		// Initialize WAL starting from manifest.log_number
		// This avoids creating intermediate empty WAL files
		let wal_path = opts.wal_dir();
		let wal_instance =
			Wal::open_with_min_log_number(&wal_path, manifest_log_number, wal::Options::default())?;

		// Initialize active memtable with its WAL number set to the initial WAL
		// This tracks which WAL the memtable's data belongs to for later flush
		let initial_memtable = Arc::new(MemTable::new());
		initial_memtable.set_wal_number(wal_instance.get_active_log_number());
		let active_memtable = Arc::new(RwLock::new(initial_memtable));

		let level_manifest = Arc::new(RwLock::new(manifest));

		// Initialize versioned index if versioned queries are enabled
		let versioned_index = if opts.enable_versioning {
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
			Some(Arc::new(VLog::new(Arc::clone(&opts), versioned_index.clone())?))
		} else {
			None
		};

		Ok(Self {
			opts,
			active_memtable,
			immutable_memtables,
			level_manifest,
			snapshot_counter: SnapshotCounter::default(),
			oracle: Arc::new(oracle),
			vlog,
			wal: parking_lot::RwLock::new(wal_instance),
			versioned_index,
			lockfile: Mutex::new(lockfile),
			error_handler: Arc::new(BackgroundErrorHandler::new()),
		})
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
		// Step 1: Flush memtable to SST
		let table = memtable.flush(table_id, Arc::clone(&self.opts)).map_err(|e| {
			Error::Other(format!("Failed to flush memtable to SST table_id={}: {}", table_id, e))
		})?;

		log::debug!("Created SST table_id={}, file_size={}", table.id, table.file_size);

		// Step 2: Prepare atomic changeset
		let mut changeset = ManifestChangeSet::default();
		changeset.new_tables.push((0, Arc::clone(&table)));
		changeset.log_number = Some(wal_number + 1);

		log::debug!(
			"Changeset prepared: table_id={}, log_number={} (WAL #{:020} flushed)",
			table_id,
			wal_number + 1,
			wal_number
		);

		// Step 3: Apply changeset atomically
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

		Ok(table)
	}

	/// Makes room for new writes by flushing the active memtable if needed.
	///
	/// Triggers a flush operation to make room for new writes.
	///
	/// This is called when the active memtable exceeds the configured size
	/// threshold. The operation proceeds in stages:
	/// 1. Acquire memtable write lock and check if flushing is needed
	/// 2. If flushing, rotate WAL while STILL holding memtable lock
	/// 3. Swap memtable while STILL holding the lock (atomically with WAL rotation)
	/// 4. Release locks, then flush memtable to SST and update manifest
	/// 5. Asynchronously clean up old WAL segments
	fn make_room_for_write(&self, force: bool) -> Result<()> {
		// Step 1: Acquire WRITE lock upfront to prevent race conditions
		// We need the write lock before checking/rotating/swapping to ensure atomicity
		let mut active_memtable = self.active_memtable.write()?;
		let size = active_memtable.size();

		if active_memtable.is_empty() {
			return Ok(());
		}

		// Check if memtable actually exceeds threshold
		// Prevents flushing small memtables from queued notifications
		// Multiple concurrent writes can each call wake_up_memtable(), creating
		// a notification queue. This check ensures we only flush when needed.
		// The 'force' parameter allows bypassing this for explicit flush calls.
		if !force && size < self.opts.max_memtable_size {
			log::debug!(
				"make_room_for_write: memtable size {} below threshold {}, skipping flush",
				size,
				self.opts.max_memtable_size
			);
			return Ok(());
		}

		log::debug!("make_room_for_write: flushing memtable size={}", size);

		// Step 2: Rotate WAL while STILL holding memtable write lock
		// We must rotate WAL and swap memtable atomically
		// to prevent the race condition described above
		let (flushed_wal_number, new_wal_number, wal_dir) = {
			let mut wal_guard = self.wal.write();
			let old_log_number = wal_guard.get_active_log_number();
			let dir = wal_guard.get_dir_path().to_path_buf();
			wal_guard.rotate().map_err(|e| {
				Error::Other(format!("Failed to rotate WAL before memtable flush: {}", e))
			})?;
			let new_log_number = wal_guard.get_active_log_number();
			drop(wal_guard);

			log::debug!(
				"WAL rotated before memtable flush: {} -> {}",
				old_log_number,
				new_log_number
			);
			(old_log_number, new_log_number, dir)
		};

		// Step 3: Swap memtable while STILL holding write lock
		// Take the memtable data that we will flush
		let flushed_memtable = std::mem::take(&mut *active_memtable);

		// Set the WAL number on the new (empty) active memtable
		// This tracks which WAL the new memtable's data will belong to
		active_memtable.set_wal_number(new_wal_number);

		// Get table ID and track immutable memtable with its WAL number
		let mut immutable_memtables = self.immutable_memtables.write()?;
		let table_id = self.level_manifest.read()?.next_table_id();
		// Track the WAL number that contains this memtable's data
		// (the old WAL before rotation)
		immutable_memtables.add(table_id, flushed_wal_number, Arc::clone(&flushed_memtable));

		// Now we can release locks - the memtable is safely in immutable_memtables
		// and no other thread can race us on this specific data
		drop(active_memtable);
		drop(immutable_memtables);

		// Step 4: Flush the memtable to SST and update manifest (slow I/O operation, no locks held)
		let _table =
			self.flush_and_update_manifest(&flushed_memtable, table_id, flushed_wal_number)?;

		// Step 5: Async WAL cleanup using values we already have
		// No need to re-acquire locks - we captured wal_dir and flushed_wal_number
		// earlier
		let min_wal_to_keep = flushed_wal_number + 1; // Same as log_number

		log::debug!("Scheduling async WAL cleanup (min_wal_to_keep={})", min_wal_to_keep);

		tokio::spawn(async move {
			match cleanup_old_segments(&wal_dir, min_wal_to_keep) {
				Ok(count) if count > 0 => {
					log::info!(
						"Cleaned up {} old WAL segments (min_wal_to_keep={})",
						count,
						min_wal_to_keep
					);
				}
				Ok(_) => {
					log::debug!("No old WAL segments to clean up");
				}
				Err(e) => {
					log::warn!("Failed to clean up old WAL segments: {}", e);
				}
			}
		});

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

		// Swap the active memtable with a new empty one
		// This allows writes to continue immediately
		let flushed_memtable = std::mem::take(&mut *active_memtable);

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

	/// Resolves a value, checking if it's a VLog pointer and retrieving from
	/// VLog if needed
	pub(crate) fn resolve_value(&self, value: &[u8]) -> Result<Value> {
		let location = ValueLocation::decode(value)?;
		location.resolve_value(self.vlog.as_ref())
	}
}

impl CompactionOperations for CoreInner {
	/// Triggers a memtable flush to create space for new writes
	fn compact_memtable(&self) -> Result<()> {
		self.make_room_for_write(false) // Don't force, respect threshold
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
	// Write batch to WAL and process VLog entries (synchronous operation)
	// Returns a new batch with VLog pointers, and pre-encoded ValueLocations
	fn write(&self, batch: &Batch, seq_num: u64, sync: bool) -> Result<Batch> {
		let mut processed_batch = Batch::new(seq_num);
		let mut timestamp_entries = Vec::with_capacity(batch.count() as usize);

		let vlog_threshold = self.core.opts.vlog_value_threshold;
		let is_versioning_enabled = self.core.opts.enable_versioning;
		let has_vlog = self.core.vlog.is_some();

		for (_, entry, current_seq_num, timestamp) in batch.entries_with_seq_nums()? {
			let ikey = InternalKey::new(entry.key.clone(), current_seq_num, entry.kind, timestamp);
			let encoded_key = ikey.encode();

			// Process value based on whether VLog is available and value size
			let (valueptr, encoded_value) = match &entry.value {
				Some(value) if has_vlog && value.len() > vlog_threshold => {
					// Large value: store in VLog and create pointer
					let vlog = self.core.vlog.as_ref().unwrap();
					let pointer = vlog.append(&encoded_key, value)?;
					let value_location = ValueLocation::with_pointer(pointer.clone());
					let encoded = value_location.encode();

					// Add to versioned index if enabled
					if is_versioning_enabled {
						timestamp_entries.push((encoded_key.clone(), encoded.clone()));
					}

					(Some(pointer), Some(encoded))
				}
				Some(value) => {
					// Small value or no VLog: store inline
					let value_location = ValueLocation::with_inline_value(value.clone());
					let encoded = value_location.encode();

					// Add to versioned index if enabled
					if is_versioning_enabled {
						timestamp_entries.push((encoded_key.clone(), encoded.clone()));
					}

					(None, Some(encoded))
				}
				None => {
					// Delete operation: no value but may need versioned index entry
					if is_versioning_enabled {
						timestamp_entries.push((encoded_key.clone(), Vec::new()));
					}
					(None, None)
				}
			};

			// Add processed entry to batch
			processed_batch.add_record_with_valueptr(
				entry.kind,
				entry.key.clone(),
				encoded_value,
				valueptr,
				timestamp,
			)?;
		}

		// Flush VLog if present
		if let Some(ref vlog) = self.core.vlog {
			if sync {
				vlog.sync()?;
			} else {
				vlog.flush()?;
			}
		}

		// Write to versioned index if present
		if is_versioning_enabled {
			let mut versioned_index_guard = self.core.versioned_index.as_ref().unwrap().write();

			// Single pass: process each entry individually
			for (encoded_key, encoded_value) in timestamp_entries {
				let ikey = InternalKey::decode(&encoded_key);

				if ikey.is_replace() {
					// For Replace: first delete all existing entries for this user key
					let user_key = ikey.user_key.clone();
					let start_key =
						InternalKey::new(user_key.clone(), 0, InternalKeyKind::Set, 0).encode();
					let end_key = InternalKey::new(
						user_key,
						seq_num,
						InternalKeyKind::Max,
						INTERNAL_KEY_TIMESTAMP_MAX,
					)
					.encode();

					// Collect and delete all existing entries for this user key
					let range_iter =
						versioned_index_guard.range(start_key.as_slice()..=end_key.as_slice())?;
					let mut keys_to_delete = Vec::new();
					for entry in range_iter {
						let (key, _) = entry?;
						keys_to_delete.push(key);
					}

					// Delete all existing entries
					for key in keys_to_delete {
						versioned_index_guard.delete(&key)?;
					}
				}

				// Insert the new entry (whether it's regular Set or SetWithDelete)
				versioned_index_guard.insert(encoded_key, encoded_value)?;
			}
		}

		// Write to WAL for durability
		let enc_bytes = processed_batch.encode()?;
		let mut wal_guard = self.core.wal.write();
		wal_guard.append(&enc_bytes)?;
		if sync {
			wal_guard.sync()?;
		}

		Ok(processed_batch)
	}

	// Apply batch to memtable (can be called concurrently)
	fn apply(&self, batch: &Batch) -> Result<()> {
		// Writes a batch of key-value pairs to the LSM tree.
		//
		// Write path in LSM trees:
		// 1. Write to WAL (Write-Ahead Log) for durability [not shown here]
		// 2. Insert into active memtable for fast access
		// 3. Trigger background flush if memtable is full
		// 4. Apply write stall if too many L0 files accumulate
		let active_memtable = self.core.active_memtable.read()?;

		// Check if memtable needs flushing
		if active_memtable.size() > self.core.opts.max_memtable_size {
			log::debug!(
				"Memtable size {} exceeds threshold {}, triggering background flush",
				active_memtable.size(),
				self.core.opts.max_memtable_size
			);
			// Wake up background thread to flush memtable
			if let Some(ref task_manager) = self.task_manager {
				task_manager.wake_up_memtable();
			}
		}

		// Add the batch to the active memtable
		active_memtable.add(batch)?;

		Ok(())
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

	/// VLog garbage collection manager
	pub(crate) vlog_gc_manager: Mutex<Option<VLogGCManager>>,
}

impl std::ops::Deref for Core {
	type Target = CoreInner;

	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

impl Core {
	/// Function to replay WAL with configurable recovery behavior on
	/// corruption.
	///
	/// # Arguments
	///
	/// * `wal_path` - Path to the WAL directory
	/// * `min_wal_number` - Minimum WAL number to replay (older segments are skipped)
	/// * `context` - Context string for error messages (e.g., "Database startup")
	/// * `recovery_mode` - Controls behavior on corruption:
	///   - `AbsoluteConsistency`: Fail immediately on any corruption
	///   - `TolerateCorruptedWithRepair`: Attempt repair and continue (default)
	/// * `set_recovered_memtable` - Callback to set the recovered memtable
	///
	/// # Returns
	///
	/// * `Ok(Some(seq_num))` - WAL was replayed successfully
	/// * `Ok(None)` - WAL was skipped (already flushed) or empty
	/// * `Err(...)` - Error during replay (corruption in AbsoluteConsistency mode, or unrecoverable
	///   error)
	pub(crate) fn replay_wal_with_repair<F>(
		wal_path: &Path,
		min_wal_number: u64,
		context: &str,
		recovery_mode: WalRecoveryMode,
		mut set_recovered_memtable: F,
	) -> Result<Option<u64>>
	where
		F: FnMut(Arc<MemTable>) -> Result<()>,
	{
		// Create a new empty memtable to recover WAL entries
		let recovered_memtable = Arc::new(MemTable::default());

		// Replay WAL
		let wal_seq_num_opt = match replay_wal(wal_path, &recovered_memtable, min_wal_number) {
			Ok(seq_num_opt) => {
				// WAL was replayed successfully (or was empty/skipped)
				seq_num_opt
			}
			Err(Error::WalCorruption {
				segment_id,
				offset,
				message,
			}) => {
				// Handle corruption based on recovery mode
				match recovery_mode {
					WalRecoveryMode::AbsoluteConsistency => {
						// Fail immediately on any corruption - no repair attempted
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
						// Current behavior: attempt repair and retry
						log::warn!(
							"Detected WAL corruption in segment {} at offset {}: {}. Attempting repair...",
							segment_id,
							offset,
							message
						);

						// Attempt to repair the corrupted segment
						if let Err(repair_err) = repair_corrupted_wal_segment(wal_path, segment_id)
						{
							log::error!("Failed to repair WAL segment: {repair_err}");
							return Err(Error::Other(format!(
								"{context} failed: WAL segment {segment_id} is corrupted and could not be repaired. {repair_err}"
							)));
						}

						// After repair, replay again to recover data from ALL segments.
						// The initial replay stopped at the corruption point and didn't process
						// subsequent segments.
						let retry_memtable = Arc::new(MemTable::default());
						match replay_wal(wal_path, &retry_memtable, min_wal_number) {
							Ok(retry_seq_num) => {
								// Successful replay after repair
								log::info!(
									"WAL replay after repair succeeded: {} entries recovered",
									retry_memtable.iter(true).count()
								);
								if !retry_memtable.is_empty() {
									set_recovered_memtable(retry_memtable)?;
								}
								return Ok(retry_seq_num);
							}
							Err(Error::WalCorruption {
								segment_id: seg_id,
								offset: off,
								message,
							}) => {
								// WAL still corrupted after repair - fail
								return Err(Error::Other(format!(
									"{context} failed: WAL segment {seg_id} still corrupted at offset {off} after repair with message: {message}"
								)));
							}
							Err(retry_err) => {
								// Replay failed after repair - fail
								return Err(Error::Other(format!(
									"{context} failed: WAL replay failed after repair. {retry_err}"
								)));
							}
						}
					}
				}
			}
			Err(e) => {
				// Other errors (IO, etc.) - propagate
				return Err(e);
			}
		};

		// Only set recovered_memtable if we didn't go through repair path
		// (repair path returns early with retry_memtable already set)
		if !recovered_memtable.is_empty() {
			set_recovered_memtable(recovered_memtable)?;
		}

		Ok(wal_seq_num_opt)
	}

	/// Creates a new LSM tree with background task management
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		log::info!("=== Starting LSM tree initialization ===");
		log::info!("Database path: {:?}", opts.path);

		let inner = Arc::new(CoreInner::new(Arc::clone(&opts))?);

		// Initialize background task manager
		let task_manager =
			Arc::new(TaskManager::new(Arc::clone(&inner) as Arc<dyn CompactionOperations>));

		let commit_env =
			Arc::new(LsmCommitEnv::new(Arc::clone(&inner), Arc::clone(&task_manager))?);

		let commit_pipeline = CommitPipeline::new(commit_env);

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
		let wal_seq_num_opt = Self::replay_wal_with_repair(
			&wal_path,
			min_wal_number,
			"Database startup",
			opts.wal_recovery_mode,
			|memtable| {
				let mut active_memtable = inner.active_memtable.write()?;
				*active_memtable = memtable;
				Ok(())
			},
		)?;

		// After WAL replay, ensure the active memtable has the correct WAL number set
		// This is needed because the recovered memtable replaces the initial one
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

		let core = Self {
			inner: Arc::clone(&inner),
			commit_pipeline: Arc::clone(&commit_pipeline),
			task_manager: Mutex::new(Some(task_manager)),
			vlog_gc_manager: Mutex::new(None),
		};

		// Initialize VLog GC manager only if VLog is enabled
		if let Some(ref vlog) = inner.vlog {
			let vlog_gc_manager = VLogGCManager::new(
				Arc::clone(vlog),
				Arc::clone(&commit_pipeline),
				Arc::clone(&inner.error_handler),
			);
			vlog_gc_manager.start();
			*core.vlog_gc_manager.lock().unwrap() = Some(vlog_gc_manager);
			log::debug!("VLog GC manager started");
		}

		log::info!("=== LSM tree initialization complete ===");

		Ok(core)
	}

	pub(crate) async fn commit(&self, batch: Batch, sync: bool) -> Result<()> {
		// Commit the batch using the commit pipeline
		self.commit_pipeline.commit(batch, sync).await
	}

	pub(crate) fn sync_commit(&self, batch: Batch, sync_wal: bool) -> Result<()> {
		// Use the synchronous commit path
		self.commit_pipeline.sync_commit(batch, sync_wal)
	}

	pub(crate) fn seq_num(&self) -> u64 {
		self.commit_pipeline.get_visible_seq_num()
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

		// Stop VLog GC manager if it exists
		let vlog_gc_manager = self.vlog_gc_manager.lock().unwrap().take();
		if let Some(vlog_gc_manager) = vlog_gc_manager {
			log::debug!("Stopping VLog GC manager...");
			vlog_gc_manager.stop().await?;
			log::debug!("VLog GC manager stopped");
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
			create_dir_all(opts.discard_stats_dir())?;
			create_dir_all(opts.delete_list_dir())?;
		}

		if opts.enable_versioning {
			create_dir_all(opts.versioned_index_dir())?;
		}

		Ok(())
	}

	/// Transactions provide a consistent, atomic view of the database.
	pub fn begin(&self) -> Result<Transaction> {
		let txn = Transaction::new(Arc::clone(&self.core), Mode::ReadWrite)?;
		Ok(txn)
	}

	/// Begins a new transaction with the specified mode
	pub fn begin_with_mode(&self, mode: Mode) -> Result<Transaction> {
		let txn = Transaction::new(Arc::clone(&self.core), mode)?;
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
			let mut active_memtable = self.core.inner.active_memtable.write()?;
			*active_memtable = Arc::new(MemTable::default());
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
		let wal_seq_num_opt = Core::replay_wal_with_repair(
			&wal_path,
			min_wal_number,
			"Database restore",
			self.core.inner.opts.wal_recovery_mode,
			|memtable| {
				let mut active_memtable = self.core.inner.active_memtable.write()?;
				*active_memtable = memtable;
				Ok(())
			},
		)?;

		// After WAL replay, ensure the active memtable has the correct WAL number set
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

	#[cfg(test)]
	pub(crate) fn get_all_vlog_stats(&self) -> Vec<(u32, u64, u64, f64)> {
		match &self.core.vlog {
			Some(vlog) => vlog.get_all_file_stats(),
			None => Vec::new(),
		}
	}

	#[cfg(test)]
	/// Updates VLog discard statistics if VLog is enabled
	pub(crate) fn update_vlog_discard_stats(&self, stats: &HashMap<u32, i64>) {
		if let Some(ref vlog) = self.core.vlog {
			vlog.update_discard_stats(stats);
		}
	}

	pub async fn close(&self) -> Result<()> {
		self.core.close().await
	}

	/// Triggers VLog garbage collection manually
	pub async fn garbage_collect_vlog(&self) -> Result<Vec<u32>> {
		match &self.core.vlog {
			Some(vlog) => vlog.garbage_collect(Arc::clone(&self.core.commit_pipeline)).await,
			None => Ok(Vec::new()),
		}
	}

	/// Flushes the active memtable to disk
	pub fn flush(&self) -> Result<()> {
		self.core.make_room_for_write(true) // Force flush, bypass threshold
	}

	pub(crate) fn sync_commit(&self, batch: Batch, sync_wal: bool) -> Result<()> {
		self.core.sync_commit(batch, sync_wal)
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

	/// Sets the VLog garbage collection discard ratio.
	pub fn with_vlog_gc_discard_ratio(mut self, ratio: f64) -> Self {
		self.opts = self.opts.with_vlog_gc_discard_ratio(ratio);
		self
	}

	/// Enables or disables versioned queries with timestamp tracking
	pub fn with_versioning(mut self, enable: bool, retention_ns: u64) -> Self {
		self.opts = self.opts.with_versioning(enable, retention_ns);
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

		fsync_directory(opts.discard_stats_dir()).map_err(|e| {
			Error::Other(format!(
				"Failed to sync discard stats directory '{}': {}",
				opts.discard_stats_dir().display(),
				e
			))
		})?;

		fsync_directory(opts.delete_list_dir()).map_err(|e| {
			Error::Other(format!(
				"Failed to sync delete list directory '{}': {}",
				opts.delete_list_dir().display(),
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
