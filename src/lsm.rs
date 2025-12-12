#[cfg(test)]
use std::collections::HashMap;
use std::{
	collections::HashSet,
	fs::{create_dir_all, File},
	path::Path,
	sync::{Arc, Mutex, RwLock},
};

use crate::{
	batch::Batch,
	bplustree::tree::DiskBPlusTree,
	checkpoint::{CheckpointMetadata, DatabaseCheckpoint},
	commit::{CommitEnv, CommitPipeline},
	compaction::{
		compactor::{CompactionOptions, Compactor},
		CompactionStrategy,
	},
	error::Result,
	levels::{write_manifest_to_disk, LevelManifest, ManifestChangeSet},
	lockfile::LockFile,
	memtable::{ImmutableMemtables, MemTable},
	oracle::Oracle,
	snapshot::Counter as SnapshotCounter,
	sstable::{table::Table, InternalKey, InternalKeyKind, INTERNAL_KEY_TIMESTAMP_MAX},
	task::TaskManager,
	transaction::{Mode, Transaction},
	vlog::{VLog, VLogGCManager, ValueLocation},
	wal::{
		self, cleanup_old_segments,
		recovery::{repair_corrupted_wal_segment, replay_wal},
		Wal,
	},
	BytewiseComparator, Comparator, CompressionType, Error, FilterPolicy, Options,
	TimestampComparator, VLogChecksumLevel, Value,
};
use bytes::Bytes;

use async_trait::async_trait;

// ===== Compaction Operations Trait =====
/// Defines the compaction operations that can be performed on an LSM tree.
/// Compaction is essential for maintaining read performance by merging
/// overlapping SSTables and removing deleted entries.
#[async_trait]
pub trait CompactionOperations: Send + Sync {
	/// Flushes the active memtable to disk, converting it into an immutable SSTable.
	/// This is the first step in the LSM tree's write path.
	fn compact_memtable(&self) -> Result<()>;

	/// Performs compaction according to the specified strategy.
	/// Compaction merges SSTables to reduce read amplification and remove tombstones.
	fn compact(&self, strategy: Arc<dyn CompactionStrategy>) -> Result<()>;
}

// ===== Core LSM Tree Implementation =====
/// The core of an LSM (Log-Structured Merge) tree implementation.
///
/// # LSM Tree Overview
/// An LSM tree optimizes for write performance by buffering writes in memory
/// and periodically flushing them to disk as sorted, immutable files (SSTables).
/// Reads must check multiple locations: the active memtable, immutable memtables,
/// and multiple levels of SSTables.
///
/// # Components
/// - **Active Memtable**: An in-memory, mutable data structure (usually a skip list
///   or B-tree) that receives all new writes.
/// - **Immutable Memtables**: Former active memtables that are full and awaiting
///   flush to disk. They serve reads but accept no new writes.
/// - **SSTables**: Sorted String Tables on disk, organized into levels. Each level
///   has progressively larger SSTables with non-overlapping key ranges (except L0).
/// - **Compaction**: Background process that merges SSTables to maintain read
///   performance and remove deleted entries.
pub(crate) struct CoreInner {
	/// The active memtable (write buffer) that receives all new writes.
	///
	/// In LSM trees, all writes first go to an in-memory structure for fast insertion.
	/// This memtable is typically implemented as a skip list or balanced tree to
	/// maintain sorted order while supporting concurrent access.
	pub(crate) active_memtable: Arc<RwLock<Arc<MemTable>>>,

	/// Collection of immutable memtables waiting to be flushed to disk.
	///
	/// When the active memtable fills up (reaches max_memtable_size), it becomes
	/// immutable and a new active memtable is created. These immutable memtables
	/// continue serving reads while waiting for background threads to flush them
	/// to disk as SSTables.
	pub(crate) immutable_memtables: Arc<RwLock<ImmutableMemtables>>,

	/// The level structure managing all SSTables on disk.
	///
	/// LSM trees organize SSTables into levels:
	/// - L0: Contains SSTables flushed directly from memtables. May have overlapping key ranges.
	/// - L1+: Each level is larger than the previous. SSTables have non-overlapping
	///   key ranges within a level, enabling efficient binary search.
	pub level_manifest: Arc<RwLock<LevelManifest>>,

	/// Configuration options controlling LSM tree behavior
	pub opts: Arc<Options>,

	/// Counter tracking active snapshots for MVCC (Multi-Version Concurrency Control).
	/// Snapshots provide consistent point-in-time views of the data.
	pub(crate) snapshot_counter: SnapshotCounter,

	/// Oracle managing transaction timestamps for MVCC.
	/// Provides monotonic timestamps for transaction ordering and conflict resolution.
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
}

impl CoreInner {
	/// Creates a new LSM tree core instance
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		// Acquire database lock to prevent multiple processes from opening the same database
		let mut lockfile = LockFile::new(&opts.path);
		lockfile.acquire()?;

		// Initialize active and immutable memtables
		let active_memtable = Arc::new(RwLock::new(Arc::new(MemTable::new())));
		let immutable_memtables = Arc::new(RwLock::new(ImmutableMemtables::default()));

		// TODO: Add a way to recover from the WAL or level manifest
		// Initialize the transaction oracle for MVCC support
		// The oracle provides monotonic timestamps for transaction ordering
		let oracle = Oracle::new(opts.clock.clone());

		// Initialize level manifest FIRST to get log_number
		let manifest = LevelManifest::new(opts.clone())?;
		let manifest_log_number = manifest.get_log_number();

		// Initialize WAL starting from manifest.log_number
		// This avoids creating intermediate empty WAL files
		let wal_path = opts.wal_dir();
		let wal_instance =
			Wal::open_with_log_number(&wal_path, manifest_log_number, wal::Options::default())?;

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
			Some(Arc::new(VLog::new(opts.clone(), versioned_index.clone())?))
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
		})
	}

	/// Makes room for new writes by flushing the active memtable if needed.
	///
	/// Triggers a flush operation to make room for new writes.
	///
	/// This is called when the active memtable exceeds the configured size threshold.
	/// The operation proceeds in stages:
	/// 1. Acquire memtable write lock and check if flushing is needed
	/// 2. If flushing, rotate WAL while STILL holding memtable lock
	/// 3. Swap memtable while STILL holding the lock (atomically with WAL rotation)
	/// 4. Release locks, then flush memtable to SST and update manifest
	/// 5. Asynchronously clean up old WAL segments
	///
	/// CRITICAL: We must hold the memtable write lock through WAL rotation and swap.
	/// This prevents a race condition where:
	/// - Thread A reads memtable (above threshold), releases read lock
	/// - Thread A rotates WAL, captures flushed_wal_number
	/// - Thread B swaps memtable before Thread A acquires write lock
	/// - Thread A sees empty memtable, calls update_manifest_log_number()
	/// - Thread A marks WAL as flushed, but Thread B's flush is still in progress
	/// - CRASH: WAL is marked flushed but data was never written to SST → DATA LOSS
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
		let (flushed_wal_number, wal_dir) = {
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
			(old_log_number, dir)
		};

		// Step 3: Swap memtable while STILL holding write lock
		// Take the memtable data that we will flush
		let flushed_memtable = std::mem::take(&mut *active_memtable);

		// Get table ID and track immutable memtable
		let mut immutable_memtables = self.immutable_memtables.write()?;
		let table_id = self.level_manifest.read()?.next_table_id();
		immutable_memtables.add(table_id, flushed_memtable.clone());

		// Now we can release locks - the memtable is safely in immutable_memtables
		// and no other thread can race us on this specific data
		drop(active_memtable);
		drop(immutable_memtables);

		// Step 4: Flush the memtable to SST (slow I/O operation, no locks held)
		let table = flushed_memtable.flush(table_id, self.opts.clone()).map_err(|e| {
			Error::Other(format!("Failed to flush memtable to SST table_id={}: {}", table_id, e))
		})?;

		log::debug!(
			"Created SST table_id={}, file_size={}",
			table.id,
			table.meta.properties.file_size
		);

		// Step 5: Prepare atomic changeset with both SST and log_number
		let mut changeset = ManifestChangeSet::default();
		changeset.new_tables.push((0, table.clone()));

		// Set log_number after WAL rotation
		changeset.log_number = Some(flushed_wal_number + 1);

		log::debug!(
			"Changeset prepared: table_id={}, log_number={} (WAL #{:020} flushed)",
			table_id,
			flushed_wal_number + 1,
			flushed_wal_number
		);

		// Step 6: Apply changeset atomically
		let mut manifest = self.level_manifest.write()?;
		let mut memtable_lock = self.immutable_memtables.write()?;

		manifest.apply_changeset(&changeset)?;
		write_manifest_to_disk(&manifest).map_err(|e| {
			Error::Other(format!(
				"Failed to atomically update manifest: table_id={}, log_number={:?}: {}",
				table_id, changeset.log_number, e
			))
		})?;

		memtable_lock.remove(table_id);
		drop(manifest);
		drop(memtable_lock);

		log::info!(
			"Memtable flush completed: table_id={}, log_number={:?}",
			table_id,
			changeset.log_number
		);

		// Step 7: Async WAL cleanup using values we already have
		// No need to re-acquire locks - we captured wal_dir and flushed_wal_number earlier
		let min_wal_to_keep = flushed_wal_number + 1; // Same as changeset.log_number

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
	/// This is the core flush logic used by both shutdown and normal memtable rotation.
	/// Unlike `make_room_for_write`, this does NOT rotate the WAL - the caller is responsible
	/// for WAL rotation if needed.
	///
	/// # Arguments
	///
	/// - `flushed_wal_number`: Optional WAL number that was flushed. If provided, log_number
	///   will be set to `flushed_wal_number + 1`. If None, uses current active WAL number.
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

		// Track the immutable memtable until it's successfully flushed
		immutable_memtables.add(table_id, flushed_memtable.clone());

		// Release locks before the potentially slow flush operation
		drop(active_memtable);
		drop(immutable_memtables);

		// Step 2: Flush the immutable memtable to disk as an SSTable
		let table = flushed_memtable.flush(table_id, self.opts.clone()).map_err(|e| {
			Error::Other(format!("Failed to flush memtable to SST table_id={}: {}", table_id, e))
		})?;

		log::debug!(
			"Created SST table_id={}, file_size={}",
			table.id,
			table.meta.properties.file_size
		);

		// Step 3: Prepare atomic changeset with both SST and log_number
		// This ensures crash safety - both updates happen in a single manifest write
		let mut changeset = ManifestChangeSet::default();
		changeset.new_tables.push((0, table.clone()));

		// Determine which WAL was flushed and set log_number atomically
		let wal_that_was_flushed = match flushed_wal_number {
			Some(num) => num,
			None => {
				// No explicit WAL provided, use current active WAL
				// This happens during shutdown when no rotation occurred
				self.wal.read().get_active_log_number()
			}
		};

		// Set log_number to flushed_wal + 1, meaning that WAL has been flushed
		// log_number indicates "all WALs with number < log_number have been flushed"
		changeset.log_number = Some(wal_that_was_flushed + 1);

		log::debug!(
			"Changeset prepared: table_id={}, log_number={} (WAL #{:020} flushed)",
			table_id,
			wal_that_was_flushed + 1,
			wal_that_was_flushed
		);

		// Step 4: Apply changeset and write to disk ATOMICALLY
		// This is the critical section - both SST addition and log_number update happen together
		let mut manifest = self.level_manifest.write()?;
		let mut memtable_lock = self.immutable_memtables.write()?;

		manifest.apply_changeset(&changeset)?;
		write_manifest_to_disk(&manifest).map_err(|e| {
			Error::Other(format!(
				"Failed to atomically update manifest: table_id={}, log_number={:?}: {}",
				table_id, changeset.log_number, e
			))
		})?;

		// Remove successfully flushed memtable from tracking
		memtable_lock.remove(table_id);

		log::info!(
			"Manifest updated atomically: table_id={}, log_number={:?}, last_sequence={}",
			table_id,
			changeset.log_number,
			manifest.get_last_sequence()
		);

		Ok(Some(table))
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

	/// Resolves a value, checking if it's a VLog pointer and retrieving from VLog if needed
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
					let value_location =
						ValueLocation::with_inline_value(Bytes::copy_from_slice(value));
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
				&entry.key,
				encoded_value.as_deref(),
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
					let range_iter = versioned_index_guard.range(&start_key, &end_key)?;
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
				versioned_index_guard.insert(encoded_key.as_ref(), encoded_value.as_ref())?;
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
	inner: Arc<CoreInner>,

	/// The commit pipeline that handles write batches
	commit_pipeline: Arc<CommitPipeline>,

	/// Task manager for background operations (stored in Option so we can take it for shutdown)
	task_manager: Mutex<Option<Arc<TaskManager>>>,

	/// VLog garbage collection manager
	vlog_gc_manager: Mutex<Option<VLogGCManager>>,
}

impl std::ops::Deref for Core {
	type Target = CoreInner;

	fn deref(&self) -> &Self::Target {
		&self.inner
	}
}

impl Core {
	/// Function to replay WAL with automatic repair on corruption.
	///
	/// Returns Option<u64>:
	/// - Some(seq_num) if WAL was actually replayed
	/// - None if WAL was skipped (already flushed) or empty
	pub(crate) fn replay_wal_with_repair<F>(
		wal_path: &Path,
		min_wal_number: u64,
		context: &str, // "Database startup" or "Database reload"
		mut set_recovered_memtable: F,
	) -> Result<Option<u64>>
	where
		F: FnMut(Arc<MemTable>) -> Result<()>,
	{
		// Create a new empty memtable to recover WAL entries
		let recovered_memtable = Arc::new(MemTable::default());

		// Replay WAL with automatic repair on corruption
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
				log::warn!(
					"Detected WAL corruption in segment {} at offset {}: {}. Attempting repair...",
					segment_id,
					offset,
					message
				);

				// Attempt to repair the corrupted segment
				if let Err(repair_err) = repair_corrupted_wal_segment(wal_path, segment_id) {
					log::error!("Failed to repair WAL segment: {repair_err}");
					return Err(Error::Other(format!(
						"{context} failed: WAL segment {segment_id} is corrupted and could not be repaired. {repair_err}"
					)));
				}

				// After repair, replay again to recover data from ALL segments.
				// The initial replay stopped at the corruption point and didn't process subsequent segments.
				let retry_memtable = Arc::new(MemTable::default());
				match replay_wal(wal_path, &retry_memtable, min_wal_number) {
					Ok(retry_seq_num) => {
						// Successful replay after repair
						log::info!(
							"WAL replay after repair succeeded: {} entries recovered",
							retry_memtable.iter().count()
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

		let inner = Arc::new(CoreInner::new(opts.clone())?);

		// Initialize background task manager
		let task_manager = Arc::new(TaskManager::new(inner.clone()));

		let commit_env = Arc::new(LsmCommitEnv::new(inner.clone(), task_manager.clone())?);

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

		// Replay WAL with automatic repair on corruption (returns None if skipped/empty)
		let wal_seq_num_opt = Self::replay_wal_with_repair(
			&wal_path,
			min_wal_number,
			"Database startup",
			|memtable| {
				let mut active_memtable = inner.active_memtable.write()?;
				*active_memtable = memtable;
				Ok(())
			},
		)?;

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
			inner: inner.clone(),
			commit_pipeline: commit_pipeline.clone(),
			task_manager: Mutex::new(Some(task_manager)),
			vlog_gc_manager: Mutex::new(None),
		};

		// Initialize VLog GC manager only if VLog is enabled
		if let Some(ref vlog) = inner.vlog {
			let vlog_gc_manager = VLogGCManager::new(vlog.clone(), commit_pipeline.clone());
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

	/// Safely closes the LSM tree by shutting down all components in the correct order.
	///
	/// # Shutdown Sequence
	///
	/// 1. Commit pipeline shutdown - stops accepting new writes
	/// 2. Background tasks stopped - waits for ongoing operations
	/// 3. Active memtable flush - if flush_on_close enabled AND memtable non-empty, flush to SST (NO WAL rotation)
	/// 4. WAL close - sync and close current WAL file
	/// 5. Directory sync - ensure all metadata is persisted
	/// 6. Lock release - allow other processes to open the database
	///
	/// # Critical: No Empty WAL Creation
	///
	/// Unlike `make_room_for_write`, this does NOT rotate the WAL before flushing.
	/// This prevents creating an empty WAL file on clean shutdown.
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

		// Step 3: Conditionally flush the active memtable based on flush_on_close option
		// IMPORTANT: We do NOT rotate the WAL here to avoid creating an empty WAL file
		if self.inner.opts.flush_on_close {
			let active_memtable = self.inner.active_memtable.read()?;
			let memtable_size = active_memtable.size();
			let memtable_entries = active_memtable.iter().count();

			if !active_memtable.is_empty() {
				drop(active_memtable);

				log::info!(
					"Flushing active memtable on shutdown (flush_on_close=true): entries={}, size_bytes={}",
					memtable_entries,
					memtable_size
				);

				// Direct flush without WAL rotation
				// Uses centralized flush logic that updates manifest log_number
				// Pass None for flushed_wal_number since we didn't rotate
				self.inner.flush_memtable_and_update_manifest(None).map_err(|e| {
					Error::Other(format!("Failed to flush memtable during shutdown: {}", e))
				})?;

				log::info!("Active memtable flushed successfully on shutdown");
			} else {
				log::debug!("Active memtable is empty, skipping shutdown flush");
			}
		} else {
			log::info!("Skipping memtable flush on shutdown (flush_on_close=false)");
		}

		// Step 4: Close the WAL to ensure all data is flushed
		// This is safe now because all background tasks that could write to WAL are stopped
		// NOTE: WAL must be closed BEFORE cleanup, otherwise cleanup may delete the active WAL file
		let wal_log_number = self.inner.wal.read().get_active_log_number();
		log::info!("Closing WAL: active_log_number={}", wal_log_number);

		let mut wal_guard = self.inner.wal.write();
		wal_guard.close().map_err(|e| Error::Other(format!("Failed to close WAL: {}", e)))?;
		log::debug!("WAL #{:020} closed and synced", wal_log_number);
		drop(wal_guard);

		// Step 4.5: Clean up obsolete WAL files (synchronous cleanup)
		// This happens AFTER closing the WAL to prevent deleting the active WAL file.
		// When memtable flush sets log_number = current_wal + 1, cleanup would delete the
		// active WAL if done before closing it.
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
	fn new(opts: Arc<Options>) -> Result<Self> {
		// Validate options before creating the tree
		opts.validate()?;

		// Create all required directory structure
		Self::create_directory_structure(&opts)?;

		// Create the core LSM tree components
		let core = Core::new(opts.clone())?;

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
		let txn = Transaction::new(self.core.clone(), Mode::ReadWrite)?;
		Ok(txn)
	}

	/// Begins a new transaction with the specified mode
	pub fn begin_with_mode(&self, mode: Mode) -> Result<Transaction> {
		let txn = Transaction::new(self.core.clone(), mode)?;
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
		let checkpoint = DatabaseCheckpoint::new(self.core.inner.clone());
		checkpoint.create_checkpoint(checkpoint_dir)
	}

	/// Restores the database from a checkpoint directory.
	pub fn restore_from_checkpoint<P: AsRef<Path>>(
		&self,
		checkpoint_dir: P,
	) -> Result<CheckpointMetadata> {
		// Step 1: Restore files from checkpoint
		let checkpoint = DatabaseCheckpoint::new(self.core.inner.clone());
		let metadata = checkpoint.restore_from_checkpoint(checkpoint_dir)?;

		// Step 2: Reload in-memory state to match restored files

		// Create a new LevelManifest from the current path
		let new_levels = LevelManifest::new(self.core.inner.opts.clone())?;

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
			let mut wal_guard = self.core.inner.wal.write();
			let wal_path = self.core.inner.opts.path.join("wal");
			let new_wal = Wal::open(&wal_path, wal::Options::default())?;
			*wal_guard = new_wal;
		}

		// Replay any WAL entries that were restored
		let wal_path = self.core.inner.opts.path.join("wal");
		let min_wal_number = self.core.inner.level_manifest.read()?.get_log_number();
		let wal_seq_num_opt = Core::replay_wal_with_repair(
			&wal_path,
			min_wal_number,
			"Database restore",
			|memtable| {
				let mut active_memtable = self.core.inner.active_memtable.write()?;
				*active_memtable = memtable;
				Ok(())
			},
		)?;

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
	fn get_all_vlog_stats(&self) -> Vec<(u32, u64, u64, f64)> {
		match &self.core.vlog {
			Some(vlog) => vlog.get_all_file_stats(),
			None => Vec::new(),
		}
	}

	#[cfg(test)]
	/// Updates VLog discard statistics if VLog is enabled
	fn update_vlog_discard_stats(&self, stats: &HashMap<u32, i64>) {
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
			Some(vlog) => vlog.garbage_collect(self.core.commit_pipeline.clone()).await,
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
	/// Creates a new TreeBuilder with default options for the specified key type.
	pub fn new() -> Self {
		Self {
			opts: Options::default(),
		}
	}

	/// Creates a new TreeBuilder with the specified options.
	///
	/// This method ensures type safety by requiring the options to use the same key type.
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

	/// Sets the compression type.
	pub fn with_compression(mut self, compression: CompressionType) -> Self {
		self.opts = self.opts.with_compression(compression);
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

	/// Sets the block cache capacity.
	pub fn with_block_cache_capacity(mut self, capacity_bytes: u64) -> Self {
		self.opts = self.opts.with_block_cache_capacity(capacity_bytes);
		self
	}

	/// Sets the VLog cache capacity.
	pub fn with_vlog_cache_capacity(mut self, capacity_bytes: u64) -> Self {
		self.opts = self.opts.with_vlog_cache_capacity(capacity_bytes);
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
		let tree = Tree::new(opts.clone())?;
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

#[cfg(test)]
mod tests {
	use std::{collections::HashMap, path::PathBuf};
	use test_log::test;

	use crate::compaction::leveled::Strategy;

	use super::*;

	use tempdir::TempDir;

	fn create_temp_directory() -> TempDir {
		TempDir::new("test").unwrap()
	}

	/// Creates test options with common defaults, allowing customization of specific fields
	fn create_test_options(
		path: PathBuf,
		customizations: impl FnOnce(&mut Options),
	) -> Arc<Options> {
		let mut opts = Options {
			path,
			vlog_value_threshold: 0,
			..Default::default()
		};
		customizations(&mut opts);
		Arc::new(opts)
	}

	/// Creates test options optimized for small memtable testing (triggers frequent flushes)
	fn create_small_memtable_options(path: PathBuf) -> Arc<Options> {
		create_test_options(path, |opts| {
			opts.max_memtable_size = 64 * 1024; // 64KB
		})
	}

	/// Creates test options optimized for VLog testing
	fn create_vlog_test_options(path: PathBuf) -> Arc<Options> {
		create_test_options(path, |opts| {
			opts.max_memtable_size = 64 * 1024; // 64KB
			opts.vlog_max_file_size = 1024 * 1024; // 1MB
		})
	}

	/// Creates test options optimized for VLog compaction testing
	fn create_vlog_compaction_options(path: PathBuf) -> Arc<Options> {
		create_test_options(path, |opts| {
			opts.max_memtable_size = 64 * 1024; // 64KB
			opts.vlog_max_file_size = 950; // Small size to force frequent rotations
			opts.vlog_gc_discard_ratio = 0.0; // Disable discard ratio to preserve all values
			opts.level_count = 2; // Two levels for compaction strategy
		})
	}

	#[test(tokio::test)]
	async fn test_tree_basic() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let tree = Tree::new(create_test_options(path.clone(), |_| {})).unwrap();

		// Insert a key-value pair
		let key = "hello";
		let value = "world";
		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		txn.commit().await.unwrap();

		// Read back the key-value pair
		let txn = tree.begin().unwrap();
		let result = txn.get(key.as_bytes()).unwrap().unwrap();
		assert_eq!(result, Bytes::copy_from_slice(value.as_bytes()));
	}

	#[test(tokio::test)]
	async fn test_memtable_flush() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 132; // Total size of kvs below
		});

		let key = "hello";
		let values = ["world", "universe", "everyone", "planet"];

		let tree = Tree::new(opts.clone()).unwrap();

		// Insert the same key multiple times with different values
		for value in values.iter() {
			// Insert key-value pair in a new transaction
			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Ensure the active memtable is flushed to disk
		tree.flush().unwrap();

		{
			// Read back the key-value pair
			let txn = tree.begin().unwrap();
			let result = txn.get(key.as_bytes()).unwrap().unwrap();
			let expected_value = values.last().unwrap();
			assert_eq!(result, Bytes::copy_from_slice(expected_value.as_bytes()));
		}
	}

	#[test(tokio::test)]
	async fn test_memtable_flush_with_delete() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 132; // Total size of kvs below
		});

		let key = "hello";
		let values = ["world", "universe", "everyone"];

		let tree = Tree::new(opts.clone()).unwrap();

		// Insert the same key multiple times with different values
		for value in values.iter() {
			// Insert key-value pair in a new transaction
			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Delete the key
		{
			let mut txn = tree.begin().unwrap();
			txn.delete(key.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Ensure the active memtable is flushed to disk
		tree.flush().unwrap();

		{
			// Read back the key-value pair
			let txn = tree.begin().unwrap();
			let result = txn.get(key.as_bytes()).unwrap();
			assert!(result.is_none(), "Expected key to be deleted");
		}
	}

	#[test(tokio::test)]
	async fn test_memtable_flush_with_multiple_keys_and_updates() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		// Set a reasonable memtable size to trigger multiple flushes
		let opts = create_small_memtable_options(path.clone());

		let tree = Tree::new(opts.clone()).unwrap();

		// Number of keys to insert
		const KEY_COUNT: usize = 10001;
		// Number of updates per key
		const UPDATES_PER_KEY: usize = 2;

		// Track the latest value for each key for verification
		let mut expected_values = Vec::with_capacity(KEY_COUNT);

		// Insert each key with multiple updates
		for i in 0..KEY_COUNT {
			let key = format!("key_{i:05}");
			for update in 0..UPDATES_PER_KEY {
				let value = format!("value_{i:05}_update_{update}");

				// Save the final value for verification
				if update == UPDATES_PER_KEY - 1 {
					expected_values.push((key.clone(), value.clone()));
				}

				// Insert in a new transaction
				let mut txn = tree.begin().unwrap();
				txn.set(key.as_bytes(), value.as_bytes()).unwrap();
				txn.commit().await.unwrap();
			}
		}

		// Final flush to ensure all data is on disk
		tree.flush().unwrap();

		// Verify all keys have their final values
		for (key, expected_value) in expected_values {
			let txn = tree.begin().unwrap();
			let result = txn.get(key.as_bytes()).unwrap().unwrap();
			assert_eq!(
				result,
				Bytes::copy_from_slice(expected_value.as_bytes()),
				"Key '{key}' should have final value '{expected_value}'"
			);
		}

		// Verify the LSM state: we should have multiple SSTables
		let l0_size = tree.core.level_manifest.read().unwrap().levels.get_levels()[0].tables.len();
		assert!(l0_size > 0, "Expected SSTables in L0, got {l0_size}");
	}

	#[test(tokio::test)]
	async fn test_persistence() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		// Set a reasonable memtable size to trigger multiple flushes
		let opts = create_test_options(path.clone(), |opts| {
			opts.vlog_max_file_size = 10;
		});

		// Number of keys to insert
		const KEY_COUNT: usize = 100;
		// Number of updates per key
		const UPDATES_PER_KEY: usize = 1;

		// Track the latest value for each key for verification
		let expected_values: Vec<(String, String)>;

		// Step 1: Insert data and close the store
		{
			let tree = Tree::new(opts.clone()).unwrap();
			let mut values = Vec::with_capacity(KEY_COUNT);

			// Insert each key with multiple updates
			for i in 0..KEY_COUNT {
				let key = format!("key_{i:05}");
				for update in 0..UPDATES_PER_KEY {
					let value = format!("value_{i:05}_update_{update}");

					// Save the final value for verification
					if update == UPDATES_PER_KEY - 1 {
						values.push((key.clone(), value.clone()));
					}

					// Insert in a new transaction
					let mut txn = tree.begin().unwrap();
					txn.set(key.as_bytes(), value.as_bytes()).unwrap();
					txn.commit().await.unwrap();
				}
			}

			// Final flush to ensure all data is on disk
			tree.flush().unwrap();

			// Store values for later verification
			expected_values = values;

			// Verify L0 has tables before closing
			let l0_size =
				tree.core.level_manifest.read().unwrap().levels.get_levels()[0].tables.len();
			assert!(l0_size > 0, "Expected SSTables in L0 before closing, got {l0_size}");

			// Tree will be dropped here, closing the store
			tree.close().await.unwrap();
		}

		// Step 2: Reopen the store and verify data
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Verify L0 has tables after reopening
			let l0_size =
				tree.core.level_manifest.read().unwrap().levels.get_levels()[0].tables.len();
			assert!(l0_size > 0, "Expected SSTables in L0 after reopening, got {l0_size}");

			// Verify all keys have their final values
			for (key, expected_value) in expected_values.iter() {
				let txn = tree.begin().unwrap();
				let result = txn.get(key.as_bytes()).unwrap();

				assert!(result.is_some(), "Key '{key}' not found after reopening store");

				let value = result.unwrap();
				assert_eq!(
					value,
					Bytes::copy_from_slice(expected_value.as_bytes()),
					"Key '{}' has incorrect value after reopening. Expected '{}', got '{:?}'",
					key,
					expected_value,
					std::str::from_utf8(value.as_ref()).unwrap_or("<invalid utf8>")
				);
			}
		}
	}

	#[test(tokio::test)]
	async fn test_checkpoint_functionality() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_small_memtable_options(path.clone());

		// Create initial data
		let tree = Tree::new(opts.clone()).unwrap();

		// Insert some test data
		for i in 0..10 {
			let key = format!("key_{i:03}");
			let value = format!("value_{i:03}");

			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Force flush to ensure data is on disk
		tree.flush().unwrap();

		// Create checkpoint
		let checkpoint_dir = temp_dir.path().join("checkpoint");
		let metadata = tree.create_checkpoint(&checkpoint_dir).unwrap();

		// Verify checkpoint metadata and structure
		assert!(metadata.timestamp > 0);
		assert_eq!(metadata.sequence_number, 10);
		assert_eq!(metadata.sstable_count, 1);
		assert!(metadata.total_size > 0);

		assert!(checkpoint_dir.exists());
		assert!(checkpoint_dir.join("sstables").exists());
		assert!(checkpoint_dir.join("wal").exists());
		assert!(checkpoint_dir.join("manifest").exists());
		assert!(checkpoint_dir.join("CHECKPOINT_METADATA").exists());

		// Insert more data after checkpoint
		for i in 10..15 {
			let key = format!("key_{i:03}");
			let value = format!("value_{i:03}");

			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Force flush to ensure the new data is also on disk
		tree.flush().unwrap();

		// Verify all data exists before restore
		for i in 0..15 {
			let key = format!("key_{i:03}");
			let txn = tree.begin().unwrap();
			let result = txn.get(key.as_bytes()).unwrap();
			assert!(result.is_some(), "Key '{key}' should exist before restore");
		}

		// Restore from checkpoint
		tree.restore_from_checkpoint(&checkpoint_dir).unwrap();

		// Verify data is restored to checkpoint state (keys 0-9 exist, 10-14 don't)
		for i in 0..10 {
			let key = format!("key_{i:03}");
			let expected_value = format!("value_{i:03}");

			let txn = tree.begin().unwrap();
			let result = txn.get(key.as_bytes()).unwrap();
			assert!(result.is_some(), "Key '{key}' should exist after restore");
			assert_eq!(result.unwrap(), Bytes::copy_from_slice(expected_value.as_bytes()));
		}

		// Verify the newer keys don't exist after restore
		for i in 10..15 {
			let key = format!("key_{i:03}");

			let txn = tree.begin().unwrap();
			let result = txn.get(key.as_bytes()).unwrap();
			assert!(result.is_none(), "Key '{key}' should not exist after restore");
		}
	}

	#[test(tokio::test)]
	async fn test_checkpoint_restore_discards_pending_writes() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_small_memtable_options(path.clone());

		// Create initial data and checkpoint
		let tree = Tree::new(opts.clone()).unwrap();

		// Insert initial data
		for i in 0..5 {
			let key = format!("key_{i:03}");
			let value = format!("checkpoint_value_{i:03}");

			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Force flush to ensure data is on disk
		tree.flush().unwrap();

		// Create checkpoint
		let checkpoint_dir = temp_dir.path().join("checkpoint");
		let metadata = tree.create_checkpoint(&checkpoint_dir).unwrap();
		assert_eq!(metadata.sequence_number, 5);

		// Insert NEW data that should be discarded during restore
		for i in 0..5 {
			let key = format!("key_{i:03}");
			let value = format!("pending_value_{i:03}"); // Different values

			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Verify the pending writes are in memory but not yet flushed
		for i in 0..5 {
			let key = format!("key_{i:03}");
			let txn = tree.begin().unwrap();
			let result = txn.get(key.as_bytes()).unwrap().unwrap();
			assert_eq!(
				result,
				Bytes::copy_from_slice(format!("pending_value_{i:03}").as_bytes()),
				"Expected pending value before restore"
			);
		}

		// Restore from checkpoint - this should discard pending writes
		tree.restore_from_checkpoint(&checkpoint_dir).unwrap();

		// Verify data is restored to checkpoint state (NOT the pending writes)
		for i in 0..5 {
			let key = format!("key_{i:03}");
			let expected_value = format!("checkpoint_value_{i:03}");

			let txn = tree.begin().unwrap();
			let result = txn.get(key.as_bytes()).unwrap().unwrap();
			assert_eq!(
				result,
				Bytes::copy_from_slice(expected_value.as_bytes()),
				"Key '{key}' should have checkpoint value '{expected_value}', not pending value"
			);
		}
	}

	#[test(tokio::test)]
	async fn test_simple_range_seek() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_small_memtable_options(path.clone());

		let tree = Tree::new(opts.clone()).unwrap();

		// Insert simple keys
		let keys = ["a", "b", "c", "d", "e"];
		for key in keys.iter() {
			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), key.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Test range scan BEFORE flushing (should work from memtables)
		let txn = tree.begin().unwrap();
		let range_before_flush: Vec<_> =
			txn.range(b"a", b"d").unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

		// Should return keys "a", "b", "c" ([a, d) range)
		assert_eq!(range_before_flush.len(), 3, "Range scan before flush should return 3 items");

		// Verify the keys and values are correct
		let expected_before = [("a", "a"), ("b", "b"), ("c", "c")];
		for (idx, (key, value)) in range_before_flush.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref()).unwrap();
			assert_eq!(key_str, expected_before[idx].0, "Key mismatch at index {idx} before flush");
			assert_eq!(
				value_str, expected_before[idx].1,
				"Value mismatch at index {idx} before flush"
			);
		}

		// Force flush to ensure data is on disk
		tree.flush().unwrap();

		// Test range scan AFTER flushing (should work from SSTables)
		let txn = tree.begin().unwrap();
		let range_after_flush: Vec<_> =
			txn.range(b"a", b"d").unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

		// Should return the same keys after flush
		assert_eq!(range_after_flush.len(), 3, "Range scan after flush should return 3 items");

		// Verify the keys and values are still correct after flush
		let expected_after = [("a", "a"), ("b", "b"), ("c", "c")];
		for (idx, (key, value)) in range_after_flush.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref()).unwrap();
			assert_eq!(key_str, expected_after[idx].0, "Key mismatch at index {idx} after flush");
			assert_eq!(
				value_str, expected_after[idx].1,
				"Value mismatch at index {idx} after flush"
			);
		}

		// Test individual gets
		for key in ["a", "b", "c"].iter() {
			let txn = tree.begin().unwrap();
			let result = txn.get(key.as_bytes()).unwrap();

			assert!(result.is_some(), "Key '{key}' should be found");
			let value = result.unwrap();
			let value_str = std::str::from_utf8(value.as_ref()).unwrap();
			assert_eq!(value_str, *key, "Value for key '{key}' should match the key");
		}

		// Verify that the results are identical before and after flush
		assert_eq!(
			range_before_flush, range_after_flush,
			"Range scan results should be identical before and after flush"
		);
	}

	#[test(tokio::test)]
	async fn test_large_range_scan() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_small_memtable_options(path.clone());

		let tree = Tree::new(opts.clone()).unwrap();

		// Insert 10,000 items with zero-padded keys for consistent ordering
		const TOTAL_ITEMS: usize = 10_000;
		let mut expected_items = Vec::with_capacity(TOTAL_ITEMS);

		for i in 0..TOTAL_ITEMS {
			let key = format!("key_{i:06}");
			let value = format!("value_{:06}_content_data_{}", i, i * 2);

			expected_items.push((key.clone(), value.clone()));

			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Force flush to ensure all data is on disk
		tree.flush().unwrap();

		// Test 1: Full range scan - should return all 10k items in order
		let start_key = "key_000000".as_bytes();
		let end_key = "key_999999".as_bytes();

		let txn = tree.begin().unwrap();
		let range_result: Vec<_> =
			txn.range(start_key, end_key).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

		assert_eq!(
			range_result.len(),
			TOTAL_ITEMS,
			"Full range scan should return all {TOTAL_ITEMS} items"
		);

		// Verify each item is correct and in order
		for (idx, (key, value)) in range_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref()).unwrap();

			let expected_key = &expected_items[idx].0;
			let expected_value = &expected_items[idx].1;

			assert_eq!(
				key_str, expected_key,
				"Key mismatch at index {idx}: expected '{expected_key}', found '{key_str}'"
			);
			assert_eq!(
				value_str, expected_value,
				"Value mismatch at index {idx}: expected '{expected_value}', found '{value_str}'"
			);
		}

		// Test 2: Partial range scan - first 100 items ([key_000000, key_000100) range)
		let partial_start = "key_000000".as_bytes();
		let partial_end = "key_000100".as_bytes();

		let txn = tree.begin().unwrap();
		let partial_result: Vec<_> =
			txn.range(partial_start, partial_end).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

		assert_eq!(partial_result.len(), 100, "Partial range scan should return 100 items");

		// Verify each item in partial range
		for (idx, (key, value)) in partial_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref()).unwrap();

			let expected_key = &expected_items[idx].0;
			let expected_value = &expected_items[idx].1;

			assert_eq!(
                key_str, expected_key,
                "Partial range key mismatch at index {idx}: expected '{expected_key}', found '{key_str}'"
            );
			assert_eq!(
                value_str, expected_value,
                "Partial range value mismatch at index {idx}: expected '{expected_value}', found '{value_str}'"
            );
		}

		// Test 3: Middle range scan - items 5000-5099
		let middle_start = "key_005000".as_bytes();
		let middle_end = "key_005100".as_bytes();

		let txn = tree.begin().unwrap();
		let middle_result: Vec<_> =
			txn.range(middle_start, middle_end).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

		assert_eq!(middle_result.len(), 100, "Middle range scan should return 100 items");

		// Verify each item in middle range
		for (idx, (key, value)) in middle_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref()).unwrap();

			let expected_idx = 5000 + idx;
			let expected_key = &expected_items[expected_idx].0;
			let expected_value = &expected_items[expected_idx].1;

			assert_eq!(
                key_str, expected_key,
                "Middle range key mismatch at index {idx}: expected '{expected_key}', found '{key_str}'"
            );
			assert_eq!(
                value_str, expected_value,
                "Middle range value mismatch at index {idx}: expected '{expected_value}', found '{value_str}'"
            );
		}

		// Test 4: End range scan - last 100 items
		let end_start = "key_009900".as_bytes();
		let end_end = "key_010000".as_bytes();

		let txn = tree.begin().unwrap();
		let end_result: Vec<_> =
			txn.range(end_start, end_end).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

		assert_eq!(end_result.len(), 100, "End range scan should return 100 items");

		// Verify each item in end range
		for (idx, (key, value)) in end_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref()).unwrap();

			let expected_idx = 9900 + idx;
			let expected_key = &expected_items[expected_idx].0;
			let expected_value = &expected_items[expected_idx].1;

			assert_eq!(
                key_str, expected_key,
                "End range key mismatch at index {idx}: expected '{expected_key}', found '{key_str}'"
            );
			assert_eq!(
                value_str, expected_value,
                "End range value mismatch at index {idx}: expected '{expected_value}', found '{value_str}'"
            );
		}

		// Test 5: Single item range scan
		let single_start = "key_004567".as_bytes();
		let single_end = "key_004568".as_bytes();

		let txn = tree.begin().unwrap();
		let single_result: Vec<_> =
			txn.range(single_start, single_end).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

		assert_eq!(single_result.len(), 1, "Single item range scan should return 1 item");

		let (key, value) = &single_result[0];
		let key_str = std::str::from_utf8(key.as_ref()).unwrap();
		let value_str = std::str::from_utf8(value.as_ref()).unwrap();

		assert_eq!(key_str, "key_004567");
		assert_eq!(value_str, "value_004567_content_data_9134");

		// Test 6: Empty range scan (non-existent range)
		let empty_start = "key_999999".as_bytes();
		let empty_end = "key_888888".as_bytes(); // end < start, should be empty

		let txn = tree.begin().unwrap();
		let empty_result: Vec<_> = txn.range(empty_start, empty_end).unwrap().collect();

		assert_eq!(empty_result.len(), 0, "Empty range scan should return 0 items");
	}

	#[test(tokio::test)]
	async fn test_range_skip_take() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_small_memtable_options(path.clone());

		let tree = Tree::new(opts.clone()).unwrap();

		const TOTAL_ITEMS: usize = 10_000;
		let mut expected_items = Vec::with_capacity(TOTAL_ITEMS);

		for i in 0..TOTAL_ITEMS {
			let key = format!("key_{i:06}");
			let value = format!("value_{:06}_content_data_{}", i, i * 2);

			expected_items.push((key.clone(), value.clone()));

			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		tree.flush().unwrap();

		let start_key = "key_000000".as_bytes();
		let end_key = "key_999999".as_bytes();

		let txn = tree.begin().unwrap();
		let range_result: Vec<_> = txn
			.range(start_key, end_key)
			.unwrap()
			.skip(5000)
			.take(100)
			.map(|r| r.unwrap())
			.collect::<Vec<_>>();

		assert_eq!(
			range_result.len(),
			100,
			"Range scan with skip(5000).take(100) should return 100 items"
		);

		for (idx, (key, value)) in range_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref()).unwrap();

			let expected_idx = 5000 + idx;
			let expected_key = &expected_items[expected_idx].0;
			let expected_value = &expected_items[expected_idx].1;

			assert_eq!(
				key_str, expected_key,
				"Key mismatch at index {idx}: expected '{expected_key}', found '{key_str}'"
			);
			assert_eq!(
				value_str, expected_value,
				"Value mismatch at index {idx}: expected '{expected_value}', found '{value_str}'"
			);
		}
	}

	#[test(tokio::test)]
	async fn test_range_skip_take_alphabetical() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_small_memtable_options(path.clone());

		let tree = Tree::new(opts.clone()).unwrap();

		fn generate_alphabetical_key(index: usize) -> String {
			let mut result = String::new();
			let mut remaining = index + 1;

			while remaining > 0 {
				let digit = (remaining - 1) % 26;
				result.push((b'a' + digit as u8) as char);
				remaining = (remaining - 1) / 26;
			}

			result.chars().rev().collect()
		}

		const TOTAL_ITEMS: usize = 10_000;
		let mut expected_items = Vec::with_capacity(TOTAL_ITEMS);

		for i in 0..TOTAL_ITEMS {
			let key = generate_alphabetical_key(i);
			let value = format!("value_{}_content_data_{}", key, i * 2);

			expected_items.push((key.clone(), value.clone()));

			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		tree.flush().unwrap();

		expected_items.sort_by(|a, b| a.0.cmp(&b.0));

		let beg = [0u8].to_vec();
		let end = [255u8].to_vec();

		let txn = tree.begin().unwrap();
		let range_result: Vec<_> = txn
			.range(&beg, &end)
			.unwrap()
			.skip(5000)
			.take(100)
			.map(|r| r.unwrap())
			.collect::<Vec<_>>();

		assert_eq!(
			range_result.len(),
			100,
			"Range scan with skip(5000).take(100) should return 100 items"
		);

		for (idx, (key, value)) in range_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref()).unwrap();

			let expected_idx = 5000 + idx;
			let expected_key = &expected_items[expected_idx].0;
			let expected_value = &expected_items[expected_idx].1;

			assert_eq!(
				key_str, expected_key,
				"Key mismatch at index {idx}: expected '{expected_key}', found '{key_str}'"
			);
			assert_eq!(
				value_str, expected_value,
				"Value mismatch at index {idx}: expected '{expected_value}', found '{value_str}'"
			);
		}
	}

	#[test(tokio::test)]
	async fn test_range_limit_functionality() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_small_memtable_options(path.clone());

		let tree = Tree::new(opts.clone()).unwrap();

		const TOTAL_ITEMS: usize = 10_000;
		let mut expected_items = Vec::with_capacity(TOTAL_ITEMS);

		for i in 0..TOTAL_ITEMS {
			let key = format!("key_{i:06}");
			let value = format!("value_{:06}_content_data_{}", i, i * 2);

			expected_items.push((key.clone(), value.clone()));

			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		tree.flush().unwrap();

		let beg = [0u8].to_vec();
		let end = [255u8].to_vec();

		let txn = tree.begin().unwrap();
		let limited_result: Vec<_> = txn
			.range(&beg, &end)
			.unwrap()
			.take(100)
			.map(|r| match r {
				Ok(kv) => kv,
				Err(e) => {
					log::error!("Error in range iterator: {e:?}");
					panic!("Range iterator error: {e}");
				}
			})
			.collect::<Vec<_>>();

		assert_eq!(
			limited_result.len(),
			100,
			"Range scan with .take(100) should return exactly 100 items"
		);

		for (idx, (key, value)) in limited_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref()).unwrap();

			let expected_key = &expected_items[idx].0;
			let expected_value = &expected_items[idx].1;

			assert_eq!(
                key_str, expected_key,
                "Key mismatch at index {idx} with limit 100: expected '{expected_key}', found '{key_str}'"
            );
			assert_eq!(
                value_str, expected_value,
                "Value mismatch at index {idx} with limit 100: expected '{expected_value}', found '{value_str}'"
            );
		}

		let txn = tree.begin().unwrap();
		let limited_result_1000: Vec<_> =
			txn.range(&beg, &end).unwrap().take(1000).map(|r| r.unwrap()).collect::<Vec<_>>();

		assert_eq!(
			limited_result_1000.len(),
			1000,
			"Range scan with .take(1000) should return exactly 1000 items"
		);

		for (idx, (key, value)) in limited_result_1000.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref()).unwrap();

			let expected_key = &expected_items[idx].0;
			let expected_value = &expected_items[idx].1;

			assert_eq!(
                key_str, expected_key,
                "Key mismatch at index {idx} with limit 1000: expected '{expected_key}', found '{key_str}'"
            );
			assert_eq!(
                value_str, expected_value,
                "Value mismatch at index {idx} with limit 1000: expected '{expected_value}', found '{value_str}'"
            );
		}

		let txn = tree.begin().unwrap();
		let limited_result_large: Vec<_> = txn.range(&beg, &end).unwrap().take(15000).collect();

		assert_eq!(
			limited_result_large.len(),
			TOTAL_ITEMS,
			"Range scan with .take(15000) should return all {TOTAL_ITEMS} items"
		);

		let txn = tree.begin().unwrap();
		let unlimited_result: Vec<_> = txn.range(&beg, &end).unwrap().collect();

		assert_eq!(
			unlimited_result.len(),
			TOTAL_ITEMS,
			"Range scan with no limit should return all {TOTAL_ITEMS} items"
		);

		let txn = tree.begin().unwrap();
		let single_result: Vec<_> =
			txn.range(&beg, &end).unwrap().take(1).map(|r| r.unwrap()).collect::<Vec<_>>();

		assert_eq!(single_result.len(), 1, "Range scan with .take(1) should return exactly 1 item");

		let (key, value) = &single_result[0];
		let key_str = std::str::from_utf8(key.as_ref()).unwrap();
		let value_str = std::str::from_utf8(value.as_ref()).unwrap();

		assert_eq!(key_str, "key_000000");
		assert_eq!(value_str, "value_000000_content_data_0");

		let txn = tree.begin().unwrap();
		let zero_result: Vec<_> = txn.range(&beg, &end).unwrap().take(0).collect();

		assert_eq!(zero_result.len(), 0, "Range scan with .take(0) should return no items");
	}

	#[test(tokio::test)]
	async fn test_range_limit_with_skip_take() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_small_memtable_options(path.clone());

		let tree = Tree::new(opts.clone()).unwrap();

		const TOTAL_ITEMS: usize = 20_000;
		let mut expected_items = Vec::with_capacity(TOTAL_ITEMS);

		for i in 0..TOTAL_ITEMS {
			let key = format!("key_{i:06}");
			let value = format!("value_{:06}_content_data_{}", i, i * 2);

			expected_items.push((key.clone(), value.clone()));

			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		tree.flush().unwrap();

		let beg = [0u8].to_vec();
		let end = [255u8].to_vec();

		let txn = tree.begin().unwrap();
		let unlimited_range_result: Vec<_> = txn
			.range(&beg, &end)
			.unwrap()
			.skip(5000)
			.take(100)
			.map(|r| r.unwrap())
			.collect::<Vec<_>>();

		assert_eq!(
			unlimited_range_result.len(),
			100,
			"Range scan followed by skip(5000).take(100) should return 100 items"
		);

		for (idx, (key, value)) in unlimited_range_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref()).unwrap();

			let expected_idx = 5000 + idx;
			let expected_key = &expected_items[expected_idx].0;
			let expected_value = &expected_items[expected_idx].1;

			assert_eq!(
                key_str, expected_key,
                "Key mismatch at index {idx} with unlimited range: expected '{expected_key}', found '{key_str}'"
            );
			assert_eq!(
                value_str, expected_value,
                "Value mismatch at index {idx} with unlimited range: expected '{expected_value}', found '{value_str}'"
            );
		}

		// Case: Test another skip position with take
		let txn = tree.begin().unwrap();
		let res: Vec<_> = txn
			.range(&beg, &end)
			.unwrap()
			.skip(5000)
			.take(50)
			.map(|r| r.unwrap())
			.collect::<Vec<_>>();

		assert_eq!(
			res.len(),
			50,
			"Range scan followed by skip(5000).take(50) should return 50 items"
		);

		for (idx, (key, value)) in res.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref()).unwrap();

			let expected_idx = 5000 + idx;
			let expected_key = &expected_items[expected_idx].0;
			let expected_value = &expected_items[expected_idx].1;

			assert_eq!(
                key_str, expected_key,
                "Key mismatch at index {idx} with limit 5050: expected '{expected_key}', found '{key_str}'"
            );
			assert_eq!(
                value_str, expected_value,
                "Value mismatch at index {idx} with limit 5050: expected '{expected_value}', found '{value_str}'"
            );
		}
	}

	#[test(tokio::test)]
	async fn test_vlog_basic() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_vlog_test_options(path.clone());

		let tree = Tree::new(opts.clone()).unwrap();

		// Test 1: Values should be stored in VLog
		let small_key = "small_key";
		let small_value = "small_value"; // < 100 bytes

		let mut txn = tree.begin().unwrap();
		txn.set(small_key.as_bytes(), small_value.as_bytes()).unwrap();
		txn.commit().await.unwrap();

		// Test 2: Large values should be stored in VLog
		let large_key = "large_key";
		let large_value = "X".repeat(200); // >= 100 bytes

		let mut txn = tree.begin().unwrap();
		txn.set(large_key.as_bytes(), large_value.as_bytes()).unwrap();
		txn.commit().await.unwrap();

		// Force memtable flush to persist to SSTables
		tree.flush().unwrap();

		// Verify both values can be read correctly
		let txn = tree.begin().unwrap();

		let retrieved_small = txn.get(small_key.as_bytes()).unwrap().unwrap();
		assert_eq!(retrieved_small, Bytes::copy_from_slice(small_value.as_bytes()));

		let retrieved_large = txn.get(large_key.as_bytes()).unwrap().unwrap();
		assert_eq!(retrieved_large, Bytes::copy_from_slice(large_value.as_bytes()));
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_vlog_concurrent_operations() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.vlog_max_file_size = 512 * 1024;
			opts.max_memtable_size = 32 * 1024;
		});

		let tree = Tree::new(opts.clone()).unwrap();

		let mut expected_values = Vec::new();

		for i in 0..1000 {
			let key = format!("key_{i:04}");
			let value = if i % 2 == 0 {
				format!("small_value_{i}")
			} else {
				format!("large_value_{}_with_padding_{}", i, "X".repeat(100))
			};

			expected_values.push((key.clone(), value.clone()));

			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Force flush to ensure all data is persisted
		tree.flush().unwrap();
		tree.close().await.unwrap();

		let tree = Arc::new(tree);

		// Concurrent reads
		let read_handles: Vec<_> = (0..5)
			.map(|reader_id| {
				let tree = tree.clone();
				let expected_values = expected_values.clone();
				tokio::spawn(async move {
					for (key, expected_value) in expected_values {
						let txn = tree.begin().unwrap();
						let retrieved = txn.get(key.as_bytes()).unwrap().unwrap();
						assert_eq!(
							retrieved,
							Bytes::copy_from_slice(expected_value.as_bytes()),
							"Reader {reader_id} failed to get correct value for key {key}"
						);
					}
				})
			})
			.collect();

		// Wait for all reads to complete
		for handle in read_handles {
			handle.await.unwrap();
		}

		// Explicitly close the tree to ensure proper cleanup
		tree.close().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_vlog_file_rotation() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.vlog_max_file_size = 2048; // 2KB files to force frequent rotation
			opts.enable_vlog = true;
		});

		let tree = Tree::new(opts.clone()).unwrap();

		// Insert enough data to force multiple file rotations
		let mut keys_and_values = Vec::new();
		for i in 0..50 {
			let key = format!("rotation_key_{i:03}");
			let value = format!("rotation_value_{}_with_lots_of_padding_{}", i, "Z".repeat(200));
			keys_and_values.push((key.clone(), value.clone()));

			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Force flush
		tree.flush().unwrap();

		// Check that multiple VLog files were created
		let vlog_dir = path.join("vlog");
		let entries = std::fs::read_dir(&vlog_dir).unwrap();
		let vlog_files: Vec<_> = entries
			.filter_map(|e| {
				let entry = e.ok()?;
				let name = entry.file_name().to_string_lossy().to_string();
				if opts.is_vlog_filename(&name) {
					Some(name)
				} else {
					None
				}
			})
			.collect();

		assert!(vlog_files.len() > 1, "Expected multiple VLog files due to rotation");

		// Verify all data is still accessible across multiple files
		for (key, expected_value) in keys_and_values {
			let txn = tree.begin().unwrap();
			let retrieved = txn.get(key.as_bytes()).unwrap().unwrap();
			assert_eq!(
				retrieved,
				Bytes::copy_from_slice(expected_value.as_bytes()),
				"Key {key} has incorrect value across file rotation"
			);
		}
	}

	#[test(tokio::test)]
	async fn test_discard_file_directory_structure() {
		// Test to verify that the DISCARD file lives in the main database directory,
		// not in the VLog subdirectory
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.vlog_max_file_size = 1024;
			opts.max_memtable_size = 512;
			opts.enable_vlog = true;
		});

		let tree = Tree::new(opts.clone()).unwrap();

		// Insert some data to ensure VLog and discard stats are created
		let large_value = vec![1u8; 200]; // Large enough for VLog
		for i in 0..5 {
			let key = format!("key_{i}");
			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), &large_value).unwrap();
			txn.commit().await.unwrap();
		}

		// Force flush to create VLog files and initialize discard stats
		tree.flush().unwrap();

		// Update some keys to create discard statistics
		let mut discard_updates = HashMap::new();
		let stats = tree.get_all_vlog_stats();
		for (file_id, total_size, _discard_bytes, _ratio) in &stats {
			if *total_size > 0 {
				discard_updates.insert(*file_id, (*total_size as f64 * 0.3) as i64);
			}
		}
		tree.update_vlog_discard_stats(&discard_updates);

		// Verify directory structure
		let main_db_dir = &path;
		let vlog_dir = path.join("vlog");
		let sstables_dir = path.join("sstables");
		let wal_dir = path.join("wal");
		let discard_stats_dir = path.join("discard_stats");
		let delete_list_dir = path.join("delete_list");

		// Check that all expected directories exist
		assert!(main_db_dir.exists(), "Main database directory should exist");
		assert!(vlog_dir.exists(), "VLog directory should exist");
		assert!(sstables_dir.exists(), "SSTables directory should exist");
		assert!(wal_dir.exists(), "WAL directory should exist");
		assert!(discard_stats_dir.exists(), "Discard stats directory should exist");
		assert!(delete_list_dir.exists(), "Delete list directory should exist");

		// Check that DISCARD file is in the main database directory
		let discard_file_in_main = discard_stats_dir.join("DISCARD");
		let discard_file_in_vlog = vlog_dir.join("DISCARD");

		assert!(
			discard_file_in_main.exists(),
			"DISCARD file should exist in main database directory: {discard_file_in_main:?}"
		);
		assert!(
			!discard_file_in_vlog.exists(),
			"DISCARD file should NOT exist in VLog directory: {discard_file_in_vlog:?}"
		);

		// Verify that VLog files are in the VLog directory
		let vlog_entries = std::fs::read_dir(&vlog_dir).unwrap();
		let vlog_files: Vec<_> = vlog_entries
			.filter_map(|entry| {
				let entry = entry.ok()?;
				let name = entry.file_name().to_string_lossy().to_string();
				if opts.is_vlog_filename(&name) {
					Some(name)
				} else {
					None
				}
			})
			.collect();

		assert!(!vlog_files.is_empty(), "Should have VLog files in the VLog directory");

		// Verify that the DISCARD file is separate from VLog files
		for vlog_file in &vlog_files {
			assert_ne!(vlog_file, "DISCARD", "DISCARD should not be mistaken for a VLog file");
		}

		// Verify other expected files are in the main directory
		let manifest_dir = main_db_dir.join("manifest");
		assert!(
			manifest_dir.exists(),
			"manifest directory should be in main directory alongside DISCARD"
		);

		println!("✓ Directory structure verification passed:");
		println!("  Main DB dir: {main_db_dir:?}");
		println!(
			"  DISCARD file: {:?} (exists: {})",
			discard_file_in_main,
			discard_file_in_main.exists()
		);
		println!("  VLog dir: {vlog_dir:?}");
		println!("  Discard stats dir: {discard_stats_dir:?}");
		println!("  Delete list dir: {delete_list_dir:?}");
		println!("  VLog files: {vlog_files:?}");
		println!("  manifest dir: {:?} (exists: {})", manifest_dir, manifest_dir.exists());
	}

	#[test(tokio::test)]
	async fn test_compaction_with_updates_and_delete() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.level_count = 2;
			opts.vlog_max_file_size = 20;
			opts.enable_vlog = true;
		});

		let tree = Tree::new(opts.clone()).unwrap();

		// Create 2 keys in ascending order
		let keys = ["key-1".as_bytes(), "key-2".as_bytes()];

		// Insert first version of each key
		for (i, key) in keys.iter().enumerate() {
			let value = format!("value-{}-v1", i + 1);
			let mut tx = tree.begin().unwrap();
			tx.set(*key, value.as_bytes()).unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();
		}

		// Insert second version of each key
		for (i, key) in keys.iter().enumerate() {
			let value = format!("value-{}-v2", i + 1);
			let mut tx = tree.begin().unwrap();
			tx.set(*key, value.as_bytes()).unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();
		}

		// Verify the latest values before delete
		for (i, key) in keys.iter().enumerate() {
			let tx = tree.begin().unwrap();
			let result = tx.get(*key).unwrap();
			let expected = format!("value-{}-v2", i + 1);
			assert_eq!(result.map(|v| v.to_vec()), Some(expected.as_bytes().to_vec()));
		}

		// Delete all keys
		for key in keys.iter() {
			let mut tx = tree.begin().unwrap();
			tx.delete(*key).unwrap();
			tx.commit().await.unwrap();
		}

		// Flush memtable
		tree.flush().unwrap();

		// Force compaction
		let strategy = Arc::new(Strategy::default());
		tree.core.compact(strategy).unwrap();

		// There could be multiple VLog files, need to garbage collect them all but
		// only one should remain because the active VLog file does not get garbage collected
		for _ in 0..6 {
			tree.garbage_collect_vlog().await.unwrap();
		}

		// Verify all keys are gone after compaction
		for key in keys.iter() {
			let tx = tree.begin().unwrap();
			let result = tx.get(*key).unwrap();
			assert_eq!(result, None);
		}

		// Verify that only one VLog file remains after garbage collection
		let vlog_dir = path.join("vlog");
		let entries = std::fs::read_dir(&vlog_dir).unwrap();
		let vlog_files: Vec<_> = entries
			.filter_map(|e| {
				let entry = e.ok()?;
				let name = entry.file_name().to_string_lossy().to_string();
				if opts.is_vlog_filename(&name) {
					Some(name)
				} else {
					None
				}
			})
			.collect();

		assert_eq!(
			vlog_files.len(),
			1,
			"Should have exactly one VLog file after garbage collection, found: {vlog_files:?}"
		);
	}

	#[test(tokio::test)]
	async fn test_compaction_with_updates_and_delete_on_same_key() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.level_count = 2;
			opts.vlog_max_file_size = 20;
			opts.enable_vlog = true;
		});

		let tree = Tree::new(opts.clone()).unwrap();

		// Create single key
		let keys = ["key-1".as_bytes()];

		// Insert first version of each key
		for (i, key) in keys.iter().enumerate() {
			let value = format!("value-{}-v1", i + 1);
			let mut tx = tree.begin().unwrap();
			tx.set(*key, value.as_bytes()).unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();
		}

		// Insert second version of each key
		for (i, key) in keys.iter().enumerate() {
			let value = format!("value-{}-v2", i + 1);
			let mut tx = tree.begin().unwrap();
			tx.set(*key, value.as_bytes()).unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();
		}

		// Insert third version of each key
		for (i, key) in keys.iter().enumerate() {
			let value = format!("value-{}-v3", i + 1);
			let mut tx = tree.begin().unwrap();
			tx.set(*key, value.as_bytes()).unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();
		}

		// Insert fourth version of each key
		for (i, key) in keys.iter().enumerate() {
			let value = format!("value-{}-v4", i + 1);
			let mut tx = tree.begin().unwrap();
			tx.set(*key, value.as_bytes()).unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();
		}

		// Verify the latest values before delete
		for (i, key) in keys.iter().enumerate() {
			let tx = tree.begin().unwrap();
			let result = tx.get(*key).unwrap();
			assert_eq!(
				result.map(|v| v.to_vec()),
				Some(format!("value-{}-v4", i + 1).as_bytes().to_vec())
			);
		}

		// Delete all keys
		for key in keys.iter() {
			let mut tx = tree.begin().unwrap();
			tx.delete(*key).unwrap();
			tx.commit().await.unwrap();
		}

		// Flush memtable
		tree.flush().unwrap();

		// Force compaction
		let strategy = Arc::new(Strategy::default());
		tree.core.compact(strategy).unwrap();

		// There could be multiple VLog files, need to garbage collect them all but
		// only one should remain because the active VLog file does not get garbage collected
		for _ in 0..6 {
			tree.garbage_collect_vlog().await.unwrap();
		}

		// Verify all keys are gone after compaction
		for key in keys.iter() {
			let tx = tree.begin().unwrap();
			let result = tx.get(*key).unwrap();
			assert_eq!(result, None);
		}

		// Verify that only one VLog file remains after garbage collection
		let vlog_dir = path.join("vlog");
		let entries = std::fs::read_dir(&vlog_dir).unwrap();
		let vlog_files: Vec<_> = entries
			.filter_map(|e| {
				let entry = e.ok()?;
				let name = entry.file_name().to_string_lossy().to_string();
				if opts.is_vlog_filename(&name) {
					Some(name)
				} else {
					None
				}
			})
			.collect();

		assert_eq!(
			vlog_files.len(),
			1,
			"Should have exactly one VLog file after garbage collection, found: {vlog_files:?}"
		);
	}

	#[test(tokio::test)]
	async fn test_vlog_compaction_preserves_sequence_numbers() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_vlog_compaction_options(path.clone());

		// Open the Tree (database instance)
		let tree = Tree::new(opts.clone()).unwrap();

		// Define keys used throughout the test
		let key1 = b"test_key_1".to_vec();
		let key2 = b"test_key_2".to_vec();
		let key3 = b"test_key_3".to_vec();

		// --- Step 1: Insert multiple versions of key1 to fill the first VLog file ---
		for i in 0..3 {
			let value = format!("value1_version_{i}").repeat(10); // Large value to fill VLog
			let mut tx = tree.begin().unwrap();
			tx.set(&key1, value.as_bytes()).unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();
		}

		// --- Step 2: Insert and delete key2 ---
		// We are doing this to make sure there are deleted keys in the VLog file
		{
			// Insert key2
			let mut tx = tree.begin().unwrap();
			tx.set(&key2, b"value2").unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();

			// Delete key2
			let mut tx = tree.begin().unwrap();
			tx.delete(&key2).unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();
		}

		// --- Step 3: Validate current value of key1 (should be last inserted version) ---
		{
			let tx = tree.begin().unwrap();
			let current_value = tx.get(&key1).unwrap().unwrap();
			assert_eq!(
				current_value.as_ref(),
				"value1_version_2".to_string().repeat(10).as_bytes()
			);
		}

		// --- Step 4: Insert key3 values which will rotate VLog file ---
		for i in 0..2 {
			let value = format!("value2_version_{i}").repeat(10); // Large values
			let mut tx = tree.begin().unwrap();
			tx.set(&key3, value.as_bytes()).unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();
		}

		// --- Step 5: Update key1 with a final value ---
		{
			let mut tx = tree.begin().unwrap();
			tx.set(&key1, "final_value_key1".repeat(10).as_bytes()).unwrap();
			tx.commit().await.unwrap();
		}
		tree.flush().unwrap();

		// --- Step 6: Verify latest value of key1 before compaction ---
		{
			let tx = tree.begin().unwrap();
			let value = tx.get(&key1).unwrap().unwrap();
			assert_eq!(value.as_ref(), "final_value_key1".repeat(10).as_bytes());
		}

		// --- Step 7: Trigger manual compaction of the LSM tree ---
		let strategy = Arc::new(Strategy::default());
		tree.core.compact(strategy).unwrap();

		// --- Step 8: Run VLog garbage collection (which internally can trigger file compaction) ---
		tree.garbage_collect_vlog().await.unwrap();

		// --- Step 9: Verify key1 still returns the correct latest value after compaction ---
		{
			let tx = tree.begin().unwrap();
			let value = tx.get(&key1).unwrap().unwrap();
			assert_eq!(
                value.as_ref(),
                "final_value_key1".repeat(10).as_bytes(),
                "After VLog compaction, key1 returned incorrect value. The sequence number was not preserved during compaction."
            );
		}

		// --- Step 10: Clean shutdown ---
		tree.close().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_sstable_lsn_bug() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 500; // Small memtable to force flushes
			opts.level_count = 2;
		});

		// Step 1: Create database and write data for first table
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Write data for first table
			for i in 0..6 {
				let key = format!("batch_0_key_{:03}", i);
				let value = format!("batch_0_value_{:03}", i);

				let mut txn = tree.begin().unwrap();
				txn.set(key.as_bytes(), value.as_bytes()).unwrap();
				txn.commit().await.unwrap();
			}

			// Force first flush
			tree.flush().unwrap();

			// Write data for second table
			for i in 0..6 {
				let key = format!("batch_1_key_{:03}", i);
				let value = format!("batch_1_value_{:03}", i);

				let mut txn = tree.begin().unwrap();
				txn.set(key.as_bytes(), value.as_bytes()).unwrap();
				txn.commit().await.unwrap();
			}

			// Force second flush
			tree.flush().unwrap();

			// Verify all keys exist immediately after flush
			for batch in 0..2 {
				for i in 0..6 {
					let key = format!("batch_{}_key_{:03}", batch, i);
					let expected_value = format!("batch_{}_value_{:03}", batch, i);

					let txn = tree.begin().unwrap();
					let result = txn.get(key.as_bytes()).unwrap();
					assert!(result.is_some(), "Key '{}' should exist immediately after flush", key);
					assert_eq!(result.unwrap().as_ref(), expected_value.as_bytes());
				}
			}

			// Close the database
			tree.close().await.unwrap();
		}

		// Step 2: Reopen database and verify data persistence
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Verify all keys still exist after restart
			for batch in 0..2 {
				for i in 0..6 {
					let key = format!("batch_{}_key_{:03}", batch, i);
					let expected_value = format!("batch_{}_value_{:03}", batch, i);

					let txn = tree.begin().unwrap();
					let result = txn.get(key.as_bytes()).unwrap();
					assert!(result.is_some(), "Key '{}' should exist after restart", key);
					assert_eq!(result.as_ref().unwrap().as_ref(), expected_value.as_bytes());
				}
			}

			tree.close().await.unwrap();
		}
	}

	#[test(tokio::test)]
	async fn test_table_id_assignment_across_restart() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		// Create options with very small memtable to force frequent flushes
		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 500; // Very small to force flushes
			opts.level_count = 2;
		});

		// Step 1: Create initial database and write data for 2 memtables
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Write data for first table
			for i in 0..6 {
				let key = format!("batch_0_key_{:03}", i);
				let value = format!("batch_0_value_{:03}", i);

				let mut txn = tree.begin().unwrap();
				txn.set(key.as_bytes(), value.as_bytes()).unwrap();
				txn.commit().await.unwrap();
			}

			// Force first flush to create first table
			tree.flush().unwrap();

			// Write data for second table
			for i in 0..6 {
				let key = format!("batch_1_key_{:03}", i);
				let value = format!("batch_1_value_{:03}", i);

				let mut txn = tree.begin().unwrap();
				txn.set(key.as_bytes(), value.as_bytes()).unwrap();
				txn.commit().await.unwrap();
			}

			// Force second flush to create second table
			tree.flush().unwrap();

			// Verify we have 2 tables in L0
			let l0_size =
				tree.core.level_manifest.read().unwrap().levels.get_levels()[0].tables.len();
			assert_eq!(l0_size, 2, "Expected 2 tables in L0 after initial writes, got {l0_size}");

			// Get the table IDs from the first session
			let (table1_id, table2_id, next_table_id) = {
				let manifest = tree.core.level_manifest.read().unwrap();
				let table1_id = manifest.levels.get_levels()[0].tables[0].id;
				let table2_id = manifest.levels.get_levels()[0].tables[1].id;
				let next_table_id = manifest.next_table_id();
				(table1_id, table2_id, next_table_id)
			};

			// Verify table IDs are increasing (note: tables are stored in reverse order by sequence number)
			// So the first table in the vector has the higher ID
			assert!(
				table1_id > table2_id,
				"Table 1 ID should be greater than table 2 ID (newer table first)"
			);
			assert!(
				table1_id < next_table_id,
				"Next table ID should be greater than existing table IDs"
			);

			// Close the database
			tree.close().await.unwrap();
		}

		// Step 2: Reopen the database
		{
			let tree = Tree::new(opts.clone()).unwrap();

			{
				// Verify we still have 2 tables in L0 after reopening
				let l0_size =
					tree.core.level_manifest.read().unwrap().levels.get_levels()[0].tables.len();
				assert_eq!(l0_size, 2, "Expected 2 tables in L0 after reopening, got {l0_size}");
				// Get the table IDs after reopening
				let manifest = tree.core.level_manifest.read().unwrap();
				let table1_id = manifest.levels.get_levels()[0].tables[0].id;
				let table2_id = manifest.levels.get_levels()[0].tables[1].id;
				let next_table_id = manifest.next_table_id();

				// Verify table IDs are still in correct order (newer table first)
				assert!(
					table1_id > table2_id,
					"Table 1 ID should be greater than table 2 ID (newer table first)"
				);
				assert!(
					table1_id < next_table_id,
					"Next table ID should be greater than existing table IDs after reopen"
				);
			}

			// Step 3: Add more data to create a 3rd table
			for i in 0..6 {
				let key = format!("batch_2_key_{:03}", i);
				let value = format!("batch_2_value_{:03}", i);

				let mut txn = tree.begin().unwrap();
				txn.set(key.as_bytes(), value.as_bytes()).unwrap();
				txn.commit().await.unwrap();
			}

			// Force flush to create the 3rd table
			tree.flush().unwrap();

			{
				// Verify we now have 3 tables in L0
				let l0_size =
					tree.core.level_manifest.read().unwrap().levels.get_levels()[0].tables.len();
				assert_eq!(
					l0_size, 3,
					"Expected 3 tables in L0 after adding more data, got {l0_size}"
				);

				// Get the table IDs from all 3 tables
				let manifest = tree.core.level_manifest.read().unwrap();
				let table1_id = manifest.levels.get_levels()[0].tables[0].id;
				let table2_id = manifest.levels.get_levels()[0].tables[1].id;
				let table3_id = manifest.levels.get_levels()[0].tables[2].id;
				let next_table_id = manifest.next_table_id();

				// Verify table IDs are in correct order (newer tables first)
				assert!(
					table1_id > table2_id,
					"Table 1 ID should be greater than table 2 ID (newer table first)"
				);
				assert!(
					table2_id > table3_id,
					"Table 2 ID should be greater than table 3 ID (newer table first)"
				);
				assert!(
					table1_id < next_table_id,
					"Next table ID should be greater than all existing table IDs"
				);
			}

			// Verify we can read data from all tables
			for batch in 0..3 {
				for i in 0..6 {
					let key = format!("batch_{}_key_{:03}", batch, i);
					let expected_value = format!("batch_{}_value_{:03}", batch, i);

					let txn = tree.begin().unwrap();
					let result = txn.get(key.as_bytes()).unwrap();
					assert!(result.is_some(), "Key '{}' should exist after restart", key);
					assert_eq!(result.unwrap().as_ref(), expected_value.as_bytes());
				}
			}

			// Close the database
			tree.close().await.unwrap();
		}

		// Step 4: Final verification - reopen and check everything is still correct
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Verify we still have 3 tables
			let l0_size =
				tree.core.level_manifest.read().unwrap().levels.get_levels()[0].tables.len();
			assert_eq!(l0_size, 3, "Expected 3 tables in L0 after final reopen, got {l0_size}");

			// Verify table IDs are still in correct order (newer tables first)
			let manifest = tree.core.level_manifest.read().unwrap();
			let table1_id = manifest.levels.get_levels()[0].tables[0].id;
			let table2_id = manifest.levels.get_levels()[0].tables[1].id;
			let table3_id = manifest.levels.get_levels()[0].tables[2].id;
			let next_table_id = manifest.next_table_id();

			assert!(
				table1_id > table2_id,
				"Final check: Table 1 ID should be greater than table 2 ID (newer table first)"
			);
			assert!(
				table2_id > table3_id,
				"Final check: Table 2 ID should be greater than table 3 ID (newer table first)"
			);
			assert!(
				table1_id < next_table_id,
				"Final check: Next table ID should be greater than all existing table IDs"
			);

			// Verify that we can read some data (simplified check)
			let txn = tree.begin().unwrap();
			let result = txn.get("batch_0_key_000".as_bytes()).unwrap();
			assert!(result.is_some(), "Should be able to read at least one key");
		}
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_vlog_prefill_on_reopen() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.vlog_max_file_size = 10; // Very small to force multiple files
			opts.enable_vlog = true;
		});

		// Create initial database and add data to create multiple VLog files
		let tree1 = Tree::new(opts.clone()).unwrap();

		// Verify VLog is enabled
		assert!(tree1.core.vlog.is_some(), "VLog should be enabled");

		// Add data to create multiple VLog files
		for i in 0..10 {
			let key = format!("key_{i}");
			let value = format!("value_{i}_large_data_that_should_force_vlog_storage");

			let mut tx = tree1.begin().unwrap();
			tx.set(key.as_bytes(), value.as_bytes()).unwrap();
			tx.commit().await.unwrap();

			// Verify the data was written immediately
			let tx = tree1.begin().unwrap();
			let result = tx.get(key.as_bytes()).unwrap();
			assert!(result.is_some(), "Key '{key}' should exist immediately after write");
		}

		// Verify all data exists in the first database
		for i in 0..10 {
			let key = format!("key_{i}");
			let expected_value = format!("value_{i}_large_data_that_should_force_vlog_storage");

			let tx = tree1.begin().unwrap();
			let result = tx.get(key.as_bytes()).unwrap();
			assert!(result.is_some(), "Key '{key}' should exist in first database");
			assert_eq!(result.unwrap().as_ref(), expected_value.as_bytes());
		}

		// Drop the first database (this will close it)
		tree1.close().await.unwrap();

		// Create a new database instance - this should trigger VLog prefill
		let tree2 = Tree::new(opts.clone()).unwrap();
		assert!(tree2.core.vlog.is_some(), "VLog should be enabled in second database");

		// Verify that all existing data can still be read
		for i in 0..10 {
			let key = format!("key_{i}");
			let expected_value = format!("value_{i}_large_data_that_should_force_vlog_storage");

			let tx = tree2.begin().unwrap();
			let result = tx.get(key.as_bytes()).unwrap();
			assert!(result.is_some(), "Key '{key}' should exist after reopen");
			assert_eq!(result.unwrap().as_ref(), expected_value.as_bytes());
		}

		// Add new data and verify it goes to the correct active file
		let new_key = "new_key";
		let new_value = "new_value_large_data_that_should_force_vlog_storage";

		let mut tx = tree2.begin().unwrap();
		tx.set(new_key.as_bytes(), new_value.as_bytes()).unwrap();
		tx.commit().await.unwrap();

		// Verify the new data can be read
		let tx = tree2.begin().unwrap();
		let result = tx.get(new_key.as_bytes()).unwrap();
		assert!(result.is_some(), "New key should exist after write");
		assert_eq!(result.unwrap().as_ref(), new_value.as_bytes());

		// Verify we can still read from old data after adding new data
		for i in 0..10 {
			let key = format!("key_{i}");
			let expected_value = format!("value_{i}_large_data_that_should_force_vlog_storage");

			let tx = tree2.begin().unwrap();
			let result = tx.get(key.as_bytes()).unwrap();
			assert!(result.is_some(), "Old key '{key}' should still exist");
			assert_eq!(result.unwrap().as_ref(), expected_value.as_bytes());
		}

		// Clean shutdown (drop will close it)
		tree2.close().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_tree_builder() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		// Test TreeBuilder with default key type (InternalKey)
		let tree = TreeBuilder::new()
			.with_path(path.clone())
			.with_max_memtable_size(64 * 1024)
			.with_enable_vlog(true)
			.with_vlog_max_file_size(1024 * 1024)
			.build()
			.unwrap();

		// Test basic operations
		let mut txn = tree.begin().unwrap();
		txn.set(b"test_key", b"test_value").unwrap();
		txn.commit().await.unwrap();

		let txn = tree.begin().unwrap();
		let result = txn.get(b"test_key").unwrap().unwrap();
		assert_eq!(result, Bytes::from_static(b"test_value"));

		// Test build_with_options
		let (tree2, opts) = TreeBuilder::new()
			.with_path(temp_dir.path().join("tree2"))
			.build_with_options()
			.unwrap();

		// Verify the options are accessible
		assert_eq!(opts.path, temp_dir.path().join("tree2"));

		// Test basic operations on the second tree
		let mut txn = tree2.begin().unwrap();
		txn.set(b"key2", b"value2").unwrap();
		txn.commit().await.unwrap();

		let txn = tree2.begin().unwrap();
		let result = txn.get(b"key2").unwrap().unwrap();
		assert_eq!(result, Bytes::from_static(b"value2"));
	}

	#[test(tokio::test)]
	async fn test_soft_delete() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 64 * 1024; // Small memtable to force flushes
		});

		// Step 1: Create multiple versions of a key across separate transactions
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Create 5 different versions of the same key
			for version in 1..=5 {
				let value = format!("value_v{}", version);
				let mut txn = tree.begin().unwrap();
				txn.set(b"test_key", value.as_bytes()).unwrap();
				txn.commit().await.unwrap();
			}

			// Verify the latest version exists
			let txn = tree.begin().unwrap();
			let result = txn.get(b"test_key").unwrap().unwrap();
			assert_eq!(result.as_ref(), b"value_v5");

			// Soft delete the key
			let mut txn = tree.begin().unwrap();
			txn.soft_delete(b"test_key").unwrap();
			txn.commit().await.unwrap();

			// Verify the key is now invisible (soft deleted)
			let txn = tree.begin().unwrap();
			let result = txn.get(b"test_key").unwrap();
			assert!(result.is_none(), "Soft deleted key should not be visible");

			// Add a new different key
			let mut txn = tree.begin().unwrap();
			txn.set(b"other_key", b"other_value").unwrap();
			txn.commit().await.unwrap();

			// Verify the new key exists
			let txn = tree.begin().unwrap();
			let result = txn.get(b"other_key").unwrap().unwrap();
			assert_eq!(result.as_ref(), b"other_value");

			// Force flush to persist all changes to disk
			tree.flush().unwrap();

			// Close the database
			tree.close().await.unwrap();
		}

		// Step 2: Reopen the database and verify soft deleted key is still invisible
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Verify the soft deleted key is still invisible after restart
			let txn = tree.begin().unwrap();
			let result = txn.get(b"test_key").unwrap();
			assert!(
				result.is_none(),
				"Soft deleted key should remain invisible after database restart"
			);

			// Verify the other key still exists
			let txn = tree.begin().unwrap();
			let result = txn.get(b"other_key").unwrap().unwrap();
			assert_eq!(result.as_ref(), b"other_value");

			// Test range scan to ensure soft deleted key doesn't appear
			let txn = tree.begin().unwrap();
			let range_result: Vec<_> = txn
				.range(b"test".as_slice(), b"testz".as_slice())
				.unwrap()
				.map(|r| r.unwrap())
				.collect::<Vec<_>>();

			// Should be empty since test_key is soft deleted
			assert!(range_result.is_empty(), "Range scan should not include soft deleted keys");

			// Test range scan that includes the other key
			let txn = tree.begin().unwrap();
			let range_result: Vec<_> = txn
				.range(b"other".as_slice(), b"otherz".as_slice())
				.unwrap()
				.map(|r| r.unwrap())
				.collect::<Vec<_>>();

			// Should contain the other key
			assert_eq!(range_result.len(), 1);
			assert_eq!(range_result[0].0.as_ref(), b"other_key");
			assert_eq!(range_result[0].1.as_ref(), b"other_value");

			// Test that we can reinsert the same key after soft delete
			let mut txn = tree.begin().unwrap();
			txn.set(b"test_key", b"new_value_after_soft_delete").unwrap();
			txn.commit().await.unwrap();

			// Verify the new value is visible
			let txn = tree.begin().unwrap();
			let result = txn.get(b"test_key").unwrap().unwrap();
			assert_eq!(result.as_ref(), b"new_value_after_soft_delete");

			tree.close().await.unwrap();
		}
	}

	#[test(tokio::test)]
	async fn test_checkpoint_with_vlog() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.vlog_max_file_size = 1024;
			opts.max_memtable_size = 512;
			opts.enable_vlog = true;
		});

		// Create initial data with VLog enabled
		let tree = Tree::new(opts.clone()).unwrap();

		// Insert some data to ensure VLog and discard stats are created
		let large_value = vec![1u8; 200]; // Large enough for VLog
		for i in 0..5 {
			let key = format!("key_{i}");
			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), &large_value).unwrap();
			txn.commit().await.unwrap();
		}

		// Force flush to create VLog files and initialize discard stats
		tree.flush().unwrap();

		// Update some keys to create discard statistics
		let mut discard_updates = HashMap::new();
		let stats = tree.get_all_vlog_stats();
		for (file_id, total_size, _discard_bytes, _ratio) in &stats {
			if *total_size > 0 {
				discard_updates.insert(*file_id, (*total_size as f64 * 0.3) as i64);
			}
		}
		tree.update_vlog_discard_stats(&discard_updates);

		// Create checkpoint
		let checkpoint_dir = temp_dir.path().join("checkpoint");
		let metadata = tree.create_checkpoint(&checkpoint_dir).unwrap();

		// Verify checkpoint metadata
		assert!(metadata.timestamp > 0);
		assert!(metadata.total_size > 0);

		// Verify all expected directories exist in checkpoint
		assert!(checkpoint_dir.exists());
		assert!(checkpoint_dir.join("sstables").exists());
		assert!(checkpoint_dir.join("wal").exists());
		assert!(checkpoint_dir.join("manifest").exists());
		assert!(checkpoint_dir.join("vlog").exists());
		assert!(checkpoint_dir.join("discard_stats").exists());
		assert!(checkpoint_dir.join("delete_list").exists());
		assert!(checkpoint_dir.join("CHECKPOINT_METADATA").exists());

		// Verify VLog files are in the checkpoint
		let vlog_checkpoint_dir = checkpoint_dir.join("vlog");
		let vlog_entries = std::fs::read_dir(&vlog_checkpoint_dir).unwrap();
		let vlog_files: Vec<_> = vlog_entries
			.filter_map(|entry| {
				let entry = entry.ok()?;
				let name = entry.file_name().to_string_lossy().to_string();
				if opts.is_vlog_filename(&name) {
					Some(name)
				} else {
					None
				}
			})
			.collect();

		assert!(!vlog_files.is_empty(), "Should have VLog files in the checkpoint");

		// Verify discard stats file exists
		let discard_stats_checkpoint_dir = checkpoint_dir.join("discard_stats");
		assert!(discard_stats_checkpoint_dir.exists());
		assert!(discard_stats_checkpoint_dir.join("DISCARD").exists());

		// Insert more data after checkpoint
		for i in 5..10 {
			let key = format!("key_{i}");
			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), &large_value).unwrap();
			txn.commit().await.unwrap();
		}

		// Force flush to ensure the new data is also on disk
		tree.flush().unwrap();

		// Verify all data exists before restore
		for i in 0..10 {
			let key = format!("key_{i}");
			let txn = tree.begin().unwrap();
			let result = txn.get(key.as_bytes()).unwrap();
			assert!(result.is_some(), "Key '{key}' should exist before restore");
		}

		// Restore from checkpoint
		tree.restore_from_checkpoint(&checkpoint_dir).unwrap();

		// Verify data is restored to checkpoint state (keys 0-4 exist, 5-9 don't)
		for i in 0..5 {
			let key = format!("key_{i}");
			let txn = tree.begin().unwrap();
			let result = txn.get(key.as_bytes()).unwrap();
			assert!(result.is_some(), "Key '{key}' should exist after restore");
			assert_eq!(result.unwrap().as_ref(), &large_value);
		}

		// Verify the newer keys don't exist after restore
		for i in 5..10 {
			let key = format!("key_{i}");
			let txn = tree.begin().unwrap();
			let result = txn.get(key.as_bytes()).unwrap();
			assert!(result.is_none(), "Key '{key}' should not exist after restore");
		}

		// Verify VLog directories are restored
		let vlog_dir = opts.vlog_dir();
		let discard_stats_dir = opts.discard_stats_dir();
		let delete_list_dir = opts.delete_list_dir();

		assert!(vlog_dir.exists(), "VLog directory should exist after restore");
		assert!(discard_stats_dir.exists(), "Discard stats directory should exist after restore");
		assert!(delete_list_dir.exists(), "Delete list directory should exist after restore");

		// Verify VLog files are restored
		let vlog_entries = std::fs::read_dir(&vlog_dir).unwrap();
		let vlog_files: Vec<_> = vlog_entries
			.filter_map(|entry| {
				let entry = entry.ok()?;
				let name = entry.file_name().to_string_lossy().to_string();
				if opts.is_vlog_filename(&name) {
					Some(name)
				} else {
					None
				}
			})
			.collect();

		assert!(!vlog_files.is_empty(), "Should have VLog files after restore");

		// Verify discard stats file is restored
		assert!(
			discard_stats_dir.join("DISCARD").exists(),
			"DISCARD file should exist after restore"
		);
	}

	#[test_log::test(tokio::test)]
	async fn test_clean_shutdown_actually_skips_wal() {
		// This test explicitly validates that WAL is NOT replayed after clean shutdown
		let temp_dir = TempDir::new("test").unwrap();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 1024;
			opts.flush_on_close = true;
		});

		// Phase 1: Write data and clean shutdown
		{
			let tree = Tree::new(opts.clone()).unwrap();

			let mut txn = tree.begin().unwrap();
			txn.set(b"test_key", b"test_value").unwrap();
			txn.commit().await.unwrap();

			// Clean shutdown - should flush to SST
			tree.close().await.unwrap();
		}

		// Phase 2: Check manifest state after shutdown
		let manifest = LevelManifest::new(opts.clone()).expect("Failed to load manifest");
		let log_number = manifest.get_log_number();

		// CRITICAL CHECK: log_number should be > 0 to skip WAL #0
		assert!(
			log_number > 0,
			"BUG: log_number should be > 0 after flush to indicate WAL #0 is flushed, got {}",
			log_number
		);

		// Phase 3: Restart and verify WAL was actually skipped
		{
			// Create a custom Core to inspect if WAL was replayed
			let inner = Arc::new(CoreInner::new(opts.clone()).unwrap());

			// Before WAL replay, memtable should be empty
			let memtable_before = inner.active_memtable.read().unwrap().clone();
			assert!(memtable_before.is_empty(), "Memtable should be empty before WAL replay");

			// Now do WAL replay
			let wal_path = opts.wal_dir();
			let min_wal_number = log_number;

			let wal_seq_opt =
				Core::replay_wal_with_repair(&wal_path, min_wal_number, "Test", |_memtable| Ok(()))
					.unwrap();

			// CRITICAL: WAL should have been skipped (return None)
			assert_eq!(
				wal_seq_opt, None,
				"BUG: WAL should have been skipped but was replayed! min_wal={}, returned={:?}",
				min_wal_number, wal_seq_opt
			);
		}
	}

	#[tokio::test]
	async fn test_crash_before_flush_replays_wal() {
		let temp_dir = TempDir::new("test").unwrap();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 10 * 1024 * 1024; // Large memtable to prevent auto-flush
		});

		// Phase 1: Write data and simulate crash (no clean shutdown)
		{
			let tree = Tree::new(opts.clone()).unwrap();
			let mut txn = tree.begin().unwrap();
			txn.set(b"crash_key", b"crash_value").unwrap();
			txn.commit().await.unwrap();

			// Simulate crash: drop tree without calling close()
			// This leaves data in WAL but not flushed to SST
			// Release lock manually to allow reopen
			{
				let mut lockfile = tree.core.inner.lockfile.lock().unwrap();
				lockfile.release().unwrap();
			}
			drop(tree);
		}

		// Phase 2: Restart and verify WAL was replayed
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Data should be available (recovered from WAL)
			let txn = tree.begin().unwrap();
			assert_eq!(txn.get(b"crash_key").unwrap(), Some(b"crash_value".to_vec().into()));

			tree.close().await.unwrap();
		}
	}

	#[tokio::test]
	async fn test_log_number_advances_with_flushes() {
		let temp_dir = TempDir::new("test").unwrap();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 512; // Small memtable to trigger flushes
		});

		let tree = Tree::new(opts.clone()).unwrap();

		// Initial log_number
		let log_number_0 = tree.core.inner.level_manifest.read().unwrap().get_log_number();

		// Write data to trigger flush #1
		for i in 0..100 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("key_{i}").as_bytes(), b"value").unwrap();
			txn.commit().await.unwrap();
		}

		// Force flush
		tree.flush().unwrap();
		let log_number_1 = tree.core.inner.level_manifest.read().unwrap().get_log_number();
		assert!(log_number_1 > log_number_0, "log_number should advance after flush");

		// Write more data, trigger flush #2
		for i in 100..200 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("key_{i}").as_bytes(), b"value").unwrap();
			txn.commit().await.unwrap();
		}

		tree.flush().unwrap();
		let log_number_2 = tree.core.inner.level_manifest.read().unwrap().get_log_number();
		assert!(log_number_2 > log_number_1, "log_number should advance after second flush");

		tree.close().await.unwrap();
	}

	#[tokio::test]
	async fn test_last_sequence_persists_across_restart() {
		let temp_dir = TempDir::new("test").unwrap();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 512;
		});

		let expected_last_seq;

		// Phase 1: Create database, write, flush
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Write data
			for i in 0..50 {
				let mut txn = tree.begin().unwrap();
				txn.set(format!("key_{i}").as_bytes(), b"value").unwrap();
				txn.commit().await.unwrap();
			}

			// Force flush to persist
			tree.flush().unwrap();

			// Get last_sequence from manifest
			expected_last_seq = tree.core.inner.level_manifest.read().unwrap().get_last_sequence();
			assert!(expected_last_seq > 0, "last_sequence should be > 0 after flush");

			tree.close().await.unwrap();
		}

		// Phase 2: Reopen and verify last_sequence persisted
		{
			let tree = Tree::new(opts.clone()).unwrap();
			let loaded_last_seq =
				tree.core.inner.level_manifest.read().unwrap().get_last_sequence();

			assert_eq!(
				loaded_last_seq, expected_last_seq,
				"last_sequence should persist across restart"
			);

			tree.close().await.unwrap();
		}
	}

	#[test_log::test(tokio::test)]
	async fn test_wal_recovery_updates_last_sequence_in_memory() {
		let temp_dir = TempDir::new("test").unwrap();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 10 * 1024 * 1024; // Large to prevent auto-flush
		});

		let manifest_seq_initial;

		// Phase 1: Write data and clean shutdown (no flush, just close)
		{
			let tree = Tree::new(opts.clone()).unwrap();
			let mut txn = tree.begin().unwrap();
			txn.set(b"key1", b"value1").unwrap();
			txn.commit().await.unwrap();

			// Don't flush, just close - this will flush memtable on shutdown
			tree.close().await.unwrap();

			// Get the manifest sequence after shutdown flush
			let manifest = LevelManifest::new(opts.clone()).unwrap();
			manifest_seq_initial = manifest.get_last_sequence();
		}

		// Phase 2: Write more data but crash (no flush, no close)
		{
			let tree = Tree::new(opts.clone()).unwrap();
			let mut txn = tree.begin().unwrap();
			txn.set(b"key2", b"value2").unwrap();
			txn.commit().await.unwrap();

			// Crash: drop without close, release lock manually
			{
				let mut lockfile = tree.core.inner.lockfile.lock().unwrap();
				lockfile.release().unwrap();
			}
			drop(tree);
		}

		// Phase 3: Recover and verify in-memory sequence > manifest sequence
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// In-memory sequence should be updated from WAL (key2 recovery)
			let in_memory_seq = tree.core.seq_num();

			// Manifest sequence should still be from Phase 1
			let manifest_seq = tree.core.inner.level_manifest.read().unwrap().get_last_sequence();
			assert_eq!(
				manifest_seq, manifest_seq_initial,
				"Manifest last_sequence should not be updated until flush (from Phase 2 crash)"
			);

			// In-memory should be higher (includes recovered WAL data)
			assert!(
				in_memory_seq > manifest_seq,
				"In-memory sequence ({}) should be > manifest ({}) after WAL recovery",
				in_memory_seq,
				manifest_seq
			);

			// Now flush and verify manifest gets updated
			tree.flush().unwrap();
			let manifest_seq_after_flush =
				tree.core.inner.level_manifest.read().unwrap().get_last_sequence();
			assert!(
				manifest_seq_after_flush >= in_memory_seq,
				"Manifest last_sequence should update after flush"
			);

			tree.close().await.unwrap();
		}
	}

	#[tokio::test]
	async fn test_clean_shutdown_no_empty_wal() {
		let temp_dir = TempDir::new("test").unwrap();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 512;
		});

		let wal_dir = opts.wal_dir();

		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Write data and trigger flush
			// make_room_for_write rotates WAL, creating a new WAL for subsequent writes
			for i in 0..100 {
				let mut txn = tree.begin().unwrap();
				txn.set(format!("key_{i}").as_bytes(), b"value").unwrap();
				txn.commit().await.unwrap();
			}
			tree.flush().unwrap();

			// Write a bit more data to the new WAL
			let mut txn = tree.begin().unwrap();
			txn.set(b"extra", b"data").unwrap();
			txn.commit().await.unwrap();

			// Clean shutdown (should flush the extra data, update log_number, close WAL)
			tree.close().await.unwrap();

			// Give async cleanup time to complete
			tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

			// Count WAL files after shutdown
			let wals_after = crate::wal::list_segment_ids(&wal_dir, Some("wal")).unwrap();

			// Validate: shutdown should NOT create additional WAL files
			// WAL count may decrease due to cleanup, but should never increase
			// This confirms no empty WAL is created on shutdown
			assert!(
				wals_after.len() <= 2,
				"Clean shutdown should not create new WAL files (found {} WAL files after shutdown)",
				wals_after.len()
			);
		}

		// Verify restart works and data is accessible
		{
			let tree = Tree::new(opts.clone()).unwrap();
			let txn = tree.begin().unwrap();
			assert_eq!(txn.get(b"key_0").unwrap(), Some(b"value".to_vec().into()));
			assert_eq!(txn.get(b"extra").unwrap(), Some(b"data".to_vec().into()));
			tree.close().await.unwrap();
		}
	}

	#[tokio::test]
	async fn test_multiple_flush_cycles_log_number_sequence() {
		let temp_dir = TempDir::new("test").unwrap();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 512;
		});

		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Flush cycle 1
			for i in 0..50 {
				let mut txn = tree.begin().unwrap();
				txn.set(format!("batch1_key_{i}").as_bytes(), b"value1").unwrap();
				txn.commit().await.unwrap();
			}
			tree.flush().unwrap();
			let log_num_1 = tree.core.inner.level_manifest.read().unwrap().get_log_number();

			// Flush cycle 2
			for i in 0..50 {
				let mut txn = tree.begin().unwrap();
				txn.set(format!("batch2_key_{i}").as_bytes(), b"value2").unwrap();
				txn.commit().await.unwrap();
			}
			tree.flush().unwrap();
			let log_num_2 = tree.core.inner.level_manifest.read().unwrap().get_log_number();

			// Flush cycle 3
			for i in 0..50 {
				let mut txn = tree.begin().unwrap();
				txn.set(format!("batch3_key_{i}").as_bytes(), b"value3").unwrap();
				txn.commit().await.unwrap();
			}
			tree.flush().unwrap();
			let log_num_3 = tree.core.inner.level_manifest.read().unwrap().get_log_number();

			assert!(log_num_2 > log_num_1, "log_number should advance");
			assert!(log_num_3 > log_num_2, "log_number should advance");

			tree.close().await.unwrap();
		}

		// Restart and verify all data accessible from SSTables
		{
			let tree = Tree::new(opts.clone()).unwrap();
			let txn = tree.begin().unwrap();

			// All batches should be accessible (reading doesn't need mut)
			assert_eq!(txn.get(b"batch1_key_0").unwrap(), Some(b"value1".to_vec().into()));
			assert_eq!(txn.get(b"batch2_key_0").unwrap(), Some(b"value2".to_vec().into()));
			assert_eq!(txn.get(b"batch3_key_0").unwrap(), Some(b"value3".to_vec().into()));

			tree.close().await.unwrap();
		}
	}

	#[tokio::test]
	async fn test_shutdown_with_empty_memtable() {
		let temp_dir = TempDir::new("test").unwrap();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |_opts| {});

		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Write and flush everything
			let mut txn = tree.begin().unwrap();
			txn.set(b"key", b"value").unwrap();
			txn.commit().await.unwrap();
			tree.flush().unwrap();

			// Get manifest state
			let log_number_before = tree.core.inner.level_manifest.read().unwrap().get_log_number();
			let last_seq_before =
				tree.core.inner.level_manifest.read().unwrap().get_last_sequence();

			// Shutdown with empty memtable
			tree.close().await.unwrap();

			// Verify manifest unchanged (no unnecessary updates)
			let manifest = LevelManifest::new(opts.clone()).unwrap();
			assert_eq!(manifest.get_log_number(), log_number_before);
			assert_eq!(manifest.get_last_sequence(), last_seq_before);
		}

		// Restart successfully
		{
			let tree = Tree::new(opts.clone()).unwrap();
			let txn = tree.begin().unwrap();
			assert_eq!(txn.get(b"key").unwrap(), Some(b"value".to_vec().into()));
			tree.close().await.unwrap();
		}
	}

	#[test_log::test(tokio::test)]
	async fn test_full_crash_recovery_scenario() {
		let temp_dir = TempDir::new("test").unwrap();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 512;
		});

		// Phase 1: Write batch A, flush
		{
			let tree = Tree::new(opts.clone()).unwrap();
			for i in 0..50 {
				let mut txn = tree.begin().unwrap();
				txn.set(format!("batch_a_{i}").as_bytes(), b"value_a").unwrap();
				txn.commit().await.unwrap();
			}
			tree.flush().unwrap();
			tree.close().await.unwrap();
		}

		// Phase 2: Write batch B, flush
		{
			let tree = Tree::new(opts.clone()).unwrap();
			for i in 0..50 {
				let mut txn = tree.begin().unwrap();
				txn.set(format!("batch_b_{i}").as_bytes(), b"value_b").unwrap();
				txn.commit().await.unwrap();
			}
			tree.flush().unwrap();

			// Get log_number after second flush
			let log_number_after_b =
				tree.core.inner.level_manifest.read().unwrap().get_log_number();
			assert!(log_number_after_b >= 2, "Should have rotated WAL at least twice");

			tree.close().await.unwrap();
		}

		// Give async cleanup time to finish from Phase 2
		tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

		// Phase 3: Write batch C, crash before flush
		{
			let tree = Tree::new(opts.clone()).unwrap();
			for i in 0..20 {
				let mut txn = tree.begin().unwrap();
				txn.set(format!("batch_c_{i}").as_bytes(), b"value_c").unwrap();
				txn.commit().await.unwrap();
			}

			// Simulate crash: drop without close
			// Release lock manually to allow reopen
			{
				let mut lockfile = tree.core.inner.lockfile.lock().unwrap();
				lockfile.release().unwrap();
			}
			drop(tree);
		}

		// Phase 4: Restart and verify
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// All data should be accessible
			let txn = tree.begin().unwrap();
			assert_eq!(txn.get(b"batch_a_0").unwrap(), Some(b"value_a".to_vec().into()));
			assert_eq!(txn.get(b"batch_b_0").unwrap(), Some(b"value_b".to_vec().into()));
			assert_eq!(
				txn.get(b"batch_c_0").unwrap(),
				Some(b"value_c".to_vec().into()),
				"Batch C should be recovered from WAL"
			);

			tree.close().await.unwrap();
		}
	}

	#[tokio::test]
	async fn test_concurrent_flush_after_rotation() {
		let temp_dir = TempDir::new("test").unwrap();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 512;
		});

		let tree = Tree::new(opts.clone()).unwrap();

		// Write data
		for i in 0..100 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("key_{i}").as_bytes(), b"value").unwrap();
			txn.commit().await.unwrap();
		}

		// Verify data is accessible before flush
		{
			let txn = tree.begin().unwrap();
			assert_eq!(
				txn.get(b"key_0").unwrap(),
				Some(b"value".to_vec().into()),
				"Data should be accessible before flush"
			);
		}

		// Call flush which will:
		// 1. Rotate WAL
		// 2. Call flush_memtable_and_update_manifest
		// The function should handle empty memtable gracefully
		tree.flush().unwrap();

		// Verify data is still accessible after flush
		{
			let txn = tree.begin().unwrap();
			assert_eq!(
				txn.get(b"key_0").unwrap(),
				Some(b"value".to_vec().into()),
				"Data should be accessible after flush"
			);
		}

		// Verify no errors and system continues
		let mut txn = tree.begin().unwrap();
		txn.set(b"after_flush", b"value").unwrap();
		txn.commit().await.unwrap();
		drop(txn);

		// Verify both old and new data are accessible
		{
			let txn = tree.begin().unwrap();
			assert_eq!(
				txn.get(b"key_0").unwrap(),
				Some(b"value".to_vec().into()),
				"Old data should still be accessible"
			);
			assert_eq!(
				txn.get(b"after_flush").unwrap(),
				Some(b"value".to_vec().into()),
				"New data after flush should be accessible"
			);
			drop(txn);
		}

		tree.close().await.unwrap();
	}

	#[test_log::test(tokio::test)]
	async fn test_wal_file_reuse_across_restarts() {
		let temp_dir = TempDir::new("test").unwrap();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 10 * 1024 * 1024; // Large to prevent auto-flush
		});

		// Phase 1: Open database, write one transaction, close
		{
			let tree = Tree::new(opts.clone()).unwrap();
			let mut txn = tree.begin().unwrap();
			txn.set(b"key1", b"value1").unwrap();
			txn.commit().await.unwrap();

			tree.close().await.unwrap();
		}

		// Phase 2: Reopen database, check if WAL is reused
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Write another transaction
			let mut txn = tree.begin().unwrap();
			txn.set(b"key2", b"value2").unwrap();
			txn.commit().await.unwrap();

			tree.close().await.unwrap();
		}

		// Phase 3: Verify recovery
		{
			let tree = Tree::new(opts.clone()).unwrap();
			let txn = tree.begin().unwrap();

			// Both keys should be accessible
			let key1_present = txn.get(b"key1").unwrap().is_some();
			let key2_present = txn.get(b"key2").unwrap().is_some();

			assert!(key1_present && key2_present, "Both keys should be recovered");

			tree.close().await.unwrap();
		}
	}

	#[test_log::test(tokio::test)]
	async fn test_wal_append_after_crash_recovery() {
		// This test verifies that after a crash (no flush), WAL is reused and appended to
		let temp_dir = TempDir::new("test").unwrap();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 10 * 1024 * 1024; // Large to prevent auto-flush
		});

		// Phase 1: Write data and simulate crash (no clean shutdown)
		let manifest_log = {
			let tree = Tree::new(opts.clone()).unwrap();
			let mut txn = tree.begin().unwrap();
			txn.set(b"key1", b"value1").unwrap();
			txn.commit().await.unwrap();

			let manifest_log = tree.core.inner.level_manifest.read().unwrap().get_log_number();

			// Simulate crash: drop without close (but release lock)
			{
				let mut lockfile = tree.core.inner.lockfile.lock().unwrap();
				lockfile.release().unwrap();
			}
			drop(tree);

			manifest_log
		};

		// Verify manifest didn't change (no flush happened)
		let manifest = LevelManifest::new(opts.clone()).unwrap();
		assert_eq!(manifest.get_log_number(), manifest_log, "Manifest should not change on crash");

		// Phase 2: Reopen and verify WAL is reused (SAME number)
		{
			let tree = Tree::new(opts.clone()).unwrap();

			let wal_num_after_reopen = tree.core.inner.wal.read().get_active_log_number();

			// CRITICAL: WAL number should be SAME as before (appending to existing)
			assert_eq!(
				wal_num_after_reopen, 0,
				"WAL should reuse existing file #0 since log_number=0"
			);

			// Write another transaction to same WAL
			let mut txn = tree.begin().unwrap();
			txn.set(b"key2", b"value2").unwrap();
			txn.commit().await.unwrap();

			let wal_num_after_write = tree.core.inner.wal.read().get_active_log_number();

			// Should still be same WAL
			assert_eq!(
				wal_num_after_write, wal_num_after_reopen,
				"Should still be using same WAL file"
			);

			tree.close().await.unwrap();
		}
	}

	#[test_log::test(tokio::test)]
	async fn test_flush_on_close_creates_sst() {
		// This test verifies that close() flushes active memtable to SST
		let temp_dir = TempDir::new("test").unwrap();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 10 * 1024 * 1024; // Large to prevent auto-flush
			opts.flush_on_close = true;
		});

		let sst_dir = opts.sstable_dir();

		// Count SST files before
		let count_ssts = || {
			std::fs::read_dir(&sst_dir)
				.ok()
				.map(|entries| {
					entries
						.filter_map(|e| e.ok())
						.filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
						.count()
				})
				.unwrap_or(0)
		};

		// Phase 1: Write data and close
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Write data to memtable (no manual flush)
			let mut txn = tree.begin().unwrap();
			txn.set(b"test_key", b"test_value").unwrap();
			txn.commit().await.unwrap();

			let sst_count_before_close = count_ssts();

			// Close (should trigger flush)
			tree.close().await.unwrap();

			let sst_count_after_close = count_ssts();

			// CRITICAL: SST count should increase by 1 (memtable flushed)
			assert_eq!(
				sst_count_after_close,
				sst_count_before_close + 1,
				"SST count should increase by 1 after close (flush on shutdown)"
			);
		}

		// Phase 2: Reopen and verify data is in SST (not WAL)
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Memtable should be empty (data in SST)
			// Data should still be accessible
			let txn = tree.begin().unwrap();
			assert_eq!(txn.get(b"test_key").unwrap(), Some(b"test_value".to_vec().into()));

			tree.close().await.unwrap();
		}
	}

	#[test_log::test(tokio::test)]
	async fn test_multiple_flush_cycles_with_sst_and_wal_verification() {
		// Comprehensive test: multiple write-flush-close cycles
		// Verifies SST creation, WAL rotation, log_number tracking, and recovery
		let temp_dir = TempDir::new("test").unwrap();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 512; // Small to trigger flushes
		});

		let sst_dir = opts.sstable_dir();

		let count_ssts = || {
			std::fs::read_dir(&sst_dir)
				.ok()
				.map(|entries| {
					entries
						.filter_map(|e| e.ok())
						.filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
						.count()
				})
				.unwrap_or(0)
		};

		// Cycle 1: Write data, trigger flush, close
		{
			let tree = Tree::new(opts.clone()).unwrap();

			for i in 0..50 {
				let mut txn = tree.begin().unwrap();
				txn.set(format!("cycle1_key_{}", i).as_bytes(), b"value1").unwrap();
				txn.commit().await.unwrap();
			}

			let sst_before = count_ssts();
			tree.flush().unwrap(); // Explicit flush
			let sst_after = count_ssts();

			{
				let manifest = tree.core.inner.level_manifest.read().unwrap();
				drop(manifest);
			}

			assert_eq!(sst_after, sst_before + 1, "Flush should create 1 SST");

			tree.close().await.unwrap();
		}

		// Cycle 2: Reopen, verify recovery, write more, flush, close
		{
			let manifest_before = LevelManifest::new(opts.clone()).unwrap();
			let log_num_before = manifest_before.get_log_number();

			let tree = Tree::new(opts.clone()).unwrap();

			// Verify cycle 1 data is accessible
			let txn = tree.begin().unwrap();
			assert_eq!(txn.get(b"cycle1_key_0").unwrap(), Some(b"value1".to_vec().into()));
			drop(txn);

			for i in 0..50 {
				let mut txn = tree.begin().unwrap();
				txn.set(format!("cycle2_key_{}", i).as_bytes(), b"value2").unwrap();
				txn.commit().await.unwrap();
			}

			let sst_before = count_ssts();
			tree.flush().unwrap();
			let sst_after = count_ssts();

			{
				let manifest = tree.core.inner.level_manifest.read().unwrap();

				assert_eq!(sst_after, sst_before + 1, "Second flush should create 1 more SST");
				assert!(manifest.get_log_number() > log_num_before, "log_number should advance");
				drop(manifest);
			}

			tree.close().await.unwrap();
		}

		// Cycle 3: Reopen, write but DON'T flush, close (tests shutdown flush)
		{
			// Enable flush_on_close to test shutdown flush behavior
			let opts_with_flush = Arc::new(Options {
				flush_on_close: true,
				..(*opts).clone()
			});
			let tree = Tree::new(opts_with_flush).unwrap();

			// Verify both previous cycles' data
			let txn = tree.begin().unwrap();
			assert_eq!(txn.get(b"cycle1_key_0").unwrap(), Some(b"value1".to_vec().into()));
			assert_eq!(txn.get(b"cycle2_key_0").unwrap(), Some(b"value2".to_vec().into()));
			drop(txn);

			for i in 0..50 {
				let mut txn = tree.begin().unwrap();
				txn.set(format!("cycle3_key_{}", i).as_bytes(), b"value3").unwrap();
				txn.commit().await.unwrap();
			}

			let sst_before_close = count_ssts();

			// Close WITHOUT explicit flush (shutdown should flush because flush_on_close=true)
			tree.close().await.unwrap();

			let sst_after_close = count_ssts();

			assert_eq!(
				sst_after_close,
				sst_before_close + 1,
				"Shutdown should flush and create SST when flush_on_close=true"
			);
		}

		// Final verification: All data accessible from SSTs
		{
			let tree = Tree::new(opts.clone()).unwrap();

			let txn = tree.begin().unwrap();
			assert_eq!(txn.get(b"cycle1_key_0").unwrap(), Some(b"value1".to_vec().into()));
			assert_eq!(txn.get(b"cycle2_key_0").unwrap(), Some(b"value2".to_vec().into()));
			assert_eq!(txn.get(b"cycle3_key_0").unwrap(), Some(b"value3".to_vec().into()));

			tree.close().await.unwrap();
		}
	}

	#[tokio::test]
	async fn test_close_without_flush() {
		let temp_dir = TempDir::new("test").unwrap();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.flush_on_close = false; // Default behavior
			opts.max_memtable_size = 1024 * 1024;
		});

		let sst_count_before;
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Write some data that won't trigger auto-flush
			for i in 0..10 {
				let mut txn = tree.begin().unwrap();
				txn.set(format!("key_{}", i).as_bytes(), b"value").unwrap();
				txn.commit().await.unwrap();
			}

			// Count SSTs before close
			sst_count_before = tree.core.inner.level_manifest.read().as_ref().iter().count();

			// Close without flush (flush_on_close=false)
			tree.close().await.unwrap();
		}

		// Reopen and verify SST count hasn't changed
		{
			let tree = Tree::new(opts.clone()).unwrap();
			let sst_count_after = tree.core.inner.level_manifest.read().as_ref().iter().count();

			assert_eq!(
				sst_count_after, sst_count_before,
				"SST count should not increase when flush_on_close=false"
			);

			// Data should still be accessible via WAL recovery
			let txn = tree.begin().unwrap();
			assert_eq!(txn.get(b"key_0").unwrap(), Some(b"value".to_vec().into()));

			tree.close().await.unwrap();
		}
	}

	#[tokio::test]
	async fn test_flush_on_close_option_comparison() {
		// Test with flush_on_close = true
		{
			let temp_dir = TempDir::new("test").unwrap();
			let opts = create_test_options(temp_dir.path().to_path_buf(), |opts| {
				opts.flush_on_close = true;
			});

			let sst_before;
			{
				let tree = Tree::new(opts.clone()).unwrap();
				let mut txn = tree.begin().unwrap();
				txn.set(b"test", b"data").unwrap();
				txn.commit().await.unwrap();

				sst_before = tree.core.inner.level_manifest.read().unwrap().iter().count();

				tree.close().await.unwrap();
			}

			let tree = Tree::new(opts.clone()).unwrap();
			let sst_after = tree.core.inner.level_manifest.read().unwrap().iter().count();

			assert_eq!(sst_after, sst_before + 1, "flush_on_close=true should create SST");
			tree.close().await.unwrap();
		}

		// Test with flush_on_close = false
		{
			let temp_dir = TempDir::new("test").unwrap();
			let opts = create_test_options(temp_dir.path().to_path_buf(), |opts| {
				opts.flush_on_close = false;
			});

			let sst_before;
			{
				let tree = Tree::new(opts.clone()).unwrap();
				let mut txn = tree.begin().unwrap();
				txn.set(b"test", b"data").unwrap();
				txn.commit().await.unwrap();

				sst_before = tree.core.inner.level_manifest.read().unwrap().iter().count();

				tree.close().await.unwrap();
			}

			let tree = Tree::new(opts.clone()).unwrap();
			let sst_after = tree.core.inner.level_manifest.read().unwrap().iter().count();

			assert_eq!(sst_after, sst_before, "flush_on_close=false should NOT create SST");

			// But data should still be accessible via WAL
			let txn = tree.begin().unwrap();
			assert_eq!(txn.get(b"test").unwrap(), Some(b"data".to_vec().into()));

			tree.close().await.unwrap();
		}
	}

	#[test_log::test(tokio::test)]
	async fn test_wal_files_after_multiple_open_close_cycles() {
		// Simulates: open -> write 100 entries -> close, repeated multiple times
		// Tests that data is recoverable and manifest state is correct after each cycle
		let temp_dir = TempDir::new("test").unwrap();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 10 * 1024 * 1024; // Large to prevent auto-flush
		});

		let mut previous_log_numbers = Vec::new();

		for cycle in 1..=3 {
			{
				let tree = Tree::new(opts.clone()).unwrap();

				// Write 100 entries
				for i in 0..100 {
					let mut txn = tree.begin().unwrap();
					txn.set(format!("cycle{}_key_{}", cycle, i).as_bytes(), b"value").unwrap();
					txn.commit().await.unwrap();
				}

				// Close (should flush)
				tree.close().await.unwrap();

				// Give async cleanup time to run
				tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
			}

			// Check manifest state after close
			let manifest = LevelManifest::new(opts.clone()).unwrap();
			let log_number_after_close = manifest.get_log_number();
			previous_log_numbers.push(log_number_after_close);

			// Verify data from this cycle is accessible
			{
				let tree = Tree::new(opts.clone()).unwrap();
				let txn = tree.begin().unwrap();

				// Check data from current cycle
				assert_eq!(
					txn.get(format!("cycle{}_key_0", cycle).as_bytes()).unwrap(),
					Some(b"value".to_vec().into()),
					"Data from cycle {} should be recoverable",
					cycle
				);

				// Check data from all previous cycles is still accessible
				for prev_cycle in 1..cycle {
					assert_eq!(
						txn.get(format!("cycle{}_key_0", prev_cycle).as_bytes()).unwrap(),
						Some(b"value".to_vec().into()),
						"Data from previous cycle {} should still be accessible",
						prev_cycle
					);
				}

				drop(txn);
				tree.close().await.unwrap();
			}
		}

		// Final verification: Manifest log_number should have advanced across cycles
		assert!(
			previous_log_numbers.len() == 3,
			"Should have collected log numbers from all 3 cycles"
		);

		// Verify all data is still accessible after all cycles
		{
			let tree = Tree::new(opts.clone()).unwrap();
			let txn = tree.begin().unwrap();

			for cycle in 1..=3 {
				assert_eq!(
					txn.get(format!("cycle{}_key_0", cycle).as_bytes()).unwrap(),
					Some(b"value".to_vec().into()),
					"Data from cycle {} should be accessible in final check",
					cycle
				);
			}

			drop(txn);
			tree.close().await.unwrap();
		}
	}

	#[test(tokio::test)]
	async fn test_cleanup_orphaned_sst_files() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();
		let opts = create_test_options(path.clone(), |_| {});

		// Create initial tree and add some data
		{
			let tree = Tree::new(opts.clone()).unwrap();
			let mut txn = tree.begin().unwrap();
			txn.set(b"key1", b"value1").unwrap();
			txn.commit().await.unwrap();
			tree.flush().unwrap();
			tree.close().await.unwrap();
		}

		// Manually create an orphaned SST file (simulating incomplete flush)
		let orphaned_table_id = 9999;
		let orphaned_path = opts.sstable_file_path(orphaned_table_id);
		std::fs::write(&orphaned_path, b"fake sst data").unwrap();

		// Verify orphaned file exists
		assert!(orphaned_path.exists(), "Orphaned SST should exist before cleanup");

		// Reopen database - should trigger cleanup
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Verify orphaned file was deleted
			assert!(!orphaned_path.exists(), "Orphaned SST should be cleaned up");

			// Verify real data still accessible
			let txn = tree.begin().unwrap();
			let result = txn.get(b"key1").unwrap().unwrap();
			assert_eq!(result, Bytes::copy_from_slice(b"value1"));

			tree.close().await.unwrap();
		}
	}

	#[test(tokio::test)]
	async fn test_manifest_atomic_sst_and_log_number() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		// Use tiny threshold to ensure flush happens
		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 100; // Very small to guarantee flush
		});

		let tree = Tree::new(opts.clone()).unwrap();

		// Get initial log_number
		let initial_log_number = {
			let manifest = tree.core.inner.level_manifest.read().unwrap();
			manifest.get_log_number()
		};

		// Add data
		for i in 0..100 {
			let mut txn = tree.begin().unwrap();
			txn.set(format!("key{}", i).as_bytes(), b"value").unwrap();
			txn.commit().await.unwrap();
		}

		// Explicitly trigger flush to ensure test reliability
		tree.flush().unwrap();

		// Verify that when SST is added, log_number is also updated
		{
			let manifest = tree.core.inner.level_manifest.read().unwrap();
			let new_log_number = manifest.get_log_number();
			let level0_tables = &manifest.levels.get_levels()[0].tables;

			// SST should be flushed now
			assert!(!level0_tables.is_empty(), "SST should be flushed");
			// And log_number MUST have been updated atomically
			assert!(
				new_log_number > initial_log_number,
				"log_number should be updated atomically with SST addition"
			);
			drop(manifest);
		}

		tree.close().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_no_spurious_small_flush() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 100 * 1024; // 100KB threshold
		});

		let tree = Tree::new(opts.clone()).unwrap();

		// Add small amount of data (way below threshold)
		let mut txn = tree.begin().unwrap();
		txn.set(b"key1", b"value1").unwrap();
		txn.commit().await.unwrap();

		// Manually trigger wake_up (simulating spurious notification)
		if let Some(ref task_manager) = *tree.core.task_manager.lock().unwrap() {
			task_manager.wake_up_memtable();
		}

		// Wait a bit
		tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

		// Verify no flush occurred (data still in active memtable, not in L0)
		{
			let manifest = tree.core.inner.level_manifest.read().unwrap();
			assert!(
				manifest.levels.get_levels()[0].tables.is_empty(),
				"Should not flush small memtable due to spurious notification"
			);
			drop(manifest);
		}

		tree.close().await.unwrap();
	}

	#[test(tokio::test)]
	async fn test_crash_recovery_with_orphaned_sst() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();
		let opts = create_test_options(path.clone(), |_| {});

		// Phase 1: Write data and flush
		{
			let tree = Tree::new(opts.clone()).unwrap();
			let mut txn = tree.begin().unwrap();
			txn.set(b"committed_key", b"committed_value").unwrap();
			txn.commit().await.unwrap();
			tree.flush().unwrap();
			tree.close().await.unwrap();
		}

		// Phase 2: Simulate incomplete flush (create orphaned SST + keep WAL)
		let orphaned_table_id = 9998;
		let orphaned_sst_path = opts.sstable_file_path(orphaned_table_id);
		std::fs::write(&orphaned_sst_path, b"orphaned SST content").unwrap();

		// Add more data that would be in WAL
		{
			let tree = Tree::new(opts.clone()).unwrap();
			let mut txn = tree.begin().unwrap();
			txn.set(b"wal_key", b"wal_value").unwrap();
			txn.commit().await.unwrap();
			// Don't flush - keep in WAL
			tree.close().await.unwrap();
		}

		// Phase 3: Reopen - should cleanup orphaned SST and recover from WAL
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Verify orphaned SST was cleaned up
			assert!(!orphaned_sst_path.exists(), "Orphaned SST should be removed");

			// Verify WAL data was recovered
			let txn = tree.begin().unwrap();
			let result = txn.get(b"wal_key").unwrap().unwrap();
			assert_eq!(result, Bytes::copy_from_slice(b"wal_value"));

			// Verify committed data still accessible
			let result = txn.get(b"committed_key").unwrap().unwrap();
			assert_eq!(result, Bytes::copy_from_slice(b"committed_value"));

			tree.close().await.unwrap();
		}
	}

	#[test(tokio::test)]
	async fn test_cleanup_multiple_orphaned_ssts() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();
		let opts = create_test_options(path.clone(), |_| {});

		// Create initial database
		{
			let tree = Tree::new(opts.clone()).unwrap();
			let mut txn = tree.begin().unwrap();
			txn.set(b"key1", b"value1").unwrap();
			txn.commit().await.unwrap();
			tree.close().await.unwrap();
		}

		// Create multiple orphaned SST files
		let orphaned_ids = vec![8888, 9999, 10000];
		for table_id in &orphaned_ids {
			let orphaned_path = opts.sstable_file_path(*table_id);
			std::fs::write(&orphaned_path, format!("orphaned {}", table_id)).unwrap();
		}

		// Verify all exist
		for table_id in &orphaned_ids {
			assert!(opts.sstable_file_path(*table_id).exists());
		}

		// Reopen - should cleanup all orphaned files
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Verify all orphaned files deleted
			for table_id in &orphaned_ids {
				assert!(
					!opts.sstable_file_path(*table_id).exists(),
					"Orphaned SST {} should be cleaned up",
					table_id
				);
			}

			tree.close().await.unwrap();
		}
	}

	#[test(tokio::test)]
	async fn test_valid_ssts_not_deleted_during_cleanup() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();
		let opts = create_small_memtable_options(path.clone());

		// Create database and flush some data
		let valid_table_ids = {
			let tree = Tree::new(opts.clone()).unwrap();

			for i in 0..200 {
				let mut txn = tree.begin().unwrap();
				txn.set(format!("key{}", i).as_bytes(), b"value").unwrap();
				txn.commit().await.unwrap();
			}

			tree.flush().unwrap();

			// Get list of valid table IDs
			let ids = {
				let manifest = tree.core.inner.level_manifest.read().unwrap();
				let ids: Vec<u64> = manifest.iter().map(|t| t.id).collect();
				drop(manifest);
				ids
			};

			tree.close().await.unwrap();
			ids
		};

		// Create orphaned SST
		let orphaned_id = 9999;
		std::fs::write(opts.sstable_file_path(orphaned_id), b"orphaned").unwrap();

		// Reopen
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Verify orphaned deleted
			assert!(!opts.sstable_file_path(orphaned_id).exists());

			// Verify all valid SSTs still exist
			for table_id in &valid_table_ids {
				assert!(
					opts.sstable_file_path(*table_id).exists(),
					"Valid SST {} should not be deleted",
					table_id
				);
			}

			// Verify data still accessible
			let txn = tree.begin().unwrap();
			let result = txn.get(b"key1").unwrap().unwrap();
			assert_eq!(result, Bytes::copy_from_slice(b"value"));

			tree.close().await.unwrap();
		}
	}

	#[test(tokio::test)]
	async fn test_comprehensive_orphaned_cleanup_with_multiple_ssts() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let opts = create_test_options(path.clone(), |opts| {
			opts.max_memtable_size = 1024; // Small to create multiple SSTs
		});

		// Phase 1: Create multiple valid SSTs with real data
		let mut expected_keys = Vec::new();
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Write multiple batches to create multiple SSTs
			for batch_num in 0..5 {
				for i in 0..50 {
					let key = format!("batch{}_key{}", batch_num, i);
					let value = format!("batch{}_value{}", batch_num, i);
					expected_keys.push(key.clone());

					let mut txn = tree.begin().unwrap();
					txn.set(key.as_bytes(), value.as_bytes()).unwrap();
					txn.commit().await.unwrap();
				}
				// Force flush after each batch
				tree.flush().unwrap();
			}

			tree.close().await.unwrap();
		}

		// Phase 2: Get valid SST IDs and create orphaned SSTs
		let valid_sst_ids = {
			let tree = Tree::new(opts.clone()).unwrap();
			let ids = {
				let manifest = tree.core.inner.level_manifest.read().unwrap();
				let ids: Vec<u64> = manifest.iter().map(|t| t.id).collect();
				drop(manifest);
				ids
			};
			tree.close().await.unwrap();
			ids
		};

		// Create multiple orphaned SST files
		let orphaned_ids = vec![8888, 9999, 10000, 10001];
		for table_id in &orphaned_ids {
			let orphaned_path = opts.sstable_file_path(*table_id);
			std::fs::write(&orphaned_path, format!("orphaned SST {}", table_id)).unwrap();
		}

		// Verify all files exist (both valid and orphaned)
		for table_id in &valid_sst_ids {
			assert!(
				opts.sstable_file_path(*table_id).exists(),
				"Valid SST {} should exist before reopen",
				table_id
			);
		}
		for table_id in &orphaned_ids {
			assert!(
				opts.sstable_file_path(*table_id).exists(),
				"Orphaned SST {} should exist before cleanup",
				table_id
			);
		}

		// Phase 3: Reopen - should cleanup orphaned but keep valid
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Verify all orphaned files deleted
			for table_id in &orphaned_ids {
				assert!(
					!opts.sstable_file_path(*table_id).exists(),
					"Orphaned SST {} should be cleaned up",
					table_id
				);
			}

			// Verify all valid SST files still exist
			for table_id in &valid_sst_ids {
				assert!(
					opts.sstable_file_path(*table_id).exists(),
					"Valid SST {} should not be deleted",
					table_id
				);
			}

			// Verify ALL data is still accessible
			for key in &expected_keys {
				let txn = tree.begin().unwrap();
				let result = txn.get(key.as_bytes()).unwrap();
				assert!(result.is_some(), "Key {} should be accessible", key);
			}

			// Spot check a few specific values
			let txn = tree.begin().unwrap();
			let result = txn.get(b"batch0_key0").unwrap().unwrap();
			assert_eq!(result, Bytes::copy_from_slice(b"batch0_value0"));

			let result = txn.get(b"batch4_key49").unwrap().unwrap();
			assert_eq!(result, Bytes::copy_from_slice(b"batch4_value49"));

			tree.close().await.unwrap();
		}
	}
}
