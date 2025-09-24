#[cfg(test)]
use std::collections::HashMap;
use std::{
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
	memtable::{ImmutableMemtables, MemTable},
	oracle::Oracle,
	snapshot::Counter as SnapshotCounter,
	sstable::{table::Table, InternalKey, ReverseTimestampKey},
	task::TaskManager,
	transaction::{Mode, Transaction},
	vlog::{VLog, VLogGCManager, ValueLocation},
	wal::{
		self,
		cleanup::cleanup_old_segments,
		recovery::{repair_corrupted_wal_segment, replay_wal},
		writer::Wal,
	},
	Comparator, CompressionType, Error, FilterPolicy, Options, VLogChecksumLevel, Value,
};

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
	///
	/// In memory-only mode, this is None since we don't persist to disk.
	pub level_manifest: Option<Arc<RwLock<LevelManifest>>>,

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
	/// In memory-only mode, this is None since we don't persist to disk.
	pub(crate) wal: Option<parking_lot::RwLock<Wal>>,

	/// Versioned B+ tree index for timestamp-based queries
	/// Maps ReverseTimestampKey -> Value for efficient time-range queries
	pub(crate) versioned_index: Option<Arc<RwLock<DiskBPlusTree>>>,
}

impl CoreInner {
	/// Creates a new LSM tree core instance
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		// Initialize active and immutable memtables
		let active_memtable = Arc::new(RwLock::new(Arc::new(MemTable::new())));
		let immutable_memtables = Arc::new(RwLock::new(ImmutableMemtables::default()));

		// TODO: Add a way to recover from the WAL or level manifest
		// Initialize the transaction oracle for MVCC support
		// The oracle provides monotonic timestamps for transaction ordering
		let oracle = Oracle::new();

		// Initialize WAL and level manifest only if not in memory-only mode
		let (wal, level_manifest) = if opts.in_memory_only {
			// In memory-only mode, we don't need WAL or level manifest since we don't persist to disk
			(None, None)
		} else {
			let wal_path = opts.wal_dir();
			let wal = Some(Wal::open(&wal_path, wal::Options::default())?);
			let manifest = LevelManifest::new(opts.clone())?;
			let level_manifest = Some(Arc::new(RwLock::new(manifest)));
			(wal, level_manifest)
		};

		let vlog = if opts.enable_vlog && !opts.in_memory_only {
			Some(Arc::new(VLog::new(opts.clone())?))
		} else {
			None
		};

		// Initialize versioned index if versioned queries are enabled
		let versioned_index = if opts.enable_versioning && !opts.in_memory_only {
			// Create the versioned index directory if it doesn't exist
			let versioned_index_dir = opts.versioned_index_dir();
			let versioned_index_path = versioned_index_dir.join("index.bpt");
			let comparator = Arc::new(crate::BytewiseComparator {});
			let tree = DiskBPlusTree::disk(&versioned_index_path, comparator)?;
			Some(Arc::new(RwLock::new(tree)))
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
			wal: wal.map(parking_lot::RwLock::new),
			versioned_index,
		})
	}

	/// Makes room for new writes by flushing the active memtable if needed.
	///
	/// This implements the core LSM write path:
	/// 1. When the active memtable is full, it becomes immutable
	/// 2. A new empty active memtable is created for new writes
	/// 3. The immutable memtable is flushed to disk as an SSTable
	/// 4. The new SSTable is added to Level 0
	fn make_room_for_write(&self) -> Result<()> {
		// Atomically flush memtable and rotate WAL under a single lock
		// This prevents race conditions where writes could go to the wrong WAL segment
		let Some((table_id, flushed_memtable)) = self.flush_active_memtable_and_rotate_wal()?
		else {
			return Ok(());
		};

		// Flush the immutable memtable to disk as an SSTable
		// This converts the in-memory sorted data structure to an on-disk format
		let table = flushed_memtable.flush(table_id, self.opts.clone())?;

		// Add the new SSTable to Level 0
		// L0 is special: it contains recently flushed SSTables that may have overlapping keys
		self.add_table_to_l0(table)?;

		// Clean up old WAL segments since the memtable has been successfully flushed
		// Done asynchronously to avoid blocking the flush operation
		let wal_guard = self.wal.as_ref().unwrap().read();
		let wal_dir = wal_guard.get_dir_path().to_path_buf();
		tokio::spawn(async move {
			if let Err(e) = cleanup_old_segments(&wal_dir) {
				eprintln!("Failed to clean up old WAL segments: {e}");
			}
		});

		Ok(())
	}

	/// Atomically flushes the active memtable and rotates WAL.
	///
	/// This prevents race conditions by holding the active_memtable write lock
	/// for both the memtable swap and WAL rotation operations.
	fn flush_active_memtable_and_rotate_wal(&self) -> Result<Option<(u64, Arc<MemTable>)>> {
		let mut active_memtable = self.active_memtable.write().unwrap();

		// Don't flush an empty memtable
		if active_memtable.is_empty() {
			return Ok(None);
		}

		let mut immutable_memtables = self.immutable_memtables.write().unwrap();

		// Atomically swap the active memtable with a new empty one
		// This allows writes to continue immediately
		let flushed_memtable = std::mem::take(&mut *active_memtable);

		// Track the immutable memtable until it's successfully flushed
		let memtable_id = self.level_manifest.as_ref().unwrap().read().unwrap().next_table_id();
		immutable_memtables.add(memtable_id, flushed_memtable.clone());

		// Now rotate the WAL while still holding the active_memtable lock
		// This ensures that:
		// - The flushed memtable corresponds to the previous WAL segment(s)
		// - New writes (to the new active memtable) go to a new WAL segment
		// - No race condition between memtable swap and WAL rotation
		let mut wal_guard = self.wal.as_ref().unwrap().write();
		wal_guard.rotate()?;

		Ok(Some((memtable_id, flushed_memtable)))
	}

	/// Adds a newly flushed SSTable to Level 0.
	///
	/// Level 0 is unique in the LSM tree hierarchy:
	/// - It contains SSTables flushed directly from memtables
	/// - SSTables may have overlapping key ranges
	/// - Queries must check all L0 SSTables
	/// - Too many L0 files triggers compaction to maintain read performance
	fn add_table_to_l0(&self, table: Arc<Table>) -> Result<()> {
		let mut original_manifest = self.level_manifest.as_ref().unwrap().write().unwrap();
		let mut memtable_lock = self.immutable_memtables.write().unwrap();
		let table_id = table.id;

		// Create a changeset to add the table
		let mut changeset = ManifestChangeSet::default();
		changeset.new_tables.push((0, table));

		// Apply the changeset
		original_manifest.apply_changeset(&changeset)?;

		// Persist the updated manifest to disk for crash recovery
		write_manifest_to_disk(&original_manifest)?;

		// Remove the successfully flushed memtable from tracking
		memtable_lock.remove(table_id);

		Ok(())
	}

	/// Resolves a value, checking if it's a VLog pointer and retrieving from VLog if needed
	pub(crate) fn resolve_value(&self, value: &[u8]) -> Result<Value> {
		let location = ValueLocation::decode(value)?;
		location.resolve_value(self.vlog.as_ref())
	}

	// /// TODO: Still needs work.
	// /// Cleans expired versions from the versioned index based on retention policy
	// pub(crate) fn clean_expired_versions(&self) -> Result<()> {
	// 	if !self.opts.enable_versioning || self.opts.versioned_history_retention_ns == 0 {
	// 		return Ok(()); // No retention limit or versioned queries disabled
	// 	}

	// 	if let Some(ref versioned_index) = self.versioned_index {
	// 		let current_time = std::time::SystemTime::now()
	// 			.duration_since(std::time::UNIX_EPOCH)
	// 			.unwrap_or_default()
	// 			.as_nanos() as u64;

	// 		let cutoff_time = current_time.saturating_sub(self.opts.versioned_history_retention_ns);

	// 		// Create a range query to find all entries older than cutoff_time
	// 		let start_key = ReverseTimestampKey::new(
	// 			vec![], // Empty user key for range start
	// 			0,
	// 			InternalKeyKind::Set,
	// 			0, // Start from beginning of time
	// 		)
	// 		.encode();

	// 		let end_key = ReverseTimestampKey::new(
	// 			vec![0xFF; 256], // Max user key for range end
	// 			u64::MAX,
	// 			InternalKeyKind::Set,
	// 			cutoff_time, // End at cutoff time
	// 		)
	// 		.encode();

	// 		let mut index_guard = versioned_index.write().unwrap();

	// 		// Get all entries in the time range
	// 		let range_iter = index_guard.range(&start_key, &end_key)?;
	// 		let mut keys_to_delete = Vec::new();

	// 		for entry in range_iter {
	// 			match entry {
	// 				Ok((key, _value)) => {
	// 					// Decode the key to check timestamp
	// 					let reverse_key = ReverseTimestampKey::decode(&key);
	// 					if reverse_key.timestamp < cutoff_time {
	// 						keys_to_delete.push(key);
	// 					}
	// 				}
	// 				Err(e) => {
	// 					eprintln!("Error iterating versioned index: {}", e);
	// 					break;
	// 				}
	// 			}
	// 		}

	// 		// Delete expired entries
	// 		for key in keys_to_delete {
	// 			if let Err(e) = index_guard.delete(&key) {
	// 				eprintln!("Failed to delete expired version: {}", e);
	// 			}
	// 		}
	// 	}

	// 	Ok(())
	// }
}

impl CompactionOperations for CoreInner {
	/// Triggers a memtable flush to create space for new writes
	fn compact_memtable(&self) -> Result<()> {
		self.make_room_for_write()
	}

	/// Performs compaction to merge SSTables and maintain read performance.
	///
	/// Compaction is crucial for LSM tree performance:
	/// - Merges overlapping SSTables to reduce read amplification
	/// - Removes deleted entries (tombstones) to reclaim space
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
	/// In memory-only mode, this is None since we don't need background tasks
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

	/// Creates a new commit environment for in-memory mode (without task manager)
	pub(crate) fn new_in_memory(core: Arc<CoreInner>) -> Result<Self> {
		Ok(Self {
			core,
			task_manager: None,
		})
	}
}

impl CommitEnv for LsmCommitEnv {
	// Write batch to WAL and process VLog entries (synchronous operation)
	// Returns a new batch with VLog pointers, and pre-encoded ValueLocations
	fn write(&self, batch: &Batch, seq_num: u64, sync: bool) -> Result<Batch> {
		// Create a new batch for processed entries with pre-encoded values
		let mut processed_batch = Batch::new(seq_num);
		let mut reverse_timestamp_entries = Vec::new();
		// Process VLog entries and create the processed batch in a single loop
		if let Some(ref vlog) = self.core.vlog {
			// Use the unified sequence number management
			for (_, entry, current_seq_num, timestamp) in batch.entries_with_seq_nums()? {
				// Create full InternalKey for VLog
				let ikey =
					InternalKey::new(entry.key.clone(), current_seq_num, entry.kind, timestamp);
				let key_bytes = ikey.encode();

				// Determine value pointer and pre-encode ValueLocation
				let (valueptr, encoded_value) = if let Some(value) = &entry.value {
					// Check if value should go to VLog based on threshold
					if value.len() > self.core.opts.vlog_value_threshold {
						let pointer = vlog.append(&key_bytes, value)?;

						// Pre-encode ValueLocation with VLog pointer
						let value_location = ValueLocation::with_pointer(pointer.clone());
						let encoded = value_location.encode();

						if self.core.opts.enable_versioning {
							// Create ReverseTimestampKey for efficient time-range queries
							let reverse_key = ReverseTimestampKey::new(
								entry.key.clone(),
								current_seq_num,
								entry.kind,
								timestamp,
							);
							let encoded_key = reverse_key.encode();
							reverse_timestamp_entries.push((encoded_key, encoded.clone()));
						}

						(Some(pointer), Some(encoded))
					} else {
						// Small value, keep inline - pre-encode ValueLocation with inline value
						let value_location = ValueLocation::with_inline_value(Arc::from(
							value.clone().into_boxed_slice(),
						));
						let encoded = value_location.encode();
						(None, Some(encoded))
					}
				} else {
					// No value (delete operation) - pass None
					// But still add to versioned index as tombstone
					if self.core.opts.enable_versioning {
						// Create ReverseTimestampKey for delete operation
						let reverse_key = ReverseTimestampKey::new(
							entry.key.clone(),
							current_seq_num,
							entry.kind,
							timestamp,
						);
						let encoded_key = reverse_key.encode();
						// For deletes, we don't need a value, just the key
						reverse_timestamp_entries.push((encoded_key, Vec::new()));
					}
					(None, None)
				};

				// Add the entry to the processed batch with pre-encoded value
				processed_batch.add_record_with_valueptr(
					entry.kind,
					&entry.key,
					encoded_value.as_deref(),
					valueptr,
					timestamp,
				)?;
			}

			// Flush VLog to ensure data is written to disk
			if sync {
				vlog.sync()?;
			} else {
				vlog.flush()?;
			}
		} else {
			// No VLog, all values stay inline - pre-encode all ValueLocations
			for entry in batch.entries().iter() {
				let encoded_value = if let Some(value) = &entry.value {
					// Pre-encode ValueLocation with inline value
					let value_location = ValueLocation::with_inline_value(Arc::from(
						value.clone().into_boxed_slice(),
					));
					Some(value_location.encode())
				} else {
					// No value (delete operation) - pass None
					None
				};

				processed_batch.add_record_with_valueptr(
					entry.kind,
					&entry.key,
					encoded_value.as_deref(),
					None,
					entry.timestamp,
				)?;
			}
		}

		// Write to versioned index
		if let Some(ref versioned_index) = self.core.versioned_index {
			let mut versioned_index_guard = versioned_index.write().unwrap();
			for (encoded_key, encoded_value) in reverse_timestamp_entries {
				versioned_index_guard.insert(encoded_key.as_ref(), encoded_value.as_ref())?;
			}
		}

		// Then write to WAL
		if let Some(ref wal) = self.core.wal {
			let mut wal_guard = wal.write();
			let enc_bytes = processed_batch.encode()?;
			wal_guard.append(&enc_bytes)?;
			if sync {
				wal_guard.sync()?;
			}
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
		let active_memtable = self.core.active_memtable.read().unwrap();

		// Check if memtable needs flushing
		if active_memtable.size() > self.core.opts.max_memtable_size {
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
	pub(crate) fn replay_wal_with_repair<F>(
		wal_path: &Path,
		context: &str, // "Database startup" or "Database reload"
		mut set_recovered_memtable: F,
	) -> Result<u64>
	where
		F: FnMut(Arc<MemTable>) -> Result<()>,
	{
		// Create a new empty memtable to recover WAL entries
		let recovered_memtable = Arc::new(MemTable::default());

		// Replay WAL with automatic repair on corruption
		let wal_seq_num = match replay_wal(wal_path, &recovered_memtable)? {
			(seq_num, None) => {
				// No corruption detected, successful replay
				seq_num
			}
			(_seq_num, Some((corrupted_segment_id, last_valid_offset))) => {
				eprintln!(
                    "Detected WAL corruption in segment {corrupted_segment_id} at offset {last_valid_offset}. Attempting repair..."
                );

				// Attempt to repair the corrupted segment
				if let Err(repair_err) =
					repair_corrupted_wal_segment(wal_path, corrupted_segment_id)
				{
					eprintln!("Failed to repair WAL segment: {repair_err}");
					// Fail fast - cannot continue with corrupted WAL
					return Err(Error::Other(format!(
                        "{context} failed: WAL segment {corrupted_segment_id} is corrupted and could not be repaired. {repair_err}"
                    )));
				}

				// After repair, try to replay again to get any additional data
				// Create a fresh memtable for the retry
				let retry_memtable = Arc::new(MemTable::default());
				match replay_wal(wal_path, &retry_memtable) {
					Ok((retry_seq_num, None)) => {
						// Successful replay after repair, use the retry memtable
						if !retry_memtable.is_empty() {
							set_recovered_memtable(retry_memtable)?;
						}
						retry_seq_num
					}
					Ok((_retry_seq_num, Some((seg_id, offset)))) => {
						// WAL is still corrupted after repair - this is a serious problem
						return Err(Error::Other(format!(
                            "{context} failed: WAL segment {seg_id} still corrupted after repair at offset {offset}. Repair was incomplete."
                        )));
					}
					Err(retry_err) => {
						// Replay failed after successful repair - also a serious problem
						return Err(Error::Other(format!(
                            "{context} failed: WAL replay failed after successful repair. {retry_err}"
                        )));
					}
				}
			}
		};

		if !recovered_memtable.is_empty() {
			set_recovered_memtable(recovered_memtable)?;
		}

		Ok(wal_seq_num)
	}

	/// Creates a new LSM tree with background task management
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		let inner = Arc::new(CoreInner::new(opts.clone())?);

		if opts.in_memory_only {
			// For in-memory mode, create a minimal core without background tasks
			let commit_env = Arc::new(LsmCommitEnv::new_in_memory(inner.clone())?);
			let commit_pipeline = CommitPipeline::new(commit_env);

			// Set initial sequence number
			commit_pipeline.set_seq_num(1);

			Ok(Self {
				inner: inner.clone(),
				commit_pipeline: commit_pipeline.clone(),
				task_manager: Mutex::new(None),
				vlog_gc_manager: Mutex::new(None),
			})
		} else {
			// Initialize background task manager
			let task_manager = Arc::new(TaskManager::new(inner.clone()));

			let commit_env = Arc::new(LsmCommitEnv::new(inner.clone(), task_manager.clone())?);

			let commit_pipeline = CommitPipeline::new(commit_env);

			// Path for the WAL directory
			let wal_path = opts.wal_dir();

			// Replay WAL with automatic repair on corruption
			let wal_seq_num =
				Self::replay_wal_with_repair(&wal_path, "Database startup", |memtable| {
					let mut active_memtable = inner.active_memtable.write().unwrap();
					*active_memtable = memtable;
					Ok(())
				})?;

			// Update sequence number to max of LSM sequence number and WAL sequence number
			let current_seq_num = inner.level_manifest.as_ref().unwrap().read().unwrap().lsn();
			let max_seq_num = std::cmp::max(current_seq_num, wal_seq_num);

			// Set visible sequence number ts to highest of lsn from Wal, or latest table in manifest
			commit_pipeline.set_seq_num(max_seq_num);

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
			}

			Ok(core)
		}
	}

	pub(crate) async fn commit(&self, batch: Batch, sync: bool) -> Result<()> {
		// Commit the batch using the commit pipeline
		self.commit_pipeline.commit(batch, sync).await
	}

	pub(crate) fn seq_num(&self) -> u64 {
		self.commit_pipeline.get_visible_seq_num()
	}

	/// Safely closes the LSM tree by shutting down all components in the correct order.
	///
	/// 1. The commit pipeline is shut down and all pending operations complete
	/// 2. Background tasks (memtable flushing, compaction) are stopped safely
	/// 3. The WAL is closed and all data is flushed to disk
	/// 4. All directories are fsync'd to ensure durability
	pub async fn close(&self) -> Result<()> {
		// Step 1: Shutdown the commit pipeline to stop accepting new writes
		self.commit_pipeline.shutdown();

		// Step 2: Wait for and stop all background tasks
		let task_manager = self.task_manager.lock().unwrap().take();
		if let Some(task_manager) = task_manager {
			task_manager.stop().await;
		}

		// Stop VLog GC manager if it exists
		let vlog_gc_manager = self.vlog_gc_manager.lock().unwrap().take();
		if let Some(vlog_gc_manager) = vlog_gc_manager {
			vlog_gc_manager.stop().await;
		}

		// Step 3: Flush the active memtable only if it exceeds the configured size
		// Skip this in memory-only mode since we don't persist to disk
		if !self.inner.opts.in_memory_only {
			let active_memtable = self.inner.active_memtable.read().unwrap();
			if active_memtable.size() > self.inner.opts.max_memtable_size {
				self.inner.compact_memtable()?;
			}
		}

		// Step 4: Close the WAL to ensure all data is flushed
		// This is safe now because all background tasks that could write to WAL are stopped
		if let Some(ref wal) = self.inner.wal {
			let mut wal_guard = wal.write();
			wal_guard.close().map_err(|e| Error::Other(format!("Failed to close WAL: {e}")))?;
		}

		// Step 5: Flush all directories to ensure durability
		// Skip this in memory-only mode since we don't persist to disk
		sync_directory_structure(&self.inner.opts)?;

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
		if opts.in_memory_only {
			return Ok(());
		}

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
			let mut levels_guard =
				self.core.inner.level_manifest.as_ref().unwrap().write().unwrap();
			*levels_guard = new_levels;
		}

		// Clear the current memtables since they would be stale after restore
		// This discards any pending writes, which is correct for restore operations
		{
			let mut active_memtable = self.core.inner.active_memtable.write().unwrap();
			*active_memtable = Arc::new(MemTable::default());
		}

		{
			let mut immutable_memtables = self.core.inner.immutable_memtables.write().unwrap();
			*immutable_memtables = ImmutableMemtables::default();
		}

		// Reopen the WAL from the restored directory
		{
			let mut wal_guard = self.core.inner.wal.as_ref().unwrap().write();
			let wal_path = self.core.inner.opts.path.join("wal");
			let new_wal = Wal::open(&wal_path, wal::Options::default())?;
			*wal_guard = new_wal;
		}

		// Replay any WAL entries that were restored
		let wal_path = self.core.inner.opts.path.join("wal");
		let wal_seq_num =
			Core::replay_wal_with_repair(&wal_path, "Database restore", |memtable| {
				let mut active_memtable = self.core.inner.active_memtable.write().unwrap();
				*active_memtable = memtable;
				Ok(())
			})?;

		// Update sequence number to max of LSM sequence number and WAL sequence number
		let current_seq_num =
			self.core.inner.level_manifest.as_ref().unwrap().read().unwrap().lsn();
		let max_seq_num = std::cmp::max(current_seq_num, wal_seq_num);

		// Set visible sequence number to highest of lsn from WAL or latest table in manifest
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
		self.core.make_room_for_write()
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
						eprintln!("Error closing store: {}", err);
					}
				});
			} else {
				eprintln!("No runtime available for closing the store correctly");
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

	/// Sets the in-memory only mode.
	pub fn with_in_memory_only(mut self, in_memory: bool) -> Self {
		self.opts = self.opts.with_in_memory_only(in_memory);
		self
	}

	/// Enables or disables versioned queries with timestamp tracking
	pub fn with_versioning(mut self, enable: bool, retention_ns: u64) -> Self {
		self.opts = self.opts.with_versioning(enable, retention_ns);
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
	let file = File::open(path)?;
	debug_assert!(file.metadata()?.is_dir());
	file.sync_all()
}

/// Syncs all directory structures for the LSM store to ensure durability
/// Returns explicit errors indicating which path failed
fn sync_directory_structure(opts: &Options) -> Result<()> {
	if opts.in_memory_only {
		return Ok(());
	}

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

	#[tokio::test]
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
		assert_eq!(result, value.as_bytes().into());
	}

	#[tokio::test]
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
			assert_eq!(result, expected_value.as_bytes().into());
		}
	}

	#[tokio::test]
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

	#[tokio::test]
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
				expected_value.as_bytes().into(),
				"Key '{key}' should have final value '{expected_value}'"
			);
		}

		// Verify the LSM state: we should have multiple SSTables
		let l0_size =
			tree.core.level_manifest.as_ref().unwrap().read().unwrap().levels.get_levels()[0]
				.tables
				.len();
		assert!(l0_size > 0, "Expected SSTables in L0, got {l0_size}");
	}

	#[tokio::test]
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
				tree.core.level_manifest.as_ref().unwrap().read().unwrap().levels.get_levels()[0]
					.tables
					.len();
			assert!(l0_size > 0, "Expected SSTables in L0 before closing, got {l0_size}");

			// Tree will be dropped here, closing the store
			tree.close().await.unwrap();
		}

		// Step 2: Reopen the store and verify data
		{
			let tree = Tree::new(opts.clone()).unwrap();

			// Verify L0 has tables after reopening
			let l0_size =
				tree.core.level_manifest.as_ref().unwrap().read().unwrap().levels.get_levels()[0]
					.tables
					.len();
			assert!(l0_size > 0, "Expected SSTables in L0 after reopening, got {l0_size}");

			// Verify all keys have their final values
			for (key, expected_value) in expected_values.iter() {
				let txn = tree.begin().unwrap();
				let result = txn.get(key.as_bytes()).unwrap();

				assert!(result.is_some(), "Key '{key}' not found after reopening store");

				let value = result.unwrap();
				assert_eq!(
					value,
					expected_value.as_bytes().into(),
					"Key '{}' has incorrect value after reopening. Expected '{}', got '{:?}'",
					key,
					expected_value,
					std::str::from_utf8(value.as_ref()).unwrap_or("<invalid utf8>")
				);
			}
		}
	}

	#[tokio::test]
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
			assert_eq!(result.unwrap(), expected_value.as_bytes().into());
		}

		// Verify the newer keys don't exist after restore
		for i in 10..15 {
			let key = format!("key_{i:03}");

			let txn = tree.begin().unwrap();
			let result = txn.get(key.as_bytes()).unwrap();
			assert!(result.is_none(), "Key '{key}' should not exist after restore");
		}
	}

	#[tokio::test]
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
				format!("pending_value_{i:03}").as_bytes().into(),
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
				expected_value.as_bytes().into(),
				"Key '{key}' should have checkpoint value '{expected_value}', not pending value"
			);
		}
	}

	#[tokio::test]
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
			txn.range(b"a", b"c", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

		// Should return keys "a", "b", "c" (inclusive range)
		assert_eq!(range_before_flush.len(), 3, "Range scan before flush should return 3 items");

		// Verify the keys and values are correct
		let expected_before = [("a", "a"), ("b", "b"), ("c", "c")];
		for (idx, (key, value)) in range_before_flush.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref().unwrap().as_ref()).unwrap();
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
			txn.range(b"a", b"c", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

		// Should return the same keys after flush
		assert_eq!(range_after_flush.len(), 3, "Range scan after flush should return 3 items");

		// Verify the keys and values are still correct after flush
		let expected_after = [("a", "a"), ("b", "b"), ("c", "c")];
		for (idx, (key, value)) in range_after_flush.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref().unwrap().as_ref()).unwrap();
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

	#[tokio::test]
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
			txn.range(start_key, end_key, None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

		assert_eq!(
			range_result.len(),
			TOTAL_ITEMS,
			"Full range scan should return all {TOTAL_ITEMS} items"
		);

		// Verify each item is correct and in order
		for (idx, (key, value)) in range_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref().unwrap().as_ref()).unwrap();

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

		// Test 2: Partial range scan - first 100 items
		let partial_start = "key_000000".as_bytes();
		let partial_end = "key_000099".as_bytes();

		let txn = tree.begin().unwrap();
		let partial_result: Vec<_> = txn
			.range(partial_start, partial_end, None)
			.unwrap()
			.map(|r| r.unwrap())
			.collect::<Vec<_>>();

		assert_eq!(partial_result.len(), 100, "Partial range scan should return 100 items");

		// Verify each item in partial range
		for (idx, (key, value)) in partial_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref().unwrap().as_ref()).unwrap();

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
		let middle_end = "key_005099".as_bytes();

		let txn = tree.begin().unwrap();
		let middle_result: Vec<_> = txn
			.range(middle_start, middle_end, None)
			.unwrap()
			.map(|r| r.unwrap())
			.collect::<Vec<_>>();

		assert_eq!(middle_result.len(), 100, "Middle range scan should return 100 items");

		// Verify each item in middle range
		for (idx, (key, value)) in middle_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref().unwrap().as_ref()).unwrap();

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
		let end_end = "key_009999".as_bytes();

		let txn = tree.begin().unwrap();
		let end_result: Vec<_> =
			txn.range(end_start, end_end, None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

		assert_eq!(end_result.len(), 100, "End range scan should return 100 items");

		// Verify each item in end range
		for (idx, (key, value)) in end_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref().unwrap().as_ref()).unwrap();

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
		let single_end = "key_004567".as_bytes();

		let txn = tree.begin().unwrap();
		let single_result: Vec<_> = txn
			.range(single_start, single_end, None)
			.unwrap()
			.map(|r| r.unwrap())
			.collect::<Vec<_>>();

		assert_eq!(single_result.len(), 1, "Single item range scan should return 1 item");

		let (key, value) = &single_result[0];
		let key_str = std::str::from_utf8(key.as_ref()).unwrap();
		let value_str = std::str::from_utf8(value.as_ref().unwrap().as_ref()).unwrap();

		assert_eq!(key_str, "key_004567");
		assert_eq!(value_str, "value_004567_content_data_9134");

		// Test 6: Empty range scan (non-existent range)
		let empty_start = "key_999999".as_bytes();
		let empty_end = "key_888888".as_bytes(); // end < start, should be empty

		let txn = tree.begin().unwrap();
		let empty_result: Vec<_> = txn.range(empty_start, empty_end, None).unwrap().collect();

		assert_eq!(empty_result.len(), 0, "Empty range scan should return 0 items");
	}

	#[tokio::test]
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
			.range(start_key, end_key, None)
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
			let value_str = std::str::from_utf8(value.as_ref().unwrap().as_ref()).unwrap();

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

	#[tokio::test]
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
			.range(&beg, &end, None)
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
			let value_str = std::str::from_utf8(value.as_ref().unwrap().as_ref()).unwrap();

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

	#[tokio::test]
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
			.range(&beg, &end, Some(100))
			.unwrap()
			.map(|r| match r {
				Ok(kv) => kv,
				Err(e) => {
					eprintln!("Error in range iterator: {e:?}");
					panic!("Range iterator error: {e}");
				}
			})
			.collect::<Vec<_>>();

		assert_eq!(
			limited_result.len(),
			100,
			"Range scan with limit 100 should return exactly 100 items"
		);

		for (idx, (key, value)) in limited_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref().unwrap().as_ref()).unwrap();

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
			txn.range(&beg, &end, Some(1000)).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

		assert_eq!(
			limited_result_1000.len(),
			1000,
			"Range scan with limit 1000 should return exactly 1000 items"
		);

		for (idx, (key, value)) in limited_result_1000.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref().unwrap().as_ref()).unwrap();

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
		let limited_result_large: Vec<_> = txn.range(&beg, &end, Some(15000)).unwrap().collect();

		assert_eq!(
			limited_result_large.len(),
			TOTAL_ITEMS,
			"Range scan with limit 15000 should return all {TOTAL_ITEMS} items"
		);

		let txn = tree.begin().unwrap();
		let unlimited_result: Vec<_> = txn.range(&beg, &end, None).unwrap().collect();

		assert_eq!(
			unlimited_result.len(),
			TOTAL_ITEMS,
			"Range scan with no limit should return all {TOTAL_ITEMS} items"
		);

		let txn = tree.begin().unwrap();
		let single_result: Vec<_> =
			txn.range(&beg, &end, Some(1)).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

		assert_eq!(single_result.len(), 1, "Range scan with limit 1 should return exactly 1 item");

		let (key, value) = &single_result[0];
		let key_str = std::str::from_utf8(key.as_ref()).unwrap();
		let value_str = std::str::from_utf8(value.as_ref().unwrap().as_ref()).unwrap();

		assert_eq!(key_str, "key_000000");
		assert_eq!(value_str, "value_000000_content_data_0");

		let txn = tree.begin().unwrap();
		let zero_result: Vec<_> = txn.range(&beg, &end, Some(0)).unwrap().collect();

		assert_eq!(zero_result.len(), 0, "Range scan with limit 0 should return no items");
	}

	#[tokio::test]
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
		let iter = txn.range(&beg, &end, Some(100)).unwrap();
		let iter = iter.into_iter();

		let range_result: Vec<_> = iter.skip(5000).take(100).collect();

		assert_eq!(
			range_result.len(),
			0,
			"Range scan with limit 100 followed by skip(5000).take(100) should return 0 items"
		);

		let txn = tree.begin().unwrap();
		let unlimited_range_result: Vec<_> = txn
			.range(&beg, &end, None)
			.unwrap()
			.skip(5000)
			.take(100)
			.map(|r| r.unwrap())
			.collect::<Vec<_>>();

		assert_eq!(
			unlimited_range_result.len(),
			100,
			"Range scan with no limit followed by skip(5000).take(100) should return 100 items"
		);

		for (idx, (key, value)) in unlimited_range_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref().unwrap().as_ref()).unwrap();

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

		let txn = tree.begin().unwrap();
		let large_limit_result: Vec<_> = txn
			.range(&beg, &end, Some(10000))
			.unwrap()
			.skip(5000)
			.take(100)
			.map(|r| r.unwrap())
			.collect::<Vec<_>>();

		assert_eq!(
			large_limit_result.len(),
			100,
			"Range scan with limit 10000 followed by skip(5000).take(100) should return 100 items"
		);

		for (idx, (key, value)) in large_limit_result.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref().unwrap().as_ref()).unwrap();

			let expected_idx = 5000 + idx;
			let expected_key = &expected_items[expected_idx].0;
			let expected_value = &expected_items[expected_idx].1;

			assert_eq!(
                key_str, expected_key,
                "Key mismatch at index {idx} with limit 10000: expected '{expected_key}', found '{key_str}'"
            );
			assert_eq!(
                value_str, expected_value,
                "Value mismatch at index {idx} with limit 10000: expected '{expected_value}', found '{value_str}'"
            );
		}

		// Case: limit larger than total items, but skip is large
		let txn = tree.begin().unwrap();
		let res: Vec<_> = txn
			.range(&beg, &end, Some(5050))
			.unwrap()
			.skip(5000)
			.take(100)
			.map(|r| r.unwrap())
			.collect::<Vec<_>>();

		assert_eq!(
			res.len(),
			50,
			"Range scan with limit 5050 followed by skip(5000).take(100) should return 50 items"
		);

		for (idx, (key, value)) in res.iter().enumerate() {
			let key_str = std::str::from_utf8(key.as_ref()).unwrap();
			let value_str = std::str::from_utf8(value.as_ref().unwrap().as_ref()).unwrap();

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

	#[tokio::test]
	async fn test_vlog() {
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
		assert_eq!(retrieved_small, small_value.as_bytes().into());

		let retrieved_large = txn.get(large_key.as_bytes()).unwrap().unwrap();
		assert_eq!(retrieved_large, large_value.as_bytes().into());
	}

	#[tokio::test(flavor = "multi_thread")]
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
							expected_value.as_bytes().into(),
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
	}

	#[tokio::test]
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
				expected_value.as_bytes().into(),
				"Key {key} has incorrect value across file rotation"
			);
		}
	}

	#[tokio::test]
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

	#[tokio::test]
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
			tx.set(key, value.as_bytes()).unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();
		}

		// Insert second version of each key
		for (i, key) in keys.iter().enumerate() {
			let value = format!("value-{}-v2", i + 1);
			let mut tx = tree.begin().unwrap();
			tx.set(key, value.as_bytes()).unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();
		}

		// Verify the latest values before delete
		for (i, key) in keys.iter().enumerate() {
			let tx = tree.begin().unwrap();
			let result = tx.get(key).unwrap();
			let expected = format!("value-{}-v2", i + 1);
			assert_eq!(result.map(|v| v.to_vec()), Some(expected.as_bytes().to_vec()));
		}

		// Delete all keys
		for key in keys.iter() {
			let mut tx = tree.begin().unwrap();
			tx.delete(key).unwrap();
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
			let result = tx.get(key).unwrap();
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

	#[tokio::test]
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
			tx.set(key, value.as_bytes()).unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();
		}

		// Insert second version of each key
		for (i, key) in keys.iter().enumerate() {
			let value = format!("value-{}-v2", i + 1);
			let mut tx = tree.begin().unwrap();
			tx.set(key, value.as_bytes()).unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();
		}

		// Insert third version of each key
		for (i, key) in keys.iter().enumerate() {
			let value = format!("value-{}-v3", i + 1);
			let mut tx = tree.begin().unwrap();
			tx.set(key, value.as_bytes()).unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();
		}

		// Insert fourth version of each key
		for (i, key) in keys.iter().enumerate() {
			let value = format!("value-{}-v4", i + 1);
			let mut tx = tree.begin().unwrap();
			tx.set(key, value.as_bytes()).unwrap();
			tx.commit().await.unwrap();
			tree.flush().unwrap();
		}

		// Verify the latest values before delete
		for (i, key) in keys.iter().enumerate() {
			let tx = tree.begin().unwrap();
			let result = tx.get(key).unwrap();
			assert_eq!(
				result.map(|v| v.to_vec()),
				Some(format!("value-{}-v4", i + 1).as_bytes().to_vec())
			);
		}

		// Delete all keys
		for key in keys.iter() {
			let mut tx = tree.begin().unwrap();
			tx.delete(key).unwrap();
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
			let result = tx.get(key).unwrap();
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

	#[tokio::test]
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

	#[tokio::test]
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

	#[tokio::test]
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
				tree.core.level_manifest.as_ref().unwrap().read().unwrap().levels.get_levels()[0]
					.tables
					.len();
			assert_eq!(l0_size, 2, "Expected 2 tables in L0 after initial writes, got {l0_size}");

			// Get the table IDs from the first session
			let (table1_id, table2_id, next_table_id) = {
				let manifest = tree.core.level_manifest.as_ref().unwrap().read().unwrap();
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
					tree.core.level_manifest.as_ref().unwrap().read().unwrap().levels.get_levels()
						[0]
					.tables
					.len();
				assert_eq!(l0_size, 2, "Expected 2 tables in L0 after reopening, got {l0_size}");
				// Get the table IDs after reopening
				let manifest = tree.core.level_manifest.as_ref().unwrap().read().unwrap();
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
					tree.core.level_manifest.as_ref().unwrap().read().unwrap().levels.get_levels()
						[0]
					.tables
					.len();
				assert_eq!(
					l0_size, 3,
					"Expected 3 tables in L0 after adding more data, got {l0_size}"
				);

				// Get the table IDs from all 3 tables
				let manifest = tree.core.level_manifest.as_ref().unwrap().read().unwrap();
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
				tree.core.level_manifest.as_ref().unwrap().read().unwrap().levels.get_levels()[0]
					.tables
					.len();
			assert_eq!(l0_size, 3, "Expected 3 tables in L0 after final reopen, got {l0_size}");

			// Verify table IDs are still in correct order (newer tables first)
			let manifest = tree.core.level_manifest.as_ref().unwrap().read().unwrap();
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

	#[tokio::test]
	async fn test_in_memory_only_mode() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		// Create options with in-memory mode enabled
		let opts = create_test_options(path.clone(), |opts| {
			opts.in_memory_only = true;
		});

		let tree = Tree::new(opts.clone()).unwrap();

		// Test basic operations
		let key = "test_key";
		let value = "test_value";

		// Insert a key-value pair
		let mut txn = tree.begin().unwrap();
		txn.set(key.as_bytes(), value.as_bytes()).unwrap();
		txn.commit().await.unwrap();

		// Read back the key-value pair
		let txn = tree.begin().unwrap();
		let result = txn.get(key.as_bytes()).unwrap().unwrap();
		assert_eq!(result, value.as_bytes().into());

		// Test multiple operations
		for i in 0..10 {
			let key = format!("key_{}", i);
			let value = format!("value_{}", i);

			let mut txn = tree.begin().unwrap();
			txn.set(key.as_bytes(), value.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Verify all keys exist
		for i in 0..10 {
			let key = format!("key_{}", i);
			let expected_value = format!("value_{}", i);

			let txn = tree.begin().unwrap();
			let result = txn.get(key.as_bytes()).unwrap().unwrap();
			assert_eq!(result, expected_value.as_bytes().into());
		}

		// Test range scan
		let txn = tree.begin().unwrap();
		let range_result: Vec<_> =
			txn.range(b"key_0", b"key_9", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

		assert_eq!(range_result.len(), 10, "Range scan should return 10 items");

		// Test delete
		let mut txn = tree.begin().unwrap();
		txn.delete("key_5".as_bytes()).unwrap();
		txn.commit().await.unwrap();

		let txn = tree.begin().unwrap();
		let result = txn.get("key_5".as_bytes()).unwrap();
		assert!(result.is_none(), "Deleted key should not exist");

		// Verify other keys still exist
		let txn = tree.begin().unwrap();
		let result = txn.get("key_4".as_bytes()).unwrap().unwrap();
		assert_eq!(result, "value_4".as_bytes().into());

		// Verify that level_manifest is None in memory-only mode
		assert!(
			tree.core.level_manifest.is_none(),
			"Level manifest should be None in memory-only mode"
		);

		// Verify that WAL is None in memory-only mode
		assert!(tree.core.wal.is_none(), "WAL should be None in memory-only mode");

		// Verify that task_manager is None in memory-only mode
		assert!(
			tree.core.task_manager.lock().unwrap().is_none(),
			"Task manager should be None in memory-only mode"
		);
	}

	#[tokio::test(flavor = "multi_thread")]
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

	#[tokio::test]
	async fn test_tree_builder() {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		// Test TreeBuilder with default key type (InternalKey)
		let tree = TreeBuilder::new()
			.with_path(path.clone())
			.with_max_memtable_size(64 * 1024)
			.with_enable_vlog(true)
			.with_vlog_max_file_size(1024 * 1024)
			.with_in_memory_only(false)
			.build()
			.unwrap();

		// Test basic operations
		let mut txn = tree.begin().unwrap();
		txn.set(b"test_key", b"test_value").unwrap();
		txn.commit().await.unwrap();

		let txn = tree.begin().unwrap();
		let result = txn.get(b"test_key").unwrap().unwrap();
		assert_eq!(result, Arc::from(b"test_value".as_slice()));

		// Test build_with_options
		let (tree2, opts) = TreeBuilder::new()
			.with_path(temp_dir.path().join("tree2"))
			.with_in_memory_only(true)
			.build_with_options()
			.unwrap();

		// Verify the options are accessible
		assert!(opts.in_memory_only);
		assert_eq!(opts.path, temp_dir.path().join("tree2"));

		// Test basic operations on the second tree
		let mut txn = tree2.begin().unwrap();
		txn.set(b"key2", b"value2").unwrap();
		txn.commit().await.unwrap();

		let txn = tree2.begin().unwrap();
		let result = txn.get(b"key2").unwrap().unwrap();
		assert_eq!(result, Arc::from(b"value2".as_slice()));
	}

	#[tokio::test]
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
				.range(b"test".as_slice(), b"testz".as_slice(), None)
				.unwrap()
				.map(|r| r.unwrap())
				.collect::<Vec<_>>();

			// Should be empty since test_key is soft deleted
			assert!(range_result.is_empty(), "Range scan should not include soft deleted keys");

			// Test range scan that includes the other key
			let txn = tree.begin().unwrap();
			let range_result: Vec<_> = txn
				.range(b"other".as_slice(), b"otherz".as_slice(), None)
				.unwrap()
				.map(|r| r.unwrap())
				.collect::<Vec<_>>();

			// Should contain the other key
			assert_eq!(range_result.len(), 1);
			assert_eq!(range_result[0].0.as_ref(), b"other_key");
			assert_eq!(range_result[0].1.as_ref().unwrap().as_ref(), b"other_value");

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

	#[tokio::test]
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
}
