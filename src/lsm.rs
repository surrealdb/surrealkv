use std::{
    fs::{create_dir_all, File},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
};

use crate::{
    batch::Batch,
    checkpoint::{CheckpointMetadata, DatabaseCheckpoint},
    commit::{CommitEnv, CommitPipeline},
    compaction::{
        compactor::{CompactionOptions, Compactor},
        CompactionStrategy,
    },
    error::Result,
    levels::{write_levels_to_disk, LevelManifest},
    memtable::{ImmutableMemtables, MemTable},
    oracle::Oracle,
    snapshot::Counter as SnapshotCounter,
    sstable::table::Table,
    task::TaskManager,
    transaction::{Mode, Transaction},
    vlog::{VLog, VLogGCManager},
    wal::{self, writer::Wal},
    Options,
};

use async_trait::async_trait;

// ===== Constants =====
pub const TABLE_FOLDER: &str = "sstables";
pub const LEVELS_MANIFEST_FILE: &str = "levels";

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
pub struct CoreInner {
    /// Monotonically increasing counter for generating unique SSTable IDs.
    /// Each SSTable needs a unique identifier for file naming and tracking.
    pub table_id_counter: Arc<AtomicU64>,

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
    pub levels: Arc<RwLock<LevelManifest>>,

    /// Configuration options controlling LSM tree behavior
    pub opts: Arc<Options>,

    /// Counter tracking active snapshots for MVCC (Multi-Version Concurrency Control).
    /// Snapshots provide consistent point-in-time views of the data.
    pub(crate) snapshot_counter: SnapshotCounter,

    /// Oracle managing transaction timestamps for MVCC.
    /// Provides monotonic timestamps for transaction ordering and conflict resolution.
    pub(crate) oracle: Arc<Oracle>,

    /// Value Log (VLog)
    pub(crate) vlog: Arc<VLog>,

    /// Write-Ahead Log (WAL) for durability
    wal: parking_lot::RwLock<Wal>,
}

impl CoreInner {
    /// Creates a new LSM tree core instance
    pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
        // Initialize the level manifest which tracks the SSTable organization
        let levels = Arc::new(RwLock::new(LevelManifest::new(opts.clone())?));

        // Initialize active and immutable memtables
        let active_memtable = Arc::new(RwLock::new(Arc::new(MemTable::default())));
        let immutable_memtables = Arc::new(RwLock::new(ImmutableMemtables::default()));

        // TODO: Add a way to recover from the WAL or level manifest
        // Initialize the transaction oracle for MVCC support
        // The oracle provides monotonic timestamps for transaction ordering
        let oracle = Oracle::new();

        let wal_path = opts.path.join("wal");
        let wal = Wal::open(&wal_path, wal::Options::default())?;

        let vlog_path = opts.path.join("vlog");
        let vlog = Arc::new(VLog::new(
            &vlog_path,
            opts.vlog_value_threshold,
            opts.vlog_max_file_size,
            opts.vlog_checksum_verification,
            active_memtable.clone(),
            immutable_memtables.clone(),
            levels.clone(),
        )?);

        Ok(Self {
            table_id_counter: Arc::new(AtomicU64::default()),
            opts,
            active_memtable,
            immutable_memtables,
            levels,
            snapshot_counter: SnapshotCounter::default(),
            oracle: Arc::new(oracle),
            vlog,
            wal: parking_lot::RwLock::new(wal),
        })
    }

    /// Generates the next unique table ID for a new SSTable
    pub fn next_table_id(&self) -> u64 {
        self.table_id_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Makes room for new writes by flushing the active memtable if needed.
    ///
    /// This implements the core LSM write path:
    /// 1. When the active memtable is full, it becomes immutable
    /// 2. A new empty active memtable is created for new writes
    /// 3. The immutable memtable is flushed to disk as an SSTable
    /// 4. The new SSTable is added to Level 0
    pub fn make_room_for_write(&self) -> Result<Option<PathBuf>> {
        // Atomically flush memtable and rotate WAL under a single lock
        // This prevents race conditions where writes could go to the wrong WAL segment
        let Some((table_id, flushed_memtable)) = self.flush_active_memtable_and_rotate_wal()?
        else {
            return Ok(None);
        };

        let table_folder = self.opts.path.join(TABLE_FOLDER);

        // Flush the immutable memtable to disk as an SSTable
        // This converts the in-memory sorted data structure to an on-disk format
        let table = flushed_memtable.flush(
            table_id,
            table_folder.clone(),
            self.opts.clone(),
            self.vlog.clone(),
        )?;

        // Add the new SSTable to Level 0
        // L0 is special: it contains recently flushed SSTables that may have overlapping keys
        self.add_table_to_l0(table)?;

        // Clean up old WAL segments since the memtable has been successfully flushed
        // Done asynchronously to avoid blocking the flush operation
        let wal_guard = self.wal.read();
        let wal_dir = wal_guard.get_dir_path().to_path_buf();
        tokio::spawn(async move {
            if let Err(e) = crate::wal::cleanup::cleanup_old_segments(&wal_dir) {
                eprintln!("Failed to clean up old WAL segments: {}", e);
            }
        });

        Ok(Some(table_folder))
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
        let memtable_id = self.next_table_id();
        immutable_memtables.add(memtable_id, flushed_memtable.clone());

        // Now rotate the WAL while still holding the active_memtable lock
        // This ensures that:
        // - The flushed memtable corresponds to the previous WAL segment(s)
        // - New writes (to the new active memtable) go to a new WAL segment
        // - No race condition between memtable swap and WAL rotation
        {
            let mut wal_guard = self.wal.write();
            wal_guard.rotate()?;
        }

        Ok(Some((memtable_id, flushed_memtable)))
    }

    /// Converts the active memtable to immutable and creates a new active memtable.
    ///
    /// This is a key operation in the LSM write path that ensures:
    /// - Writes can continue without blocking on disk I/O
    /// - The immutable memtable preserves a consistent snapshot for flushing
    ///
    /// Note: This method does NOT rotate the WAL. For coordinated memtable flush + WAL rotation,
    /// use flush_active_memtable_and_rotate_wal() instead.
    pub fn flush_active_memtable(&self) -> Option<(u64, Arc<MemTable>)> {
        let mut active_memtable = self.active_memtable.write().unwrap();

        // Don't flush an empty memtable
        if active_memtable.is_empty() {
            return None;
        }

        let mut immutable_memtables = self.immutable_memtables.write().unwrap();

        // Atomically swap the active memtable with a new empty one
        // This allows writes to continue immediately
        let flushed_memtable = std::mem::take(&mut *active_memtable);

        // Track the immutable memtable until it's successfully flushed
        let memtable_id = self.next_table_id();
        immutable_memtables.add(memtable_id, flushed_memtable.clone());

        Some((memtable_id, flushed_memtable))
    }

    /// Adds a newly flushed SSTable to Level 0.
    ///
    /// Level 0 is unique in the LSM tree hierarchy:
    /// - It contains SSTables flushed directly from memtables
    /// - SSTables may have overlapping key ranges
    /// - Queries must check all L0 SSTables
    /// - Too many L0 files triggers compaction to maintain read performance
    pub fn add_table_to_l0(&self, table: Arc<Table>) -> Result<()> {
        let mut original_manifest = self.levels.write().unwrap();
        let mut memtable_lock = self.immutable_memtables.write().unwrap();
        let mut current_levels = original_manifest.levels.clone();
        let table_id = table.id;

        // Add the SSTable to Level 0
        Arc::make_mut(
            current_levels
                .get_first_level_mut()
                .expect("Level 0 must exist in LSM tree"),
        )
        .insert(table);

        // Persist the updated level structure to disk for crash recovery
        let next_table_id = original_manifest.next_table_id.load(Ordering::SeqCst);
        write_levels_to_disk(&original_manifest.path, &current_levels, next_table_id)?;

        // Update in-memory state
        original_manifest.levels = current_levels;

        // Remove the successfully flushed memtable from tracking
        memtable_lock.remove(table_id);

        Ok(())
    }
}

impl CompactionOperations for CoreInner {
    /// Triggers a memtable flush to create space for new writes
    fn compact_memtable(&self) -> Result<()> {
        let _memtable_path = self.make_room_for_write()?;
        Ok(())
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

        Ok(())
    }
}

struct LsmCommitEnv {
    core: Arc<CoreInner>,

    /// Manages background tasks like flushing and compaction
    task_manager: Arc<TaskManager>,
}

impl LsmCommitEnv {
    /// Creates a new commit environment for the LSM tree
    pub fn new(core: Arc<CoreInner>, task_manager: Arc<TaskManager>) -> Result<Self> {
        Ok(Self { core, task_manager })
    }
}

impl CommitEnv for LsmCommitEnv {
    // Write batch to WAL (synchronous operation)
    fn write(&self, batch: &Batch, seq_num: u64, _sync_wal: bool) -> Result<()> {
        let mut wal_guard = self.core.wal.write();
        let enc_bytes = batch.encode(seq_num)?;
        wal_guard
            .append(&enc_bytes)
            .map_err(|e: crate::wal::Error| e.into())
            .map(|_| ())
    }

    // Apply batch to memtable (can be called concurrently)
    fn apply(&self, batch: &Batch, seq_num: u64) -> Result<()> {
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
            self.task_manager.wake_up_memtable();
        }

        // Add the batch to the active memtable
        active_memtable.add(batch, seq_num)?;

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
pub struct Core {
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
    fn replay_wal_with_repair<F>(
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
        let wal_seq_num = match crate::wal::recovery::replay_wal(wal_path, &recovered_memtable)? {
            (seq_num, None) => {
                // No corruption detected, successful replay
                seq_num
            }
            (_seq_num, Some((corrupted_segment_id, last_valid_offset))) => {
                eprintln!(
                    "Detected WAL corruption in segment {} at offset {}. Attempting repair...",
                    corrupted_segment_id, last_valid_offset
                );

                // Attempt to repair the corrupted segment
                if let Err(repair_err) = crate::wal::recovery::repair_corrupted_wal_segment(
                    wal_path,
                    corrupted_segment_id,
                ) {
                    eprintln!("Failed to repair WAL segment: {}", repair_err);
                    // Fail fast - cannot continue with corrupted WAL
                    return Err(crate::error::Error::Other(format!(
                        "{} failed: WAL segment {} is corrupted and could not be repaired. {}",
                        context, corrupted_segment_id, repair_err
                    )));
                } else {
                    eprintln!("Successfully repaired WAL segment {}", corrupted_segment_id);

                    // After repair, try to replay again to get any additional data
                    // Create a fresh memtable for the retry
                    let retry_memtable = Arc::new(MemTable::default());
                    match crate::wal::recovery::replay_wal(wal_path, &retry_memtable) {
                        Ok((retry_seq_num, None)) => {
                            // Successful replay after repair, use the retry memtable
                            if !retry_memtable.is_empty() {
                                set_recovered_memtable(retry_memtable)?;
                            }
                            retry_seq_num
                        }
                        Ok((_retry_seq_num, Some((seg_id, offset)))) => {
                            // WAL is still corrupted after repair - this is a serious problem
                            return Err(crate::error::Error::Other(format!(
                                "{} failed: WAL segment {} still corrupted after repair at offset {}. Repair was incomplete.",
                                context, seg_id, offset
                            )));
                        }
                        Err(retry_err) => {
                            // Replay failed after successful repair - also a serious problem
                            return Err(crate::error::Error::Other(format!(
                                "{} failed: WAL replay failed after successful repair. {}",
                                context, retry_err
                            )));
                        }
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

        // Initialize background task manager
        let task_manager = Arc::new(TaskManager::new(inner.clone()));

        let commit_env = Arc::new(LsmCommitEnv::new(inner.clone(), task_manager.clone())?);

        let commit_pipeline = CommitPipeline::new(commit_env);

        // Path for the WAL directory
        let wal_path = opts.path.join("wal");

        // Replay WAL with automatic repair on corruption
        let wal_seq_num =
            Self::replay_wal_with_repair(&wal_path, "Database startup", |memtable| {
                let mut active_memtable = inner.active_memtable.write().unwrap();
                *active_memtable = memtable;
                Ok(())
            })?;

        // Update sequence number to max of LSM sequence number and WAL sequence number
        let current_seq_num = inner.levels.read().unwrap().lsn();
        let max_seq_num = std::cmp::max(current_seq_num, wal_seq_num);

        // Set visible sequence number ts to highest of lsn from Wal, or latest table in manifest
        commit_pipeline.set_seq_num(max_seq_num);

        let core = Self {
            inner: inner.clone(),
            commit_pipeline: commit_pipeline.clone(),
            task_manager: Mutex::new(Some(task_manager)),
            vlog_gc_manager: Mutex::new(None),
        };

        // Initialize VLog GC manager
        let vlog_gc_manager = VLogGCManager::new(inner.vlog.clone(), commit_pipeline.clone());
        vlog_gc_manager.start();
        *core.vlog_gc_manager.lock().unwrap() = Some(vlog_gc_manager);

        Ok(core)
    }

    pub(crate) async fn commit(&self, batch: Batch, sync_wal: bool) -> Result<()> {
        // Commit the batch using the commit pipeline
        self.commit_pipeline.commit(batch, sync_wal).await
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
        self.commit_pipeline.shutdown().await;

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

        // Step 3: Close the WAL to ensure all data is flushed
        // This is safe now because all background tasks that could write to WAL are stopped
        {
            let mut wal_guard = self.inner.wal.write();
            wal_guard
                .close()
                .map_err(|e| crate::error::Error::Other(format!("Failed to close WAL: {}", e)))?;
        }

        // Step 4: Flush all directories to ensure durability
        let base_path = &self.inner.opts.path;
        let table_folder_path = base_path.join(TABLE_FOLDER);
        let wal_path = base_path.join("wal");

        // Sync all directories
        fsync_directory(&table_folder_path).map_err(|e| {
            crate::error::Error::Other(format!("Failed to sync table directory: {}", e))
        })?;
        fsync_directory(&wal_path).map_err(|e| {
            crate::error::Error::Other(format!("Failed to sync WAL directory: {}", e))
        })?;
        fsync_directory(base_path).map_err(|e| {
            crate::error::Error::Other(format!("Failed to sync base directory: {}", e))
        })?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct Tree {
    core: Arc<Core>,
}

impl std::ops::Deref for Tree {
    type Target = Core;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl Tree {
    /// Creates a new LSM tree with the specified options
    pub fn new(opts: Arc<Options>) -> Result<Self> {
        opts.validate()?;
        let path = opts.path.clone();

        // Create directory structure
        create_dir_all(&path)?;
        let table_folder_path = path.join(TABLE_FOLDER);
        create_dir_all(&table_folder_path)?;
        let wal_path = path.join("wal");
        create_dir_all(&wal_path)?;

        // Create the core LSM tree components
        let core = Core::new(opts)?;

        // TODO: Add file to write options manifest
        // TODO: Add version header in file similar to table in WAL

        // Ensure directory changes are persisted
        fsync_directory(&table_folder_path)?;
        fsync_directory(&wal_path)?;
        fsync_directory(&path)?;

        Ok(Self {
            core: Arc::new(core),
        })
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
            let mut levels_guard = self.core.inner.levels.write().unwrap();
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
            let mut wal_guard = self.core.inner.wal.write();
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
        let current_seq_num = self.core.inner.levels.read().unwrap().lsn();
        let max_seq_num = std::cmp::max(current_seq_num, wal_seq_num);

        // Set visible sequence number to highest of lsn from WAL or latest table in manifest
        self.core.commit_pipeline.set_seq_num(max_seq_num);

        Ok(metadata)
    }

    #[cfg(test)]
    pub fn get_all_vlog_stats(&self) -> Vec<(u64, u64, u64, f64)> {
        self.core.vlog.get_all_file_stats()
    }

    pub async fn close(&self) -> Result<()> {
        self.core.close().await
    }

    /// Triggers VLog garbage collection manually
    pub async fn garbage_collect_vlog(&self) -> Result<Vec<u64>> {
        self.core
            .vlog
            .garbage_collect(self.core.commit_pipeline.clone())
            .await
    }

    /// Flushes the active memtable to disk
    pub fn flush(&self) -> Result<Option<PathBuf>> {
        self.core.make_room_for_write()
    }
}

/// Syncs a directory to ensure all changes are persisted to disk
pub fn fsync_directory<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
    let file = File::open(path)?;
    debug_assert!(file.metadata()?.is_dir());
    file.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    #[tokio::test]
    async fn test_tree_basic() {
        let temp_dir = create_temp_directory();
        let path = temp_dir.path().to_path_buf();

        let opts = Options {
            path: path.clone(),
            ..Default::default()
        };

        let tree = Tree::new(Arc::new(opts)).unwrap();

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

        let opts = Options {
            path: path.clone(),
            max_memtable_size: 132, // Total size of kvs below
            ..Default::default()
        };

        let key = "hello";
        let values = ["world", "universe", "everyone", "planet"];

        let tree = Tree::new(Arc::new(opts)).unwrap();

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

        let opts = Options {
            path: path.clone(),
            max_memtable_size: 132, // Total size of kvs below
            ..Default::default()
        };

        let key = "hello";
        let values = ["world", "universe", "everyone"];

        let tree = Tree::new(Arc::new(opts)).unwrap();

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
        let opts = Options {
            path: path.clone(),
            max_memtable_size: 64 * 1024, // 64KB
            ..Default::default()
        };

        let tree = Tree::new(Arc::new(opts)).unwrap();

        // Number of keys to insert
        const KEY_COUNT: usize = 10001;
        // Number of updates per key
        const UPDATES_PER_KEY: usize = 2;

        // Track the latest value for each key for verification
        let mut expected_values = Vec::with_capacity(KEY_COUNT);

        // Insert each key with multiple updates
        for i in 0..KEY_COUNT {
            let key = format!("key_{:05}", i);
            for update in 0..UPDATES_PER_KEY {
                let value = format!("value_{:05}_update_{}", i, update);

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
                "Key '{}' should have final value '{}'",
                key,
                expected_value
            );
        }

        // Verify the LSM state: we should have multiple SSTables
        let l0_size = tree.core.levels.read().unwrap().levels.get_levels()[0]
            .tables
            .len();
        assert!(l0_size > 0, "Expected SSTables in L0, got {}", l0_size);
    }

    #[tokio::test]
    async fn test_persistence() {
        let temp_dir = create_temp_directory();
        let path = temp_dir.path().to_path_buf();

        // Set a reasonable memtable size to trigger multiple flushes
        let opts = Arc::new(Options {
            path: path.clone(),
            max_memtable_size: 64 * 1024, // 64KB
            ..Default::default()
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
                let key = format!("key_{:05}", i);
                for update in 0..UPDATES_PER_KEY {
                    let value = format!("value_{:05}_update_{}", i, update);

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
            let l0_size = tree.core.levels.read().unwrap().levels.get_levels()[0]
                .tables
                .len();
            assert!(
                l0_size > 0,
                "Expected SSTables in L0 before closing, got {}",
                l0_size
            );

            // Tree will be dropped here, closing the store
        }

        // Step 2: Reopen the store and verify data
        {
            let tree = Tree::new(opts.clone()).unwrap();

            // Verify L0 has tables after reopening
            let l0_size = tree.core.levels.read().unwrap().levels.get_levels()[0]
                .tables
                .len();
            assert!(
                l0_size > 0,
                "Expected SSTables in L0 after reopening, got {}",
                l0_size
            );

            // Verify all keys have their final values
            for (key, expected_value) in expected_values.iter() {
                let txn = tree.begin().unwrap();
                let result = txn.get(key.as_bytes()).unwrap();

                assert!(
                    result.is_some(),
                    "Key '{}' not found after reopening store",
                    key
                );

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

        let opts = Arc::new(Options {
            path: path.clone(),
            max_memtable_size: 64 * 1024, // 64KB
            ..Default::default()
        });

        // Create initial data
        let tree = Tree::new(opts.clone()).unwrap();

        // Insert some test data
        for i in 0..10 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}", i);

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
        assert!(checkpoint_dir.join("levels").exists());
        assert!(checkpoint_dir.join("CHECKPOINT_METADATA").exists());

        // Insert more data after checkpoint
        for i in 10..15 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{:03}", i);

            let mut txn = tree.begin().unwrap();
            txn.set(key.as_bytes(), value.as_bytes()).unwrap();
            txn.commit().await.unwrap();
        }

        // Force flush to ensure the new data is also on disk
        tree.flush().unwrap();

        // Verify all data exists before restore
        for i in 0..15 {
            let key = format!("key_{:03}", i);
            let txn = tree.begin().unwrap();
            let result = txn.get(key.as_bytes()).unwrap();
            assert!(
                result.is_some(),
                "Key '{}' should exist before restore",
                key
            );
        }

        // Restore from checkpoint
        tree.restore_from_checkpoint(&checkpoint_dir).unwrap();

        // Verify data is restored to checkpoint state (keys 0-9 exist, 10-14 don't)
        for i in 0..10 {
            let key = format!("key_{:03}", i);
            let expected_value = format!("value_{:03}", i);

            let txn = tree.begin().unwrap();
            let result = txn.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key '{}' should exist after restore", key);
            assert_eq!(result.unwrap(), expected_value.as_bytes().into());
        }

        // Verify the newer keys don't exist after restore
        for i in 10..15 {
            let key = format!("key_{:03}", i);

            let txn = tree.begin().unwrap();
            let result = txn.get(key.as_bytes()).unwrap();
            assert!(
                result.is_none(),
                "Key '{}' should not exist after restore",
                key
            );
        }
    }

    #[tokio::test]
    async fn test_checkpoint_restore_discards_pending_writes() {
        let temp_dir = create_temp_directory();
        let path = temp_dir.path().to_path_buf();

        let opts = Arc::new(Options {
            path: path.clone(),
            max_memtable_size: 64 * 1024, // 64KB
            ..Default::default()
        });

        // Create initial data and checkpoint
        let tree = Tree::new(opts.clone()).unwrap();

        // Insert initial data
        for i in 0..5 {
            let key = format!("key_{:03}", i);
            let value = format!("checkpoint_value_{:03}", i);

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
            let key = format!("key_{:03}", i);
            let value = format!("pending_value_{:03}", i); // Different values

            let mut txn = tree.begin().unwrap();
            txn.set(key.as_bytes(), value.as_bytes()).unwrap();
            txn.commit().await.unwrap();
        }

        // Verify the pending writes are in memory but not yet flushed
        for i in 0..5 {
            let key = format!("key_{:03}", i);
            let txn = tree.begin().unwrap();
            let result = txn.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(
                result,
                format!("pending_value_{:03}", i).as_bytes().into(),
                "Expected pending value before restore"
            );
        }

        // Restore from checkpoint - this should discard pending writes
        tree.restore_from_checkpoint(&checkpoint_dir).unwrap();

        // Verify data is restored to checkpoint state (NOT the pending writes)
        for i in 0..5 {
            let key = format!("key_{:03}", i);
            let expected_value = format!("checkpoint_value_{:03}", i);

            let txn = tree.begin().unwrap();
            let result = txn.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(
                result,
                expected_value.as_bytes().into(),
                "Key '{}' should have checkpoint value '{}', not pending value",
                key,
                expected_value
            );
        }
    }

    #[tokio::test]
    async fn test_simple_range_seek() {
        let temp_dir = create_temp_directory();
        let path = temp_dir.path().to_path_buf();

        let opts = Arc::new(Options {
            path: path.clone(),
            max_memtable_size: 64 * 1024, // 64KB to trigger flush
            ..Default::default()
        });

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
        let range_before_flush: Vec<_> = txn.range(b"a", b"c", None).unwrap().collect::<Vec<_>>();

        // Should return keys "a", "b", "c" (inclusive range)
        assert_eq!(
            range_before_flush.len(),
            3,
            "Range scan before flush should return 3 items"
        );

        // Verify the keys and values are correct
        let expected_before = [("a", "a"), ("b", "b"), ("c", "c")];
        for (idx, (key, value)) in range_before_flush.iter().enumerate() {
            let key_str = std::str::from_utf8(key.as_ref()).unwrap();
            let value_str = std::str::from_utf8(value.as_ref()).unwrap();
            assert_eq!(
                key_str, expected_before[idx].0,
                "Key mismatch at index {} before flush",
                idx
            );
            assert_eq!(
                value_str, expected_before[idx].1,
                "Value mismatch at index {} before flush",
                idx
            );
        }

        // Force flush to ensure data is on disk
        tree.flush().unwrap();

        // Test range scan AFTER flushing (should work from SSTables)
        let txn = tree.begin().unwrap();
        let range_after_flush: Vec<_> = txn.range(b"a", b"c", None).unwrap().collect::<Vec<_>>();

        // Should return the same keys after flush
        assert_eq!(
            range_after_flush.len(),
            3,
            "Range scan after flush should return 3 items"
        );

        // Verify the keys and values are still correct after flush
        let expected_after = [("a", "a"), ("b", "b"), ("c", "c")];
        for (idx, (key, value)) in range_after_flush.iter().enumerate() {
            let key_str = std::str::from_utf8(key.as_ref()).unwrap();
            let value_str = std::str::from_utf8(value.as_ref()).unwrap();
            assert_eq!(
                key_str, expected_after[idx].0,
                "Key mismatch at index {} after flush",
                idx
            );
            assert_eq!(
                value_str, expected_after[idx].1,
                "Value mismatch at index {} after flush",
                idx
            );
        }

        // Test individual gets
        for key in ["a", "b", "c"].iter() {
            let txn = tree.begin().unwrap();
            let result = txn.get(key.as_bytes()).unwrap();

            assert!(result.is_some(), "Key '{}' should be found", key);
            let value = result.unwrap();
            let value_str = std::str::from_utf8(value.as_ref()).unwrap();
            assert_eq!(
                value_str, *key,
                "Value for key '{}' should match the key",
                key
            );
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

        let opts = Arc::new(Options {
            path: path.clone(),
            max_memtable_size: 64 * 1024, // 64KB to trigger multiple flushes
            ..Default::default()
        });

        let tree = Tree::new(opts.clone()).unwrap();

        // Insert 10,000 items with zero-padded keys for consistent ordering
        const TOTAL_ITEMS: usize = 10_000;
        let mut expected_items = Vec::with_capacity(TOTAL_ITEMS);

        for i in 0..TOTAL_ITEMS {
            let key = format!("key_{:06}", i);
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
        let range_result: Vec<_> = txn.range(start_key, end_key, None).unwrap().collect();

        assert_eq!(
            range_result.len(),
            TOTAL_ITEMS,
            "Full range scan should return all {} items",
            TOTAL_ITEMS
        );

        // Verify each item is correct and in order
        for (idx, (key, value)) in range_result.iter().enumerate() {
            let key_str = std::str::from_utf8(key.as_ref()).unwrap();
            let value_str = std::str::from_utf8(value.as_ref()).unwrap();

            let expected_key = &expected_items[idx].0;
            let expected_value = &expected_items[idx].1;

            assert_eq!(
                key_str, expected_key,
                "Key mismatch at index {}: expected '{}', found '{}'",
                idx, expected_key, key_str
            );
            assert_eq!(
                value_str, expected_value,
                "Value mismatch at index {}: expected '{}', found '{}'",
                idx, expected_value, value_str
            );
        }

        // Test 2: Partial range scan - first 100 items
        let partial_start = "key_000000".as_bytes();
        let partial_end = "key_000099".as_bytes();

        let txn = tree.begin().unwrap();
        let partial_result: Vec<_> = txn
            .range(partial_start, partial_end, None)
            .unwrap()
            .collect();

        assert_eq!(
            partial_result.len(),
            100,
            "Partial range scan should return 100 items"
        );

        // Verify each item in partial range
        for (idx, (key, value)) in partial_result.iter().enumerate() {
            let key_str = std::str::from_utf8(key.as_ref()).unwrap();
            let value_str = std::str::from_utf8(value.as_ref()).unwrap();

            let expected_key = &expected_items[idx].0;
            let expected_value = &expected_items[idx].1;

            assert_eq!(
                key_str, expected_key,
                "Partial range key mismatch at index {}: expected '{}', found '{}'",
                idx, expected_key, key_str
            );
            assert_eq!(
                value_str, expected_value,
                "Partial range value mismatch at index {}: expected '{}', found '{}'",
                idx, expected_value, value_str
            );
        }

        // Test 3: Middle range scan - items 5000-5099
        let middle_start = "key_005000".as_bytes();
        let middle_end = "key_005099".as_bytes();

        let txn = tree.begin().unwrap();
        let middle_result: Vec<_> = txn.range(middle_start, middle_end, None).unwrap().collect();

        assert_eq!(
            middle_result.len(),
            100,
            "Middle range scan should return 100 items"
        );

        // Verify each item in middle range
        for (idx, (key, value)) in middle_result.iter().enumerate() {
            let key_str = std::str::from_utf8(key.as_ref()).unwrap();
            let value_str = std::str::from_utf8(value.as_ref()).unwrap();

            let expected_idx = 5000 + idx;
            let expected_key = &expected_items[expected_idx].0;
            let expected_value = &expected_items[expected_idx].1;

            assert_eq!(
                key_str, expected_key,
                "Middle range key mismatch at index {}: expected '{}', found '{}'",
                idx, expected_key, key_str
            );
            assert_eq!(
                value_str, expected_value,
                "Middle range value mismatch at index {}: expected '{}', found '{}'",
                idx, expected_value, value_str
            );
        }

        // Test 4: End range scan - last 100 items
        let end_start = "key_009900".as_bytes();
        let end_end = "key_009999".as_bytes();

        let txn = tree.begin().unwrap();
        let end_result: Vec<_> = txn.range(end_start, end_end, None).unwrap().collect();

        assert_eq!(
            end_result.len(),
            100,
            "End range scan should return 100 items"
        );

        // Verify each item in end range
        for (idx, (key, value)) in end_result.iter().enumerate() {
            let key_str = std::str::from_utf8(key.as_ref()).unwrap();
            let value_str = std::str::from_utf8(value.as_ref()).unwrap();

            let expected_idx = 9900 + idx;
            let expected_key = &expected_items[expected_idx].0;
            let expected_value = &expected_items[expected_idx].1;

            assert_eq!(
                key_str, expected_key,
                "End range key mismatch at index {}: expected '{}', found '{}'",
                idx, expected_key, key_str
            );
            assert_eq!(
                value_str, expected_value,
                "End range value mismatch at index {}: expected '{}', found '{}'",
                idx, expected_value, value_str
            );
        }

        // Test 5: Single item range scan
        let single_start = "key_004567".as_bytes();
        let single_end = "key_004567".as_bytes();

        let txn = tree.begin().unwrap();
        let single_result: Vec<_> = txn.range(single_start, single_end, None).unwrap().collect();

        assert_eq!(
            single_result.len(),
            1,
            "Single item range scan should return 1 item"
        );

        let (key, value) = &single_result[0];
        let key_str = std::str::from_utf8(key.as_ref()).unwrap();
        let value_str = std::str::from_utf8(value.as_ref()).unwrap();

        assert_eq!(key_str, "key_004567");
        assert_eq!(value_str, "value_004567_content_data_9134");

        // Test 6: Empty range scan (non-existent range)
        let empty_start = "key_999999".as_bytes();
        let empty_end = "key_888888".as_bytes(); // end < start, should be empty

        let txn = tree.begin().unwrap();
        let empty_result: Vec<_> = txn.range(empty_start, empty_end, None).unwrap().collect();

        assert_eq!(
            empty_result.len(),
            0,
            "Empty range scan should return 0 items"
        );
    }

    #[tokio::test]
    async fn test_range_skip_take() {
        let temp_dir = create_temp_directory();
        let path = temp_dir.path().to_path_buf();

        let opts = Arc::new(Options {
            path: path.clone(),
            max_memtable_size: 64 * 1024,
            ..Default::default()
        });

        let tree = Tree::new(opts.clone()).unwrap();

        const TOTAL_ITEMS: usize = 10_000;
        let mut expected_items = Vec::with_capacity(TOTAL_ITEMS);

        for i in 0..TOTAL_ITEMS {
            let key = format!("key_{:06}", i);
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
            .collect();

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
                "Key mismatch at index {}: expected '{}', found '{}'",
                idx, expected_key, key_str
            );
            assert_eq!(
                value_str, expected_value,
                "Value mismatch at index {}: expected '{}', found '{}'",
                idx, expected_value, value_str
            );
        }
    }

    #[tokio::test]
    async fn test_range_skip_take_alphabetical() {
        let temp_dir = create_temp_directory();
        let path = temp_dir.path().to_path_buf();

        let opts = Arc::new(Options {
            path: path.clone(),
            max_memtable_size: 64 * 1024,
            ..Default::default()
        });

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
            .collect();

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
                "Key mismatch at index {}: expected '{}', found '{}'",
                idx, expected_key, key_str
            );
            assert_eq!(
                value_str, expected_value,
                "Value mismatch at index {}: expected '{}', found '{}'",
                idx, expected_value, value_str
            );
        }
    }

    #[tokio::test]
    async fn test_range_limit_functionality() {
        let temp_dir = create_temp_directory();
        let path = temp_dir.path().to_path_buf();

        let opts = Arc::new(Options {
            path: path.clone(),
            max_memtable_size: 64 * 1024,
            ..Default::default()
        });

        let tree = Tree::new(opts.clone()).unwrap();

        const TOTAL_ITEMS: usize = 10_000;
        let mut expected_items = Vec::with_capacity(TOTAL_ITEMS);

        for i in 0..TOTAL_ITEMS {
            let key = format!("key_{:06}", i);
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
        let limited_result: Vec<_> = txn.range(&beg, &end, Some(100)).unwrap().collect();

        assert_eq!(
            limited_result.len(),
            100,
            "Range scan with limit 100 should return exactly 100 items"
        );

        for (idx, (key, value)) in limited_result.iter().enumerate() {
            let key_str = std::str::from_utf8(key.as_ref()).unwrap();
            let value_str = std::str::from_utf8(value.as_ref()).unwrap();

            let expected_key = &expected_items[idx].0;
            let expected_value = &expected_items[idx].1;

            assert_eq!(
                key_str, expected_key,
                "Key mismatch at index {} with limit 100: expected '{}', found '{}'",
                idx, expected_key, key_str
            );
            assert_eq!(
                value_str, expected_value,
                "Value mismatch at index {} with limit 100: expected '{}', found '{}'",
                idx, expected_value, value_str
            );
        }

        let txn = tree.begin().unwrap();
        let limited_result_1000: Vec<_> = txn.range(&beg, &end, Some(1000)).unwrap().collect();

        assert_eq!(
            limited_result_1000.len(),
            1000,
            "Range scan with limit 1000 should return exactly 1000 items"
        );

        for (idx, (key, value)) in limited_result_1000.iter().enumerate() {
            let key_str = std::str::from_utf8(key.as_ref()).unwrap();
            let value_str = std::str::from_utf8(value.as_ref()).unwrap();

            let expected_key = &expected_items[idx].0;
            let expected_value = &expected_items[idx].1;

            assert_eq!(
                key_str, expected_key,
                "Key mismatch at index {} with limit 1000: expected '{}', found '{}'",
                idx, expected_key, key_str
            );
            assert_eq!(
                value_str, expected_value,
                "Value mismatch at index {} with limit 1000: expected '{}', found '{}'",
                idx, expected_value, value_str
            );
        }

        let txn = tree.begin().unwrap();
        let limited_result_large: Vec<_> = txn.range(&beg, &end, Some(15000)).unwrap().collect();

        assert_eq!(
            limited_result_large.len(),
            TOTAL_ITEMS,
            "Range scan with limit 15000 should return all {} items",
            TOTAL_ITEMS
        );

        let txn = tree.begin().unwrap();
        let unlimited_result: Vec<_> = txn.range(&beg, &end, None).unwrap().collect();

        assert_eq!(
            unlimited_result.len(),
            TOTAL_ITEMS,
            "Range scan with no limit should return all {} items",
            TOTAL_ITEMS
        );

        let txn = tree.begin().unwrap();
        let single_result: Vec<_> = txn.range(&beg, &end, Some(1)).unwrap().collect();

        assert_eq!(
            single_result.len(),
            1,
            "Range scan with limit 1 should return exactly 1 item"
        );

        let (key, value) = &single_result[0];
        let key_str = std::str::from_utf8(key.as_ref()).unwrap();
        let value_str = std::str::from_utf8(value.as_ref()).unwrap();

        assert_eq!(key_str, "key_000000");
        assert_eq!(value_str, "value_000000_content_data_0");

        let txn = tree.begin().unwrap();
        let zero_result: Vec<_> = txn.range(&beg, &end, Some(0)).unwrap().collect();

        assert_eq!(
            zero_result.len(),
            0,
            "Range scan with limit 0 should return no items"
        );
    }

    #[tokio::test]
    async fn test_range_limit_with_skip_take() {
        let temp_dir = create_temp_directory();
        let path = temp_dir.path().to_path_buf();

        let opts = Arc::new(Options {
            path: path.clone(),
            max_memtable_size: 64 * 1024,
            ..Default::default()
        });

        let tree = Tree::new(opts.clone()).unwrap();

        const TOTAL_ITEMS: usize = 20_000;
        let mut expected_items = Vec::with_capacity(TOTAL_ITEMS);

        for i in 0..TOTAL_ITEMS {
            let key = format!("key_{:06}", i);
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
            .collect();

        assert_eq!(
            unlimited_range_result.len(),
            100,
            "Range scan with no limit followed by skip(5000).take(100) should return 100 items"
        );

        for (idx, (key, value)) in unlimited_range_result.iter().enumerate() {
            let key_str = std::str::from_utf8(key.as_ref()).unwrap();
            let value_str = std::str::from_utf8(value.as_ref()).unwrap();

            let expected_idx = 5000 + idx;
            let expected_key = &expected_items[expected_idx].0;
            let expected_value = &expected_items[expected_idx].1;

            assert_eq!(
                key_str, expected_key,
                "Key mismatch at index {} with unlimited range: expected '{}', found '{}'",
                idx, expected_key, key_str
            );
            assert_eq!(
                value_str, expected_value,
                "Value mismatch at index {} with unlimited range: expected '{}', found '{}'",
                idx, expected_value, value_str
            );
        }

        let txn = tree.begin().unwrap();
        let large_limit_result: Vec<_> = txn
            .range(&beg, &end, Some(10000))
            .unwrap()
            .skip(5000)
            .take(100)
            .collect();

        assert_eq!(
            large_limit_result.len(),
            100,
            "Range scan with limit 10000 followed by skip(5000).take(100) should return 100 items"
        );

        for (idx, (key, value)) in large_limit_result.iter().enumerate() {
            let key_str = std::str::from_utf8(key.as_ref()).unwrap();
            let value_str = std::str::from_utf8(value.as_ref()).unwrap();

            let expected_idx = 5000 + idx;
            let expected_key = &expected_items[expected_idx].0;
            let expected_value = &expected_items[expected_idx].1;

            assert_eq!(
                key_str, expected_key,
                "Key mismatch at index {} with limit 10000: expected '{}', found '{}'",
                idx, expected_key, key_str
            );
            assert_eq!(
                value_str, expected_value,
                "Value mismatch at index {} with limit 10000: expected '{}', found '{}'",
                idx, expected_value, value_str
            );
        }

        // Case: limit larger than total items, but skip is large
        let txn = tree.begin().unwrap();
        let res: Vec<_> = txn
            .range(&beg, &end, Some(5050))
            .unwrap()
            .skip(5000)
            .take(100)
            .collect();

        assert_eq!(
            res.len(),
            50,
            "Range scan with limit 5050 followed by skip(5000).take(100) should return 50 items"
        );

        for (idx, (key, value)) in res.iter().enumerate() {
            let key_str = std::str::from_utf8(key.as_ref()).unwrap();
            let value_str = std::str::from_utf8(value.as_ref()).unwrap();

            let expected_idx = 5000 + idx;
            let expected_key = &expected_items[expected_idx].0;
            let expected_value = &expected_items[expected_idx].1;

            assert_eq!(
                key_str, expected_key,
                "Key mismatch at index {} with limit 5050: expected '{}', found '{}'",
                idx, expected_key, key_str
            );
            assert_eq!(
                value_str, expected_value,
                "Value mismatch at index {} with limit 5050: expected '{}', found '{}'",
                idx, expected_value, value_str
            );
        }
    }

    #[tokio::test]
    async fn test_vlog() {
        let temp_dir = create_temp_directory();
        let path = temp_dir.path().to_path_buf();

        let opts = Arc::new(Options {
            path: path.clone(),
            vlog_value_threshold: 100, // Store values >= 100 bytes in VLog
            vlog_max_file_size: 1024 * 1024, // 1MB files
            max_memtable_size: 64 * 1024, // 64KB
            ..Default::default()
        });

        let tree = Tree::new(opts).unwrap();

        // Test 1: Small values should be stored inline
        let small_key = "small_key";
        let small_value = "small_value"; // < 100 bytes

        let mut txn = tree.begin().unwrap();
        txn.set(small_key.as_bytes(), small_value.as_bytes())
            .unwrap();
        txn.commit().await.unwrap();

        // Test 2: Large values should be stored in VLog
        let large_key = "large_key";
        let large_value = "X".repeat(200); // >= 100 bytes

        let mut txn = tree.begin().unwrap();
        txn.set(large_key.as_bytes(), large_value.as_bytes())
            .unwrap();
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

        let opts = Arc::new(Options {
            path: path.clone(),
            vlog_value_threshold: 50,
            vlog_max_file_size: 512 * 1024,
            max_memtable_size: 32 * 1024,
            ..Default::default()
        });

        let tree = Tree::new(opts).unwrap();

        let mut expected_values = Vec::new();

        for i in 0..1000 {
            let key = format!("key_{:04}", i);
            let value = if i % 2 == 0 {
                format!("small_value_{}", i)
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
                            "Reader {} failed to get correct value for key {}",
                            reader_id,
                            key
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
    async fn test_vlog_garbage_collection_integration() {
        let temp_dir = create_temp_directory();
        let path = temp_dir.path().to_path_buf();

        let opts = Arc::new(Options {
            path: path.clone(),
            vlog_value_threshold: 50,
            vlog_max_file_size: 1024, // Very small files to force rotation
            max_memtable_size: 64 * 1024,
            ..Default::default()
        });

        let tree = Tree::new(opts).unwrap();

        // Insert many large values to create multiple VLog files
        let num_keys = 100;
        for i in 0..num_keys {
            let key = format!("key_{:03}", i);
            let value = format!("initial_value_{}_with_padding_{}", i, "X".repeat(100));

            let mut txn = tree.begin().unwrap();
            txn.set(key.as_bytes(), value.as_bytes()).unwrap();
            txn.commit().await.unwrap();
        }

        // Force flush to create SSTables
        tree.flush().unwrap();

        // Update half the keys to create stale values in VLog
        for i in 0..num_keys / 2 {
            let key = format!("key_{:03}", i);
            let value = format!("updated_value_{}_with_padding_{}", i, "Y".repeat(120));

            let mut txn = tree.begin().unwrap();
            txn.set(key.as_bytes(), value.as_bytes()).unwrap();
            txn.commit().await.unwrap();
        }

        // Force another flush
        tree.flush().unwrap();

        // Trigger garbage collection
        let _compacted_files = tree.garbage_collect_vlog().await.unwrap();

        // Verify all data is still accessible after GC
        for i in 0..num_keys {
            let key = format!("key_{:03}", i);
            let expected_value = if i < num_keys / 2 {
                format!("updated_value_{}_with_padding_{}", i, "Y".repeat(120))
            } else {
                format!("initial_value_{}_with_padding_{}", i, "X".repeat(100))
            };

            let txn = tree.begin().unwrap();
            let retrieved = txn.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(
                retrieved,
                expected_value.as_bytes().into(),
                "Key {} has incorrect value after GC",
                key
            );
        }
    }

    #[tokio::test]
    async fn test_vlog_file_rotation() {
        let temp_dir = create_temp_directory();
        let path = temp_dir.path().to_path_buf();

        let opts = Arc::new(Options {
            path: path.clone(),
            vlog_value_threshold: 50,
            vlog_max_file_size: 2048, // 2KB files to force frequent rotation
            max_memtable_size: 64 * 1024,
            ..Default::default()
        });

        let tree = Tree::new(opts).unwrap();

        // Insert enough data to force multiple file rotations
        let mut keys_and_values = Vec::new();
        for i in 0..50 {
            let key = format!("rotation_key_{:03}", i);
            let value = format!(
                "rotation_value_{}_with_lots_of_padding_{}",
                i,
                "Z".repeat(200)
            );
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
                if name.starts_with("vlog_") && name.ends_with(".log") {
                    Some(name)
                } else {
                    None
                }
            })
            .collect();

        assert!(
            vlog_files.len() > 1,
            "Expected multiple VLog files due to rotation"
        );

        // Verify all data is still accessible across multiple files
        for (key, expected_value) in keys_and_values {
            let txn = tree.begin().unwrap();
            let retrieved = txn.get(key.as_bytes()).unwrap().unwrap();
            assert_eq!(
                retrieved,
                expected_value.as_bytes().into(),
                "Key {} has incorrect value across file rotation",
                key
            );
        }
    }

    #[tokio::test]
    async fn test_vlog_gc() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let opts = Arc::new(Options {
            path: temp_dir.path().to_path_buf(),
            vlog_value_threshold: 50, // Low threshold to force VLog usage
            max_memtable_size: 512,   // Very small to force frequent flushes
            ..Default::default()
        });

        let tree = Tree::new(opts).unwrap();

        // Insert key with large value that will go to VLog
        let large_value = vec![42u8; 200];
        let mut txn = tree.begin().unwrap();
        txn.set(b"test_key", &large_value).unwrap();
        txn.commit().await.unwrap();
        tree.flush().unwrap(); // Force flush

        // Force enough writes to ensure compaction happens
        for i in 0..10 {
            let mut txn = tree.begin().unwrap();
            txn.set(format!("key_{}", i).as_bytes(), &large_value)
                .unwrap();
            txn.commit().await.unwrap();
            tree.flush().unwrap();
        }

        // Check that VLog files exist
        let all_stats_before = tree.get_all_vlog_stats();
        assert!(!all_stats_before.is_empty(), "Should have VLog files");

        // Now delete the key - this creates a tombstone
        let mut txn = tree.begin().unwrap();
        txn.delete(b"test_key").unwrap();
        txn.commit().await.unwrap();
        tree.flush().unwrap(); // Force flush

        // Force more compactions to push data to bottom levels
        for i in 10..20 {
            let mut txn = tree.begin().unwrap();
            txn.set(format!("key_{}", i).as_bytes(), &large_value)
                .unwrap();
            txn.commit().await.unwrap();
            tree.flush().unwrap();
        }

        // Force GC
        tree.garbage_collect_vlog().await.unwrap();

        // Verify that the key is actually deleted from LSM perspective
        let txn = tree.begin().unwrap();
        assert!(
            txn.get(b"test_key").unwrap().is_none(),
            "Key should be deleted"
        );
    }

    #[tokio::test]
    async fn test_vlog_gc_discard_statistics() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let opts = Arc::new(Options {
            path: temp_dir.path().to_path_buf(),
            vlog_value_threshold: 50,
            vlog_max_file_size: 600, // Small files to force multiple files
            max_memtable_size: 512,
            ..Default::default()
        });

        let tree = Tree::new(opts).unwrap();
        let large_value = vec![1u8; 200];

        // Insert multiple keys to create multiple VLog files
        for i in 0..15 {
            let key = format!("key_{:03}", i);
            let mut txn = tree.begin().unwrap();
            txn.set(key.as_bytes(), &large_value).unwrap();
            txn.commit().await.unwrap();
        }
        tree.flush().unwrap();

        // Get initial VLog stats
        let initial_stats = tree.get_all_vlog_stats();
        assert!(!initial_stats.is_empty(), "Should have VLog files");
        assert!(
            initial_stats.len() > 1,
            "Should have multiple VLog files for meaningful GC test"
        );

        // Verify all initial entries are accessible
        for i in 0..15 {
            let key = format!("key_{:03}", i);
            let txn = tree.begin().unwrap();
            let result = txn.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "Initial key {} should exist", key);
            assert_eq!(result.unwrap(), large_value.clone().into());
        }

        // Update ALL keys to create stale entries in initial VLog files
        for i in 0..15 {
            let key = format!("key_{:03}", i);
            let updated_value = vec![2u8; 250]; // Different size to create new VLog entries
            let mut txn = tree.begin().unwrap();
            txn.set(key.as_bytes(), &updated_value).unwrap();
            txn.commit().await.unwrap();
        }
        tree.flush().unwrap();

        // Verify all updates are accessible
        for i in 0..15 {
            let key = format!("key_{:03}", i);
            let txn = tree.begin().unwrap();
            let result = txn.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "Updated key {} should exist", key);
            assert_eq!(
                result.unwrap(),
                vec![2u8; 250].into(),
                "Key {} should have updated value",
                key
            );
        }

        // Get stats after updates - should have more files now
        let stats_after_updates = tree.get_all_vlog_stats();
        assert!(
            stats_after_updates.len() > initial_stats.len(),
            "Should have more VLog files after updates ({} vs {})",
            stats_after_updates.len(),
            initial_stats.len()
        );

        // Now ALL the initial files should contain only stale entries
        // Mark ALL initial files as having high discard ratios since ALL entries were updated
        let mut discard_stats = std::collections::HashMap::new();

        for (file_id, total_size, _current_discard, _ratio) in &initial_stats {
            if *total_size > 0 {
                // Mark 100% of initial files as discardable since ALL entries were updated
                let guaranteed_discard = *total_size as i64;
                discard_stats.insert(*file_id, guaranteed_discard);
            }
        }

        assert!(
            !discard_stats.is_empty(),
            "Should have discard stats to apply"
        );
        assert_eq!(
            discard_stats.len(),
            initial_stats.len(),
            "Should mark all initial files with discard stats"
        );

        tree.core.vlog.update_discard_stats(discard_stats);

        // Trigger garbage collection with staleness detection
        let compacted_files = tree.garbage_collect_vlog().await.unwrap();

        // Since all initial entries are now stale (100% discard ratio), GC should compact files
        assert!(
            !compacted_files.is_empty(),
            "VLog GC should have compacted at least one file with real staleness detection"
        );

        let stats_after_gc = tree.get_all_vlog_stats();
        assert!(
            stats_after_updates.len() > stats_after_gc.len(),
            "Should have fewer VLog files after GC ({} vs {})",
            stats_after_updates.len(),
            stats_after_gc.len()
        );

        // Verify data integrity after GC
        for i in 0..15 {
            let key = format!("key_{:03}", i);
            let txn = tree.begin().unwrap();
            let result = txn.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should still exist after GC", key);

            // All keys should have the updated value (not the original)
            let expected_value = vec![2u8; 250];
            assert_eq!(
                result.unwrap(),
                expected_value.into(),
                "Key {} should have updated value after GC",
                key
            );
        }

        // Verify that some initial files were compacted (they contained 100% stale data)
        let initial_file_ids: std::collections::HashSet<u64> =
            initial_stats.iter().map(|(id, _, _, _)| *id).collect();
        let compacted_initial_files: Vec<_> = compacted_files
            .iter()
            .filter(|id| initial_file_ids.contains(id))
            .collect();
        assert!(
            !compacted_initial_files.is_empty(),
            "At least one initial file should have been compacted due to 100% staleness"
        );
    }

    #[tokio::test]
    async fn test_vlog_gc_staleness_detection() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let opts = Arc::new(Options {
            path: temp_dir.path().to_path_buf(),
            vlog_value_threshold: 50,
            vlog_max_file_size: 2048, // Larger files for this test
            max_memtable_size: 1024,
            ..Default::default()
        });

        let tree = Tree::new(opts).unwrap();

        // Test scenario: Create entries, update some, delete others
        let large_value_v1 = vec![1u8; 200];
        let large_value_v2 = vec![2u8; 220];

        // Insert initial values
        for i in 0..15 {
            let key = format!("key_{:03}", i);
            let mut txn = tree.begin().unwrap();
            txn.set(key.as_bytes(), &large_value_v1).unwrap();
            txn.commit().await.unwrap();
        }
        tree.flush().unwrap();

        let stats_after_initial = tree.get_all_vlog_stats();
        assert!(!stats_after_initial.is_empty(), "Should have VLog files");

        // Update keys 0-4 (creates new VLog entries, making old ones stale)
        for i in 0..5 {
            let key = format!("key_{:03}", i);
            let mut txn = tree.begin().unwrap();
            txn.set(key.as_bytes(), &large_value_v2).unwrap();
            txn.commit().await.unwrap();
        }

        // Delete keys 5-9 (makes VLog entries stale)
        for i in 5..10 {
            let key = format!("key_{:03}", i);
            let mut txn = tree.begin().unwrap();
            txn.delete(key.as_bytes()).unwrap();
            txn.commit().await.unwrap();
        }

        // Keys 10-14 remain unchanged (live entries)

        tree.flush().unwrap();

        // Force GC on specific files to test staleness detection
        for (file_id, _total_size, _discard_bytes, _ratio) in &stats_after_initial {
            // Force GC on this file regardless of discard ratio
            let was_compacted = tree.garbage_collect_vlog().await.unwrap().contains(file_id);
            if was_compacted {
                println!("Successfully compacted VLog file {}", file_id);
            }
        }

        // Verify the final state
        for i in 0..15 {
            let key = format!("key_{:03}", i);
            let txn = tree.begin().unwrap();
            let result = txn.get(key.as_bytes()).unwrap();

            if i < 5 {
                // Updated keys should have new values
                assert!(result.is_some(), "Updated key {} should exist", key);
                assert_eq!(result.unwrap(), large_value_v2.clone().into());
            } else if i < 10 {
                // Deleted keys should not exist
                assert!(result.is_none(), "Deleted key {} should not exist", key);
            } else {
                // Unchanged keys should have original values
                assert!(result.is_some(), "Unchanged key {} should exist", key);
                assert_eq!(result.unwrap(), large_value_v1.clone().into());
            }
        }
    }

    #[tokio::test]
    async fn test_vlog_gc_multiple_files() {
        // Test GC behavior with multiple VLog files
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let opts = Arc::new(Options {
            path: temp_dir.path().to_path_buf(),
            vlog_value_threshold: 50,
            vlog_max_file_size: 512, // Very small to force many files
            max_memtable_size: 256,
            ..Default::default()
        });

        let tree = Tree::new(opts).unwrap();
        let large_value = vec![1u8; 150];

        // Insert enough data to create multiple VLog files
        for i in 0..50 {
            let key = format!("key_{:04}", i);
            let mut txn = tree.begin().unwrap();
            txn.set(key.as_bytes(), &large_value).unwrap();
            txn.commit().await.unwrap();
        }
        tree.flush().unwrap();

        let initial_stats = tree.get_all_vlog_stats();
        assert!(initial_stats.len() > 1, "Should have multiple VLog files");
        println!("Created {} VLog files", initial_stats.len());

        // Update some keys to create stale entries in early files
        for i in 0..25 {
            let key = format!("key_{:04}", i);
            let updated_value = vec![2u8; 180];
            let mut txn = tree.begin().unwrap();
            txn.set(key.as_bytes(), &updated_value).unwrap();
            txn.commit().await.unwrap();
        }
        tree.flush().unwrap();

        // Simulate discard statistics for selective GC
        let mut discard_updates = std::collections::HashMap::new();
        for (file_id, total_size, _discard_bytes, _ratio) in &initial_stats {
            if *file_id % 2 == 0 {
                // Mark even-numbered files as having high discard ratio
                discard_updates.insert(*file_id, (*total_size as f64 * 0.8) as i64);
            } else {
                // Odd-numbered files have low discard ratio
                discard_updates.insert(*file_id, (*total_size as f64 * 0.1) as i64);
            }
        }
        tree.core.vlog.update_discard_stats(discard_updates);

        // Run GC multiple times
        for round in 0..3 {
            let compacted_files = tree.garbage_collect_vlog().await.unwrap();
            println!("GC round {}: compacted {:?}", round, compacted_files);
        }

        let final_stats = tree.get_all_vlog_stats();
        println!("Final VLog stats: {:?}", final_stats);

        // Verify all data integrity
        for i in 0..50 {
            let key = format!("key_{:04}", i);
            let txn = tree.begin().unwrap();
            let result = txn.get(key.as_bytes()).unwrap();
            assert!(result.is_some(), "Key {} should exist after GC", key);

            let expected_value = if i < 25 {
                vec![2u8; 180] // Updated value
            } else {
                vec![1u8; 150] // Original value
            };
            assert_eq!(result.unwrap(), expected_value.into());
        }
    }

    #[tokio::test]
    async fn test_vlog_gc_integration_with_compaction() {
        // Test that VLog GC works correctly in coordination with LSM compaction
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let opts = Arc::new(Options {
            path: temp_dir.path().to_path_buf(),
            vlog_value_threshold: 50,
            vlog_max_file_size: 2048,
            max_memtable_size: 512, // Small to trigger LSM compaction
            ..Default::default()
        });

        let tree = Tree::new(opts).unwrap();
        let large_value = vec![1u8; 200];

        // Create a pattern that will trigger both LSM compaction and VLog GC

        // Phase 1: Insert initial data
        for i in 0..30 {
            let key = format!("key_{:03}", i);
            let mut txn = tree.begin().unwrap();
            txn.set(key.as_bytes(), &large_value).unwrap();
            txn.commit().await.unwrap();
        }
        tree.flush().unwrap();

        // Phase 2: Update many keys (creates stale VLog entries)
        for i in 0..20 {
            let key = format!("key_{:03}", i);
            let updated_value = vec![2u8; 250];
            let mut txn = tree.begin().unwrap();
            txn.set(key.as_bytes(), &updated_value).unwrap();
            txn.commit().await.unwrap();
        }
        tree.flush().unwrap();

        // Phase 3: Delete some keys (creates tombstones and more stale VLog entries)
        for i in 15..25 {
            let key = format!("key_{:03}", i);
            let mut txn = tree.begin().unwrap();
            txn.delete(key.as_bytes()).unwrap();
            txn.commit().await.unwrap();
        }
        tree.flush().unwrap();

        // Phase 4: Add more data to trigger compaction
        for i in 30..60 {
            let key = format!("key_{:03}", i);
            let mut txn = tree.begin().unwrap();
            txn.set(key.as_bytes(), &large_value).unwrap();
            txn.commit().await.unwrap();
        }
        tree.flush().unwrap();

        let stats_before = tree.get_all_vlog_stats();
        println!("VLog stats before GC: {:?}", stats_before);

        // Simulate the discard tracking that would happen during LSM compaction
        let mut total_discardable = 0u64;
        let mut discard_updates = std::collections::HashMap::new();

        for (file_id, total_size, _current_discard, _ratio) in &stats_before {
            // Simulate discard amounts based on our update pattern
            let estimated_discard = (*total_size as f64 * 0.6) as i64; // 60% stale
            discard_updates.insert(*file_id, estimated_discard);
            total_discardable += estimated_discard as u64;
        }

        println!("Estimated total discardable bytes: {}", total_discardable);
        tree.core.vlog.update_discard_stats(discard_updates);

        // Trigger VLog GC
        let compacted_files = tree.garbage_collect_vlog().await.unwrap();
        println!("Compacted VLog files: {:?}", compacted_files);

        let stats_after = tree.get_all_vlog_stats();
        println!("VLog stats after GC: {:?}", stats_after);

        // Verify data integrity after GC
        for i in 0..60 {
            let key = format!("key_{:03}", i);
            let txn = tree.begin().unwrap();
            let result = txn.get(key.as_bytes()).unwrap();

            if i < 15 {
                // Keys 0-14: updated values
                assert!(result.is_some(), "Updated key {} should exist", key);
                assert_eq!(result.unwrap(), vec![2u8; 250].into());
            } else if i < 25 {
                // Keys 15-24: deleted
                assert!(result.is_none(), "Deleted key {} should not exist", key);
            } else if i < 30 {
                // Keys 25-29: original values
                assert!(result.is_some(), "Original key {} should exist", key);
                assert_eq!(result.unwrap(), large_value.clone().into());
            } else {
                // Keys 30-59: new values
                assert!(result.is_some(), "New key {} should exist", key);
                assert_eq!(result.unwrap(), large_value.clone().into());
            }
        }

        // Test that VLog GC can be called multiple times safely
        for round in 1..4 {
            let additional_compacted = tree.garbage_collect_vlog().await.unwrap();
            println!("GC round {}: compacted {:?}", round, additional_compacted);

            // Verify data integrity is maintained
            let key = "key_030"; // Test a representative key
            let txn = tree.begin().unwrap();
            let result = txn.get(key.as_bytes()).unwrap();
            assert!(
                result.is_some(),
                "Key should exist after GC round {}",
                round
            );
            assert_eq!(result.unwrap(), large_value.clone().into());
        }
    }

    #[tokio::test]
    async fn test_vlog_gc_tombstone_handling() {
        // Test that VLog GC properly handles tombstones
        let temp_dir = create_temp_directory();
        let path = temp_dir.path().to_path_buf();

        let opts = Arc::new(Options {
            path: path.clone(),
            vlog_value_threshold: 50, // Large values go to VLog
            vlog_max_file_size: 2048, // Small files to test multiple files
            max_memtable_size: 512,
            ..Default::default()
        });

        let tree = Tree::new(opts).unwrap();

        // Insert large values that will go to VLog (like the "dummy!" repeat pattern)
        let big_value = b"dummy!".repeat(8_000); // Much smaller than original for faster test

        // Insert 3 keys with large values
        let mut txn = tree.begin().unwrap();
        txn.set(b"a", &big_value).unwrap();
        txn.set(b"b", &big_value).unwrap();
        txn.set(b"c", &big_value).unwrap();
        txn.commit().await.unwrap();

        // Verify all 3 keys exist
        let txn = tree.begin().unwrap();
        assert_eq!(txn.get(b"a").unwrap().unwrap(), big_value.clone().into());
        assert_eq!(txn.get(b"b").unwrap().unwrap(), big_value.clone().into());
        assert_eq!(txn.get(b"c").unwrap().unwrap(), big_value.clone().into());

        // Flush to create SSTables and VLog files
        tree.flush().unwrap();

        // Verify data still exists after flush
        let txn = tree.begin().unwrap();
        assert_eq!(txn.get(b"a").unwrap().unwrap(), big_value.clone().into());
        assert_eq!(txn.get(b"b").unwrap().unwrap(), big_value.clone().into());
        assert_eq!(txn.get(b"c").unwrap().unwrap(), big_value.clone().into());

        // Delete key "b" (creates tombstone)
        let mut txn = tree.begin().unwrap();
        txn.delete(b"b").unwrap();
        txn.commit().await.unwrap();

        // Verify key "b" is deleted, others remain
        let txn = tree.begin().unwrap();
        assert_eq!(txn.get(b"a").unwrap().unwrap(), big_value.clone().into());
        assert!(txn.get(b"b").unwrap().is_none());
        assert_eq!(txn.get(b"c").unwrap().unwrap(), big_value.clone().into());

        // Flush to persist tombstone
        tree.flush().unwrap();

        // Verify state after tombstone flush
        let txn = tree.begin().unwrap();
        assert_eq!(txn.get(b"a").unwrap().unwrap(), big_value.clone().into());
        assert!(txn.get(b"b").unwrap().is_none());
        assert_eq!(txn.get(b"c").unwrap().unwrap(), big_value.clone().into());

        // Get VLog stats before GC
        let stats_before = tree.get_all_vlog_stats();
        assert!(!stats_before.is_empty(), "Should have VLog files");

        // Simulate discard stats (tombstone for "b" makes some data stale)
        let mut discard_updates = std::collections::HashMap::new();
        for (file_id, total_size, _discard_bytes, _ratio) in &stats_before {
            if *total_size > 0 {
                // Mark approximately 1/3 of data as stale (key "b" was deleted)
                let estimated_discard = (*total_size as f64 * 0.4) as i64;
                discard_updates.insert(*file_id, estimated_discard);
            }
        }
        tree.core.vlog.update_discard_stats(discard_updates);

        // Apply GC with conservative strategy (should handle tombstones correctly)
        let _compacted_files = tree.garbage_collect_vlog().await.unwrap();

        // Verify data after GC
        let txn = tree.begin().unwrap();
        assert_eq!(txn.get(b"a").unwrap().unwrap(), big_value.clone().into());
        assert!(txn.get(b"b").unwrap().is_none()); // Should still be deleted
        assert_eq!(txn.get(b"c").unwrap().unwrap(), big_value.clone().into());
    }

    #[tokio::test]
    async fn test_vlog_gc_space_amplification_pattern() {
        let temp_dir = create_temp_directory();
        let path = temp_dir.path().to_path_buf();

        let opts = Arc::new(Options {
            path: path.clone(),
            vlog_value_threshold: 50,
            vlog_max_file_size: 1024, // Small files for more frequent rotation
            max_memtable_size: 512,
            ..Default::default()
        });

        let tree = Tree::new(opts).unwrap();

        // Simulate the "dummy" pattern from the original tests
        let large_value = b"dummy".repeat(1_000); // Large enough for VLog

        // Phase 1: Insert initial data
        let mut txn = tree.begin().unwrap();
        txn.set(b"a", &large_value).unwrap();
        txn.set(b"b", &large_value).unwrap();
        txn.set(b"c", &large_value).unwrap();
        txn.commit().await.unwrap();

        tree.flush().unwrap();

        // Verify initial state
        let stats_initial = tree.get_all_vlog_stats();
        assert!(
            !stats_initial.is_empty(),
            "Should have VLog files after initial insert"
        );

        let total_initial_size: u64 = stats_initial.iter().map(|(_, size, _, _)| size).sum();
        assert!(total_initial_size > 0, "Should have non-zero VLog data");

        // Phase 2: Update key "a" (creates stale data in VLog)
        let small_value_a = b"a"; // Small value, will be stored inline
        let mut txn = tree.begin().unwrap();
        txn.set(b"a", small_value_a).unwrap();
        txn.commit().await.unwrap();

        // Simulate discard stats tracking
        let mut discard_updates = std::collections::HashMap::new();
        for (file_id, total_size, _discard_bytes, _ratio) in &stats_initial {
            // Estimate that ~1/3 of the data is now stale (key "a" was updated)
            let estimated_discard = (*total_size as f64 * 0.33) as i64;
            discard_updates.insert(*file_id, estimated_discard);
        }
        tree.core.vlog.update_discard_stats(discard_updates);

        let stats_after_a_update = tree.get_all_vlog_stats();
        let space_amp_after_a = calculate_space_amplification(&stats_after_a_update);
        assert!(
            space_amp_after_a > 1.0,
            "Space amplification should increase after update"
        );

        // Phase 3: Update key "b" (more stale data)
        let small_value_b = b"b";
        let mut txn = tree.begin().unwrap();
        txn.set(b"b", small_value_b).unwrap();
        txn.commit().await.unwrap();

        // Update discard stats for more staleness
        let mut discard_updates = std::collections::HashMap::new();
        for (file_id, total_size, _discard_bytes, _ratio) in &stats_initial {
            // Now ~2/3 of the original data is stale
            let estimated_discard = (*total_size as f64 * 0.67) as i64;
            discard_updates.insert(*file_id, estimated_discard);
        }
        tree.core.vlog.update_discard_stats(discard_updates);

        let stats_after_b_update = tree.get_all_vlog_stats();
        let space_amp_after_b = calculate_space_amplification(&stats_after_b_update);
        assert!(
            space_amp_after_b > space_amp_after_a,
            "Space amplification should keep increasing"
        );

        // Phase 4: Update key "c" (most data now stale)
        let small_value_c = b"c";
        let mut txn = tree.begin().unwrap();
        txn.set(b"c", small_value_c).unwrap();
        txn.commit().await.unwrap();

        // Mark all original data as stale
        let mut discard_updates = std::collections::HashMap::new();
        for (file_id, total_size, _discard_bytes, _ratio) in &stats_initial {
            // All original data is now stale (all keys updated to small values)
            let estimated_discard = *total_size as i64;
            discard_updates.insert(*file_id, estimated_discard);
        }
        tree.core.vlog.update_discard_stats(discard_updates);

        // Trigger GC to clean up stale data
        let compacted_files = tree.garbage_collect_vlog().await.unwrap();

        // Verify final state - all keys should have small values
        let txn = tree.begin().unwrap();
        assert_eq!(
            txn.get(b"a").unwrap().unwrap(),
            small_value_a.to_vec().into()
        );
        assert_eq!(
            txn.get(b"b").unwrap().unwrap(),
            small_value_b.to_vec().into()
        );
        assert_eq!(
            txn.get(b"c").unwrap().unwrap(),
            small_value_c.to_vec().into()
        );

        let stats_after_gc = tree.get_all_vlog_stats();
        if !compacted_files.is_empty() {
            // After GC, space amplification should be lower
            let space_amp_after_gc = calculate_space_amplification(&stats_after_gc);
            assert!(
                space_amp_after_gc < space_amp_after_b,
                "Space amplification should decrease after GC"
            );

            println!("Space amplification:");
            println!("  After A update: {:.2}", space_amp_after_a);
            println!("  After B update: {:.2}", space_amp_after_b);
            println!("  After GC: {:.2}", space_amp_after_gc);
        }
    }

    #[tokio::test]
    async fn test_vlog_gc_mixed_operations() {
        // Test VLog GC with mixed insert, update, and delete operations
        let temp_dir = create_temp_directory();
        let path = temp_dir.path().to_path_buf();

        let opts = Arc::new(Options {
            path: path.clone(),
            vlog_value_threshold: 50,
            vlog_max_file_size: 800, // Small files for frequent rotation
            max_memtable_size: 512,
            ..Default::default()
        });

        let tree = Tree::new(opts).unwrap();

        let large_value = b"dummy".repeat(1_000);

        // Phase 1: Insert initial data
        let mut txn = tree.begin().unwrap();
        txn.set(b"a", &large_value).unwrap();
        txn.set(b"b", &large_value).unwrap();
        txn.set(b"c", &large_value).unwrap();
        txn.commit().await.unwrap();

        tree.flush().unwrap();

        let stats_initial = tree.get_all_vlog_stats();
        assert!(!stats_initial.is_empty(), "Should have VLog files");

        // Phase 2: Mixed operations to create staleness patterns

        // Update "a" to small value (stale large value in VLog)
        let mut txn = tree.begin().unwrap();
        txn.set(b"a", b"small_a").unwrap();
        txn.commit().await.unwrap();

        // Delete "b" (stale large value in VLog)
        let mut txn = tree.begin().unwrap();
        txn.delete(b"b").unwrap();
        txn.commit().await.unwrap();

        // Keep "c" unchanged (live large value in VLog)
        // Add new key "d" with large value
        let mut txn = tree.begin().unwrap();
        txn.set(b"d", &large_value).unwrap();
        txn.commit().await.unwrap();

        tree.flush().unwrap();

        // Mark files with appropriate staleness
        let mut discard_updates = std::collections::HashMap::new();
        for (file_id, total_size, _discard_bytes, _ratio) in &stats_initial {
            // About 2/3 of original data is stale (keys "a" and "b" changed)
            let estimated_discard = (*total_size as f64 * 0.67) as i64;
            discard_updates.insert(*file_id, estimated_discard);
        }
        tree.core.vlog.update_discard_stats(discard_updates);

        // Trigger GC
        let compacted_files = tree.garbage_collect_vlog().await.unwrap();

        // Verify final state
        let txn = tree.begin().unwrap();
        assert_eq!(txn.get(b"a").unwrap().unwrap(), b"small_a".to_vec().into()); // Updated to small value
        assert!(txn.get(b"b").unwrap().is_none()); // Deleted
        assert_eq!(txn.get(b"c").unwrap().unwrap(), large_value.clone().into()); // Unchanged large value
        assert_eq!(txn.get(b"d").unwrap().unwrap(), large_value.clone().into()); // New large value

        if !compacted_files.is_empty() {
            println!("GC successfully compacted {} files", compacted_files.len());
            let stats_after_gc = tree.get_all_vlog_stats();
            let space_amp_before = calculate_space_amplification(&stats_initial);
            let space_amp_after = calculate_space_amplification(&stats_after_gc);
            println!(
                "Space amplification: {:.2} -> {:.2}",
                space_amp_before, space_amp_after
            );
        }
    }

    fn calculate_space_amplification(stats: &[(u64, u64, u64, f64)]) -> f64 {
        if stats.is_empty() {
            return 0.0;
        }

        let total_size: u64 = stats.iter().map(|(_, size, _, _)| size).sum();
        let total_discard: u64 = stats.iter().map(|(_, _, discard, _)| discard).sum();
        let live_size = total_size.saturating_sub(total_discard);

        if live_size == 0 {
            return 0.0;
        }

        total_size as f64 / live_size as f64
    }

    #[tokio::test]
    async fn test_discard_file_directory_structure() {
        // Test to verify that the DISCARD file lives in the main database directory,
        // not in the VLog subdirectory
        let temp_dir = create_temp_directory();
        let path = temp_dir.path().to_path_buf();

        let opts = Arc::new(Options {
            path: path.clone(),
            vlog_value_threshold: 50,
            vlog_max_file_size: 1024,
            max_memtable_size: 512,
            ..Default::default()
        });

        let tree = Tree::new(opts).unwrap();

        // Insert some data to ensure VLog and discard stats are created
        let large_value = vec![1u8; 200]; // Large enough for VLog
        for i in 0..5 {
            let key = format!("key_{}", i);
            let mut txn = tree.begin().unwrap();
            txn.set(key.as_bytes(), &large_value).unwrap();
            txn.commit().await.unwrap();
        }

        // Force flush to create VLog files and initialize discard stats
        tree.flush().unwrap();

        // Update some keys to create discard statistics
        let mut discard_updates = std::collections::HashMap::new();
        let stats = tree.get_all_vlog_stats();
        for (file_id, total_size, _discard_bytes, _ratio) in &stats {
            if *total_size > 0 {
                discard_updates.insert(*file_id, (*total_size as f64 * 0.3) as i64);
            }
        }
        tree.core.vlog.update_discard_stats(discard_updates);

        // Verify directory structure
        let main_db_dir = &path;
        let vlog_dir = path.join("vlog");
        let sstables_dir = path.join("sstables");
        let wal_dir = path.join("wal");

        // Check that all expected directories exist
        assert!(main_db_dir.exists(), "Main database directory should exist");
        assert!(vlog_dir.exists(), "VLog directory should exist");
        assert!(sstables_dir.exists(), "SSTables directory should exist");
        assert!(wal_dir.exists(), "WAL directory should exist");

        // Check that DISCARD file is in the main database directory
        let discard_file_in_main = main_db_dir.join("DISCARD");
        let discard_file_in_vlog = vlog_dir.join("DISCARD");

        assert!(
            discard_file_in_main.exists(),
            "DISCARD file should exist in main database directory: {:?}",
            discard_file_in_main
        );
        assert!(
            !discard_file_in_vlog.exists(),
            "DISCARD file should NOT exist in VLog directory: {:?}",
            discard_file_in_vlog
        );

        // Verify that VLog files are in the VLog directory
        let vlog_entries = std::fs::read_dir(&vlog_dir).unwrap();
        let vlog_files: Vec<_> = vlog_entries
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("vlog_") && name.ends_with(".log") {
                    Some(name)
                } else {
                    None
                }
            })
            .collect();

        assert!(
            !vlog_files.is_empty(),
            "Should have VLog files in the VLog directory"
        );

        // Verify that the DISCARD file is separate from VLog files
        for vlog_file in &vlog_files {
            assert_ne!(
                vlog_file, "DISCARD",
                "DISCARD should not be mistaken for a VLog file"
            );
        }

        // Verify other expected files are in the main directory
        let levels_file = main_db_dir.join("levels");
        assert!(
            levels_file.exists(),
            "levels manifest should be in main directory alongside DISCARD"
        );

        println!("✓ Directory structure verification passed:");
        println!("  Main DB dir: {:?}", main_db_dir);
        println!(
            "  DISCARD file: {:?} (exists: {})",
            discard_file_in_main,
            discard_file_in_main.exists()
        );
        println!("  VLog dir: {:?}", vlog_dir);
        println!("  VLog files: {:?}", vlog_files);
        println!(
            "  levels file: {:?} (exists: {})",
            levels_file,
            levels_file.exists()
        );
    }

    #[tokio::test]
    async fn test_options_with_invalid_vlog_threshold() {
        let temp_dir = create_temp_directory();
        let path = temp_dir.path().to_path_buf();

        let opts = Arc::new(Options {
            path: path.clone(),
            vlog_value_threshold: 28,
            ..Default::default()
        });

        let result = Tree::new(opts);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Invalid argument: vlog_value_threshold must be greater than 28 bytes"
        );
    }
}
