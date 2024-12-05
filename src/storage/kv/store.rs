use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::vec;

use async_channel::{bounded, Receiver, Sender};
use futures::{select, FutureExt};

use ahash::{HashMap, HashMapExt};
use bytes::{Bytes, BytesMut};
use parking_lot::RwLock;
use quick_cache::sync::Cache;
use revision::Revisioned;

use crate::storage::{
    kv::{
        compaction::restore_from_compaction,
        entry::{encode_entries, Entry, Record},
        error::{Error, Result},
        indexer::{IndexValue, Indexer},
        manifest::Manifest,
        option::Options,
        oracle::Oracle,
        reader::{Reader, RecordReader},
        repair::{repair_last_corrupted_segment, restore_repair_files},
        stats::StorageStats,
        transaction::{Durability, Mode, Transaction},
    },
    log::{Aol, Error as LogError, MultiSegmentReader, Options as LogOptions, SegmentRef},
};

pub(crate) struct StoreInner {
    pub(crate) core: Arc<Core>,
    pub(crate) is_closed: AtomicBool,
    pub(crate) is_compacting: AtomicBool,
    stop_tx: Sender<()>,
    done_rx: Receiver<()>,
    pub(crate) stats: Arc<StorageStats>,
}

// Inner representation of the store. The wrapper will handle the asynchronous closing of the store.
impl StoreInner {
    /// Creates a new MVCC key-value store with the given options.
    /// It creates a new core with the options and wraps it in an atomic reference counter.
    /// It returns the store.
    pub fn new(opts: Options) -> Result<Self> {
        // TODO: make this channel size configurable
        let (writes_tx, writes_rx) = bounded(10000);
        let (stop_tx, stop_rx) = bounded(1);

        let core = Arc::new(Core::new(opts, writes_tx)?);
        let (task_runner, done_rx) = TaskRunner::new(core.clone(), writes_rx, stop_rx);
        task_runner.spawn();

        Ok(Self {
            core,
            stop_tx,
            done_rx,
            is_closed: AtomicBool::new(false),
            is_compacting: AtomicBool::new(false),
            stats: Arc::new(StorageStats::new()),
        })
    }

    /// Closes the store. It sends a stop signal to the writer and waits for the done signal.
    pub async fn close(&self) -> Result<()> {
        if self.is_closed.load(Ordering::SeqCst) {
            return Ok(());
        }

        if self.is_compacting.load(Ordering::SeqCst) {
            return Err(Error::CompactionAlreadyInProgress);
        }

        // Send stop signal
        self.stop_tx
            .send(())
            .await
            .map_err(|e| Error::SendError(format!("{}", e)))?;

        // Wait for done signal
        self.done_rx.recv().await.map_err(|e| {
            Error::ReceiveError(format!("Error waiting for task runner to complete: {}", e))
        })?;

        self.core.close()?;

        self.is_closed.store(true, Ordering::Relaxed);

        Ok(())
    }
}

/// An MVCC-based transactional key-value store.
///
/// The store is closed asynchronously when it is dropped.
/// If you need to guarantee that the store is closed before the program continues, use the `close` method.

// This is a wrapper around the inner store to allow for asynchronous closing of the store.
#[derive(Default)]
pub struct Store {
    pub(crate) inner: Option<StoreInner>,
}

impl Store {
    /// Creates a new MVCC key-value store with the given options.
    pub fn new(opts: Options) -> Result<Self> {
        Ok(Self {
            inner: Some(StoreInner::new(opts)?),
        })
    }

    /// Begins a new read-write transaction.
    /// It creates a new transaction with the core and read-write mode, and sets the read timestamp from the oracle.
    /// It returns the transaction.
    pub fn begin(&self) -> Result<Transaction> {
        let txn = Transaction::new(self.inner.as_ref().unwrap().core.clone(), Mode::ReadWrite)?;
        Ok(txn)
    }

    /// Begins a new transaction with the given mode.
    /// It creates a new transaction with the core and the given mode, and sets the read timestamp from the oracle.
    /// It returns the transaction.
    pub fn begin_with_mode(&self, mode: Mode) -> Result<Transaction> {
        let txn = Transaction::new(self.inner.as_ref().unwrap().core.clone(), mode)?;
        Ok(txn)
    }

    /// Executes a function in a read-only transaction.
    /// It begins a new read-only transaction and executes the function with the transaction.
    /// It returns the result of the function.
    pub fn view(&self, f: impl FnOnce(&mut Transaction) -> Result<()>) -> Result<()> {
        let mut txn = self.begin_with_mode(Mode::ReadOnly)?;
        f(&mut txn)?;

        Ok(())
    }

    /// Executes a function in a read-write transaction and commits the transaction.
    /// It begins a new read-write transaction, executes the function with the transaction, and commits the transaction.
    /// It returns the result of the function.
    pub async fn write(
        self: Arc<Self>,
        f: impl FnOnce(&mut Transaction) -> Result<()>,
    ) -> Result<()> {
        let mut txn = self.begin_with_mode(Mode::ReadWrite)?;
        f(&mut txn)?;
        txn.commit().await?;

        Ok(())
    }

    /// Closes the inner store
    pub async fn close(&self) -> Result<()> {
        if let Some(inner) = self.inner.as_ref() {
            inner.close().await?;
        }

        Ok(())
    }

    /// Compacts the store.
    pub async fn compact(&self) -> Result<()> {
        if let Some(inner) = self.inner.as_ref() {
            inner.compact().await?;
        }

        Ok(())
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            // Try to get existing runtime handle first
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                // We're in a runtime, spawn normally
                handle.spawn(async move {
                    if let Err(err) = inner.close().await {
                        // TODO: use log/tracing instead of eprintln
                        eprintln!("Error closing store: {}", err);
                    }
                });
            } else {
                eprintln!("No runtime available for closing the store correctly");
            }
        }
    }
}

pub(crate) struct TaskRunner {
    core: Arc<Core>,
    writes_rx: Receiver<Task>,
    stop_rx: Receiver<()>,
    // Done channel to signal completion
    done_tx: Arc<Sender<()>>,
}

impl TaskRunner {
    fn new(
        core: Arc<Core>,
        writes_rx: Receiver<Task>,
        stop_rx: Receiver<()>,
    ) -> (Self, Receiver<()>) {
        let (done_tx, done_rx) = bounded(1);
        (
            Self {
                core,
                writes_rx,
                stop_rx,
                done_tx: Arc::new(done_tx),
            },
            done_rx,
        )
    }

    fn spawn(self) {
        let done_tx = self.done_tx.clone();

        #[cfg(not(target_arch = "wasm32"))]
        tokio::spawn(self.run(done_tx));

        #[cfg(target_arch = "wasm32")]
        wasm_bindgen_futures::spawn_local(self.run(done_tx));
    }

    async fn run(self, done_tx: Arc<Sender<()>>) {
        loop {
            select! {
                req = self.writes_rx.recv().fuse() => {
                    match req {
                        Ok(task) => self.handle_task(task).await,
                        Err(_) => break,
                    }
                },
                _ = self.stop_rx.recv().fuse() => {
                    // Consume all remaining items in writes_rx
                    while let Ok(task) = self.writes_rx.try_recv() {
                        self.handle_task(task).await;
                    }
                    break;
                },
            }
        }

        // Signal completion
        let _ = done_tx.send(()).await;
    }

    async fn handle_task(&self, task: Task) {
        let core = self.core.clone();
        if let Err(err) = core.write_request(task).await {
            eprintln!("failed to write: {:?}", err);
        }
    }
}

/// Core of the key-value store.
pub struct Core {
    /// Index for store.
    pub(crate) indexer: RwLock<Indexer>,
    /// Options for store.
    pub(crate) opts: Options,
    /// Commit log for store.
    pub(crate) clog: Option<Arc<RwLock<Aol>>>,
    /// Manifest for store to track Store state.
    pub(crate) manifest: Option<RwLock<Aol>>,
    /// Transaction ID Oracle for store.
    pub(crate) oracle: Arc<Oracle>,
    /// Value cache for store.
    /// The assumption for this cache is that it should be useful for
    /// storing offsets that are frequently accessed (especially in
    /// the case of range scans)
    pub(crate) value_cache: Cache<(u64, u64), Bytes>,
    /// Flag to indicate if the store is closed.
    is_closed: AtomicBool,
    /// Channel to send write requests to the writer
    writes_tx: Sender<Task>,
}
/// A Task contains multiple entries to be written to the disk.
#[derive(Clone)]
pub struct Task {
    /// Entries contained in this task
    entries: Vec<Entry>,
    /// Use channel to notify that the value has been persisted to disk
    done: Option<Sender<Result<()>>>,
    /// Transaction ID
    tx_id: u64,
    /// Durability
    durability: Durability,
}

impl Core {
    fn initialize_indexer() -> Indexer {
        Indexer::new()
    }

    // This function initializes the manifest log for the database to store all settings.
    pub(crate) fn initialize_manifest(dir: &Path) -> Result<Aol> {
        let manifest_subdir = dir.join("manifest");
        let mopts = LogOptions::default().with_file_extension("manifest".to_string());
        Aol::open(&manifest_subdir, &mopts).map_err(Error::from)
    }

    // This function initializes the commit log (clog) for the database.
    fn initialize_clog(opts: &Options) -> Result<Aol> {
        // It first constructs the path to the clog subdirectory within the database directory.
        let clog_subdir = opts.dir.join("clog");

        // Then it creates a LogOptions object to configure the clog.
        // The maximum file size for the clog is set to the max_segment_size option from the database options.
        // The file extension for the clog files is set to "clog".
        let copts = LogOptions::default()
            .with_max_file_size(opts.max_segment_size)
            .with_file_extension("clog".to_string());

        // It then attempts to restore any repair files in the clog subdirectory.
        // If this fails, the error is propagated up to the caller of the function.
        // This is required because the repair operation may have failed, and the
        // store should not be opened with existing repair files.
        //
        // Even though we are restoring the corrupted files, it will get repaired
        // during in the load_index function.
        restore_repair_files(clog_subdir.as_path().to_str().unwrap())?;

        // Finally, it attempts to open the clog with the specified options.
        // If this fails, the error is converted to a database error and then propagated up to the caller of the function.
        Aol::open(&clog_subdir, &copts).map_err(Error::from)
    }

    /// Creates a new Core with the given options.
    /// It initializes a new Indexer, opens or creates the manifest file,
    /// loads or creates metadata from the manifest file, updates the options with the loaded metadata,
    /// opens or creates the commit log file, loads the index from the commit log if it exists, creates
    /// and initializes an Oracle, creates and initializes a value cache, and constructs and returns
    /// the Core instance.
    pub fn new(opts: Options, writes_tx: Sender<Task>) -> Result<Self> {
        // Initialize a new Indexer with the provided options.
        let mut indexer = Self::initialize_indexer();

        let mut manifest = None;
        let mut clog = None;

        if opts.should_persist_data() {
            // Determine options for the manifest file and open or create it.
            manifest = Some(Self::initialize_manifest(&opts.dir)?);

            // Load options from the manifest file.
            let opts = Core::load_manifest(&opts, manifest.as_mut().unwrap())?;

            // Determine options for the commit log file and open or create it.
            clog = Some(Self::initialize_clog(&opts)?);

            // Restore the store from a compaction process if necessary.
            restore_from_compaction(&opts)?;

            // Load the index from the commit log if it exists.
            if clog.as_ref().unwrap().size()? > 0 {
                Core::load_index(&opts, clog.as_mut().unwrap(), &mut indexer)?;
            }
        }

        // Create and initialize an Oracle.
        let oracle = Oracle::new(&opts);
        oracle.set_ts(indexer.version());

        // Create and initialize value cache.
        let value_cache = Cache::new(opts.max_value_cache_size as usize);

        // Construct and return the Core instance.
        Ok(Self {
            indexer: RwLock::new(indexer),
            opts,
            manifest: manifest.map(RwLock::new),
            clog: clog.map(|c| Arc::new(RwLock::new(c))),
            oracle: Arc::new(oracle),
            value_cache,
            is_closed: AtomicBool::new(false),
            writes_tx,
        })
    }

    pub(crate) fn read_ts(&self) -> u64 {
        self.oracle.read_ts()
    }

    // The load_index function is responsible for loading the index from the log.
    fn load_index(opts: &Options, clog: &mut Aol, indexer: &mut Indexer) -> Result<u64> {
        // The directory where the log segments are stored is determined.
        let clog_subdir = opts.dir.join("clog");

        // The segments are read from the directory.
        let sr = SegmentRef::read_segments_from_directory(clog_subdir.as_path())
            .expect("should read segments");

        // A MultiSegmentReader is created to read from multiple segments.
        let reader = MultiSegmentReader::new(sr)?;

        // A Reader is created from the MultiSegmentReader with the maximum segment size and block size.
        let reader = Reader::new_from(reader);

        // A RecordReader is created from the Reader to read transactions.
        let mut tx_reader = RecordReader::new(reader);

        // A Record is created to hold the transactions. The maximum number of entries per transaction is specified.
        let mut tx = Record::new();

        // An Option is created to hold the segment ID and offset in case of corruption.
        let mut corruption_info: Option<(u64, u64)> = None;

        let mut num_entries = 0;
        // A loop is started to read transactions.
        loop {
            // The Record is reset for each iteration.
            tx.reset();

            // The RecordReader attempts to read into the Record.
            match tx_reader.read_into(&mut tx) {
                // If the read is successful, the entries are processed.
                Ok((segment_id, value_offset)) => {
                    Core::process_entry(&tx, opts, segment_id, value_offset, indexer)?;
                    num_entries += 1;
                }

                // If the end of the file is reached, the loop is broken.
                Err(Error::LogError(LogError::Eof)) => break,

                // If a corruption error is encountered, the segment ID and offset are stored and the loop is broken.
                Err(Error::LogError(LogError::Corruption(err))) => {
                    eprintln!("Corruption error: {:?}", err);
                    corruption_info = Some((err.segment_id, err.offset));
                    break;
                }

                // If any other error is encountered, it is returned immediately.
                Err(err) => return Err(err),
            };
        }

        // If a corruption was encountered, the last segment is repaired using the stored segment ID and offset.
        // The reason why the last segment is repaired is because the last segment is the one that was being actively
        // written to and acts like the active WAL file. Any corruption in the previous immutable segments is pure
        // corruption of the data and should be handled by the user.
        if let Some((corrupted_segment_id, corrupted_offset)) = corruption_info {
            eprintln!(
                "Repairing corrupted segment with id: {} and offset: {}",
                corrupted_segment_id, corrupted_offset
            );
            repair_last_corrupted_segment(clog, corrupted_segment_id, corrupted_offset)?;
        }

        Ok(num_entries)
    }

    fn process_entry(
        entry: &Record,
        opts: &Options,
        segment_id: u64,
        value_offset: u64,
        indexer: &mut Indexer,
    ) -> Result<()> {
        if entry.metadata.as_ref().map_or(false, |metadata| {
            metadata.is_deleted() || metadata.is_tombstone() && !opts.enable_versions
        }) {
            indexer.delete(&mut entry.key[..].into());
        } else {
            let index_value = IndexValue::new_disk(
                segment_id,
                value_offset,
                entry.metadata.clone(),
                &entry.value,
                opts.max_value_threshold,
            );

            if opts.enable_versions {
                indexer.insert(
                    &mut entry.key[..].into(),
                    index_value,
                    entry.id,
                    entry.ts,
                    false,
                )?;
            } else {
                indexer.insert_or_replace(
                    &mut entry.key[..].into(),
                    index_value,
                    entry.id,
                    entry.ts,
                    false,
                )?;
            }
        }

        Ok(())
    }

    fn load_manifest(current_opts: &Options, manifest: &mut Aol) -> Result<Options> {
        // Load existing manifests if any, else create a new one
        let existing_manifest = if manifest.size()? > 0 {
            Core::read_manifest(&current_opts.dir)?
        } else {
            Manifest::new()
        };

        // Validate the current options against the existing manifest's options
        Core::validate_options(current_opts)?;

        // Check if the current options are already the last option in the manifest
        if existing_manifest.extract_last_option().as_ref() == Some(current_opts) {
            return Ok(current_opts.clone());
        }

        // If not, create a changeset with an update operation for the current options
        let changeset = Manifest::with_update_option_change(current_opts);

        // Serialize the changeset and append it to the manifest
        let buf = changeset.serialize()?;
        manifest.append(&buf)?;

        Ok(current_opts.clone())
    }

    fn validate_options(opts: &Options) -> Result<()> {
        if opts.max_compaction_segment_size < opts.max_segment_size {
            return Err(Error::CompactionSegmentSizeTooSmall);
        }

        Ok(())
    }

    /// Loads the latest options from the manifest log.
    pub(crate) fn read_manifest(dir: &Path) -> Result<Manifest> {
        let manifest_subdir = dir.join("manifest");
        let sr = SegmentRef::read_segments_from_directory(manifest_subdir.as_path())
            .expect("should read segments");
        let reader = MultiSegmentReader::new(sr)?;
        let mut reader = Reader::new_from(reader);

        let mut manifests = Manifest::new(); // Initialize with an empty Vec

        loop {
            // Read the next transaction record from the log.
            let mut len_buf = [0; 4];
            let res = reader.read(&mut len_buf); // Read 4 bytes for the length
            if let Err(e) = res {
                if let Error::LogError(LogError::Eof) = e {
                    break;
                } else {
                    return Err(e);
                }
            }

            let len = u32::from_be_bytes(len_buf) as usize; // Convert bytes to length
            let mut md_bytes = vec![0u8; len];
            reader.read(&mut md_bytes)?; // Read the actual metadata
            let manifest = Manifest::deserialize_revisioned(&mut md_bytes.as_slice())?;
            manifests.changes.extend(manifest.changes);
        }

        Ok(manifests)
    }

    fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Relaxed)
    }

    pub(crate) fn close(&self) -> Result<()> {
        if self.is_closed() {
            return Ok(());
        }

        // Close the commit log if it exists
        if let Some(clog) = &self.clog {
            clog.write().close()?;
        }

        // Close the manifest if it exists
        if let Some(manifest) = &self.manifest {
            manifest.write().close()?;
        }
        self.is_closed.store(true, Ordering::Relaxed);

        Ok(())
    }

    pub(crate) async fn write_request(&self, req: Task) -> Result<()> {
        let done = req.done.clone();

        let result = self.write_entries(req);

        if let Some(done) = done {
            done.send(result.clone()).await?;
        }

        result
    }

    fn write_entries(&self, req: Task) -> Result<()> {
        if req.entries.is_empty() {
            return Ok(());
        }

        if self.opts.should_persist_data() {
            self.write_entries_to_disk(req)
        } else {
            self.write_index_in_memory(&req)
        }
    }

    fn write_entries_to_disk(&self, req: Task) -> Result<()> {
        // TODO: This buf can be reused by defining on core level
        let mut buf = BytesMut::new();
        let mut values_offsets = HashMap::with_capacity(req.entries.len());

        encode_entries(&req.entries, req.tx_id, &mut buf, &mut values_offsets);

        let (segment_id, current_offset) = self.append_log(&buf, req.durability)?;

        values_offsets.iter_mut().for_each(|(_, val_off)| {
            *val_off += current_offset;
        });

        self.write_entries_to_index(&req, |entry| {
            let offset = *values_offsets.get(&entry.key).unwrap();
            IndexValue::new_disk(
                segment_id,
                offset,
                entry.metadata.clone(),
                &entry.value,
                self.opts.max_value_threshold,
            )
        })
    }

    fn append_log(&self, tx_record: &BytesMut, durability: Durability) -> Result<(u64, u64)> {
        let mut clog = self.clog.as_ref().unwrap().write();

        let (segment_id, offset) = match durability {
            Durability::Immediate => {
                // Immediate durability means that the transaction is made to
                // fsync the data to disk before returning.
                let (segment_id, offset, _) = clog.append(tx_record)?;
                clog.sync()?;
                (segment_id, offset)
            }
            Durability::Eventual => {
                // Eventual durability means that the transaction is made to
                // write to disk using the write_all method. But it does not
                // fsync the data to disk before returning.
                let (segment_id, offset, _) = clog.append(tx_record)?;
                (segment_id, offset)
            }
        };

        Ok((segment_id, offset))
    }

    fn write_entries_to_index<F>(&self, task: &Task, encode_entry: F) -> Result<()>
    where
        F: Fn(&Entry) -> IndexValue,
    {
        let mut index = self.indexer.write();

        for entry in &task.entries {
            // If the entry is marked as deleted or a tombstone
            // with the replace flag set, delete it.
            if let Some(metadata) = entry.metadata.as_ref() {
                if metadata.is_deleted() || metadata.is_tombstone() && entry.replace {
                    index.delete(&mut entry.key[..].into());
                    continue;
                }
            }

            let index_value = encode_entry(entry);

            if !entry.replace {
                index.insert(
                    &mut entry.key[..].into(),
                    index_value,
                    task.tx_id,
                    entry.ts,
                    true,
                )?;
            } else {
                index.insert_or_replace(
                    &mut entry.key[..].into(),
                    index_value,
                    task.tx_id,
                    entry.ts,
                    true,
                )?;
            }
        }

        Ok(())
    }

    fn write_index_in_memory(&self, task: &Task) -> Result<()> {
        self.write_entries_to_index(task, |entry| {
            IndexValue::new_mem(entry.metadata.clone(), entry.value.clone())
        })
    }

    pub(crate) async fn send_to_write_channel(
        &self,
        entries: Vec<Entry>,
        tx_id: u64,
        durability: Durability,
    ) -> Result<Receiver<Result<()>>> {
        let (tx, rx) = bounded(1);
        let req = Task {
            entries,
            done: Some(tx),
            tx_id,
            durability,
        };
        self.writes_tx.send(req).await?;
        Ok(rx)
    }

    /// Resolves the value from the given offset in the commit log.
    /// If the offset exists in the value cache, it returns the cached value.
    /// Otherwise, it reads the value from the commit log, caches it, and returns it.
    pub(crate) fn resolve_from_offset(
        &self,
        segment_id: u64,
        value_offset: u64,
        value_len: usize,
    ) -> Result<Vec<u8>> {
        // Attempt to return the cached value if it exists
        let cache_key = (segment_id, value_offset);

        if let Some(value) = self.value_cache.get(&cache_key) {
            return Ok(value.to_vec());
        }

        // If the value is not in the cache, read it from the commit log
        let mut buf = vec![0; value_len];
        let clog = self.clog.as_ref().unwrap().read();
        clog.read_at(&mut buf, segment_id, value_offset)?;

        // Cache the newly read value for future use
        self.value_cache.insert(cache_key, Bytes::from(buf.clone()));

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use rand::prelude::SliceRandom;
    use rand::Rng;

    use std::sync::Arc;

    use crate::storage::kv::option::Options;
    use crate::storage::kv::store::Core;
    use crate::storage::kv::store::{Store, Task, TaskRunner};
    use crate::storage::kv::transaction::Durability;
    use crate::storage::log::Error as LogError;
    use crate::{Error, IsolationLevel};

    use async_channel::bounded;
    use std::sync::atomic::{AtomicU64, Ordering};

    use bytes::Bytes;
    use tempdir::TempDir;

    use skv44;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    #[tokio::test]
    async fn bulk_insert_and_reload() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Create a new store instance with VariableKey as the key type
        let store = Store::new(opts).expect("should create store");

        // Number of keys to generate
        let num_keys = 10000;

        // Create a vector to store the generated keys
        let mut keys: Vec<Bytes> = Vec::new();

        for (counter, _) in (1..=num_keys).enumerate() {
            // Convert the counter to Bytes
            let key_bytes = Bytes::from(counter.to_le_bytes().to_vec());

            // Add the key to the vector
            keys.push(key_bytes);
        }

        let default_value = Bytes::from("default_value".to_string());

        // Write the keys to the store
        for key in keys.iter() {
            // Start a new write transaction
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().await.unwrap();
        }

        // Read the keys to the store
        for key in keys.iter() {
            // Start a new read transaction
            let mut txn = store.begin().unwrap();
            let val = txn.get(key).unwrap().unwrap();
            // Assert that the value retrieved in txn matches default_value
            assert_eq!(val, default_value.as_ref());
        }

        // Drop the store to simulate closing it
        store.close().await.unwrap();

        // Create a new store instance but with values read from disk
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        // This is to ensure values are read from disk
        opts.max_value_threshold = 0;

        let store = Store::new(opts).expect("should create store");

        // Read the keys to the store
        for key in keys.iter() {
            // Start a new read transaction
            let mut txn = store.begin().unwrap();
            let val = txn.get(key).unwrap().unwrap();
            // Assert that the value retrieved in txn matches default_value
            assert_eq!(val, default_value.as_ref());
        }
    }

    #[tokio::test]
    async fn store_open_and_update_options() {
        // // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Create a new store instance with VariableKey as the key type
        let store = Store::new(opts.clone()).expect("should create store");
        store.close().await.unwrap();

        // Update the options and use them to update the new store instance
        let mut opts = opts.clone();
        opts.max_value_cache_size = 5;

        let store = Store::new(opts.clone()).expect("should create store");
        let store_opts = store.inner.as_ref().unwrap().core.opts.clone();
        assert_eq!(store_opts, opts);
    }

    #[tokio::test]
    async fn insert_close_reopen() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        let num_ops = 10;

        for i in 1..=10 {
            // (Re)open the store
            let store = Store::new(opts.clone()).expect("should create store");

            // Append num_ops items to the store
            for j in 0..num_ops {
                let id = (i - 1) * num_ops + j;
                let key = format!("key{}", id);
                let value = format!("value{}", id);
                let mut txn = store.begin().unwrap();
                txn.set(key.as_bytes(), value.as_bytes()).unwrap();
                txn.commit().await.unwrap();
            }

            // Test that the items are still in the store
            for j in 0..(num_ops * i) {
                let key = format!("key{}", j);
                let value = format!("value{}", j);
                let value = value.into_bytes();
                let mut txn = store.begin().unwrap();
                let val = txn.get(key.as_bytes()).unwrap().unwrap();

                assert_eq!(val, value);
            }

            // Close the store again
            store.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn clone_store() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Create a new store instance with VariableKey as the key type
        let store = Arc::new(Store::new(opts).expect("should create store"));

        // Number of keys to generate
        let num_keys = 100;

        // Create a vector to store the generated keys
        let mut keys: Vec<Bytes> = Vec::new();

        for (counter, _) in (1..=num_keys).enumerate() {
            // Convert the counter to Bytes
            let key_bytes = Bytes::from(counter.to_le_bytes().to_vec());

            // Add the key to the vector
            keys.push(key_bytes);
        }

        let default_value = Bytes::from("default_value".to_string());
        let store1 = store.clone();

        // Write the keys to the store
        for key in keys.iter() {
            // Start a new write transaction
            let mut txn = store1.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().await.unwrap();
        }
    }

    async fn concurrent_task(store: Arc<Store>) {
        let mut txn = store.begin().unwrap();
        txn.set(b"dummy key", b"dummy value").unwrap();
        txn.commit().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_test() {
        let mut opts = Options::new();
        opts.dir = create_temp_directory().path().to_path_buf();
        let db = Arc::new(Store::new(opts).expect("should create store"));
        let task1 = tokio::spawn(concurrent_task(db.clone()));
        let task2 = tokio::spawn(concurrent_task(db.clone()));
        let _ = tokio::try_join!(task1, task2).expect("Tasks failed");
    }

    #[tokio::test]
    async fn insert_then_read_then_delete_then_read() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_value_threshold = 0;
        opts.max_value_cache_size = 0;

        // Create a new store instance with VariableKey as the key type
        let store = Store::new(opts).expect("should create store");

        // Number of keys to generate
        let num_keys = 5000;

        // Create a vector to store the generated keys
        let mut keys: Vec<Bytes> = Vec::new();

        for (counter, _) in (1..=num_keys).enumerate() {
            // Convert the counter to Bytes
            let key_bytes = Bytes::from(counter.to_le_bytes().to_vec());

            // Add the key to the vector
            keys.push(key_bytes);
        }

        let default_value = Bytes::from("default_value".to_string());

        // Write the keys to the store
        for key in keys.iter() {
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().await.unwrap();
        }

        // Read the keys from the store
        for key in keys.iter() {
            let mut txn = store.begin().unwrap();
            let val = txn.get(key).unwrap().unwrap();
            assert_eq!(val, default_value.as_ref());
        }

        // Delete the keys from the store
        for key in keys.iter() {
            let mut txn = store.begin().unwrap();
            txn.delete(key).unwrap();
            txn.commit().await.unwrap();
        }

        // ReWrite the keys to the store
        for key in keys.iter() {
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().await.unwrap();
        }

        // Read the keys from the store
        for key in keys.iter() {
            let mut txn = store.begin().unwrap();
            let val = txn.get(key).unwrap().unwrap();
            assert_eq!(val, default_value.as_ref());
        }
    }

    #[tokio::test]
    async fn records_not_lost_when_store_is_closed() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        let key = "key".as_bytes();
        let value = "value".as_bytes();

        {
            // Create a new store instance
            let store = Store::new(opts.clone()).expect("should create store");

            // Insert an item into the store
            let mut txn = store.begin().unwrap();
            txn.set(key, value).unwrap();
            txn.commit().await.unwrap();

            drop(txn);
            store.close().await.unwrap();
        }
        {
            // Reopen the store
            let store = Store::new(opts.clone()).expect("should create store");

            // Test that the item is still in the store
            let mut txn = store.begin().unwrap();
            let val = txn.get(key).unwrap();

            assert_eq!(val.unwrap(), value);
        }
    }

    async fn test_records_when_store_is_dropped(
        durability: Durability,
        wait: bool,
        should_exist: bool,
    ) {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        let key = "key".as_bytes();
        let value = "value".as_bytes();

        {
            // Create a new store instance
            let store = Store::new(opts.clone()).expect("should create store");

            // Insert an item into the store
            let mut txn = store.begin().unwrap();
            txn.set_durability(durability);
            txn.set(key, value).unwrap();
            txn.commit().await.unwrap();

            drop(txn);
            drop(store);
        }

        if wait {
            // Give some room for the store to close asynchronously
            tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
        }

        {
            // Reopen the store
            let store = Store::new(opts.clone()).expect("should create store");

            // Test that the item is still in the store
            let mut txn = store.begin().unwrap();
            let val = txn.get(key).unwrap();

            if should_exist {
                assert_eq!(val.unwrap(), value);
            } else {
                assert!(val.is_none());
            }
        }
    }

    #[tokio::test]
    async fn eventual_durability_records_persist_after_drop() {
        test_records_when_store_is_dropped(Durability::Eventual, true, true).await;
    }

    #[tokio::test]
    async fn eventual_durability_records_persist_without_wait() {
        test_records_when_store_is_dropped(Durability::Eventual, false, true).await;
    }

    #[tokio::test]
    async fn strong_durability_records_persist() {
        test_records_when_store_is_dropped(Durability::Immediate, true, true).await;
        test_records_when_store_is_dropped(Durability::Immediate, false, true).await;
    }

    #[tokio::test]
    async fn store_closed_twice_without_error() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Create a new store instance
        let store = Store::new(opts.clone()).expect("should create store");

        // Close the store once
        assert!(
            store.close().await.is_ok(),
            "should close store without error"
        );

        // Close the store a second time
        assert!(
            store.close().await.is_ok(),
            "should close store without error"
        );
    }

    /// Returns pairs of key, value
    fn gen_data(count: usize, key_size: usize, value_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut pairs = vec![];

        for _ in 0..count {
            let key: Vec<u8> = (0..key_size).map(|_| rand::thread_rng().gen()).collect();
            let value: Vec<u8> = (0..value_size).map(|_| rand::thread_rng().gen()).collect();
            pairs.push((key, value));
        }

        pairs
    }

    async fn test_durability(durability: Durability, wait_enabled: bool) {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        let num_elements = 100;
        let pairs = gen_data(num_elements, 16, 20);

        {
            // Create a new store instance
            let store = Store::new(opts.clone()).expect("should create store");

            let mut txn = store.begin().unwrap();
            txn.set_durability(durability);

            {
                for i in 0..num_elements {
                    let (key, value) = &pairs[i % pairs.len()];
                    txn.set(key.as_slice(), value.as_slice()).unwrap();
                }
            }
            txn.commit().await.unwrap();

            drop(store);
        }

        // Wait for a while to let close be called on drop as it is executed asynchronously
        if wait_enabled {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        let store = Store::new(opts.clone()).expect("should create store");
        let mut txn = store.begin().unwrap();

        let mut key_order: Vec<usize> = (0..num_elements).collect();
        key_order.shuffle(&mut rand::thread_rng());

        {
            for i in &key_order {
                let (key, value) = &pairs[*i % pairs.len()];
                let val = txn.get(key.as_slice()).unwrap().unwrap();
                assert_eq!(&val, value);
            }
        }
    }

    #[tokio::test]
    async fn eventual_durability() {
        test_durability(Durability::Eventual, false).await;
    }

    #[tokio::test]
    async fn immediate_durability() {
        test_durability(Durability::Immediate, false).await;
    }

    #[tokio::test]
    async fn store_without_persistance() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.disk_persistence = false;

        // Create a new store instance with VariableKey as the key type
        let store = Store::new(opts.clone()).expect("should create store");

        // Number of keys to generate
        let num_keys = 10000;

        // Create a vector to store the generated keys
        let mut keys: Vec<Bytes> = Vec::new();

        for (counter, _) in (1..=num_keys).enumerate() {
            // Convert the counter to Bytes
            let key_bytes = Bytes::from(counter.to_le_bytes().to_vec());

            // Add the key to the vector
            keys.push(key_bytes);
        }

        let default_value = Bytes::from("default_value".to_string());

        // Write the keys to the store
        for key in keys.iter() {
            // Start a new write transaction
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().await.unwrap();
        }

        // Read the keys to the store
        for key in keys.iter() {
            // Start a new read transaction
            let mut txn = store.begin().unwrap();
            let val = txn.get(key).unwrap().unwrap();
            // Assert that the value retrieved in txn matches default_value
            assert_eq!(val, default_value.as_ref());
        }

        // Drop the store to simulate closing it
        store.close().await.unwrap();

        let store = Store::new(opts).expect("should create store");

        // No keys should be found in the store
        for key in keys.iter() {
            // Start a new read transaction
            let mut txn = store.begin().unwrap();
            assert!(txn.get(key).unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn basic_compaction1() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_value_threshold = 0;
        opts.max_value_cache_size = 0;

        // Create a new store instance with VariableKey as the key type
        let store = Store::new(opts.clone()).expect("should create store");

        // Number of keys to generate and write
        let num_keys_to_write = 1;

        // Create a vector to store the generated keys
        let mut keys: Vec<Bytes> = Vec::new();

        for counter in 1usize..=num_keys_to_write {
            // Convert the counter to Bytes
            let key_bytes = Bytes::from(counter.to_le_bytes().to_vec());

            // Add the key to the vector
            keys.push(key_bytes);
        }

        let default_value = Bytes::from("default_value".to_string());
        let default_value2 = Bytes::from("default_value2".to_string());

        // Write the keys to the store
        for key in keys.iter() {
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().await.unwrap();
        }

        for key in keys.iter() {
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value2).unwrap();
            txn.commit().await.unwrap();
        }

        let key_bytes = Bytes::from(2usize.to_le_bytes().to_vec());
        let mut txn = store.begin().unwrap();
        txn.set(&key_bytes, &default_value2).unwrap();
        txn.commit().await.unwrap();

        // Delete the first 5 keys from the store
        for key in keys.iter() {
            let mut txn = store.begin().unwrap();
            txn.delete(key).unwrap();
            txn.commit().await.unwrap();
        }

        store.inner.as_ref().unwrap().compact().await.unwrap();
        store.close().await.unwrap();

        let reopened_store = Store::new(opts).expect("should reopen store");
        for key in keys.iter() {
            let mut txn = reopened_store.begin().unwrap();
            assert!(txn.get(key).unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_store_with_varying_segment_sizes() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_segment_size = 84; // Initial max segment size

        let k1 = Bytes::from("k1");
        let k2 = Bytes::from("k2");
        let k3 = Bytes::from("k3");
        let k4 = Bytes::from("k4");
        let val = Bytes::from("val");

        // Step 1: Open store with initial max segment size and commit a record
        let store = Store::new(opts.clone()).expect("should create store");
        {
            let mut txn = store.begin().unwrap();
            txn.set(&k1.clone(), &val.clone()).unwrap();
            txn.commit().await.unwrap();
        }
        store.close().await.expect("should close store");

        // Step 2: Reopen store with a smaller max segment size and append a record
        opts.max_segment_size = 37; // Smaller max segment size
        let store = Store::new(opts.clone()).expect("should create store");
        {
            let mut txn = store.begin().unwrap();
            txn.set(&k2.clone(), &val).unwrap();
            txn.commit().await.unwrap();

            // Verify the first record
            let mut txn = store.begin().unwrap();
            let val = txn.get(&k1).unwrap().unwrap();
            assert_eq!(val, val);

            // Verify the second record
            let val2 = txn.get(&k2).unwrap().unwrap();
            assert_eq!(val2, val);
        }
        store.close().await.expect("should close store");

        // Step 3: Reopen store with a larger max segment size and append a record
        opts.max_segment_size = 121; // Larger max segment size
        let store = Store::new(opts.clone()).expect("should create store");
        {
            let mut txn = store.begin().unwrap();
            txn.set(&k3.clone(), &val).unwrap();
            txn.commit().await.unwrap();

            // Verify the first record
            let mut txn = store.begin().unwrap();
            let val = txn.get(&k1).unwrap().unwrap();
            assert_eq!(val, val);

            // Verify the second record
            let val2 = txn.get(&k2).unwrap().unwrap();
            assert_eq!(val2, val);

            // Verify the third record
            let val3 = txn.get(&k3).unwrap().unwrap();
            assert_eq!(val3, val);
        }
        store.close().await.expect("should close store");

        // Step 4: Reopen store with a max segment size smaller than the record
        opts.max_segment_size = 36; // Smallest max segment size
        let store = Store::new(opts.clone()).expect("should create store");
        {
            let mut txn = store.begin().unwrap();
            txn.set(&k4.clone(), &val).unwrap();
            let err = txn.commit().await.err().unwrap();
            match err {
                Error::LogError(LogError::RecordTooLarge) => (),
                _ => panic!("expected RecordTooLarge error"),
            };
        }
        store.close().await.expect("should close store");
    }

    #[tokio::test]
    async fn load_index_with_disabled_versions() {
        let opts = Options {
            dir: create_temp_directory().path().to_path_buf(),
            enable_versions: false,
            ..Default::default()
        };

        let key = b"key";
        let value1 = b"value1";
        let value2 = b"value2";

        let store = Store::new(opts.clone()).unwrap();
        let mut txn1 = store.begin().unwrap();
        txn1.set(key, value1).unwrap();
        txn1.commit().await.unwrap();

        let mut txn2 = store.begin().unwrap();
        txn2.set(key, value2).unwrap();
        txn2.commit().await.unwrap();
        store.close().await.unwrap();

        let store = Store::new(opts.clone()).unwrap();
        let txn = store.begin_with_mode(crate::Mode::ReadOnly).unwrap();
        let history = txn.get_history(key).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].0, value2);
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn insert_with_disabled_versions() {
        let opts = Options {
            dir: create_temp_directory().path().to_path_buf(),
            enable_versions: false,
            ..Default::default()
        };

        let key = b"key";
        let value1 = b"value1";
        let value2 = b"value2";

        let store = Store::new(opts.clone()).unwrap();

        let mut txn1 = store.begin().unwrap();
        txn1.set(key, value1).unwrap();
        txn1.commit().await.unwrap();

        let mut txn2 = store.begin().unwrap();
        txn2.set(key, value2).unwrap();
        txn2.commit().await.unwrap();

        let txn = store.begin_with_mode(crate::Mode::ReadOnly).unwrap();
        let history = txn.get_history(key).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].0, value2);

        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn insert_with_disabled_versions_in_memory() {
        let opts = Options {
            dir: create_temp_directory().path().to_path_buf(),
            enable_versions: false,
            disk_persistence: false,
            ..Default::default()
        };

        let key = b"key";
        let value1 = b"value1";
        let value2 = b"value2";

        let store = Store::new(opts.clone()).unwrap();

        let mut txn1 = store.begin().unwrap();
        txn1.set(key, value1).unwrap();
        txn1.commit().await.unwrap();

        let mut txn2 = store.begin().unwrap();
        txn2.set(key, value2).unwrap();
        txn2.commit().await.unwrap();

        let txn = store.begin_with_mode(crate::Mode::ReadOnly).unwrap();
        let history = txn.get_history(key).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].0, value2);

        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn insert_with_replace() {
        let opts = Options {
            dir: create_temp_directory().path().to_path_buf(),
            enable_versions: true,
            ..Default::default()
        };

        let key = b"key";
        let value1 = b"value1";
        let value2 = b"value2";

        let store = Store::new(opts.clone()).unwrap();

        let mut txn1 = store.begin().unwrap();
        txn1.set(key, value1).unwrap();
        txn1.commit().await.unwrap();

        let mut txn2 = store.begin().unwrap();
        txn2.insert_or_replace(key, value2).unwrap();
        txn2.commit().await.unwrap();

        let txn = store.begin_with_mode(crate::Mode::ReadOnly).unwrap();
        let history = txn.get_history(key).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].0, value2);

        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn insert_with_replace_in_memory() {
        let opts = Options {
            dir: create_temp_directory().path().to_path_buf(),
            enable_versions: true,
            disk_persistence: false,
            ..Default::default()
        };

        let key = b"key";
        let value1 = b"value1";
        let value2 = b"value2";

        let store = Store::new(opts.clone()).unwrap();

        let mut txn1 = store.begin().unwrap();
        txn1.set(key, value1).unwrap();
        txn1.commit().await.unwrap();

        let mut txn2 = store.begin().unwrap();
        txn2.insert_or_replace(key, value2).unwrap();
        txn2.commit().await.unwrap();

        let txn = store.begin_with_mode(crate::Mode::ReadOnly).unwrap();
        let history = txn.get_history(key).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].0, value2);

        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn delete_with_versions_disabled() {
        let opts = Options {
            dir: create_temp_directory().path().to_path_buf(),
            enable_versions: false,
            ..Default::default()
        };

        let key = Bytes::from("key");
        let value1 = b"value1";

        let store = Store::new(opts.clone()).unwrap();

        let mut txn1 = store.begin().unwrap();
        txn1.set_at_ts(&key, value1, 1).unwrap();
        txn1.commit().await.unwrap();

        let mut txn2 = store.begin().unwrap();
        txn2.soft_delete(&key).unwrap();
        txn2.commit().await.unwrap();

        let txn3 = store.begin().unwrap();
        let versions = txn3
            .scan_all_versions(key.as_ref()..=key.as_ref(), None)
            .unwrap();
        assert!(versions.is_empty());

        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn delete_with_versions_disabled_in_memory() {
        let opts = Options {
            dir: create_temp_directory().path().to_path_buf(),
            enable_versions: false,
            disk_persistence: false,
            ..Default::default()
        };

        let key = Bytes::from("key");
        let value1 = b"value1";

        let store = Store::new(opts.clone()).unwrap();

        let mut txn1 = store.begin().unwrap();
        txn1.set_at_ts(&key, value1, 1).unwrap();
        txn1.commit().await.unwrap();

        let mut txn2 = store.begin().unwrap();
        txn2.soft_delete(&key).unwrap();
        txn2.commit().await.unwrap();

        let txn3 = store.begin().unwrap();
        let versions = txn3
            .scan_all_versions(key.as_ref()..=key.as_ref(), None)
            .unwrap();
        assert!(versions.is_empty());

        store.close().await.unwrap();
    }

    // Common setup logic for creating a store
    fn create_store(dir: Option<TempDir>, is_ssi: bool) -> (Store, TempDir) {
        let temp_dir = dir.unwrap_or_else(create_temp_directory);
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        if is_ssi {
            opts.isolation_level = IsolationLevel::SerializableSnapshotIsolation;
        }
        (
            Store::new(opts.clone()).expect("should create store"),
            temp_dir,
        )
    }

    #[tokio::test]
    async fn test_store_version_after_reopen() {
        let (store, temp_dir) = create_store(None, false);

        // Define the number of keys, key size, and value size
        let num_keys = 10000u32;
        let key_size = 32;
        let value_size = 32;

        // Generate keys and values
        let keys: Vec<Bytes> = (0..num_keys)
            .map(|i| {
                let mut key = vec![0; key_size];
                key[..4].copy_from_slice(&i.to_be_bytes());
                Bytes::from(key)
            })
            .collect();

        let value = Bytes::from(vec![0; value_size]);

        // Insert keys into the store
        {
            for key in &keys {
                let mut txn = store.begin().unwrap();
                txn.set(key, &value).unwrap();
                txn.commit().await.unwrap();
            }
        }

        // Close the store
        store.close().await.unwrap();

        // Reopen the store
        let (store, _) = create_store(Some(temp_dir), false);

        // Verify if the indexer version is set correctly
        assert_eq!(
            store.inner.as_ref().unwrap().core.indexer.read().version(),
            num_keys as u64
        );

        // Verify that the keys are present in the store
        let txn = store.begin_with_mode(crate::Mode::ReadOnly).unwrap();
        for key in &keys {
            let history = txn.get_history(key).unwrap();
            assert_eq!(history.len(), 1);
            assert_eq!(history[0].0, value);
        }

        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_incremental_transaction_ids_post_store_open() {
        let (store, temp_dir) = create_store(None, false);

        let total_records = 1000;
        let multiple_keys_records = total_records / 2;

        // Define keys and values
        let keys: Vec<Bytes> = (1..=total_records)
            .map(|i| Bytes::from(format!("key{}", i)))
            .collect();
        let values: Vec<Bytes> = (1..=total_records)
            .map(|i| Bytes::from(format!("value{}", i)))
            .collect();

        // Insert multiple transactions with single keys
        for (i, key) in keys.iter().enumerate().take(multiple_keys_records) {
            let mut txn = store.begin().unwrap();
            txn.set(key, &values[i]).unwrap();
            txn.commit().await.unwrap();
        }

        // Insert multiple transactions with multiple keys
        for (i, key) in keys
            .iter()
            .enumerate()
            .skip(multiple_keys_records)
            .take(multiple_keys_records)
        {
            let mut txn = store.begin().unwrap();
            txn.set(key, &values[i]).unwrap();
            txn.set(&keys[(i + 1) % keys.len()], &values[(i + 1) % values.len()])
                .unwrap();
            txn.commit().await.unwrap();
        }

        // Close the store
        store.close().await.unwrap();

        // Add delay to ensure that the store is closed
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Reopen the store
        let (store, _) = create_store(Some(temp_dir), false);

        // Commit a new transaction with a single key
        {
            let mut txn = store.begin().unwrap();
            txn.set(&keys[0], &values[0]).unwrap();
            txn.commit().await.unwrap();

            let res = txn.get_versionstamp().unwrap();

            assert_eq!(res.0, total_records as u64 + 1);
        }

        // Commit another new transaction with multiple keys
        {
            let mut txn = store.begin().unwrap();
            txn.set(&keys[1], &values[1]).unwrap();
            txn.set(&keys[2], &values[2]).unwrap();
            txn.commit().await.unwrap();

            let res = txn.get_versionstamp().unwrap();

            assert_eq!(res.0, total_records as u64 + 2);
        }

        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_tx_id_assignment_after_migration_from_skv44() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = skv44::Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Number of transactions
        let num_transactions = 10;
        // Number of keys per transaction
        let keys_per_transaction = 5;

        let default_value = Bytes::from("default_value".to_string());

        // Create a vector to store the generated keys
        let mut keys: Vec<Bytes> = Vec::new();

        for txn_id in 0..num_transactions {
            for key_id in 0..keys_per_transaction {
                // Generate a unique key for each transaction and key_id
                let key_bytes = Bytes::from(format!("txn{}_key{}", txn_id, key_id));
                keys.push(key_bytes);
            }
        }

        // Insert multiple records in each transaction and close/reopen the store
        for txn_id in 0..num_transactions {
            // Create a new store instance with VariableKey as the key type
            let store = skv44::Store::new(opts.clone()).expect("should create store");

            // Start a new write transaction
            let mut txn = store.begin().unwrap();
            for key_id in 0..keys_per_transaction {
                let key = Bytes::from(format!("txn{}_key{}", txn_id, key_id));
                txn.set(&key, &default_value).unwrap();
            }
            txn.commit().await.unwrap();

            // Drop the store to simulate closing it
            store.close().await.unwrap();
        }

        // Create a new store instance but with values read from disk
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        let store = Store::new(opts).expect("should create store");

        // Insert a new transaction into the reopened store
        let new_key = Bytes::from("new_key");
        let new_value = Bytes::from("new_value");

        {
            // Start a new write transaction
            let mut txn = store.begin().unwrap();
            txn.set(&new_key, &new_value).unwrap();
            txn.commit().await.unwrap();
            let (new_tx_id, _) = txn.get_versionstamp().unwrap();

            let expected_tx_id = ((num_transactions - 1) * keys_per_transaction) + 2;
            assert_eq!(expected_tx_id, new_tx_id);
        }

        // Verify the new transaction
        {
            // Start a new read transaction
            let mut txn = store.begin().unwrap();
            let val = txn.get(&new_key).unwrap().unwrap();
            // Assert that the value retrieved in txn matches new_value
            assert_eq!(val, new_value.as_ref());
        }

        // Read the keys from the store to verify after reopening
        for txn_id in 0..num_transactions {
            for key_id in 0..keys_per_transaction {
                let key = Bytes::from(format!("txn{}_key{}", txn_id, key_id));
                // Start a new read transaction
                let mut txn = store.begin().unwrap();
                let val = txn.get(&key).unwrap().unwrap();
                // Assert that the value retrieved in txn matches default_value
                assert_eq!(val, default_value.as_ref());
            }
        }
    }

    #[tokio::test]
    async fn stop_task_runner_with_pending_tasks() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        let (writes_tx, writes_rx) = bounded(100);
        let (stop_tx, stop_rx) = bounded(1);
        let core = Arc::new(Core::new(opts, writes_tx.clone()).unwrap());

        let (runner, done_rx) = TaskRunner::new(core.clone(), writes_rx, stop_rx);
        runner.spawn();

        // Create a task that will take some time to process
        let (slow_done_tx, slow_done_rx) = bounded(1);
        let slow_task = Task {
            entries: vec![],
            done: Some(slow_done_tx),
            tx_id: 1,
            durability: Durability::default(),
        };

        // Send the slow task
        writes_tx.send(slow_task).await.unwrap();

        // Send stop signal immediately
        stop_tx.send(()).await.unwrap();

        // Wait for TaskRunner to finish
        done_rx
            .recv()
            .await
            .expect("TaskRunner should signal completion");

        // Verify the slow task was completed
        assert!(slow_done_rx.recv().await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stop_task_runner_concurrent_tasks() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Create a new store instance
        let store = Store::new(opts).expect("should create store");

        let (writes_tx, writes_rx) = bounded(1000); // Increased buffer size
        let (stop_tx, stop_rx) = bounded(1);
        let core = &store.inner.as_ref().unwrap().core;

        let (runner, finish_rx) = TaskRunner::new(core.clone(), writes_rx, stop_rx);
        runner.spawn();

        let task_counter = Arc::new(AtomicU64::new(0));
        let total_tasks = 1000;

        // First, send all tasks before stopping
        for i in 0..total_tasks {
            let (done_tx, done_rx) = bounded(1);
            writes_tx
                .send(Task {
                    entries: vec![],
                    done: Some(done_tx),
                    tx_id: i,
                    durability: Durability::default(),
                })
                .await
                .expect("should send task");

            let task_counter = task_counter.clone();
            tokio::spawn(async move {
                done_rx.recv().await.unwrap().unwrap();
                task_counter.fetch_add(1, Ordering::SeqCst);
            });
        }

        // Give some time for tasks to be processed
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Send stop signal
        stop_tx.send(()).await.expect("should send stop signal");

        // Wait for TaskRunner to finish
        finish_rx
            .recv()
            .await
            .expect("TaskRunner should signal completion");

        // Give some time for all task counters to be updated
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Check that all tasks that were sent were processed
        let final_count = task_counter.load(Ordering::SeqCst);
        assert_eq!(
            final_count, total_tasks,
            "Expected {} tasks to be processed, but got {}",
            total_tasks, final_count
        );
    }
}
