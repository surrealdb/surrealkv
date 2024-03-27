use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::vec;

use async_channel::{bounded, Receiver, Sender};
use futures::{select, FutureExt};
use tokio::task::{spawn, JoinHandle};

use bytes::{Bytes, BytesMut};
use hashbrown::HashMap;
use parking_lot::RwLock;
use quick_cache::sync::Cache;
use tokio::sync::Mutex as AsyncMutex;
use vart::art::KV;

use crate::storage::{
    kv::{
        entry::{Entry, TxRecord, ValueRef},
        error::{Error, Result},
        indexer::Indexer,
        option::Options,
        oracle::Oracle,
        reader::{Reader, TxReader},
        repair::{repair_last_corrupted_segment, restore_repair_files},
        transaction::{Mode, Transaction},
    },
    log::{
        aof::log::Aol, write_field, Error as LogError, Metadata, MultiSegmentReader,
        Options as LogOptions, SegmentRef, BLOCK_SIZE,
    },
};

use super::transaction::Durability;

pub(crate) struct StoreInner {
    pub(crate) core: Arc<Core>,
    pub(crate) is_closed: AtomicBool,
    stop_tx: Sender<()>,
    task_runner_handle: Arc<AsyncMutex<Option<JoinHandle<()>>>>,
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
        let task_runner_handle = TaskRunner::new(core.clone(), writes_rx, stop_rx).spawn();

        Ok(Self {
            core,
            stop_tx,
            is_closed: AtomicBool::new(false),
            task_runner_handle: Arc::new(AsyncMutex::new(Some(task_runner_handle))),
        })
    }

    /// Closes the store. It sends a stop signal to the writer and waits for the done signal.
    pub async fn close(&self) -> Result<()> {
        if self.is_closed.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }

        // Send stop signal
        self.stop_tx
            .send(())
            .await
            .map_err(|e| Error::SendError(format!("{}", e)))?;

        // Wait for task to finish
        if let Some(handle) = self.task_runner_handle.lock().await.take() {
            handle.await.map_err(|e| {
                Error::ReceiveError(format!(
                    "Error occurred while closing the kv store. JoinError: {}",
                    e
                ))
            })?;
        }

        self.core.close()?;

        self.is_closed
            .store(true, std::sync::atomic::Ordering::Relaxed);

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
}

impl Drop for Store {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            // Close the store asynchronously
            tokio::spawn(async move {
                if let Err(err) = inner.close().await {
                    // TODO: use log/tracing instead of eprintln
                    eprintln!("Error occurred while closing the kv store: {}", err);
                }
            });
        }
    }
}

pub(crate) struct TaskRunner {
    core: Arc<Core>,
    writes_rx: Receiver<Task>,
    stop_rx: Receiver<()>,
}

impl TaskRunner {
    fn new(core: Arc<Core>, writes_rx: Receiver<Task>, stop_rx: Receiver<()>) -> Self {
        Self {
            core,
            writes_rx,
            stop_rx,
        }
    }

    fn spawn(self) -> JoinHandle<()> {
        spawn(Box::pin(async move {
            loop {
                select! {
                    req = self.writes_rx.recv().fuse() => {
                        let task = req.unwrap();
                        self.handle_task(task).await
                    },
                    _ = self.stop_rx.recv().fuse() => {
                        // Consume all remaining items in writes_rx
                        while let Ok(task) = self.writes_rx.try_recv() {
                            self.handle_task(task).await;
                        }
                        drop(self);
                        return;
                    },
                }
            }
        }))
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
    pub(crate) clog: Arc<RwLock<Aol>>,
    /// Manifest for store to track Store state.
    pub(crate) manifest: RwLock<Aol>,
    /// Transaction ID Oracle for store.
    pub(crate) oracle: Arc<Oracle>,
    /// Value cache for store.
    /// The assumption for this cache is that it should be useful for
    /// storing offsets that are frequently accessed (especially in
    /// the case of range scans)
    pub(crate) value_cache: Cache<u64, Bytes>,
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
    /// Commit timestamp
    commit_ts: u64,
    /// Durability
    durability: Durability,
}

impl Core {
    fn initialize_indexer() -> Indexer {
        Indexer::new()
    }

    // This function initializes the manifest log for the database to store all settings.
    fn initialize_manifest(opts: &Options) -> Result<Aol> {
        let manifest_subdir = opts.dir.join("manifest");
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

        // Determine options for the manifest file and open or create it.
        let mut manifest = Self::initialize_manifest(&opts)?;

        // Load or create metadata from the manifest file.
        let metadata = Core::load_or_create_metadata(&opts, &mut manifest)?;

        // Update options with the loaded metadata.
        let opts = Options::from_metadata(metadata, opts.dir.clone())?;

        // Determine options for the commit log file and open or create it.
        let mut clog = Self::initialize_clog(&opts)?;

        // Load the index from the commit log if it exists.
        if clog.size()? > 0 {
            Core::load_index(&opts, &mut clog, &mut indexer)?;
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
            manifest: RwLock::new(manifest),
            clog: Arc::new(RwLock::new(clog)),
            oracle: Arc::new(oracle),
            value_cache,
            is_closed: AtomicBool::new(false),
            writes_tx,
        })
    }

    pub(crate) fn read_ts(&self) -> Result<u64> {
        if self.is_closed() {
            return Err(Error::StoreClosed);
        }

        Ok(self.oracle.read_ts())
    }

    // The load_index function is responsible for loading the index from the log.
    fn load_index(opts: &Options, clog: &mut Aol, indexer: &mut Indexer) -> Result<()> {
        // The directory where the log segments are stored is determined.
        let clog_subdir = opts.dir.join("clog");

        // The segments are read from the directory.
        let sr = SegmentRef::read_segments_from_directory(clog_subdir.as_path())
            .expect("should read segments");

        // A MultiSegmentReader is created to read from multiple segments.
        let reader = MultiSegmentReader::new(sr)?;

        // A Reader is created from the MultiSegmentReader with the maximum segment size and block size.
        let reader = Reader::new_from(reader, opts.max_segment_size, BLOCK_SIZE);

        // A TxReader is created from the Reader to read transactions.
        let mut tx_reader = TxReader::new(reader);

        // A TxRecord is created to hold the transactions. The maximum number of entries per transaction is specified.
        let mut tx = TxRecord::new(opts.max_entries_per_txn as usize);

        // An Option is created to hold the segment ID and offset in case of corruption.
        let mut corruption_info: Option<(u64, u64)> = None;

        // A loop is started to read transactions.
        loop {
            // The TxRecord is reset for each iteration.
            tx.reset();

            // The TxReader attempts to read into the TxRecord.
            match tx_reader.read_into(&mut tx) {
                // If the read is successful, the entries are processed.
                Ok(value_offsets) => Core::process_entries(&tx, opts, &value_offsets, indexer)?,

                // If the end of the file is reached, the loop is broken.
                Err(Error::LogError(LogError::Eof(_))) => break,

                // If a corruption error is encountered, the segment ID and offset are stored and the loop is broken.
                Err(Error::LogError(LogError::Corruption(err))) => {
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
            repair_last_corrupted_segment(
                clog,
                opts.max_entries_per_txn as usize,
                corrupted_segment_id,
                corrupted_offset,
            )?;
        }

        Ok(())
    }

    fn process_entries(
        tx: &TxRecord,
        opts: &Options,
        value_offsets: &HashMap<Bytes, usize>,
        indexer: &mut Indexer,
    ) -> Result<()> {
        let mut kv_pairs: Vec<KV<vart::VariableSizeKey, Bytes>> = tx
            .entries
            .iter()
            .map(|entry| {
                let index_value = ValueRef::encode(
                    &entry.key,
                    &entry.value,
                    entry.metadata.as_ref(),
                    value_offsets,
                    opts.max_value_threshold,
                );

                KV {
                    key: entry.key[..].into(),
                    value: index_value,
                    version: tx.header.id,
                    ts: tx.header.ts,
                }
            })
            .collect();

        indexer.bulk_insert(&mut kv_pairs)
    }

    fn load_or_create_metadata(opts: &Options, manifest: &mut Aol) -> Result<Metadata> {
        let current_metadata = opts.to_metadata();

        let existing_metadata = match manifest.size()? {
            0 => Core::load_manifest(opts)?,
            _ => None,
        };

        if let Some(existing) = &existing_metadata {
            if *existing == current_metadata {
                return Ok(current_metadata);
            }
        }

        let md_bytes = current_metadata.to_bytes()?;
        let mut buf = Vec::new();
        write_field(&md_bytes, &mut buf)?;
        manifest.append(&buf)?;

        Ok(current_metadata)
    }

    fn load_manifest(opts: &Options) -> Result<Option<Metadata>> {
        let manifest_subdir = opts.dir.join("manifest");
        let sr = SegmentRef::read_segments_from_directory(manifest_subdir.as_path())
            .expect("should read segments");
        let reader = MultiSegmentReader::new(sr)?;
        let mut reader = Reader::new_from(reader, 0, BLOCK_SIZE);

        let mut md: Option<Metadata> = None; // Initialize with None

        loop {
            // Read the next transaction record from the log.
            let mut len_buf = [0; 4];
            let res = reader.read(&mut len_buf); // Read 4 bytes for the length
            if let Err(e) = res {
                if let Error::LogError(LogError::Eof(_)) = e {
                    break;
                } else {
                    return Err(e);
                }
            }

            let len = u32::from_be_bytes(len_buf) as usize; // Convert bytes to length
            let mut md_bytes = vec![0u8; len];
            reader.read(&mut md_bytes)?; // Read the actual metadata
            md = Some(Metadata::new(Some(md_bytes))); // Update md with the new metadata
        }

        Ok(md)
    }

    fn is_closed(&self) -> bool {
        self.is_closed.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn close(&self) -> Result<()> {
        if self.is_closed() {
            return Ok(());
        }

        // Wait for the oracle to catch up to the latest commit transaction.
        let oracle = self.oracle.clone();
        let last_commit_ts = oracle.read_ts();
        oracle.wait_for(last_commit_ts);

        // Close the indexer
        self.indexer.write().close()?;

        // Close the commit log
        self.clog.write().close()?;

        // Close the manifest
        self.manifest.write().close()?;

        self.is_closed
            .store(true, std::sync::atomic::Ordering::Relaxed);

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

        let current_offset = self.clog.read().offset()?;
        let tx_record = TxRecord::new_with_entries(req.entries.clone(), req.tx_id, req.commit_ts);
        let mut buf = BytesMut::new();
        let mut committed_values_offsets = HashMap::new();

        tx_record.encode(&mut buf, current_offset, &mut committed_values_offsets)?;

        self.append_to_log(&buf, req.durability)?;
        self.write_to_index(&req, &committed_values_offsets)?;

        Ok(())
    }

    fn append_to_log(&self, tx_record: &BytesMut, durability: Durability) -> Result<()> {
        let mut clog = self.clog.write();

        match durability {
            Durability::Immediate => {
                // Immediate durability means that the transaction is made to
                // fsync the data to disk before returning.
                clog.append(tx_record)?;
                clog.sync()?;
            }
            Durability::Eventual => {
                clog.append(tx_record)?;
            }
        }

        Ok(())
    }

    fn write_to_index(
        &self,
        req: &Task,
        committed_values_offsets: &HashMap<Bytes, usize>,
    ) -> Result<()> {
        let mut index = self.indexer.write();
        let mut kv_pairs = Vec::new();

        for entry in &req.entries {
            let index_value = ValueRef::encode(
                &entry.key,
                &entry.value,
                entry.metadata.as_ref(),
                committed_values_offsets,
                self.opts.max_value_threshold,
            );

            kv_pairs.push(KV {
                key: entry.key[..].into(),
                value: index_value,
                version: req.tx_id,
                ts: req.commit_ts,
            });
        }

        index.bulk_insert(&mut kv_pairs)?;

        Ok(())
    }

    pub(crate) async fn send_to_write_channel(
        &self,
        entries: Vec<Entry>,
        tx_id: u64,
        commit_ts: u64,
        durability: Durability,
    ) -> Result<Receiver<Result<()>>> {
        let (tx, rx) = bounded(1);
        let req = Task {
            entries,
            done: Some(tx),
            tx_id,
            commit_ts,
            durability,
        };
        self.writes_tx.send(req).await?;
        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use rand::prelude::SliceRandom;
    use rand::Rng;
    use std::sync::Arc;

    use crate::storage::kv::option::Options;
    use crate::storage::kv::store::{Store, Task, TaskRunner};
    use crate::storage::kv::transaction::Durability;

    use async_channel::bounded;
    use std::sync::atomic::{AtomicU64, Ordering};

    use bytes::Bytes;
    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    #[tokio::test]
    async fn bulk_insert() {
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
            let txn = store.begin().unwrap();
            let val = txn.get(key).unwrap().unwrap();
            // Assert that the value retrieved in txn3 matches default_value
            assert_eq!(val, default_value.as_ref());
        }

        // Drop the store to simulate closing it
        store.close().await.unwrap();

        // Create a new Core instance with VariableKey after dropping the previous one
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_value_threshold = 3;

        let store = Store::new(opts).expect("should create store");

        // Read the keys to the store
        for key in keys.iter() {
            // Start a new read transaction
            let txn = store.begin().unwrap();
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

        drop(store);

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
                let txn = store.begin().unwrap();
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

    #[tokio::test]
    async fn stop_task_runner() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Create a new store instance with VariableKey as the key type
        let store = Store::new(opts).expect("should create store");

        let (writes_tx, writes_rx) = bounded(100);
        let (stop_tx, stop_rx) = bounded(1);
        let core = &store.inner.as_ref().unwrap().core;

        let runner = TaskRunner::new(core.clone(), writes_rx, stop_rx);
        let fut = runner.spawn();

        // Send some tasks
        let task_counter = Arc::new(AtomicU64::new(0));
        for i in 0..100 {
            let (done_tx, done_rx) = bounded(1);
            writes_tx
                .send(Task {
                    entries: vec![],
                    done: Some(done_tx),
                    tx_id: i,
                    commit_ts: i,
                    durability: Durability::default(),
                })
                .await
                .unwrap();

            let task_counter = Arc::clone(&task_counter);
            tokio::spawn(async move {
                done_rx.recv().await.unwrap().unwrap();
                task_counter.fetch_add(1, Ordering::SeqCst);
            });
        }

        // Send stop signal
        stop_tx.send(()).await.unwrap();

        // Wait for a while to let TaskRunner handle all tasks by waiting on done_rx
        fut.await.expect("TaskRunner should finish");

        // Wait for the spawned tokio thread to finish
        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;

        // Check if all tasks were handled
        assert_eq!(task_counter.load(Ordering::SeqCst), 100);
    }

    async fn concurrent_task(store: Arc<Store>) {
        let mut txn = store.begin().unwrap();
        txn.set(b"dummy key", b"dummy value").unwrap();
        txn.commit().await.unwrap();
    }

    #[tokio::test]
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
            let txn = store.begin().unwrap();
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
            let txn = store.begin().unwrap();
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
            let txn = store.begin().unwrap();
            let val = txn.get(key).unwrap();

            assert_eq!(val.unwrap(), value);
        }
    }

    // This test is relevant today because unless the store is dropped, the data will not be persisted to disk.
    // Once the store automatically syncs the data to disk, this test will not verify the intended behaviour.
    #[tokio::test(flavor = "multi_thread")]
    async fn records_not_lost_when_store_is_dropped() {
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
            drop(store);
        }

        // Give some room for the store to close asynchronously
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        {
            // Reopen the store
            let store = Store::new(opts.clone()).expect("should create store");

            // Test that the item is still in the store
            let txn = store.begin().unwrap();
            let val = txn.get(key).unwrap();

            assert_eq!(val.unwrap(), value);
        }
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

    async fn test_durability(durability: Durability) {
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

        let store = Store::new(opts.clone()).expect("should create store");
        let txn = store.begin().unwrap();

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

    #[tokio::test(flavor = "multi_thread")]
    async fn eventual_durability() {
        test_durability(Durability::Eventual).await;
    }

    #[tokio::test]
    async fn immediate_durability() {
        test_durability(Durability::Immediate).await;
    }
}
