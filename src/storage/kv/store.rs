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
        transaction::{Mode, Transaction},
    },
    log::{
        aof::log::Aol,
        {write_field, Options as LogOptions, BLOCK_SIZE}, {Error as LogError, Metadata},
    },
};

/// An MVCC-based transactional key-value store.
pub struct Store {
    pub(crate) core: Arc<Core>,
    pub(crate) is_closed: AtomicBool,
    stop_tx: Sender<()>,
    task_runner_handle: Arc<AsyncMutex<Option<JoinHandle<()>>>>,
}

impl Store {
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

    /// Begins a new read-write transaction.
    /// It creates a new transaction with the core and read-write mode, and sets the read timestamp from the oracle.
    /// It returns the transaction.
    pub fn begin(&self) -> Result<Transaction> {
        let txn = Transaction::new(self.core.clone(), Mode::ReadWrite)?;
        Ok(txn)
    }

    /// Begins a new transaction with the given mode.
    /// It creates a new transaction with the core and the given mode, and sets the read timestamp from the oracle.
    /// It returns the transaction.
    pub fn begin_with_mode(&self, mode: Mode) -> Result<Transaction> {
        let txn = Transaction::new(self.core.clone(), mode)?;
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
        Ok(())
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
}

impl Core {
    /// Creates a new Core with the given options.
    /// It initializes a new Indexer, opens or creates the manifest file,
    /// loads or creates metadata from the manifest file, updates the options with the loaded metadata,
    /// opens or creates the commit log file, loads the index from the commit log if it exists, creates
    /// and initializes an Oracle, creates and initializes a value cache, and constructs and returns
    /// the Core instance.
    pub fn new(opts: Options, writes_tx: Sender<Task>) -> Result<Self> {
        // Initialize a new Indexer with the provided options.
        let mut indexer = Indexer::new(&opts);

        // Determine options for the manifest file and open or create it.
        let manifest_subdir = opts.dir.join("manifest");
        let mopts = LogOptions::default().with_file_extension("manifest".to_string());
        let mut manifest = Aol::open(&manifest_subdir, &mopts)?;

        // Load or create metadata from the manifest file.
        let metadata = Core::load_or_create_metadata(&opts, &mopts, &mut manifest)?;

        // Update options with the loaded metadata.
        let opts = Options::from_metadata(metadata, opts.dir.clone())?;

        // Determine options for the commit log file and open or create it.
        let clog_subdir = opts.dir.join("clog");
        let copts = LogOptions::default()
            .with_max_file_size(opts.max_segment_size)
            .with_file_extension("clog".to_string());
        let clog = Aol::open(&clog_subdir, &copts)?;

        // Load the index from the commit log if it exists.
        if clog.size()? > 0 {
            Core::load_index(&opts, &copts, &mut indexer)?;
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

    fn load_index(opts: &Options, copts: &LogOptions, indexer: &mut Indexer) -> Result<()> {
        let clog_subdir = opts.dir.join("clog");
        let clog = Aol::open(&clog_subdir, copts)?;
        let reader = Reader::new_from(clog, 0, BLOCK_SIZE)?;
        let mut tx_reader = TxReader::new(reader)?;
        let mut tx = TxRecord::new(opts.max_tx_entries as usize);

        loop {
            // Reset the transaction record before reading into it.
            // Keeping the same transaction record instance avoids
            // unnecessary allocations.
            tx.reset();

            // Read the next transaction record from the log.
            let value_offsets = match tx_reader.read_into(&mut tx) {
                Ok(value_offsets) => value_offsets,
                Err(e) => {
                    if let Error::LogError(LogError::Eof(_)) = e {
                        break;
                    } else {
                        return Err(e);
                    }
                }
            };

            Core::process_entries(&tx, opts, &value_offsets, indexer)?;
        }

        Ok(())
    }

    fn process_entries(
        tx: &TxRecord,
        opts: &Options,
        value_offsets: &HashMap<Bytes, usize>,
        indexer: &mut Indexer,
    ) -> Result<()> {
        let mut kv_pairs = Vec::new();

        for entry in &tx.entries {
            let index_value = ValueRef::encode(
                &entry.key,
                &entry.value,
                entry.metadata.as_ref(),
                value_offsets,
                opts.max_value_threshold,
            );

            kv_pairs.push(KV {
                key: entry.key[..].into(),
                value: index_value,
                version: tx.header.id,
                ts: tx.header.ts,
            });
        }

        indexer.bulk_insert(&mut kv_pairs)
    }

    fn load_or_create_metadata(
        opts: &Options,
        mopts: &LogOptions,
        manifest: &mut Aol,
    ) -> Result<Metadata> {
        let current_metadata = opts.to_metadata();
        let existing_metadata = if !manifest.size()? > 0 {
            Core::load_manifest(opts, mopts)?
        } else {
            None
        };

        match existing_metadata {
            Some(existing) if existing == current_metadata => Ok(current_metadata),
            _ => {
                let md_bytes = current_metadata.to_bytes()?;
                let mut buf = Vec::new();
                write_field(&md_bytes, &mut buf)?;
                manifest.append(&buf)?;

                Ok(current_metadata)
            }
        }
    }

    fn load_manifest(opts: &Options, mopts: &LogOptions) -> Result<Option<Metadata>> {
        let manifest_subdir = opts.dir.join("manifest");
        let mlog = Aol::open(&manifest_subdir, mopts)?;
        let mut reader = Reader::new_from(mlog, 0, BLOCK_SIZE)?;

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

        self.append_to_log(&buf)?;
        self.write_to_index(&req, &committed_values_offsets)?;

        Ok(())
    }

    fn append_to_log(&self, tx_record: &BytesMut) -> Result<()> {
        let mut clog = self.clog.write();
        clog.append(tx_record)?;

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
    ) -> Result<Receiver<Result<()>>> {
        let (tx, rx) = bounded(1);
        let req = Task {
            entries,
            done: Some(tx),
            tx_id,
            commit_ts,
        };
        self.writes_tx.send(req).await?;
        Ok(rx)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::storage::kv::option::Options;
    use crate::storage::kv::store::{Store, Task, TaskRunner};

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
        opts.max_active_snapshots = 10;
        opts.max_value_cache_size = 5;

        let store = Store::new(opts.clone()).expect("should create store");
        let store_opts = store.core.opts.clone();
        assert_eq!(store_opts, opts);
    }

    #[tokio::test]
    async fn insert_close_reopen() {
        for _ in 0..10 {
            // Create a temporary directory for testing
            let temp_dir = create_temp_directory();

            // Create store options with the test directory
            let mut opts = Options::new();
            opts.dir = temp_dir.path().to_path_buf();

            // Create a new store instance with VariableKey as the key type
            let store = Store::new(opts.clone()).expect("should create store");

            // Insert 1000 items into the store
            for i in 0..1000 {
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                let mut txn = store.begin().unwrap();
                txn.set(key.as_bytes(), value.as_bytes()).unwrap();
                txn.commit().await.unwrap();
            }

            // Close the store
            store.close().await.unwrap();

            // Reopen the store
            let store = Store::new(opts.clone()).expect("should create store");

            // Test that the items are still in the store
            for i in 0..1000 {
                let key = format!("key{}", i);
                let value = format!("value{}", i);
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
        let core = &store.core;

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
}
