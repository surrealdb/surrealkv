use std::sync::Arc;
use std::vec;
use std::{num::NonZeroUsize, sync::atomic::AtomicBool};

use bytes::Bytes;
use hashbrown::HashMap;
use lru::LruCache;
use parking_lot::RwLock;

use crate::storage::{
    index::art::KV,
    kv::{
        entry::{TxRecord, ValueRef},
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
#[derive(Clone)]
pub struct Store {
    pub(crate) core: Arc<Core>,
}

impl Store {
    /// Creates a new MVCC key-value store with the given options.
    /// It creates a new core with the options and wraps it in an atomic reference counter.
    /// It returns the store.
    pub fn new(opts: Options) -> Result<Self> {
        let core = Arc::new(Core::new(opts)?);
        Ok(Self { core })
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
    pub fn write(self: Arc<Self>, f: impl FnOnce(&mut Transaction) -> Result<()>) -> Result<()> {
        let mut txn = self.begin_with_mode(Mode::ReadWrite)?;
        f(&mut txn)?;
        txn.commit()?;

        Ok(())
    }
}

impl Drop for Store {
    /// Drops the store by closing the core.
    /// If closing the core fails, it panics with an error message.
    fn drop(&mut self) {
        let err = self.core.close();
        if err.is_err() {
            panic!("failed to close core: {:?}", err);
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
    pub(crate) manifest: Aol,
    /// Transaction ID Oracle for store.
    pub(crate) oracle: Arc<Oracle>,
    /// Value cache for store.
    /// The assumption for this cache is that it could be useful for
    /// storing offsets that are frequently accessed (especially in
    /// the case of range scans)
    pub(crate) value_cache: Arc<RwLock<LruCache<u64, Bytes>>>,
    is_closed: AtomicBool,
}

impl Core {
    /// Creates a new Core with the given options.
    /// It initializes a new Indexer, opens or creates the manifest file,
    /// loads or creates metadata from the manifest file, updates the options with the loaded metadata,
    /// opens or creates the commit log file, loads the index from the commit log if it exists, creates
    /// and initializes an Oracle, creates and initializes a value cache, and constructs and returns
    /// the Core instance.
    pub fn new(opts: Options) -> Result<Self> {
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
        let cache_size = NonZeroUsize::new(opts.max_value_cache_size as usize).unwrap();
        let value_cache = Arc::new(RwLock::new(LruCache::new(cache_size)));

        // Construct and return the Core instance.
        Ok(Self {
            indexer: RwLock::new(indexer),
            opts,
            manifest,
            clog: Arc::new(RwLock::new(clog)),
            oracle: Arc::new(oracle),
            value_cache,
            is_closed: AtomicBool::new(false),
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
                    if let Error::LogError(LogError::EOF(_)) = e {
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
                if let Error::LogError(LogError::EOF(_)) = e {
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

    fn close(&self) -> Result<()> {
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

        self.is_closed
            .store(true, std::sync::atomic::Ordering::Relaxed);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::kv::option::Options;
    use crate::storage::kv::store::Store;

    use bytes::Bytes;
    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    #[test]
    fn bulk_insert() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Create a new store instance with VariableKey as the key type
        let store = Store::new(opts).expect("should create store");

        // Number of keys to generate
        let num_keys = 10000;

        // Initialize a counter
        let mut counter = 0u32;

        // Create a vector to store the generated keys
        let mut keys: Vec<Bytes> = Vec::new();

        for _ in 1..=num_keys {
            // Convert the counter to Bytes
            let key_bytes = Bytes::from(counter.to_le_bytes().to_vec());

            // Increment the counter
            counter += 1;

            // Add the key to the vector
            keys.push(key_bytes);
        }

        let default_value = Bytes::from("default_value".to_string());

        // Write the keys to the store
        for (_, key) in keys.iter().enumerate() {
            // Start a new write transaction
            let mut txn = store.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().unwrap();
        }

        // Read the keys to the store
        for (_, key) in keys.iter().enumerate() {
            // Start a new read transaction
            let txn = store.begin().unwrap();
            let val = txn.get(key).unwrap();
            // Assert that the value retrieved in txn3 matches default_value
            assert_eq!(val, default_value.as_ref());
        }

        // Drop the store to simulate closing it
        drop(store);

        // Create a new Core instance with VariableKey after dropping the previous one
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_value_threshold = 3;

        let store = Store::new(opts).expect("should create store");

        // Read the keys to the store
        for (_, key) in keys.iter().enumerate() {
            // Start a new read transaction
            let txn = store.begin().unwrap();
            let val = txn.get(key).unwrap();
            // Assert that the value retrieved in txn matches default_value
            assert_eq!(val, default_value.as_ref());
        }
    }

    #[test]
    fn store_open_and_reload_options() {
        // // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Create a new store instance with VariableKey as the key type
        let store = Store::new(opts.clone()).expect("should create store");

        drop(store);

        // Update the options and use them to update the new store instance
        let mut opts = opts.clone();
        opts.max_active_snapshots = 10;
        opts.max_value_cache_size = 5;

        let store = Store::new(opts.clone()).expect("should create store");
        let store_opts = store.core.opts.clone();
        assert_eq!(store_opts, opts);
    }

    #[test]
    fn clone_store() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Create a new store instance with VariableKey as the key type
        let store = Store::new(opts).expect("should create store");

        // Number of keys to generate
        let num_keys = 100;

        // Initialize a counter
        let mut counter = 0u32;

        // Create a vector to store the generated keys
        let mut keys: Vec<Bytes> = Vec::new();

        for _ in 1..=num_keys {
            // Convert the counter to Bytes
            let key_bytes = Bytes::from(counter.to_le_bytes().to_vec());

            // Increment the counter
            counter += 1;

            // Add the key to the vector
            keys.push(key_bytes);
        }

        let default_value = Bytes::from("default_value".to_string());
        let store1 = store.clone();

        // Write the keys to the store
        for (_, key) in keys.iter().enumerate() {
            // Start a new write transaction
            let mut txn = store1.begin().unwrap();
            txn.set(key, &default_value).unwrap();
            txn.commit().unwrap();
        }
    }
}
