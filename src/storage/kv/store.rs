use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;

use crate::storage::index::art::KV;
use crate::storage::index::VectorKey;
use crate::storage::kv::entry::{TxRecord, ValueRef};
use crate::storage::kv::error::{Error, Result};
use crate::storage::kv::indexer::Indexer;
use crate::storage::kv::option::Options;
use crate::storage::kv::oracle::Oracle;
use crate::storage::kv::reader::{Reader, TxReader};
use crate::storage::kv::transaction::{Mode, Transaction};
use crate::storage::log::aol::aol::AOL;
use crate::storage::log::wal::wal::WAL;
use crate::storage::log::Error as LogError;
use crate::storage::log::{Options as LogOptions, BLOCK_SIZE};

/// An MVCC-based transactional key-value store.
pub struct Store {
    pub(crate) core: Arc<Core>,
}

impl Store {
    /// Creates a new MVCC key-value store with the given key-value store for storage.
    pub fn new(opts: Options) -> Result<Self> {
        let core = Arc::new(Core::new(opts)?);
        Ok(Self { core })
    }

    pub fn begin(&self) -> Result<Transaction> {
        let mut txn = Transaction::new(self.core.clone(), Mode::ReadWrite)?;
        txn.read_ts = self.core.oracle.read_ts();
        Ok(txn)
    }

    pub fn begin_with_mode(&self, mode: Mode) -> Result<Transaction> {
        let mut txn = Transaction::new(self.core.clone(), mode)?;
        txn.read_ts = self.core.oracle.read_ts();
        Ok(txn)
    }

    pub fn view(&self, f: impl FnOnce(&mut Transaction) -> Result<()>) -> Result<()> {
        let mut txn = self.begin_with_mode(Mode::ReadOnly)?;
        f(&mut txn)?;

        Ok(())
    }

    pub fn write(self: Arc<Self>, f: impl FnOnce(&mut Transaction) -> Result<()>) -> Result<()> {
        let mut txn = self.begin_with_mode(Mode::ReadWrite)?;
        f(&mut txn)?;
        txn.commit()?;

        Ok(())
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        let err = self.core.close();
        if err.is_err() {
            panic!("failed to close core: {:?}", err);
        }
    }
}

pub struct Core {
    /// Index for store.
    pub(crate) indexer: RwLock<Indexer>,
    /// Options for store.
    pub(crate) opts: Options,
    /// WAL for store.
    pub(crate) wal: Arc<Option<WAL>>,
    /// Commit log for store.
    pub(crate) clog: Arc<RwLock<AOL>>,
    /// Transaction ID Oracle for store.
    pub(crate) oracle: Arc<Oracle>,
}

impl Core {
    pub fn new(opts: Options) -> Result<Self> {
        let mut indexer = Indexer::new(&opts);

        let topts = LogOptions::default();
        let clog = AOL::open(&opts.dir, &topts)?;
        if clog.size()? > 0 {
            Core::load_index(&opts, &mut indexer)?;
        }

        let oracle = Oracle::new(&opts);
        oracle.set_ts(indexer.version());

        Ok(Self {
            indexer: RwLock::new(indexer),
            opts,
            wal: Arc::new(None),
            clog: Arc::new(RwLock::new(clog)),
            oracle: Arc::new(oracle),
        })
    }

    fn load_index(opts: &Options, indexer: &mut Indexer) -> Result<()> {
        let clog = AOL::open(&opts.dir, &LogOptions::default())?;
        let reader = Reader::new_from(clog, 0, BLOCK_SIZE)?;
        let mut tx_reader = TxReader::new(reader)?;
        let mut tx = TxRecord::new(opts.max_tx_entries);

        loop {
            // Reset the transaction record before reading into it.
            // Keeping the same transaction record instance avoids
            // unnecessary allocations.
            tx.reset();

            // Read the next transaction record from the log.
            let res = tx_reader.read_into(&mut tx);
            if let Err(e) = res {
                if let Error::Log(log_error) = &e {
                    match log_error {
                        LogError::EOF => break,
                        _ => return Err(e),
                    }
                } else {
                    return Err(e);
                }
            }

            let value_offsets = res.unwrap();
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
        let mut kv_pairs: Vec<KV<VectorKey, Bytes>> = Vec::new();

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

    fn close(&self) -> Result<()> {
        self.indexer.write().close()?;
        // TODO: wait on oracle till latest txn commit ts
        self.clog.write().close()?;
        // TODO: close the wal
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
    fn test_bulk_insert() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Create a new store instance with VectorKey as the key type
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
            txn.set(&key, &default_value).unwrap();
            txn.commit().unwrap();
        }

        // Read the keys to the store
        for (_, key) in keys.iter().enumerate() {
            // Start a new read transaction
            let txn = store.begin().unwrap();
            let val = txn.get(&key).unwrap();
            // Assert that the value retrieved in txn3 matches default_value
            assert_eq!(val.value.unwrap().as_ref(), default_value.as_ref());
        }

        // Drop the store to simulate closing it
        drop(store);

        // Create a new Core instance with VectorKey after dropping the previous one
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        let store = Store::new(opts).expect("should create store");

        // Read the keys to the store
        for (_, key) in keys.iter().enumerate() {
            // Start a new read transaction
            let txn = store.begin().unwrap();
            let val = txn.get(&key).unwrap();
            // Assert that the value retrieved in txn matches default_value
            assert_eq!(val.value.unwrap().as_ref(), default_value.as_ref());
        }
    }
}
