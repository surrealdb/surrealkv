use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};

use super::entry::{TxRecord, ValueRef};
use super::store::Core;
use crate::storage::index::art::TrieError;
use crate::storage::index::art::KV;
use crate::storage::index::VectorKey;
use crate::storage::kv::entry::{
    Entry, MD_SIZE, VALUE_LENGTH_SIZE, VALUE_OFFSET_SIZE, VERSION_SIZE,
};
use crate::storage::kv::error::{Error, Result};
use crate::storage::kv::snapshot::Snapshot;
use crate::storage::kv::util::now;

/// An MVCC transaction mode.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Mode {
    /// A read-write transaction.
    ReadWrite,
    /// A read-only transaction.
    ReadOnly,
    /// A Write-only transaction.
    WriteOnly,
}

impl Mode {
    /// Checks whether the transaction mode can mutate data.
    pub(crate) fn mutable(&self) -> bool {
        match self {
            Self::ReadWrite => true,
            Self::ReadOnly => false,
            Self::WriteOnly => true,
        }
    }

    pub(crate) fn is_write_only(&self) -> bool {
        match self {
            Self::WriteOnly => true,
            _ => false,
        }
    }

    pub(crate) fn is_read_only(&self) -> bool {
        match self {
            Self::ReadOnly => true,
            _ => false,
        }
    }
}

pub struct Transaction {
    /// The read timestamp of the transaction.
    pub(crate) read_ts: u64,

    /// The transaction mode.
    mode: Mode,

    /// The snapshot that the transaction is running in.
    pub(crate) snapshot: Snapshot,

    // Reusable buffer for encoding transaction records.
    buf: BytesMut,

    /// The underlying store for the transaction. Shared between transactions using a mutex.
    pub(crate) store: Arc<Core>,

    /// The pending writes for the transaction.
    pub(crate) write_set: HashMap<Bytes, Entry>,

    // The keys that are read in the transaction from the snapshot.
    pub(crate) read_set: Mutex<Vec<(Bytes, u64)>>,

    // The offsets of values in the transaction post commit to the transaction log.
    committed_values_offsets: HashMap<Bytes, usize>,

    // The transaction is closed.
    closed: bool,
}

impl Transaction {
    /// Prepare a new transaction in the given mode.
    pub fn new(store: Arc<Core>, mode: Mode) -> Result<Self> {
        // TODO!! This should be the max txID of the index, get this from oracle
        let read_ts = store.oracle.read_ts();
        let snapshot = Snapshot::take(store.clone(), read_ts)?;

        Ok(Self {
            read_ts,
            mode,
            snapshot,
            buf: BytesMut::new(),
            store,
            write_set: HashMap::new(),
            read_set: Mutex::new(Vec::new()),
            committed_values_offsets: HashMap::new(),
            closed: false,
        })
    }

    /// Returns the transaction mode.
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// Adds a key-value pair to the store.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let entry = Entry::new(key, value);
        self.write(entry)?;
        Ok(())
    }

    /// Deletes a key from the store.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let value = Bytes::new();
        let mut entry = Entry::new(key, &value);
        entry.mark_delete();
        self.write(entry)?;
        Ok(())
    }

    /// Gets a value for a key if it exists.
    pub fn get(&self, key: &[u8]) -> Result<ValueRef> {
        if self.closed {
            return Err(Error::TxnClosed);
        }
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        // Read Your Own Writes (RYOW) semantics.

        // Check if the key is in the snapshot.
        let key = Bytes::copy_from_slice(key);
        match self.snapshot.get(&key[..].into()) {
            Ok(val_ref) => {
                // If the transaction is not read-only and the value reference has a timestamp greater than 0,
                // add the key and its timestamp to the read set for conflict detection.
                if !self.mode.is_read_only() && val_ref.ts > 0 {
                    self.read_set.lock().push((key, val_ref.ts));
                }

                Ok(val_ref)
            }
            Err(e) => {
                match &e {
                    Error::Index(trie_error) => {
                        if let TrieError::KeyNotFound = trie_error {
                            // If the transaction is not read-only, add the key to the read set.
                            // In snapshot isolation mode, this key could be added by another transaction,
                            // and keeping track of this key helps detect conflicts.
                            if !self.mode.is_read_only() {
                                self.read_set.lock().push((key, 0));
                            }
                        }
                        Err(e)
                    }
                    _ => Err(e),
                }
            }
        }
    }

    /// Writes a value for a key. None is used for deletion.
    fn write(&mut self, e: Entry) -> Result<()> {
        if !self.mode.mutable() {
            return Err(Error::TxnReadOnly);
        }
        if self.closed {
            return Err(Error::TxnClosed);
        }
        if e.key.is_empty() {
            return Err(Error::EmptyKey);
        }
        if e.key.len() as u64 > self.store.opts.max_key_size {
            return Err(Error::MaxKeyLengthExceeded);
        }
        if e.value.len() as u64 > self.store.opts.max_value_size {
            return Err(Error::MaxValueLengthExceeded);
        }

        if !self.mode.is_write_only() {
            // Convert to Bytes
            let indexed_value: Vec<u8> =
                vec![0; VERSION_SIZE + VALUE_LENGTH_SIZE + VALUE_OFFSET_SIZE + MD_SIZE + MD_SIZE];
            let indexed_value_bytes = Bytes::from(indexed_value);
            self.snapshot.set(&e.key[..].into(), indexed_value_bytes)?;
        }

        // Add the entry to pending writes
        self.write_set.insert(e.key.clone(), e);

        Ok(())
    }

    // precommit the transaction to WAL
    pub fn precommit(&mut self) -> Result<()> {
        if self.store.opts.wal_disabled {
            return Ok(());
        }

        todo!();
    }

    /// Commits the transaction, by writing all pending entries to the store.
    pub fn commit(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::TxnClosed);
        }

        if self.mode.is_read_only() {
            return Err(Error::TxnReadOnly);
        }

        if self.write_set.is_empty() {
            return Ok(());
        }

        // TODO: Use a commit pipeline to avoid blocking calls.
        // Lock the oracle to serialize commits to the transaction log.
        let oracle = self.store.oracle.clone();
        let _lock = oracle.write_lock.lock();

        // Prepare for the commit
        let (tx_id, commit_ts) = self.prepare_commit()?;

        // Add transaction records to the log
        self.add_to_transaction_log(tx_id, commit_ts)?;

        // Commit to the store index
        self.commit_to_index(tx_id, commit_ts)?;

        oracle.committed_upto(tx_id);

        Ok(())
    }

    /// Prepares for the commit by assigning commit timestamps and preparing records.
    fn prepare_commit(&mut self) -> Result<(u64, u64)> {
        let oracle = self.store.oracle.clone();
        let tx_id = oracle.new_commit_ts(self)?;
        let commit_ts = self.assign_commit_ts();
        Ok((tx_id, commit_ts))
    }

    /// Assigns commit timestamps to transaction entries.
    fn assign_commit_ts(&mut self) -> u64 {
        let commit_ts = now();
        self.write_set.iter_mut().for_each(|(_, entry)| {
            entry.ts = commit_ts;
        });
        commit_ts
    }

    /// Adds transaction records to the transaction log.
    fn add_to_transaction_log(&mut self, tx_id: u64, commit_ts: u64) -> Result<u64> {
        let current_offset = self.store.clog.read().offset()?;
        let entries: Vec<Entry> = self.write_set.values().cloned().collect();
        let tx_record = TxRecord::new_with_entries(entries, tx_id, commit_ts);
        tx_record.encode(
            &mut self.buf,
            current_offset,
            &mut self.committed_values_offsets,
        )?;

        let mut clog = self.store.clog.write();
        let (tx_offset, _) = clog.append(self.buf.as_ref())?;
        Ok(tx_offset)
    }

    /// Commits transaction changes to the store index.
    fn commit_to_index(&mut self, tx_id: u64, commit_ts: u64) -> Result<()> {
        let mut index = self.store.indexer.write();
        let mut kv_pairs = self.build_kv_pairs(tx_id, commit_ts);

        index.bulk_insert(&mut kv_pairs)?;
        Ok(())
    }

    fn build_kv_pairs(&self, tx_id: u64, commit_ts: u64) -> Vec<KV<VectorKey, Bytes>> {
        let mut kv_pairs: Vec<KV<VectorKey, Bytes>> = Vec::new();

        for (_, entry) in self.write_set.iter() {
            let index_value = self.build_index_value(entry);

            kv_pairs.push(KV {
                key: entry.key[..].into(),
                value: index_value,
                version: tx_id,
                ts: commit_ts,
            });
        }

        kv_pairs
    }

    fn build_index_value(&self, entry: &Entry) -> Bytes {
        let index_value = ValueRef::encode(
            &entry.key,
            &entry.value,
            entry.metadata.as_ref(),
            &self.committed_values_offsets,
            self.store.opts.max_value_threshold,
        );
        index_value
    }

    /// Rolls back the transaction, by removing all updated entries.
    pub fn rollback(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::TxnClosed);
        }

        self.closed = true;
        self.committed_values_offsets.clear();
        self.buf.clear();
        self.write_set.clear();
        self.read_set.lock().clear();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::sync::Arc;

    use crate::storage::kv::entry::TxRecord;
    use crate::storage::kv::error::Error;
    use crate::storage::kv::option::Options;
    use crate::storage::kv::reader::{Reader, TxReader};
    use crate::storage::kv::store::Core;
    use crate::storage::kv::store::Store;
    use crate::storage::kv::transaction::{Mode, Transaction};
    use crate::storage::log::aol::aol::AOL;
    use crate::storage::log::Options as LogOptions;

    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    #[test]
    fn test_basic_transaction() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Create a new Core instance with VectorKey as the key type
        let store = Store::new(opts).expect("should create store");

        // Define key-value pairs for the test
        let key1 = Bytes::from("foo1");
        let key2 = Bytes::from("foo2");
        let value1 = Bytes::from("baz");
        let value2 = Bytes::from("bar");

        {
            // Start a new read-write transaction (txn1)
            let mut txn1 = store.begin().unwrap();
            txn1.set(&key1, &value1).unwrap();
            txn1.set(&key2, &value1).unwrap();
            txn1.commit().unwrap();
        }

        {
            // Start another read-write transaction (txn2)
            let mut txn2 = store.begin().unwrap();
            txn2.set(&key1, &value2).unwrap();
            txn2.set(&key2, &value2).unwrap();
            txn2.commit().unwrap();
        }

        // Drop the store to simulate closing it
        drop(store);

        // Open an AOL (Append-Only Log) from the temporary directory
        let a = AOL::open(temp_dir.path(), &LogOptions::default()).expect("should create aol");

        // Create a reader for the AOL
        let r = Reader::new_from(a, 0, 10000).unwrap();
        let mut txr = TxReader::new(r).unwrap();

        // Read and assert the two transaction records from the reader
        for i in 1..3 {
            let mut tx = TxRecord::new(2);
            txr.read_into(&mut tx).unwrap();
            assert_eq!(tx.header.id, i);
            assert_eq!(tx.entries.len(), 2);
        }

        // Create a new Core instance with VectorKey after dropping the previous one
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        let store = Store::new(opts).expect("should create store");

        // Start a read-only transaction (txn3)
        let txn3 = store.begin().unwrap();
        let val = txn3.get(&key1).unwrap();

        // Assert that the value retrieved in txn3 matches value2_clone
        assert_eq!(val.value.unwrap().as_ref(), value2.as_ref());
    }

    #[test]
    fn test_mvcc_snapshot_isolation() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        let store = Arc::new(Core::new(opts).expect("should create store"));

        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");
        let value1 = Bytes::from("baz");
        let value2 = Bytes::from("bar");

        // no read conflict
        {
            let mut txn1 = Transaction::new(store.clone(), Mode::ReadWrite).unwrap();
            let mut txn2 = Transaction::new(store.clone(), Mode::ReadWrite).unwrap();

            txn1.set(&key1, &value1).unwrap();
            txn1.commit().unwrap();

            assert!(txn2.get(&key2).is_err());
            txn2.set(&key2, &value2).unwrap();
            txn2.commit().unwrap();
        }

        // blind writes should succeed if key wasn't read first
        {
            let mut txn1 = Transaction::new(store.clone(), Mode::ReadWrite).unwrap();
            let mut txn2 = Transaction::new(store.clone(), Mode::ReadWrite).unwrap();

            txn1.set(&key1, &value1).unwrap();
            txn2.set(&key1, &value2).unwrap();

            txn1.commit().unwrap();
            txn2.commit().unwrap();

            let txn3 = Transaction::new(store.clone(), Mode::ReadOnly).unwrap();
            let val = txn3.get(&key1).unwrap();
            assert_eq!(val.value.unwrap().as_ref(), value2.as_ref());
        }

        {
            let key = Bytes::from("key3");

            let mut txn1 = Transaction::new(store.clone(), Mode::ReadWrite).unwrap();
            let mut txn2 = Transaction::new(store.clone(), Mode::ReadWrite).unwrap();

            txn1.set(&key, &value1).unwrap();
            txn1.commit().unwrap();

            assert!(txn2.get(&key).is_err());
            txn2.set(&key, &value1).unwrap();
            assert!(match txn2.commit() {
                Err(err) => {
                    if let Error::TxnReadConflict = err {
                        true
                    } else {
                        false
                    }
                }
                _ => false,
            });
        }

        {
            let key = Bytes::from("key4");

            let mut txn1 = Transaction::new(store.clone(), Mode::ReadWrite).unwrap();
            txn1.set(&key, &value1).unwrap();
            txn1.commit().unwrap();

            let mut txn2 = Transaction::new(store.clone(), Mode::ReadWrite).unwrap();
            let mut txn3 = Transaction::new(store, Mode::ReadWrite).unwrap();

            txn2.delete(&key).unwrap();
            assert!(txn2.commit().is_ok());

            assert!(txn3.get(&key).is_ok());
            txn3.set(&key, &value2).unwrap();
            assert!(match txn3.commit() {
                Err(err) => {
                    if let Error::TxnReadConflict = err {
                        true
                    } else {
                        false
                    }
                }
                _ => false,
            });
        }
    }
}
