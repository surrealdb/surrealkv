use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use hashbrown::HashMap;
use parking_lot::{Mutex, RwLock};

use super::store::Core;
use crate::storage::index::art::TrieError;
use crate::storage::index::art::KV;
use crate::storage::index::VectorKey;
use crate::storage::kv::entry::{Entry, TxRecord, Value, ValueRef};
use crate::storage::kv::error::{Error, Result};
use crate::storage::kv::snapshot::{FilterFn, Snapshot, FILTERS};
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
    pub(crate) snapshot: RwLock<Snapshot>,

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
        let read_ts = now();
        let snapshot = RwLock::new(Snapshot::take(store.clone(), read_ts)?);

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
    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        if self.closed {
            return Err(Error::TxnClosed);
        }
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        // Check if the key is in the snapshot.
        let key = Bytes::copy_from_slice(key);
        match self.snapshot.read().get(&key[..].into()) {
            Ok(val_ref) => {
                // If the transaction is not read-only and the value reference has a timestamp greater than 0,
                // add the key and its timestamp to the read set for conflict detection.

                // RYOW semantics: Read your own write
                if let Some(entry) = self.write_set.get(&key) {
                    return Ok(entry.value.clone().to_vec());
                }

                if !self.mode.is_read_only() && val_ref.ts() > 0 {
                    self.read_set.lock().push((key, val_ref.ts()));
                }
                val_ref.resolve()
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
            let index_value = ValueRef::encode_mem(&e.key, &e.value, e.metadata.as_ref());

            if let Some(md) = e.metadata.as_ref() {
                if md.deleted() {
                    self.snapshot.write().delete(&e.key[..].into())?;
                } else {
                    self.snapshot.write().set(&e.key[..].into(), index_value)?;
                }
            } else {
                self.snapshot.write().set(&e.key[..].into(), index_value)?;
            }
        }

        // Add the entry to pending writes
        self.write_set.insert(e.key.clone(), e);

        Ok(())
    }

    pub fn scan<'b, R>(&'b self, range: R) -> Result<Vec<(Vec<u8>, u64, u64)>>
    where
        R: RangeBounds<VectorKey> + 'b,
    {
        let iterator = self.snapshot.write().new_reader()?;
        let ranger = iterator.range(range);
        let mut results = Vec::new();

        'outer: for (key, value, version, ts) in ranger {
            let mut val_ref = ValueRef::new(self.store.clone());
            let val_bytes_ref: &Bytes = &value;
            val_ref.decode(*version, val_bytes_ref)?;

            for filter in &FILTERS {
                if filter.apply(&val_ref, self.read_ts).is_err() {
                    continue 'outer;
                }
            }

            // Only add the key to the read set if the timestamp is less than or equal to the
            // read timestamp. This is to prevent adding keys that are added during transaction.
            if val_ref.ts() <= self.read_ts {
                self.read_set.lock().push((
                    Bytes::copy_from_slice(&key[..&key.len() - 1]), // the keys in the tart leaf are terminated with a null byte
                    val_ref.ts,
                ));
            }

            let v = val_ref.resolve()?;
            results.push((v, *version, *ts));
        }

        Ok(results)
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

        self.closed = true;

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
        self.closed = true;
        self.committed_values_offsets.clear();
        self.buf.clear();
        self.write_set.clear();
        self.read_set.lock().clear();

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        let err = self.rollback();
        if err.is_err() {
            panic!("failed to drop transaction: {:?}", err);
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::sync::Arc;

    use super::*;
    use crate::storage::kv::option::Options;
    use crate::storage::kv::store::Store;

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
        let store = Store::new(opts.clone()).expect("should create store");

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
            // Start a read-only transaction (txn3)
            let txn3 = store.begin().unwrap();
            let val = txn3.get(&key1).unwrap();
            assert_eq!(val, value1.as_ref());
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

        // Create a new Core instance with VectorKey after dropping the previous one
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        let store = Store::new(opts).expect("should create store");

        // Start a read-only transaction (txn4)
        let txn4 = store.begin().unwrap();
        let val = txn4.get(&key1).unwrap();

        // Assert that the value retrieved in txn4 matches value2
        assert_eq!(val, value2.as_ref());
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

        // read conflict when the read key was updated by another transaction
        {
            let mut txn1 = Transaction::new(store.clone(), Mode::ReadWrite).unwrap();
            let mut txn2 = Transaction::new(store.clone(), Mode::ReadWrite).unwrap();

            txn1.set(&key1, &value1).unwrap();
            txn1.commit().unwrap();

            assert!(txn2.get(&key1).is_ok());
            txn2.set(&key1, &value2).unwrap();
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
            assert_eq!(val, value2.as_ref());
        }

        // read conflict when the read key was updated by another transaction
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

        // read conflict when the read key was deleted by another transaction
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

    #[test]
    fn test_basic_scan_single_key() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Create a new Core instance with VectorKey as the key type
        let store = Store::new(opts.clone()).expect("should create store");

        // Define key-value pairs for the test
        let keys_to_insert = vec![Bytes::from("key1")];

        for key in &keys_to_insert {
            let mut txn = store.begin().unwrap();
            txn.set(&key, &key).unwrap();
            txn.commit().unwrap();
        }

        // TODO: fix passing vector key to scan
        let range = VectorKey::from_str("key1")..=VectorKey::from_str("key3");

        let txn = store.begin().unwrap();
        let results = txn.scan(range).unwrap();
        assert_eq!(results.len(), keys_to_insert.len());
    }

    #[test]
    fn test_basic_scan_multiple_keys() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Create store options with the test directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Create a new Core instance with VectorKey as the key type
        let store = Store::new(opts.clone()).expect("should create store");

        // Define key-value pairs for the test
        let keys_to_insert = vec![
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
        ];

        for key in &keys_to_insert {
            let mut txn = store.begin().unwrap();
            txn.set(&key, &key).unwrap();
            txn.commit().unwrap();
        }

        // TODO: fix passing vector key to scan
        let range = VectorKey::from_str("key1")..=VectorKey::from_str("key3");

        let txn = store.begin().unwrap();
        let results = txn.scan(range).unwrap();
        assert_eq!(results.len(), keys_to_insert.len());
    }

    #[test]
    fn test_mvcc_snapshot_isolation_with_scan() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        let store = Arc::new(Core::new(opts).expect("should create store"));

        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");
        let key3 = Bytes::from("key3");
        let key4 = Bytes::from("key4");
        let value1 = Bytes::from("value1");
        let value2 = Bytes::from("value2");
        let value3 = Bytes::from("value3");
        let value4 = Bytes::from("value4");
        let value5 = Bytes::from("value5");
        let value6 = Bytes::from("value6");

        // read conflict when scan keys have been updated in another transaction
        {
            let mut txn1 = Transaction::new(store.clone(), Mode::ReadWrite).unwrap();

            txn1.set(&key1, &value1).unwrap();
            txn1.commit().unwrap();

            let mut txn2 = Transaction::new(store.clone(), Mode::ReadWrite).unwrap();
            let mut txn3 = Transaction::new(store.clone(), Mode::ReadWrite).unwrap();

            txn2.set(&key1, &value4).unwrap();
            txn2.set(&key2, &value2).unwrap();
            txn2.set(&key3, &value3).unwrap();
            txn2.commit().unwrap();

            let range = VectorKey::from_str("key1")..=VectorKey::from_str("key4");
            let results = txn3.scan(range).unwrap();
            assert_eq!(results.len(), 1);
            txn3.set(&key2, &value5).unwrap();
            txn3.set(&key3, &value6).unwrap();

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

        // read conflict when read keys are deleted by other transaction
        {
            let mut txn1 = Transaction::new(store.clone(), Mode::ReadWrite).unwrap();

            txn1.set(&key4, &value1).unwrap();
            txn1.commit().unwrap();

            let mut txn2 = Transaction::new(store.clone(), Mode::ReadWrite).unwrap();
            let mut txn3 = Transaction::new(store.clone(), Mode::ReadWrite).unwrap();

            txn2.delete(&key4).unwrap();
            txn2.commit().unwrap();

            let range = VectorKey::from_str("key1")..=VectorKey::from_str("key5");
            txn3.scan(range).unwrap();
            txn3.set(&key4, &value2).unwrap();

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

    #[test]
    fn test_ryow() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        let store = Store::new(opts.clone()).expect("should create store");
        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let key3 = Bytes::from("k3");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");

        {
            let mut txn = store.begin().unwrap();
            txn.set(&key1, &value1).unwrap();
            txn.commit().unwrap();
        }

        {
            // Start a new read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.set(&key1, &value2).unwrap();
            assert_eq!(txn.get(&key1).unwrap(), value2.as_ref());
            assert!(txn.get(&key3).is_err());
            txn.set(&key2, &value1).unwrap();
            assert_eq!(txn.get(&key2).unwrap(), value1.as_ref());
            txn.commit().unwrap();
        }
    }

    // Common setup logic for creating a store
    fn create_store() -> Store {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        let store = Store::new(opts.clone()).expect("should create store");

        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");
        // Start a new read-write transaction (txn)
        let mut txn = store.begin().unwrap();
        txn.set(&key1, &value1).unwrap();
        txn.set(&key2, &value2).unwrap();
        txn.commit().unwrap();

        store
    }

    // The following tests are taken from hermitage (https://github.com/ept/hermitage)
    // Specifically, the tests are derived from FoundationDB tests: https://github.com/ept/hermitage/blob/master/foundationdb.md

    // G0: Write Cycles (dirty writes)
    #[test]
    fn test_snapshot_isolation_g0() {
        let store = create_store();
        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let value3 = Bytes::from("v3");
        let value4 = Bytes::from("v4");
        let value5 = Bytes::from("v5");
        let value6 = Bytes::from("v6");

        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            assert!(txn1.get(&key1).is_ok());
            assert!(txn1.get(&key2).is_ok());
            assert!(txn2.get(&key1).is_ok());
            assert!(txn2.get(&key2).is_ok());

            txn1.set(&key1, &value3).unwrap();
            txn2.set(&key1, &value4).unwrap();

            txn1.set(&key2, &value5).unwrap();

            txn1.commit().unwrap();

            txn2.set(&key2, &value6).unwrap();
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
            let txn3 = store.begin().unwrap();
            let val1 = txn3.get(&key1).unwrap();
            assert_eq!(val1, value3.as_ref());
            let val2 = txn3.get(&key2).unwrap();
            assert_eq!(val2, value5.as_ref());
        }
    }

    // G1a: Aborted Reads (dirty reads, cascaded aborts)
    #[test]
    fn test_snapshot_isolation_g1a() {
        let store = create_store();
        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");
        let value3 = Bytes::from("v3");

        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            assert!(txn1.get(&key1).is_ok());

            txn1.set(&key1, &value3).unwrap();

            let range = VectorKey::from_str("k1")..=VectorKey::from_str("k3");
            let res = txn2.scan(range.clone()).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].0, value1);

            drop(txn1);

            let res = txn2.scan(range).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].0, value1);

            txn2.commit().unwrap();
        }

        {
            let txn3 = store.begin().unwrap();
            let val1 = txn3.get(&key1).unwrap();
            assert_eq!(val1, value1.as_ref());
            let val2 = txn3.get(&key2).unwrap();
            assert_eq!(val2, value2.as_ref());
        }
    }

    // G1b: Intermediate Reads (dirty reads)
    #[test]
    fn test_snapshot_isolation_g1b() {
        let store = create_store();

        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let value1 = Bytes::from("v1");
        let value3 = Bytes::from("v3");
        let value4 = Bytes::from("v4");

        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            assert!(txn1.get(&key1).is_ok());
            assert!(txn1.get(&key2).is_ok());

            txn1.set(&key1, &value3).unwrap();

            let range = VectorKey::from_str("k1")..=VectorKey::from_str("k3");
            let res = txn2.scan(range.clone()).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].0, value1);

            txn1.set(&key1, &value4).unwrap();
            txn1.commit().unwrap();

            let res = txn2.scan(range).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].0, value1);

            txn2.commit().unwrap();
        }
    }

    // G1c: Circular Information Flow (dirty reads)
    #[test]
    fn test_snapshot_isolation_g1c() {
        let store = create_store();

        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");
        let value3 = Bytes::from("v3");
        let value4 = Bytes::from("v4");

        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            assert!(txn1.get(&key1).is_ok());
            assert!(txn2.get(&key2).is_ok());

            txn1.set(&key1, &value3).unwrap();
            txn2.set(&key2, &value4).unwrap();

            assert_eq!(txn1.get(&key2).unwrap(), value2.as_ref());
            assert_eq!(txn2.get(&key1).unwrap(), value1.as_ref());

            txn1.commit().unwrap();
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
    }

    // PMP: Predicate-Many-Preceders
    #[test]
    fn test_snapshot_isolation_pmp() {
        let store = create_store();

        let key2 = Bytes::from("k2");
        let key3 = Bytes::from("k3");
        let value3 = Bytes::from("v3");

        {
            let txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            assert!(txn1.get(&key3).is_err());
            txn2.set(&key2, &value3).unwrap();
            txn2.commit().unwrap();

            assert!(txn1.get(&key3).is_err());
        }
    }

    // PMP-Write: Circular Information Flow (dirty reads)
    #[test]
    fn test_snapshot_isolation_pmp_write() {
        let store = create_store();

        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");
        let value3 = Bytes::from("v3");

        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            assert!(txn1.get(&key1).is_ok());
            txn1.set(&key1, &value3).unwrap();

            assert_eq!(txn2.get(&key1).unwrap(), value1.as_ref());
            assert_eq!(txn2.get(&key2).unwrap(), value2.as_ref());

            txn2.delete(&key2).unwrap();
            txn1.commit().unwrap();

            let range = VectorKey::from_str("k1")..=VectorKey::from_str("k3");
            let res = txn2.scan(range.clone()).unwrap();
            assert_eq!(res.len(), 1);
            assert_eq!(res[0].0, value1);

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
    }

    // P4: Lost Update
    #[test]
    fn test_snapshot_isolation_p4() {
        let store = create_store();

        let key1 = Bytes::from("k1");
        let value3 = Bytes::from("v3");

        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            assert!(txn1.get(&key1).is_ok());
            assert!(txn2.get(&key1).is_ok());

            txn1.set(&key1, &value3).unwrap();
            txn2.set(&key1, &value3).unwrap();

            txn1.commit().unwrap();

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
    }

    // G-single: Single Anti-dependency Cycles (read skew)
    #[test]
    fn test_snapshot_isolation_g_single() {
        let store = create_store();

        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");
        let value3 = Bytes::from("v3");
        let value4 = Bytes::from("v4");

        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            assert_eq!(txn1.get(&key1).unwrap(), value1.as_ref());
            assert_eq!(txn2.get(&key1).unwrap(), value1.as_ref());
            assert_eq!(txn2.get(&key2).unwrap(), value2.as_ref());
            txn2.set(&key1, &value3).unwrap();
            txn2.set(&key2, &value4).unwrap();

            txn2.commit().unwrap();

            assert_eq!(txn1.get(&key2).unwrap(), value2.as_ref());
            txn1.commit().unwrap();
        }
    }

    // G-single-write-1: Single Anti-dependency Cycles (read skew)
    #[test]
    fn test_snapshot_isolation_g_single_write_1() {
        let store = create_store();

        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");
        let value3 = Bytes::from("v3");
        let value4 = Bytes::from("v4");

        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            assert_eq!(txn1.get(&key1).unwrap(), value1.as_ref());
            assert_eq!(txn2.get(&key1).unwrap(), value1.as_ref());
            assert_eq!(txn2.get(&key2).unwrap(), value2.as_ref());
            txn2.set(&key1, &value3).unwrap();
            txn2.set(&key2, &value4).unwrap();

            txn2.commit().unwrap();

            txn1.delete(&key2).unwrap();
            assert!(txn1.get(&key2).is_err());
            assert!(match txn1.commit() {
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

    // G-single-write-2: Single Anti-dependency Cycles (read skew)
    #[test]
    fn test_snapshot_isolation_g_single_write_2() {
        let store = create_store();

        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");
        let value3 = Bytes::from("v3");
        let value4 = Bytes::from("v4");

        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            assert_eq!(txn1.get(&key1).unwrap(), value1.as_ref());
            assert_eq!(txn2.get(&key1).unwrap(), value1.as_ref());
            assert_eq!(txn2.get(&key2).unwrap(), value2.as_ref());

            txn2.set(&key1, &value3).unwrap();

            txn1.delete(&key2).unwrap();

            txn2.set(&key2, &value4).unwrap();

            drop(txn1);

            txn2.commit().unwrap();
        }
    }

    #[test]
    fn test_snapshot_isolation_g2_item() {
        let store = create_store();

        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");
        let value3 = Bytes::from("v3");
        let value4 = Bytes::from("v4");

        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            assert_eq!(txn1.get(&key1).unwrap(), value1.as_ref());
            assert_eq!(txn1.get(&key2).unwrap(), value2.as_ref());
            assert_eq!(txn2.get(&key1).unwrap(), value1.as_ref());
            assert_eq!(txn2.get(&key2).unwrap(), value2.as_ref());

            txn1.set(&key1, &value3).unwrap();
            txn2.set(&key2, &value4).unwrap();

            txn1.commit().unwrap();

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
    }
}
