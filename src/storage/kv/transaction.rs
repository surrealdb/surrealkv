use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use hashbrown::HashMap;
use parking_lot::{Mutex, RwLock};

use crate::storage::{
    index::{art::TrieError, art::KV, VariableKey},
    kv::{
        entry::{Entry, TxRecord, Value, ValueRef},
        error::{Error, Result},
        snapshot::{FilterFn, Snapshot, FILTERS},
        store::Core,
        util::now,
    },
};

/// `Mode` is an enumeration representing the different modes a transaction can have in an MVCC (Multi-Version Concurrency Control) system.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Mode {
    /// `ReadWrite` mode allows the transaction to both read and write data.
    ReadWrite,
    /// `ReadOnly` mode allows the transaction to only read data.
    ReadOnly,
    /// `WriteOnly` mode allows the transaction to only write data.
    WriteOnly,
}

impl Mode {
    /// Checks whether the transaction mode can mutate data.
    ///
    /// # Returns
    ///
    /// * `bool` - `true` if the mode is either `ReadWrite` or `WriteOnly`, `false` otherwise.
    pub(crate) fn mutable(&self) -> bool {
        match self {
            Self::ReadWrite => true,
            Self::ReadOnly => false,
            Self::WriteOnly => true,
        }
    }

    /// Checks if the transaction mode is `WriteOnly`.
    ///
    /// # Returns
    ///
    /// * `bool` - `true` if the mode is `WriteOnly`, `false` otherwise.
    pub(crate) fn is_write_only(&self) -> bool {
        matches!(self, Self::WriteOnly)
    }

    /// Checks if the transaction mode is `ReadOnly`.
    ///
    /// # Returns
    ///
    /// * `bool` - `true` if the mode is `ReadOnly`, `false` otherwise.
    pub(crate) fn is_read_only(&self) -> bool {
        matches!(self, Self::ReadOnly)
    }
}

/// `Transaction` is a struct representing a transaction in a database.
pub struct Transaction {
    /// `read_ts` is the read timestamp of the transaction. This is the time at which the transaction started.
    pub(crate) read_ts: u64,

    /// `mode` is the transaction mode. This can be either `ReadWrite`, `ReadOnly`, or `WriteOnly`.
    mode: Mode,

    /// `snapshot` is the snapshot that the transaction is running in. This is a consistent view of the data at the time the transaction started.
    pub(crate) snapshot: RwLock<Snapshot>,

    /// `buf` is a reusable buffer for encoding transaction records. This is used to reduce memory allocations.
    buf: BytesMut,

    /// `store` is the underlying store for the transaction. This is shared between transactions.
    pub(crate) store: Arc<Core>,

    /// `write_set` is the pending writes for the transaction. These are the changes that the transaction wants to make to the data.
    pub(crate) write_set: HashMap<Bytes, Entry>,

    /// `read_set` is the keys that are read in the transaction from the snapshot. This is used for conflict detection.
    pub(crate) read_set: Mutex<Vec<(Bytes, u64)>>,

    /// `committed_values_offsets` is the offsets of values in the transaction post commit to the transaction log. This is used to locate the data in the transaction log.
    committed_values_offsets: HashMap<Bytes, usize>,

    /// `closed` indicates if the transaction is closed. A closed transaction cannot make any more changes to the data.
    closed: bool,
}

impl Transaction {
    /// Prepare a new transaction in the given mode.
    pub fn new(store: Arc<Core>, mode: Mode) -> Result<Self> {
        let snapshot = RwLock::new(Snapshot::take(store.clone(), now())?);
        let read_ts = store.read_ts()?;

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
        // If the transaction is closed, return an error.
        if self.closed {
            return Err(Error::TransactionClosed);
        }
        // If the key is empty, return an error.
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        // Create a copy of the key.
        let key = Bytes::copy_from_slice(key);

        // Attempt to get the value for the key from the snapshot.
        match self.snapshot.read().get(&key[..].into()) {
            Ok(val_ref) => {
                // RYOW semantics: Read your own write. If the key is in the write set, return the value.
                if let Some(entry) = self.write_set.get(&key) {
                    return Ok(entry.value.clone().to_vec());
                }

                // If the transaction is not read-only and the value reference has a timestamp greater than 0,
                // add the key and its timestamp to the read set for conflict detection.
                if !self.mode.is_read_only() && val_ref.ts() > 0 {
                    self.read_set.lock().push((key, val_ref.ts()));
                }

                // Resolve the value reference to get the actual value.
                val_ref.resolve()
            }
            Err(e) => {
                match &e {
                    // If the key is not found in the index, and the transaction is not read-only,
                    // add the key to the read set with a timestamp of 0.
                    Error::IndexError(trie_error) => {
                        if let TrieError::KeyNotFound = trie_error {
                            if !self.mode.is_read_only() {
                                self.read_set.lock().push((key, 0));
                            }
                        }
                        Err(e)
                    }
                    // For other errors, just propagate them.
                    _ => Err(e),
                }
            }
        }
    }

    /// Writes a value for a key. None is used for deletion.
    fn write(&mut self, e: Entry) -> Result<()> {
        // If the transaction mode is not mutable (i.e., it's read-only), return an error.
        if !self.mode.mutable() {
            return Err(Error::TransactionReadOnly);
        }
        // If the transaction is closed, return an error.
        if self.closed {
            return Err(Error::TransactionClosed);
        }
        // If the key is empty, return an error.
        if e.key.is_empty() {
            return Err(Error::EmptyKey);
        }
        // If the key length exceeds the maximum allowed key size, return an error.
        if e.key.len() as u64 > self.store.opts.max_key_size {
            return Err(Error::MaxKeyLengthExceeded);
        }
        // If the value length exceeds the maximum allowed value size, return an error.
        if e.value.len() as u64 > self.store.opts.max_value_size {
            return Err(Error::MaxValueLengthExceeded);
        }

        if self.write_set.len() as u32 >= self.store.opts.max_tx_entries {
            return Err(Error::MaxTransactionEntriesLimitExceeded);
        }

        // If the transaction mode is not write-only, update the snapshot.
        if !self.mode.is_write_only() {
            // Convert the value to Bytes.
            let index_value = ValueRef::encode_mem(&e.value, e.metadata.as_ref());

            // If the entry is marked as deleted, delete it from the snapshot.
            // Otherwise, set the key-value pair in the snapshot.
            if e.is_deleted() {
                self.snapshot.write().delete(&e.key[..].into())?;
            } else {
                self.snapshot.write().set(&e.key[..].into(), index_value)?;
            }
        }

        // Add the entry to the set of pending writes.
        self.write_set.insert(e.key.clone(), e);

        Ok(())
    }

    /// Scans a range of keys and returns a vector of tuples containing the value, version, and timestamp for each key.
    pub fn scan<'b, R>(&'b self, range: R) -> Result<Vec<(Vec<u8>, u64, u64)>>
    where
        R: RangeBounds<&'b [u8]>,
    {
        // Convert the range to a tuple of bounds of variable keys.
        let range = (
            match range.start_bound() {
                Bound::Included(start) => {
                    Bound::Included(VariableKey::from_slice_with_termination(start))
                }
                Bound::Excluded(start) => {
                    Bound::Excluded(VariableKey::from_slice_with_termination(start))
                }
                Bound::Unbounded => Bound::Unbounded,
            },
            match range.end_bound() {
                Bound::Included(end) => {
                    Bound::Included(VariableKey::from_slice_with_termination(end))
                }
                Bound::Excluded(end) => {
                    Bound::Excluded(VariableKey::from_slice_with_termination(end))
                }
                Bound::Unbounded => Bound::Unbounded,
            },
        );

        // Create a new reader for the snapshot.
        let iterator = self.snapshot.write().new_reader()?;

        // Get a range iterator for the specified range.
        let ranger = iterator.range(range);

        // Initialize an empty vector to store the results.
        let mut results = Vec::new();

        // Iterate over the keys in the range.
        'outer: for (key, value, version, ts) in ranger {
            // Create a new value reference and decode the value.
            let mut val_ref = ValueRef::new(self.store.clone());
            let val_bytes_ref: &Bytes = value;
            val_ref.decode(*version, val_bytes_ref)?;

            // Apply all filters. If any filter fails, skip this key and continue with the next one.
            for filter in &FILTERS {
                if filter.apply(&val_ref, self.read_ts).is_err() {
                    continue 'outer;
                }
            }

            // Only add the key to the read set if the timestamp is less than or equal to the
            // read timestamp. This is to prevent adding keys that are added during the transaction.
            if val_ref.ts() <= self.read_ts {
                self.read_set.lock().push((
                    Bytes::copy_from_slice(&key[..&key.len() - 1]), // the keys in the vart leaf are terminated with a null byte
                    val_ref.ts,
                ));
            }

            // Resolve the value reference to get the actual value.
            let v = val_ref.resolve()?;

            // Add the value, version, and timestamp to the results vector.
            results.push((v, *version, *ts));
        }

        // Return the results.
        Ok(results)
    }

    /// Commits the transaction, by writing all pending entries to the store.
    pub fn commit(&mut self) -> Result<()> {
        // If the transaction is closed, return an error.
        if self.closed {
            return Err(Error::TransactionClosed);
        }

        // If the transaction is read-only, return an error.
        if self.mode.is_read_only() {
            return Err(Error::TransactionReadOnly);
        }

        // If there are no pending writes, there's nothing to commit, so return early.
        if self.write_set.is_empty() {
            return Ok(());
        }

        // TODO: Use a commit pipeline to avoid blocking calls.
        // Lock the oracle to serialize commits to the transaction log.
        let oracle = self.store.oracle.clone();
        let _lock = oracle.write_lock.lock();

        // Prepare for the commit by getting a transaction ID and a commit timestamp.
        let (tx_id, commit_ts) = self.prepare_commit()?;

        // Add transaction records to the transaction log.
        self.add_to_transaction_log(tx_id, commit_ts)?;

        // Commit the changes to the store index.
        self.commit_to_index(tx_id, commit_ts)?;

        // Update the oracle to indicate that the transaction has been committed up to the given transaction ID.
        oracle.committed_upto(tx_id);

        // Mark the transaction as closed.
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

    /// Builds key-value pairs from the write set.
    fn build_kv_pairs(&self, tx_id: u64, commit_ts: u64) -> Vec<KV<VariableKey, Bytes>> {
        let mut kv_pairs = Vec::new();

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

    /// Builds an index value from an entry.
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

    /// Rolls back the transaction by removing all updated entries.
    pub fn rollback(&mut self) {
        self.closed = true;
        self.committed_values_offsets.clear();
        self.buf.clear();
        self.write_set.clear();
        self.read_set.lock().clear();
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.rollback();
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::mem::size_of;

    use super::*;
    use crate::storage::kv::option::{IsolationLevel, Options};
    use crate::storage::kv::store::Store;

    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    // Common setup logic for creating a store
    fn create_store(is_ssi: bool) -> (Store, TempDir) {
        let temp_dir = create_temp_directory();
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

    #[test]
    fn basic_transaction() {
        let (store, temp_dir) = create_store(false);

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

        // Create a new Core instance with VariableKey after dropping the previous one
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
    fn transaction_delete_scan() {
        let (store, _) = create_store(false);

        // Define key-value pairs for the test
        let key1 = Bytes::from("k1");
        let value1 = Bytes::from("baz");

        {
            // Start a new read-write transaction (txn1)
            let mut txn1 = store.begin().unwrap();
            txn1.set(&key1, &value1).unwrap();
            txn1.set(&key1, &value1).unwrap();
            txn1.commit().unwrap();
        }

        {
            // Start a read-only transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.delete(&key1).unwrap();
            txn.commit().unwrap();
        }

        {
            // Start another read-write transaction (txn)
            let txn = store.begin().unwrap();
            assert!(txn.get(&key1).is_err());
        }

        {
            let range = "k1".as_bytes()..="k3".as_bytes();
            let txn = store.begin().unwrap();
            let results = txn.scan(range).unwrap();
            assert_eq!(results.len(), 0);
        }
    }

    fn mvcc_tests(is_ssi: bool) {
        let (store, _) = create_store(is_ssi);

        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");
        let value1 = Bytes::from("baz");
        let value2 = Bytes::from("bar");

        // no read conflict
        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            txn1.set(&key1, &value1).unwrap();
            txn1.commit().unwrap();

            assert!(txn2.get(&key2).is_err());
            txn2.set(&key2, &value2).unwrap();
            txn2.commit().unwrap();
        }

        // read conflict when the read key was updated by another transaction
        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            txn1.set(&key1, &value1).unwrap();
            txn1.commit().unwrap();

            assert!(txn2.get(&key1).is_ok());
            txn2.set(&key1, &value2).unwrap();
            assert!(match txn2.commit() {
                Err(err) => {
                    if let Error::TransactionReadConflict = err {
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
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            txn1.set(&key1, &value1).unwrap();
            txn2.set(&key1, &value2).unwrap();

            txn1.commit().unwrap();
            txn2.commit().unwrap();

            let txn3 = store.begin().unwrap();
            let val = txn3.get(&key1).unwrap();
            assert_eq!(val, value2.as_ref());
        }

        // read conflict when the read key was updated by another transaction
        {
            let key = Bytes::from("key3");

            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            txn1.set(&key, &value1).unwrap();
            txn1.commit().unwrap();

            assert!(txn2.get(&key).is_err());
            txn2.set(&key, &value1).unwrap();
            assert!(match txn2.commit() {
                Err(err) => {
                    if let Error::TransactionReadConflict = err {
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

            let mut txn1 = store.begin().unwrap();
            txn1.set(&key, &value1).unwrap();
            txn1.commit().unwrap();

            let mut txn2 = store.begin().unwrap();
            let mut txn3 = store.begin().unwrap();

            txn2.delete(&key).unwrap();
            assert!(txn2.commit().is_ok());

            assert!(txn3.get(&key).is_ok());
            txn3.set(&key, &value2).unwrap();
            assert!(match txn3.commit() {
                Err(err) => {
                    if let Error::TransactionReadConflict = err {
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
    fn mvcc_serialized_snapshot_isolation() {
        mvcc_tests(true);
    }

    #[test]
    fn mvcc_snapshot_isolation() {
        mvcc_tests(false);
    }

    #[test]
    fn basic_scan_single_key() {
        let (store, _) = create_store(false);
        // Define key-value pairs for the test
        let keys_to_insert = vec![Bytes::from("key1")];

        for key in &keys_to_insert {
            let mut txn = store.begin().unwrap();
            txn.set(key, key).unwrap();
            txn.commit().unwrap();
        }

        let range = "key1".as_bytes()..="key3".as_bytes();

        let txn = store.begin().unwrap();
        let results = txn.scan(range).unwrap();
        assert_eq!(results.len(), keys_to_insert.len());
    }

    #[test]
    fn basic_scan_multiple_keys() {
        let (store, _) = create_store(false);
        // Define key-value pairs for the test
        let keys_to_insert = vec![
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
            Bytes::from("lemon"),
        ];

        for key in &keys_to_insert {
            let mut txn = store.begin().unwrap();
            txn.set(key, key).unwrap();
            txn.commit().unwrap();
        }

        let range = "key1".as_bytes()..="key3".as_bytes();

        let txn = store.begin().unwrap();
        let results = txn.scan(range).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, keys_to_insert[0]);
        assert_eq!(results[1].0, keys_to_insert[1]);
        assert_eq!(results[2].0, keys_to_insert[2]);
    }

    fn mvcc_with_scan_tests(is_ssi: bool) {
        let (store, _) = create_store(is_ssi);

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
            let mut txn1 = store.begin().unwrap();

            txn1.set(&key1, &value1).unwrap();
            txn1.commit().unwrap();

            let mut txn2 = store.begin().unwrap();
            let mut txn3 = store.begin().unwrap();

            txn2.set(&key1, &value4).unwrap();
            txn2.set(&key2, &value2).unwrap();
            txn2.set(&key3, &value3).unwrap();
            txn2.commit().unwrap();

            let range = "key1".as_bytes()..="key4".as_bytes();
            let results = txn3.scan(range).unwrap();
            assert_eq!(results.len(), 1);
            txn3.set(&key2, &value5).unwrap();
            txn3.set(&key3, &value6).unwrap();

            assert!(match txn3.commit() {
                Err(err) => {
                    if let Error::TransactionReadConflict = err {
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
            let mut txn1 = store.begin().unwrap();

            txn1.set(&key4, &value1).unwrap();
            txn1.commit().unwrap();

            let mut txn2 = store.begin().unwrap();
            let mut txn3 = store.begin().unwrap();

            txn2.delete(&key4).unwrap();
            txn2.commit().unwrap();

            let range = "key1".as_bytes()..="key5".as_bytes();
            txn3.scan(range).unwrap();
            txn3.set(&key4, &value2).unwrap();

            assert!(match txn3.commit() {
                Err(err) => {
                    if let Error::TransactionReadConflict = err {
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
    fn mvcc_serialized_snapshot_isolation_scan() {
        mvcc_with_scan_tests(true);
    }

    #[test]
    fn mvcc_snapshot_isolation_scan() {
        mvcc_with_scan_tests(false);
    }

    #[test]
    fn ryow() {
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
    fn create_hermitage_store(is_ssi: bool) -> Store {
        let (store, _) = create_store(is_ssi);

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
    fn g0_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi);
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
                    if let Error::TransactionReadConflict = err {
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

    #[test]
    fn g0() {
        g0_tests(false); // snapshot isolation
        g0_tests(true); // serializable snapshot isolation
    }

    // G1a: Aborted Reads (dirty reads, cascaded aborts)
    fn g1a_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi);
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

            let range = "k1".as_bytes()..="k3".as_bytes();
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

    #[test]
    fn g1a() {
        g1a_tests(false); // snapshot isolation
        g1a_tests(true); // serializable snapshot isolation
    }

    // G1b: Intermediate Reads (dirty reads)
    fn g1b_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi);

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

            let range = "k1".as_bytes()..="k3".as_bytes();
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

    #[test]
    fn g1b() {
        g1b_tests(false); // snapshot isolation
        g1b_tests(true); // serializable snapshot isolation
    }

    // G1c: Circular Information Flow (dirty reads)
    fn g1c_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi);

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
                    if let Error::TransactionReadConflict = err {
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
    fn g1c() {
        g1c_tests(false); // snapshot isolation
        g1c_tests(true);
    }

    // PMP: Predicate-Many-Preceders
    fn pmp_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi);

        let key3 = Bytes::from("k3");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");
        let value3 = Bytes::from("v3");

        {
            let txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            // k3 should not be visible to txn1
            let range = "k1".as_bytes()..="k3".as_bytes();
            let res = txn1.scan(range.clone()).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].0, value1);
            assert_eq!(res[1].0, value2);

            // k3 is committed by txn2
            txn2.set(&key3, &value3).unwrap();
            txn2.commit().unwrap();

            // k3 should still not be visible to txn1
            let range = "k1".as_bytes()..="k3".as_bytes();
            let res = txn1.scan(range.clone()).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].0, value1);
            assert_eq!(res[1].0, value2);
        }
    }

    #[test]
    fn pmp() {
        pmp_tests(false);
        pmp_tests(true);
    }

    // PMP-Write: Circular Information Flow (dirty reads)
    fn pmp_write_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi);

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

            let range = "k1".as_bytes()..="k2".as_bytes();
            let res = txn2.scan(range.clone()).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].0, value1);
            assert_eq!(res[1].0, value2);

            txn2.delete(&key2).unwrap();
            txn1.commit().unwrap();

            let range = "k1".as_bytes()..="k3".as_bytes();
            let res = txn2.scan(range.clone()).unwrap();
            assert_eq!(res.len(), 1);
            assert_eq!(res[0].0, value1);

            assert!(match txn2.commit() {
                Err(err) => {
                    if let Error::TransactionReadConflict = err {
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
    fn pmp_write() {
        pmp_write_tests(false);
        pmp_write_tests(true);
    }

    // P4: Lost Update
    fn p4_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi);

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
                    if let Error::TransactionReadConflict = err {
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
    fn p4() {
        p4_tests(false);
        p4_tests(true);
    }

    // G-single: Single Anti-dependency Cycles (read skew)
    fn g_single_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi);

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

    #[test]
    fn g_single() {
        g_single_tests(false);
        g_single_tests(true);
    }

    // G-single-write-1: Single Anti-dependency Cycles (read skew)
    fn g_single_write_1_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi);

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

            let range = "k1".as_bytes()..="k2".as_bytes();
            let res = txn2.scan(range.clone()).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].0, value1);
            assert_eq!(res[1].0, value2);

            txn2.set(&key1, &value3).unwrap();
            txn2.set(&key2, &value4).unwrap();

            txn2.commit().unwrap();

            txn1.delete(&key2).unwrap();
            assert!(txn1.get(&key2).is_err());
            assert!(match txn1.commit() {
                Err(err) => {
                    if let Error::TransactionReadConflict = err {
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
    fn g_single_write_1() {
        g_single_write_1_tests(false);
        g_single_write_1_tests(true);
    }

    // G-single-write-2: Single Anti-dependency Cycles (read skew)
    fn g_single_write_2_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi);

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
            let range = "k1".as_bytes()..="k2".as_bytes();
            let res = txn2.scan(range.clone()).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].0, value1);
            assert_eq!(res[1].0, value2);

            txn2.set(&key1, &value3).unwrap();

            txn1.delete(&key2).unwrap();

            txn2.set(&key2, &value4).unwrap();

            drop(txn1);

            txn2.commit().unwrap();
        }
    }

    #[test]
    fn g_single_write_2() {
        g_single_write_2_tests(false);
        g_single_write_2_tests(true);
    }

    fn g2_item_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi);

        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");
        let value3 = Bytes::from("v3");
        let value4 = Bytes::from("v4");

        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            let range = "k1".as_bytes()..="k2".as_bytes();
            let res = txn1.scan(range.clone()).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].0, value1);
            assert_eq!(res[1].0, value2);

            let res = txn2.scan(range.clone()).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].0, value1);
            assert_eq!(res[1].0, value2);

            txn1.set(&key1, &value3).unwrap();
            txn2.set(&key2, &value4).unwrap();

            txn1.commit().unwrap();

            assert!(match txn2.commit() {
                Err(err) => {
                    if let Error::TransactionReadConflict = err {
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
    fn g2_item() {
        g2_item_tests(false);
        g2_item_tests(true);
    }

    fn require_send<T: Send>(_: T) {}
    fn require_sync<T: Sync + Send>(_: T) {}

    #[test]
    fn is_send_sync() {
        let (db, _) = create_store(false);

        let txn = db.begin().unwrap();
        require_send(txn);

        let txn = db.begin().unwrap();
        require_sync(txn);
    }

    #[test]
    fn max_transaction_entries_limit_exceeded() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_tx_entries = 5;

        let store = Store::new(opts.clone()).expect("should create store");

        let num_keys = 6;
        let mut counter = 0u32;
        let mut keys = Vec::new();

        for _ in 1..=num_keys {
            // Convert the counter to Bytes
            let key_bytes = Bytes::from(counter.to_le_bytes().to_vec());

            // Increment the counter
            counter += 1;

            // Add the key to the vector
            keys.push(key_bytes);
        }

        let default_value = Bytes::from("default_value".to_string());

        // Writing the 6th key should fail
        let mut txn = store.begin().unwrap();

        for (i, key) in keys.iter().enumerate() {
            // Start a new write transaction
            if i < 5 {
                assert!(txn.set(key, &default_value).is_ok());
            } else {
                assert!(txn.set(key, &default_value).is_err());
            }
        }
    }

    const ENTRIES: usize = 4_000_00;
    const KEY_SIZE: usize = 24;
    const VALUE_SIZE: usize = 150;
    const RNG_SEED: u64 = 3;

    fn fill_slice(slice: &mut [u8], rng: &mut fastrand::Rng) {
        let mut i = 0;
        while i + size_of::<u128>() < slice.len() {
            let tmp = rng.u128(..);
            slice[i..(i + size_of::<u128>())].copy_from_slice(&tmp.to_le_bytes());
            i += size_of::<u128>()
        }
        if i + size_of::<u64>() < slice.len() {
            let tmp = rng.u64(..);
            slice[i..(i + size_of::<u64>())].copy_from_slice(&tmp.to_le_bytes());
            i += size_of::<u64>()
        }
        if i + size_of::<u32>() < slice.len() {
            let tmp = rng.u32(..);
            slice[i..(i + size_of::<u32>())].copy_from_slice(&tmp.to_le_bytes());
            i += size_of::<u32>()
        }
        if i + size_of::<u16>() < slice.len() {
            let tmp = rng.u16(..);
            slice[i..(i + size_of::<u16>())].copy_from_slice(&tmp.to_le_bytes());
            i += size_of::<u16>()
        }
        if i + size_of::<u8>() < slice.len() {
            slice[i] = rng.u8(..);
        }
    }

    fn gen_pair(rng: &mut fastrand::Rng) -> ([u8; KEY_SIZE], Vec<u8>) {
        let mut key = [0u8; KEY_SIZE];
        fill_slice(&mut key, rng);
        let mut value = vec![0u8; VALUE_SIZE];
        fill_slice(&mut value, rng);

        (key, value)
    }

    fn make_rng() -> fastrand::Rng {
        fastrand::Rng::with_seed(RNG_SEED)
    }

    #[test]
    #[ignore]
    fn insert_large_txn_and_get() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        opts.max_tx_entries = ENTRIES as u32;

        let store = Store::new(opts.clone()).expect("should create store");
        let mut rng = make_rng();

        let mut txn = store.begin().unwrap();
        for _ in 0..ENTRIES {
            let (key, value) = gen_pair(&mut rng);
            txn.set(&key, &value).unwrap();
        }
        txn.commit().unwrap();
        drop(txn);

        // Read the keys from the store
        let mut rng = make_rng();
        let txn = store.begin_with_mode(Mode::ReadOnly).unwrap();
        for i in 0..ENTRIES {
            let (key, _) = gen_pair(&mut rng);
            txn.get(&key).unwrap();
        }
    }
}
