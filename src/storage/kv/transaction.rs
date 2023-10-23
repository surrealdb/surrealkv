use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Bytes, BytesMut};
use serde::{Deserialize, Serialize};

use crate::storage::index::art::TrieError;
use crate::storage::index::KeyTrait;
use crate::storage::kv::entry::{
    Entry, MD_SIZE, VALUE_LENGTH_SIZE, VALUE_OFFSET_SIZE, VERSION_SIZE,
};
use crate::storage::kv::error::{Error, Result};
use crate::storage::kv::snapshot::Snapshot;

use super::entry::{TxRecord, ValueRef};
use super::store::Core;

/// An MVCC transaction mode.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
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

pub struct Transaction<'a, P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> {
    /// The read timestamp of the transaction.
    pub(crate) read_ts: u64,

    /// The transaction mode.
    mode: Mode,

    /// The snapshot that the transaction is running in.
    snapshot: Snapshot<P, V>,

    // Reusable buffer for encoding transaction records.
    buf: BytesMut,

    /// The underlying store for the transaction. Shared between transactions using a mutex.
    store: Arc<Core<P, V>>,

    /// The pending writes for the transaction.
    write_set: HashMap<&'a Bytes, Entry<'a>>,

    // The keys that are read in the transaction from the snapshot.
    read_set: Mutex<Vec<(&'a Bytes, u64)>>,

    // The transaction is closed.
    closed: bool,
}

impl<'a, P: KeyTrait, V: Clone + From<bytes::Bytes> + AsRef<Bytes>> Transaction<'a, P, V> {
    /// Prepare a new transaction in the given mode.
    pub fn new(store: Arc<Core<P, V>>, mode: Mode) -> Result<Self> {
        if store.closed {
            return Err(Error::StoreClosed);
        }

        // TODO!! This should be the max txID of the index, get this from oracle
        let read_ts = store.oracle.read_ts();
        let snapshot = Snapshot::take(store.clone(), read_ts)?;

        Ok(Self {
            read_ts: read_ts,
            mode,
            snapshot,
            buf: BytesMut::new(),
            store,
            write_set: HashMap::new(),
            read_set: Mutex::new(Vec::new()),
            closed: false,
        })
    }

    /// Returns the transaction mode.
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// Adds a key-value pair to the store.
    pub fn set(&mut self, key: &'a Bytes, value: Bytes) -> Result<()> {
        let entry = Entry::new(key, value);
        self.write(entry)?;
        Ok(())
    }

    /// Deletes a key from the store.
    pub fn delete(&mut self, key: &'a Bytes) -> Result<()> {
        let mut entry = Entry::new(key, Bytes::new());
        entry.mark_delete();
        self.write(entry)?;
        Ok(())
    }

    /// Gets a value for a key if it exists.
    pub fn get(&self, key: &'a Bytes) -> Result<ValueRef<P, V>> {
        if self.closed {
            return Err(Error::TxnClosed);
        }
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        // Read Your Own Writes (RYOW) semantics.

        // Check if the key is in the snapshot.
        match self.snapshot.get(&key[..].into()) {
            Ok(val_ref) => {
                // If the transaction is not read-only and the value reference has a timestamp greater than 0,
                // add the key and its timestamp to the read set for conflict detection.
                if !self.mode.is_read_only() && val_ref.ts > 0 {
                    self.read_set.lock()?.push((key, val_ref.ts));
                }

                Ok(val_ref)
            }
            Err(e) => {
                match &e {
                    // Handle specific error cases.
                    Error::IndexError(trie_error) => {
                        match trie_error {
                            // Handle the case where the key is not found.
                            TrieError::KeyNotFound => {
                                // If the transaction is not read-only, add the key to the read set.
                                // In snapshot isolation mode, this key could be added by another transaction,
                                // and keeping track of this key helps detect conflicts.
                                if !self.mode.is_read_only() {
                                    self.read_set.lock()?.push((key, 0));
                                }
                            }
                            _ => {}
                        }
                        return Err(e);
                    }
                    _ => {
                        // Handle other error cases.
                        return Err(e);
                    }
                }
            }
        }
    }

    /// Writes a value for a key. None is used for deletion.
    fn write(&mut self, e: Entry<'a>) -> Result<()> {
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
            self.snapshot
                .set(&e.key[..].into(), indexed_value_bytes.into())?;
        }

        // Add the entry to pending writes
        self.write_set.insert(&e.key, e);

        Ok(())
    }

    // TODO!!!: handle read locks on snapshot
    // TODO!!! This should be called with lock on the index
    pub fn check_conflict(&self) -> Result<()> {
        // If the transaction is write-only, there is no need to check for conflicts
        // This can lead to the scenario of blind writes where the transaction is not aware of
        // any conflicts and will overwrite any existing values. In case of concurrent transactions,
        // this can lead to lost updates, and the last transaction to commit will win.
        if self.mode.is_write_only() {
            return Ok(());
        }

        // This is the scenario of snapshot isolation where the transaction is in read-write mode.
        // Currently, only optimistic concurrency control (OCC) is supported.
        // TODO: add support for pessimistic concurrency control (serializable snapshot isolation)
        // The following steps are performed:
        //      1. Take the latest snapshot from the store
        //      2. Check if the read keys in the transaction are still valid in the latest snapshot, and
        //      the timestamp of the read keys in the transaction matches the timestamp of the latest snapshot.
        //      If the timestamp does not match, then there is a conflict.
        //      3. If the read keys are still valid, then there is no conflict
        //      4. If the read keys are not valid, then there is a conflict
        //
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Error with time, went backwards");
        let current_snapshot = Snapshot::take(self.store.clone(), ts.as_secs())?;

        let read_set = self.read_set.lock().unwrap();

        for (key, ts) in read_set.iter() {
            match current_snapshot.get(&key[..].into()) {
                Ok(val_ref) => {
                    if *ts != val_ref.ts {
                        return Err(Error::TxnReadConflict);
                    }
                }
                Err(e) => {
                    match &e {
                        Error::IndexError(trie_error) => {
                            // Handle key not found
                            match trie_error {
                                TrieError::KeyNotFound => {
                                    if *ts > 0 {
                                        return Err(Error::TxnReadConflict);
                                    }
                                    continue;
                                }
                                _ => return Err(e),
                            }
                        }
                        _ => return Err(e),
                    }
                }
            }
        }

        Ok(())
    }

    pub fn validate(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::TxnClosed);
        }

        if self.mode.is_read_only() {
            return Err(Error::TxnReadOnly);
        }

        if self.write_set.is_empty() {
            return Ok(());
        }

        self.check_conflict()?;

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

        // Prepare for the commit
        let tx_id = self.prepare_commit()?;

        // Add transaction records to the log
        self.add_to_transaction_log(tx_id)?;

        // Commit to the store index
        self.commit_to_index()?;

        Ok(())
    }

    /// Prepares for the commit by assigning commit timestamps and preparing records.
    fn prepare_commit(&mut self) -> Result<u64> {
        if !self.mode.is_write_only() {
            self.snapshot.close()?;
        }

        self.closed = true;
        let commit_ts = self.assign_commit_timestamp();
        Ok(commit_ts)
    }

    /// Assigns commit timestamps to transaction entries.
    fn assign_commit_timestamp(&mut self) -> u64 {
        let commit_ts = self.store.oracle.new_commit_ts();
        self.write_set.iter_mut().for_each(|(_, entry)| {
            entry.ts = commit_ts; // this should be time.now(), not txID
        });
        commit_ts
    }

    /// Adds transaction records to the transaction log.
    fn add_to_transaction_log(&mut self, tx_id: u64) -> Result<()> {
        let entries: Vec<Entry> = self.write_set.values().cloned().collect();
        let tx_record = TxRecord::new_from_entries(entries, tx_id);
        tx_record.encode(&mut self.buf);

        println!("buf: {:?}", self.buf.as_ref());
        let mut tlog = self.store.tlog.write()?;
        tlog.append(&self.buf.as_ref())?;
        Ok(())
    }

    /// Commits transaction changes to the store index.
    fn commit_to_index(&mut self) -> Result<()> {
        let commit_ts = self.store.oracle.new_commit_ts();
        let mut index = self.store.indexer.write()?;
        for (_, entry) in self.write_set.iter() {
            index.insert(
                &entry.key[..].into(),
                entry.value.clone().into(),
                commit_ts,
                commit_ts,
            )?;
        }
        Ok(())
    }

    /// Rolls back the transaction, by removing all updated entries.
    pub fn rollback(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::TxnClosed);
        }

        self.closed = true;
        self.write_set.clear();

        Ok(())
    }
}

impl<'a, P: KeyTrait, V: Clone + From<bytes::Bytes> + AsRef<Bytes>> Drop for Transaction<'a, P, V> {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::sync::Arc;

    use crate::storage::index::VectorKey;
    use crate::storage::kv::option::Options;
    use crate::storage::kv::reader::{Reader, TxReader};
    use crate::storage::kv::store::Core;
    use crate::storage::kv::transaction::{Mode, Transaction};
    use crate::storage::kv::util::NoopValue;
    use crate::storage::log::aol::aol::AOL;
    use crate::storage::log::Options as LogOptions;

    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    #[test]
    fn test_transaction() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        let store = Arc::new(Core::<VectorKey, NoopValue>::new(opts));
        assert_eq!(store.closed, false);

        let key1 = Bytes::from("foo1");
        let key1_clone = key1.clone();
        let key2 = Bytes::from("foo2");
        let key2_clone = key2.clone();
        let value = Bytes::from("baz");

        let mut txn = Transaction::new(store.clone(), Mode::ReadWrite).unwrap();
        txn.set(&key1_clone, value.clone()).unwrap();
        txn.set(&key2_clone, value).unwrap();
        txn.commit().unwrap();

        let a = AOL::open(temp_dir.path(), &LogOptions::default()).expect("should create aol");
        println!("offset: {:?}", a.offset());

        let r = Reader::new_from(a, 0, 100).unwrap();
        let mut txr = TxReader::new(r).unwrap();
        let hdr = txr.read_header().unwrap();
        println!("hdr: {:?}", hdr);

        let val = txr.read_entry().unwrap();
        println!("val: {:?}", val);

        let val = txr.read_entry().unwrap();
        println!("val: {:?}", val);
        // let bs = r.read_uint64().unwrap();
        // println!("bs: {:?}", bs);
        // let bs = r.read_uint64().unwrap();
        // println!("bs: {:?}", bs);
        // let bs = r.read_uint16().unwrap();
        // println!("bs: {:?}", bs);
        // let bs = r.read_uint16().unwrap();
        // println!("bs: {:?}", bs);
        // let bs = r.read_uint16().unwrap();
        // println!("bs: {:?}", bs);

        // let bs = r.read_uint32().unwrap();
        // println!("bs: {:?}", bs);
        // let bs = r.read_uint32().unwrap();
        // println!("bs: {:?}", bs);
    }
}
