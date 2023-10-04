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

use super::entry::ValueRef;
use super::store::MVCCStore;

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

pub struct Transaction<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> {
    /// The read timestamp of the transaction.
    pub(crate) read_ts: u64,

    /// The transaction mode.
    mode: Mode,

    /// The snapshot that the transaction is running in.
    snapshot: Snapshot<P, V>,

    buf: BytesMut,

    /// The underlying store for the transaction. Shared between transactions using a mutex.
    store: Arc<MVCCStore<P, V>>,

    /// The pending writes for the transaction.
    write_set: HashMap<Bytes, Entry>,

    // The keys that are read in the transaction from the snapshot.
    read_set: Mutex<Vec<(Bytes, u64)>>,

    // The transaction is closed.
    closed: bool,
}

impl<P: KeyTrait, V: Clone + From<bytes::Bytes> + AsRef<Bytes>> Transaction<P, V> {
    /// Prepare a new transaction in the given mode.
    pub fn new(store: Arc<MVCCStore<P, V>>, mode: Mode) -> Result<Self> {
        if store.closed {
            return Err(Error::StoreClosed);
        }
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Error with time, went backwards");
        let snapshot = Snapshot::take(store.clone(), ts.as_secs())?;

        Ok(Self {
            read_ts: ts.as_secs(),
            mode,
            snapshot,
            buf: BytesMut::with_capacity(store.opts.max_tx_size()),
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
    pub fn set(&mut self, key: Bytes, value: Bytes) -> Result<()> {
        let entry = Entry::new(key, value);
        self.write(entry)?;
        Ok(())
    }

    /// Deletes a key from the store.
    pub fn delete(&mut self, key: Bytes) -> Result<()> {
        let mut entry = Entry::new(key, Bytes::new());
        entry.mark_delete();
        self.write(entry)?;
        Ok(())
    }

    /// Gets a value for a key, if it exists.
    // pub fn get(&self, key: &Bytes) -> Result<Entry> {
    pub fn get(&self, key: &Bytes) -> Result<ValueRef<P, V>> {
        if self.closed {
            return Err(Error::TxnClosed);
        }
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        // RYOW: Read your own writes

        // Check if the key is in the snapshot
        match self.snapshot.get(&key[..].into()) {
            Ok(val_ref) => {
                if !self.mode.is_read_only() && val_ref.ts > 0 {
                    self.read_set.lock()?.push((key.clone(), val_ref.ts));
                }

                Ok(val_ref)
            }
            Err(e) => {
                match &e {
                    // Handle specific error cases
                    Error::IndexError(trie_error) => {
                        // Handle key not found
                        match trie_error {
                            TrieError::KeyNotFound => {
                                // The key is added to the snapshot reads if the transaction is not read-only
                                // because in snapshot isolation mode, this key could be added by another transaction
                                // and keeping track of this key will help detect conflicts.
                                if !self.mode.is_read_only() {
                                    self.read_set.lock()?.push((key.clone(), 0));
                                }
                            }
                            _ => {}
                        }
                        return Err(e);
                    }
                    _ => {
                        // Handle other error cases
                        return Err(e);
                    }
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
            self.snapshot
                .set(&e.key[..].into(), indexed_value_bytes.into())?;
        }

        // Add the entry to pending writes
        self.write_set.insert(e.key.clone(), e);

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

    /// Commits the transaction, by writing all pending entries to the store.
    pub fn commit(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::TxnClosed);
        }

        if !self.mode.is_write_only() {
            self.snapshot.close()?;
        }

        self.closed = true;

        if self.mode.is_read_only() {
            return Err(Error::TxnReadOnly);
        }

        if self.write_set.is_empty() {
            return Ok(());
        }

        self.check_conflict()?;

        let orc = self.store.oracle.clone();
        let commit_ts = orc.new_commit_ts();

        self.write_set.iter_mut().for_each(|(_, entry)| {
            entry.ts = commit_ts;
        });

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

impl<P: KeyTrait, V: Clone + From<bytes::Bytes> + AsRef<Bytes>> Drop for Transaction<P, V> {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}
