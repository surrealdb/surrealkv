use std::cmp::Ordering;
use std::collections::btree_map;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
pub use double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt};

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::lsm::Core;
use crate::snapshot::Snapshot;
use crate::sstable::InternalKeyKind;
use crate::vlog::ValueLocation;
use crate::{Key, Value};

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
    /// Checks if this transaction mode permits mutations
    pub(crate) fn mutable(&self) -> bool {
        match self {
            Self::ReadWrite => true,
            Self::ReadOnly => false,
            Self::WriteOnly => true,
        }
    }

    /// Checks if this is a write-only transaction
    pub(crate) fn is_write_only(&self) -> bool {
        matches!(self, Self::WriteOnly)
    }

    /// Checks if this is a read-only transaction
    pub(crate) fn is_read_only(&self) -> bool {
        matches!(self, Self::ReadOnly)
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub enum Durability {
    /// Writes are buffered in OS page cache.
    /// May lose recent commits on power failure but fast.
    #[default]
    Eventual,

    /// Forces fsync() on commit.
    /// Guarantees durability but significantly slower.
    Immediate,
}

// ===== Transaction Implementation =====
/// A transaction in the LSM tree providing ACID guarantees.
pub struct Transaction {
    /// `mode` is the transaction mode. This can be either `ReadWrite`, `ReadOnly`, or `WriteOnly`.
    mode: Mode,

    /// `durability` is the durability level of the transaction. This is used to determine how the transaction is committed.
    durability: Durability,

    /// `snapshot` is the snapshot that the transaction is running in. This is a consistent view of the data at the time the transaction started.
    pub(crate) snapshot: Option<Snapshot>,

    /// `core` is the underlying core for the transaction. This is shared between transactions.
    pub(crate) core: Arc<Core>,

    /// `write_set` is a map of keys to entries.
    pub(crate) write_set: BTreeMap<Bytes, Option<Entry>>,

    /// `closed` indicates if the transaction is closed. A closed transaction cannot make any more changes to the data.
    closed: bool,

    /// Tracks when this transaction started for deadlock detection
    pub(crate) start_commit_id: u64,
}

impl Transaction {
    /// Prepare a new transaction in the given mode.
    pub fn new(core: Arc<Core>, mode: Mode) -> Result<Self> {
        let read_ts = core.seq_num();

        let start_commit_id = core
            .oracle
            .transaction_commit_id
            .load(std::sync::atomic::Ordering::Acquire);

        // Only register mutable transactions with the oracle for conflict detection
        // Read-only transactions don't need conflict detection since they don't write
        if mode.mutable() {
            core.oracle.register_txn_start(start_commit_id);
        }

        let mut snapshot = None;
        if !mode.is_write_only() {
            snapshot = Some(Snapshot::new(core.clone(), read_ts));
        }

        Ok(Self {
            mode,
            snapshot,
            core,
            write_set: BTreeMap::new(),
            durability: Durability::Eventual,
            closed: false,
            start_commit_id,
        })
    }

    /// Returns the transaction mode.
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// Sets the durability level of the transaction.
    pub fn set_durability(&mut self, durability: Durability) {
        self.durability = durability;
    }

    /// Adds a key-value pair to the store.
    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let entry = Entry::new(key, Some(value), InternalKeyKind::Set);
        self.write(entry)?;
        Ok(())
    }

    // Delete all the versions of a key. This is a hard delete.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let entry = Entry::new(key, None, InternalKeyKind::Delete);
        self.write(entry)?;
        Ok(())
    }

    /// Gets a value for a key if it exists.
    pub fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        // If the transaction is closed, return an error.
        if self.closed {
            return Err(Error::TransactionClosed);
        }
        // If the key is empty, return an error.
        if key.as_ref().is_empty() {
            return Err(Error::EmptyKey);
        }

        // Do not allow reads if it is a write-only transaction
        if self.mode.is_write_only() {
            return Err(Error::TransactionWriteOnly);
        }

        // RYOW semantics: Read your own writes. If the value is in the write set, return it.
        match self.write_set.get(key) {
            Some(Some(val)) => {
                // If the entry is a tombstone, return None.
                if val.is_tombstone() {
                    return Ok(None);
                }
                let v = val.value.clone().unwrap();
                // Otherwise, return the value.
                return Ok(Some(Arc::from(v.to_vec())));
            }
            // TODO: Check if this is correct.
            Some(None) => {
                // If the entry is None, it means the key was deleted in this transaction.
                return Ok(None);
            }
            None => {}
        }

        // The value is not in the write set, so attempt to get it from the snapshot.
        match self.snapshot.as_ref().unwrap().get(key)? {
            Some(val) => {
                // Resolve the value reference through VLog if enabled
                // let resolved_value = ValueLocation::from_encoded_value(&val.0, &self.core.vlog)
                //     .unwrap_or_else(|_| val.0.to_vec());
                let resolved_value =
                    ValueLocation::from_encoded_value(&val.0, Some(self.core.vlog.as_ref()))
                        .unwrap_or_else(|_| val.0.to_vec());
                Ok(Some(resolved_value.into()))
            }
            None => Ok(None),
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

        // Set the transaction's latest savepoint number and add it to the write set.
        let key = e.key.clone();

        self.write_set.insert(key, Some(e));

        Ok(())
    }

    /// Creates an iterator for a range scan between start and end keys (inclusive).
    pub fn range<K: AsRef<[u8]>>(
        &self,
        start: K,
        end: K,
        limit: Option<usize>,
    ) -> Result<impl Iterator<Item = (Key, Value)> + '_> {
        // Get the start and end keys as byte slices
        let start_key = start.as_ref().to_vec();
        let end_key = end.as_ref().to_vec();

        // Check if keys are empty
        if start_key.is_empty() || end_key.is_empty() {
            return Err(Error::EmptyKey);
        }

        TransactionRangeIterator::new(self, start_key, end_key, limit)
    }

    /// Commits the transaction, by writing all pending entries to the store.
    pub async fn commit(&mut self) -> Result<()> {
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

        // Prepare for commit - uses the new Oracle method
        let _ = self.core.oracle.prepare_commit(self)?;

        // Unregister the transaction start point
        self.core.oracle.unregister_txn_start(self.start_commit_id);

        // Create and prepare batch directly
        let mut batch = Batch::new();

        // Add all entries to the batch
        for entry in self.write_set.values().flatten() {
            batch.add_record(
                entry.kind,
                &entry.key,
                entry.value.as_ref().map(|bytes| bytes.as_ref()),
            )?;
        }

        // Write the batch to storage
        self.core.commit(batch, false).await?;

        // Mark the transaction as closed
        self.closed = true;
        Ok(())
    }

    /// Rolls back the transaction by removing all updated entries.
    pub fn rollback(&mut self) {
        // Only unregister mutable transactions since only they get registered
        if !self.closed && self.mode.mutable() {
            self.core.oracle.unregister_txn_start(self.start_commit_id);
        }

        self.closed = true;
        self.write_set.clear();
        self.snapshot.take();
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.rollback();
    }
}

/// Represents a pending write operation in a transaction's write set
#[derive(Clone)]
pub struct Entry {
    /// The key being written
    pub(crate) key: Bytes,

    /// The value (None for deletes)
    pub(crate) value: Option<Bytes>,

    /// Type of operation (Set, Delete, etc.)
    pub(crate) kind: InternalKeyKind,
}

impl Entry {
    pub fn new(key: &[u8], value: Option<&[u8]>, kind: InternalKeyKind) -> Entry {
        Entry {
            key: Bytes::copy_from_slice(key),
            value: value.map(Bytes::copy_from_slice),
            kind,
        }
    }

    /// Checks if this entry represents a deletion (tombstone)
    pub fn is_tombstone(&self) -> bool {
        let kind = self.kind;
        if kind == InternalKeyKind::Delete || kind == InternalKeyKind::RangeDelete {
            return true;
        }

        false
    }
}

/// An iterator that performs a merging scan over a transaction's snapshot and write set.
pub struct TransactionRangeIterator<'a> {
    /// Iterator over the consistent snapshot
    snapshot_iter: DoubleEndedPeekable<Box<dyn Iterator<Item = (Key, Value)> + 'a>>,

    /// Iterator over the transaction's write set
    write_set_iter: DoubleEndedPeekable<btree_map::Range<'a, Bytes, Option<Entry>>>,

    /// Maximum number of items to return (usize::MAX for unlimited)
    limit: usize,

    /// Number of items returned so far
    count: usize,
}

impl<'a> TransactionRangeIterator<'a> {
    /// Creates a new range iterator with RYOW semantics
    pub fn new(
        tx: &'a Transaction,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: Option<usize>,
    ) -> Result<Self> {
        // Validate transaction state
        if tx.closed {
            return Err(Error::TransactionClosed);
        }

        if tx.mode.is_write_only() {
            return Err(Error::TransactionWriteOnly);
        }

        // Get snapshot
        let snapshot = match &tx.snapshot {
            Some(snap) => snap,
            None => return Err(Error::NoSnapshot),
        };

        // Convert range bounds to Bytes for the write set range
        let start_bytes = Bytes::copy_from_slice(&start_key);
        let end_bytes = Bytes::copy_from_slice(&end_key);

        // Create a snapshot iterator for the range
        let iter = snapshot.range(start_bytes.clone(), end_bytes.clone())?;
        let boxed_iter: Box<dyn Iterator<Item = (Key, Value)> + 'a> = Box::new(iter);

        // Use inclusive range for write set
        let write_set_range = (Bound::Included(start_bytes), Bound::Included(end_bytes));
        let write_set_iter = tx.write_set.range(write_set_range);

        Ok(Self {
            snapshot_iter: boxed_iter.double_ended_peekable(),
            write_set_iter: write_set_iter.double_ended_peekable(),
            limit: limit.unwrap_or(usize::MAX),
            count: 0,
        })
    }

    /// Reads from the write set and skips deleted entries
    fn read_from_write_set(&mut self) -> Option<(Key, Value)> {
        if let Some((ws_key, Some(e))) = self.write_set_iter.next() {
            if e.is_tombstone() {
                // Deleted key, skip it by recursively getting the next entry
                return self.next();
            }

            if let Some(value) = &e.value {
                return Some((ws_key.to_vec().into(), value.to_vec().into()));
            }
        }
        None
    }
}

impl Iterator for TransactionRangeIterator<'_> {
    type Item = (Key, Value);

    /// Merges results from write set and snapshot in key order
    fn next(&mut self) -> Option<Self::Item> {
        if self.count >= self.limit {
            return None;
        }

        // Fast path: if write set is empty, just use snapshot
        if self.write_set_iter.peek().is_none() {
            let result = self.snapshot_iter.next();
            if result.is_some() {
                self.count += 1;
            }
            return result;
        }

        // Fast path: if snapshot is empty, just use write set
        if self.snapshot_iter.peek().is_none() {
            let result = self.read_from_write_set();
            if result.is_some() {
                self.count += 1;
            }
            return result;
        }

        // Merge results from both iterators
        let has_snap = self.snapshot_iter.peek().is_some();
        let has_ws = self.write_set_iter.peek().is_some();

        let result = match (has_snap, has_ws) {
            (false, false) => None,
            (true, false) => self.snapshot_iter.next(),
            (false, true) => self.read_from_write_set(),
            (true, true) => {
                // Compare keys to determine which comes first
                if let (Some((snap_key, _)), Some((ws_key, _))) =
                    (self.snapshot_iter.peek(), self.write_set_iter.peek())
                {
                    match snap_key.as_ref().cmp(ws_key.as_ref()) {
                        Ordering::Less => self.snapshot_iter.next(),
                        Ordering::Greater => self.read_from_write_set(),
                        Ordering::Equal => {
                            // Same key - prioritize write set and skip snapshot
                            self.snapshot_iter.next();
                            self.read_from_write_set()
                        }
                    }
                } else {
                    // This should never happen since we checked above
                    None
                }
            }
        };

        if result.is_some() {
            self.count += 1;
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use bytes::Bytes;

    use crate::{lsm::Tree, Options};

    use super::*;

    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    // Common setup logic for creating a store
    fn create_store() -> (Tree, TempDir) {
        let temp_dir = create_temp_directory();
        let path = temp_dir.path().to_path_buf();

        let opts = Options {
            path: path.clone(),
            ..Default::default()
        };

        let tree = Tree::new(Arc::new(opts)).unwrap();
        (tree, temp_dir)
    }

    #[tokio::test]
    async fn basic_transaction() {
        let (store, _temp_dir) = create_store();

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
            txn1.commit().await.unwrap();
        }

        {
            // Start a read-only transaction (txn3)
            let txn3 = store.begin().unwrap();
            let val = txn3.get(&key1).unwrap().unwrap();
            assert_eq!(val.as_ref(), value1.as_ref());
        }

        {
            // Start another read-write transaction (txn2)
            let mut txn2 = store.begin().unwrap();
            txn2.set(&key1, &value2).unwrap();
            txn2.set(&key2, &value2).unwrap();
            txn2.commit().await.unwrap();
        }

        // Start a read-only transaction (txn4)
        let txn4 = store.begin().unwrap();
        let val = txn4.get(&key1).unwrap().unwrap();

        // Assert that the value retrieved in txn4 matches value2
        assert_eq!(val.as_ref(), value2.as_ref());
    }

    #[tokio::test]
    async fn mvcc_snapshot_isolation() {
        let (store, _) = create_store();

        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");
        let value1 = Bytes::from("baz");
        let value2 = Bytes::from("bar");

        // no conflict
        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            txn1.set(&key1, &value1).unwrap();
            txn1.commit().await.unwrap();

            assert!(txn2.get(&key2).unwrap().is_none());
            txn2.set(&key2, &value2).unwrap();
            txn2.commit().await.unwrap();
        }

        // blind writes should succeed if key wasn't read first
        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            txn1.set(&key1, &value1).unwrap();
            txn2.set(&key1, &value2).unwrap();

            txn1.commit().await.unwrap();
            assert!(match txn2.commit().await {
                Err(err) => {
                    matches!(err, Error::TransactionWriteConflict)
                }
                _ => {
                    false
                }
            });
        }

        // conflict when the read key was updated by another transaction
        {
            let key = Bytes::from("key3");

            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            txn1.set(&key, &value1).unwrap();
            txn1.commit().await.unwrap();

            assert!(txn2.get(&key).unwrap().is_none());
            txn2.set(&key, &value1).unwrap();
            assert!(match txn2.commit().await {
                Err(err) => {
                    matches!(err, Error::TransactionWriteConflict)
                }
                _ => false,
            });
        }
    }

    #[tokio::test]
    async fn ryow() {
        let (store, _) = create_store();

        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let key3 = Bytes::from("k3");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");

        // Set a key, delete it and read it in the same transaction. Should return None.
        {
            // Start a new read-write transaction (txn1)
            let mut txn1 = store.begin().unwrap();
            txn1.set(&key1, &value1).unwrap();
            txn1.delete(&key1).unwrap();
            let res = txn1.get(&key1).unwrap();
            assert!(res.is_none());
            txn1.commit().await.unwrap();
        }

        {
            let mut txn = store.begin().unwrap();
            txn.set(&key1, &value1).unwrap();
            txn.commit().await.unwrap();
        }

        {
            // Start a new read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.set(&key1, &value2).unwrap();
            assert_eq!(txn.get(&key1).unwrap().unwrap().as_ref(), value2.as_ref());
            assert!(txn.get(&key3).unwrap().is_none());
            txn.set(&key2, &value1).unwrap();
            assert_eq!(txn.get(&key2).unwrap().unwrap().as_ref(), value1.as_ref());
            txn.commit().await.unwrap();
        }
    }

    // Common setup logic for creating a store
    async fn create_hermitage_store() -> Tree {
        let (store, _) = create_store();

        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");
        // Start a new read-write transaction (txn)
        let mut txn = store.begin().unwrap();
        txn.set(&key1, &value1).unwrap();
        txn.set(&key2, &value2).unwrap();
        txn.commit().await.unwrap();

        store
    }

    // The following tests are taken from hermitage (https://github.com/ept/hermitage)
    // Specifically, the tests are derived from FoundationDB tests: https://github.com/ept/hermitage/blob/master/foundationdb.md

    // G0: Write Cycles (dirty writes)
    #[tokio::test]
    async fn g0_tests() {
        let store = create_hermitage_store().await;
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

            txn1.commit().await.unwrap();

            txn2.set(&key2, &value6).unwrap();
            assert!(match txn2.commit().await {
                Err(err) => {
                    matches!(err, Error::TransactionWriteConflict)
                }
                _ => false,
            });
        }

        {
            let txn3 = store.begin().unwrap();
            let val1 = txn3.get(&key1).unwrap().unwrap();
            assert_eq!(val1.as_ref(), value3.as_ref());
            let val2 = txn3.get(&key2).unwrap().unwrap();
            assert_eq!(val2.as_ref(), value5.as_ref());
        }
    }

    // P4: Lost Update
    #[tokio::test]
    async fn p4() {
        let store = create_hermitage_store().await;

        let key1 = Bytes::from("k1");
        let value3 = Bytes::from("v3");

        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            assert!(txn1.get(&key1).is_ok());
            assert!(txn2.get(&key1).is_ok());

            txn1.set(&key1, &value3).unwrap();
            txn2.set(&key1, &value3).unwrap();

            txn1.commit().await.unwrap();

            assert!(match txn2.commit().await {
                Err(err) => {
                    matches!(err, Error::TransactionWriteConflict)
                }
                _ => false,
            });
        }
    }

    // G-single: Single Anti-dependency Cycles (read skew)
    async fn g_single_tests() {
        let store = create_hermitage_store().await;

        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");
        let value3 = Bytes::from("v3");
        let value4 = Bytes::from("v4");

        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            assert_eq!(txn1.get(&key1).unwrap().unwrap().as_ref(), value1.as_ref());
            assert_eq!(txn2.get(&key1).unwrap().unwrap().as_ref(), value1.as_ref());
            assert_eq!(txn2.get(&key2).unwrap().unwrap().as_ref(), value2.as_ref());
            txn2.set(&key1, &value3).unwrap();
            txn2.set(&key2, &value4).unwrap();

            txn2.commit().await.unwrap();

            assert_eq!(txn1.get(&key2).unwrap().unwrap().as_ref(), value2.as_ref());
            txn1.commit().await.unwrap();
        }
    }

    #[tokio::test]
    async fn g_single() {
        g_single_tests().await;
    }

    fn require_send<T: Send>(_: T) {}
    fn require_sync<T: Sync + Send>(_: T) {}

    #[tokio::test]
    async fn is_send_sync() {
        let (db, _) = create_store();

        let txn = db.begin().unwrap();
        require_send(txn);

        let txn = db.begin().unwrap();
        require_sync(txn);
    }

    const ENTRIES: usize = 400_000;
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

    #[tokio::test]
    #[ignore]
    async fn insert_large_txn_and_get() {
        let store = create_hermitage_store().await;

        let mut rng = make_rng();

        let mut txn = store.begin().unwrap();
        for _ in 0..ENTRIES {
            let (key, value) = gen_pair(&mut rng);
            txn.set(&key, &value).unwrap();
        }
        txn.commit().await.unwrap();
        drop(txn);

        // Read the keys from the store
        let mut rng = make_rng();
        let txn = store.begin_with_mode(Mode::ReadOnly).unwrap();
        for _i in 0..ENTRIES {
            let (key, _) = gen_pair(&mut rng);
            txn.get(&key).unwrap();
        }
    }

    #[tokio::test]
    async fn sdb_delete_record_id_bug() {
        let (store, _) = create_store();

        // Define key-value pairs for the test
        let key1 = Bytes::copy_from_slice(&[
            47, 33, 110, 100, 166, 192, 229, 30, 101, 24, 73, 242, 185, 36, 233, 242, 54, 96, 72,
            52,
        ]);
        let key2 = Bytes::copy_from_slice(&[
            47, 33, 104, 98, 0, 0, 1, 141, 141, 42, 113, 8, 47, 166, 192, 229, 30, 101, 24, 73,
            242, 185, 36, 233, 242, 54, 96, 72, 52,
        ]);
        let value1 = Bytes::from("baz");

        {
            // Start a new read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.set(&key1, &value1).unwrap();
            txn.set(&key2, &value1).unwrap();
            txn.commit().await.unwrap();
        }

        let key3 = Bytes::copy_from_slice(&[47, 33, 117, 115, 114, 111, 111, 116, 0]);
        {
            // Start a new read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.set(&key3, &value1).unwrap();
            txn.commit().await.unwrap();
        }

        let key4 = Bytes::copy_from_slice(&[47, 33, 117, 115, 114, 111, 111, 116, 0]);
        let txn1 = store.begin().unwrap();
        txn1.get(&key4).unwrap();

        {
            let mut txn2 = store.begin().unwrap();
            txn2.get(&Bytes::copy_from_slice(&[
                47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72,
                50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 33, 116,
                98, 117, 115, 101, 114, 0,
            ]))
            .unwrap();
            txn2.get(&Bytes::copy_from_slice(&[
                47, 33, 110, 115, 116, 101, 115, 116, 45, 110, 115, 0,
            ]))
            .unwrap();
            txn2.get(&Bytes::copy_from_slice(&[
                47, 33, 110, 115, 116, 101, 115, 116, 45, 110, 115, 0,
            ]))
            .unwrap();
            txn2.set(
                &Bytes::copy_from_slice(&[47, 33, 110, 115, 116, 101, 115, 116, 45, 110, 115, 0]),
                &value1,
            )
            .unwrap();

            txn2.get(&Bytes::copy_from_slice(&[
                47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72,
                71, 72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
            ]))
            .unwrap();
            txn2.get(&Bytes::copy_from_slice(&[
                47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72,
                71, 72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
            ]))
            .unwrap();
            txn2.set(
                &Bytes::copy_from_slice(&[
                    47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54,
                    72, 71, 72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74,
                    69, 0,
                ]),
                &value1,
            )
            .unwrap();

            txn2.get(&Bytes::copy_from_slice(&[
                47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72,
                50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 33, 116,
                98, 117, 115, 101, 114, 0,
            ]))
            .unwrap();
            txn2.get(&Bytes::copy_from_slice(&[
                47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72,
                50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 42, 117,
                115, 101, 114, 0, 42, 0, 0, 0, 1, 106, 111, 104, 110, 0,
            ]))
            .unwrap();
            txn2.set(
                &Bytes::copy_from_slice(&[
                    47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71,
                    72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
                    42, 117, 115, 101, 114, 0, 42, 0, 0, 0, 1, 106, 111, 104, 110, 0,
                ]),
                &value1,
            )
            .unwrap();

            txn2.get(&Bytes::copy_from_slice(&[
                47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72,
                71, 72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
            ]))
            .unwrap();

            txn2.commit().await.unwrap();
        }

        {
            let mut txn3 = store.begin().unwrap();
            txn3.get(&Bytes::copy_from_slice(&[
                47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72,
                50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 42, 117,
                115, 101, 114, 0, 42, 0, 0, 0, 1, 106, 111, 104, 110, 0,
            ]))
            .unwrap();
            txn3.get(&Bytes::copy_from_slice(&[
                47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72,
                50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 33, 116,
                98, 117, 115, 101, 114, 0,
            ]))
            .unwrap();
            txn3.delete(&Bytes::copy_from_slice(&[
                47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72,
                50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 42, 117,
                115, 101, 114, 0, 42, 0, 0, 0, 1, 106, 111, 104, 110, 0,
            ]))
            .unwrap();
            txn3.get(&Bytes::copy_from_slice(&[
                47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72,
                71, 72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
            ]))
            .unwrap();
            txn3.get(&Bytes::copy_from_slice(&[
                47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72,
                71, 72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
            ]))
            .unwrap();
            txn3.set(
                &Bytes::copy_from_slice(&[
                    47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54,
                    72, 71, 72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74,
                    69, 0,
                ]),
                &value1,
            )
            .unwrap();
            txn3.commit().await.unwrap();
        }
    }

    #[tokio::test]
    async fn transaction_delete_from_index() {
        let (store, _) = create_store();

        // Define key-value pairs for the test
        let key1 = Bytes::from("foo1");
        let value = Bytes::from("baz");
        let key2 = Bytes::from("foo2");

        {
            // Start a new read-write transaction (txn1)
            let mut txn1 = store.begin().unwrap();
            txn1.set(&key1, &value).unwrap();
            txn1.set(&key2, &value).unwrap();
            txn1.commit().await.unwrap();
        }

        {
            // Start another read-write transaction (txn2)
            let mut txn2 = store.begin().unwrap();
            txn2.delete(&key1).unwrap();
            txn2.commit().await.unwrap();
        }

        {
            // Start a read-only transaction (txn3)
            let txn3 = store.begin().unwrap();
            let val = txn3.get(&key1).unwrap();
            assert!(val.is_none());
            let val = txn3.get(&key2).unwrap().unwrap();
            assert_eq!(val.as_ref(), value.as_ref());
        }

        // Start a read-only transaction (txn4)
        let txn4 = store.begin().unwrap();
        let val = txn4.get(&key1).unwrap();
        assert!(val.is_none());
        let val = txn4.get(&key2).unwrap().unwrap();
        assert_eq!(val.as_ref(), value.as_ref());
    }

    #[tokio::test]
    async fn test_insert_delete_read_key() {
        let (store, _) = create_store();

        // Key-value pair for the test
        let key = Bytes::from("test_key");
        let value1 = Bytes::from("test_value1");
        let value2 = Bytes::from("test_value2");

        // Insert key-value pair in a new transaction
        {
            let mut txn = store.begin().unwrap();
            txn.set(&key, &value1).unwrap();
            txn.commit().await.unwrap();
        }

        {
            let mut txn = store.begin().unwrap();
            txn.set(&key, &value2).unwrap();
            txn.commit().await.unwrap();
        }

        // Clear the key in a separate transaction
        {
            let mut txn = store.begin().unwrap();
            txn.delete(&key).unwrap();
            txn.commit().await.unwrap();
        }

        // Read the key in a new transaction to verify it does not exist
        {
            let txn = store.begin().unwrap();
            assert!(txn.get(&key).unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_range_basic_functionality() {
        let (store, _temp_dir) = create_store();

        // Insert some initial data
        {
            let mut tx = store.begin().unwrap();
            tx.set(b"key1", b"value1").unwrap();
            tx.set(b"key2", b"value2").unwrap();
            tx.set(b"key3", b"value3").unwrap();
            tx.set(b"key4", b"value4").unwrap();
            tx.set(b"key5", b"value5").unwrap();
            tx.commit().await.unwrap();
        }

        // Test basic range scan
        {
            let tx = store.begin().unwrap();
            let range: Vec<_> = tx
                .range(b"key2", b"key4", None)
                .unwrap()
                .collect::<Vec<_>>();

            assert_eq!(range.len(), 3);
            assert_eq!(range[0].0.as_ref(), b"key2");
            assert_eq!(range[0].1.as_ref(), b"value2");
            assert_eq!(range[1].0.as_ref(), b"key3");
            assert_eq!(range[1].1.as_ref(), b"value3");
            assert_eq!(range[2].0.as_ref(), b"key4");
            assert_eq!(range[2].1.as_ref(), b"value4");
        }
    }

    #[tokio::test]
    async fn test_range_with_limit() {
        let (store, _temp_dir) = create_store();

        // Insert test data
        {
            let mut tx = store.begin().unwrap();
            for i in 1..=10 {
                let key = format!("key{:02}", i);
                let value = format!("value{}", i);
                tx.set(key.as_bytes(), value.as_bytes()).unwrap();
            }
            tx.commit().await.unwrap();
        }

        // Test with limit
        {
            let tx = store.begin().unwrap();
            let range: Vec<_> = tx
                .range(b"key01", b"key10", Some(3))
                .unwrap()
                .collect::<Vec<_>>();

            assert_eq!(range.len(), 3);
            assert_eq!(range[0].0.as_ref(), b"key01");
            assert_eq!(range[1].0.as_ref(), b"key02");
            assert_eq!(range[2].0.as_ref(), b"key03");
        }
    }

    #[tokio::test]
    async fn test_range_read_your_own_writes() {
        let (store, _temp_dir) = create_store();

        // Insert initial data
        {
            let mut tx = store.begin().unwrap();
            tx.set(b"a", b"1").unwrap();
            tx.set(b"c", b"3").unwrap();
            tx.set(b"e", b"5").unwrap();
            tx.commit().await.unwrap();
        }

        // Test RYOW - uncommitted writes should be visible in range
        {
            let mut tx = store.begin().unwrap();

            // Add new keys
            tx.set(b"b", b"2").unwrap();
            tx.set(b"d", b"4").unwrap();

            // Modify existing key
            tx.set(b"c", b"3_modified").unwrap();

            // Range should see all changes
            let range: Vec<_> = tx.range(b"a", b"e", None).unwrap().collect::<Vec<_>>();

            assert_eq!(range.len(), 5);
            assert_eq!(range[0], (b"a".to_vec().into(), b"1".to_vec().into()));
            assert_eq!(range[1], (b"b".to_vec().into(), b"2".to_vec().into()));
            assert_eq!(
                range[2],
                (b"c".to_vec().into(), b"3_modified".to_vec().into())
            );
            assert_eq!(range[3], (b"d".to_vec().into(), b"4".to_vec().into()));
            assert_eq!(range[4], (b"e".to_vec().into(), b"5".to_vec().into()));
        }
    }

    #[tokio::test]
    async fn test_range_with_deletes() {
        let (store, _temp_dir) = create_store();

        // Insert initial data
        {
            let mut tx = store.begin().unwrap();
            tx.set(b"key1", b"value1").unwrap();
            tx.set(b"key2", b"value2").unwrap();
            tx.set(b"key3", b"value3").unwrap();
            tx.set(b"key4", b"value4").unwrap();
            tx.set(b"key5", b"value5").unwrap();
            tx.commit().await.unwrap();
        }

        // Test range with deletes in write set
        {
            let mut tx = store.begin().unwrap();

            // Delete some keys
            tx.delete(b"key2").unwrap();
            tx.delete(b"key4").unwrap();

            // Range should not see deleted keys
            let range: Vec<_> = tx
                .range(b"key1", b"key5", None)
                .unwrap()
                .collect::<Vec<_>>();

            assert_eq!(range.len(), 3);
            assert_eq!(range[0].0.as_ref(), b"key1");
            assert_eq!(range[1].0.as_ref(), b"key3");
            assert_eq!(range[2].0.as_ref(), b"key5");
        }
    }

    #[tokio::test]
    async fn test_range_delete_then_set() {
        let (store, _temp_dir) = create_store();

        // Insert initial data
        {
            let mut tx = store.begin().unwrap();
            tx.set(b"key1", b"value1").unwrap();
            tx.set(b"key2", b"value2").unwrap();
            tx.set(b"key3", b"value3").unwrap();
            tx.commit().await.unwrap();
        }

        // Test delete followed by set
        {
            let mut tx = store.begin().unwrap();

            // Delete then re-add with new value
            tx.delete(b"key2").unwrap();
            tx.set(b"key2", b"new_value2").unwrap();

            // Range should see the new value
            let range: Vec<_> = tx
                .range(b"key1", b"key3", None)
                .unwrap()
                .collect::<Vec<_>>();

            assert_eq!(range.len(), 3);
            assert_eq!(
                range[1],
                (b"key2".to_vec().into(), b"new_value2".to_vec().into())
            );
        }
    }

    #[tokio::test]
    async fn test_range_empty_result() {
        let (store, _temp_dir) = create_store();

        // Insert data outside the range we'll query
        {
            let mut tx = store.begin().unwrap();
            tx.set(b"a", b"1").unwrap();
            tx.set(b"z", b"26").unwrap();
            tx.commit().await.unwrap();
        }

        // Query range with no data
        {
            let tx = store.begin().unwrap();
            let range: Vec<_> = tx.range(b"m", b"n", None).unwrap().collect::<Vec<_>>();

            assert_eq!(range.len(), 0);
        }
    }

    #[tokio::test]
    async fn test_range_ordering() {
        let (store, _temp_dir) = create_store();

        // Insert data in non-sequential order
        {
            let mut tx = store.begin().unwrap();
            tx.set(b"key5", b"value5").unwrap();
            tx.set(b"key1", b"value1").unwrap();
            tx.set(b"key3", b"value3").unwrap();
            tx.set(b"key2", b"value2").unwrap();
            tx.set(b"key4", b"value4").unwrap();
            tx.commit().await.unwrap();
        }

        // Verify correct ordering in range
        {
            let tx = store.begin().unwrap();
            let range: Vec<_> = tx
                .range(b"key1", b"key5", None)
                .unwrap()
                .collect::<Vec<_>>();

            assert_eq!(range.len(), 5);
            for (i, item) in range.iter().enumerate().take(5) {
                let expected_key = format!("key{}", i + 1);
                assert_eq!(item.0.as_ref(), expected_key.as_bytes());
            }
        }
    }

    #[tokio::test]
    async fn test_range_boundary_conditions() {
        let (store, _temp_dir) = create_store();

        // Insert test data
        {
            let mut tx = store.begin().unwrap();
            tx.set(b"key1", b"value1").unwrap();
            tx.set(b"key2", b"value2").unwrap();
            tx.set(b"key3", b"value3").unwrap();
            tx.commit().await.unwrap();
        }

        // Test inclusive boundaries
        {
            let tx = store.begin().unwrap();

            // Range includes both start and end
            let range: Vec<_> = tx
                .range(b"key1", b"key3", None)
                .unwrap()
                .collect::<Vec<_>>();

            assert_eq!(range.len(), 3);
            assert_eq!(range[0].0.as_ref(), b"key1");
            assert_eq!(range[2].0.as_ref(), b"key3");
        }

        // Test single key range
        {
            let tx = store.begin().unwrap();
            let range: Vec<_> = tx
                .range(b"key2", b"key2", None)
                .unwrap()
                .collect::<Vec<_>>();

            assert_eq!(range.len(), 1);
            assert_eq!(range[0].0.as_ref(), b"key2");
        }
    }

    #[tokio::test]
    async fn test_range_write_sequence_order() {
        let (store, _temp_dir) = create_store();

        // Test that multiple writes to same key show latest value
        {
            let mut tx = store.begin().unwrap();

            tx.set(b"key", b"value1").unwrap();
            tx.set(b"key", b"value2").unwrap();
            tx.set(b"key", b"value3").unwrap();

            let range: Vec<_> = tx.range(b"key", b"key", None).unwrap().collect::<Vec<_>>();

            assert_eq!(range.len(), 1);
            assert_eq!(range[0].1.as_ref(), b"value3"); // Latest value
        }
    }
}
