use std::collections::HashSet;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
use bytes::Bytes;
use std::collections::hash_map::Entry as HashEntry;
use vart::{art::QueryType, VariableSizeKey};

use crate::storage::kv::{
    entry::Entry,
    error::{Error, Result},
    indexer::IndexValue,
    snapshot::{FilterFn, Snapshot, FILTERS},
    store::Core,
    util::{convert_range_bounds, now},
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

/// ScanResult is a tuple containing the key, value, timestamp, and commit timestamp of a key-value pair.
pub type ScanResult = (Vec<u8>, Vec<u8>, u64, u64);

/// ScanResult is a tuple containing the key, value, timestamp, and info about whether the key is deleted.
pub type ScanVersionResult = (Vec<u8>, Vec<u8>, u64, bool);

#[derive(Default, Debug, Copy, Clone)]
pub enum Durability {
    /// Commits with this durability level are guaranteed to be persistent eventually. The data
    /// is written to the disk, but it is not fsynced before returning from [Transaction::commit].
    #[default]
    Eventual,

    /// Commits with this durability level are guaranteed to be persistent as soon as
    /// [Transaction::commit] returns.
    ///
    /// Data is fsynced to disk before returning from [Transaction::commit]. This is the slowest
    /// durability level, but it is the safest.
    Immediate,
}

pub(crate) struct WriteSetEntry {
    pub(crate) e: Entry,
    savepoint_no: u32,
    seqno: u32,
}

impl WriteSetEntry {
    pub(crate) fn new(e: Entry, savepoint_no: u32, seqno: u32) -> Self {
        Self {
            e,
            savepoint_no,
            seqno,
        }
    }
}

pub(crate) struct ReadSetEntry {
    pub(crate) key: Bytes,
    pub(crate) ts: u64,
    pub(crate) savepoint_no: u32,
}

impl ReadSetEntry {
    pub(crate) fn new(key: Bytes, ts: u64, savepoint_no: u32) -> Self {
        Self {
            key,
            ts,
            savepoint_no,
        }
    }
}

pub(crate) struct ReadScanEntry {
    pub(crate) start: Bound<VariableSizeKey>,
    pub(crate) end: Bound<VariableSizeKey>,
    pub(crate) savepoint_no: u32,
}

impl ReadScanEntry {
    pub(crate) fn new(
        start: Bound<VariableSizeKey>,
        end: Bound<VariableSizeKey>,
        savepoint_no: u32,
    ) -> Self {
        Self {
            start,
            end,
            savepoint_no,
        }
    }
}

/// `Transaction` is a struct representing a transaction in a database.
pub struct Transaction {
    /// `read_ts` is the read timestamp of the transaction. This is the time at which the transaction started.
    pub(crate) read_ts: u64,

    /// `mode` is the transaction mode. This can be either `ReadWrite`, `ReadOnly`, or `WriteOnly`.
    mode: Mode,

    /// `snapshot` is the snapshot that the transaction is running in. This is a consistent view of the data
    ///  at the time the transaction started.
    pub(crate) snapshot: Option<Snapshot>,

    /// `core` is the underlying core for the transaction. This is shared between transactions.
    pub(crate) core: Arc<Core>,

    /// `write_set` is a map of keys to entries.
    /// These are the changes that the transaction intends to make to the data.
    /// The entries vec is used to keep different values for the same key for
    /// savepoints and rollbacks.
    pub(crate) write_set: HashMap<Bytes, Vec<WriteSetEntry>>,

    /// `read_set` is the keys that are read in the transaction from the snapshot. This is used for conflict detection.
    pub(crate) read_set: Vec<ReadSetEntry>,

    /// `read_key_ranges` is the key ranges that are read in the transaction from the snapshot. This is used for conflict detection.
    pub(crate) read_key_ranges: Vec<ReadScanEntry>,

    /// `durability` is the durability level of the transaction. This is used to determine how the transaction is committed.
    durability: Durability,

    /// `closed` indicates if the transaction is closed. A closed transaction cannot make any more changes to the data.
    closed: bool,

    /// `savepoints` indicates the current number of stacked savepoints; zero means none.
    savepoints: u32,

    /// write sequence number is used for real-time ordering of writes within a transaction.
    write_seqno: u32,

    /// `versionstamp` is a combination of the transaction ID and the commit timestamp. For internal use only.
    versionstamp: Option<(u64, u64)>,
}

impl Transaction {
    /// Prepare a new transaction in the given mode.
    pub fn new(core: Arc<Core>, mode: Mode) -> Result<Self> {
        let mut read_ts = core.read_ts();

        let mut snapshot = None;
        if !mode.is_write_only() {
            let snap = Snapshot::take(&core)?;
            // The version with which the snapshot was
            // taken supersedes the version taken above.
            read_ts = snap.version - 1;
            snapshot = Some(snap);
        }

        Ok(Self {
            read_ts,
            mode,
            snapshot,
            core,
            write_set: HashMap::new(),
            read_set: Vec::new(),
            read_key_ranges: Vec::new(),
            durability: Durability::Eventual,
            closed: false,
            savepoints: 0,
            write_seqno: 0,
            versionstamp: None,
        })
    }

    /// Bump the write sequence number and return it.
    fn next_write_seqno(&mut self) -> u32 {
        self.write_seqno += 1;
        self.write_seqno
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
        let mut entry = Entry::new(key, value);
        // Replace when versions are disabled.
        entry.set_replace(!self.core.opts.enable_versions);
        self.write(entry)?;
        Ok(())
    }

    /// Inserts if not present or replaces an existing key-value pair.
    pub fn insert_or_replace(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut entry = Entry::new(key, value);
        entry.set_replace(true);
        self.write(entry)?;
        Ok(())
    }

    /// Adds a key-value pair to the store with the given timestamp.
    pub fn set_at_ts(&mut self, key: &[u8], value: &[u8], ts: u64) -> Result<()> {
        let mut entry = Entry::new(key, value);
        entry.set_ts(ts);
        // Replace when versions are disabled.
        entry.set_replace(!self.core.opts.enable_versions);
        self.write(entry)?;
        Ok(())
    }

    /// Delete all the versions of a key. This is a hard delete.
    /// This will remove the key from the index and disk.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let value = Bytes::new();
        let mut entry = Entry::new(key, &value);
        entry.mark_delete();
        self.write(entry)?;
        Ok(())
    }

    /// Mark all versions of a key as deleted. This is a soft delete,
    /// and does not remove the key from the index, or disk, rather
    /// just marks it as deleted, and the key will remain hidden.
    pub fn soft_delete(&mut self, key: &[u8]) -> Result<()> {
        let value = Bytes::new();
        let mut entry = Entry::new(key, &value);
        entry.mark_tombstone();
        // Replace when versions are disabled.
        entry.set_replace(!self.core.opts.enable_versions);
        self.write(entry)?;
        Ok(())
    }

    /// Gets a value for a key if it exists.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // If the transaction is closed, return an error.
        if self.closed {
            return Err(Error::TransactionClosed);
        }
        // If the key is empty, return an error.
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        // Do not allow reads if it is a write-only transaction
        if self.mode.is_write_only() {
            return Err(Error::TransactionWriteOnly);
        }

        // RYOW semantics: Read your own writes. If the value is in the write set, return it.
        if let Some(last_entry) = self.write_set.get(key).and_then(|entries| entries.last()) {
            let val = if last_entry.e.is_deleted_or_tombstone() {
                None
            } else {
                Some(last_entry.e.value.clone().to_vec())
            };
            return Ok(val);
        }

        // The value is not in the write set, so attempt to get it from the snapshot.
        match self.snapshot.as_ref().unwrap().get(&key[..].into()) {
            Ok((val, version)) => {
                // If the transaction is not read-only and the value reference has a version greater than 0,
                // add the key and its version to the read set for conflict detection.
                if !self.mode.is_read_only() && version > 0 {
                    let key = Bytes::copy_from_slice(key);
                    let entry = ReadSetEntry::new(key, version, self.savepoints);
                    self.read_set.push(entry);
                }

                // Resolve the value reference to get the actual value.
                val.resolve(&self.core).map(Some)
            }
            Err(e) => {
                match &e {
                    // If the key is not found in the index, and the transaction is not read-only,
                    // add the key to the read set with a timestamp of 0.
                    Error::KeyNotFound => {
                        if !self.mode.is_read_only() {
                            let key = Bytes::copy_from_slice(key);
                            let entry = ReadSetEntry::new(key, 0, self.savepoints);
                            self.read_set.push(entry);
                        }
                        Ok(None)
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

        // If the transaction mode is not write-only, update the snapshot.
        if !self.mode.is_write_only() {
            let index_value = IndexValue::new_mem(e.metadata.clone(), e.value.clone());

            // Set the key-value pair in the snapshot.
            self.snapshot
                .as_mut()
                .unwrap()
                .set(&e.key[..].into(), index_value);
        }

        // Set the transaction's latest savepoint number and add it to the write set.
        let key = e.key.clone();
        let write_seqno = self.next_write_seqno();
        let ws_entry = WriteSetEntry::new(e, self.savepoints, write_seqno);
        match self.write_set.entry(key) {
            HashEntry::Occupied(mut oe) => {
                let entries = oe.get_mut();
                // If the latest existing value for this key belongs to the same
                // savepoint as the value we are about to write, then we can
                // overwrite it with the new value.
                if let Some(last_entry) = entries.last_mut() {
                    if last_entry.savepoint_no == ws_entry.savepoint_no {
                        *last_entry = ws_entry;
                    } else {
                        entries.push(ws_entry);
                    }
                } else {
                    entries.push(ws_entry)
                }
            }
            HashEntry::Vacant(ve) => {
                ve.insert(vec![ws_entry]);
            }
        };

        Ok(())
    }

    /// Scans a range of keys and returns a vector of tuples containing the value, version, and timestamp for each key.
    pub fn scan<'b, R>(&'b mut self, range: R, limit: Option<usize>) -> Result<Vec<ScanResult>>
    where
        R: RangeBounds<&'b [u8]>,
    {
        // Convert the range to a tuple of bounds of variable keys.
        let range = convert_range_bounds(range);

        // Keep track of the range bound predicates for conflict detection in case of SSI.
        {
            let range_start = match range.start_bound() {
                Bound::Included(start) => Bound::Included((*start).clone()),
                Bound::Excluded(start) => Bound::Included((*start).clone()),
                Bound::Unbounded => Bound::Unbounded,
            };

            let range_end = match range.end_bound() {
                Bound::Included(end) => Bound::Included((*end).clone()),
                Bound::Excluded(end) => Bound::Included((*end).clone()),
                Bound::Unbounded => Bound::Unbounded,
            };

            let rs_entry = ReadScanEntry::new(range_start, range_end, self.savepoints);
            self.read_key_ranges.push(rs_entry);
        }

        // Initialize an empty vector to store the results.
        let mut results = Vec::new();

        // Get a range iterator for the specified range.
        let snap = self.snapshot.as_ref().unwrap();
        let ranger = snap.range(range);

        // Iterate over the keys in the range.
        'outer: for (key, value, version, ts) in ranger {
            // If a limit is set and we've already got enough results, break the loop.
            if let Some(limit) = limit {
                if results.len() >= limit {
                    break;
                }
            }

            // Apply all filters. If any filter fails, skip this key and continue with the next one.
            for filter in &FILTERS {
                if filter.apply(value).is_err() {
                    continue 'outer;
                }
            }

            // Only add the key to the read set if the version is less than or equal to the
            // read timestamp. This is to prevent adding keys that are added during the transaction.
            if *version <= self.read_ts {
                let key = Bytes::copy_from_slice(&key);
                let entry = ReadSetEntry::new(key, *version, self.savepoints);
                self.read_set.push(entry);
            }

            // Resolve the value reference to get the actual value.
            let v = value.resolve(&self.core)?;

            // Add the value, version, and timestamp to the results vector.
            results.push((key, v, *version, *ts));
        }

        // Return the results.
        Ok(results)
    }

    /// Returns all existing keys within the specified range, including soft-deleted
    /// and thus hidden by tombstones.
    /// The returned keys are not added to the read set and will not cause read-write conflicts.
    pub fn keys_with_tombstones<'b, R>(&'b self, range: R) -> Result<Vec<Vec<u8>>>
    where
        R: RangeBounds<&'b [u8]>,
    {
        // Convert the range to a tuple of bounds of variable keys.
        let range = convert_range_bounds(range);
        let keys = self.snapshot.as_ref().unwrap().range(range);
        let result = keys.into_iter().map(|(key, _, _, _)| key).collect();

        Ok(result)
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
            return Ok(()); // Return when there's nothing to commit
        }

        // Lock the oracle to serialize commits to the transaction log.
        let oracle = self.core.oracle.clone();
        let write_ch_lock = oracle.write_lock.lock().await;

        // Prepare for the commit by getting a transaction ID.
        let (tx_id, commit_ts) = self.prepare_commit()?;

        // Extract the vector of entries for the current transaction,
        // respecting the insertion order recorded with WriteSetEntry::seqno.
        let mut latest_writes: Vec<WriteSetEntry> = std::mem::take(&mut self.write_set)
            .into_values()
            .filter_map(|mut entries| entries.pop())
            .collect();
        latest_writes.sort_by(|a, b| a.seqno.cmp(&b.seqno));
        let entries: Vec<Entry> = latest_writes
            .into_iter()
            .map(|ws_entry| ws_entry.e)
            .collect();

        // Commit the changes to the store index.
        let done = self
            .core
            .send_to_write_channel(entries, tx_id, self.durability)
            .await;

        if let Err(err) = done {
            oracle.committed_upto(tx_id);
            return Err(err);
        }

        drop(write_ch_lock);

        // Check if the transaction is written to the transaction log.
        let done = done.unwrap();
        let ret = done.recv().await;
        if let Err(err) = ret {
            oracle.committed_upto(tx_id);
            return Err(err.into());
        }

        // Update the oracle to indicate that the transaction has been committed up to the given transaction ID.
        oracle.committed_upto(tx_id);

        // Mark the transaction as closed.
        self.closed = true;

        // Save the versionstamp for internal use.
        self.versionstamp = Some((tx_id, commit_ts));

        // Return the transaction ID and commit timestamp.
        ret.unwrap()
    }

    /// Prepares for the commit by assigning commit timestamps and preparing records.
    fn prepare_commit(&mut self) -> Result<(u64, u64)> {
        let oracle = self.core.oracle.clone();
        let tx_id = oracle.new_commit_ts(self)?;
        let commit_ts = self.assign_commit_ts();
        Ok((tx_id, commit_ts))
    }

    /// Assigns commit timestamps to transaction entries.
    fn assign_commit_ts(&mut self) -> u64 {
        let commit_ts = now();
        for entries in self.write_set.values_mut() {
            if let Some(entry) = entries.last_mut() {
                if entry.e.ts == 0 {
                    entry.e.ts = commit_ts;
                }
            }
        }
        commit_ts
    }

    /// Rolls back the transaction by removing all updated entries.
    pub fn rollback(&mut self) {
        self.closed = true;
        self.write_set.clear();
        self.read_set.clear();
        self.snapshot.take();
        self.savepoints = 0;
        self.write_seqno = 0;
    }

    /// After calling this method the subsequent modifications within this
    /// transaction can be rolled back by calling [`rollback_to_savepoint`].
    ///
    /// This method is stackable and can be called multiple times with the
    /// corresponding calls to [`rollback_to_savepoint`].
    ///
    /// [`rollback_to_savepoint`]: Transaction::rollback_to_savepoint
    pub fn set_savepoint(&mut self) -> Result<()> {
        // If the transaction mode is not mutable (i.e., it's read-only), return an error.
        if !self.mode.mutable() {
            return Err(Error::TransactionReadOnly);
        }
        // If the transaction is closed, return an error.
        if self.closed {
            return Err(Error::TransactionClosed);
        }

        // Bump the latest savepoint number.
        self.savepoints += 1;

        Ok(())
    }

    /// Rollback the state of the transaction to the latest savepoint set by
    /// calling [`set_savepoint`].
    ///
    /// [`set_savepoint`]: Transaction::set_savepoint
    pub fn rollback_to_savepoint(&mut self) -> Result<()> {
        // If the transaction mode is not mutable (i.e., it's read-only), return an error.
        if !self.mode.mutable() {
            return Err(Error::TransactionReadOnly);
        }
        // If the transaction is closed, return an error.
        if self.closed {
            return Err(Error::TransactionClosed);
        }
        // Check that the savepoint is set
        if self.savepoints == 0 {
            return Err(Error::TransactionWithoutSavepoint);
        }

        // For every key in the write set, remove entries marked
        // for rollback since the last call to set_savepoint()
        // from its vec and the snapshot.
        // HashMap does not really like when items are removed,
        // so let's just keep empty `entries` vectors around.
        for entries in self.write_set.values_mut() {
            let pre_retain_len = entries.len();
            entries.retain(|entry| {
                if entry.savepoint_no == self.savepoints {
                    self.snapshot
                        .as_mut()
                        .unwrap()
                        .delete(&entry.e.key[..].into());
                    false
                } else {
                    true
                }
            });

            // If the transaction's mode is write-only, or if no values for this
            // key were removed due to the rollback, continue to the next key.
            if pre_retain_len == entries.len() || self.mode.is_write_only() {
                continue;
            }

            // Otherwise, the value just before the rollback needs to be added
            // back to the snapshot in order to keep scan() working.
            if let Some(latest_entry) = entries.last() {
                let index_value = IndexValue::new_mem(
                    latest_entry.e.metadata.clone(),
                    latest_entry.e.value.clone(),
                );
                self.snapshot
                    .as_mut()
                    .unwrap()
                    .set(&latest_entry.e.key[..].into(), index_value);
            }
        }

        // Remove marked entries from the read set to
        // prevent unnecessary read-write conflicts.
        self.read_set
            .retain(|entry| entry.savepoint_no != self.savepoints);

        // And also from the read scan set.
        self.read_key_ranges
            .retain(|entry| entry.savepoint_no != self.savepoints);

        // Decrement the latest savepoint number unless it's zero.
        // Cannot undeflow due to the zero check above.
        self.savepoints -= 1;

        Ok(())
    }
}

/// Implement Versioned APIs for read-only transactions.
/// These APIs do not take part in conflict detection.
impl Transaction {
    fn ensure_read_only_transaction(&self) -> Result<()> {
        // If the transaction is closed, return an error.
        if self.closed {
            return Err(Error::TransactionClosed);
        }
        // Do not allow versioned reads if it is not a read-only transaction
        if !self.mode.is_read_only() {
            return Err(Error::TransactionMustBeReadOnly);
        }
        Ok(())
    }

    /// Returns the value associated with the key at the given timestamp.
    pub fn get_at_ts(&self, key: &[u8], ts: u64) -> Result<Option<Vec<u8>>> {
        // If the key is empty, return an error.
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        // Attempt to get the value for the key from the snapshot.
        match self
            .snapshot
            .as_ref()
            .unwrap()
            .get_at_ts(&key[..].into(), ts)
        {
            Ok(value) => {
                // Resolve the value reference to get the actual value.
                value.resolve(&self.core).map(Some)
            }
            Err(Error::KeyNotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Returns all the versioned values and timestamps associated with the key.
    pub fn get_history(&self, key: &[u8]) -> Result<Vec<(Vec<u8>, u64)>> {
        self.ensure_read_only_transaction()?;

        // If the key is empty, return an error.
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }

        let mut results = Vec::new();

        // Attempt to get the value for the key from the snapshot.
        match self
            .snapshot
            .as_ref()
            .unwrap()
            .get_version_history(&key[..].into())
        {
            Ok(values) => {
                // Resolve the value reference to get the actual value.
                for (value, ts) in values {
                    let resolved_value = value.resolve(&self.core)?;
                    results.push((resolved_value, ts));
                }
            }
            Err(e) => return Err(e),
        }

        Ok(results)
    }

    /// Returns key-value pairs within the specified range, at the given timestamp.
    pub fn scan_at_ts<'b, R>(
        &'b self,
        range: R,
        ts: u64,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
    where
        R: RangeBounds<&'b [u8]>,
    {
        // Convert the range to a tuple of bounds of variable keys.
        let range = convert_range_bounds(range);
        let items = self.snapshot.as_ref().unwrap().scan_at_ts(range, ts);

        let mut results = Vec::new();
        'outer: for (key, value) in items {
            // If a limit is set and we've already got enough results, break the loop.
            if let Some(limit) = limit {
                if results.len() >= limit {
                    break;
                }
            }

            // Apply all filters. If any filter fails, skip this key and continue with the next one.
            for filter in &FILTERS {
                if filter.apply(&value).is_err() {
                    continue 'outer;
                }
            }

            // Resolve the value reference to get the actual value.
            let v = value.resolve(&self.core)?;

            results.push((key, v));
        }

        Ok(results)
    }

    /// Returns keys within the specified range, at the given timestamp.
    pub fn keys_at_ts<'b, R>(&'b self, range: R, ts: u64) -> Result<Vec<Vec<u8>>>
    where
        R: RangeBounds<&'b [u8]>,
    {
        self.ensure_read_only_transaction()?;

        // Convert the range to a tuple of bounds of variable keys.
        let range = convert_range_bounds(range);
        let keys = self.snapshot.as_ref().unwrap().keys_at_ts(range, ts);

        Ok(keys)
    }

    /// Returns the value associated with the key at the given timestamp.
    /// The query type specifies the type of query to perform.
    /// The query type can be `LatestByVersion`, `LatestByTs`, `LastLessThanTs`,
    /// `LastLessOrEqualTs`, `FirstGreaterOrEqualTs` or `FirstGreaterThanTs`.
    pub fn get_value_by_query(
        &self,
        key: &VariableSizeKey,
        query_type: QueryType,
    ) -> Result<Option<(Vec<u8>, u64, u64)>> {
        self.ensure_read_only_transaction()?;

        match self
            .snapshot
            .as_ref()
            .unwrap()
            .get_value_by_query(key, query_type)
        {
            Ok((idx_val, version, ts)) => {
                // Resolve the value reference to get the actual value.
                let value = idx_val.resolve(&self.core)?;
                Ok(Some((value, version, ts)))
            }
            Err(Error::KeyNotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Scans a range of keys and returns a vector of tuples containing the key, value, timestamp, and deletion status for each key.
    pub fn scan_all_versions<'b, R>(
        &self,
        range: R,
        limit: Option<usize>,
    ) -> Result<Vec<ScanVersionResult>>
    where
        R: RangeBounds<&'b [u8]>,
    {
        // Convert the range to a tuple of bounds of variable keys.
        let range = convert_range_bounds(range);

        // Initialize an empty vector to store the results.
        let mut results = Vec::new();

        // Initialize a HashSet to keep track of unique keys.
        let mut unique_keys = HashSet::new();

        // Get a range iterator for the specified range.
        let snap = self.snapshot.as_ref().unwrap();
        let ranger = snap.range_with_versions(range);

        // Initialize a variable to keep track of the current key being processed.
        let mut current_key: Option<Bytes> = None;

        // Initialize a vector to store versions for the current key.
        let mut current_key_versions = Vec::new();

        // Iterate over the keys in the range.
        for (key, value, _, ts) in ranger {
            // If the key changes, process the previous key's versions.
            if current_key.as_ref().map_or(false, |k| k != &key) {
                // Add the previous key's versions to the results.
                results.append(&mut current_key_versions);

                // Add the previous key to the set of unique keys.
                unique_keys.insert(current_key.take().unwrap());

                // If a limit is set and we've already got enough unique keys, break the loop.
                if let Some(limit) = limit {
                    if unique_keys.len() >= limit {
                        break;
                    }
                }
            }

            // Determine if the record is soft deleted based on the metadata.
            let is_deleted = value.metadata().is_some_and(|md| md.is_tombstone());

            // Resolve the value reference to get the actual value.
            let v = value.resolve(&self.core)?;

            // Add the key, value, version, and deletion status to the current key's versions.
            current_key_versions.push((key.clone(), v, *ts, is_deleted));

            // Update the current key being processed.
            current_key = Some(key.into());
        }

        // Process the last key's versions.
        if let Some(key) = current_key {
            results.append(&mut current_key_versions);
            unique_keys.insert(key);
        }

        // Return the results.
        Ok(results)
    }

    #[allow(unused)]
    pub(crate) fn get_versionstamp(&self) -> Option<(u64, u64)> {
        self.versionstamp
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
    use tokio::task;

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

    #[tokio::test]
    async fn basic_transaction() {
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
            txn1.commit().await.unwrap();
        }

        {
            // Start a read-only transaction (txn3)
            let mut txn3 = store.begin().unwrap();
            let val = txn3.get(&key1).unwrap().unwrap();
            assert_eq!(val, value1.as_ref());
        }

        {
            // Start another read-write transaction (txn2)
            let mut txn2 = store.begin().unwrap();
            txn2.set(&key1, &value2).unwrap();
            txn2.set(&key2, &value2).unwrap();
            txn2.commit().await.unwrap();
        }

        // Drop the store to simulate closing it
        store.close().await.unwrap();

        // Create a new Core instance with VariableSizeKey after dropping the previous one
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        let store = Store::new(opts).expect("should create store");

        // Start a read-only transaction (txn4)
        let mut txn4 = store.begin().unwrap();
        let val = txn4.get(&key1).unwrap().unwrap();

        // Assert that the value retrieved in txn4 matches value2
        assert_eq!(val, value2.as_ref());
    }

    #[tokio::test]
    async fn transaction_delete_scan() {
        let (store, _) = create_store(false);

        // Define key-value pairs for the test
        let key1 = Bytes::from("k1");
        let value1 = Bytes::from("baz");

        {
            // Start a new read-write transaction (txn1)
            let mut txn1 = store.begin().unwrap();
            txn1.set(&key1, &value1).unwrap();
            txn1.set(&key1, &value1).unwrap();
            txn1.commit().await.unwrap();
        }

        {
            // Start a read-only transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.delete(&key1).unwrap();
            txn.commit().await.unwrap();
        }

        {
            // Start another read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            assert!(txn.get(&key1).unwrap().is_none());
        }

        {
            let range = "k1".as_bytes()..="k3".as_bytes();
            let mut txn = store.begin().unwrap();
            let results = txn.scan(range, None).unwrap();
            assert_eq!(results.len(), 0);
        }
    }

    #[tokio::test]
    async fn keys_with_tombstones() {
        for is_ssi in [false, true] {
            let (store, _) = create_store(is_ssi);

            let key = Bytes::from("k");
            let value = Bytes::from("v");

            // First, insert the key.
            let mut txn1 = store.begin().unwrap();
            txn1.set(&key, &value).unwrap();
            txn1.commit().await.unwrap();

            // Then, soft-delete it.
            let mut txn2 = store.begin().unwrap();
            txn2.soft_delete(&key).unwrap();
            txn2.commit().await.unwrap();

            // keys_with_tombstones() should still return `k`
            // despite it being soft-deleted.
            let range = "k".as_bytes()..="k".as_bytes();
            let txn3 = store.begin().unwrap();
            let results = txn3.keys_with_tombstones(range).unwrap();
            assert_eq!(results, vec![b"k"]);
        }
    }

    async fn mvcc_tests(is_ssi: bool) {
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
            txn1.commit().await.unwrap();

            assert!(txn2.get(&key2).unwrap().is_none());
            txn2.set(&key2, &value2).unwrap();
            txn2.commit().await.unwrap();
        }

        // read conflict when the read key was updated by another transaction
        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            txn1.set(&key1, &value1).unwrap();
            txn1.commit().await.unwrap();

            assert!(txn2.get(&key1).is_ok());
            txn2.set(&key1, &value2).unwrap();
            assert!(match txn2.commit().await {
                Err(err) => {
                    matches!(err, Error::TransactionReadConflict)
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

            txn1.commit().await.unwrap();
            txn2.commit().await.unwrap();

            let mut txn3 = store.begin().unwrap();
            let val = txn3.get(&key1).unwrap().unwrap();
            assert_eq!(val, value2.as_ref());
        }

        // read conflict when the read key was updated by another transaction
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
                    matches!(err, Error::TransactionReadConflict)
                }
                _ => false,
            });
        }

        // read conflict when the read key was deleted by another transaction
        {
            let key = Bytes::from("key4");

            let mut txn1 = store.begin().unwrap();
            txn1.set(&key, &value1).unwrap();
            txn1.commit().await.unwrap();

            let mut txn2 = store.begin().unwrap();
            let mut txn3 = store.begin().unwrap();

            txn2.delete(&key).unwrap();
            assert!(txn2.commit().await.is_ok());

            assert!(txn3.get(&key).is_ok());
            txn3.set(&key, &value2).unwrap();
            assert!(match txn3.commit().await {
                Err(err) => {
                    matches!(err, Error::TransactionReadConflict)
                }
                _ => false,
            });
        }
    }

    #[tokio::test]
    async fn mvcc_serialized_snapshot_isolation() {
        mvcc_tests(true).await;
    }

    #[tokio::test]
    async fn mvcc_snapshot_isolation() {
        mvcc_tests(false).await;
    }

    #[tokio::test]
    async fn basic_scan_single_key() {
        let (store, _) = create_store(false);
        // Define key-value pairs for the test
        let keys_to_insert = vec![Bytes::from("key1")];

        for key in &keys_to_insert {
            let mut txn = store.begin().unwrap();
            txn.set(key, key).unwrap();
            txn.commit().await.unwrap();
        }

        let range = "key1".as_bytes()..="key3".as_bytes();

        let mut txn = store.begin().unwrap();
        let results = txn.scan(range, None).unwrap();
        assert_eq!(results.len(), keys_to_insert.len());
    }

    #[tokio::test]
    async fn basic_scan_multiple_keys() {
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
            txn.commit().await.unwrap();
        }

        let range = "key1".as_bytes()..="key3".as_bytes();

        let mut txn = store.begin().unwrap();
        let results = txn.scan(range, None).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].1, keys_to_insert[0]);
        assert_eq!(results[1].1, keys_to_insert[1]);
        assert_eq!(results[2].1, keys_to_insert[2]);
    }

    #[tokio::test]
    async fn scan_multiple_keys_within_single_transaction() {
        let (store, _) = create_store(false);
        // Define key-value pairs for the test
        let keys_to_insert = vec![
            Bytes::from("test1"),
            Bytes::from("test2"),
            Bytes::from("test3"),
        ];

        let mut txn = store.begin().unwrap();
        for key in &keys_to_insert {
            txn.set(key, key).unwrap();
        }
        txn.commit().await.unwrap();

        let range = "test1".as_bytes()..="test7".as_bytes();

        let mut txn = store.begin().unwrap();
        let results = txn.scan(range, None).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, keys_to_insert[0]);
        assert_eq!(results[1].0, keys_to_insert[1]);
        assert_eq!(results[2].0, keys_to_insert[2]);
        assert_eq!(results[0].1, keys_to_insert[0]);
        assert_eq!(results[1].1, keys_to_insert[1]);
        assert_eq!(results[2].1, keys_to_insert[2]);
    }

    async fn mvcc_with_scan_tests(is_ssi: bool) {
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
            txn1.commit().await.unwrap();

            let mut txn2 = store.begin().unwrap();
            let mut txn3 = store.begin().unwrap();

            txn2.set(&key1, &value4).unwrap();
            txn2.set(&key2, &value2).unwrap();
            txn2.set(&key3, &value3).unwrap();
            txn2.commit().await.unwrap();

            let range = "key1".as_bytes()..="key4".as_bytes();
            let results = txn3.scan(range, None).unwrap();
            assert_eq!(results.len(), 1);
            txn3.set(&key2, &value5).unwrap();
            txn3.set(&key3, &value6).unwrap();

            assert!(match txn3.commit().await {
                Err(err) => {
                    matches!(err, Error::TransactionReadConflict)
                }
                _ => false,
            });
        }

        // read conflict when read keys are deleted by other transaction
        {
            let mut txn1 = store.begin().unwrap();

            txn1.set(&key4, &value1).unwrap();
            txn1.commit().await.unwrap();

            let mut txn2 = store.begin().unwrap();
            let mut txn3 = store.begin().unwrap();

            txn2.delete(&key4).unwrap();
            txn2.commit().await.unwrap();

            let range = "key1".as_bytes()..="key5".as_bytes();
            txn3.scan(range, None).unwrap();
            txn3.set(&key4, &value2).unwrap();

            assert!(match txn3.commit().await {
                Err(err) => {
                    matches!(err, Error::TransactionReadConflict)
                }
                _ => false,
            });
        }
    }

    #[tokio::test]
    async fn mvcc_serialized_snapshot_isolation_scan() {
        mvcc_with_scan_tests(true).await;
    }

    #[tokio::test]
    async fn mvcc_snapshot_isolation_scan() {
        mvcc_with_scan_tests(false).await;
    }

    #[tokio::test]
    async fn ryow() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        let store = Store::new(opts.clone()).expect("should create store");
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
            assert_eq!(txn.get(&key1).unwrap().unwrap(), value2.as_ref());
            assert!(txn.get(&key3).unwrap().is_none());
            txn.set(&key2, &value1).unwrap();
            assert_eq!(txn.get(&key2).unwrap().unwrap(), value1.as_ref());
            txn.commit().await.unwrap();
        }
    }

    // Common setup logic for creating a store
    async fn create_hermitage_store(is_ssi: bool) -> Store {
        let (store, _) = create_store(is_ssi);

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
    async fn g0_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi).await;
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
                    matches!(err, Error::TransactionReadConflict)
                }
                _ => false,
            });
        }

        {
            let mut txn3 = store.begin().unwrap();
            let val1 = txn3.get(&key1).unwrap().unwrap();
            assert_eq!(val1, value3.as_ref());
            let val2 = txn3.get(&key2).unwrap().unwrap();
            assert_eq!(val2, value5.as_ref());
        }
    }

    #[tokio::test]
    async fn g0() {
        g0_tests(false).await; // snapshot isolation
        g0_tests(true).await; // serializable snapshot isolation
    }

    // G1a: Aborted Reads (dirty reads, cascaded aborts)
    async fn g1a_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi).await;
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
            let res = txn2.scan(range.clone(), None).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].1, value1);

            drop(txn1);

            let res = txn2.scan(range, None).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].1, value1);

            txn2.commit().await.unwrap();
        }

        {
            let mut txn3 = store.begin().unwrap();
            let val1 = txn3.get(&key1).unwrap().unwrap();
            assert_eq!(val1, value1.as_ref());
            let val2 = txn3.get(&key2).unwrap().unwrap();
            assert_eq!(val2, value2.as_ref());
        }
    }

    #[tokio::test]
    async fn g1a() {
        g1a_tests(false).await; // snapshot isolation
        g1a_tests(true).await; // serializable snapshot isolation
    }

    // G1b: Intermediate Reads (dirty reads)
    async fn g1b_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi).await;

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
            let res = txn2.scan(range.clone(), None).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].1, value1);

            txn1.set(&key1, &value4).unwrap();
            txn1.commit().await.unwrap();

            let res = txn2.scan(range, None).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].1, value1);

            txn2.commit().await.unwrap();
        }
    }

    #[tokio::test]
    async fn g1b() {
        g1b_tests(false).await; // snapshot isolation
        g1b_tests(true).await; // serializable snapshot isolation
    }

    // G1c: Circular Information Flow (dirty reads)
    async fn g1c_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi).await;

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

            assert_eq!(txn1.get(&key2).unwrap().unwrap(), value2.as_ref());
            assert_eq!(txn2.get(&key1).unwrap().unwrap(), value1.as_ref());

            txn1.commit().await.unwrap();
            assert!(match txn2.commit().await {
                Err(err) => {
                    matches!(err, Error::TransactionReadConflict)
                }
                _ => false,
            });
        }
    }

    #[tokio::test]
    async fn g1c() {
        g1c_tests(false).await; // snapshot isolation
        g1c_tests(true).await;
    }

    // PMP: Predicate-Many-Preceders
    async fn pmp_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi).await;

        let key3 = Bytes::from("k3");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");
        let value3 = Bytes::from("v3");

        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            // k3 should not be visible to txn1
            let range = "k1".as_bytes()..="k3".as_bytes();
            let res = txn1.scan(range.clone(), None).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].1, value1);
            assert_eq!(res[1].1, value2);

            // k3 is committed by txn2
            txn2.set(&key3, &value3).unwrap();
            txn2.commit().await.unwrap();

            // k3 should still not be visible to txn1
            let range = "k1".as_bytes()..="k3".as_bytes();
            let res = txn1.scan(range.clone(), None).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].1, value1);
            assert_eq!(res[1].1, value2);
        }
    }

    #[tokio::test]
    async fn pmp() {
        pmp_tests(false).await;
        pmp_tests(true).await;
    }

    // PMP-Write: Circular Information Flow (dirty reads)
    async fn pmp_write_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi).await;

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
            let res = txn2.scan(range.clone(), None).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].1, value1);
            assert_eq!(res[1].1, value2);

            txn2.delete(&key2).unwrap();
            txn1.commit().await.unwrap();

            let range = "k1".as_bytes()..="k3".as_bytes();
            let res = txn2.scan(range.clone(), None).unwrap();
            assert_eq!(res.len(), 1);
            assert_eq!(res[0].1, value1);

            assert!(match txn2.commit().await {
                Err(err) => {
                    matches!(err, Error::TransactionReadConflict)
                }
                _ => false,
            });
        }
    }

    #[tokio::test]
    async fn pmp_write() {
        pmp_write_tests(false).await;
        pmp_write_tests(true).await;
    }

    // P4: Lost Update
    async fn p4_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi).await;

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
                    matches!(err, Error::TransactionReadConflict)
                }
                _ => false,
            });
        }
    }

    #[tokio::test]
    async fn p4() {
        p4_tests(false).await;
        p4_tests(true).await;
    }

    // G-single: Single Anti-dependency Cycles (read skew)
    async fn g_single_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi).await;

        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");
        let value3 = Bytes::from("v3");
        let value4 = Bytes::from("v4");

        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            assert_eq!(txn1.get(&key1).unwrap().unwrap(), value1.as_ref());
            assert_eq!(txn2.get(&key1).unwrap().unwrap(), value1.as_ref());
            assert_eq!(txn2.get(&key2).unwrap().unwrap(), value2.as_ref());
            txn2.set(&key1, &value3).unwrap();
            txn2.set(&key2, &value4).unwrap();

            txn2.commit().await.unwrap();

            assert_eq!(txn1.get(&key2).unwrap().unwrap(), value2.as_ref());
            txn1.commit().await.unwrap();
        }
    }

    #[tokio::test]
    async fn g_single() {
        g_single_tests(false).await;
        g_single_tests(true).await;
    }

    // G-single-write-1: Single Anti-dependency Cycles (read skew)
    async fn g_single_write_1_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi).await;

        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");
        let value3 = Bytes::from("v3");
        let value4 = Bytes::from("v4");

        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            assert_eq!(txn1.get(&key1).unwrap().unwrap(), value1.as_ref());

            let range = "k1".as_bytes()..="k2".as_bytes();
            let res = txn2.scan(range.clone(), None).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].1, value1);
            assert_eq!(res[1].1, value2);

            txn2.set(&key1, &value3).unwrap();
            txn2.set(&key2, &value4).unwrap();

            txn2.commit().await.unwrap();

            txn1.delete(&key2).unwrap();
            assert!(txn1.get(&key2).unwrap().is_none());
            assert!(match txn1.commit().await {
                Err(err) => {
                    matches!(err, Error::TransactionReadConflict)
                }
                _ => false,
            });
        }
    }

    #[tokio::test]
    async fn g_single_write_1() {
        g_single_write_1_tests(false).await;
        g_single_write_1_tests(true).await;
    }

    // G-single-write-2: Single Anti-dependency Cycles (read skew)
    async fn g_single_write_2_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi).await;

        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let value1 = Bytes::from("v1");
        let value2 = Bytes::from("v2");
        let value3 = Bytes::from("v3");
        let value4 = Bytes::from("v4");

        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            assert_eq!(txn1.get(&key1).unwrap().unwrap(), value1.as_ref());
            let range = "k1".as_bytes()..="k2".as_bytes();
            let res = txn2.scan(range.clone(), None).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].1, value1);
            assert_eq!(res[1].1, value2);

            txn2.set(&key1, &value3).unwrap();

            txn1.delete(&key2).unwrap();

            txn2.set(&key2, &value4).unwrap();

            drop(txn1);

            txn2.commit().await.unwrap();
        }
    }

    #[tokio::test]
    async fn g_single_write_2() {
        g_single_write_2_tests(false).await;
        g_single_write_2_tests(true).await;
    }

    async fn g2_item_tests(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi).await;

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
            let res = txn1.scan(range.clone(), None).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].1, value1);
            assert_eq!(res[1].1, value2);

            let res = txn2.scan(range.clone(), None).unwrap();
            assert_eq!(res.len(), 2);
            assert_eq!(res[0].1, value1);
            assert_eq!(res[1].1, value2);

            txn1.set(&key1, &value3).unwrap();
            txn2.set(&key2, &value4).unwrap();

            txn1.commit().await.unwrap();

            assert!(match txn2.commit().await {
                Err(err) => {
                    matches!(err, Error::TransactionReadConflict)
                }
                _ => false,
            });
        }
    }

    #[tokio::test]
    async fn g2_item() {
        g2_item_tests(false).await;
        g2_item_tests(true).await;
    }

    fn require_send<T: Send>(_: T) {}
    fn require_sync<T: Sync + Send>(_: T) {}

    #[tokio::test]
    async fn is_send_sync() {
        let (db, _) = create_store(false);

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
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        let store = Store::new(opts.clone()).expect("should create store");
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
        let mut txn = store.begin_with_mode(Mode::ReadOnly).unwrap();
        for _i in 0..ENTRIES {
            let (key, _) = gen_pair(&mut rng);
            txn.get(&key).unwrap();
        }
    }

    #[tokio::test]
    async fn empty_scan_should_not_return_an_error() {
        let (store, _) = create_store(false);

        let range = "key1".as_bytes()..="key3".as_bytes();

        let mut txn = store.begin().unwrap();
        let results = txn.scan(range, None).unwrap();
        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn transaction_delete_and_scan_test() {
        let (store, _) = create_store(false);

        // Define key-value pairs for the test
        let key1 = Bytes::from("k1");
        let value1 = Bytes::from("baz");

        {
            // Start a new read-write transaction (txn1)
            let mut txn1 = store.begin().unwrap();
            txn1.set(&key1, &value1).unwrap();
            txn1.commit().await.unwrap();
        }

        {
            // Start a read-only transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.get(&key1).unwrap();
            txn.delete(&key1).unwrap();
            let range = "k1".as_bytes()..="k3".as_bytes();
            let results = txn.scan(range, None).unwrap();
            assert_eq!(results.len(), 0);
            txn.commit().await.unwrap();
        }
        {
            let range = "k1".as_bytes()..="k3".as_bytes();
            let mut txn = store.begin().unwrap();
            let results = txn.scan(range, None).unwrap();
            assert_eq!(results.len(), 0);
        }
    }

    #[tokio::test]
    async fn sdb_delete_record_id_bug() {
        let (store, _) = create_store(false);

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
        let mut txn1 = store.begin().unwrap();
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
                50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 33, 116,
                98, 117, 115, 101, 114, 0,
            ]))
            .unwrap();
            txn2.set(
                &Bytes::copy_from_slice(&[
                    47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71,
                    72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
                    33, 116, 98, 117, 115, 101, 114, 0,
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

    async fn g2_item_predicate(is_ssi: bool) {
        let store = create_hermitage_store(is_ssi).await;

        let key3 = Bytes::from("k3");
        let key4 = Bytes::from("k4");
        let key5 = Bytes::from("k5");
        let key6 = Bytes::from("k6");
        let key7 = Bytes::from("k7");
        let value3 = Bytes::from("v3");
        let value4 = Bytes::from("v4");

        // inserts into read ranges of already-committed transaction(s) should fail
        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            let range = "k1".as_bytes()..="k4".as_bytes();
            txn1.scan(range.clone(), None).unwrap();
            txn2.scan(range.clone(), None).unwrap();

            txn1.set(&key3, &value3).unwrap();
            txn2.set(&key4, &value4).unwrap();

            txn1.commit().await.unwrap();

            assert!(match txn2.commit().await {
                Err(err) => {
                    matches!(err, Error::TransactionReadConflict)
                }
                _ => false,
            });
        }

        // k1, k2, k3 already committed
        // inserts beyond scan range should pass
        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            let range = "k1".as_bytes()..="k3".as_bytes();
            txn1.scan(range.clone(), None).unwrap();
            txn2.scan(range.clone(), None).unwrap();

            txn1.set(&key4, &value3).unwrap();
            txn2.set(&key5, &value4).unwrap();

            txn1.commit().await.unwrap();
            txn2.commit().await.unwrap();
        }

        // k1, k2, k3, k4, k5 already committed
        // inserts in subset scan ranges should fail
        {
            let mut txn1 = store.begin().unwrap();
            let mut txn2 = store.begin().unwrap();

            let range = "k1".as_bytes()..="k7".as_bytes();
            txn1.scan(range.clone(), None).unwrap();
            let range = "k3".as_bytes()..="k7".as_bytes();
            txn2.scan(range.clone(), None).unwrap();

            txn1.set(&key6, &value3).unwrap();
            txn2.set(&key7, &value4).unwrap();

            txn1.commit().await.unwrap();
            assert!(match txn2.commit().await {
                Err(err) => {
                    matches!(err, Error::TransactionReadConflict)
                }
                _ => false,
            });
        }
    }

    #[tokio::test]
    async fn g2_predicate() {
        g2_item_predicate(true).await;
    }

    #[tokio::test]
    async fn transaction_delete_from_index() {
        let (store, temp_dir) = create_store(false);

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
            let mut txn3 = store.begin().unwrap();
            let val = txn3.get(&key1).unwrap();
            assert!(val.is_none());
            let val = txn3.get(&key2).unwrap().unwrap();
            assert_eq!(val, value.as_ref());
        }

        // Drop the store to simulate closing it
        store.close().await.unwrap();

        // sleep for a while to ensure the store is closed
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Create a new Core instance with VariableSizeKey after dropping the previous one
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        let store = Store::new(opts).expect("should create store");

        // Start a read-only transaction (txn4)
        let mut txn4 = store.begin().unwrap();
        let val = txn4.get(&key1).unwrap();
        assert!(val.is_none());
        let val = txn4.get(&key2).unwrap().unwrap();
        assert_eq!(val, value.as_ref());
    }

    #[tokio::test]
    async fn test_insert_clear_read_key() {
        let (store, _) = create_store(false);

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
            txn.soft_delete(&key).unwrap();
            txn.commit().await.unwrap();
        }

        // Read the key in a new transaction to verify it does not exist
        {
            let mut txn = store.begin().unwrap();
            assert!(txn.get(&key).unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn multiple_savepoints_without_ssi() {
        let (store, _) = create_store(false);

        // Key-value pair for the test
        let key1 = Bytes::from("test_key1");
        let value1 = Bytes::from("test_value1");
        let key2 = Bytes::from("test_key2");
        let value2 = Bytes::from("test_value2");
        let key3 = Bytes::from("test_key3");
        let value3 = Bytes::from("test_value3");

        // Start the transaction and write key1.
        let mut txn1 = store.begin().unwrap();
        txn1.set(&key1, &value1).unwrap();

        // Set the first savepoint.
        txn1.set_savepoint().unwrap();

        // Write key2 after the savepoint.
        txn1.set(&key2, &value2).unwrap();

        // Set another savepoint, stacking it onto the first one.
        txn1.set_savepoint().unwrap();

        txn1.set(&key3, &value3).unwrap();

        // Just a sanity check that all three keys are present.
        assert_eq!(txn1.get(&key1).unwrap().unwrap(), value1);
        assert_eq!(txn1.get(&key2).unwrap().unwrap(), value2);
        assert_eq!(txn1.get(&key3).unwrap().unwrap(), value3);

        // Rollback to the latest (second) savepoint. This should make key3
        // go away while keeping key1 and key2.
        txn1.rollback_to_savepoint().unwrap();
        assert_eq!(txn1.get(&key1).unwrap().unwrap(), value1);
        assert_eq!(txn1.get(&key2).unwrap().unwrap(), value2);
        assert!(txn1.get(&key3).unwrap().is_none());

        // Now roll back to the first savepoint. This should only
        // keep key1 around.
        txn1.rollback_to_savepoint().unwrap();
        assert_eq!(txn1.get(&key1).unwrap().unwrap(), value1);
        assert!(txn1.get(&key2).unwrap().is_none());
        assert!(txn1.get(&key3).unwrap().is_none());

        // Check that without any savepoints set the error is returned.
        assert!(matches!(
            txn1.rollback_to_savepoint(),
            Err(Error::TransactionWithoutSavepoint)
        ),);

        // Commit the transaction.
        txn1.commit().await.unwrap();
        drop(txn1);

        // Start another transaction and check again for the keys.
        let mut txn2 = store.begin().unwrap();
        assert_eq!(txn2.get(&key1).unwrap().unwrap(), value1);
        assert!(txn2.get(&key2).unwrap().is_none());
        assert!(txn2.get(&key3).unwrap().is_none());
        txn2.commit().await.unwrap();
    }

    #[tokio::test]
    async fn multiple_savepoints_with_ssi() {
        let (store, _) = create_store(true);

        // Key-value pair for the test
        let key1 = Bytes::from("test_key1");
        let value1 = Bytes::from("test_value1");
        let key2 = Bytes::from("test_key2");
        let value2 = Bytes::from("test_value2");
        let key3 = Bytes::from("test_key3");
        let value3 = Bytes::from("test_value3");

        // Start the transaction and write key1.
        let mut txn1 = store.begin().unwrap();
        txn1.set(&key1, &value1).unwrap();

        // Set the first savepoint.
        txn1.set_savepoint().unwrap();

        // Write key2 after the savepoint.
        txn1.set(&key2, &value2).unwrap();

        // Set another savepoint, stacking it onto the first one.
        txn1.set_savepoint().unwrap();

        txn1.set(&key3, &value3).unwrap();

        // Just a sanity check that all three keys are present.
        assert_eq!(txn1.get(&key1).unwrap().unwrap(), value1);
        assert_eq!(txn1.get(&key2).unwrap().unwrap(), value2);
        assert_eq!(txn1.get(&key3).unwrap().unwrap(), value3);

        // Rollback to the latest (second) savepoint. This should make key3
        // go away while keeping key1 and key2.
        txn1.rollback_to_savepoint().unwrap();
        assert_eq!(txn1.get(&key1).unwrap().unwrap(), value1);
        assert_eq!(txn1.get(&key2).unwrap().unwrap(), value2);
        assert!(txn1.get(&key3).unwrap().is_none());

        // Now roll back to the first savepoint. This should only
        // keep key1 around.
        txn1.rollback_to_savepoint().unwrap();
        assert_eq!(txn1.get(&key1).unwrap().unwrap(), value1);
        assert!(txn1.get(&key2).unwrap().is_none());
        assert!(txn1.get(&key3).unwrap().is_none());

        // Check that without any savepoints set the error is returned.
        assert!(matches!(
            txn1.rollback_to_savepoint(),
            Err(Error::TransactionWithoutSavepoint)
        ),);

        // Commit the transaction.
        txn1.commit().await.unwrap();
        drop(txn1);

        // Start another transaction and check again for the keys.
        let mut txn2 = store.begin().unwrap();
        assert_eq!(txn2.get(&key1).unwrap().unwrap(), value1);
        assert!(txn2.get(&key2).unwrap().is_none());
        assert!(txn2.get(&key3).unwrap().is_none());
        txn2.commit().await.unwrap();
    }

    #[tokio::test]
    async fn savepont_with_concurrent_read_txn_without_ssi() {
        let (store, _) = create_store(false);

        // Key-value pair for the test
        let key = Bytes::from("test_key1");
        let value = Bytes::from("test_value");
        let updated_value = Bytes::from("updated_test_value");
        let key2 = Bytes::from("test_key2");
        let value2 = Bytes::from("test_value2");

        // Store the entry.
        let mut txn1 = store.begin().unwrap();
        txn1.set(&key, &value).unwrap();
        txn1.commit().await.unwrap();
        drop(txn1);

        // Now open two concurrent transaction, where one
        // is reading the value and the other one is
        // updating it. Normally this would lead to a read-write
        // conflict aborting the reading transaction.
        // However, if the reading transaction rolls back
        // to a savepoint before reading the vealu, as
        // if it never happened, there should be no conflict.
        let mut read_txn = store.begin().unwrap();
        let mut update_txn = store.begin().unwrap();

        // This write is needed to force oracle.new_commit_ts().
        read_txn.set(&key2, &value2).unwrap();

        read_txn.set_savepoint().unwrap();

        read_txn.get(&key).unwrap().unwrap();
        update_txn.set(&key, &updated_value).unwrap();

        // Commit the transaction.
        update_txn.commit().await.unwrap();
        // Read transaction should commit without conflict after
        // rolling back to the savepoint before reading `key`.
        read_txn.rollback_to_savepoint().unwrap();
        read_txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn savepont_with_concurrent_read_txn_with_ssi() {
        let (store, _) = create_store(true);

        let key = Bytes::from("test_key1");
        let value = Bytes::from("test_value");
        let updated_value = Bytes::from("updated_test_value");
        let key2 = Bytes::from("test_key2");
        let value2 = Bytes::from("test_value2");

        let mut txn1 = store.begin().unwrap();
        txn1.set(&key, &value).unwrap();
        txn1.commit().await.unwrap();
        drop(txn1);

        let mut read_txn = store.begin().unwrap();
        let mut update_txn = store.begin().unwrap();

        read_txn.set(&key2, &value2).unwrap();

        read_txn.set_savepoint().unwrap();

        read_txn.get(&key).unwrap().unwrap();
        update_txn.set(&key, &updated_value).unwrap();

        update_txn.commit().await.unwrap();
        read_txn.rollback_to_savepoint().unwrap();
        read_txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn savepont_with_concurrent_scan_txn() {
        // Scan set is only considered for SSI.
        let (store, _) = create_store(true);

        let k1 = Bytes::from("k1");
        let value = Bytes::from("test_value");
        let updated_value = Bytes::from("updated_test_value");
        let k2 = Bytes::from("k2");
        let value2 = Bytes::from("test_value2");

        let mut txn1 = store.begin().unwrap();
        txn1.set(&k1, &value).unwrap();
        txn1.commit().await.unwrap();
        drop(txn1);

        let mut read_txn = store.begin().unwrap();
        let mut update_txn = store.begin().unwrap();

        read_txn.set(&k2, &value2).unwrap();

        read_txn.set_savepoint().unwrap();

        let range = "k0".as_bytes()..="k10".as_bytes();
        read_txn.scan(range, None).unwrap();
        update_txn.set(&k1, &updated_value).unwrap();

        update_txn.commit().await.unwrap();
        read_txn.rollback_to_savepoint().unwrap();
        read_txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn savepont_with_concurrent_scan_txn_with_new_write() {
        // Scan set is only considered for SSI.
        let (store, _) = create_store(true);

        let k1 = Bytes::from("k1");
        let value = Bytes::from("test_value");
        let k2 = Bytes::from("k2");
        let value2 = Bytes::from("test_value2");
        let k3 = Bytes::from("k3");
        let value3 = Bytes::from("test_value3");

        let mut txn1 = store.begin().unwrap();
        txn1.set(&k1, &value).unwrap();
        txn1.commit().await.unwrap();
        drop(txn1);

        let mut read_txn = store.begin().unwrap();
        let mut update_txn = store.begin().unwrap();

        read_txn.set(&k2, &value2).unwrap();
        // Put k1 into the read_txn's read set in order
        // to force the conflict resolution check.
        read_txn.get(&k1).unwrap();

        read_txn.set_savepoint().unwrap();

        let range = "k1".as_bytes()..="k3".as_bytes();
        read_txn.scan(range, None).unwrap();
        update_txn.set(&k3, &value3).unwrap();

        update_txn.commit().await.unwrap();
        read_txn.rollback_to_savepoint().unwrap();
        read_txn.commit().await.unwrap();
    }

    #[tokio::test]
    async fn savepoint_rollback_on_updated_key() {
        let (store, _) = create_store(false);

        let k1 = Bytes::from("k1");
        let value1 = Bytes::from("value1");
        let value2 = Bytes::from("value2");
        let value3 = Bytes::from("value3");

        let mut txn1 = store.begin().unwrap();
        txn1.set(&k1, &value1).unwrap();
        txn1.set(&k1, &value2).unwrap();
        txn1.set_savepoint().unwrap();
        txn1.set(&k1, &value3).unwrap();
        txn1.rollback_to_savepoint().unwrap();

        // The read value should be the one before the savepoint.
        assert_eq!(txn1.get(&k1).unwrap().unwrap(), value2);
    }

    #[tokio::test]
    async fn savepoint_rollback_on_updated_key_with_scan() {
        let (store, _) = create_store(false);

        let k1 = Bytes::from("k1");
        let value = Bytes::from("value1");
        let value2 = Bytes::from("value2");

        let mut txn1 = store.begin().unwrap();
        txn1.set(&k1, &value).unwrap();
        txn1.set_savepoint().unwrap();
        txn1.set(&k1, &value2).unwrap();
        txn1.rollback_to_savepoint().unwrap();

        // The scanned value should be the one before the savepoint.
        let range = "k1".as_bytes()..="k3".as_bytes();
        let sr = txn1.scan(range, None).unwrap();
        assert_eq!(sr[0].0, k1.to_vec());
        assert_eq!(
            sr[0].1,
            value.to_vec(),
            "{}",
            String::from_utf8_lossy(&sr[0].1)
        );
    }

    #[tokio::test]
    async fn ordered_writes() {
        // This test ensures that the real time order of writes
        // is preserved within a transaction.
        use crate::storage::kv::entry::Record;
        use crate::storage::kv::reader::{Reader, RecordReader};
        use crate::storage::log::Error as LogError;
        use crate::storage::log::{MultiSegmentReader, SegmentRef};

        let k1 = Bytes::from("k1");
        let v1 = Bytes::from("v1");
        let k2 = Bytes::from("k2");
        let v2 = Bytes::from("v2");
        let v3 = Bytes::from("v3");

        let (store, tmp_dir) = create_store(false);
        let mut txn = store.begin().unwrap();
        txn.set(&k1, &v1).unwrap();
        txn.set(&k2, &v2).unwrap();
        txn.set(&k1, &v3).unwrap();
        txn.commit().await.unwrap();
        store.close().await.unwrap();

        let clog_subdir = tmp_dir.path().join("clog");
        let sr = SegmentRef::read_segments_from_directory(clog_subdir.as_path()).unwrap();
        let reader = MultiSegmentReader::new(sr).unwrap();
        let reader = Reader::new_from(reader);
        let mut tx_reader = RecordReader::new(reader);

        // Expect ("k2", "v2")
        let mut log_rec = Record::new();
        tx_reader.read_into(&mut log_rec).unwrap();
        assert_eq!(log_rec.key, k2);
        assert_eq!(log_rec.value, v2);

        // Expect ("k1", "v3")
        let mut log_rec = Record::new();
        tx_reader.read_into(&mut log_rec).unwrap();
        assert_eq!(log_rec.key, k1);
        assert_eq!(log_rec.value, v3);

        // Eof
        let mut log_rec = Record::new();
        assert!(matches!(
            tx_reader.read_into(&mut log_rec),
            Err(Error::LogError(LogError::Eof))
        ),);
    }

    #[tokio::test]
    async fn test_scan_all_versions_single_key_single_version() {
        let (store, _) = create_store(false);
        let key = Bytes::from("key1");
        let value = Bytes::from("value1");

        let mut txn = store.begin().unwrap();
        txn.set_at_ts(&key, &value, 1).unwrap();
        txn.commit().await.unwrap();

        let range = key.as_ref()..=key.as_ref();
        let txn = store.begin().unwrap();
        let results = txn.scan_all_versions(range, None).unwrap();

        assert_eq!(results.len(), 1);
        let (k, v, version, is_deleted) = &results[0];
        assert_eq!(k, &key);
        assert_eq!(v, &value);
        assert_eq!(*version, 1);
        assert!(!(*is_deleted));
    }

    #[tokio::test]
    async fn test_scan_all_versions_single_key_multiple_versions() {
        let (store, _) = create_store(false);
        let key = Bytes::from("key1");

        // Insert multiple versions of the same key
        let values = [
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
        ];

        for (i, value) in values.iter().enumerate() {
            let mut txn = store.begin().unwrap();
            let version = (i + 1) as u64; // Incremental version
            txn.set_at_ts(&key, value, version).unwrap();
            txn.commit().await.unwrap();
        }

        let range = key.as_ref()..=key.as_ref();
        let txn = store.begin().unwrap();
        let results = txn.scan_all_versions(range, None).unwrap();

        // Verify that the output contains all the versions of the key
        assert_eq!(results.len(), values.len());
        for (i, (k, v, version, is_deleted)) in results.iter().enumerate() {
            assert_eq!(k, &key);
            assert_eq!(v, &values[i]);
            assert_eq!(*version, (i + 1) as u64);
            assert!(!(*is_deleted));
        }
    }

    #[tokio::test]
    async fn test_scan_all_versions_multiple_keys_single_version_each() {
        let (store, _) = create_store(false);
        let keys = vec![
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
        ];
        let value = Bytes::from("value1");

        for key in &keys {
            let mut txn = store.begin().unwrap();
            txn.set_at_ts(key, &value, 1).unwrap();
            txn.commit().await.unwrap();
        }

        let range = keys.first().unwrap().as_ref()..=keys.last().unwrap().as_ref();
        let txn = store.begin().unwrap();
        let results = txn.scan_all_versions(range, None).unwrap();

        assert_eq!(results.len(), keys.len());
        for (i, (k, v, version, is_deleted)) in results.iter().enumerate() {
            assert_eq!(k, &keys[i]);
            assert_eq!(v, &value);
            assert_eq!(*version, 1);
            assert!(!(*is_deleted));
        }
    }

    #[tokio::test]
    async fn test_scan_all_versions_multiple_keys_multiple_versions_each() {
        let (store, _) = create_store(false);

        // Define a range of keys
        let keys = vec![
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
        ];

        // Insert multiple versions for each key
        let values = [
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
        ];

        for key in &keys {
            for (i, value) in values.iter().enumerate() {
                let mut txn = store.begin().unwrap();
                let version = (i + 1) as u64; // Incremental version
                txn.set_at_ts(key, value, version).unwrap();
                txn.commit().await.unwrap();
            }
        }

        let range = keys.first().unwrap().as_ref()..=keys.last().unwrap().as_ref();
        let txn = store.begin().unwrap();
        let results = txn.scan_all_versions(range, None).unwrap();

        let mut expected_results = Vec::new();
        for key in &keys {
            for (i, value) in values.iter().enumerate() {
                expected_results.push((key.clone(), value.clone(), (i + 1) as u64, false));
            }
        }

        assert_eq!(results.len(), expected_results.len());
        for (result, expected) in results.iter().zip(expected_results.iter()) {
            let (k, v, version, is_deleted) = result;
            let (expected_key, expected_value, expected_version, expected_is_deleted) = expected;
            assert_eq!(k, expected_key);
            assert_eq!(v, expected_value);
            assert_eq!(*version, *expected_version);
            assert_eq!(*is_deleted, *expected_is_deleted);
        }
    }

    #[tokio::test]
    async fn test_scan_all_versions_deleted_records() {
        let (store, _) = create_store(false);
        let key = Bytes::from("key1");
        let value = Bytes::from("value1");

        let mut txn = store.begin().unwrap();
        txn.set_at_ts(&key, &value, 1).unwrap();
        txn.commit().await.unwrap();

        let mut txn = store.begin().unwrap();
        txn.soft_delete(&key).unwrap();
        txn.commit().await.unwrap();

        let range = key.as_ref()..=key.as_ref();
        let txn = store.begin().unwrap();
        let results = txn.scan_all_versions(range, None).unwrap();

        assert_eq!(results.len(), 2);
        let (k, v, version, is_deleted) = &results[0];
        assert_eq!(k, &key);
        assert_eq!(v, &value);
        assert_eq!(*version, 1);
        assert!(!(*is_deleted));

        let (k, v, _, is_deleted) = &results[1];
        assert_eq!(k, &key);
        assert_eq!(v, &Bytes::new());
        assert!(*is_deleted);
    }

    #[tokio::test]
    async fn test_scan_all_versions_multiple_keys_single_version_each_deleted() {
        let (store, _) = create_store(false);
        let keys = vec![
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
        ];
        let value = Bytes::from("value1");

        for key in &keys {
            let mut txn = store.begin().unwrap();
            txn.set_at_ts(key, &value, 1).unwrap();
            txn.commit().await.unwrap();
        }

        for key in &keys {
            let mut txn = store.begin().unwrap();
            txn.soft_delete(key).unwrap();
            txn.commit().await.unwrap();
        }

        let range = keys.first().unwrap().as_ref()..=keys.last().unwrap().as_ref();
        let txn = store.begin().unwrap();
        let results = txn.scan_all_versions(range, None).unwrap();

        assert_eq!(results.len(), keys.len() * 2);
        for (i, (k, v, version, is_deleted)) in results.iter().enumerate() {
            let key_index = i / 2;
            let is_deleted_version = i % 2 == 1;
            assert_eq!(k, &keys[key_index]);
            if is_deleted_version {
                assert_eq!(v, &Bytes::new());
                assert!(*is_deleted);
            } else {
                assert_eq!(v, &value);
                assert_eq!(*version, 1);
                assert!(!(*is_deleted));
            }
        }
    }

    #[tokio::test]
    async fn test_scan_all_versions_multiple_keys_multiple_versions_each_deleted() {
        let (store, _) = create_store(false);
        let keys = vec![
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
        ];
        let values = [
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
        ];

        for key in &keys {
            for (i, value) in values.iter().enumerate() {
                let mut txn = store.begin().unwrap();
                let version = (i + 1) as u64;
                txn.set_at_ts(key, value, version).unwrap();
                txn.commit().await.unwrap();
            }
        }

        for key in &keys {
            let mut txn = store.begin().unwrap();
            txn.soft_delete(key).unwrap();
            txn.commit().await.unwrap();
        }

        let range = keys.first().unwrap().as_ref()..=keys.last().unwrap().as_ref();
        let txn = store.begin().unwrap();
        let results = txn.scan_all_versions(range, None).unwrap();

        let mut expected_results = Vec::new();
        for key in &keys {
            for (i, value) in values.iter().enumerate() {
                expected_results.push((key.clone(), value.clone(), (i + 1) as u64, false));
            }
            expected_results.push((key.clone(), Bytes::new(), 0, true));
        }

        assert_eq!(results.len(), expected_results.len());
        for (result, expected) in results.iter().zip(expected_results.iter()) {
            let (k, v, version, is_deleted) = result;
            let (expected_key, expected_value, expected_version, expected_is_deleted) = expected;
            assert_eq!(k, expected_key);
            assert_eq!(v, expected_value);
            // Check version only if the record is not deleted
            // This is because a soft-deleted record has version which is
            // the commit timestamp of the transaction, and do not want to
            // make the test complex by predicting the exact value.
            if !expected_is_deleted {
                assert_eq!(*version, *expected_version);
            }
            assert_eq!(*is_deleted, *expected_is_deleted);
        }
    }

    #[tokio::test]
    async fn test_scan_all_versions_soft_and_hard_delete() {
        let (store, _) = create_store(false);
        let key = Bytes::from("key1");
        let value = Bytes::from("value1");

        let mut txn = store.begin().unwrap();
        txn.set_at_ts(&key, &value, 1).unwrap();
        txn.commit().await.unwrap();

        let mut txn = store.begin().unwrap();
        txn.soft_delete(&key).unwrap();
        txn.commit().await.unwrap();

        let mut txn = store.begin().unwrap();
        txn.delete(&key).unwrap();
        txn.commit().await.unwrap();

        let range = key.as_ref()..=key.as_ref();
        let txn = store.begin().unwrap();
        let results = txn.scan_all_versions(range, None).unwrap();

        assert_eq!(results.len(), 0);
    }

    #[tokio::test]
    async fn test_scan_all_versions_range_boundaries() {
        let (store, _) = create_store(false);
        let keys = vec![
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
        ];
        let value = Bytes::from("value1");

        for key in &keys {
            let mut txn = store.begin().unwrap();
            txn.set_at_ts(key, &value, 1).unwrap();
            txn.commit().await.unwrap();
        }

        // Inclusive range
        let range = keys.first().unwrap().as_ref()..=keys.last().unwrap().as_ref();
        let txn = store.begin().unwrap();
        let results = txn.scan_all_versions(range, None).unwrap();
        assert_eq!(results.len(), keys.len());

        // Exclusive range
        let range = keys.first().unwrap().as_ref()..keys.last().unwrap().as_ref();
        let txn = store.begin().unwrap();
        let results = txn.scan_all_versions(range, None).unwrap();
        assert_eq!(results.len(), keys.len() - 1);

        // Unbounded range
        let range = ..;
        let txn = store.begin().unwrap();
        let results = txn.scan_all_versions(range, None).unwrap();
        assert_eq!(results.len(), keys.len());
    }

    #[tokio::test]
    async fn test_scan_all_versions_with_limit() {
        let (store, _) = create_store(false);
        let keys = vec![
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
        ];
        let value = Bytes::from("value1");

        for key in &keys {
            let mut txn = store.begin().unwrap();
            txn.set_at_ts(key, &value, 1).unwrap();
            txn.commit().await.unwrap();
        }

        let range = keys.first().unwrap().as_ref()..=keys.last().unwrap().as_ref();
        let txn = store.begin().unwrap();
        let results = txn.scan_all_versions(range, Some(2)).unwrap();

        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn sdb_bug() {
        let (store, _) = create_store(false);

        // Define key-value pairs for the test
        let k1 = Bytes::from("/*test\0*test\0*user\0!fdtest\0");
        let k2 = Bytes::from("/!nstest\0");
        let k3 = Bytes::from("/*test\0!dbtest\0");
        let k4 = Bytes::from("/*test\0*test\0!tbuser\0");
        let k5 = Bytes::from("/*test\0*test\0*user\0!fdtest\0");

        let v1 = Bytes::from("\u{3}\0\u{1}\u{4}test\0\0\0");
        let v2 = Bytes::from("\u{3}\0\u{1}\u{4}test\0\0\0\0");
        let v3 = Bytes::from(
            "\u{4}\0\u{1}\u{4}user\0\0\0\u{1}\u{1}\0\u{1}\0\u{1}\0\u{1}\0\0\0\0\u{1}\0\0",
        );
        let v4 = Bytes::from("\u{4}\u{1}\u{1}\u{2}\u{4}\u{1}\u{4}test\u{1}\u{4}user\0\0\0\0\0\0\u{1}\u{1}\u{1}\u{1}\u{1}\u{1}\u{1}\u{1}\u{1}\0\0\0");

        {
            // Start a new read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.get(&k1).unwrap();
            txn.get(&k2).unwrap();
            txn.get(&k2).unwrap();
            txn.set(&k2, &v1).unwrap();

            txn.get(&k3).unwrap();
            txn.get(&k3).unwrap();
            txn.set(&k3, &v2).unwrap();

            txn.get(&k4).unwrap();
            txn.get(&k4).unwrap();
            txn.set(&k4, &v3).unwrap();

            txn.set(&k5, &v4).unwrap();

            txn.commit().await.unwrap();
        }

        {
            // Start a new read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.get(&k5).unwrap();
            txn.delete(&k5).unwrap();

            txn.commit().await.unwrap();
        }

        {
            // Start a new read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.get(&k5).unwrap();
            txn.get(&k2).unwrap();
            txn.get(&k3).unwrap();
            txn.get(&k4).unwrap();
            txn.set(&k5, &v4).unwrap();

            let start_key = "/*test\0*test\0*user\0!fd\0";
            let end_key = "/*test\0*test\0*user\0!fd�";

            let range = start_key.as_bytes()..end_key.as_bytes();
            txn.scan(range, None).unwrap();

            txn.commit().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_scan_all_versions_with_limit_with_multiple_versions_per_key() {
        let (store, _) = create_store(false);
        let keys = vec![
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
        ];
        let values = [
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
        ];

        // Insert multiple versions for each key
        for key in &keys {
            for (i, value) in values.iter().enumerate() {
                let mut txn = store.begin().unwrap();
                let version = (i + 1) as u64;
                txn.set_at_ts(key, value, version).unwrap();
                txn.commit().await.unwrap();
            }
        }

        let range = keys.first().unwrap().as_ref()..=keys.last().unwrap().as_ref();
        let txn = store.begin().unwrap();
        let results = txn.scan_all_versions(range, Some(2)).unwrap();
        assert_eq!(results.len(), 6);

        // Collect unique keys from the results
        let unique_keys: HashSet<_> = results.iter().map(|(k, _, _, _)| k.clone()).collect();

        // Verify that the number of unique keys is equal to the limit
        assert_eq!(unique_keys.len(), 2);

        // Verify that the results contain the latest version for each key
        for key in unique_keys {
            let latest_value = values.last().unwrap();
            let latest_version = values.len() as u64;
            let result = results
                .iter()
                .find(|(k, _, version, _)| k == &key && *version == latest_version)
                .unwrap();
            assert_eq!(result.1, *latest_value);
        }
    }

    #[tokio::test]
    async fn test_soft_delete_and_reinsert() {
        let (store, _) = create_store(false);

        // Define key-value pairs for the test
        let key1 = Bytes::from("k1");
        let value1 = Bytes::from("baz");

        {
            // Start a new read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.set(&key1, &value1).unwrap();
            txn.commit().await.unwrap();
        }

        {
            // Start a new read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.soft_delete(&key1).unwrap();
            txn.commit().await.unwrap();
        }

        {
            // Start a new read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            // txn.get(&key1).unwrap();
            txn.set(&key1, &value1).unwrap();
            let range = "k1".as_bytes()..="k3".as_bytes();

            txn.scan(range, None).unwrap();
            txn.commit().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_hard_delete_and_reinsert() {
        let (store, _) = create_store(false);

        // Define key-value pairs for the test
        let key1 = Bytes::from("k1");
        let value1 = Bytes::from("baz");

        {
            // Start a new read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.set(&key1, &value1).unwrap();
            txn.commit().await.unwrap();
        }

        {
            // Start a new read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.delete(&key1).unwrap();
            txn.commit().await.unwrap();
        }

        {
            // Start a new read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            // txn.get(&key1).unwrap();
            txn.set(&key1, &value1).unwrap();
            let range = "k1".as_bytes()..="k3".as_bytes();

            txn.scan(range, None).unwrap();
            txn.commit().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_hard_delete_and_reinsert_with_multiple_keys() {
        let (store, _) = create_store(false);

        // Define key-value pairs for the test
        let key1 = Bytes::from("k1");
        let key2 = Bytes::from("k2");
        let value1 = Bytes::from("baz");

        {
            // Start a new read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.set(&key1, &value1).unwrap();
            txn.set(&key2, &value1).unwrap();
            txn.commit().await.unwrap();
        }

        {
            // Start a new read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.delete(&key1).unwrap();
            txn.commit().await.unwrap();
        }

        {
            // Start a new read-write transaction (txn)
            let mut txn = store.begin().unwrap();
            txn.get(&key1).unwrap();
            txn.get(&key2).unwrap();
            txn.set(&key1, &value1).unwrap();
            let range = "k1".as_bytes()..="k3".as_bytes();

            txn.scan(range, None).unwrap();
            txn.commit().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_scan_includes_entries_before_commit() {
        let (store, _) = create_store(false);

        // Define key-value pairs for the test
        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");
        let key3 = Bytes::from("key3");
        let value1 = Bytes::from("value1");
        let value2 = Bytes::from("value2");
        let value3 = Bytes::from("value3");

        // Start a new read-write transaction (txn)
        let mut txn = store.begin().unwrap();
        txn.set(&key1, &value1).unwrap();
        txn.set(&key2, &value2).unwrap();
        txn.set(&key3, &value3).unwrap();

        // Define the range for the scan
        let range = "key1".as_bytes()..="key3".as_bytes();
        let results = txn.scan(range, None).unwrap();

        // Verify the results
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].0, key1);
        assert_eq!(results[0].1, value1);
        assert_eq!(results[1].0, key2);
        assert_eq!(results[1].1, value2);
        assert_eq!(results[2].0, key3);
        assert_eq!(results[2].1, value3);
    }

    #[tokio::test]
    async fn test_scan_all_versions_with_subsets() {
        let (store, _) = create_store(false);
        let keys = vec![
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
            Bytes::from("key4"),
            Bytes::from("key5"),
            Bytes::from("key6"),
            Bytes::from("key7"),
        ];
        let values = [
            Bytes::from("value1"),
            Bytes::from("value2"),
            Bytes::from("value3"),
        ];

        // Insert multiple versions for each key
        for key in &keys {
            for (i, value) in values.iter().enumerate() {
                let mut txn = store.begin().unwrap();
                let version = (i + 1) as u64;
                txn.set_at_ts(key, value, version).unwrap();
                txn.commit().await.unwrap();
            }
        }

        // Define subsets of the entire range
        let subsets = vec![
            (keys[0].as_ref()..=keys[2].as_ref()),
            (keys[1].as_ref()..=keys[3].as_ref()),
            (keys[2].as_ref()..=keys[4].as_ref()),
            (keys[3].as_ref()..=keys[5].as_ref()),
            (keys[4].as_ref()..=keys[6].as_ref()),
        ];

        // Scan each subset and collect versions
        for subset in subsets {
            let txn = store.begin().unwrap();
            let results = txn.scan_all_versions(subset, None).unwrap();

            // Collect unique keys from the results
            let unique_keys: HashSet<_> = results.iter().map(|(k, _, _, _)| k.clone()).collect();

            // Verify that the results contain all versions for each key in the subset
            for key in unique_keys {
                for (i, value) in values.iter().enumerate() {
                    let version = (i + 1) as u64;
                    let result = results
                        .iter()
                        .find(|(k, v, ver, _)| k == &key && v == value && *ver == version)
                        .unwrap();
                    assert_eq!(result.1, *value);
                    assert_eq!(result.2, version);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_scan_all_versions_with_batches() {
        let (store, _) = create_store(false);
        let keys = [
            Bytes::from("key1"),
            Bytes::from("key2"),
            Bytes::from("key3"),
            Bytes::from("key4"),
            Bytes::from("key5"),
        ];
        let versions = [
            vec![
                Bytes::from("v1"),
                Bytes::from("v2"),
                Bytes::from("v3"),
                Bytes::from("v4"),
            ],
            vec![Bytes::from("v1"), Bytes::from("v2")],
            vec![
                Bytes::from("v1"),
                Bytes::from("v2"),
                Bytes::from("v3"),
                Bytes::from("v4"),
            ],
            vec![Bytes::from("v1")],
            vec![Bytes::from("v1")],
        ];

        // Insert multiple versions for each key
        for (key, key_versions) in keys.iter().zip(versions.iter()) {
            for (i, value) in key_versions.iter().enumerate() {
                let mut txn = store.begin().unwrap();
                let version = (i + 1) as u64;
                txn.set_at_ts(key, value, version).unwrap();
                txn.commit().await.unwrap();
            }
        }

        // Set the batch size
        let batch_size: usize = 2;

        // Define a function to scan in batches
        fn scan_in_batches(
            store: &Store,
            batch_size: usize,
        ) -> Vec<Vec<(Bytes, Bytes, u64, bool)>> {
            let mut all_results = Vec::new();
            let mut start_key: Option<Bytes> = None;

            loop {
                let range = match &start_key {
                    Some(key) => (Bound::Excluded(key.as_ref()), Bound::Unbounded),
                    None => (Bound::Unbounded, Bound::Unbounded),
                };

                let txn = store.begin().unwrap();
                let results = txn.scan_all_versions(range, Some(batch_size)).unwrap();

                if results.is_empty() {
                    break;
                }

                // Convert Vec<u8> to Bytes
                let converted_results: Vec<(Bytes, Bytes, u64, bool)> = results
                    .into_iter()
                    .map(|(k, v, ts, is_deleted)| (Bytes::from(k), Bytes::from(v), ts, is_deleted))
                    .collect();

                all_results.push(converted_results.clone());

                // Update the start_key for the next batch
                start_key = Some(converted_results.last().unwrap().0.clone());
            }

            all_results
        }

        // Scan in batches and collect the results
        let all_results = scan_in_batches(&store, batch_size);

        // Verify the results
        let expected_results = [
            vec![
                (Bytes::from("key1"), Bytes::from("v1"), 1, false),
                (Bytes::from("key1"), Bytes::from("v2"), 2, false),
                (Bytes::from("key1"), Bytes::from("v3"), 3, false),
                (Bytes::from("key1"), Bytes::from("v4"), 4, false),
                (Bytes::from("key2"), Bytes::from("v1"), 1, false),
                (Bytes::from("key2"), Bytes::from("v2"), 2, false),
            ],
            vec![
                (Bytes::from("key3"), Bytes::from("v1"), 1, false),
                (Bytes::from("key3"), Bytes::from("v2"), 2, false),
                (Bytes::from("key3"), Bytes::from("v3"), 3, false),
                (Bytes::from("key3"), Bytes::from("v4"), 4, false),
                (Bytes::from("key4"), Bytes::from("v1"), 1, false),
            ],
            vec![(Bytes::from("key5"), Bytes::from("v1"), 1, false)],
        ];

        assert_eq!(all_results.len(), expected_results.len());

        for (batch, expected_batch) in all_results.iter().zip(expected_results.iter()) {
            assert_eq!(batch.len(), expected_batch.len());
            for (result, expected) in batch.iter().zip(expected_batch.iter()) {
                assert_eq!(result, expected);
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_transactions() {
        let (store, _) = create_store(false);
        let store = Arc::new(store);

        // Define the number of concurrent transactions
        let num_transactions = 1000;

        // Define key-value pairs for the test
        let keys: Vec<Bytes> = (0..num_transactions)
            .map(|i| Bytes::from(format!("key{}", i)))
            .collect();
        let values: Vec<Bytes> = (0..num_transactions)
            .map(|i| Bytes::from(format!("value{}", i)))
            .collect();

        // Create a vector to store the handles of the spawned tasks
        let mut handles = vec![];

        // Create a vector to store the transaction IDs
        let mut transaction_ids = vec![];

        // Create a vector to store the commit timestamps
        let mut commit_timestamps = vec![];

        // Spawn concurrent transactions
        for (key, value) in keys.iter().zip(values.iter()) {
            let store = Arc::clone(&store);
            let key = key.clone();
            let value = value.clone();

            let handle = task::spawn(async move {
                // Start a new read-write transaction
                let mut txn = store.begin().unwrap();
                txn.set(&key, &value).unwrap();
                txn.commit().await.unwrap();
                txn.versionstamp
            });

            handles.push(handle);
        }

        // Wait for all tasks to complete and collect the results
        for handle in handles {
            let result = handle.await.unwrap();
            if let Some((transaction_id, commit_ts)) = result {
                transaction_ids.push(transaction_id);
                commit_timestamps.push(commit_ts);
            }
        }

        // Verify that all transactions committed
        assert_eq!(transaction_ids.len(), keys.len());

        // Sort the transaction IDs and commit timestamps because we just
        // want to verify if the transaction ids are incremental and unique
        transaction_ids.sort();
        commit_timestamps.sort();

        // Verify that transaction IDs are incremental
        for i in 1..transaction_ids.len() {
            assert!(transaction_ids[i] > transaction_ids[i - 1]);
        }

        // Verify that commit timestamps are incremental
        for i in 1..commit_timestamps.len() {
            assert!(commit_timestamps[i] >= commit_timestamps[i - 1]);
        }

        // Drop the store to simulate closing it
        store.close().await.unwrap();
    }
}
