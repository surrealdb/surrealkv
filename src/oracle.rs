use bytes::Bytes;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicU64, Ordering};
use vart::VariableSizeKey;

use crate::error::{Error, Result};
use crate::option::Options;
use crate::snapshot::Snapshot;
use crate::transaction::Transaction;
use crate::vfs::FileSystem;

/// Oracle is responsible for managing transaction timestamps and isolation levels.
/// It supports two isolation levels: SnapshotIsolation and SerializableSnapshotIsolation.
pub(crate) struct Oracle {
    /// Isolation level of the transactions.
    isolation: IsolationLevel,
}

impl Oracle {
    /// Creates a new Oracle with the given options.
    /// It sets the isolation level based on the options.
    pub(crate) fn new(opts: &Options) -> Self {
        let isolation = match opts.isolation_level {
            crate::option::IsolationLevel::SnapshotIsolation => {
                IsolationLevel::SnapshotIsolation(SnapshotIsolation::new())
            }
            crate::option::IsolationLevel::SerializableSnapshotIsolation => {
                IsolationLevel::SerializableSnapshotIsolation(SerializableSnapshotIsolation::new())
            }
        };

        Self { isolation }
    }

    /// Generates a new commit timestamp for the given transaction.
    /// It delegates to the isolation level to generate the timestamp.
    pub(crate) fn new_commit_ts<V: FileSystem>(&self, txn: &Transaction<V>) -> Result<u64> {
        self.isolation.new_commit_ts(txn)
    }

    /// Returns the read timestamp.
    /// It delegates to the isolation level to get the timestamp.
    pub(crate) fn read_ts(&self) -> u64 {
        self.isolation.read_ts()
    }

    /// Sets the timestamp and increments it.
    /// It delegates to the isolation level to set and increment the timestamp.
    pub(crate) fn set_ts(&self, ts: u64) {
        self.isolation.set_ts(ts);
        self.isolation.increment_ts();
    }
}

/// Enum representing the isolation level of a transaction.
/// It can be either SnapshotIsolation or SerializableSnapshotIsolation.
pub(crate) enum IsolationLevel {
    SnapshotIsolation(SnapshotIsolation),
    SerializableSnapshotIsolation(SerializableSnapshotIsolation),
}

macro_rules! isolation_level_method {
    ($self:ident, $method:ident $(, $arg:ident)?) => {
        match $self {
            IsolationLevel::SnapshotIsolation(oracle) => oracle.$method($($arg)?),
            IsolationLevel::SerializableSnapshotIsolation(oracle) => oracle.$method($($arg)?),
        }
    };
}

impl IsolationLevel {
    /// Generates a new commit timestamp for the given transaction.
    /// It delegates to the specific isolation level to generate the timestamp.
    pub(crate) fn new_commit_ts<V: FileSystem>(&self, txn: &Transaction<V>) -> Result<u64> {
        isolation_level_method!(self, new_commit_ts, txn)
    }

    /// Returns the read timestamp.
    /// It delegates to the specific isolation level to get the timestamp.
    pub(crate) fn read_ts(&self) -> u64 {
        isolation_level_method!(self, read_ts)
    }

    /// Sets the timestamp.
    /// It delegates to the specific isolation level to set the timestamp.
    pub(crate) fn set_ts(&self, ts: u64) {
        isolation_level_method!(self, set_ts, ts)
    }

    /// Increments the timestamp.
    /// It delegates to the specific isolation level to increment the timestamp.
    pub(crate) fn increment_ts(&self) {
        isolation_level_method!(self, increment_ts)
    }
}

/// Struct representing the Snapshot Isolation level in a transaction.
/// It uses an atomic u64 to keep track of the next transaction ID.
pub(crate) struct SnapshotIsolation {
    next_tx_id: AtomicU64,
}

impl SnapshotIsolation {
    /// Creates a new SnapshotIsolation instance with the next transaction ID set to 0.
    pub(crate) fn new() -> Self {
        Self {
            next_tx_id: AtomicU64::new(0),
        }
    }

    /// Sets the next transaction ID to the given timestamp.
    pub(crate) fn set_ts(&self, ts: u64) {
        self.next_tx_id.store(ts, Ordering::SeqCst);
    }

    /// Generates a new commit timestamp for the given transaction.
    /// It performs optimistic concurrency control (OCC) by checking if the read keys in the transaction
    /// are still valid in the latest snapshot, and if the timestamp of the read keys matches the timestamp
    /// of the latest snapshot. If the timestamp does not match, then there is a conflict.
    pub(crate) fn new_commit_ts<V: FileSystem>(&self, txn: &Transaction<V>) -> Result<u64> {
        let current_snapshot = Snapshot::take(&txn.core)?;

        // Check write conflicts
        for key in txn.write_set.keys() {
            if let Some(last_entry) = txn.write_set.get(key).and_then(|entries| entries.last()) {
                // Detect if another transaction has written to this key.
                if current_snapshot
                    .get(&key[..].into())
                    .is_some_and(|(_, version)| version > last_entry.version)
                {
                    return Err(Error::TransactionWriteConflict);
                }
            }
        }

        let ts = self.next_tx_id.load(Ordering::SeqCst);
        self.increment_ts();
        Ok(ts)
    }

    /// Returns the read timestamp, which is the next transaction ID minus 1.
    pub(crate) fn read_ts(&self) -> u64 {
        self.next_tx_id.load(Ordering::SeqCst) - 1
    }

    /// Increments the next transaction ID by 1.
    pub(crate) fn increment_ts(&self) {
        self.next_tx_id.fetch_add(1, Ordering::SeqCst);
    }
}

fn key_in_range(
    key: &Bytes,
    range_start: &Bound<&VariableSizeKey>,
    range_end: &Bound<&VariableSizeKey>,
) -> bool {
    let key = VariableSizeKey::from_slice(key);

    let start_inclusive = match &range_start {
        Bound::Included(start) => key >= **start,
        Bound::Excluded(start) => key > **start,
        Bound::Unbounded => true,
    };

    let end_exclusive = match &range_end {
        Bound::Included(end) => key <= **end,
        Bound::Excluded(end) => key < **end,
        Bound::Unbounded => true,
    };

    start_inclusive && end_exclusive
}

/// Serializable Snapshot Isolation (SSI):
/// https://www.cse.iitb.ac.in/infolab/Data/Courses/CS632/2009/Papers/p492-fekete.pdf
///
/// Serializable Snapshot Isolation (SSI) is a specific isolation level that falls under the
/// category of "serializable" isolation levels. It provides serializability while allowing for
/// a higher degree of concurrency compared to traditional serializability mechanisms.
///
/// This implementation adopts Write Snapshot Isolation as defined in the paper:
/// https://arxiv.org/abs/2405.18393
pub(crate) struct SerializableSnapshotIsolation {
    next_tx_id: AtomicU64,
}

impl SerializableSnapshotIsolation {
    /// Creates a new SerializableSnapshotIsolation instance with the next transaction ID set to 0.
    pub(crate) fn new() -> Self {
        Self {
            next_tx_id: AtomicU64::new(0),
        }
    }

    /// Sets the next transaction ID to the given timestamp.
    pub(crate) fn set_ts(&self, ts: u64) {
        self.next_tx_id.store(ts, Ordering::SeqCst);
    }

    /// Generates a new commit timestamp for the given transaction.
    /// It performs optimistic concurrency control (OCC) by checking if the read keys in the transaction
    /// are still valid in the latest snapshot, and if the timestamp of the read keys matches the timestamp
    /// of the latest snapshot. If the timestamp does not match, then there is a conflict.
    pub(crate) fn new_commit_ts<V: FileSystem>(&self, txn: &Transaction<V>) -> Result<u64> {
        let current_snapshot = Snapshot::take(&txn.core)?;

        // Check read conflicts
        for entry in txn.read_set.iter() {
            match current_snapshot.get(&entry.key[..].into()) {
                Some((_, version)) => {
                    if entry.ts != version {
                        return Err(Error::TransactionReadConflict);
                    }
                }
                None => {
                    if entry.ts > 0 {
                        return Err(Error::TransactionReadConflict);
                    }
                }
            }
        }

        // Check write conflicts
        for key in txn.write_set.keys() {
            if let Some(last_entry) = txn.write_set.get(key).and_then(|entries| entries.last()) {
                // Detect if another transaction has written to this key.
                if current_snapshot
                    .get(&key[..].into())
                    .is_some_and(|(_, version)| version > last_entry.version)
                {
                    return Err(Error::TransactionWriteConflict);
                }
            }
        }

        // For each range, check if any write skew (including deletes) conflicts
        for range in &txn.read_key_ranges {
            // Get all writes in the range from the snapshot
            let range_writes = current_snapshot.range(range.range.clone());

            for (_, _, version, _) in range_writes {
                if version > txn.read_ts {
                    // Any modification after our read timestamp should cause a conflict
                    // regardless of whether we're deleting the key or not
                    return Err(Error::TransactionReadConflict);
                }
            }

            // Check for deletes in current transaction that affect this range
            for (key, entries) in &txn.write_set {
                if key_in_range(key, &range.range.start_bound(), &range.range.end_bound()) {
                    if let Some(entry) = entries.last() {
                        let key = VariableSizeKey::from_slice(key);
                        let res = current_snapshot.get(&key);
                        if entry.e.is_deleted_or_tombstone() && res.is_none() {
                            // This is a delete of a key that didn't exist at snapshot time
                            return Err(Error::TransactionWriteConflict);
                        }
                    }
                }
            }
        }

        let ts = self.next_tx_id.load(Ordering::SeqCst);
        self.increment_ts();
        Ok(ts)
    }

    /// Returns the read timestamp, which is the next transaction ID minus 1.
    pub(crate) fn read_ts(&self) -> u64 {
        self.next_tx_id.load(Ordering::SeqCst) - 1
    }

    /// Increments the next transaction ID by 1.
    pub(crate) fn increment_ts(&self) {
        self.next_tx_id.fetch_add(1, Ordering::SeqCst);
    }
}
