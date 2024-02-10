use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use bytes::Bytes;
use crossbeam_channel::{bounded, Receiver, Sender};
use hashbrown::{HashMap, HashSet};
use parking_lot::{Mutex, RwLock};
use tokio::sync::Mutex as AsyncMutex;
use vart::TrieError;

use crate::storage::kv::{
    error::{Error, Result},
    option::Options,
    snapshot::Snapshot,
    transaction::Transaction,
};

/// Oracle is responsible for managing transaction timestamps and isolation levels.
/// It uses a write lock to ensure that only one transaction can commit at a time.
/// It supports two isolation levels: SnapshotIsolation and SerializableSnapshotIsolation.
pub(crate) struct Oracle {
    /// Write lock to ensure that only one transaction can commit at a time.
    pub(crate) write_lock: AsyncMutex<()>,
    /// Isolation level of the transactions.
    isolation: IsolationLevel,
}

impl Oracle {
    /// Creates a new Oracle with the given options.
    /// It sets the isolation level based on the options.
    pub(crate) fn new(opts: &Options) -> Self {
        let isolation = match opts.isolation_level {
            crate::storage::kv::option::IsolationLevel::SnapshotIsolation => {
                IsolationLevel::SnapshotIsolation(SnapshotIsolation::new())
            }
            crate::storage::kv::option::IsolationLevel::SerializableSnapshotIsolation => {
                IsolationLevel::SerializableSnapshotIsolation(SerializableSnapshotIsolation::new())
            }
        };

        Self {
            write_lock: AsyncMutex::new(()),
            isolation,
        }
    }

    /// Generates a new commit timestamp for the given transaction.
    /// It delegates to the isolation level to generate the timestamp.
    pub(crate) fn new_commit_ts(&self, txn: &mut Transaction) -> Result<u64> {
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

    /// Marks the transactions as committed up to the given timestamp.
    /// If the isolation level is SerializableSnapshotIsolation, it delegates to the isolation level to mark the transactions.
    pub(crate) fn committed_upto(&self, ts: u64) {
        match &self.isolation {
            IsolationLevel::SnapshotIsolation(_) => {}
            IsolationLevel::SerializableSnapshotIsolation(oracle) => {
                oracle.txn_mark.done_upto(ts);
            }
        }
    }

    /// Waits for the transactions to commit up to the given timestamp.
    /// If the isolation level is SerializableSnapshotIsolation, it delegates to the isolation level to wait for the transactions.
    pub(crate) fn wait_for(&self, ts: u64) {
        match &self.isolation {
            IsolationLevel::SnapshotIsolation(_) => {}
            IsolationLevel::SerializableSnapshotIsolation(oracle) => {
                oracle.txn_mark.wait_for(ts);
            }
        }
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
    pub(crate) fn new_commit_ts(&self, txn: &mut Transaction) -> Result<u64> {
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
    pub(crate) fn new_commit_ts(&self, txn: &mut Transaction) -> Result<u64> {
        let current_snapshot = Snapshot::take(txn.core.clone(), self.read_ts())?;
        let read_set = txn.read_set.lock();

        for (key, ts) in read_set.iter() {
            match current_snapshot.get(&key[..].into()) {
                Ok(val_ref) => {
                    if *ts != val_ref.ts() {
                        return Err(Error::TransactionReadConflict);
                    }
                }
                Err(Error::IndexError(TrieError::KeyNotFound)) => {
                    if *ts > 0 {
                        return Err(Error::TransactionReadConflict);
                    }
                    continue;
                }
                Err(e) => return Err(e),
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

/// Struct representing a commit marker in a transaction.
/// It contains a timestamp and a set of conflict keys.
struct CommitMarker {
    ts: u64,
    conflict_keys: HashSet<Bytes>,
}

/// Struct for tracking committed transactions.
/// It maintains the next timestamp, a list of committed transactions, and the last cleanup timestamp.
#[derive(Default)]
struct CommitTracker {
    next_ts: u64,
    committed_transactions: Vec<CommitMarker>,
    last_cleanup_ts: u64,
}

impl CommitTracker {
    /// Creates a new CommitTracker instance with the next timestamp, committed transactions, and last cleanup timestamp set to 0.
    fn new() -> Self {
        Self {
            next_ts: 0,
            committed_transactions: Vec::new(),
            last_cleanup_ts: 0,
        }
    }

    /// Cleans up committed transactions with timestamps greater than the given maximum read timestamp.
    /// It updates the last cleanup timestamp and removes committed transactions with timestamps greater than the maximum read timestamp.
    fn cleanup_committed_transactions(&mut self, max_read_ts: u64) {
        assert!(max_read_ts >= self.last_cleanup_ts);

        if max_read_ts == self.last_cleanup_ts {
            return;
        }

        self.last_cleanup_ts = max_read_ts;

        self.committed_transactions
            .retain(|txn| txn.ts > max_read_ts);
    }

    /// Checks if a transaction has conflicts with committed transactions.
    /// It acquires a lock on the read set and checks if there are any conflict keys in the read set.
    fn has_conflict(&self, txn: &Transaction) -> bool {
        let read_set = txn.read_set.lock();

        if read_set.is_empty() {
            false
        } else {
            self.committed_transactions
                .iter()
                .filter(|committed_txn| committed_txn.ts > txn.read_ts)
                .any(|committed_txn| {
                    read_set
                        .iter()
                        .any(|read| committed_txn.conflict_keys.contains(&read.0))
                })
        }
    }
}

/// Serializable Snapshot Isolation (SSI):
/// https://www.cse.iitb.ac.in/infolab/Data/Courses/CS632/2009/Papers/p492-fekete.pdf
///
/// Serializable Snapshot Isolation (SSI) is a specific isolation level that falls under the
/// category of "serializable" isolation levels. It provides serializability while allowing for
/// a higher degree of concurrency compared to traditional serializability mechanisms.
///
/// SSI allows for "snapshot isolation" semantics, where a transaction sees a consistent snapshot of
/// the database as of its start time. It uses timestamps to control the order in which transactions
/// read and write data, preventing anomalies like write skew and lost updates.
///
/// This struct manages the coordination of read and write operations by maintaining timestamps
/// for transactions and tracking committed transactions.
///
/// - `commit_tracker` maintains information about committed transactions and their timestamps.
/// - `txn_mark` is a watermark used to block new transactions until previous commits are visible.
/// - `read_mark` is another watermark that marks the visibility of read operations to other transactions.
///
/// The serializable snapshot isolation (SSI) algorithm implemented here is inspired from BadgerDB.
pub(crate) struct SerializableSnapshotIsolation {
    // The `commit_tracker` keeps track of committed transactions and their timestamps.
    commit_tracker: Mutex<CommitTracker>,

    // The `txn_mark` and `read_mark` are used to manage visibility of transactions.
    // `txn_mark` blocks `new_transaction` to ensure previous commits are visible to new reads.
    txn_mark: Arc<WaterMark>,
    // `read_mark` marks the visibility of read operations to other transactions.
    read_mark: Arc<RwLock<BinaryHeap<Reverse<u64>>>>,
}

impl SerializableSnapshotIsolation {
    // Create a new instance of `SerializableSnapshotIsolation`.
    pub(crate) fn new() -> Self {
        Self {
            commit_tracker: Mutex::new(CommitTracker::new()),
            // Create a watermark for transactions.
            txn_mark: Arc::new(WaterMark::new()),
            // Create a watermark for read operations.
            read_mark: Arc::new(RwLock::new(BinaryHeap::new())),
        }
    }

    // Retrieve the read timestamp for a new read operation.
    pub(crate) fn read_ts(&self) -> u64 {
        let commit_tracker = self.commit_tracker.lock();
        let read_ts = commit_tracker.next_ts - 1;

        // Keep track of the read timestamp for active transactions.
        self.read_mark.write().push(Reverse(read_ts));

        // Wait for the current read timestamp to be visible to new transactions.
        self.txn_mark.wait_for(read_ts);
        read_ts
    }

    // Generate a new commit timestamp for a transaction.
    pub(crate) fn new_commit_ts(&self, txn: &mut Transaction) -> Result<u64> {
        let mut commit_tracker = self.commit_tracker.lock();

        // Check for conflicts between the transaction and committed transactions.
        if commit_tracker.has_conflict(txn) {
            return Err(Error::TransactionReadConflict);
        }

        // Mark that read operations are done up to the transaction's read timestamp.
        self.mark_read_operations_done(txn.read_ts);

        // Clean up committed transactions up to the current read mark.
        let max_read_ts = self.read_mark.read().peek().map_or(0, |peek| peek.0);
        commit_tracker.cleanup_committed_transactions(max_read_ts);

        let ts = commit_tracker.next_ts;
        commit_tracker.next_ts += 1;

        assert!(ts >= commit_tracker.last_cleanup_ts);

        // Add the transaction to the list of committed transactions with conflict keys.
        let conflict_keys: HashSet<Bytes> =
            txn.write_set.iter().map(|(key, _)| key.clone()).collect();

        commit_tracker
            .committed_transactions
            .push(CommitMarker { ts, conflict_keys });

        Ok(ts)
    }

    // Helper method to mark read operations as done up to a given timestamp.
    fn mark_read_operations_done(&self, read_ts: u64) {
        self.read_mark
            .write()
            .retain(|&read_timestamp| read_timestamp.0 > read_ts);
    }

    // Set the global timestamp for the system.
    pub(crate) fn set_ts(&self, ts: u64) {
        self.commit_tracker.lock().next_ts = ts;

        // Mark that read operations are done up to the given timestamp.
        self.txn_mark.done_upto(ts);

        // Mark that reads are done up to the given timestamp.
        self.read_mark.write().retain(|&read_ts| read_ts.0 > ts);
    }

    // Increment the global timestamp for the system.
    pub(crate) fn increment_ts(&self) {
        let mut commit_info = self.commit_tracker.lock();
        commit_info.next_ts += 1;
    }
}

/// `WaterMark` is a synchronization mechanism for managing transaction timestamps.
struct WaterMark {
    mark: RwLock<WaterMarkState>, // Keeps track of waiters for specific timestamps.
}

struct WaterMarkState {
    done_upto: u64,
    waiters: HashMap<u64, Arc<Mark>>,
}

impl WaterMarkState {
    fn new() -> Self {
        Self {
            done_upto: 0,
            waiters: HashMap::new(),
        }
    }
}

/// Represents a waiter for a specific timestamp.
struct Mark {
    ch: Mutex<Option<Sender<()>>>, // Sender for notifying the waiter.
    closer: Receiver<()>,          // Receiver for detecting closure.
}

impl Mark {
    fn new() -> Arc<Self> {
        let (tx, rx) = bounded(1);
        Arc::new(Self {
            ch: Mutex::new(Some(tx)),
            closer: rx,
        })
    }

    fn take(&self) -> Sender<()> {
        self.ch.lock().take().unwrap()
    }
}

impl WaterMark {
    /// Creates a new `WaterMark` with the given initial done timestamp.
    fn new() -> Self {
        WaterMark {
            mark: RwLock::new(WaterMarkState::new()),
        }
    }

    /// Marks transactions as done up to the specified timestamp.
    fn done_upto(&self, t: u64) {
        let mut mark = self.mark.write();

        let done_upto = mark.done_upto;
        if done_upto >= t {
            return;
        }

        for i in (done_upto + 1)..=t {
            if let Some(wp) = mark.waiters.get(&i) {
                wp.take();
                mark.waiters.remove(&i);
            }
        }

        mark.done_upto = t;
    }

    /// Waits for transactions to be done up to the specified timestamp.
    fn wait_for(&self, t: u64) {
        let mark = self.mark.read();
        if mark.done_upto >= t {
            return;
        }
        let should_insert = !mark.waiters.contains_key(&t);
        drop(mark);

        if should_insert {
            let mut mark = self.mark.write();
            mark.waiters.entry(t).or_insert_with(Mark::new);
            drop(mark);
        }

        let mark = self.mark.read(); // Re-acquire the read lock.
        let wp = mark.waiters.get(&t).unwrap().clone();
        drop(mark);
        matches!(wp.closer.recv(), Err(crossbeam_channel::RecvError));
    }

    /// Gets the highest completed timestamp.
    fn _done_until(&self) -> u64 {
        let mark = self.mark.read();
        mark.done_upto
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn waiters_new() {
        let hub = WaterMark::new();

        hub.done_upto(10);
        let t2 = hub._done_until();
        assert_eq!(t2, 10);

        for i in 1..=10 {
            hub.wait_for(i);
        }
    }

    #[test]
    fn waiters_async() {
        let hub = Arc::new(WaterMark::new());
        let hub_clone = Arc::clone(&hub);

        // Spawn a thread to complete timestamp 1.
        thread::spawn(move || {
            // Wait for a while and then complete timestamp 1.
            thread::sleep(std::time::Duration::from_millis(10));
            hub_clone.done_upto(10);
        });

        // Now, wait for timestamp 1 in the main thread.
        hub.wait_for(10);
    }
}
