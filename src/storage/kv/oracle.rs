use std::{
    cell::{Cell, RefCell},
    cmp::Reverse,
    collections::HashSet,
    collections::{BinaryHeap, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use bytes::Bytes;
use crossbeam_channel::{bounded, Receiver, Sender};

use crate::storage::index::KeyTrait;
use crate::storage::kv::transaction::Transaction;
use crate::storage::kv::snapshot::Snapshot;
use crate::storage::kv::error::{Error, Result};
use crate::storage::index::art::TrieError;

pub(crate) enum IsolationLevel<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> {
    SnapshotIsolation(SnapshotIsolation<P, V>),
    SerializableSnapshotIsolation(SerializableSnapshotIsolation<P, V>),
}

// impl<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> Default for IsolationLevel<P, V> {
//     fn default() -> Self {
//         IsolationLevel::SerializableSnapshotIsolation(SerializableSnapshotIsolation::default())
//     }
// }

impl<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> IsolationLevel<P, V> {
    pub(crate) fn new_commit_ts(&self, txn: &mut Transaction<P, V>) -> Result<u64> {
        match self {
            IsolationLevel::SnapshotIsolation(oracle) => oracle.new_commit_ts(txn),
            IsolationLevel::SerializableSnapshotIsolation(oracle) => oracle.new_commit_ts(txn),
        }
    }

    pub(crate) fn read_ts(&self) -> u64 {
        match self {
            IsolationLevel::SnapshotIsolation(oracle) => oracle.read_ts(),
            IsolationLevel::SerializableSnapshotIsolation(oracle) => oracle.read_ts(),
        }
    }

    pub(crate) fn set_txn_id(&self, txn_id: u64) {
        match self {
            IsolationLevel::SnapshotIsolation(oracle) => oracle.set_txn_id(txn_id),
            IsolationLevel::SerializableSnapshotIsolation(oracle) => oracle.set_txn_id(txn_id),
        }
    }
}

pub(crate) struct Oracle<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> {
    lock: Mutex<()>,
    isolation: IsolationLevel<P, V>,
}

impl<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> Oracle<P, V> {
    pub(crate) fn new() -> Self {
        Self {
            lock: Mutex::new(()),
            isolation: IsolationLevel::SnapshotIsolation(SnapshotIsolation::new()),
        }
    }

    pub(crate) fn new_commit_ts(&self, txn: &mut Transaction<P, V>) -> Result<u64> {
        self.isolation.new_commit_ts(txn)
    }

    pub(crate) fn read_ts(&self) -> u64 {
        self.isolation.read_ts()
    }

    pub(crate) fn set_txn_id(&self, txn_id: u64) {
        self.isolation.set_txn_id(txn_id);
    }
}

pub(crate) struct SnapshotIsolation<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> {
    next_tx_id: AtomicU64,
    _phantom: std::marker::PhantomData<(P, V)>,
}

impl<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> SnapshotIsolation<P, V> {
    pub(crate) fn new() -> Self {
        Self {
            next_tx_id: AtomicU64::new(0),
            _phantom: std::marker::PhantomData,
        }
    }

    pub(crate) fn set_txn_id(&self, txn_id: u64) {
        self.next_tx_id.store(txn_id, Ordering::SeqCst);
    }

    pub(crate) fn new_commit_ts(&self, txn: &mut Transaction<P, V>) -> Result<u64> {
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
        let current_snapshot = Snapshot::take(txn.store.clone(), self.read_ts())?;

        let read_set = txn.read_set.lock().unwrap();

        for (key, ts) in read_set.iter() {
            match current_snapshot.get(&key[..].into()) {
                Ok(val_ref) => {
                    if *ts != val_ref.ts {
                        return Err(Error::TxnReadConflict);
                    }
                }
                Err(e) => {
                    match &e {
                        Error::Index(trie_error) => {
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



        self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        Ok(self.next_tx_id.load(Ordering::SeqCst))
    }

    pub(crate) fn read_ts(&self) -> u64 {
        // self.next_tx_id.load(Ordering::SeqCst) - 1
        self.next_tx_id.load(Ordering::SeqCst)
    }
}

struct CommitMarker {
    ts: u64,
    conflict_keys: HashSet<Bytes>,
}

#[derive(Default)]
struct CommitTracker<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> {
    next_txn_ts: u64,
    committed_txns: Vec<CommitMarker>,
    last_cleanup_ts: u64,

    _phantom: std::marker::PhantomData<(P, V)>,
}

impl<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> CommitTracker<P, V> {
    fn cleanup_committed_transactions(&mut self, max_read_ts: u64) {
        assert!(max_read_ts >= self.last_cleanup_ts);

        if max_read_ts == self.last_cleanup_ts {
            return;
        }

        self.last_cleanup_ts = max_read_ts;
        self.committed_txns.retain(|txn| txn.ts > max_read_ts);
    }

    fn has_conflict(&self, txn: &Transaction<P, V>) -> bool {
        let read_set = txn.read_set.lock().unwrap();
        if read_set.is_empty() {
            false
        } else {
            self.committed_txns
                .iter()
                .filter(|committed_txn| committed_txn.ts > txn.read_ts)
                .any(|committed_txn| {
                    read_set
                        .iter()
                        .any(|read| committed_txn.conflict_keys.contains(read.0))
                })
        }
    }
}

pub(crate) struct SerializableSnapshotIsolation<
    P: KeyTrait,
    V: Clone + AsRef<Bytes> + From<bytes::Bytes>,
> {
    commit_tracker: Mutex<CommitTracker<P, V>>,

    /// Used to block `new_transaction`, so all previous commits are visible to a new read.
    txn_mark: Arc<WaterMark>,

    /// Used by DB.
    read_mark: Arc<WaterMark>,
}

impl<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>>
    SerializableSnapshotIsolation<P, V>
{
    pub(crate) fn read_ts(&self) -> u64 {
        let commit_tracker = self.commit_tracker.lock().unwrap();
        let read_ts = commit_tracker.next_txn_ts - 1;

        self.txn_mark.wait_for(read_ts);
        read_ts
    }

    pub(crate) fn new_commit_ts(&self, txn: &mut Transaction<P, V>) -> Result<u64> {
        let mut commit_tracker = self.commit_tracker.lock().unwrap();
        if commit_tracker.has_conflict(txn) {
            return Err(Error::TxnReadConflict);
        }
        let ts = {
            self.read_mark.done_upto(txn.read_ts);
            commit_tracker.cleanup_committed_transactions(self.read_mark.done_until());

            let txn_ts = commit_tracker.next_txn_ts;
            commit_tracker.next_txn_ts += 1;
            txn_ts
        };

        assert!(ts >= commit_tracker.last_cleanup_ts);

        commit_tracker.committed_txns.push(
            CommitMarker {
            ts,
            conflict_keys: txn.write_set.keys().map(|&k| k.clone()).collect(),
        });

        Ok(ts)
    }

    pub(crate) fn set_txn_id(&self, txn_id: u64) {
        self.commit_tracker.lock().unwrap().next_txn_ts = txn_id;
    }
}

struct WaterMark {
    done_upto: Cell<u64>,
    mutex: Mutex<()>,
    waiters: RefCell<HashMap<u64, Mark>>,
}

struct Mark {
    ch: Option<Sender<()>>,
    closer: Receiver<()>,
}

impl WaterMark {
    fn new(done_upto: u64) -> Self {
        WaterMark {
            waiters: RefCell::new(HashMap::new()),
            done_upto: Cell::new(done_upto),
            mutex: Mutex::new(()),
        }
    }

    fn done_upto(&self, t: u64) {
        let mut _guard = self.mutex.lock().unwrap();

        let done_upto = self.done_upto.get();
        if done_upto >= t {
            return;
        }

        for i in (done_upto + 1)..=t {
            let mut waiters = self.waiters.borrow_mut();
            if let Some(wp) = waiters.get_mut(&i) {
                wp.ch.take();
                waiters.remove(&i);
            }
        }

        self.done_upto.set(t);
    }

    fn wait_for(&self, t: u64) {
        let mut _guard = self.mutex.lock().unwrap();

        if self.done_upto.get() >= t {
            return;
        }

        let mut waiters = self.waiters.borrow_mut();
        let wp = waiters.entry(t).or_insert_with(|| {
            let (tx, rx) = bounded(1);
            Mark {
                ch: Some(tx),
                closer: rx,
            }
        });

        drop(_guard);

        matches!(wp.closer.recv(), Err(crossbeam_channel::RecvError));
    }

    fn done_until(&self) -> u64 {
        let _guard = self.mutex.lock().unwrap();
        self.done_upto.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_waiters_hub() {
        let mut hub = WaterMark::new(0);

        hub.done_upto(10);
        let t2 = hub.done_until();
        assert_eq!(t2, 10);
        hub.wait_for(1);
        hub.wait_for(10);
    }
}
