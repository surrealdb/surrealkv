use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) struct Oracle {
    next_tx_id: AtomicU64,
}

impl Oracle {
    pub(crate) fn new() -> Self {
        Self {
            next_tx_id: AtomicU64::new(0),
        }
    }

    pub(crate) fn set_txn_id(&self, txn_id: u64) {
        self.next_tx_id.store(txn_id, Ordering::SeqCst);
    }

    pub(crate) fn new_commit_ts(&self) -> u64 {
        self.next_tx_id.fetch_add(1, Ordering::SeqCst);
        self.next_tx_id.load(Ordering::SeqCst)
    }

    pub(crate) fn read_ts(&self) -> u64 {
        // self.next_tx_id.load(Ordering::SeqCst) - 1
        self.next_tx_id.load(Ordering::SeqCst)
    }
}
