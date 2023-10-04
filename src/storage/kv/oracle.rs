use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) struct Oracle {
    log_seq_num: AtomicU64,
    visible_seq_num: AtomicU64,
}

impl Oracle {
    pub(crate) fn new() -> Self {
        Self {
            log_seq_num: AtomicU64::new(0),
            visible_seq_num: AtomicU64::new(0),
        }
    }

    pub(crate) fn new_commit_ts(&self) -> u64 {
        self.log_seq_num.fetch_add(1, Ordering::SeqCst);
        self.log_seq_num.load(Ordering::SeqCst)
    }

    pub(crate) fn read_ts(&self) -> u64 {
        self.visible_seq_num.load(Ordering::SeqCst)
    }
}
