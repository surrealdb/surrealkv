use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) struct CompactionStats {
    records_added: AtomicU64,
    records_deleted: AtomicU64,
    tombstones: AtomicU64,
}

impl CompactionStats {
    pub(crate) fn new() -> Self {
        CompactionStats {
            records_added: AtomicU64::new(0),
            records_deleted: AtomicU64::new(0),
            tombstones: AtomicU64::new(0),
        }
    }

    // Setters
    pub(crate) fn add_record(&self) {
        self.records_added.fetch_add(1, Ordering::SeqCst);
    }

    #[allow(unused)]
    pub(crate) fn delete_record(&self) {
        self.records_deleted.fetch_add(1, Ordering::SeqCst);
    }

    #[allow(unused)]
    pub(crate) fn add_tombstone(&self) {
        self.tombstones.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn add_multiple_deleted_records(&self, n: u64) {
        self.records_deleted.fetch_add(n, Ordering::SeqCst);
    }

    #[allow(unused)]
    pub(crate) fn add_multiple_records(&self, n: u64) {
        self.records_added.fetch_add(n, Ordering::SeqCst);
    }

    // Reset
    pub(crate) fn reset(&self) {
        self.records_added.store(0, Ordering::SeqCst);
        self.records_deleted.store(0, Ordering::SeqCst);
        self.tombstones.store(0, Ordering::SeqCst);
    }

    // Getters
    #[allow(unused)]
    pub(crate) fn get_records_added(&self) -> u64 {
        self.records_added.load(Ordering::SeqCst)
    }

    #[allow(unused)]
    pub(crate) fn get_records_deleted(&self) -> u64 {
        self.records_deleted.load(Ordering::SeqCst)
    }

    #[allow(unused)]
    pub(crate) fn get_tombstones(&self) -> u64 {
        self.tombstones.load(Ordering::SeqCst)
    }
}
pub(crate) struct StorageStats {
    pub(crate) compaction_stats: CompactionStats,
    pub(crate) keys_deleted: AtomicU64,
    pub(crate) tombstone_count: AtomicU64,
}

impl StorageStats {
    pub(crate) fn new() -> Self {
        StorageStats {
            compaction_stats: CompactionStats::new(),
            keys_deleted: AtomicU64::new(0),
            tombstone_count: AtomicU64::new(0),
        }
    }

    // Methods to interact with keys_deleted and tombstone_count
    #[allow(unused)]
    pub(crate) fn key_deleted(&self) {
        self.keys_deleted.fetch_add(1, Ordering::SeqCst);
    }

    #[allow(unused)]
    pub(crate) fn tombstone_counted(&self) {
        self.tombstone_count.fetch_add(1, Ordering::SeqCst);
    }

    #[allow(unused)]
    pub(crate) fn reset(&self) {
        self.compaction_stats.reset();
        self.keys_deleted.store(0, Ordering::SeqCst);
        self.tombstone_count.store(0, Ordering::SeqCst);
    }
}
