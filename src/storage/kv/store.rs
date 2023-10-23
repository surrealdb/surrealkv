use std::sync::{Arc, RwLock};

use bytes::Bytes;

use crate::storage::index::art::Tree as tart;
use crate::storage::index::KeyTrait;
use crate::storage::kv::error::Result;
use crate::storage::kv::indexer::Indexer;
use crate::storage::kv::option::Options;
use crate::storage::kv::oracle::Oracle;
use crate::storage::kv::transaction::{Mode, Transaction};
use crate::storage::log::aol::aol::AOL;
use crate::storage::log::wal::wal::WAL;
use crate::storage::log::Options as LogOptions;

/// An MVCC-based transactional key-value store.
pub struct MVCCStore<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> {
    pub(crate) core: Arc<Core<P, V>>,
}

impl<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> MVCCStore<P, V> {
    /// Creates a new MVCC key-value store with the given key-value store for storage.
    pub fn new(opts: Options) -> Self {
        let core = Arc::new(Core::new(opts));
        Self { core }
    }

    pub fn begin<'a>(self: &'a Arc<Self>) -> Result<Transaction<'a, P, V>> {
        let mut txn = Transaction::new(self.core.clone(), Mode::ReadWrite)?;
        txn.read_ts = self.core.oracle.read_ts();
        Ok(txn)
    }

    pub fn begin_with_mode<'a>(self: &'a Arc<Self>, mode: Mode) -> Result<Transaction<'a, P, V>> {
        let mut txn = Transaction::new(self.core.clone(), mode)?;
        txn.read_ts = self.core.oracle.read_ts();
        Ok(txn)
    }

    pub fn view(
        self: Arc<Self>,
        f: impl FnOnce(&mut Transaction<P, V>) -> Result<()>,
    ) -> Result<()> {
        let mut txn = self.begin_with_mode(Mode::ReadOnly)?;
        f(&mut txn)?;

        Ok(())
    }

    pub fn write(
        self: Arc<Self>,
        f: impl FnOnce(&mut Transaction<P, V>) -> Result<()>,
    ) -> Result<()> {
        let mut txn = self.begin_with_mode(Mode::ReadWrite)?;
        f(&mut txn)?;
        txn.commit()?;

        Ok(())
    }

    pub fn closed(&self) -> bool {
        self.core.closed
    }
}

pub struct Core<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> {
    /// Index for store.
    pub(crate) indexer: RwLock<Indexer<P, V>>,
    /// Options for store.
    pub(crate) opts: Options,
    /// WAL for store.
    pub(crate) wal: Arc<WAL>,
    /// Transaction log for store.
    pub(crate) tlog: Arc<RwLock<AOL>>,
    /// Transaction ID Oracle for store.
    pub(crate) oracle: Arc<Oracle>,
    /// Flag to indicate if store is closed.
    pub(crate) closed: bool,
}

impl<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> Core<P, V> {
    /// Creates a new MVCC key-value store with the given key-value store for storage.
    pub fn new(opts: Options) -> Self {
        let lopts = LogOptions::default();
        let topts = LogOptions::default();
        // TODO: remove expect
        let wal = WAL::open(&opts.dir, lopts).expect("failed to open WAL");
        let tlog = AOL::open(&opts.dir, &topts).expect("failed to open transaction log");

        // TODO:: set it to max tx id from tx log
        let oracle = Oracle::new();
        oracle.set_txn_id(1);

        Self {
            indexer: RwLock::new(Indexer::new()),
            opts: opts,
            wal: Arc::new(wal),
            tlog: Arc::new(RwLock::new(tlog)),
            oracle: Arc::new(oracle),
            closed: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::index::VectorKey;
    use crate::storage::kv::option::Options;
    use crate::storage::kv::store::MVCCStore;
    use crate::storage::kv::util::NoopValue;

    #[test]
    fn test_new_store() {
        let opts = Options::new();
        let store = MVCCStore::<VectorKey, NoopValue>::new(opts);
        assert_eq!(store.closed(), false);
    }
}
