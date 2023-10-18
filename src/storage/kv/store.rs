use std::sync::{Arc, RwLock};

use bytes::Bytes;

use crate::storage::aol::aol::AOL;
use crate::storage::index::art::Tree as tart;
use crate::storage::index::KeyTrait;
use crate::storage::kv::error::Result;
use crate::storage::kv::option::Options;
use crate::storage::kv::oracle::Oracle;
use crate::storage::kv::transaction::{Mode, Transaction};
use crate::storage::wal::wal::WAL;

/// An MVCC-based transactional key-value store.
pub struct MVCCStore<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> {
    /// Index for store.
    pub(crate) index: RwLock<tart<P, V>>,
    /// Options for store.
    pub(crate) opts: Options,
    /// WAL for store.
    pub(crate) wal: Arc<Option<WAL>>,
    /// Value log for store.
    pub(crate) vlog: Arc<Option<AOL>>,
    /// Timestamp Oracle for store.
    pub(crate) oracle: Arc<Oracle>,
    /// Flag to indicate if store is closed.
    pub(crate) closed: bool,
}

impl<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> MVCCStore<P, V> {
    /// Creates a new MVCC key-value store with the given key-value store for storage.
    pub fn new(opts: Options) -> Self {
        Self {
            index: RwLock::new(tart::new()),
            opts,
            wal: Arc::new(None),
            vlog: Arc::new(None),
            oracle: Arc::new(Oracle::new()),
            closed: false,
        }
    }

    pub fn begin<'a>(self: &'a Arc<Self>) -> Result<Transaction<'a, P, V>> {
        let mut txn = Transaction::new(self.clone(), Mode::ReadWrite)?;
        txn.read_ts = self.oracle.read_ts();
        Ok(txn)
    }

    pub fn begin_with_mode<'a>(self: &'a Arc<Self>, mode: Mode) -> Result<Transaction<'a, P, V>> {
        let mut txn = Transaction::new(self.clone(), mode)?;
        txn.read_ts = self.oracle.read_ts();
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
        assert_eq!(store.closed, false);
    }
}
