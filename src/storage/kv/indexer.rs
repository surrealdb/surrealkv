
use std::sync::{Arc, RwLock};

use bytes::Bytes;

use crate::storage::index::art::Tree as tart;
use crate::storage::index::snapshot::Snapshot as TartSnapshot;
use crate::storage::index::KeyTrait;
use crate::storage::kv::error::{Error, Result};
use crate::storage::kv::store::MVCCStore;

pub(crate) struct Indexer<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> {
    pub(crate) index: RwLock<tart<P, V>>,
    store: Arc<MVCCStore<P, V>>,
}

impl<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> Indexer<P, V> {
    pub(crate) fn new(store: Arc<MVCCStore<P, V>>) -> Self {
        Self {
            store: store,
            index: RwLock::new(tart::new()),
        }
    }

    pub(crate) fn snapshot(&mut self) -> Result<TartSnapshot<P, V>> {
        let mut index = self.index.write()?;
        let snapshot = index.create_snapshot()?;
        std::mem::drop(index);

        Ok(snapshot)
    }

    pub(crate) fn version(&self) -> Result<u64> {
        Ok(self.index.read()?.version())
    }
}
