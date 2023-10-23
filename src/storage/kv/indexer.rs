use bytes::Bytes;

use crate::storage::index::art::Tree as tart;
use crate::storage::index::art::KV;
use crate::storage::index::snapshot::Snapshot as TartSnapshot;
use crate::storage::index::KeyTrait;
use crate::storage::kv::error::Result;

pub(crate) struct Indexer<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> {
    pub(crate) index: tart<P, V>,
}

impl<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> Indexer<P, V> {
    pub(crate) fn new() -> Self {
        Self { index: tart::new() }
    }

    pub(crate) fn snapshot(&mut self) -> Result<TartSnapshot<P, V>> {
        let snapshot = self.index.create_snapshot()?;
        Ok(snapshot)
    }

    pub(crate) fn version(&self) -> Result<u64> {
        Ok(self.index.version())
    }

    /// Set a key-value pair into the snapshot.
    pub fn insert(&mut self, key: &P, value: V, version: u64, ts: u64) -> Result<()> {
        self.index.insert(key, value, version, ts)?;
        Ok(())
    }

    pub fn bulk_insert(&mut self, kv_pairs: &[KV<P, V>]) -> Result<()> {
        self.index.bulk_insert(&kv_pairs)?;
        Ok(())
    }
}
