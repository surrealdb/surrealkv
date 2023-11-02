use bytes::Bytes;

use crate::storage::index::art::Tree as tart;
use crate::storage::index::art::KV;
use crate::storage::index::snapshot::Snapshot as TartSnapshot;
use crate::storage::index::KeyTrait;
use crate::storage::kv::error::Result;

pub(crate) struct Indexer<P: KeyTrait> {
    pub(crate) index: tart<P, Bytes>,
}

impl<P: KeyTrait> Indexer<P> {
    pub(crate) fn new() -> Self {
        Self { index: tart::new() }
    }

    pub(crate) fn snapshot(&mut self) -> Result<TartSnapshot<P, Bytes>> {
        let snapshot = self.index.create_snapshot()?;
        Ok(snapshot)
    }

    /// Set a key-value pair into the snapshot.
    pub fn insert(&mut self, key: &P, value: Bytes, version: u64, ts: u64) -> Result<()> {
        self.index.insert(key, value, version, ts)?;
        Ok(())
    }

    pub fn bulk_insert(&mut self, kv_pairs: &[KV<P, Bytes>]) -> Result<()> {
        self.index.bulk_insert(&kv_pairs)?;
        Ok(())
    }

    pub fn version(&self) -> u64 {
        self.index.version()
    }
}
