use bytes::Bytes;

use crate::storage::index::art::Tree as tart;
use crate::storage::index::art::KV;
use crate::storage::index::snapshot::Snapshot as TartSnapshot;
use crate::storage::index::VectorKey;
use crate::storage::kv::error::Result;
use crate::storage::kv::option::Options;

pub(crate) struct Indexer {
    pub(crate) index: tart<VectorKey, Bytes>,
}

impl Indexer {
    pub(crate) fn new(opts: &Options) -> Self {
        let mut index = tart::new();
        index.set_max_active_snapshots(opts.max_active_snapshots);
        Self { index }
    }

    pub(crate) fn snapshot(&mut self) -> Result<TartSnapshot<VectorKey, Bytes>> {
        let snapshot = self.index.create_snapshot()?;
        Ok(snapshot)
    }

    pub fn bulk_insert(&mut self, kv_pairs: &mut [KV<VectorKey, Bytes>]) -> Result<()> {
        // TODO: need to fix this to avoid cloning the key
        // This happens because the VectorKey transfrom from
        // a &[u8] does not terminate the key with a null byte.
        kv_pairs.iter_mut().for_each(|kv| {
            kv.key = kv.key.terminate();
        });
        self.index.bulk_insert(kv_pairs)?;
        Ok(())
    }

    pub fn version(&self) -> u64 {
        self.index.version()
    }

    pub(crate) fn close_snapshot(&mut self, snapshot_id: u64) -> Result<()> {
        self.index.close_snapshot(snapshot_id)?;
        Ok(())
    }

    pub(crate) fn close(&mut self) -> Result<()> {
        self.index.close()?;
        Ok(())
    }
}
