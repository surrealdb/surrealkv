use bytes::Bytes;

use crate::storage::{
    index::{
        art::{Tree as vart, KV},
        snapshot::Snapshot as VartSnapshot,
        VariableKey,
    },
    kv::error::Result,
    kv::option::Options,
};

/// The `Indexer` struct is responsible for managing the index of key-value pairs.
/// It uses a `vart` index, which is a type of persistent, lock-free B+ tree.
pub(crate) struct Indexer {
    pub(crate) index: vart<VariableKey, Bytes>,
}

impl Indexer {
    /// Creates a new `Indexer` instance.
    /// The maximum number of active snapshots is set based on the provided options.
    pub(crate) fn new(opts: &Options) -> Self {
        let mut index = vart::new();
        index.set_max_active_snapshots(opts.max_active_snapshots);
        Self { index }
    }

    /// Creates a snapshot of the current state of the index.
    pub(crate) fn snapshot(&mut self) -> Result<VartSnapshot<VariableKey, Bytes>> {
        let snapshot = self.index.create_snapshot()?;
        Ok(snapshot)
    }

    /// Inserts multiple key-value pairs into the index.
    /// Note: Currently, the keys are cloned to ensure they are null-terminated.
    /// This is a known issue that needs to be fixed.
    pub fn bulk_insert(&mut self, kv_pairs: &mut [KV<VariableKey, Bytes>]) -> Result<()> {
        kv_pairs.iter_mut().for_each(|kv| {
            kv.key = kv.key.terminate();
        });
        self.index.bulk_insert(kv_pairs)?;
        Ok(())
    }

    /// Returns the current version of the index.
    pub fn version(&self) -> u64 {
        self.index.version()
    }

    /// Closes a snapshot of the index.
    pub(crate) fn close_snapshot(&mut self, snapshot_id: u64) -> Result<()> {
        self.index.close_snapshot(snapshot_id)?;
        Ok(())
    }

    /// Closes the index.
    pub(crate) fn close(&mut self) -> Result<()> {
        self.index.close()?;
        Ok(())
    }
}
