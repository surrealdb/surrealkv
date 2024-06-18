use bytes::Bytes;

use crate::storage::kv::error::Result;

use vart::{
    art::{Tree as VartIndex, KV},
    snapshot::Snapshot as VartSnapshot,
    VariableSizeKey,
};

/// The `Indexer` struct is responsible for managing the index of key-value pairs.
/// It uses a `vart` index, which is a type of persistent, lock-free B+ tree.
pub(crate) struct Indexer {
    pub(crate) index: VartIndex<VariableSizeKey, Bytes>,
}

impl Indexer {
    /// Creates a new `Indexer` instance.
    /// The maximum number of active snapshots is set based on the provided options.
    pub(crate) fn new() -> Self {
        let index = VartIndex::new();
        Self { index }
    }

    /// Creates a snapshot of the current state of the index.
    pub(crate) fn snapshot(&self) -> Result<VartSnapshot<VariableSizeKey, Bytes>> {
        let snapshot = self.index.create_snapshot()?;
        Ok(snapshot)
    }

    /// Inserts multiple key-value pairs into the index.
    /// Note: Currently, the keys are cloned to ensure they are null-terminated.
    /// This is a known issue that needs to be fixed.
    pub fn bulk_insert(&mut self, kv_pairs: &mut [KV<VariableSizeKey, Bytes>]) -> Result<()> {
        kv_pairs.iter_mut().for_each(|kv| {
            kv.key = kv.key.terminate();
        });
        self.index.bulk_insert(kv_pairs, true)?;
        Ok(())
    }

    pub fn insert(
        &mut self,
        key: &mut VariableSizeKey,
        value: Bytes,
        version: u64,
        ts: u64,
        check_version: bool,
    ) -> Result<()> {
        *key = key.terminate();
        if check_version {
            self.index.insert(key, value.clone(), version, ts)?;
        } else {
            self.index
                .insert_without_version_increment_check(key, value.clone(), version, ts)?;
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &mut VariableSizeKey) -> Result<()> {
        *key = key.terminate();
        self.index.remove(key)?;
        Ok(())
    }

    /// Returns the current version of the index.
    pub fn version(&self) -> u64 {
        self.index.version()
    }

    /// Closes the index.
    pub(crate) fn close(&mut self) -> Result<()> {
        self.index.close()?;
        Ok(())
    }
}
