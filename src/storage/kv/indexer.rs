use bytes::Bytes;

use crate::storage::kv::error::Result;

use vart::{art::Tree as VartIndex, snapshot::Snapshot as VartSnapshot, VariableSizeKey};

/// The `Indexer` struct is responsible for managing the index of key-value pairs.
/// It uses a `vart` index, which is a type of persistent, lock-free B+ tree.
pub(crate) struct Indexer {
    pub(crate) index: VartIndex<VariableSizeKey, Bytes>,
}

impl Indexer {
    /// Creates a new `Indexer` instance.
    pub(crate) fn new() -> Self {
        let index = VartIndex::new();
        Self { index }
    }

    /// Creates a snapshot of the current state of the index.
    pub(crate) fn snapshot(&self) -> VartSnapshot<VariableSizeKey, Bytes> {
        self.index.create_snapshot()
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
                .insert_unchecked(key, value.clone(), version, ts)?;
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &mut VariableSizeKey) {
        *key = key.terminate();
        self.index.remove(key);
    }

    /// Returns the current version of the index.
    pub fn version(&self) -> u64 {
        self.index.version()
    }
}
