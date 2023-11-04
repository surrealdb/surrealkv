use std::sync::Arc;

use bytes::Bytes;

use super::entry::ValueRef;
use crate::storage::index::art::TrieError;
use crate::storage::index::snapshot::Snapshot as TartSnapshot;
use crate::storage::index::VectorKey;
use crate::storage::kv::error::{Error, Result};
use crate::storage::kv::store::Core;

/// A versioned snapshot for snapshot isolation.
pub(crate) struct Snapshot {
    /// The timestamp of the snapshot. This is used to determine the visibility of the
    /// key-value pairs in the snapshot. It can be used to filter out expired key-value
    /// pairs or to filter out key-value pairs based on the snapshot timestamp.
    ts: u64,
    snap: TartSnapshot<VectorKey, Bytes>,
    store: Arc<Core>,
}

impl Snapshot {
    pub(crate) fn take(store: Arc<Core>, ts: u64) -> Result<Self> {
        let snapshot = store.indexer.write().snapshot()?;

        Ok(Self {
            ts,
            snap: snapshot,
            store,
        })
    }

    /// Set a key-value pair into the snapshot.
    pub fn set(&mut self, key: &VectorKey, value: Bytes) -> Result<()> {
        // TODO: need to fix this to avoid cloning the key
        // This happens because the VectorKey transfrom from
        // a &[u8] does not terminate the key with a null byte.
        let key = &key.terminate();
        self.snap.insert(key, value, self.ts)?;
        Ok(())
    }

    /// Retrieves the value and timestamp associated with the given key from the snapshot.
    pub fn get(&self, key: &VectorKey) -> Result<ValueRef> {
        // Create a slice with your filter function if needed, e.g., [ignore_deleted]
        let filters: Vec<fn(&ValueRef, u64) -> Result<()>> = vec![ignore_deleted];

        // TODO: need to fix this to avoid cloning the key
        // This happens because the VectorKey transfrom from
        // a &[u8] does not terminate the key with a null byte.
        let key = &key.terminate();
        self.get_with_filters(key, &filters)
    }

    pub fn get_with_filters<F>(&self, key: &VectorKey, filters: &[F]) -> Result<ValueRef>
    where
        F: FilterFn,
    {
        let (val, version, _) = self.snap.get(key, self.ts)?;
        let mut val_ref = ValueRef::new(self.store.clone());
        let val_bytes_ref: &Bytes = &val;
        val_ref.decode(version, val_bytes_ref)?;

        for filter in filters {
            filter.apply(&val_ref, self.ts)?
        }

        Ok(val_ref)
    }

    pub fn close(&mut self) -> Result<()> {
        let mut indexer = self.store.indexer.write();
        indexer.close_snapshot(self.snap.id)?;
        Ok(())
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        let err = self.close();
        if err.is_err() {
            panic!("failed to close snapshot: {:?}", err);
        }
    }
}

pub(crate) trait FilterFn {
    fn apply(&self, val_ref: &ValueRef, ts: u64) -> Result<()>;
}

fn ignore_deleted(val_ref: &ValueRef, _: u64) -> Result<()> {
    let md = val_ref.key_value_metadata();
    if let Some(md) = md {
        if md.deleted() {
            return Err(Error::Index(TrieError::KeyNotFound));
        }
    }
    Ok(())
}

impl<F> FilterFn for F
where
    F: Fn(&ValueRef, u64) -> Result<()>,
{
    fn apply(&self, val_ref: &ValueRef, ts: u64) -> Result<()> {
        self(val_ref, ts)
    }
}
