use std::sync::Arc;

use bytes::Bytes;

use super::entry::{Value, ValueRef};
use crate::storage::{
    index::{
        art::TrieError, iter::IterationPointer, snapshot::Snapshot as TartSnapshot, VariableKey,
    },
    kv::error::{Error, Result},
    kv::store::Core,
};

pub(crate) const FILTERS: [fn(&ValueRef, u64) -> Result<()>; 1] = [ignore_deleted];

/// A versioned snapshot for snapshot isolation.
pub(crate) struct Snapshot {
    /// The timestamp of the snapshot. This is used to determine the visibility of the
    /// key-value pairs in the snapshot. It can be used to filter out expired key-value
    /// pairs or deleted key-value pairs based on the read timestamp.
    ts: u64,
    snap: TartSnapshot<VariableKey, Bytes>,
    store: Arc<Core>,
}

impl Snapshot {
    pub(crate) async fn take(store: Arc<Core>, ts: u64) -> Result<Self> {
        let snapshot = store.indexer.write().await.snapshot()?;

        Ok(Self {
            ts,
            snap: snapshot,
            store,
        })
    }

    /// Set a key-value pair into the snapshot.
    pub fn set(&mut self, key: &VariableKey, value: Bytes) -> Result<()> {
        // TODO: need to fix this to avoid cloning the key
        // This happens because the VariableKey transfrom from
        // a &[u8] does not terminate the key with a null byte.
        let key = &key.terminate();
        self.snap.insert(key, value, self.snap.ts)?;
        Ok(())
    }

    /// Deletes given key from the snapshot.
    pub fn delete(&mut self, key: &VariableKey) -> Result<()> {
        let key = &key.terminate();
        self.snap.remove(key)?;
        Ok(())
    }

    /// Retrieves the value and timestamp associated with the given key from the snapshot.
    pub fn get(&self, key: &VariableKey) -> Result<Box<dyn Value>> {
        // TODO: need to fix this to avoid cloning the key
        // This happens because the VariableKey transfrom from
        // a &[u8] does not terminate the key with a null byte.
        let key = &key.terminate();
        self.get_with_filters(key, &FILTERS)
    }

    pub fn get_with_filters<F>(&self, key: &VariableKey, filters: &[F]) -> Result<Box<dyn Value>>
    where
        F: FilterFn,
    {
        let (val, version, _) = self.snap.get(key)?;
        let mut val_ref = ValueRef::new(self.store.clone());
        let val_bytes_ref: &Bytes = &val;
        val_ref.decode(version, val_bytes_ref)?;

        for filter in filters {
            filter.apply(&val_ref, self.ts)?
        }

        Ok(Box::new(val_ref))
    }

    pub fn new_reader(&mut self) -> Result<IterationPointer<VariableKey, Bytes>> {
        Ok(self.snap.new_reader()?)
    }

    pub async fn close(&mut self) -> Result<()> {
        let mut indexer = self.store.indexer.write().await;
        indexer.close_snapshot(self.snap.id)?;
        Ok(())
    }
}

// impl Drop for Snapshot {
//     fn drop(&mut self) {
//         let err = self.close();
//         if err.is_err() {
//             panic!("failed to close snapshot: {:?}", err);
//         }
//     }
// }

pub(crate) trait FilterFn {
    fn apply(&self, val_ref: &ValueRef, ts: u64) -> Result<()>;
}

fn ignore_deleted(val_ref: &ValueRef, _: u64) -> Result<()> {
    let md = val_ref.key_value_metadata();
    if let Some(md) = md {
        if md.deleted() {
            return Err(Error::IndexError(TrieError::KeyNotFound));
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
