use std::sync::Arc;

use bytes::Bytes;

use super::entry::{Value, ValueRef};
use crate::storage::{
    kv::error::{Error, Result},
    kv::store::Core,
};

use vart::{
    iter::IterationPointer, snapshot::Snapshot as TartSnapshot, TrieError, VariableSizeKey,
};

pub(crate) const FILTERS: [fn(&ValueRef, u64) -> Result<()>; 1] = [ignore_deleted];

/// A versioned snapshot for snapshot isolation.
pub(crate) struct Snapshot {
    /// The timestamp of the snapshot. This is used to determine the visibility of the
    /// key-value pairs in the snapshot. It can be used to filter out expired key-value
    /// pairs or deleted key-value pairs based on the read timestamp.
    ts: u64,
    snap: TartSnapshot<VariableSizeKey, Bytes>,
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
    pub fn set(&mut self, key: &VariableSizeKey, value: Bytes) -> Result<()> {
        // TODO: need to fix this to avoid cloning the key
        // This happens because the VariableSizeKey transfrom from
        // a &[u8] does not terminate the key with a null byte.
        let key = &key.terminate();
        self.snap.insert(key, value, self.snap.ts())?;
        Ok(())
    }

    /// Retrieves the value and timestamp associated with the given key from the snapshot.
    pub fn get(&self, key: &VariableSizeKey) -> Result<Box<dyn Value>> {
        // TODO: need to fix this to avoid cloning the key
        // This happens because the VariableSizeKey transfrom from
        // a &[u8] does not terminate the key with a null byte.
        let key = &key.terminate();
        self.get_with_filters(key, &FILTERS)
    }

    pub fn get_with_filters<F>(
        &self,
        key: &VariableSizeKey,
        filters: &[F],
    ) -> Result<Box<dyn Value>>
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

    pub fn new_reader(&mut self) -> Result<IterationPointer<VariableSizeKey, Bytes>> {
        Ok(self.snap.new_reader()?)
    }
}

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
