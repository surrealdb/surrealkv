use std::sync::Arc;

use bytes::Bytes;

use super::entry::ValueRef;
use crate::storage::index::art::TrieError;
use crate::storage::index::snapshot::Snapshot as TartSnapshot;
use crate::storage::index::KeyTrait;
use crate::storage::kv::error::{Error, Result};
use crate::storage::kv::store::Core;

/// A versioned snapshot for snapshot isolation.
pub(crate) struct Snapshot<P: KeyTrait> {
    /// The timestamp of the snapshot. This is used to determine the visibility of the
    /// key-value pairs in the snapshot. It can be used to filter out expired key-value
    /// pairs or to filter out key-value pairs based on the snapshot timestamp.
    ts: u64,
    snap: TartSnapshot<P, Bytes>,
    store: Arc<Core<P>>,
}

impl<P: KeyTrait> Snapshot<P> {
    pub(crate) fn take(store: Arc<Core<P>>, ts: u64) -> Result<Self> {
        let snapshot = store.indexer.write()?.snapshot()?;

        Ok(Self {
            ts,
            snap: snapshot,
            store,
        })
    }

    /// Set a key-value pair into the snapshot.
    pub fn set(&mut self, key: &P, value: Bytes) -> Result<()> {
        self.snap.insert(key, value, self.ts)?;
        Ok(())
    }

    /// Retrieves the value and timestamp associated with the given key from the snapshot.
    pub fn get(&self, key: &P) -> Result<ValueRef<P>> {
        // Create a slice with your filter function if needed, e.g., [ignore_deleted]
        let filters: Vec<fn(&ValueRef<P>, u64) -> Result<()>> = vec![ignore_deleted];

        self.get_with_filters(key, &filters)
    }

    pub fn get_with_filters<F>(&self, key: &P, filters: &[F]) -> Result<ValueRef<P>>
    where
        F: FilterFn<P>,
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
        self.snap.close()?;
        Ok(())
    }
}

pub(crate) trait FilterFn<P: KeyTrait> {
    fn apply(&self, val_ref: &ValueRef<P>, ts: u64) -> Result<()>;
}

fn ignore_deleted<P: KeyTrait>(val_ref: &ValueRef<P>, _: u64) -> Result<()> {
    let md = val_ref.key_value_metadata();
    if let Some(md) = md {
        if md.deleted() {
            return Err(Error::Index(TrieError::KeyNotFound));
        }
    }
    Ok(())
}

impl<P: KeyTrait, F> FilterFn<P> for F
where
    F: Fn(&ValueRef<P>, u64) -> Result<()>,
{
    fn apply(&self, val_ref: &ValueRef<P>, ts: u64) -> Result<()> {
        self(val_ref, ts)
    }
}
