use std::sync::Arc;

use bytes::Bytes;

use super::entry::ValueRef;
use crate::storage::index::snapshot::Snapshot as TartSnapshot;
use crate::storage::index::KeyTrait;
use crate::storage::kv::error::{Error, Result};
use crate::storage::kv::store::MVCCStore;

/// A versioned snapshot for snapshot isolation.
pub(crate) struct Snapshot<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> {
    /// The timestamp of the snapshot. This is used to determine the visibility of the
    /// key-value pairs in the snapshot. It can be used to filter out expired key-value
    /// pairs or to filter out key-value pairs based on the snapshot timestamp.
    ts: u64,
    snap: TartSnapshot<P, V>,
    store: Arc<MVCCStore<P, V>>,
}

impl<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> Snapshot<P, V> {
    pub(crate) fn take(store: Arc<MVCCStore<P, V>>, ts: u64) -> Result<Self> {
        let mut index = store.index.write()?;
        let snapshot = index.create_snapshot()?;
        std::mem::drop(index);

        Ok(Self {
            ts,
            snap: snapshot,
            store,
        })
    }

    /// Set a key-value pair into the snapshot.
    pub fn set(&mut self, key: &P, value: V) -> Result<()> {
        println!("key: {:?} {}", key, self.ts);
        self.snap.insert(key, value, self.ts)?;
        Ok(())
    }

    /// Retrieves the value and timestamp associated with the given key from the snapshot.
    pub fn get(&self, key: &P) -> Result<ValueRef<P, V>> {
        // Create a slice with your filter function if needed, e.g., [ignore_deleted]
        let filters: Vec<fn(&ValueRef<P, V>, u64) -> Result<()>> = vec![ignore_deleted];

        self.get_with_filters(key, &filters)
    }

    pub fn get_with_filters<F>(&self, key: &P, filters: &[F]) -> Result<ValueRef<P, V>>
    where
        F: FilterFn<P, V>,
    {
        let (val, version, _) = self.snap.get(key, self.ts)?;
        let mut val_ref = ValueRef::new(self.store.clone());
        let val_bytes_ref: &Bytes = val.as_ref();
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

pub(crate) trait FilterFn<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>> {
    fn apply(&self, val_ref: &ValueRef<P, V>, ts: u64) -> Result<()>;
}

fn ignore_deleted<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>>(
    val_ref: &ValueRef<P, V>,
    _: u64,
) -> Result<()> {
    let md = val_ref.key_value_metadata();
    if let Some(md) = md {
        if md.deleted() {
            return Err(Error::KeyNotFound);
        }
    }
    Ok(())
}

impl<P: KeyTrait, V: Clone + AsRef<Bytes> + From<bytes::Bytes>, F> FilterFn<P, V> for F
where
    F: Fn(&ValueRef<P, V>, u64) -> Result<()>,
{
    fn apply(&self, val_ref: &ValueRef<P, V>, ts: u64) -> Result<()> {
        self(val_ref, ts)
    }
}
