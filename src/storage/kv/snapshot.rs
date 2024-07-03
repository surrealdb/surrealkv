use std::{ops::RangeBounds, sync::Arc};

use bytes::Bytes;

use super::entry::{Value, ValueRef};
use crate::storage::{
    kv::error::{Error, Result},
    kv::store::Core,
};

use vart::{
    art::QueryType,
    iter::{Iter, VersionedIter},
    snapshot::Snapshot as VartSnapshot,
    TrieError, VariableSizeKey,
};

pub(crate) const FILTERS: [fn(&ValueRef, u64) -> Result<()>; 1] = [ignore_deleted];

/// A versioned snapshot for snapshot isolation.
pub struct Snapshot {
    /// The timestamp of the snapshot. This is used to determine the visibility of the
    /// key-value pairs in the snapshot. It can be used to filter out expired key-value
    /// pairs or deleted key-value pairs based on the read timestamp.
    start_ts: u64,
    snap: VartSnapshot<VariableSizeKey, Bytes>,
    store: Arc<Core>,
}

impl Snapshot {
    pub(crate) fn take(store: Arc<Core>, start_ts: u64) -> Result<Self> {
        let snapshot = store.indexer.write().snapshot()?;

        Ok(Self {
            start_ts,
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
        self.snap.insert(key, value, self.snap.version())?;
        Ok(())
    }

    #[allow(unused)]
    pub fn delete(&mut self, key: &VariableSizeKey) -> Result<()> {
        // TODO: need to fix this to avoid cloning the key
        // This happens because the VariableSizeKey transfrom from
        // a &[u8] does not terminate the key with a null byte.
        let key = &key.terminate();
        self.snap.remove(key)?;
        Ok(())
    }

    fn decode_and_apply_filters(
        &self,
        val_bytes: &Bytes,
        version: u64,
        filters: &[impl FilterFn],
    ) -> Result<Box<dyn Value>> {
        let mut val_ref = ValueRef::new(self.store.clone());
        val_ref.decode(version, val_bytes)?;

        for filter in filters {
            filter.apply(&val_ref, self.start_ts)?;
        }

        Ok(Box::new(val_ref))
    }

    /// Retrieves the latest value associated with the given key from the snapshot.
    pub fn get(&self, key: &VariableSizeKey) -> Result<Box<dyn Value>> {
        // TODO: need to fix this to avoid cloning the key
        // This happens because the VariableSizeKey transfrom from
        // a &[u8] does not terminate the key with a null byte.
        let key = &key.terminate();
        let (val, version, _) = self.snap.get(key)?;
        self.decode_and_apply_filters(&val, version, &FILTERS)
    }

    /// Retrieves the value associated with the given key at the given timestamp from the snapshot.
    pub fn get_at_ts(&self, key: &VariableSizeKey, ts: u64) -> Result<Box<dyn Value>> {
        let key = &key.terminate();
        let (val, version) = self.snap.get_at_ts(key, ts)?;
        self.decode_and_apply_filters(&val, version, &FILTERS)
    }

    /// Retrieves the version history of the value associated with the given key from the snapshot.
    pub fn get_version_history(&self, key: &VariableSizeKey) -> Result<Vec<(Box<dyn Value>, u64)>> {
        let key = &key.terminate();

        let mut results = Vec::new();

        let items = self.snap.get_version_history(key)?;
        for (value, version, ts) in items {
            let result = self.decode_and_apply_filters(&value, version, &FILTERS)?;
            results.push((result, ts));
        }

        Ok(results)
    }

    /// Retrieves an iterator over the key-value pairs in the snapshot.
    pub fn iter(&self) -> Result<Iter<VariableSizeKey, Bytes>> {
        self.snap.iter().map_err(|e| e.into())
    }

    /// Retrieves a versioned iterator over the key-value pairs in the snapshot.
    pub fn versioned_iter(&self) -> Result<VersionedIter<VariableSizeKey, Bytes>> {
        self.snap.iter_with_versions().map_err(|e| e.into())
    }

    /// Returns a range query iterator over the Trie.
    pub fn range<'a, R>(
        &'a self,
        range: R,
    ) -> Result<impl Iterator<Item = (Vec<u8>, &'a Bytes, &'a u64, &'a u64)>>
    where
        R: RangeBounds<VariableSizeKey> + 'a,
    {
        self.snap.range(range).map_err(|e| e.into())
    }

    /// Returns a versioned range query iterator over the Trie.
    pub fn range_with_versions<'a, R>(
        &'a self,
        range: R,
    ) -> Result<impl Iterator<Item = (Vec<u8>, &'a Bytes, &'a u64, &'a u64)>>
    where
        R: RangeBounds<VariableSizeKey> + 'a,
    {
        self.snap.range_with_versions(range).map_err(|e| e.into())
    }

    pub fn get_value_by_query(
        &self,
        key: &VariableSizeKey,
        query_type: QueryType,
    ) -> Result<(Bytes, u64, u64)> {
        self.snap
            .get_value_by_query(key, query_type)
            .map_err(|e| e.into())
    }

    pub fn scan_at_ts<R>(&self, range: R, ts: u64) -> Result<Vec<(Vec<u8>, Bytes)>>
    where
        R: RangeBounds<VariableSizeKey>,
    {
        self.snap.scan_at_ts(range, ts).map_err(|e| e.into())
    }

    pub fn keys_at_ts<R>(&self, range: R, ts: u64) -> Result<Vec<Vec<u8>>>
    where
        R: RangeBounds<VariableSizeKey>,
    {
        self.snap.keys_at_ts(range, ts).map_err(|e| e.into())
    }
}

pub(crate) trait FilterFn {
    fn apply(&self, val_ref: &ValueRef, ts: u64) -> Result<()>;
}

fn ignore_deleted(val_ref: &ValueRef, _: u64) -> Result<()> {
    let md = val_ref.metadata();
    if let Some(md) = md {
        if md.is_deleted_or_tombstone() {
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

#[cfg(test)]
mod tests {
    use crate::storage::kv::option::Options;
    use crate::storage::kv::store::Store;

    use bytes::Bytes;
    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    #[tokio::test]
    async fn test_versioned_key_value_updates() {
        // Create a temporary directory for testing
        let temp_dir = create_temp_directory();

        // Setup store options with the temporary directory
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();

        // Initialize the store with VariableKey as the key type
        let store = Store::new(opts).expect("Failed to create store");

        // Define key and its versioned values
        let key = Bytes::from("testKey");
        let initial_value = Bytes::from("initialValue");
        let updated_value = Bytes::from("updatedValue");

        // Helper function to reduce repetition
        async fn set_value(store: &Store, key: &Bytes, value: &Bytes) {
            let mut txn = store.begin().expect("Failed to begin transaction");
            txn.set(key, value).expect("Failed to set value");
            txn.commit().await.expect("Failed to commit transaction");
        }

        // Set initial value
        set_value(&store, &key, &initial_value).await;

        // Update value
        set_value(&store, &key, &updated_value).await;

        // Retrieve and verify the history
        let history = store.get_history(&key).expect("Failed to get history");
        assert_eq!(history.len(), 2, "History should contain two entries");
        assert_eq!(
            history[0].0, initial_value,
            "First entry should match initial value"
        );
        assert_eq!(
            history[1].0, updated_value,
            "Second entry should match updated value"
        );

        // Verify timestamps are in increasing order
        assert!(
            history[0].1 < history[1].1,
            "Timestamps should be in increasing order"
        );

        // Verify retrieval at specific timestamps
        let initial_ts = history[0].1;
        let updated_ts = history[1].1;
        assert_eq!(
            store
                .get_at_ts(&key, initial_ts)
                .expect("Failed to get value at initial timestamp"),
            initial_value
        );
        assert_eq!(
            store
                .get_at_ts(&key, updated_ts)
                .expect("Failed to get value at updated timestamp"),
            updated_value
        );
    }
}
