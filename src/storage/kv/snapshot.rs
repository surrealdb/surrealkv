use std::{ops::RangeBounds, sync::Arc};

use bytes::Bytes;

use super::entry::{Value, ValueRef};
use crate::storage::{
    kv::error::{Error, Result},
    kv::store::Core,
};

use vart::{art::QueryType, iter::Iter, snapshot::Snapshot as VartSnapshot, VariableSizeKey};

pub(crate) const FILTERS: [fn(&ValueRef) -> Result<()>; 1] = [ignore_deleted];

/// A versioned snapshot for snapshot isolation.
pub struct Snapshot {
    snap: VartSnapshot<VariableSizeKey, Bytes>,
    store: Arc<Core>,
}

impl Snapshot {
    pub(crate) fn take(store: Arc<Core>) -> Result<Self> {
        let snapshot = store.indexer.write().snapshot();

        Ok(Self {
            snap: snapshot,
            store,
        })
    }

    /// Set a key-value pair into the snapshot.
    pub fn set(&mut self, key: &VariableSizeKey, value: Bytes) {
        // TODO: need to fix this to avoid cloning the key
        // This happens because the VariableSizeKey transfrom from
        // a &[u8] does not terminate the key with a null byte.
        let key = &key.terminate();
        self.snap.insert(key, value, self.snap.version());
    }

    #[allow(unused)]
    pub fn delete(&mut self, key: &VariableSizeKey) -> bool {
        // TODO: need to fix this to avoid cloning the key
        // This happens because the VariableSizeKey transfrom from
        // a &[u8] does not terminate the key with a null byte.
        let key = &key.terminate();
        self.snap.remove(key)
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
            filter.apply(&val_ref)?;
        }

        Ok(Box::new(val_ref))
    }

    /// Retrieves the latest value associated with the given key from the snapshot.
    pub fn get(&self, key: &VariableSizeKey) -> Result<Box<dyn Value>> {
        // TODO: need to fix this to avoid cloning the key
        // This happens because the VariableSizeKey transfrom from
        // a &[u8] does not terminate the key with a null byte.
        let key = &key.terminate();
        let (val, version, _) = self.snap.get(key).ok_or(Error::KeyNotFound)?;
        self.decode_and_apply_filters(&val, version, &FILTERS)
    }

    /// Retrieves the value associated with the given key at the given timestamp from the snapshot.
    pub fn get_at_ts(&self, key: &VariableSizeKey, ts: u64) -> Result<Box<dyn Value>> {
        let key = &key.terminate();
        let (val, version) = self.snap.get_at_ts(key, ts).ok_or(Error::KeyNotFound)?;
        self.decode_and_apply_filters(&val, version, &FILTERS)
    }

    /// Retrieves the version history of the value associated with the given key from the snapshot.
    pub fn get_version_history(&self, key: &VariableSizeKey) -> Result<Vec<(Box<dyn Value>, u64)>> {
        let key = &key.terminate();

        let mut results = Vec::new();

        let items = self
            .snap
            .get_version_history(key)
            .ok_or(Error::KeyNotFound)?;
        for (value, version, ts) in items {
            let result = self.decode_and_apply_filters(&value, version, &FILTERS)?;
            results.push((result, ts));
        }

        Ok(results)
    }

    /// Retrieves an iterator over the key-value pairs in the snapshot.
    pub fn iter(&self) -> Iter<VariableSizeKey, Bytes> {
        self.snap.iter()
    }

    /// Returns a range query iterator over the Trie.
    pub fn range<'a, R>(
        &'a self,
        range: R,
    ) -> impl Iterator<Item = (Vec<u8>, &'a Bytes, &'a u64, &'a u64)>
    where
        R: RangeBounds<VariableSizeKey> + 'a,
    {
        self.snap.range(range)
    }

    /// Returns a versioned range query iterator over the Trie.
    pub fn range_with_versions<'a, R>(
        &'a self,
        range: R,
    ) -> impl Iterator<Item = (Vec<u8>, &'a Bytes, &'a u64, &'a u64)>
    where
        R: RangeBounds<VariableSizeKey> + 'a,
    {
        self.snap.range_with_versions(range)
    }

    pub fn get_value_by_query(
        &self,
        key: &VariableSizeKey,
        query_type: QueryType,
    ) -> Result<(Bytes, u64, u64)> {
        self.snap
            .get_value_by_query(key, query_type)
            .ok_or(Error::KeyNotFound)
    }

    pub fn scan_at_ts<R>(&self, range: R, ts: u64) -> Vec<(Vec<u8>, Bytes)>
    where
        R: RangeBounds<VariableSizeKey>,
    {
        self.snap.scan_at_ts(range, ts)
    }

    pub fn keys_at_ts<R>(&self, range: R, ts: u64) -> Vec<Vec<u8>>
    where
        R: RangeBounds<VariableSizeKey>,
    {
        self.snap.keys_at_ts(range, ts)
    }
}

pub(crate) trait FilterFn {
    fn apply(&self, val_ref: &ValueRef) -> Result<()>;
}

fn ignore_deleted(val_ref: &ValueRef) -> Result<()> {
    let md = val_ref.metadata();
    if let Some(md) = md {
        if md.is_deleted_or_tombstone() {
            return Err(Error::KeyNotFound);
        }
    }
    Ok(())
}

impl<F> FilterFn for F
where
    F: Fn(&ValueRef) -> Result<()>,
{
    fn apply(&self, val_ref: &ValueRef) -> Result<()> {
        self(val_ref)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::kv::option::Options;
    use crate::storage::kv::store::Store;
    use crate::storage::kv::util::{convert_range_bounds, now};
    use crate::Mode;

    use bytes::Bytes;
    use tempdir::TempDir;

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    async fn set_value(store: &Store, key: &Bytes, value: &Bytes) {
        let mut txn = store.begin().expect("Failed to begin transaction");
        txn.set(key, value).expect("Failed to set value");
        txn.commit().await.expect("Failed to commit transaction");
    }

    #[tokio::test]
    async fn test_versioned_apis() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        let store = Store::new(opts).expect("Failed to create store");

        // Define multiple keys and their versioned values
        let keys_values = [
            (
                Bytes::from("k1"),
                Bytes::from("value1"),
                Bytes::from("value1Updated"),
            ),
            (
                Bytes::from("k2"),
                Bytes::from("value2"),
                Bytes::from("value2Updated"),
            ),
        ];

        // Set and update values for all keys
        for (key, initial_value, updated_value) in keys_values.iter() {
            set_value(&store, key, initial_value).await;
            set_value(&store, key, updated_value).await;
        }

        // Test keys_at_ts
        let ts = now();
        let txn = store
            .begin_with_mode(Mode::ReadOnly)
            .expect("Failed to begin transaction");

        let range = "k1".as_bytes()..="k2".as_bytes();
        let keys = txn
            .keys_at_ts(range.clone(), ts)
            .expect("Failed to get keys at timestamp");
        assert_eq!(keys[0], b"k1");
        assert_eq!(keys[1], b"k2");

        // Test scan_at_ts
        let entries = txn
            .scan_at_ts(range, ts, Some(10))
            .expect("Failed to scan at timestamp");
        assert_eq!(
            entries.len(),
            keys_values.len(),
            "Should match the number of keys"
        );
        assert_eq!(entries[0], (b"k1".to_vec(), b"value1Updated".to_vec()));
        assert_eq!(entries[1], (b"k2".to_vec(), b"value2Updated".to_vec()));

        // Enhance get_history testing
        for (key, initial_value, updated_value) in keys_values.iter() {
            let history = txn.get_history(key).expect("Failed to get history");
            assert_eq!(
                history.len(),
                2,
                "History should contain two entries for each key"
            );
            assert_eq!(
                history[0].0, *initial_value,
                "First entry should match initial value"
            );
            assert_eq!(
                history[1].0, *updated_value,
                "Second entry should match updated value"
            );

            let initial_ts = history[0].1;
            let updated_ts = history[1].1;
            assert_eq!(
                txn.get_at_ts(key, initial_ts)
                    .expect("Failed to get value at initial timestamp"),
                Some(initial_value.to_vec())
            );
            assert_eq!(
                txn.get_at_ts(key, updated_ts)
                    .expect("Failed to get value at updated timestamp"),
                Some(updated_value.to_vec())
            );
        }
    }

    #[tokio::test]
    async fn test_versioned_apis_with_snapshot() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        let store = Store::new(opts).expect("Failed to create store");

        // Define multiple keys and their versioned values
        let keys_values = [
            (
                Bytes::from("k1"),
                Bytes::from("value1"),
                Bytes::from("value1Updated"),
            ),
            (
                Bytes::from("k2"),
                Bytes::from("value2"),
                Bytes::from("value2Updated"),
            ),
        ];

        // Set and update values for all keys
        for (key, initial_value, updated_value) in keys_values.iter() {
            set_value(&store, key, initial_value).await;
            set_value(&store, key, updated_value).await;
        }

        // Test keys_at_ts
        let ts = now();
        let snap = store.get_snapshot().expect("Failed to get snapshot");

        let range = "k1".as_bytes()..="k2".as_bytes();
        let range = convert_range_bounds(range);
        let keys = snap.keys_at_ts(range.clone(), ts);
        assert!(keys.contains(&Bytes::from("k1\0").to_vec()));
        assert!(keys.contains(&Bytes::from("k2\0").to_vec()));

        // Test scan_at_ts
        let entries = snap.scan_at_ts(range, ts);
        assert_eq!(
            entries.len(),
            keys_values.len(),
            "Should match the number of keys"
        );
    }
}
