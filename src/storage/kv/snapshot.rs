use std::ops::RangeBounds;

use crate::storage::{
    kv::error::{Error, Result},
    kv::indexer::IndexValue,
    kv::store::Core,
    kv::util::now,
};

use vart::{art::QueryType, art::Tree, iter::Iter, VariableSizeKey};

/// A versioned snapshot for snapshot isolation.
pub(crate) struct Snapshot {
    snap: Tree<VariableSizeKey, IndexValue>,
    pub(crate) version: u64,
}

impl Snapshot {
    pub(crate) fn take(store: &Core) -> Result<Self> {
        // Acquire a read lock on the indexer to get the current index version.
        let index = store.indexer.read();
        let index_version = index.version();

        // Calculate the snapshot version as one greater than the current read timestamp.
        let read_version = store.read_ts() + 1;

        // Ensure that the snapshot read version is not older than the current index version.
        if read_version < index_version {
            return Err(Error::SnapshotVersionIsOld(read_version, index_version));
        }

        // Clone the current index to create the snapshot.
        let snap = index.index.clone();

        // Return the new snapshot with the calculated version.
        Ok(Self {
            snap,
            version: read_version,
        })
    }

    /// Set a key-value pair into the snapshot.
    pub(crate) fn set(&mut self, key: &VariableSizeKey, value: IndexValue) {
        self.snap
            .insert(key, value, self.version, now())
            .expect("incorrect snapshot version");
    }

    pub(crate) fn delete(&mut self, key: &VariableSizeKey) -> bool {
        self.snap.remove(key)
    }

    /// Retrieves the latest value associated with the given key from the snapshot.
    pub(crate) fn get(&self, key: &VariableSizeKey) -> Option<(IndexValue, u64)> {
        self.snap
            .get(key, self.version)
            .filter(|(val, _, _)| !val.deleted())
            .map(|(val, version, _)| (val, version))
    }

    /// Retrieves the value associated with the given key at the given timestamp from the snapshot.
    pub(crate) fn get_at_ts(&self, key: &VariableSizeKey, ts: u64) -> Option<IndexValue> {
        self.snap
            .get_at_ts(key, ts)
            .filter(|(val, _, _)| !val.deleted())
            .map(|(val, _, _)| val)
    }

    /// Retrieves the version history of the value associated with the given key from the snapshot.
    pub(crate) fn get_version_history(
        &self,
        key: &VariableSizeKey,
    ) -> Option<Vec<(IndexValue, u64)>> {
        let items = self.snap.get_version_history(key)?;

        let result = items
            .into_iter()
            .filter(|(val, _, _)| !val.deleted())
            .map(|(value, _, ts)| (value, ts))
            .collect();

        Some(result)
    }

    /// Retrieves an iterator over the key-value pairs in the snapshot.
    #[allow(unused)]
    pub(crate) fn iter(&self) -> Iter<VariableSizeKey, IndexValue> {
        self.snap.iter()
    }

    /// Returns a range query iterator over the Trie without deleted keys.
    pub(crate) fn range<'a, R>(
        &'a self,
        range: R,
    ) -> impl Iterator<Item = (Vec<u8>, &'a IndexValue, &'a u64, &'a u64)>
    where
        R: RangeBounds<VariableSizeKey> + 'a,
    {
        self.snap
            .range(range)
            .filter(|(_, snap_val, _, _)| !snap_val.deleted())
    }

    /// Returns a range query iterator over the Trie including deleted keys.
    pub(crate) fn range_with_deleted<'a, R>(
        &'a self,
        range: R,
    ) -> impl Iterator<Item = (Vec<u8>, &'a IndexValue, &'a u64, &'a u64)>
    where
        R: RangeBounds<VariableSizeKey> + 'a,
    {
        self.snap.range(range)
    }

    /// Returns a versioned range query iterator over the Trie.
    pub(crate) fn range_with_versions<'a, R>(
        &'a self,
        range: R,
    ) -> impl Iterator<Item = (Vec<u8>, &'a IndexValue, &'a u64, &'a u64)>
    where
        R: RangeBounds<VariableSizeKey> + 'a,
    {
        self.snap.range_with_versions(range)
    }

    pub(crate) fn get_value_by_query(
        &self,
        key: &VariableSizeKey,
        query_type: QueryType,
    ) -> Option<(IndexValue, u64, u64)> {
        self.snap
            .get_value_by_query(key, query_type)
            .filter(|(val, _, _)| !val.deleted())
    }

    pub(crate) fn scan_at_ts<R>(&self, range: R, ts: u64) -> Vec<(Vec<u8>, IndexValue)>
    where
        R: RangeBounds<VariableSizeKey>,
    {
        self.snap
            .scan_at_ts(range, ts)
            .into_iter()
            .filter(|(_, snap_val)| !snap_val.deleted())
            .collect()
    }

    pub(crate) fn keys_at_ts<R>(&self, range: R, ts: u64) -> Vec<Vec<u8>>
    where
        R: RangeBounds<VariableSizeKey>,
    {
        self.snap.keys_at_ts(range, ts)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::kv::option::Options;
    use crate::storage::kv::store::Store;
    use crate::storage::kv::util::now;
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
}
