use std::ops::RangeBounds;
use vart::{art::Tree, iter::Iter, VariableSizeKey};

use crate::error::{Error, Result};
use crate::indexer::IndexValue;
use crate::store::Core;

pub(crate) type VersionedEntry<'a, V> = (&'a [u8], &'a V, u64, u64);

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

    /// Retrieves the latest value associated with the given key from the snapshot.
    pub(crate) fn get(&self, key: &VariableSizeKey) -> Option<(IndexValue, u64)> {
        self.snap
            .get(key, self.version)
            .filter(|(val, _, _)| !val.deleted())
            .map(|(val, version, _)| (val, version))
    }

    /// Retrieves the value associated with the given key at the given timestamp from the snapshot.
    pub(crate) fn get_at_ts(&self, key: &VariableSizeKey, ts: u64) -> Option<(IndexValue, u64)> {
        self.snap
            .get_at_ts(key, ts)
            .filter(|(val, _, _)| !val.deleted())
            .map(|(val, _, ts)| (val, ts))
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
    ) -> impl Iterator<Item = VersionedEntry<'a, IndexValue>>
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
    ) -> impl Iterator<Item = VersionedEntry<'a, IndexValue>>
    where
        R: RangeBounds<VariableSizeKey> + 'a,
    {
        self.snap.range(range)
    }

    /// Returns a versioned range query iterator over the Trie.
    pub(crate) fn range_with_versions<'a, R>(
        &'a self,
        range: R,
    ) -> impl Iterator<Item = VersionedEntry<'a, IndexValue>>
    where
        R: RangeBounds<VariableSizeKey> + 'a,
    {
        self.snap.range_with_versions(range)
    }

    pub(crate) fn scan_at_ts<'a, R>(
        &'a self,
        range: R,
        ts: u64,
    ) -> impl Iterator<Item = VersionedEntry<'a, IndexValue>>
    where
        R: RangeBounds<VariableSizeKey> + 'a,
    {
        self.snap
            .scan_at_ts(range, ts)
            .filter(|(_, snap_val, _, _)| !snap_val.deleted())
    }
}

#[cfg(test)]
mod tests {
    use crate::option::Options;
    use crate::store::Store;
    use crate::util::now;
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
        let mut txn = store
            .begin_with_mode(Mode::ReadOnly)
            .expect("Failed to begin transaction");

        let range = "k1".as_bytes()..="k2".as_bytes();
        let keys: Vec<_> = txn.keys_at_ts(range.clone(), ts).collect();
        assert_eq!(keys[0], b"k1");
        assert_eq!(keys[1], b"k2");

        // Test scan_at_ts
        let entries: Vec<_> = txn
            .scan_at_ts(range, ts, Some(10))
            .collect::<Result<Vec<_>, _>>()
            .expect("Scan should succeed");

        assert_eq!(
            entries.len(),
            keys_values.len(),
            "Should match the number of keys"
        );
        assert_eq!(entries[0], (b"k1".as_ref(), b"value1Updated".to_vec()));
        assert_eq!(entries[1], (b"k2".as_ref(), b"value2Updated".to_vec()));

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
    async fn test_get_history_with_write_set() {
        let temp_dir = create_temp_directory();
        let mut opts = Options::new();
        opts.dir = temp_dir.path().to_path_buf();
        let store = Store::new(opts).expect("Failed to create store");
        let key = b"test_key";

        // Test 1: Only write set value
        {
            let mut txn = store.begin().expect("Failed to begin transaction");
            txn.set(key, b"write_set_value")
                .expect("Failed to set value");

            let history = txn.get_history(key).expect("Failed to get history");
            assert_eq!(history.len(), 1, "Should have one entry from write set");
            assert_eq!(history[0].0, b"write_set_value");

            // Verify timestamp is recent (the latest entry version is 0)
            assert_eq!(history[0].1, 0);
        }

        // Test 2: Value in both write set and snapshot
        {
            // First commit a value to create snapshot
            let mut txn = store.begin().expect("Failed to begin transaction");
            txn.set(key, b"snapshot_value")
                .expect("Failed to set value");
            txn.commit().await.expect("Failed to commit");

            // Now create new transaction and set write set value
            let mut txn = store.begin().expect("Failed to begin transaction");
            txn.set(key, b"write_set_value")
                .expect("Failed to set value");

            let history = txn.get_history(key).expect("Failed to get history");
            assert_eq!(
                history.len(),
                2,
                "Should have entries from both write set and snapshot"
            );

            // Write set value should be more recent (first in history)
            assert_eq!(history[0].0, b"write_set_value");
            assert_eq!(history[1].0, b"snapshot_value");
        }

        // Test 3: Multiple snapshot versions plus write set
        {
            // Commit multiple versions
            let mut txn = store.begin().expect("Failed to begin transaction");
            txn.set(key, b"snapshot_value1")
                .expect("Failed to set value");
            txn.commit().await.expect("Failed to commit");

            let mut txn = store.begin().expect("Failed to begin transaction");
            txn.set(key, b"snapshot_value2")
                .expect("Failed to set value");
            txn.commit().await.expect("Failed to commit");

            // Now add write set value
            let mut txn = store.begin().expect("Failed to begin transaction");
            txn.set(key, b"write_set_value")
                .expect("Failed to set value");

            let history = txn.get_history(key).expect("Failed to get history");
            assert_eq!(
                history.len(),
                4,
                "Should have write set + 3 snapshot entries"
            );

            assert_eq!(
                history[0].0, b"write_set_value",
                "Most recent should be write set value"
            );
            assert_eq!(
                history[1].0, b"snapshot_value",
                "First should be oldest snapshot"
            );
            assert_eq!(
                history[2].0, b"snapshot_value1",
                "Second should be latest snapshot"
            );
            assert_eq!(
                history[3].0, b"snapshot_value2",
                "Third should be latest snapshot"
            );
        }

        // Test 4: Deleted value in write set
        {
            let mut txn = store.begin().expect("Failed to begin transaction");
            txn.set(key, b"initial_value").expect("Failed to set value");
            txn.commit().await.expect("Failed to commit");

            let mut txn = store.begin().expect("Failed to begin transaction");
            txn.delete(key).expect("Failed to delete key");

            let history = txn.get_history(key).expect("Failed to get history");
            assert_eq!(history.len(), 4, "Should only have snapshot values");
            assert_eq!(history[3].0, b"initial_value");
        }

        // Test 5: Empty key
        {
            let txn = store.begin().expect("Failed to begin transaction");
            assert!(txn.get_history(b"").is_err());
        }
    }
}
