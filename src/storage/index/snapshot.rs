//! This module defines the Snapshot struct for managing snapshots within a Trie structure.
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use hashbrown::HashSet;

use crate::storage::index::art::{Node, TrieError};
use crate::storage::index::iter::IterationPointer;
use crate::storage::index::node::Version;
use crate::storage::index::KeyTrait;

/// Represents a snapshot of the data within the Trie.
pub struct Snapshot<P: KeyTrait, V: Clone> {
    pub(crate) id: u64,
    pub(crate) ts: u64,
    pub(crate) root: Option<Arc<Node<P, V>>>,
    pub(crate) readers: HashSet<u64>,
    pub(crate) max_active_readers: AtomicU64,
    pub(crate) closed: bool,
}

impl<P: KeyTrait, V: Clone> Snapshot<P, V> {
    /// Creates a new Snapshot instance with the provided snapshot_id and root node.
    pub(crate) fn new(id: u64, root: Option<Arc<Node<P, V>>>, ts: u64) -> Self {
        Snapshot {
            id,
            ts,
            root,
            readers: HashSet::new(),
            max_active_readers: AtomicU64::new(0),
            closed: false,
        }
    }

    /// Inserts a key-value pair into the snapshot.
    pub fn insert(&mut self, key: &P, value: V, ts: u64) -> Result<(), TrieError> {
        // Check if the snapshot is already closed
        self.is_closed()?;

        // Insert the key-value pair into the root node using a recursive function
        match &self.root {
            Some(root) => {
                let (new_node, _) = match Node::insert_recurse(root, key, value, self.ts, ts, 0) {
                    Ok((new_node, old_node)) => (new_node, old_node),
                    Err(err) => {
                        return Err(err);
                    }
                };

                // Update the root node with the new node after insertion
                self.root = Some(new_node);
            }
            None => {
                self.root = Some(Arc::new(Node::new_twig(
                    key.as_slice().into(),
                    key.as_slice().into(),
                    value,
                    self.ts,
                    ts,
                )))
            }
        };

        Ok(())
    }

    /// Retrieves the value and timestamp associated with the given key from the snapshot.
    pub fn get(&self, key: &P) -> Result<(V, u64, u64), TrieError> {
        // Check if the snapshot is already closed
        self.is_closed()?;

        // Use a recursive function to get the value and timestamp from the root node
        match self.root.as_ref() {
            Some(root) => Node::get_recurse(root, key, root.version())
                .map(|(_, value, version, ts)| (value, version, ts)),
            None => Err(TrieError::KeyNotFound),
        }
    }

    /// Returns the version of the snapshot.
    pub fn version(&self) -> u64 {
        self.root.as_ref().map_or(0, |root| root.version())
    }

    fn is_closed(&self) -> Result<(), TrieError> {
        if self.closed {
            return Err(TrieError::SnapshotAlreadyClosed);
        }
        Ok(())
    }

    /// Closes the snapshot, preventing further modifications, and releases associated resources.
    pub fn close(&mut self) -> Result<(), TrieError> {
        // Check if the snapshot is already closed
        self.is_closed()?;

        // Check if there are any active readers for the snapshot
        if self.max_active_readers.load(Ordering::SeqCst) > 0 {
            return Err(TrieError::SnapshotReadersNotClosed);
        }
        // Mark the snapshot as closed
        self.closed = true;

        Ok(())
    }

    pub fn new_reader(&mut self) -> Result<IterationPointer<P, V>, TrieError> {
        // Check if the snapshot is already closed
        self.is_closed()?;

        if self.root.is_none() {
            return Err(TrieError::SnapshotEmpty);
        }

        let reader_id = self.max_active_readers.fetch_add(1, Ordering::SeqCst) + 1;
        self.readers.insert(reader_id);
        Ok(IterationPointer::new(
            self.root.as_ref().unwrap().clone(),
            reader_id,
        ))
    }

    pub fn active_readers(&self) -> Result<u64, TrieError> {
        // Check if the snapshot is already closed
        self.is_closed()?;

        Ok(self.max_active_readers.load(Ordering::SeqCst))
    }

    pub fn close_reader(&mut self, reader_id: u64) -> Result<(), TrieError> {
        // Check if the snapshot is already closed
        self.is_closed()?;

        self.readers.remove(&reader_id);
        let _readers = self.max_active_readers.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    pub fn remove(&mut self, key: &P) -> Result<bool, TrieError> {
        // Check if the tree is already closed
        self.is_closed()?;

        let (new_root, is_deleted) = match &self.root {
            None => (None, false),
            Some(root) => {
                if root.is_twig() {
                    (None, true)
                } else {
                    let (new_root, removed) = Node::remove_recurse(root, key, 0);
                    if removed {
                        (new_root, true)
                    } else {
                        (self.root.clone(), true)
                    }
                }
            }
        };

        self.root = new_root;
        Ok(is_deleted)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::index::art::Tree;
    use crate::storage::index::iter::IterationPointer;
    use crate::storage::index::VariableKey;
    use std::str::FromStr;

    #[test]
    fn snapshot_creation() {
        let mut tree: Tree<VariableKey, i32> = Tree::<VariableKey, i32>::new();
        let keys = ["key_1", "key_2", "key_3"];

        for key in keys.iter() {
            assert!(tree
                .insert(&VariableKey::from_str(key).unwrap(), 1, 0, 0)
                .is_ok());
        }

        let mut snap1 = tree.create_snapshot().unwrap();
        let key_to_insert = "key_1";
        assert!(snap1
            .insert(&VariableKey::from_str(key_to_insert).unwrap(), 1, 0)
            .is_ok());

        let expected_snap_ts = keys.len() as u64 + 1;
        assert_eq!(snap1.version(), expected_snap_ts);
        assert_eq!(tree.snapshot_count(), 1);

        let expected_tree_ts = keys.len() as u64;
        assert_eq!(tree.version(), expected_tree_ts);
    }

    #[test]
    fn snapshot_isolation() {
        let mut tree: Tree<VariableKey, i32> = Tree::<VariableKey, i32>::new();
        let key_1 = VariableKey::from_str("key_1").unwrap();
        let key_2 = VariableKey::from_str("key_2").unwrap();
        let key_3_snap1 = VariableKey::from_str("key_3_snap1").unwrap();
        let key_3_snap2 = VariableKey::from_str("key_3_snap2").unwrap();

        assert!(tree.insert(&key_1, 1, 0, 0).is_ok());

        // Keys inserted before snapshot creation should be visible
        let mut snap1 = tree.create_snapshot().unwrap();
        assert_eq!(snap1.id, 0);
        assert_eq!(snap1.get(&key_1).unwrap(), (1, 1, 0));

        let mut snap2 = tree.create_snapshot().unwrap();
        assert_eq!(snap2.id, 1);
        assert_eq!(snap2.get(&key_1).unwrap(), (1, 1, 0));

        assert_eq!(tree.snapshot_count(), 2);

        // Keys inserted after snapshot creation should not be visible to other snapshots
        assert!(tree.insert(&key_2, 1, 0, 0).is_ok());
        assert!(snap1.get(&key_2).is_err());
        assert!(snap2.get(&key_2).is_err());

        // Keys inserted after snapshot creation should be visible to the snapshot that inserted them
        assert!(snap1.insert(&key_3_snap1, 2, 0).is_ok());
        assert_eq!(snap1.get(&key_3_snap1).unwrap(), (2, 2, 0));

        assert!(snap2.insert(&key_3_snap2, 3, 0).is_ok());
        assert_eq!(snap2.get(&key_3_snap2).unwrap(), (3, 2, 0));

        // Keys inserted after snapshot creation should not be visible to other snapshots
        assert!(snap1.get(&key_3_snap2).is_err());
        assert!(snap2.get(&key_3_snap1).is_err());

        assert!(snap1.close().is_ok());
        assert!(snap2.close().is_ok());

        assert!(tree.close_snapshot(snap1.id).is_ok());
        assert!(tree.close_snapshot(snap2.id).is_ok());

        assert_eq!(tree.snapshot_count(), 0);
    }

    #[test]
    fn snapshot_readers() {
        let mut tree: Tree<VariableKey, i32> = Tree::<VariableKey, i32>::new();
        let key_1 = VariableKey::from_str("key_1").unwrap();
        let key_2 = VariableKey::from_str("key_2").unwrap();
        let key_3 = VariableKey::from_str("key_3").unwrap();
        let key_4 = VariableKey::from_str("key_4").unwrap();

        assert!(tree.insert(&key_1, 1, 0, 0).is_ok());
        assert!(tree.insert(&key_2, 1, 0, 0).is_ok());
        assert!(tree.insert(&key_3, 1, 0, 0).is_ok());

        let mut snap = tree.create_snapshot().unwrap();
        assert!(snap.insert(&key_4, 1, 0).is_ok());

        // Reader 1
        let reader1 = snap.new_reader().unwrap();
        let reader1_id = reader1.id;
        assert_eq!(count_items(&reader1), 4);
        assert_eq!(reader1_id, 1);

        // Reader 2
        let reader2 = snap.new_reader().unwrap();
        let reader2_id = reader2.id;
        assert_eq!(count_items(&reader2), 4);
        assert_eq!(reader2_id, 2);

        // Active readers
        assert_eq!(snap.active_readers().unwrap(), 2);
        assert!(snap.close().is_err());

        // Close readers
        assert!(snap.close_reader(reader1_id).is_ok());
        assert!(snap.close_reader(reader2_id).is_ok());
        assert!(snap.close().is_ok());
    }

    fn count_items(reader: &IterationPointer<VariableKey, i32>) -> usize {
        let mut len = 0;
        for _ in reader.iter() {
            len += 1;
        }
        len
    }
}
