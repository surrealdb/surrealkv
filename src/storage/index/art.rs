use core::panic;
use std::cmp::min;
use std::error::Error;
use std::fmt;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use hashbrown::HashSet;

use crate::storage::index::iter::{Iter, Range};
use crate::storage::index::node::{FlatNode, Node256, Node48, NodeTrait, TwigNode, Version};
use crate::storage::index::snapshot::Snapshot;
use crate::storage::index::KeyTrait;

// Minimum and maximum number of children for Node4
const NODE4MIN: usize = 2;
const NODE4MAX: usize = 4;

// Minimum and maximum number of children for Node16
const NODE16MIN: usize = NODE4MAX + 1;
const NODE16MAX: usize = 16;

// Minimum and maximum number of children for Node48
const NODE48MIN: usize = NODE16MAX + 1;
const NODE48MAX: usize = 48;

// Minimum and maximum number of children for Node256
const NODE256MIN: usize = NODE48MAX + 1;

// Maximum number of active snapshots
pub(crate) const DEFAULT_MAX_ACTIVE_SNAPSHOTS: u64 = 10000;

// Define a custom error enum representing different error cases for the Trie
#[derive(Clone, Debug)]
pub enum TrieError {
    IllegalArguments,
    NotFound,
    KeyNotFound,
    SnapshotNotFound,
    SnapshotEmpty,
    SnapshotNotClosed,
    SnapshotAlreadyClosed,
    SnapshotReadersNotClosed,
    TreeAlreadyClosed,
    Other(String),
}

impl Error for TrieError {}

// Implement the Display trait to define how the error should be formatted as a string
impl fmt::Display for TrieError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            TrieError::IllegalArguments => write!(f, "Illegal arguments"),
            TrieError::NotFound => write!(f, "Not found"),
            TrieError::KeyNotFound => write!(f, "Key not found"),
            TrieError::SnapshotNotFound => write!(f, "Snapshot not found"),
            TrieError::SnapshotNotClosed => write!(f, "Snapshot not closed"),
            TrieError::SnapshotAlreadyClosed => write!(f, "Snapshot already closed"),
            TrieError::SnapshotReadersNotClosed => {
                write!(f, "Readers in the snapshot are not closed")
            }
            TrieError::TreeAlreadyClosed => write!(f, "Tree already closed"),
            TrieError::Other(ref message) => write!(f, "Other error: {}", message),
            TrieError::SnapshotEmpty => write!(f, "Snapshot is empty"),
        }
    }
}

/// A struct representing a node in an Adaptive Radix Trie.
///
/// The `Node` struct encapsulates a single node within the adaptive radix trie structure.
/// It holds information about the type of the node, which can be one of the various node types
/// defined by the `NodeType` enum.
///
/// # Type Parameters
///
/// - `P`: A type implementing the `Prefix` trait, defining the key prefix for the node.
/// - `V`: The type of value associated with the node.
///
/// # Fields
///
/// - `node_type`: The `NodeType` variant representing the type of the node, containing its
///                specific structure and associated data.
///
pub struct Node<P: KeyTrait + Clone, V: Clone> {
    pub(crate) node_type: NodeType<P, V>, // Type of the node
}

impl<P: KeyTrait + Clone, V: Clone> Version for Node<P, V> {
    fn version(&self) -> u64 {
        match &self.node_type {
            NodeType::Twig(twig) => twig.version(),
            NodeType::Node1(n) => n.version(),
            NodeType::Node4(n) => n.version(),
            NodeType::Node16(n) => n.version(),
            NodeType::Node48(n) => n.version(),
            NodeType::Node256(n) => n.version(),
        }
    }
}

/// An enumeration representing different types of nodes in an Adaptive Radix Trie.
///
/// The `NodeType` enum encompasses various node types that can exist within the adaptive radix trie structure.
/// It includes different types of inner nodes, such as `Node4`, `Node16`, `Node48`, and `Node256`, as well as the
/// leaf nodes represented by `TwigNode`.
///
/// # Type Parameters
///
/// - `P`: A type implementing the `Prefix` trait, which is used to define the key prefix for each node.
/// - `V`: The type of value associated with each node.
///
/// # Variants
///
/// - `Twig(TwigNode<P, V>)`: Represents a Twig node, which is a leaf node in the adaptive radix trie.
/// - `Node4(FlatNode<P, Node<P, V>, 4>)`: Represents an inner node with 4 keys and 4 children.
/// - `Node16(FlatNode<P, Node<P, V>, 16>)`: Represents an inner node with 16 keys and 16 children.
/// - `Node48(Node48<P, Node<P, V>>)`: Represents an inner node with 256 keys and 48 children.
/// - `Node256(Node256<P, Node<P, V>>)`: Represents an inner node with 256 keys and 256 children.
///
pub(crate) enum NodeType<P: KeyTrait + Clone, V: Clone> {
    // Twig node of the adaptive radix trie
    Twig(TwigNode<P, V>),
    // Inner node of the adaptive radix trie
    Node1(FlatNode<P, Node<P, V>, 1>), // Node with 1 key and 1 children
    Node4(FlatNode<P, Node<P, V>, 4>), // Node with 4 keys and 4 children
    Node16(FlatNode<P, Node<P, V>, 16>), // Node with 16 keys and 16 children
    Node48(Node48<P, Node<P, V>>),     // Node with 256 keys and 48 children
    Node256(Node256<P, Node<P, V>>),   // Node with 256 keys and 256 children
}

impl<P: KeyTrait + Clone, V: Clone> Node<P, V> {
    /// Creates a new Twig node with a given prefix, key, value, and version.
    ///
    /// Constructs a new Twig node using the provided prefix, key, and value. The version
    /// indicates the time of the insertion.
    ///
    /// # Parameters
    ///
    /// - `prefix`: The common prefix for the node.
    /// - `key`: The key associated with the Twig node.
    /// - `value`: The value to be associated with the key.
    /// - `ts`: The version when the value was inserted.
    ///
    /// # Returns
    ///
    /// Returns a new `Node` instance with a Twig node containing the provided key, value, and version.
    ///
    #[inline]
    pub(crate) fn new_twig(prefix: P, key: P, value: V, version: u64, ts: u64) -> Node<P, V> {
        // Create a new TwigNode instance using the provided prefix and key.
        let mut twig = TwigNode::new(prefix, key);

        // Insert the provided value into the TwigNode along with the version.
        twig.insert_mut(value, version, ts);

        // Return a new Node instance encapsulating the constructed Twig node.
        Self {
            node_type: NodeType::Twig(twig),
        }
    }

    /// Creates a new inner Node4 node with the provided prefix.
    ///
    /// Constructs a new Node4 node using the provided prefix. Node4 is an inner node
    /// type that can store up to 4 child pointers and uses arrays for keys and pointers.
    ///
    /// # Parameters
    ///
    /// - `prefix`: The common prefix for the Node4 node.
    ///
    /// # Returns
    ///
    /// Returns a new `Node` instance with an empty Node4 node.
    ///
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn new_node4(prefix: P) -> Self {
        // Create a new FlatNode instance using the provided prefix.
        let flat_node = FlatNode::new(prefix);

        // Create a new Node4 instance with the constructed FlatNode.
        Self {
            node_type: NodeType::Node4(flat_node),
        }
    }

    /// Checks if the current node is full based on its type.
    ///
    /// Determines if the current node is full by comparing the number of children to its
    /// capacity, which varies based on the node type.
    ///
    /// # Returns
    ///
    /// Returns `true` if the node is full, `false` otherwise.
    ///
    #[inline]
    fn is_full(&self) -> bool {
        match &self.node_type {
            NodeType::Node1(n) => self.num_children() >= n.size(),
            NodeType::Node4(n) => self.num_children() >= n.size(),
            NodeType::Node16(n) => self.num_children() >= n.size(),
            NodeType::Node48(n) => self.num_children() >= n.size(),
            NodeType::Node256(n) => self.num_children() > n.size(),
            NodeType::Twig(_) => panic!("Unexpected Twig node encountered in is_full()"),
        }
    }

    /// Adds a child node with the given key to the current node.
    ///
    /// Inserts a child node with the specified key into the current node.
    /// Depending on the node type, this may lead to growth if the node becomes full.
    ///
    /// # Parameters
    ///
    /// - `key`: The key associated with the child node.
    /// - `child`: The child node to be added.
    ///
    /// # Returns
    ///
    /// Returns a new `Node` instance with the added child node.
    ///
    #[inline]
    fn add_child(&self, key: u8, child: Node<P, V>) -> Self {
        match &self.node_type {
            NodeType::Node1(n) => {
                // Add the child node to the Node1 instance.
                let node = NodeType::Node1(n.add_child(key, child));

                // Create a new Node instance with the updated NodeType.
                let mut new_node = Self { node_type: node };

                // Check if the node has become full and needs to be grown.
                if new_node.is_full() {
                    new_node.grow();
                }

                new_node
            }
            NodeType::Node4(n) => {
                // Add the child node to the Node4 instance.
                let node = NodeType::Node4(n.add_child(key, child));

                // Create a new Node instance with the updated NodeType.
                let mut new_node = Self { node_type: node };

                // Check if the node has become full and needs to be grown.
                if new_node.is_full() {
                    new_node.grow();
                }

                new_node
            }
            NodeType::Node16(n) => {
                // Add the child node to the Node16 instance.
                let node = NodeType::Node16(n.add_child(key, child));

                // Create a new Node instance with the updated NodeType.
                let mut new_node = Self { node_type: node };

                // Check if the node has become full and needs to be grown.
                if new_node.is_full() {
                    new_node.grow();
                }

                new_node
            }
            NodeType::Node48(n) => {
                // Add the child node to the Node48 instance.
                let node = NodeType::Node48(n.add_child(key, child));

                // Create a new Node instance with the updated NodeType.
                let mut new_node = Self { node_type: node };

                // Check if the node has become full and needs to be grown.
                if new_node.is_full() {
                    new_node.grow();
                }

                new_node
            }
            NodeType::Node256(n) => {
                // Add the child node to the Node256 instance.
                let node = NodeType::Node256(n.add_child(key, child));

                // Create a new Node instance with the updated NodeType.
                let mut new_node = Self { node_type: node };

                // Check if the node has become full and needs to be grown.
                if new_node.is_full() {
                    new_node.grow();
                }

                new_node
            }
            NodeType::Twig(_) => panic!("Unexpected Twig node encountered in add_child()"),
        }
    }

    /// Grows the current node to the next bigger size.
    ///
    /// Grows the current node to a larger size based on its type.
    /// This method is typically used to upgrade nodes when they become full.
    ///
    /// ArtNodes of type NODE4 will grow to NODE16
    /// ArtNodes of type NODE16 will grow to NODE48.
    /// ArtNodes of type NODE48 will grow to NODE256.
    /// ArtNodes of type NODE256 will not grow, as they are the biggest type of ArtNodes
    #[inline]
    fn grow(&mut self) {
        match &mut self.node_type {
            NodeType::Node1(n) => {
                // Grow a Node4 to a Node16 by resizing.
                let n4 = NodeType::Node4(n.resize());
                self.node_type = n4;
            }
            NodeType::Node4(n) => {
                // Grow a Node4 to a Node16 by resizing.
                let n16 = NodeType::Node16(n.resize());
                self.node_type = n16;
            }
            NodeType::Node16(n) => {
                // Grow a Node16 to a Node48 by performing growth.
                let n48 = NodeType::Node48(n.grow());
                self.node_type = n48;
            }
            NodeType::Node48(n) => {
                // Grow a Node48 to a Node256 by performing growth.
                let n256 = NodeType::Node256(n.grow());
                self.node_type = n256;
            }
            NodeType::Node256 { .. } => {
                panic!("Node256 cannot be grown further");
            }
            NodeType::Twig(_) => panic!("Unexpected Twig node encountered in grow()"),
        }
    }

    /// Recursively searches for a child node with the specified key.
    ///
    /// Searches for a child node with the given key in the current node.
    /// The search continues recursively in the child nodes based on the node type.
    ///
    /// # Parameters
    ///
    /// - `key`: The key associated with the child node.
    ///
    /// # Returns
    ///
    /// Returns an `Option` containing a reference to the found child node or `None` if not found.
    ///
    #[inline]
    fn find_child(&self, key: u8) -> Option<&Arc<Node<P, V>>> {
        // If there are no children, return None.
        if self.num_children() == 0 {
            return None;
        }

        // Match the node type to find the child using the appropriate method.
        match &self.node_type {
            NodeType::Node1(n) => n.find_child(key),
            NodeType::Node4(n) => n.find_child(key),
            NodeType::Node16(n) => n.find_child(key),
            NodeType::Node48(n) => n.find_child(key),
            NodeType::Node256(n) => n.find_child(key),
            NodeType::Twig(_) => None,
        }
    }

    /// Replaces a child node with a new node for the given key.
    ///
    /// Replaces the child node associated with the specified key with the provided new node.
    ///
    /// # Parameters
    ///
    /// - `key`: The key associated with the child node to be replaced.
    /// - `node`: The new node to replace the existing child node.
    ///
    /// # Returns
    ///
    /// Returns a new `Node` instance with the child node replaced.
    ///
    fn replace_child(&self, key: u8, node: Arc<Node<P, V>>) -> Self {
        match &self.node_type {
            NodeType::Node1(n) => {
                // Replace the child node in the Node4 instance and update the NodeType.
                let node = NodeType::Node1(n.replace_child(key, node));
                Self { node_type: node }
            }
            NodeType::Node4(n) => {
                // Replace the child node in the Node4 instance and update the NodeType.
                let node = NodeType::Node4(n.replace_child(key, node));
                Self { node_type: node }
            }
            NodeType::Node16(n) => {
                // Replace the child node in the Node16 instance and update the NodeType.
                let node = NodeType::Node16(n.replace_child(key, node));
                Self { node_type: node }
            }
            NodeType::Node48(n) => {
                // Replace the child node in the Node48 instance and update the NodeType.
                let node = NodeType::Node48(n.replace_child(key, node));
                Self { node_type: node }
            }
            NodeType::Node256(n) => {
                // Replace the child node in the Node256 instance and update the NodeType.
                let node = NodeType::Node256(n.replace_child(key, node));
                Self { node_type: node }
            }
            NodeType::Twig(_) => panic!("Unexpected Twig node encountered in replace_child()"),
        }
    }

    /// Removes a child node with the specified key from the current node.
    ///
    /// Removes a child node with the provided key from the current node.
    /// Depending on the node type, this may trigger shrinking if the number of children becomes low.
    ///
    /// # Parameters
    ///
    /// - `key`: The key associated with the child node to be removed.
    ///
    /// # Returns
    ///
    /// Returns a new `Node` instance with the child node removed.
    ///
    #[inline]
    fn delete_child(&self, key: u8) -> Self {
        match &self.node_type {
            NodeType::Node1(n) => {
                // Delete the child node from the Node1 instance and update the NodeType.
                let node = NodeType::Node1(n.delete_child(key));

                Self { node_type: node }
            }
            NodeType::Node4(n) => {
                // Delete the child node from the Node4 instance and update the NodeType.
                let node = NodeType::Node4(n.delete_child(key));
                let mut new_node = Self { node_type: node };

                // Check if the number of remaining children is below the threshold.
                if new_node.num_children() < NODE4MIN {
                    new_node.shrink();
                }

                new_node
            }
            NodeType::Node16(n) => {
                // Delete the child node from the Node16 instance and update the NodeType.
                let node = NodeType::Node16(n.delete_child(key));
                let mut new_node = Self { node_type: node };

                // Check if the number of remaining children is below the threshold.
                if new_node.num_children() < NODE16MIN {
                    new_node.shrink();
                }

                new_node
            }
            NodeType::Node48(n) => {
                // Delete the child node from the Node48 instance and update the NodeType.
                let node = NodeType::Node48(n.delete_child(key));
                let mut new_node = Self { node_type: node };

                // Check if the number of remaining children is below the threshold.
                if new_node.num_children() < NODE48MIN {
                    new_node.shrink();
                }

                new_node
            }
            NodeType::Node256(n) => {
                // Delete the child node from the Node256 instance and update the NodeType.
                let node = NodeType::Node256(n.delete_child(key));
                let mut new_node = Self { node_type: node };

                // Check if the number of remaining children is below the threshold.
                if new_node.num_children() < NODE256MIN {
                    new_node.shrink();
                }

                new_node
            }
            NodeType::Twig(_) => panic!("Unexpected Twig node encountered in delete_child()"),
        }
    }

    /// Checks if the node type is a Twig node.
    ///
    /// Determines whether the current node is a Twig node based on its node type.
    ///
    /// # Returns
    ///
    /// Returns `true` if the node type is a Twig node, otherwise returns `false`.
    ///
    #[inline]
    pub(crate) fn is_twig(&self) -> bool {
        matches!(&self.node_type, NodeType::Twig(_))
    }

    /// Checks if the node type is an inner node.
    ///
    /// Determines whether the current node is an inner node, which includes Node4, Node16, Node48, and Node256.
    /// The check is performed based on the absence of a Twig node.
    ///
    /// # Returns
    ///
    /// Returns `true` if the node type is an inner node, otherwise returns `false`.
    ///
    #[inline]
    pub(crate) fn is_inner(&self) -> bool {
        !self.is_twig()
    }

    /// Gets the prefix associated with the node.
    ///
    /// Retrieves the prefix associated with the current node based on its node type.
    ///
    /// # Returns
    ///
    /// Returns a reference to the prefix associated with the node.
    ///
    #[inline]
    pub(crate) fn prefix(&self) -> &P {
        match &self.node_type {
            NodeType::Node1(n) => &n.prefix,
            NodeType::Node4(n) => &n.prefix,
            NodeType::Node16(n) => &n.prefix,
            NodeType::Node48(n) => &n.prefix,
            NodeType::Node256(n) => &n.prefix,
            NodeType::Twig(n) => &n.prefix,
        }
    }

    /// Sets the prefix for the node.
    ///
    /// Updates the prefix associated with the current node based on its node type.
    ///
    /// # Parameters
    ///
    /// - `prefix`: The new prefix to be associated with the node.
    ///
    #[inline]
    fn set_prefix(&mut self, prefix: P) {
        match &mut self.node_type {
            NodeType::Node1(n) => n.prefix = prefix,
            NodeType::Node4(n) => n.prefix = prefix,
            NodeType::Node16(n) => n.prefix = prefix,
            NodeType::Node48(n) => n.prefix = prefix,
            NodeType::Node256(n) => n.prefix = prefix,
            NodeType::Twig(n) => n.prefix = prefix,
        }
    }

    /// Shrinks the current node to a smaller size.
    ///
    /// Shrinks the current node to a smaller size based on its type.
    /// This method is typically used to downgrade nodes when the number of children becomes low.
    ///
    /// ArtNodes of type NODE256 will shrink to NODE48
    /// ArtNodes of type NODE48 will shrink to NODE16.
    /// ArtNodes of type NODE16 will shrink to NODE4.
    /// ArtNodes of type NODE4 will collapse into its first child.
    ///
    /// If that child is not a twig, it will concatenate its current prefix with that of its childs
    /// before replacing itself.
    fn shrink(&mut self) {
        match &mut self.node_type {
            NodeType::Node1(n) => {
                // Shrink Node1 to Node1 by resizing it.
                self.node_type = NodeType::Node1(n.resize());
            }
            NodeType::Node4(n) => {
                // Shrink Node4 to Node1 by resizing it.
                self.node_type = NodeType::Node1(n.resize());
            }
            NodeType::Node16(n) => {
                // Shrink Node16 to Node4 by resizing it.
                self.node_type = NodeType::Node4(n.resize());
            }
            NodeType::Node48(n) => {
                // Shrink Node48 to Node16 by obtaining the shrunken Node16 instance.
                let n16 = n.shrink();

                // Update the node type to Node16 after the shrinking operation.
                let new_node = NodeType::Node16(n16);
                self.node_type = new_node;
            }
            NodeType::Node256(n) => {
                // Shrink Node256 to Node48 by obtaining the shrunken Node48 instance.
                let n48 = n.shrink();

                // Update the node type to Node48 after the shrinking operation.
                self.node_type = NodeType::Node48(n48);
            }
            NodeType::Twig(_) => panic!("Twig node encountered in shrink()"),
        }
    }

    #[inline]
    pub fn num_children(&self) -> usize {
        match &self.node_type {
            NodeType::Node1(n) => n.num_children(),
            NodeType::Node4(n) => n.num_children(),
            NodeType::Node16(n) => n.num_children(),
            NodeType::Node48(n) => n.num_children(),
            NodeType::Node256(n) => n.num_children(),
            NodeType::Twig(_) => 0,
        }
    }

    /// TODO: fix having separate key and prefix traits to avoid copying
    /// Retrieves a value from a Twig node by the specified version.
    ///
    /// Retrieves a value from the current Twig node by matching the provided version.
    ///
    /// # Parameters
    ///
    /// - `ts`: The version for which to retrieve the value.
    ///
    /// # Returns
    ///
    /// Returns `Some((key, value, version))` if a matching value is found in the Twig node for the given version.
    /// If no matching value is found, returns `None`.
    ///
    #[inline]
    pub fn get_value_by_version(&self, version: u64) -> Option<(P, V, u64, u64)> {
        // Unwrap the NodeType::Twig to access the TwigNode instance.
        let NodeType::Twig(twig) = &self.node_type else {
            return None;
        };

        // Get the value from the TwigNode instance by the specified version.
        let Some(val) = twig.get_leaf_by_version(version) else {
            return None;
        };

        // Return the retrieved key, value, and version as a tuple.
        // TODO: should return copy of value or reference?
        Some((twig.key.clone(), val.value.clone(), val.version, val.ts))
    }

    pub fn node_type_name(&self) -> String {
        match &self.node_type {
            NodeType::Node1(_) => "Node1".to_string(),
            NodeType::Node4(_) => "Node4".to_string(),
            NodeType::Node16(_) => "Node16".to_string(),
            NodeType::Node48(_) => "Node48".to_string(),
            NodeType::Node256(_) => "Node256".to_string(),
            NodeType::Twig(_) => "twig".to_string(),
        }
    }

    /// Creates a clone of the current node.
    ///
    /// Creates and returns a new instance of the current node with the same node type and contents.
    ///
    /// # Returns
    ///
    /// Returns a cloned instance of the current node with the same node type and contents.
    ///
    fn clone_node(&self) -> Self {
        // Create a new instance with the same node type as the current node.
        Self {
            node_type: self.node_type.clone(),
        }
    }

    /// Inserts a key-value pair recursively into the node.
    ///
    /// Recursively inserts a key-value pair into the current node and its child nodes.
    ///
    /// # Parameters
    ///
    /// - `cur_node`: A reference to the current node.
    /// - `key`: The key to be inserted.
    /// - `value`: The value associated with the key.
    /// - `commit_version`: The version when the value was inserted.
    /// - `depth`: The depth of the insertion process.
    ///
    /// # Returns
    ///
    /// Returns the updated node and the old value (if any) for the given key.
    ///
    pub(crate) fn insert_recurse(
        cur_node: &Arc<Node<P, V>>,
        key: &P,
        value: V,
        commit_version: u64,
        ts: u64,
        depth: usize,
    ) -> Result<(Arc<Node<P, V>>, Option<V>), TrieError> {
        // Obtain the current node's prefix and its length.
        let cur_node_prefix = cur_node.prefix().clone();
        let cur_node_prefix_len = cur_node.prefix().len();

        // Determine the prefix of the key after the current depth.
        let key_prefix = key.prefix_after(depth);
        let key_prefix = key_prefix.as_slice();
        // Find the longest common prefix between the current node's prefix and the key's prefix.
        let longest_common_prefix = cur_node_prefix.longest_common_prefix(key_prefix);

        // Create a new key that represents the remaining part of the current node's prefix after the common prefix.
        let new_key = cur_node_prefix.prefix_after(longest_common_prefix);
        // Extract the prefix of the current node up to the common prefix.
        let prefix = cur_node_prefix.prefix_before(longest_common_prefix);
        // Determine whether the current node's prefix and the key's prefix match up to the common prefix.
        let is_prefix_match = min(cur_node_prefix_len, key_prefix.len()) == longest_common_prefix;

        // If the current node is a Twig node and the prefixes match up to the end of both prefixes,
        // update the existing value in the Twig node.
        if let NodeType::Twig(ref twig) = &cur_node.node_type {
            if is_prefix_match && cur_node_prefix.len() == key_prefix.len() {
                let old_val = twig.get_leaf_by_version(commit_version).unwrap();
                let new_twig = twig.insert(value, commit_version, ts);
                return Ok((
                    Arc::new(Node {
                        node_type: NodeType::Twig(new_twig),
                    }),
                    Some(old_val.value.clone()),
                ));
            }
        }

        // If the prefixes don't match, create a new Node4 with the old node and a new Twig as children.
        if !is_prefix_match {
            let mut old_node = cur_node.clone_node();
            old_node.set_prefix(new_key);
            let mut n4 = Node::new_node4(prefix);

            let k1 = cur_node_prefix.at(longest_common_prefix);
            let k2 = key_prefix[longest_common_prefix];
            let new_twig = Node::new_twig(
                key_prefix[longest_common_prefix..].into(),
                key.as_slice().into(),
                value,
                commit_version,
                ts,
            );
            n4 = n4.add_child(k1, old_node).add_child(k2, new_twig);
            return Ok((Arc::new(n4), None));
        }

        // Continue the insertion process by finding or creating the appropriate child node for the next character.
        let k = key_prefix[longest_common_prefix];
        let child_for_key = cur_node.find_child(k);
        if let Some(child) = child_for_key {
            match Node::insert_recurse(
                child,
                key,
                value,
                commit_version,
                ts,
                depth + longest_common_prefix,
            ) {
                Ok((new_child, old_value)) => {
                    let new_node = cur_node.replace_child(k, new_child);
                    return Ok((Arc::new(new_node), old_value));
                }
                Err(err) => {
                    return Err(err);
                }
            }
        };

        // If no child exists for the key's character, create a new Twig node and add it as a child.
        let new_twig = Node::new_twig(
            key_prefix[longest_common_prefix..].into(),
            key.as_slice().into(),
            value,
            commit_version,
            ts,
        );
        let new_node = cur_node.add_child(k, new_twig);
        Ok((Arc::new(new_node), None))
    }

    /// Removes a key recursively from the node and its children.
    ///
    /// Recursively removes a key from the current node and its child nodes.
    ///
    /// # Parameters
    ///
    /// - `cur_node`: A reference to the current node.
    /// - `key`: The key to be removed.
    /// - `depth`: The depth of the removal process.
    ///
    /// # Returns
    ///
    /// Returns a tuple containing the updated node (or `None`) and a flag indicating if the key was removed.
    ///
    pub(crate) fn remove_recurse(
        cur_node: &Arc<Node<P, V>>,
        key: &P,
        depth: usize,
    ) -> (Option<Arc<Node<P, V>>>, bool) {
        // Obtain the prefix of the current node.
        let prefix = cur_node.prefix().clone();

        // Determine the prefix of the key after the current depth.
        let key_prefix = key.prefix_after(depth);
        let key_prefix = key_prefix.as_slice();
        // Find the longest common prefix between the current node's prefix and the key's prefix.
        let longest_common_prefix = prefix.longest_common_prefix(key_prefix);
        // Determine whether the current node's prefix and the key's prefix match up to the common prefix.
        let is_prefix_match = min(prefix.len(), key_prefix.len()) == longest_common_prefix;

        // If the current node's prefix and the key's prefix match up to the end of both prefixes,
        // the key has been found and should be removed.
        if is_prefix_match && prefix.len() == key_prefix.len() {
            return (None, true);
        }

        // Determine the character at the common prefix position.
        let k = key_prefix[longest_common_prefix];

        // Search for a child node corresponding to the key's character.
        let child = cur_node.find_child(k);
        if let Some(child_node) = child {
            // Recursively attempt to remove the key from the child node.
            let (_new_child, removed) =
                Node::remove_recurse(child_node, key, depth + longest_common_prefix);
            if removed {
                // If the key was successfully removed from the child node, update the current node's child pointer.
                let new_node = cur_node.delete_child(k);
                return (Some(Arc::new(new_node)), true);
            }
        }

        // If the key was not found at this level, return the current node as-is.
        (Some(cur_node.clone()), false)
    }

    /// Recursively searches for a key in the node and its children.
    ///
    /// Recursively searches for a key in the current node and its child nodes, considering versions.
    ///
    /// # Parameters
    ///
    /// - `cur_node`: A reference to the current node.
    /// - `key`: The key to be searched for.
    /// - `ts`: The version for which to retrieve the value.
    ///
    /// # Returns
    ///
    /// Returns a result containing the prefix, value, and version if the key is found, or Error if not.
    ///
    pub fn get_recurse(
        cur_node: &Node<P, V>,
        key: &P,
        version: u64,
    ) -> Result<(P, V, u64, u64), TrieError> {
        // Initialize the traversal variables.
        let mut cur_node = cur_node;
        let mut depth = 0;

        // Start a loop to navigate through the tree.
        loop {
            // Determine the prefix of the key after the current depth.
            let key_prefix = key.prefix_after(depth);
            let key_prefix = key_prefix.as_slice();
            // Obtain the prefix of the current node.
            let prefix = cur_node.prefix();
            // Find the longest common prefix between the node's prefix and the key's prefix.
            let lcp = prefix.longest_common_prefix(key_prefix);

            // If the longest common prefix does not match the entire node's prefix, the key is not present.
            if lcp != prefix.len() {
                return Err(TrieError::KeyNotFound);
            }

            // If the current node's prefix length matches the key's prefix length, retrieve the value.
            if prefix.len() == key_prefix.len() {
                let Some(val) = cur_node.get_value_by_version(version) else {
                    return Err(TrieError::KeyNotFound);
                };
                return Ok((val.0, val.1, val.2, val.3));
            }

            // Determine the character at the next position after the prefix in the key.
            let k = key.at(depth + prefix.len());
            // Increment the depth by the prefix length.
            depth += prefix.len();
            // Find the child node corresponding to the character and update the current node for further traversal.
            match cur_node.find_child(k) {
                Some(child) => cur_node = child,
                None => return Err(TrieError::KeyNotFound),
            }
        }
    }

    /// Returns an iterator that iterates over child nodes of the current node.
    ///
    /// This function provides an iterator that traverses through the child nodes of the current node,
    /// returning tuples of keys and references to child nodes.
    ///
    /// # Returns
    ///
    /// Returns a boxed iterator that yields tuples containing keys and references to child nodes.
    ///
    #[allow(dead_code)]
    pub fn iter(&self) -> Box<dyn Iterator<Item = (u8, &Arc<Self>)> + '_> {
        match &self.node_type {
            NodeType::Node1(n) => Box::new(n.iter()),
            NodeType::Node4(n) => Box::new(n.iter()),
            NodeType::Node16(n) => Box::new(n.iter()),
            NodeType::Node48(n) => Box::new(n.iter()),
            NodeType::Node256(n) => Box::new(n.iter()),
            NodeType::Twig(_) => Box::new(std::iter::empty()),
        }
    }
}

/// A struct representing an Adaptive Radix Trie.
///
/// The `Tree` struct encompasses the entire adaptive radix trie data structure.
/// It manages the root node of the tree, maintains snapshots of the tree's state,
/// and keeps track of various properties related to snapshot management.
///
/// # Type Parameters
///
/// - `P`: A type implementing the `KeyTrait` trait, defining the prefix traits for nodes.
/// - `V`: The type of value associated with nodes.
///
/// # Fields
///
/// - `root`: An optional shared reference (using `Rc`) to the root node of the tree.
/// - `snapshots`: A `HashSet` storing snapshots of the tree's state, mapped by snapshot IDs.
/// - `max_snapshot_id`: An `AtomicU64` representing the maximum snapshot ID assigned.
/// - `max_active_snapshots`: The maximum number of active snapshots allowed.
///
pub struct Tree<P: KeyTrait, V: Clone> {
    /// An optional shared reference to the root node of the tree.
    pub(crate) root: Option<Arc<Node<P, V>>>,
    /// A mapping of snapshot IDs to their corresponding snapshots.
    pub(crate) snapshots: HashSet<u64>,
    /// An atomic value indicating the maximum snapshot ID assigned.
    pub(crate) max_snapshot_id: AtomicU64,
    /// The maximum number of active snapshots allowed.
    pub(crate) max_active_snapshots: u64,
    /// A flag indicating whether the tree is closed.
    pub(crate) closed: bool,
}

pub struct KV<P, V> {
    pub key: P,
    pub value: V,
    pub version: u64,
    pub ts: u64,
}

impl<P: KeyTrait, V: Clone> KV<P, V> {
    pub fn new(key: P, value: V, version: u64, timestamp: u64) -> Self {
        KV {
            key,
            value,
            version,
            ts: timestamp,
        }
    }
}

impl<P: KeyTrait + Clone, V: Clone> NodeType<P, V> {
    fn clone(&self) -> Self {
        match self {
            // twig value not actually cloned
            NodeType::Twig(twig) => NodeType::Twig(twig.clone()),
            NodeType::Node1(n) => NodeType::Node1(n.clone()),
            NodeType::Node4(n) => NodeType::Node4(n.clone()),
            NodeType::Node16(n) => NodeType::Node16(n.clone()),
            NodeType::Node48(n) => NodeType::Node48(n.clone()),
            NodeType::Node256(n) => NodeType::Node256(n.clone()),
        }
    }
}

// Default implementation for the Tree struct
impl<P: KeyTrait, V: Clone> Default for Tree<P, V> {
    fn default() -> Self {
        Tree::new()
    }
}

impl<P: KeyTrait, V: Clone> Tree<P, V> {
    pub fn new() -> Self {
        Tree {
            root: None,
            max_snapshot_id: AtomicU64::new(0),
            snapshots: HashSet::new(),
            max_active_snapshots: DEFAULT_MAX_ACTIVE_SNAPSHOTS,
            closed: false,
        }
    }

    pub fn set_max_active_snapshots(&mut self, max_active_snapshots: u64) {
        self.max_active_snapshots = max_active_snapshots;
    }

    /// Inserts a new key-value pair with the specified version into the Trie.
    ///
    /// This function inserts a new key-value pair into the Trie. If the key already exists,
    /// the previous value associated with the key is returned. The version `ts` is used to
    /// ensure proper ordering of values for versioning.
    ///
    /// # Arguments
    ///
    /// * `key`: A reference to the key to be inserted.
    /// * `value`: The value to be associated with the key.
    /// * `ts`: The version for the insertion, used for versioning.
    ///
    /// # Returns
    ///
    /// Returns `Ok(None)` if the key did not exist previously. If the key already existed,
    /// `Ok(Some(old_value))` is returned, where `old_value` is the previous value associated with the key.
    ///
    /// # Errors
    ///
    /// Returns an error if the given version is older than the root's current version.
    ///
    pub fn insert(
        &mut self,
        key: &P,
        value: V,
        version: u64,
        ts: u64,
    ) -> Result<Option<V>, TrieError> {
        // Check if the tree is already closed
        self.is_closed()?;

        let (new_root, old_node) = match &self.root {
            None => {
                let mut commit_version = version;
                if version == 0 {
                    commit_version += 1;
                }
                (
                    Arc::new(Node::new_twig(
                        key.as_slice().into(),
                        key.as_slice().into(),
                        value,
                        commit_version,
                        ts,
                    )),
                    None,
                )
            }
            Some(root) => {
                // Check if the given version is older than the root's current version.
                // If so, return an error and do not insert the new node.
                let curr_version = root.version();
                let mut commit_version = version;
                if version == 0 {
                    commit_version = curr_version + 1;
                } else if curr_version >= version {
                    return Err(TrieError::Other(
                        "given version is older than root's current version".to_string(),
                    ));
                }
                match Node::insert_recurse(root, key, value, commit_version, ts, 0) {
                    Ok((new_node, old_node)) => (new_node, old_node),
                    Err(err) => {
                        return Err(err);
                    }
                }
            }
        };

        self.root = Some(new_root);
        Ok(old_node)
    }

    pub fn bulk_insert(&mut self, kv_pairs: &[KV<P, V>]) -> Result<(), TrieError> {
        // Check if the tree is already closed
        self.is_closed()?;

        let curr_version = self.version();
        let mut new_version = 0;

        for kv in kv_pairs {
            let k = kv.key.clone(); // Clone the key
            let v = kv.value.clone(); // Clone the value
            let mut t = kv.version;

            if t == 0 {
                // Zero-valued timestamps are associated with current time plus one
                t = curr_version + 1;
            } else if kv.version < curr_version {
                return Err(TrieError::Other(
                    "given version is older than root's current version".to_string(),
                ));
            }

            // Create a new KV instance
            let new_kv = KV {
                key: k,
                value: v,
                version: t,
                ts: kv.ts,
            };

            // Insert the new KV instance using the insert function
            // self.insert(&new_kv.key, new_kv.value, new_kv.version, new_kv.ts)?;
            match &self.root {
                None => {
                    self.root = Some(Arc::new(Node::new_twig(
                        new_kv.key.as_slice().into(),
                        new_kv.key.as_slice().into(),
                        new_kv.value,
                        new_kv.version,
                        new_kv.ts,
                    )))
                }
                Some(root) => {
                    match Node::insert_recurse(
                        root,
                        &new_kv.key,
                        new_kv.value,
                        new_kv.version,
                        new_kv.ts,
                        0,
                    ) {
                        Ok((new_node, _)) => {
                            self.root = Some(new_node);
                        }
                        Err(err) => {
                            return Err(err);
                        }
                    }
                }
            }

            // Update new_version if necessary
            if t > new_version {
                new_version = t;
            }
        }

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

    pub fn get(&self, key: &P, version: u64) -> Result<(P, V, u64, u64), TrieError> {
        // Check if the tree is already closed
        self.is_closed()?;

        if self.root.is_none() {
            return Err(TrieError::Other("cannot read from empty tree".to_string()));
        }

        let root = self.root.as_ref().unwrap();
        let mut commit_version = version;
        if commit_version == 0 {
            commit_version = root.version();
        }

        Node::get_recurse(root, key, commit_version)
    }

    /// Retrieves the latest version of the Trie.
    ///
    /// This function returns the version of the latest version of the Trie. If the Trie is empty,
    /// it returns `0`.
    ///
    /// # Returns
    ///
    /// Returns the version of the latest version of the Trie, or `0` if the Trie is empty.
    ///
    pub fn version(&self) -> u64 {
        match &self.root {
            None => 0,
            Some(root) => root.version(),
        }
    }

    /// Creates a new snapshot of the Trie.
    ///
    /// This function creates a snapshot of the current state of the Trie. If successful, it returns
    /// a `Snapshot` that can be used to interact with the newly created snapshot.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the `Snapshot` if the snapshot is created successfully,
    /// or an `Err` with an appropriate error message if creation fails.
    ///
    pub fn create_snapshot(&mut self) -> Result<Snapshot<P, V>, TrieError> {
        // Check if the tree is already closed
        self.is_closed()?;

        if self.snapshots.len() >= self.max_active_snapshots as usize {
            return Err(TrieError::Other(
                "max number of snapshots reached".to_string(),
            ));
        }

        // Increment the snapshot ID atomically
        let new_snapshot_id = self.max_snapshot_id.fetch_add(1, Ordering::SeqCst);
        self.snapshots.insert(new_snapshot_id);

        let root = self.root.as_ref().cloned();
        let version = self.root.as_ref().map_or(1, |root| root.version() + 1);
        let new_snapshot = Snapshot::new(new_snapshot_id, root, version);

        Ok(new_snapshot)
    }

    /// Closes a snapshot and removes it from the list of active snapshots.
    ///
    /// This function takes a `snapshot_id` as an argument and closes the corresponding snapshot.
    /// If the snapshot exists, it is removed from the active snapshots list. If the snapshot is not
    /// found, an `Err` is returned with a `TrieError::SnapshotNotFound` variant.
    ///
    /// # Arguments
    ///
    /// * `snapshot_id` - The ID of the snapshot to be closed and removed.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the snapshot is successfully closed and removed. Returns an `Err`
    /// with `TrieError::SnapshotNotFound` if the snapshot with the given ID is not found.
    ///
    pub(crate) fn close_snapshot(&mut self, snapshot_id: u64) -> Result<(), TrieError> {
        // Check if the tree is already closed
        self.is_closed()?;

        if self.snapshots.remove(&snapshot_id) {
            Ok(())
        } else {
            Err(TrieError::SnapshotNotFound)
        }
    }

    /// Returns the count of active snapshots.
    ///
    /// This function returns the number of currently active snapshots in the Trie.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the count of active snapshots if successful, or an `Err`
    /// if there is an issue retrieving the snapshot count.
    ///
    pub fn snapshot_count(&self) -> usize {
        self.snapshots.len()
    }

    /// Creates an iterator over the Trie's key-value pairs.
    ///
    /// This function creates and returns an iterator that can be used to traverse the key-value pairs
    /// stored in the Trie. The iterator starts from the root of the Trie.
    ///
    /// # Returns
    ///
    /// Returns an `Iter` instance that iterates over the key-value pairs in the Trie.
    ///
    pub fn iter(&self) -> Iter<P, V> {
        Iter::new(self.root.as_ref())
    }

    /// Returns an iterator over a range of key-value pairs within the Trie.
    ///
    /// This function creates and returns an iterator that iterates over key-value pairs in the Trie,
    /// starting from the provided `start_key` and following the specified `range` bounds. The iterator
    /// iterates within the specified key range.
    ///
    /// # Arguments
    ///
    /// * `range` - A range that specifies the bounds for iterating over key-value pairs.
    ///
    /// # Returns
    ///
    /// Returns a `Range` iterator instance that iterates over the key-value pairs within the given range.
    /// If the Trie is empty, an empty `Range` iterator is returned.
    ///
    pub fn range<'a, R>(
        &'a self,
        range: R,
    ) -> impl Iterator<Item = (Vec<u8>, &'a V, &'a u64, &'a u64)>
    where
        R: RangeBounds<P> + 'a,
    {
        // If the Trie is empty, return an empty Range iterator
        if self.root.is_none() {
            return Range::empty(range);
        }

        let root = self.root.as_ref();
        Range::new(root, range)
    }

    fn is_closed(&self) -> Result<(), TrieError> {
        if self.closed {
            return Err(TrieError::SnapshotAlreadyClosed);
        }
        Ok(())
    }

    /// Closes the tree, preventing further modifications, and releases associated resources.
    pub fn close(&mut self) -> Result<(), TrieError> {
        // Check if the tree is already closed
        self.is_closed()?;

        // Check if there are any active readers for the snapshot
        if self.snapshot_count() > 0 {
            return Err(TrieError::SnapshotNotClosed);
        }

        // Mark the snapshot as closed
        self.closed = true;

        Ok(())
    }
}

/*
    Test cases for Adaptive Radix Tree
*/

#[cfg(test)]
mod tests {
    use super::{Tree, KV};
    use crate::storage::index::{FixedKey, VariableKey};

    use std::fs::File;
    use std::io::{self, BufRead, BufReader};
    use std::str::FromStr;

    fn read_words_from_file(file_path: &str) -> io::Result<Vec<String>> {
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let words: Vec<String> = reader.lines().filter_map(|line| line.ok()).collect();
        Ok(words)
    }

    #[test]
    fn insert_search_delete_words() {
        let mut tree: Tree<VariableKey, i32> = Tree::<VariableKey, i32>::new();
        let file_path = "testdata/words.txt";

        if let Ok(words) = read_words_from_file(file_path) {
            // Insertion phase
            for word in &words {
                let key = &VariableKey::from_str(word).unwrap();
                tree.insert(key, 1, 0, 0);
            }

            // Search phase
            for word in &words {
                let key = VariableKey::from_str(word).unwrap();
                let (_, val, _, _) = tree.get(&key, 0).unwrap();
                assert_eq!(val, 1);
            }

            // Deletion phase
            for word in &words {
                let key = VariableKey::from_str(word).unwrap();
                assert!(tree.remove(&key).unwrap());
            }
        } else if let Err(err) = read_words_from_file(file_path) {
            eprintln!("Error reading file: {}", err);
        }

        assert_eq!(tree.version(), 0);
    }

    #[test]
    fn string_insert_delete() {
        let mut tree = Tree::<VariableKey, i32>::new();

        // Insertion phase
        let insert_words = [
            "a", "aa", "aal", "aalii", "abc", "abcd", "abcde", "xyz", "axyz",
        ];

        for word in &insert_words {
            tree.insert(&VariableKey::from_str(word).unwrap(), 1, 0, 0);
        }

        // Deletion phase
        for word in &insert_words {
            assert!(tree.remove(&VariableKey::from_str(word).unwrap()).unwrap());
        }
    }

    #[test]
    fn string_long() {
        let mut tree = Tree::<VariableKey, i32>::new();

        // Insertion phase
        let words_to_insert = [
            ("amyelencephalia", 1),
            ("amyelencephalic", 2),
            ("amyelencephalous", 3),
        ];

        for (word, val) in &words_to_insert {
            tree.insert(&VariableKey::from_str(word).unwrap(), *val, 0, 0);
        }

        // Verification phase
        for (word, expected_val) in &words_to_insert {
            let (_, val, _, _) = tree.get(&VariableKey::from_str(word).unwrap(), 0).unwrap();
            assert_eq!(val, *expected_val);
        }
    }

    #[test]
    fn root_set_get() {
        let mut tree = Tree::<VariableKey, i32>::new();

        // Insertion phase
        let key = VariableKey::from_str("abc").unwrap();
        let value = 1;
        tree.insert(&key, value, 0, 0);

        // Verification phase
        let (_, val, _ts, _) = tree.get(&key, 0).unwrap();
        assert_eq!(val, value);
    }

    #[test]
    fn string_duplicate_insert() {
        let mut tree = Tree::<VariableKey, i32>::new();

        // First insertion
        let key = VariableKey::from_str("abc").unwrap();
        let value = 1;
        let result = tree.insert(&key, value, 0, 0).expect("Failed to insert");
        assert!(result.is_none());

        // Second insertion (duplicate)
        let result = tree.insert(&key, value, 0, 0).expect("Failed to insert");
        assert!(result.is_some());
    }

    // Inserting a single value into the tree and removing it should result in a nil tree root.
    #[test]
    fn insert_and_remove() {
        let mut tree = Tree::<VariableKey, i32>::new();

        // Insertion
        let key = VariableKey::from_str("test").unwrap();
        let value = 1;
        tree.insert(&key, value, 0, 0);

        // Removal
        assert!(tree.remove(&key).unwrap());

        // Verification
        assert!(tree.get(&key, 0).is_err());
    }

    #[test]
    fn inserting_keys_with_common_prefix() {
        let key1 = VariableKey::from_str("foo").unwrap();
        let key2 = VariableKey::from_str("foo2").unwrap();

        let mut tree = Tree::<VariableKey, i32>::new();

        // Insertion
        tree.insert(&key1, 1, 0, 0);
        tree.insert(&key2, 1, 0, 0);

        // Removal
        assert!(tree.remove(&key1).unwrap());

        // Root verification
        if let Some(root) = &tree.root {
            assert_eq!(root.node_type_name(), "Node1");
        } else {
            panic!("Tree root is None");
        }
    }

    // Inserting Two values into the tree and removing one of them
    // should result in a tree root of type twig
    #[test]
    fn insert2_and_remove1_and_root_should_be_node1() {
        let key1 = VariableKey::from_str("test1").unwrap();
        let key2 = VariableKey::from_str("test2").unwrap();

        let mut tree = Tree::<VariableKey, i32>::new();

        // Insertion
        tree.insert(&key1, 1, 0, 0);
        tree.insert(&key2, 1, 0, 0);

        // Removal
        assert!(tree.remove(&key1).unwrap());

        // Root verification
        if let Some(root) = &tree.root {
            assert_eq!(root.node_type_name(), "Node1");
        } else {
            panic!("Tree root is None");
        }
    }

    // // Inserting Two values into a tree and deleting them both
    // // should result in a nil tree root
    // // This tests the expansion of the root into a NODE4 and
    // // successfully collapsing into a twig and then nil upon successive removals
    // #[test]
    // fn insert2_and_remove2_and_root_should_be_nil() {
    //     let key1 = &VariableKey::from_str("test1");
    //     let key2 = &VariableKey::from_str("test2");

    //     let mut tree = Tree::<VariableKey, i32>::new();
    //     tree.insert(key1, 1, 0, 0);
    //     tree.insert(key2, 1, 0);

    //     assert_eq!(tree.remove(key1), true);
    //     assert_eq!(tree.remove(key2), true);

    //     assert!(tree.root.is_none());
    // }

    // Inserting Five values into a tree and deleting one of them
    // should result in a tree root of type NODE4
    // This tests the expansion of the root into a NODE16 and
    // successfully collapsing into a NODE4 upon successive removals
    #[test]
    fn insert5_and_remove1_and_root_should_be_node4() {
        let mut tree = Tree::<VariableKey, i32>::new();

        // Insertion
        for i in 0..5u32 {
            let key = VariableKey::from_slice(&i.to_be_bytes());
            tree.insert(&key, 1, 0, 0);
        }

        // Removal
        let key_to_remove = VariableKey::from_slice(&1u32.to_be_bytes());
        assert!(tree.remove(&key_to_remove).unwrap());

        // Root verification
        if let Some(root) = &tree.root {
            assert!(root.is_inner());
            assert_eq!(root.node_type_name(), "Node4");
        } else {
            panic!("Tree root is None");
        }
    }

    //     // Inserting Five values into a tree and deleting all of them
    //     // should result in a tree root of type nil
    //     // This tests the expansion of the root into a NODE16 and
    //     // successfully collapsing into a NODE4, twig, then nil
    //     #[test]
    //     fn insert5_and_remove5_and_root_should_be_nil() {
    //         let mut tree = Tree::<VariableKey, i32>::new();

    //         for i in 0..5u32 {
    //             let key = &VariableKey::from_slice(&i.to_be_bytes());
    //             tree.insert(key, 1);
    //         }

    //         for i in 0..5u32 {
    //             let key = &VariableKey::from_slice(&i.to_be_bytes());
    //             tree.remove(key);
    //         }

    //         assert!(tree.root.is_none());
    //     }

    // Inserting 17 values into a tree and deleting one of them should
    // result in a tree root of type NODE16
    // This tests the expansion of the root into a NODE48, and
    // successfully collapsing into a NODE16
    #[test]
    fn insert17_and_remove1_and_root_should_be_node16() {
        let mut tree = Tree::<VariableKey, i32>::new();

        // Insertion
        for i in 0..17u32 {
            let key = VariableKey::from_slice(&i.to_be_bytes());
            tree.insert(&key, 1, 0, 0);
        }

        // Removal
        let key_to_remove = VariableKey::from_slice(&2u32.to_be_bytes());
        assert!(tree.remove(&key_to_remove).unwrap());

        // Root verification
        if let Some(root) = &tree.root {
            assert!(root.is_inner());
            assert_eq!(root.node_type_name(), "Node16");
        } else {
            panic!("Tree root is None");
        }
    }

    #[test]
    fn insert17_and_root_should_be_node48() {
        let mut tree = Tree::<VariableKey, i32>::new();

        // Insertion
        for i in 0..17u32 {
            let key = VariableKey::from_slice(&i.to_be_bytes());
            tree.insert(&key, 1, 0, 0);
        }

        // Root verification
        if let Some(root) = &tree.root {
            assert!(root.is_inner());
            assert_eq!(root.node_type_name(), "Node48");
        } else {
            panic!("Tree root is None");
        }
    }

    // // Inserting 17 values into a tree and removing them all should
    // // result in a tree of root type nil
    // // This tests the expansion of the root into a NODE48, and
    // // successfully collapsing into a NODE16, NODE4, twig, and then nil
    // #[test]
    // fn insert17_and_remove17_and_root_should_be_nil() {
    //     let mut tree = Tree::<VariableKey, i32>::new();

    //     for i in 0..17u32 {
    //         let key = VariableKey::from_slice(&i.to_be_bytes());
    //         tree.insert(&key, 1);
    //     }

    //     for i in 0..17u32 {
    //         let key = VariableKey::from_slice(&i.to_be_bytes());
    //         tree.remove(&key);
    //     }

    //     assert!(tree.root.is_none());
    // }

    // Inserting 49 values into a tree and removing one of them should
    // result in a tree root of type NODE48
    // This tests the expansion of the root into a NODE256, and
    // successfully collapasing into a NODE48
    #[test]
    fn insert49_and_remove1_and_root_should_be_node48() {
        let mut tree = Tree::<VariableKey, i32>::new();

        // Insertion
        for i in 0..49u32 {
            let key = VariableKey::from_slice(&i.to_be_bytes());
            tree.insert(&key, 1, 0, 0);
        }

        // Removal
        let key_to_remove = VariableKey::from_slice(&2u32.to_be_bytes());
        assert!(tree.remove(&key_to_remove).unwrap());

        // Root verification
        if let Some(root) = &tree.root {
            assert!(root.is_inner());
            assert_eq!(root.node_type_name(), "Node48");
        } else {
            panic!("Tree root is None");
        }
    }

    #[test]
    fn insert49_and_root_should_be_node248() {
        let mut tree = Tree::<VariableKey, i32>::new();

        // Insertion
        for i in 0..49u32 {
            let key = VariableKey::from_slice(&i.to_be_bytes());
            tree.insert(&key, 1, 0, 0);
        }

        // Root verification
        if let Some(root) = &tree.root {
            assert!(root.is_inner());
            assert_eq!(root.node_type_name(), "Node256");
        } else {
            panic!("Tree root is None");
        }
    }

    //     // // Inserting 49 values into a tree and removing all of them should
    //     // // result in a nil tree root
    //     // // This tests the expansion of the root into a NODE256, and
    //     // // successfully collapsing into a Node48, Node16, Node4, twig, and finally nil
    //     // #[test]
    //     // fn insert49_and_remove49_and_root_should_be_nil() {
    //     //     let mut tree = Tree::<VariableKey, i32>::new();

    //     //     for i in 0..49u32 {
    //     //         let key = &VariableKey::from_slice(&i.to_be_bytes());
    //     //         tree.insert(key, 1);
    //     //     }

    //     //     for i in 0..49u32 {
    //     //         let key = VariableKey::from_slice(&i.to_be_bytes());
    //     //         assert_eq!(tree.remove(&key), true);
    //     //     }

    //     //     assert!(tree.root.is_none());
    //     // }

    #[derive(Debug, Clone, PartialEq)]
    struct KVT {
        k: Vec<u8>,   // Key
        version: u64, // version
    }

    #[test]
    fn timed_insertion() {
        let mut tree: Tree<VariableKey, i32> = Tree::<VariableKey, i32>::new();

        let kvts = vec![
            KVT {
                k: b"key1_0".to_vec(),
                version: 0,
            },
            KVT {
                k: b"key2_0".to_vec(),
                version: 0,
            },
            KVT {
                k: b"key3_0".to_vec(),
                version: 0,
            },
            KVT {
                k: b"key4_0".to_vec(),
                version: 0,
            },
            KVT {
                k: b"key5_0".to_vec(),
                version: 0,
            },
            KVT {
                k: b"key6_0".to_vec(),
                version: 0,
            },
        ];

        // Insertion
        for (idx, kvt) in kvts.iter().enumerate() {
            let ts = if kvt.version == 0 {
                idx as u64 + 1
            } else {
                kvt.version
            };
            assert!(tree
                .insert(&VariableKey::from(kvt.k.clone()), 1, ts, 0)
                .is_ok());
        }

        // Verification
        let mut curr_version = 1;
        for kvt in &kvts {
            let key = VariableKey::from(kvt.k.clone());
            let (_, val, version, _ts) = tree.get(&key, 0).unwrap();
            assert_eq!(val, 1);

            if kvt.version == 0 {
                assert_eq!(curr_version, version);
            } else {
                assert_eq!(kvt.version, version);
            }

            curr_version += 1;
        }

        // Root's version should match the greatest inserted version
        assert_eq!(kvts.len() as u64, tree.version());
    }

    #[test]
    fn timed_insertion_update_same_key() {
        let mut tree: Tree<VariableKey, i32> = Tree::<VariableKey, i32>::new();

        let key1 = &VariableKey::from_str("key_1").unwrap();

        // insert key1 with version 0
        assert!(tree.insert(key1, 1, 0, 1).is_ok());
        // update key1 with version 0
        assert!(tree.insert(key1, 1, 0, 3).is_ok());

        // get key1 should return version 2 as the same key was inserted and updated
        let (_, val, version, ts) = tree.get(key1, 0).unwrap();
        assert_eq!(val, 1);
        assert_eq!(version, 2);
        assert_eq!(ts, 3);

        // update key1 with older version should fail
        assert!(tree.insert(key1, 1, 1, 0).is_err());
        assert_eq!(tree.version(), 2);

        // update key1 with newer version should pass
        assert!(tree.insert(key1, 1, 8, 5).is_ok());
        let (_, val, version, ts) = tree.get(key1, 0).unwrap();
        assert_eq!(val, 1);
        assert_eq!(version, 8);
        assert_eq!(ts, 5);

        assert_eq!(tree.version(), 8);
    }

    #[test]
    fn timed_insertion_update_non_increasing_version() {
        let mut tree: Tree<VariableKey, i32> = Tree::<VariableKey, i32>::new();

        let key1 = VariableKey::from_str("key_1").unwrap();
        let key2 = VariableKey::from_str("key_2").unwrap();

        // Initial insertion
        assert!(tree.insert(&key1, 1, 10, 0).is_ok());
        let initial_version_key1 = tree.version();

        // Attempt update with non-increasing version
        assert!(tree.insert(&key1, 1, 2, 0).is_err());
        assert_eq!(initial_version_key1, tree.version());
        let (_, val, version, _) = tree.get(&key1, 0).unwrap();
        assert_eq!(val, 1);
        assert_eq!(version, 10);

        // Insert another key
        assert!(tree.insert(&key2, 1, 15, 0).is_ok());
        let initial_version_key2 = tree.version();

        // Attempt update with non-increasing version for the second key
        assert!(tree.insert(&key2, 1, 11, 0).is_err());
        assert_eq!(initial_version_key2, tree.version());
        let (_, val, version, _ts) = tree.get(&key2, 0).unwrap();
        assert_eq!(val, 1);
        assert_eq!(version, 15);

        // Check if the max version of the tree is the max of the two inserted versions
        assert_eq!(tree.version(), 15);
    }

    #[test]
    fn timed_insertion_update_equal_to_root_version() {
        let mut tree: Tree<VariableKey, i32> = Tree::<VariableKey, i32>::new();

        let key1 = VariableKey::from_str("key_1").unwrap();
        let key2 = VariableKey::from_str("key_2").unwrap();

        // Initial insertion
        assert!(tree.insert(&key1, 1, 10, 0).is_ok());
        let initial_version = tree.version();

        // Attempt update with version equal to root's version
        assert!(tree.insert(&key2, 1, initial_version, 0).is_err());
    }

    #[test]
    fn timed_deletion_check_root_ts() {
        let mut tree: Tree<VariableKey, i32> = Tree::<VariableKey, i32>::new();
        let key1 = VariableKey::from_str("key_1").unwrap();
        let key2 = VariableKey::from_str("key_2").unwrap();

        // Initial insertions
        assert!(tree.insert(&key1, 1, 0, 0).is_ok());
        assert!(tree.insert(&key2, 1, 0, 0).is_ok());
        assert_eq!(tree.version(), 2);

        // Deletions
        assert!(tree.remove(&key1).unwrap());
        assert!(tree.remove(&key2).unwrap());
        assert_eq!(tree.version(), 0);
    }

    fn from_be_bytes_key(k: &[u8]) -> u64 {
        let padded_k = if k.len() < 8 {
            let mut new_k = vec![0; 8];
            new_k[8 - k.len()..].copy_from_slice(k);
            new_k
        } else {
            k.to_vec()
        };

        let k_slice = &padded_k[..8];
        u64::from_be_bytes(k_slice.try_into().unwrap())
    }

    #[test]
    fn iter_seq_u16() {
        let mut tree = Tree::<FixedKey<16>, u16>::new();

        // Insertion
        for i in 0..u16::MAX {
            let key: FixedKey<16> = i.into();
            tree.insert(&key, i, 0, i as u64);
        }

        // Iteration and verification
        let mut len = 0usize;
        let mut expected = 0u16;

        let tree_iter = tree.iter();
        for tree_entry in tree_iter {
            let k = from_be_bytes_key(&tree_entry.0);
            assert_eq!(expected as u64, k);
            let ts = tree_entry.3;
            assert_eq!(expected as u64, *ts);
            expected = expected.wrapping_add(1);
            len += 1;
        }

        // Final assertion
        assert_eq!(len, u16::MAX as usize);
    }

    #[test]
    fn iter_seq_u8() {
        let mut tree: Tree<FixedKey<32>, u8> = Tree::<FixedKey<32>, u8>::new();

        // Insertion
        for i in 0..u8::MAX {
            let key: FixedKey<32> = i.into();
            tree.insert(&key, i, 0, 0);
        }

        // Iteration and verification
        let mut len = 0usize;
        let mut expected = 0u8;

        let tree_iter = tree.iter();
        for tree_entry in tree_iter {
            let k = from_be_bytes_key(&tree_entry.0);
            assert_eq!(expected as u64, k);
            expected = expected.wrapping_add(1);
            len += 1;
        }

        // Final assertion
        assert_eq!(len, u8::MAX as usize);
    }

    #[test]
    fn range_seq_u8() {
        let mut tree: Tree<FixedKey<8>, u8> = Tree::<FixedKey<8>, u8>::new();

        let max = u8::MAX;
        // Insertion
        for i in 0..=max {
            let key: FixedKey<8> = i.into();
            tree.insert(&key, i, 0, 0);
        }

        // Test inclusive range
        let start_key: FixedKey<8> = 5u8.into();
        let end_key: FixedKey<8> = max.into();
        let mut len = 0usize;
        for _ in tree.range(start_key..=end_key) {
            len += 1;
        }
        assert_eq!(len, max as usize - 4);

        // Test exclusive range
        let start_key: FixedKey<8> = 5u8.into();
        let end_key: FixedKey<8> = max.into();
        let mut len = 0usize;
        for _ in tree.range(start_key..end_key) {
            len += 1;
        }
        assert_eq!(len, max as usize - 5);

        // Test range with different start and end keys
        let start_key: FixedKey<8> = 3u8.into();
        let end_key: FixedKey<8> = 7u8.into();
        let mut len = 0usize;
        for _ in tree.range(start_key..=end_key) {
            len += 1;
        }
        assert_eq!(len, 5);

        // Test range with all keys
        let start_key: FixedKey<8> = 0u8.into();
        let end_key: FixedKey<8> = max.into();
        let mut len = 0usize;
        for _ in tree.range(start_key..=end_key) {
            len += 1;
        }
        assert_eq!(len, 256);
    }

    #[test]
    fn range_seq_u16() {
        let mut tree: Tree<FixedKey<16>, u16> = Tree::<FixedKey<16>, u16>::new();

        let max = u16::MAX;
        // Insertion
        for i in 0..=max {
            let key: FixedKey<16> = i.into();
            tree.insert(&key, i, 0, 0);
        }

        let mut len = 0usize;
        let start_key: FixedKey<16> = 0u8.into();
        let end_key: FixedKey<16> = max.into();

        for _ in tree.range(start_key..=end_key) {
            len += 1;
        }
        assert_eq!(len, max as usize + 1);
    }

    #[test]
    fn same_key_with_versions() {
        let mut tree = Tree::<VariableKey, i32>::new();

        // Insertions
        let key1 = VariableKey::from_str("abc").unwrap();
        let key2 = VariableKey::from_str("efg").unwrap();
        tree.insert(&key1, 1, 0, 0);
        tree.insert(&key1, 2, 10, 0);
        tree.insert(&key2, 3, 11, 0);

        // Versioned retrievals and assertions
        let (_, val, _, _) = tree.get(&key1, 1).unwrap();
        assert_eq!(val, 1);
        let (_, val, _, _) = tree.get(&key1, 10).unwrap();
        assert_eq!(val, 2);
        let (_, val, _, _) = tree.get(&key2, 11).unwrap();
        assert_eq!(val, 3);

        // Iteration and verification
        let mut len = 0;
        let tree_iter = tree.iter();
        for _ in tree_iter {
            len += 1;
        }
        assert_eq!(len, 2);
    }

    #[test]
    fn bulk_insert() {
        let mut tree: Tree<VariableKey, i32> = Tree::<VariableKey, i32>::new();
        let curr_version = tree.version();
        // Create a vector of KV<P, V>
        let kv_pairs = vec![
            KV {
                key: VariableKey::from_str("key_1").unwrap(),
                value: 1,
                version: 0,
                ts: 0,
            },
            KV {
                key: VariableKey::from_str("key_2").unwrap(),
                value: 1,
                version: 2,
                ts: 0,
            },
            KV {
                key: VariableKey::from_str("key_3").unwrap(),
                value: 1,
                version: curr_version + 1,
                ts: 0,
            },
            KV {
                key: VariableKey::from_str("key_4").unwrap(),
                value: 1,
                version: curr_version + 1,
                ts: 0,
            },
            KV {
                key: VariableKey::from_str("key_5").unwrap(),
                value: 1,
                version: curr_version + 2,
                ts: 0,
            },
            KV {
                key: VariableKey::from_str("key_6").unwrap(),
                value: 1,
                version: 0,
                ts: 0,
            },
        ];

        assert!(tree.bulk_insert(&kv_pairs).is_ok());
        assert!(tree.version() == curr_version + 2);

        for kv in kv_pairs {
            let (_, val, version, _) = tree.get(&kv.key, 0).unwrap();
            assert_eq!(val, kv.value);
            if kv.version == 0 {
                assert_eq!(version, curr_version + 1);
            } else {
                assert_eq!(version, kv.version);
            }
        }
        assert!(tree
            .insert(&VariableKey::from_str("key_7").unwrap(), 1, 0, 0)
            .is_ok());
        assert!(tree.version() == curr_version + 3);
    }
}
