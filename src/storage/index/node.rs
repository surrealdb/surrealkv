use std::sync::Arc;

use crate::storage::index::{BitArray, KeyTrait};

/*
    Immutable nodes
*/

pub trait NodeTrait<N> {
    fn clone(&self) -> Self;
    fn add_child(&self, key: u8, node: N) -> Self;
    fn find_child(&self, key: u8) -> Option<&Arc<N>>;
    fn delete_child(&self, key: u8) -> Self;
    fn num_children(&self) -> usize;
    fn size(&self) -> usize;
    fn replace_child(&self, key: u8, node: Arc<N>) -> Self;
}

pub trait Version {
    fn version(&self) -> u64;
}

#[derive(Clone)]
pub struct TwigNode<K: KeyTrait + Clone, V> {
    pub(crate) prefix: K,
    pub(crate) key: K,
    pub(crate) values: Vec<Arc<LeafValue<V>>>,
    pub(crate) version: u64, // Version for the twig node
}

#[derive(Copy, Clone)]
pub struct LeafValue<V> {
    pub(crate) value: V,
    pub(crate) version: u64,
    pub(crate) ts: u64,
}

impl<V> LeafValue<V> {
    pub fn new(value: V, version: u64, ts: u64) -> Self {
        LeafValue { value, version, ts }
    }
}

impl<K: KeyTrait + Clone, V> TwigNode<K, V> {
    pub fn new(prefix: K, key: K) -> Self {
        TwigNode {
            prefix,
            key,
            values: Vec::new(),
            version: 0,
        }
    }

    pub fn version(&self) -> u64 {
        self.values
            .iter()
            .map(|value| value.version)
            .max()
            .unwrap_or(self.version)
    }

    pub fn insert(&self, value: V, version: u64, ts: u64) -> TwigNode<K, V> {
        let mut new_values = self.values.clone();

        let new_leaf_value = LeafValue::new(value, version, ts);

        // Insert new LeafValue in sorted order
        let insertion_index =
            match new_values.binary_search_by(|v| v.version.cmp(&new_leaf_value.version)) {
                Ok(index) => index,
                Err(index) => index,
            };
        new_values.insert(insertion_index, Arc::new(new_leaf_value));

        let new_version = new_values
            .iter()
            .map(|value| value.version)
            .max()
            .unwrap_or(self.version);

        TwigNode {
            prefix: self.prefix.clone(),
            key: self.key.clone(),
            values: new_values,
            version: new_version,
        }
    }

    pub fn insert_mut(&mut self, value: V, version: u64, ts: u64) {
        let new_leaf_value = LeafValue::new(value, version, ts);

        // Insert new LeafValue in sorted order
        let insertion_index = match self
            .values
            .binary_search_by(|v| v.version.cmp(&new_leaf_value.version))
        {
            Ok(index) => index,
            Err(index) => index,
        };
        self.values
            .insert(insertion_index, Arc::new(new_leaf_value));

        self.version = self.version(); // Update LeafNode's version
    }

    pub fn get_latest_leaf(&self) -> Option<&Arc<LeafValue<V>>> {
        self.values.iter().max_by_key(|value| value.version)
    }

    pub fn get_latest_value(&self) -> Option<&V> {
        self.values
            .iter()
            .max_by_key(|value| value.version)
            .map(|value| &value.value)
    }

    pub fn get_leaf_by_version(&self, version: u64) -> Option<Arc<LeafValue<V>>> {
        self.values
            .iter()
            .filter(|value| value.version <= version)
            .max_by_key(|value| value.version)
            .cloned()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Arc<LeafValue<V>>> {
        self.values.iter()
    }
}

impl<K: KeyTrait + Clone, V> Version for TwigNode<K, V> {
    fn version(&self) -> u64 {
        self.version
    }
}

// Source: https://www.the-paper-trail.org/post/art-paper-notes/
//
// Node4: For nodes with up to four children, ART stores all the keys in a list,
// and the child pointers in a parallel list. Looking up the next character
// in a string means searching the list of child keys, and then using the
// index to look up the corresponding pointer.
//
// Node16: Keys in a Node16 are stored sorted, so binary search could be used to
// find a particular key. Nodes with from 5 to 16 children have an identical layout
// to Node4, just with 16 children per node
//
// A FlatNode is a node with a fixed number of children. It is used for nodes with
// more than 16 children. The children are stored in a fixed-size array, and the
// keys are stored in a parallel array. The keys are stored in sorted order, so
// binary search can be used to find a particular key. The FlatNode is used for
// storing Node4 and Node16 since they have identical layouts.
pub struct FlatNode<P: KeyTrait + Clone, N: Version, const WIDTH: usize> {
    pub(crate) prefix: P,
    pub(crate) version: u64,
    keys: [u8; WIDTH],
    children: Box<[Option<Arc<N>>; WIDTH]>,
    num_children: u8,
}

impl<P: KeyTrait + Clone, N: Version, const WIDTH: usize> FlatNode<P, N, WIDTH> {
    pub fn new(prefix: P) -> Self {
        let children: [Option<Arc<N>>; WIDTH] = std::array::from_fn(|_| None);

        Self {
            prefix,
            version: 0,
            keys: [0; WIDTH],
            children: Box::new(children),
            num_children: 0,
        }
    }

    fn find_pos(&self, key: u8) -> Option<usize> {
        let idx = (0..self.num_children as usize)
            .rev()
            .find(|&i| key < self.keys[i]);
        idx.or(Some(self.num_children as usize))
    }

    fn index(&self, key: u8) -> Option<usize> {
        self.keys[..std::cmp::min(WIDTH, self.num_children as usize)]
            .iter()
            .position(|&c| key == c)
    }

    pub fn resize<const NEW_WIDTH: usize>(&self) -> FlatNode<P, N, NEW_WIDTH> {
        let mut new_node = FlatNode::<P, N, NEW_WIDTH>::new(self.prefix.clone());
        for i in 0..self.num_children as usize {
            new_node.keys[i] = self.keys[i];
            new_node.children[i] = self.children[i].clone();
        }
        new_node.version = self.version;
        new_node.num_children = self.num_children;
        new_node.update_version();
        new_node
    }

    pub fn grow(&self) -> Node48<P, N> {
        let mut n48 = Node48::new(self.prefix.clone());
        for i in 0..self.num_children as usize {
            if let Some(child) = self.children[i].as_ref() {
                n48.insert_child(self.keys[i], child.clone());
            }
        }
        n48.update_version();
        n48
    }

    // Helper function to insert a child node at the specified position
    #[inline]
    fn insert_child(&mut self, idx: usize, key: u8, node: Arc<N>) {
        for i in (idx..self.num_children as usize).rev() {
            self.keys[i + 1] = self.keys[i];
            self.children[i + 1] = std::mem::replace(&mut self.children[i], None);
        }
        self.keys[idx] = key;
        self.children[idx] = Some(node);
        self.num_children += 1;
    }

    #[inline]
    fn max_child_version(&self) -> u64 {
        self.children.iter().fold(0, |acc, x| {
            if let Some(child) = x.as_ref() {
                std::cmp::max(acc, child.version())
            } else {
                acc
            }
        })
    }

    #[inline]
    fn update_version_to_max_child_version(&mut self) {
        self.version = self.max_child_version();
    }

    #[inline]
    fn update_version(&mut self) {
        // Compute the maximum version among all children
        let max_child_version = self.max_child_version();

        // If self.version is less than the maximum child version, update it.
        if self.version < max_child_version {
            self.version = max_child_version;
        }
    }

    #[inline]
    fn update_if_newer(&mut self, new_version: u64) {
        if new_version > self.version {
            self.version = new_version;
        }
    }

    #[inline]
    pub(crate) fn iter(&self) -> impl Iterator<Item = (u8, &Arc<N>)> {
        self.keys
            .iter()
            .zip(self.children.iter())
            .take(self.num_children as usize)
            .filter_map(|(&k, c)| {
                if let Some(child) = c {
                    Some((k, child))
                } else {
                    None
                }
            })
    }
}

impl<P: KeyTrait + Clone, N: Version, const WIDTH: usize> NodeTrait<N> for FlatNode<P, N, WIDTH> {
    fn clone(&self) -> Self {
        let mut new_node = Self::new(self.prefix.clone());
        for i in 0..self.num_children as usize {
            new_node.keys[i] = self.keys[i];
            new_node.children[i] =self.children[i].clone();
        }
        new_node.num_children = self.num_children;
        new_node.version = self.version;
        new_node
    }

    fn replace_child(&self, key: u8, node: Arc<N>) -> Self {
        let mut new_node = self.clone();
        let idx = new_node.index(key).unwrap();
        new_node.keys[idx] = key;
        new_node.children[idx] = Some(node);
        new_node.update_version_to_max_child_version();

        new_node
    }

    fn add_child(&self, key: u8, node: N) -> Self {
        let mut new_node = self.clone();
        let idx = self.find_pos(key).expect("node is full");

        // Update the version if the new child has a greater version
        new_node.update_if_newer(node.version());

        // Convert the node to Arc<N> and insert it
        new_node.insert_child(idx, key, Arc::new(node));
        new_node
    }

    fn find_child(&self, key: u8) -> Option<&Arc<N>> {
        let idx = self.index(key)?;
        let child = self.children[idx].as_ref();
        child
    }

    fn delete_child(&self, key: u8) -> Self {
        let mut new_node = self.clone();
        let idx = self
            .keys
            .iter()
            .take(self.num_children as usize)
            .position(|&k| k == key)
            .unwrap();
        new_node.children[idx] = None;
        for i in idx..(WIDTH - 1) {
            new_node.keys[i] = self.keys[i + 1];
            new_node.children[i] = self.children[i + 1].clone();
        }

        new_node.keys[WIDTH - 1] = 0;
        new_node.children[WIDTH - 1] = None;
        new_node.num_children -= 1;
        new_node.update_version_to_max_child_version();

        new_node
    }

    #[inline(always)]
    fn num_children(&self) -> usize {
        self.num_children as usize
    }

    #[inline(always)]
    fn size(&self) -> usize {
        WIDTH
    }
}

impl<P: KeyTrait + Clone, N: Version, const WIDTH: usize> Version for FlatNode<P, N, WIDTH> {
    fn version(&self) -> u64 {
        self.version
    }
}

impl<P: KeyTrait + Clone, N: Version, const WIDTH: usize> Drop for FlatNode<P, N, WIDTH> {
    fn drop(&mut self) {
        for value in &mut self.children[..self.num_children as usize] {
            value.take();
        }
        self.num_children = 0;
    }
}

// Source: https://www.the-paper-trail.org/post/art-paper-notes/
//
// Node48: It can hold up to three times as many keys as a Node16. As the paper says,
// when there are more than 16 children, searching for the key can become expensive,
// so instead the keys are stored implicitly in an array of 256 indexes. The entries
// in that array index a separate array of up to 48 pointers.
//
// A Node48 is a 256-entry array of pointers to children. The pointers are stored in
// a Vector Array, which is a Vector of length WIDTH (48) that stores the pointers.

pub struct Node48<P: KeyTrait + Clone, N: Version> {
    pub(crate) prefix: P,
    pub(crate) version: u64,
    keys: BitArray<u8, 256>,
    children: BitArray<Arc<N>, 48>,
    num_children: u8,
}

impl<P: KeyTrait + Clone, N: Version> Node48<P, N> {
    pub fn new(prefix: P) -> Self {
        Self {
            prefix,
            version: 0,
            keys: BitArray::new(),
            children: BitArray::new(),
            num_children: 0,
        }
    }

    pub fn insert_child(&mut self, key: u8, node: Arc<N>) {
        let pos = self.children.first_free_pos().unwrap();
        assert!(pos < 48);

        self.keys.set(key as usize, pos as u8);
        self.children.set(pos, node);
        self.num_children += 1;
    }

    pub fn shrink<const NEW_WIDTH: usize>(&self) -> FlatNode<P, N, NEW_WIDTH> {
        let mut fnode = FlatNode::new(self.prefix.clone());
        for (key, pos) in self.keys.iter() {
            let child = self.children.get(*pos as usize).unwrap().clone();
            let idx = fnode.find_pos(key as u8).expect("node is full");
            fnode.insert_child(idx, key as u8, child);
        }
        fnode.update_version();
        fnode
    }

    pub fn grow(&self) -> Node256<P, N> {
        let mut n256 = Node256::new(self.prefix.clone());
        for (key, pos) in self.keys.iter() {
            let child = self.children.get(*pos as usize).unwrap().clone();
            n256.insert_child(key as u8, child);
        }
        n256.update_version();
        n256
    }

    #[inline]
    fn max_child_version(&self) -> u64 {
        self.children
            .iter()
            .fold(0, |acc, x| std::cmp::max(acc, x.1.version()))
    }

    #[inline]
    fn update_version_to_max_child_version(&mut self) {
        self.version = self.max_child_version();
    }

    #[inline]
    fn update_version(&mut self) {
        // Compute the maximum version among all children
        let max_child_version = self.max_child_version();

        // If self.version is less than the maximum child version, update it.
        if self.version < max_child_version {
            self.version = max_child_version;
        }
    }

    #[inline]
    fn update_if_newer(&mut self, new_version: u64) {
        if new_version > self.version {
            self.version = new_version;
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (u8, &Arc<N>)> {
        self.keys
            .iter()
            .map(move |(key, pos)| (key as u8, self.children.get(*pos as usize).unwrap()))
    }
}

impl<P: KeyTrait + Clone, N: Version> NodeTrait<N> for Node48<P, N> {
    fn clone(&self) -> Self {
        Node48 {
            prefix: self.prefix.clone(),
            version: self.version,
            keys: self.keys.clone(),
            children: self.children.clone(),
            num_children: self.num_children,
        }
    }

    fn replace_child(&self, key: u8, node: Arc<N>) -> Self {
        let mut new_node = self.clone();
        let idx = new_node.keys.get(key as usize).unwrap();
        new_node.children.set(*idx as usize, node);
        new_node.update_version_to_max_child_version();

        new_node
    }

    fn add_child(&self, key: u8, node: N) -> Self {
        let mut new_node = self.clone();

        // Update the version if the new child has a greater version
        new_node.update_if_newer(node.version());

        new_node.insert_child(key, Arc::new(node));
        new_node
    }

    fn delete_child(&self, key: u8) -> Self {
        let pos = self.keys.get(key as usize).unwrap();
        let mut new_node = self.clone();
        new_node.keys.erase(key as usize);
        new_node.children.erase(*pos as usize);
        new_node.num_children -= 1;

        new_node.update_version_to_max_child_version();
        new_node
    }

    fn find_child(&self, key: u8) -> Option<&Arc<N>> {
        let idx = self.keys.get(key as usize)?;
        let child = self.children.get(*idx as usize)?;
        Some(child)
    }

    fn num_children(&self) -> usize {
        self.num_children as usize
    }

    #[inline(always)]
    fn size(&self) -> usize {
        48
    }
}

impl<P: KeyTrait + Clone, N: Version> Version for Node48<P, N> {
    fn version(&self) -> u64 {
        self.version
    }
}

impl<P: KeyTrait + Clone, N: Version> Drop for Node48<P, N> {
    fn drop(&mut self) {
        self.num_children = 0;
        self.keys.clear();
        self.children.clear();
    }
}

// Source: https://www.the-paper-trail.org/post/art-paper-notes/
//
// Node256: It is the traditional trie node, used when a node has
// between 49 and 256 children. Looking up child pointers is obviously
// very efficient - the most efficient of all the node types - and when
// occupancy is at least 49 children the wasted space is less significant.
//
// A Node256 is a 256-entry array of pointers to children. The pointers are stored in
// a Vector Array, which is a Vector of length WIDTH (256) that stores the pointers.
pub struct Node256<P: KeyTrait + Clone, N: Version> {
    pub(crate) prefix: P,    // Prefix associated with the node
    pub(crate) version: u64, // Version for node256

    children: BitArray<Arc<N>, 256>,
    num_children: usize,
}

impl<P: KeyTrait + Clone, N: Version> Node256<P, N> {
    pub fn new(prefix: P) -> Self {
        Self {
            prefix,
            version: 0,
            children: BitArray::new(),
            num_children: 0,
        }
    }

    pub fn shrink(&self) -> Node48<P, N> {
        let mut indexed = Node48::new(self.prefix.clone());
        let keys: Vec<usize> = self.children.iter_keys().collect();
        for key in keys {
            let child = self.children.get(key).unwrap().clone();
            indexed.insert_child(key as u8, child);
        }
        indexed.update_version();
        indexed
    }

    #[inline]
    fn insert_child(&mut self, key: u8, node: Arc<N>) {
        self.children.set(key as usize, node);
        self.num_children += 1;
    }

    #[inline]
    fn max_child_version(&self) -> u64 {
        self.children
            .iter()
            .fold(0, |acc, x| std::cmp::max(acc, x.1.version()))
    }

    #[inline]
    fn update_version_to_max_child_version(&mut self) {
        self.version = self.max_child_version();
    }

    #[inline]
    fn update_version(&mut self) {
        // Compute the maximum version among all children
        let max_child_version = self.max_child_version();

        // If self.version is less than the maximum child version, update it.
        if self.version < max_child_version {
            self.version = max_child_version;
        }
    }

    #[inline]
    fn update_if_newer(&mut self, new_version: u64) {
        if new_version > self.version {
            self.version = new_version;
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (u8, &Arc<N>)> {
        self.children.iter().map(|(key, node)| (key as u8, node))
    }
}

impl<P: KeyTrait + Clone, N: Version> NodeTrait<N> for Node256<P, N> {
    fn clone(&self) -> Self {
        Self {
            prefix: self.prefix.clone(),
            version: self.version,
            children: self.children.clone(),
            num_children: self.num_children,
        }
    }

    fn replace_child(&self, key: u8, node: Arc<N>) -> Self {
        let mut new_node = self.clone();

        new_node.children.set(key as usize, node);
        new_node.update_version_to_max_child_version();
        new_node
    }

    #[inline]
    fn add_child(&self, key: u8, node: N) -> Self {
        let mut new_node = self.clone();

        // Update the version if the new child has a greater version
        new_node.update_if_newer(node.version());

        new_node.insert_child(key, Arc::new(node));
        new_node
    }

    #[inline]
    fn find_child(&self, key: u8) -> Option<&Arc<N>> {
        let child = self.children.get(key as usize)?;
        Some(child)
    }

    #[inline]
    fn delete_child(&self, key: u8) -> Self {
        let mut new_node = self.clone();
        let removed = new_node.children.erase(key as usize);
        if removed.is_some() {
            new_node.num_children -= 1;
        }
        new_node.update_version_to_max_child_version();
        new_node
    }

    #[inline]
    fn num_children(&self) -> usize {
        self.num_children
    }

    #[inline(always)]
    fn size(&self) -> usize {
        256
    }
}

impl<P: KeyTrait + Clone, N: Version> Version for Node256<P, N> {
    fn version(&self) -> u64 {
        self.version
    }
}

impl<P: KeyTrait + Clone, N: Version> Drop for Node256<P, N> {
    fn drop(&mut self) {
        self.num_children = 0;
        self.children.clear();
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::index::FixedKey;

    use super::{FlatNode, Node256, Node48, NodeTrait, TwigNode, Version};
    use std::sync::Arc;

    macro_rules! impl_timestamp {
        ($($t:ty),*) => {
            $(
                impl Version for $t {
                    fn version(&self) -> u64 {
                        *self as u64
                    }
                }
            )*
        };
    }

    impl_timestamp!(usize, u8, u16, u32, u64);

    fn node_test<N: NodeTrait<usize>>(mut node: N, size: usize) {
        for i in 0..size {
            node = node.add_child(i as u8, i);
        }

        for i in 0..size {
            assert!(matches!(node.find_child(i as u8), Some(v) if *v == i.into()));
        }

        for i in 0..size {
            node = node.delete_child(i as u8);
        }

        assert_eq!(node.num_children(), 0);
    }

    #[test]
    fn flatnode() {
        let dummy_prefix: FixedKey<8> = FixedKey::create_key("foo".as_bytes());

        node_test(
            FlatNode::<FixedKey<8>, usize, 4>::new(dummy_prefix.clone()),
            4,
        );
        node_test(
            FlatNode::<FixedKey<8>, usize, 16>::new(dummy_prefix.clone()),
            16,
        );
        node_test(
            FlatNode::<FixedKey<8>, usize, 32>::new(dummy_prefix.clone()),
            32,
        );
        node_test(
            FlatNode::<FixedKey<8>, usize, 48>::new(dummy_prefix.clone()),
            48,
        );
        node_test(
            FlatNode::<FixedKey<8>, usize, 64>::new(dummy_prefix.clone()),
            64,
        );

        // Resize from 16 to 4
        let mut node = FlatNode::<FixedKey<8>, usize, 16>::new(dummy_prefix.clone());
        for i in 0..4 {
            node = node.add_child(i as u8, i);
        }

        let resized: FlatNode<FixedKey<8>, usize, 4> = node.resize();
        assert_eq!(resized.num_children, 4);
        for i in 0..4 {
            assert!(matches!(resized.find_child(i as u8), Some(v) if *v == i.into()));
        }

        // Resize from 4 to 16
        let mut node = FlatNode::<FixedKey<8>, usize, 4>::new(dummy_prefix.clone());
        for i in 0..4 {
            node = node.add_child(i as u8, i);
        }
        let mut resized: FlatNode<FixedKey<8>, usize, 16> = node.resize();
        assert_eq!(resized.num_children, 4);
        for i in 4..16 {
            resized = resized.add_child(i as u8, i);
        }
        assert_eq!(resized.num_children, 16);
        for i in 0..16 {
            assert!(matches!(resized.find_child(i as u8), Some(v) if *v == i.into()));
        }

        // Resize from 16 to 48
        let mut node = FlatNode::<FixedKey<8>, usize, 16>::new(dummy_prefix.clone());
        for i in 0..16 {
            node = node.add_child(i as u8, i);
        }

        let resized = node.grow();
        assert_eq!(resized.num_children, 16);
        for i in 0..16 {
            assert!(matches!(resized.find_child(i as u8), Some(v) if *v == i.into()));
        }

        // Additional test for adding and deleting children
        let mut node = FlatNode::<FixedKey<8>, usize, 4>::new(dummy_prefix);
        node = node.add_child(1, 1);
        node = node.add_child(2, 2);
        node = node.add_child(3, 3);
        node = node.add_child(4, 4);
        assert_eq!(node.num_children(), 4);
        assert_eq!(node.find_child(1), Some(&1.into()));
        assert_eq!(node.find_child(2), Some(&2.into()));
        assert_eq!(node.find_child(3), Some(&3.into()));
        assert_eq!(node.find_child(4), Some(&4.into()));
        assert_eq!(node.find_child(5), None);

        node = node.delete_child(1);
        node = node.delete_child(2);
        node = node.delete_child(3);
        node = node.delete_child(4);
        assert_eq!(node.num_children(), 0);
    }

    #[test]
    fn node48() {
        let dummy_prefix: FixedKey<8> = FixedKey::create_key("foo".as_bytes());

        // Create and test Node48
        let mut n48 = Node48::<FixedKey<8>, u8>::new(dummy_prefix.clone());
        for i in 0..48 {
            n48 = n48.add_child(i, i);
        }
        for i in 0..48 {
            assert_eq!(n48.find_child(i), Some(&i.into()));
        }
        for i in 0..48 {
            n48 = n48.delete_child(i);
        }
        for i in 0..48 {
            assert!(n48.find_child(i as u8).is_none());
        }

        // Resize from 48 to 16
        let mut node = Node48::<FixedKey<8>, u8>::new(dummy_prefix.clone());
        for i in 0..18 {
            node = node.add_child(i, i);
        }
        assert_eq!(node.num_children, 18);
        node = node.delete_child(0);
        node = node.delete_child(1);
        assert_eq!(node.num_children, 16);

        let resized = node.shrink::<16>();
        assert_eq!(resized.num_children, 16);
        for i in 2..18 {
            assert!(matches!(resized.find_child(i), Some(v) if *v == i.into()));
        }

        // Resize from 48 to 4
        let mut node = Node48::<FixedKey<8>, u8>::new(dummy_prefix.clone());
        for i in 0..4 {
            node = node.add_child(i, i);
        }
        let resized = node.shrink::<4>();
        assert_eq!(resized.num_children, 4);
        for i in 0..4 {
            assert!(matches!(resized.find_child(i), Some(v) if *v == i.into()));
        }

        // Resize from 48 to 256
        let mut node = Node48::<FixedKey<8>, u8>::new(dummy_prefix);
        for i in 0..48 {
            node = node.add_child(i, i);
        }

        let resized = node.grow();
        assert_eq!(resized.num_children, 48);
        for i in 0..48 {
            assert!(matches!(resized.find_child(i), Some(v) if *v == i.into()));
        }
    }

    #[test]
    fn node256() {
        let dummy_prefix: FixedKey<8> = FixedKey::create_key("foo".as_bytes());

        node_test(
            Node256::<FixedKey<8>, usize>::new(dummy_prefix.clone()),
            255,
        );

        let mut n256 = Node256::new(dummy_prefix.clone());
        for i in 0..255 {
            n256 = n256.add_child(i, i);
            assert_eq!(n256.find_child(i), Some(&i.into()));
            n256 = n256.delete_child(i);
            assert_eq!(n256.find_child(i), None);
        }

        // resize from 256 to 48
        let mut node = Node256::new(dummy_prefix);
        for i in 0..48 {
            node = node.add_child(i, i);
        }

        let resized = node.shrink();
        assert_eq!(resized.num_children, 48);
        for i in 0..48 {
            assert!(matches!(resized.find_child(i), Some(v) if *v == i.into()));
        }
    }

    #[test]
    fn flatnode_update_version() {
        const WIDTH: usize = 4;
        let dummy_prefix: FixedKey<8> = FixedKey::create_key("foo".as_bytes());

        // Prepare some child nodes
        let mut child1 = FlatNode::<FixedKey<8>, usize, WIDTH>::new(dummy_prefix.clone());
        child1.version = 5;
        let mut child2 = FlatNode::<FixedKey<8>, usize, WIDTH>::new(dummy_prefix.clone());
        child2.version = 10;
        let mut child3 = FlatNode::<FixedKey<8>, usize, WIDTH>::new(dummy_prefix.clone());
        child3.version = 3;
        let mut child4 = FlatNode::<FixedKey<8>, usize, WIDTH>::new(dummy_prefix.clone());
        child4.version = 7;

        let mut parent = FlatNode {
            prefix: dummy_prefix.clone(),
            version: 6,
            keys: [0; WIDTH],
            children: Box::new([
                Some(Arc::new(child1)),
                Some(Arc::new(child2)),
                Some(Arc::new(child3)),
                None,
            ]),
            num_children: 3,
        };
        // The maximum version among children is 10 (child2.version), so after calling update_version,
        // the parent's version should be updated to 10.
        parent.update_version();
        assert_eq!(parent.version(), 10);

        // Add a new child with a larger version (15), parent's version should update to 15
        let mut child5 = FlatNode::<FixedKey<8>, usize, WIDTH>::new(dummy_prefix.clone());
        child5.version = 15;
        parent = parent.add_child(3, child5);
        assert_eq!(parent.version(), 15);

        // Delete the child with the largest version, parent's version should update to next max (10)
        parent = parent.delete_child(3);
        assert_eq!(parent.version(), 10);

        // Update a child's version to be the largest (20), parent's version should update to 20
        let mut child6 = FlatNode::<FixedKey<8>, usize, WIDTH>::new(dummy_prefix);
        child6.version = 20;
        parent.children[2] = Some(Arc::new(child6));
        parent.update_version();
        assert_eq!(parent.version(), 20);
    }

    #[test]
    fn flatnode_repeated_update_version() {
        const WIDTH: usize = 1;
        let dummy_prefix: FixedKey<8> = FixedKey::create_key("foo".as_bytes());

        let child = FlatNode::<FixedKey<8>, usize, WIDTH>::new(dummy_prefix.clone());
        let mut parent: FlatNode<FixedKey<8>, FlatNode<FixedKey<8>, usize, 1>, 1> = FlatNode {
            prefix: dummy_prefix,
            version: 6,
            keys: [0; WIDTH],
            children: Box::new([Some(Arc::new(child))]),
            num_children: 1,
        };

        // Calling update_version once should update the version.
        parent.update_version();
        let version_after_first_update = parent.version();

        // Calling update_version again should not change the version.
        parent.update_version();
        assert_eq!(parent.version(), version_after_first_update);
    }

    #[test]
    fn node48_update_version() {
        const WIDTH: usize = 4;
        let dummy_prefix: FixedKey<8> = FixedKey::create_key("foo".as_bytes());

        // Prepare some child nodes with varying versions
        let children: Vec<_> = (0..WIDTH)
            .map(|i| {
                let mut child = FlatNode::<FixedKey<8>, usize, WIDTH>::new(dummy_prefix.clone());
                child.version = i as u64;
                child
            })
            .collect();

        let mut parent: Node48<FixedKey<8>, FlatNode<FixedKey<8>, usize, WIDTH>> =
            Node48::<FixedKey<8>, FlatNode<FixedKey<8>, usize, WIDTH>>::new(dummy_prefix);

        // Add children to parent
        for (i, child) in children.iter().enumerate() {
            parent = parent.add_child(i as u8, child.clone());
        }
        // The maximum version among children is (WIDTH - 1), so after calling update_version,
        // the parent's version should be updated to (WIDTH - 1).
        parent.update_version();
        assert_eq!(parent.version(), (WIDTH - 1) as u64);
    }

    #[test]
    fn node256_update_version() {
        const WIDTH: usize = 256;
        let dummy_prefix: FixedKey<8> = FixedKey::create_key("foo".as_bytes());

        // Prepare some child nodes with varying versions
        let children: Vec<_> = (0..WIDTH)
            .map(|i| {
                let mut child = FlatNode::<FixedKey<8>, usize, WIDTH>::new(dummy_prefix.clone());
                child.version = i as u64;
                child
            })
            .collect();

        let mut parent: Node256<FixedKey<8>, FlatNode<FixedKey<8>, usize, WIDTH>> =
            Node256::<FixedKey<8>, FlatNode<FixedKey<8>, usize, WIDTH>>::new(dummy_prefix);

        // Add children to parent
        for (i, child) in children.iter().enumerate() {
            parent = parent.add_child(i as u8, child.clone());
        }

        // The maximum version among children is (WIDTH - 1), so after calling update_version,
        // the parent's version should be updated to (WIDTH - 1).
        parent.update_version();
        assert_eq!(parent.version(), (WIDTH - 1) as u64);
    }

    // TODO: add more scenarios to this as twig nodes have the actual data with versions
    #[test]
    fn twig_nodes() {
        const WIDTH: usize = 4;
        let dummy_prefix: FixedKey<8> = FixedKey::create_key("foo".as_bytes());

        // Prepare some child nodes
        let mut twig1 =
            TwigNode::<FixedKey<8>, usize>::new(dummy_prefix.clone(), dummy_prefix.clone());
        twig1.version = 5;
        let mut twig2 =
            TwigNode::<FixedKey<8>, usize>::new(dummy_prefix.clone(), dummy_prefix.clone());
        twig2.version = 10;
        let mut twig3 =
            TwigNode::<FixedKey<8>, usize>::new(dummy_prefix.clone(), dummy_prefix.clone());
        twig3.version = 3;
        let mut twig4 =
            TwigNode::<FixedKey<8>, usize>::new(dummy_prefix.clone(), dummy_prefix.clone());
        twig4.version = 7;

        let mut parent = FlatNode {
            prefix: dummy_prefix,
            version: 0,
            keys: [0; WIDTH],
            children: Box::new([
                Some(Arc::new(twig1)),
                Some(Arc::new(twig2)),
                Some(Arc::new(twig3)),
                Some(Arc::new(twig4)),
            ]),
            num_children: 3,
        };
        // The maximum version among children is 10 (child2.version), so after calling update_version,
        // the parent's version should be updated to 10.
        parent.update_version();
        assert_eq!(parent.version(), 10);
    }

    #[test]
    fn twig_insert() {
        let dummy_prefix: FixedKey<8> = FixedKey::create_key("foo".as_bytes());

        let node = TwigNode::<FixedKey<8>, usize>::new(dummy_prefix.clone(), dummy_prefix);

        let new_node = node.insert(42, 123, 0);
        assert_eq!(node.values.len(), 0);
        assert_eq!(new_node.values.len(), 1);
        assert_eq!(new_node.values[0].value, 42);
        assert_eq!(new_node.values[0].version, 123);
    }

    #[test]
    fn twig_insert_mut() {
        let dummy_prefix: FixedKey<8> = FixedKey::create_key("foo".as_bytes());

        let mut node = TwigNode::<FixedKey<8>, usize>::new(dummy_prefix.clone(), dummy_prefix);

        node.insert_mut(42, 123, 0);
        assert_eq!(node.values.len(), 1);
        assert_eq!(node.values[0].value, 42);
        assert_eq!(node.values[0].version, 123);
    }

    #[test]
    fn twig_get_latest_leaf() {
        let dummy_prefix: FixedKey<8> = FixedKey::create_key("foo".as_bytes());
        let mut node = TwigNode::<FixedKey<8>, usize>::new(dummy_prefix.clone(), dummy_prefix);
        node.insert_mut(42, 123, 0);
        node.insert_mut(43, 124, 1);
        let latest_leaf = node.get_latest_leaf();
        assert_eq!(latest_leaf.unwrap().value, 43);
    }

    #[test]
    fn twig_get_latest_value() {
        let dummy_prefix: FixedKey<8> = FixedKey::create_key("foo".as_bytes());
        let mut node = TwigNode::<FixedKey<8>, usize>::new(dummy_prefix.clone(), dummy_prefix);
        node.insert_mut(42, 123, 0);
        node.insert_mut(43, 124, 1);
        let latest_value = node.get_latest_value();
        assert_eq!(*latest_value.unwrap(), 43);
    }

    #[test]
    fn twig_get_leaf_by_version() {
        let dummy_prefix: FixedKey<8> = FixedKey::create_key("foo".as_bytes());
        let mut node = TwigNode::<FixedKey<8>, usize>::new(dummy_prefix.clone(), dummy_prefix);
        node.insert_mut(42, 123, 0);
        node.insert_mut(43, 124, 1);
        let leaf_by_ts = node.get_leaf_by_version(123);
        assert_eq!(leaf_by_ts.unwrap().value, 42);
        let leaf_by_ts = node.get_leaf_by_version(124);
        assert_eq!(leaf_by_ts.unwrap().value, 43);
    }

    #[test]
    fn twig_iter() {
        let dummy_prefix: FixedKey<8> = FixedKey::create_key("foo".as_bytes());
        let mut node = TwigNode::<FixedKey<8>, usize>::new(dummy_prefix.clone(), dummy_prefix);
        node.insert_mut(42, 123, 0);
        node.insert_mut(43, 124, 1);
        let mut iter = node.iter();
        assert_eq!(iter.next().unwrap().value, 42);
        assert_eq!(iter.next().unwrap().value, 43);
        assert!(iter.next().is_none());
    }

    #[test]
    fn memory_leak() {
        let dummy_prefix: FixedKey<8> = FixedKey::create_key("foo".as_bytes());

        // Create and test flatnode
        let mut node = FlatNode::<FixedKey<8>, usize, 4>::new(dummy_prefix.clone());
        for i in 0..4 {
            node = node.add_child(i as u8, i);
        }

        for child in node.iter() {
            assert_eq!(Arc::strong_count(child.1), 1);
        }

        // Create and test Node48
        let mut n48 = Node48::<FixedKey<8>, u8>::new(dummy_prefix.clone());
        for i in 0..48 {
            n48 = n48.add_child(i, i);
        }

        for child in n48.iter() {
            assert_eq!(Arc::strong_count(child.1), 1);
        }

        // Create and test Node256
        let mut n256 = Node256::new(dummy_prefix);
        for i in 0..255 {
            n256 = n256.add_child(i, i);
        }

        for child in n256.iter() {
            assert_eq!(Arc::strong_count(child.1), 1);
        }
    }

    #[test]
    fn cache_line_size() {
        assert!(std::mem::size_of::<FlatNode::<FixedKey<8>, usize, 4>>() <= 64);
        assert!(std::mem::size_of::<FlatNode::<FixedKey<8>, usize, 16>>() <= 64);
    }
}
