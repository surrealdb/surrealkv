use std::collections::{Bound, VecDeque};
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::storage::index::art::{Node, NodeType};
use crate::storage::index::KeyTrait;

// TODO: need to add more tests for snapshot readers
/// A structure representing a pointer for iterating over the Trie's key-value pairs.
pub struct IterationPointer<P: KeyTrait, V: Clone> {
    pub(crate) id: u64,
    root: Arc<Node<P, V>>,
}

impl<P: KeyTrait, V: Clone> IterationPointer<P, V> {
    /// Creates a new IterationPointer instance.
    ///
    /// # Arguments
    ///
    /// * `root` - The root node of the Trie.
    /// * `id` - The ID of the snapshot.
    ///
    pub fn new(root: Arc<Node<P, V>>, id: u64) -> IterationPointer<P, V> {
        IterationPointer { id, root }
    }

    /// Returns an iterator over the key-value pairs within the Trie.
    ///
    /// # Returns
    ///
    /// Returns an Iter iterator instance.
    ///
    pub fn iter(&self) -> Iter<P, V> {
        Iter::new(Some(&self.root))
    }

    pub fn range<'a, R>(
        &'a self,
        range: R,
    ) -> impl Iterator<Item = (Vec<u8>, &'a V, &'a u64, &'a u64)>
    where
        R: RangeBounds<P> + 'a,
    {
        Range::new(Some(&self.root), range)
    }
}

/// An iterator over the nodes in the Trie.
struct NodeIter<'a, P: KeyTrait, V: Clone> {
    node: Box<dyn Iterator<Item = (u8, &'a Arc<Node<P, V>>)> + 'a>,
}

impl<'a, P: KeyTrait, V: Clone> NodeIter<'a, P, V> {
    /// Creates a new NodeIter instance.
    ///
    /// # Arguments
    ///
    /// * `iter` - An iterator over node items.
    ///
    fn new<I>(iter: I) -> Self
    where
        I: Iterator<Item = (u8, &'a Arc<Node<P, V>>)> + 'a,
    {
        Self {
            node: Box::new(iter),
        }
    }
}

impl<'a, P: KeyTrait, V: Clone> Iterator for NodeIter<'a, P, V> {
    type Item = (u8, &'a Arc<Node<P, V>>);

    fn next(&mut self) -> Option<Self::Item> {
        self.node.next()
    }
}

/// An iterator over key-value pairs in the Trie.
pub struct Iter<'a, P: KeyTrait + 'a, V: Clone> {
    inner: Box<dyn Iterator<Item = (Vec<u8>, &'a V, &'a u64, &'a u64)> + 'a>,
    _marker: std::marker::PhantomData<P>,
}

impl<'a, P: KeyTrait + 'a, V: Clone> Iter<'a, P, V> {
    /// Creates a new Iter instance.
    ///
    /// # Arguments
    ///
    /// * `node` - An optional reference to the root node of the Trie.
    ///
    pub(crate) fn new(node: Option<&'a Arc<Node<P, V>>>) -> Self {
        match node {
            Some(node) => Self {
                inner: Box::new(IterState::new(node)),
                _marker: Default::default(),
            },
            None => Self {
                inner: Box::new(std::iter::empty()),
                _marker: Default::default(),
            },
        }
    }
}

impl<'a, P: KeyTrait + 'a, V: Clone> Iterator for Iter<'a, P, V> {
    type Item = (Vec<u8>, &'a V, &'a u64, &'a u64);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// An internal state for the Iter iterator.
struct IterState<'a, P: KeyTrait + 'a, V: Clone> {
    iters: Vec<NodeIter<'a, P, V>>,
    leafs: VecDeque<(&'a P, &'a V, &'a u64, &'a u64)>,
}

impl<'a, P: KeyTrait + 'a, V: Clone> IterState<'a, P, V> {
    /// Creates a new IterState instance.
    ///
    /// # Arguments
    ///
    /// * `node` - A reference to the root node of the Trie.
    ///
    pub fn new(node: &'a Node<P, V>) -> Self {
        let mut iters = Vec::new();
        let mut leafs = VecDeque::new();

        if let NodeType::Twig(twig) = &node.node_type {
            let val = twig.get_latest_leaf();
            if let Some(v) = val {
                leafs.push_back((&twig.key, &v.value, &v.version, &v.ts));
            }
        } else {
            iters.push(NodeIter::new(node.iter()));
        }

        Self { iters, leafs }
    }

    pub fn empty() -> Self {
        Self {
            iters: Vec::new(),
            leafs: VecDeque::new(),
        }
    }

    fn forward_scan<R>(node: &'a Node<P, V>, range: &R) -> Self
    where
        R: RangeBounds<P>,
    {
        let mut leafs = VecDeque::new();
        let mut iters = Vec::new();
        if let NodeType::Twig(twig) = &node.node_type {
            if range.contains(&twig.key) {
                let val = twig.get_latest_leaf();
                if let Some(v) = val {
                    leafs.push_back((&twig.key, &v.value, &v.version, &v.ts));
                }
            }
        } else {
            iters.push(NodeIter::new(node.iter()));
        }

        Self { iters, leafs }
    }
}

impl<'a, P: KeyTrait + 'a, V: Clone> Iterator for IterState<'a, P, V> {
    type Item = (Vec<u8>, &'a V, &'a u64, &'a u64);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(node) = self.iters.last_mut() {
            let e = node.next();
            match e {
                None => {
                    self.iters.pop();
                }
                Some(other) => {
                    if let NodeType::Twig(twig) = &other.1.node_type {
                        let val = twig.get_latest_leaf();
                        if let Some(v) = val {
                            self.leafs
                                .push_back((&twig.key, &v.value, &v.version, &v.ts));
                        }
                        break;
                    } else {
                        self.iters.push(NodeIter::new(other.1.iter()));
                    }
                }
            }
        }

        self.leafs
            .pop_front()
            .map(|leaf| (leaf.0.as_slice().to_vec(), leaf.1, leaf.2, leaf.3))
    }
}

pub struct Range<'a, K: KeyTrait, V: Clone, R> {
    forward: IterState<'a, K, V>,
    range: R,
}

impl<'a, K: KeyTrait, V: Clone, R> Range<'a, K, V, R>
where
    K: Ord,
    R: RangeBounds<K>,
{
    pub(crate) fn empty(range: R) -> Self {
        Self {
            forward: IterState::empty(),
            range,
        }
    }

    pub(crate) fn new(node: Option<&'a Arc<Node<K, V>>>, range: R) -> Self
    where
        R: RangeBounds<K>,
    {
        if let Some(node) = node {
            Self {
                forward: IterState::forward_scan(node, &range),
                range,
            }
        } else {
            Self {
                forward: IterState::empty(),
                range,
            }
        }
    }
}

impl<'a, K: 'a + KeyTrait, V: Clone, R: RangeBounds<K>> Iterator for Range<'a, K, V, R> {
    type Item = (Vec<u8>, &'a V, &'a u64, &'a u64);

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(node) = self.forward.iters.last_mut() {
            let e = node.next();
            match e {
                Some(other) => {
                    if let NodeType::Twig(twig) = &other.1.node_type {
                        if self.range.contains(&twig.key) {
                            let val = twig.get_latest_leaf();
                            if let Some(v) = val {
                                self.forward
                                    .leafs
                                    .push_back((&twig.key, &v.value, &v.version, &v.ts));
                            }
                            break;
                        } else {
                            match self.range.end_bound() {
                                Bound::Included(k) if &twig.key > k => self.forward.iters.clear(),
                                Bound::Excluded(k) if &twig.key >= k => self.forward.iters.clear(),
                                _ => {}
                            }
                        }
                    } else {
                        self.forward.iters.push(NodeIter::new(other.1.iter()));
                    }
                }
                None => {
                    self.forward.iters.pop();
                }
            }
        }

        self.forward
            .leafs
            .pop_front()
            .map(|leaf| (leaf.0.as_slice().to_vec(), leaf.1, leaf.2, leaf.3))
    }
}
