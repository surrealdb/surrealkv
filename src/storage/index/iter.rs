use std::collections::{Bound, VecDeque};
use std::ops::RangeBounds;
use std::rc::Rc;

use crate::storage::index::art::{Node, NodeType};
use crate::storage::index::KeyTrait;

// TODO: need to add more tests for snapshot readers
/// A structure representing a pointer for iterating over the Trie's key-value pairs.
pub struct IterationPointer<P: KeyTrait, V: Clone> {
    pub(crate) id: u64,
    root: Rc<Node<P, V>>,
}

impl<P: KeyTrait, V: Clone> IterationPointer<P, V> {
    /// Creates a new IterationPointer instance.
    ///
    /// # Arguments
    ///
    /// * `root` - The root node of the Trie.
    /// * `id` - The ID of the snapshot.
    ///
    pub fn new(root: Rc<Node<P, V>>, id: u64) -> IterationPointer<P, V> {
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

    pub fn range<'a, R>(&'a self, range: R) -> Range<P, P, V>
    where
        R: RangeBounds<P> + 'a,
    {
        let iter = self.iter();
        return Range::for_iter(iter, range.end_bound().cloned());
    }
}

/// An iterator over the nodes in the Trie.
struct NodeIter<'a, P: KeyTrait, V: Clone> {
    node: Box<dyn Iterator<Item = (u8, &'a Rc<Node<P, V>>)> + 'a>,
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
        I: Iterator<Item = (u8, &'a Rc<Node<P, V>>)> + 'a,
    {
        Self {
            node: Box::new(iter),
        }
    }
}

impl<'a, P: KeyTrait, V: Clone> Iterator for NodeIter<'a, P, V> {
    type Item = (u8, &'a Rc<Node<P, V>>);

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
    pub(crate) fn new(node: Option<&'a Rc<Node<P, V>>>) -> Self {
        if let Some(node) = node {
            Self {
                inner: Box::new(IterState::new(node)),
                _marker: Default::default(),
            }
        } else {
            Self {
                inner: Box::new(std::iter::empty()),
                _marker: Default::default(),
            }
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
        let leafs = VecDeque::new();

        iters.push(NodeIter::new(node.iter()));

        Self { iters, leafs }
    }
}

impl<'a, P: KeyTrait + 'a, V: Clone> Iterator for IterState<'a, P, V> {
    type Item = (Vec<u8>, &'a V, &'a u64, &'a u64);

    fn next(&mut self) -> Option<Self::Item> {
        'outer: while let Some(node) = self.iters.last_mut() {
            let e = node.next();
            loop {
                match e {
                    None => {
                        self.iters.pop().unwrap();
                        break;
                    }
                    Some(other) => {
                        if other.1.is_twig() {
                            let NodeType::Twig(twig) = &other.1.node_type else {
                                panic!("should not happen");
                            };

                            for v in twig.iter() {
                                self.leafs
                                    .push_back((&twig.key, &v.value, &v.version, &v.ts));
                            }
                            break 'outer;
                        } else {
                            self.iters.push(NodeIter::new(other.1.iter()));
                            break;
                        }
                    }
                }
            }
        }

        self.leafs
            .pop_front()
            .map(|leaf| (leaf.0.as_slice().to_vec(), leaf.1, leaf.2, leaf.3))
    }
}

/// An enum representing the result of a range operation.
enum RangeResult<'a, V: Clone> {
    Yield(Option<(Vec<u8>, &'a V, &'a u64, &'a u64)>),
}

/// An iterator for the Range operation.
struct RangeIterator<'a, K: KeyTrait + 'a, P: KeyTrait, V: Clone> {
    iter: Iter<'a, P, V>,
    end_bound: Bound<K>,
    _marker: std::marker::PhantomData<P>,
}

struct EmptyRangeIterator;

trait RangeIteratorTrait<'a, K: KeyTrait + 'a, P: KeyTrait, V: Clone> {
    fn next(&mut self) -> RangeResult<'a, V>;
}

pub struct Range<'a, K: KeyTrait + 'a, P: KeyTrait, V: Clone> {
    inner: Box<dyn RangeIteratorTrait<'a, K, P, V> + 'a>,
}

impl<'a, K: KeyTrait + 'a, P: KeyTrait, V: Clone> RangeIteratorTrait<'a, K, P, V>
    for EmptyRangeIterator
{
    fn next(&mut self) -> RangeResult<'a, V> {
        RangeResult::Yield(None)
    }
}

impl<'a, K: KeyTrait, P: KeyTrait, V: Clone> RangeIterator<'a, K, P, V> {
    pub fn new(iter: Iter<'a, P, V>, end_bound: Bound<K>) -> Self {
        Self {
            iter,
            end_bound,
            _marker: Default::default(),
        }
    }
}

impl<'a, K: KeyTrait + 'a, P: KeyTrait, V: Clone> RangeIteratorTrait<'a, K, P, V>
    for RangeIterator<'a, K, P, V>
{
    fn next(&mut self) -> RangeResult<'a, V> {
        let next_item = self.iter.next();
        match next_item {
            Some((key, value, version, ts)) => {
                let next_key_slice = key.as_slice();
                match &self.end_bound {
                    Bound::Included(k) if next_key_slice == k.as_slice() => {
                        RangeResult::Yield(Some((key, value, version, ts)))
                    }
                    Bound::Excluded(k) if next_key_slice == k.as_slice() => {
                        RangeResult::Yield(None)
                    }
                    Bound::Unbounded => RangeResult::Yield(Some((key, value, version, ts))),
                    _ => RangeResult::Yield(Some((key, value, version, ts))),
                }
            }
            None => RangeResult::Yield(None),
        }
    }
}

impl<'a, K: KeyTrait, P: KeyTrait + 'a, V: Clone + 'a> Iterator for Range<'a, K, P, V> {
    type Item = (Vec<u8>, &'a V, &'a u64, &'a u64);

    fn next(&mut self) -> Option<(Vec<u8>, &'a V, &'a u64, &'a u64)> {
        match self.inner.next() {
            RangeResult::Yield(item) => item,
        }
    }
}

impl<'a, K: KeyTrait + 'a, P: KeyTrait + 'a, V: Clone> Range<'a, K, P, V> {
    pub fn empty() -> Self {
        Self {
            inner: Box::new(EmptyRangeIterator),
        }
    }

    pub fn for_iter(iter: Iter<'a, P, V>, end_bound: Bound<K>) -> Self {
        Self {
            inner: Box::new(RangeIterator::new(iter, end_bound)),
        }
    }
}
