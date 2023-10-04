// #[allow(warnings)]
pub mod art;
pub mod iter;
pub mod node;
pub mod snapshot;

use std::cmp::{Ord, Ordering, PartialOrd};
use std::fmt::Debug;

// "Partial" in the Adaptive Radix Tree paper refers to "partial keys", a technique employed
// for prefix compression in this data structure. Instead of storing entire keys in the nodes,
// ART nodes often only store partial keys, which are the differing prefixes of the keys.
// This approach significantly reduces the memory requirements of the data structure.
// Key is a trait that provides an abstraction for partial keys.
pub trait Key {
    fn at(&self, pos: usize) -> u8;
    fn len(&self) -> usize;
    fn prefix_before(&self, length: usize) -> Self;
    fn prefix_after(&self, start: usize) -> Self;
    fn longest_common_prefix(&self, slice: &[u8]) -> usize;
    fn as_slice(&self) -> &[u8];
    fn cmp(&self, other: &Self) -> Ordering;
}

pub trait KeyTrait: Key + Clone + PartialEq + Debug + for<'a> From<&'a [u8]> {}
impl<T: Key + Clone + PartialEq + Debug + for<'a> From<&'a [u8]>> KeyTrait for T {}

/*
    Key trait implementations
*/

// Source: https://www.the-paper-trail.org/post/art-paper-notes/
//
// Keys can be of two types:
// 1. Fixed-length datatypes such as 128-bit integers, or strings of exactly 64-bytes,
// don’t have any problem because there can, by construction, never be any key that’s
// a prefix of any other.
//
// 2. Variable-length datatypes such as general strings, can be transformed into types
// where no key is the prefix of any other by a simple trick: append the NULL byte to every key.
// The NULL byte, as it does in C-style strings, indicates that this is the end of the key, and
// no characters can come after it. Therefore no string with a null-byte can be a prefix of any other,
// because no string can have any characters after the NULL byte!
//
#[derive(Clone, Debug, Eq)]
pub struct ArrayKey<const SIZE: usize> {
    content: [u8; SIZE],
    len: usize,
}

impl<const SIZE: usize> PartialEq for ArrayKey<SIZE> {
    fn eq(&self, other: &Self) -> bool {
        self.content[..self.len] == other.content[..other.len]
    }
}

impl<const SIZE: usize> PartialOrd for ArrayKey<SIZE> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<const SIZE: usize> ArrayKey<SIZE> {
    // Create new instance with data ending in zero byte
    pub fn create_key(src: &[u8]) -> Self {
        assert!(src.len() < SIZE);
        let mut content = [0; SIZE];
        content[..src.len()].copy_from_slice(src);
        content[src.len()] = 0;
        Self {
            content,
            len: src.len() + 1,
        }
    }

    // Create new instance from slice
    pub fn from_slice(src: &[u8]) -> Self {
        assert!(src.len() <= SIZE);
        let mut content = [0; SIZE];
        content[..src.len()].copy_from_slice(src);
        Self {
            content,
            len: src.len(),
        }
    }

    pub fn from_str(s: &str) -> Self {
        assert!(s.len() < SIZE, "data length is greater than array length");
        let mut arr = [0; SIZE];
        arr[..s.len()].copy_from_slice(s.as_bytes());
        Self {
            content: arr,
            len: s.len() + 1,
        }
    }

    pub fn from_string(s: &String) -> Self {
        assert!(s.len() < SIZE, "data length is greater than array length");
        let mut arr = [0; SIZE];
        arr[..s.len()].copy_from_slice(s.as_bytes());
        Self {
            content: arr,
            len: s.len() + 1,
        }
    }
}

impl<const SIZE: usize> Key for ArrayKey<SIZE> {
    // Returns slice of the internal data up to the actual length
    fn as_slice(&self) -> &[u8] {
        &self.content[..self.len]
    }

    fn cmp(&self, other: &Self) -> Ordering {
        self.content[..self.len].cmp(&other.content[..other.len])
    }

    // Creates a new instance of ArrayKey consisting only of the initial part of the content
    fn prefix_before(&self, length: usize) -> Self {
        assert!(length <= self.len);
        Self::from_slice(&self.content[..length])
    }

    // Creates a new instance of ArrayKey excluding the initial part of the content
    fn prefix_after(&self, start: usize) -> Self {
        assert!(start <= self.len);
        Self::from_slice(&self.content[start..self.len])
    }

    #[inline(always)]
    fn at(&self, pos: usize) -> u8 {
        assert!(pos < self.len);
        self.content[pos]
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.len
    }

    // Returns the length of the longest common prefix between this object's content and the given byte slice
    fn longest_common_prefix(&self, key: &[u8]) -> usize {
        let len = self.len.min(key.len()).min(SIZE);
        self.content[..len]
            .iter()
            .zip(key)
            .take_while(|&(a, &b)| *a == b)
            .count()
    }
}

impl<const SIZE: usize> From<&[u8]> for ArrayKey<SIZE> {
    fn from(src: &[u8]) -> Self {
        Self::from_slice(src)
    }
}

impl<const N: usize> From<u8> for ArrayKey<N> {
    fn from(data: u8) -> Self {
        Self::from_slice(data.to_be_bytes().as_ref())
    }
}

impl<const N: usize> From<u16> for ArrayKey<N> {
    fn from(data: u16) -> Self {
        Self::from_slice(data.to_be_bytes().as_ref())
    }
}

impl<const N: usize> From<u64> for ArrayKey<N> {
    fn from(data: u64) -> Self {
        Self::from_slice(data.to_be_bytes().as_ref())
    }
}

impl<const N: usize> From<&str> for ArrayKey<N> {
    fn from(data: &str) -> Self {
        Self::from_str(data)
    }
}

impl<const N: usize> From<String> for ArrayKey<N> {
    fn from(data: String) -> Self {
        Self::from_string(&data)
    }
}
impl<const N: usize> From<&String> for ArrayKey<N> {
    fn from(data: &String) -> Self {
        Self::from_string(data)
    }
}

// A VectorKey is a variable-length datatype with NULL byte appended to it.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct VectorKey {
    data: Vec<u8>,
}

impl VectorKey {
    pub fn key(src: &[u8]) -> Self {
        let mut data = Vec::with_capacity(src.len() + 1);
        data.extend_from_slice(src);
        data.push(0);
        Self { data: data }
    }

    pub fn from_slice(src: &[u8]) -> Self {
        Self {
            data: Vec::from(src),
        }
    }

    pub fn to_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn from_string(s: &String) -> Self {
        let mut data = Vec::with_capacity(s.len() + 1);
        data.extend_from_slice(s.as_bytes());
        data.push(0);
        Self { data }
    }

    pub fn from_str(s: &str) -> Self {
        let mut data = Vec::with_capacity(s.len() + 1);
        data.extend_from_slice(s.as_bytes());
        data.push(0);
        Self { data }
    }

    pub fn from(data: Vec<u8>) -> Self {
        Self { data }
    }
}

impl From<&[u8]> for VectorKey {
    fn from(src: &[u8]) -> Self {
        Self::from_slice(src)
    }
}

impl Key for VectorKey {
    fn prefix_before(&self, length: usize) -> Self {
        assert!(length <= self.data.len());
        VectorKey::from_slice(&self.data[..length])
    }

    fn prefix_after(&self, start: usize) -> Self {
        assert!(start <= self.data.len());
        VectorKey::from_slice(&self.data[start..self.data.len()])
    }

    #[inline(always)]
    fn at(&self, pos: usize) -> u8 {
        assert!(pos < self.data.len());
        self.data[pos]
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.data.len()
    }

    // Returns the length of the longest common prefix between this object's content and the given byte slice
    fn longest_common_prefix(&self, key: &[u8]) -> usize {
        let len = self.data.len().min(key.len());
        self.data[..len]
            .iter()
            .zip(key)
            .take_while(|&(a, &b)| *a == b)
            .count()
    }

    fn cmp(&self, other: &Self) -> Ordering {
        self.data.cmp(&other.data)
    }

    fn as_slice(&self) -> &[u8] {
        &self.data[..self.data.len()]
    }
}

/*
    Sparse Array implementation
*/

/// A struct `SparseArray` is a generic wrapper over a Vector that can store elements of type `X`.
/// It has a pre-defined constant capacity `WIDTH`. The elements are stored in a `Vec<Option<X>>`,
/// allowing for storage to be dynamically allocated and deallocated.
#[derive(Clone)]
pub struct SparseArray<X, const WIDTH: usize> {
    storage: Vec<Option<X>>,
}

/// This is an implementation of the Default trait for SparseArray. It simply calls the `new` function.
impl<X, const WIDTH: usize> Default for SparseArray<X, WIDTH> {
    fn default() -> Self {
        Self::new()
    }
}

impl<X, const WIDTH: usize> SparseArray<X, WIDTH> {
    /// This function constructs a new SparseArray with a `WIDTH` number of `Option<X>` elements.
    pub fn new() -> Self {
        Self {
            storage: Vec::with_capacity(WIDTH),
        }
    }

    /// This function adds a new element `x` to the SparseArray at the first available position. If the
    /// SparseArray is full, it automatically resizes to make room for more elements. It returns the
    /// position where the element was inserted.
    pub fn push(&mut self, x: X) -> usize {
        let pos = self.first_free_pos();
        self.storage[pos] = Some(x);
        pos
    }

    /// This function removes and returns the last element in the SparseArray if it exists.
    pub fn pop(&mut self) -> Option<X> {
        self.last_used_pos()
            .and_then(|pos| self.storage[pos].take())
    }

    /// This function returns a reference to the last element in the SparseArray, if it exists.
    pub fn last(&self) -> Option<&X> {
        self.last_used_pos()
            .and_then(|pos| self.storage[pos].as_ref())
    }

    /// This function returns the position of the last used (non-None) element in the SparseArray, if it exists.
    #[inline]
    pub fn last_used_pos(&self) -> Option<usize> {
        self.storage.iter().rposition(Option::is_some)
    }

    /// This function finds the position of the first free (None) slot in the SparseArray. If all slots are filled,
    /// it expands the SparseArray and returns the position of the new slot.
    #[inline]
    pub fn first_free_pos(&mut self) -> usize {
        let pos = self.storage.iter().position(|x| x.is_none());
        match pos {
            Some(p) => p,
            None => {
                // No free position was found, so we add a new one.
                self.storage.push(None);
                self.storage.len() - 1
            }
        }
    }

    /// This function returns an `Option` containing a reference to the element at the given position, if it exists.
    #[inline]
    pub fn get(&self, pos: usize) -> Option<&X> {
        self.storage.get(pos).and_then(Option::as_ref)
    }

    /// This function returns an `Option` containing a mutable reference to the element at the given position, if it exists.
    #[inline]
    pub fn get_mut(&mut self, pos: usize) -> Option<&mut X> {
        self.storage.get_mut(pos).and_then(Option::as_mut)
    }

    /// This function sets the element at the given position to the provided value. If the position is out of bounds,
    /// it automatically resizes the SparseArray to make room for more elements.
    #[inline]
    pub fn set(&mut self, pos: usize, x: X) {
        if pos < self.storage.len() {
            self.storage[pos] = Some(x);
        } else {
            self.storage.resize_with(pos + 1, || None);
            self.storage[pos] = Some(x);
        }
    }

    /// This function removes the element at the given position from the SparseArray, returning it if it exists.
    #[inline]
    pub fn erase(&mut self, pos: usize) -> Option<X> {
        self.storage[pos].take()
    }

    /// This function clears the SparseArray, removing all elements.
    pub fn clear(&mut self) {
        self.storage.clear();
    }

    /// This function checks if the SparseArray is empty, returning `true` if it is and `false` otherwise.
    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    /// This function returns an iterator over the positions of all the used (non-None) elements in the SparseArray.
    pub fn iter_keys(&self) -> impl DoubleEndedIterator<Item = usize> + '_ {
        self.storage.iter().enumerate().filter_map(
            |(i, x)| {
                if x.is_some() {
                    Some(i)
                } else {
                    None
                }
            },
        )
    }

    /// This function returns an iterator over pairs of positions and references to all the used (non-None) elements in the SparseArray.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (usize, &X)> {
        self.storage
            .iter()
            .enumerate()
            .filter_map(|(i, x)| x.as_ref().map(|v| (i, v)))
    }
}

#[cfg(test)]
mod tests {
    use super::SparseArray;

    #[test]
    fn new() {
        let v: SparseArray<i32, 10> = SparseArray::new();
        assert_eq!(v.storage.capacity(), 10);
    }

    #[test]
    fn push_and_pop() {
        let mut v: SparseArray<i32, 10> = SparseArray::new();
        let index = v.push(5);
        assert_eq!(v.get(index), Some(&5));
        assert_eq!(v.pop(), Some(5));
    }

    #[test]
    fn last() {
        let mut v: SparseArray<i32, 10> = SparseArray::new();
        v.push(5);
        v.push(6);
        assert_eq!(v.last(), Some(&6));
    }

    #[test]
    fn last_used_pos() {
        let mut v: SparseArray<i32, 10> = SparseArray::new();
        v.push(5);
        v.push(6);
        assert_eq!(v.last_used_pos(), Some(1));
    }

    #[test]
    fn first_free_pos() {
        let mut v: SparseArray<i32, 10> = SparseArray::new();
        v.push(5);
        assert_eq!(v.first_free_pos(), 1);
    }

    #[test]
    fn get_and_set() {
        let mut v: SparseArray<i32, 10> = SparseArray::new();
        v.set(5, 6);
        assert_eq!(v.get(5), Some(&6));
    }

    #[test]
    fn get_mut() {
        let mut v: SparseArray<i32, 10> = SparseArray::new();
        v.set(5, 6);
        if let Some(value) = v.get_mut(5) {
            *value = 7;
        }
        assert_eq!(v.get(5), Some(&7));
    }

    #[test]
    fn erase() {
        let mut v: SparseArray<i32, 10> = SparseArray::new();
        v.push(5);
        assert_eq!(v.erase(0), Some(5));
        assert_eq!(v.get(0), None);
    }

    #[test]
    fn clear() {
        let mut v: SparseArray<i32, 10> = SparseArray::new();
        v.push(5);
        v.clear();
        assert!(v.is_empty());
    }

    #[test]
    fn is_empty() {
        let mut v: SparseArray<i32, 10> = SparseArray::new();
        assert!(v.is_empty());
        v.push(5);
        assert!(!v.is_empty());
    }

    #[test]
    fn iter_keys() {
        let mut v: SparseArray<i32, 10> = SparseArray::new();
        v.push(5);
        v.push(6);
        let keys: Vec<usize> = v.iter_keys().collect();
        assert_eq!(keys, vec![0, 1]);
    }

    #[test]
    fn iter() {
        let mut v: SparseArray<i32, 10> = SparseArray::new();
        v.push(5);
        v.push(6);
        let values: Vec<(usize, &i32)> = v.iter().collect();
        assert_eq!(values, vec![(0, &5), (1, &6)]);
    }
}
