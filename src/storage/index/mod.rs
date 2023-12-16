// #[allow(warnings)]
pub mod art;
pub mod iter;
pub mod node;
pub mod snapshot;

use std::cmp::{Ord, Ordering, PartialOrd};
use std::fmt::Debug;
use std::mem::MaybeUninit;

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
}

pub trait KeyTrait:
    Key + Clone + PartialEq + PartialOrd + Ord + Debug + for<'a> From<&'a [u8]>
{
}
impl<T: Key + Clone + PartialOrd + PartialEq + Ord + Debug + for<'a> From<&'a [u8]>> KeyTrait
    for T
{
}

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
pub struct FixedKey<const SIZE: usize> {
    content: [u8; SIZE],
    len: usize,
}

impl<const SIZE: usize> PartialEq for FixedKey<SIZE> {
    fn eq(&self, other: &Self) -> bool {
        self.content[..self.len] == other.content[..other.len]
    }
}

impl<const SIZE: usize> PartialOrd for FixedKey<SIZE> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<const SIZE: usize> Ord for FixedKey<SIZE> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.content[..self.len].cmp(&other.content[..other.len])
    }
}

impl<const SIZE: usize> FixedKey<SIZE> {
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

impl<const SIZE: usize> Key for FixedKey<SIZE> {
    // Returns slice of the internal data up to the actual length
    fn as_slice(&self) -> &[u8] {
        &self.content[..self.len]
    }

    // Creates a new instance of FixedKey consisting only of the initial part of the content
    fn prefix_before(&self, length: usize) -> Self {
        assert!(length <= self.len);
        Self::from_slice(&self.content[..length])
    }

    // Creates a new instance of FixedKey excluding the initial part of the content
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

impl<const SIZE: usize> From<&[u8]> for FixedKey<SIZE> {
    fn from(src: &[u8]) -> Self {
        Self::from_slice(src)
    }
}

impl<const N: usize> From<u8> for FixedKey<N> {
    fn from(data: u8) -> Self {
        Self::from_slice(data.to_be_bytes().as_ref())
    }
}

impl<const N: usize> From<u16> for FixedKey<N> {
    fn from(data: u16) -> Self {
        Self::from_slice(data.to_be_bytes().as_ref())
    }
}

impl<const N: usize> From<u64> for FixedKey<N> {
    fn from(data: u64) -> Self {
        Self::from_slice(data.to_be_bytes().as_ref())
    }
}

impl<const N: usize> From<&str> for FixedKey<N> {
    fn from(data: &str) -> Self {
        Self::from_str(data)
    }
}

impl<const N: usize> From<String> for FixedKey<N> {
    fn from(data: String) -> Self {
        Self::from_string(&data)
    }
}
impl<const N: usize> From<&String> for FixedKey<N> {
    fn from(data: &String) -> Self {
        Self::from_string(data)
    }
}

// A VariableKey is a variable-length datatype with NULL byte appended to it.
#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Debug)]
pub struct VariableKey {
    data: Vec<u8>,
}

impl VariableKey {
    pub fn key(src: &[u8]) -> Self {
        let mut data = Vec::with_capacity(src.len() + 1);
        data.extend_from_slice(src);
        data.push(0);
        Self { data }
    }

    pub fn from_slice(src: &[u8]) -> Self {
        Self {
            data: Vec::from(src),
        }
    }

    pub fn to_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn terminate(&self) -> Self {
        let mut data = Vec::with_capacity(self.data.len() + 1);
        data.extend_from_slice(&self.data);
        data.push(0);
        Self { data }
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

    pub fn from_slice_with_termination(src: &[u8]) -> Self {
        let mut data = Vec::with_capacity(src.len() + 1);
        data.extend_from_slice(src);
        data.push(0);
        Self { data }
    }
}

impl From<&[u8]> for VariableKey {
    fn from(src: &[u8]) -> Self {
        Self::from_slice(src)
    }
}

impl Key for VariableKey {
    fn prefix_before(&self, length: usize) -> Self {
        assert!(length <= self.data.len());
        VariableKey::from_slice(&self.data[..length])
    }

    fn prefix_after(&self, start: usize) -> Self {
        assert!(start <= self.data.len());
        VariableKey::from_slice(&self.data[start..self.data.len()])
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

    fn as_slice(&self) -> &[u8] {
        &self.data[..self.data.len()]
    }
}

/*
    BitArray implementation

    This implementation is inspired from the BitArray implementation by rdaum
    Reference: https://github.com/rdaum/rart-rs
*/

#[derive(Clone)]
pub struct BitSet<const SIZE: usize> {
    bits: [bool; SIZE],
}

impl<const SIZE: usize> BitSet<SIZE> {
    pub fn new() -> Self {
        Self {
            bits: [false; SIZE],
        }
    }

    pub fn first_empty(&self) -> Option<usize> {
        self.bits.iter().position(|&bit| !bit)
    }

    pub fn first_set(&self) -> Option<usize> {
        self.bits.iter().position(|&bit| bit)
    }

    pub fn set(&mut self, pos: usize) {
        if pos < self.bits.len() {
            self.bits[pos] = true;
        }
    }

    pub fn unset(&mut self, pos: usize) {
        if pos < self.bits.len() {
            self.bits[pos] = false;
        }
    }

    pub fn check(&self, pos: usize) -> bool {
        pos < self.bits.len() && self.bits[pos]
    }

    pub fn clear(&mut self) {
        for bit in &mut self.bits {
            *bit = false;
        }
    }

    pub fn last(&self) -> Option<usize> {
        self.bits.iter().rposition(|&bit| bit)
    }

    pub fn is_empty(&self) -> bool {
        self.bits.iter().all(|&bit| !bit)
    }

    pub fn size(&self) -> usize {
        self.bits.len()
    }

    pub fn capacity(&self) -> usize {
        self.bits.len()
    }
}

/// A struct `BitArray` is a generic wrapper over an array that can store elements of type `X`.
pub struct BitArray<X, const WIDTH: usize> {
    items: Box<[MaybeUninit<X>; WIDTH]>,
    bits: BitSet<WIDTH>,
}
/// This is an implementation of the Default trait for BitArray. It simply calls the `new` function.
impl<X, const WIDTH: usize> Default for BitArray<X, WIDTH> {
    fn default() -> Self {
        Self::new()
    }
}

impl<X, const WIDTH: usize> BitArray<X, WIDTH> {
    /// This function constructs a new BitArray with a `WIDTH` number of `Option<X>` elements.
    pub fn new() -> Self {
        Self {
            items: Box::new(unsafe { MaybeUninit::uninit().assume_init() }),
            bits: BitSet::new(),
        }
    }

    pub fn push(&mut self, x: X) -> Option<usize> {
        if let Some(pos) = self.bits.first_empty() {
            self.bits.set(pos);
            unsafe {
                self.items[pos].as_mut_ptr().write(x);
            }
            Some(pos)
        } else {
            None
        }
    }

    pub fn pop(&mut self) -> Option<X> {
        if let Some(pos) = self.bits.last() {
            self.bits.unset(pos);
            let old = std::mem::replace(&mut self.items[pos], MaybeUninit::uninit());
            Some(unsafe { old.assume_init() })
        } else {
            None
        }
    }

    pub fn last(&self) -> Option<&X> {
        if let Some(pos) = self.bits.last() {
            unsafe { Some(self.items[pos].assume_init_ref()) }
        } else {
            None
        }
    }

    pub fn get(&self, pos: usize) -> Option<&X> {
        if self.bits.check(pos) {
            unsafe { Some(self.items[pos].assume_init_ref()) }
        } else {
            None
        }
    }

    pub fn get_mut(&mut self, pos: usize) -> Option<&mut X> {
        if self.bits.check(pos) {
            unsafe { Some(self.items[pos].assume_init_mut()) }
        } else {
            None
        }
    }

    pub fn set(&mut self, pos: usize, x: X) {
        if pos < self.items.len() {
            unsafe {
                self.items[pos].as_mut_ptr().write(x);
            };
            self.bits.set(pos);
        }
    }

    #[inline]
    pub fn erase(&mut self, pos: usize) -> Option<X> {
        assert!(pos < WIDTH);
        if self.bits.check(pos) {
            let old = std::mem::replace(&mut self.items[pos], MaybeUninit::uninit());
            self.bits.unset(pos);
            Some(unsafe { old.assume_init() })
        } else {
            None
        }
    }

    pub fn clear(&mut self) {
        for i in 0..WIDTH {
            if self.bits.check(i) {
                unsafe { self.items[i].assume_init_drop() }
            }
        }
        self.bits.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.bits.is_empty()
    }

    pub fn iter_keys(&self) -> impl DoubleEndedIterator<Item = usize> + '_ {
        self.items.iter().enumerate().filter_map(|x| {
            if self.bits.check(x.0) {
                Some(x.0)
            } else {
                None
            }
        })
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (usize, &X)> {
        self.items.iter().enumerate().filter_map(move |x| {
            if self.bits.check(x.0) {
                unsafe { Some((x.0, x.1.assume_init_ref())) }
            } else {
                None
            }
        })
    }

    #[inline]
    pub fn first_free_pos(&self) -> Option<usize> {
        self.bits.first_empty()
    }

    #[inline]
    pub fn last_used_pos(&self) -> Option<usize> {
        self.bits.last()
    }
}

impl<X: Clone, const WIDTH: usize> Clone for BitArray<X, WIDTH> {
    fn clone(&self) -> Self {
        let mut new_items: Box<[MaybeUninit<X>; WIDTH]> =
            Box::new(unsafe { MaybeUninit::uninit().assume_init() });

        for i in 0..WIDTH {
            if self.bits.check(i) {
                new_items[i] = MaybeUninit::new(unsafe { self.items[i].assume_init_ref() }.clone());
            }
        }
        Self {
            items: new_items,
            bits: self.bits.clone(),
        }
    }
}

impl<X, const WIDTH: usize> Drop for BitArray<X, WIDTH> {
    fn drop(&mut self) {
        for i in 0..WIDTH {
            if self.bits.check(i) {
                unsafe { self.items[i].assume_init_drop() }
            }
        }
        self.bits.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::BitArray;

    #[test]
    fn push_and_pop() {
        let mut v: BitArray<i32, 10> = BitArray::new();
        let index = v.push(5).unwrap();
        assert_eq!(v.get(index), Some(&5));
        assert_eq!(v.pop(), Some(5));
    }

    #[test]
    fn last() {
        let mut v: BitArray<i32, 10> = BitArray::new();
        v.push(5);
        v.push(6);
        assert_eq!(v.last(), Some(&6));
    }

    #[test]
    fn last_used_pos() {
        let mut v: BitArray<i32, 10> = BitArray::new();
        v.push(5);
        v.push(6);
        assert_eq!(v.last_used_pos(), Some(1));
    }

    #[test]
    fn first_free_pos() {
        let mut v: BitArray<i32, 10> = BitArray::new();
        v.push(5);
        assert_eq!(v.first_free_pos().unwrap(), 1);
    }

    #[test]
    fn get_and_set() {
        let mut v: BitArray<i32, 10> = BitArray::new();
        v.set(5, 6);
        assert_eq!(v.get(5), Some(&6));
    }

    #[test]
    fn get_mut() {
        let mut v: BitArray<i32, 10> = BitArray::new();
        v.set(5, 6);
        if let Some(value) = v.get_mut(5) {
            *value = 7;
        }
        assert_eq!(v.get(5), Some(&7));
    }

    #[test]
    fn erase() {
        let mut v: BitArray<i32, 10> = BitArray::new();
        v.push(5);

        assert_eq!(*v.get(0).unwrap(), 5);
        assert_eq!(v.erase(0), Some(5));
        assert_eq!(v.get(0), None);
    }

    #[test]
    fn clear() {
        let mut v: BitArray<i32, 10> = BitArray::new();
        v.push(5);
        v.clear();
        assert!(v.is_empty());
    }

    #[test]
    fn is_empty() {
        let mut v: BitArray<i32, 10> = BitArray::new();
        assert!(v.is_empty());
        v.push(5);
        assert!(!v.is_empty());
    }

    #[test]
    fn iter_keys() {
        let mut v: BitArray<i32, 10> = BitArray::new();
        v.push(5);
        v.push(6);
        let keys: Vec<usize> = v.iter_keys().collect();
        assert_eq!(keys, vec![0, 1]);
    }

    #[test]
    fn iter() {
        let mut v: BitArray<i32, 10> = BitArray::new();
        v.push(5);
        v.push(6);
        let values: Vec<(usize, &i32)> = v.iter().collect();
        assert_eq!(values, vec![(0, &5), (1, &6)]);
    }
}
