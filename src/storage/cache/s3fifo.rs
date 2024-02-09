/// This is an experimental implementation of a cache that uses the S3-FIFO algorithm. This is not yet
/// used in the main codebase. But the implementation is kept here for future reference for replacing it
/// with the current LRU cache for caching recently accessed values.
use hashbrown::HashMap;
use std::cmp::min;
use std::collections::LinkedList;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::SeqCst;

/// Maximum frequency limit for an entry in the cache.
const MAX_FREQUENCY_LIMIT: u8 = 3;

/// Represents an entry in the cache.
struct Entry<K, V> {
    key: K,
    value: V,
    /// Frequency of access of this entry.
    freq: AtomicU8,
}

impl<K, V> Entry<K, V> {
    /// Creates a new entry with the given key and value.
    pub fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            freq: AtomicU8::new(0),
        }
    }
}

impl<K, V> Clone for Entry<K, V>
where
    K: Clone,
    V: Clone,
{
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            value: self.value.clone(),
            freq: AtomicU8::new(self.freq.load(SeqCst)),
        }
    }
}

/// Cache is an implementation of "S3-FIFO" from "FIFO Queues are ALL You Need for Cache Eviction" by
/// Juncheng Yang, et al: <https://jasony.me/publication/sosp23-s3fifo.pdf>
pub struct Cache<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug,
    V: Clone,
{
    max_main_size: usize,
    max_cache_size: usize,
    /// Small queue for entries with low frequency.
    small: LinkedList<Entry<K, V>>,
    /// Main queue for entries with high frequency.
    main: LinkedList<Entry<K, V>>,
    /// Ghost queue for evicted entries.
    ghost: LinkedList<K>,
    /// Map of all entries for quick access.
    entries: HashMap<K, Entry<K, V>>,
}

impl<K, V> Cache<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug,
    V: Clone,
{
    /// Creates a new cache with the given maximum size.
    pub fn new(max_cache_size: usize) -> Self {
        let max_small_size = max_cache_size / 10;
        let max_main_size = max_cache_size - max_small_size;

        Self {
            max_main_size,
            max_cache_size,
            small: LinkedList::new(),
            main: LinkedList::new(),
            ghost: LinkedList::new(),
            entries: HashMap::new(),
        }
    }

    /// Returns a reference to the value of the given key if it exists in the cache.
    pub fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(entry) = self.entries.get(key) {
            let freq = min(entry.freq.load(SeqCst) + 1, MAX_FREQUENCY_LIMIT);
            entry.freq.store(freq, SeqCst);
            Some(&entry.value)
        } else {
            None
        }
    }

    /// Inserts a new entry with the given key and value into the cache.
    pub fn insert(&mut self, key: K, value: V) {
        self.evict();

        if self.entries.contains_key(&key) {
            let entry = Entry::new(key, value);
            self.main.push_back(entry);
        } else {
            let entry = Entry::new(key, value);
            self.entries.insert(entry.key.clone(), entry.clone());
            self.small.push_back(entry);
        }
    }
    fn insert_m(&mut self, tail: Entry<K, V>) {
        self.main.push_front(tail);
    }

    fn insert_g(&mut self, tail: Entry<K, V>) {
        if self.ghost.len() >= self.max_main_size {
            let key = self.ghost.pop_front().unwrap();
            self.entries.remove(&key);
        }
        self.ghost.push_back(tail.key);
    }

    fn evict(&mut self) {
        if self.small.len() + self.main.len() >= self.max_cache_size {
            if self.main.len() >= self.max_main_size || self.small.is_empty() {
                self.evict_m();
            } else {
                self.evict_s();
            }
        }
    }

    fn evict_m(&mut self) {
        while let Some(tail) = self.main.pop_front() {
            let freq = tail.freq.load(SeqCst);
            if freq > 0 {
                tail.freq.store(freq - 1, SeqCst);
                self.main.push_back(tail);
            } else {
                self.entries.remove(&tail.key);
                break;
            }
        }
    }

    fn evict_s(&mut self) {
        while let Some(tail) = self.small.pop_front() {
            if tail.freq.load(SeqCst) > 1 {
                self.insert_m(tail);
            } else {
                self.insert_g(tail);
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::SeqCst;

    use super::*;

    fn assert_opt_eq<V: PartialEq + Debug>(opt: Option<&V>, v: V) {
        assert!(opt.is_some());
        assert_eq!(opt.unwrap(), &v);
    }

    #[test]
    fn test_push_and_read() {
        let mut cache = Cache::new(2);

        cache.insert("apple", "red");
        cache.insert("banana", "yellow");

        assert_opt_eq(cache.get(&"apple"), "red");
        assert_opt_eq(cache.get(&"banana"), "yellow");
    }

    #[test]
    fn test_push_removes_oldest() {
        let mut cache = Cache::new(2);

        let fruits = vec![
            ("apple", "red"),
            ("banana", "yellow"),
            ("orange", "orange"),
            ("pear", "green"),
            ("peach", "pink"),
        ];

        for (fruit, color) in fruits {
            cache.insert(fruit, color);
        }

        assert!(cache.get(&"apple").is_none());
        assert_opt_eq(cache.get(&"pear"), "green");
        assert_opt_eq(cache.get(&"peach"), "pink");

        // "apple" should been removed from the cache.
        cache.insert("apple", "red");
        cache.insert("banana", "yellow");

        // assert!(cache.get(&"pear").is_none());
        assert_opt_eq(cache.get(&"apple"), "red");
        assert_opt_eq(cache.get(&"banana"), "yellow");
    }

    #[test]
    fn test_no_memory_leaks() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        #[derive(Debug, Clone)]
        struct DropCounter;

        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, SeqCst);
            }
        }

        let n = 100;
        for _ in 0..n {
            let mut cache = Cache::new(20);
            for i in 0..n {
                cache.insert(i, DropCounter {});
            }
        }
        assert_eq!(DROP_COUNT.load(SeqCst), 2 * n * n);
    }
}
