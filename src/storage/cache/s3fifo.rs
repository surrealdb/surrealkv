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

const MAX_FREQUENCY_LIMIT: u8 = 3;

struct Entry<K, V> {
    key: K,
    value: V,
    freq: AtomicU8,
}

impl<K, V> Entry<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
            freq: AtomicU8::new(0),
        }
    }
}

/// Cache is an implementation of "S3-FIFO" from "FIFO Queues are ALL You Need for Cache Eviction" by
/// Juncheng Yang, et al: https://jasony.me/publication/sosp23-s3fifo.pdf

pub struct Cache<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug,
    V: Clone,
{
    max_small_size: usize,
    max_main_size: usize,
    small: LinkedList<Entry<K, V>>,
    main: LinkedList<Entry<K, V>>,
    ghost: LinkedList<K>,
    table: HashMap<K, Entry<K, V>>,
}
impl<K, V> Cache<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug,
    V: Clone,
{
    pub fn new(max_cache_size: usize) -> Self {
        assert!(max_cache_size > 0);
        let max_small_size = max_cache_size / 10;
        let max_main_size = max_cache_size - max_small_size;

        Self {
            max_small_size,
            max_main_size,
            small: LinkedList::new(),
            main: LinkedList::new(),
            ghost: LinkedList::new(),
            table: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(entry) = self.table.get(key) {
            let freq = min(entry.freq.load(SeqCst) + 1, MAX_FREQUENCY_LIMIT);
            entry.freq.store(freq, SeqCst);
            Some(&entry.value)
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.evict();

        if self.table.contains_key(&key) {
            let entry = Entry::new(key, value);
            self.main.push_front(entry);
        } else {
            let entry = Entry::new(key.clone(), value.clone());
            self.table.insert(entry.key.clone(), entry);
            let entry = Entry::new(key, value);
            self.small.push_front(entry);
        }
    }

    fn insert_m(&mut self, tail: Entry<K, V>) {
        self.main.push_front(tail);
        if self.main.len() >= self.max_main_size {
            self.evict_m();
        }
    }

    fn insert_g(&mut self, tail: Entry<K, V>) {
        self.ghost.push_front(tail.key);
        if self.ghost.len() >= self.max_main_size {
            let key = self.ghost.pop_back().unwrap();
            self.table.remove(&key);
        }
    }

    fn evict(&mut self) {
        if self.small.len() >= self.max_small_size {
            self.evict_s();
        } else {
            self.evict_m();
        }
    }

    fn evict_m(&mut self) {
        while let Some(tail) = self.main.pop_back() {
            let freq = tail.freq.load(SeqCst);
            if freq > 0 {
                tail.freq.store(freq - 1, SeqCst);
                self.main.push_front(tail);
            } else {
                break;
            }
        }
    }

    fn evict_s(&mut self) {
        while let Some(tail) = self.small.pop_back() {
            if tail.freq.load(SeqCst) > 1 {
                self.insert_m(tail);
            } else {
                self.insert_g(tail);
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

        cache.insert("apple", "red");
        cache.insert("banana", "yellow");
        cache.insert("orange", "orange");
        cache.insert("pear", "green");
        cache.insert("tomato", "red");

        assert!(cache.get(&"apple").is_none());
        assert!(cache.get(&"banana").is_none());
        assert!(cache.get(&"orange").is_none());
        assert_opt_eq(cache.get(&"pear"), "green");
        assert_opt_eq(cache.get(&"tomato"), "red");

        // "apple" should been removed from the cache.
        cache.insert("apple", "red");
        cache.insert("banana", "yellow");

        assert!(cache.get(&"pear").is_none());
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
