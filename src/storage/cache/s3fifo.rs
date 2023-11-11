use hashbrown::HashMap;
use std::cmp::min;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::Relaxed;

use crate::storage::cache::queue::Queue;

const MAX_FREQUENCY_LIMIT: u8 = 3;

struct Entry<K: Debug, V: Debug> {
    key: K,
    value: V,
    freq: AtomicU8,
}

impl<K: Debug, V: Debug> Debug for Entry<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Entry")
            .field("key", &self.key)
            .field("value", &self.value)
            .field("freq", &self.freq)
            .finish()
    }
}

impl<K: Debug, V: Debug> Entry<K, V> {
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
    V: Debug + Clone,
{
    min_eviction_size: usize,
    max_cache_size: usize,
    small: Queue<Entry<K, V>>,
    main: Queue<Entry<K, V>>,
    ghost: Queue<K>,
    table: HashMap<K, Entry<K, V>>,
}

impl<K, V> Cache<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug,
    V: Debug + Clone,
{
    pub fn new(cache_size: usize) -> Self {
        assert!(cache_size > 0);
        let min_eviction_size = cache_size / 10;

        Self {
            min_eviction_size,
            max_cache_size: cache_size,
            small: Queue::new(),
            main: Queue::new(),
            ghost: Queue::new(),
            table: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: &K) -> Option<&V> {
        if let Some(entry) = self.table.get(key) {
            let freq = min(entry.freq.load(Relaxed) + 1, MAX_FREQUENCY_LIMIT);
            entry.freq.store(freq, Relaxed);
            Some(&entry.value)
        } else {
            None
        }
    }

    pub fn push(&mut self, key: K, value: V) {
        self.ensure_free();

        if self.table.contains_key(&key) {
            let entry = Entry::new(key, value);
            self.main.push(entry);
        } else {
            let entry = Entry::new(key.clone(), value.clone());
            self.table.insert(entry.key.clone(), entry);
            let entry = Entry::new(key, value);
            self.small.push(entry);
        }
    }

    fn push_m(&mut self, tail: Entry<K, V>) {
        self.main.push(tail);
        if self.main.len() >= self.max_cache_size {
            self.evict_m();
        }
    }

    fn push_g(&mut self, tail: Entry<K, V>) {
        if self.ghost.len() >= self.max_cache_size {
            let key = self.ghost.pop().unwrap();
            self.table.remove(&key);
        }

        self.ghost.push(tail.key);
    }

    fn ensure_free(&mut self) {
        while self.small.len() + self.main.len() >= self.max_cache_size {
            if self.small.len() >= self.min_eviction_size {
                self.evict_s();
            } else {
                self.evict_m();
            }
        }
    }

    fn evict_m(&mut self) {
        let mut evicted = false;
        while !evicted && self.main.len() > 0 {
            if let Some(tail) = self.main.pop() {
                let freq = tail.freq.load(Relaxed);
                if freq > 0 {
                    tail.freq.store(freq - 1, Relaxed);
                    self.main.push(tail);
                } else {
                    self.table.remove(&tail.key);
                    evicted = true;
                }
            }
        }
    }

    fn evict_s(&mut self) {
        let mut evicted = false;
        while !evicted && self.small.len() > 0 {
            if let Some(tail) = self.small.pop() {
                if tail.freq.load(Relaxed) > 1 {
                    self.push_m(tail);
                } else {
                    self.push_g(tail);
                    evicted = true;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;

    use super::*;

    fn assert_opt_eq<V: PartialEq + Debug>(opt: Option<&V>, v: V) {
        assert!(opt.is_some());
        assert_eq!(opt.unwrap(), &v);
    }

    #[test]
    fn test_push_and_read() {
        let mut cache = Cache::new(2);

        cache.push("apple", "red");
        cache.push("banana", "yellow");

        assert_opt_eq(cache.get(&"apple"), "red");
        assert_opt_eq(cache.get(&"banana"), "yellow");
    }

    #[test]
    fn test_push_removes_oldest() {
        let mut cache = Cache::new(2);

        cache.push("apple", "red");
        cache.push("banana", "yellow");
        cache.push("orange", "orange");
        cache.push("pear", "green");
        cache.push("tomato", "red");

        assert!(cache.get(&"apple").is_none());
        assert_opt_eq(cache.get(&"banana"), "yellow");
        assert_opt_eq(cache.get(&"orange"), "orange");

        // "apple" should been removed from the cache.
        cache.push("apple", "orange");
        cache.push("tomato", "red");

        assert!(cache.get(&"orange").is_none());
        assert_opt_eq(cache.get(&"apple"), "orange");
        assert_opt_eq(cache.get(&"tomato"), "red");
    }

    #[test]
    fn test_no_memory_leaks() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        #[derive(Debug, Clone)]
        struct DropCounter;

        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Relaxed);
            }
        }

        let n = 100;
        for _ in 0..n {
            let mut cache = Cache::new(2);
            for i in 0..n {
                cache.push(i, DropCounter {});
            }
        }
        assert_eq!(DROP_COUNT.load(Relaxed), 2 * n * n);
    }
}
