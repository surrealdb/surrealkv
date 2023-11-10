pub mod index;
pub mod kv;
pub mod log;

use std::cmp::min;
use std::collections::VecDeque;
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
    K: PartialEq,
{
    min_eviction_size: usize,
    max_cache_size: usize,
    small: VecDeque<Entry<K, V>>,
    main: VecDeque<Entry<K, V>>,
    ghost: VecDeque<K>,
}

impl<K, V> Cache<K, V>
where
    K: PartialEq,
{
    pub fn new(cache_size: usize) -> Self {
        assert!(cache_size > 0);
        let min_eviction_size = cache_size / 10;

        Self {
            min_eviction_size,
            max_cache_size: cache_size,
            small: VecDeque::with_capacity(min_eviction_size),
            main: VecDeque::with_capacity(cache_size),
            ghost: VecDeque::with_capacity(cache_size),
        }
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        if let Some(entry) = self
            .small
            .iter()
            .chain(self.main.iter())
            .find(|e| &e.key == key)
        {
            let freq = min(entry.freq.load(SeqCst) + 1, MAX_FREQUENCY_LIMIT);
            entry.freq.store(freq, SeqCst);
            Some(&entry.value)
        } else {
            None
        }
    }

    pub fn insert(&mut self, key: K, value: V) {
        self.ensure_free();

        if self.ghost.contains(&key) {
            let entry = Entry::new(key, value);
            self.main.push_front(entry);
        } else {
            let entry = Entry::new(key, value);
            self.small.push_front(entry);
        }
    }

    fn insert_m(&mut self, tail: Entry<K, V>) {
        self.main.push_front(tail);
        if self.main.len() >= self.max_cache_size {
            self.evict_m();
        }
    }

    fn insert_g(&mut self, tail: Entry<K, V>) {
        if self.ghost.len() >= self.max_cache_size {
            self.ghost.pop_back();
        }
        self.ghost.push_front(tail.key);
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
            if let Some(tail) = self.main.pop_back() {
                let freq = tail.freq.load(SeqCst);
                if freq > 0 {
                    tail.freq.store(freq - 1, SeqCst);
                    self.main.push_front(tail);
                } else {
                    evicted = true;
                }
            }
        }
    }

    fn evict_s(&mut self) {
        let mut evicted = false;
        while !evicted && self.small.len() > 0 {
            if let Some(tail) = self.small.pop_back() {
                if tail.freq.load(SeqCst) > 1 {
                    self.insert_m(tail);
                } else {
                    self.insert_g(tail);
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
    use std::sync::atomic::Ordering::SeqCst;

    use super::*;

    fn assert_opt_eq<V: PartialEq + Debug>(opt: Option<&V>, v: V) {
        assert!(opt.is_some());
        assert_eq!(opt.unwrap(), &v);
    }

    #[test]
    fn test_insert_and_read() {
        let mut cache = Cache::new(2);

        cache.insert("apple", "red");
        cache.insert("banana", "yellow");

        assert_opt_eq(cache.get(&"apple"), "red");
        assert_opt_eq(cache.get(&"banana"), "yellow");
    }

    #[test]
    fn test_insert_removes_oldest() {
        let mut cache = Cache::new(2);

        cache.insert("apple", "red");
        cache.insert("banana", "yellow");
        cache.insert("orange", "orange");

        assert!(cache.get(&"apple").is_none());
        assert_opt_eq(cache.get(&"banana"), "yellow");
        assert_opt_eq(cache.get(&"orange"), "orange");

        // "apple" should been removed from the cache.
        cache.insert("apple", "orange");
        cache.insert("tomato", "red");

        assert!(cache.get(&"orange").is_none());
        assert_opt_eq(cache.get(&"apple"), "orange");
        assert_opt_eq(cache.get(&"tomato"), "red");
    }

    #[test]
    fn test_send() {
        use std::thread;

        let mut cache = Cache::new(2);
        cache.insert(1, "a");

        let handle = thread::spawn(move || {
            assert_eq!(cache.get(&1), Some(&"a"));
        });

        assert!(handle.join().is_ok());
    }

    #[test]
    fn test_no_memory_leaks() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        struct DropCounter;

        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, SeqCst);
            }
        }

        let n = 100;
        for _ in 0..n {
            let mut cache = Cache::new(2);
            for i in 0..n {
                cache.insert(i, DropCounter {});
            }
        }
        assert_eq!(DROP_COUNT.load(SeqCst), n * n);
    }
}
