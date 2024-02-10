use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use lru::LruCache;
use rand::{thread_rng, Rng};
use std::num::NonZeroUsize;
use surrealkv::storage::cache::s3fifo::Cache;

const CASES: usize = 100_000;

fn bench_s3fifo_cache(c: &mut Criterion) {
    c.bench_function("Test s3fifo Cache", move |b| {
        b.iter_batched(
            || {
                let mut rng = thread_rng();
                let nums: Vec<u64> = black_box(
                    (0..(CASES * 2))
                        .map(|i| {
                            if i % 2 == 0 {
                                rng.gen::<u64>() % 16384
                            } else {
                                rng.gen::<u64>() % 32768
                            }
                        })
                        .collect(),
                );
                let l = Cache::new(8192);
                (l, nums)
            },
            |(mut l, nums)| {
                (0..CASES).for_each(|v| {
                    let k = nums[v];
                    l.insert(k, k);
                });

                (0..CASES).for_each(|v| {
                    let k = nums[v];
                    let _ = l.get(&k);
                });
            },
            BatchSize::LargeInput,
        )
    });
}

fn bench_lru_cache(c: &mut Criterion) {
    c.bench_function("Test LRU cache", move |b| {
        b.iter_batched(
            || {
                let mut rng = thread_rng();
                let nums: Vec<u64> = black_box(
                    (0..(CASES * 2))
                        .map(|i| {
                            if i % 2 == 0 {
                                rng.gen::<u64>() % 16384
                            } else {
                                rng.gen::<u64>() % 32768
                            }
                        })
                        .collect(),
                );
                let l = LruCache::new(NonZeroUsize::new(8192).unwrap());
                (l, nums)
            },
            |(mut l, nums)| {
                (0..CASES).for_each(|v| {
                    let k = nums[v];
                    let _ = l.put(k, k);
                });

                (0..CASES).for_each(|v| {
                    let k = nums[v];
                    let _ = l.get(&k);
                });
            },
            BatchSize::LargeInput,
        )
    });
}

criterion_group!(cache, bench_s3fifo_cache, bench_lru_cache,);

criterion_main!(cache);
