use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion};
use hashbrown::HashSet;
use rand::{thread_rng, Rng};
use std::collections::VecDeque;

const CASES: usize = 1_000_000;

fn bench_dequeue(c: &mut Criterion) {
    c.bench_function("Test dequeue Cache", move |b| {
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
                let l = VecDeque::with_capacity(8192);
                (l, nums)
            },
            |(mut l, nums)| {
                (0..CASES).for_each(|v| {
                    let k = nums[v];
                    l.insert(k as usize, k);
                });
            },
            BatchSize::LargeInput,
        )
    });
}

fn bench_hashset(c: &mut Criterion) {
    c.bench_function("Test hashset", move |b| {
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
                let l = HashSet::new();
                (l, nums)
            },
            |(mut l, nums)| {
                (0..CASES).for_each(|v| {
                    let k = nums[v];
                    let _ = l.insert(k);
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

criterion_group!(cache, bench_dequeue, bench_hashset,);

criterion_main!(cache);
