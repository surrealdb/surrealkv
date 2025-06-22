#![allow(clippy::single_element_loop)]
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::{rngs::StdRng, thread_rng, Rng, SeedableRng};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use surrealkv::Options;
use surrealkv::Result;
use surrealkv::Store;
use tempdir::TempDir;

// Should be kept in sync with https://github.com/surrealdb/surrealdb/blob/main/src/mem/mod.rs
#[cfg_attr(any(target_os = "linux", target_os = "macos"), global_allocator)]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DEFAULT_KEY_SIZE: usize = 30;
const DEFAULT_VALUE_SIZE: usize = 200;

fn create_temp_directory() -> TempDir {
    TempDir::new("test").unwrap()
}

fn sequential_insert(c: &mut Criterion) {
    let count = AtomicU32::new(0_u32);
    let bytes = |len| -> Vec<u8> {
        count
            .fetch_add(1, Relaxed)
            .to_be_bytes()
            .into_iter()
            .cycle()
            .take(len)
            .collect()
    };

    let mut bench = |key_len, val_len| {
        let mut opts = Options::new();
        opts.dir = create_temp_directory().path().to_path_buf();
        let db = Store::new(opts).expect("should create store");

        c.bench_function(
            &format!("sequential insert key/value lengths {key_len}/{val_len}"),
            |b| {
                b.iter(|| {
                    let mut txn = db.begin().unwrap();
                    txn.set(bytes(key_len)[..].into(), bytes(val_len)[..].into())
                        .unwrap();
                    txn.commit().unwrap();
                })
            },
        );
    };

    for key_len in &[DEFAULT_KEY_SIZE] {
        for val_len in &[DEFAULT_VALUE_SIZE] {
            bench(*key_len, *val_len);
        }
    }
}

fn random_insert(c: &mut Criterion) {
    for key_len in &[DEFAULT_KEY_SIZE] {
        for val_len in &[DEFAULT_VALUE_SIZE] {
            let key_len = *key_len;
            let val_len = *val_len;

            let mut opts = Options::new();
            opts.dir = create_temp_directory().path().to_path_buf();
            let db = Store::new(opts).expect("should create store");

            // Pre-generate a pool of random keys and values
            let mut rng = rand::thread_rng();
            let num_samples = 1000;
            let keys: Vec<Vec<u8>> = (0..num_samples)
                .map(|_| (0..key_len).map(|_| rng.gen()).collect())
                .collect();
            let values: Vec<Vec<u8>> = (0..num_samples)
                .map(|_| (0..val_len).map(|_| rng.gen()).collect())
                .collect();

            c.bench_function(
                &format!("random insert key/value lengths {key_len}/{val_len}"),
                |b| {
                    let db = &db;
                    let keys = &keys;
                    let values = &values;
                    let mut idx = 0;

                    b.iter(|| {
                        let key = &keys[idx % num_samples];
                        let value = &values[idx % num_samples];
                        idx += 1;

                        let mut txn = db.begin().unwrap();
                        txn.set(key[..].into(), value[..].into()).unwrap();
                        txn.commit().unwrap();
                    })
                },
            );
        }
    }
}

// Helper function to generate random data
fn generate_random_bytes(len: usize) -> Vec<u8> {
    let mut rng = thread_rng();
    (0..len).map(|_| rng.gen::<u8>()).collect()
}

// bulk insert benchmark with batching
fn bulk_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_operations");

    // Test different batch sizes
    let batch_sizes = [1000];
    let value_sizes = [DEFAULT_VALUE_SIZE];

    for &batch_size in &batch_sizes {
        for &value_size in &value_sizes {
            group.throughput(criterion::Throughput::Elements(batch_size));

            let mut opts = Options::new();
            opts.dir = create_temp_directory().path().to_path_buf();
            let db = Store::new(opts).expect("should create store");

            // Pre-generate data to avoid generation overhead during benchmarking
            let data: Vec<(Vec<u8>, Vec<u8>)> = (0u64..batch_size)
                .map(|i| (i.to_be_bytes().to_vec(), generate_random_bytes(value_size)))
                .collect();

            group.bench_function(
                BenchmarkId::new(
                    "bulk_insert",
                    format!("batch_{batch_size}_val_{value_size}"),
                ),
                |b| {
                    b.iter(|| {
                        let data = data.clone();
                        let mut txn = db.begin().unwrap();
                        for (key, value) in data {
                            txn.set(key.as_slice(), value.as_slice()).unwrap();
                        }
                        txn.commit().unwrap()
                    })
                },
            );
        }
    }
    group.finish();
}

fn sequential_insert_read(c: &mut Criterion) {
    let max_count = AtomicU32::new(0_u32);
    let mut opts = Options::new();
    opts.dir = create_temp_directory().path().to_path_buf();
    let db = Store::new(opts).expect("should create store");

    c.bench_function("sequential inserts", |b| {
        let count = AtomicU32::new(0_u32);
        b.iter(|| {
            let mut txn = db.begin().unwrap();
            txn.set(
                count.fetch_add(1, Relaxed).to_be_bytes()[..].into(),
                vec![][..].into(),
            )
            .unwrap();
            txn.commit().unwrap();

            let current_count = count.load(Relaxed);
            if current_count > max_count.load(Relaxed) {
                max_count.store(current_count, Relaxed);
            }
        })
    });

    c.bench_function("sequential gets", |b| {
        let count = AtomicU32::new(0_u32);
        b.iter(|| {
            count.fetch_add(1, Relaxed);

            let current_count = count.load(Relaxed);
            if current_count <= max_count.load(Relaxed) {
                let mut txn = db.begin().unwrap();
                txn.get(&current_count.to_be_bytes()[..]).unwrap();
            }
        })
    });
}

fn concurrent_insert(c: &mut Criterion) {
    // Configuration
    let item_count = 100_000;

    let key_sizes = vec![DEFAULT_KEY_SIZE]; // in bytes
    let value_sizes = vec![DEFAULT_VALUE_SIZE]; // in bytes
    let thread_counts = vec![num_cpus::get()];

    let mut group = c.benchmark_group("concurrent_inserts");
    group
        .throughput(criterion::Throughput::Elements(item_count as u64))
        .sample_size(10);

    for &key_size in &key_sizes {
        for &value_size in &value_sizes {
            // Pre-generate random data for this key/value size combination
            let mut rng = rand::thread_rng();
            let data: Arc<Vec<(Vec<u8>, Vec<u8>)>> = Arc::new(
                (0..item_count)
                    .map(|_| {
                        let key: Vec<u8> = (0..key_size).map(|_| rng.gen()).collect();
                        let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
                        (key, value)
                    })
                    .collect(),
            );

            for &thread_count in &thread_counts {
                let items_per_thread = item_count / thread_count;

                let mut opts = Options::new();
                opts.dir = create_temp_directory().path().to_path_buf();
                let db = Arc::new(Store::new(opts).expect("should create store"));

                // Calculate total data size for this configuration
                let total_data_size = item_count * (key_size + value_size);
                let data_size_mb = total_data_size as f64 / (1024.0 * 1024.0);

                group.bench_function(
                    format!(
                        "k{}v{}t{}_{}MB",
                        key_size,
                        value_size,
                        thread_count,
                        data_size_mb.round()
                    ),
                    |b| {
                        let data = Arc::clone(&data);
                        let db = Arc::clone(&db);

                        b.iter(|| {
                            let mut handles = Vec::with_capacity(thread_count);

                            for thread_idx in 0..thread_count {
                                let db = Arc::clone(&db);
                                let data = Arc::clone(&data);
                                let start_idx = thread_idx * items_per_thread;
                                let end_idx = start_idx + items_per_thread;

                                let handle = std::thread::spawn(move || {
                                    let mut txn = db.begin().unwrap();

                                    for idx in start_idx..end_idx {
                                        let (ref key, ref value) = data[idx];
                                        txn.set(key.as_slice(), value.as_slice()).unwrap();
                                    }

                                    txn.commit().unwrap()
                                });

                                handles.push(handle);
                            }

                            for handle in handles {
                                handle.join().unwrap();
                            }
                        })
                    },
                );
            }
        }
    }

    group.finish();
}

// Range scan performance
fn range_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_scans");

    let scan_sizes = [10000_u32];
    let value_size = DEFAULT_VALUE_SIZE;

    for &scan_size in &scan_sizes {
        let db = {
            let mut opts = Options::new();
            opts.dir = create_temp_directory().path().to_path_buf();
            let db = Store::new(opts).expect("should create store");

            // Pre-populate with sorted data
            let mut txn = db.begin().unwrap();
            for i in 0..scan_size {
                txn.set(
                    i.to_be_bytes().to_vec().as_slice(),
                    generate_random_bytes(value_size).as_slice(),
                )
                .unwrap();
            }
            txn.commit().unwrap();
            db
        };

        group.bench_function(BenchmarkId::new("range_scan", scan_size), |b| {
            b.iter(|| {
                let mut txn = db.begin().unwrap();
                let start = 0_u64.to_be_bytes().to_vec();
                let end = scan_size.to_be_bytes().to_vec();
                let range = &start[..]..&end[..];
                txn.scan(range, None)
                    .collect::<Result<Vec<(&[u8], Vec<u8>, u64)>>>()
                    .expect("Scan should succeed");
            })
        });
    }

    group.finish();
}

// Concurrent workload with different read/write ratios
fn concurrent_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_workload");

    let operations_per_thread = 1000;
    let value_size = DEFAULT_VALUE_SIZE;
    let thread_counts = [num_cpus::get() as u64];
    let read_ratios = [0.0, 0.5, 0.95, 1.0]; // 0%, 50%, 95%, 100% reads

    for &thread_count in &thread_counts {
        for &read_ratio in &read_ratios {
            let db = {
                let mut opts = Options::new();
                opts.dir = create_temp_directory().path().to_path_buf();
                Arc::new(Store::new(opts).expect("should create store"))
            };

            let counter = Arc::new(AtomicU64::new(0));

            group.throughput(criterion::Throughput::Elements(
                operations_per_thread * thread_count,
            ));
            group.bench_function(
                format!(
                    "threads_{}_reads_{}",
                    thread_count,
                    (read_ratio * 100.0) as u32
                ),
                |b| {
                    b.iter(|| {
                        let mut handles = vec![];

                        for _ in 0..thread_count {
                            let db = db.clone();
                            let counter = counter.clone();

                            let handle = std::thread::spawn(move || {
                                let mut rng = StdRng::from_entropy();
                                for _ in 0..operations_per_thread {
                                    let mut txn = db.begin().unwrap();

                                    if rng.gen::<f64>() < read_ratio {
                                        // Read operation
                                        let key = (rng.gen::<u64>() % operations_per_thread)
                                            .to_be_bytes()
                                            .to_vec();
                                        txn.get(&key).unwrap();
                                    } else {
                                        // Write operation
                                        let key =
                                            counter.fetch_add(1, Relaxed).to_be_bytes().to_vec();
                                        txn.set(
                                            key.as_slice(),
                                            generate_random_bytes(value_size).as_slice(),
                                        )
                                        .unwrap();
                                    }
                                    txn.commit().unwrap();
                                }
                            });
                            handles.push(handle);
                        }

                        for handle in handles {
                            handle.join().unwrap();
                        }
                    })
                },
            );
        }
    }

    group.finish();
}

criterion_group!(
    benches_insert,
    sequential_insert,
    random_insert,
    bulk_insert,
    sequential_insert_read,
);
criterion_group!(benches_range, range_scan);
criterion_group!(benches_concurrent, concurrent_insert, concurrent_workload);
criterion_main!(benches_insert, benches_range, benches_concurrent,);
