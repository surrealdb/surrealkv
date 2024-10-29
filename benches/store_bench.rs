use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};

use surrealkv::Options;
use surrealkv::Store;
use tempdir::TempDir;

// Should be kept in sync with https://github.com/surrealdb/surrealdb/blob/main/src/mem/mod.rs
#[cfg_attr(any(target_os = "linux", target_os = "macos"), global_allocator)]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn create_temp_directory() -> TempDir {
    TempDir::new("test").unwrap()
}

fn bulk_insert(c: &mut Criterion) {
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
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .unwrap();

        let db = rt.block_on(async {
            let mut opts = Options::new();
            opts.dir = create_temp_directory().path().to_path_buf();
            Store::new(opts).expect("should create store")
        });

        c.bench_function(
            &format!("bulk load key/value lengths {}/{}", key_len, val_len),
            |b| {
                b.to_async(&rt).iter(|| async {
                    let mut txn = db.begin().unwrap();
                    txn.set(bytes(key_len)[..].into(), bytes(val_len)[..].into())
                        .unwrap();
                    txn.commit().await.unwrap();
                })
            },
        );
        rt.block_on(async {
            drop(db);
        });
        rt.shutdown_background();
    };

    for key_len in &[10_usize, 128, 256, 512] {
        for val_len in &[0_usize, 10, 128, 256, 512, 1024, 2048, 4096, 8192] {
            bench(*key_len, *val_len);
        }
    }
}

fn sequential_insert_read(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let max_count = AtomicU32::new(0_u32);
        let mut opts = Options::new();
        opts.dir = create_temp_directory().path().to_path_buf();
        let db = Store::new(opts).expect("should create store");

        c.bench_function("sequential inserts", |b| {
            let count = AtomicU32::new(0_u32);
            b.iter(|| async {
                let mut txn = db.begin().unwrap();
                txn.set(
                    count.fetch_add(1, Relaxed).to_be_bytes()[..].into(),
                    vec![][..].into(),
                )
                .unwrap();
                txn.commit().await.unwrap();

                let current_count = count.load(Relaxed);
                if current_count > max_count.load(Relaxed) {
                    max_count.store(current_count, Relaxed);
                }
            })
        });

        c.bench_function("sequential gets", |b| {
            let count = AtomicU32::new(0_u32);
            b.iter(|| async {
                count.fetch_add(1, Relaxed);

                let current_count = count.load(Relaxed);
                if current_count <= max_count.load(Relaxed) {
                    let mut txn = db.begin().unwrap();
                    txn.get(&current_count.to_be_bytes()[..]).unwrap();
                }
            })
        });
    });
    rt.shutdown_background();
}

fn concurrent_insert(c: &mut Criterion) {
    let total_items = 100_000;
    // Test different thread counts from 1 to num_cpus
    let thread_counts = vec![1, 2, 4, 8, num_cpus::get() as u32];

    let mut group = c.benchmark_group("concurrent_inserts");
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(total_items as u64));

    for &thread_count in &thread_counts {
        // Create a runtime with the specific thread count
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(thread_count as usize)
            .enable_all()
            .build()
            .unwrap();

        let db = rt.block_on(async {
            let mut opts = Options::new();
            opts.dir = create_temp_directory().path().to_path_buf();
            Arc::new(Store::new(opts).expect("should create store"))
        });

        // Calculate operations per thread to maintain constant total work
        let ops_per_thread = total_items / thread_count;

        group.bench_function(
            format!(
                "threads={} total_ops={} ops_per_thread={}",
                thread_count, total_items, ops_per_thread
            ),
            |b| {
                b.iter(|| {
                    let mut handles = vec![];

                    for _ in 0..thread_count {
                        let db = db.clone();

                        let handle = rt.spawn(async move {
                            let mut txn = db.begin().unwrap();
                            for _ in 0..ops_per_thread {
                                let key = nanoid::nanoid!();
                                let value = nanoid::nanoid!();
                                txn.set(key.as_bytes(), value.as_bytes()).unwrap();
                            }
                            txn.commit().await.unwrap();
                        });

                        handles.push(handle);
                    }

                    // Wait for all threads to complete
                    rt.block_on(async {
                        for handle in handles {
                            handle.await.unwrap();
                        }
                    })
                })
            },
        );

        // Cleanup
        rt.block_on(async {
            drop(db);
        });

        rt.shutdown_background();
    }

    group.finish();
}

criterion_group!(benches_sequential, bulk_insert, sequential_insert_read);
criterion_group!(benches_concurrent, concurrent_insert);
criterion_main!(benches_concurrent);
