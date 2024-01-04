use criterion::{criterion_group, criterion_main, Criterion};
use jemallocator::Jemalloc;

use surrealkv::storage::kv::option::Options;
use surrealkv::storage::kv::store::Store;
use tempdir::TempDir;

#[cfg_attr(any(target_os = "linux", target_os = "macos"), global_allocator)]
static ALLOC: Jemalloc = Jemalloc;

fn create_temp_directory() -> TempDir {
    TempDir::new("test").unwrap()
}

fn bulk_insert(c: &mut Criterion) {
    let mut count = 0_u32;
    let mut bytes = |len| -> Vec<u8> {
        count += 1;
        count
            .to_be_bytes()
            .into_iter()
            .cycle()
            .take(len)
            .clone()
            .collect()
    };

    let mut bench = |key_len, val_len| {
        let mut opts = Options::new();
        opts.dir = create_temp_directory().path().to_path_buf();
        let db = Store::new(opts).expect("should create store");

        c.bench_function(
            &format!("bulk insert key/value lengths {}/{}", key_len, val_len),
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

    for key_len in &[10_usize, 128, 256, 512] {
        for val_len in &[0_usize, 10, 128, 256, 512, 1024, 2048, 4096, 8192] {
            bench(*key_len, *val_len)
        }
    }
}

fn sequential_insert_read(c: &mut Criterion) {
    let mut max_count = 0_u32;
    let mut opts = Options::new();
    opts.dir = create_temp_directory().path().to_path_buf();
    let db = Store::new(opts).expect("should create store");

    c.bench_function("sequential inserts", |b| {
        let mut count = 0_u32;
        b.iter(|| {
            count += 1;
            let mut txn = db.begin().unwrap();
            txn.set(count.to_be_bytes()[..].into(), vec![][..].into())
                .unwrap();
            txn.commit().unwrap();
            if count > max_count {
                max_count = count;
            }
        });
    });

    c.bench_function("sequential gets", |b| {
        let mut count = 0_u32;
        b.iter(|| {
            count += 1;
            // not sure why this exceeds the max_count
            if count <= max_count {
                let txn = db.begin().unwrap();
                txn.get(count.to_be_bytes()[..].into()).unwrap();
            }
        })
    });
}

fn concurrent_insert(c: &mut Criterion) {
    let item_count = 100_000;

    let mut group = c.benchmark_group("inserts");
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(item_count as u64));

    let mut opts = Options::new();
    opts.dir = create_temp_directory().path().to_path_buf();
    let db = Store::new(opts).expect("should create store");

    for thread_count in [1_u32, 2, 4] {
        group.bench_function(
            format!("{} inserts ({} threads)", item_count, thread_count),
            |b| {
                b.iter(|| {
                    let mut threads = vec![];

                    for _ in 0..thread_count {
                        let db = db.clone();

                        threads.push(std::thread::spawn(move || {
                            for _ in 0..(item_count / thread_count) {
                                let key = nanoid::nanoid!();
                                let value = nanoid::nanoid!();
                                let mut txn = db.begin().unwrap();
                                txn.set(key.as_bytes(), value.as_bytes()).unwrap();
                                txn.commit().unwrap();
                            }
                        }));
                    }

                    for thread in threads {
                        thread.join().unwrap();
                    }
                })
            },
        );
    }
}

criterion_group!(benches_sequential, bulk_insert, sequential_insert_read);
criterion_group!(benches_concurrent, concurrent_insert);
criterion_main!(benches_sequential, benches_concurrent);
