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
        opts.max_active_snapshots = 100000000;
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
    let mut opts = Options::new();
    opts.max_active_snapshots = 100000000;
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
        })
    });

    c.bench_function("sequential gets", |b| {
        let mut count = 0_u32;
        b.iter(|| {
            count += 1;
            let txn = db.begin().unwrap();
            txn.get(count.to_be_bytes()[..].into()).unwrap();
        })
    });
}

criterion_group!(
    benches,
    bulk_insert,
    // sequential_insert_read
);
criterion_main!(benches);
