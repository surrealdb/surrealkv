use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::fs::{remove_file, File};
use std::io::Write;

fn write_all(data: &[u8]) {
    let mut file = File::create("write_all.txt").unwrap();
    file.write_all(data).unwrap();
}

fn write_all_sync(data: &[u8]) {
    let mut file = File::create("write_all_sync.txt").unwrap();
    file.write_all(data).unwrap();
    file.sync_all().unwrap();
}

fn cleanup() {
    remove_file("write_all.txt").unwrap();
    remove_file("write_all_sync.txt").unwrap();
}

fn benchmark(c: &mut Criterion) {
    let data = vec![0u8; 1024 * 1024]; // 1 MB of data

    c.bench_function("write_all", |b| b.iter(|| write_all(black_box(&data))));
    c.bench_function("write_all_sync", |b| {
        b.iter(|| write_all_sync(black_box(&data)))
    });

    cleanup();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
