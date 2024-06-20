use bytes::Buf;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fmmap::{MmapFile, MmapFileExt};
use memmap2::MmapOptions;
use rand::Rng;
use std::fs::File;
use std::io::{self, Read, Write};
use std::path::Path;

// Create a large file with random data
fn create_large_file<P: AsRef<Path>>(path: P, size: usize) -> io::Result<()> {
    let mut file = File::create(path)?;
    let mut rng = rand::thread_rng();
    let mut remaining = size;

    while remaining > 0 {
        let mut data = [0u8; 1024];
        rng.fill(&mut data[..]);
        let to_write = std::cmp::min(1024, remaining);
        file.write_all(&data[..to_write])?;
        remaining -= to_write;
    }

    Ok(())
}

// Benchmark for normal file read
fn benchmark_normal_file_read(path: &Path) {
    let mut file = File::open(path).unwrap();
    let mut buffer = [0u8; 4096];

    while let Ok(n) = file.read(&mut buffer) {
        if n == 0 {
            break;
        }
        black_box(&buffer[..n]);
    }
}

// Benchmark for mmap file read
fn benchmark_memmap2_file_read(path: &Path, size: usize) {
    let file = File::open(path).unwrap();
    let mut buffer = [0u8; 4096];
    let mut offset = 0usize; // Initialize offset at 0
    let mmap = unsafe { MmapOptions::new().map(&file).unwrap() };
    let mut reader = mmap.reader();

    loop {
        if offset >= size {
            break;
        }
        let n = reader.read(&mut buffer).unwrap();
        if n == 0 {
            break;
        }
        black_box(&buffer[..n]);
        offset += n; // Update offset by the number of bytes read
    }
}

// Benchmark for mmap file read
fn benchmark_fmmap_file_read(path: &Path, size: usize) {
    let file = MmapFile::open(path).unwrap();
    let mut buffer = [0u8; 4096];
    let mut offset = 0usize; // Initialize offset at 0

    loop {
        if offset >= size {
            break;
        }
        let n = file.read(&mut buffer, offset);
        if n == 0 {
            break;
        }
        black_box(&buffer[..n]);
        offset += n; // Update offset by the number of bytes read
    }
}

// Criterion benchmark harness
fn criterion_benchmark(c: &mut Criterion) {
    let file_path = "large_file.bin";
    let file_size = 128 * 1024 * 1024; // 128 MB
    create_large_file(file_path, file_size).unwrap();

    c.bench_function("normal_file_read", |b| {
        b.iter(|| benchmark_normal_file_read(Path::new(file_path)))
    });
    c.bench_function("memmap2_file_read", |b| {
        b.iter(|| benchmark_memmap2_file_read(Path::new(file_path), file_size))
    });
    c.bench_function("fmmap_file_read", |b| {
        b.iter(|| benchmark_fmmap_file_read(Path::new(file_path), file_size))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
