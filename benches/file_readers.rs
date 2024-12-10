use std::{path::Path, sync::Arc};

mod fd;
use fd::FileDescriptorTable;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use parking_lot::Mutex;
use surrealkv::log::{fd::SegmentReaderPool, Options as LogOptions, Segment};
use tempdir::TempDir;
use tokio::runtime::Runtime;

// Simple mutex-protected segment
struct MutexSegment {
    segment: Arc<Mutex<Segment>>,
}

impl MutexSegment {
    fn new(dir: &Path, id: u64, opts: &LogOptions) -> Self {
        Self {
            segment: Arc::new(Mutex::new(Segment::open(dir, id, opts, true).unwrap())),
        }
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> usize {
        self.segment.lock().read_at(buf, offset).unwrap()
    }
}

async fn run_mutex_benchmark(
    mutex_segment: Arc<MutexSegment>,
    concurrency: usize,
    ops_per_task: usize,
    read_size: usize,
    segment_size: usize,
) {
    let mut handles = Vec::with_capacity(concurrency);

    for task_id in 0..concurrency {
        let segment = Arc::clone(&mutex_segment);
        let handle = tokio::spawn(async move {
            let mut buf = vec![0u8; read_size];
            for i in 0..ops_per_task {
                let offset = ((task_id * ops_per_task + i) * read_size) as u64
                    % (segment_size as u64 - read_size as u64);
                segment.read_at(&mut buf, offset);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

async fn run_fd_table_benchmark(
    fd_table: Arc<FileDescriptorTable>,
    concurrency: usize,
    ops_per_task: usize,
    read_size: usize,
    segment_size: usize,
) {
    let mut handles = Vec::with_capacity(concurrency);

    for task_id in 0..concurrency {
        let fd_table = Arc::clone(&fd_table);
        let handle = tokio::spawn(async move {
            let mut buf = vec![0u8; read_size];
            for i in 0..ops_per_task {
                let offset = ((task_id * ops_per_task + i) * read_size) as u64
                    % (segment_size as u64 - read_size as u64);
                fd_table.read_at(1, &mut buf, offset).unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

async fn run_pool_benchmark(
    pool: Arc<SegmentReaderPool>,
    concurrency: usize,
    ops_per_task: usize,
    read_size: usize,
    segment_size: usize,
) {
    let mut handles = Vec::with_capacity(concurrency);

    for task_id in 0..concurrency {
        let pool = Arc::clone(&pool);
        let handle = tokio::spawn(async move {
            let mut buf = vec![0u8; read_size];
            for i in 0..ops_per_task {
                let offset = ((task_id * ops_per_task + i) * read_size) as u64
                    % (segment_size as u64 - read_size as u64);
                let reader = pool.acquire_reader().unwrap();
                reader
                    .segment
                    .as_ref()
                    .unwrap()
                    .read_at(&mut buf, offset)
                    .unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

fn setup_benchmark() -> (TempDir, LogOptions) {
    let dir = TempDir::new("test").unwrap();
    let opts = LogOptions::default();

    // Create test segment
    let segment_size = 1024 * 1024; // 1MB
    let data = vec![42u8; segment_size];
    let mut segment = Segment::open(dir.path(), 1, &opts, false).unwrap();
    segment.append(&data).unwrap();
    segment.close().unwrap();

    (dir, opts)
}

fn bench_concurrent_reads(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (dir, opts) = setup_benchmark();

    let concurrency_levels = [1, 4, 8, 16, 32];
    let mut group = c.benchmark_group("concurrent_reads");

    for &concurrency in concurrency_levels.iter() {
        // Setup parameters
        let ops_per_task = 1000;
        let read_size = 4096;
        let segment_size = 1024 * 1024;

        // Setup implementations
        let fd_table = Arc::new(FileDescriptorTable::new(concurrency));
        fd_table
            .insert_segment(dir.path(), 1, opts.clone().into())
            .unwrap();

        let reader_pool = Arc::new(
            SegmentReaderPool::new(dir.path().to_path_buf(), 1, (opts).clone(), concurrency)
                .unwrap(),
        );

        let mutex_segment = Arc::new(MutexSegment::new(dir.path(), 1, &opts));

        // Benchmark FDTable
        group.bench_with_input(
            BenchmarkId::new("fd_table", concurrency),
            &concurrency,
            |b, &c| {
                b.iter(|| {
                    rt.block_on(run_fd_table_benchmark(
                        Arc::clone(&fd_table),
                        c,
                        ops_per_task,
                        read_size,
                        segment_size,
                    ))
                });
            },
        );

        // Benchmark SegmentReaderPool
        group.bench_with_input(
            BenchmarkId::new("reader_pool", concurrency),
            &concurrency,
            |b, &c| {
                b.iter(|| {
                    rt.block_on(run_pool_benchmark(
                        Arc::clone(&reader_pool),
                        c,
                        ops_per_task,
                        read_size,
                        segment_size,
                    ))
                });
            },
        );

        // Benchmark Mutex Segment
        group.bench_with_input(
            BenchmarkId::new("mutex_segment", concurrency),
            &concurrency,
            |b, &c| {
                b.iter(|| {
                    rt.block_on(run_mutex_benchmark(
                        Arc::clone(&mutex_segment),
                        c,
                        ops_per_task,
                        read_size,
                        segment_size,
                    ))
                });
            },
        );
    }
    group.finish();
}

fn bench_high_contention(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (dir, opts) = setup_benchmark();

    let mut group = c.benchmark_group("high_contention");

    // High contention scenario: many tasks, few handles
    let concurrency = 32;
    let max_handles = 4;
    let ops_per_task = 1000;
    let read_size = 4096;
    let segment_size = 1024 * 1024;

    // Setup implementations
    let fd_table = Arc::new(FileDescriptorTable::new(max_handles));
    fd_table
        .insert_segment(dir.path(), 1, opts.clone().into())
        .unwrap();

    let reader_pool = Arc::new(
        SegmentReaderPool::new(dir.path().to_path_buf(), 1, (opts).clone(), max_handles).unwrap(),
    );

    let mutex_segment = Arc::new(MutexSegment::new(dir.path(), 1, &opts));

    group.bench_function("fd_table_contention", |b| {
        b.iter(|| {
            rt.block_on(run_fd_table_benchmark(
                Arc::clone(&fd_table),
                concurrency,
                ops_per_task,
                read_size,
                segment_size,
            ))
        });
    });

    group.bench_function("reader_pool_contention", |b| {
        b.iter(|| {
            rt.block_on(run_pool_benchmark(
                Arc::clone(&reader_pool),
                concurrency,
                ops_per_task,
                read_size,
                segment_size,
            ))
        });
    });

    group.bench_function("mutex_segment_contention", |b| {
        b.iter(|| {
            rt.block_on(run_mutex_benchmark(
                Arc::clone(&mutex_segment),
                concurrency,
                ops_per_task,
                read_size,
                segment_size,
            ))
        });
    });

    group.finish();
}

criterion_group!(benches, bench_concurrent_reads, bench_high_contention);
criterion_main!(benches);
