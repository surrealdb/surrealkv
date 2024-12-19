use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use surrealkv::storage::log::Segment;
use std::path::PathBuf;
use rand::Rng;
use rand::thread_rng;
use rand::RngCore;

use surrealkv::storage::log::Options;


fn cleanup_test_files(dir_path: &PathBuf) {
    if dir_path.exists() {
        std::fs::remove_dir_all(dir_path).expect("Failed to clean up test directory");
    }
}

const TEST_DIR: &str = "benchmark_test_files";
const CHUNK_SIZE: usize = 4096;
const NUM_CHUNKS: usize = 10_000; // Creates ~40MB of test data
const RANGE_SIZES: &[usize] = &[64_000, 256_000, 1_024_000]; // Different range sizes to test

#[derive(Debug)]
enum ReadPatternType {
    Sequential,
    RandomFixed,
    Random,
    Clustered,
}

struct ReadPattern {
    pattern_type: ReadPatternType,
    offsets: Vec<u64>,
    size_per_read: usize,
}

fn generate_read_patterns(segment_size: u64, pattern_size: usize) -> Vec<ReadPattern> {
    let mut rng = thread_rng();
    let mut patterns = Vec::new();
    
    // Sequential range scan
    patterns.push(ReadPattern {
        pattern_type: ReadPatternType::Sequential,
        offsets: (0..segment_size).step_by(CHUNK_SIZE)
            .take(pattern_size / CHUNK_SIZE)
            .collect(),
        size_per_read: CHUNK_SIZE,
    });
    
    // Random range scan with fixed intervals
    let mut offsets = Vec::with_capacity(pattern_size / CHUNK_SIZE);
    let mut current = 0;
    while current < segment_size && offsets.len() < pattern_size / CHUNK_SIZE {
        offsets.push(current);
        current += rng.gen_range(CHUNK_SIZE..CHUNK_SIZE * 10) as u64;
    }
    patterns.push(ReadPattern {
        pattern_type: ReadPatternType::RandomFixed,
        offsets,
        size_per_read: CHUNK_SIZE,
    });
    
    // Completely random range scan
    patterns.push(ReadPattern {
        pattern_type: ReadPatternType::Random,
        offsets: (0..pattern_size / CHUNK_SIZE)
            .map(|_| rng.gen_range(0..segment_size))
            .collect(),
        size_per_read: CHUNK_SIZE,
    });
    
    // Clustered range scan (simulates locality of reference)
    let cluster_size = pattern_size / 10;
    let num_clusters = 10;
    let mut cluster_offsets = Vec::with_capacity(pattern_size / CHUNK_SIZE);
    
    for _ in 0..num_clusters {
        let cluster_start = rng.gen_range(0..segment_size - cluster_size as u64);
        for offset in (cluster_start..cluster_start + cluster_size as u64)
            .step_by(CHUNK_SIZE) {
            if offset < segment_size {
                cluster_offsets.push(offset);
            }
        }
    }
    patterns.push(ReadPattern {
        pattern_type: ReadPatternType::Clustered,
        offsets: cluster_offsets,
        size_per_read: CHUNK_SIZE,
    });
    
    patterns
}

fn setup_test_segment() -> (Segment, PathBuf) {
    let dir_path = PathBuf::from(TEST_DIR);
    std::fs::create_dir_all(&dir_path).expect("Failed to create test directory");

    let mut opts = Options::default();
    opts = opts.with_max_file_size((NUM_CHUNKS * CHUNK_SIZE) as u64);

    let mut segment = Segment::open(&dir_path, 0, &opts)
        .expect("Failed to create segment");

    let mut rng = thread_rng();
    let mut chunk = vec![0u8; CHUNK_SIZE];

    // Write random data to segment
    for _ in 0..NUM_CHUNKS {
        rng.fill_bytes(&mut chunk);
        segment.append(&chunk).expect("Failed to write chunk");
    }

    segment.sync().expect("Failed to sync segment");
    (segment, dir_path)
}

fn benchmark_range_scans(c: &mut Criterion) {
    let (segment, dir_path) = setup_test_segment();
    let segment_size = segment.offset();
    
    let mut group = c.benchmark_group("range_scans");
    
    // Test different range sizes
    for &range_size in RANGE_SIZES {
        let patterns = generate_read_patterns(segment_size, range_size);
        
        for pattern in patterns {
            let pattern_name = match pattern.pattern_type {
                ReadPatternType::Sequential => "sequential",
                ReadPatternType::RandomFixed => "random_fixed",
                ReadPatternType::Random => "random",
                ReadPatternType::Clustered => "clustered",
            };
            
            let mut buffer = vec![0u8; pattern.size_per_read];
            
            group.bench_with_input(
                BenchmarkId::new(format!("{}-{}", pattern_name, range_size), range_size),
                &pattern,
                |b, pattern| {
                    b.iter(|| {
                        for &offset in &pattern.offsets {
                            black_box(segment.read_at(&mut buffer, offset).unwrap());
                        }
                    })
                },
            );
        }
    }
    
    group.finish();
    cleanup_test_files(&dir_path);
}

criterion_group!(benches, benchmark_range_scans);
criterion_main!(benches);