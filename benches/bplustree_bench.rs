use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::Rng;
use std::cmp::Ordering;
use std::sync::Arc;
use surrealkv::bplustree::tree::new_disk_tree;
use surrealkv::Comparator;
use tempfile::TempDir;

// Simple lexicographic comparator for benchmarking
#[derive(Clone)]
struct LexicographicComparator;

impl Comparator for LexicographicComparator {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    fn separator(&self, from: &[u8], _to: &[u8]) -> Vec<u8> {
        // Simple separator implementation - just return the 'from' key
        from.to_vec()
    }

    fn successor(&self, key: &[u8]) -> Vec<u8> {
        // Simple successor implementation - increment the last byte
        let mut result = key.to_vec();
        if let Some(last) = result.last_mut() {
            *last = last.wrapping_add(1);
        }
        result
    }

    fn name(&self) -> &str {
        "lexicographic"
    }
}

// Generate test data
fn generate_test_data(size: usize, key_size: usize, value_size: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rng = rand::rng();
    let mut data = Vec::with_capacity(size);
    
    for i in 0..size {
        // Generate deterministic keys to ensure consistent benchmarking
        let key = format!("{:0width$}", i, width = key_size).into_bytes();
        let value: Vec<u8> = (0..value_size).map(|_| rng.random()).collect();
        data.push((key, value));
    }
    
    data
}

fn benchmark_insert_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("bplustree_insert_sequential");
    
    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(BenchmarkId::new("insert", size), size, |b, &size| {
            b.iter(|| {
                let temp_dir = TempDir::new().unwrap();
                let db_path = temp_dir.path().join("test.db");
                let comparator = Arc::new(LexicographicComparator);
                let mut tree = new_disk_tree(&db_path, comparator).unwrap();
                
                let data = generate_test_data(size, 16, 64);
                
                for (key, value) in data {
                    tree.insert(black_box(&key), black_box(&value)).unwrap();
                }
            });
        });
    }
    
    group.finish();
}

fn benchmark_insert_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("bplustree_insert_random");
    
    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(BenchmarkId::new("insert", size), size, |b, &size| {
            b.iter(|| {
                let temp_dir = TempDir::new().unwrap();
                let db_path = temp_dir.path().join("test.db");
                let comparator = Arc::new(LexicographicComparator);
                let mut tree = new_disk_tree(&db_path, comparator).unwrap();
                
                let mut data = generate_test_data(size, 16, 64);
                // Shuffle the data for random insertion order
                use rand::seq::SliceRandom;
                data.shuffle(&mut rand::rng());
                
                for (key, value) in data {
                    tree.insert(black_box(&key), black_box(&value)).unwrap();
                }
            });
        });
    }
    
    group.finish();
}

fn benchmark_delete_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("bplustree_delete_sequential");
    
    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(BenchmarkId::new("delete", size), size, |b, &size| {
            b.iter(|| {
                let temp_dir = TempDir::new().unwrap();
                let db_path = temp_dir.path().join("test.db");
                let comparator = Arc::new(LexicographicComparator);
                let mut tree = new_disk_tree(&db_path, comparator).unwrap();
                
                // First insert data
                let data = generate_test_data(size, 16, 64);
                for (key, value) in &data {
                    tree.insert(key, value).unwrap();
                }
                
                // Then delete all data
                for (key, _) in data {
                    tree.delete(black_box(&key)).unwrap();
                }
            });
        });
    }
    
    group.finish();
}

fn benchmark_delete_random(c: &mut Criterion) {
    let mut group = c.benchmark_group("bplustree_delete_random");
    
    for size in [100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(BenchmarkId::new("delete", size), size, |b, &size| {
            b.iter(|| {
                let temp_dir = TempDir::new().unwrap();
                let db_path = temp_dir.path().join("test.db");
                let comparator = Arc::new(LexicographicComparator);
                let mut tree = new_disk_tree(&db_path, comparator).unwrap();
                
                // First insert data
                let mut data = generate_test_data(size, 16, 64);
                for (key, value) in &data {
                    tree.insert(key, value).unwrap();
                }
                
                // Shuffle for random deletion order
                use rand::seq::SliceRandom;
                data.shuffle(&mut rand::rng());
                
                // Then delete all data
                for (key, _) in data {
                    tree.delete(black_box(&key)).unwrap();
                }
            });
        });
    }
    
    group.finish();
}

fn benchmark_key_value_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("bplustree_key_value_sizes");
    
    let key_value_sizes = [
        (8, 32),   // Small keys and values
        (16, 128), // Medium keys and values
        (32, 512), // Large keys and values
        (64, 1024), // Very large keys and values
    ];
    
    for (key_size, value_size) in key_value_sizes.iter() {
        group.bench_with_input(
            BenchmarkId::new("insert", format!("k{}_v{}", key_size, value_size)),
            &(*key_size, *value_size),
            |b, &(key_size, value_size)| {
                b.iter(|| {
                    let temp_dir = TempDir::new().unwrap();
                    let db_path = temp_dir.path().join("test.db");
                    let comparator = Arc::new(LexicographicComparator);
                    let mut tree = new_disk_tree(&db_path, comparator).unwrap();
                    
                    let data = generate_test_data(10000, key_size, value_size);
                    
                    for (key, value) in data {
                        tree.insert(black_box(&key), black_box(&value)).unwrap();
                    }
                });
            },
        );
    }
    
    group.finish();
}

fn benchmark_get_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("bplustree_get_operations");
    
    for size in [1000, 10000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        
        group.bench_with_input(BenchmarkId::new("get", size), size, |b, &size| {
            b.iter(|| {
                let temp_dir = TempDir::new().unwrap();
                let db_path = temp_dir.path().join("test.db");
                let comparator = Arc::new(LexicographicComparator);
                let mut tree = new_disk_tree(&db_path, comparator).unwrap();
                
                // Insert data first
                let data = generate_test_data(size, 16, 64);
                for (key, value) in &data {
                    tree.insert(key, value).unwrap();
                }
                
                // Then perform get operations
                for (key, _) in data {
                    let _result = tree.get(black_box(&key)).unwrap();
                }
            });
        });
    }
    
    group.finish();
}

criterion_group!(
    benches,
    benchmark_insert_sequential,
    // benchmark_insert_random,
    // benchmark_delete_sequential,
    // benchmark_delete_random,
    // benchmark_key_value_sizes,
    // benchmark_get_operations
);
criterion_main!(benches);
