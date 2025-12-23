use std::sync::Arc;

use criterion::{
	black_box,
	criterion_group,
	criterion_main,
	BatchSize,
	BenchmarkId,
	Criterion,
	Throughput,
};
use rand::Rng;
use surrealkv::bplustree::tree::new_disk_tree;
use surrealkv::BytewiseComparator;
use tempfile::TempDir;

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
			b.iter_batched(
				|| {
					// Setup - not timed
					let temp_dir = TempDir::new().unwrap();
					let db_path = temp_dir.path().join("test.db");
					let comparator = Arc::new(BytewiseComparator {});
					let tree = new_disk_tree(&db_path, comparator).unwrap();
					let data = generate_test_data(size, 16, 64);
					(tree, data, temp_dir)
				},
				|(mut tree, data, _temp_dir)| {
					// Measurement - this is timed
					for (key, value) in data {
						tree.insert(black_box(&key), black_box(&value)).unwrap();
					}
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

fn benchmark_insert_random(c: &mut Criterion) {
	let mut group = c.benchmark_group("bplustree_insert_random");

	for size in [100, 1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("insert", size), size, |b, &size| {
			b.iter_batched(
				|| {
					// Setup - not timed
					let temp_dir = TempDir::new().unwrap();
					let db_path = temp_dir.path().join("test.db");
					let comparator = Arc::new(BytewiseComparator {});
					let tree = new_disk_tree(&db_path, comparator).unwrap();
					let mut data = generate_test_data(size, 16, 64);
					// Shuffle in setup
					use rand::seq::SliceRandom;
					data.shuffle(&mut rand::rng());
					(tree, data, temp_dir)
				},
				|(mut tree, data, _temp_dir)| {
					// Measurement - this is timed
					for (key, value) in data {
						tree.insert(black_box(&key), black_box(&value)).unwrap();
					}
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

fn benchmark_delete_sequential(c: &mut Criterion) {
	let mut group = c.benchmark_group("bplustree_delete_sequential");

	for size in [100, 1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("delete", size), size, |b, &size| {
			b.iter_batched(
				|| {
					// Setup - not timed
					let temp_dir = TempDir::new().unwrap();
					let db_path = temp_dir.path().join("test.db");
					let comparator = Arc::new(BytewiseComparator {});
					let mut tree = new_disk_tree(&db_path, comparator).unwrap();

					// Insert data in setup
					let data = generate_test_data(size, 16, 64);
					for (key, value) in &data {
						tree.insert(key, value).unwrap();
					}

					(tree, data, temp_dir)
				},
				|(mut tree, data, _temp_dir)| {
					// Only measure deletes
					for (key, _) in data {
						tree.delete(black_box(&key)).unwrap();
					}
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

fn benchmark_delete_random(c: &mut Criterion) {
	let mut group = c.benchmark_group("bplustree_delete_random");

	for size in [100, 1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("delete", size), size, |b, &size| {
			b.iter_batched(
				|| {
					// Setup - not timed
					let temp_dir = TempDir::new().unwrap();
					let db_path = temp_dir.path().join("test.db");
					let comparator = Arc::new(BytewiseComparator {});
					let mut tree = new_disk_tree(&db_path, comparator).unwrap();

					// Insert data in setup
					let mut data = generate_test_data(size, 16, 64);
					for (key, value) in &data {
						tree.insert(key, value).unwrap();
					}

					// Shuffle for random deletion order
					use rand::seq::SliceRandom;
					data.shuffle(&mut rand::rng());

					(tree, data, temp_dir)
				},
				|(mut tree, data, _temp_dir)| {
					// Only measure deletes
					for (key, _) in data {
						tree.delete(black_box(&key)).unwrap();
					}
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

fn benchmark_get_operations(c: &mut Criterion) {
	let mut group = c.benchmark_group("bplustree_get_operations");

	for size in [1000, 10000].iter() {
		group.throughput(Throughput::Elements(*size as u64));

		group.bench_with_input(BenchmarkId::new("get", size), size, |b, &size| {
			b.iter_batched(
				|| {
					// Setup - not timed
					let temp_dir = TempDir::new().unwrap();
					let db_path = temp_dir.path().join("test.db");
					let comparator = Arc::new(BytewiseComparator {});
					let mut tree = new_disk_tree(&db_path, comparator).unwrap();

					// Insert data in setup
					let data = generate_test_data(size, 16, 64);
					for (key, value) in &data {
						tree.insert(key, value).unwrap();
					}

					(tree, data, temp_dir)
				},
				|(tree, data, _temp_dir)| {
					// Only measure gets
					for (key, _) in data {
						let _result = tree.get(black_box(&key)).unwrap();
					}
				},
				BatchSize::SmallInput,
			);
		});
	}

	group.finish();
}

criterion_group!(
	benches,
	benchmark_insert_sequential,
	benchmark_insert_random,
	benchmark_delete_sequential,
	benchmark_delete_random,
	benchmark_get_operations
);
criterion_main!(benches);
