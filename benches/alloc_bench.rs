use std::sync::Arc;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use surrealkv::{BytewiseComparator, TreeBuilder};
use tempfile::TempDir;
use tokio::runtime::{Handle, Runtime};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
	divan::main();
}

const COUNTS: &[usize] = &[10_000];
const COMMIT_SIZES: &[usize] = &[1, 10, 100, 1000, 10_000];
const SIZES: &[(usize, usize)] = &[(4, 8), (16, 64), (256, 4096)];

// Fixed seed for reproducible benchmarks
const BENCH_SEED: u64 = 0x5EED_CAFE_DEAD_BEEF;

#[divan::bench(args = COUNTS)]
pub fn seq_insert(b: divan::Bencher<'_, '_>, count: usize) {
	let temp_dir = TempDir::new().unwrap();
	let db_path = temp_dir.path().to_path_buf();
	let rt = Runtime::new().unwrap();
	let _guard = rt.enter();

	let tree = TreeBuilder::new()
		.with_comparator(Arc::new(BytewiseComparator {}))
		.with_path(db_path)
		.build()
		.unwrap();

	let mut rng = StdRng::seed_from_u64(BENCH_SEED);

	b.counter(count).bench_local(|| {
		let key: [u8; 16] = rng.random();
		let value: [u8; 64] = rng.random();
		let mut txn = tree.begin().unwrap();
		txn.set(&key, &value).unwrap();
		Handle::current().block_on(async {
			txn.commit().await.unwrap();
		});
	});
}

#[divan::bench(args = COUNTS)]
pub fn seq_get(b: divan::Bencher<'_, '_>, count: usize) {
	let temp_dir = TempDir::new().unwrap();
	let db_path = temp_dir.path().to_path_buf();
	let rt = Runtime::new().unwrap();
	let _guard = rt.enter();

	let tree = TreeBuilder::new()
		.with_comparator(Arc::new(BytewiseComparator {}))
		.with_path(db_path)
		.build()
		.unwrap();

	let mut rng = StdRng::seed_from_u64(BENCH_SEED);
	let mut keys = Vec::with_capacity(count);
	let mut txn = tree.begin().unwrap();

	// Insert data first
	for _ in 0..count {
		let key: [u8; 16] = rng.random();
		let value: [u8; 64] = rng.random();
		txn.set(&key, &value).unwrap();
		keys.push(key);
	}
	Handle::current().block_on(async {
		txn.commit().await.unwrap();
	});

	b.counter(count).bench_local(|| {
		let txn = tree.begin().unwrap();
		for key in &keys {
			let _ = txn.get(key).unwrap();
		}
	});
}

#[divan::bench(args = COUNTS)]
pub fn seq_range(b: divan::Bencher<'_, '_>, count: usize) {
	let temp_dir = TempDir::new().unwrap();
	let db_path = temp_dir.path().to_path_buf();
	let rt = Runtime::new().unwrap();
	let _guard = rt.enter();

	let tree = TreeBuilder::new()
		.with_comparator(Arc::new(BytewiseComparator {}))
		.with_path(db_path)
		.build()
		.unwrap();

	let mut rng = StdRng::seed_from_u64(BENCH_SEED);
	let mut keys = Vec::with_capacity(count);
	let mut txn = tree.begin().unwrap();

	// Insert data first
	for _ in 0..count {
		let key: Vec<u8> = rng.random::<[u8; 16]>().to_vec();
		let value: Vec<u8> = rng.random::<[u8; 64]>().to_vec();
		txn.set(&key, &value).unwrap();
		keys.push(key);
	}
	Handle::current().block_on(async {
		txn.commit().await.unwrap();
	});

	// Sort keys to get min for range start
	keys.sort();

	// Use a sentinel value that's guaranteed to be after all keys (all 0xFF)
	let end_bound = [0xFFu8; 16].to_vec();

	b.counter(count).bench_local(|| {
		{
			let txn = tree.begin().unwrap();
			// Scan entire range from first key to end_bound (covers all keys)
			let range_iter = txn.range(keys[0].clone(), end_bound.clone()).unwrap();
			let _results: Vec<_> = range_iter.map(|r| r.unwrap()).collect();
		}
	});
}

#[divan::bench(args = COUNTS)]
pub fn seq_delete(b: divan::Bencher<'_, '_>, count: usize) {
	let temp_dir = TempDir::new().unwrap();
	let db_path = temp_dir.path().to_path_buf();
	let rt = Runtime::new().unwrap();
	let _guard = rt.enter();

	let tree = TreeBuilder::new()
		.with_comparator(Arc::new(BytewiseComparator {}))
		.with_path(db_path)
		.build()
		.unwrap();

	let mut rng = StdRng::seed_from_u64(BENCH_SEED);
	let mut keys = Vec::with_capacity(count);

	// Insert data first
	let mut txn = tree.begin().unwrap();
	for _ in 0..count {
		let key: [u8; 16] = rng.random();
		let value: [u8; 64] = rng.random();
		txn.set(&key, &value).unwrap();
		keys.push(key);
	}
	Handle::current().block_on(async {
		txn.commit().await.unwrap();
	});

	let mut key_index = 0;
	b.counter(count).bench_local(|| {
		let key = &keys[key_index % keys.len()];
		key_index += 1;
		let mut txn = tree.begin().unwrap();
		txn.delete(key).unwrap();
		Handle::current().block_on(async {
			txn.commit().await.unwrap();
		});
	});
}

#[divan::bench(args = COUNTS)]
pub fn seq_update(b: divan::Bencher<'_, '_>, count: usize) {
	let temp_dir = TempDir::new().unwrap();
	let db_path = temp_dir.path().to_path_buf();
	let rt = Runtime::new().unwrap();
	let _guard = rt.enter();

	let tree = TreeBuilder::new()
		.with_comparator(Arc::new(BytewiseComparator {}))
		.with_path(db_path)
		.build()
		.unwrap();

	let mut rng = StdRng::seed_from_u64(BENCH_SEED);
	let mut keys = Vec::with_capacity(count);

	// Insert initial data
	let mut txn = tree.begin().unwrap();
	for _ in 0..count {
		let key: [u8; 16] = rng.random();
		let value: [u8; 64] = rng.random();
		txn.set(&key, &value).unwrap();
		keys.push(key);
	}
	Handle::current().block_on(async {
		txn.commit().await.unwrap();
	});

	let mut key_index = 0;
	b.counter(count).bench_local(|| {
		// Update existing key with new value
		let key = &keys[key_index % keys.len()];
		key_index += 1;
		let value: [u8; 64] = rng.random();
		let mut txn = tree.begin().unwrap();
		txn.set(key, &value).unwrap();
		Handle::current().block_on(async {
			txn.commit().await.unwrap();
		});
	});
}

#[divan::bench(args = SIZES)]
pub fn variable_size_insert(b: divan::Bencher<'_, '_>, (key_size, value_size): (usize, usize)) {
	let temp_dir = TempDir::new().unwrap();
	let db_path = temp_dir.path().to_path_buf();
	let rt = Runtime::new().unwrap();
	let _guard = rt.enter();

	let tree = TreeBuilder::new()
		.with_comparator(Arc::new(BytewiseComparator {}))
		.with_path(db_path)
		.build()
		.unwrap();

	let mut rng = StdRng::seed_from_u64(BENCH_SEED);

	b.counter(divan::counter::ItemsCount::new(1_usize)).bench_local(|| {
		let mut key = vec![0u8; key_size];
		let mut value = vec![0u8; value_size];
		rng.fill(&mut key[..]);
		rng.fill(&mut value[..]);

		let mut txn = tree.begin().unwrap();
		txn.set(&key, &value).unwrap();
		Handle::current().block_on(async {
			txn.commit().await.unwrap();
		});
	});
}

#[divan::bench(args = COUNTS)]
pub fn mixed_workload(b: divan::Bencher<'_, '_>, count: usize) {
	let temp_dir = TempDir::new().unwrap();
	let db_path = temp_dir.path().to_path_buf();
	let rt = Runtime::new().unwrap();
	let _guard = rt.enter();

	let tree = TreeBuilder::new()
		.with_comparator(Arc::new(BytewiseComparator {}))
		.with_path(db_path)
		.build()
		.unwrap();

	let mut rng = StdRng::seed_from_u64(BENCH_SEED);
	let mut keys = Vec::with_capacity(count);

	// Insert initial data
	let mut txn = tree.begin().unwrap();
	for _ in 0..count {
		let key: [u8; 16] = rng.random();
		let value: [u8; 64] = rng.random();
		txn.set(&key, &value).unwrap();
		keys.push(key);
	}
	Handle::current().block_on(async {
		txn.commit().await.unwrap();
	});

	let mut op_count = 0;
	b.counter(divan::counter::ItemsCount::new(10_usize)).bench_local(|| {
		let mut txn = tree.begin().unwrap();

		// Mix: 70% reads, 20% updates, 10% deletes
		for i in 0..10 {
			let key = &keys[(op_count + i) % keys.len()];

			if i < 7 {
				// 70% reads
				let _ = txn.get(key).unwrap();
			} else if i < 9 {
				// 20% updates
				let value: [u8; 64] = rng.random();
				txn.set(key, &value).unwrap();
			} else {
				// 10% deletes
				txn.delete(key).unwrap();
			}
		}

		Handle::current().block_on(async {
			txn.commit().await.unwrap();
		});

		op_count += 10;
	});
}

#[divan::bench(args = COMMIT_SIZES)]
pub fn commit_overhead(b: divan::Bencher<'_, '_>, write_count: usize) {
	let temp_dir = TempDir::new().unwrap();
	let db_path = temp_dir.path().to_path_buf();
	let rt = Runtime::new().unwrap();
	let _guard = rt.enter();

	let tree = TreeBuilder::new()
		.with_comparator(Arc::new(BytewiseComparator {}))
		.with_path(db_path)
		.build()
		.unwrap();

	let mut rng = StdRng::seed_from_u64(BENCH_SEED);

	b.counter(write_count).bench_local(|| {
		let mut txn = tree.begin().unwrap();

		// Write multiple entries before commit
		for _ in 0..write_count {
			let key: [u8; 16] = rng.random();
			let value: [u8; 64] = rng.random();
			txn.set(&key, &value).unwrap();
		}

		Handle::current().block_on(async {
			txn.commit().await.unwrap();
		});
	});
}

#[divan::bench(args = COUNTS)]
pub fn partial_range_iter(b: divan::Bencher<'_, '_>, count: usize) {
	let temp_dir = TempDir::new().unwrap();
	let db_path = temp_dir.path().to_path_buf();
	let rt = Runtime::new().unwrap();
	let _guard = rt.enter();

	let tree = TreeBuilder::new()
		.with_comparator(Arc::new(BytewiseComparator {}))
		.with_path(db_path)
		.build()
		.unwrap();

	let mut rng = StdRng::seed_from_u64(BENCH_SEED);
	let mut keys = Vec::with_capacity(count);
	let mut txn = tree.begin().unwrap();

	// Insert data first
	for _ in 0..count {
		let key: [u8; 16] = rng.random();
		let value: [u8; 64] = rng.random();
		txn.set(&key, &value).unwrap();
		keys.push(key);
	}
	Handle::current().block_on(async {
		txn.commit().await.unwrap();
	});

	// Sort keys to get min for range start
	keys.sort();
	let end_bound = [0xFFu8; 16];

	b.counter(divan::counter::ItemsCount::new(100_usize)).bench_local(|| {
		let txn = tree.begin().unwrap();
		// Create iterator over all items but only consume first 100
		let range_iter = txn.range(&keys[0], &end_bound).unwrap();
		let _results: Vec<_> = range_iter.take(100).map(|r| r.unwrap()).collect();
	});
}

#[divan::bench(args = COUNTS)]
pub fn rollback_test(b: divan::Bencher<'_, '_>, count: usize) {
	let temp_dir = TempDir::new().unwrap();
	let db_path = temp_dir.path().to_path_buf();
	let rt = Runtime::new().unwrap();
	let _guard = rt.enter();

	let tree = TreeBuilder::new()
		.with_comparator(Arc::new(BytewiseComparator {}))
		.with_path(db_path)
		.build()
		.unwrap();

	let mut rng = StdRng::seed_from_u64(BENCH_SEED);

	b.counter(count).bench_local(|| {
		let mut txn = tree.begin().unwrap();

		// Do some writes
		for _ in 0..count {
			let key: [u8; 16] = rng.random();
			let value: [u8; 64] = rng.random();
			txn.set(&key, &value).unwrap();
		}

		// Then rollback instead of commit
		txn.rollback();
	});
}
