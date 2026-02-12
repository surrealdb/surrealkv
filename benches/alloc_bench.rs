use std::sync::Arc;

use rand::Rng;
use surrealkv::{BytewiseComparator, LSMIterator, TreeBuilder};
use tempfile::TempDir;
use tokio::runtime::{Handle, Runtime};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
	divan::main();
}

const COUNTS: &[usize] = &[10_000];

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

	let mut rng = rand::rng();

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

	let mut rng = rand::rng();
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

	let mut rng = rand::rng();
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

	// Use a sentinel value that's guaranteed to be after all keys (all 0xFF)
	let end_bound = [0xFFu8; 16];

	b.counter(count).bench_local(|| {
		{
			let txn = tree.begin().unwrap();
			// Scan entire range from first key to end_bound (covers all keys)
			let mut iter = txn.range(&keys[0], &end_bound).unwrap();
			let mut results = Vec::new();
			iter.seek_first().unwrap();
			let mut item_count = 0;
			while iter.valid() {
				let key = iter.key().to_owned();
				results.push(key);
				iter.next().unwrap();
				item_count += 1;
			}
			let _ = item_count;
		}
	});
}
