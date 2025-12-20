//! Compression tests for SSTable functionality
//!

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tempdir::TempDir;

use crate::ReadOptions;
use test_log::test;

use crate::sstable::{
	table::{Table, TableWriter},
	InternalKey, InternalKeyKind,
};
use crate::{CompressionType, Iterator, Options};

// ========== Helper Functions ==========

fn create_temp_directory() -> TempDir {
	TempDir::new("compression_test").unwrap()
}

fn create_compression_test_options(path: PathBuf) -> Options {
	Options {
		path,
		compression_per_level: vec![CompressionType::SnappyCompression], // L0: Snappy
		max_memtable_size: 64 * 1024,
		..Default::default()
	}
}

fn default_opts_mut() -> Options {
	let mut opts = Options::new();
	opts.block_restart_interval = 3;
	opts.index_partition_size = 100; // Force multiple partitions
	opts
}

fn wrap_buffer(src: Vec<u8>) -> Arc<dyn crate::vfs::File> {
	Arc::new(src)
}

fn generate_compressible_value(size: usize, pattern: u8) -> Vec<u8> {
	let mut value = Vec::with_capacity(size);
	let patterns = [pattern; 16]; // Repeat 16 times for good compression
	for _ in 0..(size / 16) {
		value.extend_from_slice(&patterns);
	}
	for _ in 0..(size % 16) {
		value.push(pattern);
	}
	value
}

fn generate_random_value(size: usize, rng: &mut StdRng) -> Vec<u8> {
	let mut value = vec![0u8; size];
	rng.fill(&mut value[..]);
	value
}

fn generate_json_like_value(id: usize, size: usize) -> Vec<u8> {
	let base =
		format!(r#"{{"id":{},"name":"user_{}","email":"user{}@example.com","data":"#, id, id, id);
	let mut value = base.into_bytes();
	while value.len() < size - 2 {
		value.push(b'x');
	}
	value.push(b'"');
	value.push(b'}');
	value.truncate(size);
	value
}

fn build_table_with_compression(
	data: Vec<(Vec<u8>, Vec<u8>)>,
	compression: CompressionType,
) -> (Vec<u8>, usize) {
	let mut d = Vec::new();
	let mut opts = default_opts_mut();
	opts.compression_per_level = vec![compression];
	opts.block_size = 64 * 1024;
	let opt = Arc::new(opts);

	{
		let mut builder = TableWriter::new(&mut d, 0, opt, 0);
		for (k, v) in data.iter() {
			builder
				.add(
					InternalKey::new(Bytes::copy_from_slice(k), 1, InternalKeyKind::Set, 0).into(),
					v,
				)
				.unwrap();
		}
		builder.finish().unwrap();
	}

	let size = d.len();
	(d, size)
}

// ========== Compression Tests ==========

#[test]
fn test_compression_10k_pairs_roundtrip() {
	let mut data = Vec::new();
	let mut rng = StdRng::seed_from_u64(12345);

	for i in 0..10_000 {
		let key = format!("key_{:08}", i).into_bytes();
		let value_size = 100 + (i % 400);
		let pattern = (i % 256) as u8;
		let value = generate_compressible_value(value_size, pattern);
		data.push((key, value));
	}

	let (buffer, size) =
		build_table_with_compression(data.clone(), CompressionType::SnappyCompression);

	let opts = {
		let mut opts = default_opts_mut();
		opts.compression_per_level = vec![CompressionType::SnappyCompression];
		Arc::new(opts)
	};

	let table = Arc::new(Table::new(1, opts.clone(), wrap_buffer(buffer), size as u64).unwrap());

	let mut iter = table.iter(ReadOptions::default());
	let mut count = 0;
	iter.seek_to_first();
	while iter.valid() {
		let key = iter.key();
		let value = iter.value();

		assert_eq!(key.user_key.as_ref(), &data[count].0[..]);
		assert_eq!(value.as_ref(), &data[count].1[..]);

		count += 1;
		iter.advance();
	}
	assert_eq!(count, 10_000, "Should iterate through all 10k entries");

	for _ in 0..100 {
		let idx = rng.random_range(0..10_000);
		let seek_key =
			InternalKey::new(Bytes::copy_from_slice(&data[idx].0), 2, InternalKeyKind::Set, 0);
		iter.seek(&seek_key.encode());
		assert!(iter.valid(), "Iterator should be valid after seek");
		assert_eq!(iter.key().user_key.as_ref(), &data[idx].0[..]);
		assert_eq!(iter.value().as_ref(), &data[idx].1[..]);
	}
}

#[test]
fn test_compression_size_reduction() {
	let mut data = Vec::new();

	for i in 0..10_000 {
		let key = format!("key_{:08}", i).into_bytes();
		let value = generate_compressible_value(300, b'A');
		data.push((key, value));
	}

	let (buffer_uncompressed, size_uncompressed) =
		build_table_with_compression(data.clone(), CompressionType::None);

	let (buffer_compressed, size_compressed) =
		build_table_with_compression(data.clone(), CompressionType::SnappyCompression);

	println!("Uncompressed size: {} bytes", size_uncompressed);
	println!("Compressed size: {} bytes", size_compressed);
	println!(
		"Compression ratio: {:.2}%",
		(1.0 - (size_compressed as f64 / size_uncompressed as f64)) * 100.0
	);

	assert!(
		size_compressed < size_uncompressed,
		"Compressed size should be smaller than uncompressed"
	);
	let reduction_ratio = 1.0 - (size_compressed as f64 / size_uncompressed as f64);
	assert!(
		reduction_ratio > 0.20,
		"Compression should reduce size by at least 20%, got {:.2}%",
		reduction_ratio * 100.0
	);

	let opts_uncompressed = {
		let mut opts = default_opts_mut();
		opts.compression_per_level = vec![CompressionType::None];
		Arc::new(opts)
	};
	let opts_compressed = {
		let mut opts = default_opts_mut();
		opts.compression_per_level = vec![CompressionType::SnappyCompression];
		Arc::new(opts)
	};

	let table_uncompressed = Arc::new(
		Table::new(
			1,
			opts_uncompressed,
			wrap_buffer(buffer_uncompressed),
			size_uncompressed as u64,
		)
		.unwrap(),
	);
	let table_compressed = Arc::new(
		Table::new(2, opts_compressed, wrap_buffer(buffer_compressed), size_compressed as u64)
			.unwrap(),
	);

	let mut iter_uncompressed = table_uncompressed.iter(ReadOptions::default());
	let mut iter_compressed = table_compressed.iter(ReadOptions::default());

	iter_uncompressed.seek_to_first();
	iter_compressed.seek_to_first();

	let mut count = 0;
	while iter_uncompressed.valid() && iter_compressed.valid() {
		assert_eq!(iter_uncompressed.key().user_key, iter_compressed.key().user_key);
		assert_eq!(iter_uncompressed.value(), iter_compressed.value());
		count += 1;
		iter_uncompressed.advance();
		iter_compressed.advance();
	}
	assert_eq!(count, 10_000, "Both tables should have identical 10k entries");
}

#[test]
fn test_compression_mixed_patterns() {
	let mut data = Vec::new();
	let mut rng = StdRng::seed_from_u64(54321);

	for i in 0..2_500 {
		let key = format!("highly_compress_{:08}", i).into_bytes();
		let value = generate_compressible_value(400, b'X');
		data.push((key, value));
	}

	for i in 0..2_500 {
		let key = format!("random_data_{:08}", i).into_bytes();
		let value = generate_random_value(400, &mut rng);
		data.push((key, value));
	}

	for i in 0..2_500 {
		let key = format!("json_like_{:08}", i).into_bytes();
		let value = generate_json_like_value(i, 400);
		data.push((key, value));
	}

	for i in 0..2_500 {
		let key = format!("empty_value_{:08}", i).into_bytes();
		let value = Vec::new();
		data.push((key, value));
	}

	// IMPORTANT: Sort data by key since we mixed different patterns
	data.sort_by(|a, b| a.0.cmp(&b.0));

	let (buffer, size) =
		build_table_with_compression(data.clone(), CompressionType::SnappyCompression);

	let opts = {
		let mut opts = default_opts_mut();
		opts.compression_per_level = vec![CompressionType::SnappyCompression];
		Arc::new(opts)
	};

	let table = Arc::new(Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap());

	let mut iter = table.iter(ReadOptions::default());
	iter.seek_to_first();
	let mut count = 0;

	while iter.valid() {
		assert_eq!(iter.key().user_key.as_ref(), &data[count].0[..]);
		assert_eq!(iter.value().as_ref(), &data[count].1[..]);
		count += 1;
		iter.advance();
	}

	assert_eq!(count, 10_000, "Should have all 10k mixed pattern entries");
}

#[test]
fn test_compression_iterator_operations() {
	let mut data = Vec::new();
	let mut rng = StdRng::seed_from_u64(99999);

	for i in 0..10_000 {
		let key = format!("iter_test_{:08}", i).into_bytes();
		let value = generate_compressible_value(200, (i % 256) as u8);
		data.push((key, value));
	}

	let (buffer, size) =
		build_table_with_compression(data.clone(), CompressionType::SnappyCompression);

	let opts = {
		let mut opts = default_opts_mut();
		opts.compression_per_level = vec![CompressionType::SnappyCompression];
		Arc::new(opts)
	};

	let table = Arc::new(Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap());
	let mut iter = table.iter(ReadOptions::default());

	iter.seek_to_first();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key.as_ref(), &data[0].0[..]);
	assert_eq!(iter.value().as_ref(), &data[0].1[..]);

	iter.seek_to_last();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key.as_ref(), &data[9999].0[..]);
	assert_eq!(iter.value().as_ref(), &data[9999].1[..]);

	for _ in 0..100 {
		let idx = rng.random_range(0..10_000);
		let seek_key =
			InternalKey::new(Bytes::copy_from_slice(&data[idx].0), 2, InternalKeyKind::Set, 0);
		iter.seek(&seek_key.encode());
		assert!(iter.valid(), "Should find key at index {}", idx);
		assert_eq!(iter.key().user_key.as_ref(), &data[idx].0[..]);
		assert_eq!(iter.value().as_ref(), &data[idx].1[..]);
	}

	iter.seek_to_first();
	let mut forward_count = 0;
	while iter.valid() {
		assert_eq!(iter.key().user_key.as_ref(), &data[forward_count].0[..]);
		forward_count += 1;
		iter.advance();
	}
	assert_eq!(forward_count, 10_000);

	// TableIterator prev() has complexity with partitions, so test basic backward movement
	iter.seek_to_last();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key.as_ref(), &data[9999].0[..]);

	iter.prev();
	assert!(iter.valid(), "Should be valid after first prev()");

	let prev_key = iter.key().user_key.to_vec();
	assert!(prev_key < data[9999].0, "Previous key should be less than last key");

	for _ in 0..10 {
		if !iter.prev() {
			break;
		}
	}
	assert!(iter.valid(), "Should still be valid after moving backward");

	iter.seek_to_first();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key.as_ref(), &data[0].0[..]);

	iter.seek_to_first();
	assert!(iter.valid());
	iter.advance();
	assert_eq!(iter.key().user_key.as_ref(), &data[1].0[..]);
	iter.prev();
	assert_eq!(iter.key().user_key.as_ref(), &data[0].0[..]);

	let mid_key =
		InternalKey::new(Bytes::copy_from_slice(&data[5000].0), 2, InternalKeyKind::Set, 0);
	iter.seek(&mid_key.encode());
	assert_eq!(iter.key().user_key.as_ref(), &data[5000].0[..]);

	iter.advance();
	iter.advance();
	assert_eq!(iter.key().user_key.as_ref(), &data[5002].0[..]);
	iter.prev();
	assert_eq!(iter.key().user_key.as_ref(), &data[5001].0[..]);
}

#[test]
fn test_compression_large_values() {
	let mut data = Vec::new();

	for i in 0..1_000 {
		let key = format!("large_key_{:08}", i).into_bytes();
		let value_size = 10_000 + (i * 190);
		let pattern = (i % 256) as u8;
		let value = generate_compressible_value(value_size, pattern);
		data.push((key, value));
	}

	let (buffer, size) =
		build_table_with_compression(data.clone(), CompressionType::SnappyCompression);

	println!("Large values table size: {} bytes", size);
	println!(
		"Average value size: {} bytes",
		data.iter().map(|(_, v)| v.len()).sum::<usize>() / data.len()
	);

	let opts = {
		let mut opts = default_opts_mut();
		opts.compression_per_level = vec![CompressionType::SnappyCompression];
		opts.block_size = 64 * 1024;
		Arc::new(opts)
	};

	let table = Arc::new(Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap());

	let mut iter = table.iter(ReadOptions::default());
	iter.seek_to_first();
	let mut count = 0;

	while iter.valid() {
		let key = iter.key();
		let value = iter.value();

		assert_eq!(key.user_key.as_ref(), &data[count].0[..]);
		assert_eq!(value.len(), data[count].1.len(), "Value length mismatch at index {}", count);
		assert_eq!(value.as_ref(), &data[count].1[..], "Value content mismatch at index {}", count);

		count += 1;
		iter.advance();
	}

	assert_eq!(count, 1_000, "Should have all 1000 large value entries");

	let test_indices = [0, 100, 500, 999];
	for &idx in &test_indices {
		let seek_key =
			InternalKey::new(Bytes::copy_from_slice(&data[idx].0), 2, InternalKeyKind::Set, 0);
		iter.seek(&seek_key.encode());
		assert!(iter.valid(), "Should find large value at index {}", idx);
		assert_eq!(iter.key().user_key.as_ref(), &data[idx].0[..]);
		assert_eq!(iter.value().as_ref(), &data[idx].1[..]);
	}
}

#[test]
fn test_compression_checksum_verification() {
	let mut data = Vec::new();

	for i in 0..100 {
		let key = format!("checksum_key_{:08}", i).into_bytes();
		let value = generate_compressible_value(500, b'Z');
		data.push((key, value));
	}

	let (mut buffer, size) =
		build_table_with_compression(data.clone(), CompressionType::SnappyCompression);

	let opts = {
		let mut opts = default_opts_mut();
		opts.compression_per_level = vec![CompressionType::SnappyCompression];
		Arc::new(opts)
	};

	let table =
		Arc::new(Table::new(1, opts.clone(), wrap_buffer(buffer.clone()), size as u64).unwrap());
	let mut iter = table.iter(ReadOptions::default());
	iter.seek_to_first();
	assert!(iter.valid(), "Uncorrupted table should be valid");

	// Corrupt data block (skip footer/metadata at end)
	let corruption_offset = buffer.len() / 2;
	buffer[corruption_offset] ^= 0xFF;

	let corrupted_table_result = Table::new(2, opts.clone(), wrap_buffer(buffer), size as u64);

	// Corruption should be detected either during construction or iteration
	match corrupted_table_result {
		Err(_) => {
			// SUCCESS: Corruption detected during table construction
			println!("Corruption detected during table construction");
		}
		Ok(corrupted_table) => {
			// Corruption might be in a data block - try to iterate
			let corrupted_table = Arc::new(corrupted_table);
			let mut corrupted_iter = corrupted_table.iter(ReadOptions::default());
			corrupted_iter.seek_to_first();

			let mut corruption_detected = false;
			let mut iterations = 0;

			while corrupted_iter.valid() && iterations < 100 {
				let _ = corrupted_iter.value();
				iterations += 1;
				corrupted_iter.advance();
			}

			// If we stopped before reading all 100 entries, corruption was likely detected
			if iterations < 100 {
				corruption_detected = true;
			}

			println!(
				"Corruption test: iterations before stop = {}, corruption_detected = {}",
				iterations, corruption_detected
			);

			// ASSERT: We must have detected corruption during iteration
			assert!(
				corruption_detected,
				"Corruption should have been detected during iteration, but {} entries were read successfully",
				iterations
			);
		}
	}
}

// ========== LSM Integration Tests ==========

#[test(tokio::test)]
async fn test_lsm_compression_10k_keys_with_range_scans() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();
	let opts = create_compression_test_options(path.clone());

	let tree = crate::TreeBuilder::with_options(opts).build().unwrap();

	let mut rng = StdRng::seed_from_u64(42);
	let mut keys = Vec::new();

	for i in 0..10_000 {
		let key = format!("key_{:08}_{:04}", i, rng.random_range(0..10000)).into_bytes();
		keys.push(key);
	}

	keys.sort();
	keys.dedup();

	println!("Inserting {} unique keys with compression enabled", keys.len());

	for (idx, key) in keys.iter().enumerate() {
		let value = generate_compressible_value(200, (idx % 256) as u8);
		let mut txn = tree.begin().unwrap();
		txn.set(key, &value).unwrap();
		txn.commit().await.unwrap();
	}

	tree.flush().unwrap();

	println!("Flushed all keys to SSTable");

	let mut found_count = 0;
	for key in keys.iter() {
		let txn = tree.begin().unwrap();
		let result = txn.get(key).unwrap();
		assert!(result.is_some(), "Key should exist: {:?}", String::from_utf8_lossy(key));
		found_count += 1;
	}
	assert_eq!(found_count, keys.len(), "All keys should be found");
	println!("Verified all {} keys exist", found_count);

	let txn = tree.begin().unwrap();
	let first_key = keys.first().unwrap();

	let mut iter = txn.range(first_key.as_slice(), &[0xFFu8; 100]).unwrap();

	let mut scanned_count = 0;
	let mut prev_key: Option<Vec<u8>> = None;

	while let Some(Ok((key, value))) = iter.next() {
		if let Some(ref prev) = prev_key {
			assert!(
				key.as_ref() > prev.as_slice(),
				"Keys should be in sorted order during range scan"
			);
		}

		assert!(
			keys.iter().any(|k| k.as_slice() == key.as_ref()),
			"Scanned key should be in original key set"
		);

		assert!(!value.is_empty(), "Value should not be empty");

		prev_key = Some(key.to_vec());
		scanned_count += 1;
	}

	assert_eq!(scanned_count, keys.len(), "Range scan should return all keys");
	println!("Range scan successfully iterated through all {} keys", scanned_count);

	let txn = tree.begin().unwrap();
	let start_key = &keys[0];
	let end_key = &keys[99.min(keys.len() - 1)];
	let mut partial_iter = txn.range(start_key.as_slice(), end_key.as_slice()).unwrap();

	let mut partial_count = 0;
	while let Some(Ok(_)) = partial_iter.next() {
		partial_count += 1;
	}
	assert!(
		partial_count >= 1,
		"Partial range scan should return at least 1 key, got {}",
		partial_count
	);
	println!("Partial range scan returned {} keys", partial_count);

	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn test_lsm_compression_persistence_after_reopen() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();
	let opts = create_compression_test_options(path.clone());

	{
		let tree = crate::TreeBuilder::with_options(opts.clone()).build().unwrap();

		let mut rng = StdRng::seed_from_u64(42);
		let mut keys = Vec::new();

		for i in 0..10_000 {
			let key =
				format!("persist_key_{:08}_{:04}", i, rng.random_range(0..10000)).into_bytes();
			keys.push(key);
		}

		keys.sort();
		keys.dedup();

		println!("Phase 1: Inserting {} unique keys with compression...", keys.len());

		for (idx, key) in keys.iter().enumerate() {
			let value = generate_compressible_value(250, (idx % 256) as u8);
			let mut txn = tree.begin().unwrap();
			txn.set(key, &value).unwrap();
			txn.commit().await.unwrap();

			if idx % 2000 == 0 && idx > 0 {
				println!("  Inserted {} keys", idx);
			}
		}

		println!("Flushing to SSTables...");
		tree.flush().unwrap();

		println!("Closing tree...");
		tree.close().await.unwrap();
		println!("Tree closed successfully");
	}

	{
		println!("\nPhase 2: Reopening tree and verifying data...");
		let tree = crate::TreeBuilder::with_options(opts).build().unwrap();

		let mut rng = StdRng::seed_from_u64(42); // Same seed for reproducible keys
		let mut keys = Vec::new();

		for i in 0..10_000 {
			let key =
				format!("persist_key_{:08}_{:04}", i, rng.random_range(0..10000)).into_bytes();
			keys.push(key);
		}

		keys.sort();
		keys.dedup();

		println!("Verifying {} keys exist after reopen...", keys.len());

		let mut found_count = 0;
		for (idx, key) in keys.iter().enumerate() {
			let txn = tree.begin().unwrap();
			let result = txn.get(key).unwrap();
			assert!(
				result.is_some(),
				"Key at index {} should exist after reopen: {:?}",
				idx,
				String::from_utf8_lossy(key)
			);

			let expected_value = generate_compressible_value(250, (idx % 256) as u8);
			let actual_value = result.unwrap();
			assert_eq!(
				actual_value.len(),
				expected_value.len(),
				"Value length mismatch for key at index {}",
				idx
			);
			assert_eq!(
				actual_value.as_ref(),
				expected_value.as_slice(),
				"Value content mismatch for key at index {}",
				idx
			);

			found_count += 1;

			if found_count % 2000 == 0 {
				println!("  Verified {} keys", found_count);
			}
		}

		assert_eq!(found_count, keys.len(), "All keys should be found after reopen");
		println!("All {} keys verified successfully", found_count);

		println!("Testing range scan after reopen...");
		let txn = tree.begin().unwrap();
		let first_key = keys.first().unwrap();
		let mut iter = txn.range(first_key.as_slice(), &[0xFFu8; 100]).unwrap();

		let mut scanned_count = 0;
		let mut prev_key: Option<Vec<u8>> = None;

		while let Some(Ok((key, value))) = iter.next() {
			if let Some(ref prev) = prev_key {
				assert!(
					key.as_ref() > prev.as_slice(),
					"Keys should be in sorted order during range scan after reopen"
				);
			}

			assert!(
				keys.iter().any(|k| k.as_slice() == key.as_ref()),
				"Scanned key should be in original key set"
			);

			assert!(!value.is_empty(), "Value should not be empty");
			assert_eq!(value.len(), 250, "Value should be 250 bytes");

			prev_key = Some(key.to_vec());
			scanned_count += 1;
		}

		assert_eq!(scanned_count, keys.len(), "Range scan should return all keys after reopen");
		println!("✓ Range scan successfully iterated through all {} keys", scanned_count);

		tree.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn test_lsm_compression_disk_size_comparison() {
	let temp_dir_compressed = create_temp_directory();
	let path_compressed = temp_dir_compressed.path().to_path_buf();

	let temp_dir_uncompressed = create_temp_directory();
	let path_uncompressed = temp_dir_uncompressed.path().to_path_buf();

	let opts_compressed = Options {
		path: path_compressed.clone(),
		compression_per_level: vec![CompressionType::SnappyCompression],
		max_memtable_size: 1024 * 1024, // 1MB - defer flush until explicit call
		..Default::default()
	};

	let opts_uncompressed = Options {
		path: path_uncompressed.clone(),
		compression_per_level: vec![CompressionType::None],
		max_memtable_size: 1024 * 1024,
		..Default::default()
	};

	let tree_compressed =
		crate::TreeBuilder::with_options(opts_compressed.clone()).build().unwrap();
	let tree_uncompressed =
		crate::TreeBuilder::with_options(opts_uncompressed.clone()).build().unwrap();

	let mut keys = Vec::new();
	for i in 0..10_000 {
		let key = format!("testkey_{:08}", i).into_bytes();
		keys.push(key);
	}

	println!("Inserting 10k keys into compressed tree...");
	for (idx, key) in keys.iter().enumerate() {
		let value = generate_compressible_value(500, b'A');
		let mut txn = tree_compressed.begin().unwrap();
		txn.set(key, &value).unwrap();
		txn.commit().await.unwrap();

		if idx % 1000 == 0 && idx > 0 {
			println!("  Inserted {} keys", idx);
		}
	}

	println!("Inserting 10k keys into uncompressed tree...");
	for (idx, key) in keys.iter().enumerate() {
		let value = generate_compressible_value(500, b'A');
		let mut txn = tree_uncompressed.begin().unwrap();
		txn.set(key, &value).unwrap();
		txn.commit().await.unwrap();

		if idx % 1000 == 0 && idx > 0 {
			println!("  Inserted {} keys", idx);
		}
	}

	println!("Flushing compressed tree...");
	tree_compressed.flush().unwrap();

	println!("Flushing uncompressed tree...");
	tree_uncompressed.flush().unwrap();

	tree_compressed.close().await.unwrap();
	tree_uncompressed.close().await.unwrap();

	let compressed_sst_dir = path_compressed.join("sstables");
	let uncompressed_sst_dir = path_uncompressed.join("sstables");

	let compressed_size = calculate_directory_size(&compressed_sst_dir);
	let uncompressed_size = calculate_directory_size(&uncompressed_sst_dir);

	println!("\n=== Disk Size Comparison ===");
	println!("Compressed SSTable size:   {} bytes", compressed_size);
	println!("Uncompressed SSTable size: {} bytes", uncompressed_size);

	if compressed_size > 0 && uncompressed_size > 0 {
		let reduction_ratio = 1.0 - (compressed_size as f64 / uncompressed_size as f64);
		println!("Compression ratio:         {:.2}%", reduction_ratio * 100.0);

		assert!(
			compressed_size < uncompressed_size,
			"Compressed size ({}) should be less than uncompressed size ({})",
			compressed_size,
			uncompressed_size
		);

		assert!(
			reduction_ratio > 0.20,
			"Compression should reduce size by at least 20%, got {:.2}%",
			reduction_ratio * 100.0
		);

		println!(
			"\n Compression successfully reduced disk usage by {:.2}%",
			reduction_ratio * 100.0
		);
	} else {
		panic!(
			"Failed to measure disk sizes: compressed={}, uncompressed={}",
			compressed_size, uncompressed_size
		);
	}

	let tree_compressed = crate::TreeBuilder::with_options(opts_compressed).build().unwrap();
	for i in [0, 5000, 9999].iter() {
		let key = format!("testkey_{:08}", i).into_bytes();
		let txn = tree_compressed.begin().unwrap();
		let result = txn.get(&key).unwrap();
		assert!(result.is_some(), "Key {} should exist after reopening compressed tree", i);
	}
	tree_compressed.close().await.unwrap();
}

fn calculate_directory_size(dir: &PathBuf) -> u64 {
	if !dir.exists() {
		return 0;
	}

	let mut total_size = 0u64;
	if let Ok(entries) = std::fs::read_dir(dir) {
		for entry in entries.flatten() {
			if let Ok(metadata) = entry.metadata() {
				if metadata.is_file() {
					total_size += metadata.len();
				}
			}
		}
	}
	total_size
}

// ========== Compression Per Level Tests ==========

#[test]
fn test_compression_per_level_options() {
	// Test default options (empty vector)
	let opts = Options::new();
	assert!(opts.compression_per_level.is_empty());

	// Test setting compression per level
	let opts = Options::new().with_compression_per_level(vec![
		CompressionType::None,              // L0
		CompressionType::SnappyCompression, // L1
		CompressionType::SnappyCompression, // L2+
	]);

	assert_eq!(opts.compression_per_level.len(), 3);
	assert_eq!(opts.compression_per_level[0], CompressionType::None);
	assert_eq!(opts.compression_per_level[1], CompressionType::SnappyCompression);
	assert_eq!(opts.compression_per_level[2], CompressionType::SnappyCompression);

	// Test convenience method for L0 no compression
	let opts = Options::new().with_l0_no_compression();

	assert_eq!(opts.compression_per_level.len(), 2);
	assert_eq!(opts.compression_per_level[0], CompressionType::None);
	assert_eq!(opts.compression_per_level[1], CompressionType::SnappyCompression);
}

#[test]
fn test_compression_selector_per_level() {
	let selector = crate::compression::CompressionSelector::new(vec![
		CompressionType::None,              // L0
		CompressionType::SnappyCompression, // L1
		CompressionType::SnappyCompression, // L2
	]);

	assert_eq!(selector.select_compression(0), CompressionType::None);
	assert_eq!(selector.select_compression(1), CompressionType::SnappyCompression);
	assert_eq!(selector.select_compression(2), CompressionType::SnappyCompression);
	// Higher levels use last configured compression
	assert_eq!(selector.select_compression(3), CompressionType::SnappyCompression);
	assert_eq!(selector.select_compression(10), CompressionType::SnappyCompression);
}

#[test]
fn test_table_writer_with_level_compression() {
	let mut buffer = Vec::new();

	// Test with L0 no compression
	let mut opts = default_opts_mut();
	opts.compression_per_level = vec![CompressionType::SnappyCompression];
	opts.compression_per_level = vec![CompressionType::None, CompressionType::SnappyCompression];
	let opts = Arc::new(opts);

	{
		let mut writer = TableWriter::new(&mut buffer, 1, opts.clone(), 0); // L0
		let data =
			vec![(b"key1".to_vec(), b"value1".to_vec()), (b"key2".to_vec(), b"value2".to_vec())];

		for (key, value) in data {
			let ikey = InternalKey::new(Bytes::copy_from_slice(&key), 1, InternalKeyKind::Set, 0);
			writer.add(ikey.into(), &value).unwrap();
		}
		writer.finish().unwrap();
	}

	let buffer_len = buffer.len() as u64;
	let table = Arc::new(Table::new(1, opts.clone(), wrap_buffer(buffer), buffer_len).unwrap());

	// Verify the table was created successfully
	let mut iter = table.iter(ReadOptions::default());
	iter.seek_to_first();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key.as_ref(), b"key1");
	assert_eq!(iter.value().as_ref(), b"value1");
}

#[test(tokio::test)]
async fn test_compression_per_level_sstable_creation() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let mut opts = Options::new();
	opts.path = path.clone();
	opts.compression_per_level = vec![CompressionType::SnappyCompression];
	// L0: no compression, L1+: Snappy compression
	opts.compression_per_level = vec![CompressionType::None, CompressionType::SnappyCompression];

	let tree = crate::TreeBuilder::with_options(opts).build().unwrap();

	// Insert data that will fill memtable and create L0 SSTable
	let mut keys = Vec::new();
	for i in 0..1000 {
		let key = format!("key_{:04}", i).into_bytes();
		let value = generate_compressible_value(1000, b'A'); // Highly compressible
		keys.push(key.clone());

		let mut txn = tree.begin().unwrap();
		txn.set(&key, &value).unwrap();
		txn.commit().await.unwrap();
	}

	// Force flush to create L0 SSTable
	tree.flush().unwrap();

	// Verify data can be read back
	for key in &keys {
		let txn = tree.begin().unwrap();
		let result = txn.get(key).unwrap();
		assert!(result.is_some(), "Key {:?} should exist", String::from_utf8_lossy(key));
		let value = result.unwrap();
		assert_eq!(value.len(), 1000);
	}

	tree.close().await.unwrap();
}

#[test]
fn test_empty_compression_per_level_defaults_to_none() {
	// Test that empty compression_per_level vector defaults to None compression
	let opts = Options::new();
	// compression_per_level is empty by default

	let selector = crate::compression::CompressionSelector::new(opts.compression_per_level);

	// Should default to None compression for all levels when per_level is empty
	assert_eq!(selector.select_compression(0), CompressionType::None);
	assert_eq!(selector.select_compression(1), CompressionType::None);
	assert_eq!(selector.select_compression(5), CompressionType::None);
}

#[test(tokio::test)]
async fn test_compression_per_level_with_different_levels() {
	let temp_dir = create_temp_directory();
	let path = temp_dir.path().to_path_buf();

	let mut opts = Options::new();
	opts.path = path.clone();
	opts.compression_per_level = vec![CompressionType::SnappyCompression];
	opts.level_count = 4; // L0, L1, L2, L3
	opts.max_memtable_size = 1024 * 1024; // Force frequent flushes
									   // L0: no compression, L1: no compression, L2+: Snappy
	opts.compression_per_level =
		vec![CompressionType::None, CompressionType::None, CompressionType::SnappyCompression];

	let tree = crate::TreeBuilder::with_options(opts).build().unwrap();

	// Insert enough data to potentially create multiple levels
	let mut keys = Vec::new();
	for i in 0..5000 {
		let key = format!("test_key_{:04}", i).into_bytes();
		let value = generate_compressible_value(500, b'X');
		keys.push(key.clone());

		let mut txn = tree.begin().unwrap();
		txn.set(&key, &value).unwrap();
		txn.commit().await.unwrap();
	}

	// Force flush and compaction
	tree.flush().unwrap();

	// The test mainly verifies that the system doesn't crash with per-level compression
	// In a real LSM tree, we'd need to trigger compaction to higher levels to fully test

	let txn = tree.begin().unwrap();
	let result = txn.get(&keys[0]).unwrap();
	assert!(result.is_some(), "Should be able to read data after flush");

	tree.close().await.unwrap();
}
