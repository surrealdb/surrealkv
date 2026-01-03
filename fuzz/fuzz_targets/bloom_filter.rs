#![no_main]
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::sstable::bloom::LevelDBBloomFilter;
use surrealkv::sstable::filter_block::{FilterBlockReader, FilterBlockWriter};

#[derive(Arbitrary, Debug)]
struct FuzzBloomInput {
	keys: Vec<Vec<u8>>,
	bits_per_key: u8,
	query_keys: Vec<Vec<u8>>,
}

fuzz_target!(|data: FuzzBloomInput| {
	// Limit input size
	if data.keys.len() > 10000 || data.query_keys.len() > 1000 {
		return;
	}

	// Bounds check bits_per_key (minimum 1)
	let bits_per_key = (data.bits_per_key as usize).max(1).min(20);

	// Create filter
	let policy = Arc::new(LevelDBBloomFilter::new(bits_per_key));
	let mut writer = FilterBlockWriter::new(policy.clone());

	// Add all keys to filter
	writer.start_block(0);
	for key in &data.keys {
		writer.add_key(key);
	}

	let filter_block = writer.finish();
	let reader = FilterBlockReader::new(filter_block, policy.clone());

	// Test 1: All added keys should be found (no false negatives)
	for key in &data.keys {
		let may_contain = reader.may_contain(key, 0);
		assert!(
			may_contain,
			"Filter should contain all added keys (no false negatives). Key: {:?}",
			key
		);
	}

	// Test 2: Empty filter behavior - just ensure no panic
	if data.keys.is_empty() {
		let empty_filter = FilterBlockWriter::new(policy.clone()).finish();
		let empty_reader = FilterBlockReader::new(empty_filter, policy.clone());

		for query_key in &data.query_keys {
			let _ = empty_reader.may_contain(query_key, 0);
		}
	}

	// Test 3: Multiple blocks - need enough distinct keys
	// Filter out duplicates and require > 10 keys
	let unique_keys: Vec<_> = data
		.keys
		.iter()
		.filter(|k| !k.is_empty()) // Skip empty keys
		.collect::<std::collections::HashSet<_>>()
		.into_iter()
		.collect();

	if unique_keys.len() > 10 {
		let mut multi_writer = FilterBlockWriter::new(policy.clone());

		// Split keys into multiple blocks, tracking which block each key is in
		let block_size = unique_keys.len() / 3;
		if block_size > 0 {
			let mut key_to_block_offset: Vec<usize> = Vec::new();
			let mut current_block_offset = 0;

			for (i, key) in unique_keys.iter().enumerate() {
				if i % block_size == 0 {
					current_block_offset = i * 2048;
					multi_writer.start_block(current_block_offset);
				}
				multi_writer.add_key(key);
				key_to_block_offset.push(current_block_offset);
			}

			let multi_filter = multi_writer.finish();
			let multi_reader = FilterBlockReader::new(multi_filter, policy.clone());

			// Verify keys are found using correct block offset
			for (i, key) in unique_keys.iter().enumerate() {
				let block_offset = key_to_block_offset[i];
				let may_contain = multi_reader.may_contain(key, block_offset);
				assert!(
					may_contain,
					"Key should be found in its block. Key: {:?}, Block offset: {}",
					key, block_offset
				);
			}
		}
	}
});
