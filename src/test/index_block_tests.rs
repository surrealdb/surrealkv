use std::sync::Arc;
use std::vec;

use test_log::test;

use crate::sstable::block::BlockHandle;
use crate::sstable::index_block::{BlockHandleWithKey, Index, IndexWriter};
use crate::vfs::File;
use crate::{
	CompressionType,
	InternalKey,
	InternalKeyKind,
	LSMIterator,
	Options,
	INTERNAL_KEY_SEQ_NUM_MAX,
	INTERNAL_KEY_TIMESTAMP_MAX,
};

fn wrap_buffer(src: Vec<u8>) -> Arc<dyn File> {
	Arc::new(src)
}

fn create_internal_key(user_key: Vec<u8>, sequence: u64) -> Vec<u8> {
	InternalKey::new(user_key, sequence, InternalKeyKind::Set, 0).encode()
}

#[test]
fn test_top_level_index_writer_basic() {
	let opts = Arc::new(Options::default());
	let max_block_size = 100;
	let mut writer = IndexWriter::new(opts, max_block_size);

	let key1 = create_internal_key(b"key1".to_vec(), 1);
	let handle1 = vec![1, 2, 3];
	writer.add(&key1, &handle1).unwrap();

	let mut d = Vec::new();
	let top_level_block = writer.finish(&mut d, CompressionType::None, 0).unwrap();
	assert!(!top_level_block.0.offset > 0);
}

#[test]
fn test_top_level_index_writer_multiple_blocks() {
	let opts = Arc::new(Options::default());
	let max_block_size = 50; // Small size to force multiple blocks
	let mut writer = IndexWriter::new(opts, max_block_size);

	for i in 0..10 {
		let key = create_internal_key(format!("key{i}").as_bytes().to_vec(), i as u64);
		let handle = vec![i as u8; 10]; // 10-byte handle
		writer.add(&key, &handle).unwrap();
	}

	// assert!(index_blocks.len() > 1, "Expected multiple index blocks");
	let mut d = Vec::new();
	let top_level_block = writer.finish(&mut d, CompressionType::None, 0).unwrap();
	assert!(!top_level_block.0.offset > 0);
}

// #[test]
// fn test_top_level_index_writer_empty() {
//     let opts = Arc::new(Options::default());
//     let max_block_size = 100;
//     let writer = IndexWriter::new(opts, max_block_size);

//     let top_level_block = writer.finish().unwrap();
//     assert_eq!(index_blocks.len(), 0);
//     assert!(!top_level_block.is_empty()); // Top-level block should still be
// created }

#[test]
fn test_top_level_index_writer_large_entries() {
	let opts = Arc::new(Options::default());
	let max_block_size = 1000;
	let mut writer = IndexWriter::new(opts, max_block_size);

	let large_key = create_internal_key(vec![b'a'; 500], 1);
	let large_handle = vec![b'b'; 500];
	writer.add(&large_key, &large_handle).unwrap();

	let mut d = Vec::new();
	let top_level_block = writer.finish(&mut d, CompressionType::None, 0).unwrap();
	assert!(!top_level_block.0.offset > 0);
}

#[test]
fn test_top_level_index_writer_exact_block_size() {
	let opts = Arc::new(Options::default());
	let max_block_size = 100;
	let mut writer = IndexWriter::new(opts, max_block_size);

	// Add entries that exactly fill up one block
	let key = create_internal_key(b"key".to_vec(), 1);
	let handle = vec![0; 90];
	writer.add(&key, &handle).unwrap();

	let mut d = Vec::new();
	let top_level_block = writer.finish(&mut d, CompressionType::None, 0).unwrap();
	assert!(!top_level_block.0.offset > 0);
}

// #[test]
// fn test_top_level_index() {
//     let opts = Arc::new(Options::default());
//     let max_block_size = 10;
//     let mut writer = IndexWriter::new(Arc::clone(&opts), max_block_size);

//     let key1 = create_internal_key(b"key1".to_vec(), 1);
//     let handle1 = vec![1, 2, 3];
//     writer.add(&key1, &handle1).unwrap();

//     let mut d = Vec::new();
//     let top_level_block = writer.finish(&mut d, CompressionType::None,
// 0).unwrap();     assert!(!top_level_block.0.offset > 0);

//     let f = wrap_buffer(d);
//     let top_level_index = Index::new(0, opts, f,
// &top_level_block.0).unwrap();     let block =
// top_level_index.get(&key1).unwrap();     // println!("block: {:?}",
// block.block); }

#[test]
fn test_find_block_handle_by_key() {
	let opts = Arc::new(Options::default());
	let d = Vec::new();
	let f = wrap_buffer(d);

	// Create separator keys as full encoded internal keys
	let sep_c = create_internal_key(b"c".to_vec(), 1);
	let sep_f = create_internal_key(b"f".to_vec(), 1);
	let sep_j = create_internal_key(b"j".to_vec(), 1);

	// Initialize Index with predefined blocks using encoded internal keys
	let index = Index {
		id: 0,
		opts,
		blocks: vec![
			BlockHandleWithKey::new(sep_c.clone(), BlockHandle::new(0, 10)),
			BlockHandleWithKey::new(sep_f.clone(), BlockHandle::new(10, 10)),
			BlockHandleWithKey::new(sep_j.clone(), BlockHandle::new(20, 10)),
		],
		file: Arc::clone(&f),
	};

	// A list of tuples where the first element is the encoded internal key to find,
	// and the second element is the expected separator key result.
	let test_cases: Vec<(Vec<u8>, Option<Vec<u8>>)> = vec![
		(create_internal_key(b"a".to_vec(), 1), Some(sep_c.clone())),
		(create_internal_key(b"c".to_vec(), 1), Some(sep_c)),
		(create_internal_key(b"d".to_vec(), 1), Some(sep_f.clone())),
		(create_internal_key(b"e".to_vec(), 1), Some(sep_f.clone())),
		(create_internal_key(b"f".to_vec(), 1), Some(sep_f)),
		(create_internal_key(b"g".to_vec(), 1), Some(sep_j.clone())),
		(create_internal_key(b"j".to_vec(), 1), Some(sep_j)),
		(create_internal_key(b"z".to_vec(), 1), None),
	];

	for (key, expected) in test_cases.iter() {
		let result = index.find_block_handle_by_key(key).unwrap();
		match expected {
			Some(expected_sep_key) => {
				let (_index, handle) = result.expect("Expected a block handle but got None");
				assert_eq!(&handle.separator_key, expected_sep_key, "Mismatch for key {key:?}");
			}
			None => assert!(result.is_none(), "Expected None for key {key:?}, but got Some"),
		}
	}
}

#[test]
fn test_find_block_handle_by_key_with_descending_seq_nums() {
	// Tests partition lookup with same user key spanning multiple partitions
	// using the correct descending sequence number ordering:
	// (foo, 100) < (foo, 50) < (foo, 1) in InternalKey ordering
	let opts = Arc::new(Options::default());
	let d = Vec::new();
	let f = wrap_buffer(d);

	// Simulate partitions where same user key "foo" spans multiple partitions
	// Partition 0: contains (foo, 100) to (foo, 60), separator = (foo, 60)
	// Partition 1: contains (foo, 59) to (foo, 20), separator = (foo, 20)
	// Partition 2: contains (foo, 19) to (foo, 1), separator = (g, MAX)
	let sep_foo_60 = create_internal_key(b"foo".to_vec(), 60);
	let sep_foo_20 = create_internal_key(b"foo".to_vec(), 20);
	let sep_g = InternalKey::new(
		b"g".to_vec(),
		INTERNAL_KEY_SEQ_NUM_MAX,
		InternalKeyKind::Separator,
		INTERNAL_KEY_TIMESTAMP_MAX,
	)
	.encode();

	let index = Index {
		id: 0,
		opts,
		blocks: vec![
			BlockHandleWithKey::new(sep_foo_60, BlockHandle::new(0, 100)),
			BlockHandleWithKey::new(sep_foo_20, BlockHandle::new(100, 100)),
			BlockHandleWithKey::new(sep_g, BlockHandle::new(200, 100)),
		],
		file: f,
	};

	// Test cases: (query_key, expected_partition_index)
	let test_cases = vec![
		// Query for (foo, 75): should find partition 0 (75 > 60 in seq, so (foo,75) <
		// (foo,60))
		(create_internal_key(b"foo".to_vec(), 75), Some(0)),
		// Query for (foo, 60): should find partition 0 (exact match with separator)
		(create_internal_key(b"foo".to_vec(), 60), Some(0)),
		// Query for (foo, 50): should find partition 1 (50 < 60, so (foo,50) > (foo,60))
		(create_internal_key(b"foo".to_vec(), 50), Some(1)),
		// Query for (foo, 20): should find partition 1 (exact match with separator)
		(create_internal_key(b"foo".to_vec(), 20), Some(1)),
		// Query for (foo, 10): should find partition 2 (10 < 20, so (foo,10) > (foo,20))
		(create_internal_key(b"foo".to_vec(), 10), Some(2)),
		// Query for (banana, 50): should find partition 0 ("banana" < "foo" lexicographically)
		(create_internal_key(b"banana".to_vec(), 50), Some(0)),
		// Query for (fz, 50): should find partition 2 ("foo" < "fz" < "g")
		(create_internal_key(b"fz".to_vec(), 50), Some(2)),
		// Query for (zebra, 1): should return None ("zebra" > "g")
		(create_internal_key(b"zebra".to_vec(), 1), None),
	];

	for (query_key, expected_index) in test_cases {
		let result = index.find_block_handle_by_key(&query_key).unwrap();
		let query_ikey = InternalKey::decode(&query_key);
		match expected_index {
			Some(idx) => {
				let (found_idx, _) = result.unwrap_or_else(|| {
					panic!(
						"Expected partition {} for key ({}, seq={}), got None",
						idx,
						String::from_utf8_lossy(&query_ikey.user_key),
						query_ikey.seq_num()
					)
				});
				assert_eq!(
					found_idx,
					idx,
					"Wrong partition for key ({}, seq={}): expected {}, got {}",
					String::from_utf8_lossy(&query_ikey.user_key),
					query_ikey.seq_num(),
					idx,
					found_idx
				);
			}
			None => {
				assert!(
					result.is_none(),
					"Expected None for key ({}, seq={}), got Some(partition {})",
					String::from_utf8_lossy(&query_ikey.user_key),
					query_ikey.seq_num(),
					result.map(|(i, _)| i).unwrap_or(999)
				);
			}
		}
	}
}
#[test]
fn test_find_block_handle_by_key_different_user_keys() {
	// Tests partition lookup with different user keys using shortened separators
	let opts = Arc::new(Options::default());
	let d = Vec::new();
	let f = wrap_buffer(d);

	// Partition 0: contains "apple" keys, separator = (b, MAX) [shortened from apple/banana
	// boundary] Partition 1: contains "banana", "cherry" keys, separator = (d, MAX)
	// Partition 2: contains "date" keys, separator = (e, MAX)
	let sep_b = InternalKey::new(
		b"b".to_vec(),
		INTERNAL_KEY_SEQ_NUM_MAX,
		InternalKeyKind::Separator,
		INTERNAL_KEY_TIMESTAMP_MAX,
	)
	.encode();
	let sep_d = InternalKey::new(
		b"d".to_vec(),
		INTERNAL_KEY_SEQ_NUM_MAX,
		InternalKeyKind::Separator,
		INTERNAL_KEY_TIMESTAMP_MAX,
	)
	.encode();
	let sep_e = InternalKey::new(
		b"e".to_vec(),
		INTERNAL_KEY_SEQ_NUM_MAX,
		InternalKeyKind::Separator,
		INTERNAL_KEY_TIMESTAMP_MAX,
	)
	.encode();

	let index = Index {
		id: 0,
		opts,
		blocks: vec![
			BlockHandleWithKey::new(sep_b, BlockHandle::new(0, 100)),
			BlockHandleWithKey::new(sep_d, BlockHandle::new(100, 100)),
			BlockHandleWithKey::new(sep_e, BlockHandle::new(200, 100)),
		],
		file: f,
	};

	let test_cases = vec![
		// Keys in first partition (< "b")
		(create_internal_key(b"apple".to_vec(), 100), Some(0)),
		(create_internal_key(b"aardvark".to_vec(), 50), Some(0)),
		// Key exactly at separator boundary
		(
			InternalKey::new(
				b"b".to_vec(),
				INTERNAL_KEY_SEQ_NUM_MAX,
				InternalKeyKind::Separator,
				INTERNAL_KEY_TIMESTAMP_MAX,
			)
			.encode(),
			Some(0),
		),
		// Keys in second partition ("b" < key <= "d")
		(create_internal_key(b"banana".to_vec(), 100), Some(1)),
		(create_internal_key(b"cherry".to_vec(), 50), Some(1)),
		// Keys in third partition ("d" < key <= "e")
		(create_internal_key(b"date".to_vec(), 100), Some(2)),
		// Keys beyond all partitions (> "e")
		(create_internal_key(b"fig".to_vec(), 100), None),
		(create_internal_key(b"zebra".to_vec(), 1), None),
	];

	for (query_key, expected_index) in test_cases {
		let result = index.find_block_handle_by_key(&query_key).unwrap();
		let query_ikey = InternalKey::decode(&query_key);
		match expected_index {
			Some(idx) => {
				let (found_idx, _) = result.unwrap_or_else(|| {
					panic!(
						"Expected partition {} for key '{}', got None",
						idx,
						String::from_utf8_lossy(&query_ikey.user_key)
					)
				});
				assert_eq!(
					found_idx,
					idx,
					"Wrong partition for key '{}': expected {}, got {}",
					String::from_utf8_lossy(&query_ikey.user_key),
					idx,
					found_idx
				);
			}
			None => {
				assert!(
					result.is_none(),
					"Expected None for key '{}', got Some(partition {})",
					String::from_utf8_lossy(&query_ikey.user_key),
					result.map(|(i, _)| i).unwrap_or(999)
				);
			}
		}
	}
}

#[test]
fn test_find_block_handle_returns_correct_partition_index() {
	// Verifies the optimization that returns (index, handle) tuple
	let opts = Arc::new(Options::default());
	let d = Vec::new();
	let f = wrap_buffer(d);

	let sep_keys: Vec<_> =
		(0..5).map(|i| create_internal_key(format!("key_{:02}", i).into_bytes(), 100)).collect();

	let index = Index {
		id: 0,
		opts,
		blocks: sep_keys
			.iter()
			.enumerate()
			.map(|(i, sep)| BlockHandleWithKey::new(sep.clone(), BlockHandle::new(i * 100, 100)))
			.collect(),
		file: f,
	};

	// Query for each separator key and verify correct index is returned
	for (expected_idx, sep_key) in sep_keys.iter().enumerate() {
		let result = index.find_block_handle_by_key(sep_key).unwrap();
		assert!(result.is_some(), "Should find block for separator key");
		let (idx, handle) = result.unwrap();
		assert_eq!(idx, expected_idx, "Returned index should match partition index");
		assert_eq!(
			handle.handle.offset, // <-- Remove parentheses, it's a field not a method
			expected_idx * 100,
			"Handle offset should match"
		);
	}
}

#[test]
fn test_partition_lookup_empty_partition_returns_none() {
	// Edge case: Query beyond all partitions
	let opts = Arc::new(Options::default());
	let d = Vec::new();
	let f = wrap_buffer(d);

	let sep = create_internal_key(b"zzz".to_vec(), 1);

	let index = Index {
		id: 0,
		opts,
		blocks: vec![BlockHandleWithKey::new(sep, BlockHandle::new(0, 100))],
		file: f,
	};

	// Query for key beyond the single partition
	let query = create_internal_key(b"zzzz_beyond".to_vec(), 1);
	let result = index.find_block_handle_by_key(&query).unwrap();
	assert!(result.is_none(), "Should return None for key beyond all partitions");
}

#[test]
fn test_partition_lookup_single_partition() {
	// Edge case: Only one partition
	let opts = Arc::new(Options::default());
	let d = Vec::new();
	let f = wrap_buffer(d);

	let sep = create_internal_key(b"middle".to_vec(), 1);

	let index = Index {
		id: 0,
		opts,
		blocks: vec![BlockHandleWithKey::new(sep.clone(), BlockHandle::new(0, 100))],
		file: f,
	};

	// Query before the separator
	let query = create_internal_key(b"aaa".to_vec(), 1);
	let result = index.find_block_handle_by_key(&query).unwrap();
	assert!(result.is_some());
	let (idx, _) = result.unwrap();
	assert_eq!(idx, 0);

	// Query at the separator
	let result = index.find_block_handle_by_key(&sep).unwrap();
	assert!(result.is_some());
	let (idx, _) = result.unwrap();
	assert_eq!(idx, 0);

	// Query after the separator
	let query = create_internal_key(b"zzz".to_vec(), 1);
	let result = index.find_block_handle_by_key(&query).unwrap();
	assert!(result.is_none());
}

#[test]
fn test_partition_lookup_exact_separator_match() {
	// Edge case: Query key exactly matches a separator
	let opts = Arc::new(Options::default());
	let d = Vec::new();
	let f = wrap_buffer(d);

	let sep_a = create_internal_key(b"aaa".to_vec(), 50);
	let sep_b = create_internal_key(b"bbb".to_vec(), 50);
	let sep_c = create_internal_key(b"ccc".to_vec(), 50);

	let index = Index {
		id: 0,
		opts,
		blocks: vec![
			BlockHandleWithKey::new(sep_a, BlockHandle::new(0, 100)),
			BlockHandleWithKey::new(sep_b.clone(), BlockHandle::new(100, 100)),
			BlockHandleWithKey::new(sep_c, BlockHandle::new(200, 100)),
		],
		file: f,
	};

	// Query exactly "bbb" at same seq
	let result = index.find_block_handle_by_key(&sep_b).unwrap();
	assert!(result.is_some());
	let (idx, _) = result.unwrap();
	assert_eq!(idx, 1, "Exact match should return that partition");
}

#[test]
fn test_partitioned_index_seek_correctness() {
	// Test Seek correctness with multiple partitions
	let opts = Options {
		index_partition_size: 100, // Small partition size to force multiple partitions
		block_size: 1700,          // Larger block size to create multiple data blocks
		..Default::default()
	};
	let opts = Arc::new(opts);

	let mut buffer = Vec::new();
	let mut writer = crate::sstable::table::TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Create keys with prefix length 3, make sure the key/value is big enough to fill one block
	// Pattern: "0015", "0035", "0054", "0055", "0056", "0057", "0058", "0075", "0076", "0095"
	let test_keys =
		vec!["0015", "0035", "0054", "0055", "0056", "0057", "0058", "0075", "0076", "0095"];

	for (seq, key) in test_keys.iter().enumerate() {
		let internal_key =
			InternalKey::new(key.as_bytes().to_vec(), (seq + 1) as u64, InternalKeyKind::Set, 0);
		let value = format!("v-{key}").into_bytes();
		writer.add(internal_key, &value).unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Arc::new(
		crate::sstable::table::Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap(),
	);

	// Verify we have partitioned index
	let crate::sstable::table::IndexType::Partitioned(_) = &table.index_block;

	// Test Seek to existing keys
	for (seq, key) in test_keys.iter().enumerate() {
		let seek_key =
			InternalKey::new(key.as_bytes().to_vec(), (seq + 1) as u64, InternalKeyKind::Set, 0);
		let result = table.get(&seek_key).unwrap();
		assert!(result.is_some(), "Should find key {key}");
		if let Some((found_key, found_value)) = result {
			assert_eq!(
				std::str::from_utf8(&found_key.user_key).unwrap(),
				*key,
				"Key mismatch for {key}"
			);
			assert_eq!(
				std::str::from_utf8(found_value.as_ref()).unwrap(),
				format!("v-{key}"),
				"Value mismatch for {key}"
			);
		}
	}

	// Test Seek to non-existing keys (between blocks)
	let non_existing_keys = vec!["0016", "0036", "0053", "0059", "0074", "0077", "0094"];
	for key in &non_existing_keys {
		let seek_key = InternalKey::new(key.as_bytes().to_vec(), 100, InternalKeyKind::Set, 0);
		let result = table.get(&seek_key).unwrap();
		// Should either find the next key or return None
		if let Some((found_key, _)) = result {
			// If found, verify it's a valid key
			let found_str = std::str::from_utf8(&found_key.user_key).unwrap();
			assert!(test_keys.contains(&found_str), "Found key {found_str} should be in test_keys");
		}
	}

	let mut iter = table.iter(None).unwrap();
	iter.seek_to_last().unwrap();
	assert!(iter.valid(), "Iterator should be valid after seek_to_last");

	let mut backward_keys = Vec::new();
	backward_keys.push(std::str::from_utf8(iter.key().user_key()).unwrap().to_string());
	while iter.prev().unwrap() {
		backward_keys.push(std::str::from_utf8(iter.key().user_key()).unwrap().to_string());
	}

	// Verify backward iteration matches reverse of forward iteration
	let mut forward_keys: Vec<String> = test_keys.iter().map(|s| (*s).to_string()).collect();
	forward_keys.reverse();
	assert_eq!(
		backward_keys, forward_keys,
		"Backward iteration should match reverse forward iteration"
	);
}

#[test]
fn test_partitioned_index_boundary_keys() {
	// Test keys at exact partition boundaries
	let opts = Options {
		index_partition_size: 50, // Small partition size to force multiple partitions
		block_size: 500,
		..Default::default()
	};

	let opts = Arc::new(opts);

	let mut buffer = Vec::new();
	let mut writer = crate::sstable::table::TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Create keys that will span multiple partitions
	// Add enough keys to create at least 3 partitions
	for i in 0..50 {
		let key = format!("key_{i:03}");
		let internal_key =
			InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);
		let value = format!("value_{i:03}").into_bytes();
		writer.add(internal_key, &value).unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Arc::new(
		crate::sstable::table::Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap(),
	);

	let crate::sstable::table::IndexType::Partitioned(ref partitioned_index) = table.index_block;

	assert!(partitioned_index.blocks.len() >= 2, "Should have at least 2 partitions");

	// Test first key in first partition
	let first_key = InternalKey::new(b"key_000".to_vec(), 1, InternalKeyKind::Set, 0);
	let result = table.get(&first_key).unwrap();
	assert!(result.is_some(), "Should find first key");

	// Test last key in last partition
	let last_key = InternalKey::new(b"key_049".to_vec(), 50, InternalKeyKind::Set, 0);
	let result = table.get(&last_key).unwrap();
	assert!(result.is_some(), "Should find last key");

	// Test keys just before/after partition boundaries
	// Get separator keys to understand boundaries
	for (idx, block) in partitioned_index.blocks.iter().enumerate() {
		let sep_key = InternalKey::decode(&block.separator_key);
		let sep_user_key = std::str::from_utf8(&sep_key.user_key).unwrap();

		// Test key just before separator
		if idx > 0 {
			// Try to find a key that should be in previous partition
			let test_key = InternalKey::new(
				sep_user_key.as_bytes().to_vec(),
				sep_key.seq_num() + 1, // Higher seq = earlier in ordering
				InternalKeyKind::Set,
				0,
			);
			let result = table.get(&test_key).unwrap();
			// Should find something (might be in previous partition)
			if result.is_some() {
				let (found_key, _) = result.unwrap();
				assert!(found_key.user_key <= sep_key.user_key, "Found key should be <= separator");
			}
		}
	}
}

#[test]
fn test_partitioned_index_reseek() {
	let opts = Options {
		index_partition_size: 100,
		block_size: 1700,
		..Default::default()
	};

	let opts = Arc::new(opts);

	let mut buffer = Vec::new();
	let mut writer = crate::sstable::table::TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let test_keys =
		vec!["0015", "0035", "0054", "0055", "0056", "0057", "0058", "0075", "0076", "0095"];

	for (seq, key) in test_keys.iter().enumerate() {
		let internal_key =
			InternalKey::new(key.as_bytes().to_vec(), (seq + 1) as u64, InternalKeyKind::Set, 0);
		let value = format!("v-{key}").into_bytes();
		writer.add(internal_key, &value).unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Arc::new(
		crate::sstable::table::Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap(),
	);

	let mut iter = table.iter(None).unwrap();

	// Seek to middle key
	let seek_key = InternalKey::new(b"0055".to_vec(), 4, InternalKeyKind::Set, 0);
	iter.seek(&seek_key.encode()).unwrap();
	assert!(iter.valid(), "Iterator should be valid after seek");
	assert_eq!(std::str::from_utf8(iter.key().user_key()).unwrap(), "0055", "Should find key 0055");

	// SeekToLast
	iter.seek_to_last().unwrap();
	assert!(iter.valid(), "Iterator should be valid after seek_to_last");
	assert_eq!(std::str::from_utf8(iter.key().user_key()).unwrap(), "0095", "Should find last key");

	// Reseek to same key - should work correctly
	iter.seek(&seek_key.encode()).unwrap();
	assert!(iter.valid(), "Iterator should be valid after reseek");
	assert_eq!(
		std::str::from_utf8(iter.key().user_key()).unwrap(),
		"0055",
		"Should find key 0055 after reseek"
	);

	// SeekToLast again
	iter.seek_to_last().unwrap();
	assert!(iter.valid(), "Iterator should be valid after second seek_to_last");
	assert_eq!(
		std::str::from_utf8(iter.key().user_key()).unwrap(),
		"0095",
		"Should find last key again"
	);

	// Prev twice
	iter.prev().unwrap();
	assert!(iter.valid());
	iter.prev().unwrap();
	assert!(iter.valid());
	assert_eq!(
		std::str::from_utf8(iter.key().user_key()).unwrap(),
		"0075",
		"Should find key 0075 after two prev()"
	);

	// Seek to 0095
	let seek_key_0095 = InternalKey::new(b"0095".to_vec(), 10, InternalKeyKind::Set, 0);
	iter.seek(&seek_key_0095.encode()).unwrap();
	assert!(iter.valid());
	assert_eq!(std::str::from_utf8(iter.key().user_key()).unwrap(), "0095", "Should find key 0095");

	// Prev twice
	iter.prev().unwrap();
	assert!(iter.valid());
	iter.prev().unwrap();
	assert!(iter.valid());
	assert_eq!(
		std::str::from_utf8(iter.key().user_key()).unwrap(),
		"0075",
		"Should find key 0075 after prev from 0095"
	);

	// SeekToLast
	iter.seek_to_last().unwrap();
	assert!(iter.valid());
	assert_eq!(std::str::from_utf8(iter.key().user_key()).unwrap(), "0095", "Should find last key");

	// Seek to 0095 again
	iter.seek(&seek_key_0095.encode()).unwrap();
	assert!(iter.valid());
	assert_eq!(
		std::str::from_utf8(iter.key().user_key()).unwrap(),
		"0095",
		"Should find key 0095 after reseek"
	);

	// Prev twice
	iter.prev().unwrap();
	assert!(iter.valid());
	iter.prev().unwrap();
	assert!(iter.valid());
	assert_eq!(std::str::from_utf8(iter.key().user_key()).unwrap(), "0075", "Should find key 0075");

	// Seek to 0075
	let seek_key_0075 = InternalKey::new(b"0075".to_vec(), 8, InternalKeyKind::Set, 0);
	iter.seek(&seek_key_0075.encode()).unwrap();
	assert!(iter.valid());
	assert_eq!(std::str::from_utf8(iter.key().user_key()).unwrap(), "0075", "Should find key 0075");

	// Next twice
	iter.next().unwrap();
	assert!(iter.valid());
	iter.next().unwrap();
	assert!(iter.valid());
	assert_eq!(
		std::str::from_utf8(iter.key().user_key()).unwrap(),
		"0095",
		"Should find key 0095 after two next()"
	);

	// SeekToLast
	iter.seek_to_last().unwrap();
	assert!(iter.valid());
	assert_eq!(std::str::from_utf8(iter.key().user_key()).unwrap(), "0095", "Should find last key");
}

#[test]
fn test_partitioned_index_varying_partition_sizes() {
	// Test with varying partition sizes to ensure all operations work
	let max_index_keys = 5;
	let est_max_index_key_value_size = 32;
	let est_max_index_size = max_index_keys * est_max_index_key_value_size;

	for partition_size in 1..=est_max_index_size + 1 {
		let opts = Options {
			index_partition_size: partition_size,
			block_size: 500,
			..Default::default()
		};

		let opts = Arc::new(opts);

		let mut buffer = Vec::new();
		let mut writer =
			crate::sstable::table::TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

		// Add enough entries to create multiple partitions
		for i in 0..20 {
			let key = format!("key_{i:03}");
			let internal_key =
				InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);
			let value = format!("value_{i:03}").into_bytes();
			writer.add(internal_key, &value).unwrap();
		}

		let size = writer.finish().unwrap();
		let table = Arc::new(
			crate::sstable::table::Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap(),
		);

		// Test Get operations work at this partition size
		for i in 0..20 {
			let key = format!("key_{i:03}");
			let seek_key =
				InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);
			let result = table.get(&seek_key).unwrap();
			assert!(result.is_some(), "Should find key {key} at partition_size {partition_size}");
			if let Some((found_key, found_value)) = result {
				assert_eq!(
					std::str::from_utf8(&found_key.user_key).unwrap(),
					key,
					"Key mismatch at partition_size {partition_size}"
				);
				assert_eq!(
					std::str::from_utf8(found_value.as_ref()).unwrap(),
					format!("value_{i:03}"),
					"Value mismatch at partition_size {partition_size}"
				);
			}
		}

		// Test iterator works at this partition size
		let mut iter = table.iter(None).unwrap();
		iter.seek_to_first().unwrap();
		let mut count = 0;

		while iter.valid() {
			count += 1;
			iter.next().unwrap();
		}
		assert_eq!(count, 20, "Should iterate all 20 keys at partition_size {partition_size}");
	}
}
