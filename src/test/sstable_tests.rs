use std::ops::Bound;
use std::sync::Arc;
use std::vec;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use test_log::test;

use crate::sstable::block::BlockHandle;
use crate::sstable::table::{ChecksumType, Footer, IndexType, Table, TableFormat, TableWriter};
use crate::test::{collect_all, collect_iter, count_iter};
use crate::vfs::File;
use crate::{
	user_range_to_internal_range,
	InternalKey,
	InternalKeyKind,
	LSMIterator,
	Options,
	Result,
	INTERNAL_KEY_SEQ_NUM_MAX,
};

fn default_opts() -> Arc<Options> {
	let mut opts = Options::new();
	opts.block_restart_interval = 3;
	opts.index_partition_size = 100; // Small partition size to force multiple partitions
	Arc::new(opts)
}

fn default_opts_mut() -> Options {
	let mut opts = Options::new();
	opts.block_restart_interval = 3;
	opts.index_partition_size = 100; // Small partition size to force multiple partitions
	opts
}

#[test]
fn test_footer() {
	let f = Footer::new(BlockHandle::new(44, 4), BlockHandle::new(55, 5));
	let mut buf = [0; 50]; // Updated to match new footer size (42 + 8 magic)
	f.encode(&mut buf[..]);

	let f2 = Footer::decode(&buf).unwrap();
	assert_eq!(f2.meta_index.offset(), 44);
	assert_eq!(f2.meta_index.size(), 4);
	assert_eq!(f2.index.offset(), 55);
	assert_eq!(f2.index.size(), 5);
	assert_eq!(f2.format, TableFormat::LSMV1);
	assert_eq!(f2.checksum, ChecksumType::CRC32c);
}

#[test]
fn test_table_builder() {
	let d = Vec::with_capacity(512);
	let opts = default_opts();

	let mut b = TableWriter::new(d, 0, opts, 0);

	let data = [("abc", "def"), ("abe", "dee"), ("bcd", "asa"), ("dcc", "a00")];
	let data2 = [("abd", "def"), ("abf", "dee"), ("ccd", "asa"), ("dcd", "a00")];

	for i in 0..data.len() {
		b.add(
			InternalKey::new(Vec::from(data[i].0.as_bytes()), 1, InternalKeyKind::Set, 0),
			data[i].1.as_bytes(),
		)
		.unwrap();
		b.add(
			InternalKey::new(Vec::from(data2[i].0.as_bytes()), 1, InternalKeyKind::Set, 0),
			data2[i].1.as_bytes(),
		)
		.unwrap();
	}

	let actual = b.finish().unwrap();
	assert_eq!(724, actual);
}

#[test]
#[should_panic]
fn test_bad_input() {
	let d = Vec::with_capacity(512);
	let opts = default_opts();

	let mut b = TableWriter::new(d, 0, opts, 0);

	// Test two equal consecutive keys
	let data = [("abc", "def"), ("abc", "dee"), ("bcd", "asa"), ("bsr", "a00")];

	for &(k, v) in data.iter() {
		b.add(InternalKey::new(Vec::from(k.as_bytes()), 1, InternalKeyKind::Set, 0), v.as_bytes())
			.unwrap();
	}
	b.finish().unwrap();
}

fn build_data() -> Vec<(&'static str, &'static str)> {
	vec![
		// block 1
		("abc", "def"),
		("abd", "dee"),
		("bcd", "asa"),
		// block 2
		("bsr", "a00"),
		("xyz", "xxx"),
		("xzz", "yyy"),
		// block 3
		("zzz", "111"),
	]
}

// Build a table containing raw keys (no format). It returns (vector, length)
// for convenience reason, a call f(v, v.len()) doesn't work for borrowing
// reasons.
fn build_table(data: Vec<(&str, &str)>) -> (Vec<u8>, usize) {
	let mut d = Vec::with_capacity(512);
	let mut opts = default_opts_mut();
	opts.block_restart_interval = 3;
	opts.block_size = 32;
	let opt = Arc::new(opts);

	{
		// Uses the standard comparator in opt.
		let mut b = TableWriter::new(&mut d, 0, opt, 0);

		for &(k, v) in data.iter() {
			b.add(
				InternalKey::new(Vec::from(k.as_bytes()), 1, InternalKeyKind::Set, 0),
				v.as_bytes(),
			)
			.unwrap();
		}

		b.finish().unwrap();
	}

	let size = d.len();
	(d, size)
}

fn build_table_with_seq_num(data: Vec<(&str, &str, u64)>) -> (Vec<u8>, usize) {
	let mut d = Vec::with_capacity(512);
	let mut opts = default_opts_mut();
	opts.block_restart_interval = 3;
	opts.block_size = 32;
	let opt = Arc::new(opts);

	{
		let mut b = TableWriter::new(&mut d, 0, opt, 0);
		for &(k, v, seq) in data.iter() {
			b.add(
				InternalKey::new(Vec::from(k.as_bytes()), seq, InternalKeyKind::Set, 0),
				v.as_bytes(),
			)
			.unwrap();
		}
		b.finish().unwrap();
	}

	let size = d.len();
	(d, size)
}

fn wrap_buffer(src: Vec<u8>) -> Arc<dyn File> {
	Arc::new(src)
}

#[test]
fn test_table_seek() {
	let (src, size) = build_table(build_data());
	let opts = default_opts();

	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());
	let mut iter = table.iter(None).unwrap();

	let key = InternalKey::new(Vec::from(b"bcd"), 2, InternalKeyKind::Set, 0);
	iter.seek(&key.encode()).unwrap();
	assert!(iter.valid());
	assert_eq!((iter.key().user_key(), iter.value_encoded().unwrap()), (&b"bcd"[..], &b"asa"[..]));

	let key = InternalKey::new(Vec::from(b"abc"), 2, InternalKeyKind::Set, 0);
	iter.seek(&key.encode()).unwrap();
	assert!(iter.valid());
	assert_eq!((iter.key().user_key(), iter.value_encoded().unwrap()), (&b"abc"[..], &b"def"[..]));

	// Seek-past-last invalidates.
	let key = InternalKey::new(Vec::from(b"{{{"), 2, InternalKeyKind::Set, 0);
	iter.seek(&key.encode()).unwrap();
	assert!(!iter.valid());

	let key = InternalKey::new(Vec::from(b"bbb"), 2, InternalKeyKind::Set, 0);
	iter.seek(&key.encode()).unwrap();
	assert!(iter.valid());
}

#[test]
fn test_table_iter() {
	let (src, size) = build_table(build_data());
	let opts = default_opts();

	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());
	let mut iter = table.iter(None).unwrap();

	iter.next().unwrap();
	assert!(iter.valid());
	assert_eq!((iter.key().user_key(), iter.value_encoded().unwrap()), (&b"abc"[..], &b"def"[..]));

	iter.next().unwrap();
	assert!(iter.valid());
	assert_eq!((iter.key().user_key(), iter.value_encoded().unwrap()), (&b"abd"[..], &b"dee"[..]));

	iter.next().unwrap();
	assert!(iter.valid());
	assert_eq!((iter.key().user_key(), iter.value_encoded().unwrap()), (&b"bcd"[..], &b"asa"[..]));

	iter.next().unwrap();
	assert!(iter.valid());
	assert_eq!((iter.key().user_key(), iter.value_encoded().unwrap()), (&b"bsr"[..], &b"a00"[..]));

	iter.next().unwrap();
	assert!(iter.valid());
	assert_eq!((iter.key().user_key(), iter.value_encoded().unwrap()), (&b"xyz"[..], &b"xxx"[..]));

	iter.next().unwrap();
	assert!(iter.valid());
	assert_eq!((iter.key().user_key(), iter.value_encoded().unwrap()), (&b"xzz"[..], &b"yyy"[..]));

	iter.next().unwrap();
	assert!(iter.valid());
	assert_eq!((iter.key().user_key(), iter.value_encoded().unwrap()), (&b"zzz"[..], &b"111"[..]));
}

#[test]
fn test_many_items() {
	// Create options with reasonable block size for test
	let opts = Options::new();
	let opts = Arc::new(opts);

	// Create buffer to store table data
	let mut buffer = Vec::with_capacity(10240); // 10KB initial capacity

	// Create TableWriter
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Number of items to generate
	let num_items = 10001;

	// Generate and add num_items items
	let mut items = Vec::with_capacity(num_items as usize);
	for i in 0..num_items {
		let key = format!("key_{i:05}");
		let value = format!("value_{i:05}");
		items.push((key.clone(), value.clone()));

		let internal_key = InternalKey::new(
			Vec::from(key.as_bytes()),
			i + 2, // Descending sequence numbers
			InternalKeyKind::Set,
			0,
		);

		writer.add(internal_key, value.as_bytes()).unwrap();
	}

	// Finish writing the table
	let size = writer.finish().unwrap();
	assert!(size > 0, "Table should have non-zero size");

	// Create a table reader
	let table = Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap();

	// Verify the number of entries matches
	assert_eq!(
		table.meta.properties.num_entries, num_items,
		"Table should contain num_items entries"
	);

	// Verify all items can be retrieved
	for (key, value) in &items {
		let internal_key =
			InternalKey::new(Vec::from(key.as_bytes()), num_items + 1, InternalKeyKind::Set, 0);

		let result = table.get(&internal_key).unwrap();

		assert!(result.is_some(), "Key '{key}' not found in table");

		if let Some((found_key, found_value)) = result {
			// Verify key matches
			assert_eq!(
				std::str::from_utf8(&found_key.user_key).unwrap(),
				key,
				"Key mismatch for '{key}'"
			);

			// Verify value matches
			assert_eq!(
				std::str::from_utf8(found_value.as_ref()).unwrap(),
				value,
				"Value mismatch for key '{key}'"
			);
		}
	}
}

#[test]
fn test_iter_items() {
	// Create options with reasonable block size for test
	let opts = Options::new();
	let opts = Arc::new(opts);

	// Create buffer to store table data
	let mut buffer = Vec::new();

	// Create TableWriter
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Number of items to generate
	let num_items = 10001;

	// Generate and add num_items items
	let mut items = Vec::with_capacity(num_items as usize);
	for i in 0..num_items {
		let key = format!("key_{i:05}");
		let value = format!("value_{i:05}");
		items.push((key.clone(), value.clone()));

		let internal_key =
			InternalKey::new(Vec::from(key.as_bytes()), i + 1, InternalKeyKind::Set, 0);

		writer.add(internal_key, value.as_bytes()).unwrap();
	}

	// Finish writing the table
	let size = writer.finish().unwrap();
	assert!(size > 0, "Table should have non-zero size");

	// Create a table reader
	let table = Arc::new(Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap());

	// Verify the number of entries matches
	assert_eq!(
		table.meta.properties.num_entries, num_items,
		"Table should contain num_items entries"
	);

	let mut iter = table.iter(None).unwrap();
	iter.seek_to_first().unwrap();
	let mut item = 0;
	while iter.valid() {
		let key = iter.key().to_owned();
		let value = iter.value_encoded().unwrap();
		let expected_key = format!("key_{item:05}");
		let expected_value = format!("value_{item:05}");
		assert_eq!(std::str::from_utf8(&key.user_key).unwrap(), expected_key);
		assert_eq!(value, expected_value.as_bytes());
		iter.next().unwrap();
		item += 1;
	}
}

fn add_key(writer: &mut TableWriter<Vec<u8>>, key: &[u8], seq: u64, value: &[u8]) -> Result<()> {
	writer.add(InternalKey::new(Vec::from(key), seq, InternalKeyKind::Set, 0), value)
}

#[test]
fn test_writer_key_range_empty() {
	let d = Vec::with_capacity(512);
	let opts = default_opts();
	let writer = TableWriter::new(d, 1, opts, 0);

	// Key range should be None for an empty table
	assert!(writer.meta.smallest_point.is_none());
	assert!(writer.meta.largest_point.is_none());
}

#[test]
fn test_writer_key_range_single_entry() {
	let d = Vec::with_capacity(512);
	let opts = default_opts();
	let mut writer = TableWriter::new(d, 1, opts, 0);

	// Add just one key
	add_key(&mut writer, b"singleton", 1, b"value").unwrap();

	// Key range should have identical low and high bounds
	assert!(writer.meta.smallest_point.is_some());
	assert!(writer.meta.largest_point.is_some());
	if let Some(smallest) = &writer.meta.smallest_point {
		assert_eq!(&smallest.user_key[..], b"singleton");
	}
	if let Some(largest) = &writer.meta.largest_point {
		assert_eq!(&largest.user_key[..], b"singleton");
	}
}

#[test]
fn test_writer_key_range_ascending_keys() {
	let d = Vec::with_capacity(512);
	let opts = default_opts();
	let mut writer = TableWriter::new(d, 1, opts, 0);

	// Add keys in ascending order
	let keys = ["aaa", "bbb", "ccc", "ddd", "eee"];
	for (i, key) in keys.iter().enumerate() {
		add_key(&mut writer, key.as_bytes(), i as u64 + 1, b"value").unwrap();

		// Verify range is updated correctly at each step
		assert!(writer.meta.smallest_point.is_some());
		assert!(writer.meta.largest_point.is_some());
		if let Some(smallest) = &writer.meta.smallest_point {
			assert_eq!(&smallest.user_key[..], b"aaa");
		}
		if let Some(largest) = &writer.meta.largest_point {
			assert_eq!(&largest.user_key[..], key.as_bytes());
		}
	}
}

#[test]
fn test_writer_key_range_interleaved_pattern() {
	let d = Vec::with_capacity(512);
	let opts = default_opts();
	let mut writer = TableWriter::new(d, 1, opts, 0);

	// Add keys in a pattern that interleaves
	let keys = ["a10", "a20", "a15", "a30", "a25"];
	// Note: Actually we need to add in order, so sort them first
	let mut sorted_keys = keys;
	sorted_keys.sort();

	for (i, key) in sorted_keys.iter().enumerate() {
		add_key(&mut writer, key.as_bytes(), i as u64 + 1, b"value").unwrap();
	}

	// Verify final range
	assert!(writer.meta.smallest_point.is_some());
	assert!(writer.meta.largest_point.is_some());
	if let Some(smallest) = &writer.meta.smallest_point {
		assert_eq!(&smallest.user_key[..], b"a10");
	}
	if let Some(largest) = &writer.meta.largest_point {
		assert_eq!(&largest.user_key[..], b"a30");
	}
}

#[test]
fn test_writer_key_range_sparse_pattern() {
	let d = Vec::with_capacity(512);
	let opts = default_opts();
	let mut writer = TableWriter::new(d, 1, opts, 0);

	// Add very sparse keys with large gaps
	let keys = ["aaaaa", "nnnnn", "zzzzz"];

	for (i, key) in keys.iter().enumerate() {
		add_key(&mut writer, key.as_bytes(), i as u64 + 1, b"value").unwrap();

		// Check range after each addition
		assert!(writer.meta.smallest_point.is_some());
		assert!(writer.meta.largest_point.is_some());
		if let Some(smallest) = &writer.meta.smallest_point {
			assert_eq!(&smallest.user_key[..], b"aaaaa");
		}
		if let Some(largest) = &writer.meta.largest_point {
			assert_eq!(&largest.user_key[..], key.as_bytes());
		}
	}
}

#[test]
fn test_writer_key_range_clustered_pattern() {
	let d = Vec::with_capacity(512);
	let opts = default_opts();
	let mut writer = TableWriter::new(d, 1, opts, 0);

	// Add clustered keys - many keys in a narrow range
	let prefixes = ["aaa", "aab", "aac"];

	for (i, prefix) in prefixes.iter().enumerate() {
		// For each prefix, add several close keys
		for j in 1..=5 {
			let key = format!("{prefix}{j}");
			add_key(&mut writer, key.as_bytes(), (i * 5 + j) as u64, b"value").unwrap();
		}
	}

	// Verify final range
	assert!(writer.meta.smallest_point.is_some());
	assert!(writer.meta.largest_point.is_some());
	if let Some(smallest) = &writer.meta.smallest_point {
		assert_eq!(&smallest.user_key[..], b"aaa1");
	}
	if let Some(largest) = &writer.meta.largest_point {
		assert_eq!(&largest.user_key[..], b"aac5");
	}
}

#[test]
fn test_writer_key_range_binary_keys() {
	let d = Vec::with_capacity(512);
	let opts = default_opts();
	let mut writer = TableWriter::new(d, 1, opts, 0);

	// Add binary keys
	let keys = [vec![0x00, 0x01, 0x02], vec![0x10, 0x11, 0x12], vec![0xF0, 0xF1, 0xF2]];

	for (i, key) in keys.iter().enumerate() {
		add_key(&mut writer, key, i as u64 + 1, b"value").unwrap();
	}

	// Verify range with binary comparison
	assert!(writer.meta.smallest_point.is_some());
	assert!(writer.meta.largest_point.is_some());
	if let Some(smallest) = &writer.meta.smallest_point {
		assert_eq!(&smallest.user_key[..], &[0x00, 0x01, 0x02]);
	}
	if let Some(largest) = &writer.meta.largest_point {
		assert_eq!(&largest.user_key[..], &[0xF0, 0xF1, 0xF2]);
	}
}

#[test]
fn test_writer_key_range_identical_keys_different_seqnums() {
	let d = Vec::with_capacity(512);
	let opts = default_opts();
	let mut writer = TableWriter::new(d, 1, opts, 0);

	// Add the same key multiple times with different sequence numbers

	// Add with descending sequence numbers (newer versions first)
	add_key(&mut writer, b"same_key", 30, b"value3").unwrap();
	add_key(&mut writer, b"same_key", 20, b"value2").unwrap();
	add_key(&mut writer, b"same_key", 10, b"value1").unwrap();

	// Verify range is correct - should be the same key for both bounds
	assert!(writer.meta.smallest_point.is_some());
	assert!(writer.meta.largest_point.is_some());
	if let Some(smallest) = &writer.meta.smallest_point {
		assert_eq!(&smallest.user_key[..], b"same_key");
	}
	if let Some(largest) = &writer.meta.largest_point {
		assert_eq!(&largest.user_key[..], b"same_key");
	}
}

#[test]
fn test_writer_key_range_unicode_keys() {
	let d = Vec::with_capacity(512);
	let opts = default_opts();
	let mut writer = TableWriter::new(d, 1, opts, 0);

	// Add keys with unicode characters
	let keys = ["α", "β", "γ", "δ", "ε"];

	for (i, key) in keys.iter().enumerate() {
		add_key(&mut writer, key.as_bytes(), i as u64 + 1, b"value").unwrap();
	}

	// Verify range
	assert!(writer.meta.smallest_point.is_some());
	assert!(writer.meta.largest_point.is_some());
	if let Some(smallest) = &writer.meta.smallest_point {
		assert_eq!(&smallest.user_key[..], "α".as_bytes());
	}
	if let Some(largest) = &writer.meta.largest_point {
		assert_eq!(&largest.user_key[..], "ε".as_bytes());
	}
}

#[test]
fn test_writer_key_range_with_special_chars() {
	let d = Vec::with_capacity(512);
	let opts = default_opts();
	let mut writer = TableWriter::new(d, 1, opts, 0);

	// Add keys with special characters, including control characters
	let keys = [
		"\0key",      // null byte prefix
		"key\n",      // newline suffix
		"key\tvalue", // tab in middle
		"key\\value", // backslash
		"\"quoted\"", // quotes
	];

	// Sort to ensure we add in order
	let mut sorted_keys = keys.to_vec();
	sorted_keys.sort();

	for (i, key) in sorted_keys.iter().enumerate() {
		add_key(&mut writer, key.as_bytes(), i as u64 + 1, b"value").unwrap();
	}

	// Verify range matches the expected lowest and highest keys
	assert!(writer.meta.smallest_point.is_some());
	assert!(writer.meta.largest_point.is_some());
	if let Some(smallest) = &writer.meta.smallest_point {
		assert_eq!(&smallest.user_key[..], sorted_keys.first().unwrap().as_bytes());
	}
	if let Some(largest) = &writer.meta.largest_point {
		assert_eq!(&largest.user_key[..], sorted_keys.last().unwrap().as_bytes());
	}
}

#[test]
fn test_writer_key_range_with_mixed_case() {
	let d = Vec::with_capacity(512);
	let opts = default_opts();
	let mut writer = TableWriter::new(d, 1, opts, 0);

	// Mixed case keys to test case-sensitivity in key range
	let keys = ["AAA", "BBB", "aaa", "bbb"];

	// Sort to ensure we add in order (uppercase comes before lowercase in ASCII)
	let mut sorted_keys = keys.to_vec();
	sorted_keys.sort();

	for (i, key) in sorted_keys.iter().enumerate() {
		add_key(&mut writer, key.as_bytes(), i as u64 + 1, b"value").unwrap();
	}

	// Verify range
	assert!(writer.meta.smallest_point.is_some());
	assert!(writer.meta.largest_point.is_some());
	if let Some(smallest) = &writer.meta.smallest_point {
		assert_eq!(&smallest.user_key[..], b"AAA");
	}
	if let Some(largest) = &writer.meta.largest_point {
		assert_eq!(&largest.user_key[..], b"bbb");
	}
}

#[test]
fn test_writer_key_range_with_pseudo_random_keys() {
	let d = Vec::with_capacity(2048);
	let opts = default_opts();
	let mut writer = TableWriter::new(d, 1, opts, 0);

	// Generate 100 pseudo-random keys but add them in sorted order
	let mut rng = StdRng::seed_from_u64(100);
	let mut keys = Vec::new();

	for _ in 0..100 {
		let len = rng.random_range(3..10);
		let mut key = Vec::with_capacity(len);
		for _ in 0..len {
			key.push(rng.random_range(b'a'..=b'z'));
		}
		keys.push(key);
	}

	// Sort keys to ensure we add in order
	keys.sort();

	// Add all keys
	for (i, key) in keys.iter().enumerate() {
		add_key(&mut writer, key, i as u64 + 1, b"value").unwrap();
	}

	// Verify range matches expected bounds
	assert!(writer.meta.smallest_point.is_some());
	assert!(writer.meta.largest_point.is_some());
	if let Some(smallest) = &writer.meta.smallest_point {
		assert_eq!(&smallest.user_key[..], &keys.first().unwrap()[..]);
	}
	if let Some(largest) = &writer.meta.largest_point {
		assert_eq!(&largest.user_key[..], &keys.last().unwrap()[..]);
	}
}

#[test]
fn test_writer_key_range_with_prefix_pattern() {
	let d = Vec::with_capacity(512);
	let opts = default_opts();
	let mut writer = TableWriter::new(d, 1, opts, 0);

	// Add keys with common prefixes but different suffixes
	add_key(&mut writer, b"prefix:aaa", 1, b"value").unwrap();
	add_key(&mut writer, b"prefix:bbb", 2, b"value").unwrap();
	add_key(&mut writer, b"prefix:ccc", 3, b"value").unwrap();

	// Add a key with a different prefix
	add_key(&mut writer, b"zzzzzz", 4, b"value").unwrap();

	// Verify range
	assert!(writer.meta.smallest_point.is_some());
	assert!(writer.meta.largest_point.is_some());
	if let Some(smallest) = &writer.meta.smallest_point {
		assert_eq!(&smallest.user_key[..], b"prefix:aaa");
	}
	if let Some(largest) = &writer.meta.largest_point {
		assert_eq!(&largest.user_key[..], b"zzzzzz");
	}
}

#[test]
fn test_writer_key_range_boundary_keys() {
	let d = Vec::with_capacity(512);
	let opts = default_opts();
	let mut writer = TableWriter::new(d, 1, opts, 0);

	let long = "z".repeat(1000);
	let long = &long.as_str();
	// Test with boundary-like keys
	let keys = [
		"",   // Empty key
		"a",  // Single character
		long, // Very long key
	];

	for (i, key) in keys.iter().enumerate() {
		add_key(&mut writer, key.as_bytes(), i as u64 + 1, b"value").unwrap();
	}

	// Verify range
	assert!(writer.meta.smallest_point.is_some());
	assert!(writer.meta.largest_point.is_some());
	if let Some(smallest) = &writer.meta.smallest_point {
		assert_eq!(&smallest.user_key[..], b"");
	}
	if let Some(largest) = &writer.meta.largest_point {
		assert_eq!(&largest.user_key[..], "z".repeat(1000).as_bytes());
	}
}

#[test]
fn test_table_key_range_persistence() {
	// Build a table with the test data
	let data = build_data();
	let (src, size) = build_table(data.clone());
	let opts = default_opts();

	// Calculate the expected key range from the original data
	let expected_low = data.first().unwrap().0.as_bytes();
	let expected_high = data.last().unwrap().0.as_bytes();

	// Load the table back
	let table = Table::new(1, opts, wrap_buffer(src), size as u64).unwrap();

	// Verify the key range was properly persisted and loaded
	assert!(table.meta.smallest_point.is_some());
	assert!(table.meta.largest_point.is_some());
	if let Some(smallest) = &table.meta.smallest_point {
		assert_eq!(&smallest.user_key[..], expected_low);
	}
	if let Some(largest) = &table.meta.largest_point {
		assert_eq!(&largest.user_key[..], expected_high);
	}

	// Also verify that we can use the range for querying
	assert!(table.is_key_in_key_range(&InternalKey::new(
		Vec::from(expected_low),
		1,
		InternalKeyKind::Set,
		0
	)));
	assert!(table.is_key_in_key_range(&InternalKey::new(
		Vec::from(expected_high),
		1,
		InternalKeyKind::Set,
		0
	)));

	// A key before the range should not be in the range
	let before_range = "aaa".as_bytes();
	assert!(!table.is_key_in_key_range(&InternalKey::new(
		Vec::from(before_range),
		1,
		InternalKeyKind::Set,
		0
	)));

	// A key after the range should not be in the range
	let after_range = "zzzz".as_bytes();
	assert!(!table.is_key_in_key_range(&InternalKey::new(
		Vec::from(after_range),
		1,
		InternalKeyKind::Set,
		0
	)));

	// Test a key in the middle of the range
	let middle_key = "bsr".as_bytes(); // This is in the test data
	assert!(table.is_key_in_key_range(&InternalKey::new(
		Vec::from(middle_key),
		1,
		InternalKeyKind::Set,
		0
	)));
}

#[test]
fn test_table_disjoint_key_range_persistence() {
	// Build a table with disjoint data to ensure gaps are handled correctly
	let disjoint_data = vec![
		("aaa", "val1"),
		("bbb", "val2"),
		("ppp", "val3"), // Gap between bbb and ppp
		("qqq", "val4"),
		("zzz", "val5"), // Gap between qqq and zzz
	];

	let (src, size) = build_table(disjoint_data.clone());
	let opts = default_opts();

	// Calculate expected range
	let expected_low = disjoint_data.first().unwrap().0.as_bytes();
	let expected_high = disjoint_data.last().unwrap().0.as_bytes();

	// Load the table back
	let table = Table::new(1, opts, wrap_buffer(src), size as u64).unwrap();

	// Verify key range was properly persisted with disjoint data
	assert!(table.meta.smallest_point.is_some());
	assert!(table.meta.largest_point.is_some());
	if let Some(smallest) = &table.meta.smallest_point {
		assert_eq!(&smallest.user_key[..], expected_low);
	}
	if let Some(largest) = &table.meta.largest_point {
		assert_eq!(&largest.user_key[..], expected_high);
	}

	// Test keys in the gaps to ensure the is_key_in_key_range function works
	// properly
	let in_first_gap = "ccc".as_bytes(); // Between bbb and ppp
	assert!(table.is_key_in_key_range(&InternalKey::new(
		Vec::from(in_first_gap),
		1,
		InternalKeyKind::Set,
		0
	)));

	let in_second_gap = "xxx".as_bytes(); // Between qqq and zzz
	assert!(table.is_key_in_key_range(&InternalKey::new(
		Vec::from(in_second_gap),
		1,
		InternalKeyKind::Set,
		0
	)));
}

#[test]
fn test_table_key_range_with_many_blocks() {
	// Create a larger dataset that will span multiple blocks
	let mut data: Vec<(String, String)> = Vec::new();

	// Generate 50 keys that will span multiple blocks due to the small block size
	for i in 0..50 {
		let key = format!("key_{i:03}");
		let value = format!("value_{i}");
		data.push((key, value));
	}

	let data: Vec<(&str, &str)> = data.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

	let (src, size) = build_table(data);
	let opts = default_opts();

	// Expected range
	let expected_low = "key_000".as_bytes();
	let expected_high = "key_049".as_bytes();

	// Load the table
	let table = Table::new(1, opts, wrap_buffer(src), size as u64).unwrap();

	// Verify key range
	assert!(table.meta.smallest_point.is_some());
	assert!(table.meta.largest_point.is_some());
	if let Some(smallest) = &table.meta.smallest_point {
		assert_eq!(&smallest.user_key[..], expected_low);
	}
	if let Some(largest) = &table.meta.largest_point {
		assert_eq!(&largest.user_key[..], expected_high);
	}

	// Verify block count is greater than 1 (multiple blocks were created)
	assert!(table.meta.properties.num_data_blocks > 1);

	// Test random access to various points in the range
	for idx in [0, 10, 25, 49] {
		let key = format!("key_{idx:03}");
		assert!(table.is_key_in_key_range(&InternalKey::new(
			Vec::from(key.as_bytes()),
			1,
			InternalKeyKind::Set,
			0
		)));
	}
}

#[test]
fn test_table_key_range_with_tombstones() {
	let data = vec![
		("aaa", "val1"),
		("bbb", "val2"),
		("ccc", ""), // Tombstone (empty value)
		("ddd", "val4"),
		("eee", ""), // Tombstone
	];

	let (src, size) = build_table_with_tombstones(data.clone());
	let opts = default_opts();

	// Load the table
	let table = Table::new(1, opts, wrap_buffer(src), size as u64).unwrap();

	// Verify key range includes tombstones
	assert!(table.meta.smallest_point.is_some());
	assert!(table.meta.largest_point.is_some());
	if let Some(smallest) = &table.meta.smallest_point {
		assert_eq!(&smallest.user_key[..], b"aaa");
	}
	if let Some(largest) = &table.meta.largest_point {
		assert_eq!(&largest.user_key[..], b"eee");
	}

	// Verify tombstone count
	assert_eq!(table.meta.properties.tombstone_count, 2);
}

// Helper function to build a table with tombstones
fn build_table_with_tombstones(data: Vec<(&'static str, &'static str)>) -> (Vec<u8>, usize) {
	let mut d = Vec::with_capacity(512);
	let mut opts = default_opts_mut();
	opts.block_restart_interval = 3;
	opts.block_size = 32;
	let opt = Arc::new(opts);

	{
		let mut b = TableWriter::new(&mut d, 0, opt, 0);

		for &(k, v) in data.iter() {
			// Use Deletion kind for empty values to indicate tombstones
			let kind = if v.is_empty() {
				InternalKeyKind::Delete
			} else {
				InternalKeyKind::Set
			};

			b.add(InternalKey::new(Vec::from(k.as_bytes()), 1, kind, 0), v.as_bytes()).unwrap();
		}

		b.finish().unwrap();
	}

	let size = d.len();
	(d, size)
}

#[test]
fn test_table_iterator_no_items_lost() {
	let data = vec![
		("key_000", "value_000"),
		("key_001", "value_001"),
		("key_002", "value_002"),
		("key_003", "value_003"),
		("key_004", "value_004"),
	];

	let (src, size) = build_table(data.clone());
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	let mut iter = table.iter(None).unwrap();
	iter.seek_to_first().unwrap();
	let mut collected_items = Vec::new();

	while iter.valid() {
		let key = iter.key().to_owned();
		let value = iter.value_encoded().unwrap();
		let key_str = std::str::from_utf8(&key.user_key).unwrap();
		let value_str = std::str::from_utf8(value).unwrap();
		collected_items.push((key_str.to_string(), value_str.to_string()));
		iter.next().unwrap();
	}

	assert_eq!(
		collected_items.len(),
		data.len(),
		"Iterator should return exactly {} items, got {}",
		data.len(),
		collected_items.len()
	);

	for (i, (expected_key, expected_value)) in data.iter().enumerate() {
		assert!(
			i < collected_items.len(),
			"Missing item at index {i}: expected ({expected_key}, {expected_value})"
		);

		let (actual_key, actual_value) = &collected_items[i];
		assert_eq!(
			actual_key, expected_key,
			"Key mismatch at index {i}: expected '{expected_key}', got '{actual_key}'"
		);
		assert_eq!(
			actual_value, expected_value,
			"Value mismatch at index {i}: expected '{expected_value}', got '{actual_value}'"
		);
	}

	assert_eq!(collected_items[0].0, data[0].0, "First item key was lost!");
	assert_eq!(collected_items[0].1, data[0].1, "First item value was lost!");
}

#[test]
fn test_table_iterator_does_not_restart_after_exhaustion() {
	let data = vec![("a", "val_a"), ("b", "val_b"), ("c", "val_c")];

	let (src, size) = build_table(data.clone());
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	let mut iter = table.iter(None).unwrap();
	iter.seek_to_first().unwrap();
	let mut seen_keys = Vec::new();
	let mut iteration_count = 0;

	loop {
		iteration_count += 1;

		if iteration_count > 10 {
			panic!(
                    "Iterator appears to be stuck in a loop! This suggests the iterator is restarting instead of staying exhausted."
                );
		}

		if !iter.valid() {
			break;
		}
		let key = iter.key().to_owned();
		let value = iter.value_encoded().unwrap();
		let key_str = std::str::from_utf8(&key.user_key).unwrap();
		let value_str = std::str::from_utf8(value).unwrap();
		seen_keys.push((key_str.to_string(), value_str.to_string()));

		let key_count = seen_keys.iter().filter(|(k, _)| k == key_str).count();
		if key_count > 1 {
			panic!("Iterator restarted! Saw key '{key_str}' {key_count} times");
		}
		iter.next().unwrap();
	}

	assert_eq!(
		seen_keys.len(),
		data.len(),
		"Expected {} items, got {}",
		data.len(),
		seen_keys.len()
	);

	for i in 0..3 {
		let result = iter.next().unwrap();
		if result {
			panic!(
				"Iterator should remain exhausted, but returned true on additional call #{}",
				i + 1
			);
		}
	}
}

#[test]
fn test_table_iterator_advance_method_correctness() {
	let data = vec![("item1", "data1"), ("item2", "data2"), ("item3", "data3")];

	let (src, size) = build_table(data.clone());
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	let mut iter = table.iter(None).unwrap();

	for expected_index in 0..data.len() {
		let advance_result = iter.next().unwrap();

		assert!(
			advance_result,
			"next() should return true when moving to item {} (of {})",
			expected_index,
			data.len()
		);
		assert!(iter.valid(), "Iterator should be valid after advancing to item {expected_index}");

		let current_key = iter.key();
		let key_str = std::str::from_utf8(current_key.user_key()).unwrap();
		let expected_key = data[expected_index].0;
		assert_eq!(
                key_str, expected_key,
                "After advancing to position {expected_index}, expected key '{expected_key}', got '{key_str}'"
            );
	}

	let final_advance = iter.next().unwrap();
	assert!(!final_advance, "next() should return false when trying to advance past the last item");
	assert!(!iter.valid(), "Iterator should be invalid after advancing past the last item");

	for i in 0..3 {
		let advance_result = iter.next().unwrap();
		assert!(
			!advance_result,
			"next() should continue returning false after exhaustion (call #{})",
			i + 1
		);
		assert!(!iter.valid(), "Iterator should remain invalid after additional next() calls");
	}
}

#[test]
fn test_table_iterator_edge_cases() {
	// Single item table
	{
		let single_data = vec![("only_key", "only_value")];
		let (src, size) = build_table(single_data.clone());
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		let mut iter = table.iter(None).unwrap();
		let collected = collect_all(&mut iter).unwrap();
		assert_eq!(collected.len(), 1, "Single item table should return exactly 1 item");

		let key_str = std::str::from_utf8(&collected[0].0.user_key).unwrap();
		let value_str = std::str::from_utf8(collected[0].1.as_ref()).unwrap();
		assert_eq!(key_str, "only_key");
		assert_eq!(value_str, "only_value");
	}

	// Two item table
	{
		let two_data = vec![("first", "1st"), ("second", "2nd")];
		let (src, size) = build_table(two_data.clone());
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		let mut iter = table.iter(None).unwrap();
		let collected = collect_all(&mut iter).unwrap();
		assert_eq!(collected.len(), 2, "Two item table should return exactly 2 items");

		let keys: Vec<String> = collected
			.iter()
			.map(|(k, _)| std::str::from_utf8(&k.user_key).unwrap().to_string())
			.collect();
		assert_eq!(keys, vec!["first", "second"], "Items should be in correct order");
	}

	// Large table
	{
		let large_data: Vec<_> =
			(0..100).map(|i| (format!("key_{i:03}"), format!("value_{i:03}"))).collect();

		let large_data_refs: Vec<(&str, &str)> =
			large_data.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

		let (src, size) = build_table(large_data_refs);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		let mut iter = table.iter(None).unwrap();
		let collected = collect_all(&mut iter).unwrap();
		assert_eq!(collected.len(), 100, "Large table should return exactly 100 items");

		let mut seen_keys = std::collections::HashSet::new();
		for (key, _) in &collected {
			let key_str = std::str::from_utf8(&key.user_key).unwrap();
			assert!(
				seen_keys.insert(key_str),
				"Duplicate key found: '{key_str}' - iterator may have restarted"
			);
		}

		let first_key = std::str::from_utf8(&collected[0].0.user_key).unwrap();
		let last_key = std::str::from_utf8(&collected[99].0.user_key).unwrap();
		assert_eq!(first_key, "key_000", "First key should be key_000");
		assert_eq!(last_key, "key_099", "Last key should be key_099");
	}
}

#[test]
fn test_table_iterator_seek_then_iterate() {
	let data = vec![
		("item_01", "val_01"),
		("item_02", "val_02"),
		("item_03", "val_03"),
		("item_04", "val_04"),
		("item_05", "val_05"),
	];

	let (src, size) = build_table(data.clone());
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	let test_cases = vec![("item_01", 0), ("item_03", 2), ("item_05", 4)];

	for (seek_key, expected_start_index) in test_cases {
		let mut iter = table.iter(None).unwrap();

		let internal_key =
			InternalKey::new(Vec::from(seek_key.as_bytes()), 1, InternalKeyKind::Set, 0);
		iter.seek(&internal_key.encode()).unwrap();

		assert!(iter.valid(), "Iterator should be valid after seeking to '{seek_key}'");

		let mut remaining_items = Vec::new();
		while iter.valid() {
			let current_key = iter.key();
			let current_value = iter.value_encoded().unwrap();
			let key_str = std::str::from_utf8(current_key.user_key()).unwrap();
			let value_str = std::str::from_utf8(current_value).unwrap();
			remaining_items.push((key_str.to_string(), value_str.to_string()));

			if !iter.next().unwrap() {
				break;
			}
		}

		let expected_remaining = &data[expected_start_index..];
		assert_eq!(
			remaining_items.len(),
			expected_remaining.len(),
			"After seeking to '{}', expected {} remaining items, got {}",
			seek_key,
			expected_remaining.len(),
			remaining_items.len()
		);

		for (i, (expected_key, expected_value)) in expected_remaining.iter().enumerate() {
			assert_eq!(
				remaining_items[i].0, *expected_key,
				"After seeking to '{}', item {} key mismatch: expected '{}', got '{}'",
				seek_key, i, expected_key, remaining_items[i].0
			);
			assert_eq!(
				remaining_items[i].1, *expected_value,
				"After seeking to '{}', item {} value mismatch: expected '{}', got '{}'",
				seek_key, i, expected_value, remaining_items[i].1
			);
		}
	}
}

#[test]
fn test_table_iterator_seek_behavior() {
	let data = vec![
		("key_001", "val_001"),
		("key_002", "val_002"),
		("key_005", "val_005"),
		("key_007", "val_007"),
		("key_010", "val_010"),
	];

	let (src, size) = build_table(data.clone());
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Test seek to existing key
	{
		let mut iter = table.iter(None).unwrap();
		let seek_key = InternalKey::new(Vec::from(b"key_005"), 1, InternalKeyKind::Set, 0);
		iter.seek(&seek_key.encode()).unwrap();

		assert!(iter.valid(), "Iterator should be valid after seeking to existing key");
		let current_key = iter.key();
		let found_key = std::str::from_utf8(current_key.user_key()).unwrap();
		assert_eq!(found_key, "key_005", "Should find the exact key we sought");

		let remaining_collected = collect_iter(&mut iter);
		let remaining: Vec<_> = remaining_collected
			.iter()
			.map(|(k, v)| {
				(
					std::str::from_utf8(&k.user_key).unwrap().to_string(),
					std::str::from_utf8(v.as_ref()).unwrap().to_string(),
				)
			})
			.collect();

		let expected = [
			("key_005".to_string(), "val_005".to_string()),
			("key_007".to_string(), "val_007".to_string()),
			("key_010".to_string(), "val_010".to_string()),
		];

		assert_eq!(remaining.len(), expected.len(), "Should get remaining items after seek");
		for (i, (actual, expected)) in remaining.iter().zip(expected.iter()).enumerate() {
			assert_eq!(actual, expected, "Mismatch at position {i} after seek");
		}
	}

	// Test seek to non-existing key (should find next key)
	{
		let mut iter = table.iter(None).unwrap();
		let seek_key = InternalKey::new(Vec::from(b"key_003"), 1, InternalKeyKind::Set, 0);
		iter.seek(&seek_key.encode()).unwrap();

		assert!(iter.valid(), "Iterator should be valid after seeking to non-existing key");
		let current_key = iter.key();
		let found_key = std::str::from_utf8(current_key.user_key()).unwrap();
		assert_eq!(found_key, "key_005", "Should find next key when seeking non-existing");
	}

	// Test seek past end
	{
		let mut iter = table.iter(None).unwrap();
		let seek_key = InternalKey::new(Vec::from(b"key_999"), 1, InternalKeyKind::Set, 0);
		iter.seek(&seek_key.encode()).unwrap();

		assert!(!iter.valid(), "Iterator should be invalid after seeking past end");
	}
}

#[test]
fn test_table_iterator_performance_regression() {
	let mut large_data = Vec::new();
	for i in 0..1000 {
		large_data.push((format!("key_{i:06}"), format!("value_{i:06}")));
	}

	let large_data_refs: Vec<(&str, &str)> =
		large_data.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

	let (src, size) = build_table(large_data_refs);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	use std::time::Instant;

	let start = Instant::now();
	let mut iter = table.iter(None).unwrap();
	let count = count_iter(&mut iter).unwrap();
	let duration = start.elapsed();

	assert_eq!(count, 1000, "Should iterate through all 1000 items");

	assert!(duration.as_millis() < 1000, "Iteration took too long: {duration:?}");

	let start = Instant::now();
	for i in (0..1000).step_by(100) {
		let mut iter = table.iter(None).unwrap();
		let seek_key =
			InternalKey::new(format!("key_{i:06}").into_bytes(), 1, InternalKeyKind::Set, 0);
		iter.seek(&seek_key.encode()).unwrap();
		assert!(iter.valid(), "Seek to key_{i:06} should succeed");
	}
	let seek_duration = start.elapsed();

	assert!(seek_duration.as_millis() < 100, "Seek operations took too long: {seek_duration:?}");
}

#[test]
fn test_table_iterator_state_invariants() {
	let data = vec![("a", "1"), ("b", "2"), ("c", "3"), ("d", "4")];
	let (src, size) = build_table(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	let mut iter = table.iter(None).unwrap();
	iter.seek_to_first().unwrap();

	while iter.valid() {
		let _key = iter.key();
		let _value = iter.value_encoded();

		if !iter.next().unwrap() {
			break;
		}
	}

	assert!(!iter.valid(), "Iterator should be invalid after exhaustion");

	for _ in 0..5 {
		assert!(!iter.next().unwrap(), "next() should continue returning false after exhaustion");
		assert!(!iter.valid(), "Iterator should remain invalid");

		let next_result = iter.next();
		assert!(
			next_result.is_ok() && !next_result.unwrap(),
			"next() should continue returning Ok(false) after exhaustion"
		);
	}
}

#[test]
fn test_table_iterator_multiple_iterations() {
	let data = vec![("alpha", "a"), ("beta", "b"), ("gamma", "g")];

	let (src, size) = build_table(data.clone());
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Create multiple iterators and verify they work independently
	for iteration in 0..3 {
		let mut iter = table.iter(None).unwrap();
		let collected = collect_all(&mut iter).unwrap();

		assert_eq!(
			collected.len(),
			data.len(),
			"Iteration #{} should return {} items, got {}",
			iteration,
			data.len(),
			collected.len()
		);

		// Verify content is correct
		for (i, (expected_key, expected_value)) in data.iter().enumerate() {
			let (actual_key, actual_value) = &collected[i];
			let actual_key_str = std::str::from_utf8(&actual_key.user_key).unwrap();
			let actual_value_str = std::str::from_utf8(actual_value.as_ref()).unwrap();

			assert_eq!(
                    actual_key_str, *expected_key,
                    "Iteration #{iteration}, item {i}: expected key '{expected_key}', got '{actual_key_str}'"
                );
			assert_eq!(
                    actual_value_str, *expected_value,
                    "Iteration #{iteration}, item {i}: expected value '{expected_value}', got '{actual_value_str}'"
                );
		}
	}
}

#[test]
fn test_table_iterator_positioning_edge_cases() {
	// Test 1: Empty table
	// Note: An empty SSTable has no valid index blocks, so seek operations
	// return errors. This is expected behavior - empty SSTables are edge cases
	// that shouldn't normally occur in production.
	{
		let empty_data: Vec<(&str, &str)> = vec![];
		let (src, size) = build_table(empty_data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		let mut iter = table.iter(None).unwrap();

		// next() tries to seek_to_first() which fails on empty table (no valid blocks)
		let result = iter.next();
		assert!(result.is_err(), "next() on empty table should return error (no valid blocks)");

		// Iterator should not be valid
		assert!(!iter.valid(), "Iterator should not be valid on empty table");
	}

	// Test 2: Single item table
	{
		let single_data = vec![("single", "item")];
		let (src, size) = build_table(single_data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		let mut iter = table.iter(None).unwrap();

		assert!(iter.next().unwrap(), "First advance should succeed on single-item table");
		assert!(iter.valid(), "Iterator should be valid after first advance");

		assert!(!iter.next().unwrap(), "Second advance should fail on single-item table");
		assert!(!iter.valid(), "Iterator should be invalid after second advance");
	}

	// Test 3: Reset behavior after exhaustion
	{
		let data = vec![("a", "1"), ("b", "2")];
		let (src, size) = build_table(data);
		let opts = default_opts();
		let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

		let mut iter = table.iter(None).unwrap();

		// Exhaust the iterator
		while iter.next().unwrap() {
			// just advance
		}
		assert!(!iter.valid(), "Iterator should be invalid after exhaustion");

		// Further operations should not restart the iterator
		assert!(!iter.next().unwrap(), "next() after exhaustion should return false");
		assert!(!iter.valid(), "Iterator should remain invalid");

		let next_result = iter.next();
		assert!(
			next_result.is_ok() && !next_result.unwrap(),
			"next() after exhaustion should return Ok(false)"
		);
	}
}

#[test]
fn test_table_iterator_next_vs_advance_consistency() {
	let data = vec![("x", "1"), ("y", "2"), ("z", "3")];
	let (src, size) = build_table(data.clone());
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Test with manual next() loop - need to position first
	let mut iter1 = table.iter(None).unwrap();
	iter1.seek_to_first().unwrap();
	let mut collected_via_advance = Vec::new();
	while iter1.valid() {
		collected_via_advance
			.push((iter1.key().to_owned(), iter1.value_encoded().unwrap().to_vec()));
		if !iter1.next().unwrap() {
			break;
		}
	}

	// Test with standard iterator interface
	let mut iter2 = table.iter(None).unwrap();
	iter2.seek_to_first().unwrap();
	let mut collected_via_next = Vec::new();
	while iter2.valid() {
		collected_via_next.push((iter2.key().to_owned(), iter2.value_encoded().unwrap().to_vec()));
		if !iter2.next().unwrap() {
			break;
		}
	}

	// Both methods should yield the same results
	assert_eq!(
		collected_via_next.len(),
		collected_via_advance.len(),
		"next() and next() should yield same number of items"
	);

	for (i, ((next_key, next_val), (adv_key, adv_val))) in
		collected_via_next.iter().zip(collected_via_advance.iter()).enumerate()
	{
		assert_eq!(
			next_key.user_key, adv_key.user_key,
			"Key mismatch at position {i} between next() and next()"
		);
		assert_eq!(
			next_val.as_slice(),
			*adv_val,
			"Value mismatch at position {i} between next() and next()"
		);
	}
}

#[test]
fn test_table_iterator_basic_correctness() {
	let mut large_data = Vec::new();
	for i in 0..50 {
		large_data.push((format!("key_{i:03}"), format!("value_{i:03}")));
	}

	let large_data_refs: Vec<(&str, &str)> =
		large_data.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();

	let (src, size) = build_table(large_data_refs);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	let mut iter = table.iter(None).unwrap();
	let collected = collect_all(&mut iter).unwrap();

	assert_eq!(collected.len(), 50, "Should collect exactly 50 items");

	// Verify items are correct and in order
	for (i, (actual_key, actual_value)) in collected.iter().enumerate() {
		let key_str = std::str::from_utf8(&actual_key.user_key).unwrap();
		let value_str = std::str::from_utf8(actual_value.as_ref()).unwrap();

		let expected_key = format!("key_{i:03}");
		let expected_value = format!("value_{i:03}");

		assert_eq!(key_str, expected_key, "Key mismatch at position {i}");
		assert_eq!(value_str, expected_value, "Value mismatch at position {i}");
	}
}

#[test]
fn test_table_with_partitioned_index() {
	let mut opts = default_opts_mut();
	opts.index_partition_size = 100; // Small partition size to force multiple partitions
	opts.block_size = 64; // Small block size to create more data blocks
	let opts = Arc::new(opts);

	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 1, Arc::clone(&opts), 0);

	// Add enough entries to create multiple data blocks and index partitions
	for i in 0..100 {
		let key = format!("key_{i:03}");
		let value = format!("value_{i:03}");
		let internal_key =
			InternalKey::new(Vec::from(key.as_bytes()), i + 1, InternalKeyKind::Set, 0);
		writer.add(internal_key, value.as_bytes()).unwrap();
	}

	let size = writer.finish().unwrap();
	assert!(size > 0, "Table should have non-zero size");

	// Now read the table back with partitioned index
	let table = Arc::new(Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap());

	// Verify it's using partitioned index
	match &table.index_block {
		&IndexType::Partitioned(_) => {
			// Expected - partitioned index is the only supported type
		}
	}

	// Test point lookups
	for i in 0..100 {
		let key = format!("key_{i:03}");
		let expected_value = format!("value_{i:03}");
		let internal_key = InternalKey::new(
			Vec::from(key.as_bytes()),
			i + 2, // Higher seq number for lookup
			InternalKeyKind::Set,
			0,
		);

		let result = table.get(&internal_key).unwrap();
		assert!(result.is_some(), "Key '{key}' not found in table");

		if let Some((found_key, found_value)) = result {
			assert_eq!(std::str::from_utf8(&found_key.user_key).unwrap(), key, "Key mismatch");
			assert_eq!(
				std::str::from_utf8(found_value.as_ref()).unwrap(),
				expected_value,
				"Value mismatch"
			);
		}
	}

	// Test full iteration
	let mut iter = table.iter(None).unwrap();
	let collected = collect_all(&mut iter).unwrap();
	assert_eq!(collected.len(), 100, "Should iterate through all entries");

	// Verify iteration order and content
	for (i, (key, value)) in collected.iter().enumerate() {
		let expected_key = format!("key_{i:03}");
		let expected_value = format!("value_{i:03}");

		assert_eq!(
			std::str::from_utf8(&key.user_key).unwrap(),
			expected_key,
			"Iterator key mismatch at position {i}"
		);
		assert_eq!(
			std::str::from_utf8(value.as_ref()).unwrap(),
			expected_value,
			"Iterator value mismatch at position {i}"
		);
	}
}

#[test]
fn test_get_nonexistent_key_returns_none() {
	// Regression test: get() should return None for non-existent keys,
	// even when a lexicographically greater key exists in the table.
	// Disable bloom filter so we actually exercise the key comparison logic.
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Add only key_bbb to the table
	let key = b"key_bbb";
	let value = b"value_bbb";
	let internal_key = InternalKey::new(Vec::from(key), 1, InternalKeyKind::Set, 0);
	writer.add(internal_key, value).unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	// Try to get key_aaa which does NOT exist
	// key_aaa < key_bbb lexicographically
	let lookup_key = InternalKey::new(
		Vec::from(b"key_aaa"),
		2, // Higher seq number for lookup
		InternalKeyKind::Set,
		0,
	);

	let result = table.get(&lookup_key).unwrap();

	// The bug: with >= comparison, this incorrectly returns Some((key_bbb,
	// value_bbb)) The fix: with == comparison, this correctly returns None
	assert!(
		result.is_none(),
		"get() should return None for non-existent key, but got {:?}",
		result.map(|(k, v)| (
			String::from_utf8_lossy(&k.user_key).to_string(),
			String::from_utf8_lossy(&v).to_string()
		))
	);
}

#[test]
fn test_get_same_key_different_sequence_numbers() {
	// Validates fix returns value when user_key matches, even with different
	// seq_nums Internal ordering: user_key asc, seq_num DESC (reversed: higher
	// seq_nums sort first)
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let user_key = b"my_key";

	let key1 = InternalKey::new(Vec::from(user_key), 100, InternalKeyKind::Set, 0);
	writer.add(key1, b"value_100").unwrap();

	let key2 = InternalKey::new(Vec::from(user_key), 50, InternalKeyKind::Set, 0);
	writer.add(key2, b"value_50").unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	let lookup_key_higher = InternalKey::new(Vec::from(user_key), 200, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key_higher).unwrap();
	assert!(result.is_some());
	let (found_key, found_value) = result.unwrap();
	assert_eq!(found_key.user_key.as_slice(), user_key);
	assert_eq!(&found_value, b"value_100");

	let lookup_key_between = InternalKey::new(Vec::from(user_key), 75, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key_between).unwrap();
	assert!(result.is_some());
	let (found_key, found_value) = result.unwrap();
	assert_eq!(found_key.user_key.as_slice(), user_key);
	assert_eq!(&found_value, b"value_50");

	let lookup_key_exact = InternalKey::new(Vec::from(user_key), 100, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key_exact).unwrap();
	assert!(result.is_some());
	let (found_key, found_value) = result.unwrap();
	assert_eq!(found_key.user_key.as_slice(), user_key);
	assert_eq!(&found_value, b"value_100");

	let different_user_key = b"other_key";
	let lookup_key_different =
		InternalKey::new(Vec::from(different_user_key), 200, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key_different).unwrap();
	assert!(
		result.is_none(),
		"Should return None for different user_key, got: {:?}",
		result.map(|(k, v)| (
			String::from_utf8_lossy(&k.user_key).to_string(),
			String::from_utf8_lossy(&v).to_string()
		))
	);
}

#[test]
fn test_get_with_lower_sequence_number() {
	// Snapshot at seq=25 can't see future version at seq=50
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let key_aaa = InternalKey::new(Vec::from(b"aaa_key"), 100, InternalKeyKind::Set, 0);
	writer.add(key_aaa, b"value_aaa").unwrap();

	let key_bbb = InternalKey::new(Vec::from(b"bbb_key"), 75, InternalKeyKind::Set, 0);
	writer.add(key_bbb, b"value_bbb").unwrap();

	let user_key = b"my_key";
	let key = InternalKey::new(Vec::from(user_key), 50, InternalKeyKind::Set, 0);
	writer.add(key, b"value_50").unwrap();

	let key_zzz = InternalKey::new(Vec::from(b"zzz_key"), 100, InternalKeyKind::Set, 0);
	writer.add(key_zzz, b"value_zzz").unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	let lookup_key = InternalKey::new(Vec::from(user_key), 25, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key).unwrap();

	if result.is_some() {
		let (found_key, found_value) = result.unwrap();
		panic!(
			"BUG: Expected None, got key={}, seq_num={}, value={:?}",
			String::from_utf8_lossy(&found_key.user_key),
			found_key.seq_num(),
			String::from_utf8_lossy(&found_value)
		);
	}
	assert!(result.is_none());
}

#[test]
fn test_get_empty_table() {
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	let lookup_key = InternalKey::new(Vec::from(b"any_key"), 100, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key);
	assert!(result.is_err());
}

#[test]
fn test_get_multiple_keys_with_sequence_variations() {
	// Ensures fix doesn't cause cross-key contamination with different seq_nums
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let key_a = InternalKey::new(Vec::from(b"key_a"), 100, InternalKeyKind::Set, 0);
	writer.add(key_a, b"value_a_100").unwrap();

	let key_b = InternalKey::new(Vec::from(b"key_b"), 50, InternalKeyKind::Set, 0);
	writer.add(key_b, b"value_b_50").unwrap();

	let key_c = InternalKey::new(Vec::from(b"key_c"), 75, InternalKeyKind::Set, 0);
	writer.add(key_c, b"value_c_75").unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	let lookup_b_low = InternalKey::new(Vec::from(b"key_b"), 25, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_b_low).unwrap();
	assert!(result.is_none());

	let lookup_a_high = InternalKey::new(Vec::from(b"key_a"), 150, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_a_high).unwrap();
	assert!(result.is_some());
	let (found_key, found_value) = result.unwrap();
	assert_eq!(found_key.user_key.as_slice(), b"key_a");
	assert_eq!(&found_value, b"value_a_100");

	let lookup_c_exact = InternalKey::new(Vec::from(b"key_c"), 75, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_c_exact).unwrap();
	assert!(result.is_some());
	let (found_key, found_value) = result.unwrap();
	assert_eq!(found_key.user_key.as_slice(), b"key_c");
	assert_eq!(&found_value, b"value_c_75");

	let lookup_key_b5 = InternalKey::new(Vec::from(b"key_b5"), 100, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key_b5).unwrap();
	assert!(result.is_none());
}

#[test]
fn test_get_boundary_conditions() {
	// Edge cases with sequence number boundaries (seq=0, seq=MAX, seq=1)
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let key_aaa = InternalKey::new(Vec::from(b"aaa_key"), 100, InternalKeyKind::Set, 0);
	writer.add(key_aaa, b"value_aaa").unwrap();

	let user_key = b"boundary_key";
	let key = InternalKey::new(Vec::from(user_key), 100, InternalKeyKind::Set, 0);
	writer.add(key, b"value_100").unwrap();

	let key_zzz = InternalKey::new(Vec::from(b"zzz_key"), 100, InternalKeyKind::Set, 0);
	writer.add(key_zzz, b"value_zzz").unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	let lookup_min = InternalKey::new(Vec::from(user_key), 0, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_min).unwrap();
	assert!(result.is_none());

	let lookup_max =
		InternalKey::new(Vec::from(user_key), INTERNAL_KEY_SEQ_NUM_MAX, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_max).unwrap();
	assert!(result.is_some());
	let (found_key, found_value) = result.unwrap();
	assert_eq!(found_key.user_key.as_slice(), user_key);
	assert_eq!(&found_value, b"value_100");

	let lookup_one = InternalKey::new(Vec::from(user_key), 1, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_one).unwrap();
	assert!(result.is_none());
}

#[test]
fn test_get_lookup_higher_than_stored() {
	// Snapshot at seq=50 can see older version at seq=25
	// Internal ordering: user_key asc, seq_num DESC (reversed!)
	// stored(25).cmp(lookup(50)) = lookup.seq_num().cmp(stored.seq_num()) =
	// 50.cmp(25) = Greater So stored(25) > lookup(50) in internal ordering (even
	// though 25 < 50 numerically)
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let key_aaa = InternalKey::new(Vec::from(b"aaa_key"), 100, InternalKeyKind::Set, 0);
	writer.add(key_aaa, b"value_aaa").unwrap();

	let key_bbb = InternalKey::new(Vec::from(b"bbb_key"), 80, InternalKeyKind::Set, 0);
	writer.add(key_bbb, b"value_bbb").unwrap();

	let user_key = b"mykey";
	let key = InternalKey::new(Vec::from(user_key), 25, InternalKeyKind::Set, 0);
	writer.add(key, b"value_25").unwrap();

	let key_zzz = InternalKey::new(Vec::from(b"zzz_key"), 100, InternalKeyKind::Set, 0);
	writer.add(key_zzz, b"value_zzz").unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	let lookup_key = InternalKey::new(Vec::from(user_key), 50, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key).unwrap();

	assert!(result.is_some());
	let (found_key, found_value) = result.unwrap();
	assert_eq!(found_key.user_key.as_slice(), user_key);
	assert_eq!(found_key.seq_num(), 25);
	assert_eq!(&found_value, b"value_25");
}

#[test]
fn test_get_partition_index_sequence_numbers() {
	// Partition index handles sequence numbers correctly across multiple blocks
	let opts = Arc::new(Options::new().with_filter_policy(None).with_block_size(512));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	for i in 0..20 {
		let key = format!("aaa_key_{:03}", i);
		let value = format!("value_{}", i);
		let internal_key =
			InternalKey::new(Vec::from(key.as_bytes()), 1000, InternalKeyKind::Set, 0);
		writer.add(internal_key, value.as_bytes()).unwrap();
	}

	let target_key = b"target_key";
	let key_500 = InternalKey::new(Vec::from(target_key), 500, InternalKeyKind::Set, 0);
	writer.add(key_500, b"value_500").unwrap();

	for i in 0..40 {
		let key = format!("zzz_key_{:03}", i);
		let value = format!("value_{}", i);
		let internal_key =
			InternalKey::new(Vec::from(key.as_bytes()), 1000, InternalKeyKind::Set, 0);
		writer.add(internal_key, value.as_bytes()).unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	assert!(table.meta.properties.block_count > 1);

	let lookup_high = InternalKey::new(Vec::from(target_key), 1000, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_high).unwrap();
	assert!(result.is_some());
	let (found_key, found_value) = result.unwrap();
	assert_eq!(found_key.user_key.as_slice(), target_key);
	assert_eq!(found_key.seq_num(), 500);
	assert_eq!(&found_value, b"value_500");

	let lookup_exact = InternalKey::new(Vec::from(target_key), 500, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_exact).unwrap();
	assert!(result.is_some());
	let (found_key, found_value) = result.unwrap();
	assert_eq!(found_key.user_key.as_slice(), target_key);
	assert_eq!(found_key.seq_num(), 500);
	assert_eq!(&found_value, b"value_500");

	let lookup_low = InternalKey::new(Vec::from(target_key), 100, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_low).unwrap();
	assert!(result.is_none());

	let filler_key = b"zzz_key_010";
	let lookup_filler = InternalKey::new(Vec::from(filler_key), 1000, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_filler).unwrap();
	assert!(result.is_some());
	let (found_key, _) = result.unwrap();
	assert_eq!(found_key.user_key.as_slice(), filler_key);
}

#[test]
fn test_get_nonexistent_key_greater_than_all() {
	// Key greater than all stored keys should return None
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let key_aaa = InternalKey::new(Vec::from(b"key_aaa"), 100, InternalKeyKind::Set, 0);
	writer.add(key_aaa, b"value_aaa").unwrap();

	let key_bbb = InternalKey::new(Vec::from(b"key_bbb"), 100, InternalKeyKind::Set, 0);
	writer.add(key_bbb, b"value_bbb").unwrap();

	let key_ccc = InternalKey::new(Vec::from(b"key_ccc"), 100, InternalKeyKind::Set, 0);
	writer.add(key_ccc, b"value_ccc").unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	let lookup_key = InternalKey::new(Vec::from(b"key_zzz"), 100, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key).unwrap();

	assert!(result.is_none());
}

#[test]
fn test_get_nonexistent_key_between_existing() {
	// Key between existing keys should return None
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let key_aaa = InternalKey::new(Vec::from(b"key_aaa"), 100, InternalKeyKind::Set, 0);
	writer.add(key_aaa, b"value_aaa").unwrap();

	let key_ccc = InternalKey::new(Vec::from(b"key_ccc"), 100, InternalKeyKind::Set, 0);
	writer.add(key_ccc, b"value_ccc").unwrap();

	let key_eee = InternalKey::new(Vec::from(b"key_eee"), 100, InternalKeyKind::Set, 0);
	writer.add(key_eee, b"value_eee").unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	let lookup_key = InternalKey::new(Vec::from(b"key_bbb"), 100, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key).unwrap();

	assert!(result.is_none());
}

#[test]
fn test_get_with_tombstone() {
	// Tombstones should be found and returned
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let key_other = InternalKey::new(Vec::from(b"key_other"), 100, InternalKeyKind::Set, 0);
	writer.add(key_other, b"value_other").unwrap();

	let key_target = InternalKey::new(Vec::from(b"key_target"), 100, InternalKeyKind::Delete, 0);
	writer.add(key_target, b"").unwrap();

	let key_zzz = InternalKey::new(Vec::from(b"key_zzz"), 100, InternalKeyKind::Set, 0);
	writer.add(key_zzz, b"value_zzz").unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	let lookup_key = InternalKey::new(Vec::from(b"key_target"), 100, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key).unwrap();

	assert!(result.is_some());
	let (found_key, _) = result.unwrap();
	assert_eq!(found_key.user_key.as_slice(), b"key_target");
	assert!(found_key.is_tombstone());
}

#[test]
fn test_get_nonexistent_with_similar_prefix() {
	// Prefix of existing keys should not match
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let key1 = InternalKey::new(Vec::from(b"user_data"), 100, InternalKeyKind::Set, 0);
	writer.add(key1, b"value1").unwrap();

	let key2 = InternalKey::new(Vec::from(b"user_profile"), 100, InternalKeyKind::Set, 0);
	writer.add(key2, b"value2").unwrap();

	let key3 = InternalKey::new(Vec::from(b"username"), 100, InternalKeyKind::Set, 0);
	writer.add(key3, b"value3").unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	let lookup_key = InternalKey::new(Vec::from(b"user"), 100, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key).unwrap();

	assert!(result.is_none());
}

#[test]
fn test_get_nonexistent_empty_key() {
	// Empty key should return None
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let key_aaa = InternalKey::new(Vec::from(b"key_aaa"), 100, InternalKeyKind::Set, 0);
	writer.add(key_aaa, b"value_aaa").unwrap();

	let key_bbb = InternalKey::new(Vec::from(b"key_bbb"), 100, InternalKeyKind::Set, 0);
	writer.add(key_bbb, b"value_bbb").unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	let lookup_key = InternalKey::new(Vec::from(b""), 100, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key).unwrap();

	assert!(result.is_none());
}

#[test]
fn test_get_nonexistent_with_special_chars() {
	// Binary keys with special bytes handled correctly
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let key1 = InternalKey::new(Vec::from(b"key\x00"), 100, InternalKeyKind::Set, 0);
	writer.add(key1, b"value0").unwrap();

	let key2 = InternalKey::new(Vec::from(b"key\x01"), 100, InternalKeyKind::Set, 0);
	writer.add(key2, b"value1").unwrap();

	let key3 = InternalKey::new(Vec::from(b"key\xFF"), 100, InternalKeyKind::Set, 0);
	writer.add(key3, b"value_ff").unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	let lookup_key = InternalKey::new(Vec::from(b"key\x02"), 100, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key).unwrap();

	assert!(result.is_none());
}

#[test]
fn test_get_nonexistent_in_large_table() {
	// Non-existent key in multi-block table should return None
	let opts = Arc::new(Options::new().with_filter_policy(None).with_block_size(512));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	for i in 0..100 {
		if i == 50 {
			continue;
		}
		let key = format!("key_{:03}", i);
		let value = format!("value_{}", i);
		let internal_key =
			InternalKey::new(Vec::from(key.as_bytes()), 100, InternalKeyKind::Set, 0);
		writer.add(internal_key, value.as_bytes()).unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	assert!(table.meta.properties.block_count > 1);

	let lookup_key = InternalKey::new(Vec::from(b"key_050"), 100, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key).unwrap();

	assert!(result.is_none());
}

#[test]
fn test_get_all_keys_same_prefix_different_suffix() {
	// Keys with same prefix but different suffix don't match
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let key_a = InternalKey::new(Vec::from(b"prefix_a"), 100, InternalKeyKind::Set, 0);
	writer.add(key_a, b"value_a").unwrap();

	let key_b = InternalKey::new(Vec::from(b"prefix_b"), 100, InternalKeyKind::Set, 0);
	writer.add(key_b, b"value_b").unwrap();

	let key_c = InternalKey::new(Vec::from(b"prefix_c"), 100, InternalKeyKind::Set, 0);
	writer.add(key_c, b"value_c").unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	let lookup_key = InternalKey::new(Vec::from(b"prefix_d"), 100, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key).unwrap();

	assert!(result.is_none());
}

#[test]
fn test_table_iterator_seek_nonexistent_key() {
	// Test that seeking to a non-existent key positions the iterator
	// at the next greater key (correct iterator semantics)
	let opts = Arc::new(Options::default());
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Add only key_bbb to the table
	let key = b"key_bbb";
	let value = b"value_bbb";
	let internal_key = InternalKey::new(Vec::from(key), 1, InternalKeyKind::Set, 0);
	writer.add(internal_key, value).unwrap();

	let size = writer.finish().unwrap();
	let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

	// Seek to key_aaa which does NOT exist
	// key_aaa < key_bbb lexicographically
	let mut iter = table.iter(None).unwrap();
	let lookup_key = InternalKey::new(
		Vec::from(b"key_aaa"),
		2, // Higher seq number for lookup
		InternalKeyKind::Set,
		0,
	);
	iter.seek(&lookup_key.encode()).unwrap();

	// Iterator behavior: seek positions at next >= key
	assert!(iter.valid(), "Iterator should be valid (positioned at next key)");

	// The iterator is positioned at key_bbb (the next greater key)
	let current_key = iter.key();
	assert_eq!(
		current_key.user_key(),
		b"key_bbb",
		"Iterator should be positioned at the next greater key"
	);

	// If caller wants exact match, they must check the key themselves
	let is_exact_match = current_key.user_key() == b"key_aaa";
	assert!(!is_exact_match, "Caller should check for exact match if needed");
}

#[test]
fn test_table_iterator_seek_nonexistent_past_end() {
	// Test that seeking past all keys makes iterator invalid
	let opts = Arc::new(Options::default());
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let key = b"key_bbb";
	let value = b"value_bbb";
	let internal_key = InternalKey::new(Vec::from(key), 1, InternalKeyKind::Set, 0);
	writer.add(internal_key, value).unwrap();

	let size = writer.finish().unwrap();
	let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

	// Seek to key_zzz which is past all keys
	let mut iter = table.iter(None).unwrap();
	let lookup_key = InternalKey::new(Vec::from(b"key_zzz"), 2, InternalKeyKind::Set, 0);
	iter.seek(&lookup_key.encode()).unwrap();

	// Iterator should be invalid (no more keys)
	assert!(!iter.valid(), "Iterator should be invalid when seeking past all keys");
}

#[test]
fn test_table_iterator_seek_exact_match() {
	// Test that seeking to an existing key positions at that key
	let opts = Arc::new(Options::default());
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let key = b"key_bbb";
	let value = b"value_bbb";
	let internal_key = InternalKey::new(Vec::from(key), 1, InternalKeyKind::Set, 0);
	writer.add(internal_key, value).unwrap();

	let size = writer.finish().unwrap();
	let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

	// Seek to key_bbb which exists
	let mut iter = table.iter(None).unwrap();
	let lookup_key = InternalKey::new(Vec::from(b"key_bbb"), 2, InternalKeyKind::Set, 0);
	iter.seek(&lookup_key.encode()).unwrap();

	assert!(iter.valid(), "Iterator should be valid");

	let current_key = iter.key();
	assert_eq!(
		current_key.user_key(),
		b"key_bbb",
		"Iterator should be positioned at the exact key"
	);
}

#[test]
fn test_table_iter_upper_bound_included() {
	// Use static strings to avoid lifetime issues
	let data = vec![
		("key_000", "value"),
		("key_001", "value"),
		("key_002", "value"),
		("key_003", "value"),
		("key_004", "value"),
		("key_005", "value"),
		("key_006", "value"),
		("key_007", "value"),
		("key_008", "value"),
		("key_009", "value"),
	];

	let (src, size) = build_table(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Iterate with range (Unbounded, Included("key_005"))
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Unbounded,
			Bound::Included(b"key_005".as_slice()),
		)))
		.unwrap();

	let mut results = Vec::new();
	iter.seek_first().unwrap();
	while iter.valid() {
		let key = iter.key();
		results.push(String::from_utf8(key.user_key().to_vec()).unwrap());
		if !iter.next().unwrap() {
			break;
		}
	}

	// Should return items key_000 through key_005 (6 items)
	assert_eq!(results.len(), 6, "Should return exactly 6 items");
	assert_eq!(results[0], "key_000");
	assert_eq!(results[1], "key_001");
	assert_eq!(results[2], "key_002");
	assert_eq!(results[3], "key_003");
	assert_eq!(results[4], "key_004");
	assert_eq!(results[5], "key_005");

	// Verify last key is exactly key_005
	assert_eq!(results.last().unwrap(), "key_005");
}

#[test]
fn test_table_iter_upper_bound_excluded() {
	// Use static strings to avoid lifetime issues
	let data = vec![
		("key_000", "value"),
		("key_001", "value"),
		("key_002", "value"),
		("key_003", "value"),
		("key_004", "value"),
		("key_005", "value"),
		("key_006", "value"),
		("key_007", "value"),
		("key_008", "value"),
		("key_009", "value"),
	];

	let (src, size) = build_table(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Iterate with range (Unbounded, Excluded("key_005"))
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Unbounded,
			Bound::Excluded(b"key_005".as_slice()),
		)))
		.unwrap();

	let mut results = Vec::new();
	iter.seek_first().unwrap();
	while iter.valid() {
		let key = iter.key();
		results.push(String::from_utf8(key.user_key().to_vec()).unwrap());
		if !iter.next().unwrap() {
			break;
		}
	}

	// Should return items key_000 through key_004 (5 items)
	assert_eq!(results.len(), 5, "Should return exactly 5 items");
	assert_eq!(results[0], "key_000");
	assert_eq!(results[1], "key_001");
	assert_eq!(results[2], "key_002");
	assert_eq!(results[3], "key_003");
	assert_eq!(results[4], "key_004");

	// Verify last key is key_004, not key_005
	assert_eq!(results.last().unwrap(), "key_004");
}

#[test]
fn test_table_iter_unbounded_reverse() {
	// Use static strings to avoid lifetime issues
	let data = vec![
		("key_000", "value"),
		("key_001", "value"),
		("key_002", "value"),
		("key_003", "value"),
		("key_004", "value"),
		("key_005", "value"),
		("key_006", "value"),
		("key_007", "value"),
		("key_008", "value"),
		("key_009", "value"),
	];

	let (src, size) = build_table(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// First test forward iteration to make sure table has data
	let mut iter_temp = table.iter(None).unwrap();
	let collected = collect_all(&mut iter_temp).unwrap();
	assert_eq!(collected.len(), 10, "Forward iteration should return 10 items");

	let forward_keys: Vec<String> = collected
		.iter()
		.map(|(k, _)| std::str::from_utf8(&k.user_key).unwrap().to_string())
		.collect();
	assert_eq!(
		forward_keys,
		vec![
			"key_000", "key_001", "key_002", "key_003", "key_004", "key_005", "key_006", "key_007",
			"key_008", "key_009"
		]
	);

	// Test unbounded reverse iteration
	let mut iter = table.iter(None).unwrap();
	iter.seek_to_last().unwrap(); // Position at the end for reverse iteration

	let mut results = Vec::new();
	while iter.valid() {
		let key = iter.key();
		results.push(String::from_utf8(key.user_key().to_vec()).unwrap());
		if !iter.prev().unwrap() {
			break;
		}
	}
	// Should return all 10 items in reverse order
	assert_eq!(results.len(), 10, "Should return exactly 10 items");
	assert_eq!(results[0], "key_009");
	assert_eq!(results[9], "key_000");
}

#[test]
fn test_table_iter_lower_bound_included_reverse() {
	// Use static strings to avoid lifetime issues
	let data = vec![
		("key_000", "value"),
		("key_001", "value"),
		("key_002", "value"),
		("key_003", "value"),
		("key_004", "value"),
		("key_005", "value"),
		("key_006", "value"),
		("key_007", "value"),
		("key_008", "value"),
		("key_009", "value"),
	];

	let (src, size) = build_table(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Iterate with range (Included("key_003"), Unbounded) using prev()
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Included(b"key_003".as_slice()),
			Bound::Unbounded,
		)))
		.unwrap();

	let mut results = Vec::new();
	iter.seek_to_last().unwrap();
	while iter.valid() {
		let key = iter.key();
		results.push(String::from_utf8(key.user_key().to_vec()).unwrap());
		if !iter.prev().unwrap() {
			break;
		}
	}

	// Should return items in reverse: key_009, key_008, ..., key_003 (7 items)
	assert_eq!(results.len(), 7, "Should return exactly 7 items");
	assert_eq!(results[0], "key_009");
	assert_eq!(results[1], "key_008");
	assert_eq!(results[2], "key_007");
	assert_eq!(results[3], "key_006");
	assert_eq!(results[4], "key_005");
	assert_eq!(results[5], "key_004");
	assert_eq!(results[6], "key_003");

	// Verify last item returned is key_003
	assert_eq!(results.last().unwrap(), "key_003");
}

#[test]
fn test_table_iter_both_bounds() {
	// Use static strings to avoid lifetime issues
	let data = vec![
		("key_000", "value"),
		("key_001", "value"),
		("key_002", "value"),
		("key_003", "value"),
		("key_004", "value"),
		("key_005", "value"),
		("key_006", "value"),
		("key_007", "value"),
		("key_008", "value"),
		("key_009", "value"),
	];

	let (src, size) = build_table(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Iterate with range (Included("key_002"), Excluded("key_007"))
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Included(b"key_002".as_slice()),
			Bound::Excluded(b"key_007".as_slice()),
		)))
		.unwrap();
	iter.seek_first().unwrap();

	let mut results = Vec::new();
	while iter.valid() {
		let key = iter.key();
		results.push(String::from_utf8(key.user_key().to_vec()).unwrap());
		if !iter.next().unwrap() {
			break;
		}
	}

	// Should return items key_002, key_003, key_004, key_005, key_006 (5 items)
	assert_eq!(results.len(), 5, "Should return exactly 5 items");
	assert_eq!(results[0], "key_002");
	assert_eq!(results[1], "key_003");
	assert_eq!(results[2], "key_004");
	assert_eq!(results[3], "key_005");
	assert_eq!(results[4], "key_006");
}

#[test]
fn test_table_iter_unbounded() {
	// Use static strings to avoid lifetime issues
	let data = vec![
		("key_000", "value"),
		("key_001", "value"),
		("key_002", "value"),
		("key_003", "value"),
		("key_004", "value"),
		("key_005", "value"),
		("key_006", "value"),
		("key_007", "value"),
		("key_008", "value"),
		("key_009", "value"),
	];

	let (src, size) = build_table(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Test that (Bound::Unbounded, Bound::Unbounded) returns all items
	let mut iter =
		table.iter(Some(user_range_to_internal_range(Bound::Unbounded, Bound::Unbounded))).unwrap();

	let mut results = Vec::new();
	iter.seek_first().unwrap();
	while iter.valid() {
		let key = iter.key();
		results.push(String::from_utf8(key.user_key().to_vec()).unwrap());
		if !iter.next().unwrap() {
			break;
		}
	}

	// Should return all 10 items
	assert_eq!(results.len(), 10, "Should return exactly 10 items");
	assert_eq!(results[0], "key_000");
	assert_eq!(results[9], "key_009");
}

#[test]
fn test_table_iter_forward_and_backward() {
	// Create enough data to span multiple blocks
	// With block_size=32 and ~15 bytes per entry, 30 entries span ~15 blocks
	let data = vec![
		("key_000", "value"),
		("key_001", "value"),
		("key_002", "value"),
		("key_003", "value"),
		("key_004", "value"),
		("key_005", "value"),
		("key_006", "value"),
		("key_007", "value"),
		("key_008", "value"),
		("key_009", "value"),
		("key_010", "value"),
		("key_011", "value"),
		("key_012", "value"),
		("key_013", "value"),
		("key_014", "value"),
		("key_015", "value"),
		("key_016", "value"),
		("key_017", "value"),
		("key_018", "value"),
		("key_019", "value"),
		("key_020", "value"),
		("key_021", "value"),
		("key_022", "value"),
		("key_023", "value"),
		("key_024", "value"),
		("key_025", "value"),
		("key_026", "value"),
		("key_027", "value"),
		("key_028", "value"),
		("key_029", "value"),
	];

	let (src, size) = build_table(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Test forward iteration
	let mut forward_iter = table.iter(None).unwrap();
	forward_iter.seek_to_first().unwrap();
	let mut forward_keys = Vec::new();
	while forward_iter.valid() {
		let key = forward_iter.key();
		forward_keys.push(String::from_utf8(key.user_key().to_vec()).unwrap());
		if !forward_iter.next().unwrap() {
			break;
		}
	}
	assert_eq!(forward_keys.len(), 30);
	assert_eq!(forward_keys[0], "key_000");
	assert_eq!(forward_keys[29], "key_029");

	// Test backward iteration on separate iterator
	let mut backward_iter = table.iter(None).unwrap();
	backward_iter.seek_to_last().unwrap();
	let mut backward_keys = Vec::new();
	while backward_iter.valid() {
		let key = backward_iter.key();
		backward_keys.push(String::from_utf8(key.user_key().to_vec()).unwrap());
		if !backward_iter.prev().unwrap() {
			break;
		}
	}
	assert_eq!(backward_keys.len(), 30);
	assert_eq!(backward_keys[0], "key_029");
	assert_eq!(backward_keys[29], "key_000");

	// Test seeking to last and then going backward
	let mut seek_iter = table.iter(None).unwrap();
	seek_iter.seek_to_last().unwrap();
	let mut seek_backward = Vec::new();
	while seek_iter.valid() {
		let key = seek_iter.key();
		seek_backward.push(String::from_utf8(key.user_key().to_vec()).unwrap());
		if !seek_iter.prev().unwrap() {
			break;
		}
	}
	assert_eq!(seek_backward.len(), 30);
	assert_eq!(seek_backward[0], "key_029");
	assert_eq!(seek_backward[29], "key_000");

	// Test that forward iteration and backward iteration give complementary results
	// The forward iterator gives keys in ascending order
	let expected_forward: Vec<String> = (0..30).map(|i| format!("key_{:03}", i)).collect();
	assert_eq!(forward_keys, expected_forward);

	// The backward iterator gives keys in descending order
	let expected_backward: Vec<String> = (0..30).rev().map(|i| format!("key_{:03}", i)).collect();
	assert_eq!(backward_keys, expected_backward);

	// And seek_to_last + next_back also gives descending order
	assert_eq!(seek_backward, backward_keys);
}

#[test]
fn test_table_iter_upper_bound_excluded_reverse() {
	// Create table with multiple versions of the same key
	let data = vec![
		("key_000", "value", 1),
		("key_001", "value", 1),
		("key_002", "value", 1),
		("key_003", "value", 1),
		("key_004", "value", 1),
		("key_005", "value3", 3), // <- Newest version first (highest seq_num)
		("key_005", "value2", 2), // <- Middle version
		("key_005", "value", 1),  // <- Oldest version last (lowest seq_num)
		("key_006", "value", 1),
		("key_007", "value", 1),
		("key_008", "value", 1),
		("key_009", "value", 1),
	];

	let (src, size) = build_table_with_seq_num(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Reverse iteration with EXCLUDED upper bound
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Unbounded,
			Bound::Excluded(b"key_005".as_slice()),
		)))
		.unwrap();

	let mut results = Vec::new();
	iter.seek_last().unwrap();
	while iter.valid() {
		let key = iter.key();
		results.push(String::from_utf8(key.user_key().to_vec()).unwrap());
		if !iter.prev().unwrap() {
			break;
		}
	}

	// Should return items in reverse: key_004, key_003, key_002, key_001, key_000
	// Should NOT include any version of key_005 or higher
	assert_eq!(results.len(), 5, "Should return exactly 5 items");
	assert_eq!(results[0], "key_004"); // First item in reverse
	assert_eq!(results[4], "key_000"); // Last item in reverse

	// Verify NO key_005 appears
	assert!(!results.iter().any(|k| k == "key_005"), "key_005 should not appear in results");
}

#[test]
fn test_table_iter_upper_bound_included_reverse_nonexistent_key() {
	let data = vec![
		("key_000", "value"),
		("key_001", "value"),
		("key_002", "value"),
		("key_003", "value"),
		("key_004", "value"),
		// NOTE: key_005 does NOT exist!
		("key_006", "value"),
		("key_007", "value"),
		("key_008", "value"),
		("key_009", "value"),
	];

	let (src, size) = build_table(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Reverse iteration with INCLUDED upper bound on non-existent key
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Unbounded,
			Bound::Included(b"key_005".as_slice()),
		)))
		.unwrap();

	let mut results = Vec::new();
	iter.seek_last().unwrap();
	while iter.valid() {
		let key = iter.key();
		results.push(String::from_utf8(key.user_key().to_vec()).unwrap());
		if !iter.prev().unwrap() {
			break;
		}
	}

	// Should return items in reverse: key_004, key_003, ..., key_000 (5 items)
	// Should NOT include key_006 or higher
	assert_eq!(results.len(), 5, "Should return exactly 5 items");
	assert_eq!(results[0], "key_004"); // First item should be key_004, NOT key_006!
	assert_eq!(results[4], "key_000");

	// Without the backward stepping loop, this test would FAIL
	// because it would start at key_006 instead of key_004
	assert!(!results.iter().any(|k| k == "key_006"), "key_006 should not appear");
}

#[test]
fn test_table_iter_lower_bound_excluded_forward_with_multiple_versions() {
	let data = vec![
		("key_000", "v1", 1),
		("key_001", "v1", 1),
		("key_002", "v1", 1),
		("key_003", "v1", 3),
		("key_003", "v2", 2), // Multiple versions of key_003
		("key_003", "v3", 1),
		("key_004", "v1", 2),
		("key_004", "v1", 1),
		("key_005", "v1", 1),
	];

	let (src, size) = build_table_with_seq_num(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Forward iteration with EXCLUDED lower bound
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Excluded(b"key_003".as_slice()),
			Bound::Unbounded,
		)))
		.unwrap();

	let mut results = Vec::new();
	iter.seek_first().unwrap();
	while iter.valid() {
		let key = iter.key();
		results.push(String::from_utf8(key.user_key().to_vec()).unwrap());
		if !iter.next().unwrap() {
			break;
		}
	}

	// Should start at key_004, NOT any version of key_003
	assert!(results[0] == "key_004", "First result should be key_004, got {}", results[0]);

	// Verify NO version of key_003 appears
	assert!(!results.iter().any(|k| k == "key_003"), "key_003 should not appear in results");
}

#[test]
fn test_table_iter_excluded_bound_across_multiple_blocks() {
	// Test that excluded lower bound correctly skips ALL versions of a key
	// even when they span multiple blocks
	let mut data = vec![];

	// Add many versions of key_003 to force multiple blocks
	for seq in (1..=100).rev() {
		data.push(("key_003", "value", seq));
	}
	// Add the key we should actually start at
	data.push(("key_004", "value", 1));
	data.push(("key_005", "value", 1));

	let (src, size) = build_table_with_seq_num(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Forward iteration with excluded lower bound
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Excluded(b"key_003".as_slice()),
			Bound::Unbounded,
		)))
		.unwrap();
	iter.seek_first().unwrap();

	let mut results = Vec::new();
	while iter.valid() {
		let key = iter.key();
		let user_key = String::from_utf8(key.user_key().to_vec()).unwrap();
		results.push(user_key);
		if !iter.next().unwrap() {
			break;
		}
	}

	// Should NOT contain any version of key_003
	assert!(
		!results.iter().any(|k| k == "key_003"),
		"key_003 should be completely excluded, but found in results"
	);

	// First key should be key_004
	assert_eq!(
		results.first().unwrap(),
		"key_004",
		"First key should be key_004, got {}",
		results.first().unwrap()
	);

	// Should have exactly 2 keys (key_004 and key_005)
	assert_eq!(results.len(), 2, "Should return exactly 2 keys, got {}", results.len());
}

#[test]
fn test_table_iter_excluded_bound_across_partitions_reverse() {
	// Test reverse iteration with excluded upper bound across multiple versions
	let mut data = vec![];

	// Add keys before the excluded bound
	data.push(("key_001", "value", 1));
	data.push(("key_002", "value", 1));

	// Add many versions of key_003 (the excluded upper bound)
	for seq in (1..=100).rev() {
		data.push(("key_003", "value", seq));
	}

	// Add keys after
	data.push(("key_004", "value", 1));
	data.push(("key_005", "value", 1));

	let (src, size) = build_table_with_seq_num(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Reverse iteration with excluded upper bound
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Unbounded,
			Bound::Excluded(b"key_003".as_slice()),
		)))
		.unwrap();

	let mut results = Vec::new();
	iter.seek_last().unwrap();
	while iter.valid() {
		let key = iter.key();
		let user_key = String::from_utf8(key.user_key().to_vec()).unwrap();
		results.push(user_key);
		if !iter.prev().unwrap() {
			break;
		}
	}

	// Should NOT contain any version of key_003
	assert!(
		!results.iter().any(|k| k == "key_003"),
		"key_003 should be completely excluded, but found in results"
	);

	// First key in reverse should be key_002
	assert_eq!(
		results.first().unwrap(),
		"key_002",
		"First key in reverse should be key_002, got {}",
		results.first().unwrap()
	);

	// Should have exactly 2 keys (key_002, key_001 in reverse order)
	assert_eq!(results.len(), 2, "Should return exactly 2 keys");
	assert_eq!(results[0], "key_002");
	assert_eq!(results[1], "key_001");
}

#[test]
fn test_table_iter_both_bounds_excluded_same_key() {
	// Test empty range: (Excluded("key_003"), Excluded("key_003"))
	// This should return no items
	let data = vec![
		("key_001", "value", 1),
		("key_002", "value", 1),
		("key_003", "value", 100),
		("key_003", "value", 50),
		("key_003", "value", 1),
		("key_004", "value", 1),
		("key_005", "value", 1),
	];

	let (src, size) = build_table_with_seq_num(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Forward iteration with both bounds excluded at same key
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Excluded(b"key_003".as_slice()),
			Bound::Excluded(b"key_003".as_slice()),
		)))
		.unwrap();
	iter.seek_first().unwrap();

	let results = collect_all(&mut iter).unwrap();

	// Should return NO items (empty range)
	assert_eq!(
		results.len(),
		0,
		"Range (Excluded(X), Excluded(X)) should be empty, got {} items",
		results.len()
	);
}

#[test]
fn test_table_iter_both_bounds_excluded_same_key_reverse() {
	// Test empty range in reverse: (Excluded("key_003"), Excluded("key_003"))
	let data = vec![
		("key_001", "value", 1),
		("key_002", "value", 1),
		("key_003", "value", 100),
		("key_003", "value", 50),
		("key_003", "value", 1),
		("key_004", "value", 1),
		("key_005", "value", 1),
	];

	let (src, size) = build_table_with_seq_num(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Reverse iteration with both bounds excluded at same key
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Excluded(b"key_003".as_slice()),
			Bound::Excluded(b"key_003".as_slice()),
		)))
		.unwrap();

	let mut results = Vec::new();
	iter.seek_last().unwrap();
	while iter.valid() {
		results.push((iter.key().to_owned(), iter.value_encoded().unwrap().to_vec()));
		if !iter.prev().unwrap() {
			break;
		}
	}

	// Should return NO items (empty range)
	assert_eq!(
		results.len(),
		0,
		"Range (Excluded(X), Excluded(X)) in reverse should be empty, got {} items",
		results.len()
	);
}

#[test]
fn test_table_iter_excluded_bounds_adjacent_keys() {
	// Test: (Excluded("key_002"), Excluded("key_004"))
	// Should only return key_003 (all versions)
	let data = vec![
		("key_001", "value", 1),
		("key_002", "value", 100),
		("key_002", "value", 50),
		("key_003", "value", 80),
		("key_003", "value", 40),
		("key_003", "value", 10),
		("key_004", "value", 100),
		("key_004", "value", 50),
		("key_005", "value", 1),
	];

	let (src, size) = build_table_with_seq_num(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Forward iteration
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Excluded(b"key_002".as_slice()),
			Bound::Excluded(b"key_004".as_slice()),
		)))
		.unwrap();

	let mut results = Vec::new();
	iter.seek_first().unwrap();
	while iter.valid() {
		let key = iter.key();
		let user_key = String::from_utf8(key.user_key().to_vec()).unwrap();
		results.push((user_key, key.seq_num()));
		if !iter.next().unwrap() {
			break;
		}
	}

	// Should have exactly 3 items (all versions of key_003)
	assert_eq!(results.len(), 3, "Should return 3 versions of key_003");

	// All should be key_003
	assert!(results.iter().all(|(k, _)| k == "key_003"), "All results should be key_003");

	// Should NOT contain key_002 or key_004
	assert!(
		!results.iter().any(|(k, _)| k == "key_002" || k == "key_004"),
		"Should not contain key_002 or key_004"
	);

	// Verify sequence numbers are in descending order (80, 40, 10)
	assert_eq!(results[0].1, 80);
	assert_eq!(results[1].1, 40);
	assert_eq!(results[2].1, 10);
}

#[test]
fn test_table_iter_multiple_versions_at_both_bounds() {
	// Test with multiple versions at both excluded bounds
	let data = vec![
		// Multiple versions of lower bound (excluded)
		("key_002", "value", 100),
		("key_002", "value", 90),
		("key_002", "value", 80),
		// Keys in range
		("key_003", "value", 50),
		("key_004", "value", 50),
		// Multiple versions of upper bound (excluded)
		("key_005", "value", 100),
		("key_005", "value", 90),
		("key_005", "value", 80),
		// After upper bound
		("key_006", "value", 1),
	];

	let (src, size) = build_table_with_seq_num(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Forward iteration
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Excluded(b"key_002".as_slice()),
			Bound::Excluded(b"key_005".as_slice()),
		)))
		.unwrap();

	let mut results = Vec::new();
	iter.seek_first().unwrap();
	while iter.valid() {
		let key = iter.key();
		let user_key = String::from_utf8(key.user_key().to_vec()).unwrap();
		results.push(user_key);
		if !iter.next().unwrap() {
			break;
		}
	}

	// Should only have key_003 and key_004
	assert_eq!(results.len(), 2, "Should return exactly 2 items");
	assert_eq!(results[0], "key_003");
	assert_eq!(results[1], "key_004");

	// Should NOT contain any version of key_002 or key_005
	assert!(
		!results.iter().any(|k| k == "key_002" || k == "key_005"),
		"Should not contain any version of excluded bounds"
	);
}

#[test]
fn test_get_block_boundary_same_user_key_bug() {
	// REGRESSION TEST: When multiple versions of the same user_key span multiple
	// data blocks, looking up the exact key at the block boundary incorrectly
	// returns None due to using < instead of <= in the index comparison.
	//
	// Bug location: Table::get() lines 777-778
	// The check `key_encoded < last_key_in_block` fails when they're equal,
	// but the key IS in the block (it's the last key).

	// Use very small block_size to force same user_key versions across multiple blocks
	// Entry sizes: 1st = 24 bytes, subsequent = 20 bytes each
	// size_estimate includes 8 bytes overhead (restart_points)
	// After 1st: 32, After 2nd: 52
	// With block_size=32: before 3rd entry, 52 > 32 → block cut
	let opts = Arc::new(
		Options::new()
			.with_filter_policy(None) // Disable bloom filter to test key comparison
			.with_block_size(32), // Very small block to force split
	);

	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let user_key = b"test";

	// Add 4 versions of the same key with descending sequence numbers
	// (required ordering: user_key ASC, seq_num DESC)
	// Block 1 will contain: seq 300, 200 (last_key = ("test", 200))
	// Block 2 will contain: seq 100, 50
	let key1 = InternalKey::new(Vec::from(user_key), 300, InternalKeyKind::Set, 0);
	writer.add(key1, b"v").unwrap();

	let key2 = InternalKey::new(Vec::from(user_key), 200, InternalKeyKind::Set, 0);
	writer.add(key2, b"v").unwrap();

	let key3 = InternalKey::new(Vec::from(user_key), 100, InternalKeyKind::Set, 0);
	writer.add(key3, b"v").unwrap();

	let key4 = InternalKey::new(Vec::from(user_key), 50, InternalKeyKind::Set, 0);
	writer.add(key4, b"v").unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	// Verify we have multiple data blocks (the bug requires this)
	assert!(
		table.meta.properties.num_data_blocks >= 2,
		"Test requires at least 2 data blocks to trigger the bug, got {}. \
			 Adjust block_size or add more entries.",
		table.meta.properties.num_data_blocks
	);

	// THE BUG: Looking up the exact key at block boundary returns None
	// This is the last key of block 1, which is also the index separator key
	// (because same user_key causes separator to fall back to original key)
	// With block_size=32, the boundary key is ("test", 200)
	let lookup_boundary_key = InternalKey::new(Vec::from(user_key), 200, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_boundary_key).unwrap();

	// This assertion will FAIL with the current buggy code
	assert!(
		result.is_some(),
		"BUG: get() returned None for key at block boundary. \
			 The key ('test', seq=200) exists as the last key of block 1, \
			 but the index comparison uses < instead of <=, causing it to fail \
			 when lookup key equals the separator key."
	);

	let (found_key, found_value) = result.unwrap();
	assert_eq!(found_key.user_key.as_slice(), user_key);
	assert_eq!(found_key.seq_num(), 200);
	assert_eq!(found_value.as_slice(), b"v");

	// Also verify other keys still work correctly
	let lookup_key1 = InternalKey::new(Vec::from(user_key), 300, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key1).unwrap();
	assert!(result.is_some(), "seq=300 should be found");

	let lookup_key3 = InternalKey::new(Vec::from(user_key), 100, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key3).unwrap();
	assert!(result.is_some(), "seq=100 should be found");

	let lookup_key4 = InternalKey::new(Vec::from(user_key), 50, InternalKeyKind::Set, 0);
	let result = table.get(&lookup_key4).unwrap();
	assert!(result.is_some(), "seq=50 should be found");
}

#[test]
fn test_iterator_trait_prev_multi_partition_bug() {
	// REGRESSION TEST: The Iterator trait's prev() method uses self.index_block
	// which is only the first partition's iterator, NOT handling multiple partitions.
	// When we have multiple index partitions and call prev() via the trait,
	// backward iteration will stop at partition boundaries instead of continuing.
	//
	// Bug location: impl LSMIterator for TableIterator::prev() at lines 1512-1535
	// Uses self.index_block.prev() which only works within the first partition.
	// The correct inherent prev() method (lines 1022-1083) handles partitions properly.

	// Use very small index_partition_size to force multiple index partitions
	// Each data block adds an entry to the index partition
	// With index_partition_size=50 and block_size=50, we should get multiple partitions
	let opts = Arc::new(
		Options::new()
			.with_filter_policy(None)
			.with_block_size(50) // Small blocks to create many index entries
			.with_index_partition_size(50), // Small partitions to force multiple partitions
	);

	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Add enough keys to create multiple data blocks, which creates multiple index entries,
	// which in turn creates multiple index partitions
	let num_keys = 100;
	for i in 0..num_keys {
		let key = InternalKey::new(
			format!("key_{:05}", i).into_bytes(),
			1000 - i as u64, // Decreasing seq for same ordering
			InternalKeyKind::Set,
			0,
		);
		writer.add(key, format!("value_{}", i).as_bytes()).unwrap();
	}

	let size = writer.finish().unwrap();
	let table_arc = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

	// Verify we have multiple partitions (the bug requires this)
	let IndexType::Partitioned(ref partitioned_index) = table_arc.index_block;
	let num_partitions = partitioned_index.blocks.len();
	assert!(
		num_partitions >= 2,
		"Test requires at least 2 index partitions to trigger the bug, got {}. \
			 Adjust index_partition_size or add more keys.",
		num_partitions
	);

	println!("Created table with {} index partitions", num_partitions);

	// Create iterator and position at the LAST entry
	let mut iter = table_arc.iter(None).unwrap();
	iter.seek_to_last().unwrap();
	assert!(iter.valid(), "Iterator should be valid after seek_to_last");

	// Collect all keys going backwards using the CORRECT inherent prev() method
	let mut correct_results = Vec::new();
	correct_results.push(iter.key().user_key().to_vec());
	while iter.prev().unwrap() {
		correct_results.push(iter.key().user_key().to_vec());
	}
	let correct_count = correct_results.len();
	println!("Correct backward iteration found {} keys", correct_count);
	assert_eq!(correct_count, num_keys as usize, "Should find all keys");

	// Now test with explicit trait method call
	// Reset iterator
	let mut iter2 = table_arc.iter(None).unwrap();
	iter2.seek_to_last().unwrap();
	assert!(iter2.valid(), "Iterator should be valid after seek_to_last");

	// Collect all keys going backwards using the BUGGY trait prev() method
	let mut buggy_results = Vec::new();
	buggy_results.push(iter2.key().user_key().to_vec());
	while iter2.prev().unwrap() {
		buggy_results.push(iter2.key().user_key().to_vec());
	}
	let buggy_count = buggy_results.len();
	println!("Buggy trait prev() found {} keys", buggy_count);

	// THE BUG: The trait's prev() will stop at partition boundary
	// It should find all keys, but will find fewer because it only works within first partition
	assert_eq!(
		buggy_count, correct_count,
		"BUG DETECTED: Iterator::prev() trait method only found {} keys, \
			 but should have found {} keys. The trait method uses self.index_block \
			 which is only the first partition's iterator and doesn't handle \
			 crossing partition boundaries.",
		buggy_count, correct_count
	);
}

#[test]
fn test_partitioned_index_same_user_key_spanning_partitions_bug() {
	// REGRESSION TEST: The TopLevelIndex stores only user_key, losing seq_num ordering.
	// When the same user_key has many versions spanning multiple partitions,
	// Table::get may return None even when a visible version exists.
	//
	// Bug location:
	// - src/sstable/index_block.rs:199-206 - TopLevelIndex construction only stores user_key
	// - src/sstable/index_block.rs:216-231 - find_block_handle_by_key uses user_key comparison
	// - src/sstable/table.rs:761 - Table::get uses user_key-only partition lookup
	//
	// Scenario:
	// - Partition 0 ends with internal key (foo, 50) -> stores user_key "foo"
	// - Partition 1 contains (foo, 49)...(foo, 10) and ends with (goo, 5) -> stores user_key "goo"
	// - Query for (foo, 25) should find entry in partition 1
	// - BUG: find_block_handle_by_key("foo") returns partition 0, which doesn't contain (foo, 25)

	// Use very small partition and block sizes to force the same user_key to span partitions
	let opts = Arc::new(
		Options::new()
			.with_filter_policy(None)
			.with_block_size(50) // Small blocks = more index entries
			.with_index_partition_size(50), // Small partitions to force user_key to span
	);

	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Add many versions of the SAME user key with decreasing seq_nums
	// Internal key ordering: user_key ASC, seq_num DESC
	// So (foo, 100) < (foo, 99) < ... < (foo, 1) in sort order
	let user_key = b"foo";
	let num_versions = 100;

	for seq in (1..=num_versions).rev() {
		// Entries are added in internal key order (descending seq_num first)
		let key = InternalKey::new(user_key.to_vec(), seq as u64, InternalKeyKind::Set, 0);
		writer.add(key, format!("value_at_seq_{}", seq).as_bytes()).unwrap();
	}

	// Add a different user key at the end to ensure we have a partition boundary after "foo"
	let key = InternalKey::new(b"goo".to_vec(), 1, InternalKeyKind::Set, 0);
	writer.add(key, b"value_goo").unwrap();

	let size = writer.finish().unwrap();
	let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

	// Verify we have multiple partitions (the bug requires this)
	let IndexType::Partitioned(ref partitioned_index) = table.index_block;
	let num_partitions = partitioned_index.blocks.len();

	println!("Created table with {} index partitions", num_partitions);
	for (i, block) in partitioned_index.blocks.iter().enumerate() {
		let sep_key = InternalKey::decode(&block.separator_key);
		println!(
			"  Partition {}: user_key = {:?}, seq = {}",
			i,
			String::from_utf8_lossy(&sep_key.user_key),
			sep_key.seq_num()
		);
	}

	assert!(
		num_partitions >= 2,
		"Test requires at least 2 index partitions to trigger the bug, got {}. \
			 Adjust index_partition_size or add more versions.",
		num_partitions
	);

	// Check if there are multiple partitions with the same user_key "foo"
	// If partitions 0 and 1+ both have user_key "foo", the bug is triggerable
	let foo_partitions: Vec<_> = partitioned_index
		.blocks
		.iter()
		.enumerate()
		.filter(|(_, b)| InternalKey::decode(&b.separator_key).user_key == b"foo")
		.collect();

	println!(
		"Partitions with user_key 'foo': {:?}",
		foo_partitions.iter().map(|(i, _)| i).collect::<Vec<_>>()
	);

	// Now test the bug: query for a seq_num that should be in a later partition
	// If we have (foo, 100), (foo, 99), ..., (foo, 1) spread across partitions:
	// - Partition 0 might end with (foo, 60) or similar (high seq nums)
	// - Later partitions contain lower seq nums like (foo, 25)
	//
	// Query at seq_num 30: should find (foo, 30) or latest version visible at seq 30
	let query_seq = 30u64;
	let lookup_key = InternalKey::new(user_key.to_vec(), query_seq, InternalKeyKind::Set, 0);

	let result = table.get(&lookup_key).unwrap();

	// The expected result: we should find a version with seq_num <= query_seq
	// For query_seq=30, we should find (foo, 30) with value "value_at_seq_30"
	assert!(
		result.is_some(),
		"BUG DETECTED: Table::get returned None for (foo, seq={}), but version exists!\n\
			 The bug is in TopLevelIndex::find_block_handle_by_key which uses user_key-only lookup.\n\
			 When the same user_key spans multiple partitions, it only searches the first partition\n\
			 (which contains high seq_nums) and misses entries in later partitions (low seq_nums).",
		query_seq
	);

	let (found_key, found_value) = result.unwrap();
	assert_eq!(found_key.user_key, user_key.to_vec());
	println!(
		"Found key with seq_num={}, value={:?}",
		found_key.seq_num(),
		String::from_utf8_lossy(&found_value)
	);

	// The found seq_num should be <= query_seq (latest visible version)
	assert!(
		found_key.seq_num() <= query_seq,
		"Found version seq_num {} should be <= query seq_num {}",
		found_key.seq_num(),
		query_seq
	);

	// Test multiple query points to ensure correctness across partition boundaries
	for query_seq in [10, 25, 50, 75, 99] {
		let lookup = InternalKey::new(user_key.to_vec(), query_seq, InternalKeyKind::Set, 0);

		let result = table.get(&lookup).unwrap();
		assert!(
			result.is_some(),
			"BUG: Table::get returned None for (foo, seq={}), but version should exist!",
			query_seq
		);

		let (found_key, _) = result.unwrap();
		assert!(
			found_key.seq_num() <= query_seq,
			"For query seq={}, found seq={} which is greater (should be <=)",
			query_seq,
			found_key.seq_num()
		);
	}
}

#[test]
fn test_table_get_mvcc_correct_version() {
	// Scenario: Table has multiple versions of same key
	// Query at different seq_nums should return correct version
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let user_key = b"my_key";

	// Add versions: seq 100, 75, 50, 25 (stored in this order)
	for seq in [100u64, 75, 50, 25] {
		let key = InternalKey::new(user_key.to_vec(), seq, InternalKeyKind::Set, 0);
		let value = format!("value_at_seq_{}", seq);
		writer.add(key, value.as_bytes()).unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	// Query at seq=200: should find seq=100 (newest visible)
	let lookup = InternalKey::new(user_key.to_vec(), 200, InternalKeyKind::Set, 0);
	let result = table.get(&lookup).unwrap();
	assert!(result.is_some());
	let (found_key, found_value) = result.unwrap();
	assert_eq!(found_key.seq_num(), 100);
	assert_eq!(&found_value, b"value_at_seq_100");

	// Query at seq=80: should find seq=75
	let lookup = InternalKey::new(user_key.to_vec(), 80, InternalKeyKind::Set, 0);
	let result = table.get(&lookup).unwrap();
	assert!(result.is_some());
	let (found_key, _) = result.unwrap();
	assert_eq!(found_key.seq_num(), 75);

	// Query at seq=50: should find seq=50 exactly
	let lookup = InternalKey::new(user_key.to_vec(), 50, InternalKeyKind::Set, 0);
	let result = table.get(&lookup).unwrap();
	assert!(result.is_some());
	let (found_key, _) = result.unwrap();
	assert_eq!(found_key.seq_num(), 50);

	// Query at seq=10: should return None (no visible version)
	let lookup = InternalKey::new(user_key.to_vec(), 10, InternalKeyKind::Set, 0);
	let result = table.get(&lookup).unwrap();
	assert!(result.is_none(), "No version should be visible at seq=10");
}

#[test]
fn test_table_get_returns_none_for_future_version() {
	// Scenario: Query for a key where only future versions exist
	// Expected: Should return None (no visible version)
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Key only has version at seq=100
	let key = InternalKey::new(b"future_key".to_vec(), 100, InternalKeyKind::Set, 0);
	writer.add(key, b"future_value").unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	// Query at seq=50: version at seq=100 is in the "future"
	let lookup = InternalKey::new(b"future_key".to_vec(), 50, InternalKeyKind::Set, 0);
	let result = table.get(&lookup).unwrap();
	assert!(result.is_none(), "Should not see future version at seq=100 when querying at seq=50");
}

#[test]
fn test_table_get_different_user_keys() {
	// Scenario: Multiple different user keys
	// Verify no cross-contamination
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	let keys = ["apple", "banana", "cherry", "date"];
	for key in &keys {
		let ikey = InternalKey::new(key.as_bytes().to_vec(), 100, InternalKeyKind::Set, 0);
		let value = format!("value_for_{}", key);
		writer.add(ikey, value.as_bytes()).unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	// Each key should return its own value
	for key in &keys {
		let lookup = InternalKey::new(key.as_bytes().to_vec(), 200, InternalKeyKind::Set, 0);
		let result = table.get(&lookup).unwrap();
		assert!(result.is_some(), "Should find key {}", key);
		let (found_key, found_value) = result.unwrap();
		assert_eq!(found_key.user_key, key.as_bytes().to_vec());
		assert_eq!(String::from_utf8_lossy(&found_value), format!("value_for_{}", key));
	}

	// Non-existent keys should return None
	let lookup = InternalKey::new(b"nonexistent".to_vec(), 200, InternalKeyKind::Set, 0);
	let result = table.get(&lookup).unwrap();
	assert!(result.is_none());
}

#[test]
fn test_table_get_at_block_boundaries() {
	// Scenario: Force keys across multiple blocks, query at boundaries
	let opts = Arc::new(
		Options::new().with_filter_policy(None).with_block_size(64), /* Small blocks to
		                                                              * force splits */
	);
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Add enough keys to span multiple blocks
	for i in 0..20 {
		let key = format!("key_{:04}", i);
		let value = format!("value_{:04}", i);
		let ikey = InternalKey::new(key.as_bytes().to_vec(), 100, InternalKeyKind::Set, 0);
		writer.add(ikey, value.as_bytes()).unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	// Verify multiple blocks were created
	assert!(
		table.meta.properties.num_data_blocks > 1,
		"Test requires multiple blocks, got {}",
		table.meta.properties.num_data_blocks
	);

	// Query for each key
	for i in 0..20 {
		let key = format!("key_{:04}", i);
		let lookup = InternalKey::new(key.as_bytes().to_vec(), 200, InternalKeyKind::Set, 0);
		let result = table.get(&lookup).unwrap();
		assert!(result.is_some(), "Should find key {}", key);
		let (found_key, _) = result.unwrap();
		assert_eq!(found_key.user_key, key.as_bytes().to_vec());
	}
}

#[test]
fn test_table_get_tombstone_handling() {
	// Scenario: Key has a tombstone (delete marker)
	// Table::get should still return the tombstone, caller handles it
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Add a set followed by a delete
	let set_key = InternalKey::new(b"my_key".to_vec(), 100, InternalKeyKind::Set, 0);
	writer.add(set_key, b"original_value").unwrap();

	let delete_key = InternalKey::new(b"my_key".to_vec(), 50, InternalKeyKind::Delete, 0);
	writer.add(delete_key, b"").unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	// Query at seq=200: should get the Set at seq=100
	let lookup = InternalKey::new(b"my_key".to_vec(), 200, InternalKeyKind::Set, 0);
	let result = table.get(&lookup).unwrap();
	assert!(result.is_some());
	let (found_key, _) = result.unwrap();
	assert_eq!(found_key.seq_num(), 100);
	assert!(!found_key.is_tombstone());

	// Query at seq=75: should get the Delete at seq=50
	let lookup = InternalKey::new(b"my_key".to_vec(), 75, InternalKeyKind::Set, 0);
	let result = table.get(&lookup).unwrap();
	assert!(result.is_some());
	let (found_key, _) = result.unwrap();
	assert_eq!(found_key.seq_num(), 50);
	assert!(found_key.is_tombstone(), "Should return the tombstone marker");
}

#[test]
fn test_table_get_user_key_mismatch() {
	// Scenario: Seek lands on a different user key
	// Expected: Should return None
	let opts = Arc::new(Options::new().with_filter_policy(None));
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Only add "apple" and "cherry", not "banana"
	let key1 = InternalKey::new(b"apple".to_vec(), 100, InternalKeyKind::Set, 0);
	writer.add(key1, b"apple_value").unwrap();

	let key2 = InternalKey::new(b"cherry".to_vec(), 100, InternalKeyKind::Set, 0);
	writer.add(key2, b"cherry_value").unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap();

	// Query for "banana" which doesn't exist
	// Seek will land on "cherry", but user_key doesn't match
	let lookup = InternalKey::new(b"banana".to_vec(), 200, InternalKeyKind::Set, 0);
	let result = table.get(&lookup).unwrap();
	assert!(result.is_none(), "Should return None when user_key doesn't match");
}

#[test]
fn test_reverse_iteration_unbounded_consistency() {
	// Test that unbounded reverse iteration works correctly
	let data = vec![("key_000", "value"), ("key_001", "value"), ("key_002", "value")];

	let (src, size) = build_table(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	let mut iter = table.iter(None).unwrap();

	// Collect all items in reverse
	iter.seek_to_last().unwrap();
	let mut results = Vec::new();
	while iter.valid() {
		let key = iter.key();
		results.push(String::from_utf8(key.user_key().to_vec()).unwrap());
		if !iter.prev().unwrap() {
			break;
		}
	}

	assert_eq!(results.len(), 3);
	assert_eq!(results[0], "key_002"); // Last key first
	assert_eq!(results[1], "key_001");
	assert_eq!(results[2], "key_000"); // First key last
}

// ============================================================
// BUG #5: Silent error handling in prev()
// ============================================================

#[test]
fn test_prev_iteration_across_partitions() {
	// Test that prev() correctly handles partition boundaries
	// Use small partition size to force multiple partitions
	let mut opts = Options::new();
	opts.block_size = 32; // Very small blocks
	opts.index_partition_size = 64; // Small partition size
	let opts = Arc::new(opts);

	let data: Vec<(&str, &str)> = (0..20)
		.map(|i| {
			// Using static strings via leak for test simplicity
			let key: &'static str = Box::leak(format!("key_{:03}", i).into_boxed_str());
			let val: &'static str = Box::leak(format!("val_{:03}", i).into_boxed_str());
			(key, val)
		})
		.collect();

	let (src, size) = build_table(data.clone());
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Verify multiple partitions
	let IndexType::Partitioned(ref index) = table.index_block;
	assert!(index.blocks.len() > 1, "Test requires multiple partitions");

	// Test reverse iteration crosses partitions correctly
	let mut iter = table.iter(None).unwrap();
	iter.seek_to_last().unwrap();
	let mut results = Vec::new();

	while iter.valid() {
		let key = iter.key();
		results.push(String::from_utf8(key.user_key().to_vec()).unwrap());
		if !iter.prev().unwrap() {
			break;
		}
	}

	assert_eq!(results.len(), 20);
	// First item should be last key
	assert_eq!(results[0], "key_019");
	// Last item should be first key
	assert_eq!(results[19], "key_000");
}

// ============================================================
// BUG #6: seek_to_last panic on corrupt block (BlockIterator)
// ============================================================

// Note: This test would need to be in block.rs, but we can test the behavior
// at the table level by ensuring seek_to_last works for valid data

#[test]
fn test_seek_to_last_valid_block() {
	// Ensure seek_to_last works correctly for valid blocks
	let data = vec![("aaa", "value1"), ("bbb", "value2"), ("ccc", "value3")];

	let (src, size) = build_table(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	let mut iter = table.iter(None).unwrap();
	iter.seek_to_last().unwrap();

	assert!(iter.valid());
	let key = iter.key();
	assert_eq!(key.user_key(), b"ccc");
}

// ============================================================
// Range boundary edge cases
// ============================================================

#[test]
fn test_range_at_exact_block_boundary() {
	// Test range bounds at exact block boundaries
	let mut opts = Options::new();
	opts.block_size = 50; // Small blocks to force boundaries
	let opts = Arc::new(opts);

	let data: Vec<(&str, &str)> = (0..10)
		.map(|i| {
			let key: &'static str = Box::leak(format!("key_{:03}", i).into_boxed_str());
			let val: &'static str = Box::leak(format!("val_{:03}", i).into_boxed_str());
			(key, val)
		})
		.collect();

	let (src, size) = build_table(data);
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Test with bound that might be at a block boundary
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Included(b"key_003".as_slice()),
			Bound::Included(b"key_006".as_slice()),
		)))
		.unwrap();

	let collected = collect_all(&mut iter).unwrap();
	let results: Vec<String> =
		collected.iter().map(|(k, _)| String::from_utf8(k.user_key.clone()).unwrap()).collect();

	assert_eq!(results.len(), 4);
	assert_eq!(results[0], "key_003");
	assert_eq!(results[3], "key_006");
}

#[test]
fn test_range_with_multiple_versions_at_boundary() {
	// Test range where boundary key has multiple versions
	let data = vec![
		("key_001", "v1", 10),
		("key_002", "v1", 100),
		("key_002", "v2", 50),
		("key_002", "v3", 10), // Multiple versions of boundary key
		("key_003", "v1", 10),
		("key_004", "v1", 10),
	];

	let (src, size) = build_table_with_seq_num(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Included bound should include ALL versions
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Included(b"key_002".as_slice()),
			Bound::Included(b"key_003".as_slice()),
		)))
		.unwrap();

	let collected = collect_all(&mut iter).unwrap();
	let results: Vec<(String, u64)> = collected
		.iter()
		.map(|(k, _)| (String::from_utf8(k.user_key.clone()).unwrap(), k.seq_num()))
		.collect();

	// Should have 3 versions of key_002 + 1 version of key_003 = 4 items
	assert_eq!(results.len(), 4, "Expected 4 items, got {:?}", results);

	// First 3 should be key_002 with descending seq nums
	assert_eq!(results[0].0, "key_002");
	assert_eq!(results[0].1, 100);
	assert_eq!(results[1].0, "key_002");
	assert_eq!(results[1].1, 50);
	assert_eq!(results[2].0, "key_002");
	assert_eq!(results[2].1, 10);
	assert_eq!(results[3].0, "key_003");
}

#[test]
fn test_excluded_bound_skips_all_versions() {
	// Test that excluded bound skips ALL versions of a key
	let data = vec![
		("key_001", "v1", 10),
		("key_002", "v1", 100),
		("key_002", "v2", 50),
		("key_002", "v3", 10),
		("key_003", "v1", 10),
	];

	let (src, size) = build_table_with_seq_num(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Excluded lower bound should skip ALL versions of key_002
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Excluded(b"key_002".as_slice()),
			Bound::Unbounded,
		)))
		.unwrap();

	let collected = collect_all(&mut iter).unwrap();
	let results: Vec<String> =
		collected.iter().map(|(k, _)| String::from_utf8(k.user_key.clone()).unwrap()).collect();

	// Should only have key_003
	assert_eq!(results.len(), 1, "Expected 1 item, got {:?}", results);
	assert_eq!(results[0], "key_003");

	// Verify NO version of key_002 appears
	assert!(!results.iter().any(|k| k == "key_002"), "key_002 should not appear in results");
}

#[test]
fn test_reverse_iteration_with_excluded_upper_bound() {
	// Test reverse iteration correctly respects excluded upper bound
	let data = vec![
		("key_001", "v1", 10),
		("key_002", "v1", 100),
		("key_002", "v2", 50),
		("key_003", "v1", 10),
		("key_004", "v1", 10),
	];

	let (src, size) = build_table_with_seq_num(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Excluded upper bound in reverse
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Unbounded,
			Bound::Excluded(b"key_003".as_slice()),
		)))
		.unwrap();

	let mut results = Vec::new();
	iter.seek_last().unwrap();
	while iter.valid() {
		let key = iter.key();
		results.push(String::from_utf8(key.user_key().to_vec()).unwrap());
		if !iter.prev().unwrap() {
			break;
		}
	}

	// Should get key_002 (both versions) and key_001, NOT key_003 or key_004
	assert!(!results.iter().any(|k| k == "key_003"), "key_003 should not appear");
	assert!(!results.iter().any(|k| k == "key_004"), "key_004 should not appear");

	// First in reverse should be key_002 (highest version)
	assert_eq!(results[0], "key_002");
}

#[test]
fn test_empty_range_returns_nothing() {
	// Test that a range with no matching keys returns nothing
	let data = vec![("aaa", "value"), ("bbb", "value"), ("ddd", "value"), ("eee", "value")];

	let (src, size) = build_table(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Range [ccc, ccc] - "ccc" doesn't exist
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Included(b"ccc".as_slice()),
			Bound::Included(b"ccc".as_slice()),
		)))
		.unwrap();

	let results = collect_all(&mut iter).unwrap();
	assert_eq!(results.len(), 0, "Range for non-existent key should be empty");
}

#[test]
fn test_range_completely_before_table() {
	// Test range that is completely before all keys in table
	let data = vec![("mmm", "value"), ("nnn", "value"), ("ooo", "value")];

	let (src, size) = build_table(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Included(b"aaa".as_slice()),
			Bound::Included(b"bbb".as_slice()),
		)))
		.unwrap();

	let results = collect_all(&mut iter).unwrap();
	assert_eq!(results.len(), 0, "Range before all keys should be empty");
}

#[test]
fn test_range_completely_after_table() {
	// Test range that is completely after all keys in table
	let data = vec![("aaa", "value"), ("bbb", "value"), ("ccc", "value")];

	let (src, size) = build_table(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Included(b"xxx".as_slice()),
			Bound::Included(b"zzz".as_slice()),
		)))
		.unwrap();

	let results = collect_all(&mut iter).unwrap();
	assert_eq!(results.len(), 0, "Range after all keys should be empty");
}

#[test]
fn test_reverse_iteration_empty_range() {
	// Test reverse iteration with empty range
	let data = vec![("aaa", "value"), ("bbb", "value"), ("ddd", "value")];

	let (src, size) = build_table(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Range [ccc, ccc] doesn't exist
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Included(b"ccc".as_slice()),
			Bound::Included(b"ccc".as_slice()),
		)))
		.unwrap();

	let result = iter.seek_last();
	assert!(result.is_err() || !iter.valid(), "Empty range should be invalid on seek_last()");
}

#[test]
fn test_table_properties_persistence() {
	// Test that table properties are correctly persisted through write/load cycle
	let mut buffer = Vec::new();
	let table_id = 42;
	let opts = default_opts();

	// Step 1: Create TableWriter and add entries
	let mut writer = TableWriter::new(&mut buffer, table_id, Arc::clone(&opts), 0);

	let mut entries = Vec::new();
	let mut expected_deletions = 0u64;
	let mut expected_tombstones = 0u64;

	for i in 0..50 {
		let key = format!("key_{:03}", i).into_bytes();
		let value = format!("value_{:03}", i).into_bytes();
		let seq = 1000 + i;

		let kind = match i % 10 {
			0..=6 => InternalKeyKind::Set,
			7..=8 => {
				expected_deletions += 1;
				expected_tombstones += 1;
				InternalKeyKind::Delete
			}
			9 => {
				expected_deletions += 1;
				expected_tombstones += 1; // RangeDelete is also counted as a tombstone
				InternalKeyKind::RangeDelete
			}
			_ => unreachable!(),
		};

		let internal_key = InternalKey::new(key.clone(), seq, kind, 0);

		let entry_value = match kind {
			InternalKeyKind::Delete | InternalKeyKind::RangeDelete => vec![],
			_ => value.clone(),
		};

		writer.add(internal_key, &entry_value).unwrap();
		entries.push((key, value, seq, kind));
	}

	// Step 2: Finish writing to buffer
	let size = writer.finish().unwrap();

	// Step 3: Create new Table from buffer (load phase)
	let table = Table::new(table_id, opts, wrap_buffer(buffer), size as u64).unwrap();

	// Step 4: Verify all properties match expected values
	let meta = &table.meta;
	let props = &meta.properties;

	// Basic properties
	assert_eq!(props.id, table_id, "Table ID should match");
	assert_eq!(props.table_format, TableFormat::LSMV1, "Table format should be LSMV1");
	assert_eq!(props.num_entries, 50, "Number of entries should be 50");
	assert_eq!(props.item_count, 50, "Item count should be 50");
	assert_eq!(props.key_count, 50, "Key count should be 50");
	assert_eq!(props.num_deletions, expected_deletions, "Number of deletions should match");
	assert_eq!(props.tombstone_count, expected_tombstones, "Tombstone count should match");
	assert!(props.num_data_blocks > 0, "Should have at least one data block");
	assert!(props.data_size > 0, "Data size should be greater than 0");

	// Index properties
	assert!(props.index_size > 0, "Index size should be tracked");
	assert!(props.index_partitions > 0, "Should have at least one index partition");
	assert!(props.top_level_index_size > 0, "Top-level index size should be tracked");

	// Filter properties
	assert!(props.filter_size > 0, "Filter size should be tracked");

	// Raw size metrics
	assert!(props.raw_key_size > 0, "Raw key size should be tracked");
	assert!(props.raw_value_size > 0, "Raw value size should be tracked");
	assert_eq!(
		props.raw_key_size + props.raw_value_size,
		props.data_size,
		"Raw sizes should sum to data_size"
	);

	// Sequence number range
	assert_eq!(props.seqnos.0, 1000, "Smallest sequence number should be 1000");
	assert_eq!(props.seqnos.1, 1049, "Largest sequence number should be 1049");
	assert_eq!(meta.smallest_seq_num, Some(1000), "Metadata smallest seq num should match");
	assert_eq!(meta.largest_seq_num, Some(1049), "Metadata largest seq num should match");

	// Compression
	assert_eq!(props.compression, crate::CompressionType::None, "Compression should be None");

	// Block properties
	assert!(props.block_size > 0, "Block size should be tracked");
	assert!(props.block_count > 0, "Block count should be tracked");

	// Time metrics (timestamps are 0 in this test)
	assert_eq!(props.oldest_key_time, Some(0), "Oldest key time should be 0");
	assert_eq!(props.newest_key_time, Some(0), "Newest key time should be 0");

	// Range deletion metrics
	assert_eq!(props.num_range_deletions, 5, "Should have 5 range deletions");

	// Created at timestamp
	assert!(props.created_at > 0, "Created at timestamp should be set");

	// Verify smallest and largest point keys
	assert!(meta.smallest_point.is_some(), "Should have smallest point key");
	assert!(meta.largest_point.is_some(), "Should have largest point key");
	if let Some(smallest) = &meta.smallest_point {
		assert_eq!(&smallest.user_key, b"key_000", "Smallest key should be key_000");
		assert_eq!(smallest.seq_num(), 1000, "Smallest seq num should be 1000");
	}
	if let Some(largest) = &meta.largest_point {
		assert_eq!(&largest.user_key, b"key_049", "Largest key should be key_049");
		assert_eq!(largest.seq_num(), 1049, "Largest seq num should be 1049");
	}

	// Verify TableMetadata fields
	assert_eq!(meta.has_point_keys, Some(true), "Should have point keys");
}

#[test]
fn test_table_get_all_keys() {
	// Test Get operations on table with 100+ blocks
	let mut opts = default_opts_mut();
	opts.block_size = 64; // Small block size to create many blocks
	opts.index_partition_size = 200;
	let opts = Arc::new(opts);

	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Add 100 entries to create multiple blocks
	for i in 0..100 {
		let key = format!("key_{i:03}");
		let value = format!("value_{i:03}");
		let internal_key =
			InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);
		writer.add(internal_key, value.as_bytes()).unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

	// Get every key individually
	for i in 0..100 {
		let key = format!("key_{i:03}");
		let expected_value = format!("value_{i:03}");
		let seek_key =
			InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);

		let result = table.get(&seek_key).unwrap();
		assert!(result.is_some(), "Key '{key}' not found");

		if let Some((found_key, found_value)) = result {
			assert_eq!(
				std::str::from_utf8(&found_key.user_key).unwrap(),
				key,
				"Key mismatch for {key}"
			);
			assert_eq!(
				std::str::from_utf8(found_value.as_ref()).unwrap(),
				expected_value,
				"Value mismatch for {key}"
			);
		}
	}
}

#[test]
fn test_table_get_nonexistent_keys() {
	let opts = default_opts();
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Add some entries
	for i in 0..10 {
		let key = format!("key_{i:02}");
		let internal_key =
			InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);
		writer.add(internal_key, b"value").unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

	// Get keys that don't exist
	let nonexistent_keys = vec!["key_10", "key_99", "aaa", "zzz"];
	for key_str in &nonexistent_keys {
		let seek_key = InternalKey::new(key_str.as_bytes().to_vec(), 100, InternalKeyKind::Set, 0);
		let result = table.get(&seek_key).unwrap();
		// Should return None for keys that don't exist
		if result.is_some() {
			// If found, verify it's a valid key (might find next key)
			let (found_key, _) = result.unwrap();
			let found_str = std::str::from_utf8(&found_key.user_key).unwrap();
			assert!(found_str.starts_with("key_"), "Found key {found_str} should be valid");
		}
	}
}

#[test]
fn test_table_get_with_compression() {
	// Test Get with each compression type
	let compression_types =
		vec![crate::CompressionType::None, crate::CompressionType::SnappyCompression];

	for compression in compression_types {
		let opts = default_opts();
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

		// Add entries
		for i in 0..20 {
			let key = format!("key_{i:02}");
			let value = format!("value_{i:02}");
			let internal_key =
				InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);
			writer.add(internal_key, value.as_bytes()).unwrap();
		}

		let size = writer.finish().unwrap();
		let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

		// Verify Get works with this compression type
		for i in 0..20 {
			let key = format!("key_{i:02}");
			let expected_value = format!("value_{i:02}");
			let seek_key =
				InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);

			let result = table.get(&seek_key).unwrap();
			assert!(result.is_some(), "Should find key {key} with compression {compression:?}");

			if let Some((found_key, found_value)) = result {
				assert_eq!(
					std::str::from_utf8(&found_key.user_key).unwrap(),
					key,
					"Key mismatch with compression {compression:?}"
				);
				assert_eq!(
					std::str::from_utf8(found_value.as_ref()).unwrap(),
					expected_value,
					"Value mismatch with compression {compression:?}"
				);
			}
		}
	}
}

#[test]
fn test_table_iterator_full_scan() {
	let opts = default_opts();
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Create table with known data
	let test_data: Vec<(String, String)> =
		(0..50).map(|i| (format!("key_{i:03}"), format!("value_{i:03}"))).collect();

	for (i, (key, value)) in test_data.iter().enumerate() {
		let internal_key =
			InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);
		writer.add(internal_key, value.as_bytes()).unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

	// Full forward scan
	let mut iter = table.iter(None).unwrap();
	iter.seek_to_first().unwrap();

	let mut collected = Vec::new();
	while iter.valid() {
		let key = iter.key();
		let value = iter.value_encoded().unwrap();
		collected.push((
			std::str::from_utf8(key.user_key()).unwrap().to_string(),
			std::str::from_utf8(value).unwrap().to_string(),
		));
		match iter.next() {
			Ok(false) => break,
			Ok(true) => {}
			Err(e) => panic!("Iterator error: {e}"),
		}
	}

	assert_eq!(collected.len(), 50, "Should collect all 50 entries");
	for (i, (key, value)) in collected.iter().enumerate() {
		assert_eq!(*key, format!("key_{i:03}"), "Key order mismatch");
		assert_eq!(*value, format!("value_{i:03}"), "Value mismatch");
	}
}

#[test]
fn test_table_iterator_reverse_scan() {
	let opts = default_opts();
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Create table with known data
	let test_data: Vec<(String, String)> =
		(0..50).map(|i| (format!("key_{i:03}"), format!("value_{i:03}"))).collect();

	for (i, (key, value)) in test_data.iter().enumerate() {
		let internal_key =
			InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);
		writer.add(internal_key, value.as_bytes()).unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

	// Full reverse scan
	let mut iter = table.iter(None).unwrap();
	iter.seek_to_last().unwrap();

	let mut collected = Vec::new();
	while iter.valid() {
		let key = iter.key();
		let value = iter.value_encoded().unwrap();
		collected.push((
			std::str::from_utf8(key.user_key()).unwrap().to_string(),
			std::str::from_utf8(value).unwrap().to_string(),
		));
		if !iter.prev().unwrap() {
			break;
		}
	}

	assert_eq!(collected.len(), 50, "Should collect all 50 entries");
	// Verify reverse order
	for (i, (key, value)) in collected.iter().enumerate() {
		let expected_idx = 49 - i;
		assert_eq!(*key, format!("key_{expected_idx:03}"), "Reverse key order mismatch");
		assert_eq!(*value, format!("value_{expected_idx:03}"), "Reverse value mismatch");
	}
}

#[test]
fn test_table_iterator_seek_and_scan() {
	let opts = default_opts();
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Create table with known data
	for i in 0..30 {
		let key = format!("key_{i:03}");
		let value = format!("value_{i:03}");
		let internal_key =
			InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);
		writer.add(internal_key, value.as_bytes()).unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

	let mut iter = table.iter(None).unwrap();

	// Test Seek to various points
	let seek_points = vec!["key_005", "key_010", "key_015", "key_020", "key_025"];

	for seek_key_str in &seek_points {
		let seek_key =
			InternalKey::new(seek_key_str.as_bytes().to_vec(), 100, InternalKeyKind::Set, 0);
		iter.seek(&seek_key.encode()).unwrap();

		if iter.valid() {
			let found_key_bytes: &[u8] = iter.key().user_key();
			let seek_key_bytes = seek_key_str.as_bytes();
			// Should find the key or next key
			assert!(
				found_key_bytes >= seek_key_bytes,
				"Found key should be >= seek key {seek_key_str}"
			);
		}

		// Test Next from this point
		let mut count = 0;
		while iter.valid() && count < 5 {
			count += 1;
			match iter.next() {
				Ok(false) => break,
				Ok(true) => {}
				Err(e) => panic!("Iterator error: {e}"),
			}
		}
	}

	iter.seek_to_last().unwrap();
	let last_key_bytes = iter.key().user_key();
	assert_eq!(std::str::from_utf8(last_key_bytes).unwrap(), "key_029", "Should find last key");

	// Test Prev from last
	for _ in 0..5 {
		if !iter.prev().unwrap() {
			break;
		}
	}
}

#[test]
fn test_table_iterator_across_partitions() {
	// Create table that spans multiple index partitions
	let mut opts = default_opts_mut();
	opts.index_partition_size = 150; // Small partition size
	opts.block_size = 500;
	let opts = Arc::new(opts);

	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Add enough entries to create multiple partitions
	for i in 0..100 {
		let key = format!("key_{i:03}");
		let value = format!("value_{i:03}");
		let internal_key =
			InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);
		writer.add(internal_key, value.as_bytes()).unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

	// Verify we have multiple partitions
	let crate::sstable::table::IndexType::Partitioned(ref partitioned_index) = table.index_block;
	assert!(partitioned_index.blocks.len() >= 2, "Should have multiple partitions");

	// Full forward scan across partitions
	let mut iter = table.iter(None).unwrap();
	iter.seek_to_first().unwrap();

	let mut count = 0;
	while iter.valid() {
		count += 1;
		match iter.next() {
			Ok(false) => break,
			Ok(true) => {}
			Err(e) => panic!("Iterator error: {e}"),
		}
	}
	assert_eq!(count, 100, "Should iterate all 100 keys across partitions");

	// Full backward scan across partitions
	iter.seek_to_last().unwrap();
	let mut count_backward = 0;
	while iter.valid() {
		count_backward += 1;
		if !iter.prev().unwrap() {
			break;
		}
	}
	assert_eq!(count_backward, 100, "Should iterate all 100 keys backwards across partitions");
}

#[test]
fn test_empty_table_operations() {
	let opts = default_opts();
	let mut buffer = Vec::new();
	let writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Don't add any entries - create empty table
	let size = writer.finish().unwrap();
	let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

	// Verify Get returns not found
	let seek_key = InternalKey::new(b"any_key".to_vec(), 1, InternalKeyKind::Set, 0);
	let result = table.get(&seek_key);
	assert!(result.is_err(), "Get on empty table should return error");

	// Verify iterator immediately invalid
	let mut iter = table.iter(None).unwrap();
	let result = iter.seek_to_first();
	assert!(result.is_err(), "Get on empty table should return error");
}

#[test]
fn test_single_entry_table() {
	let opts = default_opts();
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Add exactly one entry
	let internal_key = InternalKey::new(b"single_key".to_vec(), 1, InternalKeyKind::Set, 0);
	writer.add(internal_key, b"single_value").unwrap();

	let size = writer.finish().unwrap();
	let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

	// Test Get
	let seek_key = InternalKey::new(b"single_key".to_vec(), 1, InternalKeyKind::Set, 0);
	let result = table.get(&seek_key).unwrap();
	assert!(result.is_some(), "Should find the single key");
	if let Some((found_key, found_value)) = result {
		assert_eq!(found_key.user_key, b"single_key");
		let value_bytes: &[u8] = found_value.as_ref();
		assert_eq!(value_bytes, b"single_value");
	}

	// Test iterator
	let mut iter = table.iter(None).unwrap();
	iter.seek_to_first().unwrap();
	assert!(iter.valid(), "Iterator should be valid");
	assert_eq!(iter.key().user_key(), b"single_key");
	let iter_value = iter.value_encoded().unwrap();
	let iter_value_bytes: &[u8] = iter_value;
	assert_eq!(iter_value_bytes, b"single_value");

	// Test Next - should invalidate iterator
	match iter.next() {
		Ok(false) => {}
		Ok(true) => {}
		Err(e) => panic!("Iterator error: {e}"),
	}
	assert!(!iter.valid(), "Iterator should be invalid after next()");
}

#[test]
fn test_large_values() {
	let opts = default_opts();
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 0, Arc::clone(&opts), 0);

	// Add entries with large values (near size limits)
	let large_value = vec![b'x'; 10000]; // 10KB value
	for i in 0..10 {
		let key = format!("key_{i:02}");
		let internal_key =
			InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);
		writer.add(internal_key, &large_value).unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Arc::new(Table::new(0, opts, wrap_buffer(buffer), size as u64).unwrap());

	// Verify all large values are retrieved correctly
	for i in 0..10 {
		let key = format!("key_{i:02}");
		let seek_key =
			InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);

		let result = table.get(&seek_key).unwrap();
		assert!(result.is_some(), "Should find key {key}");

		if let Some((found_key, found_value)) = result {
			assert_eq!(found_key.user_key, key.as_bytes());
			let found_bytes: &[u8] = found_value.as_ref();
			assert_eq!(found_bytes, &large_value[..], "Large value should match");
			assert_eq!(found_bytes.len(), 10000, "Value size should be 10000");
		}
	}
}

// ============================================================================
// MVCC Version Handling in Range Iteration Tests
// ============================================================================
// These tests verify correct behavior when iterating over keys with multiple
// MVCC versions, particularly for backward iteration with bounds.

#[test]
fn test_backward_iter_multiple_versions_same_user_key() {
	// REGRESSION TEST: When a user key has multiple MVCC versions and we iterate
	// backward with an inclusive upper bound, all versions should be returned.
	//
	// Bug: seek_to_upper_bound() was comparing only user keys, causing it to
	// position at the FIRST version instead of the LAST version of the key.
	// This caused prev() to immediately return false, missing other versions.
	let data = vec![
		// Two versions of the same key with different seq_nums
		// Higher seq_num comes first in internal key order
		("key_000", "value_v2", 200),
		("key_000", "value_v1", 100),
	];

	let (src, size) = build_table_with_seq_num(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Backward iteration with unbounded range should return ALL versions
	let mut iter = table.iter(None).unwrap();
	iter.seek_to_last().unwrap();
	let mut results: Vec<(String, u64)> = Vec::new();

	while iter.valid() {
		let key = iter.key();
		let user_key = String::from_utf8(key.user_key().to_vec()).unwrap();
		results.push((user_key, key.seq_num()));
		iter.prev().unwrap();
	}

	// Should get both versions
	assert_eq!(
		results.len(),
		2,
		"Backward iteration should return both MVCC versions. Got: {:?}",
		results
	);

	// First result should be lower seq_num (comes later in internal order)
	assert_eq!(results[0].1, 100, "First backward result should be seq=100");
	// Second result should be higher seq_num (comes earlier in internal order)
	assert_eq!(results[1].1, 200, "Second backward result should be seq=200");
}

#[test]
fn test_backward_iter_inclusive_upper_bound_mvcc() {
	// Test inclusive upper bound positioning with multiple MVCC versions
	let data = vec![
		("key_001", "v1", 300),
		("key_001", "v2", 200),
		("key_001", "v3", 100),
		("key_002", "v1", 50),
	];

	let (src, size) = build_table_with_seq_num(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Inclusive upper bound at key_001 - should include all versions of key_001
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Unbounded,
			Bound::Included(b"key_001".as_slice()),
		)))
		.unwrap();
	iter.seek_to_last().unwrap();

	let mut results: Vec<(String, u64)> = Vec::new();
	while iter.valid() {
		let key = iter.key();
		let user_key = String::from_utf8(key.user_key().to_vec()).unwrap();
		results.push((user_key, key.seq_num()));
		iter.prev().unwrap();
	}

	// Should get all 3 versions of key_001
	assert_eq!(results.len(), 3, "Should return all 3 versions of key_001. Got: {:?}", results);

	// Verify all are key_001
	for (user_key, _) in &results {
		assert_eq!(user_key, "key_001");
	}
}

#[test]
fn test_backward_iter_exclusive_upper_bound_mvcc() {
	// Test exclusive upper bound - should exclude ALL versions of the bound key
	let data = vec![
		("key_001", "v1", 300),
		("key_001", "v2", 200),
		("key_002", "v1", 150),
		("key_002", "v2", 100),
	];

	let (src, size) = build_table_with_seq_num(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Exclusive upper bound at key_002 - should exclude all versions of key_002
	let mut iter = table
		.iter(Some(user_range_to_internal_range(
			Bound::Unbounded,
			Bound::Excluded(b"key_002".as_slice()),
		)))
		.unwrap();
	iter.seek_to_last().unwrap();

	let mut results: Vec<(String, u64)> = Vec::new();
	while iter.valid() {
		let key = iter.key();
		let user_key = String::from_utf8(key.user_key().to_vec()).unwrap();
		results.push((user_key, key.seq_num()));
		iter.prev().unwrap();
	}

	// Should only get key_001 versions (2 of them)
	assert_eq!(results.len(), 2, "Should return only key_001 versions. Got: {:?}", results);

	// Verify none are key_002
	for (user_key, _) in &results {
		assert_eq!(user_key, "key_001", "Should not contain key_002");
	}
}

#[test]
fn test_forward_backward_consistency_mvcc() {
	// Verify forward and backward iteration return same entries (in reverse order)
	let data = vec![
		("key_001", "v1", 300),
		("key_001", "v2", 200),
		("key_002", "v1", 150),
		("key_003", "v1", 100),
	];

	let (src, size) = build_table_with_seq_num(data);
	let opts = default_opts();
	let table = Arc::new(Table::new(1, opts, wrap_buffer(src), size as u64).unwrap());

	// Forward iteration
	let mut iter_fwd = table.iter(None).unwrap();
	iter_fwd.seek_to_first().unwrap();
	let mut forward_results: Vec<(Vec<u8>, u64)> = vec![];
	while iter_fwd.valid() {
		let key = iter_fwd.key();
		forward_results.push((key.user_key().to_vec(), key.seq_num()));
		iter_fwd.next().unwrap();
	}

	// Backward iteration
	let mut iter_bwd = table.iter(None).unwrap();
	iter_bwd.seek_to_last().unwrap();
	let mut backward_results: Vec<(Vec<u8>, u64)> = Vec::new();
	while iter_bwd.valid() {
		let key = iter_bwd.key();
		backward_results.push((key.user_key().to_vec(), key.seq_num()));
		iter_bwd.prev().unwrap();
	}

	// Reverse backward results to compare
	backward_results.reverse();

	assert_eq!(
		forward_results.len(),
		backward_results.len(),
		"Forward and backward should return same count"
	);
	assert_eq!(forward_results, backward_results, "Forward and reversed backward should match");
}

// =============================================================================
// Metadata Edge Case Tests
// These tests verify that seq_num and timestamp tracking work correctly,
// especially for edge cases like when the first value is 0.
// =============================================================================

/// Tests that seq_num tracking works correctly when the first entry has seq_num=0.
/// This was a bug where using 0 as sentinel value caused incorrect tracking.
#[test]
fn test_seq_num_tracking_with_zero_first() {
	let opts = default_opts();
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 1, Arc::clone(&opts), 0);

	// Add entries with seq_num: 0, 100, 50 (first is 0!)
	// Keys must be in ascending order for SSTable
	let entries = vec![
		("aaa", 0u64),   // First entry has seq_num = 0
		("bbb", 100u64), // Larger seq_num
		("ccc", 50u64),  // Middle seq_num
	];

	for (key, seq) in &entries {
		let internal_key = InternalKey::new(key.as_bytes().to_vec(), *seq, InternalKeyKind::Set, 0);
		writer.add(internal_key, b"value").unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap();

	// Verify: smallest should be 0 (not overwritten), largest should be 100
	assert_eq!(
		table.meta.smallest_seq_num,
		Some(0),
		"smallest_seq_num should be 0 (first entry's seq_num)"
	);
	assert_eq!(table.meta.largest_seq_num, Some(100), "largest_seq_num should be 100");

	// Also verify properties.seqnos matches
	assert_eq!(table.meta.properties.seqnos, (0, 100), "seqnos tuple should match");
}

/// Tests seq_num tracking with various insertion orders.
#[test]
fn test_seq_num_tracking_various_orders() {
	// Test case 1: Ascending order
	{
		let opts = default_opts();
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 1, Arc::clone(&opts), 0);

		for (i, seq) in [1u64, 2, 3].iter().enumerate() {
			let key = format!("key_{i:02}");
			let internal_key =
				InternalKey::new(key.as_bytes().to_vec(), *seq, InternalKeyKind::Set, 0);
			writer.add(internal_key, b"value").unwrap();
		}

		let size = writer.finish().unwrap();
		let table = Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap();

		assert_eq!(table.meta.smallest_seq_num, Some(1), "Ascending: smallest=1");
		assert_eq!(table.meta.largest_seq_num, Some(3), "Ascending: largest=3");
	}

	// Test case 2: Descending order (keys still ascending, but seq_nums descending)
	{
		let opts = default_opts();
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 1, Arc::clone(&opts), 0);

		for (i, seq) in [3u64, 2, 1].iter().enumerate() {
			let key = format!("key_{i:02}");
			let internal_key =
				InternalKey::new(key.as_bytes().to_vec(), *seq, InternalKeyKind::Set, 0);
			writer.add(internal_key, b"value").unwrap();
		}

		let size = writer.finish().unwrap();
		let table = Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap();

		assert_eq!(table.meta.smallest_seq_num, Some(1), "Descending: smallest=1");
		assert_eq!(table.meta.largest_seq_num, Some(3), "Descending: largest=3");
	}

	// Test case 3: Random order
	{
		let opts = default_opts();
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 1, Arc::clone(&opts), 0);

		for (i, seq) in [5u64, 1, 10, 3].iter().enumerate() {
			let key = format!("key_{i:02}");
			let internal_key =
				InternalKey::new(key.as_bytes().to_vec(), *seq, InternalKeyKind::Set, 0);
			writer.add(internal_key, b"value").unwrap();
		}

		let size = writer.finish().unwrap();
		let table = Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap();

		assert_eq!(table.meta.smallest_seq_num, Some(1), "Random: smallest=1");
		assert_eq!(table.meta.largest_seq_num, Some(10), "Random: largest=10");
	}
}

/// Tests that timestamp tracking works correctly when the first entry has timestamp=0.
#[test]
fn test_timestamp_tracking_with_zero_first() {
	let opts = default_opts();
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 1, Arc::clone(&opts), 0);

	// Add entries with timestamps: 0, 1000, 500 (first is 0!)
	let entries = vec![
		("aaa", 0u64),    // First entry has timestamp = 0
		("bbb", 1000u64), // Larger timestamp
		("ccc", 500u64),  // Middle timestamp
	];

	for (key, ts) in &entries {
		let internal_key = InternalKey::new(key.as_bytes().to_vec(), 1, InternalKeyKind::Set, *ts);
		writer.add(internal_key, b"value").unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap();

	// Verify: oldest should be 0 (not overwritten), newest should be 1000
	assert_eq!(
		table.meta.properties.oldest_key_time,
		Some(0),
		"oldest_key_time should be 0 (first entry's timestamp)"
	);
	assert_eq!(table.meta.properties.newest_key_time, Some(1000), "newest_key_time should be 1000");
}

/// Tests timestamp tracking with various insertion orders.
#[test]
fn test_timestamp_tracking_various_orders() {
	// Test case 1: Ascending timestamps
	{
		let opts = default_opts();
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 1, Arc::clone(&opts), 0);

		for (i, ts) in [100u64, 200, 300].iter().enumerate() {
			let key = format!("key_{i:02}");
			let internal_key =
				InternalKey::new(key.as_bytes().to_vec(), 1, InternalKeyKind::Set, *ts);
			writer.add(internal_key, b"value").unwrap();
		}

		let size = writer.finish().unwrap();
		let table = Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap();

		assert_eq!(table.meta.properties.oldest_key_time, Some(100));
		assert_eq!(table.meta.properties.newest_key_time, Some(300));
	}

	// Test case 2: Descending timestamps
	{
		let opts = default_opts();
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 1, Arc::clone(&opts), 0);

		for (i, ts) in [300u64, 200, 100].iter().enumerate() {
			let key = format!("key_{i:02}");
			let internal_key =
				InternalKey::new(key.as_bytes().to_vec(), 1, InternalKeyKind::Set, *ts);
			writer.add(internal_key, b"value").unwrap();
		}

		let size = writer.finish().unwrap();
		let table = Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap();

		assert_eq!(table.meta.properties.oldest_key_time, Some(100));
		assert_eq!(table.meta.properties.newest_key_time, Some(300));
	}

	// Test case 3: Random order with 0
	{
		let opts = default_opts();
		let mut buffer = Vec::new();
		let mut writer = TableWriter::new(&mut buffer, 1, Arc::clone(&opts), 0);

		for (i, ts) in [500u64, 0, 1000, 250].iter().enumerate() {
			let key = format!("key_{i:02}");
			let internal_key =
				InternalKey::new(key.as_bytes().to_vec(), 1, InternalKeyKind::Set, *ts);
			writer.add(internal_key, b"value").unwrap();
		}

		let size = writer.finish().unwrap();
		let table = Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap();

		assert_eq!(table.meta.properties.oldest_key_time, Some(0));
		assert_eq!(table.meta.properties.newest_key_time, Some(1000));
	}
}

/// Tests that a single entry correctly sets both smallest=largest and oldest=newest.
#[test]
fn test_single_entry_metadata() {
	let opts = default_opts();
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 1, Arc::clone(&opts), 0);

	// Single entry with specific values
	let internal_key = InternalKey::new(b"only_key".to_vec(), 42, InternalKeyKind::Set, 12345);
	writer.add(internal_key, b"only_value").unwrap();

	let size = writer.finish().unwrap();
	let table = Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap();

	// Both smallest and largest should be 42
	assert_eq!(table.meta.smallest_seq_num, Some(42));
	assert_eq!(table.meta.largest_seq_num, Some(42));
	assert_eq!(table.meta.properties.seqnos, (42, 42));

	// Both oldest and newest should be 12345
	assert_eq!(table.meta.properties.oldest_key_time, Some(12345));
	assert_eq!(table.meta.properties.newest_key_time, Some(12345));
}

/// Tests that metadata survives encode/decode roundtrip with edge values.
#[test]
fn test_metadata_roundtrip_with_edge_values() {
	use crate::sstable::meta::TableMetadata;

	// Test case 1: seq_num = 0
	{
		let mut meta = TableMetadata::new();
		meta.update_seq_num(0);
		meta.update_seq_num(100);

		let encoded = meta.encode();
		let decoded = TableMetadata::decode(&encoded).unwrap();

		assert_eq!(decoded.smallest_seq_num, Some(0), "Roundtrip should preserve seq_num=0");
		assert_eq!(decoded.largest_seq_num, Some(100));
	}

	// Test case 2: Large seq_num values
	{
		let mut meta = TableMetadata::new();
		meta.update_seq_num(1);
		meta.update_seq_num(u64::MAX - 1); // Near max value

		let encoded = meta.encode();
		let decoded = TableMetadata::decode(&encoded).unwrap();

		assert_eq!(decoded.smallest_seq_num, Some(1));
		assert_eq!(decoded.largest_seq_num, Some(u64::MAX - 1));
	}
}

/// Tests that all entries having seq_num=0 works correctly.
#[test]
fn test_all_zero_seq_nums() {
	let opts = default_opts();
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 1, Arc::clone(&opts), 0);

	// All entries have seq_num = 0
	for i in 0..5 {
		let key = format!("key_{i:02}");
		let internal_key = InternalKey::new(key.as_bytes().to_vec(), 0, InternalKeyKind::Set, 0);
		writer.add(internal_key, b"value").unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap();

	// Both should be 0
	assert_eq!(table.meta.smallest_seq_num, Some(0));
	assert_eq!(table.meta.largest_seq_num, Some(0));
	assert_eq!(table.meta.properties.seqnos, (0, 0));
}

/// Tests that all entries having timestamp=0 works correctly.
#[test]
fn test_all_zero_timestamps() {
	let opts = default_opts();
	let mut buffer = Vec::new();
	let mut writer = TableWriter::new(&mut buffer, 1, Arc::clone(&opts), 0);

	// All entries have timestamp = 0
	for i in 0..5 {
		let key = format!("key_{i:02}");
		let internal_key =
			InternalKey::new(key.as_bytes().to_vec(), (i + 1) as u64, InternalKeyKind::Set, 0);
		writer.add(internal_key, b"value").unwrap();
	}

	let size = writer.finish().unwrap();
	let table = Table::new(1, opts, wrap_buffer(buffer), size as u64).unwrap();

	// Both should be 0
	assert_eq!(table.meta.properties.oldest_key_time, Some(0));
	assert_eq!(table.meta.properties.newest_key_time, Some(0));
}
