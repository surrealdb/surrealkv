use std::sync::Arc;
use std::vec;

use test_log::test;

use crate::sstable::block::{Block, BlockWriter};
use crate::{InternalKey, InternalKeyKind, LSMIterator, Options};

fn generate_data() -> Vec<(&'static [u8], &'static [u8])> {
	vec![
		("key1".as_bytes(), "value1".as_bytes()),
		("loooongkey1".as_bytes(), "value2".as_bytes()),
		("medium_key2".as_bytes(), "value3".as_bytes()),
		("pkey1".as_bytes(), "value".as_bytes()),
		("pkey2".as_bytes(), "value".as_bytes()),
		("pkey3".as_bytes(), "value".as_bytes()),
	]
}

fn make_opts(block_restart_interval: Option<usize>) -> Arc<Options> {
	let mut opt = Options::default();
	if let Some(interval) = block_restart_interval {
		opt.block_restart_interval = interval;
	}
	Arc::new(opt)
}

fn make_internal_key(key: &[u8], kind: InternalKeyKind) -> Vec<u8> {
	InternalKey::new(key.to_vec(), 0, kind, 0).encode()
}

#[test]
fn test_block_empty() {
	let o = make_opts(None);
	let builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	let blockc = builder.finish().unwrap();
	assert_eq!(blockc.len(), 8);
	assert_eq!(blockc.as_slice(), &[0, 0, 0, 0, 1, 0, 0, 0]);

	let mut block_iter = Block::new(blockc, Arc::clone(&o.internal_comparator)).iter().unwrap();

	let mut i = 0;
	while block_iter.advance().unwrap() {
		i += 1;
	}

	assert_eq!(i, 0);
}

#[test]
fn test_block_iter() {
	let data = generate_data();
	let o = make_opts(None);
	let mut builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	for &(k, v) in data.iter() {
		builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
	}

	let block_contents = builder.finish().unwrap();

	let mut block_iter =
		Block::new(block_contents, Arc::clone(&o.internal_comparator)).iter().unwrap();

	let mut i = 0;
	while block_iter.advance().unwrap() {
		assert_eq!(block_iter.key().user_key(), data[i].0);
		assert_eq!(block_iter.value_encoded().unwrap(), data[i].1.to_vec());
		i += 1;
	}

	assert_eq!(i, data.len());
}

#[test]
fn test_block_iter_reverse() {
	let data = generate_data();
	let o = make_opts(Some(3));
	let mut builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	for &(k, v) in data.iter() {
		builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
	}

	let block_contents = builder.finish().unwrap();
	let mut iter = Block::new(block_contents, Arc::clone(&o.internal_comparator)).iter().unwrap();
	iter.seek_to_first().unwrap();

	// After seek_to_first, we're at first entry (key1)
	assert_eq!(iter.key().user_key(), "key1".as_bytes());
	assert_eq!(iter.value_encoded().unwrap(), b"value1".to_vec());

	// Advance to second entry (loooongkey1)
	iter.next().unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), "loooongkey1".as_bytes());
	assert_eq!(iter.value_encoded().unwrap(), b"value2".to_vec());

	// Advance to third entry (medium_key2)
	iter.next().unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), "medium_key2".as_bytes());

	// Go back to second entry (loooongkey1)
	iter.prev().unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), "loooongkey1".as_bytes());
	assert_eq!(iter.value_encoded().unwrap(), b"value2".to_vec());

	// Go to the last entry using seek_to_last
	iter.seek_to_last().unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), "pkey3".as_bytes());
	assert_eq!(iter.value_encoded().unwrap(), b"value".to_vec());

	// Move to second-to-last entry (pkey2)
	iter.prev().unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), "pkey2".as_bytes());
	assert_eq!(iter.value_encoded().unwrap(), b"value".to_vec());
}

#[test]
fn test_block_seek() {
	let data = generate_data();
	let o = make_opts(Some(3));
	let mut builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	for &(k, v) in data.iter() {
		builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
	}

	let block_contents = builder.finish().unwrap();

	let mut block_iter =
		Block::new(block_contents, Arc::clone(&o.internal_comparator)).iter().unwrap();

	let key = InternalKey::new(b"pkey2".to_vec(), 1, InternalKeyKind::Set, 0);
	block_iter.seek(&key.encode()).unwrap();
	assert!(block_iter.valid());
	assert_eq!(
		Some((block_iter.key().user_key(), block_iter.value_encoded().unwrap(),)),
		Some(("pkey2".as_bytes(), "value".as_bytes()))
	);

	let key = InternalKey::new(b"pkey0".to_vec(), 1, InternalKeyKind::Set, 0);
	block_iter.seek(&key.encode()).unwrap();
	assert!(block_iter.valid());
	assert_eq!(
		Some((block_iter.key().user_key(), block_iter.value_encoded().unwrap(),)),
		Some(("pkey1".as_bytes(), "value".as_bytes()))
	);

	let key = InternalKey::new(b"key1".to_vec(), 1, InternalKeyKind::Set, 0);
	block_iter.seek(&key.encode()).unwrap();
	assert!(block_iter.valid());
	assert_eq!(
		Some((block_iter.key().user_key(), block_iter.value_encoded().unwrap(),)),
		Some(("key1".as_bytes(), "value1".as_bytes()))
	);

	let key = InternalKey::new(b"pkey3".to_vec(), 1, InternalKeyKind::Set, 0);
	block_iter.seek(&key.encode()).unwrap();
	assert!(block_iter.valid());
	assert_eq!(
		Some((block_iter.key().user_key(), block_iter.value_encoded().unwrap(),)),
		Some(("pkey3".as_bytes(), "value".as_bytes()))
	);

	let key = InternalKey::new(b"pkey8".to_vec(), 1, InternalKeyKind::Set, 0);
	block_iter.seek(&key.encode()).unwrap();
	assert!(!block_iter.valid());
}

#[test]
fn test_block_seek_to_last() {
	// Test with different number of restarts
	for block_restart_interval in [2, 6, 10] {
		let data = generate_data();
		let o = make_opts(Some(block_restart_interval));
		let mut builder = BlockWriter::new(
			o.block_size,
			o.block_restart_interval,
			Arc::clone(&o.internal_comparator),
		);

		for &(k, v) in data.iter() {
			builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
		}

		let block_contents = builder.finish().unwrap();

		let mut block_iter =
			Block::new(block_contents, Arc::clone(&o.internal_comparator)).iter().unwrap();

		block_iter.seek_to_last().unwrap();
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key(), "pkey3".as_bytes());
		assert_eq!(block_iter.value_encoded().unwrap(), b"value".to_vec());

		block_iter.seek_to_first().unwrap();
		assert!(block_iter.valid());
		assert_eq!(block_iter.key().user_key(), "key1".as_bytes());
		assert_eq!(block_iter.value_encoded().unwrap(), b"value1".to_vec());

		block_iter.next().unwrap();
		assert!(block_iter.valid());
		block_iter.next().unwrap();
		assert!(block_iter.valid());
		block_iter.next().unwrap();
		assert!(block_iter.valid());

		assert_eq!(block_iter.key().user_key(), "pkey1".as_bytes());
		assert_eq!(block_iter.value_encoded().unwrap(), b"value".to_vec());
	}
}

#[test]
fn test_block_prev() {
	// Test backward iteration using prev()
	let data = generate_data();
	let o = make_opts(Some(2)); // Small restart interval to test restart logic
	let mut builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	for &(k, v) in data.iter() {
		builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
	}

	let block_contents = builder.finish().unwrap();
	let mut block_iter =
		Block::new(block_contents, Arc::clone(&o.internal_comparator)).iter().unwrap();

	// Test prev() from the end
	block_iter.seek_to_last().unwrap();
	assert!(block_iter.valid());
	assert_eq!(block_iter.key().user_key(), "pkey3".as_bytes());

	// Go backward one step
	assert!(block_iter.prev().unwrap());
	assert!(block_iter.valid());
	assert_eq!(block_iter.key().user_key(), "pkey2".as_bytes());

	// Go backward another step
	assert!(block_iter.prev().unwrap());
	assert!(block_iter.valid());
	assert_eq!(block_iter.key().user_key(), "pkey1".as_bytes());

	// Go backward one more step
	assert!(block_iter.prev().unwrap());
	assert!(block_iter.valid());
	assert_eq!(block_iter.key().user_key(), "medium_key2".as_bytes());

	// Go backward to the beginning
	assert!(block_iter.prev().unwrap());
	assert!(block_iter.valid());
	assert_eq!(block_iter.key().user_key(), "loooongkey1".as_bytes());

	// Go backward to the first key
	assert!(block_iter.prev().unwrap());
	assert!(block_iter.valid());
	assert_eq!(block_iter.key().user_key(), "key1".as_bytes());

	// Try to go past the beginning
	assert!(!block_iter.prev().unwrap());
	assert!(!block_iter.valid());
}

#[test]
fn test_block_double_ended_iteration() {
	// Test using both next() and next_back() (DoubleEndedIterator)
	let data = generate_data();
	let o = make_opts(Some(3));
	let mut builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	for &(k, v) in data.iter() {
		builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
	}

	let block_contents = builder.finish().unwrap();

	// Test forward iteration with next()
	let mut forward_iter =
		Block::new(block_contents.clone(), Arc::clone(&o.internal_comparator)).iter().unwrap();
	forward_iter.seek_to_first().unwrap();
	let mut forward_keys = Vec::new();
	while forward_iter.valid() {
		let key = forward_iter.key().to_owned().user_key.clone();
		forward_keys.push(String::from_utf8(key.clone()).unwrap());
		forward_iter.next().unwrap();
	}
	assert_eq!(forward_keys, vec!["key1", "loooongkey1", "medium_key2", "pkey1", "pkey2", "pkey3"]);

	// Test backward iteration using prev()
	let mut backward_iter =
		Block::new(block_contents, Arc::clone(&o.internal_comparator)).iter().unwrap();
	backward_iter.seek_to_last().unwrap();
	let mut backward_keys = Vec::new();
	while backward_iter.valid() {
		let key = backward_iter.key().user_key();
		backward_keys.push(String::from_utf8(key.to_vec()).unwrap());
		if !backward_iter.prev().unwrap() {
			break;
		}
	}
	assert_eq!(
		backward_keys,
		vec!["pkey3", "pkey2", "pkey1", "medium_key2", "loooongkey1", "key1"]
	);

	// Verify they are complementary
	assert_eq!(forward_keys, backward_keys.iter().rev().cloned().collect::<Vec<_>>());
}

#[test]
fn test_block_prev_from_middle() {
	// Test prev() starting from the middle of the block
	let data = generate_data();
	let o = make_opts(Some(2));
	let mut builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	for &(k, v) in data.iter() {
		builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
	}

	let block_contents = builder.finish().unwrap();
	let mut block_iter =
		Block::new(block_contents, Arc::clone(&o.internal_comparator)).iter().unwrap();

	// Seek to "pkey1"
	let seek_key = InternalKey::new(b"pkey1".to_vec(), 1, InternalKeyKind::Set, 0);
	block_iter.seek(&seek_key.encode()).unwrap();
	assert!(block_iter.valid());
	assert_eq!(block_iter.key().user_key(), "pkey1".as_bytes());

	// Go backward from here
	assert!(block_iter.prev().unwrap());
	assert!(block_iter.valid());
	assert_eq!(block_iter.key().user_key(), "medium_key2".as_bytes());

	assert!(block_iter.prev().unwrap());
	assert!(block_iter.valid());
	assert_eq!(block_iter.key().user_key(), "loooongkey1".as_bytes());

	assert!(block_iter.prev().unwrap());
	assert!(block_iter.valid());
	assert_eq!(block_iter.key().user_key(), "key1".as_bytes());

	// Can't go further back
	assert!(!block_iter.prev().unwrap());
	assert!(!block_iter.valid());
}

#[test]
fn test_block_mixed_next_prev() {
	// Test basic prev() functionality after positioning
	let data = generate_data();
	let o = make_opts(Some(3));
	let mut builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	for &(k, v) in data.iter() {
		builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
	}

	let block_contents = builder.finish().unwrap();
	let mut block_iter =
		Block::new(block_contents, Arc::clone(&o.internal_comparator)).iter().unwrap();

	// Position at "pkey1"
	let seek_key = InternalKey::new(b"pkey1".to_vec(), 1, InternalKeyKind::Set, 0);
	block_iter.seek(&seek_key.encode()).unwrap();
	assert!(block_iter.valid());
	assert_eq!(block_iter.key().user_key(), "pkey1".as_bytes());

	// Go backward
	assert!(block_iter.prev().unwrap());
	assert!(block_iter.valid());
	assert_eq!(block_iter.key().user_key(), "medium_key2".as_bytes());

	assert!(block_iter.prev().unwrap());
	assert!(block_iter.valid());
	assert_eq!(block_iter.key().user_key(), "loooongkey1".as_bytes());

	assert!(block_iter.prev().unwrap());
	assert!(block_iter.valid());
	assert_eq!(block_iter.key().user_key(), "key1".as_bytes());

	// Can't go further
	assert!(!block_iter.prev().unwrap());
	assert!(!block_iter.valid());
}

#[test]
fn test_block_prev_edge_cases() {
	// Test edge cases for prev()
	let data = generate_data();
	let o = make_opts(Some(5)); // Large restart interval
	let mut builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	for &(k, v) in data.iter() {
		builder.add(&make_internal_key(k, InternalKeyKind::Set), v).unwrap();
	}

	let block_contents = builder.finish().unwrap();
	let mut block_iter =
		Block::new(block_contents, Arc::clone(&o.internal_comparator)).iter().unwrap();

	// Test prev() on empty block (shouldn't happen but let's test robustness)
	let empty_block = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	)
	.finish()
	.unwrap();
	let mut empty_iter =
		Block::new(empty_block, Arc::clone(&o.internal_comparator)).iter().unwrap();
	assert!(!empty_iter.prev().unwrap());
	assert!(!empty_iter.valid());

	// Test prev() when at first entry
	block_iter.seek_to_first().unwrap();
	assert!(block_iter.valid());
	assert_eq!(block_iter.key().user_key(), "key1".as_bytes());

	assert!(!block_iter.prev().unwrap()); // Can't go before first
	assert!(!block_iter.valid());

	// Test prev() after seek_to_last()
	block_iter.seek_to_last().unwrap();
	assert!(block_iter.valid());
	assert_eq!(block_iter.key().user_key(), "pkey3".as_bytes());

	// Should be able to go backward
	assert!(block_iter.prev().unwrap());
	assert!(block_iter.valid());
	assert_eq!(block_iter.key().user_key(), "pkey2".as_bytes());
}

#[test]
fn test_block_seek_key_not_found_past_end() {
	// Scenario: Seek for a key greater than all keys in the block
	// Expected: Iterator should be invalid (valid() returns false)
	let o = make_opts(Some(3));
	let mut builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	// Add some keys
	for i in 0..5 {
		let key = format!("key_{:02}", i);
		let value = format!("value_{}", i);
		builder
			.add(&make_internal_key(key.as_bytes(), InternalKeyKind::Set), value.as_bytes())
			.unwrap();
	}

	let block = Block::new(builder.finish().unwrap(), Arc::clone(&o.internal_comparator));
	let mut iter = block.iter().unwrap();

	// Seek for a key that is greater than all keys
	let target = InternalKey::new(b"zzz_past_end".to_vec(), 1, InternalKeyKind::Set, 0);
	iter.seek(&target.encode()).unwrap();

	assert!(!iter.valid(), "Iterator should be invalid when seeking past all keys");
}

#[test]
fn test_block_seek_key_not_found_before_start() {
	// Scenario: Seek for a key less than all keys in the block
	// Expected: Iterator should land on the first key
	let o = make_opts(Some(3));
	let mut builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	// Add keys starting from "bbb"
	for i in 0..5 {
		let key = format!("key_{:02}", i);
		let value = format!("value_{}", i);
		builder
			.add(&make_internal_key(key.as_bytes(), InternalKeyKind::Set), value.as_bytes())
			.unwrap();
	}

	let block = Block::new(builder.finish().unwrap(), Arc::clone(&o.internal_comparator));
	let mut iter = block.iter().unwrap();

	// Seek for a key that is less than all keys
	let target = InternalKey::new(b"aaa_before_all".to_vec(), 1, InternalKeyKind::Set, 0);
	iter.seek(&target.encode()).unwrap();

	assert!(iter.valid(), "Iterator should be valid");
	let found_key = iter.key();
	assert_eq!(found_key.user_key(), b"key_00".to_vec(), "Should land on first key");
}

#[test]
fn test_block_seek_exact_match() {
	// Scenario: Seek for a key that exists exactly
	// Expected: Iterator lands on that exact key
	let o = make_opts(Some(3));
	let mut builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	for i in 0..10 {
		let key = format!("key_{:02}", i);
		let value = format!("value_{}", i);
		builder
			.add(&make_internal_key(key.as_bytes(), InternalKeyKind::Set), value.as_bytes())
			.unwrap();
	}

	let block = Block::new(builder.finish().unwrap(), Arc::clone(&o.internal_comparator));
	let mut iter = block.iter().unwrap();

	// Seek for key_05 which exists
	let target = InternalKey::new(b"key_05".to_vec(), 1, InternalKeyKind::Set, 0);
	iter.seek(&target.encode()).unwrap();

	assert!(iter.valid());
	let found_key = iter.key();
	assert_eq!(found_key.user_key(), b"key_05".to_vec());
}

#[test]
fn test_block_seek_between_keys() {
	// Scenario: Seek for a key that falls between two existing keys
	// Expected: Iterator lands on the first key >= target
	let o = make_opts(Some(3));
	let mut builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	builder.add(&make_internal_key(b"apple", InternalKeyKind::Set), b"v1").unwrap();
	builder.add(&make_internal_key(b"cherry", InternalKeyKind::Set), b"v2").unwrap();
	builder.add(&make_internal_key(b"grape", InternalKeyKind::Set), b"v3").unwrap();

	let block = Block::new(builder.finish().unwrap(), Arc::clone(&o.internal_comparator));
	let mut iter = block.iter().unwrap();

	// Seek for "banana" which is between "apple" and "cherry"
	let target = InternalKey::new(b"banana".to_vec(), 1, InternalKeyKind::Set, 0);
	iter.seek(&target.encode()).unwrap();

	assert!(iter.valid());
	let found_key = iter.key();
	assert_eq!(
		found_key.user_key(),
		b"cherry".to_vec(),
		"Should land on 'cherry' which is first key >= 'banana'"
	);
}

#[test]
fn test_block_seek_same_user_key_different_seq_nums() {
	// Scenario: Multiple versions of the same user key with different seq_nums
	// Verify descending seq_num ordering works correctly
	let o = make_opts(Some(3));
	let mut builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	// Add same user key with descending seq_nums (as stored in SSTable)
	// In InternalKey ordering: (foo, 100) < (foo, 50) < (foo, 1)
	for seq in [100u64, 75, 50, 25, 1] {
		let key = InternalKey::new(b"foo".to_vec(), seq, InternalKeyKind::Set, 0);
		let value = format!("value_seq_{}", seq);
		builder.add(&key.encode(), value.as_bytes()).unwrap();
	}

	let block = Block::new(builder.finish().unwrap(), Arc::clone(&o.internal_comparator));

	// Test 1: Seek for seq=80, should find seq=75 (first key >= (foo, 80))
	let mut iter = block.iter().unwrap();
	let target = InternalKey::new(b"foo".to_vec(), 80, InternalKeyKind::Set, 0);
	iter.seek(&target.encode()).unwrap();
	assert!(iter.valid());
	let found = iter.key();
	assert_eq!(found.user_key(), b"foo".to_vec());
	assert_eq!(
		found.seq_num(),
		75,
		"Seeking for seq=80 should find seq=75 (latest visible version)"
	);

	// Test 2: Seek for seq=50, should find exactly seq=50
	iter.seek(&InternalKey::new(b"foo".to_vec(), 50, InternalKeyKind::Set, 0).encode()).unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().seq_num(), 50);

	// Test 3: Seek for seq=200 (newer than all), should find seq=100
	iter.seek(&InternalKey::new(b"foo".to_vec(), 200, InternalKeyKind::Set, 0).encode()).unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().seq_num(), 100, "Should find newest version seq=100");
}

#[test]
fn test_block_single_restart_point() {
	// Scenario: Block with only 1 entry (1 restart point)
	// Verify binary search handles this edge case
	let o = make_opts(Some(10)); // Large interval, so only 1 restart point
	let mut builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	builder.add(&make_internal_key(b"only_key", InternalKeyKind::Set), b"only_value").unwrap();

	let block = Block::new(builder.finish().unwrap(), Arc::clone(&o.internal_comparator));
	let mut iter = block.iter().unwrap();

	// Seek for the exact key
	let target = InternalKey::new(b"only_key".to_vec(), 1, InternalKeyKind::Set, 0);
	iter.seek(&target.encode()).unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"only_key".to_vec());

	// Seek for key before
	iter.seek(&InternalKey::new(b"aaa".to_vec(), 1, InternalKeyKind::Set, 0).encode()).unwrap();
	assert!(iter.valid());
	assert_eq!(iter.key().user_key(), b"only_key".to_vec());

	// Seek for key after
	iter.seek(&InternalKey::new(b"zzz".to_vec(), 1, InternalKeyKind::Set, 0).encode()).unwrap();
	assert!(!iter.valid(), "Should be invalid when seeking past single entry");
}

#[test]
fn test_block_seek_at_restart_point_boundaries() {
	// Scenario: Seek for keys exactly at restart point boundaries
	let o = make_opts(Some(2)); // Restart every 2 entries
	let mut builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	// Add 6 entries = 3 restart points (at entries 0, 2, 4)
	for i in 0..6 {
		let key = format!("key_{:02}", i);
		builder.add(&make_internal_key(key.as_bytes(), InternalKeyKind::Set), b"value").unwrap();
	}

	let block = Block::new(builder.finish().unwrap(), Arc::clone(&o.internal_comparator));
	let mut iter = block.iter().unwrap();

	// Seek for each restart point key
	for i in [0, 2, 4] {
		let target_key = format!("key_{:02}", i);
		let target = InternalKey::new(target_key.as_bytes().to_vec(), 1, InternalKeyKind::Set, 0);
		iter.seek(&target.encode()).unwrap();
		assert!(iter.valid(), "Should find key at restart point {}", i);
		assert_eq!(iter.key().user_key(), target_key.as_bytes().to_vec());
	}
}

#[test]
fn test_block_iterator_valid_after_exhaustion() {
	// Scenario: Iterate through all entries, then check valid()
	let o = make_opts(Some(3));
	let mut builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	for i in 0..5 {
		builder
			.add(
				&make_internal_key(format!("key_{}", i).as_bytes(), InternalKeyKind::Set),
				b"value",
			)
			.unwrap();
	}

	let block = Block::new(builder.finish().unwrap(), Arc::clone(&o.internal_comparator));
	let mut iter = block.iter().unwrap();

	iter.seek_to_first().unwrap();
	let mut count = 0;
	while iter.valid() {
		count += 1;
		iter.advance().unwrap();
	}
	assert_eq!(count, 5);
	assert!(!iter.valid(), "Iterator should be invalid after exhaustion");
}

#[test]
fn test_seek_to_last_empty_block() {
	// Test that seek_to_last handles empty blocks gracefully
	let o = make_opts(Some(3));
	let builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	// Create empty block (just restart points, no entries)
	let block = Block::new(builder.finish().unwrap(), Arc::clone(&o.internal_comparator));
	let mut iter = block.iter().unwrap();

	iter.seek_to_last().unwrap();

	// Should be invalid, not panic
	assert!(!iter.valid(), "Empty block iterator should be invalid after seek_to_last");
}

#[test]
fn test_prev_at_first_entry() {
	// Test that prev() at first entry correctly returns false
	let o = make_opts(Some(3));
	let mut builder = BlockWriter::new(
		o.block_size,
		o.block_restart_interval,
		Arc::clone(&o.internal_comparator),
	);

	for i in 0..5 {
		let key =
			InternalKey::new(format!("key_{:02}", i).into_bytes(), 1, InternalKeyKind::Set, 0);
		builder.add(&key.encode(), b"value").unwrap();
	}

	let block = Block::new(builder.finish().unwrap(), Arc::clone(&o.internal_comparator));
	let mut iter = block.iter().unwrap();

	iter.seek_to_first().unwrap();
	assert!(iter.valid());

	let first_key = iter.key();
	assert_eq!(first_key.user_key(), b"key_00".to_vec());

	// prev() at first entry should return false
	let result = iter.prev().unwrap();
	assert!(!result, "prev() at first entry should return false");
	assert!(!iter.valid(), "Iterator should be invalid after prev() at first entry");
}
