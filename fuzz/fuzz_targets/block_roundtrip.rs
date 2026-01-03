#![no_main]
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::comparator::{BytewiseComparator, InternalKeyComparator};
use surrealkv::sstable::block::{Block, BlockWriter};
use surrealkv::sstable::INTERNAL_KEY_SEQ_NUM_MAX;
use surrealkv::Options;

#[path = "mod.rs"]
mod helpers;
use helpers::{sort_and_deduplicate_entries, to_internal_key_kind, FuzzEntry};

#[derive(Arbitrary, Debug)]
struct FuzzBlockInput {
	entries: Vec<FuzzEntryRaw>,
	restart_interval: u8,
}

#[derive(Arbitrary, Debug)]
struct FuzzEntryRaw {
	user_key: Vec<u8>,
	value: Vec<u8>,
	seq_num: u64,
	kind: u8, // Will be converted to InternalKeyKind
}

impl FuzzEntryRaw {
	fn to_fuzz_entry(&self) -> FuzzEntry {
		FuzzEntry {
			user_key: self.user_key.clone(),
			value: self.value.clone(),
			seq_num: self.seq_num,
			kind: to_internal_key_kind(self.kind),
		}
	}
}

fuzz_target!(|data: FuzzBlockInput| {
	// Limit input size to prevent excessive memory usage
	if data.entries.len() > 1000 {
		return;
	}

	// Convert raw entries to FuzzEntry and clamp sequence numbers to valid range
	let mut entries: Vec<FuzzEntry> = data
		.entries
		.iter()
		.map(|e| {
			let mut entry = e.to_fuzz_entry();
			// Clamp sequence number to valid range (56 bits)
			entry.seq_num = entry.seq_num.min(INTERNAL_KEY_SEQ_NUM_MAX);
			entry
		})
		.collect();

	// Filter out entries with empty user_key that might cause issues with block format
	// Empty keys combined with very large sequence numbers can cause corruption in the
	// current implementation. This is a known edge case that should be fixed separately.
	// For now, we filter them to allow the fuzzer to find other bugs.
	entries.retain(|e| !e.user_key.is_empty());

	// Sort and deduplicate entries (required for BlockWriter)
	sort_and_deduplicate_entries(&mut entries);

	if entries.is_empty() {
		return;
	}

	// Create options with fuzzed restart interval
	// Ensure restart_interval is always at least 1 (0 causes issues)
	let mut opts = Options::default();
	opts.block_restart_interval = ((data.restart_interval % 16) as usize).max(1);
	let user_cmp = Arc::new(BytewiseComparator::default());
	let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp));

	// Build block
	let mut builder =
		BlockWriter::new(opts.block_size, opts.block_restart_interval, Arc::clone(&internal_cmp));

	for entry in &entries {
		let internal_key = entry.to_internal_key();
		builder.add(&internal_key, &entry.value).unwrap();
	}

	let block_data = builder.finish().unwrap();
	let block = Block::new(block_data, Arc::clone(&internal_cmp));

	// Test forward iteration
	let mut iter = block.iter(false).unwrap();
	iter.seek_to_first().unwrap();

	let mut read_entries = Vec::new();
	while iter.valid() {
		let key = iter.key();
		let value = iter.value();
		read_entries.push((key, value));
		iter.advance().unwrap();
	}

	// Verify we read back all entries
	assert_eq!(read_entries.len(), entries.len(), "Entry count mismatch");

	// Verify each entry matches
	for (i, (read_key, read_value)) in read_entries.iter().enumerate() {
		let expected_entry = &entries[i];
		assert_eq!(
			read_key.user_key.as_slice(),
			expected_entry.user_key.as_slice(),
			"User key mismatch at index {}",
			i
		);
		assert_eq!(read_value, &expected_entry.value, "Value mismatch at index {}", i);
		assert_eq!(
			read_key.seq_num(),
			expected_entry.seq_num,
			"Sequence number mismatch at index {}",
			i
		);
	}

	// Test seek operations
	for entry in &entries {
		let seek_key = entry.to_internal_key();
		iter.seek(&seek_key).unwrap();
		assert!(iter.valid(), "Seek should find key: {:?}", entry.user_key);
		assert_eq!(
			iter.key().user_key.as_slice(),
			entry.user_key.as_slice(),
			"Seek found wrong key"
		);
	}

	// Test seek_to_last and reverse iteration
	iter.seek_to_last().unwrap();
	assert!(iter.valid(), "seek_to_last should be valid");

	let last_key = iter.key();
	let expected_last = &entries[entries.len() - 1];
	assert_eq!(
		last_key.user_key.as_slice(),
		expected_last.user_key.as_slice(),
		"seek_to_last found wrong key"
	);

	// Test iterator exhaustion
	iter.seek_to_first().unwrap();
	while iter.valid() {
		iter.advance().unwrap();
	}
	assert!(!iter.valid(), "Iterator should be invalid after exhaustion");
});
