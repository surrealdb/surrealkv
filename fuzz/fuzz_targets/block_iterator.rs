#![no_main]
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::comparator::{BytewiseComparator, Comparator, InternalKeyComparator};
use surrealkv::sstable::block::{Block, BlockWriter};
use surrealkv::sstable::{InternalKey, InternalKeyKind};
use surrealkv::Options;

#[path = "mod.rs"]
mod helpers;
use helpers::{sort_and_deduplicate_entries, to_internal_key_kind, FuzzEntry};

#[derive(Arbitrary, Debug)]
struct FuzzIteratorInput {
	entries: Vec<FuzzEntryRaw>,
	operations: Vec<IterOp>,
}

#[derive(Arbitrary, Debug)]
struct FuzzEntryRaw {
	user_key: Vec<u8>,
	value: Vec<u8>,
	seq_num: u64,
	kind: u8,
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

#[derive(Arbitrary, Debug)]
enum IterOp {
	SeekToFirst,
	SeekToLast,
	Seek(Vec<u8>),
	Next,
	Prev,
}

fuzz_target!(|data: FuzzIteratorInput| {
	// Limit input size
	if data.entries.len() > 1000 || data.operations.len() > 100 {
		return;
	}

	// Convert and sort entries
	let mut entries: Vec<FuzzEntry> = data.entries.iter().map(|e| e.to_fuzz_entry()).collect();
	sort_and_deduplicate_entries(&mut entries);

	if entries.is_empty() {
		return;
	}

	// Build the encoded internal keys for verification
	let internal_keys: Vec<Vec<u8>> = entries.iter().map(|e| e.to_internal_key()).collect();

	// Create block
	let opts = Options::default();
	let user_cmp = Arc::new(BytewiseComparator::default());
	let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp.clone()));

	let mut builder =
		BlockWriter::new(opts.block_size, opts.block_restart_interval, Arc::clone(&internal_cmp));

	for entry in &entries {
		let internal_key = entry.to_internal_key();
		if builder.add(&internal_key, &entry.value).is_err() {
			return; // Key ordering issue, skip this input
		}
	}

	let block_data = match builder.finish() {
		Ok(data) => data,
		Err(_) => return,
	};

	let block = Block::new(block_data, Arc::clone(&internal_cmp));
	let mut iter = match block.iter(false) {
		Ok(it) => it,
		Err(_) => return,
	};

	// Track last key for ordering verification
	let mut last_key: Option<Vec<u8>> = None;
	let mut direction: Option<bool> = None; // true = forward, false = backward

	// Execute operations
	for op in &data.operations {
		match op {
			IterOp::SeekToFirst => {
				let _ = iter.seek_to_first();
				direction = Some(true);
				last_key = if iter.valid() {
					Some(iter.key_bytes().to_vec())
				} else {
					None
				};
			}
			IterOp::SeekToLast => {
				let _ = iter.seek_to_last();
				direction = Some(false);
				last_key = if iter.valid() {
					Some(iter.key_bytes().to_vec())
				} else {
					None
				};
			}
			IterOp::Seek(key_bytes) => {
				let seek_key =
					InternalKey::new(key_bytes.clone(), 0, InternalKeyKind::Set, 0).encode();
				let _ = iter.seek(&seek_key);
				direction = Some(true);
				last_key = if iter.valid() {
					Some(iter.key_bytes().to_vec())
				} else {
					None
				};
			}
			IterOp::Next => {
				if direction == Some(false) {
					continue; // Can't mix directions
				}
				direction = Some(true);

				if iter.valid() {
					let prev_key = iter.key_bytes().to_vec();
					if let Ok(true) = iter.advance() {
						// INVARIANT: keys should be strictly increasing
						let curr_key = iter.key_bytes();
						assert!(
							internal_cmp.compare(&prev_key, curr_key) == std::cmp::Ordering::Less,
							"Forward iteration should produce strictly increasing keys"
						);
						last_key = Some(curr_key.to_vec());
					} else {
						last_key = None;
					}
				}
			}
			IterOp::Prev => {
				if direction == Some(true) {
					continue; // Can't mix directions
				}
				direction = Some(false);

				if iter.valid() {
					let next_key = iter.key_bytes().to_vec();
					if let Ok(true) = iter.prev() {
						// INVARIANT: keys should be strictly decreasing (going backward)
						let curr_key = iter.key_bytes();
						assert!(
							internal_cmp.compare(curr_key, &next_key) == std::cmp::Ordering::Less,
							"Backward iteration should produce strictly decreasing keys"
						);
						last_key = Some(curr_key.to_vec());
					} else {
						last_key = None;
					}
				}
			}
		}
	}

	// Verify full forward iteration returns all entries in order
	if let Ok(()) = iter.seek_to_first() {
		let mut count = 0;
		let mut prev_key: Option<Vec<u8>> = None;

		while iter.valid() {
			let curr_key = iter.key_bytes().to_vec();

			// Verify ordering
			if let Some(ref pk) = prev_key {
				assert!(
					internal_cmp.compare(pk, &curr_key) == std::cmp::Ordering::Less,
					"Entries should be in strictly increasing order"
				);
			}

			prev_key = Some(curr_key);
			count += 1;

			if iter.advance().is_err() {
				break;
			}
		}

		// All entries should be iterable
		assert_eq!(count, entries.len(), "Should iterate all entries");
	}
});
