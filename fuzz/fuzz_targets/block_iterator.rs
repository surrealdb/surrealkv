#![no_main]
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::comparator::{BytewiseComparator, Comparator, InternalKeyComparator};
use surrealkv::sstable::block::{Block, BlockWriter};
use surrealkv::{InternalKey, InternalKeyKind, Options};

#[path = "mod.rs"]
mod helpers;
use helpers::{sort_and_deduplicate_entries, to_internal_key_kind, FuzzEntry};

#[derive(Arbitrary, Debug)]
struct FuzzIteratorInput {
	entries: Vec<FuzzEntryRaw>,
	operations: Vec<IterOp>,
	block_restart_interval: u8,
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
	Seek {
		user_key: Vec<u8>,
		seq_num: u64,
	},
	Next,
	Prev,
	Reset,
}

fuzz_target!(|data: FuzzIteratorInput| {
	if data.entries.len() > 1000 || data.operations.len() > 100 {
		return;
	}

	let mut entries: Vec<FuzzEntry> = data.entries.iter().map(|e| e.to_fuzz_entry()).collect();
	sort_and_deduplicate_entries(&mut entries);

	if entries.is_empty() {
		test_empty_block();
		return;
	}

	let restart_interval = (data.block_restart_interval % 16).max(1) as usize;

	let entry_data: Vec<(Vec<u8>, Vec<u8>)> =
		entries.iter().map(|e| (e.to_internal_key().encode(), e.value.clone())).collect();

	let opts = Options::default();
	let user_cmp = Arc::new(BytewiseComparator::default());
	let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp.clone()));

	let mut builder =
		BlockWriter::new(opts.block_size, restart_interval, Arc::clone(&internal_cmp) as Arc<dyn Comparator>);

	for (key, value) in &entry_data {
		if builder.add(key, value).is_err() {
			return;
		}
	}

	let block_data = match builder.finish() {
		Ok(data) => data,
		Err(_) => return,
	};

	let block = Block::new(block_data, Arc::clone(&internal_cmp) as Arc<dyn Comparator>);
	let mut iter = match block.iter() {
		Ok(it) => it,
		Err(_) => return,
	};
	// After creating iterator, we must call seek_to_first before using
	if iter.seek_to_first().is_err() {
		return;
	}

	// Track direction: None = not set, true = forward, false = backward
	let mut direction: Option<bool> = None;

	for op in &data.operations {
		match op {
			IterOp::SeekToFirst => {
				let _ = iter.seek_to_first();
				direction = Some(true);
				if iter.is_valid() {
					assert_eq!(
						iter.key_bytes(),
						&entry_data[0].0,
						"SeekToFirst should position at first entry"
					);
					verify_key_value(&iter, &entry_data);
				}
			}

			IterOp::SeekToLast => {
				let _ = iter.seek_to_last();
				direction = Some(false);
				if iter.is_valid() {
					assert_eq!(
						iter.key_bytes(),
						&entry_data.last().unwrap().0,
						"SeekToLast should position at last entry"
					);
					verify_key_value(&iter, &entry_data);
				}
			}

			IterOp::Seek {
				user_key,
				seq_num,
			} => {
				let seek_key =
					InternalKey::new(user_key.clone(), *seq_num, InternalKeyKind::Set, 0).encode();
				let _ = iter.seek_internal(&seek_key);
				direction = Some(true);

				if iter.is_valid() {
					assert!(
						internal_cmp.compare(iter.key_bytes(), &seek_key)
							!= std::cmp::Ordering::Less,
						"Seek should position at key >= target"
					);

					let expected_idx = entry_data.iter().position(|(k, _)| {
						internal_cmp.compare(k, &seek_key) != std::cmp::Ordering::Less
					});

					if let Some(idx) = expected_idx {
						assert_eq!(
							iter.key_bytes(),
							&entry_data[idx].0,
							"Seek should find first key >= target"
						);
					}

					verify_key_value(&iter, &entry_data);
				} else {
					assert!(
						entry_data
							.iter()
							.all(|(k, _)| internal_cmp.compare(k, &seek_key)
								== std::cmp::Ordering::Less),
						"Iterator invalid but some keys >= seek target exist"
					);
				}
			}

			IterOp::Next => {
				if direction == Some(false) {
					continue; // Skip - no mixed directions
				}
				direction = Some(true);

				if iter.is_valid() {
					let prev_key = iter.key_bytes().to_vec();
					if let Ok(true) = iter.advance() {
						assert!(
							internal_cmp.compare(&prev_key, iter.key_bytes())
								== std::cmp::Ordering::Less,
							"Forward iteration should produce strictly increasing keys"
						);
						verify_key_value(&iter, &entry_data);
					}
				}
			}

			IterOp::Prev => {
				if direction == Some(true) {
					continue; // Skip - no mixed directions
				}
				direction = Some(false);

				if iter.is_valid() {
					let next_key = iter.key_bytes().to_vec();
					if let Ok(true) = iter.prev_internal() {
						assert!(
							internal_cmp.compare(iter.key_bytes(), &next_key)
								== std::cmp::Ordering::Less,
							"Backward iteration should produce strictly decreasing keys"
						);
						verify_key_value(&iter, &entry_data);
					}
				}
			}

			IterOp::Reset => {
				iter.reset();
				direction = None;
				assert!(!iter.is_valid(), "After reset, iterator should be invalid");
			}
		}
	}

	// Verification passes
	verify_full_forward_iteration(&block, &internal_cmp, &entry_data);
	verify_full_backward_iteration(&block, &internal_cmp, &entry_data);
	verify_seek_boundaries(&block, &internal_cmp, &entry_data);
});

fn verify_key_value(
	iter: &surrealkv::sstable::block::BlockIterator,
	entry_data: &[(Vec<u8>, Vec<u8>)],
) {
	let key = iter.key_bytes();
	if let Some((_, expected_value)) = entry_data.iter().find(|(k, _)| k == key) {
		assert_eq!(iter.value_bytes(), expected_value.as_slice(), "Value mismatch for key");
	} else {
		panic!("Iterator returned key not in original data");
	}
}

fn test_empty_block() {
	let user_cmp = Arc::new(BytewiseComparator::default());
	let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp));

	let builder = BlockWriter::new(4096, 16, Arc::clone(&internal_cmp) as Arc<dyn Comparator>);
	let block_data = builder.finish().expect("Empty block should finish");

	let block = Block::new(block_data, Arc::clone(&internal_cmp) as Arc<dyn Comparator>);
	let mut iter = block.iter().expect("Should create iterator");

	let _ = iter.seek_to_first();
	assert!(!iter.is_valid(), "Empty block iterator should be invalid after seek_to_first");

	let _ = iter.seek_to_last();
	assert!(!iter.is_valid(), "Empty block iterator should be invalid after seek_to_last");
}

fn verify_full_forward_iteration(
	block: &Block,
	internal_cmp: &Arc<InternalKeyComparator>,
	entry_data: &[(Vec<u8>, Vec<u8>)],
) {
	let mut iter = block.iter().expect("Should create iterator");

	if iter.seek_to_first().is_ok() {
		let mut count = 0;
		let mut prev_key: Option<Vec<u8>> = None;

		while iter.is_valid() {
			let curr_key = iter.key_bytes().to_vec();

			if let Some(ref pk) = prev_key {
				assert!(
					internal_cmp.compare(pk, &curr_key) == std::cmp::Ordering::Less,
					"Entries should be in strictly increasing order"
				);
			}

			assert!(
				entry_data.iter().any(|(k, _)| k == &curr_key),
				"Iterated key not in original data"
			);

			let expected_value = &entry_data.iter().find(|(k, _)| k == &curr_key).unwrap().1;
			assert_eq!(iter.value_bytes(), expected_value.as_slice());

			prev_key = Some(curr_key);
			count += 1;

			if iter.advance().is_err() {
				break;
			}
		}

		assert_eq!(count, entry_data.len(), "Should iterate all entries forward");
	}
}

fn verify_full_backward_iteration(
	block: &Block,
	internal_cmp: &Arc<InternalKeyComparator>,
	entry_data: &[(Vec<u8>, Vec<u8>)],
) {
	let mut iter = block.iter().expect("Should create iterator");

	if iter.seek_to_last().is_ok() {
		let mut count = 0;
		let mut next_key: Option<Vec<u8>> = None;

		while iter.is_valid() {
			let curr_key = iter.key_bytes().to_vec();

			if let Some(ref nk) = next_key {
				assert!(
					internal_cmp.compare(&curr_key, nk) == std::cmp::Ordering::Less,
					"Backward iteration should produce decreasing keys"
				);
			}

			assert!(
				entry_data.iter().any(|(k, _)| k == &curr_key),
				"Iterated key not in original data"
			);

			next_key = Some(curr_key);
			count += 1;

			match iter.prev_internal() {
				Ok(true) => continue,
				Ok(false) => break,
				Err(_) => break,
			}
		}

		assert_eq!(count, entry_data.len(), "Should iterate all entries backward");
	}
}

fn verify_seek_boundaries(
	block: &Block,
	internal_cmp: &Arc<InternalKeyComparator>,
	entry_data: &[(Vec<u8>, Vec<u8>)],
) {
	let mut iter = block.iter().expect("Should create iterator");

	// Seek before first key
	let before_first = InternalKey::new(vec![], u64::MAX, InternalKeyKind::Set, 0).encode();
	let _ = iter.seek_internal(&before_first);
	if iter.is_valid() {
		assert_eq!(iter.key_bytes(), &entry_data[0].0, "Seek before first should land on first");
	}

	// Seek after last key
	let after_last = InternalKey::new(vec![0xFF; 100], 0, InternalKeyKind::Set, 0).encode();
	let _ = iter.seek_internal(&after_last);
	if iter.is_valid() {
		assert!(
			internal_cmp.compare(iter.key_bytes(), &after_last) != std::cmp::Ordering::Less,
			"Seek result should be >= target"
		);
	}

	// Seek to exact keys
	for (key, _) in entry_data {
		let _ = iter.seek_internal(key);
		assert!(iter.is_valid(), "Seek to existing key should succeed");
		assert_eq!(iter.key_bytes(), key, "Seek to exact key should find that key");
	}
}
