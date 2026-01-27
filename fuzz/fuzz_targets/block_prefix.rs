#![no_main]
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::comparator::{BytewiseComparator, Comparator, InternalKeyComparator};
use surrealkv::sstable::block::{Block, BlockWriter};
use surrealkv::{InternalIterator, InternalKey, InternalKeyKind};

#[path = "mod.rs"]
mod helpers;
use helpers::{sort_and_deduplicate_entries, FuzzEntry};

/// Input designed to stress prefix compression
#[derive(Arbitrary, Debug)]
struct PrefixCompressionInput {
	/// Common prefix for all keys
	common_prefix: Vec<u8>,
	/// Different suffix patterns to test
	suffix_pattern: SuffixPattern,
	/// Restart interval
	restart_interval: u8,
	/// Operations to perform
	operations: Vec<PrefixOp>,
	/// Keys only mode
	keys_only: bool,
}

#[derive(Arbitrary, Debug)]
enum SuffixPattern {
	/// All identical suffixes (maximum prefix sharing)
	Identical {
		suffix: Vec<u8>,
		count: u8,
		seq_nums: Vec<u64>,
	},
	/// Incrementing single byte suffix
	IncrementingByte {
		start: u8,
		count: u8,
	},
	/// Hierarchical paths like file systems: prefix/a, prefix/a/b, prefix/a/b/c
	Hierarchical {
		segments: Vec<Vec<u8>>,
		depth: u8,
	},
	/// Long suffixes with small differences
	LongWithSmallDiff {
		base_suffix: Vec<u8>,
		diff_positions: Vec<u8>,
		count: u8,
	},
	/// Random suffixes (minimal prefix sharing after common prefix)
	Random {
		suffixes: Vec<Vec<u8>>,
	},
	/// Alternating between two prefixes (tests prefix truncation)
	Alternating {
		suffix_a: Vec<u8>,
		suffix_b: Vec<u8>,
		count: u8,
	},
	/// Gradually diverging suffixes
	GradualDivergence {
		base: Vec<u8>,
		divergence_points: Vec<(u8, u8)>, // (position, new_byte)
	},
}

#[derive(Arbitrary, Debug)]
enum PrefixOp {
	SeekToFirst,
	SeekToLast,
	/// Seek with prefix that matches common prefix exactly
	SeekExactPrefix,
	/// Seek with prefix that's shorter than common prefix
	SeekShorterPrefix(u8),
	/// Seek with prefix that's longer than common prefix
	SeekLongerPrefix(Vec<u8>),
	/// Seek to specific suffix
	SeekToSuffix(u8),
	/// Seek between two keys
	SeekBetween(u8, u8),
	Next,
	Prev,
	/// Iterate and verify prefix compression worked
	VerifyPrefixCompression,
}

fuzz_target!(|data: PrefixCompressionInput| {
	// Limit prefix size to avoid OOM
	if data.common_prefix.len() > 1024 {
		return;
	}

	let restart_interval = (data.restart_interval % 16).max(1) as usize;

	// Generate entries based on suffix pattern
	let entries = generate_entries_from_pattern(&data.common_prefix, &data.suffix_pattern);

	if entries.is_empty() || entries.len() > 1000 {
		return;
	}

	// Sort and deduplicate
	let mut entries = entries;
	sort_and_deduplicate_entries(&mut entries);

	if entries.is_empty() {
		return;
	}

	// Build entry data
	let entry_data: Vec<(Vec<u8>, Vec<u8>)> =
		entries.iter().map(|e| (e.to_internal_key().encode(), e.value.clone())).collect();

	let user_cmp = Arc::new(BytewiseComparator::default());
	let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp.clone()));

	let mut builder = BlockWriter::new(65536, restart_interval, Arc::clone(&internal_cmp));

	for (key, value) in &entry_data {
		if builder.add(key, value).is_err() {
			return;
		}
	}

	// Get size before finish for compression analysis
	let estimated_size = builder.size_estimate();

	let block_data = match builder.finish() {
		Ok(data) => data,
		Err(_) => return,
	};

	let actual_size = block_data.len();

	// Calculate theoretical uncompressed size
	let uncompressed_size: usize = entry_data
		.iter()
		.map(|(k, v)| k.len() + v.len() + 12) // 12 bytes overhead estimate per entry
		.sum();

	// INVARIANT: Compressed size should generally be <= uncompressed
	// (may not always hold for very small blocks due to restart point overhead)
	if entry_data.len() > restart_interval * 2 {
		assert!(
			actual_size <= uncompressed_size + 100, // Allow small overhead
			"Block size {} should not significantly exceed uncompressed size {}",
			actual_size,
			uncompressed_size
		);
	}

	let block = Block::new(block_data, Arc::clone(&internal_cmp));
	let mut iter = block.iter().expect("Should create iterator");
	let mut direction: Option<bool> = None;

	for op in &data.operations {
		match op {
			PrefixOp::SeekToFirst => {
				let _ = iter.seek_to_first();
				direction = Some(true);
				if iter.is_valid() {
					assert_eq!(iter.key_bytes(), &entry_data[0].0);
					verify_key_has_prefix(&iter, &data.common_prefix);
					verify_key_value(&iter, &entry_data, data.keys_only);
				}
			}

			PrefixOp::SeekToLast => {
				let _ = iter.seek_to_last();
				direction = Some(false);
				if iter.is_valid() {
					assert_eq!(iter.key_bytes(), &entry_data.last().unwrap().0);
					verify_key_has_prefix(&iter, &data.common_prefix);
					verify_key_value(&iter, &entry_data, data.keys_only);
				}
			}

			PrefixOp::SeekExactPrefix => {
				// Seek with just the common prefix
				let seek_key =
					InternalKey::new(data.common_prefix.clone(), u64::MAX, InternalKeyKind::Set, 0)
						.encode();

				let _ = iter.seek(&seek_key);
				direction = Some(true);

				if iter.is_valid() {
					// Should land on first key with this prefix or greater
					assert!(
						internal_cmp.compare(iter.key_bytes(), &seek_key)
							!= std::cmp::Ordering::Less,
						"Seek result should be >= target"
					);
					verify_key_value(&iter, &entry_data, data.keys_only);
				}
			}

			PrefixOp::SeekShorterPrefix(len) => {
				let prefix_len = (*len as usize) % (data.common_prefix.len().max(1));
				let short_prefix = data.common_prefix[..prefix_len].to_vec();

				let seek_key =
					InternalKey::new(short_prefix, u64::MAX, InternalKeyKind::Set, 0).encode();

				let _ = iter.seek(&seek_key);
				direction = Some(true);

				if iter.is_valid() {
					assert!(
						internal_cmp.compare(iter.key_bytes(), &seek_key)
							!= std::cmp::Ordering::Less,
						"Seek result should be >= target"
					);
					verify_key_value(&iter, &entry_data, data.keys_only);
				}
			}

			PrefixOp::SeekLongerPrefix(extra) => {
				if extra.len() > 256 {
					continue;
				}
				let mut long_prefix = data.common_prefix.clone();
				long_prefix.extend(extra);

				let seek_key =
					InternalKey::new(long_prefix, u64::MAX, InternalKeyKind::Set, 0).encode();

				let _ = iter.seek(&seek_key);
				direction = Some(true);

				// May or may not find a match depending on suffixes
				if iter.is_valid() {
					assert!(
						internal_cmp.compare(iter.key_bytes(), &seek_key)
							!= std::cmp::Ordering::Less,
						"Seek result should be >= target"
					);
					verify_key_value(&iter, &entry_data, data.keys_only);
				}
			}

			PrefixOp::SeekToSuffix(idx) => {
				let entry_idx = (*idx as usize) % entry_data.len();
				let target_key = &entry_data[entry_idx].0;

				let _ = iter.seek(target_key);
				direction = Some(true);

				if iter.is_valid() {
					assert_eq!(iter.key_bytes(), target_key);
					verify_key_value(&iter, &entry_data, data.keys_only);
				}
			}

			PrefixOp::SeekBetween(idx1, idx2) => {
				let i1 = (*idx1 as usize) % entry_data.len();
				let i2 = (*idx2 as usize) % entry_data.len();

				if i1 >= i2 || i2 - i1 < 2 {
					continue;
				}

				// Create a key between entry[i1] and entry[i2]
				let key1 = &entry_data[i1].0;
				let key2 = &entry_data[i1 + 1].0;

				// Try to create a key between them by modifying key1
				let between_user_key = InternalKey::decode(key1).user_key;
				let mut modified = between_user_key.clone();
				if !modified.is_empty() {
					// Increment last byte if possible
					let last_idx = modified.len() - 1;
					if modified[last_idx] < 255 {
						modified[last_idx] += 1;
					}
				}

				let seek_key =
					InternalKey::new(modified, u64::MAX, InternalKeyKind::Set, 0).encode();

				let _ = iter.seek(&seek_key);
				direction = Some(true);

				if iter.is_valid() {
					// Should land on key2 or later (first key >= seek_key)
					assert!(
						internal_cmp.compare(iter.key_bytes(), &seek_key)
							!= std::cmp::Ordering::Less,
						"Seek result should be >= target"
					);
					verify_key_value(&iter, &entry_data, data.keys_only);
				}
			}

			PrefixOp::Next => {
				if direction == Some(false) {
					continue;
				}
				direction = Some(true);

				if iter.is_valid() {
					let prev_key = iter.key_bytes().to_vec();
					if let Ok(true) = iter.advance() {
						assert!(
							internal_cmp.compare(&prev_key, iter.key_bytes())
								== std::cmp::Ordering::Less,
							"Keys should increase"
						);
						verify_key_has_prefix(&iter, &data.common_prefix);
						verify_key_value(&iter, &entry_data, data.keys_only);
					}
				}
			}

			PrefixOp::Prev => {
				if direction == Some(true) {
					continue;
				}
				direction = Some(false);

				if iter.is_valid() {
					let next_key = iter.key_bytes().to_vec();
					if let Ok(true) = iter.prev() {
						assert!(
							internal_cmp.compare(iter.key_bytes(), &next_key)
								== std::cmp::Ordering::Less,
							"Keys should decrease going backward"
						);
						verify_key_has_prefix(&iter, &data.common_prefix);
						verify_key_value(&iter, &entry_data, data.keys_only);
					}
				}
			}

			PrefixOp::VerifyPrefixCompression => {
				// Iterate through all entries and verify prefix is correctly reconstructed
				let mut verify_iter = block.iter().expect("Should create iterator");
				let _ = verify_iter.seek_to_first();

				let mut idx = 0;
				while verify_iter.is_valid() {
					let key = verify_iter.key_bytes();

					// INVARIANT: Every key should have the common prefix
					verify_key_has_prefix(&verify_iter, &data.common_prefix);

					// INVARIANT: Key should match expected
					assert_eq!(
						key, &entry_data[idx].0,
						"Key mismatch at index {} - prefix compression may have corrupted key",
						idx
					);

					if !data.keys_only {
						assert_eq!(
							verify_iter.value_bytes(),
							&entry_data[idx].1,
							"Value mismatch at index {}",
							idx
						);
					}

					idx += 1;
					if verify_iter.advance().is_err() {
						break;
					}
				}

				assert_eq!(idx, entry_data.len(), "Should verify all entries");
			}
		}
	}

	// Final verification
	verify_full_iteration(&block, &internal_cmp, &entry_data, &data.common_prefix, data.keys_only);
});

fn generate_entries_from_pattern(common_prefix: &[u8], pattern: &SuffixPattern) -> Vec<FuzzEntry> {
	match pattern {
		SuffixPattern::Identical {
			suffix,
			count,
			seq_nums,
		} => {
			let count = (*count).min(100) as usize;
			let mut user_key = common_prefix.to_vec();
			user_key.extend(suffix);

			(0..count)
				.map(|i| {
					let seq = seq_nums.get(i).copied().unwrap_or(i as u64);
					FuzzEntry {
						user_key: user_key.clone(),
						value: format!("value_{}", i).into_bytes(),
						seq_num: seq,
						kind: InternalKeyKind::Set,
					}
				})
				.collect()
		}

		SuffixPattern::IncrementingByte {
			start,
			count,
		} => {
			let count = (*count).min(255) as usize;
			(0..count)
				.map(|i| {
					let mut user_key = common_prefix.to_vec();
					user_key.push(start.wrapping_add(i as u8));
					FuzzEntry {
						user_key,
						value: format!("value_{}", i).into_bytes(),
						seq_num: i as u64,
						kind: InternalKeyKind::Set,
					}
				})
				.collect()
		}

		SuffixPattern::Hierarchical {
			segments,
			depth,
		} => {
			let depth = (*depth).min(10) as usize;
			let mut entries = Vec::new();

			fn generate_paths(
				prefix: &[u8],
				segments: &[Vec<u8>],
				current_depth: usize,
				max_depth: usize,
				entries: &mut Vec<FuzzEntry>,
				seq: &mut u64,
			) {
				if current_depth > max_depth || segments.is_empty() {
					return;
				}

				for seg in segments.iter().take(5) {
					let mut path = prefix.to_vec();
					path.push(b'/');
					path.extend(seg);

					entries.push(FuzzEntry {
						user_key: path.clone(),
						value: format!("value_at_depth_{}", current_depth).into_bytes(),
						seq_num: *seq,
						kind: InternalKeyKind::Set,
					});
					*seq += 1;

					generate_paths(&path, segments, current_depth + 1, max_depth, entries, seq);
				}
			}

			let mut seq = 0u64;
			generate_paths(common_prefix, segments, 0, depth, &mut entries, &mut seq);
			entries
		}

		SuffixPattern::LongWithSmallDiff {
			base_suffix,
			diff_positions,
			count,
		} => {
			let count = (*count).min(50) as usize;

			(0..count)
				.map(|i| {
					let mut user_key = common_prefix.to_vec();
					let mut suffix = base_suffix.clone();

					// Apply small differences at specified positions
					for &pos in diff_positions.iter().take(i + 1) {
						let pos = (pos as usize) % suffix.len().max(1);
						if pos < suffix.len() {
							suffix[pos] = suffix[pos].wrapping_add(1);
						}
					}

					user_key.extend(&suffix);
					FuzzEntry {
						user_key,
						value: format!("value_{}", i).into_bytes(),
						seq_num: i as u64,
						kind: InternalKeyKind::Set,
					}
				})
				.collect()
		}

		SuffixPattern::Random {
			suffixes,
		} => suffixes
			.iter()
			.take(100)
			.enumerate()
			.map(|(i, suffix)| {
				let mut user_key = common_prefix.to_vec();
				user_key.extend(suffix);
				FuzzEntry {
					user_key,
					value: format!("value_{}", i).into_bytes(),
					seq_num: i as u64,
					kind: InternalKeyKind::Set,
				}
			})
			.collect(),

		SuffixPattern::Alternating {
			suffix_a,
			suffix_b,
			count,
		} => {
			let count = (*count).min(100) as usize;

			(0..count)
				.map(|i| {
					let mut user_key = common_prefix.to_vec();
					// Add index to ensure uniqueness
					user_key.push((i / 2) as u8);
					if i % 2 == 0 {
						user_key.extend(suffix_a);
					} else {
						user_key.extend(suffix_b);
					}
					FuzzEntry {
						user_key,
						value: format!("value_{}", i).into_bytes(),
						seq_num: i as u64,
						kind: InternalKeyKind::Set,
					}
				})
				.collect()
		}

		SuffixPattern::GradualDivergence {
			base,
			divergence_points,
		} => {
			let mut entries = Vec::new();

			// First entry with base suffix
			let mut user_key = common_prefix.to_vec();
			user_key.extend(base);
			entries.push(FuzzEntry {
				user_key: user_key.clone(),
				value: b"value_base".to_vec(),
				seq_num: 0,
				kind: InternalKeyKind::Set,
			});

			// Each subsequent entry diverges at a new point
			let mut current_suffix = base.clone();
			for (i, &(pos, new_byte)) in divergence_points.iter().take(50).enumerate() {
				let pos = (pos as usize) % current_suffix.len().max(1);
				if pos < current_suffix.len() {
					current_suffix[pos] = new_byte;
				} else {
					current_suffix.push(new_byte);
				}

				let mut user_key = common_prefix.to_vec();
				user_key.extend(&current_suffix);
				entries.push(FuzzEntry {
					user_key,
					value: format!("value_diverge_{}", i).into_bytes(),
					seq_num: (i + 1) as u64,
					kind: InternalKeyKind::Set,
				});
			}

			entries
		}
	}
}

fn verify_key_has_prefix(iter: &surrealkv::sstable::block::BlockIterator, expected_prefix: &[u8]) {
	let key = iter.key_bytes();
	let internal_key = InternalKey::decode(key);
	let user_key = &internal_key.user_key;

	// User key should start with the common prefix
	assert!(
		user_key.starts_with(expected_prefix),
		"Key {:?} should start with prefix {:?}",
		user_key,
		expected_prefix
	);
}

fn verify_key_value(
	iter: &surrealkv::sstable::block::BlockIterator,
	entry_data: &[(Vec<u8>, Vec<u8>)],
	keys_only: bool,
) {
	let key = iter.key_bytes();
	if let Some((_, expected_value)) = entry_data.iter().find(|(k, _)| k == key) {
		if !keys_only {
			assert_eq!(iter.value_bytes(), expected_value.as_slice(), "Value mismatch for key");
		}
	} else {
		panic!("Iterator returned key not in original data");
	}
}

fn verify_full_iteration(
	block: &Block,
	internal_cmp: &Arc<InternalKeyComparator>,
	entry_data: &[(Vec<u8>, Vec<u8>)],
	common_prefix: &[u8],
	keys_only: bool,
) {
	let mut iter = block.iter().expect("Should create iterator");

	if iter.seek_to_first().is_err() {
		return;
	}

	let mut count = 0;
	let mut prev_key: Option<Vec<u8>> = None;

	while iter.is_valid() {
		let curr_key = iter.key_bytes().to_vec();

		// Verify ordering
		if let Some(ref pk) = prev_key {
			assert!(
				internal_cmp.compare(pk, &curr_key) == std::cmp::Ordering::Less,
				"Entries should be strictly increasing"
			);
		}

		// Verify prefix
		verify_key_has_prefix(&iter, common_prefix);

		// Verify key matches expected
		assert_eq!(&curr_key, &entry_data[count].0, "Key mismatch at position {}", count);

		// Verify value
		if !keys_only {
			assert_eq!(
				iter.value_bytes(),
				&entry_data[count].1,
				"Value mismatch at position {}",
				count
			);
		}

		prev_key = Some(curr_key);
		count += 1;

		if iter.advance().is_err() {
			break;
		}
	}

	assert_eq!(count, entry_data.len(), "Should iterate all entries");
}
