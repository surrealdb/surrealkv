#![no_main]
use std::sync::Arc;

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use surrealkv::comparator::{BytewiseComparator, Comparator, InternalKeyComparator};
use surrealkv::sstable::block::{Block, BlockWriter};
use surrealkv::LSMIterator;

#[path = "mod.rs"]
mod helpers;
use helpers::{sort_and_deduplicate_entries, to_internal_key_kind, FuzzEntry};

/// Input designed to stress restart point boundaries
#[derive(Arbitrary, Debug)]
struct RestartBoundaryInput {
	/// Restart interval (will be mapped to 1-16)
	restart_interval: u8,
	/// Number of entries relative to restart interval
	entry_count_mode: EntryCountMode,
	/// Base entries to generate keys from
	base_entries: Vec<RestartBoundaryEntry>,
	/// Operations to perform, focused on boundary positions
	operations: Vec<RestartBoundaryOp>,
	/// Whether to use keys_only mode
	keys_only: bool,
}

#[derive(Arbitrary, Debug)]
enum EntryCountMode {
	/// Exactly at restart interval boundary: n * interval
	ExactMultiple(u8),
	/// One less than boundary: n * interval - 1
	OneLess(u8),
	/// One more than boundary: n * interval + 1
	OneMore(u8),
	/// Arbitrary count from entries
	Arbitrary,
}

#[derive(Arbitrary, Debug)]
struct RestartBoundaryEntry {
	user_key: Vec<u8>,
	value: Vec<u8>,
	seq_num: u64,
	kind: u8,
}

#[derive(Arbitrary, Debug)]
enum RestartBoundaryOp {
	/// Seek to first entry of a restart point
	SeekToRestartPoint(u8),
	/// Seek to last entry before a restart point
	SeekBeforeRestartPoint(u8),
	/// Iterate forward across a restart boundary
	IterateAcrossRestartForward(u8),
	/// Iterate backward across a restart boundary
	IterateAcrossRestartBackward(u8),
	/// Seek to entry at specific position relative to restart
	SeekToPosition(PositionRelativeToRestart),
	/// Standard operations
	SeekToFirst,
	SeekToLast,
	SeekExact(u8), // Index into entries
	Next,
	Prev,
}

#[derive(Arbitrary, Debug)]
struct PositionRelativeToRestart {
	restart_index: u8,
	offset_from_restart: i8, // Can be negative (before) or positive (after)
}

fuzz_target!(|data: RestartBoundaryInput| {
	let restart_interval = (data.restart_interval % 16).max(1) as usize;

	// Convert and sort entries
	let mut entries: Vec<FuzzEntry> = data
		.base_entries
		.iter()
		.map(|e| FuzzEntry {
			user_key: e.user_key.clone(),
			value: e.value.clone(),
			seq_num: e.seq_num,
			kind: to_internal_key_kind(e.kind),
		})
		.collect();

	sort_and_deduplicate_entries(&mut entries);

	if entries.is_empty() {
		return;
	}

	// Adjust entry count based on mode
	let target_count = match data.entry_count_mode {
		EntryCountMode::ExactMultiple(n) => {
			let n = (n % 5).max(1) as usize;
			n * restart_interval
		}
		EntryCountMode::OneLess(n) => {
			let n = (n % 5).max(1) as usize;
			(n * restart_interval).saturating_sub(1).max(1)
		}
		EntryCountMode::OneMore(n) => {
			let n = (n % 5).max(1) as usize;
			n * restart_interval + 1
		}
		EntryCountMode::Arbitrary => entries.len(),
	};

	// Truncate or keep entries based on target
	let entries: Vec<FuzzEntry> = entries.into_iter().take(target_count.min(500)).collect();

	if entries.is_empty() {
		return;
	}

	// Build entry data
	let entry_data: Vec<(Vec<u8>, Vec<u8>)> =
		entries.iter().map(|e| (e.to_internal_key().encode(), e.value.clone())).collect();

	let user_cmp = Arc::new(BytewiseComparator::default());
	let internal_cmp = Arc::new(InternalKeyComparator::new(user_cmp.clone()));

	let mut builder = BlockWriter::new(65536, restart_interval, Arc::clone(&internal_cmp) as Arc<dyn Comparator>);

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
	let mut iter = block.iter().expect("Should create iterator");

	// Calculate restart point positions
	let num_restarts = (entry_data.len() + restart_interval - 1) / restart_interval;
	let restart_positions: Vec<usize> = (0..num_restarts).map(|i| i * restart_interval).collect();

	// Track direction
	let mut direction: Option<bool> = None;

	for op in &data.operations {
		match op {
			RestartBoundaryOp::SeekToRestartPoint(idx) => {
				if restart_positions.is_empty() {
					continue;
				}
				let restart_idx = (*idx as usize) % restart_positions.len();
				let entry_idx = restart_positions[restart_idx];

				if entry_idx < entry_data.len() {
					let target_key = &entry_data[entry_idx].0;
					let _ = iter.seek(target_key);
					direction = Some(true);

					if iter.valid() {
						// INVARIANT: Should land exactly on restart point entry
						assert_eq!(
							iter.key_bytes(),
							target_key,
							"Seek to restart point should land exactly on that entry"
						);

						// Verify this is actually at a restart boundary by checking
						// that we can iterate backward and forward correctly
						verify_key_value(&iter, &entry_data, data.keys_only);
					}
				}
			}

			RestartBoundaryOp::SeekBeforeRestartPoint(idx) => {
				if restart_positions.len() < 2 {
					continue;
				}
				// Skip first restart point (index 0), seek to entry just before restart
				let restart_idx = ((*idx as usize) % (restart_positions.len() - 1)) + 1;
				let entry_before_restart = restart_positions[restart_idx].saturating_sub(1);

				if entry_before_restart < entry_data.len() {
					let target_key = &entry_data[entry_before_restart].0;
					let _ = iter.seek(target_key);
					direction = Some(true);

					if iter.valid() {
						assert_eq!(
							iter.key_bytes(),
							target_key,
							"Seek before restart should land on correct entry"
						);

						// INVARIANT: Next should cross into new restart region
						if let Ok(true) = iter.advance() {
							let next_entry_idx = entry_before_restart + 1;
							if next_entry_idx < entry_data.len() {
								assert_eq!(
									iter.key_bytes(),
									&entry_data[next_entry_idx].0,
									"Next after seek before restart should give correct entry"
								);

								// This entry should be at a restart point
								assert!(
									restart_positions.contains(&next_entry_idx),
									"Entry after restart boundary should be at restart point"
								);
							}
						}
					}
				}
			}

			RestartBoundaryOp::IterateAcrossRestartForward(idx) => {
				if restart_positions.len() < 2 {
					continue;
				}

				let restart_idx = ((*idx as usize) % (restart_positions.len() - 1)) + 1;
				let boundary_entry = restart_positions[restart_idx];

				if boundary_entry == 0 || boundary_entry >= entry_data.len() {
					continue;
				}

				// Position at entry before restart boundary
				let start_entry = boundary_entry - 1;
				let _ = iter.seek(&entry_data[start_entry].0);
				direction = Some(true);

				if !iter.valid() {
					continue;
				}

				// Iterate forward across the boundary
				let mut prev_key = iter.key_bytes().to_vec();
				let mut crossed = false;

				for expected_idx in (start_entry + 1)..entry_data.len().min(start_entry + 4) {
					if let Ok(true) = iter.advance() {
						let curr_key = iter.key_bytes();

						// INVARIANT: Keys must be strictly increasing
						assert!(
							internal_cmp.compare(&prev_key, curr_key) == std::cmp::Ordering::Less,
							"Keys must increase across restart boundary"
						);

						// INVARIANT: Should match expected entry
						assert_eq!(
							curr_key, &entry_data[expected_idx].0,
							"Entry mismatch when iterating across restart boundary"
						);

						verify_key_value(&iter, &entry_data, data.keys_only);

						if expected_idx == boundary_entry {
							crossed = true;
						}

						prev_key = curr_key.to_vec();
					} else {
						break;
					}
				}

				if boundary_entry < entry_data.len() {
					assert!(crossed, "Should have crossed restart boundary");
				}
			}

			RestartBoundaryOp::IterateAcrossRestartBackward(idx) => {
				if restart_positions.len() < 2 {
					continue;
				}

				let restart_idx = ((*idx as usize) % (restart_positions.len() - 1)) + 1;
				let boundary_entry = restart_positions[restart_idx];

				if boundary_entry >= entry_data.len() {
					continue;
				}

				// Position at restart boundary entry
				let _ = iter.seek(&entry_data[boundary_entry].0);
				direction = Some(false);

				if !iter.valid() {
					continue;
				}

				// Iterate backward across the boundary
				let mut next_key = iter.key_bytes().to_vec();
				let mut crossed = false;

				let start_check = boundary_entry.saturating_sub(1);
				for expected_idx in (boundary_entry.saturating_sub(3)..=start_check).rev() {
					match iter.prev() {
						Ok(true) => {
							let curr_key = iter.key_bytes();

							// INVARIANT: Keys must be strictly decreasing (backward)
							assert!(
                                internal_cmp.compare(curr_key, &next_key) == std::cmp::Ordering::Less,
                                "Keys must decrease when iterating backward across restart boundary"
                            );

							// INVARIANT: Should match expected entry
							assert_eq!(
								curr_key, &entry_data[expected_idx].0,
								"Entry mismatch when iterating backward across restart boundary"
							);

							verify_key_value(&iter, &entry_data, data.keys_only);

							if expected_idx == boundary_entry - 1 {
								crossed = true;
							}

							next_key = curr_key.to_vec();
						}
						Ok(false) => break,
						Err(_) => break,
					}
				}

				if boundary_entry > 0 {
					assert!(crossed, "Should have crossed restart boundary backward");
				}
			}

			RestartBoundaryOp::SeekToPosition(pos) => {
				if restart_positions.is_empty() {
					continue;
				}

				let restart_idx = (pos.restart_index as usize) % restart_positions.len();
				let base_pos = restart_positions[restart_idx];

				// Safe handling of negative offset - avoid i8::MIN negation overflow
				let target_pos = match pos.offset_from_restart.cmp(&0) {
					std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => {
						base_pos.saturating_add(pos.offset_from_restart as usize)
					}
					std::cmp::Ordering::Less => {
						// Convert to i32 first to safely handle i8::MIN (-128)
						let abs_offset = (pos.offset_from_restart as i32).unsigned_abs() as usize;
						base_pos.saturating_sub(abs_offset)
					}
				};

				if target_pos < entry_data.len() {
					let target_key = &entry_data[target_pos].0;
					let _ = iter.seek(target_key);
					direction = Some(true);

					if iter.valid() {
						assert_eq!(
							iter.key_bytes(),
							target_key,
							"Seek to position should land on correct entry"
						);
						verify_key_value(&iter, &entry_data, data.keys_only);
					}
				}
			}

			RestartBoundaryOp::SeekToFirst => {
				let _ = iter.seek_to_first();
				direction = Some(true);
				if iter.valid() {
					assert_eq!(iter.key_bytes(), &entry_data[0].0);
					verify_key_value(&iter, &entry_data, data.keys_only);
				}
			}

			RestartBoundaryOp::SeekToLast => {
				let _ = iter.seek_to_last();
				direction = Some(false);
				if iter.valid() {
					assert_eq!(iter.key_bytes(), &entry_data.last().unwrap().0);
					verify_key_value(&iter, &entry_data, data.keys_only);
				}
			}

			RestartBoundaryOp::SeekExact(idx) => {
				let entry_idx = (*idx as usize) % entry_data.len();
				let target_key = &entry_data[entry_idx].0;
				let _ = iter.seek(target_key);
				direction = Some(true);

				if iter.valid() {
					assert_eq!(iter.key_bytes(), target_key);
					verify_key_value(&iter, &entry_data, data.keys_only);
				}
			}

			RestartBoundaryOp::Next => {
				if direction == Some(false) {
					continue;
				}
				direction = Some(true);

				if iter.valid() {
					let prev_key = iter.key_bytes().to_vec();
					if let Ok(true) = iter.advance() {
						assert!(
							internal_cmp.compare(&prev_key, iter.key_bytes())
								== std::cmp::Ordering::Less,
							"Keys should increase"
						);
						verify_key_value(&iter, &entry_data, data.keys_only);
					}
				}
			}

			RestartBoundaryOp::Prev => {
				if direction == Some(true) {
					continue;
				}
				direction = Some(false);

				if iter.valid() {
					let next_key = iter.key_bytes().to_vec();
					if let Ok(true) = iter.prev() {
						assert!(
							internal_cmp.compare(iter.key_bytes(), &next_key)
								== std::cmp::Ordering::Less,
							"Keys should decrease going backward"
						);
						verify_key_value(&iter, &entry_data, data.keys_only);
					}
				}
			}
		}
	}

	// Final verification: ensure all entries accessible via full iteration
	verify_full_iteration_with_restart_check(
		&block,
		&internal_cmp,
		&entry_data,
		&restart_positions,
		restart_interval,
		data.keys_only,
	);
});

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

fn verify_full_iteration_with_restart_check(
	block: &Block,
	internal_cmp: &Arc<InternalKeyComparator>,
	entry_data: &[(Vec<u8>, Vec<u8>)],
	restart_positions: &[usize],
	restart_interval: usize,
	keys_only: bool,
) {
	let mut iter = block.iter().expect("Should create iterator");

	if iter.seek_to_first().is_err() {
		return;
	}

	let mut count = 0;
	let mut prev_key: Option<Vec<u8>> = None;
	let mut entries_since_restart = 0;

	while iter.valid() {
		let curr_key = iter.key_bytes().to_vec();

		// Verify ordering
		if let Some(ref pk) = prev_key {
			assert!(
				internal_cmp.compare(pk, &curr_key) == std::cmp::Ordering::Less,
				"Entries should be strictly increasing"
			);
		}

		// Verify this is the expected entry
		assert_eq!(&curr_key, &entry_data[count].0, "Entry mismatch at position {}", count);

		// Check restart point logic
		if restart_positions.contains(&count) {
			entries_since_restart = 0;
		}
		entries_since_restart += 1;

		// INVARIANT: entries_since_restart should never exceed restart_interval + 1
		// (the +1 accounts for the restart entry itself)
		assert!(
			entries_since_restart <= restart_interval + 1,
			"Too many entries since last restart point"
		);

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
