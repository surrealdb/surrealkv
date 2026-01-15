use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use crate::clock::LogicalClock;
use crate::error::Result;
use crate::sstable::{InternalIterator, InternalKey, InternalKeyRef};
use crate::vlog::{VLog, ValueLocation, ValuePointer};
use crate::{Comparator, Value};

/// Boxed internal iterator type for dynamic dispatch
pub type BoxedInternalIterator<'a> = Box<dyn InternalIterator + 'a>;

// ============================================================================
// MergingIterator - Zero-allocation k-way merge using loser tree
// ============================================================================

/// Direction of iteration
#[derive(Clone, Copy, PartialEq)]
enum Direction {
	Forward,
	Backward,
}

/// Merge iterator level - owns iterator, no key caching
struct MergeLevel<'a> {
	iter: BoxedInternalIterator<'a>,
	valid: bool,
}

/// Loser tree for k-way merge with zero key allocations.
///
/// During iteration, keys are fetched on-demand from source iterators.
/// The tree only stores indices, not keys.
pub(crate) struct MergingIterator<'a> {
	levels: Vec<MergeLevel<'a>>,
	/// Winner index from the last tournament
	winner: usize,
	/// Number of active (non-exhausted) levels
	active_count: usize,
	/// Comparator for keys
	cmp: Arc<dyn Comparator>,
	/// Direction (forward/backward)
	direction: Direction,
}

impl<'a> MergingIterator<'a> {
	pub fn new(iterators: Vec<BoxedInternalIterator<'a>>, cmp: Arc<dyn Comparator>) -> Self {
		let levels: Vec<_> = iterators
			.into_iter()
			.map(|iter| MergeLevel {
				iter,
				valid: false,
			})
			.collect();

		Self {
			levels,
			winner: 0,
			active_count: 0,
			cmp,
			direction: Direction::Forward,
		}
	}

	/// Compare two levels by their current key (zero-copy)
	#[inline]
	fn compare(&self, a: usize, b: usize) -> Ordering {
		let level_a = &self.levels[a];
		let level_b = &self.levels[b];

		match (level_a.valid, level_b.valid) {
			(false, false) => Ordering::Equal,
			(true, false) => Ordering::Less, // a wins (valid beats invalid)
			(false, true) => Ordering::Greater, // b wins
			(true, true) => {
				// Both valid - compare keys (zero-copy from iterators)
				let key_a = level_a.iter.key().encoded();
				let key_b = level_b.iter.key().encoded();
				let ord = self.cmp.compare(key_a, key_b);
				if self.direction == Direction::Backward {
					ord.reverse()
				} else {
					ord
				}
			}
		}
	}

	/// Find the minimum (or maximum for backward) among all valid levels
	fn find_winner(&mut self) {
		if self.levels.is_empty() || self.active_count == 0 {
			return;
		}

		// Simple linear search for the winner
		let mut best_idx = None;
		for i in 0..self.levels.len() {
			if !self.levels[i].valid {
				continue;
			}
			match best_idx {
				None => best_idx = Some(i),
				Some(b) => {
					if self.compare(i, b) == Ordering::Less {
						best_idx = Some(i);
					}
				}
			}
		}

		if let Some(idx) = best_idx {
			self.winner = idx;
		}
	}

	/// Initialize for forward iteration
	fn init_forward(&mut self) -> Result<()> {
		self.direction = Direction::Forward;
		self.active_count = 0;

		// Position all iterators at first
		for level in &mut self.levels {
			level.valid = level.iter.seek_first()?;
			if level.valid {
				self.active_count += 1;
			}
		}

		self.find_winner();
		Ok(())
	}

	/// Initialize for backward iteration
	fn init_backward(&mut self) -> Result<()> {
		self.direction = Direction::Backward;
		self.active_count = 0;

		// Position all iterators at last
		for level in &mut self.levels {
			level.valid = level.iter.seek_last()?;
			if level.valid {
				self.active_count += 1;
			}
		}

		self.find_winner();
		Ok(())
	}

	/// Advance the current winner and find new winner
	fn advance_winner(&mut self) -> Result<bool> {
		if self.active_count == 0 {
			return Ok(false);
		}

		let winner_idx = self.winner;
		let level = &mut self.levels[winner_idx];

		// Advance the winning iterator
		level.valid = if self.direction == Direction::Forward {
			level.iter.next()?
		} else {
			level.iter.prev()?
		};

		if !level.valid {
			self.active_count = self.active_count.saturating_sub(1);
		}

		// Find new winner
		self.find_winner();

		Ok(self.active_count > 0 && self.levels[self.winner].valid)
	}

	/// Check if iterator is valid
	pub fn is_valid(&self) -> bool {
		self.active_count > 0 && self.levels.get(self.winner).map_or(false, |l| l.valid)
	}

	/// Get current winner's key (zero-copy)
	pub fn current_key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.is_valid());
		self.levels[self.winner].iter.key()
	}

	/// Get current winner's value (zero-copy)
	pub fn current_value(&self) -> &[u8] {
		debug_assert!(self.is_valid());
		self.levels[self.winner].iter.value()
	}
}

impl InternalIterator for MergingIterator<'_> {
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.direction = Direction::Forward;
		self.active_count = 0;

		for level in &mut self.levels {
			level.valid = level.iter.seek(target)?;
			if level.valid {
				self.active_count += 1;
			}
		}

		self.find_winner();
		Ok(self.is_valid())
	}

	fn seek_first(&mut self) -> Result<bool> {
		self.init_forward()?;
		Ok(self.is_valid())
	}

	fn seek_last(&mut self) -> Result<bool> {
		self.init_backward()?;
		Ok(self.is_valid())
	}

	fn next(&mut self) -> Result<bool> {
		if !self.is_valid() {
			return Ok(false);
		}
		self.advance_winner()
	}

	fn prev(&mut self) -> Result<bool> {
		if !self.is_valid() {
			return Ok(false);
		}
		// If we were going forward, need to switch direction
		if self.direction != Direction::Backward {
			// For simplicity, re-initialize for backward
			return self.seek_last();
		}
		self.advance_winner()
	}

	fn valid(&self) -> bool {
		self.is_valid()
	}

	fn key(&self) -> InternalKeyRef<'_> {
		self.current_key()
	}

	fn value(&self) -> &[u8] {
		self.current_value()
	}
}

// ============================================================================
// CompactionIterator - Uses MergingIterator for compaction
// ============================================================================

fn collect_vlog_discard_stats(discard_stats: &mut HashMap<u32, i64>, value: &[u8]) -> Result<()> {
	// Skip empty values (e.g., hard delete entries)
	if value.is_empty() {
		return Ok(());
	}

	// Check if this is a ValueLocation
	let location = ValueLocation::decode(value)?;
	if location.is_value_pointer() {
		let pointer = ValuePointer::decode(&location.value)?;
		let value_data_size = pointer.total_entry_size() as i64;
		*discard_stats.entry(pointer.file_id).or_insert(0) += value_data_size;
	}
	Ok(())
}

pub(crate) struct CompactionIterator<'a> {
	merge_iter: MergingIterator<'a>,
	is_bottom_level: bool,

	// Track the current key being processed
	current_user_key: Vec<u8>,

	// Buffer for accumulating all versions of the current key
	accumulated_versions: Vec<(InternalKey, Value)>,

	// Buffer for outputting the versions of the current key
	output_versions: Vec<(InternalKey, Value)>,

	// Compaction state
	/// Collected discard statistics: file_id -> total_discarded_bytes
	pub discard_stats: HashMap<u32, i64>,

	/// Reference to VLog for populating delete-list
	vlog: Option<Arc<VLog>>,

	/// Batch of stale entries to add to delete-list: (sequence_number,
	/// value_size)
	pub(crate) delete_list_batch: Vec<(u64, u64)>,

	/// Versioning configuration
	enable_versioning: bool,
	retention_period_ns: u64,

	/// Logical clock for time-based operations
	clock: Arc<dyn LogicalClock>,

	initialized: bool,
}

impl<'a> CompactionIterator<'a> {
	pub(crate) fn new(
		iterators: Vec<BoxedInternalIterator<'a>>,
		cmp: Arc<dyn Comparator>,
		is_bottom_level: bool,
		vlog: Option<Arc<VLog>>,
		enable_versioning: bool,
		retention_period_ns: u64,
		clock: Arc<dyn LogicalClock>,
	) -> Self {
		let merge_iter = MergingIterator::new(iterators, cmp);

		Self {
			merge_iter,
			is_bottom_level,
			current_user_key: Vec::new(),
			accumulated_versions: Vec::new(),
			output_versions: Vec::new(),
			discard_stats: HashMap::new(),
			vlog,
			delete_list_batch: Vec::new(),
			enable_versioning,
			retention_period_ns,
			clock,
			initialized: false,
		}
	}

	fn initialize(&mut self) -> Result<()> {
		self.merge_iter.seek_first()?;
		self.initialized = true;
		Ok(())
	}

	/// Flushes the batched delete-list entries to the VLog
	pub(crate) fn flush_delete_list_batch(&mut self) -> Result<()> {
		if let Some(ref vlog) = self.vlog {
			if !self.delete_list_batch.is_empty() {
				vlog.add_batch_to_delete_list(std::mem::take(&mut self.delete_list_batch))?;
			}
		}
		Ok(())
	}

	/// Process all accumulated versions of the current key
	/// Filters out stale entries and populates output_versions with valid
	/// entries
	fn process_accumulated_versions(&mut self) {
		if self.accumulated_versions.is_empty() {
			return;
		}

		// Sort by sequence number (descending) to get the latest version first
		self.accumulated_versions.sort_by(|a, b| b.0.seq_num().cmp(&a.0.seq_num()));

		// Check if latest version is DELETE at bottom level - if so, discard entire key
		let latest_is_delete_at_bottom = self.is_bottom_level
			&& !self.accumulated_versions.is_empty()
			&& self.accumulated_versions[0].0.is_hard_delete_marker();

		// Check if any version is Replace - if so, mark all older versions as stale
		let has_set_with_delete = self.accumulated_versions.iter().any(|(key, _)| key.is_replace());

		// Process all versions for delete list and determine which to keep
		for (i, (key, value)) in self.accumulated_versions.iter().enumerate() {
			let is_hard_delete = key.is_hard_delete_marker();
			let is_replace = key.is_replace();
			let is_latest = i == 0;

			// Determine if this entry should be marked as stale in VLog
			let should_mark_stale = if latest_is_delete_at_bottom {
				// If latest version is DELETE at bottom level, mark ALL versions as stale
				true
			} else if is_latest && !is_hard_delete && !is_replace {
				// Latest version of a regular SET operation: never mark as stale (it's being
				// returned)
				false
			} else if is_latest && is_hard_delete && self.is_bottom_level {
				// Latest version of a DELETE operation at bottom level: mark as stale (not
				// returned)
				true
			} else if is_latest && is_hard_delete && !self.is_bottom_level {
				// Latest version of a DELETE operation at non-bottom level: don't mark as stale
				// (it's being returned)
				false
			} else if is_latest && is_replace {
				// Latest version of a Replace operation: don't mark as stale (it's being
				// returned)
				false
			} else if is_hard_delete {
				// For older DELETE operations (hard delete entries): always mark as stale since
				// they don't have VLog values
				true
			} else if has_set_with_delete && !is_replace {
				// If there's a Replace operation, mark all older non-Replace versions as stale
				true
			} else {
				// Older version of a SET operation: check retention period
				if !self.enable_versioning {
					// Versioning disabled: mark older versions as stale
					true
				} else if self.retention_period_ns > 0 {
					// Versioning enabled with retention period: check if within retention
					let current_time = self.clock.now();
					let key_timestamp = key.timestamp;
					let age = current_time - key_timestamp;
					let is_within_retention = age <= self.retention_period_ns;
					// Mark as stale only if NOT within retention period
					!is_within_retention
				} else {
					// Versioning enabled with retention_period_ns == 0: keep all versions forever
					false
				}
			};

			// Add to delete list and collect discard stats if needed
			if should_mark_stale && self.vlog.is_some() {
				if key.is_tombstone() {
					// Hard Delete: add key size to delete list
					self.delete_list_batch.push((key.seq_num(), key.size() as u64));
				} else {
					let location = ValueLocation::decode(value).unwrap();
					if location.is_value_pointer() {
						let pointer = ValuePointer::decode(&location.value).unwrap();
						let value_size = pointer.total_entry_size();
						self.delete_list_batch.push((key.seq_num(), value_size));
					}
				}

				// Collect discard statistics
				if let Err(e) = collect_vlog_discard_stats(&mut self.discard_stats, value) {
					log::warn!("Error collecting discard stats: {e:?}");
				}
			}

			// Determine if this version should be kept for output
			let should_output = if latest_is_delete_at_bottom {
				// If latest version is DELETE at bottom level, output NOTHING for this key
				false
			} else if should_mark_stale {
				// Stale entries: don't output
				false
			} else if self.enable_versioning {
				// Versioning enabled: output all non-stale versions
				true
			} else {
				// Versioning disabled: only output the latest version
				is_latest
			};

			if should_output {
				self.output_versions.push((key.clone(), value.clone()));
			}
		}

		// Clear accumulated versions for the next key
		self.accumulated_versions.clear();
	}

	/// Advance to the next output entry
	pub fn advance(&mut self) -> Result<Option<(InternalKey, Value)>> {
		if !self.initialized {
			self.initialize()?;
		}

		loop {
			// First, return any pending output versions
			if !self.output_versions.is_empty() {
				// Remove from front to maintain sequence number order (already sorted
				// descending)
				return Ok(Some(self.output_versions.remove(0)));
			}

			// Get the next item from the merge iterator
			if !self.merge_iter.is_valid() {
				// No more items, process any remaining accumulated versions
				if !self.accumulated_versions.is_empty() {
					self.process_accumulated_versions();
					// Return first output version if any
					if !self.output_versions.is_empty() {
						return Ok(Some(self.output_versions.remove(0)));
					}
				}
				return Ok(None);
			}

			// Get current key and value from merge iterator (extract to owned to avoid borrow
			// issues)
			let key_owned = self.merge_iter.current_key().to_owned();
			let user_key_owned = key_owned.user_key.clone();
			let value_owned = self.merge_iter.current_value().to_vec();

			// Check if this is a new user key
			let is_new_key =
				self.current_user_key.is_empty() || user_key_owned != self.current_user_key;

			if is_new_key {
				// Process accumulated versions of the previous key
				if !self.accumulated_versions.is_empty() {
					self.process_accumulated_versions();

					// Start accumulating the new key
					self.current_user_key = user_key_owned;
					self.accumulated_versions.push((key_owned, value_owned));

					// Advance merge iterator for next iteration
					self.merge_iter.next()?;

					// Return first output version from processed key if any
					if !self.output_versions.is_empty() {
						return Ok(Some(self.output_versions.remove(0)));
					}
				} else {
					// First key - start accumulating
					self.current_user_key = user_key_owned;
					self.accumulated_versions.push((key_owned, value_owned));

					// Advance merge iterator for next iteration
					self.merge_iter.next()?;
				}
			} else {
				// Same user key - add to accumulated versions
				self.accumulated_versions.push((key_owned, value_owned));

				// Advance merge iterator for next iteration
				self.merge_iter.next()?;
			}
		}
	}
}

impl Iterator for CompactionIterator<'_> {
	type Item = Result<(InternalKey, Value)>;

	fn next(&mut self) -> Option<Self::Item> {
		match self.advance() {
			Ok(Some(item)) => Some(Ok(item)),
			Ok(None) => None,
			Err(e) => Some(Err(e)),
		}
	}
}
