use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{CompactionChoice, CompactionInput, CompactionStrategy};
use crate::levels::{Level, LevelManifest};
use crate::InternalKeyRange;

/// Compaction priority strategy for selecting files to compact
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompactionPriority {
	#[allow(unused)]
	/// Files whose range hasn't been compacted for the longest
	OldestSmallestSeqFirst,

	/// Files whose latest update is oldest
	#[allow(unused)]
	OldestLargestSeqFirst,

	/// Larger files compensated by deletes (default)
	#[default]
	ByCompensatedSize,
}

pub(crate) struct Strategy {
	// Base size for L0 (in number of tables)
	base_level_size: usize,
	// Size multiplier between levels
	size_multiplier: usize,
	// Track the last compacted level for round-robin selection
	last_compacted_level: AtomicUsize,
	// Compaction priority strategy
	compaction_priority: CompactionPriority,
}

impl Default for Strategy {
	fn default() -> Self {
		Self {
			base_level_size: 4,
			size_multiplier: 10,
			last_compacted_level: AtomicUsize::new(0),
			compaction_priority: CompactionPriority::default(),
		}
	}
}

impl Strategy {
	#[cfg(test)]
	pub(crate) fn new(base_level_size: usize, size_multiplier: usize) -> Self {
		Self {
			base_level_size,
			size_multiplier,
			last_compacted_level: AtomicUsize::new(0),
			compaction_priority: CompactionPriority::default(),
		}
	}

	fn calculate_level_size_limit(&self, level: u8) -> usize {
		if level == 0 {
			return self.base_level_size;
		}
		self.base_level_size * self.size_multiplier.pow(level as u32)
	}

	pub(crate) fn select_tables_for_compaction(
		&self,
		source_level: &Level,
		next_level: &Level,
		source_level_num: u8,
	) -> Vec<u64> {
		let mut tables = vec![];
		let mut table_id_set = HashSet::new();

		if source_level.tables.is_empty() {
			return tables;
		}

		if source_level_num == 0 {
			// L0 → L1: Pick all L0 files since they can overlap
			for table in &source_level.tables {
				if table_id_set.insert(table.id) {
					tables.push(table.id);
				}
			}
		} else {
			// L1+ → L(n+1): Select best table for compaction
			let selected_table_id = self.select_best_table_for_compaction(source_level);
			if let Some(table_id) = selected_table_id {
				table_id_set.insert(table_id);
				tables.push(table_id);
			}
		}

		// Get bounds from selected source tables using smallest_point/largest_point
		let source_tables: Vec<_> = if source_level_num == 0 {
			source_level.tables.iter().collect()
		} else {
			// For L1+, use the table(s) we actually selected by ID
			source_level.tables.iter().filter(|t| table_id_set.contains(&t.id)).collect()
		};

		// Build InternalKeyRange from smallest_point/largest_point
		let source_bounds: Option<InternalKeyRange> = source_tables
			.iter()
			.filter_map(|t| {
				let smallest = t.meta.smallest_point.as_ref()?;
				let largest = t.meta.largest_point.as_ref()?;
				Some((smallest.clone(), largest.clone()))
			})
			.fold(None, |acc, (s, l)| {
				use std::ops::Bound;
				match acc {
					None => Some((Bound::Included(s), Bound::Included(l))),
					Some((Bound::Included(acc_s), Bound::Included(acc_l))) => {
						let new_s = if s.user_key < acc_s.user_key {
							s
						} else {
							acc_s
						};
						let new_l = if l.user_key > acc_l.user_key {
							l
						} else {
							acc_l
						};
						Some((Bound::Included(new_s), Bound::Included(new_l)))
					}
					_ => acc,
				}
			});

		// Find overlapping tables from next level
		if let Some(bounds) = source_bounds {
			for table in next_level.overlapping_tables(&bounds) {
				if table_id_set.insert(table.id) {
					tables.push(table.id);
				}
			}
		}

		tables
	}

	fn select_best_table_for_compaction(&self, source_level: &Level) -> Option<u64> {
		if source_level.tables.is_empty() {
			return None;
		}

		match self.compaction_priority {
			CompactionPriority::OldestSmallestSeqFirst => {
				self.select_oldest_smallest_seq_first(source_level)
			}
			CompactionPriority::OldestLargestSeqFirst => {
				self.select_oldest_largest_seq_first(source_level)
			}
			CompactionPriority::ByCompensatedSize => self.select_by_compensated_size(source_level),
		}
	}

	/// Selects ranges that haven't been compacted for longest
	fn select_oldest_smallest_seq_first(&self, source_level: &Level) -> Option<u64> {
		if source_level.tables.is_empty() {
			return None;
		}

		#[derive(Debug)]
		struct Choice {
			table_id: u64,
			smallest_seq: u64,
			file_size: u64,
		}

		let mut choices = Vec::new();

		for source_table in &source_level.tables {
			let source_size = source_table.file_size;
			let smallest_seq = source_table.meta.properties.seqnos.0;

			choices.push(Choice {
				table_id: source_table.id,
				smallest_seq,
				file_size: source_size,
			});
		}

		// Sort by oldest smallest sequence number first, then by file size (larger
		// first)
		choices.sort_by(|a, b| match a.smallest_seq.cmp(&b.smallest_seq) {
			std::cmp::Ordering::Equal => b.file_size.cmp(&a.file_size),
			ordering => ordering,
		});

		choices.first().map(|choice| choice.table_id)
	}

	/// Selects files whose latest update is oldest (cold data)
	fn select_oldest_largest_seq_first(&self, source_level: &Level) -> Option<u64> {
		if source_level.tables.is_empty() {
			return None;
		}

		#[derive(Debug)]
		struct Choice {
			table_id: u64,
			largest_seq: u64,
			file_size: u64,
		}

		let mut choices = Vec::new();

		for source_table in &source_level.tables {
			let source_size = source_table.file_size;
			let largest_seq = source_table.meta.properties.seqnos.1;

			choices.push(Choice {
				table_id: source_table.id,
				largest_seq,
				file_size: source_size,
			});
		}

		// Sort by oldest largest sequence number first (coldest ranges), then by file
		// size
		choices.sort_by(|a, b| match a.largest_seq.cmp(&b.largest_seq) {
			std::cmp::Ordering::Equal => b.file_size.cmp(&a.file_size),
			ordering => ordering,
		});

		choices.first().map(|choice| choice.table_id)
	}

	/// Selects files based on compensated size
	pub(crate) fn select_by_compensated_size(&self, source_level: &Level) -> Option<u64> {
		if source_level.tables.is_empty() {
			return None;
		}

		#[derive(Debug)]
		struct Choice {
			table_id: u64,
			compensated_size: f64,
		}

		let mut choices = Vec::new();

		for source_table in &source_level.tables {
			let file_size = source_table.file_size;
			let num_entries = source_table.meta.properties.num_entries;
			let num_deletions = source_table.meta.properties.num_deletions;

			// Calculate compensated size:
			// Base file size, adjusted upward by delete ratio
			let compensated_size = if num_entries > 0 && num_deletions > 0 {
				let delete_ratio = num_deletions as f64 / num_entries as f64;
				file_size as f64 * (1.0 + delete_ratio * 0.5)
			} else {
				file_size as f64
			};

			choices.push(Choice {
				table_id: source_table.id,
				compensated_size,
			});
		}

		// Sort by compensated size (descending)
		choices.sort_by(|a, b| {
			b.compensated_size.partial_cmp(&a.compensated_size).unwrap_or(std::cmp::Ordering::Equal)
		});

		choices.first().map(|choice| choice.table_id)
	}

	pub(crate) fn find_compaction_level(&self, manifest: &LevelManifest) -> Option<u8> {
		let levels = manifest.levels.get_levels();
		let last_level_index = manifest.last_level_index();

		// L0 gets highest priority - check first
		// L0 files can overlap, so they accumulate quickly and block reads
		if levels[0].tables.len() >= self.calculate_level_size_limit(0) {
			return Some(0);
		}

		// Track last compacted level for round-robin fairness
		// This prevents always picking the same level when multiple need compaction
		let start = self.last_compacted_level.load(Ordering::Relaxed);
		let mut candidates: Vec<u8> = Vec::new();

		// Find all levels that exceed their size limits
		for level in 1..=last_level_index {
			let current_size = levels[level as usize].tables.len();
			let size_limit = self.calculate_level_size_limit(level);
			if current_size >= size_limit {
				candidates.push(level);
			}
		}

		// No levels need compaction
		if candidates.is_empty() {
			return None;
		}

		// Round-robin selection: pick the first candidate after our last compacted
		// level
		for level in &candidates {
			if *level as usize > start {
				self.last_compacted_level.store(*level as usize, Ordering::Relaxed);
				return Some(*level);
			}
		}

		// All candidates are <= start, so wrap around and pick the first one
		let level = candidates[0];
		self.last_compacted_level.store(level as usize, Ordering::Relaxed);
		Some(level)
	}
}

impl CompactionStrategy for Strategy {
	fn pick_levels(&self, manifest: &LevelManifest) -> CompactionChoice {
		let source_level = match self.find_compaction_level(manifest) {
			Some(level) => level,
			None => return CompactionChoice::Skip,
		};

		let levels = manifest.levels.get_levels();

		if source_level >= manifest.last_level_index() {
			return CompactionChoice::Skip;
		}

		let tables_to_merge = self.select_tables_for_compaction(
			&levels[source_level as usize],
			&levels[(source_level + 1) as usize],
			source_level,
		);

		if tables_to_merge.is_empty() {
			return CompactionChoice::Skip;
		}

		CompactionChoice::Merge(CompactionInput {
			tables_to_merge,
			source_level,
			target_level: source_level + 1,
		})
	}
}
