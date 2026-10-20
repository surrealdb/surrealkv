//! The strategy behind `Tree::compact_range`.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

use super::leveled::Strategy;
use super::{CompactionChoice, CompactionInput, CompactionStrategy};
use crate::levels::LevelManifest;
use crate::sstable::table::Table;
use crate::{Comparator, Result};

/// What [`Tree::compact_range`](crate::Tree::compact_range) does besides compacting the range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactRangeOptions {
	/// Flush the memtables before compacting, so the data they hold is compacted too. Defaults
	/// to `true`.
	pub flush_memtable: bool,
	/// The level the range is merged down to, `None` for the last level. A level that is not the
	/// last keeps its tombstones, because older data may lie below it.
	pub target_level: Option<u8>,
	/// Rewrite the range within the last level, once the data has reached it, to drop the
	/// tombstones and old versions that data already there holds. Defaults to `true`.
	pub rewrite_bottommost: bool,
}

impl Default for CompactRangeOptions {
	fn default() -> Self {
		Self {
			flush_memtable: true,
			target_level: None,
			rewrite_bottommost: true,
		}
	}
}

/// Merges every table of one level that overlaps a user-key range into the next level, or into
/// the same level when `same_level` is set.
///
/// One pick takes all the tables of the level that overlap the range, so a single merge empties
/// the range from that level: the driver steps the source level down the tree and does not loop
/// on a level, which keeps a manual compaction finite under concurrent writes.
///
/// Level 0 holds overlapping tables, newest first, and a table left behind would shadow the
/// merged data in the level below, so any overlap in level 0 takes the whole level.
pub(crate) struct RangeStrategy {
	source_level: u8,
	same_level: bool,
	/// Inclusive lower bound, unbounded when `None`
	start: Option<Vec<u8>>,
	/// Exclusive upper bound, unbounded when `None`
	end: Option<Vec<u8>>,
	comparator: Arc<dyn Comparator>,
}

impl RangeStrategy {
	pub(crate) fn new(
		source_level: u8,
		same_level: bool,
		start: Option<&[u8]>,
		end: Option<&[u8]>,
		comparator: Arc<dyn Comparator>,
	) -> Self {
		Self {
			source_level,
			same_level,
			start: start.map(<[u8]>::to_vec),
			end: end.map(<[u8]>::to_vec),
			comparator,
		}
	}

	/// Whether the user keys of the table intersect `[start, end)`. A table without points holds
	/// no key to compare, so it never intersects.
	fn overlaps(&self, table: &Table) -> bool {
		let (Some(smallest), Some(largest)) =
			(&table.meta.smallest_point, &table.meta.largest_point)
		else {
			return false;
		};
		let before_end = match &self.end {
			Some(end) => {
				self.comparator.compare(smallest.user_key.as_slice(), end) == Ordering::Less
			}
			None => true,
		};
		let after_start = match &self.start {
			Some(start) => {
				self.comparator.compare(largest.user_key.as_slice(), start) != Ordering::Less
			}
			None => true,
		};
		before_end && after_start
	}
}

impl CompactionStrategy for RangeStrategy {
	fn pick_levels(&self, manifest: &LevelManifest) -> Result<CompactionChoice> {
		let levels = manifest.levels.get_levels();
		let last = manifest.last_level_index();
		if self.source_level > last || (!self.same_level && self.source_level >= last) {
			return Ok(CompactionChoice::Skip);
		}

		let source = &levels[self.source_level as usize];
		let seeds: Vec<u64> =
			source.tables.iter().filter(|t| self.overlaps(t)).map(|t| t.id).collect();
		if seeds.is_empty() {
			return Ok(CompactionChoice::Skip);
		}

		let mut chosen: Vec<u64> = Vec::new();
		if self.source_level == 0 {
			chosen.extend(source.tables.iter().map(|t| t.id));
		} else {
			for seed in seeds {
				chosen.extend(Strategy::select_overlapping_ranges(source, seed)?);
			}
		}
		let mut selected: HashSet<u64> = HashSet::new();
		chosen.retain(|id| selected.insert(*id));

		let target_level = if self.same_level {
			self.source_level
		} else {
			self.source_level + 1
		};
		let mut tables_to_merge = chosen;
		if !self.same_level {
			let bounds = Strategy::combined_key_range(
				source.tables.iter().filter(|t| selected.contains(&t.id)),
			);
			if let Some(bounds) = bounds {
				for table in levels[target_level as usize].overlapping_tables(&bounds) {
					if selected.insert(table.id) {
						tables_to_merge.push(table.id);
					}
				}
			}
		}

		Ok(CompactionChoice::Merge(CompactionInput {
			tables_to_merge,
			source_level: self.source_level,
			target_level,
		}))
	}
}
