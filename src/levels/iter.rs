use std::sync::Arc;

use super::LevelManifest;
use crate::sstable::table::Table;

/// Iterates through all levels
pub(crate) struct LevelManifestIterator<'a> {
	level_manifest: &'a LevelManifest,
	current_level: usize,
	current_idx: usize,
}

impl<'a> LevelManifestIterator<'a> {
	#[must_use]
	pub(crate) fn new(level_manifest: &'a LevelManifest) -> Self {
		Self {
			level_manifest,
			current_idx: 0,
			current_level: 0,
		}
	}
}

impl Iterator for LevelManifestIterator<'_> {
	type Item = Arc<Table>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let table = self
				.level_manifest
				.levels
				.as_ref()
				.get(self.current_level)?
				.tables
				.get(self.current_idx)
				.cloned();

			if let Some(table) = table {
				self.current_idx += 1;
				return Some(table);
			}

			self.current_level += 1;
			self.current_idx = 0;
		}
	}
}
