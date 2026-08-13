use std::sync::Arc;

use super::LevelManifest;
use crate::sstable::table::Table;

/// Lifecycle-only iterator over every table of every owner's level set.
/// Read paths select one owner via `LevelManifest::levels_for` and must not
/// use this iterator.
pub(crate) struct LevelManifestIterator<'a> {
	level_manifest: &'a LevelManifest,
	current_owner: usize,
	current_level: usize,
	current_idx: usize,
}

impl<'a> LevelManifestIterator<'a> {
	#[must_use]
	pub(crate) fn new(level_manifest: &'a LevelManifest) -> Self {
		Self {
			level_manifest,
			current_owner: 0,
			current_idx: 0,
			current_level: 0,
		}
	}
}

impl Iterator for LevelManifestIterator<'_> {
	type Item = Arc<Table>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let (_, levels) = self.level_manifest.levels_by_owner.get(self.current_owner)?;

			match levels.as_ref().get(self.current_level) {
				Some(level) => {
					if let Some(table) = level.tables.get(self.current_idx).cloned() {
						self.current_idx += 1;
						return Some(table);
					}
					self.current_level += 1;
					self.current_idx = 0;
				}
				None => {
					self.current_owner += 1;
					self.current_level = 0;
					self.current_idx = 0;
				}
			}
		}
	}
}
