use super::LevelManifest;
use crate::sstable::table::Table;
use crate::sstable::InternalKeyTrait;
use std::sync::Arc;

/// Iterates through all levels
pub(crate) struct LevelManifestIterator<'a, K: InternalKeyTrait> {
	level_manifest: &'a LevelManifest<K>,
	current_level: usize,
	current_idx: usize,
}

impl<'a, K: InternalKeyTrait> LevelManifestIterator<'a, K> {
	#[must_use]
	pub(crate) fn new(level_manifest: &'a LevelManifest<K>) -> Self {
		Self {
			level_manifest,
			current_idx: 0,
			current_level: 0,
		}
	}
}

impl<K: InternalKeyTrait> Iterator for LevelManifestIterator<'_, K> {
	type Item = Arc<Table<K>>;

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
