use crate::{
	compaction::{CompactionChoice, CompactionInput, CompactionStrategy},
	error::Result,
	iter::{BoxedIterator, CompactionIterator},
	levels::{write_manifest_to_disk, LevelManifest, ManifestChangeSet},
	memtable::ImmutableMemtables,
	sstable::table::{Table, TableWriter},
	vfs::File,
	vlog::VLog,
	Options as LSMOptions,
};

use std::{
	collections::HashMap,
	fs::File as SysFile,
	path::{Path, PathBuf},
	sync::{Arc, RwLock, RwLockWriteGuard},
};

/// Compaction options
pub(crate) struct CompactionOptions {
	pub lopts: Arc<LSMOptions>,
	pub level_manifest: Arc<RwLock<LevelManifest>>,
	pub immutable_memtables: Arc<RwLock<ImmutableMemtables>>,
	pub vlog: Option<Arc<VLog>>,
}

impl CompactionOptions {
	pub(crate) fn from(tree: &crate::lsm::CoreInner) -> Self {
		Self {
			lopts: tree.opts.clone(),
			level_manifest: tree.level_manifest.clone(),
			immutable_memtables: tree.immutable_memtables.clone(),
			vlog: tree.vlog.clone(),
		}
	}
}

/// Handles the compaction state and operations
pub(crate) struct Compactor {
	pub(crate) options: CompactionOptions,
	pub(crate) strategy: Arc<dyn CompactionStrategy>,
}

impl Compactor {
	pub(crate) fn new(options: CompactionOptions, strategy: Arc<dyn CompactionStrategy>) -> Self {
		Self {
			options,
			strategy,
		}
	}

	pub(crate) fn compact(&self) -> Result<()> {
		let levels_guard = self.options.level_manifest.write().unwrap();
		let choice = self.strategy.pick_levels(&levels_guard);

		match choice {
			CompactionChoice::Merge(input) => self.merge_tables(levels_guard, &input),
			CompactionChoice::Skip => Ok(()),
		}
	}

	fn merge_tables(
		&self,
		mut levels: RwLockWriteGuard<'_, LevelManifest>,
		input: &CompactionInput,
	) -> Result<()> {
		let merge_result = {
			let tables = levels.get_all_tables();

			let to_merge: Vec<_> = input
				.tables_to_merge
				.iter()
				.filter_map(|&id| {
					let table_opt = tables.get(&id);
					table_opt.cloned()
				})
				.collect();

			let iterators: Vec<BoxedIterator<'_>> = to_merge
				.into_iter()
				.map(|table| Box::new(table.iter()) as BoxedIterator<'_>)
				.collect();

			// Hide tables that are being merged
			levels.hide_tables(&input.tables_to_merge);
			drop(levels);

			// Create new table
			let new_table_id = self.options.level_manifest.read().unwrap().next_table_id();
			let new_table_path = self.get_table_path(new_table_id);

			// Write merged data and collect discard statistics
			let discard_stats =
				self.write_merged_table(&new_table_path, new_table_id, iterators, input)?;

			let new_table = self.open_table(new_table_id, &new_table_path)?;
			Ok((new_table, new_table_id, discard_stats))
		};

		match merge_result {
			Ok((new_table, _, discard_stats)) => {
				self.update_manifest(input, new_table)?;
				self.cleanup_old_tables(input);

				// Update VLog discard statistics in bulk
				if !discard_stats.is_empty() {
					if let Some(ref vlog) = self.options.vlog {
						vlog.update_discard_stats(&discard_stats);
					}
				}

				Ok(())
			}
			Err(e) => {
				// Restore the original state
				let mut levels = self.options.level_manifest.write().unwrap();
				levels.unhide_tables(&input.tables_to_merge);
				Err(e)
			}
		}
	}

	fn write_merged_table(
		&self,
		path: &Path,
		table_id: u64,
		merge_iter: Vec<BoxedIterator<'_>>,
		input: &CompactionInput,
	) -> Result<HashMap<u32, i64>> {
		let file = SysFile::create(path)?;
		let mut writer = TableWriter::new(file, table_id, self.options.lopts.clone());

		// Create a compaction iterator that filters tombstones
		let max_level = self.options.lopts.level_count - 1;

		let is_bottom_level = input.target_level >= max_level;
		let mut comp_iter =
			CompactionIterator::new(merge_iter, is_bottom_level, self.options.vlog.clone());

		for (key, value) in &mut comp_iter {
			writer.add(key, &value)?;
		}

		writer.finish()?;

		// Flush any remaining delete-list entries to VLog
		comp_iter.flush_delete_list_batch()?;

		// Return collected discard statistics
		Ok(comp_iter.discard_stats)
	}

	fn update_manifest(&self, input: &CompactionInput, new_table: Arc<Table>) -> Result<()> {
		let mut manifest = self.options.level_manifest.write().unwrap();
		let _imm_guard = self.options.immutable_memtables.write();

		let new_table_id = new_table.id;

		// Check for table ID collision before making any changes
		if input.tables_to_merge.contains(&new_table_id) {
			return Err(crate::error::Error::TableIDCollision(new_table_id));
		}

		// Create a changeset for the compaction
		let mut changeset = ManifestChangeSet::default();

		// Add tables to delete (the ones being merged) - use the same efficient approach as original
		for (level_idx, level) in manifest.levels.get_levels().iter().enumerate() {
			for &table_id in &input.tables_to_merge {
				if level.tables.iter().any(|t| t.id == table_id) {
					changeset.deleted_tables.insert((level_idx as u8, table_id));
				}
			}
		}

		// Add the new table to the changeset
		changeset.new_tables.push((input.target_level, new_table.clone()));

		// Apply the changeset to remove old tables and add new table
		manifest.apply_changeset(&changeset)?;

		// Persist the updated manifest
		write_manifest_to_disk(&manifest)?;

		manifest.unhide_tables(&input.tables_to_merge);

		Ok(())
	}

	fn get_table_path(&self, table_id: u64) -> PathBuf {
		self.options.lopts.sstable_file_path(table_id)
	}

	fn cleanup_old_tables(&self, input: &CompactionInput) {
		for &table_id in &input.tables_to_merge {
			let path = self.options.lopts.sstable_file_path(table_id);
			if let Err(e) = std::fs::remove_file(path) {
				// Log error but continue with cleanup
				eprintln!("Failed to remove old table file: {e}");
			}
		}
	}

	fn open_table(&self, table_id: u64, table_path: &Path) -> Result<Arc<Table>> {
		let file = SysFile::open(table_path)?;
		let file: Arc<dyn File> = Arc::new(file);
		let file_size = file.size()?;

		Ok(Arc::new(Table::new(table_id, self.options.lopts.clone(), file, file_size)?))
	}
}
