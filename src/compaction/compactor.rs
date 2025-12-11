use crate::{
	compaction::{CompactionChoice, CompactionInput, CompactionStrategy},
	error::Result,
	iter::{BoxedIterator, CompactionIterator},
	levels::{write_manifest_to_disk, LevelManifest, ManifestChangeSet},
	lsm::CoreInner,
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
	pub(crate) lopts: Arc<LSMOptions>,
	pub(crate) level_manifest: Arc<RwLock<LevelManifest>>,
	pub(crate) immutable_memtables: Arc<RwLock<ImmutableMemtables>>,
	pub(crate) vlog: Option<Arc<VLog>>,
}

impl CompactionOptions {
	pub(crate) fn from(tree: &CoreInner) -> Self {
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
		let levels_guard = self.options.level_manifest.write()?;
		let choice = self.strategy.pick_levels(&levels_guard);

		match choice {
			CompactionChoice::Merge(input) => {
				log::error!(
					"[COMPACTION] Compaction choice: MERGE\n\
					Source level: {}\n\
					Target level: {}\n\
					Tables to merge: {} tables with IDs: {:?}",
					input.source_level,
					input.target_level,
					input.tables_to_merge.len(),
					input.tables_to_merge
				);
				self.merge_tables(levels_guard, &input)
			}
			CompactionChoice::Skip => {
				log::error!("[COMPACTION] Compaction choice: SKIP (no compaction needed)");
				Ok(())
			}
		}
	}

	fn merge_tables(
		&self,
		mut levels: RwLockWriteGuard<'_, LevelManifest>,
		input: &CompactionInput,
	) -> Result<()> {
		log::error!(
			"[COMPACTION] Starting merge_tables\n\
			Merging L{} → L{}\n\
			Number of tables to merge: {}",
			input.source_level,
			input.target_level,
			input.tables_to_merge.len()
		);

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

			log::error!(
				"[COMPACTION] Found {} tables to merge (filter_map result)",
				to_merge.len()
			);

			let iterators: Vec<BoxedIterator<'_>> = to_merge
				.into_iter()
				.map(|table| Box::new(table.iter(false)) as BoxedIterator<'_>)
				.collect();

			// Hide tables that are being merged
			levels.hide_tables(&input.tables_to_merge);
			drop(levels);

			// Create new table
			let new_table_id = self.options.level_manifest.read().unwrap().next_table_id();
			let new_table_path = self.get_table_path(new_table_id);

			log::error!(
				"[COMPACTION] Creating new merged table\n\
			New table ID: {}\n\
			Target level: L{}\n\
			Path: {:?}",
				new_table_id,
				input.target_level,
				new_table_path
			);

			// Write merged data and collect discard statistics
			let discard_stats =
				self.write_merged_table(&new_table_path, new_table_id, iterators, input)?;

			log::error!(
				"[COMPACTION] Finished writing merged table {}, opening for use",
				new_table_id
			);

			let new_table = self.open_table(new_table_id, &new_table_path)?;
			Ok((new_table, new_table_id, discard_stats))
		};

		match merge_result {
			Ok((new_table, new_table_id, discard_stats)) => {
				log::error!(
					"[COMPACTION] Merge successful, updating manifest\n\
				New table ID: {}\n\
				Removing {} old tables: {:?}",
					new_table_id,
					input.tables_to_merge.len(),
					input.tables_to_merge
				);

				self.update_manifest(input, new_table)?;
				self.cleanup_old_tables(input);

				// Update VLog discard statistics in bulk
				if !discard_stats.is_empty() {
					if let Some(ref vlog) = self.options.vlog {
						vlog.update_discard_stats(&discard_stats);
					}
				}

				log::error!(
					"[COMPACTION] Merge complete for L{} → L{}, table {} created",
					input.source_level,
					input.target_level,
					new_table_id
				);

				Ok(())
			}
			Err(e) => {
				log::error!(
					"[COMPACTION] Merge FAILED, restoring original state\n\
				Error: {:?}",
					e
				);
				// Restore the original state
				let mut levels = self.options.level_manifest.write()?;
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
		use crate::sstable::InternalKey;
		use std::sync::Arc;

		let file = SysFile::create(path)?;
		let mut writer = TableWriter::new(file, table_id, self.options.lopts.clone());

		// Create a compaction iterator that filters tombstones
		let max_level = self.options.lopts.level_count - 1;

		let is_bottom_level = input.target_level >= max_level;
		let mut comp_iter = CompactionIterator::new(
			merge_iter,
			is_bottom_level,
			self.options.vlog.clone(),
			self.options.lopts.enable_versioning,
			self.options.lopts.versioned_history_retention_ns,
			self.options.lopts.clock.clone(),
		);

		// Buffer to capture all key-value pairs for debugging on error
		let mut kv_buffer: Vec<(Arc<InternalKey>, Vec<u8>)> = Vec::new();

		let mut key_count = 0u64;
		for (key, value) in &mut comp_iter {
			key_count += 1;

			// Store the key-value pair in buffer before adding
			kv_buffer.push((key.clone(), value.to_vec()));

			if let Err(e) = writer.add(key, &value) {
				// Log all key-value pairs from the buffer to help debug
				log::error!(
					"[COMPACTION TABLE {}] ERROR during writer.add: {:?}\n\
					Dumping all {} key-value pairs from buffer:",
					table_id,
					e,
					kv_buffer.len()
				);

				for (idx, (k, v)) in kv_buffer.iter().enumerate() {
					log::error!(
						"[COMPACTION TABLE {} DUMP] Entry #{}: user_key={:?} seq={} kind={:?} ts={} value_len={}",
						table_id,
						idx + 1,
						String::from_utf8_lossy(&k.user_key),
						k.seq_num(),
						k.kind(),
						k.timestamp,
						v.len()
					);
				}

				return Err(e);
			}
		}

		log::error!(
			"[COMPACTION TABLE {}] Finished writing {} keys from compaction iterator",
			table_id,
			key_count
		);

		writer.finish()?;

		// Flush any remaining delete-list entries to VLog
		comp_iter.flush_delete_list_batch()?;

		// Return collected discard statistics
		Ok(comp_iter.discard_stats)
	}

	fn update_manifest(&self, input: &CompactionInput, new_table: Arc<Table>) -> Result<()> {
		let mut manifest = self.options.level_manifest.write()?;
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
				log::warn!("Failed to remove old table file: {e}");
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
