use std::collections::HashMap;
use std::fs::File as SysFile;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockWriteGuard};

use crate::compaction::{CompactionChoice, CompactionInput, CompactionStrategy};
use crate::error::{BackgroundErrorHandler, Result};
use crate::iter::{BoxedInternalIterator, CompactionIterator};
use crate::levels::{write_manifest_to_disk, LevelManifest, ManifestChangeSet};
use crate::lsm::CoreInner;
use crate::memtable::ImmutableMemtables;
use crate::snapshot::SnapshotTracker;
use crate::sstable::table::{Table, TableWriter};
use crate::vfs::File;
use crate::vlog::VLog;
use crate::{Comparator, Options as LSMOptions};

/// RAII guard to ensure tables are unhidden if compaction fails
struct HiddenTablesGuard {
	level_manifest: Arc<RwLock<LevelManifest>>,
	table_ids: Vec<u64>,
	committed: bool,
}

impl HiddenTablesGuard {
	fn new(level_manifest: Arc<RwLock<LevelManifest>>, table_ids: &[u64]) -> Self {
		Self {
			level_manifest,
			table_ids: table_ids.to_vec(),
			committed: false,
		}
	}

	fn commit(&mut self) {
		self.committed = true;
	}
}

impl Drop for HiddenTablesGuard {
	fn drop(&mut self) {
		if !self.committed {
			if let Ok(mut levels) = self.level_manifest.write() {
				levels.unhide_tables(&self.table_ids);
			}
		}
	}
}

/// Compaction options
pub(crate) struct CompactionOptions {
	pub(crate) lopts: Arc<LSMOptions>,
	pub(crate) level_manifest: Arc<RwLock<LevelManifest>>,
	pub(crate) immutable_memtables: Arc<RwLock<ImmutableMemtables>>,
	pub(crate) vlog: Option<Arc<VLog>>,
	pub(crate) error_handler: Arc<BackgroundErrorHandler>,
	/// Snapshot tracker for snapshot-aware compaction.
	///
	/// During compaction, we query this to get the list of active snapshot
	/// sequence numbers. Versions visible to any active snapshot must be
	/// preserved (unless hidden by a newer version in the same visibility boundary).
	pub(crate) snapshot_tracker: SnapshotTracker,
}

impl CompactionOptions {
	pub(crate) fn from(tree: &CoreInner) -> Self {
		Self {
			lopts: Arc::clone(&tree.opts),
			level_manifest: Arc::clone(&tree.level_manifest),
			immutable_memtables: Arc::clone(&tree.immutable_memtables),
			vlog: tree.vlog.clone(),
			error_handler: Arc::clone(&tree.error_handler),
			snapshot_tracker: tree.snapshot_tracker.clone(),
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
		let choice = self.strategy.pick_levels(&levels_guard)?;

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
		// Hide tables that are being merged
		levels.hide_tables(&input.tables_to_merge);

		// Create guard to ensure tables are unhidden on error
		let mut guard = HiddenTablesGuard::new(
			Arc::clone(&self.options.level_manifest),
			&input.tables_to_merge,
		);

		let tables = levels.get_all_tables();
		let to_merge: Vec<_> =
			input.tables_to_merge.iter().filter_map(|&id| tables.get(&id).cloned()).collect();

		// Keep tables alive while iterators borrow from them
		let iterators: Vec<BoxedInternalIterator<'_>> = to_merge
			.iter()
			.filter_map(|table| table.iter(None).ok())
			.map(|iter| Box::new(iter) as BoxedInternalIterator<'_>)
			.collect();

		drop(levels);

		// Create new table
		let new_table_id = self.options.level_manifest.read().unwrap().next_table_id();
		let new_table_path = self.get_table_path(new_table_id);

		// Write merged data - returns (table_created, discard_stats)
		let (table_created, discard_stats) =
			match self.write_merged_table(&new_table_path, new_table_id, iterators, input) {
				Ok(result) => result,
				Err(e) => {
					// Guard will unhide tables on drop
					return Err(e);
				}
			};

		// Open table only if one was created
		let new_table = if table_created {
			match self.open_table(new_table_id, &new_table_path) {
				Ok(table) => Some(table),
				Err(e) => {
					// Guard will unhide tables on drop
					return Err(e);
				}
			}
		} else {
			None
		};

		// Update discard stats BEFORE manifest commit for clean abort on error.
		// If this fails, the HiddenTablesGuard will unhide tables on drop,
		// leaving the system in a consistent state.
		if !discard_stats.is_empty() {
			if let Some(ref vlog) = self.options.vlog {
				vlog.update_discard_stats(&discard_stats)?;
			}
		}

		// Update manifest - this will commit the guard on success
		self.update_manifest(input, new_table, &mut guard)?;

		self.cleanup_old_tables(input);

		Ok(())
	}

	/// Returns (table_created, discard_stats)
	/// table_created: true if a table file was created and finished, false otherwise
	/// discard_stats: always populated (even when no table created) for VLog GC
	fn write_merged_table(
		&self,
		path: &Path,
		table_id: u64,
		merge_iter: Vec<BoxedInternalIterator<'_>>,
		input: &CompactionInput,
	) -> Result<(bool, HashMap<u32, i64>)> {
		let file = SysFile::create(path)?;
		let mut writer =
			TableWriter::new(file, table_id, Arc::clone(&self.options.lopts), input.target_level);

		// Get active snapshots for snapshot-aware compaction
		// This is a snapshot of the snapshot list at the start of compaction.
		// Any snapshots created during compaction will be handled by the next compaction.
		let snapshots = self.options.snapshot_tracker.get_all_snapshots();

		// Create a compaction iterator that filters tombstones and respects snapshots
		let max_level = self.options.lopts.level_count - 1;
		let is_bottom_level = input.target_level >= max_level;
		let mut comp_iter = CompactionIterator::new(
			merge_iter,
			Arc::clone(&self.options.lopts.internal_comparator) as Arc<dyn Comparator>,
			is_bottom_level,
			self.options.vlog.clone(),
			self.options.lopts.enable_versioning,
			self.options.lopts.versioned_history_retention_ns,
			Arc::clone(&self.options.lopts.clock),
			snapshots,
		);

		let mut entries = 0;
		for item in &mut comp_iter {
			let (key, value) = item?;
			writer.add(key, &value)?;
			entries += 1;
		}

		// Always flush delete-list for VLog cleanup (critical for GC)
		comp_iter.flush_delete_list_batch()?;

		// Capture discard_stats - these are populated even when entries == 0
		let discard_stats = comp_iter.discard_stats;

		if entries == 0 {
			// No entries - drop writer and remove empty file
			drop(writer);
			let _ = std::fs::remove_file(path);
			return Ok((false, discard_stats));
		}

		writer.finish()?;
		Ok((true, discard_stats))
	}

	fn update_manifest(
		&self,
		input: &CompactionInput,
		new_table: Option<Arc<Table>>,
		guard: &mut HiddenTablesGuard,
	) -> Result<()> {
		let mut manifest = self.options.level_manifest.write()?;
		let _imm_guard = self.options.immutable_memtables.write();

		// Check for table ID collision if adding a new table
		if let Some(ref table) = new_table {
			if input.tables_to_merge.contains(&table.id) {
				return Err(crate::error::Error::TableIDCollision(table.id));
			}
		}

		let mut changeset = ManifestChangeSet::default();

		// Delete old tables
		for (level_idx, level) in manifest.levels.get_levels().iter().enumerate() {
			for &table_id in &input.tables_to_merge {
				if level.tables.iter().any(|t| t.id == table_id) {
					changeset.deleted_tables.insert((level_idx as u8, table_id));
				}
			}
		}

		// Add new table if present
		if let Some(table) = new_table {
			changeset.new_tables.push((input.target_level, table));
		}

		let rollback = manifest.apply_changeset(&changeset)?;

		// Write manifest to disk - if this fails, revert in-memory state
		if let Err(e) = write_manifest_to_disk(&manifest) {
			manifest.revert_changeset(rollback);
			self.options
				.error_handler
				.set_error(e.clone(), crate::error::BackgroundErrorReason::ManifestWrite);
			return Err(e);
		}

		// Unhide tables before committing guard (they'll be removed from manifest anyway)
		manifest.unhide_tables(&input.tables_to_merge);

		// Commit guard - tables are now properly handled in manifest
		guard.commit();

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

		Ok(Arc::new(Table::new(table_id, Arc::clone(&self.options.lopts), file, file_size)?))
	}
}
