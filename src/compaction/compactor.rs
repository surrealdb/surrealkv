use std::fs::File as SysFile;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockWriteGuard};

use crate::compaction::{CompactionChoice, CompactionInput, CompactionStrategy};
use crate::error::{BackgroundErrorHandler, Result};
use crate::iter::{BoxedLSMIterator, CompactionIterator};
use crate::levels::{write_manifest_to_disk, LevelManifest, ManifestChangeSet};
use crate::lsm::{cleanup_vlog, CoreInner};
use crate::memtable::{ImmutableMemtables, MemTable};
use crate::snapshot::SnapshotTracker;
use crate::sstable::table::{Table, TableWriter};
use crate::vfs::File;
use crate::vlog::VLog;
use crate::{Comparator, Options as LSMOptions};

/// Where a compaction calls `CompactionOptions::stage_hook`, in the order it reaches them. No
/// lock is held at any of them.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompactionStage {
	/// The output tables and the SSTable directory are fsynced, and the manifest still lists the
	/// inputs. An error from the hook fails the compaction here: the inputs stay in place and the
	/// outputs are left on disk.
	OutputsDurable,
	/// The manifest that lists the outputs instead of the inputs is on disk and in memory. The
	/// input files and the obsolete value-log files are still on disk.
	ManifestWritten,
	/// The input files are removed, and the obsolete value-log files are still on disk.
	InputsRemoved,
	/// The value-log files that no table or memtable needs any more are removed.
	VlogCleaned,
}

/// See `CompactionOptions::stage_hook`. Only an error returned at `OutputsDurable` is used.
#[cfg(test)]
pub(crate) type CompactionStageHook = Arc<dyn Fn(CompactionStage) -> Result<()> + Send + Sync>;

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
	pub(crate) active_memtable: Arc<RwLock<Arc<MemTable>>>,
	pub(crate) immutable_memtables: Arc<RwLock<ImmutableMemtables>>,
	pub(crate) vlog: Option<Arc<VLog>>,
	pub(crate) error_handler: Arc<BackgroundErrorHandler>,
	/// Snapshot tracker for snapshot-aware compaction.
	///
	/// During compaction, we query this to get the list of active snapshot
	/// sequence numbers. Versions visible to any active snapshot must be
	/// preserved (unless hidden by a newer version in the same visibility boundary).
	pub(crate) snapshot_tracker: SnapshotTracker,
	/// The highest sequence number that is visible to readers.
	///
	/// A version above it was not published, and a reader that is not a registered snapshot
	/// reads at this sequence number or above. A compaction keeps every version above it, and the
	/// newest one at or below it.
	pub(crate) visible_seq_num: Arc<AtomicU64>,
	/// Runs between the read of `visible_seq_num` and the read of the snapshot list.
	#[cfg(test)]
	pub(crate) after_horizon_hook: Option<Arc<dyn Fn() + Send + Sync>>,
	/// Called at each `CompactionStage`; `CompactionOptions::from` takes the tree's hook.
	#[cfg(test)]
	pub(crate) stage_hook: Option<CompactionStageHook>,
}

impl CompactionOptions {
	pub(crate) fn from(tree: &CoreInner) -> Self {
		Self {
			lopts: Arc::clone(&tree.opts),
			level_manifest: Arc::clone(&tree.level_manifest),
			active_memtable: Arc::clone(&tree.active_memtable),
			immutable_memtables: Arc::clone(&tree.immutable_memtables),
			vlog: tree.vlog.clone(),
			error_handler: Arc::clone(&tree.error_handler),
			snapshot_tracker: tree.snapshot_tracker.clone(),
			visible_seq_num: Arc::clone(&tree.visible_seq_num),
			#[cfg(test)]
			after_horizon_hook: None,
			#[cfg(test)]
			stage_hook: tree.compaction_stage_hook.lock().clone(),
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
		let iterators: Vec<BoxedLSMIterator<'_>> = to_merge
			.iter()
			.filter_map(|table| table.iter(None).ok())
			.map(|iter| Box::new(iter) as BoxedLSMIterator<'_>)
			.collect();

		drop(levels);

		// Write merged data, rolling over to a new output file at
		// target_file_size boundaries. Returns the (id, path) of every
		// finished output.
		let outputs = match self.write_merged_table(iterators, input) {
			Ok(result) => result,
			Err(e) => {
				// Guard will unhide tables on drop
				return Err(e);
			}
		};

		#[cfg(test)]
		self.reach(CompactionStage::OutputsDurable)?;

		// Open the finished outputs
		let mut new_tables = Vec::with_capacity(outputs.len());
		for (id, path) in &outputs {
			match self.open_table(*id, path) {
				Ok(table) => new_tables.push(table),
				Err(e) => {
					// Guard will unhide tables on drop; the already-written
					// output files are removed as orphans on next startup.
					return Err(e);
				}
			}
		}

		// Update manifest - this will commit the guard on success
		self.update_manifest(input, new_tables, &mut guard)?;

		#[cfg(test)]
		let _ = self.reach(CompactionStage::ManifestWritten);

		self.cleanup_old_tables(input);

		#[cfg(test)]
		let _ = self.reach(CompactionStage::InputsRemoved);

		// After successful manifest commit, cleanup obsolete vlog files, with the manifest and the
		// queue released: what is unflushed also points into the value log.
		cleanup_vlog(
			&self.options.vlog,
			&self.options.active_memtable,
			&self.options.level_manifest,
			&self.options.immutable_memtables,
			"compaction",
		);

		#[cfg(test)]
		let _ = self.reach(CompactionStage::VlogCleaned);

		Ok(())
	}

	/// Calls the test hook, if one is set, for `stage`.
	#[cfg(test)]
	fn reach(&self, stage: CompactionStage) -> Result<()> {
		match &self.options.stage_hook {
			Some(hook) => hook(stage),
			None => Ok(()),
		}
	}

	/// Reads the visibility horizon and the active snapshots for snapshot-aware compaction.
	///
	/// The horizon is the visible sequence number, and it is read BEFORE the snapshot list. A
	/// publication that comes between the two reads then raises the visible sequence number
	/// only after the horizon was fixed: a snapshot registered by then is in the list, and one
	/// registered later was taken from a visible sequence number that is the horizon or above.
	/// A transaction takes its sequence number before it registers it, so one that does so
	/// between the two reads can still be missed. It must be the visible sequence number and
	/// never the log sequence number, which runs ahead of publication. A restore can lower the
	/// visible sequence number, which only makes a compaction keep more.
	///
	/// Any snapshots created during compaction will be handled by the next compaction.
	fn read_horizon_and_snapshots(&self) -> (u64, Vec<u64>) {
		let horizon = self.options.visible_seq_num.load(Ordering::Acquire);
		#[cfg(test)]
		if let Some(hook) = &self.options.after_horizon_hook {
			hook();
		}
		let snapshots = self.options.snapshot_tracker.get_all_snapshots();
		(horizon, snapshots)
	}

	/// Writes the merged stream into one or more output SSTs, rolling over to
	/// a new file whenever the current output reaches
	/// `Options::target_file_size`. Rollover happens only at user-key
	/// boundaries so all versions of a user key stay in one file (the same
	/// bytewise grouping `CompactionIterator` uses). Bounding output size
	/// keeps per-file writer memory (bloom/index build) and future compaction
	/// inputs independent of level size (issue #397).
	///
	/// Returns the `(table_id, path)` of every finished output; empty if the
	/// merge produced no entries and no active range deletions.
	fn write_merged_table(
		&self,
		merge_iter: Vec<BoxedLSMIterator<'_>>,
		input: &CompactionInput,
	) -> Result<Vec<(u64, PathBuf)>> {
		// Collect range deletions from all input tables being merged
		let mut range_deletions = Vec::new();
		if let Ok(manifest) = self.options.level_manifest.read() {
			for &tid in &input.tables_to_merge {
				if let Some(t) = manifest.find_table_by_id(tid) {
					range_deletions.extend(t.range_deletions.read().clone());
				}
			}
		}
		range_deletions.sort_unstable();
		range_deletions.dedup();

		let (horizon, snapshots) = self.read_horizon_and_snapshots();

		// Create a compaction iterator that filters tombstones and respects snapshots
		let max_level = self.options.lopts.level_count - 1;
		let is_bottom_level = input.target_level >= max_level;
		let mut comp_iter = CompactionIterator::new(
			merge_iter,
			Arc::clone(&self.options.lopts.internal_comparator) as Arc<dyn Comparator>,
			is_bottom_level,
			snapshots,
			horizon,
		)
		.with_range_deletions(range_deletions.clone());

		let target_file_size = self.options.lopts.target_file_size;
		let mut outputs: Vec<(u64, PathBuf)> = Vec::new();
		let mut current: Option<(TableWriter<SysFile>, u64, PathBuf)> = None;
		let mut prev_user_key: Vec<u8> = Vec::new();

		for item in &mut comp_iter {
			let (key, value) = item?;

			// Roll over once the current output is full, but only when the
			// user key changes: versions of one key must never span files.
			let roll = current.as_ref().is_some_and(|(w, _, _)| {
				w.estimated_file_size() >= target_file_size
					&& key.user_key.as_slice() != prev_user_key.as_slice()
			});
			if roll {
				if let Some((mut w, id, path)) = current.take() {
					for (start, end, seq) in &range_deletions {
						w.add_range_deletion(start.clone(), end.clone(), *seq);
					}
					self.finish_output(w, id, &path)?;
					outputs.push((id, path));
				}
			}

			// Open the next output lazily so we never create empty files.
			let (w, ..) = match &mut current {
				Some(c) => c,
				None => {
					let id = self.options.level_manifest.read()?.next_table_id();
					let path = self.get_table_path(id);
					let file = SysFile::create(&path)?;
					let writer = TableWriter::new(
						file,
						id,
						Arc::clone(&self.options.lopts),
						input.target_level,
					);
					current.insert((writer, id, path))
				}
			};

			prev_user_key.clear();
			prev_user_key.extend_from_slice(key.user_key.as_slice());
			w.add(key, &value)?;
		}

		if let Some((mut w, id, path)) = current.take() {
			for (start, end, seq) in comp_iter.active_range_deletions() {
				w.add_range_deletion(start.clone(), end.clone(), *seq);
			}
			self.finish_output(w, id, &path)?;
			outputs.push((id, path));
		} else if !comp_iter.active_range_deletions().is_empty() {
			// No point entries were produced, but active range deletions exist.
			// Write a single output SST containing the range deletions so they are preserved.
			let id = self.options.level_manifest.read()?.next_table_id();
			let path = self.get_table_path(id);
			let file = SysFile::create(&path)?;
			let mut writer =
				TableWriter::new(file, id, Arc::clone(&self.options.lopts), input.target_level);
			for (start, end, seq) in comp_iter.active_range_deletions() {
				writer.add_range_deletion(start.clone(), end.clone(), *seq);
			}
			self.finish_output(writer, id, &path)?;
			outputs.push((id, path));
		}

		if !outputs.is_empty() {
			// Durability: the directory entries of all outputs must be
			// durable BEFORE the manifest (fsynced in update_manifest)
			// references them (surrealdb/surrealdb#7426).
			crate::lsm::fsync_directory(self.options.lopts.sstable_dir()).inspect(|()| {
				#[cfg(test)]
				directory_syncs::record(&self.options.lopts.sstable_dir());
			})?;
		}

		Ok(outputs)
	}

	/// Finishes one compaction output and makes its contents durable. The
	/// file must be fsynced before the manifest references it; the directory
	/// fsync happens once after all outputs are finished.
	fn finish_output(&self, writer: TableWriter<SysFile>, id: u64, path: &Path) -> Result<()> {
		writer.finish()?;
		crate::vfs::fsync_file(path)?;
		tracing::debug!("Compaction finished output table {id} at {}", path.display());
		Ok(())
	}

	fn update_manifest(
		&self,
		input: &CompactionInput,
		new_tables: Vec<Arc<Table>>,
		guard: &mut HiddenTablesGuard,
	) -> Result<()> {
		let mut manifest = self.options.level_manifest.write()?;
		let _imm_guard = self.options.immutable_memtables.write();

		// Check for table ID collisions before adding the new tables
		for table in &new_tables {
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

		// Add all compaction outputs in the same atomic changeset
		for table in new_tables {
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
				tracing::warn!("Failed to remove old table file: {e}");
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

/// Test-only ledger of the SSTable directories a compaction made durable with a directory fsync.
///
/// A directory fsync leaves nothing a test can look at, so the compaction records each one it
/// completes. A crash simulation uses the count to tell whether the directory entries of the
/// outputs of a compaction were durable at the time of the image.
#[cfg(test)]
pub(crate) mod directory_syncs {
	use std::collections::HashMap;
	use std::path::{Path, PathBuf};
	use std::sync::{LazyLock, Mutex};

	static SYNCS: LazyLock<Mutex<HashMap<PathBuf, usize>>> = LazyLock::new(Default::default);

	fn key(path: &Path) -> PathBuf {
		path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
	}

	pub(crate) fn record(dir: &Path) {
		*SYNCS.lock().unwrap().entry(key(dir)).or_default() += 1;
	}

	/// How many directory fsyncs of `dir` a compaction has completed.
	pub(crate) fn count(dir: &Path) -> usize {
		SYNCS.lock().unwrap().get(&key(dir)).copied().unwrap_or(0)
	}
}
