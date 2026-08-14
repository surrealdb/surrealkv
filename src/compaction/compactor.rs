use std::fs::File as SysFile;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockWriteGuard};

use crate::batch::BatchOwner;
use crate::branch::{BranchCatalog, RetentionAnchors};
use crate::compaction::{CompactionChoice, CompactionInput, CompactionStrategy};
use crate::error::{BackgroundErrorHandler, Result};
use crate::iter::{BoxedLSMIterator, CompactionIterator};
use crate::levels::{LevelManifest, ManifestChangeSet};
use crate::lsm::CoreInner;
use crate::memtable::ImmutableMemtables;
use crate::snapshot::SnapshotTracker;
use crate::sstable::table::{Table, TableWriter};
use crate::vfs::File;
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
	/// Physical owner whose level set this compaction operates on. Picking
	/// and changeset application never see another owner's tables, so a
	/// mixed-owner compaction is unrepresentable.
	pub(crate) owner: BatchOwner,
	pub(crate) level_manifest: Arc<RwLock<LevelManifest>>,
	pub(crate) immutable_memtables: Arc<RwLock<ImmutableMemtables>>,
	pub(crate) error_handler: Arc<BackgroundErrorHandler>,
	/// Snapshot tracker for snapshot-aware compaction.
	///
	/// During compaction, we query this to get the list of active snapshot
	/// sequence numbers. Versions visible to any active snapshot must be
	/// preserved (unless hidden by a newer version in the same visibility boundary).
	pub(crate) snapshot_tracker: SnapshotTracker,
	/// The branch catalog: the sole source of inherited-view retention pins
	/// (design §3.3a, plan amendment C9 — never the snapshot tracker, never
	/// child state manifests).
	pub(crate) branch_catalog: Arc<RwLock<BranchCatalog>>,
	/// Where the count of pin-retained versions goes. Compaction is the only
	/// place that number exists.
	pub(crate) metrics: Arc<crate::metrics::BranchMetrics>,
	/// The caps `owner` must stay exactly readable at, sampled from the catalog
	/// when this job was created.
	pub(crate) pin_anchors: RetentionAnchors,
	/// Whether this compaction must not treat its highest level as the bottom
	/// of a read stack. True when anything still reads `owner` at a cap (an
	/// Active child's inherited view or a merge's base, both of which read below
	/// the tombstones being compacted) or when `owner` is itself a fork child
	/// (its ancestors sit below its own bottom level) — plan amendment C1(c).
	pub(crate) force_not_bottom: bool,
}

impl CompactionOptions {
	pub(crate) fn for_owner(tree: &CoreInner, owner: BatchOwner) -> Result<Self> {
		// Lock order: this is the only place a compaction job takes the catalog
		// without already holding the level manifest, and every other catalog
		// reader is leaf-level, so `level_manifest -> branch_catalog` (used at
		// publish time) is a consistent global order.
		let (pin_anchors, force_not_bottom) = {
			let catalog = tree.branch_catalog.read()?;
			let anchors = catalog.retention_anchors(owner.branch, owner.generation);
			let force_not_bottom =
				!anchors.is_empty() || catalog.record_has_parent(owner.branch, owner.generation);
			(anchors, force_not_bottom)
		};
		Ok(Self {
			lopts: Arc::clone(&tree.opts),
			owner,
			level_manifest: Arc::clone(&tree.level_manifest),
			immutable_memtables: Arc::clone(&tree.immutable_memtables),
			error_handler: Arc::clone(&tree.error_handler),
			snapshot_tracker: tree.snapshot_tracker.clone(),
			branch_catalog: Arc::clone(&tree.branch_catalog),
			metrics: Arc::clone(&tree.metrics),
			pin_anchors,
			force_not_bottom,
		})
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
		let choice = self.strategy.pick_levels(&levels_guard, self.options.owner)?;

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
		let owner = common_owner(to_merge.iter().map(|table| table.meta.owner))?;

		// Keep tables alive while iterators borrow from them
		let iterators: Vec<BoxedLSMIterator<'_>> = to_merge
			.iter()
			.filter_map(|table| table.iter(None).ok())
			.map(|iter| Box::new(iter) as BoxedLSMIterator<'_>)
			.collect();

		drop(levels);

		// Create new table
		let new_table_id = self.options.level_manifest.read().unwrap().next_table_id();
		let new_table_path = self.get_table_path(new_table_id);

		// Write merged data
		let MergeOutcome {
			table_created,
			retained_floor_advance,
		} = match self.write_merged_table(&new_table_path, new_table_id, iterators, input, owner) {
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

		// Update manifest - this will commit the guard on success
		if let Err(error) =
			self.update_manifest(input, new_table, retained_floor_advance, &mut guard)
		{
			// The output table is unreachable: nothing references it, and the
			// guard is about to restore the inputs. Remove it here rather than
			// leaving an orphan for the next open to reclaim.
			if table_created {
				if let Err(remove_error) = std::fs::remove_file(&new_table_path) {
					log::warn!("failed to remove unpublished compaction output: {remove_error}");
				}
			}
			return Err(error);
		}

		self.cleanup_old_tables(input);

		Ok(())
	}

	fn write_merged_table(
		&self,
		path: &Path,
		table_id: u64,
		merge_iter: Vec<BoxedLSMIterator<'_>>,
		input: &CompactionInput,
		owner: crate::batch::BatchOwner,
	) -> Result<MergeOutcome> {
		let file = SysFile::create(path)?;
		let mut writer = TableWriter::new_owned(
			file,
			table_id,
			Arc::clone(&self.options.lopts),
			input.target_level,
			owner,
		);

		// Get active snapshots for snapshot-aware compaction
		// This is a snapshot of the snapshot list at the start of compaction.
		// Any snapshots created during compaction will be handled by the next compaction.
		let snapshots = self.options.snapshot_tracker.get_all_snapshots();

		// Create a compaction iterator that filters tombstones and respects snapshots
		let max_level = self.options.lopts.level_count - 1;
		let is_bottom_level = input.target_level >= max_level && !self.options.force_not_bottom;
		let mut comp_iter = CompactionIterator::new(
			merge_iter,
			Arc::clone(&self.options.lopts.internal_comparator) as Arc<dyn Comparator>,
			is_bottom_level,
			self.options.lopts.enable_versioning,
			self.options.lopts.versioned_history_retention_ns,
			Arc::clone(&self.options.lopts.clock),
			snapshots,
			self.options.pin_anchors.clone(),
		);

		let mut entries = 0;
		for item in &mut comp_iter {
			let (key, value) = item?;
			writer.add(key, &value)?;
			entries += 1;
		}

		let pin_retained = comp_iter.pin_retained_versions();
		self.options.metrics.record_pin_retained(pin_retained);
		if pin_retained > 0 {
			log::debug!(
				"compaction of {:?} retained {} version(s) for the views pinned at {:?}",
				self.options.owner,
				pin_retained,
				self.options.pin_anchors
			);
		}

		let outcome = MergeOutcome {
			table_created: entries > 0,
			retained_floor_advance: comp_iter.retained_floor_advance(),
		};

		if entries == 0 {
			// No entries - drop writer and remove empty file
			drop(writer);
			let _ = std::fs::remove_file(path);
			return Ok(outcome);
		}

		writer.finish()?;

		// Durability fix: the SST's data and its directory entry must be
		// durable BEFORE the manifest (fsynced in update_manifest) references
		// this table. Otherwise a power loss after the manifest commit leaves
		// a durable manifest pointing at a zero-byte table, with the merged
		// inputs already deleted (surrealdb/surrealdb#7426).
		crate::vfs::fsync_file(path)?;
		crate::lsm::fsync_directory(self.options.lopts.sstable_dir())?;

		Ok(outcome)
	}

	fn update_manifest(
		&self,
		input: &CompactionInput,
		new_table: Option<Arc<Table>>,
		retained_floor_advance: u64,
		guard: &mut HiddenTablesGuard,
	) -> Result<()> {
		let mut manifest = self.options.level_manifest.write()?;
		let _imm_guard = self.options.immutable_memtables.write();

		// A fork or a merge published while this job was merging can pin history
		// the job already dropped: the anchors were sampled before the merge,
		// outside the manifest lock. Re-read them here, under the lock that
		// serialises publication, and refuse to publish an output built against
		// a weaker promise. The hidden inputs are restored by
		// `HiddenTablesGuard`, and the next cycle re-picks them with the anchor
		// in hand.
		let current = self
			.options
			.branch_catalog
			.read()?
			.retention_anchors(self.options.owner.branch, self.options.owner.generation);
		if let Some(unsampled_anchor) = current.appeared_since(&self.options.pin_anchors) {
			return Err(crate::error::Error::CompactionPinRaced {
				unsampled_anchor,
			});
		}

		// Check for table ID collision if adding a new table
		if let Some(ref table) = new_table {
			if input.tables_to_merge.contains(&table.id) {
				return Err(crate::error::Error::TableIDCollision(table.id));
			}
		}

		let mut changeset = ManifestChangeSet {
			owner: self.options.owner,
			..ManifestChangeSet::default()
		};

		// Delete old tables from this owner's level set
		let owner_levels = manifest.levels_for(self.options.owner).ok_or_else(|| {
			crate::error::Error::Corruption(format!(
				"compaction owner {:?} has no level set",
				self.options.owner
			))
		})?;
		for (level_idx, level) in owner_levels.get_levels().iter().enumerate() {
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

		// The floor must be durable together with the level set it describes: a
		// crash between them would leave a state whose tables no longer support
		// the views its floor still promises.
		manifest.raise_retained_floor(self.options.owner, retained_floor_advance);

		let rollback = manifest.apply_changeset(&changeset)?;

		// Publish the owner's state + root - if this fails, revert in-memory state
		if let Err(e) = manifest.persist_owner_update(self.options.owner) {
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

/// What one merge produced: whether an output table exists, and how far the
/// owner's retention floor must move before that output can be published.
struct MergeOutcome {
	table_created: bool,
	retained_floor_advance: u64,
}

fn common_owner(
	mut owners: impl Iterator<Item = crate::batch::BatchOwner>,
) -> Result<crate::batch::BatchOwner> {
	let owner = owners.next().unwrap_or_default();
	if owners.any(|candidate| candidate != owner) {
		return Err(crate::error::Error::InvalidArgument(
			"compaction cannot mix branch-owned tables".to_owned(),
		));
	}
	Ok(owner)
}

#[cfg(test)]
mod owner_tests {
	use super::*;
	use crate::batch::BatchOwner;
	use crate::{BranchGeneration, BranchId, Error};

	#[test]
	fn compaction_rejects_mixed_branch_owners_before_writing_output() {
		let first = BatchOwner {
			branch: BranchId::from_u128(1),
			generation: BranchGeneration(1),
		};
		let second = BatchOwner {
			branch: BranchId::from_u128(2),
			generation: BranchGeneration(1),
		};

		let error = common_owner([first, second].into_iter()).unwrap_err();
		assert!(matches!(error, Error::InvalidArgument(_)));
		assert_eq!(common_owner([first, first].into_iter()).unwrap(), first);
	}
}
