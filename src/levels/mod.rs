use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::File as SysFile;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use iter::LevelManifestIterator;
pub(crate) use level::{Level, Levels};

use crate::authority::format::{BranchStateManifest, RootManifest};
use crate::authority::store::AuthorityStore;
use crate::batch::BatchOwner;
use crate::error::Error;
use crate::sstable::table::Table;
use crate::vfs::File;
use crate::wal::list_segment_ids;
use crate::{Options, Result};

/// Validates that the manifest's log_number doesn't exceed actual WAL segments on disk.
/// This detects manifest corruption that could cause silent data loss.
pub(crate) fn validate_wal_log_number(wal_path: &Path, manifest_log_number: u64) -> Result<()> {
	if let Ok(segment_ids) = list_segment_ids(wal_path, Some("wal")) {
		if !segment_ids.is_empty() {
			let last_wal = *segment_ids.last().unwrap();
			if manifest_log_number > last_wal + 1 {
				return Err(Error::ManifestCorruption(format!(
					"log_number {} exceeds WAL segments (max={}). \
					 Possible manifest corruption or incomplete backup restoration.",
					manifest_log_number, last_wal
				)));
			}
		}
	}
	Ok(())
}

/// Everything `reclaim_owner` removed, kept so a failed publish can put it back.
pub(crate) struct ReclaimedOwner {
	owner: BatchOwner,
	position: usize,
	levels: Levels,
	state_version: Option<u64>,
	retained_floor: Option<u64>,
	tables: Vec<Arc<Table>>,
}

impl ReclaimedOwner {
	/// The tables this owner held, which become unreferenced once the removal
	/// is durable.
	pub(crate) fn tables(&self) -> &[Arc<Table>] {
		&self.tables
	}
}

/// Represents a set of changes to be applied to the manifest.
///
/// A changeset mutates exactly one physical owner's component set. Flush and
/// compaction are single-owner operations by construction, so a mixed-owner
/// changeset cannot be expressed.
#[derive(Clone, Default)]
pub(crate) struct ManifestChangeSet {
	/// Physical owner whose level set this changeset mutates. Every table in
	/// `new_tables` must carry this owner in its persisted metadata; apply
	/// fails closed otherwise.
	pub owner: BatchOwner,

	/// Tables to delete from manifest
	pub deleted_tables: HashSet<(u8, u64)>, // (level, table_id)

	/// Tables to add to manifest
	pub new_tables: Vec<(u8, Arc<Table>)>, // (level, table)

	/// New log_number to set (if Some)
	/// Indicates that WALs with number < log_number have been flushed
	pub log_number: Option<u64>,
}

/// Data needed to revert an applied changeset
pub(crate) struct ChangeSetRollback {
	/// Owner whose level set the changeset mutated
	pub owner: BatchOwner,
	/// Tables that were deleted (need to be re-added on revert)
	pub deleted_tables: Vec<(u8, Arc<Table>)>,
	/// Table IDs that were added (need to be removed on revert)
	pub added_table_ids: Vec<(u8, u64)>,
	/// Previous log_number if it was updated
	pub prev_log_number: Option<u64>,
	/// Previous last_sequence value
	pub prev_last_sequence: u64,
}

mod iter;
mod level;

pub type HiddenSet = HashSet<u64>;

/// Represents the levels of a log-structured merge tree, partitioned by
/// physical owner.
///
/// Each `BatchOwner` has its own complete `Levels` set; a table belongs to
/// exactly one owner's set. Reads select an owner via [`Self::levels_for`] —
/// there is deliberately no owner-blind level access, so a read path cannot
/// scan globally and filter afterwards. Table IDs remain globally unique
/// across all owners (one `next_table_id` counter).
pub(crate) struct LevelManifest {
	/// Durable lineage access (numbered immutable catalog/state/root
	/// versions); see docs/FK_AUTHORITY_FORK_DESIGN.md.
	pub(crate) authority: AuthorityStore,

	/// Per-owner level sets. Small cardinality; ordered by first appearance.
	levels_by_owner: Vec<(BatchOwner, Levels)>,

	/// Set of hidden tables that should not appear during compaction
	pub(crate) hidden_set: HiddenSet,

	/// Next table ID to use (persisted as a block-reserved watermark in root
	/// versions; recovery resumes at the watermark so ids are never reused)
	pub(crate) next_table_id: Arc<AtomicU64>,

	/// Last published state version per owner (0 = never published).
	state_versions: HashMap<BatchOwner, u64>,

	/// Lowest sequence at which a view of an owner is still complete. Raised by
	/// the compaction that drops versions and published with that compaction's
	/// state version; a historical fork below it is refused rather than served
	/// short of rows (FK4 amendment 2). Absent = nothing was ever dropped.
	retained_floors: HashMap<BatchOwner, u64>,

	/// Last published root version (0 = never published).
	root_version: u64,

	/// Newest successfully published catalog version, mirrored here for root
	/// publication. Written only after a catalog publish succeeds, so it can lag
	/// but never lead: a root recording a stale floor makes the open-time
	/// truncation check weaker, never wrong.
	pub(crate) catalog_version: Arc<AtomicU64>,

	/// Shared global commit timeline; root publishes snapshot its tail.
	timeline: std::sync::Arc<crate::timeline::Timeline>,

	/// Consulted before each durable publish. Carried here rather than passed
	/// per call because `persist_owner_update` and `persist_root` are reached
	/// from several places and none of them should have to know about it.
	fault_policy: Arc<dyn crate::failpoints::FaultPolicy>,

	/// Minimum WAL number that contains unflushed data.
	/// All WAL files with number < log_number have been flushed to SST and can
	/// be safely deleted.
	pub(crate) log_number: u64,

	/// Last sequence number persisted in the manifest.
	/// Tracks the highest sequence number across all SSTables.
	/// Updated when new tables are added during flush operations.
	pub(crate) last_sequence: u64,
}

/// Table ids are handed out in blocks of this size: each root version
/// persists a watermark at the next block boundary above the allocated
/// counter, so a crash wastes at most one block and never reuses an id.
pub(crate) const TABLE_ID_BLOCK: u64 = 1024;

impl LevelManifest {
	/// Fresh in-memory manifest for a store with no durable states yet.
	/// Nothing is published here — the first flush publishes state+root.
	#[cfg(test)]
	pub(crate) fn fresh(opts: Arc<Options>, authority: AuthorityStore) -> Self {
		assert!(opts.level_count > 0, "level_count should be >= 1");
		Self {
			fault_policy: Arc::clone(&opts.fault_policy),
			authority,
			levels_by_owner: vec![(BatchOwner::DEFAULT, Self::initialize_levels(opts.level_count))],
			hidden_set: HashSet::with_capacity(10),
			next_table_id: Arc::new(AtomicU64::new(1)),
			state_versions: HashMap::new(),
			retained_floors: HashMap::new(),
			root_version: 0,
			catalog_version: Arc::new(AtomicU64::new(0)),
			timeline: std::sync::Arc::new(crate::timeline::Timeline::new()),
			log_number: 0,
			last_sequence: 0,
		}
	}

	/// Rebuilds the runtime manifest from durable lineages: one state per
	/// Active catalog branch (absent = the branch never flushed), floors and
	/// the table-id watermark from the newest root. Fail-closed: an
	/// unloadable state or a table whose persisted owner disagrees with its
	/// listing refuses the open.
	pub(crate) fn hydrate(
		opts: Arc<Options>,
		authority: AuthorityStore,
		catalog: &crate::branch::BranchCatalog,
		catalog_version: u64,
		root: Option<&RootManifest>,
		timeline: std::sync::Arc<crate::timeline::Timeline>,
	) -> Result<Self> {
		assert!(opts.level_count > 0, "level_count should be >= 1");

		let log_number = root.map(|r| r.wal_reclaim_floor).unwrap_or(0);
		validate_wal_log_number(&opts.wal_dir(), log_number)?;

		let mut seen_table_ids: HashMap<u64, BatchOwner> = HashMap::new();
		let mut levels_by_owner: Vec<(BatchOwner, Levels)> = Vec::new();
		let mut state_versions: HashMap<BatchOwner, u64> = HashMap::new();
		let mut retained_floors: HashMap<BatchOwner, u64> = HashMap::new();
		let mut last_sequence = root.map(|r| r.visible_seq).unwrap_or(0);
		let mut max_referenced_id = 0u64;

		for record in catalog.all_records().filter(|record| !record.deleted) {
			let owner = BatchOwner {
				branch: record.id,
				generation: record.generation,
			};
			let hint = root
				.and_then(|r| {
					r.state_hints
						.iter()
						.find(|(branch, generation, _)| {
							*branch == record.id && *generation == record.generation
						})
						.map(|(_, _, version)| *version)
				})
				.unwrap_or(0);
			let Some(state) = authority.load_state(record.id, record.generation, hint)? else {
				// Never flushed: legitimately no state lineage. The DEFAULT
				// owner still needs its (empty) runtime level set.
				if owner == BatchOwner::DEFAULT {
					levels_by_owner.push((owner, Self::initialize_levels(opts.level_count)));
				}
				continue;
			};

			let mut levels_vec = Vec::with_capacity(state.levels.len().max(1));
			for (level_idx, table_ids) in state.levels.iter().enumerate() {
				let mut tables = Vec::with_capacity(table_ids.len());
				for &table_id in table_ids {
					if let Some(previous_owner) = seen_table_ids.insert(table_id, owner) {
						return Err(Error::LoadManifestFail(format!(
							"Table {table_id} is listed more than once in owned component sets (owners {previous_owner:?} and {owner:?}); a table has exactly one physical placement"
						)));
					}
					max_referenced_id = max_referenced_id.max(table_id);
					let table = match Self::load_table(table_id, Arc::clone(&opts)) {
						Ok(table) => table,
						Err(err) => {
							log::error!("Error loading table {table_id}: {err:?}");
							return Err(Error::LoadManifestFail(err.to_string()));
						}
					};
					if table.meta.owner != owner {
						return Err(Error::LoadManifestFail(format!(
							"Table {} is listed under owner {:?} but its persisted metadata names owner {:?}",
							table_id, owner, table.meta.owner
						)));
					}
					tables.push(table);
				}
				if level_idx > 0 && !tables.is_empty() {
					Self::validate_level_tables(level_idx as u8, &tables)?;
				}
				levels_vec.push(Arc::new(Level {
					tables,
				}));
			}
			while levels_vec.len() < opts.level_count as usize {
				levels_vec.push(Arc::new(Level::default()));
			}
			last_sequence = last_sequence.max(state.last_sequence);
			state_versions.insert(owner, state.state_version);
			if state.retained_floor_seq > 0 {
				retained_floors.insert(owner, state.retained_floor_seq);
			}
			levels_by_owner.push((owner, Levels(levels_vec)));
		}

		if !levels_by_owner.iter().any(|(owner, _)| *owner == BatchOwner::DEFAULT) {
			levels_by_owner
				.insert(0, (BatchOwner::DEFAULT, Self::initialize_levels(opts.level_count)));
		}

		// The watermark from root is the floor; states may reference ids the
		// root never recorded (a state published, then the root publish was
		// lost). Resume strictly above both, at a fresh block boundary.
		let watermark = root.map(|r| r.next_table_id).unwrap_or(1);
		let next_table_id =
			watermark.max((max_referenced_id / TABLE_ID_BLOCK + 1) * TABLE_ID_BLOCK);

		Ok(Self {
			fault_policy: Arc::clone(&opts.fault_policy),
			authority,
			levels_by_owner,
			hidden_set: HashSet::with_capacity(10),
			next_table_id: Arc::new(AtomicU64::new(next_table_id)),
			state_versions,
			retained_floors,
			root_version: root.map(|r| r.root_version).unwrap_or(0),
			catalog_version: Arc::new(AtomicU64::new(catalog_version)),
			timeline,
			log_number,
			last_sequence,
		})
	}

	/// Test-only: manifest whose default owner set is `levels`, with empty
	/// snapshots and zeroed floors (the shape the retained tests construct).
	#[cfg(test)]
	pub(crate) fn new_for_test(
		path: std::path::PathBuf,
		levels: Levels,
		next_table_id: Arc<AtomicU64>,
	) -> Self {
		Self {
			fault_policy: Arc::new(crate::failpoints::NoFaults),
			authority: AuthorityStore::new(path, [0; 16]),
			levels_by_owner: vec![(BatchOwner::DEFAULT, levels)],
			hidden_set: HashSet::new(),
			next_table_id,
			state_versions: HashMap::new(),
			retained_floors: HashMap::new(),
			root_version: 0,
			catalog_version: Arc::new(AtomicU64::new(0)),
			timeline: std::sync::Arc::new(crate::timeline::Timeline::new()),
			log_number: 0,
			last_sequence: 0,
		}
	}

	/// Level set for one physical owner. This is the only read access to
	/// levels; owner-blind level iteration deliberately does not exist.
	pub(crate) fn levels_for(&self, owner: BatchOwner) -> Option<&Levels> {
		self.levels_by_owner
			.iter()
			.find(|(set_owner, _)| *set_owner == owner)
			.map(|(_, levels)| levels)
	}

	/// Every physical owner that currently has a level set, default first.
	pub(crate) fn owners(&self) -> Vec<BatchOwner> {
		self.levels_by_owner.iter().map(|(owner, _)| *owner).collect()
	}

	/// Drops a reclaimed owner's entire durable footprint from the runtime
	/// manifest and returns the tables that are now unreferenced, so the caller
	/// can delete their files.
	///
	/// Only legal for an owner the catalog has tombstoned: this is the same
	/// reachability rule `cleanup_orphaned_sst_files` applies at open, run
	/// without waiting for a restart. The state lineage on disk is left alone —
	/// metadata pruning caps it, and nothing loads a deleted branch's state.
	pub(crate) fn reclaim_owner(&mut self, owner: BatchOwner) -> Option<ReclaimedOwner> {
		let position = self.levels_by_owner.iter().position(|(set, _)| *set == owner)?;
		let (_, levels) = self.levels_by_owner.remove(position);
		let state_version = self.state_versions.remove(&owner);
		let retained_floor = self.retained_floors.remove(&owner);
		let mut tables = Vec::new();
		for level in levels.get_levels() {
			for table in &level.tables {
				// A table hidden by an in-flight compaction is still this
				// owner's and still unreferenced once the owner is gone; the
				// compaction's own output is refused by the liveness guard.
				tables.push(Arc::clone(table));
			}
		}
		Some(ReclaimedOwner {
			owner,
			position,
			levels,
			state_version,
			retained_floor,
			tables,
		})
	}

	/// Puts back what [`LevelManifest::reclaim_owner`] took.
	///
	/// The removal has to happen before the root is published — the root's
	/// contents are derived from `state_versions`, so it cannot describe an
	/// owner's absence until the owner is absent. That ordering means a failed
	/// publish leaves memory ahead of disk, and this is what closes the gap.
	/// Every other publish path in this engine rolls back the same way.
	pub(crate) fn restore_owner(&mut self, reclaimed: ReclaimedOwner) {
		let ReclaimedOwner {
			owner,
			position,
			levels,
			state_version,
			retained_floor,
			..
		} = reclaimed;
		let position = position.min(self.levels_by_owner.len());
		self.levels_by_owner.insert(position, (owner, levels));
		if let Some(version) = state_version {
			self.state_versions.insert(owner, version);
		}
		if let Some(floor) = retained_floor {
			self.retained_floors.insert(owner, floor);
		}
	}

	/// Lowest sequence at which a view of `owner` is still complete. Zero means
	/// no compaction has ever dropped one of its versions, so every historical
	/// point is intact.
	pub(crate) fn retained_floor(&self, owner: BatchOwner) -> u64 {
		self.retained_floors.get(&owner).copied().unwrap_or(0)
	}

	/// Raises `owner`'s retention floor. Called with the advance a compaction
	/// computed, before that compaction's state version is published, so the
	/// floor and the level set it describes become durable together.
	pub(crate) fn raise_retained_floor(&mut self, owner: BatchOwner, floor: u64) {
		if floor == 0 {
			return;
		}
		let entry = self.retained_floors.entry(owner).or_insert(0);
		*entry = (*entry).max(floor);
	}

	fn levels_for_mut(&mut self, owner: BatchOwner) -> Option<&mut Levels> {
		self.levels_by_owner
			.iter_mut()
			.find(|(set_owner, _)| *set_owner == owner)
			.map(|(_, levels)| levels)
	}

	/// Level set for the retained default branch. Explicit convenience for
	/// the single-branch integration period and its tests.
	pub(crate) fn default_owner_levels(&self) -> &Levels {
		self.levels_for(BatchOwner::DEFAULT).expect("default owner level set always exists")
	}

	/// Test-only mutable twin of [`Self::default_owner_levels`]; production
	/// mutation goes through [`Self::apply_changeset`] exclusively.
	#[cfg(test)]
	pub(crate) fn default_owner_levels_mut(&mut self) -> &mut Levels {
		self.levels_for_mut(BatchOwner::DEFAULT).expect("default owner level set always exists")
	}

	/// Creates the owner's level set on first use. New sets take the same
	/// depth as the default set so level indexes mean the same thing for
	/// every owner.
	fn ensure_owner_levels(&mut self, owner: BatchOwner) -> &mut Levels {
		if self.levels_for(owner).is_none() {
			let depth = self.depth() as usize;
			self.levels_by_owner.push((owner, Levels::new(depth, 0)));
		}
		self.levels_for_mut(owner).expect("owner level set was just ensured")
	}

	/// Returns the minimum WAL number that contains unflushed data
	pub(crate) fn get_log_number(&self) -> u64 {
		self.log_number
	}

	/// Returns the last sequence number persisted in the manifest
	pub(crate) fn get_last_sequence(&self) -> u64 {
		self.last_sequence
	}

	/// Initializes levels with default values
	fn initialize_levels(level_count: u8) -> Levels {
		let levels = (0..level_count).map(|_| Arc::new(Level::default())).collect::<Vec<_>>();

		Levels(levels)
	}

	/// Checks what a level below L0 must satisfy for its readers to be correct.
	fn validate_level_tables(level_idx: u8, tables: &[Arc<Table>]) -> Result<()> {
		// Basic sanity check for all tables
		for table in tables {
			// Ensure both sequence numbers exist (they should always be set together)
			// and that smallest_seq_num is not greater than largest_seq_num
			let (smallest, largest) =
				match (table.meta.smallest_seq_num, table.meta.largest_seq_num) {
					(Some(s), Some(l)) => (s, l),
					(None, None) => {
						return Err(Error::LoadManifestFail(format!(
							"Table {} has no sequence numbers (possibly corrupted or empty table)",
							table.id
						)));
					}
					(smallest, largest) => {
						return Err(Error::LoadManifestFail(format!(
							"Table {} has inconsistent sequence numbers: smallest={:?}, largest={:?}",
							table.id, smallest, largest
						)));
					}
				};

			if smallest > largest {
				return Err(Error::LoadManifestFail(format!(
					"Table {} has invalid sequence numbers: smallest({}) > largest({})",
					table.id, smallest, largest
				)));
			}
		}

		// A level below L0 partitions the key space, and the order it partitions
		// it in is the order recorded here: `hydrate` pushes tables in manifest
		// order and nothing sorts them afterwards, while
		// `Level::find_first_overlapping_table` reads them with
		// `slice::partition_point`. A binary search over an unsorted vector does
		// not return a wrong answer loudly; it returns a wrong answer.
		//
		// Until 2026-08-15 this checked ascending, disjoint *sequence* ranges
		// instead — an ordering nothing establishes and no reader wants. Key
		// order and sequence order are independent: writing a high key before a
		// low one is enough to make them disagree, and that made the store
		// refuse to open. See the V8b as-built record.
		for pair in tables.windows(2) {
			let (current, next) = (&pair[0], &pair[1]);
			// A table with no point-key metadata carries no range to compare;
			// `is_before_range` already treats that as "assume overlap".
			let (Some(current_largest), Some(next_smallest)) =
				(&current.meta.largest_point, &next.meta.smallest_point)
			else {
				continue;
			};
			if current.opts.comparator.compare(&current_largest.user_key, &next_smallest.user_key)
				== Ordering::Greater
			{
				return Err(Error::LoadManifestFail(format!(
					"Level {} tables are not in ascending key order: Table {} ends at {:?} but the \
					 next table {} starts at {:?}; levels below L0 are searched by binary search \
					 and must partition the key space in order",
					level_idx,
					current.id,
					String::from_utf8_lossy(&current_largest.user_key),
					next.id,
					String::from_utf8_lossy(&next_smallest.user_key),
				)));
			}
		}

		Ok(())
	}

	/// Helper to load a single table by ID
	fn load_table(table_id: u64, opts: Arc<Options>) -> Result<Arc<Table>> {
		let table_file_path = opts.sstable_file_path(table_id);

		// Open the table file
		let file = SysFile::open(&table_file_path)?;
		let file: Arc<dyn File> = Arc::new(file);
		let file_size = file.size()?;

		// Create and return the table
		let table = Arc::new(Table::new(table_id, opts, file, file_size)?);
		Ok(table)
	}

	fn depth(&self) -> u8 {
		// All owner sets share the default set's depth; `ensure_owner_levels`
		// creates new sets at this depth so level indexes are comparable.
		self.default_owner_levels().as_ref().len() as u8
	}

	pub(crate) fn last_level_index(&self) -> u8 {
		self.depth() - 1
	}

	/// Lifecycle-only iteration over every table of every owner (checkpoint,
	/// orphan cleanup, recovery accounting). Read paths must use
	/// [`Self::levels_for`] instead — never iterate globally and filter.
	pub(crate) fn iter(&self) -> impl Iterator<Item = Arc<Table>> + '_ {
		LevelManifestIterator::new(self)
	}

	/// Lifecycle-only: all tables across all owners, keyed by globally unique
	/// table ID. See [`Self::iter`] for the read-path prohibition.
	pub(crate) fn get_all_tables(&self) -> HashMap<u64, Arc<Table>> {
		let mut output = HashMap::new();

		for table in self.iter() {
			output.insert(table.meta.properties.id, table);
		}

		output
	}

	pub(crate) fn unhide_tables(&mut self, keys: &[u64]) {
		for key in keys {
			self.hidden_set.remove(key);
		}
	}

	pub(crate) fn hide_tables(&mut self, keys: &[u64]) {
		for key in keys {
			self.hidden_set.insert(*key);
		}
	}

	/// Apply a changeset to this manifest and return rollback data.
	///
	/// Fails closed before any mutation if a new table's persisted owner does
	/// not match the changeset owner — a mixed-owner component set must be
	/// unrepresentable, not merely unlikely.
	pub(crate) fn apply_changeset(
		&mut self,
		changeset: &ManifestChangeSet,
	) -> Result<ChangeSetRollback> {
		// Owner validation happens before any state changes.
		for (_, table) in &changeset.new_tables {
			if table.meta.owner != changeset.owner {
				return Err(Error::Corruption(format!(
					"changeset for owner {:?} adds table {} owned by {:?}; mixed-owner component sets are prohibited",
					changeset.owner, table.id, table.meta.owner
				)));
			}
		}

		let mut rollback = ChangeSetRollback {
			owner: changeset.owner,
			deleted_tables: Vec::new(),
			added_table_ids: Vec::new(),
			prev_log_number: None,
			prev_last_sequence: self.last_sequence,
		};

		// Apply log_number if present, but only if it's higher
		// This prevents race conditions where concurrent flushes could move log_number
		// backward
		if let Some(log_num) = changeset.log_number {
			if log_num > self.log_number {
				rollback.prev_log_number = Some(self.log_number);
				self.log_number = log_num;
			}
		}

		let owner_levels = self.ensure_owner_levels(changeset.owner);

		// Capture deleted tables BEFORE removing them, then remove
		for (level, table_id) in &changeset.deleted_tables {
			if let Some(level_ref) = owner_levels.get_levels_mut().get_mut(*level as usize) {
				// Find and capture the table before removing
				if let Some(table) = level_ref.tables.iter().find(|t| t.id == *table_id) {
					rollback.deleted_tables.push((*level, Arc::clone(table)));
				}
				Arc::make_mut(level_ref).remove(*table_id);
			}
		}

		// Add new tables to the owner's levels and track their IDs
		for (level, table) in &changeset.new_tables {
			if let Some(level_ref) = owner_levels.get_levels_mut().get_mut(*level as usize) {
				let level_mut = Arc::make_mut(level_ref);
				if *level == 0 {
					// Level 0: sorted by sequence number (tables can overlap)
					level_mut.insert(Arc::clone(table));
				} else {
					// Level 1+: sorted by smallest key (tables cannot overlap)
					level_mut.insert_sorted_by_key(Arc::clone(table));
				}
				rollback.added_table_ids.push((*level, table.id));
			}
		}

		// Update last_sequence across the added tables
		for (_, table) in &changeset.new_tables {
			let largest = table.meta.largest_seq_num.expect("table must have largest_seq_num");
			if largest > self.last_sequence {
				self.last_sequence = largest;
			}
		}

		Ok(rollback)
	}

	/// Revert a previously applied changeset using rollback data
	pub(crate) fn revert_changeset(&mut self, rollback: ChangeSetRollback) {
		// Restore last_sequence
		self.last_sequence = rollback.prev_last_sequence;

		// Restore log_number if it was changed
		if let Some(prev_log_number) = rollback.prev_log_number {
			self.log_number = prev_log_number;
		}

		// Undo within the same owner's level set the changeset mutated
		if let Some(owner_levels) = self.levels_for_mut(rollback.owner) {
			// Remove tables that were added
			for (level, table_id) in rollback.added_table_ids {
				if let Some(level_ref) = owner_levels.get_levels_mut().get_mut(level as usize) {
					Arc::make_mut(level_ref).remove(table_id);
				}
			}

			// Re-add tables that were deleted
			for (level, table) in rollback.deleted_tables {
				if let Some(level_ref) = owner_levels.get_levels_mut().get_mut(level as usize) {
					let level_mut = Arc::make_mut(level_ref);
					if level == 0 {
						// Level 0: sorted by sequence number (tables can overlap)
						level_mut.insert(table);
					} else {
						// Level 1+: sorted by smallest key (tables cannot overlap)
						level_mut.insert_sorted_by_key(table);
					}
				}
			}
		}
	}

	/// Publishes the touched owner's state version, then a root version —
	/// the replacement for the deleted whole-file manifest write. Called
	/// under the manifest write lock (same blocking profile as before).
	///
	/// A root failure after a state success does NOT roll the version
	/// counters back: the published state is a valid superset whose
	/// referenced SSTs were made durable before this call (existing flush /
	/// compaction ordering), the stale root hint is healed by forward
	/// probing, and the next successful publish supersedes it.
	pub(crate) fn persist_owner_update(&mut self, owner: BatchOwner) -> Result<()> {
		self.fault_policy.check(crate::failpoints::FaultPoint::OwnerStatePublish)?;
		let next_state = self.state_versions.get(&owner).copied().unwrap_or(0) + 1;
		let retained_floor_seq = self.retained_floor(owner);
		let levels: Vec<Vec<u64>> = self
			.levels_for(owner)
			.ok_or_else(|| {
				Error::Corruption(format!("persisting owner {owner:?} with no level set"))
			})?
			.get_levels()
			.iter()
			.map(|level| level.tables.iter().map(|table| table.id).collect())
			.collect();
		let state = BranchStateManifest {
			branch: owner.branch,
			generation: owner.generation,
			state_version: next_state,
			last_sequence: self.last_sequence,
			flushed_log_number: self.log_number,
			retained_floor_seq,
			levels,
		};
		self.authority.publish_state(&state)?;
		self.state_versions.insert(owner, next_state);
		self.persist_root()
	}

	/// Publishes a root version alone (floor-only transitions, e.g. the
	/// shutdown replay-floor advance).
	pub(crate) fn persist_root(&mut self) -> Result<()> {
		self.fault_policy.check(crate::failpoints::FaultPoint::RootPublish)?;
		let next_root = self.root_version + 1;
		let allocated = self.next_table_id.load(std::sync::atomic::Ordering::SeqCst);
		let watermark = (allocated / TABLE_ID_BLOCK + 1) * TABLE_ID_BLOCK;

		// One hint per branch: after delete/recreate within one process the
		// map can hold a dead generation's entry — keep the newest
		// generation only, and sort strictly by branch id (format law).
		let mut hints: Vec<(crate::BranchId, crate::BranchGeneration, u64)> = Vec::new();
		for (owner, version) in &self.state_versions {
			match hints.iter_mut().find(|(branch, _, _)| *branch == owner.branch) {
				Some(existing) if existing.1 .0 < owner.generation.0 => {
					*existing = (owner.branch, owner.generation, *version);
				}
				Some(_) => {}
				None => hints.push((owner.branch, owner.generation, *version)),
			}
		}
		hints.sort_by_key(|hint| hint.0 .0);

		let root = RootManifest {
			db_id: self.authority.db_id,
			root_version: next_root,
			visible_seq: self.last_sequence,
			last_commit_ts: self.timeline.last_commit_ts(),
			timeline_tail: self.timeline.tail(crate::authority::format::MAX_TIMELINE_TAIL),
			wal_reclaim_floor: self.log_number,
			next_table_id: watermark,
			catalog_version_floor: self.catalog_version.load(std::sync::atomic::Ordering::Acquire),
			state_hints: hints,
		};
		self.authority.publish_root(&root)?;
		self.root_version = next_root;
		Ok(())
	}

	/// Generates the next unique table ID for a new SSTable
	/// This is the single source of truth for table ID generation
	pub(crate) fn next_table_id(&self) -> u64 {
		self.next_table_id.fetch_add(1, std::sync::atomic::Ordering::Release)
	}
}
