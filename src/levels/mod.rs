use std::collections::{HashMap, HashSet};
use std::fs::File as SysFile;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use iter::LevelManifestIterator;
pub(crate) use level::{Level, Levels};
use rand::Rng;

use crate::batch::BatchOwner;
use crate::error::Error;
use crate::sstable::table::Table;
use crate::vfs::File;
use crate::wal::list_segment_ids;
use crate::{Options, Result};

fn encode_owner<W: Write>(writer: &mut W, owner: BatchOwner) -> Result<()> {
	writer.write_all(&owner.branch.0)?;
	writer.write_u64::<BigEndian>(owner.generation.0)?;
	Ok(())
}

fn decode_owner<R: Read>(reader: &mut R) -> Result<BatchOwner> {
	let mut branch = [0u8; 16];
	reader.read_exact(&mut branch)?;
	let generation = reader.read_u64::<BigEndian>()?;
	Ok(BatchOwner {
		branch: crate::BranchId(branch),
		generation: crate::BranchGeneration(generation),
	})
}

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

/// Owner-partitioned manifest layout: levels are stored per physical
/// `BatchOwner`. This rewrite line starts at 1 and carries no on-disk
/// compatibility promise; any other version — including earlier lines'
/// formats — is rejected by identity, and pre-rewrite files that happen to
/// share the number fail the structural owner validation instead.
pub const MANIFEST_FORMAT_VERSION: u16 = 1;

/// Snapshot information stored in the manifest
#[derive(Debug, Clone)]
pub(crate) struct SnapshotInfo {
	/// Snapshot sequence number
	pub seq_num: u64,
	/// Creation timestamp (system time in nanoseconds)
	pub created_at: u128,
}

impl SnapshotInfo {
	pub(crate) fn encode(&self) -> Result<Vec<u8>> {
		let mut buf = Vec::new();
		buf.write_u64::<BigEndian>(self.seq_num)?;
		buf.write_u128::<BigEndian>(self.created_at)?;
		Ok(buf)
	}

	pub(crate) fn decode(mut buf: &[u8]) -> Result<Self> {
		let seq_num = buf.read_u64::<BigEndian>()?;
		let created_at = buf.read_u128::<BigEndian>()?;
		Ok(Self {
			seq_num,
			created_at,
		})
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

	/// Manifest format version if changed
	pub manifest_format_version: Option<u16>,

	/// Tables to delete from manifest
	pub deleted_tables: HashSet<(u8, u64)>, // (level, table_id)

	/// Tables to add to manifest
	pub new_tables: Vec<(u8, Arc<Table>)>, // (level, table)

	/// Snapshots to add
	pub new_snapshots: Vec<SnapshotInfo>,

	/// Snapshots to delete (by sequence number)
	pub deleted_snapshots: HashSet<u64>,

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
	/// Snapshots that were deleted
	pub deleted_snapshots: Vec<SnapshotInfo>,
	/// Snapshot seq_nums that were added
	pub added_snapshot_seqs: Vec<u64>,
	/// Previous manifest_format_version if changed
	pub prev_version: Option<u16>,
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
	/// Path of level manifest file
	pub path: PathBuf,

	/// Per-owner level sets. Small cardinality; ordered by first appearance.
	levels_by_owner: Vec<(BatchOwner, Levels)>,

	/// Set of hidden tables that should not appear during compaction
	pub(crate) hidden_set: HiddenSet,

	/// Next table ID to use (persisted to disk for safe recovery)
	pub(crate) next_table_id: Arc<AtomicU64>,

	/// Manifest format version to allow schema evolution
	pub manifest_format_version: u16,

	/// A list of read snapshots that are currently open
	pub snapshots: Vec<SnapshotInfo>,

	/// Minimum WAL number that contains unflushed data.
	/// All WAL files with number < log_number have been flushed to SST and can
	/// be safely deleted.
	pub(crate) log_number: u64,

	/// Last sequence number persisted in the manifest.
	/// Tracks the highest sequence number across all SSTables.
	/// Updated when new tables are added during flush operations.
	pub(crate) last_sequence: u64,
}

impl LevelManifest {
	pub(crate) fn new(opts: Arc<Options>) -> Result<Self> {
		assert!(opts.level_count > 0, "level_count should be >= 1");

		let manifest_file_path = opts.manifest_file_path(0);

		// Check if the manifest file already exists
		if manifest_file_path.exists() {
			// Load existing manifest from file
			return Self::load_from_file(&manifest_file_path, opts);
		}

		// If no manifest exists, create a new one

		// Initialize levels with default values
		let levels = Self::initialize_levels(opts.level_count);

		// Start with next_table_id = 1 (0 is reserved)
		let next_table_id = Arc::new(AtomicU64::new(1));

		let manifest = Self {
			path: manifest_file_path,
			levels_by_owner: vec![(BatchOwner::DEFAULT, levels)],
			hidden_set: HashSet::with_capacity(10),
			next_table_id,
			manifest_format_version: MANIFEST_FORMAT_VERSION,
			snapshots: Vec::new(),
			log_number: 0,
			last_sequence: 0,
		};

		// Write levels to disk with the counter
		write_manifest_to_disk(&manifest)?;

		Ok(manifest)
	}

	/// Test-only: manifest whose default owner set is `levels`, with empty
	/// snapshots and zeroed floors (the shape the retained tests construct).
	#[cfg(test)]
	pub(crate) fn new_for_test(
		path: PathBuf,
		levels: Levels,
		next_table_id: Arc<AtomicU64>,
	) -> Self {
		Self {
			path,
			levels_by_owner: vec![(BatchOwner::DEFAULT, levels)],
			hidden_set: HashSet::new(),
			next_table_id,
			manifest_format_version: MANIFEST_FORMAT_VERSION,
			snapshots: Vec::new(),
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

	/// Load a manifest from file and return a complete LevelManifest instance
	pub(crate) fn load_from_file<P: AsRef<Path>>(
		manifest_path: P,
		opts: Arc<Options>,
	) -> Result<Self> {
		log::info!("Loading manifest from {:?}", manifest_path.as_ref());

		// Read and parse the manifest file
		let data = std::fs::read(&manifest_path)?;
		let mut level_manifest = Cursor::new(data);

		// Read versioned manifest format. Earlier layouts are rejected by
		// identity: this line makes no on-disk compatibility promise.
		let version = level_manifest.read_u16::<BigEndian>()?;
		if version != MANIFEST_FORMAT_VERSION {
			return Err(Error::LoadManifestFail(format!(
				"Unsupported manifest format version: {}",
				version
			)));
		}

		let next_table_id = level_manifest.read_u64::<BigEndian>()?;
		let log_number = level_manifest.read_u64::<BigEndian>()?;
		let last_sequence = level_manifest.read_u64::<BigEndian>()?;

		log::debug!(
			"Manifest header: version={}, next_table_id={}, log_number={}, last_sequence={}",
			version,
			next_table_id,
			log_number,
			last_sequence
		);

		// Validate log_number against actual WAL segments BEFORE proceeding
		validate_wal_log_number(&opts.wal_dir(), log_number)?;

		// Read per-owner level sets
		let owner_count = level_manifest.read_u32::<BigEndian>()? as usize;
		if owner_count == 0 {
			return Err(Error::LoadManifestFail(
				"Manifest contains no owner level sets".to_string(),
			));
		}
		let mut owner_level_data = Vec::with_capacity(owner_count);
		for _ in 0..owner_count {
			let owner = decode_owner(&mut level_manifest)?;
			let level_data = Levels::decode(&mut level_manifest)?;
			if owner_level_data.iter().any(|(existing, _)| *existing == owner) {
				return Err(Error::LoadManifestFail(format!(
					"Manifest lists owner {owner:?} twice"
				)));
			}
			owner_level_data.push((owner, level_data));
		}
		if !owner_level_data.iter().any(|(owner, _)| *owner == BatchOwner::DEFAULT) {
			return Err(Error::LoadManifestFail(
				"Manifest is missing the default owner level set".to_string(),
			));
		}

		// Read snapshots
		let snapshot_count = level_manifest.read_u32::<BigEndian>()?;
		let mut snapshots = Vec::new();
		for _ in 0..snapshot_count {
			let snapshot_len = level_manifest.read_u32::<BigEndian>()? as usize;
			let mut snapshot_bytes = vec![0u8; snapshot_len];
			level_manifest.read_exact(&mut snapshot_bytes)?;
			let snapshot = SnapshotInfo::decode(&snapshot_bytes)?;
			snapshots.push(snapshot);
		}

		// Convert the level data into Level objects with loaded Table
		// instances, validating physical ownership fail-closed: a table listed
		// under one owner whose persisted metadata names another owner, or a
		// table listed under two owners, is corruption.
		let mut seen_table_ids: HashMap<u64, BatchOwner> = HashMap::new();
		let mut levels_by_owner = Vec::with_capacity(owner_level_data.len());
		let mut total_tables = 0usize;

		for (owner, level_data) in &owner_level_data {
			let mut levels_vec = Vec::with_capacity(level_data.len());
			for (level_idx, table_ids) in level_data.iter().enumerate() {
				let mut tables = Vec::with_capacity(table_ids.len());

				for &table_id in table_ids {
					if let Some(previous_owner) = seen_table_ids.insert(table_id, *owner) {
						return Err(Error::LoadManifestFail(format!(
							"Table {table_id} is listed more than once in owned component sets (owners {previous_owner:?} and {owner:?}); a table has exactly one physical placement"
						)));
					}
					// Load the actual table from disk
					let table = match Self::load_table(table_id, Arc::clone(&opts)) {
						Ok(table) => table,
						Err(err) => {
							log::error!("Error loading table {table_id}: {err:?}");
							return Err(Error::LoadManifestFail(err.to_string()));
						}
					};
					if table.meta.owner != *owner {
						return Err(Error::LoadManifestFail(format!(
							"Table {} is listed under owner {:?} but its persisted metadata names owner {:?}",
							table_id, owner, table.meta.owner
						)));
					}
					tables.push(table);
				}

				// Validate sequence numbers inside this owner's level
				if level_idx > 0 && !tables.is_empty() {
					Self::validate_table_sequence_numbers(level_idx as u8, &tables)?;
				}

				total_tables += tables.len();
				levels_vec.push(Arc::new(Level {
					tables,
				}));
			}
			levels_by_owner.push((*owner, Levels(levels_vec)));
		}

		log::info!(
			"Manifest loaded successfully: version={}, log_number={}, last_sequence={}, tables={}, owners={}",
			version,
			log_number,
			last_sequence,
			total_tables,
			levels_by_owner.len()
		);

		// Validate last_sequence matches the maximum sequence number across
		// all owners' tables
		let computed_max_seq = levels_by_owner
			.iter()
			.flat_map(|(_, levels)| levels.get_levels().iter())
			.flat_map(|level| level.tables.iter())
			.filter_map(|table| table.meta.largest_seq_num)
			.max()
			.unwrap_or(0);

		if computed_max_seq != last_sequence {
			return Err(Error::LoadManifestFail(format!(
				"Manifest last_sequence mismatch: stored={}, computed from tables={}",
				last_sequence, computed_max_seq
			)));
		}

		Ok(Self {
			path: manifest_path.as_ref().to_path_buf(),
			levels_by_owner,
			hidden_set: HashSet::with_capacity(10),
			next_table_id: Arc::new(AtomicU64::new(next_table_id)),
			manifest_format_version: version,
			snapshots,
			log_number,
			last_sequence,
		})
	}

	fn validate_table_sequence_numbers(level_idx: u8, tables: &[Arc<Table>]) -> Result<()> {
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

		// If we have multiple tables, check sequence continuity across all tables
		if tables.len() > 1 {
			for i in 0..tables.len() - 1 {
				let current = &tables[i];
				let next = &tables[i + 1];

				// Check if sequence numbers maintain continuity
				if let (Some(next_smallest), Some(current_largest)) =
					(next.meta.smallest_seq_num, current.meta.largest_seq_num)
				{
					if next_smallest <= current_largest {
						return Err(Error::LoadManifestFail(format!(
							"Level {} tables have overlapping sequence numbers: Table {} ({:?}-{:?}) and Table {} ({:?}-{:?})",
							level_idx,
							current.id, current.meta.smallest_seq_num, current.meta.largest_seq_num,
							next.id, next.meta.smallest_seq_num, next.meta.largest_seq_num
						)));
					}
				}
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
			deleted_snapshots: Vec::new(),
			added_snapshot_seqs: Vec::new(),
			prev_version: None,
			prev_log_number: None,
			prev_last_sequence: self.last_sequence,
		};

		// Capture and apply version change
		if let Some(version) = changeset.manifest_format_version {
			rollback.prev_version = Some(self.manifest_format_version);
			self.manifest_format_version = version;
		}

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

		// Capture and delete snapshots
		let deleted: Vec<_> = self
			.snapshots
			.iter()
			.filter(|s| changeset.deleted_snapshots.contains(&s.seq_num))
			.cloned()
			.collect();
		rollback.deleted_snapshots = deleted;
		self.snapshots.retain(|snapshot| !changeset.deleted_snapshots.contains(&snapshot.seq_num));

		// Add new snapshots and track their seq_nums
		for snapshot in &changeset.new_snapshots {
			self.snapshots.push(snapshot.clone());
			rollback.added_snapshot_seqs.push(snapshot.seq_num);
		}

		Ok(rollback)
	}

	/// Revert a previously applied changeset using rollback data
	pub(crate) fn revert_changeset(&mut self, rollback: ChangeSetRollback) {
		// Restore last_sequence
		self.last_sequence = rollback.prev_last_sequence;

		// Restore version if it was changed
		if let Some(prev_version) = rollback.prev_version {
			self.manifest_format_version = prev_version;
		}

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

		// Remove snapshots that were added
		self.snapshots.retain(|s| !rollback.added_snapshot_seqs.contains(&s.seq_num));

		// Re-add snapshots that were deleted
		for snapshot in rollback.deleted_snapshots {
			self.snapshots.push(snapshot);
		}
	}

	/// Generates the next unique table ID for a new SSTable
	/// This is the single source of truth for table ID generation
	pub(crate) fn next_table_id(&self) -> u64 {
		self.next_table_id.fetch_add(1, std::sync::atomic::Ordering::Release)
	}
}

/// Safely updates a file's content.
pub(crate) fn replace_file_content<P: AsRef<Path>>(
	file_path: P,
	new_content: &[u8],
) -> std::io::Result<()> {
	let target_path = file_path.as_ref();
	let directory = target_path
		.parent()
		.ok_or(std::io::Error::new(std::io::ErrorKind::NotFound, "Parent directory not found"))?;

	// Generate a unique temporary filename
	let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
	let temp_filename = format!(".tmp_{}_{}", timestamp, rand::rng().random::<u64>());
	let temp_path = directory.join(temp_filename);

	// Get original file permissions if the file exists
	let original_permissions = std::fs::metadata(target_path).ok().map(|m| m.permissions());

	// Create and write to the temporary file
	{
		let mut temp_file = SysFile::create(&temp_path)?;
		temp_file.write_all(new_content)?;
		temp_file.sync_all()?;
	}

	// Apply original permissions to temp file if they exist
	if let Some(permissions) = original_permissions {
		if let Err(e) = std::fs::set_permissions(&temp_path, permissions) {
			// Clean up temp file on permission error
			let _ = std::fs::remove_file(&temp_path);
			return Err(e);
		}
	}

	// Atomically replace the target file with the temporary file
	if let Err(e) = std::fs::rename(&temp_path, target_path) {
		// Clean up temp file on rename failure
		let _ = std::fs::remove_file(&temp_path);
		return Err(e);
	}

	let updated_file = crate::vfs::open_for_sync(target_path)?;
	updated_file.sync_all()?;

	// Make the rename itself durable: without a parent-directory fsync, a
	// crash can revert the directory entry to the old file even though the
	// new content was synced.
	crate::lsm::fsync_directory(directory)?;

	Ok(())
}

/// Write the full versioned manifest to disk
pub(crate) fn write_manifest_to_disk(manifest: &LevelManifest) -> Result<()> {
	let next_table_id = manifest.next_table_id.load(Ordering::SeqCst);
	let total_tables: usize = manifest
		.levels_by_owner
		.iter()
		.flat_map(|(_, levels)| levels.get_levels().iter())
		.map(|l| l.tables.len())
		.sum();

	log::debug!(
		"Writing manifest: version={}, log_number={}, last_sequence={}, next_table_id={}, total_tables={}, owners={}",
		manifest.manifest_format_version,
		manifest.log_number,
		manifest.last_sequence,
		next_table_id,
		total_tables,
		manifest.levels_by_owner.len()
	);

	let mut buf = Vec::new();

	// Write header
	buf.write_u16::<BigEndian>(manifest.manifest_format_version)?;
	buf.write_u64::<BigEndian>(next_table_id)?;
	buf.write_u64::<BigEndian>(manifest.log_number)?;
	buf.write_u64::<BigEndian>(manifest.last_sequence)?;

	// Write per-owner level sets
	buf.write_u32::<BigEndian>(manifest.levels_by_owner.len() as u32)?;
	for (owner, levels) in &manifest.levels_by_owner {
		encode_owner(&mut buf, *owner)?;
		levels.encode(&mut buf)?;
	}

	// Write snapshots
	buf.write_u32::<BigEndian>(manifest.snapshots.len() as u32)?;
	for snapshot in &manifest.snapshots {
		let snapshot_bytes = snapshot.encode()?;
		buf.write_u32::<BigEndian>(snapshot_bytes.len() as u32)?;
		buf.extend_from_slice(&snapshot_bytes);
	}

	replace_file_content(&manifest.path, &buf)?;
	log::debug!("Manifest written successfully to {:?}", manifest.path);
	Ok(())
}
