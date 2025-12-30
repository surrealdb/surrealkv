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

use crate::error::Error;
use crate::sstable::table::Table;
use crate::vfs::File;
use crate::{Options, Result};

/// Current manifest format version
pub const MANIFEST_FORMAT_VERSION_V1: u16 = 1;

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

/// Represents a set of changes to be applied to the manifest
#[derive(Clone, Default)]
pub(crate) struct ManifestChangeSet {
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

mod iter;
mod level;

pub type HiddenSet = HashSet<u64>;

/// Represents the levels of a log-structured merge tree.
pub(crate) struct LevelManifest {
	/// Path of level manifest file
	pub path: PathBuf,

	/// Levels of the LSM tree
	pub levels: Levels,

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
			levels,
			hidden_set: HashSet::with_capacity(10),
			next_table_id,
			manifest_format_version: MANIFEST_FORMAT_VERSION_V1,
			snapshots: Vec::new(),
			log_number: 0,
			last_sequence: 0,
		};

		// Write levels to disk with the counter
		write_manifest_to_disk(&manifest)?;

		Ok(manifest)
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
	fn load_from_file<P: AsRef<Path>>(manifest_path: P, opts: Arc<Options>) -> Result<Self> {
		log::info!("Loading manifest from {:?}", manifest_path.as_ref());

		// Read and parse the manifest file
		let data = std::fs::read(&manifest_path)?;
		let mut level_manifest = Cursor::new(data);

		// Read versioned manifest format
		let version = level_manifest.read_u16::<BigEndian>()?;
		if version != MANIFEST_FORMAT_VERSION_V1 {
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

		// Read levels data
		let level_data = Levels::decode(&mut level_manifest)?;

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

		// Now convert the level data into actual Level objects with Table instances
		// Use the actual number of levels from the manifest, not the configured level
		// count
		let mut levels_vec = Vec::with_capacity(level_data.len());

		// Load all levels that exist in the manifest
		for (level_idx, table_ids) in level_data.iter().enumerate() {
			let mut tables = Vec::with_capacity(table_ids.len());

			for &table_id in table_ids {
				// Load the actual table from disk
				match Self::load_table(table_id, Arc::clone(&opts)) {
					Ok(table) => tables.push(table),
					Err(err) => {
						log::error!("Error loading table {table_id}: {err:?}");
						return Err(Error::LoadManifestFail(err.to_string()));
					}
				}
			}

			// Validate sequence numbers based on level
			if level_idx > 0 && !tables.is_empty() {
				Self::validate_table_sequence_numbers(level_idx as u8, &tables)?;
			}

			// Create the level with the loaded tables
			let level = Level {
				tables,
			};
			levels_vec.push(Arc::new(level));
		}

		// Create and return the complete manifest
		let total_tables: usize = levels_vec.iter().map(|l| l.tables.len()).sum();

		log::info!(
			"Manifest loaded successfully: version={}, log_number={}, last_sequence={}, tables={}, levels={}",
			version,
			log_number,
			last_sequence,
			total_tables,
			levels_vec.len()
		);

		// Validate last_sequence matches the maximum sequence number across all tables
		let computed_max_seq = levels_vec
			.iter()
			.flat_map(|level| level.tables.iter())
			.map(|table| table.meta.largest_seq_num)
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
			levels: Levels(levels_vec),
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
			// Ensure smallest_seq_num is not greater than largest_seq_num
			if table.meta.smallest_seq_num > table.meta.largest_seq_num {
				return Err(Error::LoadManifestFail(format!(
					"Table {} has invalid sequence numbers: smallest({}) > largest({})",
					table.id, table.meta.smallest_seq_num, table.meta.largest_seq_num
				)));
			}
		}

		// If we have multiple tables, check sequence continuity across all tables
		if tables.len() > 1 {
			for i in 0..tables.len() - 1 {
				let current = &tables[i];
				let next = &tables[i + 1];

				// Check if sequence numbers maintain continuity
				if next.meta.smallest_seq_num <= current.meta.largest_seq_num {
					return Err(Error::LoadManifestFail(format!(
						"Level {} tables have overlapping sequence numbers: Table {} ({}-{}) and Table {} ({}-{})",
						level_idx,
						current.id, current.meta.smallest_seq_num, current.meta.largest_seq_num,
						next.id, next.meta.smallest_seq_num, next.meta.largest_seq_num
					)));
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
		let len = self.levels.as_ref().len() as u8;

		len
	}

	pub(crate) fn last_level_index(&self) -> u8 {
		self.depth() - 1
	}

	pub(crate) fn iter(&self) -> impl Iterator<Item = Arc<Table>> + '_ {
		LevelManifestIterator::new(self)
	}

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

	/// Apply a changeset to this manifest
	pub(crate) fn apply_changeset(&mut self, changeset: &ManifestChangeSet) -> Result<()> {
		// Apply scalar values if present in changeset
		if let Some(version) = changeset.manifest_format_version {
			self.manifest_format_version = version;
		}

		// Apply log_number if present, but only if it's higher
		// This prevents race conditions where concurrent flushes could move log_number
		// backward
		if let Some(log_num) = changeset.log_number {
			if log_num > self.log_number {
				self.log_number = log_num;
			}
		}

		// Add new tables to levels
		for (level, table) in &changeset.new_tables {
			if let Some(level_ref) = self.levels.get_levels_mut().get_mut(*level as usize) {
				let level_mut = Arc::make_mut(level_ref);
				if *level == 0 {
					// Level 0: sorted by sequence number (tables can overlap)
					level_mut.insert(Arc::clone(table));
				} else {
					// Level 1+: sorted by smallest key (tables cannot overlap)
					level_mut.insert_sorted_by_key(Arc::clone(table));
				}
			}

			// Update last_sequence if this table has a higher sequence number
			if table.meta.largest_seq_num > self.last_sequence {
				self.last_sequence = table.meta.largest_seq_num;
			}
		}

		// Delete tables from levels
		for (level, table_id) in &changeset.deleted_tables {
			if let Some(level_ref) = self.levels.get_levels_mut().get_mut(*level as usize) {
				Arc::make_mut(level_ref).remove(*table_id);
			}
		}

		// Delete snapshots
		self.snapshots.retain(|snapshot| !changeset.deleted_snapshots.contains(&snapshot.seq_num));

		// Add new snapshots
		for snapshot in &changeset.new_snapshots {
			self.snapshots.push(snapshot.clone());
		}

		Ok(())
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

	// Optionally, open and sync the updated file to ensure all changes are flushed
	// to disk.
	let updated_file = SysFile::open(target_path)?;
	updated_file.sync_all()?;

	Ok(())
}

/// Write the full versioned manifest to disk
pub(crate) fn write_manifest_to_disk(manifest: &LevelManifest) -> Result<()> {
	let next_table_id = manifest.next_table_id.load(Ordering::SeqCst);
	let total_tables: usize = manifest.levels.get_levels().iter().map(|l| l.tables.len()).sum();

	log::debug!(
		"Writing manifest: version={}, log_number={}, last_sequence={}, next_table_id={}, total_tables={}",
		manifest.manifest_format_version,
		manifest.log_number,
		manifest.last_sequence,
		next_table_id,
		total_tables
	);

	let mut buf = Vec::new();

	// Write header
	buf.write_u16::<BigEndian>(manifest.manifest_format_version)?;
	buf.write_u64::<BigEndian>(next_table_id)?;
	buf.write_u64::<BigEndian>(manifest.log_number)?;
	buf.write_u64::<BigEndian>(manifest.last_sequence)?;

	// Write levels data
	manifest.levels.encode(&mut buf)?;

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

#[cfg(test)]
mod tests {
	use std::fs::{self, File as SysFile};
	use std::sync::atomic::Ordering;

	use test_log::test;

	use super::*;
	use crate::sstable::table::TableWriter;
	use crate::sstable::{InternalKey, InternalKeyKind};
	use crate::IntoBytes;

	// Helper function to create a test table with direct file IO
	fn create_test_table(table_id: u64, num_items: u64, opts: Arc<Options>) -> Result<Arc<Table>> {
		let table_file_path = opts.sstable_file_path(table_id);

		let mut file = SysFile::create(&table_file_path)?;

		// Create TableWriter that writes directly to the file
		let mut writer = TableWriter::new(&mut file, table_id, opts.clone(), 0); // L0 for test

		// Generate and add items
		for i in 0..num_items {
			let key = format!("key_{i:05}");
			let value = format!("value_{i:05}");

			let internal_key =
				InternalKey::new(key.as_bytes().into_bytes(), i + 1, InternalKeyKind::Set, 0);

			writer.add(internal_key, value.as_bytes())?;
		}

		// Finish writing the table
		let size = writer.finish()?;

		// Open the file for reading
		let file = SysFile::open(&table_file_path)?;
		file.sync_all()?;
		let file: Arc<dyn File> = Arc::new(file);

		// Create the table
		let table = Table::new(table_id, opts, file, size as u64)?;

		Ok(Arc::new(table))
	}

	#[test]
	fn test_level_manifest_persistence() {
		let mut opts = Options::default();
		// Set up temporary directory for test
		let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
		let repo_path = temp_dir.path().to_path_buf();
		opts.path = repo_path;
		opts.level_count = 3; // Set level count for the manifest
		let opts = Arc::new(opts);

		// Create sstables directory
		let sstable_path = opts.sstable_dir();
		fs::create_dir_all(&sstable_path).expect("Failed to create sstables directory");

		// Create manifest directory
		fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest directory");

		// Create a new manifest with 3 levels
		let mut manifest = LevelManifest::new(opts.clone()).expect("Failed to create manifest");

		// Create tables and add them to the manifest
		// Create 2 tables for level 0
		let table_id1 = 1;
		let table1 =
			create_test_table(table_id1, 100, opts.clone()).expect("Failed to create table 1");

		let table_id2 = 2;
		let table2 =
			create_test_table(table_id2, 200, opts.clone()).expect("Failed to create table 2");

		// Create a table for level 1
		let table_id3 = 3;
		let table3 =
			create_test_table(table_id3, 300, opts.clone()).expect("Failed to create table 3");

		let expected_next_id = 100;
		manifest.next_table_id.store(expected_next_id, Ordering::SeqCst);

		let changeset = ManifestChangeSet {
			new_tables: vec![
				(0, table1), // Level 0
				(0, table2), // Level 0
				(1, table3), // Level 1
			],
			new_snapshots: vec![
				SnapshotInfo {
					seq_num: 10,
					created_at: std::time::SystemTime::now()
						.duration_since(std::time::UNIX_EPOCH)
						.map(|d| d.as_nanos())
						.unwrap_or(0),
				},
				SnapshotInfo {
					seq_num: 20,
					created_at: std::time::SystemTime::now()
						.duration_since(std::time::UNIX_EPOCH)
						.map(|d| d.as_nanos())
						.unwrap_or(0),
				},
			],
			..Default::default()
		};

		// Apply changeset to manifest
		manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

		// Verify last_sequence was updated correctly (table3 has largest_seq_num = 300)
		assert_eq!(manifest.get_last_sequence(), 300, "last_sequence should be 300");

		// Persist the manifest with our custom next_table_id
		write_manifest_to_disk(&manifest).expect("Failed to write to disk");

		// Load the manifest directly to verify persistence
		let manifest_path = opts.manifest_file_path(0);
		let loaded_manifest = LevelManifest::load_from_file(&manifest_path, opts.clone())
			.expect("Failed to load manifest");

		// Verify all manifest fields were persisted correctly
		assert_eq!(
			loaded_manifest.next_table_id.load(Ordering::SeqCst),
			expected_next_id,
			"Next table ID not persisted correctly"
		);
		assert_eq!(
			loaded_manifest.manifest_format_version, MANIFEST_FORMAT_VERSION_V1,
			"Manifest version not persisted correctly"
		);
		assert_eq!(loaded_manifest.snapshots.len(), 2, "Snapshots not persisted correctly");
		assert_eq!(
			loaded_manifest.snapshots[0].seq_num, 10,
			"First snapshot seq_num not persisted correctly"
		);
		assert_eq!(
			loaded_manifest.snapshots[1].seq_num, 20,
			"Second snapshot seq_num not persisted correctly"
		);

		// Verify level count matches what we created
		assert_eq!(
			loaded_manifest.levels.as_ref().len(),
			opts.level_count as usize,
			"Incorrect number of levels loaded"
		);

		// Verify table IDs were persisted correctly
		let loaded_level0 = &loaded_manifest.levels.as_ref()[0];
		let loaded_level1 = &loaded_manifest.levels.as_ref()[1];

		assert_eq!(loaded_level0.tables.len(), 2, "Level 0 should have 2 tables");
		assert!(
			loaded_level0.tables.iter().any(|t| t.id == table_id1),
			"Level 0 should contain table_id1"
		);
		assert!(
			loaded_level0.tables.iter().any(|t| t.id == table_id2),
			"Level 0 should contain table_id2"
		);

		assert_eq!(loaded_level1.tables.len(), 1, "Level 1 should have 1 table");
		assert!(
			loaded_level1.tables.iter().any(|t| t.id == table_id3),
			"Level 1 should contain table_id3"
		);

		// Create a new manifest from the same path (simulating restart/recovery)
		let new_manifest =
			LevelManifest::new(opts.clone()).expect("Failed to create manifest from existing file");

		// Verify all manifest fields were loaded correctly in the new manifest
		assert_eq!(
			new_manifest.next_table_id.load(Ordering::SeqCst),
			expected_next_id,
			"Next table ID not loaded correctly in new manifest"
		);
		assert_eq!(
			new_manifest.manifest_format_version, MANIFEST_FORMAT_VERSION_V1,
			"Manifest version not loaded correctly"
		);
		assert_eq!(new_manifest.snapshots.len(), 2, "Snapshots not loaded correctly");
		assert_eq!(new_manifest.snapshots[0].seq_num, 10, "First snapshot not loaded correctly");
		assert_eq!(new_manifest.snapshots[1].seq_num, 20, "Second snapshot not loaded correctly");
		assert_eq!(new_manifest.path, opts.manifest_file_path(0), "Path not set correctly");

		// Verify the number of levels in the new manifest
		assert_eq!(
			new_manifest.levels.as_ref().len(),
			opts.level_count as usize,
			"Incorrect number of levels in new manifest"
		);

		// Verify tables were loaded correctly
		let level0 = &new_manifest.levels.as_ref()[0];
		assert_eq!(level0.tables.len(), 2, "Level 0 should have 2 tables");
		assert!(
			level0.tables.iter().any(|t| t.id == table_id1),
			"Level 0 should contain table with ID {table_id1}"
		);
		assert!(
			level0.tables.iter().any(|t| t.id == table_id2),
			"Level 0 should contain table with ID {table_id2}"
		);

		let level1 = &new_manifest.levels.as_ref()[1];
		assert_eq!(level1.tables.len(), 1, "Level 1 should have 1 table");
		assert!(
			level1.tables.iter().any(|t| t.id == table_id3),
			"Level 1 should contain table with ID {table_id3}"
		);

		// Verify table data was loaded correctly by checking all properties
		let table1_reloaded =
			level0.tables.iter().find(|t| t.id == table_id1).expect("Table 1 not found");

		// Check Table1 basic properties
		assert_eq!(table1_reloaded.id, table_id1, "Table 1 ID mismatch");
		assert!(table1_reloaded.file_size > 0, "Table 1 file size should be greater than 0");

		// Check Table1 metadata properties
		let props1 = &table1_reloaded.meta.properties;
		assert_eq!(props1.id, table_id1, "Table 1 properties ID mismatch");
		assert_eq!(props1.num_entries, 100, "Table 1 should have 100 entries");
		assert!(props1.data_size > 0, "Table 1 data size should be greater than 0");
		assert!(props1.created_at > 0, "Table 1 created_at should be set");
		assert_eq!(props1.item_count, 100, "Table 1 item count should match entries");
		assert_eq!(props1.key_count, 100, "Table 1 key count should match entries");
		assert!(props1.block_count > 0, "Table 1 should have at least one block");
		assert_eq!(props1.seqnos, (1, 100), "Table 1 sequence numbers should be (1, 100)");

		// Check Table1 metadata fields
		assert_eq!(
			table1_reloaded.meta.smallest_seq_num, 1,
			"Table 1 smallest seq num should be 1"
		);
		assert_eq!(
			table1_reloaded.meta.largest_seq_num, 100,
			"Table 1 largest seq num should be 100"
		);
		assert!(
			table1_reloaded.meta.has_point_keys.unwrap_or(false),
			"Table 1 should have point keys"
		);
		assert!(
			table1_reloaded.meta.smallest_point.is_some(),
			"Table 1 should have smallest point key"
		);
		assert!(
			table1_reloaded.meta.largest_point.is_some(),
			"Table 1 should have largest point key"
		);

		let table2_reloaded =
			level0.tables.iter().find(|t| t.id == table_id2).expect("Table 2 not found");

		// Check Table2 basic properties
		assert_eq!(table2_reloaded.id, table_id2, "Table 2 ID mismatch");
		assert!(table2_reloaded.file_size > 0, "Table 2 file size should be greater than 0");

		// Check Table2 metadata properties
		let props2 = &table2_reloaded.meta.properties;
		assert_eq!(props2.id, table_id2, "Table 2 properties ID mismatch");
		assert_eq!(props2.num_entries, 200, "Table 2 should have 200 entries");
		assert!(props2.data_size > 0, "Table 2 data size should be greater than 0");
		assert!(props2.created_at > 0, "Table 2 created_at should be set");
		assert_eq!(props2.item_count, 200, "Table 2 item count should match entries");
		assert_eq!(props2.key_count, 200, "Table 2 key count should match entries");
		assert!(props2.block_count > 0, "Table 2 should have at least one block");
		assert_eq!(props2.seqnos, (1, 200), "Table 2 sequence numbers should be (1, 200)");

		// Check Table2 metadata fields
		assert_eq!(
			table2_reloaded.meta.smallest_seq_num, 1,
			"Table 2 smallest seq num should be 1"
		);
		assert_eq!(
			table2_reloaded.meta.largest_seq_num, 200,
			"Table 2 largest seq num should be 200"
		);
		assert!(
			table2_reloaded.meta.has_point_keys.unwrap_or(false),
			"Table 2 should have point keys"
		);
		assert!(
			table2_reloaded.meta.smallest_point.is_some(),
			"Table 2 should have smallest point key"
		);
		assert!(
			table2_reloaded.meta.largest_point.is_some(),
			"Table 2 should have largest point key"
		);

		let table3_reloaded =
			level1.tables.iter().find(|t| t.id == table_id3).expect("Table 3 not found");

		// Check Table3 basic properties
		assert_eq!(table3_reloaded.id, table_id3, "Table 3 ID mismatch");
		assert!(table3_reloaded.file_size > 0, "Table 3 file size should be greater than 0");

		// Check Table3 metadata properties
		let props3 = &table3_reloaded.meta.properties;
		assert_eq!(props3.id, table_id3, "Table 3 properties ID mismatch");
		assert_eq!(props3.num_entries, 300, "Table 3 should have 300 entries");
		assert!(props3.data_size > 0, "Table 3 data size should be greater than 0");
		assert!(props3.created_at > 0, "Table 3 created_at should be set");
		assert_eq!(props3.item_count, 300, "Table 3 item count should match entries");
		assert_eq!(props3.key_count, 300, "Table 3 key count should match entries");
		assert!(props3.block_count > 0, "Table 3 should have at least one block");
		assert_eq!(props3.seqnos, (1, 300), "Table 3 sequence numbers should be (1, 300)");

		// Check Table3 metadata fields
		assert_eq!(
			table3_reloaded.meta.smallest_seq_num, 1,
			"Table 3 smallest seq num should be 1"
		);
		assert_eq!(
			table3_reloaded.meta.largest_seq_num, 300,
			"Table 3 largest seq num should be 300"
		);
		assert!(
			table3_reloaded.meta.has_point_keys.unwrap_or(false),
			"Table 3 should have point keys"
		);
		assert!(
			table3_reloaded.meta.smallest_point.is_some(),
			"Table 3 should have smallest point key"
		);
		assert!(
			table3_reloaded.meta.largest_point.is_some(),
			"Table 3 should have largest point key"
		);

		// Verify table format and compression are set correctly
		assert_eq!(
			props1.table_format,
			crate::sstable::table::TableFormat::LSMV1,
			"Table 1 format should be LSMV1"
		);
		assert_eq!(
			props2.table_format,
			crate::sstable::table::TableFormat::LSMV1,
			"Table 2 format should be LSMV1"
		);
		assert_eq!(
			props3.table_format,
			crate::sstable::table::TableFormat::LSMV1,
			"Table 3 format should be LSMV1"
		);

		// Verify no deletions in test tables
		assert_eq!(props1.num_deletions, 0, "Table 1 should have no deletions");
		assert_eq!(props2.num_deletions, 0, "Table 2 should have no deletions");
		assert_eq!(props3.num_deletions, 0, "Table 3 should have no deletions");

		// Verify tombstone counts
		assert_eq!(props1.tombstone_count, 0, "Table 1 should have no tombstones");
		assert_eq!(props2.tombstone_count, 0, "Table 2 should have no tombstones");
		assert_eq!(props3.tombstone_count, 0, "Table 3 should have no tombstones");
	}

	// Helper function to create a test table with specific sequence numbers
	fn create_test_table_with_seq_nums(
		table_id: u64,
		seq_start: u64,
		seq_end: u64,
		opts: Arc<Options>,
	) -> Result<Arc<Table>> {
		let table_file_path = opts.sstable_file_path(table_id);

		let mut file = SysFile::create(&table_file_path)?;

		// Create TableWriter that writes directly to the file
		let mut writer = TableWriter::new(&mut file, table_id, opts.clone(), 0); // L0 for test

		// Generate and add items with specific sequence numbers
		for seq_num in seq_start..=seq_end {
			let key = format!("key_{seq_num:05}");
			let value = format!("value_{seq_num:05}");

			let internal_key =
				InternalKey::new(key.as_bytes().into_bytes(), seq_num, InternalKeyKind::Set, 0);

			writer.add(internal_key, value.as_bytes())?;
		}

		// Finish writing the table
		let size = writer.finish()?;

		// Open the file for reading
		let file = SysFile::open(&table_file_path)?;
		file.sync_all()?;
		let file: Arc<dyn File> = Arc::new(file);

		// Create the table
		let table = Table::new(table_id, opts, file, size as u64)?;

		Ok(Arc::new(table))
	}

	#[test]
	fn test_lsn_with_multiple_l0_tables() {
		let mut opts = Options::default();
		// Set up temporary directory for test
		let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
		let repo_path = temp_dir.path().to_path_buf();
		opts.path = repo_path;
		opts.level_count = 3;
		let opts = Arc::new(opts);

		// Create sstables directory
		let sstable_path = opts.sstable_dir();
		fs::create_dir_all(&sstable_path).expect("Failed to create sstables directory");

		// Create manifest directory
		fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest directory");

		// Create a new manifest
		let mut manifest = LevelManifest::new(opts.clone()).expect("Failed to create manifest");

		// Test 1: Empty manifest should have last_sequence of 0
		assert_eq!(
			manifest.get_last_sequence(),
			0,
			"Empty manifest should return last_sequence of 0"
		);

		// Test 2: Add single table via changeset
		// Create table with sequence numbers 1-10 (largest_seq_num = 10)
		let table1 = create_test_table_with_seq_nums(1, 1, 10, opts.clone())
			.expect("Failed to create table 1");

		let changeset1 = ManifestChangeSet {
			new_tables: vec![(0, table1)],
			..Default::default()
		};
		manifest.apply_changeset(&changeset1).expect("Failed to apply changeset");

		assert_eq!(manifest.get_last_sequence(), 10, "Single table should update last_sequence");

		// Test 3: Add table with higher sequence numbers
		// Create table with sequence numbers 11-20 (largest_seq_num = 20)
		let table2 = create_test_table_with_seq_nums(2, 11, 20, opts.clone())
			.expect("Failed to create table 2");

		let changeset2 = ManifestChangeSet {
			new_tables: vec![(0, table2)],
			..Default::default()
		};
		manifest.apply_changeset(&changeset2).expect("Failed to apply changeset");

		assert_eq!(
			manifest.get_last_sequence(),
			20,
			"Should update to highest sequence from new table"
		);

		// Test 4: Add table with even higher sequence numbers
		// Create table with sequence numbers 21-30 (largest_seq_num = 30)
		let table3 = create_test_table_with_seq_nums(3, 21, 30, opts.clone())
			.expect("Failed to create table 3");

		let changeset3 = ManifestChangeSet {
			new_tables: vec![(0, table3)],
			..Default::default()
		};
		manifest.apply_changeset(&changeset3).expect("Failed to apply changeset");

		assert_eq!(
			manifest.get_last_sequence(),
			30,
			"Should update to highest sequence after adding new table"
		);

		// Test 5: Verify table ordering - tables should be sorted by largest_seq_num
		// descending
		{
			let level0 = &manifest.levels.get_levels()[0];
			assert_eq!(level0.tables.len(), 3, "Should have 3 tables in L0");

			// Tables should be in descending order of their largest sequence number
			assert_eq!(
				level0.tables[0].meta.largest_seq_num, 30,
				"First table should have highest seq num"
			);
			assert_eq!(
				level0.tables[1].meta.largest_seq_num, 20,
				"Second table should have middle seq num"
			);
			assert_eq!(
				level0.tables[2].meta.largest_seq_num, 10,
				"Third table should have lowest seq num"
			);
		}

		// Test 6: Add table with lower sequence numbers (simulating out-of-order
		// insertion) Create table with sequence numbers 5-8 (largest_seq_num = 8)
		let table4 = create_test_table_with_seq_nums(4, 5, 8, opts.clone())
			.expect("Failed to create table 4");

		let changeset4 = ManifestChangeSet {
			new_tables: vec![(0, table4)],
			..Default::default()
		};
		manifest.apply_changeset(&changeset4).expect("Failed to apply changeset");

		// last_sequence should still be 30 (highest among all tables)
		assert_eq!(
			manifest.get_last_sequence(),
			30,
			"last_sequence should remain highest even after adding table with lower seq nums"
		);

		// Verify correct ordering after out-of-order insertion
		{
			let level0 = &manifest.levels.get_levels()[0];
			assert_eq!(level0.tables.len(), 4, "Should have 4 tables in L0");

			// Tables should still be in descending order of their largest sequence number
			assert_eq!(
				level0.tables[0].meta.largest_seq_num, 30,
				"First table should have seq num 30"
			);
			assert_eq!(
				level0.tables[1].meta.largest_seq_num, 20,
				"Second table should have seq num 20"
			);
			assert_eq!(
				level0.tables[2].meta.largest_seq_num, 10,
				"Third table should have seq num 10"
			);
			assert_eq!(
				level0.tables[3].meta.largest_seq_num, 8,
				"Fourth table should have seq num 8"
			);
		}

		// Test 7: Test with overlapping sequence ranges
		// Create table with sequence numbers 25-35 (largest_seq_num = 35, overlaps with
		// table3)
		let table5 =
			create_test_table_with_seq_nums(5, 25, 35, opts).expect("Failed to create table 5");

		let changeset5 = ManifestChangeSet {
			new_tables: vec![(0, table5)],
			..Default::default()
		};
		manifest.apply_changeset(&changeset5).expect("Failed to apply changeset");

		assert_eq!(
			manifest.get_last_sequence(),
			35,
			"Should return new highest last_sequence from overlapping ranges"
		);

		// Verify final ordering
		{
			let level0 = &manifest.levels.get_levels()[0];
			assert_eq!(level0.tables.len(), 5, "Should have 5 tables in L0");

			// First table should have the highest sequence number
			assert_eq!(
				level0.tables[0].meta.largest_seq_num, 35,
				"First table should have highest seq num 35"
			);
		}
	}

	#[test]
	fn test_last_sequence_persistence_across_manifest_reload() {
		let mut opts = Options::default();
		// Set up temporary directory for test
		let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
		let repo_path = temp_dir.path().to_path_buf();
		opts.path = repo_path;
		opts.level_count = 3;
		let opts = Arc::new(opts);

		// Create sstables directory
		let sstable_path = opts.sstable_dir();
		fs::create_dir_all(&sstable_path).expect("Failed to create sstables directory");

		// Create manifest directory
		fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest directory");

		let expected_last_sequence = 50;

		// Create manifest with tables via changeset and verify last_sequence
		{
			let mut manifest = LevelManifest::new(opts.clone()).expect("Failed to create manifest");

			// Create tables with different sequence ranges
			let table1 = create_test_table_with_seq_nums(1, 1, 20, opts.clone())
				.expect("Failed to create table 1");
			let table2 =
				create_test_table_with_seq_nums(2, 21, expected_last_sequence, opts.clone())
					.expect("Failed to create table 2");
			let table3 = create_test_table_with_seq_nums(3, 10, 30, opts.clone())
				.expect("Failed to create table 3");

			// Add all tables via a single changeset
			let changeset = ManifestChangeSet {
				new_tables: vec![(0, table1), (0, table2), (0, table3)],
				..Default::default()
			};
			manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

			// Verify last_sequence before persistence
			assert_eq!(
				manifest.get_last_sequence(),
				expected_last_sequence,
				"last_sequence should be {} before persistence",
				expected_last_sequence
			);

			// Persist the manifest
			write_manifest_to_disk(&manifest).expect("Failed to write manifest to disk");
		}

		// Reload manifest and verify last_sequence is preserved
		{
			let reloaded_manifest = LevelManifest::new(opts).expect("Failed to reload manifest");

			// Verify last_sequence after reload
			assert_eq!(
				reloaded_manifest.get_last_sequence(),
				expected_last_sequence,
				"last_sequence should be {} after reload",
				expected_last_sequence
			);

			// Verify table count and ordering
			let level0 = &reloaded_manifest.levels.get_levels()[0];
			assert_eq!(level0.tables.len(), 3, "Should have 3 tables after reload");

			// Verify tables are still properly ordered
			assert_eq!(
				level0.tables[0].meta.largest_seq_num, 50,
				"First table should have highest seq num"
			);
			assert_eq!(
				level0.tables[1].meta.largest_seq_num, 30,
				"Second table should have middle seq num"
			);
			assert_eq!(
				level0.tables[2].meta.largest_seq_num, 20,
				"Third table should have lowest seq num"
			);
		}
	}

	#[test]
	fn test_manifest_v1_with_log_number_and_last_sequence() {
		let mut opts = Options::default();
		let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
		let repo_path = temp_dir.path().to_path_buf();
		opts.path = repo_path;
		opts.level_count = 3;
		let opts = Arc::new(opts);

		// Create required directories
		fs::create_dir_all(opts.sstable_dir()).expect("Failed to create sstables dir");
		fs::create_dir_all(opts.manifest_dir()).expect("Failed to create manifest dir");

		// Create a manifest with log_number and last_sequence set
		let mut manifest = LevelManifest::new(opts.clone()).expect("Failed to create manifest");

		// Add a table to ensure non-trivial state
		// Table with sequence numbers 100-200, so last_sequence should be 200
		let table = create_test_table_with_seq_nums(1, 100, 200, opts.clone())
			.expect("Failed to create table");

		// Use changeset to atomically set log_number and add table
		// The changeset will automatically update last_sequence from the table's
		// largest_seq_num
		let changeset = ManifestChangeSet {
			log_number: Some(42),
			new_tables: vec![(0, table)],
			..Default::default()
		};
		manifest.apply_changeset(&changeset).expect("Failed to apply changeset");

		// Verify last_sequence was updated by the changeset
		assert_eq!(
			manifest.get_last_sequence(),
			200,
			"last_sequence should be updated by changeset"
		);

		// Persist to disk
		write_manifest_to_disk(&manifest).expect("Failed to write manifest");

		// Reload and verify
		let loaded_manifest = LevelManifest::new(opts).expect("Failed to reload manifest");

		// Verify format version is still V1
		assert_eq!(
			loaded_manifest.manifest_format_version, MANIFEST_FORMAT_VERSION_V1,
			"Should be V1 format"
		);

		// Verify new fields persisted correctly
		assert_eq!(loaded_manifest.get_log_number(), 42, "log_number should persist");
		assert_eq!(loaded_manifest.get_last_sequence(), 200, "last_sequence should persist");

		// Verify table loaded correctly
		assert_eq!(loaded_manifest.levels.get_levels()[0].tables.len(), 1);
	}
}
