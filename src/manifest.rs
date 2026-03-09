use std::collections::HashSet;
use std::fs::File as SysFile;
use std::io::{Cursor, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use rand::Rng;

use crate::error::Error;
use crate::levels::level::Levels;
use crate::levels::{Level, LevelManifest};
use crate::sstable::sst_id::SstId;
use crate::sstable::table::Table;
use crate::tablestore::TableStore;
use crate::wal::list_segment_ids;
use crate::{Options, Result};

/// Manifest format version V3: full metadata (footer + TableMetadata) per table
pub const MANIFEST_FORMAT_VERSION_V3: u16 = 3;

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
	pub deleted_tables: HashSet<(u8, SstId)>, // (level, table_id)

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
	/// Tables that were deleted (need to be re-added on revert)
	pub deleted_tables: Vec<(u8, Arc<Table>)>,
	/// Table IDs that were added (need to be removed on revert)
	pub added_table_ids: Vec<(u8, SstId)>,
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

/// Uploads manifest bytes to the object store in the background.
///
/// Uses a bounded channel — if the channel is full (receiver is slow),
/// the oldest upload is simply skipped. The next manifest write will
/// upload a fresher version anyway.
pub(crate) struct ManifestUploader {
	sender: tokio::sync::mpsc::Sender<(u64, bytes::Bytes)>,
}

impl ManifestUploader {
	pub(crate) fn new(sender: tokio::sync::mpsc::Sender<(u64, bytes::Bytes)>) -> Self {
		Self {
			sender,
		}
	}

	/// Queue a manifest upload. Non-blocking; drops if channel is full.
	pub(crate) fn queue_upload(&self, manifest_id: u64, data: Vec<u8>) {
		let _ = self.sender.try_send((manifest_id, bytes::Bytes::from(data)));
	}
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

/// Load a manifest from file and return a complete LevelManifest instance
pub(crate) async fn load_from_file<P: AsRef<Path>>(
	manifest_path: P,
	manifest_dir: PathBuf,
	opts: Arc<Options>,
	table_store: Arc<TableStore>,
) -> Result<LevelManifest> {
	log::info!("Loading manifest from {:?}", manifest_path.as_ref());
	let data = std::fs::read(&manifest_path)?;
	load_from_bytes(&data, manifest_dir, opts, table_store).await
}

/// Load a manifest from raw bytes.
pub(crate) async fn load_from_bytes(
	data: &[u8],
	manifest_dir: PathBuf,
	opts: Arc<Options>,
	table_store: Arc<TableStore>,
) -> Result<LevelManifest> {
	log::info!("Loading manifest from bytes ({} bytes)", data.len());

	let mut cursor = Cursor::new(data);

	let version = cursor.read_u16::<BigEndian>()?;
	if version != MANIFEST_FORMAT_VERSION_V3 {
		return Err(Error::LoadManifestFail(format!(
			"Unsupported manifest format version: {}",
			version
		)));
	}

	let manifest_id = cursor.read_u64::<BigEndian>()?;
	let log_number = cursor.read_u64::<BigEndian>()?;
	let last_sequence = cursor.read_u64::<BigEndian>()?;

	log::debug!(
		"Manifest header: version={}, manifest_id={}, log_number={}, last_sequence={}",
		version,
		manifest_id,
		log_number,
		last_sequence
	);

	validate_wal_log_number(&opts.wal_dir(), log_number)?;

	let level_data = Levels::decode(&mut cursor)?;

	let snapshot_count = cursor.read_u32::<BigEndian>()?;
	let mut snapshots = Vec::new();
	for _ in 0..snapshot_count {
		let snapshot_len = cursor.read_u32::<BigEndian>()? as usize;
		let mut snapshot_bytes = vec![0u8; snapshot_len];
		cursor.read_exact(&mut snapshot_bytes)?;
		let snapshot = SnapshotInfo::decode(&snapshot_bytes)?;
		snapshots.push(snapshot);
	}

	// Convert table entries into actual Level objects using pre-loaded metadata
	let mut levels_vec = Vec::with_capacity(level_data.len());
	for (level_idx, table_entries) in level_data.into_iter().enumerate() {
		let mut tables = Vec::with_capacity(table_entries.len());
		for entry in table_entries {
			match Table::open(
				entry.id,
				Arc::clone(&opts),
				Arc::clone(&table_store),
				entry.file_size,
				entry.footer,
				entry.metadata,
			)
			.await
			{
				Ok(table) => tables.push(Arc::new(table)),
				Err(err) => {
					log::error!("Error opening table {}: {err:?}", entry.id);
					return Err(Error::LoadManifestFail(err.to_string()));
				}
			}
		}

		if level_idx > 0 && !tables.is_empty() {
			validate_table_sequence_numbers(level_idx as u8, &tables)?;
		}

		levels_vec.push(Arc::new(Level {
			tables,
		}));
	}

	let total_tables: usize = levels_vec.iter().map(|l| l.tables.len()).sum();
	log::info!(
		"Manifest loaded: version={}, manifest_id={}, log_number={}, last_sequence={}, tables={}, levels={}",
		version, manifest_id, log_number, last_sequence, total_tables, levels_vec.len()
	);

	let computed_max_seq = levels_vec
		.iter()
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

	Ok(LevelManifest {
		manifest_dir,
		manifest_id,
		levels: Levels(levels_vec),
		hidden_set: HashSet::with_capacity(10),
		manifest_format_version: version,
		snapshots,
		log_number,
		last_sequence,
	})
}

fn validate_table_sequence_numbers(level_idx: u8, tables: &[Arc<Table>]) -> Result<()> {
	for table in tables {
		let (smallest, largest) = match (table.meta.smallest_seq_num, table.meta.largest_seq_num) {
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

	if tables.len() > 1 {
		for i in 0..tables.len() - 1 {
			let current = &tables[i];
			let next = &tables[i + 1];

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

/// Apply a changeset to a manifest and return rollback data
pub(crate) fn apply_changeset(
	manifest: &mut LevelManifest,
	changeset: &ManifestChangeSet,
) -> Result<ChangeSetRollback> {
	let mut rollback = ChangeSetRollback {
		deleted_tables: Vec::new(),
		added_table_ids: Vec::new(),
		deleted_snapshots: Vec::new(),
		added_snapshot_seqs: Vec::new(),
		prev_version: None,
		prev_log_number: None,
		prev_last_sequence: manifest.last_sequence,
	};

	if let Some(version) = changeset.manifest_format_version {
		rollback.prev_version = Some(manifest.manifest_format_version);
		manifest.manifest_format_version = version;
	}

	if let Some(log_num) = changeset.log_number {
		if log_num > manifest.log_number {
			rollback.prev_log_number = Some(manifest.log_number);
			manifest.log_number = log_num;
		}
	}

	for (level, table_id) in &changeset.deleted_tables {
		if let Some(level_ref) = manifest.levels.get_levels_mut().get_mut(*level as usize) {
			if let Some(table) = level_ref.tables.iter().find(|t| t.id == *table_id) {
				rollback.deleted_tables.push((*level, Arc::clone(table)));
			}
			Arc::make_mut(level_ref).remove(*table_id);
		}
	}

	for (level, table) in &changeset.new_tables {
		if let Some(level_ref) = manifest.levels.get_levels_mut().get_mut(*level as usize) {
			let level_mut = Arc::make_mut(level_ref);
			if *level == 0 {
				level_mut.insert(Arc::clone(table));
			} else {
				level_mut.insert_sorted_by_key(Arc::clone(table));
			}
			rollback.added_table_ids.push((*level, table.id));
		}

		let largest = table.meta.largest_seq_num.expect("table must have largest_seq_num");
		if largest > manifest.last_sequence {
			manifest.last_sequence = largest;
		}
	}

	let deleted: Vec<_> = manifest
		.snapshots
		.iter()
		.filter(|s| changeset.deleted_snapshots.contains(&s.seq_num))
		.cloned()
		.collect();
	rollback.deleted_snapshots = deleted;
	manifest.snapshots.retain(|snapshot| !changeset.deleted_snapshots.contains(&snapshot.seq_num));

	for snapshot in &changeset.new_snapshots {
		manifest.snapshots.push(snapshot.clone());
		rollback.added_snapshot_seqs.push(snapshot.seq_num);
	}

	Ok(rollback)
}

/// Revert a previously applied changeset using rollback data
pub(crate) fn revert_changeset(manifest: &mut LevelManifest, rollback: ChangeSetRollback) {
	manifest.last_sequence = rollback.prev_last_sequence;

	if let Some(prev_version) = rollback.prev_version {
		manifest.manifest_format_version = prev_version;
	}

	if let Some(prev_log_number) = rollback.prev_log_number {
		manifest.log_number = prev_log_number;
	}

	for (level, table_id) in rollback.added_table_ids {
		if let Some(level_ref) = manifest.levels.get_levels_mut().get_mut(level as usize) {
			Arc::make_mut(level_ref).remove(table_id);
		}
	}

	for (level, table) in rollback.deleted_tables {
		if let Some(level_ref) = manifest.levels.get_levels_mut().get_mut(level as usize) {
			let level_mut = Arc::make_mut(level_ref);
			if level == 0 {
				level_mut.insert(table);
			} else {
				level_mut.insert_sorted_by_key(table);
			}
		}
	}

	manifest.snapshots.retain(|s| !rollback.added_snapshot_seqs.contains(&s.seq_num));

	for snapshot in rollback.deleted_snapshots {
		manifest.snapshots.push(snapshot);
	}
}

/// Serialize the manifest to bytes.
pub(crate) fn serialize_manifest(manifest: &LevelManifest) -> Result<Vec<u8>> {
	let mut buf = Vec::new();

	// Write header: version | manifest_id | log_number | last_sequence
	buf.write_u16::<BigEndian>(manifest.manifest_format_version)?;
	buf.write_u64::<BigEndian>(manifest.manifest_id)?;
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

	Ok(buf)
}

/// Write the manifest to a new versioned file on disk. Increments manifest_id.
/// Returns the serialized bytes so callers can forward them to the manifest uploader.
pub(crate) fn write_manifest_to_disk(manifest: &mut LevelManifest) -> Result<Vec<u8>> {
	manifest.manifest_id += 1;

	let total_tables: usize = manifest.levels.get_levels().iter().map(|l| l.tables.len()).sum();

	log::debug!(
		"Writing manifest: version={}, manifest_id={}, log_number={}, last_sequence={}, total_tables={}",
		manifest.manifest_format_version,
		manifest.manifest_id,
		manifest.log_number,
		manifest.last_sequence,
		total_tables
	);

	let buf = serialize_manifest(manifest)?;

	let path = manifest.manifest_dir.join(format!("{:020}.manifest", manifest.manifest_id));
	replace_file_content(&path, &buf)?;
	log::debug!("Manifest written successfully to {:?}", path);
	Ok(buf)
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

	let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
	let temp_filename = format!(".tmp_{}_{}", timestamp, rand::rng().random::<u64>());
	let temp_path = directory.join(temp_filename);

	let original_permissions = std::fs::metadata(target_path).ok().map(|m| m.permissions());

	{
		let mut temp_file = SysFile::create(&temp_path)?;
		temp_file.write_all(new_content)?;
		temp_file.sync_all()?;
	}

	if let Some(permissions) = original_permissions {
		if let Err(e) = std::fs::set_permissions(&temp_path, permissions) {
			let _ = std::fs::remove_file(&temp_path);
			return Err(e);
		}
	}

	if let Err(e) = std::fs::rename(&temp_path, target_path) {
		let _ = std::fs::remove_file(&temp_path);
		return Err(e);
	}

	let updated_file = crate::vfs::open_for_sync(target_path)?;
	updated_file.sync_all()?;

	Ok(())
}

/// Find the latest local manifest file in a directory.
/// Returns (manifest_id, path) if found.
pub(crate) fn find_latest_local_manifest(manifest_dir: &Path) -> Option<(u64, PathBuf)> {
	let entries = std::fs::read_dir(manifest_dir).ok()?;
	let mut latest: Option<(u64, PathBuf)> = None;

	for entry in entries.flatten() {
		let name = entry.file_name();
		let name_str = name.to_string_lossy();
		if let Some(id_str) = name_str.strip_suffix(".manifest") {
			if let Ok(id) = id_str.parse::<u64>() {
				if latest.as_ref().map_or(true, |(best, _)| id > *best) {
					latest = Some((id, entry.path()));
				}
			}
		}
	}

	latest
}
