use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;

use iter::LevelManifestIterator;
pub(crate) use level::{Level, Levels};

use crate::manifest::{
	apply_changeset,
	find_latest_local_manifest,
	load_from_bytes,
	load_from_file,
	replace_file_content,
	revert_changeset,
	write_manifest_to_disk,
	ChangeSetRollback,
	ManifestChangeSet,
	SnapshotInfo,
	MANIFEST_FORMAT_VERSION_V3,
};
use crate::sstable::sst_id::SstId;
use crate::sstable::table::Table;
use crate::tablestore::TableStore;
use crate::{Options, Result};

mod iter;
pub(crate) mod level;

pub type HiddenSet = HashSet<SstId>;

/// Generates a new globally unique SSTable ID using ULID.
pub(crate) fn next_sst_id() -> SstId {
	ulid::Ulid::new()
}

/// Represents the levels of a log-structured merge tree.
pub(crate) struct LevelManifest {
	/// Directory where manifest files are stored
	pub manifest_dir: PathBuf,

	/// Monotonically increasing manifest version. Each write creates a new file.
	pub(crate) manifest_id: u64,

	/// Levels of the LSM tree
	pub levels: Levels,

	/// Set of hidden tables that should not appear during compaction
	pub(crate) hidden_set: HiddenSet,

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
	pub(crate) async fn new(opts: Arc<Options>, table_store: Arc<TableStore>) -> Result<Self> {
		assert!(opts.level_count > 0, "level_count should be >= 1");

		let manifest_dir = opts.manifest_dir();

		// Find latest local manifest
		let local_latest = find_latest_local_manifest(&manifest_dir);

		// Find latest remote manifest
		let remote_ids = table_store.list_manifest_ids().await.unwrap_or_default();
		let remote_latest_id = remote_ids.last().copied();

		match (local_latest, remote_latest_id) {
			// Both exist: compare manifest_id (higher wins)
			(Some((local_id, local_path)), Some(remote_id)) => {
				if remote_id > local_id {
					log::info!(
						"Using remote manifest (id: remote={} > local={})",
						remote_id,
						local_id
					);
					let remote_bytes = table_store.read_manifest(remote_id).await?;
					let local_path = opts.manifest_file_path(remote_id);
					replace_file_content(&local_path, &remote_bytes)?;
					load_from_bytes(
						&remote_bytes,
						manifest_dir,
						Arc::clone(&opts),
						Arc::clone(&table_store),
					)
					.await
				} else {
					log::info!(
						"Using local manifest (id: local={} >= remote={})",
						local_id,
						remote_id
					);
					load_from_file(&local_path, manifest_dir, opts, table_store).await
				}
			}
			// Only local exists
			(Some((_local_id, local_path)), None) => {
				load_from_file(&local_path, manifest_dir, opts, table_store).await
			}
			// Only remote exists
			(None, Some(remote_id)) => {
				log::info!("Restoring manifest from object store (id={})", remote_id);
				let remote_bytes = table_store.read_manifest(remote_id).await?;
				let local_path = opts.manifest_file_path(remote_id);
				replace_file_content(&local_path, &remote_bytes)?;
				load_from_bytes(
					&remote_bytes,
					manifest_dir,
					Arc::clone(&opts),
					Arc::clone(&table_store),
				)
				.await
			}
			// Neither exists: fresh database
			(None, None) => {
				let levels = Self::initialize_levels(opts.level_count);

				let mut manifest = Self {
					manifest_dir,
					manifest_id: 0,
					levels,
					hidden_set: HashSet::with_capacity(10),
					manifest_format_version: MANIFEST_FORMAT_VERSION_V3,
					snapshots: Vec::new(),
					log_number: 0,
					last_sequence: 0,
				};

				write_manifest_to_disk(&mut manifest)?;
				Ok(manifest)
			}
		}
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

	fn depth(&self) -> u8 {
		self.levels.as_ref().len() as u8
	}

	pub(crate) fn last_level_index(&self) -> u8 {
		self.depth() - 1
	}

	pub(crate) fn iter(&self) -> impl Iterator<Item = Arc<Table>> + '_ {
		LevelManifestIterator::new(self)
	}

	pub(crate) fn get_all_tables(&self) -> HashMap<SstId, Arc<Table>> {
		let mut output = HashMap::new();
		for table in self.iter() {
			output.insert(table.id, table);
		}
		output
	}

	pub(crate) fn unhide_tables(&mut self, keys: &[SstId]) {
		for key in keys {
			self.hidden_set.remove(key);
		}
	}

	pub(crate) fn hide_tables(&mut self, keys: &[SstId]) {
		for key in keys {
			self.hidden_set.insert(*key);
		}
	}

	/// Apply a changeset to this manifest and return rollback data
	pub(crate) fn apply_changeset(
		&mut self,
		changeset: &ManifestChangeSet,
	) -> Result<ChangeSetRollback> {
		apply_changeset(self, changeset)
	}

	/// Revert a previously applied changeset using rollback data
	pub(crate) fn revert_changeset(&mut self, rollback: ChangeSetRollback) {
		revert_changeset(self, rollback)
	}

	/// Move a table from source level to target level without merge.
	pub(crate) fn move_table(
		&mut self,
		source_level: u8,
		target_level: u8,
		table_id: SstId,
	) -> crate::Result<()> {
		let levels = self.levels.get_levels_mut();

		let source = levels.get_mut(source_level as usize).ok_or_else(|| {
			crate::error::Error::Other(format!("Invalid source level: {}", source_level))
		})?;
		let source_mut = Arc::make_mut(source);
		let table = source_mut
			.tables
			.iter()
			.find(|t| t.id == table_id)
			.cloned()
			.ok_or(crate::error::Error::TableNotFound(table_id))?;
		source_mut.remove(table_id);

		let target = levels.get_mut(target_level as usize).ok_or_else(|| {
			crate::error::Error::Other(format!("Invalid target level: {}", target_level))
		})?;
		let target_mut = Arc::make_mut(target);
		if target_level == 0 {
			target_mut.insert(table);
		} else {
			target_mut.insert_sorted_by_key(table);
		}

		Ok(())
	}
}
