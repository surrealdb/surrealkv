use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Error, Result};
use crate::levels::LevelManifest;
use crate::lsm::CoreInner;

/// Recursively copies a directory and all its contents
fn copy_dir_all(src: &Path, dst: &Path) -> std::io::Result<()> {
	fs::create_dir_all(dst)?;

	for entry in fs::read_dir(src)? {
		let entry = entry?;
		let src_path = entry.path();
		let dst_path = dst.join(entry.file_name());

		if src_path.is_dir() {
			copy_dir_all(&src_path, &dst_path)?;
		} else {
			fs::copy(&src_path, &dst_path)?;
		}
	}

	Ok(())
}

/// Resolves the links, `.` and `..` of `path`, which need not exist. Components are followed one
/// at a time and canonicalized while they exist; the ones after the first that does not are plain
/// names that creating the directory would make, so a `..` among them is resolved by name.
fn resolve(path: &Path) -> std::io::Result<PathBuf> {
	let path = if path.is_absolute() {
		path.to_path_buf()
	} else {
		std::env::current_dir()?.join(path)
	};
	let mut resolved = PathBuf::new();
	for component in path.components() {
		match component {
			Component::CurDir => {}
			Component::ParentDir => {
				resolved.pop();
			}
			component => {
				resolved.push(component);
				match fs::canonicalize(&resolved) {
					Ok(canonical) => resolved = canonical,
					Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
					Err(e) => return Err(e),
				}
			}
		}
	}
	Ok(resolved)
}

/// Keeps `Core::close` and the checkpoints from running at the same time.
///
/// A checkpoint holds a [`CheckpointGuard`] for as long as it runs. `close` refuses the
/// checkpoints that would begin after it and waits for the ones running, without holding a
/// thread: a checkpoint is synchronous and needs nothing from a runtime to finish.
#[derive(Default)]
pub(crate) struct CheckpointGate {
	state: parking_lot::Mutex<GateState>,
	/// Wakes `close` when the last running checkpoint ends.
	idle: tokio::sync::Notify,
}

#[derive(Default)]
struct GateState {
	running: usize,
	closed: bool,
}

/// A checkpoint that is running; see [`CheckpointGate`].
pub(crate) struct CheckpointGuard<'a> {
	gate: &'a CheckpointGate,
}

impl CheckpointGate {
	/// Lets a checkpoint begin, unless `close` has.
	pub(crate) fn enter(&self) -> Result<CheckpointGuard<'_>> {
		let mut state = self.state.lock();
		if state.closed {
			return Err(Error::Other(
				"Cannot create a checkpoint: the tree is closing or closed".to_string(),
			));
		}
		state.running += 1;
		Ok(CheckpointGuard {
			gate: self,
		})
	}

	/// Refuses the checkpoints that begin from now on and waits for the ones running. Only
	/// `Core::close` calls it, one caller at a time, so a single wake-up is enough. If the future
	/// is dropped before the wait is over, checkpoints are allowed again.
	pub(crate) async fn close(&self) {
		let reopen = Reopen(self);
		self.state.lock().closed = true;
		while self.running() > 0 {
			self.idle.notified().await;
		}
		std::mem::forget(reopen);
	}

	fn running(&self) -> usize {
		self.state.lock().running
	}
}

/// Allows checkpoints again when dropped; see [`CheckpointGate::close`].
struct Reopen<'a>(&'a CheckpointGate);

impl Drop for Reopen<'_> {
	fn drop(&mut self) {
		self.0.state.lock().closed = false;
	}
}

impl Drop for CheckpointGuard<'_> {
	fn drop(&mut self) {
		let mut state = self.gate.state.lock();
		state.running -= 1;
		if state.closed && state.running == 0 {
			self.gate.idle.notify_one();
		}
	}
}

/// Current checkpoint metadata format version
const CHECKPOINT_VERSION: u32 = 1;

/// Checkpoint file name
const CHECKPOINT_METADATA_FILE: &str = "CHECKPOINT_METADATA";

/// Database checkpoint metadata
#[derive(Debug, Clone)]
pub struct CheckpointMetadata {
	/// Format version for compatibility checking
	pub version: u32,
	/// Timestamp when the checkpoint was created
	pub timestamp: u64,
	/// Sequence number at the time of checkpoint
	pub sequence_number: u64,
	/// Number of SSTables included in the checkpoint
	pub sstable_count: usize,
	/// Total size of the checkpoint in bytes
	pub total_size: u64,
}

impl CheckpointMetadata {
	/// Creates new checkpoint metadata with current version
	pub fn new(
		timestamp: u64,
		sequence_number: u64,
		sstable_count: usize,
		total_size: u64,
	) -> Self {
		Self {
			version: CHECKPOINT_VERSION,
			timestamp,
			sequence_number,
			sstable_count,
			total_size,
		}
	}

	/// Checks if this metadata version is compatible with current
	/// implementation
	pub fn is_compatible(&self) -> bool {
		// For now, we only support version 1
		self.version == CHECKPOINT_VERSION
	}

	/// Serializes the metadata to binary format
	pub fn to_bytes(&self) -> Result<Vec<u8>> {
		let mut buf = Vec::new();

		// Write version first for compatibility checking
		buf.extend_from_slice(&self.version.to_be_bytes());

		// Write timestamp
		buf.extend_from_slice(&self.timestamp.to_be_bytes());

		// Write sequence number
		buf.extend_from_slice(&self.sequence_number.to_be_bytes());

		// Write sstable count (convert usize to u64 for portability)
		buf.extend_from_slice(&(self.sstable_count as u64).to_be_bytes());

		// Write total size
		buf.extend_from_slice(&self.total_size.to_be_bytes());

		Ok(buf)
	}

	/// Deserializes metadata from binary format
	pub fn from_bytes(data: &[u8]) -> Result<Self> {
		let mut reader = std::io::Cursor::new(data);

		let mut u32_buf = [0u8; 4];
		reader
			.read_exact(&mut u32_buf)
			.map_err(|e| Error::Other(format!("Failed to read version: {e}")))?;
		let version = u32::from_be_bytes(u32_buf);

		// Check if we can handle this version
		if version > CHECKPOINT_VERSION {
			return Err(Error::Other(format!(
				"Unsupported checkpoint version: {version}. Current version: {CHECKPOINT_VERSION}"
			)));
		}

		let mut u64_buf = [0u8; 8];

		reader
			.read_exact(&mut u64_buf)
			.map_err(|e| Error::Other(format!("Failed to read timestamp: {e}")))?;
		let timestamp = u64::from_be_bytes(u64_buf);

		reader
			.read_exact(&mut u64_buf)
			.map_err(|e| Error::Other(format!("Failed to read sequence_number: {e}")))?;
		let sequence_number = u64::from_be_bytes(u64_buf);

		reader
			.read_exact(&mut u64_buf)
			.map_err(|e| Error::Other(format!("Failed to read sstable_count: {e}")))?;
		let sstable_count = u64::from_be_bytes(u64_buf) as usize;

		reader
			.read_exact(&mut u64_buf)
			.map_err(|e| Error::Other(format!("Failed to read total_size: {e}")))?;
		let total_size = u64::from_be_bytes(u64_buf);

		Ok(Self {
			version,
			timestamp,
			sequence_number,
			sstable_count,
			total_size,
		})
	}
}

/// Where `create_checkpoint` calls `CoreInner::checkpoint_hook`.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CheckpointStage {
	/// The SSTables are copied, and the manifest, which is still held, is not.
	SstablesCopied,
	/// The manifest is released and the value log is not copied.
	ManifestReleased,
	/// `restore_from_checkpoint` removed the SSTables, the WAL, the manifest and the value log of
	/// the database, and copied nothing yet.
	RestoreCleared,
	/// The SSTables and the WAL are copied, and the manifest and the value log are not.
	RestoreSstablesCopied,
	/// Every file of the checkpoint is back in place. The in-memory state, which is still the
	/// state from before the restore, is not reloaded.
	RestoreFilesCopied,
	/// The in-memory manifest is the restored one, and the memtables and the WAL are still the
	/// ones from before the restore.
	RestoreManifestSwapped,
}

/// See `CoreInner::checkpoint_hook`.
#[cfg(test)]
pub(crate) type CheckpointHook = Arc<dyn Fn(CheckpointStage) + Send + Sync>;

/// Database checkpoint manager for creating consistent point-in-time snapshots
pub(crate) struct DatabaseCheckpoint {
	/// Reference to the LSM core
	core: Arc<CoreInner>,
}

impl DatabaseCheckpoint {
	/// Creates a new database checkpoint manager
	pub fn new(core: Arc<CoreInner>) -> Self {
		Self {
			core,
		}
	}

	/// Creates a new database checkpoint at the specified directory.
	///
	/// This creates a consistent point-in-time snapshot that includes:
	/// - All SSTables from all levels
	/// - Current WAL segments
	/// - Level manifest
	/// - VLog directories (if enabled)
	/// - Checkpoint metadata
	///
	/// # Arguments
	/// * `checkpoint_dir` - Directory where the checkpoint will be created
	///
	/// # Returns
	/// Metadata about the created checkpoint
	pub fn create_checkpoint<P: AsRef<Path>>(
		&self,
		checkpoint_dir: P,
	) -> Result<CheckpointMetadata> {
		// A database a commit group stopped holds batches that were never published, which a
		// checkpoint would carry into the state it restores.
		if let Some(error) = self.core.error_handler.commit_group_error() {
			return Err(error);
		}

		let checkpoint_path = checkpoint_dir.as_ref();

		// Before anything is created, flushed or replaced
		self.check_destination(checkpoint_path)?;

		// Create checkpoint directory
		fs::create_dir_all(checkpoint_path).map_err(|e| Error::Io(Arc::new(e)))?;

		// Step 1: Flush all memtables to ensure consistency
		self.flush_all_memtables()?;

		// The SSTables copied below are the ones of the manifest in memory and the manifest file
		// is copied from disk. After a write of the manifest by a flush or a direct table that
		// failed from the rename on, the file may list a table the one in memory does not, so
		// the one in memory is written again first. A compaction's manifest write is not
		// covered.
		self.core.settle_manifest()?;

		// Steps 2-6 read one version of the manifest: holding its lock keeps a flush or a
		// compaction from adding or removing tables, or rewriting the manifest file, while the
		// SSTables and the manifest are copied.
		let levels_guard = self.core.level_manifest.read()?;

		// A write that failed from the rename on since the settle sets the flag while it holds the
		// manifest lock, so with the lock held and the flag clear the file and the tables agree.
		// The settle is not repeated: a checkpoint does not wait for writers that keep failing.
		if self.core.manifest_uncertain() {
			return Err(Error::ManifestWriteUncertain(
				"the manifest was not written again before it was copied".into(),
			));
		}

		// A flush or a compaction deletes the value-log files it made obsolete as soon as it can
		// take the manifest, which is before the value log is copied below. The pin, taken while
		// the manifest is held, defers that until the copy is done.
		let _vlog_pin = self.core.vlog.as_ref().map(|vlog| vlog.pin_files());

		// Step 2: Get current sequence number from the manifest
		let sequence_number = levels_guard.get_last_sequence();

		// Step 3: Create checkpoint subdirectories
		let sstables_dir = checkpoint_path.join("sstables");
		let wal_dir = checkpoint_path.join("wal");
		fs::create_dir_all(&sstables_dir).map_err(|e| Error::Io(Arc::new(e)))?;
		fs::create_dir_all(&wal_dir).map_err(|e| Error::Io(Arc::new(e)))?;

		// Step 4: Copy all SSTables
		let (sstable_count, sstables_size) = self.copy_sstables(&levels_guard, &sstables_dir)?;

		// Step 5: Copy WAL segments
		self.create_new_wal(&wal_dir)?;

		#[cfg(test)]
		self.reach(CheckpointStage::SstablesCopied);

		// Step 6: Copy level manifest
		let manifest_size = self.copy_level_manifest(checkpoint_path)?;
		drop(levels_guard);

		#[cfg(test)]
		self.reach(CheckpointStage::ManifestReleased);

		// Step 7: Copy VLog directories if enabled
		let vlog_size = self.copy_vlog_directories(checkpoint_path)?;

		// Step 8: Create checkpoint metadata
		let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

		let metadata = CheckpointMetadata::new(
			timestamp,
			sequence_number,
			sstable_count,
			sstables_size + manifest_size + vlog_size,
		);

		// Step 9: Write metadata file
		self.write_checkpoint_metadata(checkpoint_path, &metadata)?;

		Ok(metadata)
	}

	/// Refuses a destination that is the database directory or one of its subdirectories, lies
	/// inside one, or contains one, which a checkpoint would write over. So are the entries of an
	/// existing destination that a checkpoint writes (`sstables`, `wal`, `manifest`, `vlog` and
	/// the metadata file) when they are links into the database. Links and relative paths are
	/// resolved first. An existing directory, an earlier checkpoint's included, is accepted.
	fn check_destination(&self, destination: &Path) -> Result<()> {
		let io = |e| Error::Io(Arc::new(e));
		let opts = &self.core.opts;
		let mut live = Vec::new();
		for dir in [
			opts.path.clone(),
			opts.sstable_dir(),
			opts.wal_dir(),
			opts.manifest_dir(),
			opts.vlog_dir(),
		] {
			live.push(resolve(&dir).map_err(io)?);
		}
		let entries = ["sstables", "wal", "manifest", "vlog", CHECKPOINT_METADATA_FILE];
		let written = std::iter::once(destination.to_path_buf())
			.chain(entries.iter().map(|entry| destination.join(entry)));
		for path in written {
			let resolved = resolve(&path).map_err(io)?;
			if live.iter().any(|dir| resolved.starts_with(dir) || dir.starts_with(&resolved)) {
				return Err(Error::InvalidArgument(format!(
					"Checkpoint destination {} overlaps the database at {}: it must be a directory \
					 that neither lies inside the database nor contains it",
					destination.display(),
					opts.path.display()
				)));
			}
		}
		Ok(())
	}

	/// Calls the test hook, if one is set, for `stage`.
	#[cfg(test)]
	fn reach(&self, stage: CheckpointStage) {
		let hook = self.core.checkpoint_hook.lock().clone();
		if let Some(hook) = hook {
			hook(stage);
		}
	}

	/// Restores the database from a checkpoint directory.
	/// This will overwrite the current database state
	pub fn restore_from_checkpoint<P: AsRef<Path>>(
		&self,
		checkpoint_dir: P,
	) -> Result<CheckpointMetadata> {
		let checkpoint_path = checkpoint_dir.as_ref();

		// Verify checkpoint exists and is valid
		let metadata = self.read_checkpoint_metadata(checkpoint_path)?;

		// Clear current database state
		self.clear_current_state()?;

		#[cfg(test)]
		self.reach(CheckpointStage::RestoreCleared);

		// Restore SSTables
		let sstables_source = checkpoint_path.join("sstables");
		let sstables_dest = self.core.opts.sstable_dir();
		if sstables_source.exists() {
			Self::copy_directory_sync(&sstables_source, &sstables_dest)?;
		}

		// Restore WAL segments
		let wal_source = checkpoint_path.join("wal");
		let wal_dest = self.core.opts.wal_dir();
		if wal_source.exists() {
			Self::copy_directory_sync(&wal_source, &wal_dest)?;
		}

		#[cfg(test)]
		self.reach(CheckpointStage::RestoreSstablesCopied);

		// Restore level manifest directory
		let manifest_source = checkpoint_path.join("manifest");
		let manifest_dest = self.core.opts.manifest_dir();
		if manifest_source.exists() {
			if manifest_dest.exists() {
				fs::remove_dir_all(&manifest_dest).map_err(|e| Error::Io(Arc::new(e)))?;
			}
			copy_dir_all(&manifest_source, &manifest_dest).map_err(|e| Error::Io(Arc::new(e)))?;
		}

		// Restore VLog directories if they exist in the checkpoint
		self.restore_vlog_directories(checkpoint_path)?;

		#[cfg(test)]
		self.reach(CheckpointStage::RestoreFilesCopied);

		Ok(metadata)
	}

	/// Flushes all memtables to ensure checkpoint consistency
	fn flush_all_memtables(&self) -> Result<()> {
		// Step 1: Rotate active memtable if it has data
		{
			let active = self.core.active_memtable.read()?;
			if !active.is_empty() {
				drop(active); // Release read lock before acquiring write lock
				self.core.rotate_memtable()?;
			}
		}

		// Step 2: Flush all immutable memtables synchronously
		self.core.flush_all_immutables_sync()
	}

	/// Copies all SSTables of `levels` to the checkpoint directory
	fn copy_sstables(&self, levels: &LevelManifest, dest_dir: &Path) -> Result<(usize, u64)> {
		let mut total_size = 0u64;
		let mut count = 0usize;

		// Use the iterator method to iterate over all tables
		for table in levels.iter() {
			// Construct the source path using the table ID, similar to load_table
			let source_path = self.core.opts.sstable_file_path(table.id);

			let filename = source_path
				.file_name()
				.ok_or_else(|| Error::Other("Invalid SSTable path".to_string()))?;
			let dest_path = dest_dir.join(filename);

			// A file left by an earlier checkpoint into this directory may be a link to the live
			// SSTable, which copying over it would truncate.
			match fs::remove_file(&dest_path) {
				Ok(()) => {}
				Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
				Err(e) => return Err(Error::Io(Arc::new(e))),
			}

			// Create hard link if possible (faster), otherwise copy. A name that exists now was
			// linked by a checkpoint running at the same time, and copying over it would truncate
			// the live SSTable.
			match fs::hard_link(&source_path, &dest_path) {
				Ok(()) => {}
				Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
				Err(_) => {
					fs::copy(&source_path, &dest_path).map_err(|e| Error::Io(Arc::new(e)))?;
				}
			}

			// Add to size count
			if let Ok(metadata) = fs::metadata(&dest_path) {
				total_size += metadata.len();
			}
			count += 1;
		}

		Ok((count, total_size))
	}

	/// Creates a new empty WAL directory structure for the checkpoint
	fn create_new_wal(&self, dest_dir: &Path) -> Result<()> {
		// Since we flush all memtables before creating a checkpoint,
		// all data is already persisted in SSTables. We don't need to copy
		// any WAL segments as they would only contain data that's already
		// in the SSTables.
		//
		// We create an empty WAL directory structure for the restored database.
		fs::create_dir_all(dest_dir).map_err(|e| Error::Io(Arc::new(e)))?;

		// Create an empty checkpoint subdirectory for WAL checkpoint tracking
		let checkpoint_subdir = dest_dir.join("checkpoint");
		fs::create_dir_all(&checkpoint_subdir).map_err(|e| Error::Io(Arc::new(e)))?;

		Ok(())
	}

	/// Copies the level manifest directory to the checkpoint directory
	fn copy_level_manifest(&self, dest_dir: &Path) -> Result<u64> {
		let source_path = self.core.opts.manifest_dir();
		let dest_path = dest_dir.join("manifest");

		if source_path.exists() {
			copy_dir_all(&source_path, &dest_path).map_err(|e| Error::Io(Arc::new(e)))?;

			// Calculate total size of copied directory
			let mut total_size = 0u64;
			if let Ok(entries) = fs::read_dir(&dest_path) {
				for entry in entries.flatten() {
					if let Ok(metadata) = entry.metadata() {
						if metadata.is_file() {
							total_size += metadata.len();
						}
					}
				}
			}
			return Ok(total_size);
		}

		Ok(0)
	}

	/// Copies VLog-related directories to the checkpoint directory if VLog is
	/// enabled
	fn copy_vlog_directories(&self, dest_dir: &Path) -> Result<u64> {
		if !self.core.opts.enable_vlog {
			return Ok(0);
		}

		let mut total_size = 0u64;

		// Copy VLog directory
		let vlog_source = self.core.opts.vlog_dir();
		let vlog_dest = dest_dir.join("vlog");
		if vlog_source.exists() {
			copy_dir_all(&vlog_source, &vlog_dest).map_err(|e| Error::Io(Arc::new(e)))?;
			total_size += Self::calculate_directory_size(&vlog_dest)?;
		}

		Ok(total_size)
	}

	/// Calculates the total size of a directory recursively
	fn calculate_directory_size(dir_path: &Path) -> Result<u64> {
		let mut total_size = 0u64;

		if let Ok(entries) = fs::read_dir(dir_path) {
			for entry in entries.flatten() {
				let entry_path = entry.path();
				if entry_path.is_file() {
					if let Ok(metadata) = entry_path.metadata() {
						total_size += metadata.len();
					}
				} else if entry_path.is_dir() {
					total_size += Self::calculate_directory_size(&entry_path)?;
				}
			}
		}

		Ok(total_size)
	}

	/// Restores VLog-related directories from the checkpoint
	fn restore_vlog_directories(&self, checkpoint_path: &Path) -> Result<()> {
		// Restore VLog directory
		let vlog_source = checkpoint_path.join("vlog");
		let vlog_dest = self.core.opts.vlog_dir();
		if vlog_source.exists() {
			if vlog_dest.exists() {
				fs::remove_dir_all(&vlog_dest).map_err(|e| Error::Io(Arc::new(e)))?;
			}
			copy_dir_all(&vlog_source, &vlog_dest).map_err(|e| Error::Io(Arc::new(e)))?;
		}

		Ok(())
	}

	/// Writes checkpoint metadata to a file
	fn write_checkpoint_metadata(
		&self,
		checkpoint_dir: &Path,
		metadata: &CheckpointMetadata,
	) -> Result<()> {
		let metadata_path = checkpoint_dir.join(CHECKPOINT_METADATA_FILE);
		let mut file = File::create(&metadata_path).map_err(|e| Error::Io(Arc::new(e)))?;

		let data = metadata.to_bytes()?;
		file.write_all(&data).map_err(|e| Error::Io(Arc::new(e)))?;
		file.flush().map_err(|e| Error::Io(Arc::new(e)))?;

		Ok(())
	}

	/// Reads checkpoint metadata from a file
	fn read_checkpoint_metadata(&self, checkpoint_dir: &Path) -> Result<CheckpointMetadata> {
		let metadata_path = checkpoint_dir.join(CHECKPOINT_METADATA_FILE);
		let data = fs::read(&metadata_path).map_err(|e| Error::Io(Arc::new(e)))?;

		CheckpointMetadata::from_bytes(&data)
	}

	/// Helper function to copy a directory recursively (synchronous version to
	/// avoid recursion issues)
	fn copy_directory_sync(source: &Path, dest: &Path) -> Result<u64> {
		if !source.exists() {
			return Ok(0);
		}

		fs::create_dir_all(dest).map_err(|e| Error::Io(Arc::new(e)))?;

		let mut total_size = 0u64;

		for entry in fs::read_dir(source).map_err(|e| Error::Io(Arc::new(e)))? {
			let entry = entry.map_err(|e| Error::Io(Arc::new(e)))?;

			let source_path = entry.path();
			let dest_path = dest.join(entry.file_name());

			if source_path.is_file() {
				// Create hard link if possible, otherwise copy
				if fs::hard_link(&source_path, &dest_path).is_err() {
					fs::copy(&source_path, &dest_path).map_err(|e| Error::Io(Arc::new(e)))?;
				}

				if let Ok(metadata) = fs::metadata(&dest_path) {
					total_size += metadata.len();
				}
			} else if source_path.is_dir() {
				total_size += Self::copy_directory_sync(&source_path, &dest_path)?;
			}
		}

		Ok(total_size)
	}

	/// Clears the current database state (for restoration)
	fn clear_current_state(&self) -> Result<()> {
		// Clear SSTables directory
		let sstables_dir = self.core.opts.sstable_dir();
		if sstables_dir.exists() {
			fs::remove_dir_all(&sstables_dir).map_err(|e| Error::Io(Arc::new(e)))?;
		}

		// Clear WAL directory
		let wal_dir = self.core.opts.wal_dir();
		if wal_dir.exists() {
			fs::remove_dir_all(&wal_dir).map_err(|e| Error::Io(Arc::new(e)))?;
		}

		// Remove level manifest directory
		let manifest_path = self.core.opts.manifest_dir();
		if manifest_path.exists() {
			fs::remove_dir_all(&manifest_path).map_err(|e| Error::Io(Arc::new(e)))?;
		}

		// Clear VLog directory if VLog is enabled
		if self.core.opts.enable_vlog {
			let vlog_dir = self.core.opts.vlog_dir();
			if vlog_dir.exists() {
				fs::remove_dir_all(&vlog_dir).map_err(|e| Error::Io(Arc::new(e)))?;
			}
		}

		Ok(())
	}
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
	use test_log::test;

	use super::*;

	#[test]
	fn test_checkpoint_metadata_serialization() {
		let original = CheckpointMetadata::new(
			1234567890,  // timestamp
			100,         // sequence_number
			100,         // sstable_count
			1024 * 1024, // total_size (1MB)
		);

		// Test round-trip serialization
		let bytes = original.to_bytes().expect("Serialization should succeed");
		let deserialized =
			CheckpointMetadata::from_bytes(&bytes).expect("Deserialization should succeed");

		assert_eq!(original.version, deserialized.version);
		assert_eq!(original.timestamp, deserialized.timestamp);
		assert_eq!(original.sequence_number, deserialized.sequence_number);
		assert_eq!(original.sstable_count, deserialized.sstable_count);
		assert_eq!(original.total_size, deserialized.total_size);
	}

	#[test]
	fn test_checkpoint_version_compatibility() {
		let metadata = CheckpointMetadata::new(0, 0, 0, 0);
		assert!(metadata.is_compatible());

		// Test version rejection
		let mut future_data = Vec::new();
		future_data.extend_from_slice(&999u32.to_be_bytes()); // version 999
		future_data.extend_from_slice(&[0u8; 32]); // dummy data

		let result = CheckpointMetadata::from_bytes(&future_data);
		assert!(result.is_err());
		assert!(result.unwrap_err().to_string().contains("Unsupported checkpoint version"));
	}

	#[test]
	fn test_checkpoint_metadata_binary_format() {
		let metadata = CheckpointMetadata::new(100, 200, 300, 400);
		let bytes = metadata.to_bytes().unwrap();

		// Verify the binary format structure
		// 4 bytes version + 8 bytes timestamp + 8 bytes seq + 8 bytes count + 8 bytes
		// size = 36 bytes
		assert_eq!(bytes.len(), 36);

		// Check that version is at the beginning (big endian)
		assert_eq!(&bytes[0..4], &1u32.to_be_bytes());
	}
}
