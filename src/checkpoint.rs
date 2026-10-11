use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{EncryptionError, Error, Result};
use crate::levels::{check_manifest_mode, LevelManifest};
use crate::lsm::{fsync_directory, CoreInner};
use crate::tde::EncryptionContext;
use crate::vlog::sealed;
use crate::Options;

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

/// What the checkpoints and the restores of this process are doing to a checkpoint directory, by
/// resolved path.
#[derive(Clone, Copy)]
enum Use {
	/// A checkpoint is writing it.
	Writing,
	/// This many restores are reading it.
	Reading(usize),
}

static DESTINATIONS: parking_lot::Mutex<BTreeMap<PathBuf, Use>> =
	parking_lot::Mutex::new(BTreeMap::new());

/// Wakes the checkpoints that wait for a destination when one is released.
static DESTINATION_RELEASED: parking_lot::Condvar = parking_lot::Condvar::new();

/// The right to write one checkpoint directory, or to read it for a restore. A checkpoint removes
/// and writes the files of the directory and ends with its metadata file, so two that wrote the
/// same directory at once would leave the files of one beside the metadata of the other, and a
/// restore that read it meanwhile would copy a mixture. A checkpoint waits here for the one that
/// is writing the destination and for the restores that read it; a restore does not wait, for it
/// holds the flush lock, which a checkpoint takes after it holds its destination: it is refused
/// instead. The guard is released on every path, errors and panics included. A checkpoint takes it
/// before any other lock, so that waiting for a destination never holds one.
struct DestinationGuard {
	path: PathBuf,
	writing: bool,
}

impl DestinationGuard {
	fn acquire(path: PathBuf) -> Self {
		let mut busy = DESTINATIONS.lock();
		while busy.contains_key(&path) {
			DESTINATION_RELEASED.wait(&mut busy);
		}
		busy.insert(path.clone(), Use::Writing);
		Self {
			path,
			writing: true,
		}
	}

	/// Takes the directory for reading, or returns `None` if a checkpoint is writing it. Never
	/// waits.
	fn try_read(path: PathBuf) -> Option<Self> {
		let mut busy = DESTINATIONS.lock();
		match busy.get(&path).copied() {
			Some(Use::Writing) => return None,
			Some(Use::Reading(n)) => busy.insert(path.clone(), Use::Reading(n + 1)),
			None => busy.insert(path.clone(), Use::Reading(1)),
		};
		Some(Self {
			path,
			writing: false,
		})
	}
}

impl Drop for DestinationGuard {
	fn drop(&mut self) {
		let mut busy = DESTINATIONS.lock();
		match busy.get(&self.path).copied() {
			Some(Use::Reading(n)) if !self.writing && n > 1 => {
				busy.insert(self.path.clone(), Use::Reading(n - 1));
				return;
			}
			_ => {
				busy.remove(&self.path);
			}
		}
		drop(busy);
		DESTINATION_RELEASED.notify_all();
	}
}

/// Keeps `Core::close` and the checkpoints from running at the same time.
///
/// A checkpoint holds a [`CheckpointGuard`] for as long as it runs. `close` refuses the
/// checkpoints that would begin after it and waits for the ones running, without holding a
/// thread: a checkpoint is synchronous and needs nothing from a runtime to finish.
///
/// A restore of the same tree excludes the checkpoints too, see [`exclude`](Self::exclude), and
/// `close` waits for the restore that runs and refuses the ones that would begin after it: a
/// restore that went on under a close would replace the files of a database that the close
/// releases to other processes.
#[derive(Default)]
pub(crate) struct CheckpointGate {
	state: parking_lot::Mutex<GateState>,
	/// Wakes `close` when the last running checkpoint, or the restore that runs, ends.
	idle: tokio::sync::Notify,
	/// Wakes the restores that wait for the last running checkpoint to end, and the checkpoints
	/// that wait for the last restore to be over.
	turns: parking_lot::Condvar,
}

#[derive(Default)]
struct GateState {
	running: usize,
	closed: bool,
	/// The restores that wait for the running checkpoints or for the restore that runs, or run. No
	/// checkpoint begins while there is one, so that a restore is not kept waiting by checkpoints
	/// that follow one another.
	restores: usize,
	/// A restore holds the exclusion. Restores take turns: they refuse the commits with one flag
	/// that the end of a restore clears, so a second restore that began its work before the first
	/// one's end would replace the files with the commits admitted.
	excluding: bool,
}

/// A checkpoint that is running; see [`CheckpointGate`].
pub(crate) struct CheckpointGuard<'a> {
	gate: &'a CheckpointGate,
}

impl CheckpointGate {
	/// Lets a checkpoint begin, unless `close` has, once the restores are over. A checkpoint that
	/// waits for a restore is not running, so `close` does not wait for it, and the checkpoint
	/// finds the gate closed afterwards if `close` has come meanwhile.
	pub(crate) fn enter(&self) -> Result<CheckpointGuard<'_>> {
		let mut state = self.state.lock();
		while state.restores > 0 {
			self.turns.wait(&mut state);
		}
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

	/// Refuses the checkpoints and the restores that begin from now on and waits for the
	/// checkpoints running and the restore that runs. Only `Core::close` calls it, one caller at
	/// a time, so a single wake-up is enough. If the future is dropped before the wait is over,
	/// checkpoints and restores are allowed again.
	pub(crate) async fn close(&self) {
		let reopen = Reopen(self);
		self.state.lock().closed = true;
		while self.busy() {
			self.idle.notified().await;
		}
		std::mem::forget(reopen);
	}

	/// Whether a checkpoint runs, or a restore holds the exclusion.
	fn busy(&self) -> bool {
		let state = self.state.lock();
		state.running > 0 || state.excluding
	}

	/// Waits for the checkpoints that are running and for the restore that holds the exclusion,
	/// and keeps the checkpoints that begin from now on waiting, until the guard is dropped. A
	/// restore replaces the files that a checkpoint copies, and one that went on under it would
	/// copy the files of two databases into a checkpoint that opens, with no error. A restore
	/// takes it before any other lock: a checkpoint holds its guard from its first step, and
	/// flushes under the locks that the restore takes next, so waiting for it after them would
	/// deadlock.
	///
	/// Fails, changing nothing, if `close` has begun: `close` releases the database to other
	/// processes, and a restore that went on after it would replace their files.
	pub(crate) fn exclude(&self) -> Result<CheckpointExclusion<'_>> {
		let mut state = self.state.lock();
		state.restores += 1;
		while state.running > 0 || state.excluding {
			self.turns.wait(&mut state);
		}
		if state.closed {
			state.restores -= 1;
			// The checkpoints that waited for it find the gate closed.
			self.turns.notify_all();
			return Err(Error::Other(
				"Cannot restore from a checkpoint: the tree is closing or closed".to_string(),
			));
		}
		state.excluding = true;
		Ok(CheckpointExclusion {
			gate: self,
		})
	}
}

/// A restore that excludes the checkpoints; see [`CheckpointGate::exclude`].
pub(crate) struct CheckpointExclusion<'a> {
	gate: &'a CheckpointGate,
}

impl Drop for CheckpointExclusion<'_> {
	fn drop(&mut self) {
		let mut state = self.gate.state.lock();
		state.restores -= 1;
		state.excluding = false;
		if state.closed {
			self.gate.idle.notify_one();
		}
		// The next restore, or the checkpoints that waited for the last one.
		self.gate.turns.notify_all();
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
		if state.restores > 0 && state.running == 0 {
			self.gate.turns.notify_all();
		}
	}
}

/// The file a restore keeps in the database directory from before it removes the files of the
/// database until every file of the checkpoint is back on disk. It holds the path of the
/// checkpoint.
const RESTORE_MARKER_FILE: &str = "RESTORE_IN_PROGRESS";

/// Refuses to open a directory in which a restore did not finish. A restore removes the tables,
/// the WAL, the manifest and the value log before it copies the checkpoint's files back, so what
/// an interrupted one leaves is an empty directory, or a part of the checkpoint, which would open
/// as an empty database or as a wrong one. Reads only, like `check_manifest_before_open`.
pub(crate) fn check_restore_finished(opts: &Options) -> Result<()> {
	let marker = opts.path.join(RESTORE_MARKER_FILE);
	if !marker.try_exists()? {
		return Ok(());
	}
	let checkpoint = fs::read_to_string(&marker).unwrap_or_default();
	// An empty directory is a database of its own when it is opened with a key manager, with an
	// identity that no checkpoint of another database has, and a restore refuses that.
	let advice = if opts.key_manager.is_some() || opts.encryption_context().is_some() {
		"The database is encrypted, and a checkpoint of it is restored only into the database it \
		 was taken from: copy the directory of the checkpoint to an empty place and open it there \
		 instead."
	} else {
		"Restore the checkpoint into an empty directory instead."
	};
	Err(Error::RestoreInterrupted(format!(
		"a restore of the checkpoint '{}' into {} did not finish, or is still running in another \
		 process: the files of the database are neither the database as it was nor the \
		 checkpoint, so it is not opened and nothing was changed. {advice} Only if no restore is \
		 running and the files are known to be the restored database, remove {} to open it.",
		checkpoint.trim(),
		opts.path.display(),
		marker.display()
	)))
}

/// The directory that holds `dir`: the current directory for a name that has no parent part,
/// whose `parent` is the empty path, which names nothing that can be synced.
fn parent_directory(dir: &Path) -> Option<&Path> {
	match dir.parent() {
		Some(parent) if parent.as_os_str().is_empty() => Some(Path::new(".")),
		parent => parent,
	}
}

/// Syncs the files below `dir`, and then the directories, so that what was copied or linked there
/// is on disk. A directory that does not exist has nothing to sync. What is neither a file nor a
/// directory, a named pipe for instance, is left alone: opening it waits for the other end.
fn sync_below(dir: &Path) -> std::io::Result<()> {
	let entries = match fs::read_dir(dir) {
		Ok(entries) => entries,
		Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
		Err(e) => return Err(e),
	};
	for entry in entries {
		let entry = entry?;
		let kind = entry.file_type()?;
		if kind.is_dir() {
			sync_below(&entry.path())?;
		} else if kind.is_file() {
			// A read-only handle syncs everywhere but on Windows, which wants write access.
			fs::OpenOptions::new()
				.read(true)
				.write(cfg!(windows))
				.open(entry.path())?
				.sync_all()?;
		}
	}
	fsync_directory(dir)
}

/// Whether a value-log file of a checkpoint, whose first bytes are `head` and whose length is
/// `len`, is of the kind that a database with the encryption `live` reads, see
/// `DatabaseCheckpoint::check_vlog_mode`.
///
/// In a database that is encrypted a sealed file must be of this database and open with its key,
/// and a plaintext file is `PlaintextInEncryptedDb`. In one that is not, a sealed file is
/// `NotConfigured`.
fn check_vlog_head(
	live: Option<&EncryptionContext>,
	id: u32,
	is_newest: bool,
	len: u64,
	head: &[u8],
) -> Result<()> {
	let Some(context) = live else {
		return if sealed::is_sealed_start(head) {
			Err(EncryptionError::NotConfigured.into())
		} else {
			Ok(())
		};
	};
	// A file that was cut off before its header was whole holds no entry. The whole of such a file
	// is in `head`, which is at most a header long.
	if is_newest && len <= sealed::SEALED_HEADER_LEN as u64 && sealed::is_torn(head) {
		return Ok(());
	}
	if sealed::is_sealed_start(head) {
		// The database, the file, the key and the tag, the order in which the log checks them.
		return sealed::open_header(context, id, head, true).map(drop);
	}
	if sealed::is_plain_start(head) {
		return Err(EncryptionError::PlaintextInEncryptedDb.into());
	}
	Err(Error::Corruption(format!("VLog file {id} of the checkpoint is not a value-log file")))
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

		// Only the version this build writes is known: a file of version 0 is what a metadata
		// file that a power cut left zero-filled reads as.
		if version != CHECKPOINT_VERSION {
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

		// The format has a fixed size: more bytes than that are not a metadata file of this version
		let read = reader.position() as usize;
		if read != data.len() {
			return Err(Error::Other(format!(
				"Invalid checkpoint metadata: {} bytes, expected {read}",
				data.len()
			)));
		}

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
	/// The in-memory manifest, the active memtable and the immutable queue were replaced together:
	/// the manifest is the restored one and the memtables are empty. The WAL and the sequence
	/// numbers are still the ones from before the restore.
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

	/// Creates a new database checkpoint at the specified directory. A checkpoint into a
	/// directory that another checkpoint of this process is writing, or that a restore of this
	/// process is reading, waits for it.
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

		// One checkpoint at a time writes a directory: the others wait here, holding nothing
		let _destination = DestinationGuard::acquire(
			resolve(checkpoint_path).map_err(|e| Error::Io(Arc::new(e)))?,
		);

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

		// An earlier checkpoint's metadata file would make this directory look finished while
		// its files are replaced one by one, and an interrupted checkpoint would restore with
		// the values of the old one missing. It goes before anything is copied, and the new one
		// is written after everything else.
		self.remove_stale_metadata(checkpoint_path)?;

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

		// Step 8: Make the files of the checkpoint durable before the metadata that says they are
		// all there
		for dir in ["sstables", "wal", "manifest", "vlog"] {
			sync_below(&checkpoint_path.join(dir)).map_err(|e| Error::Io(Arc::new(e)))?;
		}
		// The names of those directories are entries of this one: synced here, so that the
		// metadata file cannot be on disk without them
		fsync_directory(checkpoint_path).map_err(|e| Error::Io(Arc::new(e)))?;

		// Step 9: Create checkpoint metadata
		let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

		let metadata = CheckpointMetadata::new(
			timestamp,
			sequence_number,
			sstable_count,
			sstables_size + manifest_size + vlog_size,
		);

		// Step 10: Write metadata file
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
	///
	/// The checkpoint is validated first, and one that cannot be restored is refused before
	/// anything of the database is touched. From the first removal until its files are back and
	/// synced, a marker file in the database directory makes opening the directory fail with
	/// [`Error::RestoreInterrupted`]. A restore that failed in between is repeated on the open
	/// database; one that crashed is repeated into an empty directory. A directory that a
	/// checkpoint of this process is writing is refused with [`Error::InvalidArgument`].
	///
	/// A checkpoint of an encrypted database is restored only into the database it was taken
	/// from: another database has an identity of its own, and a checkpoint of another one is
	/// refused with [`EncryptionError::DbIdMismatch`](crate::EncryptionError::DbIdMismatch). To
	/// recover it elsewhere, copy the directory of the checkpoint and open it.
	pub fn restore_from_checkpoint<P: AsRef<Path>>(
		&self,
		checkpoint_dir: P,
	) -> Result<CheckpointMetadata> {
		let checkpoint_path = checkpoint_dir.as_ref();

		// A checkpoint of this process that is writing the directory replaces its files one by one,
		// and a restore that read them meanwhile would copy a mixture. It is refused rather than
		// waited for: see `DestinationGuard`.
		// A path that cannot be resolved is used as it is: restoring never needed it resolved, and
		// a checkpoint that writes the directory has resolved it, or has failed.
		let _source = DestinationGuard::try_read(
			resolve(checkpoint_path).unwrap_or_else(|_| checkpoint_path.to_path_buf()),
		)
		.ok_or_else(|| {
			Error::InvalidArgument(format!(
				"{} is being written by a checkpoint of this process: it cannot be restored until \
				 that checkpoint is done",
				checkpoint_path.display()
			))
		})?;

		// Verify checkpoint exists and is valid
		let metadata = self.read_checkpoint_metadata(checkpoint_path)?;

		// Everything the restore needs is read before anything of the database is removed
		self.validate_checkpoint(checkpoint_path)?;

		// From here the files of the database are not the database: if the restore stops before
		// `finish_restore`, the next open is refused instead of reading what is left
		self.begin_restore(checkpoint_path)?;

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

		self.finish_restore()?;

		#[cfg(test)]
		self.reach(CheckpointStage::RestoreFilesCopied);

		Ok(metadata)
	}

	/// Refuses a checkpoint that cannot be restored: one without its tables or manifest
	/// directory, whose manifest does not load, or one of whose listed tables is missing or
	/// damaged. Reads only, and runs before a restore removes anything, so a refused checkpoint
	/// leaves the database as it was.
	///
	/// The manifest is loaded with the loader a restore uses afterwards, from the checkpoint's
	/// own directory and with a block cache of its own: the live cache is keyed by table id, and
	/// the checkpoint's tables must not leave blocks in it under the ids of the live ones.
	fn validate_checkpoint(&self, checkpoint_path: &Path) -> Result<()> {
		let unusable = |what: String| {
			Error::InvalidArgument(format!(
				"{} is not a usable checkpoint: {what}",
				checkpoint_path.display()
			))
		};

		for dir in ["sstables", "manifest"] {
			if !checkpoint_path.join(dir).is_dir() {
				return Err(unusable(format!("it has no {dir} directory")));
			}
		}

		let opts = Arc::new(
			Options {
				path: checkpoint_path.to_path_buf(),
				..(*self.core.opts).clone()
			}
			.with_block_cache_capacity(1 << 20),
		);
		let manifest_file = opts.manifest_file_path(0);
		if !manifest_file.is_file() {
			return Err(unusable("it has no manifest file".to_string()));
		}

		// Whether the checkpoint is encrypted, and is of this database and its suite, is read
		// from the start of its manifest and of its value-log files, before anything of it is
		// opened and without a key. An encryption error is the answer as it is: a checkpoint of
		// another kind is not a damaged one, and none of them is a reason to remove anything.
		let live = self.core.opts.encryption_context();
		check_manifest_mode(live.map(Arc::as_ref), &manifest_file).map_err(|e| match e {
			Error::Encryption(_) => e,
			e => unusable(format!("its manifest is not usable: {e}")),
		})?;
		self.check_vlog_mode(checkpoint_path, live.map(Arc::as_ref)).map_err(|e| match e {
			Error::Encryption(_) => e,
			e => unusable(format!("its value log is not usable: {e}")),
		})?;

		// The tables are opened with the key of this database, which a checkpoint of it needs, and
		// a table that does not open for want of one says so, and is not called damaged.
		let levels = LevelManifest::load_from_file(&manifest_file, Arc::clone(&opts)).map_err(
			|e| match e {
				Error::Encryption(_) => e,
				e => unusable(format!("its manifest and tables do not load: {e}")),
			},
		)?;

		// The tables read their values from the value-log files the checkpoint copied
		let oldest = levels.min_oldest_vlog_file_id();
		if oldest > 0 && !opts.vlog_file_path(u64::from(oldest)).is_file() {
			return Err(unusable(format!(
				"its tables point into the value log, which has no file {oldest}"
			)));
		}

		Ok(())
	}

	/// Refuses a checkpoint whose value-log files are not of the kind of this database: sealed
	/// files where it is not encrypted, plaintext files where it is, or sealed files of another
	/// database or that its key does not open. A restore replaces the log of the database and then
	/// reloads it, and a file that the reload refuses would leave nothing to go back to, so what
	/// the reload would refuse is found here, from the first bytes of each file and without
	/// building a log over the checkpoint, which could set a file aside inside it. Reads only.
	///
	/// A checkpoint copies the file that the writer of the log is appending to as it is at that
	/// moment, which may be one that is cut off inside its header: it holds no entry, and the
	/// reload sets it aside. Only the newest file can be that, as in the log itself.
	fn check_vlog_mode(
		&self,
		checkpoint_path: &Path,
		live: Option<&EncryptionContext>,
	) -> Result<()> {
		let io = |e| Error::Io(Arc::new(e));
		let entries = match fs::read_dir(checkpoint_path.join("vlog")) {
			Ok(entries) => entries,
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
			Err(e) => return Err(io(e)),
		};
		let mut files = Vec::new();
		for entry in entries {
			let entry = entry.map_err(io)?;
			if let Some(id) =
				self.core.opts.extract_vlog_file_id(&entry.file_name().to_string_lossy())
			{
				files.push((id, entry.path()));
			}
		}
		let Some(newest) = files.iter().map(|(id, _)| *id).max() else {
			return Ok(());
		};

		for (id, path) in files {
			let file = File::open(&path).map_err(io)?;
			let len = file.metadata().map_err(io)?.len();
			let mut head = Vec::with_capacity(sealed::SEALED_HEADER_LEN);
			file.take(sealed::SEALED_HEADER_LEN as u64).read_to_end(&mut head).map_err(io)?;
			check_vlog_head(live, id, id == newest, len, &head)?;
		}
		Ok(())
	}

	/// Marks the database directory as being restored, durably, before a file of the database is
	/// removed. A marker left by an earlier restore that stopped is written again.
	fn begin_restore(&self, checkpoint_path: &Path) -> Result<()> {
		let io = |e| Error::Io(Arc::new(e));
		let marker = self.core.opts.path.join(RESTORE_MARKER_FILE);
		let checkpoint = resolve(checkpoint_path).unwrap_or_else(|_| checkpoint_path.to_path_buf());
		let mut file = File::create(&marker).map_err(io)?;
		file.write_all(checkpoint.display().to_string().as_bytes()).map_err(io)?;
		file.sync_all().map_err(io)?;
		fsync_directory(&self.core.opts.path).map_err(io)
	}

	/// Ends a restore: the files of the checkpoint are on disk, and only then is the marker
	/// removed, so that a crash leaves either the marker or a complete database.
	fn finish_restore(&self) -> Result<()> {
		let io = |e| Error::Io(Arc::new(e));
		let opts = &self.core.opts;
		for dir in [opts.sstable_dir(), opts.wal_dir(), opts.manifest_dir(), opts.vlog_dir()] {
			sync_below(&dir).map_err(io)?;
		}
		// The names of those directories are entries of this one, and without their own sync a
		// crash could keep the removal of the marker and lose them, which opens as an empty
		// database
		fsync_directory(&opts.path).map_err(io)?;
		fs::remove_file(opts.path.join(RESTORE_MARKER_FILE)).map_err(io)?;
		fsync_directory(&opts.path).map_err(io)
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

	/// Removes the metadata file of an earlier checkpoint in `checkpoint_dir`, and makes the
	/// removal durable before the files it described are replaced.
	fn remove_stale_metadata(&self, checkpoint_dir: &Path) -> Result<()> {
		let io = |e| Error::Io(Arc::new(e));
		match fs::remove_file(checkpoint_dir.join(CHECKPOINT_METADATA_FILE)) {
			Ok(()) => {}
			Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
			Err(e) => return Err(io(e)),
		}
		fsync_directory(checkpoint_dir).map_err(io)
	}

	/// Writes checkpoint metadata to a file, and syncs it and the directories that name it
	fn write_checkpoint_metadata(
		&self,
		checkpoint_dir: &Path,
		metadata: &CheckpointMetadata,
	) -> Result<()> {
		let io = |e| Error::Io(Arc::new(e));
		let metadata_path = checkpoint_dir.join(CHECKPOINT_METADATA_FILE);
		let mut file = File::create(&metadata_path).map_err(io)?;

		let data = metadata.to_bytes()?;
		file.write_all(&data).map_err(io)?;
		file.sync_all().map_err(io)?;
		fsync_directory(checkpoint_dir).map_err(io)?;
		// The checkpoint directory itself, if this checkpoint created it
		if let Some(parent) = parent_directory(checkpoint_dir) {
			fsync_directory(parent).map_err(io)?;
		}

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
	fn the_parent_of_a_relative_name_is_the_current_directory() {
		assert_eq!(parent_directory(Path::new("cp")), Some(Path::new(".")));
		assert_eq!(parent_directory(Path::new("out/cp")), Some(Path::new("out")));
		assert_eq!(parent_directory(Path::new("/out/cp")), Some(Path::new("/out")));
		assert_eq!(parent_directory(Path::new("/")), None);
	}

	#[test]
	fn a_directory_is_read_by_many_and_written_by_one() {
		let dir = PathBuf::from("/a/directory/only/this/test/uses");
		let first = DestinationGuard::try_read(dir.clone()).unwrap();
		let second = DestinationGuard::try_read(dir.clone()).unwrap();
		drop(first);
		assert!(DESTINATIONS.lock().contains_key(&dir), "one reader is left");
		drop(second);
		assert!(!DESTINATIONS.lock().contains_key(&dir), "no reader is left");
		let writer = DestinationGuard::acquire(dir.clone());
		assert!(
			DestinationGuard::try_read(dir.clone()).is_none(),
			"no reading while it is written"
		);
		drop(writer);
		assert!(DestinationGuard::try_read(dir).is_some());
	}

	#[test]
	fn a_metadata_file_of_version_zero_is_refused() {
		let error = CheckpointMetadata::from_bytes(&[0u8; 36]).unwrap_err();
		assert!(error.to_string().contains("Unsupported checkpoint version: 0"), "{error}");
	}

	#[test]
	fn a_metadata_file_of_another_size_is_refused() {
		let bytes = CheckpointMetadata::new(100, 200, 300, 400).to_bytes().unwrap();
		for len in 0..bytes.len() {
			assert!(CheckpointMetadata::from_bytes(&bytes[..len]).is_err(), "{len} bytes");
		}
		let mut longer = bytes.clone();
		longer.push(0);
		let error = CheckpointMetadata::from_bytes(&longer).unwrap_err();
		assert!(error.to_string().contains("Invalid checkpoint metadata"), "{error}");
		assert!(CheckpointMetadata::from_bytes(&bytes).is_ok());
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
