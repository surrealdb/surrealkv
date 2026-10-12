//! Migration of a RocksDB database written by SurrealDB.
//!
//! The database is read as RocksDB's own recovery would read it (see `surrealkv-compat-rocksdb`):
//! the live tables, the blob files and the write-ahead log that no table covers. A database that
//! SurrealDB ran in versioned mode, with user-defined timestamps, comes over with the versions it
//! stores and the time of each (RocksDB has already dropped any that its own retention trimmed);
//! any other is reduced to the newest value of each key, since nothing of its history exists.
//!
//! # Versions and sequence numbers
//!
//! RocksDB serves the version with the newest timestamp, and refuses a commit with a timestamp
//! older than a newer write of the key, so timestamp order and write order agree. Versions are
//! numbered in that order, newest highest, from an upper bound on their number down. The
//! timestamps are SurrealDB's hybrid logical clock (milliseconds since the epoch in the upper 48
//! bits and a counter below); they are converted to nanoseconds since the epoch, which is what
//! SurrealDB's SurrealKV backend reads `VERSION` as ([`hlc_to_nanos`], in one place).
//!
//! A delete in a versioned database is a version of its own, kept as a soft delete at its time.
//!
//! # Crash safety
//!
//! Everything is built in `_rocksdb_migration/` beside the RocksDB files and moved into place in
//! the order that leaves no state the next open cannot read: tables and value log first, the
//! manifest last, the RocksDB files into `_rocksdb_backup/` after that. A marker file says that a
//! run is unfinished: the next open rolls it forward if the manifest was placed, which is the last
//! step that matters, and back otherwise. RocksDB takes
//! its `LOCK` file with a POSIX record lock, which the lock SurrealKV takes does not conflict with,
//! so this takes that same lock: a live RocksDB process keeps the migration out.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use parking_lot::Mutex;
use surrealkv_compat_rocksdb::{is_rocksdb_dir, lock_directory, Payload, RocksDb, VersionKind};

use crate::error::{Error, Result};
use crate::levels::{replace_file_content, DecodedManifest, MANIFEST_FORMAT_VERSION};
use crate::lsm::fsync_directory;
use crate::memtable::maybe_separate_to_vlog;
use crate::migrate::{for_each_entry, Digest};
use crate::sstable::table::{Table, TableWriter};
use crate::vfs::File;
use crate::vlog::{VLog, ValueLocation};
use crate::{InternalKey, InternalKeyKind, Options, Tree};

const MARKER_FILE: &str = "MIGRATING_ROCKSDB";
const STAGING_DIR: &str = "_rocksdb_migration";
pub(crate) const BACKUP_DIR: &str = "_rocksdb_backup";

const PHASE_STAGING: &str = "staging";
const PHASE_PLACING: &str = "placing";
const PHASE_PLACED: &str = "placed";

/// What a migration did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct RocksReport {
	/// Whether the database kept user-defined timestamps (SurrealDB's versioned mode).
	pub versioned: bool,
	/// Versions written. For an unversioned database that is one per live key.
	pub versions: u64,
	/// Tables written.
	pub tables: usize,
	/// Values written to the value log, which are those above `vlog_value_threshold`, wherever
	/// RocksDB kept them.
	pub values_in_value_log: u64,
	/// Whether the write-ahead log ended in a damaged record.
	pub wal_damaged: bool,
}

/// SurrealDB's hybrid logical clock to nanoseconds since the epoch, keeping the order: the counter
/// is below 65,536 and a millisecond is a million nanoseconds. A timestamp of zero, which is what
/// garbage collection leaves on the base version of a key, stays zero.
pub(crate) fn hlc_to_nanos(hlc: u64) -> u64 {
	if hlc == 0 {
		return 0;
	}
	(hlc >> 16).saturating_mul(1_000_000).saturating_add(hlc & 0xffff)
}

fn failed(message: impl Into<String>) -> Error {
	Error::MigrationFailed(message.into())
}

fn from_reader(e: surrealkv_compat_rocksdb::Error) -> Error {
	failed(e.to_string())
}

/// Directories being migrated by this process, which the record lock cannot exclude: POSIX record
/// locks do not conflict inside one process.
fn in_progress() -> &'static Mutex<HashSet<PathBuf>> {
	static SET: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
	SET.get_or_init(|| Mutex::new(HashSet::new()))
}

struct ProcessGuard(PathBuf);

impl ProcessGuard {
	fn enter(dir: &Path) -> Result<Self> {
		let key = fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
		if !in_progress().lock().insert(key.clone()) {
			return Err(failed(format!(
				"{} is already being migrated by this process",
				dir.display()
			)));
		}
		Ok(Self(key))
	}
}

impl Drop for ProcessGuard {
	fn drop(&mut self) {
		in_progress().lock().remove(&self.0);
	}
}

/// Takes the lock RocksDB takes on `LOCK`, or fails if a RocksDB process holds it. The lock is
/// held as long as the returned guard lives.
fn lock_rocksdb(dir: &Path) -> Result<surrealkv_compat_rocksdb::DirectoryLock> {
	lock_directory(dir).map_err(|e| match e {
		surrealkv_compat_rocksdb::Error::InUse(message) => {
			failed(format!("{message}; stop it, then open the directory again to migrate it"))
		}
		other => from_reader(other),
	})
}

/// Migrates the database in `opts.path` if it is a RocksDB database, or finishes or undoes a
/// migration that was interrupted. Returns `None` if there was nothing to do.
pub(crate) fn migrate_if_needed(opts: &Arc<Options>) -> Result<Option<RocksReport>> {
	migrate_with(opts, None)
}

/// [`migrate_if_needed`] with a failure injected after `fail_after` steps, to test that a run cut
/// short at any point is resumed.
#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) fn migrate_failing_after(
	opts: &Arc<Options>,
	fail_after: usize,
) -> Result<Option<RocksReport>> {
	migrate_with(opts, Some(fail_after))
}

fn migrate_with(opts: &Arc<Options>, fail_after: Option<usize>) -> Result<Option<RocksReport>> {
	let dir = &opts.path;
	let marker = dir.join(MARKER_FILE);
	let interrupted = marker.try_exists()?;
	if !interrupted && !is_rocksdb_dir(dir) {
		return Ok(None);
	}
	// Before the directory is locked, read or moved: this migration writes without a key and keeps
	// the old files in the clear.
	Tree::refuse_migration_into_an_encrypted_database(opts, "RocksDB")?;

	let _guard = ProcessGuard::enter(dir)?;
	let _lock = lock_rocksdb(dir)?;
	let mut run = Run {
		opts,
		fail_after,
		steps: 0,
	};
	if interrupted {
		run.recover()?;
		if !is_rocksdb_dir(dir) {
			// A run that had placed its output and was finished.
			return Ok(None);
		}
	}
	if opts.manifest_file_path(0).try_exists()? {
		return Err(failed(format!(
			"{} holds a SurrealKV manifest and RocksDB files; it is not a RocksDB database to migrate",
			dir.display()
		)));
	}
	run.migrate().map(Some)
}

struct Run<'a> {
	opts: &'a Arc<Options>,
	fail_after: Option<usize>,
	steps: usize,
}

/// The entries of a table written by the migration, as the digest sees them.
fn digest_value(value: &[u8]) -> Result<(Option<u64>, bool, Vec<u8>)> {
	if value.is_empty() {
		return Ok((None, false, Vec::new()));
	}
	let location = ValueLocation::decode(value)?;
	Ok((location.timestamp, location.is_value_pointer(), location.value))
}

impl Run<'_> {
	/// A point where a run may be interrupted: after it, what was done is durable.
	fn step(&mut self, what: &str) -> Result<()> {
		tracing::debug!("RocksDB migration step: {what}");
		if self.fail_after == Some(self.steps) {
			return Err(failed(format!("injected failure after step {}: {what}", self.steps)));
		}
		self.steps += 1;
		Ok(())
	}

	fn marker_path(&self) -> PathBuf {
		self.opts.path.join(MARKER_FILE)
	}

	fn staging(&self) -> PathBuf {
		self.opts.path.join(STAGING_DIR)
	}

	fn backup(&self) -> PathBuf {
		self.opts.path.join(BACKUP_DIR)
	}

	fn write_marker(&self, phase: &str) -> Result<()> {
		replace_file_content(self.marker_path(), phase.as_bytes())?;
		fsync_directory(&self.opts.path)?;
		Ok(())
	}

	/// Finishes or undoes a run that was interrupted. The manifest is placed last, by one rename,
	/// so whether it is there says how far the run got; the marker's text is only a note.
	fn recover(&mut self) -> Result<()> {
		if self.opts.manifest_file_path(0).try_exists()? {
			return self.finish_placing();
		}
		// Before the manifest was placed nothing of the output is the database's: undo it. The
		// directories were empty or absent when the run began, so what is in them is its own.
		for sub in ["sstables", "vlog", "manifest"] {
			let path = self.opts.path.join(sub);
			if path.is_dir() {
				fs::remove_dir_all(&path)?;
			}
		}
		if self.staging().is_dir() {
			fs::remove_dir_all(self.staging())?;
		}
		fs::remove_file(self.marker_path())?;
		fsync_directory(&self.opts.path)?;
		Ok(())
	}

	/// Moves the RocksDB files into the backup and removes the marker.
	fn finish_placing(&mut self) -> Result<()> {
		let dir = self.opts.path.clone();
		let backup = self.backup();
		fs::create_dir_all(&backup)?;
		for entry in fs::read_dir(&dir)?.flatten() {
			let name = entry.file_name().to_string_lossy().to_string();
			if !is_rocksdb_file(&name, entry.path().is_dir()) {
				continue;
			}
			fs::rename(entry.path(), backup.join(&name))?;
		}
		fsync_directory(&backup)?;
		fsync_directory(&dir)?;
		self.step("RocksDB files moved")?;
		let _ = fs::remove_dir(self.staging());
		fs::remove_file(self.marker_path())?;
		fsync_directory(&dir)?;
		Ok(())
	}

	fn migrate(&mut self) -> Result<RocksReport> {
		let opts = Arc::clone(self.opts);
		// Read the database before anything is written: a directory this cannot read is left as
		// it is.
		let db = RocksDb::open(&opts.path).map_err(from_reader)?;
		for sub in ["sstables", "vlog", "wal", "manifest"] {
			let path = opts.path.join(sub);
			if path.is_dir() && fs::read_dir(&path)?.next().is_some() {
				return Err(failed(format!(
					"{} holds files beside the RocksDB database; it is not a plain RocksDB database to migrate",
					path.display()
				)));
			}
		}
		let staging = self.staging();
		if staging.is_dir() {
			fs::remove_dir_all(&staging)?;
		}
		self.write_marker(PHASE_STAGING)?;
		self.step("marker written")?;

		let versioned = db.has_user_timestamps();
		if db.wal_damaged {
			tracing::warn!("the RocksDB write-ahead log ends in a damaged record; recovery stops there, as RocksDB's does");
		}
		if let Some(cutoff) = db.manifest.history_cutoff() {
			tracing::info!("the database's history was trimmed before timestamp {cutoff}");
		}

		// The output is built as a database of its own, in the staging directory.
		for sub in ["sstables", "vlog", "manifest"] {
			fs::create_dir_all(staging.join(sub))?;
		}
		let mut staged = (*opts).clone();
		staged.path.clone_from(&staging);
		let staged = Arc::new(staged);
		let vlog = if opts.enable_vlog {
			Some(Arc::new(VLog::new(Arc::clone(&staged))?))
		} else {
			None
		};

		let level = opts.level_count.saturating_sub(1) as usize;
		let upper_bound = db.entry_upper_bound().max(1);
		let mut next_sequence = upper_bound;
		let mut report = RocksReport {
			versioned,
			wal_damaged: db.wal_damaged,
			..Default::default()
		};
		let mut blobs = db.blob_files();
		let mut expected = Digest::new();
		let mut table_ids: Vec<u64> = Vec::new();
		let mut writer: Option<(TableWriter<fs::File>, fs::File, u64)> = None;
		let mut table_bytes: u64 = 0;
		let mut current_key: Option<Vec<u8>> = None;
		let mut decided_key: Option<Vec<u8>> = None;

		for version in db.versions().map_err(from_reader)? {
			let version = version.map_err(from_reader)?;
			if !versioned {
				// Nothing sits below a full migration: the newest version of a key is all there
				// is, and a key whose newest version is a delete is gone.
				if decided_key.as_deref() == Some(version.user_key.as_slice()) {
					continue;
				}
				decided_key = Some(version.user_key.clone());
				if version.kind == VersionKind::Delete {
					continue;
				}
			}

			let new_key = current_key.as_deref() != Some(version.user_key.as_slice());
			if new_key && table_bytes >= opts.target_file_size {
				self.finish_table(&mut writer, &mut table_ids)?;
				table_bytes = 0;
			}
			if writer.is_none() {
				let id = table_ids.len() as u64 + 1;
				let file = fs::File::create(staged.sstable_file_path(id))?;
				let sync_handle = file.try_clone()?;
				writer = Some((
					TableWriter::new(file, id, Arc::clone(&staged), level as u8),
					sync_handle,
					id,
				));
			}
			current_key = Some(version.user_key.clone());

			if next_sequence == 0 {
				return Err(failed("the database holds more versions than its tables say"));
			}
			let sequence = next_sequence;
			next_sequence -= 1;

			let timestamp = version.timestamp.map(hlc_to_nanos);
			let (kind, stored) = match version.kind {
				VersionKind::Put => {
					let value = match version.payload {
						Payload::Inline(value) => value,
						Payload::Blob(blob) => {
							blobs.read(&blob, &version.user_key).map_err(from_reader)?
						}
						Payload::None => return Err(failed("a put without a value")),
					};
					let encoded =
						ValueLocation::with_inline_value(value).with_timestamp(timestamp).encode();
					let key =
						InternalKey::new(version.user_key.clone(), sequence, InternalKeyKind::Set);
					let stored = maybe_separate_to_vlog(
						&encoded,
						&key,
						vlog.as_ref(),
						opts.vlog_value_threshold,
					)?;
					if ValueLocation::decode(&stored)?.is_value_pointer() {
						report.values_in_value_log += 1;
					}
					(InternalKeyKind::Set, stored)
				}
				// Only a versioned database gets here; a delete is a version at its time.
				VersionKind::Delete => (
					InternalKeyKind::SoftDelete,
					ValueLocation::with_inline_value(Vec::new()).with_timestamp(timestamp).encode(),
				),
			};
			let (ts, pointer, payload) = digest_value(&stored)?;
			expected.add(&version.user_key, sequence, kind, ts, pointer, &payload);
			table_bytes += (version.user_key.len() + stored.len() + 16) as u64;
			let (table, _, _) = writer.as_mut().expect("opened above");
			table.add(InternalKey::new(version.user_key, sequence, kind), &stored)?;
			report.versions += 1;
		}
		self.finish_table(&mut writer, &mut table_ids)?;
		if let Some(vlog) = vlog {
			vlog.sync()?;
			// Closed before it is moved: an open file cannot be renamed everywhere.
			drop(vlog);
		}
		report.tables = table_ids.len();
		self.step("tables written")?;

		// Read every table back through the engine's reader and compare with what was written.
		let mut actual = Digest::new();
		for id in &table_ids {
			let path = staged.sstable_file_path(*id);
			let file: Arc<dyn File> = Arc::new(fs::File::open(&path)?);
			let size = fs::metadata(&path)?.len() as usize;
			let table = Table::new(*id, Arc::clone(&staged), file, size as u64)
				.map_err(|e| failed(format!("the migrated table {id} does not open: {e}")))?;
			for_each_entry(&table, &mut |entry| {
				let (ts, pointer, payload) = digest_value(&entry.value)?;
				actual.add(&entry.user_key, entry.seq, entry.kind, ts, pointer, &payload);
				Ok(())
			})?;
		}
		if expected.finish() != actual.finish() {
			return Err(failed(format!(
				"the migrated tables do not match what was read ({:?} against {:?}); nothing was changed",
				actual.finish(),
				expected.finish()
			)));
		}

		// The manifest of the migrated database: all tables on the bottom level, in key order.
		let level_count = opts.level_count as usize;
		let mut level_data = vec![Vec::new(); level_count.max(1)];
		level_data[level.min(level_count.saturating_sub(1))].clone_from(&table_ids);
		let manifest = DecodedManifest {
			version: MANIFEST_FORMAT_VERSION,
			next_table_id: table_ids.len() as u64 + 1,
			log_number: 0,
			last_sequence: upper_bound,
			level_data,
			snapshots: Vec::new(),
		};
		replace_file_content(staged.manifest_file_path(0), &manifest.encode()?)?;
		for sub in ["sstables", "vlog", "manifest"] {
			fsync_directory(staging.join(sub))?;
		}
		fsync_directory(&staging)?;
		self.step("staged database complete")?;

		// Place it: tables and value log first, the manifest last.
		self.write_marker(PHASE_PLACING)?;
		self.step("placing")?;
		for sub in ["sstables", "vlog"] {
			let to = opts.path.join(sub);
			if to.is_dir() {
				fs::remove_dir(&to)?;
			}
			fs::rename(staging.join(sub), &to)?;
			fsync_directory(&opts.path)?;
			self.step("directory placed")?;
		}
		let manifest_dir = opts.path.join("manifest");
		if manifest_dir.is_dir() {
			fs::remove_dir(&manifest_dir)?;
		}
		fs::rename(staging.join("manifest"), &manifest_dir)?;
		fsync_directory(&opts.path)?;
		self.step("manifest placed")?;
		self.write_marker(PHASE_PLACED)?;
		self.step("placed")?;
		self.finish_placing()?;

		tracing::info!(
			"Migrated the RocksDB database at {:?}: {} version(s) in {} table(s). The RocksDB files are kept in {:?}; they can be deleted once the migrated database is checked",
			opts.path,
			report.versions,
			report.tables,
			self.backup()
		);
		Ok(report)
	}

	fn finish_table(
		&mut self,
		writer: &mut Option<(TableWriter<fs::File>, fs::File, u64)>,
		table_ids: &mut Vec<u64>,
	) -> Result<()> {
		if let Some((table, sync_handle, id)) = writer.take() {
			table.finish()?;
			sync_handle.sync_all()?;
			table_ids.push(id);
		}
		Ok(())
	}
}

/// Whether a file in a database directory is one RocksDB wrote, and so one the backup takes.
fn is_rocksdb_file(name: &str, is_dir: bool) -> bool {
	if is_dir {
		return name == "archive";
	}
	name.ends_with(".sst")
		|| name.ends_with(".blob")
		|| name.ends_with(".log")
		|| name.ends_with(".dbtmp")
		|| name.starts_with("MANIFEST-")
		|| name.starts_with("OPTIONS-")
		|| name == "CURRENT"
		|| name == "IDENTITY"
		|| name == "LOG"
		|| name.starts_with("LOG.old.")
}
