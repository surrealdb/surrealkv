//! Conversion of a database of LSMV1, the format of every SurrealKV release up to v0.21.4, to
//! LSMV2, which is what 1.0 writes: internal keys `user_key ‖ trailer ‖ timestamp` in tables and a
//! version 1 manifest become flat keys `user_key ‖ trailer` and a version 2 manifest.
//!
//! This file only writes. Everything of LSMV1 that has to be read, the tables, the write-ahead log
//! and the way a table says that it is one, is read by the standalone `surrealkv-compat-v1` crate,
//! which has frozen decoders of that format and depends on nothing of SurrealKV. What is converted
//! is handed over by it as entries; the manifest has the same binary layout in both formats and is
//! read and written with the manifest of this engine.
//!
//! # Which versions
//!
//! Verified against databases written by the real engines of v0.20.0, v0.20.1, v0.20.2, v0.21.2
//! and v0.21.4, and by reopening each with the engine that wrote it. v0.20.0 and v0.20.1 logged
//! value pointers in the WAL, which converts like any other value. Older versions are not
//! converted: they numbered the value log from file 0, which garbage collection cannot tell from
//! a table without pointers, and a directory with a file 0 is refused before anything is changed.
//!
//! # What is converted
//!
//! * Every SSTable the manifest lists in LSMV1 is rewritten, one for one and level for level, into
//!   a table in LSMV2. Entries keep their user key, sequence number and kind. The timestamp that
//!   was a part of the key moves into the value (`ValueLocation`), so history survives. Values stay
//!   as they are: a pointer into the value log still points at the same bytes, and the value log is
//!   not touched.
//! * The write-ahead log that was not yet flushed is read with the rules of the version that wrote
//!   it, and written as one new level-0 table with the timestamps its records carry. Without this
//!   the timestamps of the unflushed tail would be lost, since replay discards them.
//!
//! # Crash safety
//!
//! The directory is locked for the whole run. A table is written under a temporary name,
//! synced, renamed, and read back and compared with what was read from the source before the
//! manifest names it; the manifest is then replaced atomically with that one table swapped in
//! place, and the old file is moved into the backup directory. A crash leaves either the
//! old manifest (the new table is an orphan that startup removes) or the new one, so the
//! next open classifies every listed table again and carries on. The first swap stamps the
//! manifest as version 2, which keeps older binaries, that cannot read a current table, from
//! opening a half-migrated directory. A marker file records that a migration is in progress, since
//! a version 2 manifest alone would say it is done.
//!
//! # What is kept
//!
//! The replaced tables, the old manifest and the consumed WAL segments are kept in
//! `_surrealkv_v0_backup/`. It is not a rollback point: the value log is shared, and garbage
//! collection of it follows the new tables. Delete the directory once the migrated database has
//! been verified.

use std::cmp::Reverse;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use surrealkv_compat_v1::{
	decode_value,
	read_wal_segment,
	table_layout,
	wal_segments,
	Kind,
	Layout,
	TableLayout,
	TableReader,
	WalSegment,
};

use crate::error::{Error, Result};
use crate::levels::{
	manifest_kind,
	replace_file_content,
	DecodedManifest,
	ManifestKind,
	MANIFEST_FORMAT_VERSION,
	MANIFEST_FORMAT_VERSION_V1,
};
use crate::lockfile::LockFile;
use crate::lsm::fsync_directory;
use crate::migrate::{for_each_entry, Digest};
use crate::sstable::table::{refuse_legacy_table, Table, TableWriter};
use crate::vfs::File;
use crate::vlog::{ValueLocation, BIT_VALUE_POINTER, VALUE_LOCATION_VERSION};
use crate::{InternalKey, InternalKeyKind, Options, Tree, WalRecoveryMode};

/// Where the replaced files are kept.
pub(crate) const BACKUP_DIR: &str = "_surrealkv_v0_backup";
/// Present while a migration is in progress.
const MARKER_FILE: &str = "MIGRATING";
/// Suffix of a table that is being written. Not `.sst`, so nothing mistakes it for a table.
const TEMP_SUFFIX: &str = ".migrating";

/// What a migration did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct MigrationReport {
	/// Tables rewritten from the legacy layout.
	pub tables_converted: usize,
	/// Entries read from legacy tables and written to current ones.
	pub entries_converted: u64,
	/// Entries dropped because an earlier entry had the same user key and sequence number.
	pub duplicates_dropped: u64,
	/// WAL records written into a new level-0 table.
	pub wal_entries_converted: u64,
	/// WAL segments consumed.
	pub wal_segments_converted: usize,
}

/// Migrates the database in `opts.path` if it was written by SurrealKV v0.x. Returns `None` if
/// there was nothing to do, which is the case for every database written by the current format.
pub(crate) fn migrate_if_needed(opts: &Arc<Options>) -> Result<Option<MigrationReport>> {
	migrate_with(opts, None, false)
}

/// Like [`migrate_if_needed`], but classifies the tables whatever the manifest and the marker say.
/// For a directory that turned out to hold legacy tables although nothing said so, which only
/// happens if the marker of an interrupted migration was lost.
pub(crate) fn migrate_forced(opts: &Arc<Options>) -> Result<Option<MigrationReport>> {
	migrate_with(opts, None, true)
}

/// [`migrate_if_needed`] with a failure injected after `fail_after` completed steps, to test that a
/// run cut short at any point is resumed to the same result.
#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) fn migrate_failing_after(
	opts: &Arc<Options>,
	fail_after: usize,
) -> Result<Option<MigrationReport>> {
	migrate_with(opts, Some(fail_after), false)
}

fn migrate_with(
	opts: &Arc<Options>,
	fail_after: Option<usize>,
	force: bool,
) -> Result<Option<MigrationReport>> {
	let manifest_path = opts.manifest_file_path(0);
	let marker = opts.path.join(MARKER_FILE);

	// Cheap and lock-free: a version 2 manifest with no marker is a current database. A new
	// database has no manifest.
	let Some(first_look) = read_manifest(&manifest_path)? else {
		return Ok(None);
	};
	if first_look.version != MANIFEST_FORMAT_VERSION_V1 && !force && !marker.try_exists()? {
		return Ok(None);
	}
	// There may be something to convert. This writes without a key and keeps the old files in the
	// clear, so it is refused before the directory is locked, read or moved.
	Tree::refuse_migration_into_an_encrypted_database(opts, "SurrealKV v0.x")?;

	// Another process may be migrating or using it; the database lock decides, and everything is
	// read again once it is held.
	let mut lock = LockFile::new(&opts.path);
	lock.acquire()?;
	let result = Migration::new(opts, fail_after, force).run();
	let _ = lock.release();
	// A run that stops after its first swap leaves a directory that only a resumed migration can
	// open, and neither this engine nor v0.x opens it otherwise. Say so where the error is read.
	result.map_err(|e| {
		// The marker is written before anything is replaced, so a run that stopped before it
		// changed nothing, and its error says what it found.
		if marker.try_exists().unwrap_or(true) {
			failed(format!(
				"{e}. The database is partly migrated and opening it again resumes the migration. \
				 The files that were replaced, and the manifest as it was, are in {:?}",
				opts.path.join(BACKUP_DIR)
			))
		} else {
			e
		}
	})
}

/// The manifest at `path`, or `None` if there is none or it is sealed: the manifest of an encrypted
/// database, which no release that wrote LSMV1 could have written.
fn read_manifest(path: &Path) -> Result<Option<DecodedManifest>> {
	match fs::read(path) {
		Ok(data) if manifest_kind(&data) == ManifestKind::Sealed => Ok(None),
		Ok(data) => Ok(Some(DecodedManifest::decode(data)?)),
		Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
		Err(e) => Err(e.into()),
	}
}

/// Maps an error of the reader of LSMV1 to one of this engine.
fn from_reader(e: surrealkv_compat_v1::Error) -> Error {
	match e {
		surrealkv_compat_v1::Error::Io(e) => e.into(),
		surrealkv_compat_v1::Error::Corrupted(message) => Error::Corruption(message),
		surrealkv_compat_v1::Error::Unsupported(message) => {
			failed(format!("unsupported by the reader of LSMV1: {message}"))
		}
	}
}

fn failed(message: impl Into<String>) -> Error {
	Error::MigrationFailed(message.into())
}

/// What a legacy entry becomes: its value in the current layout, and what goes into the digest.
struct Converted {
	value: Vec<u8>,
	timestamp: Option<u64>,
	pointer: bool,
	payload: Vec<u8>,
}

/// The kind of this engine that is the kind `kind` of LSMV1: the two number them alike.
fn internal_kind(kind: Kind) -> InternalKeyKind {
	InternalKeyKind::from(kind as u8)
}

/// Converts a value written by SurrealKV v0.x, which holds a `ValueLocation` for every kind but a
/// hard delete (empty), to the current layout with its timestamp.
///
/// A hard delete is a barrier that hides every older version whatever its time, so it keeps none.
/// A soft delete is a version of its own at its time, so it keeps one: an empty value that says
/// when.
fn convert_value(kind: Kind, timestamp: u64, legacy: &[u8]) -> Result<Converted> {
	match kind {
		Kind::Delete => Ok(Converted {
			value: Vec::new(),
			timestamp: None,
			pointer: false,
			payload: Vec::new(),
		}),
		Kind::SoftDelete => Ok(Converted {
			value: ValueLocation::with_inline_value(Vec::new())
				.with_timestamp(Some(timestamp))
				.encode(),
			timestamp: Some(timestamp),
			pointer: false,
			payload: Vec::new(),
		}),
		Kind::Set | Kind::Replace | Kind::Merge | Kind::LogData => {
			let stored = decode_value(kind, legacy)
				.map_err(from_reader)?
				.ok_or_else(|| failed(format!("an entry of kind {kind:?} holds no value")))?;
			let location = if stored.pointer {
				ValueLocation::new(
					BIT_VALUE_POINTER,
					stored.payload.clone(),
					VALUE_LOCATION_VERSION,
				)
			} else {
				ValueLocation::with_inline_value(stored.payload.clone())
			};
			Ok(Converted {
				value: location.with_timestamp(Some(timestamp)).encode(),
				timestamp: Some(timestamp),
				pointer: stored.pointer,
				payload: stored.payload,
			})
		}
		other => Err(failed(format!("a legacy table holds an entry of kind {other:?}"))),
	}
}

/// The unflushed WAL of the database, ready to be written as a table.
struct WalTail {
	/// `(user key, newest first)` to `(kind, value)`: the order of a table.
	entries: BTreeMap<(Vec<u8>, Reverse<u64>), (InternalKeyKind, Converted)>,
	segments: Vec<WalSegment>,
	last_segment: u64,
	max_seq: u64,
}

struct Migration<'a> {
	opts: &'a Arc<Options>,
	manifest: Option<DecodedManifest>,
	report: MigrationReport,
	fail_after: Option<usize>,
	steps: usize,
	force: bool,
}

impl<'a> Migration<'a> {
	fn new(opts: &'a Arc<Options>, fail_after: Option<usize>, force: bool) -> Self {
		Self {
			opts,
			manifest: None,
			report: MigrationReport::default(),
			fail_after,
			steps: 0,
			force,
		}
	}

	/// A point where a run may be interrupted: after it, what was done is durable.
	fn step(&mut self, what: &str) -> Result<()> {
		tracing::debug!("migration step: {what}");
		if self.fail_after == Some(self.steps) {
			return Err(failed(format!("injected failure after step {}: {what}", self.steps)));
		}
		self.steps += 1;
		Ok(())
	}

	fn manifest(&mut self) -> &mut DecodedManifest {
		self.manifest.as_mut().expect("the manifest is read first")
	}

	fn backup_dir(&self) -> PathBuf {
		self.opts.path.join(BACKUP_DIR)
	}

	fn temp_path(&self, id: u64) -> PathBuf {
		self.opts.sstable_dir().join(format!("{id:020}{TEMP_SUFFIX}"))
	}

	fn sync_dirs(&self, dirs: &[PathBuf]) -> Result<()> {
		for dir in dirs {
			fsync_directory(dir)?;
		}
		Ok(())
	}

	fn run(&mut self) -> Result<Option<MigrationReport>> {
		let manifest_path = self.opts.manifest_file_path(0);
		let marker = self.opts.path.join(MARKER_FILE);

		// Read everything again now that the lock is held.
		let Some(manifest) = read_manifest(&manifest_path)? else {
			return Ok(None);
		};
		let in_progress = self.force || marker.try_exists()?;
		self.manifest = Some(manifest);

		// A database restored by a tool that does not keep empty directories has none for tables.
		fs::create_dir_all(self.opts.sstable_dir())?;

		// Tables written under a temporary name by a run that was cut short.
		self.remove_temporary_tables()?;

		// Classify every table the manifest lists. Files it does not list are orphans, which
		// startup removes.
		let mut legacy: Vec<(usize, usize, u64)> = Vec::new();
		let mut listed = 0usize;
		for (level, ids) in self.manifest().level_data.clone().iter().enumerate() {
			for (position, &id) in ids.iter().enumerate() {
				listed += 1;
				if self.classify(id)? == TableLayout::Lsmv1 {
					legacy.push((level, position, id));
				}
			}
		}

		let version_one = self.manifest().version == MANIFEST_FORMAT_VERSION_V1;
		// A version 1 manifest that lists no table is a database whose data is all in the WAL. A
		// version 1 manifest that lists current tables is a development build of this engine,
		// whose WAL is replayed as it is.
		let wal_only = version_one && listed == 0;
		if legacy.is_empty() && !in_progress && !wal_only {
			return Ok(None);
		}

		// SurrealKV numbered its value-log files from 0 before v0.20 and from 1 since. A table that
		// points into file 0 records 0 as the oldest file it uses, which is also what a table
		// without pointers records, so garbage collection could not tell the two apart and would
		// delete a file that is still read. This migration is for v0.20.0 and later: refuse the
		// older directory before anything is changed.
		if !in_progress && self.layout().vlog_path(0).try_exists()? {
			return Err(failed(format!(
				"the value log at {} holds file 0, which only SurrealKV before v0.20 wrote; this \
				 migration covers v0.20.0 to v0.21.x. Nothing was changed. Open the database with \
				 SurrealKV v0.20 or v0.21 first",
				self.layout().vlog_dir().display()
			)));
		}

		tracing::info!(
			"Migrating the SurrealKV v0.x database at {:?}: {} table(s) in the legacy layout",
			self.opts.path,
			legacy.len()
		);

		self.prepare_backup(&manifest_path)?;
		fs::write(&marker, b"migration in progress\n")?;
		self.sync_dirs(std::slice::from_ref(&self.opts.path))?;
		self.step("marker written")?;

		if in_progress {
			self.rescue_unlisted_legacy_tables()?;
		}

		for (level, position, id) in legacy {
			self.convert_table(level, position, id)?;
		}

		self.convert_wal_tail()?;
		self.move_flushed_wal_segments()?;

		// Whatever the run converted, the manifest now says version 2.
		if self.manifest().version != MANIFEST_FORMAT_VERSION {
			self.manifest().version = MANIFEST_FORMAT_VERSION;
			self.write_manifest(&manifest_path)?;
		}

		// The B+tree index of versions that v0.x kept is derived data this engine does not read.
		// It was filled at flush and only trimmed with the value log, so it can still hold versions
		// that compaction has since dropped from the tables: keep it with the other replaced files
		// instead of deleting it.
		let versioned_index = self.opts.path.join("versioned_index");
		if versioned_index.is_dir() {
			let index_file = versioned_index.join("index.bpt");
			if fs::metadata(&index_file).is_ok_and(|m| m.len() > 0) {
				tracing::warn!(
					"the database kept a versioned index; versions that only it still held are not migrated, and the index is kept in {:?}",
					self.backup_dir().join("versioned_index")
				);
			}
			let target = self.backup_dir().join("versioned_index");
			if target.exists() {
				fs::remove_dir_all(&versioned_index)?;
			} else {
				fs::rename(&versioned_index, &target)?;
			}
			self.sync_dirs(&[self.backup_dir(), self.opts.path.clone()])?;
		}
		self.remove_temporary_tables()?;
		self.step("cleaned up")?;

		fs::remove_file(&marker)?;
		self.sync_dirs(std::slice::from_ref(&self.opts.path))?;

		tracing::info!(
			"Migration finished: {} table(s), {} entries, {} WAL records. The replaced files are in {:?}; delete it once the database is verified",
			self.report.tables_converted,
			self.report.entries_converted,
			self.report.wal_entries_converted,
			self.backup_dir()
		);
		Ok(Some(self.report.clone()))
	}

	/// The paths of the database as LSMV1 laid them out, which is where it is read from.
	fn layout(&self) -> Layout {
		Layout::new(self.opts.path.clone())
	}

	/// Which format table `id` is in, which only the reader of LSMV1 can tell.
	fn classify(&self, id: u64) -> Result<TableLayout> {
		let path = self.layout().sstable_path(id);
		fs::File::open(&path).map_err(|e| {
			failed(format!(
				"table {id} is listed by the manifest but {} cannot be opened: {e}",
				path.display()
			))
		})?;
		table_layout(&path).map_err(|e| failed(format!("table {id} cannot be classified: {e}")))
	}

	fn remove_temporary_tables(&self) -> Result<()> {
		let dir = self.opts.sstable_dir();
		let entries = match fs::read_dir(&dir) {
			Ok(entries) => entries,
			Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
			Err(e) => return Err(e.into()),
		};
		for entry in entries.flatten() {
			if entry.file_name().to_string_lossy().ends_with(TEMP_SUFFIX) {
				fs::remove_file(entry.path())?;
			}
		}
		Ok(())
	}

	fn prepare_backup(&self, manifest_path: &Path) -> Result<()> {
		let backup = self.backup_dir();
		for sub in ["sstables", "wal", "manifest"] {
			fs::create_dir_all(backup.join(sub))?;
		}
		// The manifest as it was when the migration began; a resumed run keeps the first.
		let saved = backup.join("manifest").join("00000000000000000000.manifest");
		if !saved.try_exists()? {
			let current = fs::read(manifest_path)?;
			if DecodedManifest::decode(current.clone())?.version == MANIFEST_FORMAT_VERSION_V1 {
				// Written and synced under a temporary name, so a crash never leaves a part.
				replace_file_content(&saved, &current)?;
			}
		}
		self.sync_dirs(&[
			backup.join("manifest"),
			backup.join("sstables"),
			backup.join("wal"),
			backup,
		])
	}

	/// A run cut short between naming a converted table in the manifest and moving the table it
	/// replaced leaves that table in `sstables/`, unlisted. Startup deletes unlisted tables as
	/// orphans, so move the legacy ones into the backup first.
	fn rescue_unlisted_legacy_tables(&self) -> Result<()> {
		let listed: HashSet<u64> =
			self.manifest.as_ref().expect("read").level_data.iter().flatten().copied().collect();
		let backup = self.backup_dir().join("sstables");
		let mut moved = false;
		for entry in fs::read_dir(self.opts.sstable_dir())?.flatten() {
			let name = entry.file_name().to_string_lossy().to_string();
			let Some(id) = name.strip_suffix(".sst").and_then(|stem| stem.parse::<u64>().ok())
			else {
				continue;
			};
			if listed.contains(&id) {
				continue;
			}
			if self.classify(id).is_ok_and(|layout| layout == TableLayout::Lsmv1) {
				fs::rename(entry.path(), backup.join(&name))?;
				moved = true;
			}
		}
		if moved {
			self.sync_dirs(&[self.opts.sstable_dir(), backup])?;
		}
		Ok(())
	}

	/// Moves the WAL segments the manifest counts as flushed into the backup. After a tail was
	/// converted `log_number` is past all of them, and a segment below it that stayed would make
	/// the manifest claim more WAL than there is (`validate_wal_log_number`).
	fn move_flushed_wal_segments(&mut self) -> Result<()> {
		let log_number = self.manifest().log_number;
		let wal_dir = self.layout().wal_dir();
		if !wal_dir.is_dir() {
			return Ok(());
		}
		let backup_wal = self.backup_dir().join("wal");
		for segment in wal_segments(&wal_dir).map_err(from_reader)? {
			if segment.id < log_number {
				let name = segment.path.file_name().expect("a segment has a name");
				fs::rename(&segment.path, backup_wal.join(name))?;
				self.sync_dirs(&[wal_dir.clone(), backup_wal.clone()])?;
				self.step("WAL segment moved")?;
			}
		}
		Ok(())
	}

	fn write_manifest(&mut self, manifest_path: &Path) -> Result<()> {
		let bytes = self.manifest().encode()?;
		replace_file_content(manifest_path, &bytes)?;
		self.step("manifest replaced")
	}

	/// Converts one table: write it, check it, name it in the manifest, move the old one away.
	fn convert_table(&mut self, level: usize, position: usize, source_id: u64) -> Result<()> {
		let new_id = self.manifest().next_table_id;
		let (entries, duplicates) = self.write_converted_table(source_id, new_id, level as u8)?;
		self.step("table written")?;

		let manifest = self.manifest();
		manifest.level_data[level][position] = new_id;
		manifest.next_table_id = new_id + 1;
		manifest.version = MANIFEST_FORMAT_VERSION;
		let manifest_path = self.opts.manifest_file_path(0);
		self.write_manifest(&manifest_path)?;

		let from = self.layout().sstable_path(source_id);
		let to = self.backup_dir().join("sstables").join(format!("{source_id:020}.sst"));
		fs::rename(&from, &to)?;
		self.sync_dirs(&[self.opts.sstable_dir(), self.backup_dir().join("sstables")])?;
		self.step("old table moved")?;

		self.report.tables_converted += 1;
		self.report.entries_converted += entries;
		self.report.duplicates_dropped += duplicates;
		Ok(())
	}

	/// Writes `source_id` as `new_id` in the current layout and checks the result against what was
	/// read. Returns the entries written and the duplicates dropped.
	fn write_converted_table(
		&mut self,
		source_id: u64,
		new_id: u64,
		level: u8,
	) -> Result<(u64, u64)> {
		let source =
			TableReader::open(&self.layout().sstable_path(source_id)).map_err(from_reader)?;

		let temp = self.temp_path(new_id);
		let output = fs::File::create(&temp)?;
		let sync_handle = output.try_clone()?;
		let mut writer = TableWriter::new(output, new_id, Arc::clone(self.opts), level);

		let mut expected = Digest::new();
		let mut duplicates = 0u64;
		let mut previous: Option<(Vec<u8>, u64)> = None;
		for entry in source.entries().map_err(from_reader)? {
			let entry = entry.map_err(from_reader)?;
			// Version 0.20 could write two entries for one key and sequence number; the table
			// writer needs strictly ascending keys, and the first is the one a read finds.
			if previous.as_ref().is_some_and(|(k, s)| *k == entry.user_key && *s == entry.seq) {
				duplicates += 1;
				continue;
			}
			let kind = internal_kind(entry.kind);
			let converted = convert_value(entry.kind, entry.timestamp, &entry.value)?;
			expected.add(
				&entry.user_key,
				entry.seq,
				kind,
				converted.timestamp,
				converted.pointer,
				&converted.payload,
			);
			previous = Some((entry.user_key.clone(), entry.seq));
			writer.add(InternalKey::new(entry.user_key, entry.seq, kind), &converted.value)?;
		}
		let size = writer.finish()?;
		sync_handle.sync_all()?;
		self.step("temporary table synced")?;

		let final_path = self.opts.sstable_file_path(new_id);
		fs::rename(&temp, &final_path)?;
		self.sync_dirs(&[self.opts.sstable_dir()])?;

		// Read it back through the engine's own reader and compare.
		let written: Arc<dyn File> = Arc::new(fs::File::open(&final_path)?);
		let converted = Table::new(new_id, Arc::clone(self.opts), written, size as u64)
			.map_err(|e| failed(format!("the converted table {new_id} does not open: {e}")))?;
		let mut actual = Digest::new();
		for_each_entry(&converted, &mut |entry| {
			let (timestamp, pointer, payload) = if entry.value.is_empty() {
				(None, false, Vec::new())
			} else {
				let location = ValueLocation::decode(&entry.value)?;
				(location.timestamp, location.is_value_pointer(), location.value)
			};
			actual.add(&entry.user_key, entry.seq, entry.kind, timestamp, pointer, &payload);
			Ok(())
		})?;
		if expected.finish() != actual.finish() {
			let _ = fs::remove_file(&final_path);
			return Err(failed(format!(
				"the converted copy of table {source_id} does not match it ({:?} against {:?}); nothing was changed",
				actual.finish(),
				expected.finish()
			)));
		}
		Ok((expected.finish().0, duplicates))
	}

	/// Reads the WAL that was not flushed, with the rules of the version that wrote it: a torn
	/// end of the last segment is the end of the log.
	fn read_wal_tail(&self, first_log: u64) -> Result<Option<WalTail>> {
		let mut segments: Vec<WalSegment> = wal_segments(&self.layout().wal_dir())
			.map_err(from_reader)?
			.into_iter()
			.filter(|segment| segment.id >= first_log)
			.collect();
		segments.sort_by_key(|segment| segment.id);
		let Some(last_segment) = segments.last().map(|segment| segment.id) else {
			return Ok(None);
		};

		let mut entries = BTreeMap::new();
		let mut max_seq = 0;
		for segment in &segments {
			let read = read_wal_segment(&segment.path).map_err(from_reader)?;
			for batch in &read.batches {
				// v0.20.0 and v0.20.1 wrote a large value to the value log when the batch was
				// committed and logged its pointer, as the value of the entry; later versions
				// separate values when they flush. The value of every entry is an encoded value
				// location either way, so a pointer converts like any other value.
				for (i, entry) in batch.entries.iter().enumerate() {
					let seq = batch.starting_seq + i as u64;
					if entry.kind == Kind::RangeDelete {
						tracing::warn!("the WAL holds range deletions, which v0.x never logged; leaving it to be replayed as it is");
						return Ok(None);
					}
					let legacy = entry.value.as_deref().unwrap_or(&[]);
					let converted = convert_value(entry.kind, entry.timestamp, legacy)?;
					max_seq = max_seq.max(seq);
					entries.insert(
						(entry.key.clone(), Reverse(seq)),
						(internal_kind(entry.kind), converted),
					);
				}
			}
			// v0.x repaired a damaged segment by cutting it at the damage and went on with the
			// next, unless it was set to refuse any corruption. The records before the damage
			// are in `read.batches`, and the reader says no more of the segment.
			if let Some(damage) = read.damage {
				if self.opts.wal_recovery_mode == WalRecoveryMode::AbsoluteConsistency {
					return Err(Error::WalCorruption {
						segment_id: segment.id as usize,
						offset: 0,
						message: format!("Corrupted WAL record: {damage}"),
					});
				}
				tracing::warn!(
					"WAL segment {} is damaged; its records after the damage are dropped, as v0.x repaired it: {damage}",
					segment.id
				);
			}
		}
		if entries.is_empty() {
			return Ok(None);
		}
		Ok(Some(WalTail {
			entries,
			segments,
			last_segment,
			max_seq,
		}))
	}

	/// Writes the WAL tail as a new level-0 table and moves the segments away.
	fn convert_wal_tail(&mut self) -> Result<()> {
		let first_log = self.manifest().log_number;
		let Some(tail) = self.read_wal_tail(first_log)? else {
			return Ok(());
		};

		let new_id = self.manifest().next_table_id;
		let temp = self.temp_path(new_id);
		let output = fs::File::create(&temp)?;
		let sync_handle = output.try_clone()?;
		let mut writer = TableWriter::new(output, new_id, Arc::clone(self.opts), 0);
		for ((user_key, Reverse(seq)), (kind, converted)) in &tail.entries {
			writer.add(InternalKey::new(user_key.clone(), *seq, *kind), &converted.value)?;
		}
		let size = writer.finish()?;
		sync_handle.sync_all()?;
		self.step("temporary WAL table synced")?;
		let final_path = self.opts.sstable_file_path(new_id);
		fs::rename(&temp, &final_path)?;
		self.sync_dirs(&[self.opts.sstable_dir()])?;
		Table::new(
			new_id,
			Arc::clone(self.opts),
			Arc::new(fs::File::open(&final_path)?),
			size as u64,
		)
		.map_err(|e| failed(format!("the table of the WAL tail does not open: {e}")))?;
		self.step("WAL table written")?;

		let manifest = self.manifest();
		manifest.level_data[0].insert(0, new_id);
		manifest.next_table_id = new_id + 1;
		manifest.last_sequence = manifest.last_sequence.max(tail.max_seq);
		manifest.log_number = manifest.log_number.max(tail.last_segment + 1);
		manifest.version = MANIFEST_FORMAT_VERSION;
		let manifest_path = self.opts.manifest_file_path(0);
		self.write_manifest(&manifest_path)?;

		// The segments are moved once the manifest has passed them: `move_flushed_wal_segments`.
		self.report.wal_entries_converted += tail.entries.len() as u64;
		self.report.wal_segments_converted += tail.segments.len();
		Ok(())
	}
}

/// Refuses a checkpoint whose tables are in LSMV1, before a restore deletes anything.
///
/// A restore replaces the files of the running database and the tables are only read when it is
/// opened again, so a checkpoint taken by SurrealKV v0.x would otherwise be discovered after the
/// live files were gone. Open a copy of the checkpoint as a database with `Tree::new` to migrate
/// it, then take a new checkpoint.
///
/// Only a table that is positively in LSMV1 is refused here. A checkpoint that cannot
/// be read at all, with a manifest that does not decode or a table that is missing or damaged,
/// is left to the validation that follows, which says what is wrong with it.
pub(crate) fn refuse_legacy_checkpoint(checkpoint: &Path, opts: &Options) -> Result<()> {
	let manifest_file = opts.manifest_file_path(0);
	let Some(name) = manifest_file.file_name() else {
		return Ok(());
	};
	let Ok(Some(manifest)) = read_manifest(&checkpoint.join("manifest").join(name)) else {
		return Ok(());
	};
	for id in manifest.level_data.iter().flatten() {
		let path = checkpoint.join("sstables").join(format!("{id:020}.sst"));
		let Ok(file) = fs::File::open(&path) else {
			continue;
		};
		let Ok(metadata) = file.metadata() else {
			continue;
		};
		// Asked of the same gate that refuses to open a table, so that what is refused here is
		// exactly what the next open would refuse.
		let refused = refuse_legacy_table(*id, opts, Arc::new(file), metadata.len());
		if matches!(refused, Err(Error::LegacyFormat(_))) {
			return Err(Error::LegacyFormat(format!(
				"the checkpoint at {} holds tables written by SurrealKV v0.x. Nothing was changed. \
				 Open a copy of the checkpoint directory as a database with Tree::new to migrate it, \
				 then take a new checkpoint",
				checkpoint.display()
			)));
		}
	}
	Ok(())
}
