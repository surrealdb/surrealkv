//! Diagnostics, inspection, and integrity scrubbing utilities for SurrealKV.

#![allow(
	clippy::disallowed_methods,
	reason = "std-path diagnostics for a database directory on a disk, run by a tool that holds no file system handle; none of it is on the path that opens or writes a database"
)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub use crate::describe::{describe_file, is_encrypted, CryptoSummary, FileDescription, FileState};
// The shape of the encryption status is not settled, and it is
// made of types that are hidden: it is hidden from the documentation too.
#[doc(hidden)]
pub use crate::encryption_status::{EncryptionStatus, FileTotals, KeyUsage};
use crate::error::{EncryptionError, Error, Result};
use crate::levels::LevelManifest;
use crate::sstable::index_block::IndexIterator;
use crate::sstable::table::{IndexType, Table, TABLE_FULL_FOOTER_LENGTH};
use crate::sstable::verify::verify_blocks;
pub use crate::sstable::verify::BlockCounts;
use crate::tde::EncryptionContext;
use crate::vfs::File;
use crate::wal::sealed_recovery::{decode_segment, SegmentEnd};
use crate::wal::WalContext;
use crate::{LSMIterator, Options};

/// General database storage inspection summary.
#[derive(Debug, Clone)]
pub struct DbInfo {
	pub path: PathBuf,
	/// What the header of the manifest says about the encryption of the database, if it is
	/// encrypted: its suite, the key that wrote the manifest and its identity.
	pub encryption: Option<CryptoSummary>,
	pub manifest_version: u16,
	pub next_table_id: u64,
	pub log_number: u64,
	pub last_sequence: u64,
	pub total_tables: usize,
	pub total_sst_bytes: u64,
	pub total_wal_bytes: u64,
	pub total_vlog_bytes: u64,
	pub levels: Vec<LevelSummary>,
	pub is_locked: bool,
	pub lock_pid: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct LevelSummary {
	pub level: usize,
	pub table_count: usize,
	pub total_bytes: u64,
}

/// Detailed manifest state.
#[derive(Debug, Clone)]
pub struct ManifestInfo {
	pub version: u16,
	pub next_table_id: u64,
	pub log_number: u64,
	pub last_sequence: u64,
	pub levels: Vec<LevelDetail>,
	pub snapshots: Vec<SnapshotSummary>,
}

#[derive(Debug, Clone)]
pub struct LevelDetail {
	pub level: usize,
	pub tables: Vec<TableSummary>,
}

#[derive(Debug, Clone)]
pub struct TableSummary {
	pub id: u64,
	pub file_size: u64,
	pub smallest_key: Vec<u8>,
	pub largest_key: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct SnapshotSummary {
	pub id: u64,
	pub sequence: u64,
	pub timestamp: u64,
}

/// Detailed SSTable inspection information.
#[derive(Debug, Clone)]
pub struct SstInfo {
	pub path: PathBuf,
	/// The id of the table, from the name of its file, `{id:020}.sst`, if the name is one.
	pub table_id: Option<u64>,
	/// What the header of the table says about its encryption, if its blocks are sealed.
	pub encryption: Option<CryptoSummary>,
	pub file_size: u64,
	pub format_version: u8,
	/// The checksum type that the footer of the table says: 1 for plain blocks, 2 for sealed ones.
	pub checksum_type: u8,
	pub total_entries: u64,
	pub raw_key_size: u64,
	pub raw_value_size: u64,
	pub data_blocks_count: u64,
	pub index_blocks_count: u64,
	pub filter_size: u64,
	pub created_at: u128,
	pub keys: Option<Vec<SstKeyEntry>>,
	/// The number of blocks of every kind that were read from the file and checked, when a
	/// verification was asked for: a sealed table authenticates each of them.
	pub verified_blocks: Option<usize>,
	/// The same, by the kind of block.
	pub verified: Option<BlockCounts>,
}

#[derive(Debug, Clone)]
pub struct SstKeyEntry {
	pub user_key: Vec<u8>,
	pub seq_num: u64,
	pub kind: String,
	pub value_preview: Vec<u8>,
}

/// WAL segments summary.
#[derive(Debug, Clone)]
pub struct WalInfo {
	pub segment_files: Vec<WalSegmentSummary>,
	pub total_bytes: u64,
}

#[derive(Debug, Clone)]
pub struct WalSegmentSummary {
	pub file_name: String,
	pub file_size: u64,
	/// What the header of the segment says about its encryption, if it is sealed.
	pub encryption: Option<CryptoSummary>,
	/// The number of batches in the segment, counted by decoding it. Only for a sealed segment
	/// that was read with its key: a plaintext one is not decoded.
	pub batches: Option<usize>,
	/// Whether a sealed segment that was decoded ends in a write that was cut short.
	pub torn: bool,
}

/// Comprehensive database integrity scrub report.
#[derive(Debug, Clone, Default)]
pub struct ScrubReport {
	/// The number of table files that were looked at.
	pub total_tables: usize,
	/// The number of tables that were read to the end and found whole.
	pub verified_tables: usize,
	/// The number of blocks, of every kind, that were read and found whole. It counts blocks and
	/// not files: a table of a few blocks adds a few.
	pub total_blocks: usize,
	/// The same by kind of block.
	pub blocks: BlockCounts,
	/// The number of tables in which a block was found damaged: it does not match its checksum,
	/// or, in a table that is sealed and was read with its key, it does not authenticate. It
	/// counts the tables, and each of them stops at its first block that fails.
	pub corrupt_blocks: usize,
	/// The number of tables that were not verified, which is not the same as damaged: a table that
	/// is sealed and was met without a key, or with one that does not fit, or that belongs to
	/// another database, or is of a later format. Nothing is said of what is in them.
	pub not_verified_tables: usize,
	/// What was found, a line for each table that is damaged or was not verified.
	pub errors: Vec<String>,
}

/// Says from the headers of the files of the database at `db_path` whether it is encrypted, which
/// suite and database it has, and which keys cover how many files and bytes of each kind, without
/// a key and without reading a block. Whatever is not as it should be is listed in the warnings: a
/// file in the clear in an encrypted database, a file of another database or suite, a header that
/// cannot be read. See [`EncryptionStatus`], which says when a key may be retired, and which is
/// what `Tree::encryption_status` gives for a database that is open, with the state of each key.
///
/// The key states and the active key are not known to a report that is made from files alone, and
/// are left out.
#[doc(hidden)]
pub fn encryption_report<P: AsRef<Path>>(db_path: P) -> Result<EncryptionStatus> {
	crate::encryption_status::scan_database(db_path.as_ref(), None, None)
}

/// Refuses options that name a key where this build does not take one, as opening a database
/// with them would: an inspection that decrypts is held to the same guard as a database that
/// writes, so a build that cannot make an encrypted database does not read one either.
fn check_encryption_options(options: &Options) -> Result<()> {
	if options.key_manager.is_some() || options.encryption_context.is_some() {
		options.validate()?;
	}
	Ok(())
}

/// Whether the options give the inspection a key: a key manager, or a context that the caller
/// resolved for the database it expects.
fn holds_a_key(options: &Options) -> bool {
	options.key_manager.is_some() || options.encryption_context.is_some()
}

/// The options to read the files of the database in `db_dir` with. With a key, the identity and
/// the suite of the database are the ones its manifest names, which is opened to check the key:
/// a key that does not fit, or a manifest that is not encrypted, is the error of the call and
/// nothing has been read of any other file. Reads only.
///
/// A directory that has no manifest has no identity to take, and its options are returned as
/// they were given: what is read from it then takes its identity from the file it reads.
fn options_for_database(db_dir: &Path, options: &Options) -> Result<Arc<Options>> {
	check_encryption_options(options)?;
	let mut opts = options.clone();
	opts.path = db_dir.to_path_buf();
	let opts = Arc::new(opts);
	if !holds_a_key(&opts) || !opts.manifest_file_path(0).exists() {
		return Ok(opts);
	}
	crate::levels::resolve_encryption(opts)
}

/// The table id that the name of a file gives: `{id:020}.sst`, twenty digits and the extension.
fn table_id_of(path: &Path) -> Option<u64> {
	let name = path.file_name()?.to_str()?;
	let digits = name.strip_suffix(".sst")?;
	if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
		return None;
	}
	digits.parse().ok()
}

/// The number of a segment of the log that the name of a file gives: `{id:020}.wal`.
fn segment_id_of(name: &str) -> Option<u64> {
	let digits = name.strip_suffix(".wal")?;
	if digits.len() != 20 || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
		return None;
	}
	digits.parse().ok()
}

/// The database directory and the manifest that a path names: the directory itself, or a file
/// of the manifest directory, which is two levels down.
fn database_directory(path: &Path) -> PathBuf {
	if path.is_dir() {
		path.to_path_buf()
	} else {
		path.parent().and_then(|parent| parent.parent()).unwrap_or(path).to_path_buf()
	}
}

/// Inspects a database directory and returns overall storage health.
///
/// A database that is encrypted is refused with `Error::Encryption(NotConfigured)`: nothing here
/// has a key. See [`inspect_db_with_options`].
pub fn inspect_db<P: AsRef<Path>>(db_path: P) -> Result<DbInfo> {
	inspect_db_with_options(db_path, &Options::default())
}

/// Inspects a database directory as [`inspect_db`] does, with the key manager of `options`, if it
/// has one, to read a database that is encrypted. The path of `options` is not used: the argument
/// says which database is inspected.
///
/// With a key the database is the one that its manifest names, and the key must open it
/// (`WrongKey`, `UnknownKeyId`, `KeyRetired`); a database that is not encrypted is refused as
/// `NotEncrypted`, since encryption is never added to a database that exists. Nothing is written,
/// and in a build that does not take a key manager at all the call is refused with
/// `Error::InvalidArgument`, as the opening of a database is.
pub fn inspect_db_with_options<P: AsRef<Path>>(db_path: P, options: &Options) -> Result<DbInfo> {
	let path = db_path.as_ref().to_path_buf();
	let opts = options_for_database(&path, options)?;

	// Check lock status
	let lock_path = opts.path.join("LOCK");
	#[cfg_attr(target_arch = "wasm32", allow(unused_mut))]
	let mut is_locked = false;
	let mut lock_pid = None;

	if lock_path.exists() {
		if let Ok(content) = fs::read_to_string(&lock_path) {
			if let Ok(pid) = content.trim().parse::<u32>() {
				lock_pid = Some(pid);
			}
		}
		// On non-wasm32, check if lock is actively held
		#[cfg(not(target_arch = "wasm32"))]
		{
			if let Ok(f) = fs::File::open(&lock_path) {
				if crate::lockfile::try_lock_exclusive(&f).is_err() {
					is_locked = true;
				} else {
					let _ = crate::lockfile::unlock(&f);
				}
			}
		}
	}

	// Calculate directory sizes
	let mut total_sst_bytes = 0u64;
	let mut total_wal_bytes = 0u64;
	let mut total_vlog_bytes = 0u64;

	if let Ok(entries) = fs::read_dir(opts.sstable_dir()) {
		for e in entries.flatten() {
			if e.path().extension().is_some_and(|ext| ext == "sst") {
				if let Ok(meta) = e.metadata() {
					total_sst_bytes += meta.len();
				}
			}
		}
	}

	if let Ok(entries) = fs::read_dir(opts.wal_dir()) {
		for e in entries.flatten() {
			if let Ok(meta) = e.metadata() {
				total_wal_bytes += meta.len();
			}
		}
	}

	if let Ok(entries) = fs::read_dir(opts.vlog_dir()) {
		for e in entries.flatten() {
			if let Ok(meta) = e.metadata() {
				total_vlog_bytes += meta.len();
			}
		}
	}

	// Read manifest
	let manifest_path = opts.manifest_file_path(0);
	if !manifest_path.exists() {
		return Ok(DbInfo {
			path,
			encryption: None,
			manifest_version: 0,
			next_table_id: 0,
			log_number: 0,
			last_sequence: 0,
			total_tables: 0,
			total_sst_bytes,
			total_wal_bytes,
			total_vlog_bytes,
			levels: Vec::new(),
			is_locked,
			lock_pid,
		});
	}

	let encryption = describe_file(&manifest_path)?.crypto().copied();
	let manifest = LevelManifest::load_from_file(&manifest_path, opts)?;
	let mut total_tables = 0;
	let mut levels = Vec::new();

	for (lvl_idx, lvl) in manifest.levels.get_levels().iter().enumerate() {
		let table_count = lvl.tables.len();
		total_tables += table_count;
		let total_bytes: u64 = lvl.tables.iter().map(|t| t.file_size).sum();
		levels.push(LevelSummary {
			level: lvl_idx,
			table_count,
			total_bytes,
		});
	}

	Ok(DbInfo {
		path,
		encryption,
		manifest_version: manifest.manifest_format_version,
		next_table_id: manifest.next_table_id.load(std::sync::atomic::Ordering::Relaxed),
		log_number: manifest.log_number,
		last_sequence: manifest.last_sequence,
		total_tables,
		total_sst_bytes,
		total_wal_bytes,
		total_vlog_bytes,
		levels,
		is_locked,
		lock_pid,
	})
}

/// Dumps detailed manifest state.
///
/// An encrypted manifest is refused with `Error::Encryption(NotConfigured)`: nothing here has a
/// key. See [`dump_manifest_with_options`].
pub fn dump_manifest<P: AsRef<Path>>(path: P) -> Result<ManifestInfo> {
	dump_manifest_with_options(path, &Options::default())
}

/// Dumps the manifest as [`dump_manifest`] does, opening it with the key manager of `options`, if
/// it has one, when the database is encrypted. The path of `options` is not used.
///
/// The manifest is opened with the key (`WrongKey`, `UnknownKeyId` and `KeyRetired` otherwise),
/// and the tables that it lists are opened with the identity that it names, so a table of another
/// database is `DbIdMismatch`. Nothing is written. In a build that does not take a key manager
/// the call is refused with `Error::InvalidArgument`.
pub fn dump_manifest_with_options<P: AsRef<Path>>(
	path: P,
	options: &Options,
) -> Result<ManifestInfo> {
	let opts = options_for_database(&database_directory(path.as_ref()), options)?;
	let manifest_path = opts.manifest_file_path(0);
	let manifest = LevelManifest::load_from_file(&manifest_path, opts)?;

	let mut levels = Vec::new();
	for (lvl_idx, lvl) in manifest.levels.get_levels().iter().enumerate() {
		let mut tables = Vec::new();
		for t in &lvl.tables {
			let smallest =
				t.meta.smallest_point.as_ref().map(|k| k.user_key.clone()).unwrap_or_default();
			let largest =
				t.meta.largest_point.as_ref().map(|k| k.user_key.clone()).unwrap_or_default();
			tables.push(TableSummary {
				id: t.id,
				file_size: t.file_size,
				smallest_key: smallest,
				largest_key: largest,
			});
		}
		levels.push(LevelDetail {
			level: lvl_idx,
			tables,
		});
	}

	let snapshots = manifest
		.snapshots
		.iter()
		.map(|s| SnapshotSummary {
			id: 0,
			sequence: s.seq_num,
			timestamp: s.created_at as u64,
		})
		.collect();

	Ok(ManifestInfo {
		version: manifest.manifest_format_version,
		next_table_id: manifest.next_table_id.load(std::sync::atomic::Ordering::Relaxed),
		log_number: manifest.log_number,
		last_sequence: manifest.last_sequence,
		levels,
		snapshots,
	})
}

/// Dumps deep inspection information from an SSTable.
///
/// A table whose blocks are sealed cannot be read without the key of its database, and nothing
/// here takes one: it is refused with `Error::Encryption`, before any of its blocks is read. See
/// [`dump_sst_with_options`].
pub fn dump_sst<P: AsRef<Path>>(
	path: P,
	dump_keys: bool,
	verify_checksum: bool,
) -> Result<SstInfo> {
	dump_sst_with_options(path, dump_keys, verify_checksum, &Options::default())
}

/// The options to read the table described by `described` with. Without a key they are the
/// options as they were given. With one, a table that is not encrypted is refused
/// (`NotEncrypted`, as a database that is encrypted refuses a plaintext file), and a sealed one is
/// read with the context that the options carry, if the caller gave one for the database it
/// expects, and otherwise with a context made from what the file says about itself: the database
/// and suite in its own header, since a table that is looked at alone belongs to no manifest. A
/// header that cannot be read is the error of the call.
fn options_for_table(options: &Options, described: &FileDescription) -> Result<Options> {
	let mut opts = options.clone();
	if !holds_a_key(options) {
		return Ok(opts);
	}
	match &described.state {
		FileState::Sealed(summary) if opts.encryption_context.is_none() => {
			if let Some(key_manager) = opts.key_manager.clone() {
				let context = EncryptionContext::new(key_manager, summary.db_id, summary.suite);
				opts = opts.with_encryption_context(Arc::new(context));
			}
		}
		FileState::Sealed(_) | FileState::Unfinished => {}
		FileState::Damaged(e) => return Err(e.clone().into()),
		FileState::Plaintext => return Err(EncryptionError::NotEncrypted.into()),
	}
	Ok(opts)
}

/// Dumps deep inspection information from an SSTable as [`dump_sst`] does, with the key manager
/// of `options` to read a table whose blocks are sealed.
///
/// The table is the one that its file name says, `{id:020}.sst`: a sealed table is opened as that
/// table and refuses a file that was renamed (`FileIdMismatch`), and a sealed table whose name is
/// not one is refused, since nothing says which table it is meant to be. The checksum type is the
/// one in the footer of the file. A table that is alone belongs to no manifest, so its database
/// and suite are those of its own header, unless `options` carry a context that was resolved
/// for the database that the caller expects, which then must be that of the file
/// (`DbIdMismatch`). The key must open the file (`WrongKey`, `UnknownKeyId`, `KeyRetired`), and a
/// block that does not authenticate is `AuthenticationFailed`. A table that is not encrypted is
/// refused when a key is given. In a build that does not take a key manager the call is refused
/// with `Error::InvalidArgument`.
pub fn dump_sst_with_options<P: AsRef<Path>>(
	path: P,
	dump_keys: bool,
	verify_checksum: bool,
	options: &Options,
) -> Result<SstInfo> {
	let path = path.as_ref().to_path_buf();
	check_encryption_options(options)?;
	let sys_file = fs::File::open(&path)?;
	let file_size = sys_file.metadata()?.len();

	if file_size < TABLE_FULL_FOOTER_LENGTH as u64 {
		return Err(Error::Corruption("SSTable file is too small for footer".into()));
	}

	let described = describe_file(&path)?;
	let table_id = table_id_of(&path);
	let mut opts = options_for_table(options, &described)?;
	// What a sealed table is bound to is its own id, which only its name says here. Without a
	// key it is refused by its footer before that matters.
	if holds_a_key(options) && described.is_encrypted() && table_id.is_none() {
		return Err(Error::InvalidArgument(format!(
			"cannot tell which table {} is: a table that is encrypted is read as the table that \
			 its file name says, {{id:020}}.sst",
			path.display()
		)));
	}
	opts.path = path.parent().and_then(|p| p.parent()).unwrap_or(&path).to_path_buf();

	// The key that the header names is made available before the table is opened, which a key
	// provider may need time for, as the opening of a database does for each table it lists. Only
	// for a header that is this database's and this table's, so that a table that is not is refused
	// as what it is, in the order that opening it decides.
	if let (Some(context), FileState::Sealed(summary), Some(id)) =
		(opts.encryption_context(), &described.state, table_id)
	{
		if summary.db_id == context.db_id() && summary.file_id == id {
			context.prepare(&[summary.key_id])?;
		}
	}

	let file: Arc<dyn File> = Arc::new(sys_file);
	let table = Table::new(table_id.unwrap_or(0), Arc::new(opts), Arc::clone(&file), file_size)?;
	let footer = table.source.read_footer()?;

	let props = &table.meta.properties;
	let total_entries = props.num_entries;
	let raw_key_size = props.raw_key_size;
	let raw_value_size = props.raw_value_size;
	let data_blocks_count = props.num_data_blocks;
	let index_blocks_count = props.index_partitions;
	let filter_size = props.filter_size;
	let created_at = props.created_at;

	// Every block of every kind, read from the file and checked, which is what a verification
	// is: the blocks that a lookup or a dump of the keys reads are the data blocks, and a lookup
	// can be answered from the cache.
	let verified = if verify_checksum {
		Some(verify_blocks(&table)?)
	} else {
		None
	};

	let mut keys = None;
	if dump_keys {
		let mut found = Vec::new();
		let IndexType::Partitioned(ref partitioned) = table.index_block;
		let mut index_iter = IndexIterator::new(partitioned);
		// A table of range deletions alone has no data block and an empty index.
		if !table.holds_only_range_deletions() {
			index_iter.seek_to_first()?;
		}

		while index_iter.valid() {
			let handle = index_iter.block_handle()?;
			let block = table.read_block(&handle)?;

			let mut b_iter = block.iter()?;
			b_iter.seek_first()?;
			while b_iter.valid() {
				let ikey = b_iter.key();
				let val = b_iter.value_encoded()?;
				found.push(SstKeyEntry {
					user_key: ikey.user_key().to_vec(),
					seq_num: ikey.seq_num(),
					kind: format!("{:?}", ikey.kind()),
					value_preview: val[..val.len().min(32)].to_vec(),
				});
				b_iter.next()?;
			}

			index_iter.next()?;
		}
		keys = Some(found);
	}

	Ok(SstInfo {
		path,
		table_id,
		encryption: described.crypto().copied(),
		file_size,
		format_version: props.table_format as u8,
		checksum_type: footer.checksum as u8,
		total_entries,
		raw_key_size,
		raw_value_size,
		data_blocks_count,
		index_blocks_count,
		filter_size,
		created_at,
		keys,
		verified_blocks: verified.as_ref().map(BlockCounts::total),
		verified,
	})
}

/// Dumps summary of WAL segments.
///
/// A log that has a sealed segment is refused with `Error::Encryption(NotConfigured)`: nothing
/// here has a key. See [`dump_wal_with_options`].
pub fn dump_wal<P: AsRef<Path>>(path: P) -> Result<WalInfo> {
	dump_wal_with_options(path, &Options::default())
}

/// Dumps the segments of a log as [`dump_wal`] does, and with the key manager of `options` reads
/// the segments that are sealed: each is decoded, which authenticates every batch in it, and the
/// number of batches in it is reported.
///
/// The path is a database directory, its `wal` directory, or a file in it. The database of a
/// sealed log is the one that its manifest names, if the path is a database directory or the
/// `wal` directory of one that has a manifest, and otherwise the one that the first sealed
/// segment names in its own header, so that a segment of another database is `DbIdMismatch`. The
/// key must open each segment (`WrongKey`, `UnknownKeyId`, `KeyRetired`), and a batch that does not
/// authenticate is `AuthenticationFailed`. A segment that ends in a write that was cut short is
/// reported as torn, and nothing is repaired, moved or written. With a key, a log with no sealed
/// segment is refused as not encrypted (`NotEncrypted`), and one that mixes sealed and plaintext
/// segments as `PlaintextInEncryptedDb`. In a build that does not take a key manager the call is
/// refused with `Error::InvalidArgument`.
pub fn dump_wal_with_options<P: AsRef<Path>>(path: P, options: &Options) -> Result<WalInfo> {
	check_encryption_options(options)?;
	let p = path.as_ref();
	let wal_dir = if p.is_dir() && p.join("wal").exists() {
		p.join("wal")
	} else if p.is_dir() {
		p.to_path_buf()
	} else {
		p.parent().unwrap_or(p).to_path_buf()
	};

	let mut segments = Vec::new();
	if let Ok(entries) = fs::read_dir(&wal_dir) {
		for e in entries.flatten() {
			let name = e.file_name().to_string_lossy().to_string();
			if name.ends_with(".wal") {
				if let Ok(meta) = e.metadata() {
					segments.push((name, e.path(), meta.len()));
				}
			}
		}
	}
	segments.sort_by(|a, b| a.0.cmp(&b.0));

	let mut described = Vec::with_capacity(segments.len());
	for (_, segment_path, _) in &segments {
		described.push(describe_file(segment_path)?);
	}
	let sealed = described.iter().filter(|segment| segment.is_encrypted()).count();
	let plain = described.iter().filter(|segment| segment.state == FileState::Plaintext).count();

	// Nothing is read of what cannot be opened: a log that is sealed needs a key before anything
	// is said of it, as the database does. A segment with nothing in it, which is what a crash
	// leaves when a segment was made and not yet written, is of neither kind.
	if sealed > 0 && !holds_a_key(options) {
		return Err(EncryptionError::NotConfigured.into());
	}
	if holds_a_key(options) {
		if sealed == 0 && plain > 0 {
			return Err(EncryptionError::NotEncrypted.into());
		}
		if sealed > 0 && plain > 0 {
			return Err(EncryptionError::PlaintextInEncryptedDb.into());
		}
		if let Some(FileState::Damaged(e)) = described
			.iter()
			.map(|segment| &segment.state)
			.find(|s| matches!(s, FileState::Damaged(_)))
		{
			return Err(e.clone().into());
		}
	}

	let wal = if sealed > 0 {
		Some(wal_context(&wal_dir, options, &described)?)
	} else {
		None
	};

	let mut total_bytes = 0;
	let mut segment_files = Vec::with_capacity(segments.len());
	for ((name, segment_path, len), description) in segments.into_iter().zip(&described) {
		total_bytes += len;
		let mut batches = None;
		let mut torn = false;
		if let Some(wal) = &wal {
			let id = segment_id_of(&name).ok_or_else(|| {
				Error::InvalidArgument(format!(
					"cannot tell which segment {name} is: a segment that is encrypted is read as \
					 the segment that its file name says, {{id:020}}.wal"
				))
			})?;
			let decoded = decode_segment(wal, &segment_path, id)?;
			batches = Some(decoded.batches.len());
			torn = matches!(decoded.end, SegmentEnd::Torn { .. });
		}
		segment_files.push(WalSegmentSummary {
			file_name: name,
			file_size: len,
			encryption: description.crypto().copied(),
			batches,
			torn,
		});
	}

	Ok(WalInfo {
		segment_files,
		total_bytes,
	})
}

/// The context to decode the sealed segments in `wal_dir` with: the one that the options carry if
/// the caller resolved one, the database that the manifest next to the directory names if there
/// is one, and otherwise the database and suite of the first sealed segment's own header.
fn wal_context(
	wal_dir: &Path,
	options: &Options,
	described: &[FileDescription],
) -> Result<WalContext> {
	let database = wal_dir.parent().map(Path::to_path_buf);
	let opts = match database {
		Some(db_dir) if holds_a_key(options) && db_dir.join("manifest").is_dir() => {
			options_for_database(&db_dir, options)?
		}
		_ => Arc::new(options.clone()),
	};
	if opts.encryption_context().is_some() {
		return WalContext::for_options(&opts);
	}
	let (Some(key_manager), Some(first)) =
		(opts.key_manager.clone(), described.iter().find_map(|segment| segment.crypto()))
	else {
		return Err(EncryptionError::MalformedHeader.into());
	};
	let context = EncryptionContext::new(key_manager, first.db_id, first.suite);
	let opts = (*opts).clone().with_encryption_context(Arc::new(context));
	WalContext::for_options(&opts)
}

/// What is said of a table that was not verified, from what stopped it.
fn not_verified(error: &EncryptionError) -> Option<&'static str> {
	Some(match error {
		EncryptionError::NotConfigured => "it cannot be read without a key",
		EncryptionError::WrongKey | EncryptionError::UnknownKeyId | EncryptionError::KeyRetired => {
			"the key that was given does not open it"
		}
		EncryptionError::KeyProvider(_) => "the key provider could not give the key",
		EncryptionError::UnsupportedVersion | EncryptionError::UnsupportedSuite => {
			"it is written in a newer format than this build reads"
		}
		EncryptionError::DbIdMismatch => "it belongs to another database",
		EncryptionError::FileIdMismatch => "it is not the table that its file name says",
		EncryptionError::PlaintextInEncryptedDb | EncryptionError::NotEncrypted => {
			"it is not encrypted, and the database is"
		}
		// Not about a file at all: damage, as everything that is not named here is.
		EncryptionError::AuthenticationFailed
		| EncryptionError::MalformedHeader
		| EncryptionError::PositionReuse
		| EncryptionError::InvalidInput(_)
		| EncryptionError::SuiteMismatch => return None,
	})
}

/// Comprehensively verifies CRC32 checksums of all active SSTables in a database.
///
/// Every block of every table is read, of all six kinds, and a table whose blocks are sealed is
/// read without a key as far as it can be, which is not at all: it is reported as not verified,
/// which is not damage. See [`scrub_db_with_options`].
pub fn scrub_db<P: AsRef<Path>>(db_path: P) -> Result<ScrubReport> {
	scrub_db_with_options(db_path, &Options::default())
}

/// Verifies every block of every table of a database as [`scrub_db`] does, and with the key manager
/// of `options` authenticates the blocks of the tables that are sealed.
///
/// The count is of blocks, of the meta-index, the top-level index, each partition of the index,
/// each data block and each filter index and partition: a table of a few blocks adds a few. A
/// block that is damaged is counted as a damaged table, which is what stops the read of it, and
/// a table that cannot be read for want of a key or for being another database's is not
/// verified, and is said to be. In a sealed table a block that does not authenticate is damaged,
/// and so is a header that is malformed; a header of a later format, a wrong or retired key, a
/// database that is not the one in the manifest, a table that is not the one that its name says
/// and a table in the clear in a database that is encrypted are not verified.
///
/// With a key and a manifest the identity of the database is the manifest's, and a key that does
/// not open it is the error of the call. Without a manifest each table is read as the identity
/// in its own header says. Nothing is written, and in a build that does not take a key manager
/// the call is refused with `Error::InvalidArgument`.
pub fn scrub_db_with_options<P: AsRef<Path>>(db_path: P, options: &Options) -> Result<ScrubReport> {
	let opts = options_for_database(db_path.as_ref(), options)?;
	let mut report = ScrubReport::default();

	let mut tables = Vec::new();
	for entry in fs::read_dir(opts.sstable_dir())?.flatten() {
		let p = entry.path();
		if p.extension().is_some_and(|ext| ext == "sst") {
			tables.push(p);
		}
	}
	tables.sort();

	for p in tables {
		report.total_tables += 1;
		match dump_sst_with_options(&p, false, true, &opts) {
			Ok(info) => {
				report.verified_tables += 1;
				report.total_blocks += info.verified_blocks.unwrap_or(0);
				if let Some(counts) = &info.verified {
					report.blocks.add(counts);
				}
			}
			// A table of an older format is intact: it is read once the database is opened,
			// which migrates it, and a count of corrupted blocks would say otherwise.
			Err(e @ Error::LegacyFormat(_)) => return Err(e),
			Err(e) => match &e {
				Error::Encryption(error) => match not_verified(error) {
					Some(why) => {
						report.not_verified_tables += 1;
						report.errors.push(format!("{}: not verified, {why}: {e}", p.display()));
					}
					None => {
						report.corrupt_blocks += 1;
						report.errors.push(format!("{}: {e}", p.display()));
					}
				},
				_ => {
					report.corrupt_blocks += 1;
					report.errors.push(format!("{}: {e}", p.display()));
				}
			},
		}
	}

	Ok(report)
}
