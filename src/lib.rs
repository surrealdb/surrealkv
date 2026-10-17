// A guard of a `std` or `parking_lot` lock that lives across an `.await` blocks the one thread of a
// browser's worker, or every task on a runtime that has few threads: the new awaits of the engine
// (the waits for the spare files of a pool) hold none, and this keeps it so.
#![deny(clippy::await_holding_lock)]
// Every call that names the operating system's file system is denied (`clippy.toml`, `Cargo.toml`),
// so that the engine goes through the file system seam in `src/vfs` and runs in a browser. The code
// of the tests builds and inspects directories on a disk on purpose, and is allowed here; the code
// of the engine that stays on `std::fs` is allowed where it is, each with a reason.
#![cfg_attr(
	test,
	allow(clippy::disallowed_methods, reason = "tests build and inspect directories on a disk")
)]
pub mod autotune;
mod batch;
pub use byteslice::{self, ByteSlice};
mod cache;
mod checkpoint;
mod clock;
mod compaction;
mod comparator;
mod compression;
mod describe;
mod encryption_status;
mod error;
pub mod inspect;
mod iter;
mod levels;
mod lockfile;
mod lsm;
pub mod memory;
mod memtable;
mod migrate;
mod ring;
mod rt;
// The background scrubber is planned, not implemented: nothing constructs it outside tests.
#[cfg(test)]
mod scrubber;
mod snapshot;
mod sstable;
mod stall;
// The storage seam and the wasm32 OPFS primitives. Public only so that the integration tests can
// reach them: not part of the stable API, and free to change in any release.
#[doc(hidden)]
pub mod storage;
mod task;
// Reachable only because `Options::key_manager` is a public field. The key manager, the cipher
// types and the errors are re-exported at the root, so keep the module out of the documented API.
#[doc(hidden)]
pub mod tde;
pub mod telemetry;
mod tracker;
mod transaction;
pub(crate) mod varint;
mod vfs;
mod vlog;
mod wal;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test;
// The tests that need a browser: see the module for how to run them.
#[cfg(all(test, target_arch = "wasm32", target_os = "unknown"))]
mod wasm_tests;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

pub use compaction::leveled::CompactionPriority;
pub use comparator::{BytewiseComparator, Comparator, InternalKeyComparator};
use sstable::bloom::LevelDBBloomFilter;
pub use sstable::ribbon::RibbonFilter;

use crate::clock::{DefaultLogicalClock, LogicalClock};
// The types of `Tree::encryption_status`, hidden with the types they are made of until their
// shape is settled.
#[doc(hidden)]
pub use crate::encryption_status::{EncryptionStatus, FileTotals, KeyUsage};
pub use crate::error::{
	BackgroundError,
	BackgroundErrorHandler,
	BackgroundErrorReason,
	EncryptionError,
	Error,
	ErrorSeverity,
	KeyProviderFailure,
	Result,
	WriteStallReason,
};
pub use crate::lsm::{Tree, TreeBuilder};
pub use crate::stall::WriteStallInfo;
use crate::tde::EncryptionContext;
#[doc(hidden)]
pub use crate::tde::{CipherPreference, CipherSuite, KeyManager, KeyState, SoftwareKeyManager};
pub use crate::transaction::{
	Durability,
	HistoryOptions,
	Mode,
	ReadOptions,
	Transaction,
	WriteOptions,
};
pub use crate::vfs::FileSystem;
#[doc(hidden)]
pub use crate::vfs::{CrashMode, MountReport};
pub use crate::wal::WalRecoveryReport;

/// An optimised trait for converting values to bytes only when needed
pub trait IntoBytes {
	/// Convert the key to a slice of bytes
	fn as_slice(&self) -> &[u8];
	/// Convert the key to an owned bytes slice
	fn into_bytes(self) -> Value;
}

impl IntoBytes for &[u8] {
	fn as_slice(&self) -> &[u8] {
		self
	}

	fn into_bytes(self) -> Value {
		self.to_vec()
	}
}

impl<const N: usize> IntoBytes for &[u8; N] {
	fn as_slice(&self) -> &[u8] {
		&self[..]
	}

	fn into_bytes(self) -> Value {
		self.to_vec()
	}
}

impl IntoBytes for Vec<u8> {
	fn as_slice(&self) -> &[u8] {
		self.as_slice()
	}

	fn into_bytes(self) -> Value {
		self
	}
}

impl IntoBytes for &Vec<u8> {
	fn as_slice(&self) -> &[u8] {
		&self[..]
	}

	fn into_bytes(self) -> Value {
		self.clone()
	}
}

impl IntoBytes for &str {
	fn as_slice(&self) -> &[u8] {
		self.as_bytes()
	}

	fn into_bytes(self) -> Value {
		self.as_bytes().to_vec()
	}
}

impl IntoBytes for Box<[u8]> {
	fn as_slice(&self) -> &[u8] {
		self.as_ref()
	}

	fn into_bytes(self) -> Value {
		self.into_vec()
	}
}

impl IntoBytes for ByteSlice {
	#[inline]
	fn as_slice(&self) -> &[u8] {
		ByteSlice::as_slice(self)
	}

	#[inline]
	fn into_bytes(self) -> Value {
		self.to_vec()
	}
}

impl IntoBytes for &ByteSlice {
	#[inline]
	fn as_slice(&self) -> &[u8] {
		ByteSlice::as_slice(self)
	}

	#[inline]
	fn into_bytes(self) -> Value {
		self.to_vec()
	}
}

/// Type alias for iterator results containing key-value pairs
/// Value is optional to support keys-only iteration without allocating empty
/// values
pub type IterResult = Result<(Key, Option<Value>)>;

/// The Key type used throughout the LSM tree
pub type Key = Vec<u8>;

/// The Value type used throughout the LSM tree
pub type Value = Vec<u8>;

/// Type alias for version/timestamp values
pub type Version = u64;

/// Type alias for iterator results containing only keys
pub type KeysResult = Result<Key>;

/// Type alias for iterator results containing keys and values
pub type RangeResult = Result<(Key, Value)>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VLogChecksumLevel {
	/// No checksum verification - fastest but no data integrity protection
	#[default]
	Disabled = 0,
	/// Full verification - recalculate checksum of value content
	Full = 1,
}

/// WAL recovery mode to control consistency guarantees during crash recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalRecoveryMode {
	/// Trim only a proven torn tail in the last segment. Refuse interior corruption
	/// or corruption in any earlier segment.
	#[default]
	TolerateTornTail,

	/// Fail immediately on any WAL corruption (no repair or trimming attempted).
	AbsoluteConsistency,

	/// Salvage any valid prefix before corruption in any segment, discarding
	/// all subsequent records.
	SalvageValidPrefix,

	/// Backward-compatible alias for `SalvageValidPrefix`.
	TolerateCorruptedWithRepair,
}

#[derive(Clone)]
pub struct Options {
	pub block_size: usize,
	pub block_restart_interval: usize,
	pub filter_policy: Option<Arc<dyn FilterPolicy>>,
	pub comparator: Arc<dyn Comparator>,
	pub(crate) internal_comparator: Arc<dyn Comparator>,
	pub compression_per_level: Vec<CompressionType>,
	pub(crate) block_cache: Arc<cache::BlockCache>,
	pub path: PathBuf,
	pub level_count: u8,
	pub max_memtable_size: usize,
	pub index_partition_size: usize,

	// VLog configuration
	pub vlog_max_file_size: u64,
	pub vlog_checksum_verification: VLogChecksumLevel,
	/// If true, disables `VLog` creation entirely
	pub enable_vlog: bool,
	/// If value size is less than this, it will be stored inline in `SSTable`
	pub vlog_value_threshold: usize,

	// Versioned query configuration
	/// If true, every write is stamped with a timestamp, stored with its value, and versions
	/// are kept for `get_at` and `history` instead of only the newest one.
	pub enable_versioning: bool,
	/// How long old versions are kept, in nanoseconds, when `enable_versioning` is set. A
	/// version is kept while it is younger than this, and so is the newest version that is
	/// older, which is what a read at the start of the period sees. 0 keeps every version.
	/// Default: 0
	pub versioned_history_retention_ns: u64,
	/// Logical clock for time-based operations
	pub(crate) clock: Arc<dyn LogicalClock>,

	// Shutdown configuration
	/// If true, flush active memtable to SSTable during shutdown.
	/// If false, skip flush for faster shutdown.
	///
	/// DEFAULT: true
	pub flush_on_close: bool,

	// WAL recovery configuration
	/// Controls behavior when WAL corruption is detected during recovery.
	/// Default: TolerateCorruptedWithRepair (attempt repair and continue)
	pub wal_recovery_mode: WalRecoveryMode,

	// Compaction configuration
	/// Number of L0 files that trigger compaction
	/// Default: 4
	pub level0_max_files: usize,
	/// Base size for level 1 in bytes
	/// Default: 256MB
	pub max_bytes_for_level: u64,
	/// Multiplier for calculating max bytes for each level
	/// Level N max bytes = base * multiplier^(N-1)
	/// Default: 10.0
	pub level_multiplier: f64,
	/// Target size for SSTs produced by compaction. A compaction rolls over
	/// to a new output file once the current one reaches this size (cutting
	/// only at user-key boundaries), which keeps compaction memory and the
	/// size of any single compaction's inputs bounded regardless of level
	/// size.
	/// Default: 64MB
	pub target_file_size: u64,

	/// Number of immutable memtables that triggers write stall.
	/// When immutable memtable count >= this threshold, writes block until flushes complete.
	/// Default: 2
	pub memtable_stall_threshold: usize,
	/// Number of L0 files that triggers write stall.
	/// When L0 file count >= this threshold, writes block until compactions complete.
	/// Should be >= level0_max_files (compaction trigger).
	/// Default: 12 (3x level0_max_files)
	pub l0_stall_threshold: usize,

	// Transparent Data Encryption (TDE) configuration
	/// Key manager for transparent data encryption at rest (TDE). A database that is created with
	/// one is encrypted for good: its manifest, tables, value log and write-ahead log are sealed
	/// under keys derived from it, and a database that was created without one cannot be given
	/// one. Needs the `encryption` feature, which is on by default: without it a key manager is
	/// refused with `Error::InvalidArgument`.
	/// Default: None
	pub key_manager: Option<Arc<dyn KeyManager>>,
	/// Cipher suite preference for a database that is created encrypted. `Auto` lets the library
	/// pick AES-256-GCM when the CPU has AES instructions and XChaCha20-Poly1305 otherwise, once,
	/// when the database is created. The suite is then kept in the database, and a database that
	/// exists ignores this.
	/// Options: Auto, Suite(Aes256Gcm), Suite(XChaCha20Poly1305).
	/// Default: CipherPreference::Auto
	pub encryption_cipher: CipherPreference,
	/// The encryption context a database resolves once it has read its descriptor: the key
	/// manager, the suite and the database identity. It is set by the engine after the manifest
	/// is read, and every writer and reader of a file takes its cipher from it. It is a public
	/// field only so that code outside the crate can keep building `Options` with
	/// `..Default::default()`.
	#[doc(hidden)]
	pub encryption_context: Option<Arc<EncryptionContext>>,

	/// The file system the database lives in. `std::fs` on a native build, and nothing in a
	/// browser, where one has to be mounted and given with `with_file_system`.
	pub(crate) fs: Arc<dyn vfs::Fs>,
}

/// The sizes that `Options::default()` starts from. A native build has the memory of a server and
/// these are the sizes it has always had. A browser worker is one thread in an address space of at
/// most 4 GiB that the page and every other tab have a share of, with a flush and a compaction that
/// each hold the worker for as long as they take (nothing else runs meanwhile), so its defaults are
/// smaller: a memtable, a block cache and an output table of 16 MiB. The cache of a worker has
/// four shards, and a shard admits an item of up to 97 percent of its share, which is about 4 MiB
/// here, so a block of 64 KiB is admitted with room to spare. These are starting points and not
/// measurements, and every one of them can be set.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
mod default_sizes {
	pub(super) const MEMTABLE: usize = 100 * 1024 * 1024;
	pub(super) const BLOCK_CACHE: u64 = 32 << 20;
	pub(super) const TARGET_FILE: u64 = 64 * 1024 * 1024;
}

#[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
mod default_sizes {
	pub(super) const MEMTABLE: usize = 16 * 1024 * 1024;
	pub(super) const BLOCK_CACHE: u64 = 16 << 20;
	pub(super) const TARGET_FILE: u64 = 16 * 1024 * 1024;
}

impl Default for Options {
	fn default() -> Self {
		let bf = LevelDBBloomFilter::new(10);
		// Initialize the logical clock
		let clock = Arc::new(DefaultLogicalClock::new());

		let comparator: Arc<dyn Comparator> = Arc::new(crate::BytewiseComparator {});
		let internal_comparator: Arc<dyn Comparator> =
			Arc::new(InternalKeyComparator::new(Arc::clone(&comparator)));

		Self {
			block_size: 64 * 1024, // 64KB
			block_restart_interval: 16,
			comparator,
			internal_comparator,
			compression_per_level: Vec::new(),
			filter_policy: Some(Arc::new(bf)),
			// 32MB. The cache is sharded, and each shard enforces a
			// per-item weight limit derived from capacity/shards; below
			// ~32MB that limit can reject ~16 KB index/filter partitions
			// outright, forcing every lookup to re-read them from disk.
			// (16MB in a browser, where there are four shards: see `default_sizes`.)
			block_cache: Arc::new(cache::BlockCache::with_capacity_bytes(
				default_sizes::BLOCK_CACHE,
			)),
			path: PathBuf::from(""),
			level_count: 6,
			max_memtable_size: default_sizes::MEMTABLE, // 100 MB (16 MB in a browser)
			index_partition_size: 16384,                // 16KB
			vlog_max_file_size: 256 * 1024 * 1024,      // 256MB
			vlog_checksum_verification: VLogChecksumLevel::Disabled,
			enable_vlog: false,
			vlog_value_threshold: 1024, // 1KB default
			enable_versioning: false,
			versioned_history_retention_ns: 0, // No retention limit by default
			clock,
			flush_on_close: true,
			wal_recovery_mode: WalRecoveryMode::default(),
			level0_max_files: 4,
			max_bytes_for_level: 256 * 1024 * 1024, // 256MB
			level_multiplier: 10.0,
			target_file_size: default_sizes::TARGET_FILE, // 64MB (16 MB in a browser)
			memtable_stall_threshold: 2,
			l0_stall_threshold: 12,
			key_manager: None,
			encryption_cipher: CipherPreference::default(),
			encryption_context: None,
			fs: vfs::options_default_fs(),
		}
	}
}

impl Options {
	pub fn new() -> Self {
		Self::default()
	}

	pub const fn with_block_size(mut self, value: usize) -> Self {
		self.block_size = value;
		self
	}

	pub const fn with_block_restart_interval(mut self, value: usize) -> Self {
		self.block_restart_interval = value;
		self
	}

	pub fn with_filter_policy(mut self, value: Option<Arc<dyn FilterPolicy>>) -> Self {
		self.filter_policy = value;
		self
	}

	/// Puts the database in `fs`. The default is the operating system's files on a native build,
	/// and in a browser there is no default: a file system has to be mounted and given here, or
	/// building a tree fails with an error that says so before it touches anything.
	pub fn with_file_system(mut self, fs: FileSystem) -> Self {
		self.fs = fs.0;
		self
	}

	pub fn with_comparator(mut self, value: Arc<dyn Comparator>) -> Self {
		self.internal_comparator = Arc::new(InternalKeyComparator::new(Arc::clone(&value)));
		self.comparator = value;
		self
	}

	/// Creates the database encrypted at rest with the keys of `key_manager`, or opens the one that
	/// is, which needs the same keys. Every file of the database is sealed: the manifest, the
	/// tables, the value log and the write-ahead log. `cipher` is the suite a new database uses,
	/// and an existing one keeps its own.
	///
	/// Needs the `encryption` feature, which is on by default: without it, building a tree with a
	/// key manager set fails with `Error::InvalidArgument`.
	pub fn with_encryption(
		mut self,
		key_manager: Arc<dyn KeyManager>,
		cipher: impl Into<CipherPreference>,
	) -> Self {
		self.key_manager = Some(key_manager);
		self.encryption_cipher = cipher.into();
		self
	}

	/// The resolved encryption context, if this database is encrypted. The writers and readers of
	/// every kind of file take their cipher from it.
	pub(crate) fn encryption_context(&self) -> Option<&Arc<EncryptionContext>> {
		self.encryption_context.as_ref()
	}

	/// Refuses options that name a key manager but carry no resolved context. `Tree::new` settles
	/// the context from the manifest before it opens a file, so options like these reaching a
	/// writer would mean a path that skipped that, and every writer would then write in the clear
	/// without saying so.
	pub(crate) fn ensure_encryption_resolved(&self) -> Result<()> {
		if self.key_manager.is_some() && self.encryption_context.is_none() {
			return Err(Error::InvalidArgument(
				"A key manager is configured but the encryption of the database was not resolved: \
				 refusing to write files in plaintext"
					.to_string(),
			));
		}
		Ok(())
	}

	/// Sets the resolved encryption context. The engine calls this once, with the identity and
	/// the suite that the database's descriptor names, before any file is opened.
	pub(crate) fn with_encryption_context(mut self, context: Arc<EncryptionContext>) -> Self {
		self.encryption_context = Some(context);
		self
	}

	/// Disables compression for data blocks in SSTables.
	///
	/// This clears the compression_per_level vector, causing all levels
	/// to default to CompressionType::None.
	///
	/// # Example
	///
	/// ```no_run
	/// use surrealkv::Options;
	///
	/// let opts = Options::new().without_compression();
	/// ```
	pub fn without_compression(mut self) -> Self {
		self.compression_per_level = Vec::new();
		self
	}

	/// Sets compression per level. Vector index corresponds to level number.
	/// If vector is shorter than level count, last compression type is used for
	/// higher levels. If vector is empty, global compression setting is used.
	///
	/// # Example
	///
	/// ```no_run
	/// use surrealkv::{Options, CompressionType};
	///
	/// let opts = Options::new()
	///     .with_compression_per_level(vec![
	///         CompressionType::None,        // L0: no compression
	///         CompressionType::SnappyCompression, // L1+: Snappy compression
	///     ]);
	/// ```
	pub fn with_compression_per_level(mut self, levels: Vec<CompressionType>) -> Self {
		self.compression_per_level = levels;
		self
	}

	/// Convenience method: no compression on L0, Snappy compression on other
	/// levels. Equivalent to
	/// `with_compression_per_level(vec![CompressionType::None,
	/// CompressionType::SnappyCompression])`.
	///
	/// # Example
	///
	/// ```no_run
	/// use surrealkv::Options;
	///
	/// let opts = Options::new().with_l0_no_compression();
	/// ```
	pub fn with_l0_no_compression(mut self) -> Self {
		self.compression_per_level =
			vec![CompressionType::None, CompressionType::SnappyCompression];
		self
	}

	pub fn with_path(mut self, value: PathBuf) -> Self {
		self.path = value;
		self
	}

	pub const fn with_level_count(mut self, value: u8) -> Self {
		self.level_count = value;
		self
	}

	pub const fn with_max_memtable_size(mut self, value: usize) -> Self {
		self.max_memtable_size = value;
		self
	}

	/// Sets the target size for SSTs produced by compaction; a compaction
	/// rolls over to a new output file once the current one reaches this
	/// size (cut only at user-key boundaries).
	pub const fn with_target_file_size(mut self, value: u64) -> Self {
		self.target_file_size = value;
		self
	}

	/// Sets the unified block cache capacity (includes data blocks, index
	/// blocks, and VLog values)
	pub fn with_block_cache_capacity(mut self, capacity_bytes: u64) -> Self {
		self.block_cache = Arc::new(cache::BlockCache::with_capacity_bytes(capacity_bytes));
		self
	}

	// Partitioned index configuration
	pub const fn with_index_partition_size(mut self, size: usize) -> Self {
		self.index_partition_size = size;
		self
	}

	pub const fn with_vlog_max_file_size(mut self, value: u64) -> Self {
		self.vlog_max_file_size = value;
		self
	}

	pub const fn with_vlog_checksum_verification(mut self, value: VLogChecksumLevel) -> Self {
		self.vlog_checksum_verification = value;
		self
	}

	pub const fn with_enable_vlog(mut self, value: bool) -> Self {
		self.enable_vlog = value;
		self
	}

	/// Sets the VLog value threshold in bytes.
	///
	/// Values smaller than this threshold are stored inline in SSTables.
	/// Values larger than or equal to this threshold are stored in VLog files.
	///
	/// Default: 1024 (1KB)
	///
	/// # Example
	///
	/// ```no_run
	/// use surrealkv::TreeBuilder;
	///
	/// let tree = TreeBuilder::new()
	///     .with_path("./data".into())
	///     .with_enable_vlog(true)
	///     .with_vlog_value_threshold(8192) // 8KB threshold
	///     .build()
	///     .unwrap();
	/// ```
	pub const fn with_vlog_value_threshold(mut self, value: usize) -> Self {
		self.vlog_value_threshold = value;
		self
	}

	/// Enables or disables versioned queries.
	///
	/// A versioned tree stamps every write with its timestamp (the commit time, or the one given
	/// to `set_at`), keeps the stamp with the value, and keeps old versions for
	/// `Transaction::get_at` and `Transaction::history` for `retention_ns` nanoseconds, or for
	/// ever if that is 0. It needs no value log and no index: values are stored as they are in
	/// an unversioned tree, with the eight bytes of the stamp and two for a header; a soft delete
	/// has no value and is stored as those ten bytes.
	///
	/// A time of 0, given to `set_at` or in `WriteOptions`, is the time of the commit.
	pub const fn with_versioning(mut self, value: bool, retention_ns: u64) -> Self {
		self.enable_versioning = value;
		self.versioned_history_retention_ns = retention_ns;
		self
	}

	/// Controls whether to flush the active memtable during database shutdown.
	///
	/// When enabled, ensures all in-memory data is persisted to SSTables before
	/// closing, at the cost of slower shutdown. When disabled, allows faster
	/// shutdown but unpersisted data in the active memtable will be lost if
	/// WAL is not enabled.
	///
	/// # Arguments
	///
	/// * `value` - If true, flush on close. If false, skip flush.
	///
	/// # Default
	///
	/// false (skip flush for faster shutdown)
	pub const fn with_flush_on_close(mut self, value: bool) -> Self {
		self.flush_on_close = value;
		self
	}

	/// Sets the WAL recovery mode to control behavior when corruption is
	/// detected.
	///
	/// # Arguments
	///
	/// * `mode` - The recovery mode to use:
	///   - `TolerateCorruptedWithRepair`: Attempt repair and continue (default)
	///   - `AbsoluteConsistency`: Fail immediately on any corruption
	///
	/// # Default
	///
	/// `TolerateCorruptedWithRepair`
	pub const fn with_wal_recovery_mode(mut self, mode: WalRecoveryMode) -> Self {
		self.wal_recovery_mode = mode;
		self
	}

	/// Sets the number of immutable memtables that triggers write stall.
	pub const fn with_memtable_stall_threshold(mut self, value: usize) -> Self {
		self.memtable_stall_threshold = value;
		self
	}

	/// Sets the number of L0 files that triggers write stall.
	pub const fn with_l0_stall_threshold(mut self, value: usize) -> Self {
		self.l0_stall_threshold = value;
		self
	}

	/// Returns the path for a manifest file with the given ID
	/// Format: {path}/manifest/{id:020}.manifest
	pub(crate) fn manifest_file_path(&self, id: u64) -> PathBuf {
		self.manifest_dir().join(format!("{id:020}.manifest"))
	}

	/// Returns the path for an `SSTable` file with the given ID
	/// Format: {path}/sstables/{id:020}.sst
	pub(crate) fn sstable_file_path(&self, id: u64) -> PathBuf {
		self.sstable_dir().join(format!("{id:020}.sst"))
	}

	/// Returns the path for a `VLog` file with the given ID
	/// Format: {path}/vlog/{id:020}.vlog
	pub(crate) fn vlog_file_path(&self, id: u64) -> PathBuf {
		self.vlog_dir().join(format!("{id:020}.vlog"))
	}

	/// Returns the directory path for WAL files
	pub(crate) fn wal_dir(&self) -> PathBuf {
		self.path.join("wal")
	}

	/// Returns the directory path for `SSTable` files
	pub(crate) fn sstable_dir(&self) -> PathBuf {
		self.path.join("sstables")
	}

	/// Returns the directory path for `VLog` files
	pub(crate) fn vlog_dir(&self) -> PathBuf {
		self.path.join("vlog")
	}

	/// Returns the directory path for manifest files
	pub(crate) fn manifest_dir(&self) -> PathBuf {
		self.path.join("manifest")
	}

	/// Checks if a filename matches the `VLog` file naming pattern
	/// Expected format: 20-digit zero-padded ID + ".vlog" (25 characters total)
	pub(crate) fn is_vlog_filename(&self, filename: &str) -> bool {
		filename.len() == 25
			&& std::path::Path::new(filename)
				.extension()
				.is_some_and(|ext| ext.eq_ignore_ascii_case("vlog"))
	}

	/// Extracts the file ID from a `VLog` filename
	/// Returns None if the filename doesn't match the expected pattern
	pub(crate) fn extract_vlog_file_id(&self, filename: &str) -> Option<u32> {
		if self.is_vlog_filename(filename) {
			if let Some(id_part) = filename.strip_suffix(".vlog") {
				if id_part.len() == 20 && id_part.chars().all(|c| c.is_ascii_digit()) {
					return id_part.parse::<u32>().ok();
				}
			}
		}
		None
	}

	/// Validates the configuration options for consistency and correctness
	/// This should be called when the store starts to catch configuration
	/// errors early
	pub fn validate(&self) -> Result<()> {
		self.validate_for_pointer_width(usize::BITS)
	}

	/// `validate`, for a target with `pointer_bits`-bit pointers. The limits that depend on the
	/// address space are checked against it, so that a 32-bit target's limits are tested on any
	/// host.
	fn validate_for_pointer_width(&self, pointer_bits: u32) -> Result<()> {
		// A pool is refused until every part of the engine that asks the file system whether a
		// database exists goes through it (see `vfs::POOL_FS_PORT_COMPLETE`). Nothing is created
		// before this, so the refusal leaves nothing behind.
		if !vfs::POOL_FS_PORT_COMPLETE && self.fs.kind() == vfs::FsKind::Pool {
			return Err(Error::InvalidArgument(
				"a pooled file system is not supported by this build of the engine yet".to_string(),
			));
		}

		// A build without the `encryption` feature must not seal anything, so it refuses a key
		// manager rather than writing plaintext to a database that was asked to be encrypted.
		// Refuse here, which is the first thing `Tree::new` does, so a rejection creates nothing
		// on disk. A resolved context is refused as well: it is a public field, and it is what
		// makes the files sealed. Compiled out only for this crate's own unit tests and under the
		// `encryption` feature, which is on by default.
		#[cfg(not(any(test, feature = "encryption")))]
		if self.key_manager.is_some() || self.encryption_context.is_some() {
			return Err(Error::InvalidArgument(
				"Encryption at rest is not enabled in this build: it needs the `encryption` \
				 feature. Refusing to open with a key manager; remove the key manager."
					.to_string(),
			));
		}

		// Validate level count is reasonable
		if self.level_count == 0 {
			return Err(Error::InvalidArgument("Level count must be at least 1".to_string()));
		}

		// Validate write stall configuration
		if self.memtable_stall_threshold < 2 {
			return Err(Error::InvalidArgument(
				"memtable_stall_threshold must be >= 2".to_string(),
			));
		}
		if self.l0_stall_threshold < self.level0_max_files {
			return Err(Error::InvalidArgument(format!(
				"l0_stall_threshold ({}) must be >= level0_max_files ({})",
				self.l0_stall_threshold, self.level0_max_files
			)));
		}

		check_memtable_size_is_addressable(self.max_memtable_size, pointer_bits)?;

		Ok(())
	}
}

/// The first `max_memtable_size` that a target with 32-bit pointers cannot give a memtable: the
/// arena is one allocation of at most `isize::MAX` bytes there, which is 2 GiB less one, and its
/// size is rounded down to a multiple of 64. A size from here up is refused rather than let the
/// allocation abort the process, which in a browser traps the whole instance.
const MEMTABLE_SIZE_LIMIT_32_BIT: u64 = (1 << 31) - 64;

/// Refuses a `max_memtable_size` that a target with `pointer_bits`-bit pointers cannot allocate
/// as one memtable arena. Nothing is refused on a 64-bit target, whose arenas are limited by
/// artmap alone (4 GiB).
fn check_memtable_size_is_addressable(max_memtable_size: usize, pointer_bits: u32) -> Result<()> {
	if pointer_bits <= 32 && max_memtable_size as u64 >= MEMTABLE_SIZE_LIMIT_32_BIT {
		return Err(Error::InvalidArgument(format!(
			"max_memtable_size ({max_memtable_size}) must be less than {MEMTABLE_SIZE_LIMIT_32_BIT} \
			 bytes on a target with 32-bit pointers"
		)));
	}
	Ok(())
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum CompressionType {
	None = 0,
	SnappyCompression = 1,
	ZstdCompression = 2,
}

impl CompressionType {
	pub const fn as_str(&self) -> &'static str {
		match *self {
			Self::None => "none",
			Self::SnappyCompression => "snappy",
			Self::ZstdCompression => "zstd",
		}
	}
}

impl TryFrom<u8> for CompressionType {
	type Error = Error;

	fn try_from(byte: u8) -> Result<Self> {
		match byte {
			0 => Ok(Self::None),
			1 => Ok(Self::SnappyCompression),
			2 => Ok(Self::ZstdCompression),
			_ => Err(Error::Compression(format!("Unknown compression type: {}", byte))),
		}
	}
}

pub trait FilterPolicy: Send + Sync {
	/// Return the name of this policy.  Note that if the filter encoding
	/// changes in an incompatible way, the name returned by this method
	/// must be changed.  Otherwise, old incompatible filters may be
	/// passed to methods of this type.
	fn name(&self) -> &str;

	/// `MayContain` returns whether the encoded filter may contain given key.
	/// False positives are possible, where it returns true for keys not in the
	/// original set.
	fn may_contain(&self, filter: &[u8], key: &[u8]) -> bool;

	/// Creates a filter based on given keys
	fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8>;

	/// Returns the single hash value this policy derives from a key.
	///
	/// Filter construction may buffer this hash (8 bytes) instead of the raw
	/// key, so builder memory stays constant per key regardless of key size.
	/// `create_filter_from_hashes(&[key_hash(k) for k in keys])` must produce
	/// exactly the same bytes as `create_filter(keys)`.
	fn key_hash(&self, key: &[u8]) -> u64;

	/// Creates a filter from pre-computed key hashes (see [`Self::key_hash`]).
	fn create_filter_from_hashes(&self, hashes: &[u64]) -> Vec<u8>;

	/// Approximates how many keys produce a filter of `partition_bytes`
	/// bytes — used to size filter partitions (a partition cut is requested
	/// once this many keys accumulate). The default is a conservative
	/// 1 byte/key; policies should override with their real density.
	fn filter_keys_per_partition(&self, partition_bytes: usize) -> usize {
		partition_bytes.max(1)
	}
}

use std::ops::Bound;

/// Type alias for InternalKey range bounds
pub(crate) type InternalKeyRangeBound = Bound<InternalKey>;
/// Type alias for InternalKey ranges
pub(crate) type InternalKeyRange = (InternalKeyRangeBound, InternalKeyRangeBound);

/// Converts user key bounds to InternalKeyRange for efficient iteration.
pub(crate) fn user_range_to_internal_range(
	lower: Bound<&[u8]>,
	upper: Bound<&[u8]>,
) -> InternalKeyRange {
	let start_bound = match lower {
		Bound::Unbounded => Bound::Unbounded,
		Bound::Included(key) => Bound::Included(InternalKey::new(
			key.into_bytes(),
			INTERNAL_KEY_SEQ_NUM_MAX,
			InternalKeyKind::Max,
		)),
		Bound::Excluded(key) => {
			Bound::Excluded(InternalKey::new(key.into_bytes(), 0, InternalKeyKind::Set))
		}
	};

	let end_bound = match upper {
		Bound::Unbounded => Bound::Unbounded,
		Bound::Included(key) => {
			Bound::Included(InternalKey::new(key.into_bytes(), 0, InternalKeyKind::Set))
		}
		Bound::Excluded(key) => Bound::Excluded(InternalKey::new(
			key.into_bytes(),
			INTERNAL_KEY_SEQ_NUM_MAX,
			InternalKeyKind::Max,
		)),
	};

	(start_bound, end_bound)
}

// This is the maximum valid sequence number that can be stored in the upper 56
// bits of a 64-bit integer. 1 << 56 shifts the number 1 left by 56 bits,
// resulting in a binary number with a 1 followed by 56 zeros. Subtracting 1
// gives a binary number with 56 ones, which is the maximum value for 56 bits.
pub(crate) const INTERNAL_KEY_SEQ_NUM_MAX: u64 = (1 << 56) - 1;

// Helper function for reading u64 from byte slices without unwrap()
// Safe to use when bounds have already been checked
#[inline(always)]
fn read_u64_be(buffer: &[u8], offset: usize) -> u64 {
	// SAFETY: Caller must ensure buffer has at least offset + 8 bytes
	unsafe { u64::from_be_bytes(*(buffer.as_ptr().add(offset) as *const [u8; 8])) }
}

/// Converts a trailer byte to InternalKeyKind
/// This centralizes the kind conversion logic to avoid duplication and errors
fn trailer_to_kind(trailer: u64) -> InternalKeyKind {
	let kind_byte = trailer as u8;
	match kind_byte {
		0 => InternalKeyKind::Delete,
		1 => InternalKeyKind::SoftDelete,
		2 => InternalKeyKind::Set,
		3 => InternalKeyKind::Merge,
		4 => InternalKeyKind::LogData,
		5 => InternalKeyKind::RangeDelete,
		6 => InternalKeyKind::Replace,
		7 => InternalKeyKind::Separator,
		24 => InternalKeyKind::Max,
		_ => InternalKeyKind::Invalid,
	}
}

/// Extracts sequence number from trailer
/// This centralizes the seq_num extraction logic to avoid duplication
#[inline(always)]
fn trailer_to_seq_num(trailer: u64) -> u64 {
	trailer >> 8
}

/// Checks if a key kind represents a tombstone (delete operation)
#[inline(always)]
fn is_delete_kind(kind: InternalKeyKind) -> bool {
	matches!(
		kind,
		InternalKeyKind::Delete | InternalKeyKind::SoftDelete | InternalKeyKind::RangeDelete
	)
}

/// Checks if a key kind represents a hard delete (delete operation)
#[inline(always)]
fn is_hard_delete_marker(kind: InternalKeyKind) -> bool {
	matches!(kind, InternalKeyKind::Delete | InternalKeyKind::RangeDelete)
}

/// Checks if a key kind represents a Replace operation
#[inline(always)]
fn is_replace_kind(kind: InternalKeyKind) -> bool {
	matches!(kind, InternalKeyKind::Replace)
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum InternalKeyKind {
	Delete = 0,
	SoftDelete = 1,
	Set = 2,
	Merge = 3,
	LogData = 4,
	RangeDelete = 5,
	Replace = 6, // Replaces previous key when versioning is enabled
	Separator = 7,
	Max = 24, // Leaving space for other kinds
	Invalid = 191,
}

impl From<u8> for InternalKeyKind {
	fn from(value: u8) -> Self {
		trailer_to_kind(value as u64)
	}
}

/// Internal key type used throughout the LSM tree.
///
/// This is the owned version of `InternalKeyRef`. It includes the user key,
/// timestamp, and trailer (containing sequence number and operation kind).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct InternalKey {
	/// The application's key bytes.
	pub(crate) user_key: Key,
	/// Trailer containing (seq_num << 8) | kind.
	pub(crate) trailer: u64,
}

impl InternalKey {
	pub(crate) fn new(user_key: Key, seq_num: u64, kind: InternalKeyKind) -> Self {
		Self {
			user_key,
			trailer: (seq_num << 8) | kind as u64,
		}
	}

	pub(crate) fn size(&self) -> usize {
		self.user_key.len() + 8
	}

	pub(crate) fn decode(encoded_key: &[u8]) -> Self {
		let n = encoded_key.len() - 8;
		let trailer = read_u64_be(encoded_key, n);
		let user_key = encoded_key[..n].to_vec();

		Self {
			user_key,
			trailer,
		}
	}

	/// Extract user key slice without allocation
	#[inline]
	pub(crate) fn user_key_from_encoded(encoded: &[u8]) -> &[u8] {
		&encoded[..encoded.len() - 8]
	}

	/// Extract trailer (seq_num + kind) without allocation
	#[inline]
	pub(crate) fn trailer_from_encoded(encoded: &[u8]) -> u64 {
		let n = encoded.len() - 8;
		read_u64_be(encoded, n)
	}

	/// Extract seq_num from encoded key without allocation
	#[inline]
	pub(crate) fn seq_num_from_encoded(encoded: &[u8]) -> u64 {
		trailer_to_seq_num(Self::trailer_from_encoded(encoded))
	}

	pub(crate) fn encode(&self) -> Vec<u8> {
		let mut buf = Vec::with_capacity(self.user_key.len() + 8);
		buf.extend_from_slice(&self.user_key);
		buf.extend_from_slice(&self.trailer.to_be_bytes());
		buf
	}

	#[inline]
	pub(crate) fn encoded_len(&self) -> usize {
		self.user_key.len() + 8
	}

	#[inline]
	pub(crate) fn seq_num(&self) -> u64 {
		trailer_to_seq_num(self.trailer)
	}

	pub(crate) fn kind(&self) -> InternalKeyKind {
		trailer_to_kind(self.trailer)
	}

	#[inline]
	pub(crate) fn is_tombstone(&self) -> bool {
		is_delete_kind(self.kind())
	}

	#[inline]
	pub(crate) fn is_hard_delete_marker(&self) -> bool {
		is_hard_delete_marker(self.kind())
	}

	#[cfg(all(test, not(target_arch = "wasm32")))]
	#[inline]
	pub(crate) fn is_replace(&self) -> bool {
		is_replace_kind(self.kind())
	}
}

// Used only by memtable, sstable uses internal key comparator
impl Ord for InternalKey {
	fn cmp(&self, other: &Self) -> Ordering {
		match self.user_key.cmp(&other.user_key) {
			Ordering::Equal => match other.seq_num().cmp(&self.seq_num()) {
				Ordering::Equal => self.kind().cmp(&other.kind()),
				ord => ord,
			},
			ord => ord,
		}
	}
}

impl PartialOrd for InternalKey {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

/// A zero-copy reference to an internal key.
/// Zero-copy reference to an internal key.
///
/// Provides access to all components of an internal key:
/// - User key bytes
/// - Timestamp
/// - Sequence number
/// - Operation kind (Set, Delete, etc.)
///
/// The data lives in the source buffer (arena, block, etc.)
#[derive(Clone, Copy)]
pub struct InternalKeyRef<'a> {
	encoded: &'a [u8],
	/// The time the version was written at: kept with its value, so only an iterator that has
	/// read the value can say. 0 where there is none.
	timestamp: u64,
}

impl<'a> InternalKeyRef<'a> {
	#[inline]
	pub fn from_encoded(encoded: &'a [u8]) -> Self {
		debug_assert!(encoded.len() >= 8);
		Self {
			encoded,
			timestamp: 0,
		}
	}

	/// The same key, with the time its version was written at.
	#[inline]
	pub(crate) fn with_timestamp(mut self, timestamp: u64) -> Self {
		self.timestamp = timestamp;
		self
	}

	#[inline]
	pub fn user_key(&self) -> &'a [u8] {
		InternalKey::user_key_from_encoded(self.encoded)
	}

	#[inline]
	pub fn encoded(&self) -> &'a [u8] {
		self.encoded
	}

	#[inline]
	pub fn trailer(&self) -> u64 {
		InternalKey::trailer_from_encoded(self.encoded)
	}

	#[inline]
	pub fn seq_num(&self) -> u64 {
		InternalKey::seq_num_from_encoded(self.encoded)
	}

	/// The time this version was written at, in nanoseconds: what `set_at` was given, or the time
	/// of the commit. A version written while the tree was not versioned reads at the time of the
	/// versions written before it, and at 0 if there are none. A write of a transaction that is to
	/// be given the time of its commit reports `u64::MAX` until it is committed. 0 from an
	/// iterator that does not report times: only `Transaction::history` does.
	#[inline]
	pub fn timestamp(&self) -> u64 {
		self.timestamp
	}

	#[inline]
	pub fn kind(&self) -> InternalKeyKind {
		trailer_to_kind(self.trailer())
	}

	#[inline]
	pub fn is_tombstone(&self) -> bool {
		is_delete_kind(self.kind())
	}

	#[inline]
	pub fn is_hard_delete_marker(&self) -> bool {
		is_hard_delete_marker(self.kind())
	}

	#[inline]
	pub fn is_replace(&self) -> bool {
		is_replace_kind(self.kind())
	}

	pub(crate) fn to_owned(self) -> InternalKey {
		InternalKey::decode(self.encoded)
	}
}

impl PartialEq for InternalKeyRef<'_> {
	fn eq(&self, other: &Self) -> bool {
		self.encoded == other.encoded
	}
}

impl Eq for InternalKeyRef<'_> {}

impl std::fmt::Debug for InternalKeyRef<'_> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("InternalKeyRef")
			.field("user_key", &self.user_key())
			.field("seq_num", &self.seq_num())
			.field("kind", &self.kind())
			.field("timestamp", &self.timestamp())
			.finish()
	}
}

/// Cursor-based iterator for internal key-value pairs.
///
/// This trait provides low-level access to the LSM tree's internal key-value pairs.
/// Keys are returned as `InternalKeyRef` which includes the user key, timestamp,
/// sequence number, and operation kind.
///
/// # Example
/// ```ignore
/// let mut iter = tx.range(b"a", b"z")?;
/// iter.seek(b"foo")?;  // Position at first version of "foo"
/// while iter.valid() {
///     let key_ref = iter.key();
///     let user_key = key_ref.user_key();
///     let ts = key_ref.timestamp();
///     let is_del = key_ref.is_tombstone();
///     let value = iter.value()?;
///     iter.next()?;
/// }
/// ```
pub trait LSMIterator {
	/// Seek to first key >= target. Returns Ok(true) if valid.
	/// Target is an encoded internal key. Use `encode_seek_key()` to encode a user key.
	fn seek(&mut self, target: &[u8]) -> Result<bool>;

	/// Seek to first entry. Returns Ok(true) if valid.
	fn seek_first(&mut self) -> Result<bool>;

	/// Seek to last entry. Returns Ok(true) if valid.
	fn seek_last(&mut self) -> Result<bool>;

	/// Move to next entry. Returns Ok(true) if valid.
	fn next(&mut self) -> Result<bool>;

	/// Move to previous entry. Returns Ok(true) if valid.
	fn prev(&mut self) -> Result<bool>;

	/// Check if positioned on valid entry.
	fn valid(&self) -> bool;

	/// Re-anchor to the first entry as a CONTINUATION of the traversal already in
	/// progress, rather than as a fresh, user-initiated seek. See
	/// [`reanchor_last`](Self::reanchor_last) for the full contract.
	fn reanchor_first(&mut self) -> Result<bool> {
		self.seek_first()
	}

	/// Re-anchor to the last entry as a CONTINUATION of the traversal already in
	/// progress, rather than as a fresh, user-initiated seek.
	///
	/// A merge layer that owns two sub-cursors has to reposition the one it is not
	/// currently emitting from when iteration reverses direction. If that cursor
	/// has gone invalid, "reposition" means "anchor to your extreme entry" — but
	/// it must NOT mean "start over", because the traversal is not starting over.
	///
	/// The distinction is invisible for an iterator whose only state is a
	/// position: for those the default here, a plain `seek_first`/`seek_last`, is
	/// exactly right. It matters for an iterator carrying budget or policy state
	/// accumulated over the traversal — an entry limit, a quota, a deadline — where
	/// a user-initiated seek legitimately resets that state and an internal
	/// re-anchor legitimately must not. Such an implementation MUST override these
	/// two methods to reposition while preserving what it has already spent;
	/// otherwise the reverse pass silently runs on a refunded budget and yields
	/// entries the policy had excluded. `HistoryIterator` is the one implementation
	/// in-tree that needs the override.
	///
	/// Note also that `valid()` cannot be used to tell the two situations apart —
	/// an iterator stopped by a policy and one that simply ran out of data both
	/// report `false`, and an implementation may reach the former without ever
	/// recording it. That is precisely why this is an operation on the iterator
	/// rather than a predicate the caller branches on.
	///
	/// One latent case worth knowing about: `TransactionHistoryIterator` takes the
	/// default even though it wraps a `HistoryIterator` that does not. Its own
	/// `valid()` is `current_source != None`, which goes false exactly when the
	/// inner history source is policy-stopped and the write set is spent, so if it
	/// were ever nested as the `I` of another merge layer it would inherit the
	/// default and forward a budget-resetting seek. Nothing nests it today.
	fn reanchor_last(&mut self) -> Result<bool> {
		self.seek_last()
	}

	/// Get current key (zero-copy). Caller must check valid() first.
	///
	/// Returns an `InternalKeyRef` which provides access to:
	/// - `user_key()` - the application's key bytes
	/// - `timestamp()` - the version timestamp
	/// - `seq_num()` - the sequence number
	/// - `kind()` - the operation kind (Set, Delete, etc.)
	/// - `is_tombstone()` - whether this is a delete marker
	fn key(&self) -> InternalKeyRef<'_>;

	/// Get current raw value bytes (zero-copy). Caller must check valid() first.
	///
	/// For transaction-level iterators, this returns raw bytes that may be
	/// VLog-encoded. Use `value()`, `value_ref()`, or `value_into()` for resolved values.
	fn value_encoded(&self) -> Result<&[u8]>;

	/// Get current value with VLog resolution, borrowing inline or write-set bytes
	/// without heap allocation when possible.
	///
	/// Returns `Cow::Borrowed` when the value is stored inline (or in the transaction
	/// write-set), and `Cow::Owned` when fetched from the VLog.
	fn value_ref(&self) -> Result<std::borrow::Cow<'_, [u8]>> {
		Ok(std::borrow::Cow::Borrowed(self.value_encoded()?))
	}

	/// Copy the current resolved value into `buf`, clearing it first and reusing its
	/// existing heap capacity.
	fn value_into(&self, buf: &mut Vec<u8>) -> Result<()> {
		let val = self.value_ref()?;
		buf.clear();
		buf.extend_from_slice(&val);
		Ok(())
	}

	/// Get current value as owned bytes with VLog resolution.
	///
	/// This method resolves VLog pointers to actual values, returning owned data.
	/// For iterators without VLog (internal iterators), this clones the raw bytes.
	///
	/// Default implementation clones raw bytes from `value_encoded()`.
	fn value(&self) -> Result<Value> {
		Ok(self.value_encoded()?.to_vec())
	}
}

// Compiles the Rust code blocks in the README as doctests, so the examples
// cannot drift from the API without `cargo test --doc` failing.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

#[cfg(test)]
mod options_bound_tests {
	use super::*;

	const GIB: usize = 1 << 30;

	#[test]
	fn a_32_bit_target_refuses_a_memtable_it_cannot_allocate() {
		for size in [2 * GIB, 2 * GIB - 64, 3 * GIB, u32::MAX as usize] {
			let error = check_memtable_size_is_addressable(size, 32)
				.expect_err("a memtable of 2 GiB or more cannot be allocated with 32-bit pointers");
			assert!(matches!(error, Error::InvalidArgument(_)), "{error:?}");
		}
	}

	#[test]
	fn a_32_bit_target_accepts_what_it_can_allocate() {
		for size in [0, 64, 16 * 1024 * 1024, GIB, 2 * GIB - 128, 2 * GIB - 65] {
			check_memtable_size_is_addressable(size, 32).expect("the arena fits");
		}
	}

	#[test]
	fn a_64_bit_target_refuses_nothing() {
		for size in [0, 100 * 1024 * 1024, 2 * GIB, 3 * GIB] {
			check_memtable_size_is_addressable(size, 64).expect("no 32-bit limit applies");
		}
	}

	#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
	#[test]
	fn validate_applies_the_bound_of_the_pointer_width() {
		let options = Options::default().with_max_memtable_size(2 * GIB);
		options.validate_for_pointer_width(32).expect_err("2 GiB is refused with 32-bit pointers");
		options.validate_for_pointer_width(64).expect("2 GiB is fine with 64-bit pointers");
		// The default and the size of a browser's memtable pass either way.
		for size in [Options::default().max_memtable_size, 16 * 1024 * 1024] {
			let options = Options::default().with_max_memtable_size(size);
			options.validate_for_pointer_width(32).expect("within the 32-bit limit");
			options.validate().expect("valid on this target");
		}
	}
}
