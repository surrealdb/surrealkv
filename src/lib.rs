mod batch;
pub mod bplustree;
mod cache;
mod checkpoint;
mod clock;
mod commit;
mod compaction;
mod comparator;
mod compression;
mod discard;
mod error;
mod iter;
mod levels;
mod lockfile;
mod lsm;
mod memtable;
mod oracle;
mod snapshot;
mod sstable;
mod task;
mod transaction;
mod vfs;
mod vlog;
mod wal;

#[cfg(test)]
mod test;

use crate::clock::{DefaultLogicalClock, LogicalClock};
pub use crate::error::{Error, Result};
pub use crate::lsm::{Tree, TreeBuilder};
use crate::sstable::InternalKey;
pub use crate::transaction::{Durability, Mode, ReadOptions, Transaction, WriteOptions};
pub use comparator::{BytewiseComparator, Comparator, InternalKeyComparator, TimestampComparator};

use bytes::Bytes;
use sstable::bloom::LevelDBBloomFilter;
use std::{borrow::Cow, path::PathBuf, sync::Arc};

/// An optimised trait for converting values to bytes only when needed
pub trait IntoBytes {
	/// Convert the key to a slice of bytes
	fn as_slice(&self) -> &[u8];
	/// Convert the key to an owned bytes slice
	fn into_bytes(self) -> Bytes;
}

impl IntoBytes for &[u8] {
	fn as_slice(&self) -> &[u8] {
		self
	}
	fn into_bytes(self) -> Bytes {
		Bytes::copy_from_slice(self)
	}
}

impl<const N: usize> IntoBytes for &[u8; N] {
	fn as_slice(&self) -> &[u8] {
		&self[..]
	}
	fn into_bytes(self) -> Bytes {
		Bytes::copy_from_slice(&self[..])
	}
}

impl IntoBytes for Vec<u8> {
	fn as_slice(&self) -> &[u8] {
		self.as_slice()
	}
	fn into_bytes(self) -> Bytes {
		Bytes::from(self)
	}
}

impl IntoBytes for &Vec<u8> {
	fn as_slice(&self) -> &[u8] {
		&self[..]
	}
	fn into_bytes(self) -> Bytes {
		Bytes::copy_from_slice(&self[..])
	}
}

impl IntoBytes for Bytes {
	fn as_slice(&self) -> &[u8] {
		self.as_ref()
	}
	fn into_bytes(self) -> Bytes {
		self
	}
}

impl IntoBytes for &Bytes {
	fn as_slice(&self) -> &[u8] {
		self.as_ref()
	}
	fn into_bytes(self) -> Bytes {
		self.clone()
	}
}

impl IntoBytes for &str {
	fn as_slice(&self) -> &[u8] {
		self.as_bytes()
	}
	fn into_bytes(self) -> Bytes {
		Bytes::copy_from_slice(self.as_bytes())
	}
}

impl IntoBytes for String {
	fn as_slice(&self) -> &[u8] {
		self.as_bytes()
	}
	fn into_bytes(self) -> Bytes {
		Bytes::from(self.into_bytes())
	}
}

impl IntoBytes for &String {
	fn as_slice(&self) -> &[u8] {
		self.as_bytes()
	}
	fn into_bytes(self) -> Bytes {
		Bytes::copy_from_slice(self.as_bytes())
	}
}

impl IntoBytes for Box<[u8]> {
	fn as_slice(&self) -> &[u8] {
		self.as_ref()
	}
	fn into_bytes(self) -> Bytes {
		Bytes::from(self)
	}
}

impl<'a> IntoBytes for Cow<'a, [u8]> {
	fn as_slice(&self) -> &[u8] {
		self.as_ref()
	}
	fn into_bytes(self) -> Bytes {
		match self {
			Cow::Borrowed(s) => Bytes::copy_from_slice(s),
			Cow::Owned(v) => Bytes::from(v),
		}
	}
}

/// Type alias for iterator results containing key-value pairs
/// Value is optional to support keys-only iteration without allocating empty values
pub type IterResult = Result<(Key, Option<Value>)>;

/// The Key type used throughout the LSM tree
pub type Key = Bytes;

/// The Value type used throughout the LSM tree  
pub type Value = Bytes;

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
	/// Attempt automatic repair of corrupted WAL segments and retry replay.
	#[default]
	TolerateCorruptedWithRepair,

	/// Fail immediately on any WAL corruption (no repair attempted).
	AbsoluteConsistency,
}

#[derive(Clone)]
pub struct Options {
	pub block_size: usize,
	pub block_restart_interval: usize,
	pub filter_policy: Option<Arc<dyn FilterPolicy>>,
	pub comparator: Arc<dyn Comparator>,
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
	/// Discard ratio threshold for triggering `VLog` garbage collection (0.0 - 1.0)
	/// Default: 0.5 (50% discardable data triggers GC)
	pub vlog_gc_discard_ratio: f64,
	/// If value size is less than this, it will be stored inline in `SSTable`
	pub vlog_value_threshold: usize,

	// Versioned query configuration
	/// If true, enables versioned queries with timestamp tracking
	pub enable_versioning: bool,
	/// History retention period in nanoseconds (0 means no retention limit)
	/// Default: 0 (no retention limit)
	pub versioned_history_retention_ns: u64,
	/// Logical clock for time-based operations
	pub(crate) clock: Arc<dyn LogicalClock>,

	// Shutdown configuration
	/// If true, flush active memtable to SSTable during shutdown.
	/// If false, skip flush for faster shutdown.
	///
	/// DEFAULT: false
	pub flush_on_close: bool,

	// WAL recovery configuration
	/// Controls behavior when WAL corruption is detected during recovery.
	/// Default: TolerateCorruptedWithRepair (attempt repair and continue)
	pub wal_recovery_mode: WalRecoveryMode,
}

impl Default for Options {
	fn default() -> Self {
		let bf = LevelDBBloomFilter::new(10);
		// Initialize the logical clock
		let clock = Arc::new(DefaultLogicalClock::new());

		Self {
			block_size: 64 * 1024, // 64KB
			block_restart_interval: 16,
			comparator: Arc::new(crate::BytewiseComparator {}),
			compression_per_level: Vec::new(),
			filter_policy: Some(Arc::new(bf)),
			block_cache: Arc::new(cache::BlockCache::with_capacity_bytes(1 << 20)), // 1MB cache
			path: PathBuf::from(""),
			level_count: 6,
			max_memtable_size: 100 * 1024 * 1024,  // 100 MB
			index_partition_size: 16384,           // 16KB
			vlog_max_file_size: 256 * 1024 * 1024, // 256MB
			vlog_checksum_verification: VLogChecksumLevel::Disabled,
			enable_vlog: false,
			vlog_gc_discard_ratio: 0.5, // 50% default
			vlog_value_threshold: 4096, // 4KB default
			enable_versioning: false,
			versioned_history_retention_ns: 0, // No retention limit by default
			clock,
			flush_on_close: true,
			wal_recovery_mode: WalRecoveryMode::default(),
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

	pub fn with_comparator(mut self, value: Arc<dyn Comparator>) -> Self {
		self.comparator = value;
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
	/// If vector is shorter than level count, last compression type is used for higher levels.
	/// If vector is empty, global compression setting is used.
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

	/// Convenience method: no compression on L0, Snappy compression on other levels.
	/// Equivalent to `with_compression_per_level(vec![CompressionType::None, CompressionType::SnappyCompression])`.
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

	/// Sets the unified block cache capacity (includes data blocks, index blocks, and VLog values)
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

	/// Sets the `VLog` garbage collection discard ratio.
	///
	/// # Panics
	///
	/// Panics if the value is not between 0.0 and 1.0 (inclusive).
	pub fn with_vlog_gc_discard_ratio(mut self, value: f64) -> Self {
		assert!((0.0..=1.0).contains(&value), "VLog GC discard ratio must be between 0.0 and 1.0");
		self.vlog_gc_discard_ratio = value;
		self
	}

	/// Enables or disables versioned queries with timestamp tracking
	/// When enabled, automatically configures VLog and value threshold for optimal versioned query support
	pub fn with_versioning(mut self, value: bool, retention_ns: u64) -> Self {
		self.enable_versioning = value;
		self.versioned_history_retention_ns = retention_ns;
		if value {
			// Versioned queries require VLog to be enabled
			self.enable_vlog = true;
			// All values should go to VLog for versioned queries
			self.vlog_value_threshold = 0;
		}
		self
	}

	/// Controls whether to flush the active memtable during database shutdown.
	///
	/// When enabled, ensures all in-memory data is persisted to SSTables before closing,
	/// at the cost of slower shutdown. When disabled, allows faster shutdown but unpersisted
	/// data in the active memtable will be lost if WAL is not enabled.
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

	/// Sets the WAL recovery mode to control behavior when corruption is detected.
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

	/// Returns the directory path for discard stats files
	pub(crate) fn discard_stats_dir(&self) -> PathBuf {
		self.path.join("discard_stats")
	}

	/// Returns the directory path for delete list files
	pub(crate) fn delete_list_dir(&self) -> PathBuf {
		self.path.join("delete_list")
	}

	/// Returns the directory path for versioned index files
	pub(crate) fn versioned_index_dir(&self) -> PathBuf {
		self.path.join("versioned_index")
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
	/// This should be called when the store starts to catch configuration errors early
	pub fn validate(&self) -> Result<()> {
		// Validate VLog GC discard ratio
		if !(0.0..=1.0).contains(&self.vlog_gc_discard_ratio) {
			return Err(Error::InvalidArgument(
				"VLog GC discard ratio must be between 0.0 and 1.0".to_string(),
			));
		}

		// Validate versioned queries configuration
		if self.enable_versioning {
			// Versioned queries require VLog to be enabled
			if !self.enable_vlog {
				return Err(Error::InvalidArgument(
					"Versioned queries require VLog to be enabled. Set enable_vlog to true."
						.to_string(),
				));
			}

			// Versioned queries don't work well with value threshold (values should go to VLog)
			if self.vlog_value_threshold > 0 {
				return Err(Error::InvalidArgument(
					"Versioned queries require all values to be stored in VLog. Set vlog_value_threshold to 0.".to_string(),
				));
			}
		}

		// Validate level count is reasonable
		if self.level_count == 0 {
			return Err(Error::InvalidArgument("Level count must be at least 1".to_string()));
		}

		Ok(())
	}
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum CompressionType {
	None = 0,
	SnappyCompression = 1,
}

impl CompressionType {
	pub const fn as_str(&self) -> &'static str {
		match *self {
			Self::None => "none",
			Self::SnappyCompression => "snappy",
		}
	}
}

impl From<u8> for CompressionType {
	fn from(byte: u8) -> Self {
		match byte {
			0 => Self::None,
			1 => Self::SnappyCompression,
			_ => panic!("Unknown compression type"),
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
}

pub(crate) trait Iterator {
	/// An iterator is either positioned at a key/value pair, or
	/// not valid.  This method returns true iff the iterator is valid.
	fn valid(&self) -> bool;

	/// Position at the first key in the source.  The iterator is `Valid()`
	/// after this call iff the source is not empty.
	fn seek_to_first(&mut self);

	/// Position at the last key in the source.  The iterator is
	/// `Valid()` after this call iff the source is not empty.
	fn seek_to_last(&mut self);

	/// Position at the first key in the source that is at or past target.
	/// The iterator is valid after this call iff the source contains
	/// an entry that comes at or past target.
	fn seek(&mut self, target: &[u8]) -> Option<()>;

	/// Moves to the next entry in the source.  After this call, the iterator is
	/// valid iff the iterator was not positioned at the last entry in the source.
	/// REQUIRES: `valid()`
	fn advance(&mut self) -> bool;

	/// Moves to the previous entry in the source.  After this call, the iterator
	/// is valid iff the iterator was not positioned at the first entry in source.
	/// REQUIRES: `valid()`
	fn prev(&mut self) -> bool;

	/// Return the key for the current entry.  The underlying storage for
	/// the returned slice is valid only until the next modification of
	/// the iterator.
	/// REQUIRES: `valid()`
	fn key(&self) -> Arc<InternalKey>;

	/// Return the value for the current entry.  The underlying storage for
	/// the returned slice is valid only until the next modification of
	/// the iterator.
	/// REQUIRES: `valid()`
	fn value(&self) -> Value;
}
