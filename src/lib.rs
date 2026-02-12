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
mod snapshot;
mod sstable;
mod task;
mod transaction;
mod vfs;
mod vlog;
mod wal;

#[cfg(test)]
mod test;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

pub use comparator::{BytewiseComparator, Comparator, InternalKeyComparator, TimestampComparator};
use sstable::bloom::LevelDBBloomFilter;

use crate::clock::{DefaultLogicalClock, LogicalClock};
pub use crate::error::{Error, Result};
pub use crate::lsm::{Tree, TreeBuilder};
pub use crate::transaction::{
	Durability,
	HistoryOptions,
	Mode,
	ReadOptions,
	Transaction,
	WriteOptions,
};

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
	/// Discard ratio threshold for triggering `VLog` garbage collection (0.0 -
	/// 1.0) Default: 0.5 (50% discardable data triggers GC)
	pub vlog_gc_discard_ratio: f64,
	/// If value size is less than this, it will be stored inline in `SSTable`
	pub vlog_value_threshold: usize,

	// Versioned query configuration
	/// If true, enables versioned queries with timestamp tracking
	pub enable_versioning: bool,
	/// If true, creates a B+tree index for timestamp-based queries.
	/// Requires enable_versioning to be true.
	/// When false, versioned queries will scan the LSM tree directly.
	pub enable_versioned_index: bool,
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
			block_cache: Arc::new(cache::BlockCache::with_capacity_bytes(1 << 20)), // 1MB cache
			path: PathBuf::from(""),
			level_count: 6,
			max_memtable_size: 100 * 1024 * 1024,  // 100 MB
			index_partition_size: 16384,           // 16KB
			vlog_max_file_size: 256 * 1024 * 1024, // 256MB
			vlog_checksum_verification: VLogChecksumLevel::Disabled,
			enable_vlog: false,
			vlog_gc_discard_ratio: 0.5, // 50% default
			vlog_value_threshold: 1024, // 1KB default
			enable_versioning: false,
			enable_versioned_index: false,
			versioned_history_retention_ns: 0, // No retention limit by default
			clock,
			flush_on_close: true,
			wal_recovery_mode: WalRecoveryMode::default(),
			level0_max_files: 4,
			max_bytes_for_level: 256 * 1024 * 1024, // 256MB
			level_multiplier: 10.0,
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
		self.internal_comparator = Arc::new(InternalKeyComparator::new(Arc::clone(&value)));
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

	/// Sets the VLog value threshold in bytes.
	///
	/// Values smaller than this threshold are stored inline in SSTables.
	/// Values larger than or equal to this threshold are stored in VLog files.
	///
	/// Default: 4096 (4KB)
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

	/// Enables or disables versioned queries with timestamp tracking
	/// When enabled, automatically configures VLog and value threshold for
	/// optimal versioned query support
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

	/// Enables or disables the B+tree versioned index for timestamp-based queries.
	/// When disabled, versioned queries will scan the LSM tree directly.
	/// Requires `enable_versioning` to be true for this to have any effect.
	pub const fn with_versioned_index(mut self, value: bool) -> Self {
		self.enable_versioned_index = value;
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
	/// This should be called when the store starts to catch configuration
	/// errors early
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

			// Versioned queries don't work well with value threshold (values should go to
			// VLog)
			if self.vlog_value_threshold > 0 {
				return Err(Error::InvalidArgument(
					"Versioned queries require all values to be stored in VLog. Set vlog_value_threshold to 0.".to_string(),
				));
			}
		}

		// Validate versioned index requires versioning to be enabled
		if self.enable_versioned_index && !self.enable_versioning {
			return Err(Error::InvalidArgument(
				"Versioned index requires versioning to be enabled. Call with_versioning(true, retention_ns) first.".to_string(),
			));
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

impl TryFrom<u8> for CompressionType {
	type Error = Error;

	fn try_from(byte: u8) -> Result<Self> {
		match byte {
			0 => Ok(Self::None),
			1 => Ok(Self::SnappyCompression),
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
			INTERNAL_KEY_TIMESTAMP_MAX,
		)),
		Bound::Excluded(key) => {
			Bound::Excluded(InternalKey::new(key.into_bytes(), 0, InternalKeyKind::Set, 0))
		}
	};

	let end_bound = match upper {
		Bound::Unbounded => Bound::Unbounded,
		Bound::Included(key) => {
			Bound::Included(InternalKey::new(key.into_bytes(), 0, InternalKeyKind::Set, 0))
		}
		Bound::Excluded(key) => Bound::Excluded(InternalKey::new(
			key.into_bytes(),
			INTERNAL_KEY_SEQ_NUM_MAX,
			InternalKeyKind::Max,
			INTERNAL_KEY_TIMESTAMP_MAX,
		)),
	};

	(start_bound, end_bound)
}

// This is the maximum valid sequence number that can be stored in the upper 56
// bits of a 64-bit integer. 1 << 56 shifts the number 1 left by 56 bits,
// resulting in a binary number with a 1 followed by 56 zeros. Subtracting 1
// gives a binary number with 56 ones, which is the maximum value for 56 bits.
pub(crate) const INTERNAL_KEY_SEQ_NUM_MAX: u64 = (1 << 56) - 1;
pub(crate) const INTERNAL_KEY_TIMESTAMP_MAX: u64 = u64::MAX;

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
pub struct InternalKey {
	/// The application's key bytes.
	pub user_key: Key,
	/// System time in nanoseconds since epoch.
	pub timestamp: u64,
	/// Trailer containing (seq_num << 8) | kind.
	pub trailer: u64,
}

impl InternalKey {
	pub(crate) fn new(user_key: Key, seq_num: u64, kind: InternalKeyKind, timestamp: u64) -> Self {
		Self {
			user_key,
			timestamp,
			trailer: (seq_num << 8) | kind as u64,
		}
	}

	pub(crate) fn size(&self) -> usize {
		self.user_key.len() + 16 // 8 bytes for timestamp + 8 bytes for trailer
	}

	pub(crate) fn decode(encoded_key: &[u8]) -> Self {
		let n = encoded_key.len() - 16; // 8 bytes for timestamp + 8 bytes for trailer
		let trailer = read_u64_be(encoded_key, n);
		let timestamp = read_u64_be(encoded_key, n + 8);
		let user_key = encoded_key[..n].to_vec();

		Self {
			user_key,
			timestamp,
			trailer,
		}
	}

	/// Extract user key slice without allocation
	#[inline]
	pub(crate) fn user_key_from_encoded(encoded: &[u8]) -> &[u8] {
		&encoded[..encoded.len() - 16]
	}

	/// Extract trailer (seq_num + kind) without allocation
	#[inline]
	pub(crate) fn trailer_from_encoded(encoded: &[u8]) -> u64 {
		let n = encoded.len() - 16;
		read_u64_be(encoded, n)
	}

	/// Extract seq_num from encoded key without allocation
	#[inline]
	pub(crate) fn seq_num_from_encoded(encoded: &[u8]) -> u64 {
		trailer_to_seq_num(Self::trailer_from_encoded(encoded))
	}

	pub(crate) fn encode(&self) -> Vec<u8> {
		let mut buf = self.user_key.clone();
		buf.extend_from_slice(&self.trailer.to_be_bytes());
		buf.extend_from_slice(&self.timestamp.to_be_bytes());
		buf
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

	pub(crate) fn is_hard_delete_marker(&self) -> bool {
		is_hard_delete_marker(self.kind())
	}

	pub(crate) fn is_replace(&self) -> bool {
		is_replace_kind(self.kind())
	}

	/// Compares this key with another key using timestamp-based ordering
	/// First compares by user key, then by timestamp (descending - newer
	/// timestamps first, matching LSM seq_num ordering)
	pub(crate) fn cmp_by_timestamp(&self, other: &Self) -> Ordering {
		// First compare by user key (ascending)
		match self.user_key.cmp(&other.user_key) {
			// If user keys are equal, compare by timestamp (descending - newer timestamps first)
			Ordering::Equal => other.timestamp.cmp(&self.timestamp),
			ordering => ordering,
		}
	}
}

// Used only by memtable, sstable uses internal key comparator
impl Ord for InternalKey {
	fn cmp(&self, other: &Self) -> Ordering {
		match self.user_key.cmp(&other.user_key) {
			Ordering::Equal => match other.seq_num().cmp(&self.seq_num()) {
				Ordering::Equal => match self.kind().cmp(&other.kind()) {
					Ordering::Equal => other.timestamp.cmp(&self.timestamp), // DESC for timestamp
					ord => ord,
				},
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
}

impl<'a> InternalKeyRef<'a> {
	#[inline]
	pub fn from_encoded(encoded: &'a [u8]) -> Self {
		debug_assert!(encoded.len() >= 16);
		Self {
			encoded,
		}
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

	#[inline]
	pub fn timestamp(&self) -> u64 {
		let n = self.encoded.len() - 8;
		read_u64_be(self.encoded, n)
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

	pub fn to_owned(self) -> InternalKey {
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
///     let value = iter.value_owned()?;
///     iter.next()?;
/// }
/// ```
pub trait InternalIterator {
	/// Seek to first key >= target user key. Returns Ok(true) if valid.
	///
	/// For transaction-level iterators, the target is a raw user key which
	/// will be encoded internally to position at the newest version.
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
	/// VLog-encoded. Use `value_owned()` for resolved values.
	fn value(&self) -> Result<&[u8]>;

	/// Get current value as owned bytes with VLog resolution.
	///
	/// This method resolves VLog pointers to actual values, returning owned data.
	/// For iterators without VLog (internal iterators), this clones the raw bytes.
	///
	/// Default implementation clones raw bytes from `value()`.
	fn value_owned(&self) -> Result<Value> {
		Ok(self.value()?.to_vec())
	}
}
