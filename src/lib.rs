mod batch;
pub mod bplustree;
mod cache;
mod checkpoint;
mod clock;
mod commit;
mod compaction;
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

use crate::clock::{DefaultLogicalClock, LogicalClock};
pub use crate::error::{Error, Result};
pub use crate::lsm::{Tree, TreeBuilder};
use crate::sstable::{InternalKey, INTERNAL_KEY_TIMESTAMP_MAX};
pub use crate::transaction::{Durability, Mode, ReadOptions, Transaction, WriteOptions};

use bytes::Bytes;
use sstable::{bloom::LevelDBBloomFilter, INTERNAL_KEY_SEQ_NUM_MAX};
use std::{borrow::Cow, cmp::Ordering, path::PathBuf, sync::Arc};

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

#[derive(Clone)]
pub struct Options {
	pub block_size: usize,
	pub block_restart_interval: usize,
	pub filter_policy: Option<Arc<dyn FilterPolicy>>,
	pub comparator: Arc<dyn Comparator>,
	pub compression: CompressionType,
	block_cache: Arc<cache::BlockCache>,
	vlog_cache: Arc<cache::VLogCache>,
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
			compression: CompressionType::None,
			filter_policy: Some(Arc::new(bf)),
			block_cache: Arc::new(cache::BlockCache::with_capacity_bytes(1 << 20)),
			vlog_cache: Arc::new(cache::VLogCache::with_capacity_bytes(1 << 20)),
			path: PathBuf::from(""),
			level_count: 1,
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

	pub const fn with_compression(mut self, value: CompressionType) -> Self {
		self.compression = value;
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

	// Method to set block_cache capacity
	pub fn with_block_cache_capacity(mut self, capacity_bytes: u64) -> Self {
		self.block_cache = Arc::new(cache::BlockCache::with_capacity_bytes(capacity_bytes));
		self
	}

	pub fn with_vlog_cache_capacity(mut self, capacity_bytes: u64) -> Self {
		self.vlog_cache = Arc::new(cache::VLogCache::with_capacity_bytes(capacity_bytes));
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

/// A trait for comparing keys in a key-value store.
///
/// This trait defines methods for comparing keys, generating separator keys, generating successor keys,
/// and retrieving the name of the comparator.
pub trait Comparator: Send + Sync {
	/// Compares two keys `a` and `b`.
	fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;

	/// Generates a separator key between two keys `from` and `to`.
	///
	/// This method should return a key that is greater than or equal to `from` and less than `to`.
	/// It is used to optimize the storage layout by finding a midpoint key.
	fn separator(&self, from: &[u8], to: &[u8]) -> Vec<u8>;

	/// Generates the successor key of a given key.
	///
	/// This method should return a key that is lexicographically greater than the given key.
	/// It is used to find the next key in the key space.
	fn successor(&self, key: &[u8]) -> Vec<u8>;

	/// Retrieves the name of the comparator.
	fn name(&self) -> &str;
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

#[derive(Default, Clone, Copy)]
pub struct BytewiseComparator {}

impl BytewiseComparator {}

impl Comparator for BytewiseComparator {
	#[inline]
	fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
		a.cmp(b)
	}

	#[inline]
	fn name(&self) -> &'static str {
		"leveldb.BytewiseComparator"
	}

	#[inline]
	/// Generates a separator key between two byte slices `a` and `b`.
	///
	/// This function uses a three-tier approach like `LevelDB`:
	/// 1. Find first differing byte and try to increment
	/// 2. If that fails, work backwards from end of `a` trying to increment non-0xff bytes
	/// 3. Final fallback: append a 0 byte to make it longer than `a`
	fn separator(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
		if a == b {
			return a.to_vec();
		}

		let min_size = std::cmp::min(a.len(), b.len());
		let mut diff_index = 0;

		// Find first differing position
		while diff_index < min_size && a[diff_index] == b[diff_index] {
			diff_index += 1;
		}

		// Primary: Try to find a short separator at first difference
		while diff_index < min_size {
			let diff = a[diff_index];
			if diff < 0xff && diff + 1 < b[diff_index] {
				let mut sep = Vec::from(&a[0..=diff_index]);
				sep[diff_index] += 1;
				debug_assert!(self.compare(&sep, b) == Ordering::Less);
				return sep;
			}
			diff_index += 1;
		}

		// Secondary: Work backwards from end of `a`, try incrementing non-0xff bytes
		let mut sep = Vec::with_capacity(a.len() + 1);
		sep.extend_from_slice(a);

		if !a.is_empty() {
			let mut i = a.len() - 1;
			while i > 0 && sep[i] == 0xff {
				i -= 1;
			}
			if sep[i] < 0xff {
				sep[i] += 1;
				if self.compare(&sep, b) == Ordering::Less {
					return sep;
				}
				sep[i] -= 1; // revert the change
			}
		}

		// Final backup: Append a 0 byte to make it longer than `a`
		sep.extend_from_slice(&[0]);
		sep
	}

	#[inline]
	/// Generates the successor key of a given byte slice `key`.
	///
	/// This function finds the first non-0xff byte, increments it, and truncates the result.
	/// If all bytes are 0xff, it appends a 0xff byte.
	fn successor(&self, key: &[u8]) -> Vec<u8> {
		let mut result = key.to_vec();
		for i in 0..key.len() {
			if key[i] != 0xff {
				result[i] += 1;
				result.resize(i + 1, 0); // truncate and zero-fill
				return result;
			}
		}
		// Rare path: all bytes are 0xff
		result.push(0xff);
		result
	}
}

#[derive(Clone)]
pub(crate) struct InternalKeyComparator {
	user_comparator: Arc<dyn Comparator>,
}

impl InternalKeyComparator {
	pub fn new(user_comparator: Arc<dyn Comparator>) -> Self {
		Self {
			user_comparator,
		}
	}
}

impl Comparator for InternalKeyComparator {
	fn name(&self) -> &'static str {
		"surrealkv.InternalKeyComparator"
	}

	fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
		// Decode internal keys using InternalKey
		let key_a = InternalKey::decode(a);
		let key_b = InternalKey::decode(b);
		key_a.cmp(&key_b)
	}

	fn separator(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
		if a == b {
			return a.to_vec();
		}

		// Decode keys using InternalKey
		let key_a = InternalKey::decode(a);
		let key_b = InternalKey::decode(b);

		// If user keys are different, compute a separator for user keys
		if self.user_comparator.compare(key_a.user_key.as_ref(), key_b.user_key.as_ref())
			!= Ordering::Equal
		{
			let sep =
				self.user_comparator.separator(key_a.user_key.as_ref(), key_b.user_key.as_ref());

			// Use MAX_SEQUENCE_NUMBER when separator is shorter than original
			if sep.len() < key_a.user_key.len()
				&& self.user_comparator.compare(key_a.user_key.as_ref(), &sep) == Ordering::Less
			{
				let result = InternalKey::new(
					sep,
					INTERNAL_KEY_SEQ_NUM_MAX,
					key_a.kind(),
					INTERNAL_KEY_TIMESTAMP_MAX,
				);
				return result.encode();
			}

			// Otherwise use original sequence number
			let result = InternalKey::new(sep, key_a.seq_num(), key_a.kind(), key_a.timestamp);
			return result.encode();
		}

		// User keys are the same, just return a
		a.to_vec()
	}

	fn successor(&self, key: &[u8]) -> Vec<u8> {
		let internal_key = InternalKey::decode(key);
		let user_key_succ = self.user_comparator.successor(internal_key.user_key.as_ref());

		// Create a new internal key with the successor user key and original sequence/kind
		let result = InternalKey::new(
			user_key_succ,
			internal_key.seq_num(),
			internal_key.kind(),
			internal_key.timestamp,
		);
		result.encode()
	}
}

/// A comparator that compares internal keys first by user key, then by timestamp
/// This is used for versioned queries where we want to order by user key and timestamp
#[derive(Clone)]
pub(crate) struct TimestampComparator {
	user_comparator: Arc<dyn Comparator>,
}

impl Comparator for TimestampComparator {
	fn name(&self) -> &'static str {
		"surrealkv.TimestampComparator"
	}

	fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
		// Decode internal keys using InternalKey
		let key_a = InternalKey::decode(a);
		let key_b = InternalKey::decode(b);
		// Use the timestamp-based comparison method
		key_a.cmp_by_timestamp(&key_b)
	}

	fn separator(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
		if a == b {
			return a.to_vec();
		}

		// Decode keys using InternalKey
		let key_a = InternalKey::decode(a);
		let key_b = InternalKey::decode(b);

		// If user keys are different, compute a separator for user keys
		if self.user_comparator.compare(key_a.user_key.as_ref(), key_b.user_key.as_ref())
			!= Ordering::Equal
		{
			let sep =
				self.user_comparator.separator(key_a.user_key.as_ref(), key_b.user_key.as_ref());

			// Use MAX_TIMESTAMP when separator is shorter than original
			if sep.len() < key_a.user_key.len()
				&& self.user_comparator.compare(key_a.user_key.as_ref(), &sep) == Ordering::Less
			{
				let result = InternalKey::new(
					sep,
					INTERNAL_KEY_SEQ_NUM_MAX,
					key_a.kind(),
					INTERNAL_KEY_TIMESTAMP_MAX,
				);
				return result.encode();
			}

			// Otherwise use original timestamp
			let result = InternalKey::new(sep, key_a.seq_num(), key_a.kind(), key_a.timestamp);
			return result.encode();
		}

		// Can't create a meaningful separator, return a
		a.to_vec()
	}

	fn successor(&self, key: &[u8]) -> Vec<u8> {
		let internal_key = InternalKey::decode(key);
		let user_key_succ = self.user_comparator.successor(internal_key.user_key.as_ref());

		// Create a new internal key with the successor user key and original timestamp/kind
		let result = InternalKey::new(
			user_key_succ,
			internal_key.seq_num(),
			internal_key.kind(),
			internal_key.timestamp,
		);
		result.encode()
	}
}
