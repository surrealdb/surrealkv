pub mod batch;
mod cache;
mod checkpoint;
mod commit;
mod compaction;
mod discard;
mod error;
mod iter;
mod levels;
mod lsm;
pub mod memtable;
mod oracle;
mod snapshot;
pub mod sstable;
mod task;
mod transaction;
mod vfs;
pub mod vlog;
mod wal;

pub use crate::error::{Error, Result};
pub use crate::lsm::Tree;
pub use crate::transaction::{Durability, Mode, Transaction};

use sstable::{bloom::LevelDBBloomFilter, InternalKey, INTERNAL_KEY_SEQ_NUM_MAX};
use std::{cmp::Ordering, path::PathBuf, sync::Arc};

/// Type alias for iterator results containing key-value pairs
/// Value is optional to support keys-only iteration without allocating empty values
pub type IterResult = Result<(Key, Option<Value>)>;

/// The Key type used throughout the LSM tree
pub type Key = Arc<[u8]>;

/// The Value type used throughout the LSM tree  
pub type Value = Arc<[u8]>;

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
	pub max_open_files: usize,
	pub block_size: usize,
	pub block_restart_interval: usize,
	pub filter_policy: Option<Arc<dyn FilterPolicy>>,
	pub comparator: Arc<dyn Comparator>,
	pub compression: CompressionType,
	pub block_cache: Arc<cache::BlockCache>,
	pub vlog_cache: Arc<cache::VLogCache>,
	pub path: PathBuf,
	pub level_count: u8,
	pub max_memtable_size: usize,
	pub index_partition_size: usize,

	// VLog configuration
	pub vlog_max_file_size: u64,
	pub vlog_checksum_verification: VLogChecksumLevel,
	/// If true, disables VLog creation entirely
	pub disable_vlog: bool,
	/// Discard ratio threshold for triggering VLog garbage collection (0.0 - 1.0)
	/// Default: 0.5 (50% discardable data triggers GC)
	pub vlog_gc_discard_ratio: f64,
	/// If value size is less than this, it will be stored inline in SSTable
	pub vlog_value_threshold: usize,
}

impl Default for Options {
	fn default() -> Self {
		let bf = LevelDBBloomFilter::new(10);

		Options {
			max_open_files: 1000,
			block_size: 1 << 10,
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
			vlog_max_file_size: 128 * 1024 * 1024, // 128MB
			vlog_checksum_verification: VLogChecksumLevel::Disabled,
			disable_vlog: false,
			vlog_gc_discard_ratio: 0.5, // 50% default
			vlog_value_threshold: 4096, // 4KB default
		}
	}
}

impl Options {
	pub fn new() -> Self {
		Options::default()
	}

	pub fn with_max_open_files(mut self, value: usize) -> Self {
		self.max_open_files = value;
		self
	}

	pub fn with_block_size(mut self, value: usize) -> Self {
		self.block_size = value;
		self
	}

	pub fn with_block_restart_interval(mut self, value: usize) -> Self {
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

	pub fn with_compression(mut self, value: CompressionType) -> Self {
		self.compression = value;
		self
	}

	pub fn with_block_cache(mut self, value: Arc<cache::BlockCache>) -> Self {
		self.block_cache = value;
		self
	}

	pub fn with_vlog_cache(mut self, value: Arc<cache::VLogCache>) -> Self {
		self.vlog_cache = value;
		self
	}

	pub fn with_path(mut self, value: PathBuf) -> Self {
		self.path = value;
		self
	}

	pub fn with_level_count(mut self, value: u8) -> Self {
		self.level_count = value;
		self
	}

	pub fn with_max_memtable_size(mut self, value: usize) -> Self {
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
	pub fn with_index_partition_size(mut self, size: usize) -> Self {
		self.index_partition_size = size;
		self
	}

	pub fn with_vlog_max_file_size(mut self, value: u64) -> Self {
		self.vlog_max_file_size = value;
		self
	}

	pub fn with_vlog_checksum_verification(mut self, value: VLogChecksumLevel) -> Self {
		self.vlog_checksum_verification = value;
		self
	}

	pub fn with_disable_vlog(mut self, value: bool) -> Self {
		self.disable_vlog = value;
		self
	}

	pub fn with_vlog_gc_discard_ratio(mut self, value: f64) -> Self {
		assert!((0.0..=1.0).contains(&value), "VLog GC discard ratio must be between 0.0 and 1.0");
		self.vlog_gc_discard_ratio = value;
		self
	}
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum CompressionType {
	None = 0,
	SnappyCompression = 1,
}

impl CompressionType {
	pub fn as_str(&self) -> &'static str {
		match *self {
			CompressionType::None => "none",
			CompressionType::SnappyCompression => "snappy",
		}
	}
}

impl From<u8> for CompressionType {
	fn from(byte: u8) -> Self {
		match byte {
			0 => CompressionType::None,
			1 => CompressionType::SnappyCompression,
			_ => panic!("Unknown compression type"),
		}
	}
}

/// A trait for comparing keys in a key-value store.
///
/// This trait defines methods for comparing keys, generating separator keys, generating successor keys,
/// and retrieving the name of the comparator. It is typically used in key-value stores like LevelDB
/// to define custom ordering and key manipulation logic.
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

pub trait Iterator {
	/// An iterator is either positioned at a key/value pair, or
	/// not valid.  This method returns true iff the iterator is valid.
	fn valid(&self) -> bool;

	/// Position at the first key in the source.  The iterator is Valid()
	/// after this call iff the source is not empty.
	fn seek_to_first(&mut self);

	/// Position at the last key in the source.  The iterator is
	/// Valid() after this call iff the source is not empty.
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

impl BytewiseComparator {
	pub fn new() -> Self {
		Self {}
	}
}

impl Comparator for BytewiseComparator {
	#[inline]
	fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
		a.cmp(b)
	}

	#[inline]
	fn name(&self) -> &str {
		"leveldb.BytewiseComparator"
	}

	#[inline]
	/// Generates a separator key between two byte slices `a` and `b`.
	///
	/// This function uses a three-tier approach like LevelDB:
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
				let mut sep = Vec::from(&a[0..diff_index + 1]);
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
				} else {
					sep[i] -= 1; // revert the change
				}
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
pub struct InternalKeyComparator {
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
	fn name(&self) -> &str {
		"leveldb.InternalKeyComparator"
	}

	fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
		// Decode internal keys
		let key_a = InternalKey::decode(a);
		let key_b = InternalKey::decode(b);

		// First compare by user key (ascending)
		match self.user_comparator.compare(&key_a.user_key, &key_b.user_key) {
			Ordering::Equal => {
				// If user keys are equal, compare by sequence number (descending)
				key_b.seq_num().cmp(&key_a.seq_num())
			}
			ordering => ordering,
		}
	}

	fn separator(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
		if a == b {
			return a.to_vec();
		}

		// Decode keys
		let key_a = InternalKey::decode(a);
		let key_b = InternalKey::decode(b);

		// If user keys are different, compute a separator for user keys
		if self.user_comparator.compare(&key_a.user_key, &key_b.user_key) != Ordering::Equal {
			let sep = self.user_comparator.separator(&key_a.user_key, &key_b.user_key);

			// Use MAX_SEQUENCE_NUMBER when separator is shorter than original
			if sep.len() < key_a.user_key.len()
				&& self.user_comparator.compare(&key_a.user_key, &sep) == Ordering::Less
			{
				let result = InternalKey::new(sep, INTERNAL_KEY_SEQ_NUM_MAX, key_a.kind());
				return result.encode();
			}

			// Otherwise use original sequence number
			let result = InternalKey::new(sep, key_a.seq_num(), key_a.kind());
			return result.encode();
		}

		// User keys are the same, just return a
		a.to_vec()
	}

	fn successor(&self, key: &[u8]) -> Vec<u8> {
		let internal_key = InternalKey::decode(key);
		let user_key_succ = self.user_comparator.successor(&internal_key.user_key);

		// Create a new internal key with the successor user key and original sequence/kind
		let result = InternalKey::new(user_key_succ, internal_key.seq_num(), internal_key.kind());
		result.encode()
	}
}
