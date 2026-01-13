use std::cmp::Ordering;
use std::collections::btree_map::Entry as BTreeEntry;
use std::collections::{btree_map, BTreeMap};
use std::ops::Bound;
use std::sync::Arc;

pub use double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt};

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::lsm::Core;
use crate::snapshot::Snapshot;
use crate::sstable::InternalKeyKind;
use crate::{IntoBytes, IterResult, Key, KeysResult, RangeResult, Value, Version};

/// `Mode` is an enumeration representing the different modes a transaction can
/// have in an MVCC (Multi-Version Concurrency Control) system.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Mode {
	/// `ReadWrite` mode allows the transaction to both read and write data.
	ReadWrite,
	/// `ReadOnly` mode allows the transaction to only read data.
	ReadOnly,
	/// `WriteOnly` mode allows the transaction to only write data.
	WriteOnly,
}

impl Mode {
	/// Checks if this transaction mode permits mutations
	pub(crate) fn mutable(self) -> bool {
		match self {
			Self::ReadWrite => true,
			Self::ReadOnly => false,
			Self::WriteOnly => true,
		}
	}

	/// Checks if this is a write-only transaction
	pub(crate) fn is_write_only(self) -> bool {
		matches!(self, Self::WriteOnly)
	}

	/// Checks if this is a read-only transaction
	pub(crate) fn is_read_only(self) -> bool {
		matches!(self, Self::ReadOnly)
	}
}

#[derive(Default, Debug, Copy, Clone, PartialEq)]
pub enum Durability {
	/// Writes are buffered in OS page cache.
	/// May lose recent commits on power failure but fast.
	#[default]
	Eventual,

	/// Forces fsync() on commit.
	/// Guarantees durability but significantly slower.
	Immediate,
}

/// Options for write operations in transactions.
/// This struct allows configuring various parameters for write operations
/// like set() and delete().
#[derive(Debug, Clone, PartialEq)]
pub struct WriteOptions {
	/// Durability level for the write operation
	pub durability: Durability,
	/// Optional timestamp for the write operation. If None, uses the current
	/// timestamp.
	pub timestamp: Option<u64>,
}

impl Default for WriteOptions {
	fn default() -> Self {
		Self {
			durability: Durability::Eventual,
			timestamp: None,
		}
	}
}

impl WriteOptions {
	/// Creates a new WriteOptions with default values
	pub fn new() -> Self {
		Self::default()
	}

	/// Sets the durability level for write operations
	pub fn with_durability(mut self, durability: Durability) -> Self {
		self.durability = durability;
		self
	}

	/// Sets the timestamp for write operations
	pub fn with_timestamp(mut self, timestamp: Option<u64>) -> Self {
		self.timestamp = timestamp;
		self
	}
}

/// Options for read operations in transactions.
/// This struct allows configuring various parameters for read operations
/// like get(), range(), and keys().
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReadOptions {
	/// Whether to return only keys without values (for range operations)
	pub(crate) keys_only: bool,
	/// Lower bound for iteration (inclusive), None means unbounded
	pub(crate) lower_bound: Option<Vec<u8>>,
	/// Upper bound for iteration (exclusive), None means unbounded
	pub(crate) upper_bound: Option<Vec<u8>>,
	/// Optional timestamp for point-in-time reads. If None, reads the latest
	/// version.
	pub(crate) timestamp: Option<u64>,
}

impl ReadOptions {
	/// Creates a new ReadOptions with default values
	pub fn new() -> Self {
		Self::default()
	}

	/// Sets whether to return only keys without values
	pub fn with_keys_only(mut self, keys_only: bool) -> Self {
		self.keys_only = keys_only;
		self
	}

	pub fn set_iterate_lower_bound(&mut self, bound: Option<Vec<u8>>) {
		self.lower_bound = bound;
	}

	/// Sets the upper bound for iteration (exclusive)
	pub fn set_iterate_upper_bound(&mut self, bound: Option<Vec<u8>>) {
		self.upper_bound = bound;
	}

	/// Sets the iteration bounds
	pub(crate) fn set_iterate_bounds(&mut self, lower: Option<Vec<u8>>, upper: Option<Vec<u8>>) {
		self.lower_bound = lower;
		self.upper_bound = upper;
	}

	/// Sets the timestamp for point-in-time reads
	pub fn with_timestamp(mut self, timestamp: Option<u64>) -> Self {
		self.timestamp = timestamp;
		self
	}
}

// ===== Transaction Implementation =====
/// A transaction in the LSM tree providing ACID guarantees.
pub struct Transaction {
	/// `mode` is the transaction mode. This can be either `ReadWrite`,
	/// `ReadOnly`, or `WriteOnly`.
	mode: Mode,

	/// `durability` is the durability level of the transaction. This is used to
	/// determine how the transaction is committed.
	durability: Durability,

	/// `snapshot` is the snapshot that the transaction is running in. This is a
	/// consistent view of the data at the time the transaction started.
	pub(crate) snapshot: Option<Snapshot>,

	/// `core` is the underlying core for the transaction. This is shared
	/// between transactions.
	pub(crate) core: Arc<Core>,

	/// `write_set` is a map of keys to entries.
	/// These are the changes that the transaction intends to make to the data.
	/// The entries vec is used to keep different values for the same key for
	/// savepoints and rollbacks.
	pub(crate) write_set: BTreeMap<Key, Vec<Entry>>,

	/// `closed` indicates if the transaction is closed. A closed transaction
	/// cannot make any more changes to the data.
	closed: bool,

	/// Tracks when this transaction started for deadlock detection
	pub(crate) start_commit_id: u64,

	/// `savepoints` indicates the current number of stacked savepoints; zero
	/// means none.
	savepoints: u32,

	/// write sequence number is used for real-time ordering of writes within a
	/// transaction.
	write_seqno: u32,
}

impl Transaction {
	/// Bump the write sequence number and return it.
	fn next_write_seqno(&mut self) -> u32 {
		self.write_seqno += 1;
		self.write_seqno
	}

	/// Sets the durability level for this transaction
	pub fn set_durability(&mut self, durability: Durability) {
		self.durability = durability;
	}

	/// Sets the durability level for this transaction
	pub fn with_durability(mut self, durability: Durability) -> Self {
		self.durability = durability;
		self
	}

	/// Prepare a new transaction in the given mode.
	pub(crate) fn new(core: Arc<Core>, mode: Mode) -> Result<Self> {
		let read_ts = core.seq_num();

		let start_commit_id =
			core.oracle.transaction_commit_id.load(std::sync::atomic::Ordering::Acquire);

		// Only register mutable transactions with the oracle for conflict detection
		// Read-only transactions don't need conflict detection since they don't write
		if mode.mutable() {
			core.oracle.register_txn_start(start_commit_id);
		}

		let mut snapshot = None;
		if !mode.is_write_only() {
			snapshot = Some(Snapshot::new(Arc::clone(&core), read_ts));
		}

		Ok(Self {
			mode,
			snapshot,
			core,
			write_set: BTreeMap::new(),
			durability: Durability::Eventual,
			closed: false,
			start_commit_id,
			savepoints: 0,
			write_seqno: 0,
		})
	}

	/// Inserts a key-value pair into the store.
	pub fn set<K, V>(&mut self, key: K, value: V) -> Result<()>
	where
		K: IntoBytes,
		V: IntoBytes,
	{
		self.set_with_options(key, value, &WriteOptions::default())
	}

	/// Inserts a key-value pair at with a specific timestamp.
	pub fn set_at_version<K, V>(&mut self, key: K, value: V, timestamp: u64) -> Result<()>
	where
		K: IntoBytes,
		V: IntoBytes,
	{
		self.set_with_options(key, value, &WriteOptions::default().with_timestamp(Some(timestamp)))
	}

	/// Inserts a key-value pair to the store, with custom write options.
	pub fn set_with_options<K, V>(&mut self, key: K, value: V, options: &WriteOptions) -> Result<()>
	where
		K: IntoBytes,
		V: IntoBytes,
	{
		let write_seqno = self.next_write_seqno();
		let entry = if let Some(timestamp) = options.timestamp {
			Entry::new_with_timestamp(
				key,
				Some(value),
				InternalKeyKind::Set,
				self.savepoints,
				write_seqno,
				timestamp,
			)
		} else {
			Entry::new(key, Some(value), InternalKeyKind::Set, self.savepoints, write_seqno)
		};
		self.write_with_options(entry, options)?;
		Ok(())
	}

	/// Delete all the versions of a key. This is a hard delete.
	pub fn delete<K>(&mut self, key: K) -> Result<()>
	where
		K: IntoBytes,
	{
		self.delete_with_options(key, &WriteOptions::default())
	}

	/// Delete all the versions of a key with custom write options. This is a
	/// hard delete.
	pub fn delete_with_options<K>(&mut self, key: K, options: &WriteOptions) -> Result<()>
	where
		K: IntoBytes,
	{
		let write_seqno = self.next_write_seqno();
		let entry = if let Some(timestamp) = options.timestamp {
			Entry::new_with_timestamp(
				key,
				None::<&[u8]>,
				InternalKeyKind::Delete,
				self.savepoints,
				write_seqno,
				timestamp,
			)
		} else {
			Entry::new(key, None::<&[u8]>, InternalKeyKind::Delete, self.savepoints, write_seqno)
		};
		self.write_with_options(entry, options)?;
		Ok(())
	}

	/// Soft delete a key. This will add a tombstone at the current timestamp.
	pub fn soft_delete<K>(&mut self, key: K) -> Result<()>
	where
		K: IntoBytes,
	{
		self.soft_delete_with_options(key, &WriteOptions::default())
	}

	/// Soft deletes a key at a specific timestamp. This will add a tombstone at
	/// the specified timestamp.
	pub fn soft_delete_at_version<K>(&mut self, key: K, timestamp: u64) -> Result<()>
	where
		K: IntoBytes,
	{
		self.soft_delete_with_options(key, &WriteOptions::default().with_timestamp(Some(timestamp)))
	}

	/// Soft delete a key, with custom write options. This will add a tombstone
	/// at the specified timestamp.
	pub fn soft_delete_with_options<K>(&mut self, key: K, options: &WriteOptions) -> Result<()>
	where
		K: IntoBytes,
	{
		let write_seqno = self.next_write_seqno();
		let entry = if let Some(timestamp) = options.timestamp {
			Entry::new_with_timestamp(
				key,
				None::<&[u8]>,
				InternalKeyKind::SoftDelete,
				self.savepoints,
				write_seqno,
				timestamp,
			)
		} else {
			Entry::new(
				key,
				None::<&[u8]>,
				InternalKeyKind::SoftDelete,
				self.savepoints,
				write_seqno,
			)
		};
		self.write_with_options(entry, options)?;
		Ok(())
	}

	/// Inserts a key-value pairm removing all previous versions.
	pub fn replace<K, V>(&mut self, key: K, value: V) -> Result<()>
	where
		K: IntoBytes,
		V: IntoBytes,
	{
		self.replace_with_options(key, value, &WriteOptions::default())
	}

	/// Inserts a key-value pair, removing all previous versions, with custom
	/// write options.
	pub fn replace_with_options<K, V>(
		&mut self,
		key: K,
		value: V,
		options: &WriteOptions,
	) -> Result<()>
	where
		K: IntoBytes,
		V: IntoBytes,
	{
		let write_seqno = self.next_write_seqno();
		let entry = if let Some(timestamp) = options.timestamp {
			Entry::new_with_timestamp(
				key,
				Some(value),
				InternalKeyKind::Replace,
				self.savepoints,
				write_seqno,
				timestamp,
			)
		} else {
			Entry::new(key, Some(value), InternalKeyKind::Replace, self.savepoints, write_seqno)
		};
		self.write_with_options(entry, options)?;
		Ok(())
	}

	/// Gets a value for a key if it exists.
	pub fn get<K>(&self, key: K) -> Result<Option<Value>>
	where
		K: IntoBytes,
	{
		self.get_with_options(key, &ReadOptions::default())
	}

	/// Gets a value for a key at a specific timestamp.
	pub fn get_at_version<K>(&self, key: K, timestamp: u64) -> Result<Option<Value>>
	where
		K: IntoBytes,
	{
		self.get_at_version_with_options(
			key,
			&ReadOptions::default().with_timestamp(Some(timestamp)),
		)
	}

	/// Gets a value for a key, with custom read options.
	pub fn get_with_options<K>(&self, key: K, _options: &ReadOptions) -> Result<Option<Value>>
	where
		K: IntoBytes,
	{
		// If the transaction is closed, return an error.
		if self.closed {
			return Err(Error::TransactionClosed);
		}
		// If the key is empty, return an error.
		if key.as_slice().is_empty() {
			return Err(Error::EmptyKey);
		}

		// Do not allow reads if it is a write-only transaction
		if self.mode.is_write_only() {
			return Err(Error::TransactionWriteOnly);
		}

		// RYOW semantics: Read your own writes. If the value is in the write set,
		// return it.
		if let Some(last_entry) =
			self.write_set.get(key.as_slice()).and_then(|entries| entries.last())
		{
			// If the entry is a tombstone, return None.
			if last_entry.is_tombstone() {
				return Ok(None);
			}
			if let Some(v) = &last_entry.value {
				return Ok(Some(v.clone()));
			}
			// If the entry has no value, it means the key was deleted in this transaction.
			return Ok(None);
		}

		// The value is not in the write set, so attempt to get it from the snapshot.
		match self.snapshot.as_ref().unwrap().get(key.as_slice())? {
			Some(val) => {
				// Resolve the value reference through VLog if needed
				let resolved_value = self.core.resolve_value(&val.0)?;
				Ok(Some(resolved_value))
			}
			None => Ok(None),
		}
	}

	pub fn get_at_version_with_options<K>(
		&self,
		key: K,
		options: &ReadOptions,
	) -> Result<Option<Value>>
	where
		K: IntoBytes,
	{
		// If the transaction is closed, return an error.
		if self.closed {
			return Err(Error::TransactionClosed);
		}
		// If the key is empty, return an error.
		if key.as_slice().is_empty() {
			return Err(Error::EmptyKey);
		}

		// Do not allow reads if it is a write-only transaction
		if self.mode.is_write_only() {
			return Err(Error::TransactionWriteOnly);
		}

		// If a timestamp is provided, use versioned read
		if let Some(timestamp) = options.timestamp {
			// Check if versioned queries are enabled
			if !self.core.opts.enable_versioning {
				return Err(Error::InvalidArgument("Versioned queries not enabled".to_string()));
			}

			// Query the versioned index through the snapshot
			return match &self.snapshot {
				Some(snapshot) => snapshot.get_at_version(key.as_slice(), timestamp),
				None => Err(Error::NoSnapshot),
			};
		} else {
			return Err(Error::InvalidArgument(
				"Timestamp is required for versioned queries".to_string(),
			));
		}
	}

	/// Counts keys in a range at the current timestamp.
	///
	/// Returns the number of valid (non-deleted) keys in the range [start,
	/// end). The range is inclusive of the start key, but exclusive of the end
	/// key.
	///
	/// This is more efficient than creating an iterator and counting manually,
	/// as it doesn't need to allocate or return the actual keys.
	pub fn count<K>(&self, start: K, end: K) -> Result<usize>
	where
		K: IntoBytes,
	{
		let mut options = ReadOptions::default();
		options.set_iterate_bounds(Some(start.into_bytes()), Some(end.into_bytes()));
		self.count_with_options(&options)
	}

	/// Counts keys in a range at a specific timestamp.
	///
	/// Returns the number of valid (non-deleted) keys in the range [start, end)
	/// as they existed at the specified timestamp.
	/// The range is inclusive of the start key, but exclusive of the end key.
	///
	/// This requires versioning to be enabled in the database options.
	pub fn count_at_version<K>(&self, start: K, end: K, timestamp: u64) -> Result<usize>
	where
		K: IntoBytes,
	{
		let mut options = ReadOptions::default().with_timestamp(Some(timestamp));
		options.set_iterate_bounds(Some(start.into_bytes()), Some(end.into_bytes()));
		self.count_with_options(&options)
	}

	/// Counts keys with custom read options.
	///
	/// Returns the number of valid (non-deleted) keys that match the provided
	/// options. The options can specify:
	/// - Key range bounds (iterate_lower_bound, iterate_upper_bound)
	/// - Timestamp for versioned queries
	///
	/// For versioned queries (when timestamp is specified), this requires
	/// versioning to be enabled in the database options.
	///
	/// This method is optimized to avoid creating full iterators and resolving
	/// values from the value log, making it much faster than manually counting
	/// iterator results.
	pub fn count_with_options(&self, options: &ReadOptions) -> Result<usize> {
		if self.closed {
			return Err(Error::TransactionClosed);
		}
		if self.mode.is_write_only() {
			return Err(Error::TransactionWriteOnly);
		}

		// For versioned queries, use the keys iterator approach
		// (versioned index has different structure)
		if let Some(timestamp) = options.timestamp {
			if !self.core.opts.enable_versioning {
				return Err(Error::InvalidArgument("Versioned queries not enabled".to_string()));
			}

			let start_key = options.lower_bound.clone().unwrap_or_default();
			let end_key = options.upper_bound.clone().unwrap_or_default();
			let keys_iter = self.keys_at_version(start_key, end_key, timestamp)?;
			return Ok(keys_iter.count());
		}

		// Fast path: get count from snapshot without creating iterators
		let mut count = match &self.snapshot {
			Some(snapshot) => {
				let lower = options.lower_bound.as_deref();
				let upper = options.upper_bound.as_deref();
				snapshot.count_in_range(lower, upper)?
			}
			None => return Err(Error::NoSnapshot),
		};

		// Apply write-set adjustments for uncommitted changes in this transaction
		for (key, entries) in &self.write_set {
			// Check if key is in range
			let in_range = {
				let lower_ok =
					options.lower_bound.as_ref().is_none_or(|k| key.as_slice() >= k.as_slice());
				let upper_ok =
					options.upper_bound.as_ref().is_none_or(|k| key.as_slice() < k.as_slice());
				lower_ok && upper_ok
			};
			if in_range {
				if let Some(latest_entry) = entries.last() {
					// Check what the key's state was in the snapshot
					let snapshot_had_key =
						self.snapshot.as_ref().unwrap().get(key.as_ref())?.is_some();

					// Determine current state from write-set
					let write_set_has_key = !latest_entry.is_tombstone();

					// Adjust count based on state transition
					match (snapshot_had_key, write_set_has_key) {
						(false, true) => count += 1,                      // New key added
						(true, false) => count = count.saturating_sub(1), // Key deleted
						_ => {}                                           /* No change (update
						                                                    * or still deleted) */
					}
				}
			}
		}

		Ok(count)
	}

	/// Gets keys in a key range at the current timestamp.
	///
	/// The returned iterator is a double ended iterator
	/// that can be used to iterate over the keys in the
	/// range in both forward and backward directions.
	///
	/// The iterator iterates over all keys in the range,
	/// inclusive of the start key, but not the end key.
	///
	/// This function is faster than `range()` as it doesn't
	/// fetch or resolve values from disk.
	pub fn keys<K>(
		&self,
		start: K,
		end: K,
	) -> Result<impl DoubleEndedIterator<Item = KeysResult> + '_>
	where
		K: IntoBytes,
	{
		let mut options = ReadOptions::default().with_keys_only(true);
		options.set_iterate_bounds(Some(start.into_bytes()), Some(end.into_bytes()));
		self.keys_with_options(&options)
	}

	/// Gets keys in a key range at a specific timestamp.
	///
	/// The returned iterator is a double ended iterator
	/// that can be used to iterate over the keys in the
	/// range in both forward and backward directions.
	///
	/// The iterator iterates over all keys in the range,
	/// inclusive of the start key, but not the end key.
	///
	/// This function is faster than `range()` as it doesn't
	/// fetch or resolve values from disk.
	pub fn keys_at_version<K>(
		&self,
		start: K,
		end: K,
		timestamp: u64,
	) -> Result<impl DoubleEndedIterator<Item = KeysResult> + '_>
	where
		K: IntoBytes,
	{
		let mut options =
			ReadOptions::default().with_keys_only(true).with_timestamp(Some(timestamp));
		options.set_iterate_bounds(Some(start.into_bytes()), Some(end.into_bytes()));
		self.keys_at_version_with_options(&options)
	}

	/// Gets keys in a key range, with custom read options.
	///
	/// The returned iterator is a double ended iterator
	/// that can be used to iterate over the keys in the
	/// range in both forward and backward directions.
	///
	/// The iterator iterates over all keys in the range,
	/// inclusive of the start key, but not the end key.
	///
	/// This function is faster than `range()` as it doesn't
	/// fetch or resolve values from disk.
	pub fn keys_with_options(
		&self,
		options: &ReadOptions,
	) -> Result<Box<dyn DoubleEndedIterator<Item = KeysResult> + '_>> {
		// Force keys_only to true for this method
		let options = options.clone().with_keys_only(true);
		let start_key = options.lower_bound.clone().unwrap_or_default();
		let end_key = options.upper_bound.clone().unwrap_or_default();
		Ok(Box::new(
			TransactionRangeIterator::new_with_options(self, start_key, end_key, &options)?
				.map(|result| result.map(|(key, _)| key)),
		))
	}

	pub fn keys_at_version_with_options(
		&self,
		options: &ReadOptions,
	) -> Result<Box<dyn DoubleEndedIterator<Item = KeysResult> + '_>> {
		if let Some(timestamp) = options.timestamp {
			// Check if versioned queries are enabled
			if !self.core.opts.enable_versioning {
				return Err(Error::InvalidArgument("Versioned queries not enabled".to_string()));
			}

			// Get the start and end keys from options
			let start_key = options.lower_bound.clone().unwrap_or_default();
			let end_key = options.upper_bound.clone().unwrap_or_default();

			// Query the versioned index through the snapshot
			match &self.snapshot {
				Some(snapshot) => {
					Ok(Box::new(snapshot.keys_at_version(start_key, end_key, timestamp)?.map(Ok)))
				}
				None => Err(Error::NoSnapshot),
			}
		} else {
			return Err(Error::InvalidArgument(
				"Timestamp is required for versioned queries".to_string(),
			));
		}
	}

	/// Gets keys and values in a range, at the current timestamp.
	///
	/// The returned iterator is a double ended iterator
	/// that can be used to iterate over the keys and values
	/// in the range in both forward and backward directions.
	///
	/// The iterator iterates over all keys and values in the
	/// range, inclusive of the start key, but not the end key.
	pub fn range<K>(
		&self,
		start: K,
		end: K,
	) -> Result<impl DoubleEndedIterator<Item = RangeResult> + '_>
	where
		K: IntoBytes,
	{
		let mut options = ReadOptions::default();
		options.set_iterate_bounds(Some(start.into_bytes()), Some(end.into_bytes()));
		self.range_with_options(&options)
	}

	/// Gets keys and values in a range, at a specific timestamp.
	///
	/// The returned iterator is a double ended iterator
	/// that can be used to iterate over the keys and values
	/// in the range in both forward and backward directions.
	///
	/// The iterator iterates over all keys and values in the
	/// range, inclusive of the start key, but not the end key.
	pub fn range_at_version<K>(
		&self,
		start: K,
		end: K,
		timestamp: u64,
	) -> Result<impl DoubleEndedIterator<Item = RangeResult> + '_>
	where
		K: IntoBytes,
	{
		let mut options = ReadOptions::default().with_timestamp(Some(timestamp));
		options.set_iterate_bounds(Some(start.into_bytes()), Some(end.into_bytes()));
		self.range_at_version_with_options(&options)
	}

	/// Gets keys and values in a range, with custom read options.
	///
	/// The returned iterator is a double ended iterator
	/// that can be used to iterate over the keys and values
	/// in the range in both forward and backward directions.
	///
	/// The iterator iterates over all keys and values in the
	/// range, inclusive of the start key, but not the end key.
	pub fn range_with_options(
		&self,
		options: &ReadOptions,
	) -> Result<Box<dyn DoubleEndedIterator<Item = RangeResult> + '_>> {
		let start_key = options.lower_bound.clone().unwrap_or_default();
		let end_key = options.upper_bound.clone().unwrap_or_default();
		Ok(Box::new(
			TransactionRangeIterator::new_with_options(self, start_key, end_key, options)?.map(
				|result| {
					result.and_then(|(k, v)| {
						v.ok_or_else(|| {
							Error::InvalidArgument("Expected value for range query".to_string())
						})
						.map(|value| (k, value))
					})
				},
			),
		))
	}

	pub fn range_at_version_with_options(
		&self,
		options: &ReadOptions,
	) -> Result<Box<dyn DoubleEndedIterator<Item = RangeResult> + '_>> {
		if let Some(timestamp) = options.timestamp {
			// Check if versioned queries are enabled
			if !self.core.opts.enable_versioning {
				return Err(Error::InvalidArgument("Versioned queries not enabled".to_string()));
			}

			// Get the start and end keys from options
			let start_key = options.lower_bound.clone().unwrap_or_default();
			let end_key = options.upper_bound.clone().unwrap_or_default();

			// Query the versioned index through the snapshot
			match &self.snapshot {
				Some(snapshot) => {
					Ok(Box::new(snapshot.range_at_version(start_key, end_key, timestamp)?))
				}
				None => Err(Error::NoSnapshot),
			}
		} else {
			return Err(Error::InvalidArgument(
				"Timestamp is required for versioned queries".to_string(),
			));
		}
	}

	/// Gets all versions of keys in a range.
	///
	/// Returns all historical versions of keys within the specified range,
	/// including tombstones. Range is [start, end) - start is inclusive, end
	/// is exclusive.
	///
	/// # Arguments
	/// * `start` - Start key (inclusive)
	/// * `end` - End key (exclusive)
	/// * `limit` - Optional maximum number of versions to return. If None, returns all versions.
	///
	/// # Returns
	/// A vector of tuples containing (Key, Value, Version, is_tombstone) for
	/// each version found.
	pub fn scan_all_versions<K>(
		&self,
		start: K,
		end: K,
		limit: Option<usize>,
	) -> Result<Vec<(Key, Value, Version, bool)>>
	where
		K: IntoBytes,
	{
		if self.closed {
			return Err(Error::TransactionClosed);
		}
		if self.mode.is_write_only() {
			return Err(Error::TransactionWriteOnly);
		}

		// Check if versioned queries are enabled
		if !self.core.opts.enable_versioning {
			return Err(Error::InvalidArgument("Versioned queries not enabled".to_string()));
		}

		// Query the versioned index through the snapshot
		match &self.snapshot {
			Some(snapshot) => snapshot.scan_all_versions(start, end, limit),
			None => Err(Error::NoSnapshot),
		}
	}

	/// Writes a value for a key with custom write options. None is used for
	/// deletion.
	fn write_with_options(&mut self, e: Entry, options: &WriteOptions) -> Result<()> {
		// If the transaction mode is not mutable (i.e., it's read-only), return an
		// error.
		if !self.mode.mutable() {
			return Err(Error::TransactionReadOnly);
		}
		// If the transaction is closed, return an error.
		if self.closed {
			return Err(Error::TransactionClosed);
		}
		// If the key is empty, return an error.
		if e.key.is_empty() {
			return Err(Error::EmptyKey);
		}

		self.durability = options.durability;

		// Add the entry to the write set
		let key = e.key.clone();

		match self.write_set.entry(key) {
			BTreeEntry::Occupied(mut oe) => {
				let entries = oe.get_mut();
				// If the latest existing value for this key belongs to the same
				// savepoint as the value we are about to write, then we can
				// overwrite it with the new value (same savepoint = same transaction state).
				// For different savepoints, we add a new entry to support savepoint rollbacks.
				if let Some(last_entry) = entries.last() {
					if last_entry.savepoint_no == e.savepoint_no {
						// Same savepoint - replace the last entry
						*entries.last_mut().unwrap() = e;
					} else {
						// Different savepoint - add new entry
						entries.push(e);
					}
				} else {
					entries.push(e);
				}
			}
			BTreeEntry::Vacant(ve) => {
				ve.insert(vec![e]);
			}
		}

		Ok(())
	}

	/// Commits the transaction, by writing all pending entries to the store.
	pub async fn commit(&mut self) -> Result<()> {
		// If the transaction is closed, return an error.
		if self.closed {
			return Err(Error::TransactionClosed);
		}

		// If the transaction is read-only, return an error.
		if self.mode.is_read_only() {
			return Err(Error::TransactionReadOnly);
		}

		// If there are no pending writes, there's nothing to commit, so return early.
		if self.write_set.is_empty() {
			self.closed = true;
			return Ok(());
		}

		// Prepare for commit - uses the new Oracle method
		let _ = self.core.oracle.prepare_commit(self)?;

		// Unregister the transaction start point
		self.core.oracle.unregister_txn_start(self.start_commit_id);

		// Create and prepare batch directly
		let mut batch = Batch::new(0);

		// Extract the vector of entries for the current transaction,
		// respecting the insertion order recorded with Entry::seqno.
		let mut latest_writes: Vec<Entry> =
			std::mem::take(&mut self.write_set).into_values().flatten().collect();
		latest_writes.sort_by(|a, b| a.seqno.cmp(&b.seqno));

		// Generate a single timestamp for this commit
		let commit_timestamp = self.core.opts.clock.now();

		// Add all entries to the batch
		for entry in latest_writes {
			// Use the entry's timestamp if it was explicitly set (via set_at_version),
			// otherwise use the commit timestamp
			let timestamp = if entry.timestamp != 0 {
				entry.timestamp
			} else {
				commit_timestamp
			};
			batch.add_record(entry.kind, entry.key, entry.value, timestamp)?;
		}

		// Write the batch to storage
		let should_sync = self.durability == Durability::Immediate;
		self.core.commit(batch, should_sync).await?;

		// Mark the transaction as closed
		self.closed = true;
		Ok(())
	}

	/// Rolls back the transaction by removing all updated entries.
	pub fn rollback(&mut self) {
		// Only unregister mutable transactions since only they get registered
		if !self.closed && self.mode.mutable() {
			self.core.oracle.unregister_txn_start(self.start_commit_id);
		}

		self.closed = true;
		self.write_set.clear();
		self.snapshot.take();
		self.savepoints = 0;
		self.write_seqno = 0;
	}

	/// After calling this method the subsequent modifications within this
	/// transaction can be rolled back by calling [`rollback_to_savepoint`].
	///
	/// This method is stackable and can be called multiple times with the
	/// corresponding calls to [`rollback_to_savepoint`].
	///
	/// [`rollback_to_savepoint`]: Transaction::rollback_to_savepoint
	pub fn set_savepoint(&mut self) -> Result<()> {
		// If the transaction mode is not mutable (i.e., it's read-only), return an
		// error.
		if !self.mode.mutable() {
			return Err(Error::TransactionReadOnly);
		}
		// If the transaction is closed, return an error.
		if self.closed {
			return Err(Error::TransactionClosed);
		}

		// Bump the latest savepoint number.
		self.savepoints += 1;

		Ok(())
	}

	/// Rollback the state of the transaction to the latest savepoint set by
	/// calling [`set_savepoint`].
	///
	/// [`set_savepoint`]: Transaction::set_savepoint
	pub fn rollback_to_savepoint(&mut self) -> Result<()> {
		// If the transaction mode is not mutable (i.e., it's read-only), return an
		// error.
		if !self.mode.mutable() {
			return Err(Error::TransactionReadOnly);
		}
		// If the transaction is closed, return an error.
		if self.closed {
			return Err(Error::TransactionClosed);
		}
		// Check that the savepoint is set
		if self.savepoints == 0 {
			return Err(Error::TransactionWithoutSavepoint);
		}

		// For every key in the write set, remove entries marked
		// for rollback since the last call to set_savepoint()
		// from its vec.
		for entries in self.write_set.values_mut() {
			entries.retain(|entry| entry.savepoint_no != self.savepoints);
		}

		// Remove keys with no entries left after the rollback above.
		self.write_set.retain(|_, entries| !entries.is_empty());

		// Decrement the latest savepoint number unless it's zero.
		// Cannot undeflow due to the zero check above.
		self.savepoints -= 1;

		Ok(())
	}
}

impl Drop for Transaction {
	fn drop(&mut self) {
		self.rollback();
	}
}

/// Represents a pending write operation in a transaction's write set
#[derive(Clone)]
pub(crate) struct Entry {
	/// The key being written
	pub(crate) key: Key,

	/// The value (None for deletes)
	pub(crate) value: Option<Value>,

	/// Type of operation (Set, Delete, etc.)
	pub(crate) kind: InternalKeyKind,

	/// Savepoint number when this entry was created
	pub(crate) savepoint_no: u32,

	/// Sequence number for ordering writes within a transaction
	pub(crate) seqno: u32,

	/// Timestamp for versioned queries
	pub(crate) timestamp: u64,
}

impl Entry {
	fn new<K: IntoBytes, V: IntoBytes>(
		key: K,
		value: Option<V>,
		kind: InternalKeyKind,
		savepoint_no: u32,
		seqno: u32,
	) -> Entry {
		Entry {
			key: key.into_bytes(),
			value: value.map(|v| v.into_bytes()),
			kind,
			savepoint_no,
			seqno,
			timestamp: 0, // Will be set at commit time
		}
	}

	fn new_with_timestamp<K: IntoBytes, V: IntoBytes>(
		key: K,
		value: Option<V>,
		kind: InternalKeyKind,
		savepoint_no: u32,
		seqno: u32,
		timestamp: u64,
	) -> Entry {
		Entry {
			key: key.into_bytes(),
			value: value.map(|v| v.into_bytes()),
			kind,
			savepoint_no,
			seqno,
			timestamp,
		}
	}

	/// Checks if this entry represents a deletion (tombstone)
	fn is_tombstone(&self) -> bool {
		let kind = self.kind;
		if kind == InternalKeyKind::Delete
			|| kind == InternalKeyKind::SoftDelete
			|| kind == InternalKeyKind::RangeDelete
		{
			return true;
		}

		false
	}
}

/// An iterator that performs a merging scan over a transaction's snapshot and
/// write set.
pub(crate) struct TransactionRangeIterator<'a> {
	/// Iterator over the consistent snapshot
	snapshot_iter: DoubleEndedPeekable<Box<dyn DoubleEndedIterator<Item = IterResult> + 'a>>,

	/// Iterator over the transaction's write set
	write_set_iter: DoubleEndedPeekable<btree_map::Range<'a, Key, Vec<Entry>>>,

	/// When true, only return keys without fetching values
	keys_only: bool,
}

impl<'a> TransactionRangeIterator<'a> {
	/// Creates a new range iterator with custom read options
	pub(crate) fn new_with_options(
		tx: &'a Transaction,
		start_key: Vec<u8>,
		end_key: Vec<u8>,
		options: &ReadOptions,
	) -> Result<Self> {
		// Validate transaction state
		if tx.closed {
			return Err(Error::TransactionClosed);
		}

		if tx.mode.is_write_only() {
			return Err(Error::TransactionWriteOnly);
		}

		// Get snapshot
		let snapshot = match &tx.snapshot {
			Some(snap) => snap,
			None => return Err(Error::NoSnapshot),
		};

		// Create a snapshot iterator for the range
		let iter = snapshot.range(
			Some(start_key.as_slice()),
			Some(end_key.as_slice()),
			options.keys_only,
		)?;
		let boxed_iter: Box<dyn DoubleEndedIterator<Item = IterResult> + 'a> = Box::new(iter);

		// Use inclusive-exclusive range for write set: [start, end)
		let write_set_range = (Bound::Included(start_key), Bound::Excluded(end_key));
		let write_set_iter = tx.write_set.range(write_set_range);

		Ok(Self {
			snapshot_iter: boxed_iter.double_ended_peekable(),
			write_set_iter: write_set_iter.double_ended_peekable(),
			keys_only: options.keys_only,
		})
	}

	/// Reads from the write set and skips deleted entries
	fn read_from_write_set(&mut self) -> Option<IterResult> {
		if let Some((ws_key, entries)) = self.write_set_iter.next() {
			if let Some(last_entry) = entries.last() {
				if last_entry.is_tombstone() {
					// Deleted key, skip it by recursively getting the next entry
					return self.next();
				}

				if self.keys_only {
					// For keys-only mode, return None for the value to avoid allocations
					return Some(Ok((ws_key.clone(), None)));
				} else if let Some(value) = &last_entry.value {
					return Some(Ok((ws_key.clone(), Some(value.clone()))));
				}
			}
		}
		None
	}

	/// Reads from the write set in reverse order and skips deleted entries
	fn read_from_write_set_back(&mut self) -> Option<IterResult> {
		if let Some((ws_key, entries)) = self.write_set_iter.next_back() {
			if let Some(last_entry) = entries.last() {
				if last_entry.is_tombstone() {
					// Deleted key, skip it by recursively getting the next entry
					return self.next_back();
				}

				if self.keys_only {
					// For keys-only mode, return None for the value to avoid allocations
					return Some(Ok((ws_key.clone(), None)));
				} else if let Some(value) = &last_entry.value {
					return Some(Ok((ws_key.clone(), Some(value.clone()))));
				}
			}
		}
		None
	}
}

impl Iterator for TransactionRangeIterator<'_> {
	type Item = IterResult;

	/// Merges results from write set and snapshot in key order
	fn next(&mut self) -> Option<Self::Item> {
		// Fast path: if write set is empty, just use snapshot
		if self.write_set_iter.peek().is_none() {
			return self.snapshot_iter.next();
		}

		// Fast path: if snapshot is empty, just use write set
		if self.snapshot_iter.peek().is_none() {
			return self.read_from_write_set();
		}

		// Merge results from both iterators
		let has_snap = self.snapshot_iter.peek().is_some();
		let has_ws = self.write_set_iter.peek().is_some();

		let result = match (has_snap, has_ws) {
			(false, false) => None,
			(true, false) => self.snapshot_iter.next(),
			(false, true) => self.read_from_write_set(),
			(true, true) => {
				// Compare keys to determine which comes first
				if let (Some(Ok((snap_key, _))), Some((ws_key, _))) =
					(self.snapshot_iter.peek(), self.write_set_iter.peek())
				{
					match snap_key.cmp(ws_key) {
						Ordering::Less => self.snapshot_iter.next(),
						Ordering::Greater => self.read_from_write_set(),
						Ordering::Equal => {
							// Same key - prioritize write set and skip snapshot
							self.snapshot_iter.next();
							self.read_from_write_set()
						}
					}
				} else if self.snapshot_iter.peek().is_some() {
					// Snapshot has error, propagate it
					self.snapshot_iter.next()
				} else {
					// This should never happen since we checked above
					None
				}
			}
		};

		result
	}
}

impl DoubleEndedIterator for TransactionRangeIterator<'_> {
	/// Merges results from write set and snapshot in reverse key order
	fn next_back(&mut self) -> Option<Self::Item> {
		// Fast path: if write set is empty, just use snapshot
		if self.write_set_iter.peek_back().is_none() {
			return self.snapshot_iter.next_back();
		}

		// Fast path: if snapshot is empty, just use write set
		if self.snapshot_iter.peek_back().is_none() {
			return self.read_from_write_set_back();
		}

		// Merge results from both iterators
		let has_snap = self.snapshot_iter.peek_back().is_some();
		let has_ws = self.write_set_iter.peek_back().is_some();

		let result = match (has_snap, has_ws) {
			(false, false) => None,
			(true, false) => self.snapshot_iter.next_back(),
			(false, true) => self.read_from_write_set_back(),
			(true, true) => {
				// Compare keys to determine which comes last
				if let (Some(Ok((snap_key, _))), Some((ws_key, _))) =
					(self.snapshot_iter.peek_back(), self.write_set_iter.peek_back())
				{
					match snap_key.as_slice().cmp(ws_key.as_slice()) {
						Ordering::Greater => self.snapshot_iter.next_back(),
						Ordering::Less => self.read_from_write_set_back(),
						Ordering::Equal => {
							// Same key - prioritize write set and skip snapshot
							self.snapshot_iter.next_back();
							self.read_from_write_set_back()
						}
					}
				} else if self.snapshot_iter.peek_back().is_some() {
					// Snapshot has error, propagate it
					self.snapshot_iter.next_back()
				} else {
					// This should never happen since we checked above
					None
				}
			}
		};

		result
	}
}
