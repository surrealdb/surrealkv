use std::cmp::Ordering;
use std::collections::{btree_map, btree_map::Entry as BTreeEntry, BTreeMap};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
pub use double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt};

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::lsm::Core;
use crate::snapshot::{Snapshot, VersionScanResult};
use crate::sstable::InternalKeyKind;
use crate::{IterResult, Value};

/// `Mode` is an enumeration representing the different modes a transaction can have in an MVCC (Multi-Version Concurrency Control) system.
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
	pub(crate) fn mutable(&self) -> bool {
		match self {
			Self::ReadWrite => true,
			Self::ReadOnly => false,
			Self::WriteOnly => true,
		}
	}

	/// Checks if this is a write-only transaction
	pub(crate) fn is_write_only(&self) -> bool {
		matches!(self, Self::WriteOnly)
	}

	/// Checks if this is a read-only transaction
	pub(crate) fn is_read_only(&self) -> bool {
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
/// Similar to RocksDB's WriteOptions, this struct allows configuring
/// various parameters for write operations like set() and delete().
#[derive(Debug, Clone, PartialEq)]
pub struct WriteOptions {
	/// Durability level for the write operation
	pub durability: Durability,
}

impl Default for WriteOptions {
	fn default() -> Self {
		Self {
			durability: Durability::Eventual,
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
}

/// Options for read operations in transactions.
/// Similar to RocksDB's ReadOptions, this struct allows configuring
/// various parameters for read operations like get(), range(), and keys().
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReadOptions {
	/// Whether to return only keys without values (for range operations)
	pub keys_only: bool,
	/// Maximum number of items to return (for range operations)
	pub limit: Option<usize>,
	/// Lower bound for iteration (inclusive). If set, iteration will start from this key or later.
	pub iterate_lower_bound: Option<Vec<u8>>,
	/// Upper bound for iteration (exclusive). If set, iteration will stop before this key.
	pub iterate_upper_bound: Option<Vec<u8>>,
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

	/// Sets the maximum number of items to return
	pub fn with_limit(mut self, limit: Option<usize>) -> Self {
		self.limit = limit;
		self
	}

	/// Sets the lower bound for iteration (inclusive)
	pub fn set_iterate_lower_bound(&mut self, bound: Option<Vec<u8>>) {
		self.iterate_lower_bound = bound;
	}

	/// Sets the upper bound for iteration (exclusive)
	pub fn set_iterate_upper_bound(&mut self, bound: Option<Vec<u8>>) {
		self.iterate_upper_bound = bound;
	}

	/// Sets the lower bound for iteration (inclusive) - builder pattern
	pub fn with_iterate_lower_bound(mut self, bound: Option<Vec<u8>>) -> Self {
		self.iterate_lower_bound = bound;
		self
	}

	/// Sets the upper bound for iteration (exclusive) - builder pattern
	pub fn with_iterate_upper_bound(mut self, bound: Option<Vec<u8>>) -> Self {
		self.iterate_upper_bound = bound;
		self
	}
}

// ===== Transaction Implementation =====
/// A transaction in the LSM tree providing ACID guarantees.
pub struct Transaction {
	/// `mode` is the transaction mode. This can be either `ReadWrite`, `ReadOnly`, or `WriteOnly`.
	mode: Mode,

	/// `durability` is the durability level of the transaction. This is used to determine how the transaction is committed.
	durability: Durability,

	/// `snapshot` is the snapshot that the transaction is running in. This is a consistent view of the data at the time the transaction started.
	pub(crate) snapshot: Option<Snapshot>,

	/// `core` is the underlying core for the transaction. This is shared between transactions.
	pub(crate) core: Arc<Core>,

	/// `write_set` is a map of keys to entries.
	/// These are the changes that the transaction intends to make to the data.
	/// The entries vec is used to keep different values for the same key for
	/// savepoints and rollbacks.
	pub(crate) write_set: BTreeMap<Bytes, Vec<Entry>>,

	/// `closed` indicates if the transaction is closed. A closed transaction cannot make any more changes to the data.
	closed: bool,

	/// Tracks when this transaction started for deadlock detection
	pub(crate) start_commit_id: u64,

	/// `savepoints` indicates the current number of stacked savepoints; zero means none.
	savepoints: u32,

	/// write sequence number is used for real-time ordering of writes within a transaction.
	write_seqno: u32,
}

impl Transaction {
	/// Bump the write sequence number and return it.
	fn next_write_seqno(&mut self) -> u32 {
		self.write_seqno += 1;
		self.write_seqno
	}

	pub fn set_durability(&mut self, durability: Durability) {
		self.durability = durability;
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
			snapshot = Some(Snapshot::new(core.clone(), read_ts));
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

	/// Adds a key-value pair to the store.
	pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
		self.set_with_options(key, value, &WriteOptions::default())
	}

	/// Adds a key-value pair to the store with custom write options.
	pub fn set_with_options(
		&mut self,
		key: &[u8],
		value: &[u8],
		options: &WriteOptions,
	) -> Result<()> {
		let write_seqno = self.next_write_seqno();
		let entry =
			Entry::new(key, Some(value), InternalKeyKind::Set, self.savepoints, write_seqno);
		self.write_with_options(entry, options)?;
		Ok(())
	}

	/// Sets a key-value pair with a specific timestamp
	pub fn set_at_ts(&mut self, key: &[u8], value: &[u8], timestamp: u64) -> Result<()> {
		self.set_at_ts_with_options(key, value, timestamp, &WriteOptions::default())
	}

	/// Sets a key-value pair with a specific timestamp and custom write options
	pub fn set_at_ts_with_options(
		&mut self,
		key: &[u8],
		value: &[u8],
		timestamp: u64,
		options: &WriteOptions,
	) -> Result<()> {
		let seqno = self.next_write_seqno();
		self.write_with_options(
			Entry::new_with_timestamp(
				key,
				Some(value),
				InternalKeyKind::Set,
				self.savepoints,
				seqno,
				timestamp,
			),
			options,
		)
	}

	// Delete all the versions of a key. This is a hard delete.
	pub fn delete(&mut self, key: &[u8]) -> Result<()> {
		self.delete_with_options(key, InternalKeyKind::Delete, &WriteOptions::default())
	}

	/// Soft delete a key. The key will exist on disk but never be shown in queries.
	pub fn soft_delete(&mut self, key: &[u8]) -> Result<()> {
		self.delete_with_options(key, InternalKeyKind::SoftDelete, &WriteOptions::default())
	}

	/// Delete all the versions of a key with custom write options. This is a hard delete.
	pub fn delete_with_options(
		&mut self,
		key: &[u8],
		kind: InternalKeyKind,
		options: &WriteOptions,
	) -> Result<()> {
		let write_seqno = self.next_write_seqno();
		let entry = Entry::new(key, None, kind, self.savepoints, write_seqno);
		self.write_with_options(entry, options)?;
		Ok(())
	}

	/// Gets a value for a key if it exists.
	pub fn get(&self, key: &[u8]) -> Result<Option<Value>> {
		self.get_with_options(key, &ReadOptions::default())
	}

	/// Gets a value for a key if it exists with custom read options.
	pub fn get_with_options(&self, key: &[u8], _options: &ReadOptions) -> Result<Option<Value>> {
		// If the transaction is closed, return an error.
		if self.closed {
			return Err(Error::TransactionClosed);
		}
		// If the key is empty, return an error.
		if key.as_ref().is_empty() {
			return Err(Error::EmptyKey);
		}

		// Do not allow reads if it is a write-only transaction
		if self.mode.is_write_only() {
			return Err(Error::TransactionWriteOnly);
		}

		// RYOW semantics: Read your own writes. If the value is in the write set, return it.
		if let Some(last_entry) = self.write_set.get(key).and_then(|entries| entries.last()) {
			// If the entry is a tombstone, return None.
			if last_entry.is_tombstone() {
				return Ok(None);
			}
			if let Some(v) = &last_entry.value {
				return Ok(Some(Arc::from(v.to_vec())));
			}
			// If the entry has no value, it means the key was deleted in this transaction.
			return Ok(None);
		}

		// The value is not in the write set, so attempt to get it from the snapshot.
		match self.snapshot.as_ref().unwrap().get(key)? {
			Some(val) => {
				// Resolve the value reference through VLog if needed
				let resolved_value = self.core.resolve_value(&val.0)?;
				Ok(Some(resolved_value))
			}
			None => Ok(None),
		}
	}

	/// Writes a value for a key. None is used for deletion.
	/// Writes a value for a key with custom write options. None is used for deletion.
	fn write_with_options(&mut self, e: Entry, options: &WriteOptions) -> Result<()> {
		// If the transaction mode is not mutable (i.e., it's read-only), return an error.
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

	/// Creates an iterator for a range scan between start and end keys (inclusive).
	pub fn range<Key: AsRef<[u8]>>(
		&self,
		start: Key,
		end: Key,
		limit: Option<usize>,
	) -> Result<impl DoubleEndedIterator<Item = IterResult> + '_> {
		let mut options = ReadOptions::default().with_limit(limit);
		options.set_iterate_lower_bound(Some(start.as_ref().to_vec()));
		options.set_iterate_upper_bound(Some(end.as_ref().to_vec()));
		self.range_with_options(&options)
	}

	/// Creates an iterator for a range scan with custom read options.
	/// The range bounds are taken from the ReadOptions (iterate_lower_bound and iterate_upper_bound).
	pub fn range_with_options(
		&self,
		options: &ReadOptions,
	) -> Result<impl DoubleEndedIterator<Item = IterResult> + '_> {
		// Get the start and end keys from options
		let start_key = options.iterate_lower_bound.clone().unwrap_or_default();
		let end_key = options.iterate_upper_bound.clone().unwrap_or_default();
		TransactionRangeIterator::new_with_options(self, start_key, end_key, options)
	}

	/// Creates an iterator that returns only keys in the given range.
	/// This is faster than `range()` as it doesn't fetch or resolve values from disk.
	pub fn keys<Key: AsRef<[u8]>>(
		&self,
		start: Key,
		end: Key,
		limit: Option<usize>,
	) -> Result<impl DoubleEndedIterator<Item = IterResult> + '_> {
		let mut options = ReadOptions::default().with_keys_only(true).with_limit(limit);
		options.set_iterate_lower_bound(Some(start.as_ref().to_vec()));
		options.set_iterate_upper_bound(Some(end.as_ref().to_vec()));
		self.keys_with_options(&options)
	}

	/// Creates an iterator that returns only keys with custom read options.
	/// The range bounds are taken from the ReadOptions (iterate_lower_bound and iterate_upper_bound).
	pub fn keys_with_options(
		&self,
		options: &ReadOptions,
	) -> Result<impl DoubleEndedIterator<Item = IterResult> + '_> {
		// Get the start and end keys from options
		let start_key = options.iterate_lower_bound.clone().unwrap_or_default();
		let end_key = options.iterate_upper_bound.clone().unwrap_or_default();

		// Force keys_only to true for this method
		let mut options = options.clone();
		options.keys_only = true;
		TransactionRangeIterator::new_with_options(self, start_key, end_key, &options)
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
		let commit_timestamp =
			SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as u64;

		// Add all entries to the batch
		for entry in latest_writes {
			// Use the entry's timestamp if it was explicitly set (via set_at_ts),
			// otherwise use the commit timestamp
			let timestamp = if entry.timestamp != 0 {
				entry.timestamp
			} else {
				commit_timestamp
			};
			batch.add_record(
				entry.kind,
				&entry.key,
				entry.value.as_ref().map(|bytes| bytes.as_ref()),
				timestamp,
			)?;
		}

		// Write the batch to storage
		let should_sync = self.durability == Durability::Immediate;
		self.core.commit(batch, should_sync).await?;

		// Mark the transaction as closed
		self.closed = true;
		Ok(())
	}

	/// After calling this method the subsequent modifications within this
	/// transaction can be rolled back by calling [`rollback_to_savepoint`].
	///
	/// This method is stackable and can be called multiple times with the
	/// corresponding calls to [`rollback_to_savepoint`].
	///
	/// [`rollback_to_savepoint`]: Transaction::rollback_to_savepoint
	pub fn set_savepoint(&mut self) -> Result<()> {
		// If the transaction mode is not mutable (i.e., it's read-only), return an error.
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
		// If the transaction mode is not mutable (i.e., it's read-only), return an error.
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

	/// Gets a value for a key at a specific timestamp
	pub fn get_at_timestamp(&self, key: &[u8], timestamp: u64) -> Result<Option<Value>> {
		if self.closed {
			return Err(Error::TransactionClosed);
		}
		if key.is_empty() {
			return Err(Error::EmptyKey);
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
			Some(snapshot) => snapshot.get_at_timestamp(key, timestamp),
			None => Err(Error::NoSnapshot),
		}
	}

	/// Gets keys in a key range at a specific timestamp
	pub fn keys_at_timestamp<R: RangeBounds<Vec<u8>>>(
		&self,
		key_range: R,
		timestamp: u64,
		limit: Option<usize>,
	) -> Result<Vec<Vec<u8>>> {
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
			Some(snapshot) => snapshot.keys_at_timestamp(key_range, timestamp, limit),
			None => Err(Error::NoSnapshot),
		}
	}

	/// Scans key-value pairs in a key range at a specific timestamp
	pub fn scan_at_timestamp<R: RangeBounds<Vec<u8>>>(
		&self,
		key_range: R,
		timestamp: u64,
		limit: Option<usize>,
	) -> Result<Vec<(Vec<u8>, Value)>> {
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
			Some(snapshot) => snapshot.scan_at_timestamp(key_range, timestamp, limit),
			None => Err(Error::NoSnapshot),
		}
	}

	/// Gets all versions of keys in a key range
	pub fn scan_all_timestamps<R: RangeBounds<Vec<u8>>>(
		&self,
		key_range: R,
		limit: Option<usize>,
	) -> Result<Vec<VersionScanResult>> {
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
			Some(snapshot) => snapshot.scan_all_timestamps(key_range, limit),
			None => Err(Error::NoSnapshot),
		}
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
	pub(crate) key: Bytes,

	/// The value (None for deletes)
	pub(crate) value: Option<Bytes>,

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
	fn new(
		key: &[u8],
		value: Option<&[u8]>,
		kind: InternalKeyKind,
		savepoint_no: u32,
		seqno: u32,
	) -> Entry {
		Entry {
			key: Bytes::copy_from_slice(key),
			value: value.map(Bytes::copy_from_slice),
			kind,
			savepoint_no,
			seqno,
			timestamp: 0, // Will be set at commit time
		}
	}

	fn new_with_timestamp(
		key: &[u8],
		value: Option<&[u8]>,
		kind: InternalKeyKind,
		savepoint_no: u32,
		seqno: u32,
		timestamp: u64,
	) -> Entry {
		Entry {
			key: Bytes::copy_from_slice(key),
			value: value.map(Bytes::copy_from_slice),
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

/// An iterator that performs a merging scan over a transaction's snapshot and write set.
pub(crate) struct TransactionRangeIterator<'a> {
	/// Iterator over the consistent snapshot
	snapshot_iter: DoubleEndedPeekable<Box<dyn DoubleEndedIterator<Item = IterResult> + 'a>>,

	/// Iterator over the transaction's write set
	write_set_iter: DoubleEndedPeekable<btree_map::Range<'a, Bytes, Vec<Entry>>>,

	/// Maximum number of items to return (usize::MAX for unlimited)
	limit: usize,

	/// Number of items returned so far
	count: usize,

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

		// Convert range bounds to Bytes for the write set range
		let start_bytes = Bytes::copy_from_slice(&start_key);
		let end_bytes = Bytes::copy_from_slice(&end_key);

		// Create a snapshot iterator for the range
		let iter = snapshot.range(start_bytes.clone(), end_bytes.clone(), options.keys_only)?;
		let boxed_iter: Box<dyn DoubleEndedIterator<Item = IterResult> + 'a> = Box::new(iter);

		// Use inclusive range for write set
		let write_set_range = (Bound::Included(start_bytes), Bound::Included(end_bytes));
		let write_set_iter = tx.write_set.range(write_set_range);

		Ok(Self {
			snapshot_iter: boxed_iter.double_ended_peekable(),
			write_set_iter: write_set_iter.double_ended_peekable(),
			limit: options.limit.unwrap_or(usize::MAX),
			count: 0,
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
					return Some(Ok((ws_key.to_vec().into(), None)));
				} else if let Some(value) = &last_entry.value {
					return Some(Ok((ws_key.to_vec().into(), Some(value.to_vec().into()))));
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
					return Some(Ok((ws_key.to_vec().into(), None)));
				} else if let Some(value) = &last_entry.value {
					return Some(Ok((ws_key.to_vec().into(), Some(value.to_vec().into()))));
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
		if self.count >= self.limit {
			return None;
		}

		// Fast path: if write set is empty, just use snapshot
		if self.write_set_iter.peek().is_none() {
			let result = self.snapshot_iter.next();
			if result.is_some() {
				self.count += 1;
			}
			return result;
		}

		// Fast path: if snapshot is empty, just use write set
		if self.snapshot_iter.peek().is_none() {
			let result = self.read_from_write_set();
			if result.is_some() {
				self.count += 1;
			}
			return result;
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
					match snap_key.as_ref().cmp(ws_key.as_ref()) {
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

		if result.is_some() {
			self.count += 1;
		}

		result
	}
}

impl DoubleEndedIterator for TransactionRangeIterator<'_> {
	/// Merges results from write set and snapshot in reverse key order
	fn next_back(&mut self) -> Option<Self::Item> {
		if self.count >= self.limit {
			return None;
		}

		// Fast path: if write set is empty, just use snapshot
		if self.write_set_iter.peek_back().is_none() {
			let result = self.snapshot_iter.next_back();
			if result.is_some() {
				self.count += 1;
			}
			return result;
		}

		// Fast path: if snapshot is empty, just use write set
		if self.snapshot_iter.peek_back().is_none() {
			let result = self.read_from_write_set_back();
			if result.is_some() {
				self.count += 1;
			}
			return result;
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
					match snap_key.as_ref().cmp(ws_key.as_ref()) {
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

		if result.is_some() {
			self.count += 1;
		}

		result
	}
}

#[cfg(test)]
mod tests {
	use std::{collections::HashMap, mem::size_of};

	use bytes::Bytes;

	use crate::{lsm::Tree, Options, TreeBuilder};

	use super::*;

	use tempdir::TempDir;

	fn create_temp_directory() -> TempDir {
		TempDir::new("test").unwrap()
	}

	/// Type alias for a map of keys to their version information
	/// Each key maps to a vector of (value, timestamp, is_tombstone) tuples
	#[allow(dead_code)]
	type KeyVersionsMap = HashMap<Vec<u8>, Vec<(Vec<u8>, u64, bool)>>;

	// Common setup logic for creating a store
	fn create_store() -> (Tree, TempDir) {
		let temp_dir = create_temp_directory();
		let path = temp_dir.path().to_path_buf();

		let tree = TreeBuilder::new().with_path(path).build().unwrap();
		(tree, temp_dir)
	}

	#[tokio::test]
	async fn basic_transaction() {
		let (store, _temp_dir) = create_store();

		// Define key-value pairs for the test
		let key1 = Bytes::from("foo1");
		let key2 = Bytes::from("foo2");
		let value1 = Bytes::from("baz");
		let value2 = Bytes::from("bar");

		{
			// Start a new read-write transaction (txn1)
			let mut txn1 = store.begin().unwrap();
			txn1.set(&key1, &value1).unwrap();
			txn1.set(&key2, &value1).unwrap();
			txn1.commit().await.unwrap();
		}

		{
			// Start a read-only transaction (txn3)
			let txn3 = store.begin().unwrap();
			let val = txn3.get(&key1).unwrap().unwrap();
			assert_eq!(val.as_ref(), value1.as_ref());
		}

		{
			// Start another read-write transaction (txn2)
			let mut txn2 = store.begin().unwrap();
			txn2.set(&key1, &value2).unwrap();
			txn2.set(&key2, &value2).unwrap();
			txn2.commit().await.unwrap();
		}

		// Start a read-only transaction (txn4)
		let txn4 = store.begin().unwrap();
		let val = txn4.get(&key1).unwrap().unwrap();

		// Assert that the value retrieved in txn4 matches value2
		assert_eq!(val.as_ref(), value2.as_ref());
	}

	#[tokio::test]
	async fn mvcc_snapshot_isolation() {
		let (store, _) = create_store();

		let key1 = Bytes::from("key1");
		let key2 = Bytes::from("key2");
		let value1 = Bytes::from("baz");
		let value2 = Bytes::from("bar");

		// no conflict
		{
			let mut txn1 = store.begin().unwrap();
			let mut txn2 = store.begin().unwrap();

			txn1.set(&key1, &value1).unwrap();
			txn1.commit().await.unwrap();

			assert!(txn2.get(&key2).unwrap().is_none());
			txn2.set(&key2, &value2).unwrap();
			txn2.commit().await.unwrap();
		}

		// blind writes should succeed if key wasn't read first
		{
			let mut txn1 = store.begin().unwrap();
			let mut txn2 = store.begin().unwrap();

			txn1.set(&key1, &value1).unwrap();
			txn2.set(&key1, &value2).unwrap();

			txn1.commit().await.unwrap();
			assert!(match txn2.commit().await {
				Err(err) => {
					matches!(err, Error::TransactionWriteConflict)
				}
				_ => {
					false
				}
			});
		}

		// conflict when the read key was updated by another transaction
		{
			let key = Bytes::from("key3");

			let mut txn1 = store.begin().unwrap();
			let mut txn2 = store.begin().unwrap();

			txn1.set(&key, &value1).unwrap();
			txn1.commit().await.unwrap();

			assert!(txn2.get(&key).unwrap().is_none());
			txn2.set(&key, &value1).unwrap();
			assert!(match txn2.commit().await {
				Err(err) => {
					matches!(err, Error::TransactionWriteConflict)
				}
				_ => false,
			});
		}
	}

	#[tokio::test]
	async fn ryow() {
		let (store, _) = create_store();

		let key1 = Bytes::from("k1");
		let key2 = Bytes::from("k2");
		let key3 = Bytes::from("k3");
		let value1 = Bytes::from("v1");
		let value2 = Bytes::from("v2");

		// Set a key, delete it and read it in the same transaction. Should return None.
		{
			// Start a new read-write transaction (txn1)
			let mut txn1 = store.begin().unwrap();
			txn1.set(&key1, &value1).unwrap();
			txn1.delete(&key1).unwrap();
			let res = txn1.get(&key1).unwrap();
			assert!(res.is_none());
			txn1.commit().await.unwrap();
		}

		{
			let mut txn = store.begin().unwrap();
			txn.set(&key1, &value1).unwrap();
			txn.commit().await.unwrap();
		}

		{
			// Start a new read-write transaction (txn)
			let mut txn = store.begin().unwrap();
			txn.set(&key1, &value2).unwrap();
			assert_eq!(txn.get(&key1).unwrap().unwrap().as_ref(), value2.as_ref());
			assert!(txn.get(&key3).unwrap().is_none());
			txn.set(&key2, &value1).unwrap();
			assert_eq!(txn.get(&key2).unwrap().unwrap().as_ref(), value1.as_ref());
			txn.commit().await.unwrap();
		}
	}

	// Common setup logic for creating a store
	async fn create_hermitage_store() -> Tree {
		let (store, _) = create_store();

		let key1 = Bytes::from("k1");
		let key2 = Bytes::from("k2");
		let value1 = Bytes::from("v1");
		let value2 = Bytes::from("v2");
		// Start a new read-write transaction (txn)
		let mut txn = store.begin().unwrap();
		txn.set(&key1, &value1).unwrap();
		txn.set(&key2, &value2).unwrap();
		txn.commit().await.unwrap();

		store
	}

	// The following tests are taken from hermitage (https://github.com/ept/hermitage)
	// Specifically, the tests are derived from FoundationDB tests: https://github.com/ept/hermitage/blob/master/foundationdb.md

	// G0: Write Cycles (dirty writes)
	#[tokio::test]
	async fn g0_tests() {
		let store = create_hermitage_store().await;
		let key1 = Bytes::from("k1");
		let key2 = Bytes::from("k2");
		let value3 = Bytes::from("v3");
		let value4 = Bytes::from("v4");
		let value5 = Bytes::from("v5");
		let value6 = Bytes::from("v6");

		{
			let mut txn1 = store.begin().unwrap();
			let mut txn2 = store.begin().unwrap();

			assert!(txn1.get(&key1).is_ok());
			assert!(txn1.get(&key2).is_ok());
			assert!(txn2.get(&key1).is_ok());
			assert!(txn2.get(&key2).is_ok());

			txn1.set(&key1, &value3).unwrap();
			txn2.set(&key1, &value4).unwrap();

			txn1.set(&key2, &value5).unwrap();

			txn1.commit().await.unwrap();

			txn2.set(&key2, &value6).unwrap();
			assert!(match txn2.commit().await {
				Err(err) => {
					matches!(err, Error::TransactionWriteConflict)
				}
				_ => false,
			});
		}

		{
			let txn3 = store.begin().unwrap();
			let val1 = txn3.get(&key1).unwrap().unwrap();
			assert_eq!(val1.as_ref(), value3.as_ref());
			let val2 = txn3.get(&key2).unwrap().unwrap();
			assert_eq!(val2.as_ref(), value5.as_ref());
		}
	}

	// P4: Lost Update
	#[tokio::test]
	async fn p4() {
		let store = create_hermitage_store().await;

		let key1 = Bytes::from("k1");
		let value3 = Bytes::from("v3");

		{
			let mut txn1 = store.begin().unwrap();
			let mut txn2 = store.begin().unwrap();

			assert!(txn1.get(&key1).is_ok());
			assert!(txn2.get(&key1).is_ok());

			txn1.set(&key1, &value3).unwrap();
			txn2.set(&key1, &value3).unwrap();

			txn1.commit().await.unwrap();

			assert!(match txn2.commit().await {
				Err(err) => {
					matches!(err, Error::TransactionWriteConflict)
				}
				_ => false,
			});
		}
	}

	// G-single: Single Anti-dependency Cycles (read skew)
	async fn g_single_tests() {
		let store = create_hermitage_store().await;

		let key1 = Bytes::from("k1");
		let key2 = Bytes::from("k2");
		let value1 = Bytes::from("v1");
		let value2 = Bytes::from("v2");
		let value3 = Bytes::from("v3");
		let value4 = Bytes::from("v4");

		{
			let mut txn1 = store.begin().unwrap();
			let mut txn2 = store.begin().unwrap();

			assert_eq!(txn1.get(&key1).unwrap().unwrap().as_ref(), value1.as_ref());
			assert_eq!(txn2.get(&key1).unwrap().unwrap().as_ref(), value1.as_ref());
			assert_eq!(txn2.get(&key2).unwrap().unwrap().as_ref(), value2.as_ref());
			txn2.set(&key1, &value3).unwrap();
			txn2.set(&key2, &value4).unwrap();

			txn2.commit().await.unwrap();

			assert_eq!(txn1.get(&key2).unwrap().unwrap().as_ref(), value2.as_ref());
			txn1.commit().await.unwrap();
		}
	}

	#[tokio::test]
	async fn g_single() {
		g_single_tests().await;
	}

	fn require_send<T: Send>(_: T) {}
	fn require_sync<T: Sync + Send>(_: T) {}

	#[tokio::test]
	async fn is_send_sync() {
		let (db, _) = create_store();

		let txn = db.begin().unwrap();
		require_send(txn);

		let txn = db.begin().unwrap();
		require_sync(txn);
	}

	const ENTRIES: usize = 400_000;
	const KEY_SIZE: usize = 24;
	const VALUE_SIZE: usize = 150;
	const RNG_SEED: u64 = 3;

	fn fill_slice(slice: &mut [u8], rng: &mut fastrand::Rng) {
		let mut i = 0;
		while i + size_of::<u128>() < slice.len() {
			let tmp = rng.u128(..);
			slice[i..(i + size_of::<u128>())].copy_from_slice(&tmp.to_be_bytes());
			i += size_of::<u128>()
		}
		if i + size_of::<u64>() < slice.len() {
			let tmp = rng.u64(..);
			slice[i..(i + size_of::<u64>())].copy_from_slice(&tmp.to_be_bytes());
			i += size_of::<u64>()
		}
		if i + size_of::<u32>() < slice.len() {
			let tmp = rng.u32(..);
			slice[i..(i + size_of::<u32>())].copy_from_slice(&tmp.to_be_bytes());
			i += size_of::<u32>()
		}
		if i + size_of::<u16>() < slice.len() {
			let tmp = rng.u16(..);
			slice[i..(i + size_of::<u16>())].copy_from_slice(&tmp.to_be_bytes());
			i += size_of::<u16>()
		}
		if i + size_of::<u8>() < slice.len() {
			slice[i] = rng.u8(..);
		}
	}

	fn gen_pair(rng: &mut fastrand::Rng) -> ([u8; KEY_SIZE], Vec<u8>) {
		let mut key = [0u8; KEY_SIZE];
		fill_slice(&mut key, rng);
		let mut value = vec![0u8; VALUE_SIZE];
		fill_slice(&mut value, rng);

		(key, value)
	}

	fn make_rng() -> fastrand::Rng {
		fastrand::Rng::with_seed(RNG_SEED)
	}

	#[tokio::test]
	#[ignore]
	async fn insert_large_txn_and_get() {
		let store = create_hermitage_store().await;

		let mut rng = make_rng();

		let mut txn = store.begin().unwrap();
		for _ in 0..ENTRIES {
			let (key, value) = gen_pair(&mut rng);
			txn.set(&key, &value).unwrap();
		}
		txn.commit().await.unwrap();
		drop(txn);

		// Read the keys from the store
		let mut rng = make_rng();
		let txn = store.begin_with_mode(Mode::ReadOnly).unwrap();
		for _i in 0..ENTRIES {
			let (key, _) = gen_pair(&mut rng);
			txn.get(&key).unwrap();
		}
	}

	#[tokio::test]
	async fn sdb_delete_record_id_bug() {
		let (store, _) = create_store();

		// Define key-value pairs for the test
		let key1 = Bytes::copy_from_slice(&[
			47, 33, 110, 100, 166, 192, 229, 30, 101, 24, 73, 242, 185, 36, 233, 242, 54, 96, 72,
			52,
		]);
		let key2 = Bytes::copy_from_slice(&[
			47, 33, 104, 98, 0, 0, 1, 141, 141, 42, 113, 8, 47, 166, 192, 229, 30, 101, 24, 73,
			242, 185, 36, 233, 242, 54, 96, 72, 52,
		]);
		let value1 = Bytes::from("baz");

		{
			// Start a new read-write transaction (txn)
			let mut txn = store.begin().unwrap();
			txn.set(&key1, &value1).unwrap();
			txn.set(&key2, &value1).unwrap();
			txn.commit().await.unwrap();
		}

		let key3 = Bytes::copy_from_slice(&[47, 33, 117, 115, 114, 111, 111, 116, 0]);
		{
			// Start a new read-write transaction (txn)
			let mut txn = store.begin().unwrap();
			txn.set(&key3, &value1).unwrap();
			txn.commit().await.unwrap();
		}

		let key4 = Bytes::copy_from_slice(&[47, 33, 117, 115, 114, 111, 111, 116, 0]);
		let txn1 = store.begin().unwrap();
		txn1.get(&key4).unwrap();

		{
			let mut txn2 = store.begin().unwrap();
			txn2.get(&Bytes::copy_from_slice(&[
				47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72,
				50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 33, 116,
				98, 117, 115, 101, 114, 0,
			]))
			.unwrap();
			txn2.get(&Bytes::copy_from_slice(&[
				47, 33, 110, 115, 116, 101, 115, 116, 45, 110, 115, 0,
			]))
			.unwrap();
			txn2.get(&Bytes::copy_from_slice(&[
				47, 33, 110, 115, 116, 101, 115, 116, 45, 110, 115, 0,
			]))
			.unwrap();
			txn2.set(
				&Bytes::copy_from_slice(&[47, 33, 110, 115, 116, 101, 115, 116, 45, 110, 115, 0]),
				&value1,
			)
			.unwrap();

			txn2.get(&Bytes::copy_from_slice(&[
				47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72,
				71, 72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
			]))
			.unwrap();
			txn2.get(&Bytes::copy_from_slice(&[
				47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72,
				71, 72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
			]))
			.unwrap();
			txn2.set(
				&Bytes::copy_from_slice(&[
					47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54,
					72, 71, 72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74,
					69, 0,
				]),
				&value1,
			)
			.unwrap();

			txn2.get(&Bytes::copy_from_slice(&[
				47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72,
				50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 33, 116,
				98, 117, 115, 101, 114, 0,
			]))
			.unwrap();
			txn2.get(&Bytes::copy_from_slice(&[
				47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72,
				50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 42, 117,
				115, 101, 114, 0, 42, 0, 0, 0, 1, 106, 111, 104, 110, 0,
			]))
			.unwrap();
			txn2.set(
				&Bytes::copy_from_slice(&[
					47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71,
					72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
					42, 117, 115, 101, 114, 0, 42, 0, 0, 0, 1, 106, 111, 104, 110, 0,
				]),
				&value1,
			)
			.unwrap();

			txn2.get(&Bytes::copy_from_slice(&[
				47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72,
				71, 72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
			]))
			.unwrap();

			txn2.commit().await.unwrap();
		}

		{
			let mut txn3 = store.begin().unwrap();
			txn3.get(&Bytes::copy_from_slice(&[
				47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72,
				50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 42, 117,
				115, 101, 114, 0, 42, 0, 0, 0, 1, 106, 111, 104, 110, 0,
			]))
			.unwrap();
			txn3.get(&Bytes::copy_from_slice(&[
				47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72,
				50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 33, 116,
				98, 117, 115, 101, 114, 0,
			]))
			.unwrap();
			txn3.delete(&Bytes::copy_from_slice(&[
				47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 42, 48, 49, 72, 80, 54, 72, 71, 72,
				50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0, 42, 117,
				115, 101, 114, 0, 42, 0, 0, 0, 1, 106, 111, 104, 110, 0,
			]))
			.unwrap();
			txn3.get(&Bytes::copy_from_slice(&[
				47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72,
				71, 72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
			]))
			.unwrap();
			txn3.get(&Bytes::copy_from_slice(&[
				47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54, 72,
				71, 72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74, 69, 0,
			]))
			.unwrap();
			txn3.set(
				&Bytes::copy_from_slice(&[
					47, 42, 116, 101, 115, 116, 45, 110, 115, 0, 33, 100, 98, 48, 49, 72, 80, 54,
					72, 71, 72, 50, 50, 51, 89, 82, 49, 54, 51, 90, 52, 78, 56, 72, 69, 90, 80, 74,
					69, 0,
				]),
				&value1,
			)
			.unwrap();
			txn3.commit().await.unwrap();
		}
	}

	#[tokio::test]
	async fn transaction_delete_from_index() {
		let (store, _) = create_store();

		// Define key-value pairs for the test
		let key1 = Bytes::from("foo1");
		let value = Bytes::from("baz");
		let key2 = Bytes::from("foo2");

		{
			// Start a new read-write transaction (txn1)
			let mut txn1 = store.begin().unwrap();
			txn1.set(&key1, &value).unwrap();
			txn1.set(&key2, &value).unwrap();
			txn1.commit().await.unwrap();
		}

		{
			// Start another read-write transaction (txn2)
			let mut txn2 = store.begin().unwrap();
			txn2.delete(&key1).unwrap();
			txn2.commit().await.unwrap();
		}

		{
			// Start a read-only transaction (txn3)
			let txn3 = store.begin().unwrap();
			let val = txn3.get(&key1).unwrap();
			assert!(val.is_none());
			let val = txn3.get(&key2).unwrap().unwrap();
			assert_eq!(val.as_ref(), value.as_ref());
		}

		// Start a read-only transaction (txn4)
		let txn4 = store.begin().unwrap();
		let val = txn4.get(&key1).unwrap();
		assert!(val.is_none());
		let val = txn4.get(&key2).unwrap().unwrap();
		assert_eq!(val.as_ref(), value.as_ref());
	}

	#[tokio::test]
	async fn test_insert_delete_read_key() {
		let (store, _) = create_store();

		// Key-value pair for the test
		let key = Bytes::from("test_key");
		let value1 = Bytes::from("test_value1");
		let value2 = Bytes::from("test_value2");

		// Insert key-value pair in a new transaction
		{
			let mut txn = store.begin().unwrap();
			txn.set(&key, &value1).unwrap();
			txn.commit().await.unwrap();
		}

		{
			let mut txn = store.begin().unwrap();
			txn.set(&key, &value2).unwrap();
			txn.commit().await.unwrap();
		}

		// Clear the key in a separate transaction
		{
			let mut txn = store.begin().unwrap();
			txn.delete(&key).unwrap();
			txn.commit().await.unwrap();
		}

		// Read the key in a new transaction to verify it does not exist
		{
			let txn = store.begin().unwrap();
			assert!(txn.get(&key).unwrap().is_none());
		}
	}

	#[tokio::test]
	async fn test_range_basic_functionality() {
		let (store, _temp_dir) = create_store();

		// Insert some initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key4", b"value4").unwrap();
			tx.set(b"key5", b"value5").unwrap();
			tx.commit().await.unwrap();
		}

		// Test basic range scan
		{
			let tx = store.begin().unwrap();
			let range: Vec<_> =
				tx.range(b"key2", b"key4", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

			assert_eq!(range.len(), 3);
			assert_eq!(range[0].0.as_ref(), b"key2");
			assert_eq!(range[0].1.as_ref().unwrap().as_ref(), b"value2");
			assert_eq!(range[1].0.as_ref(), b"key3");
			assert_eq!(range[1].1.as_ref().unwrap().as_ref(), b"value3");
			assert_eq!(range[2].0.as_ref(), b"key4");
			assert_eq!(range[2].1.as_ref().unwrap().as_ref(), b"value4");
		}
	}

	#[tokio::test]
	async fn test_range_with_bounds() {
		let (store, _temp_dir) = create_store();

		// Insert some initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key4", b"value4").unwrap();
			tx.set(b"key5", b"value5").unwrap();
			tx.commit().await.unwrap();
		}

		// Test range with start bound as empty
		{
			let tx = store.begin().unwrap();
			let beg = b"".as_slice();
			let range: Vec<_> =
				tx.range(beg, b"key4", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();
			assert_eq!(range.len(), 4);
			assert_eq!(range[0].0.as_ref(), b"key1");
			assert_eq!(range[1].0.as_ref(), b"key2");
			assert_eq!(range[2].0.as_ref(), b"key3");
			assert_eq!(range[3].0.as_ref(), b"key4");
		}

		// Test range with both bounds as empty
		{
			let tx = store.begin().unwrap();
			let beg = b"".as_slice();
			let end = b"".as_slice();
			let range: Vec<_> =
				tx.range(beg, end, None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();
			assert_eq!(range.len(), 0);
		}
	}

	#[tokio::test]
	async fn test_range_with_limit() {
		let (store, _temp_dir) = create_store();

		// Insert test data
		{
			let mut tx = store.begin().unwrap();
			for i in 1..=10 {
				let key = format!("key{i:02}");
				let value = format!("value{i}");
				tx.set(key.as_bytes(), value.as_bytes()).unwrap();
			}
			tx.commit().await.unwrap();
		}

		// Test with limit
		{
			let tx = store.begin().unwrap();
			let range: Vec<_> = tx
				.range(b"key01", b"key10", Some(3))
				.unwrap()
				.map(|r| r.unwrap())
				.collect::<Vec<_>>();

			assert_eq!(range.len(), 3);
			assert_eq!(range[0].0.as_ref(), b"key01");
			assert_eq!(range[1].0.as_ref(), b"key02");
			assert_eq!(range[2].0.as_ref(), b"key03");
		}
	}

	#[tokio::test]
	async fn test_range_read_your_own_writes() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"a", b"1").unwrap();
			tx.set(b"c", b"3").unwrap();
			tx.set(b"e", b"5").unwrap();
			tx.commit().await.unwrap();
		}

		// Test RYOW - uncommitted writes should be visible in range
		{
			let mut tx = store.begin().unwrap();

			// Add new keys
			tx.set(b"b", b"2").unwrap();
			tx.set(b"d", b"4").unwrap();

			// Modify existing key
			tx.set(b"c", b"3_modified").unwrap();

			// Range should see all changes
			let range: Vec<_> =
				tx.range(b"a", b"e", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

			assert_eq!(range.len(), 5);
			assert_eq!(range[0], (b"a".to_vec().into(), Some(b"1".to_vec().into())));
			assert_eq!(range[1], (b"b".to_vec().into(), Some(b"2".to_vec().into())));
			assert_eq!(range[2], (b"c".to_vec().into(), Some(b"3_modified".to_vec().into())));
			assert_eq!(range[3], (b"d".to_vec().into(), Some(b"4".to_vec().into())));
			assert_eq!(range[4], (b"e".to_vec().into(), Some(b"5".to_vec().into())));
		}
	}

	#[tokio::test]
	async fn test_range_with_deletes() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key4", b"value4").unwrap();
			tx.set(b"key5", b"value5").unwrap();
			tx.commit().await.unwrap();
		}

		// Test range with deletes in write set
		{
			let mut tx = store.begin().unwrap();

			// Delete some keys
			tx.delete(b"key2").unwrap();
			tx.delete(b"key4").unwrap();

			// Range should not see deleted keys
			let range: Vec<_> =
				tx.range(b"key1", b"key5", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

			assert_eq!(range.len(), 3);
			assert_eq!(range[0].0.as_ref(), b"key1");
			assert_eq!(range[1].0.as_ref(), b"key3");
			assert_eq!(range[2].0.as_ref(), b"key5");
		}
	}

	#[tokio::test]
	async fn test_range_delete_then_set() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.commit().await.unwrap();
		}

		// Test delete followed by set
		{
			let mut tx = store.begin().unwrap();

			// Delete then re-add with new value
			tx.delete(b"key2").unwrap();
			tx.set(b"key2", b"new_value2").unwrap();

			// Range should see the new value
			let range: Vec<_> =
				tx.range(b"key1", b"key3", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

			assert_eq!(range.len(), 3);
			assert_eq!(range[1], (b"key2".to_vec().into(), Some(b"new_value2".to_vec().into())));
		}
	}

	#[tokio::test]
	async fn test_range_empty_result() {
		let (store, _temp_dir) = create_store();

		// Insert data outside the range we'll query
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"a", b"1").unwrap();
			tx.set(b"z", b"26").unwrap();
			tx.commit().await.unwrap();
		}

		// Query range with no data
		{
			let tx = store.begin().unwrap();
			let range: Vec<_> =
				tx.range(b"m", b"n", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

			assert_eq!(range.len(), 0);
		}
	}

	#[tokio::test]
	async fn test_range_ordering() {
		let (store, _temp_dir) = create_store();

		// Insert data in non-sequential order
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key5", b"value5").unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key4", b"value4").unwrap();
			tx.commit().await.unwrap();
		}

		// Verify correct ordering in range
		{
			let tx = store.begin().unwrap();
			let range: Vec<_> =
				tx.range(b"key1", b"key5", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

			assert_eq!(range.len(), 5);
			for (i, item) in range.iter().enumerate().take(5) {
				let expected_key = format!("key{}", i + 1);
				assert_eq!(item.0.as_ref(), expected_key.as_bytes());
			}
		}
	}

	#[tokio::test]
	async fn test_range_boundary_conditions() {
		let (store, _temp_dir) = create_store();

		// Insert test data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.commit().await.unwrap();
		}

		// Test inclusive boundaries
		{
			let tx = store.begin().unwrap();

			// Range includes both start and end
			let range: Vec<_> =
				tx.range(b"key1", b"key3", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

			assert_eq!(range.len(), 3);
			assert_eq!(range[0].0.as_ref(), b"key1");
			assert_eq!(range[2].0.as_ref(), b"key3");
		}

		// Test single key range
		{
			let tx = store.begin().unwrap();
			let range: Vec<_> =
				tx.range(b"key2", b"key2", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

			assert_eq!(range.len(), 1);
			assert_eq!(range[0].0.as_ref(), b"key2");
		}
	}

	#[tokio::test]
	async fn test_range_write_sequence_order() {
		let (store, _temp_dir) = create_store();

		// Test that multiple writes to same key show latest value
		{
			let mut tx = store.begin().unwrap();

			tx.set(b"key", b"value1").unwrap();
			tx.set(b"key", b"value2").unwrap();
			tx.set(b"key", b"value3").unwrap();

			let range: Vec<_> =
				tx.range(b"key", b"key", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

			assert_eq!(range.len(), 1);
			assert_eq!(range[0].1.as_ref().unwrap().as_ref(), b"value3"); // Latest value
		}
	}

	#[tokio::test]
	async fn test_keys_method() {
		let (store, _temp_dir) = create_store();

		// Insert test data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key4", b"value4").unwrap();
			tx.set(b"key5", b"value5").unwrap();
			tx.commit().await.unwrap();
		}

		// Test with RYOW - add a key in the current transaction
		{
			let mut tx = store.begin().unwrap();

			// Add a key in the transaction (not yet committed)
			tx.set(b"key6", b"value6").unwrap();

			// Get keys only
			let keys_only: Vec<_> =
				tx.keys(b"key1", b"key9", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

			// Verify we got all 6 keys (5 from storage + 1 from write set)
			assert_eq!(keys_only.len(), 6);

			// Check the keys are in order
			for (i, (key, value)) in keys_only.iter().enumerate().take(6) {
				let expected_key = format!("key{}", i + 1);
				assert_eq!(key.as_ref(), expected_key.as_bytes());
				// Values should be None
				assert!(value.is_none(), "Value should be None for keys-only scan");
			}

			// Compare with regular range
			let regular_range: Vec<_> =
				tx.range(b"key1", b"key9", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

			// Should have same number of items
			assert_eq!(regular_range.len(), keys_only.len());

			// Keys should match and regular range values should be correct
			for i in 0..keys_only.len() {
				assert_eq!(keys_only[i].0, regular_range[i].0, "Keys should match");

				if i < 5 {
					// For keys from storage, check regular values are correct
					assert!(regular_range[i].1.is_some(), "Regular range should have values");
					assert_eq!(
						regular_range[i].1.as_ref().unwrap().as_ref(),
						format!("value{}", i + 1).as_bytes(),
						"Regular range should have correct values from storage"
					);
				} else {
					// For the key from write set
					assert!(regular_range[i].1.is_some(), "Regular range should have values");
					assert_eq!(
						regular_range[i].1.as_ref().unwrap().as_ref(),
						b"value6",
						"Regular range should have correct value from write set"
					);
				}
			}

			// Test with a deleted key
			tx.delete(b"key3").unwrap();

			let keys_after_delete: Vec<_> =
				tx.keys(b"key1", b"key9", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

			// Should have 5 keys now (key3 is deleted)
			assert_eq!(keys_after_delete.len(), 5);

			// Verify key3 is not in the results
			let key_names: Vec<_> = keys_after_delete
				.iter()
				.map(|(k, _)| String::from_utf8_lossy(k.as_ref()).to_string())
				.collect();

			assert!(!key_names.contains(&"key3".to_string()), "key3 should be removed");
		}
	}

	#[tokio::test]
	async fn test_range_value_pointer_resolution_bug() {
		let temp_dir = create_temp_directory();

		let tree = TreeBuilder::new()
			.with_path(temp_dir.path().to_path_buf())
			.with_max_memtable_size(512)
			.build()
			.unwrap();

		// Create values that will be stored in VLog (> 50 bytes)
		let key1 = b"key1";
		let key2 = b"key2";
		let key3 = b"key3";

		let large_value1 = "X".repeat(100); // > 50 bytes, goes to VLog
		let large_value2 = "Y".repeat(100); // > 50 bytes, goes to VLog
		let large_value3 = "Z".repeat(100); // > 50 bytes, goes to VLog

		// Insert the values
		{
			let mut txn = tree.begin().unwrap();
			txn.set(key1, large_value1.as_bytes()).unwrap();
			txn.set(key2, large_value2.as_bytes()).unwrap();
			txn.set(key3, large_value3.as_bytes()).unwrap();
			txn.commit().await.unwrap();
		}

		// Force flush to ensure data goes to SSTables (and VLog)
		tree.flush().unwrap();

		// Test 1: Verify get() works correctly
		{
			let txn = tree.begin().unwrap();

			let retrieved1 = txn.get(key1).unwrap().unwrap();
			let retrieved2 = txn.get(key2).unwrap().unwrap();
			let retrieved3 = txn.get(key3).unwrap().unwrap();

			assert_eq!(
				retrieved1.as_ref(),
				large_value1.as_bytes(),
				"get() should resolve value pointers correctly"
			);
			assert_eq!(
				retrieved2.as_ref(),
				large_value2.as_bytes(),
				"get() should resolve value pointers correctly"
			);
			assert_eq!(
				retrieved3.as_ref(),
				large_value3.as_bytes(),
				"get() should resolve value pointers correctly"
			);
		}

		// Test 2: Verify range() also works correctly
		{
			let txn = tree.begin().unwrap();

			let range_results: Vec<_> =
				txn.range(b"key1", b"key3", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

			assert_eq!(range_results.len(), 3, "Should get 3 items from range query");

			// Check that all values are correctly resolved (not value pointers)
			for (i, (returned_key, returned_value)) in range_results.iter().enumerate() {
				let expected_key = match i {
					0 => key1,
					1 => key2,
					2 => key3,
					_ => panic!("Unexpected index"),
				};
				let expected_value = match i {
					0 => &large_value1,
					1 => &large_value2,
					2 => &large_value3,
					_ => panic!("Unexpected index"),
				};

				assert_eq!(returned_key.as_ref(), expected_key, "Key mismatch in range result");

				// The returned value should be the actual value, not a value pointer
				assert!(returned_value.is_some(), "Range should return resolved values, not None");
				assert_eq!(
					returned_value.as_ref().unwrap().as_ref(),
					expected_value.as_bytes(),
					"Range should return resolved values, not value pointers. \
                     Expected actual value of {} bytes, but got a different value",
					expected_value.len(),
				);
			}
		}
	}

	// Savepoint tests
	mod savepoint_tests {
		use super::*;

		#[tokio::test]
		async fn multiple_savepoints() {
			let (store, _) = create_store();

			// Key-value pair for the test
			let key1 = Bytes::from("test_key1");
			let value1 = Bytes::from("test_value1");
			let key2 = Bytes::from("test_key2");
			let value2 = Bytes::from("test_value2");
			let key3 = Bytes::from("test_key3");
			let value3 = Bytes::from("test_value3");

			// Start the transaction and write key1.
			let mut txn1 = store.begin().unwrap();
			txn1.set(&key1, &value1).unwrap();

			// Set the first savepoint.
			txn1.set_savepoint().unwrap();

			// Write key2 after the savepoint.
			txn1.set(&key2, &value2).unwrap();

			// Set another savepoint, stacking it onto the first one.
			txn1.set_savepoint().unwrap();

			txn1.set(&key3, &value3).unwrap();

			// Just a sanity check that all three keys are present.
			assert_eq!(txn1.get(&key1).unwrap().unwrap().as_ref(), value1.as_ref());
			assert_eq!(txn1.get(&key2).unwrap().unwrap().as_ref(), value2.as_ref());
			assert_eq!(txn1.get(&key3).unwrap().unwrap().as_ref(), value3.as_ref());

			// Rollback to the latest (second) savepoint. This should make key3
			// go away while keeping key1 and key2.
			txn1.rollback_to_savepoint().unwrap();
			assert_eq!(txn1.get(&key1).unwrap().unwrap().as_ref(), value1.as_ref());
			assert_eq!(txn1.get(&key2).unwrap().unwrap().as_ref(), value2.as_ref());
			assert!(txn1.get(&key3).unwrap().is_none());

			// Now roll back to the first savepoint. This should only
			// keep key1 around.
			txn1.rollback_to_savepoint().unwrap();
			assert_eq!(txn1.get(&key1).unwrap().unwrap().as_ref(), value1.as_ref());
			assert!(txn1.get(&key2).unwrap().is_none());
			assert!(txn1.get(&key3).unwrap().is_none());

			// Check that without any savepoints set the error is returned.
			assert!(matches!(
				txn1.rollback_to_savepoint(),
				Err(Error::TransactionWithoutSavepoint)
			));

			// Commit the transaction.
			txn1.commit().await.unwrap();
			drop(txn1);

			// Start another transaction and check again for the keys.
			let txn2 = store.begin().unwrap();
			assert_eq!(txn2.get(&key1).unwrap().unwrap().as_ref(), value1.as_ref());
			assert!(txn2.get(&key2).unwrap().is_none());
			assert!(txn2.get(&key3).unwrap().is_none());
		}

		#[tokio::test]
		async fn savepoint_rollback_on_updated_key() {
			let (store, _) = create_store();

			let k1 = Bytes::from("k1");
			let value1 = Bytes::from("value1");
			let value2 = Bytes::from("value2");
			let value3 = Bytes::from("value3");

			let mut txn1 = store.begin().unwrap();
			txn1.set(&k1, &value1).unwrap();
			txn1.set(&k1, &value2).unwrap();
			txn1.set_savepoint().unwrap();
			txn1.set(&k1, &value3).unwrap();
			txn1.rollback_to_savepoint().unwrap();

			// The read value should be the one before the savepoint.
			assert_eq!(txn1.get(&k1).unwrap().unwrap().as_ref(), value2.as_ref());
		}

		#[tokio::test]
		async fn savepoint_rollback_with_range_scan() {
			let (store, _) = create_store();

			let k1 = Bytes::from("k1");
			let value = Bytes::from("value1");
			let value2 = Bytes::from("value2");

			let mut txn1 = store.begin().unwrap();
			txn1.set(&k1, &value).unwrap();
			txn1.set_savepoint().unwrap();
			txn1.set(&k1, &value2).unwrap();
			txn1.rollback_to_savepoint().unwrap();

			// The scanned value should be the one before the savepoint.
			let range: Vec<_> =
				txn1.range(b"k1", b"k3", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();
			assert_eq!(range.len(), 1);
			assert_eq!(range[0].0.as_ref(), k1.as_ref());
			assert_eq!(range[0].1.as_ref().unwrap().as_ref(), value.as_ref());
		}

		#[tokio::test]
		async fn savepoint_with_deletes() {
			let (store, _) = create_store();

			let k1 = Bytes::from("k1");
			let k2 = Bytes::from("k2");
			let value1 = Bytes::from("value1");
			let value2 = Bytes::from("value2");

			let mut txn1 = store.begin().unwrap();
			txn1.set(&k1, &value1).unwrap();
			txn1.set(&k2, &value2).unwrap();
			txn1.set_savepoint().unwrap();

			// Delete k1 and modify k2
			txn1.delete(&k1).unwrap();
			txn1.set(&k2, b"modified").unwrap();

			// Verify the changes
			assert!(txn1.get(&k1).unwrap().is_none());
			assert_eq!(txn1.get(&k2).unwrap().unwrap().as_ref(), b"modified");

			// Rollback to savepoint
			txn1.rollback_to_savepoint().unwrap();

			// Verify original values are restored
			assert_eq!(txn1.get(&k1).unwrap().unwrap().as_ref(), value1.as_ref());
			assert_eq!(txn1.get(&k2).unwrap().unwrap().as_ref(), value2.as_ref());
		}

		#[tokio::test]
		async fn savepoint_nested_operations() {
			let (store, _) = create_store();

			let k1 = Bytes::from("k1");
			let k2 = Bytes::from("k2");
			let k3 = Bytes::from("k3");
			let value1 = Bytes::from("value1");
			let value2 = Bytes::from("value2");
			let value3 = Bytes::from("value3");

			let mut txn1 = store.begin().unwrap();
			txn1.set(&k1, &value1).unwrap();

			// First savepoint
			txn1.set_savepoint().unwrap();
			txn1.set(&k2, &value2).unwrap();

			// Second savepoint
			txn1.set_savepoint().unwrap();
			txn1.set(&k3, &value3).unwrap();

			// Rollback to second savepoint (should remove k3)
			txn1.rollback_to_savepoint().unwrap();
			assert_eq!(txn1.get(&k1).unwrap().unwrap().as_ref(), value1.as_ref());
			assert_eq!(txn1.get(&k2).unwrap().unwrap().as_ref(), value2.as_ref());
			assert!(txn1.get(&k3).unwrap().is_none());

			// Rollback to first savepoint (should remove k2)
			txn1.rollback_to_savepoint().unwrap();
			assert_eq!(txn1.get(&k1).unwrap().unwrap().as_ref(), value1.as_ref());
			assert!(txn1.get(&k2).unwrap().is_none());
			assert!(txn1.get(&k3).unwrap().is_none());

			// Final rollback should fail
			assert!(matches!(
				txn1.rollback_to_savepoint(),
				Err(Error::TransactionWithoutSavepoint)
			));
		}
	}

	#[tokio::test]
	async fn test_soft_delete_basic_functionality() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.commit().await.unwrap();
		}

		// Verify data is visible
		{
			let tx = store.begin().unwrap();
			assert_eq!(tx.get(b"key1").unwrap().unwrap().as_ref(), b"value1");
			assert_eq!(tx.get(b"key2").unwrap().unwrap().as_ref(), b"value2");
			assert_eq!(tx.get(b"key3").unwrap().unwrap().as_ref(), b"value3");
		}

		// Soft delete key2
		{
			let mut tx = store.begin().unwrap();
			tx.soft_delete(b"key2").unwrap();
			tx.commit().await.unwrap();
		}

		// Verify soft deleted key is not visible in reads
		{
			let tx = store.begin().unwrap();
			assert_eq!(tx.get(b"key1").unwrap().unwrap().as_ref(), b"value1");
			assert!(tx.get(b"key2").unwrap().is_none()); // Should be None after soft delete
			assert_eq!(tx.get(b"key3").unwrap().unwrap().as_ref(), b"value3");
		}

		// Verify soft deleted key is not visible in range scans
		{
			let tx = store.begin().unwrap();
			let range: Vec<_> =
				tx.range(b"key1", b"key3", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();
			assert_eq!(range.len(), 2); // Only key1 and key3, key2 is filtered out
			assert_eq!(range[0].0.as_ref(), b"key1");
			assert_eq!(range[1].0.as_ref(), b"key3");
		}
	}

	#[tokio::test]
	async fn test_soft_delete_vs_hard_delete() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.commit().await.unwrap();
		}

		// Soft delete key1, hard delete key2
		{
			let mut tx = store.begin().unwrap();
			tx.soft_delete(b"key1").unwrap();
			tx.delete(b"key2").unwrap();
			tx.commit().await.unwrap();
		}

		// Both should be invisible to reads
		{
			let tx = store.begin().unwrap();
			assert!(tx.get(b"key1").unwrap().is_none()); // Soft deleted
			assert!(tx.get(b"key2").unwrap().is_none()); // Hard deleted
			assert_eq!(tx.get(b"key3").unwrap().unwrap().as_ref(), b"value3");
		}

		// Both should be invisible to range scans
		{
			let tx = store.begin().unwrap();
			let range: Vec<_> =
				tx.range(b"key1", b"key3", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();
			assert_eq!(range.len(), 1); // Only key3
			assert_eq!(range[0].0.as_ref(), b"key3");
		}
	}

	#[tokio::test]
	async fn test_soft_delete_in_transaction_write_set() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.commit().await.unwrap();
		}

		// Start a transaction and soft delete within it
		{
			let mut tx = store.begin().unwrap();

			// Soft delete key1 within the transaction
			tx.soft_delete(b"key1").unwrap();

			// Within the same transaction, the soft delete should NOT be visible
			// The key should appear as if it doesn't exist
			assert!(tx.get(b"key1").unwrap().is_none());

			// Range scan within transaction should not see soft deleted key
			let range: Vec<_> =
				tx.range(b"key1", b"key2", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();
			assert_eq!(range.len(), 1); // Only key2
			assert_eq!(range[0].0.as_ref(), b"key2");

			tx.commit().await.unwrap();
		}

		// After commit, soft deleted key should still be invisible
		{
			let tx = store.begin().unwrap();
			assert!(tx.get(b"key1").unwrap().is_none());
			assert_eq!(tx.get(b"key2").unwrap().unwrap().as_ref(), b"value2");
		}
	}

	#[tokio::test]
	async fn test_soft_delete_then_reinsert() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.commit().await.unwrap();
		}

		// Soft delete the key
		{
			let mut tx = store.begin().unwrap();
			tx.soft_delete(b"key1").unwrap();
			tx.commit().await.unwrap();
		}

		// Verify it's not visible
		{
			let tx = store.begin().unwrap();
			assert!(tx.get(b"key1").unwrap().is_none());
		}

		// Re-insert the same key with a new value
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1_new").unwrap();
			tx.commit().await.unwrap();
		}

		// Verify the new value is visible
		{
			let tx = store.begin().unwrap();
			assert_eq!(tx.get(b"key1").unwrap().unwrap().as_ref(), b"value1_new");
		}
	}

	#[tokio::test]
	async fn test_soft_delete_range_scan_filtering() {
		let (store, _temp_dir) = create_store();

		// Insert multiple keys
		{
			let mut tx = store.begin().unwrap();
			for i in 1..=10 {
				let key = format!("key{i:02}");
				let value = format!("value{i}");
				tx.set(key.as_bytes(), value.as_bytes()).unwrap();
			}
			tx.commit().await.unwrap();
		}

		// Soft delete some keys
		{
			let mut tx = store.begin().unwrap();
			tx.soft_delete(b"key02").unwrap();
			tx.soft_delete(b"key05").unwrap();
			tx.soft_delete(b"key08").unwrap();
			tx.commit().await.unwrap();
		}

		// Range scan should not include soft deleted keys
		{
			let tx = store.begin().unwrap();
			let range: Vec<_> =
				tx.range(b"key01", b"key10", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();

			// Should have 7 keys (10 - 3 soft deleted)
			assert_eq!(range.len(), 7);

			// Verify specific keys are present/absent
			let keys: std::collections::HashSet<_> =
				range.iter().map(|(k, _)| k.as_ref()).collect();
			assert!(keys.contains(&b"key01".as_ref()));
			assert!(!keys.contains(&b"key02".as_ref())); // Soft deleted
			assert!(keys.contains(&b"key03".as_ref()));
			assert!(keys.contains(&b"key04".as_ref()));
			assert!(!keys.contains(&b"key05".as_ref())); // Soft deleted
			assert!(keys.contains(&b"key06".as_ref()));
			assert!(keys.contains(&b"key07".as_ref()));
			assert!(!keys.contains(&b"key08".as_ref())); // Soft deleted
			assert!(keys.contains(&b"key09".as_ref()));
			assert!(keys.contains(&b"key10".as_ref()));
		}
	}

	#[tokio::test]
	async fn test_soft_delete_mixed_with_other_operations() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.set(b"key2", b"value2").unwrap();
			tx.set(b"key3", b"value3").unwrap();
			tx.set(b"key4", b"value4").unwrap();
			tx.commit().await.unwrap();
		}

		// Mix of operations in one transaction
		{
			let mut tx = store.begin().unwrap();
			tx.soft_delete(b"key1").unwrap(); // Soft delete
			tx.delete(b"key2").unwrap(); // Hard delete
			tx.set(b"key3", b"value3_updated").unwrap(); // Update
												// key4 remains unchanged
			tx.commit().await.unwrap();
		}

		// Verify results
		{
			let tx = store.begin().unwrap();
			assert!(tx.get(b"key1").unwrap().is_none()); // Soft deleted
			assert!(tx.get(b"key2").unwrap().is_none()); // Hard deleted
			assert_eq!(tx.get(b"key3").unwrap().unwrap().as_ref(), b"value3_updated"); // Updated
			assert_eq!(tx.get(b"key4").unwrap().unwrap().as_ref(), b"value4"); // Unchanged
		}

		// Range scan should only see updated and unchanged keys
		{
			let tx = store.begin().unwrap();
			let range: Vec<_> =
				tx.range(b"key1", b"key4", None).unwrap().map(|r| r.unwrap()).collect::<Vec<_>>();
			assert_eq!(range.len(), 2); // Only key3 and key4
			assert_eq!(range[0].0.as_ref(), b"key3");
			assert_eq!(range[1].0.as_ref(), b"key4");
		}
	}

	#[tokio::test]
	async fn test_soft_delete_rollback() {
		let (store, _temp_dir) = create_store();

		// Insert initial data
		{
			let mut tx = store.begin().unwrap();
			tx.set(b"key1", b"value1").unwrap();
			tx.commit().await.unwrap();
		}

		// Start transaction and soft delete, then rollback
		{
			let mut tx = store.begin().unwrap();
			tx.soft_delete(b"key1").unwrap();

			// Within transaction, key should be invisible
			assert!(tx.get(b"key1").unwrap().is_none());

			// Rollback the transaction
			tx.rollback();
		}

		// After rollback, key should be visible again
		{
			let tx = store.begin().unwrap();
			assert_eq!(tx.get(b"key1").unwrap().unwrap().as_ref(), b"value1");
		}
	}

	#[tokio::test]
	async fn test_versioned_queries_basic() {
		let temp_dir = create_temp_directory();
		let opts: Options =
			Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
		let tree = TreeBuilder::with_options(opts).build().unwrap();

		// Use explicit timestamps for better testing
		let ts1 = 100; // First version timestamp
		let ts2 = 200; // Second version timestamp

		// Insert data with explicit timestamps
		let mut tx1 = tree.begin().unwrap();
		tx1.set_at_ts(b"key1", b"value1_v1", ts1).unwrap();
		tx1.commit().await.unwrap();

		let mut tx2 = tree.begin().unwrap();
		tx2.set_at_ts(b"key1", b"value1_v2", ts2).unwrap();
		tx2.commit().await.unwrap();

		// Test regular get (should return latest)
		let tx = tree.begin().unwrap();
		let value = tx.get(b"key1").unwrap();
		assert_eq!(value, Some(Arc::from(b"value1_v2" as &[u8])));

		// Get all versions to verify timestamps and values
		let versions = tx.scan_all_timestamps(b"key1".to_vec()..=b"key1".to_vec(), None).unwrap();
		assert_eq!(versions.len(), 2);

		// Find versions by timestamp
		let v1 = versions.iter().find(|(_, _, timestamp, _)| *timestamp == ts1).unwrap();
		let v2 = versions.iter().find(|(_, _, timestamp, _)| *timestamp == ts2).unwrap();

		// Verify values match timestamps
		assert_eq!(v1.1.as_ref(), b"value1_v1");
		assert_eq!(v2.1.as_ref(), b"value1_v2");

		// Test get at specific timestamp (earlier version)
		let value_at_ts1 = tx.get_at_timestamp(b"key1", ts1).unwrap();
		assert_eq!(value_at_ts1, Some(Arc::from(b"value1_v1" as &[u8])));

		// Test get at later timestamp (should return latest version as of that time)
		let value_at_ts2 = tx.get_at_timestamp(b"key1", ts2).unwrap();
		assert_eq!(value_at_ts2, Some(Arc::from(b"value1_v2" as &[u8])));
	}

	#[tokio::test]
	async fn test_versioned_queries_with_deletes() {
		let temp_dir = create_temp_directory();
		let opts: Options =
			Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
		let tree = TreeBuilder::with_options(opts).build().unwrap();

		// Insert first version
		let mut tx1 = tree.begin().unwrap();
		tx1.set(b"key1", b"value1").unwrap();
		tx1.commit().await.unwrap();
		let ts1 = now();

		// Update with second version
		let mut tx2 = tree.begin().unwrap();
		tx2.set(b"key1", b"value2").unwrap();
		tx2.commit().await.unwrap();
		let ts2 = now();

		// Delete the key
		let mut tx3 = tree.begin().unwrap();
		tx3.soft_delete(b"key1").unwrap(); // Hard delete
		tx3.commit().await.unwrap();
		let ts3 = now();

		// Test regular get (should return None due to delete)
		let tx = tree.begin().unwrap();
		let value = tx.get(b"key1").unwrap();
		assert_eq!(value, None);

		// Test scan_all_timestamps to get all versions including tombstones
		let all_versions =
			tx.scan_all_timestamps(b"key1".to_vec()..=b"key1".to_vec(), None).unwrap();
		assert_eq!(all_versions.len(), 3);

		// Check values by timestamp
		let val1 = &all_versions[0];
		let val2 = &all_versions[1];
		assert!(val1.2 < val2.2);
		assert_eq!(val1.1.as_ref(), b"value1");
		assert_eq!(val2.1.as_ref(), b"value2");

		// Test scan_at_timestamp with specific timestamp to get point-in-time view
		let version_at_ts1 =
			tx.scan_at_timestamp(b"key1".to_vec()..=b"key1".to_vec(), ts1, None).unwrap();
		assert_eq!(version_at_ts1.len(), 1);
		assert_eq!(version_at_ts1[0].1.as_ref(), b"value1");

		let version_at_ts2 =
			tx.scan_at_timestamp(b"key1".to_vec()..=b"key1".to_vec(), ts2, None).unwrap();
		assert_eq!(version_at_ts2.len(), 1);
		assert_eq!(version_at_ts2[0].1.as_ref(), b"value2");

		// Test with timestamp after delete - should show nothing
		let version_at_ts3 =
			tx.scan_at_timestamp(b"key1".to_vec()..=b"key1".to_vec(), ts3, None).unwrap();
		assert_eq!(version_at_ts3.len(), 0);
	}

	#[tokio::test]
	async fn test_set_at_timestamp() {
		let temp_dir = create_temp_directory();
		let opts: Options =
			Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
		let tree = TreeBuilder::with_options(opts).build().unwrap();

		// Set a value with a specific timestamp
		let custom_timestamp = 10;
		let mut tx = tree.begin().unwrap();
		tx.set_at_ts(b"key1", b"value1", custom_timestamp).unwrap();
		tx.commit().await.unwrap();

		// Verify we can get the value at that timestamp
		let tx = tree.begin().unwrap();
		let value = tx.get_at_timestamp(b"key1", custom_timestamp).unwrap();
		assert_eq!(value, Some(Arc::from(b"value1" as &[u8])));

		// Verify we can get the value at a later timestamp
		let later_timestamp = custom_timestamp + 1000000;
		let value = tx.get_at_timestamp(b"key1", later_timestamp).unwrap();
		assert_eq!(value, Some(Arc::from(b"value1" as &[u8])));

		// Verify we can't get the value at an earlier timestamp
		let earlier_timestamp = custom_timestamp - 5;
		let value = tx.get_at_timestamp(b"key1", earlier_timestamp).unwrap();
		assert_eq!(value, None);

		// Verify using scan_all_timestamps to check the timestamp
		let versions = tx.scan_all_timestamps(b"key1".to_vec()..=b"key1".to_vec(), None).unwrap();
		assert_eq!(versions.len(), 1);
		assert_eq!(versions[0].2, custom_timestamp); // Check the timestamp
		assert_eq!(versions[0].1.as_ref(), b"value1"); // Check the value
	}

	#[tokio::test]
	async fn test_commit_timestamp_consistency() {
		let temp_dir = create_temp_directory();
		let opts: Options =
			Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
		let tree = TreeBuilder::with_options(opts).build().unwrap();

		// Set multiple values in a single transaction
		let mut tx = tree.begin().unwrap();
		tx.set(b"key1", b"value1").unwrap();
		tx.set(b"key2", b"value2").unwrap();
		tx.set(b"key3", b"value3").unwrap();
		tx.commit().await.unwrap();

		// All keys should have the same timestamp
		let tx = tree.begin().unwrap();
		let versions1 = tx.scan_all_timestamps(b"key1".to_vec()..=b"key1".to_vec(), None).unwrap();
		let versions2 = tx.scan_all_timestamps(b"key2".to_vec()..=b"key2".to_vec(), None).unwrap();
		let versions3 = tx.scan_all_timestamps(b"key3".to_vec()..=b"key3".to_vec(), None).unwrap();

		assert_eq!(versions1.len(), 1);
		assert_eq!(versions2.len(), 1);
		assert_eq!(versions3.len(), 1);

		// Compare timestamps (now the third element in the tuple)
		let ts1 = versions1[0].2;
		let ts2 = versions2[0].2;
		let ts3 = versions3[0].2;
		assert_eq!(ts1, ts2);
		assert_eq!(ts2, ts3);

		// Test mixed explicit and implicit timestamps
		let custom_timestamp = 9876543210000000000;
		let mut tx = tree.begin().unwrap();
		tx.set(b"key4", b"value4").unwrap(); // Will get commit timestamp
		tx.set_at_ts(b"key5", b"value5", custom_timestamp).unwrap(); // Explicit timestamp
		tx.set(b"key6", b"value6").unwrap(); // Will get commit timestamp
		tx.commit().await.unwrap();

		let tx = tree.begin().unwrap();
		let versions4 = tx.scan_all_timestamps(b"key4".to_vec()..=b"key4".to_vec(), None).unwrap();
		let versions5 = tx.scan_all_timestamps(b"key5".to_vec()..=b"key5".to_vec(), None).unwrap();
		let versions6 = tx.scan_all_timestamps(b"key6".to_vec()..=b"key6".to_vec(), None).unwrap();

		assert_eq!(versions4.len(), 1);
		assert_eq!(versions5.len(), 1);
		assert_eq!(versions6.len(), 1);

		// Get timestamps from scan results (timestamp is the third element in the tuple)
		let ts4 = versions4[0].2;
		let ts5 = versions5[0].2;
		let ts6 = versions6[0].2;

		// key4 and key6 should have the same timestamp (commit timestamp)
		assert_eq!(ts4, ts6);

		// key5 should have the custom timestamp
		assert_eq!(ts5, custom_timestamp);

		// key4/key6 timestamp should be different from key5 timestamp
		assert_ne!(ts4, ts5);
	}

	#[tokio::test]
	async fn test_keys_at_timestamp() {
		let temp_dir = create_temp_directory();
		let opts: Options =
			Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
		let tree = TreeBuilder::with_options(opts).build().unwrap();

		// Use explicit timestamps for better testing
		let ts1 = 100; // First batch timestamp
		let ts2 = 200; // Second batch timestamp

		// Insert data with first timestamp
		let mut tx1 = tree.begin().unwrap();
		tx1.set_at_ts(b"key1", b"value1", ts1).unwrap();
		tx1.set_at_ts(b"key2", b"value2", ts1).unwrap();
		tx1.set_at_ts(b"key3", b"value3", ts1).unwrap();
		tx1.commit().await.unwrap();

		// Insert data with second timestamp
		let mut tx2 = tree.begin().unwrap();
		tx2.set_at_ts(b"key2", b"value2_updated", ts2).unwrap(); // Update existing key
		tx2.set_at_ts(b"key4", b"value4", ts2).unwrap(); // Add new key
		tx2.commit().await.unwrap();

		// Test keys_at_timestamp at first timestamp
		let tx = tree.begin().unwrap();
		let keys_at_ts1 =
			tx.keys_at_timestamp(b"key1".to_vec()..=b"key4".to_vec(), ts1, None).unwrap();
		assert_eq!(keys_at_ts1.len(), 3);
		assert!(keys_at_ts1.contains(&b"key1".to_vec()));
		assert!(keys_at_ts1.contains(&b"key2".to_vec()));
		assert!(keys_at_ts1.contains(&b"key3".to_vec()));
		assert!(!keys_at_ts1.contains(&b"key4".to_vec())); // key4 didn't exist at ts1

		// Test keys_at_timestamp at second timestamp
		let keys_at_ts2 =
			tx.keys_at_timestamp(b"key1".to_vec()..=b"key4".to_vec(), ts2, None).unwrap();
		assert_eq!(keys_at_ts2.len(), 4);
		assert!(keys_at_ts2.contains(&b"key1".to_vec()));
		assert!(keys_at_ts2.contains(&b"key2".to_vec()));
		assert!(keys_at_ts2.contains(&b"key3".to_vec()));
		assert!(keys_at_ts2.contains(&b"key4".to_vec()));

		// Test with limit
		let keys_limited =
			tx.keys_at_timestamp(b"key1".to_vec()..=b"key4".to_vec(), ts2, Some(2)).unwrap();
		assert_eq!(keys_limited.len(), 2);

		// Test with specific key range
		let keys_range =
			tx.keys_at_timestamp(b"key2".to_vec()..=b"key3".to_vec(), ts2, None).unwrap();
		assert_eq!(keys_range.len(), 2);
		assert!(keys_range.contains(&b"key2".to_vec()));
		assert!(keys_range.contains(&b"key3".to_vec()));
	}

	#[tokio::test]
	async fn test_keys_at_timestamp_with_deletes() {
		let temp_dir = create_temp_directory();
		let opts: Options =
			Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
		let tree = TreeBuilder::with_options(opts).build().unwrap();

		// Insert data
		let mut tx1 = tree.begin().unwrap();
		tx1.set(b"key1", b"value1").unwrap();
		tx1.set(b"key2", b"value2").unwrap();
		tx1.set(b"key3", b"value3").unwrap();
		tx1.commit().await.unwrap();

		// Delete key2 (hard delete) and soft delete key3
		let mut tx2 = tree.begin().unwrap();
		tx2.delete(b"key2").unwrap();
		tx2.soft_delete(b"key3").unwrap();
		tx2.commit().await.unwrap();

		// Test keys_at_timestamp with current timestamp
		// Should only return key1 (key2 was hard deleted, key3 was soft deleted)
		let tx = tree.begin().unwrap();
		let keys =
			tx.keys_at_timestamp(b"key1".to_vec()..=b"key3".to_vec(), u64::MAX, None).unwrap();
		assert_eq!(keys.len(), 1, "Should have only 1 key after deletes");
		assert!(keys.contains(&b"key1".to_vec()));
		assert!(!keys.contains(&b"key2".to_vec())); // Hard deleted
		assert!(!keys.contains(&b"key3".to_vec())); // Soft deleted
	}

	#[tokio::test]
	async fn test_scan_at_timestamp() {
		let temp_dir = create_temp_directory();
		let opts: Options =
			Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
		let tree = TreeBuilder::with_options(opts).build().unwrap();

		// Use explicit timestamps for better testing
		let ts1 = 100; // First batch timestamp
		let ts2 = 200; // Second batch timestamp

		// Insert data with first timestamp
		let mut tx1 = tree.begin().unwrap();
		tx1.set_at_ts(b"key1", b"value1", ts1).unwrap();
		tx1.set_at_ts(b"key2", b"value2", ts1).unwrap();
		tx1.set_at_ts(b"key3", b"value3", ts1).unwrap();
		tx1.commit().await.unwrap();

		// Insert data with second timestamp
		let mut tx2 = tree.begin().unwrap();
		tx2.set_at_ts(b"key2", b"value2_updated", ts2).unwrap(); // Update existing key
		tx2.set_at_ts(b"key4", b"value4", ts2).unwrap(); // Add new key
		tx2.commit().await.unwrap();

		// Test scan_at_timestamp at first timestamp
		let tx = tree.begin().unwrap();
		let scan_at_ts1 =
			tx.scan_at_timestamp(b"key1".to_vec()..=b"key4".to_vec(), ts1, None).unwrap();
		assert_eq!(scan_at_ts1.len(), 3);

		// Check that we get key-value pairs
		let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
		for (key, value) in &scan_at_ts1 {
			found_keys.insert(key.as_ref());
			match key.as_slice() {
				b"key1" => assert_eq!(value.as_ref(), b"value1"),
				b"key2" => assert_eq!(value.as_ref(), b"value2"),
				b"key3" => assert_eq!(value.as_ref(), b"value3"),
				_ => panic!("Unexpected key: {:?}", key),
			}
		}
		assert!(found_keys.contains(&b"key1".as_ref()));
		assert!(found_keys.contains(&b"key2".as_ref()));
		assert!(found_keys.contains(&b"key3".as_ref()));
		assert!(!found_keys.contains(&b"key4".as_ref())); // key4 didn't exist at ts1

		// Test scan_at_timestamp at second timestamp
		let scan_at_ts2 =
			tx.scan_at_timestamp(b"key1".to_vec()..=b"key4".to_vec(), ts2, None).unwrap();
		assert_eq!(scan_at_ts2.len(), 4);

		let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
		for (key, value) in &scan_at_ts2 {
			found_keys.insert(key.as_ref());
			match key.as_slice() {
				b"key1" => assert_eq!(value.as_ref(), b"value1"),
				b"key2" => assert_eq!(value.as_ref(), b"value2_updated"),
				b"key3" => assert_eq!(value.as_ref(), b"value3"),
				b"key4" => assert_eq!(value.as_ref(), b"value4"),
				_ => panic!("Unexpected key: {:?}", key),
			}
		}
		assert!(found_keys.contains(&b"key1".as_ref()));
		assert!(found_keys.contains(&b"key2".as_ref()));
		assert!(found_keys.contains(&b"key3".as_ref()));
		assert!(found_keys.contains(&b"key4".as_ref()));

		// Test with limit
		let scan_limited =
			tx.scan_at_timestamp(b"key1".to_vec()..=b"key4".to_vec(), ts2, Some(2)).unwrap();
		assert_eq!(scan_limited.len(), 2);

		// Test with specific key range
		let scan_range =
			tx.scan_at_timestamp(b"key2".to_vec()..=b"key3".to_vec(), ts2, None).unwrap();
		assert_eq!(scan_range.len(), 2);
		let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
		for (key, _) in &scan_range {
			found_keys.insert(key.as_ref());
		}
		assert!(found_keys.contains(&b"key2".as_ref()));
		assert!(found_keys.contains(&b"key3".as_ref()));
	}

	fn now() -> u64 {
		std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
			as u64
	}

	#[tokio::test]
	async fn test_scan_at_timestamp_with_deletes() {
		let temp_dir = create_temp_directory();
		let opts: Options =
			Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
		let tree = TreeBuilder::with_options(opts).build().unwrap();

		// Insert data without explicit timestamps (will use auto-generated timestamps)
		let mut tx1 = tree.begin().unwrap();
		tx1.set(b"key1", b"value1").unwrap();
		tx1.set(b"key2", b"value2").unwrap();
		tx1.set(b"key3", b"value3").unwrap();
		tx1.commit().await.unwrap();
		let ts_after_insert = now();

		// Query at this point should show all three keys
		let tx_before = tree.begin().unwrap();
		let scan_before = tx_before
			.scan_at_timestamp(b"key1".to_vec()..=b"key3".to_vec(), ts_after_insert, None)
			.unwrap();
		assert_eq!(scan_before.len(), 3, "Should have all 3 keys before deletes");

		// Delete key2 (hard delete) and soft delete key3
		let mut tx2 = tree.begin().unwrap();
		tx2.delete(b"key2").unwrap();
		tx2.soft_delete(b"key3").unwrap();
		tx2.commit().await.unwrap();
		let ts_after_deletes = now();

		// Test scan_at_timestamp at a time after the deletes
		// Should only return key1 (key2 was hard deleted, key3 was soft deleted)
		let tx = tree.begin().unwrap();

		// Verify key2 is completely gone (hard deleted)
		let versions2 = tx.scan_all_timestamps(b"key2".to_vec()..=b"key2".to_vec(), None).unwrap();
		assert_eq!(versions2.len(), 0, "Hard deleted key should have no versions");

		// Perform scan at timestamp after deletes
		let scan_result = tx
			.scan_at_timestamp(b"key1".to_vec()..=b"key3".to_vec(), ts_after_deletes, None)
			.unwrap();
		assert_eq!(scan_result.len(), 1, "Should have only 1 key after deletes");

		let mut found_keys: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
		for (key, value) in &scan_result {
			found_keys.insert(key.as_ref());
			match key.as_slice() {
				b"key1" => assert_eq!(value.as_ref(), b"value1"),
				_ => panic!("Unexpected key: {:?}", key),
			}
		}
		assert!(found_keys.contains(&b"key1".as_ref()));
		assert!(!found_keys.contains(&b"key2".as_ref())); // Hard deleted
		assert!(!found_keys.contains(&b"key3".as_ref())); // Soft deleted
	}

	#[tokio::test]
	async fn test_scan_all_timestamps() {
		let temp_dir = create_temp_directory();
		let opts: Options =
			Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
		let tree = TreeBuilder::with_options(opts).build().unwrap();

		// Insert data at different timestamps
		let mut tx1 = tree.begin().unwrap();
		tx1.set(b"key1", b"value1_v1").unwrap();
		tx1.set(b"key2", b"value2_v1").unwrap();
		tx1.commit().await.unwrap();

		let mut tx2 = tree.begin().unwrap();
		tx2.set(b"key1", b"value1_v2").unwrap();
		tx2.set(b"key3", b"value3_v1").unwrap();
		tx2.commit().await.unwrap();

		let mut tx3 = tree.begin().unwrap();
		tx3.set(b"key2", b"value2_v2").unwrap();
		tx3.set(b"key4", b"value4_v1").unwrap();
		tx3.commit().await.unwrap();

		// Test scan_all_timestamps
		let tx = tree.begin().unwrap();
		let all_versions =
			tx.scan_all_timestamps(b"key1".to_vec()..=b"key4".to_vec(), None).unwrap();

		// Should get all versions of all keys in the range
		assert_eq!(all_versions.len(), 6); // 2 versions of key1 + 2 versions of key2 + 1 version of key3 + 1 version of key4

		// Group by key to verify we have all versions
		let mut key_versions: KeyVersionsMap = HashMap::new();
		for (key, value, timestamp, is_tombstone) in all_versions {
			key_versions.entry(key).or_default().push((value.to_vec(), timestamp, is_tombstone));
		}

		// Verify key1 has 2 versions
		let key1_versions = key_versions.get_mut(b"key1".as_slice()).unwrap();
		assert_eq!(key1_versions.len(), 2);
		// Sort by timestamp to get chronological order
		key1_versions.sort_by(|a, b| a.1.cmp(&b.1));
		assert_eq!(key1_versions[0].0, b"value1_v1");
		assert_eq!(key1_versions[1].0, b"value1_v2");
		assert!(!key1_versions[0].2); // Not tombstone
		assert!(!key1_versions[1].2); // Not tombstone

		// Verify key2 has 2 versions
		let key2_versions = key_versions.get_mut(b"key2".as_slice()).unwrap();
		assert_eq!(key2_versions.len(), 2);
		key2_versions.sort_by(|a, b| a.1.cmp(&b.1));
		assert_eq!(key2_versions[0].0, b"value2_v1");
		assert_eq!(key2_versions[1].0, b"value2_v2");
		assert!(!key2_versions[0].2); // Not tombstone
		assert!(!key2_versions[1].2); // Not tombstone

		// Verify key3 has 1 version
		let key3_versions = key_versions.get(b"key3".as_slice()).unwrap();
		assert_eq!(key3_versions.len(), 1);
		assert_eq!(key3_versions[0].0, b"value3_v1");
		assert!(!key3_versions[0].2); // Not tombstone

		// Verify key4 has 1 version
		let key4_versions = key_versions.get(b"key4".as_slice()).unwrap();
		assert_eq!(key4_versions.len(), 1);
		assert_eq!(key4_versions[0].0, b"value4_v1");
		assert!(!key4_versions[0].2); // Not tombstone

		// Test with limit
		let limited_versions =
			tx.scan_all_timestamps(b"key1".to_vec()..=b"key4".to_vec(), Some(4)).unwrap();
		assert_eq!(limited_versions.len(), 6);

		// Test with specific key range
		let range_versions =
			tx.scan_all_timestamps(b"key2".to_vec()..=b"key3".to_vec(), None).unwrap();
		assert_eq!(range_versions.len(), 3); // 2 versions of key2 + 1 version of key3
	}

	#[tokio::test]
	async fn test_scan_all_timestamps_with_deletes() {
		let temp_dir = create_temp_directory();
		let opts: Options =
			Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
		let tree = TreeBuilder::with_options(opts).build().unwrap();

		// Insert data
		let mut tx1 = tree.begin().unwrap();
		tx1.set(b"key1", b"value1_v1").unwrap();
		tx1.set(b"key2", b"value2_v1").unwrap();
		tx1.commit().await.unwrap();

		let mut tx2 = tree.begin().unwrap();
		tx2.set(b"key1", b"value1_v2").unwrap();
		tx2.set(b"key2", b"value2_v2").unwrap();
		tx2.commit().await.unwrap();

		let mut tx3 = tree.begin().unwrap();
		tx3.delete(b"key1").unwrap(); // Hard delete
		tx3.soft_delete(b"key2").unwrap(); // Soft delete
		tx3.commit().await.unwrap();

		// Test scan_all_timestamps
		let tx = tree.begin().unwrap();
		let all_versions =
			tx.scan_all_timestamps(b"key1".to_vec()..=b"key2".to_vec(), None).unwrap();

		// Should get all versions including soft delete markers, exclude hard-deleted keys
		assert_eq!(all_versions.len(), 3); // 3 versions of key2 (key1 is hard deleted, soft delete marker included)

		// Group by key to verify we have all versions
		let mut key_versions: KeyVersionsMap = HashMap::new();
		for (key, value, timestamp, is_tombstone) in all_versions {
			key_versions.entry(key).or_default().push((value.to_vec(), timestamp, is_tombstone));
		}

		// Verify key1 is not present (hard deleted)
		assert!(!key_versions.contains_key(b"key1".as_slice()));

		// Verify key2 has 3 versions (2 regular values + 1 soft delete marker)
		let key2_versions = key_versions.get_mut(b"key2".as_slice()).unwrap();
		assert_eq!(key2_versions.len(), 3);
		key2_versions.sort_by(|a, b| a.1.cmp(&b.1));
		assert_eq!(key2_versions[0].0, b"value2_v1");
		assert!(!key2_versions[0].2); // Not tombstone
		assert_eq!(key2_versions[1].0, b"value2_v2");
		assert!(!key2_versions[1].2); // Not tombstone
		assert_eq!(key2_versions[2].0, b""); // Empty value for soft delete
		assert!(key2_versions[2].2); // Is tombstone
	}

	#[tokio::test]
	async fn test_versioned_queries_without_versioning() {
		let temp_dir = create_temp_directory();
		let opts: Options =
			Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(false, 0);
		let tree = TreeBuilder::with_options(opts).build().unwrap();

		// Test that versioned queries fail when versioning is disabled
		let tx = tree.begin().unwrap();
		assert!(tx
			.keys_at_timestamp(b"key1".to_vec()..=b"key2".to_vec(), 123456789, None)
			.is_err());
		assert!(tx
			.scan_at_timestamp(b"key1".to_vec()..=b"key2".to_vec(), 123456789, None)
			.is_err());
		assert!(tx.scan_all_timestamps(b"key1".to_vec()..=b"key2".to_vec(), None).is_err());
	}

	// Version management tests
	mod version_tests {
		use std::collections::HashSet;

		use super::*;

		fn create_tree() -> (Tree, TempDir) {
			let temp_dir = create_temp_directory();
			let opts: Options =
				Options::new().with_path(temp_dir.path().to_path_buf()).with_versioning(true, 0);
			(TreeBuilder::with_options(opts).build().unwrap(), temp_dir)
		}

		#[tokio::test]
		async fn test_insert_multiple_versions_in_same_tx() {
			let (store, _tmp_dir) = create_tree();
			let key = Bytes::from("key1");

			// Insert multiple versions of the same key
			let values = [Bytes::from("value1"), Bytes::from("value2"), Bytes::from("value3")];

			for (i, value) in values.iter().enumerate() {
				let mut txn = store.begin().unwrap();
				let version = (i + 1) as u64; // Incremental version
				txn.set_at_ts(&key, value, version).unwrap();
				txn.commit().await.unwrap();
			}

			let txn = store.begin().unwrap();
			let results: Vec<_> = txn
				.scan_all_timestamps(key.as_ref().to_vec()..=key.as_ref().to_vec(), None)
				.unwrap();

			// Verify that the output contains all the versions of the key
			assert_eq!(results.len(), values.len());
			for (i, (k, v, version, is_deleted)) in results.iter().enumerate() {
				assert_eq!(k, &key);
				assert_eq!(v.as_ref(), values[i].as_ref());
				assert_eq!(*version, (i + 1) as u64);
				assert!(!(*is_deleted));
			}
		}

		#[tokio::test]
		async fn test_scan_all_timestamps_single_key_multiple_versions() {
			let (store, _tmp_dir) = create_tree();
			let key = Bytes::from("key1");

			// Insert multiple versions of the same key
			let values = [Bytes::from("value1"), Bytes::from("value2"), Bytes::from("value3")];

			for (i, value) in values.iter().enumerate() {
				let mut txn = store.begin().unwrap();
				let version = (i + 1) as u64; // Incremental version
				txn.set_at_ts(&key, value, version).unwrap();
				txn.commit().await.unwrap();
			}

			let txn = store.begin().unwrap();
			let results: Vec<_> = txn
				.scan_all_timestamps(key.as_ref().to_vec()..=key.as_ref().to_vec(), None)
				.unwrap();

			// Verify that the output contains all the versions of the key
			assert_eq!(results.len(), values.len());
			for (i, (k, v, version, is_deleted)) in results.iter().enumerate() {
				assert_eq!(k, &key);
				assert_eq!(v.as_ref(), values[i].as_ref());
				assert_eq!(*version, (i + 1) as u64);
				assert!(!(*is_deleted));
			}
		}

		#[tokio::test]
		async fn test_scan_all_timestamps_multiple_keys_single_version_each() {
			let (store, _tmp_dir) = create_tree();
			let keys = vec![Bytes::from("key1"), Bytes::from("key2"), Bytes::from("key3")];
			let value = Bytes::from("value1");

			for key in &keys {
				let mut txn = store.begin().unwrap();
				txn.set_at_ts(key, &value, 1).unwrap();
				txn.commit().await.unwrap();
			}

			let txn = store.begin().unwrap();
			let results: Vec<_> = txn
				.scan_all_timestamps(
					keys.first().unwrap().as_ref().to_vec()
						..=keys.last().unwrap().as_ref().to_vec(),
					None,
				)
				.unwrap();

			assert_eq!(results.len(), keys.len());
			for (i, (k, v, version, is_deleted)) in results.iter().enumerate() {
				assert_eq!(k, &keys[i]);
				assert_eq!(v.as_ref(), value.as_ref());
				assert_eq!(*version, 1);
				assert!(!(*is_deleted));
			}
		}

		#[tokio::test]
		async fn test_scan_all_timestamps_multiple_keys_multiple_versions_each() {
			let (store, _tmp_dir) = create_tree();
			let keys = vec![Bytes::from("key1"), Bytes::from("key2"), Bytes::from("key3")];
			let values = [Bytes::from("value1"), Bytes::from("value2"), Bytes::from("value3")];

			for key in &keys {
				for (i, value) in values.iter().enumerate() {
					let mut txn = store.begin().unwrap();
					let version = (i + 1) as u64;
					txn.set_at_ts(key, value, version).unwrap();
					txn.commit().await.unwrap();
				}
			}

			let txn = store.begin().unwrap();
			let results: Vec<_> = txn
				.scan_all_timestamps(
					keys.first().unwrap().as_ref().to_vec()
						..=keys.last().unwrap().as_ref().to_vec(),
					None,
				)
				.unwrap();

			let mut expected_results = Vec::new();
			for key in &keys {
				for (i, value) in values.iter().enumerate() {
					expected_results.push((key.clone(), value.clone(), (i + 1) as u64, false));
				}
			}

			assert_eq!(results.len(), expected_results.len());
			for (result, expected) in results.iter().zip(expected_results.iter()) {
				let (k, v, version, is_deleted) = result;
				let (expected_key, expected_value, expected_version, expected_is_deleted) =
					expected;
				assert_eq!(k, expected_key);
				assert_eq!(v.as_ref(), expected_value.as_ref());
				assert_eq!(*version, *expected_version);
				assert_eq!(*is_deleted, *expected_is_deleted);
			}
		}

		#[tokio::test]
		async fn test_scan_all_timestamps_deleted_records() {
			let (store, _tmp_dir) = create_tree();
			let key = Bytes::from("key1");
			let value = Bytes::from("value1");

			let mut txn = store.begin().unwrap();
			txn.set_at_ts(&key, &value, 1).unwrap();
			txn.commit().await.unwrap();

			let mut txn = store.begin().unwrap();
			txn.soft_delete(&key).unwrap();
			txn.commit().await.unwrap();

			let txn = store.begin().unwrap();
			let results: Vec<_> = txn
				.scan_all_timestamps(key.as_ref().to_vec()..=key.as_ref().to_vec(), None)
				.unwrap();

			assert_eq!(results.len(), 2);
			let (k, v, version, is_deleted) = &results[0];
			assert_eq!(k, &key);
			assert_eq!(v.as_ref(), value.as_ref());
			assert_eq!(*version, 1);
			assert!(!(*is_deleted));

			let (k, v, _, is_deleted) = &results[1];
			assert_eq!(k, &key);
			assert_eq!(v.as_ref(), Bytes::new().as_ref());
			assert!(*is_deleted);
		}

		#[tokio::test]
		async fn test_scan_all_timestamps_multiple_keys_single_version_each_deleted() {
			let (store, _tmp_dir) = create_tree();
			let keys = vec![Bytes::from("key1"), Bytes::from("key2"), Bytes::from("key3")];
			let value = Bytes::from("value1");

			for key in &keys {
				let mut txn = store.begin().unwrap();
				txn.set_at_ts(key, &value, 1).unwrap();
				txn.commit().await.unwrap();
			}

			for key in &keys {
				let mut txn = store.begin().unwrap();
				txn.soft_delete(key).unwrap();
				txn.commit().await.unwrap();
			}

			let txn = store.begin().unwrap();
			let results: Vec<_> = txn
				.scan_all_timestamps(
					keys.first().unwrap().as_ref().to_vec()
						..=keys.last().unwrap().as_ref().to_vec(),
					None,
				)
				.unwrap();

			assert_eq!(results.len(), keys.len() * 2);
			for (i, (k, v, version, is_deleted)) in results.iter().enumerate() {
				let key_index = i / 2;
				let is_deleted_version = i % 2 == 1;
				assert_eq!(k, &keys[key_index]);
				if is_deleted_version {
					assert_eq!(v.as_ref(), &Bytes::new());
					assert!(*is_deleted);
				} else {
					assert_eq!(v.as_ref(), &value);
					assert_eq!(*version, 1);
					assert!(!(*is_deleted));
				}
			}
		}

		#[tokio::test]
		async fn test_scan_all_timestamps_multiple_keys_multiple_versions_each_deleted() {
			let (store, _tmp_dir) = create_tree();
			let keys = vec![Bytes::from("key1"), Bytes::from("key2"), Bytes::from("key3")];
			let values = [Bytes::from("value1"), Bytes::from("value2"), Bytes::from("value3")];

			for key in &keys {
				for (i, value) in values.iter().enumerate() {
					let mut txn = store.begin().unwrap();
					let version = (i + 1) as u64;
					txn.set_at_ts(key, value, version).unwrap();
					txn.commit().await.unwrap();
				}
			}

			for key in &keys {
				let mut txn = store.begin().unwrap();
				txn.soft_delete(key).unwrap();
				txn.commit().await.unwrap();
			}

			let txn = store.begin().unwrap();
			let results: Vec<_> = txn
				.scan_all_timestamps(
					keys.first().unwrap().as_ref().to_vec()
						..=keys.last().unwrap().as_ref().to_vec(),
					None,
				)
				.unwrap();

			let mut expected_results = Vec::new();
			for key in &keys {
				for (i, value) in values.iter().enumerate() {
					expected_results.push((key.clone(), value.clone(), (i + 1) as u64, false));
				}
				expected_results.push((key.clone(), Bytes::new(), 0, true));
			}

			assert_eq!(results.len(), expected_results.len());
			for (result, expected) in results.iter().zip(expected_results.iter()) {
				let (k, v, version, is_deleted) = result;
				let (expected_key, expected_value, expected_version, expected_is_deleted) =
					expected;
				assert_eq!(k, expected_key);
				assert_eq!(v.as_ref(), expected_value);
				if !expected_is_deleted {
					assert_eq!(*version, *expected_version);
				}
				assert_eq!(*is_deleted, *expected_is_deleted);
			}
		}

		#[tokio::test]
		async fn test_scan_all_timestamps_soft_and_hard_delete() {
			let (store, _tmp_dir) = create_tree();
			let key = Bytes::from("key1");
			let value = Bytes::from("value1");

			let mut txn = store.begin().unwrap();
			txn.set_at_ts(&key, &value, 1).unwrap();
			txn.commit().await.unwrap();

			let mut txn = store.begin().unwrap();
			txn.soft_delete(&key).unwrap();
			txn.commit().await.unwrap();

			let mut txn = store.begin().unwrap();
			txn.delete(&key).unwrap();
			txn.commit().await.unwrap();

			let txn = store.begin().unwrap();
			let results: Vec<_> = txn
				.scan_all_timestamps(key.as_ref().to_vec()..=key.as_ref().to_vec(), None)
				.unwrap();

			assert_eq!(results.len(), 0);
		}

		#[tokio::test]
		async fn test_scan_all_timestamps_range_boundaries() {
			let (store, _tmp_dir) = create_tree();
			let keys = vec![Bytes::from("key1"), Bytes::from("key2"), Bytes::from("key3")];
			let value = Bytes::from("value1");

			for key in &keys {
				let mut txn = store.begin().unwrap();
				txn.set_at_ts(key, &value, 1).unwrap();
				txn.commit().await.unwrap();
			}

			// Inclusive range
			let txn = store.begin().unwrap();
			let results: Vec<_> = txn
				.scan_all_timestamps(
					keys.first().unwrap().as_ref().to_vec()
						..=keys.last().unwrap().as_ref().to_vec(),
					None,
				)
				.unwrap();
			assert_eq!(results.len(), keys.len());
		}

		#[tokio::test]
		async fn test_scan_all_timestamps_with_limit() {
			let (store, _tmp_dir) = create_tree();
			let keys = vec![Bytes::from("key1"), Bytes::from("key2"), Bytes::from("key3")];
			let value = Bytes::from("value1");

			for key in &keys {
				let mut txn = store.begin().unwrap();
				txn.set_at_ts(key, &value, 1).unwrap();
				txn.commit().await.unwrap();
			}

			let txn = store.begin().unwrap();
			let results: Vec<_> = txn
				.scan_all_timestamps(
					keys.first().unwrap().as_ref().to_vec()
						..=keys.last().unwrap().as_ref().to_vec(),
					Some(2),
				)
				.unwrap();

			assert_eq!(results.len(), 2);
		}

		#[tokio::test]
		async fn test_scan_all_timestamps_single_key_single_version() {
			let (store, _tmp_dir) = create_tree();
			let key = Bytes::from("key1");
			let value = Bytes::from("value1");

			let mut txn = store.begin().unwrap();
			txn.set_at_ts(&key, &value, 1).unwrap();
			txn.commit().await.unwrap();

			let txn = store.begin().unwrap();
			let results: Vec<_> = txn
				.scan_all_timestamps(key.as_ref().to_vec()..=key.as_ref().to_vec(), None)
				.unwrap();

			assert_eq!(results.len(), 1);
			let (k, v, version, is_deleted) = &results[0];
			assert_eq!(k, &key);
			assert_eq!(v.as_ref(), &value);
			assert_eq!(*version, 1);
			assert!(!(*is_deleted));
		}

		#[tokio::test]
		async fn test_scan_all_timestamps_with_limit_with_multiple_versions_per_key() {
			let (store, _tmp_dir) = create_tree();
			let keys = vec![Bytes::from("key1"), Bytes::from("key2"), Bytes::from("key3")];
			let values = [Bytes::from("value1"), Bytes::from("value2"), Bytes::from("value3")];

			// Insert multiple versions for each key
			for key in &keys {
				for (i, value) in values.iter().enumerate() {
					let mut txn = store.begin().unwrap();
					let version = (i + 1) as u64;
					txn.set_at_ts(key, value, version).unwrap();
					txn.commit().await.unwrap();
				}
			}

			let txn = store.begin().unwrap();
			let results: Vec<_> = txn
				.scan_all_timestamps(
					keys.first().unwrap().as_ref().to_vec()
						..=keys.last().unwrap().as_ref().to_vec(),
					Some(2),
				)
				.unwrap();
			assert_eq!(results.len(), 6); // 3 versions for each of 2 keys

			// Collect unique keys from the results
			let unique_keys: HashSet<_> = results.iter().map(|(k, _, _, _)| k.to_vec()).collect();

			// Verify that the number of unique keys is equal to the limit
			assert_eq!(unique_keys.len(), 2);

			// Verify that the results contain all versions for each key
			for key in unique_keys {
				let key_versions: Vec<_> =
					results.iter().filter(|(k, _, _, _)| k == &key).collect();

				assert_eq!(key_versions.len(), 3); // Should have all 3 versions

				// Check the latest version
				let latest = key_versions.iter().max_by_key(|(_, _, version, _)| version).unwrap();
				assert_eq!(latest.1.as_ref(), *values.last().unwrap());
				assert_eq!(latest.2, values.len() as u64);
			}
		}

		#[tokio::test]
		async fn test_scan_all_timestamps_with_subsets() {
			let (store, _tmp_dir) = create_tree();
			let keys = vec![
				Bytes::from("key1"),
				Bytes::from("key2"),
				Bytes::from("key3"),
				Bytes::from("key4"),
				Bytes::from("key5"),
				Bytes::from("key6"),
				Bytes::from("key7"),
			];
			let values = [Bytes::from("value1"), Bytes::from("value2"), Bytes::from("value3")];

			// Insert multiple versions for each key
			for key in &keys {
				for (i, value) in values.iter().enumerate() {
					let mut txn = store.begin().unwrap();
					let version = (i + 1) as u64;
					txn.set_at_ts(key, value, version).unwrap();
					txn.commit().await.unwrap();
				}
			}

			// Define subsets of the entire range
			let subsets = vec![
				(keys[0].as_ref(), keys[2].as_ref()),
				(keys[1].as_ref(), keys[3].as_ref()),
				(keys[2].as_ref(), keys[4].as_ref()),
				(keys[3].as_ref(), keys[5].as_ref()),
				(keys[4].as_ref(), keys[6].as_ref()),
			];

			// Scan each subset and collect versions
			for subset in subsets {
				let txn = store.begin().unwrap();
				let results: Vec<_> =
					txn.scan_all_timestamps(subset.0.to_vec()..=subset.1.to_vec(), None).unwrap();

				// Collect unique keys from the results
				let unique_keys: HashSet<_> =
					results.iter().map(|(k, _, _, _)| k.to_vec()).collect();

				// Verify that the results contain all versions for each key in the subset
				for key in unique_keys {
					for (i, value) in values.iter().enumerate() {
						let version = (i + 1) as u64;
						let result = results
							.iter()
							.find(|(k, v, ver, _)| {
								k == &key && v.as_ref() == value && *ver == version
							})
							.unwrap();
						assert_eq!(result.1.as_ref(), *value);
						assert_eq!(result.2, version);
					}
				}
			}
		}

		#[tokio::test]
		async fn test_scan_all_timestamps_range_bounds() {
			let (store, _tmp_dir) = create_tree();

			// Insert test data with multiple versions
			let mut txn = store.begin().unwrap();
			txn.set_at_ts(b"key1", b"value1", 1).unwrap();
			txn.set_at_ts(b"key1", b"value1_v2", 2).unwrap();
			txn.set_at_ts(b"key2", b"value2", 1).unwrap();
			txn.set_at_ts(b"key2", b"value2_v2", 2).unwrap();
			txn.set_at_ts(b"key3", b"value3", 1).unwrap();
			txn.set_at_ts(b"key4", b"value4", 1).unwrap();
			txn.set_at_ts(b"key5", b"value5", 1).unwrap();

			txn.commit().await.unwrap();

			// Test 1: Unbounded range should return all keys
			let txn = store.begin().unwrap();
			let results = txn.scan_all_timestamps(.., None).unwrap();
			assert_eq!(results.len(), 5); // 5 total versions (only latest per key)
								 // Check if the results are correct
			assert_eq!(
				results,
				vec![
					(b"key1".to_vec(), Arc::from(b"value1_v2".as_slice()), 2, false),
					(b"key2".to_vec(), Arc::from(b"value2_v2".as_slice()), 2, false),
					(b"key3".to_vec(), Arc::from(b"value3".as_slice()), 1, false),
					(b"key4".to_vec(), Arc::from(b"value4".as_slice()), 1, false),
					(b"key5".to_vec(), Arc::from(b"value5".as_slice()), 1, false),
				]
			);

			// Test 2: Range from key2 (inclusive) should exclude key1
			let results = txn.scan_all_timestamps(b"key2".to_vec().., None).unwrap();
			assert_eq!(results.len(), 4); // key2, key3, key4, key5 versions

			// Test 3: Range excluding key2 should exclude key2 but include others
			let results = txn
				.scan_all_timestamps((Bound::Excluded(b"key2".to_vec()), Bound::Unbounded), None)
				.unwrap();
			assert_eq!(results.len(), 3); // key3, key4, key5 versions

			// Test 4: Range excluding key5 should exclude key5
			let results = txn
				.scan_all_timestamps((Bound::Excluded(b"key5".to_vec()), Bound::Unbounded), None)
				.unwrap();
			assert_eq!(results.len(), 0); // Should be empty!
		}

		#[tokio::test]
		async fn test_scan_all_timestamps_with_batches() {
			let (store, _tmp_dir) = create_tree();
			let keys = [
				Bytes::from("key1"),
				Bytes::from("key2"),
				Bytes::from("key3"),
				Bytes::from("key4"),
				Bytes::from("key5"),
			];
			let versions = [
				vec![Bytes::from("v1"), Bytes::from("v2"), Bytes::from("v3"), Bytes::from("v4")],
				vec![Bytes::from("v1"), Bytes::from("v2")],
				vec![Bytes::from("v1"), Bytes::from("v2"), Bytes::from("v3"), Bytes::from("v4")],
				vec![Bytes::from("v1")],
				vec![Bytes::from("v1")],
			];

			// Insert multiple versions for each key
			for (key, key_versions) in keys.iter().zip(versions.iter()) {
				for (i, value) in key_versions.iter().enumerate() {
					let mut txn = store.begin().unwrap();
					let version = (i + 1) as u64;
					txn.set_at_ts(key, value, version).unwrap();
					txn.commit().await.unwrap();
				}
			}

			// Set the batch size
			let batch_size: usize = 2;

			// Define a function to scan in batches
			fn scan_in_batches(
				store: &Tree,
				batch_size: usize,
			) -> Vec<Vec<(Bytes, Bytes, u64, bool)>> {
				let mut all_results = Vec::new();
				let mut last_key = Vec::new();
				let mut first_iteration = true;

				loop {
					let txn = store.begin().unwrap();

					// Create range using a clone of last_key
					let range = if first_iteration {
						(Bound::Unbounded, Bound::Unbounded)
					} else {
						(Bound::Excluded(last_key.clone()), Bound::Unbounded)
					};

					let mut batch_results = Vec::new();
					let results = txn.scan_all_timestamps(range, Some(batch_size)).unwrap();
					for (k, v, ts, is_deleted) in results {
						// Convert borrowed key to owned immediately
						let key_bytes = Bytes::copy_from_slice(k.as_ref());
						let val_bytes = Bytes::from(v.to_vec());
						batch_results.push((key_bytes, val_bytes, ts, is_deleted));

						// Update last_key with a new vector
						last_key = k.to_vec();
					}

					if batch_results.is_empty() {
						break;
					}

					first_iteration = false;
					all_results.push(batch_results);
				}

				all_results
			}

			// Scan in batches and collect the results
			let all_results = scan_in_batches(&store, batch_size);

			// Verify the results
			let expected_results = [
				vec![
					(Bytes::from("key1"), Bytes::from("v1"), 1, false),
					(Bytes::from("key1"), Bytes::from("v2"), 2, false),
					(Bytes::from("key1"), Bytes::from("v3"), 3, false),
					(Bytes::from("key1"), Bytes::from("v4"), 4, false),
					(Bytes::from("key2"), Bytes::from("v1"), 1, false),
					(Bytes::from("key2"), Bytes::from("v2"), 2, false),
				],
				vec![
					(Bytes::from("key3"), Bytes::from("v1"), 1, false),
					(Bytes::from("key3"), Bytes::from("v2"), 2, false),
					(Bytes::from("key3"), Bytes::from("v3"), 3, false),
					(Bytes::from("key3"), Bytes::from("v4"), 4, false),
					(Bytes::from("key4"), Bytes::from("v1"), 1, false),
				],
				vec![(Bytes::from("key5"), Bytes::from("v1"), 1, false)],
			];

			assert_eq!(all_results.len(), expected_results.len());

			for (batch, expected_batch) in all_results.iter().zip(expected_results.iter()) {
				assert_eq!(batch.len(), expected_batch.len());
				for (result, expected) in batch.iter().zip(expected_batch.iter()) {
					assert_eq!(result, expected);
				}
			}
		}
	}
}
