use std::cmp::Ordering;
use std::collections::btree_map::Entry as BTreeEntry;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::lsm::Core;
#[cfg(test)]
use crate::snapshot::HistoryIteratorStats;
use crate::snapshot::{HistoryIterator, MergeDirection, Snapshot, SnapshotIterator};
use crate::{
	InternalIterator,
	InternalKey,
	InternalKeyKind,
	InternalKeyRef,
	IntoBytes,
	Key,
	Value,
};

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

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct TransactionOptions {
	pub mode: Mode,
	pub durability: Durability,
}

impl TransactionOptions {
	pub fn read_only() -> Self {
		Self::new_with_mode(Mode::ReadOnly)
	}

	pub fn write_only() -> Self {
		Self::new_with_mode(Mode::WriteOnly)
	}

	pub fn new() -> Self {
		Self::new_with_mode(Mode::ReadWrite)
	}

	pub fn new_with_mode(mode: Mode) -> Self {
		Self {
			mode,
			durability: Default::default(),
		}
	}

	pub fn with_durability(mut self, durability: Durability) -> Self {
		self.durability = durability;
		self
	}
}

impl Default for TransactionOptions {
	fn default() -> Self {
		Self::new()
	}
}

/// Options for write operations in transactions.
/// This struct allows configuring various parameters for write operations
/// like set() and delete().
#[derive(Default, Debug, Clone, PartialEq)]
pub struct WriteOptions {
	/// Optional timestamp for the write operation. If None, uses the current
	/// timestamp.
	pub timestamp: Option<u64>,
}

impl WriteOptions {
	/// Creates a new WriteOptions with default values
	pub fn new() -> Self {
		Self::default()
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

/// Options for history (versioned) iteration.
/// Controls what versions and tombstones are included in the iteration.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct HistoryOptions {
	/// Whether to include tombstones (deleted entries) in the iteration.
	/// Default: false
	pub include_tombstones: bool,
	/// Optional timestamp range filter (start_ts, end_ts) inclusive.
	/// Only versions within this range are returned.
	/// Default: None (no timestamp filtering)
	pub ts_range: Option<(u64, u64)>,
	/// Optional limit on the total number of entries/versions to return.
	/// Default: None (no limit)
	pub limit: Option<usize>,
}

impl HistoryOptions {
	/// Creates a new HistoryOptions with default values (no tombstones, no filters).
	pub fn new() -> Self {
		Self::default()
	}

	/// Include tombstones (soft-deleted entries) in the iteration.
	pub fn with_tombstones(mut self, include: bool) -> Self {
		self.include_tombstones = include;
		self
	}

	/// Set a timestamp range filter. Only versions within [start_ts, end_ts] are returned.
	pub fn with_ts_range(mut self, start_ts: u64, end_ts: u64) -> Self {
		self.ts_range = Some((start_ts, end_ts));
		self
	}

	/// Set a limit on the total number of entries/versions to return.
	pub fn with_limit(mut self, limit: usize) -> Self {
		self.limit = Some(limit);
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

	/// The sequence number when this transaction started.
	pub(crate) start_seq_num: u64,

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
	pub(crate) fn new(core: Arc<Core>, opts: TransactionOptions) -> Result<Self> {
		let TransactionOptions {
			mode,
			durability,
		} = opts;

		// Get the current visible sequence number as our start point.
		let start_seq_num = core.seq_num();

		let mut snapshot = None;
		if !mode.is_write_only() {
			snapshot = Some(Snapshot::new(Arc::clone(&core), start_seq_num));
		}

		Ok(Self {
			mode,
			snapshot,
			core,
			write_set: BTreeMap::new(),
			durability,
			closed: false,
			start_seq_num,
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
	pub fn set_at<K, V>(&mut self, key: K, value: V, timestamp: u64) -> Result<()>
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
		let ts = options.timestamp.unwrap_or(Entry::COMMIT_TIME);

		let entry =
			Entry::new(key, Some(value), InternalKeyKind::Set, self.savepoints, write_seqno, ts);

		self.write(entry)?;
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
		let ts = options.timestamp.unwrap_or(Entry::COMMIT_TIME);

		let entry = Entry::new(
			key,
			None::<&[u8]>,
			InternalKeyKind::Delete,
			self.savepoints,
			write_seqno,
			ts,
		);
		self.write(entry)?;
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
	pub fn soft_delete_at<K>(&mut self, key: K, timestamp: u64) -> Result<()>
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
		let ts = options.timestamp.unwrap_or(Entry::COMMIT_TIME);

		let entry = Entry::new(
			key,
			None::<&[u8]>,
			InternalKeyKind::SoftDelete,
			self.savepoints,
			write_seqno,
			ts,
		);
		self.write(entry)?;
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
		let ts = options.timestamp.unwrap_or(Entry::COMMIT_TIME);

		let entry = Entry::new(
			key,
			Some(value),
			InternalKeyKind::Replace,
			self.savepoints,
			write_seqno,
			ts,
		);

		self.write(entry)?;
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
	pub fn get_at<K>(&self, key: K, timestamp: u64) -> Result<Option<Value>>
	where
		K: IntoBytes,
	{
		self.get_at_with_options(key, &ReadOptions::default().with_timestamp(Some(timestamp)))
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

	pub fn get_at_with_options<K>(&self, key: K, options: &ReadOptions) -> Result<Option<Value>>
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
			match &self.snapshot {
				Some(snapshot) => snapshot.get_at(key.as_slice(), timestamp),
				None => Err(Error::NoSnapshot),
			}
		} else {
			Err(Error::InvalidArgument("Timestamp is required for versioned queries".to_string()))
		}
	}

	/// Gets keys in a key range at a specific timestamp.
	///
	/// The returned iterator is a double ended iterator
	/// that can be used to iterate over the keys in the
	/// range in both forward and backward directions.
	///
	/// The iterator iterates over all keys in the range,
	/// Gets keys and values in a range, at the current timestamp.
	///
	/// Returns a cursor-based iterator with explicit seek/next/prev methods.
	/// Use `seek_first()` to position at the first key, then `next()` to iterate.
	///
	/// The iterator iterates over all keys and values in the
	/// range, inclusive of the start key, but not the end key.
	///
	/// # Example
	/// ```ignore
	/// let mut iter = tx.range(b"a", b"z")?;
	/// iter.seek_first()?;
	/// while iter.valid() {
	///     let key = iter.key();
	///     let value = iter.value()?;
	///     iter.next()?;
	/// }
	/// ```
	pub fn range<K>(&self, start: K, end: K) -> Result<TransactionIterator<'_>>
	where
		K: IntoBytes,
	{
		let mut options = ReadOptions::default();
		options.set_iterate_bounds(Some(start.into_bytes()), Some(end.into_bytes()));
		self.range_with_options(&options)
	}

	/// Gets keys and values in a range, with custom read options.
	///
	/// Returns a cursor-based iterator with explicit seek/next/prev methods.
	///
	/// The iterator iterates over all keys and values in the
	/// range, inclusive of the start key, but not the end key.
	pub fn range_with_options(&self, options: &ReadOptions) -> Result<TransactionIterator<'_>> {
		let start_key = options.lower_bound.clone().unwrap_or_default();
		let end_key = options.upper_bound.clone().unwrap_or_default();
		let inner = TransactionRangeIterator::new_with_options(self, start_key, end_key)?;
		Ok(TransactionIterator::new(inner, Arc::clone(&self.core)))
	}

	/// Returns a unified history iterator over ALL versions of keys in the range.
	///
	/// This method returns a `TransactionHistoryIterator` that implements `InternalIterator`,
	/// providing a unified streaming API regardless of whether the B+tree index is enabled.
	///
	/// - If B+tree index is enabled: Uses streaming B+tree iteration
	/// - If B+tree index is disabled: Uses LSM-based versioned iteration
	///
	/// # Arguments
	/// * `start` - Start key (inclusive)
	/// * `end` - End key (exclusive)
	///
	/// # Example
	/// ```ignore
	/// let mut iter = tx.history(b"a", b"z")?;
	/// iter.seek_first()?;
	/// while iter.valid() {
	///     println!("key={:?} ts={} value={:?}",
	///         iter.key(), iter.timestamp(), iter.value()?);
	///     iter.next()?;
	/// }
	/// ```
	pub fn history<K>(&self, start: K, end: K) -> Result<TransactionHistoryIterator<'_>>
	where
		K: IntoBytes,
	{
		self.history_with_options(start, end, &HistoryOptions::default())
	}

	/// Returns a unified history iterator with custom options.
	///
	/// This method allows fine-grained control over the history iteration,
	/// including whether to include tombstones and optional timestamp filtering.
	///
	/// # Arguments
	/// * `start` - Start key (inclusive)
	/// * `end` - End key (exclusive)
	/// * `opts` - History iteration options
	pub fn history_with_options<K>(
		&self,
		start: K,
		end: K,
		opts: &HistoryOptions,
	) -> Result<TransactionHistoryIterator<'_>>
	where
		K: IntoBytes,
	{
		if self.closed {
			return Err(Error::TransactionClosed);
		}
		if self.mode.is_write_only() {
			return Err(Error::TransactionWriteOnly);
		}

		// Check if versioning is enabled
		if !self.core.opts.enable_versioning {
			return Err(Error::InvalidArgument("Versioning not enabled".to_string()));
		}

		let snapshot = self.snapshot.as_ref().ok_or(Error::NoSnapshot)?;

		// Use unified history_iter() which chooses the appropriate backend
		let inner = snapshot.history_iter(
			Some(start.as_slice()),
			Some(end.as_slice()),
			opts.include_tombstones,
			opts.ts_range,
			opts.limit,
		)?;

		Ok(TransactionHistoryIterator::new(inner, Arc::clone(&self.core)))
	}

	/// Writes a value for a key with custom write options. None is used for
	/// deletion.
	fn write(&mut self, e: Entry) -> Result<()> {
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

		// Add the entry to the write set
		let key = e.key.clone();

		match self.write_set.entry(key) {
			BTreeEntry::Occupied(mut oe) => {
				let entries = oe.get_mut();
				// If the latest existing value for this key belongs to the same
				// savepoint as the value we are about to write, then we can
				// overwrite it with the new value (same savepoint = same transaction state).
				// For different savepoints, we add a new entry to support savepoint rollbacks.
				//
				// Exception: When using explicit timestamps (set_at), entries with
				// different timestamps should be preserved as separate versions, not replaced.
				if let Some(last_entry) = entries.last() {
					if last_entry.savepoint_no == e.savepoint_no {
						// Same savepoint - check if timestamps differ
						// If both have explicit timestamps and they're different,
						// preserve both as separate versions
						let last_has_explicit_ts = last_entry.timestamp != Entry::COMMIT_TIME;
						let new_has_explicit_ts = e.timestamp != Entry::COMMIT_TIME;

						if last_has_explicit_ts
							&& new_has_explicit_ts
							&& last_entry.timestamp != e.timestamp
						{
							// Different explicit timestamps - keep both versions
							entries.push(e);
						} else {
							// Same timestamp or using commit time - replace
							*entries.last_mut().unwrap() = e;
						}
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

		// This checks if any key in our write set was modified after we started.
		self.validate_write_conflicts()?;

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
			// Use the entry's timestamp if it was explicitly set (via set_at),
			// otherwise use the commit timestamp
			let timestamp = if entry.timestamp != Entry::COMMIT_TIME {
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

	/// Validates that no key in our write set was modified after we started.
	/// Only checks memtables - returns TransactionRetry if history insufficient.
	fn validate_write_conflicts(&self) -> Result<()> {
		// Early check: is memtable history sufficient?
		// If our transaction started before the oldest memtable was created,
		// we can't reliably check for conflicts (data may have been flushed to SST).
		let earliest_memtable_seq = self.core.inner.get_earliest_memtable_seq()?;
		if self.start_seq_num < earliest_memtable_seq {
			return Err(Error::TransactionRetry);
		}

		// Check all keys in one batch
		self.core
			.inner
			.check_keys_conflict(self.write_set.keys().map(|k| k.as_slice()), self.start_seq_num)
	}

	pub fn rollback(&mut self) {
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
	const COMMIT_TIME: u64 = 0;

	fn new<K: IntoBytes, V: IntoBytes>(
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

/// Current source for the transaction iterator
#[derive(Clone, Copy, PartialEq)]
enum CurrentSource {
	Snapshot,
	WriteSet,
	None,
}

/// An iterator that performs a merging scan over a transaction's snapshot and
/// write set. Implements InternalIterator for zero-copy iteration.
pub(crate) struct TransactionRangeIterator<'a> {
	/// Snapshot iterator (implements InternalIterator)
	snapshot_iter: SnapshotIterator<'a>,

	/// Write-set entries for the range (collected, filtered for tombstones on access)
	write_set_entries: Vec<(&'a Key, &'a Entry)>,

	/// Current position in write-set entries (forward iteration)
	ws_pos: usize,

	/// Current position in write-set entries (backward iteration)
	ws_pos_back: usize,

	/// Current source
	current_source: CurrentSource,

	/// Buffer for write-set encoded key
	ws_encoded_key_buf: Vec<u8>,

	/// Direction and initialization state
	direction: MergeDirection,
	initialized: bool,
}

impl<'a> TransactionRangeIterator<'a> {
	/// Creates a new range iterator with custom read options
	pub(crate) fn new_with_options(
		tx: &'a Transaction,
		start_key: Vec<u8>,
		end_key: Vec<u8>,
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

		// Create a snapshot iterator for the range (now returns SnapshotIterator directly)
		let snapshot_iter = snapshot.range(Some(start_key.as_slice()), Some(end_key.as_slice()))?;

		// Collect write-set entries for the range
		// We collect references to avoid cloning, and filter tombstones during iteration
		let mut write_set_entries: Vec<(&'a Key, &'a Entry)> = Vec::new();
		for (key, entry_list) in tx.write_set.range(start_key..end_key) {
			if let Some(entry) = entry_list.last() {
				write_set_entries.push((key, entry));
			}
		}

		let ws_len = write_set_entries.len();

		Ok(Self {
			snapshot_iter,
			write_set_entries,
			ws_pos: 0,
			ws_pos_back: ws_len,
			current_source: CurrentSource::None,
			ws_encoded_key_buf: Vec::new(),
			direction: MergeDirection::Forward,
			initialized: false,
		})
	}

	/// Check if current write-set position is valid (forward)
	fn ws_valid(&self) -> bool {
		self.ws_pos < self.ws_pos_back
	}

	/// Check if current write-set position is valid (backward)
	fn ws_valid_back(&self) -> bool {
		self.ws_pos_back > self.ws_pos
	}

	/// Get current write-set key (forward)
	fn ws_key(&self) -> &[u8] {
		debug_assert!(self.ws_valid());
		self.write_set_entries[self.ws_pos].0.as_slice()
	}

	/// Get current write-set key (backward)
	fn ws_key_back(&self) -> &[u8] {
		debug_assert!(self.ws_valid_back());
		self.write_set_entries[self.ws_pos_back - 1].0.as_slice()
	}

	/// Check if current write-set entry is a tombstone (forward)
	fn ws_is_tombstone(&self) -> bool {
		debug_assert!(self.ws_valid());
		self.write_set_entries[self.ws_pos].1.is_tombstone()
	}

	/// Check if current write-set entry is a tombstone (backward)
	fn ws_is_tombstone_back(&self) -> bool {
		debug_assert!(self.ws_valid_back());
		self.write_set_entries[self.ws_pos_back - 1].1.is_tombstone()
	}

	/// Populate encoded key buffer for write-set entry
	fn populate_ws_encoded_key(&mut self) {
		if !self.ws_valid() && !self.ws_valid_back() {
			return;
		}

		let (key, entry) = if self.direction == MergeDirection::Forward {
			if !self.ws_valid() {
				return;
			}
			self.write_set_entries[self.ws_pos]
		} else {
			if !self.ws_valid_back() {
				return;
			}
			self.write_set_entries[self.ws_pos_back - 1]
		};

		self.ws_encoded_key_buf.clear();
		self.ws_encoded_key_buf.extend_from_slice(key);
		// Add trailer (seq_num << 8 | kind) and timestamp
		let trailer = ((entry.seqno as u64) << 8) | (entry.kind as u64);
		self.ws_encoded_key_buf.extend_from_slice(&trailer.to_be_bytes());
		self.ws_encoded_key_buf.extend_from_slice(&entry.timestamp.to_be_bytes());
	}

	/// Position to minimum of two sources (forward merge)
	fn position_to_min(&mut self) -> Result<bool> {
		loop {
			let snap_valid = self.snapshot_iter.valid();
			let ws_valid = self.ws_valid();

			self.current_source = match (snap_valid, ws_valid) {
				(false, false) => CurrentSource::None,
				(true, false) => CurrentSource::Snapshot,
				(false, true) => {
					if self.ws_is_tombstone() {
						// Skip tombstone and continue
						self.ws_pos += 1;
						continue;
					}
					self.populate_ws_encoded_key();
					CurrentSource::WriteSet
				}
				(true, true) => {
					let snap_key = self.snapshot_iter.key().user_key();
					let ws_key = self.ws_key();

					match snap_key.cmp(ws_key) {
						Ordering::Less => {
							// Snapshot key is smaller - return it
							CurrentSource::Snapshot
						}
						Ordering::Greater => {
							// Write-set key is smaller
							if self.ws_is_tombstone() {
								// Skip tombstone that doesn't mask snapshot
								self.ws_pos += 1;
								continue;
							}
							self.populate_ws_encoded_key();
							CurrentSource::WriteSet
						}
						Ordering::Equal => {
							// RYOW: write-set wins
							if self.ws_is_tombstone() {
								// Tombstone masks snapshot - skip both
								self.snapshot_iter.next()?;
								self.ws_pos += 1;
								continue;
							}
							self.snapshot_iter.next()?;
							self.populate_ws_encoded_key();
							CurrentSource::WriteSet
						}
					}
				}
			};
			return Ok(self.current_source != CurrentSource::None);
		}
	}

	/// Position to maximum of two sources (backward merge)
	fn position_to_max(&mut self) -> Result<bool> {
		loop {
			let snap_valid = self.snapshot_iter.valid();
			let ws_valid = self.ws_valid_back();

			self.current_source = match (snap_valid, ws_valid) {
				(false, false) => CurrentSource::None,
				(true, false) => CurrentSource::Snapshot,
				(false, true) => {
					if self.ws_is_tombstone_back() {
						// Skip tombstone and continue
						self.ws_pos_back -= 1;
						continue;
					}
					self.populate_ws_encoded_key();
					CurrentSource::WriteSet
				}
				(true, true) => {
					let snap_key = self.snapshot_iter.key().user_key();
					let ws_key = self.ws_key_back();

					match snap_key.cmp(ws_key) {
						Ordering::Greater => {
							// Snapshot key is larger - return it (backward iteration)
							CurrentSource::Snapshot
						}
						Ordering::Less => {
							// Write-set key is larger
							if self.ws_is_tombstone_back() {
								// Skip tombstone that doesn't mask snapshot
								self.ws_pos_back -= 1;
								continue;
							}
							self.populate_ws_encoded_key();
							CurrentSource::WriteSet
						}
						Ordering::Equal => {
							// RYOW: write-set wins
							if self.ws_is_tombstone_back() {
								// Tombstone masks snapshot - skip both
								self.snapshot_iter.prev()?;
								self.ws_pos_back -= 1;
								continue;
							}
							self.snapshot_iter.prev()?;
							self.populate_ws_encoded_key();
							CurrentSource::WriteSet
						}
					}
				}
			};
			return Ok(self.current_source != CurrentSource::None);
		}
	}

	/// Returns true if the current value is from the write-set (not snapshot)
	/// Used to skip ValueLocation decode for raw write-set values
	pub(crate) fn is_write_set_value(&self) -> bool {
		self.current_source == CurrentSource::WriteSet
	}
}

impl InternalIterator for TransactionRangeIterator<'_> {
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.initialized = true;

		// Seek snapshot
		self.snapshot_iter.seek(target)?;

		// Binary search in write-set entries
		let user_key = InternalKey::user_key_from_encoded(target);
		self.ws_pos = self.write_set_entries.partition_point(|(k, _)| k.as_slice() < user_key);

		self.position_to_min()
	}

	fn seek_first(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.initialized = true;

		// Seek snapshot to first
		self.snapshot_iter.seek_first()?;

		// Reset write-set position
		self.ws_pos = 0;

		self.position_to_min()
	}

	fn seek_last(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Backward;
		self.initialized = true;

		// Seek snapshot to last
		self.snapshot_iter.seek_last()?;

		// Reset write-set position to end
		self.ws_pos_back = self.write_set_entries.len();

		self.position_to_max()
	}

	fn next(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_first();
		}

		// Advance current source
		match self.current_source {
			CurrentSource::Snapshot => {
				self.snapshot_iter.next()?;
			}
			CurrentSource::WriteSet => {
				self.ws_pos += 1;
			}
			CurrentSource::None => return Ok(false),
		}

		self.position_to_min()
	}

	fn prev(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_last();
		}

		// If we were going forward, switch to backward
		if self.direction != MergeDirection::Backward {
			return self.seek_last();
		}

		// Advance current source (backward)
		match self.current_source {
			CurrentSource::Snapshot => {
				self.snapshot_iter.prev()?;
			}
			CurrentSource::WriteSet => {
				self.ws_pos_back -= 1;
			}
			CurrentSource::None => return Ok(false),
		}

		self.position_to_max()
	}

	fn valid(&self) -> bool {
		self.current_source != CurrentSource::None
	}

	fn key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.valid());
		match self.current_source {
			CurrentSource::Snapshot => self.snapshot_iter.key(),
			CurrentSource::WriteSet => InternalKeyRef::from_encoded(&self.ws_encoded_key_buf),
			CurrentSource::None => panic!("key() called on invalid iterator"),
		}
	}

	fn value(&self) -> &[u8] {
		debug_assert!(self.valid());
		match self.current_source {
			CurrentSource::Snapshot => self.snapshot_iter.value(),
			CurrentSource::WriteSet => {
				let entry = if self.direction == MergeDirection::Forward {
					self.write_set_entries[self.ws_pos].1
				} else {
					self.write_set_entries[self.ws_pos_back - 1].1
				};
				entry.value.as_ref().map_or(&[], |v| v.as_slice())
			}
			CurrentSource::None => panic!("value() called on invalid iterator"),
		}
	}
}

/// Public iterator for range scans over a transaction.
///
/// # Example
/// ```ignore
/// let mut iter = tx.range(b"a", b"z")?;
/// iter.seek_first()?;
/// while iter.valid() {
///     let key = iter.key();
///     let value = iter.value()?;
///     println!("{:?} = {:?}", key, value);
///     iter.next()?;
/// }
/// ```
pub struct TransactionIterator<'a> {
	inner: TransactionRangeIterator<'a>,
	core: Arc<Core>,
}

impl<'a> TransactionIterator<'a> {
	/// Creates a new transaction iterator
	pub(crate) fn new(inner: TransactionRangeIterator<'a>, core: Arc<Core>) -> Self {
		Self {
			inner,
			core,
		}
	}

	/// Seek to first entry. Returns true if valid.
	pub fn seek_first(&mut self) -> Result<bool> {
		self.inner.seek_first()
	}

	/// Seek to last entry. Returns true if valid.
	pub fn seek_last(&mut self) -> Result<bool> {
		self.inner.seek_last()
	}

	/// Seek to first entry >= target. Returns true if valid.
	pub fn seek<K: AsRef<[u8]>>(&mut self, target: K) -> Result<bool> {
		// Create a minimal encoded key for seeking
		let mut encoded = target.as_ref().to_vec();
		// Add minimum trailer (seq_num=0, kind=0) and timestamp=0
		encoded.extend_from_slice(&0u64.to_be_bytes());
		encoded.extend_from_slice(&0u64.to_be_bytes());
		self.inner.seek(&encoded)
	}

	/// Move to next entry. Returns true if valid.
	#[allow(clippy::should_implement_trait)]
	pub fn next(&mut self) -> Result<bool> {
		self.inner.next()
	}

	/// Move to previous entry. Returns true if valid.
	pub fn prev(&mut self) -> Result<bool> {
		self.inner.prev()
	}

	/// Check if positioned on valid entry.
	pub fn valid(&self) -> bool {
		self.inner.valid()
	}

	/// Get current key (allocates). Caller must check valid() first.
	pub fn key(&self) -> Key {
		debug_assert!(self.valid());
		self.inner.key().user_key().to_vec()
	}

	/// Get current value (may allocate for VLog resolution). Caller must check valid() first.
	/// Returns None if in keys-only mode.
	///
	/// Note: Write-set values are stored RAW. Only persisted values (memtable/SSTable)
	/// are encoded as ValueLocation. We skip decode for write-set values.
	pub fn value(&self) -> Result<Option<Value>> {
		debug_assert!(self.valid());
		let raw_value = self.inner.value();

		// Write-set values are stored raw (not ValueLocation encoded)
		// Only persisted values need ValueLocation decode
		if self.inner.is_write_set_value() {
			Ok(Some(raw_value.to_vec()))
		} else {
			self.core.resolve_value(raw_value).map(Some)
		}
	}

	/// Get current key-value pair (convenience method)
	pub fn entry(&self) -> Result<(Key, Option<Value>)> {
		Ok((self.key(), self.value()?))
	}
}

// ===== Transaction History Iterator =====

/// Public wrapper around `HistoryIterator` that provides convenient methods
/// for accessing version history with resolved values.
pub struct TransactionHistoryIterator<'a> {
	inner: HistoryIterator<'a>,
	core: Arc<Core>,
}

impl<'a> TransactionHistoryIterator<'a> {
	/// Creates a new history iterator
	pub(crate) fn new(inner: HistoryIterator<'a>, core: Arc<Core>) -> Self {
		Self {
			inner,
			core,
		}
	}

	/// Seek to first entry. Returns true if valid.
	pub fn seek_first(&mut self) -> Result<bool> {
		self.inner.seek_first()
	}

	/// Seek to last entry. Returns true if valid.
	pub fn seek_last(&mut self) -> Result<bool> {
		self.inner.seek_last()
	}

	/// Seek to first entry >= target. Returns true if valid.
	pub fn seek<K: AsRef<[u8]>>(&mut self, target: K) -> Result<bool> {
		// Create a minimal encoded key for seeking
		let mut encoded = target.as_ref().to_vec();
		// Add maximum trailer (seq_num=MAX, kind=MAX) and timestamp=MAX to find >= target
		encoded.extend_from_slice(&u64::MAX.to_be_bytes());
		encoded.extend_from_slice(&u64::MAX.to_be_bytes());
		self.inner.seek(&encoded)
	}

	/// Move to next entry. Returns true if valid.
	#[allow(clippy::should_implement_trait)]
	pub fn next(&mut self) -> Result<bool> {
		self.inner.next()
	}

	/// Move to previous entry. Returns true if valid.
	pub fn prev(&mut self) -> Result<bool> {
		self.inner.prev()
	}

	/// Check if positioned on valid entry.
	pub fn valid(&self) -> bool {
		self.inner.valid()
	}

	/// Get current user key (allocates). Caller must check valid() first.
	pub fn key(&self) -> Key {
		debug_assert!(self.valid());
		self.inner.key().user_key().to_vec()
	}

	/// Get the timestamp/version of the current entry. Caller must check valid() first.
	pub fn timestamp(&self) -> u64 {
		debug_assert!(self.valid());
		self.inner.key().timestamp()
	}

	/// Get the sequence number of the current entry. Caller must check valid() first.
	pub fn seq_num(&self) -> u64 {
		debug_assert!(self.valid());
		self.inner.key().seq_num()
	}

	/// Check if the current entry is a tombstone. Caller must check valid() first.
	pub fn is_tombstone(&self) -> bool {
		debug_assert!(self.valid());
		self.inner.key().is_tombstone()
	}

	/// Get current value (may allocate for VLog resolution). Caller must check valid() first.
	pub fn value(&self) -> Result<Value> {
		debug_assert!(self.valid());
		self.core.resolve_value(self.inner.value())
	}

	/// Get current entry as a tuple (key, value, timestamp, is_tombstone).
	/// Convenience method for collecting all version information.
	/// For tombstones, returns an empty value since tombstones have no value.
	pub fn entry(&self) -> Result<(Key, Value, u64, bool)> {
		let is_tombstone = self.is_tombstone();
		// Tombstones have no value, so use empty vec
		let value = if is_tombstone {
			Vec::new()
		} else {
			self.value()?
		};
		Ok((self.key(), value, self.timestamp(), is_tombstone))
	}

	/// Returns the iterator stats for testing/debugging
	#[cfg(test)]
	pub(crate) fn stats(&self) -> &HistoryIteratorStats {
		self.inner.stats()
	}
}

impl InternalIterator for TransactionHistoryIterator<'_> {
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.inner.seek(target)
	}

	fn seek_first(&mut self) -> Result<bool> {
		self.inner.seek_first()
	}

	fn seek_last(&mut self) -> Result<bool> {
		self.inner.seek_last()
	}

	fn next(&mut self) -> Result<bool> {
		self.inner.next()
	}

	fn prev(&mut self) -> Result<bool> {
		self.inner.prev()
	}

	fn valid(&self) -> bool {
		self.inner.valid()
	}

	fn key(&self) -> InternalKeyRef<'_> {
		self.inner.key()
	}

	fn value(&self) -> &[u8] {
		self.inner.value()
	}
}
