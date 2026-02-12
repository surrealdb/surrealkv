use std::cmp::Ordering;
use std::collections::btree_map::Entry as BTreeEntry;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::lsm::Core;
use crate::snapshot::{HistoryIterator, MergeDirection, Snapshot, SnapshotIterator};
use crate::{InternalKeyKind, InternalKeyRef, IntoBytes, Key, LSMIterator, Value};

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
		// Check if versioned queries are enabled
		if !self.core.opts.enable_versioning {
			return Err(Error::InvalidArgument("Versioned queries not enabled".to_string()));
		}

		// RYOW: Check write set first for uncommitted writes
		if let Some(entries) = self.write_set.get(key.as_slice()) {
			if let Some(entry) = entries.last() {
				// Hard delete wipes all history - return None regardless of timestamp
				if entry.is_hard_delete() {
					return Ok(None);
				}

				// Write set entry is visible if its timestamp <= query timestamp
				if entry.timestamp <= timestamp {
					if entry.is_tombstone() {
						return Ok(None);
					}
					return Ok(entry.value.clone());
				}
				// entry.timestamp > timestamp: write is "from the future", check storage
			}
		}

		// Query the versioned index through the snapshot
		match &self.snapshot {
			Some(snapshot) => snapshot.get_at(key.as_slice(), timestamp),
			None => Err(Error::NoSnapshot),
		}
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

	/// Gets keys in a key range at a specific timestamp.
	///
	/// Gets keys and values in a range, at the current timestamp.
	///
	/// Returns a cursor-based iterator implementing `LSMIterator` with explicit
	/// seek/next/prev methods. Use `seek_first()` to position at the first key,
	/// then `next()` to iterate.
	///
	/// The iterator iterates over all keys and values in the range, inclusive of
	/// the start key, but not the end key.
	///
	/// # Example
	/// ```ignore
	/// 
	/// let mut iter = tx.range(b"a", b"z")?;
	/// iter.seek_first()?;
	/// while iter.valid() {
	///     let key = iter.key().user_key();
	///     let ts = iter.key().timestamp();
	///     let value = iter.value()?;
	///     iter.next()?;
	/// }
	///
	/// // Or seek to a specific key:
	/// iter.seek(b"foo")?;  // Position at first version of "foo"
	/// ```
	pub fn range<K>(&self, start: K, end: K) -> Result<impl LSMIterator + '_>
	where
		K: IntoBytes,
	{
		let mut options = ReadOptions::default();
		options.set_iterate_bounds(Some(start.into_bytes()), Some(end.into_bytes()));
		self.range_with_options(&options)
	}

	/// Gets keys and values in a range, with custom read options.
	///
	/// Returns a cursor-based iterator implementing `LSMIterator` with explicit
	/// seek/next/prev methods.
	///
	/// The iterator iterates over all keys and values in the
	/// range, inclusive of the start key, but not the end key.
	pub fn range_with_options(&self, options: &ReadOptions) -> Result<impl LSMIterator + '_> {
		let start_key = options.lower_bound.clone().unwrap_or_default();
		let end_key = options.upper_bound.clone().unwrap_or_default();
		TransactionRangeIterator::new_with_options(self, Arc::clone(&self.core), start_key, end_key)
	}

	/// Returns a unified history iterator over ALL versions of keys in the range.
	///
	/// Returns an iterator implementing `LSMIterator`, providing a unified
	/// streaming API regardless of whether the B+tree index is enabled.
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
	///     let key_ref = iter.key();
	///     println!("key={:?} ts={} is_tombstone={} value={:?}",
	///         key_ref.user_key(), key_ref.timestamp(), key_ref.is_tombstone(),
	///         iter.value()?);
	///     iter.next()?;
	/// }
	/// ```
	pub fn history<K>(&self, start: K, end: K) -> Result<impl LSMIterator + '_>
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
	) -> Result<impl LSMIterator + '_>
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

		// RYOW: Collect write-set entries in the key range
		// Hard deletes are tracked separately and skip all snapshot history for that key
		let mut write_set_entries: Vec<(&Key, &Entry)> = Vec::new();
		let mut hard_delete_keys: std::collections::HashSet<&[u8]> =
			std::collections::HashSet::new();

		for (key, entry_list) in
			self.write_set.range(start.as_slice().to_vec()..end.as_slice().to_vec())
		{
			if let Some(entry) = entry_list.last() {
				// Hard deletes wipe all history - track them separately
				if entry.is_hard_delete() {
					hard_delete_keys.insert(key.as_slice());
					// Hard deletes are never returned (even with include_tombstones)
					continue;
				}

				// Filter by timestamp range if specified
				if let Some((ts_start, ts_end)) = opts.ts_range {
					if entry.timestamp >= ts_start && entry.timestamp <= ts_end {
						write_set_entries.push((key, entry));
					}
				} else {
					write_set_entries.push((key, entry));
				}
			}
		}

		Ok(TransactionHistoryIterator::new(
			inner,
			Arc::clone(&self.core),
			write_set_entries,
			hard_delete_keys,
			opts.include_tombstones,
		))
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

	/// Checks if this entry is a hard delete (wipes all history)
	fn is_hard_delete(&self) -> bool {
		self.kind == InternalKeyKind::Delete || self.kind == InternalKeyKind::RangeDelete
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
/// write set. Implements LSMIterator for zero-copy iteration.
pub(crate) struct TransactionRangeIterator<'a> {
	/// Snapshot iterator (implements LSMIterator)
	snapshot_iter: SnapshotIterator<'a>,

	/// Core for resolving VLog references to actual values.
	core: Arc<Core>,

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
		core: Arc<Core>,
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
			core,
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

	/// Reset both write-set positions to cover the full range.
	///
	/// This MUST be called on any seek operation to maintain the invariant:
	/// `ws_pos <= ws_pos_back` where `[ws_pos, ws_pos_back)` is the valid range.
	///
	/// Without this, direction changes after partial iteration leave stale positions:
	/// ```text
	/// Initial:       ws_pos=0, ws_pos_back=5  [A B C D E]
	/// prev() x2:     ws_pos=0, ws_pos_back=3  [A B C|D E] ← D,E "consumed"
	/// seek_first():  ws_pos=0, ws_pos_back=3  [A B C|D E] ← BUG! D,E invisible
	///
	/// With reset:
	/// seek_first():  ws_pos=0, ws_pos_back=5  [A B C D E] ← Full range restored
	/// ```
	fn reset_ws_positions(&mut self) {
		self.ws_pos = 0;
		self.ws_pos_back = self.write_set_entries.len();
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
}

impl LSMIterator for TransactionRangeIterator<'_> {
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.initialized = true;

		// Encode user key with MAX trailer/timestamp for >= seek
		// This positions at the FIRST (newest) version of the target key
		let mut encoded = target.to_vec();
		encoded.extend_from_slice(&u64::MAX.to_be_bytes()); // max trailer
		encoded.extend_from_slice(&u64::MAX.to_be_bytes()); // max timestamp
		self.snapshot_iter.seek(&encoded)?;

		// Position write-set using raw user key (binary search)
		self.reset_ws_positions();
		self.ws_pos = self.write_set_entries.partition_point(|(k, _)| k.as_slice() < target);

		self.position_to_min()
	}

	fn seek_first(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.initialized = true;

		// Seek snapshot to first
		self.snapshot_iter.seek_first()?;

		// Reset write-set to full range
		self.reset_ws_positions();

		self.position_to_min()
	}

	fn seek_last(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Backward;
		self.initialized = true;

		// Seek snapshot to last
		self.snapshot_iter.seek_last()?;

		// Reset write-set to full range (critical for correctness after forward iteration)
		self.reset_ws_positions();

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

	fn raw_value(&self) -> Result<&[u8]> {
		debug_assert!(self.valid());
		match self.current_source {
			CurrentSource::Snapshot => self.snapshot_iter.raw_value(),
			CurrentSource::WriteSet => {
				let entry = if self.direction == MergeDirection::Forward {
					self.write_set_entries[self.ws_pos].1
				} else {
					self.write_set_entries[self.ws_pos_back - 1].1
				};
				Ok(entry.value.as_ref().map_or(&[], |v| v.as_slice()))
			}
			CurrentSource::None => panic!("value() called on invalid iterator"),
		}
	}

	fn value(&self) -> Result<Value> {
		debug_assert!(self.valid());
		let raw = self.raw_value()?;
		if self.current_source == CurrentSource::WriteSet {
			Ok(raw.to_vec())
		} else {
			self.core.resolve_value(raw)
		}
	}
}

// ===== Transaction History Iterator =====
/// # TransactionHistoryIterator
///
/// A merge iterator that combines two sorted sources of key-version pairs:
///
/// 1. **Snapshot** (committed data in LSM) - all historical versions
/// 2. **Write-set** (uncommitted transaction writes) - pending changes
///
/// The iterator merges them to provide RYOW (Read Your Own Writes) semantics,
/// ensuring the transaction sees its own uncommitted writes overlaid on
/// committed data.
///
/// ## Data Model
///
/// Each entry is: `(key, timestamp, value)`
///
/// - **Key**: the user key (e.g., `"user:1"`)
/// - **Timestamp**: version number (higher = newer)
/// - **Value**: the data (or tombstone marker for soft deletes)
///
/// ## Ordering
///
/// **Forward iteration** (`seek_first`, `next`):
/// ```text
/// Primary:   key ASC       (a < b < c)
/// Secondary: timestamp DESC (100 > 50 > 10) — newer versions first
/// ```
///
/// **Backward iteration** (`seek_last`, `prev`):
/// ```text
/// Primary:   key DESC      (c > b > a)
/// Secondary: timestamp ASC (10 < 50 < 100) — older versions first
/// ```
///
/// ## Example: Basic Merge
///
/// ```text
/// Snapshot (committed):          Write-set (transaction):
/// ┌─────────┬────┬───────┐       ┌─────────┬────┬───────┐
/// │ Key     │ TS │ Value │       │ Key     │ TS │ Value │
/// ├─────────┼────┼───────┤       ├─────────┼────┼───────┤
/// │ "a"     │ 50 │ "v1"  │       │ "b"     │ 80 │ "v4"  │
/// │ "a"     │ 30 │ "v0"  │       └─────────┴────┴───────┘
/// │ "c"     │ 40 │ "v2"  │
/// └─────────┴────┴───────┘
///
/// Forward iteration produces:
///   ("a",50) ← snap wins, "a" < "b"
///   ("a",30) ← snap wins, "a" < "b"
///   ("b",80) ← write-set wins, "b" < "c"
///   ("c",40) ← snap (write-set exhausted)
/// ```
///
/// Public wrapper around `HistoryIterator` that provides convenient methods
/// for accessing version history with resolved values.
///
/// ## Architecture
///
/// ```text
/// ┌─────────────────────────────────────────────────────────────┐
/// │              TransactionHistoryIterator                     │
/// │                                                             │
/// │   ┌──────────────┐         ┌──────────────────┐             │
/// │   │   Snapshot   │         │    Write-set     │             │
/// │   │  (committed) │         │  (uncommitted)   │             │
/// │   └──────┬───────┘         └────────┬─────────┘             │
/// │          │                          │                       │
/// │          ▼                          ▼                       │
/// │        ┌──────────────────────────────┐                     │
/// │        │       Merge Logic            │                     │
/// │        │  • Compare (key, timestamp)  │                     │
/// │        │  • Pick winner by ordering   │                     │
/// │        │  • Handle RYOW conflicts     │                     │
/// │        └──────────────┬───────────────┘                     │
/// │                       │                                     │
/// │                       ▼                                     │
/// │              Merged output stream                           │
/// └─────────────────────────────────────────────────────────────┘
/// ```
///
/// ## Write-set Position Invariant
///
/// The write-set uses two pointers to track the valid range:
/// ```text
/// [ws_pos, ws_pos_back) = valid range
/// ```
///
/// **Invariant**: `ws_pos <= ws_pos_back` must always hold.
///
/// - Forward iteration advances `ws_pos` upward
/// - Backward iteration advances `ws_pos_back` downward
/// - All seek operations reset BOTH pointers to restore the full range
pub(crate) struct TransactionHistoryIterator<'a> {
	// === Core data sources ===
	/// History iterator from snapshot - provides committed data.
	/// Already sorted by (user_key ASC, timestamp DESC).
	inner: HistoryIterator<'a>,

	/// Core for resolving VLog references to actual values.
	core: Arc<Core>,

	// === RYOW (Read Your Own Writes) fields ===
	/// Write-set entries for the range, sorted by key.
	/// These are the transaction's uncommitted writes that should be
	/// visible to reads within the same transaction.
	///
	/// Assumption: one entry per key (latest write wins within transaction).
	/// Note: Hard delete entries are excluded (they go to `hard_delete_keys`).
	write_set_entries: Vec<(&'a Key, &'a Entry)>,

	/// Keys that have hard deletes in write-set.
	///
	/// Hard deletes differ from tombstones:
	/// - **Tombstone**: Marks key as deleted at a specific timestamp. Old versions are still
	///   visible in history.
	/// - **Hard delete**: Erases ALL history for the key. The key appears as if it never existed.
	///
	/// Example:
	/// ```text
	/// Snapshot:                Hard-delete-keys: {"b"}
	/// ┌─────┬────┬─────┐
	/// │ "a" │ 50 │ v1  │       After iteration:
	/// │ "b" │ 40 │ v2  │  →    [("a",50), ("c",60)]
	/// │ "b" │ 20 │ v0  │       All "b" history erased
	/// │ "c" │ 60 │ v3  │
	/// └─────┴────┴─────┘
	/// ```
	hard_delete_keys: std::collections::HashSet<&'a [u8]>,

	/// Current position in write-set entries (forward iteration).
	/// Range: 0..=len, where len means exhausted.
	///
	/// Invariant: `ws_pos <= ws_pos_back`
	ws_pos: usize,

	/// Current position in write-set entries (backward iteration).
	/// Range: 0..=len, represents one-past-the-end style index.
	/// The current backward entry is at `ws_pos_back - 1`.
	///
	/// Invariant: `ws_pos <= ws_pos_back`
	ws_pos_back: usize,

	/// Which source provided the current entry.
	current_source: CurrentSource,

	/// Buffer for encoding write-set key in internal format.
	/// Used for `LSMIterator::key()` which returns encoded keys.
	ws_encoded_key_buf: Vec<u8>,

	/// Current iteration direction.
	direction: MergeDirection,

	/// Whether the iterator has been positioned via seek/next/prev.
	initialized: bool,

	/// Whether to include tombstone entries in results.
	///
	/// - `true`: Tombstones are returned like regular entries (for compaction, etc.)
	/// - `false`: Tombstones are skipped (for normal reads)
	///
	/// Example with `include_tombstones = false`:
	/// ```text
	/// Write-set: [("x", 70, TOMBSTONE)]
	/// Snapshot:  [("x", 50, "alive")]
	///
	/// Result: [("x", 50, "alive")]  ← Tombstone skipped
	/// ```
	include_tombstones: bool,
}

impl<'a> TransactionHistoryIterator<'a> {
	/// Creates a new history iterator with RYOW support.
	///
	/// # Arguments
	///
	/// * `inner` - The underlying snapshot history iterator
	/// * `core` - Core for VLog value resolution
	/// * `write_set_entries` - Transaction's uncommitted writes, sorted by key
	/// * `hard_delete_keys` - Keys to completely erase from history
	/// * `include_tombstones` - Whether to return tombstone entries
	pub(crate) fn new(
		inner: HistoryIterator<'a>,
		core: Arc<Core>,
		write_set_entries: Vec<(&'a Key, &'a Entry)>,
		hard_delete_keys: std::collections::HashSet<&'a [u8]>,
		include_tombstones: bool,
	) -> Self {
		let ws_len = write_set_entries.len();
		Self {
			inner,
			core,
			write_set_entries,
			hard_delete_keys,
			ws_pos: 0,
			ws_pos_back: ws_len,
			current_source: CurrentSource::None,
			ws_encoded_key_buf: Vec::new(),
			direction: MergeDirection::Forward,
			initialized: false,
			include_tombstones,
		}
	}

	// =========================================================================
	// Write-set position management
	// =========================================================================

	/// Reset both write-set positions to cover the full range.
	///
	/// This MUST be called on any seek operation to maintain the invariant:
	/// ```text
	/// ws_pos <= ws_pos_back
	/// ```
	/// where `[ws_pos, ws_pos_back)` is the valid range.
	///
	/// ## Why This Matters
	///
	/// Without resetting both positions, sequences like:
	/// ```text
	/// prev() x2 → seek_first()
	/// ```
	/// would leave `ws_pos_back` at a truncated position, hiding entries:
	///
	/// ```text
	/// Without this:
	///   Initial:       ws_pos=0, ws_pos_back=5  [A B C D E]
	///   prev() x2:     ws_pos=0, ws_pos_back=3  [A B C|D E] ← D,E "consumed"
	///   seek_first():  ws_pos=0, ws_pos_back=3  [A B C|D E] ← BUG! D,E invisible
	///
	/// After this:
	///   Initial:       ws_pos=0, ws_pos_back=5  [A B C D E]
	///   prev() x2:     ws_pos=0, ws_pos_back=3  [A B C|D E]
	///   seek_first():  ws_pos=0, ws_pos_back=5  [A B C D E] ← Full range restored
	/// ```
	fn reset_ws_positions(&mut self) {
		self.ws_pos = 0;
		self.ws_pos_back = self.write_set_entries.len();
	}

	// =========================================================================
	// Write-set helper methods
	// =========================================================================
	//
	// These provide safe access to write-set entries for both forward and
	// backward iteration. Forward uses `ws_pos`, backward uses `ws_pos_back - 1`.

	/// Check if current write-set position is valid (forward iteration).
	/// Valid when `ws_pos` hasn't reached `ws_pos_back`.
	fn ws_valid(&self) -> bool {
		self.ws_pos < self.ws_pos_back
	}

	/// Check if current write-set position is valid (backward iteration).
	/// Valid when `ws_pos_back` hasn't reached `ws_pos`.
	fn ws_valid_back(&self) -> bool {
		self.ws_pos_back > self.ws_pos
	}

	/// Get current write-set key (forward iteration).
	fn ws_key(&self) -> &[u8] {
		debug_assert!(self.ws_valid());
		self.write_set_entries[self.ws_pos].0.as_slice()
	}

	/// Get current write-set key (backward iteration).
	/// Note: backward index is `ws_pos_back - 1`.
	fn ws_key_back(&self) -> &[u8] {
		debug_assert!(self.ws_valid_back());
		self.write_set_entries[self.ws_pos_back - 1].0.as_slice()
	}

	/// Get current write-set timestamp (forward iteration).
	fn ws_timestamp(&self) -> u64 {
		debug_assert!(self.ws_valid());
		self.write_set_entries[self.ws_pos].1.timestamp
	}

	/// Get current write-set timestamp (backward iteration).
	fn ws_timestamp_back(&self) -> u64 {
		debug_assert!(self.ws_valid_back());
		self.write_set_entries[self.ws_pos_back - 1].1.timestamp
	}

	/// Check if current write-set entry is a tombstone (forward iteration).
	fn ws_is_tombstone(&self) -> bool {
		debug_assert!(self.ws_valid());
		self.write_set_entries[self.ws_pos].1.is_tombstone()
	}

	/// Check if current write-set entry is a tombstone (backward iteration).
	fn ws_is_tombstone_back(&self) -> bool {
		debug_assert!(self.ws_valid_back());
		self.write_set_entries[self.ws_pos_back - 1].1.is_tombstone()
	}

	/// Populate the encoded key buffer for the current write-set entry.
	///
	/// This is needed for `LSMIterator::key()` which must return keys
	/// in the internal encoded format: `[user_key | trailer | timestamp]`
	///
	/// The trailer encodes sequence number and entry kind for LSM ordering.
	fn populate_ws_encoded_key(&mut self) {
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
		// Trailer format: (seq_num << 8) | kind
		let trailer = ((entry.seqno as u64) << 8) | (entry.kind as u64);
		self.ws_encoded_key_buf.extend_from_slice(&trailer.to_be_bytes());
		self.ws_encoded_key_buf.extend_from_slice(&entry.timestamp.to_be_bytes());
	}

	// =========================================================================
	// Core merge logic
	// =========================================================================

	/// Position to the minimum of snapshot and write-set (forward merge).
	///
	/// # Ordering: (user_key ASC, timestamp DESC)
	///
	/// Returns the entry that should come first in forward iteration order.
	///
	/// ## Merge Algorithm
	///
	/// ```text
	///     Snapshot          Write-set
	///     ┌───┐             ┌───┐
	///     │ A │─┐       ┌───│ B │
	///     └───┘ │       │   └───┘
	///           ▼       ▼
	///         ┌───────────┐
	///         │  Compare  │
	///         │ Pick MIN  │
	///         └─────┬─────┘
	///               │
	///               ▼
	///          Return winner
	///          (advance winner's pointer on next call)
	/// ```
	///
	/// ## Key Comparison Rules
	///
	/// ```text
	/// ┌────────────────────────────────────────────────────────┐
	/// │ hist_key vs ws_key                                     │
	/// ├────────────────────────────────────────────────────────┤
	/// │ hist < ws  → return Snapshot  (hist key comes first)   │
	/// │ hist > ws  → return WriteSet  (ws key comes first)     │
	/// │ hist = ws  → compare timestamps (same key, pick newer) │
	/// │   hist_ts > ws_ts → return Snapshot (hist is newer)    │
	/// │   hist_ts < ws_ts → return WriteSet (ws is newer)      │
	/// │   hist_ts = ws_ts → WriteSet wins (RYOW), skip Snapshot│
	/// └────────────────────────────────────────────────────────┘
	/// ```
	///
	/// ## RYOW (Read Your Own Writes)
	///
	/// When both sources have the same (key, timestamp), the write-set
	/// entry wins and the snapshot entry is skipped. This ensures the
	/// transaction sees its own writes.
	///
	/// ```text
	/// Snapshot:   ("x", 50, "snap")
	/// Write-set:  ("x", 50, "ws")
	///
	/// Same key, same timestamp → WriteSet wins, skip Snapshot
	/// Result: [("x", 50, "ws")]  ← Only transaction's version
	/// ```
	fn position_to_min(&mut self) -> Result<bool> {
		loop {
			let hist_valid = self.inner.valid();
			let ws_valid = self.ws_valid();

			// Skip snapshot entries for hard-deleted keys.
			// Hard deletes erase ALL history for a key.
			if hist_valid {
				let hist_key = self.inner.key().user_key();
				if self.hard_delete_keys.contains(hist_key) {
					self.inner.next()?;
					continue;
				}
			}

			self.current_source = match (hist_valid, ws_valid) {
				// Both exhausted - iteration complete
				(false, false) => CurrentSource::None,

				// Only snapshot has entries
				(true, false) => CurrentSource::Snapshot,

				// Only write-set has entries
				(false, true) => {
					// Skip tombstones if not included
					if !self.include_tombstones && self.ws_is_tombstone() {
						self.ws_pos += 1;
						continue;
					}
					self.populate_ws_encoded_key();
					CurrentSource::WriteSet
				}

				// Both have entries - need to compare and pick minimum
				(true, true) => {
					let hist_key = self.inner.key().user_key();
					let hist_ts = self.inner.key().timestamp();
					let ws_key = self.ws_key();
					let ws_ts = self.ws_timestamp();

					match hist_key.cmp(ws_key) {
						// Snapshot key is smaller - it comes first in ASC order
						Ordering::Less => CurrentSource::Snapshot,

						// Write-set key is smaller
						Ordering::Greater => {
							if !self.include_tombstones && self.ws_is_tombstone() {
								self.ws_pos += 1;
								continue;
							}
							self.populate_ws_encoded_key();
							CurrentSource::WriteSet
						}

						// Same key - compare timestamps (DESC: higher first)
						Ordering::Equal => {
							match hist_ts.cmp(&ws_ts) {
								// Snapshot has higher timestamp - it's newer, return first
								Ordering::Greater => CurrentSource::Snapshot,

								// Write-set has higher timestamp - it's newer
								Ordering::Less => {
									if !self.include_tombstones && self.ws_is_tombstone() {
										self.ws_pos += 1;
										continue;
									}
									self.populate_ws_encoded_key();
									CurrentSource::WriteSet
								}

								// Same timestamp - RYOW: write-set wins, skip snapshot
								// This ensures transaction sees its own writes
								Ordering::Equal => {
									self.inner.next()?; // Skip the shadowed snapshot entry
									if !self.include_tombstones && self.ws_is_tombstone() {
										self.ws_pos += 1;
										continue;
									}
									self.populate_ws_encoded_key();
									CurrentSource::WriteSet
								}
							}
						}
					}
				}
			};
			return Ok(self.current_source != CurrentSource::None);
		}
	}

	/// Position to the maximum of snapshot and write-set (backward merge).
	///
	/// # Ordering: (user_key DESC, timestamp ASC)
	///
	/// Returns the entry that should come first in backward iteration order.
	/// This is the mirror of `position_to_min` with reversed comparisons.
	///
	/// ## Key Comparison Rules (Backward)
	///
	/// ```text
	/// ┌────────────────────────────────────────────────────────┐
	/// │ hist_key vs ws_key                                     │
	/// ├────────────────────────────────────────────────────────┤
	/// │ hist > ws  → return Snapshot  (hist key comes first)   │
	/// │ hist < ws  → return WriteSet  (ws key comes first)     │
	/// │ hist = ws  → compare timestamps (same key, pick older) │
	/// │   hist_ts < ws_ts → return Snapshot (hist is older)    │
	/// │   hist_ts > ws_ts → return WriteSet (ws is older)      │
	/// │   hist_ts = ws_ts → WriteSet wins (RYOW), skip Snapshot│
	/// └────────────────────────────────────────────────────────┘
	/// ```
	fn position_to_max(&mut self) -> Result<bool> {
		loop {
			let hist_valid = self.inner.valid();
			let ws_valid = self.ws_valid_back();

			// Skip snapshot entries for hard-deleted keys
			if hist_valid {
				let hist_key = self.inner.key().user_key();
				if self.hard_delete_keys.contains(hist_key) {
					self.inner.prev()?;
					continue;
				}
			}

			self.current_source = match (hist_valid, ws_valid) {
				// Both exhausted
				(false, false) => CurrentSource::None,

				// Only snapshot has entries
				(true, false) => CurrentSource::Snapshot,

				// Only write-set has entries
				(false, true) => {
					if !self.include_tombstones && self.ws_is_tombstone_back() {
						self.ws_pos_back -= 1;
						continue;
					}
					self.populate_ws_encoded_key();
					CurrentSource::WriteSet
				}

				// Both have entries - compare and pick maximum
				(true, true) => {
					let hist_key = self.inner.key().user_key();
					let hist_ts = self.inner.key().timestamp();
					let ws_key = self.ws_key_back();
					let ws_ts = self.ws_timestamp_back();

					match hist_key.cmp(ws_key) {
						// Snapshot key is larger - it comes first in DESC order
						Ordering::Greater => CurrentSource::Snapshot,

						// Write-set key is larger
						Ordering::Less => {
							if !self.include_tombstones && self.ws_is_tombstone_back() {
								self.ws_pos_back -= 1;
								continue;
							}
							self.populate_ws_encoded_key();
							CurrentSource::WriteSet
						}

						// Same key - compare timestamps (ASC: lower first in backward)
						Ordering::Equal => {
							match hist_ts.cmp(&ws_ts) {
								// Snapshot has lower timestamp - it's older, return first
								// (backward)
								Ordering::Less => CurrentSource::Snapshot,

								// Write-set has lower timestamp - it's older
								Ordering::Greater => {
									if !self.include_tombstones && self.ws_is_tombstone_back() {
										self.ws_pos_back -= 1;
										continue;
									}
									self.populate_ws_encoded_key();
									CurrentSource::WriteSet
								}

								// Same timestamp - RYOW: write-set wins, skip snapshot
								Ordering::Equal => {
									self.inner.prev()?;
									if !self.include_tombstones && self.ws_is_tombstone_back() {
										self.ws_pos_back -= 1;
										continue;
									}
									self.populate_ws_encoded_key();
									CurrentSource::WriteSet
								}
							}
						}
					}
				}
			};
			return Ok(self.current_source != CurrentSource::None);
		}
	}

	/// Seek to the first entry in forward order.
	///
	/// Positions the iterator at the entry with the smallest (key, timestamp DESC).
	/// Returns `true` if a valid entry exists.
	///
	/// ## Example
	///
	/// ```text
	/// Data: [("a",50), ("a",30), ("b",40)]
	/// seek_first() → positions at ("a", 50)
	/// ```
	pub fn seek_first(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.initialized = true;

		// Position snapshot at first entry
		self.inner.seek_first()?;

		// Reset write-set to full range
		self.reset_ws_positions();

		self.position_to_min()
	}

	/// Seek to the last entry in backward order.
	///
	/// Positions the iterator at the entry with the largest (key, timestamp ASC).
	/// Returns `true` if a valid entry exists.
	///
	/// ## Example
	///
	/// ```text
	/// Data: [("a",50), ("a",30), ("b",40)]
	/// seek_last() → positions at ("b", 40)
	/// ```
	pub fn seek_last(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Backward;
		self.initialized = true;

		// Position snapshot at last entry
		self.inner.seek_last()?;

		// Reset write-set to full range (critical for correctness after forward iteration)
		self.reset_ws_positions();

		self.position_to_max()
	}

	/// Move to the next entry in forward order.
	///
	/// Advances past the current entry and positions at the next one.
	/// Returns `true` if a valid entry exists.
	///
	/// ## Note on Direction Change
	///
	/// If previously iterating backward, this resets to `seek_first()`.
	/// The iterator does not support seamless direction switching mid-iteration.
	#[allow(clippy::should_implement_trait)]
	pub fn next(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_first();
		}

		// Direction change: reset to beginning
		if self.direction != MergeDirection::Forward {
			return self.seek_first();
		}

		// Advance whichever source provided the current entry
		match self.current_source {
			CurrentSource::Snapshot => {
				self.inner.next()?;
			}
			CurrentSource::WriteSet => {
				self.ws_pos += 1;
			}
			CurrentSource::None => return Ok(false),
		}

		self.position_to_min()
	}

	/// Move to the previous entry in backward order.
	///
	/// Advances backward past the current entry and positions at the previous one.
	/// Returns `true` if a valid entry exists.
	///
	/// ## Note on Direction Change
	///
	/// If previously iterating forward, this resets to `seek_last()`.
	/// The iterator does not support seamless direction switching mid-iteration.
	pub fn prev(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_last();
		}

		// Direction change: reset to end
		if self.direction != MergeDirection::Backward {
			return self.seek_last();
		}

		// Advance whichever source provided the current entry (backward)
		match self.current_source {
			CurrentSource::Snapshot => {
				self.inner.prev()?;
			}
			CurrentSource::WriteSet => {
				self.ws_pos_back -= 1;
			}
			CurrentSource::None => return Ok(false),
		}

		self.position_to_max()
	}

	/// Check if positioned on a valid entry.
	///
	/// Must be called before accessing iterator methods.
	pub fn valid(&self) -> bool {
		self.current_source != CurrentSource::None
	}
}

// =============================================================================
// LSMIterator implementation
// =============================================================================
//
// This allows TransactionHistoryIterator to be used in places that expect
// the internal LSM iterator interface with encoded keys.

impl LSMIterator for TransactionHistoryIterator<'_> {
	/// Seek to encoded target key.
	///
	/// The target is a raw user key. It will be encoded internally with
	/// MAX trailer/timestamp to position at the first (newest) version.
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.initialized = true;

		// Encode user key with MAX trailer/timestamp for >= seek
		// This positions at the FIRST (newest) version of the target key
		let mut encoded = target.to_vec();
		encoded.extend_from_slice(&u64::MAX.to_be_bytes()); // max trailer
		encoded.extend_from_slice(&u64::MAX.to_be_bytes()); // max timestamp
		self.inner.seek(&encoded)?;

		// Position write-set using raw user key (binary search)
		self.reset_ws_positions();
		self.ws_pos = self.write_set_entries.partition_point(|(k, _)| k.as_slice() < target);

		self.position_to_min()
	}

	fn seek_first(&mut self) -> Result<bool> {
		TransactionHistoryIterator::seek_first(self)
	}

	fn seek_last(&mut self) -> Result<bool> {
		TransactionHistoryIterator::seek_last(self)
	}

	fn next(&mut self) -> Result<bool> {
		TransactionHistoryIterator::next(self)
	}

	fn prev(&mut self) -> Result<bool> {
		TransactionHistoryIterator::prev(self)
	}

	fn valid(&self) -> bool {
		TransactionHistoryIterator::valid(self)
	}

	/// Returns the current key in internal encoded format.
	///
	/// For snapshot entries, delegates to inner iterator.
	/// For write-set entries, uses the pre-populated encoded key buffer.
	fn key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.valid());
		match self.current_source {
			CurrentSource::Snapshot => self.inner.key(),
			CurrentSource::WriteSet => InternalKeyRef::from_encoded(&self.ws_encoded_key_buf),
			CurrentSource::None => panic!("key() called on invalid iterator"),
		}
	}

	/// Returns the current raw value as a byte slice.
	///
	/// For snapshot entries, may be a VLog reference requiring resolution.
	/// For write-set entries, returns the direct value bytes.
	fn raw_value(&self) -> Result<&[u8]> {
		debug_assert!(self.valid());
		match self.current_source {
			CurrentSource::Snapshot => self.inner.raw_value(),
			CurrentSource::WriteSet => {
				let entry = if self.direction == MergeDirection::Forward {
					self.write_set_entries[self.ws_pos].1
				} else {
					self.write_set_entries[self.ws_pos_back - 1].1
				};
				Ok(entry.value.as_ref().map_or(&[], |v| v.as_slice()))
			}
			CurrentSource::None => panic!("value() called on invalid iterator"),
		}
	}

	fn value(&self) -> Result<Value> {
		debug_assert!(self.valid());
		let raw = self.raw_value()?;
		if self.current_source == CurrentSource::WriteSet {
			Ok(raw.to_vec())
		} else {
			self.core.resolve_value(raw)
		}
	}
}
