use std::cmp::Ordering;
use std::collections::btree_map::Entry as BTreeEntry;
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::lsm::Core;
use crate::snapshot::{HistoryIterator, MergeDirection, Snapshot, SnapshotIterator};
use crate::tracker::ActiveTxnGuard;
use crate::{InternalKeyKind, InternalKeyRef, IntoBytes, Key, LSMIterator, Value};

/// `Mode` is an enumeration representing the different modes a transaction can
/// have in an MVCC (Multi-Version Concurrency Control) system.
#[derive(Clone, Copy, Debug, PartialEq)]
#[non_exhaustive]
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
#[non_exhaustive]
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
	/// Optional timestamp for the write operation, in nanoseconds. If None, uses the time of the
	/// commit, and so does 0. A tree that is not versioned does not keep it.
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

	/// Creates a new `ReadOptions` from any key range expression.
	///
	/// Supports:
	/// - `..` (unbounded full keyspace)
	/// - `start..end` (inclusive start, exclusive end)
	/// - `start..=end` (inclusive start, inclusive end)
	/// - `start..` (inclusive start, unbounded end)
	/// - `..end` (unbounded start, exclusive end)
	/// - `..=end` (unbounded start, inclusive end)
	pub fn from_range<R: KeyRange>(range: R) -> Self {
		Self {
			lower_bound: range.start_bound_bytes(),
			upper_bound: range.end_bound_bytes(),
		}
	}
}

/// Trait representing range bounds that can be converted into raw byte bounds
/// for database range iteration.
pub trait KeyRange {
	/// Returns the lower bound as bytes (inclusive), or None if unbounded.
	fn start_bound_bytes(&self) -> Option<Vec<u8>>;
	/// Returns the upper bound as bytes (exclusive), or None if unbounded.
	fn end_bound_bytes(&self) -> Option<Vec<u8>>;
}

impl KeyRange for std::ops::RangeFull {
	fn start_bound_bytes(&self) -> Option<Vec<u8>> {
		None
	}

	fn end_bound_bytes(&self) -> Option<Vec<u8>> {
		None
	}
}

impl<K: IntoBytes> KeyRange for std::ops::Range<K> {
	fn start_bound_bytes(&self) -> Option<Vec<u8>> {
		Some(self.start.as_slice().to_vec())
	}

	fn end_bound_bytes(&self) -> Option<Vec<u8>> {
		Some(self.end.as_slice().to_vec())
	}
}

impl<K: IntoBytes> KeyRange for std::ops::RangeInclusive<K> {
	fn start_bound_bytes(&self) -> Option<Vec<u8>> {
		Some(self.start().as_slice().to_vec())
	}

	fn end_bound_bytes(&self) -> Option<Vec<u8>> {
		let mut bytes = self.end().as_slice().to_vec();
		bytes.push(0);
		Some(bytes)
	}
}

impl<K: IntoBytes> KeyRange for std::ops::RangeFrom<K> {
	fn start_bound_bytes(&self) -> Option<Vec<u8>> {
		Some(self.start.as_slice().to_vec())
	}

	fn end_bound_bytes(&self) -> Option<Vec<u8>> {
		None
	}
}

impl<K: IntoBytes> KeyRange for std::ops::RangeTo<K> {
	fn start_bound_bytes(&self) -> Option<Vec<u8>> {
		None
	}

	fn end_bound_bytes(&self) -> Option<Vec<u8>> {
		Some(self.end.as_slice().to_vec())
	}
}

impl<K: IntoBytes> KeyRange for std::ops::RangeToInclusive<K> {
	fn start_bound_bytes(&self) -> Option<Vec<u8>> {
		None
	}

	fn end_bound_bytes(&self) -> Option<Vec<u8>> {
		let mut bytes = self.end.as_slice().to_vec();
		bytes.push(0);
		Some(bytes)
	}
}

impl<R: KeyRange> KeyRange for &R {
	fn start_bound_bytes(&self) -> Option<Vec<u8>> {
		(*self).start_bound_bytes()
	}

	fn end_bound_bytes(&self) -> Option<Vec<u8>> {
		(*self).end_bound_bytes()
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
	/// Optional limit on the number of entries/versions to return per traversal.
	///
	/// The budget is spent as entries are yielded and is carried across a change
	/// of direction, so a walk that runs forward and then back yields at most
	/// `limit` entries in total. An explicit `seek_first`/`seek_last`/`seek`
	/// starts a new traversal and restarts the budget.
	///
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

	/// Set a limit on the number of entries/versions to return per traversal.
	/// See [`HistoryOptions::limit`].
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

	/// How many range deletes the write set holds, so that a read that has no use for them does
	/// not look through the write set for any.
	range_deletes: usize,

	/// `read_set` holds the keys registered via [`get_for_update`] for
	/// commit-time conflict validation, each mapped to the savepoint number
	/// at which it was first registered. These keys join the write-set keys
	/// in the oracle conflict check at commit but are never written to
	/// storage.
	///
	/// [`get_for_update`]: Transaction::get_for_update
	pub(crate) read_set: BTreeMap<Key, u32>,

	/// `closed` indicates if the transaction is closed. A closed transaction
	/// cannot make any more changes to the data.
	closed: bool,

	/// The sequence number when this transaction started.
	pub(crate) start_seq_num: u64,

	/// The start of this transaction's conflict window in the commit ring:
	/// every commit at or below it is visible at `start_seq_num`, and commit
	/// validates against the commits after it. Unused by read-only
	/// transactions.
	commit_window: u64,

	/// `savepoints` indicates the current number of stacked savepoints; zero
	/// means none.
	savepoints: u32,

	/// write sequence number is used for real-time ordering of writes within a
	/// transaction.
	write_seqno: u32,

	/// Registry slot in `Core::active_txn_tracker`, pinning the commit ring's
	/// retired watermark at or below `commit_window` so the entries this
	/// transaction validates against stay reachable. Drop unregisters it.
	/// `Option<>` so that `rollback`/`commit` can release the slot promptly
	/// rather than waiting for the `Transaction`'s own `Drop`.
	txn_guard: Option<ActiveTxnGuard>,
}

impl Transaction {
	/// Test observer: the start of this transaction's conflict window in the commit ring.
	#[cfg(test)]
	pub(crate) fn commit_window_for_test(&self) -> u64 {
		self.commit_window
	}

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

		// Only mutating transactions (ReadWrite and WriteOnly) open a commit
		// window, because ReadOnly transactions never commit or write,
		// eliminating global write lock contention on the read path. The pin
		// is registered before the window is read, so retirement can never
		// pass the window.
		let (txn_guard, commit_window) = if mode.is_read_only() {
			(None, 0)
		} else {
			let pipeline = &core.commit_pipeline;
			let guard = core.active_txn_tracker.register(pipeline.commit_pin());
			(Some(guard), pipeline.commit_window())
		};

		// Get the current visible sequence number as our start point. Read
		// after the window, so every commit at or below it is visible here.
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
			range_deletes: 0,
			read_set: BTreeMap::new(),
			durability,
			closed: false,
			start_seq_num,
			commit_window,
			savepoints: 0,
			write_seqno: 0,
			txn_guard,
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

	/// Inserts a key-value pair with a specific timestamp, in nanoseconds, which a tree that is
	/// not versioned does not keep. A timestamp of 0 is the time of the commit.
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

	/// Deletes a range of keys [start, end) as an O(1) range tombstone.
	/// If start >= end, this is a no-op (empty range).
	pub fn delete_range<K>(&mut self, start: K, end: K) -> Result<()>
	where
		K: IntoBytes,
	{
		let start_bytes = start.into_bytes();
		let end_bytes = end.into_bytes();
		if start_bytes.as_slice() >= end_bytes.as_slice() {
			return Ok(());
		}

		let write_seqno = self.next_write_seqno();
		let entry = Entry::new(
			start_bytes,
			Some(end_bytes),
			InternalKeyKind::RangeDelete,
			self.savepoints,
			write_seqno,
			Entry::COMMIT_TIME,
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
		let write_seqno = self.next_write_seqno();

		let entry = Entry::new(
			key,
			Some(value),
			InternalKeyKind::Replace,
			self.savepoints,
			write_seqno,
			Entry::COMMIT_TIME,
		);

		self.write(entry)?;
		Ok(())
	}

	/// The range deletes of the write set, as `(start, end, seqno)`.
	fn pending_range_deletes(&self) -> Vec<(&[u8], &[u8], u32)> {
		if self.range_deletes == 0 {
			return Vec::new();
		}
		self.write_set
			.iter()
			.flat_map(|(start, entries)| {
				entries.iter().filter_map(move |entry| {
					(entry.kind == InternalKeyKind::RangeDelete)
						.then_some(entry.value.as_deref())
						.flatten()
						.map(|end| (start.as_slice(), end, entry.seqno))
				})
			})
			.collect()
	}

	/// The latest range delete of the write set that covers `key`, if there is one.
	fn pending_range_delete(&self, key: &[u8]) -> Option<&Entry> {
		if self.range_deletes == 0 {
			return None;
		}
		let mut latest: Option<&Entry> = None;
		for (start, entries) in &self.write_set {
			for entry in entries {
				if entry.kind != InternalKeyKind::RangeDelete {
					continue;
				}
				let Some(end) = entry.value.as_ref() else {
					continue;
				};
				if key >= start.as_slice()
					&& key < end.as_slice()
					&& latest.is_none_or(|prev| entry.seqno > prev.seqno)
				{
					latest = Some(entry);
				}
			}
		}
		latest
	}

	/// Gets a value for a key if it exists.
	pub fn get<K>(&self, key: K) -> Result<Option<Value>>
	where
		K: IntoBytes,
	{
		self.get_with_options(key, &ReadOptions::default())
	}

	/// Gets a value for a key if it exists, and registers the key for
	/// commit-time conflict validation.
	///
	/// Read semantics are identical to [`get`], including read-your-own-writes
	/// from the write set. In addition, the key joins the oracle conflict
	/// check at commit: if another transaction commits a write to the key
	/// after this transaction started, the commit fails with
	/// [`Error::TransactionWriteConflict`]. The locked read itself persists
	/// nothing.
	///
	/// The guarantee is snapshot validation, not mutual exclusion: commit
	/// fails only if another transaction committed a *write* to the key
	/// after this transaction's snapshot. A locked read publishes nothing,
	/// so it is invisible to other transactions' conflict checks — two
	/// transactions may lock the same key concurrently and, provided
	/// neither commits a write to it, both commit successfully. Locked
	/// reads therefore do not prevent write skew between transactions that
	/// only read the contended key; callers that need concurrent lockers of
	/// the same key to conflict with each other must write the key (for
	/// example, by writing back the value that was read).
	///
	/// Requires a `ReadWrite` transaction: read-only transactions cannot run
	/// commit-time validation, and write-only transactions cannot read.
	///
	/// [`get`]: Transaction::get
	pub fn get_for_update<K>(&mut self, key: K) -> Result<Option<Value>>
	where
		K: IntoBytes,
	{
		// Commit-time validation requires a commit, which read-only
		// transactions cannot perform.
		if !self.mode.mutable() {
			return Err(Error::TransactionReadOnly);
		}

		let key = key.into_bytes();
		let value = self.get_with_options(key.as_slice(), &ReadOptions::default())?;

		// Register the key for the commit-time oracle check. Keep the
		// earliest savepoint number so a savepoint rollback only releases
		// locks first acquired after the corresponding set_savepoint call.
		self.read_set.entry(key).or_insert(self.savepoints);

		Ok(value)
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

		// RYOW: the version of the write set that answers at `timestamp`, as it would once
		// committed. A write of the transaction at a time that has not come is not a candidate,
		// and one to be stamped with the time of its commit is after every time that has passed.
		let range_delete = self.pending_range_delete(key.as_slice());
		// What the snapshot holds for the key is hidden by a delete of the transaction
		let mut snapshot_hidden = range_delete.is_some();
		let mut best: Option<&Entry> = None;
		for entry in self.write_set.get(key.as_slice()).into_iter().flatten() {
			// A range delete of the transaction that came after the write hides it
			if range_delete.is_some_and(|r| r.seqno > entry.seqno) {
				continue;
			}
			// A hard delete wipes all history before it, and a replace as well
			if entry.is_hard_delete() {
				best = None;
				snapshot_hidden = true;
				continue;
			}
			if entry.kind == InternalKeyKind::Replace {
				best = None;
				snapshot_hidden = true;
			}
			let time = entry.effective_timestamp();
			// Of versions of one time the newest write is the answer
			if time <= timestamp && best.is_none_or(|b| time >= b.effective_timestamp()) {
				best = Some(entry);
			}
		}

		// What the snapshot holds of the key, as of the time
		let stored = match &self.snapshot {
			Some(_) if snapshot_hidden => None,
			Some(snapshot) => snapshot.version_at(key.as_slice(), timestamp)?,
			None => return Err(Error::NoSnapshot),
		};

		// A write of the transaction is newer than a stored version of the same time
		let version = match (best, stored) {
			(Some(entry), Some(stored)) if stored.time > entry.effective_timestamp() => {
				stored.value
			}
			(Some(entry), _) => (!entry.is_tombstone()).then(|| entry.value.clone()).flatten(),
			(None, stored) => stored.and_then(|stored| stored.value),
		};
		Ok(version)
	}

	/// Asynchronously gets a value for a key from the database.
	pub async fn get_async<K>(&self, key: K) -> Result<Option<Value>>
	where
		K: IntoBytes,
	{
		let key_slice = key.as_slice();
		if self.closed {
			return Err(Error::TransactionClosed);
		}
		if key_slice.is_empty() {
			return Err(Error::EmptyKey);
		}
		if self.mode.is_write_only() {
			return Err(Error::TransactionWriteOnly);
		}

		let range_delete_entry = self.pending_range_delete(key_slice);

		if let Some(last_entry) = self.write_set.get(key_slice).and_then(|entries| entries.last()) {
			if let Some(r) = range_delete_entry {
				if r.seqno > last_entry.seqno {
					return Ok(None);
				}
			}
			if last_entry.is_tombstone() {
				return Ok(None);
			}
			if let Some(v) = &last_entry.value {
				return Ok(Some(v.clone()));
			}
			return Ok(None);
		} else if range_delete_entry.is_some() {
			return Ok(None);
		}

		match self.snapshot.as_ref().unwrap().get_async(key_slice).await? {
			Some(val) => {
				let resolved_value = self.core.resolve_value(&val.0)?;
				Ok(Some(resolved_value))
			}
			None => Ok(None),
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
		let range_delete_entry = self.pending_range_delete(key.as_slice());

		if let Some(last_entry) =
			self.write_set.get(key.as_slice()).and_then(|entries| entries.last())
		{
			if let Some(r) = range_delete_entry {
				if r.seqno > last_entry.seqno {
					return Ok(None);
				}
			}
			// If the entry is a tombstone, return None.
			if last_entry.is_tombstone() {
				return Ok(None);
			}
			if let Some(v) = &last_entry.value {
				return Ok(Some(v.clone()));
			}
			// If the entry has no value, it means the key was deleted in this transaction.
			return Ok(None);
		} else if range_delete_entry.is_some() {
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

	/// Iterates over all keys and values in the database.
	///
	/// Returns a cursor-based iterator implementing `LSMIterator` with explicit
	/// seek/next/prev methods.
	///
	/// # Example
	/// ```ignore
	/// let mut iter = tx.iter()?;
	/// iter.seek_first()?;
	/// while iter.valid() {
	///     let key = iter.key().user_key();
	///     let value = iter.value()?;
	///     iter.next()?;
	/// }
	/// ```
	pub fn iter(&self) -> Result<impl LSMIterator + '_> {
		self.range_with_options(&ReadOptions::default())
	}

	/// Scans keys and values in the specified range bounds.
	///
	/// Supports all standard Rust range expressions:
	/// - `..` (unbounded full keyspace)
	/// - `start..end` (inclusive start, exclusive end)
	/// - `start..=end` (inclusive start, inclusive end)
	/// - `start..` (inclusive start, unbounded end)
	/// - `..end` (unbounded start, exclusive end)
	/// - `..=end` (unbounded start, inclusive end)
	///
	/// # Example
	/// ```ignore
	/// // Full keyspace scan
	/// let mut iter = tx.scan(..)?;
	///
	/// // Half-open range
	/// let mut iter = tx.scan(b"user:100"..b"user:200")?;
	///
	/// // Inclusive range
	/// let mut iter = tx.scan(b"a"..=b"z")?;
	///
	/// // Prefix scan
	/// let mut iter = tx.scan(b"order:"..)?;
	/// ```
	pub fn scan<R: KeyRange>(&self, range: R) -> Result<impl LSMIterator + '_> {
		let options = ReadOptions::from_range(range);
		self.range_with_options(&options)
	}

	/// Gets keys in a key range at a specific timestamp.
	///
	/// Gets the newest value of each key in a range, as of the transaction, with the writes of the
	/// transaction itself.
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
	///     let value = iter.value()?;
	///     iter.next()?;
	/// }
	///
	/// // Or seek to a specific key:
	/// iter.seek(b"foo")?;  // Position at "foo", or the first key after it
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
		TransactionRangeIterator::new_with_options(
			self,
			Arc::clone(&self.core),
			options.lower_bound.clone(),
			options.upper_bound.clone(),
		)
	}

	/// Returns a unified history iterator over ALL versions of keys in the range.
	///
	/// Returns an iterator implementing `LSMIterator`. The versions of a key come newest
	/// timestamp first, and the newest write first among those of one timestamp; the keys come in
	/// order. A write of the transaction that is to be given the time of its commit is reported
	/// at `u64::MAX` until it is committed, and a version written while the tree was not
	/// versioned is reported at the time it reads at, that of the versions written before it.
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
			None,
		)?;

		// RYOW: Collect write-set entries in the key range
		// Hard deletes are tracked separately and skip all snapshot history for that key
		let mut write_set_entries: Vec<(&Key, &Entry)> = Vec::new();
		let mut hard_delete_keys: std::collections::HashSet<&[u8]> =
			std::collections::HashSet::new();

		// The range deletes of the transaction: they hide what the snapshot holds for the keys
		// they cover, as a hard delete does for its key.
		let pending_ranges = self.pending_range_deletes();

		for (key, entry_list) in
			self.write_set.range(start.as_slice().to_vec()..end.as_slice().to_vec())
		{
			// Every write of the transaction to the key is a version, oldest write first, but
			// for those that another write of the same time supersedes
			// (a write at the time of the commit is not one at the time it is reported at)
			let superseded = Entry::superseded(entry_list, |entry| {
				(entry.timestamp == Entry::COMMIT_TIME, entry.effective_timestamp())
			});
			let mut versions: Vec<&Entry> = Vec::new();
			for (at, entry) in entry_list.iter().enumerate() {
				let superseded =
					superseded.as_ref().map_or(Superseded::Kept, |outcome| outcome[at]);
				// A range delete of the transaction that came after the write hides it
				if pending_ranges.iter().any(|(s, e, seqno)| {
					*seqno > entry.seqno && key.as_slice() >= *s && key.as_slice() < *e
				}) {
					continue;
				}
				// Hard deletes wipe all history before them - track them separately. They are
				// never returned (even with include_tombstones).
				if entry.is_hard_delete() {
					hard_delete_keys.insert(key.as_slice());
					versions.clear();
					continue;
				}
				// A replace is a version, and wipes the history before it
				if entry.kind == InternalKeyKind::Replace {
					hard_delete_keys.insert(key.as_slice());
					versions.clear();
				}
				if superseded == Superseded::Kept {
					versions.push(entry);
				}
			}
			// Newest timestamp first, and the newest write first among those of one timestamp
			versions.sort_by(|a, b| {
				b.effective_timestamp().cmp(&a.effective_timestamp()).then(b.seqno.cmp(&a.seqno))
			});
			for entry in versions {
				// Filter by timestamp range if specified
				if let Some((ts_start, ts_end)) = opts.ts_range {
					let timestamp = entry.effective_timestamp();
					if timestamp < ts_start || timestamp > ts_end {
						continue;
					}
				}
				write_set_entries.push((key, entry));
			}
		}

		let pending_ranges: Vec<(Vec<u8>, Vec<u8>)> =
			pending_ranges.into_iter().map(|(s, e, _)| (s.to_vec(), e.to_vec())).collect();

		Ok(TransactionHistoryIterator::new(
			inner,
			Arc::clone(&self.core),
			write_set_entries,
			hard_delete_keys,
			pending_ranges,
			opts.include_tombstones,
			opts.limit,
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
		if e.kind == InternalKeyKind::RangeDelete {
			self.range_deletes += 1;
		}

		match self.write_set.entry(key) {
			BTreeEntry::Occupied(mut oe) => {
				let entries = oe.get_mut();
				// A write that the transaction's latest write to this key makes irrelevant
				// replaces it, as long as both are of the same savepoint: those of different
				// savepoints stay apart so that a rollback to a savepoint can restore what was
				// there before it. Otherwise this adds an entry, and what one version of the
				// key at one time leaves out of the others is decided when the writes are read
				// or committed, see `Entry::superseded`.
				let savepoint = e.savepoint_no;
				if self.core.opts.enable_versioning {
					match e.kind {
						// A hard delete or a replace wipes the history of the key, so it makes
						// every earlier write of the savepoint irrelevant. A range delete under
						// the key hides what is written before it, and stays.
						InternalKeyKind::Delete | InternalKeyKind::Replace => {
							entries.retain(|earlier| {
								earlier.savepoint_no != savepoint
									|| earlier.kind == InternalKeyKind::RangeDelete
							});
							entries.push(e);
						}
						// A version replaces one that was written at the same time. A hard
						// delete or a replace is not replaced by it, as that would leave the
						// history before it in place.
						InternalKeyKind::Set | InternalKeyKind::SoftDelete => {
							// Of the last few writes, the one of the same time, which this
							// supersedes (or a hard delete or a replace after it hides): a key
							// that is written at a few times in turn stays as short as they are
							let from = entries.len().saturating_sub(8);
							let same_time = entries[from..]
								.iter()
								.rposition(|earlier| {
									earlier.savepoint_no == savepoint
										&& matches!(
											earlier.kind,
											InternalKeyKind::Set | InternalKeyKind::SoftDelete
										)
										&& earlier.timestamp == e.timestamp
								})
								.map(|at| from + at);
							match same_time {
								Some(at) if at + 1 == entries.len() => entries[at] = e,
								Some(at) => {
									entries.remove(at);
									entries.push(e);
								}
								None => entries.push(e),
							}
						}
						_ => entries.push(e),
					}
				} else {
					// Without versioning there is no history for a hard delete to wipe or for
					// times to tell apart: a write replaces the latest one of its savepoint,
					// unless both were given different times.
					let replaces = entries.last().is_some_and(|last| {
						last.savepoint_no == savepoint
							&& last.kind != InternalKeyKind::RangeDelete
							&& e.kind != InternalKeyKind::RangeDelete
							&& !(last.timestamp != Entry::COMMIT_TIME
								&& e.timestamp != Entry::COMMIT_TIME
								&& last.timestamp != e.timestamp)
					});
					if replaces {
						*entries.last_mut().unwrap() = e;
					} else {
						entries.push(e);
					}
				}
			}
			BTreeEntry::Vacant(ve) => {
				ve.insert(vec![e]);
			}
		}

		Ok(())
	}

	/// Commits the transaction, writing all pending entries to the store and
	/// validating any locked reads against the commit oracle.
	///
	/// A commit attempt is terminal: on success and on failure alike the
	/// transaction closes, and every subsequent operation on it returns
	/// [`Error::TransactionClosed`]. The attempt consumes the pending write
	/// and locked-read state, so a failed commit cannot be retried — begin a
	/// new transaction instead.
	pub async fn commit(&mut self) -> Result<()> {
		// If the transaction is closed, return an error.
		if self.closed {
			return Err(Error::TransactionClosed);
		}

		// If the transaction is read-only, return an error.
		if self.mode.is_read_only() {
			return Err(Error::TransactionReadOnly);
		}

		// Close before awaiting so a cancelled commit cannot be retried.
		self.closed = true;

		// Moving the guard here releases it even if commit is cancelled.
		let txn_guard = self.txn_guard.take();
		let result = self.commit_inner().await;
		drop(txn_guard);

		result
	}

	/// Performs the fallible portion of [`commit`]: batch construction,
	/// oracle validation, and the pipeline write. The caller closes the
	/// transaction and releases the watermark slot regardless of the
	/// outcome.
	///
	/// [`commit`]: Transaction::commit
	async fn commit_inner(&mut self) -> Result<()> {
		// If there are no pending writes, only the locked reads (if any) need
		// commit-time validation; nothing is persisted.
		if self.write_set.is_empty() {
			if self.read_set.is_empty() {
				return Ok(());
			}
			let read_set: Vec<Key> = std::mem::take(&mut self.read_set).into_keys().collect();
			return self
				.core
				.check_conflicts(&read_set, self.start_seq_num, self.commit_window)
				.await;
		}

		// Create and prepare batch directly. `Batch::new(0)`: the
		// `starting_seq_num` will be stamped by the commit pipeline after
		// seq allocation. The pipeline derives oracle keys from
		// `batch.entries` itself, so we don't pre-collect a parallel vector.
		let mut batch = Batch::new(0);

		// Locked-read keys not shadowed by a write join the oracle conflict
		// check alongside the batch keys; shadowed keys are already covered
		// by the batch entries.
		let read_set: Vec<Key> = std::mem::take(&mut self.read_set)
			.into_keys()
			.filter(|k| !self.write_set.contains_key(k))
			.collect();

		// Extract the vector of entries for the current transaction,
		// respecting the insertion order recorded with Entry::seqno.
		// Generate a single timestamp for this commit
		let commit_timestamp = self.core.opts.clock.now();

		// What a transaction writes to a key at one time is one version of it: the last of its
		// writes, and not the states in between that a savepoint kept apart.
		let mut latest_writes: Vec<Entry> = Vec::new();
		let versioned = self.core.opts.enable_versioning;
		for (_, entries) in std::mem::take(&mut self.write_set) {
			// A tree that is not versioned has no history for more than one version of a key to
			// be in: its writes are logged as they are.
			if !versioned {
				latest_writes.extend(entries);
				continue;
			}
			let outcome = Entry::superseded(&entries, |entry| {
				(
					false,
					if entry.timestamp == Entry::COMMIT_TIME {
						commit_timestamp
					} else {
						entry.timestamp
					},
				)
			});
			let Some(outcome) = outcome else {
				latest_writes.extend(entries);
				continue;
			};
			for (mut entry, outcome) in entries.into_iter().zip(outcome) {
				match outcome {
					Superseded::Kept => {}
					Superseded::Dropped => continue,
					Superseded::BarrierOnly => {
						entry.kind = InternalKeyKind::Delete;
						entry.value = None;
					}
				}
				latest_writes.push(entry);
			}
		}
		latest_writes.sort_by_key(|a| a.seqno);

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

		// Write the batch to storage. The pipeline runs the oracle.check +
		// seq alloc + oracle.publish + WAL atomically under `write_mutex`,
		// then runs memtable apply OUTSIDE the lock.
		let should_sync = self.durability == Durability::Immediate;
		self.core
			.commit(batch, should_sync, self.start_seq_num, self.commit_window, &read_set)
			.await
	}

	pub fn rollback(&mut self) {
		self.closed = true;
		self.write_set.clear();
		self.range_deletes = 0;
		self.read_set.clear();
		self.snapshot.take();
		self.savepoints = 0;
		self.write_seqno = 0;
		// Release the GC watermark slot eagerly (Drop is a fallback for panic paths).
		if let Some(mut g) = self.txn_guard.take() {
			g.release();
		}
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
		self.range_deletes = self
			.write_set
			.values()
			.flatten()
			.filter(|entry| entry.kind == InternalKeyKind::RangeDelete)
			.count();

		// Release locked reads first acquired since the last call to
		// set_savepoint().
		self.read_set.retain(|_, savepoint_no| *savepoint_no != self.savepoints);

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
#[derive(Clone, Debug)]
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

	/// The time of the write for ordering and reporting: its own, or, for one that is to be
	/// stamped with the time of the commit, the newest there is: it is after every version that
	/// exists, and only a read at the end of time sees it.
	fn effective_timestamp(&self) -> u64 {
		if self.timestamp == Self::COMMIT_TIME {
			u64::MAX
		} else {
			self.timestamp
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

	/// Whether this entry is a version of its key, a write that has a time: a hard delete and a
	/// range delete are not.
	fn is_version(&self) -> bool {
		matches!(
			self.kind,
			InternalKeyKind::Set | InternalKeyKind::SoftDelete | InternalKeyKind::Replace
		)
	}

	/// What becomes of each of the writes of a transaction to one key, in the order they were
	/// done in, once a version that another of them supersedes is taken out; `None` if there is
	/// nothing to take out.
	///
	/// A transaction is atomic: what it writes to a key at one time is one version of it, the
	/// last of its writes, and the others are states that no one could have read, whether
	/// they were done under another savepoint or not. Writes at different times are different
	/// versions. `time_of` tells the time a write is given, and whether it is that of the commit
	/// that is not known yet: those of two writes are the same when both parts are.
	///
	/// A replace that another write supersedes is not a version any more, but it still wipes
	/// the history of the key.
	fn superseded(
		entries: &[Entry],
		time_of: impl Fn(&Entry) -> (bool, u64),
	) -> Option<Vec<Superseded>> {
		if entries.len() < 2 {
			return None;
		}
		let mut outcome: Option<Vec<Superseded>> = None;
		// A key is written once or twice, or a few times: a set is for one written many times
		let mut few: Vec<(bool, u64)> = Vec::new();
		let mut many = std::collections::HashSet::new();
		let mut first_of_its_time = |time: (bool, u64)| {
			if entries.len() <= 16 {
				let new = !few.contains(&time);
				few.push(time);
				new
			} else {
				many.insert(time)
			}
		};
		for (at, entry) in entries.iter().enumerate().rev() {
			if entry.is_version() && !first_of_its_time(time_of(entry)) {
				outcome.get_or_insert_with(|| vec![Superseded::Kept; entries.len()])[at] =
					if entry.kind == InternalKeyKind::Replace {
						Superseded::BarrierOnly
					} else {
						Superseded::Dropped
					};
			}
		}
		outcome
	}
}

/// What `Entry::superseded` makes of a write.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Superseded {
	/// The write stands.
	Kept,
	/// The write is not a version: another one of the same time follows it.
	Dropped,
	/// A replace that is not a version any more, and wipes the history before it as a hard
	/// delete does.
	BarrierOnly,
}

/// Current source for the transaction iterator
#[derive(Clone, Copy, PartialEq)]
enum CurrentSource {
	Snapshot,
	WriteSet,
	None,
}

/// Re-anchors the *non-current* sub-cursor when a write-set merge iterator
/// reverses direction. Shared by `TransactionRangeIterator` and
/// `TransactionHistoryIterator`, which are otherwise byte-identical here.
///
/// # Why the invalid case re-anchors rather than seeks
///
/// A plain `seek_first`/`seek_last` would be wrong for any `I` that carries state
/// accumulated across the traversal, because a seek legitimately resets it —
/// `HistoryIterator` refunds its whole `limit` budget. This is a continuation of a
/// traversal in progress, not a new one, so it goes through
/// [`LSMIterator::reanchor_first`]/[`reanchor_last`](LSMIterator::reanchor_last),
/// whose contract is exactly "reposition without starting over". The default
/// implementations are the plain seeks, so position-only iterators are unaffected.
///
/// Deciding this with a *predicate* instead — asking the iterator whether it is
/// exhausted and skipping the re-anchor if not — does not work: an iterator can
/// stop early without recording that it did, and it leaves the reverse pass no
/// way to run on the remaining budget. Hence an operation, not a branch.
///
/// # The invariant this restores
///
/// After a positioning call, the merge iterator holds two sub-cursors and
/// `current_source` names the one sitting on the emitted entry. The *other*
/// cursor is, by construction, on the nearest entry at or beyond the emitted one
/// in the current direction — strictly beyond it in the ordinary case, or exactly
/// on it in the RYOW case where both sources hold the same key and the write-set
/// copy won (`is_key_equal`) — or exhausted, meaning it has no such entry at all.
///
/// Reversing direction has to turn that into "the nearest entry strictly beyond
/// the emitted one in the NEW direction", for the other cursor only; the current
/// cursor is stepped afterwards by the caller's ordinary advance. Stepping the
/// other cursor once handles both shapes: from strictly-beyond it lands on the
/// neighbour on the far side of the emitted entry, and from on-the-emitted-entry
/// it lands on that same neighbour. Three cases:
///
/// - the other cursor is positioned: step it one entry in the new direction;
/// - the other cursor is invalid: either it ran out of entries, in which case every one of them
///   lies on the new direction's side of the emitted entry, or it stopped early under a policy that
///   is still in force. Both are served by re-anchoring it to its extreme entry — a still-binding
///   policy re-applies itself there and the cursor simply stays invalid.
///
/// # What this replaced
///
/// ```ignore
/// if !inner.valid() || !self.ws_valid() {
///     self.seek_ws_first();   // seek_ws_last() in the prev() mirror
/// } else if self.current_source == CurrentSource::Snapshot {
///     self.advance_ws();
/// } else {
///     inner.next()?;
/// }
/// ```
///
/// That block had two independent faults, both live in three copies:
///
/// 1. **Teleport.** The guard fired on `!inner.valid()` — permanently true for an uncommitted
///    transaction with no committed data — and then reset the *write-set* cursor to index 0 (or the
///    last index). `advance_ws()` walked it straight back, so the iterator re-emitted the entry it
///    was already on and failed to invalidate at the range's edge.
/// 2. **Missing re-anchor.** When the exhausted cursor was `inner` and the current source was the
///    write-set, `inner` was never repositioned at all — only the `current_source == Snapshot`
///    branch ever touched it — so committed entries behind the cursor became unreachable and
///    iteration invalidated one step early.
///
/// `CurrentSource::None` cannot reach here: `next()`/`prev()` return early on an
/// invalid iterator. It is handled as a no-op so this stays total.
fn reanchor_other_source<I: LSMIterator>(
	inner: &mut I,
	ws_pos: &mut Option<usize>,
	ws_len: usize,
	current_source: CurrentSource,
	new_direction: MergeDirection,
) -> Result<()> {
	match current_source {
		CurrentSource::None => {}
		// The write-set is the other source.
		CurrentSource::Snapshot => {
			*ws_pos = match (*ws_pos, new_direction) {
				(Some(pos), MergeDirection::Forward) => (pos + 1 < ws_len).then_some(pos + 1),
				(Some(pos), MergeDirection::Backward) => pos.checked_sub(1),
				(None, MergeDirection::Forward) => (ws_len > 0).then_some(0),
				(None, MergeDirection::Backward) => ws_len.checked_sub(1),
			};
		}
		// The snapshot / history iterator is the other source.
		CurrentSource::WriteSet => match (inner.valid(), new_direction) {
			(true, MergeDirection::Forward) => {
				inner.next()?;
			}
			(true, MergeDirection::Backward) => {
				inner.prev()?;
			}
			// Re-anchor, NOT seek: this continues a traversal already in progress,
			// so any budget the cursor has spent must survive. See above.
			(false, MergeDirection::Forward) => {
				inner.reanchor_first()?;
			}
			(false, MergeDirection::Backward) => {
				inner.reanchor_last()?;
			}
		},
	}
	Ok(())
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

	/// The ranges that the transaction deleted, `(start, end)`: the keys of the snapshot they
	/// cover are not part of what the transaction reads, whichever of them the write set holds.
	pending_ranges: Vec<(&'a [u8], &'a [u8])>,

	/// Current position in write-set entries.
	/// None means write-set is exhausted/not positioned.
	/// Uses single index for both forward and backward iteration.
	ws_pos: Option<usize>,

	/// True when snapshot and write-set are positioned at the same user key.
	/// When true, advancing must move both sources together.
	is_key_equal: bool,

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
		lower: Option<Vec<u8>>,
		upper: Option<Vec<u8>>,
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
		// A missing bound is unbounded on that side, not the empty key.
		let snapshot_iter = snapshot.range(lower.as_deref(), upper.as_deref())?;

		// Collect write-set entries for the range
		let mut write_set_entries: Vec<(&'a Key, &'a Entry)> = Vec::new();
		let ws_range = (
			lower.map_or(Bound::Unbounded, Bound::Included),
			upper.map_or(Bound::Unbounded, Bound::Excluded),
		);
		// A range delete of the write set can start before the range that is read
		let ranges = tx.pending_range_deletes();
		for (key, entry_list) in tx.write_set.range::<Key, _>(ws_range) {
			if let Some(entry) = entry_list.last() {
				// A range delete that came after the write hides it
				if ranges.iter().any(|(start, end, seqno)| {
					*seqno > entry.seqno && key.as_slice() >= *start && key.as_slice() < *end
				}) {
					continue;
				}
				write_set_entries.push((key, entry));
			}
		}
		let pending_ranges = ranges.into_iter().map(|(start, end, _)| (start, end)).collect();

		Ok(Self {
			snapshot_iter,
			core,
			write_set_entries,
			pending_ranges,
			ws_pos: None,
			is_key_equal: false,
			current_source: CurrentSource::None,
			ws_encoded_key_buf: Vec::new(),
			direction: MergeDirection::Forward,
			initialized: false,
		})
	}

	/// Check if write-set is positioned at a valid entry.
	fn ws_valid(&self) -> bool {
		self.ws_pos.is_some()
	}

	/// Get current write-set key.
	fn ws_key(&self) -> &[u8] {
		debug_assert!(self.ws_valid());
		self.write_set_entries[self.ws_pos.unwrap()].0.as_slice()
	}

	/// Check if current write-set entry is a tombstone.
	fn ws_is_tombstone(&self) -> bool {
		debug_assert!(self.ws_valid());
		self.write_set_entries[self.ws_pos.unwrap()].1.is_tombstone()
	}

	/// Advance write-set position in the current direction.
	fn advance_ws(&mut self) {
		if let Some(pos) = self.ws_pos {
			self.ws_pos = match self.direction {
				MergeDirection::Forward => {
					if pos + 1 < self.write_set_entries.len() {
						Some(pos + 1)
					} else {
						None
					}
				}
				MergeDirection::Backward => {
					if pos > 0 {
						Some(pos - 1)
					} else {
						None
					}
				}
			};
		}
	}

	/// Seek write-set to first entry >= target (for forward iteration).
	fn seek_ws(&mut self, target: &[u8]) {
		let pos = self.write_set_entries.partition_point(|(k, _)| k.as_slice() < target);
		self.ws_pos = if pos < self.write_set_entries.len() {
			Some(pos)
		} else {
			None
		};
	}

	/// Seek write-set to first entry.
	fn seek_ws_first(&mut self) {
		self.ws_pos = if self.write_set_entries.is_empty() {
			None
		} else {
			Some(0)
		};
	}

	/// Seek write-set to last entry.
	fn seek_ws_last(&mut self) {
		self.ws_pos = if self.write_set_entries.is_empty() {
			None
		} else {
			Some(self.write_set_entries.len() - 1)
		};
	}

	/// Populate encoded key buffer for current write-set entry.
	fn populate_ws_encoded_key(&mut self) {
		let Some(pos) = self.ws_pos else {
			return;
		};

		let (key, entry) = self.write_set_entries[pos];

		self.ws_encoded_key_buf.clear();
		self.ws_encoded_key_buf.extend_from_slice(key);
		let trailer = ((entry.seqno as u64) << 8) | (entry.kind as u64);
		self.ws_encoded_key_buf.extend_from_slice(&trailer.to_be_bytes());
	}

	/// Whether the transaction deleted the key of the snapshot with a range delete.
	fn snapshot_key_hidden(&self) -> bool {
		!self.pending_ranges.is_empty() && self.snapshot_iter.valid() && {
			let key = self.snapshot_iter.key().user_key();
			self.pending_ranges.iter().any(|(start, end)| key >= *start && key < *end)
		}
	}

	/// Position to minimum of two sources (forward merge).
	/// Sets is_key_equal = true when both sources are at the same user key.
	fn position_to_min(&mut self) -> Result<bool> {
		loop {
			self.is_key_equal = false;
			if self.snapshot_key_hidden() {
				self.snapshot_iter.next()?;
				continue;
			}
			let snap_valid = self.snapshot_iter.valid();
			let ws_valid = self.ws_valid();

			self.current_source = match (snap_valid, ws_valid) {
				(false, false) => CurrentSource::None,
				(true, false) => CurrentSource::Snapshot,
				(false, true) => {
					if self.ws_is_tombstone() {
						self.advance_ws();
						continue;
					}
					self.populate_ws_encoded_key();
					CurrentSource::WriteSet
				}
				(true, true) => {
					let snap_key = self.snapshot_iter.key().user_key();
					let ws_key = self.ws_key();

					match snap_key.cmp(ws_key) {
						Ordering::Less => CurrentSource::Snapshot,
						Ordering::Greater => {
							if self.ws_is_tombstone() {
								self.advance_ws();
								continue;
							}
							self.populate_ws_encoded_key();
							CurrentSource::WriteSet
						}
						Ordering::Equal => {
							// RYOW: write-set wins, but keep both at same key
							self.is_key_equal = true;
							if self.ws_is_tombstone() {
								// Tombstone masks snapshot - skip both
								self.snapshot_iter.next()?;
								self.advance_ws();
								self.is_key_equal = false;
								continue;
							}
							self.populate_ws_encoded_key();
							CurrentSource::WriteSet
						}
					}
				}
			};
			return Ok(self.current_source != CurrentSource::None);
		}
	}

	/// Position to maximum of two sources (backward merge).
	/// Sets is_key_equal = true when both sources are at the same user key.
	fn position_to_max(&mut self) -> Result<bool> {
		loop {
			self.is_key_equal = false;
			if self.snapshot_key_hidden() {
				self.snapshot_iter.prev()?;
				continue;
			}
			let snap_valid = self.snapshot_iter.valid();
			let ws_valid = self.ws_valid();

			self.current_source = match (snap_valid, ws_valid) {
				(false, false) => CurrentSource::None,
				(true, false) => CurrentSource::Snapshot,
				(false, true) => {
					if self.ws_is_tombstone() {
						self.advance_ws();
						continue;
					}
					self.populate_ws_encoded_key();
					CurrentSource::WriteSet
				}
				(true, true) => {
					let snap_key = self.snapshot_iter.key().user_key();
					let ws_key = self.ws_key();

					match snap_key.cmp(ws_key) {
						Ordering::Greater => CurrentSource::Snapshot,
						Ordering::Less => {
							if self.ws_is_tombstone() {
								self.advance_ws();
								continue;
							}
							self.populate_ws_encoded_key();
							CurrentSource::WriteSet
						}
						Ordering::Equal => {
							// RYOW: write-set wins, but keep both at same key
							self.is_key_equal = true;
							if self.ws_is_tombstone() {
								// Tombstone masks snapshot - skip both
								self.snapshot_iter.prev()?;
								self.advance_ws();
								self.is_key_equal = false;
								continue;
							}
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
		self.is_key_equal = false;

		// Encode user key with MAX trailer for >= seek
		let mut encoded = target.to_vec();
		encoded.extend_from_slice(&u64::MAX.to_be_bytes()); // max trailer
		self.snapshot_iter.seek(&encoded)?;

		// Position write-set at first entry >= target
		self.seek_ws(target);

		self.position_to_min()
	}

	fn seek_first(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Forward;
		self.initialized = true;
		self.is_key_equal = false;

		self.snapshot_iter.seek_first()?;
		self.seek_ws_first();

		self.position_to_min()
	}

	fn seek_last(&mut self) -> Result<bool> {
		self.direction = MergeDirection::Backward;
		self.initialized = true;
		self.is_key_equal = false;

		self.snapshot_iter.seek_last()?;
		self.seek_ws_last();

		self.position_to_max()
	}

	fn next(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_first();
		}

		// Stepping off an already-invalid iterator is a no-op: an exhausted
		// iterator stays exhausted until an explicit re-seek. Checked before the
		// direction-change fixup so that fixup can never mutate the sub-cursors
		// of an iterator that is about to report `false` anyway.
		if !self.valid() {
			return Ok(false);
		}

		// Direction change: backward → forward
		if self.direction != MergeDirection::Forward {
			self.direction = MergeDirection::Forward;
			self.is_key_equal = false;

			reanchor_other_source(
				&mut self.snapshot_iter,
				&mut self.ws_pos,
				self.write_set_entries.len(),
				self.current_source,
				MergeDirection::Forward,
			)?;

			// Check if now at equal keys
			if self.snapshot_iter.valid()
				&& self.ws_valid()
				&& self.snapshot_iter.key().user_key() == self.ws_key()
			{
				self.is_key_equal = true;
			}
		}

		// Advance CURRENT source (or both if is_key_equal)
		if self.is_key_equal {
			self.snapshot_iter.next()?;
			self.advance_ws();
			self.is_key_equal = false;
		} else {
			match self.current_source {
				CurrentSource::Snapshot => {
					self.snapshot_iter.next()?;
				}
				CurrentSource::WriteSet => {
					self.advance_ws();
				}
				CurrentSource::None => return Ok(false),
			}
		}

		self.position_to_min()
	}

	fn prev(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_last();
		}

		// See the matching note in `next()`.
		if !self.valid() {
			return Ok(false);
		}

		// Direction change: forward → backward
		if self.direction != MergeDirection::Backward {
			self.direction = MergeDirection::Backward;
			self.is_key_equal = false;

			reanchor_other_source(
				&mut self.snapshot_iter,
				&mut self.ws_pos,
				self.write_set_entries.len(),
				self.current_source,
				MergeDirection::Backward,
			)?;

			// Check if now at equal keys
			if self.snapshot_iter.valid()
				&& self.ws_valid()
				&& self.snapshot_iter.key().user_key() == self.ws_key()
			{
				self.is_key_equal = true;
			}
		}

		// Advance CURRENT source (or both if is_key_equal)
		if self.is_key_equal {
			self.snapshot_iter.prev()?;
			self.advance_ws();
			self.is_key_equal = false;
		} else {
			match self.current_source {
				CurrentSource::Snapshot => {
					self.snapshot_iter.prev()?;
				}
				CurrentSource::WriteSet => {
					self.advance_ws();
				}
				CurrentSource::None => return Ok(false),
			}
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

	fn value_encoded(&self) -> Result<&[u8]> {
		debug_assert!(self.valid());
		match self.current_source {
			CurrentSource::Snapshot => self.snapshot_iter.value_encoded(),
			CurrentSource::WriteSet => {
				let entry = self.write_set_entries[self.ws_pos.unwrap()].1;
				Ok(entry.value.as_ref().map_or(&[], |v| v.as_slice()))
			}
			CurrentSource::None => panic!("value() called on invalid iterator"),
		}
	}

	fn value(&self) -> Result<Value> {
		debug_assert!(self.valid());
		let raw = self.value_encoded()?;
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
/// ## Write-set position
///
/// The write-set entries are one flat list, sorted by key, then by timestamp (newest first) and
/// the newest write first among those of one timestamp, so that the list is merged with the
/// versions of the snapshot as one more sorted source. A single index, `ws_pos`, walks it in
/// either direction, and a seek puts it back at the start or the end.
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
	/// One entry for each version of a key that the transaction writes: what it writes to a key at
	/// one time is one version, the last of its writes, see `Entry::superseded`.
	/// Note: Hard delete entries are excluded (they go to `hard_delete_keys`).
	write_set_entries: Vec<(&'a Key, &'a Entry)>,

	/// Keys that have hard deletes or replaces in write-set.
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

	/// The `(start, end)` of the range deletes of the write set: the snapshot's versions of the
	/// keys they cover are not part of the history the transaction sees.
	pending_ranges: Vec<(Vec<u8>, Vec<u8>)>,

	/// Current position in write-set entries.
	/// None means write-set is exhausted/not positioned.
	/// Uses single index for both forward and backward iteration.
	ws_pos: Option<usize>,

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

	/// The most entries to yield in a traversal, counting those of the write set and those of
	/// the snapshot alike.
	limit: Option<usize>,

	/// Entries yielded since the traversal began: a seek begins one, a step in either
	/// direction continues it.
	entries_returned: usize,
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
		pending_ranges: Vec<(Vec<u8>, Vec<u8>)>,
		include_tombstones: bool,
		limit: Option<usize>,
	) -> Self {
		Self {
			inner,
			core,
			write_set_entries,
			hard_delete_keys,
			pending_ranges,
			ws_pos: None,
			current_source: CurrentSource::None,
			ws_encoded_key_buf: Vec::new(),
			direction: MergeDirection::Forward,
			initialized: false,
			include_tombstones,
			limit,
			entries_returned: 0,
		}
	}

	/// Counts the entry the iterator has been positioned at against the limit: once the budget
	/// is spent the iterator is no longer valid, and steps in either direction stay so.
	fn spend(&mut self, positioned: bool) -> bool {
		if !positioned {
			return false;
		}
		if self.limit.is_some_and(|limit| self.entries_returned >= limit) {
			self.current_source = CurrentSource::None;
			return false;
		}
		self.entries_returned += 1;
		true
	}

	// =========================================================================
	// Write-set helper methods (single-index design)
	// =========================================================================

	/// Check if write-set is positioned at a valid entry.
	fn ws_valid(&self) -> bool {
		self.ws_pos.is_some()
	}

	/// Get current write-set key.
	fn ws_key(&self) -> &[u8] {
		debug_assert!(self.ws_valid());
		self.write_set_entries[self.ws_pos.unwrap()].0.as_slice()
	}

	/// The time of the current write-set entry: its own, or the newest there is for one that is
	/// to be stamped with the time of its commit, see `Entry::effective_timestamp`.
	fn ws_timestamp(&self) -> u64 {
		debug_assert!(self.ws_valid());
		self.write_set_entries[self.ws_pos.unwrap()].1.effective_timestamp()
	}

	/// Whether the snapshot's versions of `user_key` are left out of the history: the
	/// transaction erased the key, or a range it deleted covers it.
	fn snapshot_key_hidden(&self, user_key: &[u8]) -> bool {
		self.hard_delete_keys.contains(user_key)
			|| self
				.pending_ranges
				.iter()
				.any(|(start, end, ..)| user_key >= start.as_slice() && user_key < end.as_slice())
	}

	/// Check if current write-set entry is a tombstone.
	fn ws_is_tombstone(&self) -> bool {
		debug_assert!(self.ws_valid());
		self.write_set_entries[self.ws_pos.unwrap()].1.is_tombstone()
	}

	/// Advance write-set position in the current direction.
	fn advance_ws(&mut self) {
		if let Some(pos) = self.ws_pos {
			self.ws_pos = match self.direction {
				MergeDirection::Forward => {
					if pos + 1 < self.write_set_entries.len() {
						Some(pos + 1)
					} else {
						None
					}
				}
				MergeDirection::Backward => {
					if pos > 0 {
						Some(pos - 1)
					} else {
						None
					}
				}
			};
		}
	}

	/// Seek write-set to first entry >= target (for forward iteration).
	fn seek_ws(&mut self, target: &[u8]) {
		let pos = self.write_set_entries.partition_point(|(k, _)| k.as_slice() < target);
		self.ws_pos = if pos < self.write_set_entries.len() {
			Some(pos)
		} else {
			None
		};
	}

	/// Seek write-set to first entry.
	fn seek_ws_first(&mut self) {
		self.ws_pos = if self.write_set_entries.is_empty() {
			None
		} else {
			Some(0)
		};
	}

	/// Seek write-set to last entry.
	fn seek_ws_last(&mut self) {
		self.ws_pos = if self.write_set_entries.is_empty() {
			None
		} else {
			Some(self.write_set_entries.len() - 1)
		};
	}

	/// Populate the encoded key buffer for the current write-set entry.
	fn populate_ws_encoded_key(&mut self) {
		let Some(pos) = self.ws_pos else {
			return;
		};

		let (key, entry) = self.write_set_entries[pos];

		self.ws_encoded_key_buf.clear();
		self.ws_encoded_key_buf.extend_from_slice(key);
		let trailer = ((entry.seqno as u64) << 8) | (entry.kind as u64);
		self.ws_encoded_key_buf.extend_from_slice(&trailer.to_be_bytes());
	}

	// =========================================================================
	// Core merge logic
	// =========================================================================

	/// Position to the minimum of snapshot and write-set (forward merge).
	///
	/// # Ordering: (user_key ASC, timestamp DESC)
	///
	/// The versions of a key come newest timestamp first, as the snapshot's history returns
	/// them, and a write of the transaction is placed among them by its timestamp, ahead of a
	/// version of the same timestamp: it is the newer write.
	///
	/// ```text
	/// ┌────────────────────────────────────────────────────────┐
	/// │ hist_key vs ws_key                                     │
	/// ├────────────────────────────────────────────────────────┤
	/// │ hist < ws  → return Snapshot  (hist key comes first)   │
	/// │ hist > ws  → return WriteSet  (ws key comes first)     │
	/// │ hist = ws  → compare timestamps (same key)             │
	/// │   ws_ts >= hist_ts → return WriteSet                   │
	/// │   ws_ts <  hist_ts → return Snapshot                   │
	/// └────────────────────────────────────────────────────────┘
	/// ```
	///
	/// A write-set tombstone is a version of its own when tombstones are asked for, and left out
	/// when they are not: it does not hide the versions in the snapshot, which `history` lists.
	fn position_to_min(&mut self) -> Result<bool> {
		loop {
			let hist_valid = self.inner.valid();
			let ws_valid = self.ws_valid();

			// Skip snapshot entries for hard-deleted keys.
			if hist_valid {
				let hist_key = self.inner.key().user_key();
				if self.snapshot_key_hidden(hist_key) {
					self.inner.next()?;
					continue;
				}
			}

			let ws_first = match (hist_valid, ws_valid) {
				(false, false) => {
					self.current_source = CurrentSource::None;
					return Ok(false);
				}
				(true, false) => false,
				(false, true) => true,
				(true, true) => {
					let hist = self.inner.key();
					match self.ws_key().cmp(hist.user_key()) {
						Ordering::Less => true,
						Ordering::Greater => false,
						Ordering::Equal => self.ws_timestamp() >= hist.timestamp(),
					}
				}
			};

			if !ws_first {
				self.current_source = CurrentSource::Snapshot;
				return Ok(true);
			}
			if !self.include_tombstones && self.ws_is_tombstone() {
				self.advance_ws();
				continue;
			}
			self.populate_ws_encoded_key();
			self.current_source = CurrentSource::WriteSet;
			return Ok(true);
		}
	}

	/// Position to the maximum of snapshot and write-set (backward merge).
	///
	/// This is the mirror of `position_to_min`: the versions of a key come oldest timestamp
	/// first, and a write of the transaction follows a version of the same timestamp.
	fn position_to_max(&mut self) -> Result<bool> {
		loop {
			let hist_valid = self.inner.valid();
			let ws_valid = self.ws_valid();

			// Skip snapshot entries for hard-deleted keys
			if hist_valid {
				let hist_key = self.inner.key().user_key();
				if self.snapshot_key_hidden(hist_key) {
					self.inner.prev()?;
					continue;
				}
			}

			let ws_first = match (hist_valid, ws_valid) {
				(false, false) => {
					self.current_source = CurrentSource::None;
					return Ok(false);
				}
				(true, false) => false,
				(false, true) => true,
				(true, true) => {
					let hist = self.inner.key();
					match self.ws_key().cmp(hist.user_key()) {
						Ordering::Greater => true,
						Ordering::Less => false,
						Ordering::Equal => self.ws_timestamp() < hist.timestamp(),
					}
				}
			};

			if !ws_first {
				self.current_source = CurrentSource::Snapshot;
				return Ok(true);
			}
			if !self.include_tombstones && self.ws_is_tombstone() {
				self.advance_ws();
				continue;
			}
			self.populate_ws_encoded_key();
			self.current_source = CurrentSource::WriteSet;
			return Ok(true);
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

		self.entries_returned = 0;
		self.inner.seek_first()?;
		self.seek_ws_first();

		let positioned = self.position_to_min()?;
		Ok(self.spend(positioned))
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

		self.entries_returned = 0;
		self.inner.seek_last()?;
		self.seek_ws_last();

		let positioned = self.position_to_max()?;
		Ok(self.spend(positioned))
	}

	/// Move to the next entry in forward order.
	///
	/// Advances past the current entry and positions at the next one.
	/// Returns `true` if a valid entry exists.
	#[allow(clippy::should_implement_trait)]
	pub fn next(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_first();
		}

		// See the matching note in `TransactionRangeIterator::next`.
		if !self.valid() {
			return Ok(false);
		}

		// Direction change: backward → forward
		if self.direction != MergeDirection::Forward {
			self.direction = MergeDirection::Forward;

			reanchor_other_source(
				&mut self.inner,
				&mut self.ws_pos,
				self.write_set_entries.len(),
				self.current_source,
				MergeDirection::Forward,
			)?;
		}

		// Advance the current source
		match self.current_source {
			CurrentSource::Snapshot => {
				self.inner.next()?;
			}
			CurrentSource::WriteSet => {
				self.advance_ws();
			}
			CurrentSource::None => return Ok(false),
		}

		let positioned = self.position_to_min()?;
		Ok(self.spend(positioned))
	}

	/// Move to the previous entry in backward order.
	///
	/// Advances backward past the current entry and positions at the previous one.
	/// Returns `true` if a valid entry exists.
	pub fn prev(&mut self) -> Result<bool> {
		if !self.initialized {
			return self.seek_last();
		}

		// See the matching note in `TransactionRangeIterator::next`.
		if !self.valid() {
			return Ok(false);
		}

		// Direction change: forward → backward
		if self.direction != MergeDirection::Backward {
			self.direction = MergeDirection::Backward;

			reanchor_other_source(
				&mut self.inner,
				&mut self.ws_pos,
				self.write_set_entries.len(),
				self.current_source,
				MergeDirection::Backward,
			)?;
		}

		// Advance the current source
		match self.current_source {
			CurrentSource::Snapshot => {
				self.inner.prev()?;
			}
			CurrentSource::WriteSet => {
				self.advance_ws();
			}
			CurrentSource::None => return Ok(false),
		}

		let positioned = self.position_to_max()?;
		Ok(self.spend(positioned))
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

		// Encode user key with MAX trailer for >= seek
		let mut encoded = target.to_vec();
		encoded.extend_from_slice(&u64::MAX.to_be_bytes()); // max trailer
		self.entries_returned = 0;
		self.inner.seek(&encoded)?;

		// Position write-set at first entry >= target
		self.seek_ws(target);

		let positioned = self.position_to_min()?;
		Ok(self.spend(positioned))
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
			CurrentSource::WriteSet => InternalKeyRef::from_encoded(&self.ws_encoded_key_buf)
				.with_timestamp(self.ws_timestamp()),
			CurrentSource::None => panic!("key() called on invalid iterator"),
		}
	}

	/// Returns the current raw value as a byte slice.
	///
	/// For snapshot entries, may be a VLog reference requiring resolution.
	/// For write-set entries, returns the direct value bytes.
	fn value_encoded(&self) -> Result<&[u8]> {
		debug_assert!(self.valid());
		match self.current_source {
			CurrentSource::Snapshot => self.inner.value_encoded(),
			CurrentSource::WriteSet => {
				let entry = self.write_set_entries[self.ws_pos.unwrap()].1;
				Ok(entry.value.as_ref().map_or(&[], |v| v.as_slice()))
			}
			CurrentSource::None => panic!("value() called on invalid iterator"),
		}
	}

	fn value(&self) -> Result<Value> {
		debug_assert!(self.valid());
		let raw = self.value_encoded()?;
		if self.current_source == CurrentSource::WriteSet {
			Ok(raw.to_vec())
		} else {
			self.core.resolve_value(raw)
		}
	}
}
