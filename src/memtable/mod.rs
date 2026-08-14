use std::fs::File as SysFile;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

mod arena;
mod skiplist;

use arena::Arena;
pub(crate) use skiplist::max_entry_bytes;
use skiplist::{Compare, Error as SkiplistError, Skiplist, SkiplistIterator};

use crate::batch::{Batch, BatchOwner};
use crate::error::{Error, Result};
use crate::sstable::table::{Table, TableWriter};
use crate::vfs::File;
use crate::{InternalKey, InternalKeyRef, LSMIterator, Options, Value, INTERNAL_KEY_SEQ_NUM_MAX};

/// Entry in the immutable memtables list, tracking both the table ID
/// and the WAL number that contains this memtable's data.
#[derive(Clone)]
pub(crate) struct ImmutableEntry {
	/// The table ID that will be used for the SST file
	pub table_id: u64,
	/// The WAL number that was current when this memtable was active.
	/// Used to determine which WALs can be safely deleted after flush.
	pub wal_number: u64,
	/// The memtable data
	pub memtable: Arc<MemTable>,
}

#[derive(Default)]
pub(crate) struct ImmutableMemtables(Vec<ImmutableEntry>);

impl ImmutableMemtables {
	/// Adds an immutable memtable entry with its associated table ID and WAL
	/// number.
	pub(crate) fn add(&mut self, table_id: u64, wal_number: u64, memtable: Arc<MemTable>) {
		self.0.push(ImmutableEntry {
			table_id,
			wal_number,
			memtable,
		});
		self.0.sort_by_key(|entry| entry.table_id); // Maintain sorted order by ID
	}

	pub(crate) fn remove(&mut self, id_to_remove: u64) {
		if let Ok(index) = self.0.binary_search_by_key(&id_to_remove, |entry| entry.table_id) {
			self.0.remove(index);
		}
	}

	pub(crate) fn iter(&self) -> impl DoubleEndedIterator<Item = &ImmutableEntry> {
		self.0.iter()
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	/// Returns the oldest (first) immutable memtable entry.
	/// Entries are sorted by table_id, so the first entry is the oldest.
	pub(crate) fn first(&self) -> Option<&ImmutableEntry> {
		self.0.first()
	}

	#[cfg(test)]
	pub(crate) fn replay_floor_excluding(&self, table_id: u64, active_wal: u64) -> u64 {
		self.0
			.iter()
			.filter(|entry| entry.table_id != table_id)
			.map(|entry| entry.wal_number)
			.fold(active_wal, u64::min)
	}
}

pub(crate) struct MemTable {
	dependency_id: crate::wal::dependency::ComponentId,
	owner: BatchOwner,
	skiplist: Skiplist,
	latest_seq_num: AtomicU64,
	/// WAL number that was current when this memtable started receiving writes.
	/// Used to determine which WALs can be safely deleted after flush.
	wal_number: AtomicU64,
	/// Bytes reserved by in-flight `add` calls but not yet allocated in the
	/// skiplist arena. Atomically updated by `try_reserve` / `release_reservation`
	/// to ensure batch-atomic insertion: a batch either fits entirely (reservation
	/// succeeds) or the memtable is left unchanged (reservation fails with ArenaFull).
	reserved: AtomicU64,
}

impl Default for MemTable {
	fn default() -> Self {
		Self::new(1024 * 1024)
	}
}

/// Releases a `MemTable` reservation on drop. Used by `MemTable::add` to ensure
/// the reservation is freed even if `apply_batch_to_memtable` returns an error
/// (which should not happen after a successful `try_reserve`, but the guard
/// keeps the contract panic-safe and `?`-safe).
struct ReservationGuard<'a> {
	memtable: &'a MemTable,
	bytes: u64,
}

impl Drop for ReservationGuard<'_> {
	fn drop(&mut self) {
		self.memtable.release_reservation(self.bytes);
	}
}

impl MemTable {
	/// Initial `wal_number`: "no WAL dependency recorded yet". `u64::MAX` is
	/// the identity element of `record_wal_dependency`'s `fetch_min`, so the
	/// first applied batch's actual segment becomes the baseline; a zero
	/// initialization could never be raised and would pin every
	/// lazily-created memtable to segment 0.
	pub(crate) const NO_WAL_DEPENDENCY: u64 = u64::MAX;

	pub(crate) fn new(arena_capacity: usize) -> Self {
		Self::new_owned(arena_capacity, BatchOwner::DEFAULT)
	}

	pub(crate) fn new_owned(arena_capacity: usize, owner: BatchOwner) -> Self {
		// Dependency ids are opaque; starting at 1 only keeps 0 out of logs
		// and dumps — no code anywhere treats 0 as a sentinel.
		static NEXT_DEPENDENCY_ID: AtomicU64 = AtomicU64::new(1);
		let arena = Arc::new(Arena::new(arena_capacity));
		let cmp: Compare = |a, b| a.cmp(b);
		let skiplist = Skiplist::new(arena, cmp);
		MemTable {
			dependency_id: NEXT_DEPENDENCY_ID.fetch_add(1, Ordering::Relaxed),
			owner,
			skiplist,
			latest_seq_num: AtomicU64::new(0),
			wal_number: AtomicU64::new(Self::NO_WAL_DEPENDENCY),
			reserved: AtomicU64::new(0),
		}
	}

	pub(crate) fn owner(&self) -> BatchOwner {
		self.owner
	}

	pub(crate) fn dependency_id(&self) -> crate::wal::dependency::ComponentId {
		self.dependency_id
	}

	/// Sets the WAL number associated with this memtable.
	/// This should be called when the memtable starts receiving writes
	/// to track which WAL contains its data.
	pub(crate) fn set_wal_number(&self, wal_number: u64) {
		self.wal_number.store(wal_number, Ordering::Release);
	}

	/// Records the earliest actual WAL segment containing an entry applied to
	/// this memtable. This may move backwards when a delayed apply lands after
	/// a concurrent WAL/memtable rotation.
	pub(crate) fn record_wal_dependency(&self, wal_number: u64) {
		self.wal_number.fetch_min(wal_number, Ordering::AcqRel);
	}

	/// Gets the earliest WAL segment this memtable depends on.
	///
	/// A fresh memtable starts at [`Self::NO_WAL_DEPENDENCY`]; see its doc.
	/// Every non-empty memtable has a real value (apply records its actual
	/// append segment); empty actives are explicitly baselined by
	/// open/rotation.
	pub(crate) fn get_wal_number(&self) -> u64 {
		self.wal_number.load(Ordering::Acquire)
	}

	pub(crate) fn get(&self, key: &[u8], seq_no: Option<u64>) -> Option<(InternalKey, Value)> {
		let max_seq = seq_no.unwrap_or(INTERNAL_KEY_SEQ_NUM_MAX);
		let mut iter = self.skiplist.iter();
		iter.seek_ge(key);

		// Find the entry with highest sequence number <= max_seq
		while iter.is_valid() {
			let found_key = iter.key_bytes();
			if found_key != key {
				break; // Moved past our key
			}

			let found_trailer = iter.trailer();
			let found_seq = crate::trailer_to_seq_num(found_trailer);

			// Check if this entry's sequence number is <= requested seq_no
			if found_seq <= max_seq {
				// This is the newest version with seq <= max_seq
				let internal_key = InternalKey {
					user_key: found_key.to_vec(),
					timestamp: 0,
					trailer: found_trailer,
				};
				return Some((internal_key, iter.value_bytes().to_vec()));
			}

			iter.advance();
		}
		None
	}

	pub(crate) fn is_empty(&self) -> bool {
		let mut iter = self.skiplist.iter();
		iter.first();
		!iter.is_valid()
	}

	pub(crate) fn size(&self) -> usize {
		self.skiplist.size() as usize
	}

	/// Arena capacity in bytes (total, including sentinel overhead).
	pub(crate) fn arena_capacity(&self) -> usize {
		self.skiplist.arena_capacity()
	}

	/// Arena bytes still reservable: capacity minus skiplist usage (which
	/// includes the empty skiplist's own head/tail sentinel allocations)
	/// minus outstanding reservations. `try_reserve(n)` succeeds iff
	/// `n <= arena_available()` at the moment of the CAS. Right-sizing a
	/// replacement arena must use this, not raw capacity: an arena sized
	/// exactly to a batch estimate can never admit that batch.
	pub(crate) fn arena_available(&self) -> u64 {
		(self.arena_capacity() as u64)
			.saturating_sub(self.skiplist.size() as u64)
			.saturating_sub(self.reserved.load(Ordering::Acquire))
	}

	/// Builds an owned memtable guaranteed to admit a reservation of
	/// `min_reservable` bytes: starts at `max(base_capacity, min_reservable)`
	/// and grows once by the measured sentinel shortfall. This is the single
	/// right-sizing rule shared by forced rotation and WAL replay — a
	/// WAL-durable batch must never permanently fail to apply because the
	/// fresh arena's own sentinel nodes consumed its headroom.
	pub(crate) fn new_owned_admitting(
		base_capacity: usize,
		min_reservable: u64,
		owner: crate::batch::BatchOwner,
	) -> Self {
		let capacity = base_capacity.max(min_reservable as usize);
		let table = Self::new_owned(capacity, owner);
		let shortfall = min_reservable.saturating_sub(table.arena_available());
		if shortfall == 0 {
			return table;
		}
		let grown = Self::new_owned(capacity + shortfall as usize, owner);
		debug_assert!(
			grown.arena_available() >= min_reservable,
			"right-sized arena cannot admit the forcing reservation"
		);
		grown
	}

	/// Atomically reserve `bytes` of arena space for an upcoming batch insertion.
	///
	/// On `Ok(())`, the caller has exclusive claim to `bytes` of arena space and
	/// MUST eventually call `release_reservation(bytes)`. On `Err(ArenaFull)`,
	/// the memtable state is unchanged and the caller should rotate to a fresh
	/// memtable and retry.
	///
	/// This is a CAS loop; spurious failures retry until either the reservation
	/// succeeds or `ArenaFull` is observed against the freshest `reserved` value.
	pub(crate) fn try_reserve(&self, bytes: u64) -> Result<()> {
		let capacity = self.arena_capacity() as u64;
		loop {
			let current = self.reserved.load(Ordering::Acquire);
			let used = self.skiplist.size() as u64;
			let avail = capacity.saturating_sub(used).saturating_sub(current);
			if bytes > avail {
				return Err(crate::Error::ArenaFull);
			}
			if self
				.reserved
				.compare_exchange_weak(
					current,
					current + bytes,
					Ordering::AcqRel,
					Ordering::Acquire,
				)
				.is_ok()
			{
				return Ok(());
			}
		}
	}

	/// Release `bytes` previously claimed by `try_reserve`.
	pub(crate) fn release_reservation(&self, bytes: u64) {
		self.reserved.fetch_sub(bytes, Ordering::AcqRel);
	}

	/// Applies a batch of operations to the memtable **atomically**.
	///
	/// Returns `Err(ArenaFull)` if the batch will not fit; in that case the
	/// memtable state is unchanged and the caller may rotate and retry on a
	/// fresh memtable. On `Ok(())` every entry has been inserted.
	///
	/// The atomicity is provided by an upfront `try_reserve` call using a
	/// worst-case upper bound (`Batch::memtable_size_estimate`); after the
	/// reservation succeeds, the per-entry inserts cannot run out of space.
	///
	/// # Arguments
	/// * `batch` - The batch of operations to apply
	pub(crate) fn add(&self, batch: &Batch) -> Result<()> {
		if batch.owner != self.owner {
			return Err(Error::InvalidArgument(
				"batch owner does not match branch-pure memtable owner".to_owned(),
			));
		}
		let needed = batch.memtable_size_estimate();
		self.try_reserve(needed)?;
		let _guard = ReservationGuard {
			memtable: self,
			bytes: needed,
		};
		let highest_seq_num = self.apply_batch_to_memtable(batch)?;
		self.update_latest_sequence_number(highest_seq_num);
		Ok(())
	}

	/// Applies the batch of operations to the in-memory table (memtable).
	/// Returns (total_record_size, highest_seq_num_used).
	fn apply_batch_to_memtable(&self, batch: &Batch) -> Result<u64> {
		// Pre-allocate empty value Bytes for delete operations to avoid repeated
		// allocations
		let empty_val = Value::new();

		// Values are stored inline in their canonical byte representation.
		for (_i, entry, current_seq_num, timestamp) in batch.entries_with_seq_nums()? {
			let ikey = InternalKey::new(entry.key.clone(), current_seq_num, entry.kind, timestamp);

			// Use the value directly (cheap Bytes clone), or reuse empty value for deletes
			let val = if let Some(encoded_value) = &entry.value {
				encoded_value.clone()
			} else {
				// For delete operations, reuse the pre-allocated empty value
				empty_val.clone()
			};

			self.insert_into_memtable(&ikey, &val)?;
		}

		// Get the highest sequence number used from the batch
		let highest_seq_num = batch.get_highest_seq_num();

		Ok(highest_seq_num)
	}

	/// Inserts a key-value pair into the memtable.
	///
	/// `MemTable::add` reserves arena space via `try_reserve` before calling this,
	/// so `SkiplistError::ArenaFull` here would indicate the size estimator drifted
	/// from the actual skiplist node size. We assert in debug, and fall through to
	/// returning `ArenaFull` in release so the upstream rotate-and-retry path stays
	/// as a safety net rather than corrupting state via panic.
	fn insert_into_memtable(&self, key: &InternalKey, value: &Value) -> Result<()> {
		let trailer = crate::make_trailer(key.seq_num(), key.kind());

		match self.skiplist.add(&key.user_key, trailer, key.timestamp, value) {
			Ok(()) => Ok(()),
			Err(SkiplistError::RecordExists) => Ok(()), // Duplicate is not an error in memtable
			Err(SkiplistError::ArenaFull) => {
				debug_assert!(
					false,
					"ArenaFull inside insert_into_memtable after a successful try_reserve; \
					memtable_size_estimate is out of sync with skiplist node size"
				);
				log::error!("ArenaFull after reservation; memtable size estimator drift");
				Err(crate::Error::ArenaFull)
			}
		}
	}

	/// Updates the latest sequence number in the memtable.
	/// This ensures that the memtable always has the highest sequence number of
	/// the operations it contains.
	fn update_latest_sequence_number(&self, current_seq_num: u64) {
		let mut prev_seq_num = self.latest_seq_num.load(Ordering::Acquire);
		while current_seq_num > prev_seq_num {
			match self.latest_seq_num.compare_exchange_weak(
				prev_seq_num,
				current_seq_num,
				Ordering::AcqRel,
				Ordering::Acquire,
			) {
				Ok(_) => break,
				Err(x) => prev_seq_num = x,
			}
		}
	}

	#[allow(unused)]
	pub(crate) fn lsn(&self) -> u64 {
		self.latest_seq_num.load(Ordering::Acquire)
	}

	pub(crate) fn flush(&self, table_id: u64, lsm_opts: Arc<Options>) -> Result<Arc<Table>> {
		let table_file_path = lsm_opts.sstable_file_path(table_id);

		{
			let file = SysFile::create(&table_file_path)?;
			let mut table_writer =
				TableWriter::new_owned(file, table_id, Arc::clone(&lsm_opts), 0, self.owner); // Memtables always flush to L0

			let mut iter = self.iter();
			iter.seek_first()?;
			while iter.valid() {
				let key = iter.key().to_owned();
				table_writer.add(key, iter.value_encoded()?)?;
				iter.next()?;
			}
			table_writer.finish()?;
		}

		// Durability fix: the SST's data and its directory entry must be
		// durable before the manifest references this table.
		crate::vfs::fsync_file(&table_file_path)?;
		crate::lsm::fsync_directory(lsm_opts.sstable_dir())?;
		let file: Arc<dyn File> = Arc::new(SysFile::open(&table_file_path)?);
		let file_size = file.size()?;

		let created_table = Arc::new(Table::new(table_id, lsm_opts, file, file_size)?);
		Ok(created_table)
	}

	pub(crate) fn iter(&self) -> MemTableIterator<'_> {
		self.range(None, None)
	}

	/// Returns an iterator over keys in [lower, upper)
	/// Lower is inclusive, upper is exclusive
	pub(crate) fn range(
		&self,
		lower: Option<&[u8]>, // Inclusive, None = unbounded
		upper: Option<&[u8]>, // Exclusive, None = unbounded
	) -> MemTableIterator<'_> {
		let mut iter = self.skiplist.new_iter(lower, upper);

		// Pre-position for forward iteration
		if let Some(lower_key) = lower {
			iter.seek_ge(lower_key);
		} else {
			iter.first();
		}

		MemTableIterator {
			iter,
		}
	}
}

pub(crate) struct MemTableIterator<'a> {
	iter: SkiplistIterator<'a>,
}

impl LSMIterator for MemTableIterator<'_> {
	fn seek(&mut self, target: &[u8]) -> Result<bool> {
		self.iter.seek(target)
	}

	fn seek_first(&mut self) -> Result<bool> {
		self.iter.seek_first()
	}

	fn seek_last(&mut self) -> Result<bool> {
		self.iter.seek_last()
	}

	fn next(&mut self) -> Result<bool> {
		self.iter.next()
	}

	fn prev(&mut self) -> Result<bool> {
		self.iter.prev()
	}

	fn valid(&self) -> bool {
		self.iter.valid()
	}

	fn key(&self) -> InternalKeyRef<'_> {
		self.iter.key()
	}

	fn value_encoded(&self) -> Result<&[u8]> {
		self.iter.value_encoded()
	}
}
