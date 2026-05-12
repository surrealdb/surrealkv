use std::fs::File as SysFile;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

mod arena;
mod skiplist;

use arena::Arena;
use skiplist::{Compare, Error as SkiplistError, Skiplist, SkiplistIterator};

use crate::batch::{Batch, PreparedBatch};
use crate::error::Result;
use crate::sstable::table::{Table, TableWriter};
use crate::vfs::File;
use crate::vlog::{VLog, ValueLocation};
use crate::{InternalKey, InternalKeyRef, LSMIterator, Options, Value, INTERNAL_KEY_SEQ_NUM_MAX};

/// Encoded bplustree entries: Vec of (encoded_key, encoded_value) pairs.
pub(crate) type BPTreeEntries = Vec<(Vec<u8>, Vec<u8>)>;

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
}

pub(crate) struct MemTable {
	skiplist: Skiplist,
	latest_seq_num: AtomicU64,
	/// Sequence number when this memtable was created.
	earliest_seq: u64,
	/// WAL number that was current when this memtable started receiving writes.
	/// Used to determine which WALs can be safely deleted after flush.
	wal_number: AtomicU64,
}

impl Default for MemTable {
	fn default() -> Self {
		Self::new(1024 * 1024, 0)
	}
}

impl MemTable {
	pub(crate) fn new(arena_capacity: usize, earliest_seq: u64) -> Self {
		let arena = Arc::new(Arena::new(arena_capacity));
		let cmp: Compare = |a, b| a.cmp(b);
		let skiplist = Skiplist::new(arena, cmp);
		MemTable {
			skiplist,
			latest_seq_num: AtomicU64::new(0),
			earliest_seq,
			wal_number: AtomicU64::new(0),
		}
	}

	pub(crate) fn earliest_seq(&self) -> u64 {
		self.earliest_seq
	}

	/// Sets the WAL number associated with this memtable.
	/// This should be called when the memtable starts receiving writes
	/// to track which WAL contains its data.
	pub(crate) fn set_wal_number(&self, wal_number: u64) {
		self.wal_number.store(wal_number, Ordering::Release);
	}

	/// Gets the WAL number associated with this memtable.
	/// Returns 0 if the WAL number has not been set.
	pub(crate) fn get_wal_number(&self) -> u64 {
		self.wal_number.load(Ordering::Acquire)
	}

	/// Looks up the newest version of `key` visible at `seq_no` (or any
	/// seq if `seq_no` is `None`). Returns the trailer (which encodes both
	/// seq_num and kind via `trailer_to_seq_num` / `trailer_to_kind`) and
	/// the value bytes. The user_key is not returned — callers already
	/// have it.
	pub(crate) fn get(&self, key: &[u8], seq_no: Option<u64>) -> Option<(u64, Value)> {
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
				return Some((found_trailer, iter.value_bytes().to_vec()));
			}

			iter.advance();
		}
		None
	}

	/// Returns the seq_num of the newest version of `key`, regardless of
	/// kind (tombstone or not). Skips the value-clone, so it's cheaper
	/// than `get(key, None)` when the caller only needs to detect a
	/// write conflict.
	pub(crate) fn max_seq_for_key(&self, key: &[u8]) -> Option<u64> {
		let mut iter = self.skiplist.iter();
		iter.seek_ge(key);
		if iter.is_valid() && iter.key_bytes() == key {
			Some(crate::trailer_to_seq_num(iter.trailer()))
		} else {
			None
		}
	}

	pub(crate) fn is_empty(&self) -> bool {
		let mut iter = self.skiplist.iter();
		iter.first();
		!iter.is_valid()
	}

	pub(crate) fn size(&self) -> usize {
		self.skiplist.size() as usize
	}

	/// Recovery / replay path: apply a decoded WAL batch to the memtable.
	/// Reads each entry's already-encoded value bytes from `batch.entries[i].value`
	/// (which holds the ValueLocation-encoded form, as written to the WAL).
	pub(crate) fn add(&self, batch: &Batch) -> Result<()> {
		let highest_seq_num = self.apply_batch_to_memtable(batch)?;
		self.update_latest_sequence_number(highest_seq_num);
		Ok(())
	}

	/// Commit-pipeline path: apply a batch whose encoded values live as
	/// slices of `prepared.wal_bytes`. Avoids the per-entry value clone
	/// that `add` would do (since `add`'s `entry.value` is a `Vec<u8>`).
	pub(crate) fn add_prepared(&self, batch: &Batch, prepared: &PreparedBatch) -> Result<()> {
		for (i, entry, current_seq_num, timestamp) in batch.entries_with_seq_nums()? {
			let trailer = crate::make_trailer(current_seq_num, entry.kind);
			let value = prepared.value_bytes(i);
			match self.skiplist.add(&entry.key, trailer, timestamp, value) {
				Ok(()) => {}
				Err(SkiplistError::RecordExists) => {} // duplicate is not an error
				Err(SkiplistError::ArenaFull) => return Err(crate::Error::ArenaFull),
			}
		}
		self.update_latest_sequence_number(batch.get_highest_seq_num());
		Ok(())
	}

	/// Applies the entries of a decoded batch (e.g. from WAL replay) to
	/// the memtable. Each entry's `value` is the already-encoded
	/// ValueLocation form (meta + version + raw); the skiplist copies
	/// the bytes into its arena. No intermediate clones.
	fn apply_batch_to_memtable(&self, batch: &Batch) -> Result<u64> {
		for (_i, entry, current_seq_num, timestamp) in batch.entries_with_seq_nums()? {
			let trailer = crate::make_trailer(current_seq_num, entry.kind);
			let value: &[u8] = entry.value.as_deref().unwrap_or(&[]);
			match self.skiplist.add(&entry.key, trailer, timestamp, value) {
				Ok(()) => {}
				Err(SkiplistError::RecordExists) => {}
				Err(SkiplistError::ArenaFull) => return Err(crate::Error::ArenaFull),
			}
		}

		Ok(batch.get_highest_seq_num())
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

	pub(crate) fn flush(
		&self,
		table_id: u64,
		lsm_opts: Arc<Options>,
		vlog: Option<&Arc<VLog>>,
		vlog_threshold: usize,
		collect_bptree_entries: bool,
	) -> Result<(Arc<Table>, BPTreeEntries)> {
		let table_file_path = lsm_opts.sstable_file_path(table_id);
		let mut bptree_entries = Vec::new();

		{
			let file = SysFile::create(&table_file_path)?;
			let mut table_writer = TableWriter::new(file, table_id, Arc::clone(&lsm_opts), 0); // Memtables always flush to L0

			let mut iter = self.iter();
			iter.seek_first()?;
			while iter.valid() {
				let key = iter.key().to_owned();
				let raw_encoded = iter.value_encoded()?;

				// Separate large values to VLog during flush
				let sst_value = maybe_separate_to_vlog(raw_encoded, &key, vlog, vlog_threshold)?;

				if collect_bptree_entries {
					bptree_entries.push((key.encode(), sst_value.clone()));
				}

				table_writer.add(key, &sst_value)?;
				iter.next()?;
			}
			table_writer.finish()?;
		}

		// Sync VLog after all entries written (one fsync for the entire flush)
		if let Some(vlog) = vlog {
			vlog.sync()?;
		}

		let file = crate::vfs::open_for_sync(&table_file_path)?;
		file.sync_all()?;
		let file: Arc<dyn File> = Arc::new(file);
		let file_size = file.size()?;

		let created_table = Arc::new(Table::new(table_id, lsm_opts, file, file_size)?);
		Ok((created_table, bptree_entries))
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

/// During flush, decide whether to separate a value to VLog or keep inline in SST.
/// Handles backward compat: entries already containing VLog pointers pass through.
fn maybe_separate_to_vlog(
	encoded_value: &[u8],
	key: &InternalKey,
	vlog: Option<&Arc<VLog>>,
	vlog_threshold: usize,
) -> Result<Vec<u8>> {
	if encoded_value.is_empty() {
		return Ok(encoded_value.to_vec());
	}

	let location = ValueLocation::decode(encoded_value)?;

	// Already a VLog pointer (e.g. from pre-upgrade WAL recovery) — pass through
	if location.is_value_pointer() {
		return Ok(encoded_value.to_vec());
	}

	// Separate large values to VLog
	let value = &location.value;
	if let Some(vlog) = vlog {
		if value.len() > vlog_threshold {
			let encoded_key = key.encode();
			let pointer = vlog.append(&encoded_key, value)?;
			return Ok(ValueLocation::with_pointer(pointer).encode());
		}
	}

	// Keep inline
	Ok(encoded_value.to_vec())
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
