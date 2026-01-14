use std::fs::File as SysFile;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

mod arena;
mod skiplist;

use arena::Arena;
use skiplist::{Compare, Error as SkiplistError, Skiplist, SkiplistIterator};

use crate::batch::Batch;
use crate::error::Result;
use crate::iter::{BoxedInternalIterator, CompactionIterator};
use crate::sstable::table::{Table, TableWriter};
use crate::sstable::{InternalIterator, InternalKey, InternalKeyRef};
use crate::vfs::File;
use crate::{Comparator, Options, Value};

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
	/// WAL number that was current when this memtable started receiving writes.
	/// Used to determine which WALs can be safely deleted after flush.
	wal_number: AtomicU64,
}

impl Default for MemTable {
	fn default() -> Self {
		Self::new(1024 * 1024)
	}
}

impl MemTable {
	pub(crate) fn new(arena_capacity: usize) -> Self {
		let arena = Arc::new(Arena::new(arena_capacity));
		let cmp: Compare = |a, b| a.cmp(b);
		let skiplist = Skiplist::new(arena.clone(), cmp);
		MemTable {
			skiplist,
			latest_seq_num: AtomicU64::new(0),
			wal_number: AtomicU64::new(0),
		}
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

	pub(crate) fn get(&self, key: &[u8], seq_no: Option<u64>) -> Option<(InternalKey, Value)> {
		use crate::sstable::INTERNAL_KEY_SEQ_NUM_MAX;

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
			let found_seq = found_trailer >> 8;

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

	/// Adds a batch of operations to the memtable.
	/// This includes appending the batch to the Write-Ahead Log (WAL),
	/// applying the batch to the in-memory table, and updating the memtable
	/// size and latest sequence number.
	///
	/// # Arguments
	/// * `batch` - The batch of operations to apply
	/// * `starting_seq_num` - The starting sequence number for this batch (records get consecutive
	///   numbers)
	pub(crate) fn add(&self, batch: &Batch) -> Result<()> {
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

		// Process entries with pre-encoded ValueLocations
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
	/// Returns Err(ArenaFull) if there's not enough space.
	fn insert_into_memtable(&self, key: &InternalKey, value: &Value) -> Result<()> {
		let trailer = (key.seq_num() << 8) | (key.kind() as u64);

		match self.skiplist.add(&key.user_key, trailer, key.timestamp, value) {
			Ok(()) => Ok(()),
			Err(SkiplistError::RecordExists) => Ok(()), // Duplicate is not an error in memtable
			Err(SkiplistError::ArenaFull) => Err(crate::Error::ArenaFull),
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
			let mut table_writer = TableWriter::new(file, table_id, Arc::clone(&lsm_opts), 0); // Memtables always flush to L0

			let iter = self.iter();
			let iter: BoxedInternalIterator<'_> = Box::new(iter);
			let mut comp_iter = CompactionIterator::new(
				vec![iter],
				Arc::clone(&lsm_opts.internal_comparator) as Arc<dyn Comparator>,
				false,                       // not bottom level (L0 flush)
				None,                        // no vlog access in flush context
				false,                       // versioning disabled in flush context
				0,                           // retention period is 0 in flush context
				Arc::clone(&lsm_opts.clock), // clock is the system clock
			);
			for item in comp_iter.by_ref() {
				let (key, encoded_val) = item?;
				// The memtable already contains the correct ValueLocation encoding
				// (either inline or with VLog pointer), so we can use it directly
				table_writer.add(key, &encoded_val)?;
			}
			// TODO: Check how to fsync this file
			table_writer.finish()?;
		}

		let file = SysFile::open(&table_file_path)?;
		file.sync_all()?;
		let file: Arc<dyn File> = Arc::new(file);
		let file_size = file.size()?;

		let created_table = Arc::new(Table::new(table_id, lsm_opts, file, file_size)?);
		Ok(created_table)
	}

	pub(crate) fn iter(&self) -> MemTableIterator<'_> {
		self.range(None, None)
	}

	/// Returns an iterator over keys in [lower, upper)
	/// Lower is inclusive, upper is exclusive (Pebble model)
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

/// Thin wrapper around SkiplistIterator for keys_only optimization
pub(crate) struct MemTableIterator<'a> {
	iter: SkiplistIterator<'a>,
}

impl InternalIterator for MemTableIterator<'_> {
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

	fn value(&self) -> &[u8] {
		self.iter.value()
	}
}
