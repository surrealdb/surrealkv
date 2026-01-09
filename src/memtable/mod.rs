use std::fs::File as SysFile;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

mod arena;
mod skiplist;

use arena::Arena;
use skiplist::{
	encode_entry,
	encoded_entry_size,
	MemTableKeyComparator,
	SkipList,
	SkipListIterator,
};

use crate::batch::Batch;
use crate::comparator::InternalKeyComparator;
use crate::error::Result;
use crate::iter::CompactionIterator;
use crate::sstable::table::{Table, TableWriter};
use crate::sstable::{InternalKey, InternalKeyKind, INTERNAL_KEY_SEQ_NUM_MAX};
use crate::vfs::File;
use crate::{InternalKeyRange, Options, Value};

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

	pub(crate) fn len(&self) -> usize {
		self.0.len()
	}

	/// Returns the oldest (first) immutable memtable entry.
	/// Entries are sorted by table_id, so the first entry is the oldest.
	pub(crate) fn first(&self) -> Option<&ImmutableEntry> {
		self.0.first()
	}
}

pub(crate) struct MemTable {
	arena: Arc<Arena>,
	skiplist: SkipList<MemTableKeyComparator>,
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
		let comparator =
			Arc::new(InternalKeyComparator::new(Arc::new(crate::BytewiseComparator {})));
		let arena = Arc::new(Arena::new(arena_capacity));
		let mem_cmp = MemTableKeyComparator::new(Arc::clone(&comparator));
		let skiplist = SkipList::new(mem_cmp, arena.clone(), 12, 4);
		MemTable {
			arena,
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
		let seq_no = seq_no.unwrap_or(INTERNAL_KEY_SEQ_NUM_MAX);
		// Create search key: user_key + trailer + timestamp
		let search_key = InternalKey::new(key.to_vec(), seq_no, InternalKeyKind::Set, 0);
		let search_key_bytes = search_key.encode();

		// Pass internal key directly - seek() will add the length prefix internally
		let mut iter = self.skiplist.iter();
		iter.seek(&search_key_bytes);

		if iter.valid() {
			let found_key_bytes = iter.key();
			let found_key = InternalKey::decode(found_key_bytes);
			if found_key.user_key == key {
				let value_bytes = iter.value();
				return Some((found_key, value_bytes.to_vec()));
			}
		}
		None
	}

	pub(crate) fn is_empty(&self) -> bool {
		let mut iter = self.skiplist.iter();
		iter.seek_to_first();
		!iter.valid()
	}

	pub(crate) fn size(&self) -> usize {
		self.arena.size()
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
		// Calculate entry size
		let entry_size = encoded_entry_size(key.user_key.len(), value.len());

		// Allocate space for entry
		let key_ptr = self.skiplist.allocate_key(entry_size).ok_or(crate::Error::ArenaFull)?;

		// Encode entry into allocated space
		unsafe {
			encode_entry(
				std::slice::from_raw_parts_mut(key_ptr, entry_size),
				&key.user_key,
				key.seq_num(),
				key.kind(),
				key.timestamp,
				value,
			);
		}

		// Insert into skiplist
		self.skiplist.insert(key_ptr);
		Ok(())
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

			let iter = self.iter(false);
			let iter = Box::new(iter);
			let mut comp_iter = CompactionIterator::new(
				vec![iter],
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

	pub(crate) fn iter(&self, keys_only: bool) -> MemTableRangeInternalIterator<'_> {
		self.range((Bound::Unbounded, Bound::Unbounded), keys_only)
	}

	pub(crate) fn range(
		&self,
		range: InternalKeyRange,
		keys_only: bool,
	) -> MemTableRangeInternalIterator<'_> {
		MemTableRangeInternalIterator::new(&self.skiplist, range, keys_only)
	}
}

/// Adapter that wraps a SkipListIterator and implements Iterator/DoubleEndedIterator
/// This allows memtable range iterators to be used in merge iterators
pub(crate) struct MemTableRangeInternalIterator<'a> {
	start_bound: Bound<InternalKey>,
	end_bound: Bound<InternalKey>,
	keys_only: bool,
	iter: SkipListIterator<'a, MemTableKeyComparator>,
	empty_value_buffer: Vec<u8>,
	exhausted: bool,
	backward_initialized: bool,
}

impl<'a> MemTableRangeInternalIterator<'a> {
	pub(crate) fn new(
		skiplist: &'a SkipList<MemTableKeyComparator>,
		range: InternalKeyRange,
		keys_only: bool,
	) -> Self {
		let (start_bound, end_bound) = range;
		let mut iter = skiplist.iter();

		// Seek to the start of the range
		match &start_bound {
			Bound::Included(ref key) => {
				let encoded = key.encode();
				iter.seek(&encoded);
			}
			Bound::Excluded(ref key) => {
				let encoded = key.encode();
				iter.seek(&encoded);
				// Skip past the excluded start key if we landed on it
				// Extract user key without allocating
				if iter.valid() {
					let found_user_key = InternalKey::user_key_from_encoded(iter.key());
					if found_user_key == key.user_key.as_slice() {
						iter.next();
					}
				}
			}
			Bound::Unbounded => {
				iter.seek_to_first();
			}
		}

		let exhausted = !iter.valid() || !Self::check_end_bound_static(&iter, &end_bound);

		Self {
			start_bound,
			end_bound,
			keys_only,
			iter,
			empty_value_buffer: Vec::new(),
			exhausted,
			backward_initialized: false,
		}
	}

	fn check_end_bound_static(
		iter: &SkipListIterator<'a, MemTableKeyComparator>,
		end_bound: &Bound<InternalKey>,
	) -> bool {
		if !iter.valid() {
			return false;
		}

		match end_bound {
			Bound::Included(ref end_key) => {
				// Extract user key without allocating
				let current_user_key = InternalKey::user_key_from_encoded(iter.key());
				current_user_key <= end_key.user_key.as_slice()
			}
			Bound::Excluded(ref end_key) => {
				// Extract user key without allocating
				let current_user_key = InternalKey::user_key_from_encoded(iter.key());
				current_user_key < end_key.user_key.as_slice()
			}
			Bound::Unbounded => true,
		}
	}

	fn check_end_bound(&self) -> bool {
		Self::check_end_bound_static(&self.iter, &self.end_bound)
	}

	fn valid(&self) -> bool {
		!self.exhausted && self.iter.valid()
	}

	fn next_internal(&mut self) -> Result<bool> {
		if self.exhausted || !self.iter.valid() {
			self.exhausted = true;
			return Ok(false);
		}

		self.iter.next();

		if !self.iter.valid() || !self.check_end_bound() {
			self.exhausted = true;
			return Ok(false);
		}

		Ok(true)
	}

	fn prev_internal(&mut self) -> Result<bool> {
		if self.exhausted {
			return Ok(false);
		}

		self.iter.prev();

		if !self.iter.valid() {
			self.exhausted = true;
			return Ok(false);
		}

		// When going backwards, check if we've gone before the start bound
		// Extract user key without allocating
		let current_user_key = InternalKey::user_key_from_encoded(self.iter.key());

		match &self.start_bound {
			Bound::Included(ref start_key) => {
				if current_user_key < start_key.user_key.as_slice() {
					self.exhausted = true;
					return Ok(false);
				}
			}
			Bound::Excluded(ref start_key) => {
				if current_user_key <= start_key.user_key.as_slice() {
					self.exhausted = true;
					return Ok(false);
				}
			}
			Bound::Unbounded => {}
		}

		Ok(true)
	}

	#[allow(unused)]
	pub(crate) fn seek_to_first(&mut self) -> Result<()> {
		// Re-seek to the start of the range
		match &self.start_bound {
			Bound::Included(ref key) => {
				let encoded = key.encode();
				self.iter.seek(&encoded);
			}
			Bound::Excluded(ref key) => {
				let encoded = key.encode();
				self.iter.seek(&encoded);
				// Skip past the excluded start key if we landed on it
				// Extract user key without allocating
				if self.iter.valid() {
					let found_user_key = InternalKey::user_key_from_encoded(self.iter.key());
					if found_user_key == key.user_key.as_slice() {
						self.iter.next();
					}
				}
			}
			Bound::Unbounded => {
				self.iter.seek_to_first();
			}
		}

		self.exhausted = !self.iter.valid() || !self.check_end_bound();
		self.backward_initialized = false;
		Ok(())
	}

	pub(crate) fn seek_to_last(&mut self) -> Result<()> {
		// Seek to the end of the range
		match &self.end_bound {
			Bound::Included(ref key) => {
				let encoded = key.encode();
				self.iter.seek(&encoded);
				// If we found the exact key or went past it, stay there or go back
				// Extract user key without allocating
				if self.iter.valid() {
					let found_user_key = InternalKey::user_key_from_encoded(self.iter.key());
					if found_user_key > key.user_key.as_slice() {
						// We went past, go back
						self.iter.prev();
					}
				} else {
					// Went past the end, go to last entry
					self.iter.seek_to_last();
				}
			}
			Bound::Excluded(ref key) => {
				let encoded = key.encode();
				self.iter.seek(&encoded);
				// Go to the entry just before this key
				if self.iter.valid() {
					self.iter.prev();
				} else {
					self.iter.seek_to_last();
				}
			}
			Bound::Unbounded => {
				self.iter.seek_to_last();
			}
		}

		// Ensure we're still within the start bound
		// Extract user key without allocating
		if self.iter.valid() {
			let current_user_key = InternalKey::user_key_from_encoded(self.iter.key());

			match &self.start_bound {
				Bound::Included(ref start_key) => {
					if current_user_key < start_key.user_key.as_slice() {
						self.exhausted = true;
					}
				}
				Bound::Excluded(ref start_key) => {
					if current_user_key <= start_key.user_key.as_slice() {
						self.exhausted = true;
					}
				}
				Bound::Unbounded => {}
			}
		}

		self.exhausted = self.exhausted || !self.iter.valid();
		self.backward_initialized = true;
		Ok(())
	}

	pub(crate) fn key(&self) -> &[u8] {
		if self.iter.valid() && !self.exhausted {
			self.iter.key()
		} else {
			&[]
		}
	}

	pub(crate) fn value(&self) -> &[u8] {
		if self.keys_only {
			&self.empty_value_buffer
		} else if self.iter.valid() && !self.exhausted {
			self.iter.value()
		} else {
			&self.empty_value_buffer
		}
	}
}

impl Iterator for MemTableRangeInternalIterator<'_> {
	type Item = Result<(InternalKey, Value)>;

	fn next(&mut self) -> Option<Self::Item> {
		if !self.valid() {
			return None;
		}

		let key = InternalKey::decode(self.key());
		let value = self.value().to_vec();
		let _ = self.next_internal();
		Some(Ok((key, value)))
	}
}

impl DoubleEndedIterator for MemTableRangeInternalIterator<'_> {
	fn next_back(&mut self) -> Option<Self::Item> {
		if !self.backward_initialized {
			let _ = self.seek_to_last();
			self.backward_initialized = true;
		}
		if !self.valid() {
			return None;
		}

		let key = InternalKey::decode(self.key());
		let value = self.value().to_vec();
		let _ = self.prev_internal();
		Some(Ok((key, value)))
	}
}
