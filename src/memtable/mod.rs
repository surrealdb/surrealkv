use std::fs::File as SysFile;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use crossbeam_skiplist::SkipMap;

use crate::batch::Batch;
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
}

pub(crate) struct MemTable {
	map: SkipMap<InternalKey, Value>,
	latest_seq_num: AtomicU64,
	map_size: AtomicU32,
	/// WAL number that was current when this memtable started receiving writes.
	/// Used to determine which WALs can be safely deleted after flush.
	wal_number: AtomicU64,
}

impl Default for MemTable {
	fn default() -> Self {
		Self::new()
	}
}

impl MemTable {
	#[allow(unused)]
	pub(crate) fn new() -> Self {
		MemTable {
			map: SkipMap::new(),
			latest_seq_num: AtomicU64::new(0),
			map_size: AtomicU32::new(0),
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
		let range = InternalKey::new(
			key.to_vec(),
			seq_no,
			InternalKeyKind::Set, // This field is not checked in the comparator
			0,                    // This field is not checked in the comparator
		)..;

		let mut iter = self.map.range(range).take_while(|entry| &entry.key().user_key[..] == key);
		iter.next().map(|entry| (entry.key().clone(), entry.value().clone()))
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.map.is_empty()
	}

	pub(crate) fn size(&self) -> usize {
		self.map_size.load(Ordering::Acquire) as usize
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
	pub(crate) fn add(&self, batch: &Batch) -> Result<(u32, u32)> {
		let (record_size, highest_seq_num) = self.apply_batch_to_memtable(batch)?;
		let size_before = self.update_memtable_size(record_size);
		self.update_latest_sequence_number(highest_seq_num);
		Ok((record_size, size_before + record_size))
	}

	/// Applies the batch of operations to the in-memory table (memtable).
	/// Returns (total_record_size, highest_seq_num_used).
	fn apply_batch_to_memtable(&self, batch: &Batch) -> Result<(u32, u64)> {
		let mut record_size = 0;

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

			let entry_size = self.insert_into_memtable(&ikey, &val);
			record_size += entry_size;
		}

		// Get the highest sequence number used from the batch
		let highest_seq_num = batch.get_highest_seq_num();

		Ok((record_size, highest_seq_num))
	}

	/// Inserts a key-value pair into the memtable.
	fn insert_into_memtable(&self, key: &InternalKey, value: &Value) -> u32 {
		self.map.insert(key.clone(), value.clone());
		key.size() as u32 + value.len() as u32
	}

	/// Updates the size of the memtable by adding the size of the newly added
	/// records.
	fn update_memtable_size(&self, record_size: u32) -> u32 {
		self.map_size.fetch_add(record_size, std::sync::atomic::Ordering::AcqRel)
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
				false,                                   // not bottom level (L0 flush)
				None,                                    // no vlog access in flush context
				lsm_opts.enable_versioning,              // versioning disabled in flush context
				lsm_opts.versioned_history_retention_ns, // retention period is 0 in flush context
				Arc::clone(&lsm_opts.clock),             // clock is the system clock
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

	pub(crate) fn iter(
		&self,
		keys_only: bool,
	) -> impl DoubleEndedIterator<Item = Result<(InternalKey, Value)>> + '_ {
		self.map.iter().map(move |entry| {
			let key = entry.key().clone();
			let value = if keys_only {
				Value::new()
			} else {
				entry.value().clone()
			};
			Ok((key, value))
		})
	}

	pub(crate) fn range(
		&self,
		range: InternalKeyRange,
		keys_only: bool,
	) -> impl DoubleEndedIterator<Item = Result<(InternalKey, Value)>> + '_ {
		self.map.range(range).map(move |entry| {
			let key = entry.key().clone();
			let value = if keys_only {
				Value::new()
			} else {
				entry.value().clone()
			};
			Ok((key, value))
		})
	}
}
