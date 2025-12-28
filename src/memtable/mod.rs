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

#[derive(Default, Clone)]
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

	pub(crate) fn into_inner(self) -> Vec<ImmutableEntry> {
		self.0
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
				false,                       // not bottom level (L0 flush)
				None,                        // no vlog access in flush context
				false,                       // versioning disabled in flush context
				0,                           // retention period is 0 in flush context
				Arc::clone(&lsm_opts.clock), // clock is the system clock
			);
			for (key, encoded_val) in comp_iter.by_ref() {
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
	) -> impl DoubleEndedIterator<Item = (InternalKey, Value)> + '_ {
		self.map.iter().map(move |entry| {
			let key = entry.key().clone();
			let value = if keys_only {
				Value::new()
			} else {
				entry.value().clone()
			};
			(key, value)
		})
	}

	pub(crate) fn range(
		&self,
		range: InternalKeyRange,
		keys_only: bool,
	) -> impl DoubleEndedIterator<Item = (InternalKey, Value)> + '_ {
		self.map.range(range).map(move |entry| {
			let key = entry.key().clone();
			let value = if keys_only {
				Value::new()
			} else {
				entry.value().clone()
			};
			(key, value)
		})
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use test_log::test;

	use super::*;
	use crate::user_range_to_internal_range;

	fn assert_value(encoded_value: &Value, expected_value: &[u8]) {
		// Skip the tag byte (first byte) and compare the actual value content
		let value_content = &encoded_value[..];
		assert_eq!(value_content, expected_value);
	}

	#[test]
	fn memtable_get() {
		let memtable = MemTable::new();
		let key = b"foo".to_vec();
		let value = b"value".to_vec();

		let mut batch = Batch::new(1);
		batch.set(key, value.clone(), 0).unwrap();

		memtable.add(&batch).unwrap();

		let res = memtable.get(b"foo", None).unwrap();
		assert_value(&res.1, &value);
	}

	#[test]
	fn memtable_size() {
		let memtable = MemTable::new();
		let key = b"foo".to_vec();
		let value = b"value".to_vec();

		let mut batch = Batch::new(1);
		batch.set(key, value, 0).unwrap();

		memtable.add(&batch).unwrap();

		assert!(memtable.size() > 0);
	}

	#[test]
	fn memtable_lsn() {
		let memtable = MemTable::new();
		let key = b"foo".to_vec();
		let value = b"value".to_vec();
		let seq_num = 100;

		let mut batch = Batch::new(seq_num);
		batch.set(key, value, 0).unwrap();

		memtable.add(&batch).unwrap();

		assert_eq!(seq_num, memtable.lsn());
	}

	#[test]
	fn memtable_add_and_get() {
		let memtable = MemTable::new();
		let key1 = b"key1".to_vec();
		let value1 = b"value1".to_vec();

		let mut batch1 = Batch::new(1);
		batch1.set(key1, value1.clone(), 0).unwrap();

		memtable.add(&batch1).unwrap();

		let key2 = b"key2".to_vec();
		let value2 = b"value2".to_vec();

		let mut batch2 = Batch::new(2);
		batch2.set(key2, value2.clone(), 0).unwrap();

		memtable.add(&batch2).unwrap();

		let res = memtable.get(b"key1", None).unwrap();
		assert_value(&res.1, &value1);

		let res = memtable.get(b"key2", None).unwrap();
		assert_value(&res.1, &value2);
	}

	#[test]
	fn memtable_get_latest_seq_no() {
		let memtable = MemTable::new();
		let key1 = b"key1".to_vec();
		let value1 = b"value1".to_vec();
		let value2 = b"value2".to_vec();
		let value3 = b"value3".to_vec();

		let mut batch1 = Batch::new(1);
		batch1.set(key1.clone(), value1, 0).unwrap();
		memtable.add(&batch1).unwrap();

		let mut batch2 = Batch::new(2);
		batch2.set(key1.clone(), value2, 0).unwrap();
		memtable.add(&batch2).unwrap();

		let mut batch3 = Batch::new(3);
		batch3.set(key1, value3.clone(), 0).unwrap();
		memtable.add(&batch3).unwrap();

		let res = memtable.get(b"key1", None).unwrap();
		assert_value(&res.1, &value3);
	}

	#[test]
	fn memtable_prefix() {
		let memtable = MemTable::new();
		let key1 = b"foo".to_vec();
		let value1 = b"value1".to_vec();

		let key2 = b"foo1".to_vec();
		let value2 = b"value2".to_vec();

		let mut batch1 = Batch::new(0);
		batch1.set(key1, value1.clone(), 0).unwrap();
		memtable.add(&batch1).unwrap();

		let mut batch2 = Batch::new(1);
		batch2.set(key2, value2.clone(), 0).unwrap();
		memtable.add(&batch2).unwrap();

		let res = memtable.get(b"foo", None).unwrap();
		assert_value(&res.1, &value1);

		let res = memtable.get(b"foo1", None).unwrap();
		assert_value(&res.1, &value2);
	}

	type TestEntry = (Vec<u8>, Vec<u8>, InternalKeyKind, Option<u64>);

	fn create_test_memtable(entries: Vec<TestEntry>) -> (Arc<MemTable>, u64) {
		let memtable = Arc::new(MemTable::new());

		let mut last_seq = 0;

		// For test purposes, if custom sequence numbers are provided, we need to add
		// each entry individually to ensure they get the exact sequence number
		// specified
		for (key, value, kind, custom_seq) in entries {
			let seq_num = custom_seq.unwrap_or_else(|| {
				last_seq += 1;
				last_seq
			});

			// Create a single-entry batch for each record to ensure exact sequence number
			// assignment
			let mut batch = Batch::new(seq_num);
			match kind {
				InternalKeyKind::Set => {
					batch.set(key.clone(), value.clone(), 0).unwrap();
				}
				InternalKeyKind::Delete => {
					batch.delete(key.clone(), 0).unwrap();
				}
				_ => {
					// For other kinds, use add_record directly
					batch.add_record(kind, key.clone(), Some(value.clone()), 0).unwrap();
				}
			}

			memtable.add(&batch).unwrap();

			if custom_seq.is_some() {
				last_seq = std::cmp::max(last_seq, seq_num);
			}
		}

		(memtable, last_seq)
	}

	#[test]
	fn test_empty_memtable() {
		let memtable = Arc::new(MemTable::new());

		// Test that iterator is empty
		let entries: Vec<_> = memtable.iter(false).collect();
		assert!(entries.is_empty());

		// Test that is_empty returns true
		assert!(memtable.is_empty());

		// Test that size returns 0
		assert_eq!(memtable.size(), 0);
	}

	#[test]
	fn test_single_key() {
		let (memtable, _) = create_test_memtable(vec![(
			b"key1".to_vec(),
			b"value1".to_vec(),
			InternalKeyKind::Set,
			None,
		)]);

		// Collect all entries
		let entries: Vec<_> = memtable.iter(false).collect::<Vec<_>>();
		assert_eq!(entries.len(), 1);

		let (key, encoded_value) = &entries[0];
		let user_key = &key.user_key;
		assert_eq!(user_key, b"key1");

		assert_value(encoded_value, &b"value1"[..]);

		// Test get method
		let result = memtable.get(b"key1", None);
		assert!(result.is_some());
		let (ikey, encoded_val) = result.unwrap();
		assert_eq!(&ikey.user_key, b"key1");

		assert_value(&encoded_val, b"value1");
	}

	#[test]
	fn test_multiple_keys() {
		let (memtable, _) = create_test_memtable(vec![
			(b"key1".to_vec(), b"value1".to_vec(), InternalKeyKind::Set, None),
			(b"key3".to_vec(), b"value3".to_vec(), InternalKeyKind::Set, None),
			(b"key5".to_vec(), b"value5".to_vec(), InternalKeyKind::Set, None),
		]);

		// Collect all entries
		let entries: Vec<_> = memtable.iter(false).collect::<Vec<_>>();
		assert_eq!(entries.len(), 3);

		// Extract user keys for comparison
		let user_keys: Vec<_> = entries.iter().map(|(key, _)| key.user_key.clone()).collect();

		// Keys should be in lexicographic order
		assert_eq!(&user_keys[0], b"key1");
		assert_eq!(&user_keys[1], b"key3");
		assert_eq!(&user_keys[2], b"key5");

		// Test individual gets
		assert!(memtable.get(b"key1", None).is_some());
		assert!(memtable.get(b"key3", None).is_some());
		assert!(memtable.get(b"key5", None).is_some());
		assert!(memtable.get(b"key2", None).is_none());
		assert!(memtable.get(b"key4", None).is_none());
	}

	#[test]
	fn test_sequence_number_ordering() {
		// Create test with multiple sequence numbers for the same key
		let (memtable, _) = create_test_memtable(vec![
			(b"key1".to_vec(), b"value1".to_vec(), InternalKeyKind::Set, Some(10)),
			(b"key1".to_vec(), b"value2".to_vec(), InternalKeyKind::Set, Some(20)), /* Higher sequence number */
			(b"key1".to_vec(), b"value3".to_vec(), InternalKeyKind::Set, Some(5)), /* Lower sequence number */
		]);

		// Collect all entries
		let entries: Vec<_> = memtable.iter(false).collect::<Vec<_>>();
		assert_eq!(entries.len(), 3);

		// Extract sequence numbers and values
		let mut key1_entries = Vec::new();
		for (key, encoded_value) in &entries {
			let (user_key, seq_num, _) = (key.user_key.clone(), key.seq_num(), key.kind());
			if &user_key == b"key1" {
				key1_entries.push((seq_num, encoded_value));
			}
		}

		// Verify ordering - higher sequence numbers should come first
		assert_eq!(key1_entries.len(), 3);
		assert_eq!(key1_entries[0].0, 20);
		assert_eq!(key1_entries[0].1, b"value2");
		assert_eq!(key1_entries[1].0, 10);
		assert_eq!(key1_entries[1].1, b"value1");
		assert_eq!(key1_entries[2].0, 5);
		assert_eq!(key1_entries[2].1, b"value3");

		// Test get method - should return the highest sequence number
		let result = memtable.get(b"key1", None);
		assert!(result.is_some());
		let (ikey, encoded_val) = result.unwrap();
		assert_eq!(ikey.seq_num(), 20);
		assert_eq!(&encoded_val, b"value2");
	}

	#[test]
	fn test_key_updates_with_sequence_numbers() {
		// Create test with key updates
		let (memtable, _) = create_test_memtable(vec![
			(b"key1".to_vec(), b"old_value".to_vec(), InternalKeyKind::Set, Some(5)),
			(b"key1".to_vec(), b"new_value".to_vec(), InternalKeyKind::Set, Some(10)),
			(b"key2".to_vec(), b"value2".to_vec(), InternalKeyKind::Set, Some(7)),
		]);

		// Test get returns the latest value
		let result = memtable.get(b"key1", None);
		assert!(result.is_some());
		let (_, encoded_val) = result.unwrap();
		assert_value(&encoded_val, b"new_value");

		// Test get with specific sequence number
		let result = memtable.get(b"key1", Some(8));
		assert!(result.is_some());
		let (_, encoded_val) = result.unwrap();
		assert_value(&encoded_val, b"old_value"); // Should get the value with seq_num
		                                    // <= 8
	}

	#[test]
	fn test_tombstones() {
		// Create test with deleted entries
		let (memtable, _) = create_test_memtable(vec![
			(b"key1".to_vec(), b"value1".to_vec(), InternalKeyKind::Set, Some(1)),
			(b"key2".to_vec(), b"value2".to_vec(), InternalKeyKind::Set, Some(2)),
			(b"key3".to_vec(), b"value3".to_vec(), InternalKeyKind::Set, Some(3)),
			(b"key2".to_vec(), vec![], InternalKeyKind::Delete, Some(4)), // Delete key2
		]);

		// Iterator should see all entries including tombstones
		let entries: Vec<_> = memtable.iter(false).collect::<Vec<_>>();

		// Count entries for each key
		let mut key_counts = HashMap::new();
		for (key, _) in &entries {
			let user_key = &key.user_key;
			*key_counts.entry(user_key).or_insert(0) += 1;
		}

		assert_eq!(key_counts[&b"key1".to_vec()], 1);
		assert_eq!(key_counts[&b"key2".to_vec()], 2); // Original + tombstone
		assert_eq!(key_counts[&b"key3".to_vec()], 1);
	}

	#[test]
	fn test_key_kinds() {
		// Test different key kinds
		let (memtable, _) = create_test_memtable(vec![
			(b"key1".to_vec(), b"value1".to_vec(), InternalKeyKind::Set, Some(10)),
			(b"key2".to_vec(), vec![], InternalKeyKind::Delete, Some(20)),
			(b"key3".to_vec(), b"value3".to_vec(), InternalKeyKind::Set, Some(30)),
			(b"key4".to_vec(), vec![], InternalKeyKind::Delete, Some(40)),
		]);

		// All key types should be visible in the iterator
		let entries: Vec<_> = memtable.iter(false).collect::<Vec<_>>();
		assert_eq!(entries.len(), 4);

		// Extract and verify key information
		let mut key_info = Vec::new();
		for (key, encoded_value) in &entries {
			let (user_key, seq_num, kind) = (key.user_key.clone(), key.seq_num(), key.kind());
			key_info.push((user_key, seq_num, kind, encoded_value.len()));
		}

		// Verify all keys are present with correct kinds
		assert_eq!(&key_info[0].0, b"key1");
		assert_eq!(key_info[0].2, InternalKeyKind::Set);
		assert!(key_info[0].3 > 0); // Has value

		assert_eq!(&key_info[1].0, b"key2");
		assert_eq!(key_info[1].2, InternalKeyKind::Delete);
		assert_eq!(key_info[1].3, 0); // No value for delete

		assert_eq!(&key_info[2].0, b"key3");
		assert_eq!(key_info[2].2, InternalKeyKind::Set);
		assert!(key_info[2].3 > 0); // Has value

		assert_eq!(&key_info[3].0, b"key4");
		assert_eq!(key_info[3].2, InternalKeyKind::Delete);
		assert_eq!(key_info[3].3, 0); // No value for delete

		// Test get method behavior with different kinds
		let result = memtable.get(b"key1", None);
		assert!(result.is_some());
		let (ikey, _) = result.unwrap();
		assert_eq!(ikey.kind(), InternalKeyKind::Set);

		let result = memtable.get(b"key2", None);
		assert!(result.is_some());
		let (ikey, encoded_val) = result.unwrap();
		assert_eq!(ikey.kind(), InternalKeyKind::Delete);
		assert_eq!(encoded_val.len(), 0);
	}

	#[test]
	fn test_range_query() {
		// Create a memtable with many keys
		let (memtable, _) = create_test_memtable(vec![
			(b"a".to_vec(), b"value-a".to_vec(), InternalKeyKind::Set, None),
			(b"c".to_vec(), b"value-c".to_vec(), InternalKeyKind::Set, None),
			(b"e".to_vec(), b"value-e".to_vec(), InternalKeyKind::Set, None),
			(b"g".to_vec(), b"value-g".to_vec(), InternalKeyKind::Set, None),
			(b"i".to_vec(), b"value-i".to_vec(), InternalKeyKind::Set, None),
			(b"k".to_vec(), b"value-k".to_vec(), InternalKeyKind::Set, None),
			(b"m".to_vec(), b"value-m".to_vec(), InternalKeyKind::Set, None),
		]);

		// Test inclusive range
		use std::ops::Bound;
		let range_entries: Vec<_> = memtable
			.range(
				user_range_to_internal_range(
					Bound::Included("c".as_bytes()),
					Bound::Included("k".as_bytes()),
				),
				false,
			)
			.collect::<Vec<_>>();

		let user_keys: Vec<_> = range_entries.iter().map(|(key, _)| key.user_key.clone()).collect();

		assert_eq!(user_keys.len(), 5);
		assert_eq!(&user_keys[0], b"c");
		assert_eq!(&user_keys[1], b"e");
		assert_eq!(&user_keys[2], b"g");
		assert_eq!(&user_keys[3], b"i");
		assert_eq!(&user_keys[4], b"k");

		// Test exclusive range
		let range_entries: Vec<_> = memtable
			.range(
				user_range_to_internal_range(
					Bound::Included("c".as_bytes()),
					Bound::Excluded("k".as_bytes()),
				),
				false,
			)
			.collect::<Vec<_>>();

		let user_keys: Vec<_> = range_entries.iter().map(|(key, _)| key.user_key.clone()).collect();

		assert_eq!(user_keys.len(), 4); // Excludes "k"
		assert_eq!(&user_keys[0], b"c");
		assert_eq!(&user_keys[1], b"e");
		assert_eq!(&user_keys[2], b"g");
		assert_eq!(&user_keys[3], b"i");
	}

	#[test]
	fn test_range_query_with_sequence_numbers() {
		// Create a memtable with overlapping sequence numbers
		let (memtable, _) = create_test_memtable(vec![
			(b"a".to_vec(), b"value-a1".to_vec(), InternalKeyKind::Set, Some(10)),
			(b"a".to_vec(), b"value-a2".to_vec(), InternalKeyKind::Set, Some(20)), // Updated value
			(b"c".to_vec(), b"value-c1".to_vec(), InternalKeyKind::Set, Some(15)),
			(b"e".to_vec(), b"value-e1".to_vec(), InternalKeyKind::Set, Some(25)),
			(b"e".to_vec(), b"value-e2".to_vec(), InternalKeyKind::Set, Some(15)), // Older version
		]);

		// Perform a range query from "a" to "f"
		use std::ops::Bound;
		let range_entries: Vec<_> = memtable
			.range(
				user_range_to_internal_range(
					Bound::Included("a".as_bytes()),
					Bound::Excluded("f".as_bytes()),
				),
				false,
			)
			.collect::<Vec<_>>();

		// Extract user keys, sequence numbers and values
		let mut entries_info = Vec::new();
		for (key, encoded_value) in &range_entries {
			let (user_key, seq_num, _) = (key.user_key.clone(), key.seq_num(), key.kind());
			entries_info.push((user_key, seq_num, encoded_value));
		}

		// Verify we get keys in order, with highest sequence numbers first for each key
		assert_eq!(entries_info.len(), 5);

		// Key "a" entries (seq 20 then seq 10)
		assert_eq!(&entries_info[0].0, b"a");
		assert_eq!(entries_info[0].1, 20);
		assert_eq!(entries_info[0].2, b"value-a2");

		assert_eq!(entries_info[1].0, b"a");
		assert_eq!(entries_info[1].1, 10);
		assert_eq!(entries_info[1].2, b"value-a1");

		// Key "c" entry
		assert_eq!(entries_info[2].0, b"c");
		assert_eq!(entries_info[2].1, 15);
		assert_eq!(entries_info[2].2, b"value-c1");

		// Key "e" entries (seq 25 then seq 15)
		assert_eq!(entries_info[3].0, b"e");
		assert_eq!(entries_info[3].1, 25);
		assert_eq!(entries_info[3].2, b"value-e1");

		assert_eq!(entries_info[4].0, b"e");
		assert_eq!(entries_info[4].1, 15);
		assert_eq!(entries_info[4].2, b"value-e2");
	}

	#[test]
	fn test_binary_keys() {
		// Test with binary keys containing nulls and various byte values
		let (memtable, _) = create_test_memtable(vec![
			(vec![0, 0, 1], b"value1".to_vec(), InternalKeyKind::Set, None),
			(vec![0, 1, 0], b"value2".to_vec(), InternalKeyKind::Set, None),
			(vec![1, 0, 0], b"value3".to_vec(), InternalKeyKind::Set, None),
			(vec![0xFF, 0xFE, 0xFD], b"value4".to_vec(), InternalKeyKind::Set, None),
		]);

		let entries: Vec<_> = memtable.iter(false).collect::<Vec<_>>();
		assert_eq!(entries.len(), 4);

		// Extract and verify user keys are in correct order
		let user_keys: Vec<_> = entries.iter().map(|(key, _)| key.user_key.clone()).collect();

		assert_eq!(user_keys[0].as_ref(), vec![0, 0, 1]);
		assert_eq!(user_keys[1].as_ref(), vec![0, 1, 0]);
		assert_eq!(user_keys[2].as_ref(), vec![1, 0, 0]);
		assert_eq!(user_keys[3].as_ref(), vec![0xFF, 0xFE, 0xFD]);
	}

	#[test]
	fn test_large_dataset() {
		// Create a larger dataset to test performance and correctness
		let mut entries = Vec::new();
		for i in 0..1000 {
			let key = format!("key{i:04}").as_bytes().to_vec();
			let value = format!("value{i:04}").as_bytes().to_vec();
			entries.push((key, value, InternalKeyKind::Set, None));
		}

		let (memtable, _) = create_test_memtable(entries);

		// Test that all entries exist
		let all_entries: Vec<_> = memtable.iter(false).collect::<Vec<_>>();
		assert_eq!(all_entries.len(), 1000);

		// Test specific gets
		let result = memtable.get(b"key0000", None);
		assert!(result.is_some());
		let (_, encoded_val) = result.unwrap();
		assert_value(&encoded_val, b"value0000");

		let result = memtable.get(b"key0500", None);
		assert!(result.is_some());
		let (_, encoded_val) = result.unwrap();
		assert_value(&encoded_val, b"value0500");

		let result = memtable.get(b"key0999", None);
		assert!(result.is_some());
		let (_, encoded_val) = result.unwrap();
		assert_value(&encoded_val, b"value0999");

		// Test non-existent key
		let result = memtable.get(b"key1000", None);
		assert!(result.is_none());
	}

	#[test]
	fn test_memtable_size_tracking() {
		let memtable = Arc::new(MemTable::new());

		// Initially empty
		assert_eq!(memtable.size(), 0);

		// Add some data
		let mut batch = Batch::new(1);
		batch.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
		batch.set(b"key2".to_vec(), b"value2".to_vec(), 0).unwrap();

		let (record_size, total_size) = memtable.add(&batch).unwrap();
		assert!(record_size > 0);
		assert_eq!(total_size, record_size);
		assert_eq!(memtable.size(), total_size as usize);

		// Add more data
		let mut batch2 = Batch::new(2);
		batch2.set(b"key3".to_vec(), b"value3".to_vec(), 0).unwrap();

		let (record_size2, total_size2) = memtable.add(&batch2).unwrap();
		assert!(record_size2 > 0);
		assert_eq!(total_size2, total_size + record_size2);
		assert_eq!(memtable.size(), total_size2 as usize);
	}

	#[test]
	fn test_latest_sequence_number() {
		let memtable = Arc::new(MemTable::new());

		// Initially 0
		assert_eq!(memtable.lsn(), 0);

		// Add batch with seq_num 10
		let mut batch1 = Batch::new(10);
		batch1.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
		memtable.add(&batch1).unwrap();
		assert_eq!(memtable.lsn(), 10);

		// Add batch with lower seq_num - should not update
		let mut batch2 = Batch::new(5);
		batch2.set(b"key2".to_vec(), b"value2".to_vec(), 0).unwrap();
		memtable.add(&batch2).unwrap();
		assert_eq!(memtable.lsn(), 10); // Should still be 10

		// Add batch with higher seq_num
		let mut batch3 = Batch::new(20);
		batch3.set(b"key3".to_vec(), b"value3".to_vec(), 0).unwrap();
		memtable.add(&batch3).unwrap();
		assert_eq!(memtable.lsn(), 20);
	}

	#[test]
	fn test_get_highest_seq_num() {
		// Add a batch with 5 entries
		let mut batch = Batch::new(10);
		batch.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
		batch.set(b"key2".to_vec(), b"value2".to_vec(), 0).unwrap();
		batch.set(b"key3".to_vec(), b"value3".to_vec(), 0).unwrap();
		batch.set(b"key4".to_vec(), b"value4".to_vec(), 0).unwrap();
		batch.set(b"key5".to_vec(), b"value5".to_vec(), 0).unwrap();

		assert_eq!(batch.get_highest_seq_num(), 14);
	}
}
