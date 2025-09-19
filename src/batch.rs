use integer_encoding::{VarInt, VarIntWriter};

use crate::error::{Error, Result};
use crate::sstable::InternalKeyKind;
use crate::vlog::ValuePointer;

const MAX_BATCH_SIZE: u64 = 1 << 32;
const BATCH_VERSION: u8 = 1;

type RecordKey<'a> = (InternalKeyKind, &'a [u8], Option<&'a [u8]>);

/// Represents a single entry in a batch
#[derive(Debug, Clone)]
pub(crate) struct BatchEntry {
	pub kind: InternalKeyKind,
	pub key: Vec<u8>,
	pub value: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub(crate) struct Batch {
	version: u8,
	entries: Vec<BatchEntry>,
	valueptrs: Vec<Option<ValuePointer>>, // Parallel array to entries, None for inline values
}

impl Default for Batch {
	fn default() -> Self {
		Self::new()
	}
}

impl Batch {
	pub(crate) fn new() -> Self {
		Self {
			entries: Vec::new(),
			valueptrs: Vec::new(),
			version: BATCH_VERSION,
		}
	}

	// TODO: add a test for grow
	fn grow(&mut self, n: usize) -> Result<()> {
		let new_size = self.entries.len() + self.valueptrs.len() + n;
		if new_size as u64 >= MAX_BATCH_SIZE {
			return Err(Error::BatchTooLarge);
		}
		self.entries.reserve(n);
		self.valueptrs.reserve(n);
		Ok(())
	}

	pub(crate) fn encode(&self, seq_num: u64) -> Result<Vec<u8>> {
		let mut encoded = Vec::new();

		// Write version (1 byte)
		encoded.push(self.version);

		// Write sequence number (8 bytes)
		encoded.write_varint(seq_num)?;

		// Write count (4 bytes)
		encoded.write_varint(self.entries.len() as u32)?;

		// Write entries
		for entry in &self.entries {
			// Write kind (1 byte)
			encoded.push(entry.kind as u8);

			// Write key length and key
			encoded.write_varint(entry.key.len() as u64)?;
			encoded.extend_from_slice(&entry.key);

			// Write value length and value
			let value_len = entry.value.as_ref().map_or(0, |v| v.len());
			encoded.write_varint(value_len as u64)?;
			if let Some(value) = &entry.value {
				encoded.extend_from_slice(value);
			}
		}

		// Write value pointers
		for valueptr in &self.valueptrs {
			match valueptr {
				Some(ptr) => {
					encoded.push(1); // Has pointer
					encoded.extend_from_slice(&ptr.encode());
				}
				None => {
					encoded.push(0); // No pointer (inline value)
				}
			}
		}

		Ok(encoded)
	}

	#[cfg(test)]
	pub(crate) fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
		self.add_record(InternalKeyKind::Set, key, Some(value))
	}

	#[cfg(test)]
	pub(crate) fn delete(&mut self, key: &[u8]) -> Result<()> {
		self.add_record(InternalKeyKind::Delete, key, None)
	}

	pub(crate) fn add_record(
		&mut self,
		kind: InternalKeyKind,
		key: &[u8],
		value: Option<&[u8]>,
	) -> Result<()> {
		self.grow(1)?;

		let entry = BatchEntry {
			kind,
			key: key.to_vec(),
			value: value.map(|v| v.to_vec()),
		};

		self.entries.push(entry);
		self.valueptrs.push(None); // Initially no VLog pointer

		Ok(())
	}

	pub(crate) fn iter(&self) -> BatchIterator<'_> {
		BatchIterator {
			entries: &self.entries,
			pos: 0,
		}
	}

	pub(crate) fn count(&self) -> u32 {
		self.entries.len() as u32
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	/// Get entries for VLog processing
	pub(crate) fn entries(&self) -> &[BatchEntry] {
		&self.entries
	}

	/// Get mutable entries for VLog processing
	pub(crate) fn entries_mut(&mut self) -> &mut [BatchEntry] {
		&mut self.entries
	}

	/// Get value pointers
	pub(crate) fn valueptrs(&self) -> &[Option<ValuePointer>] {
		&self.valueptrs
	}

	/// Get mutable value pointers
	pub(crate) fn valueptrs_mut(&mut self) -> &mut [Option<ValuePointer>] {
		&mut self.valueptrs
	}

	/// Set a value pointer for a specific entry index
	pub(crate) fn set_valueptr(
		&mut self,
		index: usize,
		valueptr: Option<ValuePointer>,
	) -> Result<()> {
		if index >= self.valueptrs.len() {
			return Err(Error::InvalidBatchRecord);
		}
		self.valueptrs[index] = valueptr;
		Ok(())
	}

	/// Decode a batch from encoded data
	pub(crate) fn decode(data: &[u8]) -> Result<(u64, Self)> {
		if data.is_empty() {
			return Err(Error::InvalidBatchRecord);
		}

		let mut pos = 0;

		// Read version
		let version = data[pos];
		pos += 1;
		if version != BATCH_VERSION {
			return Err(Error::InvalidBatchRecord);
		}

		// Read sequence number
		let (seq_num, bytes_read) =
			u64::decode_var(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
		pos += bytes_read;

		// Read count
		let (count, bytes_read) = u32::decode_var(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
		pos += bytes_read;

		// Read entries
		let mut entries = Vec::with_capacity(count as usize);
		for _ in 0..count {
			// Read kind
			let kind_byte = data[pos];
			pos += 1;
			let kind = InternalKeyKind::from(kind_byte);
			if kind == InternalKeyKind::Invalid {
				return Err(Error::InvalidBatchRecord);
			}

			// Read key
			let (key_len, bytes_read) =
				u64::decode_var(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
			pos += bytes_read;
			let key = data[pos..pos + key_len as usize].to_vec();
			pos += key_len as usize;

			// Read value
			let (value_len, bytes_read) =
				u64::decode_var(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
			pos += bytes_read;
			let value = if value_len > 0 {
				let value_data = data[pos..pos + value_len as usize].to_vec();
				pos += value_len as usize;
				Some(value_data)
			} else {
				None
			};

			entries.push(BatchEntry {
				kind,
				key,
				value,
			});
		}

		// Read value pointers
		let mut valueptrs = Vec::with_capacity(count as usize);
		for _ in 0..count {
			let has_pointer = data[pos];
			pos += 1;
			let valueptr = if has_pointer == 1 {
				let ptr_data = &data[pos..pos + crate::vlog::VALUE_POINTER_SIZE];
				pos += crate::vlog::VALUE_POINTER_SIZE;
				Some(ValuePointer::decode(ptr_data)?)
			} else {
				None
			};
			valueptrs.push(valueptr);
		}

		Ok((
			seq_num,
			Self {
				version,
				entries,
				valueptrs,
			},
		))
	}
}

pub(crate) struct BatchIterator<'a> {
	entries: &'a [BatchEntry],
	pos: usize,
}

impl<'a> Iterator for BatchIterator<'a> {
	type Item = Result<RecordKey<'a>>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.pos >= self.entries.len() {
			return None;
		}

		let entry = &self.entries[self.pos];
		self.pos += 1;

		let record = (entry.kind, entry.key.as_slice(), entry.value.as_deref());

		Some(Ok(record))
	}
}

pub(crate) struct BatchReader {
	batch: Batch,
	pos: usize,
	seq_num: u64,
}

impl BatchReader {
	pub(crate) fn new(data: &[u8]) -> Result<Self> {
		let (seq_num, batch) = Batch::decode(data)?;

		Ok(Self {
			batch,
			pos: 0,
			seq_num,
		})
	}

	pub(crate) fn get_seq_num(&self) -> u64 {
		self.seq_num
	}

	pub(crate) fn read_record(
		&mut self,
	) -> Result<Option<(InternalKeyKind, Vec<u8>, Option<Vec<u8>>)>> {
		if self.pos >= self.batch.entries.len() {
			return Ok(None);
		}

		let entry = &self.batch.entries[self.pos];
		self.pos += 1;

		let record = (entry.kind, entry.key.clone(), entry.value.clone());

		Ok(Some(record))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_batch_new() {
		let batch = Batch::new();
		assert_eq!(batch.entries.len(), 0);
		assert_eq!(batch.count(), 0);
	}

	#[test]
	fn test_batch_grow() {
		let mut batch = Batch::new();
		assert!(batch.grow(10).is_ok());
		assert!(batch.grow(MAX_BATCH_SIZE as usize).is_err());
	}

	#[test]
	fn test_batch_encode() {
		let mut batch = Batch::new();
		batch.set(b"key1", b"value1").unwrap();
		let encoded = batch.encode(1).unwrap();
		assert!(!encoded.is_empty());
	}

	#[test]
	fn test_batch_get_count() {
		let mut batch = Batch::new();
		assert_eq!(batch.count(), 0);
		batch.set(b"key1", b"value1").unwrap();
		assert_eq!(batch.count(), 1);
	}

	#[test]
	fn test_batch_set() {
		let mut batch = Batch::new();
		batch.set(b"key1", b"value1").unwrap();
		assert_eq!(batch.count(), 1);
	}

	#[test]
	fn test_batch_delete() {
		let mut batch = Batch::new();
		batch.delete(b"key1").unwrap();
		assert_eq!(batch.count(), 1);
	}

	#[test]
	fn test_batchreader_new() {
		let mut batch = Batch::new();
		batch.set(b"key1", b"value1").unwrap();
		let encoded = batch.encode(100).unwrap();
		let reader = BatchReader::new(&encoded).unwrap();
		assert_eq!(reader.get_seq_num(), 100);
		// Note: get_count method was removed as it was unused
	}

	#[test]
	fn test_batchreader_get_seq_num() {
		let batch = Batch::new();
		let encoded = batch.encode(100).unwrap();
		let reader = BatchReader::new(&encoded).unwrap();
		assert_eq!(reader.get_seq_num(), 100);
	}

	#[test]
	fn test_batchreader_read_record() {
		let mut batch = Batch::new();
		batch.set(b"key1", b"value1").unwrap();
		let encoded = batch.encode(1).unwrap();
		let mut reader = BatchReader::new(&encoded).unwrap();
		let record = reader.read_record().unwrap().unwrap();
		assert_eq!(record.0, InternalKeyKind::Set);
		assert_eq!(record.1, b"key1");
		assert_eq!(record.2.unwrap(), b"value1");
	}

	#[test]
	fn test_batch_empty() {
		let batch = Batch::new();
		let encoded = batch.encode(1).unwrap();
		let mut reader = BatchReader::new(&encoded).unwrap();
		// Note: get_count method was removed as it was unused
		assert_eq!(reader.get_seq_num(), 1);
		assert!(reader.read_record().unwrap().is_none());
	}

	#[test]
	fn test_batch_multiple_operations() {
		let mut batch = Batch::new();
		batch.set(b"key1", b"value1").unwrap();
		batch.delete(b"key2").unwrap();
		batch.set(b"key3", b"value3").unwrap();

		assert_eq!(batch.count(), 3);

		let encoded = batch.encode(1).unwrap();
		let mut reader = BatchReader::new(&encoded).unwrap();

		let (kind, key, value) = reader.read_record().unwrap().unwrap();
		assert_eq!(kind, InternalKeyKind::Set);
		assert_eq!(key, b"key1");
		assert_eq!(value.unwrap(), b"value1");

		let (kind, key, value) = reader.read_record().unwrap().unwrap();
		assert_eq!(kind, InternalKeyKind::Delete);
		assert_eq!(key, b"key2");
		assert!(value.is_none());

		let (kind, key, value) = reader.read_record().unwrap().unwrap();
		assert_eq!(kind, InternalKeyKind::Set);
		assert_eq!(key, b"key3");
		assert_eq!(value.unwrap(), b"value3");

		assert!(reader.read_record().unwrap().is_none());
	}

	#[test]
	fn test_batch_large_key_value() {
		let large_key = vec![b'a'; 1000000];
		let large_value = vec![b'b'; 1000000];

		let mut batch = Batch::new();
		batch.set(&large_key, &large_value).unwrap();

		let encoded = batch.encode(1).unwrap();
		let mut reader = BatchReader::new(&encoded).unwrap();

		let (kind, key, value) = reader.read_record().unwrap().unwrap();
		assert_eq!(kind, InternalKeyKind::Set);
		assert_eq!(key, large_key);
		assert_eq!(value.unwrap(), large_value);
	}

	#[test]
	fn test_batch_max_size() {
		let mut batch = Batch::new();
		let key = vec![b'a'; 1000];
		let value = vec![b'b'; (MAX_BATCH_SIZE as usize) - 2000];

		assert!(batch.set(&key, &value).is_ok());
		assert!(batch.set(&key, &[0]).is_err());
	}

	#[test]
	fn test_batchreader() {
		let mut batch = Batch::new();
		batch.set(b"key1", b"value1").unwrap();
		batch.delete(b"key2").unwrap();
		batch.set(b"key3", b"value3").unwrap();

		let encoded = batch.encode(1).unwrap();
		let mut reader = BatchReader::new(&encoded).unwrap();

		let mut records = vec![];
		while let Some((kind, key, value)) = reader.read_record().unwrap() {
			records.push((kind, key, value));
		}
		assert_eq!(records.len(), 3);

		assert_eq!(records[0].0, InternalKeyKind::Set);
		assert_eq!(records[0].1, b"key1");
		assert_eq!(records[0].2.as_ref().unwrap(), &b"value1".to_vec());

		assert_eq!(records[1].0, InternalKeyKind::Delete);
		assert_eq!(records[1].1, b"key2");
		assert!(records[1].2.is_none());

		assert_eq!(records[2].0, InternalKeyKind::Set);
		assert_eq!(records[2].1, b"key3");
		assert_eq!(records[2].2.as_ref().unwrap(), &b"value3".to_vec());
	}

	#[test]
	fn test_batchreader_invalid_data() {
		let invalid_data = vec![0];
		assert!(BatchReader::new(&invalid_data).is_err());
	}

	#[test]
	fn test_batch_empty_key_and_value() {
		let mut batch = Batch::new();
		batch.set(b"", b"").unwrap();
		batch.delete(b"").unwrap();

		let encoded = batch.encode(1).unwrap();
		let mut reader = BatchReader::new(&encoded).unwrap();

		let (kind, key, value) = reader.read_record().unwrap().unwrap();
		assert_eq!(kind, InternalKeyKind::Set);
		assert_eq!(key, b"");
		assert!(value.is_none()); // Empty value now consistently returns None

		let (kind, key, value) = reader.read_record().unwrap().unwrap();
		assert_eq!(kind, InternalKeyKind::Delete);
		assert_eq!(key, b"");
		assert!(value.is_none());

		assert!(reader.read_record().unwrap().is_none());
	}

	#[test]
	fn test_batch_unicode_keys_and_values() {
		let mut batch = Batch::new();
		batch.set("🔑".as_bytes(), "🗝️".as_bytes()).unwrap();
		batch.set("こんにちは".as_bytes(), "世界".as_bytes()).unwrap();

		let encoded = batch.encode(1).unwrap();
		let mut reader = BatchReader::new(&encoded).unwrap();

		let (kind, key, value) = reader.read_record().unwrap().unwrap();
		assert_eq!(kind, InternalKeyKind::Set);
		assert_eq!(key, "🔑".as_bytes());
		assert_eq!(value.unwrap(), "🗝️".as_bytes());

		let (kind, key, value) = reader.read_record().unwrap().unwrap();
		assert_eq!(kind, InternalKeyKind::Set);
		assert_eq!(key, "こんにちは".as_bytes());
		assert_eq!(value.unwrap(), "世界".as_bytes());
	}

	#[test]
	fn test_batch_set_delete() {
		let mut batch = Batch::new();
		batch.set(b"key1", b"value1").unwrap();
		batch.delete(b"key2").unwrap();
		batch.set(b"key3", b"value3").unwrap();
		batch.delete(b"key1").unwrap();
		batch.set(b"key2", b"new_value2").unwrap();

		let encoded = batch.encode(1).unwrap();
		let mut reader = BatchReader::new(&encoded).unwrap();

		let mut records = vec![];
		while let Some((kind, key, value)) = reader.read_record().unwrap() {
			records.push((kind, key, value));
		}
		assert_eq!(records.len(), 5);

		assert_eq!(records[0].0, InternalKeyKind::Set);
		assert_eq!(records[0].1, b"key1");
		assert_eq!(records[0].2.as_ref().unwrap(), &b"value1".to_vec());

		assert_eq!(records[1].0, InternalKeyKind::Delete);
		assert_eq!(records[1].1, b"key2");
		assert!(records[1].2.is_none());

		assert_eq!(records[2].0, InternalKeyKind::Set);
		assert_eq!(records[2].1, b"key3");
		assert_eq!(records[2].2.as_ref().unwrap(), &b"value3".to_vec());

		assert_eq!(records[3].0, InternalKeyKind::Delete);
		assert_eq!(records[3].1, b"key1");
		assert!(records[3].2.is_none());

		assert_eq!(records[4].0, InternalKeyKind::Set);
		assert_eq!(records[4].1, b"key2");
		assert_eq!(records[4].2.as_ref().unwrap(), &b"new_value2".to_vec());
	}

	#[test]
	fn test_batch_sequence_numbers() {
		let mut batch = Batch::new();
		batch.set(b"key1", b"value1").unwrap();
		batch.set(b"key2", b"value2").unwrap();

		let encoded = batch.encode(100).unwrap();
		let reader = BatchReader::new(&encoded).unwrap();

		assert_eq!(reader.get_seq_num(), 100);
		// Note: get_count method was removed as it was unused
	}

	#[test]
	fn test_batch_large_number_of_records() {
		const NUM_RECORDS: usize = 10000;
		let mut batch = Batch::new();

		for i in 0..NUM_RECORDS {
			let key = format!("key{i}");
			let value = format!("value{i}");
			if i % 2 == 0 {
				batch.set(key.as_bytes(), value.as_bytes()).unwrap();
			} else {
				batch.delete(key.as_bytes()).unwrap();
			}
		}

		assert_eq!(batch.count() as usize, NUM_RECORDS);

		let encoded = batch.encode(1).unwrap();
		let mut reader = BatchReader::new(&encoded).unwrap();

		for i in 0..NUM_RECORDS {
			let (kind, key, value) = reader.read_record().unwrap().unwrap();
			let expected_key = format!("key{i}");

			if i % 2 == 0 {
				assert_eq!(kind, InternalKeyKind::Set);
				assert_eq!(key, expected_key.as_bytes());
				assert_eq!(value.unwrap(), format!("value{i}").as_bytes());
			} else {
				assert_eq!(kind, InternalKeyKind::Delete);
				assert_eq!(key, expected_key.as_bytes());
				assert!(value.is_none());
			}
		}

		assert!(reader.read_record().unwrap().is_none());
	}

	#[test]
	fn test_batch_iterator() {
		let mut batch = Batch::new();
		batch.set(b"key1", b"value1").unwrap();
		batch.delete(b"key2").unwrap();
		batch.set(b"key3", b"value3").unwrap();
		batch.delete(b"key1").unwrap();
		batch.set(b"key2", b"new_value2").unwrap();

		let mut records = vec![];
		for record in batch.iter() {
			let (kind, key, value) = record.unwrap();
			records.push((kind, key, value));
		}
		assert_eq!(records.len(), 5);

		assert_eq!(records[0].0, InternalKeyKind::Set);
		assert_eq!(records[0].1, b"key1");
		assert_eq!(records[0].2.unwrap(), b"value1");

		assert_eq!(records[1].0, InternalKeyKind::Delete);
		assert_eq!(records[1].1, b"key2");
		assert!(records[1].2.is_none());

		assert_eq!(records[2].0, InternalKeyKind::Set);
		assert_eq!(records[2].1, b"key3");
		assert_eq!(records[2].2.unwrap(), b"value3");

		assert_eq!(records[3].0, InternalKeyKind::Delete);
		assert_eq!(records[3].1, b"key1");
		assert!(records[3].2.is_none());

		assert_eq!(records[4].0, InternalKeyKind::Set);
		assert_eq!(records[4].1, b"key2");
		assert_eq!(records[4].2.unwrap(), b"new_value2");
	}

	#[test]
	fn test_iterator_zero_copy() {
		let mut batch = Batch::new();
		batch.set(b"k", b"v").unwrap();

		// Test that we can iterate over entries
		let (kind, key, value) = batch.iter().next().unwrap().unwrap();
		assert_eq!(kind, InternalKeyKind::Set);
		assert_eq!(key, b"k");
		assert_eq!(value.unwrap(), b"v");
	}

	#[test]
	fn test_reader_zero_copy() {
		let mut batch = Batch::new();
		batch.set(b"k", b"v").unwrap();
		let encoded = batch.encode(1).unwrap();
		let mut reader = BatchReader::new(&encoded).unwrap();
		let (kind, key, value) = reader.read_record().unwrap().unwrap();
		assert_eq!(kind, InternalKeyKind::Set);
		assert_eq!(key, b"k");
		assert_eq!(value.unwrap(), b"v");
	}

	#[test]
	fn test_batch_version() {
		let batch = Batch::new();
		assert_eq!(batch.version, BATCH_VERSION);

		let encoded = batch.encode(1).unwrap();
		let _reader = BatchReader::new(&encoded).unwrap();
		// Note: get_version method was removed as it was unused

		// Test with a batch that has data
		let mut batch_with_data = Batch::new();
		batch_with_data.set(b"key", b"value").unwrap();
		let encoded = batch_with_data.encode(100).unwrap();
		let reader = BatchReader::new(&encoded).unwrap();
		// Note: get_version method was removed as it was unused
		assert_eq!(reader.get_seq_num(), 100);
		// Note: get_count method was removed as it was unused
	}

	#[test]
	fn test_add_record_consistent_encoding() {
		// Test that None and Some(&[]) now encode identically (both as value_len=0)
		let mut batch_none = Batch::new();
		batch_none.add_record(InternalKeyKind::Delete, b"test_key", None).unwrap();

		let mut batch_empty = Batch::new();
		batch_empty.add_record(InternalKeyKind::Delete, b"test_key", Some(&[])).unwrap();

		let encoded_none = batch_none.encode(100).unwrap();
		let encoded_empty = batch_empty.encode(100).unwrap();

		// Now they should encode identically (both write value_len=0)
		assert_eq!(encoded_none, encoded_empty, "None and Some(&[]) should now encode identically");

		// Test reading them back - both should return None (since both encode as value_len=0)
		let mut reader_none = BatchReader::new(&encoded_none).unwrap();
		let (kind1, key1, value1) = reader_none.read_record().unwrap().unwrap();
		assert_eq!(kind1, InternalKeyKind::Delete);
		assert_eq!(key1, b"test_key");
		assert!(value1.is_none(), "None encodes as value_len=0, reads back as None");

		let mut reader_empty = BatchReader::new(&encoded_empty).unwrap();
		let (kind2, key2, value2) = reader_empty.read_record().unwrap().unwrap();
		assert_eq!(kind2, InternalKeyKind::Delete);
		assert_eq!(key2, b"test_key");
		assert!(value2.is_none(), "Some(&[]) also encodes as value_len=0, reads back as None");

		// Test with different operation types to ensure they all work
		let mut batch_merge = Batch::new();
		batch_merge.add_record(InternalKeyKind::Merge, b"merge_key", Some(b"merge_data")).unwrap();

		let encoded_merge = batch_merge.encode(300).unwrap();
		let mut reader_merge = BatchReader::new(&encoded_merge).unwrap();
		let (kind3, key3, value3) = reader_merge.read_record().unwrap().unwrap();
		assert_eq!(kind3, InternalKeyKind::Merge);
		assert_eq!(key3, b"merge_key");
		assert_eq!(value3, Some(b"merge_data".to_vec()), "Merge operations now work correctly");

		// Test with Set operations (should still work as before)
		let mut batch_set = Batch::new();
		batch_set.add_record(InternalKeyKind::Set, b"set_key", Some(b"set_data")).unwrap();

		let encoded_set = batch_set.encode(400).unwrap();
		let mut reader_set = BatchReader::new(&encoded_set).unwrap();
		let (kind4, key4, value4) = reader_set.read_record().unwrap().unwrap();
		assert_eq!(kind4, InternalKeyKind::Set);
		assert_eq!(key4, b"set_key");
		assert_eq!(value4, Some(b"set_data".to_vec()), "Set operations still work correctly");
	}
}
