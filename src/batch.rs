use bytes::Bytes;
use integer_encoding::{VarInt, VarIntWriter};

use crate::error::{Error, Result};
use crate::sstable::InternalKeyKind;
use crate::vlog::{ValuePointer, VALUE_POINTER_SIZE};

const MAX_BATCH_SIZE: u64 = 1 << 32;
const BATCH_VERSION: u8 = 1;
/// Represents a single entry in a batch
#[derive(Debug, Clone)]
pub(crate) struct BatchEntry {
	pub kind: InternalKeyKind,
	pub key: Bytes,
	pub value: Option<Bytes>,
	pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct Batch {
	version: u8,
	entries: Vec<BatchEntry>,
	valueptrs: Vec<Option<ValuePointer>>, // Parallel array to entries, None for inline values
	starting_seq_num: u64,                // Starting sequence number for this batch
	size: u64,                            // Total size of all records (not serialized)
}

impl Default for Batch {
	fn default() -> Self {
		Self::new(0)
	}
}

impl Batch {
	pub(crate) fn new(starting_seq_num: u64) -> Self {
		Self {
			entries: Vec::new(),
			valueptrs: Vec::new(),
			version: BATCH_VERSION,
			starting_seq_num,
			size: 0,
		}
	}

	// TODO: add a test for grow
	fn grow(&mut self, record_size: u64) -> Result<()> {
		if self.size + record_size > MAX_BATCH_SIZE {
			return Err(Error::BatchTooLarge);
		}
		self.size += record_size;
		self.entries.reserve(1);
		self.valueptrs.reserve(1);
		Ok(())
	}

	pub(crate) fn encode(&self) -> Result<Vec<u8>> {
		let mut encoded = Vec::new();

		// Write version (1 byte)
		encoded.push(self.version);

		// Write sequence number (8 bytes)
		encoded.write_varint(self.starting_seq_num)?;

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

			// Write timestamp (8 bytes)
			encoded.write_varint(entry.timestamp)?;
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
	pub(crate) fn set(&mut self, key: &[u8], value: &[u8], timestamp: u64) -> Result<()> {
		self.add_record(InternalKeyKind::Set, key, Some(value), timestamp)
	}

	#[cfg(test)]
	pub(crate) fn delete(&mut self, key: &[u8], timestamp: u64) -> Result<()> {
		self.add_record(InternalKeyKind::Delete, key, None, timestamp)
	}

	/// Internal method to add a record with optional value pointer
	fn add_record_internal(
		&mut self,
		kind: InternalKeyKind,
		key: &[u8],
		value: Option<&[u8]>,
		valueptr: Option<ValuePointer>,
		timestamp: u64,
	) -> Result<()> {
		let key_len = key.len();
		let value_len = value.map_or(0, |v| v.len());

		// Calculate the total size needed for this record
		let record_size = 1u64 + // kind
			(key_len as u64).required_space() as u64 +
			key_len as u64 +
			(value_len as u64).required_space() as u64 +
			value_len as u64 +
			8u64; // timestamp (8 bytes)

		self.grow(record_size)?;

		let entry = BatchEntry {
			kind,
			key: Bytes::copy_from_slice(key),
			value: value.map(Bytes::copy_from_slice),
			timestamp,
		};

		self.entries.push(entry);
		self.valueptrs.push(valueptr);

		Ok(())
	}

	pub(crate) fn add_record(
		&mut self,
		kind: InternalKeyKind,
		key: &[u8],
		value: Option<&[u8]>,
		timestamp: u64,
	) -> Result<()> {
		self.add_record_internal(kind, key, value, None, timestamp)
	}

	pub(crate) fn add_record_with_valueptr(
		&mut self,
		kind: InternalKeyKind,
		key: &[u8],
		value: Option<&[u8]>,
		valueptr: Option<ValuePointer>,
		timestamp: u64,
	) -> Result<()> {
		self.add_record_internal(kind, key, value, valueptr, timestamp)
	}

	pub(crate) fn count(&self) -> u32 {
		self.entries.len() as u32
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	/// Get entries for VLog processing
	#[cfg(test)]
	pub(crate) fn entries(&self) -> &[BatchEntry] {
		&self.entries
	}

	/// Set the starting sequence number for this batch
	pub(crate) fn set_starting_seq_num(&mut self, seq_num: u64) {
		self.starting_seq_num = seq_num;
	}

	/// Get the highest sequence number used in this batch
	pub(crate) fn get_highest_seq_num(&self) -> u64 {
		if self.entries.is_empty() {
			self.starting_seq_num
		} else {
			self.starting_seq_num + (self.entries.len() - 1) as u64
		}
	}

	/// Get an iterator over entries with their sequence numbers
	pub(crate) fn entries_with_seq_nums(
		&self,
	) -> Result<impl Iterator<Item = (usize, &BatchEntry, u64, u64)>> {
		Ok(self
			.entries
			.iter()
			.enumerate()
			.map(move |(i, entry)| (i, entry, self.starting_seq_num + i as u64, entry.timestamp)))
	}

	/// Decode a batch from encoded data
	pub(crate) fn decode(data: &[u8]) -> Result<Self> {
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
			let key = Bytes::copy_from_slice(&data[pos..pos + key_len as usize]);
			pos += key_len as usize;

			// Read value
			let (value_len, bytes_read) =
				u64::decode_var(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
			pos += bytes_read;
			let value = if value_len > 0 {
				let value_data = Bytes::copy_from_slice(&data[pos..pos + value_len as usize]);
				pos += value_len as usize;
				Some(value_data)
			} else {
				None
			};

			// Read timestamp
			let (timestamp, bytes_read) =
				u64::decode_var(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
			pos += bytes_read;

			entries.push(BatchEntry {
				kind,
				key,
				value,
				timestamp,
			});
		}

		// Read value pointers
		let mut valueptrs = Vec::with_capacity(count as usize);
		for _ in 0..count {
			let has_pointer = data[pos];
			pos += 1;
			let valueptr = if has_pointer == 1 {
				let ptr_data = &data[pos..pos + VALUE_POINTER_SIZE];
				pos += VALUE_POINTER_SIZE;
				Some(ValuePointer::decode(ptr_data)?)
			} else {
				None
			};
			valueptrs.push(valueptr);
		}

		Ok(Self {
			version,
			entries,
			valueptrs,
			starting_seq_num: seq_num,
			size: 0, // Decoded batches don't track size
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use test_log::test;

	#[test]
	fn test_batch_new() {
		let batch = Batch::new(0);
		assert_eq!(batch.entries.len(), 0);
		assert_eq!(batch.count(), 0);
	}

	#[test]
	fn test_batch_grow() {
		let mut batch = Batch::new(0);
		assert!(batch.grow(10).is_ok());
		assert!(batch.grow(MAX_BATCH_SIZE).is_err());
	}

	#[test]
	fn test_batch_encode() {
		let mut batch = Batch::new(1);
		batch.set(b"key1", b"value1", 0).unwrap();
		let encoded = batch.encode().unwrap();
		assert!(!encoded.is_empty());
	}

	#[test]
	fn test_batch_get_count() {
		let mut batch = Batch::new(0);
		assert_eq!(batch.count(), 0);
		batch.set(b"key1", b"value1", 0).unwrap();
		assert_eq!(batch.count(), 1);
	}

	#[test]
	fn test_batchreader_new() {
		let mut batch = Batch::new(100);
		batch.set(b"key1", b"value1", 0).unwrap();
		let encoded = batch.encode().unwrap();
		let reader = Batch::decode(&encoded).unwrap();
		assert_eq!(reader.starting_seq_num, 100);
	}

	#[test]
	fn test_batchreader_get_seq_num() {
		let batch = Batch::new(100);
		let encoded = batch.encode().unwrap();
		let reader = Batch::decode(&encoded).unwrap();
		assert_eq!(reader.starting_seq_num, 100);
	}

	#[test]
	fn test_batch_read_record() {
		let mut batch = Batch::new(1);
		batch.set(b"key1", b"value1", 1).unwrap();
		let encoded = batch.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();
		assert_eq!(decoded_batch.starting_seq_num, 1);

		let entries = decoded_batch.entries();
		assert_eq!(entries.len(), 1);
		assert_eq!(entries[0].kind, InternalKeyKind::Set);
		assert_eq!(entries[0].key.as_ref(), b"key1");
		assert_eq!(entries[0].value.as_ref().unwrap().as_ref(), b"value1");
		assert_eq!(entries[0].timestamp, 1);
	}

	#[test]
	fn test_batch_empty() {
		let batch = Batch::new(1);
		let encoded = batch.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();
		assert_eq!(decoded_batch.starting_seq_num, 1);
		assert!(decoded_batch.entries().is_empty());
	}

	#[test]
	fn test_batch_multiple_operations() {
		let mut batch = Batch::new(1);
		batch.set(b"key1", b"value1", 1).unwrap();
		batch.delete(b"key2", 2).unwrap();
		batch.set(b"key3", b"value3", 3).unwrap();

		assert_eq!(batch.count(), 3);

		let encoded = batch.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();
		assert_eq!(decoded_batch.starting_seq_num, 1);

		let entries = decoded_batch.entries();
		assert_eq!(entries.len(), 3);

		assert_eq!(entries[0].kind, InternalKeyKind::Set);
		assert_eq!(entries[0].key.as_ref(), b"key1");
		assert_eq!(entries[0].value.as_ref().unwrap().as_ref(), b"value1");
		assert_eq!(entries[0].timestamp, 1);

		assert_eq!(entries[1].kind, InternalKeyKind::Delete);
		assert_eq!(entries[1].key.as_ref(), b"key2");
		assert!(entries[1].value.is_none());
		assert_eq!(entries[1].timestamp, 2);

		assert_eq!(entries[2].kind, InternalKeyKind::Set);
		assert_eq!(entries[2].key.as_ref(), b"key3");
		assert_eq!(entries[2].value.as_ref().unwrap().as_ref(), b"value3");
		assert_eq!(entries[2].timestamp, 3);
	}

	#[test]
	fn test_batch_large_key_value() {
		let large_key = vec![b'a'; 1000000];
		let large_value = vec![b'b'; 1000000];

		let mut batch = Batch::new(1);
		batch.set(&large_key, &large_value, 1).unwrap();

		let encoded = batch.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();
		assert_eq!(decoded_batch.starting_seq_num, 1);

		let entries = decoded_batch.entries();
		assert_eq!(entries.len(), 1);
		assert_eq!(entries[0].kind, InternalKeyKind::Set);
		assert_eq!(entries[0].key.as_ref(), large_key);
		assert_eq!(entries[0].value.as_ref().unwrap().as_ref(), &large_value);
		assert_eq!(entries[0].timestamp, 1);
	}

	#[test]
	fn test_batch_max_size() {
		let mut batch = Batch::new(0);
		let key = vec![b'a'; 1000];
		let value = vec![b'b'; (MAX_BATCH_SIZE as usize) - 2000];

		assert!(batch.set(&key, &value, 0).is_ok());
		assert!(batch.set(&key, &[0], 0).is_err());
	}

	#[test]
	fn test_batch_iteration() {
		let mut batch = Batch::new(1);
		batch.set(b"key1", b"value1", 1).unwrap();
		batch.delete(b"key2", 2).unwrap();
		batch.set(b"key3", b"value3", 3).unwrap();

		let encoded = batch.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();
		assert_eq!(decoded_batch.starting_seq_num, 1);

		let entries = decoded_batch.entries();
		assert_eq!(entries.len(), 3);

		assert_eq!(entries[0].kind, InternalKeyKind::Set);
		assert_eq!(entries[0].key.as_ref(), b"key1");
		assert_eq!(entries[0].value.as_ref().unwrap().as_ref(), b"value1");
		assert_eq!(entries[0].timestamp, 1);

		assert_eq!(entries[1].kind, InternalKeyKind::Delete);
		assert_eq!(entries[1].key.as_ref(), b"key2");
		assert!(entries[1].value.is_none());
		assert_eq!(entries[1].timestamp, 2);

		assert_eq!(entries[2].kind, InternalKeyKind::Set);
		assert_eq!(entries[2].key.as_ref(), b"key3");
		assert_eq!(entries[2].value.as_ref().unwrap().as_ref(), b"value3");
		assert_eq!(entries[2].timestamp, 3);
	}

	#[test]
	fn test_batch_invalid_data() {
		let invalid_data = vec![0];
		assert!(Batch::decode(&invalid_data).is_err());
	}

	#[test]
	fn test_batch_empty_key_and_value() {
		let mut batch = Batch::new(1);
		batch.set(b"", b"", 0).unwrap();
		batch.delete(b"", 0).unwrap();

		let encoded = batch.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();
		assert_eq!(decoded_batch.starting_seq_num, 1);

		let entries = decoded_batch.entries();
		assert_eq!(entries.len(), 2);

		assert_eq!(entries[0].kind, InternalKeyKind::Set);
		assert_eq!(entries[0].key.as_ref(), b"");
		assert!(entries[1].value.is_none());

		assert_eq!(entries[1].kind, InternalKeyKind::Delete);
		assert_eq!(entries[1].key.as_ref(), b"");
		assert!(entries[1].value.is_none());
	}

	#[test]
	fn test_batch_unicode_keys_and_values() {
		let mut batch = Batch::new(1);
		batch.set("🔑".as_bytes(), "🗝️".as_bytes(), 1).unwrap();
		batch.set("こんにちは".as_bytes(), "世界".as_bytes(), 2).unwrap();

		let encoded = batch.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();
		assert_eq!(decoded_batch.starting_seq_num, 1);

		let entries = decoded_batch.entries();
		assert_eq!(entries.len(), 2);

		assert_eq!(entries[0].kind, InternalKeyKind::Set);
		assert_eq!(entries[0].key.as_ref(), "🔑".as_bytes());
		assert_eq!(entries[0].value.as_ref().unwrap().as_ref(), "🗝️".as_bytes());
		assert_eq!(entries[0].timestamp, 1);

		assert_eq!(entries[1].kind, InternalKeyKind::Set);
		assert_eq!(entries[1].key.as_ref(), "こんにちは".as_bytes());
		assert_eq!(entries[1].value.as_ref().unwrap().as_ref(), "世界".as_bytes());
		assert_eq!(entries[1].timestamp, 2);
	}

	#[test]
	fn test_batch_sequence_numbers() {
		let mut batch = Batch::new(100);
		batch.set(b"key1", b"value1", 1).unwrap();
		batch.set(b"key2", b"value2", 2).unwrap();

		let encoded = batch.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();

		assert_eq!(decoded_batch.starting_seq_num, 100);
	}

	#[test]
	fn test_batch_large_number_of_records() {
		const NUM_RECORDS: usize = 10000;
		let mut batch = Batch::new(1);

		for i in 0..NUM_RECORDS {
			let key = format!("key{i}");
			let value = format!("value{i}");
			if i % 2 == 0 {
				batch.set(key.as_bytes(), value.as_bytes(), i as u64).unwrap();
			} else {
				batch.delete(key.as_bytes(), i as u64).unwrap();
			}
		}

		assert_eq!(batch.count() as usize, NUM_RECORDS);

		let encoded = batch.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();
		assert_eq!(decoded_batch.starting_seq_num, 1);

		let entries = decoded_batch.entries();
		assert_eq!(entries.len(), NUM_RECORDS);

		for (i, entry) in entries.iter().enumerate().take(NUM_RECORDS) {
			let expected_key = format!("key{i}");

			if i % 2 == 0 {
				assert_eq!(entry.kind, InternalKeyKind::Set);
				assert_eq!(entry.key, expected_key.as_bytes());
				assert_eq!(entry.value.as_ref().unwrap(), format!("value{i}").as_bytes());
			} else {
				assert_eq!(entry.kind, InternalKeyKind::Delete);
				assert_eq!(entry.key, expected_key.as_bytes());
				assert!(entry.value.is_none());
			}
			assert_eq!(entry.timestamp, i as u64);
		}
	}

	#[test]
	fn test_batch_version() {
		let batch = Batch::new(1);
		assert_eq!(batch.version, BATCH_VERSION);

		let encoded = batch.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();
		assert_eq!(decoded_batch.starting_seq_num, 1);
		assert_eq!(decoded_batch.version, BATCH_VERSION);

		// Test with a batch that has data
		let mut batch_with_data = Batch::new(100);
		batch_with_data.set(b"key", b"value", 1).unwrap();
		let encoded = batch_with_data.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();
		assert_eq!(decoded_batch.starting_seq_num, 100);
		assert_eq!(decoded_batch.version, BATCH_VERSION);
	}

	#[test]
	fn test_add_record_consistent_encoding() {
		// Test that None and Some(&[]) now encode identically (both as value_len=0)
		let mut batch_none = Batch::new(100);
		batch_none.add_record(InternalKeyKind::Delete, b"test_key", None, 100).unwrap();

		let mut batch_empty = Batch::new(100);
		batch_empty.add_record(InternalKeyKind::Delete, b"test_key", Some(&[]), 100).unwrap();

		let encoded_none = batch_none.encode().unwrap();
		let encoded_empty = batch_empty.encode().unwrap();

		// Now they should encode identically (both write value_len=0)
		assert_eq!(encoded_none, encoded_empty, "None and Some(&[]) should now encode identically");

		// Test reading them back - both should return None (since both encode as value_len=0)
		let decoded_batch_none = Batch::decode(&encoded_none).unwrap();
		assert_eq!(decoded_batch_none.starting_seq_num, 100);
		let entries_none = decoded_batch_none.entries();
		assert_eq!(entries_none.len(), 1);
		assert_eq!(entries_none[0].kind, InternalKeyKind::Delete);
		assert_eq!(entries_none[0].key.as_ref(), b"test_key");
		assert!(entries_none[0].value.is_none(), "None encodes as value_len=0, reads back as None");
		assert_eq!(entries_none[0].timestamp, 100);

		let decoded_batch_empty = Batch::decode(&encoded_empty).unwrap();
		assert_eq!(decoded_batch_empty.starting_seq_num, 100);
		let entries_empty = decoded_batch_empty.entries();
		assert_eq!(entries_empty.len(), 1);
		assert_eq!(entries_empty[0].kind, InternalKeyKind::Delete);
		assert_eq!(entries_empty[0].key.as_ref(), b"test_key");
		assert!(
			entries_empty[0].value.is_none(),
			"Some(&[]) also encodes as value_len=0, reads back as None"
		);
		assert_eq!(entries_empty[0].timestamp, 100);

		// Test with different operation types to ensure they all work
		let mut batch_merge = Batch::new(300);
		batch_merge
			.add_record(InternalKeyKind::Merge, b"merge_key", Some(b"merge_data"), 300)
			.unwrap();

		let encoded_merge = batch_merge.encode().unwrap();
		let decoded_batch_merge = Batch::decode(&encoded_merge).unwrap();
		assert_eq!(decoded_batch_merge.starting_seq_num, 300);
		let entries_merge = decoded_batch_merge.entries();
		assert_eq!(entries_merge.len(), 1);
		assert_eq!(entries_merge[0].kind, InternalKeyKind::Merge);
		assert_eq!(entries_merge[0].key.as_ref(), b"merge_key");
		assert_eq!(entries_merge[0].value.as_ref().unwrap().as_ref(), b"merge_data");
		assert_eq!(entries_merge[0].timestamp, 300);

		// Test with Set operations (should still work as before)
		let mut batch_set = Batch::new(400);
		batch_set.add_record(InternalKeyKind::Set, b"set_key", Some(b"set_data"), 400).unwrap();

		let encoded_set = batch_set.encode().unwrap();
		let decoded_batch_set = Batch::decode(&encoded_set).unwrap();
		assert_eq!(decoded_batch_set.starting_seq_num, 400);
		let entries_set = decoded_batch_set.entries();
		assert_eq!(entries_set.len(), 1);
		assert_eq!(entries_set[0].kind, InternalKeyKind::Set);
		assert_eq!(entries_set[0].key.as_ref(), b"set_key");
		assert_eq!(entries_set[0].value.as_ref().unwrap().as_ref(), b"set_data");
		assert_eq!(entries_set[0].timestamp, 400);
	}

	#[test]
	fn test_batch_encode_decode() {
		// Create a batch with all possible variations
		let mut batch = Batch::new(12345);

		// Add various types of records with different value pointer scenarios
		batch.add_record(InternalKeyKind::Set, b"key1", Some(b"value1"), 1).unwrap();
		batch.add_record(InternalKeyKind::Delete, b"key2", None, 2).unwrap();
		batch.add_record(InternalKeyKind::Merge, b"key3", Some(b"merge_value"), 3).unwrap();

		// Add records with value pointers (simulating VLog pointers)
		let valueptr1 = ValuePointer::new(100, 200, 10, 20, 30);
		let valueptr2 = ValuePointer::new(400, 500, 15, 25, 35);

		batch
			.add_record_with_valueptr(
				InternalKeyKind::Set,
				b"key4",
				Some(b"large_value"),
				Some(valueptr1),
				4,
			)
			.unwrap();
		batch
			.add_record_with_valueptr(InternalKeyKind::Set, b"key5", None, Some(valueptr2), 5)
			.unwrap();
		batch.add_record_with_valueptr(InternalKeyKind::Delete, b"key6", None, None, 6).unwrap();

		// Add some edge cases
		batch.add_record(InternalKeyKind::Set, b"", Some(b"empty_key_value"), 7).unwrap();
		batch.add_record(InternalKeyKind::Set, b"empty_value_key", Some(b""), 8).unwrap();
		batch.add_record(InternalKeyKind::Set, b"", Some(b""), 9).unwrap();

		// Add unicode data
		batch.add_record(InternalKeyKind::Set, "🔑".as_bytes(), Some("🗝️".as_bytes()), 10).unwrap();
		batch
			.add_record(InternalKeyKind::Set, "こんにちは".as_bytes(), Some("世界".as_bytes()), 11)
			.unwrap();

		// Verify original batch properties
		assert_eq!(batch.starting_seq_num, 12345);
		assert_eq!(batch.count(), 11);
		assert!(!batch.is_empty());
		assert_eq!(batch.get_highest_seq_num(), 12345 + 10); // starting + count - 1

		// Encode the batch
		let encoded = batch.encode().unwrap();
		assert!(!encoded.is_empty());

		// Decode the batch
		let decoded_batch = Batch::decode(&encoded).unwrap();

		// Verify all properties are preserved
		assert_eq!(decoded_batch.version, BATCH_VERSION);
		assert_eq!(decoded_batch.starting_seq_num, 12345);
		assert_eq!(decoded_batch.count(), 11);
		assert!(!decoded_batch.is_empty());
		assert_eq!(decoded_batch.get_highest_seq_num(), 12345 + 10);

		// Verify entries are preserved correctly
		let entries = decoded_batch.entries();
		assert_eq!(entries.len(), 11);

		// Check first few entries
		assert_eq!(entries[0].kind, InternalKeyKind::Set);
		assert_eq!(entries[0].key.as_ref(), b"key1");
		assert_eq!(entries[0].value.as_ref().unwrap().as_ref(), b"value1");
		assert_eq!(entries[0].timestamp, 1);

		assert_eq!(entries[1].kind, InternalKeyKind::Delete);
		assert_eq!(entries[1].key.as_ref(), b"key2");
		assert!(entries[1].value.is_none());
		assert_eq!(entries[1].timestamp, 2);

		assert_eq!(entries[2].kind, InternalKeyKind::Merge);
		assert_eq!(entries[2].key.as_ref(), b"key3");
		assert_eq!(entries[2].value.as_ref().unwrap().as_ref(), b"merge_value");
		assert_eq!(entries[2].timestamp, 3);

		// Check entries with value pointers
		assert_eq!(entries[3].kind, InternalKeyKind::Set);
		assert_eq!(entries[3].key.as_ref(), b"key4");
		assert_eq!(entries[3].value.as_ref().unwrap().as_ref(), b"large_value");
		assert_eq!(entries[3].timestamp, 4);

		assert_eq!(entries[4].kind, InternalKeyKind::Set);
		assert_eq!(entries[4].key.as_ref(), b"key5");
		assert!(entries[4].value.is_none());
		assert_eq!(entries[4].timestamp, 5);

		assert_eq!(entries[5].kind, InternalKeyKind::Delete);
		assert_eq!(entries[5].key.as_ref(), b"key6");
		assert!(entries[5].value.is_none());
		assert_eq!(entries[5].timestamp, 6);

		// Check edge cases
		assert_eq!(entries[6].kind, InternalKeyKind::Set);
		assert_eq!(entries[6].key.as_ref(), b"");
		assert_eq!(entries[6].value.as_ref().unwrap().as_ref(), b"empty_key_value");
		assert_eq!(entries[6].timestamp, 7);

		assert_eq!(entries[7].kind, InternalKeyKind::Set);
		assert_eq!(entries[7].key.as_ref(), b"empty_value_key");
		// Empty string values decode as None (this is the intended behavior)
		assert!(entries[7].value.is_none());
		assert_eq!(entries[7].timestamp, 8);

		assert_eq!(entries[8].kind, InternalKeyKind::Set);
		assert_eq!(entries[8].key.as_ref(), b"");
		// Empty string values decode as None (this is the intended behavior)
		assert!(entries[8].value.is_none());
		assert_eq!(entries[8].timestamp, 9);

		// Check unicode entries
		assert_eq!(entries[9].kind, InternalKeyKind::Set);
		assert_eq!(entries[9].key.as_ref(), "🔑".as_bytes());
		assert_eq!(entries[9].value.as_ref().unwrap().as_ref(), "🗝️".as_bytes());
		assert_eq!(entries[9].timestamp, 10);

		assert_eq!(entries[10].kind, InternalKeyKind::Set);
		assert_eq!(entries[10].key.as_ref(), "こんにちは".as_bytes());
		assert_eq!(entries[10].value.as_ref().unwrap().as_ref(), "世界".as_bytes());
		assert_eq!(entries[10].timestamp, 11);

		// Verify value pointers are preserved correctly
		assert_eq!(decoded_batch.entries.len(), decoded_batch.valueptrs.len());

		// Test sequence number iteration
		let entries_with_seq_nums: Vec<_> =
			decoded_batch.entries_with_seq_nums().unwrap().collect();
		assert_eq!(entries_with_seq_nums.len(), 11);

		for (i, (entry_idx, entry, seq_num, timestamp)) in entries_with_seq_nums.iter().enumerate()
		{
			assert_eq!(*entry_idx, i);
			assert_eq!(*seq_num, 12345 + i as u64);
			assert_eq!(entry.kind, entries[i].kind);
			assert_eq!(entry.key, entries[i].key);
			assert_eq!(entry.value, entries[i].value);
			assert_eq!(entry.timestamp, *timestamp);
		}

		// Test that we can re-encode the decoded batch and get the same result
		let re_encoded = decoded_batch.encode().unwrap();
		assert_eq!(encoded, re_encoded, "Re-encoding should produce identical result");
	}
}
