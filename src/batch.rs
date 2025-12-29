use integer_encoding::{VarInt, VarIntWriter};

use crate::error::{Error, Result};
use crate::sstable::InternalKey;
use crate::vlog::{ValuePointer, VALUE_POINTER_SIZE};
#[cfg(test)]
use crate::UserKey;
use crate::Value;

const MAX_BATCH_SIZE: u64 = 1 << 32;
const BATCH_VERSION: u8 = 1;
/// Represents a single entry in a batch
#[derive(Debug, Clone)]
pub(crate) struct BatchEntry {
	pub key: InternalKey,
	pub value: Option<Value>,
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
	pub(crate) const fn new(starting_seq_num: u64) -> Self {
		Self {
			entries: Vec::new(),
			valueptrs: Vec::new(),
			version: BATCH_VERSION,
			starting_seq_num,
			size: 0,
		}
	}

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
		let mut encoded = Vec::with_capacity(
			1 + // version
			self.starting_seq_num.required_space() +
			(self.entries.len() as u32).required_space() +
			self.size as usize +
			(self.valueptrs.len() * VALUE_POINTER_SIZE),
		);

		// Write version (1 byte)
		encoded.push(self.version);

		// Write sequence number (8 bytes)
		encoded.write_varint(self.starting_seq_num)?;

		// Write count (4 bytes)
		encoded.write_varint(self.entries.len() as u32)?;

		// Write entries
		for entry in &self.entries {
			// Write key length and key
			encoded.write_varint(entry.key.len() as u64)?;
			encoded.extend_from_slice(&entry.key[..]);

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
					ptr.encode_into(&mut encoded);
				}
				None => {
					encoded.push(0); // No pointer (inline value)
				}
			}
		}

		Ok(encoded)
	}

	#[cfg(test)]
	pub(crate) fn set(&mut self, key: UserKey, value: Value, timestamp: u64) -> Result<()> {
		use crate::sstable::InternalKeyKind;
		self.add_record(InternalKey::encode(key, 0, InternalKeyKind::Set, timestamp), Some(value))
	}

	#[cfg(test)]
	pub(crate) fn delete(&mut self, key: UserKey, timestamp: u64) -> Result<()> {
		use crate::sstable::InternalKeyKind;
		self.add_record(InternalKey::encode(key, 0, InternalKeyKind::Delete, timestamp), None)
	}

	/// Internal method to add a record with optional value pointer
	fn add_record_internal(
		&mut self,
		key: InternalKey,
		value: Option<Value>,
		valueptr: Option<ValuePointer>,
	) -> Result<()> {
		let key_len = key.len() as u64;
		let value_len = value.as_ref().map_or(0, |v| v.len()) as u64;

		// Calculate the total size needed for this record
		let record_size = key_len.required_space() as u64
			+ key_len + value_len.required_space() as u64
			+ value_len;

		self.grow(record_size)?;

		let entry = BatchEntry {
			key,
			value,
		};

		self.entries.push(entry);
		self.valueptrs.push(valueptr);

		Ok(())
	}

	pub(crate) fn add_record(&mut self, key: InternalKey, value: Option<Value>) -> Result<()> {
		self.add_record_internal(key, value, None)
	}

	pub(crate) fn add_record_with_valueptr(
		&mut self,
		key: InternalKey,
		value: Option<Value>,
		valueptr: Option<ValuePointer>,
	) -> Result<()> {
		self.add_record_internal(key, value, valueptr)
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
	pub(crate) const fn set_starting_seq_num(&mut self, seq_num: u64) {
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
	pub(crate) fn entries_with_seq_nums(self) -> Result<impl Iterator<Item = BatchEntry>> {
		Ok(self.entries.into_iter().enumerate().map(
			move |(
				i,
				BatchEntry {
					key,
					value,
				},
			)| {
				BatchEntry {
					key: key.with_seq_num(self.starting_seq_num + i as u64),
					value,
				}
			},
		))
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
			// Read key
			let (key_len, bytes_read) =
				u64::decode_var(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
			pos += bytes_read;
			let key = InternalKey::new(data[pos..pos + key_len as usize].to_vec());
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
	use test_log::test;

	use super::*;
	use crate::sstable::InternalKeyKind;

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
		batch.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
		let encoded = batch.encode().unwrap();
		assert!(!encoded.is_empty());
	}

	#[test]
	fn test_batch_get_count() {
		let mut batch = Batch::new(0);
		assert_eq!(batch.count(), 0);
		batch.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
		assert_eq!(batch.count(), 1);
	}

	#[test]
	fn test_batchreader_new() {
		let mut batch = Batch::new(100);
		batch.set(b"key1".to_vec(), b"value1".to_vec(), 0).unwrap();
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
		batch.set(b"key1".to_vec(), b"value1".to_vec(), 1).unwrap();
		let encoded = batch.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();
		assert_eq!(decoded_batch.starting_seq_num, 1);

		let entries = decoded_batch.entries();
		assert_eq!(entries.len(), 1);
		assert_eq!(entries[0].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries[0].key.user_key(), b"key1");
		assert_eq!(entries[0].value.as_ref().unwrap().as_slice(), b"value1");
		assert_eq!(entries[0].key.timestamp(), 1);
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
		batch.set(b"key1".to_vec(), b"value1".to_vec(), 1).unwrap();
		batch.delete(b"key2".to_vec(), 2).unwrap();
		batch.set(b"key3".to_vec(), b"value3".to_vec(), 3).unwrap();

		assert_eq!(batch.count(), 3);

		let encoded = batch.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();
		assert_eq!(decoded_batch.starting_seq_num, 1);

		let entries = decoded_batch.entries();
		assert_eq!(entries.len(), 3);

		assert_eq!(entries[0].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries[0].key.user_key(), b"key1");
		assert_eq!(entries[0].value.as_ref().unwrap().as_slice(), b"value1");
		assert_eq!(entries[0].key.timestamp(), 1);

		assert_eq!(entries[1].key.kind(), InternalKeyKind::Delete);
		assert_eq!(entries[1].key.user_key(), b"key2");
		assert!(entries[1].value.is_none());
		assert_eq!(entries[1].key.timestamp(), 2);

		assert_eq!(entries[2].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries[2].key.user_key(), b"key3");
		assert_eq!(entries[2].value.as_ref().unwrap().as_slice(), b"value3");
		assert_eq!(entries[2].key.timestamp(), 3);
	}

	#[test]
	fn test_batch_large_key_value() {
		let large_key = vec![b'a'; 1000000];
		let large_value = vec![b'b'; 1000000];

		let mut batch = Batch::new(1);
		batch.set(large_key.clone(), large_value.clone(), 1).unwrap();

		let encoded = batch.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();
		assert_eq!(decoded_batch.starting_seq_num, 1);

		let entries = decoded_batch.entries();
		assert_eq!(entries.len(), 1);
		assert_eq!(entries[0].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries[0].key.user_key(), &large_key);
		assert_eq!(entries[0].value.as_ref().unwrap().as_slice(), &large_value);
		assert_eq!(entries[0].key.timestamp(), 1);
	}

	#[test]
	fn test_batch_max_size() {
		let mut batch = Batch::new(0);
		let key = vec![b'a'; 1000];
		let value = vec![b'b'; (MAX_BATCH_SIZE as usize) - 2000];

		assert!(batch.set(key.clone(), value, 0).is_ok());
		assert!(batch.set(key, vec![0], 0).is_err());
	}

	#[test]
	fn test_batch_iteration() {
		let mut batch = Batch::new(1);
		batch.set(b"key1".to_vec(), b"value1".to_vec(), 1).unwrap();
		batch.delete(b"key2".to_vec(), 2).unwrap();
		batch.set(b"key3".to_vec(), b"value3".to_vec(), 3).unwrap();

		let encoded = batch.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();
		assert_eq!(decoded_batch.starting_seq_num, 1);

		let entries = decoded_batch.entries();
		assert_eq!(entries.len(), 3);

		assert_eq!(entries[0].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries[0].key.user_key(), b"key1");
		assert_eq!(entries[0].value.as_ref().unwrap().as_slice(), b"value1");
		assert_eq!(entries[0].key.timestamp(), 1);

		assert_eq!(entries[1].key.kind(), InternalKeyKind::Delete);
		assert_eq!(entries[1].key.user_key(), b"key2");
		assert!(entries[1].value.is_none());
		assert_eq!(entries[1].key.timestamp(), 2);

		assert_eq!(entries[2].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries[2].key.user_key(), b"key3");
		assert_eq!(entries[2].value.as_ref().unwrap().as_slice(), b"value3");
		assert_eq!(entries[2].key.timestamp(), 3);
	}

	#[test]
	fn test_batch_invalid_data() {
		let invalid_data = vec![0];
		assert!(Batch::decode(&invalid_data).is_err());
	}

	#[test]
	fn test_batch_empty_key_and_value() {
		let mut batch = Batch::new(1);
		batch.set(b"".to_vec(), b"".to_vec(), 0).unwrap();
		batch.delete(b"".to_vec(), 0).unwrap();

		let encoded = batch.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();
		assert_eq!(decoded_batch.starting_seq_num, 1);

		let entries = decoded_batch.entries();
		assert_eq!(entries.len(), 2);

		assert_eq!(entries[0].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries[0].key.user_key(), b"");
		assert!(entries[1].value.is_none());

		assert_eq!(entries[1].key.kind(), InternalKeyKind::Delete);
		assert_eq!(entries[1].key.user_key(), b"");
		assert!(entries[1].value.is_none());
	}

	#[test]
	fn test_batch_unicode_keys_and_values() {
		let mut batch = Batch::new(1);
		batch.set("🔑".as_bytes().to_vec(), "🗝️".as_bytes().to_vec(), 1).unwrap();
		batch.set("こんにちは".as_bytes().to_vec(), "世界".as_bytes().to_vec(), 2).unwrap();

		let encoded = batch.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();
		assert_eq!(decoded_batch.starting_seq_num, 1);

		let entries = decoded_batch.entries();
		assert_eq!(entries.len(), 2);

		assert_eq!(entries[0].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries[0].key.user_key(), "🔑".as_bytes());
		assert_eq!(entries[0].value.as_ref().unwrap().as_slice(), "🗝️".as_bytes());
		assert_eq!(entries[0].key.timestamp(), 1);

		assert_eq!(entries[1].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries[1].key.user_key(), "こんにちは".as_bytes());
		assert_eq!(entries[1].value.as_ref().unwrap().as_slice(), "世界".as_bytes());
		assert_eq!(entries[1].key.timestamp(), 2);
	}

	#[test]
	fn test_batch_sequence_numbers() {
		let mut batch = Batch::new(100);
		batch.set(b"key1".to_vec(), b"value1".to_vec(), 1).unwrap();
		batch.set(b"key2".to_vec(), b"value2".to_vec(), 2).unwrap();

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
				batch.set(key.as_bytes().to_vec(), value.as_bytes().to_vec(), i as u64).unwrap();
			} else {
				batch.delete(key.as_bytes().to_vec(), i as u64).unwrap();
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
				assert_eq!(entry.key.kind(), InternalKeyKind::Set);
				assert_eq!(entry.key.user_key(), expected_key.as_bytes());
				assert_eq!(entry.value.as_ref().unwrap(), format!("value{i}").as_bytes());
			} else {
				assert_eq!(entry.key.kind(), InternalKeyKind::Delete);
				assert_eq!(entry.key.user_key(), expected_key.as_bytes());
				assert!(entry.value.is_none());
			}
			assert_eq!(entry.key.timestamp(), i as u64);
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
		batch_with_data.set(b"key".to_vec(), b"value".to_vec(), 1).unwrap();
		let encoded = batch_with_data.encode().unwrap();
		let decoded_batch = Batch::decode(&encoded).unwrap();
		assert_eq!(decoded_batch.starting_seq_num, 100);
		assert_eq!(decoded_batch.version, BATCH_VERSION);
	}

	#[test]
	fn test_add_record_consistent_encoding() {
		// Test that None and Some(&[]) now encode identically (both as value_len=0)
		let mut batch_none = Batch::new(100);
		batch_none
			.add_record(
				InternalKey::encode(b"test_key".to_vec(), 0, InternalKeyKind::Delete, 100),
				None,
			)
			.unwrap();

		let mut batch_empty = Batch::new(100);
		batch_empty
			.add_record(
				InternalKey::encode(b"test_key".to_vec(), 0, InternalKeyKind::Delete, 100),
				Some(vec![]),
			)
			.unwrap();

		let encoded_none = batch_none.encode().unwrap();
		let encoded_empty = batch_empty.encode().unwrap();

		// Now they should encode identically (both write value_len=0)
		assert_eq!(encoded_none, encoded_empty, "None and Some(&[]) should now encode identically");

		// Test reading them back - both should return None (since both encode as
		// value_len=0)
		let decoded_batch_none = Batch::decode(&encoded_none).unwrap();
		assert_eq!(decoded_batch_none.starting_seq_num, 100);
		let entries_none = decoded_batch_none.entries();
		assert_eq!(entries_none.len(), 1);
		assert_eq!(entries_none[0].key.kind(), InternalKeyKind::Delete);
		assert_eq!(entries_none[0].key.user_key(), b"test_key");
		assert!(entries_none[0].value.is_none(), "None encodes as value_len=0, reads back as None");
		assert_eq!(entries_none[0].key.timestamp(), 100);

		let decoded_batch_empty = Batch::decode(&encoded_empty).unwrap();
		assert_eq!(decoded_batch_empty.starting_seq_num, 100);
		let entries_empty = decoded_batch_empty.entries();
		assert_eq!(entries_empty.len(), 1);
		assert_eq!(entries_empty[0].key.kind(), InternalKeyKind::Delete);
		assert_eq!(entries_empty[0].key.user_key(), b"test_key");
		assert!(
			entries_empty[0].value.is_none(),
			"Some(&[]) also encodes as value_len=0, reads back as None"
		);
		assert_eq!(entries_empty[0].key.timestamp(), 100);

		// Test with different operation types to ensure they all work
		let mut batch_merge = Batch::new(300);
		batch_merge
			.add_record(
				InternalKey::encode(b"merge_key".to_vec(), 0, InternalKeyKind::Merge, 300),
				Some(b"merge_data".to_vec()),
			)
			.unwrap();

		let encoded_merge = batch_merge.encode().unwrap();
		let decoded_batch_merge = Batch::decode(&encoded_merge).unwrap();
		assert_eq!(decoded_batch_merge.starting_seq_num, 300);
		let entries_merge = decoded_batch_merge.entries();
		assert_eq!(entries_merge.len(), 1);
		assert_eq!(entries_merge[0].key.kind(), InternalKeyKind::Merge);
		assert_eq!(entries_merge[0].key.user_key(), b"merge_key");
		assert_eq!(entries_merge[0].value.as_ref().unwrap().as_slice(), b"merge_data");
		assert_eq!(entries_merge[0].key.timestamp(), 300);

		// Test with Set operations (should still work as before)
		let mut batch_set = Batch::new(400);
		batch_set
			.add_record(
				InternalKey::encode(b"set_key".to_vec(), 0, InternalKeyKind::Set, 400),
				Some(b"set_data".to_vec()),
			)
			.unwrap();

		let encoded_set = batch_set.encode().unwrap();
		let decoded_batch_set = Batch::decode(&encoded_set).unwrap();
		assert_eq!(decoded_batch_set.starting_seq_num, 400);
		let entries_set = decoded_batch_set.entries();
		assert_eq!(entries_set.len(), 1);
		assert_eq!(entries_set[0].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries_set[0].key.user_key(), b"set_key");
		assert_eq!(entries_set[0].value.as_ref().unwrap().as_slice(), b"set_data");
		assert_eq!(entries_set[0].key.timestamp(), 400);
	}

	#[test]
	fn test_batch_encode_decode() {
		// Create a batch with all possible variations
		let mut batch = Batch::new(12345);

		// Add various types of records with different value pointer scenarios
		batch
			.add_record(
				InternalKey::encode(b"key1".to_vec(), 0, InternalKeyKind::Set, 1),
				Some(b"value1".to_vec()),
			)
			.unwrap();
		batch
			.add_record(InternalKey::encode(b"key2".to_vec(), 0, InternalKeyKind::Delete, 2), None)
			.unwrap();
		batch
			.add_record(
				InternalKey::encode(b"key3".to_vec(), 0, InternalKeyKind::Merge, 3),
				Some(b"merge_value".to_vec()),
			)
			.unwrap();

		// Add records with value pointers (simulating VLog pointers)
		let valueptr1 = ValuePointer::new(100, 200, 10, 20, 30);
		let valueptr2 = ValuePointer::new(400, 500, 15, 25, 35);

		batch
			.add_record_with_valueptr(
				InternalKey::encode(b"key4".to_vec(), 0, InternalKeyKind::Set, 4),
				Some(b"large_value".to_vec()),
				Some(valueptr1),
			)
			.unwrap();
		batch
			.add_record_with_valueptr(
				InternalKey::encode(b"key5".to_vec(), 0, InternalKeyKind::Set, 5),
				None,
				Some(valueptr2),
			)
			.unwrap();
		batch
			.add_record_with_valueptr(
				InternalKey::encode(b"key6".to_vec(), 0, InternalKeyKind::Delete, 6),
				None,
				None,
			)
			.unwrap();

		// Add some edge cases
		batch
			.add_record(
				InternalKey::encode(b"".to_vec(), 0, InternalKeyKind::Set, 7),
				Some(b"empty_key_value".to_vec()),
			)
			.unwrap();
		batch
			.add_record(
				InternalKey::encode(b"empty_value_key".to_vec(), 0, InternalKeyKind::Set, 8),
				Some(b"".to_vec()),
			)
			.unwrap();
		batch
			.add_record(
				InternalKey::encode(b"".to_vec(), 0, InternalKeyKind::Set, 9),
				Some(b"".to_vec()),
			)
			.unwrap();

		// Add unicode data
		batch
			.add_record(
				InternalKey::encode("🔑".as_bytes().to_vec(), 0, InternalKeyKind::Set, 10),
				Some("🗝️".as_bytes().to_vec()),
			)
			.unwrap();
		batch
			.add_record(
				InternalKey::encode("こんにちは".as_bytes().to_vec(), 0, InternalKeyKind::Set, 11),
				Some("世界".as_bytes().to_vec()),
			)
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
		assert_eq!(entries[0].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries[0].key.user_key(), b"key1");
		assert_eq!(entries[0].value.as_ref().unwrap().as_slice(), b"value1");
		assert_eq!(entries[0].key.timestamp(), 1);

		assert_eq!(entries[1].key.kind(), InternalKeyKind::Delete);
		assert_eq!(entries[1].key.user_key(), b"key2");
		assert!(entries[1].value.is_none());
		assert_eq!(entries[1].key.timestamp(), 2);

		assert_eq!(entries[2].key.kind(), InternalKeyKind::Merge);
		assert_eq!(entries[2].key.user_key(), b"key3");
		assert_eq!(entries[2].value.as_ref().unwrap().as_slice(), b"merge_value");
		assert_eq!(entries[2].key.timestamp(), 3);

		// Check entries with value pointers
		assert_eq!(entries[3].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries[3].key.user_key(), b"key4");
		assert_eq!(entries[3].value.as_ref().unwrap().as_slice(), b"large_value");
		assert_eq!(entries[3].key.timestamp(), 4);

		assert_eq!(entries[4].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries[4].key.user_key(), b"key5");
		assert!(entries[4].value.is_none());
		assert_eq!(entries[4].key.timestamp(), 5);

		assert_eq!(entries[5].key.kind(), InternalKeyKind::Delete);
		assert_eq!(entries[5].key.user_key(), b"key6");
		assert!(entries[5].value.is_none());
		assert_eq!(entries[5].key.timestamp(), 6);

		// Check edge cases
		assert_eq!(entries[6].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries[6].key.user_key(), b"");
		assert_eq!(entries[6].value.as_ref().unwrap().as_slice(), b"empty_key_value");
		assert_eq!(entries[6].key.timestamp(), 7);

		assert_eq!(entries[7].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries[7].key.user_key(), b"empty_value_key");
		// Empty string values decode as None (this is the intended behavior)
		assert!(entries[7].value.is_none());
		assert_eq!(entries[7].key.timestamp(), 8);

		assert_eq!(entries[8].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries[8].key.user_key(), b"");
		// Empty string values decode as None (this is the intended behavior)
		assert!(entries[8].value.is_none());
		assert_eq!(entries[8].key.timestamp(), 9);

		// Check unicode entries
		assert_eq!(entries[9].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries[9].key.user_key(), "🔑".as_bytes());
		assert_eq!(entries[9].value.as_ref().unwrap().as_slice(), "🗝️".as_bytes());
		assert_eq!(entries[9].key.timestamp(), 10);

		assert_eq!(entries[10].key.kind(), InternalKeyKind::Set);
		assert_eq!(entries[10].key.user_key(), "こんにちは".as_bytes());
		assert_eq!(entries[10].value.as_ref().unwrap().as_slice(), "世界".as_bytes());
		assert_eq!(entries[10].key.timestamp(), 11);

		// Verify value pointers are preserved correctly
		assert_eq!(decoded_batch.entries.len(), decoded_batch.valueptrs.len());

		// Test sequence number iteration
		let entries_with_seq_nums: Vec<_> =
			decoded_batch.clone().entries_with_seq_nums().unwrap().collect();
		assert_eq!(entries_with_seq_nums.len(), 11);

		for (i, entry) in entries_with_seq_nums.iter().enumerate() {
			assert_eq!(entry.key.kind(), entries[i].key.kind());
			assert_eq!(entry.key, entries[i].key);
			assert_eq!(entry.value, entries[i].value);
			assert_eq!(entry.key.timestamp(), entries[i].key.timestamp());
		}

		// Test that we can re-encode the decoded batch and get the same result
		let re_encoded = decoded_batch.encode().unwrap();
		assert_eq!(encoded, re_encoded, "Re-encoding should produce identical result");
	}

	// record size that's passed to grow should match what's encoded in the batch
	#[test]
	fn test_add_record_internal_passes_correct_record_size_to_grow() {}
}
