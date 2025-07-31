use integer_encoding::{VarInt, VarIntWriter};

use crate::error::{Error, Result};
use crate::sstable::InternalKeyKind;

const MAX_BATCH_SIZE: u64 = 1 << 32; // Adjust as needed

type RecordKey<'a> = (InternalKeyKind, &'a [u8], Option<&'a [u8]>);
type RecordResult<'a> = Result<Option<RecordKey<'a>>>;

#[derive(Debug, Clone)]
pub struct Batch {
	data: Vec<u8>,
	count: u32,
}

impl Default for Batch {
	fn default() -> Self {
		Self::new()
	}
}

impl Batch {
	pub fn new() -> Self {
		Self {
			data: Vec::new(),
			count: 0,
		}
	}

	// TODO: add a test for grow
	fn grow(&mut self, n: usize) -> Result<()> {
		let new_size = self.data.len() + n;
		if new_size as u64 >= MAX_BATCH_SIZE {
			return Err(Error::BatchTooLarge);
		}
		self.data.reserve(n);
		Ok(())
	}

	pub fn encode(&self, seq_num: u64) -> Result<Vec<u8>> {
		let mut encoded = Vec::with_capacity(self.data.len());

		// Write sequence number (8 bytes)
		encoded.write_varint(seq_num)?;

		// Write count (4 bytes)
		encoded.write_varint(self.count)?;

		// Write data records
		encoded.extend_from_slice(&self.data[..]);

		Ok(encoded)
	}

	pub fn get_count(&self) -> u32 {
		self.count
	}

	pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
		self.add_record(InternalKeyKind::Set, key, Some(value))
	}

	pub fn delete(&mut self, key: &[u8]) -> Result<()> {
		self.add_record(InternalKeyKind::Delete, key, None)
	}

	pub(crate) fn add_record(
		&mut self,
		kind: InternalKeyKind,
		key: &[u8],
		value: Option<&[u8]>,
	) -> Result<()> {
		let key_len = key.len();
		let value_len = value.map_or(0, |v| v.len());

		// Calculate the total size needed for this record
		let record_size = 1 + // kind
            (key_len as u64).required_space() +
            key_len +
            (value_len as u64).required_space() +
            value_len;

		self.grow(record_size)?;

		// Write the record
		self.data.push(kind as u8);
		self.data.write_varint(key_len as u64)?;
		self.data.extend_from_slice(key);
		self.data.write_varint(value_len as u64)?;
		if let Some(v) = value {
			self.data.extend_from_slice(v);
		}

		self.count += 1;

		Ok(())
	}

	pub fn iter(&self) -> BatchIterator<'_> {
		BatchIterator {
			data: &self.data,
			pos: 0,
		}
	}

	pub fn count(&self) -> u32 {
		self.count
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
	}
}

pub struct BatchIterator<'a> {
	data: &'a [u8],
	pos: usize,
}

/// Helper function to decode a single record from batch data
/// Returns: Ok(Some((kind, key, value))) for successful decode,
///          Ok(None) for end of data,
///          Err(...) for decode errors
fn decode_record_at<'a>(data: &'a [u8], pos: &mut usize) -> RecordResult<'a> {
	if *pos >= data.len() {
		return Ok(None);
	}

	// Read kind byte
	let kind = InternalKeyKind::from(data[*pos]);
	*pos += 1;
	if kind == InternalKeyKind::Invalid {
		return Err(Error::InvalidBatchRecord);
	}

	// Read key length
	let (key_len, bytes_read) = u64::decode_var(&data[*pos..]).ok_or(Error::InvalidBatchRecord)?;
	*pos += bytes_read;

	// Read key bytes
	let key_start = *pos;
	let key_end = key_start + key_len as usize;
	if key_end > data.len() {
		return Err(Error::InvalidBatchRecord);
	}
	let key_slice = &data[key_start..key_end];
	*pos = key_end;

	// Read value length
	let (val_len, bytes_read) = u64::decode_var(&data[*pos..]).ok_or(Error::InvalidBatchRecord)?;
	*pos += bytes_read;

	// Read value bytes (if any)
	let value_slice = if val_len > 0 {
		let val_start = *pos;
		let val_end = val_start + val_len as usize;
		if val_end > data.len() {
			return Err(Error::InvalidBatchRecord);
		}
		let v = &data[val_start..val_end];
		*pos = val_end;
		Some(v)
	} else {
		None
	};

	Ok(Some((kind, key_slice, value_slice)))
}

impl<'a> Iterator for BatchIterator<'a> {
	type Item = Result<RecordKey<'a>>;

	fn next(&mut self) -> Option<Self::Item> {
		match decode_record_at(self.data, &mut self.pos) {
			Ok(Some(record)) => Some(Ok(record)),
			Ok(None) => None,
			Err(e) => Some(Err(e)),
		}
	}
}

pub struct BatchReader<'a> {
	data: &'a [u8],
	pos: usize,
	count: u32,
	seq_num: u64,
}

impl<'a> BatchReader<'a> {
	pub fn new(data: &'a [u8]) -> Result<Self> {
		let (seq_num, read1) = u64::decode_var(data).ok_or(Error::InvalidBatchRecord)?;
		let (count, read2) = u32::decode_var(&data[read1..]).ok_or(Error::InvalidBatchRecord)?;

		Ok(Self {
			data,
			pos: read1 + read2,
			count,
			seq_num,
		})
	}

	pub fn get_seq_num(&self) -> u64 {
		self.seq_num
	}

	pub fn get_count(&self) -> u32 {
		self.count
	}

	pub fn read_record(&mut self) -> RecordResult<'a> {
		decode_record_at(self.data, &mut self.pos)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_batch_new() {
		let batch = Batch::new();
		assert_eq!(batch.data.len(), 0);
		assert_eq!(batch.count, 0);
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
		assert_eq!(batch.get_count(), 0);
		batch.set(b"key1", b"value1").unwrap();
		assert_eq!(batch.get_count(), 1);
	}

	#[test]
	fn test_batch_set() {
		let mut batch = Batch::new();
		batch.set(b"key1", b"value1").unwrap();
		assert_eq!(batch.get_count(), 1);
	}

	#[test]
	fn test_batch_delete() {
		let mut batch = Batch::new();
		batch.delete(b"key1").unwrap();
		assert_eq!(batch.get_count(), 1);
	}

	#[test]
	fn test_batchreader_new() {
		let mut batch = Batch::new();
		batch.set(b"key1", b"value1").unwrap();
		let encoded = batch.encode(100).unwrap();
		let reader = BatchReader::new(&encoded).unwrap();
		assert_eq!(reader.get_seq_num(), 100);
		assert_eq!(reader.get_count(), 1);
	}

	#[test]
	fn test_batchreader_get_seq_num() {
		let batch = Batch::new();
		let encoded = batch.encode(100).unwrap();
		let reader = BatchReader::new(&encoded).unwrap();
		assert_eq!(reader.get_seq_num(), 100);
	}

	#[test]
	fn test_batchreader_get_count() {
		let mut batch = Batch::new();
		batch.set(b"key1", b"value1").unwrap();
		let encoded = batch.encode(1).unwrap();
		let reader = BatchReader::new(&encoded).unwrap();
		assert_eq!(reader.get_count(), 1);
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
		assert_eq!(reader.get_count(), 0);
		assert_eq!(reader.get_seq_num(), 1);
		assert!(reader.read_record().unwrap().is_none());
	}

	#[test]
	fn test_batch_multiple_operations() {
		let mut batch = Batch::new();
		batch.set(b"key1", b"value1").unwrap();
		batch.delete(b"key2").unwrap();
		batch.set(b"key3", b"value3").unwrap();

		assert_eq!(batch.get_count(), 3);

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
		assert_eq!(records[0].2.unwrap(), b"value1");

		assert_eq!(records[1].0, InternalKeyKind::Delete);
		assert_eq!(records[1].1, b"key2");
		assert!(records[1].2.is_none());

		assert_eq!(records[2].0, InternalKeyKind::Set);
		assert_eq!(records[2].1, b"key3");
		assert_eq!(records[2].2.unwrap(), b"value3");
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
	fn test_batch_sequence_numbers() {
		let mut batch = Batch::new();
		batch.set(b"key1", b"value1").unwrap();
		batch.set(b"key2", b"value2").unwrap();

		let encoded = batch.encode(100).unwrap();
		let reader = BatchReader::new(&encoded).unwrap();

		assert_eq!(reader.get_seq_num(), 100);
		assert_eq!(reader.get_count(), 2);
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

		assert_eq!(batch.get_count() as usize, NUM_RECORDS);

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

		let batch_ptr = batch.data.as_ptr() as usize;
		let batch_len = batch.data.len();
		let (kind, key, value) = batch.iter().next().unwrap().unwrap();
		assert_eq!(kind, InternalKeyKind::Set);
		let key_ptr = key.as_ptr() as usize;
		let val_ptr = value.unwrap().as_ptr() as usize;
		assert!(key_ptr >= batch_ptr && key_ptr < batch_ptr + batch_len);
		assert!(val_ptr >= batch_ptr && val_ptr < batch_ptr + batch_len);
	}

	#[test]
	fn test_reader_zero_copy() {
		let mut batch = Batch::new();
		batch.set(b"k", b"v").unwrap();
		let encoded = batch.encode(1).unwrap();
		let encoded_ptr = encoded.as_ptr() as usize;
		let encoded_len = encoded.len();
		let mut reader = BatchReader::new(&encoded).unwrap();
		let (kind, key, value) = reader.read_record().unwrap().unwrap();
		assert_eq!(kind, InternalKeyKind::Set);
		let key_ptr = key.as_ptr() as usize;
		let val_ptr = value.unwrap().as_ptr() as usize;
		assert!(key_ptr >= encoded_ptr && key_ptr < encoded_ptr + encoded_len);
		assert!(val_ptr >= encoded_ptr && val_ptr < encoded_ptr + encoded_len);
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
		assert_eq!(value3, Some(&b"merge_data"[..]), "Merge operations now work correctly");

		// Test with Set operations (should still work as before)
		let mut batch_set = Batch::new();
		batch_set.add_record(InternalKeyKind::Set, b"set_key", Some(b"set_data")).unwrap();

		let encoded_set = batch_set.encode(400).unwrap();
		let mut reader_set = BatchReader::new(&encoded_set).unwrap();
		let (kind4, key4, value4) = reader_set.read_record().unwrap().unwrap();
		assert_eq!(kind4, InternalKeyKind::Set);
		assert_eq!(key4, b"set_key");
		assert_eq!(value4, Some(&b"set_data"[..]), "Set operations still work correctly");
	}
}
