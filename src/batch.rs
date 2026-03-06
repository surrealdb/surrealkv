use integer_encoding::{VarInt, VarIntWriter};

use crate::error::{Error, Result};
use crate::{InternalKeyKind, Key, Value};

pub(crate) const MAX_BATCH_SIZE: u64 = 1 << 32;
pub(crate) const BATCH_VERSION: u8 = 1;

/// Represents a single entry in a batch
#[derive(Debug, Clone)]
pub(crate) struct BatchEntry {
	pub kind: InternalKeyKind,
	pub key: Key,
	pub value: Option<Value>,
	pub timestamp: u64,
}

/// A batch of write operations to be applied atomically.
///
/// Create a batch using `Store::new_batch()`, add operations with
/// `set()`, `set_at()`, `delete()`, then apply with `Store::apply()`.
#[derive(Debug, Clone)]
pub struct Batch {
	pub(crate) version: u8,
	pub(crate) entries: Vec<BatchEntry>,
	pub(crate) starting_seq_num: u64,
	pub(crate) size: u64,
}

impl Default for Batch {
	fn default() -> Self {
		Self::new()
	}
}

impl Batch {
	/// Creates a new empty batch.
	pub fn new() -> Self {
		Self {
			entries: Vec::new(),
			version: BATCH_VERSION,
			starting_seq_num: 0,
			size: 0,
		}
	}

	/// Creates a new batch with a specific starting sequence number (internal use).
	pub(crate) fn new_with_seq(starting_seq_num: u64) -> Self {
		Self {
			entries: Vec::new(),
			version: BATCH_VERSION,
			starting_seq_num,
			size: 0,
		}
	}

	// ===== Public Write Operations =====

	/// Adds a set operation to the batch.
	///
	/// The timestamp will be assigned when the batch is applied.
	pub fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
		self.add_record(
			InternalKeyKind::Set,
			key.as_ref().to_vec(),
			Some(value.as_ref().to_vec()),
			0, // Timestamp assigned at commit
		)
	}

	/// Adds a set operation with an explicit timestamp.
	pub fn set_at(
		&mut self,
		key: impl AsRef<[u8]>,
		value: impl AsRef<[u8]>,
		timestamp: u64,
	) -> Result<()> {
		self.add_record(
			InternalKeyKind::Set,
			key.as_ref().to_vec(),
			Some(value.as_ref().to_vec()),
			timestamp,
		)
	}

	/// Adds a delete operation to the batch.
	///
	/// The timestamp will be assigned when the batch is applied.
	pub fn delete(&mut self, key: impl AsRef<[u8]>) -> Result<()> {
		self.add_record(
			InternalKeyKind::Delete,
			key.as_ref().to_vec(),
			None,
			0, // Timestamp assigned at commit
		)
	}

	pub fn delete_at(&mut self, key: impl AsRef<[u8]>, timestamp: u64) -> Result<()> {
		self.add_record(InternalKeyKind::Delete, key.as_ref().to_vec(), None, timestamp)
	}

	/// Adds a soft delete operation to the batch.
	///
	/// Soft deletes mark a key as deleted but preserve it for version history queries.
	/// The timestamp will be assigned when the batch is applied.
	pub fn soft_delete(&mut self, key: impl AsRef<[u8]>) -> Result<()> {
		self.add_record(
			InternalKeyKind::SoftDelete,
			key.as_ref().to_vec(),
			None,
			0, // Timestamp assigned at commit
		)
	}

	/// Returns the number of entries in the batch.
	pub fn len(&self) -> usize {
		self.entries.len()
	}

	/// Returns true if the batch is empty.
	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	/// Resets the batch to empty state.
	pub fn reset(&mut self) {
		self.entries.clear();
		self.size = 0;
		self.starting_seq_num = 0;
	}

	// ===== Internal Methods =====

	/// Assigns commit timestamp to entries that don't have explicit timestamps.
	pub(crate) fn assign_commit_info(&mut self, commit_timestamp: u64) {
		for entry in &mut self.entries {
			if entry.timestamp == 0 {
				entry.timestamp = commit_timestamp;
			}
		}
	}

	// TODO: add a test for grow
	pub(crate) fn grow(&mut self, record_size: u64) -> Result<()> {
		if self.size + record_size > MAX_BATCH_SIZE {
			return Err(Error::BatchTooLarge);
		}
		self.size += record_size;
		self.entries.reserve(1);
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

		Ok(encoded)
	}

	pub(crate) fn add_record(
		&mut self,
		kind: InternalKeyKind,
		key: Key,
		value: Option<Value>,
		timestamp: u64,
	) -> Result<()> {
		let key_len = key.len();
		let value_len = value.as_ref().map_or(0, |v| v.len());

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
			key,
			value,
			timestamp,
		};
		self.entries.push(entry);

		Ok(())
	}

	pub(crate) fn count(&self) -> u32 {
		self.entries.len() as u32
	}

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

		Ok(Self {
			version,
			entries,
			starting_seq_num: seq_num,
			size: 0, // Decoded batches don't track size
		})
	}
}
