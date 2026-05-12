use integer_encoding::{VarInt, VarIntWriter};

use crate::error::{Error, Result};
use crate::vlog::{ValuePointer, VALUE_LOCATION_VERSION, VALUE_POINTER_SIZE};
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

#[derive(Debug, Clone)]
pub(crate) struct Batch {
	pub(crate) version: u8,
	pub(crate) entries: Vec<BatchEntry>,
	pub(crate) valueptrs: Vec<Option<ValuePointer>>, /* Parallel array to entries, None for
	                                                  * inline values */
	pub(crate) starting_seq_num: u64, // Starting sequence number for this batch
	pub(crate) size: u64,             // Total size of all records (not serialized)
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
	pub(crate) fn grow(&mut self, record_size: u64) -> Result<()> {
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
	pub(crate) fn set(&mut self, key: Key, value: Value, timestamp: u64) -> Result<()> {
		self.add_record(InternalKeyKind::Set, key, Some(value), timestamp)
	}

	#[cfg(test)]
	pub(crate) fn delete(&mut self, key: Key, timestamp: u64) -> Result<()> {
		self.add_record(InternalKeyKind::Delete, key, None, timestamp)
	}

	/// Internal method to add a record with optional value pointer
	fn add_record_internal(
		&mut self,
		kind: InternalKeyKind,
		key: Key,
		value: Option<Value>,
		valueptr: Option<ValuePointer>,
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
		self.valueptrs.push(valueptr);

		Ok(())
	}

	pub(crate) fn add_record(
		&mut self,
		kind: InternalKeyKind,
		key: Key,
		value: Option<Value>,
		timestamp: u64,
	) -> Result<()> {
		self.add_record_internal(kind, key, value, None, timestamp)
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

/// Result of encoding a batch for both WAL persistence and memtable insertion.
///
/// Holds the encoded WAL bytes once. Per-entry `(offset, len)` slices into
/// `wal_bytes` point at the encoded `ValueLocation` payload for each entry
/// (meta byte + version byte + raw value bytes). The skiplist apply path
/// passes these slices directly to `skiplist.add`, avoiding the
/// allocate-then-clone pattern of building a separate processed `Batch`.
///
/// The wire format of `wal_bytes` is bit-exact with `Batch::encode()` so
/// existing WAL recovery code reads the same bytes without modification.
pub(crate) struct PreparedBatch {
	/// Encoded WAL bytes, ready for `wal.append`.
	pub(crate) wal_bytes: Vec<u8>,
	/// Per-entry `(offset, len)` into `wal_bytes` for the encoded value.
	/// `(0, 0)` denotes a delete (no value bytes).
	pub(crate) value_slices: Vec<(usize, usize)>,
}

impl PreparedBatch {
	/// Returns the encoded value bytes for entry `i`, or `&[]` for deletes.
	pub(crate) fn value_bytes(&self, i: usize) -> &[u8] {
		let (off, len) = self.value_slices[i];
		if len == 0 {
			&[]
		} else {
			&self.wal_bytes[off..off + len]
		}
	}
}

/// Stream-encode a batch into WAL bytes + per-entry value slices.
///
/// Produces the same wire bytes as constructing a processed `Batch` with
/// `ValueLocation::with_inline_value(value).encode()` for each entry and
/// then calling `Batch::encode()`. Avoids the intermediate processed Batch,
/// per-entry value/key clones, and the second encoding pass.
///
/// Behavior matches `LsmCommitEnv::write`'s old loop:
/// - VLog separation is deferred to memtable flush time, so the valueptrs section is always
///   all-`None` (`count` zero bytes).
/// - Each entry's value is wrapped as `[meta=0, version=VALUE_LOCATION_VERSION, raw...]`.
pub(crate) fn encode_batch_for_wal(batch: &Batch, seq_num: u64) -> Result<PreparedBatch> {
	let count = batch.count() as usize;

	// Estimate capacity: 32 bytes header overhead + per-entry overhead +
	// total key/value bytes. Slight over-allocation is cheap; avoiding
	// resize is the goal.
	let mut estimated = 32usize;
	for entry in &batch.entries {
		estimated += 32 + entry.key.len() + entry.value.as_deref().map_or(0, |v| v.len() + 2);
	}
	estimated += count; // valueptrs section (1 byte per entry, all zero)

	let mut wal_bytes = Vec::with_capacity(estimated);
	let mut value_slices = Vec::with_capacity(count);

	// Header: matches Batch::encode lines 60-66.
	wal_bytes.push(BATCH_VERSION);
	wal_bytes.write_varint(seq_num)?;
	wal_bytes.write_varint(count as u32)?;

	// Entries section: matches Batch::encode lines 69-86.
	for entry in &batch.entries {
		wal_bytes.push(entry.kind as u8);

		wal_bytes.write_varint(entry.key.len() as u64)?;
		wal_bytes.extend_from_slice(&entry.key);

		let slice = match &entry.value {
			Some(raw) => {
				// Encoded value layout matches ValueLocation::encode for an
				// inline value: meta (1B) + version (1B) + raw bytes.
				let encoded_len = raw.len() + 2;
				wal_bytes.write_varint(encoded_len as u64)?;
				let start = wal_bytes.len();
				wal_bytes.push(0); // meta = 0 (inline, no VLog pointer)
				wal_bytes.push(VALUE_LOCATION_VERSION);
				wal_bytes.extend_from_slice(raw);
				(start, encoded_len)
			}
			None => {
				wal_bytes.write_varint(0u64)?;
				(0, 0)
			}
		};
		value_slices.push(slice);

		wal_bytes.write_varint(entry.timestamp)?;
	}

	// Valueptrs section: all-None at commit time (VLog separation deferred
	// to flush). Matches Batch::encode lines 89-99 with valueptr=None.
	for _ in 0..count {
		wal_bytes.push(0);
	}

	Ok(PreparedBatch {
		wal_bytes,
		value_slices,
	})
}
