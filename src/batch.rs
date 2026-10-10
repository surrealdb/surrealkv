use crate::error::{Error, Result};
use crate::varint::{
	decode_varint_u32,
	decode_varint_u64,
	put_varint_u32,
	put_varint_u64,
	varint_len_u64,
};
use crate::vlog::{ValuePointer, VALUE_POINTER_SIZE};
use crate::{InternalKeyKind, Key, Value};

/// The most bytes a batch may encode to: a WAL record is one batch, and the LZ4 block of a
/// compressed record says its length as a `u32`. `Batch::grow` refuses a record that would take
/// the batch past it, and `Batch::exceeds_max_size` is the same test on a whole batch.
pub(crate) const MAX_BATCH_SIZE: u64 = u32::MAX as u64;
/// The bytes of an encoded batch ahead of its entries, at most: the version, and the varints of
/// the sequence number (10 bytes) and of the entry count (5 bytes). An entry counts for 11 bytes
/// at the least, so the count of a batch at the limit fits the `u32` it is written as.
pub(crate) const ENCODED_HEADER_SIZE: u64 = 1 + 10 + 5;
/// The bytes an entry takes in an encoded batch beyond what `Batch::grow` counts for it, at
/// most: a timestamp varint of up to 10 bytes where `grow` counts 8, and its value pointer flag.
pub(crate) const ENCODED_ENTRY_SLACK: u64 = 2 + 1;
const _: () = assert!(MAX_BATCH_SIZE <= u32::MAX as u64);
pub(crate) const BATCH_VERSION: u8 = 1;
/// The fewest bytes an entry takes in an encoded batch: kind, key length, value
/// length and timestamp (one byte each when empty), and its value pointer flag.
const MIN_ENTRY_SIZE: usize = 5;

/// An upper bound on the bytes that a batch of `entries` entries, whose `grow` size is `size`,
/// encodes to when none of them holds a value pointer.
fn encoded_len_bound(size: u64, entries: usize) -> u64 {
	size.saturating_add(ENCODED_HEADER_SIZE)
		.saturating_add(ENCODED_ENTRY_SLACK.saturating_mul(entries as u64))
}

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
	// The WAL log sequence number assigned to the first entry in this batch.
	// Stamped by `CommitPipeline::commit` under `write_mutex` after the
	// oracle has validated the write set. Constructed with `0` by callers;
	// the pipeline overwrites it before the batch is written to WAL.
	pub(crate) starting_seq_num: u64,
	pub(crate) size: u64, // Total size of all records (not serialized)
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

	/// Counts a record of `record_size` bytes in the batch, or refuses it with `BatchTooLarge`
	/// if the batch would then encode to more than `MAX_BATCH_SIZE`.
	pub(crate) fn grow(&mut self, record_size: u64) -> Result<()> {
		let size = self.size.saturating_add(record_size);
		if encoded_len_bound(size, self.entries.len() + 1) > MAX_BATCH_SIZE {
			return Err(Error::BatchTooLarge);
		}
		self.size = size;
		self.entries.reserve(1);
		self.valueptrs.reserve(1);
		Ok(())
	}

	#[cfg(test)]
	pub(crate) fn encode(&self) -> Result<Vec<u8>> {
		let mut encoded = Vec::with_capacity(self.size as usize + 64);
		self.encode_into(&mut encoded)?;
		Ok(encoded)
	}

	/// A capacity hint for `encode_into`: the entry sizes `grow` tracks, the header, and slack
	/// for the value pointer flags and for a timestamp varint longer than the 8 bytes `grow`
	/// counts. Only a hint: `size` is not tracked for decoded batches, and a batch holding value
	/// pointers encodes longer, in which case the buffer simply grows.
	pub(crate) fn encoded_len_hint(&self) -> usize {
		usize::try_from(encoded_len_bound(self.size, self.entries.len())).unwrap_or(usize::MAX)
	}

	/// Whether the batch encodes to more bytes than one WAL record can hold, see
	/// `MAX_BATCH_SIZE`. `grow` keeps a batch from getting there, but the flusher changes the
	/// values of a batch (it wraps them, or replaces them by a pointer), which can.
	pub(crate) fn exceeds_max_size(&self) -> bool {
		encoded_len_bound(self.size, self.entries.len()) > MAX_BATCH_SIZE
	}

	/// Appends the encoding of this batch to `encoded`, after whatever it already holds.
	///
	/// This never clears the buffer, so a caller that frames several batches into one buffer
	/// has to remember where each one ends: one WAL record is one batch, and `decode` rejects
	/// bytes past the end of it.
	pub(crate) fn encode_into(&self, encoded: &mut Vec<u8>) -> Result<()> {
		// Write version (1 byte)
		encoded.push(self.version);

		// Write sequence number (8 bytes)
		put_varint_u64(encoded, self.starting_seq_num);

		// Write count (4 bytes)
		put_varint_u32(encoded, self.entries.len() as u32);

		// Write entries
		for entry in &self.entries {
			// Write kind (1 byte)
			encoded.push(entry.kind as u8);

			// Write key length and key
			put_varint_u64(encoded, entry.key.len() as u64);
			encoded.extend_from_slice(&entry.key);

			// Write value length and value
			let value_len = entry.value.as_ref().map_or(0, |v| v.len());
			put_varint_u64(encoded, value_len as u64);
			if let Some(value) = &entry.value {
				encoded.extend_from_slice(value);
			}

			// Write timestamp (8 bytes)
			put_varint_u64(encoded, entry.timestamp);
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

		Ok(())
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
			varint_len_u64(key_len as u64) as u64 +
			key_len as u64 +
			varint_len_u64(value_len as u64) as u64 +
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

	/// Upper bound on the bytes this batch would consume in a memtable's arena.
	/// Uses artmap's worst-case per-insert cost (see `memtable::max_entry_bytes`),
	/// so the actual allocation cannot exceed this. Used by `MemTable::add` for atomic
	/// preflight reservation.
	pub(crate) fn memtable_size_estimate(&self) -> u64 {
		use crate::memtable::max_entry_bytes;
		self.entries
			.iter()
			.map(|e| max_entry_bytes(e.key.len(), e.value.as_ref().map_or(0, |v| v.len())))
			.sum()
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

	/// Decode a batch from encoded data.
	///
	/// Every read is bounds-checked and the whole record must be consumed, so a
	/// malformed record is an error rather than a panic or a batch that is
	/// silently shorter than what was written.
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
			decode_varint_u64(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
		pos += bytes_read;

		// Read count
		let (count, bytes_read) =
			decode_varint_u32(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
		pos += bytes_read;

		// A count the rest of the record cannot hold is garbage. Reject it before it sizes
		// an allocation.
		if count as usize > (data.len() - pos) / MIN_ENTRY_SIZE {
			return Err(Error::InvalidBatchRecord);
		}

		// Read entries
		let mut entries = Vec::with_capacity(count as usize);
		for _ in 0..count {
			// Read kind
			let kind_byte = take(data, &mut pos, 1)?[0];
			let kind = InternalKeyKind::from(kind_byte);
			if kind == InternalKeyKind::Invalid {
				return Err(Error::InvalidBatchRecord);
			}

			// Read key
			// Read key length and key
			let (key_len, bytes_read) =
				decode_varint_u64(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
			pos += bytes_read;
			let key = take(data, &mut pos, key_len)?.to_vec();

			// Read value length and value
			let (value_len, bytes_read) =
				decode_varint_u64(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
			pos += bytes_read;
			let value = if value_len > 0 {
				Some(take(data, &mut pos, value_len)?.to_vec())
			} else {
				None
			};

			// Read timestamp
			let (timestamp, bytes_read) =
				decode_varint_u64(&data[pos..]).ok_or(Error::InvalidBatchRecord)?;
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
			let valueptr = match take(data, &mut pos, 1)?[0] {
				0 => None,
				1 => {
					let ptr_data = take(data, &mut pos, VALUE_POINTER_SIZE as u64)?;
					Some(ValuePointer::decode(ptr_data)?)
				}
				_ => return Err(Error::InvalidBatchRecord),
			};
			valueptrs.push(valueptr);
		}

		// Bytes left over would be silently dropped.
		if pos != data.len() {
			return Err(Error::InvalidBatchRecord);
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

/// Reads `len` bytes at `*pos` and advances past them, or fails if the record
/// ends first.
fn take<'a>(data: &'a [u8], pos: &mut usize, len: u64) -> Result<&'a [u8]> {
	let len = usize::try_from(len).map_err(|_| Error::InvalidBatchRecord)?;
	let end =
		pos.checked_add(len).filter(|&end| end <= data.len()).ok_or(Error::InvalidBatchRecord)?;
	let bytes = &data[*pos..end];
	*pos = end;
	Ok(bytes)
}
