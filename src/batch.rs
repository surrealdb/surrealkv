use integer_encoding::{VarInt, VarIntWriter};

use crate::error::{Error, Result};
use crate::{BranchGeneration, BranchId, InternalKeyKind, Key, Value};

pub(crate) const MAX_BATCH_SIZE: u64 = 1 << 32;
/// Inline-only batch encoding version. The sequence number is fixed-width so
/// the commit pipeline can stamp it in place. Earlier, incompatible layouts
/// are intentionally rejected.
pub(crate) const BATCH_VERSION: u8 = 4;

/// Physical owner of every row in a commit batch. Ownership stays in the
/// batch/component metadata and is deliberately not prefixed into user keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BatchOwner {
	pub(crate) branch: BranchId,
	pub(crate) generation: BranchGeneration,
}

impl BatchOwner {
	/// Owner used by the existing single-branch runtime during integration.
	pub(crate) const DEFAULT: Self = Self {
		branch: BranchId([0; 16]),
		generation: BranchGeneration(0),
	};
}

impl Default for BatchOwner {
	fn default() -> Self {
		Self::DEFAULT
	}
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
	pub(crate) owner: BatchOwner,
	pub(crate) entries: Vec<BatchEntry>,
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
		Self::for_owner(starting_seq_num, BatchOwner::DEFAULT)
	}

	pub(crate) fn for_owner(starting_seq_num: u64, owner: BatchOwner) -> Self {
		Self {
			entries: Vec::new(),
			version: BATCH_VERSION,
			owner,
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
		Ok(())
	}

	pub(crate) fn encode(&self) -> Result<Vec<u8>> {
		let mut encoded = Vec::new();

		// Write version (1 byte)
		encoded.push(self.version);

		// Write sequence number (fixed-width 8-byte LE).
		// Fixed width (vs varint) lets the commit pipeline stamp the seq in
		// place after pre-encoding off the write lock — see `patch_encoded_seq`.
		encoded.extend_from_slice(&self.starting_seq_num.to_le_bytes());

		// Write branch identity and generation. These bytes identify the
		// component owner; user keys remain unchanged.
		encoded.extend_from_slice(&self.owner.branch.0);
		encoded.extend_from_slice(&self.owner.generation.0.to_le_bytes());

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

	/// Stamp the commit sequence number into an already-encoded batch
	/// buffer, in place. `encode` writes the seq as 8 fixed-width LE bytes at
	/// offset `[1..9]` (right after the 1-byte version), so the commit pipeline
	/// can pre-encode a batch off the write lock with a placeholder seq and then
	/// stamp the real seq under the lock with no re-encode and no copy.
	pub(crate) fn patch_encoded_seq(buf: &mut [u8], seq: u64) {
		debug_assert!(buf.len() >= 9, "encoded batch too short to patch seq");
		debug_assert_eq!(buf[0], BATCH_VERSION, "unexpected batch version");
		buf[1..9].copy_from_slice(&seq.to_le_bytes());
	}

	#[cfg(test)]
	pub(crate) fn set(&mut self, key: Key, value: Value, timestamp: u64) -> Result<()> {
		self.add_record(InternalKeyKind::Set, key, Some(value), timestamp)
	}

	#[cfg(test)]
	pub(crate) fn delete(&mut self, key: Key, timestamp: u64) -> Result<()> {
		self.add_record(InternalKeyKind::Delete, key, None, timestamp)
	}

	pub(crate) fn add_record(
		&mut self,
		kind: InternalKeyKind,
		key: Key,
		value: Option<Value>,
		timestamp: u64,
	) -> Result<()> {
		let requires_value = matches!(
			kind,
			InternalKeyKind::Set
				| InternalKeyKind::Merge
				| InternalKeyKind::LogData
				| InternalKeyKind::Replace
		);
		if requires_value != value.is_some() {
			return Err(Error::InvalidArgument(format!(
				"invalid value presence for batch record kind {kind:?}"
			)));
		}

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

	pub(crate) fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	/// Upper bound on the bytes this batch would consume in a memtable's skiplist arena.
	/// Uses the worst-case per-entry overhead (max skiplist height + alignment padding),
	/// so the actual allocation cannot exceed this. Used by `MemTable::add` for atomic
	/// preflight reservation.
	pub(crate) fn memtable_size_estimate(&self) -> u64 {
		use crate::memtable::max_entry_bytes;
		self.entries
			.iter()
			.map(|e| max_entry_bytes(e.key.len(), e.value.as_ref().map_or(0, |v| v.len())))
			.sum()
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
		// The inline-value format intentionally rejects older batch versions.
		// Old stores require an explicit external migration.
		if version != BATCH_VERSION {
			return Err(Error::InvalidBatchRecord);
		}

		let seq_num = u64::from_le_bytes(take(data, &mut pos, 8)?.try_into().unwrap());
		let mut branch = [0; 16];
		branch.copy_from_slice(take(data, &mut pos, 16)?);
		let generation = u64::from_le_bytes(take(data, &mut pos, 8)?.try_into().unwrap());
		let owner = BatchOwner {
			branch: BranchId(branch),
			generation: BranchGeneration(generation),
		};

		// Read count
		let (count, bytes_read) =
			u32::decode_var(data.get(pos..).ok_or(Error::InvalidBatchRecord)?)
				.ok_or(Error::InvalidBatchRecord)?;
		pos += bytes_read;
		if count as usize > data.len().saturating_sub(pos) / 4 {
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
			let (key_len, bytes_read) =
				u64::decode_var(data.get(pos..).ok_or(Error::InvalidBatchRecord)?)
					.ok_or(Error::InvalidBatchRecord)?;
			pos += bytes_read;
			let key_len = usize::try_from(key_len).map_err(|_| Error::InvalidBatchRecord)?;
			let key = take(data, &mut pos, key_len)?.to_vec();

			// Read value
			let (value_len, bytes_read) =
				u64::decode_var(data.get(pos..).ok_or(Error::InvalidBatchRecord)?)
					.ok_or(Error::InvalidBatchRecord)?;
			pos += bytes_read;
			let value_len = usize::try_from(value_len).map_err(|_| Error::InvalidBatchRecord)?;
			let value = if value_len > 0 {
				let value_data = take(data, &mut pos, value_len)?.to_vec();
				Some(value_data)
			} else if matches!(
				kind,
				InternalKeyKind::Set
					| InternalKeyKind::Merge
					| InternalKeyKind::LogData
					| InternalKeyKind::Replace
			) {
				Some(Vec::new())
			} else {
				None
			};

			// Read timestamp
			let (timestamp, bytes_read) =
				u64::decode_var(data.get(pos..).ok_or(Error::InvalidBatchRecord)?)
					.ok_or(Error::InvalidBatchRecord)?;
			pos += bytes_read;

			entries.push(BatchEntry {
				kind,
				key,
				value,
				timestamp,
			});
		}

		if pos != data.len() {
			return Err(Error::InvalidBatchRecord);
		}

		Ok(Self {
			version,
			owner,
			entries,
			starting_seq_num: seq_num,
			size: 0, // Decoded batches don't track size
		})
	}
}

fn take<'a>(data: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a [u8]> {
	let end = pos.checked_add(len).ok_or(Error::InvalidBatchRecord)?;
	let bytes = data.get(*pos..end).ok_or(Error::InvalidBatchRecord)?;
	*pos = end;
	Ok(bytes)
}
