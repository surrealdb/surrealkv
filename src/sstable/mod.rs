pub(crate) mod block;
pub(crate) mod bloom;
pub(crate) mod filter_block;
pub(crate) mod index_block;
pub(crate) mod meta;
pub(crate) mod table;

use std::cmp::{Ordering, Reverse};
use std::fmt::Debug;
use std::sync::Arc;

// This is the maximum valid sequence number that can be stored in the upper 56 bits of a 64-bit integer.
// 1 << 56 shifts the number 1 left by 56 bits, resulting in a binary number with a 1 followed by 56 zeros.
// Subtracting 1 gives a binary number with 56 ones, which is the maximum value for 56 bits.
pub(crate) const INTERNAL_KEY_SEQ_NUM_MAX: u64 = (1 << 56) - 1;
pub(crate) const INTERNAL_KEY_TIMESTAMP_MAX: u64 = u64::MAX;

/// Converts a trailer byte to InternalKeyKind
/// This centralizes the kind conversion logic to avoid duplication and errors
fn trailer_to_kind(trailer: u64) -> InternalKeyKind {
	let kind_byte = trailer as u8;
	match kind_byte {
		0 => InternalKeyKind::Delete,
		1 => InternalKeyKind::SoftDelete,
		2 => InternalKeyKind::Set,
		3 => InternalKeyKind::Merge,
		4 => InternalKeyKind::LogData,
		5 => InternalKeyKind::RangeDelete,
		6 => InternalKeyKind::Separator,
		7 => InternalKeyKind::Max,
		_ => InternalKeyKind::Invalid,
	}
}

/// Extracts sequence number from trailer
/// This centralizes the seq_num extraction logic to avoid duplication
fn trailer_to_seq_num(trailer: u64) -> u64 {
	trailer >> 8
}

/// Checks if a key kind represents a tombstone (delete operation)
/// This centralizes the tombstone check logic to avoid duplication and ensure consistency
fn is_tombstone_kind(kind: InternalKeyKind) -> bool {
	matches!(
		kind,
		InternalKeyKind::Delete | InternalKeyKind::SoftDelete | InternalKeyKind::RangeDelete
	)
}

/// Calculates the size of a key with the given user key length
/// This centralizes the size calculation logic to avoid duplication
fn calculate_key_size(user_key_len: usize, has_timestamp: bool) -> usize {
	let fixed_size = if has_timestamp {
		std::mem::size_of::<u64>() * 2 + std::mem::size_of::<Arc<[u8]>>()
	} else {
		std::mem::size_of::<u64>() + std::mem::size_of::<Arc<[u8]>>()
	};
	fixed_size + user_key_len
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InternalKeyKind {
	Delete = 0,
	SoftDelete = 1,
	Set = 2,
	Merge = 3,
	LogData = 4,
	RangeDelete = 5,
	Separator = 6,
	Max = 7,
	Invalid = 8,
}

impl From<u8> for InternalKeyKind {
	fn from(value: u8) -> Self {
		trailer_to_kind(value as u64)
	}
}

/// InternalKey is the main key type used throughout the LSM tree
/// It includes a timestamp field for versioned queries
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InternalKey {
	pub(crate) user_key: Arc<[u8]>,
	pub(crate) timestamp: u64, // System time in nanoseconds since epoch
	pub(crate) trailer: u64,   // (seq_num << 8) | kind
}

impl InternalKey {
	pub(crate) fn new(
		user_key: Vec<u8>,
		seq_num: u64,
		kind: InternalKeyKind,
		timestamp: u64,
	) -> Self {
		Self {
			user_key: Arc::from(user_key.into_boxed_slice()),
			timestamp,
			trailer: (seq_num << 8) | kind as u64,
		}
	}

	pub(crate) fn size(&self) -> usize {
		calculate_key_size(self.user_key.len(), true)
	}

	pub(crate) fn decode(encoded_key: &[u8]) -> Self {
		let n = encoded_key.len() - 16; // 8 bytes for timestamp + 8 bytes for trailer
		let trailer = u64::from_be_bytes(encoded_key[n..n + 8].try_into().unwrap());
		let timestamp = u64::from_be_bytes(encoded_key[n + 8..].try_into().unwrap());
		let user_key = Arc::<[u8]>::from(&encoded_key[..n]);
		Self {
			user_key,
			timestamp,
			trailer,
		}
	}

	pub(crate) fn encode(&self) -> Vec<u8> {
		let mut buf = self.user_key.as_ref().to_vec();
		buf.extend_from_slice(&self.trailer.to_be_bytes());
		buf.extend_from_slice(&self.timestamp.to_be_bytes());
		buf
	}

	pub(crate) fn seq_num(&self) -> u64 {
		trailer_to_seq_num(self.trailer)
	}

	pub(crate) fn kind(&self) -> InternalKeyKind {
		trailer_to_kind(self.trailer)
	}

	pub(crate) fn is_tombstone(&self) -> bool {
		is_tombstone_kind(self.kind())
	}
}

impl Ord for InternalKey {
	fn cmp(&self, other: &Self) -> Ordering {
		// Same as InternalKey: user key, then sequence number, then kind, with timestamp as final tiebreaker
		match (&self.user_key, Reverse(self.seq_num()))
			.cmp(&(&other.user_key, Reverse(other.seq_num())))
		{
			Ordering::Equal => match Reverse(self.kind() as u8).cmp(&Reverse(other.kind() as u8)) {
				Ordering::Equal => Reverse(self.timestamp).cmp(&Reverse(other.timestamp)),
				ordering => ordering,
			},
			ordering => ordering,
		}
	}
}

impl PartialOrd for InternalKey {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Default for InternalKey {
	fn default() -> Self {
		Self {
			user_key: Arc::from([]),
			timestamp: 0,
			trailer: 0,
		}
	}
}

/// ReverseTimestampKey has timestamp first, then internal key structure
/// This allows efficient range queries by time
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReverseTimestampKey {
	pub(crate) timestamp: u64, // System time in nanoseconds since epoch
	pub(crate) user_key: Arc<[u8]>,
	pub(crate) trailer: u64, // Same as InternalKey: (seq_num << 8) | kind
}

impl ReverseTimestampKey {
	pub(crate) fn new(
		user_key: Vec<u8>,
		seq_num: u64,
		kind: InternalKeyKind,
		timestamp: u64,
	) -> Self {
		Self {
			timestamp,
			user_key: Arc::from(user_key.into_boxed_slice()),
			trailer: (seq_num << 8) | kind as u64,
		}
	}

	pub(crate) fn size(&self) -> usize {
		calculate_key_size(self.user_key.len(), true)
	}

	pub(crate) fn decode(encoded_key: &[u8]) -> Self {
		let n = encoded_key.len() - 16; // 8 bytes for timestamp + 8 bytes for trailer
		let timestamp = u64::from_be_bytes(encoded_key[..8].try_into().unwrap());
		let trailer = u64::from_be_bytes(encoded_key[n + 8..].try_into().unwrap());
		let user_key = Arc::<[u8]>::from(&encoded_key[8..n + 8]);
		Self {
			timestamp,
			user_key,
			trailer,
		}
	}

	pub(crate) fn encode(&self) -> Vec<u8> {
		let mut buf = Vec::with_capacity(self.size());
		buf.extend_from_slice(&self.timestamp.to_be_bytes());
		buf.extend_from_slice(self.user_key.as_ref());
		buf.extend_from_slice(&self.trailer.to_be_bytes());
		buf
	}

	pub(crate) fn seq_num(&self) -> u64 {
		trailer_to_seq_num(self.trailer)
	}

	pub(crate) fn kind(&self) -> InternalKeyKind {
		trailer_to_kind(self.trailer)
	}

	pub(crate) fn is_tombstone(&self) -> bool {
		is_tombstone_kind(self.kind())
	}
}

impl Ord for ReverseTimestampKey {
	fn cmp(&self, other: &Self) -> Ordering {
		// First compare by timestamp (ascending for chronological order), then by user key, then by sequence number (descending)
		self.timestamp
			.cmp(&other.timestamp)
			.then(self.user_key.cmp(&other.user_key))
			.then(Reverse(self.seq_num()).cmp(&Reverse(other.seq_num())))
	}
}

impl PartialOrd for ReverseTimestampKey {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Default for ReverseTimestampKey {
	fn default() -> Self {
		Self {
			timestamp: 0,
			user_key: Arc::from([]),
			trailer: 0,
		}
	}
}
