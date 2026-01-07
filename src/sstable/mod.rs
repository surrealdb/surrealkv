pub(crate) mod block;
pub(crate) mod bloom;
pub(crate) mod error;
pub(crate) mod filter_block;
pub(crate) mod index_block;
pub(crate) mod meta;
pub(crate) mod table;

use std::cmp::Ordering;
use std::fmt::Debug;

use crate::Key;

// This is the maximum valid sequence number that can be stored in the upper 56
// bits of a 64-bit integer. 1 << 56 shifts the number 1 left by 56 bits,
// resulting in a binary number with a 1 followed by 56 zeros. Subtracting 1
// gives a binary number with 56 ones, which is the maximum value for 56 bits.
pub(crate) const INTERNAL_KEY_SEQ_NUM_MAX: u64 = (1 << 56) - 1;
pub(crate) const INTERNAL_KEY_TIMESTAMP_MAX: u64 = u64::MAX;

// Helper function for reading u64 from byte slices without unwrap()
// Safe to use when bounds have already been checked
#[inline(always)]
fn read_u64_be(buffer: &[u8], offset: usize) -> u64 {
	// SAFETY: Caller must ensure buffer has at least offset + 8 bytes
	unsafe { u64::from_be_bytes(*(buffer.as_ptr().add(offset) as *const [u8; 8])) }
}

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
		6 => InternalKeyKind::Replace,
		7 => InternalKeyKind::Separator,
		24 => InternalKeyKind::Max,
		_ => InternalKeyKind::Invalid,
	}
}

/// Extracts sequence number from trailer
/// This centralizes the seq_num extraction logic to avoid duplication
#[inline(always)]
fn trailer_to_seq_num(trailer: u64) -> u64 {
	trailer >> 8
}

/// Checks if a key kind represents a tombstone (delete operation)
#[inline(always)]
fn is_delete_kind(kind: InternalKeyKind) -> bool {
	matches!(
		kind,
		InternalKeyKind::Delete | InternalKeyKind::SoftDelete | InternalKeyKind::RangeDelete
	)
}

/// Checks if a key kind represents a hard delete (delete operation)
#[inline(always)]
fn is_hard_delete_marker(kind: InternalKeyKind) -> bool {
	matches!(kind, InternalKeyKind::Delete | InternalKeyKind::RangeDelete)
}

/// Checks if a key kind represents a Replace operation
#[inline(always)]
fn is_replace_kind(kind: InternalKeyKind) -> bool {
	matches!(kind, InternalKeyKind::Replace)
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
	Replace = 6, // Replaces previous key when versioning is enabled
	Separator = 7,
	Max = 24, // Leaving space for other kinds
	Invalid = 191,
}

impl From<u8> for InternalKeyKind {
	fn from(value: u8) -> Self {
		trailer_to_kind(value as u64)
	}
}

/// InternalKey is the main key type used throughout the LSM tree
/// It includes a timestamp field for versioned queries
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct InternalKey {
	pub(crate) user_key: Key,
	pub(crate) timestamp: u64, // System time in nanoseconds since epoch
	pub(crate) trailer: u64,   // (seq_num << 8) | kind
}

impl InternalKey {
	pub(crate) fn new(user_key: Key, seq_num: u64, kind: InternalKeyKind, timestamp: u64) -> Self {
		Self {
			user_key,
			timestamp,
			trailer: (seq_num << 8) | kind as u64,
		}
	}

	pub(crate) fn size(&self) -> usize {
		self.user_key.len() + 16 // 8 bytes for timestamp + 8 bytes for trailer
	}

	pub(crate) fn decode(encoded_key: &[u8]) -> Self {
		let n = encoded_key.len() - 16; // 8 bytes for timestamp + 8 bytes for trailer
		let trailer = read_u64_be(encoded_key, n);
		let timestamp = read_u64_be(encoded_key, n + 8);
		let user_key = encoded_key[..n].to_vec();

		Self {
			user_key,
			timestamp,
			trailer,
		}
	}

	/// Extract user key slice without allocation
	#[inline]
	pub(crate) fn user_key_from_encoded(encoded: &[u8]) -> &[u8] {
		&encoded[..encoded.len() - 16]
	}

	/// Extract trailer (seq_num + kind) without allocation
	#[inline]
	pub(crate) fn trailer_from_encoded(encoded: &[u8]) -> u64 {
		let n = encoded.len() - 16;
		read_u64_be(encoded, n)
	}

	/// Extract seq_num from encoded key without allocation
	#[inline]
	pub(crate) fn seq_num_from_encoded(encoded: &[u8]) -> u64 {
		trailer_to_seq_num(Self::trailer_from_encoded(encoded))
	}

	pub(crate) fn encode(&self) -> Vec<u8> {
		let mut buf = self.user_key.clone();
		buf.extend_from_slice(&self.trailer.to_be_bytes());
		buf.extend_from_slice(&self.timestamp.to_be_bytes());
		buf
	}

	#[inline]
	pub(crate) fn seq_num(&self) -> u64 {
		trailer_to_seq_num(self.trailer)
	}

	pub(crate) fn kind(&self) -> InternalKeyKind {
		trailer_to_kind(self.trailer)
	}

	#[inline]
	pub(crate) fn is_tombstone(&self) -> bool {
		is_delete_kind(self.kind())
	}

	pub(crate) fn is_hard_delete_marker(&self) -> bool {
		is_hard_delete_marker(self.kind())
	}

	pub(crate) fn is_replace(&self) -> bool {
		is_replace_kind(self.kind())
	}

	/// Compares this key with another key using timestamp-based ordering
	/// First compares by user key, then by timestamp (ascending - older
	/// timestamps first)
	pub(crate) fn cmp_by_timestamp(&self, other: &Self) -> Ordering {
		// First compare by user key (ascending)
		match self.user_key.cmp(&other.user_key) {
			// If user keys are equal, compare by timestamp (ascending - older timestamps first)
			Ordering::Equal => self.timestamp.cmp(&other.timestamp),
			ordering => ordering,
		}
	}
}

// Used only by memtable, sstable uses internal key comparator
impl Ord for InternalKey {
	fn cmp(&self, other: &Self) -> Ordering {
		// Same as InternalKey: user key, then sequence number, then kind, with
		// timestamp as final tiebreaker First compare by user key (ascending)
		match self.user_key.cmp(&other.user_key) {
			// If user keys are equal, compare by sequence number (descending)
			Ordering::Equal => other.seq_num().cmp(&self.seq_num()),
			ordering => ordering,
		}
	}
}

impl PartialOrd for InternalKey {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}
