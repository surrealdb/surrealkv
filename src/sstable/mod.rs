pub(crate) mod block;
pub(crate) mod bloom;
pub(crate) mod filter_block;
pub(crate) mod index_block;
pub(crate) mod meta;
pub(crate) mod table;

use std::cmp::Ordering;
use std::fmt::Debug;
use std::ops::{Index, Range, RangeFrom, RangeFull, RangeTo};

use crate::UserKey;

// This is the maximum valid sequence number that can be stored in the upper 56
// bits of a 64-bit integer. 1 << 56 shifts the number 1 left by 56 bits,
// resulting in a binary number with a 1 followed by 56 zeros. Subtracting 1
// gives a binary number with 56 ones, which is the maximum value for 56 bits.
pub(crate) const INTERNAL_KEY_SEQ_NUM_MAX: u64 = (1 << 56) - 1;
pub(crate) const INTERNAL_KEY_TIMESTAMP_MAX: u64 = u64::MAX;

// Helper function for reading u64 from byte slices without unwrap()
// Safe to use when bounds have already been checked
#[inline(always)]
const fn read_u64_be(buffer: &[u8], offset: usize) -> u64 {
	// SAFETY: Caller must ensure buffer has at least offset + 8 bytes
	unsafe { u64::from_be_bytes(*(buffer.as_ptr().add(offset) as *const [u8; 8])) }
}

/// Converts a trailer byte to InternalKeyKind
/// This centralizes the kind conversion logic to avoid duplication and errors
#[inline(always)]
const fn trailer_to_kind(trailer: u64) -> InternalKeyKind {
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
const fn trailer_to_seq_num(trailer: u64) -> u64 {
	trailer >> 8
}

/// Checks if a key kind represents a tombstone (delete operation)
#[inline(always)]
const fn is_delete_kind(kind: InternalKeyKind) -> bool {
	matches!(
		kind,
		InternalKeyKind::Delete | InternalKeyKind::SoftDelete | InternalKeyKind::RangeDelete
	)
}

/// Checks if a key kind represents a hard delete (delete operation)
#[inline(always)]
const fn is_hard_delete_marker(kind: InternalKeyKind) -> bool {
	matches!(kind, InternalKeyKind::Delete | InternalKeyKind::RangeDelete)
}

/// Checks if a key kind represents a Replace operation
#[inline(always)]
const fn is_replace_kind(kind: InternalKeyKind) -> bool {
	matches!(kind, InternalKeyKind::Replace)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
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
///
/// An internal key contains the user key, timestamp, and trailer.
/// The user key is the key that is used to identify the data.
/// The timestamp is the system time in nanoseconds since epoch.
/// The trailer is the sequence number and kind of the key `(seq_num << 8) | kind`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
#[repr(transparent)]
pub(crate) struct InternalKey(Vec<u8>);

impl InternalKey {
	/// Returns a zeroed internal key.
	///
	/// Zero means vec![] user key, 0 seq_num, Separator kind `Separator`, 0 timestamp.
	///
	/// TODO(STU): Check this.
	pub(crate) fn none() -> Self {
		Self::encode(&[], 0, InternalKeyKind::Separator, 0)
	}

	pub(crate) fn encode(
		user_key: &[u8],
		seq_num: u64,
		kind: InternalKeyKind,
		timestamp: u64,
	) -> Self {
		let mut buf = Vec::with_capacity(user_key.len() + 16);
		buf.extend_from_slice(&user_key);
		buf.extend_from_slice(&((seq_num << 8) | kind as u64).to_be_bytes());
		buf.extend_from_slice(&timestamp.to_be_bytes());
		Self(buf)
	}

	pub(crate) const fn len(&self) -> usize {
		self.0.len()
	}

	pub(crate) fn new(encoded_key: Vec<u8>) -> Self {
		assert!(encoded_key.len() >= 16);
		Self(encoded_key)
	}

	#[inline(always)]
	pub(crate) fn user_key(&self) -> &[u8] {
		&self.0[..self.0.len() - 16]
	}

	#[inline]
	pub(crate) fn take_user_key(mut self) -> UserKey {
		self.0.truncate(self.0.len() - 16);
		self.0
	}

	#[inline(always)]
	pub(crate) const fn seq_num(&self) -> u64 {
		trailer_to_seq_num(self.trailer())
	}

	#[inline]
	pub(crate) fn with_seq_num(mut self, seq_num: u64) -> Self {
		let new_trailer = (seq_num << 8) | self.kind() as u64;
		let new_trailer_bytes = new_trailer.to_be_bytes();

		let start = self.0.len() - 16;
		self.0[start..start + 8].copy_from_slice(&new_trailer_bytes);
		self
	}

	#[inline(always)]
	pub(crate) const fn timestamp(&self) -> u64 {
		read_u64_be(self.0.as_slice(), self.0.len() - 8)
	}

	#[inline]
	pub(crate) fn set_timestamp(&mut self, timestamp: u64) {
		let start = self.0.len() - 8;
		let timestamp_bytes = timestamp.to_be_bytes();
		self.0[start..start + 8].copy_from_slice(&timestamp_bytes);
	}

	#[inline(always)]
	const fn trailer(&self) -> u64 {
		read_u64_be(self.0.as_slice(), self.0.len() - 16)
	}

	#[inline(always)]
	pub(crate) const fn kind(&self) -> InternalKeyKind {
		trailer_to_kind(self.trailer())
	}

	#[inline(always)]
	pub(crate) const fn is_tombstone(&self) -> bool {
		is_delete_kind(self.kind())
	}

	#[inline(always)]
	pub(crate) const fn is_hard_delete_marker(&self) -> bool {
		is_hard_delete_marker(self.kind())
	}

	#[inline(always)]
	pub(crate) const fn is_replace(&self) -> bool {
		is_replace_kind(self.kind())
	}

	/// Compares this key with another key using timestamp-based ordering
	/// First compares by user key, then by timestamp (ascending - older
	/// timestamps first)
	pub(crate) fn cmp_by_timestamp(&self, other: &Self) -> Ordering {
		// First compare by user key (ascending)
		match self.user_key().cmp(other.user_key()) {
			// If user keys are equal, compare by timestamp (ascending - older timestamps first)
			Ordering::Equal => self.timestamp().cmp(&other.timestamp()),
			ordering => ordering,
		}
	}

	#[inline]
	pub(crate) fn clear(&mut self) {
		self.0.clear();
	}

	#[inline]
	pub(crate) fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	#[inline]
	pub(crate) fn replace(&mut self, other: &Self) {
		self.0.clone_from(&other.0);
	}

	#[inline]
	pub(crate) fn truncate(&mut self, len: usize) {
		self.0.truncate(len);
	}

	#[inline]
	pub(crate) fn extend_from_slice(&mut self, slice: &[u8]) {
		self.0.extend_from_slice(slice);
	}

	pub(crate) fn iter(&self) -> impl Iterator<Item = &u8> {
		self.0.iter()
	}
}

impl Index<Range<usize>> for InternalKey {
	type Output = [u8];

	fn index(&self, index: Range<usize>) -> &Self::Output {
		&self.0[index]
	}
}

impl Index<RangeFrom<usize>> for InternalKey {
	type Output = [u8];

	fn index(&self, index: RangeFrom<usize>) -> &Self::Output {
		&self.0[index]
	}
}

impl Index<RangeTo<usize>> for InternalKey {
	type Output = [u8];

	fn index(&self, index: RangeTo<usize>) -> &Self::Output {
		&self.0[index]
	}
}

impl Index<RangeFull> for InternalKey {
	type Output = [u8];

	fn index(&self, index: RangeFull) -> &Self::Output {
		&self.0[index]
	}
}

// Used only by memtable, sstable uses internal key comparator
impl Ord for InternalKey {
	fn cmp(&self, other: &Self) -> Ordering {
		// Same as InternalKey: user key, then sequence number, then kind, with
		// timestamp as final tiebreaker First compare by user key (ascending)
		match self.user_key().cmp(other.user_key()) {
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
