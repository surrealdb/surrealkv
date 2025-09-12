pub(crate) mod block;
pub(crate) mod bloom;
pub(crate) mod filter_block;
pub(crate) mod index_block;
pub(crate) mod meta;
pub(crate) mod table;

use std::cmp::{Ordering, Reverse};
use std::fmt::Debug;
use std::sync::Arc;

// FilterPolicy is an algorithm for probabilistically encoding a set of keys.
// It is used to create a filter block that can be stored in a block-based
// file format.
pub trait FilterPolicy: Send + Sync {
	fn name(&self) -> &str;
	fn may_contain(&self, filter: &[u8], key: &[u8]) -> bool;
	fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8>;
}

/// Trait for internal key implementations that provide key format and operations
/// for the LSM tree. This allows different internal key formats to be used
/// in different scenarios while maintaining a consistent interface.
pub trait InternalKeyTrait:
	Clone + Debug + PartialEq + Eq + PartialOrd + Ord + Send + Sync + Default + 'static
{
	/// Create a new internal key from user key, sequence number, and kind
	fn new(user_key: Vec<u8>, seq_num: u64, kind: InternalKeyKind) -> Self;

	/// Decode an internal key from its encoded byte representation
	fn decode(encoded_key: &[u8]) -> Self;

	/// Encode this internal key to its byte representation
	fn encode(&self) -> Vec<u8>;

	/// Get the user key portion
	fn user_key(&self) -> &Arc<[u8]>;

	/// Get the sequence number
	fn seq_num(&self) -> u64;

	/// Get the key kind/type
	fn kind(&self) -> InternalKeyKind;

	/// Check if this key represents a tombstone (delete operation)
	fn is_tombstone(&self) -> bool;

	/// Calculate the size of this key in bytes
	fn size(&self) -> usize;
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
		match value {
			0 => InternalKeyKind::Delete,
			1 => InternalKeyKind::SoftDelete,
			2 => InternalKeyKind::Set,
			3 => InternalKeyKind::Merge,
			4 => InternalKeyKind::LogData,
			5 => InternalKeyKind::RangeDelete,
			6 => InternalKeyKind::Separator,
			7 => InternalKeyKind::Max,
			8 => InternalKeyKind::Invalid,
			_ => InternalKeyKind::Invalid, // Default to Invalid for unknown values
		}
	}
}

// This is the maximum valid sequence number that can be stored in the upper 56 bits of a 64-bit integer.
// 1 << 56 shifts the number 1 left by 56 bits, resulting in a binary number with a 1 followed by 56 zeros.
// Subtracting 1 gives a binary number with 56 ones, which is the maximum value for 56 bits.
pub(crate) const INTERNAL_KEY_SEQ_NUM_MAX: u64 = (1 << 56) - 1;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
// InternalKey is a key used for on-disk representation of a key.
//
// <user-key>.<kind>.<seq-num>
//
// It consists of the user key followed by 8-bytes of metadata:
//   - 1 byte for the type of internal key: delete or set,
//   - 7 bytes for a uint56 sequence number, in big-endian format.
pub struct InternalKey {
	pub(crate) user_key: Arc<[u8]>,
	pub(crate) trailer: u64,
}

impl InternalKey {
	pub(crate) fn new(user_key: Vec<u8>, seq_num: u64, kind: InternalKeyKind) -> Self {
		Self {
			user_key: Arc::from(user_key.into_boxed_slice()),
			trailer: (seq_num << 8) | kind as u64,
		}
	}

	// Calculates the size of the InternalKey in bytes.
	pub(crate) fn size(&self) -> usize {
		let fixed_size = std::mem::size_of::<u64>() + std::mem::size_of::<Arc<[u8]>>();
		let heap_allocated_size = self.user_key.len();
		fixed_size + heap_allocated_size
	}

	#[inline]
	pub(crate) fn decode(encoded_key: &[u8]) -> Self {
		let n = encoded_key.len() - 8;
		let trailer = u64::from_be_bytes(encoded_key[n..].try_into().unwrap());
		let user_key = Arc::<[u8]>::from(&encoded_key[..n]);
		Self {
			user_key,
			trailer,
		}
	}

	#[inline]
	pub(crate) fn encode(&self) -> Vec<u8> {
		let mut buf = self.user_key.as_ref().to_vec();
		buf.extend_from_slice(&self.trailer.to_be_bytes());
		buf
	}

	// Returns the sequence number component of the key.
	pub(crate) fn seq_num(&self) -> u64 {
		self.trailer >> 8
	}

	pub(crate) fn kind(&self) -> InternalKeyKind {
		let kind_byte = self.trailer as u8; // Extract the last byte
		match kind_byte {
			0 => InternalKeyKind::Delete,
			1 => InternalKeyKind::SoftDelete,
			2 => InternalKeyKind::Set,
			3 => InternalKeyKind::Merge,
			4 => InternalKeyKind::LogData,
			5 => InternalKeyKind::RangeDelete,
			6 => InternalKeyKind::Separator,
			7 => InternalKeyKind::Max,
			8 => InternalKeyKind::Invalid,
			_ => InternalKeyKind::Invalid,
		}
	}

	pub(crate) fn is_tombstone(&self) -> bool {
		let kind = self.kind();
		if kind == InternalKeyKind::Delete
			|| kind == InternalKeyKind::SoftDelete
			|| kind == InternalKeyKind::RangeDelete
		{
			return true;
		}

		false
	}
}

// Implement the InternalKeyTrait for the default InternalKey
impl InternalKeyTrait for InternalKey {
	fn new(user_key: Vec<u8>, seq_num: u64, kind: InternalKeyKind) -> Self {
		Self::new(user_key, seq_num, kind)
	}

	fn decode(encoded_key: &[u8]) -> Self {
		Self::decode(encoded_key)
	}

	fn encode(&self) -> Vec<u8> {
		self.encode()
	}

	fn user_key(&self) -> &Arc<[u8]> {
		&self.user_key
	}

	fn seq_num(&self) -> u64 {
		self.seq_num()
	}

	fn kind(&self) -> InternalKeyKind {
		self.kind()
	}

	fn is_tombstone(&self) -> bool {
		self.is_tombstone()
	}

	fn size(&self) -> usize {
		self.size()
	}
}

// Compares two internal keys. For equal user keys, internal keys compare in
// descending sequence number order. For equal user keys and sequence numbers,
// internal keys compare in descending kind order.
// Reverse order is used for trailer (seq_num) comparison.
impl Ord for InternalKey {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		match (&self.user_key, Reverse(self.seq_num()))
			.cmp(&(&other.user_key, Reverse(other.seq_num())))
		{
			Ordering::Equal => Reverse(self.kind() as u8).cmp(&Reverse(other.kind() as u8)),
			ordering => ordering,
		}
	}
}

impl PartialOrd for InternalKey {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}
