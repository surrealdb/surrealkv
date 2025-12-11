pub(crate) mod block;
pub(crate) mod bloom;
pub(crate) mod filter_block;
pub(crate) mod index_block;
pub(crate) mod meta;
pub(crate) mod prefetch;
pub(crate) mod table;

use bytes::Bytes;
use std::cmp::Ordering;
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
		6 => InternalKeyKind::Replace,
		7 => InternalKeyKind::Separator,
		24 => InternalKeyKind::Max,
		_ => InternalKeyKind::Invalid,
	}
}

/// Extracts sequence number from trailer
/// This centralizes the seq_num extraction logic to avoid duplication
fn trailer_to_seq_num(trailer: u64) -> u64 {
	trailer >> 8
}

/// Checks if a key kind represents a tombstone (delete operation)
fn is_delete_kind(kind: InternalKeyKind) -> bool {
	matches!(
		kind,
		InternalKeyKind::Delete | InternalKeyKind::SoftDelete | InternalKeyKind::RangeDelete
	)
}

/// Checks if a key kind represents a hard delete (delete operation)
fn is_hard_delete_marker(kind: InternalKeyKind) -> bool {
	matches!(kind, InternalKeyKind::Delete | InternalKeyKind::RangeDelete)
}

/// Checks if a key kind represents a Replace operation
fn is_replace_kind(kind: InternalKeyKind) -> bool {
	matches!(kind, InternalKeyKind::Replace)
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InternalKey {
	pub(crate) user_key: Bytes,
	pub(crate) timestamp: u64, // System time in nanoseconds since epoch
	pub(crate) trailer: u64,   // (seq_num << 8) | kind
}

impl InternalKey {
	pub(crate) fn new(
		user_key: Bytes,
		seq_num: u64,
		kind: InternalKeyKind,
		timestamp: u64,
	) -> Self {
		Self {
			user_key,
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
		let user_key = Bytes::copy_from_slice(&encoded_key[..n]);

		Self {
			user_key,
			timestamp,
			trailer,
		}
	}

	/// Extract user key slice from encoded internal key bytes without allocation.
	/// The encoded format is: [user_key][trailer:8][timestamp:8]
	#[inline]
	pub(crate) fn extract_user_key(encoded_key: &[u8]) -> &[u8] {
		&encoded_key[..encoded_key.len().saturating_sub(16)]
	}

	/// Extract sequence number from encoded internal key bytes without allocation.
	/// The trailer is at bytes [len-16..len-8] and contains (seq_num << 8) | kind.
	#[inline]
	pub(crate) fn extract_seq_num(encoded_key: &[u8]) -> u64 {
		if encoded_key.len() < 16 {
			return 0;
		}
		let n = encoded_key.len() - 16;
		let trailer = u64::from_be_bytes(encoded_key[n..n + 8].try_into().unwrap());
		trailer >> 8
	}

	/// Compare two encoded internal keys without allocation.
	/// Comparison order: user_key ascending, then seq_num descending.
	/// This is the same ordering as InternalKey::cmp but operates on raw bytes.
	#[inline]
	pub(crate) fn compare_encoded(a: &[u8], b: &[u8]) -> Ordering {
		let a_user = Self::extract_user_key(a);
		let b_user = Self::extract_user_key(b);
		match a_user.cmp(b_user) {
			Ordering::Equal => {
				// Extract seq_num and compare in descending order
				let a_seq = Self::extract_seq_num(a);
				let b_seq = Self::extract_seq_num(b);
				b_seq.cmp(&a_seq) // Descending order
			}
			ord => ord,
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
		is_delete_kind(self.kind())
	}

	pub(crate) fn is_hard_delete_marker(&self) -> bool {
		is_hard_delete_marker(self.kind())
	}

	pub(crate) fn is_replace(&self) -> bool {
		is_replace_kind(self.kind())
	}

	/// Compares this key with another key using timestamp-based ordering
	/// First compares by user key, then by timestamp (ascending - older timestamps first)
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
		// Same as InternalKey: user key, then sequence number, then kind, with timestamp as final tiebreaker
		// First compare by user key (ascending)
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

impl Default for InternalKey {
	fn default() -> Self {
		Self {
			user_key: Bytes::new(),
			timestamp: 0,
			trailer: 0,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_compare_encoded_same_user_key_different_seq_num() {
		// Keys with same user_key should be ordered by seq_num in descending order
		let key1 = InternalKey::new(Bytes::from("key1"), 100, InternalKeyKind::Set, 0);
		let key2 = InternalKey::new(Bytes::from("key1"), 50, InternalKeyKind::Set, 0);

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();

		// key1 (seq=100) should come before key2 (seq=50) due to descending seq order
		assert_eq!(InternalKey::compare_encoded(&encoded1, &encoded2), Ordering::Less);
		assert_eq!(InternalKey::compare_encoded(&encoded2, &encoded1), Ordering::Greater);

		// Same key should be equal
		assert_eq!(InternalKey::compare_encoded(&encoded1, &encoded1), Ordering::Equal);
	}

	#[test]
	fn test_compare_encoded_different_user_key() {
		// Different user keys should be ordered lexicographically
		let key1 = InternalKey::new(Bytes::from("aaa"), 100, InternalKeyKind::Set, 0);
		let key2 = InternalKey::new(Bytes::from("bbb"), 100, InternalKeyKind::Set, 0);

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();

		// "aaa" < "bbb" lexicographically
		assert_eq!(InternalKey::compare_encoded(&encoded1, &encoded2), Ordering::Less);
		assert_eq!(InternalKey::compare_encoded(&encoded2, &encoded1), Ordering::Greater);
	}

	#[test]
	fn test_compare_encoded_matches_internal_key_ord() {
		// Verify that compare_encoded produces the same ordering as InternalKey::cmp
		let test_cases = vec![
			// (user_key1, seq1, user_key2, seq2)
			("key", 100, "key", 50),
			("key", 50, "key", 100),
			("aaa", 100, "bbb", 100),
			("bbb", 100, "aaa", 100),
			("key", 1, "key", 1000),
			("", 100, "", 50),
			("a", 1, "b", 1),
		];

		for (uk1, seq1, uk2, seq2) in test_cases {
			let key1 = InternalKey::new(Bytes::from(uk1), seq1, InternalKeyKind::Set, 0);
			let key2 = InternalKey::new(Bytes::from(uk2), seq2, InternalKeyKind::Set, 0);

			let expected = key1.cmp(&key2);
			let actual = InternalKey::compare_encoded(&key1.encode(), &key2.encode());

			assert_eq!(
				actual, expected,
				"Mismatch for ({}, seq={}) vs ({}, seq={}): expected {:?}, got {:?}",
				uk1, seq1, uk2, seq2, expected, actual
			);
		}
	}

	#[test]
	fn test_extract_seq_num() {
		let key = InternalKey::new(Bytes::from("test_key"), 12345, InternalKeyKind::Set, 0);
		let encoded = key.encode();

		assert_eq!(InternalKey::extract_seq_num(&encoded), 12345);
	}

	#[test]
	fn test_extract_user_key() {
		let key = InternalKey::new(Bytes::from("test_key"), 100, InternalKeyKind::Set, 0);
		let encoded = key.encode();

		assert_eq!(InternalKey::extract_user_key(&encoded), b"test_key");
	}

	#[test]
	fn test_compare_encoded_empty_user_key() {
		let key1 = InternalKey::new(Bytes::from(""), 100, InternalKeyKind::Set, 0);
		let key2 = InternalKey::new(Bytes::from(""), 50, InternalKeyKind::Set, 0);

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();

		// Empty user keys with different seq nums
		assert_eq!(InternalKey::compare_encoded(&encoded1, &encoded2), Ordering::Less);
	}

	#[test]
	fn test_compare_encoded_large_seq_nums() {
		let key1 =
			InternalKey::new(Bytes::from("key"), INTERNAL_KEY_SEQ_NUM_MAX, InternalKeyKind::Set, 0);
		let key2 = InternalKey::new(Bytes::from("key"), 1, InternalKeyKind::Set, 0);

		let encoded1 = key1.encode();
		let encoded2 = key2.encode();

		// MAX seq should come before 1 (descending order)
		assert_eq!(InternalKey::compare_encoded(&encoded1, &encoded2), Ordering::Less);
	}
}
