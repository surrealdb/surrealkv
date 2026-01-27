//! Comparator implementations for key ordering.

use std::cmp::Ordering;
use std::sync::Arc;

use crate::{InternalKey, InternalKeyKind, INTERNAL_KEY_SEQ_NUM_MAX, INTERNAL_KEY_TIMESTAMP_MAX};

/// A trait for comparing keys in a key-value store.
///
/// This trait defines methods for comparing keys, generating separator keys,
/// generating successor keys, and retrieving the name of the comparator.
pub trait Comparator: Send + Sync {
	/// Compares two keys `a` and `b`.
	fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;

	/// Generates a separator key between two keys `from` and `to`.
	///
	/// This method should return a key that is greater than or equal to `from`
	/// and less than `to`. It is used to optimize the storage layout by
	/// finding a midpoint key.
	fn separator(&self, from: &[u8], to: &[u8]) -> Vec<u8>;

	/// Generates the successor key of a given key.
	///
	/// This method should return a key that is lexicographically greater than
	/// the given key. It is used to find the next key in the key space.
	fn successor(&self, key: &[u8]) -> Vec<u8>;

	/// Retrieves the name of the comparator.
	fn name(&self) -> &str;
}

/// A bytewise comparator that compares keys lexicographically.
#[derive(Default, Clone, Copy)]
pub struct BytewiseComparator {}

impl BytewiseComparator {}

impl Comparator for BytewiseComparator {
	#[inline]
	fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
		a.cmp(b)
	}

	#[inline]
	fn name(&self) -> &'static str {
		"leveldb.BytewiseComparator"
	}

	#[inline]
	/// Generates a separator key between two byte slices `a` (start) and `b`
	/// (limit).
	///
	/// 1. Find the common prefix
	/// 2. If one string is a prefix of the other, return unchanged
	/// 3. At first differing byte, try to increment and truncate
	/// 4. If that would exceed limit, scan forward for a non-0xFF byte to increment
	/// 5. If no shortening possible, return unchanged
	fn separator(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
		let min_length = std::cmp::min(a.len(), b.len());
		let mut diff_index = 0;

		// Find length of common prefix
		while diff_index < min_length && a[diff_index] == b[diff_index] {
			diff_index += 1;
		}

		if diff_index >= min_length {
			// Do not shorten if one string is a prefix of the other
			return a.to_vec();
		}

		let start_byte = a[diff_index];
		let limit_byte = b[diff_index];

		if start_byte >= limit_byte {
			// Cannot shorten since limit is smaller than start or start is
			// already the shortest possible.
			return a.to_vec();
		}

		// start_byte < limit_byte
		if diff_index < b.len() - 1 || start_byte + 1 < limit_byte {
			// Safe to increment and truncate
			let mut result = Vec::from(&a[..=diff_index]);
			result[diff_index] += 1;
			debug_assert!(self.compare(&result, b) == Ordering::Less);
			return result;
		}

		// Incrementing the current byte would make start >= limit.
		// Skip this byte and find the first non-0xFF byte in start to increment.
		diff_index += 1;

		while diff_index < a.len() {
			if a[diff_index] < 0xff {
				let mut result = Vec::from(&a[..=diff_index]);
				result[diff_index] += 1;
				debug_assert!(self.compare(&result, b) == Ordering::Less);
				return result;
			}
			diff_index += 1;
		}

		// Could not shorten, return original
		a.to_vec()
	}

	#[inline]
	/// Generates the successor key of a given byte slice `key`.
	///
	/// Find the first non-0xFF byte, increment it, and truncate.
	/// If all bytes are 0xFF, leave it unchanged.
	fn successor(&self, key: &[u8]) -> Vec<u8> {
		let mut result = key.to_vec();
		for i in 0..key.len() {
			if key[i] != 0xff {
				result[i] += 1;
				result.resize(i + 1, 0);
				return result;
			}
		}
		// All bytes are 0xFF - leave unchanged
		result
	}
}

#[derive(Clone)]
pub struct InternalKeyComparator {
	user_comparator: Arc<dyn Comparator>,
}

impl InternalKeyComparator {
	pub fn new(user_comparator: Arc<dyn Comparator>) -> Self {
		Self {
			user_comparator,
		}
	}
}

impl Comparator for InternalKeyComparator {
	fn name(&self) -> &'static str {
		"surrealkv.InternalKeyComparator"
	}

	fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
		let user_key_a = InternalKey::user_key_from_encoded(a);
		let user_key_b = InternalKey::user_key_from_encoded(b);

		match self.user_comparator.compare(user_key_a, user_key_b) {
			Ordering::Equal => {
				// Compare seq_num in descending order (higher = more recent)
				let seq_a = InternalKey::seq_num_from_encoded(a);
				let seq_b = InternalKey::seq_num_from_encoded(b);
				seq_b.cmp(&seq_a)
			}
			ord => ord,
		}
	}

	/// Generates a separator key between two internal keys.
	///
	/// 1. Extract user keys and compute a separator
	/// 2. If separator is same length or shorter AND logically larger, use max seq_num
	/// 3. Otherwise return the original key unchanged
	fn separator(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
		if a == b {
			return a.to_vec();
		}

		// Decode keys using InternalKey
		let key_a = InternalKey::decode(a);
		let key_b = InternalKey::decode(b);

		// If user keys are different, compute a separator for user keys
		if self.user_comparator.compare(key_a.user_key.as_ref(), key_b.user_key.as_ref())
			!= Ordering::Equal
		{
			let sep =
				self.user_comparator.separator(key_a.user_key.as_ref(), key_b.user_key.as_ref());

			// Use MAX_SEQUENCE_NUMBER when separator is same length or shorter AND
			// logically larger
			if sep.len() <= key_a.user_key.len()
				&& self.user_comparator.compare(key_a.user_key.as_ref(), &sep) == Ordering::Less
			{
				let result = InternalKey::new(
					sep,
					INTERNAL_KEY_SEQ_NUM_MAX,
					InternalKeyKind::Separator,
					INTERNAL_KEY_TIMESTAMP_MAX,
				);
				return result.encode();
			}
		}

		// Fallback: Return original key unchanged
		a.to_vec()
	}

	/// Generates a successor key for an internal key.
	fn successor(&self, key: &[u8]) -> Vec<u8> {
		let internal_key = InternalKey::decode(key);
		let user_key_succ = self.user_comparator.successor(internal_key.user_key.as_ref());

		// If successor is same length or shorter AND logically larger, use max seq_num
		if user_key_succ.len() <= internal_key.user_key.len()
			&& self.user_comparator.compare(internal_key.user_key.as_ref(), &user_key_succ)
				== Ordering::Less
		{
			let result = InternalKey::new(
				user_key_succ,
				INTERNAL_KEY_SEQ_NUM_MAX,
				InternalKeyKind::Separator,
				INTERNAL_KEY_TIMESTAMP_MAX,
			);
			return result.encode();
		}

		// Fallback: Return original key unchanged
		key.to_vec()
	}
}

/// A comparator that compares internal keys first by user key, then by
/// timestamp This is used for versioned queries where we want to order by user
/// key and timestamp
#[derive(Clone)]
pub struct TimestampComparator {
	user_comparator: Arc<dyn Comparator>,
}

impl TimestampComparator {
	pub fn new(user_comparator: Arc<dyn Comparator>) -> Self {
		Self {
			user_comparator,
		}
	}
}

impl Comparator for TimestampComparator {
	fn name(&self) -> &'static str {
		"surrealkv.TimestampComparator"
	}

	fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
		// Decode internal keys using InternalKey
		let key_a = InternalKey::decode(a);
		let key_b = InternalKey::decode(b);
		// Use the timestamp-based comparison method
		key_a.cmp_by_timestamp(&key_b)
	}

	/// Generates a separator key between two internal keys.
	///
	/// 1. Extract user keys and compute a separator
	/// 2. If separator is same length or shorter AND logically larger, use max seq_num/timestamp
	/// 3. Otherwise return the original key unchanged
	fn separator(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
		if a == b {
			return a.to_vec();
		}

		// Decode keys using InternalKey
		let key_a = InternalKey::decode(a);
		let key_b = InternalKey::decode(b);

		// If user keys are different, compute a separator for user keys
		if self.user_comparator.compare(key_a.user_key.as_ref(), key_b.user_key.as_ref())
			!= Ordering::Equal
		{
			let sep =
				self.user_comparator.separator(key_a.user_key.as_ref(), key_b.user_key.as_ref());

			// Use MAX_TIMESTAMP when separator is same length or shorter AND logically
			// larger
			if sep.len() <= key_a.user_key.len()
				&& self.user_comparator.compare(key_a.user_key.as_ref(), &sep) == Ordering::Less
			{
				let result = InternalKey::new(
					sep,
					INTERNAL_KEY_SEQ_NUM_MAX,
					InternalKeyKind::Separator,
					INTERNAL_KEY_TIMESTAMP_MAX,
				);
				return result.encode();
			}
		}

		// Fallback: Return original key unchanged
		a.to_vec()
	}

	/// Generates a successor key for an internal key.
	fn successor(&self, key: &[u8]) -> Vec<u8> {
		let internal_key = InternalKey::decode(key);
		let user_key_succ = self.user_comparator.successor(internal_key.user_key.as_ref());

		// If successor is same length or shorter AND logically larger, use max
		// seq_num/timestamp
		if user_key_succ.len() <= internal_key.user_key.len()
			&& self.user_comparator.compare(internal_key.user_key.as_ref(), &user_key_succ)
				== Ordering::Less
		{
			let result = InternalKey::new(
				user_key_succ,
				INTERNAL_KEY_SEQ_NUM_MAX,
				InternalKeyKind::Separator,
				INTERNAL_KEY_TIMESTAMP_MAX,
			);
			return result.encode();
		}

		// Fallback: Return original key unchanged
		key.to_vec()
	}
}

#[cfg(test)]
mod tests {
	use rand::{Rng, SeedableRng};

	use super::*;

	// ============================================================================
	// Basic Separator Tests
	// ============================================================================

	#[test]
	fn test_find_shortest_separator_basic() {
		let cmp = BytewiseComparator::default();

		// Test case: "abc1xyz" vs "abc3xy" should produce "abc2"
		let s1 = b"abc1xyz";
		let s2 = b"abc3xy";
		let sep = cmp.separator(s1, s2);
		assert_eq!(sep, b"abc2");

		// Another basic test
		let sep = cmp.separator(b"abcd", b"abcf");
		assert_eq!(sep, b"abce");
	}

	#[test]
	fn test_separator_equal_strings() {
		let cmp = BytewiseComparator::default();

		// Equal strings should return unchanged
		let s = b"abcdef";
		let sep = cmp.separator(s, s);
		assert_eq!(sep, s.to_vec());
	}

	#[test]
	fn test_separator_prefix_cases() {
		let cmp = BytewiseComparator::default();

		// One string is prefix of another - should return unchanged
		let sep = cmp.separator(b"abc", b"abcdef");
		assert_eq!(sep, b"abc".to_vec());

		let sep = cmp.separator(b"abcdef", b"abc");
		assert_eq!(sep, b"abcdef".to_vec());
	}

	#[test]
	fn test_separator_start_greater_than_limit() {
		let cmp = BytewiseComparator::default();

		// When start >= limit at diff position, return unchanged
		let sep = cmp.separator(b"abd", b"abc");
		assert_eq!(sep, b"abd".to_vec());
	}

	#[test]
	fn test_separator_adjacent_bytes() {
		let cmp = BytewiseComparator::default();

		// "abc" vs "abd" - can't increment 'c' to 'd' because that equals limit
		// Should scan forward (but no more bytes), so return original
		let sep = cmp.separator(b"abc", b"abd");
		assert_eq!(sep, b"abc".to_vec());

		// "abc" vs "abe" - can increment 'c' to 'd'
		let sep = cmp.separator(b"abc", b"abe");
		assert_eq!(sep, b"abd");
	}

	#[test]
	fn test_separator_scan_forward() {
		let cmp = BytewiseComparator::default();

		// "AA1AAA" vs "AA2" - at diff position, incrementing would exceed
		// Scan forward to find non-0xFF byte
		let sep = cmp.separator(b"AA1AAA", b"AA2");
		assert_eq!(sep, b"AA1B");
	}

	#[test]
	fn test_separator_all_0xff_suffix() {
		let cmp = BytewiseComparator::default();

		// Start has all 0xFF after diff position - can't shorten
		let start = &[b'A', b'A', 0x01, 0xff, 0xff, 0xff];
		let limit = &[b'A', b'A', 0x02];
		let sep = cmp.separator(start, limit);
		// Should return original since all bytes after diff are 0xFF
		assert_eq!(sep, start.to_vec());
	}

	#[test]
	fn test_separator_empty_strings() {
		let cmp = BytewiseComparator::default();

		// Empty strings
		let sep = cmp.separator(b"", b"");
		assert_eq!(sep, b"".to_vec());

		// Empty start with non-empty limit
		let sep = cmp.separator(b"", b"abc");
		assert_eq!(sep, b"".to_vec());
	}

	#[test]
	fn test_separator_wide_gap() {
		let cmp = BytewiseComparator::default();

		// Wide gap allows truncation
		let sep = cmp.separator(b"abc", b"zzz");
		assert_eq!(sep, b"b");
	}

	// ============================================================================
	// Basic Successor Tests
	// ============================================================================

	#[test]
	fn test_find_short_successor_basic() {
		let cmp = BytewiseComparator::default();

		// Basic successor tests
		assert_eq!(cmp.successor(b"abcd"), b"b");
		assert_eq!(cmp.successor(b"zzzz"), b"{"); // 'z' + 1 = '{'
	}

	#[test]
	fn test_successor_all_0xff() {
		let cmp = BytewiseComparator::default();

		// All 0xFF - leave unchanged
		let key = vec![0xff, 0xff, 0xff];
		let succ = cmp.successor(&key);
		assert_eq!(succ, key);
	}

	#[test]
	fn test_successor_empty() {
		let cmp = BytewiseComparator::default();

		// Empty key - leave unchanged
		let succ = cmp.successor(b"");
		assert_eq!(succ, b"".to_vec());
	}

	#[test]
	fn test_successor_with_0xff_prefix() {
		let cmp = BytewiseComparator::default();

		// 0xFF prefix followed by incrementable byte
		let key = vec![0xff, 0xff, b'a'];
		let succ = cmp.successor(&key);
		assert_eq!(succ, vec![0xff, 0xff, b'b']);
	}

	#[test]
	fn test_successor_single_byte() {
		let cmp = BytewiseComparator::default();

		assert_eq!(cmp.successor(b"a"), b"b");
		assert_eq!(cmp.successor(&[0x00]), vec![0x01]);
		assert_eq!(cmp.successor(&[0xfe]), vec![0xff]);
		assert_eq!(cmp.successor(&[0xff]), vec![0xff]); // unchanged
	}

	// ============================================================================
	// Randomized Fuzz Test
	// ============================================================================

	#[test]
	fn test_separator_successor_randomized() {
		let cmp = BytewiseComparator::default();

		// Boundary characters for edge case testing
		let char_list: [u8; 6] = [0, 1, 2, 253, 254, 255];

		let mut rng = rand::rngs::StdRng::seed_from_u64(301);

		for _ in 0..1000 {
			// Generate random size for s1 (skewed toward smaller sizes)
			let size1 = rng.random_range(0..16);

			// size2 is either random or within [-2, +2] of size1
			let size2 = if rng.random_bool(0.5) {
				rng.random_range(0..16)
			} else {
				let diff = rng.random_range(0..5) - 2;
				let tmp = size1 as i32 + diff;
				tmp.max(0) as usize
			};

			// Generate s1
			let mut s1 = Vec::with_capacity(size1);
			for _ in 0..size1 {
				if rng.random_bool(0.5) {
					s1.push(rng.random::<u8>());
				} else {
					s1.push(char_list[rng.random_range(0..char_list.len())]);
				}
			}

			// Generate s2 based on s1, then modify
			let mut s2 = s1.clone();
			s2.resize(size2, 0);

			if !s2.is_empty() {
				let mut pos = s2.len() - 1;
				loop {
					if pos >= s1.len() || rng.random_ratio(1, 4) {
						s2[pos] = rng.random::<u8>();
					} else if rng.random_ratio(1, 4) {
						break;
					} else {
						let diff = rng.random_range(0..5) - 2;
						let s1_char = s1[pos] as i32;
						let s2_char = (s1_char + diff).clamp(0, 255) as u8;
						s2[pos] = s2_char;
					}
					if pos == 0 {
						break;
					}
					pos -= 1;
				}
			}

			// Test separators in both directions
			for rev in 0..2 {
				if rev == 1 {
					std::mem::swap(&mut s1, &mut s2);
				}

				let separator = cmp.separator(&s1, &s2);

				if s1 == s2 {
					// Equal strings should return unchanged
					assert_eq!(s1, separator, "Equal strings should return unchanged");
				} else if s1 < s2 {
					// Valid separator invariants
					assert!(
						s1 <= separator,
						"Separator must be >= start. s1={:?}, sep={:?}",
						s1,
						separator
					);
					assert!(
						separator < s2,
						"Separator must be < limit. sep={:?}, s2={:?}",
						separator,
						s2
					);
					assert!(
						separator.len() <= s1.len().max(s2.len()),
						"Separator should not be longer than max(s1, s2)"
					);
				} else {
					// s1 > s2: separator should equal s1 (can't shorten)
					assert_eq!(s1, separator, "When s1 > s2, separator should equal s1");
				}
			}

			// Test successor
			let succ = cmp.successor(&s1);
			assert!(succ >= s1, "Successor must be >= original. s1={:?}, succ={:?}", s1, succ);
		}
	}

	// ============================================================================
	// Additional Edge Cases
	// ============================================================================

	#[test]
	fn test_separator_binary_data() {
		let cmp = BytewiseComparator::default();

		// Binary data with null bytes
		let s1 = &[0x00, 0x01, 0x02];
		let s2 = &[0x00, 0x01, 0x05];
		let sep = cmp.separator(s1, s2);
		assert_eq!(sep, vec![0x00, 0x01, 0x03]);

		// Binary data at boundaries
		let s1 = &[0x00, 0x00, 0x00];
		let s2 = &[0x00, 0x00, 0x10];
		let sep = cmp.separator(s1, s2);
		assert_eq!(sep, vec![0x00, 0x00, 0x01]);
	}

	#[test]
	fn test_separator_long_common_prefix() {
		let cmp = BytewiseComparator::default();

		// Long common prefix
		let s1 = b"aaaaaaaaaaaaaaaaaaaab";
		let s2 = b"aaaaaaaaaaaaaaaaaaaad";
		let sep = cmp.separator(s1, s2);
		assert_eq!(sep, b"aaaaaaaaaaaaaaaaaaaac");
	}

	#[test]
	fn test_compare_basic() {
		let cmp = BytewiseComparator::default();

		assert_eq!(cmp.compare(b"abc", b"abc"), Ordering::Equal);
		assert_eq!(cmp.compare(b"abc", b"abd"), Ordering::Less);
		assert_eq!(cmp.compare(b"abd", b"abc"), Ordering::Greater);
		assert_eq!(cmp.compare(b"abc", b"abcd"), Ordering::Less);
		assert_eq!(cmp.compare(b"abcd", b"abc"), Ordering::Greater);
		assert_eq!(cmp.compare(b"", b""), Ordering::Equal);
		assert_eq!(cmp.compare(b"", b"a"), Ordering::Less);
	}

	#[test]
	fn test_comparator_name() {
		let cmp = BytewiseComparator::default();
		assert_eq!(cmp.name(), "leveldb.BytewiseComparator");
	}

	// ============================================================================
	// InternalKeyComparator Tests
	// ============================================================================

	/// Helper to create an encoded internal key for testing
	fn ikey(user_key: &[u8], seq: u64, kind: InternalKeyKind) -> Vec<u8> {
		InternalKey::new(user_key.to_vec(), seq, kind, 0).encode()
	}

	/// Helper to create an encoded internal key with max seq num (for expected
	/// separator results)
	fn ikey_max_seq(user_key: &[u8]) -> Vec<u8> {
		InternalKey::new(
			user_key.to_vec(),
			INTERNAL_KEY_SEQ_NUM_MAX,
			InternalKeyKind::Separator,
			INTERNAL_KEY_TIMESTAMP_MAX,
		)
		.encode()
	}

	#[test]
	fn test_internal_key_short_separator_same_user_key() {
		let cmp = InternalKeyComparator::new(Arc::new(BytewiseComparator::default()));

		// When user keys are same, separator should return unchanged
		// (regardless of sequence numbers)
		assert_eq!(
			ikey(b"foo", 100, InternalKeyKind::Set),
			cmp.separator(
				&ikey(b"foo", 100, InternalKeyKind::Set),
				&ikey(b"foo", 99, InternalKeyKind::Set)
			)
		);

		assert_eq!(
			ikey(b"foo", 100, InternalKeyKind::Set),
			cmp.separator(
				&ikey(b"foo", 100, InternalKeyKind::Set),
				&ikey(b"foo", 101, InternalKeyKind::Set)
			)
		);

		assert_eq!(
			ikey(b"foo", 100, InternalKeyKind::Set),
			cmp.separator(
				&ikey(b"foo", 100, InternalKeyKind::Set),
				&ikey(b"foo", 100, InternalKeyKind::Set)
			)
		);

		assert_eq!(
			ikey(b"foo", 100, InternalKeyKind::Set),
			cmp.separator(
				&ikey(b"foo", 100, InternalKeyKind::Set),
				&ikey(b"foo", 100, InternalKeyKind::Delete)
			)
		);
	}

	#[test]
	fn test_internal_key_short_separator_misordered() {
		let cmp = InternalKeyComparator::new(Arc::new(BytewiseComparator::default()));

		// When user keys are misordered (start > limit), return unchanged
		assert_eq!(
			ikey(b"foo", 100, InternalKeyKind::Set),
			cmp.separator(
				&ikey(b"foo", 100, InternalKeyKind::Set),
				&ikey(b"bar", 99, InternalKeyKind::Set)
			)
		);
	}

	#[test]
	fn test_internal_key_short_separator_different_user_keys() {
		let cmp = InternalKeyComparator::new(Arc::new(BytewiseComparator::default()));

		// When user keys are different and correctly ordered, should shorten with max
		// seq "foo" vs "hello" -> "g" with max seq
		let result = cmp.separator(
			&ikey(b"foo", 100, InternalKeyKind::Set),
			&ikey(b"hello", 200, InternalKeyKind::Set),
		);
		assert_eq!(result, ikey_max_seq(b"g"));

		// "ABC1AAAAA" vs "ABC2ABB" -> "ABC2" with max seq
		let result = cmp.separator(
			&ikey(b"ABC1AAAAA", 100, InternalKeyKind::Set),
			&ikey(b"ABC2ABB", 200, InternalKeyKind::Set),
		);
		assert_eq!(result, ikey_max_seq(b"ABC2"));

		// "AAA1AAA" vs "AAA2AA" -> "AAA2" with max seq
		let result = cmp.separator(
			&ikey(b"AAA1AAA", 100, InternalKeyKind::Set),
			&ikey(b"AAA2AA", 200, InternalKeyKind::Set),
		);
		assert_eq!(result, ikey_max_seq(b"AAA2"));

		// "AAA1AAA" vs "AAA4" -> "AAA2" with max seq
		let result = cmp.separator(
			&ikey(b"AAA1AAA", 100, InternalKeyKind::Set),
			&ikey(b"AAA4", 200, InternalKeyKind::Set),
		);
		assert_eq!(result, ikey_max_seq(b"AAA2"));

		// "AAA1AAA" vs "AAA2" -> "AAA1B" with max seq (scan forward case)
		let result = cmp.separator(
			&ikey(b"AAA1AAA", 100, InternalKeyKind::Set),
			&ikey(b"AAA2", 200, InternalKeyKind::Set),
		);
		assert_eq!(result, ikey_max_seq(b"AAA1B"));

		// "AAA1AAA" vs "AAA2A" -> "AAA2" with max seq
		let result = cmp.separator(
			&ikey(b"AAA1AAA", 100, InternalKeyKind::Set),
			&ikey(b"AAA2A", 200, InternalKeyKind::Set),
		);
		assert_eq!(result, ikey_max_seq(b"AAA2"));
	}

	#[test]
	fn test_internal_key_short_separator_adjacent() {
		let cmp = InternalKeyComparator::new(Arc::new(BytewiseComparator::default()));

		// "AAA1" vs "AAA2" - adjacent, can't shorten further, return unchanged
		let result = cmp.separator(
			&ikey(b"AAA1", 100, InternalKeyKind::Set),
			&ikey(b"AAA2", 200, InternalKeyKind::Set),
		);
		assert_eq!(result, ikey(b"AAA1", 100, InternalKeyKind::Set));
	}

	#[test]
	fn test_internal_key_short_separator_prefix_cases() {
		let cmp = InternalKeyComparator::new(Arc::new(BytewiseComparator::default()));

		// When start user key is prefix of limit user key, return unchanged
		assert_eq!(
			ikey(b"foo", 100, InternalKeyKind::Set),
			cmp.separator(
				&ikey(b"foo", 100, InternalKeyKind::Set),
				&ikey(b"foobar", 200, InternalKeyKind::Set)
			)
		);

		// When limit user key is prefix of start user key, return unchanged
		assert_eq!(
			ikey(b"foobar", 100, InternalKeyKind::Set),
			cmp.separator(
				&ikey(b"foobar", 100, InternalKeyKind::Set),
				&ikey(b"foo", 200, InternalKeyKind::Set)
			)
		);
	}

	#[test]
	fn test_internal_key_shortest_successor() {
		let cmp = InternalKeyComparator::new(Arc::new(BytewiseComparator::default()));

		// Normal case: "foo" becomes "g" with max seq num
		let result = cmp.successor(&ikey(b"foo", 100, InternalKeyKind::Set));
		assert_eq!(result, ikey_max_seq(b"g"));
	}

	#[test]
	fn test_internal_key_shortest_successor_all_0xff() {
		let cmp = InternalKeyComparator::new(Arc::new(BytewiseComparator::default()));

		// All 0xFF case: returns unchanged
		let input = ikey(&[0xff, 0xff], 100, InternalKeyKind::Set);
		let result = cmp.successor(&input);
		assert_eq!(result, input);
	}

	#[test]
	fn test_internal_key_separator_adjacent_user_keys() {
		// Tests separator generation for adjacent user keys like "a" vs "b"
		// where shortening may not be possible
		let cmp = InternalKeyComparator::new(Arc::new(BytewiseComparator::default()));

		// "a" vs "b" - adjacent bytes, can't shorten
		let result = cmp.separator(
			&ikey(b"a", 100, InternalKeyKind::Set),
			&ikey(b"b", 200, InternalKeyKind::Set),
		);
		// Should return original key since can't shorten "a" to something < "b"
		assert_eq!(result, ikey(b"a", 100, InternalKeyKind::Set));

		// "aaa" vs "aab" - adjacent at last byte
		let result = cmp.separator(
			&ikey(b"aaa", 100, InternalKeyKind::Set),
			&ikey(b"aab", 200, InternalKeyKind::Set),
		);
		assert_eq!(result, ikey(b"aaa", 100, InternalKeyKind::Set));
	}

	#[test]
	fn test_internal_key_separator_with_max_seq() {
		// Verifies separator uses MAX_SEQUENCE_NUMBER when shortening
		let cmp = InternalKeyComparator::new(Arc::new(BytewiseComparator::default()));

		// "apple" vs "banana" should produce "b" with MAX seq
		let result = cmp.separator(
			&ikey(b"apple", 100, InternalKeyKind::Set),
			&ikey(b"banana", 200, InternalKeyKind::Set),
		);
		assert_eq!(result, ikey_max_seq(b"b"));

		// Verify the separator is in correct order
		let sep_ikey = InternalKey::decode(&result);
		assert_eq!(sep_ikey.seq_num(), INTERNAL_KEY_SEQ_NUM_MAX);

		// separator should be >= original and < next
		assert!(
			cmp.compare(&ikey(b"apple", 100, InternalKeyKind::Set), &result) != Ordering::Greater
		);
		assert!(
			cmp.compare(&result, &ikey(b"banana", 200, InternalKeyKind::Set)) == Ordering::Less
		);
	}

	#[test]
	fn test_internal_key_ordering_with_seq_nums() {
		// Explicitly tests that higher seq_num comes first (descending order)
		let cmp = InternalKeyComparator::new(Arc::new(BytewiseComparator::default()));

		// (foo, 100) should come BEFORE (foo, 50) in the ordering
		let key_100 = ikey(b"foo", 100, InternalKeyKind::Set);
		let key_50 = ikey(b"foo", 50, InternalKeyKind::Set);

		assert_eq!(
			cmp.compare(&key_100, &key_50),
			Ordering::Less,
			"(foo, 100) should be < (foo, 50) due to descending seq_num"
		);

		// (foo, MAX) should come BEFORE all other (foo, *) keys
		let key_max = InternalKey::new(
			b"foo".to_vec(),
			INTERNAL_KEY_SEQ_NUM_MAX,
			InternalKeyKind::Separator,
			INTERNAL_KEY_TIMESTAMP_MAX,
		)
		.encode();

		assert_eq!(
			cmp.compare(&key_max, &key_100),
			Ordering::Less,
			"(foo, MAX) should be < (foo, 100)"
		);
		assert_eq!(
			cmp.compare(&key_max, &key_50),
			Ordering::Less,
			"(foo, MAX) should be < (foo, 50)"
		);
	}
}
