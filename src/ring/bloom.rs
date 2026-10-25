const BLOOM_BITS: usize = 4096;
const BLOOM_WORDS: usize = BLOOM_BITS / 64;
const NUM_HASHES: usize = 3;

/// The bits a key sets in a `BloomFilter`. Hashing a key is the costly part of a
/// probe, so a key that is checked against many filters is hashed once into this.
#[derive(Clone, Copy)]
pub(crate) struct BloomProbe([u16; NUM_HASHES]);

impl BloomProbe {
	#[inline]
	pub fn new(key: &[u8]) -> Self {
		let h = Self::hash(key);
		let mut bits = [0u16; NUM_HASHES];
		for (i, bit) in bits.iter_mut().enumerate() {
			// Below `BLOOM_BITS`, so it fits
			*bit = (Self::nth_hash(h, i as u64) % (BLOOM_BITS as u64)) as u16;
		}
		Self(bits)
	}

	/// Compute a dual FNV-1a hash for double hashing
	#[inline]
	fn hash(key: &[u8]) -> (u64, u64) {
		let mut h1: u64 = 0xcbf2_9ce4_8422_2325;
		for &b in key {
			h1 ^= u64::from(b);
			h1 = h1.wrapping_mul(0x0100_0000_01b3);
		}
		let h2 = h1.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(31);
		(h1, h2)
	}

	/// Compute the nth hash from the dual hash pair
	#[inline]
	fn nth_hash(hashes: (u64, u64), n: u64) -> u64 {
		hashes.0.wrapping_add(n.wrapping_mul(hashes.1))
	}
}

/// A lightweight bloom filter for probabilistic set membership testing.
pub(crate) struct BloomFilter {
	/// The bit array backing the bloom filter
	bits: [u64; BLOOM_WORDS],
	/// The number of keys inserted into the filter
	count: usize,
}

impl BloomFilter {
	/// Create a new empty bloom filter
	pub const fn new() -> Self {
		Self {
			bits: [0; BLOOM_WORDS],
			count: 0,
		}
	}

	/// Insert a key into the bloom filter
	#[inline]
	pub fn insert(&mut self, key: &[u8]) {
		self.insert_probe(BloomProbe::new(key));
	}

	/// Insert a key, given its probe, into the bloom filter
	#[inline]
	pub fn insert_probe(&mut self, probe: BloomProbe) {
		for &bit in &probe.0 {
			self.bits[usize::from(bit) / 64] |= 1 << (bit % 64);
		}
		self.count += 1;
	}

	/// Check whether a key may be present in the filter
	#[cfg(test)]
	pub fn may_contain(&self, key: &[u8]) -> bool {
		self.may_contain_probe(BloomProbe::new(key))
	}

	/// Check whether a key, given its probe, may be present in the filter
	#[inline]
	pub fn may_contain_probe(&self, probe: BloomProbe) -> bool {
		self.count != 0
			&& probe.0.iter().all(|&bit| self.bits[usize::from(bit) / 64] & (1 << (bit % 64)) != 0)
	}

	/// Whether the filters share a set bit. When they share none, no key of
	/// one may be present in the other.
	#[inline]
	pub fn intersects(&self, other: &BloomFilter) -> bool {
		self.bits.iter().zip(&other.bits).fold(0, |acc, (a, b)| acc | (a & b)) != 0
	}

	/// Check whether the filter is empty
	pub const fn is_empty(&self) -> bool {
		self.count == 0
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn empty_filter_contains_nothing() {
		let bf = BloomFilter::new();
		assert!(bf.is_empty());
		assert!(!bf.may_contain(b"hello"));
	}

	#[test]
	fn inserted_keys_are_found() {
		let mut bf = BloomFilter::new();
		bf.insert(b"hello");
		assert!(!bf.is_empty());
		assert!(bf.may_contain(b"hello"));
	}

	#[test]
	fn a_probe_matches_the_key_it_was_made_from() {
		let mut bf = BloomFilter::new();
		bf.insert_probe(BloomProbe::new(b"hello"));
		assert!(bf.may_contain(b"hello"));
		assert!(bf.may_contain_probe(BloomProbe::new(b"hello")));
	}

	#[test]
	fn filters_sharing_a_key_intersect() {
		let mut a = BloomFilter::new();
		let mut b = BloomFilter::new();
		assert!(!a.intersects(&b));
		a.insert(b"hello");
		assert!(!a.intersects(&b));
		b.insert(b"hello");
		assert!(a.intersects(&b));
	}

	#[test]
	fn filters_that_do_not_intersect_contain_none_of_each_others_keys() {
		let mut a = BloomFilter::new();
		let mut b = BloomFilter::new();
		let keys: Vec<Vec<u8>> = (0u32..64).map(|i| i.to_be_bytes().to_vec()).collect();
		for k in &keys[..32] {
			a.insert(k);
		}
		for k in &keys[32..] {
			b.insert(k);
			if !a.intersects(&b) {
				assert!(keys[..32].iter().all(|k| !b.may_contain(k)));
			}
		}
	}
}
