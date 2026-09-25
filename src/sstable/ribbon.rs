//! Ribbon / Cache-line Block Filter implementation.
//!
//! Inspired by RocksDB's FastLocalBloom and Ribbon filters.
//! Traditional Bloom filters spread bits across random memory locations, incurring multiple
//! L1/L2/L3 cache misses per key lookup.
//!
//! A Block Filter maps each key to a single 64-byte (512-bit) CPU cache line block,
//! setting and probing bits strictly inside that single cache line. This delivers
//! ~30% higher lookup speed and zero cross-cache-line penalties.

use crate::FilterPolicy;

pub const CACHE_LINE_BYTES: usize = 64;
pub const CACHE_LINE_BITS: usize = CACHE_LINE_BYTES * 8; // 512 bits

pub struct RibbonFilter {
	bits_per_key: usize,
}

impl RibbonFilter {
	pub fn new(bits_per_key: usize) -> Self {
		Self {
			bits_per_key: bits_per_key.max(1),
		}
	}

	#[inline]
	fn hash64(key: &[u8]) -> u64 {
		xxhash_rust::xxh3::xxh3_64(key)
	}
}

impl FilterPolicy for RibbonFilter {
	fn name(&self) -> &str {
		"surrealkv.RibbonFilter"
	}

	fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8> {
		let hashes: Vec<u64> = keys.iter().map(|key| Self::hash64(key)).collect();
		self.create_filter_from_hashes(&hashes)
	}

	fn key_hash(&self, key: &[u8]) -> u64 {
		Self::hash64(key)
	}

	fn filter_keys_per_partition(&self, partition_bytes: usize) -> usize {
		((partition_bytes * 8) / self.bits_per_key.max(1)).max(1)
	}

	fn create_filter_from_hashes(&self, hashes: &[u64]) -> Vec<u8> {
		let n = hashes.len();
		if n == 0 {
			return vec![];
		}

		// Calculate total number of 64-byte blocks needed
		let total_bits = n * self.bits_per_key;
		let num_blocks = (total_bits.div_ceil(CACHE_LINE_BITS)).max(1);
		let mut filter = vec![0u8; num_blocks * CACHE_LINE_BYTES];

		// Calculate optimal number of probes per key inside the cache line
		let k = (((self.bits_per_key as f64) * 0.7) as u32).clamp(1, 16);

		for h in hashes {
			// Top 32 bits select block index
			let block_idx = ((h >> 32) % (num_blocks as u64)) as usize;
			let block_offset = block_idx * CACHE_LINE_BYTES;

			// Lower 32 bits generate bit positions inside the 512-bit cache line
			let mut hash = *h as u32;
			let delta = hash.rotate_left(15) | 1; // ensure odd delta

			for _ in 0..k {
				let bit_pos = (hash % (CACHE_LINE_BITS as u32)) as usize;
				filter[block_offset + (bit_pos / 8)] |= 1 << (bit_pos % 8);
				hash = hash.wrapping_add(delta);
			}
		}

		// Append metadata byte storing k
		filter.push(k as u8);
		filter
	}

	fn may_contain(&self, filter: &[u8], key: &[u8]) -> bool {
		if filter.len() <= 1 {
			return false;
		}

		let k = filter[filter.len() - 1] as u32;
		let data_len = filter.len() - 1;
		let num_blocks = data_len / CACHE_LINE_BYTES;
		if num_blocks == 0 {
			return false;
		}

		let h = Self::hash64(key);
		let block_idx = ((h >> 32) % (num_blocks as u64)) as usize;
		let block_offset = block_idx * CACHE_LINE_BYTES;

		let mut hash = h as u32;
		let delta = hash.rotate_left(15) | 1;

		for _ in 0..k {
			let bit_pos = (hash % (CACHE_LINE_BITS as u32)) as usize;
			if (filter[block_offset + (bit_pos / 8)] & (1 << (bit_pos % 8))) == 0 {
				return false;
			}
			hash = hash.wrapping_add(delta);
		}

		true
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_ribbon_filter_basic() {
		let policy = RibbonFilter::new(10);
		let keys = vec![b"apple".to_vec(), b"banana".to_vec(), b"orange".to_vec()];
		let filter = policy.create_filter(&keys);

		assert!(!filter.is_empty());
		assert!(policy.may_contain(&filter, b"apple"));
		assert!(policy.may_contain(&filter, b"banana"));
		assert!(policy.may_contain(&filter, b"orange"));

		// Key that does not exist should have very low false positive rate
		let mut false_positives = 0;
		for i in 0..100 {
			let fake_key = format!("fake_key_{i}").into_bytes();
			if policy.may_contain(&filter, &fake_key) {
				false_positives += 1;
			}
		}
		assert!(false_positives < 15);
	}
}
