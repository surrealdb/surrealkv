use crate::FilterPolicy;

// Hash returns the hash of the given data.
pub(crate) fn hash(data: &[u8], seed: u32) -> u32 {
	const M: u32 = 0xc6a4_a793;
	const R: u32 = 24;

	let mut h = seed ^ ((data.len() as u64) * M as u64) as u32;
	let mut i = 0;

	while i + 4 <= data.len() {
		let chunk = unsafe {
			let ptr = data.as_ptr().add(i) as *const u32;
			ptr.read_unaligned().to_be().to_be_bytes()
		};
		h = h.wrapping_add(u32::from_be_bytes(chunk));
		h = h.wrapping_mul(M);
		h ^= h >> 16;
		i += 4;
	}

	match data.len() - i {
		3 => {
			h = h.wrapping_add((data[i + 2] as u32) << 16);
			h = h.wrapping_add((data[i + 1] as u32) << 8);
			h = h.wrapping_add(data[i] as u32);
		}
		2 => {
			h = h.wrapping_add((data[i + 1] as u32) << 8);
			h = h.wrapping_add(data[i] as u32);
		}
		1 => {
			h = h.wrapping_add(data[i] as u32);
		}
		_ => {}
	}

	h = h.wrapping_mul(M);
	h ^= h >> R;

	h
}

pub(crate) struct LevelDBBloomFilter {
	bits_per_key: usize,
}

impl LevelDBBloomFilter {
	pub(crate) fn new(bits_per_key: usize) -> Self {
		Self {
			bits_per_key,
		}
	}

	fn bloom_hash(key: &[u8]) -> u32 {
		hash(key, 0xbc9f_1d34)
	}
}

impl FilterPolicy for LevelDBBloomFilter {
	fn name(&self) -> &str {
		"leveldb.BloomFilter"
	}

	fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8> {
		let n = keys.len();
		if n == 0 {
			return vec![];
		}

		// Calculate filter size
		let bits = n * self.bits_per_key;
		let bytes = bits.div_ceil(8);
		let bits = bytes * 8;

		let mut filter = vec![0u8; bytes + 1]; // +1 for storing k at the end

		// Calculate number of hash functions
		let k = (((self.bits_per_key as f64) * 0.7) as u32).clamp(1, 30);

		for key in keys {
			// Single hash computation per key
			let h = Self::bloom_hash(key);

			// Bit rotation for generating multiple hash values from single computation
			let delta = h.rotate_left(15);
			let mut hash = h;

			for _ in 0..k {
				let bit_pos = (hash % (bits as u32)) as usize;
				filter[bit_pos / 8] |= 1 << (bit_pos % 8);
				hash = hash.wrapping_add(delta);
			}
		}

		// Store k at the end
		filter[bytes] = k as u8;

		filter
	}

	fn may_contain(&self, filter: &[u8], key: &[u8]) -> bool {
		let bytes = filter.len();
		if bytes < 2 {
			return false;
		}

		let k = filter[bytes - 1] as u32;
		if k > 30 {
			// Reserved for potentially new encodings
			return true;
		}

		let bits = (bytes - 1) * 8;

		// Single hash computation per key lookup
		let h = Self::bloom_hash(key);

		// Use same bit rotation technique
		let delta = h.rotate_left(15);
		let mut hash = h;

		for _ in 0..k {
			let bit_pos = (hash % (bits as u32)) as usize;
			if (filter[bit_pos / 8] & (1 << (bit_pos % 8))) == 0 {
				return false;
			}
			hash = hash.wrapping_add(delta);
		}

		true
	}
}

#[cfg(test)]
mod tests {
	use test_log::test;

	use super::*;

	#[test]
	fn test_bloom_filter_creation() {
		let filter = LevelDBBloomFilter::new(10);
		assert_eq!(filter.bits_per_key, 10);

		let filter = LevelDBBloomFilter::new(100);
		assert_eq!(filter.bits_per_key, 100);

		let filter = LevelDBBloomFilter::new(1);
		assert_eq!(filter.bits_per_key, 1);
	}

	#[test]
	fn test_bloom_filter_may_contain() {
		let filter = LevelDBBloomFilter::new(10);
		let keys = vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()];
		let bloom_filter = filter.create_filter(&keys);

		assert!(filter.may_contain(&bloom_filter, b"key1"));
		assert!(filter.may_contain(&bloom_filter, b"key2"));
		assert!(filter.may_contain(&bloom_filter, b"key3"));
		assert!(!filter.may_contain(&bloom_filter, b"key4"));
	}

	#[test]
	fn test_bloom_filter_empty() {
		let filter = LevelDBBloomFilter::new(10);
		let empty_filter = vec![];
		assert!(!filter.may_contain(&empty_filter, b"key1"));
	}

	#[test]
	fn test_bloom_filter_invalid_num_hashes() {
		let filter = LevelDBBloomFilter::new(10);
		let mut bloom_filter = filter.create_filter(&[b"key1".to_vec()]);
		let bloom_filter_len = bloom_filter.len();
		bloom_filter[bloom_filter_len - 1] = 31; // invalid num_hashes
		assert!(filter.may_contain(&bloom_filter, b"key1"));
	}
}
