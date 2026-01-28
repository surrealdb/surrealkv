use std::sync::Arc;

use integer_encoding::FixedInt;

use crate::FilterPolicy;

pub(crate) const FILTER_BASE_LOG2: u32 = 11;
const FILTER_BASE: u32 = 1 << FILTER_BASE_LOG2;
const FILTER_META_LENGTH: usize = 5; // 4bytes filter offsets length + 1bytes base log

// A writer for writing filter blocks, which are used to quickly test if a key
// is present in a set of keys.
pub(crate) struct FilterBlockWriter {
	policy: Arc<dyn FilterPolicy>, // The filter policy used to generate filters.
	keys: Vec<Vec<u8>>,            // A collection of keys to be added to the filter.
	filters: Vec<u8>,              // The generated filters.
	filter_offsets: Vec<u32>,      // Offsets for each filter in the `filters` vector.
}

impl FilterBlockWriter {
	// Constructs a new `FilterBlockWriter` with a given filter policy.
	pub(crate) fn new(policy: Arc<dyn FilterPolicy>) -> Self {
		Self {
			policy,
			keys: vec![],
			filter_offsets: vec![],
			filters: vec![],
		}
	}

	// Adds a key to the list of keys that will be included in the next filter.
	pub(crate) fn add_key(&mut self, key: &[u8]) {
		let key = Vec::from(key);
		self.keys.push(key);
	}

	// Starts a new block at a given offset. This may trigger the generation of a
	// new filter if necessary.
	pub(crate) fn start_block(&mut self, block_offset: usize) {
		let filter_index = block_offset / FILTER_BASE as usize; // Calculate the index of the filter for the current block.
		let filters_len = self.filter_offsets.len();
		assert!(filter_index >= filters_len); // Ensure the filter index is not out of bounds.
		while filter_index > self.filter_offsets.len() {
			self.generate_filter(); // Generate filters until reaching the required
			               // index.
		}
	}

	// Generates a filter for the current set of keys and appends it to the filters
	// vector.
	fn generate_filter(&mut self) {
		self.filter_offsets.push(self.filters.len() as u32); // Record the current filter's offset.
		if self.keys.is_empty() {
			return; // If there are no keys, do not generate a filter.
		};

		let filter = self.policy.create_filter(&self.keys); // Generate the filter using the policy.
		self.filters.extend(filter); // Append the generated filter to the filters vector.

		self.keys.clear(); // Clear the keys, as they are now included in a filter.
	}

	// Returns the name of the filter policy.
	pub(crate) fn filter_name(&self) -> &str {
		self.policy.name()
	}

	// Finalizes the filter block, returning the complete set of filters and their
	// offsets.
	pub(crate) fn finish(mut self) -> Vec<u8> {
		if !self.keys.is_empty() {
			self.generate_filter(); // Generate a final filter for any remaining keys.
		};

		let mut result = self.filters;
		let offsets_offset = self.filter_offsets.len();
		let mut ix = result.len();
		result.resize(ix + 4 * self.filter_offsets.len() + 5, 0); // Resize the result to fit the offsets and the base log2 value.

		// Append per-filter offsets to the result.
		for offset in self.filter_offsets {
			offset.encode_fixed(&mut result[ix..ix + 4]);
			ix += 4;
		}
		// Append the total number of offsets as a 4-byte value.
		(offsets_offset as u32).encode_fixed(&mut result[ix..ix + 4]);
		ix += 4;
		// Append the base log2 value as a single byte.
		result[ix] = FILTER_BASE_LOG2 as u8;

		result
	}
}

#[derive(Clone)]
pub(crate) struct FilterBlockReader {
	policy: Arc<dyn FilterPolicy>, // The filter policy used for checking keys against filters.
	data: Vec<u8>,                 // The entire filter block data.
	filter_offsets: Vec<u32>,      // Offsets for each filter within the `data`.
	base_lg: u32,                  // The base log2 value used to calculate block index.
}

impl FilterBlockReader {
	// Constructs a new `FilterBlockReader` from the given filter block data and
	// filter policy.
	pub(crate) fn new(data: Vec<u8>, policy: Arc<dyn FilterPolicy>) -> Self {
		let n = data.len();
		let base_lg = data[n - 1] as u32; // The last byte is the base log2 value.
		let num_offset = u32::decode_fixed(&data[n - FILTER_META_LENGTH..n - 1]).unwrap() as usize; // The offsets start 5 bytes from the end.
		let mut filter_offsets = Vec::with_capacity(num_offset);
		if num_offset * 4 + FILTER_META_LENGTH > n {
			panic!("invalid filter block data");
		}

		// Calculate the start position of the offsets in the data vector.
		let offsets_offset = n - (num_offset * 4 + FILTER_META_LENGTH);
		// Extract each filter offset from the data.
		for i in 0..num_offset {
			let start = offsets_offset + i * 4;
			let end = offsets_offset + (i + 1) * 4;
			let offset = u32::decode_fixed(&data[start..end]).unwrap();
			filter_offsets.push(offset);
		}

		Self {
			policy,
			data,
			filter_offsets,
			base_lg,
		}
	}

	// Checks if a key may be present in the filter block, given a specific block
	// offset.
	pub(crate) fn may_contain(&self, key: &[u8], block_offset: usize) -> bool {
		// Directly use the provided block offset as the block index.
		let block_index = block_offset >> self.base_lg;

		// TODO: What to do in this scenario?
		if block_index >= self.filter_offsets.len() {
			return true;
		}

		let start = self.filter_offsets[block_index] as usize; // Start of the filter in the data.
		let limit = if block_index + 1 < self.filter_offsets.len() {
			self.filter_offsets[block_index + 1] as usize // End of the filter in the
		                                         // data.
		} else {
			// Subtract the filter offsets array (4 bytes per offset) + 4 bytes for offsets
			// length + 1 byte for base log
			self.data.len() - (4 * self.filter_offsets.len() + 5)
		};

		// Extract the filter for the given block index.
		let filter = &self.data[start..limit];
		// Use the filter policy to check if the key may be present in the filter.
		self.policy.may_contain(filter, key)
	}
}

#[cfg(test)]
mod tests {
	use test_log::test;

	use super::*;
	use crate::sstable::bloom::LevelDBBloomFilter;
	use crate::{InternalKey, InternalKeyKind};

	#[test]
	fn test_empty() {
		let b = FilterBlockWriter::new(Arc::new(LevelDBBloomFilter::new(10)));
		let block = b.finish();
		assert_eq!(&[0, 0, 0, 0, FILTER_BASE_LOG2 as u8][..], &*block);
		let r = FilterBlockReader::new(block, Arc::new(LevelDBBloomFilter::new(10)));
		assert!(r.may_contain("foo".as_bytes(), 0));
		assert!(r.may_contain("foo".as_bytes(), 10000));
	}

	#[test]
	fn test_single_filter() {
		let mut w = FilterBlockWriter::new(Arc::new(LevelDBBloomFilter::new(10)));
		w.start_block(100);
		w.add_key("foo".as_bytes());
		w.add_key("bar".as_bytes());
		w.add_key("box".as_bytes());

		w.start_block(200);
		w.add_key("box".as_bytes());
		w.start_block(300);
		w.add_key("hello".as_bytes());

		let block = w.finish();

		let r = FilterBlockReader::new(block, Arc::new(LevelDBBloomFilter::new(10)));
		assert!(r.may_contain("foo".as_bytes(), 100));
		assert!(r.may_contain("bar".as_bytes(), 100));
		assert!(r.may_contain("box".as_bytes(), 100));
		assert!(r.may_contain("hello".as_bytes(), 100));
		assert!(r.may_contain("foo".as_bytes(), 100));
		assert!(!r.may_contain("missing".as_bytes(), 100));
		assert!(!r.may_contain("other".as_bytes(), 100));
	}

	#[test]
	fn test_multiple_filters() {
		// Setup
		let mut w = FilterBlockWriter::new(Arc::new(LevelDBBloomFilter::new(10)));

		// First filter
		w.start_block(0);
		w.add_key("foo".as_bytes());
		w.start_block(2000);
		w.add_key("bar".as_bytes());

		// Second filter
		w.start_block(3100);
		w.add_key("box".as_bytes());

		// Third filter is empty

		// Last filter
		w.start_block(9000);
		w.add_key("box".as_bytes());
		w.add_key("hello".as_bytes());

		let block = w.finish();
		let r = FilterBlockReader::new(block, Arc::new(LevelDBBloomFilter::new(10)));

		// Assertions for first filter
		assert!(r.may_contain("foo".as_bytes(), 0));
		assert!(r.may_contain("bar".as_bytes(), 2000));
		assert!(!r.may_contain("box".as_bytes(), 0));
		assert!(!r.may_contain("hello".as_bytes(), 0));

		// Assertions for second filter
		assert!(r.may_contain("box".as_bytes(), 3100));
		assert!(!r.may_contain("foo".as_bytes(), 3100));
		assert!(!r.may_contain("bar".as_bytes(), 3100));
		assert!(!r.may_contain("hello".as_bytes(), 3100));

		// Assertions for third (empty) filter
		assert!(!r.may_contain("box".as_bytes(), 4100));
		assert!(!r.may_contain("foo".as_bytes(), 4100));
		assert!(!r.may_contain("bar".as_bytes(), 4100));
		assert!(!r.may_contain("hello".as_bytes(), 4100));

		// Assertions for last filter
		assert!(r.may_contain("box".as_bytes(), 9000));
		assert!(!r.may_contain("foo".as_bytes(), 9000));
		assert!(!r.may_contain("bar".as_bytes(), 9000));
		assert!(r.may_contain("hello".as_bytes(), 9000));
	}

	#[test]
	fn test_filter_block_many() {
		// Create a bloom filter policy with 10 bits per key
		let bits_per_key = 10;
		let filter_policy = Arc::new(LevelDBBloomFilter::new(bits_per_key));

		// Create a FilterBlockWriter
		let mut filter_writer =
			FilterBlockWriter::new(Arc::clone(&filter_policy) as Arc<dyn FilterPolicy>);

		// Start a single block
		filter_writer.start_block(0);

		let num_items = 10001;
		// Add keys to the filter
		let mut keys = Vec::with_capacity(num_items);
		for i in 0..num_items {
			// Create internal key
			let user_key = format!("key_{i:05}");
			let internal_key = InternalKey::new(
				user_key.as_bytes().to_vec(),
				(i + 1) as u64, // sequence numbers
				InternalKeyKind::Set,
				0,
			);

			// Encode and add to filter
			let encoded_key = internal_key.encode();
			filter_writer.add_key(&encoded_key);

			// Store for verification
			keys.push(encoded_key);
		}

		// Finish the filter block
		let filter_block = filter_writer.finish();

		// Create a FilterBlockReader
		let filter_reader = FilterBlockReader::new(filter_block, filter_policy);

		// Test 1: No false negatives - all keys should be found
		for key in &keys {
			assert!(filter_reader.may_contain(key, 0), "Key should be found in the filter");
		}

		// Test 2: Check for false positives with non-existent keys
		let num_samples = 1000;
		let mut false_positives = 0;
		for i in 0..num_samples {
			// Use values outside the range of existing keys
			let user_key = format!("nonexistent_{:05}", i + num_items);
			let internal_key =
				InternalKey::new(user_key.as_bytes().to_vec(), i as u64, InternalKeyKind::Set, 0);

			let encoded_key = internal_key.encode();

			if filter_reader.may_contain(&encoded_key, 0) {
				false_positives += 1;
			}
		}

		// Log false positive rate
		let false_positive_rate = (false_positives as f64 / num_samples as f64) * 100.0;
		println!(
            "False positive rate: {false_positive_rate:.2}% ({false_positives} out of {num_samples})"
        );

		assert!(
			false_positive_rate < 2.0,
			"False positive rate too high: {false_positive_rate:.2}%"
		);
	}

	#[test]
	fn test_basic_bloom_filter() {
		let policy = Arc::new(LevelDBBloomFilter::new(10));
		let mut w = FilterBlockWriter::new(Arc::clone(&policy) as Arc<dyn FilterPolicy>);

		w.start_block(0);
		w.add_key("foo".as_bytes());
		w.add_key("bar".as_bytes());
		w.add_key("baz".as_bytes());

		let block = w.finish();
		let r = FilterBlockReader::new(block, policy);

		// Should find all added keys
		assert!(r.may_contain("foo".as_bytes(), 0));
		assert!(r.may_contain("bar".as_bytes(), 0));
		assert!(r.may_contain("baz".as_bytes(), 0));

		// Should not find keys that weren't added (with high probability)
		assert!(!r.may_contain("missing".as_bytes(), 0));
		assert!(!r.may_contain("nothere".as_bytes(), 0));
	}
}
