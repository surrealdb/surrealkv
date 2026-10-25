//! Commit validation driven outside the pipeline, for `benches/validate_bench.rs`.

use std::sync::Arc;

use tokio::sync::Semaphore;

use super::bloom::BloomFilter;
use super::queue::CommitEntry;
use crate::Key;

/// A committing transaction and the window of in-flight commits it validates
/// against, all with disjoint keys so every entry is checked and none conflicts.
pub struct ValidateWindow {
	entry: CommitEntry,
	window: Vec<CommitEntry>,
	read_set: Vec<Key>,
	read_bloom: BloomFilter,
}

impl ValidateWindow {
	/// `window` entries of `keys_per_entry` keys of `key_len` bytes each, and a
	/// committer with as many write keys and `read_keys` locked reads.
	pub fn new(window: usize, keys_per_entry: usize, key_len: usize, read_keys: usize) -> Self {
		let semaphore = Arc::new(Semaphore::new(Semaphore::MAX_PERMITS));
		let mut next = 0u64;
		let mut keys = |n: usize| -> Vec<Key> {
			(0..n)
				.map(|_| {
					next += 1;
					// A shared prefix, as table keys have, with the distinct part last.
					let mut key = vec![b'k'; key_len.saturating_sub(8)];
					key.extend_from_slice(&next.to_be_bytes());
					key
				})
				.collect()
		};
		let entry_of = |keys: Vec<Key>| {
			CommitEntry::new(keys, Arc::clone(&semaphore).try_acquire_owned().unwrap())
		};
		let entry = entry_of(keys(keys_per_entry));
		let window = (0..window).map(|_| entry_of(keys(keys_per_entry))).collect();
		let read_set = keys(read_keys);
		let mut read_bloom = BloomFilter::new();
		for k in &read_set {
			read_bloom.insert(k);
		}
		Self {
			entry,
			window,
			read_set,
			read_bloom,
		}
	}

	/// Checks the committer against every entry in the window, as
	/// `CommitPipeline::validate` does, and returns how many conflict.
	pub fn validate(&self) -> usize {
		self.window
			.iter()
			.filter(|other| other.conflicts_with(&self.entry, 0, &self.read_set, &self.read_bloom))
			.count()
	}
}
