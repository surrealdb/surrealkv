//! Commit validation driven outside the pipeline, for `benches/validate_bench.rs`.

use std::sync::Arc;

use tokio::sync::Semaphore;

use super::index::{key_hash, WriteIndex};
use super::queue::CommitEntry;
use crate::Key;

/// A committing transaction and the window of in-flight commits it validates
/// against, all with disjoint keys so none conflicts.
pub struct ValidateWindow {
	entry: CommitEntry,
	index: WriteIndex,
	seq: u64,
	read_set: Vec<Key>,
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
		let index = WriteIndex::new();
		for seq in 1..=window as u64 {
			let writer = Arc::new(entry_of(keys(keys_per_entry)));
			index.insert(&writer, 0);
			writer.set_seq(seq);
		}
		let read_set = keys(read_keys);
		Self {
			entry,
			index,
			seq: window as u64 + 1,
			read_set,
		}
	}

	/// Looks the committer's keys up in the index, as
	/// `CommitPipeline::validate` does, and returns how many conflict.
	pub fn validate(&self) -> usize {
		let writes = self.entry.keys().iter().zip(self.entry.hashes().iter().copied());
		let reads = self.read_set.iter().map(|k| (k, key_hash(k)));
		writes
			.chain(reads)
			.filter(|(key, hash)| self.index.conflicts(key, *hash, self.seq, 0, 0))
			.count()
	}
}
