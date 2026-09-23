use super::bloom::BloomFilter;
use crate::Key;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

/// An entry in the OCC commit queue representing a committed (or in-flight) transaction.
pub(crate) struct CommitEntry {
	pub(crate) seq_num: u64,
	/// Sorted write keys for exact conflict checks if bloom filter matches.
	pub(crate) keys: Arc<[Key]>,
	/// Bloom filter over write keys for fast pre-checks.
	pub(crate) bloom: BloomFilter,
	/// Indicates whether this transaction aborted.
	pub(crate) aborted: bool,
}

impl CommitEntry {
	pub(crate) fn new(seq_num: u64, mut keys: Vec<Key>) -> Self {
		keys.sort();
		keys.dedup();
		let mut bloom = BloomFilter::new();
		for k in &keys {
			bloom.insert(k);
		}
		Self {
			seq_num,
			keys: keys.into(),
			bloom,
			aborted: false,
		}
	}

	/// Checks if this commit's write set is disjoint from another write set.
	pub(crate) fn is_disjoint_writeset(
		&self,
		other_keys: &[Key],
		other_bloom: &BloomFilter,
	) -> bool {
		if self.bloom.is_empty() || other_bloom.is_empty() {
			return true;
		}

		// Fast path: check if any of our keys are in the other's bloom filter
		let mut any_possible = false;
		for k in self.keys.iter() {
			if other_bloom.may_contain(k) {
				any_possible = true;
				break;
			}
		}
		if !any_possible {
			return true;
		}

		// Exact path: binary search on sorted keys
		for k in other_keys {
			if self.keys.binary_search(k).is_ok() {
				return false;
			}
		}

		true
	}

	/// Checks if this commit's write set conflicts with a read set.
	pub(crate) fn is_disjoint_readset(&self, read_keys: &[Key], read_bloom: &BloomFilter) -> bool {
		if self.bloom.is_empty() || read_bloom.is_empty() {
			return true;
		}

		// Fast path: check if any of our write keys are in the read bloom filter
		let mut any_possible = false;
		for k in self.keys.iter() {
			if read_bloom.may_contain(k) {
				any_possible = true;
				break;
			}
		}
		if !any_possible {
			return true;
		}

		// Exact path
		for k in read_keys {
			if self.keys.binary_search(k).is_ok() {
				return false;
			}
		}

		true
	}
}

/// A lock-free OCC commit queue tracking recent commits.
pub(crate) struct CommitQueue {
	queue: SkipMap<u64, Arc<CommitEntry>>,
}

impl CommitQueue {
	pub(crate) fn new() -> Self {
		Self {
			queue: SkipMap::new(),
		}
	}

	/// Inserts a new commit entry into the queue.
	pub(crate) fn insert(&self, entry: Arc<CommitEntry>) {
		self.queue.insert(entry.seq_num, entry);
	}

	/// Checks if the given write and read sets conflict with any commits in (from_seq..to_seq].
	pub(crate) fn check_conflicts(
		&self,
		from_seq: u64,
		to_seq: u64,
		write_keys: &[Key],
		write_bloom: &BloomFilter,
		read_keys: &[Key],
		read_bloom: &BloomFilter,
	) -> bool {
		for entry in self.queue.range(from_seq + 1..=to_seq) {
			let committed = entry.value();
			if committed.aborted {
				continue;
			}

			// Check write-write conflicts
			if !write_keys.is_empty() && !committed.is_disjoint_writeset(write_keys, write_bloom) {
				return false; // Conflict detected
			}

			// Check read-write conflicts (for locked reads / serializable validation)
			if !read_keys.is_empty() && !committed.is_disjoint_readset(read_keys, read_bloom) {
				return false; // Conflict detected
			}
		}

		true // No conflicts
	}

	/// Trims the queue, removing entries older than `min_active_seq`.
	pub(crate) fn prune(&self, min_active_seq: u64) {
		for entry in self.queue.range(..min_active_seq) {
			entry.remove();
		}
	}
}
