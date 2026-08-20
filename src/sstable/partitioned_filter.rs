//! # Partitioned Bloom Filter
//!
//! Completes the two-level design the partitioned INDEX (`index_block.rs`)
//! already implements: the bloom filter is stored as one small partition per
//! index partition plus a top-level filter index, instead of one monolithic
//! blob covering the whole file.
//!
//! ## Why
//!
//! The monolithic filter builder had to buffer every key of the output file
//! in memory until `finish()` (issue #397: 46 B/key — multi-GiB for large
//! compactions). The partitioned builder buffers a 4-byte hash per key and
//! drains them into finished filter bits at every index-partition cut, so
//! builder memory is O(one partition) regardless of file size.
//!
//! ## On-disk layout (all inside the existing LSMV1 container)
//!
//! ```text
//! [data blocks...]
//! [filter partition 1] [filter partition 2] ... [filter partition N]
//! [filter top-level index]   <- separator_key -> partition BlockHandle
//! [index partitions...] [top-level index] [meta index] [footer]
//! ```
//!
//! The meta index references the filter top-level index under the key
//! `partitionedfilter.<policy>`. Files written before this change carry
//! `filter.<policy>` (a monolithic filter) instead; the reader dispatches
//! per file on which key exists, so old files keep their old read path and
//! no data migration is ever needed.
//!
//! ## Alignment invariant
//!
//! Filter partition `i` covers exactly the user keys of the data blocks
//! whose index entries live in index partition `i`, and is keyed by that
//! index partition's last separator. A lookup selects the filter partition
//! with the same `partition_point` search the data index uses, so any key
//! the data index could find has its hash in the selected filter partition
//! (every partition containing any version of a user key contains that
//! user key's hash).

use std::sync::Arc;

use crate::sstable::block::BlockHandle;
use crate::sstable::index_block::BlockHandleWithKey;
use crate::sstable::table::read_verified_raw_block;
use crate::vfs::File;
use crate::{FilterPolicy, Options};

// =============================================================================
// WRITER
// =============================================================================

/// Builds per-partition bloom filters from key hashes.
///
/// Driven by `TableWriter`:
/// - `add_key` for every entry (buffers a 4-byte hash),
/// - `absorb_block` after each data block is flushed,
/// - `cut` whenever the index writer finishes an index partition,
/// - `finish` to obtain the final `(separator, filter_bytes)` list, which `TableWriter` writes out
///   and indexes.
pub(crate) struct PartitionedFilterWriter {
	policy: Arc<dyn FilterPolicy>,

	/// Hashes of keys in the data block currently being built. Kept apart
	/// from `partition_hashes` so a partition cut (which happens when the
	/// NEXT block's index entry starts a new index partition) never steals
	/// the in-progress block's keys.
	block_hashes: Vec<u32>,

	/// Hashes of keys in the data blocks covered by the current (uncut)
	/// index partition.
	partition_hashes: Vec<u32>,

	/// Once this many keys accumulate, the writer asks the index to cut a
	/// partition at its next boundary — a partition is cut when EITHER the
	/// index bytes or the filter keys reach the configured partition size.
	keys_per_partition: usize,

	/// Finished partitions: (index partition's last separator key, bits).
	finished: Vec<(Vec<u8>, Vec<u8>)>,
}

impl PartitionedFilterWriter {
	/// `partition_bytes` is the target size of one filter partition
	/// (`Options::index_partition_size` — the same knob that sizes index
	/// partitions, so one setting governs both).
	pub(crate) fn new(policy: Arc<dyn FilterPolicy>, partition_bytes: usize) -> Self {
		let keys_per_partition = policy.filter_keys_per_partition(partition_bytes);
		Self {
			policy,
			block_hashes: Vec::new(),
			partition_hashes: Vec::new(),
			keys_per_partition,
			finished: Vec::new(),
		}
	}

	/// Whether the current partition is full enough that the index should
	/// cut a partition at its next boundary.
	pub(crate) fn wants_cut(&self) -> bool {
		self.partition_hashes.len() + self.block_hashes.len() >= self.keys_per_partition
	}

	pub(crate) fn filter_name(&self) -> &str {
		self.policy.name()
	}

	/// Buffers the hash of one user key (4 bytes, key size irrelevant).
	pub(crate) fn add_key(&mut self, user_key: &[u8]) {
		self.block_hashes.push(self.policy.key_hash(user_key));
	}

	/// Moves the just-flushed data block's hashes into the current
	/// partition. Must be called after the block's index entry was added
	/// (and after `cut`, if that entry started a new index partition).
	pub(crate) fn absorb_block(&mut self) {
		self.partition_hashes.append(&mut self.block_hashes);
	}

	/// Finishes the current filter partition, keyed by the finished index
	/// partition's last separator key.
	pub(crate) fn cut(&mut self, separator_key: &[u8]) {
		if self.partition_hashes.is_empty() {
			// An index partition always covers at least one data block with
			// at least one key, so this only happens when no keys were added
			// at all (filter effectively empty).
			return;
		}
		let bits = self.policy.create_filter_from_hashes(&self.partition_hashes);
		self.partition_hashes.clear();
		self.finished.push((separator_key.to_vec(), bits));
	}

	/// Drains any remaining hashes into a final partition keyed by
	/// `last_separator` and returns all finished partitions in write order.
	pub(crate) fn finish(mut self, last_separator: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
		self.absorb_block();
		self.cut(last_separator);
		self.finished
	}
}

// =============================================================================
// READER
// =============================================================================

/// Reads partitioned bloom filters: a resident top-level list of
/// (separator, partition handle), with partitions fetched on demand through
/// the block cache.
#[derive(Clone)]
pub(crate) struct PartitionedFilterReader {
	table_id: u64,
	opts: Arc<Options>,
	policy: Arc<dyn FilterPolicy>,
	file: Arc<dyn File>,

	/// Filter partition handles with their separator keys, ascending —
	/// mirrors `Index.blocks` for the data index.
	partitions: Vec<BlockHandleWithKey>,
}

impl PartitionedFilterReader {
	/// Parses the filter top-level index block at `location`.
	pub(crate) fn new(
		table_id: u64,
		opts: Arc<Options>,
		file: Arc<dyn File>,
		location: &BlockHandle,
		policy: Arc<dyn FilterPolicy>,
	) -> crate::Result<Self> {
		let block = crate::sstable::table::read_table_block(
			Arc::clone(&opts.internal_comparator),
			Arc::clone(&file),
			location,
		)?;

		let mut partitions = Vec::new();
		let mut iter = block.iter()?;
		iter.seek_to_first()?;
		while iter.is_valid() {
			let separator_key = iter.key_bytes().to_vec();
			let (handle, _) = BlockHandle::decode(iter.value_bytes())?;
			partitions.push(BlockHandleWithKey {
				separator_key,
				handle,
			});
			iter.advance()?;
		}

		Ok(Self {
			table_id,
			opts,
			policy,
			file,
			partitions,
		})
	}

	#[cfg(test)]
	pub(crate) fn num_partitions(&self) -> usize {
		self.partitions.len()
	}

	/// Returns whether `user_key` may be present, selecting the filter
	/// partition with the same search the data index uses for
	/// `encoded_internal_key`. Errors degrade to "maybe" (true): lookups
	/// stay correct and merely lose the bloom short-circuit.
	pub(crate) fn may_contain(&self, encoded_internal_key: &[u8], user_key: &[u8]) -> bool {
		if self.partitions.is_empty() {
			return true;
		}

		// Same partition_point search as Index::find_block_handle_by_key.
		let cmp = &self.opts.internal_comparator;
		let idx = self.partitions.partition_point(|p| {
			cmp.compare(&p.separator_key, encoded_internal_key) == std::cmp::Ordering::Less
		});
		let Some(partition) = self.partitions.get(idx) else {
			// Beyond the last separator: leave the verdict to the data
			// index (which will find nothing) rather than reasoning about
			// successor-key edge cases here.
			return true;
		};

		let offset = partition.handle.offset() as u64;
		let bits =
			if let Some(bits) = self.opts.block_cache.get_filter_partition(self.table_id, offset) {
				bits
			} else {
				match read_verified_raw_block(Arc::clone(&self.file), &partition.handle) {
					Ok(bytes) => {
						let bytes = Arc::new(bytes);
						self.opts.block_cache.insert_filter_partition(
							self.table_id,
							offset,
							Arc::clone(&bytes),
						);
						bytes
					}
					Err(e) => {
						log::warn!(
						"Failed to read filter partition for table {} at offset {offset}: {e}; \
						 proceeding without the filter",
						self.table_id
					);
						return true;
					}
				}
			};

		self.policy.may_contain(&bits, user_key)
	}
}
