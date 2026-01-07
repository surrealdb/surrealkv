use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

use crate::clock::LogicalClock;
use crate::error::Result;
use crate::sstable::InternalKey;
use crate::vlog::{VLog, ValueLocation, ValuePointer};
use crate::{Key, Value};

pub type BoxedIterator<'a> = Box<dyn DoubleEndedIterator<Item = Result<(InternalKey, Value)>> + 'a>;

// Holds a key-value pair and the iterator index
#[derive(Eq)]
pub(crate) struct HeapItem {
	pub(crate) key: InternalKey,
	pub(crate) value: Value,
	pub(crate) iterator_index: usize,
}

impl PartialEq for HeapItem {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == Ordering::Equal
	}
}

impl Ord for HeapItem {
	fn cmp(&self, other: &Self) -> Ordering {
		// Invert for min-heap behavior
		other.cmp_internal(self)
	}
}

impl HeapItem {
	fn cmp_internal(&self, other: &Self) -> Ordering {
		// First compare by user key
		match self.key.user_key.cmp(&other.key.user_key) {
			Ordering::Equal => {
				// Same user key, compare by sequence number in DESCENDING order
				// (higher sequence number = more recent)
				match other.key.seq_num().cmp(&self.key.seq_num()) {
					Ordering::Equal => self.iterator_index.cmp(&other.iterator_index),
					ord => ord,
				}
			}
			ord => ord, // Different user keys
		}
	}
}

impl PartialOrd for HeapItem {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

fn collect_vlog_discard_stats(discard_stats: &mut HashMap<u32, i64>, value: &Value) -> Result<()> {
	// Skip empty values (e.g., hard delete entries)
	if value.is_empty() {
		return Ok(());
	}

	// Check if this is a ValueLocation
	let location = ValueLocation::decode(value)?;
	if location.is_value_pointer() {
		let pointer = ValuePointer::decode(&location.value)?;
		let value_data_size = pointer.total_entry_size() as i64;
		*discard_stats.entry(pointer.file_id).or_insert(0) += value_data_size;
	}
	Ok(())
}

pub(crate) struct CompactionIterator<'a> {
	iterators: Vec<BoxedIterator<'a>>,
	// Heap of iterators, ordered by their current key
	heap: BinaryHeap<HeapItem>,
	is_bottom_level: bool,

	// Track the current key being processed
	current_user_key: Option<Key>,

	// Buffer for accumulating all versions of the current key
	accumulated_versions: Vec<(InternalKey, Value)>,

	// Buffer for outputting the versions of the current key
	output_versions: Vec<(InternalKey, Value)>,

	// Compaction state
	/// Collected discard statistics: file_id -> total_discarded_bytes
	pub discard_stats: HashMap<u32, i64>,

	/// Reference to VLog for populating delete-list
	vlog: Option<Arc<VLog>>,

	/// Batch of stale entries to add to delete-list: (sequence_number,
	/// value_size)
	pub(crate) delete_list_batch: Vec<(u64, u64)>,

	/// Versioning configuration
	enable_versioning: bool,
	retention_period_ns: u64,

	/// Logical clock for time-based operations
	clock: Arc<dyn LogicalClock>,

	initialized: bool,
}

impl<'a> CompactionIterator<'a> {
	pub(crate) fn new(
		iterators: Vec<BoxedIterator<'a>>,
		is_bottom_level: bool,
		vlog: Option<Arc<VLog>>,
		enable_versioning: bool,
		retention_period_ns: u64,
		clock: Arc<dyn LogicalClock>,
	) -> Self {
		let heap = BinaryHeap::with_capacity(iterators.len());

		Self {
			iterators,
			heap,
			is_bottom_level,
			current_user_key: None,
			accumulated_versions: Vec::new(),
			output_versions: Vec::new(),
			discard_stats: HashMap::new(),
			vlog,
			delete_list_batch: Vec::new(),
			enable_versioning,
			retention_period_ns,
			clock,
			initialized: false,
		}
	}

	fn initialize(&mut self) -> Result<()> {
		// Pull the first item from each iterator and add to heap
		for (idx, iter) in self.iterators.iter_mut().enumerate() {
			if let Some(item_result) = iter.next() {
				let (key, value) = item_result?;
				self.heap.push(HeapItem {
					key,
					value,
					iterator_index: idx,
				});
			}
		}
		self.initialized = true;
		Ok(())
	}

	/// Flushes the batched delete-list entries to the VLog
	pub(crate) fn flush_delete_list_batch(&mut self) -> Result<()> {
		if let Some(ref vlog) = self.vlog {
			if !self.delete_list_batch.is_empty() {
				vlog.add_batch_to_delete_list(std::mem::take(&mut self.delete_list_batch))?;
			}
		}
		Ok(())
	}

	/// Process all accumulated versions of the current key
	/// Filters out stale entries and populates output_versions with valid
	/// entries
	fn process_accumulated_versions(&mut self) {
		if self.accumulated_versions.is_empty() {
			return;
		}

		// Sort by sequence number (descending) to get the latest version first
		self.accumulated_versions.sort_by(|a, b| b.0.seq_num().cmp(&a.0.seq_num()));

		// Check if latest version is DELETE at bottom level - if so, discard entire key
		let latest_is_delete_at_bottom = self.is_bottom_level
			&& !self.accumulated_versions.is_empty()
			&& self.accumulated_versions[0].0.is_hard_delete_marker();

		// Check if any version is Replace - if so, mark all older versions as stale
		let has_set_with_delete = self.accumulated_versions.iter().any(|(key, _)| key.is_replace());

		// Process all versions for delete list and determine which to keep
		for (i, (key, value)) in self.accumulated_versions.iter().enumerate() {
			let is_hard_delete = key.is_hard_delete_marker();
			let is_replace = key.is_replace();
			let is_latest = i == 0;

			// Determine if this entry should be marked as stale in VLog
			let should_mark_stale = if latest_is_delete_at_bottom {
				// If latest version is DELETE at bottom level, mark ALL versions as stale
				true
			} else if is_latest && !is_hard_delete && !is_replace {
				// Latest version of a regular SET operation: never mark as stale (it's being
				// returned)
				false
			} else if is_latest && is_hard_delete && self.is_bottom_level {
				// Latest version of a DELETE operation at bottom level: mark as stale (not
				// returned)
				true
			} else if is_latest && is_hard_delete && !self.is_bottom_level {
				// Latest version of a DELETE operation at non-bottom level: don't mark as stale
				// (it's being returned)
				false
			} else if is_latest && is_replace {
				// Latest version of a Replace operation: don't mark as stale (it's being
				// returned)
				false
			} else if is_hard_delete {
				// For older DELETE operations (hard delete entries): always mark as stale since
				// they don't have VLog values
				true
			} else if has_set_with_delete && !is_replace {
				// If there's a Replace operation, mark all older non-Replace versions as stale
				true
			} else {
				// Older version of a SET operation: check retention period
				if self.enable_versioning && self.retention_period_ns > 0 {
					// Get current time for retention period check
					let current_time = self.clock.now();
					let key_timestamp = key.timestamp;
					let age = current_time - key_timestamp;
					let is_within_retention = age <= self.retention_period_ns;
					// Mark as stale only if NOT within retention period
					!is_within_retention
				} else {
					// If versioning is disabled, mark older versions as stale
					true
				}
			};

			// Add to delete list and collect discard stats if needed
			if should_mark_stale && self.vlog.is_some() {
				if key.is_tombstone() {
					// Hard Delete: add key size to delete list
					self.delete_list_batch.push((key.seq_num(), key.size() as u64));
				} else {
					let location = ValueLocation::decode(value).unwrap();
					if location.is_value_pointer() {
						let pointer = ValuePointer::decode(&location.value).unwrap();
						let value_size = pointer.total_entry_size();
						self.delete_list_batch.push((key.seq_num(), value_size));
					}
				}

				// Collect discard statistics
				if let Err(e) = collect_vlog_discard_stats(&mut self.discard_stats, value) {
					log::warn!("Error collecting discard stats: {e:?}");
				}
			}

			// Determine if this version should be kept for output
			let should_output = if latest_is_delete_at_bottom {
				// If latest version is DELETE at bottom level, output NOTHING for this key
				false
			} else if should_mark_stale {
				// Stale entries: don't output
				false
			} else if self.enable_versioning {
				// Versioning enabled: output all non-stale versions
				true
			} else {
				// Versioning disabled: only output the latest version
				is_latest
			};

			if should_output {
				self.output_versions.push((key.clone(), value.clone()));
			}
		}

		// Clear accumulated versions for the next key
		self.accumulated_versions.clear();
	}
}

impl Iterator for CompactionIterator<'_> {
	type Item = Result<(InternalKey, Value)>;

	fn next(&mut self) -> Option<Self::Item> {
		if !self.initialized {
			if let Err(e) = self.initialize() {
				log::error!("[COMPACTION_ITER] Error initializing: {}", e);
				return Some(Err(e));
			}
		}

		loop {
			// First, return any pending output versions
			if !self.output_versions.is_empty() {
				// Remove from front to maintain sequence number order (already sorted
				// descending)
				return Some(Ok(self.output_versions.remove(0)));
			}

			// Get the next item from the heap
			let heap_item = match self.heap.pop() {
				Some(item) => item,
				None => {
					// No more items in heap, process any remaining accumulated versions
					if !self.accumulated_versions.is_empty() {
						self.process_accumulated_versions();
						// Return first output version if any
						if !self.output_versions.is_empty() {
							return Some(Ok(self.output_versions.remove(0)));
						}
					}
					return None;
				}
			};

			// Pull the next item from the same iterator and add back to heap
			if let Some(item_result) = self.iterators[heap_item.iterator_index].next() {
				match item_result {
					Ok((key, value)) => {
						self.heap.push(HeapItem {
							key,
							value,
							iterator_index: heap_item.iterator_index,
						});
					}
					Err(e) => {
						log::error!(
							"[COMPACTION_ITER] Error from iterator {}: {}",
							heap_item.iterator_index,
							e
						);
						return Some(Err(e));
					}
				}
			}

			let user_key = heap_item.key.user_key.clone();

			// Check if this is a new user key
			let is_new_key = match &self.current_user_key {
				None => true,
				Some(current) => user_key != *current,
			};

			if is_new_key {
				// Process accumulated versions of the previous key
				if !self.accumulated_versions.is_empty() {
					self.process_accumulated_versions();

					// Start accumulating the new key
					self.current_user_key = Some(user_key);
					self.accumulated_versions.push((heap_item.key, heap_item.value));

					// Return first output version from processed key if any
					if !self.output_versions.is_empty() {
						return Some(Ok(self.output_versions.remove(0)));
					}
				} else {
					// First key - start accumulating
					self.current_user_key = Some(user_key);
					self.accumulated_versions.push((heap_item.key, heap_item.value));
				}
			} else {
				// Same user key - add to accumulated versions
				self.accumulated_versions.push((heap_item.key, heap_item.value));
			}
		}
	}
}
