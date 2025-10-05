use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::lsm::Core;
use crate::sstable::{InternalKey, InternalKeyKind};
use double_ended_peekable::{DoubleEndedPeekable, DoubleEndedPeekableExt};


/// A versioning filter iterator that handles versioning logic (latest version per key)
pub(crate) struct VersioningFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	inner: DoubleEndedPeekable<I>,
	include_latest_only: bool,
}

/// A versioning filter iterator for scan operations that includes all versions but filters hard deletes
pub(crate) struct ScanVersioningFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	inner: DoubleEndedPeekable<I>,
	/// Current key being processed
	current_key: Option<Vec<u8>>,
	/// Current versions for the current key
	current_versions: Vec<(InternalKey, Vec<u8>, bool)>,
	/// Current position in current_versions for forward iteration
	forward_pos: usize,
	/// Whether we've reached the end
	finished: bool,
}

/// A versioning filter iterator for scan_all_timestamps that includes all versions but filters hard deletes
pub(crate) struct ScanAllTimestampsFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	inner: DoubleEndedPeekable<I>,
	/// Current key being processed
	current_key: Option<Vec<u8>>,
	/// Current versions for the current key
	current_versions: Vec<(InternalKey, Vec<u8>, bool)>,
	/// Current position in current_versions for forward iteration
	forward_pos: usize,
	/// Whether we've reached the end
	finished: bool,
	/// Buffer to hold the next key's first entry when we encounter it
	next_key_buffer: Option<(InternalKey, Vec<u8>, bool)>,
}

/// A tombstone filter iterator that filters tombstones based on parameters
pub(crate) struct TombstoneFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	inner: I,
	include_tombstones: bool,
}

/// A double-ended iterator for versioned range queries using pipeline architecture
pub(crate) struct VersionedRangeIterator {
	/// The complete pipeline: B+ Tree → TimestampFilterIter → HardDeleteFilterIter → VersioningFilterIter → TombstoneFilterIter
	pipeline: Box<dyn DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>> + 'static>,
	/// Whether the iterator has been initialized
	initialized: bool,
	/// Keep the guard alive for the lifetime of the iterator (following KMergeIterator pattern)
	_guard: Box<guardian::ArcRwLockReadGuardian<crate::bplustree::tree::DiskBPlusTree>>,
}

impl VersionedRangeIterator {
	/// Creates a new versioned range iterator using pipeline architecture
	pub(crate) fn new<R: RangeBounds<Vec<u8>>>(
		core: Arc<Core>,
		_snapshot_seq_num: u64,
		key_range: R,
		start_ts: u64,
		end_ts: u64,
		include_tombstones: bool,
		include_latest_only: bool,
	) -> Result<Self> {
		// Convert the range bounds to a concrete range for storage
		let start_bound = key_range.start_bound();
		let end_bound = key_range.end_bound();

		// Convert to InternalKey format for the B+ tree query (matching original logic)
		let start_key = match start_bound {
			Bound::Included(key) => {
				InternalKey::new(key.clone(), 0, InternalKeyKind::Set, start_ts).encode()
			}
			Bound::Excluded(key) => {
				// For excluded bounds, create a key that's lexicographically greater
				let mut next_key = key.clone();
				next_key.push(0); // Add null byte to make it greater
				InternalKey::new(next_key, 0, InternalKeyKind::Set, start_ts).encode()
			}
			Bound::Unbounded => {
				InternalKey::new(Vec::new(), 0, InternalKeyKind::Set, start_ts).encode()
			}
		};

		let end_key = match end_bound {
			Bound::Included(key) => {
				InternalKey::new(key.clone(), _snapshot_seq_num, InternalKeyKind::Max, end_ts)
					.encode()
			}
			Bound::Excluded(key) => {
				// For excluded bounds, create a key that's lexicographically greater
				let mut next_key = key.clone();
				next_key.push(0); // Add null byte to make it greater
				InternalKey::new(next_key, _snapshot_seq_num, InternalKeyKind::Max, end_ts).encode()
			}
			Bound::Unbounded => {
				InternalKey::new(vec![0xff], _snapshot_seq_num, InternalKeyKind::Max, end_ts)
					.encode()
			}
		};

		// Create the B+ tree range iterator using guardian to extend lifetime
		let versioned_index = core
			.versioned_index
			.as_ref()
			.ok_or_else(|| Error::InvalidArgument("Versioned queries not enabled".to_string()))?;
		let index_guard = guardian::ArcRwLockReadGuardian::take(versioned_index.clone()).unwrap();

		// Use unsafe to extend the lifetime of the iterator, following KMergeIterator pattern
		let boxed_guard = Box::new(index_guard);
		unsafe {
			let guard_ref: &'static guardian::ArcRwLockReadGuardian<
				crate::bplustree::tree::DiskBPlusTree,
			> = &*(&*boxed_guard
				as *const guardian::ArcRwLockReadGuardian<crate::bplustree::tree::DiskBPlusTree>);

			// Create a new range iterator with extended lifetime
			let extended_range_iter = guard_ref.range(&start_key, &end_key)?;
			let extended_converted_iter =
				extended_range_iter.map(|result| result.map_err(|e| Error::from(e)));

			// Convert B+ tree iterator to expected format: (Vec<u8>, Arc<[u8]>) → (InternalKey, Vec<u8>, bool)
			let converted_iter = extended_converted_iter.map(|result| {
				result.map(|(encoded_key, encoded_value)| {
					let internal_key = InternalKey::decode(&encoded_key);
					let is_tombstone = internal_key.is_tombstone();
					// Convert Arc<[u8]> to Vec<u8>
					let value_vec = encoded_value.to_vec();
					(internal_key, value_vec, is_tombstone)
				})
			});

			// Build the pipeline: B+ Tree → .filter() → VersioningFilterIter/ScanVersioningFilterIter → TombstoneFilterIter
			let timestamp_filtered_iter = converted_iter.filter(move |result| {
				match result {
					Ok((internal_key, _, _)) => {
						// Check timestamp bounds
						internal_key.timestamp >= start_ts && internal_key.timestamp <= end_ts
					}
					Err(_) => true, // Pass through errors
				}
			});

			// Use different versioning filters based on the use case
			let versioning_iter = if include_latest_only {
				// For latest-only queries, use VersioningFilterIter which handles latest version logic
				Box::new(VersioningFilterIter::new(timestamp_filtered_iter, include_latest_only))
					as Box<
						dyn DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>
							+ 'static,
					>
			} else if include_tombstones {
				// For scan_all_timestamps, use ScanAllTimestampsFilterIter which includes all versions and tombstones
				Box::new(ScanAllTimestampsFilterIter::new(timestamp_filtered_iter))
					as Box<
						dyn DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>
							+ 'static,
					>
			} else {
				// For scan_at_timestamp_with_deletes, use ScanVersioningFilterIter which filters hard deletes but includes all versions
				Box::new(ScanVersioningFilterIter::new(timestamp_filtered_iter))
					as Box<
						dyn DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>
							+ 'static,
					>
			};

			let tombstone_iter = TombstoneFilterIter::new(versioning_iter, include_tombstones);

			Ok(Self {
				pipeline: Box::new(tombstone_iter),
				initialized: false,
				_guard: boxed_guard,
			})
		}
	}

	/// Initializes the iterator (no-op for pipeline architecture)
	fn initialize(&mut self) -> Result<()> {
		if self.initialized {
			return Ok(());
		}
		self.initialized = true;
		Ok(())
	}
}

impl Iterator for VersionedRangeIterator {
	type Item = Result<(InternalKey, Vec<u8>, bool)>;

	fn next(&mut self) -> Option<Self::Item> {
		if let Err(e) = self.initialize() {
			return Some(Err(e));
		}
		self.pipeline.next()
	}
}

impl DoubleEndedIterator for VersionedRangeIterator {
	fn next_back(&mut self) -> Option<Self::Item> {
		if let Err(e) = self.initialize() {
			return Some(Err(e));
		}
		self.pipeline.next_back()
	}
}


// ===== VersioningFilterIter Implementation =====

impl<I> VersioningFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	pub fn new(inner: I, include_latest_only: bool) -> Self {
		let iter = inner.double_ended_peekable();
		Self {
			inner: iter,
			include_latest_only,
		}
	}
}

impl<I> Iterator for VersioningFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	type Item = Result<(InternalKey, Vec<u8>, bool)>;

	fn next(&mut self) -> Option<Self::Item> {
		// Get the first item
		let (mut latest_internal_key, mut latest_encoded_value, mut latest_is_tombstone) =
			match self.inner.next()? {
				Ok((key, value, tombstone)) => (key, value, tombstone),
				Err(e) => return Some(Err(e)),
			};

		let key = latest_internal_key.user_key.as_ref().to_vec(); // Clone the key to avoid borrowing issues

		println!("DEBUG: VersioningFilterIter processing key: {:?}, timestamp: {}, seq_num: {}, is_tombstone: {}, is_hard_delete: {}", 
			&key, latest_internal_key.timestamp, latest_internal_key.seq_num(), latest_is_tombstone, latest_internal_key.is_hard_delete_marker());

		// If include_latest_only is true, skip to the latest version of this key
		if self.include_latest_only {
			// Keep consuming versions until we find the latest one
			while let Some(next_item) = self.inner.peek() {
				match next_item {
					Ok((internal_key, _, _)) => {
						if internal_key.user_key.as_ref() == &key {
							// This is another version of the same key, consume it and update our latest
							if let Ok((new_key, new_value, new_tombstone)) =
								self.inner.next().expect("should not be empty")
							{
								latest_internal_key = new_key;
								latest_encoded_value = new_value;
								latest_is_tombstone = new_tombstone;
								println!("DEBUG: Updated to latest version - timestamp: {}, seq_num: {}, is_tombstone: {}, is_hard_delete: {}", 
									latest_internal_key.timestamp, latest_internal_key.seq_num(), latest_is_tombstone, latest_internal_key.is_hard_delete_marker());
							}
						} else {
							// Different key, stop
							break;
						}
					}
					Err(_) => {
						// If there's an error, consume it and continue
						let _ = self.inner.next().expect("should not be empty");
					}
				}
			}
		}

		// Check if the latest version is a hard delete - if so, skip this key entirely
		if latest_internal_key.is_hard_delete_marker() {
			println!("DEBUG: Latest version is hard delete, skipping key: {:?}", &key);
			return self.next(); // Skip this key and try the next one
		}

		println!(
			"DEBUG: VersioningFilterIter returning key: {:?}, is_tombstone: {}",
			&key, latest_is_tombstone
		);
		Some(Ok((latest_internal_key, latest_encoded_value, latest_is_tombstone)))
	}
}

impl<I> DoubleEndedIterator for VersioningFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	fn next_back(&mut self) -> Option<Self::Item> {
		// For now, just delegate to the inner iterator
		// TODO: Implement proper backward iteration
		None
	}
}

// ===== ScanVersioningFilterIter Implementation =====

impl<I> ScanVersioningFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	pub fn new(inner: I) -> Self {
		let iter = inner.double_ended_peekable();
		Self {
			inner: iter,
			current_key: None,
			current_versions: Vec::new(),
			forward_pos: 0,
			finished: false,
		}
	}

	/// Loads the next key and its versions from the inner iterator
	fn load_next_key(&mut self) -> Result<bool> {
		if self.finished {
			return Ok(false);
		}

		println!(
			"DEBUG: ScanVersioningFilterIter load_next_key called, current_key: {:?}",
			self.current_key
		);

		// Keep iterating as long as the key is the same, collect all versions
		while let Some(entry) = self.inner.next() {
			let (internal_key, encoded_value, is_tombstone) = entry?;
			let current_key = internal_key.user_key.as_ref().to_vec();

			println!("DEBUG: ScanVersioningFilterIter processing entry - key: {:?}, timestamp: {}, seq_num: {}, is_tombstone: {}, is_hard_delete: {}", 
				current_key, internal_key.timestamp, internal_key.seq_num(), is_tombstone, internal_key.is_hard_delete_marker());

			// If this is a new key and we already have a key to process, process it first
			if let Some(prev_key) = self.current_key.clone() {
				if prev_key != current_key {
					println!("DEBUG: ScanVersioningFilterIter new key encountered, processing previous key: {:?}", prev_key);
					// Process the previous key - this will check hard delete and filter
					if self.process_key_versions(prev_key.clone())? {
						// Start collecting versions for the new key
						self.current_key = Some(current_key);
						self.current_versions.clear(); // Clear any previous versions
						self.current_versions.push((internal_key, encoded_value, is_tombstone));
						return Ok(true);
					}
					// Clear current_key so we don't try to process it again
					self.current_key = None;
				}
			}

			// If this is the first key or same key, add to current versions
			if self.current_key.is_none() || self.current_key.as_ref() == Some(&current_key) {
				self.current_key = Some(current_key);
				self.current_versions.push((internal_key, encoded_value, is_tombstone));
				println!("DEBUG: ScanVersioningFilterIter added version to current_versions, total versions: {}", self.current_versions.len());
			}
		}

		// Process the last key if we have one
		if let Some(key) = self.current_key.take() {
			println!("DEBUG: ScanVersioningFilterIter processing final key: {:?}", key);
			if self.process_key_versions(key)? {
				return Ok(true);
			}
		}

		println!("DEBUG: ScanVersioningFilterIter no more keys to process");
		self.finished = true;
		Ok(false)
	}

	/// Processes versions for a key and prepares them for iteration
	fn process_key_versions(&mut self, key: Vec<u8>) -> Result<bool> {
		println!("DEBUG: ScanVersioningFilterIter process_key_versions called for key: {:?}", key);

		// Use the current_versions we've been collecting
		let versions = std::mem::take(&mut self.current_versions);

		println!(
			"DEBUG: ScanVersioningFilterIter processing {} versions for key: {:?}",
			versions.len(),
			key
		);
		for (i, (internal_key, _, _)) in versions.iter().enumerate() {
			println!(
				"DEBUG:   Version {}: timestamp={}, seq_num={}, is_tombstone={}, is_hard_delete={}",
				i,
				internal_key.timestamp,
				internal_key.seq_num(),
				internal_key.is_tombstone(),
				internal_key.is_hard_delete_marker()
			);
		}

		// If no versions, skip this key
		if versions.is_empty() {
			println!("DEBUG: ScanVersioningFilterIter no versions for key, skipping");
			return Ok(false);
		}

		// Always filter out keys where the latest version is a hard delete
		let latest_version = versions.last().unwrap();
		if latest_version.0.is_hard_delete_marker() {
			println!(
				"DEBUG: ScanVersioningFilterIter latest version is hard delete, skipping key: {:?}",
				key
			);
			return Ok(false);
		}

		// For ScanVersioningFilterIter, we want all versions of the key
		println!(
			"DEBUG: ScanVersioningFilterIter keeping all {} versions for key: {:?}",
			versions.len(),
			key
		);
		self.current_versions = versions;
		self.forward_pos = 0;
		Ok(true)
	}
}

impl<I> Iterator for ScanVersioningFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	type Item = Result<(InternalKey, Vec<u8>, bool)>;

	fn next(&mut self) -> Option<Self::Item> {
		println!("DEBUG: ScanVersioningFilterIter::next called, current_versions.len(): {}, forward_pos: {}", 
			self.current_versions.len(), self.forward_pos);

		// If we have no current versions, try to load the next key
		if self.current_versions.is_empty() {
			println!("DEBUG: ScanVersioningFilterIter no current versions, loading next key");
			match self.load_next_key() {
				Ok(true) => {
					println!(
						"DEBUG: ScanVersioningFilterIter loaded new key with versions, continuing"
					);
					// We loaded a new key with versions, continue to process it
				}
				Ok(false) => {
					println!("DEBUG: ScanVersioningFilterIter no more keys to process");
					// No more keys to process
					return None;
				}
				Err(e) => {
					println!("DEBUG: ScanVersioningFilterIter error loading next key: {:?}", e);
					return Some(Err(e));
				}
			}
		}

		// If we still have no versions after loading, we're done
		if self.current_versions.is_empty() {
			println!("DEBUG: ScanVersioningFilterIter still no versions after loading, done");
			return None;
		}

		// Return the next version from current_versions and remove it
		if !self.current_versions.is_empty() {
			let (internal_key, encoded_value, is_tombstone) = self.current_versions.remove(0);
			println!("DEBUG: ScanVersioningFilterIter returning version for key: {:?}, is_tombstone: {}, remaining versions: {}", 
				internal_key.user_key.as_ref(), is_tombstone, self.current_versions.len());
			Some(Ok((internal_key, encoded_value, is_tombstone)))
		} else {
			println!("DEBUG: ScanVersioningFilterIter no more versions in current_versions, loading next key");
			// We've exhausted current versions, try to load the next key
			match self.load_next_key() {
				Ok(true) => {
					println!("DEBUG: ScanVersioningFilterIter loaded next key, recursing");
					self.next()
				}
				Ok(false) => {
					println!("DEBUG: ScanVersioningFilterIter no more keys after clearing");
					None
				}
				Err(e) => {
					println!("DEBUG: ScanVersioningFilterIter error loading next key after clearing: {:?}", e);
					Some(Err(e))
				}
			}
		}
	}
}

impl<I> DoubleEndedIterator for ScanVersioningFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	fn next_back(&mut self) -> Option<Self::Item> {
		while let Some(item) = self.inner.next_back() {
			match item {
				Ok((internal_key, encoded_value, is_tombstone)) => {
					// Filter out hard deletes - if this is a hard delete, skip it
					if internal_key.is_hard_delete_marker() {
						continue;
					}
					return Some(Ok((internal_key, encoded_value, is_tombstone)));
				}
				Err(e) => return Some(Err(e)),
			}
		}
		None
	}
}

// ===== ScanAllTimestampsFilterIter Implementation =====

impl<I> ScanAllTimestampsFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	pub fn new(inner: I) -> Self {
		let iter = inner.double_ended_peekable();
		Self {
			inner: iter,
			current_key: None,
			current_versions: Vec::new(),
			forward_pos: 0,
			finished: false,
			next_key_buffer: None,
		}
	}

	/// Loads the next key and its versions from the inner iterator
	fn load_next_key(&mut self) -> Result<bool> {
		if self.finished {
			return Ok(false);
		}

		println!(
			"DEBUG: ScanAllTimestampsFilterIter load_next_key called, current_key: {:?}",
			self.current_key
		);

		// If we have a buffered entry from the previous call, use it
		if let Some((internal_key, encoded_value, is_tombstone)) = self.next_key_buffer.take() {
			let current_key = internal_key.user_key.as_ref().to_vec();
			self.current_key = Some(current_key.clone());
			self.current_versions.push((internal_key, encoded_value, is_tombstone));
			println!(
				"DEBUG: ScanAllTimestampsFilterIter using buffered entry for key: {:?}",
				current_key
			);
		}

		// Keep iterating as long as the key is the same, collect all versions
		while let Some(entry) = self.inner.next() {
			let (internal_key, encoded_value, is_tombstone) = entry?;
			let current_key = internal_key.user_key.as_ref().to_vec();

			println!("DEBUG: ScanAllTimestampsFilterIter processing entry - key: {:?}, timestamp: {}, seq_num: {}, is_tombstone: {}, is_hard_delete: {}", 
				current_key, internal_key.timestamp, internal_key.seq_num(), is_tombstone, internal_key.is_hard_delete_marker());

			// If this is a new key and we already have a key to process, process it first
			if let Some(prev_key) = self.current_key.clone() {
				if prev_key != current_key {
					println!("DEBUG: ScanAllTimestampsFilterIter new key encountered, processing previous key: {:?}", prev_key);
					// Buffer the new key's first entry for the next call
					self.next_key_buffer = Some((internal_key, encoded_value, is_tombstone));
					// Process the previous key - this will check hard delete and filter
					if self.process_key_versions(prev_key.clone())? {
						return Ok(true);
					}
					// If the previous key was filtered out, clear current_key and continue with the buffered entry
					self.current_key = None;
					// Recursively call load_next_key to process the buffered entry
					return self.load_next_key();
				}
			}

			// If this is the first key or same key, add to current versions
			if self.current_key.is_none() || self.current_key.as_ref() == Some(&current_key) {
				self.current_key = Some(current_key);
				self.current_versions.push((internal_key, encoded_value, is_tombstone));
				println!("DEBUG: ScanAllTimestampsFilterIter added version to current_versions, total versions: {}", self.current_versions.len());
			}
		}

		// Process the last key if we have one
		if let Some(key) = self.current_key.take() {
			println!("DEBUG: ScanAllTimestampsFilterIter processing final key: {:?}", key);
			if self.process_key_versions(key)? {
				return Ok(true);
			}
		}

		println!("DEBUG: ScanAllTimestampsFilterIter no more keys to process");
		self.finished = true;
		Ok(false)
	}

	/// Processes versions for a key and prepares them for iteration
	fn process_key_versions(&mut self, key: Vec<u8>) -> Result<bool> {
		println!(
			"DEBUG: ScanAllTimestampsFilterIter process_key_versions called for key: {:?}",
			key
		);

		// Use the current_versions we've been collecting
		let versions = std::mem::take(&mut self.current_versions);

		println!(
			"DEBUG: ScanAllTimestampsFilterIter processing {} versions for key: {:?}",
			versions.len(),
			key
		);
		for (i, (internal_key, _, _)) in versions.iter().enumerate() {
			println!(
				"DEBUG:   Version {}: timestamp={}, seq_num={}, is_tombstone={}, is_hard_delete={}",
				i,
				internal_key.timestamp,
				internal_key.seq_num(),
				internal_key.is_tombstone(),
				internal_key.is_hard_delete_marker()
			);
		}

		// If no versions, skip this key
		if versions.is_empty() {
			println!("DEBUG: ScanAllTimestampsFilterIter no versions for key, skipping");
			return Ok(false);
		}

		// Always filter out keys where the latest version is a hard delete
		let latest_version = versions.last().unwrap();
		if latest_version.0.is_hard_delete_marker() {
			println!("DEBUG: ScanAllTimestampsFilterIter latest version is hard delete, skipping key: {:?}", key);
			return Ok(false);
		}

		// For ScanAllTimestampsFilterIter, we want all versions of the key
		println!(
			"DEBUG: ScanAllTimestampsFilterIter keeping all {} versions for key: {:?}",
			versions.len(),
			key
		);
		self.current_versions = versions;
		self.forward_pos = 0;
		Ok(true)
	}
}

impl<I> Iterator for ScanAllTimestampsFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	type Item = Result<(InternalKey, Vec<u8>, bool)>;

	fn next(&mut self) -> Option<Self::Item> {
		println!("DEBUG: ScanAllTimestampsFilterIter::next called, current_versions.len(): {}, forward_pos: {}", 
			self.current_versions.len(), self.forward_pos);

		// If we have no current versions, try to load the next key
		if self.current_versions.is_empty() {
			println!("DEBUG: ScanAllTimestampsFilterIter no current versions, loading next key");
			match self.load_next_key() {
				Ok(true) => {
					println!("DEBUG: ScanAllTimestampsFilterIter loaded new key with versions, continuing");
					// We loaded a new key with versions, continue to process it
				}
				Ok(false) => {
					println!("DEBUG: ScanAllTimestampsFilterIter no more keys to process");
					// No more keys to process
					return None;
				}
				Err(e) => {
					println!("DEBUG: ScanAllTimestampsFilterIter error loading next key: {:?}", e);
					return Some(Err(e));
				}
			}
		}

		// If we still have no versions after loading, we're done
		if self.current_versions.is_empty() {
			println!("DEBUG: ScanAllTimestampsFilterIter still no versions after loading, done");
			return None;
		}

		// Return the next version from current_versions and remove it
		if !self.current_versions.is_empty() {
			let (internal_key, encoded_value, is_tombstone) = self.current_versions.remove(0);
			println!("DEBUG: ScanAllTimestampsFilterIter returning version for key: {:?}, is_tombstone: {}, remaining versions: {}", 
				internal_key.user_key.as_ref(), is_tombstone, self.current_versions.len());
			Some(Ok((internal_key, encoded_value, is_tombstone)))
		} else {
			println!("DEBUG: ScanAllTimestampsFilterIter no more versions in current_versions, loading next key");
			// We've exhausted current versions, try to load the next key
			match self.load_next_key() {
				Ok(true) => {
					println!("DEBUG: ScanAllTimestampsFilterIter loaded next key, recursing");
					self.next()
				}
				Ok(false) => {
					println!("DEBUG: ScanAllTimestampsFilterIter no more keys after clearing");
					None
				}
				Err(e) => {
					println!("DEBUG: ScanAllTimestampsFilterIter error loading next key after clearing: {:?}", e);
					Some(Err(e))
				}
			}
		}
	}
}

impl<I> DoubleEndedIterator for ScanAllTimestampsFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	fn next_back(&mut self) -> Option<Self::Item> {
		// For now, just delegate to the inner iterator
		// TODO: Implement proper backward iteration
		None
	}
}

// ===== TombstoneFilterIter Implementation =====

impl<I> TombstoneFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	pub fn new(inner: I, include_tombstones: bool) -> Self {
		Self {
			inner,
			include_tombstones,
		}
	}
}

impl<I> Iterator for TombstoneFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	type Item = Result<(InternalKey, Vec<u8>, bool)>;

	fn next(&mut self) -> Option<Self::Item> {
		while let Some(item) = self.inner.next() {
			match item {
				Ok((internal_key, encoded_value, is_tombstone)) => {
					println!("DEBUG: TombstoneFilterIter received - key: {:?}, is_tombstone: {}, include_tombstones: {}", 
						internal_key.user_key.as_ref(), is_tombstone, self.include_tombstones);

					// Filter tombstones if needed
					if !self.include_tombstones && is_tombstone {
						println!(
							"DEBUG: Filtering out tombstone for key: {:?}",
							internal_key.user_key.as_ref()
						);
						continue;
					}

					println!(
						"DEBUG: TombstoneFilterIter returning - key: {:?}, is_tombstone: {}",
						internal_key.user_key.as_ref(),
						is_tombstone
					);

					// If include_latest_only is true, we need to ensure we only return the latest version
					// This is handled by the VersionedFilterIter, so we just pass through
					return Some(Ok((internal_key, encoded_value, is_tombstone)));
				}
				Err(e) => return Some(Err(e)),
			}
		}
		None
	}
}

impl<I> DoubleEndedIterator for TombstoneFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	fn next_back(&mut self) -> Option<Self::Item> {
		while let Some(item) = self.inner.next_back() {
			match item {
				Ok((internal_key, encoded_value, is_tombstone)) => {
					// Filter tombstones if needed
					if !self.include_tombstones && is_tombstone {
						continue;
					}

					// If include_latest_only is true, we need to ensure we only return the latest version
					// This is handled by the VersionedFilterIter, so we just pass through
					return Some(Ok((internal_key, encoded_value, is_tombstone)));
				}
				Err(e) => return Some(Err(e)),
			}
		}
		None
	}
}
