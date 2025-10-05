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

			let converted_iter = extended_converted_iter.map(|result| {
				result.map(|(encoded_key, encoded_value)| {
					let internal_key = InternalKey::decode(&encoded_key);
					let is_tombstone = internal_key.is_tombstone();
					let value_vec = encoded_value.to_vec();
					(internal_key, value_vec, is_tombstone)
				})
			});

			let timestamp_filtered_iter = converted_iter.filter(move |result| match result {
				Ok((internal_key, _, _)) => {
					internal_key.timestamp >= start_ts && internal_key.timestamp <= end_ts
				}
				Err(_) => true,
			});

			let versioning_iter = if include_latest_only {
				Box::new(VersioningFilterIter::new(timestamp_filtered_iter, include_latest_only))
			} else if include_tombstones {
				Box::new(ScanAllTimestampsFilterIter::new(timestamp_filtered_iter))
			} else {
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

		let key = latest_internal_key.user_key.as_ref().to_vec();

		if self.include_latest_only {
			while let Some(next_item) = self.inner.peek() {
				match next_item {
					Ok((internal_key, _, _)) => {
						if internal_key.user_key.as_ref() == &key {
							if let Ok((new_key, new_value, new_tombstone)) =
								self.inner.next().expect("should not be empty")
							{
								latest_internal_key = new_key;
								latest_encoded_value = new_value;
								latest_is_tombstone = new_tombstone;
							}
						} else {
							break;
						}
					}
					Err(_) => {
						let _ = self.inner.next().expect("should not be empty");
					}
				}
			}
		}

		if latest_internal_key.is_hard_delete_marker() {
			return self.next();
		}
		Some(Ok((latest_internal_key, latest_encoded_value, latest_is_tombstone)))
	}
}

impl<I> DoubleEndedIterator for VersioningFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	fn next_back(&mut self) -> Option<Self::Item> {
		unimplemented!("VersioningFilterIter::next_back is not implemented");
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

		while let Some(entry) = self.inner.next() {
			let (internal_key, encoded_value, is_tombstone) = entry?;
			let current_key = internal_key.user_key.as_ref().to_vec();

			if let Some(prev_key) = self.current_key.clone() {
				if prev_key != current_key {
					if self.process_key_versions(prev_key.clone())? {
						self.current_key = Some(current_key);
						self.current_versions.clear();
						self.current_versions.push((internal_key, encoded_value, is_tombstone));
						return Ok(true);
					}
					self.current_key = None;
				}
			}

			if self.current_key.is_none() || self.current_key.as_ref() == Some(&current_key) {
				self.current_key = Some(current_key);
				self.current_versions.push((internal_key, encoded_value, is_tombstone));
			}
		}

		if let Some(key) = self.current_key.take() {
			if self.process_key_versions(key)? {
				return Ok(true);
			}
		}
		self.finished = true;
		Ok(false)
	}

	fn process_key_versions(&mut self, _key: Vec<u8>) -> Result<bool> {
		let versions = std::mem::take(&mut self.current_versions);

		if versions.is_empty() {
			return Ok(false);
		}

		let latest_version = versions.last().unwrap();
		if latest_version.0.is_hard_delete_marker() {
			return Ok(false);
		}

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
		if self.current_versions.is_empty() {
			match self.load_next_key() {
				Ok(true) => {}
				Ok(false) => return None,
				Err(e) => return Some(Err(e)),
			}
		}

		if self.current_versions.is_empty() {
			return None;
		}

		if !self.current_versions.is_empty() {
			let (internal_key, encoded_value, is_tombstone) = self.current_versions.remove(0);
			Some(Ok((internal_key, encoded_value, is_tombstone)))
		} else {
			match self.load_next_key() {
				Ok(true) => self.next(),
				Ok(false) => None,
				Err(e) => Some(Err(e)),
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

	fn load_next_key(&mut self) -> Result<bool> {
		if self.finished {
			return Ok(false);
		}

		if let Some((internal_key, encoded_value, is_tombstone)) = self.next_key_buffer.take() {
			let current_key = internal_key.user_key.as_ref().to_vec();
			self.current_key = Some(current_key.clone());
			self.current_versions.push((internal_key, encoded_value, is_tombstone));
		}

		while let Some(entry) = self.inner.next() {
			let (internal_key, encoded_value, is_tombstone) = entry?;
			let current_key = internal_key.user_key.as_ref().to_vec();

			if let Some(prev_key) = self.current_key.clone() {
				if prev_key != current_key {
					self.next_key_buffer = Some((internal_key, encoded_value, is_tombstone));
					if self.process_key_versions(prev_key.clone())? {
						return Ok(true);
					}
					self.current_key = None;
					return self.load_next_key();
				}
			}

			if self.current_key.is_none() || self.current_key.as_ref() == Some(&current_key) {
				self.current_key = Some(current_key);
				self.current_versions.push((internal_key, encoded_value, is_tombstone));
			}
		}

		if let Some(key) = self.current_key.take() {
			if self.process_key_versions(key)? {
				return Ok(true);
			}
		}
		self.finished = true;
		Ok(false)
	}

	fn process_key_versions(&mut self, _key: Vec<u8>) -> Result<bool> {
		let versions = std::mem::take(&mut self.current_versions);

		if versions.is_empty() {
			return Ok(false);
		}

		let latest_version = versions.last().unwrap();
		if latest_version.0.is_hard_delete_marker() {
			return Ok(false);
		}

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
		if self.current_versions.is_empty() {
			match self.load_next_key() {
				Ok(true) => {}
				Ok(false) => return None,
				Err(e) => return Some(Err(e)),
			}
		}

		if self.current_versions.is_empty() {
			return None;
		}

		if !self.current_versions.is_empty() {
			let (internal_key, encoded_value, is_tombstone) = self.current_versions.remove(0);
			Some(Ok((internal_key, encoded_value, is_tombstone)))
		} else {
			match self.load_next_key() {
				Ok(true) => self.next(),
				Ok(false) => None,
				Err(e) => Some(Err(e)),
			}
		}
	}
}

impl<I> DoubleEndedIterator for ScanAllTimestampsFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	fn next_back(&mut self) -> Option<Self::Item> {
		unimplemented!("ScanAllTimestampsFilterIter::next_back is not implemented");
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
					if !self.include_tombstones && is_tombstone {
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

impl<I> DoubleEndedIterator for TombstoneFilterIter<I>
where
	I: DoubleEndedIterator<Item = Result<(InternalKey, Vec<u8>, bool)>>,
{
	fn next_back(&mut self) -> Option<Self::Item> {
		while let Some(item) = self.inner.next_back() {
			match item {
				Ok((internal_key, encoded_value, is_tombstone)) => {
					if !self.include_tombstones && is_tombstone {
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
