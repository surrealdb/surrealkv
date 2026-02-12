#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use quick_cache::sync::Cache as QCache;
use quick_cache::{Equivalent, Weighter};

use crate::sstable::block::Block;
use crate::Value;

/// Kind constants for differentiating cache entry types
const KIND_DATA: u8 = 0;
const KIND_INDEX: u8 = 1;
const KIND_VLOG: u8 = 2;
const KIND_DATA_HISTORY: u8 = 3;

#[derive(Clone)]
pub(crate) enum Item {
	Data(Arc<Block>),
	Index(Arc<Block>),
	VLog(Value),
}

/// Cache key with kind-based differentiation.
/// - kind: Differentiates between data blocks, index blocks, and VLog values
/// - id: table_id for blocks, file_id for VLog
/// - offset: Block or value offset within the file
#[derive(Eq, std::hash::Hash, PartialEq)]
pub(crate) struct CacheKey {
	kind: u8,
	id: u64,
	offset: u64,
}

impl From<(u8, u64, u64)> for CacheKey {
	fn from((kind, id, offset): (u8, u64, u64)) -> Self {
		Self {
			kind,
			id,
			offset,
		}
	}
}

impl Equivalent<CacheKey> for (u8, u64, &u64) {
	fn equivalent(&self, key: &CacheKey) -> bool {
		self.0 == key.kind && self.1 == key.id && *self.2 == key.offset
	}
}

#[derive(Clone)]
struct BlockWeighter;

impl Weighter<CacheKey, Item> for BlockWeighter {
	fn weight(&self, _: &CacheKey, item: &Item) -> u64 {
		match item {
			Item::Data(block) => block.size() as u64,
			Item::Index(block) => block.size() as u64,
			Item::VLog(value) => value.len() as u64,
		}
	}
}

pub(crate) struct BlockCache {
	data: QCache<CacheKey, Item, BlockWeighter>,
	// Cache statistics (only enabled in tests)
	#[cfg(test)]
	data_hits: AtomicU64,
	#[cfg(test)]
	data_misses: AtomicU64,
	#[cfg(test)]
	index_hits: AtomicU64,
	#[cfg(test)]
	index_misses: AtomicU64,
	#[cfg(test)]
	vlog_hits: AtomicU64,
	#[cfg(test)]
	vlog_misses: AtomicU64,
	#[cfg(test)]
	data_history_hits: AtomicU64,
	#[cfg(test)]
	data_history_misses: AtomicU64,
}

impl BlockCache {
	pub(crate) fn with_capacity_bytes(bytes: u64) -> Self {
		Self {
			data: QCache::with_weighter(10_000, bytes, BlockWeighter),
			#[cfg(test)]
			data_hits: AtomicU64::new(0),
			#[cfg(test)]
			data_misses: AtomicU64::new(0),
			#[cfg(test)]
			index_hits: AtomicU64::new(0),
			#[cfg(test)]
			index_misses: AtomicU64::new(0),
			#[cfg(test)]
			vlog_hits: AtomicU64::new(0),
			#[cfg(test)]
			vlog_misses: AtomicU64::new(0),
			#[cfg(test)]
			data_history_hits: AtomicU64::new(0),
			#[cfg(test)]
			data_history_misses: AtomicU64::new(0),
		}
	}

	/// Inserts a data block into the cache.
	pub(crate) fn insert_data_block(&self, table_id: u64, offset: u64, block: Arc<Block>) {
		self.data.insert((KIND_DATA, table_id, offset).into(), Item::Data(block));
	}

	/// Inserts a history data block (with custom comparator) into the cache.
	pub(crate) fn insert_data_block_history(&self, table_id: u64, offset: u64, block: Arc<Block>) {
		self.data.insert((KIND_DATA_HISTORY, table_id, offset).into(), Item::Data(block));
	}

	/// Inserts an index block into the cache.
	pub(crate) fn insert_index_block(&self, table_id: u64, offset: u64, block: Arc<Block>) {
		self.data.insert((KIND_INDEX, table_id, offset).into(), Item::Index(block));
	}

	/// Inserts a VLog value into the cache.
	pub(crate) fn insert_vlog(&self, file_id: u32, offset: u64, value: Value) {
		self.data.insert((KIND_VLOG, file_id as u64, offset).into(), Item::VLog(value));
	}

	/// Retrieves a data block from the cache.
	pub(crate) fn get_data_block(&self, table_id: u64, offset: u64) -> Option<Arc<Block>> {
		let key = (KIND_DATA, table_id, &offset);
		let item = self.data.get(&key);

		#[cfg(test)]
		{
			if item.is_some() {
				self.data_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			} else {
				self.data_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			}
		}

		match item.as_ref()? {
			Item::Data(block) => Some(Arc::clone(block)),
			_ => None,
		}
	}

	/// Retrieves a history data block from the cache.
	pub(crate) fn get_data_block_history(&self, table_id: u64, offset: u64) -> Option<Arc<Block>> {
		let key = (KIND_DATA_HISTORY, table_id, &offset);
		let item = self.data.get(&key);

		#[cfg(test)]
		{
			if item.is_some() {
				self.data_history_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			} else {
				self.data_history_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			}
		}

		match item.as_ref()? {
			Item::Data(block) => Some(Arc::clone(block)),
			_ => None,
		}
	}

	/// Retrieves an index block from the cache.
	pub(crate) fn get_index_block(&self, table_id: u64, offset: u64) -> Option<Arc<Block>> {
		let key = (KIND_INDEX, table_id, &offset);
		let item = self.data.get(&key);

		#[cfg(test)]
		{
			if item.is_some() {
				self.index_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			} else {
				self.index_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			}
		}

		match item.as_ref()? {
			Item::Index(block) => Some(Arc::clone(block)),
			_ => None,
		}
	}

	/// Retrieves a VLog value from the cache.
	pub(crate) fn get_vlog(&self, file_id: u32, offset: u64) -> Option<Value> {
		let key = (KIND_VLOG, file_id as u64, &offset);
		let item = self.data.get(&key);

		#[cfg(test)]
		{
			if item.is_some() {
				self.vlog_hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			} else {
				self.vlog_misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			}
		}

		match item.as_ref()? {
			Item::VLog(value) => Some(value.clone()),
			_ => None,
		}
	}

	#[cfg(test)]
	/// Get cache statistics
	pub(crate) fn get_stats(&self) -> CacheStats {
		CacheStats {
			data_hits: self.data_hits.load(std::sync::atomic::Ordering::Relaxed),
			data_misses: self.data_misses.load(std::sync::atomic::Ordering::Relaxed),
			index_hits: self.index_hits.load(std::sync::atomic::Ordering::Relaxed),
			index_misses: self.index_misses.load(std::sync::atomic::Ordering::Relaxed),
			vlog_hits: self.vlog_hits.load(std::sync::atomic::Ordering::Relaxed),
			vlog_misses: self.vlog_misses.load(std::sync::atomic::Ordering::Relaxed),
			data_history_hits: self.data_history_hits.load(std::sync::atomic::Ordering::Relaxed),
			data_history_misses: self
				.data_history_misses
				.load(std::sync::atomic::Ordering::Relaxed),
		}
	}

	#[cfg(test)]
	/// Reset cache statistics
	pub(crate) fn reset_stats(&self) {
		self.data_hits.store(0, std::sync::atomic::Ordering::Relaxed);
		self.data_misses.store(0, std::sync::atomic::Ordering::Relaxed);
		self.index_hits.store(0, std::sync::atomic::Ordering::Relaxed);
		self.index_misses.store(0, std::sync::atomic::Ordering::Relaxed);
		self.vlog_hits.store(0, std::sync::atomic::Ordering::Relaxed);
		self.vlog_misses.store(0, std::sync::atomic::Ordering::Relaxed);
		self.data_history_hits.store(0, std::sync::atomic::Ordering::Relaxed);
		self.data_history_misses.store(0, std::sync::atomic::Ordering::Relaxed);
	}
}

/// Cache statistics (only available in tests)
#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct CacheStats {
	pub data_hits: u64,
	pub data_misses: u64,
	pub index_hits: u64,
	pub index_misses: u64,
	pub vlog_hits: u64,
	pub vlog_misses: u64,
	pub data_history_hits: u64,
	pub data_history_misses: u64,
}

#[cfg(test)]
impl CacheStats {
	pub fn total_hits(&self) -> u64 {
		self.data_hits + self.index_hits + self.vlog_hits + self.data_history_hits
	}

	pub fn total_misses(&self) -> u64 {
		self.data_misses + self.index_misses + self.vlog_misses + self.data_history_misses
	}

	pub fn total_accesses(&self) -> u64 {
		self.total_hits() + self.total_misses()
	}

	pub fn hit_ratio(&self) -> f64 {
		let total = self.total_accesses();
		if total == 0 {
			0.0
		} else {
			self.total_hits() as f64 / total as f64
		}
	}
}
