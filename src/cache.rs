use quick_cache::Weighter;
use quick_cache::{sync::Cache as QCache, Equivalent};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use crate::sstable::block::Block;
use crate::Value;

pub type CacheID = u64;

#[derive(Clone)]
pub enum Item {
	Data(Arc<Block>),
	Index(Arc<Block>),
}

// VLog cache key: (file_id, offset)
#[derive(Eq, std::hash::Hash, PartialEq)]
pub(crate) struct VLogCacheKey {
	pub file_id: u32,
	pub offset: u64,
}

impl From<(u32, u64)> for VLogCacheKey {
	fn from(value: (u32, u64)) -> Self {
		Self {
			file_id: value.0,
			offset: value.1,
		}
	}
}

impl Equivalent<VLogCacheKey> for (u32, &u64) {
	/// Checks if a tuple `(u32, &u64)` is equivalent to a `VLogCacheKey`.
	fn equivalent(&self, key: &VLogCacheKey) -> bool {
		self.0 == key.file_id && *self.1 == key.offset
	}
}

// (Type (disk or index), SSTable ID, Block offset)
#[derive(Eq, std::hash::Hash, PartialEq)]
pub(crate) struct CacheKey {
	table_id: u64,
	offset: u64,
}

impl From<(u64, u64)> for CacheKey {
	fn from(value: (u64, u64)) -> Self {
		Self {
			table_id: value.0,
			offset: value.1,
		}
	}
}

impl Equivalent<CacheKey> for (u64, &u64) {
	/// Checks if a tuple `(u64, &u64)` is equivalent to a `CacheKey`.
	fn equivalent(&self, key: &CacheKey) -> bool {
		self.0 == key.table_id && *self.1 == key.offset
	}
}

#[derive(Clone)]
struct BlockWeighter;

impl Weighter<CacheKey, Item> for BlockWeighter {
	fn weight(&self, _: &CacheKey, block: &Item) -> u64 {
		match block {
			Item::Data(block) => block.size() as u64,
			Item::Index(block) => block.size() as u64,
		}
	}
}

#[derive(Clone)]
struct VLogValueWeighter;

impl Weighter<VLogCacheKey, Value> for VLogValueWeighter {
	fn weight(&self, _: &VLogCacheKey, value: &Value) -> u64 {
		value.len() as u64
	}
}

/// Dedicated cache for VLog values
pub(crate) struct VLogCache {
	data: QCache<VLogCacheKey, Value, VLogValueWeighter>,
}

impl VLogCache {
	pub(crate) fn with_capacity_bytes(bytes: u64) -> Self {
		Self {
			data: QCache::with_weighter(10_000, bytes, VLogValueWeighter),
		}
	}

	pub(crate) fn insert(&self, file_id: u32, offset: u64, value: Value) {
		self.data.insert((file_id, offset).into(), value);
	}

	pub(crate) fn get(&self, file_id: u32, offset: u64) -> Option<Value> {
		let key = (file_id, &offset);
		self.data.get(&key)
	}
}

pub(crate) struct BlockCache {
	data: QCache<CacheKey, Item, BlockWeighter>,
	id: AtomicU64,
}

impl BlockCache {
	pub(crate) fn with_capacity_bytes(bytes: u64) -> Self {
		Self {
			data: QCache::with_weighter(10_000, bytes, BlockWeighter),
			id: AtomicU64::new(0),
		}
	}

	pub(crate) fn insert(&self, table_id: u64, offset: u64, value: Item) {
		self.data.insert((table_id, offset).into(), value);
	}

	pub(crate) fn get_data_block(&self, table_id: u64, offset: u64) -> Option<Arc<Block>> {
		let key = (table_id, &offset);
		let item = self.data.get(&key)?;

		match item {
			Item::Data(block) => Some(block.clone()),
			_ => None,
		}
	}

	pub(crate) fn get_index_block(&self, table_id: u64, offset: u64) -> Option<Arc<Block>> {
		let key = (table_id, &offset);
		let item = self.data.get(&key)?;

		match item {
			Item::Index(block) => Some(block.clone()),
			_ => None,
		}
	}

	pub(crate) fn new_cache_id(&self) -> CacheID {
		let id = self.id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
		id + 1
	}
}
