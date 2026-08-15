use std::sync::Arc;

use quick_cache::sync::Cache as QCache;
use quick_cache::{Equivalent, Weighter};

use crate::sstable::block::Block;

/// Kind constants for differentiating cache entry types
const KIND_DATA: u8 = 0;
const KIND_INDEX: u8 = 1;
const KIND_DATA_HISTORY: u8 = 3;

#[derive(Clone)]
pub(crate) enum Item {
	Data(Arc<Block>),
	Index(Arc<Block>),
}

/// Cache key with kind-based differentiation.
/// - kind: Differentiates between data and index blocks
/// - id: table ID
/// - offset: block offset within the file
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
		}
	}
}

/// The block cache: one object, identical in every build.
///
/// It deliberately carries no hit/miss counters. It used to carry six, behind
/// `#[cfg(test)]`, which meant the cache the tests exercised had a different
/// struct size and six extra atomic read-modify-writes per lookup than the one
/// that shipped — instrumentation in the hottest read path, serving four
/// assertions. Those assertions now check the observable behaviour instead: a
/// block already in the cache is served without touching the file. See
/// `crate::test::cache_tests`.
pub(crate) struct BlockCache {
	data: QCache<CacheKey, Item, BlockWeighter>,
}

impl BlockCache {
	pub(crate) fn with_capacity_bytes(bytes: u64) -> Self {
		Self {
			data: QCache::with_weighter(10_000, bytes, BlockWeighter),
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

	/// Retrieves a data block from the cache.
	pub(crate) fn get_data_block(&self, table_id: u64, offset: u64) -> Option<Arc<Block>> {
		match self.data.get(&(KIND_DATA, table_id, &offset))? {
			Item::Data(block) => Some(block),
			Item::Index(_) => None,
		}
	}

	/// Retrieves a history data block from the cache.
	pub(crate) fn get_data_block_history(&self, table_id: u64, offset: u64) -> Option<Arc<Block>> {
		match self.data.get(&(KIND_DATA_HISTORY, table_id, &offset))? {
			Item::Data(block) => Some(block),
			Item::Index(_) => None,
		}
	}

	/// Retrieves an index block from the cache.
	pub(crate) fn get_index_block(&self, table_id: u64, offset: u64) -> Option<Arc<Block>> {
		match self.data.get(&(KIND_INDEX, table_id, &offset))? {
			Item::Index(block) => Some(block),
			Item::Data(_) => None,
		}
	}
}
