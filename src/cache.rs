use quick_cache::Weighter;
use quick_cache::{sync::Cache as QCache, Equivalent};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use crate::sstable::block::Block;

pub type CacheID = u64;

#[derive(Clone)]
pub enum Item {
    Data(Arc<Block>),
    Index(Arc<Block>),
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

pub struct BlockCache {
    data: QCache<CacheKey, Item, BlockWeighter>,
    id: AtomicU64,
}

impl BlockCache {
    pub fn with_capacity_bytes(bytes: u64) -> Self {
        Self {
            data: QCache::with_weighter(10_000, bytes, BlockWeighter),
            id: AtomicU64::new(0),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn insert(&self, table_id: u64, offset: u64, value: Item) {
        self.data.insert((table_id, offset).into(), value);
    }

    pub fn get_data_block(&self, table_id: u64, offset: u64) -> Option<Arc<Block>> {
        let key = (table_id, &offset);
        let item = self.data.get(&key)?;

        match item {
            Item::Data(block) => Some(block.clone()),
            _ => None,
        }
    }

    pub fn get_index_block(&self, table_id: u64, offset: u64) -> Option<Arc<Block>> {
        let key = (table_id, &offset);
        let item = self.data.get(&key)?;

        match item {
            Item::Index(block) => Some(block.clone()),
            _ => None,
        }
    }

    pub fn new_cache_id(&self) -> CacheID {
        let id = self.id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        id + 1
    }
}
