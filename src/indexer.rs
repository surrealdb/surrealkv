use bytes::Bytes;
use vart::{art::Tree as VartIndex, VariableSizeKey};

use crate::error::Result;
use crate::meta::Metadata;
use crate::store::Core;

/// The `Indexer` struct is responsible for managing the index of key-value pairs.
/// It uses a `vart` index, which is a type of persistent, lock-free B+ tree.
pub(crate) struct Indexer {
    pub(crate) index: VartIndex<VariableSizeKey, IndexValue>,
}

impl Indexer {
    /// Creates a new `Indexer` instance.
    pub(crate) fn new() -> Self {
        let index = VartIndex::new();
        Self { index }
    }

    /// Creates a snapshot of the current state of the index.
    pub(crate) fn snapshot(&self) -> VartIndex<VariableSizeKey, IndexValue> {
        self.index.clone()
    }

    pub fn insert(
        &mut self,
        key: &mut VariableSizeKey,
        value: IndexValue,
        version: u64,
        ts: u64,
        check_version: bool,
    ) -> Result<()> {
        if check_version {
            self.index.insert(key, value, version, ts)?;
        } else {
            self.index.insert_unchecked(key, value, version, ts)?;
        }
        Ok(())
    }

    pub fn insert_or_replace(
        &mut self,
        key: &mut VariableSizeKey,
        value: IndexValue,
        version: u64,
        ts: u64,
        check_version: bool,
    ) -> Result<()> {
        if check_version {
            self.index.insert_or_replace(key, value, version, ts)?;
        } else {
            self.index
                .insert_or_replace_unchecked(key, value, version, ts)?;
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &mut VariableSizeKey) {
        self.index.remove(key);
    }

    /// Returns the current version of the index.
    pub fn version(&self) -> u64 {
        self.index.version()
    }
}

#[derive(Clone)]
pub(crate) struct DiskIndexEntry {
    segment_id: u64,
    value_offset: u64,
    value_len: usize,
    metadata: Option<Metadata>,
    inlined_value: Option<Bytes>,
}

#[derive(Clone)]
pub(crate) struct MemIndexEntry {
    metadata: Option<Metadata>,
    value: Bytes,
}

#[derive(Clone)]
pub(crate) enum IndexValue {
    Disk(DiskIndexEntry),
    Mem(MemIndexEntry),
}

impl IndexValue {
    pub(crate) fn new_disk(
        segment_id: u64,
        value_offset: u64,
        metadata: Option<Metadata>,
        value: &Bytes,
        max_value_threshold: usize,
    ) -> Self {
        let inlined_value = if value.len() <= max_value_threshold {
            // Inline value case
            Some(value.clone())
        } else {
            None
        };
        let e = DiskIndexEntry {
            segment_id,
            value_offset,
            value_len: value.len(),
            metadata,
            inlined_value,
        };
        Self::Disk(e)
    }

    pub(crate) fn new_mem(metadata: Option<Metadata>, value: Bytes) -> Self {
        let e = MemIndexEntry { metadata, value };
        Self::Mem(e)
    }

    pub(crate) fn metadata(&self) -> Option<&Metadata> {
        match self {
            Self::Disk(e) => e.metadata.as_ref(),
            Self::Mem(e) => e.metadata.as_ref(),
        }
    }

    pub(crate) fn deleted(&self) -> bool {
        self.metadata()
            .is_some_and(|md| md.is_deleted_or_tombstone())
    }

    pub(crate) fn segment_id(&self) -> u64 {
        match self {
            Self::Disk(e) => e.segment_id,
            Self::Mem(_) => panic!("unavailable for memory index entries"),
        }
    }

    pub(crate) fn resolve(&self, store: &Core) -> Result<Box<[u8]>> {
        match self {
            Self::Mem(mem_entry) => Ok(Box::from(mem_entry.value.as_ref())),
            Self::Disk(disk_entry) => match &disk_entry.inlined_value {
                Some(value) => Ok(Box::from(value.as_ref())),
                None => store.resolve_from_offset(
                    disk_entry.segment_id,
                    disk_entry.value_offset,
                    disk_entry.value_len,
                ),
            },
        }
    }
}
