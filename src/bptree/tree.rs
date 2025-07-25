use std::cmp::Ordering;
use std::io;
use std::path::Path;

use quick_cache::{sync::Cache, Weighter};

use super::{DiskStorage, Storage};

// These are type aliases for convenience
pub type DiskBPlusTree = BPlusTree<DiskStorage>;

#[derive(Clone)]
struct NodeWeighter;

impl Weighter<u64, NodeType> for NodeWeighter {
    fn weight(&self, _key: &u64, value: &NodeType) -> u64 {
        match value {
            NodeType::Internal(internal) => internal.current_size() as u64,
            NodeType::Leaf(leaf) => leaf.current_size() as u64,
        }
    }
}

pub type CompareFunction = dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync;

#[derive(Debug)]
pub enum BPlusTreeError {
    Io(io::Error),
    Serialization(String),
    Deserialization(String),
    InvalidOffset,
    InvalidNodeType,
    KeyValueTooLarge,
    Corruption(String),
}

impl std::fmt::Display for BPlusTreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BPlusTreeError::Io(e) => write!(f, "IO error: {e}"),
            BPlusTreeError::Serialization(msg) => write!(f, "Serialization error: {msg}"),
            BPlusTreeError::Deserialization(msg) => write!(f, "Deserialization error: {msg}"),
            BPlusTreeError::InvalidOffset => write!(f, "Invalid offset"),
            BPlusTreeError::InvalidNodeType => write!(f, "Invalid node type"),
            BPlusTreeError::KeyValueTooLarge => write!(f, "Key-value pair too large"),
            BPlusTreeError::Corruption(msg) => write!(f, "Corruption detected: {msg}"),
        }
    }
}

impl std::error::Error for BPlusTreeError {}

impl From<io::Error> for BPlusTreeError {
    fn from(e: io::Error) -> Self {
        BPlusTreeError::Io(e)
    }
}

pub(crate) type Result<T> = std::result::Result<T, BPlusTreeError>;

// Constants for Bplus-tree configuration
const PAGE_SIZE: usize = 4096;
const VERSION: u32 = 1;
const MAGIC: [u8; 8] = *b"BPTREE01";
const CACHE_CAPACITY: u64 = 256 * 1024 * 1024; // 256 MiB
const NODE_TYPE_INTERNAL: u8 = 0;
const NODE_TYPE_LEAF: u8 = 1;

// Constants for trunk page system
const TRUNK_PAGE_TYPE: u8 = 2; // Node type for trunk pages
const TRUNK_PAGE_HEADER_SIZE: usize = 13; // 1 byte type + 8 bytes next + 4 bytes count
const TRUNK_PAGE_ENTRY_SIZE: usize = 4; // 4-byte entries
const TRUNK_RESERVED_ENTRIES: usize = 6; // Reserve last 6 entries
const TRUNK_PAGE_MAX_ENTRIES: usize =
    (PAGE_SIZE - 4 - TRUNK_PAGE_HEADER_SIZE) / TRUNK_PAGE_ENTRY_SIZE - TRUNK_RESERVED_ENTRIES;

// Constants for size calculation
// const NODE_TYPE_SIZE: usize = 1; // 1 byte for node type
const LEAF_HEADER_SIZE: usize = 1 + 4 + 8 + 8; // type(1) + key_count(4) + next_leaf(8) + prev_leaf(8) = 21 bytes
const INTERNAL_HEADER_SIZE: usize = 1 + 4 + 4; // type(1) + key_count(4) + child_count(4) = 9 bytes
const KEY_SIZE_PREFIX: usize = 4; // 4 bytes for key length
const VALUE_SIZE_PREFIX: usize = 4; // 4 bytes for value length
const CHILD_PTR_SIZE: usize = 8; // 8 bytes per child pointer

#[derive(Debug)]
struct Header {
    root_offset: u64,
    trunk_page_head: u64, // First trunk page
    total_pages: u64,
    first_leaf_offset: u64, // First leaf node for range scans
    free_page_count: u32,   // Total number of pages in the freelist
    magic: [u8; 8],         // Magic number to identify the file format
    version: u32,           // Version number for format compatibility
}

impl Header {
    fn serialize(&self) -> [u8; 48] {
        let mut buffer = [0u8; 48];
        buffer[0..8].copy_from_slice(&self.magic);
        buffer[8..12].copy_from_slice(&self.version.to_le_bytes());
        buffer[12..20].copy_from_slice(&self.root_offset.to_le_bytes());
        buffer[20..28].copy_from_slice(&self.trunk_page_head.to_le_bytes());
        buffer[28..36].copy_from_slice(&self.total_pages.to_le_bytes());
        buffer[36..44].copy_from_slice(&self.first_leaf_offset.to_le_bytes());
        buffer[44..48].copy_from_slice(&self.free_page_count.to_le_bytes());
        buffer
    }

    fn deserialize(buffer: &[u8]) -> Result<Self> {
        if buffer.len() != 48 {
            return Err(BPlusTreeError::Deserialization(format!(
                "Invalid header size: {} bytes (expected 48 bytes)",
                buffer.len()
            )));
        }

        let magic = buffer[0..8]
            .try_into()
            .map_err(|_| BPlusTreeError::Deserialization("Invalid magic number format".into()))?;

        if magic != MAGIC {
            return Err(BPlusTreeError::Deserialization(
                "Invalid magic number".into(),
            ));
        }

        let version = u32::from_le_bytes(buffer[8..12].try_into().unwrap());
        if version != VERSION {
            return Err(BPlusTreeError::Deserialization(format!(
                "Unsupported version: {version}"
            )));
        }

        Ok(Header {
            magic,
            version,
            root_offset: u64::from_le_bytes(buffer[12..20].try_into().unwrap()),
            trunk_page_head: u64::from_le_bytes(buffer[20..28].try_into().unwrap()),
            total_pages: u64::from_le_bytes(buffer[28..36].try_into().unwrap()),
            first_leaf_offset: u64::from_le_bytes(buffer[36..44].try_into().unwrap()),
            free_page_count: u32::from_le_bytes(buffer[44..48].try_into().unwrap()),
        })
    }
}

// Base trait for nodes
trait Node {
    fn serialize(&self) -> Result<Vec<u8>>;
    fn current_size(&self) -> usize;
    fn would_fit(&self, key: &[u8], value: Option<&[u8]>) -> bool;
    fn max_size() -> usize {
        PAGE_SIZE - 4 // Reserve 4 bytes for length prefix
    }
}

#[derive(Debug, Clone)]
struct InternalNode {
    keys: Vec<Vec<u8>>,
    children: Vec<u64>,
    offset: u64,
}

impl InternalNode {
    fn new(offset: u64) -> Self {
        InternalNode {
            keys: Vec::new(),
            children: Vec::new(),
            offset,
        }
    }

    fn deserialize(buffer: &[u8], offset: u64) -> Result<Self> {
        if buffer.len() != PAGE_SIZE - 4 {
            return Err(BPlusTreeError::Deserialization(format!(
                "Invalid node size {} (expected {})",
                buffer.len(),
                PAGE_SIZE - 4
            )));
        }

        let mut buffer_slice = &buffer[1..]; // Skip node type

        let num_keys = u32::from_le_bytes(buffer_slice[..4].try_into().unwrap()) as usize;
        buffer_slice = &buffer_slice[4..];

        let mut keys = Vec::with_capacity(num_keys);

        for _ in 0..num_keys {
            // Read key
            let key_len = u32::from_le_bytes(buffer_slice[..4].try_into().unwrap()) as usize;
            buffer_slice = &buffer_slice[4..];
            if key_len > buffer_slice.len() {
                return Err(BPlusTreeError::Deserialization(format!(
                    "Key length {} exceeds available buffer size {}",
                    key_len,
                    buffer_slice.len()
                )));
            }
            keys.push(buffer_slice[..key_len].to_vec());
            buffer_slice = &buffer_slice[key_len..];
        }

        let num_children = u32::from_le_bytes(buffer_slice[..4].try_into().unwrap()) as usize;
        buffer_slice = &buffer_slice[4..];

        // Validation for child count
        if num_children != num_keys + 1 {
            return Err(BPlusTreeError::Deserialization(format!(
                "Invalid child count: got {}, expected {}",
                num_children,
                num_keys + 1
            )));
        }

        let mut children = Vec::with_capacity(num_children);
        for _ in 0..num_children {
            if buffer_slice.len() < CHILD_PTR_SIZE {
                return Err(BPlusTreeError::Deserialization(
                    "Buffer too small for child pointer".into(),
                ));
            }
            children.push(u64::from_le_bytes(buffer_slice[..8].try_into().unwrap()));
            buffer_slice = &buffer_slice[8..];
        }

        Ok(InternalNode {
            keys,
            children,
            offset,
        })
    }

    // Find the appropriate child index for a key
    fn find_child_index(&self, key: &[u8], compare: &CompareFunction) -> usize {
        // Binary search implementation
        let mut low = 0;
        let mut high = self.keys.len();

        while low < high {
            let mid = low + (high - low) / 2;
            match compare(key, &self.keys[mid]) {
                Ordering::Less => high = mid,
                Ordering::Equal => {
                    // When equal, we want the next child
                    return mid + 1;
                }
                Ordering::Greater => low = mid + 1,
            }
        }

        low
    }

    // Insert a key and child pointer at the correct position
    fn insert_key_child(&mut self, key: &[u8], child_offset: u64, compare: &CompareFunction) {
        let mut idx = 0;
        while idx < self.keys.len() && compare(key, &self.keys[idx]) == Ordering::Greater {
            idx += 1;
        }

        self.keys.insert(idx, key.to_vec());
        self.children.insert(idx + 1, child_offset);
    }

    // Check if this node can fit another entry
    fn can_fit_entry(&self, key: &[u8]) -> bool {
        self.would_fit(key, None)
    }

    fn find_split_point(&self, new_key: &[u8], compare: &CompareFunction) -> usize {
        // Find where the new key would go
        let mut insert_idx = 0;
        while insert_idx < self.keys.len()
            && (compare)(new_key, &self.keys[insert_idx]) == Ordering::Greater
        {
            insert_idx += 1;
        }

        // Create a temporary node to simulate the insertion
        let mut temp_keys = self.keys.clone();
        temp_keys.insert(insert_idx, new_key.to_vec());

        // Target size for left node after split (approximately half)
        let target_size =
            (self.current_size() + KEY_SIZE_PREFIX + new_key.len() + CHILD_PTR_SIZE) / 2;

        // Find split point that results in approximately balanced sizes
        let mut current_size = INTERNAL_HEADER_SIZE;
        let mut split_idx = 0;

        for (i, key) in temp_keys.iter().enumerate() {
            let entry_size = KEY_SIZE_PREFIX + key.len() + CHILD_PTR_SIZE;

            if current_size + entry_size > target_size && i > 0 {
                split_idx = i;
                break;
            }

            current_size += entry_size;
        }

        // Ensure we don't split at the end
        if split_idx == 0 || split_idx >= temp_keys.len() - 1 {
            split_idx = temp_keys.len() / 2;
        }

        split_idx
    }
}

impl Node for InternalNode {
    fn serialize(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::with_capacity(PAGE_SIZE - 4);

        // 1. Node type (1 byte)
        buffer.push(NODE_TYPE_INTERNAL);

        // 2. Number of keys (4 bytes)
        buffer.extend_from_slice(&(self.keys.len() as u32).to_le_bytes());

        // 3. Serialize keys with length prefixes
        for key in &self.keys {
            // Key serialization (4 + key_len bytes)
            buffer.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buffer.extend_from_slice(key);
        }

        // 4. Number of children (4 bytes)
        buffer.extend_from_slice(&(self.children.len() as u32).to_le_bytes());

        // 5. Child pointers (8 bytes each)
        for &child in &self.children {
            buffer.extend_from_slice(&child.to_le_bytes());
        }

        // 6. Calculate total space used
        let total_used = buffer.len();
        let available_space = PAGE_SIZE - 4; // Reserve 4 bytes for length prefix

        // 7. Validate size before padding
        if total_used > available_space {
            return Err(BPlusTreeError::Serialization(format!(
                "Internal node requires {total_used} bytes (max {available_space})"
            )));
        }

        // 8. Pad to fill page
        buffer.resize(available_space, 0);
        Ok(buffer)
    }

    fn current_size(&self) -> usize {
        // Base size for internal node header
        let mut size = INTERNAL_HEADER_SIZE;

        // Size for all keys with their length prefixes
        for key in &self.keys {
            size += KEY_SIZE_PREFIX + key.len();
        }

        // Size for all child pointers
        size += self.children.len() * CHILD_PTR_SIZE;

        size
    }

    fn would_fit(&self, key: &[u8], _value: Option<&[u8]>) -> bool {
        // Calculate size with additional key and child pointer
        let additional_size = KEY_SIZE_PREFIX + key.len() + CHILD_PTR_SIZE;
        self.current_size() + additional_size <= Self::max_size()
    }
}

#[derive(Debug, Clone)]
struct LeafNode {
    keys: Vec<Vec<u8>>,
    values: Vec<Vec<u8>>,
    next_leaf: u64, // 0 means no next leaf
    prev_leaf: u64, // 0 means no previous leaf
    offset: u64,
}

impl LeafNode {
    fn new(offset: u64) -> Self {
        LeafNode {
            keys: Vec::new(),
            values: Vec::new(),
            next_leaf: 0,
            prev_leaf: 0,
            offset,
        }
    }

    fn deserialize(buffer: &[u8], offset: u64) -> Result<Self> {
        if buffer.len() != PAGE_SIZE - 4 {
            return Err(BPlusTreeError::Deserialization(format!(
                "Invalid node size {} (expected {})",
                buffer.len(),
                PAGE_SIZE - 4
            )));
        }

        let mut buffer_slice = &buffer[1..]; // Skip node type

        let num_keys = u32::from_le_bytes(buffer_slice[..4].try_into().unwrap()) as usize;
        buffer_slice = &buffer_slice[4..];

        // Next and prev leaf pointers
        let next_leaf = u64::from_le_bytes(buffer_slice[..8].try_into().unwrap());
        buffer_slice = &buffer_slice[8..];
        let prev_leaf = u64::from_le_bytes(buffer_slice[..8].try_into().unwrap());
        buffer_slice = &buffer_slice[8..];

        // We validate by size
        let mut keys = Vec::with_capacity(num_keys);
        let mut values = Vec::with_capacity(num_keys);

        for _ in 0..num_keys {
            // Read key
            if buffer_slice.len() < 4 {
                return Err(BPlusTreeError::Deserialization(
                    "Truncated key length".into(),
                ));
            }
            let key_len = u32::from_le_bytes(buffer_slice[..4].try_into().unwrap()) as usize;
            buffer_slice = &buffer_slice[4..];

            if key_len > buffer_slice.len() {
                return Err(BPlusTreeError::Deserialization(format!(
                    "Key length {} exceeds available buffer size {}",
                    key_len,
                    buffer_slice.len()
                )));
            }
            keys.push(buffer_slice[..key_len].to_vec());
            buffer_slice = &buffer_slice[key_len..];

            // Read value
            if buffer_slice.len() < 4 {
                return Err(BPlusTreeError::Deserialization(
                    "Truncated value length".into(),
                ));
            }
            let value_len = u32::from_le_bytes(buffer_slice[..4].try_into().unwrap()) as usize;
            buffer_slice = &buffer_slice[4..];

            if value_len > buffer_slice.len() {
                return Err(BPlusTreeError::Deserialization(format!(
                    "Value length {} exceeds available buffer size {}",
                    value_len,
                    buffer_slice.len()
                )));
            }
            values.push(buffer_slice[..value_len].to_vec());
            buffer_slice = &buffer_slice[value_len..];
        }

        Ok(LeafNode {
            keys,
            values,
            next_leaf,
            prev_leaf,
            offset,
        })
    }

    // insert a key-value pair into a leaf node at the correct position
    fn insert(&mut self, key: &[u8], value: &[u8], compare: &CompareFunction) -> usize {
        let mut idx = 0;
        while idx < self.keys.len() && compare(key, &self.keys[idx]) == Ordering::Greater {
            idx += 1;
        }

        self.keys.insert(idx, key.to_vec());
        self.values.insert(idx, value.to_vec());
        idx
    }

    // Find a key's position in the leaf
    fn find_key(&self, key: &[u8], compare: &CompareFunction) -> Option<usize> {
        // Binary search to find exact match
        let mut low = 0;
        let mut high = self.keys.len();

        while low < high {
            let mid = low + (high - low) / 2;
            match compare(key, &self.keys[mid]) {
                Ordering::Less => high = mid,
                Ordering::Equal => return Some(mid),
                Ordering::Greater => low = mid + 1,
            }
        }

        None
    }

    // Find optimal split point based on size
    fn find_split_point(&self, key: &[u8], value: &[u8], compare: &CompareFunction) -> usize {
        // Find where the new key would go
        let mut insert_idx = 0;
        while insert_idx < self.keys.len()
            && (compare)(key, &self.keys[insert_idx]) == Ordering::Greater
        {
            insert_idx += 1;
        }

        // Create temporary arrays to simulate the insertion
        let mut temp_keys = self.keys.clone();
        let mut temp_values = self.values.clone();

        temp_keys.insert(insert_idx, key.to_vec());
        temp_values.insert(insert_idx, value.to_vec());

        // Calculate target size (approximately half the total)
        let total_size =
            self.current_size() + KEY_SIZE_PREFIX + key.len() + VALUE_SIZE_PREFIX + value.len();
        let target_size = total_size / 2;

        // Find split point that results in approximately balanced sizes
        let mut current_size = LEAF_HEADER_SIZE;
        let mut split_idx = 0;

        for (i, (key, value)) in temp_keys.iter().zip(&temp_values).enumerate() {
            let entry_size = KEY_SIZE_PREFIX + key.len() + VALUE_SIZE_PREFIX + value.len();

            if current_size + entry_size > target_size && i > 0 {
                split_idx = i;
                break;
            }

            current_size += entry_size;
        }

        // Ensure we don't split at the end
        if split_idx == 0 || split_idx >= temp_keys.len() - 1 {
            split_idx = temp_keys.len() / 2;
        }

        split_idx
    }

    // Check if this leaf can fit another key-value pair
    fn can_fit_entry(&self, key: &[u8], value: &[u8]) -> bool {
        self.would_fit(key, Some(value))
    }
}

impl Node for LeafNode {
    fn serialize(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::with_capacity(PAGE_SIZE - 4);

        // 1. Node type (1 byte)
        buffer.push(NODE_TYPE_LEAF);

        // 2. Number of keys (4 bytes)
        buffer.extend_from_slice(&(self.keys.len() as u32).to_le_bytes());

        // 3. Next leaf pointer (8 bytes)
        buffer.extend_from_slice(&self.next_leaf.to_le_bytes());

        // 4. Previous leaf pointer (8 bytes)
        buffer.extend_from_slice(&self.prev_leaf.to_le_bytes());

        // 5. Serialize keys and values
        for (key, value) in self.keys.iter().zip(&self.values) {
            // Key serialization (4 + key_len bytes)
            buffer.extend_from_slice(&(key.len() as u32).to_le_bytes());
            buffer.extend_from_slice(key);

            // Value serialization (4 + value_len bytes)
            buffer.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buffer.extend_from_slice(value);
        }

        // 6. Calculate total space used
        let total_used = buffer.len();
        let available_space = PAGE_SIZE - 4; // Reserve 4 bytes for length prefix

        // 7. Validate size before padding
        if total_used > available_space {
            return Err(BPlusTreeError::Serialization(format!(
                "Leaf node requires {total_used} bytes (max {available_space})"
            )));
        }

        // 8. Pad to fill page
        buffer.resize(available_space, 0);
        Ok(buffer)
    }

    fn current_size(&self) -> usize {
        // Base size for leaf node header
        let mut size = LEAF_HEADER_SIZE;

        // Size for all keys and values with their length prefixes
        for (key, value) in self.keys.iter().zip(&self.values) {
            size += KEY_SIZE_PREFIX + key.len() + VALUE_SIZE_PREFIX + value.len();
        }

        size
    }

    fn would_fit(&self, key: &[u8], value: Option<&[u8]>) -> bool {
        // Ensure value is provided for leaf nodes
        let value = value.expect("Value must be provided for leaf nodes");

        // Calculate size with additional key-value pair
        let additional_size = KEY_SIZE_PREFIX + key.len() + VALUE_SIZE_PREFIX + value.len();
        self.current_size() + additional_size <= Self::max_size()
    }
}

#[derive(Debug, Clone)]
struct TrunkPage {
    next_trunk: u64,      // Offset of the next trunk page
    num_free_pages: u32,  // Number of free pages in this trunk
    free_pages: Vec<u32>, // Array of free page numbers (using u32)
    offset: u64,          // Offset of this trunk page
}

impl TrunkPage {
    fn is_empty(&self) -> bool {
        self.free_pages.is_empty()
    }

    fn get_free_page(&mut self) -> Option<u64> {
        if self.is_empty() {
            return None;
        }

        let page_number = self.free_pages.pop()?;
        self.num_free_pages -= 1;

        // Convert page number back to offset (multiply by page size)
        Some(page_number as u64 * PAGE_SIZE as u64)
    }

    fn serialize(&self) -> Result<Vec<u8>> {
        let mut buffer = Vec::with_capacity(PAGE_SIZE - 4);

        // 1. Page type (1 byte)
        buffer.push(TRUNK_PAGE_TYPE);

        // 2. Next trunk pointer (8 bytes)
        buffer.extend_from_slice(&self.next_trunk.to_le_bytes());

        // 3. Number of free pages (4 bytes)
        buffer.extend_from_slice(&self.num_free_pages.to_le_bytes());

        // 4. Free page numbers (4 bytes each, little-endian to match the rest of the code)
        for &page in &self.free_pages {
            buffer.extend_from_slice(&page.to_le_bytes());
        }

        // 5. Pad to fill page
        let available_space = PAGE_SIZE - 4;
        if buffer.len() > available_space {
            return Err(BPlusTreeError::Serialization("Trunk page overflow".into()));
        }
        buffer.resize(available_space, 0);

        Ok(buffer)
    }

    fn deserialize(buffer: &[u8], offset: u64) -> Result<Self> {
        if buffer.len() != PAGE_SIZE - 4 {
            return Err(BPlusTreeError::Deserialization(
                "Invalid trunk page size".into(),
            ));
        }

        if buffer[0] != TRUNK_PAGE_TYPE {
            return Err(BPlusTreeError::Deserialization("Not a trunk page".into()));
        }

        // Read next trunk pointer (8 bytes)
        let next_trunk = u64::from_le_bytes(buffer[1..9].try_into().unwrap());

        // Read number of free pages (4 bytes)
        let num_free_pages = u32::from_le_bytes(buffer[9..13].try_into().unwrap()) as usize;

        // Check if num_free_pages is reasonable
        if num_free_pages > TRUNK_PAGE_MAX_ENTRIES {
            return Err(BPlusTreeError::Deserialization(format!(
                "Invalid number of free pages in trunk: {num_free_pages} (max {TRUNK_PAGE_MAX_ENTRIES})"
            )));
        }

        // Read free page numbers (4 bytes each, little-endian)
        let mut free_pages = Vec::with_capacity(num_free_pages);
        for i in 0..num_free_pages {
            let start = TRUNK_PAGE_HEADER_SIZE + i * 4;
            let end = start + 4;
            if end > buffer.len() {
                return Err(BPlusTreeError::Deserialization(
                    "Truncated trunk page data".into(),
                ));
            }
            free_pages.push(u32::from_le_bytes(buffer[start..end].try_into().unwrap()));
        }

        Ok(TrunkPage {
            next_trunk,
            num_free_pages: num_free_pages as u32,
            free_pages,
            offset,
        })
    }
}

#[derive(Clone)]
enum NodeType {
    Internal(InternalNode),
    Leaf(LeafNode),
}

impl NodeType {
    fn deserialize(buffer: &[u8], offset: u64) -> Result<Self> {
        if buffer.is_empty() {
            return Err(BPlusTreeError::Deserialization("Empty buffer".into()));
        }

        match buffer[0] {
            NODE_TYPE_INTERNAL => Ok(NodeType::Internal(InternalNode::deserialize(
                buffer, offset,
            )?)),
            NODE_TYPE_LEAF => Ok(NodeType::Leaf(LeafNode::deserialize(buffer, offset)?)),
            _ => Err(BPlusTreeError::InvalidNodeType),
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
pub enum Durability {
    /// Sync after every write (safe but slow)
    Always,

    #[allow(dead_code)]
    /// Only sync when flush() or close() is called (fast but risks data loss on crash)
    Manual,
}

pub struct BPlusTree<S: Storage> {
    storage: S,
    header: Header,
    cache: Cache<u64, NodeType, NodeWeighter>,
    compare: Box<CompareFunction>,
    durability: Durability,
}

impl<S: Storage> Drop for BPlusTree<S> {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            eprintln!("Error during BPlusTree drop: {e}");
        }
    }
}

impl BPlusTree<DiskStorage> {
    pub fn disk<P: AsRef<Path>>(path: P, compare: Box<CompareFunction>) -> Result<Self> {
        let storage = DiskStorage::new(path)?;
        Self::with_storage(storage, compare)
    }
}

pub fn new_disk_tree<P: AsRef<Path>>(
    path: P,
    compare: Box<CompareFunction>,
) -> Result<DiskBPlusTree> {
    DiskBPlusTree::disk(path, compare)
}

impl<S: Storage> BPlusTree<S> {
    pub fn with_storage(storage: S, compare: Box<CompareFunction>) -> Result<Self> {
        let storage_size = storage.len()?;

        let (header, cache) = if storage_size == 0 {
            // Initialize a new B+Tree
            let root_offset = PAGE_SIZE as u64;

            let header = Header {
                magic: MAGIC,
                version: VERSION,
                root_offset,
                total_pages: 2,
                first_leaf_offset: root_offset,
                trunk_page_head: 0,
                free_page_count: 0,
            };

            // Create cache
            let cache = Cache::with_weighter(CACHE_CAPACITY as usize, CACHE_CAPACITY, NodeWeighter);

            (header, cache)
        } else {
            // Read existing header
            let mut buffer = [0u8; 48];
            storage.read_at(&mut buffer, 0)?;
            let header = Header::deserialize(&buffer)?;

            let cache = Cache::with_weighter(CACHE_CAPACITY as usize, CACHE_CAPACITY, NodeWeighter);

            (header, cache)
        };

        let mut tree = BPlusTree {
            storage,
            header,
            cache,
            compare,
            durability: Durability::Always,
        };

        // Initialize storage if it's a new tree
        if storage_size == 0 {
            let header_bytes = tree.header.serialize();
            let mut buffer = vec![0u8; PAGE_SIZE];
            buffer[..header_bytes.len()].copy_from_slice(&header_bytes);

            tree.storage.write_at(&buffer, 0)?;

            // Create initial root node
            let root = LeafNode::new(tree.header.root_offset);
            tree.write_node(&NodeType::Leaf(root))?;
            tree.storage.sync()?;
        } else {
            // Read root node into cache
            tree.read_node(tree.header.root_offset)?;
        }

        Ok(tree)
    }

    pub fn sync(&mut self) -> Result<()> {
        self.storage.sync_data()?;
        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        self.storage.sync()?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        // Just to ensure all file data and metadata is synced
        self.sync()?;
        Ok(())
    }

    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // Check if combined size of key and value would ever fit in a node
        // TODO: This has to be replaced using overflow pages
        let min_entry_size = KEY_SIZE_PREFIX + key.len() + VALUE_SIZE_PREFIX + value.len();
        if min_entry_size > LeafNode::max_size() - LEAF_HEADER_SIZE {
            return Err(BPlusTreeError::KeyValueTooLarge);
        }

        // Using a stack to track the path from root to leaf
        // Each entry contains (node_offset, parent_offset)
        let mut path = Vec::new();
        let mut current_offset = self.header.root_offset;
        let mut parent_offset = None;

        // Traverse down to the leaf node where key should be inserted
        loop {
            let node_type = if let Some(node) = self.cache.get(&current_offset) {
                node.clone()
            } else {
                self.read_node(current_offset)?
            };

            match node_type {
                NodeType::Internal(internal) => {
                    // Push current node to path before moving to child
                    path.push((current_offset, parent_offset));

                    // Find the appropriate child
                    let child_idx = internal.find_child_index(key, &self.compare);
                    parent_offset = Some(current_offset);
                    current_offset = internal.children[child_idx];

                    // Continue to next iteration (deeper in the tree)
                }
                NodeType::Leaf(mut leaf) => {
                    // Found the leaf node where key should be inserted
                    // Check if the key already exists (for update case)
                    if let Some(existing_idx) = leaf.find_key(key, &self.compare) {
                        // Key exists, update the value
                        leaf.values[existing_idx] = value.to_vec();
                        self.write_node(&NodeType::Leaf(leaf))?;
                        return Ok(());
                    }

                    // Key doesn't exist, proceed with insertion
                    if leaf.can_fit_entry(key, value) {
                        // Simple case: leaf has space
                        self.insert_into_leaf(&mut leaf, key, value)?;
                        return Ok(());
                    } else {
                        // Leaf is full, need to split
                        // First perform the split
                        let (promoted_key, new_leaf_offset) =
                            self.split_leaf(&mut leaf, key, value)?;

                        // Process the split upwards
                        self.handle_splits(parent_offset, promoted_key, new_leaf_offset, path)?;
                        return Ok(());
                    }
                }
            }
        }
    }

    fn handle_splits(
        &mut self,
        mut parent_offset: Option<u64>,
        mut promoted_key: Vec<u8>,
        mut new_node_offset: u64,
        mut path: Vec<(u64, Option<u64>)>,
    ) -> Result<()> {
        loop {
            match parent_offset {
                None => {
                    // No parent means we need a new root
                    let new_root_offset = self.allocate_page()?;
                    let mut new_root = InternalNode::new(new_root_offset);

                    // Set up the new root with the old root and new node
                    new_root.keys.push(promoted_key);
                    new_root.children.push(if path.is_empty() {
                        // If path is empty, we're splitting the root leaf
                        self.header.root_offset
                    } else {
                        // Otherwise, the last node offset in the path is the current root
                        path.last().unwrap().0
                    });
                    new_root.children.push(new_node_offset);

                    // Update tree header and write the new root
                    self.header.root_offset = new_root_offset;
                    self.write_header()?;
                    self.write_node(&NodeType::Internal(new_root))?;

                    // We're done when we create a new root
                    return Ok(());
                }
                Some(offset) => {
                    // Get the parent node
                    let mut parent = match self.cache.get(&offset) {
                        Some(NodeType::Internal(ref node)) => node.clone(),
                        _ => match self.read_node(offset)? {
                            NodeType::Internal(node) => node,
                            _ => return Err(BPlusTreeError::InvalidNodeType),
                        },
                    };

                    if parent.can_fit_entry(&promoted_key) {
                        // Parent has space, insert the new key and child
                        parent.insert_key_child(&promoted_key, new_node_offset, &self.compare);
                        self.write_node(&NodeType::Internal(parent))?;

                        // Done when we find a parent with enough space
                        return Ok(());
                    } else {
                        // Parent is also full, split it and continue upward
                        let (next_promoted_key, next_new_node_offset) = self
                            .split_internal_with_child(
                                &mut parent,
                                &promoted_key,
                                new_node_offset,
                            )?;

                        // Pop the next level off the path stack
                        let (_, next_parent) = path.pop().unwrap_or_default();

                        // Move up one level in the tree
                        promoted_key = next_promoted_key;
                        new_node_offset = next_new_node_offset;
                        parent_offset = next_parent;

                        // Continue to next iteration (process grandparent)
                    }
                }
            }
        }
    }

    fn insert_into_leaf(&mut self, leaf: &mut LeafNode, key: &[u8], value: &[u8]) -> Result<()> {
        leaf.insert(key, value, &self.compare);
        self.write_node(&NodeType::Leaf(leaf.clone()))?;
        Ok(())
    }

    fn split_leaf(
        &mut self,
        leaf: &mut LeafNode,
        key: &[u8],
        value: &[u8],
    ) -> Result<(Vec<u8>, u64)> {
        // Find optimal split point
        let split_idx = leaf.find_split_point(key, value, &self.compare);

        // Create new leaf
        let new_leaf_offset = self.allocate_page()?;
        let mut new_leaf = LeafNode::new(new_leaf_offset);

        // Find insertion point for the new key-value
        let mut idx = 0;
        while idx < leaf.keys.len() && (self.compare)(key, &leaf.keys[idx]) == Ordering::Greater {
            idx += 1;
        }

        if idx < split_idx {
            // New entry belongs in the left node
            // Move entries after split_idx to new leaf
            new_leaf.keys = leaf.keys.drain(split_idx..).collect();
            new_leaf.values = leaf.values.drain(split_idx..).collect();

            // Insert the new key-value into leaf
            leaf.keys.insert(idx, key.to_vec());
            leaf.values.insert(idx, value.to_vec());
        } else {
            // New entry belongs in the right node
            // Adjust index for the right node
            let right_idx = idx - split_idx;

            // Move entries after split_idx to new leaf
            new_leaf.keys = leaf.keys.drain(split_idx..).collect();
            new_leaf.values = leaf.values.drain(split_idx..).collect();

            // Insert the new key-value into new leaf
            new_leaf.keys.insert(right_idx, key.to_vec());
            new_leaf.values.insert(right_idx, value.to_vec());
        }

        // Update leaf pointers for linked list
        new_leaf.next_leaf = leaf.next_leaf;
        new_leaf.prev_leaf = leaf.offset;
        leaf.next_leaf = new_leaf.offset;

        // Get the promoted key (first key of right node)
        let promoted_key = new_leaf.keys[0].clone();

        // If the new leaf has a next leaf, update its prev pointer
        let next_leaf_update = if new_leaf.next_leaf != 0 {
            Some(new_leaf.next_leaf)
        } else {
            None
        };

        let new_leaf_offset = new_leaf.offset;

        // Write both leaves
        self.write_node(&NodeType::Leaf(leaf.clone()))?;
        self.write_node(&NodeType::Leaf(new_leaf))?;

        // Update the next node's prev pointer if needed
        if let Some(next_offset) = next_leaf_update {
            if let NodeType::Leaf(mut next_leaf) = self.read_node(next_offset)? {
                next_leaf.prev_leaf = new_leaf_offset;
                self.write_node(&NodeType::Leaf(next_leaf))?;
            }
        }

        // Return the key that will be promoted to the parent and the new leaf offset
        Ok((promoted_key, new_leaf_offset))
    }

    fn split_internal_with_child(
        &mut self,
        node: &mut InternalNode,
        extra_key: &[u8],
        extra_child: u64,
    ) -> Result<(Vec<u8>, u64)> {
        // Insert the extra key and child into the node temporarily
        node.insert_key_child(extra_key, extra_child, &self.compare);

        // Now split the node
        let (promoted_key, new_node_offset) = self.split_internal(node, None)?;

        Ok((promoted_key, new_node_offset))
    }

    fn split_internal(
        &mut self,
        node: &mut InternalNode,
        new_key: Option<&[u8]>,
    ) -> Result<(Vec<u8>, u64)> {
        let split_idx = if let Some(key) = new_key {
            // If we're splitting due to a specific key, find optimal split point
            node.find_split_point(key, &self.compare)
        } else {
            // Otherwise use a simple middle split
            node.keys.len() / 2
        };

        // Create new internal node
        let new_node_offset = self.allocate_page()?;
        let mut new_node = InternalNode::new(new_node_offset);

        // Key to be promoted to parent
        let promoted_key = node.keys[split_idx].clone();

        // Move keys and children after split point to new node
        new_node.keys = node.keys.drain(split_idx + 1..).collect();
        new_node.children = node.children.drain(split_idx + 1..).collect();

        // Remove the middle key from the original node (it gets promoted)
        node.keys.remove(split_idx);

        // Write both nodes
        self.write_node(&NodeType::Internal(node.clone()))?;
        self.write_node(&NodeType::Internal(new_node))?;

        Ok((promoted_key, new_node_offset))
    }

    pub fn search(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.search_tree(self.header.root_offset, key)
    }

    fn search_tree(&self, node_offset: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.read_node(node_offset)? {
            NodeType::Internal(internal) => {
                // Find appropriate child
                let child_idx = internal.find_child_index(key, &self.compare);

                // Search in the appropriate child
                self.search_tree(internal.children[child_idx], key)
            }
            NodeType::Leaf(leaf) => {
                // Search for the key in the leaf
                match leaf.find_key(key, &self.compare) {
                    Some(idx) => Ok(Some(leaf.values[idx].clone())),
                    None => Ok(None),
                }
            }
        }
    }

    // Disk management methods
    fn allocate_page(&mut self) -> Result<u64> {
        // This function follows SQLite's approach (or at least what I understand of it) for page allocation:

        // Fast path: No free pages in any trunk, allocate new page
        // This happens when either:
        // - There are no trunk pages (trunk_page_head is 0)
        // - Or there are trunk pages but they're all empty (free_page_count is 0)
        if self.header.trunk_page_head == 0 || self.header.free_page_count == 0 {
            let offset = self.header.total_pages * PAGE_SIZE as u64;
            self.header.total_pages += 1;
            self.write_header()?;
            return Ok(offset);
        }

        // Traverse the trunk chain to find a non-empty trunk
        // prev_trunk_offset tracks the previous trunk page in the chain
        // current_trunk_offset points to the current trunk page being examined
        let mut prev_trunk_offset = 0;
        let mut current_trunk_offset = self.header.trunk_page_head;

        while current_trunk_offset != 0 {
            let mut trunk = self.read_trunk_page(current_trunk_offset)?;

            // Found a trunk with free pages - allocate one of them
            if !trunk.is_empty() {
                let page_offset = trunk.get_free_page().unwrap();
                self.write_trunk_page(&trunk)?;
                self.header.free_page_count -= 1;
                self.write_header()?;
                return Ok(page_offset);
            }

            // This trunk is empty, check if it has a next trunk
            let next_trunk = trunk.next_trunk;

            if next_trunk != 0 {
                // We have a next trunk, so we can reuse this empty trunk page
                // Before returning this trunk page as newly allocated space,
                // we need to update the trunk chain

                if prev_trunk_offset == 0 {
                    // This is the head trunk - update the header to point to the next trunk
                    self.header.trunk_page_head = next_trunk;
                } else {
                    // This is a trunk in the middle or end of the chain
                    // Update the previous trunk's next_trunk pointer to skip this trunk
                    // and point directly to this trunk's next trunk
                    let mut prev_trunk = self.read_trunk_page(prev_trunk_offset)?;
                    prev_trunk.next_trunk = next_trunk;
                    self.write_trunk_page(&prev_trunk)?;
                }

                // Update the header with the new trunk_page_head value
                self.write_header()?;

                // Return this empty trunk page as the newly allocated page
                return Ok(current_trunk_offset);
            }

            // Move to the next trunk in the chain
            prev_trunk_offset = current_trunk_offset;
            current_trunk_offset = trunk.next_trunk;
        }

        // If we get here, we've traversed all trunks and they're all empty
        // with no free pages, but free_page_count > 0.
        // This is a database corruption
        Err(BPlusTreeError::Corruption(format!(
            "Inconsistent free page count: header says {} free pages but none found in trunk chain",
            self.header.free_page_count
        )))
    }

    fn read_trunk_page(&mut self, offset: u64) -> Result<TrunkPage> {
        // Read directly from disk
        let mut buffer = vec![0; PAGE_SIZE];
        self.storage.read_at(&mut buffer, offset)?;

        // Validate length prefix
        let len = u32::from_le_bytes(buffer[..4].try_into().unwrap()) as usize;
        if len > PAGE_SIZE - 4 {
            return Err(BPlusTreeError::Deserialization(format!(
                "Invalid trunk page length {len}"
            )));
        }

        let trunk = TrunkPage::deserialize(&buffer[4..], offset)?;

        Ok(trunk)
    }

    fn write_trunk_page(&mut self, trunk: &TrunkPage) -> Result<()> {
        let data = trunk.serialize()?;

        let mut buffer = Vec::with_capacity(PAGE_SIZE);
        buffer.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&data);
        buffer.resize(PAGE_SIZE, 0);

        self.storage.write_at(&buffer, trunk.offset)?;

        self.maybe_sync()?;

        Ok(())
    }

    fn read_node(&self, offset: u64) -> Result<NodeType> {
        // Check if the node is in the cache first
        if let Some(node) = self.cache.get(&offset) {
            return Ok(node.clone());
        }

        // Read from disk
        let mut buffer = vec![0; PAGE_SIZE];
        self.storage.read_at(&mut buffer, offset)?;

        // Validate length prefix
        let len = u32::from_le_bytes(buffer[..4].try_into().unwrap()) as usize;
        if len > PAGE_SIZE - 4 {
            return Err(BPlusTreeError::Deserialization(format!(
                "Invalid node length {len}"
            )));
        }

        let node = NodeType::deserialize(&buffer[4..], offset)?;
        self.cache.insert(offset, node.clone());

        Ok(node)
    }

    fn write_node(&mut self, node: &NodeType) -> Result<()> {
        let data = match node {
            NodeType::Internal(internal) => internal.serialize()?,
            NodeType::Leaf(leaf) => leaf.serialize()?,
        };

        let mut buffer = Vec::with_capacity(PAGE_SIZE);
        buffer.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buffer.extend_from_slice(&data);
        buffer.resize(PAGE_SIZE, 0);

        let offset = match node {
            NodeType::Internal(internal) => internal.offset,
            NodeType::Leaf(leaf) => leaf.offset,
        };

        self.storage.write_at(&buffer, offset)?;

        self.maybe_sync()?;

        self.cache.insert(offset, node.clone());
        Ok(())
    }

    fn write_header(&mut self) -> Result<()> {
        // Write header data at offset 0
        let header_bytes = self.header.serialize();

        // Create a buffer with the header followed by zeros
        let mut buffer = vec![0u8; PAGE_SIZE];
        buffer[..header_bytes.len()].copy_from_slice(&header_bytes);

        // Write the entire page at once
        self.storage.write_at(&buffer, 0)?;

        self.maybe_sync()?;

        Ok(())
    }

    fn maybe_sync(&mut self) -> Result<()> {
        match self.durability {
            Durability::Always => self.storage.sync()?,
            Durability::Manual => { // Don't sync - only sync on flush() or close()
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Read;

    use super::*;
    use rand::Rng;
    use tempfile::NamedTempFile;

    fn create_test_tree() -> BPlusTree<DiskStorage> {
        let file = NamedTempFile::new().unwrap();
        BPlusTree::disk(file.path(), Box::new(|a, b| a.cmp(b))).unwrap()
    }

    #[test]
    fn test_basic_operations() {
        let mut tree = create_test_tree();

        // Test insertions
        tree.insert(b"key1", b"value1").unwrap();
        tree.insert(b"key2", b"value2").unwrap();
        tree.insert(b"key3", b"value3").unwrap();

        // Test retrievals
        assert_eq!(tree.search(b"key1").unwrap().unwrap(), b"value1");
        assert_eq!(tree.search(b"key2").unwrap().unwrap(), b"value2");
        assert_eq!(tree.search(b"key3").unwrap().unwrap(), b"value3");

        // Test non-existent key
        assert!(tree.search(b"nonexistent").unwrap().is_none());
    }

    #[test]
    fn test_sequential_small() {
        let mut tree = create_test_tree();

        // Insert 10 items
        for i in 0..10 {
            let key = format!("key{i:03}").into_bytes();
            let value = format!("value{i:03}").into_bytes();
            tree.insert(&key, &value).unwrap();
        }

        // Delete even numbered items
        for i in (0..10).step_by(2) {
            let key = format!("key{i:03}").into_bytes();

            // Verify key exists before deletion
            let _ = tree.search(&key).unwrap();
        }

        // Verify all odd numbered items still exist
        for i in (1..10).step_by(2) {
            let key = format!("key{i:03}").into_bytes();
            let value = format!("value{i:03}").into_bytes();

            let result = tree.search(&key).unwrap();

            assert!(
                result.is_some(),
                "Key should exist but doesn't: {:?}",
                String::from_utf8_lossy(&key)
            );
            assert_eq!(
                result.unwrap(),
                value,
                "Value mismatch for key: {:?}",
                String::from_utf8_lossy(&key)
            );
        }
    }

    const TEST_SIZE: usize = 2000;

    #[test]
    fn test_sequential_insert() {
        let mut tree = create_test_tree();

        let keys = generate_sequential_keys(TEST_SIZE);
        let values = generate_random_values(TEST_SIZE, 100);

        // Insert and immediately verify each key-value pair
        for i in 0..TEST_SIZE {
            println!("Inserting key {i}");
            tree.insert(&keys[i], &values[i]).unwrap();

            let retrieved = tree.search(&keys[i]).unwrap();
            assert!(
                retrieved.is_some(),
                "Failed to retrieve just-inserted key at index {i}"
            );
            assert_eq!(
                retrieved.unwrap(),
                values[i],
                "Retrieved value doesn't match at index {i}"
            );
        }

        // Final verification of all keys
        println!("\nFinal verification:");
        for i in 0..TEST_SIZE {
            let retrieved = tree.search(&keys[i]).unwrap();
            assert!(retrieved.is_some(), "Value at index {i} should still exist");
            assert_eq!(
                retrieved.unwrap(),
                values[i],
                "Retrieved value doesn't match at index {i}"
            );
        }
    }

    fn generate_sequential_keys(n: usize) -> Vec<Vec<u8>> {
        (0..n).map(|i| format!("key{i:010}").into_bytes()).collect()
    }

    fn generate_random_values(n: usize, value_size: usize) -> Vec<Vec<u8>> {
        let mut rng = rand::thread_rng();
        (0..n)
            .map(|_| (0..value_size).map(|_| rng.gen::<u8>()).collect())
            .collect()
    }

    #[test]
    fn test_empty_tree() {
        let tree = create_test_tree();
        assert_eq!(tree.search(b"key").unwrap(), None);
    }

    #[test]
    fn test_single_insert_search() {
        let mut tree = create_test_tree();
        tree.insert(b"key1", b"value1").unwrap();
        assert_eq!(tree.search(b"key1").unwrap(), Some(b"value1".to_vec()));
    }

    #[test]
    fn test_insert_duplicate_key_updates_value() {
        let mut tree = create_test_tree();
        tree.insert(b"key", b"value1").unwrap();
        // Second insert should update the value, not return an error
        tree.insert(b"key", b"value2").unwrap();

        // Verify the value was updated
        assert_eq!(tree.search(b"key").unwrap(), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_predecessor_successor_operations() {
        let mut tree = create_test_tree();

        let keys = vec![
            b"b".to_vec(),
            b"d".to_vec(),
            b"f".to_vec(),
            b"h".to_vec(),
            b"j".to_vec(),
        ];

        for key in &keys {
            tree.insert(key, b"value").unwrap();
        }

        // Verify remaining keys
        assert!(tree.search(b"b").unwrap().is_some());
        assert!(tree.search(b"d").unwrap().is_some());
        assert!(tree.search(b"h").unwrap().is_some());
        assert!(tree.search(b"j").unwrap().is_some());
    }

    #[test]
    fn test_tree_persistence() -> Result<()> {
        // Create a temporary file
        let file = NamedTempFile::new()?;
        let path = file.path();

        // Test data
        let test_data = vec![
            (b"apple".to_vec(), b"red".to_vec()),
            (b"banana".to_vec(), b"yellow".to_vec()),
            (b"grape".to_vec(), b"purple".to_vec()),
        ];

        // Insert data and close
        {
            let mut tree = BPlusTree::disk(path, Box::new(|a, b| a.cmp(b)))?;
            for (key, value) in &test_data {
                tree.insert(key, value)?;
            }
        } // BPlusTree is dropped here, file flushed and closed

        // Reopen and verify
        {
            let mut tree = BPlusTree::disk(path, Box::new(|a, b| a.cmp(b)))?;

            // Verify existing data
            for (key, value) in &test_data {
                assert_eq!(
                    tree.search(key)?,
                    Some(value.clone()),
                    "Key {:?} not found after reopening",
                    String::from_utf8_lossy(key)
                );
            }

            // Verify non-existent key
            assert_eq!(
                tree.search(b"mango")?,
                None,
                "Non-existent key found unexpectedly"
            );

            // Add new data and verify
            tree.insert(b"mango", b"orange")?;
            assert_eq!(
                tree.search(b"mango")?,
                Some(b"orange".to_vec()),
                "New insertion failed after reopening"
            );
        }

        // Reopen again to verify new data persisted
        {
            let tree = BPlusTree::disk(path, Box::new(|a, b| a.cmp(b)))?;
            assert_eq!(
                tree.search(b"mango")?,
                Some(b"orange".to_vec()),
                "New data didn't persist across openings"
            );
        }

        Ok(())
    }

    #[test]
    fn test_concurrent_operations() {
        let mut tree = create_test_tree();
        // Insert and delete same key repeatedly
        tree.insert(b"key", b"v1").unwrap();
        tree.insert(b"key", b"v2").unwrap();
        assert_eq!(tree.search(b"key").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn test_drop_behavior() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path();

        // Create and immediately drop
        {
            let mut tree = BPlusTree::disk(path, Box::new(|a, b| a.cmp(b))).unwrap();
            tree.insert(b"test", b"value").unwrap();
            // Explicit drop before end of scope
            drop(tree);
        }

        // Verify data post drop
        {
            let tree = BPlusTree::disk(path, Box::new(|a, b| a.cmp(b))).unwrap();
            assert_eq!(tree.search(b"test").unwrap(), Some(b"value".to_vec()));
        }
    }

    #[test]
    fn test_explicit_close() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path();

        let mut tree = BPlusTree::disk(path, Box::new(|a, b| a.cmp(b))).unwrap();
        tree.insert(b"close", b"test").unwrap();
        tree.close().unwrap(); // Explicit close
    }

    // #[test]
    // fn header_serialization_roundtrip() {
    //     let original = Header {
    //         magic: MAGIC,
    //         version: 1,
    //         root_offset: 4096,
    //         free_list_head: 0,
    //         total_pages: 2,
    //     };

    //     // Test serialization
    //     let serialized = original.serialize();

    //     // Test deserialization
    //     let deserialized = Header::deserialize(&serialized).unwrap();

    //     assert_eq!(deserialized.magic, original.magic);
    //     assert_eq!(deserialized.version, original.version);
    //     assert_eq!(deserialized.root_offset, original.root_offset);
    //     assert_eq!(deserialized.free_list_head, original.free_list_head);
    //     assert_eq!(deserialized.total_pages, original.total_pages);
    // }

    #[test]
    fn new_file_initializes_correct_header() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Create new BPlusTree
        let _tree = BPlusTree::disk(path, Box::new(|a, b| a.cmp(b))).unwrap();

        // Read header directly from file
        let mut file = File::open(path).unwrap();
        let mut buffer = [0u8; 48];
        file.read_exact(&mut buffer).unwrap();

        let header = Header::deserialize(&buffer).unwrap();

        assert_eq!(header.magic, MAGIC);
        assert_eq!(header.version, 1);
        assert_eq!(header.root_offset, PAGE_SIZE as u64);
        assert_eq!(header.free_page_count, 0);
        assert_eq!(header.trunk_page_head, 0);
        assert_eq!(header.total_pages, 2);
    }

    #[test]
    fn detect_invalid_magic() {
        let mut buffer = [0u8; 48];
        buffer[0..8].copy_from_slice(b"BADMAGIC");
        buffer[8..12].copy_from_slice(&1u32.to_le_bytes());

        match Header::deserialize(&buffer) {
            Err(BPlusTreeError::Deserialization(e)) => {
                assert!(e.contains("Invalid magic number"))
            }
            _ => panic!("Should fail on invalid magic"),
        }
    }

    #[test]
    fn detect_invalid_version() {
        let mut buffer = [0u8; 48];
        buffer[0..8].copy_from_slice(&MAGIC);
        buffer[8..12].copy_from_slice(&2u32.to_le_bytes()); // Unsupported version

        match Header::deserialize(&buffer) {
            Err(BPlusTreeError::Deserialization(e)) => {
                assert!(e.contains("Unsupported version"))
            }
            _ => panic!("Should fail on invalid version"),
        }
    }

    #[test]
    fn detect_corrupted_header() {
        let buffer = [0u8; 35]; // Too small
        match Header::deserialize(&buffer) {
            Err(BPlusTreeError::Deserialization(e)) => {
                assert!(e.contains("Invalid header size"))
            }
            _ => panic!("Should fail on undersized header"),
        }
    }

    #[test]
    fn test_tree_reopen() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path();
        let num_items = 1000;

        // Generate test data
        let test_data: Vec<(Vec<u8>, Vec<u8>)> = (0..num_items)
            .map(|i| {
                let key = format!("key_{i}").into_bytes();
                let value = format!("value_{i}").into_bytes();
                (key, value)
            })
            .collect();

        // Insert data and close
        {
            let mut tree = BPlusTree::disk(path, Box::new(|a, b| a.cmp(b))).unwrap();
            for (key, value) in &test_data {
                tree.insert(key, value).unwrap();
            }
            tree.close().unwrap();
        }

        // Reopen and verify all items
        {
            let tree = BPlusTree::disk(path, Box::new(|a, b| a.cmp(b))).unwrap();
            for (key, value) in &test_data {
                assert_eq!(
                    tree.search(key).unwrap(),
                    Some(value.clone()),
                    "Key {:?} not found after reopening",
                    String::from_utf8_lossy(key)
                );
            }
        }
    }

    #[test]
    fn test_insert_with_multiple_key_sizes() {
        // Define various sizes for testing
        let key_sizes = [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048];

        // Fixed value size
        let value_size = 128;
        let fixed_value = vec![b'v'; value_size];

        // Create key-value pairs with varying key sizes and fixed value size
        for key_size in &key_sizes {
            // Create a new tree for each run
            let mut tree = create_test_tree();

            // Insert the key-value pair `key_size` times with unique keys
            for i in 0..*key_size {
                // Create a key with a distinct pattern based on i
                let mut unique_key = vec![b'k'; *key_size];

                // Use a simple pattern: first 8 bytes store i as a u64
                // This ensures uniqueness without complex conversions
                if *key_size >= 8 {
                    // Convert i to a u64 bytes representation
                    let i_u64 = i as u64;
                    let bytes = i_u64.to_be_bytes();

                    // Place the bytes at the start of the key
                    unique_key[..8].copy_from_slice(&bytes);
                } else {
                    // For small keys, just use modulo but ensure they're unique
                    // For key sizes < 8, use the mod approach but make sure it's unique
                    // by using a combination of byte positions
                    for (j, byte) in unique_key.iter_mut().enumerate().take(*key_size) {
                        *byte = ((i * 251 + j * 241) % 256) as u8;
                    }
                }

                // Insert the unique key-value pair into the tree
                tree.insert(&unique_key, &fixed_value).unwrap();
            }

            // After each run, flush and close the tree
            tree.flush().unwrap();
            tree.close().unwrap();
        }
    }

    #[test]
    fn test_delete_with_multiple_key_sizes() {
        // Define various sizes for testing
        let key_sizes = [2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048];

        // Fixed value size
        let value_size = 128;
        let fixed_value = vec![b'v'; value_size];

        // Create key-value pairs with varying key sizes and fixed value size
        for key_size in &key_sizes {
            // Create a new tree for each run
            let mut tree = create_test_tree();

            // Track all keys for deletion testing
            let mut all_keys = Vec::with_capacity(*key_size);

            // Insert the key-value pair `key_size` times with unique keys
            for i in 0..*key_size {
                // Create a key with a distinct pattern based on i
                let mut unique_key = vec![b'k'; *key_size];

                // Use a simple pattern: first 8 bytes store i as a u64
                // This ensures uniqueness without complex conversions
                if *key_size >= 8 {
                    // Convert i to a u64 bytes representation
                    let i_u64 = i as u64;
                    let bytes = i_u64.to_be_bytes();

                    // Place the bytes at the start of the key
                    unique_key[..8].copy_from_slice(&bytes);
                } else {
                    // For small keys, just use modulo but ensure they're unique
                    // by using a combination of byte positions
                    for (j, byte) in unique_key.iter_mut().enumerate().take(*key_size) {
                        *byte = ((i * 251 + j * 241) % 256) as u8;
                    }
                }

                // Store key for later deletion
                all_keys.push(unique_key.clone());

                // Insert the unique key-value pair into the tree
                tree.insert(&unique_key, &fixed_value).unwrap();

                // Verify the key was inserted correctly
                let result = tree.search(&unique_key).unwrap();
                assert!(
                    result.is_some(),
                    "Failed to retrieve just-inserted key of size {key_size}"
                );
                assert_eq!(result.unwrap(), fixed_value);
            }

            // After each run, flush and close the tree
            tree.flush().unwrap();
            tree.close().unwrap();
        }
    }

    #[test]
    fn test_allocate_new_page_when_no_free_pages() {
        let mut btree = create_test_tree();

        // Set up initial state with no free pages
        btree.header.trunk_page_head = 0;
        btree.header.free_page_count = 0;
        btree.header.total_pages = 1; // Start with just the header page

        // Allocate a page
        let page_offset = btree.allocate_page().unwrap();

        // Verify we got a new page (page 1)
        assert_eq!(page_offset, PAGE_SIZE as u64);
        assert_eq!(btree.header.total_pages, 2);
        assert_eq!(btree.header.trunk_page_head, 0); // Still no trunk pages
        assert_eq!(btree.header.free_page_count, 0); // Still no free pages
    }
}
