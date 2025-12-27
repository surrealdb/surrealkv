use std::cmp::Ordering;
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use quick_cache::sync::Cache;
use quick_cache::Weighter;

use crate::vfs::File as VfsFile;
use crate::{Comparator, Key, Value};

// Cache for local limits (constant for a given PAGE_SIZE)
static LEAF_LIMITS: OnceLock<(usize, usize)> = OnceLock::new();
static INTERNAL_LIMITS: OnceLock<(usize, usize)> = OnceLock::new();

// These are type aliases for convenience
pub type DiskBPlusTree = BPlusTree<File>;

pub fn new_disk_tree<P: AsRef<Path>>(
	path: P,
	compare: Arc<dyn Comparator>,
) -> Result<DiskBPlusTree> {
	DiskBPlusTree::disk(path, compare)
}

#[derive(Clone)]
struct NodeWeighter;

impl Weighter<u64, Arc<NodeType>> for NodeWeighter {
	fn weight(&self, _key: &u64, value: &Arc<NodeType>) -> u64 {
		match value.as_ref() {
			NodeType::Internal(internal) => internal.current_size() as u64,
			NodeType::Leaf(leaf) => leaf.current_size() as u64,
			NodeType::Overflow(overflow) => (13 + overflow.data.len()) as u64,
		}
	}
}

#[derive(Debug)]
pub enum BPlusTreeError {
	Io(io::Error),
	Serialization(String),
	Deserialization(String),
	InvalidOffset,
	CorruptedFreeList(u64),
	InvalidNodeType,
	CorruptedTrunkPage(u64),
	UnexpectedOverflowPage(u64),
	InvalidOverflowChain(u64),
	InvalidRootNode(u64),
	InconsistentFreePageCount {
		claimed: u32,
		actual: u32,
	},
	Corruption(String),
}

impl From<crate::error::Error> for BPlusTreeError {
	fn from(err: crate::error::Error) -> Self {
		BPlusTreeError::Io(std::io::Error::other(format!("VFS error: {}", err)))
	}
}

impl std::fmt::Display for BPlusTreeError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			BPlusTreeError::Io(e) => write!(f, "IO error: {}", e),
			BPlusTreeError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
			BPlusTreeError::Deserialization(msg) => write!(f, "Deserialization error: {}", msg),
			BPlusTreeError::InvalidOffset => write!(f, "Invalid offset"),
			BPlusTreeError::CorruptedFreeList(msg) => {
				write!(f, "Corrupted free list: misaligned offset {}", msg)
			}
			BPlusTreeError::InvalidNodeType => write!(f, "Invalid node type"),
			BPlusTreeError::CorruptedTrunkPage(offset) => {
				write!(f, "Corrupted trunk page at offset {}", offset)
			}
			BPlusTreeError::UnexpectedOverflowPage(offset) => {
				write!(f, "Unexpected overflow page in tree traversal at offset {}", offset)
			}
			BPlusTreeError::InvalidOverflowChain(offset) => {
				write!(f, "Expected overflow page at offset {}, found different node type", offset)
			}
			BPlusTreeError::InvalidRootNode(offset) => {
				write!(f, "Root node is an overflow page at offset {}", offset)
			}
			BPlusTreeError::InconsistentFreePageCount {
				claimed,
				actual,
			} => {
				write!(
					f,
					"Inconsistent free page count: header claims {} but found {}",
					claimed, actual
				)
			}
			BPlusTreeError::Corruption(msg) => write!(f, "Corruption detected: {}", msg),
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

// Page constants
pub const DEFAULT_CACHE_CAPACITY: u64 = 256 * 1024 * 1024; // 256 MiB
const NODE_TYPE_INTERNAL: u8 = 0;
const NODE_TYPE_LEAF: u8 = 1;
const NODE_TYPE_OVERFLOW: u8 = 3;

// Constants for trunk page system
const TRUNK_PAGE_TYPE: u8 = 2; // Node type for trunk pages
const TRUNK_PAGE_HEADER_SIZE: usize = 13; // 1 byte type + 8 bytes next + 4 bytes count
const TRUNK_PAGE_ENTRY_SIZE: usize = 4; // 4-byte entries
const TRUNK_PAGE_MAX_ENTRIES: usize = (PAGE_SIZE - TRUNK_PAGE_HEADER_SIZE) / TRUNK_PAGE_ENTRY_SIZE;

// Constants for size calculation
const LEAF_HEADER_SIZE: usize = 1 + 4 + 8 + 8; // type(1) + key_count(4) + next_leaf(8) + prev_leaf(8) = 21 bytes
const INTERNAL_HEADER_SIZE: usize = 1 + 4 + 4; // type(1) + key_count(4) + child_count(4) = 9 bytes
const LEAF_USABLE_SIZE: usize = PAGE_SIZE - LEAF_HEADER_SIZE;
const INTERNAL_USABLE_SIZE: usize = PAGE_SIZE - INTERNAL_HEADER_SIZE;
const KEY_SIZE_PREFIX: usize = 4; // 4 bytes for key length
const VALUE_SIZE_PREFIX: usize = 4; // 4 bytes for value length
const CHILD_PTR_SIZE: usize = 8; // 8 bytes per child pointer

// Helper functions for reading integer types from byte slices without unwrap()
// These are safe to use when bounds have already been checked
#[inline(always)]
fn read_u32_be(buffer: &[u8], offset: usize) -> u32 {
	// SAFETY: Caller must ensure buffer has at least offset + 4 bytes
	unsafe { u32::from_be_bytes(*(buffer.as_ptr().add(offset) as *const [u8; 4])) }
}

#[inline(always)]
fn read_u64_be(buffer: &[u8], offset: usize) -> u64 {
	// SAFETY: Caller must ensure buffer has at least offset + 8 bytes
	unsafe { u64::from_be_bytes(*(buffer.as_ptr().add(offset) as *const [u8; 8])) }
}

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
		buffer[8..12].copy_from_slice(&self.version.to_be_bytes());
		buffer[12..20].copy_from_slice(&self.root_offset.to_be_bytes());
		buffer[20..28].copy_from_slice(&self.trunk_page_head.to_be_bytes());
		buffer[28..36].copy_from_slice(&self.total_pages.to_be_bytes());
		buffer[36..44].copy_from_slice(&self.first_leaf_offset.to_be_bytes());
		buffer[44..48].copy_from_slice(&self.free_page_count.to_be_bytes());
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
			return Err(BPlusTreeError::Deserialization("Invalid magic number".into()));
		}

		let version = read_u32_be(buffer, 8);
		if version != VERSION {
			return Err(BPlusTreeError::Deserialization(format!(
				"Unsupported version: {}",
				version
			)));
		}

		Ok(Header {
			magic,
			version,
			root_offset: read_u64_be(buffer, 12),
			trunk_page_head: read_u64_be(buffer, 20),
			total_pages: read_u64_be(buffer, 28),
			first_leaf_offset: read_u64_be(buffer, 36),
			free_page_count: read_u32_be(buffer, 44),
		})
	}
}

// Base trait for nodes
trait Node {
	fn serialize(&self) -> Result<Vec<u8>>;
	fn current_size(&self) -> usize;
	fn max_size() -> usize {
		PAGE_SIZE
	}

	// Check if this node could merge with another node
	fn can_merge_with(&self, other: &Self) -> bool;
}

#[derive(Debug, Clone)]
/// Internal node in the B+ tree
///
/// OVERFLOW OWNERSHIP INVARIANT:
/// - key_overflows contains overflow chains for keys stored in this node
/// - Each key owns its overflow chain, including keys used as separators in parents
/// - Overflows are freed when the owning node is freed via free_node_with_overflow()
/// - EXCEPTION: After merge operations, the merged-away node is freed with free_page() only,
///   because its overflows were transferred to the merged node via append()
/// - When keys are transferred between nodes, overflow ownership transfers with them
struct InternalNode {
	keys: Vec<Key>, // Full keys (reconstructed from page + overflow if needed)
	key_overflows: Vec<u64>, /* 0 if no overflow, else first overflow page offset for THIS
	                 * node's keys */
	children: Vec<u64>,
	offset: u64,
}

impl InternalNode {
	fn new(offset: u64) -> Self {
		InternalNode {
			keys: Vec::new(),
			key_overflows: Vec::new(),
			children: Vec::new(),
			offset,
		}
	}

	fn deserialize<F>(buffer: &[u8], offset: u64, read_overflow: &F) -> Result<Self>
	where
		F: Fn(u64) -> Result<Vec<u8>>,
	{
		if buffer.len() != PAGE_SIZE {
			return Err(BPlusTreeError::Deserialization(format!(
				"Invalid node size {} (expected {})",
				buffer.len(),
				PAGE_SIZE
			)));
		}

		let mut buffer_slice = &buffer[1..]; // Skip node type

		let num_keys = u32::from_be_bytes(buffer_slice[..4].try_into().unwrap()) as usize;
		buffer_slice = &buffer_slice[4..];

		let (min_local, max_local) = calculate_local_limits(false);
		let usable_size = INTERNAL_USABLE_SIZE;

		let mut keys = Vec::with_capacity(num_keys);
		let mut key_overflows = Vec::with_capacity(num_keys);

		for _ in 0..num_keys {
			// Read total key length
			let key_len_total = u32::from_be_bytes(buffer_slice[..4].try_into().unwrap()) as usize;
			buffer_slice = &buffer_slice[4..];

			// Calculate bytes on page
			let (bytes_on_page, needs_overflow) =
				calculate_overflow(key_len_total, min_local, max_local, usable_size);

			if bytes_on_page > buffer_slice.len() {
				return Err(BPlusTreeError::Deserialization(format!(
					"Calculated bytes on page {} exceeds available buffer size {}",
					bytes_on_page,
					buffer_slice.len()
				)));
			}

			// Read key data from page
			let mut key_data = buffer_slice[..bytes_on_page].to_vec();
			buffer_slice = &buffer_slice[bytes_on_page..];

			// Check if there's overflow
			let overflow_offset = if needs_overflow {
				// Read overflow page offset
				if buffer_slice.len() < 8 {
					return Err(BPlusTreeError::Deserialization(
						"Buffer too small for overflow pointer".into(),
					));
				}
				let overflow = u64::from_be_bytes(buffer_slice[..8].try_into().unwrap());
				buffer_slice = &buffer_slice[8..];

				// Read overflow data and append to key
				let overflow_data = read_overflow(overflow)?;
				key_data.extend_from_slice(&overflow_data);

				overflow
			} else {
				0
			};

			// Validate reconstructed key size
			if key_data.len() != key_len_total {
				return Err(BPlusTreeError::Deserialization(format!(
					"Reconstructed key size {} doesn't match expected {}",
					key_data.len(),
					key_len_total
				)));
			}

			keys.push(key_data);
			key_overflows.push(overflow_offset);
		}

		let num_children = u32::from_be_bytes(buffer_slice[..4].try_into().unwrap()) as usize;
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
			children.push(u64::from_be_bytes(buffer_slice[..8].try_into().unwrap()));
			buffer_slice = &buffer_slice[8..];
		}

		Ok(InternalNode {
			keys,
			key_overflows,
			children,
			offset,
		})
	}

	// Find the appropriate child index for a key
	fn find_child_index(&self, key: &[u8], compare: &dyn Comparator) -> usize {
		let mut low = 0;
		let mut high = self.keys.len();

		while low < high {
			let mid = low + (high - low) / 2;
			match compare.compare(key, &self.keys[mid]) {
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

	/// Atomically removes a key and its overflow metadata at the given index.
	/// Returns (key, overflow_offset) or None if index is out of bounds.
	fn remove_key_with_overflow(&mut self, idx: usize) -> Option<(Key, u64)> {
		if idx >= self.keys.len() {
			return None;
		}
		let key = self.keys.remove(idx);
		let overflow = if idx < self.key_overflows.len() {
			self.key_overflows.remove(idx)
		} else {
			0
		};
		Some((key, overflow))
	}

	/// Atomically extracts and returns the key+overflow at idx from parent.
	/// This is used when a parent key needs to be moved into a child node.
	fn extract_parent_key(parent: &mut InternalNode, idx: usize) -> (Key, u64) {
		let key = parent.keys[idx].clone();
		let overflow = parent.get_overflow_at(idx);
		(key, overflow)
	}

	/// Insert a key and child pointer with explicit overflow metadata
	fn insert_key_child_with_overflow(
		&mut self,
		key: Key,
		overflow: u64,
		child_offset: u64,
		compare: &dyn Comparator,
	) {
		let mut idx = 0;
		while idx < self.keys.len() && compare.compare(&key, &self.keys[idx]) == Ordering::Greater {
			idx += 1;
		}
		self.keys.insert(idx, key);
		self.key_overflows.insert(idx, overflow); // Use provided overflow
		self.children.insert(idx + 1, child_offset);
	}

	/// Safely gets the overflow offset for a key at the given index
	fn get_overflow_at(&self, idx: usize) -> u64 {
		if idx < self.key_overflows.len() {
			self.key_overflows[idx]
		} else {
			0
		}
	}

	/// Sets the overflow offset for a key at the given index
	fn set_overflow_at(&mut self, idx: usize, overflow: u64) {
		// Ensure vector is large enough
		while self.key_overflows.len() <= idx {
			self.key_overflows.push(0);
		}
		self.key_overflows[idx] = overflow;
	}

	/// Extracts key+overflow for transferring to another node (removes from
	/// this node)
	fn extract_key_with_overflow(&mut self, idx: usize) -> Option<(Key, u64)> {
		if idx >= self.keys.len() {
			return None;
		}
		let key = self.keys.remove(idx);
		let overflow = if idx < self.key_overflows.len() {
			self.key_overflows.remove(idx)
		} else {
			0
		};
		Some((key, overflow))
	}

	/// Inserts a key with its overflow at a specific position
	fn insert_key_with_overflow(&mut self, idx: usize, key: Key, overflow: u64) {
		self.keys.insert(idx, key);
		self.key_overflows.insert(idx, overflow);
	}

	fn redistribute_to_right(
		&mut self,
		right: &mut InternalNode,
		parent_key: Key,
		parent_overflow: u64,
	) -> (Key, u64) {
		right.insert_key_with_overflow(0, parent_key, parent_overflow);

		let (new_parent_key, new_parent_overflow) =
			self.extract_key_with_overflow(self.keys.len() - 1).unwrap();

		if !self.children.is_empty() {
			right.children.insert(0, self.children.pop().unwrap());
		}

		(new_parent_key, new_parent_overflow)
	}

	fn take_from_right(
		&mut self,
		right: &mut InternalNode,
		parent_key: Key,
		parent_overflow: u64,
	) -> (Key, u64) {
		self.keys.push(parent_key);
		self.key_overflows.push(parent_overflow);

		let (new_parent_key, new_parent_overflow) = right.extract_key_with_overflow(0).unwrap();

		if !right.children.is_empty() {
			self.children.push(right.children.remove(0));
		}

		(new_parent_key, new_parent_overflow)
	}

	fn merge_from_right(
		&mut self,
		mut right: InternalNode,
		separator: Key,
		separator_overflow: u64,
	) {
		self.keys.push(separator);
		self.key_overflows.push(separator_overflow);

		self.keys.append(&mut right.keys);
		self.key_overflows.append(&mut right.key_overflows);
		self.children.append(&mut right.children);
	}

	// Check if node is considered to be in underflow state
	fn is_underflow(&self) -> bool {
		self.current_size() * 100 < Self::max_size() * 50
	}

	fn calculate_size_and_max_key(&self) -> (usize, usize) {
		let (min_local, max_local) = calculate_local_limits(false);
		let usable_size = INTERNAL_USABLE_SIZE;
		let mut size = INTERNAL_HEADER_SIZE + self.children.len() * CHILD_PTR_SIZE;
		let mut max_key_len = 0;

		for key in &self.keys {
			let key_len = key.len();
			max_key_len = max_key_len.max(key_len);

			let (key_bytes_on_page, needs_overflow) =
				calculate_overflow(key_len, min_local, max_local, usable_size);
			size += KEY_SIZE_PREFIX + key_bytes_on_page + overflow_ptr_size(needs_overflow);
		}

		(size, max_key_len)
	}
}

impl Node for InternalNode {
	fn serialize(&self) -> Result<Vec<u8>> {
		let mut buffer = Vec::with_capacity(PAGE_SIZE);

		// 1. Node type (1 byte)
		buffer.push(NODE_TYPE_INTERNAL);

		// 2. Number of keys (4 bytes)
		buffer.extend_from_slice(&(self.keys.len() as u32).to_be_bytes());

		let (min_local, max_local) = calculate_local_limits(false);
		let usable_size = INTERNAL_USABLE_SIZE;

		// 3. Serialize keys with overflow support
		for (i, key) in self.keys.iter().enumerate() {
			let key_len_total = key.len();

			// Determine how much to store on page
			let (bytes_on_page, needs_overflow) =
				calculate_overflow(key_len_total, min_local, max_local, usable_size);

			// Write total key length
			buffer.extend_from_slice(&(key_len_total as u32).to_be_bytes());

			// Write key data that fits on page
			buffer.extend_from_slice(&key[..bytes_on_page]);

			// Write overflow page offset if needed (only if there's overflow)
			if needs_overflow {
				let overflow_offset = self.get_overflow_at(i);
				buffer.extend_from_slice(&overflow_offset.to_be_bytes());
			}
		}

		// 4. Number of children (4 bytes)
		buffer.extend_from_slice(&(self.children.len() as u32).to_be_bytes());

		// 5. Child pointers (8 bytes each)
		for &child in &self.children {
			buffer.extend_from_slice(&child.to_be_bytes());
		}

		// 6. Calculate total space used
		let total_used = buffer.len();

		// 7. Validate size before padding
		if total_used > PAGE_SIZE {
			return Err(BPlusTreeError::Serialization(format!(
				"Internal node requires {} bytes (max {})",
				total_used, PAGE_SIZE
			)));
		}

		// 8. Pad to fill available space
		buffer.resize(PAGE_SIZE, 0);
		Ok(buffer)
	}

	fn current_size(&self) -> usize {
		// Base size for internal node header
		let mut size = INTERNAL_HEADER_SIZE;

		let (min_local, max_local) = calculate_local_limits(false);
		let usable_size = INTERNAL_USABLE_SIZE;

		// Size for all keys accounting for overflow
		for key in &self.keys {
			let (key_bytes_on_page, needs_overflow) =
				calculate_overflow(key.len(), min_local, max_local, usable_size);
			size += KEY_SIZE_PREFIX + key_bytes_on_page;
			size += overflow_ptr_size(needs_overflow);
		}

		// Size for all child pointers
		size += self.children.len() * CHILD_PTR_SIZE;

		size
	}

	fn can_merge_with(&self, other: &Self) -> bool {
		let (self_size, self_max_key) = self.calculate_size_and_max_key();
		let (other_size, other_max_key) = other.calculate_size_and_max_key();

		let combined_size = self_size + other_size;
		let actual_merged_size = combined_size - INTERNAL_HEADER_SIZE;
		let max_key_size = self_max_key.max(other_max_key);

		let (min_local, max_local) = calculate_local_limits(false);
		let usable_size = INTERNAL_USABLE_SIZE;
		let (separator_on_page, separator_overflow) =
			calculate_overflow(max_key_size, min_local, max_local, usable_size);
		let separator_size =
			KEY_SIZE_PREFIX + separator_on_page + overflow_ptr_size(separator_overflow);

		let with_separator = actual_merged_size + separator_size;

		with_separator <= Self::max_size()
	}
}

#[derive(Debug, Clone)]
/// Leaf node in the B+ tree
///
/// OVERFLOW OWNERSHIP INVARIANT:
/// - cell_overflows contains overflow chains for the key+value pairs stored in THIS leaf
/// - Each leaf node owns the overflows for its cells
/// - Overflows are only freed when this leaf node is freed via free_node_with_overflow()
/// - When cells are moved between leaves (redistribution), overflow ownership transfers with them
struct LeafNode {
	keys: Vec<Key>,     // Full keys (reconstructed from page + overflow if needed)
	values: Vec<Value>, // Full values (reconstructed from page + overflow if needed)
	cell_overflows: Vec<u64>, /* 0 if no overflow, else first overflow page offset for cell
	                     * (key+value) */
	next_leaf: u64, // 0 means no next leaf
	prev_leaf: u64, // 0 means no previous leaf
	offset: u64,
}

impl LeafNode {
	fn new(offset: u64) -> Self {
		LeafNode {
			keys: Vec::new(),
			values: Vec::new(),
			cell_overflows: Vec::new(),
			next_leaf: 0,
			prev_leaf: 0,
			offset,
		}
	}

	fn deserialize<F>(buffer: &[u8], offset: u64, read_overflow: &F) -> Result<Self>
	where
		F: Fn(u64) -> Result<Vec<u8>>,
	{
		if buffer.len() != PAGE_SIZE {
			return Err(BPlusTreeError::Deserialization(format!(
				"Invalid node size {} (expected {})",
				buffer.len(),
				PAGE_SIZE
			)));
		}

		// Convert buffer to Bytes once for zero-copy slicing
		let buffer_bytes = buffer;
		let mut pos = 1; // Skip node type

		let num_keys = u32::from_be_bytes(buffer_bytes[pos..pos + 4].try_into().unwrap()) as usize;
		pos += 4;

		// Next and prev leaf pointers
		let next_leaf = u64::from_be_bytes(buffer_bytes[pos..pos + 8].try_into().unwrap());
		pos += 8;
		let prev_leaf = u64::from_be_bytes(buffer_bytes[pos..pos + 8].try_into().unwrap());
		pos += 8;

		let (min_local, max_local) = calculate_local_limits(true);
		let usable_size = LEAF_USABLE_SIZE;

		let mut keys = Vec::with_capacity(num_keys);
		let mut values = Vec::with_capacity(num_keys);
		let mut cell_overflows = Vec::with_capacity(num_keys);

		for _ in 0..num_keys {
			// Read key length
			if pos + 4 > buffer_bytes.len() {
				return Err(BPlusTreeError::Deserialization("Truncated key length".into()));
			}
			let key_len =
				u32::from_be_bytes(buffer_bytes[pos..pos + 4].try_into().unwrap()) as usize;
			pos += 4;

			// Read value length
			if pos + 4 > buffer_bytes.len() {
				return Err(BPlusTreeError::Deserialization("Truncated value length".into()));
			}
			let value_len =
				u32::from_be_bytes(buffer_bytes[pos..pos + 4].try_into().unwrap()) as usize;
			pos += 4;

			// Calculate combined payload size and overflow
			let payload_len = key_len + value_len;
			let (bytes_on_page, needs_overflow) =
				calculate_overflow(payload_len, min_local, max_local, usable_size);

			if pos + bytes_on_page > buffer_bytes.len() {
				return Err(BPlusTreeError::Deserialization(format!(
					"Calculated bytes on page {} exceeds available buffer size {}",
					bytes_on_page,
					buffer_bytes.len() - pos
				)));
			}

			// Check if there's overflow
			let overflow_offset = if needs_overflow {
				// Read cell payload from page
				let cell_start = pos;
				pos += bytes_on_page;

				// Read overflow page offset
				if pos + 8 > buffer_bytes.len() {
					return Err(BPlusTreeError::Deserialization(
						"Buffer too small for cell overflow pointer".into(),
					));
				}
				let overflow = u64::from_be_bytes(buffer_bytes[pos..pos + 8].try_into().unwrap());
				pos += 8;

				// For overflow case, we need to allocate and combine
				let mut cell_data = Vec::with_capacity(payload_len);
				cell_data.extend_from_slice(&buffer_bytes[cell_start..cell_start + bytes_on_page]);
				let overflow_data = read_overflow(overflow)?;
				cell_data.extend_from_slice(&overflow_data);

				// Validate reconstructed cell size
				if cell_data.len() != payload_len {
					return Err(BPlusTreeError::Deserialization(format!(
						"Reconstructed cell size {} doesn't match expected {}",
						cell_data.len(),
						payload_len
					)));
				}

				// Split cell_data into key and value
				if cell_data.len() < key_len {
					return Err(BPlusTreeError::Deserialization(format!(
						"Cell data too small {} for key length {}",
						cell_data.len(),
						key_len
					)));
				}
				let key_data = cell_data[..key_len].to_vec();
				let value_data = cell_data[key_len..].to_vec();

				keys.push(key_data);
				values.push(value_data);

				overflow
			} else {
				// No overflow - use zero-copy slicing directly from buffer
				let cell_start = pos;
				let cell_end = pos + bytes_on_page;
				pos = cell_end;

				// Validate cell size
				if bytes_on_page != payload_len {
					return Err(BPlusTreeError::Deserialization(format!(
						"Cell size {} doesn't match expected payload {}",
						bytes_on_page, payload_len
					)));
				}

				// Zero-copy slice for key and value
				let key_data = buffer_bytes[cell_start..cell_start + key_len].to_vec();
				let value_data = buffer_bytes[cell_start + key_len..cell_end].to_vec();

				keys.push(key_data);
				values.push(value_data);

				0
			};

			cell_overflows.push(overflow_offset);
		}

		Ok(LeafNode {
			keys,
			values,
			cell_overflows,
			next_leaf,
			prev_leaf,
			offset,
		})
	}

	// insert a key-value pair into a leaf node at the correct position
	// If key already exists, update the value (treat as update)
	// Returns (index, old_overflow) where old_overflow is Some(offset) if key was
	// updated, None if new
	fn insert(&mut self, key: Key, value: Value, compare: &dyn Comparator) -> (usize, Option<u64>) {
		match self.keys.binary_search_by(|k| compare.compare(k, &key)) {
			Ok(idx) => {
				// Key exists - update
				let old_overflow = self.get_overflow_at(idx);
				self.values[idx] = value;
				self.set_overflow_at(idx, 0);
				(
					idx,
					if old_overflow != 0 {
						Some(old_overflow)
					} else {
						None
					},
				)
			}
			Err(idx) => {
				// Key not found - insert
				self.insert_cell_with_overflow(idx, key, value, 0);
				(idx, None)
			}
		}
	}

	// delete a key-value pair from a leaf node
	// Returns (index, value, overflow_offset) so caller can free the overflow chain
	fn delete(&mut self, key: &[u8], compare: &dyn Comparator) -> Option<(usize, Value, u64)> {
		let idx = self.keys.binary_search_by(|k| compare.compare(k, key)).ok()?;
		let value = self.values.remove(idx);
		self.keys.remove(idx);
		// Remove overflow entry and return it so caller can free it
		let overflow = if idx < self.cell_overflows.len() {
			self.cell_overflows.remove(idx)
		} else {
			0
		};
		Some((idx, value, overflow))
	}

	// Find a key's position in the leaf
	fn find_key(&self, key: &[u8], compare: &dyn Comparator) -> Option<usize> {
		self.keys.binary_search_by(|k| compare.compare(k, key)).ok()
	}

	/// Safely gets the overflow offset for a cell at the given index
	fn get_overflow_at(&self, idx: usize) -> u64 {
		if idx < self.cell_overflows.len() {
			self.cell_overflows[idx]
		} else {
			0
		}
	}

	/// Sets the overflow offset for a cell at the given index
	fn set_overflow_at(&mut self, idx: usize, overflow: u64) {
		// Ensure vector is large enough
		while self.cell_overflows.len() <= idx {
			self.cell_overflows.push(0);
		}
		self.cell_overflows[idx] = overflow;
	}

	/// Inserts a cell (key+value) with overflow at a specific position
	fn insert_cell_with_overflow(&mut self, idx: usize, key: Key, value: Value, overflow: u64) {
		self.keys.insert(idx, key);
		self.values.insert(idx, value);
		self.cell_overflows.insert(idx, overflow);
	}

	// Redistributes keys from this leaf to the target leaf
	fn redistribute_to_right(&mut self, right: &mut LeafNode) -> Key {
		// Move last key-value pair from this node to right node
		let last_key = self.keys.pop().unwrap();
		let last_value = self.values.pop().unwrap();
		let last_overflow = self.cell_overflows.pop().unwrap_or(0);

		let separator = last_key.clone();
		right.keys.insert(0, last_key);
		right.values.insert(0, last_value);
		right.cell_overflows.insert(0, last_overflow);

		separator
	}

	// Takes keys from right leaf
	fn take_from_right(&mut self, right: &mut LeafNode) -> Key {
		// Move first key-value pair from right node to this node
		let first_key = right.keys.remove(0);
		let first_value = right.values.remove(0);
		let first_overflow = if !right.cell_overflows.is_empty() {
			right.cell_overflows.remove(0)
		} else {
			0
		};

		self.keys.push(first_key);
		self.values.push(first_value);
		self.cell_overflows.push(first_overflow);

		// Return new separator (first key of right if it exists)
		if !right.keys.is_empty() {
			right.keys[0].clone()
		} else {
			// Right is now empty, use the current last key
			// This shouldn't happen - right should not become completely empty during
			// rebalancing
			self.keys.last().expect(
				"take_from_right: left node should have at least one key after taking from right"
			).clone()
		}
	}

	fn merge_from_right(&mut self, mut right: LeafNode) {
		self.keys.append(&mut right.keys);
		self.values.append(&mut right.values);
		self.cell_overflows.append(&mut right.cell_overflows);

		self.next_leaf = right.next_leaf;
	}

	/// Find optimal split point that ensures both resulting leaves fit in a
	/// page. Uses hybrid approach: simple split for common cases, iterative
	/// algorithm when needed. Takes the new entry (key, value) and its insert
	/// position into account.
	///
	/// - `is_update`: If true, we're updating an existing entry (replacing, not adding). The total
	///   entry count remains the same. If false, we're inserting a new entry.
	fn find_split_point_for_insert(
		&self,
		new_key: &[u8],
		new_value: &[u8],
		insert_idx: usize,
		is_update: bool,
	) -> usize {
		debug_assert!(!self.keys.is_empty(), "Cannot split empty leaf");

		let (_min_local, max_local) = calculate_local_limits(true);
		let new_payload = new_key.len() + new_value.len();

		// Check if new entry needs overflow
		let new_needs_overflow = new_payload > max_local;

		// Only use expensive algorithm if NEW entry needs overflow
		// For existing overflow entries, the fast path is usually sufficient
		// This avoids O(n) iteration through all entries on every split
		let needs_complex_split = new_needs_overflow;

		if !needs_complex_split {
			// Fast path: Simple split (like old algorithm)
			let total = if is_update {
				self.keys.len() // For updates, total stays the same
			} else {
				self.keys.len() + 1 // For inserts, we add one entry
			};
			let split_idx_expanded = if insert_idx < total / 2 {
				total / 2 - 1
			} else {
				total / 2
			};

			// Convert from expanded array index to original array index (for inserts only)
			// Simple rule: if NEW is before split point, subtract 1; otherwise use as-is
			if is_update {
				// For updates, split_idx directly maps to original array index
				split_idx_expanded
			} else {
				// For inserts: convert expanded index to original index
				// If NEW is before split point, split_idx_expanded counts NEW, so subtract 1
				// If NEW is at or after split point, split_idx_expanded doesn't count NEW yet
				let split_idx = if insert_idx < split_idx_expanded {
					split_idx_expanded - 1
				} else {
					split_idx_expanded
				};
				// Clamp to valid range [0, keys.len()]
				split_idx.min(self.keys.len())
			}
		} else {
			// Slow path: Complex iterative algorithm only when necessary
			self.find_split_point_for_insert_complex(new_key, new_value, insert_idx, is_update)
		}
	}

	/// Ensures proper ordering: right page size <= left page size (cntNew[i] <=
	/// cntNew[i-1])
	fn find_split_point_for_insert_complex(
		&self,
		new_key: &[u8],
		new_value: &[u8],
		insert_idx: usize,
		is_update: bool,
	) -> usize {
		let max_page_size = PAGE_SIZE;

		// Phase 1: Calculate actual cell sizes for all entries including the new one
		let capacity = if is_update {
			self.keys.len() // For updates, total stays the same
		} else {
			self.keys.len() + 1 // For inserts, we add one entry
		};
		let mut cell_sizes = Vec::with_capacity(capacity);

		// Build cell sizes array with new entry inserted/replaced at correct position
		for (i, (key, value)) in self.keys.iter().zip(&self.values).enumerate() {
			if i == insert_idx {
				// Add new entry size at this position
				let size = leaf_entry_size(new_key, new_value);
				cell_sizes.push(size);

				// For updates, skip the old entry (we're replacing it, not adding)
				// For inserts, continue to add the old entry after the new one
				if is_update {
					continue; // Skip adding the old entry
				}
			}

			let size = leaf_entry_size(key, value);
			cell_sizes.push(size);
		}

		// Handle case where insert_idx is at the end (only for inserts, not updates)
		if !is_update && insert_idx >= self.keys.len() {
			let size = leaf_entry_size(new_key, new_value);
			cell_sizes.push(size);
		}

		let total_cells = cell_sizes.len();

		// Phase 2: Sequential left-to-right processing (no recursion)
		// Process entries sequentially from left to right, stopping at first valid
		// split point
		let mut current_page_size = LEAF_HEADER_SIZE;
		let mut split_idx = 0;

		// Process entries sequentially from left to right
		for (i, &cell_size) in cell_sizes.iter().enumerate().take(total_cells) {
			let new_page_size = current_page_size + cell_size;

			// If adding this cell would exceed page size, split before it
			// But ensure we have at least one cell on the left page
			if new_page_size > max_page_size && current_page_size > LEAF_HEADER_SIZE {
				split_idx = i;
				break;
			}

			current_page_size = new_page_size;
		}

		// If we processed all cells without finding a split point, use the last valid
		// position This shouldn't happen if we're splitting, but handle it gracefully
		if split_idx == 0 && current_page_size <= max_page_size {
			// All entries fit in one page - this shouldn't happen if we're splitting
			// Use middle split as fallback
			split_idx = total_cells / 2;
			if split_idx == 0 {
				split_idx = 1; // Ensure at least one cell on left
			}
		} else if split_idx == 0 {
			// First cell itself is too large - split after it
			split_idx = 1;
		}

		// Calculate sizes for both pages
		let mut sz_left = LEAF_HEADER_SIZE + cell_sizes[..split_idx].iter().sum::<usize>();
		let mut sz_right = LEAF_HEADER_SIZE + cell_sizes[split_idx..].iter().sum::<usize>();

		// Phase 3: Ordering check and adjustment
		// Ensure cntNew[i] <= cntNew[i-1] (right page <= left page in size)
		// This ensures proper ordering across pages
		// If right is larger than left, move cells from right to left (if possible)
		// To move a cell from right to left, we increase split_idx (which moves the
		// boundary right)
		while sz_right > sz_left && split_idx < total_cells {
			// Try moving the leftmost cell from right page to left page
			let cell_to_move = cell_sizes[split_idx];
			let new_left = sz_left + cell_to_move;
			let new_right = sz_right - cell_to_move;

			// Only move if both pages still fit and it improves ordering
			if new_left <= max_page_size && new_right <= max_page_size {
				sz_left = new_left;
				sz_right = new_right;
				split_idx += 1;
			} else {
				// Can't improve further without violating size constraints
				break;
			}
		}

		// Verify both pages fit (should always be true after sequential processing)
		if sz_left > max_page_size || sz_right > max_page_size {
			panic!(
				"find_split_point_for_insert_complex: pages don't fit after sequential processing (split_idx={}, total_cells={}, sz_left={}, sz_right={}, max_page_size={})",
				split_idx, total_cells, sz_left, sz_right, max_page_size
			);
		}

		// Convert back to original key index
		// For updates: split_idx directly maps to original array (same size)
		// For inserts: split_idx is in expanded array (with new entry), needs
		// conversion

		if is_update {
			// For updates, split_idx directly maps to original array index
			split_idx
		} else if insert_idx < split_idx {
			// New entry is on left side of split
			// In expanded array: [entry0, ..., NEW_ENTRY, entry(insert_idx), ...]
			// If split_idx=1: left=[NEW_ENTRY], right=[entry0, entry1, ...]
			// In original array, this means split at index 0 (before entry0)
			// But we can't use 0 because split_off(0) would put everything on right!
			// Instead, we need to ensure at least the new entry goes left.
			// If split_idx=1, we want: left gets new_entry, right gets all original entries
			// So we return 0, but the split logic must handle this specially
			if split_idx == 1 {
				// Special case: new entry alone on left, all original entries on right
				// Return 0, but the caller must insert new entry into left before splitting
				0
			} else if split_idx == insert_idx + 1 {
				// Edge case: split_idx(expanded) == insert_idx + 1
				// In expanded: left=[..., NEW_ENTRY], right=[entry(insert_idx), ...]
				// Algorithm calculated sz_left for left=[..., NEW_ENTRY] (excluding
				// entry(insert_idx)) To preserve size invariants, we must return insert_idx
				// (not insert_idx + 1) This ensures left gets [..., NEW_ENTRY] without
				// entry(insert_idx)
				insert_idx
			} else {
				// split_idx > insert_idx + 1: some original entries also go left
				// split_idx=3, insert_idx=1 means: left=[entry0, NEW_ENTRY, entry1],
				// right=[entry2, ...] In original array, this is split_idx=2 (before
				// entry2)
				split_idx - 1
			}
		} else {
			// New entry is on right side or at split point
			// split_idx in expanded array directly maps to original array
			split_idx
		}
	}

	// Check if this leaf can fit another key-value pair
	fn can_fit_entry(&self, key: &[u8], value: &[u8]) -> bool {
		let (min_local, max_local) = calculate_local_limits(true);
		let usable_size = LEAF_USABLE_SIZE;
		let payload_len = key.len() + value.len();
		let (bytes_on_page, needs_overflow) =
			calculate_overflow(payload_len, min_local, max_local, usable_size);

		let entry_size =
			KEY_SIZE_PREFIX + VALUE_SIZE_PREFIX + bytes_on_page + overflow_ptr_size(needs_overflow);

		self.current_size() + entry_size <= PAGE_SIZE
	}

	// Check if node is considered to be in underflow state
	fn is_underflow(&self) -> bool {
		self.current_size() * 100 < Self::max_size() * 50
	}
}

impl Node for LeafNode {
	fn serialize(&self) -> Result<Vec<u8>> {
		let mut buffer = Vec::with_capacity(PAGE_SIZE);

		// 1. Node type (1 byte)
		buffer.push(NODE_TYPE_LEAF);

		// 2. Number of keys (4 bytes)
		buffer.extend_from_slice(&(self.keys.len() as u32).to_be_bytes());

		// 3. Next leaf pointer (8 bytes)
		buffer.extend_from_slice(&self.next_leaf.to_be_bytes());

		// 4. Previous leaf pointer (8 bytes)
		buffer.extend_from_slice(&self.prev_leaf.to_be_bytes());

		let (min_local, max_local) = calculate_local_limits(true);
		let usable_size = LEAF_USABLE_SIZE;

		// 5. Serialize cells (key+value pairs) with overflow support
		for (i, (key, value)) in self.keys.iter().zip(&self.values).enumerate() {
			let key_len = key.len();
			let value_len = value.len();
			let payload_len = key_len + value_len;

			// Write key and value lengths
			buffer.extend_from_slice(&(key_len as u32).to_be_bytes());
			buffer.extend_from_slice(&(value_len as u32).to_be_bytes());

			// Calculate overflow for combined payload
			let (bytes_on_page, needs_overflow) =
				calculate_overflow(payload_len, min_local, max_local, usable_size);

			// Create combined cell data
			let mut cell_data = Vec::with_capacity(payload_len);
			cell_data.extend_from_slice(key.as_ref());
			cell_data.extend_from_slice(value.as_ref());

			// Write cell data that fits on page
			buffer.extend_from_slice(&cell_data[..bytes_on_page]);

			// Write cell overflow page offset if needed
			if needs_overflow {
				let overflow_offset = self.get_overflow_at(i);
				buffer.extend_from_slice(&overflow_offset.to_be_bytes());
			}
		}

		// 6. Calculate total space used
		let total_used = buffer.len();

		// 7. Validate size before padding
		if total_used > PAGE_SIZE {
			return Err(BPlusTreeError::Serialization(format!(
				"Leaf node requires {} bytes (max {})",
				total_used, PAGE_SIZE
			)));
		}

		// 8. Pad to fill available space
		buffer.resize(PAGE_SIZE, 0);
		Ok(buffer)
	}

	fn current_size(&self) -> usize {
		// Base size for leaf node header
		let mut size = LEAF_HEADER_SIZE;

		let (min_local, max_local) = calculate_local_limits(true);
		let usable_size = LEAF_USABLE_SIZE;

		// Size for all cells accounting for overflow
		for (key, value) in self.keys.iter().zip(&self.values) {
			let payload_len = key.len() + value.len();
			let (bytes_on_page, needs_overflow) =
				calculate_overflow(payload_len, min_local, max_local, usable_size);

			// Key and value length prefixes
			size += KEY_SIZE_PREFIX + VALUE_SIZE_PREFIX;
			// Cell payload on page
			size += bytes_on_page;
			// Overflow pointer if needed
			if needs_overflow {
				size += 8;
			}
		}

		size
	}

	fn can_merge_with(&self, other: &Self) -> bool {
		let combined_size = self.current_size() + other.current_size();
		let actual_merged_size = combined_size - LEAF_HEADER_SIZE;

		actual_merged_size <= Self::max_size()
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
	fn new(offset: u64) -> Self {
		TrunkPage {
			next_trunk: 0,
			num_free_pages: 0,
			free_pages: Vec::with_capacity(TRUNK_PAGE_MAX_ENTRIES),
			offset,
		}
	}

	fn is_full(&self) -> bool {
		self.free_pages.len() >= TRUNK_PAGE_MAX_ENTRIES
	}

	fn is_empty(&self) -> bool {
		self.free_pages.is_empty()
	}

	fn add_free_page(&mut self, page_offset: u64) -> bool {
		if self.is_full() {
			return false;
		}

		// Convert page offset to page number (divide by page size)
		let page_number = (page_offset / PAGE_SIZE as u64) as u32;
		self.free_pages.push(page_number);
		self.num_free_pages += 1;
		true
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
		let mut buffer = Vec::with_capacity(PAGE_SIZE);

		// 1. Page type (1 byte)
		buffer.push(TRUNK_PAGE_TYPE);

		// 2. Next trunk pointer (8 bytes)
		buffer.extend_from_slice(&self.next_trunk.to_be_bytes());

		// 3. Number of free pages (4 bytes)
		buffer.extend_from_slice(&self.num_free_pages.to_be_bytes());

		// 4. Free page numbers (4 bytes each, big-endian to match the rest of the code)
		for &page in &self.free_pages {
			buffer.extend_from_slice(&page.to_be_bytes());
		}

		// 5. Pad to fill available space
		let available_space = PAGE_SIZE;
		if buffer.len() > available_space {
			return Err(BPlusTreeError::Serialization("Trunk page overflow".into()));
		}
		buffer.resize(available_space, 0);

		Ok(buffer)
	}

	fn deserialize(buffer: &[u8], offset: u64) -> Result<Self> {
		if buffer.len() != PAGE_SIZE {
			return Err(BPlusTreeError::Deserialization("Invalid trunk page size".into()));
		}

		if buffer[0] != TRUNK_PAGE_TYPE {
			return Err(BPlusTreeError::Deserialization("Not a trunk page".into()));
		}

		// Read next trunk pointer (8 bytes)
		let next_trunk = u64::from_be_bytes(buffer[1..9].try_into().unwrap());

		// Read number of free pages (4 bytes)
		let num_free_pages = u32::from_be_bytes(buffer[9..13].try_into().unwrap()) as usize;

		// Check if num_free_pages is reasonable
		if num_free_pages > TRUNK_PAGE_MAX_ENTRIES {
			return Err(BPlusTreeError::Deserialization(format!(
				"Invalid number of free pages in trunk: {} (max {})",
				num_free_pages, TRUNK_PAGE_MAX_ENTRIES
			)));
		}

		// Read free page numbers (4 bytes each, big-endian)
		let mut free_pages = Vec::with_capacity(num_free_pages);
		for i in 0..num_free_pages {
			let start = TRUNK_PAGE_HEADER_SIZE + i * 4;
			let end = start + 4;
			if end > buffer.len() {
				return Err(BPlusTreeError::Deserialization("Truncated trunk page data".into()));
			}
			free_pages.push(u32::from_be_bytes(buffer[start..end].try_into().unwrap()));
		}

		Ok(TrunkPage {
			next_trunk,
			num_free_pages: num_free_pages as u32,
			free_pages,
			offset,
		})
	}
}

// Overflow page structure for storing large keys/values that don't fit on a
// single page
#[derive(Debug, Clone)]
struct OverflowPage {
	next_overflow: u64, // 0 means last in chain
	data: Vec<u8>,      // Payload data
	offset: u64,        // Offset of this overflow page
}

impl OverflowPage {
	fn new(offset: u64) -> Self {
		OverflowPage {
			next_overflow: 0,
			data: Vec::new(),
			offset,
		}
	}

	fn serialize(&self) -> Result<Vec<u8>> {
		let mut buffer = Vec::with_capacity(PAGE_SIZE);

		// 1. Page type (1 byte)
		buffer.push(NODE_TYPE_OVERFLOW);

		// 2. Next overflow pointer (8 bytes)
		buffer.extend_from_slice(&self.next_overflow.to_be_bytes());

		// 3. Data length (4 bytes)
		buffer.extend_from_slice(&(self.data.len() as u32).to_be_bytes());

		// 4. Data
		buffer.extend_from_slice(&self.data);

		// 5. Validate size
		if buffer.len() > PAGE_SIZE {
			return Err(BPlusTreeError::Serialization(format!(
				"Overflow page data too large: {} bytes (max {})",
				buffer.len(),
				PAGE_SIZE
			)));
		}

		// 6. Pad to fill page
		buffer.resize(PAGE_SIZE, 0);

		Ok(buffer)
	}

	fn deserialize(buffer: &[u8], offset: u64) -> Result<Self> {
		if buffer.len() != PAGE_SIZE {
			return Err(BPlusTreeError::Deserialization(format!(
				"Invalid overflow page size: {} (expected {})",
				buffer.len(),
				PAGE_SIZE
			)));
		}

		if buffer[0] != NODE_TYPE_OVERFLOW {
			return Err(BPlusTreeError::Deserialization("Not an overflow page".into()));
		}

		// Read next overflow pointer (8 bytes)
		let next_overflow = u64::from_be_bytes(buffer[1..9].try_into().unwrap());

		// Read data length (4 bytes)
		let data_len = u32::from_be_bytes(buffer[9..13].try_into().unwrap()) as usize;

		// Validate data length
		if data_len > PAGE_SIZE - 13 {
			return Err(BPlusTreeError::Deserialization(format!(
				"Invalid overflow data length: {}",
				data_len
			)));
		}

		// Read data
		let data = buffer[13..13 + data_len].to_vec();

		Ok(OverflowPage {
			next_overflow,
			data,
			offset,
		})
	}

	// Maximum data that can be stored in a single overflow page
	fn max_data_size() -> usize {
		PAGE_SIZE - 13 // 1 byte type + 8 bytes next_overflow + 4 bytes data_len
	}
}

#[derive(Clone)]
enum NodeType {
	Internal(InternalNode),
	Leaf(LeafNode),
	Overflow(OverflowPage),
}

#[derive(Clone, Copy, PartialEq)]
#[allow(dead_code)]
pub enum Durability {
	/// Sync after every write (safe but slow)
	Always,

	/// Only sync when flush() or close() is called (fast but risks data loss on
	/// crash)
	Manual,
}

/// - maxLocal = (usableSize - 12) * 64 / 255 - 23
/// - minLocal = (usableSize - 12) * 32 / 255 - 23
/// - maxLeaf = usableSize - 35
/// - minLeaf = (usableSize - 12) * 32 / 255 - 23
fn calculate_local_limits_internal(is_leaf: bool) -> (usize, usize) {
	let usable_size = if is_leaf {
		LEAF_USABLE_SIZE
	} else {
		INTERNAL_USABLE_SIZE
	};

	if is_leaf {
		let max_leaf = usable_size.saturating_sub(35);
		let min_leaf = ((usable_size.saturating_sub(12)) * 32 / 255).saturating_sub(23);
		(min_leaf, max_leaf)
	} else {
		let max_local = ((usable_size.saturating_sub(12)) * 64 / 255).saturating_sub(23);
		let min_local = ((usable_size.saturating_sub(12)) * 32 / 255).saturating_sub(23);
		(min_local, max_local)
	}
}

/// Get cached local limits (cached version of calculate_local_limits_internal)
/// This is much faster since the limits are constant for a given PAGE_SIZE
#[inline]
fn calculate_local_limits(is_leaf: bool) -> (usize, usize) {
	if is_leaf {
		*LEAF_LIMITS.get_or_init(|| calculate_local_limits_internal(true))
	} else {
		*INTERNAL_LIMITS.get_or_init(|| calculate_local_limits_internal(false))
	}
}

/// Calculate how much of payload to store on page
/// Returns (bytes_on_page, needs_overflow)
///
/// This minimizes unused space on overflow pages by aligning with overflow page
/// boundaries
fn calculate_overflow(
	payload_size: usize,
	min_local: usize,
	max_local: usize,
	usable_size: usize,
) -> (usize, bool) {
	if payload_size <= max_local {
		(payload_size, false)
	} else {
		// Minimize unused space on overflow pages
		// Overflow page size is usable_size - 4 (4 bytes for next page pointer)
		let overflow_page_size = usable_size.saturating_sub(4);
		let surplus = min_local + (payload_size.saturating_sub(min_local)) % overflow_page_size;
		let bytes_on_page = if surplus <= max_local {
			surplus
		} else {
			min_local
		};
		(bytes_on_page, true)
	}
}

/// Size of an overflow pointer
const OVERFLOW_PTR_SIZE: usize = 8;

/// Size of overflow pointer if needed
#[inline]
const fn overflow_ptr_size(needs_overflow: bool) -> usize {
	if needs_overflow {
		OVERFLOW_PTR_SIZE
	} else {
		0
	}
}

/// Calculate the on-page size for an internal node entry (key + child pointer +
/// overflow)
#[inline]
fn internal_entry_size(key: &[u8]) -> usize {
	let (min_local, max_local) = calculate_local_limits(false);
	let (key_on_page, needs_overflow) =
		calculate_overflow(key.len(), min_local, max_local, INTERNAL_USABLE_SIZE);
	KEY_SIZE_PREFIX + key_on_page + CHILD_PTR_SIZE + overflow_ptr_size(needs_overflow)
}

/// Calculate the on-page size for a leaf node entry (key+value + overflow)
#[inline]
fn leaf_entry_size(key: &[u8], value: &[u8]) -> usize {
	let (min_local, max_local) = calculate_local_limits(true);
	let payload_len = key.len() + value.len();
	let (bytes_on_page, needs_overflow) =
		calculate_overflow(payload_len, min_local, max_local, LEAF_USABLE_SIZE);
	KEY_SIZE_PREFIX + VALUE_SIZE_PREFIX + bytes_on_page + overflow_ptr_size(needs_overflow)
}

pub struct BPlusTree<F: VfsFile> {
	file: F,
	header: Header,
	cache: Cache<u64, Arc<NodeType>, NodeWeighter>,
	compare: Arc<dyn Comparator>,
	durability: Durability,
}

impl<F: VfsFile> Drop for BPlusTree<F> {
	fn drop(&mut self) {
		if let Err(e) = self.close() {
			log::error!("Error during BPlusTree drop: {}", e);
		}
	}
}

impl BPlusTree<File> {
	pub fn disk<P: AsRef<Path>>(path: P, compare: Arc<dyn Comparator>) -> Result<Self> {
		use std::fs::OpenOptions;
		let file =
			OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
		Self::with_file(file, compare)
	}
}

impl<F: VfsFile> BPlusTree<F> {
	pub fn with_file(file: F, compare: Arc<dyn Comparator>) -> Result<Self> {
		let storage_size = file.size()?;

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
			let cache = Cache::with_weighter(
				DEFAULT_CACHE_CAPACITY as usize,
				DEFAULT_CACHE_CAPACITY,
				NodeWeighter,
			);

			(header, cache)
		} else {
			// Read existing header
			let mut buffer = [0u8; 48];
			file.read_at(0, &mut buffer)?;
			let header = Header::deserialize(&buffer)?;

			let cache = Cache::with_weighter(
				DEFAULT_CACHE_CAPACITY as usize,
				DEFAULT_CACHE_CAPACITY,
				NodeWeighter,
			);

			(header, cache)
		};

		let mut tree = BPlusTree {
			file,
			header,
			cache,
			compare,
			durability: Durability::Manual,
		};

		// Initialize storage if it's a new tree
		if storage_size == 0 {
			let header_bytes = tree.header.serialize();
			let mut buffer = vec![0u8; PAGE_SIZE];
			buffer[..header_bytes.len()].copy_from_slice(&header_bytes);

			tree.file.write_at(0, &buffer)?;

			// Create initial root node
			let root = LeafNode::new(tree.header.root_offset);
			tree.write_node(&NodeType::Leaf(root))?;
			tree.file.sync_data()?;
		} else {
			// Read root node into cache
			tree.read_node(tree.header.root_offset)?;
		}

		Ok(tree)
	}

	#[allow(dead_code)]
	pub fn set_durability(&mut self, durability: Durability) {
		self.durability = durability;
	}

	pub fn sync(&mut self) -> Result<()> {
		self.file.sync_data()?;
		Ok(())
	}

	pub fn close(&self) -> Result<()> {
		self.file.sync()?;
		Ok(())
	}

	pub fn flush(&mut self) -> Result<()> {
		// Just to ensure all file data and metadata is synced
		self.sync()?;
		Ok(())
	}

	pub fn insert(&mut self, key: Key, value: Value) -> Result<()> {
		let mut path = Vec::new();
		let mut current_offset = self.header.root_offset;
		let mut parent_offset = None;

		loop {
			let node = self.read_node(current_offset)?;

			match node.as_ref() {
				NodeType::Internal(internal) => {
					path.push((current_offset, parent_offset));

					let child_idx = internal.find_child_index(&key, self.compare.as_ref());
					parent_offset = Some(current_offset);
					current_offset = internal.children[child_idx];
				}
				NodeType::Leaf(_) => {
					// Need to modify the leaf - extract it from Arc
					let mut leaf = self.extract_leaf_mut(node);

					if leaf.can_fit_entry(&key, &value) {
						self.insert_into_leaf(&mut leaf, key, value)?;
						return Ok(());
					} else {
						let (promoted_key, promoted_overflow, new_leaf_offset) =
							self.split_leaf(&mut leaf, key, value)?;

						self.handle_splits(
							parent_offset,
							promoted_key,
							promoted_overflow,
							new_leaf_offset,
							path,
						)?;
						return Ok(());
					}
				}
				NodeType::Overflow(_) => {
					return Err(BPlusTreeError::UnexpectedOverflowPage(current_offset));
				}
			}
		}
	}

	fn handle_splits(
		&mut self,
		mut parent_offset: Option<u64>,
		mut promoted_key: Key,
		mut promoted_overflow: u64,
		mut new_node_offset: u64,
		mut path: Vec<(u64, Option<u64>)>,
	) -> Result<()> {
		loop {
			match parent_offset {
				None => {
					let new_root_offset = self.allocate_page()?;
					let mut new_root = InternalNode::new(new_root_offset);

					new_root.keys.push(promoted_key);
					new_root.key_overflows.push(promoted_overflow);
					new_root.children.push(if path.is_empty() {
						self.header.root_offset
					} else {
						path.last().unwrap().0
					});
					new_root.children.push(new_node_offset);

					self.header.root_offset = new_root_offset;
					self.write_header()?;
					self.write_node(&NodeType::Internal(new_root))?;

					return Ok(());
				}
				Some(offset) => {
					let mut parent = self.read_internal_node(offset)?;

					let (min_local, max_local) = calculate_local_limits(false);
					let (key_on_page, key_overflow) = calculate_overflow(
						promoted_key.len(),
						min_local,
						max_local,
						INTERNAL_USABLE_SIZE,
					);
					let entry_size = KEY_SIZE_PREFIX
						+ key_on_page + CHILD_PTR_SIZE
						+ overflow_ptr_size(key_overflow);
					let would_be_size = parent.current_size() + entry_size;
					let size_threshold = InternalNode::max_size();

					if would_be_size <= size_threshold {
						parent.insert_key_child_with_overflow(
							promoted_key,
							promoted_overflow,
							new_node_offset,
							self.compare.as_ref(),
						);
						self.write_node(&NodeType::Internal(parent))?;

						return Ok(());
					} else {
						let (next_promoted_key, next_promoted_overflow, next_new_node_offset) =
							self.split_internal_with_child(
								&mut parent,
								promoted_key,
								promoted_overflow,
								new_node_offset,
							)?;

						// Path should contain the grandparent when splitting an internal node with
						// a parent
						let (_, next_parent) = path.pop().expect(
							"handle_splits: path should contain grandparent when splitting internal node with parent"
						);

						promoted_key = next_promoted_key;
						promoted_overflow = next_promoted_overflow;
						new_node_offset = next_new_node_offset;
						parent_offset = next_parent;
					}
				}
			}
		}
	}

	fn insert_into_leaf(&mut self, leaf: &mut LeafNode, key: Key, value: Value) -> Result<()> {
		let (_idx, old_overflow) = leaf.insert(key, value, self.compare.as_ref());
		if let Some(overflow) = old_overflow {
			self.free_overflow_chain(overflow)?;
		}
		let leaf_owned = std::mem::replace(leaf, LeafNode::new(leaf.offset));
		self.write_node_owned(NodeType::Leaf(leaf_owned))?;
		Ok(())
	}

	fn split_leaf(
		&mut self,
		leaf: &mut LeafNode,
		key: Key,
		value: Value,
	) -> Result<(Key, u64, u64)> {
		let idx =
			leaf.keys.binary_search_by(|k| self.compare.compare(k, &key)).unwrap_or_else(|idx| idx);

		let is_duplicate =
			idx < leaf.keys.len() && self.compare.compare(&key, &leaf.keys[idx]) == Ordering::Equal;

		// Use size-aware split point that accounts for the new entry
		// Unified function handles both inserts and updates
		let split_idx = leaf.find_split_point_for_insert(&key, &value, idx, is_duplicate);

		let new_leaf_offset = self.allocate_page()?;
		let mut new_leaf = LeafNode::new(new_leaf_offset);

		// Handle special case: split_idx=0 means new entry goes left, all original
		// entries go right
		if split_idx == 0 {
			// Move all original entries to the new leaf
			new_leaf.keys = std::mem::take(&mut leaf.keys);
			new_leaf.values = std::mem::take(&mut leaf.values);
			new_leaf.cell_overflows = std::mem::take(&mut leaf.cell_overflows);

			// Insert new entry into the (now empty) left leaf
			if is_duplicate {
				// If duplicate and split_idx=0, the duplicate must be at idx=0
				// But we've already moved it to new_leaf, so update it there
				if new_leaf.get_overflow_at(0) != 0 {
					self.free_overflow_chain(new_leaf.get_overflow_at(0))?;
				}
				new_leaf.values[0] = value;
				new_leaf.set_overflow_at(0, 0);
			} else {
				leaf.insert_cell_with_overflow(0, key, value, 0);
			}
		} else if idx < split_idx {
			// New entry goes to left leaf
			new_leaf.keys = leaf.keys.split_off(split_idx);
			new_leaf.values = leaf.values.split_off(split_idx);
			new_leaf.cell_overflows = leaf.cell_overflows.split_off(split_idx);

			if is_duplicate {
				if leaf.get_overflow_at(idx) != 0 {
					self.free_overflow_chain(leaf.get_overflow_at(idx))?;
				}
				leaf.values[idx] = value;
				leaf.set_overflow_at(idx, 0);
			} else {
				leaf.insert_cell_with_overflow(idx, key, value, 0);
			}
		} else {
			// New entry goes to right leaf
			let right_idx = idx - split_idx;

			new_leaf.keys = leaf.keys.split_off(split_idx);
			new_leaf.values = leaf.values.split_off(split_idx);
			new_leaf.cell_overflows = leaf.cell_overflows.split_off(split_idx);

			if is_duplicate {
				if new_leaf.get_overflow_at(right_idx) != 0 {
					self.free_overflow_chain(new_leaf.get_overflow_at(right_idx))?;
				}
				new_leaf.values[right_idx] = value;
				new_leaf.set_overflow_at(right_idx, 0);
			} else {
				new_leaf.insert_cell_with_overflow(right_idx, key, value, 0);
			}
		}

		new_leaf.next_leaf = leaf.next_leaf;
		new_leaf.prev_leaf = leaf.offset;
		leaf.next_leaf = new_leaf.offset;

		let promoted_key = new_leaf.keys[0].clone();
		let promoted_overflow = 0;

		let next_leaf_update = if new_leaf.next_leaf != 0 {
			Some(new_leaf.next_leaf)
		} else {
			None
		};

		let new_leaf_offset = new_leaf.offset;

		let leaf_owned = std::mem::replace(leaf, LeafNode::new(leaf.offset));
		self.write_node_owned(NodeType::Leaf(leaf_owned))?;
		self.write_node_owned(NodeType::Leaf(new_leaf))?;

		if let Some(next_offset) = next_leaf_update {
			let next_node = self.read_node(next_offset)?;
			if matches!(next_node.as_ref(), NodeType::Leaf(_)) {
				let mut next_leaf = self.extract_leaf_mut(next_node);
				next_leaf.prev_leaf = new_leaf_offset;
				self.write_node_owned(NodeType::Leaf(next_leaf))?;
			}
		}

		Ok((promoted_key, promoted_overflow, new_leaf_offset))
	}

	fn split_internal_with_child(
		&mut self,
		node: &mut InternalNode,
		extra_key: Key,
		extra_overflow: u64,
		extra_child: u64,
	) -> Result<(Key, u64, u64)> {
		let insert_idx = node
			.keys
			.binary_search_by(|key| self.compare.compare(key, &extra_key))
			.unwrap_or_else(|idx| idx);

		let mut split_idx = Self::find_split_point(node, insert_idx);

		split_idx = split_idx.min(node.keys.len() - 1);

		let new_node_offset = self.allocate_page()?;
		let mut new_node = InternalNode::new(new_node_offset);

		let promoted_key = node.keys[split_idx].clone();
		let promoted_overflow = node.get_overflow_at(split_idx);

		new_node.keys = node.keys.split_off(split_idx + 1);
		new_node.key_overflows = node.key_overflows.split_off(split_idx + 1);
		new_node.children = node.children.split_off(split_idx + 1);

		node.keys.truncate(split_idx);
		node.key_overflows.truncate(split_idx);

		if insert_idx <= split_idx {
			node.insert_key_child_with_overflow(
				extra_key,
				extra_overflow,
				extra_child,
				self.compare.as_ref(),
			);
		} else {
			let right_insert_idx = insert_idx - split_idx - 1;
			new_node.keys.insert(right_insert_idx, extra_key);
			new_node.key_overflows.insert(right_insert_idx, extra_overflow);
			new_node.children.insert(right_insert_idx + 1, extra_child);
		}

		let node_owned = std::mem::replace(node, InternalNode::new(node.offset));
		self.write_node_owned(NodeType::Internal(node_owned))?;
		self.write_node_owned(NodeType::Internal(new_node))?;

		Ok((promoted_key, promoted_overflow, new_node_offset))
	}

	/// Processes entries left-to-right, ensures ordering (right <= left)
	fn find_split_point(node: &InternalNode, insert_idx: usize) -> usize {
		debug_assert!(!node.keys.is_empty(), "Internal node must have at least 1 key to split");

		// Simple sequential approach: calculate split point based on total keys
		// Internal node keys are typically similar in size, so simple middle split
		// works well
		let total_keys = node.keys.len() + 1; // +1 for the new key being inserted

		// Sequential processing: split at middle, adjusting for insert position
		let split_idx = total_keys / 2;

		// Adjust split point based on where new key is being inserted
		// This ensures the new key goes to the correct side
		if split_idx > insert_idx {
			split_idx - 1
		} else {
			split_idx
		}
	}

	#[allow(unused)]
	pub fn get(&self, key: &[u8]) -> Result<Option<Value>> {
		let mut current_offset = self.header.root_offset;

		loop {
			let node = self.read_node(current_offset)?;

			match node.as_ref() {
				NodeType::Internal(internal) => {
					let child_idx = internal.find_child_index(key, self.compare.as_ref());
					current_offset = internal.children[child_idx];
				}
				NodeType::Leaf(leaf) => {
					return Ok(leaf
						.find_key(key, self.compare.as_ref())
						.map(|idx| leaf.values[idx].clone()));
				}
				NodeType::Overflow(_) => {
					return Err(BPlusTreeError::UnexpectedOverflowPage(current_offset));
				}
			}
		}
	}

	pub fn delete(&mut self, key: &[u8]) -> Result<Option<Value>> {
		let mut node_offset = self.header.root_offset;
		let mut path = Vec::new();

		loop {
			let node = self.read_node(node_offset)?;

			match node.as_ref() {
				NodeType::Internal(internal) => {
					let child_idx = internal.find_child_index(key, self.compare.as_ref());
					path.push((node_offset, child_idx));
					node_offset = internal.children[child_idx];
				}
				NodeType::Leaf(_) => {
					// Need to modify the leaf - extract it from Arc
					let mut leaf = self.extract_leaf_mut(node);

					match leaf.delete(key, self.compare.as_ref()) {
						Some((_, value, overflow)) => {
							if overflow != 0 {
								self.free_overflow_chain(overflow)?;
							}

							let leaf_offset = leaf.offset;
							let is_underflow = leaf.is_underflow();

							let leaf_owned =
								std::mem::replace(&mut leaf, LeafNode::new(leaf_offset));
							self.write_node_owned(NodeType::Leaf(leaf_owned))?;

							if is_underflow && !path.is_empty() {
								self.handle_underflows(&mut path)?;
							}

							self.handle_empty_root()?;

							return Ok(Some(value));
						}
						None => {
							return Ok(None);
						}
					}
				}
				NodeType::Overflow(_) => {
					return Err(BPlusTreeError::UnexpectedOverflowPage(node_offset));
				}
			}
		}
	}

	fn handle_underflows(&mut self, path: &mut Vec<(u64, usize)>) -> Result<()> {
		// Process path from leaf up to root
		while let Some((parent_offset, child_idx)) = path.pop() {
			let mut parent = self.read_internal_node(parent_offset)?;

			let child_offset = parent.children[child_idx];

			// Check if child needs rebalancing
			match self.read_node(child_offset)?.as_ref() {
				NodeType::Internal(internal) => {
					if internal.is_underflow() {
						// Handle underflow in internal node
						self.handle_underflow(&mut parent, child_idx, true)?;
						let parent_offset = parent.offset;
						let parent_owned =
							std::mem::replace(&mut parent, InternalNode::new(parent_offset));
						self.write_node_owned(NodeType::Internal(parent_owned))?;

						// If the parent itself is now in underflow, continue processing
						if parent.is_underflow() {
							continue;
						}

						// If rebalancing occurred, we're done
						if parent.keys.len() < parent.children.len() - 1 {
							break;
						}
					} else {
						// Child is fine, no need to continue
						break;
					}
				}
				NodeType::Leaf(leaf) => {
					if leaf.is_underflow() {
						// Handle underflow in leaf node
						self.handle_underflow(&mut parent, child_idx, false)?;
						let parent_offset = parent.offset;
						let parent_owned =
							std::mem::replace(&mut parent, InternalNode::new(parent_offset));
						self.write_node_owned(NodeType::Internal(parent_owned))?;

						// If the parent itself is now in underflow, continue processing
						if parent.is_underflow() {
							continue;
						}

						// If rebalancing occurred, we're done
						if parent.keys.len() < parent.children.len() - 1 {
							break;
						}
					} else {
						// Child is fine, no need to continue
						break;
					}
				}
				NodeType::Overflow(_) => {
					return Err(BPlusTreeError::UnexpectedOverflowPage(child_offset));
				}
			}
		}

		Ok(())
	}

	// Function to handle underflow in both internal and leaf nodes
	fn handle_underflow(
		&mut self,
		parent: &mut InternalNode,
		child_idx: usize,
		is_internal: bool,
	) -> Result<()> {
		// Read siblings once and cache them (performance optimization)
		let left_sibling_node = if child_idx > 0 {
			Some(self.read_node(parent.children[child_idx - 1])?)
		} else {
			None
		};

		let right_sibling_node = if child_idx < parent.children.len() - 1 {
			Some(self.read_node(parent.children[child_idx + 1])?)
		} else {
			None
		};

		// Try to borrow from left sibling first
		if let Some(ref left_arc) = left_sibling_node {
			if is_internal {
				// For internal nodes
				if let NodeType::Internal(left_node) = left_arc.as_ref() {
					// Check if left sibling has enough to redistribute and is not in underflow
					if !left_node.is_underflow() {
						let mut left_node_mut = self.extract_internal_mut(Arc::clone(left_arc));
						let mut right_node_mut =
							self.read_internal_node(parent.children[child_idx])?;
						self.redistribute_internal_from_left(
							parent,
							child_idx - 1,
							&mut left_node_mut,
							&mut right_node_mut,
						)?;
						return Ok(());
					}
				}
			} else {
				// For leaf nodes
				if let NodeType::Leaf(left_node) = left_arc.as_ref() {
					// Check if left sibling has enough to redistribute and is not in underflow
					if !left_node.is_underflow() {
						let mut left_node_mut = self.extract_leaf_mut(Arc::clone(left_arc));
						let mut right_node_mut = self.read_leaf_node(parent.children[child_idx])?;
						self.redistribute_leaf_from_left(
							parent,
							child_idx - 1,
							&mut left_node_mut,
							&mut right_node_mut,
						)?;
						return Ok(());
					}
				}
			}
		}

		// Try to borrow from right sibling if left redistribution wasn't possible
		if let Some(ref right_arc) = right_sibling_node {
			if is_internal {
				// For internal nodes
				if let NodeType::Internal(right_node) = right_arc.as_ref() {
					// Check if right sibling has enough to redistribute and is not in underflow
					if !right_node.is_underflow() {
						let mut left_node_mut =
							self.read_internal_node(parent.children[child_idx])?;
						let mut right_node_mut = self.extract_internal_mut(Arc::clone(right_arc));
						self.redistribute_internal_from_right(
							parent,
							child_idx,
							&mut left_node_mut,
							&mut right_node_mut,
						)?;
						return Ok(());
					}
				}
			} else {
				// For leaf nodes
				if let NodeType::Leaf(right_node) = right_arc.as_ref() {
					// Check if right sibling has enough to redistribute and is not in underflow
					if !right_node.is_underflow() {
						let mut left_node_mut = self.read_leaf_node(parent.children[child_idx])?;
						let mut right_node_mut = self.extract_leaf_mut(Arc::clone(right_arc));
						self.redistribute_leaf_from_right(
							parent,
							child_idx,
							&mut left_node_mut,
							&mut right_node_mut,
						)?;
						return Ok(());
					}
				}
			}
		}

		// If we reach here, redistribution wasn't possible, we need to merge nodes

		// Prefer merging with left sibling if possible
		if let Some(left_arc) = left_sibling_node {
			let left_idx = child_idx - 1;
			if is_internal {
				let mut left_node_mut = self.extract_internal_mut(left_arc);
				let right_node_mut = self.read_internal_node(parent.children[child_idx])?;
				self.merge_internal_nodes(
					parent,
					left_idx,
					child_idx,
					&mut left_node_mut,
					right_node_mut,
				)?;
			} else {
				let mut left_node_mut = self.extract_leaf_mut(left_arc);
				let right_node_mut = self.read_leaf_node(parent.children[child_idx])?;
				self.merge_leaf_nodes(
					parent,
					left_idx,
					child_idx,
					&mut left_node_mut,
					right_node_mut,
				)?;
			}
		}
		// Otherwise merge with right sibling
		else if let Some(right_arc) = right_sibling_node {
			let right_idx = child_idx + 1;
			if is_internal {
				let mut left_node_mut = self.read_internal_node(parent.children[child_idx])?;
				let right_node_mut = self.extract_internal_mut(right_arc);
				self.merge_internal_nodes(
					parent,
					child_idx,
					right_idx,
					&mut left_node_mut,
					right_node_mut,
				)?;
			} else {
				let mut left_node_mut = self.read_leaf_node(parent.children[child_idx])?;
				let right_node_mut = self.extract_leaf_mut(right_arc);
				self.merge_leaf_nodes(
					parent,
					child_idx,
					right_idx,
					&mut left_node_mut,
					right_node_mut,
				)?;
			}
		}
		// There should always be a sibling to merge with unless this is the root
		// which is handled separately in handle_empty_root

		Ok(())
	}

	fn handle_empty_root(&mut self) -> Result<()> {
		match self.read_node(self.header.root_offset)?.as_ref() {
			NodeType::Internal(internal) => {
				if internal.keys.is_empty() && internal.children.len() == 1 {
					let old_root_offset = self.header.root_offset;
					let new_root_offset = internal.children[0];
					self.header.root_offset = new_root_offset;
					self.write_header()?;

					self.free_node_with_overflow(old_root_offset)?;
				}
			}
			NodeType::Leaf(_) => {}
			NodeType::Overflow(_) => {
				return Err(BPlusTreeError::InvalidRootNode(self.header.root_offset));
			}
		}
		Ok(())
	}

	fn redistribute_internal_from_left(
		&mut self,
		parent: &mut InternalNode,
		left_idx: usize,
		left_node: &mut InternalNode,
		right_node: &mut InternalNode,
	) -> Result<()> {
		let last_entry_size = if !left_node.keys.is_empty() {
			let last_key = left_node.keys.last().unwrap();
			internal_entry_size(last_key)
		} else {
			return Err(BPlusTreeError::Serialization(
				"Left internal node is unexpectedly empty during redistribution".into(),
			));
		};

		// Calculate sizes once
		let left_size = left_node.current_size();
		let right_size = right_node.current_size();

		if Self::should_redistribute_with_sizes(left_size, right_size, last_entry_size, true) {
			let (parent_key, parent_overflow) = InternalNode::extract_parent_key(parent, left_idx);
			let (new_parent_key, new_parent_overflow) =
				left_node.redistribute_to_right(right_node, parent_key, parent_overflow);

			parent.keys[left_idx] = new_parent_key;
			parent.set_overflow_at(left_idx, new_parent_overflow);

			self.write_node_owned(NodeType::Internal(left_node.clone()))?;
			self.write_node_owned(NodeType::Internal(right_node.clone()))?;
		}

		Ok(())
	}

	fn redistribute_internal_from_right(
		&mut self,
		parent: &mut InternalNode,
		left_idx: usize,
		left_node: &mut InternalNode,
		right_node: &mut InternalNode,
	) -> Result<()> {
		let first_entry_size = if !right_node.keys.is_empty() {
			internal_entry_size(&right_node.keys[0])
		} else {
			return Err(BPlusTreeError::Serialization(
				"Right internal node is unexpectedly empty during redistribution".into(),
			));
		};

		// Calculate sizes once
		let left_size = left_node.current_size();
		let right_size = right_node.current_size();

		if Self::should_redistribute_with_sizes(left_size, right_size, first_entry_size, false) {
			let (parent_key, parent_overflow) = InternalNode::extract_parent_key(parent, left_idx);

			let (new_parent_key, new_parent_overflow) =
				left_node.take_from_right(right_node, parent_key, parent_overflow);

			parent.keys[left_idx] = new_parent_key;
			parent.set_overflow_at(left_idx, new_parent_overflow);

			self.write_node_owned(NodeType::Internal(left_node.clone()))?;
			self.write_node_owned(NodeType::Internal(right_node.clone()))?;
		}

		Ok(())
	}

	fn redistribute_leaf_from_left(
		&mut self,
		parent: &mut InternalNode,
		left_idx: usize,
		left_node: &mut LeafNode,
		right_node: &mut LeafNode,
	) -> Result<()> {
		let last_idx = left_node.keys.len() - 1;
		let last_entry_size = if last_idx < left_node.keys.len() {
			leaf_entry_size(&left_node.keys[last_idx], &left_node.values[last_idx])
		} else {
			return Err(BPlusTreeError::Serialization(
				"Left node is unexpectedly empty during redistribution".into(),
			));
		};

		// Calculate sizes once
		let left_size = left_node.current_size();
		let right_size = right_node.current_size();

		if Self::should_redistribute_with_sizes(left_size, right_size, last_entry_size, true) {
			let new_separator = left_node.redistribute_to_right(right_node);

			parent.keys[left_idx] = new_separator;

			self.write_node_owned(NodeType::Leaf(left_node.clone()))?;
			self.write_node_owned(NodeType::Leaf(right_node.clone()))?;
		}

		Ok(())
	}

	fn redistribute_leaf_from_right(
		&mut self,
		parent: &mut InternalNode,
		left_idx: usize,
		left_node: &mut LeafNode,
		right_node: &mut LeafNode,
	) -> Result<()> {
		let first_entry_size = if !right_node.keys.is_empty() {
			leaf_entry_size(&right_node.keys[0], &right_node.values[0])
		} else {
			return Err(BPlusTreeError::Serialization(
				"Right node is unexpectedly empty during redistribution".into(),
			));
		};

		// Calculate sizes once
		let left_size = left_node.current_size();
		let right_size = right_node.current_size();

		if Self::should_redistribute_with_sizes(left_size, right_size, first_entry_size, false) {
			let new_separator = left_node.take_from_right(right_node);

			parent.keys[left_idx] = new_separator;

			self.write_node_owned(NodeType::Leaf(left_node.clone()))?;
			self.write_node_owned(NodeType::Leaf(right_node.clone()))?;
		}

		Ok(())
	}

	fn should_redistribute_with_sizes(
		left_size: usize,
		right_size: usize,
		entry_size: usize,
		taking_from_left: bool,
	) -> bool {
		let total_size = left_size + right_size;
		let target_size = total_size / 2;

		let before_left_diff = ((left_size as i64) - (target_size as i64)).abs();
		let before_right_diff = ((right_size as i64) - (target_size as i64)).abs();
		let before_total_diff = before_left_diff + before_right_diff;

		let (after_left_size, after_right_size) = if taking_from_left {
			if entry_size > left_size {
				return false;
			}
			(left_size - entry_size, right_size + entry_size)
		} else {
			if entry_size > right_size {
				return false;
			}
			(left_size + entry_size, right_size - entry_size)
		};

		let after_left_diff = ((after_left_size as i64) - (target_size as i64)).abs();
		let after_right_diff = ((after_right_size as i64) - (target_size as i64)).abs();
		let after_total_diff = after_left_diff + after_right_diff;

		after_total_diff < before_total_diff
	}

	fn merge_internal_nodes(
		&mut self,
		parent: &mut InternalNode,
		left_idx: usize,
		right_idx: usize,
		left_node: &mut InternalNode,
		right_node: InternalNode,
	) -> Result<()> {
		if !left_node.can_merge_with(&right_node) {
			return Ok(());
		}

		let right_offset = parent.children[right_idx];
		let (separator, separator_overflow) = parent.remove_key_with_overflow(left_idx).unwrap();

		left_node.merge_from_right(right_node, separator, separator_overflow);

		parent.children.remove(right_idx);

		self.write_node_owned(NodeType::Internal(left_node.clone()))?;

		self.free_page(right_offset)?;

		Ok(())
	}

	fn merge_leaf_nodes(
		&mut self,
		parent: &mut InternalNode,
		left_idx: usize,
		right_idx: usize,
		left_node: &mut LeafNode,
		right_node: LeafNode,
	) -> Result<()> {
		if !left_node.can_merge_with(&right_node) {
			return Ok(());
		}

		let left_offset = left_node.offset;
		let right_offset = parent.children[right_idx];
		let (_removed_key, removed_overflow) = parent.remove_key_with_overflow(left_idx).unwrap();
		parent.children.remove(right_idx);

		if removed_overflow != 0 {
			self.free_overflow_chain(removed_overflow)?;
		}

		let next_leaf = right_node.next_leaf;

		left_node.merge_from_right(right_node);

		if next_leaf != 0 {
			let next_node = self.read_node(next_leaf)?;
			if matches!(next_node.as_ref(), NodeType::Leaf(_)) {
				let mut next_leaf_node = self.extract_leaf_mut(next_node);
				next_leaf_node.prev_leaf = left_offset;
				self.write_node(&NodeType::Leaf(next_leaf_node))?;
			}
		}

		self.write_node_owned(NodeType::Leaf(left_node.clone()))?;

		self.free_page(right_offset)?;

		Ok(())
	}

	fn allocate_page(&mut self) -> Result<u64> {
		if self.header.trunk_page_head == 0 || self.header.free_page_count == 0 {
			let offset = self.header.total_pages * PAGE_SIZE as u64;
			self.header.total_pages += 1;
			self.write_header()?;
			return Ok(offset);
		}

		let mut prev_trunk_offset = 0;
		let mut current_trunk_offset = self.header.trunk_page_head;

		while current_trunk_offset != 0 {
			let mut trunk = self.read_trunk_page(current_trunk_offset)?;

			if !trunk.is_empty() {
				let page_offset = trunk.get_free_page().unwrap();
				self.write_trunk_page(&trunk)?;
				self.header.free_page_count -= 1;
				self.write_header()?;
				return Ok(page_offset);
			}

			let next_trunk = trunk.next_trunk;

			if next_trunk != 0 {
				if prev_trunk_offset == 0 {
					self.header.trunk_page_head = next_trunk;
				} else {
					let mut prev_trunk = self.read_trunk_page(prev_trunk_offset)?;
					prev_trunk.next_trunk = next_trunk;
					self.write_trunk_page(&prev_trunk)?;
				}

				self.write_header()?;

				return Ok(current_trunk_offset);
			}

			prev_trunk_offset = current_trunk_offset;
			current_trunk_offset = trunk.next_trunk;
		}

		Err(BPlusTreeError::InconsistentFreePageCount {
			claimed: self.header.free_page_count,
			actual: 0,
		})
	}

	fn free_node_with_overflow(&mut self, offset: u64) -> Result<()> {
		let node = self.read_node(offset)?;

		match node.as_ref() {
			NodeType::Internal(internal) => {
				for &overflow_offset in &internal.key_overflows {
					if overflow_offset != 0 {
						self.free_overflow_chain(overflow_offset)?;
					}
				}
			}
			NodeType::Leaf(leaf) => {
				for &overflow_offset in &leaf.cell_overflows {
					if overflow_offset != 0 {
						self.free_overflow_chain(overflow_offset)?;
					}
				}
			}
			NodeType::Overflow(_) => {}
		}

		self.free_page(offset)
	}

	fn free_page(&mut self, offset: u64) -> Result<()> {
		if offset < PAGE_SIZE as u64 || offset >= self.header.total_pages * PAGE_SIZE as u64 {
			return Err(BPlusTreeError::InvalidOffset);
		}

		self.cache.remove(&offset);

		if self.header.trunk_page_head == 0 {
			let trunk = TrunkPage::new(offset);

			self.header.trunk_page_head = offset;
			self.write_header()?;

			self.write_trunk_page(&trunk)?;
		} else {
			let mut current_trunk_offset = self.header.trunk_page_head;

			loop {
				let mut trunk = self.read_trunk_page(current_trunk_offset)?;

				if !trunk.is_full() {
					trunk.add_free_page(offset);
					self.write_trunk_page(&trunk)?;

					self.header.free_page_count += 1;
					self.write_header()?;
					break;
				}

				if trunk.next_trunk == 0 {
					let new_trunk = TrunkPage::new(offset);

					trunk.next_trunk = offset;

					self.write_trunk_page(&trunk)?;
					self.write_trunk_page(&new_trunk)?;

					break;
				}

				current_trunk_offset = trunk.next_trunk;
			}
		}

		Ok(())
	}

	fn read_trunk_page(&mut self, offset: u64) -> Result<TrunkPage> {
		// Read directly from disk
		let mut buffer = vec![0; PAGE_SIZE];
		self.file.read_at(offset, &mut buffer)?;

		// Deserialize the full page
		let trunk = TrunkPage::deserialize(&buffer, offset)?;

		Ok(trunk)
	}

	fn write_trunk_page(&mut self, trunk: &TrunkPage) -> Result<()> {
		let data = trunk.serialize()?;

		// Write data directly to storage
		self.file.write_at(trunk.offset, &data)?;

		self.maybe_sync()?;

		Ok(())
	}

	fn read_node(&self, offset: u64) -> Result<Arc<NodeType>> {
		// Check if the node is in the cache first
		if let Some(arc_node) = self.cache.get(&offset) {
			return Ok(Arc::clone(&arc_node));
		}

		// Read from disk
		let mut buffer = vec![0; PAGE_SIZE];
		self.file.read_at(offset, &mut buffer)?;

		// Deserialize based on node type, with overflow support
		let node = if buffer.is_empty() {
			return Err(BPlusTreeError::Deserialization("Empty buffer".into()));
		} else {
			let node_type = buffer[0];
			match node_type {
				NODE_TYPE_INTERNAL => {
					let internal =
						InternalNode::deserialize(&buffer, offset, &|overflow_offset| {
							self.read_overflow_chain(overflow_offset)
						})?;
					NodeType::Internal(internal)
				}
				NODE_TYPE_LEAF => {
					let leaf = LeafNode::deserialize(&buffer, offset, &|overflow_offset| {
						self.read_overflow_chain(overflow_offset)
					})?;
					NodeType::Leaf(leaf)
				}
				NODE_TYPE_OVERFLOW => {
					NodeType::Overflow(OverflowPage::deserialize(&buffer, offset)?)
				}
				_ => {
					return Err(BPlusTreeError::InvalidNodeType);
				}
			}
		};

		let arc_node = Arc::new(node);
		self.cache.insert(offset, Arc::clone(&arc_node));
		Ok(arc_node)
	}

	/// Extract internal node from offset
	fn read_internal_node(&self, offset: u64) -> Result<InternalNode> {
		match self.read_node(offset)?.as_ref() {
			NodeType::Internal(node) => Ok(node.clone()),
			_ => Err(BPlusTreeError::InvalidNodeType),
		}
	}

	/// Extract leaf node from offset
	fn read_leaf_node(&self, offset: u64) -> Result<LeafNode> {
		match self.read_node(offset)?.as_ref() {
			NodeType::Leaf(node) => Ok(node.clone()),
			_ => Err(BPlusTreeError::InvalidNodeType),
		}
	}

	/// Extract a mutable InternalNode from Arc<NodeType>
	/// Unwraps if possible (no other refs), otherwise clones
	fn extract_internal_mut(&self, arc_node: Arc<NodeType>) -> InternalNode {
		match Arc::try_unwrap(arc_node) {
			Ok(node_type) => match node_type {
				NodeType::Internal(node) => node,
				_ => panic!("Expected Internal node"),
			},
			Err(arc) => match arc.as_ref() {
				NodeType::Internal(node) => node.clone(),
				_ => panic!("Expected Internal node"),
			},
		}
	}

	/// Extract a mutable LeafNode from Arc<NodeType>
	/// Unwraps if possible (no other refs), otherwise clones
	fn extract_leaf_mut(&self, arc_node: Arc<NodeType>) -> LeafNode {
		match Arc::try_unwrap(arc_node) {
			Ok(node_type) => match node_type {
				NodeType::Leaf(node) => node,
				_ => panic!("Expected Leaf node"),
			},
			Err(arc) => match arc.as_ref() {
				NodeType::Leaf(node) => node.clone(),
				_ => panic!("Expected Leaf node"),
			},
		}
	}

	/// Extract a mutable OverflowPage from Arc<NodeType>
	/// Unwraps if possible (no other refs), otherwise clones
	fn extract_overflow_mut(&self, arc_node: Arc<NodeType>) -> OverflowPage {
		match Arc::try_unwrap(arc_node) {
			Ok(node_type) => match node_type {
				NodeType::Overflow(node) => node,
				_ => panic!("Expected Overflow node"),
			},
			Err(arc) => match arc.as_ref() {
				NodeType::Overflow(node) => node.clone(),
				_ => panic!("Expected Overflow node"),
			},
		}
	}

	/// Prepare internal node for writing by creating overflow pages for large
	/// keys
	fn prepare_internal_node_overflow(&mut self, node: &mut InternalNode) -> Result<()> {
		let (min_local, max_local) = calculate_local_limits(false);
		let usable_size = INTERNAL_USABLE_SIZE;

		// Ensure key_overflows vec is properly sized
		while node.key_overflows.len() < node.keys.len() {
			node.key_overflows.push(0);
		}

		// Check each key to see if it needs overflow
		for (i, key) in node.keys.iter().enumerate() {
			let (bytes_on_page, needs_overflow) =
				calculate_overflow(key.len(), min_local, max_local, usable_size);

			if needs_overflow {
				let overflow_data = &key[bytes_on_page..];
				let existing_overflow = if i < node.key_overflows.len() {
					node.key_overflows[i]
				} else {
					0
				};

				if existing_overflow == 0 {
					let overflow_offset = self.write_overflow_chain(overflow_data)?;
					while node.key_overflows.len() <= i {
						node.key_overflows.push(0);
					}
					node.key_overflows[i] = overflow_offset;
				}
			} else if i < node.key_overflows.len() {
				node.key_overflows[i] = 0;
			}
		}

		Ok(())
	}

	/// Prepare leaf node for writing by creating overflow pages for large cells
	/// (key+value)
	fn prepare_leaf_node_overflow(&mut self, node: &mut LeafNode) -> Result<()> {
		let (min_local, max_local) = calculate_local_limits(true);
		let usable_size = LEAF_USABLE_SIZE;

		// Ensure overflow vec is properly sized
		while node.cell_overflows.len() < node.keys.len() {
			node.cell_overflows.push(0);
		}

		// Check each cell (key+value pair) to see if it needs overflow
		for (i, (key, value)) in node.keys.iter().zip(&node.values).enumerate() {
			let payload_len = key.len() + value.len();
			let (bytes_on_page, needs_overflow) =
				calculate_overflow(payload_len, min_local, max_local, usable_size);

			if needs_overflow {
				let mut cell_data = Vec::with_capacity(payload_len);
				cell_data.extend_from_slice(key);
				cell_data.extend_from_slice(value);

				let overflow_data = &cell_data[bytes_on_page..];
				let existing_overflow = if i < node.cell_overflows.len() {
					node.cell_overflows[i]
				} else {
					0
				};

				if existing_overflow == 0 {
					let overflow_offset = self.write_overflow_chain(overflow_data)?;
					while node.cell_overflows.len() <= i {
						node.cell_overflows.push(0);
					}
					node.cell_overflows[i] = overflow_offset;
				}
			} else if i < node.cell_overflows.len() {
				node.cell_overflows[i] = 0;
			}
		}

		Ok(())
	}

	fn write_node(&mut self, node: &NodeType) -> Result<()> {
		self.write_node_owned(node.clone())
	}

	fn write_node_owned(&mut self, mut node: NodeType) -> Result<()> {
		// Prepare overflow pages
		match &mut node {
			NodeType::Internal(internal) => {
				self.prepare_internal_node_overflow(internal)?;
			}
			NodeType::Leaf(leaf) => {
				self.prepare_leaf_node_overflow(leaf)?;
			}
			NodeType::Overflow(_) => {}
		};

		let data = match &node {
			NodeType::Internal(internal) => internal.serialize()?,
			NodeType::Leaf(leaf) => leaf.serialize()?,
			NodeType::Overflow(overflow) => overflow.serialize()?,
		};

		let offset = match &node {
			NodeType::Internal(internal) => internal.offset,
			NodeType::Leaf(leaf) => leaf.offset,
			NodeType::Overflow(overflow) => overflow.offset,
		};

		// Write data directly to storage
		self.file.write_at(offset, &data)?;

		self.maybe_sync()?;

		self.cache.insert(offset, Arc::new(node));
		Ok(())
	}

	fn write_header(&mut self) -> Result<()> {
		// Write header data at offset 0
		let header_bytes = self.header.serialize();

		// Create a buffer with the header followed by zeros
		let mut buffer = vec![0u8; PAGE_SIZE];
		buffer[..header_bytes.len()].copy_from_slice(&header_bytes);

		// Write the entire page at once
		self.file.write_at(0, &buffer)?;

		self.maybe_sync()?;

		Ok(())
	}

	fn maybe_sync(&mut self) -> Result<()> {
		match self.durability {
			Durability::Always => self.file.sync_data()?,
			Durability::Manual => { // Don't sync - only sync on flush() or close()
			}
		}
		Ok(())
	}

	fn write_overflow_chain(&mut self, data: &[u8]) -> Result<u64> {
		if data.is_empty() {
			return Err(BPlusTreeError::Serialization(
				"Cannot create overflow chain for empty data".into(),
			));
		}

		let max_data_per_page = OverflowPage::max_data_size();

		let mut remaining_data = data;
		let mut first_page_offset = 0u64;
		let mut prev_page_offset = 0u64;

		while !remaining_data.is_empty() {
			// Allocate a new overflow page
			let page_offset = self.allocate_page()?;

			if first_page_offset == 0 {
				first_page_offset = page_offset;
			}

			// Determine how much data to store in this page
			let chunk_size = remaining_data.len().min(max_data_per_page);
			let chunk_data = &remaining_data[..chunk_size];
			remaining_data = &remaining_data[chunk_size..];

			// Create and write the overflow page
			let mut overflow_page = OverflowPage::new(page_offset);
			overflow_page.data = chunk_data.to_vec();
			overflow_page.next_overflow = 0; // Will be updated if there's more data

			self.write_node(&NodeType::Overflow(overflow_page.clone()))?;

			// If this is not the first page, update the previous page to point to this one
			if prev_page_offset != 0 {
				let prev_node = self.read_node(prev_page_offset)?;
				let mut prev_overflow = self.extract_overflow_mut(prev_node);
				prev_overflow.next_overflow = page_offset;
				self.write_node(&NodeType::Overflow(prev_overflow))?;
			}

			prev_page_offset = page_offset;
		}

		self.maybe_sync()?;
		Ok(first_page_offset)
	}

	/// Read data from an overflow chain starting at the given offset
	fn read_overflow_chain(&self, first_page: u64) -> Result<Vec<u8>> {
		if first_page == 0 {
			return Ok(Vec::new());
		}

		let mut result = Vec::new();
		let mut current_offset = first_page;

		while current_offset != 0 {
			let node = self.read_node(current_offset)?;
			match node.as_ref() {
				NodeType::Overflow(overflow) => {
					result.extend_from_slice(&overflow.data);
					current_offset = overflow.next_overflow;
				}
				_ => {
					return Err(BPlusTreeError::InvalidOverflowChain(current_offset));
				}
			}
		}

		Ok(result)
	}

	fn free_overflow_chain(&mut self, first_page: u64) -> Result<()> {
		if first_page == 0 {
			return Ok(());
		}

		let mut current_offset = first_page;

		while current_offset != 0 {
			let node = self.read_node(current_offset)?;
			match node.as_ref() {
				NodeType::Overflow(overflow) => {
					let next_offset = overflow.next_overflow;
					self.free_page(current_offset)?;
					current_offset = next_offset;
				}
				_ => {
					return Err(BPlusTreeError::InvalidOverflowChain(current_offset));
				}
			}
		}

		self.maybe_sync()?;
		Ok(())
	}

	pub fn range(&self, start_key: &[u8], end_key: &[u8]) -> Result<RangeScanIterator<'_, F>> {
		RangeScanIterator::new(self, start_key, end_key)
	}

	/// Calculate the height of the B+ tree.
	#[cfg(test)]
	pub fn calculate_tree_stats(&mut self) -> Result<(usize, usize, usize, usize)> {
		let root_offset = self.header.root_offset;
		let (height, node_count, total_keys, leaf_nodes) =
			self.calculate_subtree_stats(root_offset, 1)?;

		Ok((height, node_count, total_keys, leaf_nodes))
	}

	#[cfg(test)]
	fn calculate_subtree_stats(
		&mut self,
		node_offset: u64,
		current_level: usize,
	) -> Result<(usize, usize, usize, usize)> {
		match self.read_node(node_offset)?.as_ref() {
			NodeType::Leaf(leaf) => {
				// For leaf nodes:
				// - Height is current level
				// - Node count is 1
				// - Total keys is number of keys in this leaf
				// - Leaf nodes is 1
				Ok((current_level, 1, leaf.keys.len(), 1))
			}
			NodeType::Internal(internal) => {
				let mut max_height = 0;
				let mut total_nodes = 1; // Count this node
				let mut total_keys = internal.keys.len();
				let mut leaf_count = 0;

				for &child_offset in &internal.children {
					let (child_height, child_nodes, child_keys, child_leaves) =
						self.calculate_subtree_stats(child_offset, current_level + 1)?;

					max_height = max_height.max(child_height);
					total_nodes += child_nodes;
					total_keys += child_keys;
					leaf_count += child_leaves;
				}

				Ok((max_height, total_nodes, total_keys, leaf_count))
			}
			NodeType::Overflow(_) => Err(BPlusTreeError::UnexpectedOverflowPage(node_offset)),
		}
	}

	#[cfg(test)]
	fn print_tree_stats(&mut self) -> Result<()> {
		Ok(())
	}
}

pub struct RangeScanIterator<'a, F: VfsFile> {
	tree: &'a BPlusTree<F>,
	current_leaf: Option<LeafNode>,
	end_key: Vec<u8>,
	current_idx: usize,
	current_end_idx: usize, // Pre-calculated end position in current leaf
	reached_end: bool,
}

impl<'a, F: VfsFile> RangeScanIterator<'a, F> {
	pub(crate) fn new(tree: &'a BPlusTree<F>, start_key: &[u8], end_key: &[u8]) -> Result<Self> {
		// Find the leaf containing the start key
		let mut node_offset = tree.header.root_offset;

		// Traverse the tree to find the starting leaf
		let leaf = loop {
			match tree.read_node(node_offset)?.as_ref() {
				NodeType::Internal(internal) => {
					let idx = internal.find_child_index(start_key, tree.compare.as_ref());
					node_offset = internal.children[idx];
				}
				NodeType::Leaf(leaf) => {
					break leaf.clone();
				}
				NodeType::Overflow(_) => {
					return Err(BPlusTreeError::UnexpectedOverflowPage(node_offset));
				}
			}
		};

		// Find the first key >= start_key in the leaf
		let current_idx =
			leaf.keys.partition_point(|k| tree.compare.compare(k, start_key) == Ordering::Less);

		// Pre-calculate end index in this leaf
		let current_end_idx =
			leaf.keys.partition_point(|k| tree.compare.compare(k, end_key) != Ordering::Greater);

		Ok(RangeScanIterator {
			tree,
			current_leaf: Some(leaf),
			end_key: end_key.to_vec(),
			current_idx,
			current_end_idx,
			reached_end: false,
		})
	}
}

impl<F: VfsFile> Iterator for RangeScanIterator<'_, F> {
	type Item = Result<(Key, Value)>;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			if self.reached_end {
				return None;
			}

			if let Some(leaf) = &self.current_leaf {
				// Check if we've exhausted current leaf
				if self.current_idx >= self.current_end_idx {
					// Move to the next leaf if possible
					if leaf.next_leaf == 0 {
						self.reached_end = true;
						return None;
					}

					// Load the next leaf and calculate its end index
					match self.tree.read_node(leaf.next_leaf) {
						Ok(arc_node) => match arc_node.as_ref() {
							NodeType::Leaf(next_leaf) => {
								let next_end_idx = next_leaf.keys.partition_point(|k| {
									self.tree.compare.compare(k, &self.end_key) != Ordering::Greater
								});

								self.current_leaf = Some(next_leaf.clone());
								self.current_idx = 0;
								self.current_end_idx = next_end_idx;
								continue;
							}
							_ => return Some(Err(BPlusTreeError::InvalidNodeType)),
						},
						Err(e) => return Some(Err(e)),
					}
				}

				// Return current entry from stored leaf (no range check needed -
				// pre-calculated!)
				let result = Ok((
					leaf.keys[self.current_idx].clone(),
					leaf.values[self.current_idx].clone(),
				));
				self.current_idx += 1;
				return Some(result);
			}

			self.reached_end = true;
			return None;
		}
	}
}

#[cfg(test)]
mod tests {
	use std::fs::File;
	use std::io::Read;

	use rand::rngs::StdRng;
	use rand::{Rng, SeedableRng};
	use tempfile::NamedTempFile;
	use test_log::test;

	use super::*;

	#[derive(Clone)]
	struct TestComparator;

	impl Comparator for TestComparator {
		fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
			a.cmp(b)
		}

		fn separator(&self, from: &[u8], to: &[u8]) -> Vec<u8> {
			// Simple separator implementation for tests
			if from.len() < to.len() {
				from.to_vec()
			} else {
				to.to_vec()
			}
		}

		fn successor(&self, key: &[u8]) -> Vec<u8> {
			let mut result = key.to_vec();
			result.push(0);
			result
		}

		fn name(&self) -> &str {
			"TestComparator"
		}
	}

	#[derive(Clone)]
	struct U32Comparator;

	impl Comparator for U32Comparator {
		fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
			let a_num = u32::from_be_bytes(a.try_into().unwrap());
			let b_num = u32::from_be_bytes(b.try_into().unwrap());
			a_num.cmp(&b_num)
		}

		fn separator(&self, from: &[u8], to: &[u8]) -> Vec<u8> {
			let from_num = u32::from_be_bytes(from.try_into().unwrap());
			let to_num = u32::from_be_bytes(to.try_into().unwrap());
			if from_num < to_num {
				((from_num + to_num) / 2).to_be_bytes().to_vec()
			} else {
				from.to_vec()
			}
		}

		fn successor(&self, key: &[u8]) -> Vec<u8> {
			let key_num = u32::from_be_bytes(key.try_into().unwrap());
			(key_num + 1).to_be_bytes().to_vec()
		}

		fn name(&self) -> &str {
			"U32Comparator"
		}
	}

	#[derive(Clone)]
	struct BinaryComparator;

	impl Comparator for BinaryComparator {
		fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
			a.cmp(b)
		}

		fn separator(&self, from: &[u8], to: &[u8]) -> Vec<u8> {
			// Simple separator implementation for tests
			if from.len() < to.len() {
				from.to_vec()
			} else {
				to.to_vec()
			}
		}

		fn successor(&self, key: &[u8]) -> Vec<u8> {
			let mut result = key.to_vec();
			result.push(0);
			result
		}

		fn name(&self) -> &str {
			"BinaryComparator"
		}
	}

	fn create_test_tree(sync: bool) -> BPlusTree<File> {
		let file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(file.path(), Arc::new(TestComparator)).unwrap();
		tree.set_durability(if sync {
			Durability::Always
		} else {
			Durability::Manual
		});
		tree
	}

	#[test]
	fn test_basic_operations() {
		let mut tree = create_test_tree(true);

		// Test insertions
		tree.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
		tree.insert(b"key2".to_vec(), b"value2".to_vec()).unwrap();
		tree.insert(b"key3".to_vec(), b"value3".to_vec()).unwrap();

		// Test retrievals
		assert_eq!(tree.get(b"key1").unwrap().unwrap(), b"value1");
		assert_eq!(tree.get(b"key2").unwrap().unwrap(), b"value2");
		assert_eq!(tree.get(b"key3").unwrap().unwrap(), b"value3");

		// Test non-existent key
		assert!(tree.get(b"nonexistent").unwrap().is_none());

		// Test deletions
		assert_eq!(tree.delete(b"key2").unwrap().unwrap(), b"value2");
		assert!(tree.get(b"key2").unwrap().is_none());
	}

	#[test]
	fn test_sequential_small() {
		let mut tree = create_test_tree(true);

		// Insert 10 items
		for i in 0..10 {
			let key = format!("key{:03}", i).into_bytes();
			let value = format!("value{:03}", i).into_bytes();
			tree.insert(key, value).unwrap();
		}

		// Delete even numbered items
		for i in (0..10).step_by(2) {
			let key = format!("key{:03}", i).into_bytes();

			// Verify key exists before deletion
			let _ = tree.get(&key).unwrap();

			// Attempt deletion
			let _ = tree.delete(&key).unwrap();

			// Verify key no longer exists
			let after_delete = tree.get(&key).unwrap();
			assert!(
				after_delete.is_none(),
				"Key still exists after deletion: {:?}",
				String::from_utf8_lossy(&key)
			);
		}

		// Verify all odd numbered items still exist
		for i in (1..10).step_by(2) {
			let key = format!("key{:03}", i).into_bytes();
			let value = format!("value{:03}", i).into_bytes();

			let result = tree.get(&key).unwrap();

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
		let mut tree = create_test_tree(false);

		let keys = generate_sequential_keys(TEST_SIZE);
		let values = generate_random_values(TEST_SIZE, 100);

		for i in 0..TEST_SIZE {
			tree.insert(keys[i].clone(), values[i].clone()).unwrap();

			let retrieved = tree.get(&keys[i]).unwrap();
			assert!(retrieved.is_some(), "Failed to retrieve just-inserted key at index {}", i);
			assert_eq!(
				retrieved.unwrap(),
				values[i],
				"Retrieved value doesn't match at index {}",
				i
			);
		}

		// Delete even-indexed entries and verify after each deletion
		for i in (0..TEST_SIZE).step_by(2) {
			// Verify the key exists before deletion
			let exists = tree.get(&keys[i]).unwrap();
			assert!(exists.is_some(), "Key {} not found before deletion attempt", i);

			// Attempt deletion
			let deleted = tree.delete(&keys[i]).unwrap();
			assert!(
				deleted.is_some(),
				"Failed to delete value at index {} (key: {:?})",
				i,
				String::from_utf8_lossy(&keys[i])
			);

			// Verify the deletion
			let after_delete = tree.get(&keys[i]).unwrap();
			assert!(
				after_delete.is_none(),
				"Key still exists after deletion at index {} (key: {:?})",
				i,
				String::from_utf8_lossy(&keys[i])
			);

			// Verify adjacent keys weren't affected
			if i > 0 {
				let prev = tree.get(&keys[i - 1]).unwrap();
				assert_eq!(
					prev.unwrap(),
					values[i - 1],
					"Previous value corrupted at index {}",
					i - 1
				);
			}
			if i < TEST_SIZE - 1 {
				let next = tree.get(&keys[i + 1]).unwrap();
				assert_eq!(next.unwrap(), values[i + 1], "Next value corrupted at index {}", i + 1);
			}
		}

		for i in 0..TEST_SIZE {
			let retrieved = tree.get(&keys[i]).unwrap();
			if i % 2 == 0 {
				assert!(retrieved.is_none(), "Value at index {} should have been deleted", i);
			} else {
				assert!(retrieved.is_some(), "Value at index {} should still exist", i);
				assert_eq!(
					retrieved.unwrap(),
					values[i],
					"Retrieved value doesn't match at index {}",
					i
				);
			}
		}
	}

	fn generate_sequential_keys(n: usize) -> Vec<Vec<u8>> {
		(0..n).map(|i| format!("key{:010}", i).into_bytes()).collect()
	}

	fn generate_random_values(n: usize, value_size: usize) -> Vec<Vec<u8>> {
		let mut rng = rand::rng();
		(0..n).map(|_| (0..value_size).map(|_| rng.random::<u8>()).collect()).collect()
	}

	#[test]
	fn test_sequential_delete_samples() {
		let mut tree = create_test_tree(false);

		let mut data = Vec::new();
		for i in 0..TEST_SIZE {
			let key = format!("key{:03}", i).into_bytes();
			let value = format!("value{:03}", i).into_bytes();
			data.push((key, value));
		}

		// Insert all items
		for (key, value) in data.iter() {
			tree.insert(key.clone(), value.clone()).unwrap();
		}

		tree.print_tree_stats().unwrap();

		// Sample size configuration
		let max_samples = 50;

		// Delete items sequentially
		for (i, (key, expected_value)) in data.iter().enumerate() {
			// Verify key exists before deletion
			let exists = tree.get(key).unwrap();
			assert!(
				exists.is_some(),
				"Key should exist before deletion: {:?}",
				String::from_utf8_lossy(key)
			);

			// Perform deletion
			match tree.delete(key) {
				Ok(Some(value)) => {
					assert_eq!(
						&value,
						expected_value,
						"Deleted value doesn't match for key: {:?}",
						String::from_utf8_lossy(key)
					);
				}
				Ok(None) => panic!(
					"Key reported as not found during deletion: {:?}",
					String::from_utf8_lossy(key)
				),
				Err(e) => panic!("Error deleting key {:?}: {}", String::from_utf8_lossy(key), e),
			}

			// Verify key no longer exists
			let after_delete = tree.get(key).unwrap();
			assert!(
				after_delete.is_none(),
				"Key still exists after deletion: {:?}",
				String::from_utf8_lossy(key)
			);

			// Verify a sample of remaining keys
			let remaining = data.len() - i - 1;
			if remaining > 0 {
				let sample_size = std::cmp::min(max_samples, remaining);

				for j in 0..sample_size {
					// Evenly distribute samples across remaining keys
					let idx = i + 1 + (j * remaining / sample_size);
					let (remain_key, remain_value) = &data[idx];

					match tree.get(remain_key).unwrap() {
						Some(v) => {
							assert_eq!(
								&v,
								remain_value,
								"Value mismatch for remaining key: {:?}",
								String::from_utf8_lossy(remain_key)
							);
						}
						None => panic!(
							"Remaining key not found: {:?}",
							String::from_utf8_lossy(remain_key)
						),
					}
				}
			}
		}
	}

	#[test]
	#[ignore]
	fn test_sequential_delete_all() {
		let mut tree = create_test_tree(false);

		let mut data = Vec::new();
		for i in 0..TEST_SIZE {
			let key = format!("key{:03}", i).into_bytes();
			let value = format!("value{:03}", i).into_bytes();
			data.push((key, value));
		}

		// Insert all items
		for (key, value) in data.iter() {
			tree.insert(key.clone(), value.clone()).unwrap();
		}

		tree.print_tree_stats().unwrap();

		// Delete items sequentially
		for (i, (key, expected_value)) in data.iter().enumerate() {
			// Verify key exists before deletion
			let exists = tree.get(key).unwrap();
			assert!(
				exists.is_some(),
				"Key should exist before deletion: {:?}",
				String::from_utf8_lossy(key)
			);

			// Perform deletion
			match tree.delete(key) {
				Ok(Some(value)) => {
					assert_eq!(
						&value,
						expected_value,
						"Deleted value doesn't match for key: {:?}",
						String::from_utf8_lossy(key)
					);
				}
				Ok(None) => panic!(
					"Key reported as not found during deletion: {:?}",
					String::from_utf8_lossy(key)
				),
				Err(e) => panic!("Error deleting key {:?}: {}", String::from_utf8_lossy(key), e),
			}

			// Verify key no longer exists
			let after_delete = tree.get(key).unwrap();
			assert!(
				after_delete.is_none(),
				"Key still exists after deletion: {:?}",
				String::from_utf8_lossy(key)
			);

			// Verify remaining keys
			for (remain_key, remain_value) in data.iter().take(TEST_SIZE).skip(i + 1) {
				match tree.get(remain_key).unwrap() {
					Some(v) => {
						assert_eq!(
							&v,
							remain_value,
							"Value mismatch for remaining key: {:?}",
							String::from_utf8_lossy(remain_key)
						);
					}
					None => {
						panic!("Remaining key not found: {:?}", String::from_utf8_lossy(remain_key))
					}
				}
			}
		}
	}

	#[test]
	fn test_empty_tree() {
		let tree = create_test_tree(true);
		assert_eq!(tree.get(b"key").unwrap(), None);
	}

	#[test]
	fn test_single_insert_search() {
		let mut tree = create_test_tree(true);
		tree.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
		assert_eq!(tree.get(b"key1").unwrap().as_deref(), Some(b"value1".as_ref()));
	}

	#[test]
	fn test_update() {
		let mut tree = create_test_tree(true);

		// Insert multiple keys to ensure we're working with a leaf node
		tree.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
		tree.insert(b"key2".to_vec(), b"value2".to_vec()).unwrap();
		tree.insert(b"key3".to_vec(), b"value3".to_vec()).unwrap();

		// Update existing key in the same leaf
		tree.insert(b"key2".to_vec(), b"updated_value2".to_vec()).unwrap();

		// Verify the update
		assert_eq!(tree.get(b"key1").unwrap().as_deref(), Some(b"value1".as_ref()));
		assert_eq!(tree.get(b"key2").unwrap().as_deref(), Some(b"updated_value2".as_ref()));
		assert_eq!(tree.get(b"key3").unwrap().as_deref(), Some(b"value3".as_ref()));
	}

	#[test]
	fn test_multiple_updates_same_key() {
		let mut tree = create_test_tree(true);

		// Insert initial key
		tree.insert(b"key".to_vec(), b"value1".to_vec()).unwrap();
		assert_eq!(tree.get(b"key").unwrap().as_deref(), Some(b"value1".as_ref()));

		// Update multiple times
		tree.insert(b"key".to_vec(), b"value2".to_vec()).unwrap();
		assert_eq!(tree.get(b"key").unwrap().as_deref(), Some(b"value2".as_ref()));

		tree.insert(b"key".to_vec(), b"value3".to_vec()).unwrap();
		assert_eq!(tree.get(b"key").unwrap().as_deref(), Some(b"value3".as_ref()));

		tree.insert(b"key".to_vec(), b"final_value".to_vec()).unwrap();
		assert_eq!(tree.get(b"key").unwrap().as_deref(), Some(b"final_value".as_ref()));
	}

	#[test]
	fn test_update_no_new_nodes_created() {
		let mut tree = create_test_tree(true);

		// Insert initial keys to create some tree structure
		tree.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
		tree.insert(b"key2".to_vec(), b"value2".to_vec()).unwrap();
		tree.insert(b"key3".to_vec(), b"value3".to_vec()).unwrap();

		// Get initial tree stats
		let (height_before, nodes_before, keys_before, leaves_before) =
			tree.calculate_tree_stats().unwrap();

		// Update existing keys - should not create new nodes
		tree.insert(b"key1".to_vec(), b"updated_value1".to_vec()).unwrap();
		tree.insert(b"key2".to_vec(), b"updated_value2".to_vec()).unwrap();
		tree.insert(b"key3".to_vec(), b"updated_value3".to_vec()).unwrap();

		// Get stats after updates
		let (height_after, nodes_after, keys_after, leaves_after) =
			tree.calculate_tree_stats().unwrap();

		// Verify no new nodes were created
		assert_eq!(height_before, height_after, "Tree height should not change");
		assert_eq!(nodes_before, nodes_after, "Node count should not change");
		assert_eq!(leaves_before, leaves_after, "Leaf count should not change");
		assert_eq!(keys_before, keys_after, "Key count should not change");

		// Verify values were actually updated
		assert_eq!(tree.get(b"key1").unwrap().as_deref(), Some(b"updated_value1".as_ref()));
		assert_eq!(tree.get(b"key2").unwrap().as_deref(), Some(b"updated_value2".as_ref()));
		assert_eq!(tree.get(b"key3").unwrap().as_deref(), Some(b"updated_value3".as_ref()));
	}

	#[test]
	fn test_sequential_inserts_and_deletes() {
		let max_keys = 100;
		let mut tree = create_test_tree(true);
		// Insert enough to cause multiple splits
		for i in 0..(max_keys * 3) {
			let key = format!("key{}", i).into_bytes();
			let value = format!("value{}", i).into_bytes();
			tree.insert(key, value).unwrap();
		}

		// Verify all exist
		for i in 0..(max_keys * 3) {
			let key = format!("key{}", i).into_bytes();
			assert!(tree.get(&key).unwrap().is_some());
		}

		// Delete all in reverse order
		for i in (0..(max_keys * 3)).rev() {
			let key = format!("key{}", i).into_bytes();
			assert!(tree.delete(&key).unwrap().is_some());
		}

		// Verify all deleted
		for i in 0..(max_keys * 3) {
			let key = format!("key{}", i).into_bytes();
			assert!(tree.get(&key).unwrap().is_none());
		}
	}

	#[test]
	fn test_predecessor_successor_operations() {
		let mut tree = create_test_tree(true);

		let keys = vec![b"b".to_vec(), b"d".to_vec(), b"f".to_vec(), b"h".to_vec(), b"j".to_vec()];

		for key in &keys {
			tree.insert(key.clone(), b"value".to_vec()).unwrap();
		}

		// Delete middle key to force predecessor/successor use
		assert!(tree.delete(b"f").unwrap().is_some());

		// Verify remaining keys
		assert!(tree.get(b"b").unwrap().is_some());
		assert!(tree.get(b"d").unwrap().is_some());
		assert!(tree.get(b"h").unwrap().is_some());
		assert!(tree.get(b"j").unwrap().is_some());
	}

	#[test]
	fn test_edge_key_positions() {
		let max_keys = 100;
		let mut tree = create_test_tree(true);
		// Test first and last positions in nodes
		let mut keys = Vec::new();
		for i in 0..max_keys * 2 {
			keys.push(format!("key{:04}", i).into_bytes());
		}

		for key in &keys {
			tree.insert(key.clone(), b"value".to_vec()).unwrap();
		}

		// Delete first and last keys in sequence
		tree.delete(&keys[0]).unwrap();
		tree.delete(&keys[keys.len() - 1]).unwrap();

		// Verify deletions
		assert!(tree.get(&keys[0]).unwrap().is_none());
		assert!(tree.get(&keys[keys.len() - 1]).unwrap().is_none());
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
			let mut tree = BPlusTree::disk(path, Arc::new(TestComparator))?;
			for (key, value) in &test_data {
				tree.insert(key.clone(), value.clone())?;
			}
		} // BPlusTree is dropped here, file flushed and closed

		// Reopen and verify
		{
			let mut tree = BPlusTree::disk(path, Arc::new(TestComparator))?;

			// Verify existing data
			for (key, value) in &test_data {
				assert_eq!(
					tree.get(key)?,
					Some(value.clone()),
					"Key {:?} not found after reopening",
					String::from_utf8_lossy(key)
				);
			}

			// Verify non-existent key
			assert_eq!(tree.get(b"mango")?, None, "Non-existent key found unexpectedly");

			// Add new data and verify
			tree.insert(b"mango".to_vec(), b"orange".to_vec())?;
			assert_eq!(
				tree.get(b"mango")?,
				Some(b"orange".to_vec()),
				"New insertion failed after reopening"
			);
		}

		// Reopen again to verify new data persisted
		{
			let tree = BPlusTree::disk(path, Arc::new(TestComparator))?;
			assert_eq!(
				tree.get(b"mango")?,
				Some(b"orange".to_vec()),
				"New data didn't persist across openings"
			);
		}

		Ok(())
	}

	#[test]
	fn test_delete_persistence() -> Result<()> {
		let file = NamedTempFile::new()?;
		let path = file.path();

		// Insert test data
		{
			let mut tree = BPlusTree::disk(path, Arc::new(TestComparator))?;
			tree.insert(b"one".to_vec(), b"1".to_vec())?;
			tree.insert(b"two".to_vec(), b"2".to_vec())?;
			tree.insert(b"three".to_vec(), b"3".to_vec())?;
		}

		// Delete and verify
		{
			let mut tree = BPlusTree::disk(path, Arc::new(TestComparator))?;
			assert_eq!(tree.delete(b"two")?.as_deref(), Some(b"2".as_ref()));
			assert_eq!(tree.get(b"two")?, None);
		}

		// Reopen and verify deletion persisted
		{
			let tree = BPlusTree::disk(path, Arc::new(TestComparator))?;
			assert_eq!(tree.get(b"two")?, None, "Deleted key still exists after reopening");
			assert_eq!(
				tree.get(b"one")?,
				Some(b"1".to_vec()),
				"Existing key missing after deletion"
			);
		}

		Ok(())
	}

	#[test]
	fn test_concurrent_operations() {
		let mut tree = create_test_tree(true);
		// Insert and delete same key repeatedly
		tree.insert(b"key".to_vec(), b"v1".to_vec()).unwrap();
		tree.delete(b"key").unwrap();
		tree.insert(b"key".to_vec(), b"v2".to_vec()).unwrap();
		assert_eq!(tree.get(b"key").unwrap().as_deref(), Some(b"v2".as_ref()));
	}

	#[test]
	fn test_drop_behavior() {
		let file = NamedTempFile::new().unwrap();
		let path = file.path();

		// Create and immediately drop
		{
			let mut tree = BPlusTree::disk(path, Arc::new(TestComparator)).unwrap();
			tree.insert(b"test".to_vec(), b"value".to_vec()).unwrap();
			// Explicit drop before end of scope
			drop(tree);
		}

		// Verify data post drop
		{
			let tree = BPlusTree::disk(path, Arc::new(TestComparator)).unwrap();
			assert_eq!(tree.get(b"test").unwrap().as_deref(), Some(b"value".as_ref()));
		}
	}

	#[test]
	fn test_explicit_close() {
		let file = NamedTempFile::new().unwrap();
		let path = file.path();

		let mut tree = BPlusTree::disk(path, Arc::new(TestComparator)).unwrap();
		tree.insert(b"close".to_vec(), b"test".to_vec()).unwrap();
		tree.close().unwrap(); // Explicit close
	}

	#[test]
	fn new_file_initializes_correct_header() {
		let temp_file = NamedTempFile::new().unwrap();
		let path = temp_file.path();

		// Create new BPlusTree
		let _tree = BPlusTree::disk(path, Arc::new(TestComparator)).unwrap();

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
		buffer[8..12].copy_from_slice(&1u32.to_be_bytes());

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
		buffer[8..12].copy_from_slice(&2u32.to_be_bytes()); // Unsupported version

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
				let key = format!("key_{}", i).into_bytes();
				let value = format!("value_{}", i).into_bytes();
				(key, value)
			})
			.collect();

		// Insert data and close
		{
			let mut tree = BPlusTree::disk(path, Arc::new(TestComparator)).unwrap();
			for (key, value) in &test_data {
				tree.insert(key.clone(), value.clone()).unwrap();
			}
			tree.close().unwrap();
		}

		// Reopen and verify all items
		{
			let tree = BPlusTree::disk(path, Arc::new(TestComparator)).unwrap();
			for (key, value) in &test_data {
				assert_eq!(
					tree.get(key).unwrap(),
					Some(value.clone()),
					"Key {:?} not found after reopening",
					String::from_utf8_lossy(key)
				);
			}
		}
	}

	fn serialize_u32(n: u32) -> Vec<u8> {
		n.to_be_bytes().to_vec()
	}

	fn deserialize_pair(pair: (Key, Value)) -> (u32, u32) {
		(
			u32::from_be_bytes((&*pair.0).try_into().unwrap()),
			u32::from_be_bytes((&*pair.1).try_into().unwrap()),
		)
	}

	#[test]
	fn test_range_basic() {
		let file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(file.path(), Arc::new(U32Comparator)).unwrap();

		// Insert test data
		for i in 1..=10 {
			tree.insert(serialize_u32(i), serialize_u32(i * 10)).unwrap();
		}

		// Test range 3-7
		let results = tree.range(&serialize_u32(3), &serialize_u32(7)).unwrap();
		let expected: Vec<_> = (3..=7).map(|i| (i, i * 10)).collect();
		assert_eq!(
			results.into_iter().map(|res| deserialize_pair(res.unwrap())).collect::<Vec<_>>(),
			expected
		);
	}

	#[test]
	fn test_range_spanning_leaves() {
		let file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(file.path(), Arc::new(U32Comparator)).unwrap();

		// Insert enough data to create multiple levels
		for i in 1..=20 {
			tree.insert(serialize_u32(i), serialize_u32(i * 10)).unwrap();
		}

		// Test range 5-15
		let results = tree.range(&serialize_u32(5), &serialize_u32(15)).unwrap();
		let expected: Vec<_> = (5..=15).map(|i| (i, i * 10)).collect();
		assert_eq!(
			results.into_iter().map(|res| deserialize_pair(res.unwrap())).collect::<Vec<_>>(),
			expected
		);
	}

	#[test]
	fn test_full_range() {
		let file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(file.path(), Arc::new(U32Comparator)).unwrap();

		for i in 1..=10 {
			tree.insert(serialize_u32(i), serialize_u32(i * 10)).unwrap();
		}

		let results: Vec<_> = tree.range(&serialize_u32(1), &serialize_u32(10)).unwrap().collect();

		assert_eq!(results.len(), 10);
	}

	#[test]
	fn test_invalid_range() {
		let file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(file.path(), Arc::new(U32Comparator)).unwrap();

		for i in 1..=5 {
			tree.insert(serialize_u32(i), vec![]).unwrap();
		}

		// Start > end should return empty
		let results: Vec<_> = tree.range(&serialize_u32(3), &serialize_u32(1)).unwrap().collect();
		assert!(results.is_empty());
	}

	#[test]
	fn test_missing_boundaries() {
		let file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(file.path(), Arc::new(U32Comparator)).unwrap();

		// Insert sparse keys
		for i in (1..=10).step_by(2) {
			tree.insert(serialize_u32(i), vec![]).unwrap();
		}

		// Range covering missing start/end
		let results = tree.range(&serialize_u32(2), &serialize_u32(9)).unwrap();
		let expected = vec![3, 5, 7, 9];
		assert_eq!(
			results
				.into_iter()
				.map(|res| res.map(|(k, _)| u32::from_be_bytes((&*k).try_into().unwrap())).unwrap())
				.collect::<Vec<_>>(),
			expected
		);
	}

	#[test]
	fn test_exact_match_range() {
		let file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(file.path(), Arc::new(U32Comparator)).unwrap();

		tree.insert(serialize_u32(5), vec![]).unwrap();

		// Single key range
		let results: Vec<_> = tree.range(&serialize_u32(5), &serialize_u32(5)).unwrap().collect();
		assert_eq!(results.len(), 1);

		// Non-existent exact range
		let results: Vec<_> = tree.range(&serialize_u32(3), &serialize_u32(3)).unwrap().collect();
		assert!(results.is_empty());
	}

	#[test]
	fn test_range_after_modifications() {
		let file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(file.path(), Arc::new(U32Comparator)).unwrap();

		// Insert initial data
		for i in 1..=10 {
			tree.insert(serialize_u32(i), vec![]).unwrap();
		}

		// Delete some keys
		tree.delete(&serialize_u32(3)).unwrap();
		tree.delete(&serialize_u32(7)).unwrap();

		// Add new keys
		tree.insert(serialize_u32(12), vec![]).unwrap();
		tree.insert(serialize_u32(15), vec![]).unwrap();

		// Test range scan
		let results = tree.range(&serialize_u32(5), &serialize_u32(15)).unwrap();
		let expected = vec![5, 6, 8, 9, 10, 12, 15];
		assert_eq!(
			results
				.into_iter()
				.map(|res| res.map(|(k, _)| u32::from_be_bytes((&*k).try_into().unwrap())).unwrap())
				.collect::<Vec<_>>(),
			expected
		);
	}

	#[test]
	fn test_range() {
		let mut tree = create_test_tree(true);
		tree.insert(b"key1".to_vec(), b"value1".to_vec()).unwrap();
		tree.insert(b"key3".to_vec(), b"value3".to_vec()).unwrap();
		tree.insert(b"key2".to_vec(), b"value2".to_vec()).unwrap();

		let mut iter = tree.range(b"key2", b"key3").unwrap();
		assert_eq!(iter.next().unwrap().unwrap(), (b"key2".to_vec(), b"value2".to_vec()));
		assert_eq!(iter.next().unwrap().unwrap(), (b"key3".to_vec(), b"value3".to_vec()));
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_range_empty() {
		let mut tree = create_test_tree(true);
		tree.insert(b"a".to_vec(), b"1".to_vec()).unwrap();
		tree.insert(b"c".to_vec(), b"3".to_vec()).unwrap();
		let mut iter = tree.range(b"b", b"b").unwrap();
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_large_dataset_range() {
		let mut tree = create_test_tree(false);
		// Insert 10000 keys
		for i in 0..10000 {
			let key = format!("key_{:05}", i).into_bytes();
			let value = format!("value_{:05}", i).into_bytes();
			tree.insert(key, value).unwrap();
		}

		// Range scan from key_05009 to key_05500
		let start = b"key_05000";
		let end = b"key_05500";
		let mut iter = tree.range(start, end).unwrap();
		for i in 5000..=5500 {
			let expected_key = format!("key_{:05}", i).into_bytes();
			let expected_value = format!("value_{:05}", i).into_bytes();
			assert_eq!(iter.next().unwrap().unwrap(), (expected_key, expected_value));
		}
		assert!(iter.next().is_none());
	}

	fn key(i: u32) -> Vec<u8> {
		format!("key{:010}", i).into_bytes()
	}

	fn value(i: u32) -> Vec<u8> {
		format!("value{:010}", i).into_bytes()
	}

	#[test]
	fn test_internal_node_merge_bug() {
		let file = NamedTempFile::new().unwrap();

		let insert_count = 20000;

		{
			let mut tree = BPlusTree::disk(&file, Arc::new(BinaryComparator)).unwrap();

			// First, insert 1026+ keys to ensure we have at least 3 levels
			for i in 0..insert_count {
				tree.insert(key(i), value(i)).unwrap();
			}

			// Delete a pattern of keys that forces internal node merges
			for i in (0..insert_count).step_by(2) {
				tree.delete(&key(i)).unwrap();
			}

			// Check deleted items
			for i in (0..insert_count).step_by(2) {
				assert!(tree.get(&key(i)).unwrap().is_none(), "Deleted key {} should not exist", i);
			}

			// Check items that weren't deleted
			for i in (1..insert_count).step_by(2) {
				let result = tree.get(&key(i)).unwrap();
				assert!(result.is_some(), "Non-deleted key {} should exist", i);
				assert_eq!(result.unwrap(), value(i), "Value for key {} is incorrect", i);
			}
		}
	}

	#[test]
	fn test_trunk_page_free_list_management() {
		let file = NamedTempFile::new().unwrap();

		// Number of items to insert and then delete to create free pages
		let insert_count = 10000;

		// Create a new B+ tree
		{
			let mut tree = BPlusTree::disk(&file, Arc::new(BinaryComparator)).unwrap();

			// Step 1: Insert data
			for i in 0..insert_count {
				tree.insert(key(i), value(i)).unwrap();
			}

			// Verify initial state
			assert_eq!(tree.header.free_page_count, 0, "Should have no free pages initially");
			assert_eq!(tree.header.trunk_page_head, 0, "Should have no trunk pages initially");

			// Record number of pages before deletion
			let pages_after_insert = tree.header.total_pages;

			// Step 2: Delete some data to create free pages
			for i in (0..insert_count).step_by(2) {
				tree.delete(&key(i)).unwrap();
			}

			// Verify free pages were created and tracked
			assert!(tree.header.free_page_count > 0, "Should have free pages after deletion");
			assert!(tree.header.trunk_page_head > 0, "Should have at least one trunk page");

			// Step 3: Count trunk pages and free pages
			let mut free_pages_in_trunks = 0;
			let mut current_trunk = tree.header.trunk_page_head;

			while current_trunk != 0 {
				let trunk = tree.read_trunk_page(current_trunk).unwrap();
				free_pages_in_trunks += trunk.num_free_pages;
				current_trunk = trunk.next_trunk;
			}

			// Verify counts match
			assert_eq!(
				{ free_pages_in_trunks },
				tree.header.free_page_count,
				"Free page count in header should match actual count in trunk pages"
			);

			// Step 4: Insert new data that should reuse free pages
			let initial_free_count = tree.header.free_page_count;

			// Insert some new data - should reuse free pages
			for i in insert_count..(insert_count + 100) {
				tree.insert(key(i), value(i)).unwrap();
			}

			// Verify some free pages were reused
			assert!(
				tree.header.free_page_count < initial_free_count,
				"Some free pages should have been reused"
			);

			// Step 5: Verify total page count hasn't increased as much as it would without
			// reuse The increase in total pages should be less than the number of new
			// inserts because we're reusing free pages
			assert!(
				tree.header.total_pages - pages_after_insert < 100,
				"Should have reused pages instead of allocating all new ones"
			);

			// Step 6: Verify tree integrity by reading data

			// Check deleted items
			for i in (0..insert_count).step_by(2) {
				assert!(tree.get(&key(i)).unwrap().is_none(), "Deleted key {} should not exist", i);
			}

			// Check items that weren't deleted
			for i in (1..insert_count).step_by(2) {
				let result = tree.get(&key(i)).unwrap();
				assert!(result.is_some(), "Non-deleted key {} should exist", i);
				assert_eq!(result.unwrap(), value(i), "Value for key {} is incorrect", i);
			}

			// Check newly inserted items
			for i in insert_count..(insert_count + 100) {
				let result = tree.get(&key(i)).unwrap();
				assert!(result.is_some(), "Newly inserted key {} should exist", i);
				assert_eq!(result.unwrap(), value(i), "Value for key {} is incorrect", i);
			}

			// Step 7: Create extreme fragmentation by deleting all remaining items

			// Delete all remaining items from first batch
			for i in (1..insert_count).step_by(2) {
				tree.delete(&key(i)).unwrap();
			}

			// And delete newly inserted items
			for i in insert_count..(insert_count + 100) {
				tree.delete(&key(i)).unwrap();
			}

			// Count trunk pages again
			free_pages_in_trunks = 0;
			current_trunk = tree.header.trunk_page_head;

			while current_trunk != 0 {
				let trunk = tree.read_trunk_page(current_trunk).unwrap();
				free_pages_in_trunks += trunk.num_free_pages;
				current_trunk = trunk.next_trunk;
			}

			// Verify counts still match
			assert_eq!(
				{ free_pages_in_trunks },
				tree.header.free_page_count,
				"Final free page count in header should match actual count in trunk pages"
			);

			// Close the tree properly
			tree.close().unwrap();
		}

		// Step 8: Reopen and verify trunk pages are preserved
		{
			let mut tree = BPlusTree::disk(&file, Arc::new(BinaryComparator)).unwrap();

			// Verify free page count was preserved
			assert!(tree.header.free_page_count > 0, "Free pages should be preserved after reopen");

			// Verify trunk page chain
			assert!(tree.header.trunk_page_head > 0, "Trunk page head should be preserved");

			// Count trunk pages and verify they match the expected count from before
			let mut free_pages_in_trunks = 0;
			let mut current_trunk = tree.header.trunk_page_head;

			while current_trunk != 0 {
				let trunk = tree.read_trunk_page(current_trunk).unwrap();
				free_pages_in_trunks += trunk.num_free_pages;
				current_trunk = trunk.next_trunk;
			}

			// Verify counts still match after reopen
			assert_eq!(
				{ free_pages_in_trunks },
				tree.header.free_page_count,
				"Reopened free page count in header should match actual count in trunk pages"
			);
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
			let mut tree = create_test_tree(false);

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
				tree.insert(unique_key.clone(), fixed_value.clone()).unwrap();
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
			let mut tree = create_test_tree(false);

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
				tree.insert(unique_key.clone(), fixed_value.clone()).unwrap();

				// Verify the key was inserted correctly
				let result = tree.get(&unique_key).unwrap();
				assert!(
					result.is_some(),
					"Failed to retrieve just-inserted key of size {}",
					key_size
				);
				assert_eq!(result.unwrap(), fixed_value);
			}

			// Delete every other key (evens)
			for (idx, key) in all_keys.iter().enumerate() {
				if idx % 2 == 0 {
					// Delete even-indexed keys
					let result = tree.delete(key).unwrap();
					assert!(
						result.is_some(),
						"Failed to delete key of size {} at index {}",
						key_size,
						idx
					);
					assert_eq!(result.unwrap(), fixed_value);

					// Verify the key no longer exists
					assert!(
						tree.get(key).unwrap().is_none(),
						"Key of size {} still exists after deletion",
						key_size
					);
				}
			}

			// Verify remaining odd-indexed keys still exist
			for (idx, key) in all_keys.iter().enumerate() {
				if idx % 2 == 1 {
					let result = tree.get(key).unwrap();
					assert!(
						result.is_some(),
						"Odd-indexed key of size {} missing after deletion of even keys",
						key_size
					);
					assert_eq!(result.unwrap(), fixed_value);
				}
			}

			// After each run, flush and close the tree
			tree.flush().unwrap();
			tree.close().unwrap();
		}
	}

	// Long sequence of insertions and deletions to stress test size handling
	#[test]
	fn test_insertion_deletion_sequence() {
		let mut tree = create_test_tree(false);

		// Use deterministic random for reproducibility
		let mut rng = StdRng::seed_from_u64(123);

		// Track all inserted keys
		let mut all_keys = vec![];
		let mut active_keys = std::collections::HashSet::new();

		let num_initial = 100;

		for i in 0..num_initial {
			// Create key with sequence number and random size
			let key_size = rng.random_range(10..400);
			let mut key = format!("key_{:05}_", i).into_bytes();
			key.extend(vec![b'k'; key_size - key.len()]);

			let value_size = rng.random_range(10..200);
			let value = vec![b'v'; value_size];

			// Insert and track
			tree.insert(key.clone(), value.clone()).unwrap();
			all_keys.push((key.clone(), value));
			active_keys.insert(key);

			// Periodically flush
			if i % 20 == 19 {
				tree.flush().unwrap();
			}
		}

		let num_deletions = 40;

		let mut keys_to_delete = active_keys.iter().cloned().collect::<Vec<_>>();
		for _ in 0..num_deletions {
			if keys_to_delete.is_empty() {
				break;
			}

			let idx = rng.random_range(0..keys_to_delete.len());
			let key = keys_to_delete.swap_remove(idx);

			tree.delete(&key).unwrap();
			active_keys.remove(&key);

			// Periodic flush
			if rng.random_bool(0.2) {
				tree.flush().unwrap();
			}
		}

		let num_additional = 30;

		for i in 0..num_additional {
			// Every 3rd key is very large
			let key_size = if i % 3 == 0 {
				rng.random_range(1000..2000)
			} else {
				rng.random_range(10..200)
			};

			let mut key = format!("additional_{:05}_", i).into_bytes();
			key.extend(vec![b'k'; key_size - key.len()]);

			let value_size = rng.random_range(10..100);
			let value = vec![b'v'; value_size];

			// Insert and track
			tree.insert(key.clone(), value.clone()).unwrap();
			all_keys.push((key.clone(), value));
			active_keys.insert(key);

			// Periodic flush
			if i % 10 == 9 {
				tree.flush().unwrap();
			}
		}

		// Final flush
		tree.flush().unwrap();

		// Verify all active keys can be found
		for key in &active_keys {
			let expected_value =
				all_keys.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone()).unwrap();

			let retrieved = tree.get(key).unwrap();
			assert!(retrieved.is_some(), "Active key of size {} not found", key.len());
			assert_eq!(retrieved.unwrap(), expected_value);
		}

		// Verify deleted keys no longer exist
		for (key, _) in &all_keys {
			if !active_keys.contains(key) {
				assert!(
					tree.get(key).unwrap().is_none(),
					"Deleted key of size {} still exists",
					key.len()
				);
			}
		}
	}

	#[test]
	fn test_allocate_new_page_when_no_free_pages() {
		let mut btree = create_test_tree(true);

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

	#[test]
	fn test_allocate_page_from_trunk() {
		let mut btree = create_test_tree(true);

		// Set up initial state with one trunk page containing free pages
		let trunk_offset = PAGE_SIZE as u64; // Trunk at page 1
		btree.header.trunk_page_head = trunk_offset;
		btree.header.free_page_count = 3;
		btree.header.total_pages = 5; // Header + trunk + 3 free pages

		// Create a trunk page with 3 free pages
		let mut trunk = TrunkPage::new(trunk_offset);
		trunk.add_free_page(2 * PAGE_SIZE as u64); // Free page 2
		trunk.add_free_page(3 * PAGE_SIZE as u64); // Free page 3
		trunk.add_free_page(4 * PAGE_SIZE as u64); // Free page 4

		// Write the trunk page to the "database"
		btree.write_trunk_page(&trunk).unwrap();

		// Allocate a page
		let page_offset = btree.allocate_page().unwrap();

		// Verify we got the first free page from the trunk
		assert_eq!(page_offset, 4 * PAGE_SIZE as u64); // Pages are popped in reverse order
		assert_eq!(btree.header.free_page_count, 2); // One less free page

		// Allocate another page
		let page_offset = btree.allocate_page().unwrap();

		// Verify we got the second free page from the trunk
		assert_eq!(page_offset, 3 * PAGE_SIZE as u64);
		assert_eq!(btree.header.free_page_count, 1);

		// Allocate the last free page
		let page_offset = btree.allocate_page().unwrap();

		// Verify we got the last free page from the trunk
		assert_eq!(page_offset, 2 * PAGE_SIZE as u64);
		assert_eq!(btree.header.free_page_count, 0);

		// One more allocation should create a new page since the trunk is empty
		let page_offset = btree.allocate_page().unwrap();
		assert_eq!(page_offset, 5 * PAGE_SIZE as u64);
		assert_eq!(btree.header.total_pages, 6);
	}

	#[test]
	fn test_repurpose_empty_trunk() {
		let mut btree = create_test_tree(true);

		// Set up initial state with two trunk pages
		// The first trunk is empty, the second has free pages
		let trunk1_offset = PAGE_SIZE as u64; // Trunk1 at page 1
		let trunk2_offset = 2 * PAGE_SIZE as u64; // Trunk2 at page 2

		btree.header.trunk_page_head = trunk1_offset;
		btree.header.free_page_count = 2;
		btree.header.total_pages = 5; // Header + 2 trunks + 2 free pages

		// Create first trunk (empty) pointing to second trunk
		let mut trunk1 = TrunkPage::new(trunk1_offset);
		trunk1.next_trunk = trunk2_offset;
		btree.write_trunk_page(&trunk1).unwrap();

		// Create second trunk with 2 free pages
		let mut trunk2 = TrunkPage::new(trunk2_offset);
		trunk2.add_free_page(3 * PAGE_SIZE as u64); // Free page 3
		trunk2.add_free_page(4 * PAGE_SIZE as u64); // Free page 4
		btree.write_trunk_page(&trunk2).unwrap();

		// Allocate a page - should repurpose the empty first trunk
		let page_offset = btree.allocate_page().unwrap();

		// Verify we got the empty trunk1 page
		assert_eq!(page_offset, trunk1_offset);

		// Verify the trunk chain now starts at trunk2
		assert_eq!(btree.header.trunk_page_head, trunk2_offset);

		// Free page count should still be 2 since we didn't use a free page
		assert_eq!(btree.header.free_page_count, 2);

		// Next allocation should get a free page from trunk2
		let page_offset = btree.allocate_page().unwrap();
		assert_eq!(page_offset, 4 * PAGE_SIZE as u64);
		assert_eq!(btree.header.free_page_count, 1);
	}

	#[test]
	fn test_multiple_trunk_pages_chain() {
		let mut btree = create_test_tree(true);

		// Set up a chain of 3 trunk pages, each with one free page
		let trunk1_offset = PAGE_SIZE as u64; // Trunk1 at page 1
		let trunk2_offset = 2 * PAGE_SIZE as u64; // Trunk2 at page 2
		let trunk3_offset = 3 * PAGE_SIZE as u64; // Trunk3 at page 3

		btree.header.trunk_page_head = trunk1_offset;
		btree.header.free_page_count = 3;
		btree.header.total_pages = 7; // Header + 3 trunks + 3 free pages

		// Create trunk1 with one free page, pointing to trunk2
		let mut trunk1 = TrunkPage::new(trunk1_offset);
		trunk1.add_free_page(4 * PAGE_SIZE as u64); // Free page 4
		trunk1.next_trunk = trunk2_offset;
		btree.write_trunk_page(&trunk1).unwrap();

		// Create trunk2 with one free page, pointing to trunk3
		let mut trunk2 = TrunkPage::new(trunk2_offset);
		trunk2.add_free_page(5 * PAGE_SIZE as u64); // Free page 5
		trunk2.next_trunk = trunk3_offset;
		btree.write_trunk_page(&trunk2).unwrap();

		// Create trunk3 with one free page
		let mut trunk3 = TrunkPage::new(trunk3_offset);
		trunk3.add_free_page(6 * PAGE_SIZE as u64); // Free page 6
		btree.write_trunk_page(&trunk3).unwrap();

		// Allocate a page - should come from trunk1
		let page_offset = btree.allocate_page().unwrap();
		assert_eq!(page_offset, 4 * PAGE_SIZE as u64);
		assert_eq!(btree.header.free_page_count, 2);

		// Now trunk1 is empty but has a next trunk
		// Next allocation should come from trunk2
		let page_offset = btree.allocate_page().unwrap();

		assert_eq!(page_offset, trunk1_offset);
	}

	#[test]
	fn test_free_page_basic() {
		let mut btree = create_test_tree(true);

		// Start with no free pages
		btree.header.trunk_page_head = 0;
		btree.header.free_page_count = 0;
		btree.header.total_pages = 4; // Header + 3 allocated pages

		// Free page 2 (instead of page 1)
		btree.free_page(2 * PAGE_SIZE as u64).unwrap();

		// Page 2 should now be a trunk page
		assert_eq!(btree.header.trunk_page_head, 2 * PAGE_SIZE as u64);
		assert_eq!(btree.header.free_page_count, 0); // Still 0 because page 2 became a trunk

		// Now free page 3
		btree.free_page(3 * PAGE_SIZE as u64).unwrap();

		// Page 3 should be on the free list, tracked by trunk page 2
		assert_eq!(btree.header.free_page_count, 1);

		// Verify by allocating a page - should get page 3
		let page_offset = btree.allocate_page().unwrap();
		assert_eq!(page_offset, 3 * PAGE_SIZE as u64);
		assert_eq!(btree.header.free_page_count, 0);
	}

	#[test]
	fn test_free_page_multiple() {
		let mut btree = create_test_tree(true);

		// Start with no free pages
		btree.header.trunk_page_head = 0;
		btree.header.free_page_count = 0;
		btree.header.total_pages = 10; // Header + 9 allocated pages

		// Free multiple pages (start from page 2 instead of page 1)
		for i in 2..10 {
			btree.free_page(i * PAGE_SIZE as u64).unwrap();
		}

		// First freed page (page 2) becomes the trunk
		assert_eq!(btree.header.trunk_page_head, 2 * PAGE_SIZE as u64);

		// Should have 7 pages on the free list (8 freed - 1 trunk)
		assert_eq!(btree.header.free_page_count, 7);

		// Allocate all pages back
		for _ in 0..7 {
			let _ = btree.allocate_page().unwrap();
		}

		// All free pages should be used up
		assert_eq!(btree.header.free_page_count, 0);

		// Next allocation should create a new page at the end of the file
		let page_offset = btree.allocate_page().unwrap();
		assert_eq!(page_offset, 10 * PAGE_SIZE as u64);
		assert_eq!(btree.header.total_pages, 11);

		// Trunk page head should still be at page 2
		assert_eq!(btree.header.trunk_page_head, 2 * PAGE_SIZE as u64);
	}

	#[test]
	fn test_allocate_with_inconsistent_count() {
		let mut btree = create_test_tree(true);

		// Create inconsistent state - free_page_count > 0 but no actual free pages
		btree.header.trunk_page_head = PAGE_SIZE as u64; // Trunk at page 1
		btree.header.free_page_count = 5; // Claim 5 free pages
		btree.header.total_pages = 2; // Header + trunk

		// Create empty trunk with no next trunk
		let trunk = TrunkPage::new(PAGE_SIZE as u64);
		btree.write_trunk_page(&trunk).unwrap();

		// Try to allocate - should detect corruption
		let result = btree.allocate_page();
		assert!(result.is_err());

		// Verify the error is an inconsistent free page count error
		match result {
			Err(BPlusTreeError::InconsistentFreePageCount {
				..
			}) => (),
			_ => panic!("Expected InconsistentFreePageCount error, got {:?}", result),
		}
	}

	#[test]
	fn test_free_then_allocate_cycle() {
		let mut btree = create_test_tree(true);

		// Start with 5 pages (header + 4 data pages)
		btree.header.trunk_page_head = 0;
		btree.header.free_page_count = 0;
		btree.header.total_pages = 5;

		// Free pages 2 and 3
		btree.free_page(2 * PAGE_SIZE as u64).unwrap();
		btree.free_page(3 * PAGE_SIZE as u64).unwrap();

		// Page 2 should be a trunk, page 3 should be on free list
		assert_eq!(btree.header.trunk_page_head, 2 * PAGE_SIZE as u64);
		assert_eq!(btree.header.free_page_count, 1);

		// Allocate a page - should get page 3
		let page_offset = btree.allocate_page().unwrap();
		assert_eq!(page_offset, 3 * PAGE_SIZE as u64);
		assert_eq!(btree.header.free_page_count, 0);

		// Free page 4
		btree.free_page(4 * PAGE_SIZE as u64).unwrap();

		// Page 4 should now be on the free list
		assert_eq!(btree.header.free_page_count, 1);

		// Allocate again - should get page 4
		let page_offset = btree.allocate_page().unwrap();
		assert_eq!(page_offset, 4 * PAGE_SIZE as u64);
		assert_eq!(btree.header.free_page_count, 0);

		// One more allocation should create a new page
		let page_offset = btree.allocate_page().unwrap();
		assert_eq!(page_offset, 5 * PAGE_SIZE as u64);
		assert_eq!(btree.header.total_pages, 6);

		// Trunk page at page 2 should still exist (but be empty)
		assert_eq!(btree.header.trunk_page_head, 2 * PAGE_SIZE as u64);

		// One final allocation should create a new page
		let page_offset = btree.allocate_page().unwrap();
		assert_eq!(page_offset, 6 * PAGE_SIZE as u64); // This should be page 6, not 5 again
		assert_eq!(btree.header.total_pages, 7); // Total pages should now be 7
	}

	#[test]
	fn test_no_overflow_page_leak() {
		// Deterministic test: Repeating identical operations should use identical pages
		// If cycle 2 uses more pages than cycle 1, there's a leak
		let file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(file.path(), Arc::new(TestComparator)).unwrap();

		let large_key_size = 2000; // Requires overflow in internal nodes
		let test_count = 100;

		for i in 0..test_count {
			let key = vec![i as u8; large_key_size];
			let value = vec![i as u8; large_key_size];
			tree.insert(key, value).unwrap();
		}

		for i in 0..test_count {
			let key = vec![i as u8; large_key_size];
			tree.delete(&key).unwrap();
		}

		let pages_after_cycle1 = tree.header.total_pages;
		let free_after_cycle1 = tree.header.free_page_count;

		for i in 0..test_count {
			let key = vec![i as u8; large_key_size];
			let value = vec![i as u8; large_key_size];
			tree.insert(key, value).unwrap();
		}

		for i in 0..test_count {
			let key = vec![i as u8; large_key_size];
			tree.delete(&key).unwrap();
		}

		let pages_after_cycle2 = tree.header.total_pages;
		let free_after_cycle2 = tree.header.free_page_count;

		let in_use_cycle1 = pages_after_cycle1 - free_after_cycle1 as u64;
		let in_use_cycle2 = pages_after_cycle2 - free_after_cycle2 as u64;

		assert_eq!(
			in_use_cycle2, in_use_cycle1,
			"Page leak detected! Cycle 1: {} in use, Cycle 2: {} in use",
			in_use_cycle1, in_use_cycle2
		);

		tree.close().unwrap();
	}

	#[test]
	fn test_sequential_split_ordering_guarantee() {
		// Test that sequential split algorithm ensures right page <= left page
		// (ordering guarantee)
		let mut tree = create_test_tree(true);

		// Insert entries that will cause multiple splits
		// Use varying sizes to test the ordering guarantee
		for i in 0..100 {
			let key = format!("key{:04}", i).into_bytes();
			let value = vec![i as u8; 100]; // Fixed size values
			tree.insert(key, value).unwrap();
		}

		// Verify all entries can be retrieved (ensures splits worked correctly)
		for i in 0..100 {
			let key = format!("key{:04}", i).into_bytes();
			let result = tree.get(&key).unwrap();
			assert!(result.is_some(), "Key {} should exist", i);
			assert_eq!(result.unwrap(), vec![i as u8; 100]);
		}

		// The ordering guarantee is enforced internally by the split algorithm
		// If splits didn't maintain ordering, we would see panics or incorrect
		// behavior This test verifies the algorithm works correctly through
		// successful operations
	}

	#[test]
	fn test_sequential_split_no_recursion() {
		// Test that sequential split processes entries left-to-right without recursion
		let mut tree = create_test_tree(true);

		// Insert entries with sizes that would cause recursion in the old algorithm
		// The new sequential algorithm should handle this without issues
		let mut keys = Vec::new();
		for i in 0..50 {
			// Vary sizes to create challenging split scenarios
			let size = if i % 3 == 0 {
				200 // Larger entries
			} else if i % 3 == 1 {
				50 // Medium entries
			} else {
				10 // Small entries
			};
			let key = format!("key{:04}", i).into_bytes();
			let value = vec![i as u8; size];
			keys.push((key.clone(), value.clone()));
			tree.insert(key, value).unwrap();
		}

		// Verify all entries are still accessible after splits
		for (key, expected_value) in &keys {
			let result = tree.get(key).unwrap();
			assert!(result.is_some(), "Key {:?} should exist", key);
			assert_eq!(&result.unwrap(), expected_value);
		}
	}
}
