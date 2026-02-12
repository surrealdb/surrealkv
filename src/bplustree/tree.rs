use std::cmp::Ordering;
use std::fs::File;
use std::io;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use quick_cache::sync::Cache;
use quick_cache::Weighter;

use crate::vfs::File as VfsFile;
use crate::{Comparator, InternalKeyRef, LSMIterator};

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
const KEY_SIZE_PREFIX: usize = 4; // 4 bytes for key length
const VALUE_SIZE_PREFIX: usize = 4; // 4 bytes for value length
const CHILD_PTR_SIZE: usize = 8; // 8 bytes per child pointer
/// Size of an overflow pointer
const OVERFLOW_PTR_SIZE: usize = 8;
// Overflow page: 1 (type) + 8 (next_ptr) + 4 (data_len) + data
const OVERFLOW_PAGE_CAPACITY: usize = PAGE_SIZE - 13; // = 4083

// Pre-computed constants (no runtime calculation needed)
const LEAF_USABLE: usize = PAGE_SIZE - LEAF_HEADER_SIZE;
const INTERNAL_USABLE: usize = PAGE_SIZE - INTERNAL_HEADER_SIZE;

const LEAF_MIN_LOCAL: usize = ((LEAF_USABLE - 12) * 32 / 255) - 23;
const LEAF_MAX_LOCAL: usize = ((LEAF_USABLE - 12) * 64 / 255) - 23;
const INTERNAL_MIN_LOCAL: usize = ((INTERNAL_USABLE - 12) * 32 / 255) - 23;
const INTERNAL_MAX_LOCAL: usize = ((INTERNAL_USABLE - 12) * 64 / 255) - 23;

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
	keys: Vec<Bytes>, // Full keys (reconstructed from page + overflow if needed)
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
		F: Fn(u64) -> Result<Bytes>,
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

		let mut keys = Vec::with_capacity(num_keys);
		let mut key_overflows = Vec::with_capacity(num_keys);

		for _ in 0..num_keys {
			// Read total key length
			let key_len_total = u32::from_be_bytes(buffer_slice[..4].try_into().unwrap()) as usize;
			buffer_slice = &buffer_slice[4..];

			// Calculate bytes on page
			let (bytes_on_page, needs_overflow) =
				calculate_overflow(key_len_total, min_local, max_local);

			if bytes_on_page > buffer_slice.len() {
				return Err(BPlusTreeError::Deserialization(format!(
					"Calculated bytes on page {} exceeds available buffer size {}",
					bytes_on_page,
					buffer_slice.len()
				)));
			}

			// Read key data from page
			let mut key_data = BytesMut::from(&buffer_slice[..bytes_on_page]);
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

			keys.push(key_data.freeze());
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
	fn remove_key_with_overflow(&mut self, idx: usize) -> Option<(Bytes, u64)> {
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
	fn extract_parent_key(parent: &mut InternalNode, idx: usize) -> (Bytes, u64) {
		let key = parent.keys[idx].clone();
		let overflow = parent.get_overflow_at(idx);
		(key, overflow)
	}

	/// Insert a key and child pointer with explicit overflow metadata
	fn insert_key_child_with_overflow(
		&mut self,
		key: Bytes,
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
	fn extract_key_with_overflow(&mut self, idx: usize) -> Option<(Bytes, u64)> {
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
	fn insert_key_with_overflow(&mut self, idx: usize, key: Bytes, overflow: u64) {
		self.keys.insert(idx, key);
		self.key_overflows.insert(idx, overflow);
	}

	fn redistribute_to_right(
		&mut self,
		right: &mut InternalNode,
		parent_key: Bytes,
		parent_overflow: u64,
	) -> (Bytes, u64) {
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
		parent_key: Bytes,
		parent_overflow: u64,
	) -> (Bytes, u64) {
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
		separator: Bytes,
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
		self.current_size() * 100 < Self::max_size() * 35
	}

	fn calculate_size_and_max_key(&self) -> (usize, usize) {
		let (min_local, max_local) = calculate_local_limits(false);
		let mut size = INTERNAL_HEADER_SIZE + self.children.len() * CHILD_PTR_SIZE;
		let mut max_key_len = 0;

		for key in &self.keys {
			let key_len = key.len();
			max_key_len = max_key_len.max(key_len);

			let (key_bytes_on_page, needs_overflow) =
				calculate_overflow(key_len, min_local, max_local);
			size += KEY_SIZE_PREFIX + key_bytes_on_page + overflow_ptr_size(needs_overflow);
		}

		(size, max_key_len)
	}

	fn can_merge_with(&self, other: &Self, separator: &Bytes) -> bool {
		let (self_size, _) = self.calculate_size_and_max_key();
		let (other_size, _) = other.calculate_size_and_max_key();

		let combined_size = self_size + other_size;
		let actual_merged_size = combined_size - INTERNAL_HEADER_SIZE;

		let (min_local, max_local) = calculate_local_limits(false);
		let (separator_on_page, needs_overflow) =
			calculate_overflow(separator.len(), min_local, max_local);
		let separator_size =
			KEY_SIZE_PREFIX + separator_on_page + overflow_ptr_size(needs_overflow);

		let with_separator = actual_merged_size + separator_size;
		with_separator <= Self::max_size()
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

		// 3. Serialize keys with overflow support
		for (i, key) in self.keys.iter().enumerate() {
			let key_len_total = key.len();

			// Determine how much to store on page
			let (bytes_on_page, needs_overflow) =
				calculate_overflow(key_len_total, min_local, max_local);

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

		// Size for all keys accounting for overflow
		for key in &self.keys {
			let (key_bytes_on_page, needs_overflow) =
				calculate_overflow(key.len(), min_local, max_local);
			size += KEY_SIZE_PREFIX + key_bytes_on_page;
			size += overflow_ptr_size(needs_overflow);
		}

		// Size for all child pointers
		size += self.children.len() * CHILD_PTR_SIZE;

		size
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
	keys: Vec<Bytes>,   // Full keys (reconstructed from page + overflow if needed)
	values: Vec<Bytes>, // Full values (reconstructed from page + overflow if needed)
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
		F: Fn(u64) -> Result<Bytes>,
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
				calculate_overflow(payload_len, min_local, max_local);

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
				let mut cell_data = BytesMut::with_capacity(payload_len);
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
				let key_data = cell_data.split_to(key_len).freeze();
				let value_data = cell_data.freeze();

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
				let key_data =
					Bytes::copy_from_slice(&buffer_bytes[cell_start..cell_start + key_len]);
				let value_data =
					Bytes::copy_from_slice(&buffer_bytes[cell_start + key_len..cell_end]);

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
	fn insert(
		&mut self,
		key: Bytes,
		value: Bytes,
		compare: &dyn Comparator,
	) -> (usize, Option<u64>) {
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
	fn delete(&mut self, key: &[u8], compare: &dyn Comparator) -> Option<(usize, Bytes, u64)> {
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
	fn insert_cell_with_overflow(&mut self, idx: usize, key: Bytes, value: Bytes, overflow: u64) {
		self.keys.insert(idx, key);
		self.values.insert(idx, value);
		self.cell_overflows.insert(idx, overflow);
	}

	// Redistributes keys from this leaf to the target leaf
	fn redistribute_to_right(&mut self, right: &mut LeafNode) -> Bytes {
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
	fn take_from_right(&mut self, right: &mut LeafNode) -> Bytes {
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

	// Check if this leaf can fit another key-value pair
	fn can_fit_entry(&self, key: &[u8], value: &[u8]) -> bool {
		self.current_size() + leaf_cell_size(key, value) <= PAGE_SIZE
	}

	// Check if node is considered to be in underflow state
	fn is_underflow(&self) -> bool {
		self.current_size() * 100 < Self::max_size() * 35
	}

	fn can_merge_with(&self, other: &Self) -> bool {
		let combined_size = self.current_size() + other.current_size();
		let actual_merged_size = combined_size - LEAF_HEADER_SIZE;

		actual_merged_size <= Self::max_size()
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
				calculate_overflow(payload_len, min_local, max_local);

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

		// Size for all cells accounting for overflow
		for (key, value) in self.keys.iter().zip(&self.values) {
			let payload_len = key.len() + value.len();
			let (bytes_on_page, needs_overflow) =
				calculate_overflow(payload_len, min_local, max_local);

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
	data: Bytes,        // Payload data
	offset: u64,        // Offset of this overflow page
}

impl OverflowPage {
	fn new(offset: u64) -> Self {
		OverflowPage {
			next_overflow: 0,
			data: Bytes::new(),
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
		let data = Bytes::copy_from_slice(&buffer[13..13 + data_len]);

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

/// This ensures at least 4 cells fit per page, making splits always possible
fn calculate_local_limits(is_leaf: bool) -> (usize, usize) {
	if is_leaf {
		(LEAF_MIN_LOCAL, LEAF_MAX_LOCAL)
	} else {
		(INTERNAL_MIN_LOCAL, INTERNAL_MAX_LOCAL)
	}
}

/// Calculate how much of payload to store on page
fn calculate_overflow(payload_size: usize, min_local: usize, max_local: usize) -> (usize, bool) {
	if payload_size <= max_local {
		(payload_size, false)
	} else {
		// SQLite's K formula: minimize wasted space on last overflow page
		let surplus = min_local + (payload_size - min_local) % OVERFLOW_PAGE_CAPACITY;
		let bytes_on_page = if surplus <= max_local {
			surplus
		} else {
			min_local
		};
		(bytes_on_page, true)
	}
}

/// Calculate on-page size for a leaf cell
fn leaf_cell_size(key: &[u8], value: &[u8]) -> usize {
	let (min_local, max_local) = calculate_local_limits(true);
	let payload_len = key.len() + value.len();
	let (bytes_on_page, needs_overflow) = calculate_overflow(payload_len, min_local, max_local);

	KEY_SIZE_PREFIX
		+ VALUE_SIZE_PREFIX
		+ bytes_on_page
		+ if needs_overflow {
			8
		} else {
			0
		}
}

/// Calculate on-page size for an internal entry
fn internal_entry_size(key: &[u8]) -> usize {
	let (min_local, max_local) = calculate_local_limits(false);
	let (bytes_on_page, needs_overflow) = calculate_overflow(key.len(), min_local, max_local);

	KEY_SIZE_PREFIX
		+ bytes_on_page
		+ if needs_overflow {
			8
		} else {
			0
		}
}

/// Size of overflow pointer if needed
const fn overflow_ptr_size(needs_overflow: bool) -> usize {
	if needs_overflow {
		OVERFLOW_PTR_SIZE
	} else {
		0
	}
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

	pub fn insert(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
		let key = Bytes::copy_from_slice(key.as_ref());
		let value = Bytes::copy_from_slice(value.as_ref());
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
		mut promoted_key: Bytes,
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
					let (key_on_page, key_overflow) =
						calculate_overflow(promoted_key.len(), min_local, max_local);
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

	fn insert_into_leaf(&mut self, leaf: &mut LeafNode, key: Bytes, value: Bytes) -> Result<()> {
		let (_idx, old_overflow) = leaf.insert(key, value, self.compare.as_ref());
		if let Some(overflow) = old_overflow {
			self.free_overflow_chain(overflow)?;
		}
		let leaf_owned = std::mem::replace(leaf, LeafNode::new(leaf.offset));
		self.write_node_owned(NodeType::Leaf(leaf_owned))?;
		Ok(())
	}

	#[allow(unused)]
	pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
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

	pub fn delete(&mut self, key: &[u8]) -> Result<Option<Bytes>> {
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
				if let NodeType::Internal(left_node) = left_arc.as_ref() {
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
			} else if let NodeType::Leaf(left_node) = left_arc.as_ref() {
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

		// Try to borrow from right sibling if left redistribution wasn't possible
		if let Some(ref right_arc) = right_sibling_node {
			if is_internal {
				if let NodeType::Internal(right_node) = right_arc.as_ref() {
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
			} else if let NodeType::Leaf(right_node) = right_arc.as_ref() {
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

		// If we reach here, redistribution wasn't possible, we need to merge nodes

		// Prefer merging with left sibling if possible
		if let Some(left_arc) = left_sibling_node {
			let left_idx = child_idx - 1;
			if is_internal {
				let mut left_node_mut = self.extract_internal_mut(left_arc);
				let right_node_mut = self.read_internal_node(parent.children[child_idx])?;

				// FIXED: Pass actual separator to can_merge_with
				let separator = &parent.keys[left_idx];
				if left_node_mut.can_merge_with(&right_node_mut, separator) {
					self.merge_internal_nodes(
						parent,
						left_idx,
						child_idx,
						&mut left_node_mut,
						right_node_mut,
					)?;
				}
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
			return Ok(());
		}

		// Otherwise merge with right sibling
		if let Some(right_arc) = right_sibling_node {
			let right_idx = child_idx + 1;
			if is_internal {
				let mut left_node_mut = self.read_internal_node(parent.children[child_idx])?;
				let right_node_mut = self.extract_internal_mut(right_arc);

				// FIXED: Pass actual separator to can_merge_with
				// Separator between child_idx and right_idx is at parent.keys[child_idx]
				let separator = &parent.keys[child_idx];
				if left_node_mut.can_merge_with(&right_node_mut, separator) {
					self.merge_internal_nodes(
						parent,
						child_idx,
						right_idx,
						&mut left_node_mut,
						right_node_mut,
					)?;
				}
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
		if left_node.keys.is_empty() {
			return Err(BPlusTreeError::Serialization(
				"Left internal node is unexpectedly empty during redistribution".into(),
			));
		}

		// What left will lose (last key + child)
		let last_key = left_node.keys.last().unwrap();
		let left_loses = internal_entry_size(last_key) + CHILD_PTR_SIZE;

		// What right will gain (parent key + child)
		let parent_key = &parent.keys[left_idx];
		let right_gains = internal_entry_size(parent_key) + CHILD_PTR_SIZE;

		let left_size = left_node.current_size();
		let right_size = right_node.current_size();

		let left_after = left_size - left_loses;
		let right_after = right_size + right_gains;

		// Validate both child nodes fit
		if right_after > PAGE_SIZE || left_after > PAGE_SIZE {
			return Ok(());
		}

		// CRITICAL: Validate parent size change
		// Old parent key goes down, left's last key goes up
		let old_parent_entry_size = internal_entry_size(parent_key);
		let new_parent_entry_size = internal_entry_size(last_key);
		let parent_after = parent.current_size() - old_parent_entry_size + new_parent_entry_size;

		if parent_after > PAGE_SIZE {
			return Ok(()); // Skip redistribution, would overflow parent
		}

		// Check if redistribution improves balance
		let before_diff = (left_size as i64 - right_size as i64).abs();
		let after_diff = (left_after as i64 - right_after as i64).abs();

		if after_diff >= before_diff {
			return Ok(());
		}

		// Proceed with redistribution
		let (parent_key, parent_overflow) = InternalNode::extract_parent_key(parent, left_idx);
		let (new_parent_key, new_parent_overflow) =
			left_node.redistribute_to_right(right_node, parent_key, parent_overflow);

		parent.keys[left_idx] = new_parent_key;
		parent.set_overflow_at(left_idx, new_parent_overflow);

		self.write_node_owned(NodeType::Internal(left_node.clone()))?;
		self.write_node_owned(NodeType::Internal(right_node.clone()))?;

		Ok(())
	}

	fn redistribute_internal_from_right(
		&mut self,
		parent: &mut InternalNode,
		left_idx: usize,
		left_node: &mut InternalNode,
		right_node: &mut InternalNode,
	) -> Result<()> {
		if right_node.keys.is_empty() {
			return Err(BPlusTreeError::Serialization(
				"Right internal node is unexpectedly empty during redistribution".into(),
			));
		}

		// What right will lose (first key + child)
		let first_key = &right_node.keys[0];
		let right_loses = internal_entry_size(first_key) + CHILD_PTR_SIZE;

		// What left will gain (parent key + child)
		let parent_key = &parent.keys[left_idx];
		let left_gains = internal_entry_size(parent_key) + CHILD_PTR_SIZE;

		let left_size = left_node.current_size();
		let right_size = right_node.current_size();

		let left_after = left_size + left_gains;
		let right_after = right_size - right_loses;

		// Validate both child nodes fit
		if left_after > PAGE_SIZE || right_after > PAGE_SIZE {
			return Ok(());
		}

		// CRITICAL: Validate parent size change
		// Old parent key goes down, right's first key goes up
		let old_parent_entry_size = internal_entry_size(parent_key);
		let new_parent_entry_size = internal_entry_size(first_key);
		let parent_after = parent.current_size() - old_parent_entry_size + new_parent_entry_size;

		if parent_after > PAGE_SIZE {
			return Ok(()); // Skip redistribution, would overflow parent
		}

		// Check if redistribution improves balance
		let before_diff = (left_size as i64 - right_size as i64).abs();
		let after_diff = (left_after as i64 - right_after as i64).abs();

		if after_diff >= before_diff {
			return Ok(());
		}

		// Proceed with redistribution
		let (parent_key, parent_overflow) = InternalNode::extract_parent_key(parent, left_idx);
		let (new_parent_key, new_parent_overflow) =
			left_node.take_from_right(right_node, parent_key, parent_overflow);

		parent.keys[left_idx] = new_parent_key;
		parent.set_overflow_at(left_idx, new_parent_overflow);

		self.write_node_owned(NodeType::Internal(left_node.clone()))?;
		self.write_node_owned(NodeType::Internal(right_node.clone()))?;

		Ok(())
	}

	fn redistribute_leaf_from_left(
		&mut self,
		parent: &mut InternalNode,
		left_idx: usize,
		left_node: &mut LeafNode,
		right_node: &mut LeafNode,
	) -> Result<()> {
		if left_node.keys.is_empty() {
			return Err(BPlusTreeError::Serialization(
				"Left leaf node is unexpectedly empty during redistribution".into(),
			));
		}

		let last_idx = left_node.keys.len() - 1;
		let last_key = &left_node.keys[last_idx];
		let last_value = &left_node.values[last_idx];
		let last_entry_size = leaf_cell_size(last_key, last_value);

		let left_size = left_node.current_size();
		let right_size = right_node.current_size();

		// Check leaf nodes would fit
		let left_after = left_size - last_entry_size;
		let right_after = right_size + last_entry_size;

		if left_after > PAGE_SIZE || right_after > PAGE_SIZE {
			return Ok(());
		}

		// CRITICAL: Check parent can accommodate new separator
		let old_sep = &parent.keys[left_idx];
		let old_sep_entry_size = internal_entry_size(old_sep);
		let new_sep_entry_size = internal_entry_size(last_key); // last_key becomes new separator

		let parent_after = parent.current_size() - old_sep_entry_size + new_sep_entry_size;
		if parent_after > PAGE_SIZE {
			return Ok(()); // Skip redistribution, would overflow parent
		}

		// Check if redistribution improves balance
		let before_diff = (left_size as i64 - right_size as i64).abs();
		let after_diff = (left_after as i64 - right_after as i64).abs();

		if after_diff >= before_diff {
			return Ok(()); // Doesn't improve balance
		}

		// Proceed with redistribution
		let new_separator = left_node.redistribute_to_right(right_node);
		parent.keys[left_idx] = new_separator;

		self.write_node_owned(NodeType::Leaf(left_node.clone()))?;
		self.write_node_owned(NodeType::Leaf(right_node.clone()))?;

		Ok(())
	}

	fn redistribute_leaf_from_right(
		&mut self,
		parent: &mut InternalNode,
		left_idx: usize,
		left_node: &mut LeafNode,
		right_node: &mut LeafNode,
	) -> Result<()> {
		if right_node.keys.is_empty() {
			return Err(BPlusTreeError::Serialization(
				"Right leaf node is unexpectedly empty during redistribution".into(),
			));
		}

		let first_key = &right_node.keys[0];
		let first_value = &right_node.values[0];
		let first_entry_size = leaf_cell_size(first_key, first_value);

		let left_size = left_node.current_size();
		let right_size = right_node.current_size();

		// Check leaf nodes would fit
		let left_after = left_size + first_entry_size;
		let right_after = right_size - first_entry_size;

		if left_after > PAGE_SIZE || right_after > PAGE_SIZE {
			return Ok(());
		}

		// CRITICAL: Check parent can accommodate new separator
		// After taking from right, the NEW first key of right becomes the separator
		let old_sep = &parent.keys[left_idx];
		let old_sep_entry_size = internal_entry_size(old_sep);

		// New separator will be right's second key (which becomes first after removal)
		let new_sep_entry_size = if right_node.keys.len() > 1 {
			internal_entry_size(&right_node.keys[1])
		} else {
			// Right will be empty after this, use moved key as separator
			internal_entry_size(first_key)
		};

		let parent_after = parent.current_size() - old_sep_entry_size + new_sep_entry_size;
		if parent_after > PAGE_SIZE {
			return Ok(()); // Skip redistribution, would overflow parent
		}

		// Check if redistribution improves balance
		let before_diff = (left_size as i64 - right_size as i64).abs();
		let after_diff = (left_after as i64 - right_after as i64).abs();

		if after_diff >= before_diff {
			return Ok(()); // Doesn't improve balance
		}

		// Proceed with redistribution
		let new_separator = left_node.take_from_right(right_node);
		parent.keys[left_idx] = new_separator;

		self.write_node_owned(NodeType::Leaf(left_node.clone()))?;
		self.write_node_owned(NodeType::Leaf(right_node.clone()))?;

		Ok(())
	}

	fn merge_internal_nodes(
		&mut self,
		parent: &mut InternalNode,
		left_idx: usize,
		right_idx: usize,
		left_node: &mut InternalNode,
		right_node: InternalNode,
	) -> Result<()> {
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

		// Ensure key_overflows vec is properly sized
		while node.key_overflows.len() < node.keys.len() {
			node.key_overflows.push(0);
		}

		// Check each key to see if it needs overflow
		for (i, key) in node.keys.iter().enumerate() {
			let (bytes_on_page, needs_overflow) =
				calculate_overflow(key.len(), min_local, max_local);

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

		// Ensure overflow vec is properly sized
		while node.cell_overflows.len() < node.keys.len() {
			node.cell_overflows.push(0);
		}

		// Check each cell (key+value pair) to see if it needs overflow
		for (i, (key, value)) in node.keys.iter().zip(&node.values).enumerate() {
			let payload_len = key.len() + value.len();
			let (bytes_on_page, needs_overflow) =
				calculate_overflow(payload_len, min_local, max_local);

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
			overflow_page.data = Bytes::copy_from_slice(chunk_data);
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
	fn read_overflow_chain(&self, first_page: u64) -> Result<Bytes> {
		if first_page == 0 {
			return Ok(Bytes::new());
		}

		let mut result = BytesMut::new();
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

		Ok(result.freeze())
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

	pub fn range<'a, R: RangeBounds<&'a [u8]>>(
		&self,
		range: R,
	) -> Result<RangeScanIterator<'_, F>> {
		// Convert Bound<&&[u8]> to Bound<&[u8]> by dereferencing
		let start = match range.start_bound() {
			Bound::Included(k) => Bound::Included(*k),
			Bound::Excluded(k) => Bound::Excluded(*k),
			Bound::Unbounded => Bound::Unbounded,
		};
		let end = match range.end_bound() {
			Bound::Included(k) => Bound::Included(*k),
			Bound::Excluded(k) => Bound::Excluded(*k),
			Bound::Unbounded => Bound::Unbounded,
		};
		RangeScanIterator::new(self, start, end)
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

	/// Split a leaf node after inserting a new entry.
	///
	/// INVARIANTS:
	/// - Before insert: sum(cell_sizes) ≤ 4075 (fits in page)
	/// - After insert: sum ≤ 4075 + max_cell ≤ 4075 + 1011 = 5086
	/// - With max_cell ≤ 1011 and 4 cells guaranteed to fit, split always succeeds
	fn split_leaf(
		&mut self,
		leaf: &mut LeafNode,
		key: Bytes,
		value: Bytes,
	) -> Result<(Bytes, u64, u64)> {
		// STEP 1: Insert the new entry
		let (_, old_overflow) = leaf.insert(key, value, self.compare.as_ref());
		if let Some(ov) = old_overflow {
			self.free_overflow_chain(ov)?;
		}

		let n = leaf.keys.len();
		assert!(n >= 2, "Need at least 2 entries to split");

		// STEP 2: Pre-compute cell sizes
		let mut prefix_sum = Vec::with_capacity(n + 1);
		prefix_sum.push(0);

		let mut running_sum = 0usize;
		for (k, v) in leaf.keys.iter().zip(&leaf.values) {
			running_sum += leaf_cell_size(k, v);
			prefix_sum.push(running_sum);
		}
		let total_size = running_sum;

		// Valid split at k means:
		// - Left (cells 0..k): LEAF_HEADER_SIZE + prefix_sum[k] ≤ PAGE_SIZE
		// - Right (cells k..n): LEAF_HEADER_SIZE + (total - prefix_sum[k]) ≤ PAGE_SIZE
		//
		// Equivalent to: prefix_sum[k] ∈ [total - 4075, 4075]

		let lower_bound = total_size.saturating_sub(PAGE_SIZE - LEAF_HEADER_SIZE);
		let upper_bound = PAGE_SIZE - LEAF_HEADER_SIZE;

		let mut split_idx = None;
		let mut best_diff = usize::MAX;

		for (k, item) in prefix_sum.iter().enumerate().take(n).skip(1) {
			if *item >= lower_bound && *item <= upper_bound {
				let left_size = *item;
				let right_size = total_size - *item;
				let diff = (left_size as isize - right_size as isize).unsigned_abs();

				if diff < best_diff {
					best_diff = diff;
					split_idx = Some(k);
				}
			}
		}

		let split_idx = split_idx
			.expect("BUG: No valid split point found. This indicates max_local is too large.");

		// STEP 4: Perform the split
		let new_leaf_offset = self.allocate_page()?;
		let mut new_leaf = LeafNode::new(new_leaf_offset);

		new_leaf.keys = leaf.keys.split_off(split_idx);
		new_leaf.values = leaf.values.split_off(split_idx);
		new_leaf.cell_overflows = leaf.cell_overflows.split_off(split_idx);

		// STEP 5: Update linked list
		new_leaf.next_leaf = leaf.next_leaf;
		new_leaf.prev_leaf = leaf.offset;
		leaf.next_leaf = new_leaf.offset;

		// STEP 6: Get separator key (copy of first key in right leaf)
		let separator_key = new_leaf.keys[0].clone();

		// STEP 7: Validate (debug only)
		#[cfg(debug_assertions)]
		{
			let left_size = LEAF_HEADER_SIZE + prefix_sum[split_idx];
			let right_size = LEAF_HEADER_SIZE + (total_size - prefix_sum[split_idx]);
			assert!(left_size <= PAGE_SIZE, "Left leaf {} > PAGE_SIZE {}", left_size, PAGE_SIZE);
			assert!(right_size <= PAGE_SIZE, "Right leaf {} > PAGE_SIZE {}", right_size, PAGE_SIZE);
			assert!(!leaf.keys.is_empty(), "Left leaf empty after split");
			assert!(!new_leaf.keys.is_empty(), "Right leaf empty after split");
		}

		// STEP 8: Write nodes
		let next_leaf_offset = new_leaf.next_leaf;
		let leaf_offset = leaf.offset;
		let leaf_owned = std::mem::replace(leaf, LeafNode::new(leaf_offset));
		self.write_node_owned(NodeType::Leaf(leaf_owned))?;

		let new_offset = new_leaf.offset;
		self.write_node_owned(NodeType::Leaf(new_leaf))?;

		// Update next leaf's prev pointer
		if next_leaf_offset != 0 {
			let next_node = self.read_node(next_leaf_offset)?;
			if let NodeType::Leaf(_) = next_node.as_ref() {
				let mut next_leaf_node = self.extract_leaf_mut(next_node);
				next_leaf_node.prev_leaf = new_offset;
				self.write_node_owned(NodeType::Leaf(next_leaf_node))?;
			}
		}

		Ok((separator_key, 0, new_offset))
	}

	// =========================================================================
	// INTERNAL NODE SPLIT
	// =========================================================================

	/// Split an internal node after inserting a new key+child.
	///
	/// INVARIANTS:
	/// - With max_entry ≤ 1011, at least 4 keys fit per page
	/// - After split: each side has ≥ 1 key, which always fits
	fn split_internal_with_child(
		&mut self,
		node: &mut InternalNode,
		extra_key: Bytes,
		extra_overflow: u64,
		extra_child: u64,
	) -> Result<(Bytes, u64, u64)> {
		// STEP 1: Insert the new key+child
		node.insert_key_child_with_overflow(
			extra_key,
			extra_overflow,
			extra_child,
			self.compare.as_ref(),
		);

		let n = node.keys.len();
		assert!(n >= 2, "Need at least 2 keys to split internal node");

		// STEP 2: Pre-compute entry sizes
		let entry_sizes: Vec<usize> = node.keys.iter().map(|k| internal_entry_size(k)).collect();

		// STEP 3: Compute prefix sums for key sizes
		let mut key_prefix = vec![0usize; n + 1];
		for i in 0..n {
			key_prefix[i + 1] = key_prefix[i] + entry_sizes[i];
		}
		let total_key_size = key_prefix[n];

		// STEP 4: Find valid split point
		//
		// After removing promoted key at index p:
		// - Left: p keys, p+1 children
		// - Right: n-1-p keys, n-p children
		//
		// Left size = INTERNAL_HEADER_SIZE + key_prefix[p] + (p+1) * CHILD_PTR_SIZE
		// Right size = INTERNAL_HEADER_SIZE + (total_key_size - key_prefix[p+1]) + (n-p) *
		// CHILD_PTR_SIZE

		let mut best_p = n / 2;
		let mut best_diff = usize::MAX;
		let mut found_valid = false;

		for p in 1..(n.saturating_sub(1).max(1) + 1) {
			if p >= n {
				break;
			}

			let left_key_size = key_prefix[p];
			let right_key_size = total_key_size - key_prefix[p + 1];

			let left_size = INTERNAL_HEADER_SIZE + left_key_size + (p + 1) * CHILD_PTR_SIZE;
			let right_size = INTERNAL_HEADER_SIZE + right_key_size + (n - p) * CHILD_PTR_SIZE;

			if left_size <= PAGE_SIZE && right_size <= PAGE_SIZE {
				found_valid = true;
				let diff = (left_size as isize - right_size as isize).unsigned_abs();
				if diff < best_diff {
					best_diff = diff;
					best_p = p;
				}
			}
		}

		assert!(found_valid, "BUG: No valid internal split point found");

		// STEP 5: Extract promoted key
		let promoted_key = node.keys.remove(best_p);
		let promoted_overflow = if best_p < node.key_overflows.len() {
			node.key_overflows.remove(best_p)
		} else {
			0
		};

		// STEP 6: Split remaining keys and children
		let new_node_offset = self.allocate_page()?;
		let mut new_node = InternalNode::new(new_node_offset);

		new_node.keys = node.keys.split_off(best_p);
		new_node.key_overflows = node.key_overflows.split_off(best_p);
		new_node.children = node.children.split_off(best_p + 1);

		// STEP 7: Validate
		#[cfg(debug_assertions)]
		{
			assert_eq!(
				node.children.len(),
				node.keys.len() + 1,
				"Left: children {} != keys {} + 1",
				node.children.len(),
				node.keys.len()
			);
			assert_eq!(
				new_node.children.len(),
				new_node.keys.len() + 1,
				"Right: children {} != keys {} + 1",
				new_node.children.len(),
				new_node.keys.len()
			);
			assert!(
				node.current_size() <= PAGE_SIZE,
				"Left internal {} > PAGE_SIZE",
				node.current_size()
			);
			assert!(
				new_node.current_size() <= PAGE_SIZE,
				"Right internal {} > PAGE_SIZE",
				new_node.current_size()
			);
		}

		// STEP 8: Write nodes
		let node_offset = node.offset;
		let node_owned = std::mem::replace(node, InternalNode::new(node_offset));
		self.write_node_owned(NodeType::Internal(node_owned))?;

		let new_offset = new_node.offset;
		self.write_node_owned(NodeType::Internal(new_node))?;

		Ok((promoted_key, promoted_overflow, new_offset))
	}
}

pub struct RangeScanIterator<'a, F: VfsFile> {
	tree: &'a BPlusTree<F>,
	current_leaf: Option<LeafNode>,
	end: Bound<Bytes>, // Included/Excluded/Unbounded
	current_idx: usize,
	current_end_idx: usize, // Pre-calculated end position in current leaf
	reached_end: bool,
}

impl<'a, F: VfsFile> RangeScanIterator<'a, F> {
	pub(crate) fn new(
		tree: &'a BPlusTree<F>,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
	) -> Result<Self> {
		// Handle start bound
		let start_key = match start {
			Bound::Included(key) => key,
			Bound::Excluded(key) => {
				// For excluded start, we need to find the next key after this one
				// This is handled by partition_point which finds first key >= start_key
				// So we can use the key as-is and skip it if it matches exactly
				key
			}
			Bound::Unbounded => &[] as &[u8], // Empty slice means start from beginning
		};

		// Find the leaf containing the start key
		let mut node_offset = tree.header.root_offset;

		// Traverse the tree to find the starting leaf
		let leaf = loop {
			match tree.read_node(node_offset)?.as_ref() {
				NodeType::Internal(internal) => {
					let idx = if start_key.is_empty() {
						0 // Start from first child for unbounded
					} else {
						internal.find_child_index(start_key, tree.compare.as_ref())
					};
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
		let current_idx = if start_key.is_empty() {
			0 // Start from beginning for unbounded
		} else {
			match start {
				Bound::Included(key) => {
					leaf.keys.partition_point(|k| tree.compare.compare(k, key) == Ordering::Less)
				}
				Bound::Excluded(key) => {
					// Find first key > start_key
					// partition_point returns first index where compare(k, key) ==
					// Ordering::Greater
					leaf.keys.partition_point(|k| tree.compare.compare(k, key) != Ordering::Greater)
				}
				Bound::Unbounded => 0,
			}
		};

		// Handle end bound
		let end = match end {
			Bound::Included(key) => Bound::Included(Bytes::copy_from_slice(key)),
			Bound::Excluded(key) => Bound::Excluded(Bytes::copy_from_slice(key)),
			Bound::Unbounded => Bound::Unbounded,
		};

		// Pre-calculate end index in this leaf
		let current_end_idx = match &end {
			Bound::Included(end_k) => leaf
				.keys
				.partition_point(|k| tree.compare.compare(k, end_k.as_ref()) != Ordering::Greater),
			Bound::Excluded(end_k) => leaf
				.keys
				.partition_point(|k| tree.compare.compare(k, end_k.as_ref()) == Ordering::Less),
			Bound::Unbounded => leaf.keys.len(), // Unbounded: include all keys in leaf
		};

		Ok(RangeScanIterator {
			tree,
			current_leaf: Some(leaf),
			end,
			current_idx,
			current_end_idx,
			reached_end: false,
		})
	}
}

impl<F: VfsFile> Iterator for RangeScanIterator<'_, F> {
	type Item = Result<(Bytes, Bytes)>;

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
								let next_end_idx = match &self.end {
									Bound::Included(end_k) => next_leaf.keys.partition_point(|k| {
										self.tree.compare.compare(k, end_k.as_ref())
											!= Ordering::Greater
									}),
									Bound::Excluded(end_k) => next_leaf.keys.partition_point(|k| {
										self.tree.compare.compare(k, end_k.as_ref())
											== Ordering::Less
									}),
									Bound::Unbounded => next_leaf.keys.len(), /* Unbounded:
									                                           * include all keys */
								};

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

/// Cursor-based iterator over a BPlusTree that implements LSMIterator.
///
/// The BPlusTree stores encoded internal keys directly (when used as versioned index),
/// so this iterator provides zero-copy access to those keys and values.
pub struct BPlusTreeIterator<'a, F: VfsFile> {
	/// Reference to the tree being iterated
	tree: &'a BPlusTree<F>,

	/// Current leaf node (cloned from cache for efficient access)
	current_leaf: Option<LeafNode>,

	/// Current position within the leaf's keys array
	current_idx: usize,

	/// Whether the iterator has been exhausted
	exhausted: bool,
}

impl<'a, F: VfsFile> BPlusTreeIterator<'a, F> {
	/// Create a new unpositioned iterator over the tree
	pub fn new(tree: &'a BPlusTree<F>) -> Self {
		BPlusTreeIterator {
			tree,
			current_leaf: None,
			current_idx: 0,
			exhausted: false,
		}
	}

	/// Check if positioned on a valid entry
	fn is_valid(&self) -> bool {
		if self.exhausted {
			return false;
		}
		match &self.current_leaf {
			Some(leaf) => self.current_idx < leaf.keys.len(),
			None => false,
		}
	}

	/// Navigate from root to the first (leftmost) leaf
	fn navigate_to_first_leaf(&mut self) -> Result<()> {
		let first_leaf_offset = self.tree.header.first_leaf_offset;
		if first_leaf_offset == 0 {
			self.current_leaf = None;
			return Ok(());
		}

		let node = self.tree.read_node(first_leaf_offset)?;

		match node.as_ref() {
			NodeType::Leaf(leaf) => {
				self.current_leaf = Some(leaf.clone());
				Ok(())
			}
			_ => Err(BPlusTreeError::InvalidNodeType),
		}
	}

	/// Navigate from root to the last (rightmost) leaf
	fn navigate_to_last_leaf(&mut self) -> Result<()> {
		let mut node_offset = self.tree.header.root_offset;
		if node_offset == 0 {
			self.current_leaf = None;
			return Ok(());
		}

		loop {
			let node = self.tree.read_node(node_offset)?;

			match node.as_ref() {
				NodeType::Internal(internal) => {
					// Navigate to rightmost child
					node_offset =
						*internal.children.last().ok_or(BPlusTreeError::InvalidNodeType)?;
				}
				NodeType::Leaf(leaf) => {
					self.current_leaf = Some(leaf.clone());
					return Ok(());
				}
				NodeType::Overflow(_) => {
					return Err(BPlusTreeError::UnexpectedOverflowPage(node_offset));
				}
			}
		}
	}

	/// Navigate to the leaf containing the target key
	fn navigate_to_key(&mut self, target: &[u8]) -> Result<()> {
		let mut node_offset = self.tree.header.root_offset;
		if node_offset == 0 {
			self.current_leaf = None;
			return Ok(());
		}

		loop {
			let node = self.tree.read_node(node_offset)?;

			match node.as_ref() {
				NodeType::Internal(internal) => {
					let idx = internal.find_child_index(target, self.tree.compare.as_ref());
					node_offset = internal.children[idx];
				}
				NodeType::Leaf(leaf) => {
					self.current_leaf = Some(leaf.clone());
					return Ok(());
				}
				NodeType::Overflow(_) => {
					return Err(BPlusTreeError::UnexpectedOverflowPage(node_offset));
				}
			}
		}
	}

	/// Load the next leaf using the doubly-linked list
	fn advance_to_next_leaf(&mut self) -> Result<bool> {
		if let Some(leaf) = &self.current_leaf {
			if leaf.next_leaf == 0 {
				return Ok(false);
			}

			let node = self.tree.read_node(leaf.next_leaf)?;

			match node.as_ref() {
				NodeType::Leaf(next_leaf) => {
					self.current_leaf = Some(next_leaf.clone());
					self.current_idx = 0;
					Ok(true)
				}
				_ => Err(BPlusTreeError::InvalidNodeType),
			}
		} else {
			Ok(false)
		}
	}

	/// Load the previous leaf using the doubly-linked list
	fn retreat_to_prev_leaf(&mut self) -> Result<bool> {
		if let Some(leaf) = &self.current_leaf {
			if leaf.prev_leaf == 0 {
				return Ok(false);
			}

			let node = self.tree.read_node(leaf.prev_leaf)?;

			match node.as_ref() {
				NodeType::Leaf(prev_leaf) => {
					self.current_leaf = Some(prev_leaf.clone());
					self.current_idx = prev_leaf.keys.len().saturating_sub(1);
					Ok(true)
				}
				_ => Err(BPlusTreeError::InvalidNodeType),
			}
		} else {
			Ok(false)
		}
	}
}

impl<F: VfsFile> LSMIterator for BPlusTreeIterator<'_, F> {
	/// Seek to first key >= target.
	/// Target is an encoded internal key.
	fn seek(&mut self, target: &[u8]) -> crate::error::Result<bool> {
		self.exhausted = false;

		// Navigate to the leaf that might contain the target
		self.navigate_to_key(target).map_err(|e| crate::error::Error::BPlusTree(e.to_string()))?;

		if let Some(leaf) = &self.current_leaf {
			// Binary search for first key >= target
			self.current_idx = leaf
				.keys
				.partition_point(|k| self.tree.compare.compare(k, target) == Ordering::Less);

			// If we're past the end of this leaf, advance to next
			if self.current_idx >= leaf.keys.len()
				&& !self
					.advance_to_next_leaf()
					.map_err(|e| crate::error::Error::BPlusTree(e.to_string()))?
			{
				self.exhausted = true;
				return Ok(false);
			}
		}

		Ok(self.is_valid())
	}

	/// Seek to first entry.
	fn seek_first(&mut self) -> crate::error::Result<bool> {
		self.exhausted = false;

		self.navigate_to_first_leaf().map_err(|e| crate::error::Error::BPlusTree(e.to_string()))?;
		self.current_idx = 0;

		// Handle empty tree or empty first leaf
		if let Some(leaf) = &self.current_leaf {
			if leaf.keys.is_empty() {
				// Try to find first non-empty leaf
				while self
					.advance_to_next_leaf()
					.map_err(|e| crate::error::Error::BPlusTree(e.to_string()))?
				{
					if let Some(l) = &self.current_leaf {
						if !l.keys.is_empty() {
							break;
						}
					}
				}
			}
		}

		Ok(self.is_valid())
	}

	/// Seek to last entry.
	fn seek_last(&mut self) -> crate::error::Result<bool> {
		self.exhausted = false;

		self.navigate_to_last_leaf().map_err(|e| crate::error::Error::BPlusTree(e.to_string()))?;

		if let Some(leaf) = &self.current_leaf {
			if leaf.keys.is_empty() {
				// Try to find last non-empty leaf
				while self
					.retreat_to_prev_leaf()
					.map_err(|e| crate::error::Error::BPlusTree(e.to_string()))?
				{
					if let Some(l) = &self.current_leaf {
						if !l.keys.is_empty() {
							self.current_idx = l.keys.len() - 1;
							break;
						}
					}
				}
			} else {
				self.current_idx = leaf.keys.len() - 1;
			}
		}

		Ok(self.is_valid())
	}

	/// Move to next entry.
	fn next(&mut self) -> crate::error::Result<bool> {
		// Auto-position on first call (following TableIterator pattern)
		if !self.is_valid() && !self.exhausted {
			return self.seek_first();
		}

		if !self.is_valid() {
			return Ok(false);
		}

		self.current_idx += 1;

		// Check if we need to advance to next leaf
		if let Some(leaf) = &self.current_leaf {
			if self.current_idx >= leaf.keys.len()
				&& !self
					.advance_to_next_leaf()
					.map_err(|e| crate::error::Error::BPlusTree(e.to_string()))?
			{
				self.exhausted = true;
				return Ok(false);
			}
		}

		Ok(self.is_valid())
	}

	/// Move to previous entry.
	fn prev(&mut self) -> crate::error::Result<bool> {
		// Auto-position on first call (following TableIterator pattern)
		if !self.is_valid() && !self.exhausted {
			return self.seek_last();
		}

		if !self.is_valid() {
			return Ok(false);
		}

		if self.current_idx == 0 {
			// Need to retreat to previous leaf
			if !self
				.retreat_to_prev_leaf()
				.map_err(|e| crate::error::Error::BPlusTree(e.to_string()))?
			{
				self.exhausted = true;
				return Ok(false);
			}
		} else {
			self.current_idx -= 1;
		}

		Ok(self.is_valid())
	}

	/// Check if positioned on valid entry.
	fn valid(&self) -> bool {
		self.is_valid()
	}

	/// Get current key (zero-copy).
	/// The BPlusTree stores encoded internal keys directly.
	fn key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.is_valid());
		let leaf = self.current_leaf.as_ref().expect("valid() should be true");
		InternalKeyRef::from_encoded(&leaf.keys[self.current_idx])
	}

	/// Get current value (zero-copy).
	fn value_encoded(&self) -> crate::error::Result<&[u8]> {
		debug_assert!(self.is_valid());
		let leaf = self.current_leaf.as_ref().expect("valid() should be true");
		Ok(&leaf.values[self.current_idx])
	}
}

impl<F: VfsFile> BPlusTree<F> {
	/// Creates a new cursor-based iterator implementing LSMIterator.
	/// The iterator starts unpositioned; call seek(), seek_first(), or
	/// seek_last() to position it.
	pub fn internal_iterator(&self) -> BPlusTreeIterator<'_, F> {
		BPlusTreeIterator::new(self)
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
	use crate::{InternalKey, InternalKeyKind};

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
		tree.insert(b"key1", b"value1").unwrap();
		tree.insert(b"key2", b"value2").unwrap();
		tree.insert(b"key3", b"value3").unwrap();

		// Test retrievals
		assert_eq!(tree.get(b"key1").unwrap().unwrap().as_ref(), b"value1");
		assert_eq!(tree.get(b"key2").unwrap().unwrap().as_ref(), b"value2");
		assert_eq!(tree.get(b"key3").unwrap().unwrap().as_ref(), b"value3");

		// Test non-existent key
		assert!(tree.get(b"nonexistent").unwrap().is_none());

		// Test deletions
		assert_eq!(tree.delete(b"key2").unwrap().unwrap().as_ref(), b"value2");
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
		tree.insert(b"key1", b"value1").unwrap();
		assert_eq!(tree.get(b"key1").unwrap().as_deref(), Some(b"value1".as_ref()));
	}

	#[test]
	fn test_update() {
		let mut tree = create_test_tree(true);

		// Insert multiple keys to ensure we're working with a leaf node
		tree.insert(b"key1", b"value1").unwrap();
		tree.insert(b"key2", b"value2").unwrap();
		tree.insert(b"key3", b"value3").unwrap();

		// Update existing key in the same leaf
		tree.insert(b"key2", b"updated_value2").unwrap();

		// Verify the update
		assert_eq!(tree.get(b"key1").unwrap().as_deref(), Some(b"value1".as_ref()));
		assert_eq!(tree.get(b"key2").unwrap().as_deref(), Some(b"updated_value2".as_ref()));
		assert_eq!(tree.get(b"key3").unwrap().as_deref(), Some(b"value3".as_ref()));
	}

	#[test]
	fn test_multiple_updates_same_key() {
		let mut tree = create_test_tree(true);

		// Insert initial key
		tree.insert(b"key", b"value1").unwrap();
		assert_eq!(tree.get(b"key").unwrap().as_deref(), Some(b"value1".as_ref()));

		// Update multiple times
		tree.insert(b"key", b"value2").unwrap();
		assert_eq!(tree.get(b"key").unwrap().as_deref(), Some(b"value2".as_ref()));

		tree.insert(b"key", b"value3").unwrap();
		assert_eq!(tree.get(b"key").unwrap().as_deref(), Some(b"value3".as_ref()));

		tree.insert(b"key", b"final_value").unwrap();
		assert_eq!(tree.get(b"key").unwrap().as_deref(), Some(b"final_value".as_ref()));
	}

	#[test]
	fn test_update_no_new_nodes_created() {
		let mut tree = create_test_tree(true);

		// Insert initial keys to create some tree structure
		tree.insert(b"key1", b"value1").unwrap();
		tree.insert(b"key2", b"value2").unwrap();
		tree.insert(b"key3", b"value3").unwrap();

		// Get initial tree stats
		let (height_before, nodes_before, keys_before, leaves_before) =
			tree.calculate_tree_stats().unwrap();

		// Update existing keys - should not create new nodes
		tree.insert(b"key1", b"updated_value1").unwrap();
		tree.insert(b"key2", b"updated_value2").unwrap();
		tree.insert(b"key3", b"updated_value3").unwrap();

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
			tree.insert(key.clone(), b"value").unwrap();
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
			tree.insert(key.clone(), b"value").unwrap();
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
					tree.get(key)?.as_ref().map(|b| b.as_ref()),
					Some(value.as_slice()),
					"Key {:?} not found after reopening",
					String::from_utf8_lossy(key)
				);
			}

			// Verify non-existent key
			assert_eq!(tree.get(b"mango")?, None, "Non-existent key found unexpectedly");

			// Add new data and verify
			tree.insert(b"mango", b"orange")?;
			assert_eq!(
				tree.get(b"mango")?.as_ref().map(|b| b.as_ref()),
				Some(b"orange".as_ref()),
				"New insertion failed after reopening"
			);
		}

		// Reopen again to verify new data persisted
		{
			let tree = BPlusTree::disk(path, Arc::new(TestComparator))?;
			assert_eq!(
				tree.get(b"mango")?.as_ref().map(|b| b.as_ref()),
				Some(b"orange".as_ref()),
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
			tree.insert(b"one", b"1")?;
			tree.insert(b"two", b"2")?;
			tree.insert(b"three", b"3")?;
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
				tree.get(b"one")?.as_ref().map(|b| b.as_ref()),
				Some(b"1".as_ref()),
				"Existing key missing after deletion"
			);
		}

		Ok(())
	}

	#[test]
	fn test_concurrent_operations() {
		let mut tree = create_test_tree(true);
		// Insert and delete same key repeatedly
		tree.insert(b"key", b"v1").unwrap();
		tree.delete(b"key").unwrap();
		tree.insert(b"key", b"v2").unwrap();
		assert_eq!(tree.get(b"key").unwrap().as_deref(), Some(b"v2".as_ref()));
	}

	#[test]
	fn test_drop_behavior() {
		let file = NamedTempFile::new().unwrap();
		let path = file.path();

		// Create and immediately drop
		{
			let mut tree = BPlusTree::disk(path, Arc::new(TestComparator)).unwrap();
			tree.insert(b"test", b"value").unwrap();
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
		tree.insert(b"close", b"test").unwrap();
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
					tree.get(key).unwrap().as_ref().map(|b| b.as_ref()),
					Some(value.as_slice()),
					"Key {:?} not found after reopening",
					String::from_utf8_lossy(key)
				);
			}
		}
	}

	fn serialize_u32(n: u32) -> Vec<u8> {
		n.to_be_bytes().to_vec()
	}

	fn deserialize_pair(pair: (Bytes, Bytes)) -> (u32, u32) {
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
		let results =
			tree.range(serialize_u32(3).as_slice()..=serialize_u32(7).as_slice()).unwrap();
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
		let results =
			tree.range(serialize_u32(5).as_slice()..=serialize_u32(15).as_slice()).unwrap();
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

		let results: Vec<_> = tree
			.range(serialize_u32(1).as_slice()..=serialize_u32(10).as_slice())
			.unwrap()
			.collect();

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
		let results: Vec<_> =
			tree.range(serialize_u32(3).as_slice()..serialize_u32(1).as_slice()).unwrap().collect();
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
		let results =
			tree.range(serialize_u32(2).as_slice()..=serialize_u32(9).as_slice()).unwrap();
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
		let results: Vec<_> = tree
			.range(serialize_u32(5).as_slice()..=serialize_u32(5).as_slice())
			.unwrap()
			.collect();
		assert_eq!(results.len(), 1);

		// Non-existent exact range
		let results: Vec<_> = tree
			.range(serialize_u32(3).as_slice()..=serialize_u32(3).as_slice())
			.unwrap()
			.collect();
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
		let results =
			tree.range(serialize_u32(5).as_slice()..=serialize_u32(15).as_slice()).unwrap();
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
		tree.insert(b"key1", b"value1").unwrap();
		tree.insert(b"key3", b"value3").unwrap();
		tree.insert(b"key2", b"value2").unwrap();

		let mut iter = tree.range(b"key2".as_slice()..=b"key3".as_slice()).unwrap();
		let (k1, v1) = iter.next().unwrap().unwrap();
		assert_eq!((k1.as_ref(), v1.as_ref()), (b"key2".as_ref(), b"value2".as_ref()));
		let (k2, v2) = iter.next().unwrap().unwrap();
		assert_eq!((k2.as_ref(), v2.as_ref()), (b"key3".as_ref(), b"value3".as_ref()));
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_range_empty() {
		let mut tree = create_test_tree(true);
		tree.insert(b"a", b"1").unwrap();
		tree.insert(b"c", b"3").unwrap();
		let mut iter = tree.range(b"b".as_slice()..b"b".as_slice()).unwrap();
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
		let mut iter = tree.range(start.as_slice()..=end.as_slice()).unwrap();
		for i in 5000..=5500 {
			let expected_key = format!("key_{:05}", i).into_bytes();
			let expected_value = format!("value_{:05}", i).into_bytes();
			let (k, v) = iter.next().unwrap().unwrap();
			assert_eq!(
				(k.as_ref(), v.as_ref()),
				(expected_key.as_slice(), expected_value.as_slice())
			);
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

			let prefix = format!("additional_{:05}_", i).into_bytes();
			let key_size = key_size.max(prefix.len()); // Ensure key_size >= prefix length
			let mut key = prefix;
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

	// ==================== BPlusTreeIterator Tests ====================

	// Helper to create an encoded internal key for testing
	fn make_internal_key(user_key: &[u8], seq_num: u64) -> Vec<u8> {
		InternalKey::new(
			user_key.to_vec(),
			seq_num,
			InternalKeyKind::Set,
			seq_num, // use seq_num as timestamp for simplicity
		)
		.encode()
	}

	// Helper to collect all entries from an iterator (forward)
	fn collect_forward(iter: &mut BPlusTreeIterator<'_, std::fs::File>) -> Vec<(Vec<u8>, Vec<u8>)> {
		let mut result = Vec::new();
		if !iter.seek_first().unwrap() {
			return result;
		}
		while iter.valid() {
			result.push((iter.key().user_key().to_vec(), iter.value().unwrap()));
			if !iter.next().unwrap() {
				break;
			}
		}
		result
	}

	// Helper to collect all entries from an iterator (backward)
	fn collect_backward(
		iter: &mut BPlusTreeIterator<'_, std::fs::File>,
	) -> Vec<(Vec<u8>, Vec<u8>)> {
		let mut result = Vec::new();
		if !iter.seek_last().unwrap() {
			return result;
		}
		while iter.valid() {
			result.push((iter.key().user_key().to_vec(), iter.value().unwrap()));
			if !iter.prev().unwrap() {
				break;
			}
		}
		result
	}

	// ========== Empty Tree Tests ==========

	#[test]
	fn test_internal_iterator_empty_tree() {
		let temp_file = NamedTempFile::new().unwrap();
		let tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		let mut iter = tree.internal_iterator();

		// All positioning operations should return false on empty tree
		assert!(!iter.seek_first().unwrap());
		assert!(!iter.valid());

		assert!(!iter.seek_last().unwrap());
		assert!(!iter.valid());

		assert!(!iter.seek(b"any_key").unwrap());
		assert!(!iter.valid());
	}

	#[test]
	fn test_internal_iterator_empty_tree_next_prev() {
		let temp_file = NamedTempFile::new().unwrap();
		let tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		let mut iter = tree.internal_iterator();

		// next() on unpositioned empty iterator should try seek_first and fail
		assert!(!iter.next().unwrap());
		assert!(!iter.valid());

		// prev() on unpositioned empty iterator should try seek_last and fail
		let mut iter2 = tree.internal_iterator();
		assert!(!iter2.prev().unwrap());
		assert!(!iter2.valid());
	}

	// ========== Single Element Tests ==========

	#[test]
	fn test_internal_iterator_single_element_seek_first() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"only_key", 1), b"only_value").unwrap();

		let mut iter = tree.internal_iterator();
		assert!(iter.seek_first().unwrap());
		assert!(iter.valid());
		assert_eq!(iter.key().user_key(), b"only_key");
		assert_eq!(iter.value_encoded().unwrap(), b"only_value");
	}

	#[test]
	fn test_internal_iterator_single_element_seek_last() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"only_key", 1), b"only_value").unwrap();

		let mut iter = tree.internal_iterator();
		assert!(iter.seek_last().unwrap());
		assert!(iter.valid());
		assert_eq!(iter.key().user_key(), b"only_key");
		assert_eq!(iter.value_encoded().unwrap(), b"only_value");
	}

	#[test]
	fn test_internal_iterator_single_element_next_exhausts() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"only_key", 1), b"only_value").unwrap();

		let mut iter = tree.internal_iterator();
		assert!(iter.seek_first().unwrap());
		assert!(iter.valid());

		// next() should exhaust the iterator
		assert!(!iter.next().unwrap());
		assert!(!iter.valid());
	}

	#[test]
	fn test_internal_iterator_single_element_prev_exhausts() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"only_key", 1), b"only_value").unwrap();

		let mut iter = tree.internal_iterator();
		assert!(iter.seek_first().unwrap());
		assert!(iter.valid());

		// prev() at first element should exhaust the iterator
		assert!(!iter.prev().unwrap());
		assert!(!iter.valid());
	}

	// ========== Forward Iteration Tests ==========

	#[test]
	fn test_internal_iterator_forward_basic() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"aaa", 1), b"val1").unwrap();
		tree.insert(make_internal_key(b"bbb", 2), b"val2").unwrap();
		tree.insert(make_internal_key(b"ccc", 3), b"val3").unwrap();

		let mut iter = tree.internal_iterator();
		let collected = collect_forward(&mut iter);

		assert_eq!(collected.len(), 3);
		assert_eq!(collected[0], (b"aaa".to_vec(), b"val1".to_vec()));
		assert_eq!(collected[1], (b"bbb".to_vec(), b"val2".to_vec()));
		assert_eq!(collected[2], (b"ccc".to_vec(), b"val3".to_vec()));
	}

	#[test]
	fn test_internal_iterator_forward_ordering() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		// Insert in random order
		tree.insert(make_internal_key(b"charlie", 1), b"3").unwrap();
		tree.insert(make_internal_key(b"alpha", 2), b"1").unwrap();
		tree.insert(make_internal_key(b"delta", 3), b"4").unwrap();
		tree.insert(make_internal_key(b"bravo", 4), b"2").unwrap();

		let mut iter = tree.internal_iterator();
		let collected = collect_forward(&mut iter);

		// Should be in sorted order
		assert_eq!(collected.len(), 4);
		assert_eq!(collected[0].0, b"alpha".to_vec());
		assert_eq!(collected[1].0, b"bravo".to_vec());
		assert_eq!(collected[2].0, b"charlie".to_vec());
		assert_eq!(collected[3].0, b"delta".to_vec());
	}

	// ========== Backward Iteration Tests ==========

	#[test]
	fn test_internal_iterator_backward_basic() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"aaa", 1), b"val1").unwrap();
		tree.insert(make_internal_key(b"bbb", 2), b"val2").unwrap();
		tree.insert(make_internal_key(b"ccc", 3), b"val3").unwrap();

		let mut iter = tree.internal_iterator();
		let collected = collect_backward(&mut iter);

		// Should be in reverse order
		assert_eq!(collected.len(), 3);
		assert_eq!(collected[0], (b"ccc".to_vec(), b"val3".to_vec()));
		assert_eq!(collected[1], (b"bbb".to_vec(), b"val2".to_vec()));
		assert_eq!(collected[2], (b"aaa".to_vec(), b"val1".to_vec()));
	}

	#[test]
	fn test_internal_iterator_backward_from_seek_last() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		for i in 0..10 {
			let key = format!("key{:02}", i);
			let val = format!("val{:02}", i);
			tree.insert(make_internal_key(key.as_bytes(), i as u64), val.as_bytes()).unwrap();
		}

		let mut iter = tree.internal_iterator();
		assert!(iter.seek_last().unwrap());
		assert_eq!(iter.key().user_key(), b"key09");

		// Iterate backward
		let mut count = 0;
		while iter.valid() {
			count += 1;
			if !iter.prev().unwrap() {
				break;
			}
		}
		assert_eq!(count, 10);
	}

	// ========== Bidirectional Movement Tests ==========

	#[test]
	fn test_internal_iterator_next_then_prev() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"aaa", 1), b"val1").unwrap();
		tree.insert(make_internal_key(b"bbb", 2), b"val2").unwrap();
		tree.insert(make_internal_key(b"ccc", 3), b"val3").unwrap();

		let mut iter = tree.internal_iterator();
		assert!(iter.seek_first().unwrap());
		assert_eq!(iter.key().user_key(), b"aaa");

		// Move forward
		assert!(iter.next().unwrap());
		assert_eq!(iter.key().user_key(), b"bbb");

		// Move back
		assert!(iter.prev().unwrap());
		assert_eq!(iter.key().user_key(), b"aaa");
	}

	#[test]
	fn test_internal_iterator_prev_then_next() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"aaa", 1), b"val1").unwrap();
		tree.insert(make_internal_key(b"bbb", 2), b"val2").unwrap();
		tree.insert(make_internal_key(b"ccc", 3), b"val3").unwrap();

		let mut iter = tree.internal_iterator();
		assert!(iter.seek_last().unwrap());
		assert_eq!(iter.key().user_key(), b"ccc");

		// Move backward
		assert!(iter.prev().unwrap());
		assert_eq!(iter.key().user_key(), b"bbb");

		// Move forward
		assert!(iter.next().unwrap());
		assert_eq!(iter.key().user_key(), b"ccc");
	}

	#[test]
	fn test_internal_iterator_zigzag_movement() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		for i in 0..5 {
			let key = format!("key{}", i);
			tree.insert(make_internal_key(key.as_bytes(), i as u64), b"value").unwrap();
		}

		let mut iter = tree.internal_iterator();
		assert!(iter.seek_first().unwrap());

		// key0 -> key1 -> key0 -> key1 -> key2 -> key1
		assert_eq!(iter.key().user_key(), b"key0");
		assert!(iter.next().unwrap());
		assert_eq!(iter.key().user_key(), b"key1");
		assert!(iter.prev().unwrap());
		assert_eq!(iter.key().user_key(), b"key0");
		assert!(iter.next().unwrap());
		assert_eq!(iter.key().user_key(), b"key1");
		assert!(iter.next().unwrap());
		assert_eq!(iter.key().user_key(), b"key2");
		assert!(iter.prev().unwrap());
		assert_eq!(iter.key().user_key(), b"key1");
	}

	#[test]
	fn test_internal_iterator_full_forward_then_backward() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		let keys: Vec<String> = (0..10).map(|i| format!("key{:02}", i)).collect();
		for (i, key) in keys.iter().enumerate() {
			tree.insert(make_internal_key(key.as_bytes(), i as u64), b"value").unwrap();
		}

		let mut iter = tree.internal_iterator();

		// Forward to end
		assert!(iter.seek_first().unwrap());
		let mut forward_keys = vec![iter.key().user_key().to_vec()];
		while iter.next().unwrap() {
			forward_keys.push(iter.key().user_key().to_vec());
		}
		assert_eq!(forward_keys.len(), 10);

		// Now backward to start
		assert!(iter.seek_last().unwrap());
		let mut backward_keys = vec![iter.key().user_key().to_vec()];
		while iter.prev().unwrap() {
			backward_keys.push(iter.key().user_key().to_vec());
		}
		assert_eq!(backward_keys.len(), 10);

		// Should be reverse of each other
		backward_keys.reverse();
		assert_eq!(forward_keys, backward_keys);
	}

	// ========== Seek Behavior Tests ==========

	#[test]
	fn test_internal_iterator_seek_exact_match() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"aaa", 1), b"val1").unwrap();
		tree.insert(make_internal_key(b"bbb", 2), b"val2").unwrap();
		tree.insert(make_internal_key(b"ccc", 3), b"val3").unwrap();

		let mut iter = tree.internal_iterator();

		// Seek to exact key
		assert!(iter.seek(&make_internal_key(b"bbb", 0)).unwrap());
		assert!(iter.valid());
		assert_eq!(iter.key().user_key(), b"bbb");
		assert_eq!(iter.value_encoded().unwrap(), b"val2");
	}

	#[test]
	fn test_internal_iterator_seek_between_keys() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"aaa", 1), b"val1").unwrap();
		tree.insert(make_internal_key(b"ccc", 2), b"val3").unwrap();
		tree.insert(make_internal_key(b"eee", 3), b"val5").unwrap();

		let mut iter = tree.internal_iterator();

		// Seek to key between aaa and ccc - should land on ccc
		assert!(iter.seek(&make_internal_key(b"bbb", 0)).unwrap());
		assert!(iter.valid());
		assert_eq!(iter.key().user_key(), b"ccc");

		// Seek to key between ccc and eee - should land on eee
		assert!(iter.seek(&make_internal_key(b"ddd", 0)).unwrap());
		assert!(iter.valid());
		assert_eq!(iter.key().user_key(), b"eee");
	}

	#[test]
	fn test_internal_iterator_seek_before_first() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"bbb", 1), b"val2").unwrap();
		tree.insert(make_internal_key(b"ccc", 2), b"val3").unwrap();

		let mut iter = tree.internal_iterator();

		// Seek before first key should land on first
		assert!(iter.seek(&make_internal_key(b"aaa", 0)).unwrap());
		assert!(iter.valid());
		assert_eq!(iter.key().user_key(), b"bbb");
	}

	#[test]
	fn test_internal_iterator_seek_after_last() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"aaa", 1), b"val1").unwrap();
		tree.insert(make_internal_key(b"bbb", 2), b"val2").unwrap();

		let mut iter = tree.internal_iterator();

		// Seek past last key should fail
		assert!(!iter.seek(&make_internal_key(b"zzz", 0)).unwrap());
		assert!(!iter.valid());
	}

	#[test]
	fn test_internal_iterator_seek_then_iterate_forward() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		for i in 0..10 {
			let key = format!("key{:02}", i);
			tree.insert(make_internal_key(key.as_bytes(), i as u64), b"value").unwrap();
		}

		let mut iter = tree.internal_iterator();

		// Seek to middle
		assert!(iter.seek(&make_internal_key(b"key05", 0)).unwrap());
		assert_eq!(iter.key().user_key(), b"key05");

		// Iterate forward from there
		let mut count = 1; // already at key05
		while iter.next().unwrap() {
			count += 1;
		}
		assert_eq!(count, 5); // key05, key06, key07, key08, key09
	}

	#[test]
	fn test_internal_iterator_seek_then_iterate_backward() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		for i in 0..10 {
			let key = format!("key{:02}", i);
			tree.insert(make_internal_key(key.as_bytes(), i as u64), b"value").unwrap();
		}

		let mut iter = tree.internal_iterator();

		// Seek to middle
		assert!(iter.seek(&make_internal_key(b"key05", 0)).unwrap());
		assert_eq!(iter.key().user_key(), b"key05");

		// Iterate backward from there
		let mut count = 1; // already at key05
		while iter.prev().unwrap() {
			count += 1;
		}
		assert_eq!(count, 6); // key05, key04, key03, key02, key01, key00
	}

	#[test]
	fn test_internal_iterator_multiple_seeks() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"aaa", 1), b"val1").unwrap();
		tree.insert(make_internal_key(b"bbb", 2), b"val2").unwrap();
		tree.insert(make_internal_key(b"ccc", 3), b"val3").unwrap();
		tree.insert(make_internal_key(b"ddd", 4), b"val4").unwrap();
		tree.insert(make_internal_key(b"eee", 5), b"val5").unwrap();

		let mut iter = tree.internal_iterator();

		// Multiple seeks should all work
		assert!(iter.seek(&make_internal_key(b"ccc", 0)).unwrap());
		assert_eq!(iter.key().user_key(), b"ccc");

		assert!(iter.seek(&make_internal_key(b"aaa", 0)).unwrap());
		assert_eq!(iter.key().user_key(), b"aaa");

		assert!(iter.seek(&make_internal_key(b"eee", 0)).unwrap());
		assert_eq!(iter.key().user_key(), b"eee");

		assert!(iter.seek(&make_internal_key(b"bbb", 0)).unwrap());
		assert_eq!(iter.key().user_key(), b"bbb");
	}

	// ========== Exhaustion and State Tests ==========

	#[test]
	fn test_internal_iterator_exhaustion_forward() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"key1", 1), b"val1").unwrap();
		tree.insert(make_internal_key(b"key2", 2), b"val2").unwrap();

		let mut iter = tree.internal_iterator();
		assert!(iter.seek_first().unwrap());

		// Exhaust the iterator
		assert!(iter.next().unwrap()); // key1 -> key2
		assert!(!iter.next().unwrap()); // past end
		assert!(!iter.valid());

		// Multiple next() calls should continue returning false
		for _ in 0..3 {
			assert!(!iter.next().unwrap());
			assert!(!iter.valid());
		}
	}

	#[test]
	fn test_internal_iterator_exhaustion_backward() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"key1", 1), b"val1").unwrap();
		tree.insert(make_internal_key(b"key2", 2), b"val2").unwrap();

		let mut iter = tree.internal_iterator();
		assert!(iter.seek_last().unwrap());

		// Exhaust the iterator backward
		assert!(iter.prev().unwrap()); // key2 -> key1
		assert!(!iter.prev().unwrap()); // before start
		assert!(!iter.valid());

		// Multiple prev() calls should continue returning false
		for _ in 0..3 {
			assert!(!iter.prev().unwrap());
			assert!(!iter.valid());
		}
	}

	#[test]
	fn test_internal_iterator_seek_resets_after_exhaustion() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"aaa", 1), b"val1").unwrap();
		tree.insert(make_internal_key(b"bbb", 2), b"val2").unwrap();
		tree.insert(make_internal_key(b"ccc", 3), b"val3").unwrap();

		let mut iter = tree.internal_iterator();

		// Exhaust forward
		assert!(iter.seek_first().unwrap());
		while iter.next().unwrap() {}
		assert!(!iter.valid());

		// seek() should reset and work
		assert!(iter.seek(&make_internal_key(b"bbb", 0)).unwrap());
		assert!(iter.valid());
		assert_eq!(iter.key().user_key(), b"bbb");
	}

	#[test]
	fn test_internal_iterator_seek_first_resets_after_exhaustion() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"aaa", 1), b"val1").unwrap();
		tree.insert(make_internal_key(b"bbb", 2), b"val2").unwrap();

		let mut iter = tree.internal_iterator();

		// Exhaust forward
		assert!(iter.seek_first().unwrap());
		while iter.next().unwrap() {}
		assert!(!iter.valid());

		// seek_first() should reset and work
		assert!(iter.seek_first().unwrap());
		assert!(iter.valid());
		assert_eq!(iter.key().user_key(), b"aaa");
	}

	#[test]
	fn test_internal_iterator_seek_last_resets_after_exhaustion() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"aaa", 1), b"val1").unwrap();
		tree.insert(make_internal_key(b"bbb", 2), b"val2").unwrap();

		let mut iter = tree.internal_iterator();

		// Exhaust backward
		assert!(iter.seek_last().unwrap());
		while iter.prev().unwrap() {}
		assert!(!iter.valid());

		// seek_last() should reset and work
		assert!(iter.seek_last().unwrap());
		assert!(iter.valid());
		assert_eq!(iter.key().user_key(), b"bbb");
	}

	// ========== Large Dataset Tests ==========

	#[test]
	fn test_internal_iterator_many_entries() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		let count = 100;
		for i in 0..count {
			let key = format!("key{:04}", i);
			let val = format!("val{:04}", i);
			tree.insert(make_internal_key(key.as_bytes(), i as u64), val.as_bytes()).unwrap();
		}

		let mut iter = tree.internal_iterator();
		let collected = collect_forward(&mut iter);

		assert_eq!(collected.len(), count);

		// Verify ordering
		for (i, item) in collected.iter().enumerate().take(count) {
			let expected_key = format!("key{:04}", i);
			assert_eq!(item.0, expected_key.as_bytes());
		}
	}

	#[test]
	fn test_internal_iterator_many_entries_backward() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		let count = 100;
		for i in 0..count {
			let key = format!("key{:04}", i);
			let val = format!("val{:04}", i);
			tree.insert(make_internal_key(key.as_bytes(), i as u64), val.as_bytes()).unwrap();
		}

		let mut iter = tree.internal_iterator();
		let collected = collect_backward(&mut iter);

		assert_eq!(collected.len(), count);

		// Verify reverse ordering
		for (i, item) in collected.iter().enumerate().take(count) {
			let expected_key = format!("key{:04}", count - 1 - i);
			assert_eq!(item.0, expected_key.as_bytes());
		}
	}

	#[test]
	fn test_internal_iterator_forward_backward_match() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		for i in 0..50 {
			let key = format!("key{:04}", i);
			tree.insert(make_internal_key(key.as_bytes(), i as u64), b"value").unwrap();
		}

		let mut iter = tree.internal_iterator();
		let forward = collect_forward(&mut iter);

		let mut iter2 = tree.internal_iterator();
		let mut backward = collect_backward(&mut iter2);
		backward.reverse();

		assert_eq!(forward, backward);
	}

	#[test]
	fn test_internal_iterator_across_multiple_leaves() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		// Insert enough data to span multiple leaves
		// Larger values force more leaf splits
		for i in 0..100 {
			let key = format!("key{:04}", i);
			let val = vec![i as u8; 100]; // 100 byte values
			tree.insert(make_internal_key(key.as_bytes(), i as u64), &val).unwrap();
		}

		let mut iter = tree.internal_iterator();

		// Forward iteration
		assert!(iter.seek_first().unwrap());
		let mut forward_count = 1;
		while iter.next().unwrap() {
			forward_count += 1;
		}
		assert_eq!(forward_count, 100);

		// Backward iteration
		assert!(iter.seek_last().unwrap());
		let mut backward_count = 1;
		while iter.prev().unwrap() {
			backward_count += 1;
		}
		assert_eq!(backward_count, 100);
	}

	#[test]
	fn test_internal_iterator_bidirectional_at_leaf_boundary() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		// Insert data that will span multiple leaves
		for i in 0..50 {
			let key = format!("key{:04}", i);
			let val = vec![0u8; 200]; // Large values to force splits
			tree.insert(make_internal_key(key.as_bytes(), i as u64), &val).unwrap();
		}

		let mut iter = tree.internal_iterator();

		// Navigate to somewhere in the middle
		assert!(iter.seek(&make_internal_key(b"key0025", 0)).unwrap());

		// Zigzag across potential leaf boundaries
		for _ in 0..10 {
			let key_before = iter.key().user_key().to_vec();
			if iter.next().unwrap() {
				let key_after = iter.key().user_key().to_vec();
				assert!(iter.prev().unwrap());
				assert_eq!(iter.key().user_key(), key_before.as_slice());
				assert!(iter.next().unwrap());
				assert_eq!(iter.key().user_key(), key_after.as_slice());
			} else {
				break;
			}
		}
	}

	// ========== Edge Cases ==========

	#[test]
	fn test_internal_iterator_two_elements() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"first", 1), b"val1").unwrap();
		tree.insert(make_internal_key(b"second", 2), b"val2").unwrap();

		let mut iter = tree.internal_iterator();

		// Forward
		assert!(iter.seek_first().unwrap());
		assert_eq!(iter.key().user_key(), b"first");
		assert!(iter.next().unwrap());
		assert_eq!(iter.key().user_key(), b"second");
		assert!(!iter.next().unwrap());

		// Backward
		assert!(iter.seek_last().unwrap());
		assert_eq!(iter.key().user_key(), b"second");
		assert!(iter.prev().unwrap());
		assert_eq!(iter.key().user_key(), b"first");
		assert!(!iter.prev().unwrap());
	}

	#[test]
	fn test_internal_iterator_values_correct() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		tree.insert(make_internal_key(b"key1", 1), b"value_one").unwrap();
		tree.insert(make_internal_key(b"key2", 2), b"value_two").unwrap();
		tree.insert(make_internal_key(b"key3", 3), b"value_three").unwrap();

		let mut iter = tree.internal_iterator();
		assert!(iter.seek_first().unwrap());

		assert_eq!(iter.value_encoded().unwrap(), b"value_one");
		assert!(iter.next().unwrap());
		assert_eq!(iter.value_encoded().unwrap(), b"value_two");
		assert!(iter.next().unwrap());
		assert_eq!(iter.value_encoded().unwrap(), b"value_three");
	}

	#[test]
	fn test_internal_iterator_multiple_independent_iterators() {
		let temp_file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(temp_file.path(), Arc::new(TestComparator)).unwrap();

		for i in 0..10 {
			let key = format!("key{:02}", i);
			tree.insert(make_internal_key(key.as_bytes(), i as u64), b"value").unwrap();
		}

		// Create two iterators
		let mut iter1 = tree.internal_iterator();
		let mut iter2 = tree.internal_iterator();

		// Position them at different locations
		assert!(iter1.seek_first().unwrap());
		assert!(iter2.seek_last().unwrap());

		assert_eq!(iter1.key().user_key(), b"key00");
		assert_eq!(iter2.key().user_key(), b"key09");

		// Moving one shouldn't affect the other
		assert!(iter1.next().unwrap());
		assert_eq!(iter1.key().user_key(), b"key01");
		assert_eq!(iter2.key().user_key(), b"key09"); // Unchanged

		assert!(iter2.prev().unwrap());
		assert_eq!(iter2.key().user_key(), b"key08");
		assert_eq!(iter1.key().user_key(), b"key01"); // Unchanged
	}
}
