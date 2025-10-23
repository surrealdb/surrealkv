use std::cmp::Ordering;
use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::Arc;

use quick_cache::{sync::Cache, Weighter};

use crate::vfs::File as VfsFile;
use crate::{Comparator, Key, Value};

// These are type aliases for convenience
pub type DiskBPlusTree = BPlusTree<File>;

#[derive(Clone)]
struct NodeWeighter;

impl Weighter<u64, NodeType> for NodeWeighter {
	fn weight(&self, _key: &u64, value: &NodeType) -> u64 {
		match value {
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
	KeyValueTooLarge,
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
			BPlusTreeError::KeyValueTooLarge => write!(f, "Key-value pair too large"),
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
const CACHE_CAPACITY: u64 = 256 * 1024 * 1024; // 256 MiB
const NODE_TYPE_INTERNAL: u8 = 0;
const NODE_TYPE_LEAF: u8 = 1;
const NODE_TYPE_OVERFLOW: u8 = 3;

// Constants for trunk page system
const TRUNK_PAGE_TYPE: u8 = 2; // Node type for trunk pages
const TRUNK_PAGE_HEADER_SIZE: usize = 13; // 1 byte type + 8 bytes next + 4 bytes count
const TRUNK_PAGE_ENTRY_SIZE: usize = 4; // 4-byte entries
const TRUNK_RESERVED_ENTRIES: usize = 6; // Reserve last 6 entries
const TRUNK_PAGE_MAX_ENTRIES: usize =
	(PAGE_SIZE - TRUNK_PAGE_HEADER_SIZE) / TRUNK_PAGE_ENTRY_SIZE - TRUNK_RESERVED_ENTRIES;

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
			return Err(BPlusTreeError::Deserialization("Invalid magic number".into()));
		}

		let version = u32::from_le_bytes(buffer[8..12].try_into().unwrap());
		if version != VERSION {
			return Err(BPlusTreeError::Deserialization(format!(
				"Unsupported version: {}",
				version
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
	fn max_size() -> usize {
		PAGE_SIZE
	}

	// Check if this node could merge with another node
	fn can_merge_with(&self, other: &Self) -> bool;
}

#[derive(Debug, Clone)]
struct InternalNode {
	keys: Vec<Vec<u8>>, // Full keys (reconstructed from page + overflow if needed)
	key_overflows: Vec<u64>, // 0 if no overflow, else first overflow page offset
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

		let num_keys = u32::from_le_bytes(buffer_slice[..4].try_into().unwrap()) as usize;
		buffer_slice = &buffer_slice[4..];

		// Calculate max bytes per entry for overflow
		let max_per_entry = calculate_max_bytes_per_entry(false);

		let mut keys = Vec::with_capacity(num_keys);
		let mut key_overflows = Vec::with_capacity(num_keys);

		for _ in 0..num_keys {
			// Read total key length
			let key_len_total = u32::from_le_bytes(buffer_slice[..4].try_into().unwrap()) as usize;
			buffer_slice = &buffer_slice[4..];

			// Calculate bytes on page using simple overflow algorithm
			let (bytes_on_page, needs_overflow) = calculate_overflow(key_len_total, max_per_entry);

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
				let overflow = u64::from_le_bytes(buffer_slice[..8].try_into().unwrap());
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
			key_overflows,
			children,
			offset,
		})
	}

	// Find the appropriate child index for a key
	fn find_child_index(&self, key: &[u8], compare: &dyn Comparator) -> usize {
		// Binary search implementation
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

	// Insert a key and child pointer at the correct position
	fn insert_key_child(&mut self, key: &[u8], child_offset: u64, compare: &dyn Comparator) {
		let mut idx = 0;
		while idx < self.keys.len() && compare.compare(key, &self.keys[idx]) == Ordering::Greater {
			idx += 1;
		}

		self.keys.insert(idx, key.to_vec());
		self.key_overflows.insert(idx, 0); // New key, no overflow yet
		self.children.insert(idx + 1, child_offset);
	}

	// Redistributes a key and child from this node to the right node
	fn redistribute_to_right(&mut self, right: &mut InternalNode, parent_key: Vec<u8>) -> Vec<u8> {
		// Move parent key down to right node
		right.keys.insert(0, parent_key);
		right.key_overflows.insert(0, 0); // Parent key redistributed, reset overflow

		// Move last key from this node to be the new parent key
		let new_parent_key = self.keys.pop().unwrap();
		if !self.key_overflows.is_empty() {
			self.key_overflows.pop();
		}

		// Move last child from this node to right node
		if !self.children.is_empty() {
			right.children.insert(0, self.children.pop().unwrap());
		}

		// Return the new parent key
		new_parent_key
	}

	// Takes a key and child from the right node
	fn take_from_right(&mut self, right: &mut InternalNode, parent_key: Vec<u8>) -> Vec<u8> {
		// Move parent key down to this node
		self.keys.push(parent_key);
		self.key_overflows.push(0); // Parent key redistributed, reset overflow

		// Move first key from right node to be the new parent key
		let new_parent_key = right.keys.remove(0);
		if !right.key_overflows.is_empty() {
			right.key_overflows.remove(0);
		}

		// Move first child from right node to this node
		if !right.children.is_empty() {
			self.children.push(right.children.remove(0));
		}

		// Return the new parent key
		new_parent_key
	}

	// Merges right node into this node
	fn merge_from_right(&mut self, right: &InternalNode, separator: Vec<u8>) {
		// Add separator key from parent
		self.keys.push(separator);
		self.key_overflows.push(0); // Separator from parent, no overflow

		// Append right node's keys and children
		self.keys.extend(right.keys.clone());
		self.key_overflows.extend(right.key_overflows.clone());
		self.children.extend(right.children.clone());
	}

	// Check if node is considered to be in underflow state
	fn is_underflow(&self) -> bool {
		// Consider a node to be in underflow if it has less than 30% capacity utilized
		// This is based on my reading from sqllite implementation
		self.current_size() < Self::max_size() * 30 / 100
	}
}

impl Node for InternalNode {
	fn serialize(&self) -> Result<Vec<u8>> {
		let mut buffer = Vec::with_capacity(PAGE_SIZE);

		// 1. Node type (1 byte)
		buffer.push(NODE_TYPE_INTERNAL);

		// 2. Number of keys (4 bytes)
		buffer.extend_from_slice(&(self.keys.len() as u32).to_le_bytes());

		// Calculate max bytes per entry for overflow
		let max_per_entry = calculate_max_bytes_per_entry(false);

		// 3. Serialize keys with overflow support
		for (i, key) in self.keys.iter().enumerate() {
			let key_len_total = key.len();

			// Determine how much to store on page
			let (bytes_on_page, needs_overflow) = calculate_overflow(key_len_total, max_per_entry);

			// Write total key length
			buffer.extend_from_slice(&(key_len_total as u32).to_le_bytes());

			// Write key data that fits on page
			buffer.extend_from_slice(&key[..bytes_on_page]);

			// Write overflow page offset if needed (only if there's overflow)
			if needs_overflow {
				let overflow_offset = if i < self.key_overflows.len() {
					self.key_overflows[i]
				} else {
					0 // Should not happen if properly initialized
				};
				buffer.extend_from_slice(&overflow_offset.to_le_bytes());
			}
		}

		// 4. Number of children (4 bytes)
		buffer.extend_from_slice(&(self.children.len() as u32).to_le_bytes());

		// 5. Child pointers (8 bytes each)
		for &child in &self.children {
			buffer.extend_from_slice(&child.to_le_bytes());
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

		// Calculate max bytes per entry for overflow
		let max_per_entry = calculate_max_bytes_per_entry(false);

		// Size for all keys accounting for overflow
		for key in &self.keys {
			let (key_bytes_on_page, needs_overflow) = calculate_overflow(key.len(), max_per_entry);
			size += KEY_SIZE_PREFIX + key_bytes_on_page;
			if needs_overflow {
				size += 8; // overflow pointer
			}
		}

		// Size for all child pointers
		size += self.children.len() * CHILD_PTR_SIZE;

		size
	}

	fn can_merge_with(&self, other: &Self) -> bool {
		let combined_size = self.current_size() + other.current_size();
		let actual_merged_size = combined_size - INTERNAL_HEADER_SIZE;

		// We also need to add space for the separator key from parent
		// Use the maximum key size instead of average to be safe
		let max_key_size = if !self.keys.is_empty() && !other.keys.is_empty() {
			let self_max = self.keys.iter().map(|k| k.len()).max().unwrap_or(0);
			let other_max = other.keys.iter().map(|k| k.len()).max().unwrap_or(0);
			self_max.max(other_max)
		} else if !self.keys.is_empty() {
			self.keys.iter().map(|k| k.len()).max().unwrap_or(0)
		} else if !other.keys.is_empty() {
			other.keys.iter().map(|k| k.len()).max().unwrap_or(0)
		} else {
			0 // Edge case: both nodes have no keys
		};

		// Calculate separator size accounting for overflow
		let max_per_entry = calculate_max_bytes_per_entry(false);
		let (separator_on_page, separator_overflow) =
			calculate_overflow(max_key_size, max_per_entry);
		let separator_size = KEY_SIZE_PREFIX
			+ separator_on_page
			+ (if separator_overflow {
				8
			} else {
				0
			});

		let with_separator = actual_merged_size + separator_size;

		// Check if the combined size is within the maximum
		with_separator <= Self::max_size()
	}
}

#[derive(Debug, Clone)]
struct LeafNode {
	keys: Vec<Vec<u8>>, // Full keys (reconstructed from page + overflow if needed)
	key_overflows: Vec<u64>, // 0 if no overflow, else first overflow page offset
	values: Vec<Vec<u8>>, // Full values (reconstructed from page + overflow if needed)
	value_overflows: Vec<u64>, // 0 if no overflow, else first overflow page offset
	next_leaf: u64,     // 0 means no next leaf
	prev_leaf: u64,     // 0 means no previous leaf
	offset: u64,
}

impl LeafNode {
	fn new(offset: u64) -> Self {
		LeafNode {
			keys: Vec::new(),
			key_overflows: Vec::new(),
			values: Vec::new(),
			value_overflows: Vec::new(),
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

		let mut buffer_slice = &buffer[1..]; // Skip node type

		let num_keys = u32::from_le_bytes(buffer_slice[..4].try_into().unwrap()) as usize;
		buffer_slice = &buffer_slice[4..];

		// Next and prev leaf pointers
		let next_leaf = u64::from_le_bytes(buffer_slice[..8].try_into().unwrap());
		buffer_slice = &buffer_slice[8..];
		let prev_leaf = u64::from_le_bytes(buffer_slice[..8].try_into().unwrap());
		buffer_slice = &buffer_slice[8..];

		// Calculate max bytes per entry for overflow
		let max_per_entry = calculate_max_bytes_per_entry(true);

		let mut keys = Vec::with_capacity(num_keys);
		let mut key_overflows = Vec::with_capacity(num_keys);
		let mut values = Vec::with_capacity(num_keys);
		let mut value_overflows = Vec::with_capacity(num_keys);

		for _ in 0..num_keys {
			// Read key with overflow support
			if buffer_slice.len() < 4 {
				return Err(BPlusTreeError::Deserialization("Truncated key length".into()));
			}
			let key_len_total = u32::from_le_bytes(buffer_slice[..4].try_into().unwrap()) as usize;
			buffer_slice = &buffer_slice[4..];

			// Calculate bytes on page for key
			let (key_bytes_on_page, key_needs_overflow) =
				calculate_overflow(key_len_total, max_per_entry);

			if key_bytes_on_page > buffer_slice.len() {
				return Err(BPlusTreeError::Deserialization(format!(
					"Calculated key bytes on page {} exceeds available buffer size {}",
					key_bytes_on_page,
					buffer_slice.len()
				)));
			}

			// Read key data from page
			let mut key_data = buffer_slice[..key_bytes_on_page].to_vec();
			buffer_slice = &buffer_slice[key_bytes_on_page..];

			// Check if key has overflow
			let key_overflow_offset = if key_needs_overflow {
				// Read overflow page offset
				if buffer_slice.len() < 8 {
					return Err(BPlusTreeError::Deserialization(
						"Buffer too small for key overflow pointer".into(),
					));
				}
				let overflow = u64::from_le_bytes(buffer_slice[..8].try_into().unwrap());
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
			key_overflows.push(key_overflow_offset);

			// Read total value length
			if buffer_slice.len() < 4 {
				return Err(BPlusTreeError::Deserialization("Truncated value length".into()));
			}
			let value_len_total =
				u32::from_le_bytes(buffer_slice[..4].try_into().unwrap()) as usize;
			buffer_slice = &buffer_slice[4..];

			// Calculate bytes on page using simple overflow algorithm
			let (bytes_on_page, needs_overflow) =
				calculate_overflow(value_len_total, max_per_entry);

			if bytes_on_page > buffer_slice.len() {
				return Err(BPlusTreeError::Deserialization(format!(
					"Calculated bytes on page {} exceeds available buffer size {}",
					bytes_on_page,
					buffer_slice.len()
				)));
			}

			// Read value data from page
			let mut value_data = buffer_slice[..bytes_on_page].to_vec();
			buffer_slice = &buffer_slice[bytes_on_page..];

			// Check if there's overflow
			let overflow_offset = if needs_overflow {
				// Read overflow page offset
				if buffer_slice.len() < 8 {
					return Err(BPlusTreeError::Deserialization(
						"Buffer too small for overflow pointer".into(),
					));
				}
				let overflow = u64::from_le_bytes(buffer_slice[..8].try_into().unwrap());
				buffer_slice = &buffer_slice[8..];

				// Read overflow data and append to value
				let overflow_data = read_overflow(overflow)?;
				value_data.extend_from_slice(&overflow_data);

				overflow
			} else {
				0
			};

			// Validate reconstructed value size
			if value_data.len() != value_len_total {
				return Err(BPlusTreeError::Deserialization(format!(
					"Reconstructed value size {} doesn't match expected {}",
					value_data.len(),
					value_len_total
				)));
			}

			values.push(value_data);
			value_overflows.push(overflow_offset);
		}

		Ok(LeafNode {
			keys,
			key_overflows,
			values,
			value_overflows,
			next_leaf,
			prev_leaf,
			offset,
		})
	}

	// insert a key-value pair into a leaf node at the correct position
	// If key already exists, update the value (treat as update)
	fn insert(&mut self, key: &[u8], value: &[u8], compare: &dyn Comparator) -> usize {
		let mut idx = 0;
		while idx < self.keys.len() && compare.compare(key, &self.keys[idx]) == Ordering::Greater {
			idx += 1;
		}

		// Check if key already exists at this position
		if idx < self.keys.len() && compare.compare(key, &self.keys[idx]) == Ordering::Equal {
			// Key exists, update the value
			self.values[idx] = value.to_vec();
			// Reset overflow for this value (will be recalculated on write)
			if idx < self.value_overflows.len() {
				self.value_overflows[idx] = 0;
			}
		} else {
			// Key doesn't exist, insert new entry
			self.keys.insert(idx, key.to_vec());
			self.key_overflows.insert(idx, 0); // New key, no overflow yet
			self.values.insert(idx, value.to_vec());
			self.value_overflows.insert(idx, 0); // New value, no overflow yet
		}
		idx
	}

	// delete a key-value pair from a leaf node
	fn delete(&mut self, key: &[u8], compare: &dyn Comparator) -> Option<(usize, Vec<u8>)> {
		let idx = self.keys.iter().position(|k| compare.compare(key, k) == Ordering::Equal)?;
		let value = self.values.remove(idx);
		self.keys.remove(idx);
		// Remove overflow entries as well
		if idx < self.key_overflows.len() {
			self.key_overflows.remove(idx);
		}
		if idx < self.value_overflows.len() {
			self.value_overflows.remove(idx);
		}
		Some((idx, value))
	}

	// Find a key's position in the leaf
	fn find_key(&self, key: &[u8], compare: &dyn Comparator) -> Option<usize> {
		// Binary search to find exact match
		let mut low = 0;
		let mut high = self.keys.len();

		while low < high {
			let mid = low + (high - low) / 2;
			match compare.compare(key, &self.keys[mid]) {
				Ordering::Less => high = mid,
				Ordering::Equal => return Some(mid),
				Ordering::Greater => low = mid + 1,
			}
		}

		None
	}

	// Redistributes keys from this leaf to the target leaf
	fn redistribute_to_right(&mut self, right: &mut LeafNode) -> Vec<u8> {
		// Move last key-value pair from this node to right node
		let last_key = self.keys.pop().unwrap();
		let last_value = self.values.pop().unwrap();

		right.keys.insert(0, last_key.clone());
		right.values.insert(0, last_value);

		// Return the separator key (first key in right node)
		right.keys[0].clone()
	}

	// Takes keys from right leaf
	fn take_from_right(&mut self, right: &mut LeafNode) -> Vec<u8> {
		// Move first key-value pair from right node to this node
		let first_key = right.keys.remove(0);
		let first_value = right.values.remove(0);

		self.keys.push(first_key.clone());
		self.values.push(first_value);

		// If right node still has keys, return its first key as the new separator
		if !right.keys.is_empty() {
			right.keys[0].clone()
		} else {
			first_key
		}
	}

	// Merges the right node into this node
	fn merge_from_right(&mut self, right: &LeafNode) {
		// Append all keys and values from right node
		self.keys.extend(right.keys.clone());
		self.values.extend(right.values.clone());

		// Update next_leaf pointer
		self.next_leaf = right.next_leaf;
	}

	// Find optimal split point - simple midpoint with overflow support
	fn find_split_point(&self, _key: &[u8], _value: &[u8], _compare: &dyn Comparator) -> usize {
		// Simple O(1) midpoint split - works because overflow pages guarantee
		// values can always fit with overflow support
		let total_entries = self.keys.len() + 1; // including new entry
		let mut split_idx = total_entries / 2;

		// Clamp to valid range
		split_idx = split_idx.clamp(1, total_entries - 1);

		split_idx
	}

	// Check if this leaf can fit another key-value pair
	fn can_fit_entry(&self, key: &[u8], value: &[u8]) -> bool {
		let max_per_entry = calculate_max_bytes_per_entry(true);
		let (key_on_page, key_overflow) = calculate_overflow(key.len(), max_per_entry);
		let (val_on_page, val_overflow) = calculate_overflow(value.len(), max_per_entry);

		let entry_size = KEY_SIZE_PREFIX
			+ key_on_page
			+ VALUE_SIZE_PREFIX
			+ val_on_page
			+ (if key_overflow {
				8
			} else {
				0
			}) + (if val_overflow {
			8
		} else {
			0
		});

		self.current_size() + entry_size <= PAGE_SIZE
	}

	// Check if node is considered to be in underflow state
	fn is_underflow(&self) -> bool {
		// Consider a node to be in underflow if it has less than 30% capacity utilized
		// This is based on my reading from sqllite implementation
		self.current_size() < Self::max_size() * 30 / 100
	}
}

impl Node for LeafNode {
	fn serialize(&self) -> Result<Vec<u8>> {
		let mut buffer = Vec::with_capacity(PAGE_SIZE);

		// 1. Node type (1 byte)
		buffer.push(NODE_TYPE_LEAF);

		// 2. Number of keys (4 bytes)
		buffer.extend_from_slice(&(self.keys.len() as u32).to_le_bytes());

		// 3. Next leaf pointer (8 bytes)
		buffer.extend_from_slice(&self.next_leaf.to_le_bytes());

		// 4. Previous leaf pointer (8 bytes)
		buffer.extend_from_slice(&self.prev_leaf.to_le_bytes());

		// Calculate max bytes per entry for overflow
		let max_per_entry = calculate_max_bytes_per_entry(true);

		// 5. Serialize keys and values with overflow support
		for (i, (key, value)) in self.keys.iter().zip(&self.values).enumerate() {
			// Key serialization with overflow support
			let key_len_total = key.len();
			let (key_bytes_on_page, key_needs_overflow) =
				calculate_overflow(key_len_total, max_per_entry);

			// Write total key length
			buffer.extend_from_slice(&(key_len_total as u32).to_le_bytes());

			// Write key data that fits on page
			buffer.extend_from_slice(&key[..key_bytes_on_page]);

			// Write key overflow page offset if needed
			if key_needs_overflow {
				let overflow_offset = if i < self.key_overflows.len() {
					self.key_overflows[i]
				} else {
					0
				};
				buffer.extend_from_slice(&overflow_offset.to_le_bytes());
			}

			// Value serialization with overflow support
			let value_len_total = value.len();
			let (value_bytes_on_page, value_needs_overflow) =
				calculate_overflow(value_len_total, max_per_entry);

			// Write total value length
			buffer.extend_from_slice(&(value_len_total as u32).to_le_bytes());

			// Write value data that fits on page
			buffer.extend_from_slice(&value[..value_bytes_on_page]);

			// Write value overflow page offset if needed
			if value_needs_overflow {
				let overflow_offset = if i < self.value_overflows.len() {
					self.value_overflows[i]
				} else {
					0 // Should not happen if properly initialized
				};
				buffer.extend_from_slice(&overflow_offset.to_le_bytes());
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

		// Calculate max bytes per entry for overflow
		let max_per_entry = calculate_max_bytes_per_entry(true);

		// Size for all keys and values accounting for overflow
		// We need to calculate what's actually stored on the page, not total payload
		for (key, value) in self.keys.iter().zip(&self.values) {
			// Key size on page
			let (key_bytes_on_page, key_needs_overflow) =
				calculate_overflow(key.len(), max_per_entry);
			size += KEY_SIZE_PREFIX + key_bytes_on_page;
			if key_needs_overflow {
				size += 8; // overflow pointer
			}

			// Value size on page
			let (value_bytes_on_page, value_needs_overflow) =
				calculate_overflow(value.len(), max_per_entry);
			size += VALUE_SIZE_PREFIX + value_bytes_on_page;
			if value_needs_overflow {
				size += 8; // overflow pointer
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
		buffer.extend_from_slice(&self.next_trunk.to_le_bytes());

		// 3. Number of free pages (4 bytes)
		buffer.extend_from_slice(&self.num_free_pages.to_le_bytes());

		// 4. Free page numbers (4 bytes each, little-endian to match the rest of the code)
		for &page in &self.free_pages {
			buffer.extend_from_slice(&page.to_le_bytes());
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
		let next_trunk = u64::from_le_bytes(buffer[1..9].try_into().unwrap());

		// Read number of free pages (4 bytes)
		let num_free_pages = u32::from_le_bytes(buffer[9..13].try_into().unwrap()) as usize;

		// Check if num_free_pages is reasonable
		if num_free_pages > TRUNK_PAGE_MAX_ENTRIES {
			return Err(BPlusTreeError::Deserialization(format!(
				"Invalid number of free pages in trunk: {} (max {})",
				num_free_pages, TRUNK_PAGE_MAX_ENTRIES
			)));
		}

		// Read free page numbers (4 bytes each, little-endian)
		let mut free_pages = Vec::with_capacity(num_free_pages);
		for i in 0..num_free_pages {
			let start = TRUNK_PAGE_HEADER_SIZE + i * 4;
			let end = start + 4;
			if end > buffer.len() {
				return Err(BPlusTreeError::Deserialization("Truncated trunk page data".into()));
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

// Overflow page structure for storing large keys/values that don't fit on a single page
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
		buffer.extend_from_slice(&self.next_overflow.to_le_bytes());

		// 3. Data length (4 bytes)
		buffer.extend_from_slice(&(self.data.len() as u32).to_le_bytes());

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
		let next_overflow = u64::from_le_bytes(buffer[1..9].try_into().unwrap());

		// Read data length (4 bytes)
		let data_len = u32::from_le_bytes(buffer[9..13].try_into().unwrap()) as usize;

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

	/// Only sync when flush() or close() is called (fast but risks data loss on crash)
	Manual,
}

// Simple overflow for sequential B+ tree storage

/// Calculate maximum bytes per entry to ensure minimum entries per page
fn calculate_max_bytes_per_entry(is_leaf: bool) -> usize {
	let min_entries = if is_leaf {
		3
	} else {
		4
	};
	let header_size = if is_leaf {
		LEAF_HEADER_SIZE
	} else {
		INTERNAL_HEADER_SIZE
	};
	let usable = PAGE_SIZE - header_size;

	// Reserve space for: entry metadata (length fields) + overflow pointers
	let metadata_per_entry = if is_leaf {
		KEY_SIZE_PREFIX + VALUE_SIZE_PREFIX + 16 // +16 for potential overflow ptrs (key+value)
	} else {
		KEY_SIZE_PREFIX + CHILD_PTR_SIZE + 8 // +8 for overflow ptr
	};

	(usable / min_entries).saturating_sub(metadata_per_entry)
}

/// Calculate how much of payload to store on page
/// Returns (bytes_on_page, needs_overflow)
fn calculate_overflow(payload_size: usize, max_per_entry: usize) -> (usize, bool) {
	if payload_size <= max_per_entry {
		(payload_size, false)
	} else {
		// Keep half or max_per_entry, whichever is smaller
		// Ensures we store meaningful data on page while supporting overflow
		let bytes_on_page = (payload_size / 2).min(max_per_entry);
		(bytes_on_page.max(256).min(max_per_entry), true)
	}
}

pub struct BPlusTree<F: VfsFile> {
	file: F,
	header: Header,
	cache: Cache<u64, NodeType, NodeWeighter>,
	compare: Arc<dyn Comparator>,
	durability: Durability,
}

impl<F: VfsFile> Drop for BPlusTree<F> {
	fn drop(&mut self) {
		if let Err(e) = self.flush() {
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
			let cache = Cache::with_weighter(CACHE_CAPACITY as usize, CACHE_CAPACITY, NodeWeighter);

			(header, cache)
		} else {
			// Read existing header
			let mut buffer = [0u8; 48];
			file.read_at(0, &mut buffer)?;
			let header = Header::deserialize(&buffer)?;

			let cache = Cache::with_weighter(CACHE_CAPACITY as usize, CACHE_CAPACITY, NodeWeighter);

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
			tree.file.sync()?;
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

	pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
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
					let child_idx = internal.find_child_index(key, self.compare.as_ref());
					parent_offset = Some(current_offset);
					current_offset = internal.children[child_idx];

					// Continue to next iteration (deeper in the tree)
				}
				NodeType::Leaf(mut leaf) => {
					// Found the leaf node where key should be inserted
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
				NodeType::Overflow(_) => {
					return Err(BPlusTreeError::Corruption(format!(
						"Unexpected overflow page in tree traversal at offset {}",
						current_offset
					)));
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

					// Check if adding this entry would put us close to the maximum size
					// Use overflow-aware calculation for promoted key
					let max_per_entry = calculate_max_bytes_per_entry(false);
					let (key_on_page, key_overflow) =
						calculate_overflow(promoted_key.len(), max_per_entry);
					let entry_size = KEY_SIZE_PREFIX
						+ key_on_page + CHILD_PTR_SIZE
						+ (if key_overflow {
							8
						} else {
							0
						});
					let would_be_size = parent.current_size() + entry_size;
					let size_threshold = InternalNode::max_size();

					if would_be_size <= size_threshold {
						// Parent has enough space with buffer, insert the new key and child
						parent.insert_key_child(
							&promoted_key,
							new_node_offset,
							self.compare.as_ref(),
						);
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
		leaf.insert(key, value, self.compare.as_ref());
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
		let split_idx = leaf.find_split_point(key, value, self.compare.as_ref());

		// Create new leaf
		let new_leaf_offset = self.allocate_page()?;
		let mut new_leaf = LeafNode::new(new_leaf_offset);

		// Find insertion point for the new key-value
		let mut idx = 0;
		while idx < leaf.keys.len()
			&& self.compare.compare(key, &leaf.keys[idx]) == Ordering::Greater
		{
			idx += 1;
		}

		// Check if key already exists
		let is_duplicate =
			idx < leaf.keys.len() && self.compare.compare(key, &leaf.keys[idx]) == Ordering::Equal;

		if idx < split_idx {
			// New entry belongs in the left node
			// Move entries after split_idx to new leaf
			new_leaf.keys = leaf.keys.drain(split_idx..).collect();
			new_leaf.key_overflows = leaf.key_overflows.drain(split_idx..).collect();
			new_leaf.values = leaf.values.drain(split_idx..).collect();
			new_leaf.value_overflows = leaf.value_overflows.drain(split_idx..).collect();

			if is_duplicate {
				// Update existing key-value in left node
				leaf.values[idx] = value.to_vec();
				// Reset overflow for updated value
				if idx < leaf.value_overflows.len() {
					leaf.value_overflows[idx] = 0;
				}
			} else {
				// Insert the new key-value into leaf
				leaf.keys.insert(idx, key.to_vec());
				leaf.key_overflows.insert(idx, 0);
				leaf.values.insert(idx, value.to_vec());
				leaf.value_overflows.insert(idx, 0);
			}
		} else {
			// New entry belongs in the right node
			// Adjust index for the right node
			let right_idx = idx - split_idx;

			// Move entries after split_idx to new leaf
			new_leaf.keys = leaf.keys.drain(split_idx..).collect();
			new_leaf.key_overflows = leaf.key_overflows.drain(split_idx..).collect();
			new_leaf.values = leaf.values.drain(split_idx..).collect();
			new_leaf.value_overflows = leaf.value_overflows.drain(split_idx..).collect();

			if is_duplicate {
				// Update existing key-value in right node
				new_leaf.values[right_idx] = value.to_vec();
				// Reset overflow for updated value
				if right_idx < new_leaf.value_overflows.len() {
					new_leaf.value_overflows[right_idx] = 0;
				}
			} else {
				// Insert the new key-value into new leaf
				new_leaf.keys.insert(right_idx, key.to_vec());
				new_leaf.key_overflows.insert(right_idx, 0);
				new_leaf.values.insert(right_idx, value.to_vec());
				new_leaf.value_overflows.insert(right_idx, 0);
			}
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
		// Find where the extra key would be inserted
		let insert_idx = node
			.keys
			.binary_search_by(|key| self.compare.compare(key, extra_key))
			.unwrap_or_else(|idx| idx);

		let mut split_idx = Self::find_split_point(node, extra_key, insert_idx);

		// Just bounds check the split_idx before using it
		split_idx = split_idx.min(node.keys.len() - 1);

		// Create new internal node
		let new_node_offset = self.allocate_page()?;
		let mut new_node = InternalNode::new(new_node_offset);

		// Key to be promoted to parent
		let promoted_key = node.keys[split_idx].clone();

		// Move keys, key_overflows, and children after split point to new node
		new_node.keys = node.keys.split_off(split_idx + 1);
		new_node.key_overflows = node.key_overflows.split_off(split_idx + 1);
		new_node.children = node.children.split_off(split_idx + 1);

		// Remove the middle key from the original node (it gets promoted)
		node.keys.truncate(split_idx);
		node.key_overflows.truncate(split_idx);

		// NOW insert the extra key/child into the appropriate node
		if insert_idx <= split_idx {
			// Extra key goes to left node (original)
			node.insert_key_child(extra_key, extra_child, self.compare.as_ref());
		} else {
			// Extra key goes to right node (new)
			let right_insert_idx = insert_idx - split_idx - 1;
			new_node.keys.insert(right_insert_idx, extra_key.to_vec());
			new_node.key_overflows.insert(right_insert_idx, 0);
			new_node.children.insert(right_insert_idx + 1, extra_child);
		}

		// Write both nodes
		self.write_node(&NodeType::Internal(node.clone()))?;
		self.write_node(&NodeType::Internal(new_node))?;

		Ok((promoted_key, new_node_offset))
	}

	fn find_split_point(
		node: &InternalNode,
		_extra_key: &[u8], // Not needed for midpoint split
		insert_idx: usize,
	) -> usize {
		let total_keys = node.keys.len() + 1; // including extra_key
		let mut split_idx = total_keys / 2;

		// Clamp to valid range
		split_idx = split_idx.clamp(1, node.keys.len() - 1);

		// Adjust for virtual key space (accounting for extra_key insertion)
		if split_idx > insert_idx {
			split_idx - 1
		} else {
			split_idx
		}
	}

	#[allow(unused)]
	pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
		self.get_internal(self.header.root_offset, key)
	}

	fn get_internal(&self, node_offset: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
		match self.read_node(node_offset)? {
			NodeType::Internal(internal) => {
				// Find appropriate child
				let child_idx = internal.find_child_index(key, self.compare.as_ref());

				// Search in the appropriate child
				self.get_internal(internal.children[child_idx], key)
			}
			NodeType::Leaf(leaf) => {
				// Search for the key in the leaf
				match leaf.find_key(key, self.compare.as_ref()) {
					Some(idx) => Ok(Some(leaf.values[idx].clone())),
					None => Ok(None),
				}
			}
			NodeType::Overflow(_) => Err(BPlusTreeError::Corruption(format!(
				"Unexpected overflow page in tree traversal at offset {}",
				node_offset
			))),
		}
	}

	pub fn delete(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
		// Start with the root node
		let mut node_offset = self.header.root_offset;
		let mut path = Vec::new(); // Track path from root to leaf

		// Step 1: Traverse to the leaf containing the key (or where it should be)
		loop {
			let node_type = if let Some(node) = self.cache.get(&node_offset) {
				node.clone()
			} else {
				self.read_node(node_offset)?
			};

			match node_type {
				NodeType::Internal(internal) => {
					// Find child index and push current node to path
					let child_idx = internal.find_child_index(key, self.compare.as_ref());
					path.push((node_offset, child_idx));
					node_offset = internal.children[child_idx];
				}
				NodeType::Leaf(mut leaf) => {
					// Found the leaf node, attempt to delete
					match leaf.delete(key, self.compare.as_ref()) {
						Some((_, value)) => {
							// Write updated leaf first
							self.write_node(&NodeType::Leaf(leaf.clone()))?;

							// Step 2: Handle node underflow from bottom up
							self.handle_underflows(&mut path)?;

							// Step 3: Handle potentially empty root
							self.handle_empty_root()?;

							return Ok(Some(value));
						}
						None => return Ok(None), // Key not found
					}
				}
				NodeType::Overflow(_) => {
					return Err(BPlusTreeError::Corruption(format!(
						"Unexpected overflow page in tree traversal at offset {}",
						node_offset
					)));
				}
			}
		}
	}

	fn handle_underflows(&mut self, path: &mut Vec<(u64, usize)>) -> Result<()> {
		// Process path from leaf up to root
		while let Some((parent_offset, child_idx)) = path.pop() {
			let mut parent = match self.read_node(parent_offset)? {
				NodeType::Internal(internal) => internal,
				_ => return Err(BPlusTreeError::InvalidNodeType),
			};

			let child_offset = parent.children[child_idx];

			// Check if child needs rebalancing
			match self.read_node(child_offset)? {
				NodeType::Internal(internal) => {
					if internal.is_underflow() {
						// Handle underflow in internal node
						self.handle_underflow(&mut parent, child_idx, true)?;
						self.write_node(&NodeType::Internal(parent.clone()))?;

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
						self.write_node(&NodeType::Internal(parent.clone()))?;

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
					return Err(BPlusTreeError::Corruption(format!(
						"Unexpected overflow page in tree traversal at offset {}",
						child_offset
					)));
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
		// Try to borrow from left sibling first
		if child_idx > 0 {
			let left_sibling_offset = parent.children[child_idx - 1];

			if is_internal {
				// For internal nodes
				if let NodeType::Internal(left_node) = self.read_node(left_sibling_offset)? {
					// Check if left sibling has enough to redistribute and is not in underflow
					if !left_node.is_underflow() {
						self.redistribute_internal_from_left(parent, child_idx - 1, child_idx)?;
						return Ok(());
					}
				}
			} else {
				// For leaf nodes
				if let NodeType::Leaf(left_node) = self.read_node(left_sibling_offset)? {
					// Check if left sibling has enough to redistribute and is not in underflow
					if !left_node.is_underflow() {
						self.redistribute_leaf_from_left(parent, child_idx - 1, child_idx)?;
						return Ok(());
					}
				}
			}
		}

		// Try to borrow from right sibling if left redistribution wasn't possible
		if child_idx < parent.children.len() - 1 {
			let right_sibling_offset = parent.children[child_idx + 1];

			if is_internal {
				// For internal nodes
				if let NodeType::Internal(right_node) = self.read_node(right_sibling_offset)? {
					// Check if right sibling has enough to redistribute and is not in underflow
					if !right_node.is_underflow() {
						self.redistribute_internal_from_right(parent, child_idx, child_idx + 1)?;
						return Ok(());
					}
				}
			} else {
				// For leaf nodes
				if let NodeType::Leaf(right_node) = self.read_node(right_sibling_offset)? {
					// Check if right sibling has enough to redistribute and is not in underflow
					if !right_node.is_underflow() {
						self.redistribute_leaf_from_right(parent, child_idx, child_idx + 1)?;
						return Ok(());
					}
				}
			}
		}

		// If we reach here, redistribution wasn't possible, we need to merge nodes

		// Prefer merging with left sibling if possible
		if child_idx > 0 {
			let left_idx = child_idx - 1;
			if is_internal {
				self.merge_internal_nodes(parent, left_idx, child_idx)?;
			} else {
				self.merge_leaf_nodes(parent, left_idx, child_idx)?;
			}
		}
		// Otherwise merge with right sibling
		else if child_idx < parent.children.len() - 1 {
			let right_idx = child_idx + 1;
			if is_internal {
				self.merge_internal_nodes(parent, child_idx, right_idx)?;
			} else {
				self.merge_leaf_nodes(parent, child_idx, right_idx)?;
			}
		}
		// There should always be a sibling to merge with unless this is the root
		// which is handled separately in handle_empty_root

		Ok(())
	}

	// Handles the case where the root might be empty after deletion
	fn handle_empty_root(&mut self) -> Result<()> {
		match self.read_node(self.header.root_offset)? {
			NodeType::Internal(internal) => {
				if internal.keys.is_empty() && internal.children.len() == 1 {
					// The root is empty, make its only child the new root
					let old_root_offset = self.header.root_offset;
					self.header.root_offset = internal.children[0];
					self.write_header()?;

					// Free the old root with overflow chains
					self.free_node_with_overflow(old_root_offset)?;
				}
			}
			NodeType::Leaf(_) => {
				// If the root is a leaf - we don't remove it even if empty
				// This makes the tree always have a root
			}
			NodeType::Overflow(_) => {
				return Err(BPlusTreeError::Corruption(format!(
					"Root node is an overflow page at offset {}",
					self.header.root_offset
				)));
			}
		}
		Ok(())
	}

	fn redistribute_internal_from_left(
		&mut self,
		parent: &mut InternalNode,
		left_idx: usize,
		right_idx: usize,
	) -> Result<()> {
		let left_offset = parent.children[left_idx];
		let right_offset = parent.children[right_idx];

		let mut left_node = match self.read_node(left_offset)? {
			NodeType::Internal(node) => node,
			_ => return Err(BPlusTreeError::InvalidNodeType),
		};

		let mut right_node = match self.read_node(right_offset)? {
			NodeType::Internal(node) => node,
			_ => return Err(BPlusTreeError::InvalidNodeType),
		};

		// Calculate size of last entry in left node (accounting for overflow)
		let last_entry_size = if !left_node.keys.is_empty() {
			let max_per_entry = calculate_max_bytes_per_entry(false);
			let key_len = left_node.keys.last().unwrap().len();
			let (key_on_page, key_overflow) = calculate_overflow(key_len, max_per_entry);

			KEY_SIZE_PREFIX
				+ key_on_page
				+ CHILD_PTR_SIZE
				+ (if key_overflow {
					8
				} else {
					0
				})
		} else {
			return Err(BPlusTreeError::Serialization(
				"Left internal node is unexpectedly empty during redistribution".into(),
			));
		};

		// Only redistribute if it would improve balance
		if Self::should_redistribute_nodes(
			&left_node,
			&right_node,
			left_node.keys.len() - 1,
			last_entry_size,
		) {
			// Get the current parent key
			let parent_key = parent.keys[left_idx].clone();
			let new_parent_key = left_node.redistribute_to_right(&mut right_node, parent_key);

			// Update parent key
			parent.keys[left_idx] = new_parent_key;

			// Write updated nodes
			self.write_node(&NodeType::Internal(left_node))?;
			self.write_node(&NodeType::Internal(right_node))?;
		}

		// Parent will be written by the calling function
		Ok(())
	}

	fn redistribute_internal_from_right(
		&mut self,
		parent: &mut InternalNode,
		left_idx: usize,
		right_idx: usize,
	) -> Result<()> {
		let left_offset = parent.children[left_idx];
		let right_offset = parent.children[right_idx];

		let mut left_node = match self.read_node(left_offset)? {
			NodeType::Internal(node) => node,
			_ => return Err(BPlusTreeError::InvalidNodeType),
		};

		let mut right_node = match self.read_node(right_offset)? {
			NodeType::Internal(node) => node,
			_ => return Err(BPlusTreeError::InvalidNodeType),
		};

		// Calculate size of first entry in right node (accounting for overflow)
		let first_entry_size = if !right_node.keys.is_empty() {
			let max_per_entry = calculate_max_bytes_per_entry(false);
			let key_len = right_node.keys[0].len();
			let (key_on_page, key_overflow) = calculate_overflow(key_len, max_per_entry);

			KEY_SIZE_PREFIX
				+ key_on_page
				+ CHILD_PTR_SIZE
				+ (if key_overflow {
					8
				} else {
					0
				})
		} else {
			return Err(BPlusTreeError::Serialization(
				"Right internal node is unexpectedly empty during redistribution".into(),
			));
		};

		// Only redistribute if it would improve balance
		if Self::should_redistribute_nodes(&left_node, &right_node, 0, first_entry_size) {
			// Get the parent key
			let parent_key = parent.keys[left_idx].clone();

			// Move first key from right node to parent
			let new_parent_key = left_node.take_from_right(&mut right_node, parent_key);

			// Update parent key
			parent.keys[left_idx] = new_parent_key;

			// Write updated nodes
			self.write_node(&NodeType::Internal(left_node))?;
			self.write_node(&NodeType::Internal(right_node))?;
		}

		// Parent will be written by the calling function
		Ok(())
	}

	fn redistribute_leaf_from_left(
		&mut self,
		parent: &mut InternalNode,
		left_idx: usize,
		right_idx: usize,
	) -> Result<()> {
		let left_offset = parent.children[left_idx];
		let right_offset = parent.children[right_idx];

		let mut left_node = match self.read_node(left_offset)? {
			NodeType::Leaf(node) => node,
			_ => return Err(BPlusTreeError::InvalidNodeType),
		};

		let mut right_node = match self.read_node(right_offset)? {
			NodeType::Leaf(node) => node,
			_ => return Err(BPlusTreeError::InvalidNodeType),
		};

		// Get size of last entry in left node (accounting for overflow)
		let last_idx = left_node.keys.len() - 1;
		let last_entry_size =
			if last_idx < left_node.keys.len() {
				let max_per_entry = calculate_max_bytes_per_entry(true);
				let key_len = left_node.keys[last_idx].len();
				let value_len = left_node.values[last_idx].len();

				let (key_on_page, key_overflow) = calculate_overflow(key_len, max_per_entry);
				let (val_on_page, val_overflow) = calculate_overflow(value_len, max_per_entry);

				KEY_SIZE_PREFIX
					+ key_on_page + VALUE_SIZE_PREFIX
					+ val_on_page + (if key_overflow {
					8
				} else {
					0
				}) + (if val_overflow {
					8
				} else {
					0
				})
			} else {
				return Err(BPlusTreeError::Serialization(
					"Left node is unexpectedly empty during redistribution".into(),
				));
			};

		// Only redistribute if it would improve balance
		if Self::should_redistribute_nodes(&left_node, &right_node, last_idx, last_entry_size) {
			// Move last key-value pair from left to right
			let new_separator = left_node.redistribute_to_right(&mut right_node);

			// Update parent key
			parent.keys[left_idx] = new_separator;

			// Write updated nodes
			self.write_node(&NodeType::Leaf(left_node))?;
			self.write_node(&NodeType::Leaf(right_node))?;
		}

		// Parent will be written by the calling function
		Ok(())
	}

	fn redistribute_leaf_from_right(
		&mut self,
		parent: &mut InternalNode,
		left_idx: usize,
		right_idx: usize,
	) -> Result<()> {
		let left_offset = parent.children[left_idx];
		let right_offset = parent.children[right_idx];

		let mut left_node = match self.read_node(left_offset)? {
			NodeType::Leaf(node) => node,
			_ => return Err(BPlusTreeError::InvalidNodeType),
		};

		let mut right_node = match self.read_node(right_offset)? {
			NodeType::Leaf(node) => node,
			_ => return Err(BPlusTreeError::InvalidNodeType),
		};

		// Get size of first entry in right node (accounting for overflow)
		let first_entry_size =
			if !right_node.keys.is_empty() {
				let max_per_entry = calculate_max_bytes_per_entry(true);
				let key_len = right_node.keys[0].len();
				let value_len = right_node.values[0].len();

				let (key_on_page, key_overflow) = calculate_overflow(key_len, max_per_entry);
				let (val_on_page, val_overflow) = calculate_overflow(value_len, max_per_entry);

				KEY_SIZE_PREFIX
					+ key_on_page + VALUE_SIZE_PREFIX
					+ val_on_page + (if key_overflow {
					8
				} else {
					0
				}) + (if val_overflow {
					8
				} else {
					0
				})
			} else {
				return Err(BPlusTreeError::Serialization(
					"Right node is unexpectedly empty during redistribution".into(),
				));
			};

		// Only redistribute if it would improve balance
		if Self::should_redistribute_nodes(&left_node, &right_node, 0, first_entry_size) {
			// Move first key-value pair from right to left
			let new_separator = left_node.take_from_right(&mut right_node);

			// Update parent key
			parent.keys[left_idx] = new_separator;

			// Write updated nodes
			self.write_node(&NodeType::Leaf(left_node))?;
			self.write_node(&NodeType::Leaf(right_node))?;
		}

		// Parent will be written by the calling function
		Ok(())
	}

	fn should_redistribute_nodes<T: Node>(
		left_node: &T,
		right_node: &T,
		entry_idx: usize,
		entry_size: usize,
	) -> bool {
		let left_size = left_node.current_size();
		let right_size = right_node.current_size();
		let total_size = left_size + right_size;
		let target_size = total_size / 2;

		// Calculate balance metrics before redistribution
		let before_left_diff = ((left_size as i64) - (target_size as i64)).abs();
		let before_right_diff = ((right_size as i64) - (target_size as i64)).abs();
		let before_total_diff = before_left_diff + before_right_diff;

		// Calculate balance metrics after potential redistribution
		// Guard against underflow
		let after_left_size = if entry_idx == 0 {
			left_size + entry_size // Adding from right
		} else {
			// Taking from left - check for underflow
			if entry_size > left_size {
				// Can't redistribute if entry is larger than entire left node
				return false;
			}
			left_size - entry_size
		};

		let after_right_size = if entry_idx == 0 {
			// Taking from right - check for underflow
			if entry_size > right_size {
				// Can't redistribute if entry is larger than entire right node
				return false;
			}
			right_size - entry_size
		} else {
			right_size + entry_size // Adding from left
		};

		let after_left_diff = ((after_left_size as i64) - (target_size as i64)).abs();
		let after_right_diff = ((after_right_size as i64) - (target_size as i64)).abs();
		let after_total_diff = after_left_diff + after_right_diff;

		// Only redistribute if it would improve balance
		after_total_diff < before_total_diff
	}

	fn merge_internal_nodes(
		&mut self,
		parent: &mut InternalNode,
		left_idx: usize,
		right_idx: usize,
	) -> Result<()> {
		let left_offset = parent.children[left_idx];
		let right_offset = parent.children[right_idx];

		let mut left_node = match self.read_node(left_offset)? {
			NodeType::Internal(node) => node,
			_ => return Err(BPlusTreeError::InvalidNodeType),
		};

		let right_node = match self.read_node(right_offset)? {
			NodeType::Internal(node) => node,
			_ => return Err(BPlusTreeError::InvalidNodeType),
		};

		// Check if nodes can be merged
		if !left_node.can_merge_with(&right_node) {
			// If they can't be merged, we keep the nodes in their current state
			// This is safe but may lead to lower space utilization
			return Ok(());
		}

		// Take the separator key from parent - this becomes the middle key
		// between the left node's last key and the right node's first key
		let separator = parent.keys.remove(left_idx);

		// Move separator key from parent into left node as the boundary between subtrees
		left_node.merge_from_right(&right_node, separator);

		// Remove the right child pointer from parent (the separator was already removed)
		parent.children.remove(right_idx);

		// Write updated left node
		self.write_node(&NodeType::Internal(left_node))?;

		// Free the right node's page with overflow chains
		self.free_node_with_overflow(right_offset)?;

		// Parent node will be written by the calling function
		Ok(())
	}

	fn merge_leaf_nodes(
		&mut self,
		parent: &mut InternalNode,
		left_idx: usize,
		right_idx: usize,
	) -> Result<()> {
		let left_offset = parent.children[left_idx];
		let right_offset = parent.children[right_idx];

		let (mut left_node, right_node) =
			match (self.read_node(left_offset)?, self.read_node(right_offset)?) {
				(NodeType::Leaf(left), NodeType::Leaf(right)) => (left, right),
				_ => return Err(BPlusTreeError::InvalidNodeType),
			};

		// Check if nodes can be merged
		if !left_node.can_merge_with(&right_node) {
			// If they can't be merged, we keep the nodes in their current state
			return Ok(());
		}

		// Update parent in-memory
		parent.keys.remove(left_idx);
		parent.children.remove(right_idx);

		// Get next leaf pointer before merging
		let next_leaf = right_node.next_leaf;

		left_node.merge_from_right(&right_node);

		// Update next leaf's prev pointer if needed
		if next_leaf != 0 {
			if let NodeType::Leaf(mut next_leaf_node) = self.read_node(next_leaf)? {
				next_leaf_node.prev_leaf = left_offset;
				self.write_node(&NodeType::Leaf(next_leaf_node))?;
			}
		}

		// Write left node
		self.write_node(&NodeType::Leaf(left_node))?;

		// Free the right node's page with overflow chains
		self.free_node_with_overflow(right_offset)?;

		Ok(())
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

	// Free page management methods with free_page_count tracking
	/// Free a node and all its associated overflow chains
	fn free_node_with_overflow(&mut self, offset: u64) -> Result<()> {
		// Read the node first to get overflow information
		let node = self.read_node(offset)?;

		// Free overflow chains based on node type
		match node {
			NodeType::Internal(internal) => {
				// Free all key overflow chains
				for &overflow_offset in &internal.key_overflows {
					if overflow_offset != 0 {
						self.free_overflow_chain(overflow_offset)?;
					}
				}
			}
			NodeType::Leaf(leaf) => {
				// Free all key overflow chains
				for &overflow_offset in &leaf.key_overflows {
					if overflow_offset != 0 {
						self.free_overflow_chain(overflow_offset)?;
					}
				}
				// Free all value overflow chains
				for &overflow_offset in &leaf.value_overflows {
					if overflow_offset != 0 {
						self.free_overflow_chain(overflow_offset)?;
					}
				}
			}
			NodeType::Overflow(_) => {
				// Overflow pages don't have sub-overflows
			}
		}

		// Now free the node itself
		self.free_page(offset)
	}

	fn free_page(&mut self, offset: u64) -> Result<()> {
		// Validate offset
		if offset < PAGE_SIZE as u64 || offset >= self.header.total_pages * PAGE_SIZE as u64 {
			return Err(BPlusTreeError::InvalidOffset);
		}

		// Remove from cache
		self.cache.remove(&offset);

		// Add to trunk page system
		if self.header.trunk_page_head == 0 {
			// No trunk pages yet, so create one by turning this freed page into a trunk page
			let trunk = TrunkPage::new(offset);

			// Update header
			self.header.trunk_page_head = offset;
			// Don't increment free_page_count - this page is now a trunk, not a free page
			self.write_header()?;

			// Write the empty trunk page
			self.write_trunk_page(&trunk)?;
		} else {
			// Find a trunk with space
			let mut current_trunk_offset = self.header.trunk_page_head;

			loop {
				let mut trunk = self.read_trunk_page(current_trunk_offset)?;

				if !trunk.is_full() {
					// Found space, add the page
					trunk.add_free_page(offset);
					self.write_trunk_page(&trunk)?;

					// Update free page count
					self.header.free_page_count += 1;
					self.write_header()?;
					break;
				}

				if trunk.next_trunk == 0 {
					// No next trunk, create new one by turning this freed page into a trunk
					let new_trunk = TrunkPage::new(offset);

					// Link to current trunk
					trunk.next_trunk = offset;

					// Write the updated trunk
					self.write_trunk_page(&trunk)?;
					self.write_trunk_page(&new_trunk)?;

					// No need to update free_page_count - page is used as trunk, not a free page
					break;
				}

				// Move to next trunk
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

	fn read_node(&self, offset: u64) -> Result<NodeType> {
		// Check if the node is in the cache first
		if let Some(node) = self.cache.get(&offset) {
			return Ok(node.clone());
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
					// Use overflow-aware deserialization for internal nodes
					let internal =
						InternalNode::deserialize(&buffer, offset, &|overflow_offset| {
							self.read_overflow_chain(overflow_offset)
						})?;
					NodeType::Internal(internal)
				}
				NODE_TYPE_LEAF => {
					// Use overflow-aware deserialization for leaf nodes
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

		self.cache.insert(offset, node.clone());
		Ok(node)
	}

	/// Prepare internal node for writing by creating overflow pages for large keys
	fn prepare_internal_node_overflow(&mut self, node: &mut InternalNode) -> Result<()> {
		// Calculate max bytes per entry
		let max_per_entry = calculate_max_bytes_per_entry(false);

		// Ensure key_overflows vec is properly sized
		while node.key_overflows.len() < node.keys.len() {
			node.key_overflows.push(0);
		}

		// Check each key to see if it needs overflow
		for (i, key) in node.keys.iter().enumerate() {
			let (bytes_on_page, needs_overflow) = calculate_overflow(key.len(), max_per_entry);

			if needs_overflow {
				// Key needs overflow
				let overflow_data = &key[bytes_on_page..];
				let existing_overflow = node.key_overflows[i];

				// Only create overflow if it doesn't exist
				if existing_overflow == 0 {
					// Create new overflow chain
					let overflow_offset = self.write_overflow_chain(overflow_data)?;
					node.key_overflows[i] = overflow_offset;
				}
			} else if node.key_overflows[i] != 0 {
				// Key no longer needs overflow, free old chain
				self.free_overflow_chain(node.key_overflows[i])?;
				node.key_overflows[i] = 0;
			}
		}

		Ok(())
	}

	/// Prepare leaf node for writing by creating overflow pages for large keys/values
	fn prepare_leaf_node_overflow(&mut self, node: &mut LeafNode) -> Result<()> {
		// Calculate max bytes per entry
		let max_per_entry = calculate_max_bytes_per_entry(true);

		// Ensure overflow vecs are properly sized
		while node.key_overflows.len() < node.keys.len() {
			node.key_overflows.push(0);
		}
		while node.value_overflows.len() < node.values.len() {
			node.value_overflows.push(0);
		}

		// Check each key to see if it needs overflow
		for (i, key) in node.keys.iter().enumerate() {
			let (bytes_on_page, needs_overflow) = calculate_overflow(key.len(), max_per_entry);

			if needs_overflow {
				// Key needs overflow
				let overflow_data = &key[bytes_on_page..];
				let existing_overflow = node.key_overflows[i];

				// Only create overflow if it doesn't exist
				if existing_overflow == 0 {
					let overflow_offset = self.write_overflow_chain(overflow_data)?;
					node.key_overflows[i] = overflow_offset;
				}
			} else if node.key_overflows[i] != 0 {
				// Key no longer needs overflow, free old chain
				self.free_overflow_chain(node.key_overflows[i])?;
				node.key_overflows[i] = 0;
			}
		}

		// Check each value to see if it needs overflow
		for (i, value) in node.values.iter().enumerate() {
			let (bytes_on_page, needs_overflow) = calculate_overflow(value.len(), max_per_entry);

			if needs_overflow {
				// Value needs overflow
				let overflow_data = &value[bytes_on_page..];
				let existing_overflow = node.value_overflows[i];

				// Only create overflow if it doesn't exist
				if existing_overflow == 0 {
					let overflow_offset = self.write_overflow_chain(overflow_data)?;
					node.value_overflows[i] = overflow_offset;
				}
			} else if node.value_overflows[i] != 0 {
				// Value no longer needs overflow, free old chain
				self.free_overflow_chain(node.value_overflows[i])?;
				node.value_overflows[i] = 0;
			}
		}

		Ok(())
	}

	fn write_node(&mut self, node: &NodeType) -> Result<()> {
		// Prepare node with overflow pages if needed
		let node = match node {
			NodeType::Internal(internal) => {
				let mut internal = internal.clone();
				self.prepare_internal_node_overflow(&mut internal)?;
				NodeType::Internal(internal)
			}
			NodeType::Leaf(leaf) => {
				let mut leaf = leaf.clone();
				self.prepare_leaf_node_overflow(&mut leaf)?;
				NodeType::Leaf(leaf)
			}
			other => other.clone(),
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

	/// Write data to overflow pages, creating a chain as needed
	/// Returns the offset of the first overflow page
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
				if let NodeType::Overflow(mut prev_overflow) = prev_node {
					prev_overflow.next_overflow = page_offset;
					self.write_node(&NodeType::Overflow(prev_overflow))?;
				}
			}

			prev_page_offset = page_offset;
		}

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
			match node {
				NodeType::Overflow(overflow) => {
					result.extend_from_slice(&overflow.data);
					current_offset = overflow.next_overflow;
				}
				_ => {
					return Err(BPlusTreeError::Corruption(format!(
						"Expected overflow page at offset {}, found different node type",
						current_offset
					)));
				}
			}
		}

		Ok(result)
	}

	/// Free all pages in an overflow chain
	fn free_overflow_chain(&mut self, first_page: u64) -> Result<()> {
		if first_page == 0 {
			return Ok(());
		}

		let mut current_offset = first_page;

		while current_offset != 0 {
			let node = self.read_node(current_offset)?;
			match node {
				NodeType::Overflow(overflow) => {
					let next_offset = overflow.next_overflow;
					self.free_page(current_offset)?;
					current_offset = next_offset;
				}
				_ => {
					return Err(BPlusTreeError::Corruption(format!(
						"Expected overflow page at offset {}, found different node type",
						current_offset
					)));
				}
			}
		}

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
		match self.read_node(node_offset)? {
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
			NodeType::Overflow(_) => Err(BPlusTreeError::Corruption(format!(
				"Unexpected overflow page in tree at offset {}",
				node_offset
			))),
		}
	}

	// Print basic tree statistics
	#[cfg(test)]
	fn print_tree_stats(&mut self) -> Result<()> {
		let (height, node_count, total_keys, leaf_nodes) = self.calculate_tree_stats()?;

		println!("B+ Tree Statistics:");
		println!("-------------------");
		println!("Tree Height: {}", height);
		println!("Total Nodes: {}", node_count);
		println!("Total Keys: {}", total_keys);
		println!("Leaf Nodes: {}", leaf_nodes);
		println!("Internal Nodes: {}", node_count - leaf_nodes);
		println!("-------------------");

		Ok(())
	}
}

pub struct RangeScanIterator<'a, F: VfsFile> {
	tree: &'a BPlusTree<F>,
	current_leaf: Option<LeafNode>,
	end_key: Vec<u8>,
	current_idx: usize,
	reached_end: bool,
}

impl<'a, F: VfsFile> RangeScanIterator<'a, F> {
	pub(crate) fn new(tree: &'a BPlusTree<F>, start_key: &[u8], end_key: &[u8]) -> Result<Self> {
		// Find the leaf containing the start key
		let mut node_offset = tree.header.root_offset;

		// Traverse the tree to find the starting leaf
		let leaf = loop {
			match tree.read_node(node_offset)? {
				NodeType::Internal(internal) => {
					// Find the child that would contain the key
					let mut idx = 0;
					while idx < internal.keys.len()
						&& tree.compare.compare(start_key, &internal.keys[idx]) >= Ordering::Equal
					{
						idx += 1;
					}
					node_offset = internal.children[idx];
				}
				NodeType::Leaf(leaf) => {
					break leaf;
				}
				NodeType::Overflow(_) => {
					return Err(BPlusTreeError::Corruption(format!(
						"Unexpected overflow page in tree traversal at offset {}",
						node_offset
					)));
				}
			}
		};

		// Find the first key >= start_key in the leaf
		let current_idx =
			leaf.keys.partition_point(|k| tree.compare.compare(k, start_key) == Ordering::Less);

		Ok(RangeScanIterator {
			tree,
			current_leaf: Some(leaf),
			end_key: end_key.to_vec(),
			current_idx,
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
				// Check if we've reached the end of the current leaf
				if self.current_idx >= leaf.keys.len() {
					// Move to the next leaf if possible
					if leaf.next_leaf == 0 {
						self.reached_end = true;
						return None;
					}

					// Load the next leaf
					match self.tree.read_node(leaf.next_leaf) {
						Ok(NodeType::Leaf(next_leaf)) => {
							self.current_leaf = Some(next_leaf);
							self.current_idx = 0;
							// Continue the loop
							continue;
						}
						Ok(_) => return Some(Err(BPlusTreeError::InvalidNodeType)),
						Err(e) => return Some(Err(e)),
					}
				}

				// Check if the current key is within range
				let key = &leaf.keys[self.current_idx];
				if self.tree.compare.compare(key, &self.end_key) == Ordering::Greater {
					self.reached_end = true;
					return None;
				}

				// Return the current key-value pair and advance
				let result = Ok((
					Arc::from(key.as_ref()),
					Arc::from(leaf.values[self.current_idx].as_ref()),
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
	use test_log::test;

	use super::*;
	use rand::{rngs::StdRng, Rng, SeedableRng};
	use tempfile::NamedTempFile;

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
			let a_num = u32::from_le_bytes(a.try_into().unwrap());
			let b_num = u32::from_le_bytes(b.try_into().unwrap());
			a_num.cmp(&b_num)
		}

		fn separator(&self, from: &[u8], to: &[u8]) -> Vec<u8> {
			let from_num = u32::from_le_bytes(from.try_into().unwrap());
			let to_num = u32::from_le_bytes(to.try_into().unwrap());
			if from_num < to_num {
				((from_num + to_num) / 2).to_le_bytes().to_vec()
			} else {
				from.to_vec()
			}
		}

		fn successor(&self, key: &[u8]) -> Vec<u8> {
			let key_num = u32::from_le_bytes(key.try_into().unwrap());
			(key_num + 1).to_le_bytes().to_vec()
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
			tree.insert(&key, &value).unwrap();
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

		// Insert and immediately verify each key-value pair
		for i in 0..TEST_SIZE {
			// println!("Inserting key {}", i);
			tree.insert(&keys[i], &values[i]).unwrap();

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

		// Final verification of all keys
		println!("\nFinal verification:");
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
			tree.insert(key, value).unwrap();
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
			tree.insert(key, value).unwrap();
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
		assert_eq!(tree.get(b"key1").unwrap(), Some(b"value1".to_vec()));
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
		assert_eq!(tree.get(b"key1").unwrap(), Some(b"value1".to_vec()));
		assert_eq!(tree.get(b"key2").unwrap(), Some(b"updated_value2".to_vec()));
		assert_eq!(tree.get(b"key3").unwrap(), Some(b"value3".to_vec()));
	}

	#[test]
	fn test_multiple_updates_same_key() {
		let mut tree = create_test_tree(true);

		// Insert initial key
		tree.insert(b"key", b"value1").unwrap();
		assert_eq!(tree.get(b"key").unwrap(), Some(b"value1".to_vec()));

		// Update multiple times
		tree.insert(b"key", b"value2").unwrap();
		assert_eq!(tree.get(b"key").unwrap(), Some(b"value2".to_vec()));

		tree.insert(b"key", b"value3").unwrap();
		assert_eq!(tree.get(b"key").unwrap(), Some(b"value3".to_vec()));

		tree.insert(b"key", b"final_value").unwrap();
		assert_eq!(tree.get(b"key").unwrap(), Some(b"final_value".to_vec()));
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
		assert_eq!(tree.get(b"key1").unwrap(), Some(b"updated_value1".to_vec()));
		assert_eq!(tree.get(b"key2").unwrap(), Some(b"updated_value2".to_vec()));
		assert_eq!(tree.get(b"key3").unwrap(), Some(b"updated_value3".to_vec()));
	}

	#[test]
	fn test_sequential_inserts_and_deletes() {
		let max_keys = 100;
		let mut tree = create_test_tree(true);
		// Insert enough to cause multiple splits
		for i in 0..(max_keys * 3) {
			let key = format!("key{}", i).into_bytes();
			let value = format!("value{}", i).into_bytes();
			tree.insert(&key, &value).unwrap();
		}

		// Verify all exist
		for i in 0..(max_keys * 3) {
			let key = format!("key{}", i).into_bytes();
			assert!(tree.get(&key).unwrap().is_some());
		}

		// Delete all in reverse order
		for i in (0..(max_keys * 3)).rev() {
			let key = format!("key{}", i).into_bytes();
			// println!("Deleting key: {:?}", String::from_utf8_lossy(&key));
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
			tree.insert(key, b"value").unwrap();
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
			tree.insert(key, b"value").unwrap();
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
				tree.insert(key, value)?;
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
			tree.insert(b"mango", b"orange")?;
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
			tree.insert(b"one", b"1")?;
			tree.insert(b"two", b"2")?;
			tree.insert(b"three", b"3")?;
		}

		// Delete and verify
		{
			let mut tree = BPlusTree::disk(path, Arc::new(TestComparator))?;
			assert_eq!(tree.delete(b"two")?, Some(b"2".to_vec()));
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
		tree.insert(b"key", b"v1").unwrap();
		tree.delete(b"key").unwrap();
		tree.insert(b"key", b"v2").unwrap();
		assert_eq!(tree.get(b"key").unwrap(), Some(b"v2".to_vec()));
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
			assert_eq!(tree.get(b"test").unwrap(), Some(b"value".to_vec()));
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
				let key = format!("key_{}", i).into_bytes();
				let value = format!("value_{}", i).into_bytes();
				(key, value)
			})
			.collect();

		// Insert data and close
		{
			let mut tree = BPlusTree::disk(path, Arc::new(TestComparator)).unwrap();
			for (key, value) in &test_data {
				tree.insert(key, value).unwrap();
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
		n.to_le_bytes().to_vec()
	}

	fn deserialize_pair(pair: (Key, Value)) -> (u32, u32) {
		(
			u32::from_le_bytes((&*pair.0).try_into().unwrap()),
			u32::from_le_bytes((&*pair.1).try_into().unwrap()),
		)
	}

	#[test]
	fn test_range_basic() {
		let file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(file.path(), Arc::new(U32Comparator)).unwrap();

		// Insert test data
		for i in 1..=10 {
			tree.insert(&serialize_u32(i), &serialize_u32(i * 10)).unwrap();
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
			tree.insert(&serialize_u32(i), &serialize_u32(i * 10)).unwrap();
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
			tree.insert(&serialize_u32(i), &serialize_u32(i * 10)).unwrap();
		}

		let results: Vec<_> = tree.range(&serialize_u32(1), &serialize_u32(10)).unwrap().collect();

		assert_eq!(results.len(), 10);
	}

	#[test]
	fn test_invalid_range() {
		let file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(file.path(), Arc::new(U32Comparator)).unwrap();

		for i in 1..=5 {
			tree.insert(&serialize_u32(i), &[]).unwrap();
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
			tree.insert(&serialize_u32(i), &[]).unwrap();
		}

		// Range covering missing start/end
		let results = tree.range(&serialize_u32(2), &serialize_u32(9)).unwrap();
		let expected = vec![3, 5, 7, 9];
		assert_eq!(
			results
				.into_iter()
				.map(|res| res.map(|(k, _)| u32::from_le_bytes((&*k).try_into().unwrap())).unwrap())
				.collect::<Vec<_>>(),
			expected
		);
	}

	#[test]
	fn test_exact_match_range() {
		let file = NamedTempFile::new().unwrap();
		let mut tree = BPlusTree::disk(file.path(), Arc::new(U32Comparator)).unwrap();

		tree.insert(&serialize_u32(5), &[]).unwrap();

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
			tree.insert(&serialize_u32(i), &[]).unwrap();
		}

		// Delete some keys
		tree.delete(&serialize_u32(3)).unwrap();
		tree.delete(&serialize_u32(7)).unwrap();

		// Add new keys
		tree.insert(&serialize_u32(12), &[]).unwrap();
		tree.insert(&serialize_u32(15), &[]).unwrap();

		// Test range scan
		let results = tree.range(&serialize_u32(5), &serialize_u32(15)).unwrap();
		let expected = vec![5, 6, 8, 9, 10, 12, 15];
		assert_eq!(
			results
				.into_iter()
				.map(|res| res.map(|(k, _)| u32::from_le_bytes((&*k).try_into().unwrap())).unwrap())
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

		let mut iter = tree.range(b"key2", b"key3").unwrap();
		assert_eq!(
			iter.next().unwrap().unwrap(),
			(Arc::from(b"key2" as &[u8]), Arc::from(b"value2" as &[u8]))
		);
		assert_eq!(
			iter.next().unwrap().unwrap(),
			(Arc::from(b"key3" as &[u8]), Arc::from(b"value3" as &[u8]))
		);
		assert!(iter.next().is_none());
	}

	#[test]
	fn test_range_empty() {
		let mut tree = create_test_tree(true);
		tree.insert(b"a", b"1").unwrap();
		tree.insert(b"c", b"3").unwrap();
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
			tree.insert(&key, &value).unwrap();
		}

		// Range scan from key_05009 to key_05500
		let start = b"key_05000";
		let end = b"key_05500";
		let mut iter = tree.range(start, end).unwrap();
		for i in 5000..=5500 {
			let expected_key = format!("key_{:05}", i).into_bytes();
			let expected_value = format!("value_{:05}", i).into_bytes();
			assert_eq!(
				iter.next().unwrap().unwrap(),
				(Arc::from(expected_key.as_slice()), Arc::from(expected_value.as_slice()))
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
				tree.insert(&key(i), &value(i)).unwrap();
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
				tree.insert(&key(i), &value(i)).unwrap();
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
				tree.insert(&key(i), &value(i)).unwrap();
			}

			// Verify some free pages were reused
			assert!(
				tree.header.free_page_count < initial_free_count,
				"Some free pages should have been reused"
			);

			// Step 5: Verify total page count hasn't increased as much as it would without reuse
			// The increase in total pages should be less than the number of new inserts
			// because we're reusing free pages
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
				tree.insert(&unique_key, &fixed_value).unwrap();

				// Verify the key was inserted correctly
				let result = tree.get(&unique_key).unwrap();
				assert!(
					result.is_some(),
					"Failed to retrieve just-inserted key of size {}",
					key_size
				);
				assert_eq!(result.unwrap(), fixed_value);
			}

			println!("Testing deletion for key size {}", key_size);

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

		// Phase 1: Insert many keys of varying sizes
		let num_initial = 100;
		println!("Phase 1: Inserting {} initial keys", num_initial);

		for i in 0..num_initial {
			// Create key with sequence number and random size
			let key_size = rng.random_range(10..400);
			let mut key = format!("key_{:05}_", i).into_bytes();
			key.extend(vec![b'k'; key_size - key.len()]);

			let value_size = rng.random_range(10..200);
			let value = vec![b'v'; value_size];

			// Insert and track
			tree.insert(&key, &value).unwrap();
			all_keys.push((key.clone(), value));
			active_keys.insert(key);

			// Periodically flush
			if i % 20 == 19 {
				tree.flush().unwrap();
			}
		}

		// Phase 2: Delete random keys
		let num_deletions = 40;
		println!("Phase 2: Deleting {} random keys", num_deletions);

		let mut keys_to_delete = active_keys.iter().cloned().collect::<Vec<_>>();
		for _ in 0..num_deletions {
			if keys_to_delete.is_empty() {
				break;
			}

			let idx = rng.random_range(0..keys_to_delete.len());
			let key = keys_to_delete.swap_remove(idx);

			// println!("Deleting key of size {}", key.len());
			tree.delete(&key).unwrap();
			active_keys.remove(&key);

			// Periodic flush
			if rng.random_bool(0.2) {
				tree.flush().unwrap();
			}
		}

		// Phase 3: Insert more keys, some very large
		let num_additional = 30;
		println!("Phase 3: Inserting {} additional keys, some very large", num_additional);

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
			tree.insert(&key, &value).unwrap();
			all_keys.push((key.clone(), value));
			active_keys.insert(key);

			// Periodic flush
			if i % 10 == 9 {
				tree.flush().unwrap();
			}
		}

		// Final flush
		tree.flush().unwrap();

		// Verification phase
		println!("Verification: Checking all remaining keys can be found");

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

		// Verify the error is a corruption error
		match result {
			Err(BPlusTreeError::Corruption(_)) => (),
			_ => panic!("Expected corruption error, got {:?}", result),
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
}
