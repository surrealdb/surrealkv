use std::cmp::Ordering;
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
use std::sync::Arc;

use rand::Rng;

use crate::comparator::{Comparator, InternalKeyComparator};
use crate::memtable::arena::{Arena, NODE_ALIGNMENT};
use crate::sstable::InternalKeyKind;

// ============================================================================
// Constants (matching Pebble)
// ============================================================================

const MAX_HEIGHT: usize = 20;
const P_VALUE: f64 = 1.0 / std::f64::consts::E; // ~0.368

/// Size of the Links struct
const LINKS_SIZE: usize = std::mem::size_of::<Links>();

/// Maximum node size (with full tower)
const MAX_NODE_SIZE: usize = std::mem::size_of::<Node>() + (MAX_HEIGHT - 1) * LINKS_SIZE;

/// Precomputed probabilities for random height generation
fn probabilities() -> &'static [u32; MAX_HEIGHT] {
	static PROBABILITIES: std::sync::OnceLock<[u32; MAX_HEIGHT]> = std::sync::OnceLock::new();
	PROBABILITIES.get_or_init(|| {
		let mut p = [0u32; MAX_HEIGHT];
		let mut prob = 1.0f64;
		for i in 0..MAX_HEIGHT {
			p[i] = (u32::MAX as f64 * prob) as u32;
			prob *= P_VALUE;
		}
		p
	})
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Compute maximum space needed for a node with given key and value sizes.
pub fn max_node_size(key_size: u32, value_size: u32) -> u64 {
	const MAX_PADDING: u64 = NODE_ALIGNMENT as u64 - 1;
	MAX_NODE_SIZE as u64 + key_size as u64 + value_size as u64 + MAX_PADDING
}

/// Encode an entry for the memtable.
/// Format: [4 bytes internal_key_len][user_key][8 bytes trailer][8 bytes timestamp][4 bytes
/// value_len][value]
pub fn encoded_entry_size(user_key_len: usize, value_len: usize) -> usize {
	let internal_key_size = user_key_len + 16; // user_key + trailer(8) + timestamp(8)
	4 + internal_key_size + 4 + value_len
}

pub fn encode_entry(
	buf: &mut [u8],
	user_key: &[u8],
	seq_num: u64,
	kind: InternalKeyKind,
	timestamp: u64,
	value: &[u8],
) -> usize {
	let internal_key_size = user_key.len() + 16;
	let mut offset = 0;

	buf[offset..offset + 4].copy_from_slice(&(internal_key_size as u32).to_be_bytes());
	offset += 4;

	buf[offset..offset + user_key.len()].copy_from_slice(user_key);
	offset += user_key.len();

	let trailer = (seq_num << 8) | (kind as u64);
	buf[offset..offset + 8].copy_from_slice(&trailer.to_be_bytes());
	offset += 8;

	buf[offset..offset + 8].copy_from_slice(&timestamp.to_be_bytes());
	offset += 8;

	buf[offset..offset + 4].copy_from_slice(&(value.len() as u32).to_be_bytes());
	offset += 4;

	buf[offset..offset + value.len()].copy_from_slice(value);
	offset += value.len();

	offset
}

#[inline]
unsafe fn read_length_prefixed_slice(ptr: *const u8) -> &'static [u8] {
	let len = u32::from_be_bytes(*(ptr as *const [u8; 4])) as usize;
	std::slice::from_raw_parts(ptr.add(4), len)
}

// ============================================================================
// Comparator Trait
// ============================================================================

pub trait SkipListComparator: Send + Sync {
	/// Compare two user keys (not internal keys, not length-prefixed)
	fn compare_user_key(&self, a: &[u8], b: &[u8]) -> Ordering;
}

pub struct MemTableKeyComparator {
	user_cmp: Arc<dyn Comparator>,
}

impl MemTableKeyComparator {
	pub fn new(_internal_cmp: Arc<InternalKeyComparator>) -> Self {
		// Extract user comparator - InternalKeyComparator wraps a user comparator
		// For now, we use BytewiseComparator directly since that's what memtable uses
		Self {
			user_cmp: Arc::new(crate::BytewiseComparator {}),
		}
	}
}

impl SkipListComparator for MemTableKeyComparator {
	fn compare_user_key(&self, a: &[u8], b: &[u8]) -> Ordering {
		self.user_cmp.compare(a, b)
	}
}

// ============================================================================
// Links Structure (doubly-linked tower level)
// ============================================================================

#[repr(C)]
struct Links {
	next_offset: AtomicU32,
	prev_offset: AtomicU32,
}

impl Links {
	#[inline]
	fn init(&self, prev_offset: u32, next_offset: u32) {
		self.next_offset.store(next_offset, AtomicOrdering::Relaxed);
		self.prev_offset.store(prev_offset, AtomicOrdering::Relaxed);
	}
}

// ============================================================================
// Node Structure
// ============================================================================

/// Node with offset-based key/value storage and variable-height tower.
///
/// Memory layout in arena:
/// [unused tower links for heights > node_height][Node struct][key bytes][value bytes]
#[repr(C)]
struct Node {
	/// Offset into arena where key bytes start
	key_offset: u32,
	/// Length of user key in bytes (actually full entry size for allocate_key compatibility)
	key_size: u32,
	/// InternalKeyTrailer: (seq_num << 8) | kind
	/// During allocation, upper 32 bits stash the height temporarily
	key_trailer: u64,
	/// Length of value in bytes
	value_size: u32,
	/// Padding for 8-byte alignment of tower
	_padding: u32,
	/// Variable-size tower (only first element in struct, rest via pointer math)
	/// tower[0] is level 0, tower[height-1] is highest level
	tower: [Links; 1],
}

impl Node {
	/// Get key bytes from arena (returns the full entry starting at key_offset)
	#[inline]
	fn get_key_bytes<'a>(&self, arena: &'a Arena) -> &'a [u8] {
		// key_offset points to the length-prefixed entry
		let entry_ptr = arena.get_pointer(self.key_offset);
		unsafe { read_length_prefixed_slice(entry_ptr) }
	}

	/// Get value bytes from arena
	#[inline]
	fn get_value<'a>(&self, arena: &'a Arena) -> &'a [u8] {
		let entry_ptr = arena.get_pointer(self.key_offset);
		unsafe {
			let key_len = u32::from_be_bytes(*(entry_ptr as *const [u8; 4])) as usize;
			let value_ptr = entry_ptr.add(4 + key_len);
			read_length_prefixed_slice(value_ptr)
		}
	}

	/// Get next offset at given height
	#[inline]
	fn next_offset(&self, h: usize) -> u32 {
		unsafe {
			let links = (self.tower.as_ptr()).add(h);
			(*links).next_offset.load(AtomicOrdering::Acquire)
		}
	}

	/// Get prev offset at given height
	#[inline]
	fn prev_offset(&self, h: usize) -> u32 {
		unsafe {
			let links = (self.tower.as_ptr()).add(h);
			(*links).prev_offset.load(AtomicOrdering::Acquire)
		}
	}

	/// CAS on next offset at given height
	#[inline]
	fn cas_next_offset(&self, h: usize, old: u32, new: u32) -> bool {
		unsafe {
			let links = (self.tower.as_ptr()).add(h);
			(*links)
				.next_offset
				.compare_exchange(old, new, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
				.is_ok()
		}
	}

	/// CAS on prev offset at given height
	#[inline]
	fn cas_prev_offset(&self, h: usize, old: u32, new: u32) -> bool {
		unsafe {
			let links = (self.tower.as_ptr()).add(h);
			(*links)
				.prev_offset
				.compare_exchange(old, new, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
				.is_ok()
		}
	}

	/// Initialize tower links at given height
	#[inline]
	fn tower_init(&self, h: usize, prev_offset: u32, next_offset: u32) {
		unsafe {
			let links = (self.tower.as_ptr()).add(h);
			(*links).init(prev_offset, next_offset);
		}
	}
}

/// Allocate a raw node in the arena (for head/tail sentinels)
fn new_raw_node(arena: &Arena, height: u32, key_size: u32, value_size: u32) -> Option<u32> {
	// Compute unused tower size
	let unused_size = (MAX_HEIGHT - height as usize) * LINKS_SIZE;
	let node_size = MAX_NODE_SIZE - unused_size;
	let total_size = node_size as u32 + key_size + value_size;

	let node_offset = arena.alloc(total_size, NODE_ALIGNMENT, unused_size as u32)?;

	let node = arena.get_pointer(node_offset) as *mut Node;
	unsafe {
		(*node).key_offset = node_offset + node_size as u32;
		(*node).key_size = key_size;
		(*node).key_trailer = 0;
		(*node).value_size = value_size;
		(*node)._padding = 0;
	}

	Some(node_offset)
}

// ============================================================================
// Splice Structure (for insert position caching)
// ============================================================================

#[derive(Clone, Copy, Default)]
struct Splice {
	prev: u32,
	next: u32,
}

impl Splice {
	#[inline]
	fn init(&mut self, prev: u32, next: u32) {
		self.prev = prev;
		self.next = next;
	}
}

// ============================================================================
// Inserter (caches splice positions for sequential inserts)
// ============================================================================

/// Holds state between consecutive insertions into a skiplist.
///
/// Inserting a key requires finding the "splice" (prev, next) at every level.
/// The Inserter caches the most recently used splice, avoiding recomputation
/// when inserts are sequential.
#[derive(Default)]
pub struct Inserter {
	spl: [Splice; MAX_HEIGHT],
	height: u32,
}

impl Inserter {
	pub fn new() -> Self {
		Self::default()
	}
}

// ============================================================================
// SkipList
// ============================================================================

pub struct SkipList<C: SkipListComparator> {
	arena: Arc<Arena>,
	cmp: C,
	head: u32, // Offset to head sentinel node
	tail: u32, // Offset to tail sentinel node
	height: AtomicU32,
	initial_size: usize, // Arena size after head/tail allocation (to subtract from size())
	#[cfg(test)]
	testing: bool,
}

unsafe impl<C: SkipListComparator> Send for SkipList<C> {}
unsafe impl<C: SkipListComparator> Sync for SkipList<C> {}

impl<C: SkipListComparator> SkipList<C> {
	/// Create a new skiplist.
	///
	/// Note: `max_height` and `branching` parameters are ignored for Pebble compatibility.
	/// We always use MAX_HEIGHT=20 and P_VALUE=1/e.
	pub fn new(cmp: C, arena: Arc<Arena>, _max_height: i32, _branching: u16) -> Self {
		// Allocate head sentinel (full height, no key/value)
		let head =
			new_raw_node(&arena, MAX_HEIGHT as u32, 0, 0).expect("arena too small for head node");

		// Allocate tail sentinel (full height, no key/value)
		let tail =
			new_raw_node(&arena, MAX_HEIGHT as u32, 0, 0).expect("arena too small for tail node");

		// Link all levels: head -> tail, tail <- head
		let head_node = arena.get_pointer(head) as *mut Node;
		let tail_node = arena.get_pointer(tail) as *mut Node;

		for i in 0..MAX_HEIGHT {
			unsafe {
				(*head_node).tower_init(i, 0, tail);
				(*tail_node).tower_init(i, head, 0);
			}
		}

		let initial_size = arena.size();

		Self {
			arena,
			cmp,
			head,
			tail,
			height: AtomicU32::new(1),
			initial_size,
			#[cfg(test)]
			testing: false,
		}
	}

	/// Returns current height of the skiplist.
	pub fn height(&self) -> u32 {
		self.height.load(AtomicOrdering::Acquire)
	}

	/// Returns the arena backing this skiplist.
	pub fn arena(&self) -> &Arc<Arena> {
		&self.arena
	}

	/// Returns the number of bytes allocated from the arena (excluding head/tail sentinels).
	pub fn size(&self) -> usize {
		let current_size = self.arena.size();
		if current_size > self.initial_size {
			current_size - self.initial_size
		} else {
			0
		}
	}

	// ========================================================================
	// Random Height Generation
	// ========================================================================

	fn random_height(&self) -> u32 {
		let mut rng = rand::rng();
		let rnd: u32 = rng.random();
		let mut h = 1u32;
		let probs = probabilities();
		while h < MAX_HEIGHT as u32 && rnd <= probs[h as usize] {
			h += 1;
		}
		h
	}

	// ========================================================================
	// Node Access Helpers
	// ========================================================================

	#[inline]
	fn get_node(&self, offset: u32) -> *const Node {
		self.arena.get_pointer(offset) as *const Node
	}

	#[inline]
	fn get_next(&self, nd_offset: u32, level: usize) -> u32 {
		unsafe { (*self.get_node(nd_offset)).next_offset(level) }
	}

	#[inline]
	fn get_prev(&self, nd_offset: u32, level: usize) -> u32 {
		unsafe { (*self.get_node(nd_offset)).prev_offset(level) }
	}

	// ========================================================================
	// Key Comparison
	// ========================================================================

	/// Compare internal keys: first by user key, then by trailer (descending).
	/// Matches Pebble's comparison semantics exactly.
	/// Returns Ordering of (user_key_a, trailer_a) vs (user_key_b, trailer_b)
	/// Zero allocation - compares user keys directly, then trailers.
	#[inline]
	fn compare_internal_keys(
		&self,
		user_key_a: &[u8],
		trailer_a: u64,
		user_key_b: &[u8],
		trailer_b: u64,
	) -> Ordering {
		// First compare user keys
		match self.cmp.compare_user_key(user_key_a, user_key_b) {
			Ordering::Equal => {
				// User keys equal - compare trailers in DESCENDING order
				// Higher trailer (newer seq num) comes FIRST in sorted order
				trailer_b.cmp(&trailer_a)
			}
			other => other,
		}
	}

	/// Returns true if key comes AFTER nd's key in sort order.
	/// Matches Pebble's keyIsAfterNode exactly.
	fn key_is_after_node(&self, nd_offset: u32, user_key: &[u8], trailer: u64) -> bool {
		if nd_offset == self.head {
			return true; // Key is always after head
		}
		if nd_offset == self.tail {
			return false; // Key is never after tail
		}

		let node = self.get_node(nd_offset);
		let nd_internal_key = unsafe { (*node).get_key_bytes(&self.arena) };

		// Extract user key: internal_key = [user_key][trailer:8][timestamp:8]
		if nd_internal_key.len() < 16 {
			return false;
		}
		let nd_user_key = &nd_internal_key[..nd_internal_key.len() - 16];
		let nd_trailer = unsafe { (*node).key_trailer };

		// Compare user keys first
		match self.cmp.compare_user_key(nd_user_key, user_key) {
			Ordering::Less => true,     // nd < key → key is AFTER
			Ordering::Greater => false, // nd > key → key is NOT after
			Ordering::Equal => {
				// User keys equal - compare trailers
				if trailer == nd_trailer {
					false // Equal keys
				} else {
					// Lower trailer = comes AFTER (descending order)
					trailer < nd_trailer
				}
			}
		}
	}

	// ========================================================================
	// Find Splice (with caching for sequential inserts)
	// ========================================================================

	/// Find splice positions for inserting key.
	/// Uses cached splice from Inserter when possible.
	/// Returns true if exact key match found.
	fn find_splice(&self, key: &[u8], key_trailer: u64, ins: &mut Inserter) -> bool {
		let list_height = self.height();
		let mut level: i32;
		let mut prev = self.head;

		if ins.height < list_height {
			// Our cached height is less than current list height.
			// New levels were added, recompute from scratch.
			ins.height = list_height;
			level = list_height as i32;
		} else {
			// Try to reuse cached splice. Find lowest level where splice is valid.
			level = 0;
			for l in 0..list_height as usize {
				let spl = &ins.spl[l];
				if self.get_next(spl.prev, l) != spl.next {
					// Splice invalidated by concurrent insert, keep searching up
					continue;
				}

				// Splice is structurally valid. Check if key is bracketed.
				if spl.prev != self.head && !self.key_is_after_node(spl.prev, key, key_trailer) {
					// Key lies before splice.prev, search from top
					level = list_height as i32;
				} else if spl.next != self.tail
					&& self.key_is_after_node(spl.next, key, key_trailer)
				{
					// Key lies after splice.next, search from top
					level = list_height as i32;
				} else {
					// Key is bracketed by splice! Start from here.
					prev = spl.prev;
				}
				break;
			}
		}

		// Descend from level-1 down to 0, finding splice at each level
		let mut found = false;
		let mut next = 0u32;

		for l in (0..level as usize).rev() {
			let prev_level_next = next;

			loop {
				// Get next node at this level
				next = self.get_next(prev, l);

				// Optimization: if next is same as previous level, we're done
				if next == prev_level_next {
					break;
				}

				if next == self.tail {
					break;
				}

				// Compare key with next node's key (matches Pebble skl.go:463-481)
				let next_node = self.get_node(next);
				let next_internal_key = unsafe { (*next_node).get_key_bytes(&self.arena) };
				if next_internal_key.len() < 16 {
					break;
				}
				// CORRECT: user key starts at index 0, not 4
				let next_user_key = &next_internal_key[..next_internal_key.len() - 16];
				let next_trailer = unsafe { (*next_node).key_trailer };

				// Compare user keys first (like Pebble line 465)
				let cmp = self.cmp.compare_user_key(key, next_user_key);
				if cmp == Ordering::Less {
					// key.userKey < next.userKey → found position
					break;
				}
				if cmp == Ordering::Equal {
					// User-key equality - compare trailers (like Pebble lines 470-481)
					if key_trailer == next_trailer {
						found = true;
						break;
					}
					if key_trailer > next_trailer {
						// Higher trailer = comes before in sorted order
						break;
					}
				}
				// cmp == Greater → keep moving right
				prev = next;
			}

			ins.spl[l].init(prev, next);
		}

		found
	}

	/// Find splice for a single level, starting from given node.
	fn find_splice_for_level(
		&self,
		key: &[u8],
		key_trailer: u64,
		level: usize,
		mut prev: u32,
	) -> (u32, u32, bool) {
		loop {
			let next = self.get_next(prev, level);

			if next == self.tail {
				return (prev, next, false);
			}

			let next_node = self.get_node(next);
			let next_internal_key = unsafe { (*next_node).get_key_bytes(&self.arena) };
			if next_internal_key.len() < 16 {
				return (prev, next, false);
			}
			// CORRECT: user key starts at index 0, not 4
			let next_user_key = &next_internal_key[..next_internal_key.len() - 16];
			let next_trailer = unsafe { (*next_node).key_trailer };

			// Compare user keys first (matches Pebble skl.go:507-523)
			let cmp = self.cmp.compare_user_key(key, next_user_key);
			if cmp == Ordering::Less {
				return (prev, next, false);
			}
			if cmp == Ordering::Equal {
				// User-key equality - compare trailers
				if key_trailer == next_trailer {
					return (prev, next, true);
				}
				if key_trailer > next_trailer {
					// Higher trailer = comes before
					return (prev, next, false);
				}
			}
			// cmp == Greater → keep moving right
			prev = next;
		}
	}

	// ========================================================================
	// Insert (with Inserter for batch optimization)
	// ========================================================================

	/// Allocate space for an entry and return pointer to write to.
	/// Returns None if arena is full.
	pub fn allocate_key(&self, entry_size: usize) -> Option<*mut u8> {
		let height = self.random_height();

		// Calculate node allocation size
		let unused_size = (MAX_HEIGHT - height as usize) * LINKS_SIZE;
		let node_size = MAX_NODE_SIZE - unused_size;
		let total_size = node_size + entry_size;

		let node_offset =
			self.arena.alloc(total_size as u32, NODE_ALIGNMENT, unused_size as u32)?;
		let node = self.arena.get_pointer(node_offset) as *mut Node;

		unsafe {
			(*node).key_offset = node_offset + node_size as u32;
			(*node).key_size = entry_size as u32; // Will be updated by caller
			(*node).key_trailer = (height as u64) << 32; // Stash height temporarily
			(*node).value_size = 0;
			(*node)._padding = 0;
		}

		Some(self.arena.get_pointer(node_offset + node_size as u32))
	}

	/// Insert a pre-allocated key into the skiplist.
	/// The key pointer must have been returned by allocate_key().
	pub fn insert(&self, key: *const u8) -> bool {
		let mut ins = Inserter::new();
		self.insert_with_inserter(key, &mut ins)
	}

	/// Insert with an Inserter for batch optimization.
	pub fn insert_with_inserter(&self, key: *const u8, ins: &mut Inserter) -> bool {
		unsafe {
			// Get node from key pointer
			let entry_ptr = key;

			// Read internal key from length-prefixed format
			let internal_key = read_length_prefixed_slice(entry_ptr);

			// Extract user key and trailer from internal key
			// Internal key format: [user_key][8 bytes trailer][8 bytes timestamp]
			if internal_key.len() < 16 {
				return false;
			}
			let user_key = &internal_key[..internal_key.len() - 16];
			let trailer = u64::from_be_bytes(
				internal_key[internal_key.len() - 16..internal_key.len() - 8].try_into().unwrap(),
			);

			// Calculate node offset from key pointer
			let key_offset = self.arena.get_pointer_offset(entry_ptr);

			// Node is before the key data - find node_size from MAX_NODE_SIZE and height
			// Height was stashed in key_trailer
			let mut found_node_offset = 0u32;
			let mut found_height = 0u32;

			// Search backwards to find the node
			for h in 1..=MAX_HEIGHT as u32 {
				let unused_size = (MAX_HEIGHT - h as usize) * LINKS_SIZE;
				let node_size = MAX_NODE_SIZE - unused_size;

				if key_offset >= node_size as u32 {
					let candidate_offset = key_offset - node_size as u32;
					let candidate = self.arena.get_pointer(candidate_offset) as *const Node;

					// Check if this looks like our node
					if (*candidate).key_offset == key_offset {
						let stashed = (*candidate).key_trailer;
						let stashed_height = (stashed >> 32) as u32;
						if stashed_height == h {
							found_node_offset = candidate_offset;
							found_height = h;
							break;
						}
					}
				}
			}

			if found_node_offset == 0 {
				return false; // Couldn't find node
			}

			let node_offset = found_node_offset;
			let height = found_height;
			let node = self.arena.get_pointer(node_offset) as *mut Node;

			// Store actual trailer now
			(*node).key_trailer = trailer;

			// Try to increase skiplist height
			let mut list_height = self.height();
			while height > list_height {
				match self.height.compare_exchange_weak(
					list_height,
					height,
					AtomicOrdering::AcqRel,
					AtomicOrdering::Acquire,
				) {
					Ok(_) => break,
					Err(h) => list_height = h,
				}
			}

			// Find splice positions
			if self.find_splice(user_key, trailer, ins) {
				// Exact key already exists
				return false;
			}

			#[cfg(test)]
			if self.testing {
				std::thread::yield_now();
			}

			// Link node at each level from bottom up
			let mut invalidate_splice = false;

			for i in 0..height as usize {
				let mut prev = ins.spl[i].prev;
				let mut next = ins.spl[i].next;

				if prev == 0 {
					// New level created, use head/tail
					if next != 0 {
						panic!("next expected to be 0 when prev is 0");
					}
					prev = self.head;
					next = self.tail;
				}

				loop {
					let prev_offset = prev;
					let next_offset = next;

					// Initialize new node's links for this level
					(*node).tower_init(i, prev_offset, next_offset);

					// Check if next's prev link needs help
					let next_node = self.get_node(next_offset);
					let next_prev = (*next_node).prev_offset(i);
					if next_prev != prev_offset {
						let prev_node = self.get_node(prev_offset);
						let prev_next = (*prev_node).next_offset(i);
						if prev_next == next_offset {
							// Help concurrent inserter by fixing next's prev
							(*next_node).cas_prev_offset(i, next_prev, prev_offset);
						}
					}

					// CAS prev's next to point to our node
					let prev_node = self.get_node(prev_offset) as *const Node;
					if (*prev_node).cas_next_offset(i, next_offset, node_offset) {
						// Success! Update next's prev to point to our node
						#[cfg(test)]
						if self.testing {
							std::thread::yield_now();
						}

						let next_node = self.get_node(next_offset);
						(*next_node).cas_prev_offset(i, prev_offset, node_offset);
						break;
					}

					// CAS failed, recompute splice for this level
					let (new_prev, new_next, found) =
						self.find_splice_for_level(user_key, trailer, i, prev);
					if found {
						if i != 0 {
							panic!("found duplicate at non-base level");
						}
						return false;
					}
					prev = new_prev;
					next = new_next;
					invalidate_splice = true;
				}
			}

			if invalidate_splice {
				ins.height = 0;
			} else {
				// Update cached splice - new node becomes prev for subsequent inserts
				for i in 0..height as usize {
					ins.spl[i].prev = node_offset;
				}
			}

			true
		}
	}

	// ========================================================================
	// Read Operations
	// ========================================================================

	/// Find node with key >= target. Returns node offset (may be tail if not found).
	/// target is a length-prefixed internal key entry (from InternalKey::encode())
	unsafe fn find_greater_or_equal(&self, target: &[u8]) -> u32 {
		// Extract user key and trailer from target
		if target.len() < 4 {
			return self.tail;
		}
		let internal_key_len = u32::from_be_bytes(*(target.as_ptr() as *const [u8; 4])) as usize;
		if target.len() < 4 + internal_key_len || internal_key_len < 16 {
			return self.tail;
		}
		let target_internal_key = &target[4..4 + internal_key_len];
		let target_user_key = &target_internal_key[..target_internal_key.len() - 16];
		let target_trailer = u64::from_be_bytes(
			target_internal_key[target_internal_key.len() - 16..target_internal_key.len() - 8]
				.try_into()
				.unwrap(),
		);

		let mut prev = self.head;
		let mut level = self.height() as i32 - 1;
		let mut last_bigger = 0u32;

		loop {
			let next = self.get_next(prev, level as usize);

			if next == self.tail || next == last_bigger {
				if level == 0 {
					return next;
				}
				last_bigger = next;
				level -= 1;
				continue;
			}

			let next_node = self.get_node(next);
			let next_internal_key = (*next_node).get_key_bytes(&self.arena);
			if next_internal_key.len() < 16 {
				if level == 0 {
					return next;
				}
				last_bigger = next;
				level -= 1;
				continue;
			}

			let next_user_key = &next_internal_key[..next_internal_key.len() - 16];
			let next_trailer = (*next_node).key_trailer;

			let cmp = self.compare_internal_keys(
				next_user_key,
				next_trailer,
				target_user_key,
				target_trailer,
			);

			match cmp {
				Ordering::Equal => return next,
				Ordering::Greater if level == 0 => return next,
				Ordering::Less => prev = next,
				Ordering::Greater => {
					last_bigger = next;
					level -= 1;
				}
			}
		}
	}

	/// Find last node with key < target. Returns node offset (may be head if not found).
	/// target is a length-prefixed internal key entry (from InternalKey::encode())
	unsafe fn find_less_than(&self, target: &[u8]) -> u32 {
		// Extract user key and trailer from target
		if target.len() < 4 {
			return self.head;
		}
		let internal_key_len = u32::from_be_bytes(*(target.as_ptr() as *const [u8; 4])) as usize;
		if target.len() < 4 + internal_key_len || internal_key_len < 16 {
			return self.head;
		}
		let target_internal_key = &target[4..4 + internal_key_len];
		let target_user_key = &target_internal_key[..target_internal_key.len() - 16];
		let target_trailer = u64::from_be_bytes(
			target_internal_key[target_internal_key.len() - 16..target_internal_key.len() - 8]
				.try_into()
				.unwrap(),
		);

		let mut prev = self.head;
		let mut level = self.height() as i32 - 1;
		let mut last_not_after = 0u32;

		loop {
			let next = self.get_next(prev, level as usize);

			if next != last_not_after && next != self.tail {
				let next_node = self.get_node(next);
				let next_internal_key = (*next_node).get_key_bytes(&self.arena);
				if next_internal_key.len() >= 16 {
					let next_user_key = &next_internal_key[..next_internal_key.len() - 16];
					let next_trailer = (*next_node).key_trailer;

					let cmp = self.compare_internal_keys(
						next_user_key,
						next_trailer,
						target_user_key,
						target_trailer,
					);
					if cmp == Ordering::Less {
						prev = next;
						continue;
					}
				}
			}

			if level == 0 {
				return prev;
			}
			last_not_after = next;
			level -= 1;
		}
	}

	/// Find last node in the list. Returns node offset (may be head if empty).
	unsafe fn find_last(&self) -> u32 {
		let mut x = self.head;
		let mut level = self.height() as i32 - 1;

		loop {
			let next = self.get_next(x, level as usize);
			if next == self.tail {
				if level == 0 {
					return x;
				}
				level -= 1;
			} else {
				x = next;
			}
		}
	}

	pub fn iter(&self) -> SkipListIterator<'_, C> {
		SkipListIterator {
			list: self,
			nd: self.head,
		}
	}
}

// ============================================================================
// Iterator
// ============================================================================

pub struct SkipListIterator<'a, C: SkipListComparator> {
	list: &'a SkipList<C>,
	nd: u32, // Current node offset
}

impl<C: SkipListComparator> SkipListIterator<'_, C> {
	#[inline]
	pub fn valid(&self) -> bool {
		self.nd != self.list.head && self.nd != self.list.tail && self.nd != 0
	}

	#[inline]
	pub fn key(&self) -> &[u8] {
		assert!(self.valid());
		unsafe {
			let node = self.list.get_node(self.nd);
			// Return the full length-prefixed entry (key + value)
			let key_ptr = self.list.arena.get_pointer((*node).key_offset);
			read_length_prefixed_slice(key_ptr)
		}
	}

	#[inline]
	pub fn value(&self) -> &[u8] {
		assert!(self.valid());
		unsafe {
			let node = self.list.get_node(self.nd);
			let entry_ptr = self.list.arena.get_pointer((*node).key_offset);
			let key_len = u32::from_be_bytes(*(entry_ptr as *const [u8; 4])) as usize;
			let value_ptr = entry_ptr.add(4 + key_len);
			read_length_prefixed_slice(value_ptr)
		}
	}

	/// Move to next node - O(1)
	#[inline]
	pub fn next(&mut self) {
		assert!(self.valid());
		self.nd = self.list.get_next(self.nd, 0);
	}

	/// Move to previous node - O(1) with doubly-linked list!
	#[inline]
	pub fn prev(&mut self) {
		assert!(self.valid());
		self.nd = self.list.get_prev(self.nd, 0);
	}

	/// Seek to first node with key >= target
	/// target is an encoded internal key (from InternalKey::encode())
	/// Format: [user_key][trailer:8][timestamp:8] (NOT length-prefixed)
	pub fn seek(&mut self, target: &[u8]) {
		unsafe {
			// target is NOT length-prefixed - it's from InternalKey::encode()
			// Extract user key and trailer, then create length-prefixed format for comparison
			if target.len() < 16 {
				self.nd = self.list.tail;
				return;
			}
			let user_key = &target[..target.len() - 16];
			let trailer =
				u64::from_be_bytes(target[target.len() - 16..target.len() - 8].try_into().unwrap());

			// Create length-prefixed format for find_greater_or_equal
			let internal_key_len = user_key.len() + 16;
			let mut encoded = Vec::with_capacity(4 + internal_key_len);
			encoded.extend_from_slice(&(internal_key_len as u32).to_be_bytes());
			encoded.extend_from_slice(user_key);
			encoded.extend_from_slice(&trailer.to_be_bytes());
			encoded.extend_from_slice(&target[target.len() - 8..]); // timestamp

			self.nd = self.list.find_greater_or_equal(&encoded);
		}
	}

	/// Seek to first node
	#[inline]
	pub fn seek_to_first(&mut self) {
		self.nd = self.list.get_next(self.list.head, 0);
	}

	/// Seek to last node
	pub fn seek_to_last(&mut self) {
		unsafe {
			self.nd = self.list.find_last();
			if self.nd == self.list.head {
				self.nd = self.list.tail; // Empty list
			}
		}
	}
}
