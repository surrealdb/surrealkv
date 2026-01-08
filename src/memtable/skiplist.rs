//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2  and Apache 2.0 License.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Arena is an implementation of Allocator class. For a request of small size,
// it allocates a block with pre-defined block size. For a request of big
// size, it uses malloc to directly get the requested size.

// Concurrent Skip List
//
// insert() - Lock-free using CAS, safe for concurrent use
//
// Thread Safety:
// - Reads are always lock-free (use Acquire loads)
// - insert() is fully lock-free via CAS retry loops
//
// Memory Ordering:
// - Release store when linking node (publishes node to readers)
// - Acquire load when reading next pointers (sees published data)
// - CAS with AcqRel ordering for concurrent modifications

use std::cmp::Ordering;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicI32, AtomicPtr, Ordering as AtomicOrdering};
use std::sync::Arc;

use rand::Rng;

use crate::comparator::{Comparator, InternalKeyComparator};
use crate::memtable::arena::ConcurrentArena;
use crate::sstable::InternalKeyKind;

// ============================================================================
// Helper Functions
// ============================================================================

#[inline]
unsafe fn read_length_prefixed_slice(ptr: *const u8) -> &'static [u8] {
	let len = u32::from_be_bytes(*(ptr as *const [u8; 4])) as usize;
	std::slice::from_raw_parts(ptr.add(4), len)
}

pub fn encoded_entry_size(user_key_len: usize, value_len: usize) -> usize {
	let internal_key_size = user_key_len + 16;
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

// ============================================================================
// Comparator Trait
// ============================================================================

pub trait SkipListComparator: Send + Sync {
	fn compare(&self, a: *const u8, b: *const u8) -> Ordering;
}

pub struct MemTableKeyComparator {
	internal_cmp: Arc<InternalKeyComparator>,
}

impl MemTableKeyComparator {
	pub fn new(internal_cmp: Arc<InternalKeyComparator>) -> Self {
		Self {
			internal_cmp,
		}
	}
}

impl SkipListComparator for MemTableKeyComparator {
	fn compare(&self, a: *const u8, b: *const u8) -> Ordering {
		unsafe {
			let key_a = read_length_prefixed_slice(a);
			let key_b = read_length_prefixed_slice(b);
			Comparator::compare(&*self.internal_cmp, key_a, key_b)
		}
	}
}

// ============================================================================
// Node Structure
// ============================================================================

#[repr(C)]
struct Node {
	next: [AtomicPtr<Node>; 1],
}

impl Node {
	unsafe fn key(&self) -> *const u8 {
		(self as *const Node as *const u8).add(std::mem::size_of::<AtomicPtr<Node>>())
	}

	unsafe fn next_at_level(&self, level: usize) -> &AtomicPtr<Node> {
		&*((self as *const Node as *const AtomicPtr<Node>).sub(level))
	}

	/// Load next pointer with Acquire ordering (for readers)
	unsafe fn next(&self, level: usize) -> *mut Node {
		if level == 0 {
			self.next[0].load(AtomicOrdering::Acquire)
		} else {
			self.next_at_level(level).load(AtomicOrdering::Acquire)
		}
	}

	/// Store next pointer with Release ordering (publishes node)
	unsafe fn set_next(&self, level: usize, x: *mut Node) {
		if level == 0 {
			self.next[0].store(x, AtomicOrdering::Release);
		} else {
			self.next_at_level(level).store(x, AtomicOrdering::Release);
		}
	}

	/// CAS operation for lock-free insertion
	/// Returns true if successful, false if another thread modified the pointer
	unsafe fn cas_next(&self, level: usize, expected: *mut Node, new: *mut Node) -> bool {
		let atomic = if level == 0 {
			&self.next[0]
		} else {
			self.next_at_level(level)
		};

		atomic
			.compare_exchange(
				expected,
				new,
				AtomicOrdering::AcqRel, // Success: acquire previous writes, release our writes
				AtomicOrdering::Acquire, // Failure: acquire to see what changed
			)
			.is_ok()
	}

	unsafe fn stash_height(&mut self, height: i32) {
		let ptr = &mut self.next[0] as *mut AtomicPtr<Node> as *mut i32;
		*ptr = height;
	}

	unsafe fn unstash_height(&self) -> i32 {
		let ptr = &self.next[0] as *const AtomicPtr<Node> as *const i32;
		*ptr
	}
}

// ============================================================================
// Splice - Thread-local insertion state
// ============================================================================

/// Splice caches the predecessor and successor nodes at each level.
///
/// Invariant: prev[i+1].key <= prev[i].key < next[i].key <= next[i+1].key
///
/// For concurrent inserts, each thread must have its own Splice.
/// The splice may become stale due to concurrent modifications,
/// which is detected and handled by CAS failures.
pub struct Splice {
	height: i32,
	prev: Vec<*mut Node>,
	next: Vec<*mut Node>,
}

impl Splice {
	pub fn new(max_height: usize) -> Self {
		Self {
			height: 0,
			prev: vec![null_mut(); max_height + 1],
			next: vec![null_mut(); max_height + 1],
		}
	}
}

// Splice contains raw pointers but is only used thread-locally
unsafe impl Send for Splice {}

// ============================================================================
// InlineSkipList
// ============================================================================

pub struct InlineSkipList<C: SkipListComparator> {
	arena: Arc<ConcurrentArena>,
	head: *mut Node,
	max_height: AtomicI32,
	comparator: C,
	k_max_height: u16,
	k_scaled_inverse_branching: u32,
}

unsafe impl<C: SkipListComparator> Send for InlineSkipList<C> {}
unsafe impl<C: SkipListComparator> Sync for InlineSkipList<C> {}

impl<C: SkipListComparator> InlineSkipList<C> {
	pub fn new(
		comparator: C,
		arena: Arc<ConcurrentArena>,
		max_height: i32,
		branching: u16,
	) -> Self {
		let max_height = max_height.max(1) as u16;
		let k_scaled_inverse_branching = u32::MAX / branching as u32;

		let prefix = std::mem::size_of::<AtomicPtr<Node>>() * (max_height as usize - 1);
		let head_size = prefix + std::mem::size_of::<Node>();
		let raw = arena.allocate(head_size);
		let head_ptr = unsafe { raw.add(prefix) as *mut Node };

		unsafe {
			for i in 0..max_height as usize {
				let node = &*head_ptr;
				if i == 0 {
					node.next[0].store(null_mut(), AtomicOrdering::Relaxed);
				} else {
					node.next_at_level(i).store(null_mut(), AtomicOrdering::Relaxed);
				}
			}
		}

		Self {
			arena,
			head: head_ptr,
			max_height: AtomicI32::new(1),
			comparator,
			k_max_height: max_height,
			k_scaled_inverse_branching,
		}
	}

	fn random_height(&self) -> i32 {
		let mut rng = rand::rng();
		let mut height = 1;
		while height < self.k_max_height as i32
			&& rng.random::<u32>() < self.k_scaled_inverse_branching
		{
			height += 1;
		}
		height
	}

	pub fn allocate_key(&self, key_size: usize) -> *mut u8 {
		let height = self.random_height();
		let prefix = std::mem::size_of::<AtomicPtr<Node>>() * (height as usize - 1);
		let total_size = prefix + std::mem::size_of::<Node>() + key_size;
		let raw = self.arena.allocate(total_size);

		unsafe {
			let node_ptr = raw.add(prefix) as *mut Node;
			(*node_ptr).stash_height(height);
			(*node_ptr).key() as *mut u8
		}
	}

	// ========================================================================
	// Concurrent Insert (lock-free using CAS)
	// ========================================================================

	/// Insert a key concurrently without external synchronization.
	/// Uses CAS operations to handle concurrent modifications.
	/// Each call creates a thread-local Splice on the stack.
	pub fn insert(&self, key: *const u8) -> bool {
		// Thread-local splice - no sharing between threads
		let mut splice = Splice::new(self.k_max_height as usize);
		unsafe { self.insert_impl(key, &mut splice, false) }
	}

	// ========================================================================
	// Core Insert Implementation
	// ========================================================================

	/// Core insert implementation parameterized by UseCAS.
	///
	/// When UseCAS=false: Simple linking, requires external synchronization
	/// When UseCAS=true:  CAS-based linking, lock-free concurrent
	///
	/// TLA+ Correctness:
	/// - Linearization point: When prev[0].next is successfully updated to point to new node
	/// - For CAS mode: The successful CAS at level 0 is the linearization point
	/// - Sorted invariant maintained: We only link when prev.key < new.key < next.key
	unsafe fn insert_impl(
		&self,
		key: *const u8,
		splice: &mut Splice,
		allow_partial_splice_fix: bool,
	) -> bool {
		// Get node from key pointer (key is stored after Node struct)
		let node_ptr = key.sub(std::mem::size_of::<AtomicPtr<Node>>()) as *mut Node;
		let height = (*node_ptr).unstash_height();
		assert!(height >= 1 && height <= self.k_max_height as i32);

		// Update max_height atomically if this node is taller
		let mut max_h = self.max_height.load(AtomicOrdering::Relaxed);
		while height > max_h {
			match self.max_height.compare_exchange_weak(
				max_h,
				height,
				AtomicOrdering::AcqRel,
				AtomicOrdering::Relaxed,
			) {
				Ok(_) => {
					max_h = height;
					break;
				}
				Err(x) => max_h = x,
			}
		}

		// Determine how much of the splice needs recomputation
		let recompute_height =
			self.compute_recompute_height(key, splice, max_h, allow_partial_splice_fix);

		// Recompute stale splice levels
		if recompute_height > 0 {
			self.recompute_splice_levels(key, splice, recompute_height);
		}

		// Link the node at each level
		self.insert_with_cas(node_ptr, key, splice, height)
	}

	/// Compute how many levels of the splice need to be recomputed
	unsafe fn compute_recompute_height(
		&self,
		key: *const u8,
		splice: &mut Splice,
		max_h: i32,
		allow_partial_splice_fix: bool,
	) -> i32 {
		if splice.height < max_h {
			// Splice never used or max_height grew
			splice.prev[max_h as usize] = self.head;
			splice.next[max_h as usize] = null_mut();
			splice.height = max_h;
			return max_h;
		}

		// Check if existing splice still brackets the key
		let mut recompute_height = 0i32;

		while recompute_height < max_h {
			let level = recompute_height as usize;

			// Check if splice is tight (no insertions between prev and next)
			if (*splice.prev[level]).next(level) != splice.next[level] {
				recompute_height += 1;
				continue;
			}

			// Check if key is before splice
			if splice.prev[level] != self.head {
				let prev_key = (*splice.prev[level]).key();
				if self.comparator.compare(key, prev_key) != Ordering::Greater {
					if allow_partial_splice_fix {
						let bad = splice.prev[level];
						while splice.prev[recompute_height as usize] == bad {
							recompute_height += 1;
						}
					} else {
						recompute_height = max_h;
					}
					continue;
				}
			}

			// Check if key is after splice
			if !splice.next[level].is_null() {
				let next_key = (*splice.next[level]).key();
				if self.comparator.compare(key, next_key) != Ordering::Less {
					if allow_partial_splice_fix {
						let bad = splice.next[level];
						while splice.next[recompute_height as usize] == bad {
							recompute_height += 1;
						}
					} else {
						recompute_height = max_h;
					}
					continue;
				}
			}

			// This level brackets the key correctly
			break;
		}

		recompute_height
	}

	/// CAS-based insertion for concurrent use
	///
	/// For each level from 0 to height-1:
	/// 1. Set new node's next pointer
	/// 2. CAS predecessor's next from expected to new node
	/// 3. On CAS failure: recompute splice for this level and retry
	///
	/// # Linearization Point
	/// The successful CAS at level 0 is the linearization point. Once level 0
	/// is linked, the node is visible to readers. Higher levels are optimization
	/// for faster traversal and do not affect correctness.
	///
	/// # Splice Invariant
	/// For correctness, we maintain: prev[i+1].key <= prev[i].key < key < next[i].key <=
	/// next[i+1].key When a CAS fails at level i > 0, the splice may become inconsistent with the
	/// actual list structure due to concurrent modifications. We must recompute
	/// all remaining levels from top down to restore the invariant.
	///
	/// # Memory Ordering
	/// - Node's next pointers are set with Release ordering before CAS to ensure visibility on
	///   weakly-ordered architectures (ARM, PowerPC).
	/// - CAS uses AcqRel to both acquire previous writes and release our writes.
	unsafe fn insert_with_cas(
		&self,
		node_ptr: *mut Node,
		key: *const u8,
		splice: &mut Splice,
		height: i32,
	) -> bool {
		let mut splice_is_valid = true;

		for i in 0..height as usize {
			loop {
				// Check for duplicate key (only at level 0)
				if i == 0 {
					if !splice.next[i].is_null() {
						let next_key = (*splice.next[i]).key();
						if self.comparator.compare(next_key, key) != Ordering::Greater {
							return false; // Duplicate or key already exists
						}
					}
					if splice.prev[i] != self.head {
						let prev_key = (*splice.prev[i]).key();
						if self.comparator.compare(prev_key, key) != Ordering::Less {
							return false; // Duplicate
						}
					}
				}

				// Set new node's next pointer with Release ordering.
				// This ensures the node's data is visible to readers on ARM/weak memory models
				// before the CAS makes the node reachable.
				(*node_ptr).set_next(i, splice.next[i]);

				// CAS: Try to link node into the list
				// This is the linearization point when successful at level 0
				if (*splice.prev[i]).cas_next(i, splice.next[i], node_ptr) {
					// Success! Node is now linked at this level
					break;
				}

				// CAS failed - another thread modified the list concurrently.
				// Recompute prev and next for this level.
				self.find_splice_for_level(key, splice.prev[i], null_mut(), i, splice);

				// CRITICAL FIX: When CAS fails at level i > 0, the splice invariant
				// may be violated because another thread's insertion could have changed
				// the list structure. We must recompute ALL remaining levels (i down to 0)
				// from top down to ensure consistency.
				//
				// Without this fix, we could end up with:
				// - splice.prev[i] pointing to a node that was unlinked
				// - splice.next[i] pointing to a node before splice.prev[i]
				// - Violation of: prev[i].key < key < next[i].key
				if i > 0 {
					splice_is_valid = false;
					// Recompute all levels from i down to 0 to restore invariant
					self.recompute_splice_levels(key, splice, i as i32);
				}
			}
		}

		// Debug assertion: verify splice invariant in debug builds
		#[cfg(debug_assertions)]
		{
			self.debug_validate_splice(key, splice, height);
		}

		// Update splice for subsequent inserts (if still valid)
		if splice_is_valid {
			for i in 0..height as usize {
				splice.prev[i] = node_ptr;
			}
		} else {
			splice.height = 0; // Force full recompute next time
		}

		true
	}

	/// Validate splice invariant in debug builds.
	/// Checks that prev[i].key < key < next[i].key for all levels.
	#[cfg(debug_assertions)]
	unsafe fn debug_validate_splice(&self, key: *const u8, splice: &Splice, height: i32) {
		for i in 0..height as usize {
			// Check prev[i].key < key
			if splice.prev[i] != self.head {
				let prev_key = (*splice.prev[i]).key();
				debug_assert!(
					self.comparator.compare(prev_key, key) == Ordering::Less,
					"Splice invariant violated: prev[{i}].key >= key"
				);
			}
			// Check key < next[i].key
			if !splice.next[i].is_null() {
				let next_key = (*splice.next[i]).key();
				debug_assert!(
					self.comparator.compare(key, next_key) == Ordering::Less,
					"Splice invariant violated: key >= next[{i}].key"
				);
			}
		}
	}

	/// Find the splice (prev, next) for a single level
	/// Used to repair splice after CAS failure
	unsafe fn find_splice_for_level(
		&self,
		key: *const u8,
		mut before: *mut Node,
		after: *mut Node,
		level: usize,
		splice: &mut Splice,
	) {
		loop {
			let next = (*before).next(level);

			// Prefetch for better cache performance
			if !next.is_null() {
				#[cfg(target_arch = "x86_64")]
				{
					std::arch::x86_64::_mm_prefetch(
						(*next).next(level) as *const i8,
						std::arch::x86_64::_MM_HINT_T0,
					);
				}
			}

			if next == after || next.is_null() {
				splice.prev[level] = before;
				splice.next[level] = next;
				return;
			}

			let next_key = (*next).key();
			if self.comparator.compare(key, next_key) != Ordering::Greater {
				splice.prev[level] = before;
				splice.next[level] = next;
				return;
			}

			before = next;
		}
	}

	/// Recompute splice levels from highest down to 0
	unsafe fn recompute_splice_levels(
		&self,
		key: *const u8,
		splice: &mut Splice,
		recompute_level: i32,
	) {
		// key is already a pointer to length-prefixed data in the arena
		// No need to decode and re-encode - just use it directly

		for level in (0..recompute_level as usize).rev() {
			let before = if level + 1 < splice.prev.len() {
				splice.prev[level + 1]
			} else {
				self.head
			};
			let after = if level + 1 < splice.next.len() {
				splice.next[level + 1]
			} else {
				null_mut()
			};

			// Use the key pointer directly - it's already properly formatted
			self.find_splice_for_level(key, before, after, level, splice);
		}
	}

	// ========================================================================
	// Read Operations (always lock-free)
	// ========================================================================

	unsafe fn find_greater_or_equal(&self, key: *const u8) -> *mut Node {
		let mut x = self.head;
		let mut level = self.max_height.load(AtomicOrdering::Relaxed) - 1;
		let mut last_bigger: *mut Node = null_mut();

		loop {
			let next = (*x).next(level as usize);

			if !next.is_null() {
				#[cfg(target_arch = "x86_64")]
				{
					std::arch::x86_64::_mm_prefetch(
						(*next).key() as *const i8,
						std::arch::x86_64::_MM_HINT_T0,
					);
				}
			}

			let cmp = if next.is_null() || next == last_bigger {
				Ordering::Greater
			} else {
				let next_key = (*next).key();
				self.comparator.compare(next_key, key)
			};

			match cmp {
				Ordering::Equal => return next,
				Ordering::Greater if level == 0 => return next,
				Ordering::Less => x = next,
				Ordering::Greater => {
					last_bigger = next;
					level -= 1;
				}
			}
		}
	}

	unsafe fn find_less_than(&self, key: *const u8) -> *mut Node {
		let mut level = self.max_height.load(AtomicOrdering::Relaxed) - 1;
		let mut x = self.head;
		let mut last_not_after: *mut Node = null_mut();

		loop {
			let next = (*x).next(level as usize);

			if !next.is_null() {
				#[cfg(target_arch = "x86_64")]
				{
					std::arch::x86_64::_mm_prefetch(
						(*next).key() as *const i8,
						std::arch::x86_64::_MM_HINT_T0,
					);
				}
			}

			if next != last_not_after && !next.is_null() {
				let next_key = (*next).key();
				if self.comparator.compare(next_key, key) == Ordering::Less {
					x = next;
					continue;
				}
			}

			if level == 0 {
				return x;
			} else {
				last_not_after = next;
				level -= 1;
			}
		}
	}

	unsafe fn find_last(&self) -> *mut Node {
		let mut x = self.head;
		let mut level = self.max_height.load(AtomicOrdering::Relaxed) - 1;

		loop {
			let next = (*x).next(level as usize);
			if next.is_null() {
				if level == 0 {
					return x;
				} else {
					level -= 1;
				}
			} else {
				x = next;
			}
		}
	}

	pub fn iter(&self) -> SkipListIterator<'_, C> {
		SkipListIterator {
			list: self,
			node: null_mut(),
		}
	}
}

// ============================================================================
// Iterator
// ============================================================================

pub struct SkipListIterator<'a, C: SkipListComparator> {
	list: &'a InlineSkipList<C>,
	node: *const Node,
}

impl<'a, C: SkipListComparator> SkipListIterator<'a, C> {
	pub fn valid(&self) -> bool {
		!self.node.is_null()
	}

	pub fn key(&self) -> &'static [u8] {
		assert!(self.valid());
		unsafe { read_length_prefixed_slice((*self.node).key()) }
	}

	pub fn value(&self) -> &'static [u8] {
		assert!(self.valid());
		unsafe {
			let entry_ptr = (*self.node).key();
			let key_len = u32::from_be_bytes(*(entry_ptr as *const [u8; 4])) as usize;
			let value_ptr = entry_ptr.add(4 + key_len);
			read_length_prefixed_slice(value_ptr)
		}
	}

	pub fn next(&mut self) {
		assert!(self.valid());
		unsafe {
			self.node = (*self.node).next(0);
		}
	}

	pub fn prev(&mut self) {
		assert!(self.valid());
		unsafe {
			// Use the length-prefixed key pointer for comparison
			let key_ptr = (*self.node).key();
			self.node = self.list.find_less_than(key_ptr);
			if self.node == self.list.head {
				self.node = null_mut();
			}
		}
	}

	pub fn seek(&mut self, target: &[u8]) {
		unsafe {
			let mut encoded = Vec::with_capacity(4 + target.len());
			encoded.extend_from_slice(&(target.len() as u32).to_be_bytes());
			encoded.extend_from_slice(target);
			self.node = self.list.find_greater_or_equal(encoded.as_ptr());
		}
	}

	pub fn seek_to_first(&mut self) {
		unsafe {
			self.node = (*self.list.head).next(0);
		}
	}

	pub fn seek_to_last(&mut self) {
		unsafe {
			self.node = self.list.find_last();
			if self.node == self.list.head {
				self.node = null_mut();
			}
		}
	}
}
