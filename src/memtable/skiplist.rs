use std::cmp::Ordering;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicI32, AtomicPtr, Ordering as AtomicOrdering};
use std::sync::Arc;

use rand::Rng;

use crate::comparator::{Comparator, InternalKeyComparator};
use crate::memtable::arena::Arena;
use crate::sstable::InternalKeyKind;

// ============================================================================
// Helper Functions
// ============================================================================

#[inline(always)]
unsafe fn read_length_prefixed_slice(ptr: *const u8) -> &'static [u8] {
	let len = u32::from_be_bytes(*(ptr as *const [u8; 4])) as usize;
	std::slice::from_raw_parts(ptr.add(4), len)
}

#[inline(always)]
unsafe fn read_key_length(ptr: *const u8) -> usize {
	u32::from_be_bytes(*(ptr as *const [u8; 4])) as usize
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
	fn compare_key_with_slice(&self, key_ptr: *const u8, slice: &[u8]) -> Ordering;
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
	#[inline]
	fn compare(&self, a: *const u8, b: *const u8) -> Ordering {
		unsafe {
			let key_a = read_length_prefixed_slice(a);
			let key_b = read_length_prefixed_slice(b);
			Comparator::compare(&*self.internal_cmp, key_a, key_b)
		}
	}

	#[inline]
	fn compare_key_with_slice(&self, key_ptr: *const u8, slice: &[u8]) -> Ordering {
		unsafe {
			let key = read_length_prefixed_slice(key_ptr);
			Comparator::compare(&*self.internal_cmp, key, slice)
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
	#[inline(always)]
	unsafe fn key(&self) -> *const u8 {
		(self as *const Node as *const u8).add(std::mem::size_of::<AtomicPtr<Node>>())
	}

	#[inline(always)]
	unsafe fn next_at_level(&self, level: usize) -> &AtomicPtr<Node> {
		&*((self as *const Node as *const AtomicPtr<Node>).sub(level))
	}

	/// Acquire load for concurrent access (seek, insert)
	#[inline(always)]
	unsafe fn next(&self, level: usize) -> *mut Node {
		if level == 0 {
			self.next[0].load(AtomicOrdering::Acquire)
		} else {
			self.next_at_level(level).load(AtomicOrdering::Acquire)
		}
	}

	/// Relaxed load for forward iteration - FAST PATH
	/// Safe because: append-only structure, we've already "entered" via Acquire
	#[inline(always)]
	unsafe fn next_level0_relaxed(&self) -> *mut Node {
		self.next[0].load(AtomicOrdering::Relaxed)
	}

	#[inline(always)]
	unsafe fn set_next(&self, level: usize, x: *mut Node) {
		if level == 0 {
			self.next[0].store(x, AtomicOrdering::Release);
		} else {
			self.next_at_level(level).store(x, AtomicOrdering::Release);
		}
	}

	#[inline]
	unsafe fn cas_next(&self, level: usize, expected: *mut Node, new: *mut Node) -> bool {
		let atomic = if level == 0 {
			&self.next[0]
		} else {
			self.next_at_level(level)
		};

		atomic
			.compare_exchange(expected, new, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
			.is_ok()
	}

	#[inline(always)]
	unsafe fn stash_height(&mut self, height: i32) {
		let ptr = &mut self.next[0] as *mut AtomicPtr<Node> as *mut i32;
		*ptr = height;
	}

	#[inline(always)]
	unsafe fn unstash_height(&self) -> i32 {
		let ptr = &self.next[0] as *const AtomicPtr<Node> as *const i32;
		*ptr
	}
}

// ============================================================================
// Position
// ============================================================================

#[derive(Clone, Copy)]
struct Position {
	prev: *mut Node,
	next: *mut Node,
}

impl Position {
	#[inline(always)]
	const fn new(prev: *mut Node, next: *mut Node) -> Self {
		Self {
			prev,
			next,
		}
	}
}

// ============================================================================
// SkipList
// ============================================================================

pub struct SkipList<C: SkipListComparator> {
	arena: Arc<Arena>,
	head: *mut Node,
	max_height: AtomicI32,
	comparator: C,
	k_max_height: u16,
	k_scaled_inverse_branching: u32,
}

unsafe impl<C: SkipListComparator> Send for SkipList<C> {}
unsafe impl<C: SkipListComparator> Sync for SkipList<C> {}

impl<C: SkipListComparator> SkipList<C> {
	pub fn new(comparator: C, arena: Arc<Arena>, max_height: i32, branching: u16) -> Self {
		let max_height = max_height.max(1) as u16;
		let k_scaled_inverse_branching = u32::MAX / branching as u32;

		let prefix = std::mem::size_of::<AtomicPtr<Node>>() * (max_height as usize - 1);
		let head_size = prefix + std::mem::size_of::<Node>();
		let raw = arena.allocate(head_size);
		let head_ptr = unsafe { raw.add(prefix) as *mut Node };

		unsafe {
			for i in 0..max_height as usize {
				if i == 0 {
					(*head_ptr).next[0].store(null_mut(), AtomicOrdering::Relaxed);
				} else {
					(*head_ptr).next_at_level(i).store(null_mut(), AtomicOrdering::Relaxed);
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

	#[inline]
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

	pub fn allocate_key(&self, key_size: usize) -> Option<*mut u8> {
		let height = self.random_height();
		let prefix = std::mem::size_of::<AtomicPtr<Node>>() * (height as usize - 1);
		let total_size = prefix + std::mem::size_of::<Node>() + key_size;
		let raw = self.arena.try_allocate(total_size)?;

		unsafe {
			let node_ptr = raw.add(prefix) as *mut Node;
			(*node_ptr).stash_height(height);
			Some((*node_ptr).key() as *mut u8)
		}
	}

	#[inline]
	unsafe fn find_level(&self, key: *const u8, mut x: *mut Node, level: usize) -> Position {
		loop {
			let next = (*x).next(level);

			if next.is_null() {
				return Position::new(x, next);
			}

			let next_key = (*next).key();
			if self.comparator.compare(key, next_key) != Ordering::Greater {
				return Position::new(x, next);
			}

			x = next;
		}
	}

	unsafe fn find_all_positions(&self, key: *const u8, max_h: i32) -> Vec<Position> {
		let mut positions = vec![Position::new(null_mut(), null_mut()); max_h as usize];
		let mut x = self.head;

		for level in (0..max_h as usize).rev() {
			let pos = self.find_level(key, x, level);
			positions[level] = pos;
			x = pos.prev;
		}

		positions
	}

	pub fn insert(&self, key: *const u8) -> bool {
		unsafe {
			let node_ptr = key.sub(std::mem::size_of::<AtomicPtr<Node>>()) as *mut Node;
			let height = (*node_ptr).unstash_height();
			debug_assert!(height >= 1 && height <= self.k_max_height as i32);

			let mut max_h = self.max_height.load(AtomicOrdering::Relaxed);
			while height > max_h {
				match self.max_height.compare_exchange_weak(
					max_h,
					height,
					AtomicOrdering::AcqRel,
					AtomicOrdering::Relaxed,
				) {
					Ok(_) => break,
					Err(x) => max_h = x,
				}
			}
			let max_h = self.max_height.load(AtomicOrdering::Relaxed);

			let mut positions = self.find_all_positions(key, max_h);

			for level in 0..height as usize {
				loop {
					let pos = positions[level];

					if level == 0 {
						if pos.prev != self.head {
							let prev_key = (*pos.prev).key();
							if self.comparator.compare(prev_key, key) != Ordering::Less {
								return false;
							}
						}
						if !pos.next.is_null() {
							let next_key = (*pos.next).key();
							if self.comparator.compare(next_key, key) != Ordering::Greater {
								return false;
							}
						}
					}

					(*node_ptr).set_next(level, pos.next);

					if (*pos.prev).cas_next(level, pos.next, node_ptr) {
						break;
					}

					positions[level] = self.find_level(key, pos.prev, level);
				}
			}

			true
		}
	}

	unsafe fn find_greater_or_equal_by_slice(&self, target: &[u8]) -> *mut Node {
		let mut x = self.head;
		let mut level = self.max_height.load(AtomicOrdering::Acquire) - 1;
		let mut last_bigger: *mut Node = null_mut();

		loop {
			let next = (*x).next(level as usize);

			let cmp = if next.is_null() || next == last_bigger {
				Ordering::Greater
			} else {
				self.comparator.compare_key_with_slice((*next).key(), target)
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
		let mut level = self.max_height.load(AtomicOrdering::Acquire) - 1;
		let mut x = self.head;
		let mut last_not_after: *mut Node = null_mut();

		loop {
			let next = (*x).next(level as usize);

			if next != last_not_after && !next.is_null() {
				if self.comparator.compare((*next).key(), key) == Ordering::Less {
					x = next;
					continue;
				}
			}

			if level == 0 {
				return x;
			}
			last_not_after = next;
			level -= 1;
		}
	}

	unsafe fn find_last(&self) -> *mut Node {
		let mut x = self.head;
		let mut level = self.max_height.load(AtomicOrdering::Acquire) - 1;

		loop {
			let next = (*x).next(level as usize);
			if next.is_null() {
				if level == 0 {
					return x;
				}
				level -= 1;
			} else {
				x = next;
			}
		}
	}

	#[inline]
	pub fn iter(&self) -> SkipListIterator<'_, C> {
		SkipListIterator::new(self)
	}
}

// ============================================================================
// Iterator - Optimized for Range Scans
// ============================================================================

pub struct SkipListIterator<'a, C: SkipListComparator> {
	list: &'a SkipList<C>,
	node: *const Node,
}

impl<'a, C: SkipListComparator> SkipListIterator<'a, C> {
	#[inline(always)]
	fn new(list: &'a SkipList<C>) -> Self {
		Self {
			list,
			node: null_mut(),
		}
	}

	#[inline(always)]
	pub fn valid(&self) -> bool {
		!self.node.is_null()
	}

	#[inline(always)]
	pub fn key(&self) -> &'static [u8] {
		debug_assert!(self.valid());
		unsafe { read_length_prefixed_slice((*self.node).key()) }
	}

	#[inline(always)]
	pub fn value(&self) -> &'static [u8] {
		debug_assert!(self.valid());
		unsafe {
			let entry_ptr = (*self.node).key();
			let key_len = read_key_length(entry_ptr);
			let value_ptr = entry_ptr.add(4 + key_len);
			read_length_prefixed_slice(value_ptr)
		}
	}

	/// FAST PATH: Relaxed load, no branch, prefetch next
	#[inline(always)]
	pub fn next(&mut self) {
		debug_assert!(self.valid());
		unsafe {
			// Relaxed is safe: append-only, we entered via Acquire in seek
			let next = (*self.node).next_level0_relaxed();
			self.node = next;
		}
	}

	pub fn prev(&mut self) {
		debug_assert!(self.valid());
		unsafe {
			let key_ptr = (*self.node).key();
			let prev = self.list.find_less_than(key_ptr);
			if prev == self.list.head {
				self.node = null_mut();
			} else {
				self.node = prev;
			}
		}
	}

	#[inline(always)]
	pub fn seek(&mut self, target: &[u8]) {
		unsafe {
			self.node = self.list.find_greater_or_equal_by_slice(target);
		}
	}

	#[inline(always)]
	pub fn seek_to_first(&mut self) {
		unsafe {
			// Use Acquire here to "enter" the structure
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
