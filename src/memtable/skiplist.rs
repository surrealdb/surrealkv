use std::cmp::Ordering;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicI32, AtomicPtr, Ordering as AtomicOrdering};
use std::sync::Arc;

use rand::Rng;

use crate::comparator::{Comparator, InternalKeyComparator};
use crate::memtable::arena::Arena;
use crate::sstable::InternalKeyKind;

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

	unsafe fn next(&self, level: usize) -> *mut Node {
		if level == 0 {
			self.next[0].load(AtomicOrdering::Acquire)
		} else {
			self.next_at_level(level).load(AtomicOrdering::Acquire)
		}
	}

	unsafe fn set_next(&self, level: usize, x: *mut Node) {
		if level == 0 {
			self.next[0].store(x, AtomicOrdering::Release);
		} else {
			self.next_at_level(level).store(x, AtomicOrdering::Release);
		}
	}

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

	unsafe fn stash_height(&mut self, height: i32) {
		let ptr = &mut self.next[0] as *mut AtomicPtr<Node> as *mut i32;
		*ptr = height;
	}

	unsafe fn unstash_height(&self) -> i32 {
		let ptr = &self.next[0] as *const AtomicPtr<Node> as *const i32;
		*ptr
	}
}

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

	/// Insert a key using lock-free CAS.
	/// Returns false if key already exists.
	pub fn insert(&self, key: *const u8) -> bool {
		unsafe {
			let node_ptr = key.sub(std::mem::size_of::<AtomicPtr<Node>>()) as *mut Node;
			let height = (*node_ptr).unstash_height();
			assert!(height >= 1 && height <= self.k_max_height as i32);

			// Update max_height if needed
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

			// Find predecessors and successors at each level
			let mut prev = vec![null_mut::<Node>(); max_h as usize];
			let mut next = vec![null_mut::<Node>(); max_h as usize];

			self.find_position(key, max_h, &mut prev, &mut next);

			// Check for duplicate at level 0
			if !next[0].is_null() {
				let next_key = (*next[0]).key();
				if self.comparator.compare(next_key, key) == Ordering::Equal {
					return false;
				}
			}

			// Link at each level using CAS
			for i in 0..height as usize {
				loop {
					(*node_ptr).set_next(i, next[i]);

					if (*prev[i]).cas_next(i, next[i], node_ptr) {
						break;
					}

					// CAS failed, recompute this level
					self.find_position_for_level(key, i, &mut prev, &mut next);
				}
			}

			true
		}
	}

	/// Find predecessors and successors at all levels
	unsafe fn find_position(
		&self,
		key: *const u8,
		max_h: i32,
		prev: &mut [*mut Node],
		next: &mut [*mut Node],
	) {
		let mut x = self.head;

		for level in (0..max_h as usize).rev() {
			loop {
				let next_node = (*x).next(level);

				if next_node.is_null() {
					break;
				}

				let next_key = (*next_node).key();
				if self.comparator.compare(key, next_key) != Ordering::Greater {
					break;
				}

				x = next_node;
			}

			prev[level] = x;
			next[level] = (*x).next(level);
		}
	}

	/// Recompute a single level after CAS failure
	unsafe fn find_position_for_level(
		&self,
		key: *const u8,
		level: usize,
		prev: &mut [*mut Node],
		next: &mut [*mut Node],
	) {
		// Start from predecessor at level above (or head)
		let mut x = if level + 1 < prev.len() {
			prev[level + 1]
		} else {
			self.head
		};

		loop {
			let next_node = (*x).next(level);

			if next_node.is_null() {
				break;
			}

			let next_key = (*next_node).key();
			if self.comparator.compare(key, next_key) != Ordering::Greater {
				break;
			}

			x = next_node;
		}

		prev[level] = x;
		next[level] = (*x).next(level);
	}

	unsafe fn find_greater_or_equal(&self, key: *const u8) -> *mut Node {
		let mut x = self.head;
		let mut level = self.max_height.load(AtomicOrdering::Relaxed) - 1;

		loop {
			let next = (*x).next(level as usize);

			let cmp = if next.is_null() {
				Ordering::Greater
			} else {
				let next_key = (*next).key();
				self.comparator.compare(next_key, key)
			};

			match cmp {
				Ordering::Equal => return next,
				Ordering::Greater if level == 0 => return next,
				Ordering::Less => x = next,
				Ordering::Greater => level -= 1,
			}
		}
	}

	unsafe fn find_less_than(&self, key: *const u8) -> *mut Node {
		let mut level = self.max_height.load(AtomicOrdering::Relaxed) - 1;
		let mut x = self.head;

		loop {
			let next = (*x).next(level as usize);

			if !next.is_null() {
				let next_key = (*next).key();
				if self.comparator.compare(next_key, key) == Ordering::Less {
					x = next;
					continue;
				}
			}

			if level == 0 {
				return x;
			} else {
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

pub struct SkipListIterator<'a, C: SkipListComparator> {
	list: &'a SkipList<C>,
	node: *const Node,
}

impl<C: SkipListComparator> SkipListIterator<'_, C> {
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
