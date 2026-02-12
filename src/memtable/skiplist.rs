// Copyright 2017 Dgraph Labs, Inc. and Contributors
// Modifications copyright (C) 2017 Andy Kimball and Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This code is based on the original implementation in arenaskl by pebble.
// https://github.com/cockroachdb/pebble/blob/master/internal/arenaskl/skl.go

use std::cmp::Ordering;
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
use std::sync::Arc;

use rand::Rng;

use crate::error::Result as CrateResult;
use crate::memtable::arena::Arena;
use crate::{InternalKey, InternalKeyRef, LSMIterator};

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
		for p_item in p.iter_mut() {
			*p_item = (u32::MAX as f64 * prob) as u32;
			prob *= P_VALUE;
		}
		p
	})
}

#[derive(Debug, Clone, PartialEq)]
pub enum Error {
	RecordExists,
	ArenaFull,
}

pub type Compare = fn(&[u8], &[u8]) -> Ordering;

#[repr(C)]
struct Links {
	next_offset: AtomicU32,
	prev_offset: AtomicU32,
}

impl Links {
	#[inline]
	fn init(&self, prev_offset: u32, next_offset: u32) {
		self.next_offset.store(next_offset, AtomicOrdering::Release);
		self.prev_offset.store(prev_offset, AtomicOrdering::Release);
	}
}

// ============================================================================
// Node Structure
// ============================================================================

/// Node struct
#[repr(C)]
struct Node {
	/// Offset to user_key bytes in arena
	key_offset: u32,
	/// Size of user_key in bytes
	key_size: u32,
	/// Trailer: (seq_num << 8) | kind
	key_trailer: u64,
	/// Timestamp: System time in nanoseconds
	key_timestamp: u64,
	/// Size of value in bytes
	value_size: u32,
	/// Padding for 8-byte alignment of tower
	_padding: u32,
	/// Variable-height tower (only first element in struct, rest via pointer math)
	tower: [Links; 1],
}

impl Node {
	/// Get key bytes
	#[inline]
	fn get_key_bytes<'a>(&self, arena: &'a Arena) -> &'a [u8] {
		arena.get_bytes(self.key_offset, self.key_size)
	}

	/// Get value bytes
	#[inline]
	fn get_value<'a>(&self, arena: &'a Arena) -> &'a [u8] {
		arena.get_bytes(self.key_offset + self.key_size, self.value_size)
	}

	/// Get next offset at height h
	#[inline]
	fn next_offset(&self, h: usize) -> u32 {
		unsafe {
			let links = (self.tower.as_ptr()).add(h);
			(*links).next_offset.load(AtomicOrdering::Acquire)
		}
	}

	/// Get prev offset at height h
	#[inline]
	fn prev_offset(&self, h: usize) -> u32 {
		unsafe {
			let links = (self.tower.as_ptr()).add(h);
			(*links).prev_offset.load(AtomicOrdering::Acquire)
		}
	}

	/// CAS next offset
	#[inline]
	fn cas_next_offset(&self, h: usize, old: u32, val: u32) -> bool {
		unsafe {
			let links = (self.tower.as_ptr()).add(h);
			(*links)
				.next_offset
				.compare_exchange(old, val, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
				.is_ok()
		}
	}

	/// CAS prev offset
	#[inline]
	fn cas_prev_offset(&self, h: usize, old: u32, val: u32) -> bool {
		unsafe {
			let links = (self.tower.as_ptr()).add(h);
			(*links)
				.prev_offset
				.compare_exchange(old, val, AtomicOrdering::AcqRel, AtomicOrdering::Acquire)
				.is_ok()
		}
	}

	/// Initialize tower links at height h
	#[inline]
	fn tower_init(&self, h: usize, prev_offset: u32, next_offset: u32) {
		unsafe {
			let links = (self.tower.as_ptr()).add(h);
			(*links).init(prev_offset, next_offset);
		}
	}
}

/// Allocate a new node
fn new_node(
	arena: &Arena,
	height: u32,
	key: &[u8],
	trailer: u64,
	timestamp: u64,
	value: &[u8],
) -> Option<*mut Node> {
	if height < 1 || height > MAX_HEIGHT as u32 {
		panic!("height cannot be less than one or greater than the max height");
	}

	let nd = new_raw_node(arena, height, key.len() as u32, value.len() as u32)?;

	unsafe {
		(*nd).key_trailer = trailer;
		(*nd).key_timestamp = timestamp;
		// Copy key bytes directly (no length prefix!)
		let key_bytes = arena.get_bytes_mut((*nd).key_offset, (*nd).key_size);
		key_bytes.copy_from_slice(key);
		// Copy value bytes contiguously after key
		let val_bytes = arena.get_bytes_mut((*nd).key_offset + (*nd).key_size, (*nd).value_size);
		val_bytes.copy_from_slice(value);
	}

	Some(nd)
}

/// Allocate raw node without data
fn new_raw_node(arena: &Arena, height: u32, key_size: u32, value_size: u32) -> Option<*mut Node> {
	// Compute unused tower size
	let unused_size = (MAX_HEIGHT - height as usize) * LINKS_SIZE;
	let node_size = MAX_NODE_SIZE - unused_size;

	let node_offset = arena.alloc(
		(node_size as u32) + key_size + value_size,
		8, // nodeAlignment
		unused_size as u32,
	)?;

	let nd = arena.get_pointer(node_offset) as *mut Node;
	unsafe {
		(*nd).key_offset = node_offset + node_size as u32;
		(*nd).key_size = key_size;
		(*nd).value_size = value_size;
	}

	Some(nd)
}

// ============================================================================
// Splice Structure
// ============================================================================

#[derive(Clone, Copy, Default)]
struct Splice {
	prev: *mut Node,
	next: *mut Node,
}

impl Splice {
	#[inline]
	fn init(&mut self, prev: *mut Node, next: *mut Node) {
		self.prev = prev;
		self.next = next;
	}
}

// ============================================================================
// Inserter
// ============================================================================

/// Inserter for batch optimization
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
// Skiplist
// ============================================================================

/// Skiplist struct
pub struct Skiplist {
	arena: Arc<Arena>,
	cmp: Compare,
	head: *mut Node,
	tail: *mut Node,
	height: AtomicU32,
}

unsafe impl Send for Skiplist {}
unsafe impl Sync for Skiplist {}

impl Skiplist {
	/// Constructor
	pub fn new(arena: Arc<Arena>, cmp: Compare) -> Self {
		// Allocate head sentinel (full height, no key/value)
		let head = new_raw_node(&arena, MAX_HEIGHT as u32, 0, 0)
			.expect("arenaSize is not large enough to hold the head node");
		unsafe {
			(*head).key_offset = 0;
		}

		// Allocate tail sentinel (full height, no key/value)
		let tail = new_raw_node(&arena, MAX_HEIGHT as u32, 0, 0)
			.expect("arenaSize is not large enough to hold the tail node");
		unsafe {
			(*tail).key_offset = 0;
		}

		// Link all head/tail levels together
		let head_offset = arena.get_pointer_offset(head as *const u8);
		let tail_offset = arena.get_pointer_offset(tail as *const u8);

		for i in 0..MAX_HEIGHT {
			unsafe {
				(*head).tower_init(i, 0, tail_offset);
				(*tail).tower_init(i, head_offset, 0);
			}
		}

		Self {
			arena,
			cmp,
			head,
			tail,
			height: AtomicU32::new(1),
		}
	}

	/// Height getter
	#[inline]
	pub fn height(&self) -> u32 {
		self.height.load(AtomicOrdering::Acquire)
	}

	/// Size getter
	#[inline]
	pub fn size(&self) -> u32 {
		self.arena.size() as u32
	}

	/// Add a key
	pub fn add(&self, key: &[u8], trailer: u64, timestamp: u64, value: &[u8]) -> Result<(), Error> {
		let mut ins = Inserter::new();
		self.add_internal(key, trailer, timestamp, value, &mut ins)
	}

	/// Internal add
	fn add_internal(
		&self,
		key: &[u8],
		trailer: u64,
		timestamp: u64,
		value: &[u8],
		ins: &mut Inserter,
	) -> Result<(), Error> {
		// Find splice
		if self.find_splice(key, trailer, timestamp, ins) {
			return Err(Error::RecordExists);
		}

		// Allocate node
		let (nd, height) = self.new_node(key, trailer, timestamp, value)?;
		let nd_offset = self.arena.get_pointer_offset(nd as *const u8);

		// Link at each level
		let mut invalidate_splice = false;

		for i in 0..height as usize {
			let mut prev = ins.spl[i].prev;
			let mut next = ins.spl[i].next;

			// Handle nil splice (new height level)
			if prev.is_null() {
				if !next.is_null() {
					panic!("next is expected to be nil, since prev is nil");
				}
				prev = self.head;
				next = self.tail;
			}

			// CAS linking loop
			loop {
				let prev_offset = self.arena.get_pointer_offset(prev as *const u8);
				let next_offset = self.arena.get_pointer_offset(next as *const u8);

				unsafe {
					(*nd).tower_init(i, prev_offset, next_offset);

					// Help concurrent inserter
					let next_prev_offset = (*next).prev_offset(i);
					if next_prev_offset != prev_offset {
						let prev_next_offset = (*prev).next_offset(i);
						if prev_next_offset == next_offset {
							(*next).cas_prev_offset(i, next_prev_offset, prev_offset);
						}
					}

					// CAS to insert
					if (*prev).cas_next_offset(i, next_offset, nd_offset) {
						(*next).cas_prev_offset(i, prev_offset, nd_offset);
						break;
					}
				}

				// CAS failed, recompute splice
				let (new_prev, new_next, found) =
					self.find_splice_for_level(key, trailer, timestamp, i, prev);
				if found {
					if i != 0 {
						panic!("how can another thread have inserted a node at a non-base level?");
					}
					return Err(Error::RecordExists);
				}
				prev = new_prev;
				next = new_next;
				invalidate_splice = true;
			}
		}

		// Update cached splice
		if invalidate_splice {
			ins.height = 0;
		} else {
			for i in 0..height as usize {
				ins.spl[i].prev = nd;
			}
		}

		Ok(())
	}

	/// Create new node with height CAS
	fn new_node(
		&self,
		key: &[u8],
		trailer: u64,
		timestamp: u64,
		value: &[u8],
	) -> Result<(*mut Node, u32), Error> {
		let height = self.random_height();
		let nd = new_node(&self.arena, height, key, trailer, timestamp, value)
			.ok_or(Error::ArenaFull)?;

		// Try to increase height via CAS
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

		Ok((nd, height))
	}

	/// Random height
	fn random_height(&self) -> u32 {
		let rnd: u32 = rand::rng().random();
		let mut h = 1u32;
		let probs = probabilities();
		while h < MAX_HEIGHT as u32 && rnd <= probs[h as usize] {
			h += 1;
		}
		h
	}

	/// Find splice
	fn find_splice(&self, key: &[u8], trailer: u64, timestamp: u64, ins: &mut Inserter) -> bool {
		let list_height = self.height();
		let mut level: i32;
		let mut prev = self.head;

		if ins.height < list_height {
			ins.height = list_height;
			level = ins.height as i32;
		} else {
			level = 0;
			for l in 0..list_height as usize {
				let spl = &ins.spl[l];
				if self.get_next(spl.prev, l) != spl.next {
					continue;
				}

				if (spl.prev != self.head
					&& !self.key_is_after_node(spl.prev, key, trailer, timestamp))
					|| (spl.next != self.tail
						&& self.key_is_after_node(spl.next, key, trailer, timestamp))
				{
					level = list_height as i32;
				} else {
					prev = spl.prev;
				}
				break;
			}
		}

		let mut found = false;
		let mut next: *mut Node = std::ptr::null_mut();

		for l in (0..level as usize).rev() {
			let prev_level_next = next;

			loop {
				next = self.get_next(prev, l);

				if next == prev_level_next {
					break;
				}
				if next == self.tail {
					break;
				}

				let next_key = unsafe { (*next).get_key_bytes(&self.arena) };
				let cmp = (self.cmp)(key, next_key);

				if cmp == Ordering::Less {
					break;
				}
				if cmp == Ordering::Equal {
					let next_trailer = unsafe { (*next).key_trailer };
					if trailer == next_trailer {
						// Trailer equal - check timestamp as tiebreaker
						let next_timestamp = unsafe { (*next).key_timestamp };
						if timestamp == next_timestamp {
							found = true;
							break;
						}
						// Higher timestamp comes first (DESC), so stop if our timestamp > next
						if timestamp > next_timestamp {
							break;
						}
						// timestamp < next_timestamp, continue searching
					} else if trailer > next_trailer {
						break;
					}
				}

				prev = next;
			}

			ins.spl[l].init(prev, next);
		}

		found
	}

	/// Find splice for single level
	fn find_splice_for_level(
		&self,
		key: &[u8],
		trailer: u64,
		timestamp: u64,
		level: usize,
		start: *mut Node,
	) -> (*mut Node, *mut Node, bool) {
		let mut prev = start;

		loop {
			let next = self.get_next(prev, level);

			if next == self.tail {
				return (prev, next, false);
			}

			let next_key = unsafe { (*next).get_key_bytes(&self.arena) };
			let cmp = (self.cmp)(key, next_key);

			if cmp == Ordering::Less {
				return (prev, next, false);
			}
			if cmp == Ordering::Equal {
				let next_trailer = unsafe { (*next).key_trailer };
				if trailer == next_trailer {
					// Trailer equal - check timestamp as tiebreaker
					let next_timestamp = unsafe { (*next).key_timestamp };
					if timestamp == next_timestamp {
						return (prev, next, true); // True duplicate
					}
					// Higher timestamp comes first (DESC), so stop if our timestamp > next
					if timestamp > next_timestamp {
						return (prev, next, false);
					}
					// timestamp < next_timestamp, continue searching
				} else if trailer > next_trailer {
					return (prev, next, false);
				}
			}

			prev = next;
		}
	}

	/// Key comparison
	#[inline]
	fn key_is_after_node(&self, nd: *mut Node, key: &[u8], trailer: u64, timestamp: u64) -> bool {
		if nd == self.head {
			return true;
		}
		if nd == self.tail {
			return false;
		}

		let nd_key = unsafe { (*nd).get_key_bytes(&self.arena) };
		let cmp = (self.cmp)(nd_key, key);

		match cmp {
			Ordering::Less => true,
			Ordering::Greater => false,
			Ordering::Equal => {
				let nd_trailer = unsafe { (*nd).key_trailer };
				if trailer == nd_trailer {
					// Trailer equal - use timestamp as tiebreaker (DESC order)
					let nd_timestamp = unsafe { (*nd).key_timestamp };
					if timestamp == nd_timestamp {
						false // Same key
					} else {
						// Higher timestamp comes first, so key is "after" node if timestamp <
						// nd_timestamp
						timestamp < nd_timestamp
					}
				} else {
					trailer < nd_trailer
				}
			}
		}
	}

	/// Get next node
	#[inline]
	fn get_next(&self, nd: *mut Node, h: usize) -> *mut Node {
		let offset = unsafe { (*nd).next_offset(h) };
		self.arena.get_pointer(offset) as *mut Node
	}

	/// Get prev node
	#[inline]
	fn get_prev(&self, nd: *mut Node, h: usize) -> *mut Node {
		let offset = unsafe { (*nd).prev_offset(h) };
		self.arena.get_pointer(offset) as *mut Node
	}

	/// Create iterator
	pub(crate) fn iter(&self) -> SkiplistIterator<'_> {
		self.new_iter(None, None)
	}

	/// Create iterator with bounds
	pub(crate) fn new_iter<'b>(
		&'b self,
		lower: Option<&[u8]>,
		upper: Option<&[u8]>,
	) -> SkiplistIterator<'b> {
		SkiplistIterator {
			list: self,
			nd: self.head,
			lower: lower.map(|s| s.to_vec()),
			upper: upper.map(|s| s.to_vec()),
			lower_node: std::ptr::null_mut(),
			upper_node: std::ptr::null_mut(),
			encoded_key_buf: Vec::new(),
		}
	}
}

// ============================================================================
// Iterator
// ============================================================================

/// Iterator
pub(crate) struct SkiplistIterator<'a> {
	list: &'a Skiplist,
	nd: *mut Node,
	lower: Option<Vec<u8>>,   // Inclusive lower bound
	upper: Option<Vec<u8>>,   // Exclusive upper bound
	lower_node: *mut Node,    // Cached node at lower bound
	upper_node: *mut Node,    // Cached node at upper bound
	encoded_key_buf: Vec<u8>, // Buffer for encoded key to return InternalKeyRef
}

impl<'a> SkiplistIterator<'a> {
	/// Check if iterator is valid
	#[inline]
	pub fn is_valid(&self) -> bool {
		self.nd != self.list.head
			&& self.nd != self.list.tail
			&& !self.nd.is_null()
			&& self.nd != self.lower_node
			&& self.nd != self.upper_node
	}

	/// Get current key (user_key bytes only)
	#[inline]
	pub fn key_bytes(&self) -> &[u8] {
		debug_assert!(self.is_valid());
		unsafe { (*self.nd).get_key_bytes(&self.list.arena) }
	}

	/// Get current trailer
	#[inline]
	pub fn trailer(&self) -> u64 {
		debug_assert!(self.is_valid());
		unsafe { (*self.nd).key_trailer }
	}

	/// Get current value
	#[inline]
	pub fn value_bytes(&self) -> &[u8] {
		debug_assert!(self.is_valid());
		unsafe { (*self.nd).get_value(&self.list.arena) }
	}

	/// Populate encoded key buffer for InternalKeyRef
	fn populate_encoded_key(&mut self) {
		if !self.is_valid() {
			return;
		}
		// Access node data directly to avoid borrow conflict
		let (key_bytes, trailer, timestamp) = unsafe {
			let node = &*self.nd;
			(node.get_key_bytes(&self.list.arena), node.key_trailer, node.key_timestamp)
		};
		self.encoded_key_buf.clear();
		self.encoded_key_buf.extend_from_slice(key_bytes);
		self.encoded_key_buf.extend_from_slice(&trailer.to_be_bytes());
		self.encoded_key_buf.extend_from_slice(&timestamp.to_be_bytes());
	}

	/// Move to first entry within bounds
	pub fn first(&mut self) {
		// If we have a lower bound, seek to it; otherwise start at the beginning
		if let Some(ref lower) = self.lower.clone() {
			self.seek_ge(lower);
		} else {
			self.nd = self.list.get_next(self.list.head, 0);
		}

		if self.nd == self.list.tail || self.nd == self.upper_node {
			return;
		}
		// Check upper bound
		if let Some(upper) = self.upper.as_deref() {
			if self.is_valid() {
				let key = self.key_bytes();
				if (self.list.cmp)(upper, key) <= Ordering::Equal {
					self.upper_node = self.nd;
					self.nd = self.list.tail;
				}
			}
		}
	}

	/// Move to last entry
	pub fn last(&mut self) {
		self.nd = self.list.get_prev(self.list.tail, 0);
		if self.nd == self.list.head || self.nd == self.lower_node {
			return;
		}
		// Check upper bound first - if entry is at or past upper, move backward
		if let Some(upper) = self.upper.as_deref() {
			while self.is_valid() {
				let key = self.key_bytes();
				if (self.list.cmp)(upper, key) == Ordering::Greater {
					// key < upper, so this entry is valid
					break;
				}
				// key >= upper, skip this entry
				self.nd = self.list.get_prev(self.nd, 0);
				if self.nd == self.list.head || self.nd == self.lower_node {
					return;
				}
			}
		}
		// Check lower bound
		if let Some(lower) = self.lower.as_deref() {
			if self.is_valid() {
				let key = self.key_bytes();
				if (self.list.cmp)(lower, key) == Ordering::Greater {
					self.lower_node = self.nd;
					self.nd = self.list.head;
				}
			}
		}
	}

	/// Advance to next entry (internal method)
	pub fn advance(&mut self) {
		debug_assert!(self.is_valid());
		self.nd = self.list.get_next(self.nd, 0);
		if self.nd == self.list.tail || self.nd == self.upper_node {
			return;
		}
		// Check upper bound
		if let Some(upper) = self.upper.as_deref() {
			if self.is_valid() {
				let key = self.key_bytes();
				if (self.list.cmp)(upper, key) <= Ordering::Equal {
					self.upper_node = self.nd;
					self.nd = self.list.tail;
				}
			}
		}
	}

	/// Move to prev entry (internal method)
	pub fn prev_internal(&mut self) {
		debug_assert!(self.is_valid());
		self.nd = self.list.get_prev(self.nd, 0);
		if self.nd == self.list.head || self.nd == self.lower_node {
			return;
		}
		// Check lower bound
		if let Some(lower) = self.lower.as_deref() {
			if self.is_valid() {
				let key = self.key_bytes();
				if (self.list.cmp)(lower, key) == Ordering::Greater {
					self.lower_node = self.nd;
					self.nd = self.list.head;
				}
			}
		}
	}

	/// Seek to first entry >= key
	pub fn seek_ge(&mut self, key: &[u8]) {
		let (_, next) = self.seek_for_base_splice(key);
		self.nd = next;
		if self.nd == self.list.tail || self.nd == self.upper_node {
			return;
		}
		// Check upper bound
		if let Some(upper) = self.upper.as_deref() {
			if self.is_valid() {
				let current_key = self.key_bytes();
				if (self.list.cmp)(upper, current_key) <= Ordering::Equal {
					self.upper_node = self.nd;
					self.nd = self.list.tail;
				}
			}
		}
	}

	/// Seek helper
	fn seek_for_base_splice(&self, key: &[u8]) -> (*mut Node, *mut Node) {
		let mut prev = self.list.head;
		let mut next: *mut Node = std::ptr::null_mut();

		for level in (0..self.list.height() as usize).rev() {
			let prev_level_next = next;

			loop {
				next = self.list.get_next(prev, level);

				if next == prev_level_next {
					break;
				}
				if next == self.list.tail {
					break;
				}

				let next_key = unsafe { (*next).get_key_bytes(&self.list.arena) };
				let cmp = (self.list.cmp)(key, next_key);

				if cmp <= Ordering::Equal {
					break;
				}

				prev = next;
			}
		}

		(prev, next)
	}
}

// ============================================================================
// LSMIterator Implementation
// ============================================================================

impl LSMIterator for SkiplistIterator<'_> {
	/// Seek to first entry >= target.
	/// Target is an encoded internal key, we extract user_key for comparison.
	fn seek(&mut self, target: &[u8]) -> CrateResult<bool> {
		let user_key = InternalKey::user_key_from_encoded(target);
		self.seek_ge(user_key);
		self.populate_encoded_key();
		Ok(self.is_valid())
	}

	/// Seek to first entry.
	fn seek_first(&mut self) -> CrateResult<bool> {
		self.first();
		self.populate_encoded_key();
		Ok(self.is_valid())
	}

	/// Seek to last entry.
	fn seek_last(&mut self) -> CrateResult<bool> {
		self.last();
		self.populate_encoded_key();
		Ok(self.is_valid())
	}

	/// Move to next entry.
	fn next(&mut self) -> CrateResult<bool> {
		if !self.is_valid() {
			return Ok(false);
		}
		self.advance();
		self.populate_encoded_key();
		Ok(self.is_valid())
	}

	/// Move to previous entry.
	fn prev(&mut self) -> CrateResult<bool> {
		if !self.is_valid() {
			return Ok(false);
		}
		self.prev_internal();
		self.populate_encoded_key();
		Ok(self.is_valid())
	}

	/// Check if positioned on valid entry.
	fn valid(&self) -> bool {
		self.is_valid()
	}

	/// Get current key as zero-copy reference.
	fn key(&self) -> InternalKeyRef<'_> {
		debug_assert!(self.is_valid());
		InternalKeyRef::from_encoded(&self.encoded_key_buf)
	}

	/// Get current value as zero-copy reference.
	fn value_encoded(&self) -> CrateResult<&[u8]> {
		debug_assert!(self.is_valid());
		Ok(self.value_bytes())
	}
}
