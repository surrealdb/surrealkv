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

use std::sync::atomic::{AtomicU64, Ordering};

/// Maximum arena size (u32::MAX to fit in offset)
pub(crate) const MAX_ARENA_SIZE: usize = u32::MAX as usize;

pub(crate) struct Arena {
	/// Current allocation offset (atomically incremented)
	n: AtomicU64,
	/// Pre-allocated buffer
	pub(crate) buf: Box<[u8]>,
}

// Safety: Arena uses atomic operations for all mutations
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Arena {
	pub(crate) fn new(capacity: usize) -> Self {
		let capacity = capacity.min(MAX_ARENA_SIZE);
		let buf = vec![0u8; capacity].into_boxed_slice();

		Self {
			// Start at 1 to reserve offset 0 as "null"
			n: AtomicU64::new(1),
			buf,
		}
	}

	/// Returns the number of bytes allocated.
	pub(crate) fn size(&self) -> usize {
		let s = self.n.load(Ordering::Relaxed);
		if s > self.buf.len() as u64 {
			// Saturate at capacity if last allocation failed
			self.buf.len()
		} else {
			s as usize
		}
	}

	/// Allocate `size` bytes with given alignment.
	///
	/// `overflow` ensures that many extra bytes after the buffer are inside
	/// the arena (used for truncated node structures).
	///
	/// Returns the offset, or None if arena is full.
	pub(crate) fn alloc(&self, size: u32, alignment: u32, overflow: u32) -> Option<u32> {
		debug_assert!(alignment.is_power_of_two());

		// Check if already full
		let orig_size = self.n.load(Ordering::Relaxed);
		if orig_size > self.buf.len() as u64 {
			return None;
		}

		// Pad allocation to ensure alignment
		let padded = size as u64 + alignment as u64 - 1;

		let new_size = self.n.fetch_add(padded, Ordering::Relaxed) + padded;
		if new_size + overflow as u64 > self.buf.len() as u64 {
			return None; // Arena full
		}

		// Calculate aligned offset
		let offset = (new_size as u32 - size) & !(alignment - 1);
		Some(offset)
	}

	/// Get a byte slice from the arena by offset.
	#[inline]
	pub(crate) fn get_bytes(&self, offset: u32, size: u32) -> &[u8] {
		if offset == 0 {
			return &[];
		}
		&self.buf[offset as usize..(offset + size) as usize]
	}

	/// Get a mutable byte slice from the arena by offset.
	///
	/// # Safety
	/// Caller must ensure no other references to this region exist.
	#[allow(clippy::mut_from_ref)]
	#[inline]
	pub(crate) unsafe fn get_bytes_mut(&self, offset: u32, size: u32) -> &mut [u8] {
		if offset == 0 {
			return &mut [];
		}
		let ptr = self.buf.as_ptr().add(offset as usize) as *mut u8;
		std::slice::from_raw_parts_mut(ptr, size as usize)
	}

	/// Convert offset to raw pointer.
	#[inline]
	pub(crate) fn get_pointer(&self, offset: u32) -> *mut u8 {
		if offset == 0 {
			std::ptr::null_mut()
		} else {
			unsafe { self.buf.as_ptr().add(offset as usize) as *mut u8 }
		}
	}

	/// Convert raw pointer back to offset.
	#[inline]
	pub(crate) fn get_pointer_offset(&self, ptr: *const u8) -> u32 {
		if ptr.is_null() {
			0
		} else {
			(ptr as usize - self.buf.as_ptr() as usize) as u32
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_basic_allocation() {
		let arena = Arena::new(4096);

		let o1 = arena.alloc(100, 8, 0).unwrap();
		let o2 = arena.alloc(200, 8, 0).unwrap();
		let o3 = arena.alloc(300, 8, 0).unwrap();

		assert!(o1 > 0);
		assert!(o2 > o1);
		assert!(o3 > o2);
	}

	#[test]
	fn test_get_bytes() {
		let arena = Arena::new(4096);
		let offset = arena.alloc(10, 8, 0).unwrap();

		unsafe {
			let slice = arena.get_bytes_mut(offset, 10);
			slice.copy_from_slice(b"0123456789");
		}

		let read = arena.get_bytes(offset, 10);
		assert_eq!(read, b"0123456789");
	}

	#[test]
	fn test_pointer_offset_roundtrip() {
		let arena = Arena::new(4096);
		let offset = arena.alloc(64, 8, 0).unwrap();
		let ptr = arena.get_pointer(offset);
		let back = arena.get_pointer_offset(ptr);
		assert_eq!(offset, back);
	}

	#[test]
	fn test_arena_full() {
		let arena = Arena::new(100);
		assert!(arena.alloc(50, 8, 0).is_some());
		assert!(arena.alloc(100, 8, 0).is_none());
	}

	#[test]
	fn test_concurrent_allocation() {
		use std::sync::Arc;
		use std::thread;

		let arena = Arc::new(Arena::new(1024 * 1024));
		let mut handles = vec![];

		for _ in 0..8 {
			let arena = Arc::clone(&arena);
			handles.push(thread::spawn(move || {
				for _ in 0..1000 {
					let offset = arena.alloc(64, 8, 0).unwrap();
					assert!(offset > 0);
					unsafe {
						let slice = arena.get_bytes_mut(offset, 64);
						std::ptr::write_bytes(slice.as_mut_ptr(), 0xAB, 64);
					}
				}
			}));
		}

		for h in handles {
			h.join().unwrap();
		}
	}
}
