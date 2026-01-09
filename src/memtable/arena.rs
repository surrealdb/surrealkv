use std::sync::atomic::{AtomicU64, Ordering};

const NODE_ALIGNMENT: usize = std::mem::align_of::<usize>(); // 8 on 64-bit

/// Lock-free arena allocator (Pebble/arenaskl style)
///
/// Uses a simple atomic bump pointer instead of per-CPU shards.
/// Much simpler than the RocksDB approach, truly lock-free.
pub struct Arena {
	/// Current allocation offset (atomically incremented)
	n: AtomicU64,
	/// Pre-allocated buffer
	buf: Box<[u8]>,
}

// Safety: Arena uses atomic operations for all mutations
unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Arena {
	/// Create a new arena with the given capacity.
	///
	/// The capacity should be large enough for all expected allocations,
	/// as the arena cannot grow once created.
	pub fn new(capacity: usize) -> Self {
		let capacity = capacity.min(u32::MAX as usize);
		let buf = vec![0u8; capacity].into_boxed_slice();

		Self {
			// Start at 1 to reserve offset 0 as "null"
			n: AtomicU64::new(1),
			buf,
		}
	}

	/// Allocate `size` bytes with the given alignment.
	///
	/// This is lock-free - uses only atomic operations.
	/// Returns None if the arena is full.
	fn alloc_inner(&self, size: usize, alignment: usize) -> Option<u32> {
		debug_assert!(alignment.is_power_of_two());

		// Pad allocation to ensure alignment
		let padded = size as u64 + alignment as u64 - 1;

		// Atomically bump the pointer
		let new_size = self.n.fetch_add(padded, Ordering::Relaxed) + padded;

		if new_size > self.buf.len() as u64 {
			return None; // Arena full
		}

		// Calculate aligned offset
		let offset = ((new_size as u32) - size as u32) & !(alignment as u32 - 1);
		Some(offset)
	}

	/// Allocate aligned memory, returning a pointer.
	///
	/// Panics if the arena is full. For fallible allocation, use `try_allocate`.
	pub fn allocate(&self, size: usize) -> *mut u8 {
		self.try_allocate(size).expect("Arena: allocation failed - arena is full")
	}

	/// Try to allocate aligned memory, returning None if full.
	pub fn try_allocate(&self, size: usize) -> Option<*mut u8> {
		let alignment = NODE_ALIGNMENT.max(std::mem::align_of::<usize>());
		let offset = self.alloc_inner(size, alignment)?;
		Some(self.get_pointer(offset))
	}

	/// Convert offset to pointer.
	#[inline]
	fn get_pointer(&self, offset: u32) -> *mut u8 {
		if offset == 0 {
			std::ptr::null_mut()
		} else {
			unsafe { self.buf.as_ptr().add(offset as usize) as *mut u8 }
		}
	}

	pub fn size(&self) -> usize {
		let s = self.n.load(Ordering::Relaxed);
		s.min(self.buf.len() as u64) as usize
	}

	pub fn capacity(&self) -> usize {
		self.buf.len()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_basic_allocation() {
		let arena = Arena::new(4096);

		let p1 = arena.allocate(100);
		let p2 = arena.allocate(200);
		let p3 = arena.allocate(300);

		assert!(!p1.is_null());
		assert!(!p2.is_null());
		assert!(!p3.is_null());

		// Check they don't overlap
		assert!(p2 as usize >= p1 as usize + 100);
		assert!(p3 as usize >= p2 as usize + 200);
	}

	#[test]
	fn test_alignment() {
		let arena = Arena::new(4096);

		for _ in 0..100 {
			let ptr = arena.allocate(17); // Odd size
			assert_eq!(ptr as usize % 4, 0); // Should be 4-byte aligned
		}
	}

	#[test]
	fn test_arena_full() {
		let arena = Arena::new(100);

		// This should succeed
		assert!(arena.try_allocate(50).is_some());

		// This should fail (not enough space)
		assert!(arena.try_allocate(100).is_none());
	}

	#[test]
	fn test_concurrent_allocation() {
		use std::sync::Arc;
		use std::thread;

		let arena = Arc::new(Arena::new(1024 * 1024)); // 1MB
		let mut handles = vec![];

		for _ in 0..8 {
			let arena = Arc::clone(&arena);
			handles.push(thread::spawn(move || {
				for _ in 0..1000 {
					let ptr = arena.allocate(64);
					assert!(!ptr.is_null());
					// Write to verify we got valid memory
					unsafe {
						std::ptr::write_bytes(ptr, 0xAB, 64);
					}
				}
			}));
		}

		for h in handles {
			h.join().unwrap();
		}
	}
}
