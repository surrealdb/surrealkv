//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2  and Apache 2.0 License.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Arena is an implementation of Allocator class. For a request of small size,
// it allocates a block with pre-defined block size. For a request of big
// size, it uses malloc to directly get the requested size.

use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};

pub(crate) const K_MIN_BLOCK_SIZE: usize = 4096;
const K_MAX_BLOCK_SIZE: usize = 2 << 30;
const K_ALIGN_UNIT: usize = std::mem::align_of::<usize>();

// ConcurrentArena - RocksDB-style thread-safe memory allocator
//
// Design:
// - Per-CPU shards to minimize contention
// - Each shard has its own spinlock and memory region
// - Large allocations bypass shards and go directly to main arena
// - Shards refill from main arena when exhausted
// - Lazy shard instantiation to avoid memory waste

use std::alloc::{alloc, dealloc, Layout};
use std::cell::UnsafeCell;
use std::ptr::null_mut;

// ============================================================================
// Constants
// ============================================================================

const K_MAX_SHARD_BLOCK_SIZE: usize = 128 * 1024; // 128KB per shard max

// ============================================================================
// SpinLock - Lightweight spinning lock
// ============================================================================

pub struct SpinLock {
	locked: AtomicBool,
}

impl SpinLock {
	pub const fn new() -> Self {
		Self {
			locked: AtomicBool::new(false),
		}
	}

	#[inline]
	pub fn lock(&self) {
		while self
			.locked
			.compare_exchange_weak(false, true, Ordering::Acquire, Ordering::Relaxed)
			.is_err()
		{
			// Spin with backoff
			while self.locked.load(Ordering::Relaxed) {
				std::hint::spin_loop();
			}
		}
	}

	#[inline]
	pub fn try_lock(&self) -> bool {
		self.locked.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok()
	}

	#[inline]
	pub fn unlock(&self) {
		self.locked.store(false, Ordering::Release);
	}
}

pub struct SpinLockGuard<'a> {
	lock: &'a SpinLock,
}

impl<'a> SpinLockGuard<'a> {
	pub fn new(lock: &'a SpinLock) -> Self {
		lock.lock();
		Self {
			lock,
		}
	}
}

impl<'a> Drop for SpinLockGuard<'a> {
	fn drop(&mut self) {
		self.lock.unlock();
	}
}

/// Returns the current CPU ID (or approximation)
#[inline]
fn get_cpu_id() -> usize {
	// Thread-local cached CPU ID
	thread_local! {
		static CPU_ID: std::cell::Cell<usize> = const { std::cell::Cell::new(usize::MAX) };
	}

	CPU_ID.with(|id| {
		let cached = id.get();
		if cached == usize::MAX {
			// First access, get actual CPU
			let cpu = get_current_cpu();
			id.set(cpu);
			cpu
		} else {
			cached
		}
	})
}

#[cfg(target_os = "linux")]
fn get_current_cpu() -> usize {
	// Use sched_getcpu on Linux
	unsafe {
		let cpu = libc::sched_getcpu();
		if cpu >= 0 {
			cpu as usize
		} else {
			0
		}
	}
}

#[cfg(not(target_os = "linux"))]
fn get_current_cpu() -> usize {
	// Fallback: use thread ID hash
	use std::hash::{Hash, Hasher};
	let mut hasher = std::collections::hash_map::DefaultHasher::new();
	std::thread::current().id().hash(&mut hasher);
	hasher.finish() as usize
}

// ============================================================================
// Shard - Per-CPU allocation cache
// ============================================================================

#[repr(C, align(64))]
struct Shard {
	mutex: SpinLock,
	free_begin: AtomicPtr<u8>,
	free_end: AtomicPtr<u8>,
	allocated_and_unused: AtomicUsize,
	_padding: [u8; 32], // Pad to exactly 64 bytes (cache line size)
}

impl Shard {
	fn new() -> Self {
		Self {
			mutex: SpinLock::new(),
			free_begin: AtomicPtr::new(null_mut()),
			free_end: AtomicPtr::new(null_mut()),
			allocated_and_unused: AtomicUsize::new(0),
			_padding: [0; 32],
		}
	}
}

// ============================================================================
// Arena - Basic leveldb type arena
// ============================================================================

struct Arena {
	blocks: Vec<(*mut u8, Layout)>,
	aligned_alloc_ptr: *mut u8,
	unaligned_alloc_ptr: *mut u8,
	alloc_bytes_remaining: usize,
	blocks_memory: usize,
	irregular_block_num: usize,
	block_size: usize,
}

impl Arena {
	fn new(block_size: usize) -> Self {
		let block_size = Self::optimize_block_size(block_size);

		Self {
			blocks: Vec::new(),
			aligned_alloc_ptr: null_mut(),
			unaligned_alloc_ptr: null_mut(),
			alloc_bytes_remaining: 0,
			blocks_memory: 0,
			irregular_block_num: 0,
			block_size,
		}
	}

	fn optimize_block_size(size: usize) -> usize {
		let size = size.clamp(K_MIN_BLOCK_SIZE, K_MAX_BLOCK_SIZE);
		// Round up to multiple of K_ALIGN_UNIT
		(size + K_ALIGN_UNIT - 1) & !(K_ALIGN_UNIT - 1)
	}

	fn allocated_and_unused(&self) -> usize {
		self.alloc_bytes_remaining
	}

	fn memory_allocated_bytes(&self) -> usize {
		self.blocks_memory
	}

	fn irregular_block_num(&self) -> usize {
		self.irregular_block_num
	}

	fn allocate_aligned(&mut self, bytes: usize) -> *mut u8 {
		let current_mod = (self.aligned_alloc_ptr as usize) & (K_ALIGN_UNIT - 1);
		let slop = if current_mod == 0 {
			0
		} else {
			K_ALIGN_UNIT - current_mod
		};
		let needed = bytes + slop;

		if needed <= self.alloc_bytes_remaining {
			let result = unsafe { self.aligned_alloc_ptr.add(slop) };
			self.aligned_alloc_ptr = unsafe { self.aligned_alloc_ptr.add(needed) };
			self.alloc_bytes_remaining -= needed;
			debug_assert!((result as usize) & (K_ALIGN_UNIT - 1) == 0);
			result
		} else {
			self.allocate_fallback(bytes, true)
		}
	}

	fn allocate_fallback(&mut self, bytes: usize, aligned: bool) -> *mut u8 {
		if bytes > self.block_size / 4 {
			// Large allocation - allocate separately
			self.irregular_block_num += 1;
			return self.allocate_new_block(bytes);
		}

		// Allocate new regular block
		let size = self.block_size;
		let block_head = self.allocate_new_block(size);
		self.alloc_bytes_remaining = size - bytes;

		if aligned {
			self.aligned_alloc_ptr = unsafe { block_head.add(bytes) };
			self.unaligned_alloc_ptr = unsafe { block_head.add(size) };
			block_head
		} else {
			self.aligned_alloc_ptr = block_head;
			self.unaligned_alloc_ptr = unsafe { block_head.add(size - bytes) };
			self.unaligned_alloc_ptr
		}
	}

	fn allocate_new_block(&mut self, block_bytes: usize) -> *mut u8 {
		let layout = Layout::from_size_align(block_bytes, K_ALIGN_UNIT).unwrap();
		let block = unsafe { alloc(layout) };
		if block.is_null() {
			panic!("Arena: allocation failed for {} bytes", block_bytes);
		}
		self.blocks.push((block, layout));
		self.blocks_memory += block_bytes;
		block
	}
}

impl Drop for Arena {
	fn drop(&mut self) {
		for (ptr, layout) in self.blocks.drain(..) {
			unsafe { dealloc(ptr, layout) };
		}
	}
}

// ============================================================================
// ConcurrentArena - Thread-safe arena with per-CPU shards
// ============================================================================

pub struct ConcurrentArena {
	// Per-CPU shards
	shards: Box<[Shard]>,
	num_shards: usize,
	shard_block_size: usize,

	// Main arena (protected by arena_mutex)
	arena: UnsafeCell<Arena>,
	arena_mutex: SpinLock,

	// Cached stats (atomic for lock-free reads)
	arena_allocated_and_unused: AtomicUsize,
	memory_allocated_bytes: AtomicUsize,
	irregular_block_num: AtomicUsize,
}

// Safety: ConcurrentArena uses proper synchronization
unsafe impl Send for ConcurrentArena {}
unsafe impl Sync for ConcurrentArena {}

impl ConcurrentArena {
	pub fn new(block_size: usize) -> Self {
		let block_size = Arena::optimize_block_size(block_size);

		// Determine number of shards based on available CPUs
		let num_cpus = std::thread::available_parallelism().map(|p| p.get()).unwrap_or(4);
		// Round up to power of 2 for efficient modulo
		let num_shards = num_cpus.next_power_of_two();

		// Shard block size: min of 1/8 block size and 128KB max
		let shard_block_size = (block_size / 8).min(K_MAX_SHARD_BLOCK_SIZE);

		// Create shards
		let shards: Vec<Shard> = (0..num_shards).map(|_| Shard::new()).collect();

		let arena = Arena::new(block_size);
		let arena_allocated_and_unused = arena.allocated_and_unused();
		let memory_allocated_bytes = arena.memory_allocated_bytes();
		let irregular_block_num = arena.irregular_block_num();

		Self {
			shards: shards.into_boxed_slice(),
			num_shards,
			shard_block_size,
			arena: UnsafeCell::new(arena),
			arena_mutex: SpinLock::new(),
			arena_allocated_and_unused: AtomicUsize::new(arena_allocated_and_unused),
			memory_allocated_bytes: AtomicUsize::new(memory_allocated_bytes),
			irregular_block_num: AtomicUsize::new(irregular_block_num),
		}
	}

	/// Allocate aligned memory (main entry point)
	pub fn allocate(&self, bytes: usize) -> *mut u8 {
		assert!(bytes > 0);

		// Round up to pointer alignment
		let rounded_up = (bytes + K_ALIGN_UNIT - 1) & !(K_ALIGN_UNIT - 1);

		self.allocate_impl(rounded_up)
	}

	/// Core allocation implementation
	fn allocate_impl(&self, bytes: usize) -> *mut u8 {
		// Large allocations go directly to arena
		if bytes > self.shard_block_size / 4 {
			return self.allocate_from_arena(bytes);
		}

		// Try to allocate from per-CPU shard
		let cpu = get_cpu_id();
		let shard_idx = cpu & (self.num_shards - 1);
		let shard = &self.shards[shard_idx];

		// Try to acquire shard lock without blocking
		if !shard.mutex.try_lock() {
			// Contention detected - try another shard or go to arena
			return self.allocate_from_arena_or_other_shard(bytes, shard_idx);
		}

		// We have the shard lock
		let result = self.allocate_from_shard(shard, bytes);
		shard.mutex.unlock();
		result
	}

	/// Allocate from a specific shard (caller must hold shard lock)
	///
	/// # Safety Requirements
	/// The caller MUST hold the shard's spinlock before calling this function.
	/// This ensures exclusive access to the shard's allocation state.
	///
	/// # Memory Ordering
	/// Uses Relaxed ordering for shard state because:
	/// 1. The spinlock provides synchronization between threads
	/// 2. Within a single thread, program order is preserved
	/// 3. The spinlock's Release on unlock ensures visibility to next acquirer
	fn allocate_from_shard(&self, shard: &Shard, bytes: usize) -> *mut u8 {
		let avail = shard.allocated_and_unused.load(Ordering::Relaxed);

		if avail >= bytes {
			// Fast path: allocate from shard's free region
			let begin = shard.free_begin.load(Ordering::Relaxed);
			let result = begin;
			shard.free_begin.store(unsafe { begin.add(bytes) }, Ordering::Relaxed);
			shard.allocated_and_unused.store(avail - bytes, Ordering::Relaxed);
			return result;
		}

		// Slow path: refill shard from main arena
		self.refill_shard(shard, bytes)
	}

	/// Refill a shard from the main arena.
	///
	/// # Safety Requirements
	/// The caller MUST hold the shard's spinlock before calling this function.
	///
	/// # Concurrency Design
	/// This function acquires the arena mutex to allocate a new block, then updates
	/// the shard's state. The ordering is critical:
	///
	/// 1. Acquire arena mutex (synchronizes with other arena allocations)
	/// 2. Allocate block from arena
	/// 3. Update shard pointers (while still holding shard lock)
	/// 4. Release arena mutex (but shard lock still held by caller)
	///
	/// The shard lock held by caller ensures no other thread can read shard state
	/// until the caller releases it, at which point the Release ordering on unlock
	/// makes all our writes visible.
	///
	/// # Memory Ordering
	/// Shard stores use Relaxed ordering because the shard spinlock provides
	/// synchronization. The spinlock's Acquire on lock and Release on unlock
	/// establish the necessary happens-before relationships.
	fn refill_shard(&self, shard: &Shard, bytes: usize) -> *mut u8 {
		let _arena_guard = SpinLockGuard::new(&self.arena_mutex);
		let arena = unsafe { &mut *self.arena.get() };

		// Determine refill size based on current arena state
		let exact = arena.allocated_and_unused();
		let refill_size = if exact >= self.shard_block_size / 2 && exact < self.shard_block_size * 2
		{
			exact
		} else {
			self.shard_block_size
		};

		// Get new block from arena
		let new_block = arena.allocate_aligned(refill_size);
		self.fixup(arena);

		// Update shard state atomically.
		// SAFETY: Caller holds shard lock, so no concurrent access to shard state.
		// The stores use Relaxed ordering because the shard spinlock provides
		// synchronization - its Release on unlock will make these writes visible
		// to any thread that subsequently acquires the lock.
		shard.free_begin.store(unsafe { new_block.add(bytes) }, Ordering::Relaxed);
		shard.free_end.store(unsafe { new_block.add(refill_size) }, Ordering::Relaxed);
		shard.allocated_and_unused.store(refill_size - bytes, Ordering::Relaxed);

		// Arena mutex released here (via _arena_guard drop).
		// Shard lock still held by caller - will be released after this returns.

		new_block
	}

	/// Allocate directly from main arena
	fn allocate_from_arena(&self, bytes: usize) -> *mut u8 {
		let _guard = SpinLockGuard::new(&self.arena_mutex);
		let arena = unsafe { &mut *self.arena.get() };
		let result = arena.allocate_aligned(bytes);
		self.fixup(arena);
		result
	}

	/// Try other shards or fall back to arena
	fn allocate_from_arena_or_other_shard(&self, bytes: usize, original_shard: usize) -> *mut u8 {
		// Try a few other shards
		for offset in 1..4 {
			let shard_idx = (original_shard + offset) & (self.num_shards - 1);
			let shard = &self.shards[shard_idx];

			if shard.mutex.try_lock() {
				let result = self.allocate_from_shard(shard, bytes);
				shard.mutex.unlock();
				return result;
			}
		}

		// All shards busy, go to arena
		self.allocate_from_arena(bytes)
	}

	/// Update cached stats from arena
	fn fixup(&self, arena: &Arena) {
		self.arena_allocated_and_unused.store(arena.allocated_and_unused(), Ordering::Relaxed);
		self.memory_allocated_bytes.store(arena.memory_allocated_bytes(), Ordering::Relaxed);
		self.irregular_block_num.store(arena.irregular_block_num(), Ordering::Relaxed);
	}
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_basic_allocation() {
		let arena = ConcurrentArena::new(K_MIN_BLOCK_SIZE);

		let p1 = arena.allocate(100);
		let p2 = arena.allocate(200);
		let p3 = arena.allocate(300);

		assert!(!p1.is_null());
		assert!(!p2.is_null());
		assert!(!p3.is_null());

		// Check alignment
		assert_eq!((p1 as usize) % K_ALIGN_UNIT, 0);
		assert_eq!((p2 as usize) % K_ALIGN_UNIT, 0);
		assert_eq!((p3 as usize) % K_ALIGN_UNIT, 0);
	}

	#[test]
	fn test_large_allocation() {
		let arena = ConcurrentArena::new(K_MIN_BLOCK_SIZE);

		// Allocate larger than shard_block_size / 4
		let large_size = K_MAX_SHARD_BLOCK_SIZE;
		let ptr = arena.allocate(large_size);

		assert!(!ptr.is_null());
		assert_eq!((ptr as usize) % K_ALIGN_UNIT, 0);
	}
}
