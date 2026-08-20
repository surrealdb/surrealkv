//! Proof tests for the compaction/write hot-loop allocation hygiene branch.
//!
//! Protocol: each proof is committed FIRST, asserting the CURRENT wasteful
//! behavior with measured numbers; the fix flips the assertion in the same
//! commit, turning the proof into a permanent regression guard.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;

use tempfile::TempDir;

use crate::sstable::table::TableWriter;
use crate::vlog::ValueLocation;
use crate::{InternalKey, InternalKeyKind, Options};

// Per-thread CUMULATIVE allocation counter (never decremented) — measures
// transient allocation churn that live-byte accounting cannot see.
thread_local! {
	static TOTAL_ALLOCATED: Cell<u64> = const { Cell::new(0) };
}

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		let ptr = System.alloc(layout);
		if !ptr.is_null() {
			let _ = TOTAL_ALLOCATED.try_with(|c| c.set(c.get() + layout.size() as u64));
		}
		ptr
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		System.dealloc(ptr, layout);
	}
}

#[global_allocator]
static COUNTING_ALLOCATOR: CountingAllocator = CountingAllocator;

fn total_allocated() -> u64 {
	TOTAL_ALLOCATED.with(|c| c.get())
}

/// P5: `TableWriter::add` must not copy the value just to inspect the
/// value-pointer meta bit. Pre-fix, `ValueLocation::decode` read_to_end-
/// copied every value per entry: one 1 MB add() allocated 4,260,134 bytes;
/// with the zero-copy peek it allocates ~3.2 MB (block-buffer append and
/// flush machinery — legitimate, though further reducible). The bound
/// below sits between the two so reintroducing a per-entry value copy
/// fails the test.
#[test]
fn proof_p5_add_copies_every_value_to_peek_pointer_bit() {
	const VALUE_SIZE: usize = 1024 * 1024;
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("p5.sst");
	let opts = Arc::new(Options::new());

	let value = ValueLocation::with_inline_value(vec![0x5Au8; VALUE_SIZE]).encode();
	let file = std::fs::File::create(&path).unwrap();
	let mut writer = TableWriter::new(file, 1, Arc::clone(&opts), 1);

	// Warm up: first add pays one-time block-buffer growth.
	let key0 = InternalKey::new(b"key-a".to_vec(), 1, InternalKeyKind::Set, 0);
	writer.add(key0, &value).unwrap();

	let before = total_allocated();
	let key1 = InternalKey::new(b"key-b".to_vec(), 2, InternalKeyKind::Set, 0);
	writer.add(key1, &value).unwrap();
	let allocated = total_allocated() - before;

	eprintln!("P5: one add() of a {VALUE_SIZE}-byte value allocated {allocated} bytes");

	// Post-fix bound: measured ~3.2 MB (block machinery) vs 4,260,134 B with
	// the per-entry decode copy. 3.5 MB catches any reintroduced full-value
	// copy while allowing block-buffer variance.
	assert!(
		allocated <= 3_500_000,
		"add() allocated {allocated} B for a 1 MB value — a per-entry value copy appears to be \
		 back"
	);
}
