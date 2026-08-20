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

/// P5: `TableWriter::add` calls `ValueLocation::decode` on every entry just
/// to test the value-pointer meta bit; `decode_from` builds a full copy of
/// the value via `read_to_end` (with geometric regrowth). With vlog disabled
/// — the default — every value byte streamed through flush/compaction is
/// copied for nothing.
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

	// BUG: the decode copy alone accounts for ~2x the value size
	// (read_to_end regrowth) on top of the block-buffer append the writer
	// legitimately needs. Post-fix this must drop below ~3.5 MB.
	assert!(
		allocated >= (VALUE_SIZE * 2) as u64,
		"expected the wasteful decode copy (>= 2x value size), got {allocated} — if this fails \
		 the fix landed and this proof must flip to an upper bound"
	);
}
