//! Proof tests for issue #397: level compaction needs RAM comparable to the
//! merged level size, and an interrupted compaction self-sustains an OOM
//! restart loop.
//!
//! Protocol: each proof is committed FIRST, asserting the CURRENT broken
//! behavior with measured numbers; the fix flips the assertion to the
//! post-fix expectation in the same commit as the fix, turning the proof
//! into a permanent regression guard.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::HashSet;
use std::fs::File as SysFile;
use std::io::SeekFrom;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use tempfile::TempDir;

use crate::compaction::compactor::{CompactionOptions, Compactor};
use crate::compaction::leveled::Strategy;
use crate::error::BackgroundErrorHandler;
use crate::levels::{write_manifest_to_disk, LevelManifest, Levels, MANIFEST_FORMAT_VERSION_V1};
use crate::memtable::ImmutableMemtables;
use crate::snapshot::SnapshotTracker;
use crate::sstable::table::{Table, TableWriter};
use crate::vfs::File;
use crate::vlog::ValueLocation;
use crate::{InternalKey, InternalKeyKind, LSMIterator, Options, INTERNAL_KEY_SEQ_NUM_MAX};

// --- Thread-local counting allocator ---------------------------------------
//
// Tracks live heap bytes per thread so a test can measure the residency of
// exactly the allocations it performs, independent of tests on other
// threads. Attribution is by allocating/freeing thread, which is exact for
// the single-threaded measurements below.

thread_local! {
	static LIVE_BYTES: Cell<i64> = const { Cell::new(0) };
	static HIGH_WATER: Cell<i64> = const { Cell::new(0) };
}

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
	unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
		let ptr = System.alloc(layout);
		if !ptr.is_null() {
			let _ = LIVE_BYTES.try_with(|c| {
				let live = c.get() + layout.size() as i64;
				c.set(live);
				let _ = HIGH_WATER.try_with(|h| {
					if live > h.get() {
						h.set(live);
					}
				});
			});
		}
		ptr
	}

	unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
		System.dealloc(ptr, layout);
		let _ = LIVE_BYTES.try_with(|c| c.set(c.get() - layout.size() as i64));
	}
}

#[global_allocator]
static COUNTING_ALLOCATOR: CountingAllocator = CountingAllocator;

fn live_bytes() -> i64 {
	LIVE_BYTES.with(|c| c.get())
}

/// Resets the live-heap high-water mark to the current live level;
/// `high_water_since` reads the peak growth since the last reset.
fn reset_high_water() -> i64 {
	let live = live_bytes();
	HIGH_WATER.with(|h| h.set(live));
	live
}

fn high_water_since(baseline: i64) -> i64 {
	HIGH_WATER.with(|h| h.get()) - baseline
}

// --- Read-counting file wrapper ----------------------------------------------

/// A `vfs::File` over an in-memory buffer that counts `read_at` calls, for
/// proving how many disk reads a lookup path performs.
struct CountingFile {
	inner: Vec<u8>,
	reads: Arc<AtomicU64>,
}

impl File for CountingFile {
	fn write(&mut self, buf: &[u8]) -> crate::Result<usize> {
		File::write(&mut self.inner, buf)
	}
	fn flush(&mut self) -> crate::Result<()> {
		File::flush(&mut self.inner)
	}
	fn close(&mut self) -> crate::Result<()> {
		File::close(&mut self.inner)
	}
	fn seek(&mut self, pos: SeekFrom) -> crate::Result<u64> {
		File::seek(&mut self.inner, pos)
	}
	fn read(&mut self, buf: &mut [u8]) -> crate::Result<usize> {
		File::read(&mut self.inner, buf)
	}
	fn read_all(&mut self, buf: &mut Vec<u8>) -> crate::Result<usize> {
		File::read_all(&mut self.inner, buf)
	}
	fn lock(&self) -> crate::Result<()> {
		File::lock(&self.inner)
	}
	fn unlock(&self) -> crate::Result<()> {
		File::unlock(&self.inner)
	}
	fn read_at(&self, offset: u64, buf: &mut [u8]) -> crate::Result<usize> {
		self.reads.fetch_add(1, Ordering::Relaxed);
		self.inner.read_at(offset, buf)
	}
	fn write_at(&mut self, offset: u64, buf: &[u8]) -> crate::Result<usize> {
		File::write_at(&mut self.inner, offset, buf)
	}
	fn sync(&self) -> crate::Result<()> {
		File::sync(&self.inner)
	}
	fn sync_data(&self) -> crate::Result<()> {
		File::sync_data(&self.inner)
	}
	fn size(&self) -> crate::Result<u64> {
		File::size(&self.inner)
	}
}

/// Builds a store directory with `num_tables` L0 SSTs of `entries_per_table`
/// entries each (~1 KB inline values, disjoint key ranges and seq ranges), a
/// manifest referencing them, and a compactor ready to run.
fn build_l0_store(
	num_tables: usize,
	entries_per_table: usize,
	target_file_size: u64,
) -> (TempDir, Arc<RwLock<LevelManifest>>, Compactor) {
	let temp_dir = TempDir::new().unwrap();
	let mut opts = Options::new();
	opts.path = temp_dir.path().to_path_buf();
	opts.target_file_size = target_file_size;
	// The default trigger is 4 L0 files; more files -> compaction fires.
	let opts = Arc::new(opts);

	std::fs::create_dir_all(opts.sstable_dir()).unwrap();
	std::fs::create_dir_all(opts.manifest_dir()).unwrap();
	std::fs::create_dir_all(opts.vlog_dir()).unwrap();

	let mut levels = Levels::new(opts.level_count as usize, 10);
	let value = ValueLocation::with_inline_value(vec![0xEFu8; 1024]).encode();

	for t in 0..num_tables {
		let id = (t + 1) as u64;
		let path = opts.sstable_file_path(id);
		let file = SysFile::create(&path).unwrap();
		let mut writer = TableWriter::new(file, id, Arc::clone(&opts), 0);
		for i in 0..entries_per_table {
			// Disjoint per-table key and seq ranges.
			let k = t * entries_per_table + i;
			let key = InternalKey::new(
				format!("key{k:012}").into_bytes(),
				(t * entries_per_table + i + 1) as u64,
				InternalKeyKind::Set,
				0,
			);
			writer.add(key, &value).unwrap();
		}
		writer.finish().unwrap();

		let file = SysFile::open(&path).unwrap();
		let file_size = file.metadata().unwrap().len();
		let table =
			Table::new(id, Arc::clone(&opts), Arc::new(file) as Arc<dyn File>, file_size).unwrap();
		std::sync::Arc::make_mut(&mut levels.get_levels_mut()[0]).insert(Arc::new(table));
	}

	let manifest = LevelManifest {
		path: opts.manifest_file_path(0),
		levels,
		hidden_set: HashSet::new(),
		next_table_id: Arc::new(AtomicU64::new(num_tables as u64 + 1)),
		manifest_format_version: MANIFEST_FORMAT_VERSION_V1,
		snapshots: Vec::new(),
		log_number: 0,
		last_sequence: (num_tables * entries_per_table) as u64,
	};
	write_manifest_to_disk(&manifest).unwrap();
	let manifest = Arc::new(RwLock::new(manifest));

	let strategy = Arc::new(Strategy::from_options(Arc::clone(&opts)));
	let compaction_options = CompactionOptions {
		lopts: Arc::clone(&opts),
		level_manifest: Arc::clone(&manifest),
		immutable_memtables: Arc::new(RwLock::new(ImmutableMemtables::default())),
		vlog: None,
		error_handler: Arc::new(BackgroundErrorHandler::new()),
		snapshot_tracker: SnapshotTracker::new(),
		versioned_index: None,
	};
	let compactor = Compactor::new(compaction_options, strategy);

	(temp_dir, manifest, compactor)
}

// --- B-P1: filter build memory ------------------------------------------------

/// Writes `n` entries with values of `value_size` bytes through a
/// `TableWriter` backed by a real file (so file contents leave the heap), and
/// returns the live heap bytes retained by the writer just before `finish()`.
fn retained_bytes_before_finish(n: usize, value_size: usize, with_filter: bool) -> i64 {
	let dir = TempDir::new().unwrap();
	let path = dir.path().join("proof.sst");

	let mut opts = Options::new();
	if !with_filter {
		opts = opts.with_filter_policy(None);
	}
	let opts = Arc::new(opts);

	let value = vec![0xABu8; value_size];
	let file = SysFile::create(&path).unwrap();

	let baseline = live_bytes();
	let mut writer = TableWriter::new(file, 1, Arc::clone(&opts), 1);
	for i in 0..n {
		let key = InternalKey::new(
			format!("key{i:012}").into_bytes(),
			(i + 1) as u64,
			InternalKeyKind::Set,
			0,
		);
		writer.add(key, &value).unwrap();
	}
	let retained = live_bytes() - baseline;

	writer.finish().unwrap();
	retained
}

/// B-P1: the filter builder buffers EVERY user key of the output file in RAM
/// until `finish()` (one owned Vec per key), so writer memory grows linearly
/// with the entry count — the root memory bug of issue #397. The ablation
/// arm (no filter policy) isolates the filter's share.
#[test]
fn proof_bp1_filter_buffers_every_key_until_finish() {
	// Small-entry regime (~115 B/entry): pointer-sized values, like vlog mode.
	let small_n = retained_bytes_before_finish(100_000, 100, true);
	let small_4n = retained_bytes_before_finish(400_000, 100, true);
	// Ablation: identical writes without a filter policy.
	let small_4n_no_filter = retained_bytes_before_finish(400_000, 100, false);

	// Large-entry regime (~2.5 KB/entry): inline values, vlog off.
	let large_n = retained_bytes_before_finish(8_000, 2_500, true);
	let large_4n = retained_bytes_before_finish(32_000, 2_500, true);

	let filter_share = small_4n - small_4n_no_filter;

	eprintln!("B-P1: small regime: N=100k → {small_n} B, 4N=400k → {small_4n} B");
	eprintln!("B-P1: small regime ablation (no filter), 4N → {small_4n_no_filter} B");
	eprintln!("B-P1: filter share at 4N = {filter_share} B ({} B/key)", filter_share / 400_000);
	eprintln!("B-P1: large regime: N=8k → {large_n} B, 4N=32k → {large_4n} B");

	// BUG (issue #397): residency is O(#entries) — 4x the entries retain
	// ~4x the bytes, and the filter's key buffer dominates (>= 15 B/key of
	// raw keys + per-key allocation overhead).
	assert!(
		small_4n >= small_n * 3,
		"small regime: expected linear growth, got N={small_n} vs 4N={small_4n}"
	);
	assert!(
		large_4n >= large_n * 3,
		"large regime: expected linear growth, got N={large_n} vs 4N={large_4n}"
	);
	assert!(
		filter_share >= 400_000 * 15,
		"expected the filter key buffer to dominate; share = {filter_share} B — if this fails, \
		 the partitioned-filter fix landed and this proof must flip to a per-key bound"
	);
}

// --- B-P2: absent-key lookups must not read data ------------------------------

/// Builds an SST of roughly `total_bytes` (1 KB values) through the real
/// `TableWriter`, opens it over a read-counting file, and returns the table
/// plus the read counter. `n_keys` reports how many keys were written.
fn build_counted_table(total_bytes: usize) -> (Table, Arc<AtomicU64>, usize) {
	let opts = Arc::new(Options::new());
	let value = ValueLocation::with_inline_value(vec![0xCDu8; 1024]).encode();
	let n = total_bytes / (value.len() + 16);

	let mut buf = Vec::new();
	{
		let mut writer = TableWriter::new(&mut buf, 1, Arc::clone(&opts), 1);
		for i in 0..n {
			let key = InternalKey::new(
				format!("key{i:012}").into_bytes(),
				(i + 1) as u64,
				InternalKeyKind::Set,
				0,
			);
			writer.add(key, &value).unwrap();
		}
		writer.finish().unwrap();
	}

	let size = buf.len() as u64;
	let reads = Arc::new(AtomicU64::new(0));
	let file = CountingFile {
		inner: buf,
		reads: Arc::clone(&reads),
	};
	let table = Table::new(1, opts, Arc::new(file) as Arc<dyn File>, size).unwrap();
	(table, reads, n)
}

/// B-P2 (invariant that must SURVIVE the partitioned-filter redesign):
/// bloom-filtered absent-key lookups are cheap in disk reads —
/// (a) repeating the same absent key after a warm-up performs ZERO reads
///     (everything the rejection needs is cached), and
/// (b) N distinct absent-key lookups read strictly less than N distinct
///     present-key lookups (present keys must fetch data blocks; absent
///     keys must never reach them).
#[test]
fn proof_bp2_absent_key_lookups_do_not_read_data() {
	let (table, reads, n) = build_counted_table(4 * 1024 * 1024);

	let absent_key = |i: usize| {
		// "kez" sorts inside the table's range so the index cannot reject it
		// before the filter is consulted.
		InternalKey::new(
			format!("kez{i:012}").into_bytes(),
			INTERNAL_KEY_SEQ_NUM_MAX,
			InternalKeyKind::Set,
			0,
		)
	};
	let present_key = |i: usize| {
		InternalKey::new(
			format!("key{i:012}").into_bytes(),
			INTERNAL_KEY_SEQ_NUM_MAX,
			InternalKeyKind::Set,
			0,
		)
	};

	// (a) Warm one absent key, then repeat it: zero further reads.
	assert!(table.get(&absent_key(0)).unwrap().is_none());
	let before = reads.load(Ordering::Relaxed);
	for _ in 0..50 {
		assert!(table.get(&absent_key(0)).unwrap().is_none());
	}
	let repeated_absent_reads = reads.load(Ordering::Relaxed) - before;

	// (b) 50 distinct absent keys vs 50 distinct present keys.
	let before = reads.load(Ordering::Relaxed);
	for i in 0..50 {
		assert!(table.get(&absent_key(1 + i * 97)).unwrap().is_none());
	}
	let absent_reads = reads.load(Ordering::Relaxed) - before;

	let before = reads.load(Ordering::Relaxed);
	for i in 0..50 {
		let k = (i * (n / 50)).min(n - 1);
		assert!(table.get(&present_key(k)).unwrap().is_some(), "present key {k} must be found");
	}
	let present_reads = reads.load(Ordering::Relaxed) - before;

	eprintln!(
		"B-P2: repeated-absent reads = {repeated_absent_reads}, distinct-absent reads = \
		 {absent_reads}, distinct-present reads = {present_reads}"
	);

	assert_eq!(
		repeated_absent_reads, 0,
		"a warmed absent-key lookup must be answered entirely from memory"
	);
	assert!(
		absent_reads < present_reads,
		"absent-key lookups ({absent_reads} reads) must stay strictly cheaper than present-key \
		 lookups ({present_reads} reads) — bloom must reject before any data block is fetched"
	);
}

/// P3: an L0→L1 compaction splits its output at `target_file_size`
/// boundaries — each output SST stays near the target no matter how large
/// the merged input is, so output sizes, future compaction inputs, and
/// crash-retry work are all independent of level size. (Issue #397: the
/// whole merge previously landed in ONE file — 12,601,804 bytes from this
/// exact input.)
#[test]
fn proof_p3_compaction_splits_output_at_target_file_size() {
	// 6 L0 tables x 2,000 entries x ~1 KB values ≈ 12.5 MB of input,
	// compacted with a 2 MB output target.
	const TARGET: u64 = 2 * 1024 * 1024;
	let (_dir, manifest, compactor) = build_l0_store(6, 2_000, TARGET);

	compactor.compact().unwrap();

	let guard = manifest.read().unwrap();
	let levels = guard.levels.get_levels();
	let l1_sizes: Vec<u64> = levels[1].tables.iter().map(|t| t.file_size).collect();
	let total: u64 = l1_sizes.iter().sum();

	eprintln!("P3: L1 outputs = {}, sizes = {l1_sizes:?}, total = {total}", l1_sizes.len());

	assert!(levels[0].tables.is_empty(), "L0 must be fully merged");
	assert!(total > 10 * 1024 * 1024, "input should be ~12 MB, got {total}");
	assert!(
		l1_sizes.len() > 1,
		"expected the merge to split into multiple outputs (== 1 is the issue #397 \
		 monolithic-output bug)"
	);
	// Rollover triggers at the target and then finishes the file (footer,
	// index, filter), so each output may exceed the target by the metadata
	// plus one block; 25% is generous headroom.
	let max_allowed = TARGET + TARGET / 4;
	for (i, size) in l1_sizes.iter().enumerate() {
		assert!(*size <= max_allowed, "output {i} is {size} bytes, above target+25% ({max_allowed})");
	}

	// The split outputs must form disjoint, sorted ranges and preserve every
	// key: read all keys back through table iterators.
	let mut all_keys = Vec::new();
	for table in &levels[1].tables {
		let mut iter = table.iter(None).unwrap();
		iter.seek_first().unwrap();
		while iter.valid() {
			all_keys.push(iter.key().to_owned().user_key.clone());
			iter.next().unwrap();
		}
	}
	assert_eq!(all_keys.len(), 12_000, "every input key must survive the split");
	let mut sorted = all_keys.clone();
	sorted.sort();
	sorted.dedup();
	assert_eq!(sorted.len(), 12_000, "keys must be unique across outputs");
	assert_eq!(all_keys, sorted, "outputs must be sorted and non-overlapping");
}
