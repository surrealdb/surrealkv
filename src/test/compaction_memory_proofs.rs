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
use test_log::test;

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

// --- B-P3: old-data upgrade lifecycle (zero migration) ------------------------

fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) {
	std::fs::create_dir_all(dst).unwrap();
	for entry in std::fs::read_dir(src).unwrap() {
		let entry = entry.unwrap();
		let target = dst.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir_recursive(&entry.path(), &target);
		} else {
			std::fs::copy(entry.path(), &target).unwrap();
		}
	}
}

/// B-P3: a store written BY v0.21.3 code flows through the entire upgrade
/// lifecycle with zero migration. The committed fixture
/// (tests/fixtures/v0_21_3_store: 3 L0 SSTs, 600 keys, monolithic legacy
/// filters, generated by a main-branch build) is:
///   Stage 1 — opened through the REAL recovery path (Tree::new: manifest
///             load, orphan cleanup, compaction wake) and fully read via
///             the LEGACY filter path;
///   Stage 2 — mixed with NEW data written by the new code (old-format and
///             new-format SSTs side by side in one store), all readable;
///   Stage 3 — compacted (old + new files merged into new-format outputs),
///             reopened, and fully read again.
#[test(tokio::test)]
async fn proof_bp3_v0_21_3_store_full_upgrade_lifecycle() {
	let fixture =
		std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/v0_21_3_store");
	assert!(fixture.exists(), "fixture missing: {}", fixture.display());

	// Work on a copy: opening a store mutates it (orphan cleanup, WAL).
	let dir = TempDir::new().unwrap();
	let store_path = dir.path().join("store");
	copy_dir_recursive(&fixture, &store_path);

	// The fixture's tables must carry LEGACY (monolithic) filters and be
	// readable through the legacy dispatch.
	{
		let mut opts = Options::new();
		opts.path = store_path.clone();
		let opts = Arc::new(opts);
		let manifest =
			LevelManifest::load_from_file(opts.manifest_file_path(0), Arc::clone(&opts)).unwrap();
		let mut legacy = 0;
		for level in manifest.levels.get_levels() {
			for table in &level.tables {
				match &table.filter {
					crate::sstable::table::TableFilter::Legacy(_) => legacy += 1,
					other => panic!(
						"fixture table {} must carry a legacy filter, got {}",
						table.id,
						match other {
							crate::sstable::table::TableFilter::Partitioned(_) => "partitioned",
							_ => "none",
						}
					),
				}
			}
		}
		assert_eq!(legacy, 3, "fixture must contain exactly the 3 v0.21.3 tables");
	}

	let old_value = vec![0xEFu8; 200];
	let new_value = vec![0xABu8; 200];

	// Stage 1 + 2: open through the real recovery path, read all old keys,
	// then write new data with the new code and read both generations.
	{
		let tree = crate::TreeBuilder::new().with_path(store_path.clone()).build().unwrap();

		{
			let txn = tree.begin().unwrap();
			for k in 0..600usize {
				let key = format!("key{k:012}");
				let got = txn.get(key.as_bytes()).unwrap();
				assert_eq!(
					got.as_deref(),
					Some(old_value.as_slice()),
					"old key {key} unreadable after upgrade"
				);
			}
			assert!(txn.get(b"key999999999999").unwrap().is_none());
		}

		// Stage 2: mix in new-format data.
		let mut txn = tree.begin().unwrap();
		for k in 0..200usize {
			let key = format!("new{k:012}");
			txn.set(key.as_bytes(), &new_value).unwrap();
		}
		txn.commit().await.unwrap();
		tree.flush().unwrap(); // new-format L0 SST alongside the old ones

		{
			let txn = tree.begin().unwrap();
			for k in (0..600usize).step_by(7) {
				let key = format!("key{k:012}");
				assert_eq!(txn.get(key.as_bytes()).unwrap().as_deref(), Some(old_value.as_slice()));
			}
			for k in 0..200usize {
				let key = format!("new{k:012}");
				assert_eq!(txn.get(key.as_bytes()).unwrap().as_deref(), Some(new_value.as_slice()));
			}
		}

		// Stage 3: compact old + new files together (outputs are new-format).
		let strategy = Arc::new(Strategy::from_options(Arc::new(
			Options::new().with_path(store_path.clone()),
		)));
		tree.compact(strategy).unwrap();
		tree.close().await.unwrap();
	}

	// Reopen after compaction and read EVERYTHING back.
	{
		let tree = crate::TreeBuilder::new().with_path(store_path.clone()).build().unwrap();
		let txn = tree.begin().unwrap();
		for k in 0..600usize {
			let key = format!("key{k:012}");
			assert_eq!(
				txn.get(key.as_bytes()).unwrap().as_deref(),
				Some(old_value.as_slice()),
				"old key {key} lost after compaction rewrite"
			);
		}
		for k in 0..200usize {
			let key = format!("new{k:012}");
			assert_eq!(
				txn.get(key.as_bytes()).unwrap().as_deref(),
				Some(new_value.as_slice()),
				"new key {key} lost after compaction rewrite"
			);
		}
		drop(txn);
		tree.close().await.unwrap();
	}

	// The compacted store's tables must now be new-format (partitioned or,
	// for tiny outputs, still valid filters) — and remain fully readable,
	// which the reads above already proved.
	{
		let mut opts = Options::new();
		opts.path = store_path;
		let opts = Arc::new(opts);
		let manifest =
			LevelManifest::load_from_file(opts.manifest_file_path(0), Arc::clone(&opts)).unwrap();
		let mut partitioned = 0;
		let mut legacy = 0;
		for level in manifest.levels.get_levels() {
			for table in &level.tables {
				match &table.filter {
					crate::sstable::table::TableFilter::Partitioned(_) => partitioned += 1,
					crate::sstable::table::TableFilter::Legacy(_) => legacy += 1,
					_ => {}
				}
			}
		}
		assert!(partitioned > 0, "compaction outputs must carry partitioned filters");
		assert_eq!(legacy, 0, "all legacy tables were compacted into new-format outputs");
	}
}

// --- P4: end-to-end — compaction peak memory vs merged input size ------------

/// P4 (issue #397's headline claim, end-to-end): peak heap growth during a
/// compaction must be independent of the merged input size. Runs a ~125 MB
/// merge and a ~500 MB merge (4x) and compares live-heap high-water during
/// `compact()`. Ignored by default: run once per change with
/// `cargo test --lib proof_p4 -- --ignored --nocapture` (writes 625 MB of
/// SSTs; a few minutes).
///
/// Baseline measured on main (29fda1b) with the same harness:
/// S = 6,192,578 B, 4S = 23,668,848 B, ratio 3.82 — linear in input.
#[test]
#[ignore = "measurement harness: writes 625 MB; run once per change"]
fn proof_p4_compaction_peak_memory_independent_of_input_size() {
	// S: 6 tables x 20k entries x ~1 KB = ~125 MB input.
	let (_dir_s, _manifest_s, compactor_s) = build_l0_store(6, 20_000, 64 * 1024 * 1024);
	let base = reset_high_water();
	compactor_s.compact().unwrap();
	let peak_s = high_water_since(base);

	// 4S: 6 tables x 80k entries x ~1 KB = ~500 MB input.
	let (_dir_4s, _manifest_4s, compactor_4s) = build_l0_store(6, 80_000, 64 * 1024 * 1024);
	let base = reset_high_water();
	compactor_4s.compact().unwrap();
	let peak_4s = high_water_since(base);

	eprintln!(
		"P4: peak heap during compact(): S(125MB input) = {peak_s} B, 4S(500MB input) = \
		 {peak_4s} B, ratio = {:.2}",
		peak_4s as f64 / peak_s.max(1) as f64
	);

	// Flat means 4x the input does not cost 4x the memory; allow 2x headroom
	// for per-output metadata differences.
	assert!(
		peak_4s < peak_s * 2,
		"compaction peak memory scales with input size again (S = {peak_s} B, 4S = {peak_4s} B)"
	);
}

// --- GB-scale A/B: legacy writer vs partitioned writer, same binary -----------

/// Issue #397 at the reporter's scale, as a direct A/B in one binary:
/// building ONE large SST (23M keys ≈ a ~2.7 GB file with ~100 B entries)
/// needed over 1 GB of RAM for the bloom filter ALONE under the legacy
/// design — the exact `FilterBlockWriter` struct shipped in ≤ v0.21.3,
/// still compiled for tests, fed the same keys — while the partitioned
/// writer stays under 100 MB on identical input. Arm B deliberately builds
/// one UNSPLIT table (writer output discarded via io::sink, so only builder
/// memory is measured): even without target_file_size splitting, the filter
/// fix alone removes the GB-scale residency; splitting bounds it further in
/// real compactions.
///
/// Ignored by default: run once per change with
/// `cargo test --lib gb_scale -- --ignored --nocapture` (~1-2 minutes).
#[test]
#[ignore = "GB-scale measurement (~1-2 min); run once per change"]
fn proof_gb_scale_legacy_filter_needs_1gb_partitioned_does_not() {
	use crate::sstable::bloom::LevelDBBloomFilter;
	use crate::sstable::filter_block::FilterBlockWriter;

	const KEYS: usize = 23_000_000;
	let policy: Arc<dyn crate::FilterPolicy> = Arc::new(LevelDBBloomFilter::new(10));

	// Arm A: the legacy monolithic filter builder exactly as shipped —
	// add_key buffers every raw key until finish().
	let base = reset_high_water();
	let old_peak;
	{
		let mut legacy = FilterBlockWriter::new(Arc::clone(&policy));
		for k in 0..KEYS {
			legacy.add_key(format!("key{k:012}").as_bytes());
		}
		old_peak = high_water_since(base);
	}

	// Arm B: the partitioned writer building the same single (unsplit)
	// table: same 23M keys, ~100 B values, output discarded.
	let opts = Arc::new(Options::new());
	let value = ValueLocation::with_inline_value(vec![0xEFu8; 100]).encode();
	let base = reset_high_water();
	let new_peak;
	{
		let mut writer = TableWriter::new(std::io::sink(), 1, Arc::clone(&opts), 1);
		for k in 0..KEYS {
			let key = InternalKey::new(
				format!("key{k:012}").into_bytes(),
				(k + 1) as u64,
				InternalKeyKind::Set,
				0,
			);
			writer.add(key, &value).unwrap();
		}
		new_peak = high_water_since(base);
	}

	eprintln!(
		"GB-scale: legacy filter peak = {old_peak} B ({:.2} GB), partitioned writer peak = \
		 {new_peak} B ({:.1} MB), reduction = {:.0}x",
		old_peak as f64 / (1 << 30) as f64,
		new_peak as f64 / (1 << 20) as f64,
		old_peak as f64 / new_peak.max(1) as f64
	);

	assert!(
		old_peak >= 900 * 1024 * 1024,
		"legacy arm expected ~1 GB for 23M keys, got {old_peak} B — measurement broken?"
	);
	assert!(
		new_peak <= 100 * 1024 * 1024,
		"partitioned writer used {new_peak} B for 23M keys — the O(keys) filter residency \
		 (issue #397) is back"
	);
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

/// B-P1: the partitioned filter builder buffers a 4-byte HASH per key
/// (drained into finished partition bits at every index-partition cut), not
/// the raw key. Pre-fix the builder held every user key as an owned Vec
/// until `finish()`: 18.78 MB at 400k keys, 46 B/key, 98.9% of writer
/// residency — the root memory bug of issue #397. Post-fix measured:
/// 2.78 MB at 400k keys, filter share 2.58 MB ≈ 6.4 B/key (hash + finished
/// bloom bits + buffer overheads). The ablation arm (no filter policy)
/// isolates the filter's share.
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

	// Post-fix bounds: the filter retains ~6.4 B/key (4 B hash + ~1.25 B/key
	// finished bloom bits + buffer overheads) instead of 46 B/key of raw
	// keys. 8 B/key of headroom still fails hard on any return of raw-key
	// buffering.
	assert!(
		filter_share <= 400_000 * 8,
		"filter memory exceeds 8 B/key (share = {filter_share} B at 400k keys) — the raw-key \
		 buffering bug (issue #397) is back"
	);
	assert!(
		small_4n <= 400_000 * 10,
		"small regime: writer retains {small_4n} B at 400k entries, expected <= 10 B/key"
	);
	assert!(
		large_4n <= 32_000 * 16,
		"large regime: writer retains {large_4n} B at 32k entries, expected <= 16 B/key"
	);
	// Guard the measurement itself.
	assert!(small_n > 0 && large_n > 0, "allocation counter appears broken");
}

/// Companion to B-P1/B-P2: end-to-end read correctness of the partitioned
/// filter through the real read path. Every written key must be found (no
/// bloom false negatives across any partition) and in-range absent keys
/// must return None.
#[test]
fn partitioned_filter_reads_have_no_false_negatives() {
	let (table, _reads, n) = build_counted_table(4 * 1024 * 1024);

	// The table must actually have a partitioned, multi-partition filter.
	let partitions = match &table.filter {
		crate::sstable::table::TableFilter::Partitioned(f) => f.num_partitions(),
		_ => panic!("new tables must carry a partitioned filter"),
	};
	assert!(partitions > 1, "test requires a multi-partition filter, got {partitions}");

	for i in (0..n).step_by(23) {
		let key = InternalKey::new(
			format!("key{i:012}").into_bytes(),
			INTERNAL_KEY_SEQ_NUM_MAX,
			InternalKeyKind::Set,
			0,
		);
		assert!(table.get(&key).unwrap().is_some(), "false negative for present key index {i}");
	}
	for i in (1..n).step_by(101) {
		let key = InternalKey::new(
			format!("key{i:012}x").into_bytes(),
			INTERNAL_KEY_SEQ_NUM_MAX,
			InternalKeyKind::Set,
			0,
		);
		assert!(table.get(&key).unwrap().is_none(), "phantom hit for absent key index {i}");
	}
}

// --- B-P2: absent-key lookups must not read data ------------------------------

/// Builds an SST of roughly `total_bytes` (100 B values, so a multi-MB
/// table spans multiple filter partitions) through the real `TableWriter`,
/// opens it over a read-counting file, and returns the table plus the read
/// counter and the number of keys written.
fn build_counted_table(total_bytes: usize) -> (Table, Arc<AtomicU64>, usize) {
	// A realistic block-cache size: with the 1 MB default, quick_cache's
	// per-shard weight limit can reject ~16 KB filter/index partitions
	// outright, making every lookup re-read them.
	let opts = Arc::new(Options::new().with_block_cache_capacity(8 * 1024 * 1024));
	let value = ValueLocation::with_inline_value(vec![0xCDu8; 100]).encode();
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
		// "key<i>x" sorts strictly BETWEEN two present keys, so neither the
		// table range check nor the index can reject it — only the bloom
		// filter can. (A suffix beyond the last key would take the
		// past-the-end fast path and never exercise the filter.)
		InternalKey::new(
			format!("key{i:012}x").into_bytes(),
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

	// (b) 50 distinct absent keys vs 50 distinct present keys. Indices stay
	// well inside the written key range (0..n) so every probe is in-range.
	let before = reads.load(Ordering::Relaxed);
	for i in 0..50 {
		let k = (1 + i * (n / 64)).min(n - 1);
		assert!(table.get(&absent_key(k)).unwrap().is_none());
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
