//! Proof tests for issue #397: level compaction needs RAM comparable to the
//! merged level size, and an interrupted compaction self-sustains an OOM
//! restart loop.
//!
//! Protocol: each proof is committed FIRST, asserting the CURRENT broken
//! behavior with measured numbers; the fix flips the assertion to the
//! post-fix expectation in the same commit as the fix, turning the proof
//! into a permanent regression guard.

use std::collections::HashSet;
use std::fs::File as SysFile;
use std::sync::atomic::AtomicU64;
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
use crate::{InternalKey, InternalKeyKind, LSMIterator, Options};

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
