//! The crash cases of the plaintext suite, run over crash images of a memory pool.
//!
//! The suites `crash_consistency_tests`, `wal_torn_tail_tests`, `wal_group_crash_tests` and
//! `compaction_crash_tests` make their crashes with `std::fs`: they copy the directory, cut a file
//! and zero the files that an fsync ledger says were never synced. A pool has no ledger and no
//! disk to copy, and what it has instead is `FileSystem::simulate_crash`, which mounts the
//! memory as a given way of crashing would have left it (only what was flushed, a prefix of what
//! was not, a write that is torn, the length kept and the bytes not). The tests here ask the same
//! questions of those images:
//!
//! * a power loss after a compaction keeps what was synced, and a table that the manifest lists and
//!   that is empty is refused cleanly;
//! * the three losses of the tail of a log that the first suite names (a recovery that splits a
//!   segment, a memtable that fills in the middle of a group, a rotation during an append);
//! * a record that is torn at every byte, then more commits, a second crash and a third, and every
//!   commit that was acknowledged is there after each recovery;
//! * a group of batches that is torn at every point of its write is recovered as a prefix of the
//!   group, never as a part of a batch;
//! * the images taken at every stage of a compaction from each level to the next open to the data
//!   that was acknowledged, compact again, and open again after another crash.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use test_log::test;

use super::fs_engine_tests::{commit_range, key_of, options_in, value_of, within};
use crate::batch::Batch;
use crate::compaction::compactor::CompactionStage;
use crate::compaction::leveled::Strategy;
use crate::compaction::{CompactionChoice, CompactionInput, CompactionStrategy};
use crate::levels::LevelManifest;
use crate::ring::PipelineHook;
use crate::{CrashMode, Durability, Error, FileSystem, Mode, Options, Result, Tree, TreeBuilder};

/// Every way of crashing that the memory pool has, with the parameters that a few writes reach.
const MODES: &[CrashMode] = &[
	CrashMode::LoseUnsynced,
	CrashMode::ZeroTail,
	CrashMode::Prefix(0),
	CrashMode::Prefix(1),
	CrashMode::Prefix(3),
	CrashMode::Prefix(usize::MAX),
];

/// The crash image of `fs` in `mode`: a pool of its own, which the engine opens as it opens any
/// database that a crash left.
fn image(fs: &FileSystem, mode: CrashMode) -> FileSystem {
	fs.simulate_crash(mode).expect("a memory pool crashes")
}

/// `options` for a database in `fs`.
fn in_fs(options: &Options, fs: &FileSystem) -> Options {
	Options {
		fs: Arc::clone(&fs.0),
		..options.clone()
	}
}

/// Opens the database of `options` in the image `fs`.
async fn open_image(options: &Options, fs: &FileSystem, what: &str) -> Tree {
	TreeBuilder::with_options(in_fs(options, fs))
		.build_async()
		.await
		.unwrap_or_else(|e| panic!("{what}: the database does not open: {e}"))
}

/// The tree stops as a crash stops it: nothing is closed, flushed or released in an orderly way.
fn crash(tree: Tree, fs: &FileSystem) {
	tree.abandon();
	fs.abandon();
	drop(tree);
}

fn present(tree: &Tree, range: std::ops::Range<usize>, len: usize, what: &str) {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	for i in range {
		assert_eq!(txn.get(key_of(i)).unwrap(), Some(value_of(i, len)), "{what}: key {i} was lost");
	}
}

/// The keys of `range` that are there, which must be a prefix of the range: a commit that is
/// there came after every commit before it.
fn prefix_of(tree: &Tree, range: std::ops::Range<usize>, len: usize, what: &str) -> usize {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let start = range.start;
	let mut kept = 0;
	let mut gap = false;
	for i in range {
		match txn.get(key_of(i)).unwrap() {
			Some(value) => {
				assert!(!gap, "{what}: key {i} is there after a key that is not");
				assert_eq!(value, value_of(i, len), "{what}: key {i} has another value");
				kept = i + 1 - start;
			}
			None => gap = true,
		}
	}
	kept
}

// ---------------------------------------------------------------------------------------------
// crash_consistency_tests
// ---------------------------------------------------------------------------------------------

/// A merge of the tables of two levels into the second.
struct MergeLevels {
	source: u8,
	target: u8,
}

impl CompactionStrategy for MergeLevels {
	fn pick_levels(&self, manifest: &LevelManifest) -> Result<CompactionChoice> {
		let levels = manifest.levels.get_levels();
		let mut tables_to_merge: Vec<u64> =
			levels[self.source as usize].tables.iter().map(|t| t.id).collect();
		if self.target != self.source {
			tables_to_merge.extend(levels[self.target as usize].tables.iter().map(|t| t.id));
		}
		if tables_to_merge.is_empty() {
			return Ok(CompactionChoice::Skip);
		}
		Ok(CompactionChoice::Merge(CompactionInput {
			tables_to_merge,
			target_level: self.target,
			source_level: self.source,
		}))
	}
}

fn compact(tree: &Tree, source: u8, target: u8) -> Result<()> {
	tree.compact(Arc::new(MergeLevels {
		source,
		target,
	}))
}

/// Writes keys `range` as one commit that is synced, and flushes the memtable to a table.
async fn commit_and_flush(tree: &Tree, range: std::ops::Range<usize>) {
	commit_range(tree, range, 100, Durability::Immediate).await;
	tree.flush().unwrap();
}

/// The options of the crash tests: nothing is flushed on close, two tables in level 0 compact.
fn crash_options(fs: &FileSystem) -> Options {
	let mut options = options_in(fs, 4 * 1024 * 1024);
	options.flush_on_close = false;
	options.level0_max_files = 2;
	options
}

/// A power loss shortly after a compaction: the flushed tables, the compaction output and the
/// manifest that lists them were all synced before the call that depended on them, so every way of
/// crashing leaves a database that opens to everything that was acknowledged. (On `std` the same
/// is asked of the ledger of synced files, in `crash_consistency_tests`.)
#[test(tokio::test)]
async fn a_power_loss_after_a_compaction_keeps_every_synced_commit_in_every_crash_image() {
	within("power loss after a compaction", async {
		for mode in MODES {
			let fs = FileSystem::memory();
			let options = crash_options(&fs);
			let tree = TreeBuilder::with_options(options.clone()).build_async().await.unwrap();
			commit_and_flush(&tree, 0..100).await;
			commit_and_flush(&tree, 100..200).await;
			tree.compact(Arc::new(Strategy::from_options(Arc::new(options.clone())))).unwrap();
			{
				let manifest = tree.core.inner.level_manifest.read().unwrap();
				let levels = manifest.levels.get_levels();
				assert!(levels[0].tables.is_empty(), "L0 is empty after the compaction");
				assert_eq!(levels[1].tables.len(), 1, "the output is in L1");
			}
			// A flush after the compaction: the manifest lists the output and a newer table.
			commit_and_flush(&tree, 200..300).await;
			// And a commit that is only in the log.
			commit_range(&tree, 300..320, 100, Durability::Immediate).await;
			let crashed = image(&fs, *mode);
			crash(tree, &fs);

			let tree = open_image(&options, &crashed, &format!("{mode:?}")).await;
			present(&tree, 0..320, 100, &format!("{mode:?}"));
			tree.close().await.unwrap();
		}
	})
	.await;
}

/// A manifest that lists a table whose file is empty is refused with the error that says so, and
/// the database is not opened over it: the state that a table that was not synced before the
/// manifest led to.
#[test(tokio::test)]
async fn a_manifest_that_lists_an_empty_table_is_refused_cleanly() {
	within("empty table", async {
		let fs = FileSystem::memory();
		let options = crash_options(&fs);
		let tree = TreeBuilder::with_options(options.clone()).build_async().await.unwrap();
		commit_and_flush(&tree, 0..100).await;
		commit_and_flush(&tree, 100..200).await;
		let table_id = {
			let manifest = tree.core.inner.level_manifest.read().unwrap();
			manifest.levels.get_levels()[0].tables.first().expect("a table in L0").id
		};
		let crashed = image(&fs, CrashMode::Prefix(usize::MAX));
		crash(tree, &fs);

		let path = options.sstable_file_path(table_id);
		crashed.raw_truncate_for_test(path.to_str().unwrap(), 0).unwrap();
		match TreeBuilder::with_options(in_fs(&options, &crashed)).build_async().await {
			Ok(_) => panic!("the database opened over an empty table"),
			Err(Error::LoadManifestFail(message)) => {
				assert!(message.contains("file size is too small"), "{message}");
			}
			Err(other) => panic!("expected LoadManifestFail, got {other}"),
		}
	})
	.await;
}

/// Recovery splits a segment over several memtables when the memtable got smaller between runs.
/// Flushing the first part must not move the log number past the segment: the rest of it is only
/// in the log, and a second crash must not lose it.
#[test(tokio::test)]
async fn a_crash_after_a_recovery_that_splits_a_segment_does_not_lose_its_tail() {
	within("split segment", async {
		const KEYS: usize = 200;
		for mode in [CrashMode::Prefix(usize::MAX), CrashMode::LoseUnsynced, CrashMode::ZeroTail] {
			let fs = FileSystem::memory();
			let big = {
				let mut options = options_in(&fs, 4 * 1024 * 1024);
				options.flush_on_close = false;
				options
			};
			let tree = TreeBuilder::with_options(big.clone()).build_async().await.unwrap();
			commit_range(&tree, 0..KEYS, 100, Durability::Immediate).await;
			let first = image(&fs, mode);
			crash(tree, &fs);

			// A memtable far too small for that segment: the recovery splits it, and the database
			// crashes again before the last part is flushed.
			let mut small = big.clone();
			small.max_memtable_size = 16 * 1024;
			let tree = open_image(&small, &first, "the split recovery").await;
			assert!(tree.core.inner.l0_file_count() > 0, "part of the segment was flushed");
			let second = image(&first, mode);
			crash(tree, &first);

			let tree = open_image(&big, &second, "after the second crash").await;
			present(&tree, 0..KEYS, 100, &format!("{mode:?}"));
			tree.close().await.unwrap();
		}
	})
	.await;
}

/// A group of batches that fills a memtable in its middle, rotating the log with it: the rest of
/// the group is in a memtable of the next segment while its records are in the previous one, and
/// a flush of the older memtable must not move the log number past them.
#[test(tokio::test)]
async fn a_crash_after_a_memtable_fills_in_the_middle_of_a_group_does_not_lose_the_rest() {
	within("fill in a group", async {
		const KEYS: u64 = 200;
		for mode in [CrashMode::Prefix(usize::MAX), CrashMode::LoseUnsynced, CrashMode::ZeroTail] {
			let fs = FileSystem::memory();
			let mut options = options_in(&fs, 16 * 1024);
			options.flush_on_close = false;
			let tree = TreeBuilder::with_options(options.clone()).build_async().await.unwrap();
			let first_seq = tree.core.inner.visible_seq_num.load(Ordering::Acquire) + 1;
			let batches: Vec<Batch> = (0..KEYS)
				.map(|i| {
					let mut batch = Batch::new(first_seq + i);
					batch.set(key_of(i as usize), value_of(i as usize, 100), 0).unwrap();
					batch
				})
				.collect();
			// Written and synced as one group, much larger than one memtable.
			tree.core.commit_pipeline.flush_group(&batches, true).await.unwrap();
			assert!(
				!tree.core.inner.immutable_memtables.read().unwrap().is_empty(),
				"the group filled a memtable"
			);
			tree.core.inner.flush_all_immutables_sync().unwrap();
			let crashed = image(&fs, mode);
			crash(tree, &fs);

			let tree = open_image(&options, &crashed, &format!("{mode:?}")).await;
			present(&tree, 0..KEYS as usize, 100, &format!("{mode:?}"));
			tree.close().await.unwrap();
		}
	})
	.await;
}

/// A rotation that is made from outside the flusher (a checkpoint does it) lands while a run of
/// batches is being logged. The run's records are in the segment that was rotated out and flushed,
/// so applying them to the new memtable alone would lose them in a crash.
#[test(tokio::test)]
async fn a_crash_after_a_rotation_during_a_log_append_does_not_lose_the_run() {
	within("rotate during an append", async {
		for mode in [CrashMode::Prefix(usize::MAX), CrashMode::LoseUnsynced, CrashMode::ZeroTail] {
			let fs = FileSystem::memory();
			let mut options = options_in(&fs, 1 << 20);
			options.flush_on_close = false;
			let tree = TreeBuilder::with_options(options.clone()).build_async().await.unwrap();
			commit_range(&tree, 0..1, 100, Durability::Immediate).await;

			let fired = Arc::new(AtomicBool::new(false));
			{
				let core = Arc::clone(&tree.core.inner);
				let fired = Arc::clone(&fired);
				tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
					if matches!(point, PipelineHook::AfterWalSync { .. })
						&& !fired.swap(true, Ordering::SeqCst)
					{
						core.rotate_memtable().unwrap();
						core.flush_all_immutables_sync().unwrap();
					}
				})));
			}
			let mut batch = Batch::new(tree.core.inner.visible_seq_num.load(Ordering::Acquire) + 1);
			batch.set(key_of(1), value_of(1, 100), 0).unwrap();
			tree.core.commit_pipeline.flush_group(&[batch], true).await.unwrap();
			assert!(fired.load(Ordering::SeqCst), "the rotation hook fired");
			tree.core.commit_pipeline.set_hook(None);
			let crashed = image(&fs, mode);
			crash(tree, &fs);

			let tree = open_image(&options, &crashed, &format!("{mode:?}")).await;
			present(&tree, 0..2, 100, &format!("{mode:?}"));
			tree.close().await.unwrap();
		}
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// wal_torn_tail_tests
// ---------------------------------------------------------------------------------------------

/// The segment that the commits go to: the highest that has bytes in it.
fn active_segment(fs: &FileSystem) -> String {
	fs.list_for_test("db/wal")
		.iter()
		.rev()
		.map(|name| format!("db/wal/{name}"))
		.find(|path| fs.file_len_for_test(path).is_some_and(|len| len > 0))
		.expect("the log holds the commits")
}

/// A record of the log cut at `n` bytes of its length, for every `n`: ten commits that were
/// synced, one that was not and is torn, a recovery, five commits more, a second crash, a
/// recovery, a third crash, and a recovery. Every commit that was acknowledged is there after each
/// of them, the torn one never is, and the repaired segment is not appended to again.
#[test(tokio::test)]
async fn a_record_that_is_torn_at_every_byte_is_dropped_and_the_commits_around_it_are_kept() {
	within("torn at every byte", async {
		// Measure the record: its bytes are what the unsynced commit adds to the segment.
		let record_len = {
			let fs = FileSystem::memory();
			let options = options_in(&fs, 1 << 20);
			let tree = TreeBuilder::with_options(options).build_async().await.unwrap();
			commit_range(&tree, 0..10, 64, Durability::Immediate).await;
			let segment = active_segment(&fs);
			let acked = fs.file_len_for_test(&segment).unwrap();
			commit_range(&tree, 10..11, 64, Durability::Eventual).await;
			let record = fs.file_len_for_test(&segment).unwrap() - acked;
			crash(tree, &fs);
			record
		};
		assert!(record_len >= 64, "the record holds its value: {record_len} bytes");

		for cut in 0..=record_len {
			let fs = FileSystem::memory();
			let mut options = options_in(&fs, 1 << 20);
			options.flush_on_close = false;
			let tree = TreeBuilder::with_options(options.clone()).build_async().await.unwrap();
			commit_range(&tree, 0..10, 64, Durability::Immediate).await;
			let segment = active_segment(&fs);
			let acked = fs.file_len_for_test(&segment).unwrap();
			commit_range(&tree, 10..11, 64, Durability::Eventual).await;
			let first = image(&fs, CrashMode::Prefix(usize::MAX));
			crash(tree, &fs);
			first.raw_truncate_for_test(&segment, acked + cut).unwrap();

			let what = format!("cut at {cut} of {record_len}");
			let tree = open_image(&options, &first, &what).await;
			present(&tree, 0..10, 64, &what);
			let torn = prefix_of(&tree, 10..11, 64, &what);
			assert_eq!(torn, usize::from(cut == record_len), "{what}: the torn record");
			commit_range(&tree, 11..16, 64, Durability::Immediate).await;
			let second = image(&first, CrashMode::LoseUnsynced);
			crash(tree, &first);

			let what = format!("{what}, second crash");
			let tree = open_image(&options, &second, &what).await;
			present(&tree, 0..10, 64, &what);
			present(&tree, 11..16, 64, &what);
			commit_range(&tree, 16..18, 64, Durability::Immediate).await;
			let third = image(&second, CrashMode::LoseUnsynced);
			crash(tree, &second);

			let what = format!("{what}, third crash");
			let tree = open_image(&options, &third, &what).await;
			present(&tree, 0..10, 64, &what);
			present(&tree, 11..18, 64, &what);
			tree.close().await.unwrap();
		}
	})
	.await;
}

/// The same record torn by the crash model itself and not by a cut: the first `k` writes that were
/// not synced are kept, and the next one only in its first `n` bytes.
#[test(tokio::test)]
async fn a_write_that_the_crash_tears_leaves_a_log_that_recovers_to_the_synced_commits() {
	within("torn writes", async {
		for k in 0..6usize {
			for n in [0usize, 1, 2, 3, 7, 8, 9, 15, 16, 31, 33, 100] {
				let fs = FileSystem::memory();
				let mut options = options_in(&fs, 1 << 20);
				options.flush_on_close = false;
				let tree = TreeBuilder::with_options(options.clone()).build_async().await.unwrap();
				commit_range(&tree, 0..10, 64, Durability::Immediate).await;
				commit_range(&tree, 10..14, 64, Durability::Eventual).await;
				let crashed = image(&fs, CrashMode::Tear(k, n));
				crash(tree, &fs);

				let what = format!("Tear({k}, {n})");
				let tree = open_image(&options, &crashed, &what).await;
				present(&tree, 0..10, 64, &what);
				prefix_of(&tree, 10..14, 64, &what);
				commit_range(&tree, 20..25, 64, Durability::Immediate).await;
				let again = image(&crashed, CrashMode::LoseUnsynced);
				crash(tree, &crashed);

				let what = format!("{what}, second crash");
				let tree = open_image(&options, &again, &what).await;
				present(&tree, 0..10, 64, &what);
				present(&tree, 20..25, 64, &what);
				tree.close().await.unwrap();
			}
		}
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// wal_group_crash_tests
// ---------------------------------------------------------------------------------------------

/// A group of batches that is torn at every point of its write: what is recovered is the synced
/// commits and a prefix of the group, whole batches only.
#[test(tokio::test)]
async fn a_group_that_is_torn_at_every_point_of_its_write_recovers_as_a_prefix_of_whole_batches() {
	within("torn group", async {
		const GROUP: u64 = 24;
		let mut kept_sizes = BTreeSet::new();
		for k in 0..8usize {
			for n in [0usize, 1, 4, 5, 8, 9, 31, 64, 150, 1000] {
				let fs = FileSystem::memory();
				let mut options = options_in(&fs, 1 << 20);
				options.flush_on_close = false;
				let tree = TreeBuilder::with_options(options.clone()).build_async().await.unwrap();
				commit_range(&tree, 0..5, 64, Durability::Immediate).await;
				let first_seq = tree.core.inner.visible_seq_num.load(Ordering::Acquire) + 1;
				// Batches of two entries: a torn batch would show as one entry of a pair.
				let batches: Vec<Batch> = (0..GROUP)
					.map(|b| {
						let mut batch = Batch::new(first_seq + b);
						for e in 0..2usize {
							let i = 100 + 2 * b as usize + e;
							batch.set(key_of(i), value_of(i, 64), 0).unwrap();
						}
						batch
					})
					.collect();
				// Appended and applied, not synced.
				tree.core.commit_pipeline.flush_group(&batches, false).await.unwrap();
				let crashed = image(&fs, CrashMode::Tear(k, n));
				crash(tree, &fs);

				let what = format!("Tear({k}, {n})");
				let tree = open_image(&options, &crashed, &what).await;
				present(&tree, 0..5, 64, &what);
				let kept = prefix_of(&tree, 100..100 + 2 * GROUP as usize, 64, &what);
				assert_eq!(kept % 2, 0, "{what}: a batch is there in part: {kept} entries");
				kept_sizes.insert(kept);
				// The database takes more commits, and they survive another crash.
				commit_range(&tree, 300..305, 64, Durability::Immediate).await;
				let again = image(&crashed, CrashMode::LoseUnsynced);
				crash(tree, &crashed);
				let what = format!("{what}, second crash");
				let tree = open_image(&options, &again, &what).await;
				present(&tree, 0..5, 64, &what);
				present(&tree, 300..305, 64, &what);
				tree.close().await.unwrap();
			}
		}
		assert!(
			kept_sizes.len() > 1,
			"the tears cut the group at different places: {kept_sizes:?}"
		);
	})
	.await;
}

// ---------------------------------------------------------------------------------------------
// compaction_crash_tests
// ---------------------------------------------------------------------------------------------

/// What the tree must hold.
type Model = BTreeMap<usize, Option<Vec<u8>>>;

const KEYS: usize = 90;

fn tagged(tag: &str, i: usize, big: bool) -> Vec<u8> {
	let mut v = format!("{tag}_{i:04}_").into_bytes();
	let len = if big {
		1500
	} else {
		100
	};
	let seed = v.len();
	while v.len() < len {
		v.push(b'a' + ((v.len() * 7 + i + seed) % 26) as u8);
	}
	v
}

/// Commits `ops` (a key and a value, or a delete) as one transaction, synced.
async fn commit_ops(tree: &Tree, model: &mut Model, ops: Vec<(usize, Option<Vec<u8>>)>) {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(Durability::Immediate);
	for (i, value) in ops {
		match &value {
			Some(v) => txn.set(key_of(i), v.clone()).unwrap(),
			None => txn.delete(key_of(i)).unwrap(),
		}
		model.insert(i, value);
	}
	txn.commit().await.unwrap();
}

/// Every key reads its value or is absent, and a scan returns exactly the live keys.
fn verify(tree: &Tree, model: &Model, what: &str) {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	for (i, expected) in model {
		let got = txn.get(key_of(*i)).unwrap();
		assert!(
			got.as_deref() == expected.as_deref(),
			"{what}: key {i}: got {:?} bytes, expected {:?} bytes",
			got.as_ref().map(Vec::len),
			expected.as_ref().map(Vec::len)
		);
	}
	let live: Vec<Vec<u8>> =
		model.iter().filter(|(_, v)| v.is_some()).map(|(i, _)| key_of(*i)).collect();
	let scanned = super::collect_transaction_all(&mut txn.iter().unwrap()).unwrap();
	assert_eq!(
		scanned.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
		live,
		"{what}: the keys of a full scan"
	);
}

fn tables_per_level(tree: &Tree) -> Vec<Vec<u64>> {
	let manifest = tree.core.inner.level_manifest.read().unwrap();
	manifest
		.levels
		.get_levels()
		.iter()
		.map(|level| {
			let mut ids: Vec<u64> = level.tables.iter().map(|t| t.id).collect();
			ids.sort_unstable();
			ids
		})
		.collect()
}

/// The options of a database that compacts only when told to: four levels, small tables, a value
/// log, and no limit on level 0.
fn compaction_options(fs: &FileSystem) -> Options {
	let mut options = options_in(fs, 8 * 1024 * 1024);
	options.flush_on_close = false;
	options.level_count = 4;
	options.enable_vlog = true;
	options.vlog_value_threshold = 1024;
	options.vlog_max_file_size = 8 * 1024;
	options.target_file_size = 2 * 1024;
	options.level0_max_files = 10_000;
	options.l0_stall_threshold = 10_000;
	options.memtable_stall_threshold = 10_000;
	options.max_bytes_for_level = 1 << 40;
	options
}

/// The layout of `compaction_crash_tests`: L3 holds a seed of every key, L2 a second generation
/// with deletes that hide seeds, L0 three tables that set, delete and set again, and a few commits
/// that are only in the log.
async fn layout(tree: &Tree) -> Model {
	let mut model = Model::new();
	let seeds = (0..KEYS).map(|i| (i, Some(tagged("seed", i, false)))).collect();
	commit_ops(tree, &mut model, seeds).await;
	tree.flush().unwrap();
	compact(tree, 0, 3).unwrap();

	let mut ops = Vec::new();
	for i in 0..KEYS {
		if i % 7 == 0 {
			ops.push((i, None));
		} else if i % 3 == 0 {
			ops.push((i, Some(tagged("mid", i, i % 9 == 0))));
		}
	}
	commit_ops(tree, &mut model, ops).await;
	tree.flush().unwrap();
	compact(tree, 0, 2).unwrap();

	let r1 = (0..KEYS).filter(|i| i % 2 == 0).map(|i| (i, Some(tagged("r1", i, i % 10 == 0))));
	commit_ops(tree, &mut model, r1.collect()).await;
	tree.flush().unwrap();

	let mut r2 = Vec::new();
	for i in 0..KEYS {
		if i % 5 == 1 {
			r2.push((i, None));
		} else if i % 4 == 0 {
			r2.push((i, Some(tagged("r2", i, i % 8 == 0))));
		}
	}
	commit_ops(tree, &mut model, r2).await;
	tree.flush().unwrap();

	let mut r3 = Vec::new();
	for i in 0..KEYS {
		if i % 35 == 0 {
			r3.push((i, Some(tagged("again", i, true))));
		} else if i % 6 == 0 {
			r3.push((i, Some(tagged("r3", i, i % 12 == 0))));
		}
	}
	commit_ops(tree, &mut model, r3).await;
	tree.flush().unwrap();

	let tail = vec![
		(2, Some(tagged("tail", 2, false))),
		(KEYS, Some(tagged("tail", KEYS, true))),
		(KEYS + 1, Some(tagged("tail", KEYS + 1, false))),
		(4, None),
		(10, None),
	];
	commit_ops(tree, &mut model, tail).await;
	let levels = tables_per_level(tree);
	assert_eq!(levels[0].len(), 3, "three tables in L0: {levels:?}");
	assert!(levels[2].len() >= 2 && levels[3].len() >= 2, "L2 and L3 hold tables: {levels:?}");
	verify(tree, &model, "the layout");
	model
}

#[derive(Clone, Copy, Debug)]
enum Shape {
	L0ToL1,
	L1ToL2,
	L2ToL3,
}

impl Shape {
	fn source(self) -> u8 {
		match self {
			Shape::L0ToL1 => 0,
			Shape::L1ToL2 => 1,
			Shape::L2ToL3 => 2,
		}
	}

	/// Runs the compactions that come before this one, so that its inputs are in place.
	fn advance(self, tree: &Tree) {
		match self {
			Shape::L0ToL1 => {}
			Shape::L1ToL2 => compact(tree, 0, 1).unwrap(),
			Shape::L2ToL3 => {
				compact(tree, 0, 1).unwrap();
				compact(tree, 1, 2).unwrap();
			}
		}
	}
}

/// The compaction of one shape with an image of the pool at each of its stages, in each way of
/// crashing: every image opens to the data that was acknowledged, compacts again, and opens again
/// after another crash.
async fn stage_images(shape: Shape) {
	let fs = FileSystem::memory();
	let options = compaction_options(&fs);
	let tree = TreeBuilder::with_options(options.clone()).build_async().await.unwrap();
	let model = layout(&tree).await;
	shape.advance(&tree);

	type Taken = Vec<(CompactionStage, CrashMode, FileSystem)>;
	let taken: Arc<Mutex<Taken>> = Arc::new(Mutex::new(Vec::new()));
	{
		let taken = Arc::clone(&taken);
		let fs = fs.clone();
		*tree.core.inner.compaction_stage_hook.lock() = Some(Arc::new(move |stage| {
			for mode in [
				CrashMode::Prefix(usize::MAX),
				CrashMode::LoseUnsynced,
				CrashMode::ZeroTail,
				CrashMode::Prefix(0),
			] {
				taken.lock().unwrap().push((stage, mode, image(&fs, mode)));
			}
			Ok(())
		}));
	}
	compact(&tree, shape.source(), shape.source() + 1).unwrap();
	*tree.core.inner.compaction_stage_hook.lock() = None;
	verify(&tree, &model, &format!("{shape:?}: the tree that compacted"));
	crash(tree, &fs);

	let images = std::mem::take(&mut *taken.lock().unwrap());
	let stages: BTreeSet<String> = images.iter().map(|(s, _, _)| format!("{s:?}")).collect();
	assert_eq!(stages.len(), 4, "{shape:?}: an image at every stage: {stages:?}");
	for (stage, mode, crashed) in images {
		let what = format!("{shape:?} at {stage:?} ({mode:?})");
		let tree = open_image(&options, &crashed, &what).await;
		verify(&tree, &model, &what);
		// Compaction goes on from what recovery left, and the result survives another crash.
		compact(&tree, 0, 1).unwrap();
		verify(&tree, &model, &format!("{what}: compacted again"));
		let again = image(&crashed, CrashMode::LoseUnsynced);
		crash(tree, &crashed);
		let what = format!("{what}, second crash");
		let tree = open_image(&options, &again, &what).await;
		verify(&tree, &model, &what);
		tree.close().await.unwrap();
	}
}

#[test(tokio::test)]
async fn the_images_at_every_stage_of_a_compaction_from_l0_open_to_the_acknowledged_data() {
	within("L0 to L1", stage_images(Shape::L0ToL1)).await;
}

#[test(tokio::test)]
async fn the_images_at_every_stage_of_a_compaction_from_l1_open_to_the_acknowledged_data() {
	within("L1 to L2", stage_images(Shape::L1ToL2)).await;
}

#[test(tokio::test)]
async fn the_images_at_every_stage_of_a_compaction_to_the_bottom_level_open_to_the_data() {
	within("L2 to L3", stage_images(Shape::L2ToL3)).await;
}
