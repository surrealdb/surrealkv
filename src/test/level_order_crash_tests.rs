//! Level ordering and read precedence under a crash, and a randomised crash sweep.
//!
//! A read looks at the active memtable, the immutable memtables newest first, the L0 tables in
//! the order the manifest lists them (largest sequence number first) and then at one table of
//! each deeper level, and the first version it finds wins. Everything here rests on that order
//! being the order of the sequence numbers, never of the level positions or of the table ids, and
//! on every manifest the tree writes, and every state a crash leaves behind, keeping it.
//!
//! A crash image is a copy of the database directory taken at a chosen instant (between
//! operations, or at one of the stages of a compaction on the thread that drives it), with the
//! SSTables that were never fsynced truncated to nothing. Each image is opened in a fresh tree
//! and has to read back exactly the model of the instant it was taken, and to satisfy the
//! structural invariants of `check_invariants` (unique table ids, L0 newest first, sorted and
//! disjoint L1+ tables, table metadata that matches the table, `last_sequence` covering every
//! table, no table file missing and no stray one left).
//!
//! Groups:
//!
//! * precedence by sequence: one key rewritten in the memtables, in several L0 tables, in L1 and in
//!   L2 (`a_value_rewritten_in_every_layer...`), a tombstone in L0 over a value in L2 and a re-put
//!   over a tombstone (`a_tombstone_in_l0_over_a_value_in_l2...`), range deletes that span levels
//!   (`range_deletes_spanning_levels...`), and reads at an older snapshot;
//! * the bounds of a range delete (start included, end excluded) in each layer
//!   (`a_range_delete_hides_its_start_and_not_its_end...`), a tombstone that has to stay above a
//!   value in the bottom level (`a_tombstone_is_kept_above_a_value_in_the_bottom_level...`), and a
//!   range delete that is not published yet, which a compaction must not act on
//!   (`a_compaction_keeps_the_versions_that_a_range_delete_not_published_yet_covers`);
//! * precedence is not by table id: a memtable sealed before a compaction takes its table id
//!   earlier than the compaction's outputs, and is flushed after it (`a_table_flushed_during...`),
//!   in a tree whose L0 is also its bottom level, where the compaction's output is in L0 too
//!   (`in_a_one_level_tree...`);
//! * level layout after a crash at each compaction stage, for workloads with several tables in
//!   every level (`every_compaction_stage_image_of_a_multi_table_workload...`);
//! * the next-level tables that share only a boundary key with a compaction's input, and the
//!   versions that snapshots retain, which an output must not cut in two
//!   (`a_compaction_takes_the_next_level_tables...`, `all_the_versions_of_a_key...`), and a
//!   bottom-level table that a range delete emptied (`a_bottom_level_table_emptied...`, and the
//!   ignored `a_scan_works_in_a_tree_with_a_bottom_level_table_emptied...`);
//! * L0 order across a restart: a compaction of a subset of L0, the WAL segments of several
//!   memtables flushed by recovery, a flush after the reopen (`the_remaining_l0_tables...`,
//!   `recovery_flushes_of_several_wal_segments...`, `a_flush_after_a_reopen...`);
//! * the manifest on disk lists L0 in sequence order after every step and a load keeps the order
//!   (`the_manifest_on_disk_lists_l0...`); `last_sequence` is restored so that a write after a
//!   reopen outranks every table (`last_sequence_is_restored...`);
//! * a memtable that holds only a range deletion, flushed and compacted into L1, and the range
//!   delete whose end key is one byte long, which cannot be flushed (`a_memtable_holding...`,
//!   `a_range_delete_flushed_on_its_own...`);
//! * a deterministic randomised sweep against a `BTreeMap` model, one test per seed
//!   (`sweep_seed_*`).
//!
//! The manifest load does not re-sort L0 or L1+: it keeps the order the manifest lists, so what
//! is guaranteed, and tested here, is that every manifest that is written lists the tables in
//! the order the invariants require, and that a load and a reopen keep it.
//!
//! Every tree here has its background tasks stopped right after it is opened: flushes and
//! compactions happen only where a test calls them, and the stage hooks only see those.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tempdir::TempDir;
use test_log::test;

use super::collect_transaction_all;
use crate::compaction::compactor::{CompactionStage, CompactionStageHook};
use crate::compaction::leveled::Strategy;
use crate::compaction::{CompactionChoice, CompactionInput, CompactionStrategy};
use crate::levels::{LevelManifest, Levels};
use crate::sstable::table::Table;
use crate::vfs::sync_tracker;
use crate::{LSMIterator, Mode, Options, Transaction, Tree};

/// How long a commit may take before the test calls it hung.
const WATCHDOG: Duration = Duration::from_secs(30);

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

const ALL_STAGES: [CompactionStage; 4] = [
	CompactionStage::OutputsDurable,
	CompactionStage::ManifestWritten,
	CompactionStage::InputsRemoved,
	CompactionStage::VlogCleaned,
];

// ---------------------------------------------------------------------------
// helpers: trees, images, hooks
// ---------------------------------------------------------------------------

/// The size of the tables a compaction writes, and the depth of the tree.
#[derive(Clone, Copy)]
struct Shape {
	target_file_size: u64,
	level_count: u8,
}

impl Shape {
	/// Outputs split every few hundred bytes, so that a level holds several tables.
	const SPLIT: Shape = Shape {
		target_file_size: 700,
		level_count: 4,
	};
	/// Outputs split every few hundred bytes, in a tree of L0 to L2.
	const SPLIT_SHALLOW: Shape = Shape {
		target_file_size: 700,
		level_count: 3,
	};
	/// One output table per compaction, in a tree of L0 to L3.
	const WIDE: Shape = Shape {
		target_file_size: 64 << 20,
		level_count: 4,
	};
}

/// Options of a tree that never compacts or stalls by itself: the tests call every flush and
/// compaction. Every tree gets its own options, and with them its own block cache: a cache
/// shared between two directories would serve the blocks of table N of one as those of the other.
fn options(path: &Path, shape: Shape) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		// Crash semantics: nothing may be flushed on drop or close.
		flush_on_close: false,
		enable_vlog: false,
		max_memtable_size: 4 << 20,
		level_count: shape.level_count,
		target_file_size: shape.target_file_size,
		level0_max_files: 10_000,
		memtable_stall_threshold: 10_000,
		l0_stall_threshold: 10_000,
		..Default::default()
	})
}

/// Opens a tree and stops its background tasks before they ran: nothing flushes or compacts
/// unless the test does. The commit flusher stays, so commits work.
async fn open_stopped(opts: Arc<Options>) -> Tree {
	let tree = Tree::new(opts).unwrap();
	let tasks = tree.core.task_manager.lock().unwrap().take().unwrap();
	tasks.stop().await;
	tree
}

/// Abandons a tree the way a crash does, without a flush and without a close.
fn finish(tree: Tree) {
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
	tree.core.is_closed.store(true, Ordering::SeqCst);
	drop(tree);
}

fn copy_dir(src: &Path, dst: &Path) {
	std::fs::create_dir_all(dst).unwrap();
	for entry in std::fs::read_dir(src).unwrap() {
		let entry = entry.unwrap();
		if entry.file_name() == "LOCK" {
			continue;
		}
		let to = dst.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &to);
		} else {
			std::fs::copy(entry.path(), &to).unwrap();
		}
	}
}

/// A deterministic generator, so that a failing seed can be replayed.
struct Rng(u64);

impl Rng {
	fn next(&mut self) -> u64 {
		self.0 ^= self.0 << 13;
		self.0 ^= self.0 >> 7;
		self.0 ^= self.0 << 17;
		self.0
	}

	fn below(&mut self, n: usize) -> usize {
		(self.next() % n as u64) as usize
	}
}

/// A crash image and the model of the instant it was taken.
struct Image {
	path: PathBuf,
	label: String,
	model: Model,
	universe: BTreeSet<Vec<u8>>,
}

/// Copies `src` the way a crash leaves it: the SSTables that no fsync made durable are cut to
/// nothing in the copy (the manifest and the WAL are not tracked, and stay as they are).
fn copy_image(
	src: &Path,
	root: &Path,
	n: usize,
	label: String,
	model: &Model,
	universe: &BTreeSet<Vec<u8>>,
) -> Image {
	let path = root.join(format!("image{n}"));
	copy_dir(src, &path);
	if let Ok(entries) = std::fs::read_dir(src.join("sstables")) {
		for entry in entries {
			let original = entry.unwrap().path();
			if original.extension().is_some_and(|e| e == "sst") {
				let copy = path.join("sstables").join(original.file_name().unwrap());
				if sync_tracker::was_synced(&original) {
					// What survives the crash is durable from then on: a tree opened on the
					// image and copied again must not lose it twice.
					sync_tracker::record(&copy);
				} else {
					std::fs::OpenOptions::new().write(true).open(copy).unwrap().set_len(0).unwrap();
				}
			}
		}
	}
	Image {
		path,
		label,
		model: model.clone(),
		universe: universe.clone(),
	}
}

/// The images a compaction took, by stage.
type Taken = Arc<Mutex<Vec<(CompactionStage, Image)>>>;

/// Installs a hook that takes a crash image of `src` at every stage of the next compaction, after
/// running `during`.
fn install_image_hook(db: &Db, during: impl Fn(CompactionStage) + Send + Sync + 'static) -> Taken {
	let taken: Taken = Arc::default();
	let (src, root) = (db.path.clone(), db.images.path().to_path_buf());
	let (counter, model, universe) =
		(Arc::clone(&db.next_image), db.model.clone(), db.universe.clone());
	let sink = Arc::clone(&taken);
	let hook: CompactionStageHook = Arc::new(move |stage| {
		during(stage);
		let n = counter.fetch_add(1, Ordering::SeqCst);
		let image = copy_image(&src, &root, n, format!("{stage:?}"), &model, &universe);
		sink.lock().unwrap().push((stage, image));
		Ok(())
	});
	*db.tree.core.inner.compaction_stage_hook.lock() = Some(hook);
	taken
}

/// What a test writes: a tree, the model of what it holds, and the directories of its images.
struct Db {
	_dir: TempDir,
	images: TempDir,
	path: PathBuf,
	tree: Tree,
	model: Model,
	universe: BTreeSet<Vec<u8>>,
	next_image: Arc<AtomicUsize>,
}

/// A write of a batch.
#[derive(Clone, Debug)]
enum Op {
	Put(Vec<u8>, Vec<u8>),
	Del(Vec<u8>),
}

impl Db {
	async fn open(shape: Shape) -> Db {
		let dir = TempDir::new("level_order").unwrap();
		let path = dir.path().to_path_buf();
		let tree = open_stopped(options(&path, shape)).await;
		Db {
			_dir: dir,
			images: TempDir::new("level_order_images").unwrap(),
			path,
			tree,
			model: Model::new(),
			universe: BTreeSet::new(),
			next_image: Arc::new(AtomicUsize::new(0)),
		}
	}

	/// Continues from `image` in a reopened tree (the image directory is written to).
	async fn continue_from(image: &Image, shape: Shape) -> Db {
		let tree = reopen(image, shape).await;
		Db {
			_dir: TempDir::new("level_order").unwrap(),
			images: TempDir::new("level_order_images").unwrap(),
			path: image.path.clone(),
			tree,
			model: image.model.clone(),
			universe: image.universe.clone(),
			next_image: Arc::new(AtomicUsize::new(0)),
		}
	}

	async fn commit(&self, mut txn: Transaction) {
		tokio::time::timeout(WATCHDOG, txn.commit())
			.await
			.expect("a commit never finished")
			.unwrap();
	}

	/// Writes `key` with a value that says which write it is.
	async fn put(&mut self, key: &str, tag: &str) {
		self.put_padded(key, tag, 0).await;
	}

	async fn put_padded(&mut self, key: &str, tag: &str, pad: usize) {
		let value = value_of(key, tag, pad);
		let mut txn = self.tree.begin().unwrap();
		txn.set(key.as_bytes(), value.as_slice()).unwrap();
		self.commit(txn).await;
		self.universe.insert(key.as_bytes().to_vec());
		self.model.insert(key.as_bytes().to_vec(), value);
	}

	async fn del(&mut self, key: &str) {
		let mut txn = self.tree.begin().unwrap();
		txn.delete(key.as_bytes()).unwrap();
		self.commit(txn).await;
		self.universe.insert(key.as_bytes().to_vec());
		self.model.remove(key.as_bytes());
	}

	/// A range delete is a transaction of its own.
	async fn del_range(&mut self, start: &str, end: &str) {
		let mut txn = self.tree.begin().unwrap();
		txn.delete_range(start.as_bytes(), end.as_bytes()).unwrap();
		self.commit(txn).await;
		self.universe.insert(start.as_bytes().to_vec());
		let doomed: Vec<Vec<u8>> = self
			.model
			.range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
			.map(|(k, _)| k.clone())
			.collect();
		for key in doomed {
			self.model.remove(&key);
		}
	}

	/// Writes every key of `keys` in one transaction.
	async fn put_all(&mut self, keys: &[&str], tag: &str) {
		let ops: Vec<Op> =
			keys.iter().map(|k| Op::Put(k.as_bytes().to_vec(), value_of(k, tag, 0))).collect();
		self.batch(ops).await;
	}

	/// A transaction of several writes to distinct keys.
	async fn batch(&mut self, ops: Vec<Op>) {
		let mut txn = self.tree.begin().unwrap();
		for op in &ops {
			match op {
				Op::Put(k, v) => txn.set(k.as_slice(), v.as_slice()).unwrap(),
				Op::Del(k) => txn.delete(k.as_slice()).unwrap(),
			}
		}
		self.commit(txn).await;
		for op in ops {
			match op {
				Op::Put(k, v) => {
					self.universe.insert(k.clone());
					self.model.insert(k, v);
				}
				Op::Del(k) => {
					self.universe.insert(k.clone());
					self.model.remove(&k);
				}
			}
		}
	}

	/// Seals the active memtable: it becomes an immutable one with a table id and a WAL segment.
	fn rotate(&self) {
		self.tree.core.inner.rotate_memtable().unwrap();
	}

	/// Flushes the active and the immutable memtables to L0 tables, oldest first.
	fn flush(&self) {
		self.tree.flush().unwrap();
	}

	/// Moves what the WAL buffers hold into the file, where a copy of the directory sees it. An
	/// fsync would make the same images (the files are read through the page cache) and cost the
	/// disk of a busy machine a sync for every image.
	fn flush_wal_buffers(&self) {
		self.tree.flush_wal(false).unwrap();
	}

	fn layout(&self) -> Vec<Vec<u64>> {
		layout(&self.tree)
	}

	fn immutables(&self) -> usize {
		self.tree.core.inner.immutable_count()
	}

	/// A point read of the live tree.
	fn get(&self, key: &str) -> Option<Vec<u8>> {
		let txn = self.tree.begin_with_mode(Mode::ReadOnly).unwrap();
		txn.get(key.as_bytes()).unwrap()
	}

	/// Compacts `source` into the level below it with the tables the real strategy selects for
	/// it, whatever the scores say, and checks that the compaction merged something.
	fn compact_level(&self, source: u8) {
		let before = self.layout();
		self.tree.compact(Arc::new(Forced::new(&self.tree, source))).unwrap();
		assert_ne!(before, self.layout(), "the compaction of level {source} merged nothing");
	}

	/// A crash image of this instant: the WAL buffers are flushed first, so what was acknowledged
	/// is there.
	fn image(&self, label: &str) -> Image {
		self.flush_wal_buffers();
		let n = self.next_image.fetch_add(1, Ordering::SeqCst);
		copy_image(
			&self.path,
			self.images.path(),
			n,
			label.to_string(),
			&self.model,
			&self.universe,
		)
	}

	/// Runs a compaction with a crash image taken at each of its stages, on this thread. `during`
	/// runs at every stage before the image is taken. Returns the images by stage, and fails if
	/// the compaction did not reach all four.
	fn compact_with_images(
		&self,
		strategy: impl CompactionStrategy + 'static,
		during: impl Fn(CompactionStage) + Send + Sync + 'static,
	) -> Vec<(CompactionStage, Image)> {
		self.flush_wal_buffers();
		let taken = install_image_hook(self, during);
		let result = self.tree.compact(Arc::new(strategy));
		*self.tree.core.inner.compaction_stage_hook.lock() = None;
		result.unwrap();
		let images = std::mem::take(&mut *taken.lock().unwrap());
		let stages: Vec<CompactionStage> = images.iter().map(|(s, _)| *s).collect();
		assert_eq!(
			stages, ALL_STAGES,
			"the compaction did not reach every stage (a Skip, or a hook that never fired)"
		);
		images
	}

	/// Reads every key of the universe, by point reads on both read paths and by a scan, and
	/// compares with the model.
	async fn assert_reads(&self, ctx: &str) {
		let txn = self.tree.begin_with_mode(Mode::ReadOnly).unwrap();
		assert_reads_in(&txn, &self.model, &self.universe, ctx).await;
	}

	/// A read-only transaction, and the model it must keep reading.
	fn snapshot(&self) -> (Transaction, Model, BTreeSet<Vec<u8>>) {
		(
			self.tree.begin_with_mode(Mode::ReadOnly).unwrap(),
			self.model.clone(),
			self.universe.clone(),
		)
	}
}

/// The value of a write: it names the key and the write, and is `pad` bytes longer.
fn value_of(key: &str, tag: &str, pad: usize) -> Vec<u8> {
	let mut value = format!("{tag}:{key}").into_bytes();
	value.extend(std::iter::repeat_n(b'.', pad));
	value
}

/// The table ids of each level, in the order the manifest holds them.
fn layout(tree: &Tree) -> Vec<Vec<u64>> {
	tree.core
		.inner
		.level_manifest
		.read()
		.unwrap()
		.levels
		.get_levels()
		.iter()
		.map(|level| level.tables.iter().map(|t| t.id).collect())
		.collect()
}

/// Whether a table of level `level` holds a tombstone of the user key `key`.
fn level_holds_tombstone(tree: &Tree, level: usize, key: &str) -> bool {
	let manifest = tree.core.inner.level_manifest.read().unwrap();
	for table in manifest.levels.get_levels()[level].tables.iter() {
		let mut iter = table.iter(None).unwrap();
		iter.seek_first().unwrap();
		while iter.valid() {
			let entry = iter.key();
			if entry.user_key() == key.as_bytes() && entry.is_tombstone() {
				return true;
			}
			if !iter.next().unwrap() {
				break;
			}
		}
	}
	false
}

fn text(value: Option<&[u8]>) -> Option<String> {
	value.map(|v| String::from_utf8_lossy(v).into_owned())
}

/// The point reads of `txn` (on both read paths) against `model`, for every key of `universe`.
async fn assert_point_reads_in(
	txn: &Transaction,
	model: &Model,
	universe: &BTreeSet<Vec<u8>>,
	ctx: &str,
) {
	for key in universe {
		let want = model.get(key).cloned();
		let name = String::from_utf8_lossy(key);
		let got = txn.get(key.as_slice()).unwrap();
		assert_eq!(text(got.as_deref()), text(want.as_deref()), "{ctx}: get of {name:?}");
		let got = txn.get_async(key.as_slice()).await.unwrap();
		assert_eq!(text(got.as_deref()), text(want.as_deref()), "{ctx}: get_async of {name:?}");
	}
}

/// The scan of `txn` against `model`.
fn assert_scan_in(txn: &Transaction, model: &Model, ctx: &str) {
	let mut iter = txn.iter().unwrap();
	let got = collect_transaction_all(&mut iter).unwrap();
	let show = |entries: &[(Vec<u8>, Vec<u8>)]| -> Vec<(String, String)> {
		entries
			.iter()
			.map(|(k, v)| {
				(String::from_utf8_lossy(k).into_owned(), String::from_utf8_lossy(v).into_owned())
			})
			.collect()
	};
	let want: Vec<(Vec<u8>, Vec<u8>)> = model.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
	assert_eq!(show(&got), show(&want), "{ctx}: the scan");
}

/// The point reads and the scan of `txn` against `model`, for every key of `universe`.
async fn assert_reads_in(
	txn: &Transaction,
	model: &Model,
	universe: &BTreeSet<Vec<u8>>,
	ctx: &str,
) {
	assert_point_reads_in(txn, model, universe, ctx).await;
	assert_scan_in(txn, model, ctx);
}

// ---------------------------------------------------------------------------
// helpers: strategies
// ---------------------------------------------------------------------------

/// Compacts `source` into `source + 1` whatever the scores say, with the tables the real
/// strategy selects: all of L0 and what it overlaps in L1, or the best table of a deeper level
/// and what it overlaps.
struct Forced {
	source: u8,
	selector: Strategy,
}

impl Forced {
	fn new(tree: &Tree, source: u8) -> Self {
		Self {
			source,
			selector: Strategy::from_options(Arc::clone(&tree.core.inner.opts)),
		}
	}
}

impl CompactionStrategy for Forced {
	fn pick_levels(&self, manifest: &LevelManifest) -> crate::Result<CompactionChoice> {
		let levels = manifest.levels.get_levels();
		let (source, target) = (self.source as usize, self.source as usize + 1);
		let tables_to_merge = self.selector.select_tables_for_compaction(
			&levels[source],
			&levels[target],
			self.source,
		)?;
		if tables_to_merge.is_empty() {
			return Ok(CompactionChoice::Skip);
		}
		Ok(CompactionChoice::Merge(CompactionInput {
			tables_to_merge,
			source_level: self.source,
			target_level: self.source + 1,
		}))
	}
}

/// Compacts the L0 tables `ids` (which must be the oldest ones) and what they overlap in L1.
struct L0Subset {
	ids: Vec<u64>,
}

impl CompactionStrategy for L0Subset {
	fn pick_levels(&self, manifest: &LevelManifest) -> crate::Result<CompactionChoice> {
		let levels = manifest.levels.get_levels();
		let chosen: Vec<&Arc<Table>> =
			levels[0].tables.iter().filter(|t| self.ids.contains(&t.id)).collect();
		assert_eq!(chosen.len(), self.ids.len(), "an L0 table of the subset is not in L0");
		let mut tables_to_merge = self.ids.clone();
		if let Some(range) = Strategy::combined_key_range(chosen.iter().copied()) {
			for table in levels[1].overlapping_tables(&range) {
				tables_to_merge.push(table.id);
			}
		}
		Ok(CompactionChoice::Merge(CompactionInput {
			tables_to_merge,
			source_level: 0,
			target_level: 1,
		}))
	}
}

// ---------------------------------------------------------------------------
// helpers: invariants
// ---------------------------------------------------------------------------

/// The header and the levels of a manifest file.
struct DiskManifest {
	next_table_id: u64,
	last_sequence: u64,
	levels: Vec<Vec<u64>>,
}

fn read_manifest(dir: &Path) -> DiskManifest {
	let data = std::fs::read(dir.join("manifest").join(format!("{:020}.manifest", 0))).unwrap();
	let mut cursor = std::io::Cursor::new(&data[..]);
	let mut u16_buf = [0u8; 2];
	let mut u64_buf = [0u8; 8];
	cursor.read_exact(&mut u16_buf).unwrap();
	cursor.read_exact(&mut u64_buf).unwrap();
	let next_table_id = u64::from_be_bytes(u64_buf);
	cursor.read_exact(&mut u64_buf).unwrap(); // log number
	cursor.read_exact(&mut u64_buf).unwrap();
	let last_sequence = u64::from_be_bytes(u64_buf);
	let levels = Levels::decode(&mut cursor).unwrap();
	DiskManifest {
		next_table_id,
		last_sequence,
		levels,
	}
}

fn sst_ids_on_disk(dir: &Path) -> BTreeSet<u64> {
	let mut ids = BTreeSet::new();
	for entry in std::fs::read_dir(dir.join("sstables")).unwrap() {
		let name = entry.unwrap().file_name().to_string_lossy().to_string();
		if let Some(stem) = name.strip_suffix(".sst") {
			ids.insert(stem.parse::<u64>().unwrap_or_else(|_| panic!("odd table file {name}")));
		}
	}
	ids
}

/// The structural invariants of the levels of `tree`, at a point where no compaction is running.
///
/// * a table id is listed once in the whole manifest;
/// * L0 is ordered by the rule of `Level::insert`: the largest sequence number of a table, newest
///   first;
/// * the tables of L1 and deeper that hold point keys are sorted by smallest key, and their key
///   ranges are pairwise disjoint (all versions of a key are in one table);
/// * the key range and the sequence range in the metadata of a table are what the table holds, and
///   `last_sequence` is at least the largest sequence number of every table;
/// * the manifest on disk lists what the manifest in memory does, and its `next_table_id` is past
///   every table id;
/// * the tables on disk are the tables the manifest lists: none missing, none stray;
/// * no table is left hidden by a compaction.
fn check_invariants(tree: &Tree, ctx: &str) {
	let inner = &tree.core.inner;
	let manifest = inner.level_manifest.read().unwrap();
	let mut ids = HashSet::new();
	let mut max_id = 0;
	let mut max_seq = 0;
	let mut range_seqs = Vec::new();
	assert!(manifest.hidden_set.is_empty(), "{ctx}: tables are left hidden by a compaction");
	for (li, level) in manifest.levels.get_levels().iter().enumerate() {
		for table in level.tables.iter() {
			assert!(
				ids.insert(table.id),
				"{ctx}: table {} is listed twice (again in L{li})",
				table.id
			);
			max_id = max_id.max(table.id);
			let (lo, hi) = (
				table.meta.smallest_seq_num.expect("a table has a smallest sequence number"),
				table.meta.largest_seq_num.expect("a table has a largest sequence number"),
			);
			assert_eq!(
				table.meta.properties.seqnos,
				(lo, hi),
				"{ctx}: table {} properties disagree with its sequence metadata",
				table.id
			);
			assert!(lo <= hi, "{ctx}: table {} has seq {lo} > {hi}", table.id);
			max_seq = max_seq.max(hi);
			for (_, _, seq) in table.range_deletions.read().iter() {
				range_seqs.push(*seq);
			}
			check_table_content(table, ctx);
		}
		if li == 0 {
			for pair in level.tables.windows(2) {
				assert!(
					pair[0].meta.properties.seqnos.1 >= pair[1].meta.properties.seqnos.1,
					"{ctx}: L0 is not newest first: table {} (largest seq {}) precedes table {} \
					 (largest seq {})",
					pair[0].id,
					pair[0].meta.properties.seqnos.1,
					pair[1].id,
					pair[1].meta.properties.seqnos.1
				);
			}
		} else {
			let with_points: Vec<&Arc<Table>> =
				level.tables.iter().filter(|t| t.meta.smallest_point.is_some()).collect();
			for pair in with_points.windows(2) {
				let prev = pair[0].meta.largest_point.as_ref().unwrap();
				let next = pair[1].meta.smallest_point.as_ref().unwrap();
				assert!(
					prev.user_key < next.user_key,
					"{ctx}: L{li} tables {} (largest {:?}) and {} (smallest {:?}) are not sorted \
					 and disjoint",
					pair[0].id,
					String::from_utf8_lossy(&prev.user_key),
					pair[1].id,
					String::from_utf8_lossy(&next.user_key)
				);
			}
		}
	}
	assert!(
		manifest.last_sequence >= max_seq,
		"{ctx}: last_sequence {} is below the largest sequence number of a table, {max_seq}",
		manifest.last_sequence
	);
	let visible = inner.visible_seq_num.load(Ordering::SeqCst);
	for seq in range_seqs {
		assert!(
			seq <= manifest.last_sequence.max(visible),
			"{ctx}: a range deletion at sequence {seq} is above last_sequence {}",
			manifest.last_sequence
		);
	}
	assert!(
		manifest.next_table_id.load(Ordering::SeqCst) > max_id,
		"{ctx}: next_table_id is not past table {max_id}"
	);
	assert!(
		visible >= manifest.last_sequence,
		"{ctx}: the visible sequence number {visible} is below last_sequence {}",
		manifest.last_sequence
	);

	// The manifest on disk is the one in memory.
	let disk = read_manifest(&inner.opts.path);
	let memory: Vec<Vec<u64>> = manifest
		.levels
		.get_levels()
		.iter()
		.map(|level| level.tables.iter().map(|t| t.id).collect())
		.collect();
	assert_eq!(disk.levels, memory, "{ctx}: the manifest on disk lists other tables than memory");
	assert_eq!(disk.last_sequence, manifest.last_sequence, "{ctx}: last_sequence on disk");
	assert!(disk.next_table_id > max_id, "{ctx}: next_table_id on disk is not past table {max_id}");

	// The files on disk are the tables of the manifest.
	let on_disk = sst_ids_on_disk(&inner.opts.path);
	let listed: BTreeSet<u64> = ids.into_iter().collect();
	assert_eq!(on_disk, listed, "{ctx}: the table files on disk are not the tables listed");
}

/// The key and sequence ranges of the metadata of `table` are those of its entries.
fn check_table_content(table: &Arc<Table>, ctx: &str) {
	let mut iter = table.iter(None).unwrap();
	iter.seek_first().unwrap();
	let (mut first, mut last): (Option<Vec<u8>>, Option<Vec<u8>>) = (None, None);
	let (mut lo, mut hi) = (u64::MAX, 0u64);
	let mut count = 0usize;
	while iter.valid() {
		let key = iter.key();
		let user_key = key.user_key().to_vec();
		if first.is_none() {
			first = Some(user_key.clone());
		}
		assert!(
			last.as_ref().is_none_or(|l| *l <= user_key),
			"{ctx}: table {} holds its keys out of order",
			table.id
		);
		last = Some(user_key);
		lo = lo.min(key.seq_num());
		hi = hi.max(key.seq_num());
		count += 1;
		if !iter.next().unwrap() {
			break;
		}
	}
	if count == 0 {
		assert!(
			table.meta.smallest_point.is_none() && table.meta.largest_point.is_none(),
			"{ctx}: table {} holds no points but its metadata has a key range",
			table.id
		);
		return;
	}
	let smallest = table.meta.smallest_point.as_ref().expect("a table with points has a range");
	let largest = table.meta.largest_point.as_ref().expect("a table with points has a range");
	assert_eq!(
		Some(&smallest.user_key),
		first.as_ref(),
		"{ctx}: table {} smallest key in its metadata is not the one it holds",
		table.id
	);
	assert_eq!(
		Some(&largest.user_key),
		last.as_ref(),
		"{ctx}: table {} largest key in its metadata is not the one it holds",
		table.id
	);
	let (meta_lo, meta_hi) =
		(table.meta.smallest_seq_num.unwrap(), table.meta.largest_seq_num.unwrap());
	assert!(
		meta_lo <= lo && hi <= meta_hi,
		"{ctx}: table {} holds sequence numbers {lo}..={hi} outside its metadata \
		 {meta_lo}..={meta_hi}",
		table.id
	);
}

/// Opens `image`, checks the invariants and every read against its model, and returns the tree
/// (stopped) for more checks; end it with `finish`.
async fn reopen(image: &Image, shape: Shape) -> Tree {
	let ctx = format!("image `{}`", image.label);
	let tree = open_stopped(options(&image.path, shape)).await;
	check_invariants(&tree, &ctx);
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	assert_reads_in(&txn, &image.model, &image.universe, &ctx).await;
	drop(txn);
	tree
}

/// `reopen`, and the tree is abandoned. Returns the layout after the open.
async fn verify(image: &Image, shape: Shape) -> Vec<Vec<u64>> {
	let tree = reopen(image, shape).await;
	let layout = layout(&tree);
	finish(tree);
	layout
}

/// The layout an image has: the inputs of a compaction until its manifest is written, the
/// outputs from then on.
fn expected_at(stage: CompactionStage, before: &[Vec<u64>], after: &[Vec<u64>]) -> Vec<Vec<u64>> {
	if stage == CompactionStage::OutputsDurable {
		before.to_vec()
	} else {
		after.to_vec()
	}
}

// ---------------------------------------------------------------------------
// precedence by sequence
// ---------------------------------------------------------------------------

const K: [&str; 10] = ["k00", "k01", "k02", "k03", "k04", "k05", "k06", "k07", "k08", "k09"];

/// One key set rewritten, generation after generation, so that every layer holds a version of
/// the same keys: L2 the oldest, then L1, four L0 tables, an immutable memtable and the active
/// one. Reads of the newest by sequence are checked at every step, in the live tree and in a
/// crash image, a reader that began mid-way keeps reading its own version through the flushes and
/// compactions, and every stage of an L0 to L1 and of an L1 to L2 compaction is imaged.
#[test(tokio::test)]
async fn a_value_rewritten_in_every_layer_reads_back_as_the_newest_by_sequence_at_every_step() {
	let mut db = Db::open(Shape::WIDE).await;

	// L2: generation A of every key.
	db.put_all(&K, "A").await;
	db.flush();
	db.compact_level(0);
	db.compact_level(1);
	assert_eq!(db.layout()[0].len() + db.layout()[1].len(), 0);
	assert!(!db.layout()[2].is_empty(), "generation A is in L2");
	db.assert_reads("A in L2").await;
	verify(&db.image("A in L2"), Shape::WIDE).await;

	// L1: generation B of k00..k07.
	db.put_all(&K[..8], "B").await;
	db.flush();
	db.compact_level(0);
	assert!(!db.layout()[1].is_empty() && !db.layout()[2].is_empty());
	db.assert_reads("B in L1, A in L2").await;
	verify(&db.image("B in L1, A in L2"), Shape::WIDE).await;

	// A reader that began here keeps reading generations A and B.
	let (old, old_model, old_universe) = db.snapshot();

	// L0: three tables rewriting the same keys, with a tombstone and a re-put in between.
	db.put_all(&K[..6], "C").await;
	db.flush();
	db.put("k03", "C2").await;
	db.del("k02").await;
	db.flush();
	db.put("k02", "C3").await;
	db.put("k09", "C3").await;
	db.flush();
	assert_eq!(db.layout()[0].len(), 3, "three L0 tables");
	db.assert_reads("three L0 tables over L1 over L2").await;
	assert_eq!(db.get("k03").as_deref(), Some(&b"C2:k03"[..]), "the newer L0 table wins");
	assert_eq!(db.get("k02").as_deref(), Some(&b"C3:k02"[..]), "a re-put over a tombstone wins");
	verify(&db.image("three L0 tables"), Shape::WIDE).await;

	// A range delete over keys that three layers hold.
	db.put("k08", "D").await;
	db.del_range("k04", "k07").await;
	db.flush();
	assert_eq!(db.layout()[0].len(), 4, "four L0 tables");
	db.assert_reads("a range delete in L0").await;
	assert_eq!(db.get("k04"), None);
	assert_eq!(db.get("k07").as_deref(), Some(&b"B:k07"[..]), "the end of a range is exclusive");

	// An immutable memtable and the active one on top of everything.
	db.put("k00", "E").await;
	db.put("k05", "E5").await;
	db.rotate();
	assert_eq!(db.immutables(), 1);
	db.put("k01", "F").await;
	db.del("k07").await;
	db.assert_reads("the active and an immutable memtable over four L0 tables").await;
	assert_eq!(db.get("k05").as_deref(), Some(&b"E5:k05"[..]), "a re-put over a range delete");
	assert_reads_in(&old, &old_model, &old_universe, "the old reader before the flush").await;
	// The WAL holds the two unflushed memtables; recovery turns them into two newer L0 tables.
	let tree = reopen(&db.image("all layers populated"), Shape::WIDE).await;
	assert_eq!(
		layout(&tree)[0].len(),
		4 + 1,
		"recovery flushes the sealed memtable to a newer L0 table and keeps the active one in memory"
	);
	finish(tree);

	// Everything to disk, then the compactions with an image at each stage.
	db.flush();
	assert_eq!(db.layout()[0].len(), 6);
	db.assert_reads("six L0 tables").await;
	assert_reads_in(&old, &old_model, &old_universe, "the old reader after the flush").await;
	let before = db.layout();
	let images = db.compact_with_images(Forced::new(&db.tree, 0), |_| {});
	let after = db.layout();
	assert!(after[0].is_empty() && !after[1].is_empty(), "L0 went to L1: {after:?}");
	for (stage, image) in &images {
		let layout = verify(image, Shape::WIDE).await;
		assert_eq!(layout, expected_at(*stage, &before, &after), "the image at {stage:?}");
	}
	db.assert_reads("after L0 to L1").await;
	assert_reads_in(&old, &old_model, &old_universe, "the old reader after L0 to L1").await;

	let before = db.layout();
	let images = db.compact_with_images(Forced::new(&db.tree, 1), |_| {});
	let after = db.layout();
	assert!(after[1].is_empty() && !after[2].is_empty(), "L1 went to L2: {after:?}");
	for (stage, image) in &images {
		let layout = verify(image, Shape::WIDE).await;
		assert_eq!(layout, expected_at(*stage, &before, &after), "the image at {stage:?}");
	}
	db.assert_reads("after L1 to L2").await;
	assert_reads_in(&old, &old_model, &old_universe, "the old reader after L1 to L2").await;
	check_invariants(&db.tree, "the driving tree at the end");
	drop(old);
	finish(db.tree);
}

/// A tombstone flushed to L0 shadows a value that is two levels down. Compacting it to L1 must
/// keep it (L2 is not the bottom of a four-level tree and still holds the value), a re-put over
/// the tombstone must win over it, and every crash image at every stage reads the model.
#[test(tokio::test)]
async fn a_tombstone_in_l0_over_a_value_in_l2_survives_an_l0_to_l1_compaction_and_every_crash_image(
) {
	let mut db = Db::open(Shape::WIDE).await;
	db.put_all(&["a", "b", "c", "d"], "old").await;
	db.flush();
	db.compact_level(0);
	db.compact_level(1);
	assert_eq!(db.layout()[2].len(), 1);

	// Tombstones for b and c, a rewrite of d, and a key that exists nowhere else.
	db.del("b").await;
	db.del("c").await;
	db.put("d", "new").await;
	db.put("e", "fresh").await;
	db.flush();
	db.assert_reads("tombstones in L0 over L2").await;
	assert_eq!(db.model.len(), 3);
	let image = db.image("tombstones in L0 over L2");
	assert_eq!(verify(&image, Shape::WIDE).await[0].len(), 1);

	// L0 to L1: the tombstones must go with the data to L1 and still shadow L2.
	let before = db.layout();
	let images = db.compact_with_images(Forced::new(&db.tree, 0), |_| {});
	for (stage, image) in &images {
		let layout = verify(image, Shape::WIDE).await;
		if *stage == CompactionStage::OutputsDurable {
			assert_eq!(layout, before);
		} else {
			assert!(layout[0].is_empty() && !layout[1].is_empty() && !layout[2].is_empty());
		}
	}
	db.assert_reads("tombstones in L1 over L2").await;
	assert_eq!(db.get("b"), None);
	assert_eq!(db.get("c"), None);

	// A re-put over the tombstone, flushed to L0 over L1 over L2: the newest wins.
	db.put("b", "again").await;
	db.flush();
	db.assert_reads("a re-put in L0 over a tombstone in L1 over a value in L2").await;
	assert_eq!(db.get("b").as_deref(), Some(&b"again:b"[..]));
	verify(&db.image("re-put over a tombstone"), Shape::WIDE).await;

	// The next compactions, imaged at every stage: re-put, then deletes again.
	for (_, image) in &db.compact_with_images(Forced::new(&db.tree, 0), |_| {}) {
		verify(image, Shape::WIDE).await;
	}
	db.del("b").await;
	db.flush();
	db.assert_reads("deleted again").await;
	for (_, image) in &db.compact_with_images(Forced::new(&db.tree, 0), |_| {}) {
		verify(image, Shape::WIDE).await;
	}
	for (_, image) in &db.compact_with_images(Forced::new(&db.tree, 1), |_| {}) {
		verify(image, Shape::WIDE).await;
	}
	db.assert_reads("everything in L2").await;
	check_invariants(&db.tree, "end");
	finish(db.tree);
}

/// Range deletes in different layers, over keys that other layers hold, in both directions: a
/// range delete newer than the data it covers hides it wherever it is, and one older than a
/// rewrite does not hide the rewrite.
#[test(tokio::test)]
async fn range_deletes_spanning_levels_hide_exactly_the_older_versions_in_every_crash_image() {
	let mut db = Db::open(Shape::WIDE).await;
	let keys: Vec<String> = (0..12).map(|i| format!("r{i:02}")).collect();
	let refs: Vec<&str> = keys.iter().map(String::as_str).collect();

	// L2: every key. L1: a rewrite of r00..r07 with a range delete over r02..r05 (which then
	// covers the L2 versions and the L1 rewrites of its own table).
	db.put_all(&refs, "L2").await;
	db.flush();
	db.compact_level(0);
	db.compact_level(1);
	db.put_all(&refs[..8], "L1").await;
	db.del_range("r02", "r05").await;
	db.flush();
	db.compact_level(0);
	assert!(!db.layout()[1].is_empty() && !db.layout()[2].is_empty());
	db.assert_reads("range delete in L1 over L1 and L2").await;
	assert!(db.get("r03").is_none() && db.get("r05").is_some());
	verify(&db.image("range delete in L1"), Shape::WIDE).await;

	// L0: a re-put inside the deleted range (newer than the range delete), a range delete over
	// r06..r10 that the L1 rewrites and the L2 versions are older than, and a put over it.
	db.put("r03", "L0a").await;
	db.flush();
	db.del_range("r06", "r10").await;
	db.put("r08", "L0b").await;
	db.flush();
	db.assert_reads("range deletes in L0 and L1").await;
	assert_eq!(db.get("r03").as_deref(), Some(&b"L0a:r03"[..]));
	assert_eq!(db.get("r08").as_deref(), Some(&b"L0b:r08"[..]));
	assert!(db.get("r06").is_none() && db.get("r09").is_none());
	assert!(db.get("r10").is_some(), "the end of a range is exclusive");
	let (old, old_model, old_universe) = db.snapshot();
	verify(&db.image("range deletes in L0 and L1"), Shape::WIDE).await;

	// A range delete in the memtable over everything the layers below hold.
	db.put("r00", "mem").await;
	db.del_range("r01", "r04").await;
	db.assert_reads("a range delete in the memtable").await;
	verify(&db.image("range delete in the memtable"), Shape::WIDE).await;
	db.rotate();
	db.put("r11", "mem2").await;
	db.assert_reads("a range delete in an immutable memtable").await;
	verify(&db.image("range delete in an immutable memtable"), Shape::WIDE).await;

	// Compactions with an image at every stage; the model never changes.
	db.flush();
	for source in [0u8, 1] {
		for (_, image) in db.compact_with_images(Forced::new(&db.tree, source), |_| {}) {
			assert_eq!(image.model, db.model);
			verify(&image, Shape::WIDE).await;
		}
		db.assert_reads(&format!("after compacting L{source}")).await;
		assert_reads_in(&old, &old_model, &old_universe, "the old reader").await;
	}
	check_invariants(&db.tree, "end");
	drop(old);
	finish(db.tree);
}

/// The same range delete in each layer in turn, over older versions that L2 holds: it hides its
/// start key and the keys up to, but not including, its end key, it leaves the key before its start
/// alone, wherever it sits (the active memtable, an immutable one, L0 or L1), and so does the crash
/// image of each of them.
#[test(tokio::test)]
async fn a_range_delete_hides_its_start_and_not_its_end_in_whichever_layer_holds_it() {
	for place in ["the active memtable", "an immutable memtable", "L0", "L1"] {
		let mut db = Db::open(Shape::WIDE).await;
		db.put_all(&["d0", "e0", "f0", "g0", "h0"], "old").await;
		db.flush();
		db.compact_level(0);
		db.compact_level(1);
		assert!(!db.layout()[2].is_empty());
		db.del_range("e0", "g0").await;
		match place {
			"the active memtable" => {}
			"an immutable memtable" => db.rotate(),
			"L0" => db.flush(),
			_ => {
				db.flush();
				db.compact_level(0);
			}
		}
		let ctx = format!("a range delete in {place}");
		db.assert_reads(&ctx).await;
		assert_eq!(
			db.get("d0").as_deref(),
			Some(&b"old:d0"[..]),
			"{ctx}: the key before the start"
		);
		assert_eq!(db.get("e0"), None, "{ctx}: the start key");
		assert_eq!(db.get("f0"), None, "{ctx}: a key inside");
		assert_eq!(db.get("g0").as_deref(), Some(&b"old:g0"[..]), "{ctx}: the end key stays");
		assert_eq!(db.get("h0").as_deref(), Some(&b"old:h0"[..]), "{ctx}: the key after the end");
		verify(&db.image(&ctx), Shape::WIDE).await;
		finish(db.tree);
	}
}

/// A tombstone has to stay above a value that a lower level still holds: with the value in L3 (the
/// bottom level of a four-level tree), the compactions of the tombstone from L0 to L1 and from L1
/// to L2 must keep it, in the table and in every crash image, and only the compaction that merges
/// it into L3 may drop it together with the value it hides.
#[test(tokio::test)]
async fn a_tombstone_is_kept_above_a_value_in_the_bottom_level_until_it_reaches_it() {
	let mut db = Db::open(Shape::WIDE).await;
	db.put_all(&["t0", "t1", "t2"], "bottom").await;
	db.flush();
	for source in 0..3 {
		db.compact_level(source);
	}
	let layout = db.layout();
	assert!(layout[..3].iter().all(Vec::is_empty) && !layout[3].is_empty(), "L3 only: {layout:?}");

	db.del("t1").await;
	db.put("t2", "new").await;
	db.flush();
	db.assert_reads("a tombstone in L0 over a value in L3").await;
	for source in 0..2u8 {
		let ctx = format!("the tombstone after its compaction from L{source}");
		let images = db.compact_with_images(Forced::new(&db.tree, source), |_| {});
		assert!(
			level_holds_tombstone(&db.tree, source as usize + 1, "t1"),
			"{ctx}: the tombstone is not in L{} above the value in L3",
			source + 1
		);
		for (stage, image) in &images {
			verify(image, Shape::WIDE).await;
			assert_eq!(image.model, db.model, "{ctx}: {stage:?}");
		}
		db.assert_reads(&ctx).await;
		assert_eq!(db.get("t1"), None, "{ctx}");
	}
	// It meets the value it hides in L3 now, and goes with it.
	for (_, image) in &db.compact_with_images(Forced::new(&db.tree, 2), |_| {}) {
		verify(image, Shape::WIDE).await;
	}
	db.assert_reads("the tombstone merged into L3").await;
	assert_eq!(db.get("t1"), None);
	assert_eq!(db.get("t2").as_deref(), Some(&b"new:t2"[..]));
	check_invariants(&db.tree, "end");
	finish(db.tree);
}

/// A range delete whose sequence number is not published yet (a commit in flight) hides nothing
/// for the readers at the published sequence number, and the compaction that carries it must not
/// drop the versions it covers: a reader at the published sequence number still reads them after
/// the compaction, and the range delete takes effect when its sequence number is published.
#[test(tokio::test)]
async fn a_compaction_keeps_the_versions_that_a_range_delete_not_published_yet_covers() {
	let mut db = Db::open(Shape::WIDE).await;
	db.put_all(&["u0", "u1", "u2", "u3"], "old").await;
	db.flush();
	let (published_model, published_universe) = (db.model.clone(), db.universe.clone());
	let published = db.tree.core.inner.visible_seq_num.load(Ordering::SeqCst);
	db.del_range("u1", "u3").await;
	db.flush();
	let with_range = db.tree.core.inner.visible_seq_num.load(Ordering::SeqCst);
	assert!(with_range > published);

	// The range delete is in a table, but its commit has not been published.
	db.tree.core.inner.visible_seq_num.store(published, Ordering::SeqCst);
	db.compact_level(0);
	{
		let txn = db.tree.begin_with_mode(Mode::ReadOnly).unwrap();
		assert_reads_in(
			&txn,
			&published_model,
			&published_universe,
			"a reader at the published sequence number after the compaction",
		)
		.await;
	}
	// Published: the range delete hides what it covers.
	db.tree.core.inner.visible_seq_num.store(with_range, Ordering::SeqCst);
	db.assert_reads("the range delete published").await;
	assert_eq!(db.get("u1"), None);
	assert_eq!(db.get("u3").as_deref(), Some(&b"old:u3"[..]), "the end of the range stays");
	verify(&db.image("range delete published"), Shape::WIDE).await;
	finish(db.tree);
}

// ---------------------------------------------------------------------------
// precedence is not by table id
// ---------------------------------------------------------------------------

/// A memtable sealed before a compaction has a lower table id than the compaction's outputs, and
/// it is flushed after the compaction began (here at its `OutputsDurable` stage). The L0 table is
/// newer by sequence and older by id: it has to win over the L1 table, and sit in front of the
/// older L0 table, in the live tree and in every image.
#[test(tokio::test)]
async fn a_table_flushed_during_a_compaction_has_a_lower_id_than_its_output_and_still_wins() {
	let mut db = Db::open(Shape::WIDE).await;
	let keys = ["m0", "m1", "m2", "m3", "m4", "m5"];
	db.put_all(&keys, "gen1").await;
	db.flush();
	db.put_all(&keys[..4], "gen2").await;
	db.flush();
	// Two sealed memtables, with table ids before the compaction's outputs.
	db.put_all(&keys[2..], "gen3").await;
	db.rotate();
	db.put("m0", "gen4").await;
	db.del("m1").await;
	db.rotate();
	assert_eq!(db.immutables(), 2);
	let sealed_ids: Vec<u64> =
		db.tree.core.inner.immutable_memtables.read().unwrap().iter().map(|e| e.table_id).collect();
	let before = db.layout();

	// The flush of both sealed memtables lands while the compaction is between its outputs and
	// its manifest write.
	let weak = Arc::downgrade(&db.tree.core.inner);
	let flushed = Arc::new(AtomicUsize::new(0));
	let seen = Arc::clone(&flushed);
	let images = db.compact_with_images(Forced::new(&db.tree, 0), move |stage| {
		if stage == CompactionStage::OutputsDurable {
			weak.upgrade().unwrap().flush_all_immutables_sync().unwrap();
			seen.fetch_add(1, Ordering::SeqCst);
		}
	});
	assert_eq!(flushed.load(Ordering::SeqCst), 1, "the flush ran inside the compaction");
	let after = db.layout();
	assert_eq!(after[0].len(), 2, "the two flushed memtables are in L0: {after:?}");
	assert!(!after[1].is_empty(), "the old tables are in L1: {after:?}");
	let output_ids = &after[1];
	assert!(
		after[0].iter().all(|id| output_ids.iter().all(|o| id < o)),
		"the L0 tables {:?} were sealed before the outputs {output_ids:?} got their ids",
		after[0]
	);
	let mut newest_first = sealed_ids.clone();
	newest_first.sort_unstable_by(|a, b| b.cmp(a));
	assert_eq!(after[0], newest_first, "L0 holds the later sealed memtable first");
	assert_ne!(before, after);
	db.assert_reads("sealed memtables flushed during the compaction").await;
	assert_eq!(db.get("m0").as_deref(), Some(&b"gen4:m0"[..]));
	assert_eq!(db.get("m1"), None, "the tombstone in L0 shadows the value in L1");
	assert_eq!(db.get("m2").as_deref(), Some(&b"gen3:m2"[..]));
	assert_eq!(db.get("m3").as_deref(), Some(&b"gen3:m3"[..]));
	assert_eq!(db.get("m5").as_deref(), Some(&b"gen3:m5"[..]));
	for (stage, image) in &images {
		let layout = verify(image, Shape::WIDE).await;
		// The image at OutputsDurable was taken after the flush: the two sealed memtables are in
		// L0 next to the inputs the manifest still lists; later images have the outputs in L1.
		if *stage == CompactionStage::OutputsDurable {
			assert_eq!(layout[0].len(), 4, "{layout:?}");
			assert!(layout[1].is_empty());
		} else {
			assert_eq!(layout, after, "{stage:?}");
		}
	}
	check_invariants(&db.tree, "the driving tree");
	let tree = reopen(&db.image("quiescent"), Shape::WIDE).await;
	assert_eq!(layout(&tree), after);
	finish(tree);
	finish(db.tree);
}

/// In a tree of one level the bottom level is L0, and a compaction merges its tables into a new
/// L0 table with a greater id than any input. A memtable sealed before the compaction is
/// flushed while it runs, and is newer than everything the compaction merges but has a lower id
/// than its output: L0 holds the flushed table in front of the output, whichever of them has the
/// greater id, and the key every table holds reads back as the newest.
#[test(tokio::test)]
async fn in_a_one_level_tree_a_table_flushed_during_an_l0_compaction_stays_in_front_of_its_output()
{
	const ONE_LEVEL: Shape = Shape {
		target_file_size: 64 << 20,
		level_count: 1,
	};
	let mut db = Db::open(ONE_LEVEL).await;
	let keys = ["u0", "u1", "u2", "u3"];
	db.put_all(&keys, "gen1").await;
	db.flush();
	db.put_all(&keys[1..], "gen2").await;
	db.del("u3").await;
	db.flush();
	db.put("u0", "gen3").await;
	db.put("u3", "gen3").await;
	db.rotate();
	assert_eq!(db.immutables(), 1);
	let sealed_id =
		db.tree.core.inner.immutable_memtables.read().unwrap().first().unwrap().table_id;
	let before = db.layout();
	assert_eq!(before[0].len(), 2);

	let trigger = Arc::new(Options {
		level0_max_files: 2,
		..Default::default()
	});
	let weak = Arc::downgrade(&db.tree.core.inner);
	let flushed = Arc::new(AtomicUsize::new(0));
	let seen = Arc::clone(&flushed);
	let images =
		db.compact_with_images(Strategy::from_options(Arc::clone(&trigger)), move |stage| {
			if stage == CompactionStage::OutputsDurable {
				weak.upgrade().unwrap().flush_all_immutables_sync().unwrap();
				seen.fetch_add(1, Ordering::SeqCst);
			}
		});
	assert_eq!(flushed.load(Ordering::SeqCst), 1, "the flush ran inside the compaction");
	let after = db.layout();
	assert_eq!(after[0].len(), 2, "the flushed table and the compaction's output: {after:?}");
	assert_eq!(after[0][0], sealed_id, "the flushed table is in front: {after:?}");
	assert!(after[0][1] > sealed_id, "its output got a greater id: {after:?}");
	db.assert_reads("the flushed table in front of the output").await;
	assert_eq!(db.get("u0").as_deref(), Some(&b"gen3:u0"[..]));
	assert_eq!(db.get("u1").as_deref(), Some(&b"gen2:u1"[..]));
	assert_eq!(db.get("u3").as_deref(), Some(&b"gen3:u3"[..]), "a re-put over a deleted key");
	for (stage, image) in &images {
		let layout = verify(image, ONE_LEVEL).await;
		if *stage == CompactionStage::OutputsDurable {
			// Taken after the flush and before the manifest names the output.
			assert_eq!(layout[0].len(), 3, "{layout:?}");
			assert_eq!(layout[0][0], sealed_id);
		} else {
			assert_eq!(layout, after, "{stage:?}");
		}
	}
	check_invariants(&db.tree, "the driving tree");
	finish(db.tree);
}

// ---------------------------------------------------------------------------
// the compaction of a multi-table workload
// ---------------------------------------------------------------------------

/// Several overlapping L0 tables of wide values, so that the outputs split into several tables
/// per level. After every stage of an L0 to L1 compaction, of a second one that overlaps only
/// some of the L1 tables, and of an L1 to L2 compaction, the image has sorted and disjoint
/// tables in every level and every key reads back.
#[test(tokio::test)]
async fn every_compaction_stage_image_of_a_multi_table_workload_keeps_levels_sorted_and_disjoint() {
	let mut db = Db::open(Shape::SPLIT).await;
	let key = |i: usize| format!("w{i:03}");
	let mut rng = Rng(0x5eed_cafe_f00d_0001);

	// Three overlapping waves over 60 keys.
	for wave in 0..3 {
		for i in 0..60 {
			if rng.below(10) < 7 {
				db.put_padded(&key(i), &format!("w{wave}"), 30 + rng.below(30)).await;
			}
		}
		db.flush();
	}
	assert_eq!(db.layout()[0].len(), 3);
	db.assert_reads("three waves in L0").await;

	let before = db.layout();
	let images = db.compact_with_images(Forced::new(&db.tree, 0), |_| {});
	let after = db.layout();
	assert!(after[1].len() >= 3, "the outputs split into several L1 tables: {after:?}");
	for (stage, image) in &images {
		let layout = verify(image, Shape::SPLIT).await;
		assert_eq!(layout, expected_at(*stage, &before, &after), "{stage:?}");
	}

	// A fourth wave over only some keys, rewritten and deleted: it overlaps part of L1.
	for i in 5..25 {
		if rng.below(4) == 0 {
			db.del(&key(i)).await;
		} else {
			db.put_padded(&key(i), "w3", 40).await;
		}
	}
	db.flush();
	let before = db.layout();
	let images = db.compact_with_images(Forced::new(&db.tree, 0), |_| {});
	let after = db.layout();
	assert!(
		after[1].iter().any(|id| before[1].contains(id)),
		"the L1 tables the wave does not overlap are left in place: {before:?} -> {after:?}"
	);
	for (stage, image) in &images {
		let layout = verify(image, Shape::SPLIT).await;
		assert_eq!(layout, expected_at(*stage, &before, &after), "{stage:?}");
	}
	db.assert_reads("after the second L0 to L1 compaction").await;

	// L1 to L2: the best table and its overlaps.
	let before = db.layout();
	let images = db.compact_with_images(Forced::new(&db.tree, 1), |_| {});
	let after = db.layout();
	assert!(!after[2].is_empty());
	for (stage, image) in &images {
		let layout = verify(image, Shape::SPLIT).await;
		assert_eq!(layout, expected_at(*stage, &before, &after), "{stage:?}");
	}
	db.assert_reads("after L1 to L2").await;
	check_invariants(&db.tree, "end");
	finish(db.tree);
}

/// The user keys at which the tables of `level` begin and end.
fn table_bounds(tree: &Tree, level: usize) -> Vec<(String, String)> {
	let manifest = tree.core.inner.level_manifest.read().unwrap();
	manifest.levels.get_levels()[level]
		.tables
		.iter()
		.map(|t| {
			let key = |k: &Option<crate::InternalKey>| {
				String::from_utf8(k.as_ref().expect("a table with points").user_key.clone())
					.unwrap()
			};
			(key(&t.meta.smallest_point), key(&t.meta.largest_point))
		})
		.collect()
}

/// A compaction takes the tables of the next level whose key range touches that of its input, a
/// table that shares only one boundary key with the input included: if it were left out, the output
/// would put a second version of that key in a table of its own next to it. The input of each
/// compaction here is one new version of the last key of an L1 table, and then of the first key
/// of another one, and every stage of each is imaged.
#[test(tokio::test)]
async fn a_compaction_takes_the_next_level_tables_that_share_a_boundary_key_with_its_input() {
	let mut db = Db::open(Shape::SPLIT).await;
	let keys: Vec<String> = (0..12).map(|i| format!("g{i:02}")).collect();
	for key in &keys {
		db.put_padded(key, "first", 300).await;
	}
	db.flush();
	db.compact_level(0);
	let bounds = table_bounds(&db.tree, 1);
	assert!(bounds.len() >= 3, "the outputs split into several L1 tables: {bounds:?}");
	db.assert_reads("the first L1 tables").await;

	// The last key of the first table, which the second one does not hold.
	let (last_of_first, first_of_second) = (bounds[0].1.clone(), bounds[1].0.clone());
	assert!(last_of_first < first_of_second);
	db.put(&last_of_first, "boundary-hi").await;
	db.flush();
	let before = db.layout();
	let images = db.compact_with_images(Forced::new(&db.tree, 0), |_| {});
	let after = db.layout();
	assert!(
		before[1].iter().any(|id| !after[1].contains(id)),
		"the L1 table that holds the key was part of the compaction: {before:?} -> {after:?}"
	);
	for (stage, image) in &images {
		let layout = verify(image, Shape::SPLIT).await;
		assert_eq!(layout, expected_at(*stage, &before, &after), "{stage:?}");
	}
	db.assert_reads("after a new version of the last key of an L1 table").await;
	check_invariants(&db.tree, "after the first boundary compaction");

	// The first key of a table that is not the first.
	let bounds = table_bounds(&db.tree, 1);
	assert!(bounds.len() >= 3, "{bounds:?}");
	let first_of_middle = bounds[1].0.clone();
	db.put(&first_of_middle, "boundary-lo").await;
	db.flush();
	let before = db.layout();
	let images = db.compact_with_images(Forced::new(&db.tree, 0), |_| {});
	let after = db.layout();
	assert_ne!(before[1], after[1], "{before:?} -> {after:?}");
	for (stage, image) in &images {
		let layout = verify(image, Shape::SPLIT).await;
		assert_eq!(layout, expected_at(*stage, &before, &after), "{stage:?}");
	}
	db.assert_reads("after a new version of the first key of an L1 table").await;
	check_invariants(&db.tree, "after the second boundary compaction");
	finish(db.tree);
}

/// Snapshots that read different versions of one key make a compaction keep all of them, and the
/// output must not be cut between two versions of a key even when it is over its size: a table of
/// L1 or deeper holds every version of its keys, or the tables of the level overlap.
#[test(tokio::test)]
async fn all_the_versions_of_a_key_that_snapshots_retain_stay_in_one_output_table() {
	let mut db = Db::open(Shape::SPLIT).await;
	db.put_padded("v0", "first", 300).await;
	let mut readers = Vec::new();
	for round in 0..5 {
		db.put_padded("v1", &format!("r{round}"), 300).await;
		readers.push(db.snapshot());
		db.flush();
	}
	db.put_padded("v2", "last", 300).await;
	db.flush();
	for (txn, model, universe) in &readers {
		assert_reads_in(txn, model, universe, "a reader before the compaction").await;
	}
	let before = db.layout();
	let images = db.compact_with_images(Forced::new(&db.tree, 0), |_| {});
	let after = db.layout();
	assert!(after[1].len() >= 2, "the output is cut into several tables: {after:?}");
	for (stage, image) in &images {
		let layout = verify(image, Shape::SPLIT).await;
		assert_eq!(layout, expected_at(*stage, &before, &after), "{stage:?}");
	}
	check_invariants(&db.tree, "the driving tree");
	for (txn, model, universe) in &readers {
		assert_reads_in(txn, model, universe, "a reader after the compaction").await;
	}
	db.assert_reads("the newest versions").await;
	drop(readers);
	finish(db.tree);
}

// ---------------------------------------------------------------------------
// L0 order across a restart
// ---------------------------------------------------------------------------

/// Three L0 tables rewrite the same keys; a compaction takes only the two oldest (and what they
/// overlap), which leaves the newest in L0 over their output in L1. The newest L0 table keeps
/// winning in every image, after a reopen, and after a newer flush made after the reopen, which
/// goes in front of it, and after a second reopen.
#[test(tokio::test)]
async fn the_remaining_l0_tables_keep_their_order_after_a_compaction_of_the_oldest_ones_and_a_restart(
) {
	let mut db = Db::open(Shape::WIDE).await;
	let keys = ["p0", "p1", "p2", "p3", "p4"];
	db.put_all(&keys, "t1").await;
	db.flush();
	db.put_all(&keys[1..], "t2").await;
	db.flush();
	db.put_all(&keys[2..], "t3").await;
	db.del("p0").await;
	db.flush();
	let l0 = db.layout()[0].clone();
	assert_eq!(l0.len(), 3);
	let (t3, t2, t1) = (l0[0], l0[1], l0[2]);
	assert!(t3 > t2 && t2 > t1, "newest first: {l0:?}");
	db.assert_reads("three L0 tables").await;

	let before = db.layout();
	let images = db.compact_with_images(
		L0Subset {
			ids: vec![t1, t2],
		},
		|_| {},
	);
	let after = db.layout();
	assert_eq!(after[0], vec![t3], "the newest table is the one left in L0: {after:?}");
	assert_eq!(after[1].len(), 1);
	assert!(after[1][0] > t3, "the output has a greater id than the table that outranks it");
	for (stage, image) in &images {
		let layout = verify(image, Shape::WIDE).await;
		assert_eq!(layout, expected_at(*stage, &before, &after), "{stage:?}");
	}
	db.assert_reads("t3 over the output of t1 and t2").await;
	assert_eq!(db.get("p0"), None);
	assert_eq!(db.get("p1").as_deref(), Some(&b"t2:p1"[..]));
	assert_eq!(db.get("p2").as_deref(), Some(&b"t3:p2"[..]));

	// Crash, reopen: the order and the reads are as they were.
	let image = db.image("after the subset compaction");
	assert_eq!(verify(&image, Shape::WIDE).await, after);

	// A flush after a reopen goes in front of the older table, and survives another reopen.
	let mut db2 = Db::continue_from(&image, Shape::WIDE).await;
	db2.put("p2", "t4").await;
	db2.put("p0", "t4").await;
	db2.flush();
	let layout2 = db2.layout();
	assert_eq!(layout2[0].len(), 2, "{layout2:?}");
	assert_eq!(layout2[0][1], t3);
	assert!(layout2[0][0] > t3);
	db2.assert_reads("a flush after the reopen").await;
	assert_eq!(db2.get("p2").as_deref(), Some(&b"t4:p2"[..]));
	assert_eq!(db2.get("p0").as_deref(), Some(&b"t4:p0"[..]), "a re-put over a tombstone in L0");
	let image2 = db2.image("after the flush after the reopen");
	assert_eq!(verify(&image2, Shape::WIDE).await, layout2, "a reopen keeps the order of L0");
	// A compaction after the reopen takes both L0 tables.
	db2.compact_level(0);
	db2.assert_reads("compacted after the reopen").await;
	check_invariants(&db2.tree, "after the reopen");
	finish(db2.tree);
	finish(db.tree);
}

/// An image with several memtables that never reached an SSTable: recovery replays one WAL
/// segment per memtable and flushes all but the last, oldest first, to L0 tables whose ids it
/// allocates then. They must be ordered by sequence, in front of the L0 table that was already
/// there, and the keys rewritten in each segment read back as the newest.
#[test(tokio::test)]
async fn recovery_flushes_of_several_wal_segments_enter_l0_newest_first() {
	let mut db = Db::open(Shape::WIDE).await;
	db.put_all(&["s0", "s1", "s2", "s3"], "base").await;
	db.flush();
	let base = db.layout()[0][0];
	// Three sealed memtables and an active one, each rewriting some of the same keys.
	db.put("s0", "m1").await;
	db.put("s1", "m1").await;
	db.rotate();
	db.put("s1", "m2").await;
	db.del("s2").await;
	db.rotate();
	db.put("s2", "m3").await;
	db.put("s4", "m3").await;
	db.rotate();
	db.put("s0", "m4").await;
	db.assert_reads("four unflushed memtables").await;
	assert_eq!(db.immutables(), 3);
	let image = db.image("four unflushed memtables");
	let mut db2 = Db::continue_from(&image, Shape::WIDE).await;
	let l0 = db2.layout()[0].clone();
	assert!(l0.len() >= 4, "base plus the recovered memtables: {l0:?}");
	assert_eq!(*l0.last().unwrap(), base, "the table that was there stays the oldest: {l0:?}");
	// Recovery flushes the memtables oldest first, so a table's id grows with its sequence numbers.
	assert!(l0.windows(2).all(|w| w[0] > w[1]), "ids follow the order of the flushes: {l0:?}");
	{
		let manifest = db2.tree.core.inner.level_manifest.read().unwrap();
		let seqs: Vec<u64> = manifest.levels.get_levels()[0]
			.tables
			.iter()
			.map(|t| t.meta.properties.seqnos.1)
			.collect();
		assert!(seqs.windows(2).all(|w| w[0] > w[1]), "strictly newest first: {seqs:?}");
	}
	// And writes after the recovery outrank all of them.
	db2.put("s1", "after").await;
	db2.flush();
	let l0b = db2.layout()[0].clone();
	assert_eq!(l0b.len(), l0.len() + 1, "{l0b:?}");
	db2.assert_reads("a write after the recovery").await;
	assert_eq!(db2.get("s1").as_deref(), Some(&b"after:s1"[..]));
	db2.compact_level(0);
	db2.assert_reads("everything compacted").await;
	check_invariants(&db2.tree, "end");
	finish(db2.tree);
	finish(db.tree);
}

/// The reopen is of a quiescent image (everything flushed, nothing to replay): the writes after
/// it are logged and flushed, and outrank every table of L0 and of L1.
#[test(tokio::test)]
async fn a_flush_after_a_reopen_outranks_the_tables_that_were_there() {
	let mut db = Db::open(Shape::WIDE).await;
	for round in 0..4 {
		db.put_all(&["f0", "f1", "f2"], &format!("r{round}")).await;
		db.flush();
	}
	db.compact_level(0);
	db.put("f1", "after-compaction").await;
	db.flush();
	let image = db.image("quiescent");
	let mut db2 = Db::continue_from(&image, Shape::WIDE).await;
	db2.put("f0", "after-reopen").await;
	db2.put("f1", "after-reopen").await;
	db2.flush();
	assert_eq!(db2.get("f1").as_deref(), Some(&b"after-reopen:f1"[..]));
	assert_eq!(db2.get("f2").as_deref(), Some(&b"r3:f2"[..]));
	db2.assert_reads("after the reopen").await;
	let image2 = db2.image("after the flush after the reopen");
	assert_eq!(verify(&image2, Shape::WIDE).await, db2.layout());
	finish(db2.tree);
	finish(db.tree);
}

// ---------------------------------------------------------------------------
// the manifest on disk
// ---------------------------------------------------------------------------

/// Every manifest the tree writes lists L0 newest first, by sequence number, and L1 and deeper
/// sorted by key; a load keeps the order it finds (it does not sort), so the order on disk is the
/// order after a reopen. Checked after each of a series of flushes, subset compactions and
/// compactions, on the disk image and on the reopened tree.
#[test(tokio::test)]
async fn the_manifest_on_disk_lists_l0_in_sequence_order_after_every_step_and_a_load_keeps_it() {
	let mut db = Db::open(Shape::SPLIT).await;
	let check = |db: &Db, step: &str| {
		db.flush_wal_buffers();
		let disk = read_manifest(&db.path);
		let memory = db.layout();
		assert_eq!(disk.levels, memory, "{step}: the manifest on disk is the one in memory");
		let manifest = db.tree.core.inner.level_manifest.read().unwrap();
		assert!(disk.last_sequence == manifest.last_sequence && disk.next_table_id > 0);
		let seqs: Vec<u64> = manifest.levels.get_levels()[0]
			.tables
			.iter()
			.map(|t| t.meta.properties.seqnos.1)
			.collect();
		assert!(
			seqs.windows(2).all(|w| w[0] > w[1]),
			"{step}: L0 on disk is not newest first: {seqs:?}"
		);
		for (li, level) in manifest.levels.get_levels().iter().enumerate().skip(1) {
			let smallest: Vec<Vec<u8>> = level
				.tables
				.iter()
				.filter_map(|t| t.meta.smallest_point.as_ref().map(|k| k.user_key.clone()))
				.collect();
			assert!(smallest.windows(2).all(|w| w[0] < w[1]), "{step}: L{li} is not sorted by key");
		}
	};
	let mut images: Vec<(Image, Vec<Vec<u64>>)> = Vec::new();
	for round in 0..5 {
		for i in (round..40).step_by(3) {
			db.put_padded(&format!("o{i:03}"), &format!("r{round}"), 20).await;
		}
		db.flush();
		check(&db, &format!("flush {round}"));
		images.push((db.image(&format!("flush {round}")), db.layout()));
	}
	let ids = db.layout()[0].clone();
	assert_eq!(ids.len(), 5);
	// The two oldest to L1.
	db.tree
		.compact(Arc::new(L0Subset {
			ids: vec![ids[4], ids[3]],
		}))
		.unwrap();
	assert_eq!(db.layout()[0], ids[..3].to_vec(), "the three newest stay in L0, in order");
	check(&db, "subset compaction");
	images.push((db.image("subset compaction"), db.layout()));
	for _ in 0..2 {
		db.put_padded("o001", "late", 10).await;
		db.flush();
		check(&db, "a flush over the compaction");
		images.push((db.image("a flush over the compaction"), db.layout()));
	}
	db.compact_level(0);
	check(&db, "L0 to L1");
	images.push((db.image("L0 to L1"), db.layout()));
	db.compact_level(1);
	check(&db, "L1 to L2");
	images.push((db.image("L1 to L2"), db.layout()));
	for (image, expected) in &images {
		let layout = verify(image, Shape::SPLIT).await;
		assert_eq!(&layout, expected, "{}: a load keeps the order of the manifest", image.label);
	}
	finish(db.tree);
}

/// With nothing in the WAL to replay, the sequence number a reopened tree writes with comes only
/// from the manifest's `last_sequence`. A write after the reopen of a fully flushed image must
/// get a sequence number above every table's, so that it wins over the versions in L0 and in L1,
/// and so that it is flushed to an L0 table at the front.
#[test(tokio::test)]
async fn last_sequence_is_restored_so_a_write_after_a_reopen_outranks_every_table() {
	let mut db = Db::open(Shape::WIDE).await;
	for round in 0..3 {
		db.put_all(&["q0", "q1", "q2"], &format!("r{round}")).await;
		db.flush();
	}
	db.compact_level(0);
	db.put_all(&["q1", "q2"], "l0").await;
	db.flush();
	let image = db.image("fully flushed");
	let mut db2 = Db::continue_from(&image, Shape::WIDE).await;
	let largest = {
		let manifest = db2.tree.core.inner.level_manifest.read().unwrap();
		let largest = manifest.iter().map(|t| t.meta.largest_seq_num.unwrap()).max().unwrap();
		assert!(largest > 0);
		assert_eq!(manifest.last_sequence, largest, "last_sequence is the largest table sequence");
		largest
	};
	assert!(db2.tree.core.inner.visible_seq_num.load(Ordering::SeqCst) >= largest);
	db2.put("q1", "new").await;
	db2.del("q2").await;
	db2.assert_reads("new writes over restored tables").await;
	assert_eq!(db2.get("q1").as_deref(), Some(&b"new:q1"[..]));
	assert_eq!(db2.get("q2"), None);
	db2.flush();
	{
		let manifest = db2.tree.core.inner.level_manifest.read().unwrap();
		let front = &manifest.levels.get_levels()[0].tables[0];
		assert!(
			front.meta.smallest_seq_num.unwrap() > largest,
			"the flushed table of the new writes has sequence numbers above every older table"
		);
	}
	db2.assert_reads("flushed").await;
	verify(&db2.image("after the writes after the reopen"), Shape::WIDE).await;
	finish(db2.tree);
	finish(db.tree);
}

// ---------------------------------------------------------------------------
// a table that holds only a range deletion
// ---------------------------------------------------------------------------

/// A memtable that holds only a range delete is flushed to an L0 table that hides the older
/// versions of the keys it covers, in the tables below it and after a crash.
#[test(tokio::test)]
async fn a_memtable_holding_only_a_range_delete_flushes_to_an_l0_table_that_hides_older_keys() {
	let mut db = Db::open(Shape::WIDE).await;
	db.put_all(&["a1", "a2", "b1", "b2"], "old").await;
	db.flush();
	db.del_range("a0", "b0").await;
	db.flush();
	{
		let manifest = db.tree.core.inner.level_manifest.read().unwrap();
		let front = &manifest.levels.get_levels()[0].tables[0];
		let distinct: BTreeSet<(Vec<u8>, Vec<u8>)> =
			front.range_deletions.read().iter().map(|(a, b, _)| (a.clone(), b.clone())).collect();
		assert_eq!(
			distinct,
			BTreeSet::from([(b"a0".to_vec(), b"b0".to_vec())]),
			"the front table holds the range delete"
		);
	}
	db.assert_reads("a range delete flushed on its own").await;
	assert_eq!(db.get("a1"), None);
	assert_eq!(db.get("b1").as_deref(), Some(&b"old:b1"[..]));
	verify(&db.image("a range delete flushed on its own"), Shape::WIDE).await;
	db.compact_level(0);
	db.assert_reads("the range delete compacted into L1").await;
	finish(db.tree);
}

/// A range delete whose end key is one byte long cannot be flushed: the flush decodes the end key
/// stored in the range delete's entry as an encoded value, and a value needs two bytes. Every flush
/// of that memtable fails, so the commits behind it eventually stall. Keys of two bytes or more
/// are not affected, which is why the other tests here use them.
#[test(tokio::test)]
#[ignore = "flushing a memtable with a range delete whose end key is one byte fails with an IO \
            error (failed to fill whole buffer): the flush decodes the end key as a ValueLocation"]
async fn a_memtable_holding_a_range_delete_with_a_one_byte_end_key_flushes() {
	let mut db = Db::open(Shape::WIDE).await;
	db.put_all(&["a1", "a2", "b1", "b2"], "old").await;
	db.flush();
	db.del_range("a", "b").await;
	let flushed = db.tree.flush();
	assert!(flushed.is_ok(), "the flush of a one-byte range delete end key failed: {flushed:?}");
	db.assert_reads("a range delete with a one-byte end key").await;
	assert_eq!(db.get("a1"), None);
	assert_eq!(db.get("b1").as_deref(), Some(&b"old:b1"[..]));
	finish(db.tree);
}

/// A range delete flushed on its own and compacted into L1 puts a table among the tables of L1
/// whose only point is the entry of the range delete itself (a range of one key, `f0`). L1 is
/// searched by the key ranges of its tables, so that table must not hide the tables on either side
/// of it, and not those that are added after it: every key is found, in the live tree and after a
/// reopen. A table with no point at all is the subject of the next test.
#[test(tokio::test)]
async fn a_range_delete_flushed_on_its_own_does_not_hide_the_l1_tables_around_it() {
	let mut db = Db::open(Shape::WIDE).await;
	// L2: the older versions of every key.
	db.put_all(&["a1", "a2", "m1", "m2", "z1", "z2"], "old").await;
	db.flush();
	db.compact_level(0);
	db.compact_level(1);
	// L1, one table at a time.
	db.put_all(&["a1", "a2"], "A").await;
	db.flush();
	db.compact_level(0);
	db.put_all(&["m1", "m2"], "M").await;
	db.flush();
	db.compact_level(0);
	// A range delete on its own, over keys that no table of L1 holds a point of.
	db.del_range("f0", "g0").await;
	db.flush();
	db.compact_level(0);
	db.assert_reads("the range delete in L1").await;
	// Tables that sort on both sides of it and after it.
	db.put_all(&["c1", "h1", "z1"], "N").await;
	db.flush();
	db.compact_level(0);
	db.assert_reads("tables around the range delete").await;
	assert_eq!(db.get("z1").as_deref(), Some(&b"N:z1"[..]));
	assert_eq!(db.get("h1").as_deref(), Some(&b"N:h1"[..]));
	assert_eq!(db.get("m1").as_deref(), Some(&b"M:m1"[..]));
	assert_eq!(db.get("a2").as_deref(), Some(&b"A:a2"[..]));
	check_invariants(&db.tree, "the driving tree");
	verify(&db.image("a range delete among the L1 tables"), Shape::WIDE).await;
	finish(db.tree);
}

/// A bottom level of several tables (L3 of a four-level tree), and the tables of L1 and L2 that
/// are compacted into it one at a time.
async fn db_with_a_split_bottom_level() -> (Db, Vec<String>, Vec<(String, String)>) {
	let mut db = Db::open(Shape::SPLIT).await;
	let keys: Vec<String> = (0..12).map(|i| format!("n{i:02}")).collect();
	for key in &keys {
		db.put_padded(key, "old", 300).await;
	}
	db.flush();
	db.compact_level(0);
	// One table of a level goes down at a time: they all end up in L3, side by side.
	for source in 1..3 {
		while !db.layout()[source].is_empty() {
			db.compact_level(source as u8);
		}
	}
	let bounds = table_bounds(&db.tree, 3);
	assert!(bounds.len() >= 3, "the bottom level holds several tables: {bounds:?}");
	assert!(db.layout()[..3].iter().all(Vec::is_empty));
	(db, keys, bounds)
}

/// The number of tables of L3 that hold a range delete and no point.
fn tables_without_points(tree: &Tree) -> usize {
	let manifest = tree.core.inner.level_manifest.read().unwrap();
	manifest.levels.get_levels()[3]
		.tables
		.iter()
		.filter(|t| t.meta.smallest_point.is_none() && t.has_range_deletions())
		.count()
}

/// In the bottom level a compaction drops the entry of a range delete once it has dropped what the
/// range delete covered, and a table whose every key was covered comes out with no point at all:
/// only the range delete in its metadata. Such a table has no key range, and the tables of the
/// level on either side of it must stay reachable by point reads, in the live tree and in a copy
/// that is reopened. (A scan over such a tree is the subject of the ignored test below.)
#[test(tokio::test)]
async fn a_bottom_level_table_emptied_by_a_range_delete_leaves_the_tables_around_it_readable() {
	let (mut db, keys, bounds) = db_with_a_split_bottom_level().await;

	// A range delete over the keys of the second table of L3, and no others.
	let (first, end) = (bounds[1].0.clone(), bounds[2].0.clone());
	db.del_range(&first, &end).await;
	db.flush();
	db.compact_level(0);
	db.compact_level(1);
	let before = db.layout();
	db.compact_level(2);
	let after = db.layout();
	assert_ne!(before, after);
	assert_eq!(tables_without_points(&db.tree), 1, "the table the range delete emptied: {after:?}");
	assert_eq!(after[3].len(), bounds.len(), "it replaced the table it emptied: {after:?}");

	let txn = db.tree.begin_with_mode(Mode::ReadOnly).unwrap();
	assert_point_reads_in(&txn, &db.model, &db.universe, "a table without points in L3").await;
	drop(txn);
	for key in keys.iter().filter(|k| **k < first || **k >= end) {
		assert!(db.get(key).is_some(), "{key} is still there");
	}
	assert_eq!(db.get(&first), None);
	// Keys that no table holds, before the first one and in the gaps between tables.
	for absent in ["a00", "n03x", "n05x", "n09x"] {
		assert_eq!(db.get(absent), None, "{absent} is in no table");
	}

	// The same after a reopen, which loads the tables in the order the manifest lists them.
	let image = db.image("a table without points in the bottom level");
	let tree = open_stopped(options(&image.path, Shape::SPLIT)).await;
	assert_eq!(tables_without_points(&tree), 1);
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	assert_point_reads_in(&txn, &image.model, &image.universe, "reopened").await;
	drop(txn);
	finish(tree);
	finish(db.tree);
}

/// The point read of a key that sorts after every key of the bottom level, in a bottom level whose
/// last table has no points. The level is searched by the end keys of its tables, a table without
/// points has none and is taken to be the one that could hold the key, and the read opens it: it
/// fails with `EmptyCorruptPartitionedIndex` instead of finding nothing, on both read paths.
#[test(tokio::test)]
#[ignore = "a point read of a key past the last key of a level fails with EmptyCorruptPartitionedIndex \
            when the last table of the level has a range delete and no point"]
async fn a_read_past_the_last_key_works_in_a_level_whose_last_table_has_no_points() {
	let (mut db, _keys, bounds) = db_with_a_split_bottom_level().await;
	let (first, end) = (bounds[1].0.clone(), bounds[2].0.clone());
	db.del_range(&first, &end).await;
	db.flush();
	db.compact_level(0);
	db.compact_level(1);
	db.compact_level(2);
	assert_eq!(tables_without_points(&db.tree), 1);
	let txn = db.tree.begin_with_mode(Mode::ReadOnly).unwrap();
	for absent in ["n99", "zz"] {
		let got = txn.get(absent.as_bytes());
		assert!(matches!(got, Ok(None)), "get of {absent}: {got:?}");
		let got = txn.get_async(absent.as_bytes()).await;
		assert!(matches!(got, Ok(None)), "get_async of {absent}: {got:?}");
	}
	drop(txn);
	finish(db.tree);
}

/// The scan of the tree of the test above: a table with only a range delete in it has no data
/// blocks, and a scan that opens an iterator on it fails with `EmptyCorruptPartitionedIndex`
/// (point reads never open one), so no range scan works any more once a compaction made such a
/// table. The compaction itself writes the table on purpose (it keeps the range delete).
#[test(tokio::test)]
#[ignore = "a scan fails with EmptyCorruptPartitionedIndex once a bottom-level compaction has left \
            a table with a range delete and no point"]
async fn a_scan_works_in_a_tree_with_a_bottom_level_table_emptied_by_a_range_delete() {
	let (mut db, _keys, bounds) = db_with_a_split_bottom_level().await;
	let (first, end) = (bounds[1].0.clone(), bounds[2].0.clone());
	db.del_range(&first, &end).await;
	db.flush();
	db.compact_level(0);
	db.compact_level(1);
	db.compact_level(2);
	assert_eq!(tables_without_points(&db.tree), 1);
	let txn = db.tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let mut iter = txn.iter().unwrap();
	let scanned = collect_transaction_all(&mut iter);
	assert!(scanned.is_ok(), "the scan failed: {:?}", scanned.map(|v| v.len()));
	drop(iter);
	assert_scan_in(&txn, &db.model, "a table without points in L3");
	drop(txn);
	finish(db.tree);
}

// ---------------------------------------------------------------------------
// the randomised sweep
// ---------------------------------------------------------------------------

const SWEEP_KEYS: usize = 36;
const SWEEP_OPS: usize = 220;
/// The cap on the crash images of each kind that one seed takes.
const SWEEP_IMAGES: usize = 60;

fn sweep_key(i: usize) -> String {
	format!("s{i:02}")
}

/// What a seed did, so that no sweep passes vacuously.
#[derive(Default, Debug)]
struct SweepStats {
	merged: usize,
	/// The deepest level that held a table after a merge.
	deepest: usize,
	stage_images: usize,
	quiescent_images: usize,
	flushes: usize,
}

/// What a sweep runs on.
#[derive(Clone, Copy)]
struct SweepKind {
	shape: Shape,
	/// The size of L1 above which the strategy compacts it into L2.
	max_bytes_for_level: u64,
	/// Whether the workload has range deletes.
	range_deletes: bool,
}

impl SweepKind {
	/// Three levels and a small L1: the compactions reach the bottom level, where tombstones are
	/// dropped. No range deletes: a range delete compacted into the bottom level can leave a
	/// table with no points, which a scan cannot read (see the ignored tests above).
	const BOTTOM: SweepKind = SweepKind {
		shape: Shape::SPLIT_SHALLOW,
		max_bytes_for_level: 1024,
		range_deletes: false,
	};
	/// Four levels, range deletes: the compactions reach L2 at most, never the bottom level.
	const SHALLOW: SweepKind = SweepKind {
		shape: Shape::SPLIT,
		max_bytes_for_level: 4096,
		range_deletes: true,
	};
}

/// The index of the deepest level that holds a table.
fn deepest_level(layout: &[Vec<u64>]) -> usize {
	layout.iter().rposition(|level| !level.is_empty()).unwrap_or(0)
}

/// Reopens an image of a sweep, checks it, and for some of them compacts the reopened tree and
/// checks again. The image is removed afterwards.
async fn check_sweep_image(
	image: &Image,
	kind: SweepKind,
	trigger: &Arc<Options>,
	seed: u64,
	step: usize,
	compact: bool,
	trace: &[String],
) {
	let ctx = format!(
		"seed {seed:#x} step {step} image `{}`; the last operations: {:?}",
		image.label,
		&trace[trace.len().saturating_sub(6)..]
	);
	let tree = open_stopped(options(&image.path, kind.shape)).await;
	check_invariants(&tree, &ctx);
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	assert_reads_in(&txn, &image.model, &image.universe, &ctx).await;
	drop(txn);
	if compact {
		tree.compact(Arc::new(Strategy::from_options(Arc::clone(trigger)))).unwrap();
		let ctx = format!("{ctx} (after a compaction of the reopened tree)");
		check_invariants(&tree, &ctx);
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		assert_reads_in(&txn, &image.model, &image.universe, &ctx).await;
	}
	finish(tree);
	let _ = std::fs::remove_dir_all(&image.path);
}

/// A random workload against a model: puts, overwrites, deletes, range deletes, batches,
/// flushes, memtable rotations and compactions by the real strategy with low triggers. Crash
/// images are taken between operations and at the stages of the compactions, each reopened and
/// compared with the model of the instant it was taken. Every image is checked for the
/// structural invariants, and a third of them are compacted after the reopen and checked again.
async fn sweep(seed: u64, kind: SweepKind) -> SweepStats {
	let mut rng = Rng(seed);
	let mut db = Db::open(kind.shape).await;
	let trigger = Arc::new(Options {
		level0_max_files: 3,
		max_bytes_for_level: kind.max_bytes_for_level,
		level_multiplier: 2.0,
		..Default::default()
	});
	let mut stats = SweepStats::default();
	let mut tag = 0u64;
	let mut trace: Vec<String> = Vec::new();
	// A reader that began some steps ago and has to keep reading what it saw through the flushes
	// and compactions since.
	let mut snapshot: Option<(Transaction, Model, BTreeSet<Vec<u8>>)> = None;

	for step in 0..SWEEP_OPS {
		let dice = rng.below(100);
		let mut flushed = false;
		if dice < 34 {
			let key = sweep_key(rng.below(SWEEP_KEYS));
			tag += 1;
			let pad = rng.below(50);
			trace.push(format!("put {key} #{tag}"));
			db.put_padded(&key, &format!("v{tag}"), pad).await;
		} else if dice < 48 {
			let n = 2 + rng.below(4);
			let mut chosen = BTreeSet::new();
			while chosen.len() < n {
				chosen.insert(rng.below(SWEEP_KEYS));
			}
			let mut ops = Vec::new();
			for i in chosen {
				tag += 1;
				if rng.below(4) == 0 {
					ops.push(Op::Del(sweep_key(i).into_bytes()));
				} else {
					ops.push(Op::Put(
						sweep_key(i).into_bytes(),
						value_of(&sweep_key(i), &format!("b{tag}"), rng.below(40)),
					));
				}
			}
			trace.push(format!("batch of {} ending #{tag}", ops.len()));
			db.batch(ops).await;
		} else if dice < 58 || (dice < 64 && !kind.range_deletes) {
			let key = sweep_key(rng.below(SWEEP_KEYS));
			trace.push(format!("delete {key}"));
			db.del(&key).await;
		} else if dice < 64 {
			let a = rng.below(SWEEP_KEYS);
			let b = (a + 1 + rng.below(8)).min(SWEEP_KEYS);
			trace.push(format!("range delete {}..{}", sweep_key(a), sweep_key(b)));
			db.del_range(&sweep_key(a), &sweep_key(b)).await;
		} else if dice < 74 {
			trace.push("rotate".to_string());
			db.rotate();
		} else if dice < 92 {
			trace.push("flush".to_string());
			db.flush();
			stats.flushes += 1;
			flushed = true;
		} else {
			trace.push("compact".to_string());
		}

		// The compactions the real strategy asks for, up to three in a row.
		if dice >= 92 || flushed {
			for _ in 0..3 {
				let before = db.layout();
				let with_images =
					stats.stage_images < SWEEP_IMAGES && (stats.merged < 2 || rng.below(2) == 0);
				let strategy = Strategy::from_options(Arc::clone(&trigger));
				if with_images {
					db.flush_wal_buffers();
					let taken = install_image_hook(&db, |_| {});
					let result = db.tree.compact(Arc::new(strategy));
					*db.tree.core.inner.compaction_stage_hook.lock() = None;
					result.unwrap();
					let images = std::mem::take(&mut *taken.lock().unwrap());
					if db.layout() == before {
						assert!(images.is_empty(), "a Skip reached a stage");
						break;
					}
					let stages: Vec<CompactionStage> = images.iter().map(|(s, _)| *s).collect();
					assert_eq!(stages, ALL_STAGES, "seed {seed:#x}: a merge missed a stage");
					stats.merged += 1;
					stats.deepest = stats.deepest.max(deepest_level(&db.layout()));
					trace.push("compaction".to_string());
					for (_, image) in &images {
						stats.stage_images += 1;
						let compact = rng.below(3) == 0;
						check_sweep_image(image, kind, &trigger, seed, step, compact, &trace).await;
					}
				} else {
					db.tree.compact(Arc::new(strategy)).unwrap();
					if db.layout() == before {
						break;
					}
					stats.merged += 1;
					stats.deepest = stats.deepest.max(deepest_level(&db.layout()));
					trace.push("compaction".to_string());
				}
			}
		}

		// A quiescent image now and then, and after every flush.
		if ((flushed && rng.below(2) == 0) || rng.below(12) == 0)
			&& stats.quiescent_images < SWEEP_IMAGES
		{
			let image = db.image(&format!("after {}", trace.last().unwrap()));
			stats.quiescent_images += 1;
			let compact = rng.below(3) == 0;
			check_sweep_image(&image, kind, &trigger, seed, step, compact, &trace).await;
		}
		if step % 10 == 9 {
			db.assert_reads(&format!("seed {seed:#x} step {step}: the live tree")).await;
		}
		if step % 25 == 7 {
			snapshot = Some(db.snapshot());
		}
		if step % 5 == 4 {
			if let Some((txn, model, universe)) = &snapshot {
				let ctx = format!(
					"seed {seed:#x} step {step}: the old reader; the last operations: {:?}",
					&trace[trace.len().saturating_sub(6)..]
				);
				assert_reads_in(txn, model, universe, &ctx).await;
			}
		}
	}
	drop(snapshot);

	db.assert_reads(&format!("seed {seed:#x}: the live tree at the end")).await;
	check_invariants(&db.tree, &format!("seed {seed:#x}: the live tree at the end"));
	// The last image has nothing to replay.
	db.flush();
	let image = db.image("the end, fully flushed");
	check_sweep_image(&image, kind, &trigger, seed, SWEEP_OPS, true, &trace).await;
	finish(db.tree);
	assert!(stats.merged >= 1, "seed {seed:#x} never merged: {stats:?}");
	assert!(
		stats.stage_images >= 4,
		"seed {seed:#x} took no images inside a compaction: {stats:?}"
	);
	if !kind.range_deletes {
		let bottom = kind.shape.level_count as usize - 1;
		assert_eq!(
			stats.deepest, bottom,
			"seed {seed:#x} never compacted into L{bottom}: {stats:?}"
		);
	}
	stats
}

macro_rules! sweep_seeds {
	($kind:expr; $($name:ident => $seed:expr),* $(,)?) => {
		$(
			#[test(tokio::test)]
			async fn $name() {
				let seed: u64 = $seed;
				let stats = sweep(seed, $kind).await;
				eprintln!("seed {seed:#x}: {stats:?}");
			}
		)*
	};
}

sweep_seeds! {
	SweepKind::SHALLOW;
	sweep_seed_01 => 0x9e37_79b9_7f4a_7c15,
	sweep_seed_02 => 0x0123_4567_89ab_cdef,
	sweep_seed_03 => 0xdead_beef_cafe_f00d,
	sweep_seed_04 => 0x1357_9bdf_2468_ace0,
	sweep_seed_05 => 0x0f0f_0f0f_1234_5678,
	sweep_seed_06 => 0xabcd_ef01_2345_6789,
	sweep_seed_07 => 0x5555_aaaa_3333_cccc,
	sweep_seed_08 => 0x1111_2222_3333_4444,
	sweep_seed_09 => 0x7777_8888_9999_0001,
	sweep_seed_10 => 0xfeed_face_0bad_c0de,
	sweep_seed_11 => 0x2468_ace0_1357_9bdf,
	sweep_seed_12 => 0xc0ff_ee00_dead_10cc,
	sweep_seed_13 => 0x1d2c_3b4a_5968_7786,
	sweep_seed_14 => 0xa5a5_5a5a_f0f0_0f0f,
	sweep_seed_15 => 0x0bad_f00d_1234_9876,
	sweep_seed_16 => 0x3141_5926_5358_9793,
}

sweep_seeds! {
	SweepKind::BOTTOM;
	bottom_sweep_seed_01 => 0x4d595df4_d0f33173,
	bottom_sweep_seed_02 => 0x9e37_79b9_0000_0001,
	bottom_sweep_seed_03 => 0x2545_f491_4f6c_dd1d,
	bottom_sweep_seed_04 => 0x6c62_272e_07bb_0142,
	bottom_sweep_seed_05 => 0xd6e8_feb8_6659_fd93,
	bottom_sweep_seed_06 => 0x8cb9_2ba7_2f3d_8dd7,
}
