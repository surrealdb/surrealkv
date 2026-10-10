//! A compaction under a crash.
//!
//! A compaction writes its outputs, makes them durable, replaces its inputs with them in the
//! manifest, removes the inputs and then removes the value-log files that nothing points into any
//! more. A crash can come between any two of these steps, and the directory it leaves must open
//! to the data that was acknowledged, with no table lost or counted twice. The compaction stage
//! hook (`CompactionStage`) stops a compaction at each of the steps, and a crash image is a copy
//! of the database directory taken there. Each image is reopened and checked against a model.
//!
//! The groups:
//!
//! 1. Every stage of a compaction from L0 to L1, from L1 to L2 and from L2 to the bottom level, on
//!    a tree with several versions of its keys, deletes, values in the value log, a target file
//!    size that splits each output into several tables, and older data below the target that a
//!    dropped tombstone would bring back. The image is reopened, read, scanned, compared with the
//!    files of its manifest, compacted further and reopened again.
//! 2. The same images after a power loss, where a table that was never fsynced comes back empty and
//!    one whose directory entry was never synced is gone.
//! 3. Table ids: the outputs of a compaction take their ids before the manifest that records the
//!    counter is written, so a reopened tree must not hand the same ids out again.
//! 4. A compaction that fails after its outputs are written.
//! 5. Commits that return while a compaction is at a stage are in the image taken there.
//! 6. A second compaction after the first settled, and a flush that lands during a compaction.
//! 7. The states a crash can leave around the manifest replacement: a temporary manifest beside the
//!    manifest, outputs beside the old manifest, inputs beside the new one.
//! 8. Randomised workloads, seeded, with an image at a random stage.

#![cfg(not(target_arch = "wasm32"))]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Cursor, Read};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;
use tokio::task::JoinHandle;

use super::collect_transaction_all;
use crate::compaction::compactor::{directory_syncs, CompactionStage};
use crate::compaction::{CompactionChoice, CompactionInput, CompactionStrategy};
use crate::error::Error;
use crate::levels::{fail_replacements, heal_replacements, LevelManifest, Levels, ReplaceFailure};
use crate::vfs::sync_tracker;
use crate::{Durability, Mode, Options, Result, Tree};

/// How long a test waits for a thread or a hook: one that never arrives fails the test instead of
/// hanging the suite.
const WATCHDOG: Duration = Duration::from_secs(45);

/// The keys of the workload are `key_0000` to `key_0079`.
const KEYS: usize = 80;

/// The levels of every tree here: L3 is the bottom level.
const LEVELS: usize = 4;

/// The stages of a compaction in the order it reaches them.
const ALL_STAGES: [CompactionStage; 4] = [
	CompactionStage::OutputsDurable,
	CompactionStage::ManifestWritten,
	CompactionStage::InputsRemoved,
	CompactionStage::VlogCleaned,
];

// ---------------------------------------------------------------------------
// the tree and its model
// ---------------------------------------------------------------------------

/// The options of every tree here. No background compaction can start (the L0 and level size
/// triggers are out of reach), so every compaction is one a test starts, and its hook is the
/// test's. The tables are small and the value-log files short, so a compaction writes several
/// outputs and a workload spreads over several value-log files.
fn opts(path: &Path) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		// Crash semantics: nothing is flushed on drop or close.
		flush_on_close: false,
		level_count: LEVELS as u8,
		enable_vlog: true,
		vlog_value_threshold: 1024,
		vlog_max_file_size: 8 * 1024,
		target_file_size: 2 * 1024,
		max_memtable_size: 8 * 1024 * 1024,
		level0_max_files: 10_000,
		l0_stall_threshold: 10_000,
		memtable_stall_threshold: 10_000,
		max_bytes_for_level: 1 << 40,
		..Default::default()
	})
}

fn open(dir: &Path) -> Tree {
	Tree::new(opts(dir)).unwrap_or_else(|e| panic!("opening {} failed: {e}", dir.display()))
}

/// Kills a tree the way a crash does: the lock is released and the background tasks end, and
/// nothing is flushed or closed. The caller drops it (current-thread runtime and no await before
/// the next open, so the best-effort close that a drop spawns never runs).
fn kill(tree: &Tree) {
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
	tree.core.is_closed.store(true, Ordering::SeqCst);
}

fn key(i: usize) -> Vec<u8> {
	format!("key_{i:04}").into_bytes()
}

/// The value of `key(i)` written under `tag`: 100 bytes, or 1500 bytes, which is separated into
/// the value log, when `big`.
fn val(tag: &str, i: usize, big: bool) -> Vec<u8> {
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

/// What the tree must hold: a value for a key that is set, `None` for one that is deleted.
#[derive(Clone, Default)]
struct Model {
	keys: BTreeMap<usize, Option<Vec<u8>>>,
}

impl Model {
	fn live(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
		self.keys.iter().filter_map(|(i, v)| v.as_ref().map(|v| (key(*i), v.clone()))).collect()
	}
}

enum Op {
	Set(usize, Vec<u8>),
	Del(usize),
	/// Deletes `key(from)` up to, but not including, `key(to)` with one range tombstone. A commit
	/// that holds one has no other operation on the keys of the range.
	DelRange(usize, usize),
}

/// Commits `ops` as one transaction and applies them to the model.
async fn commit(tree: &Tree, model: &mut Model, ops: Vec<Op>, durability: Durability) {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(durability);
	for op in ops {
		match op {
			Op::Set(i, v) => {
				txn.set(key(i), v.clone()).unwrap();
				model.keys.insert(i, Some(v));
			}
			Op::Del(i) => {
				txn.delete(key(i)).unwrap();
				model.keys.insert(i, None);
			}
			Op::DelRange(from, to) => {
				txn.delete_range(key(from), key(to)).unwrap();
				for i in from..to {
					model.keys.insert(i, None);
				}
			}
		}
	}
	txn.commit().await.unwrap();
}

/// Asserts that two values are the same without printing 1500 bytes of each.
fn same(what: &str, got: Option<&[u8]>, expected: Option<&[u8]>) {
	let brief = |v: Option<&[u8]>| match v {
		None => "absent".to_string(),
		Some(v) => format!(
			"{} bytes starting {:?}",
			v.len(),
			String::from_utf8_lossy(&v[..v.len().min(16)])
		),
	};
	assert!(got == expected, "{what}: got {}, expected {}", brief(got), brief(expected));
}

/// Every key reads its newest value or is absent, a scan returns exactly the model, and a range
/// scan returns the part of it.
fn verify_data(tree: &Tree, model: &Model, what: &str) {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	for (i, expected) in &model.keys {
		let got = txn.get(key(*i)).unwrap();
		same(&format!("{what}: get key {i}"), got.as_deref(), expected.as_deref());
	}
	let expected = model.live();
	let scanned = collect_transaction_all(&mut txn.iter().unwrap()).unwrap();
	assert_eq!(
		scanned.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
		expected.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
		"{what}: the keys of a full scan"
	);
	for ((k, got), (_, want)) in scanned.iter().zip(&expected) {
		same(&format!("{what}: scan {}", String::from_utf8_lossy(k)), Some(got), Some(want));
	}
	let (lo, hi) = (key(10), key(40));
	let ranged = collect_transaction_all(&mut txn.range(lo.clone(), hi.clone()).unwrap()).unwrap();
	let in_range: Vec<_> = expected.iter().filter(|(k, _)| *k >= lo && *k < hi).collect();
	assert_eq!(
		ranged.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
		in_range.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
		"{what}: the keys of a range scan"
	);
}

// ---------------------------------------------------------------------------
// compaction strategies
// ---------------------------------------------------------------------------

/// Merges every table of `source` and of `target` into `target`. A compaction here is always
/// the one the test asks for: the real strategy would pick nothing until a level is full.
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

// ---------------------------------------------------------------------------
// what is on disk
// ---------------------------------------------------------------------------

fn sst_path(dir: &Path, id: u64) -> PathBuf {
	dir.join("sstables").join(format!("{id:020}.sst"))
}

/// The ids of the table files in `dir`.
fn ssts(dir: &Path) -> BTreeSet<u64> {
	let mut ids = BTreeSet::new();
	for entry in fs::read_dir(dir.join("sstables")).unwrap() {
		let name = entry.unwrap().file_name().to_string_lossy().into_owned();
		if let Some(stem) = name.strip_suffix(".sst") {
			ids.insert(stem.parse::<u64>().unwrap_or_else(|_| panic!("odd table file {name}")));
		}
	}
	ids
}

/// The names of the value-log files in `dir`.
fn vlogs(dir: &Path) -> BTreeSet<String> {
	match fs::read_dir(dir.join("vlog")) {
		Ok(entries) => {
			entries.map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect()
		}
		Err(_) => BTreeSet::new(),
	}
}

/// The manifest file as the bytes on disk say, decoded without opening the tables.
struct OnDisk {
	next_table_id: u64,
	/// The table ids of each level, in id order.
	levels: Vec<Vec<u64>>,
}

fn manifest_path(dir: &Path) -> PathBuf {
	dir.join("manifest").join(format!("{:020}.manifest", 0))
}

fn read_manifest(dir: &Path) -> OnDisk {
	read_manifest_file(&manifest_path(dir))
}

fn read_manifest_file(path: &Path) -> OnDisk {
	let bytes = fs::read(path).unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
	let mut cursor = Cursor::new(bytes);
	let mut version = [0u8; 2];
	cursor.read_exact(&mut version).unwrap();
	let mut word = [0u8; 8];
	cursor.read_exact(&mut word).unwrap();
	let next_table_id = u64::from_be_bytes(word);
	cursor.read_exact(&mut word).unwrap(); // log number
	cursor.read_exact(&mut word).unwrap(); // last sequence
	let mut levels = Levels::decode(&mut cursor).unwrap();
	for level in &mut levels {
		level.sort_unstable();
	}
	OnDisk {
		next_table_id,
		levels,
	}
}

/// The table ids of each level of the open tree, in id order.
fn ids_by_level(tree: &Tree) -> Vec<Vec<u64>> {
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

fn flatten(levels: &[Vec<u64>]) -> BTreeSet<u64> {
	levels.iter().flatten().copied().collect()
}

/// A table is listed once: the manifest never names an id twice, in one level or two.
fn assert_no_duplicates(levels: &[Vec<u64>], what: &str) {
	let total: usize = levels.iter().map(Vec::len).sum();
	assert_eq!(flatten(levels).len(), total, "{what}: a table is listed twice in {levels:?}");
}

/// The files of the table directory are exactly the tables of the manifest: nothing is missing
/// and nothing is left over.
fn assert_files_match_manifest(tree: &Tree, dir: &Path, what: &str) {
	let live = ids_by_level(tree);
	assert_no_duplicates(&live, what);
	assert_eq!(ssts(dir), flatten(&live), "{what}: table files against the tables of the manifest");
}

// ---------------------------------------------------------------------------
// the layout
// ---------------------------------------------------------------------------

/// Which compaction a test stops.
#[derive(Clone, Copy, Debug)]
enum Shape {
	L0ToL1,
	L1ToL2,
	/// Into the bottom level, where tombstones and shadowed versions are dropped.
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

	fn target(self) -> u8 {
		self.source() + 1
	}
}

/// A tree in the layout every scenario starts from, with its model.
///
/// L3 holds a seed value of every key. L2 holds a second generation: some keys set again, one in
/// seven deleted, so that a tombstone in L2 hides a value in L3. L0 holds three tables that set
/// keys again (some of them to values in the value log), delete others and set some deleted ones
/// again. A few commits more are only in the log. Every compaction that does not reach the bottom
/// level has to keep the tombstones, or the older values below come back.
struct Layout {
	tree: Tree,
	model: Model,
}

async fn build(dir: &Path) -> Layout {
	let tree = open(dir);
	let mut model = Model::default();

	// The seeds, in the bottom level.
	let seeds = (0..KEYS).map(|i| Op::Set(i, val("seed", i, false))).collect();
	commit(&tree, &mut model, seeds, Durability::Eventual).await;
	tree.flush().unwrap();
	compact(&tree, 0, 3).unwrap();

	// The second generation, in L2.
	let mut ops = Vec::new();
	for i in 0..KEYS {
		if i % 7 == 0 {
			ops.push(Op::Del(i));
		} else if i % 3 == 0 {
			ops.push(Op::Set(i, val("mid", i, i % 9 == 0)));
		}
	}
	commit(&tree, &mut model, ops, Durability::Eventual).await;
	tree.flush().unwrap();
	compact(&tree, 0, 2).unwrap();

	// Three tables in L0.
	let mut r1 = Vec::new();
	for i in (0..KEYS).filter(|i| i % 2 == 0) {
		r1.push(Op::Set(i, val("r1", i, i % 10 == 0)));
	}
	commit(&tree, &mut model, r1, Durability::Eventual).await;
	tree.flush().unwrap();

	let mut r2 = Vec::new();
	for i in 0..KEYS {
		if i % 5 == 1 {
			r2.push(Op::Del(i));
		} else if i % 4 == 0 {
			r2.push(Op::Set(i, val("r2", i, i % 8 == 0)));
		}
	}
	commit(&tree, &mut model, r2, Durability::Eventual).await;
	tree.flush().unwrap();

	let mut r3 = Vec::new();
	for i in 0..KEYS {
		if i % 35 == 0 {
			// Keys that an older table deleted.
			r3.push(Op::Set(i, val("again", i, true)));
		} else if i % 6 == 0 {
			r3.push(Op::Set(i, val("r3", i, i % 12 == 0)));
		}
	}
	commit(&tree, &mut model, r3, Durability::Eventual).await;
	tree.flush().unwrap();

	// Acknowledged and durable, and only in the log.
	let tail = vec![
		Op::Set(2, val("tail", 2, false)),
		Op::Set(KEYS, val("tail", KEYS, true)),
		Op::Set(KEYS + 1, val("tail", KEYS + 1, false)),
		Op::Del(4),
		Op::Del(10),
	];
	commit(&tree, &mut model, tail, Durability::Immediate).await;

	let levels = ids_by_level(&tree);
	assert_eq!(levels[0].len(), 3, "three tables in L0: {levels:?}");
	assert!(
		levels[2].len() >= 2 && levels[3].len() >= 2,
		"L2 and L3 hold several tables: {levels:?}"
	);
	verify_data(&tree, &model, "the layout");
	Layout {
		tree,
		model,
	}
}

/// Runs the compactions that come before the one of `shape`, so that its inputs are in place.
fn advance(tree: &Tree, shape: Shape) {
	match shape {
		Shape::L0ToL1 => {}
		Shape::L1ToL2 => compact(tree, 0, 1).unwrap(),
		Shape::L2ToL3 => {
			compact(tree, 0, 1).unwrap();
			compact(tree, 1, 2).unwrap();
		}
	}
	let levels = ids_by_level(tree);
	let source = shape.source() as usize;
	assert!(!levels[source].is_empty(), "{shape:?}: nothing to compact in L{source}: {levels:?}");
	assert!(
		source == 0 || levels[source].len() >= 2,
		"{shape:?}: the source level holds several tables: {levels:?}"
	);
}

// ---------------------------------------------------------------------------
// crash images
// ---------------------------------------------------------------------------

/// A copy of the database directory taken at a compaction stage.
#[derive(Clone, Debug)]
struct Image {
	stage: CompactionStage,
	dir: PathBuf,
	/// The tables of the image that no fsync had been recorded for when it was taken: after a
	/// power loss they may be empty.
	unsynced: Vec<u64>,
	/// How many directory fsyncs of the table directory had completed.
	dir_syncs: usize,
}

/// Copies a database directory as a crash leaves it. The log goes first and the value log after:
/// a log record points to a value that was written before it, so a value-log file copied later
/// holds every value a copied record points to.
fn copy_image(src: &Path, dst: &Path) {
	fs::create_dir_all(dst).unwrap();
	let mut entries: Vec<_> = fs::read_dir(src).unwrap().map(|e| e.unwrap()).collect();
	entries.sort_by_key(|e| e.file_name() != "wal");
	for entry in entries {
		if entry.file_name() == "LOCK" {
			continue;
		}
		let to = dst.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_image(&entry.path(), &to);
		} else {
			fs::copy(entry.path(), &to).unwrap();
		}
	}
}

fn take_image(db: &Path, dst: &Path, stage: CompactionStage) -> Image {
	let unsynced =
		ssts(db).into_iter().filter(|id| !sync_tracker::was_synced(&sst_path(db, *id))).collect();
	let dir_syncs = directory_syncs::count(&db.join("sstables"));
	copy_image(db, dst);
	Image {
		stage,
		dir: dst.to_path_buf(),
		unsynced,
		dir_syncs,
	}
}

/// What a test runs when a compaction reaches a stage.
type StageAction = Arc<dyn Fn(CompactionStage) + Send + Sync>;

/// The observer of the compactions of a tree: it records the stages, takes crash images, can
/// fail a compaction at `OutputsDurable`, and runs an action of the test at a stage.
struct Probe {
	db: PathBuf,
	root: PathBuf,
	capture: Vec<CompactionStage>,
	fired: Mutex<Vec<CompactionStage>>,
	images: Mutex<Vec<Image>>,
	/// The compactions that fail at `OutputsDurable` before one succeeds.
	failures: AtomicUsize,
	/// Runs at a stage before the image of it is copied.
	before: Mutex<Option<StageAction>>,
	/// Runs at a stage after the image of it is copied.
	action: Mutex<Option<StageAction>>,
}

impl Probe {
	fn new(db: &Path, root: &Path, capture: &[CompactionStage]) -> Arc<Self> {
		Arc::new(Self {
			db: db.to_path_buf(),
			root: root.to_path_buf(),
			capture: capture.to_vec(),
			fired: Mutex::new(Vec::new()),
			images: Mutex::new(Vec::new()),
			failures: AtomicUsize::new(0),
			before: Mutex::new(None),
			action: Mutex::new(None),
		})
	}

	fn install(self: &Arc<Self>, tree: &Tree) {
		let probe = Arc::clone(self);
		*tree.core.inner.compaction_stage_hook.lock() =
			Some(Arc::new(move |stage| probe.reach(stage)));
	}

	fn uninstall(tree: &Tree) {
		*tree.core.inner.compaction_stage_hook.lock() = None;
	}

	fn reach(&self, stage: CompactionStage) -> Result<()> {
		let number = {
			let mut fired = self.fired.lock().unwrap();
			fired.push(stage);
			fired.len()
		};
		let before = self.before.lock().unwrap().clone();
		if let Some(before) = before {
			before(stage);
		}
		if self.capture.contains(&stage) {
			let dst = self.root.join(format!("{number:03}-{stage:?}"));
			let image = take_image(&self.db, &dst, stage);
			self.images.lock().unwrap().push(image);
		}
		let action = self.action.lock().unwrap().clone();
		if let Some(action) = action {
			action(stage);
		}
		if stage == CompactionStage::OutputsDurable
			&& self
				.failures
				.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
				.is_ok()
		{
			return Err(Error::Io(std::io::Error::other("injected compaction failure").into()));
		}
		Ok(())
	}

	fn fired(&self) -> Vec<CompactionStage> {
		self.fired.lock().unwrap().clone()
	}

	fn images(&self) -> Vec<Image> {
		self.images.lock().unwrap().clone()
	}

	fn image(&self, stage: CompactionStage) -> Image {
		self.images()
			.into_iter()
			.find(|i| i.stage == stage)
			.unwrap_or_else(|| panic!("no image at {stage:?}"))
	}
}

/// What a compaction did, as the tree that ran it shows after the compaction: the tables of each
/// level before, which of them it merged, the tables it wrote and where.
struct Plan {
	before: Vec<Vec<u64>>,
	target: u8,
	input: BTreeSet<u64>,
	outputs: BTreeSet<u64>,
}

impl Plan {
	/// `before` and `after` are the tables of each level around a compaction of `source` and
	/// `target` that merged every table of the two.
	fn new(before: Vec<Vec<u64>>, after: &[Vec<u64>], source: u8, target: u8) -> Self {
		Self::with_outputs(before, after, source, target, 2)
	}

	/// As `new`, for a compaction whose output may be a single table.
	fn with_outputs(
		before: Vec<Vec<u64>>,
		after: &[Vec<u64>],
		source: u8,
		target: u8,
		at_least: usize,
	) -> Self {
		let mut input: BTreeSet<u64> = before[source as usize].iter().copied().collect();
		input.extend(before[target as usize].iter().copied());
		let outputs: BTreeSet<u64> = after[target as usize]
			.iter()
			.copied()
			.filter(|id| !flatten(&before).contains(id))
			.collect();
		assert!(
			outputs.len() >= at_least,
			"the target file size splits the output into several tables: {before:?} -> {after:?}"
		);
		for id in &outputs {
			assert!(!input.contains(id), "an output reuses the id {id} of an input");
		}
		Self {
			before,
			target,
			input,
			outputs,
		}
	}

	/// The tables of each level the manifest lists once the compaction replaced its inputs.
	fn after(&self) -> Vec<Vec<u64>> {
		let mut levels: Vec<Vec<u64>> = self
			.before
			.iter()
			.map(|level| level.iter().copied().filter(|id| !self.input.contains(id)).collect())
			.collect();
		levels[self.target as usize].extend(self.outputs.iter().copied());
		for level in &mut levels {
			level.sort_unstable();
		}
		levels
	}

	/// The table files in the directory at `stage`. `landed` are tables a flush added to L0.
	fn files_at(&self, stage: CompactionStage, landed: &[u64]) -> BTreeSet<u64> {
		let mut files = flatten(&self.before);
		files.extend(landed.iter().copied());
		match stage {
			CompactionStage::OutputsDurable | CompactionStage::ManifestWritten => {
				files.extend(self.outputs.iter().copied());
			}
			CompactionStage::InputsRemoved | CompactionStage::VlogCleaned => {
				files.extend(self.outputs.iter().copied());
				for id in &self.input {
					files.remove(id);
				}
			}
		}
		files
	}
}

/// What the directory of a crash image holds before anything opens it, stage by stage.
fn assert_raw_image(image: &Image, plan: &Plan, landed: &[u64], orphans: &[u64]) {
	let stage = image.stage;
	let manifest = read_manifest(&image.dir);
	let mut expected = match stage {
		CompactionStage::OutputsDurable => plan.before.clone(),
		_ => plan.after(),
	};
	expected[0].extend(landed.iter().copied());
	expected[0].sort_unstable();
	assert_eq!(manifest.levels, expected, "{stage:?}: the tables the manifest lists");
	assert_no_duplicates(&manifest.levels, &format!("{stage:?}"));
	let mut files = plan.files_at(stage, landed);
	files.extend(orphans.iter().copied());
	assert_eq!(
		ssts(&image.dir),
		files,
		"{stage:?}: the table files in the image; before {:?}, outputs {:?}, landed {landed:?}, \
		 orphans {orphans:?}",
		plan.before,
		plan.outputs
	);
	if stage != CompactionStage::OutputsDurable {
		let max_output = *plan.outputs.iter().max().unwrap();
		assert!(
			manifest.next_table_id > max_output,
			"{stage:?}: the manifest records a counter of {} that does not cover the output \
			 {max_output}",
			manifest.next_table_id
		);
	}
	// Every table is durable, and every output was before the manifest could list it.
	assert!(
		image.unsynced.is_empty(),
		"{stage:?}: tables {:?} had not been fsynced when the image was taken",
		image.unsynced
	);
	for id in &plan.outputs {
		assert!(
			fs::metadata(sst_path(&image.dir, *id)).unwrap().len() > 0,
			"{stage:?}: the output {id} is empty"
		);
	}
}

/// What a power loss leaves of an image: a table that was never fsynced is empty, and the
/// outputs of a compaction whose directory fsync had not completed are gone.
fn power_loss(image: &Image, outputs: &BTreeSet<u64>, before_syncs: usize, root: &Path) -> PathBuf {
	let dst = root.join(format!("power-loss-{:?}", image.stage));
	if dst.exists() {
		fs::remove_dir_all(&dst).unwrap();
	}
	copy_image(&image.dir, &dst);
	for id in &image.unsynced {
		fs::OpenOptions::new().write(true).open(sst_path(&dst, *id)).unwrap().set_len(0).unwrap();
	}
	if image.dir_syncs <= before_syncs {
		for id in outputs {
			let _ = fs::remove_file(sst_path(&dst, *id));
		}
	}
	dst
}

// ---------------------------------------------------------------------------
// reopening an image
// ---------------------------------------------------------------------------

/// What an open directory holds, to compare the second open of an image with the first.
#[derive(Debug, PartialEq, Eq)]
struct Snapshot {
	levels: Vec<Vec<u64>>,
	ssts: BTreeSet<u64>,
	vlogs: BTreeSet<String>,
}

fn snapshot(tree: &Tree, dir: &Path) -> Snapshot {
	Snapshot {
		levels: ids_by_level(tree),
		ssts: ssts(dir),
		vlogs: vlogs(dir),
	}
}

/// Opens the image in `dir` and checks what recovery made of it: the data, the tables of the
/// manifest (plus any that a replay flushed to L0), and no file that the manifest does not list.
fn open_and_check(dir: &Path, model: &Model, what: &str) -> (Tree, OnDisk) {
	let raw = read_manifest(dir);
	let tree = open(dir);
	verify_data(&tree, model, what);
	let live = ids_by_level(&tree);
	for (level, ids) in live.iter().enumerate() {
		if level == 0 {
			for id in &raw.levels[0] {
				assert!(
					ids.contains(id),
					"{what}: L0 lost the table {id}: {live:?} from {:?}",
					raw.levels
				);
			}
			for id in ids.iter().filter(|id| !raw.levels[0].contains(id)) {
				assert!(
					*id >= raw.next_table_id,
					"{what}: a table {id} that a replay flushed reuses an id below the counter {}",
					raw.next_table_id
				);
			}
		} else {
			assert_eq!(ids, &raw.levels[level], "{what}: the tables of L{level}");
		}
	}
	assert_files_match_manifest(&tree, dir, what);
	(tree, raw)
}

/// Opens the image twice and compares the two opens. With `deep` it then keeps going on the
/// tree: new writes and flushes, a compaction down every level, and a last crash and open that
/// has to find all of it.
async fn recover(dir: &Path, model: &Model, deep: bool, what: &str) {
	let (tree, raw) = open_and_check(dir, model, &format!("{what}: first open"));
	let first = snapshot(&tree, dir);
	kill(&tree);
	drop(tree);

	let (tree, _) = open_and_check(dir, model, &format!("{what}: second open"));
	assert_eq!(snapshot(&tree, dir), first, "{what}: a second open changes nothing");
	if !deep {
		kill(&tree);
		return;
	}

	// New data and new tables after the reopen: nothing collides with a table of the image, or
	// with an output it left behind, and everything stays intact.
	let mut model = model.clone();
	let mut known = flatten(&ids_by_level(&tree));
	let mut new_ids = Vec::new();
	for round in 0..3usize {
		let ops = vec![
			Op::Set(5, val(&format!("post{round}"), 5, round == 1)),
			Op::Set(KEYS + 10 + round, val("post", KEYS + 10 + round, round == 2)),
			Op::Del(11 + round),
		];
		commit(&tree, &mut model, ops, Durability::Eventual).await;
		tree.flush().unwrap();
		let now = ids_by_level(&tree);
		let added: Vec<u64> = now[0].iter().copied().filter(|id| !known.contains(id)).collect();
		assert_eq!(added.len(), 1, "{what}: flush {round} adds one table: {now:?}");
		assert!(
			added[0] >= raw.next_table_id,
			"{what}: the new table {} is below the counter {} that the manifest recorded",
			added[0],
			raw.next_table_id
		);
		known.insert(added[0]);
		new_ids.push(added[0]);
	}
	verify_data(&tree, &model, &format!("{what}: after new flushes"));
	assert_files_match_manifest(&tree, dir, &format!("{what}: after new flushes"));

	// A compaction down every level succeeds, and the data survives it.
	for level in 0..(LEVELS as u8 - 1) {
		let levels = ids_by_level(&tree);
		if levels[level as usize].is_empty() {
			continue;
		}
		compact(&tree, level, level + 1)
			.unwrap_or_else(|e| panic!("{what}: compacting L{level} after the reopen failed: {e}"));
		let now = ids_by_level(&tree);
		assert!(now[level as usize].is_empty(), "{what}: L{level} was merged away: {now:?}");
		assert_ne!(
			now[level as usize + 1],
			levels[level as usize + 1],
			"{what}: the compaction of L{level} wrote nothing to L{}",
			level + 1
		);
		verify_data(&tree, &model, &format!("{what}: after compacting L{level}"));
		assert_files_match_manifest(&tree, dir, &format!("{what}: after compacting L{level}"));
	}
	kill(&tree);
	drop(tree);

	// And the result survives one more crash.
	let (tree, _) = open_and_check(dir, &model, &format!("{what}: last open"));
	kill(&tree);
}

// ---------------------------------------------------------------------------
// 1 and 2: every stage of every compaction, and the power loss
// ---------------------------------------------------------------------------

/// Stops a compaction of `shape` at `stage`, takes the image there, lets the compaction finish
/// and checks the image: raw, reopened, after a power loss.
async fn stage_scenario(shape: Shape, stage: CompactionStage) {
	let dir = TempDir::new("cc-db").unwrap();
	let images = TempDir::new("cc-images").unwrap();
	let db = dir.path().join("db");
	let Layout {
		tree,
		model,
	} = build(&db).await;
	advance(&tree, shape);

	let before = ids_by_level(&tree);
	let before_syncs = directory_syncs::count(&db.join("sstables"));
	let probe = Probe::new(&db, images.path(), &[stage]);
	probe.install(&tree);
	compact(&tree, shape.source(), shape.target()).unwrap();
	Probe::uninstall(&tree);
	assert_eq!(probe.fired(), ALL_STAGES.to_vec(), "{shape:?}: the stages of one compaction");

	let after = ids_by_level(&tree);
	let plan = Plan::new(before, &after, shape.source(), shape.target());
	assert_eq!(after, plan.after(), "{shape:?}: the live tree after the compaction");
	verify_data(&tree, &model, &format!("{shape:?}: live tree after the compaction"));
	assert_files_match_manifest(&tree, &db, &format!("{shape:?}: live tree after the compaction"));

	let image = probe.image(stage);
	let what = format!("{shape:?} at {stage:?}");
	assert!(
		image.dir_syncs > before_syncs,
		"{what}: the table directory was fsynced before the image"
	);
	assert_raw_image(&image, &plan, &[], &[]);
	// Recovery changes the directory it opens, so it works on a copy of the image.
	let work = images.path().join(format!("work-{stage:?}"));
	copy_image(&image.dir, &work);
	recover(&work, &model, true, &what).await;

	let lost = power_loss(&image, &plan.outputs, before_syncs, images.path());
	recover(&lost, &model, false, &format!("{what} after a power loss")).await;
	kill(&tree);
}

macro_rules! stage_tests {
	($($name:ident: $shape:expr, $stage:expr;)*) => {
		$(
			#[test(tokio::test)]
			async fn $name() {
				stage_scenario($shape, $stage).await
			}
		)*
	};
}

stage_tests! {
	a_crash_at_outputs_durable_of_an_l0_to_l1_compaction_keeps_every_acknowledged_key: Shape::L0ToL1, CompactionStage::OutputsDurable;
	a_crash_after_the_manifest_write_of_an_l0_to_l1_compaction_keeps_every_acknowledged_key: Shape::L0ToL1, CompactionStage::ManifestWritten;
	a_crash_after_the_inputs_are_removed_of_an_l0_to_l1_compaction_keeps_every_acknowledged_key: Shape::L0ToL1, CompactionStage::InputsRemoved;
	a_crash_after_the_vlog_cleanup_of_an_l0_to_l1_compaction_keeps_every_acknowledged_key: Shape::L0ToL1, CompactionStage::VlogCleaned;
	a_crash_at_outputs_durable_of_an_l1_to_l2_compaction_keeps_every_acknowledged_key: Shape::L1ToL2, CompactionStage::OutputsDurable;
	a_crash_after_the_manifest_write_of_an_l1_to_l2_compaction_keeps_every_acknowledged_key: Shape::L1ToL2, CompactionStage::ManifestWritten;
	a_crash_after_the_inputs_are_removed_of_an_l1_to_l2_compaction_keeps_every_acknowledged_key: Shape::L1ToL2, CompactionStage::InputsRemoved;
	a_crash_after_the_vlog_cleanup_of_an_l1_to_l2_compaction_keeps_every_acknowledged_key: Shape::L1ToL2, CompactionStage::VlogCleaned;
	a_crash_at_outputs_durable_of_a_bottom_level_compaction_keeps_every_acknowledged_key: Shape::L2ToL3, CompactionStage::OutputsDurable;
	a_crash_after_the_manifest_write_of_a_bottom_level_compaction_keeps_every_acknowledged_key: Shape::L2ToL3, CompactionStage::ManifestWritten;
	a_crash_after_the_inputs_are_removed_of_a_bottom_level_compaction_keeps_every_acknowledged_key: Shape::L2ToL3, CompactionStage::InputsRemoved;
	a_crash_after_the_vlog_cleanup_of_a_bottom_level_compaction_keeps_every_acknowledged_key: Shape::L2ToL3, CompactionStage::VlogCleaned;
}

// ---------------------------------------------------------------------------
// 1 again: the stages one after the other, and the value log
// ---------------------------------------------------------------------------

/// One compaction of each shape with an image at every stage: each image is the directory one
/// step further, and the value log loses files only at the last stage.
#[test(tokio::test)]
async fn the_four_stages_of_a_compaction_leave_four_directories_one_step_apart() {
	for shape in [Shape::L0ToL1, Shape::L1ToL2, Shape::L2ToL3] {
		let dir = TempDir::new("cc-db").unwrap();
		let images = TempDir::new("cc-images").unwrap();
		let db = dir.path().join("db");
		let Layout {
			tree,
			model,
		} = build(&db).await;
		advance(&tree, shape);
		let before = ids_by_level(&tree);
		let before_syncs = directory_syncs::count(&db.join("sstables"));

		let probe = Probe::new(&db, images.path(), &ALL_STAGES);
		probe.install(&tree);
		compact(&tree, shape.source(), shape.target()).unwrap();
		Probe::uninstall(&tree);
		assert_eq!(probe.fired(), ALL_STAGES.to_vec(), "{shape:?}: the stages");
		let plan = Plan::new(before, &ids_by_level(&tree), shape.source(), shape.target());

		let found = probe.images();
		assert_eq!(found.len(), 4, "{shape:?}: one image per stage");
		for image in &found {
			assert!(
				image.dir_syncs > before_syncs,
				"{shape:?}: {:?}: directory fsync",
				image.stage
			);
			assert_raw_image(image, &plan, &[], &[]);
			let work = images.path().join(format!("work-{:?}", image.stage));
			copy_image(&image.dir, &work);
			recover(&work, &model, false, &format!("{shape:?} at {:?}", image.stage)).await;
		}

		// Nothing leaves the value log before the inputs are gone, and nothing but the files that
		// no table or memtable needs leaves it after.
		let logs: Vec<BTreeSet<String>> = found.iter().map(|i| vlogs(&i.dir)).collect();
		assert!(!logs[0].is_empty(), "{shape:?}: the workload uses the value log");
		assert_eq!(logs[0], logs[1], "{shape:?}: the manifest write removes no value-log file");
		assert_eq!(logs[1], logs[2], "{shape:?}: removing the inputs removes no value-log file");
		assert!(logs[3].is_subset(&logs[2]), "{shape:?}: the cleanup only removes value-log files");
		kill(&tree);
	}
}

/// The ids of the value-log files in a set of names.
fn vlog_ids(names: &BTreeSet<String>) -> Vec<u64> {
	names.iter().map(|n| n.split('.').next().unwrap().parse().unwrap()).collect()
}

/// Every value of the first tables is overwritten in later tables, so the compaction drops the
/// old versions, and the value-log files that held them go at the last stage and not before. The
/// files that remain are exactly the ones the values of the output point into, and the images on
/// both sides of the removal read every value.
#[test(tokio::test)]
async fn the_value_log_files_of_overwritten_values_are_removed_at_the_last_stage_only() {
	let dir = TempDir::new("cc-db").unwrap();
	let images = TempDir::new("cc-images").unwrap();
	let db = dir.path().join("db");
	let tree = open(&db);
	let mut model = Model::default();
	for generation in ["old", "new"] {
		let ops = (0..24).map(|i| Op::Set(i, val(generation, i, true))).collect();
		commit(&tree, &mut model, ops, Durability::Eventual).await;
		tree.flush().unwrap();
	}
	// Unflushed, and in the newest value-log file.
	commit(&tree, &mut model, vec![Op::Set(30, val("tail", 30, true))], Durability::Immediate)
		.await;
	let before = ids_by_level(&tree);
	assert_eq!(before[0].len(), 2);
	let logs_before = vlogs(&db);
	assert!(logs_before.len() >= 8, "the workload spreads over several value-log files");

	let before_syncs = directory_syncs::count(&db.join("sstables"));
	let probe = Probe::new(&db, images.path(), &ALL_STAGES);
	probe.install(&tree);
	compact(&tree, 0, 1).unwrap();
	Probe::uninstall(&tree);
	assert_eq!(probe.fired(), ALL_STAGES.to_vec());
	let plan = Plan::with_outputs(before, &ids_by_level(&tree), 0, 1, 1);

	let found = probe.images();
	let logs: Vec<BTreeSet<String>> = found.iter().map(|i| vlogs(&i.dir)).collect();
	assert_eq!(logs[0], logs_before);
	assert_eq!(logs[1], logs_before, "the manifest write keeps every value-log file");
	assert_eq!(logs[2], logs_before, "removing the inputs keeps every value-log file");
	assert!(
		logs[3].len() < logs[2].len(),
		"the cleanup removes the files of the overwritten values: {:?} -> {:?}",
		logs[2],
		logs[3]
	);
	// What goes is the oldest files, and the output's values are all in what stays.
	let kept = vlog_ids(&logs[3]);
	let removed: Vec<u64> =
		vlog_ids(&logs[2]).into_iter().filter(|id| !kept.contains(id)).collect();
	assert!(removed.iter().max() < kept.iter().min(), "removed {removed:?}, kept {kept:?}");

	for image in &found {
		assert_raw_image(image, &plan, &[], &[]);
		assert!(image.dir_syncs > before_syncs);
		let work = images.path().join(format!("work-{:?}", image.stage));
		copy_image(&image.dir, &work);
		recover(
			&work,
			&model,
			image.stage == CompactionStage::VlogCleaned,
			&format!("{:?}", image.stage),
		)
		.await;
	}
	verify_data(&tree, &model, "the live tree");
	kill(&tree);
}

// ---------------------------------------------------------------------------
// 3: table ids
// ---------------------------------------------------------------------------

/// The outputs of a compaction have ids the manifest does not know when it crashes before the
/// manifest write. After the reopen the tree flushes enough new tables to cover the ids of the
/// outputs it left behind: none of them collides with another table, and the data stays intact
/// when each of them is made, when it is compacted and across a crash.
#[test(tokio::test)]
async fn tables_flushed_after_a_crash_before_the_manifest_write_do_not_collide_with_the_orphans() {
	let dir = TempDir::new("cc-db").unwrap();
	let images = TempDir::new("cc-images").unwrap();
	let db = dir.path().join("db");
	let Layout {
		tree,
		model,
	} = build(&db).await;
	let before = ids_by_level(&tree);
	let probe = Probe::new(&db, images.path(), &[CompactionStage::OutputsDurable]);
	probe.install(&tree);
	compact(&tree, 0, 1).unwrap();
	Probe::uninstall(&tree);
	let plan = Plan::new(before, &ids_by_level(&tree), 0, 1);
	let image = probe.image(CompactionStage::OutputsDurable);
	kill(&tree);
	drop(tree);

	let raw = read_manifest(&image.dir);
	let orphans: Vec<u64> = plan.outputs.iter().copied().collect();
	assert!(
		orphans.iter().all(|id| *id >= raw.next_table_id),
		"the old manifest's counter {} is below the outputs {orphans:?}",
		raw.next_table_id
	);
	let tree = open(&image.dir);
	let mut model = model;
	assert_eq!(ssts(&image.dir), flatten(&ids_by_level(&tree)), "the orphans are reclaimed");
	let mut known = flatten(&ids_by_level(&tree));
	let top = *orphans.iter().max().unwrap();
	let mut round = 0usize;
	let mut new_ids = Vec::new();
	// Flush until the new ids pass every orphan id.
	while new_ids.iter().max().is_none_or(|id| *id <= top) {
		assert!(round < 40, "the new ids never passed the orphans {orphans:?}: {new_ids:?}");
		let ops = vec![
			Op::Set(round % KEYS, val(&format!("fresh{round}"), round % KEYS, round % 3 == 0)),
			Op::Set(KEYS + 20 + round, val("fresh", KEYS + 20 + round, false)),
		];
		commit(&tree, &mut model, ops, Durability::Eventual).await;
		tree.flush().unwrap();
		let now = ids_by_level(&tree);
		let added: Vec<u64> = now[0].iter().copied().filter(|id| !known.contains(id)).collect();
		assert_eq!(added.len(), 1, "flush {round} adds exactly one table: {now:?}");
		assert!(
			added[0] >= raw.next_table_id,
			"the new table {} is below the counter {}",
			added[0],
			raw.next_table_id
		);
		known.insert(added[0]);
		new_ids.push(added[0]);
		verify_data(&tree, &model, &format!("after flush {round}"));
		assert_files_match_manifest(&tree, &image.dir, &format!("after flush {round}"));
		round += 1;
	}
	let distinct: BTreeSet<u64> = new_ids.iter().copied().collect();
	assert_eq!(distinct.len(), new_ids.len(), "every new table has its own id: {new_ids:?}");

	// All of it, compacted down, and across a crash.
	for level in 0..(LEVELS as u8 - 1) {
		compact(&tree, level, level + 1).unwrap();
		verify_data(&tree, &model, &format!("after compacting L{level}"));
	}
	assert_files_match_manifest(&tree, &image.dir, "after compacting everything");
	kill(&tree);
	drop(tree);
	let (tree, _) = open_and_check(&image.dir, &model, "after the second crash");
	kill(&tree);
}

/// After the manifest write the counter it recorded covers every output: the tables flushed after
/// a reopen all get ids above the outputs, and none reuses the id of an input or an output.
#[test(tokio::test)]
async fn tables_flushed_after_a_crash_after_the_manifest_write_get_ids_above_every_output() {
	let dir = TempDir::new("cc-db").unwrap();
	let images = TempDir::new("cc-images").unwrap();
	let db = dir.path().join("db");
	let Layout {
		tree,
		model,
	} = build(&db).await;
	let before = ids_by_level(&tree);
	let probe = Probe::new(&db, images.path(), &[CompactionStage::ManifestWritten]);
	probe.install(&tree);
	compact(&tree, 0, 1).unwrap();
	Probe::uninstall(&tree);
	let plan = Plan::new(before, &ids_by_level(&tree), 0, 1);
	let image = probe.image(CompactionStage::ManifestWritten);
	let ever_used: BTreeSet<u64> = flatten(&plan.before).union(&plan.outputs).copied().collect();
	kill(&tree);
	drop(tree);

	let raw = read_manifest(&image.dir);
	assert!(raw.next_table_id > *ever_used.iter().max().unwrap(), "counter {}", raw.next_table_id);
	let (tree, _) = open_and_check(&image.dir, &model, "after the manifest write");
	let mut model = model;
	let mut new_ids = Vec::new();
	for round in 0..4usize {
		let ops = vec![Op::Set(round, val(&format!("fresh{round}"), round, false))];
		commit(&tree, &mut model, ops, Durability::Eventual).await;
		tree.flush().unwrap();
		let now = ids_by_level(&tree);
		let added: Vec<u64> = now[0]
			.iter()
			.copied()
			.filter(|id| !flatten(&plan.after()).contains(id) && !new_ids.contains(id))
			.collect();
		assert_eq!(added.len(), 1, "flush {round} adds one table: {now:?}");
		assert!(!ever_used.contains(&added[0]), "the new table {} reuses an old id", added[0]);
		new_ids.push(added[0]);
	}
	verify_data(&tree, &model, "after the new tables");
	assert_files_match_manifest(&tree, &image.dir, "after the new tables");
	kill(&tree);
}

// ---------------------------------------------------------------------------
// 4: a compaction that fails
// ---------------------------------------------------------------------------

/// The hook fails the compaction at `OutputsDurable`, after the outputs are written. The inputs
/// are visible again and every read is right, nothing is lost or listed twice, an image taken
/// after the failure recovers (the failed outputs are reclaimed), and a retry succeeds with ids
/// of its own and leaves an image that recovers as well.
#[test(tokio::test)]
async fn a_compaction_that_fails_after_its_outputs_are_written_loses_nothing_and_can_be_retried() {
	for shape in [Shape::L0ToL1, Shape::L1ToL2] {
		let dir = TempDir::new("cc-db").unwrap();
		let images = TempDir::new("cc-images").unwrap();
		let retry_images = TempDir::new("cc-retry").unwrap();
		let db = dir.path().join("db");
		let Layout {
			tree,
			model,
		} = build(&db).await;
		advance(&tree, shape);
		let before = ids_by_level(&tree);
		let files_before = ssts(&db);

		let probe = Probe::new(&db, images.path(), &[CompactionStage::OutputsDurable]);
		probe.failures.store(1, Ordering::SeqCst);
		probe.install(&tree);
		let result = compact(&tree, shape.source(), shape.target());
		Probe::uninstall(&tree);
		let error = result.expect_err("the compaction fails when its hook does");
		assert!(error.to_string().contains("injected compaction failure"), "{shape:?}: {error}");
		assert_eq!(
			probe.fired(),
			vec![CompactionStage::OutputsDurable],
			"{shape:?}: a failed compaction goes no further"
		);

		// The tree: the same tables, none hidden, and every read right.
		assert_eq!(ids_by_level(&tree), before, "{shape:?}: the tables after the failure");
		assert!(
			tree.core.inner.level_manifest.read().unwrap().hidden_set.is_empty(),
			"{shape:?}: no table stays hidden from readers"
		);
		verify_data(&tree, &model, &format!("{shape:?}: after the failed compaction"));
		let orphans: Vec<u64> = ssts(&db).difference(&files_before).copied().collect();
		assert!(orphans.len() >= 2, "{shape:?}: the failed outputs stay on disk: {orphans:?}");

		// A crash after the failure: the image holds the old manifest and the failed outputs.
		let after_failure =
			take_image(&db, &images.path().join("after-failure"), CompactionStage::OutputsDurable);
		assert_eq!(read_manifest(&after_failure.dir).levels, before, "{shape:?}: the manifest");
		let work = images.path().join("work-after-failure");
		copy_image(&after_failure.dir, &work);
		recover(&work, &model, true, &format!("{shape:?} after the failed compaction")).await;

		// The retry succeeds and takes ids of its own.
		let retry = Probe::new(&db, retry_images.path(), &ALL_STAGES);
		retry.install(&tree);
		compact(&tree, shape.source(), shape.target()).unwrap();
		Probe::uninstall(&tree);
		assert_eq!(retry.fired(), ALL_STAGES.to_vec(), "{shape:?}: the stages of the retry");
		let after = ids_by_level(&tree);
		let plan = Plan::new(before.clone(), &after, shape.source(), shape.target());
		assert!(
			plan.outputs.iter().all(|id| !orphans.contains(id)),
			"{shape:?}: the retry reuses the id of a failed output: {:?} and {orphans:?}",
			plan.outputs
		);
		assert_no_duplicates(&after, &format!("{shape:?}: after the retry"));
		verify_data(&tree, &model, &format!("{shape:?}: after the retry"));
		let mut files = flatten(&after);
		files.extend(orphans.iter().copied());
		assert_eq!(ssts(&db), files, "{shape:?}: the failed outputs are all that is left over");

		for image in retry.images() {
			assert_raw_image(&image, &plan, &[], &orphans);
			let work = retry_images.path().join(format!("work-{:?}", image.stage));
			copy_image(&image.dir, &work);
			recover(&work, &model, false, &format!("{shape:?} retry at {:?}", image.stage)).await;
		}
		let last = take_image(&db, &retry_images.path().join("last"), CompactionStage::VlogCleaned);
		recover(&last.dir, &model, true, &format!("{shape:?} after the retry")).await;
		kill(&tree);
	}
}

/// Removes the failpoint of a manifest when the test ends, however it ends.
struct Healed(PathBuf);

impl Drop for Healed {
	fn drop(&mut self) {
		heal_replacements(&self.0);
	}
}

/// The manifest write of a compaction fails, after its outputs are written and fsynced. Before the
/// rename the tree must be put back exactly as it was: the same tables in the same levels (the
/// in-memory change is reverted), none hidden, every read right, the manifest on disk untouched
/// and no temporary file beside it. After the rename (the failure of a sync that follows it) the
/// new manifest may be in place, so the tree stops and the directory is the one of a crash after
/// the manifest write. In both cases a crash image recovers every key.
#[test(tokio::test)]
async fn a_failed_manifest_write_inside_a_compaction_leaves_the_tree_and_the_directory_right() {
	for (shape, at) in [
		(Shape::L0ToL1, ReplaceFailure::BeforeRename),
		(Shape::L1ToL2, ReplaceFailure::BeforeRename),
		(Shape::L2ToL3, ReplaceFailure::BeforeRename),
		(Shape::L0ToL1, ReplaceFailure::AfterRename),
	] {
		let what = format!("{shape:?}, a manifest write failing {at:?}");
		let dir = TempDir::new("cc-db").unwrap();
		let images = TempDir::new("cc-images").unwrap();
		let db = dir.path().join("db");
		let Layout {
			tree,
			model,
		} = build(&db).await;
		advance(&tree, shape);
		let before = ids_by_level(&tree);
		let files_before = ssts(&db);
		let manifest = tree.core.inner.level_manifest.read().unwrap().path.clone();
		let manifest_before = fs::read(&manifest).unwrap();
		let _healed = Healed(manifest.clone());

		let probe = Probe::new(&db, images.path(), &[]);
		probe.install(&tree);
		fail_replacements(&manifest, at, 0, 1);
		let result = compact(&tree, shape.source(), shape.target());
		Probe::uninstall(&tree);
		let error = result.expect_err("the compaction fails when its manifest write does");
		assert_eq!(
			probe.fired(),
			vec![CompactionStage::OutputsDurable],
			"{what}: a compaction whose manifest write failed goes no further ({error})"
		);
		let orphans: Vec<u64> = ssts(&db).difference(&files_before).copied().collect();
		assert!(orphans.len() >= 2, "{what}: the outputs stay on disk: {orphans:?}");
		assert!(
			fs::read_dir(manifest.parent().unwrap()).unwrap().count() == 1,
			"{what}: a temporary manifest was left beside the manifest"
		);
		verify_data(&tree, &model, &format!("{what}: live tree after the failure"));

		let raw = read_manifest(&db);
		match at {
			ReplaceFailure::BeforeRename => {
				assert!(matches!(error, Error::Io(_)), "{what}: {error:?}");
				assert_eq!(
					fs::read(&manifest).unwrap(),
					manifest_before,
					"{what}: the manifest on disk was touched"
				);
				assert_eq!(raw.levels, before, "{what}: the tables the manifest on disk lists");
				// The in-memory change of the compaction is reverted, and nothing is hidden.
				assert_eq!(ids_by_level(&tree), before, "{what}: the tables of the live tree");
				assert!(
					tree.core.inner.level_manifest.read().unwrap().hidden_set.is_empty(),
					"{what}: no table stays hidden from readers"
				);
			}
			ReplaceFailure::AfterRename => {
				assert!(matches!(error, Error::ManifestWriteUncertain(_)), "{what}: {error:?}");
				assert_ne!(
					fs::read(&manifest).unwrap(),
					manifest_before,
					"{what}: the new manifest is in place"
				);
				let plan = Plan::new(before.clone(), &raw.levels, shape.source(), shape.target());
				assert_eq!(raw.levels, plan.after(), "{what}: the tables of the new manifest");
				assert!(
					plan.input.iter().all(|id| sst_path(&db, *id).exists()),
					"{what}: the inputs are still on disk"
				);
			}
		}

		// A crash right after the failure.
		let image =
			take_image(&db, &images.path().join("after-failure"), CompactionStage::OutputsDurable);
		kill(&tree);
		drop(tree);
		assert_eq!(read_manifest(&image.dir).levels, raw.levels, "{what}: the image's manifest");
		recover(&image.dir, &model, true, &what).await;
	}
}

// ---------------------------------------------------------------------------
// 5: commits that return while a compaction runs
// ---------------------------------------------------------------------------

/// The keys a commit was acknowledged for, with their values.
type Acked = BTreeMap<Vec<u8>, Vec<u8>>;

/// The value a writer thread `w` gives its `n`th key.
fn writer_value(w: usize, n: usize) -> Vec<u8> {
	let mut v = format!("writer_{w}_{n:06}_").into_bytes();
	v.resize(60, b'.');
	v
}

/// Whether `key` is one a writer wrote, and which: its thread and number.
fn writer_key(key: &[u8]) -> Option<(usize, usize)> {
	let text = std::str::from_utf8(key).ok()?.strip_prefix("w")?;
	let (w, n) = text.split_once('_')?;
	Some((w.parse().ok()?, n.parse().ok()?))
}

fn writer_key_of(w: usize, n: usize) -> Vec<u8> {
	format!("w{w}_{n:06}").into_bytes()
}

/// The data of an image of the writers' test: the model's keys exactly, every key a writer got an
/// acknowledgement for with its value, and whatever other writer keys the image caught with the
/// value that writer gives them.
fn verify_with_writers(tree: &Tree, model: &Model, acked: &Acked, what: &str) {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	for (i, expected) in &model.keys {
		same(
			&format!("{what}: key {i}"),
			txn.get(key(*i)).unwrap().as_deref(),
			expected.as_deref(),
		);
	}
	for (k, v) in acked {
		same(
			&format!("{what}: acknowledged {}", String::from_utf8_lossy(k)),
			txn.get(k.clone()).unwrap().as_deref(),
			Some(v),
		);
	}
	let scanned = collect_transaction_all(&mut txn.iter().unwrap()).unwrap();
	let base: Vec<Vec<u8>> =
		scanned.iter().filter(|(k, _)| writer_key(k).is_none()).map(|(k, _)| k.clone()).collect();
	let expected: Vec<Vec<u8>> = model.live().into_iter().map(|(k, _)| k).collect();
	assert_eq!(base, expected, "{what}: the keys that are not a writer's");
	for (k, v) in scanned.iter().filter(|(k, _)| writer_key(k).is_some()) {
		let (w, n) = writer_key(k).unwrap();
		same(
			&format!("{what}: writer key {}", String::from_utf8_lossy(k)),
			Some(v),
			Some(&writer_value(w, n)),
		);
	}
}

/// Three tasks commit with `Durability::Immediate` while a compaction goes through its stages.
/// Before each image is copied the keys acknowledged so far are recorded, and the compaction
/// waits after each image for more commits to return. Every acknowledged key is in the image taken
/// after its acknowledgement, with its value, beside the data the compaction merged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commits_acknowledged_before_an_image_are_in_the_image_taken_at_every_stage() {
	let dir = TempDir::new("cc-db").unwrap();
	let images = TempDir::new("cc-images").unwrap();
	let db = dir.path().join("db");
	let Layout {
		tree,
		model,
	} = build(&db).await;
	let tree = Arc::new(tree);
	let before = ids_by_level(&tree);

	let acked: Arc<Mutex<Acked>> = Arc::default();
	let progress = Arc::new(AtomicUsize::new(0));
	let stop = Arc::new(AtomicBool::new(false));
	let writers: Vec<JoinHandle<()>> = (0..3usize)
		.map(|w| {
			let (tree, acked, progress, stop) =
				(Arc::clone(&tree), Arc::clone(&acked), Arc::clone(&progress), Arc::clone(&stop));
			tokio::spawn(async move {
				let mut n = 0usize;
				while !stop.load(Ordering::SeqCst) {
					let mut txn = tree.begin().unwrap();
					txn.set_durability(Durability::Immediate);
					let (k, v) = (writer_key_of(w, n), writer_value(w, n));
					txn.set(k.clone(), v.clone()).unwrap();
					txn.commit().await.unwrap();
					acked.lock().unwrap().insert(k, v);
					progress.fetch_add(1, Ordering::SeqCst);
					n += 1;
				}
			})
		})
		.collect();
	let deadline = Instant::now() + WATCHDOG;
	while progress.load(Ordering::SeqCst) < 5 {
		assert!(Instant::now() < deadline, "the writers never made progress");
		tokio::time::sleep(Duration::from_millis(2)).await;
	}

	let at_stage: Arc<Mutex<Vec<(CompactionStage, Acked)>>> = Arc::default();
	let probe = Probe::new(&db, images.path(), &ALL_STAGES);
	*probe.before.lock().unwrap() = Some({
		let (acked, at_stage) = (Arc::clone(&acked), Arc::clone(&at_stage));
		Arc::new(move |stage| {
			let snapshot = acked.lock().unwrap().clone();
			at_stage.lock().unwrap().push((stage, snapshot));
		})
	});
	*probe.action.lock().unwrap() = Some({
		let progress = Arc::clone(&progress);
		Arc::new(move |stage| {
			// Let the writers get more commits through before the compaction moves on.
			let target = progress.load(Ordering::SeqCst) + 4;
			let deadline = Instant::now() + WATCHDOG;
			while progress.load(Ordering::SeqCst) < target {
				assert!(Instant::now() < deadline, "no commit returned after {stage:?}");
				std::thread::sleep(Duration::from_millis(1));
			}
		})
	});
	probe.install(&tree);
	let compaction = {
		let tree = Arc::clone(&tree);
		tokio::task::spawn_blocking(move || compact(&tree, 0, 1))
	};
	tokio::time::timeout(WATCHDOG, compaction)
		.await
		.expect("the compaction finishes")
		.unwrap()
		.unwrap();
	Probe::uninstall(&tree);
	stop.store(true, Ordering::SeqCst);
	for writer in writers {
		tokio::time::timeout(WATCHDOG, writer).await.expect("a writer stops").unwrap();
	}
	assert_eq!(probe.fired(), ALL_STAGES.to_vec());

	let plan = Plan::new(before, &ids_by_level(&tree), 0, 1);
	let acked_at = at_stage.lock().unwrap().clone();
	let found = probe.images();
	assert_eq!(acked_at.len(), 4);
	for window in acked_at.windows(2) {
		assert!(
			window[1].1.len() > window[0].1.len(),
			"commits returned between {:?} and {:?}",
			window[0].0,
			window[1].0
		);
	}
	for (image, (stage, acknowledged)) in found.iter().zip(&acked_at) {
		assert_eq!(image.stage, *stage);
		assert!(!acknowledged.is_empty());
		assert_raw_image(image, &plan, &[], &[]);
		let work = images.path().join(format!("work-{stage:?}"));
		copy_image(&image.dir, &work);
		let what = format!("{stage:?} with {} acknowledged keys", acknowledged.len());
		let tree = open(&work);
		verify_with_writers(&tree, &model, acknowledged, &what);
		assert_files_match_manifest(&tree, &work, &what);
		let first = snapshot(&tree, &work);
		kill(&tree);
		drop(tree);
		let tree = open(&work);
		verify_with_writers(&tree, &model, acknowledged, &format!("{what}, second open"));
		assert_eq!(snapshot(&tree, &work), first, "{what}: a second open changes nothing");
		kill(&tree);
	}
	// The live tree has every acknowledged key too.
	let all = acked.lock().unwrap().clone();
	verify_with_writers(&tree, &model, &all, "the live tree");
	kill(&tree);
}

// ---------------------------------------------------------------------------
// 6: a second compaction, and a flush during a compaction
// ---------------------------------------------------------------------------

/// A second compaction after the first settled: the first compaction's inputs are gone and its
/// outputs are in the manifest when new tables arrive, and the second one merges them with the
/// outputs of the first. An image at each stage of the second recovers all of it.
#[test(tokio::test)]
async fn a_crash_during_a_second_compaction_after_the_first_one_settled_keeps_every_key() {
	let dir = TempDir::new("cc-db").unwrap();
	let images = TempDir::new("cc-images").unwrap();
	let db = dir.path().join("db");
	let Layout {
		tree,
		mut model,
	} = build(&db).await;
	compact(&tree, 0, 1).unwrap();
	let settled = ids_by_level(&tree);
	assert!(settled[0].is_empty() && settled[1].len() >= 2, "{settled:?}");
	assert_files_match_manifest(&tree, &db, "after the first compaction");

	// New tables on top of the outputs of the first compaction, overwriting and deleting keys that
	// are in them and in the levels below.
	for round in 0..2usize {
		let mut ops = Vec::new();
		for i in (0..KEYS).filter(|i| i % 3 == round) {
			if i % 9 == 0 {
				ops.push(Op::Del(i));
			} else {
				ops.push(Op::Set(i, val(&format!("second{round}"), i, i % 2 == 0)));
			}
		}
		commit(&tree, &mut model, ops, Durability::Eventual).await;
		tree.flush().unwrap();
	}
	let before = ids_by_level(&tree);
	assert_eq!(before[0].len(), 2);
	let before_syncs = directory_syncs::count(&db.join("sstables"));

	let probe = Probe::new(&db, images.path(), &ALL_STAGES);
	probe.install(&tree);
	compact(&tree, 0, 1).unwrap();
	Probe::uninstall(&tree);
	assert_eq!(probe.fired(), ALL_STAGES.to_vec());
	let after = ids_by_level(&tree);
	let plan = Plan::new(before, &after, 0, 1);
	assert!(
		plan.input.is_superset(&settled[1].iter().copied().collect()),
		"the second compaction merged the outputs of the first: {:?}",
		plan.input
	);
	verify_data(&tree, &model, "the live tree");
	for image in probe.images() {
		assert!(image.dir_syncs > before_syncs);
		assert_raw_image(&image, &plan, &[], &[]);
		let work = images.path().join(format!("work-{:?}", image.stage));
		copy_image(&image.dir, &work);
		recover(
			&work,
			&model,
			image.stage == CompactionStage::ManifestWritten,
			&format!("second compaction at {:?}", image.stage),
		)
		.await;
	}
	kill(&tree);
}

/// A flush lands between the pick of a compaction and its manifest write: the compaction parks
/// at `OutputsDurable` while the test commits an overwrite of keys that are in its inputs and
/// flushes it, and then lets it finish. The newer table wins on the live tree and in the image
/// of every stage after the flush, the image before it holds the older state, and a second
/// compaction then merges the table that landed.
#[test(tokio::test)]
async fn a_flush_that_lands_while_a_compaction_is_parked_stays_newer_than_the_outputs() {
	let dir = TempDir::new("cc-db").unwrap();
	let images = TempDir::new("cc-images").unwrap();
	let db = dir.path().join("db");
	let Layout {
		tree,
		model,
	} = build(&db).await;
	let tree = Arc::new(tree);
	let before = ids_by_level(&tree);
	let model_before = model.clone();

	let (parked_tx, parked_rx) = mpsc::channel::<()>();
	let (go_tx, go_rx) = mpsc::channel::<()>();
	let go_rx = Mutex::new(go_rx);
	let parked_tx = Mutex::new(parked_tx);
	let probe = Probe::new(&db, images.path(), &ALL_STAGES);
	*probe.action.lock().unwrap() = Some(Arc::new(move |stage| {
		if stage == CompactionStage::OutputsDurable {
			parked_tx.lock().unwrap().send(()).unwrap();
			go_rx.lock().unwrap().recv_timeout(WATCHDOG).expect("the test lets the compaction go");
		}
	}));
	probe.install(&tree);
	let compaction = {
		let tree = Arc::clone(&tree);
		std::thread::spawn(move || compact(&tree, 0, 1))
	};
	parked_rx.recv_timeout(WATCHDOG).expect("the compaction reaches OutputsDurable");

	// While it is parked: overwrite keys that are in its inputs, delete one, and flush.
	let mut model = model;
	let ops = vec![
		Op::Set(0, val("late", 0, false)),
		Op::Set(2, val("late", 2, true)),
		Op::Set(6, val("late", 6, false)),
		Op::Del(8),
		Op::Del(12),
		Op::Set(KEYS + 5, val("late", KEYS + 5, false)),
	];
	commit(&tree, &mut model, ops, Durability::Eventual).await;
	tree.flush().unwrap();
	let parked = ids_by_level(&tree);
	let landed: Vec<u64> = parked[0].iter().copied().filter(|id| !before[0].contains(id)).collect();
	assert_eq!(landed.len(), 1, "the flush added one table to L0: {parked:?}");
	let parked_image =
		take_image(&db, &images.path().join("parked"), CompactionStage::OutputsDurable);
	go_tx.send(()).unwrap();
	let deadline = Instant::now() + WATCHDOG;
	while !compaction.is_finished() {
		assert!(Instant::now() < deadline, "the compaction did not finish after it was let go");
		tokio::time::sleep(Duration::from_millis(2)).await;
	}
	compaction.join().expect("the compaction thread").expect("the compaction succeeds");
	Probe::uninstall(&tree);
	assert_eq!(probe.fired(), ALL_STAGES.to_vec());

	let after = ids_by_level(&tree);
	let plan = Plan::new(before, &after, 0, 1);
	assert_eq!(after[0], landed, "the table the flush added is the only one in L0 now");
	assert!(!plan.outputs.contains(&landed[0]), "the flush table has an id of its own");
	verify_data(&tree, &model, "the live tree: the late writes are newer than the outputs");

	// The image at OutputsDurable was taken before the flush: it holds the earlier state.
	let first = probe.image(CompactionStage::OutputsDurable);
	assert_raw_image(&first, &plan, &[], &[]);
	let work = images.path().join("work-first");
	copy_image(&first.dir, &work);
	recover(&work, &model_before, false, "before the flush landed").await;
	// The parked image, and the ones after the manifest write, hold the flush table.
	assert_eq!(read_manifest(&parked_image.dir).levels[0], parked[0], "the parked manifest");
	let work = images.path().join("work-parked");
	copy_image(&parked_image.dir, &work);
	recover(&work, &model, true, "parked with the flush landed").await;
	for image in probe.images().iter().filter(|i| i.stage != CompactionStage::OutputsDurable) {
		assert_raw_image(image, &plan, &landed, &[]);
		let work = images.path().join(format!("work-{:?}", image.stage));
		copy_image(&image.dir, &work);
		recover(&work, &model, true, &format!("{:?} with the flush landed", image.stage)).await;
	}

	// And the next compaction merges the table that landed with the outputs.
	let again_before = ids_by_level(&tree);
	let again = Probe::new(&db, &images.path().join("again"), &[CompactionStage::ManifestWritten]);
	again.install(&tree);
	compact(&tree, 0, 1).unwrap();
	Probe::uninstall(&tree);
	let again_plan = Plan::new(again_before, &ids_by_level(&tree), 0, 1);
	assert!(again_plan.input.contains(&landed[0]));
	assert_raw_image(&again.image(CompactionStage::ManifestWritten), &again_plan, &[], &[]);
	let work = images.path().join("work-again");
	copy_image(&again.image(CompactionStage::ManifestWritten).dir, &work);
	recover(&work, &model, false, "the second compaction").await;
	verify_data(&tree, &model, "the live tree after the second compaction");
	kill(&tree);
}

// ---------------------------------------------------------------------------
// 7: the states a crash leaves around the manifest replacement
// ---------------------------------------------------------------------------

/// Takes the images at `OutputsDurable` (the old manifest, with the outputs beside it) and at
/// `ManifestWritten` (the new manifest, with the inputs beside it) of an L0 to L1 compaction.
async fn manifest_images(images: &Path, db: &Path) -> (Model, Plan, Image, Image) {
	let Layout {
		tree,
		model,
	} = build(db).await;
	let before = ids_by_level(&tree);
	let probe = Probe::new(
		db,
		images,
		&[CompactionStage::OutputsDurable, CompactionStage::ManifestWritten],
	);
	probe.install(&tree);
	compact(&tree, 0, 1).unwrap();
	Probe::uninstall(&tree);
	let plan = Plan::new(before, &ids_by_level(&tree), 0, 1);
	let old = probe.image(CompactionStage::OutputsDurable);
	let new = probe.image(CompactionStage::ManifestWritten);
	kill(&tree);
	(model, plan, old, new)
}

/// The name a temporary manifest of `replace_file_content` has.
fn temp_manifest_name(n: u64) -> String {
	format!(".tmp_{}_{}", 1_760_000_000_000_000_000u64 + n, 7_000_000_000_000_000_000u64 + n)
}

/// The replacement of the manifest writes a temporary file beside it, syncs it and renames it
/// over the manifest. A crash before the rename leaves the temporary file, whole or partly
/// written or empty, beside the manifest that is still the old one; and a temporary file of an
/// earlier replacement can lie beside a manifest that has been replaced since. Open does not look
/// at it: the tables, the data and the files of the directory are those of the manifest.
#[test(tokio::test)]
async fn a_temporary_manifest_beside_a_valid_manifest_has_no_effect_on_recovery() {
	let dir = TempDir::new("cc-db").unwrap();
	let images = TempDir::new("cc-images").unwrap();
	let db = dir.path().join("db");
	let (model, plan, old, new) = manifest_images(images.path(), &db).await;
	let old_bytes = fs::read(manifest_path(&old.dir)).unwrap();
	let new_bytes = fs::read(manifest_path(&new.dir)).unwrap();
	assert_ne!(old_bytes, new_bytes, "the compaction changed the manifest");

	let cases: Vec<(&str, &Image, Vec<u8>)> = vec![
		("the new manifest, whole, before the rename", &old, new_bytes.clone()),
		("the new manifest, half written", &old, new_bytes[..new_bytes.len() / 2].to_vec()),
		("an empty temporary file", &old, Vec::new()),
		("the old manifest left by an earlier replacement", &new, old_bytes.clone()),
		("a half written old manifest", &new, old_bytes[..old_bytes.len() / 3].to_vec()),
	];
	for (n, (what, image, bytes)) in cases.into_iter().enumerate() {
		let work = images.path().join(format!("work-{n}"));
		copy_image(&image.dir, &work);
		let temp = work.join("manifest").join(temp_manifest_name(n as u64));
		fs::write(&temp, &bytes).unwrap();
		let names: BTreeSet<String> = fs::read_dir(work.join("manifest"))
			.unwrap()
			.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
			.collect();
		assert_eq!(names.len(), 2, "{what}: the manifest and the temporary file: {names:?}");
		assert_raw_image(image, &plan, &[], &[]);
		recover(&work, &model, true, what).await;
	}
}

/// A crash while the outputs are written leaves files that are partly written, empty or whole,
/// beside the old manifest, and perhaps an output whose directory entry never reached the disk.
/// None of them is opened, and all of them are reclaimed.
#[test(tokio::test)]
async fn outputs_that_a_crash_left_partly_written_beside_the_old_manifest_are_reclaimed() {
	let dir = TempDir::new("cc-db").unwrap();
	let images = TempDir::new("cc-images").unwrap();
	let db = dir.path().join("db");
	let (model, plan, old, _) = manifest_images(images.path(), &db).await;
	let outputs: Vec<u64> = plan.outputs.iter().copied().collect();
	assert!(outputs.len() >= 2, "several outputs: {outputs:?}");
	let truncate = |dir: &Path, id: u64, len: u64| {
		fs::OpenOptions::new().write(true).open(sst_path(dir, id)).unwrap().set_len(len).unwrap();
	};

	// The first output got half of its data and the second none.
	let work = images.path().join("work-partial");
	copy_image(&old.dir, &work);
	let half = fs::metadata(sst_path(&work, outputs[0])).unwrap().len() / 2;
	truncate(&work, outputs[0], half);
	truncate(&work, outputs[1], 0);
	assert_eq!(read_manifest(&work).levels, plan.before, "the manifest is the old one");
	recover(&work, &model, true, "outputs partly written beside the old manifest").await;

	// The first output was never created, and the others are whole.
	let work = images.path().join("work-missing");
	copy_image(&old.dir, &work);
	fs::remove_file(sst_path(&work, outputs[0])).unwrap();
	recover(&work, &model, true, "an output that was never created").await;

	// All of them are partly written.
	let work = images.path().join("work-all-partial");
	copy_image(&old.dir, &work);
	for id in &outputs {
		let len = fs::metadata(sst_path(&work, *id)).unwrap().len();
		truncate(&work, *id, len / 3);
	}
	recover(&work, &model, true, "every output partly written").await;
}

/// A crash after the manifest write and part of the removal of the inputs leaves the new
/// manifest with some of its replaced inputs still on disk. The inputs that remain are reclaimed
/// at open and are never read.
#[test(tokio::test)]
async fn inputs_that_a_crash_left_beside_the_new_manifest_are_reclaimed_at_open() {
	let dir = TempDir::new("cc-db").unwrap();
	let images = TempDir::new("cc-images").unwrap();
	let db = dir.path().join("db");
	let (model, plan, _, new) = manifest_images(images.path(), &db).await;
	let inputs: Vec<u64> = plan.input.iter().copied().collect();

	// All of them, then half of them removed as a crash in the middle of the removal leaves it.
	for (n, removed) in [0, inputs.len() / 2].into_iter().enumerate() {
		let work = images.path().join(format!("work-{n}"));
		copy_image(&new.dir, &work);
		for id in &inputs[..removed] {
			fs::remove_file(sst_path(&work, *id)).unwrap();
		}
		let left: Vec<u64> = inputs[removed..].to_vec();
		assert!(!left.is_empty());
		assert!(left.iter().all(|id| sst_path(&work, *id).exists()));
		assert_eq!(read_manifest(&work).levels, plan.after(), "the manifest is the new one");

		let what = format!("{} of {} inputs removed", removed, inputs.len());
		let (tree, _) = open_and_check(&work, &model, &what);
		for id in &inputs {
			assert!(!sst_path(&work, *id).exists(), "{what}: the input {id} is reclaimed");
		}
		kill(&tree);
		drop(tree);
		recover(&work, &model, true, &what).await;
	}
}

// ---------------------------------------------------------------------------
// range deletions
// ---------------------------------------------------------------------------

/// A range tombstone has to leave a compaction that is not into the bottom level with the outputs,
/// or the older values it hides in the levels below come back. The seeds are in L3, the tombstone
/// and then (when `with_points`) newer values, some of them inside its range, are in L0, and a
/// crash image at each stage of the compaction into L1 is reopened and read. Without points the
/// compaction has nothing to write but the tombstone.
async fn range_scenario(with_points: bool) {
	let dir = TempDir::new("cc-db").unwrap();
	let images = TempDir::new("cc-images").unwrap();
	let db = dir.path().join("db");
	let tree = open(&db);
	let mut model = Model::default();
	let seeds = (0..KEYS).map(|i| Op::Set(i, val("seed", i, i % 9 == 0))).collect();
	commit(&tree, &mut model, seeds, Durability::Eventual).await;
	tree.flush().unwrap();
	compact(&tree, 0, 3).unwrap();

	commit(&tree, &mut model, vec![Op::DelRange(20, 60)], Durability::Eventual).await;
	tree.flush().unwrap();
	if with_points {
		// Newer than the tombstone: inside its range and outside of it.
		let ops = (10..70)
			.filter(|i| i % 2 == 0)
			.map(|i| Op::Set(i, val("newer", i, i % 10 == 0)))
			.chain([Op::Del(70), Op::Del(30)])
			.collect();
		commit(&tree, &mut model, ops, Durability::Eventual).await;
		tree.flush().unwrap();
	}
	// Acknowledged and only in the log.
	commit(&tree, &mut model, vec![Op::Set(41, val("tail", 41, false))], Durability::Immediate)
		.await;
	let before = ids_by_level(&tree);
	assert_eq!(
		before[0].len(),
		if with_points {
			2
		} else {
			1
		},
		"{before:?}"
	);
	verify_data(&tree, &model, "before the compaction");

	let probe = Probe::new(&db, images.path(), &ALL_STAGES);
	probe.install(&tree);
	compact(&tree, 0, 1).unwrap();
	Probe::uninstall(&tree);
	assert_eq!(probe.fired(), ALL_STAGES.to_vec());
	let plan = Plan::with_outputs(
		before,
		&ids_by_level(&tree),
		0,
		1,
		if with_points {
			2
		} else {
			1
		},
	);
	verify_data(&tree, &model, "the live tree: the range stays deleted");
	for image in probe.images() {
		let what = format!("range deletion, points {with_points}, at {:?}", image.stage);
		assert_raw_image(&image, &plan, &[], &[]);
		let work = images.path().join(format!("work-{:?}", image.stage));
		copy_image(&image.dir, &work);
		recover(&work, &model, image.stage == CompactionStage::ManifestWritten, &what).await;
	}
	kill(&tree);
}

#[test(tokio::test)]
async fn a_range_tombstone_survives_a_compaction_crash_beside_newer_values() {
	range_scenario(true).await
}

#[test(tokio::test)]
async fn a_range_tombstone_alone_survives_a_compaction_crash() {
	range_scenario(false).await
}

/// A compaction into the bottom level in which a range tombstone covers every point entry of its
/// inputs. The tombstone entry goes at the bottom level and so do the values it covers, so the
/// merge has no point entry to write: the compaction has either no output, or one table that holds
/// the range deletion alone. Every image reopens to the keys deleted, and the live tree scans.
#[test(tokio::test)]
#[ignore = "product defect: the table of range deletions alone that write_merged_table writes when \
            every point is covered has an empty partitioned index, and a point get that reaches it \
            fails with SSTable(EmptyCorruptPartitionedIndex), on the live tree right after the \
            compaction"]
async fn a_bottom_level_compaction_whose_points_are_all_covered_by_a_range_tombstone_recovers() {
	let dir = TempDir::new("cc-db").unwrap();
	let images = TempDir::new("cc-images").unwrap();
	let db = dir.path().join("db");
	let tree = open(&db);
	let mut model = Model::default();
	let seeds = (20..60).map(|i| Op::Set(i, val("seed", i, i % 9 == 0))).collect();
	commit(&tree, &mut model, seeds, Durability::Eventual).await;
	tree.flush().unwrap();
	compact(&tree, 0, 3).unwrap();
	commit(&tree, &mut model, vec![Op::DelRange(20, 60)], Durability::Eventual).await;
	tree.flush().unwrap();
	// Acknowledged and only in the log, outside of the range.
	commit(&tree, &mut model, vec![Op::Set(5, val("tail", 5, false))], Durability::Immediate).await;
	let before = ids_by_level(&tree);
	assert_eq!((before[0].len(), before[3].is_empty()), (1, false), "{before:?}");

	let probe = Probe::new(&db, images.path(), &ALL_STAGES);
	probe.install(&tree);
	compact(&tree, 0, 3).unwrap();
	Probe::uninstall(&tree);
	assert_eq!(probe.fired(), ALL_STAGES.to_vec());
	verify_data(&tree, &model, "the live tree: every key of the range is deleted");
	let after = ids_by_level(&tree);
	assert!(after[0].is_empty(), "{after:?}");
	assert_files_match_manifest(&tree, &db, "the live tree after the compaction");
	for image in probe.images() {
		let what = format!("covered points at {:?}", image.stage);
		let work = images.path().join(format!("work-{:?}", image.stage));
		copy_image(&image.dir, &work);
		recover(&work, &model, false, &what).await;
	}
	kill(&tree);
}

// ---------------------------------------------------------------------------
// 3 again: a replay flush that takes the id of an orphan
// ---------------------------------------------------------------------------

/// An open that has to flush a memtable of the log takes its table id from the counter of the
/// manifest, and after a crash before the manifest write that id is the one of an output the
/// compaction left on disk. The flush writes its table over that file, and the orphans that are
/// left are reclaimed after it. The log holds two segments because the tree was reopened once
/// after its first tail was written, so that the replay flushes the first segment.
#[test(tokio::test)]
async fn a_table_flushed_by_the_replay_may_take_the_id_of_an_orphan_output_without_harm() {
	let dir = TempDir::new("cc-db").unwrap();
	let images = TempDir::new("cc-images").unwrap();
	let db = dir.path().join("db");
	let Layout {
		tree,
		mut model,
	} = build(&db).await;
	kill(&tree);
	drop(tree);
	// The reopened tree continues in a fresh log segment, and the first tail stays in the old one.
	let tree = open(&db);
	let tail = vec![
		Op::Set(20, val("tail2", 20, true)),
		Op::Set(KEYS + 2, val("tail2", KEYS + 2, false)),
		Op::Del(21),
	];
	commit(&tree, &mut model, tail, Durability::Immediate).await;
	verify_data(&tree, &model, "after the reopen");

	let before = ids_by_level(&tree);
	let probe = Probe::new(&db, images.path(), &[CompactionStage::OutputsDurable]);
	probe.install(&tree);
	compact(&tree, 0, 1).unwrap();
	Probe::uninstall(&tree);
	let plan = Plan::new(before, &ids_by_level(&tree), 0, 1);
	let image = probe.image(CompactionStage::OutputsDurable);
	kill(&tree);
	drop(tree);
	assert!(
		fs::read_dir(image.dir.join("wal")).unwrap().count() >= 2,
		"the log holds more than one segment"
	);

	let raw = read_manifest(&image.dir);
	let (tree, _) = open_and_check(&image.dir, &model, "after the replay flush");
	let live = ids_by_level(&tree);
	let flushed: Vec<u64> =
		live[0].iter().copied().filter(|id| !raw.levels[0].contains(id)).collect();
	assert_eq!(flushed.len(), 1, "the replay flushed the first segment to a table: {live:?}");
	assert!(
		plan.outputs.contains(&flushed[0]),
		"the replay's table {} took the id of an output the compaction left: {:?}",
		flushed[0],
		plan.outputs
	);
	kill(&tree);
	drop(tree);
	recover(&image.dir, &model, true, "after the replay flush, again").await;
}

// ---------------------------------------------------------------------------
// 8: randomised workloads
// ---------------------------------------------------------------------------

/// xorshift64, seeded.
struct Rng(u64);

impl Rng {
	fn new(seed: u64) -> Self {
		Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
	}

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

/// A random workload of several rounds, each flushed and sometimes pushed down a level, then a
/// compaction of a random level with an image at a random stage. The image recovers the model,
/// plainly and after a power loss.
async fn random_scenario(seed: u64) {
	let mut rng = Rng::new(seed);
	let dir = TempDir::new("cc-db").unwrap();
	let images = TempDir::new("cc-images").unwrap();
	let db = dir.path().join("db");
	let tree = open(&db);
	let mut model = Model::default();
	let keys = 50;
	for round in 0..(3 + rng.below(4)) {
		let mut ops = Vec::new();
		for _ in 0..(10 + rng.below(30)) {
			let i = rng.below(keys);
			if rng.below(10) < 3 {
				ops.push(Op::Del(i));
			} else {
				let big = rng.below(4) == 0;
				ops.push(Op::Set(i, val(&format!("s{seed}r{round}"), i, big)));
			}
		}
		commit(&tree, &mut model, ops, Durability::Eventual).await;
		tree.flush().unwrap();
		if rng.below(2) == 0 {
			let levels = ids_by_level(&tree);
			let full: Vec<usize> = (0..LEVELS - 1).filter(|l| !levels[*l].is_empty()).collect();
			if !full.is_empty() {
				let level = full[rng.below(full.len())] as u8;
				compact(&tree, level, level + 1).unwrap();
			}
		}
	}
	let tail = vec![Op::Set(rng.below(keys), val("tail", seed as usize, rng.below(2) == 0))];
	commit(&tree, &mut model, tail, Durability::Immediate).await;
	verify_data(&tree, &model, &format!("seed {seed}: before the compaction"));

	let levels = ids_by_level(&tree);
	let full: Vec<usize> = (0..LEVELS - 1).filter(|l| !levels[*l].is_empty()).collect();
	let source = full[rng.below(full.len())] as u8;
	let stage = ALL_STAGES[rng.below(4)];
	let before_syncs = directory_syncs::count(&db.join("sstables"));
	let files_before = ssts(&db);
	let probe = Probe::new(&db, images.path(), &[stage]);
	probe.install(&tree);
	compact(&tree, source, source + 1).unwrap();
	Probe::uninstall(&tree);
	assert_eq!(probe.fired(), ALL_STAGES.to_vec(), "seed {seed}: the stages");
	let image = probe.image(stage);
	let what = format!("seed {seed}: L{source} at {stage:?}");
	assert!(image.unsynced.is_empty(), "{what}: unsynced tables {:?}", image.unsynced);
	// The files the compaction made, which a power loss may take.
	let outputs: BTreeSet<u64> = ssts(&image.dir).difference(&files_before).copied().collect();
	verify_data(&tree, &model, &format!("{what}: live tree"));

	let work = images.path().join("work");
	copy_image(&image.dir, &work);
	recover(&work, &model, seed % 2 == 0, &what).await;
	let lost = power_loss(&image, &outputs, before_syncs, images.path());
	recover(&lost, &model, false, &format!("{what} after a power loss")).await;
	kill(&tree);
}

/// Eight seeds: different workloads, different levels, different stages. The seed is in every
/// message.
#[test(tokio::test)]
async fn randomised_workloads_recover_from_an_image_at_a_random_stage() {
	for seed in 1..=8u64 {
		random_scenario(seed).await;
	}
}
