//! `Tree::restore_from_checkpoint` under concurrency and under a crash.
//!
//! A restore removes the SSTables, the WAL, the manifest and the value log of a live database,
//! copies the files of a checkpoint back, and then reloads the in-memory state (manifest,
//! memtables, WAL, sequence numbers, oracle). Commits are blocked and the flush lock is held while
//! it runs, and nothing else is excluded. The tests here say what a restore must leave behind
//! however other work interleaves with it, and what a crash in the middle of it leaves on disk.
//!
//! Groups:
//! - exactness: the content after a restore equals the checkpoint, value-log values included,
//!   sequence numbers and the oracle restart from the checkpoint, and the result survives a close
//!   and a crash (`exact_*`, `commits_after_a_restore_*`, `large_values_*`)
//! - concurrent committers: writers that commit throughout a restore (`writers_*`,
//!   `a_commit_issued_during_a_restore_*`), and commits that the flusher holds across one
//!   (`commits_in_the_pipeline_*`)
//! - concurrent readers: transactions and iterators begun before a restore, and one begun after it
//!   (`readers_*`)
//! - a compaction in flight, parked at each of its stages while the restore runs (`a_compaction_*`)
//! - flushes and checkpoints that come in during a restore (`a_flush_*`, `a_checkpoint_*`)
//! - a crash in the middle of a restore, one image per stage (`a_crash_*`, `the_checkpoint_*`,
//!   `acknowledged_commits_*`)
//! - checkpoints that cannot be restored: the restore fails and the database must be untouched
//!   (`a_restore_of_a_checkpoint_*`)
//! - successive restores, another value-log layout, and the value log under restore (`restoring_*`)
//!
//! Every hook-based test checks that its hook fired, every wait has a watchdog, and randomised
//! parts are seeded.

use std::collections::BTreeMap;
use std::ops::Range;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;

use super::wal_group_budget_tests::{copy_dir, mark_closed, release_lock};
use super::{collect_transaction_all, collect_transaction_reverse};
use crate::checkpoint::{CheckpointMetadata, CheckpointStage};
use crate::compaction::compactor::CompactionStage;
use crate::compaction::leveled::Strategy;
use crate::lsm::{CompactionOperations, FlushHook};
use crate::ring::PipelineHook;
use crate::{Durability, Error, LSMIterator, Mode, Options, Transaction, Tree};

/// How long a test waits for something that is expected to happen: a hook to be reached, a call
/// to return.
const WATCHDOG: Duration = Duration::from_secs(30);

/// How long a test gives a call to finish before it concludes that the call waits for something.
const GRACE: Duration = Duration::from_millis(500);

/// Keys `0..UNIVERSE` are the ones the fixture writes.
const UNIVERSE: usize = 130;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

// ---------------------------------------------------------------------------
// data
// ---------------------------------------------------------------------------

fn key_of(i: usize) -> Vec<u8> {
	format!("key_{i:05}").into_bytes()
}

fn show(key: &[u8]) -> String {
	String::from_utf8_lossy(key).into_owned()
}

/// The value of key `i` in generation `gen`. With `vlog`, every third value is large enough to be
/// separated into the value log, and its size varies.
fn val_of(gen: &str, i: usize, vlog: bool) -> Vec<u8> {
	let len = if vlog && i % 3 == 0 {
		400 + (i % 5) * 50
	} else {
		40 + i % 17
	};
	let mut v = format!("{gen}:{i:05}:").into_bytes();
	let fill = b'a' + (i % 26) as u8;
	while v.len() < len {
		v.push(fill);
	}
	v
}

enum Op {
	Set(Vec<u8>, Vec<u8>),
	Del(Vec<u8>),
}

fn sets(range: Range<usize>, gen: &str, vlog: bool) -> Vec<Op> {
	range.map(|i| Op::Set(key_of(i), val_of(gen, i, vlog))).collect()
}

fn dels(range: Range<usize>) -> Vec<Op> {
	range.map(|i| Op::Del(key_of(i))).collect()
}

/// A read-write transaction that is fsynced when it commits.
fn txn(tree: &Tree) -> Transaction {
	tree.begin().unwrap().with_durability(Durability::Immediate)
}

/// Commits `ops` in one transaction and applies them to `model`.
async fn apply(tree: &Tree, model: &mut Model, ops: Vec<Op>) -> crate::Result<()> {
	let mut t = txn(tree);
	for op in &ops {
		match op {
			Op::Set(k, v) => t.set(&k[..], &v[..])?,
			Op::Del(k) => t.delete(&k[..])?,
		}
	}
	t.commit().await?;
	for op in ops {
		match op {
			Op::Set(k, v) => {
				model.insert(k, v);
			}
			Op::Del(k) => {
				model.remove(&k);
			}
		}
	}
	Ok(())
}

/// Everything the transaction sees, in key order.
fn scan(t: &Transaction) -> crate::Result<Model> {
	let mut it = t.iter()?;
	it.seek_first()?;
	let mut all = Model::new();
	while it.valid() {
		all.insert(it.key().user_key().to_vec(), it.value()?);
		it.next()?;
	}
	Ok(all)
}

fn contents(tree: &Tree) -> Model {
	let t = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	scan(&t).unwrap()
}

/// Fails with the first differences between `actual` and `expected`.
fn assert_same(actual: &Model, expected: &Model, what: &str) {
	let mut problems = Vec::new();
	for (k, v) in expected {
		match actual.get(k) {
			None => problems.push(format!("missing {}", show(k))),
			Some(a) if a != v => problems.push(format!(
				"wrong value for {}: {} bytes starting {:?}, expected {} bytes starting {:?}",
				show(k),
				a.len(),
				show(&a[..a.len().min(12)]),
				v.len(),
				show(&v[..v.len().min(12)])
			)),
			_ => {}
		}
	}
	for k in actual.keys() {
		if !expected.contains_key(k) {
			problems.push(format!("unexpected {}", show(k)));
		}
	}
	assert!(
		problems.is_empty(),
		"{what}: {} differences, the first ones: {:?}",
		problems.len(),
		&problems[..problems.len().min(6)]
	);
}

/// The tree holds exactly `expected`, by a scan, a reverse scan and a lookup of every key of the
/// fixture.
fn assert_exact(tree: &Tree, expected: &Model, what: &str) {
	let t = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	assert_same(&scan(&t).unwrap(), expected, &format!("{what}: scan"));
	let mut it = t.iter().unwrap();
	let backwards: Vec<(Vec<u8>, Vec<u8>)> = collect_transaction_reverse(&mut it).unwrap();
	let mut forwards: Vec<(Vec<u8>, Vec<u8>)> =
		expected.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
	forwards.reverse();
	assert_eq!(backwards.len(), forwards.len(), "{what}: reverse scan length");
	assert!(backwards == forwards, "{what}: reverse scan differs");
	for i in 0..UNIVERSE {
		let k = key_of(i);
		assert_eq!(
			t.get(&k[..]).unwrap().as_deref(),
			expected.get(&k).map(Vec::as_slice),
			"{what}: lookup of {}",
			show(&k)
		);
	}
}

/// The tree holds exactly `expected`, by a scan and by a lookup of each of its keys.
fn assert_exact_with_extra(tree: &Tree, expected: &Model, what: &str) {
	let t = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	assert_same(&scan(&t).unwrap(), expected, what);
	for (k, v) in expected {
		assert_eq!(t.get(&k[..]).unwrap().as_ref(), Some(v), "{what}: lookup of {}", show(k));
	}
}

// ---------------------------------------------------------------------------
// the tree and its files
// ---------------------------------------------------------------------------

fn options(path: &Path, vlog: bool) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		// Crash semantics: nothing is flushed on drop or close.
		flush_on_close: false,
		enable_vlog: vlog,
		vlog_value_threshold: 128,
		// Small files, so that the fixture's value log has several of them.
		vlog_max_file_size: 4 * 1024,
		max_memtable_size: 32 * 1024 * 1024,
		memtable_stall_threshold: 1000,
		// The background tasks compact nothing: a test compacts by hand.
		level0_max_files: 1000,
		l0_stall_threshold: 2000,
		..Default::default()
	})
}

/// A tree to share between threads. Not a clone of the `Tree`: dropping a clone closes the store
/// under the other holders.
fn open(opts: Arc<Options>) -> Arc<Tree> {
	Arc::new(Tree::new(opts).unwrap())
}

/// Stops a tree that is abandoned the way a crash abandons it.
fn abandon(tree: &Tree) {
	mark_closed(tree);
	release_lock(tree);
}

/// Opens a copy of a database directory, reads it with `f` and abandons it.
fn with_image<T>(image: &Path, vlog: bool, f: impl FnOnce(&Tree) -> T) -> Result<T, Error> {
	let tree = Tree::new(options(image, vlog))?;
	let out = f(&tree);
	abandon(&tree);
	Ok(out)
}

/// The ids of the tables the manifest lists.
fn table_ids(tree: &Tree) -> Vec<u64> {
	let mut ids: Vec<u64> =
		tree.core.inner.level_manifest.read().unwrap().iter().map(|t| t.id).collect();
	ids.sort_unstable();
	ids
}

/// The ids of the `.sst` files in the SSTable directory.
fn sst_files(opts: &Options) -> Vec<u64> {
	let mut ids = Vec::new();
	if let Ok(entries) = std::fs::read_dir(opts.sstable_dir()) {
		for entry in entries {
			let name = entry.unwrap().file_name().to_string_lossy().into_owned();
			if let Some(id) = name.strip_suffix(".sst") {
				if let Ok(id) = id.parse::<u64>() {
					ids.push(id);
				}
			}
		}
	}
	ids.sort_unstable();
	ids
}

/// Every table the manifest lists is a file in the SSTable directory.
fn assert_tables_on_disk(tree: &Tree, opts: &Options, what: &str) {
	let files = sst_files(opts);
	for id in table_ids(tree) {
		assert!(files.contains(&id), "{what}: the manifest lists table {id}, files: {files:?}");
	}
}

/// A hash of every file under `path` (the LOCK file excepted), by relative name.
fn fingerprint(path: &Path) -> BTreeMap<String, u64> {
	fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<String, u64>) {
		let Ok(entries) = std::fs::read_dir(dir) else {
			return;
		};
		for entry in entries {
			let entry = entry.unwrap();
			if entry.file_name() == "LOCK" {
				continue;
			}
			let rel = entry.path().strip_prefix(root).unwrap().to_string_lossy().into_owned();
			if entry.file_type().unwrap().is_dir() {
				out.insert(format!("{rel}/"), 0);
				walk(root, &entry.path(), out);
			} else {
				let bytes = std::fs::read(entry.path()).unwrap_or_default();
				let mut h = 0xcbf2_9ce4_8422_2325u64;
				for b in &bytes {
					h ^= u64::from(*b);
					h = h.wrapping_mul(0x0100_0000_01b3);
				}
				out.insert(rel, h ^ (bytes.len() as u64).rotate_left(32));
			}
		}
	}
	let mut out = BTreeMap::new();
	walk(path, path, &mut out);
	out
}

/// Seeded xorshift, so that a failing run is reproduced by its printed seed.
struct Rng(u64);

impl Rng {
	fn next(&mut self) -> u64 {
		let mut x = self.0;
		x ^= x << 13;
		x ^= x >> 7;
		x ^= x << 17;
		self.0 = x;
		x
	}
}

// ---------------------------------------------------------------------------
// threads, gates and hooks
// ---------------------------------------------------------------------------

/// A call on a thread of its own, without a runtime. One that never returns fails its test after
/// twice `WATCHDOG`, leaving a parked thread behind, instead of hanging the test binary.
struct Call<T> {
	done: Arc<AtomicBool>,
	result: mpsc::Receiver<std::thread::Result<T>>,
}

fn on_thread<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Call<T> {
	let (tx, result) = mpsc::channel();
	let done = Arc::new(AtomicBool::new(false));
	let finished = Arc::clone(&done);
	std::thread::spawn(move || {
		let result = catch_unwind(AssertUnwindSafe(f));
		finished.store(true, Ordering::SeqCst);
		let _ = tx.send(result);
	});
	Call {
		done,
		result,
	}
}

impl<T> Call<T> {
	fn is_finished(&self) -> bool {
		self.done.load(Ordering::SeqCst)
	}

	/// Waits up to `wait` for the call to finish.
	fn finishes_within(&self, wait: Duration) -> bool {
		let deadline = Instant::now() + wait;
		while !self.is_finished() {
			if Instant::now() >= deadline {
				return false;
			}
			std::thread::sleep(Duration::from_millis(2));
		}
		true
	}

	/// The call's result; a panic of the call is raised again here.
	fn wait(self) -> T {
		match self.result.recv_timeout(2 * WATCHDOG) {
			Ok(Ok(value)) => value,
			Ok(Err(panic)) => resume_unwind(panic),
			Err(_) => panic!("a call did not return within {:?}: a deadlock", 2 * WATCHDOG),
		}
	}
}

fn restore_call(tree: &Arc<Tree>, ckpt: &Path) -> Call<crate::Result<CheckpointMetadata>> {
	let (tree, ckpt) = (Arc::clone(tree), ckpt.to_path_buf());
	on_thread(move || tree.restore_from_checkpoint(&ckpt))
}

fn checkpoint_call(tree: &Arc<Tree>, dir: &Path) -> Call<crate::Result<CheckpointMetadata>> {
	let (tree, dir) = (Arc::clone(tree), dir.to_path_buf());
	on_thread(move || tree.create_checkpoint(&dir))
}

/// Parks the first caller of the hook that `gate` returns until `release`, and counts every
/// arrival.
struct Gate {
	reached: mpsc::Receiver<()>,
	release: mpsc::Sender<()>,
	hits: Arc<AtomicUsize>,
}

fn gate() -> (Gate, Arc<dyn Fn() + Send + Sync>) {
	let (reached_tx, reached) = mpsc::channel();
	let (release, release_rx) = mpsc::channel();
	let release_rx = Mutex::new(release_rx);
	let hits = Arc::new(AtomicUsize::new(0));
	let counted = Arc::clone(&hits);
	let parked = AtomicBool::new(false);
	let pass: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
		counted.fetch_add(1, Ordering::SeqCst);
		if !parked.swap(true, Ordering::SeqCst) {
			let _ = reached_tx.send(());
			// Bounded, so that a broken test fails instead of hanging.
			let _ = release_rx.lock().unwrap().recv_timeout(WATCHDOG);
		}
	});
	(
		Gate {
			reached,
			release,
			hits,
		},
		pass,
	)
}

impl Gate {
	/// Waits for the hook to be reached.
	fn arrived(&self, what: &str) {
		self.reached
			.recv_timeout(WATCHDOG)
			.unwrap_or_else(|_| panic!("{what}: the hook was never reached"));
	}

	fn release(&self) {
		let _ = self.release.send(());
	}

	fn hits(&self) -> usize {
		self.hits.load(Ordering::SeqCst)
	}
}

/// Parks a restore or a checkpoint when it reaches `stage`.
fn park_checkpoint_at(tree: &Tree, stage: CheckpointStage) -> Gate {
	let (gate, pass) = gate();
	*tree.core.inner.checkpoint_hook.lock() = Some(Arc::new(move |at| {
		if at == stage {
			pass();
		}
	}));
	gate
}

/// Parks a compaction when it reaches `stage`.
fn park_compaction_at(tree: &Tree, stage: CompactionStage) -> Gate {
	let (gate, pass) = gate();
	*tree.core.inner.compaction_stage_hook.lock() = Some(Arc::new(move |at| {
		if at == stage {
			pass();
		}
		Ok(())
	}));
	gate
}

fn clear_hooks(tree: &Tree) {
	*tree.core.inner.flush_hook.lock() = None;
	*tree.core.inner.checkpoint_hook.lock() = None;
	*tree.core.inner.compaction_stage_hook.lock() = None;
}

/// A strategy that compacts as soon as two level-0 tables exist.
fn eager(opts: &Arc<Options>) -> Arc<Strategy> {
	Arc::new(Strategy::from_options(Arc::new(Options {
		level0_max_files: 2,
		..(**opts).clone()
	})))
}

/// Compacts the level-0 tables into level 1 on a thread of its own.
fn compact_call(tree: &Arc<Tree>, opts: &Arc<Options>) -> Call<crate::Result<()>> {
	let (tree, strategy) = (Arc::clone(tree), eager(opts));
	on_thread(move || tree.compact(strategy))
}

const RESTORE_STAGES: [CheckpointStage; 4] = [
	CheckpointStage::RestoreCleared,
	CheckpointStage::RestoreSstablesCopied,
	CheckpointStage::RestoreFilesCopied,
	CheckpointStage::RestoreManifestSwapped,
];

fn is_restore_stage(stage: CheckpointStage) -> bool {
	RESTORE_STAGES.contains(&stage)
}

// ---------------------------------------------------------------------------
// the fixture
// ---------------------------------------------------------------------------

/// A tree that was checkpointed and changed afterwards.
///
/// Keys `0..60` are written in generation "a", of which `0..10` are overwritten in "b" and
/// `55..60` deleted before the checkpoint: `at_ckpt`. Afterwards `60..100` are added, `10..30`
/// overwritten and `30..40` deleted (and flushed to a table), and then `100..120` are added,
/// `0..5` overwritten, `5..8` deleted and `55..58` written again, left in the memtable and the
/// WAL: `before`.
struct World {
	_dir: TempDir,
	_ckpt_root: TempDir,
	path: PathBuf,
	opts: Arc<Options>,
	tree: Arc<Tree>,
	ckpt: PathBuf,
	vlog: bool,
	at_ckpt: Model,
	before: Model,
	ckpt_seq: u64,
	tables_at_ckpt: Vec<u64>,
}

async fn build(vlog: bool) -> World {
	let dir = TempDir::new("restore_conc").unwrap();
	let ckpt_root = TempDir::new("restore_conc_ckpt").unwrap();
	let opts = options(dir.path(), vlog);
	let tree = open(Arc::clone(&opts));
	let mut m = Model::new();
	apply(&tree, &mut m, sets(0..30, "a", vlog)).await.unwrap();
	tree.flush().unwrap();
	apply(&tree, &mut m, sets(30..60, "a", vlog)).await.unwrap();
	apply(&tree, &mut m, sets(0..10, "b", vlog)).await.unwrap();
	apply(&tree, &mut m, dels(55..60)).await.unwrap();
	let at_ckpt = m.clone();
	let ckpt = ckpt_root.path().join("ckpt");
	let meta = tree.create_checkpoint(&ckpt).unwrap();
	let tables_at_ckpt = table_ids(&tree);
	assert!(tables_at_ckpt.len() >= 2, "setup: the checkpoint holds the flushed tables");

	apply(&tree, &mut m, sets(60..100, "c", vlog)).await.unwrap();
	apply(&tree, &mut m, sets(10..30, "c", vlog)).await.unwrap();
	apply(&tree, &mut m, dels(30..40)).await.unwrap();
	tree.flush().unwrap();
	apply(&tree, &mut m, sets(100..120, "d", vlog)).await.unwrap();
	apply(&tree, &mut m, sets(0..5, "d", vlog)).await.unwrap();
	apply(&tree, &mut m, dels(5..8)).await.unwrap();
	apply(&tree, &mut m, sets(55..58, "e", vlog)).await.unwrap();
	let before = m;
	assert_ne!(before, at_ckpt, "setup: the tree changed after the checkpoint");
	assert_same(&contents(&tree), &before, "setup");
	World {
		path: dir.path().to_path_buf(),
		_dir: dir,
		_ckpt_root: ckpt_root,
		opts,
		tree,
		ckpt,
		vlog,
		at_ckpt,
		before,
		ckpt_seq: meta.sequence_number,
		tables_at_ckpt,
	}
}

impl World {
	/// Closes the tree cleanly and opens the directory again.
	async fn reopen(&mut self) -> Arc<Tree> {
		self.tree.close().await.unwrap();
		open(options(&self.path, self.vlog))
	}
}

// ---------------------------------------------------------------------------
// exactness
// ---------------------------------------------------------------------------

/// After a restore the tree holds the checkpoint: what was written after it is gone, what was
/// deleted after it is back, what was overwritten after it has the checkpoint's value.
async fn exact(vlog: bool) {
	let mut w = build(vlog).await;
	let meta = w.tree.restore_from_checkpoint(&w.ckpt).unwrap();
	assert_eq!(meta.sequence_number, w.ckpt_seq, "the metadata is the checkpoint's");
	assert_exact(&w.tree, &w.at_ckpt, "after the restore");
	let t = w.tree.begin_with_mode(Mode::ReadOnly).unwrap();
	for i in 60..UNIVERSE {
		assert_eq!(t.get(&key_of(i)[..]).unwrap(), None, "key {i} written after the checkpoint");
	}
	for i in 30..40 {
		assert_eq!(
			t.get(&key_of(i)[..]).unwrap(),
			Some(val_of("a", i, vlog)),
			"key {i} deleted after the checkpoint"
		);
	}
	for i in 10..30 {
		assert_eq!(
			t.get(&key_of(i)[..]).unwrap(),
			Some(val_of("a", i, vlog)),
			"key {i} overwritten after the checkpoint"
		);
	}
	for i in 0..10 {
		assert_eq!(t.get(&key_of(i)[..]).unwrap(), Some(val_of("b", i, vlog)), "key {i}");
	}
	// A range scan sees the same.
	let mut it = t.range(key_of(20), key_of(70)).unwrap();
	let ranged: Vec<(Vec<u8>, Vec<u8>)> = collect_transaction_all(&mut it).unwrap();
	let expected: Vec<(Vec<u8>, Vec<u8>)> =
		w.at_ckpt.range(key_of(20)..key_of(70)).map(|(k, v)| (k.clone(), v.clone())).collect();
	assert!(ranged == expected, "the range scan after the restore");
	drop(it);
	drop(t);

	// A close and a reopen keep it.
	let tree = w.reopen().await;
	assert_exact(&tree, &w.at_ckpt, "after the close and the reopen");
	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn exact_a_restore_returns_the_checkpoint_content() {
	exact(false).await;
}

#[test(tokio::test)]
async fn exact_a_restore_returns_the_checkpoint_content_with_a_value_log() {
	exact(true).await;
}

/// New commits after a restore continue from the restored sequence number, are readable, and a
/// transaction that conflicts only with a commit from before the restore is not refused, while one
/// that conflicts with a commit after it still is.
async fn after_the_restore(vlog: bool) {
	let mut w = build(vlog).await;
	let pre_restore_seq = w.tree.core.seq_num();
	assert!(pre_restore_seq > w.ckpt_seq + 20, "setup: the tree is well ahead of the checkpoint");
	w.tree.restore_from_checkpoint(&w.ckpt).unwrap();
	assert_eq!(
		w.tree.core.seq_num(),
		w.ckpt_seq,
		"the visible sequence number is the restored one"
	);

	// Three entries: three sequence numbers above the restored ones.
	let mut m = w.at_ckpt.clone();
	apply(&w.tree, &mut m, sets(200..203, "p", vlog)).await.unwrap();
	assert_eq!(w.tree.core.seq_num(), w.ckpt_seq + 3, "the next commit continues from the restore");
	assert_exact_with_extra(&w.tree, &m, "after one commit");

	// Key 15 was overwritten in generation "c" by a commit from before the restore, a ghost of a
	// future that no longer exists.
	let mut t = txn(&w.tree);
	assert_eq!(t.get_for_update(&key_of(15)[..]).unwrap(), Some(val_of("a", 15, vlog)));
	t.set(&key_of(15)[..], &b"after the restore"[..]).unwrap();
	t.commit().await.expect("a ghost commit from before the restore must not conflict");
	m.insert(key_of(15), b"after the restore".to_vec());

	// A blind write of a key that a pre-restore commit wrote, too.
	let mut t = txn(&w.tree);
	t.set(&key_of(105)[..], &b"blind"[..]).unwrap();
	t.commit().await.expect("a blind write over a ghost must not conflict");
	m.insert(key_of(105), b"blind".to_vec());

	// A real conflict is still refused.
	let mut first = txn(&w.tree);
	let mut second = txn(&w.tree);
	first.set(&key_of(16)[..], &b"first"[..]).unwrap();
	second.set(&key_of(16)[..], &b"second"[..]).unwrap();
	first.commit().await.unwrap();
	m.insert(key_of(16), b"first".to_vec());
	assert!(
		matches!(second.commit().await, Err(Error::TransactionWriteConflict)),
		"the oracle still catches a conflict between two commits after the restore"
	);
	assert_exact_with_extra(&w.tree, &m, "after the conflicts");

	let tree = w.reopen().await;
	assert_exact_with_extra(&tree, &m, "after the close and the reopen");
	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn commits_after_a_restore_continue_from_the_restored_sequence_and_the_oracle_is_reset() {
	after_the_restore(false).await;
}

#[test(tokio::test)]
#[ignore = "a restore leaves the value log writing to its deleted files: large values committed afterwards are unreadable"]
async fn commits_after_a_restore_continue_from_the_restored_sequence_with_a_value_log() {
	after_the_restore(true).await;
}

/// Table ids restart from the restored manifest, so a table written after the restore can have the
/// id of a table from before it. The blocks that were cached for the old table must not be served
/// for the new one.
#[test(tokio::test)]
#[ignore = "a restore does not clear the block cache: a table written afterwards under a reused id is read through the blocks cached for the old table"]
async fn a_table_id_reused_after_a_restore_does_not_serve_the_blocks_cached_for_the_old_table() {
	let mut w = build(false).await;
	// Reads that fill the block cache with the blocks of the tables written after the checkpoint,
	// whose ids the restore hands out again.
	let before = w.before.clone();
	assert_exact(&w.tree, &before, "before the restore");
	assert_exact(&w.tree, &before, "before the restore, again");
	let old_ids = table_ids(&w.tree);

	w.tree.restore_from_checkpoint(&w.ckpt).unwrap();
	assert_eq!(table_ids(&w.tree), w.tables_at_ckpt);
	let mut m = w.at_ckpt.clone();
	// New data for the key range of the old tables, flushed to tables with reused ids.
	apply(&w.tree, &mut m, sets(60..100, "n", false)).await.unwrap();
	apply(&w.tree, &mut m, sets(10..30, "n", false)).await.unwrap();
	apply(&w.tree, &mut m, dels(30..40)).await.unwrap();
	w.tree.flush().unwrap();
	let reused: Vec<u64> =
		table_ids(&w.tree).into_iter().filter(|id| old_ids.contains(id)).collect();
	assert!(
		reused.iter().any(|id| !w.tables_at_ckpt.contains(id)),
		"setup: a table id from before the restore is used again ({old_ids:?} -> {:?})",
		table_ids(&w.tree)
	);
	assert_exact_with_extra(&w.tree, &m, "reads after the flush of reused ids");
	let tree = w.reopen().await;
	assert_exact_with_extra(&tree, &m, "after the reopen");
	tree.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// the value log
// ---------------------------------------------------------------------------

/// Large values that are committed after a restore are readable at once, and after a close and a
/// reopen (or a crash). The restore replaces the files of the tree's value log by the checkpoint's.
async fn large_values_after_a_restore(crash: bool) {
	let mut w = build(true).await;
	w.tree.restore_from_checkpoint(&w.ckpt).unwrap();
	let mut m = w.at_ckpt.clone();
	// More than one value-log file's worth, so that the log rolls over.
	apply(&w.tree, &mut m, sets(300..330, "v", true)).await.unwrap();
	apply(&w.tree, &mut m, sets(60..90, "v", true)).await.unwrap();
	assert_exact_with_extra(&w.tree, &m, "right after the commits");
	if crash {
		let image = TempDir::new("restore_conc_image").unwrap();
		copy_dir(&w.path, image.path());
		abandon(&w.tree);
		let recovered = with_image(image.path(), true, contents).unwrap();
		assert_same(&recovered, &m, "an image taken after the acknowledged commits");
		return;
	}
	let tree = w.reopen().await;
	assert_exact_with_extra(&tree, &m, "after the close and the reopen");
	tree.close().await.unwrap();
}

#[test(tokio::test)]
#[ignore = "a restore leaves the value log writing to its deleted files: large values committed afterwards are unreadable"]
async fn large_values_committed_after_a_restore_are_readable_and_survive_a_reopen() {
	large_values_after_a_restore(false).await;
}

#[test(tokio::test)]
#[ignore = "a restore leaves the value log writing to its deleted files: large values committed afterwards are lost in a crash"]
async fn large_values_committed_after_a_restore_survive_a_crash() {
	large_values_after_a_restore(true).await;
}

// ---------------------------------------------------------------------------
// concurrent committers
// ---------------------------------------------------------------------------

/// One commit of a writer task.
struct Rec {
	key: Vec<u8>,
	value: Vec<u8>,
	begin: u64,
	end: u64,
	outcome: Result<(), String>,
}

const WRITERS: usize = 4;
/// Far more commits than a restore and the wait after it need: the cap only bounds a runaway.
const WRITER_CAP: usize = 20_000;
const WANTED_PER_SIDE: usize = 40;

/// Writers that commit single keys with an fsync each throughout a restore. The tree ends up as the
/// checkpoint plus the commits that were acknowledged after the restore returned (and those that
/// were in flight across it, which may or may not have landed). A commit that returned an error
/// must not be in the tree; with `failed_may_land` that one rule is not checked, and the keys of
/// the failed commits are allowed in the tree.
async fn writers_during_a_restore(vlog: bool, failed_may_land: bool) {
	let mut w = build(vlog).await;
	let tick = Arc::new(AtomicU64::new(1));
	let stop = Arc::new(AtomicBool::new(false));
	let acked_before = Arc::new(AtomicUsize::new(0));
	let acked_after = Arc::new(AtomicUsize::new(0));
	let restore_done = Arc::new(AtomicU64::new(u64::MAX));
	let mut writers = Vec::new();
	for t in 0..WRITERS {
		let tree = Arc::clone(&w.tree);
		let (tick, stop) = (Arc::clone(&tick), Arc::clone(&stop));
		let (before, after) = (Arc::clone(&acked_before), Arc::clone(&acked_after));
		let restore_done = Arc::clone(&restore_done);
		writers.push(tokio::spawn(async move {
			let mut recs = Vec::new();
			for i in 0..WRITER_CAP {
				if stop.load(Ordering::SeqCst) {
					break;
				}
				let key = format!("w{t}_{i:05}").into_bytes();
				let value = val_of(&format!("w{t}"), i, vlog);
				let begin = tick.fetch_add(1, Ordering::SeqCst);
				let outcome = match tree.begin() {
					Ok(tx) => {
						let mut tx = tx.with_durability(Durability::Immediate);
						match tx.set(&key[..], &value[..]) {
							Ok(()) => tx.commit().await.map_err(|e| format!("{e:?}")),
							Err(e) => Err(format!("{e:?}")),
						}
					}
					Err(e) => Err(format!("{e:?}")),
				};
				let end = tick.fetch_add(1, Ordering::SeqCst);
				if outcome.is_ok() {
					if begin > restore_done.load(Ordering::SeqCst) {
						after.fetch_add(1, Ordering::SeqCst);
					} else {
						before.fetch_add(1, Ordering::SeqCst);
					}
				}
				let failed = outcome.is_err();
				recs.push(Rec {
					key,
					value,
					begin,
					end,
					outcome,
				});
				if failed {
					// A commit refused during the restore: back off instead of spinning.
					tokio::time::sleep(Duration::from_millis(1)).await;
				} else {
					tokio::task::yield_now().await;
				}
			}
			recs
		}));
	}

	let deadline = Instant::now() + WATCHDOG;
	while acked_before.load(Ordering::SeqCst) < WANTED_PER_SIDE {
		assert!(Instant::now() < deadline, "the writers never acknowledged commits");
		tokio::time::sleep(Duration::from_millis(2)).await;
	}
	let restore_start = tick.fetch_add(1, Ordering::SeqCst);
	let (tree, ckpt) = (Arc::clone(&w.tree), w.ckpt.clone());
	let restoring = tokio::task::spawn_blocking(move || tree.restore_from_checkpoint(&ckpt));
	let restored = tokio::time::timeout(WATCHDOG, restoring)
		.await
		.expect("the restore did not finish while writers committed: a deadlock")
		.unwrap();
	restored.expect("the restore failed");
	let done = tick.fetch_add(1, Ordering::SeqCst);
	restore_done.store(done, Ordering::SeqCst);

	let deadline = Instant::now() + WATCHDOG;
	while acked_after.load(Ordering::SeqCst) < WANTED_PER_SIDE {
		assert!(
			Instant::now() < deadline,
			"the writers acknowledged only {} commits after the restore",
			acked_after.load(Ordering::SeqCst)
		);
		tokio::time::sleep(Duration::from_millis(2)).await;
	}
	stop.store(true, Ordering::SeqCst);
	let mut recs = Vec::new();
	for writer in writers {
		recs.extend(
			tokio::time::timeout(WATCHDOG, writer)
				.await
				.expect("a writer never finished")
				.expect("a writer panicked"),
		);
	}

	// Every failure is a clean one, and the commits that failed are not in the tree.
	let mut allowed = w.at_ckpt.clone();
	let (mut must, mut must_not, mut either, mut failed) = (0, 0, 0, 0);
	for r in &recs {
		match &r.outcome {
			Ok(()) if r.begin > done => {
				must += 1;
				allowed.insert(r.key.clone(), r.value.clone());
			}
			Ok(()) if r.end < restore_start => must_not += 1,
			Ok(()) => {
				either += 1;
				allowed.insert(r.key.clone(), r.value.clone());
			}
			Err(e) => {
				failed += 1;
				assert!(
					e.contains("PipelineStall") || e.contains("Pipeline stall"),
					"a commit failed with something else: {e}"
				);
				if failed_may_land {
					allowed.insert(r.key.clone(), r.value.clone());
				}
			}
		}
	}
	assert!(must >= WANTED_PER_SIDE, "commits acknowledged after the restore: {must}");
	assert!(must_not >= 1, "commits acknowledged before the restore: {must_not}");
	println!("writers: {must} after, {must_not} before, {either} across, {failed} failed");

	let live = contents(&w.tree);
	for r in &recs {
		let present = live.get(&r.key);
		match &r.outcome {
			Ok(()) if r.begin > done => {
				assert_eq!(
					present,
					Some(&r.value),
					"{} acknowledged after the restore",
					show(&r.key)
				)
			}
			Ok(()) if r.end < restore_start => {
				assert_eq!(present, None, "{} was acknowledged before the restore", show(&r.key))
			}
			Ok(()) => {
				if let Some(v) = present {
					assert_eq!(v, &r.value, "{} committed across the restore", show(&r.key));
				}
			}
			Err(e) if !failed_may_land => {
				assert_eq!(present, None, "{} failed with {e} and is in the tree", show(&r.key))
			}
			Err(_) => {
				if let Some(v) = present {
					assert_eq!(
						v,
						&r.value,
						"{} failed and is in the tree with another value",
						show(&r.key)
					);
				}
			}
		}
	}
	for k in live.keys() {
		assert!(
			allowed.contains_key(k),
			"{} is in the tree and nobody may have written it",
			show(k)
		);
	}
	for (k, v) in &w.at_ckpt {
		assert_eq!(live.get(k), Some(v), "checkpoint key {} after the restore", show(k));
	}

	// More commits, then a close and a reopen keep exactly what is there.
	let mut m = live.clone();
	apply(&w.tree, &mut m, sets(400..410, "z", vlog)).await.unwrap();
	assert_same(&contents(&w.tree), &m, "after more commits");
	let tree = w.reopen().await;
	assert_same(&contents(&tree), &m, "after the close and the reopen");
	tree.close().await.unwrap();
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn writers_racing_a_restore_leave_the_checkpoint_plus_the_commits_after_it() {
	writers_during_a_restore(false, true).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
#[ignore = "a commit that fails during a restore, with an error, can still land in the restored database"]
async fn writers_racing_a_restore_never_land_a_commit_that_returned_an_error() {
	writers_during_a_restore(false, false).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
#[ignore = "a restore leaves the value log writing to its deleted files: large values acknowledged after the restore are unreadable"]
async fn writers_racing_a_restore_leave_the_checkpoint_plus_the_commits_after_it_with_a_value_log()
{
	writers_during_a_restore(true, true).await;
}

/// A commit that is issued while a restore holds the files fails cleanly and at once, and leaves no
/// trace after the restore, at every stage of the restore.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_commit_issued_during_a_restore_fails_cleanly_and_leaves_no_trace() {
	for stage in RESTORE_STAGES {
		let mut w = build(false).await;
		let gate = park_checkpoint_at(&w.tree, stage);
		let restoring = restore_call(&w.tree, &w.ckpt);
		gate.arrived(&format!("{stage:?}"));
		let (tree, key) = (Arc::clone(&w.tree), format!("during_{stage:?}"));
		let commit = tokio::spawn(async move {
			let mut t = tree.begin().unwrap().with_durability(Durability::Immediate);
			t.set(key.as_bytes(), &b"in the middle of a restore"[..]).unwrap();
			t.commit().await
		});
		let outcome = tokio::time::timeout(WATCHDOG, commit)
			.await
			.unwrap_or_else(|_| panic!("{stage:?}: a commit hung while a restore ran"))
			.unwrap();
		assert!(
			matches!(outcome, Err(Error::PipelineStall)),
			"{stage:?}: a commit during a restore must fail cleanly, got {outcome:?}"
		);
		gate.release();
		restoring.wait().unwrap();
		assert_eq!(gate.hits(), 1, "{stage:?}: the stage was reached once");
		clear_hooks(&w.tree);
		assert_exact(&w.tree, &w.at_ckpt, &format!("{stage:?}: after the restore"));
		let t = w.tree.begin_with_mode(Mode::ReadOnly).unwrap();
		assert_eq!(
			t.get(format!("during_{stage:?}").as_bytes()).unwrap(),
			None,
			"{stage:?}: the failed commit left a trace"
		);
		drop(t);
		// The pipeline works again.
		let mut m = w.at_ckpt.clone();
		apply(&w.tree, &mut m, sets(300..305, "after", false)).await.unwrap();
		assert_exact_with_extra(&w.tree, &m, &format!("{stage:?}: a commit after the restore"));
		let tree = w.reopen().await;
		tree.close().await.unwrap();
	}
}

// ---------------------------------------------------------------------------
// commits held in the pipeline across a restore
// ---------------------------------------------------------------------------

/// Where the flusher is held while a restore runs.
#[derive(Clone, Copy, Debug)]
enum Park {
	/// A group is logged and synced; its first apply round, and the check of the restore that it
	/// makes, are ahead.
	AfterWalSync,
	/// The first apply round passed that check and is about to apply the group.
	BeforeApplyRound,
}

/// Holds the flusher at `park`, the first time it gets there.
fn park_flusher_at(tree: &Tree, park: Park) -> Gate {
	let (gate, pass) = gate();
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		let here = matches!(
			(park, point),
			(Park::AfterWalSync, PipelineHook::AfterWalSync { .. })
				| (
					Park::BeforeApplyRound,
					PipelineHook::BeforeApplyRound {
						round: 0
					}
				)
		);
		if here {
			pass();
		}
	})));
	gate
}

/// One durable single-key commit, on a task of its own.
fn commit_task(tree: &Arc<Tree>, key: &str) -> tokio::task::JoinHandle<crate::Result<()>> {
	let (tree, key) = (Arc::clone(tree), key.to_string());
	tokio::spawn(async move {
		let mut t = tree.begin().unwrap().with_durability(Durability::Immediate);
		t.set(key.as_bytes(), &b"held in the pipeline"[..]).unwrap();
		t.commit().await
	})
}

/// Whether a commit failed with the clean error of a pipeline that a restore interrupted.
fn is_stall(outcome: &crate::Result<()>) -> bool {
	match outcome {
		Err(Error::PipelineStall) => true,
		Err(e) => format!("{e:?}").contains("Pipeline stall"),
		Ok(()) => false,
	}
}

/// The flusher holds a commit at `park`, a second commit is accepted behind it, and a restore runs
/// to the end. Neither commit may be acknowledged: they were accepted for the database that the
/// restore replaced. With `landing`, neither may be in the tree afterwards either, even once new
/// commits have passed the sequence numbers they were given before the restore.
async fn commits_in_the_pipeline_across_a_restore(park: Park, landing: bool) {
	let mut w = build(false).await;
	let gate = park_flusher_at(&w.tree, park);
	let held = commit_task(&w.tree, "held_in_the_pipeline");
	gate.arrived(&format!("{park:?}"));
	let behind = commit_task(&w.tree, "queued_behind_it");
	let deadline = Instant::now() + WATCHDOG;
	while w.tree.core.commit_pipeline.accepted_waiting() < 2 {
		assert!(Instant::now() < deadline, "{park:?}: the second commit was never accepted");
		tokio::time::sleep(Duration::from_millis(2)).await;
	}

	let restoring = restore_call(&w.tree, &w.ckpt);
	assert!(
		restoring.finishes_within(WATCHDOG),
		"{park:?}: the restore waited for the commit the flusher holds"
	);
	restoring.wait().unwrap();
	gate.release();
	let held = tokio::time::timeout(WATCHDOG, held)
		.await
		.unwrap_or_else(|_| panic!("{park:?}: the held commit never returned"))
		.unwrap();
	let behind = tokio::time::timeout(WATCHDOG, behind)
		.await
		.unwrap_or_else(|_| panic!("{park:?}: the queued commit never returned"))
		.unwrap();
	w.tree.core.commit_pipeline.set_hook(None);
	assert!(is_stall(&held), "{park:?}: the commit held across the restore returned {held:?}");
	assert!(
		is_stall(&behind),
		"{park:?}: the commit queued across the restore returned {behind:?}"
	);

	if landing {
		// Both were given sequence numbers from before the restore, above the checkpoint's. Commits
		// that pass those numbers would make them readable, if they were in the tree.
		let mut m = w.at_ckpt.clone();
		apply(&w.tree, &mut m, sets(300..500, "after", false)).await.unwrap();
		assert_exact_with_extra(&w.tree, &m, &format!("{park:?}: after the restore"));
		let tree = w.reopen().await;
		assert_exact_with_extra(&tree, &m, &format!("{park:?}: after the reopen"));
		tree.close().await.unwrap();
	} else {
		w.tree.close().await.unwrap();
	}
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn commits_in_the_pipeline_when_a_restore_runs_fail_and_leave_nothing() {
	commits_in_the_pipeline_across_a_restore(Park::AfterWalSync, true).await;
}

/// A group that passed the restore check of its apply round when the restore ran is not
/// acknowledged: the restore replaced the database it was logged for.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_commit_applied_across_a_restore_is_not_acknowledged() {
	commits_in_the_pipeline_across_a_restore(Park::BeforeApplyRound, false).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
#[ignore = "a group that is applied after a restore replaced the memtable lands in the restored database, though its commits fail"]
async fn a_commit_applied_across_a_restore_leaves_nothing_in_the_restored_database() {
	commits_in_the_pipeline_across_a_restore(Park::BeforeApplyRound, true).await;
}

/// Nothing of the memtable that a restore replaced comes back, whatever happens afterwards: rows of
/// it carry sequence numbers above the checkpoint's, so they are hidden until new commits pass
/// those numbers, and then they would reappear next to the checkpoint's rows.
#[test(tokio::test)]
async fn a_restore_leaves_nothing_of_the_old_memtable_behind_once_commits_pass_its_sequence_numbers(
) {
	let mut w = build(false).await;
	let ahead = w.tree.core.seq_num() - w.ckpt_seq;
	assert!(ahead > 20, "setup: the tree is {ahead} sequence numbers ahead of the checkpoint");
	w.tree.restore_from_checkpoint(&w.ckpt).unwrap();
	assert_eq!(w.tree.core.seq_num(), w.ckpt_seq);
	let mut m = w.at_ckpt.clone();
	// One commit of more entries than the tree was ahead, then single commits.
	let count = ahead as usize + 40;
	apply(&w.tree, &mut m, sets(300..300 + count, "after", false)).await.unwrap();
	for i in 0..5 {
		apply(&w.tree, &mut m, sets(900 + i..901 + i, "single", false)).await.unwrap();
	}
	assert!(w.tree.core.seq_num() > w.ckpt_seq + ahead, "the sequence numbers passed the old ones");
	assert_exact_with_extra(&w.tree, &m, "after the commits");
	let tree = w.reopen().await;
	assert_exact_with_extra(&tree, &m, "after the close and the reopen");
	tree.close().await.unwrap();
}

/// The commit history that a transaction begun before a restore keeps pinned is dropped by the
/// restore: it describes commits of a database that is gone, and nothing else would free it while
/// that transaction is open.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_restore_drops_the_commit_history_that_an_old_transaction_pinned() {
	let w = build(false).await;
	let pipeline = Arc::clone(&w.tree.core.commit_pipeline);
	let held = w.tree.begin().unwrap();
	// Far more commits than the ring holds, so that the ring laps and the pinned ones are kept in
	// the overflow map.
	for i in 0..4 * crate::ring::COMMIT_RING_CAPACITY {
		let mut t = w.tree.begin().unwrap();
		t.set(format!("history_{i:06}").as_bytes(), &b"v"[..]).unwrap();
		t.commit().await.unwrap();
	}
	let pinned = pipeline.watermarks().2;
	assert!(
		pinned > 1000,
		"setup: the open transaction keeps {pinned} commits in the overflow map"
	);
	w.tree.restore_from_checkpoint(&w.ckpt).unwrap();
	assert_eq!(pipeline.watermarks().2, 0, "the overflow map after the restore");
	drop(held);
	assert_exact(&w.tree, &w.at_ckpt, "after the restore");
	w.tree.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// concurrent readers
// ---------------------------------------------------------------------------

/// What a reader that began before the restore may see for `key`: its value before the restore, or
/// the checkpoint's, but nothing else.
fn allowed_value(w: &World, key: &[u8], got: Option<&Vec<u8>>) -> bool {
	got == w.before.get(key) || got == w.at_ckpt.get(key)
}

/// Reads with `t`, which began before the restore, and records what is wrong with it in `bad`.
/// An error is clean and allowed.
fn read_with_old_transaction(w: &World, t: &Transaction, at: &str, bad: &mut Vec<String>) {
	for i in 0..UNIVERSE {
		let k = key_of(i);
		if let Ok(got) = t.get(&k[..]) {
			if !allowed_value(w, &k, got.as_ref()) {
				bad.push(format!("{at}: lookup of {} returned neither state", show(&k)));
			}
		}
	}
	if let Ok(all) = scan(t) {
		for (k, v) in &all {
			if !allowed_value(w, k, Some(v)) {
				bad.push(format!("{at}: the scan returned a row of neither state: {}", show(k)));
			}
		}
	}
}

/// A read transaction begun before a restore reads at every stage of the restore, and after it,
/// without a panic and without a value that is in neither state.
async fn readers_old_transaction(vlog: bool) {
	let w = build(vlog).await;
	let reader = Arc::new(Mutex::new(w.tree.begin_with_mode(Mode::ReadOnly).unwrap()));
	let world = Arc::new(w);
	let bad = Arc::new(Mutex::new(Vec::new()));
	let stages = Arc::new(Mutex::new(Vec::new()));
	{
		let (hooked, reader) = (Arc::clone(&world), Arc::clone(&reader));
		let (bad, stages) = (Arc::clone(&bad), Arc::clone(&stages));
		let world = Arc::clone(&world);
		*hooked.tree.core.inner.checkpoint_hook.lock() = Some(Arc::new(move |stage| {
			if is_restore_stage(stage) {
				stages.lock().unwrap().push(stage);
				let mut problems = Vec::new();
				let t = reader.lock().unwrap();
				read_with_old_transaction(&world, &t, &format!("{stage:?}"), &mut problems);
				bad.lock().unwrap().extend(problems);
			}
		}));
	}
	world.tree.restore_from_checkpoint(&world.ckpt).unwrap();
	assert_eq!(*stages.lock().unwrap(), RESTORE_STAGES.to_vec(), "every restore stage was reached");
	clear_hooks(&world.tree);
	let mut problems = Vec::new();
	{
		let t = reader.lock().unwrap();
		read_with_old_transaction(&world, &t, "after the restore", &mut problems);
	}
	bad.lock().unwrap().extend(problems);
	{
		let bad = bad.lock().unwrap();
		assert!(bad.is_empty(), "values in neither state: {:?}", &bad[..bad.len().min(6)]);
	}

	// A transaction begun after the restore sees the checkpoint and nothing else.
	assert_exact(&world.tree, &world.at_ckpt, "a transaction begun after the restore");
	drop(reader);
	world.tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn readers_begun_before_a_restore_never_see_a_value_of_neither_state() {
	readers_old_transaction(false).await;
}

#[test(tokio::test)]
async fn readers_begun_before_a_restore_never_see_a_value_of_neither_state_with_a_value_log() {
	readers_old_transaction(true).await;
}

/// A transaction begun while a restore is in progress, at the stage where the new manifest is in
/// place and the memtables and the WAL are still the old ones, sees one state and not a mix of
/// both: old memtable rows over restored tables would show keys that the checkpoint does not hold
/// next to values it does.
#[test(tokio::test)]
#[ignore = "a transaction begun after a restore swapped the manifest and before it replaced the memtables reads the old memtable over the restored tables"]
async fn readers_begun_between_the_manifest_swap_and_the_memtable_swap_see_one_state() {
	let w = build(false).await;
	let seen: Arc<Mutex<Option<Result<Model, String>>>> = Arc::new(Mutex::new(None));
	let tree = Arc::clone(&w.tree);
	let slot = Arc::clone(&seen);
	*w.tree.core.inner.checkpoint_hook.lock() = Some(Arc::new(move |stage| {
		if stage == CheckpointStage::RestoreManifestSwapped {
			let t = tree.begin_with_mode(Mode::ReadOnly).unwrap();
			*slot.lock().unwrap() = Some(scan(&t).map_err(|e| format!("{e:?}")));
		}
	}));
	w.tree.restore_from_checkpoint(&w.ckpt).unwrap();
	clear_hooks(&w.tree);
	let seen = seen.lock().unwrap().take().expect("the manifest-swapped stage was reached");
	if let Ok(all) = seen {
		assert!(
			all == w.before || all == w.at_ckpt,
			"a reader between the two swaps saw {} keys: neither the {} keys from before the \
			 restore nor the {} of the checkpoint",
			all.len(),
			w.before.len(),
			w.at_ckpt.len()
		);
	}
	assert_exact(&w.tree, &w.at_ckpt, "after the restore");
	w.tree.close().await.unwrap();
}

/// An iterator that is half way through a scan when a restore happens ends cleanly or continues:
/// never a panic, keys stay strictly increasing, every row is in one of the two states.
#[test(tokio::test)]
async fn readers_an_iterator_open_across_a_restore_keeps_ordered_rows_of_the_two_states() {
	let w = build(false).await;
	let t = w.tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let mut it = t.iter().unwrap();
	it.seek_first().unwrap();
	let mut rows: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
	for _ in 0..40 {
		assert!(it.valid());
		rows.push((it.key().user_key().to_vec(), it.value().unwrap()));
		it.next().unwrap();
	}
	assert!(
		rows.iter().all(|(k, v)| w.before.get(k) == Some(v)),
		"the rows before the restore are the pre-restore ones"
	);

	w.tree.restore_from_checkpoint(&w.ckpt).unwrap();

	let mut failed = false;
	while it.valid() {
		let key = it.key().user_key().to_vec();
		match it.value() {
			Ok(v) => {
				assert!(
					allowed_value(&w, &key, Some(&v)),
					"row {} is in neither state",
					show(&key)
				);
				rows.push((key, v));
			}
			Err(_) => {
				failed = true;
				break;
			}
		}
		if it.next().is_err() {
			failed = true;
			break;
		}
	}
	for pair in rows.windows(2) {
		assert!(
			pair[0].0 < pair[1].0,
			"keys {} and {} are out of order",
			show(&pair[0].0),
			show(&pair[1].0)
		);
	}
	println!("iterator across a restore: {} rows, failed cleanly: {failed}", rows.len());
	drop(it);
	drop(t);
	assert_exact(&w.tree, &w.at_ckpt, "a transaction begun after the restore");
	w.tree.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// a compaction in flight
// ---------------------------------------------------------------------------

/// A restore comes in while a compaction is parked at `stage`. The restore either finishes while
/// the compaction is parked or waits for it; either way, once both are done the tree is the
/// checkpoint (plus what is committed afterwards), every table of the manifest is a file, and the
/// state survives a close and a reopen. With `flushes`, that many rounds of commits and flushes
/// follow the restore before the compaction is released, which hand out table ids again.
async fn restore_during_a_compaction(stage: CompactionStage, flushes: usize) {
	let mut w = build(false).await;
	let gate = park_compaction_at(&w.tree, stage);
	let compaction = compact_call(&w.tree, &w.opts);
	gate.arrived(&format!("{stage:?}"));
	let tables_before = table_ids(&w.tree);
	let restoring = restore_call(&w.tree, &w.ckpt);
	let restore_first = restoring.finishes_within(6 * GRACE);
	let mut m = w.at_ckpt.clone();
	if restore_first {
		for round in 0..flushes {
			let gen = format!("r{round}");
			let range = 100 + round * 5..105 + round * 5;
			apply(&w.tree, &mut m, sets(range, &gen, false)).await.unwrap();
			w.tree.flush().unwrap();
		}
	}
	gate.release();
	let compacted = compaction.wait();
	restoring.wait().unwrap();
	assert_eq!(gate.hits(), 1, "{stage:?}: the compaction reached the stage once");
	clear_hooks(&w.tree);
	println!(
		"{stage:?}: restore first: {restore_first}, compaction: {compacted:?}, tables {tables_before:?} -> {:?}",
		table_ids(&w.tree)
	);

	assert_tables_on_disk(&w.tree, &w.opts, &format!("{stage:?}: after both"));
	assert_exact_with_extra(&w.tree, &m, &format!("{stage:?}: after both"));
	let tree = w.reopen().await;
	assert_tables_on_disk(&tree, &w.opts, &format!("{stage:?}: after the reopen"));
	assert_exact_with_extra(&tree, &m, &format!("{stage:?}: after the reopen"));
	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn a_compaction_that_resumes_after_a_restore_from_its_outputs_stage_leaves_the_checkpoint() {
	restore_during_a_compaction(CompactionStage::OutputsDurable, 0).await;
}

#[test(tokio::test)]
#[ignore = "a compaction running during a restore applies its changeset to the restored manifest: a table id reused by a flush is listed twice and the checkpoint's tables are dropped"]
async fn a_compaction_whose_output_id_a_flush_reuses_after_a_restore_leaves_the_checkpoint() {
	restore_during_a_compaction(CompactionStage::OutputsDurable, 2).await;
}

#[test(tokio::test)]
#[ignore = "a compaction running during a restore deletes the restored tables that have the ids of its inputs, so the manifest lists missing files"]
async fn a_compaction_that_resumes_after_a_restore_from_its_manifest_stage_leaves_the_checkpoint() {
	restore_during_a_compaction(CompactionStage::ManifestWritten, 0).await;
}

#[test(tokio::test)]
async fn a_compaction_that_resumes_after_a_restore_from_its_inputs_stage_leaves_the_checkpoint() {
	restore_during_a_compaction(CompactionStage::InputsRemoved, 0).await;
}

#[test(tokio::test)]
async fn a_compaction_that_resumes_after_a_restore_from_its_last_stage_leaves_the_checkpoint() {
	restore_during_a_compaction(CompactionStage::VlogCleaned, 0).await;
}

/// The same with a value log: the compaction's cleanup of the value log, which runs after a
/// restore replaced the files, must not remove a file the restored tables point into.
#[test(tokio::test)]
async fn a_compaction_that_resumes_after_a_restore_with_a_value_log_keeps_the_values() {
	let mut w = build(true).await;
	let gate = park_compaction_at(&w.tree, CompactionStage::InputsRemoved);
	let compaction = compact_call(&w.tree, &w.opts);
	gate.arrived("InputsRemoved");
	let restoring = restore_call(&w.tree, &w.ckpt);
	let restore_first = restoring.finishes_within(6 * GRACE);
	gate.release();
	compaction.wait().unwrap();
	restoring.wait().unwrap();
	assert_eq!(gate.hits(), 1, "the compaction reached the stage once");
	clear_hooks(&w.tree);
	println!("value log compaction: restore first: {restore_first}");
	assert_exact(&w.tree, &w.at_ckpt, "after both");
	let tree = w.reopen().await;
	assert_exact(&tree, &w.at_ckpt, "after the reopen");
	tree.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// flushes and checkpoints during a restore
// ---------------------------------------------------------------------------

/// A flush that starts while a restore runs waits for it, and finds a queue that the restore
/// emptied: no table of the pre-restore memtable is written or listed. Once with the files
/// removed, and once with the manifest already replaced and the memtables not: the flush lock is
/// held through the whole restore, not only through its first step.
#[test(tokio::test)]
async fn a_flush_that_starts_during_a_restore_waits_for_it_and_adds_no_table() {
	for stage in [CheckpointStage::RestoreCleared, CheckpointStage::RestoreManifestSwapped] {
		let mut w = build(false).await;
		w.tree.core.inner.rotate_memtable().unwrap();
		assert_eq!(
			w.tree.core.inner.immutable_count(),
			1,
			"{stage:?}: setup: a pre-restore memtable is queued"
		);
		let gate = park_checkpoint_at(&w.tree, stage);
		let restoring = restore_call(&w.tree, &w.ckpt);
		gate.arrived(&format!("{stage:?}"));
		let inner = Arc::clone(&w.tree.core.inner);
		let flushing = on_thread(move || inner.flush_all_immutables_sync());
		assert!(
			!flushing.finishes_within(GRACE),
			"{stage:?}: a flush ran while the restore held the flush lock"
		);
		let inner = Arc::clone(&w.tree.core.inner);
		let background = on_thread(move || inner.compact_memtable());
		assert!(
			!background.finishes_within(GRACE),
			"{stage:?}: a background flush ran during the restore"
		);
		gate.release();
		restoring.wait().unwrap();
		flushing.wait().unwrap();
		background.wait().unwrap();
		assert_eq!(gate.hits(), 1, "{stage:?}: the stage was reached once");
		clear_hooks(&w.tree);
		assert_eq!(
			table_ids(&w.tree),
			w.tables_at_ckpt,
			"{stage:?}: only the checkpoint's tables are listed"
		);
		assert_eq!(
			sst_files(&w.opts),
			w.tables_at_ckpt,
			"{stage:?}: only the checkpoint's tables are files"
		);
		assert_exact(&w.tree, &w.at_ckpt, &format!("{stage:?}: after the restore and the flushes"));
		let tree = w.reopen().await;
		assert_exact(&tree, &w.at_ckpt, &format!("{stage:?}: after the reopen"));
		tree.close().await.unwrap();
	}
}

/// A flush parked with its table written and not listed, and a restore that waits for it: after
/// both, the flushed table is neither listed nor a file, the tree is the checkpoint and new
/// commits work.
#[test(tokio::test)]
async fn a_flush_parked_before_it_lists_its_table_does_not_leak_it_into_a_restore() {
	let mut w = build(false).await;
	let (gate, pass) = gate();
	let hook: FlushHook = Arc::new(move |_table_id| {
		pass();
		Ok(())
	});
	*w.tree.core.inner.flush_hook.lock() = Some(hook);
	w.tree.core.inner.rotate_memtable().unwrap();
	let inner = Arc::clone(&w.tree.core.inner);
	let flushing = on_thread(move || inner.flush_all_immutables_sync());
	gate.arrived("the flush hook");
	let restoring = restore_call(&w.tree, &w.ckpt);
	assert!(!restoring.finishes_within(GRACE), "the restore went ahead of a flush in flight");
	gate.release();
	flushing.wait().unwrap();
	restoring.wait().unwrap();
	assert_eq!(gate.hits(), 1, "the flush hook fired once");
	clear_hooks(&w.tree);
	assert_eq!(table_ids(&w.tree), w.tables_at_ckpt);
	assert_eq!(sst_files(&w.opts), w.tables_at_ckpt, "the flushed table's file is gone");
	let mut m = w.at_ckpt.clone();
	apply(&w.tree, &mut m, sets(300..310, "n", false)).await.unwrap();
	assert_exact_with_extra(&w.tree, &m, "after a commit");
	let tree = w.reopen().await;
	assert_exact_with_extra(&tree, &m, "after the reopen");
	tree.close().await.unwrap();
}

/// Opens a copy of a checkpoint directory as a database and returns what it holds.
fn open_checkpoint_copy(ckpt: &Path, vlog: bool) -> Result<Model, Error> {
	let copy = TempDir::new("restore_conc_ckpt_copy").unwrap();
	copy_dir(ckpt, copy.path());
	with_image(copy.path(), vlog, contents)
}

/// A checkpoint that began first and is parked with the manifest held, and a restore that begins
/// while it is parked. The checkpoint is the state from before the restore or the one after it:
/// never a mix, and it opens.
#[test(tokio::test)]
async fn a_checkpoint_parked_before_a_restore_is_the_state_before_it_or_after_it() {
	let mut w = build(false).await;
	let second = TempDir::new("restore_conc_second").unwrap();
	let second_dir = second.path().join("ckpt");
	let gate = park_checkpoint_at(&w.tree, CheckpointStage::SstablesCopied);
	let checkpointing = checkpoint_call(&w.tree, &second_dir);
	gate.arrived("SstablesCopied");
	let restoring = restore_call(&w.tree, &w.ckpt);
	let ran_ahead = restoring.finishes_within(GRACE);
	gate.release();
	let meta = checkpointing.wait();
	restoring.wait().unwrap();
	assert_eq!(gate.hits(), 1, "the checkpoint reached its stage once");
	clear_hooks(&w.tree);
	println!("restore ran ahead of the checkpoint: {ran_ahead}, checkpoint: {meta:?}");
	let meta = meta.expect("the checkpoint failed");
	let opened = open_checkpoint_copy(&second_dir, false)
		.unwrap_or_else(|e| panic!("the second checkpoint does not open: {e:?}"));
	assert!(
		opened == w.before || opened == w.at_ckpt,
		"the second checkpoint holds {} keys, neither the {} of the state before the restore nor \
		 the {} of the checkpoint (restore ran ahead: {ran_ahead}, its sequence number {})",
		opened.len(),
		w.before.len(),
		w.at_ckpt.len(),
		meta.sequence_number
	);
	assert_exact(&w.tree, &w.at_ckpt, "the live tree after both");
	let tree = w.reopen().await;
	assert_exact(&tree, &w.at_ckpt, "the live tree after the reopen");
	tree.close().await.unwrap();
}

/// A checkpoint that begins while a restore is parked in the middle. It fails or waits; when it
/// succeeds it is the state before the restore or the one after it, and when it fails it leaves
/// nothing that looks like a checkpoint. The live tree is the checkpoint afterwards.
#[test(tokio::test)]
async fn a_checkpoint_begun_during_a_restore_is_never_a_mix_of_the_two_states() {
	for stage in [CheckpointStage::RestoreCleared, CheckpointStage::RestoreFilesCopied] {
		let mut w = build(false).await;
		let second = TempDir::new("restore_conc_second").unwrap();
		let second_dir = second.path().join("ckpt");
		let gate = park_checkpoint_at(&w.tree, stage);
		let restoring = restore_call(&w.tree, &w.ckpt);
		gate.arrived(&format!("{stage:?}"));
		let checkpointing = checkpoint_call(&w.tree, &second_dir);
		let early = checkpointing.finishes_within(GRACE);
		gate.release();
		let outcome = checkpointing.wait();
		restoring.wait().unwrap();
		clear_hooks(&w.tree);
		println!(
			"{stage:?}: the checkpoint finished while the restore was parked: {early}: {outcome:?}"
		);
		match outcome {
			Ok(_) => {
				let opened = open_checkpoint_copy(&second_dir, false)
					.unwrap_or_else(|e| panic!("{stage:?}: the checkpoint does not open: {e:?}"));
				assert!(
					opened == w.before || opened == w.at_ckpt,
					"{stage:?}: the checkpoint holds {} keys: neither the state before the restore ({}) \
					 nor the checkpoint's ({})",
					opened.len(),
					w.before.len(),
					w.at_ckpt.len()
				);
			}
			Err(_) => assert!(
				!second_dir.join("CHECKPOINT_METADATA").exists(),
				"{stage:?}: a failed checkpoint left a metadata file"
			),
		}
		assert_exact(&w.tree, &w.at_ckpt, &format!("{stage:?}: the live tree after both"));
		let mut m = w.at_ckpt.clone();
		apply(&w.tree, &mut m, sets(300..305, "n", false)).await.unwrap();
		let tree = w.reopen().await;
		assert_exact_with_extra(&tree, &m, &format!("{stage:?}: after the reopen"));
		tree.close().await.unwrap();
	}
}

// ---------------------------------------------------------------------------
// a crash in the middle of a restore
// ---------------------------------------------------------------------------

/// Runs the restore with a hook that copies the database directory at every stage, and hashes the
/// checkpoint at every stage. Returns the images by stage and the checkpoint's hashes.
fn restore_taking_images(
	w: &World,
	images: &Path,
) -> (BTreeMap<String, PathBuf>, Vec<BTreeMap<String, u64>>) {
	let taken = Arc::new(Mutex::new(BTreeMap::new()));
	let hashes = Arc::new(Mutex::new(Vec::new()));
	let (db, ckpt, images) = (w.path.clone(), w.ckpt.clone(), images.to_path_buf());
	let (t, h) = (Arc::clone(&taken), Arc::clone(&hashes));
	*w.tree.core.inner.checkpoint_hook.lock() = Some(Arc::new(move |stage| {
		if is_restore_stage(stage) {
			let dst = images.join(format!("{stage:?}"));
			copy_dir(&db, &dst);
			t.lock().unwrap().insert(format!("{stage:?}"), dst);
			h.lock().unwrap().push(fingerprint(&ckpt));
		}
	}));
	w.tree.restore_from_checkpoint(&w.ckpt).unwrap();
	clear_hooks(&w.tree);
	let taken = taken.lock().unwrap().clone();
	assert_eq!(taken.len(), RESTORE_STAGES.len(), "an image at every stage: {taken:?}");
	let hashes = hashes.lock().unwrap().clone();
	(taken, hashes)
}

/// Restore never changes the checkpoint it reads: its files are the same before the restore, at
/// every stage, and after it.
#[test(tokio::test)]
async fn the_checkpoint_directory_is_never_modified_by_a_restore() {
	for vlog in [false, true] {
		let w = build(vlog).await;
		let before = fingerprint(&w.ckpt);
		assert!(before.keys().any(|k| k.ends_with(".sst")), "setup: the checkpoint holds tables");
		let images = TempDir::new("restore_conc_images").unwrap();
		let (_, at_stages) = restore_taking_images(&w, images.path());
		assert_eq!(at_stages.len(), RESTORE_STAGES.len());
		for (i, seen) in at_stages.iter().enumerate() {
			assert_eq!(*seen, before, "vlog {vlog}: the checkpoint changed by stage {i}");
		}
		assert_eq!(fingerprint(&w.ckpt), before, "vlog {vlog}: the checkpoint after the restore");
		abandon(&w.tree);
	}
}

/// The database that was restored from a checkpoint writes, flushes, compacts and closes without
/// touching the checkpoint, and the checkpoint restores again afterwards.
#[test(tokio::test)]
async fn the_checkpoint_is_untouched_by_the_database_that_was_restored_from_it() {
	for vlog in [false, true] {
		let mut w = build(vlog).await;
		let before = fingerprint(&w.ckpt);
		w.tree.restore_from_checkpoint(&w.ckpt).unwrap();
		let mut m = w.at_ckpt.clone();
		for round in 0..3 {
			let gen = format!("g{round}");
			apply(&w.tree, &mut m, sets(0..40, &gen, vlog)).await.unwrap();
			w.tree.flush().unwrap();
		}
		w.tree.compact(eager(&w.opts)).unwrap();
		apply(&w.tree, &mut m, dels(0..10)).await.unwrap();
		assert_exact_with_extra(&w.tree, &m, &format!("vlog {vlog}: after the activity"));
		assert_eq!(fingerprint(&w.ckpt), before, "vlog {vlog}: the checkpoint after the activity");
		let tree = w.reopen().await;
		assert_eq!(fingerprint(&w.ckpt), before, "vlog {vlog}: the checkpoint after the close");
		tree.restore_from_checkpoint(&w.ckpt).unwrap();
		assert_exact(&tree, &w.at_ckpt, &format!("vlog {vlog}: the second restore"));
		tree.close().await.unwrap();
	}
}

/// Takes the images of a restore for a fixture, and returns them with the fixture.
async fn images_of_a_restore(vlog: bool) -> (World, TempDir, BTreeMap<String, PathBuf>) {
	let w = build(vlog).await;
	let images = TempDir::new("restore_conc_images").unwrap();
	let (taken, _) = restore_taking_images(&w, images.path());
	(w, images, taken)
}

/// An image whose files are all in place opens as the checkpoint, and restoring on that fresh
/// open gives the checkpoint again.
async fn image_with_every_file_in_place(vlog: bool, stage: CheckpointStage) {
	let (w, _images, taken) = images_of_a_restore(vlog).await;
	let image = &taken[&format!("{stage:?}")];
	let held = with_image(image, vlog, contents)
		.unwrap_or_else(|e| panic!("{stage:?}: the image does not open: {e:?}"));
	assert_same(&held, &w.at_ckpt, &format!("{stage:?}: the image"));
	// The restore is repeatable on a fresh open of the image.
	let tree = open(options(image, vlog));
	let m = tree.restore_from_checkpoint(&w.ckpt).unwrap();
	assert_eq!(m.sequence_number, w.ckpt_seq);
	assert_exact(&tree, &w.at_ckpt, &format!("{stage:?}: restoring again on the image"));
	let mut model = w.at_ckpt.clone();
	apply(&tree, &mut model, sets(300..305, "n", vlog)).await.unwrap();
	assert_exact_with_extra(
		&tree,
		&model,
		&format!("{stage:?}: a commit after restoring the image"),
	);
	abandon(&tree);
	abandon(&w.tree);
}

#[test(tokio::test)]
async fn a_crash_when_the_files_are_back_but_the_memory_is_not_reloaded_reopens_as_the_checkpoint()
{
	image_with_every_file_in_place(false, CheckpointStage::RestoreFilesCopied).await;
}

#[test(tokio::test)]
async fn a_crash_after_the_manifest_swap_reopens_as_the_checkpoint() {
	image_with_every_file_in_place(false, CheckpointStage::RestoreManifestSwapped).await;
}

#[test(tokio::test)]
async fn a_crash_when_the_files_are_back_reopens_as_the_checkpoint_with_a_value_log() {
	image_with_every_file_in_place(true, CheckpointStage::RestoreFilesCopied).await;
}

/// A crash that leaves the tables and no manifest is refused instead of opened as an empty
/// database, and the tables are still there afterwards.
#[test(tokio::test)]
async fn a_crash_with_the_tables_copied_and_no_manifest_is_refused() {
	let (w, _images, taken) = images_of_a_restore(false).await;
	let image = &taken["RestoreSstablesCopied"];
	let tables = |files: BTreeMap<String, u64>| -> BTreeMap<String, u64> {
		files.into_iter().filter(|(k, _)| k.starts_with("sstables/")).collect()
	};
	let tables_before = tables(fingerprint(image));
	assert!(tables_before.keys().any(|k| k.ends_with(".sst")), "setup: the image holds tables");
	assert!(
		!fingerprint(image).keys().any(|k| k.starts_with("manifest/") && !k.ends_with('/')),
		"setup: the image has no manifest"
	);
	let opened = Tree::new(options(image, false));
	assert!(opened.is_err(), "an image with tables and no manifest opened");
	assert_eq!(
		tables(fingerprint(image)),
		tables_before,
		"the refused open must not delete tables"
	);
	abandon(&w.tree);
}

/// An image from a crash in the middle of a restore never opens as a plausible but wrong database:
/// it is refused, or it holds the checkpoint.
async fn image_never_a_plausible_wrong_database(stage: CheckpointStage) {
	let (w, _images, taken) = images_of_a_restore(false).await;
	let image = &taken[&format!("{stage:?}")];
	match with_image(image, false, contents) {
		Err(_) => {}
		Ok(held) => assert!(
			held == w.at_ckpt,
			"{stage:?}: the image opened as a database of {} keys, which is not the checkpoint's {} \
			 and was not refused",
			held.len(),
			w.at_ckpt.len()
		),
	}
	abandon(&w.tree);
}

#[test(tokio::test)]
#[ignore = "an image of a crash right after a restore removed the database state opens as an empty database instead of being refused"]
async fn a_crash_after_the_state_is_cleared_must_not_open_as_an_empty_database() {
	image_never_a_plausible_wrong_database(CheckpointStage::RestoreCleared).await;
}

#[test(tokio::test)]
async fn a_crash_with_the_tables_copied_never_opens_as_a_wrong_database() {
	image_never_a_plausible_wrong_database(CheckpointStage::RestoreSstablesCopied).await;
}

/// The commits that are acknowledged after a restore are in the WAL that the restore reopened:
/// a crash image taken right after them recovers the checkpoint and exactly those commits.
async fn acknowledged_commits_survive_a_crash(vlog: bool) {
	let w = build(vlog).await;
	w.tree.restore_from_checkpoint(&w.ckpt).unwrap();
	let mut m = w.at_ckpt.clone();
	apply(&w.tree, &mut m, sets(300..310, "p", vlog)).await.unwrap();
	apply(&w.tree, &mut m, dels(20..25)).await.unwrap();
	apply(&w.tree, &mut m, sets(15..20, "q", vlog)).await.unwrap();
	let image = TempDir::new("restore_conc_image").unwrap();
	copy_dir(&w.path, image.path());
	abandon(&w.tree);
	let recovered = with_image(image.path(), vlog, contents).unwrap();
	assert_same(&recovered, &m, "an image taken after the restore and the acknowledged commits");
}

#[test(tokio::test)]
async fn acknowledged_commits_after_a_restore_survive_a_crash() {
	acknowledged_commits_survive_a_crash(false).await;
}

#[test(tokio::test)]
#[ignore = "a restore leaves the value log writing to its deleted files: large values committed afterwards are lost in a crash"]
async fn acknowledged_commits_after_a_restore_survive_a_crash_with_a_value_log() {
	acknowledged_commits_survive_a_crash(true).await;
}

/// A crash after the restore returned, with nothing committed since: the image is the checkpoint.
#[test(tokio::test)]
async fn a_crash_right_after_a_restore_recovers_the_checkpoint() {
	for vlog in [false, true] {
		let w = build(vlog).await;
		w.tree.restore_from_checkpoint(&w.ckpt).unwrap();
		let image = TempDir::new("restore_conc_image").unwrap();
		copy_dir(&w.path, image.path());
		abandon(&w.tree);
		let recovered = with_image(image.path(), vlog, contents).unwrap();
		assert_same(
			&recovered,
			&w.at_ckpt,
			&format!("vlog {vlog}: an image right after the restore"),
		);
	}
}

// ---------------------------------------------------------------------------
// checkpoints that cannot be restored
// ---------------------------------------------------------------------------

/// Spoils the checkpoint with `spoil`, restores it, and checks that the restore failed and that the
/// database is untouched: it still holds what it held, its files are the same, and it holds it
/// after a close and a reopen. Every problem found is reported together.
async fn bad_checkpoint(what: &str, spoil: impl FnOnce(&Path)) {
	let w = build(false).await;
	spoil(&w.ckpt);
	let files = fingerprint(&w.path);
	let outcome = w.tree.restore_from_checkpoint(&w.ckpt);
	assert!(outcome.is_err(), "{what}: restoring a bad checkpoint succeeded: {outcome:?}");
	let mut problems = Vec::new();
	let live = contents(&w.tree);
	if live != w.before {
		problems.push(format!(
			"the live tree holds {} keys, not the {} it held",
			live.len(),
			w.before.len()
		));
	}
	let after = fingerprint(&w.path);
	let changed: Vec<&String> = files
		.keys()
		.chain(after.keys())
		.filter(|k| files.get(*k) != after.get(*k))
		.collect::<std::collections::BTreeSet<_>>()
		.into_iter()
		.collect();
	if !changed.is_empty() {
		problems.push(format!(
			"the failed restore changed {} files of the database: {:?}",
			changed.len(),
			&changed[..changed.len().min(8)]
		));
	}
	// What the failed restore left must still open, with everything in it.
	if let Err(e) = w.tree.close().await {
		problems.push(format!("the database does not close after the failed restore: {e:?}"));
	}
	match Tree::new(options(&w.path, false)) {
		Err(e) => {
			problems.push(format!("the database does not open after the failed restore: {e:?}"))
		}
		Ok(tree) => {
			let held = contents(&tree);
			if held != w.before {
				problems.push(format!(
					"after a reopen the database holds {} keys, not the {} it held",
					held.len(),
					w.before.len()
				));
			}
			abandon(&tree);
		}
	}
	assert!(problems.is_empty(), "{what}: {problems:?}");
}

/// The `.sst` files of a checkpoint, in name order.
fn checkpoint_tables(ckpt: &Path) -> Vec<PathBuf> {
	let mut ssts: Vec<PathBuf> = std::fs::read_dir(ckpt.join("sstables"))
		.unwrap()
		.map(|e| e.unwrap().path())
		.filter(|p| p.extension().is_some_and(|e| e == "sst"))
		.collect();
	ssts.sort();
	assert!(!ssts.is_empty(), "setup: the checkpoint holds tables");
	ssts
}

#[test(tokio::test)]
async fn a_restore_of_a_checkpoint_without_metadata_fails_and_leaves_the_database() {
	bad_checkpoint("no metadata", |ckpt| {
		std::fs::remove_file(ckpt.join("CHECKPOINT_METADATA")).unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn a_restore_of_a_checkpoint_with_torn_metadata_fails_and_leaves_the_database() {
	bad_checkpoint("torn metadata", |ckpt| {
		let bytes = std::fs::read(ckpt.join("CHECKPOINT_METADATA")).unwrap();
		std::fs::write(ckpt.join("CHECKPOINT_METADATA"), &bytes[..bytes.len() / 2]).unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn a_restore_of_a_checkpoint_with_empty_metadata_fails_and_leaves_the_database() {
	bad_checkpoint("empty metadata", |ckpt| {
		std::fs::write(ckpt.join("CHECKPOINT_METADATA"), b"").unwrap();
	})
	.await;
}

#[test(tokio::test)]
async fn a_restore_of_a_checkpoint_of_an_unsupported_version_fails_and_leaves_the_database() {
	bad_checkpoint("unsupported version", |ckpt| {
		let mut bytes = std::fs::read(ckpt.join("CHECKPOINT_METADATA")).unwrap();
		bytes[..4].copy_from_slice(&7u32.to_be_bytes());
		std::fs::write(ckpt.join("CHECKPOINT_METADATA"), bytes).unwrap();
	})
	.await;
}

#[test(tokio::test)]
#[ignore = "a restore of a checkpoint that cannot be loaded fails only after it removed the database's files, leaving a database that does not open"]
async fn a_restore_of_a_checkpoint_missing_a_listed_table_fails_and_leaves_the_database() {
	bad_checkpoint("a listed table is missing", |ckpt| {
		std::fs::remove_file(&checkpoint_tables(ckpt)[0]).unwrap();
	})
	.await;
}

#[test(tokio::test)]
#[ignore = "a restore of a checkpoint that cannot be loaded fails only after it removed the database's files, leaving a database that does not open"]
async fn a_restore_of_a_checkpoint_with_a_truncated_table_fails_and_leaves_the_database() {
	bad_checkpoint("a table is truncated", |ckpt| {
		let table = checkpoint_tables(ckpt)[0].clone();
		// A copy, not the link: truncating the file would truncate the live table too.
		let bytes = std::fs::read(&table).unwrap();
		std::fs::remove_file(&table).unwrap();
		std::fs::write(&table, &bytes[..bytes.len() / 3]).unwrap();
	})
	.await;
}

#[test(tokio::test)]
#[ignore = "a restore of a checkpoint that cannot be loaded fails only after it removed the database's files, leaving a database that does not open"]
async fn a_restore_of_a_checkpoint_without_a_tables_directory_fails_and_leaves_the_database() {
	bad_checkpoint("no sstables directory", |ckpt| {
		std::fs::remove_dir_all(ckpt.join("sstables")).unwrap();
	})
	.await;
}

#[test(tokio::test)]
#[ignore = "a restore of a checkpoint that cannot be loaded fails only after it removed the database's files, leaving a database that does not open"]
async fn a_restore_of_a_checkpoint_without_a_manifest_fails_and_leaves_the_database() {
	bad_checkpoint("no manifest directory", |ckpt| {
		std::fs::remove_dir_all(ckpt.join("manifest")).unwrap();
	})
	.await;
}

// ---------------------------------------------------------------------------
// successive restores and other layouts
// ---------------------------------------------------------------------------

/// Restoring the same checkpoint twice in a row, with commits and deletes between, gives the
/// checkpoint every time.
async fn restoring_twice(vlog: bool) {
	let mut w = build(vlog).await;
	w.tree.restore_from_checkpoint(&w.ckpt).unwrap();
	assert_exact(&w.tree, &w.at_ckpt, "the first restore");
	w.tree.restore_from_checkpoint(&w.ckpt).unwrap();
	assert_exact(&w.tree, &w.at_ckpt, "the second restore, back to back");
	let mut m = w.at_ckpt.clone();
	apply(&w.tree, &mut m, sets(0..50, "mid", vlog)).await.unwrap();
	apply(&w.tree, &mut m, dels(10..20)).await.unwrap();
	assert_exact_with_extra(&w.tree, &m, "between the restores");
	w.tree.restore_from_checkpoint(&w.ckpt).unwrap();
	assert_exact(&w.tree, &w.at_ckpt, "the third restore");
	let tree = w.reopen().await;
	assert_exact(&tree, &w.at_ckpt, "after the reopen");
	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn restoring_the_same_checkpoint_twice_gives_the_checkpoint_each_time() {
	restoring_twice(false).await;
}

#[test(tokio::test)]
async fn restoring_the_same_checkpoint_twice_gives_the_checkpoint_each_time_with_a_value_log() {
	restoring_twice(true).await;
}

/// Restoring an older checkpoint after a newer one, and the newer after the older, each time gives
/// exactly the checkpoint that was restored.
async fn restoring_in_every_order(vlog: bool) {
	let dir = TempDir::new("restore_conc_order").unwrap();
	let roots = TempDir::new("restore_conc_order_ckpt").unwrap();
	let opts = options(dir.path(), vlog);
	let tree = open(Arc::clone(&opts));
	let mut m = Model::new();
	let mut states = Vec::new();
	let mut ckpts = Vec::new();
	for round in 0..3 {
		let gen = format!("r{round}");
		apply(&tree, &mut m, sets(round * 20..round * 20 + 40, &gen, vlog)).await.unwrap();
		if round > 0 {
			apply(&tree, &mut m, dels(round * 10..round * 10 + 5)).await.unwrap();
		}
		let cp = roots.path().join(format!("ckpt{round}"));
		tree.create_checkpoint(&cp).unwrap();
		states.push(m.clone());
		ckpts.push(cp);
	}
	apply(&tree, &mut m, sets(100..110, "latest", vlog)).await.unwrap();
	for (name, round) in [("newest", 2), ("oldest", 0), ("middle", 1), ("newest again", 2)] {
		tree.restore_from_checkpoint(&ckpts[round]).unwrap();
		assert_same(&contents(&tree), &states[round], &format!("restoring {name}"));
		// Work on top of each restore, so that it is not a fresh state that is read.
		let mut next = states[round].clone();
		apply(&tree, &mut next, sets(120..125, &format!("on_{name}"), vlog)).await.unwrap();
		assert_same(&contents(&tree), &next, &format!("a commit after restoring {name}"));
	}
	tree.restore_from_checkpoint(&ckpts[2]).unwrap();
	tree.close().await.unwrap();
	let tree = open(options(dir.path(), vlog));
	assert_same(&contents(&tree), &states[2], "after the reopen");
	tree.close().await.unwrap();
}

#[test(tokio::test)]
async fn restoring_an_older_checkpoint_after_a_newer_one_and_back() {
	restoring_in_every_order(false).await;
}

#[test(tokio::test)]
#[ignore = "a restore leaves the value log writing to its deleted files: large values committed afterwards are unreadable"]
async fn restoring_an_older_checkpoint_after_a_newer_one_and_back_with_a_value_log() {
	restoring_in_every_order(true).await;
}

/// Builds a tree with the value log on in `dir`, checkpoints it, and returns what it held and the
/// checkpoint's directory.
async fn source_checkpoint(dir: &Path, roots: &Path, vlog: bool) -> (Model, PathBuf) {
	let source = open(options(dir, vlog));
	let mut model = Model::new();
	apply(&source, &mut model, sets(0..45, "s", vlog)).await.unwrap();
	source.flush().unwrap();
	apply(&source, &mut model, sets(45..60, "s", vlog)).await.unwrap();
	let ckpt = roots.join("ckpt");
	source.create_checkpoint(&ckpt).unwrap();
	source.close().await.unwrap();
	(model, ckpt)
}

/// A checkpoint taken from another tree, whose value log has other files under the same ids, is
/// restored into a tree that has its own value-log files: every value of the checkpoint is read
/// from the checkpoint's files. The target has written its data and read none of it, so that no
/// cache holds anything of it.
#[test(tokio::test)]
async fn restoring_a_checkpoint_from_a_tree_with_another_value_log_layout() {
	let source_dir = TempDir::new("restore_conc_source").unwrap();
	let roots = TempDir::new("restore_conc_source_ckpt").unwrap();
	let (source_model, ckpt) = source_checkpoint(source_dir.path(), roots.path(), true).await;
	let source_files = std::fs::read_dir(ckpt.join("vlog")).unwrap().count();

	let target_dir = TempDir::new("restore_conc_target").unwrap();
	let target_opts = options(target_dir.path(), true);
	let target = open(Arc::clone(&target_opts));
	let mut written = Model::new();
	apply(&target, &mut written, sets(0..60, "t", true)).await.unwrap();
	target.flush().unwrap();
	apply(&target, &mut written, sets(60..90, "t", true)).await.unwrap();
	let target_files = std::fs::read_dir(target_opts.vlog_dir()).unwrap().count();
	assert!(target_files > 1 && source_files > 1, "setup: several value-log files each");

	target.restore_from_checkpoint(&ckpt).unwrap();
	assert_same(
		&contents(&target),
		&source_model,
		"the target after restoring the other tree's checkpoint",
	);
	let t = target.begin_with_mode(Mode::ReadOnly).unwrap();
	for (k, v) in &source_model {
		assert_eq!(t.get(&k[..]).unwrap().as_ref(), Some(v), "lookup of {}", show(k));
	}
	drop(t);
	target.close().await.unwrap();
	let tree = open(options(target_dir.path(), true));
	assert_same(&contents(&tree), &source_model, "after the reopen");
	tree.close().await.unwrap();
}

/// A checkpoint of a tree that has no value log has no value-log directory. Restoring it into a
/// tree that has one removes that tree's value-log files: nothing in the restored tables points
/// into them, and nothing would ever collect them.
#[test(tokio::test)]
async fn restoring_a_checkpoint_without_a_value_log_removes_the_value_log_files_of_the_database() {
	let source_dir = TempDir::new("restore_conc_source").unwrap();
	let roots = TempDir::new("restore_conc_source_ckpt").unwrap();
	let (source_model, ckpt) = source_checkpoint(source_dir.path(), roots.path(), false).await;
	assert!(!ckpt.join("vlog").exists(), "setup: the checkpoint has no value log");

	let target_dir = TempDir::new("restore_conc_target").unwrap();
	let target_opts = options(target_dir.path(), true);
	let target = open(Arc::clone(&target_opts));
	let mut written = Model::new();
	apply(&target, &mut written, sets(0..60, "t", true)).await.unwrap();
	let vlog_files = |dir: &Path| -> usize {
		std::fs::read_dir(dir)
			.map(|entries| entries.filter_map(Result::ok).filter(|e| e.path().is_file()).count())
			.unwrap_or(0)
	};
	assert!(vlog_files(&target_opts.vlog_dir()) > 1, "setup: the target has value-log files");

	target.restore_from_checkpoint(&ckpt).unwrap();
	assert_eq!(
		vlog_files(&target_opts.vlog_dir()),
		0,
		"value-log files of the old database are left after the restore"
	);
	assert_same(&contents(&target), &source_model, "the target after the restore");
	target.close().await.unwrap();
}

/// The WAL of a checkpoint is replayed by a restore. A checkpoint that `create_checkpoint` makes
/// has an empty one, but the files of a database that stopped without flushing are a valid source,
/// and the restore must not lose what is in their WAL. The restored database continues in a segment
/// of its own: the segments it was given are links to the checkpoint's files, and its commits must
/// never be appended to them.
#[test(tokio::test)]
async fn a_restore_replays_the_wal_of_a_checkpoint_and_continues_in_a_segment_of_its_own() {
	let w = build(false).await;
	let roots = TempDir::new("restore_conc_carried").unwrap();
	let carried = roots.path().join("carried");
	copy_dir(&w.path, &carried);
	let meta = CheckpointMetadata::new(0, w.tree.core.seq_num(), 0, 0);
	std::fs::write(carried.join("CHECKPOINT_METADATA"), meta.to_bytes().unwrap()).unwrap();
	let files = fingerprint(&carried);
	assert!(
		files.keys().any(|k| k.starts_with("wal/") && k.ends_with(".wal")),
		"setup: the directory holds a WAL segment: {:?}",
		files.keys().collect::<Vec<_>>()
	);
	let source = open_checkpoint_copy(&carried, false).unwrap();
	assert_same(&source, &w.before, "setup: the directory as a database");

	let target_dir = TempDir::new("restore_conc_target").unwrap();
	let target = open(options(target_dir.path(), false));
	let mut written = Model::new();
	apply(&target, &mut written, sets(0..60, "t", false)).await.unwrap();
	target.flush().unwrap();
	apply(&target, &mut written, sets(60..90, "t", false)).await.unwrap();

	target.restore_from_checkpoint(&carried).unwrap();
	assert_same(&contents(&target), &w.before, "after restoring a checkpoint that carries a WAL");
	let mut m = w.before.clone();
	apply(&target, &mut m, sets(300..310, "n", false)).await.unwrap();
	apply(&target, &mut m, dels(0..4)).await.unwrap();
	assert_exact_with_extra(&target, &m, "after commits on the restored database");
	assert_eq!(fingerprint(&carried), files, "the checkpoint's files after the commits");

	// What the restored database acknowledged is in its own files.
	let image = TempDir::new("restore_conc_image").unwrap();
	copy_dir(target_dir.path(), image.path());
	abandon(&target);
	abandon(&w.tree);
	let recovered = with_image(image.path(), false, contents).unwrap();
	assert_same(&recovered, &m, "an image after the commits");
}

/// A checkpoint of another tree has tables under the ids of the tables the target has read, and
/// the restore must not leave the target reading through the blocks it cached for its own tables.
#[test(tokio::test)]
#[ignore = "a restore does not clear the block cache: tables of the restored checkpoint are read through blocks cached for the old tables with the same ids"]
async fn restoring_the_checkpoint_of_another_tree_serves_its_tables_and_not_cached_blocks() {
	let source_dir = TempDir::new("restore_conc_source").unwrap();
	let roots = TempDir::new("restore_conc_source_ckpt").unwrap();
	let (source_model, ckpt) = source_checkpoint(source_dir.path(), roots.path(), false).await;
	let w = build(false).await;
	let before = w.before.clone();
	assert_exact(&w.tree, &before, "the target before the restore");
	w.tree.restore_from_checkpoint(&ckpt).unwrap();
	assert_same(
		&contents(&w.tree),
		&source_model,
		"the target after restoring the other tree's checkpoint",
	);
	w.tree.close().await.unwrap();
}

/// Rounds of: restore the checkpoint, then work that makes value-log files obsolete (each flush and
/// each compaction runs the collection), seeded and bounded. After each restore the tree is the
/// checkpoint exactly, and after the work it holds what the work wrote.
#[test(tokio::test)]
#[ignore = "a restore does not clear the block cache: tables written after a restore are read through the blocks cached for the old tables with the same ids"]
async fn restoring_between_rounds_of_work_that_collects_the_value_log() {
	let mut w = build(true).await;
	let seed = 0x5EED_0000_0000_1357u64;
	let mut rng = Rng(seed);
	for round in 0..4 {
		w.tree.restore_from_checkpoint(&w.ckpt).unwrap();
		assert_exact(
			&w.tree,
			&w.at_ckpt,
			&format!("seed {seed:#x} round {round}: after the restore"),
		);
		let mut m = w.at_ckpt.clone();
		for step in 0..3 {
			let start = (rng.next() % 40) as usize;
			let gen = format!("g{round}{step}");
			apply(&w.tree, &mut m, sets(start..start + 20, &gen, true)).await.unwrap();
			if rng.next() % 2 == 0 {
				apply(&w.tree, &mut m, dels(start + 5..start + 10)).await.unwrap();
			}
			w.tree.flush().unwrap();
		}
		w.tree.compact(eager(&w.opts)).unwrap();
		assert_same(
			&contents(&w.tree),
			&m,
			&format!("seed {seed:#x} round {round}: after the work"),
		);
	}
	w.tree.restore_from_checkpoint(&w.ckpt).unwrap();
	let tree = w.reopen().await;
	assert_exact(&tree, &w.at_ckpt, &format!("seed {seed:#x}: after the reopen"));
	tree.close().await.unwrap();
}
