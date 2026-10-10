//! A commit larger than a memtable whose table cannot be registered in the manifest.
//!
//! The commit, or its group, fails with the error of the write, no background error is recorded,
//! the table file of the write is removed unless the manifest on disk may list it, and the next
//! commit succeeds. A group that failed after part of it was applied still stops the database.
//! A manifest write that failed from the rename on is remembered, and the manifest in memory is
//! written again before the next direct table, before the next memtable flush and before a
//! checkpoint copies the manifest.
//!
//! The failpoint is `levels::fail_replacements_sequence`, armed from `direct_table_hook`, which
//! runs between the table of the direct write and its manifest write: the failure then lands on
//! that write and on no other, whatever the flushes before it did.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use tempdir::TempDir;
use test_log::test;

use super::collect_transaction_all;
use super::manifest_write_failure_tests::{
	copy_dir_all,
	flush_in_background,
	immutables,
	manifest_path,
	table_ids,
	wait_until,
	Healed,
	REACHED,
	STUCK,
};
use crate::error::{BackgroundErrorReason, ErrorSeverity, Result};
use crate::levels::{
	fail_replacements,
	fail_replacements_sequence,
	heal_replacements,
	replacements_pending,
	LevelManifest,
	ReplaceFailure,
};
use crate::lsm::FlushHook;
use crate::ring::{CommitStage, PipelineHook};
use crate::{Error, Mode, Options, Tree};

const KIB: usize = 1024;

/// A memtable that a value of a hundred KiB nearly fills.
const MEMTABLE: usize = 128 * KIB;

const NKEYS: usize = 6;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn key(i: usize) -> Vec<u8> {
	format!("key_{i}").into_bytes()
}

/// A value that says which commit wrote it.
fn value_of(tag: u64, len: usize) -> Vec<u8> {
	let mut v = vec![tag as u8; len.max(16)];
	v[..8].copy_from_slice(&tag.to_le_bytes());
	v
}

/// A value too large for a memtable, which sends its commit down the direct path.
fn big_value(tag: u64) -> Vec<u8> {
	value_of(tag, MEMTABLE + 1000)
}

fn describe(value: Option<&Vec<u8>>) -> String {
	match value {
		None => "nothing".to_string(),
		Some(v) if v.len() >= 16 => {
			format!(
				"commit {} of {} bytes",
				u64::from_le_bytes(v[..8].try_into().unwrap()),
				v.len()
			)
		}
		Some(v) => format!("{} bytes", v.len()),
	}
}

fn assert_same(got: &Model, want: &Model, what: &str) {
	for (k, v) in want {
		assert!(
			got.get(k) == Some(v),
			"{what}: {} read {}, expected {}",
			String::from_utf8_lossy(k),
			describe(got.get(k)),
			describe(Some(v))
		);
	}
	for k in got.keys() {
		assert!(want.contains_key(k), "{what}: unexpected key {}", String::from_utf8_lossy(k));
	}
}

/// `got` holds `model`, except that each of the commits in `ambiguous` may be there whole or not
/// at all: their committers were told they failed.
fn assert_recovers(got: &Model, model: &Model, ambiguous: &[Vec<(Vec<u8>, Vec<u8>)>], what: &str) {
	let mut want = model.clone();
	for commit in ambiguous {
		if commit.iter().all(|(k, v)| got.get(k) == Some(v)) {
			want.extend(commit.iter().cloned());
		}
	}
	assert_same(got, &want, what);
}

fn options(path: &Path, vlog: bool) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		max_memtable_size: MEMTABLE,
		// No compaction can start, and so none can meet an armed failpoint.
		level0_max_files: 1000,
		memtable_stall_threshold: 10_000,
		l0_stall_threshold: 10_000,
		enable_vlog: vlog,
		vlog_value_threshold: 1024,
		..Default::default()
	})
}

async fn open_tree(path: &Path, background: bool, vlog: bool) -> Arc<Tree> {
	let tree = Arc::new(Tree::new(options(path, vlog)).unwrap());
	if !background {
		let tasks = tree.core.task_manager.lock().unwrap().take().unwrap();
		tasks.stop().await;
	}
	tree
}

/// Lets a tree that a test is done with leave its directory as a crash does.
fn abandon(tree: &Tree) {
	tree.core.is_closed.store(true, Ordering::SeqCst);
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
}

fn all_keys(tree: &Tree) -> Model {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	let all = collect_transaction_all(&mut txn.iter().unwrap()).unwrap();
	all.into_iter().collect()
}

/// What a copy of the directory of `tree`, opened as a crash recovers.
async fn crash_image(tree: &Tree, root: &Path, name: &str) -> Model {
	// The background flush deletes WAL and value log files after it has emptied the queue: let it
	// finish and stop it, or the copy of the live directory races those deletions.
	let tasks = tree.core.task_manager.lock().unwrap().take();
	if let Some(tasks) = tasks {
		tasks.stop().await;
	}
	let image = root.join(name);
	copy_dir_all(&tree.core.inner.opts.path, &image);
	let recovered = open_tree(&image, false, tree.core.inner.opts.enable_vlog).await;
	let all = all_keys(&recovered);
	assert_eq!(
		disk_tables(&recovered).expect("the image lists tables that have their files"),
		table_ids(&recovered)
	);
	abandon(&recovered);
	all
}

fn log_number(tree: &Tree) -> u64 {
	tree.core.inner.level_manifest.read().unwrap().get_log_number()
}

/// The ids of the `.sst` files in the directory of tables.
fn sst_ids(tree: &Tree) -> Vec<u64> {
	let mut ids: Vec<u64> = std::fs::read_dir(tree.core.inner.opts.sstable_dir())
		.unwrap()
		.filter_map(|entry| {
			let entry = entry.unwrap();
			let path = entry.path();
			(entry.file_type().unwrap().is_file() && path.extension().is_some_and(|e| e == "sst"))
				.then(|| path.file_stem().unwrap().to_str().unwrap().parse().unwrap())
		})
		.collect();
	ids.sort_unstable();
	ids
}

/// The ids of the tables the manifest file lists. Loading it fails if a listed table has no file.
fn disk_tables(tree: &Tree) -> Result<Vec<u64>> {
	let opts = Arc::clone(&tree.core.inner.opts);
	let manifest = LevelManifest::load_from_file(opts.manifest_file_path(0), opts)?;
	let mut ids: Vec<u64> = manifest.iter().map(|t| t.id).collect();
	ids.sort_unstable();
	Ok(ids)
}

fn assert_unique(ids: &[u64], what: &str) {
	let distinct: BTreeSet<u64> = ids.iter().copied().collect();
	assert_eq!(distinct.len(), ids.len(), "{what}: a table is listed twice: {ids:?}");
}

/// The tables on disk are the tables listed, and listed once.
fn assert_no_orphans(tree: &Tree, what: &str) {
	let listed = table_ids(tree);
	assert_unique(&listed, what);
	assert_eq!(sst_ids(tree), listed, "{what}: the table files are not the tables listed");
}

/// A failed commit recorded no background error and stopped nothing. `recover` is asked last, as
/// `get_error` shows only the most severe entry: it finds an error of this reason if there is one.
fn assert_healthy(tree: &Tree, what: &str) {
	let handler = &tree.core.inner.error_handler;
	assert!(handler.check_error().is_ok(), "{what}: {:?}", handler.check_error());
	assert!(!handler.is_db_stopped(), "{what}: the database is stopped");
	assert!(handler.get_error().is_none(), "{what}: {:?}", handler.get_error());
	assert!(
		!handler.recover(BackgroundErrorReason::ManifestWrite),
		"{what}: a manifest write error was recorded"
	);
}

async fn commit_value(tree: &Tree, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
	let mut txn = tree.begin()?;
	txn.set(key, value)?;
	txn.commit().await
}

/// The entries of an oversized commit. With the value log the values are not part of what a
/// memtable holds, so it takes many entries to exceed one.
fn oversized_entries(vlog: bool, tag: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
	if vlog {
		(0..4000).map(|i| (format!("vbig_{i:05}").into_bytes(), value_of(tag, 1100))).collect()
	} else {
		vec![(key(0), big_value(tag))]
	}
}

async fn commit_entries(tree: &Tree, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
	let mut txn = tree.begin()?;
	for (key, value) in entries {
		txn.set(key.clone(), value.clone())?;
	}
	txn.commit().await
}

/// An oversized commit that overwrites `key_{i}`.
async fn commit_big(tree: &Tree, i: usize, tag: u64) -> Result<()> {
	commit_value(tree, key(i), big_value(tag)).await
}

async fn commit_small(tree: &Tree, model: &mut Model, i: usize, tag: u64) {
	let value = value_of(tag, 100);
	commit_value(tree, key(i), value.clone()).await.unwrap();
	model.insert(key(i), value);
}

/// Writes every key once, so that the active memtable holds something the direct commit seals.
async fn seed(tree: &Tree, model: &mut Model, tag: u64) {
	let mut txn = tree.begin().unwrap();
	for i in 0..NKEYS {
		let value = value_of(tag, 100);
		txn.set(key(i), value.clone()).unwrap();
		model.insert(key(i), value);
	}
	txn.commit().await.unwrap();
}

/// What `install_armer` records, and how a test arms the manifest failpoint for the next direct
/// write.
struct Armer {
	calls: Arc<AtomicUsize>,
	plan: Arc<Mutex<Option<Vec<ReplaceFailure>>>>,
	/// `log_number` at the last call.
	log: Arc<AtomicU64>,
	/// The tables listed at the last call.
	listed: Arc<Mutex<Vec<u64>>>,
}

impl Armer {
	/// The next direct write that reaches its table fails its manifest writes at `steps`.
	fn arm(&self, steps: &[ReplaceFailure]) {
		*self.plan.lock().unwrap() = Some(steps.to_vec());
	}

	fn calls(&self) -> usize {
		self.calls.load(Ordering::SeqCst)
	}
}

/// Installs the hook that counts the direct writes that have written their table and, when armed,
/// arms the failpoint before the write of the manifest. The flushes before the table are over by
/// then, so the failpoint is for the write of the manifest of the table and for what follows it.
fn install_armer(tree: &Tree) -> Armer {
	let manifest = manifest_path(tree);
	let level_manifest = Arc::clone(&tree.core.inner.level_manifest);
	let armer = Armer {
		calls: Arc::new(AtomicUsize::new(0)),
		plan: Arc::new(Mutex::new(None)),
		log: Arc::new(AtomicU64::new(0)),
		listed: Arc::new(Mutex::new(Vec::new())),
	};
	let (calls, plan, log, listed) = (
		Arc::clone(&armer.calls),
		Arc::clone(&armer.plan),
		Arc::clone(&armer.log),
		Arc::clone(&armer.listed),
	);
	let hook: FlushHook = Arc::new(move |_| {
		calls.fetch_add(1, Ordering::SeqCst);
		{
			let manifest = level_manifest.read().unwrap();
			log.store(manifest.get_log_number(), Ordering::SeqCst);
			let mut ids: Vec<u64> = manifest.iter().map(|t| t.id).collect();
			ids.sort_unstable();
			*listed.lock().unwrap() = ids;
		}
		if let Some(steps) = plan.lock().unwrap().take() {
			fail_replacements_sequence(&manifest, &steps);
		}
		Ok(())
	});
	*tree.core.inner.direct_table_hook.lock() = Some(hook);
	armer
}

/// A tree that has been through the double failure: the manifest write of a direct table landed
/// and the one that settled it failed. The manifest on disk lists the table, the one in memory
/// does not, the file of the table is kept, and the commit failed.
struct Unsettled {
	dir: TempDir,
	tree: Arc<Tree>,
	/// What the tree holds: the failed commit is not in it.
	model: Model,
	/// The commit that failed.
	failed: Vec<(Vec<u8>, Vec<u8>)>,
	manifest: PathBuf,
	armer: Armer,
	/// The table of the failed commit.
	table: u64,
	_healed: Healed,
}

async fn unsettled(background: bool, vlog: bool, before: Option<&Path>) -> Unsettled {
	let dir = TempDir::new("direct_manifest_unsettled").unwrap();
	let tree = open_tree(dir.path(), background, vlog).await;
	let mut model = Model::new();
	seed(&tree, &mut model, 1).await;
	if let Some(checkpoint) = before {
		tree.create_checkpoint(checkpoint).unwrap();
	}
	let manifest = manifest_path(&tree);
	let healed = Healed(manifest.clone());
	let armer = install_armer(&tree);
	armer.arm(&[ReplaceFailure::AfterRename, ReplaceFailure::BeforeRename]);

	let entries = oversized_entries(vlog, 9);
	let error = commit_entries(&tree, &entries).await.expect_err("the manifest write fails");
	assert!(
		error.to_string().contains("Manifest write outcome unknown"),
		"the error keeps the kind of the write: {error}"
	);
	assert_eq!(armer.calls(), 1, "the failure landed on the direct write");
	assert!(!replacements_pending(&manifest), "both writes failed");
	assert!(tree.core.inner.manifest_uncertain());
	let listed = disk_tables(&tree).expect("the manifest on disk lists tables that have files");
	let memory = table_ids(&tree);
	let extra: Vec<u64> = listed.iter().copied().filter(|id| !memory.contains(id)).collect();
	assert_eq!(extra.len(), 1, "the disk lists one table that memory does not: {listed:?}");
	assert!(sst_ids(&tree).contains(&extra[0]), "the table is kept");
	Unsettled {
		dir,
		tree,
		model,
		failed: entries,
		manifest,
		armer,
		table: extra[0],
		_healed: healed,
	}
}

// ---------------------------------------------------------------------------
// one failure, nothing recorded
// ---------------------------------------------------------------------------

/// A manifest write that fails before the rename fails the commit with the error of the write,
/// whose group fails like any other that nothing of was applied. Nothing is recorded, the next
/// commits succeed, the oversized one too, and every read is as the commits say.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_direct_commit_that_cannot_write_the_manifest_fails_and_the_next_commit_succeeds() {
	let dir = TempDir::new("direct_manifest_fail").unwrap();
	let tree = open_tree(dir.path(), false, false).await;
	let mut model = Model::new();
	seed(&tree, &mut model, 1).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());
	let armer = install_armer(&tree);
	armer.arm(&[ReplaceFailure::BeforeRename]);

	let error = commit_big(&tree, 0, 2).await.expect_err("the manifest write fails");
	assert!(matches!(error, Error::Io(_)), "{error:?}");
	let text = error.to_string();
	assert!(text.contains("Group commit failed"), "{text}");
	assert!(text.contains("injected failure before the rename"), "{text}");
	assert_eq!(armer.calls(), 1, "the failure landed on the direct write");
	assert!(!replacements_pending(&manifest), "the direct write consumed the failure");
	assert!(!tree.core.inner.manifest_uncertain());
	assert_healthy(&tree, "after the failed commit");
	assert_same(&all_keys(&tree), &model, "the failed commit is not readable");

	commit_small(&tree, &mut model, 1, 3).await;
	commit_big(&tree, 0, 4).await.unwrap();
	model.insert(key(0), big_value(4));
	assert_eq!(armer.calls(), 2);
	assert_same(&all_keys(&tree), &model, "after the next commits");
	assert_unique(&table_ids(&tree), "the listed tables");
	assert_healthy(&tree, "after the next commits");
	abandon(&tree);
}

/// A failure before the rename leaves no file of the table, the manifest in memory as it was and
/// `log_number` where it was, so the sealed segment is still replayed. The failed commit is whole
/// or absent after a crash, and the commits before and after it are not lost.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_failed_direct_commit_leaves_no_table_file_and_keeps_log_number() {
	let dir = TempDir::new("direct_manifest_files").unwrap();
	let images = TempDir::new("direct_manifest_files_image").unwrap();
	let tree = open_tree(dir.path(), false, false).await;
	let mut model = Model::new();
	seed(&tree, &mut model, 1).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());
	let armer = install_armer(&tree);
	assert_no_orphans(&tree, "before");
	armer.arm(&[ReplaceFailure::BeforeRename]);

	commit_big(&tree, 0, 2).await.expect_err("the manifest write fails");
	assert_eq!(armer.calls(), 1);
	assert_no_orphans(&tree, "after the failed commit");
	assert_eq!(
		table_ids(&tree),
		*armer.listed.lock().unwrap(),
		"the listed tables are those the flush before the write left"
	);
	assert_eq!(log_number(&tree), armer.log.load(Ordering::SeqCst), "log_number did not move");

	let failed = [vec![(key(0), big_value(2))]];
	let image = crash_image(&tree, images.path(), "failed").await;
	assert_recovers(&image, &model, &failed, "the image after the failed commit");

	commit_big(&tree, 0, 5).await.unwrap();
	model.insert(key(0), big_value(5));
	assert_no_orphans(&tree, "after the next commit");
	let image = crash_image(&tree, images.path(), "next").await;
	assert_same(&image, &model, "the image after the next commit");
	abandon(&tree);
}

/// An error after the file of the table is written, other than a failed manifest write, removes
/// the file. A path that cannot be created is not the file of this write and is left alone.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn an_error_after_the_table_file_is_written_removes_it() {
	let dir = TempDir::new("direct_manifest_hook").unwrap();
	let tree = open_tree(dir.path(), false, false).await;
	let mut model = Model::new();
	seed(&tree, &mut model, 1).await;
	let calls = Arc::new(AtomicUsize::new(0));
	let counter = Arc::clone(&calls);
	let hook: FlushHook = Arc::new(move |_| {
		if counter.fetch_add(1, Ordering::SeqCst) == 0 {
			Err(Error::Other("injected failure after the table was written".into()))
		} else {
			Ok(())
		}
	});
	*tree.core.inner.direct_table_hook.lock() = Some(hook);

	let error = commit_big(&tree, 0, 2).await.expect_err("the table fails");
	assert!(error.to_string().contains("injected failure after the table"), "{error}");
	assert_eq!(calls.load(Ordering::SeqCst), 1);
	assert_no_orphans(&tree, "after the failed table");
	assert_healthy(&tree, "after the failed table");
	assert_same(&all_keys(&tree), &model, "the failed commit is not readable");

	commit_big(&tree, 0, 3).await.unwrap();
	model.insert(key(0), big_value(3));
	assert_no_orphans(&tree, "after the next commit");
	let listed = table_ids(&tree);
	assert_unique(&listed, "the listed tables");

	// A directory where the table goes: the file cannot be created, and the directory stays.
	let next = tree.core.inner.level_manifest.read().unwrap().next_table_id.load(Ordering::SeqCst);
	commit_small(&tree, &mut model, 2, 6).await;
	let blocker = tree.core.inner.opts.sstable_file_path(next + 1);
	std::fs::create_dir(&blocker).unwrap();
	commit_big(&tree, 1, 7).await.expect_err("the table cannot be created");
	assert!(blocker.is_dir(), "the directory that was in the way is still there");
	std::fs::remove_dir(&blocker).unwrap();
	assert_healthy(&tree, "after the blocked table");
	commit_big(&tree, 1, 8).await.unwrap();
	model.insert(key(1), big_value(8));
	assert_same(&all_keys(&tree), &model, "after the blocked table");
	assert_no_orphans(&tree, "after the blocked table");
	abandon(&tree);
}

// ---------------------------------------------------------------------------
// a write that may have landed
// ---------------------------------------------------------------------------

/// A manifest write that fails after the rename keeps its kind in the error. The manifest in
/// memory is written again at once, which ends the doubt, so the table, which that manifest does
/// not list, is removed. A crash right after holds the earlier commits and lists no missing file.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_direct_commit_whose_manifest_write_may_have_landed_settles_and_removes_the_table() {
	let dir = TempDir::new("direct_manifest_after").unwrap();
	let images = TempDir::new("direct_manifest_after_image").unwrap();
	let tree = open_tree(dir.path(), false, false).await;
	let mut model = Model::new();
	seed(&tree, &mut model, 1).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());
	let armer = install_armer(&tree);
	armer.arm(&[ReplaceFailure::AfterRename]);

	let error = commit_big(&tree, 0, 2).await.expect_err("the manifest write fails");
	let text = error.to_string();
	assert!(text.contains("Group commit failed"), "{text}");
	assert!(text.contains("Manifest write outcome unknown"), "{text}");
	assert_eq!(armer.calls(), 1);
	assert!(!replacements_pending(&manifest));
	assert!(!tree.core.inner.manifest_uncertain(), "the manifest was written again");
	assert_no_orphans(&tree, "after the failed commit");
	assert_eq!(disk_tables(&tree).unwrap(), table_ids(&tree), "the disk lists what memory does");
	assert_healthy(&tree, "after the failed commit");
	assert_same(&all_keys(&tree), &model, "the failed commit is not readable");

	let image = crash_image(&tree, images.path(), "after").await;
	assert_recovers(&image, &model, &[vec![(key(0), big_value(2))]], "the image");

	commit_big(&tree, 0, 3).await.unwrap();
	model.insert(key(0), big_value(3));
	assert_no_orphans(&tree, "after the next commit");
	assert_same(&all_keys(&tree), &model, "after the next commit");
	abandon(&tree);
}

/// The write lands and writing the manifest in memory again fails too: the disk lists a table
/// that memory does not. The file is kept, so a crash cannot leave a manifest that names a missing
/// file; the flag is set, the database takes commits, and the next write of the manifest repairs
/// it. The kept file is removed by the next open.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_direct_commit_whose_manifest_write_stays_uncertain_keeps_the_table_until_it_is_settled()
{
	let images = TempDir::new("direct_manifest_uncertain_image").unwrap();
	let mut fx = unsettled(false, false, None).await;
	let tree = Arc::clone(&fx.tree);
	assert_healthy(&tree, "with the manifest unsettled");
	assert_same(&all_keys(&tree), &fx.model, "the failed commit is not readable");

	commit_small(&tree, &mut fx.model, 1, 3).await;
	let image = crash_image(&tree, images.path(), "unsettled").await;
	assert_recovers(
		&image,
		&fx.model,
		&[fx.failed.clone()],
		"the image with the manifest unsettled",
	);

	commit_big(&tree, 2, 4).await.unwrap();
	fx.model.insert(key(2), big_value(4));
	assert_eq!(fx.armer.calls(), 2);
	assert!(!tree.core.inner.manifest_uncertain(), "the next write settled the manifest");
	assert_eq!(disk_tables(&tree).unwrap(), table_ids(&tree));
	assert!(!table_ids(&tree).contains(&fx.table), "the kept table is not listed again");
	assert_same(&all_keys(&tree), &fx.model, "after the next commit");
	assert_healthy(&tree, "after the next commit");

	tree.close().await.unwrap();
	let reopened = open_tree(fx.dir.path(), false, false).await;
	assert_no_orphans(&reopened, "after the reopen");
	assert!(!sst_ids(&reopened).contains(&fx.table), "the open removed the kept table");
	assert_recovers(&all_keys(&reopened), &fx.model, &[fx.failed.clone()], "the reopened tree");
	abandon(&reopened);
}

// ---------------------------------------------------------------------------
// the checkpoint
// ---------------------------------------------------------------------------

async fn checkpoint(tree: &Arc<Tree>, to: &Path) -> Result<()> {
	let (tree, to) = (Arc::clone(tree), to.to_path_buf());
	tokio::task::spawn_blocking(move || tree.create_checkpoint(&to).map(|_| ())).await.unwrap()
}

/// A checkpoint writes the manifest in memory first when the one on disk may differ, so the
/// manifest it copies lists the tables it copies. One that cannot write it fails and records
/// nothing, and the flag stays set.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_taken_while_the_manifest_is_unsettled_can_be_restored() {
	let out = TempDir::new("direct_manifest_checkpoint").unwrap();
	let fx = unsettled(false, false, None).await;
	let cp = out.path().join("cp");
	checkpoint(&fx.tree, &cp).await.expect("the checkpoint settles the manifest first");
	let copy = Tree::new(options(&cp, false))
		.unwrap_or_else(|e| panic!("the checkpoint lists a table it did not copy: {e}"));
	assert_same(&all_keys(&copy), &fx.model, "the checkpoint");
	assert!(!table_ids(&copy).contains(&fx.table));
	assert!(!fx.tree.core.inner.manifest_uncertain());
	let tasks = copy.core.task_manager.lock().unwrap().take().unwrap();
	tasks.stop().await;
	abandon(&copy);
	abandon(&fx.tree);

	// A manifest that cannot be written: the checkpoint fails and the flag stays.
	let fx = unsettled(false, false, None).await;
	fail_replacements(&fx.manifest, ReplaceFailure::BeforeRename, 0, usize::MAX);
	let error = checkpoint(&fx.tree, &out.path().join("cp_failed"))
		.await
		.expect_err("the manifest cannot be settled");
	assert!(!error.to_string().is_empty());
	assert!(fx.tree.core.inner.manifest_uncertain());
	assert_healthy(&fx.tree, "after the failed checkpoint");
	heal_replacements(&fx.manifest);
	checkpoint(&fx.tree, &out.path().join("cp_healed")).await.expect("the retry succeeds");
	assert!(!fx.tree.core.inner.manifest_uncertain());
	abandon(&fx.tree);
}

// ---------------------------------------------------------------------------
// groups
// ---------------------------------------------------------------------------

/// Holds the flusher after it logged the first group, until three commits were accepted: the
/// first, the one that logged, and the two behind it, which are then one group.
struct Hold {
	accepted: Arc<AtomicUsize>,
	logged: Arc<AtomicBool>,
}

fn hold_flusher(tree: &Tree) -> Hold {
	let accepted = Arc::new(AtomicUsize::new(0));
	let logged = Arc::new(AtomicBool::new(false));
	let counter = Arc::clone(&accepted);
	tree.core.commit_pipeline.set_commit_hook(Some(Arc::new(move |stage| {
		if stage == CommitStage::Accepted {
			counter.fetch_add(1, Ordering::SeqCst);
		}
	})));
	let (queued, flag) = (Arc::clone(&accepted), Arc::clone(&logged));
	let first = AtomicBool::new(true);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::AfterWalSync {
			..
		} = point
		{
			if first.swap(false, Ordering::SeqCst) {
				flag.store(true, Ordering::SeqCst);
				tokio::task::block_in_place(|| {
					let started = std::time::Instant::now();
					while queued.load(Ordering::SeqCst) < 3 && started.elapsed() < REACHED {
						std::thread::sleep(Duration::from_millis(1));
					}
				});
			}
		}
	})));
	Hold {
		accepted,
		logged,
	}
}

fn spawn_commit(
	tree: &Arc<Tree>,
	key: &str,
	value: Vec<u8>,
) -> tokio::task::JoinHandle<Result<()>> {
	let (tree, key) = (Arc::clone(tree), key.as_bytes().to_vec());
	tokio::spawn(async move { commit_value(&tree, key, value).await })
}

async fn finished(handle: tokio::task::JoinHandle<Result<()>>, what: &str) -> Result<()> {
	tokio::time::timeout(STUCK, handle)
		.await
		.unwrap_or_else(|_| panic!("{what}: stuck for more than {STUCK:?}"))
		.unwrap()
}

/// A group of two commits, the first or the second of them oversized, behind a commit of its
/// own that was logged first. Returns the results of the two, the first of them first.
async fn run_group(tree: &Arc<Tree>, model: &mut Model, big_first: bool) -> Vec<Result<()>> {
	let hold = hold_flusher(tree);
	let primer = spawn_commit(tree, "primer", value_of(1, 100));
	wait_until("the first commit to be logged", || hold.logged.load(Ordering::SeqCst)).await;
	model.insert(b"primer".to_vec(), value_of(1, 100));
	let order = if big_first {
		[("grp_a", big_value(2)), ("grp_b", value_of(3, 100))]
	} else {
		[("grp_a", value_of(3, 100)), ("grp_b", big_value(2))]
	};
	let mut handles = Vec::new();
	for (i, (key, value)) in order.into_iter().enumerate() {
		handles.push(spawn_commit(tree, key, value));
		wait_until("the commit to be accepted", || hold.accepted.load(Ordering::SeqCst) >= i + 2)
			.await;
	}
	let mut results = Vec::new();
	for handle in handles {
		results.push(finished(handle, "a committer").await);
	}
	finished(primer, "the first commit").await.unwrap();
	tree.core.commit_pipeline.set_hook(None);
	tree.core.commit_pipeline.set_commit_hook(None);
	results
}

fn group_keys(recovered: &Model, model: &Model, what: &str) {
	let found = ["grp_a", "grp_b"].iter().filter(|k| recovered.contains_key(k.as_bytes())).count();
	assert!(found == 0 || found == 2, "{what}: the group is split, {found} of 2 are there");
	let mut rest = recovered.clone();
	rest.remove(b"grp_a".as_slice());
	rest.remove(b"grp_b".as_slice());
	assert_same(&rest, model, what);
}

/// The oversized commit is the first of its group, so nothing of the group was applied: every
/// committer fails with the error of the write, nothing is recorded, `log_number` stays and the
/// group is whole or absent after a crash. The next commit succeeds.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_failed_first_batch_of_a_group_fails_the_group_without_stopping_the_database() {
	let dir = TempDir::new("direct_manifest_group_first").unwrap();
	let images = TempDir::new("direct_manifest_group_first_image").unwrap();
	let tree = open_tree(dir.path(), false, false).await;
	let mut model = Model::new();
	seed(&tree, &mut model, 1).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());
	let armer = install_armer(&tree);
	armer.arm(&[ReplaceFailure::BeforeRename]);

	let results = run_group(&tree, &mut model, true).await;
	for result in &results {
		let error = result.as_ref().expect_err("the group fails");
		assert!(matches!(error, Error::Io(_)), "{error:?}");
		assert!(error.to_string().contains("Group commit failed"), "{error}");
	}
	assert_eq!(armer.calls(), 1, "the failure landed on the direct write");
	assert!(!replacements_pending(&manifest));
	assert_healthy(&tree, "after the failed group");
	assert_eq!(log_number(&tree), armer.log.load(Ordering::SeqCst), "log_number did not move");
	assert_no_orphans(&tree, "after the failed group");

	let image = crash_image(&tree, images.path(), "group").await;
	group_keys(&image, &model, "the image after the failed group");
	commit_small(&tree, &mut model, 1, 4).await;
	assert_same(
		&all_keys(&tree),
		&model,
		"the commit after the group succeeds and the group is not readable",
	);
	abandon(&tree);
}

/// A direct write that fails after the batch before it was applied stops the database, as any
/// other failure of a group does then: the batches of the group are in the memtables or the
/// segment, and recovery decides. No manifest write error is recorded beside it.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_direct_failure_after_part_of_the_group_was_applied_still_stops_the_database() {
	let dir = TempDir::new("direct_manifest_group_stop").unwrap();
	let images = TempDir::new("direct_manifest_group_stop_image").unwrap();
	let tree = open_tree(dir.path(), false, false).await;
	let mut model = Model::new();
	seed(&tree, &mut model, 1).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());
	let armer = install_armer(&tree);
	armer.arm(&[ReplaceFailure::BeforeRename]);

	let results = run_group(&tree, &mut model, false).await;
	for result in &results {
		assert!(matches!(result, Err(Error::DatabaseStopped(_))), "{result:?}");
	}
	assert_eq!(armer.calls(), 1, "the failure landed on the direct write");
	let handler = &tree.core.inner.error_handler;
	assert!(handler.commit_group_error().is_some());
	assert!(handler.is_db_stopped());
	assert!(
		!handler.recover(BackgroundErrorReason::ManifestWrite),
		"a manifest write error was recorded beside the stop"
	);
	assert_eq!(handler.get_error().unwrap().reason, BackgroundErrorReason::CommitGroup);
	assert_eq!(handler.get_error().unwrap().severity, ErrorSeverity::Unrecoverable);
	assert!(commit_value(&tree, b"later".to_vec(), value_of(5, 100)).await.is_err());
	assert!(tree.create_checkpoint(images.path().join("cp")).is_err(), "a checkpoint is refused");
	assert_ne!(
		tree.core.inner.group_wal_pin.load(Ordering::Acquire),
		u64::MAX,
		"the segment of the group is still pinned"
	);
	assert_eq!(sst_ids(&tree), table_ids(&tree), "no table file of the failed write is left");

	let image = crash_image(&tree, images.path(), "stopped").await;
	group_keys(&image, &model, "the image after the stop");
	abandon(&tree);
}

// ---------------------------------------------------------------------------
// many failures, settled and unsettled
// ---------------------------------------------------------------------------

/// Failures of both kinds, one commit in two, leave no orphan and no duplicate id, every commit
/// that was acknowledged survives a crash and a reopen, and a commit that failed is whole or
/// absent.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn repeated_transient_direct_failures_leave_no_orphans_and_no_duplicate_ids() {
	let dir = TempDir::new("direct_manifest_repeated").unwrap();
	let images = TempDir::new("direct_manifest_repeated_image").unwrap();
	let tree = open_tree(dir.path(), false, false).await;
	let mut model = Model::new();
	seed(&tree, &mut model, 1).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());
	let armer = install_armer(&tree);
	let mut failed = Vec::new();

	for round in 0..30u64 {
		let name = format!("round_{round}").into_bytes();
		if round % 2 == 1 {
			let at = if round % 4 == 1 {
				ReplaceFailure::BeforeRename
			} else {
				ReplaceFailure::AfterRename
			};
			armer.arm(&[at]);
			let value = big_value(round + 10);
			commit_value(&tree, name.clone(), value.clone())
				.await
				.expect_err("the manifest write fails");
			failed.push(vec![(name, value)]);
			assert!(!replacements_pending(&manifest), "round {round}: {at:?} landed on the write");
		} else {
			let value = big_value(round + 10);
			commit_value(&tree, name.clone(), value.clone()).await.unwrap();
			model.insert(name, value);
		}
		assert_eq!(armer.calls(), round as usize + 1, "round {round}");
		assert!(!tree.core.inner.manifest_uncertain(), "round {round}");
		assert_healthy(&tree, &format!("round {round}"));
		assert_same(&all_keys(&tree), &model, &format!("round {round}"));
	}
	assert_no_orphans(&tree, "after the rounds");

	let image = crash_image(&tree, images.path(), "rounds").await;
	assert_recovers(&image, &model, &failed, "the image after the rounds");
	tree.close().await.unwrap();
	let reopened = open_tree(dir.path(), false, false).await;
	assert_no_orphans(&reopened, "after the reopen");
	assert_recovers(&all_keys(&reopened), &model, &failed, "the reopened tree");
	abandon(&reopened);
}

/// With the manifest unsettled and the disk still failing, the settle that comes before the table
/// fails the commit before a file is created: no file, no stop, and the install does not begin.
/// Once the disk works the next commit settles the manifest and succeeds.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_direct_commit_with_the_manifest_unsettled_and_the_disk_failing_creates_no_table() {
	for at in [ReplaceFailure::BeforeRename, ReplaceFailure::AfterRename] {
		let mut fx = unsettled(false, false, None).await;
		let tree = Arc::clone(&fx.tree);
		let files = sst_ids(&tree);
		let calls = fx.armer.calls();
		fail_replacements(&fx.manifest, at, 0, usize::MAX);

		let error = commit_big(&tree, 1, 4).await.expect_err("the manifest cannot be settled");
		let text = error.to_string();
		assert!(text.contains("Group commit failed"), "{at:?}: {text}");
		if at == ReplaceFailure::AfterRename {
			assert!(text.contains("Manifest write outcome unknown"), "{at:?}: {text}");
		}
		assert_eq!(sst_ids(&tree), files, "{at:?}: no table file was created");
		assert_eq!(fx.armer.calls(), calls, "{at:?}: the install did not begin");
		assert!(tree.core.inner.manifest_uncertain(), "{at:?}: the manifest is still unsettled");
		assert_healthy(&tree, &format!("{at:?}: after the refused commit"));

		heal_replacements(&fx.manifest);
		commit_big(&tree, 1, 5).await.unwrap();
		fx.model.insert(key(1), big_value(5));
		assert!(!tree.core.inner.manifest_uncertain(), "{at:?}: the manifest was settled");
		assert_eq!(disk_tables(&tree).unwrap(), table_ids(&tree), "{at:?}");
		assert_same(&all_keys(&tree), &fx.model, &format!("{at:?}: after the next commit"));
		abandon(&tree);
	}
}

// ---------------------------------------------------------------------------
// close, restore, a checkpoint while the install runs
// ---------------------------------------------------------------------------

/// A tree closed with the manifest unsettled reopens: its tables have their files, the kept file
/// is gone, the failed commit is whole or absent, and the commits before and after it are intact.
/// With a commit in the memtable the shutdown flush settles the manifest first.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_and_reopen_right_after_an_unsettled_uncertain_failure() {
	for pending in [false, true] {
		let mut fx = unsettled(false, false, None).await;
		if pending {
			commit_small(&fx.tree, &mut fx.model, 2, 6).await;
		}
		fx.tree.close().await.expect("the tree closes");

		let reopened = open_tree(fx.dir.path(), false, false).await;
		assert!(disk_tables(&reopened).is_ok(), "pending {pending}");
		assert_no_orphans(&reopened, &format!("pending {pending}: after the reopen"));
		assert_recovers(
			&all_keys(&reopened),
			&fx.model,
			&[fx.failed.clone()],
			&format!("pending {pending}: the reopened tree"),
		);
		let mut model = all_keys(&reopened);
		commit_small(&reopened, &mut model, 3, 7).await;
		commit_big(&reopened, 4, 8).await.unwrap();
		model.insert(key(4), big_value(8));
		assert_same(
			&all_keys(&reopened),
			&model,
			&format!("pending {pending}: after more commits"),
		);
		assert_no_orphans(&reopened, &format!("pending {pending}: after more commits"));
		abandon(&reopened);
	}
}

/// A checkpoint that runs while a direct install is between its file and its manifest write
/// copies only tables that are listed, and restores to the state before the commit. The commit
/// then fails and its file is removed.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_concurrent_with_a_direct_install_never_copies_the_table() {
	let dir = TempDir::new("direct_manifest_concurrent").unwrap();
	let out = TempDir::new("direct_manifest_concurrent_out").unwrap();
	let tree = open_tree(dir.path(), false, false).await;
	let mut model = Model::new();
	seed(&tree, &mut model, 1).await;
	let manifest = manifest_path(&tree);
	let _healed = Healed(manifest.clone());

	let (reached_tx, reached_rx) = mpsc::channel();
	let (release_tx, release_rx) = mpsc::channel::<()>();
	let (reached_tx, release_rx) = (Mutex::new(reached_tx), Mutex::new(release_rx));
	let failpoint = manifest.clone();
	let hook: FlushHook = Arc::new(move |_| {
		let _ = reached_tx.lock().unwrap().send(());
		// Bounded, so a broken test fails instead of hanging.
		let _ = release_rx.lock().unwrap().recv_timeout(REACHED);
		fail_replacements_sequence(&failpoint, &[ReplaceFailure::BeforeRename]);
		Ok(())
	});
	*tree.core.inner.direct_table_hook.lock() = Some(hook);

	let commit = spawn_commit(&tree, "key_0", big_value(2));
	wait_until("the install to reach its table", || reached_rx.try_recv().is_ok()).await;
	let table: Vec<u64> =
		sst_ids(&tree).into_iter().filter(|id| !table_ids(&tree).contains(id)).collect();
	assert_eq!(table.len(), 1, "the table of the install is on disk and not listed");

	let cp = out.path().join("cp");
	checkpoint(&tree, &cp).await.expect("the checkpoint does not wait for the install");
	release_tx.send(()).unwrap();
	finished(commit, "the commit").await.expect_err("the manifest write fails");
	assert!(!replacements_pending(&manifest));
	assert_no_orphans(&tree, "after the failed commit");

	let copy = Tree::new(options(&cp, false)).unwrap();
	assert!(!table_ids(&copy).contains(&table[0]), "the checkpoint does not list the table");
	assert_same(&all_keys(&copy), &model, "the checkpoint");
	let tasks = copy.core.task_manager.lock().unwrap().take().unwrap();
	tasks.stop().await;
	abandon(&copy);
	abandon(&tree);
}

/// A restore with the flag set leaves a usable database: the manifest in memory is the restored
/// one, the next write of the manifest settles the flag, and the tree reopens.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn restore_after_a_double_failure_with_the_flag_set() {
	let out = TempDir::new("direct_manifest_restore").unwrap();
	let cp = out.path().join("cp");
	let mut fx = unsettled(false, false, Some(&cp)).await;
	let tree = Arc::clone(&fx.tree);
	let at_checkpoint = fx.model.clone();

	tree.restore_from_checkpoint(&cp).unwrap();
	assert_same(&all_keys(&tree), &at_checkpoint, "the restored tree");
	fx.model = at_checkpoint;

	commit_big(&tree, 1, 4).await.unwrap();
	fx.model.insert(key(1), big_value(4));
	assert!(!tree.core.inner.manifest_uncertain());
	assert_same(&all_keys(&tree), &fx.model, "after the next commit");
	assert_healthy(&tree, "after the restore");

	tree.close().await.unwrap();
	let reopened = open_tree(fx.dir.path(), false, false).await;
	assert_no_orphans(&reopened, "after the reopen");
	assert_same(&all_keys(&reopened), &fx.model, "the reopened tree");
	abandon(&reopened);
}

// ---------------------------------------------------------------------------
// the background flush
// ---------------------------------------------------------------------------

/// A flag left set meets the background flush. With a disk that works the flush settles the
/// manifest first and nothing is recorded. With a disk that keeps failing after the rename the
/// settle of the flush fails as an uncertain write does in a flush: fatal, the database is
/// stopped and the error says why. That is the one way a direct failure can end in a stop.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_flag_left_set_meets_the_background_flush() {
	// A disk that works.
	{
		let images = TempDir::new("direct_manifest_flag_image").unwrap();
		let mut fx = unsettled(true, false, None).await;
		let tree = Arc::clone(&fx.tree);
		commit_small(&tree, &mut fx.model, 1, 3).await;
		flush_in_background(&tree);
		wait_until("the flush to settle the manifest and finish", || {
			immutables(&tree) == 0 && !tree.core.inner.manifest_uncertain()
		})
		.await;
		wait_until("the flush to end its error", || {
			tree.core.inner.error_handler.check_error().is_ok()
		})
		.await;
		assert_healthy(&tree, "after the background flush");
		assert_eq!(disk_tables(&tree).unwrap(), table_ids(&tree));
		assert_same(&all_keys(&tree), &fx.model, "after the background flush");
		let image = crash_image(&tree, images.path(), "flushed").await;
		assert_recovers(&image, &fx.model, &[fx.failed.clone()], "the image");
		tree.close().await.unwrap();
		let reopened = open_tree(fx.dir.path(), false, false).await;
		assert_no_orphans(&reopened, "after the reopen");
		assert_recovers(&all_keys(&reopened), &fx.model, &[fx.failed.clone()], "the reopened tree");
		abandon(&reopened);
	}

	// A disk that keeps failing after the rename.
	{
		let images = TempDir::new("direct_manifest_flag_stop_image").unwrap();
		let mut fx = unsettled(true, false, None).await;
		let tree = Arc::clone(&fx.tree);
		commit_small(&tree, &mut fx.model, 1, 3).await;
		fail_replacements(&fx.manifest, ReplaceFailure::AfterRename, 0, usize::MAX);
		flush_in_background(&tree);
		wait_until("the flush to fail", || tree.core.inner.error_handler.check_error().is_err())
			.await;
		let failure = tree.core.inner.error_handler.get_error().unwrap();
		assert_eq!(failure.reason, BackgroundErrorReason::MemtablaFlush);
		assert_eq!(failure.severity, ErrorSeverity::FatalError);
		assert!(matches!(failure.error, Error::ManifestWriteUncertain(_)), "{:?}", failure.error);
		assert!(!tree.core.inner.error_handler.recover(BackgroundErrorReason::MemtablaFlush));
		let refused = commit_value(&tree, b"later".to_vec(), value_of(5, 100)).await;
		assert!(refused.is_err(), "the stopped database refuses commits");

		heal_replacements(&fx.manifest);
		let image = crash_image(&tree, images.path(), "stopped").await;
		assert_recovers(&image, &fx.model, &[fx.failed.clone()], "the image after the stop");
		abandon(&tree);
	}
}

// ---------------------------------------------------------------------------
// the value log
// ---------------------------------------------------------------------------

/// With the value log in use, a table kept by a double failure, then a flush and the cleanup of
/// the value log it makes, leave a database that reopens and reads every value: the files of the
/// failed batch stay pinned by the memtable that holds its record until its flush has written
/// the manifest again.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_kept_table_and_a_later_flush_with_vlog_cleanup_leave_a_reopenable_database() {
	let images = TempDir::new("direct_manifest_vlog_image").unwrap();
	let mut fx = unsettled(false, true, None).await;
	let tree = Arc::clone(&fx.tree);
	let value = value_of(6, 2000);
	commit_value(&tree, key(2), value.clone()).await.unwrap();
	fx.model.insert(key(2), value);

	let image = crash_image(&tree, images.path(), "before").await;
	assert_recovers(&image, &fx.model, &[fx.failed.clone()], "the image before the flush");

	tree.core.inner.rotate_memtable().unwrap();
	tree.core.inner.flush_all_immutables_sync().unwrap();
	assert!(!tree.core.inner.manifest_uncertain(), "the flush settled the manifest");
	assert_eq!(disk_tables(&tree).unwrap(), table_ids(&tree));
	assert_same(&all_keys(&tree), &fx.model, "after the flush");

	let image = crash_image(&tree, images.path(), "after").await;
	assert_recovers(&image, &fx.model, &[fx.failed.clone()], "the image after the flush");
	tree.close().await.unwrap();
	let reopened = open_tree(fx.dir.path(), false, true).await;
	assert_no_orphans(&reopened, "after the reopen");
	assert_recovers(&all_keys(&reopened), &fx.model, &[fx.failed.clone()], "the reopened tree");
	abandon(&reopened);
}
