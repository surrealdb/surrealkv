//! A commit bigger than the memtable is written straight to an L0 table, and reads consult the
//! memtables before the tables. A read-only transaction that begins after such a commit was
//! acknowledged must read it, however the writes it replaced are held: in the memtable the commit
//! sealed, in a flush that was already running, or in a queue that nothing is flushing. The
//! checks cover point reads, forward and backward scans, snapshots from before and after the
//! commit, a transaction that reads its own writes over the base, and the tree after a crash
//! and after a reopen.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;

use super::{collect_transaction_all, collect_transaction_reverse};
use crate::lsm::FlushHook;
use crate::{Durability, Error, Mode, Options, Transaction, Tree};

const KIB: usize = 1024;

/// A memtable that a commit of a few hundred KiB does not fit.
const MEMTABLE: usize = 256 * KIB;

/// Keys a client of the randomised check owns.
const CLIENT_KEYS: usize = 6;

/// How long a commit or a wait may take before the test calls it hung.
const WATCHDOG: Duration = Duration::from_secs(60);

/// How long a test waits for a flush it expects to reach the gate.
const REACHED: Duration = Duration::from_secs(20);

/// How long a test gives a writer that is expected to wait for a flush to finish.
const GRACE: Duration = Duration::from_millis(300);

/// How long a test gives a commit that cannot finish while a flush is held.
const HELD: Duration = Duration::from_secs(1);

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// Keys and values in key order.
type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// Versioning needs the value log, with every value in it.
fn options(path: &Path, background: bool, versioning: bool) -> Arc<Options> {
	let mut opts = Options {
		path: path.to_path_buf(),
		max_memtable_size: MEMTABLE,
		enable_vlog: versioning,
		enable_versioning: versioning,
		..Default::default()
	};
	if !background {
		// Nothing flushes or compacts, so nothing may wait for it to.
		opts.memtable_stall_threshold = 10_000;
		opts.l0_stall_threshold = 10_000;
	}
	if versioning {
		opts.vlog_value_threshold = 0;
	}
	Arc::new(opts)
}

/// Stops the background flush and compaction, so a memtable that is queued stays queued until
/// something else flushes it.
async fn stop_background(tree: &Tree) {
	let tasks = tree.core.task_manager.lock().unwrap().take().unwrap();
	tasks.stop().await;
}

fn mark_closed(tree: &Tree) {
	tree.core.is_closed.store(true, Ordering::SeqCst);
}

fn release_lock(tree: &Tree) {
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
}

/// Copies the files of a live tree, as a crash would leave them.
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

/// A value that says which commit wrote it.
fn value_of(tag: u64, len: usize) -> Vec<u8> {
	let mut v = vec![tag as u8; len.max(16)];
	v[..8].copy_from_slice(&tag.to_le_bytes());
	v
}

fn describe(value: &Option<Vec<u8>>) -> String {
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

struct Rng(u64);

impl Rng {
	fn next(&mut self) -> u64 {
		self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
		let mut z = self.0;
		z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
		z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
		z ^ (z >> 31)
	}

	fn below(&mut self, n: u64) -> u64 {
		self.next() % n
	}
}

/// The tables the manifest lists.
fn table_count(tree: &Tree) -> usize {
	tree.core.inner.level_manifest.read().unwrap().get_all_tables().len()
}

/// Whether the active memtable holds nothing.
fn active_is_empty(tree: &Tree) -> bool {
	tree.core.inner.active_memtable.read().unwrap().is_empty()
}

/// What `txn` scans in `[lo, hi)`, forwards and backwards.
fn scan(txn: &Transaction, lo: &[u8], hi: &[u8]) -> (Entries, Entries) {
	let forward =
		collect_transaction_all(&mut txn.range(lo.to_vec(), hi.to_vec()).unwrap()).unwrap();
	let backward =
		collect_transaction_reverse(&mut txn.range(lo.to_vec(), hi.to_vec()).unwrap()).unwrap();
	(forward, backward)
}

/// What `model` holds in `[lo, hi)`, in key order.
fn within(model: &Model, lo: &[u8], hi: &[u8]) -> Entries {
	model.range(lo.to_vec()..hi.to_vec()).map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// Panics unless `got` is `want`, naming the key where they first differ.
fn assert_same(got: &[(Vec<u8>, Vec<u8>)], want: &[(Vec<u8>, Vec<u8>)], what: &str) {
	for (g, w) in got.iter().zip(want) {
		assert!(
			g == w,
			"{what}: at {}, read {}, expected {}",
			String::from_utf8_lossy(&w.0),
			describe(&Some(g.1.clone())),
			describe(&Some(w.1.clone()))
		);
	}
	assert_eq!(got.len(), want.len(), "{what}: number of entries");
}

/// Reads `keys` and scans `[lo, hi)` through `txn` and compares them with `model`.
fn assert_reads(
	txn: &Transaction,
	model: &Model,
	keys: &[Vec<u8>],
	lo: &[u8],
	hi: &[u8],
	what: &str,
) {
	for key in keys {
		let seen = txn.get(key).unwrap();
		assert!(
			seen.as_ref() == model.get(key),
			"{what}: {} read {}, expected {}",
			String::from_utf8_lossy(key),
			describe(&seen),
			describe(&model.get(key).cloned())
		);
	}
	let want = within(model, lo, hi);
	let (forward, backward) = scan(txn, lo, hi);
	assert_same(&forward, &want, &format!("{what}: forward scan"));
	let mut want_backward = want;
	want_backward.reverse();
	assert_same(&backward, &want_backward, &format!("{what}: backward scan"));
}

// ---------------------------------------------------------------------------
// deterministic: the writes a direct commit replaces are still in memory
// ---------------------------------------------------------------------------

const KEYS: [&[u8]; 4] = [b"key_0", b"key_1", b"key_2", b"key_3"];

#[derive(Clone, Copy, Debug)]
enum Replace {
	/// Sets the key to a new value.
	Set,
	/// Deletes the key.
	Delete,
	/// Deletes the key with a range tombstone.
	DeleteRange,
}

fn key_list() -> Vec<Vec<u8>> {
	KEYS.iter().map(|k| k.to_vec()).collect()
}

/// Commits `key` to a small value, which lands in the active memtable.
async fn commit_small(tree: &Tree, model: &mut Model, key: &[u8], tag: u64) {
	let value = value_of(tag, 100);
	let mut txn = tree.begin().unwrap();
	txn.set(key.to_vec(), value.clone()).unwrap();
	txn.commit().await.unwrap();
	model.insert(key.to_vec(), value);
}

/// A transaction that does not fit the memtable and replaces `key` as `replace` says, not yet
/// committed.
fn big_transaction(
	tree: &Tree,
	model: &mut Model,
	key: &[u8],
	replace: Replace,
	tag: u64,
) -> Transaction {
	let mut txn = tree.begin().unwrap();
	for i in 0..(MEMTABLE / KIB + 100) {
		let filler = format!("filler_{tag:05}_{i:05}").into_bytes();
		let value = value_of(tag, KIB);
		txn.set(filler.clone(), value.clone()).unwrap();
		model.insert(filler, value);
	}
	match replace {
		Replace::Set => {
			let value = value_of(tag, 100);
			txn.set(key.to_vec(), value.clone()).unwrap();
			model.insert(key.to_vec(), value);
		}
		Replace::Delete => {
			txn.delete(key.to_vec()).unwrap();
			model.remove(key);
		}
		Replace::DeleteRange => {
			let mut end = key.to_vec();
			end.push(0);
			txn.delete_range(key.to_vec(), end).unwrap();
			model.remove(key);
		}
	}
	txn
}

/// Commits a batch that does not fit the memtable and replaces `key` as `replace` says.
async fn commit_bigger_than_the_memtable(
	tree: &Tree,
	model: &mut Model,
	key: &[u8],
	replace: Replace,
	tag: u64,
) {
	big_transaction(tree, model, key, replace, tag).commit().await.unwrap();
}

/// Reads `model`'s keys through `get_async`, which has its own copy of the lookup.
async fn assert_async_reads(tree: &Tree, model: &Model, what: &str) {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	for key in KEYS {
		let seen = txn.get_async(key).await.unwrap();
		assert!(
			seen.as_ref() == model.get(key),
			"{what}: get_async({}) read {}, expected {}",
			String::from_utf8_lossy(key),
			describe(&seen),
			describe(&model.get(key).cloned())
		);
	}
}

/// Alternates small commits and commits bigger than the memtable over the same keys with the
/// background flush stopped, and reads everything back through a fresh transaction after each.
/// Then reads it from a crash image and after a reopen.
async fn direct_commits_over_queued_writes(replace: Replace, versioning: bool) {
	let dir = TempDir::new("direct_l0_order").unwrap();
	let live = dir.path().join("live");
	let tree = Tree::new(options(&live, false, versioning)).unwrap();
	stop_background(&tree).await;
	let keys = key_list();
	let mut model = Model::new();
	let mut tag = 0;

	for (round, replaced) in KEYS.iter().enumerate() {
		// The memtable holds an older write of every key.
		for key in &keys {
			tag += 1;
			commit_small(&tree, &mut model, key, tag).await;
		}
		let before = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		let before_model = model.clone();
		let tables = table_count(&tree);

		tag += 1;
		commit_bigger_than_the_memtable(&tree, &mut model, replaced, replace, tag).await;
		assert!(active_is_empty(&tree), "control: the commit did not go to the memtable");
		assert!(table_count(&tree) > tables, "control: the commit went to a table");

		let after = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		let what = format!("{replace:?} round {round}");
		assert_reads(&after, &model, &keys, b"", b"\xff", &format!("{what}, after"));
		// A snapshot taken before the commit does not see it, however the sources are ordered.
		assert_reads(&before, &before_model, &keys, b"", b"\xff", &format!("{what}, before"));
	}

	// A write after the last direct commit shadows it, and a direct commit after that one
	// shadows the write.
	tag += 1;
	commit_small(&tree, &mut model, KEYS[0], tag).await;
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	assert_reads(&txn, &model, &keys, b"", b"\xff", "a small write after a direct commit");
	drop(txn);

	let image = dir.path().join("image");
	copy_dir(&live, &image);
	let recovered = Tree::new(options(&image, true, versioning)).unwrap();
	{
		let txn = recovered.begin_with_mode(Mode::ReadOnly).unwrap();
		assert_reads(&txn, &model, &keys, b"", b"\xff", "crash image");
	}
	mark_closed(&recovered);
	release_lock(&recovered);

	tree.close().await.unwrap();
	let reopened = Tree::new(options(&live, true, versioning)).unwrap();
	{
		let txn = reopened.begin_with_mode(Mode::ReadOnly).unwrap();
		assert_reads(&txn, &model, &keys, b"", b"\xff", "reopened");
	}
	reopened.close().await.unwrap();
}

#[test(tokio::test)]
async fn a_direct_commit_replacing_a_value_the_sealed_memtable_holds_is_read_back() {
	direct_commits_over_queued_writes(Replace::Set, false).await;
}

#[test(tokio::test)]
async fn a_direct_commit_deleting_a_key_the_sealed_memtable_holds_is_read_back() {
	direct_commits_over_queued_writes(Replace::Delete, false).await;
}

#[test(tokio::test)]
async fn a_direct_range_delete_over_a_key_the_sealed_memtable_holds_is_read_back() {
	direct_commits_over_queued_writes(Replace::DeleteRange, false).await;
}

#[test(tokio::test)]
async fn a_direct_commit_with_a_value_log_and_versioning_is_read_back() {
	direct_commits_over_queued_writes(Replace::Set, true).await;
}

/// Several memtables are queued, the newest of them sealed by the direct commit: every one of
/// them is behind the table in sequence number, and the commit has to leave none of them in
/// front of it.
#[test(tokio::test)]
async fn a_direct_commit_behind_several_queued_memtables_is_read_back() {
	let dir = TempDir::new("direct_l0_order_queue").unwrap();
	let tree = Tree::new(options(dir.path(), false, false)).unwrap();
	stop_background(&tree).await;
	let keys = key_list();
	let mut model = Model::new();
	let mut tag = 0;

	for (round, replaced) in KEYS.iter().take(3).enumerate() {
		for key in &keys {
			tag += 1;
			commit_small(&tree, &mut model, key, tag).await;
			tree.core.inner.rotate_memtable().unwrap();
		}
		tag += 1;
		commit_bigger_than_the_memtable(&tree, &mut model, replaced, Replace::Set, tag).await;
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		assert_reads(&txn, &model, &keys, b"", b"\xff", &format!("round {round}"));
	}
	tree.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// deterministic: a flush is already running when the commit arrives
// ---------------------------------------------------------------------------

/// Holds every flush of an immutable memtable after its table is written and before the manifest
/// lists it, so the memtable is still queued for as long as the gate is shut.
struct Gate {
	reached: AtomicBool,
	open: AtomicBool,
}

impl Gate {
	fn install(tree: &Tree) -> Arc<Self> {
		let gate = Arc::new(Gate {
			reached: AtomicBool::new(false),
			open: AtomicBool::new(false),
		});
		let held = Arc::clone(&gate);
		let hook: FlushHook = Arc::new(move |_| {
			held.reached.store(true, Ordering::SeqCst);
			let deadline = Instant::now() + WATCHDOG;
			while !held.open.load(Ordering::SeqCst) {
				assert!(Instant::now() < deadline, "the gate was never opened");
				std::thread::sleep(Duration::from_millis(1));
			}
			Ok(())
		});
		*tree.core.inner.flush_hook.lock() = Some(hook);
		gate
	}

	async fn until_reached(&self) {
		let deadline = Instant::now() + REACHED;
		while !self.reached.load(Ordering::SeqCst) {
			assert!(Instant::now() < deadline, "no flush reached the gate");
			tokio::time::sleep(Duration::from_millis(1)).await;
		}
	}

	fn open(&self) {
		self.open.store(true, Ordering::SeqCst);
	}
}

/// Opens the gate when dropped, so a test that fails does not leave a flush held.
struct OpenOnDrop(Arc<Gate>);

impl Drop for OpenOnDrop {
	fn drop(&mut self) {
		self.0.open();
	}
}

/// The background flush of an older memtable is held when a commit bigger than the memtable
/// arrives. The commit flushes the queue before it writes its table, so it is not acknowledged
/// while that flush is held, and a read in the meantime sees the last acknowledged state. Once the
/// flush ends the commit is acknowledged, and a fresh read-only transaction reads it.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_direct_commit_waits_for_a_flush_that_is_held_and_is_then_read_back() {
	let dir = TempDir::new("direct_l0_order_held").unwrap();
	let tree = Arc::new(Tree::new(options(dir.path(), true, false)).unwrap());
	let keys = key_list();
	let mut model = Model::new();

	commit_small(&tree, &mut model, KEYS[0], 1).await;
	let gate = Gate::install(&tree);
	let _open = OpenOnDrop(Arc::clone(&gate));
	tree.core.inner.rotate_memtable().unwrap();
	tree.core.task_manager.lock().unwrap().as_ref().unwrap().wake_up_memtable();
	gate.until_reached().await;

	// The second write of the key is in the active memtable, which the direct commit seals.
	commit_small(&tree, &mut model, KEYS[0], 2).await;
	let acknowledged = model.clone();
	let mut next = model.clone();
	let mut commit = {
		let tree = Arc::clone(&tree);
		tokio::spawn(async move {
			commit_bigger_than_the_memtable(&tree, &mut next, KEYS[0], Replace::Set, 3).await;
			next
		})
	};
	assert!(
		tokio::time::timeout(HELD, &mut commit).await.is_err(),
		"the commit was acknowledged while the flush of an older memtable was held"
	);
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	assert_reads(&txn, &acknowledged, &keys, b"", b"\xff", "while the flush is held");
	drop(txn);

	gate.open();
	let expected = tokio::time::timeout(WATCHDOG, commit).await.expect("the commit hung").unwrap();
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	assert_reads(&txn, &expected, &keys, b"", b"\xff", "after the flush");
	drop(txn);

	*tree.core.inner.flush_hook.lock() = None;
	tree.close().await.unwrap();
}

/// A crash while the commit flushes the memtable it sealed, before its own table exists, leaves
/// the commit in the WAL behind the writes it replaced. Its record is in the file the image copies,
/// and replay applies it in log order, so the image holds the commit and never the value it
/// replaced.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_crash_while_a_direct_commit_flushes_the_memtable_it_sealed_recovers_the_commit() {
	let dir = TempDir::new("direct_l0_order_crash").unwrap();
	let live = dir.path().join("live");
	let image = dir.path().join("image");
	let tree = Tree::new(options(&live, false, false)).unwrap();
	stop_background(&tree).await;
	let keys = key_list();
	let mut model = Model::new();
	commit_small(&tree, &mut model, KEYS[0], 1).await;
	commit_small(&tree, &mut model, KEYS[1], 2).await;

	// The first flush that reaches its hook takes the image: its table is written and the
	// manifest does not list it.
	let taken = Arc::new(AtomicBool::new(false));
	{
		let (taken, live, image) = (Arc::clone(&taken), live.clone(), image.clone());
		let hook: FlushHook = Arc::new(move |_| {
			if !taken.swap(true, Ordering::SeqCst) {
				copy_dir(&live, &image);
			}
			Ok(())
		});
		*tree.core.inner.flush_hook.lock() = Some(hook);
	}
	commit_bigger_than_the_memtable(&tree, &mut model, KEYS[0], Replace::Set, 3).await;
	assert!(taken.load(Ordering::SeqCst), "control: the commit flushed the memtable it sealed");

	let recovered = Tree::new(options(&image, true, false)).unwrap();
	{
		let txn = recovered.begin_with_mode(Mode::ReadOnly).unwrap();
		let seen: Model =
			collect_transaction_all(&mut txn.iter().unwrap()).unwrap().into_iter().collect();
		assert!(
			seen == model,
			"the image holds {} keys, expected the {} of the state after the commit; key_0 reads {}",
			seen.len(),
			model.len(),
			describe(&seen.get(KEYS[0]).cloned())
		);
	}
	mark_closed(&recovered);
	release_lock(&recovered);

	*tree.core.inner.flush_hook.lock() = None;
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	assert_reads(&txn, &model, &keys, b"", b"\xff", "live tree");
	drop(txn);
	tree.close().await.unwrap();
}

/// A direct commit seals the active memtable and flushes the queue on the flusher, which the
/// background task does not see: a writer stalled on the length of the queue is woken when the
/// queue shrinks, not left for the next flush or compaction to do it.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_writer_stalled_on_the_memtable_queue_is_woken_by_the_flush_of_a_direct_commit() {
	let dir = TempDir::new("direct_l0_order_stall").unwrap();
	let mut opts = (*options(dir.path(), true, false)).clone();
	opts.memtable_stall_threshold = 2;
	let tree = Arc::new(Tree::new(Arc::new(opts)).unwrap());
	stop_background(&tree).await;
	let mut model = Model::new();

	// One memtable is queued, below the threshold, and the active one holds a write.
	commit_small(&tree, &mut model, KEYS[0], 1).await;
	tree.core.inner.rotate_memtable().unwrap();
	commit_small(&tree, &mut model, KEYS[0], 2).await;
	assert_eq!(tree.core.inner.immutable_count(), 1);

	// The direct commit seals the active memtable, which takes the queue to the threshold, and
	// then flushes it with nothing else to do it: the flush is held on the first memtable.
	let gate = Gate::install(&tree);
	let _open = OpenOnDrop(Arc::clone(&gate));
	let commit = {
		let tree = Arc::clone(&tree);
		tokio::spawn(async move {
			let mut model = Model::new();
			commit_bigger_than_the_memtable(&tree, &mut model, KEYS[0], Replace::Set, 3).await;
		})
	};
	gate.until_reached().await;
	assert_eq!(tree.core.inner.immutable_count(), 2);

	let waiter = {
		let tree = Arc::clone(&tree);
		tokio::spawn(async move { tree.core.write_stall.check().await })
	};
	tokio::time::sleep(GRACE).await;
	assert!(!waiter.is_finished(), "control: a writer waits while the queue is at the threshold");

	gate.open();
	tokio::time::timeout(WATCHDOG, commit).await.expect("the commit hung").unwrap();
	let stalled = tokio::time::timeout(Duration::from_secs(10), waiter)
		.await
		.expect("the writer was not woken when the queue was flushed")
		.unwrap()
		.unwrap();
	assert!(stalled.is_some(), "the writer was stalled and then released");
	assert_eq!(tree.core.inner.immutable_count(), 0);

	*tree.core.inner.flush_hook.lock() = None;
	tree.close().await.unwrap();
}

/// The asynchronous point read goes through its own copy of the lookup, which has to put a direct
/// commit in front of the writes it replaced as well.
#[test(tokio::test)]
async fn get_async_reads_a_direct_commit_in_front_of_the_memtable_it_sealed() {
	for replace in [Replace::Set, Replace::Delete, Replace::DeleteRange] {
		let dir = TempDir::new("direct_l0_order_async").unwrap();
		let tree = Tree::new(options(dir.path(), false, false)).unwrap();
		stop_background(&tree).await;
		let mut model = Model::new();
		for (i, key) in KEYS.iter().enumerate() {
			commit_small(&tree, &mut model, key, i as u64 + 1).await;
		}
		commit_bigger_than_the_memtable(&tree, &mut model, KEYS[1], replace, 9).await;
		assert_async_reads(&tree, &model, &format!("{replace:?}")).await;
		tree.close().await.unwrap();
	}
}

/// A flush of the sealed memtable that fails is a failed commit: the commit's table is not
/// written in front of the memtable that is still queued, whatever a read that begins after the
/// commit returned sees is consistent with its outcome, and a later commit over the same keys,
/// once the flush works again, reads back, leaves nothing queued, and does not make the failed
/// commit visible.
#[test(tokio::test)]
async fn a_commit_whose_flush_of_the_sealed_memtable_fails_is_not_read_in_front_of_it() {
	let dir = TempDir::new("direct_l0_order_flush_fails").unwrap();
	let tree = Tree::new(options(dir.path(), false, false)).unwrap();
	stop_background(&tree).await;
	let mut model = Model::new();
	for (i, key) in KEYS.iter().enumerate() {
		commit_small(&tree, &mut model, key, i as u64 + 1).await;
	}
	let before = model.clone();

	let failed = Arc::new(AtomicUsize::new(0));
	{
		let failed = Arc::clone(&failed);
		let hook: FlushHook = Arc::new(move |_| {
			if failed.fetch_add(1, Ordering::SeqCst) == 0 {
				return Err(Error::Other("injected flush failure".into()));
			}
			Ok(())
		});
		*tree.core.inner.flush_hook.lock() = Some(hook);
	}
	let mut after = model.clone();
	let mut txn = big_transaction(&tree, &mut after, KEYS[0], Replace::Set, 5);
	let outcome = tokio::time::timeout(WATCHDOG, txn.commit()).await.expect("the commit hung");
	assert!(failed.load(Ordering::SeqCst) >= 1, "control: the commit flushed what it sealed");
	assert!(outcome.is_err(), "a commit that could not flush what it sealed was acknowledged");

	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	assert_reads(&txn, &before, &key_list(), b"", b"\xff", "after the failed commit");
	drop(txn);

	// The flush works again: a commit over the same keys goes through and is read back.
	*tree.core.inner.flush_hook.lock() = None;
	let mut next = before.clone();
	let mut txn = big_transaction(&tree, &mut next, KEYS[0], Replace::Set, 6);
	tokio::time::timeout(WATCHDOG, txn.commit()).await.expect("the commit hung").unwrap();
	assert_eq!(tree.core.inner.immutable_count(), 0, "a memtable is queued in front of the table");
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	assert_reads(&txn, &next, &key_list(), b"", b"\xff", "after the flush works again");
	drop(txn);
	assert_async_reads(&tree, &next, "after the flush works again").await;
	tree.close().await.unwrap();
}

/// The level-0 tables are consulted newest first, and a direct commit goes in front of the table
/// of the memtable it sealed: their sequence number ranges follow each other without a gap or an
/// overlap, and a compaction of them keeps the newest value of every key.
#[test(tokio::test)]
async fn level_zero_puts_a_direct_table_in_front_of_the_one_it_sealed_and_compacts_them() {
	let dir = TempDir::new("direct_l0_order_level_zero").unwrap();
	let mut opts = (*options(dir.path(), false, false)).clone();
	opts.level0_max_files = 2;
	let opts = Arc::new(opts);
	let tree = Tree::new(Arc::clone(&opts)).unwrap();
	stop_background(&tree).await;
	let keys = key_list();
	let mut model = Model::new();
	let mut tag = 0;

	let ranges = |tree: &Tree| -> Vec<(u64, u64)> {
		let manifest = tree.core.inner.level_manifest.read().unwrap();
		manifest.levels.get_levels()[0]
			.tables
			.iter()
			.map(|t| (t.meta.properties.seqnos.0, t.meta.properties.seqnos.1))
			.collect()
	};
	for (round, replaced) in KEYS.iter().enumerate() {
		for key in &keys {
			tag += 1;
			commit_small(&tree, &mut model, key, tag).await;
		}
		tag += 1;
		commit_bigger_than_the_memtable(&tree, &mut model, replaced, Replace::Set, tag).await;

		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		assert_reads(&txn, &model, &keys, b"", b"\xff", &format!("round {round}"));
		drop(txn);
		let tables = ranges(&tree);
		assert_eq!(
			tables.len(),
			2 * (round + 1),
			"a table for the memtable and one for the commit"
		);
		for pair in tables.windows(2) {
			assert!(
				pair[0].0 > pair[1].1,
				"round {round}: tables {pair:?} overlap in sequence numbers or are out of order"
			);
		}
	}

	tree.compact(Arc::new(crate::compaction::leveled::Strategy::from_options(opts))).unwrap();
	assert!(ranges(&tree).len() < 8, "control: the compaction merged level-0 tables");
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	assert_reads(&txn, &model, &keys, b"", b"\xff", "after the compaction");
	drop(txn);

	// A commit after the compaction goes in front of the merged table.
	tag += 1;
	commit_small(&tree, &mut model, KEYS[2], tag).await;
	tag += 1;
	commit_bigger_than_the_memtable(&tree, &mut model, KEYS[2], Replace::Set, tag).await;
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	assert_reads(&txn, &model, &keys, b"", b"\xff", "a commit after the compaction");
	drop(txn);
	tree.close().await.unwrap();
}

// ---------------------------------------------------------------------------
// randomised: acknowledged commits against a model
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Cfg {
	seed: u64,
	clients: usize,
	rounds: usize,
	/// Whether the background flush and compaction run.
	background: bool,
	/// How long the clients may run.
	budget: Duration,
}

enum Op {
	Set(Vec<u8>, Vec<u8>),
	Delete(Vec<u8>),
	DeleteRange(Vec<u8>, Vec<u8>),
}

fn client_key(prefix: &[u8], index: usize) -> Vec<u8> {
	let mut key = prefix.to_vec();
	key.extend_from_slice(format!("k{index}").as_bytes());
	key
}

/// The writes of one commit of a client: sets and deletes of distinct keys, or a range delete and
/// sets and deletes of keys outside it. A big commit holds values that do not fit the memtable.
fn plan(rng: &mut Rng, prefix: &[u8], tag: u64, big: bool) -> Vec<Op> {
	let mut free: Vec<usize> = (0..CLIENT_KEYS).collect();
	let mut ops = Vec::new();
	if rng.below(6) == 0 {
		let a = rng.below(CLIENT_KEYS as u64) as usize;
		let b = a + 1 + rng.below((CLIENT_KEYS - a) as u64) as usize;
		let end = if b == CLIENT_KEYS {
			let mut end = prefix.to_vec();
			end.push(0xff);
			end
		} else {
			client_key(prefix, b)
		};
		ops.push(Op::DeleteRange(client_key(prefix, a), end));
		free.retain(|k| *k < a || *k >= b);
	}
	let writes = if big {
		1 + rng.below(2)
	} else {
		1 + rng.below(3)
	};
	for _ in 0..writes {
		if free.is_empty() {
			break;
		}
		let key = client_key(prefix, free.swap_remove(rng.below(free.len() as u64) as usize));
		let len = if big {
			MEMTABLE + rng.below(MEMTABLE as u64) as usize
		} else {
			16 + rng.below(2000) as usize
		};
		if !big && rng.below(6) == 0 {
			ops.push(Op::Delete(key));
		} else {
			ops.push(Op::Set(key, value_of(tag, len)));
		}
	}
	ops
}

fn apply(model: &mut Model, op: &Op) {
	match op {
		Op::Set(key, value) => {
			model.insert(key.clone(), value.clone());
		}
		Op::Delete(key) => {
			model.remove(key);
		}
		Op::DeleteRange(start, end) => {
			let doomed: Vec<_> =
				model.range(start.clone()..end.clone()).map(|(k, _)| k.clone()).collect();
			for key in doomed {
				model.remove(&key);
			}
		}
	}
}

/// One client: commits to the keys under its own prefix and, after each acknowledgement, reads
/// them back through a fresh read-only transaction, a snapshot taken just before the commit and
/// the transaction that wrote them. Returns what its keys hold at the end.
async fn client(tree: Arc<Tree>, id: usize, cfg: Cfg, deadline: Instant) -> Model {
	let mut rng = Rng(cfg.seed ^ (id as u64 + 1).wrapping_mul(0xD1B5_4A32_D192_ED03));
	let prefix = format!("c{id:02}/").into_bytes();
	let mut hi = prefix.clone();
	hi.push(0xff);
	let keys: Vec<Vec<u8>> = (0..CLIENT_KEYS).map(|i| client_key(&prefix, i)).collect();
	let mut model = Model::new();

	for round in 0..cfg.rounds {
		if Instant::now() > deadline {
			break;
		}
		let tag = (id * 1000 + round + 1) as u64;
		let big = rng.below(10) < 4;
		let ops = plan(&mut rng, &prefix, tag, big);
		let range_delete = ops.iter().any(|op| matches!(op, Op::DeleteRange(..)));

		// A compaction drops the versions a range tombstone covers without asking whether a
		// snapshot can still see them, so a snapshot is only checked across a range delete
		// while nothing compacts.
		let snapshot = (rng.below(3) == 0 && (!range_delete || !cfg.background))
			.then(|| (tree.begin_with_mode(Mode::ReadOnly).unwrap(), model.clone()));
		let mut next = model.clone();
		let mut txn = tree.begin().unwrap();
		txn.set_durability(if rng.below(3) == 0 {
			Durability::Eventual
		} else {
			Durability::Immediate
		});
		for op in &ops {
			match op {
				Op::Set(key, value) => txn.set(key.clone(), value.clone()).unwrap(),
				Op::Delete(key) => txn.delete(key.clone()).unwrap(),
				Op::DeleteRange(start, end) => {
					txn.delete_range(start.clone(), end.clone()).unwrap()
				}
			}
			apply(&mut next, op);
		}
		if !range_delete {
			// The transaction reads what it wrote over the base.
			for key in &keys {
				let seen = txn.get(key).unwrap();
				assert!(
					seen.as_ref() == next.get(key),
					"client {id} round {round}: before the commit {} read {}, expected {}",
					String::from_utf8_lossy(key),
					describe(&seen),
					describe(&next.get(key).cloned())
				);
			}
			let forward =
				collect_transaction_all(&mut txn.range(prefix.clone(), hi.clone()).unwrap())
					.unwrap();
			assert_same(
				&forward,
				&within(&next, &prefix, &hi),
				&format!("client {id} round {round}: scan before the commit"),
			);
		}
		tokio::time::timeout(WATCHDOG, txn.commit())
			.await
			.expect("a commit hung")
			.unwrap_or_else(|e| panic!("client {id} round {round}: commit failed: {e:?}"));
		model = next;

		let fresh = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		assert_reads(
			&fresh,
			&model,
			&keys,
			&prefix,
			&hi,
			&format!(
				"client {id} round {round} ({}), fresh",
				if big {
					"big"
				} else {
					"small"
				}
			),
		);
		if let Some((snapshot, snapshot_model)) = snapshot {
			assert_reads(
				&snapshot,
				&snapshot_model,
				&keys,
				&prefix,
				&hi,
				&format!("client {id} round {round}, snapshot from before the commit"),
			);
		}
	}
	model
}

/// Runs clients that commit in groups together, some of the commits bigger than the memtable,
/// and checks everything they acknowledged against a model while it runs, in the live tree at the
/// end, in a crash image and after a reopen.
async fn model_check(cfg: Cfg) {
	let dir = TempDir::new("direct_l0_order_model").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(options(&live, cfg.background, false)).unwrap());
	if !cfg.background {
		stop_background(&tree).await;
	}
	let deadline = Instant::now() + cfg.budget;
	let handles: Vec<_> = (0..cfg.clients)
		.map(|id| tokio::spawn(client(Arc::clone(&tree), id, cfg, deadline)))
		.collect();
	let mut model = Model::new();
	for handle in handles {
		let part = tokio::time::timeout(WATCHDOG, handle).await.expect("a client hung").unwrap();
		model.extend(part);
	}

	let check_all = |tree: &Tree, what: &str| {
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		assert_reads(&txn, &model, &[], b"", b"\xff", what);
	};
	check_all(&tree, "live");

	if cfg.background {
		// Nothing deletes a segment or a table under the copy.
		stop_background(&tree).await;
	}
	let image = dir.path().join("image");
	copy_dir(&live, &image);
	let recovered = Tree::new(options(&image, true, false)).unwrap();
	check_all(&recovered, "crash image");
	mark_closed(&recovered);
	release_lock(&recovered);

	tree.close().await.unwrap();
	let reopened = Tree::new(options(&live, true, false)).unwrap();
	check_all(&reopened, "reopened");
	reopened.close().await.unwrap();
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_randomised_workload_reads_back_every_acknowledged_commit_with_the_background_flush_running(
) {
	for seed in 1..=2 {
		model_check(Cfg {
			seed,
			clients: 4,
			rounds: 12,
			background: true,
			budget: Duration::from_secs(40),
		})
		.await;
	}
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_randomised_workload_reads_back_every_acknowledged_commit_with_the_background_flush_stopped(
) {
	for seed in 11..=12 {
		model_check(Cfg {
			seed,
			clients: 4,
			rounds: 12,
			background: false,
			budget: Duration::from_secs(40),
		})
		.await;
	}
}
