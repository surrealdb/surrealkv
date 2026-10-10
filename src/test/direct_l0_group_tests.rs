//! Groups of commits with a commit bigger than the memtable among them, a flush of the
//! memtables it seals that fails, a crash or a close or a checkpoint while it flushes, and a
//! direct table that cannot be written. Whatever the shape, every read after the group returns
//! the newest write of every key, and a crash image taken in the middle of a group recovers a
//! prefix of it. The same groups run on a tree recovered from a crash image, whose memtable holds
//! rows of the crashed segments, with queued memtables behind it: every acknowledged commit
//! survives two more crashes, and a group that stopped the database is recovered whole.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tempdir::TempDir;
use test_log::test;

use super::{collect_transaction_all, collect_transaction_reverse};
use crate::batch::Batch;
use crate::lsm::FlushHook;
use crate::memtable::{max_entry_bytes, MemTable};
use crate::ring::PipelineHook;
use crate::{Durability, Error, InternalKeyKind, Mode, Options, Transaction, Tree};

const KIB: usize = 1024;

/// A memtable that a value of a hundred KiB nearly fills.
const MEMTABLE: usize = 128 * KIB;

const WATCHDOG: Duration = Duration::from_secs(90);

type Model = BTreeMap<Vec<u8>, Vec<u8>>;
type Entries = Vec<(Vec<u8>, Vec<u8>)>;

fn options(path: &Path, background: bool) -> Arc<Options> {
	let mut opts = Options {
		path: path.to_path_buf(),
		max_memtable_size: MEMTABLE,
		level0_max_files: 2,
		..Default::default()
	};
	if !background {
		opts.memtable_stall_threshold = 10_000;
		opts.l0_stall_threshold = 10_000;
	}
	Arc::new(opts)
}

async fn stop_background(tree: &Tree) {
	let tasks = tree.core.task_manager.lock().unwrap().take().unwrap();
	tasks.stop().await;
}

/// Lets a tree that is dropped (or that a test is done with) leave its directory as a crash does.
fn abandon(tree: &Tree) {
	tree.core.is_closed.store(true, Ordering::SeqCst);
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
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

/// A value that says which commit wrote it.
fn value_of(tag: u64, len: usize) -> Vec<u8> {
	let mut v = vec![tag as u8; len.max(16)];
	v[..8].copy_from_slice(&tag.to_le_bytes());
	v
}

fn tag_of(value: &[u8]) -> u64 {
	u64::from_le_bytes(value[..8].try_into().unwrap())
}

fn describe(value: &Option<Vec<u8>>) -> String {
	match value {
		None => "nothing".to_string(),
		Some(v) if v.len() >= 16 => format!("commit {} of {} bytes", tag_of(v), v.len()),
		Some(v) => format!("{} bytes", v.len()),
	}
}
fn scan(txn: &Transaction, lo: &[u8], hi: &[u8]) -> (Entries, Entries) {
	let forward =
		collect_transaction_all(&mut txn.range(lo.to_vec(), hi.to_vec()).unwrap()).unwrap();
	let backward =
		collect_transaction_reverse(&mut txn.range(lo.to_vec(), hi.to_vec()).unwrap()).unwrap();
	(forward, backward)
}

fn within(model: &Model, lo: &[u8], hi: &[u8]) -> Entries {
	model.range(lo.to_vec()..hi.to_vec()).map(|(k, v)| (k.clone(), v.clone())).collect()
}

fn assert_same(got: &[(Vec<u8>, Vec<u8>)], want: &[(Vec<u8>, Vec<u8>)], what: &str) {
	for (g, w) in got.iter().zip(want) {
		assert!(
			g == w,
			"{what}: at {} read {} (key {}), expected {}",
			String::from_utf8_lossy(&w.0),
			describe(&Some(g.1.clone())),
			String::from_utf8_lossy(&g.0),
			describe(&Some(w.1.clone()))
		);
	}
	assert_eq!(got.len(), want.len(), "{what}: number of entries");
}

/// Reads `keys` one by one and scans `[lo, hi)` forwards and backwards, whole and over a middle
/// slice, through `txn`.
fn assert_reads(txn: &Transaction, model: &Model, keys: &[Vec<u8>], what: &str) {
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
	for (lo, hi) in [(&b""[..], &b"\xff"[..]), (&b"key_1"[..], &b"key_4"[..])] {
		let want = within(model, lo, hi);
		let (forward, backward) = scan(txn, lo, hi);
		assert_same(&forward, &want, &format!("{what}: forward scan"));
		let mut want_backward = want;
		want_backward.reverse();
		assert_same(&backward, &want_backward, &format!("{what}: backward scan"));
	}
}

const NKEYS: usize = 6;

fn key(i: usize) -> Vec<u8> {
	format!("key_{i}").into_bytes()
}

fn keys() -> Vec<Vec<u8>> {
	(0..NKEYS).map(key).collect()
}

#[derive(Clone, Debug)]
enum Op {
	Set(Vec<u8>, Vec<u8>),
	Delete(Vec<u8>),
	DeleteRange(Vec<u8>, Vec<u8>),
}

fn small(i: usize, tag: u64) -> Op {
	Op::Set(key(i), value_of(tag, 100))
}

fn big(i: usize, tag: u64) -> Op {
	Op::Set(key(i), value_of(tag, MEMTABLE + 1000))
}

fn batch_of(ops: &[Op], start: u64) -> Batch {
	let mut batch = Batch::new(start);
	for (i, op) in ops.iter().enumerate() {
		let ts = start + i as u64;
		match op {
			Op::Set(k, v) => batch.add_record(InternalKeyKind::Set, k.clone(), Some(v.clone()), ts),
			Op::Delete(k) => batch.add_record(InternalKeyKind::Delete, k.clone(), None, ts),
			Op::DeleteRange(a, b) => {
				batch.add_record(InternalKeyKind::RangeDelete, a.clone(), Some(b.clone()), ts)
			}
		}
		.unwrap();
	}
	batch
}

fn apply(model: &mut Model, ops: &[Op]) {
	for op in ops {
		match op {
			Op::Set(k, v) => {
				model.insert(k.clone(), v.clone());
			}
			Op::Delete(k) => {
				model.remove(k);
			}
			Op::DeleteRange(a, b) => {
				let doomed: Vec<_> =
					model.range(a.clone()..b.clone()).map(|(k, _)| k.clone()).collect();
				for k in doomed {
					model.remove(&k);
				}
			}
		}
	}
}

/// The batches as `flush_entries` hands them to `flush_group`: numbered on from the next
/// sequence number.
fn stamped(tree: &Tree, group: &[Vec<Op>]) -> Vec<Batch> {
	let pipeline = &tree.core.commit_pipeline;
	group
		.iter()
		.map(|ops| {
			let start = pipeline.log_seq_num.fetch_add(ops.len() as u64, Ordering::SeqCst);
			batch_of(ops, start)
		})
		.collect()
}

/// `flush_group` leaves publishing to the flusher.
fn publish_all(tree: &Tree) {
	let pipeline = &tree.core.commit_pipeline;
	tree.core
		.inner
		.visible_seq_num
		.store(pipeline.log_seq_num.load(Ordering::SeqCst) - 1, Ordering::SeqCst);
}

fn read_only(tree: &Tree) -> Transaction {
	tree.begin_with_mode(Mode::ReadOnly).unwrap()
}

fn all_keys(tree: &Tree) -> Model {
	let txn = read_only(tree);
	let all = collect_transaction_all(&mut txn.iter().unwrap()).unwrap();
	all.into_iter().collect()
}

/// Seeds the memtable with one write of every key, so the group replaces something that is in
/// memory.
async fn seed(tree: &Tree, model: &mut Model, tag: u64) {
	let mut txn = tree.begin().unwrap();
	for i in 0..NKEYS {
		let v = value_of(tag, 100);
		txn.set(key(i), v.clone()).unwrap();
		model.insert(key(i), v);
	}
	txn.commit().await.unwrap();
}

/// Hands `group` to `flush_group` as one group and checks every key, the scans, a snapshot from
/// before the group and, for the other options, a crash image and a reopen.
async fn check_group(group: Vec<Vec<Op>>, background: bool, what: &str) {
	let dir = TempDir::new("adv_l0_group").unwrap();
	let live = dir.path().join("live");
	let tree = Tree::new(options(&live, background)).unwrap();
	if !background {
		stop_background(&tree).await;
	}
	let mut model = Model::new();
	seed(&tree, &mut model, 1).await;
	let before = read_only(&tree);
	let before_model = model.clone();

	let batches = stamped(&tree, &group);
	tree.core.commit_pipeline.flush_group(&batches, true).await.unwrap();
	publish_all(&tree);
	for ops in &group {
		apply(&mut model, ops);
	}

	let after = read_only(&tree);
	assert_reads(&after, &model, &keys(), &format!("{what}: after"));
	let ranged = group.iter().flatten().any(|op| matches!(op, Op::DeleteRange(..)));
	if !(background && ranged) {
		assert_reads(&before, &before_model, &keys(), &format!("{what}: snapshot before"));
	}
	drop(before);
	drop(after);

	if background {
		// Nothing deletes a segment or a table under the copy.
		stop_background(&tree).await;
	}
	let image = dir.path().join("image");
	copy_dir(&live, &image);
	let recovered = Tree::new(options(&image, true)).unwrap();
	assert_reads(&read_only(&recovered), &model, &keys(), &format!("{what}: crash image"));
	abandon(&recovered);

	tree.close().await.unwrap();
	let reopened = Tree::new(options(&live, true)).unwrap();
	assert_reads(&read_only(&reopened), &model, &keys(), &format!("{what}: reopened"));
	reopened.close().await.unwrap();
}

/// Every position of the direct batch in a group of three, over the keys that are in memory.
fn shapes() -> Vec<(&'static str, Vec<Vec<Op>>)> {
	vec![
		("direct first", vec![vec![big(0, 2)], vec![small(0, 3)], vec![small(1, 4)]]),
		("direct middle", vec![vec![small(0, 2)], vec![big(0, 3)], vec![small(0, 4)]]),
		("direct last", vec![vec![small(0, 2)], vec![small(1, 3)], vec![big(0, 4)]]),
		("direct alone", vec![vec![big(0, 2)]]),
		(
			"two direct batches over one key",
			vec![vec![big(0, 2)], vec![small(0, 3)], vec![big(0, 4)], vec![small(1, 5)]],
		),
		(
			"direct delete in the middle",
			vec![vec![small(0, 2)], vec![Op::Delete(key(0)), big(1, 3)], vec![small(2, 4)]],
		),
		(
			"direct range delete in the middle",
			vec![
				vec![small(0, 2), small(1, 2)],
				vec![Op::DeleteRange(key(0), key(2)), big(3, 3)],
				vec![small(1, 4)],
			],
		),
		(
			"a write after a direct range delete is not covered by it",
			vec![vec![Op::DeleteRange(key(0), key(4)), big(5, 2)], vec![small(2, 3)]],
		),
		(
			"direct set then small delete",
			vec![vec![big(0, 2)], vec![Op::Delete(key(0))], vec![small(1, 3)]],
		),
	]
}

#[test(tokio::test)]
async fn every_position_of_a_direct_batch_in_a_group_reads_back_with_the_flush_stopped() {
	for (what, group) in shapes() {
		check_group(group, false, what).await;
	}
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn every_position_of_a_direct_batch_in_a_group_reads_back_with_the_flush_running() {
	for (what, group) in shapes() {
		check_group(group, true, what).await;
	}
}

/// The longest value whose batch, with the inline wrapper the flusher adds, needs at most
/// `target` bytes of memtable.
fn value_len_for(key_len: usize, target: u64) -> usize {
	(0..=MEMTABLE * 2).rev().find(|len| max_entry_bytes(key_len, len + 2) <= target).unwrap()
}

#[test(tokio::test)]
async fn a_batch_one_byte_either_side_of_the_memtable_size_reads_back() {
	let dir = TempDir::new("adv_l0_boundary").unwrap();
	let tree = Tree::new(options(dir.path(), false)).unwrap();
	stop_background(&tree).await;
	let mut model = Model::new();
	let mut tag = 1;
	seed(&tree, &mut model, tag).await;

	let klen = key(0).len();
	let mut went_to_a_table = 0;
	for delta in [-3i64, -2, -1, 0, 1, 2, 3] {
		let len = value_len_for(klen, (MEMTABLE as i64 + delta) as u64);
		let estimate = max_entry_bytes(klen, len + 2);
		for into in [0usize, 1] {
			tag += 1;
			let tables = tree.core.inner.level_manifest.read().unwrap().get_all_tables().len();
			let v = value_of(tag, len);
			let mut txn = tree.begin().unwrap();
			txn.set(key(into), v.clone()).unwrap();
			txn.commit().await.unwrap();
			model.insert(key(into), v);
			let now = tree.core.inner.level_manifest.read().unwrap().get_all_tables().len();
			if estimate > MEMTABLE as u64 {
				assert!(now > tables, "control: estimate {estimate} goes to a table");
				went_to_a_table += 1;
			}
			// A small write after, which sits in front of the table if it is read first.
			tag += 1;
			let v = value_of(tag, 100);
			let mut txn = tree.begin().unwrap();
			txn.set(key(2), v.clone()).unwrap();
			txn.commit().await.unwrap();
			model.insert(key(2), v);
			assert_reads(
				&read_only(&tree),
				&model,
				&keys(),
				&format!("delta {delta} into {into} (estimate {estimate})"),
			);
		}
	}
	assert!(went_to_a_table > 0, "control: some batch went to a table");
	tree.close().await.unwrap();
}

/// A batch under `max_memtable_size` that does not fit an empty arena is written to L0 as well,
/// between two writes of the same key.
#[test(tokio::test)]
async fn a_batch_that_fits_no_arena_reads_back_between_writes_of_its_key() {
	let fresh = MemTable::new(MEMTABLE);
	let klen = key(0).len();
	let len = (0..=MEMTABLE)
		.rev()
		.find(|len| {
			let estimate = max_entry_bytes(klen, len + 2);
			estimate <= MEMTABLE as u64 && !fresh.can_fit(estimate)
		})
		.expect("a batch between the arena and the memtable size");
	let group = vec![
		vec![small(0, 2)],
		vec![Op::Set(key(0), value_of(3, len))],
		vec![small(0, 4), small(1, 4)],
	];
	check_group(group.clone(), false, "no arena, stopped").await;
	check_group(group, true, "no arena, running").await;
}
/// The states a recovery may reach after a crash in the middle of `group`: what the group's
/// batches leave when a prefix of them is applied to `base`.
fn prefix_states(base: &Model, group: &[Vec<Op>]) -> Vec<Model> {
	let mut states = vec![base.clone()];
	let mut model = base.clone();
	for ops in group {
		apply(&mut model, ops);
		states.push(model.clone());
	}
	states
}

fn recovered_state(image: &Path) -> Model {
	let recovered = Tree::new(options(image, true)).unwrap();
	let state = all_keys(&recovered);
	abandon(&recovered);
	state
}

/// An image taken while a group with a direct batch in the middle is on its way, at every point
/// a hook can reach, recovers a prefix of the group and never a mix of old and new.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_crash_image_at_every_hook_of_a_group_with_a_direct_batch_recovers_a_prefix() {
	for durable in [true, false] {
		let dir = TempDir::new("adv_l0_prefix").unwrap();
		let live = dir.path().join("live");
		let tree = Tree::new(options(&live, false)).unwrap();
		stop_background(&tree).await;
		let mut model = Model::new();
		seed(&tree, &mut model, 1).await;
		let base = model.clone();

		let group = vec![
			vec![small(0, 2), small(1, 2)],
			vec![big(0, 3)],
			vec![small(0, 4), small(2, 4)],
			vec![big(1, 5)],
			vec![small(1, 6)],
		];
		let images: Arc<Mutex<Vec<(String, std::path::PathBuf)>>> = Arc::default();
		let take = {
			let (live, images, root) =
				(live.clone(), Arc::clone(&images), dir.path().to_path_buf());
			Arc::new(move |label: String| {
				let mut images = images.lock().unwrap();
				let to = root.join(format!("image_{}", images.len()));
				copy_dir(&live, &to);
				images.push((label, to));
			})
		};
		{
			let take = Arc::clone(&take);
			tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| match point {
				PipelineHook::AfterWalSync {
					..
				} => (*take)("after the wal".to_string()),
				PipelineHook::BeforeApplyRound {
					round,
				} => (*take)(format!("before apply round {round}")),
				PipelineHook::BeforeFencedApply => (*take)("before the fenced apply".to_string()),
				PipelineHook::BeforeWalHeal
				| PipelineHook::AfterWalReplace
				| PipelineHook::AfterWalHeal => {}
			})));
		}
		{
			let take = Arc::clone(&take);
			let hook: FlushHook = Arc::new(move |id| {
				(*take)(format!("in the flush of table {id}"));
				Ok(())
			});
			*tree.core.inner.flush_hook.lock() = Some(hook);
		}

		let batches = stamped(&tree, &group);
		tree.core.commit_pipeline.flush_group(&batches, durable).await.unwrap();
		publish_all(&tree);
		tree.core.commit_pipeline.set_hook(None);
		*tree.core.inner.flush_hook.lock() = None;
		for ops in &group {
			apply(&mut model, ops);
		}
		assert_reads(&read_only(&tree), &model, &keys(), "live");

		let states = prefix_states(&base, &group);
		let images = images.lock().unwrap().clone();
		assert!(images.len() >= 5, "control: images were taken ({})", images.len());
		for (label, image) in &images {
			let state = recovered_state(image);
			let at = states.iter().position(|s| *s == state);
			assert!(
				at.is_some(),
				"durable={durable}, image {label}: recovered a state that is no prefix of the group \
				 (key_0 {}, key_1 {}, key_2 {})",
				describe(&state.get(&key(0)).cloned()),
				describe(&state.get(&key(1)).cloned()),
				describe(&state.get(&key(2)).cloned()),
			);
			println!("durable={durable}: image {label} recovered prefix {}", at.unwrap());
		}
		tree.close().await.unwrap();
	}
}

/// A flush that fails inside a direct commit fails the commit, leaves the writes the memtable
/// holds readable, and the tree takes the next commits, direct ones included.
#[test(tokio::test)]
async fn a_failed_flush_inside_a_direct_commit_fails_it_and_the_tree_goes_on() {
	let dir = TempDir::new("adv_l0_failflush").unwrap();
	let live = dir.path().join("live");
	let tree = Tree::new(options(&live, false)).unwrap();
	stop_background(&tree).await;
	let mut model = Model::new();
	seed(&tree, &mut model, 1).await;

	let failures = Arc::new(AtomicUsize::new(1));
	{
		let failures = Arc::clone(&failures);
		let hook: FlushHook = Arc::new(move |_| {
			if failures.load(Ordering::SeqCst) > 0 {
				failures.fetch_sub(1, Ordering::SeqCst);
				return Err(Error::Other("injected flush failure".into()));
			}
			Ok(())
		});
		*tree.core.inner.flush_hook.lock() = Some(hook);
	}

	let mut txn = tree.begin().unwrap();
	txn.set(key(0), value_of(2, MEMTABLE + 1000)).unwrap();
	let outcome = txn.commit().await;
	assert!(outcome.is_err(), "the commit must fail with its flush");
	assert_eq!(failures.load(Ordering::SeqCst), 0, "control: the flush ran");
	assert_reads(&read_only(&tree), &model, &keys(), "after the failed commit");
	assert_eq!(tree.core.inner.immutable_count(), 1, "the memtable is still queued");

	// The next commits go through, the direct one flushing what the failed one left.
	for tag in 3..6 {
		let v = value_of(tag, 100);
		let mut txn = tree.begin().unwrap();
		txn.set(key(1), v.clone()).unwrap();
		txn.commit().await.unwrap();
		model.insert(key(1), v);
		let v = value_of(tag + 10, MEMTABLE + 1000);
		let mut txn = tree.begin().unwrap();
		txn.set(key(0), v.clone()).unwrap();
		txn.commit().await.unwrap();
		model.insert(key(0), v);
		assert_reads(&read_only(&tree), &model, &keys(), &format!("round {tag}"));
	}
	let image = dir.path().join("image");
	copy_dir(&live, &image);
	let recovered = Tree::new(options(&image, true)).unwrap();
	assert_reads(&read_only(&recovered), &model, &keys(), "crash image");
	abandon(&recovered);
	*tree.core.inner.flush_hook.lock() = None;
	tree.close().await.unwrap();
	let reopened = Tree::new(options(&live, true)).unwrap();
	assert_reads(&read_only(&reopened), &model, &keys(), "reopened");
	reopened.close().await.unwrap();
}

/// The second of two queued memtables fails to flush: the first is in a table, the second still
/// queued, and nothing reads the older value in front of the commit that eventually succeeds.
#[test(tokio::test)]
async fn a_flush_that_fails_behind_another_leaves_the_queue_readable() {
	let dir = TempDir::new("adv_l0_failsecond").unwrap();
	let tree = Tree::new(options(dir.path(), false)).unwrap();
	stop_background(&tree).await;
	let mut model = Model::new();
	seed(&tree, &mut model, 1).await;
	tree.core.inner.rotate_memtable().unwrap();
	let v = value_of(2, 100);
	let mut txn = tree.begin().unwrap();
	txn.set(key(0), v.clone()).unwrap();
	txn.commit().await.unwrap();
	model.insert(key(0), v);

	let calls = Arc::new(AtomicUsize::new(0));
	{
		let calls = Arc::clone(&calls);
		let hook: FlushHook = Arc::new(move |_| {
			if calls.fetch_add(1, Ordering::SeqCst) == 1 {
				return Err(Error::Other("injected flush failure".into()));
			}
			Ok(())
		});
		*tree.core.inner.flush_hook.lock() = Some(hook);
	}
	let mut txn = tree.begin().unwrap();
	txn.set(key(0), value_of(3, MEMTABLE + 1000)).unwrap();
	assert!(txn.commit().await.is_err());
	assert_eq!(calls.load(Ordering::SeqCst), 2, "control: two flushes were tried");
	assert_eq!(tree.core.inner.immutable_count(), 1);
	assert_reads(&read_only(&tree), &model, &keys(), "after the failure");

	let v = value_of(4, MEMTABLE + 1000);
	let mut txn = tree.begin().unwrap();
	txn.set(key(0), v.clone()).unwrap();
	txn.commit().await.unwrap();
	model.insert(key(0), v);
	assert_reads(&read_only(&tree), &model, &keys(), "after the retry");
	*tree.core.inner.flush_hook.lock() = None;
	tree.close().await.unwrap();
}
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
		let deadline = Instant::now() + Duration::from_secs(30);
		while !self.reached.load(Ordering::SeqCst) {
			assert!(Instant::now() < deadline, "no flush reached the gate");
			tokio::time::sleep(Duration::from_millis(1)).await;
		}
	}
}

struct OpenOnDrop(Arc<Gate>);

impl Drop for OpenOnDrop {
	fn drop(&mut self) {
		self.0.open.store(true, Ordering::SeqCst);
	}
}

/// `close` while the flusher is inside the flush of a direct commit waits for that commit, and
/// whatever the commit reports is what a reopen finds.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn close_while_a_direct_commit_flushes_waits_for_it_and_loses_nothing() {
	for background in [false, true] {
		let dir = TempDir::new("adv_l0_close").unwrap();
		let live = dir.path().join("live");
		let tree = Arc::new(Tree::new(options(&live, background)).unwrap());
		if !background {
			stop_background(&tree).await;
		}
		let mut model = Model::new();
		seed(&tree, &mut model, 1).await;
		let before = model.clone();
		let gate = Gate::install(&tree);
		let _open = OpenOnDrop(Arc::clone(&gate));

		let commit = {
			let tree = Arc::clone(&tree);
			tokio::spawn(async move {
				let mut txn = tree.begin().unwrap();
				txn.set(key(0), value_of(2, MEMTABLE + 1000)).unwrap();
				txn.commit().await
			})
		};
		gate.until_reached().await;
		let closing = {
			let tree = Arc::clone(&tree);
			tokio::spawn(async move { tree.close().await })
		};
		tokio::time::sleep(Duration::from_millis(300)).await;
		assert!(!closing.is_finished(), "control: close waits for the commit in the flusher");
		gate.open.store(true, Ordering::SeqCst);
		let outcome =
			tokio::time::timeout(WATCHDOG, commit).await.expect("the commit hung").unwrap();
		tokio::time::timeout(WATCHDOG, closing).await.expect("close hung").unwrap().unwrap();

		*tree.core.inner.flush_hook.lock() = None;
		let reopened = Tree::new(options(&live, true)).unwrap();
		let state = all_keys(&reopened);
		match outcome {
			Ok(()) => {
				assert_eq!(
					state.get(&key(0)).map(|v| v.len()),
					Some(MEMTABLE + 1000),
					"background={background}: an acknowledged commit is read after a reopen"
				);
			}
			Err(_) => {
				assert!(
					state == before || state.get(&key(0)).map(|v| v.len()) == Some(MEMTABLE + 1000),
					"background={background}: a failed commit left a torn state"
				);
			}
		}
		reopened.close().await.unwrap();
	}
}

/// A checkpoint that begins while the flusher is inside the flush of a direct commit waits for
/// it, and what it holds is the tree before the commit or after it.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_checkpoint_taken_during_the_flush_of_a_direct_commit_is_whole() {
	let dir = TempDir::new("adv_l0_checkpoint").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(options(&live, false)).unwrap());
	stop_background(&tree).await;
	let mut model = Model::new();
	seed(&tree, &mut model, 1).await;
	let before = model.clone();
	let gate = Gate::install(&tree);
	let _open = OpenOnDrop(Arc::clone(&gate));

	let commit = {
		let tree = Arc::clone(&tree);
		tokio::spawn(async move {
			let mut txn = tree.begin().unwrap();
			txn.set(key(0), value_of(2, MEMTABLE + 1000)).unwrap();
			txn.commit().await
		})
	};
	gate.until_reached().await;
	let checkpoint_dir = dir.path().join("checkpoint");
	let checkpoint = {
		let (tree, to) = (Arc::clone(&tree), checkpoint_dir.clone());
		std::thread::spawn(move || tree.create_checkpoint(&to))
	};
	tokio::time::sleep(Duration::from_millis(300)).await;
	assert!(!checkpoint.is_finished(), "control: the checkpoint waits for the flush");
	gate.open.store(true, Ordering::SeqCst);
	tokio::time::timeout(WATCHDOG, commit).await.expect("the commit hung").unwrap().unwrap();
	checkpoint.join().unwrap().unwrap();

	let other = Tree::new(options(&dir.path().join("other"), true)).unwrap();
	other.restore_from_checkpoint(&checkpoint_dir).unwrap();
	let state = all_keys(&other);
	let mut after = before.clone();
	after.insert(key(0), value_of(2, MEMTABLE + 1000));
	assert!(
		state == before || state == after,
		"the checkpoint holds a state that is neither before the commit nor after it: key_0 is {}",
		describe(&state.get(&key(0)).cloned())
	);
	other.close().await.unwrap();
	*tree.core.inner.flush_hook.lock() = None;
	tree.close().await.unwrap();
}
/// The value log in use with every value in it, so that pointers, not values, fill the memtable.
fn vlog_options(path: &Path, background: bool) -> Arc<Options> {
	let mut opts = (*options(path, background)).clone();
	opts.enable_vlog = true;
	opts.vlog_value_threshold = 0;
	Arc::new(opts)
}

/// Fillers whose pointers into the value log still need more than a memtable, and the write.
fn vlog_big(i: usize, tag: u64) -> Vec<Op> {
	let mut ops: Vec<Op> = (0..250)
		.map(|n| Op::Set(format!("zz_{tag}_{n:04}").into_bytes(), value_of(tag, 1000)))
		.collect();
	ops.push(small(i, tag));
	ops
}

/// A group with a batch that goes to a table because its pointers into the value log do not fit a
/// memtable. A transaction that began before the group keeps reading what it began with, and a
/// transaction that begins after it reads every write of the group.
async fn vlog_group(group: Vec<Vec<Op>>, background: bool, what: &str) {
	let dir = TempDir::new("adv_l0_vlog").unwrap();
	let tree = Tree::new(vlog_options(dir.path(), background)).unwrap();
	if !background {
		stop_background(&tree).await;
	}
	let mut model = Model::new();
	seed(&tree, &mut model, 1).await;
	let before = read_only(&tree);
	let before_model = model.clone();

	let batches = stamped(&tree, &group);
	for ops in &group {
		apply(&mut model, ops);
	}
	let direct = batches.iter().filter(|b| b.memtable_size_estimate() > MEMTABLE as u64).count();
	tree.core.commit_pipeline.flush_group(&batches, true).await.unwrap();
	publish_all(&tree);

	let after = read_only(&tree);
	assert_reads(&after, &model, &keys(), &format!("{what}: after"));
	assert_reads(&before, &before_model, &keys(), &format!("{what}: before"));
	assert_eq!(all_keys(&tree), model, "{what}: a transaction that begins after the group");
	assert!(direct > 0, "{what}: control: a batch went to a table");
	drop(before);
	drop(after);
	tree.close().await.unwrap();
}

fn vlog_shapes() -> Vec<(&'static str, Vec<Vec<Op>>)> {
	vec![
		("direct first", vec![vlog_big(0, 2), vec![small(0, 3)], vec![small(1, 4)]]),
		("direct middle", vec![vec![small(0, 2)], vlog_big(0, 3), vec![small(0, 4)]]),
		("direct last", vec![vec![small(0, 2)], vec![small(1, 3)], vlog_big(0, 4)]),
		(
			"two direct batches over one key",
			vec![vlog_big(0, 2), vec![small(0, 3)], vlog_big(0, 4), vec![small(1, 5)]],
		),
	]
}

#[test(tokio::test)]
async fn value_log_pointers_in_a_direct_batch_read_back_with_the_flush_stopped() {
	for (what, group) in vlog_shapes() {
		vlog_group(group, false, what).await;
	}
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn value_log_pointers_in_a_direct_batch_read_back_with_the_flush_running() {
	for (what, group) in vlog_shapes() {
		vlog_group(group, true, what).await;
	}
}

/// Fails the table write of the direct batch in the middle of a group, with a directory where its
/// file would go, and returns what a crash image of the directory recovers, with the states the
/// group's prefixes leave.
async fn recover_after_a_failed_direct_write() -> (Model, Vec<Model>) {
	let dir = TempDir::new("adv_l0_failed_direct").unwrap();
	let live = dir.path().join("live");
	let tree = Tree::new(options(&live, false)).unwrap();
	stop_background(&tree).await;
	let mut model = Model::new();
	seed(&tree, &mut model, 1).await;
	let base = model.clone();

	let group = vec![vec![small(1, 2), small(2, 2)], vec![big(0, 3)], vec![small(3, 4)]];
	// The memtable the first batch is in is queued by the seal, which takes the next table id, and
	// the direct batch takes the one after it.
	let next = tree.core.inner.level_manifest.read().unwrap().next_table_id.load(Ordering::SeqCst);
	let blocker = tree.core.inner.opts.sstable_file_path(next + 1);
	std::fs::create_dir(&blocker).unwrap();
	let batches = stamped(&tree, &group);
	let outcome = tree.core.commit_pipeline.flush_group(&batches, true).await;
	assert!(outcome.is_err(), "control: the group fails with the table of its direct batch");
	std::fs::remove_dir(&blocker).unwrap();

	let image = dir.path().join("image");
	copy_dir(&live, &image);
	abandon(&tree);
	(recovered_state(&image), prefix_states(&base, &group))
}

/// The memtable the direct batch sealed is flushed before the batch is written, so the batches
/// ahead of it are in a table when the write fails. `log_number` is held at the segment that holds
/// the group while a batch of it is not applied yet, so the segment stays and recovery still finds
/// the group whole.
#[test(tokio::test)]
async fn a_group_whose_direct_write_fails_is_recovered_whole() {
	let (state, states) = recover_after_a_failed_direct_write().await;
	let whole = states.last().unwrap();
	assert!(
		state == *whole,
		"the group is recovered in full, got prefix {:?}",
		states.iter().position(|s| *s == state)
	);
}

// ---------------------------------------------------------------------------
// a recovered tree: its memtable holds rows of the crashed segments and is tagged with the
// segment recovery opened
// ---------------------------------------------------------------------------

/// The numbers of the WAL segments of `tree` that hold bytes.
fn non_empty_segments(tree: &Tree) -> Vec<u64> {
	let wal = tree.core.inner.wal.read();
	let mut ids = Vec::new();
	for entry in std::fs::read_dir(wal.get_dir_path()).unwrap() {
		let path = entry.unwrap().path();
		if path.extension().is_some_and(|ext| ext == "wal")
			&& std::fs::metadata(&path).unwrap().len() > 0
		{
			ids.push(path.file_stem().unwrap().to_str().unwrap().parse().unwrap());
		}
	}
	ids.sort();
	ids
}

/// Opens a copy of a crashed tree, with the background tasks stopped.
async fn reopen(image: &Path) -> Tree {
	let tree = Tree::new(options(image, false)).unwrap();
	stop_background(&tree).await;
	tree
}

/// A tree that was committed to and crashed, opened from the image of its directory, with
/// `queued` memtables queued on it. The rows of the crashed segments are in the first memtable,
/// the one tagged with the segment the recovery opened. Returns the tree, its directory and what
/// it holds.
async fn recovered_tree(root: &Path, queued: usize) -> (Tree, std::path::PathBuf, Model) {
	let first = root.join("first");
	let crashed = Tree::new(options(&first, false)).unwrap();
	stop_background(&crashed).await;
	let mut model = Model::new();
	seed(&crashed, &mut model, 1).await;
	let value = value_of(2, 100);
	let mut txn = crashed.begin().unwrap();
	txn.set(key(1), value.clone()).unwrap();
	txn.commit().await.unwrap();
	model.insert(key(1), value);
	let image = root.join("crash_one");
	copy_dir(&first, &image);
	abandon(&crashed);

	let tree = reopen(&image).await;
	{
		let inner = &tree.core.inner;
		let active = inner.active_memtable.read().unwrap();
		let segment = inner.wal.read().get_active_log_number();
		assert!(!active.is_empty(), "control: recovery put the crashed rows in the memtable");
		assert_eq!(active.get_wal_number(), segment, "control: it is tagged with the new segment");
		let held = non_empty_segments(&tree);
		assert!(
			!held.is_empty() && held.iter().all(|s| *s < segment),
			"control: the rows are in segments older than the tag ({held:?}, tag {segment})"
		);
	}
	assert_reads(&read_only(&tree), &model, &keys(), "recovered");

	// The recovered memtable is queued first, and the memtables after it are not recovered ones.
	for i in 0..queued {
		let value = value_of(10 + i as u64, 100);
		let mut txn = tree.begin().unwrap();
		txn.set(key(2 + i), value.clone()).unwrap();
		txn.commit().await.unwrap();
		model.insert(key(2 + i), value);
		tree.core.inner.rotate_memtable().unwrap();
	}
	let value = value_of(20, 100);
	let mut txn = tree.begin().unwrap();
	txn.set(key(5), value.clone()).unwrap();
	txn.commit().await.unwrap();
	model.insert(key(5), value);
	assert_eq!(tree.core.inner.immutable_count(), queued, "control: the queue");
	(tree, image, model)
}

/// Two crashes and two recoveries in a row: the directory is copied, the copy opened, copied
/// before anything was flushed and opened again. Every one holds `expected`.
async fn assert_survives_two_crashes(
	live: &Path,
	tree: &Tree,
	root: &Path,
	expected: &Model,
	what: &str,
) {
	let second = root.join("crash_two");
	copy_dir(live, &second);
	abandon(tree);
	let again = reopen(&second).await;
	assert_eq!(all_keys(&again), *expected, "{what}: the image of the recovered tree");
	assert!(!non_empty_segments(&again).is_empty(), "control: the image holds records");

	let third = root.join("crash_three");
	copy_dir(&second, &third);
	abandon(&again);
	let twice = reopen(&third).await;
	assert_eq!(all_keys(&twice), *expected, "{what}: the image of the tree recovered twice");

	// What the tree recovered twice acknowledges survives one more.
	let mut expected = expected.clone();
	let value = value_of(30, 100);
	let mut txn = twice.begin().unwrap();
	txn.set(key(0), value.clone()).unwrap();
	txn.commit().await.unwrap();
	expected.insert(key(0), value);
	let fourth = root.join("crash_four");
	copy_dir(&third, &fourth);
	abandon(&twice);
	let last = reopen(&fourth).await;
	assert_eq!(all_keys(&last), expected, "{what}: the image of the tree recovered three times");
	abandon(&last);
}

/// A group with an oversized batch goes through a recovered tree whose queue holds `queued`
/// memtables, the first of them the one with the recovered rows. Every read is the newest write,
/// the queue is empty, and the crash images of the tree, taken before any later flush, recover
/// every acknowledged commit and the whole group, twice.
async fn group_on_a_recovered_tree(group: Vec<Vec<Op>>, queued: usize, durable: bool, what: &str) {
	let what = format!("{what}, {queued} queued, durable={durable}");
	let dir = TempDir::new("l0_recovered").unwrap();
	let (tree, live, mut model) = recovered_tree(dir.path(), queued).await;

	let batches = stamped(&tree, &group);
	tree.core.commit_pipeline.flush_group(&batches, durable).await.unwrap();
	publish_all(&tree);
	for ops in &group {
		apply(&mut model, ops);
	}
	assert_reads(&read_only(&tree), &model, &keys(), &format!("{what}: live"));
	assert_eq!(tree.core.inner.immutable_count(), 0, "{what}: the queue was flushed");

	assert_survives_two_crashes(&live, &tree, dir.path(), &model, &what).await;
}

#[test(tokio::test)]
async fn a_direct_group_on_a_recovered_tree_survives_two_crashes() {
	let shapes = [
		("direct alone", vec![vec![big(0, 40)]]),
		("direct first", vec![vec![big(0, 40)], vec![small(0, 41)], vec![small(1, 42)]]),
		("direct middle", vec![vec![small(0, 40)], vec![big(1, 41)], vec![small(0, 42)]]),
		("direct last", vec![vec![small(0, 40), small(1, 40)], vec![big(0, 41)]]),
	];
	for (what, group) in shapes {
		for queued in [0, 2] {
			for durable in [true, false] {
				group_on_a_recovered_tree(group.clone(), queued, durable, what).await;
			}
		}
	}
}

/// Where a group on a recovered tree fails after part of it was applied.
#[derive(Clone, Copy, Debug)]
enum Fails {
	/// The flush at this position of the inline flush: the recovered memtable first, the sealed
	/// one last.
	Flush(usize),
	/// The table of the oversized batch, after the queue was flushed.
	DirectTable,
}

/// One transaction that writes `ops`, as a batch of its own.
async fn commit_ops(tree: Arc<Tree>, ops: Vec<Op>) -> crate::Result<()> {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(Durability::Immediate);
	for op in ops {
		match op {
			Op::Set(k, v) => txn.set(k, v).unwrap(),
			_ => unreachable!("the groups of this test only set"),
		}
	}
	txn.commit().await
}

/// A group fails after its first batch was applied, on a recovered tree with `queued` memtables
/// queued: the database stops, nothing of the group is readable, and the crash images recover
/// every acknowledged commit and the whole group, twice. The group is one commit group of the
/// flusher: the transactions are spawned before the flusher is polled, on the current-thread
/// runtime.
async fn failing_group_on_a_recovered_tree(queued: usize, fails: Fails) {
	let what = format!("{fails:?}, {queued} queued");
	let dir = TempDir::new("l0_recovered_fail").unwrap();
	let (tree, live, base) = recovered_tree(dir.path(), queued).await;
	let tree = Arc::new(tree);

	// The transactions write different keys, or they would conflict.
	let group = vec![vec![small(0, 40), small(1, 40)], vec![big(2, 41)], vec![small(3, 42)]];
	let next = tree.core.inner.level_manifest.read().unwrap().next_table_id.load(Ordering::SeqCst);
	// The seal queues the memtable the first batch is in and takes `next`, the direct batch takes
	// the id after it.
	let blocker = tree.core.inner.opts.sstable_file_path(next + 1);
	let calls = Arc::new(AtomicUsize::new(0));
	match fails {
		Fails::Flush(at) => {
			assert!(at <= queued, "the test asks for a flush that is not there");
			let calls = Arc::clone(&calls);
			let hook: FlushHook = Arc::new(move |_| {
				if calls.fetch_add(1, Ordering::SeqCst) == at {
					return Err(Error::Other("injected flush failure".into()));
				}
				Ok(())
			});
			*tree.core.inner.flush_hook.lock() = Some(hook);
		}
		Fails::DirectTable => std::fs::create_dir(&blocker).unwrap(),
	}
	let handles: Vec<_> =
		group.iter().map(|ops| tokio::spawn(commit_ops(Arc::clone(&tree), ops.clone()))).collect();
	for handle in handles {
		let outcome = handle.await.unwrap();
		assert!(
			matches!(outcome, Err(Error::DatabaseStopped(_))),
			"{what}: every committer of the group is told the database stopped, got {outcome:?}"
		);
	}
	*tree.core.inner.flush_hook.lock() = None;
	match fails {
		Fails::DirectTable => std::fs::remove_dir(&blocker).unwrap(),
		Fails::Flush(at) => {
			assert!(calls.load(Ordering::SeqCst) > at, "{what}: control: the flush ran");
		}
	}
	assert!(tree.core.inner.error_handler.is_db_stopped(), "{what}");
	assert_reads(&read_only(&tree), &base, &keys(), &format!("{what}: after the failure"));

	let mut whole = base;
	for ops in &group {
		apply(&mut whole, ops);
	}
	assert_survives_two_crashes(&live, &tree, dir.path(), &whole, &what).await;
}

#[test(tokio::test)]
async fn a_failed_flush_before_a_direct_batch_on_a_recovered_tree_recovers_the_group_whole() {
	for (queued, at) in [(0, 0), (2, 0), (2, 1), (2, 2)] {
		failing_group_on_a_recovered_tree(queued, Fails::Flush(at)).await;
	}
}

#[test(tokio::test)]
async fn a_failed_direct_table_on_a_recovered_tree_recovers_the_group_whole() {
	for queued in [0, 2] {
		failing_group_on_a_recovered_tree(queued, Fails::DirectTable).await;
	}
}
