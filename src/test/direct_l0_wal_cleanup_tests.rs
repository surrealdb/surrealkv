//! A commit larger than a memtable goes straight to a table of level zero, and removes the WAL
//! segments that table made obsolete.
//!
//! The write seals the segment that holds the record of the batch and moves the manifest's
//! `log_number` past it. Nothing else removes that segment until the next memtable flush or the
//! close, and a run of commits that are all oversized never has one, so each of them used to leave
//! a segment on disk. The segments that the rest of the commit group still needs, and the ones
//! that a queued memtable still holds records for, are kept: they are below the `log_number` that
//! the manifest records, which is the only bound the cleanup uses.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use tempdir::TempDir;
use test_log::test;

use super::wal_group_budget_tests::{copy_dir, mark_closed, release_lock};
use super::wal_sealed_tests::context;
use crate::batch::Batch;
use crate::lsm::FlushHook;
use crate::ring::PipelineHook;
use crate::tde::CipherSuite;
use crate::{Durability, InternalKeyKind, Mode, Options, Tree};

const KIB: usize = 1024;

/// A memtable that a value of seventy KiB does not fit, nor 33 entries of 8 B and 8 B.
const MEMTABLE: usize = 64 * KIB;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn options(path: &Path, encrypted: bool) -> Arc<Options> {
	let opts = Options {
		path: path.to_path_buf(),
		max_memtable_size: MEMTABLE,
		// Crash semantics: nothing may be flushed or removed on close.
		flush_on_close: false,
		// No compaction can start, and nothing stalls behind a flush that is stopped.
		level0_max_files: 1000,
		l0_stall_threshold: 10_000,
		..Default::default()
	};
	Arc::new(if encrypted {
		opts.with_encryption_context(context(CipherSuite::Aes256Gcm))
	} else {
		opts
	})
}

async fn stop_background(tree: &Tree) {
	let tasks = tree.core.task_manager.lock().unwrap().take().unwrap();
	tasks.stop().await;
}

/// Leaves the directory of a tree as a crash does.
fn crash(tree: &Tree) {
	mark_closed(tree);
	release_lock(tree);
}

fn key(i: usize) -> Vec<u8> {
	format!("key_{i:06}").into_bytes()
}

fn big_value(tag: u64) -> Vec<u8> {
	let mut v = vec![tag as u8; MEMTABLE + 6 * KIB];
	v[..8].copy_from_slice(&tag.to_le_bytes());
	v
}

/// The numbers of the WAL segments in `dir`, empty ones included.
fn wal_segments(dir: &Path) -> Vec<u64> {
	let mut ids = Vec::new();
	for entry in std::fs::read_dir(dir.join("wal")).unwrap() {
		let path = entry.unwrap().path();
		if path.extension().is_some_and(|ext| ext == "wal") {
			ids.push(path.file_stem().unwrap().to_str().unwrap().parse().unwrap());
		}
	}
	ids.sort();
	ids
}

fn log_number(tree: &Tree) -> u64 {
	tree.core.inner.level_manifest.read().unwrap().get_log_number()
}

fn active_segment(tree: &Tree) -> u64 {
	tree.core.inner.wal.read().get_active_log_number()
}

async fn commit_set(tree: &Tree, key: Vec<u8>, value: Vec<u8>) {
	let mut txn = tree.begin().unwrap();
	txn.set(key, value).unwrap();
	txn.commit().await.unwrap();
}

fn read_all(tree: &Tree, keys: &Model) -> Model {
	let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
	keys.keys().filter_map(|k| txn.get(k).unwrap().map(|v| (k.clone(), v))).collect()
}

/// Opens a copy of a crashed directory and asserts that it holds `model`.
async fn assert_image_recovers(image: &Path, encrypted: bool, model: &Model, what: &str) {
	let tree = Tree::new(options(image, encrypted)).unwrap();
	stop_background(&tree).await;
	let got = read_all(&tree, model);
	for (k, v) in model {
		assert!(got.get(k) == Some(v), "{what}: {} was not recovered", String::from_utf8_lossy(k));
	}
	crash(&tree);
}

// ---------------------------------------------------------------------------
// a run of oversized commits
// ---------------------------------------------------------------------------

async fn direct_commits_leave_no_obsolete_wal_segments(encrypted: bool) {
	let dir = TempDir::new("l0_wal_cleanup").unwrap();
	let live = dir.path().join("live");
	let tree = Tree::new(options(&live, encrypted)).unwrap();
	stop_background(&tree).await;

	let mut model = Model::new();
	for i in 0..20u64 {
		let (k, v) = (key(i as usize), big_value(i + 1));
		commit_set(&tree, k.clone(), v.clone()).await;
		model.insert(k, v);

		let segments = wal_segments(&live);
		assert!(
			segments.len() <= 2,
			"after oversized commit {i} the WAL holds {} segments: {segments:?}",
			segments.len()
		);
		assert_eq!(
			log_number(&tree),
			active_segment(&tree),
			"after oversized commit {i}: the manifest is past every sealed segment"
		);
	}

	// The segments that are gone were not needed: a crash image holds every commit.
	let image = dir.path().join("image");
	copy_dir(&live, &image);
	crash(&tree);
	assert_image_recovers(&image, encrypted, &model, "image after 20 oversized commits").await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn direct_commits_leave_no_obsolete_wal_segments_plain() {
	direct_commits_leave_no_obsolete_wal_segments(false).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn direct_commits_leave_no_obsolete_wal_segments_encrypted() {
	direct_commits_leave_no_obsolete_wal_segments(true).await;
}

// ---------------------------------------------------------------------------
// the segment the rest of a group needs
// ---------------------------------------------------------------------------

fn batch_of(start: u64, kvs: &[(Vec<u8>, Vec<u8>)]) -> Batch {
	let mut batch = Batch::new(start);
	for (i, (k, v)) in kvs.iter().enumerate() {
		batch
			.add_record(InternalKeyKind::Set, k.clone(), Some(v.clone()), start + i as u64)
			.unwrap();
	}
	batch
}

/// The batches as `flush_entries` hands them to `flush_group`, numbered on from the next sequence
/// number.
fn stamped(tree: &Tree, group: &[Vec<(Vec<u8>, Vec<u8>)>]) -> Vec<Batch> {
	let pipeline = &tree.core.commit_pipeline;
	group
		.iter()
		.map(|kvs| {
			let start = pipeline.log_seq_num.fetch_add(kvs.len() as u64, Ordering::SeqCst);
			batch_of(start, kvs)
		})
		.collect()
}

fn publish_all(tree: &Tree) {
	let pipeline = &tree.core.commit_pipeline;
	tree.core
		.inner
		.visible_seq_num
		.store(pipeline.log_seq_num.load(Ordering::SeqCst) - 1, Ordering::SeqCst);
}

fn small(i: usize, tag: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
	vec![(key(i), tag.to_le_bytes().to_vec())]
}

fn big(i: usize, tag: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
	vec![(key(i), big_value(tag))]
}

/// What the second apply round of a group saw: the manifest's `log_number`, the segments on disk,
/// and an image of the directory.
type RoundOne = (u64, Vec<u64>, PathBuf);

/// A group whose oversized batch is followed by others keeps the segment that holds their only
/// record until they are applied.
async fn a_direct_batch_keeps_the_segment_its_group_rest_needs(encrypted: bool) {
	let dir = TempDir::new("l0_wal_rest").unwrap();
	let live = dir.path().join("live");
	let tree = Tree::new(options(&live, encrypted)).unwrap();
	stop_background(&tree).await;
	commit_set(&tree, key(100), small(100, 1)[0].1.clone()).await;

	// [big, small, small]: at the second round the rest is not applied, and its record is in the
	// segment of the group, which is the one that the direct batch sealed.
	let group = vec![big(0, 2), small(1, 3), small(2, 4)];
	let group_segment = active_segment(&tree);
	let seen: Arc<Mutex<Vec<RoundOne>>> = Arc::default();
	{
		let (seen, live, root) = (Arc::clone(&seen), live.clone(), dir.path().to_path_buf());
		let inner = Arc::clone(&tree.core.inner);
		tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
			if let PipelineHook::BeforeApplyRound {
				round: 1,
			} = point
			{
				let log = inner.level_manifest.read().unwrap().get_log_number();
				let image = root.join("image_round_one");
				copy_dir(&live, &image);
				seen.lock().unwrap().push((log, wal_segments(&live), image));
			}
		})));
	}
	let batches = stamped(&tree, &group);
	tree.core.commit_pipeline.flush_group(&batches, true).await.unwrap();
	publish_all(&tree);
	tree.core.commit_pipeline.set_hook(None);

	let seen = seen.lock().unwrap().clone();
	assert_eq!(seen.len(), 1, "control: the group has a second round");
	let (log, segments, image) = &seen[0];
	assert!(
		*log <= group_segment,
		"the manifest moved past the segment of the group's rest ({log} > {group_segment})"
	);
	assert!(
		segments.contains(&group_segment),
		"the segment {group_segment} of the group's rest was removed: {segments:?}"
	);
	let model: Model = group.iter().flatten().cloned().collect();
	assert_image_recovers(image, encrypted, &model, "image before the rest is applied").await;
	crash(&tree);
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_direct_batch_keeps_the_segment_its_group_rest_needs_plain() {
	a_direct_batch_keeps_the_segment_its_group_rest_needs(false).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_direct_batch_keeps_the_segment_its_group_rest_needs_encrypted() {
	a_direct_batch_keeps_the_segment_its_group_rest_needs(true).await;
}

/// A group whose last batch is the oversized one has no rest, and its segment goes with the
/// table.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_group_that_ends_with_a_direct_batch_leaves_no_obsolete_segment() {
	let dir = TempDir::new("l0_wal_last").unwrap();
	let live = dir.path().join("live");
	let tree = Tree::new(options(&live, false)).unwrap();
	stop_background(&tree).await;
	commit_set(&tree, key(100), small(100, 1)[0].1.clone()).await;

	let group = vec![small(1, 3), small(2, 4), big(0, 2)];
	let group_segment = active_segment(&tree);
	let batches = stamped(&tree, &group);
	tree.core.commit_pipeline.flush_group(&batches, true).await.unwrap();
	publish_all(&tree);

	let segments = wal_segments(&live);
	assert_eq!(
		log_number(&tree),
		active_segment(&tree),
		"the manifest is past the segment of the group"
	);
	assert!(
		!segments.contains(&group_segment),
		"the segment {group_segment} of the group was left behind: {segments:?}"
	);
	let mut model: Model = group.iter().flatten().cloned().collect();
	model.insert(key(100), small(100, 1)[0].1.clone());
	let image = dir.path().join("image");
	copy_dir(&live, &image);
	crash(&tree);
	assert_image_recovers(&image, false, &model, "image after the group").await;
}

// ---------------------------------------------------------------------------
// through the flusher
// ---------------------------------------------------------------------------

/// Commits of 33 entries of 8 B and 8 B are larger than the memtable by their estimate, though not
/// by their payload. Committers that race form groups of them, and every one goes down the direct
/// path. The WAL ends with the active segment and what the stall threshold allows to queue, not
/// one segment per commit.
async fn many_direct_groups_do_not_accumulate_segments(encrypted: bool) {
	const COMMITTERS: usize = 64;
	const ROUNDS: usize = 4;
	let dir = TempDir::new("l0_wal_many").unwrap();
	let live = dir.path().join("live");
	let opts = options(&live, encrypted);
	let threshold = opts.memtable_stall_threshold;
	let tree = Arc::new(Tree::new(opts).unwrap());

	let mut model = Model::new();
	for c in 0..COMMITTERS {
		for r in 0..ROUNDS {
			for e in 0..33 {
				let k = format!("k{c:02}{r}{e:02}").into_bytes();
				model.insert(k.clone(), k[..8.min(k.len())].to_vec());
			}
		}
	}
	let mut handles = Vec::new();
	for c in 0..COMMITTERS {
		let tree = Arc::clone(&tree);
		handles.push(tokio::spawn(async move {
			for r in 0..ROUNDS {
				let mut txn = tree.begin().unwrap();
				txn.set_durability(Durability::Eventual);
				for e in 0..33 {
					let k = format!("k{c:02}{r}{e:02}").into_bytes();
					let v = k[..8.min(k.len())].to_vec();
					txn.set(k, v).unwrap();
				}
				txn.commit().await.unwrap();
			}
		}));
	}
	for h in handles {
		h.await.unwrap();
	}

	let segments = wal_segments(&live);
	assert!(
		segments.len() <= threshold + 3,
		"after {} oversized commits the WAL holds {} segments",
		COMMITTERS * ROUNDS,
		segments.len()
	);
	let image = dir.path().join("image");
	copy_dir(&live, &image);
	crash(&tree);
	assert_image_recovers(&image, encrypted, &model, "image after the commits").await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn many_direct_groups_do_not_accumulate_segments_plain() {
	many_direct_groups_do_not_accumulate_segments(false).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn many_direct_groups_do_not_accumulate_segments_encrypted() {
	many_direct_groups_do_not_accumulate_segments(true).await;
}

// ---------------------------------------------------------------------------
// crash images around the two durable steps of a direct write
// ---------------------------------------------------------------------------

/// An image copied at the moment the hook runs.
fn image_hook(live: &Path, root: &Path, name: &'static str) -> (FlushHook, Arc<Mutex<Vec<u64>>>) {
	let seen: Arc<Mutex<Vec<u64>>> = Arc::default();
	let hook: FlushHook = {
		let (live, root, seen) = (live.to_path_buf(), root.to_path_buf(), Arc::clone(&seen));
		Arc::new(move |n| {
			let image = root.join(name);
			assert!(!image.exists(), "{name}: the hook ran twice");
			copy_dir(&live, &image);
			seen.lock().unwrap().push(n);
			Ok(())
		})
	};
	(hook, seen)
}

/// A direct write that fails before its manifest write leaves the segment of its record where
/// it is: the image taken then recovers the commits before it, and the failed one whole or not at
/// all.
async fn a_failed_direct_commit_keeps_its_segment(encrypted: bool) {
	let dir = TempDir::new("l0_wal_failed").unwrap();
	let live = dir.path().join("live");
	let tree = Tree::new(options(&live, encrypted)).unwrap();
	stop_background(&tree).await;
	let mut model = Model::new();
	for i in 0..3 {
		let (k, v) = (key(i), big_value(i as u64 + 1));
		commit_set(&tree, k.clone(), v.clone()).await;
		model.insert(k, v);
	}
	let segment = active_segment(&tree);

	let (hook, seen) = image_hook(&live, dir.path(), "image");
	let table_hook: FlushHook = Arc::new(move |id| {
		hook(id)?;
		Err(crate::Error::Other("the table is not registered".to_string()))
	});
	*tree.core.inner.direct_table_hook.lock() = Some(table_hook);
	let mut txn = tree.begin().unwrap();
	txn.set(key(9), big_value(9)).unwrap();
	assert!(txn.commit().await.is_err(), "the direct write fails");
	*tree.core.inner.direct_table_hook.lock() = None;
	assert_eq!(seen.lock().unwrap().len(), 1, "control: the hook ran");

	assert!(
		wal_segments(&live).contains(&segment),
		"the segment {segment} of the failed commit was removed"
	);
	assert_eq!(log_number(&tree), segment, "the manifest did not move");
	assert_image_recovers(&dir.path().join("image"), encrypted, &model, "image of the failure")
		.await;
	crash(&tree);
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_failed_direct_commit_keeps_its_segment_plain() {
	a_failed_direct_commit_keeps_its_segment(false).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_failed_direct_commit_keeps_its_segment_encrypted() {
	a_failed_direct_commit_keeps_its_segment(true).await;
}

/// An image taken after the manifest moved and before any segment is removed holds every commit,
/// the leftover segments below its `log_number` are not replayed, and the next flush removes them.
/// Whichever of them a crash had already removed, it holds every commit.
async fn an_image_between_the_manifest_and_the_unlink_recovers(encrypted: bool) {
	let dir = TempDir::new("l0_wal_unlink").unwrap();
	let live = dir.path().join("live");
	let tree = Tree::new(options(&live, encrypted)).unwrap();
	stop_background(&tree).await;
	let mut model = Model::new();
	for i in 0..4 {
		let (k, v) = (key(i), big_value(i as u64 + 1));
		commit_set(&tree, k.clone(), v.clone()).await;
		model.insert(k, v);
	}

	// Leftovers for the image to hold: the segments of the commits so far are gone, so the next
	// commit is the one whose unlink the image precedes.
	let (hook, seen) = image_hook(&live, dir.path(), "image");
	*tree.core.inner.direct_cleanup_hook.lock() = Some(hook);
	let (k, v) = (key(10), big_value(10));
	commit_set(&tree, k.clone(), v.clone()).await;
	model.insert(k, v);
	*tree.core.inner.direct_cleanup_hook.lock() = None;
	let logs = seen.lock().unwrap().clone();
	assert_eq!(logs.len(), 1, "control: the hook ran once");
	let log = logs[0];
	let image = dir.path().join("image");
	let leftovers: Vec<u64> = wal_segments(&image).into_iter().filter(|s| *s < log).collect();
	assert!(!leftovers.is_empty(), "control: the image holds the segments the unlink removes");
	assert!(
		!wal_segments(&live).iter().any(|s| *s < log),
		"the commit removed the segments below its log number {log}"
	);
	assert!(
		wal_segments(&image).iter().any(|s| *s >= log),
		"no segment from the log number {log} on is missing from the image"
	);
	assert_image_recovers(&image, encrypted, &model, "image before the unlink").await;

	// Whichever subset of the leftovers a crash removed, the rest is not needed.
	for (n, drop_these) in
		[leftovers.iter().step_by(2).copied().collect::<Vec<_>>(), leftovers.clone()]
			.into_iter()
			.enumerate()
	{
		let partial = dir.path().join(format!("partial_{n}"));
		copy_dir(&image, &partial);
		for id in drop_these {
			std::fs::remove_file(partial.join("wal").join(format!("{id:020}.wal"))).unwrap();
		}
		assert_image_recovers(&partial, encrypted, &model, "image with part of the unlink").await;
	}

	// The next flush of a reopened tree removes the leftovers: nothing below the log number is
	// read, and nothing needs it.
	let reopened = Tree::new(options(&image, encrypted)).unwrap();
	stop_background(&reopened).await;
	commit_set(&reopened, key(20), vec![1; 100]).await;
	reopened.core.inner.rotate_memtable().unwrap();
	reopened.core.inner.flush_all_immutables_sync().unwrap();
	let log = log_number(&reopened);
	assert!(
		!wal_segments(&image).iter().any(|s| *s < log),
		"the flush left segments below the log number {log}: {:?}",
		wal_segments(&image)
	);
	crash(&reopened);
	crash(&tree);
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn an_image_between_the_manifest_and_the_unlink_recovers_plain() {
	an_image_between_the_manifest_and_the_unlink_recovers(false).await;
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn an_image_between_the_manifest_and_the_unlink_recovers_encrypted() {
	an_image_between_the_manifest_and_the_unlink_recovers(true).await;
}

/// A cleanup that cannot remove a segment does not fail the commit, and the next oversized commit
/// removes what was left.
#[cfg(unix)]
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_cleanup_that_fails_does_not_fail_the_commit() {
	use std::os::unix::fs::PermissionsExt;

	let dir = TempDir::new("l0_wal_cleanup_fails").unwrap();
	let live = dir.path().join("live");
	let tree = Tree::new(options(&live, false)).unwrap();
	stop_background(&tree).await;
	let mut model = Model::new();
	commit_set(&tree, key(0), big_value(1)).await;
	model.insert(key(0), big_value(1));

	let wal = live.join("wal");
	let locked = Arc::new(Mutex::new(false));
	let hook: FlushHook = {
		let (wal, locked) = (wal.clone(), Arc::clone(&locked));
		Arc::new(move |_| {
			std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o555)).unwrap();
			// A process that runs as root can remove files from a directory it cannot write to.
			*locked.lock().unwrap() = std::fs::File::create(wal.join("probe")).is_err();
			Ok(())
		})
	};
	*tree.core.inner.direct_cleanup_hook.lock() = Some(hook);
	commit_set(&tree, key(1), big_value(2)).await;
	model.insert(key(1), big_value(2));
	*tree.core.inner.direct_cleanup_hook.lock() = None;
	std::fs::set_permissions(&wal, std::fs::Permissions::from_mode(0o755)).unwrap();
	let _ = std::fs::remove_file(wal.join("probe"));
	if !*locked.lock().unwrap() {
		println!("skipped: this process can write to a directory that is read-only");
		crash(&tree);
		return;
	}

	let log = log_number(&tree);
	assert!(
		wal_segments(&live).iter().any(|s| *s < log),
		"control: the cleanup could not remove the segments below {log}"
	);
	commit_set(&tree, key(2), big_value(3)).await;
	model.insert(key(2), big_value(3));
	let log = log_number(&tree);
	assert!(
		!wal_segments(&live).iter().any(|s| *s < log),
		"the next oversized commit left segments below {log}: {:?}",
		wal_segments(&live)
	);
	let image = dir.path().join("image");
	copy_dir(&live, &image);
	crash(&tree);
	assert_image_recovers(&image, false, &model, "image after the failed cleanup").await;
}

/// A memtable that is queued and older than the direct batch holds `log_number` back: the manifest
/// does not move, and no segment that the queued memtable's records are in is removed.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_queued_immutable_keeps_its_segment() {
	let dir = TempDir::new("l0_wal_queued").unwrap();
	let live = dir.path().join("live");
	let tree = Tree::new(options(&live, false)).unwrap();
	stop_background(&tree).await;
	let inner = &tree.core.inner;

	commit_set(&tree, key(0), vec![1; 100]).await;
	let queued_segment = active_segment(&tree);
	inner.rotate_memtable().unwrap();
	assert_eq!(inner.immutable_count(), 1, "control: the memtable is queued");
	let log_before = log_number(&tree);

	// The direct write as `write_direct_to_l0` makes it, with the queue left as it is.
	let sealed = inner.seal_active_wal_segment().unwrap();
	assert!(sealed > queued_segment, "control: the batch is newer than the queued memtable");
	let batch = batch_of(1_000, &big(1, 7));
	let table_id = inner.level_manifest.read().unwrap().next_table_id();
	inner.write_batch_direct_to_l0_sst(&batch, table_id, sealed, u64::MAX).unwrap();
	inner.remove_obsolete_wal_segments();

	assert_eq!(log_number(&tree), log_before, "the manifest did not move past the queued memtable");
	assert!(
		wal_segments(&live).contains(&queued_segment),
		"the segment {queued_segment} of the queued memtable was removed"
	);
	let image = dir.path().join("image");
	copy_dir(&live, &image);
	crash(&tree);
	let mut model = Model::new();
	model.insert(key(0), vec![1; 100]);
	assert_image_recovers(&image, false, &model, "image with a queued memtable").await;
}
