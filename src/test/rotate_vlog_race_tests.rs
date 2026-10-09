//! Sealing a WAL segment only after the value log is synced, under races, failures and power loss.
//!
//! A rotation syncs the value log under the WAL lock, right before it fsyncs the segment. These
//! tests check what that rests on:
//!
//! * the dirty tracking of the value log: a sync settles only what it covered when it took its fds,
//!   never what was created while it ran, and a failed directory fsync settles nothing;
//! * the ordering at byte level: at every fsync of a segment, every value log pointer in the
//!   segment points to bytes that a completed fsync covered, with commits, rotations and Immediate
//!   groups racing, with and without a global affinitypool;
//! * a rotation that overtakes a group between its value log sync and its WAL append, or between
//!   its WAL append and its apply;
//! * a power loss cut at every step of random histories (Eventual and Immediate commits,
//!   `flush_wal`, rotations, checkpoints), where the image keeps each value log file only up to the
//!   length an fsync had covered and the active segment only up to its last fsync;
//! * failures: a rotation whose value log sync or directory fsync fails, from the flusher and from
//!   a caller.
//!
//! The byte-level checks use `vlog::SYNCED_VLOG_LENGTHS`, next to `vlog::SYNCED_VLOG_FILES`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tempfile::TempDir;
use test_log::test;

use crate::batch::Batch;
use crate::ring::PipelineHook;
use crate::vlog::{VLog, ValueLocation, ValuePointer, SYNCED_VLOG_FILES, SYNCED_VLOG_LENGTHS};
use crate::wal::reader::Reader;
use crate::wal::Error as WalError;
use crate::{Durability, Error, Mode, Options, Tree, VLogChecksumLevel};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

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

/// A value of `len` bytes that no other `index` shares.
fn payload(index: usize, len: usize) -> Vec<u8> {
	let mut x = (index as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
	(0..len)
		.map(|_| {
			x ^= x << 13;
			x ^= x >> 7;
			x ^= x << 17;
			x as u8
		})
		.collect()
}

fn value_of(seed: u8, len: usize) -> Vec<u8> {
	(0..len).map(|i| seed.wrapping_add((i % 251) as u8)).collect()
}

fn on_another_thread<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
	let (done, wait) = std::sync::mpsc::channel();
	std::thread::spawn(move || {
		let _ = done.send(f());
	});
	wait.recv_timeout(Duration::from_secs(30)).expect("the call did not return: a lock is held")
}

fn synced_under(dir: &Path) -> Vec<PathBuf> {
	SYNCED_VLOG_FILES.lock().iter().filter(|path| path.starts_with(dir)).cloned().collect()
}

fn fsyncs_of(dir: &Path, file: &Path) -> usize {
	synced_under(dir).iter().filter(|synced| *synced == file).count()
}

/// How many bytes of `file` a completed fsync is sure to have made durable.
fn durable_len(file: &Path) -> u64 {
	SYNCED_VLOG_LENGTHS
		.lock()
		.iter()
		.filter(|(path, _)| path == file)
		.map(|(_, len)| *len)
		.max()
		.unwrap_or(0)
}

fn vlog_files(live: &Path) -> Vec<PathBuf> {
	let mut files: Vec<PathBuf> = fs::read_dir(live.join("vlog"))
		.map(|rd| rd.map(|e| e.unwrap().path()).collect())
		.unwrap_or_default();
	files.sort();
	files
}

fn seg_path(dir: &Path, id: u64) -> PathBuf {
	dir.join("wal").join(format!("{id:020}.wal"))
}

fn segment_ids(dir: &Path) -> Vec<u64> {
	let mut ids: Vec<u64> = fs::read_dir(dir.join("wal"))
		.unwrap()
		.filter_map(|e| {
			let name = e.unwrap().file_name().to_string_lossy().to_string();
			name.strip_suffix(".wal").and_then(|s| s.parse().ok())
		})
		.collect();
	ids.sort();
	ids
}

fn copy_dir(src: &Path, dst: &Path) {
	fs::create_dir_all(dst).unwrap();
	for entry in fs::read_dir(src).unwrap() {
		let entry = entry.unwrap();
		if entry.file_name() == "LOCK" {
			continue;
		}
		let to = dst.join(entry.file_name());
		if entry.file_type().unwrap().is_dir() {
			copy_dir(&entry.path(), &to);
		} else {
			fs::copy(entry.path(), &to).unwrap();
		}
	}
}

fn set_len(path: &Path, len: u64) {
	fs::OpenOptions::new().write(true).open(path).unwrap().set_len(len).unwrap();
}

fn file_len(path: &Path) -> u64 {
	fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// Eventual commits of 40 KB into value log files of 64 KB: the third rolls the file over.
fn vlog_opts_with(path: &Path, max_memtable_size: usize) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		max_memtable_size,
		flush_on_close: false,
		enable_vlog: true,
		vlog_value_threshold: 100,
		vlog_max_file_size: 64 * 1024,
		memtable_stall_threshold: 100_000,
		level0_max_files: 10_000,
		l0_stall_threshold: 10_000,
		..Default::default()
	})
}

fn vlog_opts(path: &Path) -> Arc<Options> {
	vlog_opts_with(path, 100 * 1024 * 1024)
}

fn vlog_of(tree: &Tree) -> &Arc<VLog> {
	tree.core.inner.vlog.as_ref().expect("the tree has a value log")
}

fn active_no(tree: &Tree) -> u64 {
	tree.core.inner.wal.read().get_active_log_number()
}

fn mark_closed(tree: &Tree) {
	tree.core.is_closed.store(true, Ordering::SeqCst);
}

fn release_lock(tree: &Tree) {
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
}

/// Kills a tree: releases the lock file and drops it without `close()`.
fn crash(tree: Arc<Tree>) {
	let tree = Arc::try_unwrap(tree).ok().expect("no other owner");
	release_lock(&tree);
	mark_closed(&tree);
	drop(tree);
}

async fn stop_background_tasks(tree: &Tree) {
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;
}

fn get(tree: &Tree, key: &str) -> crate::Result<Option<Vec<u8>>> {
	tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key.as_bytes())
}

async fn try_put(
	tree: &Tree,
	key: &str,
	value: &[u8],
	durability: Durability,
) -> crate::Result<()> {
	let mut txn = tree.begin().unwrap();
	txn.set_durability(durability);
	txn.set(key.as_bytes(), value).unwrap();
	txn.commit().await
}

async fn put(tree: &Tree, key: &str, value: &[u8], durability: Durability) {
	try_put(tree, key, value, durability).await.unwrap();
}

/// A tree with Eventual commits of 40 KB across a vlog rollover (two files, fsynced by nobody),
/// its background tasks stopped.
async fn eventual_commits_across_a_rollover(live: &Path) -> Arc<Tree> {
	let tree = Arc::new(Tree::new(vlog_opts(live)).unwrap());
	stop_background_tasks(&tree).await;
	for i in 0..3 {
		put(&tree, &format!("e{i}"), &payload(i, 40_000), Durability::Eventual).await;
	}
	assert_eq!(vlog_files(live).len(), 2, "the third value rolled the file over");
	assert!(synced_under(live).is_empty(), "an Eventual commit fsyncs nothing");
	tree
}

fn eventual_values() -> Vec<(String, Vec<u8>)> {
	(0..3).map(|i| (format!("e{i}"), payload(i, 40_000))).collect()
}

/// Every record of a segment, decoded as a batch.
fn batches_of(path: &Path) -> Vec<Batch> {
	let Ok(file) = File::open(path) else {
		return Vec::new();
	};
	let mut reader = Reader::new(file);
	let mut out = Vec::new();
	loop {
		match reader.read() {
			Ok((record, _)) => out.push(Batch::decode(record).unwrap()),
			Err(WalError::IO(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => return out,
			Err(e) => panic!("{} must read end to end: {e}", path.display()),
		}
	}
}

/// How far into its file the entry a pointer points to goes: the header, the key, the value and
/// the checksum.
fn entry_end(pointer: &ValuePointer) -> u64 {
	pointer.offset + 8 + pointer.key_size as u64 + pointer.value_size as u64 + 4
}

/// The pointers in the records of `segment` whose entries no completed fsync of their value log
/// file covers: a record that a power loss could leave behind with its value gone. Also how many
/// pointers it checked, so that a check which found nothing to look at cannot pass for one that
/// found nothing wrong.
fn uncovered_pointers(vlog: &VLog, live: &Path, segment: u64) -> (Vec<String>, usize) {
	let mut bad = Vec::new();
	let mut checked = 0;
	for batch in batches_of(&seg_path(live, segment)) {
		for entry in &batch.entries {
			let Some(value) = entry.value.as_deref() else {
				continue;
			};
			let Some(payload) = ValueLocation::peek_pointer_payload(value) else {
				continue;
			};
			let pointer = ValuePointer::decode(payload).unwrap();
			checked += 1;
			let file = vlog.vlog_file_path(pointer.file_id);
			let (end, durable) = (entry_end(&pointer), durable_len(&file));
			if end > durable {
				bad.push(format!(
					"segment {segment}: key {} points to {}..{end} of {} and {durable} bytes are fsynced",
					String::from_utf8_lossy(&entry.key),
					pointer.offset,
					file.display()
				));
			}
		}
	}
	(bad, checked)
}

/// What the observers of the fsyncs of a segment saw.
#[derive(Default)]
struct Watch {
	violations: Mutex<Vec<String>>,
	fsyncs: AtomicUsize,
	/// The pointers the checks looked at, in all fsyncs.
	pointers: AtomicUsize,
}

impl Watch {
	/// No pointer was uncovered at an fsync, and some pointers were looked at.
	fn assert_ordered(&self) {
		let violations = self.violations.lock().unwrap();
		assert!(violations.is_empty(), "{violations:?}");
		assert!(self.fsyncs.load(Ordering::SeqCst) > 0, "the segment was never fsynced");
		assert!(
			self.pointers.load(Ordering::SeqCst) > 0,
			"the segments checked held no value log pointer: the check proved nothing"
		);
	}
}

/// Makes every fsync of the segment that is active now check, inside the fsync, that every
/// pointer in the segment is covered. Returns the segment.
fn watch_active_segment(tree: &Tree, live: &Path, watch: &Arc<Watch>) -> u64 {
	let inner = &tree.core.inner;
	let mut wal = inner.wal.write();
	let segment = wal.get_active_log_number();
	let (vlog, live, watch) = (Arc::clone(vlog_of(tree)), live.to_path_buf(), Arc::clone(watch));
	wal.set_sync_observer(Some(Arc::new(move || {
		let (bad, checked) = uncovered_pointers(&vlog, &live, segment);
		watch.fsyncs.fetch_add(1, Ordering::SeqCst);
		watch.pointers.fetch_add(checked, Ordering::SeqCst);
		if !bad.is_empty() {
			watch.violations.lock().unwrap().extend(bad);
		}
	})));
	segment
}

/// The image of `live` after a power loss that kept every value log file up to the length an
/// fsync had covered, and the active segment up to `wal_synced_len` bytes.
fn power_loss_image(live: &Path, image: &Path, active: u64, wal_synced_len: u64) {
	copy_dir(live, image);
	for file in vlog_files(live) {
		let in_image = image.join("vlog").join(file.file_name().unwrap());
		set_len(&in_image, durable_len(&file).min(file_len(&in_image)));
	}
	let segment = seg_path(image, active);
	if segment.exists() {
		set_len(&segment, wal_synced_len.min(file_len(&segment)));
	}
}

/// Opens an image and compares every key of `universe` with `expected`.
fn assert_recovers(
	image: &Path,
	universe: &BTreeSet<String>,
	expected: &BTreeMap<String, Vec<u8>>,
) {
	let recovered = Tree::new(vlog_opts(image)).unwrap();
	for key in universe {
		let got = get(&recovered, key).unwrap_or_else(|e| panic!("{key}: {e}"));
		assert_eq!(got.as_ref(), expected.get(key), "{key} in the recovered image");
	}
	mark_closed(&recovered);
	release_lock(&recovered);
}

/// Restores a directory's permissions when dropped, so a failed test leaves it removable.
struct Locked(PathBuf);

#[cfg(unix)]
impl Locked {
	/// Takes every permission off `dir`, or returns `None` if that does not keep this process out
	/// of it (it runs as root).
	fn new(dir: &Path) -> Option<Self> {
		use std::os::unix::fs::PermissionsExt;
		fs::set_permissions(dir, fs::Permissions::from_mode(0o000)).unwrap();
		let locked = Self(dir.to_path_buf());
		File::open(dir).is_err().then_some(locked)
	}
}

#[cfg(unix)]
impl Drop for Locked {
	fn drop(&mut self) {
		use std::os::unix::fs::PermissionsExt;
		let _ = fs::set_permissions(&self.0, fs::Permissions::from_mode(0o755));
	}
}

// ---------------------------------------------------------------------------
// the dirty tracking of the value log
// ---------------------------------------------------------------------------

fn small_vlog() -> (Arc<VLog>, TempDir) {
	let dir = TempDir::new().unwrap();
	let opts = Arc::new(Options {
		path: dir.path().to_path_buf(),
		vlog_max_file_size: 1024,
		vlog_checksum_verification: VLogChecksumLevel::Full,
		..Default::default()
	});
	std::fs::create_dir_all(opts.vlog_dir()).unwrap();
	(Arc::new(VLog::new(opts).unwrap()), dir)
}

/// A file created while a sync runs is not covered by it, nor is its name: the next `sync_dirty`
/// fsyncs the file and the directory.
#[test(tokio::test)]
async fn a_file_created_during_a_sync_is_dirty_after_it() {
	let (vlog, dir) = small_vlog();
	let first = vlog.append(b"key-0", &value_of(0, 600)).unwrap();
	let old = vlog.vlog_file_path(first.file_id);
	let base = vlog.dir_syncs();

	let appender = Arc::downgrade(&vlog);
	vlog.set_sync_gap(Some(Arc::new(move || {
		let appender = appender.upgrade().unwrap();
		on_another_thread(move || {
			appender.append(b"key-1", &value_of(1, 600)).unwrap();
			appender.append(b"key-2", &value_of(2, 600)).unwrap();
		});
	})));
	vlog.sync().unwrap();
	vlog.set_sync_gap(None);
	assert_eq!(vlog.dir_syncs() - base, 1, "the sync covered the first file's name");
	let new = vlog.vlog_file_path(2);
	assert!(new.exists(), "the gap rolled the file over");
	assert_eq!(fsyncs_of(dir.path(), &new), 0);

	vlog.sync_dirty().unwrap();
	assert_eq!(fsyncs_of(dir.path(), &new), 1, "the file created during the sync was fsynced");
	assert!(fsyncs_of(dir.path(), &old) >= 2, "the file it rolled over from was fsynced again");
	assert_eq!(vlog.dir_syncs() - base, 2, "and so was the directory, for the new name");
}

/// A directory fsync that fails fails the sync and settles nothing: the next `sync_dirty`
/// fsyncs the files and the directory again.
#[cfg(unix)]
#[test(tokio::test)]
async fn a_failed_directory_fsync_leaves_the_log_dirty() {
	let (vlog, dir) = small_vlog();
	let first = vlog.append(b"key-0", &value_of(0, 100)).unwrap();
	let file = vlog.vlog_file_path(first.file_id);
	let base = vlog.dir_syncs();

	{
		let Some(_locked) = Locked::new(&dir.path().join("vlog")) else {
			return;
		};
		assert!(vlog.sync().is_err(), "the directory cannot be fsynced");
		assert!(vlog.sync_dirty().is_err(), "and the log is dirty: it tries again");
	}
	assert_eq!(vlog.dir_syncs() - base, 0);
	let before = fsyncs_of(dir.path(), &file);
	vlog.sync_dirty().unwrap();
	assert_eq!(vlog.dir_syncs() - base, 1, "the directory is fsynced once it can be");
	assert_eq!(fsyncs_of(dir.path(), &file), before + 1);
	vlog.sync_dirty().unwrap();
	assert_eq!(fsyncs_of(dir.path(), &file), before + 1, "and nothing is dirty after that");
}

// ---------------------------------------------------------------------------
// failures
// ---------------------------------------------------------------------------

/// A directory fsync that fails during a rotation fails the rotation and nothing else: the WAL
/// keeps its segment and bytes and is not poisoned, the memtable keeps its commits and its tag.
/// Once the directory can be fsynced the rotation succeeds, and a power loss image recovers
/// every value.
#[cfg(unix)]
#[test(tokio::test)]
async fn a_failed_directory_fsync_fails_the_rotation_and_leaves_the_wal_as_it_was() {
	let dir = TempDir::new().unwrap();
	let live = dir.path().join("live");
	let tree = eventual_commits_across_a_rollover(&live).await;
	let inner = Arc::clone(&tree.core.inner);
	let watch = Arc::new(Watch::default());
	// The segment the commits went to: a tree opens in a fresh segment, so it is not segment 0.
	let first = watch_active_segment(&tree, &live, &watch);
	let segment = seg_path(&live, first);
	let logged = fs::read(&segment).unwrap();
	assert!(!logged.is_empty(), "the commits are in the active segment");
	let segments = segment_ids(&live);

	{
		let Some(_locked) = Locked::new(&live.join("vlog")) else {
			crash(tree);
			return;
		};
		assert!(inner.rotate_memtable().is_err(), "the vlog directory cannot be fsynced");
		assert_eq!(watch.fsyncs.load(Ordering::SeqCst), 0, "the segment was fsynced anyway");
		assert_eq!(inner.wal.read().get_active_log_number(), first);
		assert_eq!(segment_ids(&live), segments, "no segment was created");
		assert_eq!(fs::read(&segment).unwrap(), logged);
		assert!(inner.wal.read().pending_sync());
		assert!(!inner.wal.read().sync_failed(), "a vlog failure does not poison the WAL");
		let active = inner.active_memtable.read().unwrap();
		assert_eq!(active.get_wal_number(), first);
		assert!(!active.is_empty());
	}

	// Commits go on in the same segment; the rotation after the failure fsyncs, in order.
	put(&tree, "after", &payload(11, 40_000), Durability::Eventual).await;
	inner.rotate_memtable().unwrap();
	assert_eq!(watch.fsyncs.load(Ordering::SeqCst), 1);
	watch.assert_ordered();
	assert_eq!(active_no(&tree), first + 1);

	let mut expected: BTreeMap<String, Vec<u8>> = eventual_values().into_iter().collect();
	expected.insert("after".to_string(), payload(11, 40_000));
	let universe: BTreeSet<String> = expected.keys().cloned().collect();
	let image = dir.path().join("image");
	power_loss_image(&live, &image, first + 1, 0);
	assert_recovers(&image, &universe, &expected);
	drop(inner);
	crash(tree);
}

/// The rotation the flusher makes when the next batch does not fit the memtable fails with the
/// commit that needs it, when the vlog fsync fails: the commit is not logged and not applied,
/// the WAL is not poisoned, and the same commit goes through on retry.
#[test(tokio::test)]
async fn a_commit_whose_rotation_cannot_fsync_the_vlog_fails_whole_and_is_not_logged() {
	let dir = TempDir::new().unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(vlog_opts_with(&live, 16 * 1024)).unwrap());
	stop_background_tasks(&tree).await;
	let watch = Arc::new(Watch::default());
	let first = watch_active_segment(&tree, &live, &watch);

	// Eventual commits fsync nothing, so the first fsync of the vlog is the rotation's.
	vlog_of(&tree).fail_next_sync();
	let mut acked = Vec::new();
	let mut failed = None;
	for i in 0..5_000usize {
		let (key, value) = (format!("size{i:05}"), payload(i, 300));
		match try_put(&tree, &key, &value, Durability::Eventual).await {
			Ok(()) => acked.push((key, value)),
			Err(_) => {
				failed = Some((key, value));
				break;
			}
		}
	}
	let (failed_key, failed_value) = failed.expect("the rotation never failed");
	assert!(!acked.is_empty());
	assert_eq!(get(&tree, &failed_key).unwrap(), None, "a failed commit is visible");
	assert_eq!(active_no(&tree), first);
	assert!(!tree.core.inner.wal.read().sync_failed());
	assert_eq!(watch.fsyncs.load(Ordering::SeqCst), 0, "the segment was fsynced");
	let logged: Vec<String> = batches_of(&seg_path(&live, first))
		.iter()
		.flat_map(|b| b.entries.iter().map(|e| String::from_utf8_lossy(&e.key).to_string()))
		.collect();
	assert!(
		acked.iter().all(|(key, _)| logged.contains(key)),
		"the segment holds what was acknowledged: {logged:?}"
	);
	assert!(!logged.contains(&failed_key), "the failed commit was logged: {logged:?}");

	put(&tree, &failed_key, &failed_value, Durability::Eventual).await;
	assert_eq!(active_no(&tree), first + 1, "the retry rotated");
	assert_eq!(get(&tree, &failed_key).unwrap(), Some(failed_value.clone()));
	for (key, value) in &acked {
		assert_eq!(get(&tree, key).unwrap().as_ref(), Some(value), "{key}");
	}
	watch.assert_ordered();
	assert_eq!(watch.fsyncs.load(Ordering::SeqCst), 1);

	// Everything acknowledged before the rotation is in the sealed segment, with its values.
	let expected: BTreeMap<String, Vec<u8>> = acked.iter().cloned().collect();
	let universe: BTreeSet<String> = expected.keys().cloned().chain([failed_key.clone()]).collect();
	let image = dir.path().join("image");
	power_loss_image(&live, &image, first + 1, 0);
	assert_recovers(&image, &universe, &expected);
	crash(tree);
}

/// The rotation in the middle of a group, once part of the group was applied, cannot fsync the
/// value log: the group stops the database like any other that fails then, with the cause in the
/// error. Nothing is published past it, a later commit fails, and a crash image recovers the whole
/// group, which was logged before any of it was applied.
#[test(tokio::test)]
async fn a_failed_vlog_fsync_in_a_rotation_after_part_of_a_group_was_applied_stops_the_database() {
	let dir = TempDir::new().unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(vlog_opts_with(&live, 4 * 1024)).unwrap());
	stop_background_tasks(&tree).await;
	put(&tree, "before", &payload(0, 300), Durability::Eventual).await;
	let visible = tree.core.seq_num();

	// Eventual commits fsync nothing, so the first fsync of the value log is the rotation's.
	vlog_of(&tree).fail_next_sync();
	let group: Vec<(String, Vec<u8>)> =
		(0..60).map(|i| (format!("group{i:02}"), payload(i + 1, 300))).collect();
	let mut handles = Vec::new();
	for (key, value) in &group {
		let (tree, key, value) = (Arc::clone(&tree), key.clone(), value.clone());
		handles.push(tokio::spawn(async move {
			try_put(&tree, &key, &value, Durability::Eventual).await
		}));
	}
	let mut errors = Vec::new();
	for handle in handles {
		match handle.await.unwrap() {
			Err(Error::DatabaseStopped(message)) => errors.push(message),
			other => panic!("expected the database to be stopped, got {other:?}"),
		}
	}
	assert!(
		errors[0].contains("injected fsync failure"),
		"the cause is the value log fsync: {}",
		errors[0]
	);
	assert!(errors.windows(2).all(|w| w[0] == w[1]), "one error for the whole group");

	assert_eq!(tree.core.seq_num(), visible, "nothing was published past the group");
	assert_eq!(get(&tree, "before").unwrap(), Some(payload(0, 300)));
	for (key, _) in &group {
		assert_eq!(get(&tree, key).unwrap(), None, "{key} is visible");
	}
	assert!(matches!(
		try_put(&tree, "after", &payload(99, 300), Durability::Eventual).await,
		Err(Error::DatabaseStopped(_))
	));

	let image = dir.path().join("image");
	copy_dir(&live, &image);
	let universe: BTreeSet<String> = group.iter().map(|(key, _)| key.clone()).collect();
	let expected: BTreeMap<String, Vec<u8>> = group.iter().cloned().collect();
	assert_recovers(&image, &universe, &expected);
	crash(tree);
}

// ---------------------------------------------------------------------------
// interleavings
// ---------------------------------------------------------------------------

/// A rotation that comes between the WAL append of an Eventual group and its apply (the group is
/// stale and logged again): the segment it seals holds the group's record, so the group's values
/// are fsynced before it.
#[test(tokio::test)]
async fn a_rotation_between_a_groups_append_and_its_apply_fsyncs_its_values_first() {
	let dir = TempDir::new().unwrap();
	let live = dir.path().join("live");
	let tree = eventual_commits_across_a_rollover(&live).await;
	let inner = Arc::clone(&tree.core.inner);
	let watch = Arc::new(Watch::default());
	let sealed = watch_active_segment(&tree, &live, &watch);

	let rotated = Arc::new(AtomicBool::new(false));
	{
		let (inner, rotated) = (Arc::clone(&inner), Arc::clone(&rotated));
		tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
			if let PipelineHook::AfterWalSync {
				..
			} = point
			{
				if !rotated.swap(true, Ordering::SeqCst) {
					inner.rotate_memtable().unwrap();
				}
			}
		})));
	}
	put(&tree, "late", &payload(20, 40_000), Durability::Eventual).await;
	tree.core.commit_pipeline.set_hook(None);

	assert!(rotated.load(Ordering::SeqCst));
	assert_eq!(active_no(&tree), sealed + 1);
	assert_eq!(watch.fsyncs.load(Ordering::SeqCst), 1);
	watch.assert_ordered();
	assert_eq!(get(&tree, "late").unwrap(), Some(payload(20, 40_000)));
	let logged: Vec<String> = batches_of(&seg_path(&live, sealed))
		.iter()
		.flat_map(|b| b.entries.iter().map(|e| String::from_utf8_lossy(&e.key).to_string()))
		.collect();
	assert!(
		logged.contains(&"late".to_string()),
		"the sealed segment holds the record: {logged:?}"
	);
	drop(inner);
	crash(tree);
}

/// A rotation that comes while an Immediate group's value log sync is between taking its fds and
/// fsyncing them, before the group has appended anything: the rotation fsyncs the values itself
/// (the sync in flight covers nothing yet), the group goes on to the new segment and is
/// acknowledged, and a power loss image recovers it with everything before.
#[test(tokio::test)]
async fn a_rotation_during_an_immediate_groups_vlog_sync_orders_vlog_before_wal() {
	let dir = TempDir::new().unwrap();
	let live = dir.path().join("live");
	let tree = eventual_commits_across_a_rollover(&live).await;
	let inner = Arc::clone(&tree.core.inner);
	let watch = Arc::new(Watch::default());
	let sealed = watch_active_segment(&tree, &live, &watch);

	let entered = Arc::new(AtomicBool::new(false));
	{
		let inner = Arc::clone(&inner);
		vlog_of(&tree).set_sync_gap(Some(Arc::new(move || {
			if entered.swap(true, Ordering::SeqCst) {
				return;
			}
			let inner = Arc::clone(&inner);
			on_another_thread(move || inner.rotate_memtable().unwrap());
		})));
	}
	put(&tree, "immediate", &payload(30, 10_000), Durability::Immediate).await;
	vlog_of(&tree).set_sync_gap(None);

	assert_eq!(active_no(&tree), sealed + 1, "the rotation ran");
	assert_eq!(watch.fsyncs.load(Ordering::SeqCst), 1);
	watch.assert_ordered();
	let mut expected: BTreeMap<String, Vec<u8>> = eventual_values().into_iter().collect();
	expected.insert("immediate".to_string(), payload(30, 10_000));
	let universe: BTreeSet<String> = expected.keys().cloned().collect();
	let image = dir.path().join("image");
	power_loss_image(&live, &image, active_no(&tree), file_len(&seg_path(&live, sealed + 1)));
	assert_recovers(&image, &universe, &expected);
	drop(inner);
	crash(tree);
}

// ---------------------------------------------------------------------------
// a power loss cut at every step of random histories
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
enum Op {
	Eventual,
	Immediate,
	FlushWal(bool),
	Rotate,
	Checkpoint,
}

const HISTORY_KEYS: usize = 7;

/// A random history, single-threaded so that what is durable after each step is known: a commit
/// is durable once the WAL was fsynced after it (an Immediate commit, `flush_wal(true)`, or the
/// rotation that seals its segment, alone or as a checkpoint's). After every step a power loss
/// image keeps the value log files up to the length an fsync covered and the active segment up
/// to its last fsync, and it recovers exactly the durable state, every value readable.
async fn history(seed: u64, steps: usize) {
	let dir = TempDir::new().unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(vlog_opts(&live)).unwrap());
	stop_background_tasks(&tree).await;
	let mut rng = Rng(seed);
	let universe: BTreeSet<String> = (0..HISTORY_KEYS).map(|i| format!("k{i}")).collect();
	let mut model: BTreeMap<String, Vec<u8>> = BTreeMap::new();
	let mut durable = model.clone();
	let mut wal_synced_len = 0;
	let mut rotations = 0;

	for step in 0..steps {
		let op = match rng.below(100) {
			0..=34 => Op::Eventual,
			35..=46 => Op::Immediate,
			47..=54 => Op::FlushWal(true),
			55..=59 => Op::FlushWal(false),
			60..=84 => Op::Rotate,
			_ => Op::Checkpoint,
		};
		let before = active_no(&tree);
		let mut commit = |durability| {
			let key = format!("k{}", rng.below(HISTORY_KEYS));
			let len = [50, 300, 5_000, 40_000][rng.below(4)];
			(key, payload(seed as usize * 1_000 + step, len), durability)
		};
		match op {
			Op::Eventual | Op::Immediate => {
				let (key, value, durability) = commit(if matches!(op, Op::Eventual) {
					Durability::Eventual
				} else {
					Durability::Immediate
				});
				put(&tree, &key, &value, durability).await;
				model.insert(key, value);
				if durability == Durability::Immediate {
					durable = model.clone();
					wal_synced_len = file_len(&seg_path(&live, active_no(&tree)));
				}
			}
			Op::FlushWal(sync) => {
				tree.flush_wal(sync).unwrap();
				if sync {
					durable = model.clone();
					wal_synced_len = file_len(&seg_path(&live, active_no(&tree)));
				}
			}
			Op::Rotate => tree.core.inner.rotate_memtable().unwrap(),
			Op::Checkpoint => {
				tree.create_checkpoint(dir.path().join(format!("checkpoint{step}"))).unwrap();
			}
		}
		if active_no(&tree) != before {
			// The rotation fsynced the segment it sealed, and everything in it.
			rotations += 1;
			durable = model.clone();
			wal_synced_len = file_len(&seg_path(&live, active_no(&tree)));
		}

		for (key, value) in &model {
			assert_eq!(
				get(&tree, key).unwrap().as_ref(),
				Some(value),
				"seed {seed} step {step}: {key} live"
			);
		}
		let image = dir.path().join(format!("image{step}"));
		power_loss_image(&live, &image, active_no(&tree), wal_synced_len);
		let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
			assert_recovers(&image, &universe, &durable)
		}));
		if let Err(e) = outcome {
			let message = e
				.downcast_ref::<String>()
				.cloned()
				.or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
				.unwrap_or_default();
			panic!("seed {seed} step {step} after {op:?}: {message}");
		}
		fs::remove_dir_all(&image).unwrap();
	}
	assert!(rotations > 0, "seed {seed} never rotated");
	crash(tree);
}

#[test(tokio::test)]
#[ignore = "slow: reopens a copy of the tree after every step of four histories"]
async fn a_power_loss_at_any_step_of_a_random_history_recovers_the_durable_state() {
	for seed in 1..=4u64 {
		history(0x5EED_0000 + seed, 14).await;
	}
}

// ---------------------------------------------------------------------------
// racing commits and rotations
// ---------------------------------------------------------------------------

const POOL_CHILD: &str = "SKV_ROTATE_RACE_POOL_CHILD";

/// Writers (mostly Eventual, some Immediate, values of 120 B to 3 KB into value log files of
/// 64 KB), a thread that rotates the memtable over and over, and memtables small enough that the
/// flusher rotates too, all at once on a multi-threaded runtime. Every fsync of a segment checks,
/// inside the fsync, that each value log pointer in the segment is covered by a completed fsync
/// of its file. No hook runs between the checks: the interleavings are the ones the scheduler
/// makes. The writers go on until the rotating thread has sealed enough segments, however fast
/// the disk is.
async fn racing_commits_and_rotations() {
	const SEALED: usize = 10;
	let dir = TempDir::new().unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(vlog_opts_with(&live, 16 * 1024)).unwrap());
	stop_background_tasks(&tree).await;
	let watch = Arc::new(Watch::default());
	let stop = Arc::new(AtomicBool::new(false));

	let rotator = {
		let (tree, live, watch, stop) =
			(Arc::clone(&tree), live.clone(), Arc::clone(&watch), Arc::clone(&stop));
		tokio::task::spawn_blocking(move || {
			let mut armed = watch_active_segment(&tree, &live, &watch);
			let mut sealed = 0;
			while sealed < SEALED {
				std::thread::sleep(Duration::from_millis(2));
				let before = active_no(&tree);
				tree.core.inner.rotate_memtable().unwrap();
				if active_no(&tree) != before {
					sealed += 1;
				}
				// The segment just made, or one the flusher made: watch it.
				if active_no(&tree) != armed {
					armed = watch_active_segment(&tree, &live, &watch);
				}
			}
			stop.store(true, Ordering::SeqCst);
		})
	};

	let writers: Vec<_> = (0..3usize)
		.map(|w| {
			let (tree, stop) = (Arc::clone(&tree), Arc::clone(&stop));
			tokio::spawn(async move {
				let mut rng = Rng(0xFACE + w as u64);
				let mut acked = Vec::new();
				let mut n = 0usize;
				while !stop.load(Ordering::SeqCst) && n < 20_000 {
					let (key, value) =
						(format!("w{w}_{n}"), payload(w * 100_000 + n, 120 + rng.below(2_900)));
					let durability = if rng.below(7) == 0 {
						Durability::Immediate
					} else {
						Durability::Eventual
					};
					put(&tree, &key, &value, durability).await;
					acked.push((key, value));
					n += 1;
				}
				acked
			})
		})
		.collect();
	let mut acked = Vec::new();
	let all = async {
		for writer in writers {
			acked.extend(writer.await.unwrap());
		}
	};
	tokio::time::timeout(Duration::from_secs(300), all)
		.await
		.expect("the writers did not finish: a lock is held");
	rotator.await.unwrap();

	watch.assert_ordered();
	let fsyncs = watch.fsyncs.load(Ordering::SeqCst);
	assert!(fsyncs >= SEALED / 2, "the test must race rotations: {fsyncs} fsyncs were checked");
	for (key, value) in &acked {
		assert_eq!(get(&tree, key).unwrap().as_ref(), Some(value), "{key}");
	}

	// Everything acknowledged is durable after a final sync, with its values.
	tree.flush_wal(true).unwrap();
	let expected: BTreeMap<String, Vec<u8>> = acked.iter().cloned().collect();
	let universe: BTreeSet<String> = expected.keys().cloned().collect();
	let image = dir.path().join("image");
	power_loss_image(&live, &image, active_no(&tree), file_len(&seg_path(&live, active_no(&tree))));
	assert_recovers(&image, &universe, &expected);
	crash(tree);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_commits_and_rotations_never_seal_a_record_ahead_of_its_value() {
	if std::env::var_os(POOL_CHILD).is_none() {
		racing_commits_and_rotations().await;
	}
}

/// The same in a process of its own with a global affinitypool, as a server has: the WAL appends
/// and fsyncs run on pool threads while rotations run inline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn child_racing_commits_and_rotations_with_a_global_pool() {
	if std::env::var_os(POOL_CHILD).is_some() {
		affinitypool::Builder::new().worker_threads(2).build().build_global().unwrap();
		racing_commits_and_rotations().await;
	}
}

#[test]
fn racing_commits_and_rotations_with_a_global_pool() {
	if std::env::var_os(POOL_CHILD).is_some() {
		return;
	}
	let out = std::process::Command::new(std::env::current_exe().unwrap())
		.args([
			"--exact",
			"test::rotate_vlog_race_tests::child_racing_commits_and_rotations_with_a_global_pool",
			"--test-threads=1",
		])
		.env(POOL_CHILD, "1")
		.output()
		.unwrap();
	let stdout = String::from_utf8_lossy(&out.stdout);
	let stderr = String::from_utf8_lossy(&out.stderr);
	assert!(out.status.success(), "child failed:\n{stdout}\n{stderr}");
	assert!(stdout.contains("1 passed"), "the child must have run:\n{stdout}");
}
