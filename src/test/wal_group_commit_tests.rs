//! Group commit must give every batch of a commit group its own WAL record.
//!
//! The commit flusher drains every accepted entry in the commit ring as one
//! group, writes the group to the WAL, syncs once, then applies and
//! acknowledges all of it. Recovery decodes exactly one batch per WAL record
//! (`Batch::decode` rejects bytes past the end of the batch), so a group goes
//! to the WAL as one record per batch, written in one append. A group that
//! lost a batch on the way (for example by encoding every batch over the one
//! before it in a shared buffer) would still apply and acknowledge every
//! batch, and nothing would be lost until an unclean stop, which is why only
//! a crash test can see it.
//!
//! Each test keeps a control, so that it cannot pass because nothing was
//! written (single-batch groups, or a clean restart).

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use crate::batch::Batch;
use crate::ring::PipelineHook;
use crate::vlog::ValueLocation;
use crate::wal::parallel_recovery::decode_segment_batches;
use crate::wal::{segment_name, BLOCK_SIZE, HEADER_SIZE};
use crate::{Durability, InternalKeyKind, Mode, Options, Tree};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

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

/// Every WAL record in `dir/wal`, decoded the way recovery decodes it (one
/// `Batch::decode` per record), in segment order.
fn wal_batches(dir: &Path) -> Vec<Batch> {
	let mut ids: Vec<(u64, PathBuf)> = Vec::new();
	for entry in std::fs::read_dir(dir.join("wal")).unwrap() {
		let path = entry.unwrap().path();
		if path.extension().is_some_and(|ext| ext == "wal") {
			let id: u64 = path.file_stem().unwrap().to_str().unwrap().parse().unwrap();
			ids.push((id, path));
		}
	}
	ids.sort();
	let mut out = Vec::new();
	for (id, path) in ids {
		out.extend(decode_segment_batches(&path, id).unwrap().1);
	}
	out
}

/// The segment `tree`'s commits go to: opening a tree continues in a fresh segment, so this is
/// not segment 0.
fn active_segment_path(dir: &Path, tree: &Tree) -> PathBuf {
	let id = tree.core.inner.wal.read().get_active_log_number();
	dir.join("wal").join(segment_name(id, "wal"))
}

fn options(path: &Path) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		// Crash semantics: nothing may be flushed on drop or close.
		flush_on_close: false,
		..Default::default()
	})
}

/// Stops a deliberately crashed or throwaway tree from spawning a best-effort
/// `close()` when it is dropped.
fn mark_closed(tree: &Tree) {
	tree.core.is_closed.store(true, Ordering::SeqCst);
}

fn release_lock(tree: &Tree) {
	tree.core.inner.lockfile.lock().unwrap().release().unwrap();
	tree.core.abort_background_tasks();
}

#[derive(Clone, Copy, Debug)]
enum Crash {
	/// Copy the live directory (every acknowledged commit has reached the OS,
	/// and with `Immediate` durability it has been fsynced) and open the copy.
	CopyWhileOpen,
	/// Release the lock file, drop the tree without `close()` and reopen the
	/// same directory. Must run on a current-thread runtime with no await
	/// between the drop and the reopen, so the spawned best-effort close of the
	/// dropped tree can never run.
	DropWithoutClose,
}

struct Outcome {
	acked: usize,
	missing: Vec<String>,
	/// WAL records found in the crashed directory.
	wal_records: usize,
}

fn key_of(writer: usize, i: usize) -> String {
	format!("key_{writer:03}_{i:05}")
}

/// Runs `writers` tasks that each commit `per_writer` single-key transactions,
/// then crashes and reopens. Single-key commits are one batch each, so with no
/// rotation the WAL must hold exactly one record per acknowledged commit.
async fn run(writers: usize, per_writer: usize, durability: Durability, crash: Crash) -> Outcome {
	let dir = TempDir::new("wal_group").unwrap();
	let path = dir.path().to_path_buf();
	let opts = options(&path);
	// Shared through an `Arc`, never `Tree::clone`: dropping any clone of a
	// `Tree` spawns `core.close()`.
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());

	let mut handles = Vec::new();
	for w in 0..writers {
		let tree = Arc::clone(&tree);
		handles.push(tokio::spawn(async move {
			let mut acked = Vec::new();
			for i in 0..per_writer {
				let key = key_of(w, i);
				let mut txn = tree.begin().unwrap();
				txn.set_durability(durability);
				txn.set(key.as_bytes(), key.as_bytes()).unwrap();
				if txn.commit().await.is_ok() {
					acked.push(key);
				}
			}
			acked
		}));
	}
	let mut acked: Vec<String> = Vec::new();
	for h in handles {
		acked.extend(h.await.unwrap());
	}

	// Before the crash the live store serves every acknowledged key, so any
	// loss below is a durability loss and not a visibility bug.
	{
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		for key in &acked {
			assert_eq!(
				txn.get(key.as_bytes()).unwrap().as_deref(),
				Some(key.as_bytes()),
				"{key} is acknowledged but not readable from the live tree"
			);
		}
	}

	let (reopen_opts, _copy_guard, crashed_dir) = match crash {
		Crash::CopyWhileOpen => {
			let copy = TempDir::new("wal_group_copy").unwrap();
			let copy_path = copy.path().join("db");
			copy_dir(&path, &copy_path);
			let crashed = copy_path.clone();
			(options(&copy_path), Some(copy), crashed)
		}
		Crash::DropWithoutClose => {
			release_lock(&tree);
			mark_closed(&tree);
			drop(tree);
			(Arc::clone(&opts), None, path.clone())
		}
	};

	let wal_records = wal_batches(&crashed_dir).len();

	let reopened = Tree::new(reopen_opts).unwrap();
	let missing = {
		let txn = reopened.begin_with_mode(Mode::ReadOnly).unwrap();
		acked
			.iter()
			.filter(|key| txn.get(key.as_bytes()).unwrap().as_deref() != Some(key.as_bytes()))
			.cloned()
			.collect::<Vec<_>>()
	};
	mark_closed(&reopened);
	release_lock(&reopened);

	Outcome {
		acked: acked.len(),
		missing,
		wal_records,
	}
}

fn assert_nothing_lost(label: &str, o: &Outcome) {
	assert!(o.acked > 0, "{label}: no commit was acknowledged");
	assert!(
		o.missing.is_empty(),
		"{label}: {} of {} acknowledged commits were lost after the unclean stop, first few: {:?}",
		o.missing.len(),
		o.acked,
		&o.missing[..o.missing.len().min(5)]
	);
	// The direct cause, and a check that the recovered keys did not come from
	// somewhere else: one record per single-key commit.
	assert_eq!(
		o.wal_records, o.acked,
		"{label}: every single-key commit is one batch and must have its own WAL record"
	);
}

// ---------------------------------------------------------------------------
// deterministic: drive flush_group directly
// ---------------------------------------------------------------------------

/// A raw (pre-vlog-separation) batch of `Set`s starting at the next sequence
/// numbers, exactly as `flush_entries` hands them to `flush_group`.
fn raw_batch(tree: &Tree, entries: &[(&str, &str)]) -> Batch {
	let pipeline = &tree.core.commit_pipeline;
	let start = pipeline.log_seq_num.fetch_add(entries.len() as u64, Ordering::SeqCst);
	let mut batch = Batch::new(start);
	for (k, v) in entries {
		batch
			.add_record(InternalKeyKind::Set, k.as_bytes().to_vec(), Some(v.as_bytes().to_vec()), 0)
			.unwrap();
	}
	batch
}

/// The decoded WAL entries as `(key, value)`, with the inline `ValueLocation`
/// framing the flusher applies before the WAL append removed.
fn entries_of(batch: &Batch) -> Vec<(String, String)> {
	batch
		.entries
		.iter()
		.map(|e| {
			let location = ValueLocation::decode(e.value.as_ref().unwrap()).unwrap();
			(String::from_utf8(e.key.clone()).unwrap(), String::from_utf8(location.value).unwrap())
		})
		.collect()
}

/// Why this is red if a batch of the group is not written: the decoder below finds fewer
/// records than batches, and the missing ones are absent from the segment. If the group were
/// written as one record holding all its batches, the decoder would reject it for the bytes
/// past the end of the first batch.
#[test(tokio::test)]
async fn flush_group_writes_one_wal_record_per_batch() {
	for sync in [true, false] {
		let dir = TempDir::new("wal_group").unwrap();
		let tree = Tree::new(options(dir.path())).unwrap();

		let batches = vec![
			raw_batch(&tree, &[("a1", "va1")]),
			raw_batch(&tree, &[("b1", "vb1"), ("b2", "vb2"), ("b3", "vb3")]),
			raw_batch(&tree, &[("c1", "vc1"), ("c2", "vc2")]),
			raw_batch(&tree, &[("d1", "vd1")]),
		];
		tree.core.commit_pipeline.flush_group(&batches, sync).await.unwrap();

		let wal = wal_batches(dir.path());
		assert_eq!(wal.len(), batches.len(), "sync={sync}: one WAL record per batch of the group");
		for (i, (decoded, sent)) in wal.iter().zip(&batches).enumerate() {
			assert_eq!(
				decoded.starting_seq_num, sent.starting_seq_num,
				"sync={sync}: batch {i} starting sequence number"
			);
			assert_eq!(decoded.count(), sent.count(), "sync={sync}: batch {i} entry count");
		}
		assert_eq!(entries_of(&wal[0]), [("a1".into(), "va1".into())]);
		assert_eq!(
			entries_of(&wal[1]),
			[("b1".into(), "vb1".into()), ("b2".into(), "vb2".into()), ("b3".into(), "vb3".into())]
		);
		assert_eq!(entries_of(&wal[2]), [("c1".into(), "vc1".into()), ("c2".into(), "vc2".into())]);
		assert_eq!(entries_of(&wal[3]), [("d1".into(), "vd1".into())]);

		// The memtable got the same group, so the live store and the WAL agree.
		let active = tree.core.inner.active_memtable.read().unwrap();
		for key in ["a1", "b1", "b2", "b3", "c1", "c2", "d1"] {
			assert!(active.get(key.as_bytes(), None).is_some(), "sync={sync}: {key} in memtable");
		}
		drop(active);

		mark_closed(&tree);
		release_lock(&tree);
	}
}

/// Control for the test above: a group of one batch is the simplest group, so this one passes
/// whatever happens to the others.
#[test(tokio::test)]
async fn flush_group_with_a_single_batch_writes_one_record() {
	let dir = TempDir::new("wal_group").unwrap();
	let tree = Tree::new(options(dir.path())).unwrap();

	let batches = vec![raw_batch(&tree, &[("only", "v")])];
	tree.core.commit_pipeline.flush_group(&batches, true).await.unwrap();

	let wal = wal_batches(dir.path());
	assert_eq!(wal.len(), 1);
	assert_eq!(entries_of(&wal[0]), [("only".into(), "v".into())]);

	mark_closed(&tree);
	release_lock(&tree);
}

/// Large values are separated to the value log before the WAL append, and the
/// value log is flushed and synced around it. Every batch of the group must
/// still get its own record, each carrying a pointer instead of the value, and
/// the values must read back after an unclean stop.
#[test(tokio::test)]
async fn flush_group_with_value_log_writes_one_record_per_batch() {
	let dir = TempDir::new("wal_group").unwrap();
	let opts = Arc::new(Options {
		path: dir.path().to_path_buf(),
		flush_on_close: false,
		enable_vlog: true,
		vlog_value_threshold: 16,
		..Default::default()
	});
	let tree = Tree::new(Arc::clone(&opts)).unwrap();

	let big = |c: char| c.to_string().repeat(200);
	let (va, vb, vc) = (big('a'), big('b'), big('c'));
	let batches = vec![
		raw_batch(&tree, &[("k1", va.as_str())]),
		raw_batch(&tree, &[("k2", "small"), ("k3", vb.as_str())]),
		raw_batch(&tree, &[("k4", vc.as_str())]),
	];
	tree.core.commit_pipeline.flush_group(&batches, true).await.unwrap();

	let wal = wal_batches(dir.path());
	assert_eq!(wal.len(), 3, "one WAL record per batch of the group");
	for (i, batch) in wal.iter().enumerate() {
		for entry in &batch.entries {
			let location = ValueLocation::decode(entry.value.as_ref().unwrap()).unwrap();
			let separated = entry.key != b"k2";
			assert_eq!(
				location.is_value_pointer(),
				separated,
				"batch {i} key {:?}: large values are pointers, small ones inline",
				String::from_utf8_lossy(&entry.key)
			);
		}
	}

	release_lock(&tree);
	mark_closed(&tree);
	drop(tree);
	let reopened = Tree::new(opts).unwrap();
	{
		let txn = reopened.begin_with_mode(Mode::ReadOnly).unwrap();
		for (key, value) in [("k1", &va), ("k2", &"small".to_string()), ("k3", &vb), ("k4", &vc)] {
			assert_eq!(
				txn.get(key.as_bytes()).unwrap().as_deref(),
				Some(value.as_bytes()),
				"{key} after recovery"
			);
		}
	}
	mark_closed(&reopened);
	release_lock(&reopened);
}

// ---------------------------------------------------------------------------
// deterministic group formation through the commit ring
// ---------------------------------------------------------------------------

/// On a current-thread runtime every spawned commit runs up to its first
/// pending await (the verdict), and so is accepted into the ring, before the
/// flusher task is polled. One round of K commits is therefore one group of K,
/// and with no rotation the WAL must hold K records for it.
async fn run_rounds(rounds: usize, group: usize, durability: Durability) -> Outcome {
	let dir = TempDir::new("wal_group").unwrap();
	let path = dir.path().to_path_buf();
	let opts = options(&path);
	let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());

	let mut acked: Vec<String> = Vec::new();
	for round in 0..rounds {
		let mut handles = Vec::new();
		for w in 0..group {
			let tree = Arc::clone(&tree);
			handles.push(tokio::spawn(async move {
				let key = key_of(round, w);
				let mut txn = tree.begin().unwrap();
				txn.set_durability(durability);
				txn.set(key.as_bytes(), key.as_bytes()).unwrap();
				txn.commit().await.map(|()| key)
			}));
		}
		for h in handles {
			acked.push(h.await.unwrap().unwrap());
		}
	}

	release_lock(&tree);
	mark_closed(&tree);
	drop(tree);
	let wal_records = wal_batches(&path).len();
	let reopened = Tree::new(opts).unwrap();
	let missing = {
		let txn = reopened.begin_with_mode(Mode::ReadOnly).unwrap();
		acked
			.iter()
			.filter(|key| txn.get(key.as_bytes()).unwrap().as_deref() != Some(key.as_bytes()))
			.cloned()
			.collect::<Vec<_>>()
	};
	mark_closed(&reopened);
	release_lock(&reopened);

	Outcome {
		acked: acked.len(),
		missing,
		wal_records,
	}
}

#[test(tokio::test)]
async fn one_flusher_pass_of_many_commits_survives_unclean_stop_immediate() {
	assert_nothing_lost(
		"rounds of 16, Immediate",
		&run_rounds(10, 16, Durability::Immediate).await,
	);
}

#[test(tokio::test)]
async fn one_flusher_pass_of_many_commits_survives_unclean_stop_eventual() {
	assert_nothing_lost("rounds of 16, Eventual", &run_rounds(10, 16, Durability::Eventual).await);
}

// ---------------------------------------------------------------------------
// end to end: concurrent writers, unclean stop
// ---------------------------------------------------------------------------

/// 64 writers x 50 single-key `Immediate` commits on a multi-threaded runtime,
/// crash by copying the directory while the tree is open.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn concurrent_immediate_commits_survive_copy_while_open() {
	let o = run(64, 50, Durability::Immediate, Crash::CopyWhileOpen).await;
	assert_eq!(o.acked, 64 * 50);
	assert_nothing_lost("64x50 Immediate, copy while open", &o);
}

/// Same workload, crash by dropping the tree without `close()`. Current-thread
/// runtime: the writers still interleave at every commit's verdict await.
#[test(tokio::test)]
async fn concurrent_immediate_commits_survive_drop_without_close() {
	let o = run(64, 50, Durability::Immediate, Crash::DropWithoutClose).await;
	assert_eq!(o.acked, 64 * 50);
	assert_nothing_lost("64x50 Immediate, drop without close", &o);
}

/// The default durability (`Eventual`: no fsync, but the WAL append still
/// reaches the OS), crash by dropping without `close()`. The group loss does
/// not depend on `sync`, so this must lose nothing either.
#[test(tokio::test)]
async fn concurrent_eventual_commits_survive_drop_without_close() {
	let o = run(64, 50, Durability::default(), Crash::DropWithoutClose).await;
	assert_eq!(o.acked, 64 * 50);
	assert_nothing_lost("64x50 Eventual, drop without close", &o);
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn concurrent_eventual_commits_survive_copy_while_open() {
	let o = run(64, 50, Durability::default(), Crash::CopyWhileOpen).await;
	assert_eq!(o.acked, 64 * 50);
	assert_nothing_lost("64x50 Eventual, copy while open", &o);
}

/// Control: one sequential writer means every group has exactly one batch. This passes
/// whether or not a group keeps every batch, which is what keeps the concurrent tests honest.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn single_writer_control_loses_nothing() {
	for crash in [Crash::CopyWhileOpen] {
		let o = run(1, 100, Durability::Immediate, crash).await;
		assert_eq!(o.acked, 100);
		assert_nothing_lost(&format!("1x100 Immediate, {crash:?}"), &o);
	}
}

#[test(tokio::test)]
async fn single_writer_control_loses_nothing_drop_without_close() {
	let o = run(1, 100, Durability::Immediate, Crash::DropWithoutClose).await;
	assert_eq!(o.acked, 100);
	assert_nothing_lost("1x100 Immediate, drop without close", &o);
}

/// How little concurrency is enough to lose commits: with two writers a group
/// can already hold two batches.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
async fn concurrency_sweep_loses_nothing() {
	for writers in [2usize, 4, 16] {
		let per_writer = 400 / writers;
		let o = run(writers, per_writer, Durability::Immediate, Crash::CopyWhileOpen).await;
		assert_eq!(o.acked, writers * per_writer);
		assert_nothing_lost(&format!("{writers} writers"), &o);
	}
}

// ---------------------------------------------------------------------------
// a group is one append
// ---------------------------------------------------------------------------

/// Commits one single-key transaction per entry, all spawned before the flusher is polled.
/// On the current-thread runtime that is deterministic: each spawned commit runs up to its
/// verdict await, so every one is accepted into the commit ring before the flusher task
/// runs, and the whole call is ONE group, in order.
async fn commit_group(
	tree: &Arc<Tree>,
	entries: &[(String, Vec<u8>)],
	durability: Durability,
) -> Vec<crate::Result<()>> {
	let mut handles = Vec::new();
	for (key, value) in entries {
		let (tree, key, value) = (Arc::clone(tree), key.clone(), value.clone());
		handles.push(tokio::spawn(async move {
			let mut txn = tree.begin().unwrap();
			txn.set_durability(durability);
			txn.set(key.as_bytes(), &value).unwrap();
			txn.commit().await
		}));
	}
	let mut out = Vec::new();
	for h in handles {
		out.push(h.await.unwrap());
	}
	out
}

/// Records the size of every group that reaches the end of the WAL step of `flush_group`.
fn observe_wal_groups(tree: &Tree) -> Arc<std::sync::Mutex<Vec<usize>>> {
	let sizes = Arc::new(std::sync::Mutex::new(Vec::new()));
	let sink = Arc::clone(&sizes);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::AfterWalSync {
			batches,
		} = point
		{
			sink.lock().unwrap().push(batches);
		}
	})));
	sizes
}

fn file_writes(tree: &Tree) -> usize {
	tree.core.inner.wal.read().file_writes()
}

/// Groups of every shape, appended in one call: each batch is one record, every record is
/// in the segment after an unclean stop with its value, and the group is one `AfterWalSync`
/// of exactly its batch count. The shapes cross the 32 KiB block and the write buffer in
/// every way a group can: many small batches, a few that each fill most of a block, and a
/// large batch between small ones.
#[test(tokio::test)]
async fn groups_of_every_shape_survive_an_unclean_stop() {
	let shapes: Vec<(&str, Vec<usize>)> = vec![
		("eight small", vec![8; 8]),
		("fifty small", vec![40; 50]),
		("five of 10 KB", vec![10_000; 5]),
		("near block size", vec![32_700, 32_760, 32_761, 32_762]),
		("large between small", vec![5, 100_000, 5, 40_000, 5]),
		("one large", vec![150_000]),
	];
	for (name, lens) in &shapes {
		for sync in [true, false] {
			let dir = TempDir::new("wal_group").unwrap();
			let opts = options(dir.path());
			let tree = Tree::new(Arc::clone(&opts)).unwrap();
			let sizes = observe_wal_groups(&tree);

			let values: Vec<(String, String)> = lens
				.iter()
				.enumerate()
				.map(|(i, len)| (format!("{name}-{i}"), format!("{i:03}").repeat(len / 3 + 1)))
				.collect();
			let batches: Vec<Batch> =
				values.iter().map(|(k, v)| raw_batch(&tree, &[(k.as_str(), v.as_str())])).collect();
			tree.core.commit_pipeline.flush_group(&batches, sync).await.unwrap();

			assert_eq!(*sizes.lock().unwrap(), [batches.len()], "{name}, sync={sync}: one group");
			let wal = wal_batches(dir.path());
			assert_eq!(wal.len(), batches.len(), "{name}, sync={sync}: one record per batch");
			for ((decoded, sent), (k, v)) in wal.iter().zip(&batches).zip(&values) {
				assert_eq!(decoded.starting_seq_num, sent.starting_seq_num, "{name}: sequence");
				assert_eq!(entries_of(decoded), [(k.clone(), v.clone())], "{name}: the record");
			}

			release_lock(&tree);
			mark_closed(&tree);
			drop(tree);
			let reopened = Tree::new(opts).unwrap();
			{
				let txn = reopened.begin_with_mode(Mode::ReadOnly).unwrap();
				for (k, v) in &values {
					assert_eq!(
						txn.get(k.as_bytes()).unwrap().as_deref(),
						Some(v.as_bytes()),
						"{name}, sync={sync}: {k} after the unclean stop"
					);
				}
			}
			mark_closed(&reopened);
			release_lock(&reopened);
		}
	}
}

/// A commit group of K transactions reaches the file in one `write(2)`, where one append per
/// batch took K. The group is one WAL record per batch, so the recovered store is the same.
#[test(tokio::test)]
async fn a_group_under_32_kib_is_one_write() {
	let dir = TempDir::new("wal_group").unwrap();
	let tree = Arc::new(Tree::new(options(dir.path())).unwrap());
	let sizes = observe_wal_groups(&tree);

	for (round, group) in [1usize, 2, 8, 16, 64].into_iter().enumerate() {
		for durability in [Durability::Immediate, Durability::Eventual] {
			let entries: Vec<(String, Vec<u8>)> =
				(0..group).map(|i| (key_of(round, i), vec![b'v'; 40])).collect();
			let before = file_writes(&tree);
			let results = commit_group(&tree, &entries, durability).await;
			assert!(results.iter().all(|r| r.is_ok()), "group of {group} {durability:?}");
			assert_eq!(
				sizes.lock().unwrap().last(),
				Some(&group),
				"the commits must have formed one group of {group}"
			);
			assert_eq!(
				file_writes(&tree) - before,
				1,
				"a group of {group} {durability:?} commits is one write"
			);
		}
	}
	assert_eq!(wal_batches(dir.path()).len(), 2 * (1 + 2 + 8 + 16 + 64));

	mark_closed(&tree);
	release_lock(&tree);
}

/// A group whose append fails fails as a whole: every waiter gets the error, nothing is
/// acknowledged or readable, and the segment is cut back to where the group started, at
/// every point the append can fail at. Later commits fail cleanly too (the writer is
/// poisoned until a rotation), and a restart recovers the acknowledged commits only.
#[test(tokio::test)]
async fn a_failing_group_aborts_every_waiter_and_acknowledges_nothing() {
	// Eight commits of one small batch each are 16 appends and a flush: 17 points to fail at,
	// and the group goes through when it is let fail after the 17th.
	for fail_after in 0..=17 {
		let dir = TempDir::new("wal_group").unwrap();
		let opts = options(dir.path());
		let tree = Arc::new(Tree::new(Arc::clone(&opts)).unwrap());
		let segment = active_segment_path(dir.path(), &tree);

		let acked: Vec<(String, Vec<u8>)> =
			(0..3).map(|i| (key_of(0, i), vec![b'a'; 40])).collect();
		for result in commit_group(&tree, &acked, Durability::Immediate).await {
			result.unwrap();
		}
		let acked_len = std::fs::metadata(&segment).unwrap().len();
		assert!(acked_len > 0, "the acknowledged commits are in the segment");

		let doomed: Vec<(String, Vec<u8>)> =
			(0..8).map(|i| (key_of(1, i), vec![b'd'; 40])).collect();
		tree.core.inner.wal.write().fail_writes_after(fail_after);
		let results = commit_group(&tree, &doomed, Durability::Immediate).await;
		if results.iter().all(|r| r.is_ok()) {
			// The control: past the last write the group is acknowledged and recovered.
			assert_eq!(fail_after, 17, "the group has 16 appends and a flush to fail at");
			assert_eq!(wal_batches(dir.path()).len(), acked.len() + doomed.len());
			mark_closed(&tree);
			release_lock(&tree);
			return;
		}
		assert!(
			results.iter().all(|r| r.is_err()),
			"fail_after={fail_after}: the group fails as a whole, got {results:?}"
		);
		assert_eq!(
			std::fs::metadata(&segment).unwrap().len(),
			acked_len,
			"fail_after={fail_after}"
		);
		{
			let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
			for (k, _) in &doomed {
				assert_eq!(txn.get(k.as_bytes()).unwrap(), None, "{k} was never acknowledged");
			}
		}
		let later =
			commit_group(&tree, &[(key_of(2, 0), vec![b'l'; 40])], Durability::Immediate).await;
		assert!(later[0].is_err(), "fail_after={fail_after}: the poisoned writer takes no more");
		assert_eq!(std::fs::metadata(&segment).unwrap().len(), acked_len);

		release_lock(&tree);
		mark_closed(&tree);
		drop(tree);
		let reopened = Tree::new(opts).unwrap();
		{
			let txn = reopened.begin_with_mode(Mode::ReadOnly).unwrap();
			for (k, v) in &acked {
				assert_eq!(txn.get(k.as_bytes()).unwrap().as_deref(), Some(v.as_slice()));
			}
			for (k, _) in doomed.iter().chain(&[(key_of(2, 0), vec![])]) {
				assert_eq!(txn.get(k.as_bytes()).unwrap(), None, "fail_after={fail_after}: {k}");
			}
		}
		mark_closed(&reopened);
		release_lock(&reopened);
	}
	panic!("the group never went through, so there is no control");
}

// ---------------------------------------------------------------------------
// the bytes in the segment
// ---------------------------------------------------------------------------

/// Where in its block a record of `len` bytes that starts at `offset` in its block ends,
/// framed the way the WAL writer frames it: a 7-byte header per fragment, and padding when
/// fewer than 7 bytes are left in a block.
fn block_offset_after(mut offset: usize, mut len: usize) -> usize {
	loop {
		if BLOCK_SIZE - offset < HEADER_SIZE {
			offset = 0;
		}
		let fragment = len.min(BLOCK_SIZE - offset - HEADER_SIZE);
		offset += HEADER_SIZE + fragment;
		len -= fragment;
		if len == 0 {
			return offset;
		}
	}
}

/// The value length at which a one-key batch, as `flush_group` logs it, ends with `left` bytes
/// to spare in its block when it is appended to a segment of `segment_len` bytes. At least
/// 20 000, so that every length has the same varint widths and the encoded size is the
/// length plus a constant, which one probe measures.
fn value_len_leaving(tree: &Tree, key: &str, segment_len: usize, left: usize) -> usize {
	let encoded_len = |len: usize| {
		let start = tree.core.commit_pipeline.log_seq_num.load(Ordering::SeqCst);
		let mut batch = Batch::new(start);
		let value = ValueLocation::with_inline_value(vec![b'p'; len]).encode();
		batch.add_record(InternalKeyKind::Set, key.as_bytes().to_vec(), Some(value), 0).unwrap();
		batch.encode().unwrap().len()
	};
	let overhead = encoded_len(20_000) - 20_000;
	let want = BLOCK_SIZE - left;
	(20_000..20_000 + 2 * BLOCK_SIZE)
		.find(|len| block_offset_after(segment_len % BLOCK_SIZE, len + overhead) == want)
		.expect("some length in two blocks' worth ends the record where asked")
}

/// Runs a fixed sequence of groups through `flush_group` and returns the length, CRC-32 and
/// XXH3 of the segment it wrote. The groups mix sizes on both sides of the 32 KiB block and
/// write buffer, so records are fragmented, padded and coalesced in every way a commit can.
async fn golden_segment_digest() -> (u64, u32, u64) {
	let dir = TempDir::new("wal_group").unwrap();
	let tree = Tree::new(options(dir.path())).unwrap();
	let segment = active_segment_path(dir.path(), &tree);
	let segment_len = || std::fs::metadata(&segment).unwrap().len() as usize;
	let value = |len: usize, c: char| c.to_string().repeat(len);

	let group = |sync: bool, batches: Vec<Batch>| {
		let pipeline = &tree.core.commit_pipeline;
		async move { pipeline.flush_group(&batches, sync).await.unwrap() }
	};

	group(true, vec![raw_batch(&tree, &[("a1", "va1")])]).await;
	group(
		false,
		vec![
			raw_batch(&tree, &[("b1", "vb1"), ("b2", "vb2")]),
			raw_batch(&tree, &[("c1", "vc1")]),
			raw_batch(&tree, &[("d1", "vd1"), ("d2", "vd2"), ("d3", "vd3")]),
		],
	)
	.await;
	let (big, mid, tiny) = (value(70_000, 'b'), value(32_700, 'm'), value(1, 't'));
	group(
		true,
		vec![
			raw_batch(&tree, &[("e1", big.as_str())]),
			raw_batch(&tree, &[("e2", tiny.as_str())]),
			raw_batch(&tree, &[("e3", mid.as_str())]),
			raw_batch(&tree, &[("e4", "ve4")]),
			raw_batch(&tree, &[("e5", mid.as_str()), ("e6", tiny.as_str())]),
		],
	)
	.await;
	let pair = value(40_000, 'p');
	group(
		false,
		vec![
			raw_batch(&tree, &[("f1", pair.as_str())]),
			raw_batch(&tree, &[("f2", pair.as_str())]),
		],
	)
	.await;

	// A record that ends 0 to 8 bytes short of the end of its block, followed by a group of
	// small ones: with fewer than 7 bytes left the group starts with padding, with 7 the
	// next record starts with an empty first fragment, and with 0 it starts a block. Each
	// is a lone record in its group, then the first of one.
	for left in [1, 2, 3, 4, 5, 6, 0, 7, 8] {
		let len = value_len_leaving(&tree, "leaver", segment_len(), left);
		let leaver = value(len, 'l');
		group(left % 2 == 0, vec![raw_batch(&tree, &[("leaver", leaver.as_str())])]).await;
		assert_eq!(
			segment_len() % BLOCK_SIZE,
			(BLOCK_SIZE - left) % BLOCK_SIZE,
			"the record must end {left} bytes short of the block"
		);
		group(
			left % 2 == 1,
			vec![
				raw_batch(&tree, &[("s1", "small")]),
				raw_batch(&tree, &[("s2", "small"), ("s3", "small")]),
				raw_batch(&tree, &[("s4", mid.as_str())]),
			],
		)
		.await;
		// Now as the first record of a group, followed by two more.
		let len = value_len_leaving(&tree, "leaver", segment_len(), left);
		let leaver = value(len, 'k');
		group(
			true,
			vec![
				raw_batch(&tree, &[("leaver", leaver.as_str())]),
				raw_batch(&tree, &[("after", "small")]),
				raw_batch(&tree, &[("after2", big.as_str())]),
			],
		)
		.await;
	}

	let bytes = std::fs::read(&segment).unwrap();
	let digest = (bytes.len() as u64, crc32fast::hash(&bytes), xxhash_rust::xxh3::xxh3_64(&bytes));
	mark_closed(&tree);
	release_lock(&tree);
	digest
}

/// The segment bytes of a fixed commit sequence, captured from the tree that appends the
/// records of a group one `Wal::append` at a time. Appending a group in one go must not
/// change a byte: same framing, same fragmentation, same padding, same CRCs.
#[test(tokio::test)]
async fn flush_group_segment_bytes_are_unchanged() {
	const GOLDEN: (u64, u32, u64) = (1_708_465, 2_612_805_897, 468_995_306_339_891_516);
	assert_eq!(golden_segment_digest().await, GOLDEN);
}
