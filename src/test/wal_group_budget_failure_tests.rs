//! A commit group whose WAL buffer the allocator refuses, and the estimate that buffer is sized
//! from.
//!
//! The flusher reserves the WAL buffer of a group with `try_reserve`, so a refused allocation
//! fails the group's commits with an error, instead of aborting the process. The reservation is
//! made before anything of the group is appended to the WAL: the segment is byte-identical, the
//! writer is not poisoned, and the commits that follow go through. The step that appends part
//! of a group again after a rotation reserves nothing, because the group is already partly
//! applied by then and failing it would leave that part visible. A randomised workload checks
//! all of it under groups that the byte budget cuts, with refusals injected. The helpers are
//! those of `wal_group_budget_tests`.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use tempdir::TempDir;
use test_log::test;

use super::wal_group_budget_tests::{
	assert_recovers,
	assert_ring_settled,
	commit,
	commit_with,
	hold_first_group,
	mark_closed,
	options,
	outcome,
	queue_in_order,
	recover_keys,
	release_lock,
	until,
	value_of,
	wal_bytes,
	MIB,
	WATCHDOG,
};
use crate::batch::Batch;
use crate::ring::{PipelineHook, COMMIT_RING_CAPACITY};
use crate::{Durability, Error, InternalKeyKind, Mode, Options, Tree};

// ---------------------------------------------------------------------------
// an allocation the allocator refuses
// ---------------------------------------------------------------------------

/// The WAL buffer of a group is reserved with `try_reserve`: when the allocator refuses, the
/// group's commits fail with an error, nothing reaches the segment, and the flusher goes on. The
/// refusal is injected for every buffer of at least 1 MiB.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_wal_buffer_the_allocator_refuses_fails_the_group_and_leaves_the_segment_alone() {
	let dir = TempDir::new("wal_group_budget").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(options(&live, false)).unwrap());
	let pipeline = &tree.core.commit_pipeline;
	let flusher = hold_first_group(&tree);

	let kept = ("kept".to_string(), value_of(0, 200));
	let gate = commit(&tree, &kept.0, kept.1.clone());
	until("the flusher to hold its first group", || flusher.is_held()).await;
	let segment_before = wal_bytes(&live);
	assert!(
		segment_before.values().any(|bytes| !bytes.is_empty()),
		"control: the segment holds a record"
	);

	pipeline.set_wal_reserve_fails_from(MIB);

	// One group of three commits of 600 KiB: its buffer is about 1.8 MiB.
	let doomed: Vec<(String, Vec<u8>)> =
		(0..3).map(|i| (format!("doomed_{i}"), value_of(i + 1, 600 * 1024))).collect();
	let handles = queue_in_order(&tree, &doomed, 1).await;
	flusher.release();
	outcome(gate).await.unwrap();
	for handle in handles {
		let result = outcome(handle).await;
		assert!(result.is_err(), "the group fails as a whole, got {result:?}");
	}
	assert_eq!(flusher.groups(), [1], "the failed group never reached its WAL step");
	assert_eq!(wal_bytes(&live), segment_before, "the segment is byte-identical");
	{
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		for (key, _) in &doomed {
			assert_eq!(txn.get(key.as_bytes()).unwrap(), None, "{key} was not acknowledged");
		}
	}
	assert_ring_settled(&tree, 4, "failed group").await;

	// A single commit over the limit fails the same way.
	let alone = outcome(commit(&tree, "alone", value_of(9, 2 * MIB))).await;
	assert!(alone.is_err(), "got {alone:?}");
	assert_eq!(wal_bytes(&live), segment_before, "the segment is still byte-identical");

	// Smaller groups still go through: the writer was not poisoned and the flusher is alive.
	let later: Vec<(String, Vec<u8>)> =
		(0..4).map(|i| (format!("later_{i}"), value_of(20 + i, 1000))).collect();
	for (key, value) in &later {
		outcome(commit(&tree, key, value.clone())).await.unwrap();
	}
	assert_ring_settled(&tree, 9, "after the failures").await;
	assert_ne!(wal_bytes(&live), segment_before, "control: later commits are logged");

	// With the refusal lifted the same commits go through.
	pipeline.set_wal_reserve_fails_from(usize::MAX);
	outcome(commit(&tree, "alone", value_of(9, 2 * MIB))).await.unwrap();

	let mut all = vec![kept];
	all.extend(later);
	all.push(("alone".to_string(), value_of(9, 2 * MIB)));
	let doomed_keys: Vec<String> = doomed.iter().map(|(k, _)| k.clone()).collect();
	assert_recovers(
		&live,
		&dir.path().join("image"),
		false,
		&all,
		&doomed_keys,
		"after the failures",
	);

	pipeline.set_hook(None);
	mark_closed(&tree);
	release_lock(&tree);
}

/// The groups of a value log tree are separated into the log before the WAL buffer is reserved,
/// so a refusal leaves unreferenced bytes in the log. The group fails, the next groups are
/// logged after them, and every pointer that is acknowledged reads back, live and from a crash
/// image.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_refused_wal_buffer_with_a_value_log_fails_the_group_and_later_pointers_still_read() {
	let dir = TempDir::new("wal_group_budget").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(options(&live, true)).unwrap());
	let pipeline = &tree.core.commit_pipeline;
	let flusher = hold_first_group(&tree);

	let first = ("first".to_string(), value_of(0, 4000));
	let gate = commit(&tree, &first.0, first.1.clone());
	until("the flusher to hold its first group", || flusher.is_held()).await;

	// Every WAL buffer is refused: a group of values that go to the log has a small record, so
	// the threshold is below any record.
	pipeline.set_wal_reserve_fails_from(1);
	let doomed: Vec<(String, Vec<u8>)> =
		(0..3).map(|i| (format!("doomed_{i}"), value_of(10 + i, 300_000))).collect();
	let handles = queue_in_order(&tree, &doomed, 1).await;
	flusher.release();
	outcome(gate).await.unwrap();
	for handle in handles {
		assert!(outcome(handle).await.is_err(), "the group fails as a whole");
	}
	pipeline.set_wal_reserve_fails_from(usize::MAX);

	let later: Vec<(String, Vec<u8>)> =
		(0..4).map(|i| (format!("later_{i}"), value_of(30 + i, 200_000 + i * 1000))).collect();
	for (key, value) in &later {
		outcome(commit(&tree, key, value.clone())).await.unwrap();
	}
	assert_ring_settled(&tree, 1 + 3 + 4, "value log").await;

	let mut all = vec![first];
	all.extend(later);
	{
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		for (key, value) in &all {
			assert_eq!(
				txn.get(key.as_bytes()).unwrap().as_deref(),
				Some(value.as_slice()),
				"{key}"
			);
		}
		for (key, _) in &doomed {
			assert_eq!(txn.get(key.as_bytes()).unwrap(), None, "{key} is not acknowledged");
		}
	}
	let doomed_keys: Vec<String> = doomed.iter().map(|(k, _)| k.clone()).collect();
	assert_recovers(&live, &dir.path().join("image"), true, &all, &doomed_keys, "value log");

	pipeline.set_hook(None);
	mark_closed(&tree);
	release_lock(&tree);
}

/// A group that straddles a memtable rotation applies its first commits, rotates, and appends
/// the rest again in the new segment. That second append is encoded into the buffers of the
/// first, so it reserves nothing: with every new WAL buffer refused from the moment the rotation
/// has happened, the group still succeeds whole. A reservation there could fail the group after
/// part of it was applied, and the commits it reported as failed would become readable once the
/// next group moved the visible sequence number past them.
#[test(tokio::test)]
async fn a_re_append_after_a_rotation_needs_no_buffer_of_its_own() {
	let dir = TempDir::new("wal_group_budget").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(
		Tree::new(Arc::new(Options {
			path: live.clone(),
			max_memtable_size: 4096,
			flush_on_close: false,
			memtable_stall_threshold: 100_000,
			level0_max_files: 10_000,
			l0_stall_threshold: 10_000,
			..Default::default()
		}))
		.unwrap(),
	);
	let tm = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tm.stop().await;

	let prefill: Vec<(String, Vec<u8>)> =
		(0..10).map(|i| (format!("pre{i:03}"), value_of(i, 100))).collect();
	for (key, value) in &prefill {
		outcome(commit(&tree, key, value.clone())).await.unwrap();
	}

	let pipeline = Arc::downgrade(&tree.core.commit_pipeline);
	let refused = Arc::new(AtomicBool::new(false));
	let flag = Arc::clone(&refused);
	tree.core.commit_pipeline.set_hook(Some(Arc::new(move |point| {
		if let PipelineHook::BeforeApplyRound {
			round: 1,
		} = point
		{
			if let Some(pipeline) = pipeline.upgrade() {
				pipeline.set_wal_reserve_fails_from(0);
				flag.store(true, Ordering::SeqCst);
			}
		}
	})));

	// One group on the current-thread runtime: spawned before the flusher is polled.
	let group: Vec<(String, Vec<u8>)> =
		(0..24).map(|i| (format!("grp{i:03}"), value_of(100 + i, 100))).collect();
	let handles: Vec<_> = group.iter().map(|(k, v)| commit(&tree, k, v.clone())).collect();
	let mut results = Vec::new();
	for handle in handles {
		results.push(outcome(handle).await);
	}
	assert!(refused.load(Ordering::SeqCst), "control: the group needed a second round");
	tree.core.commit_pipeline.set_wal_reserve_fails_from(usize::MAX);
	tree.core.commit_pipeline.set_hook(None);
	assert!(results.iter().all(|r| r.is_ok()), "the group is acknowledged whole: {results:?}");

	// The next group makes everything below its sequence number visible.
	outcome(commit(&tree, "later", value_of(500, 100))).await.unwrap();
	let mut all: Vec<(String, Vec<u8>)> = prefill.into_iter().chain(group).collect();
	all.push(("later".to_string(), value_of(500, 100)));
	{
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		for (key, value) in &all {
			assert_eq!(
				txn.get(key.as_bytes()).unwrap().as_deref(),
				Some(value.as_slice()),
				"{key}"
			);
		}
	}
	assert_recovers(&live, &dir.path().join("image"), false, &all, &[], "re-append");
	mark_closed(&tree);
	release_lock(&tree);
}

// ---------------------------------------------------------------------------
// the estimate the budget and the reservation rest on
// ---------------------------------------------------------------------------

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

/// The WAL buffer is reserved at exactly the length hint of the batches, as the flusher reserves
/// it after the values are wrapped, and written without growing: a record longer than its hint
/// would double the buffer behind the back of the fallible reservation, and leave it over what
/// the flusher keeps. Checked over lengths that straddle every varint boundary, and timestamps of
/// every width.
#[test]
fn the_length_hint_covers_the_encoding_of_every_shape_of_batch() {
	let lengths = [0usize, 1, 2, 125, 126, 127, 128, 129, 16_382, 16_383, 16_384, 16_385, 70_000];
	let stamps = [
		0u64,
		1,
		127,
		128,
		16_383,
		16_384,
		u32::MAX as u64,
		1 << 55,
		(1 << 56) - 1,
		1 << 56,
		1_700_000_000_000_000_000,
		u64::MAX,
	];
	let kinds = [
		InternalKeyKind::Set,
		InternalKeyKind::Delete,
		InternalKeyKind::RangeDelete,
		InternalKeyKind::Merge,
		InternalKeyKind::SoftDelete,
		InternalKeyKind::Replace,
	];
	let mut rng = Rng(0xBA7C4);
	for round in 0..3000 {
		let mut plain = Batch::new(rng.next() >> rng.below(64));
		let mut wrapped = Batch::new(plain.starting_seq_num);
		for _ in 0..1 + rng.below(6) {
			let kind = kinds[rng.below(kinds.len() as u64) as usize];
			let key = vec![7u8; lengths[rng.below(lengths.len() as u64) as usize]];
			let value = match rng.below(3) {
				0 => None,
				_ => Some(vec![9u8; lengths[rng.below(lengths.len() as u64) as usize]]),
			};
			let stamp = stamps[rng.below(stamps.len() as u64) as usize];
			// The flusher wraps every value but a range delete's end key in two bytes.
			let wrapped_value = match (&value, kind) {
				(Some(v), kind) if kind != InternalKeyKind::RangeDelete => {
					let mut w = vec![0u8; 2];
					w.extend_from_slice(v);
					Some(w)
				}
				(v, _) => v.clone(),
			};
			plain.add_record(kind, key.clone(), value, stamp).unwrap();
			wrapped.add_record(kind, key, wrapped_value, stamp).unwrap();
		}
		for (name, batch) in [("as accepted", &plain), ("as logged", &wrapped)] {
			let mut buf = Vec::new();
			batch.encode_into(&mut buf).unwrap();
			assert!(
				buf.len() <= batch.encoded_len_hint(),
				"round {round}, {name}: {} bytes encoded, hint {}",
				buf.len(),
				batch.encoded_len_hint()
			);
			let mut exact = Vec::new();
			exact.try_reserve_exact(batch.encoded_len_hint()).unwrap();
			let ptr = exact.as_ptr();
			batch.encode_into(&mut exact).unwrap();
			assert_eq!(exact.as_ptr(), ptr, "round {round}, {name}: the reservation grew");
		}
		assert!(
			plain.encoded_len_hint() <= wrapped.encoded_len_hint(),
			"round {round}: wrapping never lowers the estimate"
		);
	}
}

// ---------------------------------------------------------------------------
// a randomised workload, with refusals injected, on both runtimes
// ---------------------------------------------------------------------------

struct Chaos {
	seed: u64,
	tasks: usize,
	rounds: usize,
	vlog: bool,
	memtable: usize,
	/// Refuse WAL buffers of at least this many bytes while the workload runs.
	refuse_from: Option<usize>,
	/// Read every key back after each commit. Off where a commit bigger than the memtable goes
	/// straight to an L0 table.
	read_back: bool,
}

fn chaos_options(path: &Path, cfg: &Chaos) -> Arc<Options> {
	Arc::new(Options {
		path: path.to_path_buf(),
		flush_on_close: false,
		enable_vlog: cfg.vlog,
		max_memtable_size: cfg.memtable,
		..Default::default()
	})
}

/// A value, summarised: its task, round and length.
fn describe(value: &Option<Vec<u8>>) -> String {
	match value {
		None => "nothing".to_string(),
		Some(v) if v.len() >= 16 => format!(
			"task {} round {} of {} bytes",
			u64::from_le_bytes(v[..8].try_into().unwrap()),
			u64::from_le_bytes(v[8..16].try_into().unwrap()),
			v.len()
		),
		Some(v) => format!("{} bytes", v.len()),
	}
}

fn chaos_value(task: usize, round: usize, len: usize) -> Vec<u8> {
	let mut v = vec![(task * 31 + round) as u8; len.max(16)];
	v[..8].copy_from_slice(&(task as u64).to_le_bytes());
	v[8..16].copy_from_slice(&(round as u64).to_le_bytes());
	v
}

/// Every task writes its own key (with a hot key shared by all in some commits, so that some
/// commits are aborted by validation and leave complete entries in the ring), in sizes from a
/// few bytes to several MiB, with both durabilities. A commit that returns an error leaves its
/// key as it was, and one that returns Ok is read back at once. At the end every key holds the
/// last value its task had acknowledged, in the live tree and in a crash image, and the ring is
/// settled.
async fn chaos(cfg: Chaos) {
	let dir = TempDir::new("wal_group_budget").unwrap();
	let live = dir.path().join("live");
	let tree = Arc::new(Tree::new(chaos_options(&live, &cfg)).unwrap());
	if let Some(bytes) = cfg.refuse_from {
		tree.core.commit_pipeline.set_wal_reserve_fails_from(bytes);
	}
	let failures = Arc::new(AtomicUsize::new(0));
	let mut handles = Vec::new();
	for task in 0..cfg.tasks {
		let (tree, failures) = (Arc::clone(&tree), Arc::clone(&failures));
		let (seed, rounds, read_back) = (cfg.seed, cfg.rounds, cfg.read_back);
		handles.push(tokio::spawn(async move {
			let mut rng = Rng(seed ^ (task as u64 + 1).wrapping_mul(0xD1B5_4A32_D192_ED03));
			let key = format!("task_{task:02}");
			let mut last: Option<Vec<u8>> = None;
			for round in 0..rounds {
				let len = match rng.below(10) {
					0..=4 => 16 + rng.below(2000) as usize,
					5..=6 => 100_000 + rng.below(200_000) as usize,
					7..=8 => MIB + rng.below(600_000) as usize,
					_ => 2 * MIB + rng.below(2_500_000) as usize,
				};
				let value = chaos_value(task, round, len);
				let durability = if rng.below(3) == 0 {
					Durability::Eventual
				} else {
					Durability::Immediate
				};
				let mut txn = tree.begin().unwrap();
				txn.set_durability(durability);
				txn.set(key.as_bytes(), &value).unwrap();
				if rng.below(4) == 0 {
					txn.set(b"hot", &value[..16]).unwrap();
				}
				match tokio::time::timeout(WATCHDOG, txn.commit()).await.expect("a commit hung") {
					Ok(()) => last = Some(value),
					Err(Error::TransactionWriteConflict) => {}
					Err(_) => {
						failures.fetch_add(1, Ordering::SeqCst);
					}
				}
				if !read_back {
					continue;
				}
				let seen =
					tree.begin_with_mode(Mode::ReadOnly).unwrap().get(key.as_bytes()).unwrap();
				assert!(
					seen == last,
					"{key} after round {round}: read {}, expected {}",
					describe(&seen),
					describe(&last)
				);
			}
			last
		}));
	}
	let mut last: Vec<(String, Option<Vec<u8>>)> = Vec::new();
	for (task, handle) in handles.into_iter().enumerate() {
		last.push((format!("task_{task:02}"), handle.await.unwrap()));
	}
	if cfg.refuse_from.is_some() {
		assert!(failures.load(Ordering::SeqCst) > 0, "control: some group was refused");
	}

	// Every key ends on an acknowledged Immediate commit that follows all its earlier ones.
	tree.core.commit_pipeline.set_wal_reserve_fails_from(usize::MAX);
	for (task, (key, value)) in last.iter_mut().enumerate() {
		let fin = chaos_value(task, 9999, 700 + task);
		outcome(commit_with(&tree, key, fin.clone(), Durability::Immediate)).await.unwrap();
		*value = Some(fin);
	}
	outcome(commit(&tree, "end", vec![1; 10])).await.unwrap();

	let pipeline = &tree.core.commit_pipeline;
	until("every permit to come back", || pipeline.free_permits() == COMMIT_RING_CAPACITY / 2)
		.await;
	let (completed, taken, overflow) = pipeline.watermarks();
	assert_eq!(completed, pipeline.ring.published(), "every entry is complete");
	assert!(taken <= completed && overflow == 0);
	assert_eq!(pipeline.accepted_waiting(), 0);

	let keys: Vec<String> = last.iter().map(|(k, _)| k.clone()).collect();
	{
		let txn = tree.begin_with_mode(Mode::ReadOnly).unwrap();
		for (key, value) in &last {
			let got = txn.get(key.as_bytes()).unwrap();
			assert!(
				got == *value,
				"{key} live: read {}, expected {}",
				describe(&got),
				describe(value)
			);
		}
	}
	// Memtable flushes and compactions delete files: the copy is taken once they have stopped.
	let tasks = tree.core.task_manager.lock().unwrap().clone().unwrap();
	tasks.stop().await;
	let recovered = recover_keys(&live, chaos_options(&dir.path().join("image"), &cfg), &keys);
	for ((key, value), got) in last.iter().zip(recovered) {
		assert!(
			got == *value,
			"{key} after a crash: read {}, expected {}",
			describe(&got),
			describe(value)
		);
	}
	mark_closed(&tree);
	release_lock(&tree);
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_randomised_workload_keeps_every_acknowledged_commit_and_nothing_else() {
	for seed in 1..=2 {
		chaos(Chaos {
			seed,
			tasks: 6,
			rounds: 8,
			vlog: false,
			memtable: 12 * MIB,
			refuse_from: None,
			read_back: true,
		})
		.await;
	}
}

/// Memtables smaller than the biggest commits: those go straight to L0 tables, in the middle of
/// groups that the budget cuts.
#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_randomised_workload_with_commits_bigger_than_the_memtable_keeps_every_acknowledged_commit(
) {
	for (seed, refuse_from) in [(41, None), (42, Some(MIB))] {
		chaos(Chaos {
			seed,
			tasks: 6,
			rounds: 8,
			vlog: false,
			memtable: 3 * MIB,
			refuse_from,
			read_back: false,
		})
		.await;
	}
}

#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
async fn a_randomised_workload_with_a_value_log_and_refused_buffers_keeps_every_acknowledged_commit(
) {
	for seed in 51..=52 {
		chaos(Chaos {
			seed,
			tasks: 6,
			rounds: 12,
			vlog: true,
			memtable: 12 * MIB,
			refuse_from: Some(300),
			read_back: true,
		})
		.await;
	}
}

#[test(tokio::test)]
async fn a_randomised_workload_on_a_current_thread_runtime_keeps_every_acknowledged_commit() {
	for seed in 31..=32 {
		chaos(Chaos {
			seed,
			tasks: 6,
			rounds: 12,
			vlog: false,
			memtable: 12 * MIB,
			refuse_from: Some(2 * MIB),
			read_back: true,
		})
		.await;
	}
}
