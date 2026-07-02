// Synchronous single-commit-thread pipeline.
//
// Replaces the Pebble-style pipeline (commit.rs: write_mutex + lock-free ring +
// semaphore + per-commit oneshot + FIFO publish ratchet), which collapsed under
// concurrency (per-commit lock convoy). A prototype benchmark on the 64-core
// Linux target (benches/committer_proto.rs) proved this design beats it:
// full-park at N=128 sustains ~480K commits/s (1.4x the old pipeline, ~rocksdb
// parity) with no collapse.
//
// Design: ONE dedicated commit thread owns all serial state (seq counter,
// CommitOracle, WAL, memtable apply, visible_seq_num) — NO lock on the hot path.
// Committers `pre_serialize` off-thread (parallel), submit a request over a
// crossbeam-channel (its Sender is Sync, so any thread can submit via &), and
// PARK (no spin — on Linux the commit thread's unpark is cheap; the wake path
// sustains >1M/s, so the thread is work-bound not wake-bound). The commit thread
// drains whatever is queued as a GROUP and processes it serially: per member
// oracle.check -> seq alloc -> oracle.publish -> patch seq; then ONE batched WAL
// append+flush (fsync iff any member is sync); then apply each to the memtable;
// then advance visible_seq_num to the group's last seq (FIFO is inherent — single
// thread, submission order). Then it completes the group (writes each result and
// unparks parked waiters).

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle, Thread};

use crossbeam_channel::{Receiver, Sender};

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::oracle::CommitOracle;
use crate::stall::WriteStallController;

/// Output of [`CommitEnv::pre_serialize`]: the processed batch (values wrapped
/// in `ValueLocation`, ready for memtable apply) plus its WAL encoding carrying
/// a placeholder sequence number. The real seq is stamped in place by the commit
/// thread via [`Batch::patch_encoded_seq`].
pub(crate) struct PreparedWrite {
	pub(crate) processed_batch: Batch,
	pub(crate) bytes: Vec<u8>,
}

/// LSM operations the commit thread (and the off-thread pre-serialize) call into.
pub trait CommitEnv: Send + Sync + 'static {
	/// Build the WAL-ready encoding of `batch` (value-location wrapping + encode).
	/// Called by the COMMITTER thread (off the commit thread, in parallel) — this
	/// is the expensive part. Returns the processed batch (for `apply`) plus its
	/// encoded bytes with a placeholder seq.
	fn pre_serialize(&self, batch: &Batch) -> Result<PreparedWrite>;

	/// Append a whole group of pre-encoded WAL records in ONE acquisition and
	/// flush ONCE (fsync iff `sync`). Called by the commit thread; the seqs are
	/// already patched into each record. Coalescing the flush is the group-commit
	/// win — one `write()` syscall per group instead of per commit.
	fn wal_append_group(&self, records: &[&[u8]], sync: bool) -> Result<()>;

	/// Apply a processed batch to the memtable. Called by the commit thread.
	fn apply(&self, batch: &Batch) -> Result<()>;

	/// Check for background errors before committing.
	fn check_background_error(&self) -> Result<()>;

	/// Smallest `start_seq_num` of any currently-live transaction (oracle GC
	/// watermark). Test envs may return 0.
	fn oldest_active_start_seq(&self) -> u64;
}

// Completion states for the park/unpark handoff.
const ST_INIT: u8 = 0;
const ST_PARKED: u8 = 1;
const ST_DONE: u8 = 2;

/// Per-commit completion handle. The committer parks on it; the commit thread
/// writes the result and wakes it.
struct Completion {
	state: AtomicU8,
	waiter: Thread,
	result: UnsafeCell<Option<Result<()>>>,
}

// SAFETY: `result` is written exactly once by the commit thread BEFORE it stores
// `ST_DONE` with Release ordering; the committer reads it only AFTER observing
// `ST_DONE` with Acquire ordering. The state atomic establishes the
// happens-before; there is no concurrent access to `result`.
unsafe impl Sync for Completion {}

impl Completion {
	fn new() -> Arc<Self> {
		Arc::new(Self {
			state: AtomicU8::new(ST_INIT),
			waiter: thread::current(),
			result: UnsafeCell::new(None),
		})
	}
}

struct CommitReq {
	prepared: PreparedWrite,
	sync: bool,
	start_seq: u64,
	comp: Arc<Completion>,
	// Filled in by the commit thread during group processing:
	seq: u64,
	had_seq: bool,
	result: Result<()>,
}

enum Msg {
	Commit(CommitReq),
	Shutdown,
}

struct Shared {
	visible_seq_num: Arc<AtomicU64>,
	/// Next seq to allocate. Single-writer (the commit thread) in steady state;
	/// also set by `set_seq_num` at open and (under `restore_lock` write) at restore.
	log_seq_num: AtomicU64,
	oracle: Arc<CommitOracle>,
	/// The commit thread takes a READ guard around each group; restore takes the
	/// WRITE guard to pause the thread between groups.
	restore_lock: RwLock<()>,
	shutdown: AtomicBool,
}

pub(crate) struct Committer {
	tx: Sender<Msg>,
	shared: Arc<Shared>,
	env: Arc<dyn CommitEnv>,
	write_stall: Arc<WriteStallController>,
	handle: Mutex<Option<JoinHandle<()>>>,
}

impl Committer {
	pub(crate) fn new(
		env: Arc<dyn CommitEnv>,
		visible_seq_num: Arc<AtomicU64>,
		write_stall: Arc<WriteStallController>,
	) -> Arc<Self> {
		let shared = Arc::new(Shared {
			visible_seq_num,
			log_seq_num: AtomicU64::new(1),
			oracle: Arc::new(CommitOracle::new()),
			restore_lock: RwLock::new(()),
			shutdown: AtomicBool::new(false),
		});
		let (tx, rx) = crossbeam_channel::unbounded::<Msg>();
		let handle = {
			let shared = Arc::clone(&shared);
			let env = Arc::clone(&env);
			thread::Builder::new()
				.name("surrealkv-commit".into())
				.spawn(move || run_commit_loop(rx, shared, env))
				.expect("spawn commit thread")
		};
		Arc::new(Self {
			tx,
			shared,
			env,
			write_stall,
			handle: Mutex::new(Some(handle)),
		})
	}

	/// Set the seq counters on open / after restore. See `reset_oracle_for_restore`.
	pub(crate) fn set_seq_num(&self, seq_num: u64) {
		if seq_num > 0 {
			self.shared.visible_seq_num.store(seq_num, Ordering::Release);
			self.shared.log_seq_num.store(seq_num + 1, Ordering::Release);
		}
	}

	/// Pause the commit thread (between groups) until the returned guard drops.
	/// Used by `Tree::restore_from_checkpoint` to serialize the restore against
	/// commits. Returns `impl Drop` so callers don't bind to the backing lock.
	pub(crate) fn lock_writes(&self) -> impl Drop + '_ {
		self.shared.restore_lock.write().unwrap_or_else(|e| e.into_inner())
	}

	/// Discard all oracle entries and set `kept_since = max_seq`. Called from
	/// restore after the seq counter is rewound (NOT at startup).
	pub(crate) fn reset_oracle_for_restore(&self, max_seq: u64) {
		self.shared.oracle.reset_for_restore(max_seq);
	}

	pub(crate) fn get_visible_seq_num(&self) -> u64 {
		self.shared.visible_seq_num.load(Ordering::Acquire)
	}

	#[cfg(test)]
	pub(crate) fn oracle(&self) -> &Arc<CommitOracle> {
		&self.shared.oracle
	}

	pub(crate) fn shutdown(&self) {
		// Idempotent: set the flag, wake the commit thread, join it once.
		if self.shared.shutdown.swap(true, Ordering::AcqRel) {
			return;
		}
		let _ = self.tx.send(Msg::Shutdown);
		if let Ok(mut g) = self.handle.lock() {
			if let Some(h) = g.take() {
				let _ = h.join();
			}
		}
	}

	/// Submit a batch and block until it is durable + visible (or fails).
	pub(crate) fn commit(&self, batch: Batch, sync: bool, start_seq: u64) -> Result<()> {
		if self.shared.shutdown.load(Ordering::Acquire) {
			return Err(Error::PipelineStall);
		}
		self.env.check_background_error()?;
		if batch.is_empty() {
			return Ok(());
		}

		// Backpressure: block here (off the commit thread) if the memtable/L0
		// backlog is over the stall threshold, so stalled writers don't queue.
		self.write_stall.check()?;

		// Expensive encode OFF the commit thread (parallel across committers).
		let prepared = self.env.pre_serialize(&batch)?;

		let comp = Completion::new();
		let req = CommitReq {
			prepared,
			sync,
			start_seq,
			comp: Arc::clone(&comp),
			seq: 0,
			had_seq: false,
			result: Ok(()),
		};
		self.tx.send(Msg::Commit(req)).map_err(|_| Error::PipelineStall)?;

		// Park until the commit thread completes us (no spin — Linux unpark is
		// cheap; spinning regressed at N=128 in the prototype).
		if comp
			.state
			.compare_exchange(ST_INIT, ST_PARKED, Ordering::AcqRel, Ordering::Acquire)
			.is_ok()
		{
			while comp.state.load(Ordering::Acquire) != ST_DONE {
				thread::park();
			}
		}
		// SAFETY: state == ST_DONE (Acquire) ⇒ the commit thread finished writing
		// `result` before its Release store; no other accessor remains.
		unsafe { (*comp.result.get()).take().expect("commit result set before ST_DONE") }
	}
}

impl Drop for Committer {
	fn drop(&mut self) {
		self.shutdown();
	}
}

#[inline]
fn keys_of(b: &Batch) -> impl Iterator<Item = &[u8]> + Clone {
	b.entries.iter().map(|e| e.key.as_slice())
}

fn run_commit_loop(rx: Receiver<Msg>, shared: Arc<Shared>, env: Arc<dyn CommitEnv>) {
	loop {
		let first = match rx.recv() {
			Ok(Msg::Commit(r)) => r,
			_ => break, // Shutdown or channel disconnected
		};
		let mut group: Vec<CommitReq> = vec![first];
		let mut stop = false;
		loop {
			match rx.try_recv() {
				Ok(Msg::Commit(r)) => group.push(r),
				Ok(Msg::Shutdown) => {
					stop = true;
					break;
				}
				Err(_) => break,
			}
		}

		{
			// Pause point for restore: uncontended in steady state.
			let _g = shared.restore_lock.read().unwrap_or_else(|e| e.into_inner());
			process_group(&mut group, &shared, env.as_ref());
		}

		// Complete: publish each result, wake parked waiters.
		for mut req in group {
			let result = std::mem::replace(&mut req.result, Ok(()));
			// SAFETY: sole writer of `result`, before the Release store below.
			unsafe { *req.comp.result.get() = Some(result) };
			let prev = req.comp.state.swap(ST_DONE, Ordering::Release);
			if prev == ST_PARKED {
				req.comp.waiter.unpark();
			}
		}

		if stop {
			break;
		}
	}

	// Fail any stragglers so their callers don't block forever.
	while let Ok(Msg::Commit(req)) = rx.try_recv() {
		unsafe { *req.comp.result.get() = Some(Err(Error::PipelineStall)) };
		let prev = req.comp.state.swap(ST_DONE, Ordering::Release);
		if prev == ST_PARKED {
			req.comp.waiter.unpark();
		}
	}
}

/// Serial group processing on the commit thread. Order: oracle.check -> seq ->
/// oracle.publish -> patch (per member); ONE batched WAL append+flush; apply each;
/// advance visible_seq to the group's highest allocated seq.
fn process_group(group: &mut [CommitReq], shared: &Shared, env: &dyn CommitEnv) {
	// 1. validate + allocate seq + publish + patch
	for req in group.iter_mut() {
		let count = req.prepared.processed_batch.count() as u64;
		if let Err(e) = shared.oracle.check(keys_of(&req.prepared.processed_batch), req.start_seq) {
			req.result = Err(e); // conflict/retry: no seq consumed
			continue;
		}
		let seq = shared.log_seq_num.fetch_add(count, Ordering::SeqCst);
		req.seq = seq;
		req.had_seq = true;
		let oldest = env.oldest_active_start_seq().min(req.start_seq);
		shared.oracle.publish(keys_of(&req.prepared.processed_batch), seq, count, oldest);
		Batch::patch_encoded_seq(&mut req.prepared.bytes, seq);
		req.prepared.processed_batch.set_starting_seq_num(seq);
	}

	// 2. ONE batched WAL append + flush for all members that allocated a seq.
	{
		let records: Vec<&[u8]> =
			group.iter().filter(|r| r.had_seq).map(|r| r.prepared.bytes.as_slice()).collect();
		if !records.is_empty() {
			let any_sync = group.iter().any(|r| r.had_seq && r.sync);
			if let Err(e) = env.wal_append_group(&records, any_sync) {
				// WAL failed: roll back oracle entries + fail every seq'd member.
				for req in group.iter_mut().filter(|r| r.had_seq) {
					let count = req.prepared.processed_batch.count() as u64;
					let stamp = req.seq + count - 1;
					shared.oracle.rollback(keys_of(&req.prepared.processed_batch), stamp);
					req.result = Err(Error::CommitFail(e.to_string()));
					req.had_seq = false; // skip apply; seq stays consumed (gap)
				}
			}
		}
	}

	// 3. apply each WAL-durable member; advance visibility over all seq'd members.
	let mut max_seq = 0u64;
	for req in group.iter_mut() {
		if !req.had_seq {
			continue;
		}
		let count = req.prepared.processed_batch.count() as u64;
		max_seq = max_seq.max(req.seq + count - 1);
		match env.apply(&req.prepared.processed_batch) {
			Ok(()) => req.result = Ok(()),
			Err(e) => {
				// apply failed: roll back oracle (no reader saw it); seq is a gap,
				// the durable WAL record replays on recovery, shadowed by a later seq.
				let stamp = req.seq + count - 1;
				shared.oracle.rollback(keys_of(&req.prepared.processed_batch), stamp);
				req.result = Err(Error::CommitFail(e.to_string()));
			}
		}
	}

	// 4. advance visible_seq (single writer, monotonic) over the group's seqs,
	//    including apply-failed gaps (matches the old pipeline's publish()).
	if max_seq > 0 {
		let cur = shared.visible_seq_num.load(Ordering::Acquire);
		if max_seq > cur {
			shared.visible_seq_num.store(max_seq, Ordering::Release);
		}
	}
}
