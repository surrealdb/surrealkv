// This commit pipeline is inspired by Pebble's commit pipeline.

use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::thread::Thread;

use parking_lot::Mutex;

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::oracle::CommitOracle;
use crate::stall::WriteStallController;

/// Capacity of the lock-free commit ring buffer (slot array + index mask).
/// Must be a power of two. Commit is synchronous, so the number of in-flight
/// commits is bounded by the number of caller threads (each blocks until done)
/// — RocksDB's model. This capacity only needs to exceed that thread count;
/// 1024 is far above any realistic writer-thread pool, so the ring can never
/// overflow.
const COMMIT_QUEUE_SIZE: usize = 1024;
const DEQUEUE_BITS: u32 = 32;

/// Spin iterations before yielding while waiting (RocksDB uses 200 pause loops).
const WAIT_SPINS: u32 = 256;
/// Yield budget before parking, in microseconds (RocksDB default 100µs).
const WAIT_YIELD_US: u64 = 100;

/// 3-phase wait (spin → yield → park) on a predicate, mirroring RocksDB's
/// `AwaitState`. Most waits resolve in the spin/yield window (the leader signals
/// within microseconds), so they never hit a futex park / context switch.
/// `done` must be re-checked each step (park may wake spuriously).
#[inline]
fn spin_yield_park<F: Fn() -> bool>(done: F) {
	for _ in 0..WAIT_SPINS {
		if done() {
			return;
		}
		std::hint::spin_loop();
	}
	let start = std::time::Instant::now();
	while start.elapsed() < std::time::Duration::from_micros(WAIT_YIELD_US) {
		if done() {
			return;
		}
		std::thread::yield_now();
	}
	while !done() {
		std::thread::park();
	}
}

/// Output of [`CommitEnv::pre_serialize`]: the processed batch (values wrapped
/// in `ValueLocation`, ready for memtable apply) plus its WAL encoding carrying
/// a placeholder sequence number. The group leader stamps the real seq into the
/// bytes in place (`Batch::patch_encoded_seq`) before appending to the WAL.
pub(crate) struct PreparedWrite {
	pub(crate) processed_batch: Batch,
	pub(crate) bytes: Vec<u8>,
}

// Trait for commit operations
pub trait CommitEnv: Send + Sync + 'static {
	// Build the WAL-ready encoding of `batch` (value-location wrapping + encode)
	// OUTSIDE the commit `write_mutex` — this is the expensive part. Returns the
	// processed batch (for `apply`) plus its encoded bytes with a placeholder seq.
	fn pre_serialize(&self, batch: &Batch) -> Result<PreparedWrite>;

	// Append a whole group of pre-encoded WAL records in ONE lock acquisition,
	// then flush once (and fsync once iff `sync`). Called by the group leader
	// UNDER `write_mutex`. Amortizes the `write()` syscall across the group.
	fn wal_append_group(&self, records: &[&[u8]], sync: bool) -> Result<()>;

	// Apply processed batch to memtable
	fn apply(&self, batch: &Batch) -> Result<()>;

	// Check for background errors before committing
	fn check_background_error(&self) -> Result<()>;

	// Smallest `start_seq_num` of any currently-live transaction.
	// Used by the commit oracle to compute its GC threshold under
	// `write_mutex`. Implementations that don't run real transactions
	// (test envs) may return 0.
	fn oldest_active_start_seq(&self) -> u64;
}

// Lock-free commit queue entry. Completion is delivered synchronously via
// `complete()` (from `publish()`), which wakes the committer parked in `wait()`.
struct CommitBatch {
	seq_num: AtomicU64,
	count: u32, // Number of entries in the batch
	applied: AtomicBool,
	/// Won once by the first `complete()` caller (claim); later callers are
	/// no-ops, so a `publish()` Ok cannot clobber an earlier Err.
	claimed: AtomicBool,
	/// Result is visible once `done` is observed true (Acquire).
	done: AtomicBool,
	result: UnsafeCell<Option<Result<()>>>,
	/// The committing thread, parked in `wait()`; woken by `complete()`.
	waiter: Thread,
}

// SAFETY: `result` is written exactly once by the single `complete()` claimer
// before `done` is set (Release); `wait()` reads it only after observing `done`
// (Acquire). No concurrent access.
unsafe impl Sync for CommitBatch {}

impl CommitBatch {
	fn new(count: u32, waiter: Thread) -> Arc<Self> {
		Arc::new(Self {
			seq_num: AtomicU64::new(0),
			count,
			applied: AtomicBool::new(false),
			claimed: AtomicBool::new(false),
			done: AtomicBool::new(false),
			result: UnsafeCell::new(None),
			waiter,
		})
	}

	fn set_seq_num(&self, seq: u64) {
		self.seq_num.store(seq, Ordering::Release);
	}

	fn get_seq_num(&self) -> u64 {
		self.seq_num.load(Ordering::Acquire)
	}

	fn mark_applied(&self) {
		self.applied.store(true, Ordering::Release);
	}

	fn is_applied(&self) -> bool {
		self.applied.load(Ordering::Acquire)
	}

	/// Deliver the commit result and wake the waiter. Idempotent: only the first
	/// caller wins the claim and writes the result.
	fn complete(&self, result: Result<()>) {
		if self.claimed.swap(true, Ordering::AcqRel) {
			return;
		}
		// SAFETY: we won the claim — sole writer of `result`, before `done`.
		unsafe { *self.result.get() = Some(result) };
		self.done.store(true, Ordering::Release);
		self.waiter.unpark();
	}

	/// Block (spin → yield → park) until completed, then take the result.
	fn wait(&self) -> Result<()> {
		spin_yield_park(|| self.done.load(Ordering::Acquire));
		// SAFETY: `done` observed true ⇒ `result` written by the claimer.
		unsafe { (*self.result.get()).take().unwrap() }
	}
}

/// Writer-group states.
const WS_INIT: u8 = 0;
const WS_WAL_OK: u8 = 1;
const WS_WAL_ERR: u8 = 2;

/// A committer's node in the lock-free write group (RocksDB-style group commit).
/// Stack-allocated by the committer and shared with the group leader via a raw
/// pointer on the Treiber stack `newest_writer`. Its address is stable for the
/// duration of the synchronous `commit()` call (RocksDB's `Writer` model).
struct WriteRequest {
	/// Pre-encoded WAL record (placeholder seq). Leader patches the seq + reads
	/// it for the WAL append; the owner never touches it after linking.
	bytes: UnsafeCell<Vec<u8>>,
	/// Processed batch. Leader reads keys (oracle) + stamps seq; owner applies
	/// after observing `state == WS_WAL_OK`.
	processed_batch: UnsafeCell<Batch>,
	commit_batch: Arc<CommitBatch>,
	start_seq: u64,
	sync: bool,
	link_next: AtomicPtr<WriteRequest>,
	/// WS_INIT → WS_WAL_OK / WS_WAL_ERR (Release by leader, Acquire by owner).
	state: AtomicU8,
	/// WAL outcome error (set by leader before `state` when WS_WAL_ERR).
	outcome: UnsafeCell<Option<Error>>,
	/// Owner thread, unparked by the leader when it sets `state`.
	thread: Thread,
}

// SAFETY: each `UnsafeCell` field is accessed by exactly one thread at a time,
// gated by the CAS-link/swap (leader's first access) and the `state`
// Release/Acquire handoff (owner's access). See `run_leader_group`.
unsafe impl Send for WriteRequest {}
unsafe impl Sync for WriteRequest {}

// Lock-free single-producer, multi-consumer commit queue
//
// commitQueue is a lock-free fixed-size single-producer, multi-consumer
// queue. The single producer can enqueue (push) to the head, and consumers can
// dequeue (pop) from the tail.
struct CommitQueue {
	// Head and tail packed into single atomic
	// head = index of next slot to fill (high 32 bits)
	// tail = index of oldest data in queue (low 32 bits)
	head_tail: AtomicU64,
	slots: [AtomicPtr<CommitBatch>; COMMIT_QUEUE_SIZE],
}

impl CommitQueue {
	fn new() -> Self {
		Self {
			head_tail: AtomicU64::new(0),
			slots: std::array::from_fn(|_| AtomicPtr::new(std::ptr::null_mut())),
		}
	}

	fn unpack(&self, ptrs: u64) -> (u32, u32) {
		let head = (ptrs >> DEQUEUE_BITS) as u32;
		let tail = ptrs as u32;
		(head, tail)
	}

	fn pack(&self, head: u32, tail: u32) -> u64 {
		((head as u64) << DEQUEUE_BITS) | (tail as u64)
	}

	// Single producer enqueue
	fn enqueue(&self, batch: Arc<CommitBatch>) {
		let ptrs = self.head_tail.load(Ordering::Acquire);
		let (head, tail) = self.unpack(ptrs);

		// Check if queue is full
		if tail.wrapping_add(COMMIT_QUEUE_SIZE as u32) == head {
			// Queue is full. This should never be reached because the semaphore
			// limits the number of concurrent operations.
			panic!("commit queue overflow - should not be reached");
		}

		let slot_idx = (head & (COMMIT_QUEUE_SIZE as u32 - 1)) as usize;
		let slot = &self.slots[slot_idx];

		// Check if the head slot has been released by dequeueApplied
		while !slot.load(Ordering::Acquire).is_null() {
			// Another thread is still cleaning up the tail, so the queue is
			// actually still full.
			std::hint::spin_loop();
		}

		// The head slot is free
		let batch_ptr = Arc::into_raw(batch);
		slot.store(batch_ptr as *mut CommitBatch, Ordering::Release);

		// Increment head
		self.head_tail.fetch_add(1 << DEQUEUE_BITS, Ordering::Release);
	}

	// Multi-consumer dequeue - removes the earliest enqueued Batch, if it is
	// applied
	fn dequeue_applied(&self) -> Option<Arc<CommitBatch>> {
		loop {
			let ptrs = self.head_tail.load(Ordering::Acquire);
			let (head, tail) = self.unpack(ptrs);

			if tail == head {
				// Queue is empty
				return None;
			}

			let slot_idx = (tail & (COMMIT_QUEUE_SIZE as u32 - 1)) as usize;
			let slot = &self.slots[slot_idx];
			let batch_ptr = slot.load(Ordering::Acquire);

			if batch_ptr.is_null() {
				// The batch is not ready to be dequeued, or another thread has
				// already dequeued it.
				return None;
			}

			// Check if batch is applied (safely through raw pointer)
			let is_applied = unsafe { (*batch_ptr).is_applied() };
			if !is_applied {
				return None;
			}

			let new_ptrs = self.pack(head, tail.wrapping_add(1));
			if self
				.head_tail
				.compare_exchange_weak(ptrs, new_ptrs, Ordering::Release, Ordering::Relaxed)
				.is_ok()
			{
				// We now own slot.
				slot.store(std::ptr::null_mut(), Ordering::Release);

				let batch = unsafe { Arc::from_raw(batch_ptr) };
				return Some(batch);
			}
			// CAS failed, retry the whole loop
		}
	}
}

pub(crate) struct CommitPipeline {
	env: Arc<dyn CommitEnv>,
	log_seq_num: AtomicU64,
	visible_seq_num: Arc<AtomicU64>,
	// In-memory write-set conflict map. Updated under `write_mutex` atomically
	// with seq allocation; queried under `write_mutex` before seq allocation.
	// This is the source of truth for "has key K been committed at seq > S?",
	// not the memtable — which is why `apply()` can run outside `write_mutex`.
	oracle: Arc<CommitOracle>,
	// Single producer - only one thread can write to WAL at a time
	write_mutex: Mutex<()>,
	// Lock-free single-producer, multi-consumer commit queue
	pending: CommitQueue,
	// Head of the lock-free write group (Treiber stack of stack-allocated
	// `WriteRequest` nodes). The first committer to link an empty list is the
	// group leader. No semaphore: sync commit bounds in-flight by caller threads.
	newest_writer: AtomicPtr<WriteRequest>,
	shutdown: AtomicBool,
	// Write stall controller - checked before acquiring write_mutex
	write_stall: Arc<WriteStallController>,
}

impl CommitPipeline {
	pub(crate) fn new(
		env: Arc<dyn CommitEnv>,
		visible_seq_num: Arc<AtomicU64>,
		write_stall: Arc<WriteStallController>,
	) -> Arc<Self> {
		Arc::new(Self {
			env,
			log_seq_num: AtomicU64::new(1),
			visible_seq_num,
			oracle: Arc::new(CommitOracle::new()),
			write_mutex: Mutex::new(()),
			pending: CommitQueue::new(),
			newest_writer: AtomicPtr::new(ptr::null_mut()),
			shutdown: AtomicBool::new(false),
			write_stall,
		})
	}

	pub(crate) fn set_seq_num(&self, seq_num: u64) {
		if seq_num > 0 {
			self.visible_seq_num.store(seq_num, Ordering::Release);
			self.log_seq_num.store(seq_num + 1, Ordering::Release);
		}
	}

	/// Block new commits from entering the critical section until the returned
	/// guard is dropped. Used by `Tree::restore_from_checkpoint` to serialize
	/// the multi-step restore (manifest reload, memtable wipe, WAL replay,
	/// seq-counter rewind, oracle reset) against concurrent commits. Returns
	/// `impl Drop` so callers don't bind to which internal lock backs it.
	pub(crate) fn lock_writes(&self) -> impl Drop + '_ {
		self.write_mutex.lock()
	}

	/// Discard all oracle entries and set `kept_since = max_seq`.
	///
	/// Called from `Tree::restore_from_checkpoint` after the seq counter has
	/// been rewound. The running process may have accumulated entries with
	/// seqs that are now greater than the restored `max_seq`; those would
	/// falsely conflict with new post-restore txns.
	///
	/// NOT called at startup — see `set_seq_num` above.
	pub(crate) fn reset_oracle_for_restore(&self, max_seq: u64) {
		self.oracle.reset_for_restore(max_seq);
	}

	pub(crate) fn commit(&self, batch: Batch, sync: bool, start_seq: u64) -> Result<()> {
		if self.shutdown.load(Ordering::Acquire) {
			return Err(Error::PipelineStall);
		}

		// Check for background errors before proceeding
		self.env.check_background_error()?;

		if batch.is_empty() {
			return Ok(());
		}

		// Check write stall BEFORE entering the group (no lock held). Sync: a
		// stalled committer blocks its own thread; the flush that clears the
		// stall runs on a dedicated std::thread, so nothing is starved.
		self.write_stall.check()?;

		// Pre-serialize OUTSIDE the group/lock: clone+wrap values and encode the
		// batch (the expensive part). On failure nothing has been allocated,
		// enqueued, or written — `?` simply returns.
		let prepared = self.env.pre_serialize(&batch)?;
		let count = prepared.processed_batch.count();
		drop(batch); // keys now live in `prepared.processed_batch`

		let commit_batch = CommitBatch::new(count, std::thread::current());

		// Stack-allocated write-group node (RocksDB `Writer` model). Stable
		// address for this function's duration; shared with the leader via `*mut`.
		let req = WriteRequest {
			bytes: UnsafeCell::new(prepared.bytes),
			processed_batch: UnsafeCell::new(prepared.processed_batch),
			commit_batch: Arc::clone(&commit_batch),
			start_seq,
			sync,
			link_next: AtomicPtr::new(ptr::null_mut()),
			state: AtomicU8::new(WS_INIT),
			outcome: UnsafeCell::new(None),
			thread: std::thread::current(),
		};
		let wp: *mut WriteRequest = &req as *const WriteRequest as *mut WriteRequest;

		// Join the write group. The first committer to link an empty list is the
		// LEADER: it runs oracle/seq/enqueue/WAL for the WHOLE group under one
		// `write_mutex` hold + one flush. Followers wait (spin → yield → park).
		let is_leader = self.join_group(wp);
		if is_leader {
			self.run_leader_group(wp);
		} else {
			spin_yield_park(|| req.state.load(Ordering::Acquire) != WS_INIT);
		}

		// WAL outcome (set by the leader, Acquire-synchronized with `state`).
		if req.state.load(Ordering::Acquire) == WS_WAL_ERR {
			// SAFETY: leader wrote `outcome` before the Release store of `state`.
			// For an enqueued member it also rolled back the oracle, completed our
			// CommitBatch with the error, and drained it from `pending`; a
			// conflicting member was never enqueued.
			let err = unsafe { (*req.outcome.get()).take() }.unwrap_or(Error::PipelineStall);
			return Err(err);
		}

		// === Memtable apply — OUTSIDE any lock, in PARALLEL across group members.
		// SAFETY: the leader stamped our seq into `processed_batch` before the
		// Release store of WS_WAL_OK (observed above, Acquire) and no longer
		// touches the node.
		let apply_result = self.env.apply(unsafe { &*req.processed_batch.get() });

		let apply_err = if let Err(ref e) = apply_result {
			// Roll back this txn's oracle entries (seq-match guarded) so a
			// subsequent same-key commit doesn't false-abort on a ghost stamp.
			let pb = unsafe { &*req.processed_batch.get() };
			let c = pb.count() as u64;
			let stamp = commit_batch.get_seq_num() + c - 1;
			self.oracle.rollback(pb.entries.iter().map(|e| e.key.as_slice()), stamp);
			// Complete with Err FIRST, then mark_applied, so a concurrent
			// publish() can't dequeue and complete(Ok) before our Err lands.
			let err = Error::CommitFail(e.to_string());
			commit_batch.complete(Err(err.clone()));
			Some(err)
		} else {
			None
		};

		commit_batch.mark_applied();

		// Publish (multi-consumer) - MUST always run to drain the queue.
		self.publish();

		if let Some(err) = apply_err {
			return Err(err);
		}

		commit_batch.wait()
	}

	/// CAS-link `w` onto the write group (Treiber push). Returns `true` if this
	/// committer linked an empty list and is therefore the group LEADER.
	fn join_group(&self, w: *mut WriteRequest) -> bool {
		loop {
			let head = self.newest_writer.load(Ordering::Acquire);
			// SAFETY: `w` is our own node, not yet visible to other threads.
			unsafe { (*w).link_next.store(head, Ordering::Relaxed) };
			if self
				.newest_writer
				.compare_exchange_weak(head, w, Ordering::AcqRel, Ordering::Acquire)
				.is_ok()
			{
				return head.is_null();
			}
		}
	}

	/// Leader path: drain the write group and, under a SINGLE `write_mutex`
	/// acquisition, run oracle.check → seq alloc → oracle.publish → enqueue →
	/// stamp-seq for each member in FIFO order, then append ALL records + flush
	/// once. Finally signal every follower its WAL outcome (targeted `unpark`).
	///
	/// Invariant: `write_mutex` is held across the ENTIRE member loop (never
	/// released mid-group) and `fetch_add`+`enqueue` are adjacent per member, so
	/// enqueue order == seq order (the FIFO `pending` queue requires it).
	fn run_leader_group(&self, self_w: *mut WriteRequest) {
		let guard = self.write_mutex.lock();

		// Close the group: steal the whole stack. Anyone linking after this sees
		// an empty head and becomes the next group's leader.
		let mut node = self.newest_writer.swap(ptr::null_mut(), Ordering::AcqRel);

		// Treiber stack is newest-first; reverse to FIFO (= arrival / seq order).
		let mut group: Vec<*mut WriteRequest> = Vec::new();
		while !node.is_null() {
			group.push(node);
			// SAFETY: nodes stay alive (owners blocked) until we set their state.
			node = unsafe { (*node).link_next.load(Ordering::Relaxed) };
		}
		group.reverse();

		let mut records: Vec<&[u8]> = Vec::with_capacity(group.len());
		let mut enqueued: Vec<*mut WriteRequest> = Vec::with_capacity(group.len());

		for &w in &group {
			// SAFETY: leader-exclusive access (owner parked); see WriteRequest doc.
			let pb = unsafe { &mut *(*w).processed_batch.get() };
			let cnt = pb.count() as u64;
			let start_seq = unsafe { (*w).start_seq };

			// Conflict check. A failing member consumes NO seq and is NOT
			// enqueued; record its error in `outcome`.
			if let Err(e) =
				self.oracle.check(pb.entries.iter().map(|e| e.key.as_slice()), start_seq)
			{
				unsafe { *(*w).outcome.get() = Some(e) };
				continue;
			}

			let seq = self.log_seq_num.fetch_add(cnt, Ordering::SeqCst);
			let oldest_active = self.env.oldest_active_start_seq().min(start_seq);
			// Publish BEFORE the next member's check so intra-group same-key
			// write-write conflicts are detected.
			self.oracle.publish(
				pb.entries.iter().map(|e| e.key.as_slice()),
				seq,
				cnt,
				oldest_active,
			);

			unsafe { (*w).commit_batch.set_seq_num(seq) };
			pb.set_starting_seq_num(seq);
			// Stamp the seq into the pre-encoded WAL record in place.
			let bytes = unsafe { &mut *(*w).bytes.get() };
			Batch::patch_encoded_seq(bytes, seq);

			// Enqueue (single producer = leader, FIFO/seq order).
			unsafe { self.pending.enqueue(Arc::clone(&(*w).commit_batch)) };

			// SAFETY: slice valid for this fn — the node lives in `group` and
			// owners don't touch `bytes` after linking.
			let slice_ptr: *const [u8] = bytes.as_slice();
			records.push(unsafe { &*slice_ptr });
			enqueued.push(w);
		}

		let need_sync = enqueued.iter().any(|&w| unsafe { (*w).sync });

		let wal_failed = if records.is_empty() {
			false
		} else {
			match self.env.wal_append_group(&records, need_sync) {
				Ok(()) => false,
				Err(e) => {
					// Coalesced WAL write failed: every enqueued member is in
					// doubt. Roll back (seq-match guarded), complete(Err), and
					// mark applied so `publish()` drains it. Record the error.
					for &w in &enqueued {
						let pb = unsafe { &*(*w).processed_batch.get() };
						let c = pb.count() as u64;
						let stamp = unsafe { (*w).commit_batch.get_seq_num() } + c - 1;
						self.oracle.rollback(pb.entries.iter().map(|e| e.key.as_slice()), stamp);
						unsafe {
							(*w).commit_batch.complete(Err(e.clone()));
							(*w).commit_batch.mark_applied();
							*(*w).outcome.get() = Some(e.clone());
						}
					}
					true
				}
			}
		};

		drop(guard);

		// Followers return early on WS_WAL_ERR and never call publish(), so the
		// leader must drain the failed-but-enqueued batches here.
		if wal_failed {
			self.publish();
		}

		// Signal every member its WAL outcome. A member is WS_WAL_OK iff it has
		// no recorded `outcome` (passed check AND the WAL write succeeded).
		// Clone the thread BEFORE the Release store of `state`: once a follower
		// observes the state it may return and free its stack node.
		for &w in &group {
			let st = if unsafe { (*(*w).outcome.get()).is_some() } {
				WS_WAL_ERR
			} else {
				WS_WAL_OK
			};
			if w == self_w {
				// Leader reads its own state after this function returns.
				unsafe { (*w).state.store(st, Ordering::Release) };
			} else {
				let t = unsafe { (*w).thread.clone() };
				unsafe { (*w).state.store(st, Ordering::Release) };
				t.unpark();
			}
		}
	}

	#[cfg(test)]
	pub(crate) fn oracle(&self) -> &Arc<CommitOracle> {
		&self.oracle
	}

	fn publish(&self) {
		// Multi-consumer publish loop
		loop {
			let dequeued = self.pending.dequeue_applied();

			match dequeued {
				Some(batch) => {
					// Publish this batch's sequence number
					let new_visible = batch.get_seq_num() + batch.count as u64 - 1;

					loop {
						let current = self.visible_seq_num.load(Ordering::Acquire);
						if new_visible <= current {
							// Already published by another thread
							break;
						}

						if self
							.visible_seq_num
							.compare_exchange_weak(
								current,
								new_visible,
								Ordering::Release,
								Ordering::Relaxed,
							)
							.is_ok()
						{
							break;
						}
					}

					// Complete this batch
					batch.complete(Ok(()));
				}
				None => {
					// No more applied batches, done
					break;
				}
			}
		}
	}

	pub(crate) fn get_visible_seq_num(&self) -> u64 {
		self.visible_seq_num.load(Ordering::Acquire)
	}

	pub(crate) fn shutdown(&self) {
		self.shutdown.store(true, Ordering::Release);
	}
}

impl Drop for CommitPipeline {
	fn drop(&mut self) {
		self.shutdown();
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use test_log::test;

	use super::*;
	use crate::InternalKeyKind;

	fn test_visible_seq_num() -> Arc<AtomicU64> {
		Arc::new(AtomicU64::new(0))
	}

	struct MockStallProvider;

	impl crate::stall::WriteStallCountProvider for MockStallProvider {
		fn get_stall_counts(&self) -> crate::stall::StallCounts {
			crate::stall::StallCounts {
				immutable_memtables: 0,
				l0_files: 0,
			}
		}
	}

	fn test_write_stall() -> Arc<crate::stall::WriteStallController> {
		let provider: Arc<dyn crate::stall::WriteStallCountProvider> = Arc::new(MockStallProvider);
		let thresholds = crate::stall::StallThresholds {
			memtable_limit: 2,
			l0_file_limit: 12,
		};
		Arc::new(crate::stall::WriteStallController::new(provider, thresholds))
	}

	// Test helpers mirroring the old `write` (copy batch + encode), split into
	// the new pre_serialize / write_prepared shape.
	fn mock_pre_serialize(batch: &Batch) -> Result<PreparedWrite> {
		let mut new_batch = Batch::new(0);
		for entry in batch.entries() {
			new_batch.add_record(
				entry.kind,
				entry.key.clone(),
				entry.value.clone(),
				entry.timestamp,
			)?;
		}
		let bytes = new_batch.encode()?;
		Ok(PreparedWrite {
			processed_batch: new_batch,
			bytes,
		})
	}

	struct MockEnv;

	impl CommitEnv for MockEnv {
		fn pre_serialize(&self, batch: &Batch) -> Result<PreparedWrite> {
			mock_pre_serialize(batch)
		}

		fn wal_append_group(&self, _records: &[&[u8]], _sync: bool) -> Result<()> {
			Ok(())
		}

		fn apply(&self, _batch: &Batch) -> Result<()> {
			Ok(())
		}

		fn check_background_error(&self) -> Result<()> {
			Ok(())
		}

		fn oldest_active_start_seq(&self) -> u64 {
			0
		}
	}

	#[test(tokio::test)]
	async fn test_single_commit() {
		let pipeline =
			CommitPipeline::new(Arc::new(MockEnv), test_visible_seq_num(), test_write_stall());

		let mut batch = Batch::new(0);
		batch
			.add_record(InternalKeyKind::Set, b"key1".to_vec(), Some(b"value1".to_vec()), 0)
			.unwrap();

		let result = pipeline.commit(batch, false, 0);
		assert!(result.is_ok(), "Single commit failed: {result:?}");

		let visible = pipeline.get_visible_seq_num();
		assert_eq!(
			visible, 1,
			"Expected visible=1 after one commit with count=1 (highest seq num used)"
		);

		pipeline.shutdown();
	}

	#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
	async fn test_sequential_commits() {
		let pipeline =
			CommitPipeline::new(Arc::new(MockEnv), test_visible_seq_num(), test_write_stall());

		// First test sequential commits to verify basic functionality
		for i in 0..5 {
			let mut batch = Batch::new(0);
			batch
				.add_record(
					InternalKeyKind::Set,
					format!("key{i}").into_bytes(),
					Some(vec![1, 2, 3]),
					i,
				)
				.unwrap();
			let result = pipeline.commit(batch, false, 0);
			assert!(result.is_ok(), "Sequential commit {i} failed: {result:?}");
		}

		assert_eq!(pipeline.get_visible_seq_num(), 5);

		pipeline.shutdown();
	}

	#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
	async fn test_concurrent_commits() {
		let pipeline =
			CommitPipeline::new(Arc::new(MockEnv), test_visible_seq_num(), test_write_stall());

		let mut handles = vec![];
		for i in 0..10 {
			let pipeline = Arc::clone(&pipeline);
			let handle = tokio::spawn(async move {
				let mut batch = Batch::new(0);
				batch
					.add_record(
						InternalKeyKind::Set,
						format!("key{i}").into_bytes(),
						Some(vec![1, 2, 3]),
						i,
					)
					.unwrap();
				pipeline.commit(batch, false, 0)
			});
			handles.push(handle);
		}

		for (i, handle) in handles.into_iter().enumerate() {
			let result = handle.await.unwrap();
			assert!(result.is_ok(), "Commit {i} failed: {result:?}");
		}

		// Give time for all publishing to complete
		let start = std::time::Instant::now();
		while pipeline.get_visible_seq_num() < 10 && start.elapsed() < Duration::from_secs(5) {
			tokio::time::sleep(Duration::from_millis(10)).await;
		}

		// Verify sequence numbers are published correctly
		assert_eq!(pipeline.get_visible_seq_num(), 10, "Not all batches were published");

		// Shutdown the pipeline
		pipeline.shutdown();
	}

	struct DelayedMockEnv;

	impl CommitEnv for DelayedMockEnv {
		fn pre_serialize(&self, batch: &Batch) -> Result<PreparedWrite> {
			mock_pre_serialize(batch)
		}

		fn wal_append_group(&self, _records: &[&[u8]], _sync: bool) -> Result<()> {
			// Simulate WAL write cost for the group (as the old `write` did).
			let start = std::time::Instant::now();
			while start.elapsed() < Duration::from_micros(100) {
				std::hint::spin_loop();
			}
			Ok(())
		}

		fn apply(&self, _batch: &Batch) -> Result<()> {
			let start = std::time::Instant::now();
			while start.elapsed() < Duration::from_micros(50) {
				std::hint::spin_loop();
			}
			Ok(())
		}

		fn check_background_error(&self) -> Result<()> {
			Ok(())
		}

		fn oldest_active_start_seq(&self) -> u64 {
			0
		}
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_concurrent_commits_with_delays() {
		let pipeline = CommitPipeline::new(
			Arc::new(DelayedMockEnv),
			test_visible_seq_num(),
			test_write_stall(),
		);

		let mut handles = vec![];
		for i in 0..5 {
			let pipeline = Arc::clone(&pipeline);
			let handle = tokio::spawn(async move {
				let mut batch = Batch::new(0);
				batch
					.add_record(
						InternalKeyKind::Set,
						format!("key{i}").into_bytes(),
						Some(vec![1, 2, 3]),
						i,
					)
					.unwrap();
				pipeline.commit(batch, false, 0)
			});
			handles.push(handle);
		}

		for handle in handles {
			assert!(handle.await.unwrap().is_ok());
		}

		// Verify sequence numbers are published correctly
		assert_eq!(pipeline.get_visible_seq_num(), 5);

		// Shutdown the pipeline
		pipeline.shutdown();
	}

	// ==========================================================================
	// TESTS FOR QUEUE OVERFLOW BUG FIX
	// ==========================================================================
	//
	// Add these tests to your existing #[cfg(test)] mod tests { ... }
	//
	// To verify the bug exists (before fix):
	//   1. Comment out your fix
	//   2. Run: cargo test test_queue_overflow --nocapture
	//   3. Should panic with "commit queue overflow - should not be reached"
	//
	// To verify the fix works (after fix):
	//   1. Apply the fix
	//   2. Run: cargo test test_queue_overflow
	//   3. Should pass
	// ==========================================================================

	struct AlwaysFailApplyEnv;

	impl CommitEnv for AlwaysFailApplyEnv {
		fn pre_serialize(&self, batch: &Batch) -> Result<PreparedWrite> {
			mock_pre_serialize(batch)
		}

		fn wal_append_group(&self, _records: &[&[u8]], _sync: bool) -> Result<()> {
			Ok(())
		}

		fn apply(&self, _batch: &Batch) -> Result<()> {
			Err(Error::CommitFail("simulated apply failure".into()))
		}

		fn check_background_error(&self) -> Result<()> {
			Ok(())
		}

		fn oldest_active_start_seq(&self) -> u64 {
			0
		}
	}

	/// Minimal reproduction: all applies fail.
	///
	/// WITHOUT FIX: panics at iteration 8 with "commit queue overflow"
	/// WITH FIX: completes all 20 iterations, returning errors
	#[test(tokio::test)]
	async fn test_queue_overflow_all_fail() {
		let pipeline = CommitPipeline::new(
			Arc::new(AlwaysFailApplyEnv),
			test_visible_seq_num(),
			test_write_stall(),
		);

		for i in 0..20 {
			let mut batch = Batch::new(0);
			batch
				.add_record(
					InternalKeyKind::Set,
					format!("key{i}").into_bytes(),
					Some(b"value".to_vec()),
					0,
				)
				.unwrap();

			let result = pipeline.commit(batch, false, 0);
			assert!(result.is_err(), "Expected error at iteration {i}");
		}

		pipeline.shutdown();
	}

	struct FailNTimesEnv {
		// Track how many calls have been made
		call_count: std::sync::atomic::AtomicUsize,
		// Fail the first N calls
		fail_until: usize,
	}

	impl FailNTimesEnv {
		fn new(fail_count: usize) -> Self {
			Self {
				call_count: std::sync::atomic::AtomicUsize::new(0),
				fail_until: fail_count,
			}
		}
	}

	impl CommitEnv for FailNTimesEnv {
		fn pre_serialize(&self, batch: &Batch) -> Result<PreparedWrite> {
			mock_pre_serialize(batch)
		}

		fn wal_append_group(&self, _records: &[&[u8]], _sync: bool) -> Result<()> {
			Ok(())
		}

		fn apply(&self, _batch: &Batch) -> Result<()> {
			// Increment call count and get previous value
			let call_num = self.call_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

			// Fail if we haven't reached fail_until yet
			if call_num < self.fail_until {
				Err(Error::CommitFail("simulated failure".into()))
			} else {
				Ok(())
			}
		}

		fn check_background_error(&self) -> Result<()> {
			Ok(())
		}

		fn oldest_active_start_seq(&self) -> u64 {
			0
		}
	}

	/// Reproduction: first N fail, then succeed.
	///
	/// WITHOUT FIX: panics when attempting commit after failures fill queue
	/// WITH FIX: fails N times, then succeeds
	#[test(tokio::test)]
	async fn test_queue_overflow_partial_fail() {
		let fail_count = 10; // Fail first 10, then succeed
		let env = Arc::new(FailNTimesEnv::new(fail_count));
		let pipeline = CommitPipeline::new(env, test_visible_seq_num(), test_write_stall());

		for i in 0..20 {
			let mut batch = Batch::new(0);
			batch
				.add_record(
					InternalKeyKind::Set,
					format!("key{i}").into_bytes(),
					Some(b"value".to_vec()),
					i as u64,
				)
				.unwrap();

			let result = pipeline.commit(batch, false, 0);

			if i < fail_count {
				assert!(result.is_err(), "Expected error at iteration {i}");
			} else {
				assert!(result.is_ok(), "Expected success at iteration {i}, got {result:?}");
			}
		}

		// Verify visible_seq_num advanced for successful commits
		// Successful commits: 10..20, each with 1 entry
		// Sequence numbers: fails consumed 1-10, successes got 11-20
		let visible = pipeline.get_visible_seq_num();
		assert_eq!(visible, 20, "Expected visible_seq_num=20");

		pipeline.shutdown();
	}

	/// When `apply()` fails, the commit pipeline must roll the oracle entries
	/// for the failed batch back. Otherwise a subsequent same-key transaction
	/// would false-abort against a "ghost" stamp left behind by a non-applied
	/// commit. Verified directly against `CommitPipeline` with an injected
	/// failure on the first apply.
	#[test(tokio::test)]
	async fn test_apply_failure_releases_oracle_entry() {
		let env = Arc::new(FailNTimesEnv::new(1)); // First apply fails, then succeeds.
		let pipeline = CommitPipeline::new(env, test_visible_seq_num(), test_write_stall());

		// First commit on key K: apply fails. Oracle entry stamped at the
		// allocated seq, then rolled back inside `commit()` on the apply error.
		let mut batch = Batch::new(0);
		batch.add_record(InternalKeyKind::Set, b"K".to_vec(), Some(b"v1".to_vec()), 0).unwrap();
		let r1 = pipeline.commit(batch, false, 0);
		assert!(r1.is_err(), "expected first apply to fail, got {r1:?}");
		assert_eq!(pipeline.oracle().len(), 0, "rollback should have removed the entry");

		// Second commit on the same key K with start_seq=0 (i.e. the same
		// "old" snapshot the failed one was reasoning from). With rollback,
		// the oracle is empty so this MUST succeed — no ghost conflict.
		let mut batch = Batch::new(0);
		batch.add_record(InternalKeyKind::Set, b"K".to_vec(), Some(b"v2".to_vec()), 0).unwrap();
		let r2 = pipeline.commit(batch, false, 0);
		assert!(r2.is_ok(), "second commit on K should succeed (no ghost), got {r2:?}");

		pipeline.shutdown();
	}

	/// A misbehaving `CommitEnv` reporting `oldest_active_start_seq` >
	/// `start_seq` (e.g. caller bypassed `Transaction::new` and the
	/// `(None, None)` fallback returned an over-large `visible_seq_num`)
	/// must NOT advance `kept_since` past the committing txn's snapshot.
	/// Change A clamps `oldest_active = env.oldest_active_start_seq().min(start_seq)`
	/// in the publish call site.
	struct OverreportingEnv {
		overreport: u64,
	}

	impl CommitEnv for OverreportingEnv {
		fn pre_serialize(&self, batch: &Batch) -> Result<PreparedWrite> {
			mock_pre_serialize(batch)
		}

		fn wal_append_group(&self, _records: &[&[u8]], _sync: bool) -> Result<()> {
			Ok(())
		}

		fn apply(&self, _batch: &Batch) -> Result<()> {
			Ok(())
		}

		fn check_background_error(&self) -> Result<()> {
			Ok(())
		}

		fn oldest_active_start_seq(&self) -> u64 {
			self.overreport
		}
	}

	#[test(tokio::test)]
	async fn test_publish_clamps_oldest_active_by_start_seq() {
		use crate::oracle::GC_INTERVAL;

		// Env reports an `oldest_active` way above any plausible start_seq.
		let env = Arc::new(OverreportingEnv {
			overreport: 10_000_000,
		});
		let pipeline = CommitPipeline::new(env, test_visible_seq_num(), test_write_stall());

		// Drive enough commits past GC_INTERVAL with a tiny start_seq (= 0).
		// Without the clamp, the GC body would advance `kept_since` to
		// 10_000_000 and every subsequent `check(start_seq=0)` would
		// TransactionRetry. With the clamp, `kept_since` cannot exceed
		// the committer's start_seq (= 0 here), so it stays at 0.
		for i in 0..(GC_INTERVAL + 2) {
			let mut batch = Batch::new(0);
			batch
				.add_record(
					InternalKeyKind::Set,
					format!("k{i}").into_bytes(),
					Some(b"v".to_vec()),
					0,
				)
				.unwrap();
			let r = pipeline.commit(batch, false, 0);
			assert!(r.is_ok(), "iter {i}: commit must succeed, got {r:?}");
		}

		// Sanity: kept_since never advanced past the per-commit start_seq=0.
		assert_eq!(
			pipeline.oracle().kept_since(),
			0,
			"clamp must hold kept_since at the committer's start_seq",
		);

		pipeline.shutdown();
	}
}
