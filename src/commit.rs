// This commit pipeline is inspired by Pebble's commit pipeline.

use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{oneshot, Semaphore};

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::oracle::CommitOracle;
use crate::stall::WriteStallController;

const MAX_CONCURRENT_COMMITS: usize = 8;
const DEQUEUE_BITS: u32 = 32;

// Trait for commit operations
pub trait CommitEnv: Send + Sync + 'static {
	// Write batch to WAL and process VLog entries (synchronous operation)
	// Returns a new batch with VLog pointers applied
	fn write(&self, batch: &Batch, seq_num: u64, sync: bool) -> Result<Batch>;

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

// Lock-free commit queue entry
struct CommitBatch {
	seq_num: AtomicU64,
	count: u32, // Number of entries in the batch
	applied: AtomicBool,
	complete_tx: Mutex<Option<oneshot::Sender<Result<()>>>>,
}

impl CommitBatch {
	fn new(count: u32) -> (Arc<Self>, oneshot::Receiver<Result<()>>) {
		let (tx, rx) = oneshot::channel();
		let commit = Arc::new(Self {
			seq_num: AtomicU64::new(0),
			count,
			applied: AtomicBool::new(false),
			complete_tx: Mutex::new(Some(tx)),
		});
		(commit, rx)
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

	fn complete(&self, result: Result<()>) {
		let mut guard = self.complete_tx.lock();
		if let Some(tx) = guard.take() {
			let _ = tx.send(result);
		}
	}
}

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
	slots: [AtomicPtr<CommitBatch>; MAX_CONCURRENT_COMMITS],
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
		if tail.wrapping_add(MAX_CONCURRENT_COMMITS as u32) == head {
			// Queue is full. This should never be reached because the semaphore
			// limits the number of concurrent operations.
			panic!("commit queue overflow - should not be reached");
		}

		let slot_idx = (head & (MAX_CONCURRENT_COMMITS as u32 - 1)) as usize;
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

			let slot_idx = (tail & (MAX_CONCURRENT_COMMITS as u32 - 1)) as usize;
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
	// Semaphore for flow control
	commit_sem: Arc<Semaphore>,
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
			commit_sem: Arc::new(Semaphore::new(MAX_CONCURRENT_COMMITS - 1)),
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

	pub(crate) async fn commit(&self, mut batch: Batch, sync: bool, start_seq: u64) -> Result<()> {
		if self.shutdown.load(Ordering::Acquire) {
			return Err(Error::PipelineStall);
		}

		// Check for background errors before proceeding
		self.env.check_background_error()?;

		if batch.is_empty() {
			return Ok(());
		}

		// Check write stall BEFORE acquiring any locks.
		// This ensures stalled writers wait here without blocking others.
		self.write_stall.check().await?;

		// Acquire permit for flow control
		let _permit = self.commit_sem.acquire().await.map_err(|_| Error::PipelineStall)?;

		let (commit_batch, complete_rx) = CommitBatch::new(batch.count());

		// === CRITICAL SECTION under write_mutex ===
		//
		// Atomically: validate write keys against the oracle map, allocate
		// the commit seq, insert oracle entries with that seq, enqueue, and
		// write to WAL. The single-producer invariant on `pending` requires
		// seq allocation and `enqueue` to be in the same critical section
		// (otherwise the FIFO `dequeue_applied` invariant breaks).
		//
		// Memtable `apply()` is intentionally OUTSIDE this block — that is
		// the throughput unlock. While T1 is applying, T2 can enter
		// `write_mutex` and run validate + seq + oracle + WAL.
		//
		// Two distinct failure modes inside the section:
		//   1. `oracle.check` failure: nothing has been allocated, enqueued, or written. Propagate
		//      the error with `?`; no cleanup.
		//   2. `env.write` (WAL) failure: oracle entries were already published and the batch was
		//      enqueued. Roll back those entries (seq-match guard preserves concurrent
		//      overwriters), drain the queue slot, release the lock, and return the error.
		//
		// Keys are derived from `batch.entries` (single source of truth).
		// Duplicate keys within a batch (e.g. from savepoint history) are
		// harmless: oracle.check/publish are idempotent on the same key.
		let (processed_batch, allocated_seq): (Batch, u64) = {
			let _guard = self.write_mutex.lock();

			// Validate against the oracle. No state has changed yet; on
			// failure `?` simply returns the error to the caller.
			self.oracle.check(batch.entries.iter().map(|e| e.key.as_slice()), start_seq)?;

			let count = batch.count() as u64;
			let seq_num = self.log_seq_num.fetch_add(count, Ordering::SeqCst);

			// Publish the oracle entries with the allocated seq.
			//
			// Clamp `oldest_active` by this txn's own `start_seq`. Defense-in-depth
			// against a caller that drives `Core::commit` without registering in
			// `active_txn_tracker` (the `(None, None)` fallback in
			// `oldest_active_start_seq` returns `visible_seq_num`, which may exceed
			// `start_seq` once `visible_seq_num` has advanced past the committing
			// txn's snapshot). With this clamp, `kept_since` can never advance past
			// the committing txn's own snapshot — regardless of caller hygiene.
			let oldest_active = self.env.oldest_active_start_seq().min(start_seq);
			self.oracle.publish(
				batch.entries.iter().map(|e| e.key.as_slice()),
				seq_num,
				count,
				oldest_active,
			);

			// Stamp the commit_batch & batch.
			commit_batch.set_seq_num(seq_num);
			batch.set_starting_seq_num(seq_num);

			// Enqueue (single producer, same critical section as seq alloc).
			self.pending.enqueue(Arc::clone(&commit_batch));

			// WAL + VLog (serialized under lock).
			match self.env.write(&batch, seq_num, sync) {
				Ok(processed) => (processed, seq_num),
				Err(e) => {
					// WAL failed AFTER oracle.publish. Roll back the entries
					// we stamped; the seq-match guard leaves concurrent
					// overwriters untouched.
					let stamp = seq_num + count - 1;
					self.oracle.rollback(batch.entries.iter().map(|e| e.key.as_slice()), stamp);
					// The batch is in `pending` and was never marked applied.
					// Order matters: complete with Err FIRST, then mark_applied,
					// so a concurrent publish() can't dequeue and call
					// complete(Ok) before our Err is set.
					commit_batch.complete(Err(e.clone()));
					commit_batch.mark_applied();
					// Release write_mutex before draining the queue.
					drop(_guard);
					self.publish();
					return Err(e);
				}
			}
		};
		// === END CRITICAL SECTION ===

		// Memtable apply — OUTSIDE write_mutex. The next committer can already
		// be inside the critical section. This restores the pipeline overlap
		// that PR #378 destroyed.
		let apply_result = self.env.apply(&processed_batch);

		// =========================================================================
		// Failure-path invariants
		// =========================================================================
		// (a) Zombie prevention: every enqueued batch MUST eventually be
		//     marked applied and drained from `pending`, otherwise the queue
		//     fills and panics with "commit queue overflow" (see [c]).
		// (b) Oracle rollback on apply failure: when apply fails, the WAL
		//     record is durable but the memtable did not receive it. Other
		//     in-flight txns that would conflict with this seq's stamp must
		//     not be falsely aborted, so we roll back the oracle entries
		//     for this batch (seq-match guarded — concurrent overwriters
		//     keep their stamps).
		// (c) Sequence number gaps: the allocated `seq_num` is consumed
		//     irrespective of apply success. On WAL replay after restart,
		//     a WAL-durable but apply-failed entry will be re-inserted into
		//     the memtable; a later same-key writer's higher seq shadows it.
		// =========================================================================

		let apply_err = if let Err(ref e) = apply_result {
			// Roll back this txn's oracle entries so subsequent same-key
			// commits don't false-abort against a ghost stamp.
			let count = batch.count() as u64;
			let stamp = allocated_seq + count - 1;
			self.oracle.rollback(batch.entries.iter().map(|e| e.key.as_slice()), stamp);

			// Order matters: complete with Err FIRST, then mark_applied below.
			// Otherwise a concurrent publish() could dequeue the (already
			// applied) batch and call complete(Ok) before our Err lands.
			let err = Error::CommitFail(e.to_string());
			commit_batch.complete(Err(err.clone()));
			Some(err)
		} else {
			None
		};

		commit_batch.mark_applied();

		// Publish (multi-consumer) - MUST always run to drain queue
		self.publish();

		if let Some(err) = apply_err {
			return Err(err);
		}

		complete_rx.await.map_err(|_| Error::PipelineStall)?
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

	struct MockEnv;

	impl CommitEnv for MockEnv {
		fn write(&self, batch: &Batch, _seq_num: u64, _sync: bool) -> Result<Batch> {
			// Create a copy of the batch for testing
			let mut new_batch = Batch::new(_seq_num);
			for entry in batch.entries() {
				new_batch.add_record(
					entry.kind,
					entry.key.clone(),
					entry.value.clone(),
					entry.timestamp,
				)?;
			}
			Ok(new_batch)
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

		let result = pipeline.commit(batch, false, 0).await;
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
			let result = pipeline.commit(batch, false, 0).await;
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
				pipeline.commit(batch, false, 0).await
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
		fn write(&self, batch: &Batch, _seq_num: u64, _sync: bool) -> Result<Batch> {
			let start = std::time::Instant::now();
			while start.elapsed() < Duration::from_micros(100) {
				std::hint::spin_loop();
			}
			// Create a copy of the batch for testing
			let mut new_batch = Batch::new(_seq_num);
			for entry in batch.entries() {
				new_batch.add_record(
					entry.kind,
					entry.key.clone(),
					entry.value.clone(),
					entry.timestamp,
				)?;
			}
			Ok(new_batch)
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
				pipeline.commit(batch, false, 0).await
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
		fn write(&self, batch: &Batch, seq_num: u64, _sync: bool) -> Result<Batch> {
			let mut new_batch = Batch::new(seq_num);
			for entry in batch.entries() {
				new_batch.add_record(
					entry.kind,
					entry.key.clone(),
					entry.value.clone(),
					entry.timestamp,
				)?;
			}
			Ok(new_batch)
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

			let result = pipeline.commit(batch, false, 0).await;
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
		fn write(&self, batch: &Batch, seq_num: u64, _sync: bool) -> Result<Batch> {
			let mut new_batch = Batch::new(seq_num);
			for entry in batch.entries() {
				new_batch.add_record(
					entry.kind,
					entry.key.clone(),
					entry.value.clone(),
					entry.timestamp,
				)?;
			}
			Ok(new_batch)
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

			let result = pipeline.commit(batch, false, 0).await;

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
		let r1 = pipeline.commit(batch, false, 0).await;
		assert!(r1.is_err(), "expected first apply to fail, got {r1:?}");
		assert_eq!(pipeline.oracle().len(), 0, "rollback should have removed the entry");

		// Second commit on the same key K with start_seq=0 (i.e. the same
		// "old" snapshot the failed one was reasoning from). With rollback,
		// the oracle is empty so this MUST succeed — no ghost conflict.
		let mut batch = Batch::new(0);
		batch.add_record(InternalKeyKind::Set, b"K".to_vec(), Some(b"v2".to_vec()), 0).unwrap();
		let r2 = pipeline.commit(batch, false, 0).await;
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
		fn write(&self, batch: &Batch, seq_num: u64, _sync: bool) -> Result<Batch> {
			let mut new_batch = Batch::new(seq_num);
			for entry in batch.entries() {
				new_batch.add_record(
					entry.kind,
					entry.key.clone(),
					entry.value.clone(),
					entry.timestamp,
				)?;
			}
			Ok(new_batch)
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
			let r = pipeline.commit(batch, false, 0).await;
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
