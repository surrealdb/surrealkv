// This commit pipeline is inspired by Pebble's commit pipeline.

use std::sync::{
	atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering},
	Arc, Mutex,
};

use tokio::sync::{oneshot, Semaphore};

use crate::{
	batch::Batch,
	error::{Error, Result},
};

const MAX_CONCURRENT_COMMITS: usize = 16;
const DEQUEUE_BITS: u32 = 32;

// Trait for commit operations
pub trait CommitEnv: Send + Sync + 'static {
	// Write batch to WAL and process VLog entries (synchronous operation)
	// Returns a new batch with VLog pointers applied
	fn write(&self, batch: &Batch, seq_num: u64, sync: bool) -> Result<Batch>;

	// Apply processed batch to memtable
	fn apply(&self, batch: &Batch) -> Result<()>;
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
		if let Ok(mut guard) = self.complete_tx.try_lock() {
			if let Some(tx) = guard.take() {
				let _ = tx.send(result);
			}
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

	// Multi-consumer dequeue - removes the earliest enqueued Batch, if it is applied
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
	// Single producer - only one thread can write to WAL at a time
	write_mutex: Mutex<()>,
	// Lock-free single-producer, multi-consumer commit queue
	pending: CommitQueue,
	// Semaphore for flow control
	commit_sem: Arc<Semaphore>,
	shutdown: AtomicBool,
}

impl CommitPipeline {
	pub(crate) fn new(env: Arc<dyn CommitEnv>) -> Arc<Self> {
		Arc::new(Self {
			env,
			log_seq_num: AtomicU64::new(1),
			visible_seq_num: Arc::new(AtomicU64::new(0)),
			write_mutex: Mutex::new(()),
			pending: CommitQueue::new(),
			commit_sem: Arc::new(Semaphore::new(MAX_CONCURRENT_COMMITS - 1)),
			shutdown: AtomicBool::new(false),
		})
	}

	pub(crate) fn set_seq_num(&self, seq_num: u64) {
		if seq_num > 0 {
			self.visible_seq_num.store(seq_num, Ordering::Release);
			self.log_seq_num.store(seq_num + 1, Ordering::Release);
		}
	}

	pub(crate) async fn commit(&self, mut batch: Batch, sync: bool) -> Result<()> {
		if self.shutdown.load(Ordering::Acquire) {
			return Err(Error::PipelineStall);
		}

		if batch.is_empty() {
			return Ok(());
		}

		// Acquire permit for flow control
		let _permit = self.commit_sem.acquire().await.map_err(|_| Error::PipelineStall)?;

		let (commit_batch, complete_rx) = CommitBatch::new(batch.count());

		// Phase 1: Prepare (single producer) - assign sequence number
		let seq_num = self.prepare(&mut batch, commit_batch.clone())?;

		// Phase 2: Write to WAL and process VLog (synchronous)
		let processed_batch = {
			let env = self.env.clone();
			env.write(&batch, seq_num, sync)?
		};

		// Phase 3: Apply to memtable (can be concurrent)
		let apply_result = {
			let env = self.env.clone();
			env.apply(&processed_batch)
		};

		match apply_result {
			Ok(_) => {
				commit_batch.mark_applied();
				// Phase 4: Publish (multi-consumer)
				self.publish();
			}
			Err(e) => {
				let err = Error::CommitFail(e.to_string());
				commit_batch.complete(Err(err.clone()));
				return Err(err);
			}
		}

		// Wait for completion
		complete_rx.await.map_err(|_| Error::PipelineStall)?
	}

	fn prepare(&self, batch: &mut Batch, commit_batch: Arc<CommitBatch>) -> Result<u64> {
		let _guard = self.write_mutex.lock().unwrap();

		// Assign sequence number atomically
		let count = batch.count() as u64;
		let seq_num = self.log_seq_num.fetch_add(count, Ordering::SeqCst);
		commit_batch.set_seq_num(seq_num);

		// Set the starting sequence number in the batch for unified sequence number management
		batch.set_starting_seq_num(seq_num);

		// Enqueue in pending queue (single producer operation)
		self.pending.enqueue(commit_batch.clone());

		Ok(seq_num)
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

	use crate::sstable::InternalKeyKind;

	use super::*;

	struct MockEnv;

	impl CommitEnv for MockEnv {
		fn write(&self, batch: &Batch, _seq_num: u64, _sync: bool) -> Result<Batch> {
			// Create a copy of the batch for testing
			let mut new_batch = Batch::new(_seq_num);
			for entry in batch.entries() {
				new_batch.add_record(
					entry.kind,
					&entry.key,
					entry.value.as_deref(),
					entry.timestamp,
				)?;
			}
			Ok(new_batch)
		}

		fn apply(&self, _batch: &Batch) -> Result<()> {
			Ok(())
		}
	}

	#[test(tokio::test)]
	async fn test_single_commit() {
		let pipeline = CommitPipeline::new(Arc::new(MockEnv));

		let mut batch = Batch::new(0);
		batch.add_record(InternalKeyKind::Set, b"key1", Some(b"value1"), 0).unwrap();

		let result = pipeline.commit(batch, false).await;
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
		let pipeline = CommitPipeline::new(Arc::new(MockEnv));

		// First test sequential commits to verify basic functionality
		for i in 0..5 {
			let mut batch = Batch::new(0);
			batch
				.add_record(
					InternalKeyKind::Set,
					&format!("key{i}").into_bytes(),
					Some(&[1, 2, 3]),
					i,
				)
				.unwrap();
			let result = pipeline.commit(batch, false).await;
			assert!(result.is_ok(), "Sequential commit {i} failed: {result:?}");
		}

		assert_eq!(pipeline.get_visible_seq_num(), 5);

		pipeline.shutdown();
	}

	#[test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
	async fn test_concurrent_commits() {
		let pipeline = CommitPipeline::new(Arc::new(MockEnv));

		let mut handles = vec![];
		for i in 0..10 {
			let pipeline = pipeline.clone();
			let handle = tokio::spawn(async move {
				let mut batch = Batch::new(0);
				batch
					.add_record(
						InternalKeyKind::Set,
						&format!("key{i}").into_bytes(),
						Some(&[1, 2, 3]),
						i,
					)
					.unwrap();
				pipeline.commit(batch, false).await
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
					&entry.key,
					entry.value.as_deref(),
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
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_concurrent_commits_with_delays() {
		let pipeline = CommitPipeline::new(Arc::new(DelayedMockEnv));

		let mut handles = vec![];
		for i in 0..5 {
			let pipeline = pipeline.clone();
			let handle = tokio::spawn(async move {
				let mut batch = Batch::new(0);
				batch
					.add_record(
						InternalKeyKind::Set,
						&format!("key{i}").into_bytes(),
						Some(&[1, 2, 3]),
						i,
					)
					.unwrap();
				pipeline.commit(batch, false).await
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
}
