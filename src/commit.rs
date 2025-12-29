// This commit pipeline is inspired by Pebble's commit pipeline.

use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{oneshot, Semaphore};

use crate::batch::Batch;
use crate::error::{Error, Result};

const MAX_CONCURRENT_COMMITS: usize = 8;
const DEQUEUE_BITS: u32 = 32;

// Trait for commit operations
pub trait CommitEnv: Send + Sync + 'static {
	// Write batch to WAL and process VLog entries (synchronous operation)
	// Returns a new batch with VLog pointers applied
	fn write(&self, batch: Batch, seq_num: u64, sync: bool) -> Result<Batch>;

	// Apply processed batch to memtable
	fn apply(&self, batch: Batch) -> Result<()>;
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

	const fn unpack(&self, ptrs: u64) -> (u32, u32) {
		let head = (ptrs >> DEQUEUE_BITS) as u32;
		let tail = ptrs as u32;
		(head, tail)
	}

	const fn pack(&self, head: u32, tail: u32) -> u64 {
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

	pub fn sync_commit(&self, mut batch: Batch, sync_wal: bool) -> Result<()> {
		if self.shutdown.load(Ordering::Acquire) {
			return Err(Error::PipelineStall);
		}

		if batch.is_empty() {
			return Ok(());
		}

		// Acquire write lock to ensure serialization
		let _guard = self.write_mutex.lock();

		// Assign sequence number
		let count = batch.count() as u64;
		let seq_num = self.log_seq_num.fetch_add(count, Ordering::SeqCst);

		// Set the starting sequence number in the batch
		batch.set_starting_seq_num(seq_num);

		// Write to WAL and get processed batch
		let processed_batch = self.env.write(batch, seq_num, sync_wal)?;

		// Apply processed batch to memtable
		self.env.apply(processed_batch)?;

		// Update visible sequence number
		let new_visible = seq_num + count - 1;
		let mut current = self.visible_seq_num.load(Ordering::Acquire);
		while new_visible > current {
			match self.visible_seq_num.compare_exchange_weak(
				current,
				new_visible,
				Ordering::Release,
				Ordering::Relaxed,
			) {
				Ok(_) => break,
				Err(actual) => current = actual,
			}
		}

		Ok(())
	}

	pub(crate) async fn commit(&self, batch: Batch, sync: bool) -> Result<()> {
		if self.shutdown.load(Ordering::Acquire) {
			return Err(Error::PipelineStall);
		}

		if batch.is_empty() {
			return Ok(());
		}

		// Acquire permit for flow control
		let _permit = self.commit_sem.acquire().await.map_err(|_| Error::PipelineStall)?;

		let (commit_batch, complete_rx) = CommitBatch::new(batch.count());

		// Phase 1: Assign sequence number and write to WAL (serialized)
		let processed_batch = self.prepare(batch, Arc::clone(&commit_batch), sync)?;

		// Phase 2: Apply to memtable (concurrent)
		let apply_result = {
			let env = Arc::clone(&self.env);
			env.apply(processed_batch)
		};

		match apply_result {
			Ok(_) => {
				commit_batch.mark_applied();
				// Phase 3: Publish (multi-consumer)
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

	fn prepare(
		&self,
		mut batch: Batch,
		commit_batch: Arc<CommitBatch>,
		sync: bool,
	) -> Result<Batch> {
		// Assign sequence number atomically and write to WAL (serialized)
		let _guard = self.write_mutex.lock();

		let count = batch.count() as u64;
		let seq_num = self.log_seq_num.fetch_add(count, Ordering::SeqCst);

		// Set sequence numbers in batch and commit batch
		commit_batch.set_seq_num(seq_num);
		batch.set_starting_seq_num(seq_num);

		// Enqueue operation (single producer)
		self.pending.enqueue(commit_batch);

		// Write to WAL and process VLog (serialized under lock)
		let processed_batch = self.env.write(batch, seq_num, sync)?;

		Ok(processed_batch)
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
	use crate::sstable::{InternalKey, InternalKeyKind};

	struct MockEnv;

	impl CommitEnv for MockEnv {
		fn write(&self, batch: Batch, seq_num: u64, _sync: bool) -> Result<Batch> {
			// Create a copy of the batch for testing
			let mut new_batch = Batch::new(seq_num);
			for entry in batch.entries() {
				new_batch.add_record(entry.key.clone(), entry.value.clone())?;
			}
			Ok(new_batch)
		}

		fn apply(&self, _batch: Batch) -> Result<()> {
			Ok(())
		}
	}

	#[test(tokio::test)]
	async fn test_single_commit() {
		let pipeline = CommitPipeline::new(Arc::new(MockEnv));

		let mut batch = Batch::new(0);
		batch
			.add_record(
				InternalKey::encode(b"key1".to_vec(), 0, InternalKeyKind::Set, 0),
				Some(b"value1".to_vec()),
			)
			.unwrap();

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
					InternalKey::encode(format!("key{i}").into_bytes(), 0, InternalKeyKind::Set, 0),
					Some(vec![1, 2, 3]),
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
						InternalKey::encode(
							format!("key{i}").into_bytes(),
							0,
							InternalKeyKind::Set,
							0,
						),
						Some(vec![1, 2, 3]),
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
		fn write(&self, batch: Batch, _seq_num: u64, _sync: bool) -> Result<Batch> {
			let start = std::time::Instant::now();
			while start.elapsed() < Duration::from_micros(100) {
				std::hint::spin_loop();
			}
			// Create a copy of the batch for testing
			let mut new_batch = Batch::new(_seq_num);
			for entry in batch.entries() {
				new_batch.add_record(entry.key.clone(), entry.value.clone())?;
			}
			Ok(new_batch)
		}

		fn apply(&self, _batch: Batch) -> Result<()> {
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
						InternalKey::encode(
							format!("key{i}").into_bytes(),
							0,
							InternalKeyKind::Set,
							0,
						),
						Some(vec![1, 2, 3]),
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

	// ===== Sync Commit Tests =====

	#[test]
	fn test_sync_commit_single() {
		let pipeline = CommitPipeline::new(Arc::new(MockEnv));

		let mut batch = Batch::new(0);
		batch
			.add_record(InternalKeyKind::Set, b"key1".to_vec(), Some(b"value1".to_vec()), 0)
			.unwrap();

		let result = pipeline.sync_commit(batch, false);
		assert!(result.is_ok(), "Sync commit failed: {result:?}");

		let visible = pipeline.get_visible_seq_num();
		assert_eq!(visible, 1, "Expected visible=1 after sync commit");

		pipeline.shutdown();
	}

	#[test]
	fn test_sync_commit_multiple_sequential() {
		let pipeline = CommitPipeline::new(Arc::new(MockEnv));

		// Test multiple sequential sync commits
		for i in 0..5 {
			let mut batch = Batch::new(0);
			batch
				.add_record(
					InternalKeyKind::Set,
					format!("key{i}").into_bytes(),
					Some(vec![1, 2, 3]),
					i as u64,
				)
				.unwrap();

			let result = pipeline.sync_commit(batch, false);
			assert!(result.is_ok(), "Sync commit {i} failed: {result:?}");
		}

		// Verify final sequence number
		assert_eq!(pipeline.get_visible_seq_num(), 5);

		pipeline.shutdown();
	}

	#[test]
	fn test_sync_commit_empty_batch() {
		let pipeline = CommitPipeline::new(Arc::new(MockEnv));

		let batch = Batch::new(0);
		let result = pipeline.sync_commit(batch, false);
		assert!(result.is_ok(), "Empty batch sync commit should succeed");

		// Sequence number should not change for empty batch
		assert_eq!(pipeline.get_visible_seq_num(), 0);

		pipeline.shutdown();
	}

	#[test]
	fn test_sync_commit_with_sync_wal() {
		let pipeline = CommitPipeline::new(Arc::new(MockEnv));

		let mut batch = Batch::new(0);
		batch
			.add_record(InternalKeyKind::Set, b"key1".to_vec(), Some(b"value1".to_vec()), 0)
			.unwrap();

		let result = pipeline.sync_commit(batch, true); // sync_wal = true
		assert!(result.is_ok(), "Sync commit with sync_wal failed: {result:?}");

		pipeline.shutdown();
	}

	#[test]
	fn test_sync_commit_sequence_number_consistency() {
		let pipeline = CommitPipeline::new(Arc::new(MockEnv));

		// Set initial sequence number
		pipeline.set_seq_num(100);

		let mut batch1 = Batch::new(0);
		batch1
			.add_record(InternalKeyKind::Set, b"key1".to_vec(), Some(b"value1".to_vec()), 0)
			.unwrap();

		let mut batch2 = Batch::new(0);
		batch2
			.add_record(InternalKeyKind::Set, b"key2".to_vec(), Some(b"value2".to_vec()), 1)
			.unwrap();
		batch2
			.add_record(InternalKeyKind::Set, b"key3".to_vec(), Some(b"value3".to_vec()), 2)
			.unwrap();

		// Commit first batch
		pipeline.sync_commit(batch1, false).unwrap();
		assert_eq!(pipeline.get_visible_seq_num(), 101); // 100 + 1 - 1

		// Commit second batch (2 records)
		pipeline.sync_commit(batch2, false).unwrap();
		assert_eq!(pipeline.get_visible_seq_num(), 103); // 101 + 2 - 1

		pipeline.shutdown();
	}

	#[test]
	fn test_sync_commit_after_shutdown() {
		let pipeline = CommitPipeline::new(Arc::new(MockEnv));
		pipeline.shutdown();

		let mut batch = Batch::new(0);
		batch
			.add_record(InternalKeyKind::Set, b"key1".to_vec(), Some(b"value1".to_vec()), 0)
			.unwrap();

		let result = pipeline.sync_commit(batch, false);
		assert!(result.is_err(), "Sync commit after shutdown should fail");
	}

	// Mock environment that tracks calls for testing
	struct TrackingMockEnv {
		write_calls: std::sync::atomic::AtomicU64,
		apply_calls: std::sync::atomic::AtomicU64,
		last_sync_wal: std::sync::atomic::AtomicBool,
	}

	impl TrackingMockEnv {
		fn new() -> Self {
			Self {
				write_calls: std::sync::atomic::AtomicU64::new(0),
				apply_calls: std::sync::atomic::AtomicU64::new(0),
				last_sync_wal: std::sync::atomic::AtomicBool::new(false),
			}
		}

		fn write_calls(&self) -> u64 {
			self.write_calls.load(std::sync::atomic::Ordering::Relaxed)
		}

		fn apply_calls(&self) -> u64 {
			self.apply_calls.load(std::sync::atomic::Ordering::Relaxed)
		}

		fn last_sync_wal(&self) -> bool {
			self.last_sync_wal.load(std::sync::atomic::Ordering::Relaxed)
		}
	}

	impl CommitEnv for TrackingMockEnv {
		fn write(&self, batch: Batch, _seq_num: u64, sync_wal: bool) -> Result<Batch> {
			self.write_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			self.last_sync_wal.store(sync_wal, std::sync::atomic::Ordering::Relaxed);

			// Create a copy of the batch for testing (like MockEnv does)
			let mut new_batch = Batch::new(_seq_num);
			for entry in batch.entries() {
				new_batch.add_record(entry.key.clone(), entry.value.clone())?;
			}
			Ok(new_batch)
		}

		fn apply(&self, _batch: Batch) -> Result<()> {
			self.apply_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			Ok(())
		}
	}

	#[test]
	fn test_sync_commit_calls_write_and_apply() {
		let env = Arc::new(TrackingMockEnv::new());
		let pipeline = CommitPipeline::new(env.clone());

		let mut batch = Batch::new(0);
		batch
			.add_record(InternalKeyKind::Set, b"key1".to_vec(), Some(b"value1".to_vec()), 0)
			.unwrap();

		// Verify initial state
		assert_eq!(env.write_calls(), 0);
		assert_eq!(env.apply_calls(), 0);

		// Perform sync commit
		pipeline.sync_commit(batch, true).unwrap();

		// Verify both write and apply were called
		assert_eq!(env.write_calls(), 1);
		assert_eq!(env.apply_calls(), 1);
		assert!(env.last_sync_wal());

		pipeline.shutdown();
	}

	#[test]
	fn test_sync_commit_multiple_batches_tracking() {
		let env = Arc::new(TrackingMockEnv::new());
		let pipeline = CommitPipeline::new(env.clone());

		// Commit multiple batches
		for i in 0..3 {
			let mut batch = Batch::new(0);
			batch
				.add_record(
					InternalKey::encode(format!("key{i}").into_bytes(), 0, InternalKeyKind::Set, 0),
					Some(vec![1, 2, 3]),
				)
				.unwrap();

			pipeline.sync_commit(batch, i % 2 == 0).unwrap(); // Alternate sync_wal
		}

		// Verify correct number of calls
		assert_eq!(env.write_calls(), 3);
		assert_eq!(env.apply_calls(), 3);

		pipeline.shutdown();
	}

	#[test]
	fn test_sync_commit_concurrent_access() {
		let env = Arc::new(TrackingMockEnv::new());
		let pipeline_clone = CommitPipeline::new(env.clone());
		let pipeline = Arc::new(pipeline_clone);

		// Test concurrent access to sync_commit
		let mut handles = vec![];
		let num_threads = 8;
		let batches_per_thread = 5;

		for thread_id in 0..num_threads {
			let pipeline = pipeline.clone();
			let _env = env.clone();

			let handle = std::thread::spawn(move || {
				for batch_id in 0..batches_per_thread {
					let mut batch = Batch::new(0);
					batch
						.add_record(
							InternalKey::encode(
								format!("thread_{thread_id}_batch_{batch_id}").into_bytes(),
								0,
								InternalKeyKind::Set,
								0,
							),
							Some(vec![thread_id as u8, batch_id as u8]),
						)
						.unwrap();

					// This should be safe even under concurrency due to write_mutex
					pipeline.sync_commit(batch, false).unwrap();
				}
			});
			handles.push(handle);
		}

		// Wait for all threads to complete
		for handle in handles {
			handle.join().unwrap();
		}

		// Verify all operations were processed
		let expected_calls = num_threads * batches_per_thread;
		assert_eq!(env.write_calls(), expected_calls);
		assert_eq!(env.apply_calls(), expected_calls);

		// Verify sequence numbers are continuous and correct
		let final_visible = pipeline.get_visible_seq_num();
		assert_eq!(final_visible, expected_calls);

		pipeline.shutdown();
	}
}
