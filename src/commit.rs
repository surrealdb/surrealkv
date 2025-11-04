// Lock-free commit pipeline using atomic SkipMap-based queues.

use std::sync::{
	atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
	Arc,
};
use std::time::Duration;

use crossbeam_skiplist::SkipMap;

use bytes::Bytes;

use crate::{
	batch::Batch,
	clock::LogicalClock,
	error::{Error, Result},
	transaction::Entry,
};

// Trait for commit operations
pub trait CommitEnv: Send + Sync + 'static {
	// Write batch to WAL and process VLog entries (synchronous operation)
	// Returns a new batch with VLog pointers applied
	fn write(&self, batch: &Batch, seq_num: u64, sync: bool) -> Result<Batch>;

	// Apply processed batch to memtable
	fn apply(&self, batch: &Batch) -> Result<()>;
}

// Entry in the commit queue for ordering and conflict detection
pub(crate) struct Commit {
	pub(crate) batch: Arc<Batch>,
	pub(crate) id: u64,
}

impl Commit {
	/// Check if two writesets have overlapping keys (conflict detection)
	/// Extracts keys from batches and uses two-pointer algorithm for O(n+m) complexity
	fn is_disjoint_writeset(&self, other: &Commit) -> bool {
		// Extract keys from both batches
		let keys_a: Vec<Bytes> =
			self.batch.as_ref().entries().iter().map(|e| Bytes::copy_from_slice(&e.key)).collect();
		let keys_b: Vec<Bytes> =
			other.batch.as_ref().entries().iter().map(|e| Bytes::copy_from_slice(&e.key)).collect();

		if keys_a.is_empty() || keys_b.is_empty() {
			return true;
		}

		let mut a = keys_a.iter();
		let mut b = keys_b.iter();
		let mut next_a = a.next();
		let mut next_b = b.next();

		while let (Some(ka), Some(kb)) = (next_a, next_b) {
			match ka.cmp(kb) {
				std::cmp::Ordering::Less => next_a = a.next(),
				std::cmp::Ordering::Greater => next_b = b.next(),
				std::cmp::Ordering::Equal => return false, // Overlap found!
			}
		}
		true // No overlap
	}
}

// Entry in the merge queue for data versioning
pub(crate) struct Merge {
	pub(crate) batch: Arc<Batch>,
	pub(crate) id: u64,
}

// Lock-free commit pipeline using atomic SkipMap queues
pub(crate) struct CommitPipeline {
	env: Arc<dyn CommitEnv>,

	// SkipMap-based queues (no fixed size limit)
	// Made public for read access from transactions
	pub(crate) commit_queue: SkipMap<u64, Arc<Commit>>,
	pub(crate) merge_queue: SkipMap<u64, Arc<Merge>>,

	// Atomic counters for commit/merge versioning
	commit_id: AtomicU64,
	merge_id: AtomicU64, // Sequence number counter for data versioning

	// Unique ID generators for CAS operations
	transaction_queue_id: AtomicU64,
	transaction_merge_id: AtomicU64,

	// Track active transactions for cleanup
	active_txn_starts: SkipMap<u64, AtomicUsize>,

	// Maximum entries in commit_queue before cleanup
	max_queue_size: usize,

	// Logical clock for timestamp generation
	clock: Arc<dyn LogicalClock>,

	shutdown: AtomicBool,
}

impl CommitPipeline {
	pub(crate) fn new(env: Arc<dyn CommitEnv>, clock: Arc<dyn LogicalClock>) -> Arc<Self> {
		Arc::new(Self {
			env,
			commit_queue: SkipMap::new(),
			merge_queue: SkipMap::new(),
			commit_id: AtomicU64::new(0),
			merge_id: AtomicU64::new(0), // Start at 0 so first seq_num is 1
			transaction_queue_id: AtomicU64::new(0),
			transaction_merge_id: AtomicU64::new(0),
			active_txn_starts: SkipMap::new(),
			max_queue_size: 10000,
			clock,
			shutdown: AtomicBool::new(false),
		})
	}

	pub(crate) fn set_seq_num(&self, seq_num: u64) {
		if seq_num > 0 {
			self.merge_id.store(seq_num, Ordering::Release);
		}
	}

	/// Register a new transaction start for conflict detection
	pub(crate) fn register_txn_start(&self, start_commit_id: u64) {
		let entry =
			self.active_txn_starts.get_or_insert_with(start_commit_id, || AtomicUsize::new(0));
		entry.value().fetch_add(1, Ordering::Relaxed);
	}

	/// Unregister a transaction when it commits or aborts
	pub(crate) fn unregister_txn_start(&self, start_commit_id: u64) {
		if let Some(entry) = self.active_txn_starts.get(&start_commit_id) {
			let prev_count = entry.value().fetch_sub(1, Ordering::Relaxed);

			// Only remove if we decremented from 1 to 0
			if prev_count == 1 {
				// Double-check by trying to remove only if counter is 0
				if entry.value().load(Ordering::Relaxed) == 0 {
					self.active_txn_starts.remove(&start_commit_id);
				}
			}
		}
	}

	/// Get the current commit ID for new transactions
	pub(crate) fn get_commit_id(&self) -> u64 {
		self.commit_id.load(Ordering::Acquire)
	}

	/// Commit a transaction's writeset (unified commit API)
	/// Handles batch creation, timestamp assignment, conflict detection, and WAL writing
	pub(crate) fn commit(
		&self,
		latest_writes: Vec<Entry>,
		sync: bool,
		start_commit_id: u64,
	) -> Result<()> {
		if self.shutdown.load(Ordering::Acquire) {
			return Err(Error::PipelineStall);
		}

		if latest_writes.is_empty() {
			return Ok(());
		}

		// Create batch with timestamps
		let mut batch = Batch::new(0);
		let commit_timestamp = self.clock.now();

		for entry in latest_writes {
			let timestamp = if entry.timestamp != 0 {
				entry.timestamp
			} else {
				commit_timestamp
			};
			batch.add_record(
				entry.kind,
				&entry.key,
				entry.value.as_ref().map(|bytes| bytes.as_ref()),
				timestamp,
			)?;
		}

		let batch_arc = Arc::new(batch);

		// Insert into commit queue for ordering
		let (commit_version, commit_entry) = self.atomic_commit(Commit {
			batch: batch_arc.clone(),
			id: self.transaction_queue_id.fetch_add(1, Ordering::AcqRel) + 1,
		})?;

		// Check for write-write conflicts
		for tx in self.commit_queue.range((start_commit_id + 1)..commit_version) {
			if !tx.value().is_disjoint_writeset(&commit_entry) {
				self.commit_queue.remove(&commit_version);
				return Err(Error::TransactionWriteConflict);
			}
		}

		// Insert into merge queue and write to WAL (serialized by CAS)
		let (seq_num, processed_batch) = self.atomic_merge(
			Merge {
				batch: batch_arc,
				id: self.transaction_merge_id.fetch_add(1, Ordering::AcqRel) + 1,
			},
			sync,
		)?;

		// Apply to memtable
		self.env.apply(&processed_batch)?;

		// Update visible sequence number
		let count = processed_batch.count() as u64;
		self.update_visible_seq_num(seq_num, count);

		// Cleanup old entries
		self.cleanup_commit_queue();
		self.merge_queue.remove(&seq_num);

		Ok(())
	}

	/// Commit a batch directly (for internal operations like VLog GC)
	/// Converts batch to Entry vector and uses unified commit API
	pub(crate) fn commit_batch(&self, batch: Batch, sync: bool) -> Result<()> {
		if batch.is_empty() {
			return Ok(());
		}

		// Capture start point for proper conflict detection
		let start_commit_id = self.get_commit_id();

		// Convert batch entries to Entry vector
		let entries: Vec<Entry> = batch
			.entries()
			.iter()
			.map(|e| Entry {
				key: Bytes::copy_from_slice(&e.key),
				value: e.value.clone().map(|v| Bytes::copy_from_slice(&v)),
				kind: e.kind,
				savepoint_no: 0,
				seqno: 0,
				timestamp: e.timestamp,
			})
			.collect();

		// Use proper start_commit_id for snapshot isolation semantics
		self.commit(entries, sync, start_commit_id)
	}

	/// Atomically insert into commit queue
	fn atomic_commit(&self, updates: Commit) -> Result<(u64, Arc<Commit>)> {
		let mut spins = 0;
		let id = updates.id;
		let updates = Arc::new(updates);

		loop {
			let version = self.commit_id.load(Ordering::Acquire) + 1;

			let entry = self.commit_queue.get_or_insert_with(version, || Arc::clone(&updates));

			if id == entry.value().id {
				self.commit_id.fetch_add(1, Ordering::Release);
				return Ok((version, entry.value().clone()));
			}

			// Backoff strategy
			if spins < 10 {
				std::hint::spin_loop();
			} else if spins < 100 {
				std::thread::yield_now();
			} else {
				std::thread::park_timeout(Duration::from_micros(10));
			}
			spins += 1;
		}
	}

	/// Clean up the commit queue based on active transactions
	fn cleanup_commit_queue(&self) {
		// Only clean up if we have too many entries
		if self.commit_queue.len() <= self.max_queue_size {
			return;
		}

		// Find the oldest active transaction start point
		let oldest_active = match self.active_txn_starts.iter().next() {
			Some(entry) => {
				// There are active transactions - we can safely clean up entries
				// older than the oldest active transaction's start point
				*entry.key()
			}
			None => {
				// No active transactions currently.
				// New transactions will get start_commit_id = current commit_id
				// So we can safely clean up entries older than the current commit ID
				// since no future transaction will have a start_commit_id older than this
				self.commit_id.load(Ordering::Acquire)
			}
		};

		// Only remove entries that are not needed for conflict detection
		let keys_to_remove: Vec<u64> =
			self.commit_queue.range(..oldest_active).map(|e| *e.key()).collect();

		for key in keys_to_remove {
			self.commit_queue.remove(&key);
		}
	}

	/// Atomically insert into merge queue and write to WAL
	/// The winning thread (that claims the seq_num) writes to WAL - guarantees sequential order
	fn atomic_merge(&self, updates: Merge, sync: bool) -> Result<(u64, Batch)> {
		let mut spins = 0;
		let id = updates.id;
		let batch = (*updates.batch).clone(); // Clone batch before Arc
		let updates = Arc::new(updates);

		loop {
			// Get next sequence number
			let seq_num = self.merge_id.load(Ordering::Acquire) + 1;

			let entry = self.merge_queue.get_or_insert_with(seq_num, || Arc::clone(&updates));

			if id == entry.value().id {
				// CAS winner writes to WAL with assigned sequence number
				let mut batch_with_seq = batch.clone();
				batch_with_seq.set_starting_seq_num(seq_num);
				let processed_batch = self.env.write(&batch_with_seq, seq_num, sync)?;

				self.merge_id.fetch_add(1, Ordering::Release);
				return Ok((seq_num, processed_batch));
			}

			// Backoff strategy
			if spins < 10 {
				std::hint::spin_loop();
			} else if spins < 100 {
				std::thread::yield_now();
			} else {
				std::thread::park_timeout(Duration::from_micros(10));
			}
			spins += 1;
		}
	}

	pub(crate) fn get_visible_seq_num(&self) -> u64 {
		// Returns the highest sequence number that has been assigned and applied
		// Updated by update_visible_seq_num after each batch is applied
		self.merge_id.load(Ordering::Acquire)
	}

	/// Update visible sequence number after applying a batch
	fn update_visible_seq_num(&self, seq_num: u64, count: u64) {
		// Calculate highest seq used: seq_num + count - 1
		// We need to track this for correct snapshot isolation
		let highest_seq = seq_num + count - 1;

		// Update merge_id to reflect highest used
		let mut current = self.merge_id.load(Ordering::Acquire);
		while highest_seq > current {
			match self.merge_id.compare_exchange_weak(
				current,
				highest_seq,
				Ordering::Release,
				Ordering::Acquire, // Must use Acquire on failure to see other thread's update
			) {
				Ok(_) => break,
				Err(actual) => current = actual,
			}
		}
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
	use test_log::test;

	use crate::{clock::MockLogicalClock, sstable::InternalKeyKind};

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

	#[test]
	fn test_single_commit() {
		let pipeline = CommitPipeline::new(Arc::new(MockEnv), Arc::new(MockLogicalClock::new()));

		let mut batch = Batch::new(0);
		batch.add_record(InternalKeyKind::Set, b"key1", Some(b"value1"), 0).unwrap();

		let result = pipeline.commit_batch(batch, false);
		assert!(result.is_ok(), "Single commit failed: {result:?}");

		let visible = pipeline.get_visible_seq_num();
		assert_eq!(
			visible, 1,
			"Expected visible=1 after one commit with count=1 (highest seq num used)"
		);

		pipeline.shutdown();
	}

	#[test]
	fn test_sequential_commits() {
		let pipeline = CommitPipeline::new(Arc::new(MockEnv), Arc::new(MockLogicalClock::new()));

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
			let result = pipeline.commit_batch(batch, false);
			assert!(result.is_ok(), "Sequential commit {i} failed: {result:?}");
		}

		assert_eq!(pipeline.get_visible_seq_num(), 5);

		pipeline.shutdown();
	}

	#[test]
	fn test_concurrent_commits() {
		let pipeline =
			Arc::new(CommitPipeline::new(Arc::new(MockEnv), Arc::new(MockLogicalClock::new())));

		let mut handles = vec![];
		for i in 0..10 {
			let pipeline = pipeline.clone();
			let handle = std::thread::spawn(move || {
				let mut batch = Batch::new(0);
				batch
					.add_record(
						InternalKeyKind::Set,
						&format!("key{i}").into_bytes(),
						Some(&[1, 2, 3]),
						i,
					)
					.unwrap();
				pipeline.commit_batch(batch, false)
			});
			handles.push(handle);
		}

		for (i, handle) in handles.into_iter().enumerate() {
			let result = handle.join().unwrap();
			assert!(result.is_ok(), "Commit {i} failed: {result:?}");
		}

		// Verify all batches were published
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

	#[test]
	fn test_concurrent_commits_with_delays() {
		let pipeline = Arc::new(CommitPipeline::new(
			Arc::new(DelayedMockEnv),
			Arc::new(MockLogicalClock::new()),
		));

		let mut handles = vec![];
		for i in 0..5 {
			let pipeline = pipeline.clone();
			let handle = std::thread::spawn(move || {
				let mut batch = Batch::new(0);
				batch
					.add_record(
						InternalKeyKind::Set,
						&format!("key{i}").into_bytes(),
						Some(&[1, 2, 3]),
						i,
					)
					.unwrap();
				pipeline.commit_batch(batch, false)
			});
			handles.push(handle);
		}

		for handle in handles {
			assert!(handle.join().unwrap().is_ok());
		}

		// Verify sequence numbers are published correctly
		assert_eq!(pipeline.get_visible_seq_num(), 5);

		// Shutdown the pipeline
		pipeline.shutdown();
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
		fn write(&self, batch: &Batch, _seq_num: u64, sync_wal: bool) -> Result<Batch> {
			self.write_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			self.last_sync_wal.store(sync_wal, std::sync::atomic::Ordering::Relaxed);

			// Create a copy of the batch for testing (like MockEnv does)
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
			self.apply_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
			Ok(())
		}
	}

	#[test]
	fn test_commit_calls_write_and_apply() {
		let env = Arc::new(TrackingMockEnv::new());
		let pipeline = CommitPipeline::new(env.clone(), Arc::new(MockLogicalClock::new()));

		let mut batch = Batch::new(0);
		batch.add_record(InternalKeyKind::Set, b"key1", Some(b"value1"), 0).unwrap();

		// Verify initial state
		assert_eq!(env.write_calls(), 0);
		assert_eq!(env.apply_calls(), 0);

		// Perform commit
		pipeline.commit_batch(batch, true).unwrap();

		// Verify both write and apply were called
		assert_eq!(env.write_calls(), 1);
		assert_eq!(env.apply_calls(), 1);
		assert!(env.last_sync_wal());

		pipeline.shutdown();
	}

	#[test]
	fn test_commit_multiple_batches_tracking() {
		let env = Arc::new(TrackingMockEnv::new());
		let pipeline = CommitPipeline::new(env.clone(), Arc::new(MockLogicalClock::new()));

		// Commit multiple batches
		for i in 0..3 {
			let mut batch = Batch::new(0);
			batch
				.add_record(
					InternalKeyKind::Set,
					&format!("key{i}").into_bytes(),
					Some(&[1, 2, 3]),
					i as u64,
				)
				.unwrap();

			pipeline.commit_batch(batch, i % 2 == 0).unwrap(); // Alternate sync_wal
		}

		// Verify correct number of calls
		assert_eq!(env.write_calls(), 3);
		assert_eq!(env.apply_calls(), 3);

		pipeline.shutdown();
	}

	#[test]
	fn test_commit_concurrent_access() {
		let env = Arc::new(TrackingMockEnv::new());
		let pipeline =
			Arc::new(CommitPipeline::new(env.clone(), Arc::new(MockLogicalClock::new())));

		// Test concurrent access to commit
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
							InternalKeyKind::Set,
							&format!("thread_{thread_id}_batch_{batch_id}").into_bytes(),
							Some(&[thread_id as u8, batch_id as u8]),
							batch_id,
						)
						.unwrap();

					// This should be safe even under concurrency due to lock-free design
					pipeline.commit_batch(batch, false).unwrap();
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

	#[test]
	fn test_commit_after_shutdown() {
		let pipeline = CommitPipeline::new(Arc::new(MockEnv), Arc::new(MockLogicalClock::new()));
		pipeline.shutdown();

		let mut batch = Batch::new(0);
		batch.add_record(InternalKeyKind::Set, b"key1", Some(b"value1"), 0).unwrap();

		let result = pipeline.commit_batch(batch, false);
		assert!(result.is_err(), "Commit after shutdown should fail");
	}
}
