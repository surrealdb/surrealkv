use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{oneshot, Notify};

use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::lsm::CoreInner;
use crate::stall::WriteStallController;
use crate::task::TaskManager;
use crate::vlog::ValueLocation;
use crate::Key;

use super::bloom::BloomFilter;
use super::queue::{CommitEntry, CommitQueue};
use super::Ring;

/// Coordinates the lock-free commit ring, OCC conflict queue, and background flusher.
pub(crate) struct CommitPipeline {
	pub(crate) ring: Arc<Ring>,
	pub(crate) queue: Arc<CommitQueue>,
	pub(crate) inner: Arc<CoreInner>,
	pub(crate) write_stall: Arc<WriteStallController>,
	pub(crate) task_manager: Option<Arc<TaskManager>>,
	pub(crate) notify_flusher: Arc<Notify>,
	pub(crate) shutdown: AtomicBool,
	/// Sequence number allocator for batch entries
	pub(crate) log_seq_num: AtomicU64,
	/// Flag used to pause all incoming commits during a full restore from checkpoint.
	pub(crate) restoring: AtomicBool,
}

pub(crate) struct RestoreGuard<'a> {
	restoring: &'a AtomicBool,
}

impl Drop for RestoreGuard<'_> {
	fn drop(&mut self) {
		self.restoring.store(false, Ordering::Release);
	}
}

impl CommitPipeline {
	pub(crate) fn new(
		inner: Arc<CoreInner>,
		write_stall: Arc<WriteStallController>,
		task_manager: Option<Arc<TaskManager>>,
		start_seq: u64,
	) -> Self {
		let ring = Arc::new(Ring::new(1024, 1));
		let queue = Arc::new(CommitQueue::new());
		let notify_flusher = Arc::new(Notify::new());
		let seq = start_seq.max(1);

		Self {
			ring,
			queue,
			inner,
			write_stall,
			task_manager,
			notify_flusher,
			shutdown: AtomicBool::new(false),
			log_seq_num: AtomicU64::new(seq),
			restoring: AtomicBool::new(false),
		}
	}

	/// Starts the background flusher task.
	pub(crate) fn start_flusher(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
		let pipeline = Arc::clone(self);
		tokio::spawn(async move {
			pipeline.run_flusher().await;
		})
	}

	/// Validates locked reads against the commit queue without submitting a write.
	pub(crate) fn check_conflicts(&self, read_keys: &[Key], start_seq: u64) -> Result<()> {
		if read_keys.is_empty() {
			return Ok(());
		}
		let mut read_bloom = BloomFilter::new();
		for k in read_keys {
			read_bloom.insert(k);
		}
		let current_seq = self.ring.next.load(Ordering::Acquire);
		if !self.queue.check_conflicts(start_seq, current_seq, &[], &BloomFilter::new(), read_keys, &read_bloom) {
			return Err(Error::TransactionWriteConflict);
		}
		Ok(())
	}

	/// Commits a batch using the lock-free OCC ring buffer pipeline.
	pub(crate) async fn commit(
		&self,
		mut batch: Batch,
		sync: bool,
		start_seq: u64,
		read_set: &[Key],
	) -> Result<()> {
		if self.shutdown.load(Ordering::Acquire) {
			return Err(Error::PipelineStall);
		}

		// Check for background errors before proceeding
		self.inner.error_handler.check_error()?;

		// If batch is empty, only validate conflicts for locked reads
		if batch.is_empty() {
			return self.check_conflicts(read_set, start_seq);
		}

		// Write stall backpressure
		self.write_stall.check().await?;

		// Check if restore is in progress
		if self.restoring.load(Ordering::Acquire) {
			return Err(Error::PipelineStall);
		}

		// Extract write keys and build bloom filters
		let write_keys: Vec<Key> = batch.entries.iter().map(|e| e.key.clone()).collect();
		let mut write_bloom = BloomFilter::new();
		for k in &write_keys {
			write_bloom.insert(k);
		}

		let mut read_bloom = BloomFilter::new();
		for k in read_set {
			read_bloom.insert(k);
		}

		// Allocate contiguous sequence numbers for the entries in this batch
		let count = batch.count() as u64;
		let start_batch_seq = self.log_seq_num.fetch_add(count, Ordering::SeqCst);
		let end_batch_seq = if count > 0 { start_batch_seq + count - 1 } else { start_batch_seq };
		batch.set_starting_seq_num(start_batch_seq);

		// Atomically claim next slot in the ring
		let slot_id = self.ring.claim();

		// Wait until the slot is available (in case of ring lap wrap-around)
		while !self.ring.has_room(slot_id) {
			tokio::task::yield_now().await;
		}

		let slot = self.ring.slot(slot_id);

		// Check for OCC conflicts against commits made after start_seq
		if !self.queue.check_conflicts(start_seq, end_batch_seq, &write_keys, &write_bloom, read_set, &read_bloom) {
			// Conflict detected: mark slot aborted and publish so flusher can skip it
			{
				let mut data = slot.data.lock();
				data.seq = slot_id;
				data.aborted = true;
			}
			slot.publish(slot_id);
			self.notify_flusher.notify_one();
			return Err(Error::TransactionWriteConflict);
		}

		// No conflict: register in commit queue
		let commit_entry = Arc::new(CommitEntry::new(end_batch_seq, write_keys));
		self.queue.insert(commit_entry);

		// Set up completion notification channel
		let (complete_tx, complete_rx) = oneshot::channel();

		// Write batch into the slot
		{
			let mut data = slot.data.lock();
			data.seq = slot_id;
			data.max_seq = end_batch_seq;
			data.batch = Some(batch);
			data.sync = sync;
			data.complete_tx = Some(complete_tx);
			data.aborted = false;
		}

		// Publish slot
		slot.publish(slot_id);

		// Wake up background flusher
		self.notify_flusher.notify_one();

		// Await durability & memtable application
		complete_rx.await.map_err(|_| Error::PipelineStall)?
	}

	/// Background flusher loop performing group commit.
	async fn run_flusher(&self) {
		loop {
			if self.shutdown.load(Ordering::Acquire) {
				break;
			}

			let taken = self.ring.taken();
			let mut next_seq = taken + 1;

			if !self.ring.drainable(next_seq) {
				// Wait for new published slots
				self.notify_flusher.notified().await;
				continue;
			}

			// Gather all contiguous ready slots for group commit
			let mut batches_to_apply = Vec::new();
			let mut completion_senders = Vec::new();
			let mut need_sync = false;
			let mut last_seq = taken;
			let mut highest_max_seq = self.inner.visible_seq_num.load(Ordering::Acquire);

			while self.ring.drainable(next_seq) {
				let slot = self.ring.slot(next_seq);
				let mut data = slot.data.lock();

				if !data.aborted {
					if let Some(batch) = data.batch.take() {
						if data.sync {
							need_sync = true;
						}
						highest_max_seq = highest_max_seq.max(data.max_seq);
						batches_to_apply.push(batch);
					}
					if let Some(tx) = data.complete_tx.take() {
						completion_senders.push(tx);
					}
				}

				last_seq = next_seq;
				next_seq += 1;
			}

			if !batches_to_apply.is_empty() {
				let flush_res = self.flush_group(&batches_to_apply, need_sync);

				match flush_res {
					Ok(()) => {
						// Advance visible sequence number
						self.inner.visible_seq_num.store(highest_max_seq, Ordering::Release);
						self.ring.advance_taken(last_seq);

						// Complete all waiting transactions
						for tx in completion_senders {
							let _ = tx.send(Ok(()));
						}
					}
					Err(e) => {
						log::error!("Error during group commit flush: {:?}", e);
						self.ring.advance_taken(last_seq);
						for tx in completion_senders {
							let _ = tx.send(Err(Error::Io(std::io::Error::new(
								std::io::ErrorKind::Other,
								"Group commit failed",
							).into())));
						}
					}
				}
			} else if last_seq > taken {
				// Only aborted slots were encountered
				self.ring.advance_taken(last_seq);
			}

			// Prune old commit queue entries
			let oldest_active = self.inner.oldest_active_start_seq();
			self.queue.prune(oldest_active);
		}
	}

	/// Flushes a group of batches to WAL and applies them to the Memtable.
	fn flush_group(&self, batches: &[Batch], sync: bool) -> Result<()> {
		// 1. Process batches (inline values) and encode for WAL
		let mut processed_batches = Vec::with_capacity(batches.len());
		for batch in batches {
			let mut processed = Batch::new(batch.starting_seq_num);
			for (_, entry, _seq, timestamp) in batch.entries_with_seq_nums()? {
				let encoded_value = match &entry.value {
					Some(value) => {
						let value_location = ValueLocation::with_inline_value(value.clone());
						Some(value_location.encode())
					}
					None => None,
				};
				processed.add_record(entry.kind, entry.key.clone(), encoded_value, timestamp)?;
			}
			processed_batches.push(processed);
		}

		// 2. Append all batches to WAL in a single lock acquisition reusing a buffer
		{
			let mut wal_guard = self.inner.wal.write();
			let mut wal_buffer = Vec::with_capacity(4096);
			for batch in &processed_batches {
				batch.encode_into(&mut wal_buffer)?;
				wal_guard.append(&wal_buffer)?;
			}
			if sync {
				wal_guard.sync()?;
			}
		}

		// 3. Apply to Active Memtable
		for batch in &processed_batches {
			let res = {
				let active = self.inner.active_memtable.read()?;
				active.add(batch)
			};

			if let Err(Error::ArenaFull) = res {
				self.inner.rotate_memtable()?;
				if let Some(ref tm) = self.task_manager {
					tm.wake_up_memtable();
				}
				let active = self.inner.active_memtable.read()?;
				active.add(batch)?;
			}
		}

		Ok(())
	}

	/// Shuts down the pipeline.
	pub(crate) fn shutdown(&self) {
		self.shutdown.store(true, Ordering::Release);
		self.notify_flusher.notify_one();
	}

	/// Locks writes for database restore.
	pub(crate) fn lock_writes(&self) -> RestoreGuard<'_> {
		self.restoring.store(true, Ordering::SeqCst);
		RestoreGuard {
			restoring: &self.restoring,
		}
	}

	/// Resets the pipeline for restore.
	pub(crate) fn reset_for_restore(&self, seq: u64) {
		self.ring.advance_taken(seq);
		self.ring.next.store(seq + 1, Ordering::Release);
		self.log_seq_num.store(seq + 1, Ordering::Release);
		self.inner.visible_seq_num.store(seq, Ordering::Release);
	}
}
