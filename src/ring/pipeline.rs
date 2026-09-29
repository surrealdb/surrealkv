use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{oneshot, Notify, Semaphore};

use super::bloom::BloomFilter;
use super::commit_ring::{CommitRing, SlotRead, DEFAULT_COMMIT_RING_CAPACITY};
use super::queue::{CommitEntry, EntryState, Payload};
use super::sync::backoff;
use crate::batch::Batch;
use crate::error::{Error, Result};
use crate::lsm::CoreInner;
use crate::stall::WriteStallController;
use crate::storage::{AffinityLogStore, LogStore};
use crate::task::TaskManager;
use crate::vlog::ValueLocation;
use crate::Key;

/// Coordinates OCC conflict detection over the commit ring with the
/// background flusher that makes accepted commits durable and visible.
///
/// A committer claims the next ring sequence, publishes its write set, and
/// then validates against every entry between its transaction's commit
/// window and its own sequence, waiting only for entries claimed earlier but
/// not yet published. Because every entry is published before it validates,
/// of any two overlapping committers the later one always sees the earlier.
/// Validation touches only the slots it reads, never a shared lock, so its
/// cost is parallel in the number of commits in flight.
///
/// The flusher drains accepted entries in ring order, assigns their LSM
/// sequence numbers in that same order, writes and syncs the WAL, applies the
/// memtable, advances `visible_seq_num`, and only then marks the entries
/// visible and advances the ring's completed prefix. A transaction that reads
/// the completed prefix before `visible_seq_num` therefore sees every commit
/// at or below the prefix in its snapshot, and validates against the rest.
pub(crate) struct CommitPipeline {
	pub(crate) ring: CommitRing<CommitEntry>,
	/// Entries that a lap of the ring overwrote while a live conflict window
	/// could still reach them, keyed by ring sequence. Only long-lived
	/// transactions, whose window spans more than a lap, ever read it.
	overflow: Mutex<BTreeMap<u64, Arc<CommitEntry>>>,
	/// Admission permits, one per claimed entry, released only once the
	/// completed prefix has passed it. At most half the ring's capacity of
	/// entries therefore sit above the completed prefix, so the previous
	/// occupant of a slot being claimed is always complete and publishing
	/// never waits on the flusher.
	admission: Arc<Semaphore>,
	pub(crate) inner: Arc<CoreInner>,
	pub(crate) write_stall: Arc<WriteStallController>,
	pub(crate) task_manager: Option<Arc<TaskManager>>,
	pub(crate) notify_flusher: Arc<Notify>,
	pub(crate) shutdown: AtomicBool,
	/// Sequence number allocator for batch entries, advanced only by the
	/// flusher (and restore), in ring order.
	pub(crate) log_seq_num: AtomicU64,
	/// Flag used to pause all incoming commits during a full restore from checkpoint.
	pub(crate) restoring: AtomicBool,
	/// Bumped by every restore; commits accepted in an earlier epoch are not
	/// applied to the restored state.
	restore_epoch: AtomicU64,
	/// Asynchronous log store interface for the WAL.
	pub(crate) log_store: Arc<dyn LogStore>,
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
		let ring = CommitRing::new(DEFAULT_COMMIT_RING_CAPACITY, 1);
		let admission = Arc::new(Semaphore::new(DEFAULT_COMMIT_RING_CAPACITY / 2));
		let notify_flusher = Arc::new(Notify::new());
		let seq = start_seq.max(1);
		let log_store: Arc<dyn LogStore> =
			Arc::new(AffinityLogStore::new(Arc::clone(&inner.wal.inner)));

		Self {
			ring,
			overflow: Mutex::new(BTreeMap::new()),
			admission,
			inner,
			write_stall,
			task_manager,
			notify_flusher,
			shutdown: AtomicBool::new(false),
			log_seq_num: AtomicU64::new(seq),
			restoring: AtomicBool::new(false),
			restore_epoch: AtomicU64::new(0),
			log_store,
		}
	}

	/// Starts the background flusher task.
	pub(crate) fn start_flusher(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
		let pipeline = Arc::clone(self);
		tokio::spawn(async move {
			pipeline.run_flusher().await;
		})
	}

	/// The value a mutating transaction pins in `active_txn_tracker` at
	/// begin. Read before [`commit_window`](Self::commit_window), so the
	/// retired watermark can never pass the window it then reads.
	pub(crate) fn commit_pin(&self) -> u64 {
		self.ring.taken()
	}

	/// The start of a beginning transaction's conflict window: every commit
	/// at or below it is visible in any snapshot read afterwards. Read after
	/// registering the pin, and before reading `visible_seq_num`.
	pub(crate) fn commit_window(&self) -> u64 {
		self.ring.completed()
	}

	/// Validates locked reads against every commit in the transaction's
	/// window without submitting a write.
	pub(crate) async fn check_conflicts(
		&self,
		read_keys: &[Key],
		start_seq: u64,
		window: u64,
	) -> Result<()> {
		if read_keys.is_empty() {
			return Ok(());
		}
		let read_bloom = bloom_of(read_keys);
		let permit =
			Arc::clone(&self.admission).acquire_owned().await.map_err(|_| Error::PipelineStall)?;
		// The claimed sequence bounds the window; the entry publishes nothing.
		let entry = Arc::new(CommitEntry::new(Vec::new(), permit));
		let seq = self.publish(&entry);
		let result = self.validate(&entry, seq, start_seq, window, read_keys, &read_bloom);
		entry.abort();
		self.ring.advance_completed();
		self.notify_flusher.notify_one();
		result
	}

	/// Commits a batch through the commit ring and the group-commit flusher.
	pub(crate) async fn commit(
		&self,
		batch: Batch,
		sync: bool,
		start_seq: u64,
		window: u64,
		read_set: &[Key],
	) -> Result<()> {
		if self.shutdown.load(Ordering::Acquire) {
			return Err(Error::PipelineStall);
		}

		// Check for background errors before proceeding
		self.inner.error_handler.check_error()?;

		// If batch is empty, only validate conflicts for locked reads
		if batch.is_empty() {
			return self.check_conflicts(read_set, start_seq, window).await;
		}

		// Write stall backpressure
		self.write_stall.check().await?;

		// The last suspension point before the entry is claimed: from here to
		// the verdict nothing awaits, so a cancelled commit can never leave a
		// claimed entry unpublished or undecided.
		let permit =
			Arc::clone(&self.admission).acquire_owned().await.map_err(|_| Error::PipelineStall)?;

		// Check if restore is in progress
		if self.restoring.load(Ordering::Acquire) {
			return Err(Error::PipelineStall);
		}

		let write_keys: Vec<Key> = batch.entries.iter().map(|e| e.key.clone()).collect();
		let read_bloom = bloom_of(read_set);
		let entry = Arc::new(CommitEntry::new(write_keys, permit));
		let seq = self.publish(&entry);

		if let Err(e) = self.validate(&entry, seq, start_seq, window, read_set, &read_bloom) {
			entry.abort();
			self.ring.advance_completed();
			self.notify_flusher.notify_one();
			return Err(e);
		}

		let (complete_tx, complete_rx) = oneshot::channel();
		entry.accept(Payload {
			batch,
			sync,
			complete_tx,
			epoch: self.restore_epoch.load(Ordering::SeqCst),
		});
		self.notify_flusher.notify_one();

		// Await durability & memtable application
		complete_rx.await.map_err(|_| Error::PipelineStall)?
	}

	/// Claims the next ring sequence and publishes `entry` into it.
	fn publish(&self, entry: &Arc<CommitEntry>) -> u64 {
		let seq = self.ring.claim();
		self.ring.publish_with(seq, Arc::clone(entry), |prev_seq, prev| {
			// A live window may still reach an entry above the retired
			// watermark: keep it where validation looks once its slot is gone.
			if prev_seq > self.ring.taken() {
				self.overflow.lock().insert(prev_seq, Arc::clone(prev));
			}
		});
		self.ring.advance_published();
		seq
	}

	/// Checks `entry`, published at `seq`, against every commit in
	/// `(window, seq)` that its snapshot at `start_seq` does not already see.
	/// Entries still in flight count as conflicts: they may yet commit.
	fn validate(
		&self,
		entry: &CommitEntry,
		seq: u64,
		start_seq: u64,
		window: u64,
		read_set: &[Key],
		read_bloom: &BloomFilter,
	) -> Result<()> {
		let conflicts = |other: &CommitEntry| -> bool {
			if other.is_ignorable_at(start_seq) {
				return false;
			}
			!other.is_disjoint_writeset(entry.keys(), entry.bloom())
				|| (!read_set.is_empty() && !other.is_disjoint_readset(read_set, read_bloom))
		};
		for s in (window + 1)..seq {
			let mut spins = 0;
			let conflict = loop {
				match self.ring.read(s, |other| conflicts(other)) {
					SlotRead::Ready(conflict) => break conflict,
					// Claimed before us but not yet published: the claimer
					// publishes without awaiting, so this is brief.
					SlotRead::Pending => {
						backoff(spins);
						spins += 1;
					}
					// Lapped while our window was open: it was kept in the
					// overflow. Retired entries sit below every live window,
					// so if it is missing, treat it conservatively.
					SlotRead::Gone => {
						break self.overflow.lock().get(&s).is_none_or(|other| conflicts(other));
					}
				}
			};
			if conflict {
				return Err(Error::TransactionWriteConflict);
			}
		}
		Ok(())
	}

	/// Background flusher loop performing group commit.
	async fn run_flusher(&self) {
		// The last ring sequence the flusher has consumed.
		let mut drained = self.ring.completed();
		loop {
			let shutdown = self.shutdown.load(Ordering::Acquire);

			// Gather every contiguous decided entry after `drained`, with the
			// permits to release once the completed prefix passes them.
			let mut group: Vec<(Arc<CommitEntry>, Payload)> = Vec::new();
			let mut permits = Vec::new();
			let mut next = drained + 1;
			loop {
				match self.ring.get(next) {
					SlotRead::Ready(entry) => match entry.state() {
						EntryState::InFlight => break,
						EntryState::Accepted => {
							let payload =
								entry.take_payload().expect("accepted entries carry a payload");
							permits.extend(entry.take_permit());
							group.push((entry, payload));
						}
						EntryState::Complete => permits.extend(entry.take_permit()),
					},
					SlotRead::Pending => break,
					// Unreachable while admission holds: an undrained entry
					// still holds its permit, so it is never lapped.
					SlotRead::Gone => {}
				}
				next += 1;
			}

			if next == drained + 1 {
				if shutdown {
					break;
				}
				// Wait for new published or decided entries
				self.notify_flusher.notified().await;
				continue;
			}
			drained = next - 1;

			if !group.is_empty() {
				self.flush_entries(group).await;
			}
			// Every drained entry is now complete, so the completed prefix
			// covers them and their permits may admit new claims.
			self.ring.advance_completed();
			drop(permits);
			self.retire();
		}
	}

	/// Assigns LSM sequence numbers to a group of accepted entries in ring
	/// order, makes them durable and visible, and completes their commits.
	async fn flush_entries(&self, group: Vec<(Arc<CommitEntry>, Payload)>) {
		let epoch = self.restore_epoch.load(Ordering::SeqCst);
		let mut entries = Vec::with_capacity(group.len());
		let mut batches = Vec::with_capacity(group.len());
		let mut waiters = Vec::with_capacity(group.len());
		let mut need_sync = false;
		for (entry, payload) in group {
			let Payload {
				mut batch,
				sync,
				complete_tx,
				epoch: accepted_in,
			} = payload;
			if accepted_in != epoch {
				// Accepted before a restore: never apply it to the restored state.
				entry.abort();
				let _ = complete_tx.send(Err(Error::PipelineStall));
				continue;
			}
			let count = batch.count() as u64;
			let start = self.log_seq_num.fetch_add(count, Ordering::SeqCst);
			batch.set_starting_seq_num(start);
			need_sync |= sync;
			entries.push((entry, start + count - 1));
			batches.push(batch);
			waiters.push(complete_tx);
		}
		if batches.is_empty() {
			return;
		}

		let result = self.flush_group(&batches, need_sync).await;
		let applied = result.is_ok() && self.restore_epoch.load(Ordering::SeqCst) == epoch;
		if applied {
			// Visibility first: an entry may only be marked visible, and so
			// fall below a new transaction's window, once `visible_seq_num`
			// covers it.
			let highest = entries.last().map(|(_, max)| *max).unwrap_or_default();
			self.inner.visible_seq_num.store(highest, Ordering::SeqCst);
			for (entry, max) in &entries {
				entry.make_visible(*max);
			}
		} else {
			if let Err(e) = &result {
				tracing::error!("Error during group commit flush: {:?}", e);
			}
			for (entry, _) in &entries {
				entry.abort();
			}
		}
		for complete_tx in waiters {
			let _ = complete_tx.send(match &result {
				Ok(()) if applied => Ok(()),
				Ok(()) => Err(Error::PipelineStall),
				Err(_) => Err(Error::Io(std::io::Error::other("Group commit failed").into())),
			});
		}
	}

	/// Advances the retired watermark to the oldest pinned transaction and
	/// drops overflow entries that no live window can reach any more.
	fn retire(&self) {
		// Read the completed prefix before scanning the pins: a transaction
		// that registers after the scan reads its window after this value.
		let completed = self.ring.completed();
		let bound = match self.inner.active_txn_tracker.oldest() {
			Some(pin) => pin.min(completed),
			None => completed,
		};
		self.ring.advance_taken(bound);
		let taken = self.ring.taken();
		let mut overflow = self.overflow.lock();
		if overflow.first_key_value().is_some_and(|(s, _)| *s <= taken) {
			*overflow = overflow.split_off(&(taken + 1));
		}
	}

	/// Flushes a group of batches to WAL and applies them to the Memtable.
	async fn flush_group(&self, batches: &[Batch], sync: bool) -> Result<()> {
		// 1. Process batches: separate large values to VLog if enabled (WiscKey WAL bypass)
		// and encode for WAL
		let vlog_threshold = self.inner.opts.vlog_value_threshold;
		let vlog = self.inner.vlog.as_ref();

		let mut processed_batches = Vec::with_capacity(batches.len());
		for batch in batches {
			let mut processed = Batch::new(batch.starting_seq_num);
			for (_, entry, seq_num, timestamp) in batch.entries_with_seq_nums()? {
				let encoded_value = if entry.kind == crate::InternalKeyKind::RangeDelete {
					entry.value.clone()
				} else {
					match &entry.value {
						Some(value) => {
							if let Some(vlog_inst) = vlog {
								if value.len() > vlog_threshold {
									let ikey = crate::InternalKey::new(
										entry.key.clone(),
										seq_num,
										entry.kind,
									);
									let encoded_key = ikey.encode();
									let pointer = vlog_inst.append(&encoded_key, value)?;
									let value_location = ValueLocation::with_pointer(pointer);
									Some(value_location.encode())
								} else {
									let value_location =
										ValueLocation::with_inline_value(value.clone());
									Some(value_location.encode())
								}
							} else {
								let value_location =
									ValueLocation::with_inline_value(value.clone());
								Some(value_location.encode())
							}
						}
						None => None,
					}
				};
				processed.add_record(entry.kind, entry.key.clone(), encoded_value, timestamp)?;
			}
			processed_batches.push(processed);
		}

		// 2. Append all batches to WAL asynchronously via LogStore in a single write
		if let Some(vlog_inst) = vlog {
			vlog_inst.flush()?;
		}

		let total_capacity: usize = processed_batches.iter().map(|b| b.size as usize + 64).sum();
		let mut wal_buffer = Vec::with_capacity(total_capacity);
		for batch in &processed_batches {
			batch.encode_into(&mut wal_buffer)?;
		}
		if !wal_buffer.is_empty() {
			self.log_store.append(&wal_buffer).await?;
		}
		if sync {
			if let Some(vlog_inst) = vlog {
				vlog_inst.sync()?;
			}
			self.log_store.sync().await?;
		}

		// 3. Apply to Active Memtable (or Direct-to-L0 Flush if oversized)
		let mut active = self.inner.active_memtable.read()?;
		for batch in &processed_batches {
			let needed = batch.memtable_size_estimate();
			if needed > self.inner.opts.max_memtable_size as u64 {
				// Batch exceeds max_memtable_size: bypass memtable and flush directly to L0.
				// Seal the active memtable's WAL segment first (rotating it out if it holds
				// earlier writes, or just rotating the WAL if it's already empty) so earlier
				// writes stay ordered before this L0 table, and so no future write can land
				// in the segment `write_batch_direct_to_l0_sst` is about to mark as captured.
				drop(active);
				let batch_wal_number = self.inner.seal_active_wal_segment()?;
				if let Some(ref tm) = self.task_manager {
					tm.wake_up_memtable();
				}

				let table_id = self.inner.level_manifest.read()?.next_table_id();
				self.inner.write_batch_direct_to_l0_sst(batch, table_id, batch_wal_number)?;

				if let Some(ref tm) = self.task_manager {
					tm.wake_up_level();
				}
				active = self.inner.active_memtable.read()?;
			} else {
				match active.add(batch) {
					Ok(()) => {}
					Err(Error::ArenaFull) => {
						drop(active);
						self.inner.rotate_memtable()?;
						if let Some(ref tm) = self.task_manager {
							tm.wake_up_memtable();
						}
						active = self.inner.active_memtable.read()?;
						if let Err(Error::ArenaFull) = active.add(batch) {
							// If it still doesn't fit even in an empty fresh memtable,
							// fallback to direct-to-L0 flush rather than failing hard.
							// `active` is guaranteed empty here (freshly rotated), so seal
							// its WAL segment too before advancing log_number past it.
							drop(active);
							let batch_wal_number = self.inner.seal_active_wal_segment()?;
							let table_id = self.inner.level_manifest.read()?.next_table_id();
							self.inner.write_batch_direct_to_l0_sst(
								batch,
								table_id,
								batch_wal_number,
							)?;
							if let Some(ref tm) = self.task_manager {
								tm.wake_up_level();
							}
							active = self.inner.active_memtable.read()?;
						}
					}
					Err(e) => return Err(e),
				}
			}
		}

		Ok(())
	}

	/// Shuts down the pipeline. The flusher makes every commit already
	/// accepted durable before it exits.
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

	/// Resets the pipeline for restore. Commits accepted before the restore
	/// fail rather than land in the restored state.
	pub(crate) fn reset_for_restore(&self, seq: u64) {
		self.restore_epoch.fetch_add(1, Ordering::SeqCst);
		self.log_seq_num.store(seq + 1, Ordering::SeqCst);
		self.inner.visible_seq_num.store(seq, Ordering::SeqCst);
		self.overflow.lock().clear();
	}
}

fn bloom_of(keys: &[Key]) -> BloomFilter {
	let mut bloom = BloomFilter::new();
	for k in keys {
		bloom.insert(k);
	}
	bloom
}
