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

/// Observation points inside `flush_group`, so tests can act at an exact step
/// of a commit group (for example rotate the memtable between the WAL sync and
/// the memtable apply) instead of racing the flusher.
#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(crate) enum PipelineHook {
	/// The group's WAL records are appended and, if the group syncs, synced;
	/// nothing is applied to a memtable yet.
	AfterWalSync {
		batches: usize,
	},
	/// An apply round is about to start. Round 0 is the first; later rounds
	/// start after the group was repaired or moved on (a rotation, a re-append
	/// of stale records, or a direct-to-L0 write).
	BeforeApplyRound {
		round: usize,
	},
}

#[cfg(test)]
pub(crate) type PipelineHookFn = Arc<dyn Fn(PipelineHook) + Send + Sync>;

/// How many times in a row a group may find its records in a segment other than the active
/// memtable's tag, with nothing applied in between, and log them again. One pass past a
/// rotation fixes it. A group that is still stale after that many passes fails: rotations keep
/// overtaking it, or the tag is one that no rotation will make equal to the segment.
const MAX_STALE_ROUNDS: u32 = 8;

/// Why `CommitPipeline::apply_run` stopped.
enum ApplyStop {
	/// Every remaining batch was applied.
	Done,
	/// The next batch exceeds `max_memtable_size`.
	Oversized,
	/// The next batch does not fit the active memtable, which is empty or not.
	ArenaFull {
		empty: bool,
	},
	/// The next batch was logged in a different segment than the one the active
	/// memtable is tagged with.
	Stale {
		active_tag: u64,
	},
}

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
	/// Asynchronous log store interface for the WAL. The concrete type, because the
	/// pipeline needs the segment each record was appended to.
	pub(crate) log_store: Arc<AffinityLogStore>,
	/// Test-only observer of `flush_group` steps.
	#[cfg(test)]
	hook: Mutex<Option<PipelineHookFn>>,
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
		let log_store = Arc::new(AffinityLogStore::new(Arc::clone(&inner.wal.inner)));

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
			#[cfg(test)]
			hook: Mutex::new(None),
		}
	}

	/// Installs (or clears) the observer called at each `PipelineHook` point.
	#[cfg(test)]
	pub(crate) fn set_hook(&self, hook: Option<PipelineHookFn>) {
		*self.hook.lock() = hook;
	}

	#[cfg(test)]
	fn fire(&self, point: PipelineHook) {
		// Clone out of the mutex first: the observer may block or call back in.
		let hook = self.hook.lock().clone();
		if let Some(hook) = hook {
			hook(point);
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
	pub(crate) async fn flush_group(&self, batches: &[Batch], sync: bool) -> Result<()> {
		let epoch = self.restore_epoch.load(Ordering::SeqCst);

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

		// 2. Append the batches to the WAL asynchronously, one record per batch, in one call
		// for the whole group, then sync once.
		//
		// Recovery decodes exactly one batch per record (`Batch::decode` rejects bytes
		// past the end of the batch), so the group goes to the WAL as the end offset of
		// every record next to the bytes of all of them, never as one record. Encoding
		// appends to the shared buffer, so every batch of the group is in it: all of them
		// are applied and acknowledged.
		//
		// The group is all or nothing. If anything fails, the WAL cuts the segment back to
		// where it was before the group and refuses further appends, so no record of a group
		// that was reported failed can be replayed after a crash.
		//
		// The segment the records landed in is kept: step 3 only applies a batch to a
		// memtable tagged with that segment.
		if let Some(vlog_inst) = vlog {
			vlog_inst.flush()?;
		}

		// The worst-case memtable size of each batch, computed once for the oversized test and
		// the rotation check.
		let max_memtable_size = self.inner.opts.max_memtable_size as u64;
		let estimates: Vec<u64> =
			processed_batches.iter().map(Batch::memtable_size_estimate).collect();
		let oversized: Vec<bool> =
			estimates.iter().map(|bytes| *bytes > max_memtable_size).collect();

		// If the first batch does not fit what is left of the active memtable, rotate
		// now, so it is logged in the segment of the memtable that will receive it.
		// This is only an optimisation for the common case (a lone writer then never
		// logs a record twice): every other case is repaired in step 3, because the
		// estimate is a worst case, so how far a group gets into a memtable is only
		// known by applying it.
		if oversized.first() == Some(&false) && self.needs_rotation_for(estimates[0])? {
			self.inner.rotate_memtable()?;
			if let Some(ref tm) = self.task_manager {
				tm.wake_up_memtable();
			}
		}

		let n = processed_batches.len();
		let mut wal_buf =
			Vec::with_capacity(processed_batches.iter().map(Batch::encoded_len_hint).sum());
		let mut wal_ends = Vec::with_capacity(n);
		for batch in &processed_batches {
			batch.encode_into(&mut wal_buf)?;
			wal_ends.push(wal_buf.len());
		}
		// One lock, one hand-off to the pool and one write for the group, and still one WAL
		// record per batch. The WAL only rotates under the lock the append holds, so the
		// whole group is in one segment. The buffers go to the pool and come back, and are
		// dropped instead if the append fails.
		let (segment, mut wal_buf, mut wal_ends) =
			self.log_store.append_group_returning_segment(wal_buf, wal_ends).await?;
		let mut segments = vec![segment; n];
		if sync {
			if let Some(vlog_inst) = vlog {
				vlog_inst.sync()?;
			}
			self.log_store.sync().await?;
		}
		#[cfg(test)]
		self.fire(PipelineHook::AfterWalSync {
			batches: n,
		});

		// 3. Apply to the active memtable (or direct-to-L0 flush if oversized).
		//
		// A batch is applied to a memtable only if its WAL record is in the segment
		// that memtable is tagged with. Flushing the memtable tagged N moves the
		// manifest's `log_number` past N and deletes segment N, so a batch applied to
		// a memtable tagged N + 1 whose only record is in segment N would be lost by
		// a crash before that memtable is flushed. The comparison is made under the
		// active memtable's read lock, which a rotation needs exclusively, so the tag
		// cannot change between the check and the insert.
		//
		// A batch whose record is in an older segment (a rotation, a direct-to-L0
		// write or a checkpoint happened after the append) is appended again, which
		// lands it in the current segment. The old record is stale: nothing relies
		// on it, and it goes away with its segment.
		//
		// A rotation can come between the append and the apply again, so this repeats.
		// A group that is found stale `MAX_STALE_ROUNDS` times in a row, with nothing
		// applied in between, is failed instead of logged again.
		let mut at = 0;
		let mut stale_rounds = 0;
		#[cfg(test)]
		let mut round = 0;
		while at < n {
			// A restore replaces the WAL and the memtables underneath this group:
			// stop before logging anything more into the restored WAL.
			if self.restoring.load(Ordering::Acquire)
				|| self.restore_epoch.load(Ordering::SeqCst) != epoch
			{
				return Err(Error::PipelineStall);
			}
			#[cfg(test)]
			{
				self.fire(PipelineHook::BeforeApplyRound {
					round,
				});
				round += 1;
			}

			let (next, stop) = self.apply_run(&processed_batches, &oversized, &segments, at)?;
			if next > at {
				stale_rounds = 0;
			}
			at = next;
			match stop {
				ApplyStop::Done => {}
				// Bypass the memtable: the batch exceeds `max_memtable_size`, or it cannot
				// fit even an empty memtable (rotating an empty memtable is a no-op, so
				// retrying would never end).
				ApplyStop::Oversized
				| ApplyStop::ArenaFull {
					empty: true,
				} => {
					self.write_direct_to_l0(&processed_batches[at])?;
					at += 1;
					stale_rounds = 0;
				}
				ApplyStop::ArenaFull {
					empty: false,
				} => {
					self.inner.rotate_memtable()?;
					if let Some(ref tm) = self.task_manager {
						tm.wake_up_memtable();
					}
				}
				ApplyStop::Stale {
					active_tag,
				} => {
					if stale_rounds == MAX_STALE_ROUNDS {
						return Err(tag_mismatch(&segments[at..], active_tag));
					}
					stale_rounds += 1;
					self.reappend_stale(
						&processed_batches[at..],
						&oversized[at..],
						&mut segments[at..],
						active_tag,
						sync,
						&mut wal_buf,
						&mut wal_ends,
					)
					.await?;
				}
			}
		}

		Ok(())
	}

	/// Whether a batch that needs `bytes` of memtable does not fit what is left of a
	/// non-empty active memtable, so the memtable has to rotate before the batch is logged.
	fn needs_rotation_for(&self, bytes: u64) -> Result<bool> {
		let active = self.inner.active_memtable.read()?;
		Ok(!active.is_empty() && !active.can_fit(bytes))
	}

	/// Applies `batches[from..]` to the active memtable under one read guard and
	/// returns the index it got to and why it stopped. `oversized` says which batches
	/// are too big for a memtable and `segments` where each batch's record is.
	///
	/// Synchronous on purpose: no lock guard may live across an await in the
	/// flusher, which runs as a spawned task.
	fn apply_run(
		&self,
		batches: &[Batch],
		oversized: &[bool],
		segments: &[u64],
		from: usize,
	) -> Result<(usize, ApplyStop)> {
		let active = self.inner.active_memtable.read()?;
		let tag = active.get_wal_number();
		let mut at = from;
		while at < batches.len() {
			if oversized[at] {
				return Ok((at, ApplyStop::Oversized));
			}
			if segments[at] != tag {
				return Ok((
					at,
					ApplyStop::Stale {
						active_tag: tag,
					},
				));
			}
			match active.add(&batches[at]) {
				Ok(()) => at += 1,
				Err(Error::ArenaFull) => {
					return Ok((
						at,
						ApplyStop::ArenaFull {
							empty: active.is_empty(),
						},
					));
				}
				Err(e) => return Err(e),
			}
		}
		Ok((at, ApplyStop::Done))
	}

	/// Appends again the batches whose record is not in the segment the active memtable is
	/// tagged with (see `encode_stale`), and syncs if the group syncs.
	///
	/// A record already in that segment is never appended a second time. The value
	/// log needs no second pass: its entries were synced with the first.
	///
	/// The stale batches go to the WAL as one group, so they land in one segment. They are
	/// encoded into the group's own buffers, which the first append gave back.
	#[allow(clippy::too_many_arguments)]
	async fn reappend_stale(
		&self,
		batches: &[Batch],
		oversized: &[bool],
		segments: &mut [u64],
		active_tag: u64,
		sync: bool,
		wal_buf: &mut Vec<u8>,
		wal_ends: &mut Vec<usize>,
	) -> Result<()> {
		let stale = encode_stale(batches, oversized, segments, active_tag, wal_buf, wal_ends)?;
		if stale.is_empty() {
			return Ok(());
		}
		let (segment, buf, ends) = self
			.log_store
			.append_group_returning_segment(std::mem::take(wal_buf), std::mem::take(wal_ends))
			.await?;
		*wal_buf = buf;
		*wal_ends = ends;
		for j in stale {
			segments[j] = segment;
		}
		if sync {
			self.log_store.sync().await?;
		}
		Ok(())
	}

	/// Writes `batch` straight to a new L0 table, bypassing the memtable.
	///
	/// The active memtable's WAL segment is sealed first (rotated out if it holds
	/// earlier writes, or just rotated if it is empty), so earlier writes stay
	/// ordered before the table and no later write can land in the segment that
	/// `write_batch_direct_to_l0_sst` marks as captured.
	fn write_direct_to_l0(&self, batch: &Batch) -> Result<()> {
		let sealed_wal_number = self.inner.seal_active_wal_segment()?;
		if let Some(ref tm) = self.task_manager {
			tm.wake_up_memtable();
		}

		let table_id = self.inner.level_manifest.read()?.next_table_id();
		self.inner.write_batch_direct_to_l0_sst(batch, table_id, sealed_wal_number)?;

		if let Some(ref tm) = self.task_manager {
			tm.wake_up_level();
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

/// Encodes, one record each, the batches whose record is not in the segment `tag` into the
/// emptied `wal_buf` and `wal_ends`, and returns the index of each of these batches. Stops at the
/// first oversized batch: it is written to an L0 table instead, which makes its record redundant
/// and seals the segment, so a record behind it would be stale again before it could be applied.
///
/// The buffers are the group's own, which the first append gave back.
fn encode_stale(
	batches: &[Batch],
	oversized: &[bool],
	segments: &[u64],
	tag: u64,
	wal_buf: &mut Vec<u8>,
	wal_ends: &mut Vec<usize>,
) -> Result<Vec<usize>> {
	wal_buf.clear();
	wal_ends.clear();
	let mut stale = Vec::new();
	for j in 0..batches.len() {
		if oversized[j] {
			break;
		}
		if segments[j] == tag {
			continue;
		}
		batches[j].encode_into(wal_buf)?;
		wal_ends.push(wal_buf.len());
		stale.push(j);
	}
	Ok(stale)
}

/// The error of a group whose records stay in segments other than the active memtable's tag
/// however often they are logged again.
fn tag_mismatch(segments: &[u64], tag: u64) -> Error {
	Error::Other(format!(
		"WAL segment and memtable tag mismatch: records in segments {segments:?}, active \
		 memtable tagged {tag}"
	))
}

fn bloom_of(keys: &[Key]) -> BloomFilter {
	let mut bloom = BloomFilter::new();
	for k in keys {
		bloom.insert(k);
	}
	bloom
}
